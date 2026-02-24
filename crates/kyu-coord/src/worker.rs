//! Worker pool: threads that pull tasks from the queue and execute them.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::task::{Task, TaskQueue, TaskResult};

/// Callback invoked when a worker completes (or fails) a task.
pub type TaskHandler = dyn Fn(Task) -> TaskResult + Send + Sync + 'static;

/// Pool of worker threads pulling from a shared `TaskQueue`.
///
/// Workers block on the queue waiting for tasks. When a task arrives,
/// the worker invokes the handler and reports the result.
pub struct WorkerPool {
    workers: Vec<JoinHandle<()>>,
    queue: Arc<TaskQueue>,
    shutdown: Arc<AtomicBool>,
}

impl WorkerPool {
    /// Create a worker pool with `num_workers` threads.
    ///
    /// The `handler` is called for each task. It receives the `Task` and
    /// should return a `TaskResult`.
    pub fn new(num_workers: usize, queue: Arc<TaskQueue>, handler: Arc<TaskHandler>) -> Self {
        let shutdown = Arc::new(AtomicBool::new(false));
        let mut workers = Vec::with_capacity(num_workers);

        for worker_id in 0..num_workers {
            let q = Arc::clone(&queue);
            let h = Arc::clone(&handler);
            let s = Arc::clone(&shutdown);

            let handle = thread::Builder::new()
                .name(format!("kyu-worker-{worker_id}"))
                .spawn(move || {
                    Self::worker_loop(&q, &*h, &s);
                })
                .expect("failed to spawn worker thread");

            workers.push(handle);
        }

        Self {
            workers,
            queue,
            shutdown,
        }
    }

    fn worker_loop(queue: &TaskQueue, handler: &TaskHandler, shutdown: &AtomicBool) {
        loop {
            if shutdown.load(Ordering::Relaxed) {
                break;
            }

            // Try to pop a task (non-blocking).
            if let Some(task) = queue.try_pop() {
                let _result = handler(task);
                // Result handling (logging, metrics) would go here.
                continue;
            }

            // No task available — sleep briefly before retrying.
            // This avoids using pop_blocking during shutdown scenarios.
            if shutdown.load(Ordering::Relaxed) {
                break;
            }
            thread::sleep(Duration::from_millis(10));
        }
    }

    /// Number of worker threads.
    pub fn num_workers(&self) -> usize {
        self.workers.len()
    }

    /// Signal all workers to stop and wait for them to finish.
    pub fn shutdown(self) {
        self.shutdown.store(true, Ordering::Relaxed);
        self.queue.notify_all();

        for handle in self.workers {
            let _ = handle.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::{Task, TaskPriority};
    use crate::tenant::TenantId;

    use std::sync::atomic::AtomicUsize;

    #[test]
    fn worker_pool_processes_tasks() {
        let queue = Arc::new(TaskQueue::new());
        let counter = Arc::new(AtomicUsize::new(0));
        let counter2 = Arc::clone(&counter);

        let handler: Arc<TaskHandler> = Arc::new(move |task: Task| {
            counter2.fetch_add(1, Ordering::Relaxed);
            TaskResult::Success {
                task_id: task.id,
                message: "ok".into(),
            }
        });

        let pool = WorkerPool::new(2, Arc::clone(&queue), handler);

        // Submit 5 tasks.
        for _ in 0..5 {
            queue.push(Task::new(
                TenantId::new("t1"),
                "RETURN 1".into(),
                TaskPriority::Normal,
            ));
        }

        // Wait for processing.
        thread::sleep(Duration::from_millis(200));

        assert_eq!(counter.load(Ordering::Relaxed), 5);
        pool.shutdown();
    }

    #[test]
    fn worker_pool_shutdown_gracefully() {
        let queue = Arc::new(TaskQueue::new());
        let handler: Arc<TaskHandler> = Arc::new(|task: Task| TaskResult::Success {
            task_id: task.id,
            message: "ok".into(),
        });

        let pool = WorkerPool::new(2, Arc::clone(&queue), handler);
        assert_eq!(pool.num_workers(), 2);

        // Shutdown without any tasks — should not hang.
        pool.shutdown();
    }

    #[test]
    fn worker_pool_handles_errors() {
        let queue = Arc::new(TaskQueue::new());
        let results = Arc::new(Mutex::new(Vec::new()));
        let results2 = Arc::clone(&results);

        use std::sync::Mutex;
        let handler: Arc<TaskHandler> = Arc::new(move |task: Task| {
            let result = TaskResult::Error {
                task_id: task.id,
                error: "simulated failure".into(),
            };
            results2.lock().unwrap().push(task.id);
            result
        });

        let pool = WorkerPool::new(1, Arc::clone(&queue), handler);

        queue.push(Task::new(
            TenantId::new("t1"),
            "FAIL".into(),
            TaskPriority::Normal,
        ));

        thread::sleep(Duration::from_millis(100));

        assert_eq!(results.lock().unwrap().len(), 1);
        pool.shutdown();
    }
}

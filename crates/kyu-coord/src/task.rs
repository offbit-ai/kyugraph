//! Task queue: priority-ordered work items for the worker pool.

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Condvar, Mutex};
use std::time::Instant;

use crate::tenant::TenantId;

/// Unique identifier for a task.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct TaskId(pub u64);

static NEXT_TASK_ID: AtomicU64 = AtomicU64::new(1);

impl TaskId {
    /// Allocate the next globally unique task ID.
    pub fn next() -> Self {
        Self(NEXT_TASK_ID.fetch_add(1, AtomicOrdering::Relaxed))
    }
}

impl std::fmt::Display for TaskId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "task-{}", self.0)
    }
}

/// Task priority level.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum TaskPriority {
    Low = 0,
    Normal = 1,
    High = 2,
}

/// A unit of work submitted to the coordinator.
pub struct Task {
    pub id: TaskId,
    pub tenant_id: TenantId,
    pub query: String,
    pub priority: TaskPriority,
    pub submitted_at: Instant,
}

impl Task {
    /// Create a new task with the given priority.
    pub fn new(tenant_id: TenantId, query: String, priority: TaskPriority) -> Self {
        Self {
            id: TaskId::next(),
            tenant_id,
            query,
            priority,
            submitted_at: Instant::now(),
        }
    }
}

/// Result of executing a task.
pub enum TaskResult {
    /// Query completed successfully with the given result string.
    Success { task_id: TaskId, message: String },
    /// Query failed with an error.
    Error { task_id: TaskId, error: String },
}

/// Wrapper for priority ordering in the binary heap.
struct PrioritizedTask {
    task: Task,
}

impl PartialEq for PrioritizedTask {
    fn eq(&self, other: &Self) -> bool {
        self.task.id == other.task.id
    }
}

impl Eq for PrioritizedTask {}

impl PartialOrd for PrioritizedTask {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PrioritizedTask {
    fn cmp(&self, other: &Self) -> Ordering {
        // Higher priority first, then earlier submission time.
        self.task
            .priority
            .cmp(&other.task.priority)
            .then_with(|| other.task.submitted_at.cmp(&self.task.submitted_at))
    }
}

/// Thread-safe priority task queue.
///
/// Tasks are dequeued in priority order (High > Normal > Low).
/// Within the same priority, earlier submissions are dequeued first (FIFO).
pub struct TaskQueue {
    heap: Mutex<BinaryHeap<PrioritizedTask>>,
    condvar: Condvar,
}

impl TaskQueue {
    pub fn new() -> Self {
        Self {
            heap: Mutex::new(BinaryHeap::new()),
            condvar: Condvar::new(),
        }
    }

    /// Submit a task to the queue.
    pub fn push(&self, task: Task) {
        let mut heap = self.heap.lock().unwrap();
        heap.push(PrioritizedTask { task });
        self.condvar.notify_one();
    }

    /// Pop the highest-priority task, or `None` if the queue is empty.
    pub fn try_pop(&self) -> Option<Task> {
        let mut heap = self.heap.lock().unwrap();
        heap.pop().map(|p| p.task)
    }

    /// Block until a task is available, then pop it.
    pub fn pop_blocking(&self) -> Task {
        let mut heap = self.heap.lock().unwrap();
        loop {
            if let Some(pt) = heap.pop() {
                return pt.task;
            }
            heap = self.condvar.wait(heap).unwrap();
        }
    }

    /// Number of pending tasks.
    pub fn len(&self) -> usize {
        self.heap.lock().unwrap().len()
    }

    /// Whether the queue is empty.
    pub fn is_empty(&self) -> bool {
        self.heap.lock().unwrap().is_empty()
    }

    /// Wake all waiting workers (used during shutdown).
    pub fn notify_all(&self) {
        self.condvar.notify_all();
    }
}

impl Default for TaskQueue {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_task(priority: TaskPriority, query: &str) -> Task {
        Task::new(TenantId::new("t1"), query.into(), priority)
    }

    #[test]
    fn task_id_auto_increment() {
        let id1 = TaskId::next();
        let id2 = TaskId::next();
        assert!(id2.0 > id1.0);
    }

    #[test]
    fn task_id_display() {
        let id = TaskId(42);
        assert_eq!(id.to_string(), "task-42");
    }

    #[test]
    fn queue_push_and_pop() {
        let q = TaskQueue::new();
        q.push(make_task(TaskPriority::Normal, "MATCH (n) RETURN n"));
        assert_eq!(q.len(), 1);
        assert!(!q.is_empty());

        let task = q.try_pop().unwrap();
        assert_eq!(task.query, "MATCH (n) RETURN n");
        assert!(q.is_empty());
    }

    #[test]
    fn queue_empty_returns_none() {
        let q = TaskQueue::new();
        assert!(q.try_pop().is_none());
    }

    #[test]
    fn queue_priority_ordering() {
        let q = TaskQueue::new();

        // Push in order: low, normal, high.
        q.push(make_task(TaskPriority::Low, "low"));
        q.push(make_task(TaskPriority::Normal, "normal"));
        q.push(make_task(TaskPriority::High, "high"));

        // Pop should return: high, normal, low.
        assert_eq!(q.try_pop().unwrap().query, "high");
        assert_eq!(q.try_pop().unwrap().query, "normal");
        assert_eq!(q.try_pop().unwrap().query, "low");
    }

    #[test]
    fn queue_same_priority_fifo() {
        let q = TaskQueue::new();

        q.push(make_task(TaskPriority::Normal, "first"));
        // Small delay to ensure different timestamps.
        std::thread::sleep(std::time::Duration::from_millis(1));
        q.push(make_task(TaskPriority::Normal, "second"));

        assert_eq!(q.try_pop().unwrap().query, "first");
        assert_eq!(q.try_pop().unwrap().query, "second");
    }

    #[test]
    fn pop_blocking_wakes_on_push() {
        use std::sync::Arc;
        use std::thread;
        use std::time::Duration;

        let q = Arc::new(TaskQueue::new());
        let q2 = Arc::clone(&q);

        let handle = thread::spawn(move || {
            let task = q2.pop_blocking();
            task.query
        });

        // Give the blocking thread time to start waiting.
        thread::sleep(Duration::from_millis(50));
        q.push(make_task(TaskPriority::Normal, "wakeup"));

        let result = handle.join().unwrap();
        assert_eq!(result, "wakeup");
    }

    #[test]
    fn priority_ordering_enum() {
        assert!(TaskPriority::High > TaskPriority::Normal);
        assert!(TaskPriority::Normal > TaskPriority::Low);
    }
}

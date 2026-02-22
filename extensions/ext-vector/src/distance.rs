//! SIMD-accelerated distance functions for vector similarity search.
//!
//! Uses NEON intrinsics on aarch64 (Apple Silicon), with scalar fallback
//! for other architectures (auto-vectorized by LLVM to AVX2/SSE).

/// Distance metric for vector similarity.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DistanceMetric {
    /// Squared Euclidean distance: sum((a[i] - b[i])^2).
    L2,
    /// Cosine distance: 1 - (a . b) / (|a| * |b|).
    Cosine,
}

impl DistanceMetric {
    /// Compute distance between two vectors.
    #[inline]
    pub fn distance(self, a: &[f32], b: &[f32]) -> f32 {
        match self {
            DistanceMetric::L2 => l2_distance(a, b),
            DistanceMetric::Cosine => cosine_distance(a, b),
        }
    }
}

/// Squared L2 (Euclidean) distance.
#[inline]
pub fn l2_distance(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len());

    #[cfg(target_arch = "aarch64")]
    {
        l2_neon(a, b)
    }

    #[cfg(not(target_arch = "aarch64"))]
    {
        l2_scalar(a, b)
    }
}

/// Cosine distance: 1.0 - cosine_similarity.
#[inline]
pub fn cosine_distance(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len());

    #[cfg(target_arch = "aarch64")]
    {
        cosine_neon(a, b)
    }

    #[cfg(not(target_arch = "aarch64"))]
    {
        cosine_scalar(a, b)
    }
}

// ---- Scalar implementations (auto-vectorize well with LLVM) ----

#[allow(dead_code)]
fn l2_scalar(a: &[f32], b: &[f32]) -> f32 {
    let mut sum = 0.0f32;
    for i in 0..a.len() {
        let d = a[i] - b[i];
        sum += d * d;
    }
    sum
}

#[allow(dead_code)]
fn cosine_scalar(a: &[f32], b: &[f32]) -> f32 {
    let mut dot = 0.0f32;
    let mut norm_a = 0.0f32;
    let mut norm_b = 0.0f32;
    for i in 0..a.len() {
        dot += a[i] * b[i];
        norm_a += a[i] * a[i];
        norm_b += b[i] * b[i];
    }
    let denom = (norm_a * norm_b).sqrt();
    if denom == 0.0 {
        1.0 // zero vectors → maximum distance
    } else {
        1.0 - dot / denom
    }
}

// ---- NEON implementations (aarch64) ----

#[cfg(target_arch = "aarch64")]
fn l2_neon(a: &[f32], b: &[f32]) -> f32 {
    use core::arch::aarch64::*;

    let n = a.len();
    let chunks = n / 4;
    let sum;

    // SAFETY: We read exactly `chunks * 4` aligned f32 values from both slices.
    unsafe {
        let mut acc = vdupq_n_f32(0.0);
        let pa = a.as_ptr();
        let pb = b.as_ptr();

        for i in 0..chunks {
            let va = vld1q_f32(pa.add(i * 4));
            let vb = vld1q_f32(pb.add(i * 4));
            let diff = vsubq_f32(va, vb);
            acc = vfmaq_f32(acc, diff, diff);
        }

        sum = vaddvq_f32(acc);
    }

    // Scalar tail for remaining elements.
    let mut tail_sum = sum;
    for i in (chunks * 4)..n {
        let d = a[i] - b[i];
        tail_sum += d * d;
    }
    tail_sum
}

#[cfg(target_arch = "aarch64")]
fn cosine_neon(a: &[f32], b: &[f32]) -> f32 {
    use core::arch::aarch64::*;

    let n = a.len();
    let chunks = n / 4;
    let (dot, norm_a, norm_b);

    // SAFETY: We read exactly `chunks * 4` aligned f32 values from both slices.
    unsafe {
        let mut acc_dot = vdupq_n_f32(0.0);
        let mut acc_na = vdupq_n_f32(0.0);
        let mut acc_nb = vdupq_n_f32(0.0);
        let pa = a.as_ptr();
        let pb = b.as_ptr();

        for i in 0..chunks {
            let va = vld1q_f32(pa.add(i * 4));
            let vb = vld1q_f32(pb.add(i * 4));
            acc_dot = vfmaq_f32(acc_dot, va, vb);
            acc_na = vfmaq_f32(acc_na, va, va);
            acc_nb = vfmaq_f32(acc_nb, vb, vb);
        }

        dot = vaddvq_f32(acc_dot);
        norm_a = vaddvq_f32(acc_na);
        norm_b = vaddvq_f32(acc_nb);
    }

    // Scalar tail.
    let mut t_dot = dot;
    let mut t_na = norm_a;
    let mut t_nb = norm_b;
    for i in (chunks * 4)..n {
        t_dot += a[i] * b[i];
        t_na += a[i] * a[i];
        t_nb += b[i] * b[i];
    }

    let denom = (t_na * t_nb).sqrt();
    if denom == 0.0 {
        1.0
    } else {
        1.0 - t_dot / denom
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn l2_known_vectors() {
        let a = [1.0f32, 0.0, 0.0];
        let b = [0.0f32, 1.0, 0.0];
        let d = l2_distance(&a, &b);
        assert!((d - 2.0).abs() < 1e-6, "L2([1,0,0], [0,1,0]) = {d}, expected 2.0");
    }

    #[test]
    fn l2_identical() {
        let a = [1.0f32, 2.0, 3.0];
        assert!((l2_distance(&a, &a) - 0.0).abs() < 1e-6);
    }

    #[test]
    fn cosine_orthogonal() {
        let a = [1.0f32, 0.0, 0.0];
        let b = [0.0f32, 1.0, 0.0];
        let d = cosine_distance(&a, &b);
        assert!((d - 1.0).abs() < 1e-6, "cosine orthogonal = {d}, expected 1.0");
    }

    #[test]
    fn cosine_identical() {
        let a = [1.0f32, 2.0, 3.0];
        let d = cosine_distance(&a, &a);
        assert!(d.abs() < 1e-5, "cosine identical = {d}, expected ~0.0");
    }

    #[test]
    fn cosine_zero_vector() {
        let a = [0.0f32; 3];
        let b = [1.0f32, 2.0, 3.0];
        assert_eq!(cosine_distance(&a, &b), 1.0);
    }

    #[test]
    fn high_dim_consistency() {
        // Test with higher-dimensional vectors to exercise SIMD lanes.
        let dim = 128;
        let a: Vec<f32> = (0..dim).map(|i| i as f32 * 0.1).collect();
        let b: Vec<f32> = (0..dim).map(|i| (i as f32 * 0.1) + 0.5).collect();

        let l2 = l2_distance(&a, &b);
        // Each element differs by 0.5, so L2 = dim * 0.25 = 32.0
        assert!((l2 - 32.0).abs() < 0.01, "L2 128-d = {l2}, expected 32.0");

        let cos = cosine_distance(&a, &b);
        assert!(cos >= 0.0 && cos <= 1.0, "cosine 128-d = {cos}, out of range");
    }

    #[test]
    fn metric_dispatch() {
        let a = [1.0f32, 0.0];
        let b = [0.0f32, 1.0];

        let l2 = DistanceMetric::L2.distance(&a, &b);
        let cos = DistanceMetric::Cosine.distance(&a, &b);
        assert!((l2 - 2.0).abs() < 1e-6);
        assert!((cos - 1.0).abs() < 1e-6);
    }
}

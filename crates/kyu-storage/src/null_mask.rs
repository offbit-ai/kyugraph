//! Packed u64 bitset for null tracking.
//!
//! Matches Kuzu's `NullMask`: bit=1 means NULL, bit=0 means non-null.
//! Each u64 entry tracks 64 values.

const BITS_PER_ENTRY_LOG2: u32 = 6;
const BITS_PER_ENTRY: u64 = 1 << BITS_PER_ENTRY_LOG2;
const NO_NULL_ENTRY: u64 = 0;
const ALL_NULL_ENTRY: u64 = !0u64;

/// Packed null bitmask where bit=1 means NULL, bit=0 means non-null.
#[derive(Clone, Debug)]
pub struct NullMask {
    data: Vec<u64>,
    may_contain_nulls: bool,
}

impl NullMask {
    /// Create a new null mask with all values set to non-null.
    pub fn new(capacity: u64) -> Self {
        let num_entries = num_entries_for(capacity);
        Self {
            data: vec![NO_NULL_ENTRY; num_entries],
            may_contain_nulls: false,
        }
    }

    /// Create a null mask with all values set to null.
    pub fn all_null(capacity: u64) -> Self {
        let num_entries = num_entries_for(capacity);
        Self {
            data: vec![ALL_NULL_ENTRY; num_entries],
            may_contain_nulls: true,
        }
    }

    /// Fast-path: returns `true` if no nulls are guaranteed absent.
    #[inline]
    pub fn has_no_nulls_guarantee(&self) -> bool {
        !self.may_contain_nulls
    }

    /// Check whether a specific position is null.
    #[inline]
    pub fn is_null(&self, pos: u64) -> bool {
        let (entry_idx, bit_idx) = entry_and_bit(pos);
        (self.data[entry_idx] >> bit_idx) & 1 != 0
    }

    /// Set a specific position to null or non-null.
    #[inline]
    pub fn set_null(&mut self, pos: u64, is_null: bool) {
        let (entry_idx, bit_idx) = entry_and_bit(pos);
        if is_null {
            self.data[entry_idx] |= 1u64 << bit_idx;
            self.may_contain_nulls = true;
        } else {
            self.data[entry_idx] &= !(1u64 << bit_idx);
        }
    }

    /// Set all positions to non-null.
    pub fn set_all_non_null(&mut self) {
        if !self.may_contain_nulls {
            return;
        }
        self.data.fill(NO_NULL_ENTRY);
        self.may_contain_nulls = false;
    }

    /// Set all positions to null.
    pub fn set_all_null(&mut self) {
        self.data.fill(ALL_NULL_ENTRY);
        self.may_contain_nulls = true;
    }

    /// Set a range of bits to null or non-null.
    pub fn set_null_range(&mut self, offset: u64, count: u64, is_null: bool) {
        if count == 0 {
            return;
        }
        if is_null {
            self.may_contain_nulls = true;
        }
        let fill = if is_null { ALL_NULL_ENTRY } else { NO_NULL_ENTRY };
        let end = offset + count;
        let (start_entry, start_bit) = entry_and_bit(offset);
        let (end_entry, end_bit) = entry_and_bit(end);

        if start_entry == end_entry {
            let mask = lower_mask(count) << start_bit;
            if is_null {
                self.data[start_entry] |= mask;
            } else {
                self.data[start_entry] &= !mask;
            }
            return;
        }

        // First partial entry
        if start_bit != 0 {
            let mask = ALL_NULL_ENTRY << start_bit;
            if is_null {
                self.data[start_entry] |= mask;
            } else {
                self.data[start_entry] &= !mask;
            }
        } else {
            self.data[start_entry] = fill;
        }

        // Full entries in the middle
        for entry in &mut self.data[start_entry + if start_bit != 0 { 1 } else { 0 }..end_entry] {
            *entry = fill;
        }

        // Last partial entry
        if end_bit != 0 {
            let mask = lower_mask(end_bit as u64);
            if is_null {
                self.data[end_entry] |= mask;
            } else {
                self.data[end_entry] &= !mask;
            }
        }
    }

    /// Count the number of null bits set in the mask.
    pub fn count_nulls(&self) -> u64 {
        if !self.may_contain_nulls {
            return 0;
        }
        self.data.iter().map(|e| e.count_ones() as u64).sum()
    }

    /// Copy null bits from another mask.
    /// Returns `true` if any null bit was copied.
    pub fn copy_from(
        &mut self,
        src: &NullMask,
        src_offset: u64,
        dst_offset: u64,
        count: u64,
    ) -> bool {
        if count == 0 {
            return false;
        }
        if src.has_no_nulls_guarantee() {
            self.set_null_range(dst_offset, count, false);
            return false;
        }

        let mut any_null = false;
        for i in 0..count {
            let is_null = src.is_null(src_offset + i);
            self.set_null(dst_offset + i, is_null);
            any_null |= is_null;
        }
        any_null
    }

    /// Resize the mask to a new capacity. New bits are set to non-null.
    pub fn resize(&mut self, new_capacity: u64) {
        let new_num_entries = num_entries_for(new_capacity);
        self.data.resize(new_num_entries, NO_NULL_ENTRY);
    }

    /// Number of u64 entries in the backing buffer.
    pub fn num_entries(&self) -> usize {
        self.data.len()
    }

    /// Total number of bits the mask can track.
    pub fn capacity(&self) -> u64 {
        self.data.len() as u64 * BITS_PER_ENTRY
    }

    /// Access the raw data slice (read-only).
    pub fn data(&self) -> &[u64] {
        &self.data
    }

    /// Construct from a raw u64 vec (used by JIT output).
    pub fn from_raw(data: Vec<u64>, capacity: u64) -> Self {
        let may_contain_nulls = data.iter().any(|&w| w != 0);
        let mut mask = Self { data, may_contain_nulls };
        let needed = num_entries_for(capacity);
        mask.data.resize(needed, NO_NULL_ENTRY);
        mask
    }
}

#[inline]
fn entry_and_bit(pos: u64) -> (usize, u32) {
    let entry = (pos >> BITS_PER_ENTRY_LOG2) as usize;
    let bit = (pos & (BITS_PER_ENTRY - 1)) as u32;
    (entry, bit)
}

#[inline]
fn num_entries_for(capacity: u64) -> usize {
    capacity.div_ceil(BITS_PER_ENTRY) as usize
}

#[inline]
fn lower_mask(count: u64) -> u64 {
    if count >= 64 {
        ALL_NULL_ENTRY
    } else {
        (1u64 << count) - 1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_mask_all_non_null() {
        let mask = NullMask::new(100);
        assert!(mask.has_no_nulls_guarantee());
        for i in 0..100 {
            assert!(!mask.is_null(i));
        }
    }

    #[test]
    fn all_null_mask() {
        let mask = NullMask::all_null(100);
        assert!(!mask.has_no_nulls_guarantee());
        for i in 0..100 {
            assert!(mask.is_null(i));
        }
    }

    #[test]
    fn set_and_check_null() {
        let mut mask = NullMask::new(128);
        mask.set_null(0, true);
        mask.set_null(63, true);
        mask.set_null(64, true);
        mask.set_null(127, true);

        assert!(mask.is_null(0));
        assert!(mask.is_null(63));
        assert!(mask.is_null(64));
        assert!(mask.is_null(127));
        assert!(!mask.is_null(1));
        assert!(!mask.is_null(62));
        assert!(!mask.is_null(65));
    }

    #[test]
    fn set_null_then_clear() {
        let mut mask = NullMask::new(64);
        mask.set_null(10, true);
        assert!(mask.is_null(10));
        mask.set_null(10, false);
        assert!(!mask.is_null(10));
    }

    #[test]
    fn count_nulls_empty() {
        let mask = NullMask::new(256);
        assert_eq!(mask.count_nulls(), 0);
    }

    #[test]
    fn count_nulls_some() {
        let mut mask = NullMask::new(256);
        mask.set_null(0, true);
        mask.set_null(100, true);
        mask.set_null(255, true);
        assert_eq!(mask.count_nulls(), 3);
    }

    #[test]
    fn set_null_range_within_single_entry() {
        let mut mask = NullMask::new(64);
        mask.set_null_range(4, 8, true);
        for i in 0..64 {
            assert_eq!(mask.is_null(i), (4..12).contains(&i), "pos {i}");
        }
    }

    #[test]
    fn set_null_range_across_entries() {
        let mut mask = NullMask::new(256);
        mask.set_null_range(60, 10, true);
        for i in 0..256 {
            assert_eq!(mask.is_null(i), (60..70).contains(&i), "pos {i}");
        }
    }

    #[test]
    fn set_null_range_full_entries() {
        let mut mask = NullMask::new(256);
        mask.set_null_range(0, 256, true);
        assert_eq!(mask.count_nulls(), 256);
    }

    #[test]
    fn set_null_range_clear() {
        let mut mask = NullMask::all_null(128);
        mask.set_null_range(10, 20, false);
        for i in 0..128 {
            assert_eq!(mask.is_null(i), !(10..30).contains(&i), "pos {i}");
        }
    }

    #[test]
    fn copy_from_basic() {
        let mut src = NullMask::new(64);
        src.set_null(5, true);
        src.set_null(10, true);

        let mut dst = NullMask::new(64);
        let any_null = dst.copy_from(&src, 0, 0, 64);
        assert!(any_null);
        assert!(dst.is_null(5));
        assert!(dst.is_null(10));
        assert!(!dst.is_null(0));
    }

    #[test]
    fn copy_from_with_offset() {
        let mut src = NullMask::new(64);
        src.set_null(0, true);

        let mut dst = NullMask::new(128);
        dst.copy_from(&src, 0, 64, 1);
        assert!(dst.is_null(64));
        assert!(!dst.is_null(0));
    }

    #[test]
    fn copy_from_no_nulls_source() {
        let src = NullMask::new(64);
        let mut dst = NullMask::all_null(64);
        let any_null = dst.copy_from(&src, 0, 0, 64);
        assert!(!any_null);
        for i in 0..64 {
            assert!(!dst.is_null(i));
        }
    }

    #[test]
    fn resize_grow() {
        let mut mask = NullMask::new(64);
        mask.set_null(0, true);
        mask.resize(256);
        assert!(mask.is_null(0));
        assert!(!mask.is_null(128));
        assert_eq!(mask.num_entries(), 4);
    }

    #[test]
    fn resize_shrink() {
        let mut mask = NullMask::new(256);
        mask.set_null(0, true);
        mask.resize(64);
        assert!(mask.is_null(0));
        assert_eq!(mask.num_entries(), 1);
    }

    #[test]
    fn set_all_non_null() {
        let mut mask = NullMask::all_null(128);
        assert_eq!(mask.count_nulls(), 128);
        mask.set_all_non_null();
        assert!(mask.has_no_nulls_guarantee());
        assert_eq!(mask.count_nulls(), 0);
    }

    #[test]
    fn set_all_null() {
        let mut mask = NullMask::new(128);
        mask.set_all_null();
        assert_eq!(mask.count_nulls(), 128);
    }
}

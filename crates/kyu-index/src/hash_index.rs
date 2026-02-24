//! In-memory hash index with linear hashing and 256 sub-indices.
//!
//! Matches Kuzu's hash index architecture:
//! - 256 sub-indices, selected by top 8 bits of hash
//! - Each sub-index uses linear hashing with split pointer
//! - 256-byte slots with 32-byte header (fingerprints + validity mask + overflow pointer)
//! - Fingerprint-based probing for cache efficiency

use std::hash::{DefaultHasher, Hash, Hasher};

/// Number of sub-indices in the hash index.
const NUM_SUB_INDICES: usize = 256;

/// Capacity of a slot in bytes.
const SLOT_CAPACITY_BYTES: usize = 256;

/// Maximum number of fingerprints per slot header.
const FINGERPRINT_CAPACITY: usize = 20;

/// Invalid overflow slot ID sentinel.
const INVALID_OVERFLOW_SLOT_ID: u64 = u64::MAX;

/// Slot header: 32 bytes containing fingerprints, validity mask, and overflow pointer.
#[derive(Clone, Debug)]
struct SlotHeader {
    fingerprints: [u8; FINGERPRINT_CAPACITY],
    validity_mask: u32,
    next_ovf_slot_id: u64,
}

#[allow(dead_code)]
impl SlotHeader {
    fn new() -> Self {
        Self {
            fingerprints: [0; FINGERPRINT_CAPACITY],
            validity_mask: 0,
            next_ovf_slot_id: INVALID_OVERFLOW_SLOT_ID,
        }
    }

    #[inline]
    fn is_valid(&self, pos: usize) -> bool {
        (self.validity_mask >> pos) & 1 != 0
    }

    #[inline]
    fn set_valid(&mut self, pos: usize, fingerprint: u8) {
        self.validity_mask |= 1u32 << pos;
        self.fingerprints[pos] = fingerprint;
    }

    #[inline]
    fn clear_valid(&mut self, pos: usize) {
        self.validity_mask &= !(1u32 << pos);
    }

    #[inline]
    fn num_valid(&self) -> u32 {
        self.validity_mask.count_ones()
    }

    fn has_overflow(&self) -> bool {
        self.next_ovf_slot_id != INVALID_OVERFLOW_SLOT_ID
    }

    fn reset(&mut self) {
        self.validity_mask = 0;
        self.next_ovf_slot_id = INVALID_OVERFLOW_SLOT_ID;
    }
}

/// A single entry in a slot: key + value (offset).
#[derive(Clone, Debug)]
struct SlotEntry<K: Clone> {
    key: K,
    value: u64,
}

/// Compute the slot capacity for a given key type.
/// Matches Kuzu: min((256 - 32) / sizeof(SlotEntry<K>), 20)
const fn slot_capacity<K>() -> usize {
    let entry_size = std::mem::size_of::<K>() + std::mem::size_of::<u64>();
    let max_by_space = (SLOT_CAPACITY_BYTES - 32) / entry_size;
    if max_by_space < FINGERPRINT_CAPACITY {
        max_by_space
    } else {
        FINGERPRINT_CAPACITY
    }
}

/// A slot containing a header and entries.
#[derive(Clone, Debug)]
struct Slot<K: Clone> {
    header: SlotHeader,
    entries: Vec<SlotEntry<K>>,
}

impl<K: Clone + Default> Slot<K> {
    fn new() -> Self {
        let cap = slot_capacity::<K>();
        Self {
            header: SlotHeader::new(),
            entries: (0..cap)
                .map(|_| SlotEntry {
                    key: K::default(),
                    value: 0,
                })
                .collect(),
        }
    }
}

/// A single sub-index using linear hashing.
struct HashSubIndex<K: Clone + Default + Eq + Hash> {
    p_slots: Vec<Slot<K>>,
    o_slots: Vec<Slot<K>>,
    level: u64,
    level_hash_mask: u64,
    higher_level_hash_mask: u64,
    next_split_slot_id: u64,
    num_entries: u64,
    first_free_overflow_slot_id: u64,
}

impl<K: Clone + Default + Eq + Hash> HashSubIndex<K> {
    fn new() -> Self {
        // Start with level 1 (2 primary slots).
        let initial_slots = 2;
        Self {
            p_slots: (0..initial_slots).map(|_| Slot::new()).collect(),
            o_slots: Vec::new(),
            level: 1,
            level_hash_mask: 1,        // (1 << 1) - 1
            higher_level_hash_mask: 3, // (1 << 2) - 1
            next_split_slot_id: 0,
            num_entries: 0,
            first_free_overflow_slot_id: INVALID_OVERFLOW_SLOT_ID,
        }
    }

    fn num_p_slots(&self) -> u64 {
        self.p_slots.len() as u64
    }

    /// Compute the primary slot ID for a hash value.
    fn get_slot_id(&self, hash: u64) -> u64 {
        let slot_id = hash & self.level_hash_mask;
        if slot_id < self.next_split_slot_id {
            hash & self.higher_level_hash_mask
        } else {
            slot_id
        }
    }

    /// Insert a key-value pair. Returns true if inserted, false if key already exists.
    fn insert(&mut self, key: K, value: u64, fingerprint: u8, hash: u64) -> bool {
        let slot_id = self.get_slot_id(hash);

        // Check if key already exists
        if self
            .lookup_in_chain(slot_id as usize, &key, fingerprint)
            .is_some()
        {
            return false;
        }

        // Try to insert in the primary slot chain
        if self.insert_in_chain(slot_id as usize, key, value, fingerprint) {
            self.num_entries += 1;
            // Check load factor and split if needed
            let capacity = slot_capacity::<K>() as u64;
            if self.num_entries > self.num_p_slots() * capacity * 8 / 10 {
                self.split();
            }
            true
        } else {
            false
        }
    }

    /// Insert into a slot chain (primary + overflow). Returns true on success.
    fn insert_in_chain(&mut self, slot_idx: usize, key: K, value: u64, fingerprint: u8) -> bool {
        let cap = slot_capacity::<K>();

        // Try primary slot first
        let slot = &mut self.p_slots[slot_idx];
        for pos in 0..cap {
            if !slot.header.is_valid(pos) {
                slot.header.set_valid(pos, fingerprint);
                slot.entries[pos] = SlotEntry { key, value };
                return true;
            }
        }

        // Walk overflow chain
        let mut ovf_id = self.p_slots[slot_idx].header.next_ovf_slot_id;
        let mut last_slot_is_primary = true;
        let mut last_slot_idx = slot_idx;

        while ovf_id != INVALID_OVERFLOW_SLOT_ID {
            last_slot_is_primary = false;
            last_slot_idx = ovf_id as usize;
            let o_slot = &mut self.o_slots[ovf_id as usize];
            for pos in 0..cap {
                if !o_slot.header.is_valid(pos) {
                    o_slot.header.set_valid(pos, fingerprint);
                    o_slot.entries[pos] = SlotEntry { key, value };
                    return true;
                }
            }
            ovf_id = o_slot.header.next_ovf_slot_id;
        }

        // Need a new overflow slot
        let new_ovf_id = self.allocate_overflow_slot();
        if last_slot_is_primary {
            self.p_slots[last_slot_idx].header.next_ovf_slot_id = new_ovf_id;
        } else {
            self.o_slots[last_slot_idx].header.next_ovf_slot_id = new_ovf_id;
        }

        let o_slot = &mut self.o_slots[new_ovf_id as usize];
        o_slot.header.set_valid(0, fingerprint);
        o_slot.entries[0] = SlotEntry { key, value };
        true
    }

    /// Allocate a new overflow slot (reuse free list or append).
    fn allocate_overflow_slot(&mut self) -> u64 {
        if self.first_free_overflow_slot_id != INVALID_OVERFLOW_SLOT_ID {
            let id = self.first_free_overflow_slot_id;
            self.first_free_overflow_slot_id = self.o_slots[id as usize].header.next_ovf_slot_id;
            self.o_slots[id as usize].header.reset();
            id
        } else {
            let id = self.o_slots.len() as u64;
            self.o_slots.push(Slot::new());
            id
        }
    }

    /// Look up a key in a slot chain. Returns the value if found.
    fn lookup_in_chain(&self, slot_idx: usize, key: &K, fingerprint: u8) -> Option<u64> {
        let cap = slot_capacity::<K>();

        // Check primary slot
        let slot = &self.p_slots[slot_idx];
        for pos in 0..cap {
            if slot.header.is_valid(pos)
                && slot.header.fingerprints[pos] == fingerprint
                && slot.entries[pos].key == *key
            {
                return Some(slot.entries[pos].value);
            }
        }

        // Walk overflow chain
        let mut ovf_id = slot.header.next_ovf_slot_id;
        while ovf_id != INVALID_OVERFLOW_SLOT_ID {
            let o_slot = &self.o_slots[ovf_id as usize];
            for pos in 0..cap {
                if o_slot.header.is_valid(pos)
                    && o_slot.header.fingerprints[pos] == fingerprint
                    && o_slot.entries[pos].key == *key
                {
                    return Some(o_slot.entries[pos].value);
                }
            }
            ovf_id = o_slot.header.next_ovf_slot_id;
        }

        None
    }

    /// Look up a key. Returns the value if found.
    fn lookup(&self, key: &K, fingerprint: u8, hash: u64) -> Option<u64> {
        let slot_id = self.get_slot_id(hash);
        self.lookup_in_chain(slot_id as usize, key, fingerprint)
    }

    /// Delete a key. Returns true if the key was found and removed.
    fn delete(&mut self, key: &K, fingerprint: u8, hash: u64) -> bool {
        let slot_id = self.get_slot_id(hash);
        let cap = slot_capacity::<K>();

        // Check primary slot
        let slot = &mut self.p_slots[slot_id as usize];
        for pos in 0..cap {
            if slot.header.is_valid(pos)
                && slot.header.fingerprints[pos] == fingerprint
                && slot.entries[pos].key == *key
            {
                slot.header.clear_valid(pos);
                self.num_entries -= 1;
                return true;
            }
        }

        // Walk overflow chain
        let mut ovf_id = slot.header.next_ovf_slot_id;
        while ovf_id != INVALID_OVERFLOW_SLOT_ID {
            let o_slot = &mut self.o_slots[ovf_id as usize];
            for pos in 0..cap {
                if o_slot.header.is_valid(pos)
                    && o_slot.header.fingerprints[pos] == fingerprint
                    && o_slot.entries[pos].key == *key
                {
                    o_slot.header.clear_valid(pos);
                    self.num_entries -= 1;
                    return true;
                }
            }
            ovf_id = o_slot.header.next_ovf_slot_id;
        }

        false
    }

    /// Split the next slot (linear hashing).
    fn split(&mut self) {
        let old_slot_idx = self.next_split_slot_id as usize;

        // Add a new primary slot
        self.p_slots.push(Slot::new());
        let new_slot_idx = self.p_slots.len() - 1;

        // Collect all entries from the old slot chain
        let cap = slot_capacity::<K>();
        let mut entries_to_rehash = Vec::new();

        // From primary slot
        let slot = &self.p_slots[old_slot_idx];
        for pos in 0..cap {
            if slot.header.is_valid(pos) {
                entries_to_rehash.push((
                    slot.entries[pos].key.clone(),
                    slot.entries[pos].value,
                    slot.header.fingerprints[pos],
                ));
            }
        }

        // From overflow chain
        let mut ovf_ids = Vec::new();
        let mut ovf_id = self.p_slots[old_slot_idx].header.next_ovf_slot_id;
        while ovf_id != INVALID_OVERFLOW_SLOT_ID {
            ovf_ids.push(ovf_id);
            let o_slot = &self.o_slots[ovf_id as usize];
            for pos in 0..cap {
                if o_slot.header.is_valid(pos) {
                    entries_to_rehash.push((
                        o_slot.entries[pos].key.clone(),
                        o_slot.entries[pos].value,
                        o_slot.header.fingerprints[pos],
                    ));
                }
            }
            ovf_id = o_slot.header.next_ovf_slot_id;
        }

        // Reset the old primary slot
        self.p_slots[old_slot_idx].header.reset();

        // Return overflow slots to free list
        for ovf_id in ovf_ids {
            self.o_slots[ovf_id as usize].header.reset();
            self.o_slots[ovf_id as usize].header.next_ovf_slot_id =
                self.first_free_overflow_slot_id;
            self.first_free_overflow_slot_id = ovf_id;
        }

        // Increment split pointer
        if self.next_split_slot_id < (1u64 << self.level) - 1 {
            self.next_split_slot_id += 1;
        } else {
            self.level += 1;
            self.next_split_slot_id = 0;
            self.level_hash_mask = (1u64 << self.level) - 1;
            self.higher_level_hash_mask = (1u64 << (self.level + 1)) - 1;
        }

        // Re-insert entries into old or new slot
        for (key, value, fingerprint) in entries_to_rehash {
            let hash = compute_key_hash(&key);
            let sub_hash = sub_index_hash(hash);
            let slot_id = self.get_slot_id(sub_hash);

            debug_assert!(
                slot_id == old_slot_idx as u64 || slot_id == new_slot_idx as u64,
                "split rehash went to unexpected slot {slot_id} (old={old_slot_idx}, new={new_slot_idx})"
            );

            self.insert_in_chain(slot_id as usize, key, value, fingerprint);
        }
    }
}

/// Compute a hash for a key using the standard hasher.
fn compute_key_hash<K: Hash>(key: &K) -> u64 {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    hasher.finish()
}

/// Extract the sub-index ID from a hash (top 8 bits).
#[inline]
fn sub_index_id(hash: u64) -> usize {
    ((hash >> 56) & 0xFF) as usize
}

/// Extract the fingerprint from a hash (bits 48-55).
#[inline]
fn fingerprint(hash: u64) -> u8 {
    ((hash >> 48) & 0xFF) as u8
}

/// Extract the hash used within a sub-index (lower 48 bits).
#[inline]
fn sub_index_hash(hash: u64) -> u64 {
    hash & 0x0000_FFFF_FFFF_FFFF
}

/// In-memory hash index with 256 sub-indices and linear hashing.
///
/// Supports insert, lookup, delete, and contains operations for
/// primary key -> row offset mappings.
pub struct HashIndex<K: Clone + Default + Eq + Hash + 'static> {
    sub_indices: Vec<HashSubIndex<K>>,
}

impl<K: Clone + Default + Eq + Hash + 'static> HashIndex<K> {
    /// Create a new empty hash index.
    pub fn new() -> Self {
        let sub_indices = (0..NUM_SUB_INDICES).map(|_| HashSubIndex::new()).collect();
        Self { sub_indices }
    }

    /// Insert a key-value pair. Returns `true` if inserted, `false` if key exists.
    pub fn insert(&mut self, key: K, value: u64) -> bool {
        let hash = compute_key_hash(&key);
        let idx = sub_index_id(hash);
        let fp = fingerprint(hash);
        let sh = sub_index_hash(hash);
        self.sub_indices[idx].insert(key, value, fp, sh)
    }

    /// Look up a key. Returns the value if found.
    pub fn lookup(&self, key: &K) -> Option<u64> {
        let hash = compute_key_hash(key);
        let idx = sub_index_id(hash);
        let fp = fingerprint(hash);
        let sh = sub_index_hash(hash);
        self.sub_indices[idx].lookup(key, fp, sh)
    }

    /// Delete a key. Returns `true` if the key was found and removed.
    pub fn delete(&mut self, key: &K) -> bool {
        let hash = compute_key_hash(key);
        let idx = sub_index_id(hash);
        let fp = fingerprint(hash);
        let sh = sub_index_hash(hash);
        self.sub_indices[idx].delete(key, fp, sh)
    }

    /// Check if a key exists.
    pub fn contains(&self, key: &K) -> bool {
        self.lookup(key).is_some()
    }

    /// Total number of entries across all sub-indices.
    pub fn num_entries(&self) -> u64 {
        self.sub_indices.iter().map(|s| s.num_entries).sum()
    }
}

impl<K: Clone + Default + Eq + Hash + 'static> Default for HashIndex<K> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slot_header_new() {
        let h = SlotHeader::new();
        assert_eq!(h.validity_mask, 0);
        assert_eq!(h.num_valid(), 0);
        assert!(!h.has_overflow());
    }

    #[test]
    fn slot_header_set_valid() {
        let mut h = SlotHeader::new();
        h.set_valid(0, 42);
        assert!(h.is_valid(0));
        assert_eq!(h.fingerprints[0], 42);
        assert_eq!(h.num_valid(), 1);
    }

    #[test]
    fn slot_header_clear_valid() {
        let mut h = SlotHeader::new();
        h.set_valid(3, 10);
        assert!(h.is_valid(3));
        h.clear_valid(3);
        assert!(!h.is_valid(3));
    }

    #[test]
    fn slot_capacity_i64() {
        // entry = 8 (key) + 8 (value) = 16 bytes
        // (256 - 32) / 16 = 14
        let cap = slot_capacity::<i64>();
        assert_eq!(cap, 14);
    }

    #[test]
    fn slot_capacity_i32() {
        // entry = 4 (key) + 8 (value) = 12 bytes
        // (256 - 32) / 12 = 18
        let cap = slot_capacity::<i32>();
        assert_eq!(cap, 18);
    }

    #[test]
    fn slot_capacity_i128() {
        // entry = 16 (key) + 8 (value) = 24 bytes
        // (256 - 32) / 24 = 9
        let cap = slot_capacity::<i128>();
        assert_eq!(cap, 9);
    }

    #[test]
    fn hash_index_new() {
        let idx = HashIndex::<i64>::new();
        assert_eq!(idx.num_entries(), 0);
    }

    #[test]
    fn hash_index_insert_and_lookup() {
        let mut idx = HashIndex::new();
        assert!(idx.insert(42i64, 100));
        assert_eq!(idx.lookup(&42), Some(100));
        assert_eq!(idx.num_entries(), 1);
    }

    #[test]
    fn hash_index_duplicate_insert() {
        let mut idx = HashIndex::new();
        assert!(idx.insert(42i64, 100));
        assert!(!idx.insert(42i64, 200)); // duplicate
        assert_eq!(idx.lookup(&42), Some(100)); // original value preserved
        assert_eq!(idx.num_entries(), 1);
    }

    #[test]
    fn hash_index_lookup_missing() {
        let idx = HashIndex::<i64>::new();
        assert_eq!(idx.lookup(&42), None);
    }

    #[test]
    fn hash_index_contains() {
        let mut idx = HashIndex::new();
        assert!(!idx.contains(&42i64));
        idx.insert(42, 100);
        assert!(idx.contains(&42));
    }

    #[test]
    fn hash_index_delete() {
        let mut idx = HashIndex::new();
        idx.insert(42i64, 100);
        assert!(idx.delete(&42));
        assert!(!idx.contains(&42));
        assert_eq!(idx.num_entries(), 0);
    }

    #[test]
    fn hash_index_delete_missing() {
        let mut idx = HashIndex::<i64>::new();
        assert!(!idx.delete(&42));
    }

    #[test]
    fn hash_index_many_inserts() {
        let mut idx = HashIndex::new();
        for i in 0..1000i64 {
            assert!(idx.insert(i, i as u64 * 10));
        }
        assert_eq!(idx.num_entries(), 1000);

        for i in 0..1000i64 {
            assert_eq!(idx.lookup(&i), Some(i as u64 * 10));
        }
    }

    #[test]
    fn hash_index_many_inserts_and_deletes() {
        let mut idx = HashIndex::new();
        for i in 0..500i64 {
            idx.insert(i, i as u64);
        }

        // Delete even keys
        for i in (0..500i64).step_by(2) {
            assert!(idx.delete(&i));
        }

        assert_eq!(idx.num_entries(), 250);

        // Odd keys should still exist
        for i in (1..500i64).step_by(2) {
            assert_eq!(idx.lookup(&i), Some(i as u64));
        }

        // Even keys should be gone
        for i in (0..500i64).step_by(2) {
            assert!(!idx.contains(&i));
        }
    }

    #[test]
    fn hash_index_triggers_splits() {
        let mut idx = HashIndex::new();
        // Insert enough to trigger multiple splits
        for i in 0..10_000i64 {
            idx.insert(i, i as u64);
        }
        assert_eq!(idx.num_entries(), 10_000);

        // All lookups should still work after splits
        for i in 0..10_000i64 {
            assert_eq!(idx.lookup(&i), Some(i as u64), "lookup failed for {i}");
        }
    }

    #[test]
    fn hash_index_string_keys() {
        let mut idx = HashIndex::new();
        for i in 0..100 {
            let key = format!("key_{i}");
            idx.insert(key, i);
        }
        assert_eq!(idx.num_entries(), 100);

        for i in 0..100 {
            let key = format!("key_{i}");
            assert_eq!(idx.lookup(&key), Some(i));
        }
    }

    #[test]
    fn hash_index_u32_keys() {
        let mut idx = HashIndex::new();
        idx.insert(u32::MAX, 1);
        idx.insert(0u32, 2);
        idx.insert(42u32, 3);

        assert_eq!(idx.lookup(&u32::MAX), Some(1));
        assert_eq!(idx.lookup(&0u32), Some(2));
        assert_eq!(idx.lookup(&42u32), Some(3));
    }

    #[test]
    fn hash_index_default() {
        let idx: HashIndex<i64> = HashIndex::default();
        assert_eq!(idx.num_entries(), 0);
    }

    #[test]
    fn hash_index_insert_delete_reinsert() {
        let mut idx = HashIndex::new();
        idx.insert(42i64, 100);
        idx.delete(&42);
        assert!(idx.insert(42, 200));
        assert_eq!(idx.lookup(&42), Some(200));
    }

    #[test]
    fn sub_index_hash_extraction() {
        let hash: u64 = 0xAB_CD_1234_5678_9ABC;
        assert_eq!(sub_index_id(hash), 0xAB);
        assert_eq!(fingerprint(hash), 0xCD);
        assert_eq!(sub_index_hash(hash), 0x1234_5678_9ABC);
    }

    #[test]
    fn hash_index_stress_insert_lookup_delete() {
        let mut idx = HashIndex::new();
        let n = 50_000i64;

        // Insert all
        for i in 0..n {
            assert!(idx.insert(i, i as u64 * 3));
        }
        assert_eq!(idx.num_entries(), n as u64);

        // Lookup all
        for i in 0..n {
            assert_eq!(idx.lookup(&i), Some(i as u64 * 3), "lookup failed for {i}");
        }

        // Delete all
        for i in 0..n {
            assert!(idx.delete(&i), "delete failed for {i}");
        }
        assert_eq!(idx.num_entries(), 0);

        // All gone
        for i in 0..n {
            assert!(!idx.contains(&i));
        }
    }
}

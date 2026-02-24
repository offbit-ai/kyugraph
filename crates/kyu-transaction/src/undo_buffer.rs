//! Arena-style undo buffer for transaction rollback.
//!
//! Records are written as raw bytes into a pre-allocated contiguous buffer.
//! No per-record heap allocation, cache-friendly sequential writes.
//! Buffer starts at 4KB and doubles on overflow (amortized O(1) append).

/// Initial capacity for the undo buffer (4KB).
const UNDO_BUFFER_INIT_CAPACITY: usize = 4096;

/// Size of one undo record in bytes:
/// [type: u8][node_group_idx: u64][start_row: u64][num_rows: u64] = 25 bytes.
const UNDO_RECORD_SIZE: usize = 1 + 8 + 8 + 8;

/// Type tag for undo records.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UndoRecordType {
    InsertInfo = 0,
    DeleteInfo = 1,
}

impl UndoRecordType {
    fn from_byte(b: u8) -> Option<Self> {
        match b {
            0 => Some(Self::InsertInfo),
            1 => Some(Self::DeleteInfo),
            _ => None,
        }
    }
}

/// A decoded undo record.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UndoRecord {
    pub record_type: UndoRecordType,
    pub node_group_idx: u64,
    pub start_row: u64,
    pub num_rows: u64,
}

/// Arena-style byte buffer for undo records.
///
/// Records are appended contiguously with no per-record allocation.
/// Forward iteration reads linearly; reverse iteration collects offsets first.
pub struct UndoBuffer {
    buffer: Vec<u8>,
    record_count: u32,
}

impl Default for UndoBuffer {
    fn default() -> Self {
        Self::new()
    }
}

impl UndoBuffer {
    pub fn new() -> Self {
        Self {
            buffer: Vec::with_capacity(UNDO_BUFFER_INIT_CAPACITY),
            record_count: 0,
        }
    }

    /// Write an insertion undo record.
    pub fn create_insert_info(&mut self, node_group_idx: u64, start_row: u64, num_rows: u64) {
        self.write_record(
            UndoRecordType::InsertInfo,
            node_group_idx,
            start_row,
            num_rows,
        );
    }

    /// Write a deletion undo record.
    pub fn create_delete_info(&mut self, node_group_idx: u64, start_row: u64, num_rows: u64) {
        self.write_record(
            UndoRecordType::DeleteInfo,
            node_group_idx,
            start_row,
            num_rows,
        );
    }

    /// Write a record into the byte buffer.
    fn write_record(
        &mut self,
        record_type: UndoRecordType,
        node_group_idx: u64,
        start_row: u64,
        num_rows: u64,
    ) {
        self.buffer.push(record_type as u8);
        self.buffer.extend_from_slice(&node_group_idx.to_ne_bytes());
        self.buffer.extend_from_slice(&start_row.to_ne_bytes());
        self.buffer.extend_from_slice(&num_rows.to_ne_bytes());
        self.record_count += 1;
    }

    /// Read a record from a buffer position.
    fn read_record(buf: &[u8], offset: usize) -> UndoRecord {
        let record_type = UndoRecordType::from_byte(buf[offset]).expect("invalid undo record type");
        let node_group_idx = u64::from_ne_bytes(buf[offset + 1..offset + 9].try_into().unwrap());
        let start_row = u64::from_ne_bytes(buf[offset + 9..offset + 17].try_into().unwrap());
        let num_rows = u64::from_ne_bytes(buf[offset + 17..offset + 25].try_into().unwrap());
        UndoRecord {
            record_type,
            node_group_idx,
            start_row,
            num_rows,
        }
    }

    /// Forward iteration: call `callback` for each record in append order.
    pub fn commit<F>(&self, commit_ts: u64, mut callback: F)
    where
        F: FnMut(UndoRecord, u64),
    {
        let mut offset = 0;
        while offset + UNDO_RECORD_SIZE <= self.buffer.len() {
            let record = Self::read_record(&self.buffer, offset);
            callback(record, commit_ts);
            offset += UNDO_RECORD_SIZE;
        }
    }

    /// Reverse iteration: call `callback` for each record in reverse order.
    pub fn rollback<F>(&self, mut callback: F)
    where
        F: FnMut(UndoRecord),
    {
        // Collect offsets for reverse iteration.
        let mut offsets = Vec::with_capacity(self.record_count as usize);
        let mut offset = 0;
        while offset + UNDO_RECORD_SIZE <= self.buffer.len() {
            offsets.push(offset);
            offset += UNDO_RECORD_SIZE;
        }
        for &off in offsets.iter().rev() {
            let record = Self::read_record(&self.buffer, off);
            callback(record);
        }
    }

    /// Iterate all records in forward order.
    pub fn iter(&self) -> UndoBufferIter<'_> {
        UndoBufferIter {
            buffer: &self.buffer,
            offset: 0,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.record_count == 0
    }

    pub fn record_count(&self) -> u32 {
        self.record_count
    }

    /// Reset buffer position without deallocating.
    pub fn clear(&mut self) {
        self.buffer.clear();
        self.record_count = 0;
    }

    /// Total bytes used in the buffer.
    pub fn byte_size(&self) -> usize {
        self.buffer.len()
    }
}

/// Forward iterator over undo records.
pub struct UndoBufferIter<'a> {
    buffer: &'a [u8],
    offset: usize,
}

impl<'a> Iterator for UndoBufferIter<'a> {
    type Item = UndoRecord;

    fn next(&mut self) -> Option<Self::Item> {
        if self.offset + UNDO_RECORD_SIZE > self.buffer.len() {
            return None;
        }
        let record = UndoBuffer::read_record(self.buffer, self.offset);
        self.offset += UNDO_RECORD_SIZE;
        Some(record)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_undo_buffer() {
        let buf = UndoBuffer::new();
        assert!(buf.is_empty());
        assert_eq!(buf.record_count(), 0);
        assert_eq!(buf.byte_size(), 0);
    }

    #[test]
    fn create_insert_info() {
        let mut buf = UndoBuffer::new();
        buf.create_insert_info(1, 0, 100);

        assert_eq!(buf.record_count(), 1);
        assert_eq!(buf.byte_size(), UNDO_RECORD_SIZE);
    }

    #[test]
    fn create_delete_info() {
        let mut buf = UndoBuffer::new();
        buf.create_delete_info(2, 50, 10);

        assert_eq!(buf.record_count(), 1);
        assert_eq!(buf.byte_size(), UNDO_RECORD_SIZE);
    }

    #[test]
    fn multiple_records() {
        let mut buf = UndoBuffer::new();
        buf.create_insert_info(0, 0, 100);
        buf.create_insert_info(0, 100, 50);
        buf.create_delete_info(1, 0, 10);

        assert_eq!(buf.record_count(), 3);
        assert_eq!(buf.byte_size(), 3 * UNDO_RECORD_SIZE);
    }

    #[test]
    fn commit_forward_iteration() {
        let mut buf = UndoBuffer::new();
        buf.create_insert_info(0, 0, 100);
        buf.create_delete_info(1, 50, 10);

        let mut records = Vec::new();
        buf.commit(42, |record, ts| {
            records.push((record, ts));
        });

        assert_eq!(records.len(), 2);
        assert_eq!(records[0].0.record_type, UndoRecordType::InsertInfo);
        assert_eq!(records[0].0.node_group_idx, 0);
        assert_eq!(records[0].0.start_row, 0);
        assert_eq!(records[0].0.num_rows, 100);
        assert_eq!(records[0].1, 42);

        assert_eq!(records[1].0.record_type, UndoRecordType::DeleteInfo);
        assert_eq!(records[1].0.node_group_idx, 1);
    }

    #[test]
    fn rollback_reverse_iteration() {
        let mut buf = UndoBuffer::new();
        buf.create_insert_info(0, 0, 100);
        buf.create_insert_info(1, 0, 50);
        buf.create_delete_info(2, 0, 10);

        let mut types = Vec::new();
        buf.rollback(|record| {
            types.push(record.record_type);
        });

        // Reverse order.
        assert_eq!(
            types,
            vec![
                UndoRecordType::DeleteInfo,
                UndoRecordType::InsertInfo,
                UndoRecordType::InsertInfo,
            ]
        );
    }

    #[test]
    fn iter_records() {
        let mut buf = UndoBuffer::new();
        buf.create_insert_info(0, 0, 10);
        buf.create_delete_info(1, 5, 5);

        let records: Vec<_> = buf.iter().collect();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].record_type, UndoRecordType::InsertInfo);
        assert_eq!(records[1].record_type, UndoRecordType::DeleteInfo);
    }

    #[test]
    fn clear_resets() {
        let mut buf = UndoBuffer::new();
        buf.create_insert_info(0, 0, 100);
        assert!(!buf.is_empty());

        buf.clear();
        assert!(buf.is_empty());
        assert_eq!(buf.record_count(), 0);
        assert_eq!(buf.byte_size(), 0);
    }

    #[test]
    fn empty_commit() {
        let buf = UndoBuffer::new();
        let mut count = 0;
        buf.commit(0, |_, _| count += 1);
        assert_eq!(count, 0);
    }

    #[test]
    fn empty_rollback() {
        let buf = UndoBuffer::new();
        let mut count = 0;
        buf.rollback(|_| count += 1);
        assert_eq!(count, 0);
    }

    #[test]
    fn record_data_roundtrip() {
        let mut buf = UndoBuffer::new();
        buf.create_insert_info(u64::MAX - 1, 12345, 67890);

        let records: Vec<_> = buf.iter().collect();
        assert_eq!(records[0].node_group_idx, u64::MAX - 1);
        assert_eq!(records[0].start_row, 12345);
        assert_eq!(records[0].num_rows, 67890);
    }

    #[test]
    fn undo_record_type_from_byte() {
        assert_eq!(
            UndoRecordType::from_byte(0),
            Some(UndoRecordType::InsertInfo)
        );
        assert_eq!(
            UndoRecordType::from_byte(1),
            Some(UndoRecordType::DeleteInfo)
        );
        assert_eq!(UndoRecordType::from_byte(2), None);
        assert_eq!(UndoRecordType::from_byte(255), None);
    }

    #[test]
    fn record_size_constant() {
        assert_eq!(UNDO_RECORD_SIZE, 25);
    }

    #[test]
    fn many_records_grows_buffer() {
        let mut buf = UndoBuffer::new();
        for i in 0..1000 {
            buf.create_insert_info(i, i * 10, 10);
        }
        assert_eq!(buf.record_count(), 1000);
        assert_eq!(buf.byte_size(), 1000 * UNDO_RECORD_SIZE);

        // Verify last record.
        let records: Vec<_> = buf.iter().collect();
        assert_eq!(records[999].node_group_idx, 999);
    }
}

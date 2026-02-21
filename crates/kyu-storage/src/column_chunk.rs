//! Column chunk: in-memory columnar buffer with typed access.
//!
//! `ColumnChunkData` stores fixed-size values in a flat byte buffer with a `NullMask`.
//! `BoolChunkData` is a 1-bit-per-value specialization.
//! `ColumnChunk` is the type-erased enum wrapper.

use kyu_common::InternalId;
use kyu_types::{Interval, LogicalType, PhysicalType};

use crate::null_mask::NullMask;
use crate::storage_types::{
    ColumnChunkMetadata, ColumnChunkStats, ResidencyState, StorageValue,
};

/// Trait for fixed-size types that can be stored in a column chunk.
/// Uses safe `from_ne_bytes`/`to_ne_bytes` patterns — no `unsafe`.
pub trait FixedSizeValue: Copy + Sized {
    const SIZE: usize = std::mem::size_of::<Self>();
    fn to_bytes(&self) -> Vec<u8>;
    fn from_bytes(bytes: &[u8]) -> Self;
    fn to_storage_value(&self) -> Option<StorageValue>;
}

macro_rules! impl_fixed_size_int {
    ($ty:ty, $variant:ident) => {
        impl FixedSizeValue for $ty {
            fn to_bytes(&self) -> Vec<u8> {
                self.to_ne_bytes().to_vec()
            }
            fn from_bytes(bytes: &[u8]) -> Self {
                let mut arr = [0u8; std::mem::size_of::<$ty>()];
                arr.copy_from_slice(&bytes[..std::mem::size_of::<$ty>()]);
                <$ty>::from_ne_bytes(arr)
            }
            fn to_storage_value(&self) -> Option<StorageValue> {
                Some(StorageValue::$variant(*self as _))
            }
        }
    };
}

impl_fixed_size_int!(i8, SignedInt);
impl_fixed_size_int!(i16, SignedInt);
impl_fixed_size_int!(i32, SignedInt);
impl_fixed_size_int!(i64, SignedInt);
impl_fixed_size_int!(u8, UnsignedInt);
impl_fixed_size_int!(u16, UnsignedInt);
impl_fixed_size_int!(u32, UnsignedInt);
impl_fixed_size_int!(u64, UnsignedInt);

impl FixedSizeValue for i128 {
    fn to_bytes(&self) -> Vec<u8> {
        self.to_ne_bytes().to_vec()
    }
    fn from_bytes(bytes: &[u8]) -> Self {
        let mut arr = [0u8; 16];
        arr.copy_from_slice(&bytes[..16]);
        i128::from_ne_bytes(arr)
    }
    fn to_storage_value(&self) -> Option<StorageValue> {
        Some(StorageValue::SignedInt128(*self))
    }
}

impl FixedSizeValue for f32 {
    fn to_bytes(&self) -> Vec<u8> {
        self.to_ne_bytes().to_vec()
    }
    fn from_bytes(bytes: &[u8]) -> Self {
        let mut arr = [0u8; 4];
        arr.copy_from_slice(&bytes[..4]);
        f32::from_ne_bytes(arr)
    }
    fn to_storage_value(&self) -> Option<StorageValue> {
        Some(StorageValue::Float(*self as f64))
    }
}

impl FixedSizeValue for f64 {
    fn to_bytes(&self) -> Vec<u8> {
        self.to_ne_bytes().to_vec()
    }
    fn from_bytes(bytes: &[u8]) -> Self {
        let mut arr = [0u8; 8];
        arr.copy_from_slice(&bytes[..8]);
        f64::from_ne_bytes(arr)
    }
    fn to_storage_value(&self) -> Option<StorageValue> {
        Some(StorageValue::Float(*self))
    }
}

impl FixedSizeValue for InternalId {
    fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(16);
        buf.extend_from_slice(&self.table_id.to_ne_bytes());
        buf.extend_from_slice(&self.offset.to_ne_bytes());
        buf
    }
    fn from_bytes(bytes: &[u8]) -> Self {
        let mut tid = [0u8; 8];
        let mut off = [0u8; 8];
        tid.copy_from_slice(&bytes[..8]);
        off.copy_from_slice(&bytes[8..16]);
        InternalId::new(u64::from_ne_bytes(tid), u64::from_ne_bytes(off))
    }
    fn to_storage_value(&self) -> Option<StorageValue> {
        None
    }
}

impl FixedSizeValue for Interval {
    fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(16);
        buf.extend_from_slice(&self.months.to_ne_bytes());
        buf.extend_from_slice(&self.days.to_ne_bytes());
        buf.extend_from_slice(&self.micros.to_ne_bytes());
        buf
    }
    fn from_bytes(bytes: &[u8]) -> Self {
        let mut m = [0u8; 4];
        let mut d = [0u8; 4];
        let mut u = [0u8; 8];
        m.copy_from_slice(&bytes[..4]);
        d.copy_from_slice(&bytes[4..8]);
        u.copy_from_slice(&bytes[8..16]);
        Interval::new(
            i32::from_ne_bytes(m),
            i32::from_ne_bytes(d),
            i64::from_ne_bytes(u),
        )
    }
    fn to_storage_value(&self) -> Option<StorageValue> {
        None
    }
}

/// In-memory columnar buffer for fixed-size values.
pub struct ColumnChunkData {
    data_type: LogicalType,
    num_bytes_per_value: usize,
    buffer: Vec<u8>,
    null_data: NullMask,
    capacity: u64,
    num_values: u64,
    stats: ColumnChunkStats,
    metadata: ColumnChunkMetadata,
    residency_state: ResidencyState,
}

impl ColumnChunkData {
    /// Create a new column chunk for the given type and capacity.
    pub fn new(data_type: LogicalType, capacity: u64) -> Self {
        let physical = data_type.physical_type();
        let num_bytes_per_value = physical.fixed_size().unwrap_or(0);
        Self {
            data_type,
            num_bytes_per_value,
            buffer: vec![0u8; num_bytes_per_value * capacity as usize],
            null_data: NullMask::new(capacity),
            capacity,
            num_values: 0,
            stats: ColumnChunkStats::default(),
            metadata: ColumnChunkMetadata::default(),
            residency_state: ResidencyState::InMemory,
        }
    }

    pub fn data_type(&self) -> &LogicalType {
        &self.data_type
    }

    pub fn capacity(&self) -> u64 {
        self.capacity
    }

    pub fn num_values(&self) -> u64 {
        self.num_values
    }

    pub fn is_null(&self, pos: u64) -> bool {
        self.null_data.is_null(pos)
    }

    pub fn set_null(&mut self, pos: u64, is_null: bool) {
        self.null_data.set_null(pos, is_null);
    }

    pub fn null_mask(&self) -> &NullMask {
        &self.null_data
    }

    pub fn null_mask_mut(&mut self) -> &mut NullMask {
        &mut self.null_data
    }

    pub fn stats(&self) -> &ColumnChunkStats {
        &self.stats
    }

    pub fn metadata(&self) -> &ColumnChunkMetadata {
        &self.metadata
    }

    pub fn set_metadata(&mut self, metadata: ColumnChunkMetadata) {
        self.metadata = metadata;
    }

    pub fn residency_state(&self) -> ResidencyState {
        self.residency_state
    }

    pub fn set_residency_state(&mut self, state: ResidencyState) {
        self.residency_state = state;
    }

    /// Get a typed value at the given position.
    pub fn get_value<T: FixedSizeValue>(&self, pos: u64) -> T {
        debug_assert!(pos < self.num_values);
        let offset = pos as usize * self.num_bytes_per_value;
        T::from_bytes(&self.buffer[offset..offset + self.num_bytes_per_value])
    }

    /// Set a typed value at the given position.
    pub fn set_value<T: FixedSizeValue>(&mut self, pos: u64, val: T) {
        debug_assert!(pos < self.capacity);
        let offset = pos as usize * self.num_bytes_per_value;
        let bytes = val.to_bytes();
        self.buffer[offset..offset + self.num_bytes_per_value].copy_from_slice(&bytes);
        self.null_data.set_null(pos, false);
        if let Some(sv) = val.to_storage_value() {
            self.stats.update(sv);
        }
    }

    /// Append a typed value, incrementing num_values.
    pub fn append_value<T: FixedSizeValue>(&mut self, val: T) {
        let pos = self.num_values;
        debug_assert!(pos < self.capacity);
        self.set_value(pos, val);
        self.num_values += 1;
    }

    /// Append a null value, incrementing num_values.
    pub fn append_null(&mut self) {
        let pos = self.num_values;
        debug_assert!(pos < self.capacity);
        self.null_data.set_null(pos, true);
        self.num_values += 1;
    }

    /// Get raw bytes at a position.
    pub fn get_raw(&self, pos: u64) -> &[u8] {
        let offset = pos as usize * self.num_bytes_per_value;
        &self.buffer[offset..offset + self.num_bytes_per_value]
    }

    /// Set raw bytes at a position.
    pub fn set_raw(&mut self, pos: u64, bytes: &[u8]) {
        debug_assert!(pos < self.capacity);
        debug_assert_eq!(bytes.len(), self.num_bytes_per_value);
        let offset = pos as usize * self.num_bytes_per_value;
        self.buffer[offset..offset + self.num_bytes_per_value].copy_from_slice(bytes);
        self.null_data.set_null(pos, false);
    }

    /// Scan a range of typed values, returning `None` for nulls.
    pub fn scan_range<T: FixedSizeValue>(&self, start: u64, count: u64) -> Vec<Option<T>> {
        let mut result = Vec::with_capacity(count as usize);
        for i in start..start + count {
            if self.null_data.is_null(i) {
                result.push(None);
            } else {
                result.push(Some(self.get_value::<T>(i)));
            }
        }
        result
    }

    /// Get the byte buffer backing this chunk (read-only).
    pub fn buffer(&self) -> &[u8] {
        &self.buffer
    }

    /// Number of bytes per value.
    pub fn num_bytes_per_value(&self) -> usize {
        self.num_bytes_per_value
    }

    /// Set num_values directly (used when bulk-loading).
    pub fn set_num_values(&mut self, n: u64) {
        self.num_values = n;
    }
}

/// Packed 1-bit-per-value boolean column chunk.
pub struct BoolChunkData {
    data_type: LogicalType,
    /// Packed boolean values (bit=1 means true).
    values: NullMask,
    null_data: NullMask,
    capacity: u64,
    num_values: u64,
    residency_state: ResidencyState,
}

impl BoolChunkData {
    pub fn new(capacity: u64) -> Self {
        Self {
            data_type: LogicalType::Bool,
            values: NullMask::new(capacity),
            null_data: NullMask::new(capacity),
            capacity,
            num_values: 0,
            residency_state: ResidencyState::InMemory,
        }
    }

    pub fn data_type(&self) -> &LogicalType {
        &self.data_type
    }

    pub fn capacity(&self) -> u64 {
        self.capacity
    }

    pub fn num_values(&self) -> u64 {
        self.num_values
    }

    pub fn is_null(&self, pos: u64) -> bool {
        self.null_data.is_null(pos)
    }

    pub fn set_null(&mut self, pos: u64, is_null: bool) {
        self.null_data.set_null(pos, is_null);
    }

    pub fn null_mask(&self) -> &NullMask {
        &self.null_data
    }

    pub fn residency_state(&self) -> ResidencyState {
        self.residency_state
    }

    /// Get a boolean value at the given position.
    /// Bit=1 in the values mask means `true`.
    pub fn get_bool(&self, pos: u64) -> bool {
        self.values.is_null(pos)
    }

    /// Set a boolean value at the given position.
    pub fn set_bool(&mut self, pos: u64, val: bool) {
        self.values.set_null(pos, val);
        self.null_data.set_null(pos, false);
    }

    /// Append a boolean value.
    pub fn append_bool(&mut self, val: bool) {
        let pos = self.num_values;
        debug_assert!(pos < self.capacity);
        self.set_bool(pos, val);
        self.num_values += 1;
    }

    /// Append a null value.
    pub fn append_null(&mut self) {
        let pos = self.num_values;
        debug_assert!(pos < self.capacity);
        self.null_data.set_null(pos, true);
        self.num_values += 1;
    }

    /// Scan a range of boolean values, returning `None` for nulls.
    pub fn scan_range(&self, start: u64, count: u64) -> Vec<Option<bool>> {
        let mut result = Vec::with_capacity(count as usize);
        for i in start..start + count {
            if self.null_data.is_null(i) {
                result.push(None);
            } else {
                result.push(Some(self.get_bool(i)));
            }
        }
        result
    }

    /// Set num_values directly (used when bulk-loading).
    pub fn set_num_values(&mut self, n: u64) {
        self.num_values = n;
    }
}

/// In-memory string column chunk: stores `Option<SmolStr>` per row.
///
/// Variable-length strings can't be stored in the fixed-size byte buffer
/// of `ColumnChunkData`. This variant stores strings directly as `SmolStr`
/// values, matching kyu-types' string representation.
pub struct StringChunkData {
    data: Vec<Option<smol_str::SmolStr>>,
    capacity: u64,
    num_values: u64,
    residency_state: ResidencyState,
}

impl StringChunkData {
    pub fn new(capacity: u64) -> Self {
        let mut data = Vec::with_capacity(capacity as usize);
        data.resize(capacity as usize, None);
        Self {
            data,
            capacity,
            num_values: 0,
            residency_state: ResidencyState::InMemory,
        }
    }

    pub fn capacity(&self) -> u64 {
        self.capacity
    }

    pub fn num_values(&self) -> u64 {
        self.num_values
    }

    pub fn is_null(&self, pos: u64) -> bool {
        self.data[pos as usize].is_none()
    }

    pub fn set_null(&mut self, pos: u64, is_null: bool) {
        if is_null {
            self.data[pos as usize] = None;
        }
    }

    pub fn residency_state(&self) -> ResidencyState {
        self.residency_state
    }

    /// Append a string value.
    pub fn append_string(&mut self, val: smol_str::SmolStr) {
        let pos = self.num_values as usize;
        debug_assert!(self.num_values < self.capacity);
        self.data[pos] = Some(val);
        self.num_values += 1;
    }

    /// Append a null value.
    pub fn append_null(&mut self) {
        debug_assert!(self.num_values < self.capacity);
        // data[pos] is already None from initialization
        self.num_values += 1;
    }

    /// Set a string value at a specific position.
    pub fn set_string(&mut self, pos: u64, val: smol_str::SmolStr) {
        self.data[pos as usize] = Some(val);
    }

    /// Get a string reference at the given position. Returns `None` for nulls.
    pub fn get(&self, pos: u64) -> Option<&smol_str::SmolStr> {
        self.data[pos as usize].as_ref()
    }

    /// Scan a range of string values.
    pub fn scan_range(&self, start: u64, count: u64) -> &[Option<smol_str::SmolStr>] {
        &self.data[start as usize..(start + count) as usize]
    }

    /// Set num_values directly (used when bulk-loading).
    pub fn set_num_values(&mut self, n: u64) {
        self.num_values = n;
    }
}

/// Type-erased column chunk wrapper.
pub enum ColumnChunk {
    Fixed(ColumnChunkData),
    Bool(BoolChunkData),
    String(StringChunkData),
}

impl ColumnChunk {
    /// Create the appropriate column chunk for a logical type.
    pub fn new(data_type: LogicalType, capacity: u64) -> Self {
        let physical = data_type.physical_type();
        if physical == PhysicalType::Bool {
            ColumnChunk::Bool(BoolChunkData::new(capacity))
        } else if physical == PhysicalType::String {
            ColumnChunk::String(StringChunkData::new(capacity))
        } else {
            ColumnChunk::Fixed(ColumnChunkData::new(data_type, capacity))
        }
    }

    pub fn data_type(&self) -> &LogicalType {
        match self {
            Self::Fixed(c) => c.data_type(),
            Self::Bool(c) => c.data_type(),
            Self::String(_) => &LogicalType::String,
        }
    }

    pub fn num_values(&self) -> u64 {
        match self {
            Self::Fixed(c) => c.num_values(),
            Self::Bool(c) => c.num_values(),
            Self::String(c) => c.num_values(),
        }
    }

    pub fn capacity(&self) -> u64 {
        match self {
            Self::Fixed(c) => c.capacity(),
            Self::Bool(c) => c.capacity(),
            Self::String(c) => c.capacity(),
        }
    }

    pub fn is_null(&self, pos: u64) -> bool {
        match self {
            Self::Fixed(c) => c.is_null(pos),
            Self::Bool(c) => c.is_null(pos),
            Self::String(c) => c.is_null(pos),
        }
    }

    pub fn set_null(&mut self, pos: u64, is_null: bool) {
        match self {
            Self::Fixed(c) => c.set_null(pos, is_null),
            Self::Bool(c) => c.set_null(pos, is_null),
            Self::String(c) => c.set_null(pos, is_null),
        }
    }

    pub fn append_null(&mut self) {
        match self {
            Self::Fixed(c) => c.append_null(),
            Self::Bool(c) => c.append_null(),
            Self::String(c) => c.append_null(),
        }
    }

    pub fn residency_state(&self) -> ResidencyState {
        match self {
            Self::Fixed(c) => c.residency_state(),
            Self::Bool(c) => c.residency_state(),
            Self::String(c) => c.residency_state(),
        }
    }

    /// Set raw bytes at a position. Only valid for Fixed chunks.
    pub fn set_raw(&mut self, pos: u64, bytes: &[u8]) {
        match self {
            Self::Fixed(c) => c.set_raw(pos, bytes),
            Self::Bool(_) => panic!("set_raw not supported for BoolChunkData"),
            Self::String(_) => panic!("set_raw not supported for StringChunkData"),
        }
    }

    /// Set num_values directly (used when bulk-loading).
    pub fn set_num_values(&mut self, n: u64) {
        match self {
            Self::Fixed(c) => c.set_num_values(n),
            Self::Bool(c) => c.set_num_values(n),
            Self::String(c) => c.set_num_values(n),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_size_value_i64_roundtrip() {
        let val: i64 = -42;
        let bytes = val.to_bytes();
        assert_eq!(i64::from_bytes(&bytes), val);
    }

    #[test]
    fn fixed_size_value_f64_roundtrip() {
        let val: f64 = 3.14159;
        let bytes = val.to_bytes();
        assert_eq!(f64::from_bytes(&bytes), val);
    }

    #[test]
    fn fixed_size_value_internal_id_roundtrip() {
        let val = InternalId::new(3, 42);
        let bytes = val.to_bytes();
        assert_eq!(InternalId::from_bytes(&bytes), val);
    }

    #[test]
    fn fixed_size_value_interval_roundtrip() {
        let val = Interval::new(14, 5, 3_723_000_000);
        let bytes = val.to_bytes();
        assert_eq!(Interval::from_bytes(&bytes), val);
    }

    #[test]
    fn fixed_size_value_i128_roundtrip() {
        let val: i128 = -170_141_183_460_469_231_731_687_303_715_884_105_728;
        let bytes = val.to_bytes();
        assert_eq!(i128::from_bytes(&bytes), val);
    }

    #[test]
    fn column_chunk_data_new() {
        let chunk = ColumnChunkData::new(LogicalType::Int64, 100);
        assert_eq!(chunk.num_values(), 0);
        assert_eq!(chunk.capacity(), 100);
        assert_eq!(chunk.num_bytes_per_value(), 8);
        assert_eq!(chunk.residency_state(), ResidencyState::InMemory);
    }

    #[test]
    fn column_chunk_data_append_and_get() {
        let mut chunk = ColumnChunkData::new(LogicalType::Int64, 10);
        chunk.append_value(42i64);
        chunk.append_value(-7i64);
        chunk.append_value(100i64);

        assert_eq!(chunk.num_values(), 3);
        assert_eq!(chunk.get_value::<i64>(0), 42);
        assert_eq!(chunk.get_value::<i64>(1), -7);
        assert_eq!(chunk.get_value::<i64>(2), 100);
    }

    #[test]
    fn column_chunk_data_set_value() {
        let mut chunk = ColumnChunkData::new(LogicalType::Int32, 10);
        chunk.set_num_values(3);
        chunk.set_value(0, 10i32);
        chunk.set_value(1, 20i32);
        chunk.set_value(2, 30i32);

        assert_eq!(chunk.get_value::<i32>(0), 10);
        assert_eq!(chunk.get_value::<i32>(1), 20);
        assert_eq!(chunk.get_value::<i32>(2), 30);
    }

    #[test]
    fn column_chunk_data_nulls() {
        let mut chunk = ColumnChunkData::new(LogicalType::Int64, 10);
        chunk.append_value(42i64);
        chunk.append_null();
        chunk.append_value(100i64);

        assert!(!chunk.is_null(0));
        assert!(chunk.is_null(1));
        assert!(!chunk.is_null(2));
    }

    #[test]
    fn column_chunk_data_scan_range() {
        let mut chunk = ColumnChunkData::new(LogicalType::Int64, 10);
        chunk.append_value(1i64);
        chunk.append_null();
        chunk.append_value(3i64);

        let result = chunk.scan_range::<i64>(0, 3);
        assert_eq!(result, vec![Some(1), None, Some(3)]);
    }

    #[test]
    fn column_chunk_data_raw_access() {
        let mut chunk = ColumnChunkData::new(LogicalType::Int32, 10);
        chunk.set_num_values(1);
        let val: i32 = 42;
        chunk.set_raw(0, &val.to_ne_bytes());
        assert_eq!(chunk.get_value::<i32>(0), 42);

        let raw = chunk.get_raw(0);
        assert_eq!(raw, &val.to_ne_bytes());
    }

    #[test]
    fn column_chunk_data_stats_tracking() {
        let mut chunk = ColumnChunkData::new(LogicalType::Int64, 10);
        chunk.append_value(10i64);
        chunk.append_value(-5i64);
        chunk.append_value(20i64);

        assert_eq!(chunk.stats().min, Some(StorageValue::SignedInt(-5)));
        assert_eq!(chunk.stats().max, Some(StorageValue::SignedInt(20)));
    }

    #[test]
    fn column_chunk_data_f64_stats() {
        let mut chunk = ColumnChunkData::new(LogicalType::Double, 10);
        chunk.append_value(1.5f64);
        chunk.append_value(3.7f64);
        chunk.append_value(0.1f64);

        assert_eq!(chunk.stats().min, Some(StorageValue::Float(0.1)));
        assert_eq!(chunk.stats().max, Some(StorageValue::Float(3.7)));
    }

    #[test]
    fn column_chunk_data_internal_id() {
        let mut chunk = ColumnChunkData::new(LogicalType::InternalId, 10);
        let id = InternalId::new(3, 42);
        chunk.append_value(id);
        assert_eq!(chunk.get_value::<InternalId>(0), id);
    }

    #[test]
    fn bool_chunk_data_new() {
        let chunk = BoolChunkData::new(100);
        assert_eq!(chunk.num_values(), 0);
        assert_eq!(chunk.capacity(), 100);
        assert_eq!(*chunk.data_type(), LogicalType::Bool);
    }

    #[test]
    fn bool_chunk_data_append_and_get() {
        let mut chunk = BoolChunkData::new(10);
        chunk.append_bool(true);
        chunk.append_bool(false);
        chunk.append_bool(true);

        assert_eq!(chunk.num_values(), 3);
        assert!(chunk.get_bool(0));
        assert!(!chunk.get_bool(1));
        assert!(chunk.get_bool(2));
    }

    #[test]
    fn bool_chunk_data_nulls() {
        let mut chunk = BoolChunkData::new(10);
        chunk.append_bool(true);
        chunk.append_null();
        chunk.append_bool(false);

        assert!(!chunk.is_null(0));
        assert!(chunk.is_null(1));
        assert!(!chunk.is_null(2));
    }

    #[test]
    fn bool_chunk_data_scan_range() {
        let mut chunk = BoolChunkData::new(10);
        chunk.append_bool(true);
        chunk.append_null();
        chunk.append_bool(false);

        let result = chunk.scan_range(0, 3);
        assert_eq!(result, vec![Some(true), None, Some(false)]);
    }

    #[test]
    fn column_chunk_enum_fixed() {
        let mut chunk = ColumnChunk::new(LogicalType::Int64, 10);
        assert_eq!(*chunk.data_type(), LogicalType::Int64);
        assert_eq!(chunk.capacity(), 10);

        match &mut chunk {
            ColumnChunk::Fixed(c) => c.append_value(42i64),
            _ => panic!("expected Fixed"),
        }
        assert_eq!(chunk.num_values(), 1);
    }

    #[test]
    fn column_chunk_enum_bool() {
        let mut chunk = ColumnChunk::new(LogicalType::Bool, 10);
        assert_eq!(*chunk.data_type(), LogicalType::Bool);

        match &mut chunk {
            ColumnChunk::Bool(c) => c.append_bool(true),
            _ => panic!("expected Bool"),
        }
        assert_eq!(chunk.num_values(), 1);
    }

    #[test]
    fn column_chunk_enum_null() {
        let mut chunk = ColumnChunk::new(LogicalType::Int32, 10);
        chunk.append_null();
        assert_eq!(chunk.num_values(), 1);
        assert!(chunk.is_null(0));
    }

    #[test]
    fn column_chunk_set_raw() {
        let mut chunk = ColumnChunk::new(LogicalType::Int32, 10);
        chunk.set_num_values(1);
        let val: i32 = 99;
        chunk.set_raw(0, &val.to_ne_bytes());
        match &chunk {
            ColumnChunk::Fixed(c) => assert_eq!(c.get_value::<i32>(0), 99),
            _ => panic!("expected Fixed"),
        }
    }

    #[test]
    fn column_chunk_multiple_types() {
        // Test u8
        let mut c = ColumnChunkData::new(LogicalType::UInt8, 4);
        c.append_value(255u8);
        assert_eq!(c.get_value::<u8>(0), 255);

        // Test u16
        let mut c = ColumnChunkData::new(LogicalType::UInt16, 4);
        c.append_value(65535u16);
        assert_eq!(c.get_value::<u16>(0), 65535);

        // Test u32
        let mut c = ColumnChunkData::new(LogicalType::UInt32, 4);
        c.append_value(u32::MAX);
        assert_eq!(c.get_value::<u32>(0), u32::MAX);

        // Test f32
        let mut c = ColumnChunkData::new(LogicalType::Float, 4);
        c.append_value(1.5f32);
        assert_eq!(c.get_value::<f32>(0), 1.5);
    }
}

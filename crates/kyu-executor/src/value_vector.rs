//! ValueVector — columnar value container that preserves flat byte buffers.
//!
//! Instead of converting ColumnChunk's flat bytes into Vec<TypedValue> (~32 bytes
//! per tagged enum), ValueVector keeps the native representation: flat byte buffers
//! for fixed-size types, packed bits for booleans, and SmolStr vecs for strings.
//! Values are extracted on-demand via `get_value()`, avoiding upfront materialization.

use kyu_storage::{BoolChunkData, ColumnChunkData, NullMask, StringChunkData};
use kyu_types::{LogicalType, TypedValue};
use smol_str::SmolStr;

/// A column of values: either backed by a flat byte buffer (from storage),
/// packed bits (booleans), SmolStr vec (strings), or owned TypedValues
/// (computed by operators).
#[derive(Clone, Debug)]
pub enum ValueVector {
    /// Fixed-size values in a flat byte buffer + NullMask.
    Flat(FlatVector),
    /// Packed 1-bit booleans + NullMask.
    Bool(BoolVector),
    /// Variable-length strings.
    String(StringVector),
    /// Operator-computed values (fallback for expressions, aggregates, etc.).
    Owned(Vec<TypedValue>),
}

/// Fixed-size values stored in a flat byte buffer with a separate null mask.
/// For i64 columns: 8 bytes/value vs 32 bytes for TypedValue::Int64.
#[derive(Clone, Debug)]
pub struct FlatVector {
    data: Vec<u8>,
    null_mask: NullMask,
    logical_type: LogicalType,
    num_values: usize,
    stride: usize,
}

/// Packed 1-bit boolean values + separate null mask.
#[derive(Clone, Debug)]
pub struct BoolVector {
    values: NullMask,
    null_mask: NullMask,
    num_values: usize,
}

/// Variable-length string values.
#[derive(Clone, Debug)]
pub struct StringVector {
    data: Vec<Option<SmolStr>>,
    num_values: usize,
}

/// Logical selection over a ValueVector / DataChunk.
/// `None` indices = identity selection [0, 1, 2, ..., N-1].
#[derive(Clone, Debug)]
pub struct SelectionVector {
    indices: Option<Vec<u32>>,
    count: usize,
}

// ---------------------------------------------------------------------------
// SelectionVector
// ---------------------------------------------------------------------------

impl SelectionVector {
    /// All rows selected in order.
    pub fn identity(count: usize) -> Self {
        Self {
            indices: None,
            count,
        }
    }

    /// Explicit subset of physical row indices.
    pub fn from_indices(indices: Vec<u32>) -> Self {
        let count = indices.len();
        Self {
            indices: Some(indices),
            count,
        }
    }

    /// Map a logical index to a physical index.
    #[inline]
    pub fn get(&self, logical: usize) -> usize {
        match &self.indices {
            None => logical,
            Some(idx) => idx[logical] as usize,
        }
    }

    pub fn len(&self) -> usize {
        self.count
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Returns `true` when this is an identity selection (no indirection).
    pub fn is_identity(&self) -> bool {
        self.indices.is_none()
    }

    /// Raw pointer to the indices array, or null for identity selection (for JIT).
    pub fn indices_ptr(&self) -> *const u32 {
        match &self.indices {
            Some(v) => v.as_ptr(),
            None => std::ptr::null(),
        }
    }
}

// ---------------------------------------------------------------------------
// FlatVector
// ---------------------------------------------------------------------------

impl FlatVector {
    /// Construct from a ColumnChunkData by copying its byte buffer and NullMask.
    pub fn from_column_chunk(c: &ColumnChunkData, num_rows: usize) -> Self {
        let stride = c.num_bytes_per_value();
        let byte_count = num_rows * stride;
        Self {
            data: c.buffer()[..byte_count].to_vec(),
            null_mask: c.null_mask().clone(),
            logical_type: c.data_type().clone(),
            num_values: num_rows,
            stride,
        }
    }

    /// Construct from raw components (used by JIT projection output).
    pub fn from_raw(
        data: Vec<u8>,
        null_mask: NullMask,
        logical_type: LogicalType,
        num_values: usize,
        stride: usize,
    ) -> Self {
        Self {
            data,
            null_mask,
            logical_type,
            num_values,
            stride,
        }
    }

    /// Extract a single TypedValue at a physical index.
    pub fn get_value(&self, idx: usize) -> TypedValue {
        if self.null_mask.is_null(idx as u64) {
            return TypedValue::Null;
        }
        let offset = idx * self.stride;
        let bytes = &self.data[offset..offset + self.stride];
        match &self.logical_type {
            LogicalType::Int8 => {
                TypedValue::Int8(i8::from_ne_bytes(bytes[..1].try_into().unwrap()))
            }
            LogicalType::Int16 => {
                TypedValue::Int16(i16::from_ne_bytes(bytes[..2].try_into().unwrap()))
            }
            LogicalType::Int32 => {
                TypedValue::Int32(i32::from_ne_bytes(bytes[..4].try_into().unwrap()))
            }
            LogicalType::Int64 | LogicalType::Serial => {
                TypedValue::Int64(i64::from_ne_bytes(bytes[..8].try_into().unwrap()))
            }
            LogicalType::Float => {
                TypedValue::Float(f32::from_ne_bytes(bytes[..4].try_into().unwrap()))
            }
            LogicalType::Double => {
                TypedValue::Double(f64::from_ne_bytes(bytes[..8].try_into().unwrap()))
            }
            _ => TypedValue::Null,
        }
    }

    pub fn is_null(&self, idx: usize) -> bool {
        self.null_mask.is_null(idx as u64)
    }

    pub fn len(&self) -> usize {
        self.num_values
    }

    pub fn is_empty(&self) -> bool {
        self.num_values == 0
    }

    /// Direct access to the null mask for batch evaluation.
    pub fn null_mask(&self) -> &NullMask {
        &self.null_mask
    }

    /// The logical type of values in this vector.
    pub fn logical_type(&self) -> &LogicalType {
        &self.logical_type
    }

    /// Raw pointer to the flat byte buffer (for JIT compiled code).
    pub fn data_ptr(&self) -> *const u8 {
        self.data.as_ptr()
    }

    /// Value stride in bytes.
    pub fn stride(&self) -> usize {
        self.stride
    }

    /// Reinterpret the flat byte buffer as a typed i64 slice.
    /// Caller must ensure `logical_type` is Int64 or Serial.
    pub fn data_as_i64_slice(&self) -> &[i64] {
        debug_assert_eq!(self.stride, 8);
        let ptr = self.data.as_ptr() as *const i64;
        unsafe { std::slice::from_raw_parts(ptr, self.num_values) }
    }

    /// Reinterpret the flat byte buffer as a typed i32 slice.
    pub fn data_as_i32_slice(&self) -> &[i32] {
        debug_assert_eq!(self.stride, 4);
        let ptr = self.data.as_ptr() as *const i32;
        unsafe { std::slice::from_raw_parts(ptr, self.num_values) }
    }

    /// Reinterpret the flat byte buffer as a typed f64 slice.
    pub fn data_as_f64_slice(&self) -> &[f64] {
        debug_assert_eq!(self.stride, 8);
        let ptr = self.data.as_ptr() as *const f64;
        unsafe { std::slice::from_raw_parts(ptr, self.num_values) }
    }

    /// Reinterpret the flat byte buffer as a typed f32 slice.
    pub fn data_as_f32_slice(&self) -> &[f32] {
        debug_assert_eq!(self.stride, 4);
        let ptr = self.data.as_ptr() as *const f32;
        unsafe { std::slice::from_raw_parts(ptr, self.num_values) }
    }
}

// ---------------------------------------------------------------------------
// BoolVector
// ---------------------------------------------------------------------------

impl BoolVector {
    /// Construct from a BoolChunkData by cloning both NullMasks.
    pub fn from_bool_chunk(c: &BoolChunkData, num_rows: usize) -> Self {
        Self {
            values: c.values_mask().clone(),
            null_mask: c.null_mask().clone(),
            num_values: num_rows,
        }
    }

    pub fn get_value(&self, idx: usize) -> TypedValue {
        if self.null_mask.is_null(idx as u64) {
            TypedValue::Null
        } else {
            TypedValue::Bool(self.values.is_null(idx as u64))
        }
    }

    pub fn is_null(&self, idx: usize) -> bool {
        self.null_mask.is_null(idx as u64)
    }

    pub fn len(&self) -> usize {
        self.num_values
    }

    pub fn is_empty(&self) -> bool {
        self.num_values == 0
    }
}

// ---------------------------------------------------------------------------
// StringVector
// ---------------------------------------------------------------------------

impl StringVector {
    /// Construct from a StringChunkData by copying the data slice.
    pub fn from_string_chunk(c: &StringChunkData, num_rows: usize) -> Self {
        Self {
            data: c.data_slice()[..num_rows].to_vec(),
            num_values: num_rows,
        }
    }

    pub fn get_value(&self, idx: usize) -> TypedValue {
        match &self.data[idx] {
            Some(s) => TypedValue::String(s.clone()),
            None => TypedValue::Null,
        }
    }

    pub fn is_null(&self, idx: usize) -> bool {
        self.data[idx].is_none()
    }

    pub fn len(&self) -> usize {
        self.num_values
    }

    pub fn is_empty(&self) -> bool {
        self.num_values == 0
    }

    /// Direct access to the underlying string data for batch evaluation.
    pub fn data(&self) -> &[Option<SmolStr>] {
        &self.data
    }
}

// ---------------------------------------------------------------------------
// ValueVector dispatch
// ---------------------------------------------------------------------------

impl ValueVector {
    /// Extract a TypedValue at a physical index.
    pub fn get_value(&self, idx: usize) -> TypedValue {
        match self {
            Self::Flat(v) => v.get_value(idx),
            Self::Bool(v) => v.get_value(idx),
            Self::String(v) => v.get_value(idx),
            Self::Owned(v) => v[idx].clone(),
        }
    }

    /// Check if the value at a physical index is null.
    pub fn is_null(&self, idx: usize) -> bool {
        match self {
            Self::Flat(v) => v.is_null(idx),
            Self::Bool(v) => v.is_null(idx),
            Self::String(v) => v.is_null(idx),
            Self::Owned(v) => v[idx].is_null(),
        }
    }

    /// Number of physical values stored.
    pub fn len(&self) -> usize {
        match self {
            Self::Flat(v) => v.len(),
            Self::Bool(v) => v.len(),
            Self::String(v) => v.len(),
            Self::Owned(v) => v.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Push a value onto an Owned vector. Panics on non-Owned variants.
    pub fn push(&mut self, val: TypedValue) {
        match self {
            Self::Owned(v) => v.push(val),
            _ => panic!("ValueVector::push only supported on Owned variant"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flat_vector_int64_roundtrip() {
        let mut chunk = ColumnChunkData::new(LogicalType::Int64, 8);
        chunk.append_value::<i64>(42);
        chunk.append_value::<i64>(-7);
        chunk.append_null();

        let vec = FlatVector::from_column_chunk(&chunk, 3);
        assert_eq!(vec.len(), 3);
        assert_eq!(vec.get_value(0), TypedValue::Int64(42));
        assert_eq!(vec.get_value(1), TypedValue::Int64(-7));
        assert_eq!(vec.get_value(2), TypedValue::Null);
        assert!(!vec.is_null(0));
        assert!(vec.is_null(2));
    }

    #[test]
    fn flat_vector_double_roundtrip() {
        let mut chunk = ColumnChunkData::new(LogicalType::Double, 4);
        chunk.append_value::<f64>(3.14);
        chunk.append_value::<f64>(-2.5);

        let vec = FlatVector::from_column_chunk(&chunk, 2);
        assert_eq!(vec.get_value(0), TypedValue::Double(3.14));
        assert_eq!(vec.get_value(1), TypedValue::Double(-2.5));
    }

    #[test]
    fn bool_vector_roundtrip() {
        let mut chunk = BoolChunkData::new(8);
        chunk.append_bool(true);
        chunk.append_bool(false);
        chunk.append_null();

        let vec = BoolVector::from_bool_chunk(&chunk, 3);
        assert_eq!(vec.len(), 3);
        assert_eq!(vec.get_value(0), TypedValue::Bool(true));
        assert_eq!(vec.get_value(1), TypedValue::Bool(false));
        assert_eq!(vec.get_value(2), TypedValue::Null);
    }

    #[test]
    fn string_vector_roundtrip() {
        let mut chunk = StringChunkData::new(8);
        chunk.append_string(SmolStr::new("hello"));
        chunk.append_null();
        chunk.append_string(SmolStr::new("world"));

        let vec = StringVector::from_string_chunk(&chunk, 3);
        assert_eq!(vec.len(), 3);
        assert_eq!(vec.get_value(0), TypedValue::String(SmolStr::new("hello")));
        assert_eq!(vec.get_value(1), TypedValue::Null);
        assert_eq!(vec.get_value(2), TypedValue::String(SmolStr::new("world")));
    }

    #[test]
    fn owned_push_and_get() {
        let mut vec = ValueVector::Owned(Vec::new());
        vec.push(TypedValue::Int64(1));
        vec.push(TypedValue::Int64(2));
        assert_eq!(vec.len(), 2);
        assert_eq!(vec.get_value(0), TypedValue::Int64(1));
        assert_eq!(vec.get_value(1), TypedValue::Int64(2));
    }

    #[test]
    fn value_vector_dispatch() {
        let mut chunk = ColumnChunkData::new(LogicalType::Int64, 4);
        chunk.append_value::<i64>(99);
        let vv = ValueVector::Flat(FlatVector::from_column_chunk(&chunk, 1));
        assert_eq!(vv.get_value(0), TypedValue::Int64(99));
        assert!(!vv.is_null(0));
        assert_eq!(vv.len(), 1);
    }

    #[test]
    fn selection_vector_identity() {
        let sel = SelectionVector::identity(5);
        assert_eq!(sel.len(), 5);
        assert_eq!(sel.get(0), 0);
        assert_eq!(sel.get(4), 4);
    }

    #[test]
    fn selection_vector_explicit() {
        let sel = SelectionVector::from_indices(vec![2, 5, 7]);
        assert_eq!(sel.len(), 3);
        assert_eq!(sel.get(0), 2);
        assert_eq!(sel.get(1), 5);
        assert_eq!(sel.get(2), 7);
    }

    #[test]
    fn selection_vector_empty() {
        let sel = SelectionVector::from_indices(vec![]);
        assert!(sel.is_empty());
        assert_eq!(sel.len(), 0);
    }
}

use kyu_common::InternalId;
use smol_str::SmolStr;

use crate::interval::Interval;

/// A typed value used during parsing, binding, and planning.
///
/// This is the ergonomic representation. The storage-optimized 32-byte
/// untagged `Value` union for the executor is introduced in Phase 3
/// when the storage layer and vectorized execution need it.
#[derive(Clone, Debug)]
pub enum TypedValue {
    Null,
    Bool(bool),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Int128(i128),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    Float(f32),
    Double(f64),
    Date(i64),
    Timestamp(i64),
    TimestampSec(i64),
    TimestampMs(i64),
    TimestampNs(i64),
    TimestampTz(i64),
    Interval(Interval),
    String(SmolStr),
    Blob(Vec<u8>),
    Uuid(SmolStr),
    InternalId(InternalId),
    Serial(i64),
    List(Vec<TypedValue>),
    Array(Vec<TypedValue>),
    Struct(Vec<(SmolStr, TypedValue)>),
    Map(Vec<(TypedValue, TypedValue)>),
}

// Manual PartialEq: use to_bits() for floats to get total ordering.
impl PartialEq for TypedValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Null, Self::Null) => true,
            (Self::Bool(a), Self::Bool(b)) => a == b,
            (Self::Int8(a), Self::Int8(b)) => a == b,
            (Self::Int16(a), Self::Int16(b)) => a == b,
            (Self::Int32(a), Self::Int32(b)) => a == b,
            (Self::Int64(a), Self::Int64(b)) => a == b,
            (Self::Int128(a), Self::Int128(b)) => a == b,
            (Self::UInt8(a), Self::UInt8(b)) => a == b,
            (Self::UInt16(a), Self::UInt16(b)) => a == b,
            (Self::UInt32(a), Self::UInt32(b)) => a == b,
            (Self::UInt64(a), Self::UInt64(b)) => a == b,
            (Self::Float(a), Self::Float(b)) => a.to_bits() == b.to_bits(),
            (Self::Double(a), Self::Double(b)) => a.to_bits() == b.to_bits(),
            (Self::Date(a), Self::Date(b)) => a == b,
            (Self::Timestamp(a), Self::Timestamp(b)) => a == b,
            (Self::TimestampSec(a), Self::TimestampSec(b)) => a == b,
            (Self::TimestampMs(a), Self::TimestampMs(b)) => a == b,
            (Self::TimestampNs(a), Self::TimestampNs(b)) => a == b,
            (Self::TimestampTz(a), Self::TimestampTz(b)) => a == b,
            (Self::Interval(a), Self::Interval(b)) => a == b,
            (Self::String(a), Self::String(b)) => a == b,
            (Self::Blob(a), Self::Blob(b)) => a == b,
            (Self::Uuid(a), Self::Uuid(b)) => a == b,
            (Self::InternalId(a), Self::InternalId(b)) => a == b,
            (Self::Serial(a), Self::Serial(b)) => a == b,
            (Self::List(a), Self::List(b)) => a == b,
            (Self::Array(a), Self::Array(b)) => a == b,
            (Self::Struct(a), Self::Struct(b)) => a == b,
            (Self::Map(a), Self::Map(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for TypedValue {}

impl std::hash::Hash for TypedValue {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            Self::Null => {}
            Self::Bool(v) => v.hash(state),
            Self::Int8(v) => v.hash(state),
            Self::Int16(v) => v.hash(state),
            Self::Int32(v) => v.hash(state),
            Self::Int64(v) => v.hash(state),
            Self::Int128(v) => v.hash(state),
            Self::UInt8(v) => v.hash(state),
            Self::UInt16(v) => v.hash(state),
            Self::UInt32(v) => v.hash(state),
            Self::UInt64(v) => v.hash(state),
            Self::Float(v) => v.to_bits().hash(state),
            Self::Double(v) => v.to_bits().hash(state),
            Self::Date(v) => v.hash(state),
            Self::Timestamp(v) => v.hash(state),
            Self::TimestampSec(v) => v.hash(state),
            Self::TimestampMs(v) => v.hash(state),
            Self::TimestampNs(v) => v.hash(state),
            Self::TimestampTz(v) => v.hash(state),
            Self::Interval(v) => v.hash(state),
            Self::String(v) => v.hash(state),
            Self::Blob(v) => v.hash(state),
            Self::Uuid(v) => v.hash(state),
            Self::InternalId(v) => v.hash(state),
            Self::Serial(v) => v.hash(state),
            Self::List(v) => v.hash(state),
            Self::Array(v) => v.hash(state),
            Self::Struct(v) => v.hash(state),
            Self::Map(v) => v.hash(state),
        }
    }
}

impl TypedValue {
    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    /// Try to extract a boolean.
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Bool(v) => Some(*v),
            _ => None,
        }
    }

    /// Try to extract an i64 (works for all integer types that fit).
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Self::Int8(v) => Some(*v as i64),
            Self::Int16(v) => Some(*v as i64),
            Self::Int32(v) => Some(*v as i64),
            Self::Int64(v) | Self::Date(v) | Self::Timestamp(v) | Self::Serial(v) => Some(*v),
            Self::UInt8(v) => Some(*v as i64),
            Self::UInt16(v) => Some(*v as i64),
            Self::UInt32(v) => Some(*v as i64),
            _ => None,
        }
    }

    /// Try to extract an f64 (works for Float and Double).
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Self::Float(v) => Some(*v as f64),
            Self::Double(v) => Some(*v),
            _ => None,
        }
    }

    /// Try to extract a string reference.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::String(s) | Self::Uuid(s) => Some(s.as_str()),
            _ => None,
        }
    }

    /// Try to extract an InternalId.
    pub fn as_internal_id(&self) -> Option<InternalId> {
        match self {
            Self::InternalId(id) => Some(*id),
            _ => None,
        }
    }
}

impl std::fmt::Display for TypedValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Null => write!(f, "NULL"),
            Self::Bool(v) => write!(f, "{v}"),
            Self::Int8(v) => write!(f, "{v}"),
            Self::Int16(v) => write!(f, "{v}"),
            Self::Int32(v) => write!(f, "{v}"),
            Self::Int64(v) | Self::Serial(v) => write!(f, "{v}"),
            Self::Int128(v) => write!(f, "{v}"),
            Self::UInt8(v) => write!(f, "{v}"),
            Self::UInt16(v) => write!(f, "{v}"),
            Self::UInt32(v) => write!(f, "{v}"),
            Self::UInt64(v) => write!(f, "{v}"),
            Self::Float(v) => write!(f, "{v}"),
            Self::Double(v) => write!(f, "{v}"),
            Self::Date(v) => write!(f, "{v}"),
            Self::Timestamp(v)
            | Self::TimestampSec(v)
            | Self::TimestampMs(v)
            | Self::TimestampNs(v)
            | Self::TimestampTz(v) => write!(f, "{v}"),
            Self::Interval(iv) => write!(f, "{iv}"),
            Self::String(s) | Self::Uuid(s) => write!(f, "{s}"),
            Self::Blob(b) => write!(f, "\\x{}", hex_encode(b)),
            Self::InternalId(id) => write!(f, "{id}"),
            Self::List(items) | Self::Array(items) => {
                write!(f, "[")?;
                for (i, item) in items.iter().enumerate() {
                    if i > 0 {
                        write!(f, ",")?;
                    }
                    write!(f, "{item}")?;
                }
                write!(f, "]")
            }
            Self::Struct(fields) => {
                write!(f, "{{")?;
                for (i, (name, val)) in fields.iter().enumerate() {
                    if i > 0 {
                        write!(f, ",")?;
                    }
                    write!(f, "{name}: {val}")?;
                }
                write!(f, "}}")
            }
            Self::Map(entries) => {
                write!(f, "{{")?;
                for (i, (k, v)) in entries.iter().enumerate() {
                    if i > 0 {
                        write!(f, ",")?;
                    }
                    write!(f, "{k}={v}")?;
                }
                write!(f, "}}")
            }
        }
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn null() {
        let v = TypedValue::Null;
        assert!(v.is_null());
        assert_eq!(v.to_string(), "NULL");
    }

    #[test]
    fn bool_round_trip() {
        let v = TypedValue::Bool(true);
        assert_eq!(v.as_bool(), Some(true));
        assert!(!v.is_null());
    }

    #[test]
    fn integer_round_trips() {
        assert_eq!(TypedValue::Int8(42).as_i64(), Some(42));
        assert_eq!(TypedValue::Int16(-1000).as_i64(), Some(-1000));
        assert_eq!(TypedValue::Int32(100_000).as_i64(), Some(100_000));
        assert_eq!(TypedValue::Int64(i64::MAX).as_i64(), Some(i64::MAX));
        assert_eq!(TypedValue::UInt8(255).as_i64(), Some(255));
        assert_eq!(TypedValue::UInt16(65535).as_i64(), Some(65535));
        assert_eq!(TypedValue::UInt32(u32::MAX).as_i64(), Some(u32::MAX as i64));
    }

    #[test]
    fn float_round_trips() {
        let f = TypedValue::Float(3.14);
        assert!((f.as_f64().unwrap() - 3.14f32 as f64).abs() < 1e-6);

        let d = TypedValue::Double(2.718281828);
        assert!((d.as_f64().unwrap() - 2.718281828).abs() < 1e-9);
    }

    #[test]
    fn string_round_trip() {
        let v = TypedValue::String(SmolStr::new("hello"));
        assert_eq!(v.as_str(), Some("hello"));
        assert_eq!(v.to_string(), "hello");
    }

    #[test]
    fn internal_id_round_trip() {
        let id = InternalId::new(1, 42);
        let v = TypedValue::InternalId(id);
        assert_eq!(v.as_internal_id(), Some(id));
    }

    #[test]
    fn list_display() {
        let v = TypedValue::List(vec![
            TypedValue::Int64(1),
            TypedValue::Int64(2),
            TypedValue::Int64(3),
        ]);
        assert_eq!(v.to_string(), "[1,2,3]");
    }

    #[test]
    fn struct_display() {
        let v = TypedValue::Struct(vec![
            (SmolStr::new("name"), TypedValue::String(SmolStr::new("Alice"))),
            (SmolStr::new("age"), TypedValue::Int64(30)),
        ]);
        assert_eq!(v.to_string(), "{name: Alice,age: 30}");
    }

    #[test]
    fn wrong_type_returns_none() {
        let v = TypedValue::String(SmolStr::new("hello"));
        assert_eq!(v.as_bool(), None);
        assert_eq!(v.as_i64(), None);
        assert_eq!(v.as_f64(), None);
    }

    #[test]
    fn clone_and_eq() {
        let a = TypedValue::List(vec![TypedValue::Int64(1)]);
        let b = a.clone();
        assert_eq!(a, b);
    }
}

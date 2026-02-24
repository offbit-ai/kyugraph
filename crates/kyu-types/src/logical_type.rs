use crate::physical_type::PhysicalType;
use smol_str::SmolStr;

/// Logical data type representing user-facing type semantics.
/// Multiple logical types may share the same `PhysicalType`
/// (e.g., Date/Timestamp/TimestampNs all use Int64 physically).
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum LogicalType {
    Any,
    Bool,
    Int8,
    Int16,
    Int32,
    Int64,
    Int128,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float,
    Double,
    Date,
    Timestamp,
    TimestampSec,
    TimestampMs,
    TimestampNs,
    TimestampTz,
    Interval,
    Decimal {
        precision: u8,
        scale: u8,
    },
    InternalId,
    Serial,
    String,
    Blob,
    Uuid,
    Node,
    Rel,
    RecursiveRel,
    List(Box<LogicalType>),
    Array {
        element: Box<LogicalType>,
        size: u64,
    },
    Struct(Vec<(SmolStr, LogicalType)>),
    Map {
        key: Box<LogicalType>,
        value: Box<LogicalType>,
    },
    Union(Vec<(SmolStr, LogicalType)>),
    Pointer,
}

impl LogicalType {
    /// Returns the physical storage type for this logical type.
    pub fn physical_type(&self) -> PhysicalType {
        match self {
            Self::Any => PhysicalType::String,
            Self::Bool => PhysicalType::Bool,
            Self::Int8 => PhysicalType::Int8,
            Self::Int16 => PhysicalType::Int16,
            Self::Int32 => PhysicalType::Int32,
            Self::Int64
            | Self::Date
            | Self::Timestamp
            | Self::TimestampSec
            | Self::TimestampMs
            | Self::TimestampNs
            | Self::TimestampTz
            | Self::Serial => PhysicalType::Int64,
            Self::Int128 | Self::Decimal { .. } => PhysicalType::Int128,
            Self::UInt8 => PhysicalType::UInt8,
            Self::UInt16 => PhysicalType::UInt16,
            Self::UInt32 => PhysicalType::UInt32,
            Self::UInt64 => PhysicalType::UInt64,
            Self::Float => PhysicalType::Float32,
            Self::Double => PhysicalType::Float64,
            Self::Interval => PhysicalType::Interval,
            Self::InternalId => PhysicalType::InternalId,
            Self::String | Self::Blob | Self::Uuid => PhysicalType::String,
            Self::List(_) | Self::Map { .. } => PhysicalType::List,
            Self::Array { .. } => PhysicalType::Array,
            Self::Struct(_) | Self::Node | Self::Rel | Self::RecursiveRel | Self::Union(_) => {
                PhysicalType::Struct
            }
            Self::Pointer => PhysicalType::Int64,
        }
    }

    /// Human-readable type name for error messages.
    pub fn type_name(&self) -> std::borrow::Cow<'static, str> {
        match self {
            Self::Any => "ANY".into(),
            Self::Bool => "BOOL".into(),
            Self::Int8 => "INT8".into(),
            Self::Int16 => "INT16".into(),
            Self::Int32 => "INT32".into(),
            Self::Int64 => "INT64".into(),
            Self::Int128 => "INT128".into(),
            Self::UInt8 => "UINT8".into(),
            Self::UInt16 => "UINT16".into(),
            Self::UInt32 => "UINT32".into(),
            Self::UInt64 => "UINT64".into(),
            Self::Float => "FLOAT".into(),
            Self::Double => "DOUBLE".into(),
            Self::Date => "DATE".into(),
            Self::Timestamp => "TIMESTAMP".into(),
            Self::TimestampSec => "TIMESTAMP_SEC".into(),
            Self::TimestampMs => "TIMESTAMP_MS".into(),
            Self::TimestampNs => "TIMESTAMP_NS".into(),
            Self::TimestampTz => "TIMESTAMP_TZ".into(),
            Self::Interval => "INTERVAL".into(),
            Self::Decimal { precision, scale } => format!("DECIMAL({precision},{scale})").into(),
            Self::InternalId => "INTERNAL_ID".into(),
            Self::Serial => "SERIAL".into(),
            Self::String => "STRING".into(),
            Self::Blob => "BLOB".into(),
            Self::Uuid => "UUID".into(),
            Self::Node => "NODE".into(),
            Self::Rel => "REL".into(),
            Self::RecursiveRel => "RECURSIVE_REL".into(),
            Self::List(inner) => format!("{}[]", inner.type_name()).into(),
            Self::Array { element, size } => format!("{}[{size}]", element.type_name()).into(),
            Self::Struct(fields) => {
                let fields_str: Vec<_> = fields
                    .iter()
                    .map(|(name, ty)| format!("{name}: {}", ty.type_name()))
                    .collect();
                format!("STRUCT({})", fields_str.join(", ")).into()
            }
            Self::Map { key, value } => {
                format!("MAP({}, {})", key.type_name(), value.type_name()).into()
            }
            Self::Union(fields) => {
                let fields_str: Vec<_> = fields
                    .iter()
                    .map(|(name, ty)| format!("{name}: {}", ty.type_name()))
                    .collect();
                format!("UNION({})", fields_str.join(", ")).into()
            }
            Self::Pointer => "POINTER".into(),
        }
    }

    /// Whether this is a numeric type (integer or floating-point).
    pub fn is_numeric(&self) -> bool {
        matches!(
            self,
            Self::Int8
                | Self::Int16
                | Self::Int32
                | Self::Int64
                | Self::Int128
                | Self::UInt8
                | Self::UInt16
                | Self::UInt32
                | Self::UInt64
                | Self::Float
                | Self::Double
                | Self::Serial
                | Self::Decimal { .. }
        )
    }

    /// Whether this is an integer type (signed or unsigned).
    pub fn is_integer(&self) -> bool {
        matches!(
            self,
            Self::Int8
                | Self::Int16
                | Self::Int32
                | Self::Int64
                | Self::Int128
                | Self::UInt8
                | Self::UInt16
                | Self::UInt32
                | Self::UInt64
                | Self::Serial
        )
    }

    /// Whether this is a temporal type.
    pub fn is_temporal(&self) -> bool {
        matches!(
            self,
            Self::Date
                | Self::Timestamp
                | Self::TimestampSec
                | Self::TimestampMs
                | Self::TimestampNs
                | Self::TimestampTz
                | Self::Interval
        )
    }

    /// Whether this is a nested type (List, Array, Struct, Map, Union).
    pub fn is_nested(&self) -> bool {
        matches!(
            self,
            Self::List(_)
                | Self::Array { .. }
                | Self::Struct(_)
                | Self::Map { .. }
                | Self::Union(_)
        )
    }
}

impl std::fmt::Display for LogicalType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.type_name())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn physical_type_mapping_integers() {
        assert_eq!(LogicalType::Int8.physical_type(), PhysicalType::Int8);
        assert_eq!(LogicalType::Int16.physical_type(), PhysicalType::Int16);
        assert_eq!(LogicalType::Int32.physical_type(), PhysicalType::Int32);
        assert_eq!(LogicalType::Int64.physical_type(), PhysicalType::Int64);
        assert_eq!(LogicalType::Int128.physical_type(), PhysicalType::Int128);
        assert_eq!(LogicalType::UInt8.physical_type(), PhysicalType::UInt8);
        assert_eq!(LogicalType::UInt16.physical_type(), PhysicalType::UInt16);
        assert_eq!(LogicalType::UInt32.physical_type(), PhysicalType::UInt32);
        assert_eq!(LogicalType::UInt64.physical_type(), PhysicalType::UInt64);
    }

    #[test]
    fn physical_type_mapping_temporal() {
        assert_eq!(LogicalType::Date.physical_type(), PhysicalType::Int64);
        assert_eq!(LogicalType::Timestamp.physical_type(), PhysicalType::Int64);
        assert_eq!(
            LogicalType::TimestampNs.physical_type(),
            PhysicalType::Int64
        );
        assert_eq!(LogicalType::Serial.physical_type(), PhysicalType::Int64);
        assert_eq!(
            LogicalType::Interval.physical_type(),
            PhysicalType::Interval
        );
    }

    #[test]
    fn physical_type_mapping_strings() {
        assert_eq!(LogicalType::String.physical_type(), PhysicalType::String);
        assert_eq!(LogicalType::Blob.physical_type(), PhysicalType::String);
        assert_eq!(LogicalType::Uuid.physical_type(), PhysicalType::String);
    }

    #[test]
    fn physical_type_mapping_nested() {
        let list = LogicalType::List(Box::new(LogicalType::Int64));
        assert_eq!(list.physical_type(), PhysicalType::List);

        let arr = LogicalType::Array {
            element: Box::new(LogicalType::Float),
            size: 1536,
        };
        assert_eq!(arr.physical_type(), PhysicalType::Array);

        let strukt = LogicalType::Struct(vec![
            (SmolStr::new("name"), LogicalType::String),
            (SmolStr::new("age"), LogicalType::Int64),
        ]);
        assert_eq!(strukt.physical_type(), PhysicalType::Struct);

        let map = LogicalType::Map {
            key: Box::new(LogicalType::String),
            value: Box::new(LogicalType::Int64),
        };
        assert_eq!(map.physical_type(), PhysicalType::List);
    }

    #[test]
    fn type_name_simple() {
        assert_eq!(LogicalType::Bool.type_name().as_ref(), "BOOL");
        assert_eq!(LogicalType::Int64.type_name().as_ref(), "INT64");
        assert_eq!(LogicalType::String.type_name().as_ref(), "STRING");
    }

    #[test]
    fn type_name_complex() {
        let list = LogicalType::List(Box::new(LogicalType::Int64));
        assert_eq!(list.type_name().as_ref(), "INT64[]");

        let arr = LogicalType::Array {
            element: Box::new(LogicalType::Float),
            size: 1536,
        };
        assert_eq!(arr.type_name().as_ref(), "FLOAT[1536]");

        let dec = LogicalType::Decimal {
            precision: 18,
            scale: 3,
        };
        assert_eq!(dec.type_name().as_ref(), "DECIMAL(18,3)");
    }

    #[test]
    fn classification_methods() {
        assert!(LogicalType::Int64.is_numeric());
        assert!(LogicalType::Float.is_numeric());
        assert!(!LogicalType::String.is_numeric());

        assert!(LogicalType::Int32.is_integer());
        assert!(LogicalType::UInt64.is_integer());
        assert!(!LogicalType::Float.is_integer());

        assert!(LogicalType::Date.is_temporal());
        assert!(LogicalType::Interval.is_temporal());
        assert!(!LogicalType::Int64.is_temporal());

        assert!(LogicalType::List(Box::new(LogicalType::Any)).is_nested());
        assert!(!LogicalType::String.is_nested());
    }

    #[test]
    fn equality_and_hash() {
        use std::collections::HashSet;

        let a = LogicalType::List(Box::new(LogicalType::Int64));
        let b = LogicalType::List(Box::new(LogicalType::Int64));
        let c = LogicalType::List(Box::new(LogicalType::Int32));
        assert_eq!(a, b);
        assert_ne!(a, c);

        let mut set = HashSet::new();
        set.insert(a);
        set.insert(b);
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn display() {
        assert_eq!(LogicalType::Int64.to_string(), "INT64");
        let list = LogicalType::List(Box::new(LogicalType::String));
        assert_eq!(list.to_string(), "STRING[]");
    }
}

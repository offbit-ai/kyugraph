/// Physical storage representation of a value.
/// Controls how bytes are laid out in columns and the Value union.
///
/// Discriminant values match the C++ `PhysicalTypeID` for on-disk compatibility.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum PhysicalType {
    Bool = 1,
    Int64 = 2,
    Int32 = 3,
    Int16 = 4,
    Int8 = 5,
    UInt64 = 6,
    UInt32 = 7,
    UInt16 = 8,
    UInt8 = 9,
    Int128 = 10,
    Float64 = 11,
    Float32 = 12,
    Interval = 13,
    InternalId = 14,
    String = 20,
    List = 22,
    Array = 23,
    Struct = 24,
}

impl PhysicalType {
    /// Size in bytes of the fixed-size inline representation.
    /// Returns `None` for variable-length types (String, List, Struct).
    pub const fn fixed_size(&self) -> Option<usize> {
        match self {
            Self::Bool => Some(1),
            Self::Int8 | Self::UInt8 => Some(1),
            Self::Int16 | Self::UInt16 => Some(2),
            Self::Int32 | Self::UInt32 | Self::Float32 => Some(4),
            Self::Int64 | Self::UInt64 | Self::Float64 => Some(8),
            Self::Int128 => Some(16),
            Self::Interval => Some(16),    // months(4) + days(4) + micros(8)
            Self::InternalId => Some(16),  // table_id(8) + offset(8)
            Self::String => None,
            Self::List => None,
            Self::Array => None,
            Self::Struct => None,
        }
    }

    /// Whether this type has a fixed size (stored inline in Value).
    pub const fn is_fixed_size(&self) -> bool {
        self.fixed_size().is_some()
    }
}

impl std::fmt::Display for PhysicalType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            Self::Bool => "BOOL",
            Self::Int8 => "INT8",
            Self::Int16 => "INT16",
            Self::Int32 => "INT32",
            Self::Int64 => "INT64",
            Self::Int128 => "INT128",
            Self::UInt8 => "UINT8",
            Self::UInt16 => "UINT16",
            Self::UInt32 => "UINT32",
            Self::UInt64 => "UINT64",
            Self::Float32 => "FLOAT",
            Self::Float64 => "DOUBLE",
            Self::Interval => "INTERVAL",
            Self::InternalId => "INTERNAL_ID",
            Self::String => "STRING",
            Self::List => "LIST",
            Self::Array => "ARRAY",
            Self::Struct => "STRUCT",
        };
        write!(f, "{name}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn discriminant_values_match_cpp() {
        assert_eq!(PhysicalType::Bool as u8, 1);
        assert_eq!(PhysicalType::Int64 as u8, 2);
        assert_eq!(PhysicalType::Int32 as u8, 3);
        assert_eq!(PhysicalType::Int16 as u8, 4);
        assert_eq!(PhysicalType::Int8 as u8, 5);
        assert_eq!(PhysicalType::UInt64 as u8, 6);
        assert_eq!(PhysicalType::UInt32 as u8, 7);
        assert_eq!(PhysicalType::UInt16 as u8, 8);
        assert_eq!(PhysicalType::UInt8 as u8, 9);
        assert_eq!(PhysicalType::Int128 as u8, 10);
        assert_eq!(PhysicalType::Float64 as u8, 11);
        assert_eq!(PhysicalType::Float32 as u8, 12);
        assert_eq!(PhysicalType::Interval as u8, 13);
        assert_eq!(PhysicalType::InternalId as u8, 14);
        assert_eq!(PhysicalType::String as u8, 20);
        assert_eq!(PhysicalType::List as u8, 22);
        assert_eq!(PhysicalType::Array as u8, 23);
        assert_eq!(PhysicalType::Struct as u8, 24);
    }

    #[test]
    fn fixed_sizes() {
        assert_eq!(PhysicalType::Bool.fixed_size(), Some(1));
        assert_eq!(PhysicalType::Int8.fixed_size(), Some(1));
        assert_eq!(PhysicalType::Int16.fixed_size(), Some(2));
        assert_eq!(PhysicalType::Int32.fixed_size(), Some(4));
        assert_eq!(PhysicalType::Int64.fixed_size(), Some(8));
        assert_eq!(PhysicalType::Int128.fixed_size(), Some(16));
        assert_eq!(PhysicalType::Float32.fixed_size(), Some(4));
        assert_eq!(PhysicalType::Float64.fixed_size(), Some(8));
        assert_eq!(PhysicalType::Interval.fixed_size(), Some(16));
        assert_eq!(PhysicalType::InternalId.fixed_size(), Some(16));
        assert_eq!(PhysicalType::String.fixed_size(), None);
        assert_eq!(PhysicalType::List.fixed_size(), None);
        assert_eq!(PhysicalType::Array.fixed_size(), None);
        assert_eq!(PhysicalType::Struct.fixed_size(), None);
    }

    #[test]
    fn is_fixed_size() {
        assert!(PhysicalType::Int64.is_fixed_size());
        assert!(!PhysicalType::String.is_fixed_size());
        assert!(!PhysicalType::List.is_fixed_size());
    }

    #[test]
    fn enum_is_one_byte() {
        assert_eq!(std::mem::size_of::<PhysicalType>(), 1);
    }

    #[test]
    fn display() {
        assert_eq!(PhysicalType::Bool.to_string(), "BOOL");
        assert_eq!(PhysicalType::String.to_string(), "STRING");
        assert_eq!(PhysicalType::InternalId.to_string(), "INTERNAL_ID");
    }
}

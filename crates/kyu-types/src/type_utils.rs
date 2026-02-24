use crate::logical_type::LogicalType;

/// Cost of an implicit cast from `from` to `to`.
/// Returns `None` if no implicit cast is possible.
/// Lower cost = more preferred.
pub fn implicit_cast_cost(from: &LogicalType, to: &LogicalType) -> Option<u32> {
    if from == to {
        return Some(0);
    }

    // Any can be implicitly cast to anything.
    if matches!(from, LogicalType::Any) {
        return Some(1);
    }

    match (from, to) {
        // Integer widening (signed)
        (LogicalType::Int8, LogicalType::Int16) => Some(1),
        (LogicalType::Int8, LogicalType::Int32) => Some(2),
        (LogicalType::Int8, LogicalType::Int64) => Some(3),
        (LogicalType::Int8, LogicalType::Int128) => Some(4),
        (LogicalType::Int16, LogicalType::Int32) => Some(1),
        (LogicalType::Int16, LogicalType::Int64) => Some(2),
        (LogicalType::Int16, LogicalType::Int128) => Some(3),
        (LogicalType::Int32, LogicalType::Int64) => Some(1),
        (LogicalType::Int32, LogicalType::Int128) => Some(2),
        (LogicalType::Int64, LogicalType::Int128) => Some(1),

        // Integer widening (unsigned)
        (LogicalType::UInt8, LogicalType::UInt16) => Some(1),
        (LogicalType::UInt8, LogicalType::UInt32) => Some(2),
        (LogicalType::UInt8, LogicalType::UInt64) => Some(3),
        (LogicalType::UInt16, LogicalType::UInt32) => Some(1),
        (LogicalType::UInt16, LogicalType::UInt64) => Some(2),
        (LogicalType::UInt32, LogicalType::UInt64) => Some(1),

        // Unsigned to signed widening
        (LogicalType::UInt8, LogicalType::Int16) => Some(1),
        (LogicalType::UInt8, LogicalType::Int32) => Some(2),
        (LogicalType::UInt8, LogicalType::Int64) => Some(3),
        (LogicalType::UInt16, LogicalType::Int32) => Some(1),
        (LogicalType::UInt16, LogicalType::Int64) => Some(2),
        (LogicalType::UInt32, LogicalType::Int64) => Some(1),

        // Integer to float
        (from, LogicalType::Float) if from.is_integer() => Some(10),
        (from, LogicalType::Double) if from.is_integer() => Some(10),

        // Float to double
        (LogicalType::Float, LogicalType::Double) => Some(1),

        // Anything to string
        (_, LogicalType::String) => Some(100),

        _ => None,
    }
}

/// Whether two types can be compared with =, <, >, etc.
pub fn are_comparable(a: &LogicalType, b: &LogicalType) -> bool {
    if a == b {
        return true;
    }

    // Numeric types are mutually comparable.
    if a.is_numeric() && b.is_numeric() {
        return true;
    }

    // Temporal types of the same kind are comparable.
    if a.is_temporal() && b.is_temporal() {
        return true;
    }

    // String/Blob are comparable with each other.
    if matches!(
        (a, b),
        (LogicalType::String, LogicalType::Blob) | (LogicalType::Blob, LogicalType::String)
    ) {
        return true;
    }

    // Any is comparable with anything.
    if matches!(a, LogicalType::Any) || matches!(b, LogicalType::Any) {
        return true;
    }

    false
}

/// The result type of a binary arithmetic operation (a op b).
/// Returns `None` if arithmetic is not defined for these types.
pub fn arithmetic_result_type(a: &LogicalType, b: &LogicalType) -> Option<LogicalType> {
    match (a, b) {
        // Same integer type -> same type
        (LogicalType::Int8, LogicalType::Int8) => Some(LogicalType::Int8),
        (LogicalType::Int16, LogicalType::Int16) => Some(LogicalType::Int16),
        (LogicalType::Int32, LogicalType::Int32) => Some(LogicalType::Int32),
        (LogicalType::Int64, LogicalType::Int64) => Some(LogicalType::Int64),
        (LogicalType::Int128, LogicalType::Int128) => Some(LogicalType::Int128),

        // Mixed signed integers -> wider type
        (a, b) if a.is_integer() && b.is_integer() => {
            // Find the wider type using implicit cast
            if implicit_cast_cost(a, b).is_some() {
                Some(b.clone())
            } else if implicit_cast_cost(b, a).is_some() {
                Some(a.clone())
            } else {
                // Fallback: both go to Int64
                Some(LogicalType::Int64)
            }
        }

        // Float operations
        (LogicalType::Float, LogicalType::Float) => Some(LogicalType::Float),
        (LogicalType::Double, LogicalType::Double) => Some(LogicalType::Double),
        (LogicalType::Float, LogicalType::Double) | (LogicalType::Double, LogicalType::Float) => {
            Some(LogicalType::Double)
        }

        // Integer + float -> float
        (a, LogicalType::Float) if a.is_integer() => Some(LogicalType::Float),
        (LogicalType::Float, b) if b.is_integer() => Some(LogicalType::Float),
        (a, LogicalType::Double) if a.is_integer() => Some(LogicalType::Double),
        (LogicalType::Double, b) if b.is_integer() => Some(LogicalType::Double),

        // Interval arithmetic
        (LogicalType::Interval, LogicalType::Interval) => Some(LogicalType::Interval),

        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_type_zero_cost() {
        assert_eq!(
            implicit_cast_cost(&LogicalType::Int64, &LogicalType::Int64),
            Some(0)
        );
    }

    #[test]
    fn integer_widening() {
        assert_eq!(
            implicit_cast_cost(&LogicalType::Int8, &LogicalType::Int16),
            Some(1)
        );
        assert_eq!(
            implicit_cast_cost(&LogicalType::Int8, &LogicalType::Int64),
            Some(3)
        );
        assert_eq!(
            implicit_cast_cost(&LogicalType::Int32, &LogicalType::Int64),
            Some(1)
        );
    }

    #[test]
    fn no_narrowing() {
        assert_eq!(
            implicit_cast_cost(&LogicalType::Int64, &LogicalType::Int32),
            None
        );
        assert_eq!(
            implicit_cast_cost(&LogicalType::Int32, &LogicalType::Int16),
            None
        );
    }

    #[test]
    fn integer_to_float() {
        assert!(implicit_cast_cost(&LogicalType::Int32, &LogicalType::Double).is_some());
        assert!(implicit_cast_cost(&LogicalType::Int64, &LogicalType::Float).is_some());
    }

    #[test]
    fn float_widening() {
        assert_eq!(
            implicit_cast_cost(&LogicalType::Float, &LogicalType::Double),
            Some(1)
        );
        assert_eq!(
            implicit_cast_cost(&LogicalType::Double, &LogicalType::Float),
            None
        );
    }

    #[test]
    fn anything_to_string() {
        assert!(implicit_cast_cost(&LogicalType::Int64, &LogicalType::String).is_some());
        assert!(implicit_cast_cost(&LogicalType::Bool, &LogicalType::String).is_some());
    }

    #[test]
    fn comparable_same_type() {
        assert!(are_comparable(&LogicalType::Int64, &LogicalType::Int64));
        assert!(are_comparable(&LogicalType::String, &LogicalType::String));
    }

    #[test]
    fn comparable_numerics() {
        assert!(are_comparable(&LogicalType::Int32, &LogicalType::Float));
        assert!(are_comparable(&LogicalType::UInt64, &LogicalType::Double));
    }

    #[test]
    fn not_comparable() {
        assert!(!are_comparable(&LogicalType::String, &LogicalType::Int64));
        assert!(!are_comparable(
            &LogicalType::Bool,
            &LogicalType::List(Box::new(LogicalType::Int64))
        ));
    }

    #[test]
    fn arithmetic_same_type() {
        assert_eq!(
            arithmetic_result_type(&LogicalType::Int64, &LogicalType::Int64),
            Some(LogicalType::Int64)
        );
        assert_eq!(
            arithmetic_result_type(&LogicalType::Double, &LogicalType::Double),
            Some(LogicalType::Double)
        );
    }

    #[test]
    fn arithmetic_widening() {
        assert_eq!(
            arithmetic_result_type(&LogicalType::Int32, &LogicalType::Int64),
            Some(LogicalType::Int64)
        );
        assert_eq!(
            arithmetic_result_type(&LogicalType::Float, &LogicalType::Double),
            Some(LogicalType::Double)
        );
    }

    #[test]
    fn arithmetic_int_float() {
        assert_eq!(
            arithmetic_result_type(&LogicalType::Int32, &LogicalType::Double),
            Some(LogicalType::Double)
        );
    }

    #[test]
    fn arithmetic_not_defined() {
        assert_eq!(
            arithmetic_result_type(&LogicalType::String, &LogicalType::Int64),
            None
        );
    }
}

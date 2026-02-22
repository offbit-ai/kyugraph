//! kyu-copy: COPY FROM readers for CSV, Parquet, and Arrow IPC formats.
//!
//! Provides a unified `DataReader` trait and `open_reader()` factory that
//! auto-detects the file format by extension.

mod csv_reader;
mod parquet_reader;
mod arrow_reader;

pub use csv_reader::CsvReader;
pub use parquet_reader::ParquetReader;
pub use arrow_reader::ArrowIpcReader;

use kyu_common::{KyuError, KyuResult};
use kyu_types::{LogicalType, TypedValue};

/// Trait for reading rows from an external data source.
///
/// Implementations yield one row at a time as `Vec<TypedValue>`,
/// matching the target table's column schema.
pub trait DataReader: Iterator<Item = KyuResult<Vec<TypedValue>>> {
    /// The expected column types for each row.
    fn schema(&self) -> &[LogicalType];
}

/// Open a data reader for the given file path, auto-detecting format by extension.
///
/// Supported extensions: `.csv`, `.tsv`, `.parquet`, `.arrow`, `.ipc`.
pub fn open_reader(path: &str, schema: &[LogicalType]) -> KyuResult<Box<dyn DataReader>> {
    let lower = path.to_lowercase();
    if lower.ends_with(".parquet") {
        Ok(Box::new(ParquetReader::open(path, schema)?))
    } else if lower.ends_with(".arrow") || lower.ends_with(".ipc") {
        Ok(Box::new(ArrowIpcReader::open(path, schema)?))
    } else {
        // Default to CSV (covers .csv, .tsv, and anything else).
        Ok(Box::new(CsvReader::open(path, schema)?))
    }
}

/// Parse a string field into a TypedValue based on the target LogicalType.
pub(crate) fn parse_field(field: &str, ty: &LogicalType) -> KyuResult<TypedValue> {
    if field.is_empty() {
        return Ok(TypedValue::Null);
    }
    match ty {
        LogicalType::Int8 => field
            .parse::<i8>()
            .map(TypedValue::Int8)
            .map_err(|e| KyuError::Copy(format!("cannot parse '{field}' as INT8: {e}"))),
        LogicalType::Int16 => field
            .parse::<i16>()
            .map(TypedValue::Int16)
            .map_err(|e| KyuError::Copy(format!("cannot parse '{field}' as INT16: {e}"))),
        LogicalType::Int32 => field
            .parse::<i32>()
            .map(TypedValue::Int32)
            .map_err(|e| KyuError::Copy(format!("cannot parse '{field}' as INT32: {e}"))),
        LogicalType::Int64 | LogicalType::Serial => field
            .parse::<i64>()
            .map(TypedValue::Int64)
            .map_err(|e| KyuError::Copy(format!("cannot parse '{field}' as INT64: {e}"))),
        LogicalType::Float => field
            .parse::<f32>()
            .map(TypedValue::Float)
            .map_err(|e| KyuError::Copy(format!("cannot parse '{field}' as FLOAT: {e}"))),
        LogicalType::Double => field
            .parse::<f64>()
            .map(TypedValue::Double)
            .map_err(|e| KyuError::Copy(format!("cannot parse '{field}' as DOUBLE: {e}"))),
        LogicalType::Bool => match field.to_lowercase().as_str() {
            "true" | "1" | "t" | "yes" => Ok(TypedValue::Bool(true)),
            "false" | "0" | "f" | "no" => Ok(TypedValue::Bool(false)),
            _ => Err(KyuError::Copy(format!("cannot parse '{field}' as BOOL"))),
        },
        LogicalType::String => Ok(TypedValue::String(smol_str::SmolStr::new(field))),
        _ => Err(KyuError::Copy(format!(
            "unsupported type {} for import",
            ty.type_name()
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_field_types() {
        assert_eq!(parse_field("42", &LogicalType::Int64).unwrap(), TypedValue::Int64(42));
        assert_eq!(parse_field("3.14", &LogicalType::Double).unwrap(), TypedValue::Double(3.14));
        assert_eq!(parse_field("true", &LogicalType::Bool).unwrap(), TypedValue::Bool(true));
        assert_eq!(
            parse_field("hello", &LogicalType::String).unwrap(),
            TypedValue::String(smol_str::SmolStr::new("hello"))
        );
        assert_eq!(parse_field("", &LogicalType::Int64).unwrap(), TypedValue::Null);
    }

    #[test]
    fn parse_field_errors() {
        assert!(parse_field("not_a_number", &LogicalType::Int64).is_err());
        assert!(parse_field("not_bool", &LogicalType::Bool).is_err());
    }
}

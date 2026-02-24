//! Parquet file reader.

use std::fs::File;

use arrow::array::{
    Array, AsArray, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
    Int64Array,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use kyu_common::{KyuError, KyuResult};
use kyu_types::{LogicalType, TypedValue};

use crate::DataReader;

/// Reads rows from a Parquet file, converting Arrow arrays to TypedValue rows.
pub struct ParquetReader {
    schema: Vec<LogicalType>,
    /// Pre-loaded rows from all row groups.
    rows: std::vec::IntoIter<Vec<TypedValue>>,
}

impl ParquetReader {
    /// Open a Parquet file and read all row groups into memory.
    pub fn open(path: &str, schema: &[LogicalType]) -> KyuResult<Self> {
        let file =
            File::open(path).map_err(|e| KyuError::Copy(format!("cannot open '{path}': {e}")))?;

        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| KyuError::Copy(format!("invalid Parquet file '{path}': {e}")))?;

        let reader = builder
            .build()
            .map_err(|e| KyuError::Copy(format!("cannot read Parquet '{path}': {e}")))?;

        let mut all_rows = Vec::new();

        for batch_result in reader {
            let batch =
                batch_result.map_err(|e| KyuError::Copy(format!("Parquet batch error: {e}")))?;

            let num_rows = batch.num_rows();
            let num_cols = schema.len().min(batch.num_columns());

            for row_idx in 0..num_rows {
                let mut row = Vec::with_capacity(schema.len());
                for (col_idx, col_type) in schema.iter().enumerate().take(num_cols) {
                    let col = batch.column(col_idx);
                    let value = extract_value(col.as_ref(), row_idx, col_type)?;
                    row.push(value);
                }
                // Pad with Null if Parquet has fewer columns than schema.
                for _ in num_cols..schema.len() {
                    row.push(TypedValue::Null);
                }
                all_rows.push(row);
            }
        }

        Ok(Self {
            schema: schema.to_vec(),
            rows: all_rows.into_iter(),
        })
    }
}

impl DataReader for ParquetReader {
    fn schema(&self) -> &[LogicalType] {
        &self.schema
    }
}

impl Iterator for ParquetReader {
    type Item = KyuResult<Vec<TypedValue>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.rows.next().map(Ok)
    }
}

/// Extract a TypedValue from an Arrow array at the given row index.
fn extract_value(
    array: &dyn Array,
    row: usize,
    target_type: &LogicalType,
) -> KyuResult<TypedValue> {
    if array.is_null(row) {
        return Ok(TypedValue::Null);
    }

    match target_type {
        LogicalType::Int8 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int8Array>()
                .ok_or_else(|| KyuError::Copy("expected Int8 column in Parquet".into()))?;
            Ok(TypedValue::Int8(arr.value(row)))
        }
        LogicalType::Int16 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or_else(|| KyuError::Copy("expected Int16 column in Parquet".into()))?;
            Ok(TypedValue::Int16(arr.value(row)))
        }
        LogicalType::Int32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| KyuError::Copy("expected Int32 column in Parquet".into()))?;
            Ok(TypedValue::Int32(arr.value(row)))
        }
        LogicalType::Int64 | LogicalType::Serial => {
            let arr = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| KyuError::Copy("expected Int64 column in Parquet".into()))?;
            Ok(TypedValue::Int64(arr.value(row)))
        }
        LogicalType::Float => {
            let arr = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| KyuError::Copy("expected Float32 column in Parquet".into()))?;
            Ok(TypedValue::Float(arr.value(row)))
        }
        LogicalType::Double => {
            let arr = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| KyuError::Copy("expected Float64 column in Parquet".into()))?;
            Ok(TypedValue::Double(arr.value(row)))
        }
        LogicalType::Bool => {
            let arr = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| KyuError::Copy("expected Boolean column in Parquet".into()))?;
            Ok(TypedValue::Bool(arr.value(row)))
        }
        LogicalType::String => {
            let arr = array.as_string::<i32>();
            Ok(TypedValue::String(smol_str::SmolStr::new(arr.value(row))))
        }
        _ => Err(KyuError::Copy(format!(
            "unsupported type {} for Parquet import",
            target_type.type_name()
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use std::sync::Arc;

    fn write_test_parquet(dir: &std::path::Path, name: &str) -> String {
        let path = dir.join(name);
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let ids = Int64Array::from(vec![1, 2, 3]);
        let names = StringArray::from(vec!["Alice", "Bob", "Charlie"]);
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(ids), Arc::new(names)])
            .unwrap();

        let file = File::create(&path).unwrap();
        let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        path.to_str().unwrap().to_string()
    }

    #[test]
    fn read_parquet_basic() {
        let dir = std::env::temp_dir().join("kyu_parquet_reader_test");
        let _ = std::fs::create_dir_all(&dir);
        let path = write_test_parquet(&dir, "test.parquet");

        let schema = vec![LogicalType::Int64, LogicalType::String];
        let reader = ParquetReader::open(&path, &schema).unwrap();
        let rows: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();

        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0][0], TypedValue::Int64(1));
        assert_eq!(
            rows[0][1],
            TypedValue::String(smol_str::SmolStr::new("Alice"))
        );
        assert_eq!(rows[2][0], TypedValue::Int64(3));
        assert_eq!(
            rows[2][1],
            TypedValue::String(smol_str::SmolStr::new("Charlie"))
        );

        let _ = std::fs::remove_dir_all(&dir);
    }
}

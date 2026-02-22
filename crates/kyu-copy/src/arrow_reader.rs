//! Arrow IPC file reader.

use std::fs::File;

use arrow::array::{
    Array, AsArray, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
    Int64Array,
};
use arrow::ipc::reader::FileReader;

use kyu_common::{KyuError, KyuResult};
use kyu_types::{LogicalType, TypedValue};

use crate::DataReader;

/// Reads rows from an Arrow IPC file (.arrow / .ipc).
pub struct ArrowIpcReader {
    schema: Vec<LogicalType>,
    rows: std::vec::IntoIter<Vec<TypedValue>>,
}

impl ArrowIpcReader {
    /// Open an Arrow IPC file and read all record batches into memory.
    pub fn open(path: &str, schema: &[LogicalType]) -> KyuResult<Self> {
        let file = File::open(path)
            .map_err(|e| KyuError::Copy(format!("cannot open '{path}': {e}")))?;

        let reader = FileReader::try_new(file, None)
            .map_err(|e| KyuError::Copy(format!("invalid Arrow IPC file '{path}': {e}")))?;

        let mut all_rows = Vec::new();

        for batch_result in reader {
            let batch = batch_result
                .map_err(|e| KyuError::Copy(format!("Arrow IPC batch error: {e}")))?;

            let num_rows = batch.num_rows();
            let num_cols = schema.len().min(batch.num_columns());

            for row_idx in 0..num_rows {
                let mut row = Vec::with_capacity(schema.len());
                for (col_idx, col_type) in schema.iter().enumerate().take(num_cols) {
                    let col = batch.column(col_idx);
                    let value = extract_value(col.as_ref(), row_idx, col_type)?;
                    row.push(value);
                }
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

impl DataReader for ArrowIpcReader {
    fn schema(&self) -> &[LogicalType] {
        &self.schema
    }
}

impl Iterator for ArrowIpcReader {
    type Item = KyuResult<Vec<TypedValue>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.rows.next().map(Ok)
    }
}

/// Extract a TypedValue from an Arrow array at the given row index.
fn extract_value(array: &dyn Array, row: usize, target_type: &LogicalType) -> KyuResult<TypedValue> {
    if array.is_null(row) {
        return Ok(TypedValue::Null);
    }

    match target_type {
        LogicalType::Int8 => {
            let arr = array.as_any().downcast_ref::<Int8Array>()
                .ok_or_else(|| KyuError::Copy("expected Int8 column in Arrow IPC".into()))?;
            Ok(TypedValue::Int8(arr.value(row)))
        }
        LogicalType::Int16 => {
            let arr = array.as_any().downcast_ref::<Int16Array>()
                .ok_or_else(|| KyuError::Copy("expected Int16 column in Arrow IPC".into()))?;
            Ok(TypedValue::Int16(arr.value(row)))
        }
        LogicalType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>()
                .ok_or_else(|| KyuError::Copy("expected Int32 column in Arrow IPC".into()))?;
            Ok(TypedValue::Int32(arr.value(row)))
        }
        LogicalType::Int64 | LogicalType::Serial => {
            let arr = array.as_any().downcast_ref::<Int64Array>()
                .ok_or_else(|| KyuError::Copy("expected Int64 column in Arrow IPC".into()))?;
            Ok(TypedValue::Int64(arr.value(row)))
        }
        LogicalType::Float => {
            let arr = array.as_any().downcast_ref::<Float32Array>()
                .ok_or_else(|| KyuError::Copy("expected Float32 column in Arrow IPC".into()))?;
            Ok(TypedValue::Float(arr.value(row)))
        }
        LogicalType::Double => {
            let arr = array.as_any().downcast_ref::<Float64Array>()
                .ok_or_else(|| KyuError::Copy("expected Float64 column in Arrow IPC".into()))?;
            Ok(TypedValue::Double(arr.value(row)))
        }
        LogicalType::Bool => {
            let arr = array.as_any().downcast_ref::<BooleanArray>()
                .ok_or_else(|| KyuError::Copy("expected Boolean column in Arrow IPC".into()))?;
            Ok(TypedValue::Bool(arr.value(row)))
        }
        LogicalType::String => {
            let arr = array.as_string::<i32>();
            Ok(TypedValue::String(smol_str::SmolStr::new(arr.value(row))))
        }
        _ => Err(KyuError::Copy(format!(
            "unsupported type {} for Arrow IPC import",
            target_type.type_name()
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ipc::writer::FileWriter;
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    fn write_test_arrow(dir: &std::path::Path, name: &str) -> String {
        let path = dir.join(name);
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let ids = Int64Array::from(vec![10, 20, 30]);
        let names = StringArray::from(vec!["X", "Y", "Z"]);
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(ids), Arc::new(names)],
        )
        .unwrap();

        let file = File::create(&path).unwrap();
        let mut writer = FileWriter::try_new(file, &schema).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();

        path.to_str().unwrap().to_string()
    }

    #[test]
    fn read_arrow_ipc_basic() {
        let dir = std::env::temp_dir().join("kyu_arrow_reader_test");
        let _ = std::fs::create_dir_all(&dir);
        let path = write_test_arrow(&dir, "test.arrow");

        let schema = vec![LogicalType::Int64, LogicalType::String];
        let reader = ArrowIpcReader::open(&path, &schema).unwrap();
        let rows: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();

        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0][0], TypedValue::Int64(10));
        assert_eq!(rows[0][1], TypedValue::String(smol_str::SmolStr::new("X")));
        assert_eq!(rows[2][0], TypedValue::Int64(30));

        let _ = std::fs::remove_dir_all(&dir);
    }
}

//! CSV/TSV file reader.

use std::fs::File;
use std::io::BufReader;

use kyu_common::{KyuError, KyuResult};
use kyu_types::{LogicalType, TypedValue};

use crate::{DataReader, parse_field};

/// Reads rows from a CSV file, parsing each field according to the target schema.
pub struct CsvReader {
    reader: csv::Reader<BufReader<File>>,
    schema: Vec<LogicalType>,
}

impl CsvReader {
    /// Open a CSV file with the given target column schema.
    pub fn open(path: &str, schema: &[LogicalType]) -> KyuResult<Self> {
        let file = File::open(path)
            .map_err(|e| KyuError::Copy(format!("cannot open '{path}': {e}")))?;
        let reader = csv::ReaderBuilder::new()
            .has_headers(true)
            .from_reader(BufReader::new(file));
        Ok(Self {
            reader,
            schema: schema.to_vec(),
        })
    }
}

impl DataReader for CsvReader {
    fn schema(&self) -> &[LogicalType] {
        &self.schema
    }
}

impl Iterator for CsvReader {
    type Item = KyuResult<Vec<TypedValue>>;

    fn next(&mut self) -> Option<Self::Item> {
        let record = match self.reader.records().next()? {
            Ok(r) => r,
            Err(e) => return Some(Err(KyuError::Copy(format!("CSV parse error: {e}")))),
        };

        let mut values = Vec::with_capacity(self.schema.len());
        for (i, ty) in self.schema.iter().enumerate() {
            let field = record.get(i).unwrap_or("");
            match parse_field(field, ty) {
                Ok(v) => values.push(v),
                Err(e) => return Some(Err(e)),
            }
        }
        Some(Ok(values))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn write_csv(dir: &std::path::Path, name: &str, content: &str) -> String {
        let path = dir.join(name);
        let mut f = File::create(&path).unwrap();
        f.write_all(content.as_bytes()).unwrap();
        path.to_str().unwrap().to_string()
    }

    #[test]
    fn read_csv_basic() {
        let dir = std::env::temp_dir().join("kyu_csv_reader_test");
        let _ = std::fs::create_dir_all(&dir);
        let path = write_csv(&dir, "basic.csv", "id,name\n1,Alice\n2,Bob\n");

        let schema = vec![LogicalType::Int64, LogicalType::String];
        let reader = CsvReader::open(&path, &schema).unwrap();
        let rows: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0], TypedValue::Int64(1));
        assert_eq!(rows[0][1], TypedValue::String(smol_str::SmolStr::new("Alice")));
        assert_eq!(rows[1][0], TypedValue::Int64(2));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn read_csv_with_nulls() {
        let dir = std::env::temp_dir().join("kyu_csv_null_test");
        let _ = std::fs::create_dir_all(&dir);
        let path = write_csv(&dir, "nulls.csv", "id,value\n1,\n2,42\n");

        let schema = vec![LogicalType::Int64, LogicalType::Int64];
        let reader = CsvReader::open(&path, &schema).unwrap();
        let rows: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();

        assert_eq!(rows[0][1], TypedValue::Null);
        assert_eq!(rows[1][1], TypedValue::Int64(42));

        let _ = std::fs::remove_dir_all(&dir);
    }
}

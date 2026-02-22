//! Kafka topic reader for streaming ingestion.
//!
//! Consumes JSON-encoded messages from a Kafka topic and yields rows
//! matching the target table schema. Each JSON message must be a flat
//! object whose keys correspond to column names.
//!
//! Usage (via Cypher):
//! ```cypher
//! COPY Person FROM 'kafka://broker:9092/topic_name'
//! ```

use std::time::Duration;

use kyu_common::{KyuError, KyuResult};
use kyu_types::{LogicalType, TypedValue};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message;
use smol_str::SmolStr;

use crate::{parse_field, DataReader};

/// Default timeout for polling a single message.
const POLL_TIMEOUT: Duration = Duration::from_secs(5);

/// Maximum number of messages to consume per batch.
const DEFAULT_BATCH_SIZE: usize = 10_000;

/// Reads rows from a Kafka topic.
///
/// Each consumed message is expected to be a JSON object with keys matching
/// the `column_names` provided at construction. Missing keys yield NULL.
pub struct KafkaReader {
    consumer: BaseConsumer,
    schema: Vec<LogicalType>,
    column_names: Vec<SmolStr>,
    remaining: usize,
    finished: bool,
}

/// Parsed Kafka connection info from a URL like `kafka://broker:9092/topic`.
pub struct KafkaUrl {
    pub brokers: String,
    pub topic: String,
    pub group_id: Option<String>,
}

/// Parse a Kafka URL: `kafka://host:port/topic[?group_id=xxx]`
pub fn parse_kafka_url(url: &str) -> KyuResult<KafkaUrl> {
    let stripped = url
        .strip_prefix("kafka://")
        .ok_or_else(|| KyuError::Copy("Kafka URL must start with 'kafka://'".into()))?;

    // Split off query string.
    let (path, query) = stripped.split_once('?').unwrap_or((stripped, ""));
    let group_id = query
        .split('&')
        .find_map(|kv| kv.strip_prefix("group_id="))
        .map(String::from);

    // Split broker from topic.
    let (brokers, topic) = path
        .rsplit_once('/')
        .ok_or_else(|| KyuError::Copy("Kafka URL must contain /topic after broker".into()))?;

    if brokers.is_empty() || topic.is_empty() {
        return Err(KyuError::Copy(
            "Kafka URL must have non-empty broker and topic".into(),
        ));
    }

    Ok(KafkaUrl {
        brokers: brokers.to_string(),
        topic: topic.to_string(),
        group_id,
    })
}

impl KafkaReader {
    /// Connect to a Kafka topic and prepare for consumption.
    ///
    /// - `brokers`: comma-separated Kafka broker addresses (e.g., "localhost:9092")
    /// - `topic`: Kafka topic to consume from
    /// - `group_id`: consumer group ID
    /// - `schema`: target column types
    /// - `column_names`: column names corresponding to JSON keys
    /// - `batch_size`: maximum messages to consume (0 = use default)
    pub fn open(
        brokers: &str,
        topic: &str,
        group_id: &str,
        schema: &[LogicalType],
        column_names: &[SmolStr],
        batch_size: usize,
    ) -> KyuResult<Self> {
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("group.id", group_id)
            .set("auto.offset.reset", "earliest")
            .set("enable.auto.commit", "true")
            .create()
            .map_err(|e| KyuError::Copy(format!("Kafka consumer error: {e}")))?;

        consumer
            .subscribe(&[topic])
            .map_err(|e| KyuError::Copy(format!("Kafka subscribe error: {e}")))?;

        let batch = if batch_size == 0 {
            DEFAULT_BATCH_SIZE
        } else {
            batch_size
        };

        Ok(Self {
            consumer,
            schema: schema.to_vec(),
            column_names: column_names.to_vec(),
            remaining: batch,
            finished: false,
        })
    }

    /// Open from a Kafka URL.
    pub fn from_url(
        url: &str,
        schema: &[LogicalType],
        column_names: &[SmolStr],
    ) -> KyuResult<Self> {
        let parsed = parse_kafka_url(url)?;
        let group_id = parsed
            .group_id
            .unwrap_or_else(|| format!("kyugraph-{}", parsed.topic));
        Self::open(
            &parsed.brokers,
            &parsed.topic,
            &group_id,
            schema,
            column_names,
            DEFAULT_BATCH_SIZE,
        )
    }

    /// Parse a JSON message payload into a row of TypedValues.
    fn parse_json_message(&self, payload: &[u8]) -> KyuResult<Vec<TypedValue>> {
        // Parse JSON with simd-json for performance.
        let mut buf = payload.to_vec();
        let value: simd_json::OwnedValue = simd_json::to_owned_value(&mut buf)
            .map_err(|e| KyuError::Copy(format!("JSON parse error: {e}")))?;

        let obj = match &value {
            simd_json::OwnedValue::Object(map) => map,
            _ => {
                return Err(KyuError::Copy(
                    "Kafka message must be a JSON object".into(),
                ))
            }
        };

        let mut row = Vec::with_capacity(self.schema.len());
        for (col_idx, col_type) in self.schema.iter().enumerate() {
            let col_name = &self.column_names[col_idx];
            let val = match obj.get(col_name.as_str()) {
                Some(json_val) => json_value_to_typed(json_val, col_type)?,
                None => TypedValue::Null,
            };
            row.push(val);
        }
        Ok(row)
    }
}

impl DataReader for KafkaReader {
    fn schema(&self) -> &[LogicalType] {
        &self.schema
    }
}

impl Iterator for KafkaReader {
    type Item = KyuResult<Vec<TypedValue>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished || self.remaining == 0 {
            return None;
        }

        // Poll for a message with timeout.
        match self.consumer.poll(POLL_TIMEOUT) {
            Some(Ok(msg)) => {
                self.remaining -= 1;
                match msg.payload() {
                    Some(payload) => Some(self.parse_json_message(payload)),
                    None => {
                        // Tombstone (null payload) → skip.
                        self.next()
                    }
                }
            }
            Some(Err(e)) => {
                self.finished = true;
                Some(Err(KyuError::Copy(format!("Kafka poll error: {e}"))))
            }
            None => {
                // Timeout — no more messages available.
                self.finished = true;
                None
            }
        }
    }
}

/// Convert a simd-json value to a TypedValue.
fn json_value_to_typed(
    val: &simd_json::OwnedValue,
    target: &LogicalType,
) -> KyuResult<TypedValue> {
    use simd_json::OwnedValue;

    if matches!(val, OwnedValue::Static(simd_json::StaticNode::Null)) {
        return Ok(TypedValue::Null);
    }

    match target {
        LogicalType::Bool => match val {
            OwnedValue::Static(simd_json::StaticNode::Bool(b)) => Ok(TypedValue::Bool(*b)),
            _ => Err(KyuError::Copy(format!("expected bool, got {val:?}"))),
        },
        LogicalType::Int8 => match val {
            OwnedValue::Static(simd_json::StaticNode::I64(n)) => Ok(TypedValue::Int8(*n as i8)),
            _ => Err(KyuError::Copy(format!("expected int8, got {val:?}"))),
        },
        LogicalType::Int16 => match val {
            OwnedValue::Static(simd_json::StaticNode::I64(n)) => Ok(TypedValue::Int16(*n as i16)),
            _ => Err(KyuError::Copy(format!("expected int16, got {val:?}"))),
        },
        LogicalType::Int32 => match val {
            OwnedValue::Static(simd_json::StaticNode::I64(n)) => Ok(TypedValue::Int32(*n as i32)),
            _ => Err(KyuError::Copy(format!("expected int32, got {val:?}"))),
        },
        LogicalType::Int64 | LogicalType::Serial => match val {
            OwnedValue::Static(simd_json::StaticNode::I64(n)) => Ok(TypedValue::Int64(*n)),
            OwnedValue::Static(simd_json::StaticNode::U64(n)) => Ok(TypedValue::Int64(*n as i64)),
            _ => Err(KyuError::Copy(format!("expected int64, got {val:?}"))),
        },
        LogicalType::Float => match val {
            OwnedValue::Static(simd_json::StaticNode::F64(f)) => Ok(TypedValue::Float(*f as f32)),
            OwnedValue::Static(simd_json::StaticNode::I64(n)) => {
                Ok(TypedValue::Float(*n as f32))
            }
            _ => Err(KyuError::Copy(format!("expected float, got {val:?}"))),
        },
        LogicalType::Double => match val {
            OwnedValue::Static(simd_json::StaticNode::F64(f)) => Ok(TypedValue::Double(*f)),
            OwnedValue::Static(simd_json::StaticNode::I64(n)) => {
                Ok(TypedValue::Double(*n as f64))
            }
            _ => Err(KyuError::Copy(format!("expected double, got {val:?}"))),
        },
        LogicalType::String => match val {
            OwnedValue::String(s) => Ok(TypedValue::String(SmolStr::new(s))),
            other => {
                // Coerce non-string to string representation.
                Ok(TypedValue::String(SmolStr::new(format!("{other}"))))
            }
        },
        _ => {
            // Fallback: convert to string via parse_field.
            let s = match val {
                OwnedValue::String(s) => s.clone(),
                other => format!("{other}"),
            };
            parse_field(&s, target)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_kafka_url_basic() {
        let url = parse_kafka_url("kafka://localhost:9092/my_topic").unwrap();
        assert_eq!(url.brokers, "localhost:9092");
        assert_eq!(url.topic, "my_topic");
        assert!(url.group_id.is_none());
    }

    #[test]
    fn parse_kafka_url_with_group() {
        let url =
            parse_kafka_url("kafka://broker1:9092,broker2:9092/events?group_id=ingest").unwrap();
        assert_eq!(url.brokers, "broker1:9092,broker2:9092");
        assert_eq!(url.topic, "events");
        assert_eq!(url.group_id.as_deref(), Some("ingest"));
    }

    #[test]
    fn parse_kafka_url_invalid() {
        assert!(parse_kafka_url("http://localhost:9092/topic").is_err());
        assert!(parse_kafka_url("kafka:///topic").is_err());
        assert!(parse_kafka_url("kafka://broker/").is_err());
    }

    #[test]
    fn json_value_bool() {
        let val = simd_json::OwnedValue::Static(simd_json::StaticNode::Bool(true));
        let result = json_value_to_typed(&val, &LogicalType::Bool).unwrap();
        assert_eq!(result, TypedValue::Bool(true));
    }

    #[test]
    fn json_value_int64() {
        let val = simd_json::OwnedValue::Static(simd_json::StaticNode::I64(42));
        let result = json_value_to_typed(&val, &LogicalType::Int64).unwrap();
        assert_eq!(result, TypedValue::Int64(42));
    }

    #[test]
    fn json_value_double() {
        let val = simd_json::OwnedValue::Static(simd_json::StaticNode::F64(3.14));
        let result = json_value_to_typed(&val, &LogicalType::Double).unwrap();
        assert_eq!(result, TypedValue::Double(3.14));
    }

    #[test]
    fn json_value_string() {
        let val = simd_json::OwnedValue::String("hello".to_string());
        let result = json_value_to_typed(&val, &LogicalType::String).unwrap();
        assert_eq!(result, TypedValue::String(SmolStr::new("hello")));
    }

    #[test]
    fn json_value_null() {
        let val = simd_json::OwnedValue::Static(simd_json::StaticNode::Null);
        let result = json_value_to_typed(&val, &LogicalType::Int64).unwrap();
        assert_eq!(result, TypedValue::Null);
    }

    #[test]
    fn json_value_int_to_double() {
        let val = simd_json::OwnedValue::Static(simd_json::StaticNode::I64(7));
        let result = json_value_to_typed(&val, &LogicalType::Double).unwrap();
        assert_eq!(result, TypedValue::Double(7.0));
    }

    #[test]
    fn json_parse_message() {
        let reader = KafkaReader {
            consumer: ClientConfig::new()
                .set("bootstrap.servers", "localhost:9092")
                .set("group.id", "test")
                .create()
                .unwrap(),
            schema: vec![LogicalType::Int64, LogicalType::String],
            column_names: vec![SmolStr::new("id"), SmolStr::new("name")],
            remaining: 100,
            finished: false,
        };

        let payload = br#"{"id": 42, "name": "Alice"}"#;
        let row = reader.parse_json_message(payload).unwrap();
        assert_eq!(row.len(), 2);
        assert_eq!(row[0], TypedValue::Int64(42));
        assert_eq!(row[1], TypedValue::String(SmolStr::new("Alice")));
    }

    #[test]
    fn json_parse_missing_field() {
        let reader = KafkaReader {
            consumer: ClientConfig::new()
                .set("bootstrap.servers", "localhost:9092")
                .set("group.id", "test")
                .create()
                .unwrap(),
            schema: vec![LogicalType::Int64, LogicalType::String],
            column_names: vec![SmolStr::new("id"), SmolStr::new("name")],
            remaining: 100,
            finished: false,
        };

        let payload = br#"{"id": 99}"#;
        let row = reader.parse_json_message(payload).unwrap();
        assert_eq!(row.len(), 2);
        assert_eq!(row[0], TypedValue::Int64(99));
        assert_eq!(row[1], TypedValue::Null);
    }
}

//! Arrow Flight gRPC server for KyuGraph.
//!
//! Exposes KyuGraph as an Arrow Flight endpoint. Clients send Cypher queries
//! as Flight tickets and receive results as Arrow RecordBatch streams.

use std::sync::Arc;

use arrow::array::{
    BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
    NullArray, StringBuilder,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

use kyu_executor::QueryResult;
use kyu_types::{LogicalType, TypedValue};

use crate::Database;

// ---- QueryResult → RecordBatch conversion ----

/// Map a KyuGraph LogicalType to an Arrow DataType.
fn logical_to_arrow(ty: &LogicalType) -> DataType {
    match ty {
        LogicalType::Bool => DataType::Boolean,
        LogicalType::Int8 => DataType::Int8,
        LogicalType::Int16 => DataType::Int16,
        LogicalType::Int32 => DataType::Int32,
        LogicalType::Int64 | LogicalType::Serial => DataType::Int64,
        LogicalType::Float => DataType::Float32,
        LogicalType::Double => DataType::Float64,
        LogicalType::String => DataType::Utf8,
        _ => DataType::Utf8, // fallback: serialize as string
    }
}

/// Convert a QueryResult into an Arrow RecordBatch.
///
/// Returns `None` if the result has zero columns (DDL/DML with no output).
pub fn to_record_batch(result: &QueryResult) -> Option<RecordBatch> {
    let nc = result.num_columns();
    if nc == 0 {
        return None;
    }
    let nr = result.num_rows();

    let fields: Vec<Field> = result
        .column_names
        .iter()
        .zip(&result.column_types)
        .map(|(name, ty)| Field::new(name.as_str(), logical_to_arrow(ty), true))
        .collect();
    let schema = Arc::new(Schema::new(fields));

    let columns: Vec<Arc<dyn arrow::array::Array>> = (0..nc)
        .map(|col_idx| build_arrow_column(result, col_idx, nr, &result.column_types[col_idx]))
        .collect();

    RecordBatch::try_new(schema, columns).ok()
}

/// Build a single Arrow array column from a QueryResult.
fn build_arrow_column(
    result: &QueryResult,
    col_idx: usize,
    num_rows: usize,
    ty: &LogicalType,
) -> Arc<dyn arrow::array::Array> {
    match ty {
        LogicalType::Bool => {
            let values: Vec<Option<bool>> = (0..num_rows)
                .map(|r| match &result.row(r)[col_idx] {
                    TypedValue::Bool(v) => Some(*v),
                    TypedValue::Null => None,
                    _ => None,
                })
                .collect();
            Arc::new(BooleanArray::from(values))
        }
        LogicalType::Int8 => {
            let values: Vec<Option<i8>> = (0..num_rows)
                .map(|r| match &result.row(r)[col_idx] {
                    TypedValue::Int8(v) => Some(*v),
                    TypedValue::Null => None,
                    _ => None,
                })
                .collect();
            Arc::new(Int8Array::from(values))
        }
        LogicalType::Int16 => {
            let values: Vec<Option<i16>> = (0..num_rows)
                .map(|r| match &result.row(r)[col_idx] {
                    TypedValue::Int16(v) => Some(*v),
                    TypedValue::Null => None,
                    _ => None,
                })
                .collect();
            Arc::new(Int16Array::from(values))
        }
        LogicalType::Int32 => {
            let values: Vec<Option<i32>> = (0..num_rows)
                .map(|r| match &result.row(r)[col_idx] {
                    TypedValue::Int32(v) => Some(*v),
                    TypedValue::Null => None,
                    _ => None,
                })
                .collect();
            Arc::new(Int32Array::from(values))
        }
        LogicalType::Int64 | LogicalType::Serial => {
            let values: Vec<Option<i64>> = (0..num_rows)
                .map(|r| match &result.row(r)[col_idx] {
                    TypedValue::Int64(v) => Some(*v),
                    TypedValue::Null => None,
                    _ => None,
                })
                .collect();
            Arc::new(Int64Array::from(values))
        }
        LogicalType::Float => {
            let values: Vec<Option<f32>> = (0..num_rows)
                .map(|r| match &result.row(r)[col_idx] {
                    TypedValue::Float(v) => Some(*v),
                    TypedValue::Null => None,
                    _ => None,
                })
                .collect();
            Arc::new(Float32Array::from(values))
        }
        LogicalType::Double => {
            let values: Vec<Option<f64>> = (0..num_rows)
                .map(|r| match &result.row(r)[col_idx] {
                    TypedValue::Double(v) => Some(*v),
                    TypedValue::Null => None,
                    _ => None,
                })
                .collect();
            Arc::new(Float64Array::from(values))
        }
        LogicalType::String => {
            let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 16);
            for r in 0..num_rows {
                match &result.row(r)[col_idx] {
                    TypedValue::String(s) => builder.append_value(s.as_str()),
                    TypedValue::Null => builder.append_null(),
                    other => builder.append_value(format!("{other:?}")),
                }
            }
            Arc::new(builder.finish())
        }
        _ => {
            // Fallback: null column for unsupported types.
            Arc::new(NullArray::new(num_rows))
        }
    }
}

// ---- Arrow Flight Service ----

/// KyuGraph Arrow Flight service.
///
/// Each `do_get` executes the Cypher query encoded in the ticket and streams
/// the result as Arrow RecordBatches. DDL and mutations are supported via
/// `do_action`.
pub struct KyuFlightService {
    db: Arc<Database>,
}

impl KyuFlightService {
    pub fn new(db: Arc<Database>) -> Self {
        Self { db }
    }

    #[allow(clippy::result_large_err)]
    fn execute_query(&self, cypher: &str) -> Result<QueryResult, Status> {
        let conn = self.db.connect();
        conn.query(cypher)
            .map_err(|e| Status::internal(format!("query error: {e}")))
    }
}

#[tonic::async_trait]
impl FlightService for KyuFlightService {
    type HandshakeStream = BoxStream<'static, Result<HandshakeResponse, Status>>;
    type ListFlightsStream = BoxStream<'static, Result<FlightInfo, Status>>;
    type DoGetStream = BoxStream<'static, Result<FlightData, Status>>;
    type DoPutStream = BoxStream<'static, Result<PutResult, Status>>;
    type DoExchangeStream = BoxStream<'static, Result<FlightData, Status>>;
    type DoActionStream = BoxStream<'static, Result<arrow_flight::Result, Status>>;
    type ListActionsStream = BoxStream<'static, Result<ActionType, Status>>;

    /// Execute a Cypher query and stream the result as Arrow RecordBatches.
    ///
    /// The ticket body is the UTF-8 encoded Cypher query string.
    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner();
        let cypher = String::from_utf8(ticket.ticket.to_vec())
            .map_err(|_| Status::invalid_argument("ticket must be valid UTF-8"))?;

        let result = self.execute_query(&cypher)?;

        let batches: Vec<RecordBatch> = match to_record_batch(&result) {
            Some(batch) => vec![batch],
            None => {
                // DDL/DML — return empty schema with zero rows.
                let schema = Arc::new(Schema::new(vec![Field::new(
                    "ok",
                    DataType::Boolean,
                    false,
                )]));
                vec![RecordBatch::try_new(
                    schema,
                    vec![Arc::new(BooleanArray::from(vec![true]))],
                )
                .unwrap()]
            }
        };

        let schema = batches[0].schema();
        let batch_stream = futures::stream::iter(batches.into_iter().map(Ok));
        let flight_stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(batch_stream)
            .map_err(|e| Status::internal(e.to_string()));

        Ok(Response::new(flight_stream.boxed()))
    }

    /// Execute DDL/DML statements via actions.
    ///
    /// Action type: "query". Body: UTF-8 Cypher string.
    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        let action = request.into_inner();

        match action.r#type.as_str() {
            "query" => {
                let cypher = String::from_utf8(action.body.to_vec())
                    .map_err(|_| Status::invalid_argument("body must be valid UTF-8"))?;
                self.execute_query(&cypher)?;
                let result = arrow_flight::Result {
                    body: bytes::Bytes::from("OK"),
                };
                let stream = futures::stream::once(async { Ok(result) });
                Ok(Response::new(stream.boxed()))
            }
            other => Err(Status::invalid_argument(format!(
                "unknown action type: {other}"
            ))),
        }
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        let actions = vec![Ok(ActionType {
            r#type: "query".into(),
            description: "Execute a Cypher DDL/DML statement".into(),
        })];
        Ok(Response::new(futures::stream::iter(actions).boxed()))
    }

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("handshake not supported"))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("list_flights not supported"))
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("get_flight_info not supported"))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("poll_flight_info not supported"))
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("get_schema not supported"))
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("do_put not supported"))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange not supported"))
    }
}

/// Start the Arrow Flight server.
///
/// Binds to `host:port` and serves until the process is terminated.
pub async fn serve_flight(db: Arc<Database>, host: &str, port: u16) -> Result<(), Box<dyn std::error::Error>> {
    let addr = format!("{host}:{port}").parse()?;
    let service = KyuFlightService::new(db);
    println!("KyuGraph Flight server listening on {addr}");
    Server::builder()
        .add_service(FlightServiceServer::new(service))
        .serve(addr)
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use kyu_types::LogicalType;
    use smol_str::SmolStr;

    #[test]
    fn to_record_batch_empty_columns() {
        let result = QueryResult::new(vec![], vec![]);
        assert!(to_record_batch(&result).is_none());
    }

    #[test]
    fn to_record_batch_int64() {
        let mut result = QueryResult::new(
            vec![SmolStr::new("x")],
            vec![LogicalType::Int64],
        );
        result.push_row(vec![TypedValue::Int64(42)]);
        result.push_row(vec![TypedValue::Int64(99)]);

        let batch = to_record_batch(&result).unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 1);

        let col = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(col.value(0), 42);
        assert_eq!(col.value(1), 99);
    }

    #[test]
    fn to_record_batch_mixed_types() {
        let mut result = QueryResult::new(
            vec![SmolStr::new("id"), SmolStr::new("name"), SmolStr::new("score")],
            vec![LogicalType::Int64, LogicalType::String, LogicalType::Double],
        );
        result.push_row(vec![
            TypedValue::Int64(1),
            TypedValue::String(SmolStr::new("Alice")),
            TypedValue::Double(95.5),
        ]);
        result.push_row(vec![
            TypedValue::Int64(2),
            TypedValue::Null,
            TypedValue::Double(87.3),
        ]);

        let batch = to_record_batch(&result).unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 3);

        // Check string column has a null.
        let name_col = batch.column(1);
        assert!(!name_col.is_null(0));
        assert!(name_col.is_null(1));
    }

    #[test]
    fn to_record_batch_bool_float() {
        let mut result = QueryResult::new(
            vec![SmolStr::new("active"), SmolStr::new("temp")],
            vec![LogicalType::Bool, LogicalType::Float],
        );
        result.push_row(vec![TypedValue::Bool(true), TypedValue::Float(3.14)]);

        let batch = to_record_batch(&result).unwrap();
        let bool_col = batch.column(0).as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(bool_col.value(0));

        let float_col = batch.column(1).as_any().downcast_ref::<Float32Array>().unwrap();
        assert!((float_col.value(0) - 3.14).abs() < 0.01);
    }

    #[test]
    fn roundtrip_via_database() {
        let db = Database::in_memory();
        let conn = db.connect();
        conn.query("CREATE NODE TABLE Person (id INT64, name STRING, PRIMARY KEY (id))")
            .unwrap();
        conn.query("CREATE (n:Person {id: 1, name: 'Alice'})").unwrap();
        conn.query("CREATE (n:Person {id: 2, name: 'Bob'})").unwrap();

        let result = conn.query("MATCH (p:Person) RETURN p.id, p.name").unwrap();
        let batch = to_record_batch(&result).unwrap();

        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.schema().fields().len(), 2);
        assert_eq!(batch.schema().field(0).data_type(), &DataType::Int64);
        assert_eq!(batch.schema().field(1).data_type(), &DataType::Utf8);
    }
}

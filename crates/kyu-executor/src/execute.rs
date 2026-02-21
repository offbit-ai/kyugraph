//! Execution driver — top-level entry point for query execution.

use kyu_binder::BoundStatement;
use kyu_common::{KyuError, KyuResult};
use kyu_planner::LogicalPlan;
use kyu_types::LogicalType;
use smol_str::SmolStr;

use crate::context::ExecutionContext;
use crate::mapper::map_plan;
use crate::result::QueryResult;

/// Execute a logical plan, collecting results into a `QueryResult`.
pub fn execute(
    plan: &LogicalPlan,
    output_schema: &[(SmolStr, LogicalType)],
    ctx: &ExecutionContext<'_>,
) -> KyuResult<QueryResult> {
    let mut physical = map_plan(plan)?;

    let column_names: Vec<SmolStr> = output_schema.iter().map(|(n, _)| n.clone()).collect();
    let column_types: Vec<LogicalType> = output_schema.iter().map(|(_, t)| t.clone()).collect();
    let mut result = QueryResult::new(column_names, column_types);

    while let Some(chunk) = physical.next(ctx)? {
        result.push_chunk(&chunk);
    }

    Ok(result)
}

/// Execute a bound statement end-to-end (plan + execute).
pub fn execute_statement(
    stmt: &BoundStatement,
    ctx: &ExecutionContext<'_>,
) -> KyuResult<QueryResult> {
    match stmt {
        BoundStatement::Query(query) => {
            let plan = kyu_planner::build_query_plan(query, &ctx.catalog)?;
            execute(&plan, &query.output_schema, ctx)
        }
        _ => Err(KyuError::NotImplemented(
            "non-query execution not yet supported".into(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::MockStorage;
    use kyu_binder::Binder;
    use kyu_catalog::{CatalogContent, NodeTableEntry, Property, RelTableEntry};
    use kyu_common::id::{PropertyId, TableId};
    use kyu_expression::FunctionRegistry;
    use kyu_types::TypedValue;

    fn make_catalog() -> CatalogContent {
        let mut catalog = CatalogContent::new();
        catalog
            .add_node_table(NodeTableEntry {
                table_id: TableId(0),
                name: SmolStr::new("Person"),
                properties: vec![
                    Property::new(PropertyId(0), "name", LogicalType::String, true),
                    Property::new(PropertyId(1), "age", LogicalType::Int64, false),
                ],
                primary_key_idx: 0,
                num_rows: 0,
                comment: None,
            })
            .unwrap();
        catalog
            .add_rel_table(RelTableEntry {
                table_id: TableId(1),
                name: SmolStr::new("KNOWS"),
                from_table_id: TableId(0),
                to_table_id: TableId(0),
                properties: vec![Property::new(
                    PropertyId(2),
                    "since",
                    LogicalType::Int64,
                    false,
                )],
                num_rows: 0,
                comment: None,
            })
            .unwrap();
        catalog
    }

    fn make_storage() -> MockStorage {
        let mut storage = MockStorage::new();
        // Person table: name, age columns (matching catalog property order).
        storage.insert_table(
            TableId(0),
            vec![
                vec![
                    TypedValue::String(SmolStr::new("Alice")),
                    TypedValue::Int64(25),
                ],
                vec![
                    TypedValue::String(SmolStr::new("Bob")),
                    TypedValue::Int64(30),
                ],
                vec![
                    TypedValue::String(SmolStr::new("Charlie")),
                    TypedValue::Int64(35),
                ],
                vec![
                    TypedValue::String(SmolStr::new("Diana")),
                    TypedValue::Int64(28),
                ],
            ],
        );
        // KNOWS table: src_name, dst_name, since (simplified — no internal IDs for now).
        storage.insert_table(
            TableId(1),
            vec![
                vec![
                    TypedValue::String(SmolStr::new("Alice")),
                    TypedValue::String(SmolStr::new("Bob")),
                    TypedValue::Int64(2020),
                ],
                vec![
                    TypedValue::String(SmolStr::new("Bob")),
                    TypedValue::String(SmolStr::new("Charlie")),
                    TypedValue::Int64(2021),
                ],
            ],
        );
        storage
    }

    fn run_query(cypher: &str) -> KyuResult<QueryResult> {
        let catalog = make_catalog();
        let storage = make_storage();
        let ctx = ExecutionContext::new(catalog.clone(), &storage);

        let parse_result = kyu_parser::parse(cypher);
        let stmt = parse_result
            .ast
            .ok_or_else(|| KyuError::Binder(format!("parse failed: {:?}", parse_result.errors)))?;
        let mut binder = Binder::new(catalog, FunctionRegistry::with_builtins());
        let bound = binder.bind(&stmt)?;
        execute_statement(&bound, &ctx)
    }

    #[test]
    fn return_literal() {
        let result = run_query("RETURN 1 AS x").unwrap();
        assert_eq!(result.num_rows(), 1);
        assert_eq!(result.rows[0], vec![TypedValue::Int64(1)]);
    }

    #[test]
    fn return_arithmetic() {
        let result = run_query("RETURN 1 + 2 AS sum").unwrap();
        assert_eq!(result.num_rows(), 1);
        assert_eq!(result.rows[0], vec![TypedValue::Int64(3)]);
    }

    #[test]
    fn return_multiple_columns() {
        let result = run_query("RETURN 'hello' AS greeting, 42 AS answer").unwrap();
        assert_eq!(result.num_rows(), 1);
        assert_eq!(result.num_columns(), 2);
        assert_eq!(result.rows[0][0], TypedValue::String(SmolStr::new("hello")));
        assert_eq!(result.rows[0][1], TypedValue::Int64(42));
    }

    #[test]
    fn return_null_is_null() {
        let result = run_query("RETURN null IS NULL AS t").unwrap();
        assert_eq!(result.num_rows(), 1);
        assert_eq!(result.rows[0], vec![TypedValue::Bool(true)]);
    }

    #[test]
    fn return_case_expression() {
        let result =
            run_query("RETURN CASE WHEN true THEN 'yes' ELSE 'no' END AS v").unwrap();
        assert_eq!(result.num_rows(), 1);
        assert_eq!(
            result.rows[0],
            vec![TypedValue::String(SmolStr::new("yes"))]
        );
    }

    #[test]
    fn unwind_list() {
        let result = run_query("UNWIND [1, 2, 3] AS x RETURN x").unwrap();
        assert_eq!(result.num_rows(), 3);
        assert_eq!(result.rows[0], vec![TypedValue::Int64(1)]);
        assert_eq!(result.rows[1], vec![TypedValue::Int64(2)]);
        assert_eq!(result.rows[2], vec![TypedValue::Int64(3)]);
    }
}

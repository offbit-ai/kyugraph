//! Table and column statistics for cost-based optimization.
//!
//! Statistics are derived from catalog metadata. `num_rows` comes directly
//! from the catalog; column-level statistics use heuristic defaults until
//! real histograms are collected.

use hashbrown::HashMap;
use kyu_catalog::CatalogContent;
use kyu_common::TableId;

/// Statistics for a single column.
#[derive(Clone, Debug)]
pub struct ColumnStatistics {
    /// Estimated number of distinct values.
    pub distinct_count: u64,
    /// Estimated number of null values.
    pub null_count: u64,
}

/// Statistics for a single table.
#[derive(Clone, Debug)]
pub struct TableStatistics {
    pub num_rows: u64,
    pub column_stats: Vec<ColumnStatistics>,
}

/// Aggregated statistics for all tables, keyed by TableId.
pub type StatsMap = HashMap<TableId, TableStatistics>;

/// Derive statistics for all tables from the catalog.
///
/// Uses heuristic defaults for column-level statistics:
/// - Primary key columns: NDV = num_rows (unique by definition)
/// - Other columns: NDV = max(1, num_rows / 10) as a rough estimate
/// - Null count: 0 (optimistic default)
pub fn derive_statistics(catalog: &CatalogContent) -> StatsMap {
    let mut stats = HashMap::new();

    for entry in catalog.node_tables() {
        let nr = entry.num_rows;
        let col_stats: Vec<ColumnStatistics> = entry
            .properties
            .iter()
            .enumerate()
            .map(|(i, _prop)| {
                let ndv = if i == entry.primary_key_idx {
                    nr // PK is unique
                } else {
                    nr.max(1) / 10 // heuristic
                };
                ColumnStatistics {
                    distinct_count: ndv.max(1),
                    null_count: 0,
                }
            })
            .collect();
        stats.insert(
            entry.table_id,
            TableStatistics {
                num_rows: nr,
                column_stats: col_stats,
            },
        );
    }

    for entry in catalog.rel_tables() {
        let nr = entry.num_rows;
        let col_stats: Vec<ColumnStatistics> = entry
            .properties
            .iter()
            .map(|_prop| ColumnStatistics {
                distinct_count: nr.max(1) / 10,
                null_count: 0,
            })
            .collect();
        stats.insert(
            entry.table_id,
            TableStatistics {
                num_rows: nr,
                column_stats: col_stats,
            },
        );
    }

    stats
}

/// Look up row count for a table, defaulting to 1000 if unknown.
pub fn row_count(stats: &StatsMap, table_id: TableId) -> f64 {
    stats
        .get(&table_id)
        .map(|s| s.num_rows as f64)
        .unwrap_or(1000.0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use kyu_catalog::{NodeTableEntry, Property, RelTableEntry};
    use kyu_types::LogicalType;
    use smol_str::SmolStr;

    fn make_catalog() -> CatalogContent {
        let mut catalog = CatalogContent::new();
        let tid0 = catalog.alloc_table_id();
        let pid0 = catalog.alloc_property_id();
        let pid1 = catalog.alloc_property_id();
        catalog
            .add_node_table(NodeTableEntry {
                table_id: tid0,
                name: SmolStr::new("Person"),
                properties: vec![
                    Property::new(pid0, "id", LogicalType::Int64, true),
                    Property::new(pid1, "name", LogicalType::String, false),
                ],
                primary_key_idx: 0,
                num_rows: 1000,
                comment: None,
            })
            .unwrap();
        let tid1 = catalog.alloc_table_id();
        let pid2 = catalog.alloc_property_id();
        catalog
            .add_rel_table(RelTableEntry {
                table_id: tid1,
                name: SmolStr::new("Knows"),
                from_table_id: tid0,
                to_table_id: tid0,
                properties: vec![Property::new(pid2, "since", LogicalType::Int64, false)],
                num_rows: 5000,
                comment: None,
            })
            .unwrap();
        catalog
    }

    #[test]
    fn derive_node_stats() {
        let catalog = make_catalog();
        let stats = derive_statistics(&catalog);
        let node_stats = &stats[&TableId(0)];
        assert_eq!(node_stats.num_rows, 1000);
        assert_eq!(node_stats.column_stats.len(), 2);
        // PK column: NDV == num_rows
        assert_eq!(node_stats.column_stats[0].distinct_count, 1000);
        // Non-PK: NDV = num_rows / 10
        assert_eq!(node_stats.column_stats[1].distinct_count, 100);
    }

    #[test]
    fn derive_rel_stats() {
        let catalog = make_catalog();
        let stats = derive_statistics(&catalog);
        let rel_stats = &stats[&TableId(1)];
        assert_eq!(rel_stats.num_rows, 5000);
        assert_eq!(rel_stats.column_stats.len(), 1);
        assert_eq!(rel_stats.column_stats[0].distinct_count, 500);
    }

    #[test]
    fn row_count_known() {
        let catalog = make_catalog();
        let stats = derive_statistics(&catalog);
        assert_eq!(row_count(&stats, TableId(0)), 1000.0);
    }

    #[test]
    fn row_count_unknown() {
        let stats = StatsMap::new();
        assert_eq!(row_count(&stats, TableId(99)), 1000.0);
    }

    #[test]
    fn empty_table_stats() {
        let mut catalog = CatalogContent::new();
        let tid = catalog.alloc_table_id();
        let pid = catalog.alloc_property_id();
        catalog
            .add_node_table(NodeTableEntry {
                table_id: tid,
                name: SmolStr::new("Empty"),
                properties: vec![Property::new(pid, "id", LogicalType::Int64, true)],
                primary_key_idx: 0,
                num_rows: 0,
                comment: None,
            })
            .unwrap();
        let stats = derive_statistics(&catalog);
        let s = &stats[&tid];
        assert_eq!(s.num_rows, 0);
        // NDV should be at least 1
        assert_eq!(s.column_stats[0].distinct_count, 1);
    }
}

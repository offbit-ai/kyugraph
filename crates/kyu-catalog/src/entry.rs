use kyu_common::{PropertyId, TableId};
use kyu_types::LogicalType;
use smol_str::SmolStr;

/// A single property (column) definition within a table.
#[derive(Clone, Debug, PartialEq)]
pub struct Property {
    pub id: PropertyId,
    pub name: SmolStr,
    pub data_type: LogicalType,
    pub is_primary_key: bool,
    pub default_value: Option<SmolStr>,
}

impl Property {
    pub fn new(
        id: PropertyId,
        name: impl Into<SmolStr>,
        data_type: LogicalType,
        is_primary_key: bool,
    ) -> Self {
        Self {
            id,
            name: name.into(),
            data_type,
            is_primary_key,
            default_value: None,
        }
    }

    pub fn with_default(mut self, default: impl Into<SmolStr>) -> Self {
        self.default_value = Some(default.into());
        self
    }
}

/// Schema entry for a node table.
#[derive(Clone, Debug, PartialEq)]
pub struct NodeTableEntry {
    pub table_id: TableId,
    pub name: SmolStr,
    pub properties: Vec<Property>,
    /// Index into `properties` for the primary key column.
    pub primary_key_idx: usize,
    pub num_rows: u64,
    pub comment: Option<SmolStr>,
}

impl NodeTableEntry {
    /// Get the primary key property.
    pub fn primary_key_property(&self) -> &Property {
        &self.properties[self.primary_key_idx]
    }

    /// Find a property by name (case-insensitive).
    pub fn find_property(&self, name: &str) -> Option<&Property> {
        let lower = name.to_lowercase();
        self.properties.iter().find(|p| p.name.to_lowercase() == lower)
    }

    /// Get the number of properties.
    pub fn num_properties(&self) -> usize {
        self.properties.len()
    }
}

/// Schema entry for a relationship table.
#[derive(Clone, Debug, PartialEq)]
pub struct RelTableEntry {
    pub table_id: TableId,
    pub name: SmolStr,
    /// TableId of the source node table.
    pub from_table_id: TableId,
    /// TableId of the destination node table.
    pub to_table_id: TableId,
    pub properties: Vec<Property>,
    pub num_rows: u64,
    pub comment: Option<SmolStr>,
}

impl RelTableEntry {
    /// Find a property by name (case-insensitive).
    pub fn find_property(&self, name: &str) -> Option<&Property> {
        let lower = name.to_lowercase();
        self.properties.iter().find(|p| p.name.to_lowercase() == lower)
    }

    /// Get the number of properties.
    pub fn num_properties(&self) -> usize {
        self.properties.len()
    }
}

/// A catalog entry is either a node table or a relationship table.
#[derive(Clone, Debug, PartialEq)]
pub enum CatalogEntry {
    NodeTable(NodeTableEntry),
    RelTable(RelTableEntry),
}

impl CatalogEntry {
    /// Get the table ID.
    pub fn table_id(&self) -> TableId {
        match self {
            Self::NodeTable(e) => e.table_id,
            Self::RelTable(e) => e.table_id,
        }
    }

    /// Get the table name.
    pub fn name(&self) -> &SmolStr {
        match self {
            Self::NodeTable(e) => &e.name,
            Self::RelTable(e) => &e.name,
        }
    }

    /// Get the properties.
    pub fn properties(&self) -> &[Property] {
        match self {
            Self::NodeTable(e) => &e.properties,
            Self::RelTable(e) => &e.properties,
        }
    }

    pub fn is_node_table(&self) -> bool {
        matches!(self, Self::NodeTable(_))
    }

    pub fn is_rel_table(&self) -> bool {
        matches!(self, Self::RelTable(_))
    }

    /// Get the inner node table entry, if this is one.
    pub fn as_node_table(&self) -> Option<&NodeTableEntry> {
        match self {
            Self::NodeTable(e) => Some(e),
            _ => None,
        }
    }

    /// Get the inner rel table entry, if this is one.
    pub fn as_rel_table(&self) -> Option<&RelTableEntry> {
        match self {
            Self::RelTable(e) => Some(e),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_property(id: u32, name: &str, ty: LogicalType, pk: bool) -> Property {
        Property::new(PropertyId(id), name, ty, pk)
    }

    fn sample_node_table() -> NodeTableEntry {
        NodeTableEntry {
            table_id: TableId(0),
            name: SmolStr::new("Person"),
            properties: vec![
                sample_property(0, "id", LogicalType::Serial, true),
                sample_property(1, "name", LogicalType::String, false),
                sample_property(2, "age", LogicalType::Int64, false),
            ],
            primary_key_idx: 0,
            num_rows: 0,
            comment: None,
        }
    }

    fn sample_rel_table() -> RelTableEntry {
        RelTableEntry {
            table_id: TableId(1),
            name: SmolStr::new("Knows"),
            from_table_id: TableId(0),
            to_table_id: TableId(0),
            properties: vec![sample_property(3, "since", LogicalType::Date, false)],
            num_rows: 0,
            comment: None,
        }
    }

    #[test]
    fn property_new() {
        let p = Property::new(PropertyId(0), "name", LogicalType::String, false);
        assert_eq!(p.name, "name");
        assert_eq!(p.data_type, LogicalType::String);
        assert!(!p.is_primary_key);
        assert!(p.default_value.is_none());
    }

    #[test]
    fn property_with_default() {
        let p = Property::new(PropertyId(0), "status", LogicalType::String, false)
            .with_default("active");
        assert_eq!(p.default_value.as_deref(), Some("active"));
    }

    #[test]
    fn node_table_primary_key() {
        let nt = sample_node_table();
        assert_eq!(nt.primary_key_property().name, "id");
        assert!(nt.primary_key_property().is_primary_key);
    }

    #[test]
    fn node_table_find_property() {
        let nt = sample_node_table();
        assert!(nt.find_property("name").is_some());
        assert!(nt.find_property("NAME").is_some()); // case-insensitive
        assert!(nt.find_property("nonexistent").is_none());
    }

    #[test]
    fn node_table_num_properties() {
        let nt = sample_node_table();
        assert_eq!(nt.num_properties(), 3);
    }

    #[test]
    fn rel_table_find_property() {
        let rt = sample_rel_table();
        assert!(rt.find_property("since").is_some());
        assert!(rt.find_property("SINCE").is_some());
        assert!(rt.find_property("missing").is_none());
    }

    #[test]
    fn rel_table_num_properties() {
        let rt = sample_rel_table();
        assert_eq!(rt.num_properties(), 1);
    }

    #[test]
    fn catalog_entry_table_id() {
        let ne = CatalogEntry::NodeTable(sample_node_table());
        assert_eq!(ne.table_id(), TableId(0));
        let re = CatalogEntry::RelTable(sample_rel_table());
        assert_eq!(re.table_id(), TableId(1));
    }

    #[test]
    fn catalog_entry_name() {
        let ne = CatalogEntry::NodeTable(sample_node_table());
        assert_eq!(ne.name().as_str(), "Person");
        let re = CatalogEntry::RelTable(sample_rel_table());
        assert_eq!(re.name().as_str(), "Knows");
    }

    #[test]
    fn catalog_entry_properties() {
        let ne = CatalogEntry::NodeTable(sample_node_table());
        assert_eq!(ne.properties().len(), 3);
    }

    #[test]
    fn catalog_entry_type_checks() {
        let ne = CatalogEntry::NodeTable(sample_node_table());
        assert!(ne.is_node_table());
        assert!(!ne.is_rel_table());

        let re = CatalogEntry::RelTable(sample_rel_table());
        assert!(re.is_rel_table());
        assert!(!re.is_node_table());
    }

    #[test]
    fn catalog_entry_as_node_table() {
        let ne = CatalogEntry::NodeTable(sample_node_table());
        assert!(ne.as_node_table().is_some());
        assert!(ne.as_rel_table().is_none());
    }

    #[test]
    fn catalog_entry_as_rel_table() {
        let re = CatalogEntry::RelTable(sample_rel_table());
        assert!(re.as_rel_table().is_some());
        assert!(re.as_node_table().is_none());
    }

    #[test]
    fn catalog_entry_clone_eq() {
        let a = CatalogEntry::NodeTable(sample_node_table());
        let b = a.clone();
        assert_eq!(a, b);
    }
}

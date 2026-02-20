use std::collections::HashMap;
use std::sync::RwLock;

use kyu_common::{KyuError, KyuResult, PropertyId, TableId};
use smol_str::SmolStr;

use crate::entry::{CatalogEntry, NodeTableEntry, Property, RelTableEntry};

/// The inner state of the catalog: a versioned collection of table entries.
///
/// Following Kuzu's dual-version pattern: the `Catalog` holds a read snapshot
/// and a mutable write version. `begin_write()` clones the read version;
/// `commit_write()` promotes the write version to read.
///
/// Internally uses HashMap indexes for O(1) lookup by name and by table ID,
/// avoiding linear scans and per-entry `to_lowercase()` allocations.
#[derive(Clone, Debug)]
pub struct CatalogContent {
    /// Primary storage: TableId -> CatalogEntry.
    entries: HashMap<TableId, CatalogEntry>,
    /// Index: lowercased name -> TableId for O(1) case-insensitive name lookup.
    name_index: HashMap<SmolStr, TableId>,
    pub next_table_id: u64,
    pub next_property_id: u32,
    pub version: u64,
}

impl CatalogContent {
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
            name_index: HashMap::new(),
            next_table_id: 0,
            next_property_id: 0,
            version: 0,
        }
    }

    /// Allocate a new table ID.
    pub fn alloc_table_id(&mut self) -> TableId {
        let id = TableId(self.next_table_id);
        self.next_table_id += 1;
        id
    }

    /// Allocate a new property ID.
    pub fn alloc_property_id(&mut self) -> PropertyId {
        let id = PropertyId(self.next_property_id);
        self.next_property_id += 1;
        id
    }

    /// Find an entry by name (case-insensitive). O(1).
    pub fn find_by_name(&self, name: &str) -> Option<&CatalogEntry> {
        let lower = SmolStr::new(name.to_lowercase());
        let tid = self.name_index.get(&lower)?;
        self.entries.get(tid)
    }

    /// Find an entry by table ID. O(1).
    pub fn find_by_id(&self, table_id: TableId) -> Option<&CatalogEntry> {
        self.entries.get(&table_id)
    }

    /// Check if a table name already exists (case-insensitive). O(1).
    pub fn contains_name(&self, name: &str) -> bool {
        let lower = SmolStr::new(name.to_lowercase());
        self.name_index.contains_key(&lower)
    }

    /// Get all node table entries.
    pub fn node_tables(&self) -> Vec<&NodeTableEntry> {
        self.entries
            .values()
            .filter_map(|e| e.as_node_table())
            .collect()
    }

    /// Get all relationship table entries.
    pub fn rel_tables(&self) -> Vec<&RelTableEntry> {
        self.entries
            .values()
            .filter_map(|e| e.as_rel_table())
            .collect()
    }

    /// Get the total number of entries.
    pub fn num_tables(&self) -> usize {
        self.entries.len()
    }

    /// Remove an entry by table ID. Returns the removed entry, if found. O(1).
    pub fn remove_by_id(&mut self, table_id: TableId) -> Option<CatalogEntry> {
        let entry = self.entries.remove(&table_id)?;
        let lower = SmolStr::new(entry.name().to_lowercase());
        self.name_index.remove(&lower);
        Some(entry)
    }

    /// Remove an entry by name (case-insensitive). Returns the removed entry. O(1).
    pub fn remove_by_name(&mut self, name: &str) -> Option<CatalogEntry> {
        let lower = SmolStr::new(name.to_lowercase());
        let tid = self.name_index.remove(&lower)?;
        self.entries.remove(&tid)
    }

    /// Add a new node table entry. Validates name uniqueness.
    pub fn add_node_table(&mut self, entry: NodeTableEntry) -> KyuResult<()> {
        if self.contains_name(&entry.name) {
            return Err(KyuError::Catalog(format!(
                "table '{}' already exists",
                entry.name
            )));
        }
        let tid = entry.table_id;
        let lower = SmolStr::new(entry.name.to_lowercase());
        self.name_index.insert(lower, tid);
        self.entries.insert(tid, CatalogEntry::NodeTable(entry));
        Ok(())
    }

    /// Add a new relationship table entry. Validates name uniqueness.
    pub fn add_rel_table(&mut self, entry: RelTableEntry) -> KyuResult<()> {
        if self.contains_name(&entry.name) {
            return Err(KyuError::Catalog(format!(
                "table '{}' already exists",
                entry.name
            )));
        }
        let tid = entry.table_id;
        let lower = SmolStr::new(entry.name.to_lowercase());
        self.name_index.insert(lower, tid);
        self.entries.insert(tid, CatalogEntry::RelTable(entry));
        Ok(())
    }

    /// Find a mutable reference to a node table entry by name. O(1).
    pub fn find_node_table_mut(&mut self, name: &str) -> Option<&mut NodeTableEntry> {
        let lower = SmolStr::new(name.to_lowercase());
        let tid = *self.name_index.get(&lower)?;
        match self.entries.get_mut(&tid)? {
            CatalogEntry::NodeTable(nt) => Some(nt),
            _ => None,
        }
    }

    /// Find a mutable reference to a rel table entry by name. O(1).
    pub fn find_rel_table_mut(&mut self, name: &str) -> Option<&mut RelTableEntry> {
        let lower = SmolStr::new(name.to_lowercase());
        let tid = *self.name_index.get(&lower)?;
        match self.entries.get_mut(&tid)? {
            CatalogEntry::RelTable(rt) => Some(rt),
            _ => None,
        }
    }

    /// Find a mutable entry (any kind) by name, for rename/comment operations. O(1).
    pub fn find_entry_mut(&mut self, name: &str) -> Option<&mut CatalogEntry> {
        let lower = SmolStr::new(name.to_lowercase());
        let tid = *self.name_index.get(&lower)?;
        self.entries.get_mut(&tid)
    }

    /// Add a property to an existing table.
    pub fn add_property_to_table(
        &mut self,
        table_name: &str,
        property: Property,
    ) -> KyuResult<()> {
        let entry = self.find_entry_mut(table_name).ok_or_else(|| {
            KyuError::Catalog(format!("table '{table_name}' not found"))
        })?;

        let props = match entry {
            CatalogEntry::NodeTable(nt) => &mut nt.properties,
            CatalogEntry::RelTable(rt) => &mut rt.properties,
        };

        // Check for duplicate property name
        let prop_lower = property.name.to_lowercase();
        if props.iter().any(|p| p.name.to_lowercase() == prop_lower) {
            return Err(KyuError::Catalog(format!(
                "property '{}' already exists in table '{table_name}'",
                property.name
            )));
        }

        props.push(property);
        Ok(())
    }

    /// Drop a property from an existing table by name.
    pub fn drop_property_from_table(
        &mut self,
        table_name: &str,
        property_name: &str,
    ) -> KyuResult<()> {
        let entry = self.find_entry_mut(table_name).ok_or_else(|| {
            KyuError::Catalog(format!("table '{table_name}' not found"))
        })?;

        let (props, pk_idx) = match entry {
            CatalogEntry::NodeTable(nt) => (&mut nt.properties, Some(nt.primary_key_idx)),
            CatalogEntry::RelTable(rt) => (&mut rt.properties, None),
        };

        let prop_lower = property_name.to_lowercase();
        let pos = props
            .iter()
            .position(|p| p.name.to_lowercase() == prop_lower)
            .ok_or_else(|| {
                KyuError::Catalog(format!(
                    "property '{property_name}' not found in table '{table_name}'"
                ))
            })?;

        // Cannot drop primary key column
        if pk_idx == Some(pos) {
            return Err(KyuError::Catalog(format!(
                "cannot drop primary key property '{property_name}'"
            )));
        }

        props.remove(pos);

        // Adjust primary_key_idx if we removed a property before it
        if let CatalogEntry::NodeTable(nt) = entry
            && pos < nt.primary_key_idx
        {
            nt.primary_key_idx -= 1;
        }

        Ok(())
    }

    /// Rename a property in a table.
    pub fn rename_property(
        &mut self,
        table_name: &str,
        old_name: &str,
        new_name: &str,
    ) -> KyuResult<()> {
        let entry = self.find_entry_mut(table_name).ok_or_else(|| {
            KyuError::Catalog(format!("table '{table_name}' not found"))
        })?;

        let props = match entry {
            CatalogEntry::NodeTable(nt) => &mut nt.properties,
            CatalogEntry::RelTable(rt) => &mut rt.properties,
        };

        let new_lower = new_name.to_lowercase();
        if props.iter().any(|p| p.name.to_lowercase() == new_lower) {
            return Err(KyuError::Catalog(format!(
                "property '{new_name}' already exists in table '{table_name}'"
            )));
        }

        let old_lower = old_name.to_lowercase();
        let prop = props
            .iter_mut()
            .find(|p| p.name.to_lowercase() == old_lower)
            .ok_or_else(|| {
                KyuError::Catalog(format!(
                    "property '{old_name}' not found in table '{table_name}'"
                ))
            })?;

        prop.name = SmolStr::new(new_name);
        Ok(())
    }

    /// Rename a table entry. Updates the name index.
    pub fn rename_table(&mut self, old_name: &str, new_name: &SmolStr) -> KyuResult<()> {
        if self.contains_name(new_name) {
            return Err(KyuError::Catalog(format!(
                "table '{new_name}' already exists"
            )));
        }

        let old_lower = SmolStr::new(old_name.to_lowercase());
        let tid = self.name_index.remove(&old_lower).ok_or_else(|| {
            KyuError::Catalog(format!("table '{old_name}' not found"))
        })?;

        let new_lower = SmolStr::new(new_name.to_lowercase());
        self.name_index.insert(new_lower, tid);

        let entry = self.entries.get_mut(&tid).unwrap();
        match entry {
            CatalogEntry::NodeTable(nt) => nt.name = new_name.clone(),
            CatalogEntry::RelTable(rt) => rt.name = new_name.clone(),
        }
        Ok(())
    }
}

impl Default for CatalogContent {
    fn default() -> Self {
        Self::new()
    }
}

/// Thread-safe catalog with RwLock-based dual-version pattern.
///
/// Readers get a consistent snapshot; a single writer can mutate
/// via `begin_write()` / `commit_write()`.
pub struct Catalog {
    inner: RwLock<CatalogContent>,
}

impl Catalog {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(CatalogContent::new()),
        }
    }

    /// Get a read-only snapshot of the catalog content.
    pub fn read(&self) -> CatalogContent {
        self.inner.read().unwrap().clone()
    }

    /// Begin a write transaction: clone the current state for mutation.
    pub fn begin_write(&self) -> CatalogContent {
        self.inner.read().unwrap().clone()
    }

    /// Commit a modified catalog content, replacing the current state.
    pub fn commit_write(&self, mut content: CatalogContent) {
        content.version += 1;
        *self.inner.write().unwrap() = content;
    }

    /// Get the current catalog version.
    pub fn version(&self) -> u64 {
        self.inner.read().unwrap().version
    }

    /// Get the number of tables.
    pub fn num_tables(&self) -> usize {
        self.inner.read().unwrap().num_tables()
    }
}

impl Default for Catalog {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use kyu_types::LogicalType;

    use super::*;

    fn make_node_entry(content: &mut CatalogContent, name: &str) -> NodeTableEntry {
        let table_id = content.alloc_table_id();
        let pk_id = content.alloc_property_id();
        NodeTableEntry {
            table_id,
            name: SmolStr::new(name),
            properties: vec![Property::new(pk_id, "id", LogicalType::Serial, true)],
            primary_key_idx: 0,
            num_rows: 0,
            comment: None,
        }
    }

    fn make_rel_entry(
        content: &mut CatalogContent,
        name: &str,
        from: TableId,
        to: TableId,
    ) -> RelTableEntry {
        let table_id = content.alloc_table_id();
        RelTableEntry {
            table_id,
            name: SmolStr::new(name),
            from_table_id: from,
            to_table_id: to,
            properties: vec![],
            num_rows: 0,
            comment: None,
        }
    }

    #[test]
    fn catalog_content_new() {
        let c = CatalogContent::new();
        assert_eq!(c.num_tables(), 0);
        assert_eq!(c.version, 0);
    }

    #[test]
    fn alloc_ids() {
        let mut c = CatalogContent::new();
        assert_eq!(c.alloc_table_id(), TableId(0));
        assert_eq!(c.alloc_table_id(), TableId(1));
        assert_eq!(c.alloc_property_id(), PropertyId(0));
        assert_eq!(c.alloc_property_id(), PropertyId(1));
    }

    #[test]
    fn add_and_find_node_table() {
        let mut c = CatalogContent::new();
        let entry = make_node_entry(&mut c, "Person");
        c.add_node_table(entry).unwrap();

        assert_eq!(c.num_tables(), 1);
        assert!(c.find_by_name("Person").is_some());
        assert!(c.find_by_name("person").is_some()); // case-insensitive
        assert!(c.find_by_name("Missing").is_none());
    }

    #[test]
    fn add_duplicate_name_fails() {
        let mut c = CatalogContent::new();
        let e1 = make_node_entry(&mut c, "Person");
        let e2 = make_node_entry(&mut c, "Person");
        c.add_node_table(e1).unwrap();
        assert!(c.add_node_table(e2).is_err());
    }

    #[test]
    fn add_and_find_rel_table() {
        let mut c = CatalogContent::new();
        let person = make_node_entry(&mut c, "Person");
        let person_id = person.table_id;
        c.add_node_table(person).unwrap();

        let knows = make_rel_entry(&mut c, "Knows", person_id, person_id);
        c.add_rel_table(knows).unwrap();

        assert_eq!(c.num_tables(), 2);
        assert!(c.find_by_name("Knows").is_some());
    }

    #[test]
    fn find_by_id() {
        let mut c = CatalogContent::new();
        let entry = make_node_entry(&mut c, "Person");
        let tid = entry.table_id;
        c.add_node_table(entry).unwrap();

        assert!(c.find_by_id(tid).is_some());
        assert!(c.find_by_id(TableId(999)).is_none());
    }

    #[test]
    fn remove_by_name() {
        let mut c = CatalogContent::new();
        let entry = make_node_entry(&mut c, "Person");
        c.add_node_table(entry).unwrap();

        let removed = c.remove_by_name("Person");
        assert!(removed.is_some());
        assert_eq!(c.num_tables(), 0);
    }

    #[test]
    fn remove_by_id() {
        let mut c = CatalogContent::new();
        let entry = make_node_entry(&mut c, "Person");
        let tid = entry.table_id;
        c.add_node_table(entry).unwrap();

        let removed = c.remove_by_id(tid);
        assert!(removed.is_some());
        assert_eq!(c.num_tables(), 0);
    }

    #[test]
    fn node_tables_and_rel_tables() {
        let mut c = CatalogContent::new();
        let person = make_node_entry(&mut c, "Person");
        let pid = person.table_id;
        c.add_node_table(person).unwrap();

        let org = make_node_entry(&mut c, "Organization");
        c.add_node_table(org).unwrap();

        let works_at = make_rel_entry(&mut c, "WorksAt", pid, TableId(1));
        c.add_rel_table(works_at).unwrap();

        assert_eq!(c.node_tables().len(), 2);
        assert_eq!(c.rel_tables().len(), 1);
    }

    #[test]
    fn add_property_to_table() {
        let mut c = CatalogContent::new();
        let entry = make_node_entry(&mut c, "Person");
        c.add_node_table(entry).unwrap();

        let prop_id = c.alloc_property_id();
        let prop = Property::new(prop_id, "email", LogicalType::String, false);
        c.add_property_to_table("Person", prop).unwrap();

        let table = c.find_by_name("Person").unwrap();
        assert_eq!(table.properties().len(), 2);
    }

    #[test]
    fn add_duplicate_property_fails() {
        let mut c = CatalogContent::new();
        let entry = make_node_entry(&mut c, "Person");
        c.add_node_table(entry).unwrap();

        let prop = Property::new(c.alloc_property_id(), "id", LogicalType::Int64, false);
        assert!(c.add_property_to_table("Person", prop).is_err());
    }

    #[test]
    fn drop_property() {
        let mut c = CatalogContent::new();
        let mut entry = make_node_entry(&mut c, "Person");
        let email_id = c.alloc_property_id();
        entry
            .properties
            .push(Property::new(email_id, "email", LogicalType::String, false));
        c.add_node_table(entry).unwrap();

        c.drop_property_from_table("Person", "email").unwrap();
        let table = c.find_by_name("Person").unwrap();
        assert_eq!(table.properties().len(), 1);
    }

    #[test]
    fn drop_primary_key_fails() {
        let mut c = CatalogContent::new();
        let entry = make_node_entry(&mut c, "Person");
        c.add_node_table(entry).unwrap();

        assert!(c.drop_property_from_table("Person", "id").is_err());
    }

    #[test]
    fn rename_property() {
        let mut c = CatalogContent::new();
        let entry = make_node_entry(&mut c, "Person");
        c.add_node_table(entry).unwrap();

        c.rename_property("Person", "id", "person_id").unwrap();
        let table = c.find_by_name("Person").unwrap();
        assert!(table.properties().iter().any(|p| p.name == "person_id"));
    }

    #[test]
    fn rename_property_duplicate_fails() {
        let mut c = CatalogContent::new();
        let mut entry = make_node_entry(&mut c, "Person");
        entry.properties.push(Property::new(
            c.alloc_property_id(),
            "name",
            LogicalType::String,
            false,
        ));
        c.add_node_table(entry).unwrap();

        assert!(c.rename_property("Person", "id", "name").is_err());
    }

    #[test]
    fn catalog_read_write() {
        let catalog = Catalog::new();
        assert_eq!(catalog.version(), 0);
        assert_eq!(catalog.num_tables(), 0);

        let mut content = catalog.begin_write();
        let entry = make_node_entry(&mut content, "Person");
        content.add_node_table(entry).unwrap();
        catalog.commit_write(content);

        assert_eq!(catalog.version(), 1);
        assert_eq!(catalog.num_tables(), 1);

        let snapshot = catalog.read();
        assert_eq!(snapshot.num_tables(), 1);
        assert_eq!(snapshot.version, 1);
    }

    #[test]
    fn catalog_multiple_commits() {
        let catalog = Catalog::new();

        let mut c = catalog.begin_write();
        let entry = make_node_entry(&mut c, "Person");
        c.add_node_table(entry).unwrap();
        catalog.commit_write(c);

        let mut c = catalog.begin_write();
        let entry = make_node_entry(&mut c, "Organization");
        c.add_node_table(entry).unwrap();
        catalog.commit_write(c);

        assert_eq!(catalog.version(), 2);
        assert_eq!(catalog.num_tables(), 2);
    }

    #[test]
    fn rename_table() {
        let mut c = CatalogContent::new();
        let entry = make_node_entry(&mut c, "Person");
        c.add_node_table(entry).unwrap();

        c.rename_table("Person", &SmolStr::new("People")).unwrap();
        assert!(c.find_by_name("People").is_some());
        assert!(c.find_by_name("Person").is_none());
    }

    #[test]
    fn rename_table_conflict() {
        let mut c = CatalogContent::new();
        let e1 = make_node_entry(&mut c, "Person");
        let e2 = make_node_entry(&mut c, "Organization");
        c.add_node_table(e1).unwrap();
        c.add_node_table(e2).unwrap();

        assert!(c.rename_table("Person", &SmolStr::new("Organization")).is_err());
    }

    #[test]
    fn name_index_consistency_after_remove() {
        let mut c = CatalogContent::new();
        let entry = make_node_entry(&mut c, "Person");
        let tid = entry.table_id;
        c.add_node_table(entry).unwrap();

        c.remove_by_id(tid);
        assert!(!c.contains_name("Person"));
        assert!(c.find_by_name("Person").is_none());

        // Can re-add the same name
        let entry2 = make_node_entry(&mut c, "Person");
        c.add_node_table(entry2).unwrap();
        assert!(c.find_by_name("Person").is_some());
    }
}

//! Scope management for the semantic binder.
//!
//! Maps variable names to (index, type, table_id) with frame stacking for
//! WITH/RETURN chaining. Case-insensitive lookup.

use kyu_common::id::TableId;
use kyu_common::{KyuError, KyuResult};
use kyu_types::LogicalType;
use smol_str::SmolStr;

/// Information about a bound variable in scope.
#[derive(Clone, Debug)]
pub struct VariableInfo {
    pub index: u32,
    pub data_type: LogicalType,
    /// TableId if this variable is bound to a node/rel pattern.
    pub table_id: Option<TableId>,
    pub name: SmolStr,
}

/// Variable scope for expression binding.
///
/// Supports stacking (push/pop) for WITH/subquery scoping. Variables are
/// assigned monotonic u32 indices for fast lookup at runtime.
pub struct BinderScope {
    frames: Vec<ScopeFrame>,
    next_index: u32,
}

struct ScopeFrame {
    variables: Vec<(SmolStr, VariableInfo)>,
}

impl Default for BinderScope {
    fn default() -> Self {
        Self::new()
    }
}

impl BinderScope {
    pub fn new() -> Self {
        Self {
            frames: vec![ScopeFrame {
                variables: Vec::new(),
            }],
            next_index: 0,
        }
    }

    /// Push a new scope frame (for WITH, subquery entry).
    pub fn push_frame(&mut self) {
        self.frames.push(ScopeFrame {
            variables: Vec::new(),
        });
    }

    /// Pop the current scope frame.
    pub fn pop_frame(&mut self) {
        if self.frames.len() > 1 {
            self.frames.pop();
        }
    }

    /// Define a new variable in the current frame.
    ///
    /// Returns the VariableInfo. Errors on duplicate in the same frame.
    pub fn define(
        &mut self,
        name: &str,
        data_type: LogicalType,
        table_id: Option<TableId>,
    ) -> KyuResult<VariableInfo> {
        let lower = SmolStr::new(name.to_lowercase());

        // Check for duplicate in current frame.
        let frame = self.frames.last().unwrap();
        if frame.variables.iter().any(|(n, _)| *n == lower) {
            return Err(KyuError::Binder(format!(
                "variable '{name}' already defined in this scope"
            )));
        }

        let index = self.next_index;
        self.next_index += 1;

        let info = VariableInfo {
            index,
            data_type,
            table_id,
            name: lower.clone(),
        };

        let frame = self.frames.last_mut().unwrap();
        frame.variables.push((lower, info.clone()));

        Ok(info)
    }

    /// Look up a variable by name (searches current frame first, then outer).
    pub fn resolve(&self, name: &str) -> Option<&VariableInfo> {
        let lower = name.to_lowercase();
        for frame in self.frames.iter().rev() {
            for (n, info) in &frame.variables {
                if n.as_str() == lower {
                    return Some(info);
                }
            }
        }
        None
    }

    /// Get total number of variables defined across all frames.
    pub fn num_variables(&self) -> u32 {
        self.next_index
    }

    /// Get all variables in the current (innermost) frame.
    pub fn current_variables(&self) -> &[(SmolStr, VariableInfo)] {
        &self.frames.last().unwrap().variables
    }

    /// Replace current frame with a new one from a projection (WITH/RETURN).
    ///
    /// The new scope contains only the projected variables with new indices.
    /// Previous variables become invisible (WITH semantics).
    pub fn new_from_projection(&mut self, projected: Vec<(SmolStr, LogicalType)>) {
        // Pop the current frame.
        if self.frames.len() > 1 {
            self.frames.pop();
        } else {
            self.frames.last_mut().unwrap().variables.clear();
        }

        // Push a new frame with projected variables.
        let mut new_frame = ScopeFrame {
            variables: Vec::with_capacity(projected.len()),
        };
        for (name, data_type) in projected {
            let lower = SmolStr::new(name.to_lowercase());
            let index = self.next_index;
            self.next_index += 1;
            new_frame.variables.push((
                lower.clone(),
                VariableInfo {
                    index,
                    data_type,
                    table_id: None,
                    name: lower,
                },
            ));
        }
        self.frames.push(new_frame);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn define_and_resolve() {
        let mut scope = BinderScope::new();
        let info = scope.define("x", LogicalType::Int64, None).unwrap();
        assert_eq!(info.index, 0);
        assert_eq!(info.data_type, LogicalType::Int64);

        let resolved = scope.resolve("x").unwrap();
        assert_eq!(resolved.index, 0);
    }

    #[test]
    fn case_insensitive_resolve() {
        let mut scope = BinderScope::new();
        scope.define("Person", LogicalType::Node, Some(TableId(1))).unwrap();

        assert!(scope.resolve("person").is_some());
        assert!(scope.resolve("PERSON").is_some());
        assert!(scope.resolve("Person").is_some());
    }

    #[test]
    fn duplicate_in_same_frame_errors() {
        let mut scope = BinderScope::new();
        scope.define("x", LogicalType::Int64, None).unwrap();
        assert!(scope.define("x", LogicalType::String, None).is_err());
    }

    #[test]
    fn sequential_indices() {
        let mut scope = BinderScope::new();
        let a = scope.define("a", LogicalType::Int64, None).unwrap();
        let b = scope.define("b", LogicalType::String, None).unwrap();
        assert_eq!(a.index, 0);
        assert_eq!(b.index, 1);
        assert_eq!(scope.num_variables(), 2);
    }

    #[test]
    fn push_pop_frame() {
        let mut scope = BinderScope::new();
        scope.define("outer", LogicalType::Int64, None).unwrap();

        scope.push_frame();
        scope.define("inner", LogicalType::String, None).unwrap();

        // Both visible from inner frame.
        assert!(scope.resolve("outer").is_some());
        assert!(scope.resolve("inner").is_some());

        scope.pop_frame();
        // Only outer visible after pop.
        assert!(scope.resolve("outer").is_some());
        assert!(scope.resolve("inner").is_none());
    }

    #[test]
    fn inner_frame_shadows_outer() {
        let mut scope = BinderScope::new();
        scope.define("x", LogicalType::Int64, None).unwrap();

        scope.push_frame();
        scope.define("x", LogicalType::String, None).unwrap();

        let info = scope.resolve("x").unwrap();
        assert_eq!(info.data_type, LogicalType::String);
    }

    #[test]
    fn resolve_not_found() {
        let scope = BinderScope::new();
        assert!(scope.resolve("nonexistent").is_none());
    }

    #[test]
    fn current_variables() {
        let mut scope = BinderScope::new();
        scope.define("a", LogicalType::Int64, None).unwrap();
        scope.define("b", LogicalType::String, None).unwrap();

        let vars = scope.current_variables();
        assert_eq!(vars.len(), 2);
        assert_eq!(vars[0].0.as_str(), "a");
        assert_eq!(vars[1].0.as_str(), "b");
    }

    #[test]
    fn new_from_projection() {
        let mut scope = BinderScope::new();
        scope.define("old_var", LogicalType::Int64, None).unwrap();

        scope.new_from_projection(vec![
            (SmolStr::new("name"), LogicalType::String),
            (SmolStr::new("age"), LogicalType::Int64),
        ]);

        // Old variable is gone.
        assert!(scope.resolve("old_var").is_none());

        // New variables are available.
        assert!(scope.resolve("name").is_some());
        assert!(scope.resolve("age").is_some());
        assert_eq!(scope.current_variables().len(), 2);
    }

    #[test]
    fn table_id_preserved() {
        let mut scope = BinderScope::new();
        let info = scope
            .define("p", LogicalType::Node, Some(TableId(42)))
            .unwrap();
        assert_eq!(info.table_id, Some(TableId(42)));

        let resolved = scope.resolve("p").unwrap();
        assert_eq!(resolved.table_id, Some(TableId(42)));
    }
}

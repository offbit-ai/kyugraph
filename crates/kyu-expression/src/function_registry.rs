//! Function registry — scalar and aggregate function signatures.
//!
//! Pre-populated with built-in functions at startup. Resolves function calls
//! by name + argument types, selecting the best overload via implicit cast cost.

use hashbrown::HashMap;
use kyu_common::{KyuError, KyuResult};
use kyu_types::type_utils::implicit_cast_cost;
use kyu_types::LogicalType;
use smol_str::SmolStr;

use crate::bound_expr::FunctionId;

/// Whether a function is scalar (row-at-a-time) or aggregate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FunctionKind {
    Scalar,
    Aggregate,
}

/// A function signature: expected arg types and return type.
#[derive(Clone, Debug)]
pub struct FunctionSignature {
    pub id: FunctionId,
    pub name: SmolStr,
    pub kind: FunctionKind,
    pub param_types: Vec<LogicalType>,
    pub variadic: bool,
    pub return_type: LogicalType,
}

/// Registry of all known functions.
///
/// Populated once at startup; immutable during query processing.
/// Functions are indexed by `FunctionId` (O(1) lookup) and by name
/// (case-insensitive, O(1) via HashMap).
pub struct FunctionRegistry {
    signatures: Vec<FunctionSignature>,
    name_index: HashMap<SmolStr, Vec<usize>>,
}

impl Default for FunctionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl FunctionRegistry {
    pub fn new() -> Self {
        Self {
            signatures: Vec::new(),
            name_index: HashMap::new(),
        }
    }

    /// Create a registry pre-populated with built-in functions.
    pub fn with_builtins() -> Self {
        let mut reg = Self::new();
        register_builtins(&mut reg);
        reg
    }

    /// Register a function signature. Returns the assigned FunctionId.
    pub fn register(&mut self, name: &str, kind: FunctionKind, param_types: Vec<LogicalType>, variadic: bool, return_type: LogicalType) -> FunctionId {
        let id = FunctionId(self.signatures.len() as u32);
        let lower_name = SmolStr::new(name.to_lowercase());
        let sig = FunctionSignature {
            id,
            name: lower_name.clone(),
            kind,
            param_types,
            variadic,
            return_type,
        };
        let idx = self.signatures.len();
        self.signatures.push(sig);
        self.name_index
            .entry(lower_name)
            .or_default()
            .push(idx);
        id
    }

    /// Resolve a function call: name + actual arg types → best matching signature.
    ///
    /// Considers implicit coercions. Returns the matching signature.
    pub fn resolve(
        &self,
        name: &str,
        arg_types: &[LogicalType],
    ) -> KyuResult<&FunctionSignature> {
        let lower = name.to_lowercase();
        let overloads = self
            .name_index
            .get(lower.as_str())
            .ok_or_else(|| KyuError::Binder(format!("unknown function '{name}'")))?;

        let mut best: Option<(usize, u32)> = None; // (index, total_cost)

        for &idx in overloads {
            let sig = &self.signatures[idx];
            if let Some(cost) = match_cost(sig, arg_types) {
                match best {
                    None => best = Some((idx, cost)),
                    Some((_, best_cost)) if cost < best_cost => {
                        best = Some((idx, cost));
                    }
                    _ => {}
                }
            }
        }

        match best {
            Some((idx, _)) => Ok(&self.signatures[idx]),
            None => {
                let type_names: Vec<_> = arg_types.iter().map(|t| t.type_name().to_string()).collect();
                Err(KyuError::Binder(format!(
                    "no matching overload for {}({})",
                    name,
                    type_names.join(", "),
                )))
            }
        }
    }

    /// Look up by FunctionId (O(1)).
    pub fn get(&self, id: FunctionId) -> Option<&FunctionSignature> {
        self.signatures.get(id.0 as usize)
    }

    /// Number of registered functions.
    pub fn len(&self) -> usize {
        self.signatures.len()
    }

    /// Whether the registry is empty.
    pub fn is_empty(&self) -> bool {
        self.signatures.is_empty()
    }
}

/// Compute the total implicit cast cost for matching `arg_types` against `sig`.
/// Returns `None` if args don't match.
fn match_cost(sig: &FunctionSignature, arg_types: &[LogicalType]) -> Option<u32> {
    if sig.variadic {
        if arg_types.len() < sig.param_types.len() {
            return None;
        }
    } else if arg_types.len() != sig.param_types.len() {
        return None;
    }

    let mut total = 0u32;

    // Match declared params.
    for (param, arg) in sig.param_types.iter().zip(arg_types.iter()) {
        if matches!(param, LogicalType::Any) {
            // Any accepts anything at cost 0.
            continue;
        }
        let cost = implicit_cast_cost(arg, param)?;
        total += cost;
    }

    // For variadic, extra args are accepted at cost 0 (type checking is loose).
    Some(total)
}

fn register_builtins(reg: &mut FunctionRegistry) {
    use FunctionKind::{Aggregate, Scalar};
    use LogicalType::*;

    // Numeric scalar functions — register common overloads.
    for ty in &[Int64, Double] {
        reg.register("abs", Scalar, vec![ty.clone()], false, ty.clone());
    }
    reg.register("floor", Scalar, vec![Double], false, Double);
    reg.register("ceil", Scalar, vec![Double], false, Double);
    reg.register("round", Scalar, vec![Double], false, Double);
    reg.register("sqrt", Scalar, vec![Double], false, Double);
    reg.register("log", Scalar, vec![Double], false, Double);
    reg.register("log2", Scalar, vec![Double], false, Double);
    reg.register("log10", Scalar, vec![Double], false, Double);
    reg.register("sin", Scalar, vec![Double], false, Double);
    reg.register("cos", Scalar, vec![Double], false, Double);
    reg.register("tan", Scalar, vec![Double], false, Double);
    reg.register("sign", Scalar, vec![Int64], false, Int64);
    reg.register("sign", Scalar, vec![Double], false, Int64);
    reg.register("greatest", Scalar, vec![Any], true, Any);
    reg.register("least", Scalar, vec![Any], true, Any);

    // String scalar functions.
    reg.register("lower", Scalar, vec![String], false, String);
    reg.register("upper", Scalar, vec![String], false, String);
    reg.register("length", Scalar, vec![String], false, Int64);
    reg.register("size", Scalar, vec![String], false, Int64);
    reg.register("trim", Scalar, vec![String], false, String);
    reg.register("ltrim", Scalar, vec![String], false, String);
    reg.register("rtrim", Scalar, vec![String], false, String);
    reg.register("reverse", Scalar, vec![String], false, String);
    reg.register(
        "substring",
        Scalar,
        vec![String, Int64, Int64],
        false,
        String,
    );
    reg.register("left", Scalar, vec![String, Int64], false, String);
    reg.register("right", Scalar, vec![String, Int64], false, String);
    reg.register(
        "replace",
        Scalar,
        vec![String, String, String],
        false,
        String,
    );
    reg.register("concat", Scalar, vec![String], true, String);
    reg.register("lpad", Scalar, vec![String, Int64, String], false, String);
    reg.register("rpad", Scalar, vec![String, Int64, String], false, String);

    // Conversion functions.
    reg.register("tostring", Scalar, vec![Any], false, String);
    reg.register("tostring", Scalar, vec![String], false, String);
    reg.register("tointeger", Scalar, vec![Any], false, Int64);
    reg.register("tofloat", Scalar, vec![Any], false, Double);
    reg.register("toboolean", Scalar, vec![Any], false, Bool);

    // Utility.
    reg.register("coalesce", Scalar, vec![Any], true, Any);
    reg.register("typeof", Scalar, vec![Any], false, String);
    reg.register("hash", Scalar, vec![Any], false, Int64);

    // List functions.
    reg.register(
        "range",
        Scalar,
        vec![Int64, Int64],
        false,
        List(Box::new(Int64)),
    );
    reg.register("size", Scalar, vec![List(Box::new(Any))], false, Int64);
    reg.register("length", Scalar, vec![List(Box::new(Any))], false, Int64);

    // JSON functions.
    reg.register("json_extract", Scalar, vec![String, String], false, String);
    reg.register("json_valid", Scalar, vec![String], false, Bool);
    reg.register("json_type", Scalar, vec![String], false, String);
    reg.register("json_keys", Scalar, vec![String], false, List(Box::new(String)));
    reg.register("json_array_length", Scalar, vec![String], false, Int64);
    reg.register("json_contains", Scalar, vec![String, String], false, Bool);
    reg.register("json_set", Scalar, vec![String, String, String], false, String);

    // Aggregate functions.
    reg.register("count", Aggregate, vec![Any], false, Int64);
    reg.register("sum", Aggregate, vec![Int64], false, Int64);
    reg.register("sum", Aggregate, vec![Double], false, Double);
    reg.register("avg", Aggregate, vec![Int64], false, Double);
    reg.register("avg", Aggregate, vec![Double], false, Double);
    reg.register("min", Aggregate, vec![Any], false, Any);
    reg.register("max", Aggregate, vec![Any], false, Any);
    reg.register("collect", Aggregate, vec![Any], false, List(Box::new(Any)));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_registry() {
        let reg = FunctionRegistry::new();
        assert!(reg.is_empty());
        assert_eq!(reg.len(), 0);
    }

    #[test]
    fn register_and_get() {
        let mut reg = FunctionRegistry::new();
        let id = reg.register("foo", FunctionKind::Scalar, vec![LogicalType::Int64], false, LogicalType::Int64);
        assert_eq!(id.0, 0);

        let sig = reg.get(id).unwrap();
        assert_eq!(sig.name.as_str(), "foo");
        assert_eq!(sig.return_type, LogicalType::Int64);
        assert_eq!(sig.kind, FunctionKind::Scalar);
    }

    #[test]
    fn resolve_exact_match() {
        let reg = FunctionRegistry::with_builtins();
        let sig = reg.resolve("abs", &[LogicalType::Int64]).unwrap();
        assert_eq!(sig.return_type, LogicalType::Int64);
    }

    #[test]
    fn resolve_case_insensitive() {
        let reg = FunctionRegistry::with_builtins();
        let sig = reg.resolve("ABS", &[LogicalType::Int64]).unwrap();
        assert_eq!(sig.name.as_str(), "abs");
    }

    #[test]
    fn resolve_with_implicit_coercion() {
        let reg = FunctionRegistry::with_builtins();
        // abs(Int32) should match abs(Int64) via implicit cast.
        let sig = reg.resolve("abs", &[LogicalType::Int32]).unwrap();
        assert_eq!(sig.return_type, LogicalType::Int64);
    }

    #[test]
    fn resolve_best_overload() {
        let reg = FunctionRegistry::with_builtins();
        // abs(Double) should prefer abs(Double) over abs(Int64).
        let sig = reg.resolve("abs", &[LogicalType::Double]).unwrap();
        assert_eq!(sig.return_type, LogicalType::Double);
    }

    #[test]
    fn resolve_unknown_function() {
        let reg = FunctionRegistry::with_builtins();
        let result = reg.resolve("nonexistent", &[LogicalType::Int64]);
        assert!(result.is_err());
    }

    #[test]
    fn resolve_wrong_arg_count() {
        let reg = FunctionRegistry::with_builtins();
        let result = reg.resolve("abs", &[]);
        assert!(result.is_err());
    }

    #[test]
    fn resolve_aggregate() {
        let reg = FunctionRegistry::with_builtins();
        let sig = reg.resolve("count", &[LogicalType::Int64]).unwrap();
        assert_eq!(sig.kind, FunctionKind::Aggregate);
        assert_eq!(sig.return_type, LogicalType::Int64);
    }

    #[test]
    fn resolve_string_function() {
        let reg = FunctionRegistry::with_builtins();
        let sig = reg.resolve("upper", &[LogicalType::String]).unwrap();
        assert_eq!(sig.return_type, LogicalType::String);
    }

    #[test]
    fn resolve_multi_arg_function() {
        let reg = FunctionRegistry::with_builtins();
        let sig = reg
            .resolve("substring", &[LogicalType::String, LogicalType::Int64, LogicalType::Int64])
            .unwrap();
        assert_eq!(sig.return_type, LogicalType::String);
    }

    #[test]
    fn resolve_variadic_function() {
        let reg = FunctionRegistry::with_builtins();
        // coalesce(Any...) — accepts any number of args >= 1.
        let sig = reg
            .resolve("coalesce", &[LogicalType::Int64, LogicalType::Int64, LogicalType::Int64])
            .unwrap();
        assert_eq!(sig.name.as_str(), "coalesce");
    }

    #[test]
    fn builtins_populated() {
        let reg = FunctionRegistry::with_builtins();
        assert!(reg.len() > 20);
    }

    #[test]
    fn function_id_sequential() {
        let mut reg = FunctionRegistry::new();
        let id0 = reg.register("a", FunctionKind::Scalar, vec![], false, LogicalType::Bool);
        let id1 = reg.register("b", FunctionKind::Scalar, vec![], false, LogicalType::Bool);
        assert_eq!(id0.0, 0);
        assert_eq!(id1.0, 1);
    }

    #[test]
    fn get_nonexistent_id() {
        let reg = FunctionRegistry::new();
        assert!(reg.get(FunctionId(999)).is_none());
    }
}

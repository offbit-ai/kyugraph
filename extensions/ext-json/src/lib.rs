//! ext-json: SIMD-accelerated JSON scalar functions.
//!
//! JSON functions (`json_extract`, `json_valid`, `json_type`, `json_keys`,
//! `json_array_length`, `json_contains`, `json_set`) are registered as built-in
//! scalar functions in `kyu-expression`'s FunctionRegistry and evaluator, powered
//! by `simd-json` for SIMD-accelerated parsing (AVX2/SSE4.2/NEON).
//!
//! This crate exists as the extension namespace. The actual implementations live
//! in `kyu_expression::evaluator` to enable use in any expression context
//! (e.g., `RETURN json_extract(n.data, '$.name')`).

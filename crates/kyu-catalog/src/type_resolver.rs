use kyu_common::{KyuError, KyuResult};
use kyu_types::LogicalType;
use smol_str::SmolStr;

/// Resolve a DDL type name (from `ColumnDefinition.data_type`) to a `LogicalType`.
///
/// Handles the full range of Kuzu-compatible type names, including nested types
/// like `INT64[]`, `FLOAT[1536]`, `MAP(STRING, INT64)`, `STRUCT(name STRING, age INT64)`,
/// and `UNION(a INT64, b STRING)`.
pub fn resolve_type(name: &str) -> KyuResult<LogicalType> {
    let upper = name.trim().to_uppercase();
    resolve_type_inner(&upper)
}

fn resolve_type_inner(s: &str) -> KyuResult<LogicalType> {
    // Check for array suffix: TYPE[N] (fixed-size array)
    if let Some(inner) = try_parse_fixed_array(s) {
        return inner;
    }

    // Check for list suffix: TYPE[] (variable-length list)
    if let Some(inner) = s.strip_suffix("[]") {
        let element = resolve_type_inner(inner.trim())?;
        return Ok(LogicalType::List(Box::new(element)));
    }

    // Check for parameterized types: MAP(...), STRUCT(...), UNION(...), DECIMAL(...)
    if let Some(inner) = strip_prefix_params(s, "MAP") {
        return parse_map(inner);
    }
    if let Some(inner) = strip_prefix_params(s, "STRUCT") {
        return parse_struct_or_union(inner, false);
    }
    if let Some(inner) = strip_prefix_params(s, "UNION") {
        return parse_struct_or_union(inner, true);
    }
    if let Some(inner) = strip_prefix_params(s, "DECIMAL") {
        return parse_decimal(inner);
    }
    if let Some(inner) = strip_prefix_params(s, "LIST") {
        let element = resolve_type_inner(inner.trim())?;
        return Ok(LogicalType::List(Box::new(element)));
    }

    // Scalar types
    match s {
        "BOOL" | "BOOLEAN" => Ok(LogicalType::Bool),
        "INT8" | "TINYINT" => Ok(LogicalType::Int8),
        "INT16" | "SMALLINT" => Ok(LogicalType::Int16),
        "INT32" | "INT" | "INTEGER" => Ok(LogicalType::Int32),
        "INT64" | "BIGINT" => Ok(LogicalType::Int64),
        "INT128" => Ok(LogicalType::Int128),
        "UINT8" => Ok(LogicalType::UInt8),
        "UINT16" => Ok(LogicalType::UInt16),
        "UINT32" => Ok(LogicalType::UInt32),
        "UINT64" => Ok(LogicalType::UInt64),
        "FLOAT" | "REAL" => Ok(LogicalType::Float),
        "DOUBLE" => Ok(LogicalType::Double),
        "DATE" => Ok(LogicalType::Date),
        "TIMESTAMP" => Ok(LogicalType::Timestamp),
        "TIMESTAMP_SEC" => Ok(LogicalType::TimestampSec),
        "TIMESTAMP_MS" => Ok(LogicalType::TimestampMs),
        "TIMESTAMP_NS" => Ok(LogicalType::TimestampNs),
        "TIMESTAMP_TZ" => Ok(LogicalType::TimestampTz),
        "INTERVAL" => Ok(LogicalType::Interval),
        "SERIAL" => Ok(LogicalType::Serial),
        "STRING" | "VARCHAR" => Ok(LogicalType::String),
        "BLOB" | "BYTEA" => Ok(LogicalType::Blob),
        "UUID" => Ok(LogicalType::Uuid),
        "INTERNAL_ID" => Ok(LogicalType::InternalId),
        _ => Err(KyuError::Catalog(format!("unknown type: {s}"))),
    }
}

/// Try to parse `TYPE[N]` (fixed-size array). Returns `None` if not an array pattern.
fn try_parse_fixed_array(s: &str) -> Option<KyuResult<LogicalType>> {
    let bracket_open = s.rfind('[')?;
    let bracket_close = s.rfind(']')?;
    if bracket_close != s.len() - 1 || bracket_close <= bracket_open + 1 {
        return None;
    }
    let size_str = &s[bracket_open + 1..bracket_close];
    // If size_str is empty, it's a list `TYPE[]`, not a fixed array
    if size_str.is_empty() {
        return None;
    }
    let size: u64 = match size_str.trim().parse() {
        Ok(n) => n,
        Err(_) => return None,
    };
    let element_str = s[..bracket_open].trim();
    Some(
        resolve_type_inner(element_str).map(|el| LogicalType::Array {
            element: Box::new(el),
            size,
        }),
    )
}

/// Strip a prefix like `MAP(...)` and return the inner content.
fn strip_prefix_params<'a>(s: &'a str, prefix: &str) -> Option<&'a str> {
    let s = s.strip_prefix(prefix)?;
    let s = s.trim_start();
    let s = s.strip_prefix('(')?;
    let s = s.strip_suffix(')')?;
    Some(s)
}

/// Parse `KEY_TYPE, VALUE_TYPE` inside MAP(...).
fn parse_map(inner: &str) -> KyuResult<LogicalType> {
    let (left, right) = split_top_level_comma(inner).ok_or_else(|| {
        KyuError::Catalog(format!("MAP requires two type arguments, got: {inner}"))
    })?;
    let key = resolve_type_inner(left.trim())?;
    let value = resolve_type_inner(right.trim())?;
    Ok(LogicalType::Map {
        key: Box::new(key),
        value: Box::new(value),
    })
}

/// Parse `name TYPE, name TYPE, ...` inside STRUCT(...) or UNION(...).
fn parse_struct_or_union(inner: &str, is_union: bool) -> KyuResult<LogicalType> {
    let fields = split_all_top_level_commas(inner);
    let mut result = Vec::with_capacity(fields.len());
    for field in fields {
        let field = field.trim();
        let (name, ty) = split_field_name_type(field).ok_or_else(|| {
            let kind = if is_union { "UNION" } else { "STRUCT" };
            KyuError::Catalog(format!("{kind} field must be `name TYPE`, got: {field}"))
        })?;
        let logical_type = resolve_type_inner(ty.trim())?;
        result.push((SmolStr::new(name.to_lowercase()), logical_type));
    }
    if is_union {
        Ok(LogicalType::Union(result))
    } else {
        Ok(LogicalType::Struct(result))
    }
}

/// Parse `PRECISION, SCALE` inside DECIMAL(...).
fn parse_decimal(inner: &str) -> KyuResult<LogicalType> {
    let (left, right) = split_top_level_comma(inner).ok_or_else(|| {
        KyuError::Catalog(format!(
            "DECIMAL requires precision and scale, got: {inner}"
        ))
    })?;
    let precision: u8 = left
        .trim()
        .parse()
        .map_err(|_| KyuError::Catalog(format!("invalid DECIMAL precision: {}", left.trim())))?;
    let scale: u8 = right
        .trim()
        .parse()
        .map_err(|_| KyuError::Catalog(format!("invalid DECIMAL scale: {}", right.trim())))?;
    Ok(LogicalType::Decimal { precision, scale })
}

/// Split on the first top-level comma (respecting parentheses nesting).
fn split_top_level_comma(s: &str) -> Option<(&str, &str)> {
    let mut depth = 0u32;
    for (i, c) in s.char_indices() {
        match c {
            '(' | '[' => depth += 1,
            ')' | ']' => depth = depth.saturating_sub(1),
            ',' if depth == 0 => {
                return Some((&s[..i], &s[i + 1..]));
            }
            _ => {}
        }
    }
    None
}

/// Split on all top-level commas (respecting parentheses nesting).
fn split_all_top_level_commas(s: &str) -> Vec<&str> {
    let mut result = Vec::new();
    let mut depth = 0u32;
    let mut start = 0;
    for (i, c) in s.char_indices() {
        match c {
            '(' | '[' => depth += 1,
            ')' | ']' => depth = depth.saturating_sub(1),
            ',' if depth == 0 => {
                result.push(&s[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    result.push(&s[start..]);
    result
}

/// Split `name TYPE` into (name, type). Handles the first whitespace boundary.
fn split_field_name_type(s: &str) -> Option<(&str, &str)> {
    let s = s.trim();
    let space = s.find(|c: char| c.is_whitespace())?;
    let name = &s[..space];
    let ty = s[space..].trim_start();
    if name.is_empty() || ty.is_empty() {
        return None;
    }
    Some((name, ty))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scalar_types() {
        assert_eq!(resolve_type("INT64").unwrap(), LogicalType::Int64);
        assert_eq!(resolve_type("BIGINT").unwrap(), LogicalType::Int64);
        assert_eq!(resolve_type("STRING").unwrap(), LogicalType::String);
        assert_eq!(resolve_type("VARCHAR").unwrap(), LogicalType::String);
        assert_eq!(resolve_type("BOOL").unwrap(), LogicalType::Bool);
        assert_eq!(resolve_type("BOOLEAN").unwrap(), LogicalType::Bool);
        assert_eq!(resolve_type("DOUBLE").unwrap(), LogicalType::Double);
        assert_eq!(resolve_type("FLOAT").unwrap(), LogicalType::Float);
        assert_eq!(resolve_type("REAL").unwrap(), LogicalType::Float);
        assert_eq!(resolve_type("DATE").unwrap(), LogicalType::Date);
        assert_eq!(resolve_type("TIMESTAMP").unwrap(), LogicalType::Timestamp);
        assert_eq!(resolve_type("UUID").unwrap(), LogicalType::Uuid);
        assert_eq!(resolve_type("BLOB").unwrap(), LogicalType::Blob);
        assert_eq!(resolve_type("BYTEA").unwrap(), LogicalType::Blob);
        assert_eq!(resolve_type("SERIAL").unwrap(), LogicalType::Serial);
    }

    #[test]
    fn case_insensitive() {
        assert_eq!(resolve_type("int64").unwrap(), LogicalType::Int64);
        assert_eq!(resolve_type("String").unwrap(), LogicalType::String);
        assert_eq!(resolve_type("bool").unwrap(), LogicalType::Bool);
    }

    #[test]
    fn integer_aliases() {
        assert_eq!(resolve_type("INT").unwrap(), LogicalType::Int32);
        assert_eq!(resolve_type("INTEGER").unwrap(), LogicalType::Int32);
        assert_eq!(resolve_type("TINYINT").unwrap(), LogicalType::Int8);
        assert_eq!(resolve_type("SMALLINT").unwrap(), LogicalType::Int16);
    }

    #[test]
    fn list_type() {
        assert_eq!(
            resolve_type("INT64[]").unwrap(),
            LogicalType::List(Box::new(LogicalType::Int64))
        );
        assert_eq!(
            resolve_type("STRING[]").unwrap(),
            LogicalType::List(Box::new(LogicalType::String))
        );
    }

    #[test]
    fn list_function_syntax() {
        assert_eq!(
            resolve_type("LIST(INT64)").unwrap(),
            LogicalType::List(Box::new(LogicalType::Int64))
        );
    }

    #[test]
    fn fixed_array_type() {
        assert_eq!(
            resolve_type("FLOAT[1536]").unwrap(),
            LogicalType::Array {
                element: Box::new(LogicalType::Float),
                size: 1536,
            }
        );
    }

    #[test]
    fn decimal_type() {
        assert_eq!(
            resolve_type("DECIMAL(18, 3)").unwrap(),
            LogicalType::Decimal {
                precision: 18,
                scale: 3,
            }
        );
    }

    #[test]
    fn map_type() {
        assert_eq!(
            resolve_type("MAP(STRING, INT64)").unwrap(),
            LogicalType::Map {
                key: Box::new(LogicalType::String),
                value: Box::new(LogicalType::Int64),
            }
        );
    }

    #[test]
    fn struct_type() {
        assert_eq!(
            resolve_type("STRUCT(name STRING, age INT64)").unwrap(),
            LogicalType::Struct(vec![
                (SmolStr::new("name"), LogicalType::String),
                (SmolStr::new("age"), LogicalType::Int64),
            ])
        );
    }

    #[test]
    fn union_type() {
        assert_eq!(
            resolve_type("UNION(a INT64, b STRING)").unwrap(),
            LogicalType::Union(vec![
                (SmolStr::new("a"), LogicalType::Int64),
                (SmolStr::new("b"), LogicalType::String),
            ])
        );
    }

    #[test]
    fn nested_list_of_list() {
        assert_eq!(
            resolve_type("INT64[][]").unwrap(),
            LogicalType::List(Box::new(LogicalType::List(Box::new(LogicalType::Int64))))
        );
    }

    #[test]
    fn unknown_type_errors() {
        assert!(resolve_type("FOOBAR").is_err());
    }

    #[test]
    fn whitespace_tolerance() {
        assert_eq!(resolve_type("  INT64  ").unwrap(), LogicalType::Int64);
    }

    #[test]
    fn timestamp_variants() {
        assert_eq!(
            resolve_type("TIMESTAMP_SEC").unwrap(),
            LogicalType::TimestampSec
        );
        assert_eq!(
            resolve_type("TIMESTAMP_MS").unwrap(),
            LogicalType::TimestampMs
        );
        assert_eq!(
            resolve_type("TIMESTAMP_NS").unwrap(),
            LogicalType::TimestampNs
        );
        assert_eq!(
            resolve_type("TIMESTAMP_TZ").unwrap(),
            LogicalType::TimestampTz
        );
    }

    #[test]
    fn unsigned_ints() {
        assert_eq!(resolve_type("UINT8").unwrap(), LogicalType::UInt8);
        assert_eq!(resolve_type("UINT16").unwrap(), LogicalType::UInt16);
        assert_eq!(resolve_type("UINT32").unwrap(), LogicalType::UInt32);
        assert_eq!(resolve_type("UINT64").unwrap(), LogicalType::UInt64);
    }
}

//! kyu-binder: semantic analysis, name -> ID resolution.

pub mod binder;
pub mod bound_statement;
pub mod expression_binder;
pub mod scope;

pub use binder::Binder;
pub use bound_statement::*;
pub use expression_binder::BindContext;
pub use scope::{BinderScope, VariableInfo};

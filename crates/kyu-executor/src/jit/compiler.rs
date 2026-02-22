//! Cranelift JIT compiler — lowers BoundExpression trees to native code.
//!
//! Compiled functions operate directly on FlatVector byte buffers and NullMask
//! u64 arrays. No TypedValue allocation, no match dispatch on the hot path.

use std::collections::BTreeSet;

use cranelift_codegen::ir::condcodes::{FloatCC, IntCC};
use cranelift_codegen::ir::types;
use cranelift_codegen::ir::{AbiParam, BlockArg, InstBuilder, MemFlags, Type, Value};
use cranelift_codegen::settings::{self, Configurable};
use cranelift_frontend::{FunctionBuilder, FunctionBuilderContext};
use cranelift_jit::{JITBuilder, JITModule};
use cranelift_module::{default_libcall_names, Linkage, Module};

use kyu_expression::BoundExpression;
use kyu_parser::ast::{BinaryOp, ComparisonOp, UnaryOp};
use kyu_types::LogicalType;

/// Errors during JIT compilation.
#[derive(Debug)]
pub enum JitError {
    /// Expression contains nodes that cannot be JIT-compiled.
    Unsupported(String),
    /// Cranelift compilation failed.
    Codegen(String),
    /// Module/ISA setup failed.
    Setup(String),
}

impl std::fmt::Display for JitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JitError::Unsupported(msg) => write!(f, "unsupported: {msg}"),
            JitError::Codegen(msg) => write!(f, "codegen: {msg}"),
            JitError::Setup(msg) => write!(f, "setup: {msg}"),
        }
    }
}

/// A JIT-compiled filter function.
///
/// Evaluates a predicate over columnar data and writes passing row indices
/// to an output buffer. The `_module` field keeps code pages alive.
pub struct CompiledFilter {
    /// Native function pointer.
    ///
    /// Signature: `(col_ptrs: *const *const u8, null_ptrs: *const *const u64,
    ///              sel_ptr: *const u32, num_rows: u32, out_sel: *mut u32) -> u32`
    ///
    /// - `col_ptrs`: array of pointers to flat column data buffers
    /// - `null_ptrs`: array of pointers to NullMask u64 arrays
    /// - `sel_ptr`: selection vector indices (null pointer = identity)
    /// - `num_rows`: number of logical rows to evaluate
    /// - `out_sel`: output buffer for selected physical row indices
    /// - returns: number of selected rows written to `out_sel`
    fn_ptr: unsafe extern "C" fn(*const *const u8, *const *const u64, *const u32, u32, *mut u32) -> u32,
    /// Keeps the JIT code pages alive.
    _module: JITModule,
    /// Which column indices this expression reads (sorted, deduplicated).
    pub col_indices: Vec<u32>,
}

// SAFETY: After `JITModule::finalize_definitions()`, code pages are immutable and
// the fn_ptr is a plain function pointer safe to call from any thread. The `_module`
// field only keeps the code memory alive and is never mutated post-finalization.
unsafe impl Send for CompiledFilter {}
unsafe impl Sync for CompiledFilter {}

impl CompiledFilter {
    /// Execute the compiled filter on raw column data.
    ///
    /// # Safety
    /// Caller must ensure pointers are valid and buffers are large enough.
    #[inline]
    pub unsafe fn execute(
        &self,
        col_ptrs: &[*const u8],
        null_ptrs: &[*const u64],
        sel_ptr: *const u32,
        num_rows: u32,
        out_sel: &mut [u32],
    ) -> u32 {
        unsafe {
            (self.fn_ptr)(
                col_ptrs.as_ptr(),
                null_ptrs.as_ptr(),
                sel_ptr,
                num_rows,
                out_sel.as_mut_ptr(),
            )
        }
    }
}

/// A JIT-compiled projection function.
///
/// Evaluates an expression over columnar data and writes computed values
/// into a flat output buffer + null mask.
pub struct CompiledProjection {
    /// Native function pointer.
    ///
    /// Signature: `(col_ptrs: *const *const u8, null_ptrs: *const *const u64,
    ///              sel_ptr: *const u32, num_rows: u32,
    ///              out_data: *mut u8, out_nulls: *mut u64)`
    fn_ptr: unsafe extern "C" fn(*const *const u8, *const *const u64, *const u32, u32, *mut u8, *mut u64),
    /// Keeps the JIT code pages alive.
    _module: JITModule,
    /// Which column indices this expression reads (sorted, deduplicated).
    pub col_indices: Vec<u32>,
    /// The output logical type.
    pub output_type: LogicalType,
}

// SAFETY: Same as CompiledFilter — post-finalization JITModule is safe to share.
unsafe impl Send for CompiledProjection {}
unsafe impl Sync for CompiledProjection {}

impl CompiledProjection {
    /// Execute the compiled projection on raw column data.
    ///
    /// # Safety
    /// Caller must ensure pointers are valid and buffers are large enough.
    /// `out_data` must hold at least `num_rows * type_stride` bytes.
    /// `out_nulls` must hold at least `ceil(num_rows / 64)` u64 entries.
    #[inline]
    pub unsafe fn execute(
        &self,
        col_ptrs: &[*const u8],
        null_ptrs: &[*const u64],
        sel_ptr: *const u32,
        num_rows: u32,
        out_data: &mut [u8],
        out_nulls: &mut [u64],
    ) {
        unsafe {
            (self.fn_ptr)(
                col_ptrs.as_ptr(),
                null_ptrs.as_ptr(),
                sel_ptr,
                num_rows,
                out_data.as_mut_ptr(),
                out_nulls.as_mut_ptr(),
            )
        }
    }
}

/// Compile a filter expression to a native function.
///
/// The expression must be JIT-eligible (checked by `is_jit_eligible`).
/// Returns `Err(JitError::Unsupported)` if any node can't be compiled.
pub fn compile_filter(expr: &BoundExpression) -> Result<CompiledFilter, JitError> {
    // Collect referenced column indices.
    let mut col_set = BTreeSet::new();
    collect_columns(expr, &mut col_set);
    let col_indices: Vec<u32> = col_set.into_iter().collect();

    // Build a mapping: column_index → position in col_ptrs array.
    let col_map: Vec<(u32, usize)> = col_indices.iter().enumerate().map(|(i, &c)| (c, i)).collect();

    // Set up Cranelift ISA for the host.
    let mut flag_builder = settings::builder();
    flag_builder
        .set("opt_level", "speed")
        .map_err(|e| JitError::Setup(e.to_string()))?;
    flag_builder
        .set("is_pic", "false")
        .map_err(|e| JitError::Setup(e.to_string()))?;

    let isa_builder =
        cranelift_native::builder().map_err(|e| JitError::Setup(e.to_string()))?;
    let isa = isa_builder
        .finish(settings::Flags::new(flag_builder))
        .map_err(|e| JitError::Setup(e.to_string()))?;

    let builder = JITBuilder::with_isa(isa, default_libcall_names());
    // On macOS Apple Silicon, JITBuilder handles MAP_JIT automatically.
    let mut module = JITModule::new(builder);
    let mut ctx = module.make_context();
    let mut func_ctx = FunctionBuilderContext::new();

    let ptr_type = module.target_config().pointer_type();

    // Signature: (col_ptrs, null_ptrs, sel_ptr, num_rows, out_sel) -> count
    ctx.func.signature.params = vec![
        AbiParam::new(ptr_type), // col_ptrs: *const *const u8
        AbiParam::new(ptr_type), // null_ptrs: *const *const u64
        AbiParam::new(ptr_type), // sel_ptr: *const u32
        AbiParam::new(types::I32), // num_rows: u32
        AbiParam::new(ptr_type), // out_sel: *mut u32
    ];
    ctx.func.signature.returns = vec![AbiParam::new(types::I32)]; // count

    let func_id = module
        .declare_function("jit_filter", Linkage::Local, &ctx.func.signature)
        .map_err(|e| JitError::Codegen(e.to_string()))?;

    {
        let mut builder = FunctionBuilder::new(&mut ctx.func, &mut func_ctx);

        // Blocks
        let entry_block = builder.create_block();
        let loop_header = builder.create_block();
        let loop_body = builder.create_block();
        let sel_identity_block = builder.create_block();
        let sel_explicit_block = builder.create_block();
        let after_sel_block = builder.create_block();
        let loop_pass = builder.create_block();
        let loop_inc = builder.create_block();
        let exit_block = builder.create_block();

        // after_sel_block receives phys_row as a block parameter (I32).
        builder.append_block_param(after_sel_block, types::I32);

        builder.append_block_params_for_function_params(entry_block);
        builder.switch_to_block(entry_block);

        // Extract parameters.
        let col_ptrs_arg = builder.block_params(entry_block)[0];
        let null_ptrs_arg = builder.block_params(entry_block)[1];
        let sel_ptr_arg = builder.block_params(entry_block)[2];
        let num_rows_arg = builder.block_params(entry_block)[3];
        let out_sel_arg = builder.block_params(entry_block)[4];

        // Variables: loop index (i), output count
        let var_i = builder.declare_var(types::I32);
        let var_count = builder.declare_var(types::I32);

        let zero_i32 = builder.ins().iconst(types::I32, 0);
        builder.def_var(var_i, zero_i32);
        builder.def_var(var_count, zero_i32);

        // Load column data pointers from the col_ptrs array.
        // col_data[k] = col_ptrs[k] (each is a *const u8)
        let col_data: Vec<Value> = col_map
            .iter()
            .map(|&(_, pos)| {
                let offset = (pos as i32) * ptr_type.bytes() as i32;
                builder
                    .ins()
                    .load(ptr_type, MemFlags::trusted(), col_ptrs_arg, offset)
            })
            .collect();

        // Load null mask pointers.
        let null_data: Vec<Value> = col_map
            .iter()
            .map(|&(_, pos)| {
                let offset = (pos as i32) * ptr_type.bytes() as i32;
                builder
                    .ins()
                    .load(ptr_type, MemFlags::trusted(), null_ptrs_arg, offset)
            })
            .collect();

        builder.ins().jump(loop_header, &[]);

        // Loop header: check i < num_rows
        builder.switch_to_block(loop_header);
        let i = builder.use_var(var_i);
        let cmp = builder.ins().icmp(IntCC::UnsignedLessThan, i, num_rows_arg);
        builder.ins().brif(cmp, loop_body, &[], exit_block, &[]);

        // Loop body: resolve physical row index via branch to avoid null ptr load.
        builder.switch_to_block(loop_body);

        // Branch: if sel_ptr is null → identity, else → load from sel_ptr.
        let sel_is_null = builder.ins().icmp_imm(IntCC::Equal, sel_ptr_arg, 0);
        builder.ins().brif(sel_is_null, sel_identity_block, &[], sel_explicit_block, &[]);

        // Identity selection: phys_row = i
        builder.switch_to_block(sel_identity_block);
        let i_identity = builder.use_var(var_i);
        builder.ins().jump(after_sel_block, &[BlockArg::Value(i_identity)]);

        // Explicit selection: phys_row = sel_ptr[i]
        builder.switch_to_block(sel_explicit_block);
        let i_explicit = builder.use_var(var_i);
        let i_ext = builder.ins().uextend(ptr_type, i_explicit);
        let sel_offset = builder.ins().ishl_imm(i_ext, 2); // i * 4
        let sel_addr = builder.ins().iadd(sel_ptr_arg, sel_offset);
        let sel_loaded = builder.ins().load(types::I32, MemFlags::trusted(), sel_addr, 0);
        builder.ins().jump(after_sel_block, &[BlockArg::Value(sel_loaded)]);

        // After selection: phys_row is the block parameter.
        builder.switch_to_block(after_sel_block);
        let phys_row = builder.block_params(after_sel_block)[0];

        // Emit expression evaluation.
        let result = emit_expr(
            &mut builder,
            expr,
            phys_row,
            &col_data,
            &null_data,
            &col_map,
            ptr_type,
        )?;

        // Branch: if result is true (nonzero i8), write to output.
        let is_true = builder.ins().icmp_imm(IntCC::NotEqual, result, 0);
        builder.ins().brif(is_true, loop_pass, &[], loop_inc, &[]);

        // Loop pass: write phys_row to out_sel[count], increment count.
        builder.switch_to_block(loop_pass);
        let count = builder.use_var(var_count);
        let count_ext = builder.ins().uextend(ptr_type, count);
        let out_offset = builder.ins().ishl_imm(count_ext, 2); // count * 4
        let out_addr = builder.ins().iadd(out_sel_arg, out_offset);
        builder.ins().store(MemFlags::trusted(), phys_row, out_addr, 0);
        let count_inc = builder.ins().iadd_imm(count, 1);
        builder.def_var(var_count, count_inc);
        builder.ins().jump(loop_inc, &[]);

        // Loop increment: i += 1, jump back to header.
        builder.switch_to_block(loop_inc);
        let i = builder.use_var(var_i);
        let i_next = builder.ins().iadd_imm(i, 1);
        builder.def_var(var_i, i_next);
        builder.ins().jump(loop_header, &[]);

        // Exit: return count.
        builder.switch_to_block(exit_block);
        let count = builder.use_var(var_count);
        builder.ins().return_(&[count]);

        builder.seal_all_blocks();
        builder.finalize();
    }

    module
        .define_function(func_id, &mut ctx)
        .map_err(|e| JitError::Codegen(e.to_string()))?;
    module
        .finalize_definitions()
        .map_err(|e| JitError::Codegen(e.to_string()))?;

    let raw_ptr = module.get_finalized_function(func_id);
    // SAFETY: The function signature matches our declared Cranelift signature exactly.
    // The `_module` field on CompiledFilter keeps the code pages alive.
    let fn_ptr = unsafe {
        std::mem::transmute::<
            *const u8,
            unsafe extern "C" fn(*const *const u8, *const *const u64, *const u32, u32, *mut u32) -> u32,
        >(raw_ptr)
    };

    Ok(CompiledFilter {
        fn_ptr,
        _module: module,
        col_indices,
    })
}

/// Compile a projection expression to a native function.
///
/// The expression must be JIT-eligible. The compiled function evaluates the
/// expression for each row and writes the result into a flat output buffer.
/// If any input variable is null, the output null bit is set and computation
/// is skipped for that row.
pub fn compile_projection(expr: &BoundExpression) -> Result<CompiledProjection, JitError> {
    let output_type = expr.result_type().clone();
    let _output_clif = clif_type(&output_type)?; // validate type is JIT-compatible
    let output_stride = type_stride(&output_type)?;

    let mut col_set = BTreeSet::new();
    collect_columns(expr, &mut col_set);
    let col_indices: Vec<u32> = col_set.into_iter().collect();
    let col_map: Vec<(u32, usize)> = col_indices.iter().enumerate().map(|(i, &c)| (c, i)).collect();

    // Set up Cranelift ISA.
    let mut flag_builder = settings::builder();
    flag_builder
        .set("opt_level", "speed")
        .map_err(|e| JitError::Setup(e.to_string()))?;
    flag_builder
        .set("is_pic", "false")
        .map_err(|e| JitError::Setup(e.to_string()))?;

    let isa_builder =
        cranelift_native::builder().map_err(|e| JitError::Setup(e.to_string()))?;
    let isa = isa_builder
        .finish(settings::Flags::new(flag_builder))
        .map_err(|e| JitError::Setup(e.to_string()))?;

    let builder = JITBuilder::with_isa(isa, default_libcall_names());
    let mut module = JITModule::new(builder);
    let mut ctx = module.make_context();
    let mut func_ctx = FunctionBuilderContext::new();

    let ptr_type = module.target_config().pointer_type();

    // Signature: (col_ptrs, null_ptrs, sel_ptr, num_rows, out_data, out_nulls) -> void
    ctx.func.signature.params = vec![
        AbiParam::new(ptr_type), // col_ptrs
        AbiParam::new(ptr_type), // null_ptrs
        AbiParam::new(ptr_type), // sel_ptr
        AbiParam::new(types::I32), // num_rows
        AbiParam::new(ptr_type), // out_data
        AbiParam::new(ptr_type), // out_nulls
    ];
    // No return value.

    let func_id = module
        .declare_function("jit_project", Linkage::Local, &ctx.func.signature)
        .map_err(|e| JitError::Codegen(e.to_string()))?;

    {
        let mut builder = FunctionBuilder::new(&mut ctx.func, &mut func_ctx);

        let entry_block = builder.create_block();
        let loop_header = builder.create_block();
        let loop_body = builder.create_block();
        let sel_identity_block = builder.create_block();
        let sel_explicit_block = builder.create_block();
        let after_sel_block = builder.create_block();
        let null_check_block = builder.create_block();
        let compute_block = builder.create_block();
        let set_null_block = builder.create_block();
        let loop_inc = builder.create_block();
        let exit_block = builder.create_block();

        // after_sel_block receives phys_row as block parameter (I32).
        builder.append_block_param(after_sel_block, types::I32);

        builder.append_block_params_for_function_params(entry_block);
        builder.switch_to_block(entry_block);

        let col_ptrs_arg = builder.block_params(entry_block)[0];
        let null_ptrs_arg = builder.block_params(entry_block)[1];
        let sel_ptr_arg = builder.block_params(entry_block)[2];
        let num_rows_arg = builder.block_params(entry_block)[3];
        let out_data_arg = builder.block_params(entry_block)[4];
        let out_nulls_arg = builder.block_params(entry_block)[5];

        let var_i = builder.declare_var(types::I32);
        let zero_i32 = builder.ins().iconst(types::I32, 0);
        builder.def_var(var_i, zero_i32);

        // Load column data and null mask pointers.
        let col_data: Vec<Value> = col_map
            .iter()
            .map(|&(_, pos)| {
                let offset = (pos as i32) * ptr_type.bytes() as i32;
                builder
                    .ins()
                    .load(ptr_type, MemFlags::trusted(), col_ptrs_arg, offset)
            })
            .collect();

        let null_data: Vec<Value> = col_map
            .iter()
            .map(|&(_, pos)| {
                let offset = (pos as i32) * ptr_type.bytes() as i32;
                builder
                    .ins()
                    .load(ptr_type, MemFlags::trusted(), null_ptrs_arg, offset)
            })
            .collect();

        builder.ins().jump(loop_header, &[]);

        // Loop header: check i < num_rows
        builder.switch_to_block(loop_header);
        let i = builder.use_var(var_i);
        let cmp = builder.ins().icmp(IntCC::UnsignedLessThan, i, num_rows_arg);
        builder.ins().brif(cmp, loop_body, &[], exit_block, &[]);

        // Loop body: resolve selection.
        builder.switch_to_block(loop_body);
        let sel_is_null = builder.ins().icmp_imm(IntCC::Equal, sel_ptr_arg, 0);
        builder.ins().brif(sel_is_null, sel_identity_block, &[], sel_explicit_block, &[]);

        // Identity selection: phys_row = i
        builder.switch_to_block(sel_identity_block);
        let i_identity = builder.use_var(var_i);
        builder.ins().jump(after_sel_block, &[BlockArg::Value(i_identity)]);

        // Explicit selection: phys_row = sel_ptr[i]
        builder.switch_to_block(sel_explicit_block);
        let i_explicit = builder.use_var(var_i);
        let i_ext = builder.ins().uextend(ptr_type, i_explicit);
        let sel_offset = builder.ins().ishl_imm(i_ext, 2);
        let sel_addr = builder.ins().iadd(sel_ptr_arg, sel_offset);
        let sel_loaded = builder.ins().load(types::I32, MemFlags::trusted(), sel_addr, 0);
        builder.ins().jump(after_sel_block, &[BlockArg::Value(sel_loaded)]);

        // After selection: phys_row is the block parameter.
        builder.switch_to_block(after_sel_block);
        let phys_row = builder.block_params(after_sel_block)[0];

        // Check nulls for all referenced columns. If any is null, jump to set_null_block.
        if col_indices.is_empty() {
            // Pure literal expression — no null checks needed.
            builder.ins().jump(compute_block, &[]);
        } else {
            builder.ins().jump(null_check_block, &[]);
        }

        builder.switch_to_block(null_check_block);
        // OR together null bits for all referenced columns.
        let mut any_null = builder.ins().iconst(types::I8, 0);
        for &(_, pos) in &col_map {
            let bit = emit_null_check(&mut builder, null_data[pos], phys_row, ptr_type);
            any_null = builder.ins().bor(any_null, bit);
        }
        let is_any_null = builder.ins().icmp_imm(IntCC::NotEqual, any_null, 0);
        builder.ins().brif(is_any_null, set_null_block, &[], compute_block, &[]);

        // Compute: evaluate expression and write result.
        builder.switch_to_block(compute_block);
        let result = emit_expr(
            &mut builder,
            expr,
            phys_row,
            &col_data,
            &null_data,
            &col_map,
            ptr_type,
        )?;

        // Write result to out_data[i * stride].
        let i_val = builder.use_var(var_i);
        let i_ext = builder.ins().uextend(ptr_type, i_val);
        let out_offset = builder.ins().imul_imm(i_ext, output_stride as i64);
        let out_addr = builder.ins().iadd(out_data_arg, out_offset);
        builder.ins().store(MemFlags::trusted(), result, out_addr, 0);

        // Clear output null bit: out_nulls[i/64] &= ~(1 << (i%64))
        // Actually, if output null mask is pre-initialized to zeros, we can skip this.
        // The caller initializes out_nulls to all-zero (non-null), so we only need
        // to set bits in the set_null path.
        builder.ins().jump(loop_inc, &[]);

        // Set null: mark output null bit for row i.
        builder.switch_to_block(set_null_block);
        let i_val = builder.use_var(var_i);
        emit_set_null_bit(&mut builder, out_nulls_arg, i_val, ptr_type);
        builder.ins().jump(loop_inc, &[]);

        // Loop increment.
        builder.switch_to_block(loop_inc);
        let i = builder.use_var(var_i);
        let i_next = builder.ins().iadd_imm(i, 1);
        builder.def_var(var_i, i_next);
        builder.ins().jump(loop_header, &[]);

        // Exit.
        builder.switch_to_block(exit_block);
        builder.ins().return_(&[]);

        builder.seal_all_blocks();
        builder.finalize();
    }

    module
        .define_function(func_id, &mut ctx)
        .map_err(|e| JitError::Codegen(e.to_string()))?;
    module
        .finalize_definitions()
        .map_err(|e| JitError::Codegen(e.to_string()))?;

    let raw_ptr = module.get_finalized_function(func_id);
    let fn_ptr = unsafe {
        std::mem::transmute::<
            *const u8,
            unsafe extern "C" fn(*const *const u8, *const *const u64, *const u32, u32, *mut u8, *mut u64),
        >(raw_ptr)
    };

    Ok(CompiledProjection {
        fn_ptr,
        _module: module,
        col_indices,
        output_type,
    })
}

/// Collect all column indices referenced by the expression tree.
fn collect_columns(expr: &BoundExpression, out: &mut BTreeSet<u32>) {
    match expr {
        BoundExpression::Variable { index, .. } => {
            out.insert(*index);
        }
        BoundExpression::Literal { .. } => {}
        BoundExpression::UnaryOp { operand, .. } => collect_columns(operand, out),
        BoundExpression::BinaryOp { left, right, .. } => {
            collect_columns(left, out);
            collect_columns(right, out);
        }
        BoundExpression::Comparison { left, right, .. } => {
            collect_columns(left, out);
            collect_columns(right, out);
        }
        BoundExpression::IsNull { expr, .. } => collect_columns(expr, out),
        BoundExpression::Cast { expr, .. } => collect_columns(expr, out),
        _ => {}
    }
}

/// Cranelift type for a LogicalType.
fn clif_type(ty: &LogicalType) -> Result<Type, JitError> {
    match ty {
        LogicalType::Int8 => Ok(types::I8),
        LogicalType::Int16 => Ok(types::I16),
        LogicalType::Int32 => Ok(types::I32),
        LogicalType::Int64 | LogicalType::Serial => Ok(types::I64),
        LogicalType::Float => Ok(types::F32),
        LogicalType::Double => Ok(types::F64),
        LogicalType::Bool => Ok(types::I8),
        _ => Err(JitError::Unsupported(format!("type {ty:?}"))),
    }
}

/// Byte stride for a LogicalType (used to compute pointer offsets into flat buffers).
fn type_stride(ty: &LogicalType) -> Result<i32, JitError> {
    match ty {
        LogicalType::Int8 | LogicalType::Bool => Ok(1),
        LogicalType::Int16 => Ok(2),
        LogicalType::Int32 | LogicalType::Float => Ok(4),
        LogicalType::Int64 | LogicalType::Serial | LogicalType::Double => Ok(8),
        _ => Err(JitError::Unsupported(format!("stride for {ty:?}"))),
    }
}

/// Emit Cranelift IR for an expression node. Returns a Value representing
/// the result: I64/I32/F64/F32 for data values, I8 for booleans.
fn emit_expr(
    builder: &mut FunctionBuilder,
    expr: &BoundExpression,
    phys_row: Value,
    col_data: &[Value],
    null_data: &[Value],
    col_map: &[(u32, usize)],
    ptr_type: Type,
) -> Result<Value, JitError> {
    match expr {
        BoundExpression::Literal { value, result_type } => {
            emit_literal(builder, value, result_type)
        }

        BoundExpression::Variable { index, result_type } => {
            let pos = col_map
                .iter()
                .find(|&&(c, _)| c == *index)
                .map(|&(_, p)| p)
                .ok_or_else(|| JitError::Codegen(format!("column {index} not in col_map")))?;

            let ty = clif_type(result_type)?;
            let stride = type_stride(result_type)?;

            // offset = phys_row * stride
            let row_ext = builder.ins().uextend(ptr_type, phys_row);
            let offset = builder.ins().imul_imm(row_ext, stride as i64);
            let addr = builder.ins().iadd(col_data[pos], offset);
            Ok(builder.ins().load(ty, MemFlags::trusted(), addr, 0))
        }

        BoundExpression::UnaryOp { op, operand, result_type } => {
            let val = emit_expr(builder, operand, phys_row, col_data, null_data, col_map, ptr_type)?;
            match op {
                UnaryOp::Minus => {
                    let ty = clif_type(result_type)?;
                    if ty.is_float() {
                        Ok(builder.ins().fneg(val))
                    } else {
                        Ok(builder.ins().ineg(val))
                    }
                }
                UnaryOp::Not => {
                    // Boolean NOT: XOR with 1
                    Ok(builder.ins().bxor_imm(val, 1))
                }
                UnaryOp::BitwiseNot => Ok(builder.ins().bnot(val)),
            }
        }

        BoundExpression::BinaryOp { op, left, right, result_type } => {
            let lhs = emit_expr(builder, left, phys_row, col_data, null_data, col_map, ptr_type)?;
            let rhs = emit_expr(builder, right, phys_row, col_data, null_data, col_map, ptr_type)?;

            let ty = clif_type(result_type)?;
            emit_binop(builder, op, lhs, rhs, ty)
        }

        BoundExpression::Comparison { op, left, right } => {
            let lhs = emit_expr(builder, left, phys_row, col_data, null_data, col_map, ptr_type)?;
            let rhs = emit_expr(builder, right, phys_row, col_data, null_data, col_map, ptr_type)?;

            let left_type = left.result_type();
            let left_clif = clif_type(left_type)?;
            emit_cmp(builder, op, lhs, rhs, left_clif)
        }

        BoundExpression::IsNull { expr, negated } => {
            // For IsNull we need to check the null mask bit for the variable.
            // If the child is a Variable, check its null mask directly.
            // For complex sub-expressions, we can't easily check nulls — fall back.
            if let BoundExpression::Variable { index, .. } = expr.as_ref() {
                let pos = col_map
                    .iter()
                    .find(|&&(c, _)| c == *index)
                    .map(|&(_, p)| p)
                    .ok_or_else(|| {
                        JitError::Codegen(format!("column {index} not in col_map"))
                    })?;

                let bit = emit_null_check(builder, null_data[pos], phys_row, ptr_type);
                if *negated {
                    // IS NOT NULL: invert
                    Ok(builder.ins().bxor_imm(bit, 1))
                } else {
                    Ok(bit)
                }
            } else {
                Err(JitError::Unsupported("IsNull on non-variable".into()))
            }
        }

        BoundExpression::Cast { expr, target_type } => {
            let val = emit_expr(builder, expr, phys_row, col_data, null_data, col_map, ptr_type)?;
            let src_type = expr.result_type();
            emit_cast(builder, val, src_type, target_type)
        }

        _ => Err(JitError::Unsupported(format!("{expr:?}"))),
    }
}

fn emit_literal(
    builder: &mut FunctionBuilder,
    value: &kyu_types::TypedValue,
    _result_type: &LogicalType,
) -> Result<Value, JitError> {
    use kyu_types::TypedValue;
    match value {
        TypedValue::Int8(v) => Ok(builder.ins().iconst(types::I8, *v as i64)),
        TypedValue::Int16(v) => Ok(builder.ins().iconst(types::I16, *v as i64)),
        TypedValue::Int32(v) => Ok(builder.ins().iconst(types::I32, *v as i64)),
        TypedValue::Int64(v) => Ok(builder.ins().iconst(types::I64, *v)),
        TypedValue::Float(v) => Ok(builder.ins().f32const(*v)),
        TypedValue::Double(v) => Ok(builder.ins().f64const(*v)),
        TypedValue::Bool(v) => Ok(builder.ins().iconst(types::I8, if *v { 1 } else { 0 })),
        _ => Err(JitError::Unsupported(format!("literal {value:?}"))),
    }
}

fn emit_binop(
    builder: &mut FunctionBuilder,
    op: &BinaryOp,
    lhs: Value,
    rhs: Value,
    ty: Type,
) -> Result<Value, JitError> {
    if ty.is_float() {
        match op {
            BinaryOp::Add => Ok(builder.ins().fadd(lhs, rhs)),
            BinaryOp::Sub => Ok(builder.ins().fsub(lhs, rhs)),
            BinaryOp::Mul => Ok(builder.ins().fmul(lhs, rhs)),
            BinaryOp::Div => Ok(builder.ins().fdiv(lhs, rhs)),
            _ => Err(JitError::Unsupported(format!("float binop {op:?}"))),
        }
    } else {
        match op {
            BinaryOp::Add => Ok(builder.ins().iadd(lhs, rhs)),
            BinaryOp::Sub => Ok(builder.ins().isub(lhs, rhs)),
            BinaryOp::Mul => Ok(builder.ins().imul(lhs, rhs)),
            BinaryOp::Div => Ok(builder.ins().sdiv(lhs, rhs)),
            BinaryOp::Mod => Ok(builder.ins().srem(lhs, rhs)),
            BinaryOp::And | BinaryOp::BitwiseAnd => Ok(builder.ins().band(lhs, rhs)),
            BinaryOp::Or | BinaryOp::BitwiseOr => Ok(builder.ins().bor(lhs, rhs)),
            BinaryOp::ShiftLeft => Ok(builder.ins().ishl(lhs, rhs)),
            BinaryOp::ShiftRight => Ok(builder.ins().sshr(lhs, rhs)),
            _ => Err(JitError::Unsupported(format!("int binop {op:?}"))),
        }
    }
}

fn emit_cmp(
    builder: &mut FunctionBuilder,
    op: &ComparisonOp,
    lhs: Value,
    rhs: Value,
    ty: Type,
) -> Result<Value, JitError> {
    if ty.is_float() {
        let cc = match op {
            ComparisonOp::Eq => FloatCC::Equal,
            ComparisonOp::Neq => FloatCC::NotEqual,
            ComparisonOp::Lt => FloatCC::LessThan,
            ComparisonOp::Le => FloatCC::LessThanOrEqual,
            ComparisonOp::Gt => FloatCC::GreaterThan,
            ComparisonOp::Ge => FloatCC::GreaterThanOrEqual,
            _ => return Err(JitError::Unsupported(format!("float cmp {op:?}"))),
        };
        // fcmp returns I8 (0 or 1) in modern Cranelift.
        Ok(builder.ins().fcmp(cc, lhs, rhs))
    } else {
        let cc = match op {
            ComparisonOp::Eq => IntCC::Equal,
            ComparisonOp::Neq => IntCC::NotEqual,
            ComparisonOp::Lt => IntCC::SignedLessThan,
            ComparisonOp::Le => IntCC::SignedLessThanOrEqual,
            ComparisonOp::Gt => IntCC::SignedGreaterThan,
            ComparisonOp::Ge => IntCC::SignedGreaterThanOrEqual,
            _ => return Err(JitError::Unsupported(format!("int cmp {op:?}"))),
        };
        Ok(builder.ins().icmp(cc, lhs, rhs))
    }
}

/// Emit a null check: returns I8 (1 if null, 0 if not null).
///
/// null_mask layout: bit=1 means NULL, packed in u64 words.
/// null_bit = (null_ptr[phys_row / 64] >> (phys_row % 64)) & 1
fn emit_null_check(
    builder: &mut FunctionBuilder,
    null_ptr: Value,
    phys_row: Value,
    ptr_type: Type,
) -> Value {
    let row_ext = builder.ins().uextend(types::I64, phys_row);

    // word_index = phys_row >> 6 (divide by 64)
    let word_idx = builder.ins().ushr_imm(row_ext, 6);
    // byte_offset = word_index * 8
    let byte_offset = builder.ins().ishl_imm(word_idx, 3);
    // Load the u64 word.
    let word_idx_ptr = if ptr_type == types::I64 {
        byte_offset
    } else {
        builder.ins().ireduce(ptr_type, byte_offset)
    };
    let word_addr = builder.ins().iadd(null_ptr, word_idx_ptr);
    let word = builder.ins().load(types::I64, MemFlags::trusted(), word_addr, 0);

    // bit_index = phys_row & 63
    let bit_idx = builder.ins().band_imm(row_ext, 63);
    // shifted = word >> bit_index
    let shifted = builder.ins().ushr(word, bit_idx);
    // result = shifted & 1
    let one = builder.ins().iconst(types::I64, 1);
    let bit = builder.ins().band(shifted, one);

    // Truncate to I8 for boolean.
    builder.ins().ireduce(types::I8, bit)
}

/// Emit code to set a null bit in an output null mask.
///
/// out_nulls[row / 64] |= (1 << (row % 64))
fn emit_set_null_bit(
    builder: &mut FunctionBuilder,
    out_nulls: Value,
    row: Value,
    ptr_type: Type,
) {
    let row_ext = builder.ins().uextend(types::I64, row);

    // word_index = row >> 6
    let word_idx = builder.ins().ushr_imm(row_ext, 6);
    // byte_offset = word_index * 8
    let byte_offset = builder.ins().ishl_imm(word_idx, 3);
    let word_idx_ptr = if ptr_type == types::I64 {
        byte_offset
    } else {
        builder.ins().ireduce(ptr_type, byte_offset)
    };
    let word_addr = builder.ins().iadd(out_nulls, word_idx_ptr);

    // Load current word.
    let word = builder.ins().load(types::I64, MemFlags::trusted(), word_addr, 0);

    // bit_index = row & 63
    let bit_idx = builder.ins().band_imm(row_ext, 63);
    // bit_mask = 1 << bit_index
    let one = builder.ins().iconst(types::I64, 1);
    let bit_mask = builder.ins().ishl(one, bit_idx);

    // word |= bit_mask
    let new_word = builder.ins().bor(word, bit_mask);
    builder.ins().store(MemFlags::trusted(), new_word, word_addr, 0);
}

/// Emit a numeric cast.
fn emit_cast(
    builder: &mut FunctionBuilder,
    val: Value,
    src_type: &LogicalType,
    target_type: &LogicalType,
) -> Result<Value, JitError> {
    let src_clif = clif_type(src_type)?;
    let tgt_clif = clif_type(target_type)?;

    if src_clif == tgt_clif {
        return Ok(val);
    }

    // Integer → integer (widening/narrowing)
    if src_clif.is_int() && tgt_clif.is_int() {
        return if tgt_clif.bits() > src_clif.bits() {
            Ok(builder.ins().sextend(tgt_clif, val))
        } else {
            Ok(builder.ins().ireduce(tgt_clif, val))
        };
    }

    // Integer → float
    if src_clif.is_int() && tgt_clif.is_float() {
        // Widen to I64 first if needed, then convert.
        let wide = if src_clif.bits() < 64 {
            builder.ins().sextend(types::I64, val)
        } else {
            val
        };
        if tgt_clif == types::F64 {
            return Ok(builder.ins().fcvt_from_sint(types::F64, wide));
        } else {
            let f64_val = builder.ins().fcvt_from_sint(types::F64, wide);
            return Ok(builder.ins().fdemote(types::F32, f64_val));
        }
    }

    // Float → integer
    if src_clif.is_float() && tgt_clif.is_int() {
        let promoted = if src_clif == types::F32 {
            builder.ins().fpromote(types::F64, val)
        } else {
            val
        };
        let i64_val = builder.ins().fcvt_to_sint(types::I64, promoted);
        return if tgt_clif.bits() < 64 {
            Ok(builder.ins().ireduce(tgt_clif, i64_val))
        } else {
            Ok(i64_val)
        };
    }

    // Float → float (F32 ↔ F64)
    if src_clif.is_float() && tgt_clif.is_float() {
        return if tgt_clif.bits() > src_clif.bits() {
            Ok(builder.ins().fpromote(tgt_clif, val))
        } else {
            Ok(builder.ins().fdemote(tgt_clif, val))
        };
    }

    Err(JitError::Unsupported(format!(
        "cast {src_type:?} → {target_type:?}"
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_chunk::DataChunk;
    use crate::value_vector::{FlatVector, SelectionVector, ValueVector};
    use kyu_expression::BoundExpression;
    use kyu_storage::ColumnChunkData;
    use kyu_types::{LogicalType, TypedValue};

    fn make_i64_chunk(values: &[i64]) -> DataChunk {
        let mut col = ColumnChunkData::new(LogicalType::Int64, values.len() as u64);
        for &v in values {
            col.append_value::<i64>(v);
        }
        let flat = FlatVector::from_column_chunk(&col, values.len());
        DataChunk::from_vectors(
            vec![ValueVector::Flat(flat)],
            SelectionVector::identity(values.len()),
        )
    }

    fn make_i64_chunk_with_nulls(values: &[Option<i64>]) -> DataChunk {
        let mut col = ColumnChunkData::new(LogicalType::Int64, values.len() as u64);
        for val in values {
            match val {
                Some(v) => col.append_value::<i64>(*v),
                None => col.append_null(),
            }
        }
        let flat = FlatVector::from_column_chunk(&col, values.len());
        DataChunk::from_vectors(
            vec![ValueVector::Flat(flat)],
            SelectionVector::identity(values.len()),
        )
    }

    /// Helper: run a compiled filter against a DataChunk with one i64 column.
    unsafe fn run_filter(compiled: &CompiledFilter, chunk: &DataChunk) -> Vec<u32> {
        let col = chunk.column(0);
        let flat = match col {
            ValueVector::Flat(f) => f,
            _ => panic!("expected Flat"),
        };

        let col_ptrs = vec![flat.data_ptr()];
        let null_ptrs = vec![flat.null_mask().data().as_ptr()];
        let sel = chunk.selection();
        let sel_ptr = sel.indices_ptr();
        let n = sel.len() as u32;
        let mut out = vec![0u32; n as usize];

        let count = unsafe {
            compiled.execute(&col_ptrs, &null_ptrs, sel_ptr, n, &mut out)
        };
        out.truncate(count as usize);
        out
    }

    #[test]
    fn jit_filter_simple_gt() {
        // col[0] > 15
        let expr = BoundExpression::Comparison {
            op: ComparisonOp::Gt,
            left: Box::new(BoundExpression::Variable {
                index: 0,
                result_type: LogicalType::Int64,
            }),
            right: Box::new(BoundExpression::Literal {
                value: TypedValue::Int64(15),
                result_type: LogicalType::Int64,
            }),
        };

        let compiled = compile_filter(&expr).expect("compilation failed");
        let chunk = make_i64_chunk(&[10, 20, 5, 30, 3]);

        let selected = unsafe { run_filter(&compiled, &chunk) };
        // Physical rows with values > 15: row 1 (20), row 3 (30)
        assert_eq!(selected, vec![1, 3]);
    }

    #[test]
    fn jit_filter_eq() {
        // col[0] == 20
        let expr = BoundExpression::Comparison {
            op: ComparisonOp::Eq,
            left: Box::new(BoundExpression::Variable {
                index: 0,
                result_type: LogicalType::Int64,
            }),
            right: Box::new(BoundExpression::Literal {
                value: TypedValue::Int64(20),
                result_type: LogicalType::Int64,
            }),
        };

        let compiled = compile_filter(&expr).expect("compilation failed");
        let chunk = make_i64_chunk(&[10, 20, 30, 20]);

        let selected = unsafe { run_filter(&compiled, &chunk) };
        assert_eq!(selected, vec![1, 3]);
    }

    #[test]
    fn jit_filter_compound_and() {
        // col[0] > 5 AND col[0] < 25
        let left = BoundExpression::Comparison {
            op: ComparisonOp::Gt,
            left: Box::new(BoundExpression::Variable {
                index: 0,
                result_type: LogicalType::Int64,
            }),
            right: Box::new(BoundExpression::Literal {
                value: TypedValue::Int64(5),
                result_type: LogicalType::Int64,
            }),
        };
        let right = BoundExpression::Comparison {
            op: ComparisonOp::Lt,
            left: Box::new(BoundExpression::Variable {
                index: 0,
                result_type: LogicalType::Int64,
            }),
            right: Box::new(BoundExpression::Literal {
                value: TypedValue::Int64(25),
                result_type: LogicalType::Int64,
            }),
        };
        let expr = BoundExpression::BinaryOp {
            op: BinaryOp::And,
            left: Box::new(left),
            right: Box::new(right),
            result_type: LogicalType::Bool,
        };

        let compiled = compile_filter(&expr).expect("compilation failed");
        let chunk = make_i64_chunk(&[3, 10, 25, 20, 5]);

        let selected = unsafe { run_filter(&compiled, &chunk) };
        // Values > 5 AND < 25: 10 (row 1), 20 (row 3)
        assert_eq!(selected, vec![1, 3]);
    }

    #[test]
    fn jit_filter_with_selection_vector() {
        // col[0] > 10, but only consider rows [0, 2, 4] via selection vector.
        let expr = BoundExpression::Comparison {
            op: ComparisonOp::Gt,
            left: Box::new(BoundExpression::Variable {
                index: 0,
                result_type: LogicalType::Int64,
            }),
            right: Box::new(BoundExpression::Literal {
                value: TypedValue::Int64(10),
                result_type: LogicalType::Int64,
            }),
        };

        let compiled = compile_filter(&expr).expect("compilation failed");

        // Physical data: [5, 20, 30, 3, 15]
        let mut col = ColumnChunkData::new(LogicalType::Int64, 5);
        for v in [5i64, 20, 30, 3, 15] {
            col.append_value::<i64>(v);
        }
        let flat = FlatVector::from_column_chunk(&col, 5);
        let chunk = DataChunk::from_vectors(
            vec![ValueVector::Flat(flat)],
            SelectionVector::from_indices(vec![0, 2, 4]),
        );

        let selected = unsafe { run_filter(&compiled, &chunk) };
        // Selection [0, 2, 4] → values [5, 30, 15]
        // > 10: 30 (phys row 2), 15 (phys row 4)
        assert_eq!(selected, vec![2, 4]);
    }

    #[test]
    fn jit_filter_with_nulls() {
        // IS NOT NULL for col[0]
        let expr = BoundExpression::IsNull {
            expr: Box::new(BoundExpression::Variable {
                index: 0,
                result_type: LogicalType::Int64,
            }),
            negated: true,
        };

        let compiled = compile_filter(&expr).expect("compilation failed");
        let chunk = make_i64_chunk_with_nulls(&[Some(10), None, Some(30), None, Some(50)]);

        let selected = unsafe { run_filter(&compiled, &chunk) };
        assert_eq!(selected, vec![0, 2, 4]);
    }

    #[test]
    fn jit_filter_empty_chunk() {
        let expr = BoundExpression::Comparison {
            op: ComparisonOp::Gt,
            left: Box::new(BoundExpression::Variable {
                index: 0,
                result_type: LogicalType::Int64,
            }),
            right: Box::new(BoundExpression::Literal {
                value: TypedValue::Int64(0),
                result_type: LogicalType::Int64,
            }),
        };

        let compiled = compile_filter(&expr).expect("compilation failed");
        let chunk = make_i64_chunk(&[]);

        let selected = unsafe { run_filter(&compiled, &chunk) };
        assert!(selected.is_empty());
    }

    #[test]
    fn jit_filter_all_pass() {
        // col[0] >= 0 — all values pass.
        let expr = BoundExpression::Comparison {
            op: ComparisonOp::Ge,
            left: Box::new(BoundExpression::Variable {
                index: 0,
                result_type: LogicalType::Int64,
            }),
            right: Box::new(BoundExpression::Literal {
                value: TypedValue::Int64(0),
                result_type: LogicalType::Int64,
            }),
        };

        let compiled = compile_filter(&expr).expect("compilation failed");
        let chunk = make_i64_chunk(&[1, 2, 3, 4, 5]);

        let selected = unsafe { run_filter(&compiled, &chunk) };
        assert_eq!(selected, vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn jit_filter_none_pass() {
        // col[0] < 0 — no values pass.
        let expr = BoundExpression::Comparison {
            op: ComparisonOp::Lt,
            left: Box::new(BoundExpression::Variable {
                index: 0,
                result_type: LogicalType::Int64,
            }),
            right: Box::new(BoundExpression::Literal {
                value: TypedValue::Int64(0),
                result_type: LogicalType::Int64,
            }),
        };

        let compiled = compile_filter(&expr).expect("compilation failed");
        let chunk = make_i64_chunk(&[1, 2, 3, 4, 5]);

        let selected = unsafe { run_filter(&compiled, &chunk) };
        assert!(selected.is_empty());
    }

    // -----------------------------------------------------------------------
    // Projection tests
    // -----------------------------------------------------------------------

    fn make_two_i64_columns(a: &[i64], b: &[i64]) -> DataChunk {
        assert_eq!(a.len(), b.len());
        let n = a.len();
        let mut col_a = ColumnChunkData::new(LogicalType::Int64, n as u64);
        let mut col_b = ColumnChunkData::new(LogicalType::Int64, n as u64);
        for &v in a {
            col_a.append_value::<i64>(v);
        }
        for &v in b {
            col_b.append_value::<i64>(v);
        }
        DataChunk::from_vectors(
            vec![
                ValueVector::Flat(FlatVector::from_column_chunk(&col_a, n)),
                ValueVector::Flat(FlatVector::from_column_chunk(&col_b, n)),
            ],
            SelectionVector::identity(n),
        )
    }

    fn make_f64_chunk(values: &[f64]) -> DataChunk {
        let mut col = ColumnChunkData::new(LogicalType::Double, values.len() as u64);
        for &v in values {
            col.append_value::<f64>(v);
        }
        let flat = FlatVector::from_column_chunk(&col, values.len());
        DataChunk::from_vectors(
            vec![ValueVector::Flat(flat)],
            SelectionVector::identity(values.len()),
        )
    }

    /// Run a compiled projection on a DataChunk, returning i64 output values.
    unsafe fn run_projection_i64(
        compiled: &CompiledProjection,
        chunk: &DataChunk,
    ) -> Vec<Option<i64>> {
        let n = chunk.selection().len();
        let stride = type_stride(&compiled.output_type).unwrap() as usize;
        let mut out_data = vec![0u8; n * stride];
        let null_entries = (n + 63) / 64;
        let mut out_nulls = vec![0u64; null_entries];

        // Collect pointers for all referenced columns.
        let mut col_ptrs = Vec::new();
        let mut null_ptrs = Vec::new();
        for &col_idx in &compiled.col_indices {
            let col = chunk.column(col_idx as usize);
            let flat = match col {
                ValueVector::Flat(f) => f,
                _ => panic!("expected Flat"),
            };
            col_ptrs.push(flat.data_ptr());
            null_ptrs.push(flat.null_mask().data().as_ptr());
        }

        let sel = chunk.selection();
        let sel_ptr = sel.indices_ptr();

        unsafe {
            compiled.execute(
                &col_ptrs,
                &null_ptrs,
                sel_ptr,
                n as u32,
                &mut out_data,
                &mut out_nulls,
            );
        }

        // Read back results.
        (0..n)
            .map(|i| {
                let word = out_nulls[i / 64];
                let is_null = (word >> (i % 64)) & 1 != 0;
                if is_null {
                    None
                } else {
                    let offset = i * stride;
                    let bytes: [u8; 8] = out_data[offset..offset + 8].try_into().unwrap();
                    Some(i64::from_ne_bytes(bytes))
                }
            })
            .collect()
    }

    /// Run a compiled projection on a DataChunk, returning f64 output values.
    unsafe fn run_projection_f64(
        compiled: &CompiledProjection,
        chunk: &DataChunk,
    ) -> Vec<Option<f64>> {
        let n = chunk.selection().len();
        let stride = type_stride(&compiled.output_type).unwrap() as usize;
        let mut out_data = vec![0u8; n * stride];
        let null_entries = (n + 63) / 64;
        let mut out_nulls = vec![0u64; null_entries];

        let mut col_ptrs = Vec::new();
        let mut null_ptrs = Vec::new();
        for &col_idx in &compiled.col_indices {
            let col = chunk.column(col_idx as usize);
            let flat = match col {
                ValueVector::Flat(f) => f,
                _ => panic!("expected Flat"),
            };
            col_ptrs.push(flat.data_ptr());
            null_ptrs.push(flat.null_mask().data().as_ptr());
        }

        let sel = chunk.selection();
        let sel_ptr = sel.indices_ptr();

        unsafe {
            compiled.execute(
                &col_ptrs,
                &null_ptrs,
                sel_ptr,
                n as u32,
                &mut out_data,
                &mut out_nulls,
            );
        }

        (0..n)
            .map(|i| {
                let word = out_nulls[i / 64];
                let is_null = (word >> (i % 64)) & 1 != 0;
                if is_null {
                    None
                } else {
                    let offset = i * stride;
                    let bytes: [u8; 8] = out_data[offset..offset + 8].try_into().unwrap();
                    Some(f64::from_ne_bytes(bytes))
                }
            })
            .collect()
    }

    #[test]
    fn jit_projection_i64_multiply() {
        // col[0] * 2
        let expr = BoundExpression::BinaryOp {
            op: BinaryOp::Mul,
            left: Box::new(BoundExpression::Variable {
                index: 0,
                result_type: LogicalType::Int64,
            }),
            right: Box::new(BoundExpression::Literal {
                value: TypedValue::Int64(2),
                result_type: LogicalType::Int64,
            }),
            result_type: LogicalType::Int64,
        };

        let compiled = compile_projection(&expr).expect("compilation failed");
        let chunk = make_i64_chunk(&[10, 20, 30]);

        let results = unsafe { run_projection_i64(&compiled, &chunk) };
        assert_eq!(results, vec![Some(20), Some(40), Some(60)]);
    }

    #[test]
    fn jit_projection_two_cols_add() {
        // col[0] + col[1]
        let expr = BoundExpression::BinaryOp {
            op: BinaryOp::Add,
            left: Box::new(BoundExpression::Variable {
                index: 0,
                result_type: LogicalType::Int64,
            }),
            right: Box::new(BoundExpression::Variable {
                index: 1,
                result_type: LogicalType::Int64,
            }),
            result_type: LogicalType::Int64,
        };

        let compiled = compile_projection(&expr).expect("compilation failed");
        let chunk = make_two_i64_columns(&[1, 2, 3], &[10, 20, 30]);

        let results = unsafe { run_projection_i64(&compiled, &chunk) };
        assert_eq!(results, vec![Some(11), Some(22), Some(33)]);
    }

    #[test]
    fn jit_projection_complex_arithmetic() {
        // col[0] * 2 + col[1] * 3
        let left = BoundExpression::BinaryOp {
            op: BinaryOp::Mul,
            left: Box::new(BoundExpression::Variable {
                index: 0,
                result_type: LogicalType::Int64,
            }),
            right: Box::new(BoundExpression::Literal {
                value: TypedValue::Int64(2),
                result_type: LogicalType::Int64,
            }),
            result_type: LogicalType::Int64,
        };
        let right = BoundExpression::BinaryOp {
            op: BinaryOp::Mul,
            left: Box::new(BoundExpression::Variable {
                index: 1,
                result_type: LogicalType::Int64,
            }),
            right: Box::new(BoundExpression::Literal {
                value: TypedValue::Int64(3),
                result_type: LogicalType::Int64,
            }),
            result_type: LogicalType::Int64,
        };
        let expr = BoundExpression::BinaryOp {
            op: BinaryOp::Add,
            left: Box::new(left),
            right: Box::new(right),
            result_type: LogicalType::Int64,
        };

        let compiled = compile_projection(&expr).expect("compilation failed");
        let chunk = make_two_i64_columns(&[5, 10], &[1, 2]);

        let results = unsafe { run_projection_i64(&compiled, &chunk) };
        // 5*2 + 1*3 = 13, 10*2 + 2*3 = 26
        assert_eq!(results, vec![Some(13), Some(26)]);
    }

    #[test]
    fn jit_projection_with_nulls() {
        // col[0] * 2, with some null inputs
        let expr = BoundExpression::BinaryOp {
            op: BinaryOp::Mul,
            left: Box::new(BoundExpression::Variable {
                index: 0,
                result_type: LogicalType::Int64,
            }),
            right: Box::new(BoundExpression::Literal {
                value: TypedValue::Int64(2),
                result_type: LogicalType::Int64,
            }),
            result_type: LogicalType::Int64,
        };

        let compiled = compile_projection(&expr).expect("compilation failed");
        let chunk = make_i64_chunk_with_nulls(&[Some(10), None, Some(30), None]);

        let results = unsafe { run_projection_i64(&compiled, &chunk) };
        assert_eq!(results, vec![Some(20), None, Some(60), None]);
    }

    #[test]
    fn jit_projection_f64_arithmetic() {
        // col[0] * 2.5
        let expr = BoundExpression::BinaryOp {
            op: BinaryOp::Mul,
            left: Box::new(BoundExpression::Variable {
                index: 0,
                result_type: LogicalType::Double,
            }),
            right: Box::new(BoundExpression::Literal {
                value: TypedValue::Double(2.5),
                result_type: LogicalType::Double,
            }),
            result_type: LogicalType::Double,
        };

        let compiled = compile_projection(&expr).expect("compilation failed");
        let chunk = make_f64_chunk(&[1.0, 2.0, 4.0]);

        let results = unsafe { run_projection_f64(&compiled, &chunk) };
        assert_eq!(results, vec![Some(2.5), Some(5.0), Some(10.0)]);
    }

    #[test]
    fn jit_projection_f64_sub_div() {
        // (col[0] - 10.0) / 2.0
        let sub = BoundExpression::BinaryOp {
            op: BinaryOp::Sub,
            left: Box::new(BoundExpression::Variable {
                index: 0,
                result_type: LogicalType::Double,
            }),
            right: Box::new(BoundExpression::Literal {
                value: TypedValue::Double(10.0),
                result_type: LogicalType::Double,
            }),
            result_type: LogicalType::Double,
        };
        let expr = BoundExpression::BinaryOp {
            op: BinaryOp::Div,
            left: Box::new(sub),
            right: Box::new(BoundExpression::Literal {
                value: TypedValue::Double(2.0),
                result_type: LogicalType::Double,
            }),
            result_type: LogicalType::Double,
        };

        let compiled = compile_projection(&expr).expect("compilation failed");
        let chunk = make_f64_chunk(&[20.0, 30.0, 10.0]);

        let results = unsafe { run_projection_f64(&compiled, &chunk) };
        assert_eq!(results, vec![Some(5.0), Some(10.0), Some(0.0)]);
    }

    #[test]
    fn jit_projection_negation() {
        // -col[0]
        let expr = BoundExpression::UnaryOp {
            op: UnaryOp::Minus,
            operand: Box::new(BoundExpression::Variable {
                index: 0,
                result_type: LogicalType::Int64,
            }),
            result_type: LogicalType::Int64,
        };

        let compiled = compile_projection(&expr).expect("compilation failed");
        let chunk = make_i64_chunk(&[5, -3, 0]);

        let results = unsafe { run_projection_i64(&compiled, &chunk) };
        assert_eq!(results, vec![Some(-5), Some(3), Some(0)]);
    }

    #[test]
    fn jit_filter_f64_comparison() {
        // col[0] > 2.5 (filter on f64 column)
        let expr = BoundExpression::Comparison {
            op: ComparisonOp::Gt,
            left: Box::new(BoundExpression::Variable {
                index: 0,
                result_type: LogicalType::Double,
            }),
            right: Box::new(BoundExpression::Literal {
                value: TypedValue::Double(2.5),
                result_type: LogicalType::Double,
            }),
        };

        let compiled = compile_filter(&expr).expect("compilation failed");
        let chunk = make_f64_chunk(&[1.0, 3.0, 2.5, 4.0, 0.5]);

        let selected = unsafe { run_filter(&compiled, &chunk) };
        // Values > 2.5: 3.0 (row 1), 4.0 (row 3)
        assert_eq!(selected, vec![1, 3]);
    }
}

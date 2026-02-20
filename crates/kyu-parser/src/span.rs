/// Byte offset span in source text.
pub type Span = std::ops::Range<usize>;

/// A value with an associated source span.
pub type Spanned<T> = (T, Span);

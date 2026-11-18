// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! The syntax tree. Every string in it borrows from the SQL text.
//!
//! The tree is generic over an [`AstExt`], which supplies the types stored
//! in the extension slots ([`ExprKind::Ext`] and [`Select::modifiers`]).

use std::borrow::Cow;
use std::fmt;

use crate::dialect::AstExt;

/// A byte range in the SQL text.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Span {
    pub start: u32,
    pub end: u32,
}

impl Span {
    /// The SQL text this span covers. `sql` must be the text that was parsed.
    pub fn text(self, sql: &str) -> &str {
        &sql[self.start as usize..self.end as usize]
    }

    /// The line and column where the span starts.
    pub fn start_location(self, sql: &str) -> Location {
        Location::at(sql, self.start)
    }

    /// The line and column just after the span's last character.
    pub fn end_location(self, sql: &str) -> Location {
        Location::at(sql, self.end)
    }
}

/// A position in SQL text, for error messages and editors. Both numbers start at
/// 1, and `column` counts characters rather than bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Location {
    pub line: u32,
    pub column: u32,
}

impl Location {
    /// The location of byte `offset` in `sql`, found by scanning from the start.
    fn at(sql: &str, offset: u32) -> Self {
        let before = &sql[..offset as usize];
        let line_start = before.rfind('\n').map_or(0, |newline| newline + 1);
        Self {
            line: before.matches('\n').count() as u32 + 1,
            column: before[line_start..].chars().count() as u32 + 1,
        }
    }
}

/// A node that knows which part of the SQL text it came from.
pub trait Spanned {
    /// The node's position, or an empty span if it did not come from the parser.
    fn span(&self) -> Span;
}

/// An identifier. `text` is the source text between any quotes, with escapes
/// left in place; use [`Ident::value`] to resolve them.
#[derive(Debug, Clone)]
pub struct Ident<'a> {
    pub text: Cow<'a, str>,
    pub quote: Option<char>,
    pub span: Span,
}

impl<'a> DataType<'a> {
    /// A column or cast with no type written, in dialects that allow that.
    pub fn unspecified(span: Span) -> Self {
        Self {
            qualifier: Vec::new(),
            name: Ident {
                text: "".into(),
                quote: None,
                span,
            },
            angle: None,
            args: Vec::new(),
            suffix: DataTypeSuffix::None,
            array: ArrayType::None,
        }
    }

    pub fn is_unspecified(&self) -> bool {
        self.name.text.is_empty()
    }
}

impl<'a> Ident<'a> {
    /// The identifier's value. Only allocates if it contains an escaped quote.
    pub fn value(&self) -> Cow<'a, str> {
        lend(&self.text, |text| match self.quote {
            Some(quote) => unescape(text, quote),
            None => Cow::Borrowed(text),
        })
    }
}

/// Applies `f` to `text`, keeping the result borrowed for `'a` when `text`
/// borrows from the source. Owned text gives an owned result.
fn lend<'a>(text: &Cow<'a, str>, f: impl for<'b> FnOnce(&'b str) -> Cow<'b, str>) -> Cow<'a, str> {
    match text {
        Cow::Borrowed(text) => f(text),
        Cow::Owned(text) => Cow::Owned(f(text).into_owned()),
    }
}

/// Spans are ignored so that trees can be compared by structure.
impl PartialEq for Ident<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.text == other.text && self.quote == other.quote
    }
}

/// A string literal. `text` is the source text between the quotes, with
/// escapes left in place; use [`StringLiteral::value`] to resolve them.
#[derive(Debug, Clone, PartialEq)]
pub struct StringLiteral<'a> {
    pub text: Cow<'a, str>,
    /// The quote character, or `$` for text between `$$` markers.
    pub quote: char,
    /// True if a backslash introduces an escape sequence in this dialect.
    pub backslash_escapes: bool,
    /// True for a national character string, written `N'text'`.
    pub national: bool,
    pub prefix: StringPrefix,
    /// True if the quotes were tripled, as in `'''text'''`.
    pub triple: bool,
}

/// A letter written directly before a string's opening quote.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StringPrefix {
    #[default]
    None,
    /// `B'text'`
    Byte,
    /// `R'text'`, in which backslashes have no meaning.
    Raw,
    /// `E'text'`, in which a backslash introduces an escape.
    Escape,
    /// `U&'text'`, in which `\\XXXX` writes a Unicode code point.
    Unicode,
    /// `B'text'` as a bit string, which holds only `0` and `1`.
    Bit,
}

impl<'a> StringLiteral<'a> {
    /// The string's value. Only allocates if it contains an escape.
    pub fn value(&self) -> Cow<'a, str> {
        lend(&self.text, |text| {
            if self.prefix == StringPrefix::Raw || (self.triple && !self.backslash_escapes) {
                Cow::Borrowed(text)
            } else if self.prefix == StringPrefix::Escape {
                // The lexer has checked the escapes; an invalid one is kept as written.
                match unescape_escape_string(text, self.quote) {
                    Ok(value) => value,
                    Err(_) => Cow::Borrowed(text),
                }
            } else if self.backslash_escapes && text.contains('\\') {
                // Inside triple quotes, a doubled quote is just two quotes.
                let quote = if self.triple { '\0' } else { self.quote };
                Cow::Owned(unescape_backslashes(text, quote))
            } else if self.triple {
                Cow::Borrowed(text)
            } else {
                unescape(text, self.quote)
            }
        })
    }
}

/// Resolves the escapes of PostgreSQL's `E'text'`: `\b`, `\f`, `\n`, `\r`
/// and `\t`, one to three octal digits, `\x` and one or two hex digits, which
/// write bytes, `\uXXXX` and `\UXXXXXXXX`, which write code points, and a
/// backslash before any other character, which writes that character. Fails if
/// the bytes are not UTF-8, or the string would contain a NUL.
pub fn unescape_escape_string(text: &str, quote: char) -> Result<Cow<'_, str>, &'static str> {
    if !text.contains('\\') {
        return Ok(unescape(text, quote));
    }
    let mut value = Vec::with_capacity(text.len());
    let bytes = text.as_bytes();
    let mut i = 0;
    // Reads up to `max` digits in `radix` from `bytes[i..]`.
    let digits = |i: &mut usize, radix: u32, max: usize| -> Option<u32> {
        let start = *i;
        let mut n = 0u32;
        while *i < bytes.len() && *i - start < max {
            let Some(d) = char::from(bytes[*i]).to_digit(radix) else {
                break;
            };
            n = n * radix + d;
            *i += 1;
        }
        (*i > start).then_some(n)
    };
    while i < bytes.len() {
        let b = bytes[i];
        i += 1;
        if char::from(b) == quote && bytes.get(i) == Some(&b) {
            i += 1;
            value.push(b);
            continue;
        }
        if b != b'\\' {
            value.push(b);
            continue;
        }
        let Some(&escaped) = bytes.get(i) else {
            return Err("unterminated escape in string");
        };
        i += 1;
        match escaped {
            b'b' => value.push(8),
            b'f' => value.push(12),
            b'n' => value.push(b'\n'),
            b'r' => value.push(b'\r'),
            b't' => value.push(b'\t'),
            b'0'..=b'7' => {
                i -= 1;
                let n = digits(&mut i, 8, 3).unwrap_or(0);
                value.push((n & 0xff) as u8);
            }
            b'x' if bytes.get(i).is_some_and(u8::is_ascii_hexdigit) => {
                let n = digits(&mut i, 16, 2).unwrap_or(0);
                value.push(n as u8);
            }
            b'u' | b'U' => {
                let len = if escaped == b'u' { 4 } else { 8 };
                let start = i;
                let mut code = digits(&mut i, 16, len).filter(|_| i - start == len);
                // A UTF-16 surrogate pair is written as two `\u` escapes.
                if let Some(high @ 0xD800..=0xDBFF) = code {
                    code = None;
                    if bytes.get(i) == Some(&b'\\') && bytes.get(i + 1) == Some(&b'u') {
                        i += 2;
                        let start = i;
                        if let Some(low @ 0xDC00..=0xDFFF) =
                            digits(&mut i, 16, 4).filter(|_| i - start == 4)
                        {
                            code = Some(0x10000 + ((high - 0xD800) << 10) + (low - 0xDC00));
                        }
                    }
                }
                let c = code
                    .and_then(char::from_u32)
                    .ok_or("invalid Unicode escape in string")?;
                value.extend_from_slice(c.encode_utf8(&mut [0; 4]).as_bytes());
            }
            other => {
                // Another character, which may be more than one byte.
                value.push(other);
            }
        }
    }
    if value.contains(&0) {
        return Err("a string cannot contain a NUL character");
    }
    String::from_utf8(value)
        .map(Cow::Owned)
        .map_err(|_| "invalid byte sequence in string")
}

fn unescape_backslashes(text: &str, quote: char) -> String {
    let mut value = String::with_capacity(text.len());
    let mut chars = text.chars().peekable();
    while let Some(c) = chars.next() {
        if c == quote && chars.peek() == Some(&quote) {
            chars.next();
            value.push(quote);
        } else if c != '\\' {
            value.push(c);
        } else {
            match chars.next() {
                Some('0') => value.push('\0'),
                Some('b') => value.push('\u{8}'),
                Some('n') => value.push('\n'),
                Some('r') => value.push('\r'),
                Some('t') => value.push('\t'),
                Some('Z') => value.push('\u{1a}'),
                // These keep their backslash so that `LIKE` sees them as literals.
                Some(wildcard @ ('%' | '_')) => {
                    value.push('\\');
                    value.push(wildcard);
                }
                Some(other) => value.push(other),
                None => {}
            }
        }
    }
    value
}

fn unescape(text: &str, quote: char) -> Cow<'_, str> {
    // A `[name]` identifier escapes its closing bracket.
    let quote = if quote == '[' { ']' } else { quote };
    if text.contains(quote) {
        let doubled: String = [quote, quote].iter().collect();
        Cow::Owned(text.replace(&doubled, quote.encode_utf8(&mut [0; 4])))
    } else {
        Cow::Borrowed(text)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Literal<'a> {
    Number(Cow<'a, str>),
    String(StringLiteral<'a>),
    /// The digits of `X'AB'` or `0xAB`.
    HexString(Cow<'a, str>),
    /// `$tag$text$tag$`, where `tag` may be empty.
    DollarQuoted {
        tag: Cow<'a, str>,
        text: Cow<'a, str>,
    },
    Null,
    Boolean(bool),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOp {
    Neg,
    Plus,
    Not,
    /// `!expr`, which means the same as `NOT expr`.
    BangNot,
    /// `~expr`
    BitNot,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BinaryOp<'a> {
    Or,
    And,
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    /// `||`
    StringConcat,
    /// `|`
    BitOr,
    /// `&`
    BitAnd,
    /// `^`
    BitXor,
    /// `<<`
    ShiftLeft,
    /// `>>`
    ShiftRight,
    /// An operator a dialect adds, rendered as this text.
    Named(Cow<'a, str>),
}

/// The keyword of a quantified comparison.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Quantifier {
    Any,
    Some,
    All,
}

/// How a cast was written in the source.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CastStyle {
    /// `CAST(expr AS ty)`
    Function,
    /// `TRY_CAST(expr AS ty)`
    TryFunction,
    /// `SAFE_CAST(expr AS ty)`
    SafeFunction,
    /// `expr::ty`
    DoubleColon,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ExprKind<'a, X: AstExt> {
    Ident(Ident<'a>),
    Literal(Literal<'a>),
    Unary {
        op: UnaryOp,
        expr: Box<Expr<'a, X>>,
    },
    Binary {
        left: Box<Expr<'a, X>>,
        op: BinaryOp<'a>,
        right: Box<Expr<'a, X>>,
    },
    Cast {
        expr: Box<Expr<'a, X>>,
        /// Boxed so that the common expressions stay small.
        ty: Box<DataType<'a>>,
        style: CastStyle,
    },
    /// A bind parameter, as written: `?`, `$1` or `:name`.
    Placeholder(Cow<'a, str>),
    /// `INTERVAL value unit`
    Interval(Box<Interval<'a, X>>),
    /// A literal introduced by its type: `DATE '2020-01-01'`.
    TypedString(Box<TypedString<'a>>),
    /// Less common syntax. Boxed so that it adds no size to [`Expr`], and
    /// no locals to the derived traits that recurse through every level.
    Special(Box<SpecialExpr<'a, X>>),
    /// A qualified name such as `t.a`, with at least two parts.
    Compound(ObjectName<'a>),
    Function {
        name: ObjectName<'a>,
        args: Vec<FunctionArg<'a, X>>,
        /// Boxed so that plain calls do not pay for syntax they do not use.
        options: Option<Box<FunctionOptions<'a, X>>>,
    },
    /// A scalar subquery: `(SELECT ...)`.
    Subquery(Box<Query<'a, X>>),
    /// `EXISTS (SELECT ...)`
    Exists(Box<Query<'a, X>>),
    /// `expr [NOT] IN (a, b, ...)`
    InList {
        expr: Box<Expr<'a, X>>,
        list: Vec<Expr<'a, X>>,
        negated: bool,
    },
    /// `expr [NOT] IN (SELECT ...)`
    InSubquery {
        expr: Box<Expr<'a, X>>,
        subquery: Box<Query<'a, X>>,
        negated: bool,
    },
    /// `expr IS [NOT] test`
    Is {
        expr: Box<Expr<'a, X>>,
        test: IsTest<'a, X>,
        negated: bool,
    },
    /// `expr [NOT] LIKE pattern [ESCAPE escape]`
    Like {
        expr: Box<Expr<'a, X>>,
        pattern: Box<Expr<'a, X>>,
        negated: bool,
        kind: LikeKind<'a>,
        escape: Option<Box<Expr<'a, X>>>,
    },
    /// `expr [NOT] BETWEEN low AND high`
    Between {
        expr: Box<Expr<'a, X>>,
        low: Box<Expr<'a, X>>,
        high: Box<Expr<'a, X>>,
        negated: bool,
    },
    Case(Box<CaseExpr<'a, X>>),
    /// A parenthesized expression.
    Nested(Box<Expr<'a, X>>),
    /// Syntax that only the dialect knows about.
    Ext(X::Expr<'a>),
}

/// An expression, and the source text it covers.
///
/// `span` is ignored by `==`, like the span of an [`Ident`].
pub struct Expr<'a, X: AstExt> {
    pub kind: ExprKind<'a, X>,
    pub span: Span,
}

impl<'a, X: AstExt> Expr<'a, X> {
    pub fn new(kind: ExprKind<'a, X>, span: Span) -> Self {
        Self { kind, span }
    }
}

/// A node that did not come from the parser has an empty span.
impl<'a, X: AstExt> From<ExprKind<'a, X>> for Expr<'a, X> {
    fn from(kind: ExprKind<'a, X>) -> Self {
        Self::new(kind, Span::default())
    }
}

// Written by hand so that each level of a deep tree adds no stack frames.
impl<X: AstExt> Clone for Expr<'_, X> {
    #[inline(always)]
    fn clone(&self) -> Self {
        Self {
            kind: self.kind.clone(),
            span: self.span,
        }
    }
}

impl<X: AstExt> fmt::Debug for Expr<'_, X> {
    #[inline(always)]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.kind, f)?;
        write!(f, "@{}..{}", self.span.start, self.span.end)
    }
}

impl<X: AstExt> PartialEq for Expr<'_, X> {
    #[inline(always)]
    fn eq(&self, other: &Self) -> bool {
        self.kind == other.kind
    }
}

impl<X: AstExt> Spanned for Expr<'_, X> {
    fn span(&self) -> Span {
        self.span
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TypedString<'a> {
    pub ty: DataType<'a>,
    pub value: StringLiteral<'a>,
}

/// `INTERVAL value [unit [(precision)] [TO unit [(precision)]]]`. Units are
/// the source text; rendering upper-cases them.
#[derive(Debug, Clone, PartialEq)]
pub struct Interval<'a, X: AstExt> {
    pub value: Expr<'a, X>,
    pub leading_field: Option<Cow<'a, str>>,
    pub leading_precision: Option<Cow<'a, str>>,
    pub last_field: Option<Cow<'a, str>>,
    pub fractional_seconds_precision: Option<Cow<'a, str>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrimSide {
    Both,
    Leading,
    Trailing,
}

/// Expressions rare enough to live behind a box. A `field` is the source text
/// of a date or time unit, which rendering upper-cases.
#[derive(Debug, Clone, PartialEq)]
pub enum SpecialExpr<'a, X: AstExt> {
    /// `left op ANY (right)`, and likewise for `SOME` and `ALL`.
    Quantified {
        left: Expr<'a, X>,
        op: BinaryOp<'a>,
        quantifier: Quantifier,
        /// An [`ExprKind::Subquery`], or whatever was in the parentheses.
        right: Expr<'a, X>,
    },
    /// `(expr).field`
    FieldAccess { expr: Expr<'a, X>, field: Ident<'a> },
    /// `ANY (a, b)`, the right side of a `LIKE`.
    QuantifiedList {
        quantifier: Quantifier,
        items: Vec<Expr<'a, X>>,
    },
    /// `{d '2020-01-01'}` or `{fn f()}`: an ODBC escape sequence.
    OdbcEscape {
        kind: Cow<'a, str>,
        expr: Expr<'a, X>,
    },
    /// `expr[lower:upper]` or `expr[lower:upper:step]`, any bound optional.
    Slice {
        expr: Expr<'a, X>,
        lower: Option<Expr<'a, X>>,
        upper: Option<Expr<'a, X>>,
        step: Option<Expr<'a, X>>,
    },
    /// `expr AT TIME ZONE zone`
    AtTimeZone {
        expr: Expr<'a, X>,
        zone: Expr<'a, X>,
    },
    /// `expr COLLATE name`
    Collate {
        expr: Expr<'a, X>,
        collation: ObjectName<'a>,
    },
    /// `expr[index]`
    Subscript {
        expr: Expr<'a, X>,
        index: Expr<'a, X>,
    },
    /// `ARRAY[a, b]`
    Array(Vec<Expr<'a, X>>),
    /// `(a, b)`, with at least two elements.
    Tuple(Vec<Expr<'a, X>>),
    /// `PRIOR expr`, inside `CONNECT BY`.
    Prior(Expr<'a, X>),
    /// `x -> body` or `(x, y) -> body`.
    Lambda {
        params: Vec<Ident<'a>>,
        parenthesized: bool,
        body: Expr<'a, X>,
    },
    /// `CONNECT_BY_ROOT expr`
    ConnectByRoot(Expr<'a, X>),
    /// `expr (+)`, marking the outer side of a join.
    OuterJoinMark(Expr<'a, X>),
    /// `EXTRACT(field FROM expr)`
    Extract {
        field: Cow<'a, str>,
        expr: Expr<'a, X>,
    },
    /// `CEIL(expr TO field)` or `FLOOR(expr TO field)`
    Round {
        ceil: bool,
        expr: Expr<'a, X>,
        field: Cow<'a, str>,
    },
    /// `POSITION(needle IN haystack)`
    Position {
        needle: Expr<'a, X>,
        haystack: Expr<'a, X>,
    },
    /// `SUBSTRING(expr FROM start FOR length)`
    Substring {
        /// True if spelled `SUBSTR`.
        shorthand: bool,
        expr: Expr<'a, X>,
        from: Option<Expr<'a, X>>,
        length: Option<Expr<'a, X>>,
    },
    /// `TRIM(side what FROM expr)`
    Trim {
        side: Option<TrimSide>,
        what: Option<Expr<'a, X>>,
        expr: Expr<'a, X>,
    },
    /// `OVERLAY(expr PLACING what FROM start FOR length)`
    Overlay {
        expr: Expr<'a, X>,
        what: Expr<'a, X>,
        from: Expr<'a, X>,
        length: Option<Expr<'a, X>>,
    },
    /// `ROLLUP (...)`, `CUBE (...)` or `GROUPING SETS (...)` in `GROUP BY`.
    Grouping {
        kind: GroupingKind,
        /// Each set's expressions. A set of one is written without parentheses
        /// in `ROLLUP` and `CUBE`.
        sets: Vec<Vec<Expr<'a, X>>>,
    },
    /// A function called without parentheses, such as `CURRENT_DATE`. Which
    /// words these are is up to the dialect: see
    /// [`Dialect::is_niladic_function`](crate::dialect::Dialect::is_niladic_function).
    Niladic(Ident<'a>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupingKind {
    Rollup,
    Cube,
    Sets,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SelectItemKind<'a, X: AstExt> {
    Wildcard,
    /// `t.*`
    QualifiedWildcard(ObjectName<'a>),
    /// A wildcard with options such as `* EXCLUDE (a)`.
    WildcardWithOptions {
        qualifier: Option<ObjectName<'a>>,
        options: Box<WildcardOptions<'a, X>>,
    },
    Expr {
        expr: Expr<'a, X>,
        alias: Option<Ident<'a>>,
    },
    /// `expr AS (a, b)`, for expressions that produce several columns.
    ExprWithAliases {
        expr: Expr<'a, X>,
        aliases: Vec<Ident<'a>>,
    },
}

/// An item in a select list, and the source text it covers.
///
/// `span` is ignored by `==`, like the span of an [`Ident`].
pub struct SelectItem<'a, X: AstExt> {
    pub kind: SelectItemKind<'a, X>,
    pub span: Span,
}

impl<'a, X: AstExt> SelectItem<'a, X> {
    pub fn new(kind: SelectItemKind<'a, X>, span: Span) -> Self {
        Self { kind, span }
    }
}

/// A node that did not come from the parser has an empty span.
impl<'a, X: AstExt> From<SelectItemKind<'a, X>> for SelectItem<'a, X> {
    fn from(kind: SelectItemKind<'a, X>) -> Self {
        Self::new(kind, Span::default())
    }
}

// Written by hand so that each level of a deep tree adds no stack frames.
impl<X: AstExt> Clone for SelectItem<'_, X> {
    #[inline(always)]
    fn clone(&self) -> Self {
        Self {
            kind: self.kind.clone(),
            span: self.span,
        }
    }
}

impl<X: AstExt> fmt::Debug for SelectItem<'_, X> {
    #[inline(always)]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.kind, f)?;
        write!(f, "@{}..{}", self.span.start, self.span.end)
    }
}

impl<X: AstExt> PartialEq for SelectItem<'_, X> {
    #[inline(always)]
    fn eq(&self, other: &Self) -> bool {
        self.kind == other.kind
    }
}

impl<X: AstExt> Spanned for SelectItem<'_, X> {
    fn span(&self) -> Span {
        self.span
    }
}

/// What can follow a `*` in dialects that extend it.
#[derive(Debug, Clone, PartialEq)]
pub struct WildcardOptions<'a, X: AstExt> {
    pub ilike: Option<StringLiteral<'a>>,
    pub exclude: Vec<Ident<'a>>,
    /// True if the `EXCLUDE` columns were written in parentheses.
    pub exclude_parens: bool,
    /// True if `exclude` was written with the keyword `EXCEPT`.
    pub except_keyword: bool,
    /// `REPLACE (expr AS name, ...)`
    pub replace: Vec<(Expr<'a, X>, Ident<'a>)>,
    /// `RENAME from AS to`
    pub rename: Vec<(Ident<'a>, Ident<'a>)>,
    /// True if the `RENAME` pairs were written in parentheses.
    pub rename_parens: bool,
}

/// A possibly qualified name such as `schema.table`. Always has a part.
#[derive(Debug, Clone, PartialEq)]
pub struct ObjectName<'a> {
    pub parts: Vec<Ident<'a>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum FunctionArg<'a, X: AstExt> {
    /// `*`, as in `COUNT(*)`.
    Wildcard,
    /// `t.*`
    QualifiedWildcard(ObjectName<'a>),
    Expr(Expr<'a, X>),
    /// `name => value`
    Named {
        name: Ident<'a>,
        value: Expr<'a, X>,
    },
    /// A named argument written with another operator, such as `name := value`.
    NamedWith {
        name: Ident<'a>,
        operator: Cow<'a, str>,
        value: Expr<'a, X>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum TableFactor<'a, X: AstExt> {
    /// A table factor that only one dialect has.
    Ext(X::TableFactor<'a>),
    Named(ObjectName<'a>),
    /// A subquery in `FROM`.
    Derived(Box<Query<'a, X>>),
    /// `VALUES (...), (...)` used directly as a table.
    Values(Vec<Vec<Expr<'a, X>>>),
    /// `(a JOIN b)`
    NestedJoin(Box<TableWithJoins<'a, X>>),
    /// A table-valued function. `call` is an [`ExprKind::Function`], or an
    /// [`ExprKind::Ext`] for a call a dialect extends.
    Function {
        call: Box<Expr<'a, X>>,
        with_ordinality: bool,
    },
    /// `LATERAL` before a subquery or function.
    Lateral(Box<TableFactor<'a, X>>),
    /// A table read as of a version or time: `t VERSION AS OF 1`.
    TimeTravel(Box<TimeTravel<'a, X>>),
    /// A table read as of another point in time: `t AT(TIMESTAMP => x)`.
    Versioned {
        name: ObjectName<'a>,
        /// The calls after the name, each an [`ExprKind::Function`].
        calls: Vec<Expr<'a, X>>,
    },
    /// `table SAMPLE (10)`
    Sampled(Box<TableSample<'a, X>>),
    /// `XMLTABLE(...)`
    XmlTable(Box<XmlTable<'a, X>>),
    Pivot(Box<Pivot<'a, X>>),
    Unpivot(Box<Unpivot<'a, X>>),
    MatchRecognize(Box<MatchRecognize<'a, X>>),
}

/// `XMLTABLE([XMLNAMESPACES(...),] row [PASSING ...] COLUMNS ...)`
#[derive(Debug, Clone, PartialEq)]
pub struct XmlTable<'a, X: AstExt> {
    pub namespaces: Vec<XmlNamespace<'a, X>>,
    pub row_expression: Expr<'a, X>,
    pub passing: Vec<XmlPassingArgument<'a, X>>,
    pub columns: Vec<XmlTableColumn<'a, X>>,
}

/// `uri AS name` in `XMLNAMESPACES`.
#[derive(Debug, Clone, PartialEq)]
pub struct XmlNamespace<'a, X: AstExt> {
    pub uri: Expr<'a, X>,
    pub name: Ident<'a>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct XmlPassingArgument<'a, X: AstExt> {
    pub by_value: bool,
    pub expr: Expr<'a, X>,
    pub alias: Option<Ident<'a>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct XmlTableColumn<'a, X: AstExt> {
    pub name: Ident<'a>,
    pub kind: XmlColumnKind<'a, X>,
}

/// What an `XMLTABLE` column reads.
#[derive(Debug, Clone, PartialEq)]
pub enum XmlColumnKind<'a, X: AstExt> {
    ForOrdinality,
    Data {
        data_type: Box<DataType<'a>>,
        path: Option<Expr<'a, X>>,
        default: Option<Expr<'a, X>>,
        not_null: bool,
    },
}

/// `name kind AS OF at`
#[derive(Debug, Clone, PartialEq)]
pub struct TimeTravel<'a, X: AstExt> {
    pub name: ObjectName<'a>,
    /// `TIMESTAMP` or `VERSION`.
    pub kind: Cow<'a, str>,
    pub at: Expr<'a, X>,
}

/// `table MATCH_RECOGNIZE(...)`
#[derive(Debug, Clone, PartialEq)]
pub struct MatchRecognize<'a, X: AstExt> {
    pub table: TableRef<'a, X>,
    pub partition_by: Vec<Expr<'a, X>>,
    pub order_by: Vec<OrderByItem<'a, X>>,
    /// `expr AS alias`, each with its alias.
    pub measures: Vec<(Expr<'a, X>, Ident<'a>)>,
    pub rows_per_match: Option<RowsPerMatch>,
    pub after_match_skip: Option<AfterMatchSkip<'a>>,
    pub pattern: MatchPattern<'a>,
    /// `symbol AS definition`
    pub symbols: Vec<(Ident<'a>, Expr<'a, X>)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowsPerMatch {
    OneRow,
    AllRows(Option<EmptyMatches>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmptyMatches {
    Show,
    Omit,
    WithUnmatchedRows,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AfterMatchSkip<'a> {
    PastLastRow,
    ToNextRow,
    ToFirst(Ident<'a>),
    ToLast(Ident<'a>),
}

/// A regular expression over row symbols.
#[derive(Debug, Clone, PartialEq)]
pub enum MatchPattern<'a> {
    Symbol(Ident<'a>),
    /// `^`
    Start,
    /// `$`
    End,
    /// `{- symbol -}`
    Exclude(Ident<'a>),
    /// `PERMUTE(a, b)`
    Permute(Vec<Ident<'a>>),
    /// Patterns in sequence.
    Concat(Vec<MatchPattern<'a>>),
    /// `( pattern )`
    Group(Box<MatchPattern<'a>>),
    /// `a | b`
    Alternation(Vec<MatchPattern<'a>>),
    Repetition {
        pattern: Box<MatchPattern<'a>>,
        quantifier: PatternQuantifier<'a>,
        /// True if followed by `?`, matching as little as possible.
        reluctant: bool,
    },
}

/// Bounds are the source text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PatternQuantifier<'a> {
    /// `*`
    ZeroOrMore,
    /// `+`
    OneOrMore,
    /// `?`
    AtMostOne,
    /// `{n}`
    Exactly(Cow<'a, str>),
    /// `{n,}`
    AtLeast(Cow<'a, str>),
    /// `{,m}`
    AtMost(Cow<'a, str>),
    /// `{n,m}`
    Range(Cow<'a, str>, Cow<'a, str>),
}

/// An expression with an optional alias, as in a `PIVOT` value list.
#[derive(Debug, Clone, PartialEq)]
pub struct ExprWithAlias<'a, X: AstExt> {
    pub expr: Expr<'a, X>,
    pub alias: Option<Ident<'a>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TableSample<'a, X: AstExt> {
    pub table: TableRef<'a, X>,
    pub sample: SampleClause<'a, X>,
}

/// What a sample asks for, apart from the table it applies to.
#[derive(Debug, Clone, PartialEq)]
pub struct SampleClause<'a, X: AstExt> {
    /// True if written `TABLESAMPLE`, false if `SAMPLE`.
    pub long_form: bool,
    /// `BERNOULLI`, `ROW`, `SYSTEM` or `BLOCK`.
    pub method: Option<Cow<'a, str>>,
    pub amount: SampleAmount<'a, X>,
    pub parenthesized: bool,
    pub seed: Option<SampleSeed<'a>>,
    /// The start of the sample: `SAMPLE 0.1 OFFSET 0.5`.
    pub offset: Option<Box<Expr<'a, X>>>,
}

/// How much of a table a sample takes.
#[derive(Debug, Clone, PartialEq)]
pub enum SampleAmount<'a, X: AstExt> {
    Quantity {
        value: Expr<'a, X>,
        unit: Option<SampleUnit>,
    },
    /// `BUCKET 3 OUT OF 16 [ON expr]`
    Bucket {
        bucket: Cow<'a, str>,
        total: Cow<'a, str>,
        on: Option<Box<Expr<'a, X>>>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SampleUnit {
    Rows,
    Percent,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SampleSeed<'a> {
    /// True if written `REPEATABLE`, false if `SEED`.
    pub repeatable: bool,
    pub value: Cow<'a, str>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Pivot<'a, X: AstExt> {
    pub table: TableRef<'a, X>,
    pub clause: PivotClause<'a, X>,
}

/// `PIVOT(...)`, apart from the table it applies to.
#[derive(Debug, Clone, PartialEq)]
pub struct PivotClause<'a, X: AstExt> {
    pub aggregates: Vec<ExprWithAlias<'a, X>>,
    /// More than one is written in parentheses.
    pub columns: Vec<Expr<'a, X>>,
    pub source: PivotSource<'a, X>,
    pub default_on_null: Option<Expr<'a, X>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum PivotSource<'a, X: AstExt> {
    List(Vec<ExprWithAlias<'a, X>>),
    /// `ANY [ORDER BY ...]`
    Any(Vec<OrderByItem<'a, X>>),
    Query(Box<Query<'a, X>>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Unpivot<'a, X: AstExt> {
    pub table: TableRef<'a, X>,
    pub clause: UnpivotClause<'a, X>,
}

/// `UNPIVOT(...)`, apart from the table it applies to.
#[derive(Debug, Clone, PartialEq)]
pub struct UnpivotClause<'a, X: AstExt> {
    /// `Some(true)` for `INCLUDE NULLS`, `Some(false)` for `EXCLUDE NULLS`.
    pub include_nulls: Option<bool>,
    pub value: Expr<'a, X>,
    pub name: Ident<'a>,
    pub columns: Vec<ExprWithAlias<'a, X>>,
}

/// The alias of a table in `FROM`.
#[derive(Debug, Clone, PartialEq)]
pub struct TableAlias<'a> {
    pub name: Ident<'a>,
    /// True if written with `AS`.
    pub explicit: bool,
    /// Names for the table's columns: `AS t (a, b)`.
    pub columns: Vec<Ident<'a>>,
    /// The type of each column, or empty if the columns have none.
    pub column_types: Vec<DataType<'a>>,
}

#[derive(Debug, Clone)]
pub struct TableRef<'a, X: AstExt> {
    pub factor: TableFactor<'a, X>,
    pub alias: Option<TableAlias<'a>>,
    /// Ignored by `==`, like the span of an [`Ident`].
    pub span: Span,
}

impl<X: AstExt> PartialEq for TableRef<'_, X> {
    fn eq(&self, other: &Self) -> bool {
        self.factor == other.factor && self.alias == other.alias
    }
}

impl<X: AstExt> Spanned for TableRef<'_, X> {
    fn span(&self) -> Span {
        self.span
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JoinKind<'a> {
    /// A join that only one dialect has, holding all of its keywords, such as
    /// `"KEEP JOIN"`.
    Custom(Cow<'a, str>),
    Inner,
    Left,
    Right,
    Full,
    Cross,
    /// `ASOF JOIN`
    AsOf,
    /// `SEMI JOIN`
    Semi,
    /// `LEFT SEMI JOIN`
    LeftSemi,
    /// `ANTI JOIN`
    Anti,
    /// `LEFT ANTI JOIN`
    LeftAnti,
    /// `RIGHT SEMI JOIN`
    RightSemi,
    /// `RIGHT ANTI JOIN`
    RightAnti,
}

#[derive(Debug, Clone, PartialEq)]
pub enum JoinConstraint<'a, X: AstExt> {
    /// A join condition that only one dialect has.
    Ext(X::JoinConstraint<'a>),
    None,
    On(Expr<'a, X>),
    Using(Vec<Ident<'a>>),
    /// `NATURAL`, which is written before the join keywords.
    Natural,
    /// `MATCH_CONDITION (condition)`, for an `ASOF JOIN`, followed by
    /// `constraint`: [`JoinConstraint::None`], [`JoinConstraint::On`] or
    /// [`JoinConstraint::Using`].
    AsOf {
        condition: Expr<'a, X>,
        constraint: Box<JoinConstraint<'a, X>>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct Join<'a, X: AstExt> {
    pub kind: JoinKind<'a>,
    /// True if the optional `INNER` or `OUTER` keyword was written.
    pub spelled_out: bool,
    pub table: TableRef<'a, X>,
    pub constraint: JoinConstraint<'a, X>,
}

/// One comma-separated item of a `FROM` clause.
#[derive(Debug, Clone, PartialEq)]
pub struct TableWithJoins<'a, X: AstExt> {
    pub relation: TableRef<'a, X>,
    pub joins: Vec<Join<'a, X>>,
}

/// A `WITH` clause.
#[derive(Debug, Clone, PartialEq)]
pub struct With<'a, X: AstExt> {
    /// True for `WITH RECURSIVE`.
    pub recursive: bool,
    pub ctes: Vec<Cte<'a, X>>,
}

/// A common table expression: `name AS (query)`.
#[derive(Debug, Clone, PartialEq)]
pub struct Cte<'a, X: AstExt> {
    pub name: Ident<'a>,
    /// The column names listed after the name.
    pub columns: Vec<Ident<'a>>,
    /// `Some(true)` for `MATERIALIZED`, `Some(false)` for `NOT MATERIALIZED`.
    pub materialized: Option<bool>,
    pub query: Query<'a, X>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderByItem<'a, X: AstExt> {
    pub expr: Expr<'a, X>,
    /// `Some(true)` for `ASC`, `Some(false)` for `DESC`, `None` if unspecified.
    pub asc: Option<bool>,
    /// `Some(true)` for `NULLS FIRST`, `Some(false)` for `NULLS LAST`.
    pub nulls_first: Option<bool>,
    /// The operator in `USING op`, instead of `ASC` or `DESC`.
    pub using: Option<Box<OperatorName<'a>>>,
    /// What a dialect adds after the sort options.
    pub ext: X::OrderByItem<'a>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Limit<'a, X: AstExt> {
    /// `None` for an `OFFSET` with no `LIMIT`.
    pub count: Option<Expr<'a, X>>,
    pub offset: Option<Expr<'a, X>>,
    /// The optional word after the offset.
    pub offset_rows: OffsetRows,
    /// True if written as `LIMIT offset, count`.
    pub offset_comma: bool,
    /// True for `LIMIT ALL`, in which case `count` is `None`.
    pub all: bool,
    /// The columns in `LIMIT count BY columns`.
    pub by: Vec<Expr<'a, X>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OffsetRows {
    None,
    Row,
    Rows,
}

/// `FETCH FIRST [quantity [PERCENT]] ROWS {ONLY | WITH TIES}`
#[derive(Debug, Clone, PartialEq)]
pub struct Fetch<'a, X: AstExt> {
    pub quantity: Option<Expr<'a, X>>,
    pub percent: bool,
    pub with_ties: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockType {
    Update,
    Share,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NonBlock {
    Nowait,
    SkipLocked,
}

/// `FOR {UPDATE | SHARE} [OF name] [NOWAIT | SKIP LOCKED]`
#[derive(Debug, Clone, PartialEq)]
pub struct LockClause<'a> {
    pub lock_type: LockType,
    pub of: Option<ObjectName<'a>>,
    pub nonblock: Option<NonBlock>,
}

/// A complete query. `WITH`, `ORDER BY` and `LIMIT` apply to the whole body,
/// including every branch of a `UNION`.
#[derive(Debug, Clone, PartialEq)]
pub struct Query<'a, X: AstExt> {
    /// Boxed, like other rare syntax, to keep every query small.
    pub with: Option<Box<With<'a, X>>>,
    pub body: SetExpr<'a, X>,
    pub order_by: Vec<OrderByItem<'a, X>>,
    /// Spark's `CLUSTER BY`, `DISTRIBUTE BY` and `SORT BY`.
    pub distribution: Option<Box<QueryDistribution<'a, X>>>,
    pub limit: Option<Box<Limit<'a, X>>>,
    /// Boxed, like other rare syntax, to keep every query small.
    pub fetch: Option<Box<Fetch<'a, X>>>,
    pub locks: Vec<LockClause<'a>>,
    /// Dialect-specific syntax at the end of the query.
    pub tail: X::QueryTail<'a>,
    /// The `|> ...` steps after everything else, in order.
    pub pipes: Vec<PipeOperator<'a, X>>,
}

impl<'a, X: AstExt> Query<'a, X> {
    /// The common table expressions of the `WITH` clause, if any.
    pub fn ctes(&self) -> &[Cte<'a, X>] {
        self.with.as_deref().map_or(&[], |with| &with.ctes)
    }
}

/// One `|> ...` step, which takes the table so far as its input.
#[derive(Debug, Clone, PartialEq)]
pub enum PipeOperator<'a, X: AstExt> {
    Select(Vec<SelectItem<'a, X>>),
    Extend(Vec<SelectItem<'a, X>>),
    Set(Vec<Assignment<'a, X>>),
    Drop(Vec<Ident<'a>>),
    As(Ident<'a>),
    Where(Expr<'a, X>),
    Limit {
        count: Expr<'a, X>,
        offset: Option<Expr<'a, X>>,
    },
    OrderBy(Vec<OrderByItem<'a, X>>),
    Aggregate {
        aggregates: Vec<PipeAggregate<'a, X>>,
        group_by: Vec<PipeAggregate<'a, X>>,
    },
    TableSample(Box<SampleClause<'a, X>>),
    Rename(Vec<PipeRename<'a>>),
    /// `UNION`, `INTERSECT` or `EXCEPT` with parenthesized queries.
    SetOperation {
        op: SetOperator,
        quantifier: SetQuantifier,
        by_name: bool,
        queries: Vec<Query<'a, X>>,
    },
    Call {
        function: Expr<'a, X>,
        alias: Option<Ident<'a>>,
    },
    Pivot {
        clause: Box<PivotClause<'a, X>>,
        alias: Option<Ident<'a>>,
    },
    Unpivot {
        clause: Box<UnpivotClause<'a, X>>,
        alias: Option<Ident<'a>>,
    },
    Join(Box<Join<'a, X>>),
}

/// An item of `|> AGGREGATE`, or of its `GROUP BY`.
#[derive(Debug, Clone, PartialEq)]
pub struct PipeAggregate<'a, X: AstExt> {
    pub expr: ExprWithAlias<'a, X>,
    /// `Some(true)` for `ASC`, `Some(false)` for `DESC`, `None` if unspecified.
    pub asc: Option<bool>,
}

/// `name AS new_name` in `|> RENAME`.
#[derive(Debug, Clone, PartialEq)]
pub struct PipeRename<'a> {
    pub name: Ident<'a>,
    pub new_name: Ident<'a>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetOperator {
    Union,
    Except,
    Intersect,
    /// A synonym for `EXCEPT` in some dialects.
    Minus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetQuantifier {
    None,
    All,
    Distinct,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SetExpr<'a, X: AstExt> {
    Select(Box<Select<'a, X>>),
    /// A parenthesized query, which can have its own `ORDER BY` and `LIMIT`.
    Query(Box<Query<'a, X>>),
    /// `VALUES (a, b), (c, d)`
    Values(Vec<Vec<Expr<'a, X>>>),
    /// `VALUES ROW(a, b), ROW(c, d)`
    RowValues(Vec<Vec<Expr<'a, X>>>),
    /// `TABLE name`, which selects every column of a table.
    Table(ObjectName<'a>),
    /// An `INSERT`, `UPDATE` or `DELETE` used as a query, in `WITH`.
    Statement(Box<Statement<'a, X>>),
    SetOperation {
        left: Box<SetExpr<'a, X>>,
        op: SetOperator,
        quantifier: SetQuantifier,
        /// True if `BY NAME` followed the quantifier.
        by_name: bool,
        right: Box<SetExpr<'a, X>>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GroupByModifier {
    Rollup,
    Cube,
    /// `WITH TOTALS`
    Totals,
    /// Several modifiers, in the order written.
    Chain(Vec<GroupByModifier>),
}

#[derive(Debug, Clone)]
pub struct Select<'a, X: AstExt> {
    /// Dialect-specific syntax directly after `SELECT`.
    pub modifiers: X::Select<'a>,
    pub quantifier: SetQuantifier,
    pub projection: Vec<SelectItem<'a, X>>,
    /// `INTO` after the select list.
    pub into: Option<Box<SelectInto<'a, X>>>,
    pub from: Vec<TableWithJoins<'a, X>>,
    /// `LATERAL VIEW` clauses after `FROM`.
    pub lateral_views: Vec<LateralView<'a, X>>,
    /// `PREWHERE condition`, read before `WHERE`.
    pub prewhere: Option<Box<Expr<'a, X>>>,
    pub selection: Option<Expr<'a, X>>,
    pub connect_by: Vec<ConnectBy<'a, X>>,
    pub group_by: Vec<Expr<'a, X>>,
    /// True for `GROUP BY ALL`, in which case `group_by` is empty.
    pub group_by_all: bool,
    /// `WITH ROLLUP` or `WITH CUBE` after `GROUP BY`.
    pub group_by_modifier: Option<GroupByModifier>,
    /// `GROUPING SETS (...)` after a `GROUP BY` list, holding an
    /// [`SpecialExpr::Grouping`].
    pub grouping_sets: Option<Box<Expr<'a, X>>>,
    pub having: Option<Expr<'a, X>>,
    /// `QUALIFY condition`, in dialects that have it.
    pub qualify: Option<Expr<'a, X>>,
    /// `WINDOW name AS (...)`
    pub windows: Vec<NamedWindow<'a, X>>,
    /// True if `FROM` was written before `SELECT`, which can then be left out.
    pub from_first: bool,
    /// Ignored by `==`, like the span of an [`Ident`].
    pub span: Span,
}

impl<X: AstExt> PartialEq for Select<'_, X> {
    fn eq(&self, other: &Self) -> bool {
        // Destructured so that a new field cannot be left out.
        let Select {
            modifiers,
            quantifier,
            projection,
            into,
            from,
            lateral_views,
            prewhere,
            selection,
            connect_by,
            group_by,
            group_by_all,
            group_by_modifier,
            grouping_sets,
            having,
            qualify,
            windows,
            from_first,
            span: _,
        } = self;
        *modifiers == other.modifiers
            && *quantifier == other.quantifier
            && *projection == other.projection
            && *into == other.into
            && *from == other.from
            && *lateral_views == other.lateral_views
            && *prewhere == other.prewhere
            && *selection == other.selection
            && *connect_by == other.connect_by
            && *group_by == other.group_by
            && *group_by_all == other.group_by_all
            && *group_by_modifier == other.group_by_modifier
            && *grouping_sets == other.grouping_sets
            && *having == other.having
            && *qualify == other.qualify
            && *windows == other.windows
            && *from_first == other.from_first
    }
}

impl<X: AstExt> Spanned for Select<'_, X> {
    fn span(&self) -> Span {
        self.span
    }
}

impl<X: AstExt> Spanned for SetExpr<'_, X> {
    /// The span of a `SELECT`, or of both sides of a set operation. Other
    /// bodies, such as `VALUES`, have an empty span.
    fn span(&self) -> Span {
        match self {
            SetExpr::Select(select) => select.span,
            SetExpr::Query(query) => query.body.span(),
            SetExpr::Statement(statement) => statement.span,
            SetExpr::SetOperation { left, right, .. } => {
                let (left, right) = (left.span(), right.span());
                if left == Span::default() || right == Span::default() {
                    Span::default()
                } else {
                    Span {
                        start: left.start,
                        end: right.end,
                    }
                }
            }
            SetExpr::Values(_) | SetExpr::RowValues(_) | SetExpr::Table(_) => Span::default(),
        }
    }
}

/// `INTO [TEMPORARY | TEMP | UNLOGGED] [TABLE] targets`
#[derive(Debug, Clone, PartialEq)]
pub struct SelectInto<'a, X: AstExt> {
    /// `TEMPORARY`, `TEMP` or `UNLOGGED`, as written.
    pub modifier: Option<Cow<'a, str>>,
    pub table_keyword: bool,
    /// The table or variables, each a name or placeholder.
    pub targets: Vec<Expr<'a, X>>,
}

/// `LATERAL VIEW [OUTER] function name [AS] a, b`
#[derive(Debug, Clone, PartialEq)]
pub struct LateralView<'a, X: AstExt> {
    pub outer: bool,
    pub function: Expr<'a, X>,
    pub name: ObjectName<'a>,
    pub columns: Vec<Ident<'a>>,
}

/// How a query's rows are spread between and ordered within partitions.
#[derive(Debug, Clone, PartialEq)]
pub struct QueryDistribution<'a, X: AstExt> {
    pub cluster_by: Vec<Expr<'a, X>>,
    pub distribute_by: Vec<Expr<'a, X>>,
    pub sort_by: Vec<OrderByItem<'a, X>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ConnectBy<'a, X: AstExt> {
    /// `CONNECT BY [NOCYCLE] relationships`
    ConnectBy {
        nocycle: bool,
        relationships: Vec<Expr<'a, X>>,
    },
    /// `START WITH condition`
    StartWith(Expr<'a, X>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct NamedWindow<'a, X: AstExt> {
    pub name: Ident<'a>,
    pub window: WindowOver<'a, X>,
}

/// `OUTPUT items [INTO target]` on `INSERT`, `UPDATE`, `DELETE` or `MERGE`.
#[derive(Debug, Clone, PartialEq)]
pub struct OutputClause<'a, X: AstExt> {
    pub items: Vec<SelectItem<'a, X>>,
    pub into: Option<ObjectName<'a>>,
}

/// A single SQL statement.
#[derive(Debug, Clone, PartialEq)]
pub enum StatementKind<'a, X: AstExt> {
    Query(Box<Query<'a, X>>),
    Insert(Box<Insert<'a, X>>),
    Update(Box<Update<'a, X>>),
    Delete(Box<Delete<'a, X>>),
    CreateTable(Box<CreateTable<'a, X>>),
    CreateView(Box<CreateView<'a, X>>),
    CreateIndex(Box<CreateIndex<'a, X>>),
    CreateSchema(Box<CreateSchema<'a>>),
    CreateDatabase(Box<CreateDatabase<'a>>),
    Drop(Box<DropObject<'a>>),
    Truncate(Box<Truncate<'a>>),
    AlterTable(Box<AlterTable<'a, X>>),
    Grant(Box<Grant<'a>>),
    Revoke(Box<Revoke<'a>>),
    Merge(Box<Merge<'a, X>>),
    /// `DROP FUNCTION` or `DROP PROCEDURE`.
    DropRoutine(Box<DropRoutine<'a, X>>),
    Show(Box<Show<'a, X>>),
    CreateSequence(Box<CreateSequence<'a>>),
    /// `ALTER VIEW name [(columns)] AS query`
    AlterView(Box<AlterView<'a, X>>),
    /// `ALTER INDEX name RENAME TO new_name`
    AlterIndexRename {
        name: ObjectName<'a>,
        new_name: ObjectName<'a>,
    },
    /// `CREATE ROLE [IF NOT EXISTS] names`
    CreateRole(Box<CreateRole<'a>>),
    /// `ANALYZE [TABLE] [names]`
    Analyze(Box<Analyze<'a>>),
    Transaction(Box<Transaction<'a>>),
    /// `USE name`
    Use(Box<ObjectName<'a>>),
    /// `EXPLAIN`, `DESCRIBE` or `DESC`.
    Explain(Box<Explain<'a, X>>),
    Set(Box<SetStatement<'a, X>>),
    Comment(Box<CommentOn<'a>>),
    /// `CALL name [(args)]`, holding an [`ExprKind::Function`] or [`ExprKind::Compound`].
    Call(Box<Expr<'a, X>>),
    /// A statement that only the dialect knows about.
    Ext(X::Statement<'a>),
}

/// A statement, and the source text it covers.
///
/// `span` is ignored by `==`, like the span of an [`Ident`].
pub struct Statement<'a, X: AstExt> {
    pub kind: StatementKind<'a, X>,
    pub span: Span,
}

impl<'a, X: AstExt> Statement<'a, X> {
    pub fn new(kind: StatementKind<'a, X>, span: Span) -> Self {
        Self { kind, span }
    }
}

/// A node that did not come from the parser has an empty span.
impl<'a, X: AstExt> From<StatementKind<'a, X>> for Statement<'a, X> {
    fn from(kind: StatementKind<'a, X>) -> Self {
        Self::new(kind, Span::default())
    }
}

// Written by hand so that each level of a deep tree adds no stack frames.
impl<X: AstExt> Clone for Statement<'_, X> {
    #[inline(always)]
    fn clone(&self) -> Self {
        Self {
            kind: self.kind.clone(),
            span: self.span,
        }
    }
}

impl<X: AstExt> fmt::Debug for Statement<'_, X> {
    #[inline(always)]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.kind, f)?;
        write!(f, "@{}..{}", self.span.start, self.span.end)
    }
}

impl<X: AstExt> PartialEq for Statement<'_, X> {
    #[inline(always)]
    fn eq(&self, other: &Self) -> bool {
        self.kind == other.kind
    }
}

impl<X: AstExt> Spanned for Statement<'_, X> {
    fn span(&self) -> Span {
        self.span
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum InsertSource<'a, X: AstExt> {
    Values(Vec<Vec<Expr<'a, X>>>),
    /// `VALUES ROW(a, b), ROW(c, d)`
    RowValues(Vec<Vec<Expr<'a, X>>>),
    /// `SET a = 1, b = 2`
    Set(Vec<Assignment<'a, X>>),
    Query(Box<Query<'a, X>>),
    DefaultValues,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Insert<'a, X: AstExt> {
    /// Dialect-specific syntax directly after `INSERT`.
    pub modifiers: X::Insert<'a>,
    /// False if `INTO` was left out, in dialects where it is optional.
    pub into: bool,
    pub table: ObjectName<'a>,
    pub columns: Vec<Ident<'a>>,
    pub output: Option<Box<OutputClause<'a, X>>>,
    pub source: InsertSource<'a, X>,
    pub returning: Vec<SelectItem<'a, X>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Assignment<'a, X: AstExt> {
    pub target: ObjectName<'a>,
    pub value: Expr<'a, X>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Update<'a, X: AstExt> {
    pub table: TableRef<'a, X>,
    /// Joins written before `SET`, in dialects that have them.
    pub joins: Vec<Join<'a, X>>,
    pub assignments: Vec<Assignment<'a, X>>,
    /// `FROM` tables, after the assignments or before `SET`.
    pub from: Vec<TableWithJoins<'a, X>>,
    /// True if `FROM` was written before `SET`.
    pub from_before_set: bool,
    pub output: Option<Box<OutputClause<'a, X>>>,
    pub selection: Option<Expr<'a, X>>,
    /// `ORDER BY` after the assignments, in dialects that have it.
    pub order_by: Vec<OrderByItem<'a, X>>,
    /// `LIMIT` after the assignments, in dialects that have it.
    pub limit: Option<Box<Limit<'a, X>>>,
    pub returning: Vec<SelectItem<'a, X>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Delete<'a, X: AstExt> {
    /// The table deleted from. In a multi-table form, the first of `targets`.
    pub table: TableRef<'a, X>,
    /// The tables of a multi-table `DELETE`.
    pub multi: Option<Box<MultiTableDelete<'a>>>,
    /// The tables after `USING`, or after `FROM` in a multi-table form.
    pub using: Vec<TableWithJoins<'a, X>>,
    pub output: Option<Box<OutputClause<'a, X>>>,
    pub selection: Option<Expr<'a, X>>,
    /// `ORDER BY` after the table, in dialects that have it.
    pub order_by: Vec<OrderByItem<'a, X>>,
    /// `LIMIT` after the table, in dialects that have it.
    pub limit: Option<Box<Limit<'a, X>>>,
    pub returning: Vec<SelectItem<'a, X>>,
}

/// The tables a multi-table `DELETE` removes rows from.
#[derive(Debug, Clone, PartialEq)]
pub struct MultiTableDelete<'a> {
    pub targets: Vec<ObjectName<'a>>,
    /// True for `DELETE a, b FROM ...`, false for `DELETE FROM a, b USING ...`.
    pub from_keyword: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DataType<'a> {
    /// The schema of a qualified name: `public.mood` has `public` here.
    pub qualifier: Vec<Ident<'a>>,
    pub name: Ident<'a>,
    /// The arguments in `ARRAY<INT>` and `Array(Int)`.
    pub angle: Option<Box<AngleArguments<'a>>>,
    /// The numbers in `decimal(10, 2)`.
    pub args: Vec<Cow<'a, str>>,
    pub suffix: DataTypeSuffix,
    pub array: ArrayType<'a>,
}

/// What is between the angle brackets of a complex type.
#[derive(Debug, Clone, PartialEq)]
pub enum AngleArguments<'a> {
    Types(Vec<DataType<'a>>),
    Fields(Vec<StructField<'a>>),
    /// The same, written in parentheses: `Array(Int16)`.
    ParenTypes(Vec<DataType<'a>>),
    /// `Tuple(a Int8)`, or a struct definition written in parentheses.
    ParenFields(Vec<StructField<'a>>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct StructField<'a> {
    /// `None` for a field written as just its type.
    pub name: Option<Ident<'a>>,
    /// True if written `name: type`.
    pub colon: bool,
    pub data_type: DataType<'a>,
    pub not_null: bool,
    pub comment: Option<StringLiteral<'a>>,
    /// `OPTIONS(key = value, ...)`, which only literal values can fill.
    pub options: Vec<StructFieldOption<'a>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StructFieldOption<'a> {
    pub key: Ident<'a>,
    pub value: Literal<'a>,
}

/// How a data type is made into an array. Sizes are the source text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArrayType<'a> {
    None,
    /// `INT[]` or `INT[3][4]`, with one entry per dimension.
    Brackets(Vec<Option<Cow<'a, str>>>),
    /// `INT ARRAY` or `INT ARRAY[3]`.
    Keyword(Option<Cow<'a, str>>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum ColumnOption<'a, X: AstExt> {
    /// `COLLATE name`
    Collate(ObjectName<'a>),
    /// `IDENTITY` or `AUTOINCREMENT`, with its parameters.
    Identity {
        autoincrement: bool,
        params: Option<IdentityParams<'a>>,
        /// `Some(true)` for `ORDER`, `Some(false)` for `NOORDER`.
        order: Option<bool>,
    },
    /// `[WITH] {MASKING | PROJECTION} POLICY name [USING (columns)]`
    Policy {
        with: bool,
        masking: bool,
        name: ObjectName<'a>,
        using: Vec<Ident<'a>>,
    },
    /// `[WITH] TAG (name = 'value', ...)`
    Tags {
        with: bool,
        tags: Vec<Tag<'a>>,
    },
    /// `COMMENT 'text'`
    Comment(StringLiteral<'a>),
    /// `OPTIONS(key = value, ...)`
    Options(Vec<ExprOption<'a, X>>),
    /// `OPTIONS (name 'value', ...)`
    GenericOptions(Vec<GenericOption<'a>>),
    /// `NULLS DISTINCT` or `NULLS NOT DISTINCT` after `UNIQUE`.
    NullsDistinct(bool),
    /// `GENERATED ... AS ...`
    Generated(Box<Generated<'a, X>>),
    /// An attribute of the constraint before it, such as `DEFERRABLE`.
    Attribute(ConstraintAttribute),
    NotNull,
    Null,
    PrimaryKey,
    Unique,
    Default(Expr<'a, X>),
    /// A keyword a dialect adds before an optional expression, such as
    /// `MATERIALIZED expr`.
    Keyword {
        keyword: Cow<'a, str>,
        expr: Option<Expr<'a, X>>,
    },
    AutoIncrement,
    Check(Expr<'a, X>),
    References(ForeignKeyRef<'a>),
    /// `CONSTRAINT name` before another option.
    Named {
        name: Ident<'a>,
        option: Box<ColumnOption<'a, X>>,
    },
}

/// `key = value`, in an `OPTIONS(...)` list.
#[derive(Debug, Clone, PartialEq)]
pub struct ExprOption<'a, X: AstExt> {
    pub name: Ident<'a>,
    pub value: Expr<'a, X>,
}

/// Numbers are the source text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdentityParams<'a> {
    pub seed: Cow<'a, str>,
    pub increment: Cow<'a, str>,
    /// True if written `START seed INCREMENT step`, false if `(seed, step)`.
    pub start_form: bool,
}

/// A number with the sign it was written with, if any.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignedNumber<'a> {
    /// `-`, `+`, or empty.
    pub sign: Cow<'a, str>,
    pub digits: Cow<'a, str>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SequenceOption<'a> {
    Increment {
        by: bool,
        value: SignedNumber<'a>,
    },
    /// `None` for `NO MINVALUE`.
    MinValue(Option<SignedNumber<'a>>),
    /// `None` for `NO MAXVALUE`.
    MaxValue(Option<SignedNumber<'a>>),
    Start {
        with: bool,
        value: SignedNumber<'a>,
    },
    Cache(SignedNumber<'a>),
    /// `false` for `NO CYCLE`.
    Cycle(bool),
    /// `OWNED BY name`, where the name can be `NONE`.
    OwnedBy(ObjectName<'a>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateSequence<'a> {
    pub temporary: bool,
    pub if_not_exists: bool,
    pub name: ObjectName<'a>,
    pub data_type: Option<DataType<'a>>,
    pub options: Vec<SequenceOption<'a>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GeneratedWhen {
    Always,
    ByDefault,
    /// `AS (expr)` with `GENERATED` left out.
    Omitted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GeneratedStorage {
    Stored,
    Virtual,
}

#[derive(Debug, Clone, PartialEq)]
pub enum GeneratedKind<'a, X: AstExt> {
    /// `AS IDENTITY [(options)]`
    Identity(Option<Vec<SequenceOption<'a>>>),
    /// `AS (expr) [STORED | VIRTUAL]`
    Expr {
        expr: Expr<'a, X>,
        storage: Option<GeneratedStorage>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct Generated<'a, X: AstExt> {
    pub when: Option<GeneratedWhen>,
    pub kind: GeneratedKind<'a, X>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConstraintAttribute {
    /// `false` for `NOT DEFERRABLE`.
    Deferrable(bool),
    /// `true` for `INITIALLY DEFERRED`, `false` for `INITIALLY IMMEDIATE`.
    InitiallyDeferred(bool),
    /// `false` for `NOT ENFORCED`.
    Enforced(bool),
    NotValid,
    NoInherit,
}

/// A `name = 'value'` tag on an object.
#[derive(Debug, Clone, PartialEq)]
pub struct Tag<'a> {
    pub name: ObjectName<'a>,
    pub value: StringLiteral<'a>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDef<'a, X: AstExt> {
    pub name: Ident<'a>,
    pub data_type: DataType<'a>,
    pub options: Vec<ColumnOption<'a, X>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TableConstraintKind<'a, X: AstExt> {
    PrimaryKey(Vec<Ident<'a>>),
    /// `PRIMARY KEY USING INDEX name`
    PrimaryKeyUsingIndex(Ident<'a>),
    Unique(Vec<Ident<'a>>),
    /// `UNIQUE USING INDEX name`
    UniqueUsingIndex(Ident<'a>),
    Check(Expr<'a, X>),
    ForeignKey {
        /// The name in `FOREIGN KEY name (columns)`, in dialects that have it.
        index_name: Option<Ident<'a>>,
        columns: Vec<Ident<'a>>,
        references: ForeignKeyRef<'a>,
    },
    /// A key or index that a dialect adds to a table: `KEY name (a, b)`.
    Key {
        /// All of its keywords, such as `"FULLTEXT INDEX"`.
        keyword: Cow<'a, str>,
        name: Option<Ident<'a>>,
        /// The method in `USING name`, before the columns.
        using: Option<Ident<'a>>,
        columns: Vec<IndexColumn<'a, X>>,
        options: Vec<KeyOption<'a>>,
    },
    /// `NOT NULL column`
    NotNull(Ident<'a>),
    /// `EXCLUDE [USING method] (element, ...)`
    Exclude(Box<ExcludeConstraint<'a, X>>),
}

/// Text after the columns of a [`TableConstraintKind::Key`].
#[derive(Debug, Clone, PartialEq)]
pub enum KeyOption<'a> {
    /// `USING name`
    Using(Ident<'a>),
    /// `COMMENT 'text'`
    Comment(StringLiteral<'a>),
}

/// `EXCLUDE [USING method] (element, ...)`, with its predicate if any.
#[derive(Debug, Clone, PartialEq)]
pub struct ExcludeConstraint<'a, X: AstExt> {
    pub using: Option<Ident<'a>>,
    pub elements: Vec<ExcludeElement<'a, X>>,
    /// The `WHERE (condition)`, which follows any `INCLUDE`.
    pub predicate: Option<Expr<'a, X>>,
}

/// `column WITH operator`, in an exclusion constraint.
#[derive(Debug, Clone, PartialEq)]
pub struct ExcludeElement<'a, X: AstExt> {
    pub column: IndexColumn<'a, X>,
    pub operator: ExcludeOperator<'a>,
}

/// The operator of an exclusion constraint element.
#[derive(Debug, Clone, PartialEq)]
pub enum ExcludeOperator<'a> {
    Plain(Cow<'a, str>),
    /// `OPERATOR(schema.op)`
    Qualified(OperatorName<'a>),
}

/// `[schema.]operator`
#[derive(Debug, Clone, PartialEq)]
pub struct OperatorName<'a> {
    pub schema: Vec<Ident<'a>>,
    pub operator: Cow<'a, str>,
}

/// `FOR VALUES ...`, or `DEFAULT`, on a partition.
#[derive(Debug, Clone, PartialEq)]
pub enum PartitionBound<'a, X: AstExt> {
    Default,
    /// `FOR VALUES IN (a, b)`
    In(Vec<Expr<'a, X>>),
    /// `FOR VALUES FROM (a) TO (b)`
    Range {
        from: Vec<RangeBound<'a, X>>,
        to: Vec<RangeBound<'a, X>>,
    },
    /// `FOR VALUES WITH (MODULUS m, REMAINDER r)`
    Hash {
        modulus: Cow<'a, str>,
        remainder: Cow<'a, str>,
    },
}

/// A value in `FOR VALUES FROM (...) TO (...)`.
#[derive(Debug, Clone, PartialEq)]
pub enum RangeBound<'a, X: AstExt> {
    MinValue,
    MaxValue,
    Value(Expr<'a, X>),
}

/// `name 'value'`, in an `OPTIONS (...)` list. In `ALTER` it may be prefixed
/// with `ADD`, `SET` or `DROP`, and `DROP` takes no value.
#[derive(Debug, Clone, PartialEq)]
pub struct GenericOption<'a> {
    pub action: Option<Cow<'a, str>>,
    pub name: Ident<'a>,
    pub value: Option<StringLiteral<'a>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TableConstraint<'a, X: AstExt> {
    pub name: Option<Ident<'a>>,
    pub kind: TableConstraintKind<'a, X>,
    /// The columns in `INCLUDE (a, b)`.
    pub include: Vec<Ident<'a>>,
    /// `Some(true)` for `NULLS DISTINCT`, `Some(false)` for `NULLS NOT DISTINCT`.
    pub nulls_distinct: Option<bool>,
    pub attributes: Vec<ConstraintAttribute>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateTable<'a, X: AstExt> {
    pub or_replace: bool,
    pub temporary: bool,
    pub name: ObjectName<'a>,
    pub if_not_exists: bool,
    pub columns: Vec<ColumnDef<'a, X>>,
    pub constraints: Vec<TableConstraint<'a, X>>,
    /// The query in `CREATE TABLE t AS SELECT ...`.
    pub query: Option<Box<Query<'a, X>>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum IsTest<'a, X: AstExt> {
    Null,
    True,
    False,
    Unknown,
    DistinctFrom(Box<Expr<'a, X>>),
    /// `IS JSON [type] [{WITH | WITHOUT} UNIQUE [KEYS]]`
    Json {
        kind: Option<JsonPredicateType>,
        unique_keys: Option<JsonUniqueKeys>,
    },
    /// `IS [form] NORMALIZED`
    Normalized(Option<NormalForm>),
}

/// The type an `IS JSON` predicate tests for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JsonPredicateType {
    Value,
    Array,
    Object,
    Scalar,
}

/// `{WITH | WITHOUT} UNIQUE [KEYS]` in an `IS JSON` predicate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JsonUniqueKeys {
    pub with: bool,
    /// True if `KEYS` was written.
    pub keys: bool,
}

/// A Unicode normalization form.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NormalForm {
    Nfc,
    Nfd,
    Nfkc,
    Nfkd,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LikeKind<'a> {
    Like,
    ILike,
    SimilarTo,
    /// A keyword a dialect adds, such as a regular expression match.
    Named(Cow<'a, str>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct CaseWhen<'a, X: AstExt> {
    pub condition: Expr<'a, X>,
    pub result: Expr<'a, X>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CaseExpr<'a, X: AstExt> {
    pub operand: Option<Expr<'a, X>>,
    pub branches: Vec<CaseWhen<'a, X>>,
    pub else_result: Option<Expr<'a, X>>,
}

/// The less common parts of a function call.
#[derive(Debug, Clone, PartialEq)]
pub struct FunctionOptions<'a, X: AstExt> {
    /// The first argument list of a parametric call: `f(0.5)(x)`.
    pub parameters: Vec<FunctionArg<'a, X>>,
    pub quantifier: SetQuantifier,
    pub order_by: Vec<OrderByItem<'a, X>>,
    /// `WITHIN GROUP (ORDER BY ...)`
    pub within_group: Vec<OrderByItem<'a, X>>,
    pub filter: Option<Expr<'a, X>>,
    pub null_treatment: Option<NullTreatment>,
    /// `IGNORE NULLS` or `RESPECT NULLS` inside the parentheses.
    pub inner_null_treatment: Option<NullTreatment>,
    /// `SEPARATOR 'text'` after the arguments, in dialects that have it.
    pub separator: Option<StringLiteral<'a>>,
    /// `ON OVERFLOW ...` after the arguments, in dialects that have it.
    pub on_overflow: Option<Box<ListAggOnOverflow<'a, X>>>,
    pub over: Option<WindowOver<'a, X>>,
}

/// `ON OVERFLOW ERROR` or `ON OVERFLOW TRUNCATE [filler] {WITH | WITHOUT} COUNT`.
#[derive(Debug, Clone, PartialEq)]
pub enum ListAggOnOverflow<'a, X: AstExt> {
    Error,
    Truncate {
        filler: Option<Expr<'a, X>>,
        with_count: bool,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NullTreatment {
    Ignore,
    Respect,
}

#[derive(Debug, Clone, PartialEq)]
pub enum WindowOver<'a, X: AstExt> {
    Named(Ident<'a>),
    Spec(WindowSpec<'a, X>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct WindowSpec<'a, X: AstExt> {
    /// The window this one extends: `OVER (w ORDER BY a)`.
    pub base: Option<Ident<'a>>,
    pub partition_by: Vec<Expr<'a, X>>,
    pub order_by: Vec<OrderByItem<'a, X>>,
    pub frame: Option<WindowFrame<'a, X>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameUnits {
    Rows,
    Range,
    Groups,
}

#[derive(Debug, Clone, PartialEq)]
pub enum FrameBound<'a, X: AstExt> {
    CurrentRow,
    UnboundedPreceding,
    UnboundedFollowing,
    Preceding(Expr<'a, X>),
    Following(Expr<'a, X>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct WindowFrame<'a, X: AstExt> {
    pub units: FrameUnits,
    pub start: FrameBound<'a, X>,
    /// Present in the `BETWEEN start AND end` form.
    pub end: Option<FrameBound<'a, X>>,
}

/// The words that complete a multi-word data type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataTypeSuffix {
    None,
    /// `DOUBLE PRECISION`
    Precision,
    /// `CHARACTER VARYING`
    Varying,
    /// `TIMESTAMP WITH TIME ZONE`
    WithTimeZone,
    /// `TIMESTAMP WITHOUT TIME ZONE`
    WithoutTimeZone,
    /// The unit range of an `INTERVAL` type, such as `DAY TO HOUR`. Any
    /// precision is in the type's arguments.
    IntervalFields(IntervalFields),
    /// `INT UNSIGNED`
    Unsigned,
    /// `INT ZEROFILL`
    Zerofill,
    /// `INT UNSIGNED ZEROFILL`
    UnsignedZerofill,
    /// `DOUBLE PRECISION UNSIGNED`
    PrecisionUnsigned,
    /// `SIGNED INTEGER` or `UNSIGNED INTEGER`, with the first word as the name.
    Integer,
    /// Arguments written with signs or as non-integers, such as `CHAR(-1)`,
    /// which render separated by `, `. Holds the sign byte (`+`, `-` or 0) of
    /// the first two arguments.
    SignedArgs([u8; 2]),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IntervalFields {
    Year,
    Month,
    Day,
    Hour,
    Minute,
    Second,
    YearToMonth,
    DayToHour,
    DayToMinute,
    DayToSecond,
    HourToMinute,
    HourToSecond,
    MinuteToSecond,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReferentialAction {
    Cascade,
    Restrict,
    SetNull,
    SetDefault,
    NoAction,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ForeignKeyRef<'a> {
    pub table: ObjectName<'a>,
    pub columns: Vec<Ident<'a>>,
    /// `FULL`, `PARTIAL` or `SIMPLE`.
    pub match_kind: Option<Cow<'a, str>>,
    pub on_delete: Option<ReferentialAction>,
    pub on_update: Option<ReferentialAction>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObjectType<'a> {
    Table,
    View,
    MaterializedView,
    Index,
    Schema,
    Database,
    Sequence,
    /// A kind of object a dialect adds, rendered as this text.
    Named(Cow<'a, str>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DropBehavior {
    Cascade,
    Restrict,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DropObject<'a> {
    /// True if `TEMPORARY` follows `DROP`.
    pub temporary: bool,
    pub object_type: ObjectType<'a>,
    pub if_exists: bool,
    pub names: Vec<ObjectName<'a>>,
    /// The table in `DROP INDEX i ON t`.
    pub on: Option<ObjectName<'a>>,
    pub behavior: Option<DropBehavior>,
    /// `PURGE`, for dialects that have it.
    pub purge: bool,
}

/// What is granted. Words are the source text; rendering upper-cases them.
#[derive(Debug, Clone, PartialEq)]
pub enum Privileges<'a> {
    /// `ALL [PRIVILEGES]`
    All {
        privileges: bool,
    },
    List(Vec<Privilege<'a>>),
    /// `[DATABASE] ROLE name`
    Role {
        database: bool,
        name: ObjectName<'a>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct Privilege<'a> {
    pub words: Vec<Cow<'a, str>>,
    /// The columns in `UPDATE (a, b)`.
    pub columns: Vec<Ident<'a>>,
}

/// What a grant applies to.
#[derive(Debug, Clone, PartialEq)]
pub struct GrantObjects<'a> {
    /// The words before the names, such as `ALL TABLES IN SCHEMA`.
    pub kind: Vec<Cow<'a, str>>,
    pub names: Vec<GrantName<'a>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct GrantName<'a> {
    pub name: ObjectName<'a>,
    /// The argument types of a function.
    pub args: Option<Vec<DataType<'a>>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Grantee<'a> {
    /// The words before the name, such as `DATABASE ROLE`.
    pub kind: Vec<Cow<'a, str>>,
    pub name: ObjectName<'a>,
    /// The host in `user@host`, in dialects that have it.
    pub host: Option<Ident<'a>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CurrentGrants {
    Copy,
    Revoke,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Grant<'a> {
    pub privileges: Privileges<'a>,
    /// `None` when granting a role.
    pub objects: Option<GrantObjects<'a>>,
    pub grantees: Vec<Grantee<'a>>,
    pub with_grant_option: bool,
    pub current_grants: Option<CurrentGrants>,
    pub as_grantor: Option<Ident<'a>>,
    pub granted_by: Option<Ident<'a>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Revoke<'a> {
    pub grant_option_for: bool,
    pub privileges: Privileges<'a>,
    pub objects: GrantObjects<'a>,
    pub grantees: Vec<Grantee<'a>>,
    pub granted_by: Option<Ident<'a>>,
    pub behavior: Option<DropBehavior>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Merge<'a, X: AstExt> {
    pub into: bool,
    pub table: TableRef<'a, X>,
    pub source: TableRef<'a, X>,
    pub on: Expr<'a, X>,
    pub clauses: Vec<MergeClause<'a, X>>,
    pub output: Option<Box<OutputClause<'a, X>>>,
    pub returning: Vec<SelectItem<'a, X>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeClauseKind {
    Matched,
    NotMatched,
    NotMatchedBySource,
    NotMatchedByTarget,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MergeClause<'a, X: AstExt> {
    pub kind: MergeClauseKind,
    pub predicate: Option<Expr<'a, X>>,
    pub action: MergeAction<'a, X>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum MergeAction<'a, X: AstExt> {
    Update {
        /// `None` for `UPDATE SET *`.
        assignments: Option<Vec<Assignment<'a, X>>>,
        predicate: Option<Expr<'a, X>>,
        delete_predicate: Option<Expr<'a, X>>,
    },
    Delete,
    Insert {
        columns: Vec<ObjectName<'a>>,
        rows: Vec<Vec<Expr<'a, X>>>,
        predicate: Option<Expr<'a, X>>,
    },
    /// `INSERT *`
    InsertAll,
    /// `INSERT [(columns)] ROW`
    InsertRow {
        columns: Vec<ObjectName<'a>>,
        predicate: Option<Expr<'a, X>>,
    },
    DoNothing,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RoutineArg<'a, X: AstExt> {
    /// `IN`, `OUT`, `INOUT` or `VARIADIC`.
    pub mode: Option<Cow<'a, str>>,
    pub name: Option<Ident<'a>>,
    pub data_type: DataType<'a>,
    /// The default value. The flag is true if written with `=`, false if
    /// written with `DEFAULT`.
    pub default: Option<(bool, Expr<'a, X>)>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RoutineSignature<'a, X: AstExt> {
    pub name: ObjectName<'a>,
    /// `None` if the name has no parentheses.
    pub args: Option<Vec<RoutineArg<'a, X>>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DropRoutine<'a, X: AstExt> {
    pub procedure: bool,
    pub if_exists: bool,
    pub routines: Vec<RoutineSignature<'a, X>>,
    pub behavior: Option<DropBehavior>,
}

/// `SHOW ...`. What can be shown differs widely between databases, so the
/// object is the words as written.
#[derive(Debug, Clone, PartialEq)]
pub struct Show<'a, X: AstExt> {
    /// Words such as `TERSE` and `FULL`.
    pub modifiers: Vec<Cow<'a, str>>,
    /// What is shown, such as `TABLES` or `MATERIALIZED VIEWS`.
    pub object: Vec<Cow<'a, str>>,
    /// The object in `SHOW CREATE TABLE name`.
    pub name: Option<ObjectName<'a>>,
    /// In the order written.
    pub clauses: Vec<ShowClause<'a, X>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ShowClause<'a, X: AstExt> {
    History,
    Like(StringLiteral<'a>),
    Where(Expr<'a, X>),
    StartsWith(StringLiteral<'a>),
    Limit(Expr<'a, X>),
    /// A bare string filter: `SHOW CATALOGS 'a*'`.
    Pattern(StringLiteral<'a>),
    /// `FROM 'value'`, the position to continue listing from.
    FromValue(StringLiteral<'a>),
    /// `{IN | FROM} [scope] [name]`
    Location {
        from: bool,
        scope: Option<Cow<'a, str>>,
        name: Option<ObjectName<'a>>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsolationLevel {
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Serializable,
    Snapshot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionMode {
    IsolationLevel(IsolationLevel),
    ReadOnly,
    ReadWrite,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FinishKind {
    Commit,
    End,
    Rollback,
    /// `ABORT`, which is `ROLLBACK` by another name.
    Abort,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Transaction<'a> {
    Start(Vec<TransactionMode>),
    /// `BEGIN [TRAN | TRANSACTION | WORK]`
    Begin(Option<Cow<'a, str>>),
    Finish {
        kind: FinishKind,
        chain: bool,
        savepoint: Option<Ident<'a>>,
    },
    Savepoint(Ident<'a>),
    Release(Ident<'a>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExplainFormat<'a> {
    /// True if written `FORMAT=name`.
    pub equals: bool,
    pub name: Cow<'a, str>,
}

/// An option in `EXPLAIN (name value, ...)`.
#[derive(Debug, Clone, PartialEq)]
pub struct ExplainOption<'a> {
    pub name: Ident<'a>,
    /// `-`, `+`, or empty.
    pub sign: Cow<'a, str>,
    /// The value's source text.
    pub value: Option<Cow<'a, str>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ExplainTarget<'a, X: AstExt> {
    Statement(Box<Statement<'a, X>>),
    Table {
        /// True for `DESCRIBE TABLE t`.
        table_keyword: bool,
        name: ObjectName<'a>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct Explain<'a, X: AstExt> {
    /// `EXPLAIN`, `DESCRIBE` or `DESC`.
    pub keyword: Cow<'a, str>,
    pub options: Vec<ExplainOption<'a>>,
    /// `EXTENDED` or `FORMATTED`.
    pub detail: Option<Cow<'a, str>>,
    pub analyze: bool,
    pub verbose: bool,
    pub estimate: bool,
    pub query_plan: bool,
    pub format: Option<ExplainFormat<'a>>,
    /// True if the keyword `TABLE` was written before the table's name.
    pub target: ExplainTarget<'a, X>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SetAssignment<'a, X: AstExt> {
    pub scope: Option<Cow<'a, str>>,
    pub name: ObjectName<'a>,
    pub value: Expr<'a, X>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SetStatement<'a, X: AstExt> {
    Variable {
        /// `LOCAL`, `SESSION` or `GLOBAL`.
        scope: Option<Cow<'a, str>>,
        name: ObjectName<'a>,
        values: Vec<Expr<'a, X>>,
    },
    /// Several variables set at once.
    Assignments(Vec<SetAssignment<'a, X>>),
    Role {
        scope: Option<Cow<'a, str>>,
        role: Expr<'a, X>,
    },
    Authorization {
        scope: Option<Cow<'a, str>>,
        user: Expr<'a, X>,
    },
    TimeZone {
        scope: Option<Cow<'a, str>>,
        value: Expr<'a, X>,
    },
    Transaction {
        /// True for `SET SESSION CHARACTERISTICS AS TRANSACTION`.
        characteristics: bool,
        modes: Vec<TransactionMode>,
        snapshot: Option<StringLiteral<'a>>,
    },
    Names {
        charset: Expr<'a, X>,
        collate: Option<Ident<'a>>,
    },
}

/// `COMMENT [IF EXISTS] ON kind name IS comment`
#[derive(Debug, Clone, PartialEq)]
pub struct CommentOn<'a> {
    pub if_exists: bool,
    pub kind: Vec<Cow<'a, str>>,
    pub name: ObjectName<'a>,
    /// `None` for `IS NULL`.
    pub comment: Option<StringLiteral<'a>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Truncate<'a> {
    pub table_keyword: bool,
    pub if_exists: bool,
    pub names: Vec<ObjectName<'a>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AlterColumnAction<'a, X: AstExt> {
    SetNotNull,
    DropNotNull,
    SetDefault(Expr<'a, X>),
    DropDefault,
    /// `TYPE ty`, or `SET DATA TYPE ty` when `set_data` is true.
    SetType {
        data_type: DataType<'a>,
        set_data: bool,
        /// The conversion in `USING expr`.
        using: Option<Expr<'a, X>>,
    },
    AddGenerated(Box<Generated<'a, X>>),
    /// `OPTIONS (...)`
    GenericOptions(Vec<GenericOption<'a>>),
}

/// Where `FIRST` or `AFTER name` puts a column.
#[derive(Debug, Clone, PartialEq)]
pub enum ColumnPosition<'a> {
    First,
    After(Ident<'a>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum AlterTableOp<'a, X: AstExt> {
    /// `SET TBLPROPERTIES (key = value, ...)`
    SetTblProperties(Vec<(Expr<'a, X>, Expr<'a, X>)>),
    /// `ADD [IF NOT EXISTS] PARTITION (...) [PARTITION (...)]...`
    AddPartitions {
        if_not_exists: bool,
        partitions: Vec<Vec<Expr<'a, X>>>,
    },
    /// `DROP [IF EXISTS] PARTITION (...)`
    DropPartition {
        if_exists: bool,
        partition: Vec<Expr<'a, X>>,
    },
    /// `PARTITION (...) RENAME TO PARTITION (...) [[SET] LOCATION 'path']`
    RenamePartition {
        old: Vec<Expr<'a, X>>,
        new: Vec<Expr<'a, X>>,
        /// The path, and whether `SET` was written before `LOCATION`.
        location: Option<(bool, StringLiteral<'a>)>,
    },
    AddColumn {
        column_keyword: bool,
        if_not_exists: bool,
        column: ColumnDef<'a, X>,
        position: Option<ColumnPosition<'a>>,
    },
    /// A fixed phrase, such as `SET LOGGED`.
    Phrase(Cow<'a, str>),
    /// A fixed phrase and a name, such as `ENABLE TRIGGER name`.
    PhraseName {
        phrase: Cow<'a, str>,
        name: Ident<'a>,
    },
    /// A fixed phrase and a qualified name, such as `INHERIT parent`.
    PhraseObject {
        phrase: Cow<'a, str>,
        name: ObjectName<'a>,
    },
    /// `ATTACH PARTITION name bound`
    AttachPartition {
        name: ObjectName<'a>,
        bound: PartitionBound<'a, X>,
    },
    /// `DETACH PARTITION name [CONCURRENTLY | FINALIZE]`
    DetachPartition {
        name: ObjectName<'a>,
        mode: Option<Cow<'a, str>>,
    },
    /// `OPTIONS (...)`, in `ALTER FOREIGN TABLE`.
    GenericOptions(Vec<GenericOption<'a>>),
    /// `RESET (name, ...)`
    ResetOptions(Vec<ObjectName<'a>>),
    /// `name [=] value`, such as `ALGORITHM = COPY`.
    Setting {
        name: Cow<'a, str>,
        equals: bool,
        value: Cow<'a, str>,
    },
    /// `RENAME CONSTRAINT old TO new`
    RenameConstraint {
        old: Ident<'a>,
        new: Ident<'a>,
    },
    /// `RENAME AS name`
    RenameAs(ObjectName<'a>),
    /// `CHANGE COLUMN old definition`
    ChangeColumn {
        old: Ident<'a>,
        column: ColumnDef<'a, X>,
        position: Option<ColumnPosition<'a>>,
    },
    /// `MODIFY COLUMN definition`
    ModifyColumn {
        column: ColumnDef<'a, X>,
        position: Option<ColumnPosition<'a>>,
    },
    /// `SWAP WITH name`
    SwapWith(ObjectName<'a>),
    /// `CLUSTER BY (exprs)`
    ClusterBy(Vec<Expr<'a, X>>),
    /// `ALTER SORTKEY(columns)`
    AlterSortKey(Vec<Ident<'a>>),
    /// `SET (name = value, ...)`
    SetOptions(IndexStorage<'a, X>),
    DropColumn {
        column_keyword: bool,
        if_exists: bool,
        name: Ident<'a>,
        behavior: Option<DropBehavior>,
    },
    /// `DROP [COLUMN] [IF EXISTS] a, b` in dialects that list several columns.
    DropColumns {
        column_keyword: bool,
        if_exists: bool,
        names: Vec<Ident<'a>>,
        behavior: Option<DropBehavior>,
    },
    /// `DROP FOREIGN KEY name [behavior]`
    DropForeignKey {
        name: Ident<'a>,
        behavior: Option<DropBehavior>,
    },
    RenameColumn {
        old: Ident<'a>,
        new: Ident<'a>,
    },
    RenameTable(ObjectName<'a>),
    AlterColumn {
        column_keyword: bool,
        name: Ident<'a>,
        action: AlterColumnAction<'a, X>,
    },
    AddConstraint(TableConstraint<'a, X>),
    DropConstraint {
        if_exists: bool,
        name: Ident<'a>,
        behavior: Option<DropBehavior>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct AlterTable<'a, X: AstExt> {
    pub if_exists: bool,
    /// `ALTER TABLE ONLY`, for dialects that have it.
    pub only: bool,
    pub name: ObjectName<'a>,
    pub operations: Vec<AlterTableOp<'a, X>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateView<'a, X: AstExt> {
    pub or_replace: bool,
    pub temporary: bool,
    pub materialized: bool,
    pub if_not_exists: bool,
    pub name: ObjectName<'a>,
    pub columns: Vec<Ident<'a>>,
    /// Storage parameters: `WITH (name = value, name)`.
    pub with: IndexStorage<'a, X>,
    pub query: Box<Query<'a, X>>,
    /// True if `WITH NO SCHEMA BINDING` followed the query.
    pub no_schema_binding: bool,
}

/// `CREATE [OR REPLACE] ROLE [IF NOT EXISTS] name, ...`
#[derive(Debug, Clone, PartialEq)]
pub struct CreateRole<'a> {
    pub or_replace: bool,
    pub if_not_exists: bool,
    pub names: Vec<ObjectName<'a>>,
}

/// `ANALYZE [TABLE] [names]`
#[derive(Debug, Clone, PartialEq)]
pub struct Analyze<'a> {
    pub table_keyword: bool,
    pub names: Vec<ObjectName<'a>>,
}

/// `ALTER VIEW name [(columns)] AS query`
#[derive(Debug, Clone, PartialEq)]
pub struct AlterView<'a, X: AstExt> {
    pub name: ObjectName<'a>,
    pub columns: Vec<Ident<'a>>,
    pub query: Box<Query<'a, X>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateIndex<'a, X: AstExt> {
    pub unique: bool,
    pub concurrently: bool,
    /// `CREATE INDEX ASYNC`
    pub asynchronous: bool,
    pub if_not_exists: bool,
    pub name: Option<ObjectName<'a>>,
    pub using_before_on: Option<Ident<'a>>,
    pub table: ObjectName<'a>,
    pub using: Option<Ident<'a>>,
    pub columns: Vec<IndexColumn<'a, X>>,
    /// `USING` written after the column list.
    pub trailing_using: Option<Ident<'a>>,
    pub include: Vec<Ident<'a>>,
    /// `Some(true)` for `NULLS DISTINCT`, `Some(false)` for `NULLS NOT DISTINCT`.
    pub nulls_distinct: Option<bool>,
    /// Storage parameters: `WITH (name = value, name)`.
    pub with: IndexStorage<'a, X>,
    pub predicate: Option<Expr<'a, X>>,
    /// `ALGORITHM = value` and `LOCK = value`, in the order written.
    pub settings: Vec<(Cow<'a, str>, Cow<'a, str>)>,
}

/// Modifiers written before an index's name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct IndexFlags {
    pub concurrently: bool,
    pub asynchronous: bool,
}

/// Index storage parameters: each a name and an optional value.
pub type IndexStorage<'a, X> = Vec<(ObjectName<'a>, Option<Expr<'a, X>>)>;

#[derive(Debug, Clone, PartialEq)]
pub struct IndexColumn<'a, X: AstExt> {
    pub item: OrderByItem<'a, X>,
    pub operator_class: Option<ObjectName<'a>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateSchema<'a> {
    pub or_replace: bool,
    pub if_not_exists: bool,
    /// `None` in `CREATE SCHEMA AUTHORIZATION owner`.
    pub name: Option<ObjectName<'a>>,
    pub authorization: Option<Ident<'a>>,
    /// The schema in `CLONE source`.
    pub clone: Option<ObjectName<'a>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateDatabase<'a> {
    pub if_not_exists: bool,
    pub name: ObjectName<'a>,
}

/// The modifiers written between `CREATE` and the object type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CreateFlags {
    pub or_replace: bool,
    pub temporary: bool,
}

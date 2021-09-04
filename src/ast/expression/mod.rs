mod display;
use super::*;

/// An SQL expression of any type.
///
/// The parser does not distinguish between expressions of different types
/// (e.g. boolean vs string), so the caller must handle expressions of
/// inappropriate type, like `WHERE 1` or `SELECT 1=1`, as necessary.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum Expr {
    /// Identifier e.g. table name or column name
    Identifier(Ident),
    /// Unqualified wildcard (`*`). SQL allows this in limited contexts, such as:
    /// - right after `SELECT` (which is represented as a [SelectItem::Wildcard] instead)
    /// - or as part of an aggregate function, e.g. `COUNT(*)`,
    ///
    /// ...but we currently also accept it in contexts where it doesn't make
    /// sense, such as `* + *`
    Wildcard,
    /// Qualified wildcard, e.g. `alias.*` or `schema.table.*`.
    /// (Same caveats apply to `QualifiedWildcard` as to `Wildcard`.)
    QualifiedWildcard(Vec<Ident>),
    /// Multi-part identifier, e.g. `table_alias.column` or `schema.table.col`
    CompoundIdentifier(Vec<Ident>),
    /// `IS NULL` expression
    IsNull(Box<Expr>),
    /// `IS NOT NULL` expression
    IsNotNull(Box<Expr>),
    /// `[ NOT ] IN (val1, val2, ...)`
    InList(InList),
    /// `[ NOT ] IN (SELECT ...)`
    InSubquery(InSubquery),
    /// `<expr> [ NOT ] BETWEEN <low> AND <high>`
    Between(Between),
    /// Binary operation e.g. `1 + 1` or `foo > bar`
    BinaryOp(BinaryOp),
    /// Unary operation e.g. `NOT foo`
    UnaryOp(UnaryOp),
    /// CAST an expression to a different data type e.g. `CAST(foo AS VARCHAR(123))`
    Cast(Cast),
    /// TRY_CAST an expression to a different data type e.g. `TRY_CAST(foo AS VARCHAR(123))`
    //  this differs from CAST in the choice of how to implement invalid conversions
    TryCast(TryCast),
    /// EXTRACT(DateTimeField FROM <expr>)
    Extract(Extract),
    /// SUBSTRING(<expr> [FROM <expr>] [FOR <expr>])
    Substring(Substring),
    /// TRIM([BOTH | LEADING | TRAILING] <expr> [FROM <expr>])\
    /// Or\
    /// TRIM(<expr>)
    Trim(Trim),
    /// `expr COLLATE collation`
    Collate(Collate),
    /// Nested expression e.g. `(foo > bar)` or `(1)`
    Nested(Box<Expr>),
    /// A literal value, such as string, number, date or NULL
    Value(Value),
    /// A constant of form `<data_type> 'value'`.
    /// This can represent ANSI SQL `DATE`, `TIME`, and `TIMESTAMP` literals (such as `DATE '2020-01-01'`),
    /// as well as constants of other types (a non-standard PostgreSQL extension).
    TypedString(TypedString),
    MapAccess(MapAccess),
    /// Scalar function call e.g. `LEFT(foo, 5)`
    Function(Function),
    /// `CASE [<operand>] WHEN <condition> THEN <result> ... [ELSE <result>] END`
    ///
    /// Note we only recognize a complete single expression as `<condition>`,
    /// not `< 0` nor `1, 2, 3` as allowed in a `<simple when clause>` per
    /// <https://jakewheat.github.io/sql-overview/sql-2011-foundation-grammar.html#simple-when-clause>
    Case(Case),
    /// An exists expression `EXISTS(SELECT ...)`, used in expressions like
    /// `WHERE EXISTS (SELECT ...)`.
    Exists(Box<Query>),
    /// A parenthesized subquery `(SELECT ...)`, used in expression like
    /// `SELECT (subquery) AS x` or `WHERE (subquery) = x`
    Subquery(Box<Query>),
    /// The `LISTAGG` function `SELECT LISTAGG(...) WITHIN GROUP (ORDER BY ...)`
    ListAgg(ListAgg),
}

/// `CASE [<operand>] WHEN <condition> THEN <result> ... [ELSE <result>] END`
///
/// Note we only recognize a complete single expression as `<condition>`,
/// not `< 0` nor `1, 2, 3` as allowed in a `<simple when clause>` per
/// <https://jakewheat.github.io/sql-overview/sql-2011-foundation-grammar.html#simple-when-clause>
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct Case {
    pub operand: Option<Box<Expr>>,
    pub conditions: Vec<Expr>,
    pub results: Vec<Expr>,
    pub else_result: Option<Box<Expr>>,
}

/// `[ NOT ] IN (val1, val2, ...)`
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct InList {
    pub expr: Box<Expr>,
    pub list: Vec<Expr>,
    pub negated: bool,
}
/// `[ NOT ] IN (SELECT ...)`
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct InSubquery {
    pub expr: Box<Expr>,
    pub subquery: Box<Query>,
    pub negated: bool,
}
/// `<expr> [ NOT ] BETWEEN <low> AND <high>`
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct Between {
    pub expr: Box<Expr>,
    pub negated: bool,
    pub low: Box<Expr>,
    pub high: Box<Expr>,
}
/// Binary operation e.g. `1 + 1` or `foo > bar`
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct BinaryOp {
    pub left: Box<Expr>,
    pub op: BinaryOperator,
    pub right: Box<Expr>,
}
/// Unary operation e.g. `NOT foo`
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct UnaryOp {
    pub op: UnaryOperator,
    pub expr: Box<Expr>,
}
/// CAST an expression to a different data type e.g. `CAST(foo AS VARCHAR(123))`
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct Cast {
    pub expr: Box<Expr>,
    pub data_type: DataType,
}
/// TRY_CAST an expression to a different data type e.g. `TRY_CAST(foo AS VARCHAR(123))`
//  this differs from CAST in the choice of how to implement invalid conversions
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct TryCast {
    pub expr: Box<Expr>,
    pub data_type: DataType,
}
/// EXTRACT(DateTimeField FROM <expr>)
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct Extract {
    pub field: DateTimeField,
    pub expr: Box<Expr>,
}
/// SUBSTRING(<expr> [FROM <expr>] [FOR <expr>])
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct Substring {
    pub expr: Box<Expr>,
    pub substring_from: Option<Box<Expr>>,
    pub substring_for: Option<Box<Expr>>,
}
/// TRIM([BOTH | LEADING | TRAILING] <expr> [FROM <expr>])\
/// Or\
/// TRIM(<expr>)
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct Trim {
    pub expr: Box<Expr>,
    // ([BOTH | LEADING | TRAILING], <expr>)
    pub trim_where: Option<(TrimWhereField, Box<Expr>)>,
}
/// `expr COLLATE collation`
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct Collate {
    pub expr: Box<Expr>,
    pub collation: ObjectName,
}

/// A constant of form `<data_type> 'value'`.
/// This can represent ANSI SQL `DATE`, `TIME`, and `TIMESTAMP` literals (such as `DATE '2020-01-01'`),
/// as well as constants of other types (a non-standard PostgreSQL extension).
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct TypedString {
    pub data_type: DataType,
    pub value: String,
}
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct MapAccess {
    pub column: Box<Expr>,
    pub key: String,
}

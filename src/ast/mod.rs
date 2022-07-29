// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! SQL Abstract Syntax Tree (AST) types
mod data_type;
mod operator;
mod query;
mod value;

#[cfg(not(feature = "std"))]
use alloc::{
    boxed::Box,
    string::{String, ToString},
    vec::Vec,
};
use core::fmt;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use crate::dialect::{Dialect, DialectDisplay};

pub use self::data_type::DataType;
pub use self::operator::{BinaryOperator, UnaryOperator};
pub use self::query::{
    Cte, Fetch, Join, JoinConstraint, JoinOperator, LateralView, LockType, Offset, OffsetRows,
    OrderByExpr, Query, Select, SelectInto, SelectItem, SetExpr, SetOperator, TableAlias,
    TableFactor, TableWithJoins, Top, Values, With,
};
pub use self::value::{DateTimeField, TrimWhereField, Value};

struct DisplaySeparated<'a, T>
where
    T: DialectDisplay,
{
    slice: &'a [T],
    sep: &'static str,
}

impl<'a, T> DialectDisplay for DisplaySeparated<'a, T>
where
    T: DialectDisplay,
{
    fn fmt(&self, f: &mut (dyn fmt::Write), dialect: &Dialect) -> fmt::Result {
        let mut delim = "";
        for t in self.slice {
            write!(f, "{}", delim)?;
            delim = self.sep;
            write!(f, "{}", t.sql(dialect)?)?;
        }
        Ok(())
    }
}

fn display_separated<'a, T>(slice: &'a [T], sep: &'static str) -> DisplaySeparated<'a, T>
where
    T: DialectDisplay,
{
    DisplaySeparated { slice, sep }
}

fn display_comma_separated<T>(slice: &[T]) -> DisplaySeparated<'_, T>
where
    T: DialectDisplay,
{
    DisplaySeparated { slice, sep: ", " }
}

/// An identifier, decomposed into its value or character data and the quote style.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Ident {
    /// The value of the identifier without quotes.
    pub value: String,
    /// The starting quote if any. Valid quote characters are the single quote,
    /// double quote, backtick, and opening square bracket.
    pub quote_style: Option<char>,
}

impl Ident {
    /// Create a new identifier with the given value and no quotes.
    pub fn new<S>(value: S) -> Self
    where
        S: Into<String>,
    {
        Ident {
            value: value.into(),
            quote_style: None,
        }
    }

    /// Create a new quoted identifier with the given quote and value. This function
    /// panics if the given quote is not a valid quote character.
    pub fn with_quote<S>(quote: char, value: S) -> Self
    where
        S: Into<String>,
    {
        assert!(quote == '\'' || quote == '"' || quote == '`' || quote == '[');
        Ident {
            value: value.into(),
            quote_style: Some(quote),
        }
    }
}

impl From<&str> for Ident {
    fn from(value: &str) -> Self {
        Ident {
            value: value.to_string(),
            quote_style: None,
        }
    }
}

impl DialectDisplay for Ident {
    fn fmt(&self, f: &mut (dyn fmt::Write), dialect: &Dialect) -> fmt::Result {
        match dialect.quote_style {
            Some(q) if q == '"' || q == '\'' || q == '`' => {
                let escaped = value::escape_quoted_string(&self.value, q);
                write!(f, "{}{}{}", q, escaped.sql(dialect)?, q)
            }
            Some(q) if q == '[' => write!(f, "[{}]", self.value),
            None => f.write_str(&self.value),
            _ => panic!("unexpected quote style"),
        }
    }
}

/// A name of a table, view, custom type, etc., possibly multi-part, i.e. db.schema.obj
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct ObjectName(pub Vec<Ident>);

impl DialectDisplay for ObjectName {
    fn fmt(&self, f: &mut (dyn fmt::Write), dialect: &Dialect) -> fmt::Result {
        write!(f, "{}", display_separated(&self.0, ".").sql(dialect)?)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
/// Represents an Array Expression, either
/// `ARRAY[..]`, or `[..]`
pub struct Array {
    /// The list of expressions between brackets
    pub elem: Vec<Expr>,

    /// `true` for  `ARRAY[..]`, `false` for `[..]`
    pub named: bool,
}

impl DialectDisplay for Array {
    fn fmt(&self, f: &mut (dyn fmt::Write), dialect: &Dialect) -> fmt::Result {
        write!(
            f,
            "{}[{}]",
            if self.named { "ARRAY" } else { "" },
            display_comma_separated(&self.elem).sql(dialect)?
        )
    }
}

/// JsonOperator
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum JsonOperator {
    /// -> keeps the value as json
    Arrow,
    /// ->> keeps the value as text or int.
    LongArrow,
    /// #> Extracts JSON sub-object at the specified path
    HashArrow,
    /// #>> Extracts JSON sub-object at the specified path as text
    HashLongArrow,
}

impl DialectDisplay for JsonOperator {
    fn fmt(&self, f: &mut (dyn fmt::Write), _dialect: &Dialect) -> fmt::Result {
        match self {
            JsonOperator::Arrow => {
                write!(f, "->")
            }
            JsonOperator::LongArrow => {
                write!(f, "->>")
            }
            JsonOperator::HashArrow => {
                write!(f, "#>")
            }
            JsonOperator::HashLongArrow => {
                write!(f, "#>>")
            }
        }
    }
}

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
    /// Multi-part identifier, e.g. `table_alias.column` or `schema.table.col`
    CompoundIdentifier(Vec<Ident>),
    /// JSON access (postgres)  eg: data->'tags'
    JsonAccess {
        left: Box<Expr>,
        operator: JsonOperator,
        right: Box<Expr>,
    },
    /// CompositeAccess (postgres) eg: SELECT (information_schema._pg_expandarray(array['i','i'])).n
    CompositeAccess { expr: Box<Expr>, key: Ident },
    /// `IS FALSE` operator
    IsFalse(Box<Expr>),
    /// `IS TRUE` operator
    IsTrue(Box<Expr>),
    /// `IS NULL` operator
    IsNull(Box<Expr>),
    /// `IS NOT NULL` operator
    IsNotNull(Box<Expr>),
    /// `IS DISTINCT FROM` operator
    IsDistinctFrom(Box<Expr>, Box<Expr>),
    /// `IS NOT DISTINCT FROM` operator
    IsNotDistinctFrom(Box<Expr>, Box<Expr>),
    /// `[ NOT ] IN (val1, val2, ...)`
    InList {
        expr: Box<Expr>,
        list: Vec<Expr>,
        negated: bool,
    },
    /// `[ NOT ] IN (SELECT ...)`
    InSubquery {
        expr: Box<Expr>,
        subquery: Box<Query>,
        negated: bool,
    },
    /// `[ NOT ] IN UNNEST(array_expression)`
    InUnnest {
        expr: Box<Expr>,
        array_expr: Box<Expr>,
        negated: bool,
    },
    /// `<expr> [ NOT ] BETWEEN <low> AND <high>`
    Between {
        expr: Box<Expr>,
        negated: bool,
        low: Box<Expr>,
        high: Box<Expr>,
    },
    /// Binary operation e.g. `1 + 1` or `foo > bar`
    BinaryOp {
        left: Box<Expr>,
        op: BinaryOperator,
        right: Box<Expr>,
    },
    /// Any operation e.g. `1 ANY (1)` or `foo > ANY(bar)`, It will be wrapped in the right side of BinaryExpr
    AnyOp(Box<Expr>),
    /// ALL operation e.g. `1 ALL (1)` or `foo > ALL(bar)`, It will be wrapped in the right side of BinaryExpr
    AllOp(Box<Expr>),
    /// Unary operation e.g. `NOT foo`
    UnaryOp { op: UnaryOperator, expr: Box<Expr> },
    /// CAST an expression to a different data type e.g. `CAST(foo AS VARCHAR(123))`
    Cast {
        expr: Box<Expr>,
        data_type: DataType,
    },
    /// TRY_CAST an expression to a different data type e.g. `TRY_CAST(foo AS VARCHAR(123))`
    //  this differs from CAST in the choice of how to implement invalid conversions
    TryCast {
        expr: Box<Expr>,
        data_type: DataType,
    },
    /// AT a timestamp to a different timezone e.g. `FROM_UNIXTIME(0) AT TIME ZONE 'UTC-06:00'`
    AtTimeZone {
        timestamp: Box<Expr>,
        time_zone: String,
    },
    /// EXTRACT(DateTimeField FROM <expr>)
    Extract {
        field: DateTimeField,
        expr: Box<Expr>,
    },
    /// POSITION(<expr> in <expr>)
    Position { expr: Box<Expr>, r#in: Box<Expr> },
    /// SUBSTRING(<expr> [FROM <expr>] [FOR <expr>])
    Substring {
        expr: Box<Expr>,
        substring_from: Option<Box<Expr>>,
        substring_for: Option<Box<Expr>>,
    },
    /// TRIM([BOTH | LEADING | TRAILING] <expr> [FROM <expr>])\
    /// Or\
    /// TRIM(<expr>)
    Trim {
        expr: Box<Expr>,
        // ([BOTH | LEADING | TRAILING], <expr>)
        trim_where: Option<(TrimWhereField, Box<Expr>)>,
    },
    /// `expr COLLATE collation`
    Collate {
        expr: Box<Expr>,
        collation: ObjectName,
    },
    /// Nested expression e.g. `(foo > bar)` or `(1)`
    Nested(Box<Expr>),
    /// A literal value, such as string, number, date or NULL
    Value(Value),
    /// A constant of form `<data_type> 'value'`.
    /// This can represent ANSI SQL `DATE`, `TIME`, and `TIMESTAMP` literals (such as `DATE '2020-01-01'`),
    /// as well as constants of other types (a non-standard PostgreSQL extension).
    TypedString { data_type: DataType, value: String },
    /// Access a map-like object by field (e.g. `column['field']` or `column[4]`
    /// Note that depending on the dialect, struct like accesses may be
    /// parsed as [`ArrayIndex`] or [`MapAccess`]
    /// <https://clickhouse.com/docs/en/sql-reference/data-types/map/>
    MapAccess { column: Box<Expr>, keys: Vec<Expr> },
    /// Scalar function call e.g. `LEFT(foo, 5)`
    Function(Function),
    /// `CASE [<operand>] WHEN <condition> THEN <result> ... [ELSE <result>] END`
    ///
    /// Note we only recognize a complete single expression as `<condition>`,
    /// not `< 0` nor `1, 2, 3` as allowed in a `<simple when clause>` per
    /// <https://jakewheat.github.io/sql-overview/sql-2011-foundation-grammar.html#simple-when-clause>
    Case {
        operand: Option<Box<Expr>>,
        conditions: Vec<Expr>,
        results: Vec<Expr>,
        else_result: Option<Box<Expr>>,
    },
    /// An exists expression `[ NOT ] EXISTS(SELECT ...)`, used in expressions like
    /// `WHERE [ NOT ] EXISTS (SELECT ...)`.
    Exists { subquery: Box<Query>, negated: bool },
    /// A parenthesized subquery `(SELECT ...)`, used in expression like
    /// `SELECT (subquery) AS x` or `WHERE (subquery) = x`
    Subquery(Box<Query>),
    /// The `LISTAGG` function `SELECT LISTAGG(...) WITHIN GROUP (ORDER BY ...)`
    ListAgg(ListAgg),
    /// The `GROUPING SETS` expr.
    GroupingSets(Vec<Vec<Expr>>),
    /// The `CUBE` expr.
    Cube(Vec<Vec<Expr>>),
    /// The `ROLLUP` expr.
    Rollup(Vec<Vec<Expr>>),
    /// ROW / TUPLE a single value, such as `SELECT (1, 2)`
    Tuple(Vec<Expr>),
    /// An array index expression e.g. `(ARRAY[1, 2])[1]` or `(current_schemas(FALSE))[1]`
    ArrayIndex { obj: Box<Expr>, indexes: Vec<Expr> },
    /// An array expression e.g. `ARRAY[1, 2]`
    Array(Array),
}

impl DialectDisplay for Expr {
    fn fmt(&self, f: &mut (dyn fmt::Write), dialect: &Dialect) -> fmt::Result {
        match self {
            Expr::Identifier(s) => write!(f, "{}", s.sql(dialect)?),
            Expr::MapAccess { column, keys } => {
                write!(f, "{}", column.sql(dialect)?)?;
                for k in keys {
                    match k {
                        k @ Expr::Value(Value::Number(_, _)) => write!(f, "[{}]", k.sql(dialect)?)?,
                        Expr::Value(Value::SingleQuotedString(s)) => write!(f, "[\"{}\"]", s)?,
                        _ => write!(f, "[{}]", k.sql(dialect)?)?,
                    }
                }
                Ok(())
            }
            Expr::CompoundIdentifier(s) => write!(f, "{}", display_separated(s, ".").sql(dialect)?),
            Expr::IsTrue(ast) => write!(f, "{} IS TRUE", ast.sql(dialect)?),
            Expr::IsFalse(ast) => write!(f, "{} IS FALSE", ast.sql(dialect)?),
            Expr::IsNull(ast) => write!(f, "{} IS NULL", ast.sql(dialect)?),
            Expr::IsNotNull(ast) => write!(f, "{} IS NOT NULL", ast.sql(dialect)?),
            Expr::InList {
                expr,
                list,
                negated,
            } => write!(
                f,
                "{} {}IN ({})",
                expr.sql(dialect)?,
                if *negated { "NOT " } else { "" },
                display_comma_separated(list).sql(dialect)?
            ),
            Expr::InSubquery {
                expr,
                subquery,
                negated,
            } => write!(
                f,
                "{} {}IN ({})",
                expr.sql(dialect)?,
                if *negated { "NOT " } else { "" },
                subquery.sql(dialect)?
            ),
            Expr::InUnnest {
                expr,
                array_expr,
                negated,
            } => write!(
                f,
                "{} {}IN UNNEST({})",
                expr.sql(dialect)?,
                if *negated { "NOT " } else { "" },
                array_expr.sql(dialect)?
            ),
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => write!(
                f,
                "{} {}BETWEEN {} AND {}",
                expr.sql(dialect)?,
                if *negated { "NOT " } else { "" },
                low.sql(dialect)?,
                high.sql(dialect)?
            ),
            Expr::BinaryOp { left, op, right } => write!(f, "{} {} {}", left.sql(dialect)?, op.sql(dialect)?, right.sql(dialect)?),
            Expr::AnyOp(expr) => write!(f, "ANY({})", expr.sql(dialect)?),
            Expr::AllOp(expr) => write!(f, "ALL({})", expr.sql(dialect)?),
            Expr::UnaryOp { op, expr } => {
                if op == &UnaryOperator::PGPostfixFactorial {
                    write!(f, "{}{}", expr.sql(dialect)?, op.sql(dialect)?)
                } else {
                    write!(f, "{} {}", op.sql(dialect)?, expr.sql(dialect)?)
                }
            }
            Expr::Cast { expr, data_type } => write!(f, "CAST({} AS {})", expr.sql(dialect)?, data_type.sql(dialect)?),
            Expr::TryCast { expr, data_type } => write!(f, "TRY_CAST({} AS {})", expr.sql(dialect)?, data_type.sql(dialect)?),
            Expr::Extract { field, expr } => write!(f, "EXTRACT({} FROM {})", field.sql(dialect)?, expr.sql(dialect)?),
            Expr::Position { expr, r#in } => write!(f, "POSITION({} IN {})", expr.sql(dialect)?, r#in.sql(dialect)?),
            Expr::Collate { expr, collation } => write!(f, "{} COLLATE {}", expr.sql(dialect)?, collation.sql(dialect)?),
            Expr::Nested(ast) => write!(f, "({})", ast.sql(dialect)?),
            Expr::Value(v) => write!(f, "{}", v.sql(dialect)?),
            Expr::TypedString { data_type, value } => {
                write!(f, "{}", data_type.sql(dialect)?)?;
                write!(f, " '{}'", &value::escape_single_quote_string(value).sql(dialect)?)
            }
            Expr::Function(fun) => write!(f, "{}", fun.sql(dialect)?),
            Expr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => {
                write!(f, "CASE")?;
                if let Some(operand) = operand {
                    write!(f, " {}", operand.sql(dialect)?)?;
                }
                for (c, r) in conditions.iter().zip(results) {
                    write!(f, " WHEN {} THEN {}", c.sql(dialect)?, r.sql(dialect)?)?;
                }

                if let Some(else_result) = else_result {
                    write!(f, " ELSE {}", else_result.sql(dialect)?)?;
                }
                write!(f, " END")
            }
            Expr::Exists { subquery, negated } => write!(
                f,
                "{}EXISTS ({})",
                if *negated { "NOT " } else { "" },
                subquery.sql(dialect)?
            ),
            Expr::Subquery(s) => write!(f, "({})", s.sql(dialect)?),
            Expr::ListAgg(listagg) => write!(f, "{}", listagg.sql(dialect)?),
            Expr::GroupingSets(sets) => {
                write!(f, "GROUPING SETS (")?;
                let mut sep = "";
                for set in sets {
                    write!(f, "{}", sep)?;
                    sep = ", ";
                    write!(f, "({})", display_comma_separated(set).sql(dialect)?)?;
                }
                write!(f, ")")
            }
            Expr::Cube(sets) => {
                write!(f, "CUBE (")?;
                let mut sep = "";
                for set in sets {
                    write!(f, "{}", sep)?;
                    sep = ", ";
                    if set.len() == 1 {
                        write!(f, "{}", set[0].sql(dialect)?)?;
                    } else {
                        write!(f, "({})", display_comma_separated(set).sql(dialect)?)?;
                    }
                }
                write!(f, ")")
            }
            Expr::Rollup(sets) => {
                write!(f, "ROLLUP (")?;
                let mut sep = "";
                for set in sets {
                    write!(f, "{}", sep)?;
                    sep = ", ";
                    if set.len() == 1 {
                        write!(f, "{}", set[0].sql(dialect)?)?;
                    } else {
                        write!(f, "({})", display_comma_separated(set).sql(dialect)?)?;
                    }
                }
                write!(f, ")")
            }
            Expr::Substring {
                expr,
                substring_from,
                substring_for,
            } => {
                write!(f, "SUBSTRING({}", expr.sql(dialect)?)?;
                if let Some(from_part) = substring_from {
                    write!(f, " FROM {}", from_part.sql(dialect)?)?;
                }
                if let Some(from_part) = substring_for {
                    write!(f, " FOR {}", from_part.sql(dialect)?)?;
                }

                write!(f, ")")
            }
            Expr::IsDistinctFrom(a, b) => write!(f, "{} IS DISTINCT FROM {}", a.sql(dialect)?, b.sql(dialect)?),
            Expr::IsNotDistinctFrom(a, b) => write!(f, "{} IS NOT DISTINCT FROM {}", a.sql(dialect)?, b.sql(dialect)?),
            Expr::Trim { expr, trim_where } => {
                write!(f, "TRIM(")?;
                if let Some((ident, trim_char)) = trim_where {
                    write!(f, "{} {} FROM {}", ident.sql(dialect)?, trim_char.sql(dialect)?, expr.sql(dialect)?)?;
                } else {
                    write!(f, "{}", expr.sql(dialect)?)?;
                }

                write!(f, ")")
            }
            Expr::Tuple(exprs) => {
                write!(f, "({})", display_comma_separated(exprs).sql(dialect)?)
            }
            Expr::ArrayIndex { obj, indexes } => {
                write!(f, "{}", obj.sql(dialect)?)?;
                for i in indexes {
                    write!(f, "[{}]", i.sql(dialect)?)?;
                }
                Ok(())
            }
            Expr::Array(set) => {
                write!(f, "{}", set.sql(dialect)?)
            }
            Expr::JsonAccess {
                left,
                operator,
                right,
            } => {
                write!(f, "{} {} {}", left.sql(dialect)?, operator.sql(dialect)?, right.sql(dialect)?)
            }
            Expr::CompositeAccess { expr, key } => {
                write!(f, "{}.{}", expr.sql(dialect)?, key.sql(dialect)?)
            }
            Expr::AtTimeZone {
                timestamp,
                time_zone,
            } => {
                write!(f, "{} AT TIME ZONE '{}'", timestamp.sql(dialect)?, time_zone)
            }
        }
    }
}

/// A window specification (i.e. `OVER (PARTITION BY .. ORDER BY .. etc.)`)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct WindowSpec {
    pub partition_by: Vec<Expr>,
    pub order_by: Vec<OrderByExpr>,
    pub window_frame: Option<WindowFrame>,
}

impl DialectDisplay for WindowSpec {
    fn fmt(&self, f: &mut (dyn fmt::Write), dialect: &Dialect) -> fmt::Result {
        let mut delim = "";
        if !self.partition_by.is_empty() {
            delim = " ";
            write!(
                f,
                "PARTITION BY {}",
                display_comma_separated(&self.partition_by).sql(dialect)?
            )?;
        }
        if !self.order_by.is_empty() {
            f.write_str(delim)?;
            delim = " ";
            write!(f, "ORDER BY {}", display_comma_separated(&self.order_by).sql(dialect)?)?;
        }
        if let Some(window_frame) = &self.window_frame {
            f.write_str(delim)?;
            if let Some(end_bound) = &window_frame.end_bound {
                write!(
                    f,
                    "{} BETWEEN {} AND {}",
                    window_frame.units.sql(dialect)?, window_frame.start_bound.sql(dialect)?, end_bound.sql(dialect)?
                )?;
            } else {
                write!(f, "{} {}", window_frame.units.sql(dialect)?, window_frame.start_bound.sql(dialect)?)?;
            }
        }
        Ok(())
    }
}

/// Specifies the data processed by a window function, e.g.
/// `RANGE UNBOUNDED PRECEDING` or `ROWS BETWEEN 5 PRECEDING AND CURRENT ROW`.
///
/// Note: The parser does not validate the specified bounds; the caller should
/// reject invalid bounds like `ROWS UNBOUNDED FOLLOWING` before execution.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct WindowFrame {
    pub units: WindowFrameUnits,
    pub start_bound: WindowFrameBound,
    /// The right bound of the `BETWEEN .. AND` clause. The end bound of `None`
    /// indicates the shorthand form (e.g. `ROWS 1 PRECEDING`), which must
    /// behave the same as `end_bound = WindowFrameBound::CurrentRow`.
    pub end_bound: Option<WindowFrameBound>,
    // TBD: EXCLUDE
}

impl Default for WindowFrame {
    /// returns default value for window frame
    ///
    /// see https://www.sqlite.org/windowfunctions.html#frame_specifications
    fn default() -> Self {
        Self {
            units: WindowFrameUnits::Range,
            start_bound: WindowFrameBound::Preceding(None),
            end_bound: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum WindowFrameUnits {
    Rows,
    Range,
    Groups,
}

impl DialectDisplay for WindowFrameUnits {
    fn fmt(&self, f: &mut (dyn fmt::Write), _dialect: &Dialect) -> fmt::Result {
        f.write_str(match self {
            WindowFrameUnits::Rows => "ROWS",
            WindowFrameUnits::Range => "RANGE",
            WindowFrameUnits::Groups => "GROUPS",
        })
    }
}

/// Specifies [WindowFrame]'s `start_bound` and `end_bound`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum WindowFrameBound {
    /// `CURRENT ROW`
    CurrentRow,
    /// `<N> PRECEDING` or `UNBOUNDED PRECEDING`
    Preceding(Option<u64>),
    /// `<N> FOLLOWING` or `UNBOUNDED FOLLOWING`.
    Following(Option<u64>),
}

impl DialectDisplay for WindowFrameBound {
    fn fmt(&self, f: &mut (dyn fmt::Write), _dialect: &Dialect) -> fmt::Result {
        match self {
            WindowFrameBound::CurrentRow => f.write_str("CURRENT ROW"),
            WindowFrameBound::Preceding(None) => f.write_str("UNBOUNDED PRECEDING"),
            WindowFrameBound::Following(None) => f.write_str("UNBOUNDED FOLLOWING"),
            WindowFrameBound::Preceding(Some(n)) => write!(f, "{} PRECEDING", n),
            WindowFrameBound::Following(Some(n)) => write!(f, "{} FOLLOWING", n),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum AddDropSync {
    ADD,
    DROP,
    SYNC,
}

impl DialectDisplay for AddDropSync {
    fn fmt(&self, f: &mut (dyn fmt::Write), _dialect: &Dialect) -> fmt::Result {
        match self {
            AddDropSync::SYNC => f.write_str("SYNC PARTITIONS"),
            AddDropSync::DROP => f.write_str("DROP PARTITIONS"),
            AddDropSync::ADD => f.write_str("ADD PARTITIONS"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum ShowCreateObject {
    Event,
    Function,
    Procedure,
    Table,
    Trigger,
    View,
}

impl DialectDisplay for ShowCreateObject {
    fn fmt(&self, f: &mut (dyn fmt::Write), _dialect: &Dialect) -> fmt::Result {
        match self {
            ShowCreateObject::Event => f.write_str("EVENT"),
            ShowCreateObject::Function => f.write_str("FUNCTION"),
            ShowCreateObject::Procedure => f.write_str("PROCEDURE"),
            ShowCreateObject::Table => f.write_str("TABLE"),
            ShowCreateObject::Trigger => f.write_str("TRIGGER"),
            ShowCreateObject::View => f.write_str("VIEW"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum CommentObject {
    Column,
    Table,
}

impl DialectDisplay for CommentObject {
    fn fmt(&self, f: &mut (dyn fmt::Write), _dialect: &Dialect) -> fmt::Result {
        match self {
            CommentObject::Column => f.write_str("COLUMN"),
            CommentObject::Table => f.write_str("TABLE"),
        }
    }
}

/// Specific direction for FETCH statement
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum FetchDirection {
    Count { limit: Value },
    Next,
    Prior,
    First,
    Last,
    Absolute { limit: Value },
    Relative { limit: Value },
    All,
    // FORWARD
    // FORWARD count
    Forward { limit: Option<Value> },
    ForwardAll,
    // BACKWARD
    // BACKWARD count
    Backward { limit: Option<Value> },
    BackwardAll,
}

impl DialectDisplay for FetchDirection {
    fn fmt(&self, f: &mut (dyn fmt::Write), dialect: &Dialect) -> fmt::Result {
        match self {
            FetchDirection::Count { limit } => f.write_str(&limit.sql(dialect)?)?,
            FetchDirection::Next => f.write_str("NEXT")?,
            FetchDirection::Prior => f.write_str("PRIOR")?,
            FetchDirection::First => f.write_str("FIRST")?,
            FetchDirection::Last => f.write_str("LAST")?,
            FetchDirection::Absolute { limit } => {
                f.write_str("ABSOLUTE ")?;
                f.write_str(&limit.sql(dialect)?)?;
            }
            FetchDirection::Relative { limit } => {
                f.write_str("RELATIVE ")?;
                f.write_str(&limit.sql(dialect)?)?;
            }
            FetchDirection::All => f.write_str("ALL")?,
            FetchDirection::Forward { limit } => {
                f.write_str("FORWARD")?;

                if let Some(l) = limit {
                    f.write_str(" ")?;
                    f.write_str(&l.sql(dialect)?)?;
                }
            }
            FetchDirection::ForwardAll => f.write_str("FORWARD ALL")?,
            FetchDirection::Backward { limit } => {
                f.write_str("BACKWARD")?;

                if let Some(l) = limit {
                    f.write_str(" ")?;
                    f.write_str(&l.sql(dialect)?)?;
                }
            }
            FetchDirection::BackwardAll => f.write_str("BACKWARD ALL")?,
        };

        Ok(())
    }
}

/// A privilege on a database object (table, sequence, etc.).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum Action {
    Connect,
    Create,
    Delete,
    Execute,
    Insert { columns: Option<Vec<Ident>> },
    References { columns: Option<Vec<Ident>> },
    Select { columns: Option<Vec<Ident>> },
    Temporary,
    Trigger,
    Truncate,
    Update { columns: Option<Vec<Ident>> },
    Usage,
}

impl DialectDisplay for Action {
    fn fmt(&self, f: &mut (dyn fmt::Write), dialect: &Dialect) -> fmt::Result {
        match self {
            Action::Connect => f.write_str("CONNECT")?,
            Action::Create => f.write_str("CREATE")?,
            Action::Delete => f.write_str("DELETE")?,
            Action::Execute => f.write_str("EXECUTE")?,
            Action::Insert { .. } => f.write_str("INSERT")?,
            Action::References { .. } => f.write_str("REFERENCES")?,
            Action::Select { .. } => f.write_str("SELECT")?,
            Action::Temporary => f.write_str("TEMPORARY")?,
            Action::Trigger => f.write_str("TRIGGER")?,
            Action::Truncate => f.write_str("TRUNCATE")?,
            Action::Update { .. } => f.write_str("UPDATE")?,
            Action::Usage => f.write_str("USAGE")?,
        };
        match self {
            Action::Insert { columns }
            | Action::References { columns }
            | Action::Select { columns }
            | Action::Update { columns } => {
                if let Some(columns) = columns {
                    write!(f, " ({})", display_comma_separated(columns).sql(dialect)?)?;
                }
            }
            _ => (),
        };
        Ok(())
    }
}

/// Objects on which privileges are granted in a GRANT statement.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum GrantObjects {
    /// Grant privileges on `ALL SEQUENCES IN SCHEMA <schema_name> [, ...]`
    AllSequencesInSchema { schemas: Vec<ObjectName> },
    /// Grant privileges on `ALL TABLES IN SCHEMA <schema_name> [, ...]`
    AllTablesInSchema { schemas: Vec<ObjectName> },
    /// Grant privileges on specific schemas
    Schemas(Vec<ObjectName>),
    /// Grant privileges on specific sequences
    Sequences(Vec<ObjectName>),
    /// Grant privileges on specific tables
    Tables(Vec<ObjectName>),
}

impl DialectDisplay for GrantObjects {
    fn fmt(&self, f: &mut (dyn fmt::Write), dialect: &Dialect) -> fmt::Result {
        match self {
            GrantObjects::Sequences(sequences) => {
                write!(f, "SEQUENCE {}", display_comma_separated(sequences).sql(dialect)?)
            }
            GrantObjects::Schemas(schemas) => {
                write!(f, "SCHEMA {}", display_comma_separated(schemas).sql(dialect)?)
            }
            GrantObjects::Tables(tables) => {
                write!(f, "{}", display_comma_separated(tables).sql(dialect)?)
            }
            GrantObjects::AllSequencesInSchema { schemas } => {
                write!(
                    f,
                    "ALL SEQUENCES IN SCHEMA {}",
                    display_comma_separated(schemas).sql(dialect)?
                )
            }
            GrantObjects::AllTablesInSchema { schemas } => {
                write!(
                    f,
                    "ALL TABLES IN SCHEMA {}",
                    display_comma_separated(schemas).sql(dialect)?
                )
            }
        }
    }
}

/// SQL assignment `foo = expr` as used in SQLUpdate
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Assignment {
    pub id: Vec<Ident>,
    pub value: Expr,
}

impl DialectDisplay for Assignment {
    fn fmt(&self, f: &mut (dyn fmt::Write), dialect: &Dialect) -> fmt::Result {
        write!(f, "{} = {}", display_separated(&self.id, ".").sql(dialect)?, self.value.sql(dialect)?)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum FunctionArgExpr {
    Expr(Expr),
    /// Qualified wildcard, e.g. `alias.*` or `schema.table.*`.
    QualifiedWildcard(ObjectName),
    /// An unqualified `*`
    Wildcard,
}

impl DialectDisplay for FunctionArgExpr {
    fn fmt(&self, f: &mut (dyn fmt::Write), dialect: &Dialect) -> fmt::Result {
        match self {
            FunctionArgExpr::Expr(expr) => write!(f, "{}", expr.sql(dialect)?),
            FunctionArgExpr::QualifiedWildcard(prefix) => write!(f, "{}.*", prefix.sql(dialect)?),
            FunctionArgExpr::Wildcard => f.write_str("*"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum FunctionArg {
    Named { name: Ident, arg: FunctionArgExpr },
    Unnamed(FunctionArgExpr),
}

impl DialectDisplay for FunctionArg {
    fn fmt(&self, f: &mut (dyn fmt::Write), dialect: &Dialect) -> fmt::Result {
        match self {
            FunctionArg::Named { name, arg } => write!(f, "{} => {}", name.sql(dialect)?, arg.sql(dialect)?),
            FunctionArg::Unnamed(unnamed_arg) => write!(f, "{}", unnamed_arg.sql(dialect)?),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum CloseCursor {
    All,
    Specific { name: Ident },
}

impl DialectDisplay for CloseCursor {
    fn fmt(&self, f: &mut (dyn fmt::Write), dialect: &Dialect) -> fmt::Result {
        match self {
            CloseCursor::All => write!(f, "ALL"),
            CloseCursor::Specific { name } => write!(f, "{}", name.sql(dialect)?),
        }
    }
}

/// A function call
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Function {
    pub name: ObjectName,
    pub args: Vec<FunctionArg>,
    pub over: Option<WindowSpec>,
    // aggregate functions may specify eg `COUNT(DISTINCT x)`
    pub distinct: bool,
}

impl DialectDisplay for Function {
    fn fmt(&self, f: &mut (dyn fmt::Write), dialect: &Dialect) -> fmt::Result {
        write!(
            f,
            "{}({}{})",
            self.name.sql(dialect)?,
            if self.distinct { "DISTINCT " } else { "" },
            display_comma_separated(&self.args).sql(dialect)?,
        )?;
        if let Some(o) = &self.over {
            write!(f, " OVER ({})", o.sql(dialect)?)?;
        }
        Ok(())
    }
}

/// External table's available file format
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum FileFormat {
    TEXTFILE,
    SEQUENCEFILE,
    ORC,
    PARQUET,
    AVRO,
    RCFILE,
    JSONFILE,
}

impl DialectDisplay for FileFormat {
    fn fmt(&self, f: &mut (dyn fmt::Write), _dialect: &Dialect) -> fmt::Result {
        use self::FileFormat::*;
        f.write_str(match self {
            TEXTFILE => "TEXTFILE",
            SEQUENCEFILE => "SEQUENCEFILE",
            ORC => "ORC",
            PARQUET => "PARQUET",
            AVRO => "AVRO",
            RCFILE => "RCFILE",
            JSONFILE => "JSONFILE",
        })
    }
}

/// A `LISTAGG` invocation `LISTAGG( [ DISTINCT ] <expr>[, <separator> ] [ON OVERFLOW <on_overflow>] ) )
/// [ WITHIN GROUP (ORDER BY <within_group1>[, ...] ) ]`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct ListAgg {
    pub distinct: bool,
    pub expr: Box<Expr>,
    pub separator: Option<Box<Expr>>,
    pub on_overflow: Option<ListAggOnOverflow>,
    pub within_group: Vec<OrderByExpr>,
}

impl DialectDisplay for ListAgg {
    fn fmt(&self, f: &mut (dyn fmt::Write), dialect: &Dialect) -> fmt::Result {
        write!(
            f,
            "LISTAGG({}{}",
            if self.distinct { "DISTINCT " } else { "" },
            self.expr.sql(dialect)?
        )?;
        if let Some(separator) = &self.separator {
            write!(f, ", {}", separator.sql(dialect)?)?;
        }
        if let Some(on_overflow) = &self.on_overflow {
            write!(f, "{}", on_overflow.sql(dialect)?)?;
        }
        write!(f, ")")?;
        if !self.within_group.is_empty() {
            write!(
                f,
                " WITHIN GROUP (ORDER BY {})",
                display_comma_separated(&self.within_group).sql(dialect)?
            )?;
        }
        Ok(())
    }
}

/// The `ON OVERFLOW` clause of a LISTAGG invocation
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum ListAggOnOverflow {
    /// `ON OVERFLOW ERROR`
    Error,

    /// `ON OVERFLOW TRUNCATE [ <filler> ] WITH[OUT] COUNT`
    Truncate {
        filler: Option<Box<Expr>>,
        with_count: bool,
    },
}

impl DialectDisplay for ListAggOnOverflow {
    fn fmt(&self, f: &mut (dyn fmt::Write), dialect: &Dialect) -> fmt::Result {
        write!(f, " ON OVERFLOW")?;
        match self {
            ListAggOnOverflow::Error => write!(f, " ERROR"),
            ListAggOnOverflow::Truncate { filler, with_count } => {
                write!(f, " TRUNCATE")?;
                if let Some(filler) = filler {
                    write!(f, " {}", filler.sql(dialect)?)?;
                }
                if *with_count {
                    write!(f, " WITH")?;
                } else {
                    write!(f, " WITHOUT")?;
                }
                write!(f, " COUNT")
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum ObjectType {
    Table,
    View,
    Index,
    Schema,
}

impl DialectDisplay for ObjectType {
    fn fmt(&self, f: &mut (dyn fmt::Write), _dialect: &Dialect) -> fmt::Result {
        f.write_str(match self {
            ObjectType::Table => "TABLE",
            ObjectType::View => "VIEW",
            ObjectType::Index => "INDEX",
            ObjectType::Schema => "SCHEMA",
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum KillType {
    Connection,
    Query,
    Mutation,
}

impl DialectDisplay for KillType {
    fn fmt(&self, f: &mut (dyn fmt::Write), _dialect: &Dialect) -> fmt::Result {
        f.write_str(match self {
            // MySQL
            KillType::Connection => "CONNECTION",
            KillType::Query => "QUERY",
            // Clickhouse supports Mutation
            KillType::Mutation => "MUTATION",
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct SqlOption {
    pub name: Ident,
    pub value: Value,
}

impl DialectDisplay for SqlOption {
    fn fmt(&self, f: &mut (dyn fmt::Write), dialect: &Dialect) -> fmt::Result {
        write!(f, "{} = {}", self.name.sql(dialect)?, self.value.sql(dialect)?)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum TransactionMode {
    AccessMode(TransactionAccessMode),
    IsolationLevel(TransactionIsolationLevel),
}

impl DialectDisplay for TransactionMode {
    fn fmt(&self, f: &mut (dyn fmt::Write), dialect: &Dialect) -> fmt::Result {
        use TransactionMode::*;
        match self {
            AccessMode(access_mode) => write!(f, "{}", access_mode.sql(dialect)?),
            IsolationLevel(iso_level) => write!(f, "ISOLATION LEVEL {}", iso_level.sql(dialect)?),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum TransactionAccessMode {
    ReadOnly,
    ReadWrite,
}

impl DialectDisplay for TransactionAccessMode {
    fn fmt(&self, f: &mut (dyn fmt::Write), _dialect: &Dialect) -> fmt::Result {
        use TransactionAccessMode::*;
        f.write_str(match self {
            ReadOnly => "READ ONLY",
            ReadWrite => "READ WRITE",
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum TransactionIsolationLevel {
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

impl DialectDisplay for TransactionIsolationLevel {
    fn fmt(&self, f: &mut (dyn fmt::Write), _dialect: &Dialect) -> fmt::Result {
        use TransactionIsolationLevel::*;
        f.write_str(match self {
            ReadUncommitted => "READ UNCOMMITTED",
            ReadCommitted => "READ COMMITTED",
            RepeatableRead => "REPEATABLE READ",
            Serializable => "SERIALIZABLE",
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum ShowStatementFilter {
    Like(String),
    ILike(String),
    Where(Expr),
}

impl DialectDisplay for ShowStatementFilter {
    fn fmt(&self, f: &mut (dyn fmt::Write), dialect: &Dialect) -> fmt::Result {
        use ShowStatementFilter::*;
        match self {
            Like(pattern) => write!(f, "LIKE '{}'", value::escape_single_quote_string(pattern).sql(dialect)?),
            ILike(pattern) => write!(f, "ILIKE {}", value::escape_single_quote_string(pattern).sql(dialect)?),
            Where(expr) => write!(f, "WHERE {}", expr.sql(dialect)?),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum SetVariableValue {
    Ident(Ident),
    Literal(Value),
}

impl DialectDisplay for SetVariableValue {
    fn fmt(&self, f: &mut (dyn fmt::Write), dialect: &Dialect) -> fmt::Result {
        use SetVariableValue::*;
        match self {
            Ident(ident) => write!(f, "{}", ident.sql(dialect)?),
            Literal(literal) => write!(f, "{}", literal.sql(dialect)?),
        }
    }
}

/// Sqlite specific syntax
///
/// https://sqlite.org/lang_conflict.html
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum SqliteOnConflict {
    Rollback,
    Abort,
    Fail,
    Ignore,
    Replace,
}

impl DialectDisplay for SqliteOnConflict {
    fn fmt(&self, f: &mut (dyn fmt::Write), _dialect: &Dialect) -> fmt::Result {
        use SqliteOnConflict::*;
        match self {
            Rollback => write!(f, "ROLLBACK"),
            Abort => write!(f, "ABORT"),
            Fail => write!(f, "FAIL"),
            Ignore => write!(f, "IGNORE"),
            Replace => write!(f, "REPLACE"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum CopyTarget {
    Stdin,
    Stdout,
    File {
        /// The path name of the input or output file.
        filename: String,
    },
    Program {
        /// A command to execute
        command: String,
    },
}

impl DialectDisplay for CopyTarget {
    fn fmt(&self, f: &mut (dyn fmt::Write), dialect: &Dialect) -> fmt::Result {
        use CopyTarget::*;
        match self {
            Stdin { .. } => write!(f, "STDIN"),
            Stdout => write!(f, "STDOUT"),
            File { filename } => write!(f, "'{}'", value::escape_single_quote_string(filename).sql(dialect)?),
            Program { command } => write!(
                f,
                "PROGRAM '{}'",
                value::escape_single_quote_string(command).sql(dialect)?
            ),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum OnCommit {
    DeleteRows,
    PreserveRows,
    Drop,
}

/// An option in `COPY` statement.
///
/// <https://www.postgresql.org/docs/14/sql-copy.html>
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum CopyOption {
    /// FORMAT format_name
    Format(Ident),
    /// FREEZE \[ boolean \]
    Freeze(bool),
    /// DELIMITER 'delimiter_character'
    Delimiter(char),
    /// NULL 'null_string'
    Null(String),
    /// HEADER \[ boolean \]
    Header(bool),
    /// QUOTE 'quote_character'
    Quote(char),
    /// ESCAPE 'escape_character'
    Escape(char),
    /// FORCE_QUOTE { ( column_name [, ...] ) | * }
    ForceQuote(Vec<Ident>),
    /// FORCE_NOT_NULL ( column_name [, ...] )
    ForceNotNull(Vec<Ident>),
    /// FORCE_NULL ( column_name [, ...] )
    ForceNull(Vec<Ident>),
    /// ENCODING 'encoding_name'
    Encoding(String),
}

impl DialectDisplay for CopyOption {
    fn fmt(&self, f: &mut (dyn fmt::Write), dialect: &Dialect) -> fmt::Result {
        use CopyOption::*;
        match self {
            Format(name) => write!(f, "FORMAT {}", name.sql(dialect)?),
            Freeze(true) => write!(f, "FREEZE"),
            Freeze(false) => write!(f, "FREEZE FALSE"),
            Delimiter(char) => write!(f, "DELIMITER '{}'", char),
            Null(string) => write!(f, "NULL '{}'", value::escape_single_quote_string(string).sql(dialect)?),
            Header(true) => write!(f, "HEADER"),
            Header(false) => write!(f, "HEADER FALSE"),
            Quote(char) => write!(f, "QUOTE '{}'", char),
            Escape(char) => write!(f, "ESCAPE '{}'", char),
            ForceQuote(columns) => write!(f, "FORCE_QUOTE ({})", display_comma_separated(columns).sql(dialect)?),
            ForceNotNull(columns) => {
                write!(f, "FORCE_NOT_NULL ({})", display_comma_separated(columns).sql(dialect)?)
            }
            ForceNull(columns) => write!(f, "FORCE_NULL ({})", display_comma_separated(columns).sql(dialect)?),
            Encoding(name) => write!(f, "ENCODING '{}'", value::escape_single_quote_string(name).sql(dialect)?),
        }
    }
}

/// An option in `COPY` statement before PostgreSQL version 9.0.
///
/// <https://www.postgresql.org/docs/8.4/sql-copy.html>
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum CopyLegacyOption {
    /// BINARY
    Binary,
    /// DELIMITER \[ AS \] 'delimiter_character'
    Delimiter(char),
    /// NULL \[ AS \] 'null_string'
    Null(String),
    /// CSV ...
    Csv(Vec<CopyLegacyCsvOption>),
}

impl DialectDisplay for CopyLegacyOption {
    fn fmt(&self, f: &mut (dyn fmt::Write), dialect: &Dialect) -> fmt::Result {
        use CopyLegacyOption::*;
        match self {
            Binary => write!(f, "BINARY"),
            Delimiter(char) => write!(f, "DELIMITER '{}'", char),
            Null(string) => write!(f, "NULL '{}'", value::escape_single_quote_string(string).sql(dialect)?),
            Csv(opts) => write!(f, "CSV {}", display_separated(opts, " ").sql(dialect)?),
        }
    }
}

/// A `CSV` option in `COPY` statement before PostgreSQL version 9.0.
///
/// <https://www.postgresql.org/docs/8.4/sql-copy.html>
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum CopyLegacyCsvOption {
    /// HEADER
    Header,
    /// QUOTE \[ AS \] 'quote_character'
    Quote(char),
    /// ESCAPE \[ AS \] 'escape_character'
    Escape(char),
    /// FORCE QUOTE { column_name [, ...] | * }
    ForceQuote(Vec<Ident>),
    /// FORCE NOT NULL column_name [, ...]
    ForceNotNull(Vec<Ident>),
}

impl DialectDisplay for CopyLegacyCsvOption {
    fn fmt(&self, f: &mut (dyn fmt::Write), dialect: &Dialect) -> fmt::Result {
        use CopyLegacyCsvOption::*;
        match self {
            Header => write!(f, "HEADER"),
            Quote(char) => write!(f, "QUOTE '{}'", char),
            Escape(char) => write!(f, "ESCAPE '{}'", char),
            ForceQuote(columns) => write!(f, "FORCE QUOTE {}", display_comma_separated(columns).sql(dialect)?),
            ForceNotNull(columns) => {
                write!(f, "FORCE NOT NULL {}", display_comma_separated(columns).sql(dialect)?)
            }
        }
    }
}

///
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum MergeClause {
    MatchedUpdate {
        predicate: Option<Expr>,
        assignments: Vec<Assignment>,
    },
    MatchedDelete(Option<Expr>),
    NotMatched {
        predicate: Option<Expr>,
        columns: Vec<Ident>,
        values: Values,
    },
}

impl DialectDisplay for MergeClause {
    fn fmt(&self, f: &mut (dyn fmt::Write), dialect: &Dialect) -> fmt::Result {
        use MergeClause::*;
        write!(f, "WHEN")?;
        match self {
            MatchedUpdate {
                predicate,
                assignments,
            } => {
                write!(f, " MATCHED")?;
                if let Some(pred) = predicate {
                    write!(f, " AND {}", pred.sql(dialect)?)?;
                }
                write!(
                    f,
                    " THEN UPDATE SET {}",
                    display_comma_separated(assignments).sql(dialect)?
                )
            }
            MatchedDelete(predicate) => {
                write!(f, " MATCHED")?;
                if let Some(pred) = predicate {
                    write!(f, " AND {}", pred.sql(dialect)?)?;
                }
                write!(f, " THEN DELETE")
            }
            NotMatched {
                predicate,
                columns,
                values,
            } => {
                write!(f, " NOT MATCHED")?;
                if let Some(pred) = predicate {
                    write!(f, " AND {}", pred.sql(dialect)?)?;
                }
                write!(
                    f,
                    " THEN INSERT ({}) {}",
                    display_comma_separated(columns).sql(dialect)?,
                    values.sql(dialect)?
                )
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum DiscardObject {
    ALL,
    PLANS,
    SEQUENCES,
    TEMP,
}

impl DialectDisplay for DiscardObject {
    fn fmt(&self, f: &mut (dyn fmt::Write), _dialect: &Dialect) -> fmt::Result {
        match self {
            DiscardObject::ALL => f.write_str("ALL"),
            DiscardObject::PLANS => f.write_str("PLANS"),
            DiscardObject::SEQUENCES => f.write_str("SEQUENCES"),
            DiscardObject::TEMP => f.write_str("TEMP"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum CreateFunctionUsing {
    Jar(String),
    File(String),
    Archive(String),
}

impl DialectDisplay for CreateFunctionUsing {
    fn fmt(&self, f: &mut (dyn fmt::Write), _dialect: &Dialect) -> fmt::Result {
        write!(f, "USING ")?;
        match self {
            CreateFunctionUsing::Jar(uri) => write!(f, "JAR '{uri}'"),
            CreateFunctionUsing::File(uri) => write!(f, "FILE '{uri}'"),
            CreateFunctionUsing::Archive(uri) => write!(f, "ARCHIVE '{uri}'"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_window_frame_default() {
        let window_frame = WindowFrame::default();
        assert_eq!(WindowFrameBound::Preceding(None), window_frame.start_bound);
    }

    #[test]
    fn test_grouping_sets_display() {
        let dialect: Dialect = Default::default();

        // a and b in different group
        let grouping_sets = Expr::GroupingSets(vec![
            vec![Expr::Identifier(Ident::new("a"))],
            vec![Expr::Identifier(Ident::new("b"))],
        ]);
        assert_eq!("GROUPING SETS ((a), (b))", format!("{}", grouping_sets.sql(&dialect).unwrap()));

        // a and b in the same group
        let grouping_sets = Expr::GroupingSets(vec![vec![
            Expr::Identifier(Ident::new("a")),
            Expr::Identifier(Ident::new("b")),
        ]]);
        assert_eq!("GROUPING SETS ((a, b))", format!("{}", grouping_sets.sql(&dialect).unwrap()));

        // (a, b) and (c, d) in different group
        let grouping_sets = Expr::GroupingSets(vec![
            vec![
                Expr::Identifier(Ident::new("a")),
                Expr::Identifier(Ident::new("b")),
            ],
            vec![
                Expr::Identifier(Ident::new("c")),
                Expr::Identifier(Ident::new("d")),
            ],
        ]);
        assert_eq!(
            "GROUPING SETS ((a, b), (c, d))",
            format!("{}", grouping_sets.sql(&dialect).unwrap())
        );
    }

    #[test]
    fn test_rollup_display() {
        let dialect: Dialect = Default::default();

        let rollup = Expr::Rollup(vec![vec![Expr::Identifier(Ident::new("a"))]]);
        assert_eq!("ROLLUP (a)", format!("{}", rollup.sql(&dialect).unwrap()));

        let rollup = Expr::Rollup(vec![vec![
            Expr::Identifier(Ident::new("a")),
            Expr::Identifier(Ident::new("b")),
        ]]);
        assert_eq!("ROLLUP ((a, b))", format!("{}", rollup.sql(&dialect).unwrap()));

        let rollup = Expr::Rollup(vec![
            vec![Expr::Identifier(Ident::new("a"))],
            vec![Expr::Identifier(Ident::new("b"))],
        ]);
        assert_eq!("ROLLUP (a, b)", format!("{}", rollup.sql(&dialect).unwrap()));

        let rollup = Expr::Rollup(vec![
            vec![Expr::Identifier(Ident::new("a"))],
            vec![
                Expr::Identifier(Ident::new("b")),
                Expr::Identifier(Ident::new("c")),
            ],
            vec![Expr::Identifier(Ident::new("d"))],
        ]);
        assert_eq!("ROLLUP (a, (b, c), d)", format!("{}", rollup.sql(&dialect).unwrap()));
    }

    #[test]
    fn test_cube_display() {
        let dialect: Dialect = Default::default();

        let cube = Expr::Cube(vec![vec![Expr::Identifier(Ident::new("a"))]]);
        assert_eq!("CUBE (a)", format!("{}", cube.sql(&dialect).unwrap()));

        let cube = Expr::Cube(vec![vec![
            Expr::Identifier(Ident::new("a")),
            Expr::Identifier(Ident::new("b")),
        ]]);
        assert_eq!("CUBE ((a, b))", format!("{}", cube.sql(&dialect).unwrap()));

        let cube = Expr::Cube(vec![
            vec![Expr::Identifier(Ident::new("a"))],
            vec![Expr::Identifier(Ident::new("b"))],
        ]);
        assert_eq!("CUBE (a, b)", format!("{}", cube.sql(&dialect).unwrap()));

        let cube = Expr::Cube(vec![
            vec![Expr::Identifier(Ident::new("a"))],
            vec![
                Expr::Identifier(Ident::new("b")),
                Expr::Identifier(Ident::new("c")),
            ],
            vec![Expr::Identifier(Ident::new("d"))],
        ]);
        assert_eq!("CUBE (a, (b, c), d)", format!("{}", cube.sql(&dialect).unwrap()));
    }
}

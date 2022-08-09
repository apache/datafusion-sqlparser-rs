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
mod ddl;
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

pub use self::data_type::DataType;
pub use self::ddl::{
    AlterColumnOperation, AlterTableOperation, ColumnDef, ColumnOption, ColumnOptionDef,
    ReferentialAction, TableConstraint,
};
pub use self::operator::{BinaryOperator, UnaryOperator};
pub use self::query::{
    Cte, Fetch, Join, JoinConstraint, JoinOperator, LateralView, LockType, Offset, OffsetRows,
    OrderByExpr, Query, Select, SelectInto, SelectItem, SetExpr, SetOperator, TableAlias,
    TableFactor, TableWithJoins, Top, Values, With,
};
pub use self::value::{DateTimeField, TrimWhereField, Value};

struct DisplaySeparated<'a, T>
where
    T: fmt::Display,
{
    slice: &'a [T],
    sep: &'static str,
}

impl<'a, T> fmt::Display for DisplaySeparated<'a, T>
where
    T: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut delim = "";
        for t in self.slice {
            write!(f, "{}", delim)?;
            delim = self.sep;
            write!(f, "{}", t)?;
        }
        Ok(())
    }
}

fn display_separated<'a, T>(slice: &'a [T], sep: &'static str) -> DisplaySeparated<'a, T>
where
    T: fmt::Display,
{
    DisplaySeparated { slice, sep }
}

fn display_comma_separated<T>(slice: &[T]) -> DisplaySeparated<'_, T>
where
    T: fmt::Display,
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

impl fmt::Display for Ident {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.quote_style {
            Some(q) if q == '"' || q == '\'' || q == '`' => {
                let escaped = value::escape_quoted_string(&self.value, q);
                write!(f, "{}{}{}", q, escaped, q)
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

impl fmt::Display for ObjectName {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", display_separated(&self.0, "."))
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

impl fmt::Display for Array {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}[{}]",
            if self.named { "ARRAY" } else { "" },
            display_comma_separated(&self.elem)
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

impl fmt::Display for JsonOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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
    /// SAFE_CAST an expression to a different data type e.g. `SAFE_CAST(foo AS FLOAT64)`
    //  only available for BigQuery: https://cloud.google.com/bigquery/docs/reference/standard-sql/functions-and-operators#safe_casting
    //  this works the same as `TRY_CAST`
    SafeCast {
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

impl fmt::Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Expr::Identifier(s) => write!(f, "{}", s),
            Expr::MapAccess { column, keys } => {
                write!(f, "{}", column)?;
                for k in keys {
                    match k {
                        k @ Expr::Value(Value::Number(_, _)) => write!(f, "[{}]", k)?,
                        Expr::Value(Value::SingleQuotedString(s)) => write!(f, "[\"{}\"]", s)?,
                        _ => write!(f, "[{}]", k)?,
                    }
                }
                Ok(())
            }
            Expr::CompoundIdentifier(s) => write!(f, "{}", display_separated(s, ".")),
            Expr::IsTrue(ast) => write!(f, "{} IS TRUE", ast),
            Expr::IsFalse(ast) => write!(f, "{} IS FALSE", ast),
            Expr::IsNull(ast) => write!(f, "{} IS NULL", ast),
            Expr::IsNotNull(ast) => write!(f, "{} IS NOT NULL", ast),
            Expr::InList {
                expr,
                list,
                negated,
            } => write!(
                f,
                "{} {}IN ({})",
                expr,
                if *negated { "NOT " } else { "" },
                display_comma_separated(list)
            ),
            Expr::InSubquery {
                expr,
                subquery,
                negated,
            } => write!(
                f,
                "{} {}IN ({})",
                expr,
                if *negated { "NOT " } else { "" },
                subquery
            ),
            Expr::InUnnest {
                expr,
                array_expr,
                negated,
            } => write!(
                f,
                "{} {}IN UNNEST({})",
                expr,
                if *negated { "NOT " } else { "" },
                array_expr
            ),
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => write!(
                f,
                "{} {}BETWEEN {} AND {}",
                expr,
                if *negated { "NOT " } else { "" },
                low,
                high
            ),
            Expr::BinaryOp { left, op, right } => write!(f, "{} {} {}", left, op, right),
            Expr::AnyOp(expr) => write!(f, "ANY({})", expr),
            Expr::AllOp(expr) => write!(f, "ALL({})", expr),
            Expr::UnaryOp { op, expr } => {
                if op == &UnaryOperator::PGPostfixFactorial {
                    write!(f, "{}{}", expr, op)
                } else {
                    write!(f, "{} {}", op, expr)
                }
            }
            Expr::Cast { expr, data_type } => write!(f, "CAST({} AS {})", expr, data_type),
            Expr::TryCast { expr, data_type } => write!(f, "TRY_CAST({} AS {})", expr, data_type),
            Expr::SafeCast { expr, data_type } => write!(f, "SAFE_CAST({} AS {})", expr, data_type),
            Expr::Extract { field, expr } => write!(f, "EXTRACT({} FROM {})", field, expr),
            Expr::Position { expr, r#in } => write!(f, "POSITION({} IN {})", expr, r#in),
            Expr::Collate { expr, collation } => write!(f, "{} COLLATE {}", expr, collation),
            Expr::Nested(ast) => write!(f, "({})", ast),
            Expr::Value(v) => write!(f, "{}", v),
            Expr::TypedString { data_type, value } => {
                write!(f, "{}", data_type)?;
                write!(f, " '{}'", &value::escape_single_quote_string(value))
            }
            Expr::Function(fun) => write!(f, "{}", fun),
            Expr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => {
                write!(f, "CASE")?;
                if let Some(operand) = operand {
                    write!(f, " {}", operand)?;
                }
                for (c, r) in conditions.iter().zip(results) {
                    write!(f, " WHEN {} THEN {}", c, r)?;
                }

                if let Some(else_result) = else_result {
                    write!(f, " ELSE {}", else_result)?;
                }
                write!(f, " END")
            }
            Expr::Exists { subquery, negated } => write!(
                f,
                "{}EXISTS ({})",
                if *negated { "NOT " } else { "" },
                subquery
            ),
            Expr::Subquery(s) => write!(f, "({})", s),
            Expr::ListAgg(listagg) => write!(f, "{}", listagg),
            Expr::GroupingSets(sets) => {
                write!(f, "GROUPING SETS (")?;
                let mut sep = "";
                for set in sets {
                    write!(f, "{}", sep)?;
                    sep = ", ";
                    write!(f, "({})", display_comma_separated(set))?;
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
                        write!(f, "{}", set[0])?;
                    } else {
                        write!(f, "({})", display_comma_separated(set))?;
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
                        write!(f, "{}", set[0])?;
                    } else {
                        write!(f, "({})", display_comma_separated(set))?;
                    }
                }
                write!(f, ")")
            }
            Expr::Substring {
                expr,
                substring_from,
                substring_for,
            } => {
                write!(f, "SUBSTRING({}", expr)?;
                if let Some(from_part) = substring_from {
                    write!(f, " FROM {}", from_part)?;
                }
                if let Some(from_part) = substring_for {
                    write!(f, " FOR {}", from_part)?;
                }

                write!(f, ")")
            }
            Expr::IsDistinctFrom(a, b) => write!(f, "{} IS DISTINCT FROM {}", a, b),
            Expr::IsNotDistinctFrom(a, b) => write!(f, "{} IS NOT DISTINCT FROM {}", a, b),
            Expr::Trim { expr, trim_where } => {
                write!(f, "TRIM(")?;
                if let Some((ident, trim_char)) = trim_where {
                    write!(f, "{} {} FROM {}", ident, trim_char, expr)?;
                } else {
                    write!(f, "{}", expr)?;
                }

                write!(f, ")")
            }
            Expr::Tuple(exprs) => {
                write!(f, "({})", display_comma_separated(exprs))
            }
            Expr::ArrayIndex { obj, indexes } => {
                write!(f, "{}", obj)?;
                for i in indexes {
                    write!(f, "[{}]", i)?;
                }
                Ok(())
            }
            Expr::Array(set) => {
                write!(f, "{}", set)
            }
            Expr::JsonAccess {
                left,
                operator,
                right,
            } => {
                write!(f, "{} {} {}", left, operator, right)
            }
            Expr::CompositeAccess { expr, key } => {
                write!(f, "{}.{}", expr, key)
            }
            Expr::AtTimeZone {
                timestamp,
                time_zone,
            } => {
                write!(f, "{} AT TIME ZONE '{}'", timestamp, time_zone)
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

impl fmt::Display for WindowSpec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut delim = "";
        if !self.partition_by.is_empty() {
            delim = " ";
            write!(
                f,
                "PARTITION BY {}",
                display_comma_separated(&self.partition_by)
            )?;
        }
        if !self.order_by.is_empty() {
            f.write_str(delim)?;
            delim = " ";
            write!(f, "ORDER BY {}", display_comma_separated(&self.order_by))?;
        }
        if let Some(window_frame) = &self.window_frame {
            f.write_str(delim)?;
            if let Some(end_bound) = &window_frame.end_bound {
                write!(
                    f,
                    "{} BETWEEN {} AND {}",
                    window_frame.units, window_frame.start_bound, end_bound
                )?;
            } else {
                write!(f, "{} {}", window_frame.units, window_frame.start_bound)?;
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

impl fmt::Display for WindowFrameUnits {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
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

impl fmt::Display for WindowFrameBound {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
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

impl fmt::Display for AddDropSync {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
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

impl fmt::Display for ShowCreateObject {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
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

impl fmt::Display for CommentObject {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            CommentObject::Column => f.write_str("COLUMN"),
            CommentObject::Table => f.write_str("TABLE"),
        }
    }
}

/// A top-level statement (SELECT, INSERT, CREATE, etc.)
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum Statement {
    /// Analyze (Hive)
    Analyze {
        table_name: ObjectName,
        partitions: Option<Vec<Expr>>,
        for_columns: bool,
        columns: Vec<Ident>,
        cache_metadata: bool,
        noscan: bool,
        compute_statistics: bool,
    },
    /// Truncate (Hive)
    Truncate {
        table_name: ObjectName,
        partitions: Option<Vec<Expr>>,
    },
    /// Msck (Hive)
    Msck {
        table_name: ObjectName,
        repair: bool,
        partition_action: Option<AddDropSync>,
    },
    /// SELECT
    Query(Box<Query>),
    /// INSERT
    Insert {
        /// Only for Sqlite
        or: Option<SqliteOnConflict>,
        /// INTO - optional keyword
        into: bool,
        /// TABLE
        table_name: ObjectName,
        /// COLUMNS
        columns: Vec<Ident>,
        /// Overwrite (Hive)
        overwrite: bool,
        /// A SQL query that specifies what to insert
        source: Box<Query>,
        /// partitioned insert (Hive)
        partitioned: Option<Vec<Expr>>,
        /// Columns defined after PARTITION
        after_columns: Vec<Ident>,
        /// whether the insert has the table keyword (Hive)
        table: bool,
        on: Option<OnInsert>,
    },
    // TODO: Support ROW FORMAT
    Directory {
        overwrite: bool,
        local: bool,
        path: String,
        file_format: Option<FileFormat>,
        source: Box<Query>,
    },
    Copy {
        /// TABLE
        table_name: ObjectName,
        /// COLUMNS
        columns: Vec<Ident>,
        /// If true, is a 'COPY TO' statement. If false is a 'COPY FROM'
        to: bool,
        /// The source of 'COPY FROM', or the target of 'COPY TO'
        target: CopyTarget,
        /// WITH options (from PostgreSQL version 9.0)
        options: Vec<CopyOption>,
        /// WITH options (before PostgreSQL version 9.0)
        legacy_options: Vec<CopyLegacyOption>,
        /// VALUES a vector of values to be copied
        values: Vec<Option<String>>,
    },
    /// Close - closes the portal underlying an open cursor.
    Close {
        /// Cursor name
        cursor: CloseCursor,
    },
    /// UPDATE
    Update {
        /// TABLE
        table: TableWithJoins,
        /// Column assignments
        assignments: Vec<Assignment>,
        /// Table which provide value to be set
        from: Option<TableWithJoins>,
        /// WHERE
        selection: Option<Expr>,
    },
    /// DELETE
    Delete {
        /// FROM
        table_name: TableFactor,
        /// USING (Snowflake, Postgres)
        using: Option<TableFactor>,
        /// WHERE
        selection: Option<Expr>,
    },
    /// CREATE VIEW
    CreateView {
        or_replace: bool,
        materialized: bool,
        /// View name
        name: ObjectName,
        columns: Vec<Ident>,
        query: Box<Query>,
        with_options: Vec<SqlOption>,
    },
    /// CREATE TABLE
    CreateTable {
        or_replace: bool,
        temporary: bool,
        external: bool,
        global: Option<bool>,
        if_not_exists: bool,
        /// Table name
        name: ObjectName,
        /// Optional schema
        columns: Vec<ColumnDef>,
        constraints: Vec<TableConstraint>,
        hive_distribution: HiveDistributionStyle,
        hive_formats: Option<HiveFormat>,
        table_properties: Vec<SqlOption>,
        with_options: Vec<SqlOption>,
        file_format: Option<FileFormat>,
        location: Option<String>,
        query: Option<Box<Query>>,
        without_rowid: bool,
        like: Option<ObjectName>,
        clone: Option<ObjectName>,
        engine: Option<String>,
        default_charset: Option<String>,
        collation: Option<String>,
        on_commit: Option<OnCommit>,
        /// Click house "ON CLUSTER" clause:
        /// <https://clickhouse.com/docs/en/sql-reference/distributed-ddl/>
        on_cluster: Option<String>,
    },
    /// SQLite's `CREATE VIRTUAL TABLE .. USING <module_name> (<module_args>)`
    CreateVirtualTable {
        name: ObjectName,
        if_not_exists: bool,
        module_name: Ident,
        module_args: Vec<Ident>,
    },
    /// CREATE INDEX
    CreateIndex {
        /// index name
        name: ObjectName,
        table_name: ObjectName,
        columns: Vec<OrderByExpr>,
        unique: bool,
        if_not_exists: bool,
    },
    /// ALTER TABLE
    AlterTable {
        /// Table name
        name: ObjectName,
        operation: AlterTableOperation,
    },
    /// DROP
    Drop {
        /// The type of the object to drop: TABLE, VIEW, etc.
        object_type: ObjectType,
        /// An optional `IF EXISTS` clause. (Non-standard.)
        if_exists: bool,
        /// One or more objects to drop. (ANSI SQL requires exactly one.)
        names: Vec<ObjectName>,
        /// Whether `CASCADE` was specified. This will be `false` when
        /// `RESTRICT` or no drop behavior at all was specified.
        cascade: bool,
        /// Hive allows you specify whether the table's stored data will be
        /// deleted along with the dropped table
        purge: bool,
    },
    /// DECLARE - Declaring Cursor Variables
    ///
    /// Note: this is a PostgreSQL-specific statement,
    /// but may also compatible with other SQL.
    Declare {
        /// Cursor name
        name: Ident,
        /// Causes the cursor to return data in binary rather than in text format.
        binary: bool,
        /// None = Not specified
        /// Some(true) = INSENSITIVE
        /// Some(false) = ASENSITIVE
        sensitive: Option<bool>,
        /// None = Not specified
        /// Some(true) = SCROLL
        /// Some(false) = NO SCROLL
        scroll: Option<bool>,
        /// None = Not specified
        /// Some(true) = WITH HOLD, specifies that the cursor can continue to be used after the transaction that created it successfully commits
        /// Some(false) = WITHOUT HOLD, specifies that the cursor cannot be used outside of the transaction that created it
        hold: Option<bool>,
        query: Box<Query>,
    },
    /// FETCH - retrieve rows from a query using a cursor
    ///
    /// Note: this is a PostgreSQL-specific statement,
    /// but may also compatible with other SQL.
    Fetch {
        /// Cursor name
        name: Ident,
        direction: FetchDirection,
        /// Optional, It's possible to fetch rows form cursor to the table
        into: Option<ObjectName>,
    },
    /// DISCARD [ ALL | PLANS | SEQUENCES | TEMPORARY | TEMP ]
    ///
    /// Note: this is a PostgreSQL-specific statement,
    /// but may also compatible with other SQL.
    Discard { object_type: DiscardObject },
    /// SET [ SESSION | LOCAL ] ROLE role_name
    ///
    /// Note: this is a PostgreSQL-specific statement,
    /// but may also compatible with other SQL.
    SetRole {
        local: bool,
        // SESSION is the default if neither SESSION nor LOCAL appears.
        session: bool,
        role_name: Option<Ident>,
    },
    /// SET <variable>
    ///
    /// Note: this is not a standard SQL statement, but it is supported by at
    /// least MySQL and PostgreSQL. Not all MySQL-specific syntatic forms are
    /// supported yet.
    SetVariable {
        local: bool,
        hivevar: bool,
        variable: ObjectName,
        value: Vec<SetVariableValue>,
    },
    /// SET NAMES 'charset_name' [COLLATE 'collation_name']
    ///
    /// Note: this is a MySQL-specific statement.
    SetNames {
        charset_name: String,
        collation_name: Option<String>,
    },
    /// SET NAMES DEFAULT
    ///
    /// Note: this is a MySQL-specific statement.
    SetNamesDefault {},
    /// SHOW <variable>
    ///
    /// Note: this is a PostgreSQL-specific statement.
    ShowVariable { variable: Vec<Ident> },
    /// SHOW VARIABLES
    ///
    /// Note: this is a MySQL-specific statement.
    ShowVariables { filter: Option<ShowStatementFilter> },
    /// SHOW CREATE TABLE
    ///
    /// Note: this is a MySQL-specific statement.
    ShowCreate {
        obj_type: ShowCreateObject,
        obj_name: ObjectName,
    },
    /// SHOW COLUMNS
    ///
    /// Note: this is a MySQL-specific statement.
    ShowColumns {
        extended: bool,
        full: bool,
        table_name: ObjectName,
        filter: Option<ShowStatementFilter>,
    },
    /// SHOW TABLES
    ///
    /// Note: this is a MySQL-specific statement.
    ShowTables {
        extended: bool,
        full: bool,
        db_name: Option<Ident>,
        filter: Option<ShowStatementFilter>,
    },
    /// SHOW COLLATION
    ///
    /// Note: this is a MySQL-specific statement.
    ShowCollation { filter: Option<ShowStatementFilter> },
    /// USE
    ///
    /// Note: This is a MySQL-specific statement.
    Use { db_name: Ident },
    /// `{ BEGIN [ TRANSACTION | WORK ] | START TRANSACTION } ...`
    StartTransaction { modes: Vec<TransactionMode> },
    /// `SET TRANSACTION ...`
    SetTransaction {
        modes: Vec<TransactionMode>,
        snapshot: Option<Value>,
        session: bool,
    },
    /// `COMMENT ON ...`
    ///
    /// Note: this is a PostgreSQL-specific statement.
    Comment {
        object_type: CommentObject,
        object_name: ObjectName,
        comment: Option<String>,
    },
    /// `COMMIT [ TRANSACTION | WORK ] [ AND [ NO ] CHAIN ]`
    Commit { chain: bool },
    /// `ROLLBACK [ TRANSACTION | WORK ] [ AND [ NO ] CHAIN ]`
    Rollback { chain: bool },
    /// CREATE SCHEMA
    CreateSchema {
        schema_name: ObjectName,
        if_not_exists: bool,
    },
    /// CREATE DATABASE
    CreateDatabase {
        db_name: ObjectName,
        if_not_exists: bool,
        location: Option<String>,
        managed_location: Option<String>,
    },
    /// CREATE FUNCTION
    ///
    /// Hive: https://cwiki.apache.org/confluence/display/hive/languagemanual+ddl#LanguageManualDDL-Create/Drop/ReloadFunction
    CreateFunction {
        temporary: bool,
        name: ObjectName,
        class_name: String,
        using: Option<CreateFunctionUsing>,
    },
    /// `ASSERT <condition> [AS <message>]`
    Assert {
        condition: Expr,
        message: Option<Expr>,
    },
    /// GRANT privileges ON objects TO grantees
    Grant {
        privileges: Privileges,
        objects: GrantObjects,
        grantees: Vec<Ident>,
        with_grant_option: bool,
        granted_by: Option<Ident>,
    },
    /// REVOKE privileges ON objects FROM grantees
    Revoke {
        privileges: Privileges,
        objects: GrantObjects,
        grantees: Vec<Ident>,
        granted_by: Option<Ident>,
        cascade: bool,
    },
    /// `DEALLOCATE [ PREPARE ] { name | ALL }`
    ///
    /// Note: this is a PostgreSQL-specific statement.
    Deallocate { name: Ident, prepare: bool },
    /// `EXECUTE name [ ( parameter [, ...] ) ]`
    ///
    /// Note: this is a PostgreSQL-specific statement.
    Execute { name: Ident, parameters: Vec<Expr> },
    /// `PREPARE name [ ( data_type [, ...] ) ] AS statement`
    ///
    /// Note: this is a PostgreSQL-specific statement.
    Prepare {
        name: Ident,
        data_types: Vec<DataType>,
        statement: Box<Statement>,
    },
    /// KILL [CONNECTION | QUERY | MUTATION]
    ///
    /// See <https://clickhouse.com/docs/ru/sql-reference/statements/kill/>
    /// See <https://dev.mysql.com/doc/refman/8.0/en/kill.html>
    Kill {
        modifier: Option<KillType>,
        // processlist_id
        id: u64,
    },
    /// EXPLAIN TABLE
    /// Note: this is a MySQL-specific statement. See <https://dev.mysql.com/doc/refman/8.0/en/explain.html>
    ExplainTable {
        // If true, query used the MySQL `DESCRIBE` alias for explain
        describe_alias: bool,
        // Table name
        table_name: ObjectName,
    },
    /// EXPLAIN / DESCRIBE for select_statement
    Explain {
        // If true, query used the MySQL `DESCRIBE` alias for explain
        describe_alias: bool,
        /// Carry out the command and show actual run times and other statistics.
        analyze: bool,
        // Display additional information regarding the plan.
        verbose: bool,
        /// A SQL query that specifies what to explain
        statement: Box<Statement>,
    },
    /// SAVEPOINT -- define a new savepoint within the current transaction
    Savepoint { name: Ident },
    // MERGE INTO statement, based on Snowflake. See <https://docs.snowflake.com/en/sql-reference/sql/merge.html>
    Merge {
        // optional INTO keyword
        into: bool,
        // Specifies the table to merge
        table: TableFactor,
        // Specifies the table or subquery to join with the target table
        source: TableFactor,
        // Specifies the expression on which to join the target table and source
        on: Box<Expr>,
        // Specifies the actions to perform when values match or do not match.
        clauses: Vec<MergeClause>,
    },
}

impl fmt::Display for Statement {
    // Clippy thinks this function is too complicated, but it is painful to
    // split up without extracting structs for each `Statement` variant.
    #[allow(clippy::cognitive_complexity)]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Statement::Kill { modifier, id } => {
                write!(f, "KILL ")?;

                if let Some(m) = modifier {
                    write!(f, "{} ", m)?;
                }

                write!(f, "{}", id)
            }
            Statement::ExplainTable {
                describe_alias,
                table_name,
            } => {
                if *describe_alias {
                    write!(f, "DESCRIBE ")?;
                } else {
                    write!(f, "EXPLAIN ")?;
                }

                write!(f, "{}", table_name)
            }
            Statement::Explain {
                describe_alias,
                verbose,
                analyze,
                statement,
            } => {
                if *describe_alias {
                    write!(f, "DESCRIBE ")?;
                } else {
                    write!(f, "EXPLAIN ")?;
                }

                if *analyze {
                    write!(f, "ANALYZE ")?;
                }

                if *verbose {
                    write!(f, "VERBOSE ")?;
                }

                write!(f, "{}", statement)
            }
            Statement::Query(s) => write!(f, "{}", s),
            Statement::Declare {
                name,
                binary,
                sensitive,
                scroll,
                hold,
                query,
            } => {
                write!(f, "DECLARE {} ", name)?;

                if *binary {
                    write!(f, "BINARY ")?;
                }

                if let Some(sensitive) = sensitive {
                    if *sensitive {
                        write!(f, "INSENSITIVE ")?;
                    } else {
                        write!(f, "ASENSITIVE ")?;
                    }
                }

                if let Some(scroll) = scroll {
                    if *scroll {
                        write!(f, "SCROLL ")?;
                    } else {
                        write!(f, "NO SCROLL ")?;
                    }
                }

                write!(f, "CURSOR ")?;

                if let Some(hold) = hold {
                    if *hold {
                        write!(f, "WITH HOLD ")?;
                    } else {
                        write!(f, "WITHOUT HOLD ")?;
                    }
                }

                write!(f, "FOR {}", query)
            }
            Statement::Fetch {
                name,
                direction,
                into,
            } => {
                write!(f, "FETCH {} ", direction)?;

                write!(f, "IN {}", name)?;

                if let Some(into) = into {
                    write!(f, " INTO {}", into)?;
                }

                Ok(())
            }
            Statement::Directory {
                overwrite,
                local,
                path,
                file_format,
                source,
            } => {
                write!(
                    f,
                    "INSERT{overwrite}{local} DIRECTORY '{path}'",
                    overwrite = if *overwrite { " OVERWRITE" } else { "" },
                    local = if *local { " LOCAL" } else { "" },
                    path = path
                )?;
                if let Some(ref ff) = file_format {
                    write!(f, " STORED AS {}", ff)?
                }
                write!(f, " {}", source)
            }
            Statement::Msck {
                table_name,
                repair,
                partition_action,
            } => {
                write!(
                    f,
                    "MSCK {repair}TABLE {table}",
                    repair = if *repair { "REPAIR " } else { "" },
                    table = table_name
                )?;
                if let Some(pa) = partition_action {
                    write!(f, " {}", pa)?;
                }
                Ok(())
            }
            Statement::Truncate {
                table_name,
                partitions,
            } => {
                write!(f, "TRUNCATE TABLE {}", table_name)?;
                if let Some(ref parts) = partitions {
                    if !parts.is_empty() {
                        write!(f, " PARTITION ({})", display_comma_separated(parts))?;
                    }
                }
                Ok(())
            }
            Statement::Analyze {
                table_name,
                partitions,
                for_columns,
                columns,
                cache_metadata,
                noscan,
                compute_statistics,
            } => {
                write!(f, "ANALYZE TABLE {}", table_name)?;
                if let Some(ref parts) = partitions {
                    if !parts.is_empty() {
                        write!(f, " PARTITION ({})", display_comma_separated(parts))?;
                    }
                }

                if *compute_statistics {
                    write!(f, " COMPUTE STATISTICS")?;
                }
                if *noscan {
                    write!(f, " NOSCAN")?;
                }
                if *cache_metadata {
                    write!(f, " CACHE METADATA")?;
                }
                if *for_columns {
                    write!(f, " FOR COLUMNS")?;
                    if !columns.is_empty() {
                        write!(f, " {}", display_comma_separated(columns))?;
                    }
                }
                Ok(())
            }
            Statement::Insert {
                or,
                into,
                table_name,
                overwrite,
                partitioned,
                columns,
                after_columns,
                source,
                table,
                on,
            } => {
                if let Some(action) = or {
                    write!(f, "INSERT OR {} INTO {} ", action, table_name)?;
                } else {
                    write!(
                        f,
                        "INSERT{over}{int}{tbl} {table_name} ",
                        table_name = table_name,
                        over = if *overwrite { " OVERWRITE" } else { "" },
                        int = if *into { " INTO" } else { "" },
                        tbl = if *table { " TABLE" } else { "" }
                    )?;
                }
                if !columns.is_empty() {
                    write!(f, "({}) ", display_comma_separated(columns))?;
                }
                if let Some(ref parts) = partitioned {
                    if !parts.is_empty() {
                        write!(f, "PARTITION ({}) ", display_comma_separated(parts))?;
                    }
                }
                if !after_columns.is_empty() {
                    write!(f, "({}) ", display_comma_separated(after_columns))?;
                }
                write!(f, "{}", source)?;

                if let Some(on) = on {
                    write!(f, "{}", on)
                } else {
                    Ok(())
                }
            }

            Statement::Copy {
                table_name,
                columns,
                to,
                target,
                options,
                legacy_options,
                values,
            } => {
                write!(f, "COPY {}", table_name)?;
                if !columns.is_empty() {
                    write!(f, " ({})", display_comma_separated(columns))?;
                }
                write!(f, " {} {}", if *to { "TO" } else { "FROM" }, target)?;
                if !options.is_empty() {
                    write!(f, " ({})", display_comma_separated(options))?;
                }
                if !legacy_options.is_empty() {
                    write!(f, " {}", display_separated(legacy_options, " "))?;
                }
                if !values.is_empty() {
                    writeln!(f, ";")?;
                    let mut delim = "";
                    for v in values {
                        write!(f, "{}", delim)?;
                        delim = "\t";
                        if let Some(v) = v {
                            write!(f, "{}", v)?;
                        } else {
                            write!(f, "\\N")?;
                        }
                    }
                    write!(f, "\n\\.")?;
                }
                Ok(())
            }
            Statement::Update {
                table,
                assignments,
                from,
                selection,
            } => {
                write!(f, "UPDATE {}", table)?;
                if !assignments.is_empty() {
                    write!(f, " SET {}", display_comma_separated(assignments))?;
                }
                if let Some(from) = from {
                    write!(f, " FROM {}", from)?;
                }
                if let Some(selection) = selection {
                    write!(f, " WHERE {}", selection)?;
                }
                Ok(())
            }
            Statement::Delete {
                table_name,
                using,
                selection,
            } => {
                write!(f, "DELETE FROM {}", table_name)?;
                if let Some(using) = using {
                    write!(f, " USING {}", using)?;
                }
                if let Some(selection) = selection {
                    write!(f, " WHERE {}", selection)?;
                }
                Ok(())
            }
            Statement::Close { cursor } => {
                write!(f, "CLOSE {}", cursor)?;

                Ok(())
            }
            Statement::CreateDatabase {
                db_name,
                if_not_exists,
                location,
                managed_location,
            } => {
                write!(f, "CREATE DATABASE")?;
                if *if_not_exists {
                    write!(f, " IF NOT EXISTS")?;
                }
                write!(f, " {}", db_name)?;
                if let Some(l) = location {
                    write!(f, " LOCATION '{}'", l)?;
                }
                if let Some(ml) = managed_location {
                    write!(f, " MANAGEDLOCATION '{}'", ml)?;
                }
                Ok(())
            }
            Statement::CreateFunction {
                temporary,
                name,
                class_name,
                using,
            } => {
                write!(
                    f,
                    "CREATE {temp}FUNCTION {name} AS '{class_name}'",
                    temp = if *temporary { "TEMPORARY " } else { "" },
                )?;
                if let Some(u) = using {
                    write!(f, " {}", u)?;
                }
                Ok(())
            }
            Statement::CreateView {
                name,
                or_replace,
                columns,
                query,
                materialized,
                with_options,
            } => {
                write!(
                    f,
                    "CREATE {or_replace}{materialized}VIEW {name}",
                    or_replace = if *or_replace { "OR REPLACE " } else { "" },
                    materialized = if *materialized { "MATERIALIZED " } else { "" },
                    name = name
                )?;
                if !with_options.is_empty() {
                    write!(f, " WITH ({})", display_comma_separated(with_options))?;
                }
                if !columns.is_empty() {
                    write!(f, " ({})", display_comma_separated(columns))?;
                }
                write!(f, " AS {}", query)
            }
            Statement::CreateTable {
                name,
                columns,
                constraints,
                table_properties,
                with_options,
                or_replace,
                if_not_exists,
                hive_distribution,
                hive_formats,
                external,
                global,
                temporary,
                file_format,
                location,
                query,
                without_rowid,
                like,
                clone,
                default_charset,
                engine,
                collation,
                on_commit,
                on_cluster,
            } => {
                // We want to allow the following options
                // Empty column list, allowed by PostgreSQL:
                //   `CREATE TABLE t ()`
                // No columns provided for CREATE TABLE AS:
                //   `CREATE TABLE t AS SELECT a from t2`
                // Columns provided for CREATE TABLE AS:
                //   `CREATE TABLE t (a INT) AS SELECT a from t2`
                write!(
                    f,
                    "CREATE {or_replace}{external}{global}{temporary}TABLE {if_not_exists}{name}",
                    or_replace = if *or_replace { "OR REPLACE " } else { "" },
                    external = if *external { "EXTERNAL " } else { "" },
                    global = global
                        .map(|global| {
                            if global {
                                "GLOBAL "
                            } else {
                                "LOCAL "
                            }
                        })
                        .unwrap_or(""),
                    if_not_exists = if *if_not_exists { "IF NOT EXISTS " } else { "" },
                    temporary = if *temporary { "TEMPORARY " } else { "" },
                    name = name,
                )?;
                if let Some(on_cluster) = on_cluster {
                    write!(
                        f,
                        " ON CLUSTER {}",
                        on_cluster.replace('{', "'{").replace('}', "}'")
                    )?;
                }
                if !columns.is_empty() || !constraints.is_empty() {
                    write!(f, " ({}", display_comma_separated(columns))?;
                    if !columns.is_empty() && !constraints.is_empty() {
                        write!(f, ", ")?;
                    }
                    write!(f, "{})", display_comma_separated(constraints))?;
                } else if query.is_none() && like.is_none() && clone.is_none() {
                    // PostgreSQL allows `CREATE TABLE t ();`, but requires empty parens
                    write!(f, " ()")?;
                }
                // Only for SQLite
                if *without_rowid {
                    write!(f, " WITHOUT ROWID")?;
                }

                // Only for Hive
                if let Some(l) = like {
                    write!(f, " LIKE {}", l)?;
                }

                if let Some(c) = clone {
                    write!(f, " CLONE {}", c)?;
                }

                match hive_distribution {
                    HiveDistributionStyle::PARTITIONED { columns } => {
                        write!(f, " PARTITIONED BY ({})", display_comma_separated(columns))?;
                    }
                    HiveDistributionStyle::CLUSTERED {
                        columns,
                        sorted_by,
                        num_buckets,
                    } => {
                        write!(f, " CLUSTERED BY ({})", display_comma_separated(columns))?;
                        if !sorted_by.is_empty() {
                            write!(f, " SORTED BY ({})", display_comma_separated(sorted_by))?;
                        }
                        if *num_buckets > 0 {
                            write!(f, " INTO {} BUCKETS", num_buckets)?;
                        }
                    }
                    HiveDistributionStyle::SKEWED {
                        columns,
                        on,
                        stored_as_directories,
                    } => {
                        write!(
                            f,
                            " SKEWED BY ({})) ON ({})",
                            display_comma_separated(columns),
                            display_comma_separated(on)
                        )?;
                        if *stored_as_directories {
                            write!(f, " STORED AS DIRECTORIES")?;
                        }
                    }
                    _ => (),
                }

                if let Some(HiveFormat {
                    row_format,
                    storage,
                    location,
                }) = hive_formats
                {
                    match row_format {
                        Some(HiveRowFormat::SERDE { class }) => {
                            write!(f, " ROW FORMAT SERDE '{}'", class)?
                        }
                        Some(HiveRowFormat::DELIMITED) => write!(f, " ROW FORMAT DELIMITED")?,
                        None => (),
                    }
                    match storage {
                        Some(HiveIOFormat::IOF {
                            input_format,
                            output_format,
                        }) => write!(
                            f,
                            " STORED AS INPUTFORMAT {} OUTPUTFORMAT {}",
                            input_format, output_format
                        )?,
                        Some(HiveIOFormat::FileFormat { format }) if !*external => {
                            write!(f, " STORED AS {}", format)?
                        }
                        _ => (),
                    }
                    if !*external {
                        if let Some(loc) = location {
                            write!(f, " LOCATION '{}'", loc)?;
                        }
                    }
                }
                if *external {
                    write!(
                        f,
                        " STORED AS {} LOCATION '{}'",
                        file_format.as_ref().unwrap(),
                        location.as_ref().unwrap()
                    )?;
                }
                if !table_properties.is_empty() {
                    write!(
                        f,
                        " TBLPROPERTIES ({})",
                        display_comma_separated(table_properties)
                    )?;
                }
                if !with_options.is_empty() {
                    write!(f, " WITH ({})", display_comma_separated(with_options))?;
                }
                if let Some(query) = query {
                    write!(f, " AS {}", query)?;
                }
                if let Some(engine) = engine {
                    write!(f, " ENGINE={}", engine)?;
                }
                if let Some(default_charset) = default_charset {
                    write!(f, " DEFAULT CHARSET={}", default_charset)?;
                }
                if let Some(collation) = collation {
                    write!(f, " COLLATE={}", collation)?;
                }

                if on_commit.is_some() {
                    let on_commit = match on_commit {
                        Some(OnCommit::DeleteRows) => "ON COMMIT DELETE ROWS",
                        Some(OnCommit::PreserveRows) => "ON COMMIT PRESERVE ROWS",
                        Some(OnCommit::Drop) => "ON COMMIT DROP",
                        None => "",
                    };
                    write!(f, " {}", on_commit)?;
                }

                Ok(())
            }
            Statement::CreateVirtualTable {
                name,
                if_not_exists,
                module_name,
                module_args,
            } => {
                write!(
                    f,
                    "CREATE VIRTUAL TABLE {if_not_exists}{name} USING {module_name}",
                    if_not_exists = if *if_not_exists { "IF NOT EXISTS " } else { "" },
                    name = name,
                    module_name = module_name
                )?;
                if !module_args.is_empty() {
                    write!(f, " ({})", display_comma_separated(module_args))?;
                }
                Ok(())
            }
            Statement::CreateIndex {
                name,
                table_name,
                columns,
                unique,
                if_not_exists,
            } => write!(
                f,
                "CREATE {unique}INDEX {if_not_exists}{name} ON {table_name}({columns})",
                unique = if *unique { "UNIQUE " } else { "" },
                if_not_exists = if *if_not_exists { "IF NOT EXISTS " } else { "" },
                name = name,
                table_name = table_name,
                columns = display_separated(columns, ",")
            ),
            Statement::AlterTable { name, operation } => {
                write!(f, "ALTER TABLE {} {}", name, operation)
            }
            Statement::Drop {
                object_type,
                if_exists,
                names,
                cascade,
                purge,
            } => write!(
                f,
                "DROP {}{} {}{}{}",
                object_type,
                if *if_exists { " IF EXISTS" } else { "" },
                display_comma_separated(names),
                if *cascade { " CASCADE" } else { "" },
                if *purge { " PURGE" } else { "" }
            ),
            Statement::Discard { object_type } => {
                write!(f, "DISCARD {object_type}", object_type = object_type)?;
                Ok(())
            }
            Statement::SetRole {
                local,
                session,
                role_name,
            } => {
                write!(
                    f,
                    "SET {local}{session}ROLE",
                    local = if *local { "LOCAL " } else { "" },
                    session = if *session { "SESSION " } else { "" },
                )?;
                if let Some(role_name) = role_name {
                    write!(f, " {}", role_name)?;
                } else {
                    f.write_str(" NONE")?;
                }
                Ok(())
            }
            Statement::SetVariable {
                local,
                variable,
                hivevar,
                value,
            } => {
                f.write_str("SET ")?;
                if *local {
                    f.write_str("LOCAL ")?;
                }
                write!(
                    f,
                    "{hivevar}{name} = {value}",
                    hivevar = if *hivevar { "HIVEVAR:" } else { "" },
                    name = variable,
                    value = display_comma_separated(value)
                )
            }
            Statement::SetNames {
                charset_name,
                collation_name,
            } => {
                f.write_str("SET NAMES ")?;
                f.write_str(charset_name)?;

                if let Some(collation) = collation_name {
                    f.write_str(" COLLATE ")?;
                    f.write_str(collation)?;
                };

                Ok(())
            }
            Statement::SetNamesDefault {} => {
                f.write_str("SET NAMES DEFAULT")?;

                Ok(())
            }
            Statement::ShowVariable { variable } => {
                write!(f, "SHOW")?;
                if !variable.is_empty() {
                    write!(f, " {}", display_separated(variable, " "))?;
                }
                Ok(())
            }
            Statement::ShowVariables { filter } => {
                write!(f, "SHOW VARIABLES")?;
                if filter.is_some() {
                    write!(f, " {}", filter.as_ref().unwrap())?;
                }
                Ok(())
            }
            Statement::ShowCreate { obj_type, obj_name } => {
                write!(
                    f,
                    "SHOW CREATE {obj_type} {obj_name}",
                    obj_type = obj_type,
                    obj_name = obj_name,
                )?;
                Ok(())
            }
            Statement::ShowColumns {
                extended,
                full,
                table_name,
                filter,
            } => {
                write!(
                    f,
                    "SHOW {extended}{full}COLUMNS FROM {table_name}",
                    extended = if *extended { "EXTENDED " } else { "" },
                    full = if *full { "FULL " } else { "" },
                    table_name = table_name,
                )?;
                if let Some(filter) = filter {
                    write!(f, " {}", filter)?;
                }
                Ok(())
            }
            Statement::ShowTables {
                extended,
                full,
                db_name,
                filter,
            } => {
                write!(
                    f,
                    "SHOW {extended}{full}TABLES",
                    extended = if *extended { "EXTENDED " } else { "" },
                    full = if *full { "FULL " } else { "" },
                )?;
                if let Some(db_name) = db_name {
                    write!(f, " FROM {}", db_name)?;
                }
                if let Some(filter) = filter {
                    write!(f, " {}", filter)?;
                }
                Ok(())
            }
            Statement::Use { db_name } => {
                write!(f, "USE {}", db_name)?;
                Ok(())
            }
            Statement::ShowCollation { filter } => {
                write!(f, "SHOW COLLATION")?;
                if let Some(filter) = filter {
                    write!(f, " {}", filter)?;
                }
                Ok(())
            }
            Statement::StartTransaction { modes } => {
                write!(f, "START TRANSACTION")?;
                if !modes.is_empty() {
                    write!(f, " {}", display_comma_separated(modes))?;
                }
                Ok(())
            }
            Statement::SetTransaction {
                modes,
                snapshot,
                session,
            } => {
                if *session {
                    write!(f, "SET SESSION CHARACTERISTICS AS TRANSACTION")?;
                } else {
                    write!(f, "SET TRANSACTION")?;
                }
                if !modes.is_empty() {
                    write!(f, " {}", display_comma_separated(modes))?;
                }
                if let Some(snapshot_id) = snapshot {
                    write!(f, " SNAPSHOT {}", snapshot_id)?;
                }
                Ok(())
            }
            Statement::Commit { chain } => {
                write!(f, "COMMIT{}", if *chain { " AND CHAIN" } else { "" },)
            }
            Statement::Rollback { chain } => {
                write!(f, "ROLLBACK{}", if *chain { " AND CHAIN" } else { "" },)
            }
            Statement::CreateSchema {
                schema_name,
                if_not_exists,
            } => write!(
                f,
                "CREATE SCHEMA {if_not_exists}{name}",
                if_not_exists = if *if_not_exists { "IF NOT EXISTS " } else { "" },
                name = schema_name
            ),
            Statement::Assert { condition, message } => {
                write!(f, "ASSERT {}", condition)?;
                if let Some(m) = message {
                    write!(f, " AS {}", m)?;
                }
                Ok(())
            }
            Statement::Grant {
                privileges,
                objects,
                grantees,
                with_grant_option,
                granted_by,
            } => {
                write!(f, "GRANT {} ", privileges)?;
                write!(f, "ON {} ", objects)?;
                write!(f, "TO {}", display_comma_separated(grantees))?;
                if *with_grant_option {
                    write!(f, " WITH GRANT OPTION")?;
                }
                if let Some(grantor) = granted_by {
                    write!(f, " GRANTED BY {}", grantor)?;
                }
                Ok(())
            }
            Statement::Revoke {
                privileges,
                objects,
                grantees,
                granted_by,
                cascade,
            } => {
                write!(f, "REVOKE {} ", privileges)?;
                write!(f, "ON {} ", objects)?;
                write!(f, "FROM {}", display_comma_separated(grantees))?;
                if let Some(grantor) = granted_by {
                    write!(f, " GRANTED BY {}", grantor)?;
                }
                write!(f, " {}", if *cascade { "CASCADE" } else { "RESTRICT" })?;
                Ok(())
            }
            Statement::Deallocate { name, prepare } => write!(
                f,
                "DEALLOCATE {prepare}{name}",
                prepare = if *prepare { "PREPARE " } else { "" },
                name = name,
            ),
            Statement::Execute { name, parameters } => {
                write!(f, "EXECUTE {}", name)?;
                if !parameters.is_empty() {
                    write!(f, "({})", display_comma_separated(parameters))?;
                }
                Ok(())
            }
            Statement::Prepare {
                name,
                data_types,
                statement,
            } => {
                write!(f, "PREPARE {} ", name)?;
                if !data_types.is_empty() {
                    write!(f, "({}) ", display_comma_separated(data_types))?;
                }
                write!(f, "AS {}", statement)
            }
            Statement::Comment {
                object_type,
                object_name,
                comment,
            } => {
                write!(f, "COMMENT ON {} {} IS ", object_type, object_name)?;
                if let Some(c) = comment {
                    write!(f, "'{}'", c)
                } else {
                    write!(f, "NULL")
                }
            }
            Statement::Savepoint { name } => {
                write!(f, "SAVEPOINT ")?;
                write!(f, "{}", name)
            }
            Statement::Merge {
                into,
                table,
                source,
                on,
                clauses,
            } => {
                write!(
                    f,
                    "MERGE{int} {table} USING {source} ",
                    int = if *into { " INTO" } else { "" }
                )?;
                write!(f, "ON {} ", on)?;
                write!(f, "{}", display_separated(clauses, " "))
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[non_exhaustive]
pub enum OnInsert {
    /// ON DUPLICATE KEY UPDATE (MySQL when the key already exists, then execute an update instead)
    DuplicateKeyUpdate(Vec<Assignment>),
}

impl fmt::Display for OnInsert {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::DuplicateKeyUpdate(expr) => write!(
                f,
                " ON DUPLICATE KEY UPDATE {}",
                display_comma_separated(expr)
            ),
        }
    }
}

/// Privileges granted in a GRANT statement or revoked in a REVOKE statement.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum Privileges {
    /// All privileges applicable to the object type
    All {
        /// Optional keyword from the spec, ignored in practice
        with_privileges_keyword: bool,
    },
    /// Specific privileges (e.g. `SELECT`, `INSERT`)
    Actions(Vec<Action>),
}

impl fmt::Display for Privileges {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Privileges::All {
                with_privileges_keyword,
            } => {
                write!(
                    f,
                    "ALL{}",
                    if *with_privileges_keyword {
                        " PRIVILEGES"
                    } else {
                        ""
                    }
                )
            }
            Privileges::Actions(actions) => {
                write!(f, "{}", display_comma_separated(actions))
            }
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

impl fmt::Display for FetchDirection {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            FetchDirection::Count { limit } => f.write_str(&limit.to_string())?,
            FetchDirection::Next => f.write_str("NEXT")?,
            FetchDirection::Prior => f.write_str("PRIOR")?,
            FetchDirection::First => f.write_str("FIRST")?,
            FetchDirection::Last => f.write_str("LAST")?,
            FetchDirection::Absolute { limit } => {
                f.write_str("ABSOLUTE ")?;
                f.write_str(&limit.to_string())?;
            }
            FetchDirection::Relative { limit } => {
                f.write_str("RELATIVE ")?;
                f.write_str(&limit.to_string())?;
            }
            FetchDirection::All => f.write_str("ALL")?,
            FetchDirection::Forward { limit } => {
                f.write_str("FORWARD")?;

                if let Some(l) = limit {
                    f.write_str(" ")?;
                    f.write_str(&l.to_string())?;
                }
            }
            FetchDirection::ForwardAll => f.write_str("FORWARD ALL")?,
            FetchDirection::Backward { limit } => {
                f.write_str("BACKWARD")?;

                if let Some(l) = limit {
                    f.write_str(" ")?;
                    f.write_str(&l.to_string())?;
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

impl fmt::Display for Action {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
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
                    write!(f, " ({})", display_comma_separated(columns))?;
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

impl fmt::Display for GrantObjects {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            GrantObjects::Sequences(sequences) => {
                write!(f, "SEQUENCE {}", display_comma_separated(sequences))
            }
            GrantObjects::Schemas(schemas) => {
                write!(f, "SCHEMA {}", display_comma_separated(schemas))
            }
            GrantObjects::Tables(tables) => {
                write!(f, "{}", display_comma_separated(tables))
            }
            GrantObjects::AllSequencesInSchema { schemas } => {
                write!(
                    f,
                    "ALL SEQUENCES IN SCHEMA {}",
                    display_comma_separated(schemas)
                )
            }
            GrantObjects::AllTablesInSchema { schemas } => {
                write!(
                    f,
                    "ALL TABLES IN SCHEMA {}",
                    display_comma_separated(schemas)
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

impl fmt::Display for Assignment {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} = {}", display_separated(&self.id, "."), self.value)
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

impl fmt::Display for FunctionArgExpr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            FunctionArgExpr::Expr(expr) => write!(f, "{}", expr),
            FunctionArgExpr::QualifiedWildcard(prefix) => write!(f, "{}.*", prefix),
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

impl fmt::Display for FunctionArg {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            FunctionArg::Named { name, arg } => write!(f, "{} => {}", name, arg),
            FunctionArg::Unnamed(unnamed_arg) => write!(f, "{}", unnamed_arg),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum CloseCursor {
    All,
    Specific { name: Ident },
}

impl fmt::Display for CloseCursor {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            CloseCursor::All => write!(f, "ALL"),
            CloseCursor::Specific { name } => write!(f, "{}", name),
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
    // Some functions must be called without trailing parentheses, for example Postgres
    // do it for current_catalog, current_schema, etc. This flags is used for formatting.
    pub special: bool,
}

impl fmt::Display for Function {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if self.special {
            write!(f, "{}", self.name)?;
        } else {
            write!(
                f,
                "{}({}{})",
                self.name,
                if self.distinct { "DISTINCT " } else { "" },
                display_comma_separated(&self.args),
            )?;

            if let Some(o) = &self.over {
                write!(f, " OVER ({})", o)?;
            }
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

impl fmt::Display for FileFormat {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
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

impl fmt::Display for ListAgg {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "LISTAGG({}{}",
            if self.distinct { "DISTINCT " } else { "" },
            self.expr
        )?;
        if let Some(separator) = &self.separator {
            write!(f, ", {}", separator)?;
        }
        if let Some(on_overflow) = &self.on_overflow {
            write!(f, "{}", on_overflow)?;
        }
        write!(f, ")")?;
        if !self.within_group.is_empty() {
            write!(
                f,
                " WITHIN GROUP (ORDER BY {})",
                display_comma_separated(&self.within_group)
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

impl fmt::Display for ListAggOnOverflow {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, " ON OVERFLOW")?;
        match self {
            ListAggOnOverflow::Error => write!(f, " ERROR"),
            ListAggOnOverflow::Truncate { filler, with_count } => {
                write!(f, " TRUNCATE")?;
                if let Some(filler) = filler {
                    write!(f, " {}", filler)?;
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

impl fmt::Display for ObjectType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
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

impl fmt::Display for KillType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
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
pub enum HiveDistributionStyle {
    PARTITIONED {
        columns: Vec<ColumnDef>,
    },
    CLUSTERED {
        columns: Vec<Ident>,
        sorted_by: Vec<ColumnDef>,
        num_buckets: i32,
    },
    SKEWED {
        columns: Vec<ColumnDef>,
        on: Vec<ColumnDef>,
        stored_as_directories: bool,
    },
    NONE,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum HiveRowFormat {
    SERDE { class: String },
    DELIMITED,
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[allow(clippy::large_enum_variant)]
pub enum HiveIOFormat {
    IOF {
        input_format: Expr,
        output_format: Expr,
    },
    FileFormat {
        format: FileFormat,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct HiveFormat {
    pub row_format: Option<HiveRowFormat>,
    pub storage: Option<HiveIOFormat>,
    pub location: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct SqlOption {
    pub name: Ident,
    pub value: Value,
}

impl fmt::Display for SqlOption {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} = {}", self.name, self.value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum TransactionMode {
    AccessMode(TransactionAccessMode),
    IsolationLevel(TransactionIsolationLevel),
}

impl fmt::Display for TransactionMode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use TransactionMode::*;
        match self {
            AccessMode(access_mode) => write!(f, "{}", access_mode),
            IsolationLevel(iso_level) => write!(f, "ISOLATION LEVEL {}", iso_level),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum TransactionAccessMode {
    ReadOnly,
    ReadWrite,
}

impl fmt::Display for TransactionAccessMode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
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

impl fmt::Display for TransactionIsolationLevel {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
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

impl fmt::Display for ShowStatementFilter {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use ShowStatementFilter::*;
        match self {
            Like(pattern) => write!(f, "LIKE '{}'", value::escape_single_quote_string(pattern)),
            ILike(pattern) => write!(f, "ILIKE {}", value::escape_single_quote_string(pattern)),
            Where(expr) => write!(f, "WHERE {}", expr),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum SetVariableValue {
    Ident(Ident),
    Literal(Value),
}

impl fmt::Display for SetVariableValue {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use SetVariableValue::*;
        match self {
            Ident(ident) => write!(f, "{}", ident),
            Literal(literal) => write!(f, "{}", literal),
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

impl fmt::Display for SqliteOnConflict {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
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

impl fmt::Display for CopyTarget {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use CopyTarget::*;
        match self {
            Stdin { .. } => write!(f, "STDIN"),
            Stdout => write!(f, "STDOUT"),
            File { filename } => write!(f, "'{}'", value::escape_single_quote_string(filename)),
            Program { command } => write!(
                f,
                "PROGRAM '{}'",
                value::escape_single_quote_string(command)
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

impl fmt::Display for CopyOption {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use CopyOption::*;
        match self {
            Format(name) => write!(f, "FORMAT {}", name),
            Freeze(true) => write!(f, "FREEZE"),
            Freeze(false) => write!(f, "FREEZE FALSE"),
            Delimiter(char) => write!(f, "DELIMITER '{}'", char),
            Null(string) => write!(f, "NULL '{}'", value::escape_single_quote_string(string)),
            Header(true) => write!(f, "HEADER"),
            Header(false) => write!(f, "HEADER FALSE"),
            Quote(char) => write!(f, "QUOTE '{}'", char),
            Escape(char) => write!(f, "ESCAPE '{}'", char),
            ForceQuote(columns) => write!(f, "FORCE_QUOTE ({})", display_comma_separated(columns)),
            ForceNotNull(columns) => {
                write!(f, "FORCE_NOT_NULL ({})", display_comma_separated(columns))
            }
            ForceNull(columns) => write!(f, "FORCE_NULL ({})", display_comma_separated(columns)),
            Encoding(name) => write!(f, "ENCODING '{}'", value::escape_single_quote_string(name)),
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

impl fmt::Display for CopyLegacyOption {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use CopyLegacyOption::*;
        match self {
            Binary => write!(f, "BINARY"),
            Delimiter(char) => write!(f, "DELIMITER '{}'", char),
            Null(string) => write!(f, "NULL '{}'", value::escape_single_quote_string(string)),
            Csv(opts) => write!(f, "CSV {}", display_separated(opts, " ")),
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

impl fmt::Display for CopyLegacyCsvOption {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use CopyLegacyCsvOption::*;
        match self {
            Header => write!(f, "HEADER"),
            Quote(char) => write!(f, "QUOTE '{}'", char),
            Escape(char) => write!(f, "ESCAPE '{}'", char),
            ForceQuote(columns) => write!(f, "FORCE QUOTE {}", display_comma_separated(columns)),
            ForceNotNull(columns) => {
                write!(f, "FORCE NOT NULL {}", display_comma_separated(columns))
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

impl fmt::Display for MergeClause {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use MergeClause::*;
        write!(f, "WHEN")?;
        match self {
            MatchedUpdate {
                predicate,
                assignments,
            } => {
                write!(f, " MATCHED")?;
                if let Some(pred) = predicate {
                    write!(f, " AND {}", pred)?;
                }
                write!(
                    f,
                    " THEN UPDATE SET {}",
                    display_comma_separated(assignments)
                )
            }
            MatchedDelete(predicate) => {
                write!(f, " MATCHED")?;
                if let Some(pred) = predicate {
                    write!(f, " AND {}", pred)?;
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
                    write!(f, " AND {}", pred)?;
                }
                write!(
                    f,
                    " THEN INSERT ({}) {}",
                    display_comma_separated(columns),
                    values
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

impl fmt::Display for DiscardObject {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
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

impl fmt::Display for CreateFunctionUsing {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
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
        // a and b in different group
        let grouping_sets = Expr::GroupingSets(vec![
            vec![Expr::Identifier(Ident::new("a"))],
            vec![Expr::Identifier(Ident::new("b"))],
        ]);
        assert_eq!("GROUPING SETS ((a), (b))", format!("{}", grouping_sets));

        // a and b in the same group
        let grouping_sets = Expr::GroupingSets(vec![vec![
            Expr::Identifier(Ident::new("a")),
            Expr::Identifier(Ident::new("b")),
        ]]);
        assert_eq!("GROUPING SETS ((a, b))", format!("{}", grouping_sets));

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
            format!("{}", grouping_sets)
        );
    }

    #[test]
    fn test_rollup_display() {
        let rollup = Expr::Rollup(vec![vec![Expr::Identifier(Ident::new("a"))]]);
        assert_eq!("ROLLUP (a)", format!("{}", rollup));

        let rollup = Expr::Rollup(vec![vec![
            Expr::Identifier(Ident::new("a")),
            Expr::Identifier(Ident::new("b")),
        ]]);
        assert_eq!("ROLLUP ((a, b))", format!("{}", rollup));

        let rollup = Expr::Rollup(vec![
            vec![Expr::Identifier(Ident::new("a"))],
            vec![Expr::Identifier(Ident::new("b"))],
        ]);
        assert_eq!("ROLLUP (a, b)", format!("{}", rollup));

        let rollup = Expr::Rollup(vec![
            vec![Expr::Identifier(Ident::new("a"))],
            vec![
                Expr::Identifier(Ident::new("b")),
                Expr::Identifier(Ident::new("c")),
            ],
            vec![Expr::Identifier(Ident::new("d"))],
        ]);
        assert_eq!("ROLLUP (a, (b, c), d)", format!("{}", rollup));
    }

    #[test]
    fn test_cube_display() {
        let cube = Expr::Cube(vec![vec![Expr::Identifier(Ident::new("a"))]]);
        assert_eq!("CUBE (a)", format!("{}", cube));

        let cube = Expr::Cube(vec![vec![
            Expr::Identifier(Ident::new("a")),
            Expr::Identifier(Ident::new("b")),
        ]]);
        assert_eq!("CUBE ((a, b))", format!("{}", cube));

        let cube = Expr::Cube(vec![
            vec![Expr::Identifier(Ident::new("a"))],
            vec![Expr::Identifier(Ident::new("b"))],
        ]);
        assert_eq!("CUBE (a, b)", format!("{}", cube));

        let cube = Expr::Cube(vec![
            vec![Expr::Identifier(Ident::new("a"))],
            vec![
                Expr::Identifier(Ident::new("b")),
                Expr::Identifier(Ident::new("c")),
            ],
            vec![Expr::Identifier(Ident::new("d"))],
        ]);
        assert_eq!("CUBE (a, (b, c), d)", format!("{}", cube));
    }
}

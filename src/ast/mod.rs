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

use crate::prelude::*;
use core::fmt;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

pub use self::data_type::DataType;
pub use self::ddl::{
    AlterTableOperation, ColumnDef, ColumnOption, ColumnOptionDef, ReferentialAction,
    TableConstraint,
};
pub use self::operator::{BinaryOperator, UnaryOperator};
pub use self::query::{
    Cte, Fetch, Join, JoinConstraint, JoinOperator, LateralView, Offset, OffsetRows, OrderByExpr,
    Query, Select, SelectItem, SetExpr, SetOperator, TableAlias, TableFactor, TableWithJoins, Top,
    Values, With,
};
pub use self::value::{DateTimeField, Value};

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
            Some(q) if q == '"' || q == '\'' || q == '`' => write!(f, "{}{}{}", q, self.value, q),
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
    /// Unary operation e.g. `NOT foo`
    UnaryOp {
        op: UnaryOperator,
        expr: Box<Expr>,
    },
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
    /// EXTRACT(DateTimeField FROM <expr>)
    Extract {
        field: DateTimeField,
        expr: Box<Expr>,
    },
    /// SUBSTRING(<expr> [FROM <expr>] [FOR <expr>])
    Substring {
        expr: Box<Expr>,
        substring_from: Option<Box<Expr>>,
        substring_for: Option<Box<Expr>>,
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
    TypedString {
        data_type: DataType,
        value: String,
    },
    MapAccess {
        column: Box<Expr>,
        key: String,
    },
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
    /// An exists expression `EXISTS(SELECT ...)`, used in expressions like
    /// `WHERE EXISTS (SELECT ...)`.
    Exists(Box<Query>),
    /// A parenthesized subquery `(SELECT ...)`, used in expression like
    /// `SELECT (subquery) AS x` or `WHERE (subquery) = x`
    Subquery(Box<Query>),
    /// The `LISTAGG` function `SELECT LISTAGG(...) WITHIN GROUP (ORDER BY ...)`
    ListAgg(ListAgg),
}

impl fmt::Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Expr::Identifier(s) => write!(f, "{}", s),
            Expr::MapAccess { column, key } => write!(f, "{}[\"{}\"]", column, key),
            Expr::Wildcard => f.write_str("*"),
            Expr::QualifiedWildcard(q) => write!(f, "{}.*", display_separated(q, ".")),
            Expr::CompoundIdentifier(s) => write!(f, "{}", display_separated(s, ".")),
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
            Expr::UnaryOp { op, expr } => {
                if op == &UnaryOperator::PGPostfixFactorial {
                    write!(f, "{}{}", expr, op)
                } else {
                    write!(f, "{} {}", op, expr)
                }
            }
            Expr::Cast { expr, data_type } => write!(f, "CAST({} AS {})", expr, data_type),
            Expr::TryCast { expr, data_type } => write!(f, "TRY_CAST({} AS {})", expr, data_type),
            Expr::Extract { field, expr } => write!(f, "EXTRACT({} FROM {})", field, expr),
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
            Expr::Exists(s) => write!(f, "EXISTS ({})", s),
            Expr::Subquery(s) => write!(f, "({})", s),
            Expr::ListAgg(listagg) => write!(f, "{}", listagg),
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
            if let Some(end_bound) = &window_frame.end_bound {
                f.write_str(delim)?;
                write!(
                    f,
                    "{} BETWEEN {} AND {}",
                    window_frame.units, window_frame.start_bound, end_bound
                )?;
            } else {
                f.write_str(delim)?;
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
        /// VALUES a vector of values to be copied
        values: Vec<Option<String>>,
    },
    /// UPDATE
    Update {
        /// TABLE
        table_name: ObjectName,
        /// Column assignments
        assignments: Vec<Assignment>,
        /// WHERE
        selection: Option<Expr>,
    },
    /// DELETE
    Delete {
        /// FROM
        table_name: ObjectName,
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
    /// SET <variable>
    ///
    /// Note: this is not a standard SQL statement, but it is supported by at
    /// least MySQL and PostgreSQL. Not all MySQL-specific syntatic forms are
    /// supported yet.
    SetVariable {
        local: bool,
        hivevar: bool,
        variable: Ident,
        value: Vec<SetVariableValue>,
    },
    /// SHOW <variable>
    ///
    /// Note: this is a PostgreSQL-specific statement.
    ShowVariable { variable: Vec<Ident> },
    /// SHOW COLUMNS
    ///
    /// Note: this is a MySQL-specific statement.
    ShowColumns {
        extended: bool,
        full: bool,
        table_name: ObjectName,
        filter: Option<ShowStatementFilter>,
    },
    /// `{ BEGIN [ TRANSACTION | WORK ] | START TRANSACTION } ...`
    StartTransaction { modes: Vec<TransactionMode> },
    /// `SET TRANSACTION ...`
    SetTransaction { modes: Vec<TransactionMode> },
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
    /// `ASSERT <condition> [AS <message>]`
    Assert {
        condition: Expr,
        message: Option<Expr>,
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
    /// EXPLAIN
    Explain {
        /// Carry out the command and show actual run times and other statistics.
        analyze: bool,
        // Display additional information regarding the plan.
        verbose: bool,
        /// A SQL query that specifies what to explain
        statement: Box<Statement>,
    },
}

impl fmt::Display for Statement {
    // Clippy thinks this function is too complicated, but it is painful to
    // split up without extracting structs for each `Statement` variant.
    #[allow(clippy::cognitive_complexity)]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Statement::Explain {
                verbose,
                analyze,
                statement,
            } => {
                write!(f, "EXPLAIN ")?;

                if *analyze {
                    write!(f, "ANALYZE ")?;
                }

                if *verbose {
                    write!(f, "VERBOSE ")?;
                }

                write!(f, "{}", statement)
            }
            Statement::Query(s) => write!(f, "{}", s),
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
                table_name,
                overwrite,
                partitioned,
                columns,
                after_columns,
                source,
                table,
            } => {
                if let Some(action) = or {
                    write!(f, "INSERT OR {} INTO {} ", action, table_name)?;
                } else {
                    write!(
                        f,
                        "INSERT {act}{tbl} {table_name} ",
                        table_name = table_name,
                        act = if *overwrite { "OVERWRITE" } else { "INTO" },
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
                write!(f, "{}", source)
            }

            Statement::Copy {
                table_name,
                columns,
                values,
            } => {
                write!(f, "COPY {}", table_name)?;
                if !columns.is_empty() {
                    write!(f, " ({})", display_comma_separated(columns))?;
                }
                write!(f, " FROM stdin; ")?;
                if !values.is_empty() {
                    writeln!(f)?;
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
                }
                write!(f, "\n\\.")
            }
            Statement::Update {
                table_name,
                assignments,
                selection,
            } => {
                write!(f, "UPDATE {}", table_name)?;
                if !assignments.is_empty() {
                    write!(f, " SET {}", display_comma_separated(assignments))?;
                }
                if let Some(selection) = selection {
                    write!(f, " WHERE {}", selection)?;
                }
                Ok(())
            }
            Statement::Delete {
                table_name,
                selection,
            } => {
                write!(f, "DELETE FROM {}", table_name)?;
                if let Some(selection) = selection {
                    write!(f, " WHERE {}", selection)?;
                }
                Ok(())
            }
            Statement::CreateDatabase {
                db_name,
                if_not_exists,
                location,
                managed_location,
            } => {
                write!(f, "CREATE")?;
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
                temporary,
                file_format,
                location,
                query,
                without_rowid,
                like,
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
                    "CREATE {or_replace}{external}{temporary}TABLE {if_not_exists}{name}",
                    or_replace = if *or_replace { "OR REPLACE " } else { "" },
                    external = if *external { "EXTERNAL " } else { "" },
                    if_not_exists = if *if_not_exists { "IF NOT EXISTS " } else { "" },
                    temporary = if *temporary { "TEMPORARY " } else { "" },
                    name = name,
                )?;
                if !columns.is_empty() || !constraints.is_empty() {
                    write!(f, " ({}", display_comma_separated(columns))?;
                    if !columns.is_empty() && !constraints.is_empty() {
                        write!(f, ", ")?;
                    }
                    write!(f, "{})", display_comma_separated(constraints))?;
                } else if query.is_none() && like.is_none() {
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
            Statement::ShowVariable { variable } => {
                write!(f, "SHOW")?;
                if !variable.is_empty() {
                    write!(f, " {}", display_separated(variable, " "))?;
                }
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
            Statement::StartTransaction { modes } => {
                write!(f, "START TRANSACTION")?;
                if !modes.is_empty() {
                    write!(f, " {}", display_comma_separated(modes))?;
                }
                Ok(())
            }
            Statement::SetTransaction { modes } => {
                write!(f, "SET TRANSACTION")?;
                if !modes.is_empty() {
                    write!(f, " {}", display_comma_separated(modes))?;
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
        }
    }
}

/// SQL assignment `foo = expr` as used in SQLUpdate
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Assignment {
    pub id: Ident,
    pub value: Expr,
}

impl fmt::Display for Assignment {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} = {}", self.id, self.value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum FunctionArg {
    Named { name: Ident, arg: Expr },
    Unnamed(Expr),
}

impl fmt::Display for FunctionArg {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            FunctionArg::Named { name, arg } => write!(f, "{} => {}", name, arg),
            FunctionArg::Unnamed(unnamed_arg) => write!(f, "{}", unnamed_arg),
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

impl fmt::Display for Function {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum HiveIOFormat {
    IOF {
        input_format: Expr,
        output_format: Expr,
    },
    FileFormat {
        format: FileFormat,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct HiveFormat {
    pub row_format: Option<HiveRowFormat>,
    pub storage: Option<HiveIOFormat>,
    pub location: Option<String>,
}

impl Default for HiveFormat {
    fn default() -> Self {
        HiveFormat {
            row_format: None,
            location: None,
            storage: None,
        }
    }
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

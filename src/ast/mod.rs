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

use std::ops::Deref;

pub use self::data_type::DataType;
pub use self::ddl::{
    AlterTableOperation, ColumnDef, ColumnOption, ColumnOptionDef, TableConstraint,
};
pub use self::operator::{BinaryOperator, UnaryOperator};
pub use self::query::{
    Cte, Fetch, Join, JoinConstraint, JoinOperator, OrderByExpr, Query, Select, SelectItem,
    SetExpr, SetOperator, TableAlias, TableFactor, TableWithJoins, Values,
};
pub use self::value::{SQLDateTimeField, Value};

/// Like `vec.join(", ")`, but for any types implementing ToString.
fn comma_separated_string<I>(iter: I) -> String
where
    I: IntoIterator,
    I::Item: Deref,
    <I::Item as Deref>::Target: ToString,
{
    iter.into_iter()
        .map(|t| t.deref().to_string())
        .collect::<Vec<String>>()
        .join(", ")
}

/// Identifier name, in the originally quoted form (e.g. `"id"`)
pub type Ident = String;

/// An SQL expression of any type.
///
/// The parser does not distinguish between expressions of different types
/// (e.g. boolean vs string), so the caller must handle expressions of
/// inappropriate type, like `WHERE 1` or `SELECT 1=1`, as necessary.
#[derive(Debug, Clone, PartialEq, Hash)]
pub enum Expr {
    /// Identifier e.g. table name or column name
    Identifier(Ident),
    /// Unqualified wildcard (`*`). SQL allows this in limited contexts, such as:
    /// - right after `SELECT` (which is represented as a [SQLSelectItem::Wildcard] instead)
    /// - or as part of an aggregate function, e.g. `COUNT(*)`,
    ///
    /// ...but we currently also accept it in contexts where it doesn't make
    /// sense, such as `* + *`
    Wildcard,
    /// Qualified wildcard, e.g. `alias.*` or `schema.table.*`.
    /// (Same caveats apply to SQLQualifiedWildcard as to SQLWildcard.)
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
    UnaryOp { op: UnaryOperator, expr: Box<Expr> },
    /// CAST an expression to a different data type e.g. `CAST(foo AS VARCHAR(123))`
    Cast {
        expr: Box<Expr>,
        data_type: DataType,
    },
    Extract {
        field: SQLDateTimeField,
        expr: Box<Expr>,
    },
    /// `expr COLLATE collation`
    Collate {
        expr: Box<Expr>,
        collation: ObjectName,
    },
    /// Nested expression e.g. `(foo > bar)` or `(1)`
    Nested(Box<Expr>),
    /// SQLValue
    Value(Value),
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
}

impl ToString for Expr {
    fn to_string(&self) -> String {
        match self {
            Expr::Identifier(s) => s.to_string(),
            Expr::Wildcard => "*".to_string(),
            Expr::QualifiedWildcard(q) => q.join(".") + ".*",
            Expr::CompoundIdentifier(s) => s.join("."),
            Expr::IsNull(ast) => format!("{} IS NULL", ast.as_ref().to_string()),
            Expr::IsNotNull(ast) => format!("{} IS NOT NULL", ast.as_ref().to_string()),
            Expr::InList {
                expr,
                list,
                negated,
            } => format!(
                "{} {}IN ({})",
                expr.as_ref().to_string(),
                if *negated { "NOT " } else { "" },
                comma_separated_string(list)
            ),
            Expr::InSubquery {
                expr,
                subquery,
                negated,
            } => format!(
                "{} {}IN ({})",
                expr.as_ref().to_string(),
                if *negated { "NOT " } else { "" },
                subquery.to_string()
            ),
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => format!(
                "{} {}BETWEEN {} AND {}",
                expr.to_string(),
                if *negated { "NOT " } else { "" },
                low.to_string(),
                high.to_string()
            ),
            Expr::BinaryOp { left, op, right } => format!(
                "{} {} {}",
                left.as_ref().to_string(),
                op.to_string(),
                right.as_ref().to_string()
            ),
            Expr::UnaryOp { op, expr } => {
                format!("{} {}", op.to_string(), expr.as_ref().to_string())
            }
            Expr::Cast { expr, data_type } => format!(
                "CAST({} AS {})",
                expr.as_ref().to_string(),
                data_type.to_string()
            ),
            Expr::Extract { field, expr } => {
                format!("EXTRACT({} FROM {})", field.to_string(), expr.to_string())
            }
            Expr::Collate { expr, collation } => format!(
                "{} COLLATE {}",
                expr.as_ref().to_string(),
                collation.to_string()
            ),
            Expr::Nested(ast) => format!("({})", ast.as_ref().to_string()),
            Expr::Value(v) => v.to_string(),
            Expr::Function(f) => f.to_string(),
            Expr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => {
                let mut s = "CASE".to_string();
                if let Some(operand) = operand {
                    s += &format!(" {}", operand.to_string());
                }
                s += &conditions
                    .iter()
                    .zip(results)
                    .map(|(c, r)| format!(" WHEN {} THEN {}", c.to_string(), r.to_string()))
                    .collect::<Vec<String>>()
                    .join("");
                if let Some(else_result) = else_result {
                    s += &format!(" ELSE {}", else_result.to_string())
                }
                s + " END"
            }
            Expr::Exists(s) => format!("EXISTS ({})", s.to_string()),
            Expr::Subquery(s) => format!("({})", s.to_string()),
        }
    }
}

/// A window specification (i.e. `OVER (PARTITION BY .. ORDER BY .. etc.)`)
#[derive(Debug, Clone, PartialEq, Hash)]
pub struct WindowSpec {
    pub partition_by: Vec<Expr>,
    pub order_by: Vec<OrderByExpr>,
    pub window_frame: Option<WindowFrame>,
}

impl ToString for WindowSpec {
    fn to_string(&self) -> String {
        let mut clauses = vec![];
        if !self.partition_by.is_empty() {
            clauses.push(format!(
                "PARTITION BY {}",
                comma_separated_string(&self.partition_by)
            ))
        };
        if !self.order_by.is_empty() {
            clauses.push(format!(
                "ORDER BY {}",
                comma_separated_string(&self.order_by)
            ))
        };
        if let Some(window_frame) = &self.window_frame {
            if let Some(end_bound) = &window_frame.end_bound {
                clauses.push(format!(
                    "{} BETWEEN {} AND {}",
                    window_frame.units.to_string(),
                    window_frame.start_bound.to_string(),
                    end_bound.to_string()
                ));
            } else {
                clauses.push(format!(
                    "{} {}",
                    window_frame.units.to_string(),
                    window_frame.start_bound.to_string()
                ));
            }
        }
        clauses.join(" ")
    }
}

/// Specifies the data processed by a window function, e.g.
/// `RANGE UNBOUNDED PRECEDING` or `ROWS BETWEEN 5 PRECEDING AND CURRENT ROW`.
#[derive(Debug, Clone, PartialEq, Hash)]
pub struct WindowFrame {
    pub units: WindowFrameUnits,
    pub start_bound: WindowFrameBound,
    /// The right bound of the `BETWEEN .. AND` clause.
    pub end_bound: Option<WindowFrameBound>,
    // TBD: EXCLUDE
}

#[derive(Debug, Clone, PartialEq, Hash)]
pub enum WindowFrameUnits {
    Rows,
    Range,
    Groups,
}

impl ToString for WindowFrameUnits {
    fn to_string(&self) -> String {
        match self {
            WindowFrameUnits::Rows => "ROWS".to_string(),
            WindowFrameUnits::Range => "RANGE".to_string(),
            WindowFrameUnits::Groups => "GROUPS".to_string(),
        }
    }
}

impl FromStr for WindowFrameUnits {
    type Err = ParserError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "ROWS" => Ok(WindowFrameUnits::Rows),
            "RANGE" => Ok(WindowFrameUnits::Range),
            "GROUPS" => Ok(WindowFrameUnits::Groups),
            _ => Err(ParserError::ParserError(format!(
                "Expected ROWS, RANGE, or GROUPS, found: {}",
                s
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Hash)]
pub enum WindowFrameBound {
    /// "CURRENT ROW"
    CurrentRow,
    /// "<N> PRECEDING" or "UNBOUNDED PRECEDING"
    Preceding(Option<u64>),
    /// "<N> FOLLOWING" or "UNBOUNDED FOLLOWING". This can only appear in
    /// SQLWindowFrame::end_bound.
    Following(Option<u64>),
}

impl ToString for WindowFrameBound {
    fn to_string(&self) -> String {
        match self {
            WindowFrameBound::CurrentRow => "CURRENT ROW".to_string(),
            WindowFrameBound::Preceding(None) => "UNBOUNDED PRECEDING".to_string(),
            WindowFrameBound::Following(None) => "UNBOUNDED FOLLOWING".to_string(),
            WindowFrameBound::Preceding(Some(n)) => format!("{} PRECEDING", n),
            WindowFrameBound::Following(Some(n)) => format!("{} FOLLOWING", n),
        }
    }
}

/// A top-level statement (SELECT, INSERT, CREATE, etc.)
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq, Hash)]
pub enum Statement {
    /// SELECT
    Query(Box<Query>),
    /// INSERT
    Insert {
        /// TABLE
        table_name: ObjectName,
        /// COLUMNS
        columns: Vec<Ident>,
        /// A SQL query that specifies what to insert
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
        /// View name
        name: ObjectName,
        columns: Vec<Ident>,
        query: Box<Query>,
        materialized: bool,
        with_options: Vec<SqlOption>,
    },
    /// CREATE TABLE
    CreateTable {
        /// Table name
        name: ObjectName,
        /// Optional schema
        columns: Vec<ColumnDef>,
        constraints: Vec<TableConstraint>,
        with_options: Vec<SqlOption>,
        external: bool,
        file_format: Option<FileFormat>,
        location: Option<String>,
    },
    /// ALTER TABLE
    AlterTable {
        /// Table name
        name: ObjectName,
        operation: AlterTableOperation,
    },
    /// DROP TABLE
    Drop {
        object_type: ObjectType,
        if_exists: bool,
        names: Vec<ObjectName>,
        cascade: bool,
    },
    /// `{ BEGIN [ TRANSACTION | WORK ] | START TRANSACTION } ...`
    StartTransaction { modes: Vec<TransactionMode> },
    /// `SET TRANSACTION ...`
    SetTransaction { modes: Vec<TransactionMode> },
    /// `COMMIT [ TRANSACTION | WORK ] [ AND [ NO ] CHAIN ]`
    Commit { chain: bool },
    /// `ROLLBACK [ TRANSACTION | WORK ] [ AND [ NO ] CHAIN ]`
    Rollback { chain: bool },
}

impl ToString for Statement {
    fn to_string(&self) -> String {
        match self {
            Statement::Query(s) => s.to_string(),
            Statement::Insert {
                table_name,
                columns,
                source,
            } => {
                let mut s = format!("INSERT INTO {} ", table_name.to_string());
                if !columns.is_empty() {
                    s += &format!("({}) ", columns.join(", "));
                }
                s += &source.to_string();
                s
            }
            Statement::Copy {
                table_name,
                columns,
                values,
            } => {
                let mut s = format!("COPY {}", table_name.to_string());
                if !columns.is_empty() {
                    s += &format!(" ({})", comma_separated_string(columns));
                }
                s += " FROM stdin; ";
                if !values.is_empty() {
                    s += &format!(
                        "\n{}",
                        values
                            .iter()
                            .map(|v| v.clone().unwrap_or_else(|| "\\N".to_string()))
                            .collect::<Vec<String>>()
                            .join("\t")
                    );
                }
                s += "\n\\.";
                s
            }
            Statement::Update {
                table_name,
                assignments,
                selection,
            } => {
                let mut s = format!("UPDATE {}", table_name.to_string());
                if !assignments.is_empty() {
                    s += " SET ";
                    s += &comma_separated_string(assignments);
                }
                if let Some(selection) = selection {
                    s += &format!(" WHERE {}", selection.to_string());
                }
                s
            }
            Statement::Delete {
                table_name,
                selection,
            } => {
                let mut s = format!("DELETE FROM {}", table_name.to_string());
                if let Some(selection) = selection {
                    s += &format!(" WHERE {}", selection.to_string());
                }
                s
            }
            Statement::CreateView {
                name,
                columns,
                query,
                materialized,
                with_options,
            } => {
                let modifier = if *materialized { " MATERIALIZED" } else { "" };
                let with_options = if !with_options.is_empty() {
                    format!(" WITH ({})", comma_separated_string(with_options))
                } else {
                    "".into()
                };
                let columns = if !columns.is_empty() {
                    format!(" ({})", comma_separated_string(columns))
                } else {
                    "".into()
                };
                format!(
                    "CREATE{} VIEW {}{}{} AS {}",
                    modifier,
                    name.to_string(),
                    with_options,
                    columns,
                    query.to_string(),
                )
            }
            Statement::CreateTable {
                name,
                columns,
                constraints,
                with_options,
                external,
                file_format,
                location,
            } => {
                let mut s = format!(
                    "CREATE {}TABLE {} ({}",
                    if *external { "EXTERNAL " } else { "" },
                    name.to_string(),
                    comma_separated_string(columns)
                );
                if !constraints.is_empty() {
                    s += &format!(", {}", comma_separated_string(constraints));
                }
                s += ")";
                if *external {
                    s += &format!(
                        " STORED AS {} LOCATION '{}'",
                        file_format.as_ref().unwrap().to_string(),
                        location.as_ref().unwrap()
                    );
                }
                if !with_options.is_empty() {
                    s += &format!(" WITH ({})", comma_separated_string(with_options));
                }
                s
            }
            Statement::AlterTable { name, operation } => {
                format!("ALTER TABLE {} {}", name.to_string(), operation.to_string())
            }
            Statement::Drop {
                object_type,
                if_exists,
                names,
                cascade,
            } => format!(
                "DROP {}{} {}{}",
                object_type.to_string(),
                if *if_exists { " IF EXISTS" } else { "" },
                comma_separated_string(names),
                if *cascade { " CASCADE" } else { "" },
            ),
            Statement::StartTransaction { modes } => format!(
                "START TRANSACTION{}",
                if modes.is_empty() {
                    "".into()
                } else {
                    format!(" {}", comma_separated_string(modes))
                }
            ),
            Statement::SetTransaction { modes } => format!(
                "SET TRANSACTION{}",
                if modes.is_empty() {
                    "".into()
                } else {
                    format!(" {}", comma_separated_string(modes))
                }
            ),
            Statement::Commit { chain } => {
                format!("COMMIT{}", if *chain { " AND CHAIN" } else { "" },)
            }
            Statement::Rollback { chain } => {
                format!("ROLLBACK{}", if *chain { " AND CHAIN" } else { "" },)
            }
        }
    }
}

/// A name of a table, view, custom type, etc., possibly multi-part, i.e. db.schema.obj
#[derive(Debug, Clone, PartialEq, Hash)]
pub struct ObjectName(pub Vec<Ident>);

impl ToString for ObjectName {
    fn to_string(&self) -> String {
        self.0.join(".")
    }
}

/// SQL assignment `foo = expr` as used in SQLUpdate
#[derive(Debug, Clone, PartialEq, Hash)]
pub struct Assignment {
    pub id: Ident,
    pub value: Expr,
}

impl ToString for Assignment {
    fn to_string(&self) -> String {
        format!("{} = {}", self.id, self.value.to_string())
    }
}

/// SQL function
#[derive(Debug, Clone, PartialEq, Hash)]
pub struct Function {
    pub name: ObjectName,
    pub args: Vec<Expr>,
    pub over: Option<WindowSpec>,
    // aggregate functions may specify eg `COUNT(DISTINCT x)`
    pub distinct: bool,
}

impl ToString for Function {
    fn to_string(&self) -> String {
        let mut s = format!(
            "{}({}{})",
            self.name.to_string(),
            if self.distinct { "DISTINCT " } else { "" },
            comma_separated_string(&self.args),
        );
        if let Some(o) = &self.over {
            s += &format!(" OVER ({})", o.to_string())
        }
        s
    }
}

/// External table's available file format
#[derive(Debug, Clone, PartialEq, Hash)]
pub enum FileFormat {
    TEXTFILE,
    SEQUENCEFILE,
    ORC,
    PARQUET,
    AVRO,
    RCFILE,
    JSONFILE,
}

impl ToString for FileFormat {
    fn to_string(&self) -> String {
        use self::FileFormat::*;
        match self {
            TEXTFILE => "TEXTFILE".to_string(),
            SEQUENCEFILE => "SEQUENCEFILE".to_string(),
            ORC => "ORC".to_string(),
            PARQUET => "PARQUET".to_string(),
            AVRO => "AVRO".to_string(),
            RCFILE => "RCFILE".to_string(),
            JSONFILE => "TEXTFILE".to_string(),
        }
    }
}

use crate::parser::ParserError;
use std::str::FromStr;
impl FromStr for FileFormat {
    type Err = ParserError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        use self::FileFormat::*;
        match s {
            "TEXTFILE" => Ok(TEXTFILE),
            "SEQUENCEFILE" => Ok(SEQUENCEFILE),
            "ORC" => Ok(ORC),
            "PARQUET" => Ok(PARQUET),
            "AVRO" => Ok(AVRO),
            "RCFILE" => Ok(RCFILE),
            "JSONFILE" => Ok(JSONFILE),
            _ => Err(ParserError::ParserError(format!(
                "Unexpected file format: {}",
                s
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Hash)]
pub enum ObjectType {
    Table,
    View,
}

impl ObjectType {
    fn to_string(&self) -> String {
        match self {
            ObjectType::Table => "TABLE".into(),
            ObjectType::View => "VIEW".into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Hash)]
pub struct SqlOption {
    pub name: Ident,
    pub value: Value,
}

impl ToString for SqlOption {
    fn to_string(&self) -> String {
        format!("{} = {}", self.name.to_string(), self.value.to_string())
    }
}

#[derive(Debug, Clone, PartialEq, Hash)]
pub enum TransactionMode {
    AccessMode(TransactionAccessMode),
    IsolationLevel(TransactionIsolationLevel),
}

impl ToString for TransactionMode {
    fn to_string(&self) -> String {
        use TransactionMode::*;
        match self {
            AccessMode(access_mode) => access_mode.to_string(),
            IsolationLevel(iso_level) => format!("ISOLATION LEVEL {}", iso_level.to_string()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Hash)]
pub enum TransactionAccessMode {
    ReadOnly,
    ReadWrite,
}

impl ToString for TransactionAccessMode {
    fn to_string(&self) -> String {
        use TransactionAccessMode::*;
        match self {
            ReadOnly => "READ ONLY".into(),
            ReadWrite => "READ WRITE".into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Hash)]
pub enum TransactionIsolationLevel {
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

impl ToString for TransactionIsolationLevel {
    fn to_string(&self) -> String {
        use TransactionIsolationLevel::*;
        match self {
            ReadUncommitted => "READ UNCOMMITTED".into(),
            ReadCommitted => "READ COMMITTED".into(),
            RepeatableRead => "REPEATABLE READ".into(),
            Serializable => "SERIALIZABLE".into(),
        }
    }
}

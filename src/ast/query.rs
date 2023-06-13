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

#[cfg(not(feature = "std"))]
use alloc::{boxed::Box, vec::Vec};

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

#[cfg(feature = "visitor")]
use sqlparser_derive::{Visit, VisitMut};

use crate::ast::*;

/// The most complete variant of a `SELECT` query expression, optionally
/// including `WITH`, `UNION` / other set operations, and `ORDER BY`.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct Query {
    /// WITH (common table expressions, or CTEs)
    pub with: Option<With>,
    /// SELECT or UNION / EXCEPT / INTERSECT
    pub body: Box<SetExpr>,
    /// ORDER BY
    pub order_by: Vec<OrderByExpr>,
    /// `LIMIT { <N> | ALL }`
    pub limit: Option<Expr>,
    /// `OFFSET <N> [ { ROW | ROWS } ]`
    pub offset: Option<Offset>,
    /// `FETCH { FIRST | NEXT } <N> [ PERCENT ] { ROW | ROWS } | { ONLY | WITH TIES }`
    pub fetch: Option<Fetch>,
    /// `FOR { UPDATE | SHARE } [ OF table_name ] [ SKIP LOCKED | NOWAIT ]`
    pub locks: Vec<LockClause>,
}

impl fmt::Display for Query {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let Some(ref with) = self.with {
            write!(f, "{with} ")?;
        }
        write!(f, "{}", self.body)?;
        if !self.order_by.is_empty() {
            write!(f, " ORDER BY {}", display_comma_separated(&self.order_by))?;
        }
        if let Some(ref limit) = self.limit {
            write!(f, " LIMIT {limit}")?;
        }
        if let Some(ref offset) = self.offset {
            write!(f, " {offset}")?;
        }
        if let Some(ref fetch) = self.fetch {
            write!(f, " {fetch}")?;
        }
        if !self.locks.is_empty() {
            write!(f, " {}", display_separated(&self.locks, " "))?;
        }
        Ok(())
    }
}

/// A node in a tree, representing a "query body" expression, roughly:
/// `SELECT ... [ {UNION|EXCEPT|INTERSECT} SELECT ...]`
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum SetExpr {
    /// Restricted SELECT .. FROM .. HAVING (no ORDER BY or set operations)
    Select(Box<Select>),
    /// Parenthesized SELECT subquery, which may include more set operations
    /// in its body and an optional ORDER BY / LIMIT.
    Query(Box<Query>),
    /// UNION/EXCEPT/INTERSECT of two queries
    SetOperation {
        op: SetOperator,
        set_quantifier: SetQuantifier,
        left: Box<SetExpr>,
        right: Box<SetExpr>,
    },
    Values(Values),
    Insert(Statement),
    Update(Statement),
    Table(Box<Table>),
}

impl fmt::Display for SetExpr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SetExpr::Select(s) => write!(f, "{s}"),
            SetExpr::Query(q) => write!(f, "({q})"),
            SetExpr::Values(v) => write!(f, "{v}"),
            SetExpr::Insert(v) => write!(f, "{v}"),
            SetExpr::Update(v) => write!(f, "{v}"),
            SetExpr::Table(t) => write!(f, "{t}"),
            SetExpr::SetOperation {
                left,
                right,
                op,
                set_quantifier,
            } => {
                write!(f, "{left} {op}")?;
                match set_quantifier {
                    SetQuantifier::All | SetQuantifier::Distinct => write!(f, " {set_quantifier}")?,
                    SetQuantifier::None => write!(f, "{set_quantifier}")?,
                }
                write!(f, " {right}")?;
                Ok(())
            }
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum SetOperator {
    Union,
    Except,
    Intersect,
}

impl fmt::Display for SetOperator {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(match self {
            SetOperator::Union => "UNION",
            SetOperator::Except => "EXCEPT",
            SetOperator::Intersect => "INTERSECT",
        })
    }
}

/// A quantifier for [SetOperator].
// TODO: Restrict parsing specific SetQuantifier in some specific dialects.
// For example, BigQuery does not support `DISTINCT` for `EXCEPT` and `INTERSECT`
#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum SetQuantifier {
    All,
    Distinct,
    None,
}

impl fmt::Display for SetQuantifier {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SetQuantifier::All => write!(f, "ALL"),
            SetQuantifier::Distinct => write!(f, "DISTINCT"),
            SetQuantifier::None => write!(f, ""),
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
/// A [`TABLE` command]( https://www.postgresql.org/docs/current/sql-select.html#SQL-TABLE)
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct Table {
    pub table_name: Option<String>,
    pub schema_name: Option<String>,
}

impl fmt::Display for Table {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let Some(ref schema_name) = self.schema_name {
            write!(
                f,
                "TABLE {}.{}",
                schema_name,
                self.table_name.as_ref().unwrap(),
            )?;
        } else {
            write!(f, "TABLE {}", self.table_name.as_ref().unwrap(),)?;
        }
        Ok(())
    }
}

/// A restricted variant of `SELECT` (without CTEs/`ORDER BY`), which may
/// appear either as the only body item of a `Query`, or as an operand
/// to a set operation like `UNION`.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct Select {
    pub distinct: Option<Distinct>,
    /// MSSQL syntax: `TOP (<N>) [ PERCENT ] [ WITH TIES ]`
    pub top: Option<Top>,
    /// projection expressions
    pub projection: Vec<SelectItem>,
    /// INTO
    pub into: Option<SelectInto>,
    /// FROM
    pub from: Vec<TableWithJoins>,
    /// LATERAL VIEWs
    pub lateral_views: Vec<LateralView>,
    /// WHERE
    pub selection: Option<Expr>,
    /// GROUP BY
    pub group_by: Vec<Expr>,
    /// CLUSTER BY (Hive)
    pub cluster_by: Vec<Expr>,
    /// DISTRIBUTE BY (Hive)
    pub distribute_by: Vec<Expr>,
    /// SORT BY (Hive)
    pub sort_by: Vec<Expr>,
    /// HAVING
    pub having: Option<Expr>,
    /// WINDOW AS
    pub named_window: Vec<NamedWindowDefinition>,
    /// QUALIFY (Snowflake)
    pub qualify: Option<Expr>,
}

impl fmt::Display for Select {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SELECT")?;
        if let Some(ref distinct) = self.distinct {
            write!(f, " {distinct}")?;
        }
        if let Some(ref top) = self.top {
            write!(f, " {top}")?;
        }
        write!(f, " {}", display_comma_separated(&self.projection))?;

        if let Some(ref into) = self.into {
            write!(f, " {into}")?;
        }

        if !self.from.is_empty() {
            write!(f, " FROM {}", display_comma_separated(&self.from))?;
        }
        if !self.lateral_views.is_empty() {
            for lv in &self.lateral_views {
                write!(f, "{lv}")?;
            }
        }
        if let Some(ref selection) = self.selection {
            write!(f, " WHERE {selection}")?;
        }
        if !self.group_by.is_empty() {
            write!(f, " GROUP BY {}", display_comma_separated(&self.group_by))?;
        }
        if !self.cluster_by.is_empty() {
            write!(
                f,
                " CLUSTER BY {}",
                display_comma_separated(&self.cluster_by)
            )?;
        }
        if !self.distribute_by.is_empty() {
            write!(
                f,
                " DISTRIBUTE BY {}",
                display_comma_separated(&self.distribute_by)
            )?;
        }
        if !self.sort_by.is_empty() {
            write!(f, " SORT BY {}", display_comma_separated(&self.sort_by))?;
        }
        if let Some(ref having) = self.having {
            write!(f, " HAVING {having}")?;
        }
        if !self.named_window.is_empty() {
            write!(f, " WINDOW {}", display_comma_separated(&self.named_window))?;
        }
        if let Some(ref qualify) = self.qualify {
            write!(f, " QUALIFY {qualify}")?;
        }
        Ok(())
    }
}

/// A hive LATERAL VIEW with potential column aliases
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct LateralView {
    /// LATERAL VIEW
    pub lateral_view: Expr,
    /// LATERAL VIEW table name
    pub lateral_view_name: ObjectName,
    /// LATERAL VIEW optional column aliases
    pub lateral_col_alias: Vec<Ident>,
    /// LATERAL VIEW OUTER
    pub outer: bool,
}

impl fmt::Display for LateralView {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            " LATERAL VIEW{outer} {} {}",
            self.lateral_view,
            self.lateral_view_name,
            outer = if self.outer { " OUTER" } else { "" }
        )?;
        if !self.lateral_col_alias.is_empty() {
            write!(
                f,
                " AS {}",
                display_comma_separated(&self.lateral_col_alias)
            )?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct NamedWindowDefinition(pub Ident, pub WindowSpec);

impl fmt::Display for NamedWindowDefinition {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} AS ({})", self.0, self.1)
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct With {
    pub recursive: bool,
    pub cte_tables: Vec<Cte>,
}

impl fmt::Display for With {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "WITH {}{}",
            if self.recursive { "RECURSIVE " } else { "" },
            display_comma_separated(&self.cte_tables)
        )
    }
}

/// A single CTE (used after `WITH`): `alias [(col1, col2, ...)] AS ( query )`
/// The names in the column list before `AS`, when specified, replace the names
/// of the columns returned by the query. The parser does not validate that the
/// number of columns in the query matches the number of columns in the query.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct Cte {
    pub alias: TableAlias,
    pub query: Box<Query>,
    pub from: Option<Ident>,
}

impl fmt::Display for Cte {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} AS ({})", self.alias, self.query)?;
        if let Some(ref fr) = self.from {
            write!(f, " FROM {fr}")?;
        }
        Ok(())
    }
}

/// One item of the comma-separated list following `SELECT`
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum SelectItem {
    /// Any expression, not followed by `[ AS ] alias`
    UnnamedExpr(Expr),
    /// An expression, followed by `[ AS ] alias`
    ExprWithAlias { expr: Expr, alias: Ident },
    /// `alias.*` or even `schema.table.*`
    QualifiedWildcard(ObjectName, WildcardAdditionalOptions),
    /// An unqualified `*`
    Wildcard(WildcardAdditionalOptions),
}

/// Single aliased identifier
///
/// # Syntax
/// ```plaintext
/// <ident> AS <alias>
/// ```
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct IdentWithAlias {
    pub ident: Ident,
    pub alias: Ident,
}

impl fmt::Display for IdentWithAlias {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} AS {}", self.ident, self.alias)
    }
}

/// Additional options for wildcards, e.g. Snowflake `EXCLUDE`/`RENAME` and Bigquery `EXCEPT`.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash, Default)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct WildcardAdditionalOptions {
    /// `[EXCLUDE...]`.
    pub opt_exclude: Option<ExcludeSelectItem>,
    /// `[EXCEPT...]`.
    pub opt_except: Option<ExceptSelectItem>,
    /// `[RENAME ...]`.
    pub opt_rename: Option<RenameSelectItem>,
    /// `[REPLACE]`
    ///  BigQuery syntax: <https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#select_replace>
    pub opt_replace: Option<ReplaceSelectItem>,
}

impl fmt::Display for WildcardAdditionalOptions {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let Some(exclude) = &self.opt_exclude {
            write!(f, " {exclude}")?;
        }
        if let Some(except) = &self.opt_except {
            write!(f, " {except}")?;
        }
        if let Some(rename) = &self.opt_rename {
            write!(f, " {rename}")?;
        }
        if let Some(replace) = &self.opt_replace {
            write!(f, " {replace}")?;
        }
        Ok(())
    }
}

/// Snowflake `EXCLUDE` information.
///
/// # Syntax
/// ```plaintext
/// <col_name>
/// | (<col_name>, <col_name>, ...)
/// ```
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum ExcludeSelectItem {
    /// Single column name without parenthesis.
    ///
    /// # Syntax
    /// ```plaintext
    /// <col_name>
    /// ```
    Single(Ident),
    /// Multiple column names inside parenthesis.
    /// # Syntax
    /// ```plaintext
    /// (<col_name>, <col_name>, ...)
    /// ```
    Multiple(Vec<Ident>),
}

impl fmt::Display for ExcludeSelectItem {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "EXCLUDE")?;
        match self {
            Self::Single(column) => {
                write!(f, " {column}")?;
            }
            Self::Multiple(columns) => {
                write!(f, " ({})", display_comma_separated(columns))?;
            }
        }
        Ok(())
    }
}

/// Snowflake `RENAME` information.
///
/// # Syntax
/// ```plaintext
/// <col_name> AS <col_alias>
/// | (<col_name> AS <col_alias>, <col_name> AS <col_alias>, ...)
/// ```
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum RenameSelectItem {
    /// Single column name with alias without parenthesis.
    ///
    /// # Syntax
    /// ```plaintext
    /// <col_name> AS <col_alias>
    /// ```
    Single(IdentWithAlias),
    /// Multiple column names with aliases inside parenthesis.
    /// # Syntax
    /// ```plaintext
    /// (<col_name> AS <col_alias>, <col_name> AS <col_alias>, ...)
    /// ```
    Multiple(Vec<IdentWithAlias>),
}

impl fmt::Display for RenameSelectItem {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "RENAME")?;
        match self {
            Self::Single(column) => {
                write!(f, " {column}")?;
            }
            Self::Multiple(columns) => {
                write!(f, " ({})", display_comma_separated(columns))?;
            }
        }
        Ok(())
    }
}

/// Bigquery `EXCEPT` information, with at least one column.
///
/// # Syntax
/// ```plaintext
/// EXCEPT (<col_name> [, ...])
/// ```
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct ExceptSelectItem {
    /// First guaranteed column.
    pub first_element: Ident,
    /// Additional columns. This list can be empty.
    pub additional_elements: Vec<Ident>,
}

impl fmt::Display for ExceptSelectItem {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "EXCEPT ")?;
        if self.additional_elements.is_empty() {
            write!(f, "({})", self.first_element)?;
        } else {
            write!(
                f,
                "({}, {})",
                self.first_element,
                display_comma_separated(&self.additional_elements)
            )?;
        }
        Ok(())
    }
}

/// Bigquery `REPLACE` information.
///
/// # Syntax
/// ```plaintext
/// REPLACE (<new_expr> [AS] <col_name>)
/// REPLACE (<col_name> [AS] <col_alias>, <col_name> [AS] <col_alias>, ...)
/// ```
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct ReplaceSelectItem {
    pub items: Vec<Box<ReplaceSelectElement>>,
}

impl fmt::Display for ReplaceSelectItem {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "REPLACE")?;
        write!(f, " ({})", display_comma_separated(&self.items))?;
        Ok(())
    }
}

/// # Syntax
/// ```plaintext
/// <expr> [AS] <column_name>
/// ```
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct ReplaceSelectElement {
    pub expr: Expr,
    pub column_name: Ident,
    pub as_keyword: bool,
}

impl fmt::Display for ReplaceSelectElement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.as_keyword {
            write!(f, "{} AS {}", self.expr, self.column_name)
        } else {
            write!(f, "{} {}", self.expr, self.column_name)
        }
    }
}

impl fmt::Display for SelectItem {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self {
            SelectItem::UnnamedExpr(expr) => write!(f, "{expr}"),
            SelectItem::ExprWithAlias { expr, alias } => write!(f, "{expr} AS {alias}"),
            SelectItem::QualifiedWildcard(prefix, additional_options) => {
                write!(f, "{prefix}.*")?;
                write!(f, "{additional_options}")?;
                Ok(())
            }
            SelectItem::Wildcard(additional_options) => {
                write!(f, "*")?;
                write!(f, "{additional_options}")?;
                Ok(())
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct TableWithJoins {
    pub relation: TableFactor,
    pub joins: Vec<Join>,
}

impl fmt::Display for TableWithJoins {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.relation)?;
        for join in &self.joins {
            write!(f, "{join}")?;
        }
        Ok(())
    }
}

/// A table name or a parenthesized subquery with an optional alias
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum TableFactor {
    Table {
        #[cfg_attr(feature = "visitor", visit(with = "visit_relation"))]
        name: ObjectName,
        alias: Option<TableAlias>,
        /// Arguments of a table-valued function, as supported by Postgres
        /// and MSSQL. Note that deprecated MSSQL `FROM foo (NOLOCK)` syntax
        /// will also be parsed as `args`.
        ///
        /// This field's value is `Some(v)`, where `v` is a (possibly empty)
        /// vector of arguments, in the case of a table-valued function call,
        /// whereas it's `None` in the case of a regular table name.
        args: Option<Vec<FunctionArg>>,
        /// MSSQL-specific `WITH (...)` hints such as NOLOCK.
        with_hints: Vec<Expr>,
    },
    Derived {
        lateral: bool,
        subquery: Box<Query>,
        alias: Option<TableAlias>,
    },
    /// `TABLE(<expr>)[ AS <alias> ]`
    TableFunction {
        expr: Expr,
        alias: Option<TableAlias>,
    },
    /// ```sql
    /// SELECT * FROM UNNEST ([10,20,30]) as numbers WITH OFFSET;
    /// +---------+--------+
    /// | numbers | offset |
    /// +---------+--------+
    /// | 10      | 0      |
    /// | 20      | 1      |
    /// | 30      | 2      |
    /// +---------+--------+
    /// ```
    UNNEST {
        alias: Option<TableAlias>,
        array_expr: Box<Expr>,
        with_offset: bool,
        with_offset_alias: Option<Ident>,
    },
    /// Represents a parenthesized table factor. The SQL spec only allows a
    /// join expression (`(foo <JOIN> bar [ <JOIN> baz ... ])`) to be nested,
    /// possibly several times.
    ///
    /// The parser may also accept non-standard nesting of bare tables for some
    /// dialects, but the information about such nesting is stripped from AST.
    NestedJoin {
        table_with_joins: Box<TableWithJoins>,
        alias: Option<TableAlias>,
    },
    /// Represents PIVOT operation on a table.
    /// For example `FROM monthly_sales PIVOT(sum(amount) FOR MONTH IN ('JAN', 'FEB'))`
    /// See <https://docs.snowflake.com/en/sql-reference/constructs/pivot>
    Pivot {
        #[cfg_attr(feature = "visitor", visit(with = "visit_relation"))]
        name: ObjectName,
        table_alias: Option<TableAlias>,
        aggregate_function: Expr, // Function expression
        value_column: Vec<Ident>,
        pivot_values: Vec<Value>,
        pivot_alias: Option<TableAlias>,
    },
}

impl fmt::Display for TableFactor {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            TableFactor::Table {
                name,
                alias,
                args,
                with_hints,
            } => {
                write!(f, "{name}")?;
                if let Some(args) = args {
                    write!(f, "({})", display_comma_separated(args))?;
                }
                if let Some(alias) = alias {
                    write!(f, " AS {alias}")?;
                }
                if !with_hints.is_empty() {
                    write!(f, " WITH ({})", display_comma_separated(with_hints))?;
                }
                Ok(())
            }
            TableFactor::Derived {
                lateral,
                subquery,
                alias,
            } => {
                if *lateral {
                    write!(f, "LATERAL ")?;
                }
                write!(f, "({subquery})")?;
                if let Some(alias) = alias {
                    write!(f, " AS {alias}")?;
                }
                Ok(())
            }
            TableFactor::TableFunction { expr, alias } => {
                write!(f, "TABLE({expr})")?;
                if let Some(alias) = alias {
                    write!(f, " AS {alias}")?;
                }
                Ok(())
            }
            TableFactor::UNNEST {
                alias,
                array_expr,
                with_offset,
                with_offset_alias,
            } => {
                write!(f, "UNNEST({array_expr})")?;
                if let Some(alias) = alias {
                    write!(f, " AS {alias}")?;
                }
                if *with_offset {
                    write!(f, " WITH OFFSET")?;
                }
                if let Some(alias) = with_offset_alias {
                    write!(f, " AS {alias}")?;
                }
                Ok(())
            }
            TableFactor::NestedJoin {
                table_with_joins,
                alias,
            } => {
                write!(f, "({table_with_joins})")?;
                if let Some(alias) = alias {
                    write!(f, " AS {alias}")?;
                }
                Ok(())
            }
            TableFactor::Pivot {
                name,
                table_alias,
                aggregate_function,
                value_column,
                pivot_values,
                pivot_alias,
            } => {
                write!(f, "{}", name)?;
                if table_alias.is_some() {
                    write!(f, " AS {}", table_alias.as_ref().unwrap())?;
                }
                write!(
                    f,
                    " PIVOT({} FOR {} IN (",
                    aggregate_function,
                    Expr::CompoundIdentifier(value_column.to_vec())
                )?;
                for value in pivot_values {
                    write!(f, "{}", value)?;
                    if !value.eq(pivot_values.last().unwrap()) {
                        write!(f, ", ")?;
                    }
                }
                write!(f, "))")?;
                if pivot_alias.is_some() {
                    write!(f, " AS {}", pivot_alias.as_ref().unwrap())?;
                }
                Ok(())
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct TableAlias {
    pub name: Ident,
    pub columns: Vec<Ident>,
}

impl fmt::Display for TableAlias {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.name)?;
        if !self.columns.is_empty() {
            write!(f, " ({})", display_comma_separated(&self.columns))?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct Join {
    pub relation: TableFactor,
    pub join_operator: JoinOperator,
}

impl fmt::Display for Join {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fn prefix(constraint: &JoinConstraint) -> &'static str {
            match constraint {
                JoinConstraint::Natural => "NATURAL ",
                _ => "",
            }
        }
        fn suffix(constraint: &'_ JoinConstraint) -> impl fmt::Display + '_ {
            struct Suffix<'a>(&'a JoinConstraint);
            impl<'a> fmt::Display for Suffix<'a> {
                fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                    match self.0 {
                        JoinConstraint::On(expr) => write!(f, " ON {expr}"),
                        JoinConstraint::Using(attrs) => {
                            write!(f, " USING({})", display_comma_separated(attrs))
                        }
                        _ => Ok(()),
                    }
                }
            }
            Suffix(constraint)
        }
        match &self.join_operator {
            JoinOperator::Inner(constraint) => write!(
                f,
                " {}JOIN {}{}",
                prefix(constraint),
                self.relation,
                suffix(constraint)
            ),
            JoinOperator::LeftOuter(constraint) => write!(
                f,
                " {}LEFT JOIN {}{}",
                prefix(constraint),
                self.relation,
                suffix(constraint)
            ),
            JoinOperator::RightOuter(constraint) => write!(
                f,
                " {}RIGHT JOIN {}{}",
                prefix(constraint),
                self.relation,
                suffix(constraint)
            ),
            JoinOperator::FullOuter(constraint) => write!(
                f,
                " {}FULL JOIN {}{}",
                prefix(constraint),
                self.relation,
                suffix(constraint)
            ),
            JoinOperator::CrossJoin => write!(f, " CROSS JOIN {}", self.relation),
            JoinOperator::LeftSemi(constraint) => write!(
                f,
                " {}LEFT SEMI JOIN {}{}",
                prefix(constraint),
                self.relation,
                suffix(constraint)
            ),
            JoinOperator::RightSemi(constraint) => write!(
                f,
                " {}RIGHT SEMI JOIN {}{}",
                prefix(constraint),
                self.relation,
                suffix(constraint)
            ),
            JoinOperator::LeftAnti(constraint) => write!(
                f,
                " {}LEFT ANTI JOIN {}{}",
                prefix(constraint),
                self.relation,
                suffix(constraint)
            ),
            JoinOperator::RightAnti(constraint) => write!(
                f,
                " {}RIGHT ANTI JOIN {}{}",
                prefix(constraint),
                self.relation,
                suffix(constraint)
            ),
            JoinOperator::CrossApply => write!(f, " CROSS APPLY {}", self.relation),
            JoinOperator::OuterApply => write!(f, " OUTER APPLY {}", self.relation),
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum JoinOperator {
    Inner(JoinConstraint),
    LeftOuter(JoinConstraint),
    RightOuter(JoinConstraint),
    FullOuter(JoinConstraint),
    CrossJoin,
    /// LEFT SEMI (non-standard)
    LeftSemi(JoinConstraint),
    /// RIGHT SEMI (non-standard)
    RightSemi(JoinConstraint),
    /// LEFT ANTI (non-standard)
    LeftAnti(JoinConstraint),
    /// RIGHT ANTI (non-standard)
    RightAnti(JoinConstraint),
    /// CROSS APPLY (non-standard)
    CrossApply,
    /// OUTER APPLY (non-standard)
    OuterApply,
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum JoinConstraint {
    On(Expr),
    Using(Vec<Ident>),
    Natural,
    None,
}

/// An `ORDER BY` expression
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct OrderByExpr {
    pub expr: Expr,
    /// Optional `ASC` or `DESC`
    pub asc: Option<bool>,
    /// Optional `NULLS FIRST` or `NULLS LAST`
    pub nulls_first: Option<bool>,
}

impl fmt::Display for OrderByExpr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.expr)?;
        match self.asc {
            Some(true) => write!(f, " ASC")?,
            Some(false) => write!(f, " DESC")?,
            None => (),
        }
        match self.nulls_first {
            Some(true) => write!(f, " NULLS FIRST")?,
            Some(false) => write!(f, " NULLS LAST")?,
            None => (),
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct Offset {
    pub value: Expr,
    pub rows: OffsetRows,
}

impl fmt::Display for Offset {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "OFFSET {}{}", self.value, self.rows)
    }
}

/// Stores the keyword after `OFFSET <number>`
#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum OffsetRows {
    /// Omitting ROW/ROWS is non-standard MySQL quirk.
    None,
    Row,
    Rows,
}

impl fmt::Display for OffsetRows {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            OffsetRows::None => Ok(()),
            OffsetRows::Row => write!(f, " ROW"),
            OffsetRows::Rows => write!(f, " ROWS"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct Fetch {
    pub with_ties: bool,
    pub percent: bool,
    pub quantity: Option<Expr>,
}

impl fmt::Display for Fetch {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let extension = if self.with_ties { "WITH TIES" } else { "ONLY" };
        if let Some(ref quantity) = self.quantity {
            let percent = if self.percent { " PERCENT" } else { "" };
            write!(f, "FETCH FIRST {quantity}{percent} ROWS {extension}")
        } else {
            write!(f, "FETCH FIRST ROWS {extension}")
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct LockClause {
    pub lock_type: LockType,
    pub of: Option<ObjectName>,
    pub nonblock: Option<NonBlock>,
}

impl fmt::Display for LockClause {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "FOR {}", &self.lock_type)?;
        if let Some(ref of) = self.of {
            write!(f, " OF {of}")?;
        }
        if let Some(ref nb) = self.nonblock {
            write!(f, " {nb}")?;
        }
        Ok(())
    }
}

#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum LockType {
    Share,
    Update,
}

impl fmt::Display for LockType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let select_lock = match self {
            LockType::Share => "SHARE",
            LockType::Update => "UPDATE",
        };
        write!(f, "{select_lock}")
    }
}

#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum NonBlock {
    Nowait,
    SkipLocked,
}

impl fmt::Display for NonBlock {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let nonblock = match self {
            NonBlock::Nowait => "NOWAIT",
            NonBlock::SkipLocked => "SKIP LOCKED",
        };
        write!(f, "{nonblock}")
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum Distinct {
    /// DISTINCT
    Distinct,

    /// DISTINCT ON({column names})
    On(Vec<Expr>),
}

impl fmt::Display for Distinct {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Distinct::Distinct => write!(f, "DISTINCT"),
            Distinct::On(col_names) => {
                let col_names = display_comma_separated(col_names);
                write!(f, "DISTINCT ON ({col_names})")
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct Top {
    /// SQL semantic equivalent of LIMIT but with same structure as FETCH.
    pub with_ties: bool,
    pub percent: bool,
    pub quantity: Option<Expr>,
}

impl fmt::Display for Top {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let extension = if self.with_ties { " WITH TIES" } else { "" };
        if let Some(ref quantity) = self.quantity {
            let percent = if self.percent { " PERCENT" } else { "" };
            write!(f, "TOP ({quantity}){percent}{extension}")
        } else {
            write!(f, "TOP{extension}")
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct Values {
    /// Was there an explicit ROWs keyword (MySQL)?
    /// <https://dev.mysql.com/doc/refman/8.0/en/values.html>
    pub explicit_row: bool,
    pub rows: Vec<Vec<Expr>>,
}

impl fmt::Display for Values {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "VALUES ")?;
        let prefix = if self.explicit_row { "ROW" } else { "" };
        let mut delim = "";
        for row in &self.rows {
            write!(f, "{delim}")?;
            delim = ", ";
            write!(f, "{prefix}({})", display_comma_separated(row))?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct SelectInto {
    pub temporary: bool,
    pub unlogged: bool,
    pub table: bool,
    pub name: ObjectName,
}

impl fmt::Display for SelectInto {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let temporary = if self.temporary { " TEMPORARY" } else { "" };
        let unlogged = if self.unlogged { " UNLOGGED" } else { "" };
        let table = if self.table { " TABLE" } else { "" };

        write!(f, "INTO{}{}{} {}", temporary, unlogged, table, self.name)
    }
}

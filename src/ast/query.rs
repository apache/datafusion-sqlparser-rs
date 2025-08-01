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

#[cfg(not(feature = "std"))]
use alloc::{boxed::Box, vec::Vec};

use helpers::attached_token::AttachedToken;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

#[cfg(feature = "visitor")]
use sqlparser_derive::{Visit, VisitMut};

use crate::{
    ast::*,
    display_utils::{indented_list, SpaceOrNewline},
    tokenizer::{Token, TokenWithSpan},
};

/// The most complete variant of a `SELECT` query expression, optionally
/// including `WITH`, `UNION` / other set operations, and `ORDER BY`.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
#[cfg_attr(feature = "visitor", visit(with = "visit_query"))]
pub struct Query {
    /// WITH (common table expressions, or CTEs)
    pub with: Option<With>,
    /// SELECT or UNION / EXCEPT / INTERSECT
    pub body: Box<SetExpr>,
    /// ORDER BY
    pub order_by: Option<OrderBy>,
    /// `LIMIT ... OFFSET ... | LIMIT <offset>, <limit>`
    pub limit_clause: Option<LimitClause>,
    /// `FETCH { FIRST | NEXT } <N> [ PERCENT ] { ROW | ROWS } | { ONLY | WITH TIES }`
    pub fetch: Option<Fetch>,
    /// `FOR { UPDATE | SHARE } [ OF table_name ] [ SKIP LOCKED | NOWAIT ]`
    pub locks: Vec<LockClause>,
    /// `FOR XML { RAW | AUTO | EXPLICIT | PATH } [ , ELEMENTS ]`
    /// `FOR JSON { AUTO | PATH } [ , INCLUDE_NULL_VALUES ]`
    /// (MSSQL-specific)
    pub for_clause: Option<ForClause>,
    /// ClickHouse syntax: `SELECT * FROM t SETTINGS key1 = value1, key2 = value2`
    ///
    /// [ClickHouse](https://clickhouse.com/docs/en/sql-reference/statements/select#settings-in-select-query)
    pub settings: Option<Vec<Setting>>,
    /// `SELECT * FROM t FORMAT JSONCompact`
    ///
    /// [ClickHouse](https://clickhouse.com/docs/en/sql-reference/statements/select/format)
    /// (ClickHouse-specific)
    pub format_clause: Option<FormatClause>,

    /// Pipe operator
    pub pipe_operators: Vec<PipeOperator>,
}

impl fmt::Display for Query {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let Some(ref with) = self.with {
            with.fmt(f)?;
            SpaceOrNewline.fmt(f)?;
        }
        self.body.fmt(f)?;
        if let Some(ref order_by) = self.order_by {
            f.write_str(" ")?;
            order_by.fmt(f)?;
        }

        if let Some(ref limit_clause) = self.limit_clause {
            limit_clause.fmt(f)?;
        }
        if let Some(ref settings) = self.settings {
            f.write_str(" SETTINGS ")?;
            display_comma_separated(settings).fmt(f)?;
        }
        if let Some(ref fetch) = self.fetch {
            f.write_str(" ")?;
            fetch.fmt(f)?;
        }
        if !self.locks.is_empty() {
            f.write_str(" ")?;
            display_separated(&self.locks, " ").fmt(f)?;
        }
        if let Some(ref for_clause) = self.for_clause {
            f.write_str(" ")?;
            for_clause.fmt(f)?;
        }
        if let Some(ref format) = self.format_clause {
            f.write_str(" ")?;
            format.fmt(f)?;
        }
        for pipe_operator in &self.pipe_operators {
            f.write_str(" |> ")?;
            pipe_operator.fmt(f)?;
        }
        Ok(())
    }
}

/// Query syntax for ClickHouse ADD PROJECTION statement.
/// Its syntax is similar to SELECT statement, but it is used to add a new projection to a table.
/// Syntax is `SELECT <COLUMN LIST EXPR> [GROUP BY] [ORDER BY]`
///
/// [ClickHouse](https://clickhouse.com/docs/en/sql-reference/statements/alter/projection#add-projection)
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct ProjectionSelect {
    pub projection: Vec<SelectItem>,
    pub order_by: Option<OrderBy>,
    pub group_by: Option<GroupByExpr>,
}

impl fmt::Display for ProjectionSelect {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SELECT {}", display_comma_separated(&self.projection))?;
        if let Some(ref group_by) = self.group_by {
            write!(f, " {group_by}")?;
        }
        if let Some(ref order_by) = self.order_by {
            write!(f, " {order_by}")?;
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
    Delete(Statement),
    Table(Box<Table>),
}

impl SetExpr {
    /// If this `SetExpr` is a `SELECT`, returns the [`Select`].
    pub fn as_select(&self) -> Option<&Select> {
        if let Self::Select(select) = self {
            Some(&**select)
        } else {
            None
        }
    }
}

impl fmt::Display for SetExpr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SetExpr::Select(s) => s.fmt(f),
            SetExpr::Query(q) => {
                f.write_str("(")?;
                q.fmt(f)?;
                f.write_str(")")
            }
            SetExpr::Values(v) => v.fmt(f),
            SetExpr::Insert(v) => v.fmt(f),
            SetExpr::Update(v) => v.fmt(f),
            SetExpr::Delete(v) => v.fmt(f),
            SetExpr::Table(t) => t.fmt(f),
            SetExpr::SetOperation {
                left,
                right,
                op,
                set_quantifier,
            } => {
                left.fmt(f)?;
                SpaceOrNewline.fmt(f)?;
                op.fmt(f)?;
                match set_quantifier {
                    SetQuantifier::All
                    | SetQuantifier::Distinct
                    | SetQuantifier::ByName
                    | SetQuantifier::AllByName
                    | SetQuantifier::DistinctByName => {
                        f.write_str(" ")?;
                        set_quantifier.fmt(f)?;
                    }
                    SetQuantifier::None => {}
                }
                SpaceOrNewline.fmt(f)?;
                right.fmt(f)?;
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
    Minus,
}

impl fmt::Display for SetOperator {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(match self {
            SetOperator::Union => "UNION",
            SetOperator::Except => "EXCEPT",
            SetOperator::Intersect => "INTERSECT",
            SetOperator::Minus => "MINUS",
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
    ByName,
    AllByName,
    DistinctByName,
    None,
}

impl fmt::Display for SetQuantifier {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SetQuantifier::All => write!(f, "ALL"),
            SetQuantifier::Distinct => write!(f, "DISTINCT"),
            SetQuantifier::ByName => write!(f, "BY NAME"),
            SetQuantifier::AllByName => write!(f, "ALL BY NAME"),
            SetQuantifier::DistinctByName => write!(f, "DISTINCT BY NAME"),
            SetQuantifier::None => Ok(()),
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

/// What did this select look like?
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum SelectFlavor {
    /// `SELECT *`
    Standard,
    /// `FROM ... SELECT *`
    FromFirst,
    /// `FROM *`
    FromFirstNoSelect,
}

/// A restricted variant of `SELECT` (without CTEs/`ORDER BY`), which may
/// appear either as the only body item of a `Query`, or as an operand
/// to a set operation like `UNION`.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct Select {
    /// Token for the `SELECT` keyword
    pub select_token: AttachedToken,
    /// `SELECT [DISTINCT] ...`
    pub distinct: Option<Distinct>,
    /// MSSQL syntax: `TOP (<N>) [ PERCENT ] [ WITH TIES ]`
    pub top: Option<Top>,
    /// Whether the top was located before `ALL`/`DISTINCT`
    pub top_before_distinct: bool,
    /// projection expressions
    pub projection: Vec<SelectItem>,
    /// Excluded columns from the projection expression which are not specified
    /// directly after a wildcard.
    ///
    /// [Redshift](https://docs.aws.amazon.com/redshift/latest/dg/r_EXCLUDE_list.html)
    pub exclude: Option<ExcludeSelectItem>,
    /// INTO
    pub into: Option<SelectInto>,
    /// FROM
    pub from: Vec<TableWithJoins>,
    /// LATERAL VIEWs
    pub lateral_views: Vec<LateralView>,
    /// ClickHouse syntax: `PREWHERE a = 1 WHERE b = 2`,
    /// and it can be used together with WHERE selection.
    ///
    /// [ClickHouse](https://clickhouse.com/docs/en/sql-reference/statements/select/prewhere)
    pub prewhere: Option<Expr>,
    /// WHERE
    pub selection: Option<Expr>,
    /// GROUP BY
    pub group_by: GroupByExpr,
    /// CLUSTER BY (Hive)
    pub cluster_by: Vec<Expr>,
    /// DISTRIBUTE BY (Hive)
    pub distribute_by: Vec<Expr>,
    /// SORT BY (Hive)
    pub sort_by: Vec<OrderByExpr>,
    /// HAVING
    pub having: Option<Expr>,
    /// WINDOW AS
    pub named_window: Vec<NamedWindowDefinition>,
    /// QUALIFY (Snowflake)
    pub qualify: Option<Expr>,
    /// The positioning of QUALIFY and WINDOW clauses differ between dialects.
    /// e.g. BigQuery requires that WINDOW comes after QUALIFY, while DUCKDB accepts
    /// WINDOW before QUALIFY.
    /// We accept either positioning and flag the accepted variant.
    pub window_before_qualify: bool,
    /// BigQuery syntax: `SELECT AS VALUE | SELECT AS STRUCT`
    pub value_table_mode: Option<ValueTableMode>,
    /// STARTING WITH .. CONNECT BY
    pub connect_by: Option<ConnectBy>,
    /// Was this a FROM-first query?
    pub flavor: SelectFlavor,
}

impl fmt::Display for Select {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.flavor {
            SelectFlavor::Standard => {
                write!(f, "SELECT")?;
            }
            SelectFlavor::FromFirst => {
                write!(f, "FROM {} SELECT", display_comma_separated(&self.from))?;
            }
            SelectFlavor::FromFirstNoSelect => {
                write!(f, "FROM {}", display_comma_separated(&self.from))?;
            }
        }

        if let Some(value_table_mode) = self.value_table_mode {
            f.write_str(" ")?;
            value_table_mode.fmt(f)?;
        }

        if let Some(ref top) = self.top {
            if self.top_before_distinct {
                f.write_str(" ")?;
                top.fmt(f)?;
            }
        }
        if let Some(ref distinct) = self.distinct {
            f.write_str(" ")?;
            distinct.fmt(f)?;
        }
        if let Some(ref top) = self.top {
            if !self.top_before_distinct {
                f.write_str(" ")?;
                top.fmt(f)?;
            }
        }

        if !self.projection.is_empty() {
            indented_list(f, &self.projection)?;
        }

        if let Some(exclude) = &self.exclude {
            write!(f, " {exclude}")?;
        }

        if let Some(ref into) = self.into {
            f.write_str(" ")?;
            into.fmt(f)?;
        }

        if self.flavor == SelectFlavor::Standard && !self.from.is_empty() {
            SpaceOrNewline.fmt(f)?;
            f.write_str("FROM")?;
            indented_list(f, &self.from)?;
        }
        if !self.lateral_views.is_empty() {
            for lv in &self.lateral_views {
                lv.fmt(f)?;
            }
        }
        if let Some(ref prewhere) = self.prewhere {
            f.write_str(" PREWHERE ")?;
            prewhere.fmt(f)?;
        }
        if let Some(ref selection) = self.selection {
            SpaceOrNewline.fmt(f)?;
            f.write_str("WHERE")?;
            SpaceOrNewline.fmt(f)?;
            Indent(selection).fmt(f)?;
        }
        match &self.group_by {
            GroupByExpr::All(_) => {
                SpaceOrNewline.fmt(f)?;
                self.group_by.fmt(f)?;
            }
            GroupByExpr::Expressions(exprs, _) => {
                if !exprs.is_empty() {
                    SpaceOrNewline.fmt(f)?;
                    self.group_by.fmt(f)?;
                }
            }
        }
        if !self.cluster_by.is_empty() {
            SpaceOrNewline.fmt(f)?;
            f.write_str("CLUSTER BY")?;
            SpaceOrNewline.fmt(f)?;
            Indent(display_comma_separated(&self.cluster_by)).fmt(f)?;
        }
        if !self.distribute_by.is_empty() {
            SpaceOrNewline.fmt(f)?;
            f.write_str("DISTRIBUTE BY")?;
            SpaceOrNewline.fmt(f)?;
            display_comma_separated(&self.distribute_by).fmt(f)?;
        }
        if !self.sort_by.is_empty() {
            SpaceOrNewline.fmt(f)?;
            f.write_str("SORT BY")?;
            SpaceOrNewline.fmt(f)?;
            Indent(display_comma_separated(&self.sort_by)).fmt(f)?;
        }
        if let Some(ref having) = self.having {
            SpaceOrNewline.fmt(f)?;
            f.write_str("HAVING")?;
            SpaceOrNewline.fmt(f)?;
            Indent(having).fmt(f)?;
        }
        if self.window_before_qualify {
            if !self.named_window.is_empty() {
                SpaceOrNewline.fmt(f)?;
                f.write_str("WINDOW")?;
                SpaceOrNewline.fmt(f)?;
                display_comma_separated(&self.named_window).fmt(f)?;
            }
            if let Some(ref qualify) = self.qualify {
                SpaceOrNewline.fmt(f)?;
                f.write_str("QUALIFY")?;
                SpaceOrNewline.fmt(f)?;
                qualify.fmt(f)?;
            }
        } else {
            if let Some(ref qualify) = self.qualify {
                SpaceOrNewline.fmt(f)?;
                f.write_str("QUALIFY")?;
                SpaceOrNewline.fmt(f)?;
                qualify.fmt(f)?;
            }
            if !self.named_window.is_empty() {
                SpaceOrNewline.fmt(f)?;
                f.write_str("WINDOW")?;
                SpaceOrNewline.fmt(f)?;
                display_comma_separated(&self.named_window).fmt(f)?;
            }
        }
        if let Some(ref connect_by) = self.connect_by {
            SpaceOrNewline.fmt(f)?;
            connect_by.fmt(f)?;
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

/// An expression used in a named window declaration.
///
/// ```sql
/// WINDOW mywindow AS [named_window_expr]
/// ```
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum NamedWindowExpr {
    /// A direct reference to another named window definition.
    /// [BigQuery]
    ///
    /// Example:
    /// ```sql
    /// WINDOW mywindow AS prev_window
    /// ```
    ///
    /// [BigQuery]: https://cloud.google.com/bigquery/docs/reference/standard-sql/window-function-calls#ref_named_window
    NamedWindow(Ident),
    /// A window expression.
    ///
    /// Example:
    /// ```sql
    /// WINDOW mywindow AS (ORDER BY 1)
    /// ```
    WindowSpec(WindowSpec),
}

impl fmt::Display for NamedWindowExpr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            NamedWindowExpr::NamedWindow(named_window) => {
                write!(f, "{named_window}")?;
            }
            NamedWindowExpr::WindowSpec(window_spec) => {
                write!(f, "({window_spec})")?;
            }
        };
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct NamedWindowDefinition(pub Ident, pub NamedWindowExpr);

impl fmt::Display for NamedWindowDefinition {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} AS {}", self.0, self.1)
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct With {
    /// Token for the "WITH" keyword
    pub with_token: AttachedToken,
    pub recursive: bool,
    pub cte_tables: Vec<Cte>,
}

impl fmt::Display for With {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("WITH ")?;
        if self.recursive {
            f.write_str("RECURSIVE ")?;
        }
        display_comma_separated(&self.cte_tables).fmt(f)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum CteAsMaterialized {
    /// The `WITH` statement specifies `AS MATERIALIZED` behavior
    Materialized,
    /// The `WITH` statement specifies `AS NOT MATERIALIZED` behavior
    NotMaterialized,
}

impl fmt::Display for CteAsMaterialized {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            CteAsMaterialized::Materialized => {
                write!(f, "MATERIALIZED")?;
            }
            CteAsMaterialized::NotMaterialized => {
                write!(f, "NOT MATERIALIZED")?;
            }
        };
        Ok(())
    }
}

/// A single CTE (used after `WITH`): `<alias> [(col1, col2, ...)] AS <materialized> ( <query> )`
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
    pub materialized: Option<CteAsMaterialized>,
    /// Token for the closing parenthesis
    pub closing_paren_token: AttachedToken,
}

impl fmt::Display for Cte {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.materialized.as_ref() {
            None => {
                self.alias.fmt(f)?;
                f.write_str(" AS (")?;
                NewLine.fmt(f)?;
                Indent(&self.query).fmt(f)?;
                NewLine.fmt(f)?;
                f.write_str(")")?;
            }
            Some(materialized) => {
                self.alias.fmt(f)?;
                f.write_str(" AS ")?;
                materialized.fmt(f)?;
                f.write_str(" (")?;
                NewLine.fmt(f)?;
                Indent(&self.query).fmt(f)?;
                NewLine.fmt(f)?;
                f.write_str(")")?;
            }
        };
        if let Some(ref fr) = self.from {
            write!(f, " FROM {fr}")?;
        }
        Ok(())
    }
}

/// Represents an expression behind a wildcard expansion in a projection.
/// `SELECT T.* FROM T;
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum SelectItemQualifiedWildcardKind {
    /// Expression is an object name.
    /// e.g. `alias.*` or even `schema.table.*`
    ObjectName(ObjectName),
    /// Select star on an arbitrary expression.
    /// e.g. `STRUCT<STRING>('foo').*`
    Expr(Expr),
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
    /// An expression, followed by a wildcard expansion.
    /// e.g. `alias.*`, `STRUCT<STRING>('foo').*`
    QualifiedWildcard(SelectItemQualifiedWildcardKind, WildcardAdditionalOptions),
    /// An unqualified `*`
    Wildcard(WildcardAdditionalOptions),
}

impl fmt::Display for SelectItemQualifiedWildcardKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self {
            SelectItemQualifiedWildcardKind::ObjectName(object_name) => {
                write!(f, "{object_name}.*")
            }
            SelectItemQualifiedWildcardKind::Expr(expr) => write!(f, "{expr}.*"),
        }
    }
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
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct WildcardAdditionalOptions {
    /// The wildcard token `*`
    pub wildcard_token: AttachedToken,
    /// `[ILIKE...]`.
    ///  Snowflake syntax: <https://docs.snowflake.com/en/sql-reference/sql/select#parameters>
    pub opt_ilike: Option<IlikeSelectItem>,
    /// `[EXCLUDE...]`.
    pub opt_exclude: Option<ExcludeSelectItem>,
    /// `[EXCEPT...]`.
    ///  Clickhouse syntax: <https://clickhouse.com/docs/en/sql-reference/statements/select#except>
    pub opt_except: Option<ExceptSelectItem>,
    /// `[REPLACE]`
    ///  BigQuery syntax: <https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#select_replace>
    ///  Clickhouse syntax: <https://clickhouse.com/docs/en/sql-reference/statements/select#replace>
    ///  Snowflake syntax: <https://docs.snowflake.com/en/sql-reference/sql/select#parameters>
    pub opt_replace: Option<ReplaceSelectItem>,
    /// `[RENAME ...]`.
    pub opt_rename: Option<RenameSelectItem>,
}

impl Default for WildcardAdditionalOptions {
    fn default() -> Self {
        Self {
            wildcard_token: TokenWithSpan::wrap(Token::Mul).into(),
            opt_ilike: None,
            opt_exclude: None,
            opt_except: None,
            opt_replace: None,
            opt_rename: None,
        }
    }
}

impl fmt::Display for WildcardAdditionalOptions {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let Some(ilike) = &self.opt_ilike {
            write!(f, " {ilike}")?;
        }
        if let Some(exclude) = &self.opt_exclude {
            write!(f, " {exclude}")?;
        }
        if let Some(except) = &self.opt_except {
            write!(f, " {except}")?;
        }
        if let Some(replace) = &self.opt_replace {
            write!(f, " {replace}")?;
        }
        if let Some(rename) = &self.opt_rename {
            write!(f, " {rename}")?;
        }
        Ok(())
    }
}

/// Snowflake `ILIKE` information.
///
/// # Syntax
/// ```plaintext
/// ILIKE <value>
/// ```
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct IlikeSelectItem {
    pub pattern: String,
}

impl fmt::Display for IlikeSelectItem {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "ILIKE '{}'",
            value::escape_single_quote_string(&self.pattern)
        )?;
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
        use core::fmt::Write;
        match &self {
            SelectItem::UnnamedExpr(expr) => expr.fmt(f),
            SelectItem::ExprWithAlias { expr, alias } => {
                expr.fmt(f)?;
                f.write_str(" AS ")?;
                alias.fmt(f)
            }
            SelectItem::QualifiedWildcard(kind, additional_options) => {
                kind.fmt(f)?;
                additional_options.fmt(f)
            }
            SelectItem::Wildcard(additional_options) => {
                f.write_char('*')?;
                additional_options.fmt(f)
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
        self.relation.fmt(f)?;
        for join in &self.joins {
            SpaceOrNewline.fmt(f)?;
            join.fmt(f)?;
        }
        Ok(())
    }
}

/// Joins a table to itself to process hierarchical data in the table.
///
/// See <https://docs.snowflake.com/en/sql-reference/constructs/connect-by>.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct ConnectBy {
    /// START WITH
    pub condition: Expr,
    /// CONNECT BY
    pub relationships: Vec<Expr>,
}

impl fmt::Display for ConnectBy {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "START WITH {condition} CONNECT BY {relationships}",
            condition = self.condition,
            relationships = display_comma_separated(&self.relationships)
        )
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct Setting {
    pub key: Ident,
    pub value: Expr,
}

impl fmt::Display for Setting {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} = {}", self.key, self.value)
    }
}

/// An expression optionally followed by an alias.
///
/// Example:
/// ```sql
/// 42 AS myint
/// ```
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct ExprWithAlias {
    pub expr: Expr,
    pub alias: Option<Ident>,
}

impl fmt::Display for ExprWithAlias {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let ExprWithAlias { expr, alias } = self;
        write!(f, "{expr}")?;
        if let Some(alias) = alias {
            write!(f, " AS {alias}")?;
        }
        Ok(())
    }
}

/// An expression optionally followed by an alias and order by options.
///
/// Example:
/// ```sql
/// 42 AS myint ASC
/// ```
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct ExprWithAliasAndOrderBy {
    pub expr: ExprWithAlias,
    pub order_by: OrderByOptions,
}

impl fmt::Display for ExprWithAliasAndOrderBy {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}{}", self.expr, self.order_by)
    }
}

/// Arguments to a table-valued function
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct TableFunctionArgs {
    pub args: Vec<FunctionArg>,
    /// ClickHouse-specific SETTINGS clause.
    /// For example,
    /// `SELECT * FROM executable('generate_random.py', TabSeparated, 'id UInt32, random String', SETTINGS send_chunk_header = false, pool_size = 16)`
    /// [`executable` table function](https://clickhouse.com/docs/en/engines/table-functions/executable)
    pub settings: Option<Vec<Setting>>,
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum TableIndexHintType {
    Use,
    Ignore,
    Force,
}

impl fmt::Display for TableIndexHintType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(match self {
            TableIndexHintType::Use => "USE",
            TableIndexHintType::Ignore => "IGNORE",
            TableIndexHintType::Force => "FORCE",
        })
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum TableIndexType {
    Index,
    Key,
}

impl fmt::Display for TableIndexType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(match self {
            TableIndexType::Index => "INDEX",
            TableIndexType::Key => "KEY",
        })
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum TableIndexHintForClause {
    Join,
    OrderBy,
    GroupBy,
}

impl fmt::Display for TableIndexHintForClause {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(match self {
            TableIndexHintForClause::Join => "JOIN",
            TableIndexHintForClause::OrderBy => "ORDER BY",
            TableIndexHintForClause::GroupBy => "GROUP BY",
        })
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct TableIndexHints {
    pub hint_type: TableIndexHintType,
    pub index_type: TableIndexType,
    pub for_clause: Option<TableIndexHintForClause>,
    pub index_names: Vec<Ident>,
}

impl fmt::Display for TableIndexHints {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} {} ", self.hint_type, self.index_type)?;
        if let Some(for_clause) = &self.for_clause {
            write!(f, "FOR {for_clause} ")?;
        }
        write!(f, "({})", display_comma_separated(&self.index_names))
    }
}

/// A table name or a parenthesized subquery with an optional alias
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
#[cfg_attr(feature = "visitor", visit(with = "visit_table_factor"))]
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
        args: Option<TableFunctionArgs>,
        /// MSSQL-specific `WITH (...)` hints such as NOLOCK.
        with_hints: Vec<Expr>,
        /// Optional version qualifier to facilitate table time-travel, as
        /// supported by BigQuery and MSSQL.
        version: Option<TableVersion>,
        //  Optional table function modifier to generate the ordinality for column.
        /// For example, `SELECT * FROM generate_series(1, 10) WITH ORDINALITY AS t(a, b);`
        /// [WITH ORDINALITY](https://www.postgresql.org/docs/current/functions-srf.html), supported by Postgres.
        with_ordinality: bool,
        /// [Partition selection](https://dev.mysql.com/doc/refman/8.0/en/partitioning-selection.html), supported by MySQL.
        partitions: Vec<Ident>,
        /// Optional PartiQL JsonPath: <https://partiql.org/dql/from.html>
        json_path: Option<JsonPath>,
        /// Optional table sample modifier
        /// See: <https://jakewheat.github.io/sql-overview/sql-2016-foundation-grammar.html#sample-clause>
        sample: Option<TableSampleKind>,
        /// Optional index hints(mysql)
        /// See: <https://dev.mysql.com/doc/refman/8.4/en/index-hints.html>
        index_hints: Vec<TableIndexHints>,
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
    /// `e.g. LATERAL FLATTEN(<args>)[ AS <alias> ]`
    Function {
        lateral: bool,
        name: ObjectName,
        args: Vec<FunctionArg>,
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
        array_exprs: Vec<Expr>,
        with_offset: bool,
        with_offset_alias: Option<Ident>,
        with_ordinality: bool,
    },
    /// The `JSON_TABLE` table-valued function.
    /// Part of the SQL standard, but implemented only by MySQL, Oracle, and DB2.
    ///
    /// <https://modern-sql.com/blog/2017-06/whats-new-in-sql-2016#json_table>
    /// <https://dev.mysql.com/doc/refman/8.0/en/json-table-functions.html#function_json-table>
    ///
    /// ```sql
    /// SELECT * FROM JSON_TABLE(
    ///    '[{"a": 1, "b": 2}, {"a": 3, "b": 4}]',
    ///    '$[*]' COLUMNS(
    ///        a INT PATH '$.a' DEFAULT '0' ON EMPTY,
    ///        b INT PATH '$.b' NULL ON ERROR
    ///     )
    /// ) AS jt;
    /// ````
    JsonTable {
        /// The JSON expression to be evaluated. It must evaluate to a json string
        json_expr: Expr,
        /// The path to the array or object to be iterated over.
        /// It must evaluate to a json array or object.
        json_path: Value,
        /// The columns to be extracted from each element of the array or object.
        /// Each column must have a name and a type.
        columns: Vec<JsonTableColumn>,
        /// The alias for the table.
        alias: Option<TableAlias>,
    },
    /// The MSSQL's `OPENJSON` table-valued function.
    ///
    /// ```sql
    /// OPENJSON( jsonExpression [ , path ] )  [ <with_clause> ]
    ///
    /// <with_clause> ::= WITH ( { colName type [ column_path ] [ AS JSON ] } [ ,...n ] )
    /// ````
    ///
    /// Reference: <https://learn.microsoft.com/en-us/sql/t-sql/functions/openjson-transact-sql?view=sql-server-ver16#syntax>
    OpenJsonTable {
        /// The JSON expression to be evaluated. It must evaluate to a json string
        json_expr: Expr,
        /// The path to the array or object to be iterated over.
        /// It must evaluate to a json array or object.
        json_path: Option<Value>,
        /// The columns to be extracted from each element of the array or object.
        /// Each column must have a name and a type.
        columns: Vec<OpenJsonTableColumn>,
        /// The alias for the table.
        alias: Option<TableAlias>,
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
    ///
    /// [BigQuery](https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#pivot_operator)
    /// [Snowflake](https://docs.snowflake.com/en/sql-reference/constructs/pivot)
    Pivot {
        table: Box<TableFactor>,
        aggregate_functions: Vec<ExprWithAlias>, // Function expression
        value_column: Vec<Ident>,
        value_source: PivotValueSource,
        default_on_null: Option<Expr>,
        alias: Option<TableAlias>,
    },
    /// An UNPIVOT operation on a table.
    ///
    /// Syntax:
    /// ```sql
    /// table UNPIVOT [ { INCLUDE | EXCLUDE } NULLS ] (value FOR name IN (column1, [ column2, ... ])) [ alias ]
    /// ```
    ///
    /// See <https://docs.snowflake.com/en/sql-reference/constructs/unpivot>.
    /// See <https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-qry-select-unpivot>.
    Unpivot {
        table: Box<TableFactor>,
        value: Expr,
        name: Ident,
        columns: Vec<ExprWithAlias>,
        null_inclusion: Option<NullInclusion>,
        alias: Option<TableAlias>,
    },
    /// A `MATCH_RECOGNIZE` operation on a table.
    ///
    /// See <https://docs.snowflake.com/en/sql-reference/constructs/match_recognize>.
    MatchRecognize {
        table: Box<TableFactor>,
        /// `PARTITION BY <expr> [, ... ]`
        partition_by: Vec<Expr>,
        /// `ORDER BY <expr> [, ... ]`
        order_by: Vec<OrderByExpr>,
        /// `MEASURES <expr> [AS] <alias> [, ... ]`
        measures: Vec<Measure>,
        /// `ONE ROW PER MATCH | ALL ROWS PER MATCH [ <option> ]`
        rows_per_match: Option<RowsPerMatch>,
        /// `AFTER MATCH SKIP <option>`
        after_match_skip: Option<AfterMatchSkip>,
        /// `PATTERN ( <pattern> )`
        pattern: MatchRecognizePattern,
        /// `DEFINE <symbol> AS <expr> [, ... ]`
        symbols: Vec<SymbolDefinition>,
        alias: Option<TableAlias>,
    },
    /// The `XMLTABLE` table-valued function.
    /// Part of the SQL standard, supported by PostgreSQL, Oracle, and DB2.
    ///
    /// <https://www.postgresql.org/docs/15/functions-xml.html#FUNCTIONS-XML-PROCESSING>
    ///
    /// ```sql
    /// SELECT xmltable.*
    /// FROM xmldata,
    /// XMLTABLE('//ROWS/ROW'
    ///     PASSING data
    ///     COLUMNS id int PATH '@id',
    ///     ordinality FOR ORDINALITY,
    ///     "COUNTRY_NAME" text,
    ///     country_id text PATH 'COUNTRY_ID',
    ///     size_sq_km float PATH 'SIZE[@unit = "sq_km"]',
    ///     size_other text PATH 'concat(SIZE[@unit!="sq_km"], " ", SIZE[@unit!="sq_km"]/@unit)',
    ///     premier_name text PATH 'PREMIER_NAME' DEFAULT 'not specified'
    /// );
    /// ````
    XmlTable {
        /// Optional XMLNAMESPACES clause (empty if not present)
        namespaces: Vec<XmlNamespaceDefinition>,
        /// The row-generating XPath expression.
        row_expression: Expr,
        /// The PASSING clause specifying the document expression.
        passing: XmlPassingClause,
        /// The columns to be extracted from each generated row.
        columns: Vec<XmlTableColumn>,
        /// The alias for the table.
        alias: Option<TableAlias>,
    },
}

/// The table sample modifier options
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum TableSampleKind {
    /// Table sample located before the table alias option
    BeforeTableAlias(Box<TableSample>),
    /// Table sample located after the table alias option
    AfterTableAlias(Box<TableSample>),
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct TableSample {
    pub modifier: TableSampleModifier,
    pub name: Option<TableSampleMethod>,
    pub quantity: Option<TableSampleQuantity>,
    pub seed: Option<TableSampleSeed>,
    pub bucket: Option<TableSampleBucket>,
    pub offset: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum TableSampleModifier {
    Sample,
    TableSample,
}

impl fmt::Display for TableSampleModifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TableSampleModifier::Sample => write!(f, "SAMPLE")?,
            TableSampleModifier::TableSample => write!(f, "TABLESAMPLE")?,
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct TableSampleQuantity {
    pub parenthesized: bool,
    pub value: Expr,
    pub unit: Option<TableSampleUnit>,
}

impl fmt::Display for TableSampleQuantity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.parenthesized {
            write!(f, "(")?;
        }
        write!(f, "{}", self.value)?;
        if let Some(unit) = &self.unit {
            write!(f, " {unit}")?;
        }
        if self.parenthesized {
            write!(f, ")")?;
        }
        Ok(())
    }
}

/// The table sample method names
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum TableSampleMethod {
    Row,
    Bernoulli,
    System,
    Block,
}

impl fmt::Display for TableSampleMethod {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TableSampleMethod::Bernoulli => write!(f, "BERNOULLI"),
            TableSampleMethod::Row => write!(f, "ROW"),
            TableSampleMethod::System => write!(f, "SYSTEM"),
            TableSampleMethod::Block => write!(f, "BLOCK"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct TableSampleSeed {
    pub modifier: TableSampleSeedModifier,
    pub value: Value,
}

impl fmt::Display for TableSampleSeed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ({})", self.modifier, self.value)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum TableSampleSeedModifier {
    Repeatable,
    Seed,
}

impl fmt::Display for TableSampleSeedModifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TableSampleSeedModifier::Repeatable => write!(f, "REPEATABLE"),
            TableSampleSeedModifier::Seed => write!(f, "SEED"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum TableSampleUnit {
    Rows,
    Percent,
}

impl fmt::Display for TableSampleUnit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TableSampleUnit::Percent => write!(f, "PERCENT"),
            TableSampleUnit::Rows => write!(f, "ROWS"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct TableSampleBucket {
    pub bucket: Value,
    pub total: Value,
    pub on: Option<Expr>,
}

impl fmt::Display for TableSampleBucket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BUCKET {} OUT OF {}", self.bucket, self.total)?;
        if let Some(on) = &self.on {
            write!(f, " ON {on}")?;
        }
        Ok(())
    }
}
impl fmt::Display for TableSample {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.modifier)?;
        if let Some(name) = &self.name {
            write!(f, " {name}")?;
        }
        if let Some(quantity) = &self.quantity {
            write!(f, " {quantity}")?;
        }
        if let Some(seed) = &self.seed {
            write!(f, " {seed}")?;
        }
        if let Some(bucket) = &self.bucket {
            write!(f, " ({bucket})")?;
        }
        if let Some(offset) = &self.offset {
            write!(f, " OFFSET {offset}")?;
        }
        Ok(())
    }
}

/// The source of values in a `PIVOT` operation.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum PivotValueSource {
    /// Pivot on a static list of values.
    ///
    /// See <https://docs.snowflake.com/en/sql-reference/constructs/pivot#pivot-on-a-specified-list-of-column-values-for-the-pivot-column>.
    List(Vec<ExprWithAlias>),
    /// Pivot on all distinct values of the pivot column.
    ///
    /// See <https://docs.snowflake.com/en/sql-reference/constructs/pivot#pivot-on-all-distinct-column-values-automatically-with-dynamic-pivot>.
    Any(Vec<OrderByExpr>),
    /// Pivot on all values returned by a subquery.
    ///
    /// See <https://docs.snowflake.com/en/sql-reference/constructs/pivot#pivot-on-column-values-using-a-subquery-with-dynamic-pivot>.
    Subquery(Box<Query>),
}

impl fmt::Display for PivotValueSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PivotValueSource::List(values) => write!(f, "{}", display_comma_separated(values)),
            PivotValueSource::Any(order_by) => {
                write!(f, "ANY")?;
                if !order_by.is_empty() {
                    write!(f, " ORDER BY {}", display_comma_separated(order_by))?;
                }
                Ok(())
            }
            PivotValueSource::Subquery(query) => write!(f, "{query}"),
        }
    }
}

/// An item in the `MEASURES` subclause of a `MATCH_RECOGNIZE` operation.
///
/// See <https://docs.snowflake.com/en/sql-reference/constructs/match_recognize#measures-specifying-additional-output-columns>.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct Measure {
    pub expr: Expr,
    pub alias: Ident,
}

impl fmt::Display for Measure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} AS {}", self.expr, self.alias)
    }
}

/// The rows per match option in a `MATCH_RECOGNIZE` operation.
///
/// See <https://docs.snowflake.com/en/sql-reference/constructs/match_recognize#row-s-per-match-specifying-the-rows-to-return>.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum RowsPerMatch {
    /// `ONE ROW PER MATCH`
    OneRow,
    /// `ALL ROWS PER MATCH <mode>`
    AllRows(Option<EmptyMatchesMode>),
}

impl fmt::Display for RowsPerMatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RowsPerMatch::OneRow => write!(f, "ONE ROW PER MATCH"),
            RowsPerMatch::AllRows(mode) => {
                write!(f, "ALL ROWS PER MATCH")?;
                if let Some(mode) = mode {
                    write!(f, " {mode}")?;
                }
                Ok(())
            }
        }
    }
}

/// The after match skip option in a `MATCH_RECOGNIZE` operation.
///
/// See <https://docs.snowflake.com/en/sql-reference/constructs/match_recognize#after-match-skip-specifying-where-to-continue-after-a-match>.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum AfterMatchSkip {
    /// `PAST LAST ROW`
    PastLastRow,
    /// `TO NEXT ROW`
    ToNextRow,
    /// `TO FIRST <symbol>`
    ToFirst(Ident),
    /// `TO LAST <symbol>`
    ToLast(Ident),
}

impl fmt::Display for AfterMatchSkip {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "AFTER MATCH SKIP ")?;
        match self {
            AfterMatchSkip::PastLastRow => write!(f, "PAST LAST ROW"),
            AfterMatchSkip::ToNextRow => write!(f, " TO NEXT ROW"),
            AfterMatchSkip::ToFirst(symbol) => write!(f, "TO FIRST {symbol}"),
            AfterMatchSkip::ToLast(symbol) => write!(f, "TO LAST {symbol}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum EmptyMatchesMode {
    /// `SHOW EMPTY MATCHES`
    Show,
    /// `OMIT EMPTY MATCHES`
    Omit,
    /// `WITH UNMATCHED ROWS`
    WithUnmatched,
}

impl fmt::Display for EmptyMatchesMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EmptyMatchesMode::Show => write!(f, "SHOW EMPTY MATCHES"),
            EmptyMatchesMode::Omit => write!(f, "OMIT EMPTY MATCHES"),
            EmptyMatchesMode::WithUnmatched => write!(f, "WITH UNMATCHED ROWS"),
        }
    }
}

/// A symbol defined in a `MATCH_RECOGNIZE` operation.
///
/// See <https://docs.snowflake.com/en/sql-reference/constructs/match_recognize#define-defining-symbols>.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct SymbolDefinition {
    pub symbol: Ident,
    pub definition: Expr,
}

impl fmt::Display for SymbolDefinition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} AS {}", self.symbol, self.definition)
    }
}

/// A symbol in a `MATCH_RECOGNIZE` pattern.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum MatchRecognizeSymbol {
    /// A named symbol, e.g. `S1`.
    Named(Ident),
    /// A virtual symbol representing the start of the of partition (`^`).
    Start,
    /// A virtual symbol representing the end of the partition (`$`).
    End,
}

impl fmt::Display for MatchRecognizeSymbol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MatchRecognizeSymbol::Named(symbol) => write!(f, "{symbol}"),
            MatchRecognizeSymbol::Start => write!(f, "^"),
            MatchRecognizeSymbol::End => write!(f, "$"),
        }
    }
}

/// The pattern in a `MATCH_RECOGNIZE` operation.
///
/// See <https://docs.snowflake.com/en/sql-reference/constructs/match_recognize#pattern-specifying-the-pattern-to-match>.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum MatchRecognizePattern {
    /// A named symbol such as `S1` or a virtual symbol such as `^`.
    Symbol(MatchRecognizeSymbol),
    /// {- symbol -}
    Exclude(MatchRecognizeSymbol),
    /// PERMUTE(symbol_1, ..., symbol_n)
    Permute(Vec<MatchRecognizeSymbol>),
    /// pattern_1 pattern_2 ... pattern_n
    Concat(Vec<MatchRecognizePattern>),
    /// ( pattern )
    Group(Box<MatchRecognizePattern>),
    /// pattern_1 | pattern_2 | ... | pattern_n
    Alternation(Vec<MatchRecognizePattern>),
    /// e.g. pattern*
    Repetition(Box<MatchRecognizePattern>, RepetitionQuantifier),
}

impl fmt::Display for MatchRecognizePattern {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use MatchRecognizePattern::*;
        match self {
            Symbol(symbol) => write!(f, "{symbol}"),
            Exclude(symbol) => write!(f, "{{- {symbol} -}}"),
            Permute(symbols) => write!(f, "PERMUTE({})", display_comma_separated(symbols)),
            Concat(patterns) => write!(f, "{}", display_separated(patterns, " ")),
            Group(pattern) => write!(f, "( {pattern} )"),
            Alternation(patterns) => write!(f, "{}", display_separated(patterns, " | ")),
            Repetition(pattern, op) => write!(f, "{pattern}{op}"),
        }
    }
}

/// Determines the minimum and maximum allowed occurrences of a pattern in a
/// `MATCH_RECOGNIZE` operation.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum RepetitionQuantifier {
    /// `*`
    ZeroOrMore,
    /// `+`
    OneOrMore,
    /// `?`
    AtMostOne,
    /// `{n}`
    Exactly(u32),
    /// `{n,}`
    AtLeast(u32),
    /// `{,n}`
    AtMost(u32),
    /// `{n,m}
    Range(u32, u32),
}

impl fmt::Display for RepetitionQuantifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use RepetitionQuantifier::*;
        match self {
            ZeroOrMore => write!(f, "*"),
            OneOrMore => write!(f, "+"),
            AtMostOne => write!(f, "?"),
            Exactly(n) => write!(f, "{{{n}}}"),
            AtLeast(n) => write!(f, "{{{n},}}"),
            AtMost(n) => write!(f, "{{,{n}}}"),
            Range(n, m) => write!(f, "{{{n},{m}}}"),
        }
    }
}

impl fmt::Display for TableFactor {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            TableFactor::Table {
                name,
                alias,
                args,
                with_hints,
                version,
                partitions,
                with_ordinality,
                json_path,
                sample,
                index_hints,
            } => {
                name.fmt(f)?;
                if let Some(json_path) = json_path {
                    json_path.fmt(f)?;
                }
                if !partitions.is_empty() {
                    write!(f, "PARTITION ({})", display_comma_separated(partitions))?;
                }
                if let Some(args) = args {
                    write!(f, "(")?;
                    write!(f, "{}", display_comma_separated(&args.args))?;
                    if let Some(ref settings) = args.settings {
                        if !args.args.is_empty() {
                            write!(f, ", ")?;
                        }
                        write!(f, "SETTINGS {}", display_comma_separated(settings))?;
                    }
                    write!(f, ")")?;
                }
                if *with_ordinality {
                    write!(f, " WITH ORDINALITY")?;
                }
                if let Some(TableSampleKind::BeforeTableAlias(sample)) = sample {
                    write!(f, " {sample}")?;
                }
                if let Some(alias) = alias {
                    write!(f, " AS {alias}")?;
                }
                if !index_hints.is_empty() {
                    write!(f, " {}", display_separated(index_hints, " "))?;
                }
                if !with_hints.is_empty() {
                    write!(f, " WITH ({})", display_comma_separated(with_hints))?;
                }
                if let Some(version) = version {
                    write!(f, "{version}")?;
                }
                if let Some(TableSampleKind::AfterTableAlias(sample)) = sample {
                    write!(f, " {sample}")?;
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
                f.write_str("(")?;
                NewLine.fmt(f)?;
                Indent(subquery).fmt(f)?;
                NewLine.fmt(f)?;
                f.write_str(")")?;
                if let Some(alias) = alias {
                    write!(f, " AS {alias}")?;
                }
                Ok(())
            }
            TableFactor::Function {
                lateral,
                name,
                args,
                alias,
            } => {
                if *lateral {
                    write!(f, "LATERAL ")?;
                }
                write!(f, "{name}")?;
                write!(f, "({})", display_comma_separated(args))?;
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
                array_exprs,
                with_offset,
                with_offset_alias,
                with_ordinality,
            } => {
                write!(f, "UNNEST({})", display_comma_separated(array_exprs))?;

                if *with_ordinality {
                    write!(f, " WITH ORDINALITY")?;
                }

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
            TableFactor::JsonTable {
                json_expr,
                json_path,
                columns,
                alias,
            } => {
                write!(
                    f,
                    "JSON_TABLE({json_expr}, {json_path} COLUMNS({columns}))",
                    columns = display_comma_separated(columns)
                )?;
                if let Some(alias) = alias {
                    write!(f, " AS {alias}")?;
                }
                Ok(())
            }
            TableFactor::OpenJsonTable {
                json_expr,
                json_path,
                columns,
                alias,
            } => {
                write!(f, "OPENJSON({json_expr}")?;
                if let Some(json_path) = json_path {
                    write!(f, ", {json_path}")?;
                }
                write!(f, ")")?;
                if !columns.is_empty() {
                    write!(f, " WITH ({})", display_comma_separated(columns))?;
                }
                if let Some(alias) = alias {
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
                table,
                aggregate_functions,
                value_column,
                value_source,
                default_on_null,
                alias,
            } => {
                write!(
                    f,
                    "{table} PIVOT({} FOR {} IN ({value_source})",
                    display_comma_separated(aggregate_functions),
                    Expr::CompoundIdentifier(value_column.to_vec()),
                )?;
                if let Some(expr) = default_on_null {
                    write!(f, " DEFAULT ON NULL ({expr})")?;
                }
                write!(f, ")")?;
                if alias.is_some() {
                    write!(f, " AS {}", alias.as_ref().unwrap())?;
                }
                Ok(())
            }
            TableFactor::Unpivot {
                table,
                null_inclusion,
                value,
                name,
                columns,
                alias,
            } => {
                write!(f, "{table} UNPIVOT")?;
                if let Some(null_inclusion) = null_inclusion {
                    write!(f, " {null_inclusion} ")?;
                }
                write!(
                    f,
                    "({} FOR {} IN ({}))",
                    value,
                    name,
                    display_comma_separated(columns)
                )?;
                if alias.is_some() {
                    write!(f, " AS {}", alias.as_ref().unwrap())?;
                }
                Ok(())
            }
            TableFactor::MatchRecognize {
                table,
                partition_by,
                order_by,
                measures,
                rows_per_match,
                after_match_skip,
                pattern,
                symbols,
                alias,
            } => {
                write!(f, "{table} MATCH_RECOGNIZE(")?;
                if !partition_by.is_empty() {
                    write!(f, "PARTITION BY {} ", display_comma_separated(partition_by))?;
                }
                if !order_by.is_empty() {
                    write!(f, "ORDER BY {} ", display_comma_separated(order_by))?;
                }
                if !measures.is_empty() {
                    write!(f, "MEASURES {} ", display_comma_separated(measures))?;
                }
                if let Some(rows_per_match) = rows_per_match {
                    write!(f, "{rows_per_match} ")?;
                }
                if let Some(after_match_skip) = after_match_skip {
                    write!(f, "{after_match_skip} ")?;
                }
                write!(f, "PATTERN ({pattern}) ")?;
                write!(f, "DEFINE {})", display_comma_separated(symbols))?;
                if alias.is_some() {
                    write!(f, " AS {}", alias.as_ref().unwrap())?;
                }
                Ok(())
            }
            TableFactor::XmlTable {
                row_expression,
                passing,
                columns,
                alias,
                namespaces,
            } => {
                write!(f, "XMLTABLE(")?;
                if !namespaces.is_empty() {
                    write!(
                        f,
                        "XMLNAMESPACES({}), ",
                        display_comma_separated(namespaces)
                    )?;
                }
                write!(
                    f,
                    "{row_expression}{passing} COLUMNS {columns})",
                    columns = display_comma_separated(columns)
                )?;
                if let Some(alias) = alias {
                    write!(f, " AS {alias}")?;
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
    pub columns: Vec<TableAliasColumnDef>,
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

/// SQL column definition in a table expression alias.
/// Most of the time, the data type is not specified.
/// But some table-valued functions do require specifying the data type.
///
/// See <https://www.postgresql.org/docs/17/queries-table-expressions.html#QUERIES-TABLEFUNCTIONS>
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct TableAliasColumnDef {
    /// Column name alias
    pub name: Ident,
    /// Some table-valued functions require specifying the data type in the alias.
    pub data_type: Option<DataType>,
}

impl TableAliasColumnDef {
    /// Create a new table alias column definition with only a name and no type
    pub fn from_name<S: Into<String>>(name: S) -> Self {
        TableAliasColumnDef {
            name: Ident::new(name),
            data_type: None,
        }
    }
}

impl fmt::Display for TableAliasColumnDef {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.name)?;
        if let Some(ref data_type) = self.data_type {
            write!(f, " {data_type}")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum TableVersion {
    /// When the table version is defined using `FOR SYSTEM_TIME AS OF`.
    /// For example: `SELECT * FROM tbl FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)`
    ForSystemTimeAsOf(Expr),
    /// When the table version is defined using a function.
    /// For example: `SELECT * FROM tbl AT(TIMESTAMP => '2020-08-14 09:30:00')`
    Function(Expr),
}

impl Display for TableVersion {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            TableVersion::ForSystemTimeAsOf(e) => write!(f, " FOR SYSTEM_TIME AS OF {e}")?,
            TableVersion::Function(func) => write!(f, " {func}")?,
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct Join {
    pub relation: TableFactor,
    /// ClickHouse supports the optional `GLOBAL` keyword before the join operator.
    /// See [ClickHouse](https://clickhouse.com/docs/en/sql-reference/statements/select/join)
    pub global: bool,
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
            impl fmt::Display for Suffix<'_> {
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
        if self.global {
            write!(f, "GLOBAL ")?;
        }

        match &self.join_operator {
            JoinOperator::Join(constraint) => f.write_fmt(format_args!(
                "{}JOIN {}{}",
                prefix(constraint),
                self.relation,
                suffix(constraint)
            )),
            JoinOperator::Inner(constraint) => f.write_fmt(format_args!(
                "{}INNER JOIN {}{}",
                prefix(constraint),
                self.relation,
                suffix(constraint)
            )),
            JoinOperator::Left(constraint) => f.write_fmt(format_args!(
                "{}LEFT JOIN {}{}",
                prefix(constraint),
                self.relation,
                suffix(constraint)
            )),
            JoinOperator::LeftOuter(constraint) => f.write_fmt(format_args!(
                "{}LEFT OUTER JOIN {}{}",
                prefix(constraint),
                self.relation,
                suffix(constraint)
            )),
            JoinOperator::Right(constraint) => f.write_fmt(format_args!(
                "{}RIGHT JOIN {}{}",
                prefix(constraint),
                self.relation,
                suffix(constraint)
            )),
            JoinOperator::RightOuter(constraint) => f.write_fmt(format_args!(
                "{}RIGHT OUTER JOIN {}{}",
                prefix(constraint),
                self.relation,
                suffix(constraint)
            )),
            JoinOperator::FullOuter(constraint) => f.write_fmt(format_args!(
                "{}FULL JOIN {}{}",
                prefix(constraint),
                self.relation,
                suffix(constraint)
            )),
            JoinOperator::CrossJoin => f.write_fmt(format_args!("CROSS JOIN {}", self.relation)),
            JoinOperator::Semi(constraint) => f.write_fmt(format_args!(
                "{}SEMI JOIN {}{}",
                prefix(constraint),
                self.relation,
                suffix(constraint)
            )),
            JoinOperator::LeftSemi(constraint) => f.write_fmt(format_args!(
                "{}LEFT SEMI JOIN {}{}",
                prefix(constraint),
                self.relation,
                suffix(constraint)
            )),
            JoinOperator::RightSemi(constraint) => f.write_fmt(format_args!(
                "{}RIGHT SEMI JOIN {}{}",
                prefix(constraint),
                self.relation,
                suffix(constraint)
            )),
            JoinOperator::Anti(constraint) => f.write_fmt(format_args!(
                "{}ANTI JOIN {}{}",
                prefix(constraint),
                self.relation,
                suffix(constraint)
            )),
            JoinOperator::LeftAnti(constraint) => f.write_fmt(format_args!(
                "{}LEFT ANTI JOIN {}{}",
                prefix(constraint),
                self.relation,
                suffix(constraint)
            )),
            JoinOperator::RightAnti(constraint) => f.write_fmt(format_args!(
                "{}RIGHT ANTI JOIN {}{}",
                prefix(constraint),
                self.relation,
                suffix(constraint)
            )),
            JoinOperator::CrossApply => f.write_fmt(format_args!("CROSS APPLY {}", self.relation)),
            JoinOperator::OuterApply => f.write_fmt(format_args!("OUTER APPLY {}", self.relation)),
            JoinOperator::AsOf {
                match_condition,
                constraint,
            } => f.write_fmt(format_args!(
                "ASOF JOIN {} MATCH_CONDITION ({match_condition}){}",
                self.relation,
                suffix(constraint)
            )),
            JoinOperator::StraightJoin(constraint) => f.write_fmt(format_args!(
                "STRAIGHT_JOIN {}{}",
                self.relation,
                suffix(constraint)
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum JoinOperator {
    Join(JoinConstraint),
    Inner(JoinConstraint),
    Left(JoinConstraint),
    LeftOuter(JoinConstraint),
    Right(JoinConstraint),
    RightOuter(JoinConstraint),
    FullOuter(JoinConstraint),
    CrossJoin,
    /// SEMI (non-standard)
    Semi(JoinConstraint),
    /// LEFT SEMI (non-standard)
    LeftSemi(JoinConstraint),
    /// RIGHT SEMI (non-standard)
    RightSemi(JoinConstraint),
    /// ANTI (non-standard)
    Anti(JoinConstraint),
    /// LEFT ANTI (non-standard)
    LeftAnti(JoinConstraint),
    /// RIGHT ANTI (non-standard)
    RightAnti(JoinConstraint),
    /// CROSS APPLY (non-standard)
    CrossApply,
    /// OUTER APPLY (non-standard)
    OuterApply,
    /// `ASOF` joins are used for joining tables containing time-series data
    /// whose timestamp columns do not match exactly.
    ///
    /// See <https://docs.snowflake.com/en/sql-reference/constructs/asof-join>.
    AsOf {
        match_condition: Expr,
        constraint: JoinConstraint,
    },
    /// STRAIGHT_JOIN (non-standard)
    ///
    /// See <https://dev.mysql.com/doc/refman/8.4/en/join.html>.
    StraightJoin(JoinConstraint),
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum JoinConstraint {
    On(Expr),
    Using(Vec<ObjectName>),
    Natural,
    None,
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum OrderByKind {
    /// ALL syntax of [DuckDB] and [ClickHouse].
    ///
    /// [DuckDB]:  <https://duckdb.org/docs/sql/query_syntax/orderby>
    /// [ClickHouse]: <https://clickhouse.com/docs/en/sql-reference/statements/select/order-by>
    All(OrderByOptions),

    /// Expressions
    Expressions(Vec<OrderByExpr>),
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct OrderBy {
    pub kind: OrderByKind,

    /// Optional: `INTERPOLATE`
    /// Supported by [ClickHouse syntax]
    pub interpolate: Option<Interpolate>,
}

impl fmt::Display for OrderBy {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ORDER BY")?;
        match &self.kind {
            OrderByKind::Expressions(exprs) => {
                write!(f, " {}", display_comma_separated(exprs))?;
            }
            OrderByKind::All(all) => {
                write!(f, " ALL{all}")?;
            }
        }

        if let Some(ref interpolate) = self.interpolate {
            match &interpolate.exprs {
                Some(exprs) => write!(f, " INTERPOLATE ({})", display_comma_separated(exprs))?,
                None => write!(f, " INTERPOLATE")?,
            }
        }

        Ok(())
    }
}

/// An `ORDER BY` expression
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct OrderByExpr {
    pub expr: Expr,
    pub options: OrderByOptions,
    /// Optional: `WITH FILL`
    /// Supported by [ClickHouse syntax]: <https://clickhouse.com/docs/en/sql-reference/statements/select/order-by#order-by-expr-with-fill-modifier>
    pub with_fill: Option<WithFill>,
}

impl fmt::Display for OrderByExpr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}{}", self.expr, self.options)?;
        if let Some(ref with_fill) = self.with_fill {
            write!(f, " {with_fill}")?
        }
        Ok(())
    }
}

/// ClickHouse `WITH FILL` modifier for `ORDER BY` clause.
/// Supported by [ClickHouse syntax]
///
/// [ClickHouse syntax]: <https://clickhouse.com/docs/en/sql-reference/statements/select/order-by#order-by-expr-with-fill-modifier>
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct WithFill {
    pub from: Option<Expr>,
    pub to: Option<Expr>,
    pub step: Option<Expr>,
}

impl fmt::Display for WithFill {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "WITH FILL")?;
        if let Some(ref from) = self.from {
            write!(f, " FROM {from}")?;
        }
        if let Some(ref to) = self.to {
            write!(f, " TO {to}")?;
        }
        if let Some(ref step) = self.step {
            write!(f, " STEP {step}")?;
        }
        Ok(())
    }
}

/// ClickHouse `INTERPOLATE` clause for use in `ORDER BY` clause when using `WITH FILL` modifier.
/// Supported by [ClickHouse syntax]
///
/// [ClickHouse syntax]: <https://clickhouse.com/docs/en/sql-reference/statements/select/order-by#order-by-expr-with-fill-modifier>
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct InterpolateExpr {
    pub column: Ident,
    pub expr: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct Interpolate {
    pub exprs: Option<Vec<InterpolateExpr>>,
}

impl fmt::Display for InterpolateExpr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.column)?;
        if let Some(ref expr) = self.expr {
            write!(f, " AS {expr}")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct OrderByOptions {
    /// Optional `ASC` or `DESC`
    pub asc: Option<bool>,
    /// Optional `NULLS FIRST` or `NULLS LAST`
    pub nulls_first: Option<bool>,
}

impl fmt::Display for OrderByOptions {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
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
pub enum LimitClause {
    /// Standard SQL syntax
    ///
    /// `LIMIT <limit> [BY <expr>,<expr>,...] [OFFSET <offset>]`
    LimitOffset {
        /// `LIMIT { <N> | ALL }`
        limit: Option<Expr>,
        /// `OFFSET <N> [ { ROW | ROWS } ]`
        offset: Option<Offset>,
        /// `BY { <expr>,<expr>,... } }`
        ///
        /// [ClickHouse](https://clickhouse.com/docs/sql-reference/statements/select/limit-by)
        limit_by: Vec<Expr>,
    },
    /// [MySQL]-specific syntax; the order of expressions is reversed.
    ///
    /// `LIMIT <offset>, <limit>`
    ///
    /// [MySQL]: https://dev.mysql.com/doc/refman/8.4/en/select.html
    OffsetCommaLimit { offset: Expr, limit: Expr },
}

impl fmt::Display for LimitClause {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            LimitClause::LimitOffset {
                limit,
                limit_by,
                offset,
            } => {
                if let Some(ref limit) = limit {
                    write!(f, " LIMIT {limit}")?;
                }
                if let Some(ref offset) = offset {
                    write!(f, " {offset}")?;
                }
                if !limit_by.is_empty() {
                    debug_assert!(limit.is_some());
                    write!(f, " BY {}", display_separated(limit_by, ", "))?;
                }
                Ok(())
            }
            LimitClause::OffsetCommaLimit { offset, limit } => {
                write!(f, " LIMIT {offset}, {limit}")
            }
        }
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

/// Pipe syntax, first introduced in Google BigQuery.
/// Example:
///
/// ```sql
/// FROM Produce
/// |> WHERE sales > 0
/// |> AGGREGATE SUM(sales) AS total_sales, COUNT(*) AS num_sales
///    GROUP BY item;
/// ```
///
/// See <https://cloud.google.com/bigquery/docs/reference/standard-sql/pipe-syntax#pipe_syntax>
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum PipeOperator {
    /// Limits the number of rows to return in a query, with an optional OFFSET clause to skip over rows.
    ///
    /// Syntax: `|> LIMIT <n> [OFFSET <m>]`
    ///
    /// See more at <https://cloud.google.com/bigquery/docs/reference/standard-sql/pipe-syntax#limit_pipe_operator>
    Limit { expr: Expr, offset: Option<Expr> },
    /// Filters the results of the input table.
    ///
    /// Syntax: `|> WHERE <condition>`
    ///
    /// See more at <https://cloud.google.com/bigquery/docs/reference/standard-sql/pipe-syntax#where_pipe_operator>
    Where { expr: Expr },
    /// `ORDER BY <expr> [ASC|DESC], ...`
    OrderBy { exprs: Vec<OrderByExpr> },
    /// Produces a new table with the listed columns, similar to the outermost SELECT clause in a table subquery in standard syntax.
    ///
    /// Syntax `|> SELECT <expr> [[AS] alias], ...`
    ///
    /// See more at <https://cloud.google.com/bigquery/docs/reference/standard-sql/pipe-syntax#select_pipe_operator>
    Select { exprs: Vec<SelectItem> },
    /// Propagates the existing table and adds computed columns, similar to SELECT *, new_column in standard syntax.
    ///
    /// Syntax: `|> EXTEND <expr> [[AS] alias], ...`
    ///
    /// See more at <https://cloud.google.com/bigquery/docs/reference/standard-sql/pipe-syntax#extend_pipe_operator>
    Extend { exprs: Vec<SelectItem> },
    /// Replaces the value of a column in the current table, similar to SELECT * REPLACE (expression AS column) in standard syntax.
    ///
    /// Syntax: `|> SET <column> = <expression>, ...`
    ///
    /// See more at <https://cloud.google.com/bigquery/docs/reference/standard-sql/pipe-syntax#set_pipe_operator>
    Set { assignments: Vec<Assignment> },
    /// Removes listed columns from the current table, similar to SELECT * EXCEPT (column) in standard syntax.
    ///
    /// Syntax: `|> DROP <column>, ...`
    ///
    /// See more at <https://cloud.google.com/bigquery/docs/reference/standard-sql/pipe-syntax#drop_pipe_operator>
    Drop { columns: Vec<Ident> },
    /// Introduces a table alias for the input table, similar to applying the AS alias clause on a table subquery in standard syntax.
    ///
    /// Syntax: `|> AS <alias>`
    ///
    /// See more at <https://cloud.google.com/bigquery/docs/reference/standard-sql/pipe-syntax#as_pipe_operator>
    As { alias: Ident },
    /// Performs aggregation on data across grouped rows or an entire table.
    ///
    /// Syntax: `|> AGGREGATE <agg_expr> [[AS] alias], ...`
    ///
    /// Syntax:
    /// ```norust
    /// |> AGGREGATE [<agg_expr> [[AS] alias], ...]
    /// GROUP BY <grouping_expr> [AS alias], ...
    /// ```
    ///
    /// See more at <https://cloud.google.com/bigquery/docs/reference/standard-sql/pipe-syntax#aggregate_pipe_operator>
    Aggregate {
        full_table_exprs: Vec<ExprWithAliasAndOrderBy>,
        group_by_expr: Vec<ExprWithAliasAndOrderBy>,
    },
    /// Selects a random sample of rows from the input table.
    /// Syntax: `|> TABLESAMPLE SYSTEM (10 PERCENT)
    /// See more at <https://cloud.google.com/bigquery/docs/reference/standard-sql/pipe-syntax#tablesample_pipe_operator>
    TableSample { sample: Box<TableSample> },
    /// Renames columns in the input table.
    ///
    /// Syntax: `|> RENAME old_name AS new_name, ...`
    ///
    /// See more at <https://cloud.google.com/bigquery/docs/reference/standard-sql/pipe-syntax#rename_pipe_operator>
    Rename { mappings: Vec<IdentWithAlias> },
    /// Combines the input table with one or more tables using UNION.
    ///
    /// Syntax: `|> UNION [ALL|DISTINCT] (<query>), (<query>), ...`
    ///
    /// See more at <https://cloud.google.com/bigquery/docs/reference/standard-sql/pipe-syntax#union_pipe_operator>
    Union {
        set_quantifier: SetQuantifier,
        queries: Vec<Query>,
    },
    /// Returns only the rows that are present in both the input table and the specified tables.
    ///
    /// Syntax: `|> INTERSECT [DISTINCT] (<query>), (<query>), ...`
    ///
    /// See more at <https://cloud.google.com/bigquery/docs/reference/standard-sql/pipe-syntax#intersect_pipe_operator>
    Intersect {
        set_quantifier: SetQuantifier,
        queries: Vec<Query>,
    },
    /// Returns only the rows that are present in the input table but not in the specified tables.
    ///
    /// Syntax: `|> EXCEPT DISTINCT (<query>), (<query>), ...`
    ///
    /// See more at <https://cloud.google.com/bigquery/docs/reference/standard-sql/pipe-syntax#except_pipe_operator>
    Except {
        set_quantifier: SetQuantifier,
        queries: Vec<Query>,
    },
    /// Calls a table function or procedure that returns a table.
    ///
    /// Syntax: `|> CALL function_name(args) [AS alias]`
    ///
    /// See more at <https://cloud.google.com/bigquery/docs/reference/standard-sql/pipe-syntax#call_pipe_operator>
    Call {
        function: Function,
        alias: Option<Ident>,
    },
    /// Pivots data from rows to columns.
    ///
    /// Syntax: `|> PIVOT(aggregate_function(column) FOR pivot_column IN (value1, value2, ...)) [AS alias]`
    ///
    /// See more at <https://cloud.google.com/bigquery/docs/reference/standard-sql/pipe-syntax#pivot_pipe_operator>
    Pivot {
        aggregate_functions: Vec<ExprWithAlias>,
        value_column: Vec<Ident>,
        value_source: PivotValueSource,
        alias: Option<Ident>,
    },
    /// The `UNPIVOT` pipe operator transforms columns into rows.
    ///
    /// Syntax:
    /// ```sql
    /// |> UNPIVOT(value_column FOR name_column IN (column1, column2, ...)) [alias]
    /// ```
    ///
    /// See more at <https://cloud.google.com/bigquery/docs/reference/standard-sql/pipe-syntax#unpivot_pipe_operator>
    Unpivot {
        value_column: Ident,
        name_column: Ident,
        unpivot_columns: Vec<Ident>,
        alias: Option<Ident>,
    },
    /// Joins the input table with another table.
    ///
    /// Syntax: `|> [JOIN_TYPE] JOIN <table> [alias] ON <condition>` or `|> [JOIN_TYPE] JOIN <table> [alias] USING (<columns>)`
    ///
    /// See more at <https://cloud.google.com/bigquery/docs/reference/standard-sql/pipe-syntax#join_pipe_operator>
    Join(Join),
}

impl fmt::Display for PipeOperator {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            PipeOperator::Select { exprs } => {
                write!(f, "SELECT {}", display_comma_separated(exprs.as_slice()))
            }
            PipeOperator::Extend { exprs } => {
                write!(f, "EXTEND {}", display_comma_separated(exprs.as_slice()))
            }
            PipeOperator::Set { assignments } => {
                write!(f, "SET {}", display_comma_separated(assignments.as_slice()))
            }
            PipeOperator::Drop { columns } => {
                write!(f, "DROP {}", display_comma_separated(columns.as_slice()))
            }
            PipeOperator::As { alias } => {
                write!(f, "AS {alias}")
            }
            PipeOperator::Limit { expr, offset } => {
                write!(f, "LIMIT {expr}")?;
                if let Some(offset) = offset {
                    write!(f, " OFFSET {offset}")?;
                }
                Ok(())
            }
            PipeOperator::Aggregate {
                full_table_exprs,
                group_by_expr,
            } => {
                write!(f, "AGGREGATE")?;
                if !full_table_exprs.is_empty() {
                    write!(
                        f,
                        " {}",
                        display_comma_separated(full_table_exprs.as_slice())
                    )?;
                }
                if !group_by_expr.is_empty() {
                    write!(f, " GROUP BY {}", display_comma_separated(group_by_expr))?;
                }
                Ok(())
            }

            PipeOperator::Where { expr } => {
                write!(f, "WHERE {expr}")
            }
            PipeOperator::OrderBy { exprs } => {
                write!(f, "ORDER BY {}", display_comma_separated(exprs.as_slice()))
            }

            PipeOperator::TableSample { sample } => {
                write!(f, "{sample}")
            }
            PipeOperator::Rename { mappings } => {
                write!(f, "RENAME {}", display_comma_separated(mappings))
            }
            PipeOperator::Union {
                set_quantifier,
                queries,
            } => Self::fmt_set_operation(f, "UNION", set_quantifier, queries),
            PipeOperator::Intersect {
                set_quantifier,
                queries,
            } => Self::fmt_set_operation(f, "INTERSECT", set_quantifier, queries),
            PipeOperator::Except {
                set_quantifier,
                queries,
            } => Self::fmt_set_operation(f, "EXCEPT", set_quantifier, queries),
            PipeOperator::Call { function, alias } => {
                write!(f, "CALL {function}")?;
                Self::fmt_optional_alias(f, alias)
            }
            PipeOperator::Pivot {
                aggregate_functions,
                value_column,
                value_source,
                alias,
            } => {
                write!(
                    f,
                    "PIVOT({} FOR {} IN ({}))",
                    display_comma_separated(aggregate_functions),
                    Expr::CompoundIdentifier(value_column.to_vec()),
                    value_source
                )?;
                Self::fmt_optional_alias(f, alias)
            }
            PipeOperator::Unpivot {
                value_column,
                name_column,
                unpivot_columns,
                alias,
            } => {
                write!(
                    f,
                    "UNPIVOT({} FOR {} IN ({}))",
                    value_column,
                    name_column,
                    display_comma_separated(unpivot_columns)
                )?;
                Self::fmt_optional_alias(f, alias)
            }
            PipeOperator::Join(join) => write!(f, "{join}"),
        }
    }
}

impl PipeOperator {
    /// Helper function to format optional alias for pipe operators
    fn fmt_optional_alias(f: &mut fmt::Formatter<'_>, alias: &Option<Ident>) -> fmt::Result {
        if let Some(alias) = alias {
            write!(f, " AS {alias}")?;
        }
        Ok(())
    }

    /// Helper function to format set operations (UNION, INTERSECT, EXCEPT) with queries
    fn fmt_set_operation(
        f: &mut fmt::Formatter<'_>,
        operation: &str,
        set_quantifier: &SetQuantifier,
        queries: &[Query],
    ) -> fmt::Result {
        write!(f, "{operation}")?;
        match set_quantifier {
            SetQuantifier::None => {}
            _ => {
                write!(f, " {set_quantifier}")?;
            }
        }
        write!(f, " ")?;
        let parenthesized_queries: Vec<String> =
            queries.iter().map(|query| format!("({query})")).collect();
        write!(f, "{}", display_comma_separated(&parenthesized_queries))
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
    /// MSSQL only.
    pub with_ties: bool,
    /// MSSQL only.
    pub percent: bool,
    pub quantity: Option<TopQuantity>,
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum TopQuantity {
    // A parenthesized expression. MSSQL only.
    Expr(Expr),
    // An unparenthesized integer constant.
    Constant(u64),
}

impl fmt::Display for Top {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let extension = if self.with_ties { " WITH TIES" } else { "" };
        if let Some(ref quantity) = self.quantity {
            let percent = if self.percent { " PERCENT" } else { "" };
            match quantity {
                TopQuantity::Expr(quantity) => write!(f, "TOP ({quantity}){percent}{extension}"),
                TopQuantity::Constant(quantity) => {
                    write!(f, "TOP {quantity}{percent}{extension}")
                }
            }
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
        f.write_str("VALUES")?;
        let prefix = if self.explicit_row { "ROW" } else { "" };
        let mut delim = "";
        for row in &self.rows {
            f.write_str(delim)?;
            delim = ",";
            SpaceOrNewline.fmt(f)?;
            Indent(format_args!("{prefix}({})", display_comma_separated(row))).fmt(f)?;
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

/// ClickHouse supports GROUP BY WITH modifiers(includes ROLLUP|CUBE|TOTALS).
/// e.g. GROUP BY year WITH ROLLUP WITH TOTALS
///
/// [ClickHouse]: <https://clickhouse.com/docs/en/sql-reference/statements/select/group-by#rollup-modifier>
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum GroupByWithModifier {
    Rollup,
    Cube,
    Totals,
    /// Hive supports GROUP BY GROUPING SETS syntax.
    /// e.g. GROUP BY year , month GROUPING SETS((year,month),(year),(month))
    ///
    /// [Hive]: <https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=30151323#EnhancedAggregation,Cube,GroupingandRollup-GROUPINGSETSclause>
    GroupingSets(Expr),
}

impl fmt::Display for GroupByWithModifier {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            GroupByWithModifier::Rollup => write!(f, "WITH ROLLUP"),
            GroupByWithModifier::Cube => write!(f, "WITH CUBE"),
            GroupByWithModifier::Totals => write!(f, "WITH TOTALS"),
            GroupByWithModifier::GroupingSets(expr) => {
                write!(f, "{expr}")
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum GroupByExpr {
    /// ALL syntax of [Snowflake], [DuckDB] and [ClickHouse].
    ///
    /// [Snowflake]: <https://docs.snowflake.com/en/sql-reference/constructs/group-by#label-group-by-all-columns>
    /// [DuckDB]:  <https://duckdb.org/docs/sql/query_syntax/groupby.html>
    /// [ClickHouse]: <https://clickhouse.com/docs/en/sql-reference/statements/select/group-by#group-by-all>
    ///
    /// ClickHouse also supports WITH modifiers after GROUP BY ALL and expressions.
    ///
    /// [ClickHouse]: <https://clickhouse.com/docs/en/sql-reference/statements/select/group-by#rollup-modifier>
    All(Vec<GroupByWithModifier>),

    /// Expressions
    Expressions(Vec<Expr>, Vec<GroupByWithModifier>),
}

impl fmt::Display for GroupByExpr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            GroupByExpr::All(modifiers) => {
                write!(f, "GROUP BY ALL")?;
                if !modifiers.is_empty() {
                    write!(f, " {}", display_separated(modifiers, " "))?;
                }
                Ok(())
            }
            GroupByExpr::Expressions(col_names, modifiers) => {
                f.write_str("GROUP BY")?;
                SpaceOrNewline.fmt(f)?;
                Indent(display_comma_separated(col_names)).fmt(f)?;
                if !modifiers.is_empty() {
                    write!(f, " {}", display_separated(modifiers, " "))?;
                }
                Ok(())
            }
        }
    }
}

/// FORMAT identifier or FORMAT NULL clause, specific to ClickHouse.
///
/// [ClickHouse]: <https://clickhouse.com/docs/en/sql-reference/statements/select/format>
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum FormatClause {
    Identifier(Ident),
    Null,
}

impl fmt::Display for FormatClause {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            FormatClause::Identifier(ident) => write!(f, "FORMAT {ident}"),
            FormatClause::Null => write!(f, "FORMAT NULL"),
        }
    }
}

/// FORMAT identifier in input context, specific to ClickHouse.
///
/// [ClickHouse]: <https://clickhouse.com/docs/en/interfaces/formats>
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct InputFormatClause {
    pub ident: Ident,
    pub values: Vec<Expr>,
}

impl fmt::Display for InputFormatClause {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "FORMAT {}", self.ident)?;

        if !self.values.is_empty() {
            write!(f, " {}", display_comma_separated(self.values.as_slice()))?;
        }

        Ok(())
    }
}

/// FOR XML or FOR JSON clause, specific to MSSQL
/// (formats the output of a query as XML or JSON)
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum ForClause {
    Browse,
    Json {
        for_json: ForJson,
        root: Option<String>,
        include_null_values: bool,
        without_array_wrapper: bool,
    },
    Xml {
        for_xml: ForXml,
        elements: bool,
        binary_base64: bool,
        root: Option<String>,
        r#type: bool,
    },
}

impl fmt::Display for ForClause {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ForClause::Browse => write!(f, "FOR BROWSE"),
            ForClause::Json {
                for_json,
                root,
                include_null_values,
                without_array_wrapper,
            } => {
                write!(f, "FOR JSON ")?;
                write!(f, "{for_json}")?;
                if let Some(root) = root {
                    write!(f, ", ROOT('{root}')")?;
                }
                if *include_null_values {
                    write!(f, ", INCLUDE_NULL_VALUES")?;
                }
                if *without_array_wrapper {
                    write!(f, ", WITHOUT_ARRAY_WRAPPER")?;
                }
                Ok(())
            }
            ForClause::Xml {
                for_xml,
                elements,
                binary_base64,
                root,
                r#type,
            } => {
                write!(f, "FOR XML ")?;
                write!(f, "{for_xml}")?;
                if *binary_base64 {
                    write!(f, ", BINARY BASE64")?;
                }
                if *r#type {
                    write!(f, ", TYPE")?;
                }
                if let Some(root) = root {
                    write!(f, ", ROOT('{root}')")?;
                }
                if *elements {
                    write!(f, ", ELEMENTS")?;
                }
                Ok(())
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum ForXml {
    Raw(Option<String>),
    Auto,
    Explicit,
    Path(Option<String>),
}

impl fmt::Display for ForXml {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ForXml::Raw(root) => {
                write!(f, "RAW")?;
                if let Some(root) = root {
                    write!(f, "('{root}')")?;
                }
                Ok(())
            }
            ForXml::Auto => write!(f, "AUTO"),
            ForXml::Explicit => write!(f, "EXPLICIT"),
            ForXml::Path(root) => {
                write!(f, "PATH")?;
                if let Some(root) = root {
                    write!(f, "('{root}')")?;
                }
                Ok(())
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum ForJson {
    Auto,
    Path,
}

impl fmt::Display for ForJson {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ForJson::Auto => write!(f, "AUTO"),
            ForJson::Path => write!(f, "PATH"),
        }
    }
}

/// A single column definition in MySQL's `JSON_TABLE` table valued function.
///
/// See
/// - [MySQL's JSON_TABLE documentation](https://dev.mysql.com/doc/refman/8.0/en/json-table-functions.html#function_json-table)
/// - [Oracle's JSON_TABLE documentation](https://docs.oracle.com/en/database/oracle/oracle-database/21/sqlrf/JSON_TABLE.html)
/// - [MariaDB's JSON_TABLE documentation](https://mariadb.com/kb/en/json_table/)
///
/// ```sql
/// SELECT *
/// FROM JSON_TABLE(
///     '["a", "b"]',
///     '$[*]' COLUMNS (
///         name FOR ORDINALITY,
///         value VARCHAR(20) PATH '$',
///         NESTED PATH '$[*]' COLUMNS (
///             value VARCHAR(20) PATH '$'
///         )
///     )
/// ) AS jt;
/// ```
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum JsonTableColumn {
    /// A named column with a JSON path
    Named(JsonTableNamedColumn),
    /// The FOR ORDINALITY column, which is a special column that returns the index of the current row in a JSON array.
    ForOrdinality(Ident),
    /// A set of nested columns, which extracts data from a nested JSON array.
    Nested(JsonTableNestedColumn),
}

impl fmt::Display for JsonTableColumn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JsonTableColumn::Named(json_table_named_column) => {
                write!(f, "{json_table_named_column}")
            }
            JsonTableColumn::ForOrdinality(ident) => write!(f, "{ident} FOR ORDINALITY"),
            JsonTableColumn::Nested(json_table_nested_column) => {
                write!(f, "{json_table_nested_column}")
            }
        }
    }
}

/// A nested column in a JSON_TABLE column list
///
/// See <https://mariadb.com/kb/en/json_table/#nested-paths>
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct JsonTableNestedColumn {
    pub path: Value,
    pub columns: Vec<JsonTableColumn>,
}

impl fmt::Display for JsonTableNestedColumn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "NESTED PATH {} COLUMNS ({})",
            self.path,
            display_comma_separated(&self.columns)
        )
    }
}

/// A single column definition in MySQL's `JSON_TABLE` table valued function.
///
/// See <https://mariadb.com/kb/en/json_table/#path-columns>
///
/// ```sql
///         value VARCHAR(20) PATH '$'
/// ```
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct JsonTableNamedColumn {
    /// The name of the column to be extracted.
    pub name: Ident,
    /// The type of the column to be extracted.
    pub r#type: DataType,
    /// The path to the column to be extracted. Must be a literal string.
    pub path: Value,
    /// true if the column is a boolean set to true if the given path exists
    pub exists: bool,
    /// The empty handling clause of the column
    pub on_empty: Option<JsonTableColumnErrorHandling>,
    /// The error handling clause of the column
    pub on_error: Option<JsonTableColumnErrorHandling>,
}

impl fmt::Display for JsonTableNamedColumn {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{} {}{} PATH {}",
            self.name,
            self.r#type,
            if self.exists { " EXISTS" } else { "" },
            self.path
        )?;
        if let Some(on_empty) = &self.on_empty {
            write!(f, " {on_empty} ON EMPTY")?;
        }
        if let Some(on_error) = &self.on_error {
            write!(f, " {on_error} ON ERROR")?;
        }
        Ok(())
    }
}

/// Stores the error handling clause of a `JSON_TABLE` table valued function:
/// {NULL | DEFAULT json_string | ERROR} ON {ERROR | EMPTY }
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum JsonTableColumnErrorHandling {
    Null,
    Default(Value),
    Error,
}

impl fmt::Display for JsonTableColumnErrorHandling {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            JsonTableColumnErrorHandling::Null => write!(f, "NULL"),
            JsonTableColumnErrorHandling::Default(json_string) => {
                write!(f, "DEFAULT {json_string}")
            }
            JsonTableColumnErrorHandling::Error => write!(f, "ERROR"),
        }
    }
}

/// A single column definition in MSSQL's `OPENJSON WITH` clause.
///
/// ```sql
/// colName type [ column_path ] [ AS JSON ]
/// ```
///
/// Reference: <https://learn.microsoft.com/en-us/sql/t-sql/functions/openjson-transact-sql?view=sql-server-ver16#syntax>
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct OpenJsonTableColumn {
    /// The name of the column to be extracted.
    pub name: Ident,
    /// The type of the column to be extracted.
    pub r#type: DataType,
    /// The path to the column to be extracted. Must be a literal string.
    pub path: Option<String>,
    /// The `AS JSON` option.
    pub as_json: bool,
}

impl fmt::Display for OpenJsonTableColumn {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} {}", self.name, self.r#type)?;
        if let Some(path) = &self.path {
            write!(f, " '{}'", value::escape_single_quote_string(path))?;
        }
        if self.as_json {
            write!(f, " AS JSON")?;
        }
        Ok(())
    }
}

/// BigQuery supports ValueTables which have 2 modes:
/// `SELECT [ALL | DISTINCT] AS STRUCT`
/// `SELECT [ALL | DISTINCT] AS VALUE`
///
/// <https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#value_tables>
/// <https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#select_list>
#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum ValueTableMode {
    AsStruct,
    AsValue,
    DistinctAsStruct,
    DistinctAsValue,
}

impl fmt::Display for ValueTableMode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ValueTableMode::AsStruct => write!(f, "AS STRUCT"),
            ValueTableMode::AsValue => write!(f, "AS VALUE"),
            ValueTableMode::DistinctAsStruct => write!(f, "DISTINCT AS STRUCT"),
            ValueTableMode::DistinctAsValue => write!(f, "DISTINCT AS VALUE"),
        }
    }
}

/// The `FROM` clause of an `UPDATE TABLE` statement
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum UpdateTableFromKind {
    /// Update Statement where the 'FROM' clause is before the 'SET' keyword (Supported by Snowflake)
    /// For Example: `UPDATE FROM t1 SET t1.name='aaa'`
    BeforeSet(Vec<TableWithJoins>),
    /// Update Statement where the 'FROM' clause is after the 'SET' keyword (Which is the standard way)
    /// For Example: `UPDATE SET t1.name='aaa' FROM t1`
    AfterSet(Vec<TableWithJoins>),
}

/// Defines the options for an XmlTable column: Named or ForOrdinality
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum XmlTableColumnOption {
    /// A named column with a type, optional path, and default value.
    NamedInfo {
        /// The type of the column to be extracted.
        r#type: DataType,
        /// The path to the column to be extracted. If None, defaults to the column name.
        path: Option<Expr>,
        /// Default value if path does not match
        default: Option<Expr>,
        /// Whether the column is nullable (NULL=true, NOT NULL=false)
        nullable: bool,
    },
    /// The FOR ORDINALITY marker
    ForOrdinality,
}

/// A single column definition in XMLTABLE
///
/// ```sql
/// COLUMNS
///     id int PATH '@id',
///     ordinality FOR ORDINALITY,
///     "COUNTRY_NAME" text,
///     country_id text PATH 'COUNTRY_ID',
///     size_sq_km float PATH 'SIZE[@unit = "sq_km"]',
///     size_other text PATH 'concat(SIZE[@unit!="sq_km"], " ", SIZE[@unit!="sq_km"]/@unit)',
///     premier_name text PATH 'PREMIER_NAME' DEFAULT 'not specified'
/// ```
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct XmlTableColumn {
    /// The name of the column.
    pub name: Ident,
    /// Column options: type/path/default or FOR ORDINALITY
    pub option: XmlTableColumnOption,
}

impl fmt::Display for XmlTableColumn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name)?;
        match &self.option {
            XmlTableColumnOption::NamedInfo {
                r#type,
                path,
                default,
                nullable,
            } => {
                write!(f, " {type}")?;
                if let Some(p) = path {
                    write!(f, " PATH {p}")?;
                }
                if let Some(d) = default {
                    write!(f, " DEFAULT {d}")?;
                }
                if !*nullable {
                    write!(f, " NOT NULL")?;
                }
                Ok(())
            }
            XmlTableColumnOption::ForOrdinality => {
                write!(f, " FOR ORDINALITY")
            }
        }
    }
}

/// Argument passed in the XMLTABLE PASSING clause
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct XmlPassingArgument {
    pub expr: Expr,
    pub alias: Option<Ident>,
    pub by_value: bool, // True if BY VALUE is specified
}

impl fmt::Display for XmlPassingArgument {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.by_value {
            write!(f, "BY VALUE ")?;
        }
        write!(f, "{}", self.expr)?;
        if let Some(alias) = &self.alias {
            write!(f, " AS {alias}")?;
        }
        Ok(())
    }
}

/// The PASSING clause for XMLTABLE
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct XmlPassingClause {
    pub arguments: Vec<XmlPassingArgument>,
}

impl fmt::Display for XmlPassingClause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if !self.arguments.is_empty() {
            write!(f, " PASSING {}", display_comma_separated(&self.arguments))?;
        }
        Ok(())
    }
}

/// Represents a single XML namespace definition in the XMLNAMESPACES clause.
///
/// `namespace_uri AS namespace_name`
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct XmlNamespaceDefinition {
    /// The namespace URI (a text expression).
    pub uri: Expr,
    /// The alias for the namespace (a simple identifier).
    pub name: Ident,
}

impl fmt::Display for XmlNamespaceDefinition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} AS {}", self.uri, self.name)
    }
}

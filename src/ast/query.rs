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

use super::*;

/// The most complete variant of a `SELECT` query expression, optionally
/// including `WITH`, `UNION` / other set operations, and `ORDER BY`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Query {
    /// WITH (common table expressions, or CTEs)
    pub ctes: Vec<Cte>,
    /// SELECT or UNION / EXCEPT / INTECEPT
    pub body: SetExpr,
    /// ORDER BY
    pub order_by: Vec<OrderByExpr>,
    /// `LIMIT { <N> | ALL }`
    pub limit: Option<Expr>,
    /// `OFFSET <N> { ROW | ROWS }`
    pub offset: Option<Expr>,
    /// `FETCH { FIRST | NEXT } <N> [ PERCENT ] { ROW | ROWS } | { ONLY | WITH TIES }`
    pub fetch: Option<Fetch>,
}

impl ToString for Query {
    fn to_string(&self) -> String {
        let mut s = String::new();
        if !self.ctes.is_empty() {
            s += &format!("WITH {} ", comma_separated_string(&self.ctes))
        }
        s += &self.body.to_string();
        if !self.order_by.is_empty() {
            s += &format!(" ORDER BY {}", comma_separated_string(&self.order_by));
        }
        if let Some(ref limit) = self.limit {
            s += &format!(" LIMIT {}", limit.to_string());
        }
        if let Some(ref offset) = self.offset {
            s += &format!(" OFFSET {} ROWS", offset.to_string());
        }
        if let Some(ref fetch) = self.fetch {
            s.push(' ');
            s += &fetch.to_string();
        }
        s
    }
}

/// A node in a tree, representing a "query body" expression, roughly:
/// `SELECT ... [ {UNION|EXCEPT|INTERSECT} SELECT ...]`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SetExpr {
    /// Restricted SELECT .. FROM .. HAVING (no ORDER BY or set operations)
    Select(Box<Select>),
    /// Parenthesized SELECT subquery, which may include more set operations
    /// in its body and an optional ORDER BY / LIMIT.
    Query(Box<Query>),
    /// UNION/EXCEPT/INTERSECT of two queries
    SetOperation {
        op: SetOperator,
        all: bool,
        left: Box<SetExpr>,
        right: Box<SetExpr>,
    },
    Values(Values),
    // TODO: ANSI SQL supports `TABLE` here.
}

impl ToString for SetExpr {
    fn to_string(&self) -> String {
        match self {
            SetExpr::Select(s) => s.to_string(),
            SetExpr::Query(q) => format!("({})", q.to_string()),
            SetExpr::Values(v) => v.to_string(),
            SetExpr::SetOperation {
                left,
                right,
                op,
                all,
            } => {
                let all_str = if *all { " ALL" } else { "" };
                format!(
                    "{} {}{} {}",
                    left.to_string(),
                    op.to_string(),
                    all_str,
                    right.to_string()
                )
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SetOperator {
    Union,
    Except,
    Intersect,
}

impl ToString for SetOperator {
    fn to_string(&self) -> String {
        match self {
            SetOperator::Union => "UNION".to_string(),
            SetOperator::Except => "EXCEPT".to_string(),
            SetOperator::Intersect => "INTERSECT".to_string(),
        }
    }
}

/// A restricted variant of `SELECT` (without CTEs/`ORDER BY`), which may
/// appear either as the only body item of an `SQLQuery`, or as an operand
/// to a set operation like `UNION`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Select {
    pub distinct: bool,
    /// projection expressions
    pub projection: Vec<SelectItem>,
    /// FROM
    pub from: Vec<TableWithJoins>,
    /// WHERE
    pub selection: Option<Expr>,
    /// GROUP BY
    pub group_by: Vec<Expr>,
    /// HAVING
    pub having: Option<Expr>,
}

impl ToString for Select {
    fn to_string(&self) -> String {
        let mut s = format!(
            "SELECT{} {}",
            if self.distinct { " DISTINCT" } else { "" },
            comma_separated_string(&self.projection)
        );
        if !self.from.is_empty() {
            s += &format!(" FROM {}", comma_separated_string(&self.from));
        }
        if let Some(ref selection) = self.selection {
            s += &format!(" WHERE {}", selection.to_string());
        }
        if !self.group_by.is_empty() {
            s += &format!(" GROUP BY {}", comma_separated_string(&self.group_by));
        }
        if let Some(ref having) = self.having {
            s += &format!(" HAVING {}", having.to_string());
        }
        s
    }
}

/// A single CTE (used after `WITH`): `alias [(col1, col2, ...)] AS ( query )`
/// The names in the column list before `AS`, when specified, replace the names
/// of the columns returned by the query. The parser does not validate that the
/// number of columns in the query matches the number of columns in the query.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Cte {
    pub alias: TableAlias,
    pub query: Query,
}

impl ToString for Cte {
    fn to_string(&self) -> String {
        format!("{} AS ({})", self.alias.to_string(), self.query.to_string())
    }
}

/// One item of the comma-separated list following `SELECT`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SelectItem {
    /// Any expression, not followed by `[ AS ] alias`
    UnnamedExpr(Expr),
    /// An expression, followed by `[ AS ] alias`
    ExprWithAlias { expr: Expr, alias: Ident },
    /// `alias.*` or even `schema.table.*`
    QualifiedWildcard(ObjectName),
    /// An unqualified `*`
    Wildcard,
}

impl ToString for SelectItem {
    fn to_string(&self) -> String {
        match &self {
            SelectItem::UnnamedExpr(expr) => expr.to_string(),
            SelectItem::ExprWithAlias { expr, alias } => {
                format!("{} AS {}", expr.to_string(), alias)
            }
            SelectItem::QualifiedWildcard(prefix) => format!("{}.*", prefix.to_string()),
            SelectItem::Wildcard => "*".to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableWithJoins {
    pub relation: TableFactor,
    pub joins: Vec<Join>,
}

impl ToString for TableWithJoins {
    fn to_string(&self) -> String {
        let mut s = self.relation.to_string();
        for join in &self.joins {
            s += &join.to_string();
        }
        s
    }
}

/// A table name or a parenthesized subquery with an optional alias
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TableFactor {
    Table {
        name: ObjectName,
        alias: Option<TableAlias>,
        /// Arguments of a table-valued function, as supported by Postgres
        /// and MSSQL. Note that deprecated MSSQL `FROM foo (NOLOCK)` syntax
        /// will also be parsed as `args`.
        args: Vec<Expr>,
        /// MSSQL-specific `WITH (...)` hints such as NOLOCK.
        with_hints: Vec<Expr>,
    },
    Derived {
        lateral: bool,
        subquery: Box<Query>,
        alias: Option<TableAlias>,
    },
    /// Represents a parenthesized join expression, such as
    /// `(foo <JOIN> bar [ <JOIN> baz ... ])`.
    /// The inner `TableWithJoins` can have no joins only if its
    /// `relation` is itself a `TableFactor::NestedJoin`.
    NestedJoin(Box<TableWithJoins>),
}

impl ToString for TableFactor {
    fn to_string(&self) -> String {
        match self {
            TableFactor::Table {
                name,
                alias,
                args,
                with_hints,
            } => {
                let mut s = name.to_string();
                if !args.is_empty() {
                    s += &format!("({})", comma_separated_string(args))
                };
                if let Some(alias) = alias {
                    s += &format!(" AS {}", alias.to_string());
                }
                if !with_hints.is_empty() {
                    s += &format!(" WITH ({})", comma_separated_string(with_hints));
                }
                s
            }
            TableFactor::Derived {
                lateral,
                subquery,
                alias,
            } => {
                let mut s = String::new();
                if *lateral {
                    s += "LATERAL ";
                }
                s += &format!("({})", subquery.to_string());
                if let Some(alias) = alias {
                    s += &format!(" AS {}", alias.to_string());
                }
                s
            }
            TableFactor::NestedJoin(table_reference) => {
                format!("({})", table_reference.to_string())
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableAlias {
    pub name: Ident,
    pub columns: Vec<Ident>,
}

impl ToString for TableAlias {
    fn to_string(&self) -> String {
        let mut s = self.name.clone();
        if !self.columns.is_empty() {
            s += &format!(" ({})", comma_separated_string(&self.columns));
        }
        s
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Join {
    pub relation: TableFactor,
    pub join_operator: JoinOperator,
}

impl ToString for Join {
    fn to_string(&self) -> String {
        fn prefix(constraint: &JoinConstraint) -> String {
            match constraint {
                JoinConstraint::Natural => "NATURAL ".to_string(),
                _ => "".to_string(),
            }
        }
        fn suffix(constraint: &JoinConstraint) -> String {
            match constraint {
                JoinConstraint::On(expr) => format!(" ON {}", expr.to_string()),
                JoinConstraint::Using(attrs) => format!(" USING({})", attrs.join(", ")),
                _ => "".to_string(),
            }
        }
        match &self.join_operator {
            JoinOperator::Inner(constraint) => format!(
                " {}JOIN {}{}",
                prefix(constraint),
                self.relation.to_string(),
                suffix(constraint)
            ),
            JoinOperator::LeftOuter(constraint) => format!(
                " {}LEFT JOIN {}{}",
                prefix(constraint),
                self.relation.to_string(),
                suffix(constraint)
            ),
            JoinOperator::RightOuter(constraint) => format!(
                " {}RIGHT JOIN {}{}",
                prefix(constraint),
                self.relation.to_string(),
                suffix(constraint)
            ),
            JoinOperator::FullOuter(constraint) => format!(
                " {}FULL JOIN {}{}",
                prefix(constraint),
                self.relation.to_string(),
                suffix(constraint)
            ),
            JoinOperator::CrossJoin => format!(" CROSS JOIN {}", self.relation.to_string()),
            JoinOperator::CrossApply => format!(" CROSS APPLY {}", self.relation.to_string()),
            JoinOperator::OuterApply => format!(" OUTER APPLY {}", self.relation.to_string()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum JoinOperator {
    Inner(JoinConstraint),
    LeftOuter(JoinConstraint),
    RightOuter(JoinConstraint),
    FullOuter(JoinConstraint),
    CrossJoin,
    /// CROSS APPLY (non-standard)
    CrossApply,
    /// OUTER APPLY (non-standard)
    OuterApply,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum JoinConstraint {
    On(Expr),
    Using(Vec<Ident>),
    Natural,
}

/// SQL ORDER BY expression
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct OrderByExpr {
    pub expr: Expr,
    pub asc: Option<bool>,
}

impl ToString for OrderByExpr {
    fn to_string(&self) -> String {
        match self.asc {
            Some(true) => format!("{} ASC", self.expr.to_string()),
            Some(false) => format!("{} DESC", self.expr.to_string()),
            None => self.expr.to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Fetch {
    pub with_ties: bool,
    pub percent: bool,
    pub quantity: Option<Expr>,
}

impl ToString for Fetch {
    fn to_string(&self) -> String {
        let extension = if self.with_ties { "WITH TIES" } else { "ONLY" };
        if let Some(ref quantity) = self.quantity {
            let percent = if self.percent { " PERCENT" } else { "" };
            format!(
                "FETCH FIRST {}{} ROWS {}",
                quantity.to_string(),
                percent,
                extension
            )
        } else {
            format!("FETCH FIRST ROWS {}", extension)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Values(pub Vec<Vec<Expr>>);

impl ToString for Values {
    fn to_string(&self) -> String {
        let rows = self
            .0
            .iter()
            .map(|row| format!("({})", comma_separated_string(row)));
        format!("VALUES {}", comma_separated_string(rows))
    }
}

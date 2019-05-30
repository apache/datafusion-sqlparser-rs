use super::*;

/// The most complete variant of a `SELECT` query expression, optionally
/// including `WITH`, `UNION` / other set operations, and `ORDER BY`.
#[derive(Debug, Clone, PartialEq, Hash)]
pub struct SQLQuery {
    /// WITH (common table expressions, or CTEs)
    pub ctes: Vec<Cte>,
    /// SELECT or UNION / EXCEPT / INTECEPT
    pub body: SQLSetExpr,
    /// ORDER BY
    pub order_by: Vec<SQLOrderByExpr>,
    /// LIMIT { <N> | ALL }
    pub limit: Option<ASTNode>,
    /// OFFSET <N> { ROW | ROWS }
    pub offset: Option<ASTNode>,
    /// FETCH { FIRST | NEXT } <N> [ PERCENT ] { ROW | ROWS } | { ONLY | WITH TIES }
    pub fetch: Option<Fetch>,
}

impl ToString for SQLQuery {
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
#[derive(Debug, Clone, PartialEq, Hash)]
pub enum SQLSetExpr {
    /// Restricted SELECT .. FROM .. HAVING (no ORDER BY or set operations)
    Select(Box<SQLSelect>),
    /// Parenthesized SELECT subquery, which may include more set operations
    /// in its body and an optional ORDER BY / LIMIT.
    Query(Box<SQLQuery>),
    /// UNION/EXCEPT/INTERSECT of two queries
    SetOperation {
        op: SQLSetOperator,
        all: bool,
        left: Box<SQLSetExpr>,
        right: Box<SQLSetExpr>,
    },
    Values(SQLValues),
    // TODO: ANSI SQL supports `TABLE` here.
}

impl ToString for SQLSetExpr {
    fn to_string(&self) -> String {
        match self {
            SQLSetExpr::Select(s) => s.to_string(),
            SQLSetExpr::Query(q) => format!("({})", q.to_string()),
            SQLSetExpr::Values(v) => v.to_string(),
            SQLSetExpr::SetOperation {
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

#[derive(Debug, Clone, PartialEq, Hash)]
pub enum SQLSetOperator {
    Union,
    Except,
    Intersect,
}

impl ToString for SQLSetOperator {
    fn to_string(&self) -> String {
        match self {
            SQLSetOperator::Union => "UNION".to_string(),
            SQLSetOperator::Except => "EXCEPT".to_string(),
            SQLSetOperator::Intersect => "INTERSECT".to_string(),
        }
    }
}

/// A restricted variant of `SELECT` (without CTEs/`ORDER BY`), which may
/// appear either as the only body item of an `SQLQuery`, or as an operand
/// to a set operation like `UNION`.
#[derive(Debug, Clone, PartialEq, Hash)]
pub struct SQLSelect {
    pub distinct: bool,
    /// projection expressions
    pub projection: Vec<SQLSelectItem>,
    /// FROM
    pub relation: Option<TableFactor>,
    /// JOIN
    pub joins: Vec<Join>,
    /// WHERE
    pub selection: Option<ASTNode>,
    /// GROUP BY
    pub group_by: Vec<ASTNode>,
    /// HAVING
    pub having: Option<ASTNode>,
}

impl ToString for SQLSelect {
    fn to_string(&self) -> String {
        let mut s = format!(
            "SELECT{} {}",
            if self.distinct { " DISTINCT" } else { "" },
            comma_separated_string(&self.projection)
        );
        if let Some(ref relation) = self.relation {
            s += &format!(" FROM {}", relation.to_string());
        }
        for join in &self.joins {
            s += &join.to_string();
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
#[derive(Debug, Clone, PartialEq, Hash)]
pub struct Cte {
    pub alias: SQLIdent,
    pub query: SQLQuery,
    pub renamed_columns: Vec<SQLIdent>,
}

impl ToString for Cte {
    fn to_string(&self) -> String {
        let mut s = self.alias.clone();
        if !self.renamed_columns.is_empty() {
            s += &format!(" ({})", comma_separated_string(&self.renamed_columns));
        }
        s + &format!(" AS ({})", self.query.to_string())
    }
}

/// One item of the comma-separated list following `SELECT`
#[derive(Debug, Clone, PartialEq, Hash)]
pub enum SQLSelectItem {
    /// Any expression, not followed by `[ AS ] alias`
    UnnamedExpression(ASTNode),
    /// An expression, followed by `[ AS ] alias`
    ExpressionWithAlias { expr: ASTNode, alias: SQLIdent },
    /// `alias.*` or even `schema.table.*`
    QualifiedWildcard(SQLObjectName),
    /// An unqualified `*`
    Wildcard,
}

impl ToString for SQLSelectItem {
    fn to_string(&self) -> String {
        match &self {
            SQLSelectItem::UnnamedExpression(expr) => expr.to_string(),
            SQLSelectItem::ExpressionWithAlias { expr, alias } => {
                format!("{} AS {}", expr.to_string(), alias)
            }
            SQLSelectItem::QualifiedWildcard(prefix) => format!("{}.*", prefix.to_string()),
            SQLSelectItem::Wildcard => "*".to_string(),
        }
    }
}

/// A table name or a parenthesized subquery with an optional alias
#[derive(Debug, Clone, PartialEq, Hash)]
pub enum TableFactor {
    Table {
        name: SQLObjectName,
        alias: Option<TableAlias>,
        /// Arguments of a table-valued function, as supported by Postgres
        /// and MSSQL. Note that deprecated MSSQL `FROM foo (NOLOCK)` syntax
        /// will also be parsed as `args`.
        args: Vec<ASTNode>,
        /// MSSQL-specific `WITH (...)` hints such as NOLOCK.
        with_hints: Vec<ASTNode>,
    },
    Derived {
        lateral: bool,
        subquery: Box<SQLQuery>,
        alias: Option<TableAlias>,
    },
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
        }
    }
}

#[derive(Debug, Clone, PartialEq, Hash)]
pub struct TableAlias {
    pub name: SQLIdent,
    pub columns: Vec<SQLIdent>,
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

#[derive(Debug, Clone, PartialEq, Hash)]
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
            JoinOperator::Cross => format!(" CROSS JOIN {}", self.relation.to_string()),
            JoinOperator::Implicit => format!(", {}", self.relation.to_string()),
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
        }
    }
}

#[derive(Debug, Clone, PartialEq, Hash)]
pub enum JoinOperator {
    Inner(JoinConstraint),
    LeftOuter(JoinConstraint),
    RightOuter(JoinConstraint),
    FullOuter(JoinConstraint),
    Implicit,
    Cross,
}

#[derive(Debug, Clone, PartialEq, Hash)]
pub enum JoinConstraint {
    On(ASTNode),
    Using(Vec<SQLIdent>),
    Natural,
}

/// SQL ORDER BY expression
#[derive(Debug, Clone, PartialEq, Hash)]
pub struct SQLOrderByExpr {
    pub expr: ASTNode,
    pub asc: Option<bool>,
}

impl ToString for SQLOrderByExpr {
    fn to_string(&self) -> String {
        match self.asc {
            Some(true) => format!("{} ASC", self.expr.to_string()),
            Some(false) => format!("{} DESC", self.expr.to_string()),
            None => self.expr.to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Hash)]
pub struct Fetch {
    pub with_ties: bool,
    pub percent: bool,
    pub quantity: Option<ASTNode>,
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

#[derive(Debug, Clone, PartialEq, Hash)]
pub struct SQLValues(pub Vec<Vec<ASTNode>>);

impl ToString for SQLValues {
    fn to_string(&self) -> String {
        let rows = self
            .0
            .iter()
            .map(|row| format!("({})", comma_separated_string(row)));
        format!("VALUES {}", comma_separated_string(rows))
    }
}

use super::*;

#[derive(Debug, Clone, PartialEq)]
pub struct SQLSelect {
    /// projection expressions
    pub projection: Vec<ASTNode>,
    /// FROM
    pub relation: Option<TableFactor>,
    // JOIN
    pub joins: Vec<Join>,
    /// WHERE
    pub selection: Option<Box<ASTNode>>,
    /// ORDER BY
    pub order_by: Option<Vec<SQLOrderByExpr>>,
    /// GROUP BY
    pub group_by: Option<Vec<ASTNode>>,
    /// HAVING
    pub having: Option<Box<ASTNode>>,
    /// LIMIT
    pub limit: Option<Box<ASTNode>>,
}

impl ToString for SQLSelect {
    fn to_string(&self) -> String {
        let mut s = format!(
            "SELECT {}",
            self.projection
                .iter()
                .map(|p| p.to_string())
                .collect::<Vec<String>>()
                .join(", ")
        );
        if let Some(ref relation) = self.relation {
            s += &format!(" FROM {}", relation.to_string());
        }
        for join in &self.joins {
            s += &join.to_string();
        }
        if let Some(ref selection) = self.selection {
            s += &format!(" WHERE {}", selection.as_ref().to_string());
        }
        if let Some(ref group_by) = self.group_by {
            s += &format!(
                " GROUP BY {}",
                group_by
                    .iter()
                    .map(|g| g.to_string())
                    .collect::<Vec<String>>()
                    .join(", ")
            );
        }
        if let Some(ref having) = self.having {
            s += &format!(" HAVING {}", having.as_ref().to_string());
        }
        if let Some(ref order_by) = self.order_by {
            s += &format!(
                " ORDER BY {}",
                order_by
                    .iter()
                    .map(|o| o.to_string())
                    .collect::<Vec<String>>()
                    .join(", ")
            );
        }
        if let Some(ref limit) = self.limit {
            s += &format!(" LIMIT {}", limit.as_ref().to_string());
        }
        s
    }
}

/// A table name or a parenthesized subquery with an optional alias
#[derive(Debug, Clone, PartialEq)]
pub enum TableFactor {
    Table {
        name: SQLObjectName,
        alias: Option<SQLIdent>,
    },
    Derived {
        subquery: Box<SQLSelect>,
        alias: Option<SQLIdent>,
    },
}

impl ToString for TableFactor {
    fn to_string(&self) -> String {
        let (base, alias) = match self {
            TableFactor::Table { name, alias } => (name.to_string(), alias),
            TableFactor::Derived { subquery, alias } => {
                (format!("({})", subquery.to_string()), alias)
            }
        };
        if let Some(alias) = alias {
            format!("{} AS {}", base, alias)
        } else {
            base
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
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
                JoinConstraint::On(expr) => format!("ON {}", expr.to_string()),
                JoinConstraint::Using(attrs) => format!("USING({})", attrs.join(", ")),
                _ => "".to_string(),
            }
        }
        match &self.join_operator {
            JoinOperator::Inner(constraint) => format!(
                " {}JOIN {} {}",
                prefix(constraint),
                self.relation.to_string(),
                suffix(constraint)
            ),
            JoinOperator::Cross => format!(" CROSS JOIN {}", self.relation.to_string()),
            JoinOperator::Implicit => format!(", {}", self.relation.to_string()),
            JoinOperator::LeftOuter(constraint) => format!(
                " {}LEFT JOIN {} {}",
                prefix(constraint),
                self.relation.to_string(),
                suffix(constraint)
            ),
            JoinOperator::RightOuter(constraint) => format!(
                " {}RIGHT JOIN {} {}",
                prefix(constraint),
                self.relation.to_string(),
                suffix(constraint)
            ),
            JoinOperator::FullOuter(constraint) => format!(
                " {}FULL JOIN {} {}",
                prefix(constraint),
                self.relation.to_string(),
                suffix(constraint)
            ),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum JoinOperator {
    Inner(JoinConstraint),
    LeftOuter(JoinConstraint),
    RightOuter(JoinConstraint),
    FullOuter(JoinConstraint),
    Implicit,
    Cross,
}

#[derive(Debug, Clone, PartialEq)]
pub enum JoinConstraint {
    On(ASTNode),
    Using(Vec<SQLIdent>),
    Natural,
}

/// SQL ORDER BY expression
#[derive(Debug, Clone, PartialEq)]
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

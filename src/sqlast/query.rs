use super::*;

#[derive(Debug, Clone, PartialEq)]
pub struct SQLSelect {
    /// projection expressions
    pub projection: Vec<ASTNode>,
    /// FROM
    pub relation: Option<Box<ASTNode>>, // TableFactor
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
            s += &format!(" FROM {}", relation.as_ref().to_string());
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

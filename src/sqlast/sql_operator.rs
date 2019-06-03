/// SQL Operator
#[derive(Debug, Clone, PartialEq, Hash)]
pub enum SQLOperator {
    Plus,
    Minus,
    Multiply,
    Divide,
    Modulus,
    Gt,
    Lt,
    GtEq,
    LtEq,
    Eq,
    NotEq,
    And,
    Or,
    Not,
    Like,
    NotLike,
}

impl ToString for SQLOperator {
    fn to_string(&self) -> String {
        match self {
            SQLOperator::Plus => "+".to_string(),
            SQLOperator::Minus => "-".to_string(),
            SQLOperator::Multiply => "*".to_string(),
            SQLOperator::Divide => "/".to_string(),
            SQLOperator::Modulus => "%".to_string(),
            SQLOperator::Gt => ">".to_string(),
            SQLOperator::Lt => "<".to_string(),
            SQLOperator::GtEq => ">=".to_string(),
            SQLOperator::LtEq => "<=".to_string(),
            SQLOperator::Eq => "=".to_string(),
            SQLOperator::NotEq => "<>".to_string(),
            SQLOperator::And => "AND".to_string(),
            SQLOperator::Or => "OR".to_string(),
            SQLOperator::Not => "NOT".to_string(),
            SQLOperator::Like => "LIKE".to_string(),
            SQLOperator::NotLike => "NOT LIKE".to_string(),
        }
    }
}

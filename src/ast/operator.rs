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

/// Unary operators
#[derive(Debug, Clone, PartialEq, Hash)]
pub enum SQLUnaryOperator {
    Plus,
    Minus,
    Not,
}

impl ToString for SQLUnaryOperator {
    fn to_string(&self) -> String {
        match self {
            SQLUnaryOperator::Plus => "+".to_string(),
            SQLUnaryOperator::Minus => "-".to_string(),
            SQLUnaryOperator::Not => "NOT".to_string(),
        }
    }
}

/// Binary operators
#[derive(Debug, Clone, PartialEq, Hash)]
pub enum SQLBinaryOperator {
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
    Like,
    NotLike,
}

impl ToString for SQLBinaryOperator {
    fn to_string(&self) -> String {
        match self {
            SQLBinaryOperator::Plus => "+".to_string(),
            SQLBinaryOperator::Minus => "-".to_string(),
            SQLBinaryOperator::Multiply => "*".to_string(),
            SQLBinaryOperator::Divide => "/".to_string(),
            SQLBinaryOperator::Modulus => "%".to_string(),
            SQLBinaryOperator::Gt => ">".to_string(),
            SQLBinaryOperator::Lt => "<".to_string(),
            SQLBinaryOperator::GtEq => ">=".to_string(),
            SQLBinaryOperator::LtEq => "<=".to_string(),
            SQLBinaryOperator::Eq => "=".to_string(),
            SQLBinaryOperator::NotEq => "<>".to_string(),
            SQLBinaryOperator::And => "AND".to_string(),
            SQLBinaryOperator::Or => "OR".to_string(),
            SQLBinaryOperator::Like => "LIKE".to_string(),
            SQLBinaryOperator::NotLike => "NOT LIKE".to_string(),
        }
    }
}

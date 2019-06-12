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
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum UnaryOperator {
    Plus,
    Minus,
    Not,
}

impl ToString for UnaryOperator {
    fn to_string(&self) -> String {
        match self {
            UnaryOperator::Plus => "+".to_string(),
            UnaryOperator::Minus => "-".to_string(),
            UnaryOperator::Not => "NOT".to_string(),
        }
    }
}

/// Binary operators
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum BinaryOperator {
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

impl ToString for BinaryOperator {
    fn to_string(&self) -> String {
        match self {
            BinaryOperator::Plus => "+".to_string(),
            BinaryOperator::Minus => "-".to_string(),
            BinaryOperator::Multiply => "*".to_string(),
            BinaryOperator::Divide => "/".to_string(),
            BinaryOperator::Modulus => "%".to_string(),
            BinaryOperator::Gt => ">".to_string(),
            BinaryOperator::Lt => "<".to_string(),
            BinaryOperator::GtEq => ">=".to_string(),
            BinaryOperator::LtEq => "<=".to_string(),
            BinaryOperator::Eq => "=".to_string(),
            BinaryOperator::NotEq => "<>".to_string(),
            BinaryOperator::And => "AND".to_string(),
            BinaryOperator::Or => "OR".to_string(),
            BinaryOperator::Like => "LIKE".to_string(),
            BinaryOperator::NotLike => "NOT LIKE".to_string(),
        }
    }
}

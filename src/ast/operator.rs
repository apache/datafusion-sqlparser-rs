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

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use std::fmt;

/// Unary operators
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum UnaryOperator {
    Plus,
    Minus,
    Not,
    PGBitwiseNot,
    PGSqrt,
    PGCbrt,
    PGFactorial,
    PGInfixFactorial,
    PGAbs,
}

impl fmt::Display for UnaryOperator {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(match self {
            UnaryOperator::Plus => "+",
            UnaryOperator::Minus => "-",
            UnaryOperator::Not => "NOT",
            UnaryOperator::PGBitwiseNot => "~",
            UnaryOperator::PGSqrt => "|/",
            UnaryOperator::PGCbrt => "||/",
            UnaryOperator::PGFactorial => "!",
            UnaryOperator::PGInfixFactorial => "!!",
            UnaryOperator::PGAbs => "@",
        })
    }
}

/// Binary operators
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum BinaryOperator {
    Plus,
    Minus,
    Multiply,
    Divide,
    Modulus,
    StringConcat,
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
    BitwiseOr,
    BitwiseAnd,
    BitwiseXor,
    PGBitwiseXor,
    PGBitwiseShiftLeft,
    PGBitwiseShiftRight,
}

impl fmt::Display for BinaryOperator {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(match self {
            BinaryOperator::Plus => "+",
            BinaryOperator::Minus => "-",
            BinaryOperator::Multiply => "*",
            BinaryOperator::Divide => "/",
            BinaryOperator::Modulus => "%",
            BinaryOperator::StringConcat => "||",
            BinaryOperator::Gt => ">",
            BinaryOperator::Lt => "<",
            BinaryOperator::GtEq => ">=",
            BinaryOperator::LtEq => "<=",
            BinaryOperator::Eq => "=",
            BinaryOperator::NotEq => "<>",
            BinaryOperator::And => "AND",
            BinaryOperator::Or => "OR",
            BinaryOperator::Like => "LIKE",
            BinaryOperator::NotLike => "NOT LIKE",
            BinaryOperator::BitwiseOr => "|",
            BinaryOperator::BitwiseAnd => "&",
            BinaryOperator::BitwiseXor => "^",
            BinaryOperator::PGBitwiseXor => "#",
            BinaryOperator::PGBitwiseShiftLeft => "<<",
            BinaryOperator::PGBitwiseShiftRight => ">>",
        })
    }
}

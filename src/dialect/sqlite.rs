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
use alloc::{boxed::Box, string::String};

use crate::ast::{BinaryOperator, DataType, Expr, ObjectName, Statement};
use crate::dialect::{BindPlaceholderStyle, Dialect, Precedence};
use crate::keywords::{self, Keyword};
use crate::parser::{Parser, ParserError};
use crate::tokenizer::Token;

/// Precedence of `<`, `<=`, `>` and `>=`, between `=` and the bitwise operators, see
/// <https://www.sqlite.org/lang_expr.html#operators_and_parse_affecting_attributes>
const COMPARISON_PREC: u8 = 21;

/// A [`Dialect`] for [SQLite](https://www.sqlite.org)
///
/// This dialect allows columns in a
/// [`CREATE TABLE`](https://sqlite.org/lang_createtable.html) statement with no
/// type specified, as in `CREATE TABLE t1 (a)`. In the AST, these columns will
/// have the data type [`Unspecified`](crate::ast::DataType::Unspecified).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SQLiteDialect {}

impl Dialect for SQLiteDialect {
    // see https://www.sqlite.org/lang_keywords.html
    // parse `...`, [...] and "..." as identifier
    // TODO: support depending on the context tread '...' as identifier too.
    fn is_delimited_identifier_start(&self, ch: char) -> bool {
        ch == '`' || ch == '"' || ch == '['
    }

    fn identifier_quote_style(&self, _identifier: &str) -> Option<char> {
        Some('`')
    }

    fn ordered_bind_placeholder_style(&self) -> Option<BindPlaceholderStyle> {
        Some(BindPlaceholderStyle::QuestionMark)
    }

    fn is_identifier_start(&self, ch: char) -> bool {
        // See https://www.sqlite.org/draft/tokenreq.html
        ch.is_ascii_lowercase() || ch.is_ascii_uppercase() || ch == '_' || ch >= '\u{0080}'
    }

    fn supports_filter_during_aggregation(&self) -> bool {
        true
    }

    fn supports_start_transaction_modifier(&self) -> bool {
        true
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        self.is_identifier_start(ch) || ch.is_ascii_digit() || ch == '$'
    }

    fn parse_statement(&self, parser: &mut Parser) -> Option<Result<Statement, ParserError>> {
        if parser.parse_keyword(Keyword::REPLACE) {
            parser.prev_token();
            Some(parser.parse_insert(parser.get_current_token().clone()))
        } else {
            None
        }
    }

    fn parse_data_type(&self, parser: &mut Parser) -> Option<Result<DataType, ParserError>> {
        parse_type_with_numeric_modifiers(parser).map(Ok)
    }

    fn parse_infix(
        &self,
        parser: &mut crate::parser::Parser,
        expr: &crate::ast::Expr,
        precedence: u8,
    ) -> Option<Result<crate::ast::Expr, ParserError>> {
        // Parse MATCH, REGEXP and GLOB as operators
        // See <https://www.sqlite.org/lang_expr.html#the_like_glob_regexp_match_and_extract_operators>
        for (keyword, op) in [
            (Keyword::REGEXP, BinaryOperator::Regexp),
            (Keyword::MATCH, BinaryOperator::Match),
            (Keyword::GLOB, BinaryOperator::Glob),
        ] {
            if parser.parse_keyword(keyword) {
                let left = Box::new(expr.clone());
                let right = Box::new(match parser.parse_subexpr(precedence) {
                    Ok(expr) => expr,
                    Err(e) => return Some(Err(e)),
                });
                return Some(Ok(Expr::BinaryOp { left, op, right }));
            }
        }
        None
    }

    /// `&`, `|`, `<<`, and `>>` share one left-associative precedence level.
    /// See <https://www.sqlite.org/lang_expr.html#operators_and_parse_affecting_attributes>
    fn get_next_precedence(&self, parser: &Parser) -> Option<Result<u8, ParserError>> {
        match parser.peek_token_ref().token {
            Token::Ampersand | Token::Pipe | Token::ShiftLeft | Token::ShiftRight => {
                Some(Ok(self.prec_value(Precedence::Ampersand)))
            }
            Token::Lt | Token::LtEq | Token::Gt | Token::GtEq => Some(Ok(COMPARISON_PREC)),
            _ => None,
        }
    }

    fn prec_value(&self, prec: Precedence) -> u8 {
        match prec {
            Precedence::Period => 100,
            Precedence::DoubleColon => 50,
            Precedence::AtTz => 41,
            Precedence::MulDivModOp => 40,
            Precedence::PlusMinus => 30,
            Precedence::Xor => 25,
            Precedence::Ampersand => 24,
            Precedence::Caret => 23,
            Precedence::Pipe | Precedence::Colon | Precedence::PgOther => 22,
            Precedence::Between | Precedence::Eq => 20,
            Precedence::Like => 19,
            Precedence::Is => 17,
            Precedence::UnaryNot => 15,
            Precedence::And => 10,
            Precedence::Or => 5,
        }
    }

    fn supports_in_empty_list(&self) -> bool {
        true
    }

    fn supports_limit_comma(&self) -> bool {
        true
    }

    fn supports_asc_desc_in_column_definition(&self) -> bool {
        true
    }

    fn supports_dollar_placeholder(&self) -> bool {
        true
    }

    /// SQLite supports `NOTNULL` as aliases for `IS NOT NULL`
    /// See: <https://sqlite.org/syntax/expr.html>
    fn supports_notnull_operator(&self) -> bool {
        true
    }

    fn supports_comma_separated_trim(&self) -> bool {
        true
    }

    fn supports_numeric_literal_underscores(&self) -> bool {
        true
    }

    fn supports_double_eq_assignment(&self) -> bool {
        true
    }

    fn supports_string_literal_column_names(&self) -> bool {
        true
    }

    fn supports_cast_empty_data_type_to_unspecified(&self) -> bool {
        true
    }

    fn supports_national_string_literal(&self) -> bool {
        false
    }

    fn supports_parens_around_table_factor(&self) -> bool {
        true
    }

    fn supports_unterminated_block_comments(&self) -> bool {
        true
    }

    fn is_column_alias(&self, kw: &Keyword, _parser: &mut Parser) -> bool {
        // https://www.sqlite.org/lang_keywords.html
        !matches!(
            kw,
            Keyword::SELECT
                | Keyword::WHERE
                | Keyword::GROUP
                | Keyword::HAVING
                | Keyword::ORDER
                | Keyword::LIMIT
                | Keyword::UNION
                | Keyword::EXCEPT
                | Keyword::INTERSECT
                | Keyword::RETURNING
                | Keyword::VALUES
                | Keyword::FROM
                | Keyword::INTO
        )
    }

    fn supports_over_keyword_as_column_alias(&self) -> bool {
        true
    }

    fn supports_indexed_by(&self) -> bool {
        true
    }

    /// `INDEXED BY` and `NOT INDEXED` follow the table name or alias.
    fn is_table_alias(&self, kw: &Keyword, parser: &mut Parser) -> bool {
        match kw {
            Keyword::INDEXED => false,
            Keyword::NOT => !parser.peek_keyword(Keyword::INDEXED),
            _ => !keywords::RESERVED_FOR_TABLE_ALIAS.contains(kw),
        }
    }

    fn supports_comma_join_constraint(&self) -> bool {
        true
    }

    fn supports_aggregate_order_by_with_empty_args(&self) -> bool {
        true
    }

    /// See <https://www.sqlite.org/lang_expr.html#binaryops>
    fn supports_bitwise_shift_operators(&self) -> bool {
        true
    }

    fn supports_in_table_expression(&self) -> bool {
        true
    }

    /// The keywords SQLite does not accept as a name, as opposed to the
    /// keywords its grammar lets fall back to an identifier.
    /// See <https://www.sqlite.org/lang_keywords.html> and the `%fallback ID`
    /// list in [parse.y](https://sqlite.org/src/file?name=src/parse.y)
    fn is_table_factor(&self, kw: &Keyword, _parser: &mut Parser) -> bool {
        !matches!(
            kw,
            Keyword::ADD
                | Keyword::ALL
                | Keyword::ALTER
                | Keyword::AND
                | Keyword::AS
                | Keyword::AUTOINCREMENT
                | Keyword::BETWEEN
                | Keyword::CASE
                | Keyword::CHECK
                | Keyword::COLLATE
                | Keyword::COMMIT
                | Keyword::CONSTRAINT
                | Keyword::CREATE
                | Keyword::DEFAULT
                | Keyword::DEFERRABLE
                | Keyword::DELETE
                | Keyword::DISTINCT
                | Keyword::DROP
                | Keyword::ELSE
                | Keyword::ESCAPE
                | Keyword::EXCEPT
                | Keyword::EXISTS
                | Keyword::FOREIGN
                | Keyword::FROM
                | Keyword::GROUP
                | Keyword::HAVING
                | Keyword::IN
                | Keyword::INDEX
                | Keyword::INSERT
                | Keyword::INTERSECT
                | Keyword::INTO
                | Keyword::IS
                | Keyword::JOIN
                | Keyword::LIMIT
                | Keyword::NOT
                | Keyword::NOTHING
                | Keyword::NOTNULL
                | Keyword::NULL
                | Keyword::ON
                | Keyword::OR
                | Keyword::ORDER
                | Keyword::PRIMARY
                | Keyword::REFERENCES
                | Keyword::RETURNING
                | Keyword::SELECT
                | Keyword::SET
                | Keyword::TABLE
                | Keyword::THEN
                | Keyword::TO
                | Keyword::TRANSACTION
                | Keyword::UNION
                | Keyword::UNIQUE
                | Keyword::UPDATE
                | Keyword::USING
                | Keyword::VALUES
                | Keyword::WHEN
                | Keyword::WHERE
        )
    }

    /// Comparison and postfix operators bind within the BETWEEN low bound in SQLite (<https://www.sqlite.org/lang_expr.html>).
    fn between_low_bound_precedence(&self) -> u8 {
        self.prec_value(Precedence::And)
    }
}

/// SQLite permits signed and decimal numeric type modifiers.
fn parse_type_with_numeric_modifiers(parser: &mut Parser) -> Option<DataType> {
    let (data_type, token_count) = {
        let first = parser.peek_token_ref();
        let Token::Word(word) = &first.token else {
            return None;
        };
        if parser.peek_nth_token_ref(1).token != Token::LParen {
            return None;
        }
        let mut modifiers = [None; 2];
        let mut nth = 2;
        let mut is_nonstandard = false;
        let mut closed = false;
        for modifier in &mut modifiers {
            let sign = match parser.peek_nth_token_ref(nth).token {
                Token::Minus => "-",
                Token::Plus => "+",
                _ => "",
            };
            if !sign.is_empty() {
                is_nonstandard = true;
                nth += 1;
            }
            let Token::Number(number, _) = &parser.peek_nth_token_ref(nth).token else {
                return None;
            };
            is_nonstandard |= number.parse::<u64>().is_err();
            *modifier = Some((sign, number));
            match parser.peek_nth_token_ref(nth + 1).token {
                Token::RParen => {
                    closed = true;
                    break;
                }
                Token::Comma => nth += 2,
                _ => return None,
            }
        }
        if !closed || !is_nonstandard {
            return None;
        }
        let modifiers = modifiers
            .into_iter()
            .flatten()
            .map(|(sign, number)| {
                let mut value = String::with_capacity(sign.len() + number.len());
                value.push_str(sign);
                value.push_str(number);
                crate::ast::TypeModifier::Number(value, false)
            })
            .collect();
        (
            DataType::Custom(
                ObjectName::from(word.clone().into_ident(first.span)),
                modifiers,
            ),
            nth + 2,
        )
    };
    for _ in 0..token_count {
        parser.advance_token();
    }
    Some(data_type)
}

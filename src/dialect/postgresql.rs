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
use log::debug;

use crate::dialect::{Dialect, Precedence};
use crate::keywords::Keyword;
use crate::parser::{Parser, ParserError};
use crate::tokenizer::Token;

/// A [`Dialect`] for [PostgreSQL](https://www.postgresql.org/)
#[derive(Debug)]
pub struct PostgreSqlDialect {}

const PERIOD_PREC: u8 = 200;
const DOUBLE_COLON_PREC: u8 = 140;
const BRACKET_PREC: u8 = 130;
const COLLATE_PREC: u8 = 120;
const AT_TZ_PREC: u8 = 110;
const CARET_PREC: u8 = 100;
const MUL_DIV_MOD_OP_PREC: u8 = 90;
const PLUS_MINUS_PREC: u8 = 80;
// there's no XOR operator in PostgreSQL, but support it here to avoid breaking tests
const XOR_PREC: u8 = 75;
const PG_OTHER_PREC: u8 = 70;
const BETWEEN_LIKE_PREC: u8 = 60;
const EQ_PREC: u8 = 50;
const IS_PREC: u8 = 40;
const NOT_PREC: u8 = 30;
const AND_PREC: u8 = 20;
const OR_PREC: u8 = 10;

impl Dialect for PostgreSqlDialect {
    fn identifier_quote_style(&self, _identifier: &str) -> Option<char> {
        Some('"')
    }

    fn is_delimited_identifier_start(&self, ch: char) -> bool {
        ch == '"' // Postgres does not support backticks to quote identifiers
    }

    fn is_identifier_start(&self, ch: char) -> bool {
        ch.is_alphabetic() || ch == '_' ||
        // PostgreSQL implements Unicode characters in identifiers.
        !ch.is_ascii()
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        ch.is_alphabetic() || ch.is_ascii_digit() || ch == '$' || ch == '_'  ||
        // PostgreSQL implements Unicode characters in identifiers.
        !ch.is_ascii()
    }

    fn supports_unicode_string_literal(&self) -> bool {
        true
    }

    /// See <https://www.postgresql.org/docs/current/sql-createoperator.html>
    fn is_custom_operator_part(&self, ch: char) -> bool {
        matches!(
            ch,
            '+' | '-'
                | '*'
                | '/'
                | '<'
                | '>'
                | '='
                | '~'
                | '!'
                | '@'
                | '#'
                | '%'
                | '^'
                | '&'
                | '|'
                | '`'
                | '?'
        )
    }

    fn get_next_precedence(&self, parser: &Parser) -> Option<Result<u8, ParserError>> {
        let token = parser.peek_token();
        debug!("get_next_precedence() {token:?}");

        // we only return some custom value here when the behaviour (not merely the numeric value) differs
        // from the default implementation
        match token.token {
            Token::Word(w) if w.keyword == Keyword::COLLATE => Some(Ok(COLLATE_PREC)),
            Token::LBracket => Some(Ok(BRACKET_PREC)),
            Token::Arrow
            | Token::LongArrow
            | Token::HashArrow
            | Token::HashLongArrow
            | Token::AtArrow
            | Token::ArrowAt
            | Token::HashMinus
            | Token::AtQuestion
            | Token::AtAt
            | Token::Question
            | Token::QuestionAnd
            | Token::QuestionPipe
            | Token::ExclamationMark
            | Token::Overlap
            | Token::CaretAt
            | Token::StringConcat
            | Token::Sharp
            | Token::ShiftRight
            | Token::ShiftLeft
            | Token::CustomBinaryOperator(_) => Some(Ok(PG_OTHER_PREC)),
            _ => None,
        }
    }

    fn supports_filter_during_aggregation(&self) -> bool {
        true
    }

    fn supports_group_by_expr(&self) -> bool {
        true
    }

    fn prec_value(&self, prec: Precedence) -> u8 {
        match prec {
            Precedence::Period => PERIOD_PREC,
            Precedence::DoubleColon => DOUBLE_COLON_PREC,
            Precedence::AtTz => AT_TZ_PREC,
            Precedence::MulDivModOp => MUL_DIV_MOD_OP_PREC,
            Precedence::PlusMinus => PLUS_MINUS_PREC,
            Precedence::Xor => XOR_PREC,
            Precedence::Ampersand => PG_OTHER_PREC,
            Precedence::Caret => CARET_PREC,
            Precedence::Pipe => PG_OTHER_PREC,
            Precedence::Between => BETWEEN_LIKE_PREC,
            Precedence::Eq => EQ_PREC,
            Precedence::Like => BETWEEN_LIKE_PREC,
            Precedence::Is => IS_PREC,
            Precedence::PgOther => PG_OTHER_PREC,
            Precedence::UnaryNot => NOT_PREC,
            Precedence::And => AND_PREC,
            Precedence::Or => OR_PREC,
        }
    }

    fn allow_extract_custom(&self) -> bool {
        true
    }

    fn allow_extract_single_quotes(&self) -> bool {
        true
    }

    fn supports_create_index_with_clause(&self) -> bool {
        true
    }

    /// see <https://www.postgresql.org/docs/current/sql-explain.html>
    fn supports_explain_with_utility_options(&self) -> bool {
        true
    }

    /// see <https://www.postgresql.org/docs/current/sql-listen.html>
    /// see <https://www.postgresql.org/docs/current/sql-unlisten.html>
    /// see <https://www.postgresql.org/docs/current/sql-notify.html>
    fn supports_listen_notify(&self) -> bool {
        true
    }

    /// see <https://www.postgresql.org/docs/13/functions-math.html>
    fn supports_factorial_operator(&self) -> bool {
        true
    }

    /// see <https://www.postgresql.org/docs/current/sql-comment.html>
    fn supports_comment_on(&self) -> bool {
        true
    }

    /// See <https://www.postgresql.org/docs/current/sql-load.html>
    fn supports_load_extension(&self) -> bool {
        true
    }

    /// See <https://www.postgresql.org/docs/current/functions-json.html>
    ///
    /// Required to support the colon in:
    /// ```sql
    /// SELECT json_object('a': 'b')
    /// ```
    fn supports_named_fn_args_with_colon_operator(&self) -> bool {
        true
    }

    /// See <https://www.postgresql.org/docs/current/functions-json.html>
    ///
    /// Required to support the label in:
    /// ```sql
    /// SELECT json_object('label': 'value')
    /// ```
    fn supports_named_fn_args_with_expr_name(&self) -> bool {
        true
    }

    /// Return true if the dialect supports empty projections in SELECT statements
    ///
    /// Example
    /// ```sql
    /// SELECT from table_name
    /// ```
    fn supports_empty_projections(&self) -> bool {
        true
    }

    fn supports_nested_comments(&self) -> bool {
        true
    }

    fn supports_string_escape_constant(&self) -> bool {
        true
    }

    fn supports_numeric_literal_underscores(&self) -> bool {
        true
    }

    /// See: <https://www.postgresql.org/docs/current/arrays.html#ARRAYS-DECLARATION>
    fn supports_array_typedef_with_brackets(&self) -> bool {
        true
    }

    fn supports_geometric_types(&self) -> bool {
        true
    }

    fn supports_set_names(&self) -> bool {
        true
    }

    fn supports_alter_column_type_using(&self) -> bool {
        true
    }

    /// Postgres supports `NOTNULL` as an alias for `IS NOT NULL`
    /// See: <https://www.postgresql.org/docs/17/functions-comparison.html>
    fn supports_notnull_operator(&self) -> bool {
        true
    }

    /// [Postgres] supports optional field and precision options for `INTERVAL` data type.
    ///
    /// [Postgres]: https://www.postgresql.org/docs/17/datatype-datetime.html
    fn supports_interval_options(&self) -> bool {
        true
    }
}

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

use crate::ast::{CommentObject, Statement};
use crate::dialect::{Dialect, Precedence};
use crate::keywords::Keyword;
use crate::parser::{Parser, ParserError};
use crate::tokenizer::Token;

/// A [`Dialect`] for [PostgreSQL](https://www.postgresql.org/)
#[derive(Debug)]
pub struct PostgreSqlDialect {}

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
        // See https://www.postgresql.org/docs/11/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS
        // We don't yet support identifiers beginning with "letters with
        // diacritical marks"
        ch.is_alphabetic() || ch == '_'
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        ch.is_alphabetic() || ch.is_ascii_digit() || ch == '$' || ch == '_'
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
        debug!("get_next_precedence() {:?}", token);

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

    fn parse_statement(&self, parser: &mut Parser) -> Option<Result<Statement, ParserError>> {
        if parser.parse_keyword(Keyword::COMMENT) {
            Some(parse_comment(parser))
        } else {
            None
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
}

pub fn parse_comment(parser: &mut Parser) -> Result<Statement, ParserError> {
    let if_exists = parser.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);

    parser.expect_keyword(Keyword::ON)?;
    let token = parser.next_token();

    let (object_type, object_name) = match token.token {
        Token::Word(w) if w.keyword == Keyword::COLUMN => {
            let object_name = parser.parse_object_name(false)?;
            (CommentObject::Column, object_name)
        }
        Token::Word(w) if w.keyword == Keyword::TABLE => {
            let object_name = parser.parse_object_name(false)?;
            (CommentObject::Table, object_name)
        }
        _ => parser.expected("comment object_type", token)?,
    };

    parser.expect_keyword(Keyword::IS)?;
    let comment = if parser.parse_keyword(Keyword::NULL) {
        None
    } else {
        Some(parser.parse_literal_string()?)
    };
    Ok(Statement::Comment {
        object_type,
        object_name,
        comment,
        if_exists,
    })
}

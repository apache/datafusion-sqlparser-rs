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
use crate::dialect::Dialect;
use crate::keywords::Keyword;
use crate::parser::{Parser, ParserError, Precedence};
use crate::tokenizer::Token;

/// A [`Dialect`] for [PostgreSQL](https://www.postgresql.org/)
#[derive(Debug)]
pub struct PostgreSqlDialect {}

const BRACKET_PREC: u8 = 130;
const COLLATE_PREC: u8 = 120;

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

        macro_rules! p {
            ($precedence:ident) => {self.precedence_numeric(Precedence::$precedence)};
        }

        let precedence = match token.token {
            Token::Word(w) if w.keyword == Keyword::OR => p!(Or),
            Token::Word(w) if w.keyword == Keyword::XOR => p!(Xor),
            Token::Word(w) if w.keyword == Keyword::AND => p!(And),
            Token::Word(w) if w.keyword == Keyword::AT => {
                match (
                    parser.peek_nth_token(1).token,
                    parser.peek_nth_token(2).token,
                ) {
                    (Token::Word(w), Token::Word(w2))
                        if w.keyword == Keyword::TIME && w2.keyword == Keyword::ZONE =>
                    {
                        p!(AtTz)
                    }
                    _ => p!(Unknown),
                }
            }

            Token::Word(w) if w.keyword == Keyword::NOT => match parser.peek_nth_token(1).token {
                // The precedence of NOT varies depending on keyword that
                // follows it. If it is followed by IN, BETWEEN, or LIKE,
                // it takes on the precedence of those tokens. Otherwise, it
                // is not an infix operator, and therefore has zero
                // precedence.
                Token::Word(w) if w.keyword == Keyword::IN => p!(Between),
                Token::Word(w) if w.keyword == Keyword::BETWEEN => p!(Between),
                Token::Word(w) if w.keyword == Keyword::LIKE => p!(Between),
                Token::Word(w) if w.keyword == Keyword::ILIKE => p!(Between),
                Token::Word(w) if w.keyword == Keyword::RLIKE => p!(Between),
                Token::Word(w) if w.keyword == Keyword::REGEXP => p!(Between),
                Token::Word(w) if w.keyword == Keyword::SIMILAR => p!(Between),
                _ => p!(Unknown),
            },
            Token::Word(w) if w.keyword == Keyword::IS => p!(Is),
            Token::Word(w) if w.keyword == Keyword::IN => p!(Between),
            Token::Word(w) if w.keyword == Keyword::BETWEEN => p!(Between),
            Token::Word(w) if w.keyword == Keyword::LIKE => p!(Between),
            Token::Word(w) if w.keyword == Keyword::ILIKE => p!(Between),
            Token::Word(w) if w.keyword == Keyword::RLIKE => p!(Between),
            Token::Word(w) if w.keyword == Keyword::REGEXP => p!(Between),
            Token::Word(w) if w.keyword == Keyword::SIMILAR => p!(Between),
            Token::Word(w) if w.keyword == Keyword::OPERATOR => p!(Between),
            Token::Word(w) if w.keyword == Keyword::DIV => p!(MulDivModOp),
            Token::Word(w) if w.keyword == Keyword::COLLATE => COLLATE_PREC,
            Token::Eq
            | Token::Lt
            | Token::LtEq
            | Token::Neq
            | Token::Gt
            | Token::GtEq
            | Token::DoubleEq
            | Token::Tilde
            | Token::TildeAsterisk
            | Token::ExclamationMarkTilde
            | Token::ExclamationMarkTildeAsterisk
            | Token::DoubleTilde
            | Token::DoubleTildeAsterisk
            | Token::ExclamationMarkDoubleTilde
            | Token::ExclamationMarkDoubleTildeAsterisk
            | Token::Spaceship => p!(Eq),
            Token::Pipe => p!(Pipe),
            Token::Caret => p!(Caret),
            Token::Ampersand => p!(Ampersand),
            Token::Plus | Token::Minus => p!(PlusMinus),
            Token::Mul | Token::Div | Token::Mod => p!(MulDivModOp),
            Token::DoubleColon => p!(DoubleColon),
            Token::LBracket => BRACKET_PREC,
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
            | Token::CustomBinaryOperator(_) => p!(PgOther),
            _ => p!(Unknown),
        };
        Some(Ok(precedence))
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

    /*
    const DOUBLE_COLON_PREC: u8 = 140;
    const BRACKET_PREC: u8 = 130;
    const COLLATE_PREC: u8 = 120;
    const AT_TZ_PREC: u8 = 110;
    const CARET_PREC: u8 = 100;
    const MUL_DIV_MOD_OP_PREC: u8 = 90;
    const PLUS_MINUS_PREC: u8 = 80;
    const PG_OTHER_PREC: u8 = 70;
    const BETWEEN_LIKE_PREC: u8 = 60;
    const EQ_PREC: u8 = 50;
    const IS_PREC: u8 = 40;
    const NOT_PREC: u8 = 30;
    const AND_PREC: u8 = 20;
    const OR_PREC: u8 = 10;
    const UNKNOWN_PREC: u8 = 0;
     */
    /// based on https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-PRECEDENCE
    fn precedence_numeric(&self, p: Precedence) -> u8 {
        match p {
            Precedence::DoubleColon => 140,
            Precedence::AtTz => 110,
            Precedence::MulDivModOp => 90,
            Precedence::PlusMinus => 80,
            Precedence::Caret => 110,
            Precedence::Between => 60,
            Precedence::Eq => 50,
            Precedence::Like => 60,
            Precedence::Is => 40,
            Precedence::PgOther | Precedence::Pipe | Precedence::Ampersand => 70,
            Precedence::UnaryNot => 30,
            Precedence::And => 20,
            Precedence::Xor => 79,
            Precedence::Or => 10,
            Precedence::Unknown => 0,
        }
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

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

use crate::ast::{CommentObject, Statement};
use crate::dialect::Dialect;
use crate::keywords::Keyword;
use crate::parser::{Parser, ParserError};
use crate::tokenizer::Token;

#[derive(Debug)]
pub struct PostgreSqlDialect {}

impl Dialect for PostgreSqlDialect {
    fn is_identifier_start(&self, ch: char) -> bool {
        // See https://www.postgresql.org/docs/11/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS
        // We don't yet support identifiers beginning with "letters with
        // diacritical marks and non-Latin letters"
        ('a'..='z').contains(&ch) || ('A'..='Z').contains(&ch) || ch == '_'
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        ('a'..='z').contains(&ch)
            || ('A'..='Z').contains(&ch)
            || ('0'..='9').contains(&ch)
            || ch == '$'
            || ch == '_'
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
}

pub fn parse_comment(parser: &mut Parser) -> Result<Statement, ParserError> {
    parser.expect_keyword(Keyword::ON)?;
    let token = parser.next_token();

    let (object_type, object_name) = match token.token {
        Token::Word(w) if w.keyword == Keyword::COLUMN => {
            let object_name = parser.parse_object_name()?;
            (CommentObject::Column, object_name)
        }
        Token::Word(w) if w.keyword == Keyword::TABLE => {
            let object_name = parser.parse_object_name()?;
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
    })
}

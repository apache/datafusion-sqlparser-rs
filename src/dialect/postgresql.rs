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

use crate::ast::{CommentObject, ObjectName, Statement};
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
        // diacritical marks"
        ch.is_alphabetic() || ch == '_'
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        ch.is_alphabetic() || ch.is_ascii_digit() || ch == '$' || ch == '_'
    }

    fn parse_statement(&self, parser: &mut Parser) -> Option<Result<Statement, ParserError>> {
        if parser.parse_keyword(Keyword::COMMENT) {
            Some(parse_comment(parser))
        } else if parser.parse_keyword(Keyword::VACUUM) {
            Some(parse_vacuum(parser))
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
}

const VACUUM_BASIC_OPTIONS: [Keyword; 4] = [
    Keyword::FULL,
    Keyword::FREEZE,
    Keyword::VERBOSE,
    Keyword::ANALYZE,
];
const VACUUM_OPTIONS: [Keyword; 14] = [
    Keyword::FULL,
    Keyword::FREEZE,
    Keyword::VERBOSE,
    Keyword::ANALYZE,
    Keyword::DISABLE_PAGE_SKIPPING,
    Keyword::SKIP_LOCKED,
    Keyword::INDEX_CLEANUP,
    Keyword::PROCESS_MAIN,
    Keyword::PROCESS_TOAST,
    Keyword::TRUNCATE,
    Keyword::PARALLEL,
    Keyword::SKIP_DATABASE_STATS,
    Keyword::ONLY_DATABASE_STATS,
    Keyword::BUFFER_USAGE_LIMIT,
];

pub fn parse_vacuum(parser: &mut Parser) -> Result<Statement, ParserError> {
    let mut options: Vec<(Keyword, Option<String>)> = vec![];
    let mut table_and_columns: Vec<(ObjectName, Vec<ObjectName>)> = vec![];

    if !parser.consume_token(&Token::EOF) {
        parse_vacuum_options(parser, &mut options)?;
    }

    if !parser.consume_token(&Token::EOF) {
        parse_multiple_table_and_columns(parser, &mut table_and_columns)?;
    }

    return Ok(Statement::Vacuum {
        table_and_columns,
        options,
    });
}

fn parse_vacuum_options(
    parser: &mut Parser<'_>,
    options: &mut Vec<(Keyword, Option<String>)>,
) -> Result<(), ParserError> {
    Ok(if parser.consume_token(&Token::LParen) {
        if parser.consume_token(&Token::RParen) {
            return parser.expected("vacuum option", parser.peek_token());
        }
        loop {
            if parser.consume_token(&Token::EOF) {
                return parser.expected("')'", parser.peek_token());
            }

            let mut opt: (Keyword, Option<String>) =
                (parser.expect_one_of_keywords(&VACUUM_OPTIONS)?, None);

            let next_tok = parser.next_token();
            if next_tok == Token::Comma {
                options.push(opt);
                continue;
            } else if next_tok == Token::RParen {
                options.push(opt);
                break;
            }

            opt.1 = match opt.0 {
                Keyword::FULL
                | Keyword::FREEZE
                | Keyword::VERBOSE
                | Keyword::ANALYZE
                | Keyword::DISABLE_PAGE_SKIPPING
                | Keyword::SKIP_LOCKED
                | Keyword::PROCESS_MAIN
                | Keyword::PROCESS_TOAST
                | Keyword::TRUNCATE
                | Keyword::SKIP_DATABASE_STATS
                | Keyword::ONLY_DATABASE_STATS => match &next_tok.token {
                    Token::Word(w) => match parse_boolean_value(&w.value) {
                        Some(value) => Some(value),
                        None => {
                            return parser.expected("vacuum option boolean value", next_tok);
                        }
                    },
                    Token::Number(value, _) => {
                        if value == "1" {
                            Some("TRUE".to_string())
                        } else if value == "0" {
                            Some("FALSE".to_string())
                        } else {
                            return parser.expected("vacuum option boolean value", next_tok);
                        }
                    }
                    Token::SingleQuotedString(value) | Token::DoubleQuotedString(value) => {
                        match parse_boolean_value(value) {
                            Some(value) => Some(value),
                            None => {
                                return parser.expected("vacuum option boolean value", next_tok);
                            }
                        }
                    }
                    _ => {
                        return parser.expected("vacuum option boolean value", next_tok);
                    }
                },
                Keyword::INDEX_CLEANUP => match &next_tok.token {
                    Token::Word(w) => match parse_index_cleanup_value(&w.value) {
                        Some(value) => Some(value),
                        None => {
                            return parser.expected("vacuum option index cleanup value", next_tok);
                        }
                    },
                    Token::SingleQuotedString(value) | Token::DoubleQuotedString(value) => {
                        match parse_index_cleanup_value(value) {
                            Some(value) => Some(value),
                            None => {
                                return parser
                                    .expected("vacuum option index cleanup value", next_tok);
                            }
                        }
                    }
                    _ => {
                        return parser.expected("vacuum option index cleanup value", next_tok);
                    }
                },
                Keyword::PARALLEL | Keyword::BUFFER_USAGE_LIMIT => match &next_tok.token {
                    Token::Number(_, _) => Some(next_tok.token.to_string()),
                    _ => {
                        return parser.expected("vacuum option boolean value", next_tok);
                    }
                },
                _ => {
                    return parser.expected("vacuum option", parser.peek_token());
                }
            };

            options.push(opt);

            if parser.consume_token(&Token::Comma) {
                continue;
            } else if parser.consume_token(&Token::RParen) {
                break;
            } else {
                return parser.expected("vacuum option", parser.peek_token());
            }
        }
    } else {
        // In this mode, the options should be in this specific order
        for opt_key in VACUUM_BASIC_OPTIONS {
            if parser.parse_keyword(opt_key) {
                options.push((opt_key, None));
            }
        }

        let remaining_opt = parser.parse_one_of_keywords(&VACUUM_BASIC_OPTIONS);
        if let Some(opt) = remaining_opt {
            return Err(ParserError::ParserError(format!(
                "Found an out-of-order vacuum option: {:?}",
                opt
            )));
        }
    })
}

fn parse_index_cleanup_value(value: &String) -> Option<String> {
    let value = value.to_uppercase();
    if value == "ON" || value == "OFF" || value == "AUTO" {
        Some(value)
    } else {
        None
    }
}

fn parse_boolean_value(value: &String) -> Option<String> {
    let value = value.to_uppercase();
    if value == "ON" || value == "TRUE" {
        Some("TRUE".to_string())
    } else if value == "OFF" || value == "FALSE" {
        Some("FALSE".to_string())
    } else {
        None
    }
}

pub fn parse_multiple_table_and_columns(
    parser: &mut Parser<'_>,
    table_and_columns: &mut Vec<(ObjectName, Vec<ObjectName>)>,
) -> Result<(), ParserError> {
    loop {
        let (table_name, columns) = parse_table_and_columns(parser)?;
        table_and_columns.push((table_name, columns));

        if parser.consume_token(&Token::EOF) {
            break;
        } else if !parser.consume_token(&Token::Comma) {
            return parser.expected("',' or EOF after table name", parser.peek_token());
        }
    }
    Ok(())
}

pub fn parse_table_and_columns(
    parser: &mut Parser<'_>,
) -> Result<(ObjectName, Vec<ObjectName>), ParserError> {
    let table_name = parser.parse_object_name()?;
    let columns = parse_column_list(parser)?;
    Ok((table_name, columns))
}

pub fn parse_column_list(parser: &mut Parser<'_>) -> Result<Vec<ObjectName>, ParserError> {
    let mut columns = vec![];
    if !parser.consume_token(&Token::LParen) {
        return Ok(columns);
    }

    if parser.consume_token(&Token::RParen) {
        return parser.expected("column name", parser.peek_token());
    }

    loop {
        columns.push(parser.parse_object_name()?);

        let comma = parser.consume_token(&Token::Comma);
        if parser.consume_token(&Token::RParen) {
            // allow a trailing comma, even though it's not in standard
            break;
        } else if !comma {
            return parser.expected("',' or ')' after column definition", parser.peek_token());
        }
    }

    Ok(columns)
}

pub fn parse_comment(parser: &mut Parser) -> Result<Statement, ParserError> {
    let if_exists = parser.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);

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
        if_exists,
    })
}

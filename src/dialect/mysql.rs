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
use alloc::boxed::Box;
#[cfg(not(feature = "std"))]
use alloc::format;
#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

use crate::{
    ast::{
        BinaryOperator, Expr, Ident, LockTable, LockTableType, SqlOption, Statement, StorageType,
        TablespaceOption, Value,
    },
    dialect::Dialect,
    keywords::Keyword,
    parser::{Parser, ParserError},
    tokenizer::{Token, Word},
};

use super::keywords;

const RESERVED_FOR_TABLE_ALIAS_MYSQL: &[Keyword] = &[Keyword::USE, Keyword::IGNORE, Keyword::FORCE];

/// A [`Dialect`] for [MySQL](https://www.mysql.com/)
#[derive(Debug)]
pub struct MySqlDialect {}

impl Dialect for MySqlDialect {
    fn is_identifier_start(&self, ch: char) -> bool {
        // See https://dev.mysql.com/doc/refman/8.0/en/identifiers.html.
        // Identifiers which begin with a digit are recognized while tokenizing numbers,
        // so they can be distinguished from exponent numeric literals.
        ch.is_alphabetic()
            || ch == '_'
            || ch == '$'
            || ch == '@'
            || ('\u{0080}'..='\u{ffff}').contains(&ch)
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        self.is_identifier_start(ch) || ch.is_ascii_digit()
    }

    fn is_delimited_identifier_start(&self, ch: char) -> bool {
        ch == '`'
    }

    fn identifier_quote_style(&self, _identifier: &str) -> Option<char> {
        Some('`')
    }

    // See https://dev.mysql.com/doc/refman/8.0/en/string-literals.html#character-escape-sequences
    fn supports_string_literal_backslash_escape(&self) -> bool {
        true
    }

    fn ignores_wildcard_escapes(&self) -> bool {
        true
    }

    fn supports_numeric_prefix(&self) -> bool {
        true
    }

    fn parse_infix(
        &self,
        parser: &mut crate::parser::Parser,
        expr: &crate::ast::Expr,
        _precedence: u8,
    ) -> Option<Result<crate::ast::Expr, ParserError>> {
        // Parse DIV as an operator
        if parser.parse_keyword(Keyword::DIV) {
            Some(Ok(Expr::BinaryOp {
                left: Box::new(expr.clone()),
                op: BinaryOperator::MyIntegerDivide,
                right: Box::new(parser.parse_expr().unwrap()),
            }))
        } else {
            None
        }
    }

    fn parse_statement(&self, parser: &mut Parser) -> Option<Result<Statement, ParserError>> {
        if parser.parse_keywords(&[Keyword::LOCK, Keyword::TABLES]) {
            Some(parse_lock_tables(parser))
        } else if parser.parse_keywords(&[Keyword::UNLOCK, Keyword::TABLES]) {
            Some(parse_unlock_tables(parser))
        } else {
            None
        }
    }

    fn require_interval_qualifier(&self) -> bool {
        true
    }

    fn supports_limit_comma(&self) -> bool {
        true
    }

    /// See: <https://dev.mysql.com/doc/refman/8.4/en/create-table-select.html>
    fn supports_create_table_select(&self) -> bool {
        true
    }

    /// See: <https://dev.mysql.com/doc/refman/8.4/en/insert.html>
    fn supports_insert_set(&self) -> bool {
        true
    }

    fn supports_user_host_grantee(&self) -> bool {
        true
    }

    fn is_table_factor_alias(&self, explicit: bool, kw: &Keyword, _parser: &mut Parser) -> bool {
        explicit
            || (!keywords::RESERVED_FOR_TABLE_ALIAS.contains(kw)
                && !RESERVED_FOR_TABLE_ALIAS_MYSQL.contains(kw))
    }

    fn supports_table_hints(&self) -> bool {
        true
    }

    fn requires_single_line_comment_whitespace(&self) -> bool {
        true
    }

    fn supports_match_against(&self) -> bool {
        true
    }

    fn supports_set_names(&self) -> bool {
        true
    }
    /// Dialect-specific table/view option parser override
    ///
    /// This method is called to parse the next table/view option.
    ///
    /// If `None` is returned, falls back to the default behavior.
    /// <https://dev.mysql.com/doc/refman/8.4/en/create-table.html>
    fn parse_plain_option(&self, parser: &mut Parser) -> Result<Option<SqlOption>, ParserError> {
        //Some consts
        const COMPRESSION_OPTS: [&str; 3] = ["ZLIB", "LZ4", "NONE"];
        const INSERT_METHODS_OPTS: [&str; 3] = ["NO", "FIRST", "LAST"];

        let keyword = match parser.parse_one_of_keywords(&[
            Keyword::INSERT_METHOD,
            Keyword::KEY_BLOCK_SIZE,
            Keyword::ROW_FORMAT,
            Keyword::DATA,
            Keyword::INDEX,
            Keyword::PACK_KEYS,
            Keyword::STATS_AUTO_RECALC,
            Keyword::STATS_PERSISTENT,
            Keyword::STATS_SAMPLE_PAGES,
            Keyword::DELAY_KEY_WRITE,
            Keyword::COMPRESSION,
            Keyword::ENCRYPTION,
            Keyword::MAX_ROWS,
            Keyword::MIN_ROWS,
            Keyword::AUTOEXTEND_SIZE,
            Keyword::AVG_ROW_LENGTH,
            Keyword::CHECKSUM,
            Keyword::CONNECTION,
            Keyword::ENGINE_ATTRIBUTE,
            Keyword::PASSWORD,
            Keyword::SECONDARY_ENGINE_ATTRIBUTE,
            Keyword::START,
            Keyword::TABLESPACE,
            Keyword::UNION,
        ]) {
            Some(keyword) => keyword,
            None => return Ok(None),
        };

        //Optional equal sign
        let _ = parser.consume_token(&Token::Eq);
        let value = parser.next_token();
        let span = value.span;

        match (keyword, value.token) {
            //Handled in common code
            // ENGINE [=] engine_name
            // AUTO_INCREMENT [=] value
            // [DEFAULT] CHARACTER SET [=] charset_name
            // [DEFAULT] COLLATE [=] collation_name

            // KEY_BLOCK_SIZE [=] value
            (Keyword::KEY_BLOCK_SIZE, Token::Number(n, l)) => Ok(Some(SqlOption::KeyValue {
                key: Ident::new("KEY_BLOCK_SIZE"),
                value: Expr::value(Value::Number(Parser::parse(n, value.span.start)?, l)),
            })),

            // ROW_FORMAT [=] {DEFAULT | DYNAMIC | FIXED | COMPRESSED | REDUNDANT | COMPACT}
            (Keyword::ROW_FORMAT, Token::Word(w)) => Ok(Some(SqlOption::KeyValue {
                key: Ident::new("ROW_FORMAT"),
                value: Expr::Identifier(Ident::with_span(span, w.value)),
            })),

            // {DATA | INDEX} DIRECTORY [=] 'absolute path to directory'
            (Keyword::DATA, Token::Word(w)) if w.keyword == Keyword::DIRECTORY => {
                let _ = parser.consume_token(&Token::Eq);

                let next_token = parser.next_token();

                if let Token::SingleQuotedString(s) = next_token.token {
                    Ok(Some(SqlOption::KeyValue {
                        key: Ident::new("DATA DIRECTORY"),
                        value: Expr::value(Value::SingleQuotedString(s)),
                    }))
                } else {
                    parser.expected("Token::SingleQuotedString", next_token)?
                }
            }
            (Keyword::INDEX, Token::Word(w)) if w.keyword == Keyword::DIRECTORY => {
                let _ = parser.consume_token(&Token::Eq);
                let next_token = parser.next_token();

                if let Token::SingleQuotedString(s) = next_token.token {
                    Ok(Some(SqlOption::KeyValue {
                        key: Ident::new("INDEX DIRECTORY"),
                        value: Expr::value(Value::SingleQuotedString(s)),
                    }))
                } else {
                    parser.expected("Token::SingleQuotedString", next_token)?
                }
            }

            // PACK_KEYS [=] {0 | 1 | DEFAULT}
            (Keyword::PACK_KEYS, Token::Number(n, l)) => Ok(Some(SqlOption::KeyValue {
                key: Ident::new("PACK_KEYS"),
                value: Expr::value(Value::Number(Parser::parse(n, value.span.start)?, l)),
            })),

            (Keyword::PACK_KEYS, Token::Word(s)) if s.value.to_uppercase() == "DEFAULT" => {
                Ok(Some(SqlOption::KeyValue {
                    key: Ident::new("PACK_KEYS"),
                    value: Expr::value(Value::SingleQuotedString(s.value)),
                }))
            }

            // STATS_AUTO_RECALC [=] {DEFAULT | 0 | 1}
            (Keyword::STATS_AUTO_RECALC, Token::Number(n, l)) => Ok(Some(SqlOption::KeyValue {
                key: Ident::new("STATS_AUTO_RECALC"),
                value: Expr::value(Value::Number(Parser::parse(n, value.span.start)?, l)),
            })),

            (Keyword::STATS_AUTO_RECALC, Token::Word(s)) if s.value.to_uppercase() == "DEFAULT" => {
                Ok(Some(SqlOption::KeyValue {
                    key: Ident::new("STATS_AUTO_RECALC"),
                    value: Expr::value(Value::SingleQuotedString(s.value)),
                }))
            }

            //STATS_PERSISTENT [=] {DEFAULT | 0 | 1}
            (Keyword::STATS_PERSISTENT, Token::Number(n, l)) => Ok(Some(SqlOption::KeyValue {
                key: Ident::new("STATS_PERSISTENT"),
                value: Expr::value(Value::Number(Parser::parse(n, value.span.start)?, l)),
            })),

            (Keyword::STATS_PERSISTENT, Token::Word(s)) if s.value.to_uppercase() == "DEFAULT" => {
                Ok(Some(SqlOption::KeyValue {
                    key: Ident::new("STATS_PERSISTENT"),
                    value: Expr::value(Value::SingleQuotedString(s.value)),
                }))
            }

            // STATS_SAMPLE_PAGES [=] value
            (Keyword::STATS_SAMPLE_PAGES, Token::Number(n, l)) => Ok(Some(SqlOption::KeyValue {
                key: Ident::new("STATS_SAMPLE_PAGES"),
                value: Expr::value(Value::Number(Parser::parse(n, value.span.start)?, l)),
            })),

            // DELAY_KEY_WRITE [=] {0 | 1}
            (Keyword::DELAY_KEY_WRITE, Token::Number(n, l)) => Ok(Some(SqlOption::KeyValue {
                key: Ident::new("DELAY_KEY_WRITE"),
                value: Expr::value(Value::Number(Parser::parse(n, value.span.start)?, l)),
            })),

            // COMPRESSION [=] {'ZLIB' | 'LZ4' | 'NONE'}
            (Keyword::COMPRESSION, Token::SingleQuotedString(s))
                if COMPRESSION_OPTS.contains(&s.to_uppercase().as_str()) =>
            {
                Ok(Some(SqlOption::KeyValue {
                    key: Ident::new("COMPRESSION"),
                    value: Expr::value(Value::SingleQuotedString(s)),
                }))
            }

            // ENCRYPTION [=] {'Y' | 'N'}
            (Keyword::ENCRYPTION, Token::SingleQuotedString(s)) => Ok(Some(SqlOption::KeyValue {
                key: Ident::new("ENCRYPTION"),
                value: Expr::value(Value::SingleQuotedString(s)),
            })),

            // MAX_ROWS [=] value
            (Keyword::MAX_ROWS, Token::Number(n, l)) => Ok(Some(SqlOption::KeyValue {
                key: Ident::new("MAX_ROWS"),
                value: Expr::value(Value::Number(Parser::parse(n, value.span.start)?, l)),
            })),

            // MIN_ROWS [=] value
            (Keyword::MIN_ROWS, Token::Number(n, l)) => Ok(Some(SqlOption::KeyValue {
                key: Ident::new("MIN_ROWS"),
                value: Expr::value(Value::Number(Parser::parse(n, value.span.start)?, l)),
            })),

            // AUTOEXTEND_SIZE [=] value
            (Keyword::AUTOEXTEND_SIZE, Token::Number(n, l)) => Ok(Some(SqlOption::KeyValue {
                key: Ident::new("AUTOEXTEND_SIZE"),
                value: Expr::value(Value::Number(Parser::parse(n, value.span.start)?, l)),
            })),

            // AVG_ROW_LENGTH [=] value
            (Keyword::AVG_ROW_LENGTH, Token::Number(n, l)) => Ok(Some(SqlOption::KeyValue {
                key: Ident::new("AVG_ROW_LENGTH"),
                value: Expr::value(Value::Number(Parser::parse(n, value.span.start)?, l)),
            })),

            // CHECKSUM [=] {0 | 1}
            (Keyword::CHECKSUM, Token::Number(n, l)) => Ok(Some(SqlOption::KeyValue {
                key: Ident::new("CHECKSUM"),
                value: Expr::value(Value::Number(Parser::parse(n, value.span.start)?, l)),
            })),

            // CONNECTION [=] 'connect_string'
            (Keyword::CONNECTION, Token::SingleQuotedString(s)) => Ok(Some(SqlOption::KeyValue {
                key: Ident::new("CONNECTION"),
                value: Expr::value(Value::SingleQuotedString(s)),
            })),

            // ENGINE_ATTRIBUTE [=] 'string'
            (Keyword::ENGINE_ATTRIBUTE, Token::SingleQuotedString(s)) => {
                Ok(Some(SqlOption::KeyValue {
                    key: Ident::new("ENGINE_ATTRIBUTE"),
                    value: Expr::value(Value::SingleQuotedString(s)),
                }))
            }

            // PASSWORD [=] 'string'
            (Keyword::PASSWORD, Token::SingleQuotedString(s)) => Ok(Some(SqlOption::KeyValue {
                key: Ident::new("PASSWORD"),
                value: Expr::value(Value::SingleQuotedString(s)),
            })),

            // SECONDARY_ENGINE_ATTRIBUTE [=] 'string'
            (Keyword::SECONDARY_ENGINE_ATTRIBUTE, Token::SingleQuotedString(s)) => {
                Ok(Some(SqlOption::KeyValue {
                    key: Ident::new("SECONDARY_ENGINE_ATTRIBUTE"),
                    value: Expr::value(Value::SingleQuotedString(s)),
                }))
            }

            // START TRANSACTION
            (Keyword::START, Token::Word(w)) if w.keyword == Keyword::TRANSACTION => {
                Ok(Some(SqlOption::Ident(Ident::new("START TRANSACTION"))))
            }

            // INSERT_METHOD [=] { NO | FIRST | LAST }
            (Keyword::INSERT_METHOD, Token::Word(w))
                if INSERT_METHODS_OPTS.contains(&w.value.to_uppercase().as_ref()) =>
            {
                Ok(Some(SqlOption::KeyValue {
                    key: Ident::new("INSERT_METHOD"),
                    value: Expr::Identifier(Ident::new(w.value)),
                }))
            }

            // TABLESPACE tablespace_name [STORAGE DISK] | [TABLESPACE tablespace_name] STORAGE MEMORY
            (Keyword::TABLESPACE, Token::Word(Word { value: name, .. }))
            | (Keyword::TABLESPACE, Token::SingleQuotedString(name)) => {
                let storage = match parser.parse_keyword(Keyword::STORAGE) {
                    true => {
                        let _ = parser.consume_token(&Token::Eq);
                        let storage_token = parser.next_token();
                        match &storage_token.token {
                            Token::Word(w) => match w.value.to_uppercase().as_str() {
                                "DISK" => Some(StorageType::Disk),
                                "MEMORY" => Some(StorageType::Memory),
                                _ => parser
                                    .expected("Storage type (DISK or MEMORY)", storage_token)?,
                            },
                            _ => parser.expected("Token::Word", storage_token)?,
                        }
                    }
                    false => None,
                };

                Ok(Some(SqlOption::TableSpace(TablespaceOption {
                    name,
                    storage,
                })))
            }

            // UNION [=] (tbl_name[,tbl_name]...)
            (Keyword::UNION, Token::LParen) => {
                let tables: Vec<Ident> =
                    parser.parse_comma_separated0(Parser::parse_identifier, Token::RParen)?;
                parser.expect_token(&Token::RParen)?;

                Ok(Some(SqlOption::Union(tables)))
            }

            _ => Err(ParserError::ParserError(format!(
                "Table option {keyword:?} does not have a matching value"
            ))),
        }
    }
}

/// `LOCK TABLES`
/// <https://dev.mysql.com/doc/refman/8.0/en/lock-tables.html>
fn parse_lock_tables(parser: &mut Parser) -> Result<Statement, ParserError> {
    let tables = parser.parse_comma_separated(parse_lock_table)?;
    Ok(Statement::LockTables { tables })
}

// tbl_name [[AS] alias] lock_type
fn parse_lock_table(parser: &mut Parser) -> Result<LockTable, ParserError> {
    let table = parser.parse_identifier()?;
    let alias =
        parser.parse_optional_alias(&[Keyword::READ, Keyword::WRITE, Keyword::LOW_PRIORITY])?;
    let lock_type = parse_lock_tables_type(parser)?;

    Ok(LockTable {
        table,
        alias,
        lock_type,
    })
}

// READ [LOCAL] | [LOW_PRIORITY] WRITE
fn parse_lock_tables_type(parser: &mut Parser) -> Result<LockTableType, ParserError> {
    if parser.parse_keyword(Keyword::READ) {
        if parser.parse_keyword(Keyword::LOCAL) {
            Ok(LockTableType::Read { local: true })
        } else {
            Ok(LockTableType::Read { local: false })
        }
    } else if parser.parse_keyword(Keyword::WRITE) {
        Ok(LockTableType::Write {
            low_priority: false,
        })
    } else if parser.parse_keywords(&[Keyword::LOW_PRIORITY, Keyword::WRITE]) {
        Ok(LockTableType::Write { low_priority: true })
    } else {
        parser.expected("an lock type in LOCK TABLES", parser.peek_token())
    }
}

/// UNLOCK TABLES
/// <https://dev.mysql.com/doc/refman/8.0/en/lock-tables.html>
fn parse_unlock_tables(_parser: &mut Parser) -> Result<Statement, ParserError> {
    Ok(Statement::UnlockTables)
}

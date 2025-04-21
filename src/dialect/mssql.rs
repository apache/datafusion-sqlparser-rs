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

use crate::ast::helpers::attached_token::AttachedToken;
use crate::ast::{
    BeginEndStatements, ConditionalStatementBlock, ConditionalStatements, IfStatement, Statement,
};
use crate::dialect::Dialect;
use crate::keywords::{self, Keyword};
use crate::parser::{Parser, ParserError};
use crate::tokenizer::Token;
#[cfg(not(feature = "std"))]
use alloc::{vec, vec::Vec};

const RESERVED_FOR_COLUMN_ALIAS: &[Keyword] = &[Keyword::IF, Keyword::ELSE];

/// A [`Dialect`] for [Microsoft SQL Server](https://www.microsoft.com/en-us/sql-server/)
#[derive(Debug)]
pub struct MsSqlDialect {}

impl Dialect for MsSqlDialect {
    fn is_delimited_identifier_start(&self, ch: char) -> bool {
        ch == '"' || ch == '['
    }

    fn is_identifier_start(&self, ch: char) -> bool {
        // See https://docs.microsoft.com/en-us/sql/relational-databases/databases/database-identifiers?view=sql-server-2017#rules-for-regular-identifiers
        ch.is_alphabetic() || ch == '_' || ch == '#' || ch == '@'
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        ch.is_alphabetic()
            || ch.is_ascii_digit()
            || ch == '@'
            || ch == '$'
            || ch == '#'
            || ch == '_'
    }

    /// SQL Server has `CONVERT(type, value)` instead of `CONVERT(value, type)`
    /// <https://learn.microsoft.com/en-us/sql/t-sql/functions/cast-and-convert-transact-sql?view=sql-server-ver16>
    fn convert_type_before_value(&self) -> bool {
        true
    }

    fn supports_outer_join_operator(&self) -> bool {
        true
    }

    fn supports_connect_by(&self) -> bool {
        true
    }

    fn supports_eq_alias_assignment(&self) -> bool {
        true
    }

    fn supports_try_convert(&self) -> bool {
        true
    }

    /// In MSSQL, there is no boolean type, and `true` and `false` are valid column names
    fn supports_boolean_literals(&self) -> bool {
        false
    }

    fn supports_named_fn_args_with_colon_operator(&self) -> bool {
        true
    }

    fn supports_named_fn_args_with_expr_name(&self) -> bool {
        true
    }

    fn supports_named_fn_args_with_rarrow_operator(&self) -> bool {
        false
    }

    fn supports_start_transaction_modifier(&self) -> bool {
        true
    }

    fn supports_end_transaction_modifier(&self) -> bool {
        true
    }

    /// See: <https://learn.microsoft.com/en-us/sql/t-sql/statements/set-statements-transact-sql>
    fn supports_set_stmt_without_operator(&self) -> bool {
        true
    }

    /// See: <https://learn.microsoft.com/en-us/sql/relational-databases/tables/querying-data-in-a-system-versioned-temporal-table>
    fn supports_timestamp_versioning(&self) -> bool {
        true
    }

    /// See <https://learn.microsoft.com/en-us/sql/t-sql/language-elements/slash-star-comment-transact-sql?view=sql-server-ver16>
    fn supports_nested_comments(&self) -> bool {
        true
    }

    /// See <https://learn.microsoft.com/en-us/sql/t-sql/queries/from-transact-sql>
    fn supports_object_name_double_dot_notation(&self) -> bool {
        true
    }

    fn is_column_alias(&self, kw: &Keyword, _parser: &mut Parser) -> bool {
        !keywords::RESERVED_FOR_COLUMN_ALIAS.contains(kw) && !RESERVED_FOR_COLUMN_ALIAS.contains(kw)
    }

    fn parse_statement(&self, parser: &mut Parser) -> Option<Result<Statement, ParserError>> {
        if parser.peek_keyword(Keyword::IF) {
            Some(self.parse_if_stmt(parser))
        } else {
            None
        }
    }
}

impl MsSqlDialect {
    /// ```sql
    /// IF boolean_expression
    ///     { sql_statement | statement_block }
    /// [ ELSE
    ///     { sql_statement | statement_block } ]
    /// ```
    fn parse_if_stmt(&self, parser: &mut Parser) -> Result<Statement, ParserError> {
        let if_token = parser.expect_keyword(Keyword::IF)?;

        let condition = parser.parse_expr()?;

        let if_block = if parser.peek_keyword(Keyword::BEGIN) {
            let begin_token = parser.expect_keyword(Keyword::BEGIN)?;
            let statements = self.parse_statement_list(parser, Some(Keyword::END))?;
            let end_token = parser.expect_keyword(Keyword::END)?;
            ConditionalStatementBlock {
                start_token: AttachedToken(if_token),
                condition: Some(condition),
                then_token: None,
                conditional_statements: ConditionalStatements::BeginEnd(BeginEndStatements {
                    begin_token: AttachedToken(begin_token),
                    statements,
                    end_token: AttachedToken(end_token),
                }),
            }
        } else {
            let stmt = parser.parse_statement()?;
            ConditionalStatementBlock {
                start_token: AttachedToken(if_token),
                condition: Some(condition),
                then_token: None,
                conditional_statements: ConditionalStatements::Sequence {
                    statements: vec![stmt],
                },
            }
        };

        while let Token::SemiColon = parser.peek_token_ref().token {
            parser.advance_token();
        }

        let mut else_block = None;
        if parser.peek_keyword(Keyword::ELSE) {
            let else_token = parser.expect_keyword(Keyword::ELSE)?;
            if parser.peek_keyword(Keyword::BEGIN) {
                let begin_token = parser.expect_keyword(Keyword::BEGIN)?;
                let statements = self.parse_statement_list(parser, Some(Keyword::END))?;
                let end_token = parser.expect_keyword(Keyword::END)?;
                else_block = Some(ConditionalStatementBlock {
                    start_token: AttachedToken(else_token),
                    condition: None,
                    then_token: None,
                    conditional_statements: ConditionalStatements::BeginEnd(BeginEndStatements {
                        begin_token: AttachedToken(begin_token),
                        statements,
                        end_token: AttachedToken(end_token),
                    }),
                });
            } else {
                let stmt = parser.parse_statement()?;
                else_block = Some(ConditionalStatementBlock {
                    start_token: AttachedToken(else_token),
                    condition: None,
                    then_token: None,
                    conditional_statements: ConditionalStatements::Sequence {
                        statements: vec![stmt],
                    },
                });
            }
        }

        Ok(Statement::If(IfStatement {
            if_block,
            else_block,
            elseif_blocks: Vec::new(),
            end_token: None,
        }))
    }

    /// Parse a sequence of statements, optionally separated by semicolon.
    ///
    /// Stops parsing when reaching EOF or the given keyword.
    fn parse_statement_list(
        &self,
        parser: &mut Parser,
        terminal_keyword: Option<Keyword>,
    ) -> Result<Vec<Statement>, ParserError> {
        let mut stmts = Vec::new();
        loop {
            if let Token::EOF = parser.peek_token_ref().token {
                break;
            }
            if let Some(term) = terminal_keyword {
                if parser.peek_keyword(term) {
                    break;
                }
            }
            stmts.push(parser.parse_statement()?);
            while let Token::SemiColon = parser.peek_token_ref().token {
                parser.advance_token();
            }
        }
        Ok(stmts)
    }
}

// Copyright 2018 Grove Enterprises LLC
//
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

//! SQL Parser

use log::debug;

use super::dialect::keywords;
use super::dialect::Dialect;
use super::sqlast::*;
use super::sqltokenizer::*;
use std::error::Error;

#[derive(Debug, Clone, PartialEq)]
pub enum ParserError {
    TokenizerError(String),
    ParserError(String),
}

// Use `Parser::expected` instead, if possible
macro_rules! parser_err {
    ($MSG:expr) => {
        Err(ParserError::ParserError($MSG.to_string()))
    };
}

#[derive(PartialEq)]
pub enum IsOptional {
    Optional,
    Mandatory,
}
use IsOptional::*;

impl From<TokenizerError> for ParserError {
    fn from(e: TokenizerError) -> Self {
        ParserError::TokenizerError(format!("{:?}", e))
    }
}

impl std::fmt::Display for ParserError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "sql parser error: {}",
            match self {
                ParserError::TokenizerError(s) => s,
                ParserError::ParserError(s) => s,
            }
        )
    }
}

impl Error for ParserError {}

/// SQL Parser
pub struct Parser {
    tokens: Vec<Token>,
    index: usize,
}

impl Parser {
    /// Parse the specified tokens
    pub fn new(tokens: Vec<Token>) -> Self {
        Parser { tokens, index: 0 }
    }

    /// Parse a SQL statement and produce an Abstract Syntax Tree (AST)
    pub fn parse_sql(dialect: &dyn Dialect, sql: String) -> Result<Vec<SQLStatement>, ParserError> {
        let mut tokenizer = Tokenizer::new(dialect, &sql);
        let tokens = tokenizer.tokenize()?;
        let mut parser = Parser::new(tokens);
        let mut stmts = Vec::new();
        let mut expecting_statement_delimiter = false;
        debug!("Parsing sql '{}'...", sql);
        loop {
            // ignore empty statements (between successive statement delimiters)
            while parser.consume_token(&Token::SemiColon) {
                expecting_statement_delimiter = false;
            }

            if parser.peek_token().is_none() {
                break;
            } else if expecting_statement_delimiter {
                return parser.expected("end of statement", parser.peek_token());
            }

            let statement = parser.parse_statement()?;
            stmts.push(statement);
            expecting_statement_delimiter = true;
        }
        Ok(stmts)
    }

    /// Parse a single top-level statement (such as SELECT, INSERT, CREATE, etc.),
    /// stopping before the statement separator, if any.
    pub fn parse_statement(&mut self) -> Result<SQLStatement, ParserError> {
        match self.next_token() {
            Some(t) => match t {
                Token::SQLWord(ref w) if w.keyword != "" => match w.keyword.as_ref() {
                    "SELECT" | "WITH" => {
                        self.prev_token();
                        Ok(SQLStatement::SQLQuery(Box::new(self.parse_query()?)))
                    }
                    "CREATE" => Ok(self.parse_create()?),
                    "DROP" => Ok(self.parse_drop()?),
                    "DELETE" => Ok(self.parse_delete()?),
                    "INSERT" => Ok(self.parse_insert()?),
                    "ALTER" => Ok(self.parse_alter()?),
                    "COPY" => Ok(self.parse_copy()?),
                    _ => parser_err!(format!(
                        "Unexpected keyword {:?} at the beginning of a statement",
                        w.to_string()
                    )),
                },
                unexpected => self.expected(
                    "a keyword at the beginning of a statement",
                    Some(unexpected),
                ),
            },
            None => self.expected("SQL statement", None),
        }
    }

    /// Parse a new expression
    pub fn parse_expr(&mut self) -> Result<ASTNode, ParserError> {
        self.parse_subexpr(0)
    }

    /// Parse tokens until the precedence changes
    pub fn parse_subexpr(&mut self, precedence: u8) -> Result<ASTNode, ParserError> {
        debug!("parsing expr");
        let mut expr = self.parse_prefix()?;
        debug!("prefix: {:?}", expr);
        loop {
            let next_precedence = self.get_next_precedence()?;
            debug!("next precedence: {:?}", next_precedence);
            if precedence >= next_precedence {
                break;
            }

            expr = self.parse_infix(expr, next_precedence)?;
        }
        Ok(expr)
    }

    /// Parse expression for DEFAULT clause in CREATE TABLE
    pub fn parse_default_expr(&mut self, precedence: u8) -> Result<ASTNode, ParserError> {
        debug!("parsing expr");
        let mut expr = self.parse_prefix()?;
        debug!("prefix: {:?}", expr);
        loop {
            // stop parsing on `NULL` | `NOT NULL`
            match self.peek_token() {
                Some(Token::SQLWord(ref k)) if k.keyword == "NOT" || k.keyword == "NULL" => break,
                _ => {}
            }

            let next_precedence = self.get_next_precedence()?;
            debug!("next precedence: {:?}", next_precedence);
            if precedence >= next_precedence {
                break;
            }

            expr = self.parse_infix(expr, next_precedence)?;
        }
        Ok(expr)
    }

    /// Parse an expression prefix
    pub fn parse_prefix(&mut self) -> Result<ASTNode, ParserError> {
        let tok = self
            .next_token()
            .ok_or_else(|| ParserError::ParserError("Unexpected EOF".to_string()))?;
        let expr = match tok {
            Token::SQLWord(w) => match w.keyword.as_ref() {
                "TRUE" | "FALSE" | "NULL" => {
                    self.prev_token();
                    self.parse_sql_value()
                }
                "CASE" => self.parse_case_expression(),
                "CAST" => self.parse_cast_expression(),
                "NOT" => {
                    let p = self.get_precedence(&Token::make_keyword("NOT"))?;
                    Ok(ASTNode::SQLUnary {
                        operator: SQLOperator::Not,
                        expr: Box::new(self.parse_subexpr(p)?),
                    })
                }
                // Here `w` is a word, check if it's a part of a multi-part
                // identifier, a function call, or a simple identifier:
                _ => match self.peek_token() {
                    Some(Token::LParen) | Some(Token::Period) => {
                        let mut id_parts: Vec<SQLIdent> = vec![w.as_sql_ident()];
                        let mut ends_with_wildcard = false;
                        while self.consume_token(&Token::Period) {
                            match self.next_token() {
                                Some(Token::SQLWord(w)) => id_parts.push(w.as_sql_ident()),
                                Some(Token::Mult) => {
                                    ends_with_wildcard = true;
                                    break;
                                }
                                unexpected => {
                                    return self
                                        .expected("an identifier or a '*' after '.'", unexpected);
                                }
                            }
                        }
                        if ends_with_wildcard {
                            Ok(ASTNode::SQLQualifiedWildcard(id_parts))
                        } else if self.consume_token(&Token::LParen) {
                            self.prev_token();
                            self.parse_function(SQLObjectName(id_parts))
                        } else {
                            Ok(ASTNode::SQLCompoundIdentifier(id_parts))
                        }
                    }
                    _ => Ok(ASTNode::SQLIdentifier(w.as_sql_ident())),
                },
            }, // End of Token::SQLWord
            Token::Mult => Ok(ASTNode::SQLWildcard),
            tok @ Token::Minus | tok @ Token::Plus => {
                let p = self.get_precedence(&tok)?;
                let operator = if tok == Token::Plus {
                    SQLOperator::Plus
                } else {
                    SQLOperator::Minus
                };
                Ok(ASTNode::SQLUnary {
                    operator,
                    expr: Box::new(self.parse_subexpr(p)?),
                })
            }
            Token::Number(_) | Token::SingleQuotedString(_) | Token::NationalStringLiteral(_) => {
                self.prev_token();
                self.parse_sql_value()
            }
            Token::LParen => {
                let expr = if self.parse_keyword("SELECT") || self.parse_keyword("WITH") {
                    self.prev_token();
                    ASTNode::SQLSubquery(Box::new(self.parse_query()?))
                } else {
                    ASTNode::SQLNested(Box::new(self.parse_expr()?))
                };
                self.expect_token(&Token::RParen)?;
                Ok(expr)
            }
            unexpected => self.expected("an expression", Some(unexpected)),
        }?;

        if self.parse_keyword("COLLATE") {
            Ok(ASTNode::SQLCollate {
                expr: Box::new(expr),
                collation: self.parse_object_name()?,
            })
        } else {
            Ok(expr)
        }
    }

    pub fn parse_function(&mut self, name: SQLObjectName) -> Result<ASTNode, ParserError> {
        self.expect_token(&Token::LParen)?;
        let all = self.parse_keyword("ALL");
        let distinct = self.parse_keyword("DISTINCT");
        if all && distinct {
            return parser_err!(format!(
                "Cannot specify both ALL and DISTINCT in function: {}",
                name.to_string(),
            ));
        }
        let args = self.parse_optional_args()?;
        let over = if self.parse_keyword("OVER") {
            // TBD: support window names (`OVER mywin`) in place of inline specification
            self.expect_token(&Token::LParen)?;
            let partition_by = if self.parse_keywords(vec!["PARTITION", "BY"]) {
                // a list of possibly-qualified column names
                self.parse_expr_list()?
            } else {
                vec![]
            };
            let order_by = if self.parse_keywords(vec!["ORDER", "BY"]) {
                self.parse_order_by_expr_list()?
            } else {
                vec![]
            };
            let window_frame = self.parse_window_frame()?;

            Some(SQLWindowSpec {
                partition_by,
                order_by,
                window_frame,
            })
        } else {
            None
        };

        Ok(ASTNode::SQLFunction {
            name,
            args,
            over,
            distinct,
        })
    }

    pub fn parse_window_frame(&mut self) -> Result<Option<SQLWindowFrame>, ParserError> {
        let window_frame = match self.peek_token() {
            Some(Token::SQLWord(w)) => {
                let units = w.keyword.parse::<SQLWindowFrameUnits>()?;
                self.next_token();
                if self.parse_keyword("BETWEEN") {
                    let start_bound = self.parse_window_frame_bound()?;
                    self.expect_keyword("AND")?;
                    let end_bound = Some(self.parse_window_frame_bound()?);
                    Some(SQLWindowFrame {
                        units,
                        start_bound,
                        end_bound,
                    })
                } else {
                    let start_bound = self.parse_window_frame_bound()?;
                    let end_bound = None;
                    Some(SQLWindowFrame {
                        units,
                        start_bound,
                        end_bound,
                    })
                }
            }
            Some(Token::RParen) => None,
            unexpected => return self.expected("'ROWS', 'RANGE', 'GROUPS', or ')'", unexpected),
        };
        self.expect_token(&Token::RParen)?;
        Ok(window_frame)
    }

    /// "CURRENT ROW" | ( (<positive number> | "UNBOUNDED") ("PRECEDING" | FOLLOWING) )
    pub fn parse_window_frame_bound(&mut self) -> Result<SQLWindowFrameBound, ParserError> {
        if self.parse_keywords(vec!["CURRENT", "ROW"]) {
            Ok(SQLWindowFrameBound::CurrentRow)
        } else {
            let rows = if self.parse_keyword("UNBOUNDED") {
                None
            } else {
                let rows = self.parse_literal_int()?;
                if rows < 0 {
                    parser_err!(format!(
                        "The number of rows must be non-negative, got {}",
                        rows
                    ))?;
                }
                Some(rows as u64)
            };
            if self.parse_keyword("PRECEDING") {
                Ok(SQLWindowFrameBound::Preceding(rows))
            } else if self.parse_keyword("FOLLOWING") {
                Ok(SQLWindowFrameBound::Following(rows))
            } else {
                self.expected("PRECEDING or FOLLOWING", self.peek_token())
            }
        }
    }

    pub fn parse_case_expression(&mut self) -> Result<ASTNode, ParserError> {
        let mut operand = None;
        if !self.parse_keyword("WHEN") {
            operand = Some(Box::new(self.parse_expr()?));
            self.expect_keyword("WHEN")?;
        }
        let mut conditions = vec![];
        let mut results = vec![];
        loop {
            conditions.push(self.parse_expr()?);
            self.expect_keyword("THEN")?;
            results.push(self.parse_expr()?);
            if !self.parse_keyword("WHEN") {
                break;
            }
        }
        let else_result = if self.parse_keyword("ELSE") {
            Some(Box::new(self.parse_expr()?))
        } else {
            None
        };
        self.expect_keyword("END")?;
        Ok(ASTNode::SQLCase {
            operand,
            conditions,
            results,
            else_result,
        })
    }

    /// Parse a SQL CAST function e.g. `CAST(expr AS FLOAT)`
    pub fn parse_cast_expression(&mut self) -> Result<ASTNode, ParserError> {
        self.expect_token(&Token::LParen)?;
        let expr = self.parse_expr()?;
        self.expect_keyword("AS")?;
        let data_type = self.parse_data_type()?;
        self.expect_token(&Token::RParen)?;
        Ok(ASTNode::SQLCast {
            expr: Box::new(expr),
            data_type,
        })
    }

    /// Parse an operator following an expression
    pub fn parse_infix(&mut self, expr: ASTNode, precedence: u8) -> Result<ASTNode, ParserError> {
        debug!("parsing infix");
        let tok = self.next_token().unwrap(); // safe as EOF's precedence is the lowest

        let regular_binary_operator = match tok {
            Token::Eq => Some(SQLOperator::Eq),
            Token::Neq => Some(SQLOperator::NotEq),
            Token::Gt => Some(SQLOperator::Gt),
            Token::GtEq => Some(SQLOperator::GtEq),
            Token::Lt => Some(SQLOperator::Lt),
            Token::LtEq => Some(SQLOperator::LtEq),
            Token::Plus => Some(SQLOperator::Plus),
            Token::Minus => Some(SQLOperator::Minus),
            Token::Mult => Some(SQLOperator::Multiply),
            Token::Mod => Some(SQLOperator::Modulus),
            Token::Div => Some(SQLOperator::Divide),
            Token::SQLWord(ref k) => match k.keyword.as_ref() {
                "AND" => Some(SQLOperator::And),
                "OR" => Some(SQLOperator::Or),
                "LIKE" => Some(SQLOperator::Like),
                "NOT" => {
                    if self.parse_keyword("LIKE") {
                        Some(SQLOperator::NotLike)
                    } else {
                        None
                    }
                }
                _ => None,
            },
            _ => None,
        };

        if let Some(op) = regular_binary_operator {
            Ok(ASTNode::SQLBinaryExpr {
                left: Box::new(expr),
                op,
                right: Box::new(self.parse_subexpr(precedence)?),
            })
        } else if let Token::SQLWord(ref k) = tok {
            match k.keyword.as_ref() {
                "IS" => {
                    if self.parse_keyword("NULL") {
                        Ok(ASTNode::SQLIsNull(Box::new(expr)))
                    } else if self.parse_keywords(vec!["NOT", "NULL"]) {
                        Ok(ASTNode::SQLIsNotNull(Box::new(expr)))
                    } else {
                        self.expected("NULL or NOT NULL after IS", self.peek_token())
                    }
                }
                "NOT" | "IN" | "BETWEEN" => {
                    self.prev_token();
                    let negated = self.parse_keyword("NOT");
                    if self.parse_keyword("IN") {
                        self.parse_in(expr, negated)
                    } else if self.parse_keyword("BETWEEN") {
                        self.parse_between(expr, negated)
                    } else {
                        self.expected("IN or BETWEEN after NOT", self.peek_token())
                    }
                }
                // Can only happen if `get_precedence` got out of sync with this function
                _ => panic!("No infix parser for token {:?}", tok),
            }
        } else if Token::DoubleColon == tok {
            self.parse_pg_cast(expr)
        } else {
            // Can only happen if `get_precedence` got out of sync with this function
            panic!("No infix parser for token {:?}", tok)
        }
    }

    /// Parses the parens following the `[ NOT ] IN` operator
    pub fn parse_in(&mut self, expr: ASTNode, negated: bool) -> Result<ASTNode, ParserError> {
        self.expect_token(&Token::LParen)?;
        let in_op = if self.parse_keyword("SELECT") || self.parse_keyword("WITH") {
            self.prev_token();
            ASTNode::SQLInSubquery {
                expr: Box::new(expr),
                subquery: Box::new(self.parse_query()?),
                negated,
            }
        } else {
            ASTNode::SQLInList {
                expr: Box::new(expr),
                list: self.parse_expr_list()?,
                negated,
            }
        };
        self.expect_token(&Token::RParen)?;
        Ok(in_op)
    }

    /// Parses `BETWEEN <low> AND <high>`, assuming the `BETWEEN` keyword was already consumed
    pub fn parse_between(&mut self, expr: ASTNode, negated: bool) -> Result<ASTNode, ParserError> {
        // Stop parsing subexpressions for <low> and <high> on tokens with
        // precedence lower than that of `BETWEEN`, such as `AND`, `IS`, etc.
        let prec = self.get_precedence(&Token::make_keyword("BETWEEN"))?;
        let low = self.parse_subexpr(prec)?;
        self.expect_keyword("AND")?;
        let high = self.parse_subexpr(prec)?;
        Ok(ASTNode::SQLBetween {
            expr: Box::new(expr),
            negated,
            low: Box::new(low),
            high: Box::new(high),
        })
    }

    /// Parse a postgresql casting style which is in the form of `expr::datatype`
    pub fn parse_pg_cast(&mut self, expr: ASTNode) -> Result<ASTNode, ParserError> {
        Ok(ASTNode::SQLCast {
            expr: Box::new(expr),
            data_type: self.parse_data_type()?,
        })
    }

    /// Get the precedence of the next token
    pub fn get_next_precedence(&self) -> Result<u8, ParserError> {
        if let Some(token) = self.peek_token() {
            self.get_precedence(&token)
        } else {
            Ok(0)
        }
    }

    /// Get the precedence of a token
    pub fn get_precedence(&self, tok: &Token) -> Result<u8, ParserError> {
        debug!("get_precedence() {:?}", tok);

        match tok {
            Token::SQLWord(k) if k.keyword == "OR" => Ok(5),
            Token::SQLWord(k) if k.keyword == "AND" => Ok(10),
            Token::SQLWord(k) if k.keyword == "NOT" => Ok(15),
            Token::SQLWord(k) if k.keyword == "IS" => Ok(17),
            Token::SQLWord(k) if k.keyword == "IN" => Ok(20),
            Token::SQLWord(k) if k.keyword == "BETWEEN" => Ok(20),
            Token::SQLWord(k) if k.keyword == "LIKE" => Ok(20),
            Token::Eq | Token::Lt | Token::LtEq | Token::Neq | Token::Gt | Token::GtEq => Ok(20),
            Token::Plus | Token::Minus => Ok(30),
            Token::Mult | Token::Div | Token::Mod => Ok(40),
            Token::DoubleColon => Ok(50),
            _ => Ok(0),
        }
    }

    /// Return first non-whitespace token that has not yet been processed
    pub fn peek_token(&self) -> Option<Token> {
        if let Some(n) = self.til_non_whitespace() {
            self.token_at(n)
        } else {
            None
        }
    }

    /// Get the next token skipping whitespace and increment the token index
    pub fn next_token(&mut self) -> Option<Token> {
        loop {
            match self.next_token_no_skip() {
                Some(Token::Whitespace(_)) => {
                    continue;
                }
                token => {
                    return token;
                }
            }
        }
    }

    /// get the index for non whitepsace token
    fn til_non_whitespace(&self) -> Option<usize> {
        let mut index = self.index;
        loop {
            match self.token_at(index) {
                Some(Token::Whitespace(_)) => {
                    index += 1;
                }
                Some(_) => {
                    return Some(index);
                }
                None => {
                    return None;
                }
            }
        }
    }

    /// see the token at this index
    fn token_at(&self, n: usize) -> Option<Token> {
        if let Some(token) = self.tokens.get(n) {
            Some(token.clone())
        } else {
            None
        }
    }

    pub fn next_token_no_skip(&mut self) -> Option<Token> {
        if self.index < self.tokens.len() {
            self.index += 1;
            Some(self.tokens[self.index - 1].clone())
        } else {
            None
        }
    }

    /// Push back the last one non-whitespace token
    pub fn prev_token(&mut self) -> Option<Token> {
        // TODO: returned value is unused (available via peek_token)
        loop {
            match self.prev_token_no_skip() {
                Some(Token::Whitespace(_)) => {
                    continue;
                }
                token => {
                    return token;
                }
            }
        }
    }

    /// Get the previous token and decrement the token index
    fn prev_token_no_skip(&mut self) -> Option<Token> {
        if self.index > 0 {
            self.index -= 1;
            Some(self.tokens[self.index].clone())
        } else {
            None
        }
    }

    /// Report unexpected token
    fn expected<T>(&self, expected: &str, found: Option<Token>) -> Result<T, ParserError> {
        parser_err!(format!(
            "Expected {}, found: {}",
            expected,
            found.map_or("EOF".to_string(), |t| t.to_string())
        ))
    }

    /// Look for an expected keyword and consume it if it exists
    #[must_use]
    pub fn parse_keyword(&mut self, expected: &'static str) -> bool {
        // Ideally, we'd accept a enum variant, not a string, but since
        // it's not trivial to maintain the enum without duplicating all
        // the keywords three times, we'll settle for a run-time check that
        // the string actually represents a known keyword...
        assert!(keywords::ALL_KEYWORDS.contains(&expected));
        match self.peek_token() {
            Some(Token::SQLWord(ref k)) if expected.eq_ignore_ascii_case(&k.keyword) => {
                self.next_token();
                true
            }
            _ => false,
        }
    }

    /// Look for an expected sequence of keywords and consume them if they exist
    #[must_use]
    pub fn parse_keywords(&mut self, keywords: Vec<&'static str>) -> bool {
        let index = self.index;
        for keyword in keywords {
            if !self.parse_keyword(&keyword) {
                //println!("parse_keywords aborting .. did not find {}", keyword);
                // reset index and return immediately
                self.index = index;
                return false;
            }
        }
        true
    }

    /// Look for one of the given keywords and return the one that matches.
    #[must_use]
    pub fn parse_one_of_keywords(&mut self, keywords: &[&'static str]) -> Option<&'static str> {
        for keyword in keywords {
            assert!(keywords::ALL_KEYWORDS.contains(keyword));
        }
        match self.peek_token() {
            Some(Token::SQLWord(ref k)) => keywords
                .iter()
                .find(|keyword| keyword.eq_ignore_ascii_case(&k.keyword))
                .map(|keyword| {
                    self.next_token();
                    *keyword
                }),
            _ => None,
        }
    }

    /// Bail out if the current token is not one of the expected keywords, or consume it if it is
    #[must_use]
    pub fn expect_one_of_keywords(
        &mut self,
        keywords: &[&'static str],
    ) -> Result<&'static str, ParserError> {
        if let Some(keyword) = self.parse_one_of_keywords(keywords) {
            Ok(keyword)
        } else {
            self.expected(
                &format!("one of {}", keywords.join(" or ")),
                self.peek_token(),
            )
        }
    }

    /// Bail out if the current token is not an expected keyword, or consume it if it is
    pub fn expect_keyword(&mut self, expected: &'static str) -> Result<(), ParserError> {
        if self.parse_keyword(expected) {
            Ok(())
        } else {
            self.expected(expected, self.peek_token())
        }
    }

    /// Consume the next token if it matches the expected token, otherwise return false
    #[must_use]
    pub fn consume_token(&mut self, expected: &Token) -> bool {
        match self.peek_token() {
            Some(ref t) => {
                if *t == *expected {
                    self.next_token();
                    true
                } else {
                    false
                }
            }
            _ => false,
        }
    }

    /// Bail out if the current token is not an expected keyword, or consume it if it is
    pub fn expect_token(&mut self, expected: &Token) -> Result<(), ParserError> {
        if self.consume_token(expected) {
            Ok(())
        } else {
            self.expected(&expected.to_string(), self.peek_token())
        }
    }

    /// Parse a SQL CREATE statement
    pub fn parse_create(&mut self) -> Result<SQLStatement, ParserError> {
        if self.parse_keyword("TABLE") {
            self.parse_create_table()
        } else if self.parse_keyword("MATERIALIZED") || self.parse_keyword("VIEW") {
            self.prev_token();
            self.parse_create_view()
        } else if self.parse_keyword("EXTERNAL") {
            self.parse_create_external_table()
        } else {
            self.expected("TABLE or VIEW after CREATE", self.peek_token())
        }
    }

    pub fn parse_create_external_table(&mut self) -> Result<SQLStatement, ParserError> {
        self.expect_keyword("TABLE")?;
        let table_name = self.parse_object_name()?;
        let (columns, constraints) = self.parse_columns()?;
        self.expect_keyword("STORED")?;
        self.expect_keyword("AS")?;
        let file_format = self.parse_identifier()?.parse::<FileFormat>()?;

        self.expect_keyword("LOCATION")?;
        let location = self.parse_literal_string()?;

        Ok(SQLStatement::SQLCreateTable {
            name: table_name,
            columns,
            constraints,
            external: true,
            file_format: Some(file_format),
            location: Some(location),
        })
    }

    pub fn parse_create_view(&mut self) -> Result<SQLStatement, ParserError> {
        let materialized = self.parse_keyword("MATERIALIZED");
        self.expect_keyword("VIEW")?;
        // Many dialects support `OR REPLACE` | `OR ALTER` right after `CREATE`, but we don't (yet).
        // ANSI SQL and Postgres support RECURSIVE here, but we don't support it either.
        let name = self.parse_object_name()?;
        // Parenthesized "output" columns list could be handled here.
        // Some dialects allow WITH here, followed by some keywords (e.g. MS SQL)
        // or `(k1=v1, k2=v2, ...)` (Postgres)
        self.expect_keyword("AS")?;
        let query = Box::new(self.parse_query()?);
        // Optional `WITH [ CASCADED | LOCAL ] CHECK OPTION` is widely supported here.
        Ok(SQLStatement::SQLCreateView {
            name,
            query,
            materialized,
        })
    }

    pub fn parse_drop(&mut self) -> Result<SQLStatement, ParserError> {
        let object_type = if self.parse_keyword("TABLE") {
            SQLObjectType::Table
        } else if self.parse_keyword("VIEW") {
            SQLObjectType::View
        } else {
            return parser_err!(format!(
                "Unexpected token after DROP: {:?}",
                self.peek_token()
            ));
        };
        let if_exists = self.parse_keywords(vec!["IF", "EXISTS"]);
        let mut names = vec![self.parse_object_name()?];
        loop {
            let token = &self.next_token();
            if let Some(Token::Comma) = token {
                names.push(self.parse_object_name()?)
            } else {
                if token.is_some() {
                    self.prev_token();
                }
                break;
            }
        }
        let cascade = self.parse_keyword("CASCADE");
        let restrict = self.parse_keyword("RESTRICT");
        if cascade && restrict {
            return parser_err!("Cannot specify both CASCADE and RESTRICT in DROP");
        }
        Ok(SQLStatement::SQLDrop {
            object_type,
            if_exists,
            names,
            cascade,
        })
    }

    pub fn parse_create_table(&mut self) -> Result<SQLStatement, ParserError> {
        let table_name = self.parse_object_name()?;
        // parse optional column list (schema)
        let (columns, constraints) = self.parse_columns()?;

        Ok(SQLStatement::SQLCreateTable {
            name: table_name,
            columns,
            constraints,
            external: false,
            file_format: None,
            location: None,
        })
    }

    fn parse_columns(&mut self) -> Result<(Vec<SQLColumnDef>, Vec<TableConstraint>), ParserError> {
        let mut columns = vec![];
        let mut constraints = vec![];
        if !self.consume_token(&Token::LParen) {
            return Ok((columns, constraints));
        }

        loop {
            if let Some(constraint) = self.parse_optional_table_constraint()? {
                constraints.push(constraint);
            } else if let Some(Token::SQLWord(column_name)) = self.peek_token() {
                self.next_token();
                let data_type = self.parse_data_type()?;
                let is_primary = self.parse_keywords(vec!["PRIMARY", "KEY"]);
                let is_unique = self.parse_keyword("UNIQUE");
                let default = if self.parse_keyword("DEFAULT") {
                    let expr = self.parse_default_expr(0)?;
                    Some(expr)
                } else {
                    None
                };
                let allow_null = if self.parse_keywords(vec!["NOT", "NULL"]) {
                    false
                } else {
                    let _ = self.parse_keyword("NULL");
                    true
                };
                debug!("default: {:?}", default);

                columns.push(SQLColumnDef {
                    name: column_name.as_sql_ident(),
                    data_type,
                    allow_null,
                    is_primary,
                    is_unique,
                    default,
                });
            } else {
                return self.expected("column name or constraint definition", self.peek_token());
            }
            let comma = self.consume_token(&Token::Comma);
            if self.consume_token(&Token::RParen) {
                // allow a trailing comma, even though it's not in standard
                break;
            } else if !comma {
                return self.expected("',' or ')' after column definition", self.peek_token());
            }
        }

        Ok((columns, constraints))
    }

    pub fn parse_optional_table_constraint(
        &mut self,
    ) -> Result<Option<TableConstraint>, ParserError> {
        let name = if self.parse_keyword("CONSTRAINT") {
            Some(self.parse_identifier()?)
        } else {
            None
        };
        match self.next_token() {
            Some(Token::SQLWord(ref k)) if k.keyword == "PRIMARY" || k.keyword == "UNIQUE" => {
                let is_primary = k.keyword == "PRIMARY";
                if is_primary {
                    self.expect_keyword("KEY")?;
                }
                let columns = self.parse_parenthesized_column_list(Mandatory)?;
                Ok(Some(TableConstraint::Unique {
                    name,
                    columns,
                    is_primary,
                }))
            }
            Some(Token::SQLWord(ref k)) if k.keyword == "FOREIGN" => {
                self.expect_keyword("KEY")?;
                let columns = self.parse_parenthesized_column_list(Mandatory)?;
                self.expect_keyword("REFERENCES")?;
                let foreign_table = self.parse_object_name()?;
                let referred_columns = self.parse_parenthesized_column_list(Mandatory)?;
                Ok(Some(TableConstraint::ForeignKey {
                    name,
                    columns,
                    foreign_table,
                    referred_columns,
                }))
            }
            Some(Token::SQLWord(ref k)) if k.keyword == "CHECK" => {
                self.expect_token(&Token::LParen)?;
                let expr = Box::new(self.parse_expr()?);
                self.expect_token(&Token::RParen)?;
                Ok(Some(TableConstraint::Check { name, expr }))
            }
            unexpected => {
                if name.is_some() {
                    self.expected("PRIMARY, UNIQUE, FOREIGN, or CHECK", unexpected)
                } else {
                    if unexpected.is_some() {
                        self.prev_token();
                    }
                    Ok(None)
                }
            }
        }
    }

    pub fn parse_alter(&mut self) -> Result<SQLStatement, ParserError> {
        self.expect_keyword("TABLE")?;
        let _ = self.parse_keyword("ONLY");
        let table_name = self.parse_object_name()?;
        let operation = if self.parse_keyword("ADD") {
            if let Some(constraint) = self.parse_optional_table_constraint()? {
                AlterTableOperation::AddConstraint(constraint)
            } else {
                return self.expected("a constraint in ALTER TABLE .. ADD", self.peek_token());
            }
        } else {
            return self.expected("ADD after ALTER TABLE", self.peek_token());
        };
        Ok(SQLStatement::SQLAlterTable {
            name: table_name,
            operation,
        })
    }

    /// Parse a copy statement
    pub fn parse_copy(&mut self) -> Result<SQLStatement, ParserError> {
        let table_name = self.parse_object_name()?;
        let columns = self.parse_parenthesized_column_list(Optional)?;
        self.expect_keyword("FROM")?;
        self.expect_keyword("STDIN")?;
        self.expect_token(&Token::SemiColon)?;
        let values = self.parse_tsv()?;
        Ok(SQLStatement::SQLCopy {
            table_name,
            columns,
            values,
        })
    }

    /// Parse a tab separated values in
    /// COPY payload
    fn parse_tsv(&mut self) -> Result<Vec<Option<String>>, ParserError> {
        let values = self.parse_tab_value()?;
        Ok(values)
    }

    fn parse_sql_value(&mut self) -> Result<ASTNode, ParserError> {
        Ok(ASTNode::SQLValue(self.parse_value()?))
    }

    fn parse_tab_value(&mut self) -> Result<Vec<Option<String>>, ParserError> {
        let mut values = vec![];
        let mut content = String::from("");
        while let Some(t) = self.next_token_no_skip() {
            match t {
                Token::Whitespace(Whitespace::Tab) => {
                    values.push(Some(content.to_string()));
                    content.clear();
                }
                Token::Whitespace(Whitespace::Newline) => {
                    values.push(Some(content.to_string()));
                    content.clear();
                }
                Token::Backslash => {
                    if self.consume_token(&Token::Period) {
                        return Ok(values);
                    }
                    if let Some(token) = self.next_token() {
                        if let Token::SQLWord(SQLWord { value: v, .. }) = token {
                            if v == "N" {
                                values.push(None);
                            }
                        }
                    } else {
                        continue;
                    }
                }
                _ => {
                    content.push_str(&t.to_string());
                }
            }
        }
        Ok(values)
    }

    /// Parse a literal value (numbers, strings, date/time, booleans)
    fn parse_value(&mut self) -> Result<Value, ParserError> {
        match self.next_token() {
            Some(t) => match t {
                Token::SQLWord(k) => match k.keyword.as_ref() {
                    "TRUE" => Ok(Value::Boolean(true)),
                    "FALSE" => Ok(Value::Boolean(false)),
                    "NULL" => Ok(Value::Null),
                    _ => {
                        return parser_err!(format!("No value parser for keyword {}", k.keyword));
                    }
                },
                Token::Number(ref n) if n.contains('.') => match n.parse::<f64>() {
                    Ok(n) => Ok(Value::Double(n)),
                    Err(e) => parser_err!(format!("Could not parse '{}' as f64: {}", n, e)),
                },
                Token::Number(ref n) => match n.parse::<i64>() {
                    Ok(n) => Ok(Value::Long(n)),
                    Err(e) => parser_err!(format!("Could not parse '{}' as i64: {}", n, e)),
                },
                Token::SingleQuotedString(ref s) => Ok(Value::SingleQuotedString(s.to_string())),
                Token::NationalStringLiteral(ref s) => {
                    Ok(Value::NationalStringLiteral(s.to_string()))
                }
                _ => parser_err!(format!("Unsupported value: {:?}", t)),
            },
            None => parser_err!("Expecting a value, but found EOF"),
        }
    }

    /// Parse a literal integer/long
    pub fn parse_literal_int(&mut self) -> Result<i64, ParserError> {
        match self.next_token() {
            Some(Token::Number(s)) => s.parse::<i64>().map_err(|e| {
                ParserError::ParserError(format!("Could not parse '{}' as i64: {}", s, e))
            }),
            other => parser_err!(format!("Expected literal int, found {:?}", other)),
        }
    }

    /// Parse a literal double
    pub fn parse_literal_double(&mut self) -> Result<f64, ParserError> {
        match self.next_token() {
            Some(Token::Number(s)) => s.parse::<f64>().map_err(|e| {
                ParserError::ParserError(format!("Could not parse '{}' as f64: {}", s, e))
            }),
            other => parser_err!(format!("Expected literal number, found {:?}", other)),
        }
    }

    /// Parse a literal string
    pub fn parse_literal_string(&mut self) -> Result<String, ParserError> {
        match self.next_token() {
            Some(Token::SingleQuotedString(ref s)) => Ok(s.clone()),
            other => parser_err!(format!("Expected literal string, found {:?}", other)),
        }
    }

    /// Parse a SQL datatype (in the context of a CREATE TABLE statement for example)
    pub fn parse_data_type(&mut self) -> Result<SQLType, ParserError> {
        match self.next_token() {
            Some(Token::SQLWord(k)) => match k.keyword.as_ref() {
                "BOOLEAN" => Ok(SQLType::Boolean),
                "FLOAT" => Ok(SQLType::Float(self.parse_optional_precision()?)),
                "REAL" => Ok(SQLType::Real),
                "DOUBLE" => {
                    let _ = self.parse_keyword("PRECISION");
                    Ok(SQLType::Double)
                }
                "SMALLINT" => Ok(SQLType::SmallInt),
                "INT" | "INTEGER" => Ok(SQLType::Int),
                "BIGINT" => Ok(SQLType::BigInt),
                "VARCHAR" => Ok(SQLType::Varchar(self.parse_optional_precision()?)),
                "CHAR" | "CHARACTER" => {
                    if self.parse_keyword("VARYING") {
                        Ok(SQLType::Varchar(self.parse_optional_precision()?))
                    } else {
                        Ok(SQLType::Char(self.parse_optional_precision()?))
                    }
                }
                "UUID" => Ok(SQLType::Uuid),
                "DATE" => Ok(SQLType::Date),
                "TIMESTAMP" => {
                    // TBD: we throw away "with/without timezone" information
                    if self.parse_keyword("WITH") || self.parse_keyword("WITHOUT") {
                        self.expect_keyword("TIME")?;
                        self.expect_keyword("ZONE")?;
                    }
                    Ok(SQLType::Timestamp)
                }
                "TIME" => {
                    // TBD: we throw away "with/without timezone" information
                    if self.parse_keyword("WITH") || self.parse_keyword("WITHOUT") {
                        self.expect_keyword("TIME")?;
                        self.expect_keyword("ZONE")?;
                    }
                    Ok(SQLType::Time)
                }
                "REGCLASS" => Ok(SQLType::Regclass),
                "TEXT" => {
                    if self.consume_token(&Token::LBracket) {
                        // Note: this is postgresql-specific
                        self.expect_token(&Token::RBracket)?;
                        Ok(SQLType::Array(Box::new(SQLType::Text)))
                    } else {
                        Ok(SQLType::Text)
                    }
                }
                "BYTEA" => Ok(SQLType::Bytea),
                "NUMERIC" => {
                    let (precision, scale) = self.parse_optional_precision_scale()?;
                    Ok(SQLType::Decimal(precision, scale))
                }
                _ => {
                    self.prev_token();
                    let type_name = self.parse_object_name()?;
                    Ok(SQLType::Custom(type_name))
                }
            },
            other => self.expected("a data type name", other),
        }
    }

    /// Parse `AS identifier` (or simply `identifier` if it's not a reserved keyword)
    /// Some examples with aliases: `SELECT 1 foo`, `SELECT COUNT(*) AS cnt`,
    /// `SELECT ... FROM t1 foo, t2 bar`, `SELECT ... FROM (...) AS bar`
    pub fn parse_optional_alias(
        &mut self,
        reserved_kwds: &[&str],
    ) -> Result<Option<SQLIdent>, ParserError> {
        let after_as = self.parse_keyword("AS");
        let maybe_alias = self.next_token();
        match maybe_alias {
            // Accept any identifier after `AS` (though many dialects have restrictions on
            // keywords that may appear here). If there's no `AS`: don't parse keywords,
            // which may start a construct allowed in this position, to be parsed as aliases.
            // (For example, in `FROM t1 JOIN` the `JOIN` will always be parsed as a keyword,
            // not an alias.)
            Some(Token::SQLWord(ref w))
                if after_as || !reserved_kwds.contains(&w.keyword.as_str()) =>
            {
                Ok(Some(w.as_sql_ident()))
            }
            ref not_an_ident if after_as => parser_err!(format!(
                "Expected an identifier after AS, got {:?}",
                not_an_ident
            )),
            Some(_not_an_ident) => {
                self.prev_token();
                Ok(None) // no alias found
            }
            None => Ok(None),
        }
    }

    /// Parse one or more identifiers with the specified separator between them
    pub fn parse_list_of_ids(&mut self, separator: &Token) -> Result<Vec<SQLIdent>, ParserError> {
        let mut idents = vec![];
        let mut expect_identifier = true;
        loop {
            let token = &self.next_token();
            match token {
                Some(Token::SQLWord(s)) if expect_identifier => {
                    expect_identifier = false;
                    idents.push(s.as_sql_ident());
                }
                Some(token) if token == separator && !expect_identifier => {
                    expect_identifier = true;
                    continue;
                }
                _ => {
                    if token.is_some() {
                        self.prev_token();
                    }
                    break;
                }
            }
        }
        if expect_identifier {
            self.expected("identifier", self.peek_token())
        } else {
            Ok(idents)
        }
    }

    /// Parse a possibly qualified, possibly quoted identifier, e.g.
    /// `foo` or `myschema."table"`
    pub fn parse_object_name(&mut self) -> Result<SQLObjectName, ParserError> {
        Ok(SQLObjectName(self.parse_list_of_ids(&Token::Period)?))
    }

    /// Parse a simple one-word identifier (possibly quoted, possibly a keyword)
    pub fn parse_identifier(&mut self) -> Result<SQLIdent, ParserError> {
        match self.next_token() {
            Some(Token::SQLWord(w)) => Ok(w.as_sql_ident()),
            unexpected => self.expected("identifier", unexpected),
        }
    }

    /// Parse a parenthesized comma-separated list of unqualified, possibly quoted identifiers
    pub fn parse_parenthesized_column_list(
        &mut self,
        optional: IsOptional,
    ) -> Result<Vec<SQLIdent>, ParserError> {
        if self.consume_token(&Token::LParen) {
            let cols = self.parse_list_of_ids(&Token::Comma)?;
            self.expect_token(&Token::RParen)?;
            Ok(cols)
        } else if optional == Optional {
            Ok(vec![])
        } else {
            self.expected("a list of columns in parentheses", self.peek_token())
        }
    }

    pub fn parse_precision(&mut self) -> Result<usize, ParserError> {
        //TODO: error handling
        Ok(self.parse_optional_precision()?.unwrap())
    }

    pub fn parse_optional_precision(&mut self) -> Result<Option<usize>, ParserError> {
        if self.consume_token(&Token::LParen) {
            let n = self.parse_literal_int()?;
            //TODO: check return value of reading rparen
            self.expect_token(&Token::RParen)?;
            Ok(Some(n as usize))
        } else {
            Ok(None)
        }
    }

    pub fn parse_optional_precision_scale(
        &mut self,
    ) -> Result<(Option<usize>, Option<usize>), ParserError> {
        if self.consume_token(&Token::LParen) {
            let n = self.parse_literal_int()?;
            let scale = if self.consume_token(&Token::Comma) {
                Some(self.parse_literal_int()? as usize)
            } else {
                None
            };
            self.expect_token(&Token::RParen)?;
            Ok((Some(n as usize), scale))
        } else {
            Ok((None, None))
        }
    }

    pub fn parse_delete(&mut self) -> Result<SQLStatement, ParserError> {
        self.expect_keyword("FROM")?;
        let table_name = self.parse_object_name()?;
        let selection = if self.parse_keyword("WHERE") {
            Some(self.parse_expr()?)
        } else {
            None
        };

        Ok(SQLStatement::SQLDelete {
            table_name,
            selection,
        })
    }

    /// Parse a query expression, i.e. a `SELECT` statement optionally
    /// preceeded with some `WITH` CTE declarations and optionally followed
    /// by `ORDER BY`. Unlike some other parse_... methods, this one doesn't
    /// expect the initial keyword to be already consumed
    pub fn parse_query(&mut self) -> Result<SQLQuery, ParserError> {
        let ctes = if self.parse_keyword("WITH") {
            // TODO: optional RECURSIVE
            self.parse_cte_list()?
        } else {
            vec![]
        };

        let body = self.parse_query_body(0)?;

        let order_by = if self.parse_keywords(vec!["ORDER", "BY"]) {
            self.parse_order_by_expr_list()?
        } else {
            vec![]
        };

        let limit = if self.parse_keyword("LIMIT") {
            self.parse_limit()?
        } else {
            None
        };

        let offset = if self.parse_keyword("OFFSET") {
            Some(self.parse_offset()?)
        } else {
            None
        };

        let fetch = if self.parse_keyword("FETCH") {
            Some(self.parse_fetch()?)
        } else {
            None
        };

        Ok(SQLQuery {
            ctes,
            body,
            limit,
            order_by,
            offset,
            fetch,
        })
    }

    /// Parse one or more (comma-separated) `alias AS (subquery)` CTEs,
    /// assuming the initial `WITH` was already consumed.
    fn parse_cte_list(&mut self) -> Result<Vec<Cte>, ParserError> {
        let mut cte = vec![];
        loop {
            let alias = self.parse_identifier()?;
            let renamed_columns = self.parse_parenthesized_column_list(Optional)?;
            self.expect_keyword("AS")?;
            self.expect_token(&Token::LParen)?;
            cte.push(Cte {
                alias,
                query: self.parse_query()?,
                renamed_columns,
            });
            self.expect_token(&Token::RParen)?;
            if !self.consume_token(&Token::Comma) {
                break;
            }
        }
        Ok(cte)
    }

    /// Parse a "query body", which is an expression with roughly the
    /// following grammar:
    /// ```text
    ///   query_body ::= restricted_select | '(' subquery ')' | set_operation
    ///   restricted_select ::= 'SELECT' [expr_list] [ from ] [ where ] [ groupby_having ]
    ///   subquery ::= query_body [ order_by_limit ]
    ///   set_operation ::= query_body { 'UNION' | 'EXCEPT' | 'INTERSECT' } [ 'ALL' ] query_body
    /// ```
    fn parse_query_body(&mut self, precedence: u8) -> Result<SQLSetExpr, ParserError> {
        // We parse the expression using a Pratt parser, as in `parse_expr()`.
        // Start by parsing a restricted SELECT or a `(subquery)`:
        let mut expr = if self.parse_keyword("SELECT") {
            SQLSetExpr::Select(Box::new(self.parse_select()?))
        } else if self.consume_token(&Token::LParen) {
            // CTEs are not allowed here, but the parser currently accepts them
            let subquery = self.parse_query()?;
            self.expect_token(&Token::RParen)?;
            SQLSetExpr::Query(Box::new(subquery))
        } else {
            return self.expected("SELECT or a subquery in the query body", self.peek_token());
        };

        loop {
            // The query can be optionally followed by a set operator:
            let next_token = self.peek_token();
            let op = self.parse_set_operator(&next_token);
            let next_precedence = match op {
                // UNION and EXCEPT have the same binding power and evaluate left-to-right
                Some(SQLSetOperator::Union) | Some(SQLSetOperator::Except) => 10,
                // INTERSECT has higher precedence than UNION/EXCEPT
                Some(SQLSetOperator::Intersect) => 20,
                // Unexpected token or EOF => stop parsing the query body
                None => break,
            };
            if precedence >= next_precedence {
                break;
            }
            self.next_token(); // skip past the set operator
            expr = SQLSetExpr::SetOperation {
                left: Box::new(expr),
                op: op.unwrap(),
                all: self.parse_keyword("ALL"),
                right: Box::new(self.parse_query_body(next_precedence)?),
            };
        }

        Ok(expr)
    }

    fn parse_set_operator(&mut self, token: &Option<Token>) -> Option<SQLSetOperator> {
        match token {
            Some(Token::SQLWord(w)) if w.keyword == "UNION" => Some(SQLSetOperator::Union),
            Some(Token::SQLWord(w)) if w.keyword == "EXCEPT" => Some(SQLSetOperator::Except),
            Some(Token::SQLWord(w)) if w.keyword == "INTERSECT" => Some(SQLSetOperator::Intersect),
            _ => None,
        }
    }

    /// Parse a restricted `SELECT` statement (no CTEs / `UNION` / `ORDER BY`),
    /// assuming the initial `SELECT` was already consumed
    pub fn parse_select(&mut self) -> Result<SQLSelect, ParserError> {
        let all = self.parse_keyword("ALL");
        let distinct = self.parse_keyword("DISTINCT");
        if all && distinct {
            return parser_err!("Cannot specify both ALL and DISTINCT in SELECT");
        }
        let projection = self.parse_select_list()?;

        let (relation, joins) = if self.parse_keyword("FROM") {
            let relation = Some(self.parse_table_factor()?);
            let joins = self.parse_joins()?;
            (relation, joins)
        } else {
            (None, vec![])
        };

        let selection = if self.parse_keyword("WHERE") {
            Some(self.parse_expr()?)
        } else {
            None
        };

        let group_by = if self.parse_keywords(vec!["GROUP", "BY"]) {
            self.parse_expr_list()?
        } else {
            vec![]
        };

        let having = if self.parse_keyword("HAVING") {
            Some(self.parse_expr()?)
        } else {
            None
        };

        Ok(SQLSelect {
            distinct,
            projection,
            selection,
            relation,
            joins,
            group_by,
            having,
        })
    }

    /// A table name or a parenthesized subquery, followed by optional `[AS] alias`
    pub fn parse_table_factor(&mut self) -> Result<TableFactor, ParserError> {
        let lateral = self.parse_keyword("LATERAL");
        if self.consume_token(&Token::LParen) {
            let subquery = Box::new(self.parse_query()?);
            self.expect_token(&Token::RParen)?;
            let alias = self.parse_optional_alias(keywords::RESERVED_FOR_TABLE_ALIAS)?;
            Ok(TableFactor::Derived {
                lateral,
                subquery,
                alias,
            })
        } else if lateral {
            self.expected("subquery after LATERAL", self.peek_token())
        } else {
            let name = self.parse_object_name()?;
            // Postgres, MSSQL: table-valued functions:
            let args = if self.consume_token(&Token::LParen) {
                self.parse_optional_args()?
            } else {
                vec![]
            };
            let alias = self.parse_optional_alias(keywords::RESERVED_FOR_TABLE_ALIAS)?;
            // MSSQL-specific table hints:
            let mut with_hints = vec![];
            if self.parse_keyword("WITH") {
                if self.consume_token(&Token::LParen) {
                    with_hints = self.parse_expr_list()?;
                    self.expect_token(&Token::RParen)?;
                } else {
                    // rewind, as WITH may belong to the next statement's CTE
                    self.prev_token();
                }
            };
            Ok(TableFactor::Table {
                name,
                alias,
                args,
                with_hints,
            })
        }
    }

    fn parse_join_constraint(&mut self, natural: bool) -> Result<JoinConstraint, ParserError> {
        if natural {
            Ok(JoinConstraint::Natural)
        } else if self.parse_keyword("ON") {
            let constraint = self.parse_expr()?;
            Ok(JoinConstraint::On(constraint))
        } else if self.parse_keyword("USING") {
            let columns = self.parse_parenthesized_column_list(Mandatory)?;
            Ok(JoinConstraint::Using(columns))
        } else {
            self.expected("ON, or USING after JOIN", self.peek_token())
        }
    }

    fn parse_joins(&mut self) -> Result<Vec<Join>, ParserError> {
        let mut joins = vec![];
        loop {
            let natural = match &self.peek_token() {
                Some(Token::Comma) => {
                    self.next_token();
                    let relation = self.parse_table_factor()?;
                    let join = Join {
                        relation,
                        join_operator: JoinOperator::Implicit,
                    };
                    joins.push(join);
                    continue;
                }
                Some(Token::SQLWord(kw)) if kw.keyword == "CROSS" => {
                    self.next_token();
                    self.expect_keyword("JOIN")?;
                    let relation = self.parse_table_factor()?;
                    let join = Join {
                        relation,
                        join_operator: JoinOperator::Cross,
                    };
                    joins.push(join);
                    continue;
                }
                Some(Token::SQLWord(kw)) if kw.keyword == "NATURAL" => {
                    self.next_token();
                    true
                }
                Some(_) => false,
                None => return Ok(joins),
            };

            let join = match &self.peek_token() {
                Some(Token::SQLWord(kw)) if kw.keyword == "INNER" => {
                    self.next_token();
                    self.expect_keyword("JOIN")?;
                    Join {
                        relation: self.parse_table_factor()?,
                        join_operator: JoinOperator::Inner(self.parse_join_constraint(natural)?),
                    }
                }
                Some(Token::SQLWord(kw)) if kw.keyword == "JOIN" => {
                    self.next_token();
                    Join {
                        relation: self.parse_table_factor()?,
                        join_operator: JoinOperator::Inner(self.parse_join_constraint(natural)?),
                    }
                }
                Some(Token::SQLWord(kw)) if kw.keyword == "LEFT" => {
                    self.next_token();
                    let _ = self.parse_keyword("OUTER");
                    self.expect_keyword("JOIN")?;
                    Join {
                        relation: self.parse_table_factor()?,
                        join_operator: JoinOperator::LeftOuter(
                            self.parse_join_constraint(natural)?,
                        ),
                    }
                }
                Some(Token::SQLWord(kw)) if kw.keyword == "RIGHT" => {
                    self.next_token();
                    let _ = self.parse_keyword("OUTER");
                    self.expect_keyword("JOIN")?;
                    Join {
                        relation: self.parse_table_factor()?,
                        join_operator: JoinOperator::RightOuter(
                            self.parse_join_constraint(natural)?,
                        ),
                    }
                }
                Some(Token::SQLWord(kw)) if kw.keyword == "FULL" => {
                    self.next_token();
                    let _ = self.parse_keyword("OUTER");
                    self.expect_keyword("JOIN")?;
                    Join {
                        relation: self.parse_table_factor()?,
                        join_operator: JoinOperator::FullOuter(
                            self.parse_join_constraint(natural)?,
                        ),
                    }
                }
                _ => break,
            };
            joins.push(join);
        }

        Ok(joins)
    }

    /// Parse an INSERT statement
    pub fn parse_insert(&mut self) -> Result<SQLStatement, ParserError> {
        self.expect_keyword("INTO")?;
        let table_name = self.parse_object_name()?;
        let columns = self.parse_parenthesized_column_list(Optional)?;
        self.expect_keyword("VALUES")?;
        self.expect_token(&Token::LParen)?;
        let values = self.parse_expr_list()?;
        self.expect_token(&Token::RParen)?;
        Ok(SQLStatement::SQLInsert {
            table_name,
            columns,
            values: vec![values],
        })
    }

    /// Parse a comma-delimited list of SQL expressions
    pub fn parse_expr_list(&mut self) -> Result<Vec<ASTNode>, ParserError> {
        let mut expr_list: Vec<ASTNode> = vec![];
        loop {
            expr_list.push(self.parse_expr()?);
            match self.peek_token() {
                Some(Token::Comma) => self.next_token(),
                _ => break,
            };
        }
        Ok(expr_list)
    }

    pub fn parse_optional_args(&mut self) -> Result<Vec<ASTNode>, ParserError> {
        if self.consume_token(&Token::RParen) {
            Ok(vec![])
        } else {
            let args = self.parse_expr_list()?;
            self.expect_token(&Token::RParen)?;
            Ok(args)
        }
    }

    /// Parse a comma-delimited list of projections after SELECT
    pub fn parse_select_list(&mut self) -> Result<Vec<SQLSelectItem>, ParserError> {
        let mut projections: Vec<SQLSelectItem> = vec![];
        loop {
            let expr = self.parse_expr()?;
            if let ASTNode::SQLWildcard = expr {
                projections.push(SQLSelectItem::Wildcard);
            } else if let ASTNode::SQLQualifiedWildcard(prefix) = expr {
                projections.push(SQLSelectItem::QualifiedWildcard(SQLObjectName(prefix)));
            } else {
                // `expr` is a regular SQL expression and can be followed by an alias
                if let Some(alias) =
                    self.parse_optional_alias(keywords::RESERVED_FOR_COLUMN_ALIAS)?
                {
                    projections.push(SQLSelectItem::ExpressionWithAlias { expr, alias });
                } else {
                    projections.push(SQLSelectItem::UnnamedExpression(expr));
                }
            }

            match self.peek_token() {
                Some(Token::Comma) => self.next_token(),
                _ => break,
            };
        }
        Ok(projections)
    }

    /// Parse a comma-delimited list of SQL ORDER BY expressions
    pub fn parse_order_by_expr_list(&mut self) -> Result<Vec<SQLOrderByExpr>, ParserError> {
        let mut expr_list: Vec<SQLOrderByExpr> = vec![];
        loop {
            let expr = self.parse_expr()?;

            let asc = if self.parse_keyword("ASC") {
                Some(true)
            } else if self.parse_keyword("DESC") {
                Some(false)
            } else {
                None
            };

            expr_list.push(SQLOrderByExpr { expr, asc });

            if let Some(Token::Comma) = self.peek_token() {
                self.next_token();
            } else {
                break;
            }
        }
        Ok(expr_list)
    }

    /// Parse a LIMIT clause
    pub fn parse_limit(&mut self) -> Result<Option<ASTNode>, ParserError> {
        if self.parse_keyword("ALL") {
            Ok(None)
        } else {
            self.parse_literal_int()
                .map(|n| Some(ASTNode::SQLValue(Value::Long(n))))
        }
    }

    /// Parse an OFFSET clause
    pub fn parse_offset(&mut self) -> Result<ASTNode, ParserError> {
        let value = self
            .parse_literal_int()
            .map(|n| ASTNode::SQLValue(Value::Long(n)))?;
        self.expect_one_of_keywords(&["ROW", "ROWS"])?;
        Ok(value)
    }

    /// Parse a FETCH clause
    pub fn parse_fetch(&mut self) -> Result<Fetch, ParserError> {
        self.expect_one_of_keywords(&["FIRST", "NEXT"])?;
        let (quantity, percent) = if self.parse_one_of_keywords(&["ROW", "ROWS"]).is_some() {
            (None, false)
        } else {
            let quantity = self.parse_sql_value()?;
            let percent = self.parse_keyword("PERCENT");
            self.expect_one_of_keywords(&["ROW", "ROWS"])?;
            (Some(quantity), percent)
        };
        let with_ties = if self.parse_keyword("ONLY") {
            false
        } else if self.parse_keywords(vec!["WITH", "TIES"]) {
            true
        } else {
            return self.expected("one of ONLY or WITH TIES", self.peek_token());
        };
        Ok(Fetch {
            with_ties,
            percent,
            quantity,
        })
    }
}

impl SQLWord {
    pub fn as_sql_ident(&self) -> SQLIdent {
        self.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::all_dialects;

    #[test]
    fn test_prev_index() {
        let sql = "SELECT version()";
        all_dialects().run_parser_method(sql, |parser| {
            assert_eq!(parser.prev_token(), None);
            assert_eq!(parser.next_token(), Some(Token::make_keyword("SELECT")));
            assert_eq!(parser.next_token(), Some(Token::make_word("version", None)));
            assert_eq!(parser.prev_token(), Some(Token::make_word("version", None)));
            assert_eq!(parser.peek_token(), Some(Token::make_word("version", None)));
            assert_eq!(parser.prev_token(), Some(Token::make_keyword("SELECT")));
            assert_eq!(parser.prev_token(), None);
        });
    }
}

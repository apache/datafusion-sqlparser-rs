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

use super::dialect::keywords;
use super::dialect::Dialect;
use super::sqlast::*;
use super::sqltokenizer::*;
use chrono::{offset::FixedOffset, DateTime, NaiveDate, NaiveDateTime, NaiveTime};

#[derive(Debug, Clone, PartialEq)]
pub enum ParserError {
    TokenizerError(String),
    ParserError(String),
}

macro_rules! parser_err {
    ($MSG:expr) => {
        Err(ParserError::ParserError($MSG.to_string()))
    };
}

impl From<TokenizerError> for ParserError {
    fn from(e: TokenizerError) -> Self {
        ParserError::TokenizerError(format!("{:?}", e))
    }
}

/// SQL Parser
pub struct Parser {
    tokens: Vec<Token>,
    index: usize,
}

impl Parser {
    /// Parse the specified tokens
    pub fn new(tokens: Vec<Token>) -> Self {
        Parser {
            tokens: tokens,
            index: 0,
        }
    }

    /// Parse a SQL statement and produce an Abstract Syntax Tree (AST)
    pub fn parse_sql(dialect: &Dialect, sql: String) -> Result<Vec<SQLStatement>, ParserError> {
        let mut tokenizer = Tokenizer::new(dialect, &sql);
        let tokens = tokenizer.tokenize()?;
        let mut parser = Parser::new(tokens);
        let mut stmts = Vec::new();
        let mut expecting_statement_delimiter = false;
        loop {
            // ignore empty statements (between successive statement delimiters)
            while parser.consume_token(&Token::SemiColon) {
                expecting_statement_delimiter = false;
            }

            if parser.peek_token().is_none() {
                break;
            } else if expecting_statement_delimiter {
                return parser_err!(format!(
                    "Expected end of statement, found: {}",
                    parser.peek_token().unwrap().to_string()
                ));
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
                        Ok(SQLStatement::SQLSelect(self.parse_query()?))
                    }
                    "CREATE" => Ok(self.parse_create()?),
                    "DELETE" => Ok(self.parse_delete()?),
                    "INSERT" => Ok(self.parse_insert()?),
                    "ALTER" => Ok(self.parse_alter()?),
                    "COPY" => Ok(self.parse_copy()?),
                    _ => parser_err!(format!(
                        "Unexpected keyword {:?} at the beginning of a statement",
                        w.to_string()
                    )),
                },
                unexpected => parser_err!(format!(
                    "Unexpected {:?} at the beginning of a statement",
                    unexpected
                )),
            },
            _ => parser_err!("Unexpected end of file"),
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
        match self.next_token() {
            Some(t) => match t {
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
                    // another SQLWord:
                    _ => match self.peek_token() {
                        Some(Token::LParen) => self.parse_function(w.as_sql_ident()),
                        Some(Token::Period) => {
                            let mut id_parts: Vec<SQLIdent> = vec![w.as_sql_ident()];
                            let mut ends_with_wildcard = false;
                            while self.consume_token(&Token::Period) {
                                match self.next_token() {
                                    Some(Token::SQLWord(w)) => id_parts.push(w.as_sql_ident()),
                                    Some(Token::Mult) => {
                                        ends_with_wildcard = true;
                                        break;
                                    }
                                    _ => {
                                        return parser_err!(format!(
                                            "Error parsing compound identifier"
                                        ));
                                    }
                                }
                            }
                            if ends_with_wildcard {
                                Ok(ASTNode::SQLQualifiedWildcard(id_parts))
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
                    Ok(ASTNode::SQLUnary {
                        operator: self.to_sql_operator(&tok)?,
                        expr: Box::new(self.parse_subexpr(p)?),
                    })
                }
                Token::Number(_)
                | Token::SingleQuotedString(_)
                | Token::NationalStringLiteral(_) => {
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
                _ => parser_err!(format!(
                    "Did not expect {:?} at the beginning of an expression",
                    t
                )),
            },
            None => parser_err!(format!("Prefix parser expected a keyword but hit EOF")),
        }
    }

    pub fn parse_function(&mut self, id: SQLIdent) -> Result<ASTNode, ParserError> {
        self.expect_token(&Token::LParen)?;
        if self.consume_token(&Token::RParen) {
            Ok(ASTNode::SQLFunction {
                id: id,
                args: vec![],
            })
        } else {
            let args = self.parse_expr_list()?;
            self.expect_token(&Token::RParen)?;
            Ok(ASTNode::SQLFunction { id, args })
        }
    }

    pub fn parse_case_expression(&mut self) -> Result<ASTNode, ParserError> {
        if self.parse_keywords(vec!["WHEN"]) {
            let mut conditions = vec![];
            let mut results = vec![];
            let mut else_result = None;
            loop {
                conditions.push(self.parse_expr()?);
                self.expect_keyword("THEN")?;
                results.push(self.parse_expr()?);
                if self.parse_keywords(vec!["ELSE"]) {
                    else_result = Some(Box::new(self.parse_expr()?));
                    if self.parse_keywords(vec!["END"]) {
                        break;
                    } else {
                        return parser_err!("Expecting END after a CASE..ELSE");
                    }
                }
                if self.parse_keywords(vec!["END"]) {
                    break;
                }
                self.expect_keyword("WHEN")?;
            }
            Ok(ASTNode::SQLCase {
                conditions,
                results,
                else_result,
            })
        } else {
            // TODO: implement "simple" case
            // https://jakewheat.github.io/sql-overview/sql-2011-foundation-grammar.html#simple-case
            parser_err!("Simple case not implemented")
        }
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

    /// Parse an expression infix (typically an operator)
    pub fn parse_infix(&mut self, expr: ASTNode, precedence: u8) -> Result<ASTNode, ParserError> {
        debug!("parsing infix");
        match self.next_token() {
            Some(tok) => match tok {
                Token::SQLWord(ref k) if k.keyword == "IS" => {
                    if self.parse_keywords(vec!["NULL"]) {
                        Ok(ASTNode::SQLIsNull(Box::new(expr)))
                    } else if self.parse_keywords(vec!["NOT", "NULL"]) {
                        Ok(ASTNode::SQLIsNotNull(Box::new(expr)))
                    } else {
                        parser_err!(format!(
                            "Expected NULL or NOT NULL after IS, found {:?}",
                            self.peek_token()
                        ))
                    }
                }
                Token::SQLWord(ref k) if k.keyword == "NOT" => {
                    if self.parse_keyword("IN") {
                        self.parse_in(expr, true)
                    } else if self.parse_keyword("BETWEEN") {
                        self.parse_between(expr, true)
                    } else if self.parse_keyword("LIKE") {
                        Ok(ASTNode::SQLBinaryExpr {
                            left: Box::new(expr),
                            op: SQLOperator::NotLike,
                            right: Box::new(self.parse_subexpr(precedence)?),
                        })
                    } else {
                        parser_err!(format!(
                            "Expected IN or LIKE after NOT, found {:?}",
                            self.peek_token()
                        ))
                    }
                }
                Token::SQLWord(ref k) if k.keyword == "IN" => self.parse_in(expr, false),
                Token::SQLWord(ref k) if k.keyword == "BETWEEN" => self.parse_between(expr, false),
                Token::DoubleColon => self.parse_pg_cast(expr),
                Token::SQLWord(_)
                | Token::Eq
                | Token::Neq
                | Token::Gt
                | Token::GtEq
                | Token::Lt
                | Token::LtEq
                | Token::Plus
                | Token::Minus
                | Token::Mult
                | Token::Mod
                | Token::Div => Ok(ASTNode::SQLBinaryExpr {
                    left: Box::new(expr),
                    op: self.to_sql_operator(&tok)?,
                    right: Box::new(self.parse_subexpr(precedence)?),
                }),
                _ => parser_err!(format!("No infix parser for token {:?}", tok)),
            },
            // This is not supposed to happen, because of the precedence check
            // in parse_subexpr.
            None => parser_err!("Unexpected EOF in parse_infix"),
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
        let low = self.parse_prefix()?;
        self.expect_keyword("AND")?;
        let high = self.parse_prefix()?;
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

    /// Convert a token operator to an AST operator
    pub fn to_sql_operator(&self, tok: &Token) -> Result<SQLOperator, ParserError> {
        match tok {
            &Token::Eq => Ok(SQLOperator::Eq),
            &Token::Neq => Ok(SQLOperator::NotEq),
            &Token::Lt => Ok(SQLOperator::Lt),
            &Token::LtEq => Ok(SQLOperator::LtEq),
            &Token::Gt => Ok(SQLOperator::Gt),
            &Token::GtEq => Ok(SQLOperator::GtEq),
            &Token::Plus => Ok(SQLOperator::Plus),
            &Token::Minus => Ok(SQLOperator::Minus),
            &Token::Mult => Ok(SQLOperator::Multiply),
            &Token::Div => Ok(SQLOperator::Divide),
            &Token::Mod => Ok(SQLOperator::Modulus),
            &Token::SQLWord(ref k) if k.keyword == "AND" => Ok(SQLOperator::And),
            &Token::SQLWord(ref k) if k.keyword == "OR" => Ok(SQLOperator::Or),
            //&Token::SQLWord(ref k) if k.keyword == "NOT" => Ok(SQLOperator::Not),
            &Token::SQLWord(ref k) if k.keyword == "LIKE" => Ok(SQLOperator::Like),
            _ => parser_err!(format!("Unsupported SQL operator {:?}", tok)),
        }
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
            &Token::SQLWord(ref k) if k.keyword == "OR" => Ok(5),
            &Token::SQLWord(ref k) if k.keyword == "AND" => Ok(10),
            &Token::SQLWord(ref k) if k.keyword == "NOT" => Ok(15),
            &Token::SQLWord(ref k) if k.keyword == "IS" => Ok(17),
            &Token::SQLWord(ref k) if k.keyword == "IN" => Ok(20),
            &Token::SQLWord(ref k) if k.keyword == "BETWEEN" => Ok(20),
            &Token::SQLWord(ref k) if k.keyword == "LIKE" => Ok(20),
            &Token::Eq | &Token::Lt | &Token::LtEq | &Token::Neq | &Token::Gt | &Token::GtEq => {
                Ok(20)
            }
            &Token::Plus | &Token::Minus => Ok(30),
            &Token::Mult | &Token::Div | &Token::Mod => Ok(40),
            &Token::DoubleColon => Ok(50),
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
                    index = index + 1;
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
            self.index = self.index + 1;
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
            self.index = self.index - 1;
            Some(self.tokens[self.index].clone())
        } else {
            None
        }
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

    /// Bail out if the current token is not an expected keyword, or consume it if it is
    pub fn expect_keyword(&mut self, expected: &'static str) -> Result<(), ParserError> {
        if self.parse_keyword(expected) {
            Ok(())
        } else {
            parser_err!(format!(
                "Expected keyword {}, found {:?}",
                expected,
                self.peek_token()
            ))
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
            parser_err!(format!(
                "Expected token {:?}, found {:?}",
                expected,
                self.peek_token()
            ))
        }
    }

    /// Parse a SQL CREATE statement
    pub fn parse_create(&mut self) -> Result<SQLStatement, ParserError> {
        if self.parse_keyword("TABLE") {
            self.parse_create_table()
        } else if self.parse_keyword("VIEW") {
            self.parse_create_view()
        } else {
            parser_err!(format!(
                "Unexpected token after CREATE: {:?}",
                self.peek_token()
            ))
        }
    }

    pub fn parse_create_view(&mut self) -> Result<SQLStatement, ParserError> {
        // Many dialects support `OR REPLACE` | `OR ALTER` right after `CREATE`, but we don't (yet).
        // ANSI SQL and Postgres support RECURSIVE here, but we don't support it either.
        let name = self.parse_object_name()?;
        // Parenthesized "output" columns list could be handled here.
        // Some dialects allow WITH here, followed by some keywords (e.g. MS SQL)
        // or `(k1=v1, k2=v2, ...)` (Postgres)
        self.expect_keyword("AS")?;
        let query = self.parse_query()?;
        // Optional `WITH [ CASCADED | LOCAL ] CHECK OPTION` is widely supported here.
        Ok(SQLStatement::SQLCreateView { name, query })
    }

    pub fn parse_create_table(&mut self) -> Result<SQLStatement, ParserError> {
        let table_name = self.parse_object_name()?;
        // parse optional column list (schema)
        let mut columns = vec![];
        if self.consume_token(&Token::LParen) {
            loop {
                match self.next_token() {
                    Some(Token::SQLWord(column_name)) => {
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
                        } else if self.parse_keyword("NULL") {
                            true
                        } else {
                            true
                        };
                        debug!("default: {:?}", default);

                        columns.push(SQLColumnDef {
                            name: column_name.as_sql_ident(),
                            data_type: data_type,
                            allow_null,
                            is_primary,
                            is_unique,
                            default,
                        });
                        match self.next_token() {
                            Some(Token::Comma) => {}
                            Some(Token::RParen) => {
                                break;
                            }
                            other => {
                                return parser_err!(format!(
                                    "Expected ',' or ')' after column definition but found {:?}",
                                    other
                                ));
                            }
                        }
                    }
                    unexpected => {
                        return parser_err!(format!("Expected column name, got {:?}", unexpected));
                    }
                }
            }
        }
        Ok(SQLStatement::SQLCreateTable {
            name: table_name,
            columns,
        })
    }

    pub fn parse_table_key(&mut self, constraint_name: SQLIdent) -> Result<TableKey, ParserError> {
        let is_primary_key = self.parse_keywords(vec!["PRIMARY", "KEY"]);
        let is_unique_key = self.parse_keywords(vec!["UNIQUE", "KEY"]);
        let is_foreign_key = self.parse_keywords(vec!["FOREIGN", "KEY"]);
        self.expect_token(&Token::LParen)?;
        let column_names = self.parse_column_names()?;
        self.expect_token(&Token::RParen)?;
        let key = Key {
            name: constraint_name,
            columns: column_names,
        };
        if is_primary_key {
            Ok(TableKey::PrimaryKey(key))
        } else if is_unique_key {
            Ok(TableKey::UniqueKey(key))
        } else if is_foreign_key {
            self.expect_keyword("REFERENCES")?;
            let foreign_table = self.parse_object_name()?;
            self.expect_token(&Token::LParen)?;
            let referred_columns = self.parse_column_names()?;
            self.expect_token(&Token::RParen)?;
            Ok(TableKey::ForeignKey {
                key,
                foreign_table,
                referred_columns,
            })
        } else {
            parser_err!(format!(
                "Expecting primary key, unique key, or foreign key, found: {:?}",
                self.peek_token()
            ))
        }
    }

    pub fn parse_alter(&mut self) -> Result<SQLStatement, ParserError> {
        self.expect_keyword("TABLE")?;
        let _ = self.parse_keyword("ONLY");
        let table_name = self.parse_object_name()?;
        let operation: Result<AlterOperation, ParserError> =
            if self.parse_keywords(vec!["ADD", "CONSTRAINT"]) {
                let constraint_name = self.parse_identifier()?;
                let table_key = self.parse_table_key(constraint_name)?;
                Ok(AlterOperation::AddConstraint(table_key))
            } else {
                return parser_err!(format!(
                    "Expecting ADD CONSTRAINT, found :{:?}",
                    self.peek_token()
                ));
            };
        Ok(SQLStatement::SQLAlterTable {
            name: table_name,
            operation: operation?,
        })
    }

    /// Parse a copy statement
    pub fn parse_copy(&mut self) -> Result<SQLStatement, ParserError> {
        let table_name = self.parse_object_name()?;
        let columns = if self.consume_token(&Token::LParen) {
            let column_names = self.parse_column_names()?;
            self.expect_token(&Token::RParen)?;
            column_names
        } else {
            vec![]
        };
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
            Some(t) => {
                match t {
                    Token::SQLWord(k) => match k.keyword.as_ref() {
                        "TRUE" => Ok(Value::Boolean(true)),
                        "FALSE" => Ok(Value::Boolean(false)),
                        "NULL" => Ok(Value::Null),
                        _ => {
                            return parser_err!(format!(
                                "No value parser for keyword {}",
                                k.keyword
                            ));
                        }
                    },
                    //TODO: parse the timestamp here (see parse_timestamp_value())
                    Token::Number(ref n) if n.contains(".") => match n.parse::<f64>() {
                        Ok(n) => Ok(Value::Double(n)),
                        Err(e) => parser_err!(format!("Could not parse '{}' as f64: {}", n, e)),
                    },
                    Token::Number(ref n) => match n.parse::<i64>() {
                        Ok(n) => Ok(Value::Long(n)),
                        Err(e) => parser_err!(format!("Could not parse '{}' as i64: {}", n, e)),
                    },
                    Token::SingleQuotedString(ref s) => {
                        Ok(Value::SingleQuotedString(s.to_string()))
                    }
                    Token::NationalStringLiteral(ref s) => {
                        Ok(Value::NationalStringLiteral(s.to_string()))
                    }
                    _ => parser_err!(format!("Unsupported value: {:?}", t)),
                }
            }
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

    pub fn parse_timezone_offset(&mut self) -> Result<i8, ParserError> {
        match self.next_token() {
            Some(Token::Plus) => {
                let n = self.parse_literal_int()?;
                Ok(n as i8)
            }
            Some(Token::Minus) => {
                let n = self.parse_literal_int()?;
                Ok(-n as i8)
            }
            other => parser_err!(format!(
                "Expecting `+` or `-` in timezone, but found {:?}",
                other
            )),
        }
    }

    pub fn parse_timestamp_value(&mut self) -> Result<Value, ParserError> {
        let year = self.parse_literal_int()?;
        let date = self.parse_date(year)?;
        if let Ok(time) = self.parse_time() {
            let date_time = NaiveDateTime::new(date, time);
            match self.peek_token() {
                Some(token) => match token {
                    Token::Plus | Token::Minus => {
                        let tz = self.parse_timezone_offset()?;
                        let offset = FixedOffset::east(tz as i32 * 3600);
                        Ok(Value::Timestamp(DateTime::from_utc(date_time, offset)))
                    }
                    _ => Ok(Value::DateTime(date_time)),
                },
                _ => Ok(Value::DateTime(date_time)),
            }
        } else {
            parser_err!(format!(
                "Expecting time after date, but found {:?}",
                self.peek_token()
            ))
        }
    }

    pub fn parse_date(&mut self, year: i64) -> Result<NaiveDate, ParserError> {
        if self.consume_token(&Token::Minus) {
            let month = self.parse_literal_int()?;
            if self.consume_token(&Token::Minus) {
                let day = self.parse_literal_int()?;
                let date = NaiveDate::from_ymd(year as i32, month as u32, day as u32);
                Ok(date)
            } else {
                parser_err!(format!(
                    "Expecting `-` for date separator, found {:?}",
                    self.peek_token()
                ))
            }
        } else {
            parser_err!(format!(
                "Expecting `-` for date separator, found {:?}",
                self.peek_token()
            ))
        }
    }

    pub fn parse_time(&mut self) -> Result<NaiveTime, ParserError> {
        let hour = self.parse_literal_int()?;
        self.expect_token(&Token::Colon)?;
        let min = self.parse_literal_int()?;
        self.expect_token(&Token::Colon)?;
        // On one hand, the SQL specs defines <seconds fraction> ::= <unsigned integer>,
        // so it would be more correct to parse it as such
        let sec = self.parse_literal_double()?;
        // On the other, chrono only supports nanoseconds, which should(?) fit in seconds-as-f64...
        let nanos = (sec.fract() * 1_000_000_000.0).round();
        Ok(NaiveTime::from_hms_nano(
            hour as u32,
            min as u32,
            sec as u32,
            nanos as u32,
        ))
    }

    /// Parse a SQL datatype (in the context of a CREATE TABLE statement for example)
    pub fn parse_data_type(&mut self) -> Result<SQLType, ParserError> {
        match self.next_token() {
            Some(Token::SQLWord(k)) => match k.keyword.as_ref() {
                "BOOLEAN" => Ok(SQLType::Boolean),
                "FLOAT" => Ok(SQLType::Float(self.parse_optional_precision()?)),
                "REAL" => Ok(SQLType::Real),
                "DOUBLE" => {
                    if self.parse_keyword("PRECISION") {
                        Ok(SQLType::Double)
                    } else {
                        Ok(SQLType::Double)
                    }
                }
                "SMALLINT" => Ok(SQLType::SmallInt),
                "INT" | "INTEGER" => Ok(SQLType::Int),
                "BIGINT" => Ok(SQLType::BigInt),
                "VARCHAR" => Ok(SQLType::Varchar(self.parse_optional_precision()?)),
                "CHARACTER" => {
                    if self.parse_keyword("VARYING") {
                        Ok(SQLType::Varchar(self.parse_optional_precision()?))
                    } else {
                        Ok(SQLType::Char(self.parse_optional_precision()?))
                    }
                }
                "UUID" => Ok(SQLType::Uuid),
                "DATE" => Ok(SQLType::Date),
                "TIMESTAMP" => {
                    if self.parse_keyword("WITH") {
                        if self.parse_keywords(vec!["TIME", "ZONE"]) {
                            Ok(SQLType::Timestamp)
                        } else {
                            parser_err!(format!(
                                "Expecting 'time zone', found: {:?}",
                                self.peek_token()
                            ))
                        }
                    } else if self.parse_keyword("WITHOUT") {
                        if self.parse_keywords(vec!["TIME", "ZONE"]) {
                            Ok(SQLType::Timestamp)
                        } else {
                            parser_err!(format!(
                                "Expecting 'time zone', found: {:?}",
                                self.peek_token()
                            ))
                        }
                    } else {
                        Ok(SQLType::Timestamp)
                    }
                }
                "TIME" => {
                    if self.parse_keyword("WITH") {
                        if self.parse_keywords(vec!["TIME", "ZONE"]) {
                            Ok(SQLType::Time)
                        } else {
                            parser_err!(format!(
                                "Expecting 'time zone', found: {:?}",
                                self.peek_token()
                            ))
                        }
                    } else if self.parse_keyword("WITHOUT") {
                        if self.parse_keywords(vec!["TIME", "ZONE"]) {
                            Ok(SQLType::Time)
                        } else {
                            parser_err!(format!(
                                "Expecting 'time zone', found: {:?}",
                                self.peek_token()
                            ))
                        }
                    } else {
                        Ok(SQLType::Timestamp)
                    }
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
            other => parser_err!(format!("Invalid data type: '{:?}'", other)),
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
            parser_err!(format!(
                "Expecting identifier, found {:?}",
                self.peek_token()
            ))
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
            unexpected => parser_err!(format!("Expected identifier, found {:?}", unexpected)),
        }
    }

    /// Parse a comma-separated list of unqualified, possibly quoted identifiers
    pub fn parse_column_names(&mut self) -> Result<Vec<SQLIdent>, ParserError> {
        Ok(self.parse_list_of_ids(&Token::Comma)?)
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

        self.expect_keyword("SELECT")?;
        let body = self.parse_select()?;

        let order_by = if self.parse_keywords(vec!["ORDER", "BY"]) {
            Some(self.parse_order_by_expr_list()?)
        } else {
            None
        };

        let limit = if self.parse_keyword("LIMIT") {
            self.parse_limit()?
        } else {
            None
        };

        Ok(SQLQuery {
            ctes,
            body,
            limit,
            order_by,
        })
    }

    /// Parse one or more (comma-separated) `alias AS (subquery)` CTEs,
    /// assuming the initial `WITH` was already consumed.
    fn parse_cte_list(&mut self) -> Result<Vec<Cte>, ParserError> {
        let mut cte = vec![];
        loop {
            let alias = self.parse_identifier()?;
            // TODO: Optional `( <column list> )`
            self.expect_keyword("AS")?;
            self.expect_token(&Token::LParen)?;
            cte.push(Cte {
                alias,
                query: self.parse_query()?,
            });
            self.expect_token(&Token::RParen)?;
            if !self.consume_token(&Token::Comma) {
                break;
            }
        }
        return Ok(cte);
    }

    /// Parse a restricted `SELECT` statement (no CTEs / `UNION` / `ORDER BY`),
    /// assuming the initial `SELECT` was already consumed
    pub fn parse_select(&mut self) -> Result<SQLSelect, ParserError> {
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
            Some(self.parse_expr_list()?)
        } else {
            None
        };

        let having = if self.parse_keyword("HAVING") {
            Some(self.parse_expr()?)
        } else {
            None
        };

        Ok(SQLSelect {
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
        if self.consume_token(&Token::LParen) {
            let subquery = Box::new(self.parse_query()?);
            self.expect_token(&Token::RParen)?;
            let alias = self.parse_optional_alias(keywords::RESERVED_FOR_TABLE_ALIAS)?;
            Ok(TableFactor::Derived { subquery, alias })
        } else {
            let name = self.parse_object_name()?;
            let alias = self.parse_optional_alias(keywords::RESERVED_FOR_TABLE_ALIAS)?;
            Ok(TableFactor::Table { name, alias })
        }
    }

    fn parse_join_constraint(&mut self, natural: bool) -> Result<JoinConstraint, ParserError> {
        if natural {
            Ok(JoinConstraint::Natural)
        } else if self.parse_keyword("ON") {
            let constraint = self.parse_expr()?;
            Ok(JoinConstraint::On(constraint))
        } else if self.parse_keyword("USING") {
            self.expect_token(&Token::LParen)?;
            let attributes = self.parse_column_names()?;
            self.expect_token(&Token::RParen)?;
            Ok(JoinConstraint::Using(attributes))
        } else {
            parser_err!(format!(
                "Unexpected token after JOIN: {:?}",
                self.peek_token()
            ))
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
        let columns = if self.consume_token(&Token::LParen) {
            let column_names = self.parse_column_names()?;
            self.expect_token(&Token::RParen)?;
            column_names
        } else {
            vec![]
        };
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
                    projections.push(SQLSelectItem::ExpressionWithAlias(expr, alias));
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
}

impl SQLWord {
    pub fn as_sql_ident(&self) -> SQLIdent {
        self.to_string()
    }
}

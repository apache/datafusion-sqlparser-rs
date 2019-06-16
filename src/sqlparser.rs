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

pub enum IsLateral {
    Lateral,
    NotLateral,
}
use IsLateral::*;

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
    /// The index of the first unprocessed token in `self.tokens`
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
                    "SELECT" | "WITH" | "VALUES" => {
                        self.prev_token();
                        Ok(SQLStatement::SQLQuery(Box::new(self.parse_query()?)))
                    }
                    "CREATE" => Ok(self.parse_create()?),
                    "DROP" => Ok(self.parse_drop()?),
                    "DELETE" => Ok(self.parse_delete()?),
                    "INSERT" => Ok(self.parse_insert()?),
                    "UPDATE" => Ok(self.parse_update()?),
                    "ALTER" => Ok(self.parse_alter()?),
                    "COPY" => Ok(self.parse_copy()?),
                    "START" => Ok(self.parse_start_transaction()?),
                    "SET" => Ok(self.parse_set_transaction()?),
                    // `BEGIN` is a nonstandard but common alias for the
                    // standard `START TRANSACTION` statement. It is supported
                    // by at least PostgreSQL and MySQL.
                    "BEGIN" => Ok(self.parse_begin()?),
                    "COMMIT" => Ok(self.parse_commit()?),
                    "ROLLBACK" => Ok(self.parse_rollback()?),
                    _ => parser_err!(format!(
                        "Unexpected keyword {:?} at the beginning of a statement",
                        w.to_string()
                    )),
                },
                Token::LParen => {
                    self.prev_token();
                    Ok(SQLStatement::SQLQuery(Box::new(self.parse_query()?)))
                }
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
                "DATE" => Ok(ASTNode::SQLValue(Value::Date(self.parse_literal_string()?))),
                "EXISTS" => self.parse_exists_expression(),
                "EXTRACT" => self.parse_extract_expression(),
                "INTERVAL" => self.parse_literal_interval(),
                "NOT" => Ok(ASTNode::SQLUnaryOp {
                    op: SQLUnaryOperator::Not,
                    expr: Box::new(self.parse_subexpr(Self::UNARY_NOT_PREC)?),
                }),
                "TIME" => Ok(ASTNode::SQLValue(Value::Time(self.parse_literal_string()?))),
                "TIMESTAMP" => Ok(ASTNode::SQLValue(Value::Timestamp(
                    self.parse_literal_string()?,
                ))),
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
                let op = if tok == Token::Plus {
                    SQLUnaryOperator::Plus
                } else {
                    SQLUnaryOperator::Minus
                };
                Ok(ASTNode::SQLUnaryOp {
                    op,
                    expr: Box::new(self.parse_subexpr(Self::PLUS_MINUS_PREC)?),
                })
            }
            Token::Number(_)
            | Token::SingleQuotedString(_)
            | Token::NationalStringLiteral(_)
            | Token::HexStringLiteral(_) => {
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

        Ok(ASTNode::SQLFunction(SQLFunction {
            name,
            args,
            over,
            distinct,
        }))
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
                let rows = self.parse_literal_uint()?;
                Some(rows)
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

    /// Parse a SQL EXISTS expression e.g. `WHERE EXISTS(SELECT ...)`.
    pub fn parse_exists_expression(&mut self) -> Result<ASTNode, ParserError> {
        self.expect_token(&Token::LParen)?;
        let exists_node = ASTNode::SQLExists(Box::new(self.parse_query()?));
        self.expect_token(&Token::RParen)?;
        Ok(exists_node)
    }

    pub fn parse_extract_expression(&mut self) -> Result<ASTNode, ParserError> {
        self.expect_token(&Token::LParen)?;
        let field = self.parse_date_time_field()?;
        self.expect_keyword("FROM")?;
        let expr = self.parse_expr()?;
        self.expect_token(&Token::RParen)?;
        Ok(ASTNode::SQLExtract {
            field,
            expr: Box::new(expr),
        })
    }

    // This function parses date/time fields for both the EXTRACT function-like
    // operator and interval qualifiers. EXTRACT supports a wider set of
    // date/time fields than interval qualifiers, so this function may need to
    // be split in two.
    pub fn parse_date_time_field(&mut self) -> Result<SQLDateTimeField, ParserError> {
        let tok = self.next_token();
        if let Some(Token::SQLWord(ref k)) = tok {
            match k.keyword.as_ref() {
                "YEAR" => Ok(SQLDateTimeField::Year),
                "MONTH" => Ok(SQLDateTimeField::Month),
                "DAY" => Ok(SQLDateTimeField::Day),
                "HOUR" => Ok(SQLDateTimeField::Hour),
                "MINUTE" => Ok(SQLDateTimeField::Minute),
                "SECOND" => Ok(SQLDateTimeField::Second),
                _ => self.expected("date/time field", tok)?,
            }
        } else {
            self.expected("date/time field", tok)?
        }
    }

    /// Parse an INTERVAL literal.
    ///
    /// Some syntactically valid intervals:
    ///
    ///   1. `INTERVAL '1' DAY`
    ///   2. `INTERVAL '1-1' YEAR TO MONTH`
    ///   3. `INTERVAL '1' SECOND`
    ///   4. `INTERVAL '1:1:1.1' HOUR (5) TO SECOND (5)`
    ///   5. `INTERVAL '1.1' SECOND (2, 2)`
    ///   6. `INTERVAL '1:1' HOUR (5) TO MINUTE (5)`
    ///
    /// Note that we do not currently attempt to parse the quoted value.
    pub fn parse_literal_interval(&mut self) -> Result<ASTNode, ParserError> {
        // The SQL standard allows an optional sign before the value string, but
        // it is not clear if any implementations support that syntax, so we
        // don't currently try to parse it. (The sign can instead be included
        // inside the value string.)

        // The first token in an interval is a string literal which specifies
        // the duration of the interval.
        let value = self.parse_literal_string()?;

        // Following the string literal is a qualifier which indicates the units
        // of the duration specified in the string literal.
        //
        // Note that PostgreSQL allows omitting the qualifier, but we currently
        // require at least the leading field, in accordance with the ANSI spec.
        let leading_field = self.parse_date_time_field()?;

        let (leading_precision, last_field, fsec_precision) =
            if leading_field == SQLDateTimeField::Second {
                // SQL mandates special syntax for `SECOND TO SECOND` literals.
                // Instead of
                //     `SECOND [(<leading precision>)] TO SECOND[(<fractional seconds precision>)]`
                // one must use the special format:
                //     `SECOND [( <leading precision> [ , <fractional seconds precision>] )]`
                let last_field = None;
                let (leading_precision, fsec_precision) = self.parse_optional_precision_scale()?;
                (leading_precision, last_field, fsec_precision)
            } else {
                let leading_precision = self.parse_optional_precision()?;
                if self.parse_keyword("TO") {
                    let last_field = Some(self.parse_date_time_field()?);
                    let fsec_precision = if last_field == Some(SQLDateTimeField::Second) {
                        self.parse_optional_precision()?
                    } else {
                        None
                    };
                    (leading_precision, last_field, fsec_precision)
                } else {
                    (leading_precision, None, None)
                }
            };

        Ok(ASTNode::SQLValue(Value::Interval {
            value,
            leading_field,
            leading_precision,
            last_field,
            fractional_seconds_precision: fsec_precision,
        }))
    }

    /// Parse an operator following an expression
    pub fn parse_infix(&mut self, expr: ASTNode, precedence: u8) -> Result<ASTNode, ParserError> {
        debug!("parsing infix");
        let tok = self.next_token().unwrap(); // safe as EOF's precedence is the lowest

        let regular_binary_operator = match tok {
            Token::Eq => Some(SQLBinaryOperator::Eq),
            Token::Neq => Some(SQLBinaryOperator::NotEq),
            Token::Gt => Some(SQLBinaryOperator::Gt),
            Token::GtEq => Some(SQLBinaryOperator::GtEq),
            Token::Lt => Some(SQLBinaryOperator::Lt),
            Token::LtEq => Some(SQLBinaryOperator::LtEq),
            Token::Plus => Some(SQLBinaryOperator::Plus),
            Token::Minus => Some(SQLBinaryOperator::Minus),
            Token::Mult => Some(SQLBinaryOperator::Multiply),
            Token::Mod => Some(SQLBinaryOperator::Modulus),
            Token::Div => Some(SQLBinaryOperator::Divide),
            Token::SQLWord(ref k) => match k.keyword.as_ref() {
                "AND" => Some(SQLBinaryOperator::And),
                "OR" => Some(SQLBinaryOperator::Or),
                "LIKE" => Some(SQLBinaryOperator::Like),
                "NOT" => {
                    if self.parse_keyword("LIKE") {
                        Some(SQLBinaryOperator::NotLike)
                    } else {
                        None
                    }
                }
                _ => None,
            },
            _ => None,
        };

        if let Some(op) = regular_binary_operator {
            Ok(ASTNode::SQLBinaryOp {
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
                // Can only happen if `get_next_precedence` got out of sync with this function
                _ => panic!("No infix parser for token {:?}", tok),
            }
        } else if Token::DoubleColon == tok {
            self.parse_pg_cast(expr)
        } else {
            // Can only happen if `get_next_precedence` got out of sync with this function
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
        let low = self.parse_subexpr(Self::BETWEEN_PREC)?;
        self.expect_keyword("AND")?;
        let high = self.parse_subexpr(Self::BETWEEN_PREC)?;
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

    const UNARY_NOT_PREC: u8 = 15;
    const BETWEEN_PREC: u8 = 20;
    const PLUS_MINUS_PREC: u8 = 30;

    /// Get the precedence of the next token
    pub fn get_next_precedence(&self) -> Result<u8, ParserError> {
        if let Some(token) = self.peek_token() {
            debug!("get_next_precedence() {:?}", token);

            match &token {
                Token::SQLWord(k) if k.keyword == "OR" => Ok(5),
                Token::SQLWord(k) if k.keyword == "AND" => Ok(10),
                Token::SQLWord(k) if k.keyword == "NOT" => match &self.peek_nth_token(1) {
                    // The precedence of NOT varies depending on keyword that
                    // follows it. If it is followed by IN, BETWEEN, or LIKE,
                    // it takes on the precedence of those tokens. Otherwise it
                    // is not an infix operator, and therefore has zero
                    // precedence.
                    Some(Token::SQLWord(k)) if k.keyword == "IN" => Ok(Self::BETWEEN_PREC),
                    Some(Token::SQLWord(k)) if k.keyword == "BETWEEN" => Ok(Self::BETWEEN_PREC),
                    Some(Token::SQLWord(k)) if k.keyword == "LIKE" => Ok(Self::BETWEEN_PREC),
                    _ => Ok(0),
                },
                Token::SQLWord(k) if k.keyword == "IS" => Ok(17),
                Token::SQLWord(k) if k.keyword == "IN" => Ok(Self::BETWEEN_PREC),
                Token::SQLWord(k) if k.keyword == "BETWEEN" => Ok(Self::BETWEEN_PREC),
                Token::SQLWord(k) if k.keyword == "LIKE" => Ok(Self::BETWEEN_PREC),
                Token::Eq | Token::Lt | Token::LtEq | Token::Neq | Token::Gt | Token::GtEq => {
                    Ok(20)
                }
                Token::Plus | Token::Minus => Ok(Self::PLUS_MINUS_PREC),
                Token::Mult | Token::Div | Token::Mod => Ok(40),
                Token::DoubleColon => Ok(50),
                _ => Ok(0),
            }
        } else {
            Ok(0)
        }
    }

    /// Return the first non-whitespace token that has not yet been processed
    /// (or None if reached end-of-file)
    pub fn peek_token(&self) -> Option<Token> {
        self.peek_nth_token(0)
    }

    /// Return nth non-whitespace token that has not yet been processed
    pub fn peek_nth_token(&self, mut n: usize) -> Option<Token> {
        let mut index = self.index;
        loop {
            index += 1;
            match self.tokens.get(index - 1) {
                Some(Token::Whitespace(_)) => continue,
                non_whitespace => {
                    if n == 0 {
                        return non_whitespace.cloned();
                    }
                    n -= 1;
                }
            }
        }
    }

    /// Return the first non-whitespace token that has not yet been processed
    /// (or None if reached end-of-file) and mark it as processed. OK to call
    /// repeatedly after reaching EOF.
    pub fn next_token(&mut self) -> Option<Token> {
        loop {
            self.index += 1;
            match self.tokens.get(self.index - 1) {
                Some(Token::Whitespace(_)) => continue,
                token => return token.cloned(),
            }
        }
    }

    /// Return the first unprocessed token, possibly whitespace.
    pub fn next_token_no_skip(&mut self) -> Option<&Token> {
        self.index += 1;
        self.tokens.get(self.index - 1)
    }

    /// Push back the last one non-whitespace token. Must be called after
    /// `next_token()`, otherwise might panic. OK to call after
    /// `next_token()` indicates an EOF.
    pub fn prev_token(&mut self) {
        loop {
            assert!(self.index > 0);
            self.index -= 1;
            if let Some(Token::Whitespace(_)) = self.tokens.get(self.index) {
                continue;
            }
            return;
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
        match &self.peek_token() {
            Some(t) if *t == *expected => {
                self.next_token();
                true
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
            with_options: vec![],
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
        let columns = self.parse_parenthesized_column_list(Optional)?;
        let with_options = if self.parse_keyword("WITH") {
            self.parse_with_options()?
        } else {
            vec![]
        };
        self.expect_keyword("AS")?;
        let query = Box::new(self.parse_query()?);
        // Optional `WITH [ CASCADED | LOCAL ] CHECK OPTION` is widely supported here.
        Ok(SQLStatement::SQLCreateView {
            name,
            columns,
            query,
            materialized,
            with_options,
        })
    }

    pub fn parse_drop(&mut self) -> Result<SQLStatement, ParserError> {
        let object_type = if self.parse_keyword("TABLE") {
            SQLObjectType::Table
        } else if self.parse_keyword("VIEW") {
            SQLObjectType::View
        } else {
            return self.expected("TABLE or VIEW after DROP", self.peek_token());
        };
        let if_exists = self.parse_keywords(vec!["IF", "EXISTS"]);
        let mut names = vec![];
        loop {
            names.push(self.parse_object_name()?);
            if !self.consume_token(&Token::Comma) {
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

        let with_options = if self.parse_keyword("WITH") {
            self.parse_with_options()?
        } else {
            vec![]
        };

        Ok(SQLStatement::SQLCreateTable {
            name: table_name,
            columns,
            constraints,
            with_options,
            external: false,
            file_format: None,
            location: None,
        })
    }

    fn parse_columns(&mut self) -> Result<(Vec<SQLColumnDef>, Vec<TableConstraint>), ParserError> {
        let mut columns = vec![];
        let mut constraints = vec![];
        if !self.consume_token(&Token::LParen) || self.consume_token(&Token::RParen) {
            return Ok((columns, constraints));
        }

        loop {
            if let Some(constraint) = self.parse_optional_table_constraint()? {
                constraints.push(constraint);
            } else if let Some(Token::SQLWord(column_name)) = self.peek_token() {
                self.next_token();
                let data_type = self.parse_data_type()?;
                let collation = if self.parse_keyword("COLLATE") {
                    Some(self.parse_object_name()?)
                } else {
                    None
                };
                let mut options = vec![];
                loop {
                    match self.peek_token() {
                        None | Some(Token::Comma) | Some(Token::RParen) => break,
                        _ => options.push(self.parse_column_option_def()?),
                    }
                }

                columns.push(SQLColumnDef {
                    name: column_name.as_sql_ident(),
                    data_type,
                    collation,
                    options,
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

    pub fn parse_column_option_def(&mut self) -> Result<ColumnOptionDef, ParserError> {
        let name = if self.parse_keyword("CONSTRAINT") {
            Some(self.parse_identifier()?)
        } else {
            None
        };

        let option = if self.parse_keywords(vec!["NOT", "NULL"]) {
            ColumnOption::NotNull
        } else if self.parse_keyword("NULL") {
            ColumnOption::Null
        } else if self.parse_keyword("DEFAULT") {
            ColumnOption::Default(self.parse_expr()?)
        } else if self.parse_keywords(vec!["PRIMARY", "KEY"]) {
            ColumnOption::Unique { is_primary: true }
        } else if self.parse_keyword("UNIQUE") {
            ColumnOption::Unique { is_primary: false }
        } else if self.parse_keyword("REFERENCES") {
            let foreign_table = self.parse_object_name()?;
            let referred_columns = self.parse_parenthesized_column_list(Mandatory)?;
            ColumnOption::ForeignKey {
                foreign_table,
                referred_columns,
            }
        } else if self.parse_keyword("CHECK") {
            self.expect_token(&Token::LParen)?;
            let expr = self.parse_expr()?;
            self.expect_token(&Token::RParen)?;
            ColumnOption::Check(expr)
        } else {
            return self.expected("column option", self.peek_token());
        };

        Ok(ColumnOptionDef { name, option })
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
                    self.prev_token();
                    Ok(None)
                }
            }
        }
    }

    pub fn parse_with_options(&mut self) -> Result<Vec<SQLOption>, ParserError> {
        self.expect_token(&Token::LParen)?;
        let mut options = vec![];
        loop {
            let name = self.parse_identifier()?;
            self.expect_token(&Token::Eq)?;
            let value = self.parse_value()?;
            options.push(SQLOption { name, value });
            if !self.consume_token(&Token::Comma) {
                break;
            }
        }
        self.expect_token(&Token::RParen)?;
        Ok(options)
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
                    Ok(n) => Ok(Value::Double(n.into())),
                    Err(e) => parser_err!(format!("Could not parse '{}' as f64: {}", n, e)),
                },
                Token::Number(ref n) => match n.parse::<u64>() {
                    Ok(n) => Ok(Value::Long(n)),
                    Err(e) => parser_err!(format!("Could not parse '{}' as u64: {}", n, e)),
                },
                Token::SingleQuotedString(ref s) => Ok(Value::SingleQuotedString(s.to_string())),
                Token::NationalStringLiteral(ref s) => {
                    Ok(Value::NationalStringLiteral(s.to_string()))
                }
                Token::HexStringLiteral(ref s) => Ok(Value::HexStringLiteral(s.to_string())),
                _ => parser_err!(format!("Unsupported value: {:?}", t)),
            },
            None => parser_err!("Expecting a value, but found EOF"),
        }
    }

    /// Parse an unsigned literal integer/long
    pub fn parse_literal_uint(&mut self) -> Result<u64, ParserError> {
        match self.next_token() {
            Some(Token::Number(s)) => s.parse::<u64>().map_err(|e| {
                ParserError::ParserError(format!("Could not parse '{}' as u64: {}", s, e))
            }),
            other => self.expected("literal int", other),
        }
    }

    /// Parse a literal string
    pub fn parse_literal_string(&mut self) -> Result<String, ParserError> {
        match self.next_token() {
            Some(Token::SingleQuotedString(ref s)) => Ok(s.clone()),
            other => self.expected("literal string", other),
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
                // Interval types can be followed by a complicated interval
                // qualifier that we don't currently support. See
                // parse_interval_literal for a taste.
                "INTERVAL" => Ok(SQLType::Interval),
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
                "NUMERIC" | "DECIMAL" | "DEC" => {
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
        match self.next_token() {
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
            // MSSQL supports single-quoted strings as aliases for columns
            // We accept them as table aliases too, although MSSQL does not.
            Some(Token::SingleQuotedString(ref s)) => Ok(Some(format!("'{}'", s))),
            not_an_ident => {
                if after_as {
                    return self.expected("an identifier after AS", not_an_ident);
                }
                self.prev_token();
                Ok(None) // no alias found
            }
        }
    }

    /// Parse `AS identifier` when the AS is describing a table-valued object,
    /// like in `... FROM generate_series(1, 10) AS t (col)`. In this case
    /// the alias is allowed to optionally name the columns in the table, in
    /// addition to the table itself.
    pub fn parse_optional_table_alias(
        &mut self,
        reserved_kwds: &[&str],
    ) -> Result<Option<TableAlias>, ParserError> {
        match self.parse_optional_alias(reserved_kwds)? {
            Some(name) => {
                let columns = self.parse_parenthesized_column_list(Optional)?;
                Ok(Some(TableAlias { name, columns }))
            }
            None => Ok(None),
        }
    }

    /// Parse one or more identifiers with the specified separator between them
    pub fn parse_list_of_ids(&mut self, separator: &Token) -> Result<Vec<SQLIdent>, ParserError> {
        let mut idents = vec![];
        loop {
            idents.push(self.parse_identifier()?);
            if !self.consume_token(separator) {
                break;
            }
        }
        Ok(idents)
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

    pub fn parse_optional_precision(&mut self) -> Result<Option<u64>, ParserError> {
        if self.consume_token(&Token::LParen) {
            let n = self.parse_literal_uint()?;
            self.expect_token(&Token::RParen)?;
            Ok(Some(n))
        } else {
            Ok(None)
        }
    }

    pub fn parse_optional_precision_scale(
        &mut self,
    ) -> Result<(Option<u64>, Option<u64>), ParserError> {
        if self.consume_token(&Token::LParen) {
            let n = self.parse_literal_uint()?;
            let scale = if self.consume_token(&Token::Comma) {
                Some(self.parse_literal_uint()?)
            } else {
                None
            };
            self.expect_token(&Token::RParen)?;
            Ok((Some(n), scale))
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
            let alias = TableAlias {
                name: self.parse_identifier()?,
                columns: self.parse_parenthesized_column_list(Optional)?,
            };
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
        } else if self.parse_keyword("VALUES") {
            SQLSetExpr::Values(self.parse_values()?)
        } else {
            return self.expected(
                "SELECT, VALUES, or a subquery in the query body",
                self.peek_token(),
            );
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

        // Note that for keywords to be properly handled here, they need to be
        // added to `RESERVED_FOR_COLUMN_ALIAS` / `RESERVED_FOR_TABLE_ALIAS`,
        // otherwise they may be parsed as an alias as part of the `projection`
        // or `from`.

        let mut from = vec![];
        if self.parse_keyword("FROM") {
            loop {
                from.push(self.parse_table_and_joins()?);
                if !self.consume_token(&Token::Comma) {
                    break;
                }
            }
        }

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
            from,
            selection,
            group_by,
            having,
        })
    }

    pub fn parse_table_and_joins(&mut self) -> Result<TableWithJoins, ParserError> {
        let relation = self.parse_table_factor()?;
        let mut joins = vec![];
        loop {
            let join = if self.parse_keyword("CROSS") {
                self.expect_keyword("JOIN")?;
                Join {
                    relation: self.parse_table_factor()?,
                    join_operator: JoinOperator::Cross,
                }
            } else {
                let natural = self.parse_keyword("NATURAL");
                let peek_keyword = if let Some(Token::SQLWord(kw)) = self.peek_token() {
                    kw.keyword
                } else {
                    String::default()
                };

                let join_operator_type = match peek_keyword.as_ref() {
                    "INNER" | "JOIN" => {
                        let _ = self.parse_keyword("INNER");
                        self.expect_keyword("JOIN")?;
                        JoinOperator::Inner
                    }
                    kw @ "LEFT" | kw @ "RIGHT" | kw @ "FULL" => {
                        let _ = self.next_token();
                        let _ = self.parse_keyword("OUTER");
                        self.expect_keyword("JOIN")?;
                        match kw {
                            "LEFT" => JoinOperator::LeftOuter,
                            "RIGHT" => JoinOperator::RightOuter,
                            "FULL" => JoinOperator::FullOuter,
                            _ => unreachable!(),
                        }
                    }
                    _ if natural => {
                        return self.expected("a join type after NATURAL", self.peek_token());
                    }
                    _ => break,
                };
                let relation = self.parse_table_factor()?;
                let join_constraint = self.parse_join_constraint(natural)?;
                Join {
                    relation,
                    join_operator: join_operator_type(join_constraint),
                }
            };
            joins.push(join);
        }
        Ok(TableWithJoins { relation, joins })
    }

    /// A table name or a parenthesized subquery, followed by optional `[AS] alias`
    pub fn parse_table_factor(&mut self) -> Result<TableFactor, ParserError> {
        if self.parse_keyword("LATERAL") {
            // LATERAL must always be followed by a subquery.
            if !self.consume_token(&Token::LParen) {
                self.expected("subquery after LATERAL", self.peek_token())?;
            }
            return self.parse_derived_table_factor(Lateral);
        }

        if self.consume_token(&Token::LParen) {
            let index = self.index;
            // A left paren introduces either a derived table (i.e., a subquery)
            // or a nested join. It's nearly impossible to determine ahead of
            // time which it is... so we just try to parse both.
            //
            // Here's an example that demonstrates the complexity:
            //                     /-------------------------------------------------------\
            //                     | /-----------------------------------\                 |
            //     SELECT * FROM ( ( ( (SELECT 1) UNION (SELECT 2) ) AS t1 NATURAL JOIN t2 ) )
            //                   ^ ^ ^ ^
            //                   | | | |
            //                   | | | |
            //                   | | | (4) belongs to a SQLSetExpr::Query inside the subquery
            //                   | | (3) starts a derived table (subquery)
            //                   | (2) starts a nested join
            //                   (1) an additional set of parens around a nested join
            //
            match self.parse_derived_table_factor(NotLateral) {
                // The recently consumed '(' started a derived table, and we've
                // parsed the subquery, followed by the closing ')', and the
                // alias of the derived table. In the example above this is
                // case (3), and the next token would be `NATURAL`.
                Ok(table_factor) => Ok(table_factor),
                Err(_) => {
                    // The '(' we've recently consumed does not start a derived
                    // table. For valid input this can happen either when the
                    // token following the paren can't start a query (e.g. `foo`
                    // in `FROM (foo NATURAL JOIN bar)`, or when the '(' we've
                    // consumed is followed by another '(' that starts a
                    // derived table, like (3), or another nested join (2).
                    //
                    // Ignore the error and back up to where we were before.
                    // Either we'll be able to parse a valid nested join, or
                    // we won't, and we'll return that error instead.
                    self.index = index;
                    let table_and_joins = self.parse_table_and_joins()?;
                    match table_and_joins.relation {
                        TableFactor::NestedJoin { .. } => (),
                        _ => {
                            if table_and_joins.joins.is_empty() {
                                // The SQL spec prohibits derived tables and bare
                                // tables from appearing alone in parentheses.
                                self.expected("joined table", self.peek_token())?
                            }
                        }
                    }
                    self.expect_token(&Token::RParen)?;
                    Ok(TableFactor::NestedJoin(Box::new(table_and_joins)))
                }
            }
        } else {
            let name = self.parse_object_name()?;
            // Postgres, MSSQL: table-valued functions:
            let args = if self.consume_token(&Token::LParen) {
                self.parse_optional_args()?
            } else {
                vec![]
            };
            let alias = self.parse_optional_table_alias(keywords::RESERVED_FOR_TABLE_ALIAS)?;
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

    pub fn parse_derived_table_factor(
        &mut self,
        lateral: IsLateral,
    ) -> Result<TableFactor, ParserError> {
        let subquery = Box::new(self.parse_query()?);
        self.expect_token(&Token::RParen)?;
        let alias = self.parse_optional_table_alias(keywords::RESERVED_FOR_TABLE_ALIAS)?;
        Ok(TableFactor::Derived {
            lateral: match lateral {
                Lateral => true,
                NotLateral => false,
            },
            subquery,
            alias,
        })
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

    /// Parse an INSERT statement
    pub fn parse_insert(&mut self) -> Result<SQLStatement, ParserError> {
        self.expect_keyword("INTO")?;
        let table_name = self.parse_object_name()?;
        let columns = self.parse_parenthesized_column_list(Optional)?;
        let source = Box::new(self.parse_query()?);
        Ok(SQLStatement::SQLInsert {
            table_name,
            columns,
            source,
        })
    }

    pub fn parse_update(&mut self) -> Result<SQLStatement, ParserError> {
        let table_name = self.parse_object_name()?;
        self.expect_keyword("SET")?;
        let mut assignments = vec![];
        loop {
            let id = self.parse_identifier()?;
            self.expect_token(&Token::Eq)?;
            let value = self.parse_expr()?;
            assignments.push(SQLAssignment { id, value });
            if !self.consume_token(&Token::Comma) {
                break;
            }
        }
        let selection = if self.parse_keyword("WHERE") {
            Some(self.parse_expr()?)
        } else {
            None
        };
        Ok(SQLStatement::SQLUpdate {
            table_name,
            assignments,
            selection,
        })
    }

    /// Parse a comma-delimited list of SQL expressions
    pub fn parse_expr_list(&mut self) -> Result<Vec<ASTNode>, ParserError> {
        let mut expr_list: Vec<ASTNode> = vec![];
        loop {
            expr_list.push(self.parse_expr()?);
            if !self.consume_token(&Token::Comma) {
                break;
            }
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

            if !self.consume_token(&Token::Comma) {
                break;
            }
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
            if !self.consume_token(&Token::Comma) {
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
            self.parse_literal_uint()
                .map(|n| Some(ASTNode::SQLValue(Value::Long(n))))
        }
    }

    /// Parse an OFFSET clause
    pub fn parse_offset(&mut self) -> Result<ASTNode, ParserError> {
        let value = self
            .parse_literal_uint()
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

    pub fn parse_values(&mut self) -> Result<SQLValues, ParserError> {
        let mut values = vec![];
        loop {
            self.expect_token(&Token::LParen)?;
            values.push(self.parse_expr_list()?);
            self.expect_token(&Token::RParen)?;
            if !self.consume_token(&Token::Comma) {
                break;
            }
        }
        Ok(SQLValues(values))
    }

    pub fn parse_start_transaction(&mut self) -> Result<SQLStatement, ParserError> {
        self.expect_keyword("TRANSACTION")?;
        Ok(SQLStatement::SQLStartTransaction {
            modes: self.parse_transaction_modes()?,
        })
    }

    pub fn parse_begin(&mut self) -> Result<SQLStatement, ParserError> {
        let _ = self.parse_one_of_keywords(&["TRANSACTION", "WORK"]);
        Ok(SQLStatement::SQLStartTransaction {
            modes: self.parse_transaction_modes()?,
        })
    }

    pub fn parse_set_transaction(&mut self) -> Result<SQLStatement, ParserError> {
        self.expect_keyword("TRANSACTION")?;
        Ok(SQLStatement::SQLSetTransaction {
            modes: self.parse_transaction_modes()?,
        })
    }

    pub fn parse_transaction_modes(&mut self) -> Result<Vec<TransactionMode>, ParserError> {
        let mut modes = vec![];
        let mut required = false;
        loop {
            let mode = if self.parse_keywords(vec!["ISOLATION", "LEVEL"]) {
                let iso_level = if self.parse_keywords(vec!["READ", "UNCOMMITTED"]) {
                    TransactionIsolationLevel::ReadUncommitted
                } else if self.parse_keywords(vec!["READ", "COMMITTED"]) {
                    TransactionIsolationLevel::ReadCommitted
                } else if self.parse_keywords(vec!["REPEATABLE", "READ"]) {
                    TransactionIsolationLevel::RepeatableRead
                } else if self.parse_keyword("SERIALIZABLE") {
                    TransactionIsolationLevel::Serializable
                } else {
                    self.expected("isolation level", self.peek_token())?
                };
                TransactionMode::IsolationLevel(iso_level)
            } else if self.parse_keywords(vec!["READ", "ONLY"]) {
                TransactionMode::AccessMode(TransactionAccessMode::ReadOnly)
            } else if self.parse_keywords(vec!["READ", "WRITE"]) {
                TransactionMode::AccessMode(TransactionAccessMode::ReadWrite)
            } else if required || self.peek_token().is_some() {
                self.expected("transaction mode", self.peek_token())?
            } else {
                break;
            };
            modes.push(mode);
            // ANSI requires a comma after each transaction mode, but
            // PostgreSQL, for historical reasons, does not. We follow
            // PostgreSQL in making the comma optional, since that is strictly
            // more general.
            required = self.consume_token(&Token::Comma);
        }
        Ok(modes)
    }

    pub fn parse_commit(&mut self) -> Result<SQLStatement, ParserError> {
        Ok(SQLStatement::SQLCommit {
            chain: self.parse_commit_rollback_chain()?,
        })
    }

    pub fn parse_rollback(&mut self) -> Result<SQLStatement, ParserError> {
        Ok(SQLStatement::SQLRollback {
            chain: self.parse_commit_rollback_chain()?,
        })
    }

    pub fn parse_commit_rollback_chain(&mut self) -> Result<bool, ParserError> {
        let _ = self.parse_one_of_keywords(&["TRANSACTION", "WORK"]);
        if self.parse_keyword("AND") {
            let chain = !self.parse_keyword("NO");
            self.expect_keyword("CHAIN")?;
            Ok(chain)
        } else {
            Ok(false)
        }
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
        let sql = "SELECT version";
        all_dialects().run_parser_method(sql, |parser| {
            assert_eq!(parser.peek_token(), Some(Token::make_keyword("SELECT")));
            assert_eq!(parser.next_token(), Some(Token::make_keyword("SELECT")));
            parser.prev_token();
            assert_eq!(parser.next_token(), Some(Token::make_keyword("SELECT")));
            assert_eq!(parser.next_token(), Some(Token::make_word("version", None)));
            parser.prev_token();
            assert_eq!(parser.peek_token(), Some(Token::make_word("version", None)));
            assert_eq!(parser.next_token(), Some(Token::make_word("version", None)));
            assert_eq!(parser.peek_token(), None);
            parser.prev_token();
            assert_eq!(parser.next_token(), Some(Token::make_word("version", None)));
            assert_eq!(parser.next_token(), None);
            assert_eq!(parser.next_token(), None);
            parser.prev_token();
        });
    }
}

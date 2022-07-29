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

#[cfg(not(feature = "std"))]
use alloc::{
    boxed::Box,
    format,
    string::{String, ToString},
    vec,
    vec::Vec,
};
use core::fmt;

use log::debug;

use crate::ast::*;
use crate::keywords::{self, Keyword};
use crate::tokenizer::*;

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

// Returns a successful result if the optional expression is some
macro_rules! return_ok_if_some {
    ($e:expr) => {{
        if let Some(v) = $e {
            return Ok(v);
        }
    }};
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
use crate::dialect::DialectDisplay;

pub enum WildcardExpr {
    Expr(Expr),
    QualifiedWildcard(ObjectName),
    Wildcard,
}

impl From<WildcardExpr> for FunctionArgExpr {
    fn from(wildcard_expr: WildcardExpr) -> Self {
        match wildcard_expr {
            WildcardExpr::Expr(expr) => Self::Expr(expr),
            WildcardExpr::QualifiedWildcard(prefix) => Self::QualifiedWildcard(prefix),
            WildcardExpr::Wildcard => Self::Wildcard,
        }
    }
}

impl From<TokenizerError> for ParserError {
    fn from(e: TokenizerError) -> Self {
        ParserError::TokenizerError(e.to_string())
    }
}

impl fmt::Display for ParserError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
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

#[cfg(feature = "std")]
impl std::error::Error for ParserError {}

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

    /// Parse a SQL query and produce an Abstract Syntax Tree (AST)
    pub fn parse_sql_query(sql: &str) -> Result<Query, ParserError> {
        let mut tokenizer = Tokenizer::new(sql);
        let tokens = tokenizer.tokenize()?;
        let mut parser = Parser::new(tokens);
        debug!("Parsing sql '{}'...", sql);
        let query = parser.parse_query()?;

        if parser.peek_token() == Token::EOF {
            Ok(query)
        } else {
            Err(ParserError::ParserError(format!(
                "Expected end of query, found: {}",
                parser.peek_token()
            )))
        }
    }

    /// Parse a new expression including wildcard & qualified wildcard
    pub fn parse_wildcard_expr(&mut self) -> Result<WildcardExpr, ParserError> {
        let index = self.index;

        match self.next_token() {
            Token::Word(w) if self.peek_token() == Token::Period => {
                let mut id_parts: Vec<Ident> = vec![w.to_ident()];

                while self.consume_token(&Token::Period) {
                    match self.next_token() {
                        Token::Word(w) => id_parts.push(w.to_ident()),
                        Token::Mul => {
                            return Ok(WildcardExpr::QualifiedWildcard(ObjectName(id_parts)));
                        }
                        unexpected => {
                            return self.expected("an identifier or a '*' after '.'", unexpected);
                        }
                    }
                }
            }
            Token::Mul => {
                return Ok(WildcardExpr::Wildcard);
            }
            _ => (),
        };

        self.index = index;
        self.parse_expr().map(WildcardExpr::Expr)
    }

    /// Parse a new expression
    pub fn parse_expr(&mut self) -> Result<Expr, ParserError> {
        self.parse_subexpr(0)
    }

    /// Parse tokens until the precedence changes
    pub fn parse_subexpr(&mut self, precedence: u8) -> Result<Expr, ParserError> {
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
    pub fn parse_prefix(&mut self) -> Result<Expr, ParserError> {
        // PostgreSQL allows any string literal to be preceded by a type name, indicating that the
        // string literal represents a literal of that type. Some examples:
        //
        //      DATE '2020-05-20'
        //      TIMESTAMP WITH TIME ZONE '2020-05-20 7:43:54'
        //      BOOL 'true'
        //
        // The first two are standard SQL, while the latter is a PostgreSQL extension. Complicating
        // matters is the fact that INTERVAL string literals may optionally be followed by special
        // keywords, e.g.:
        //
        //      INTERVAL '7' DAY
        //
        // Note also that naively `SELECT date` looks like a syntax error because the `date` type
        // name is not followed by a string literal, but in fact in PostgreSQL it is a valid
        // expression that should parse as the column name "date".
        return_ok_if_some!(self.maybe_parse(|parser| {
            match parser.parse_data_type()? {
                DataType::Interval => parser.parse_literal_interval(),
                // PostgreSQL allows almost any identifier to be used as custom data type name,
                // and we support that in `parse_data_type()`. But unlike Postgres we don't
                // have a list of globally reserved keywords (since they vary across dialects),
                // so given `NOT 'a' LIKE 'b'`, we'd accept `NOT` as a possible custom data type
                // name, resulting in `NOT 'a'` being recognized as a `TypedString` instead of
                // an unary negation `NOT ('a' LIKE 'b')`. To solve this, we don't accept the
                // `type 'string'` syntax for the custom data types at all.
                DataType::Custom(..) => parser_err!("dummy"),
                data_type => Ok(Expr::TypedString {
                    data_type,
                    value: parser.parse_literal_string()?,
                }),
            }
        }));

        let expr = match self.next_token() {
            Token::Word(w) => match w.keyword {
                Keyword::TRUE | Keyword::FALSE | Keyword::NULL => {
                    self.prev_token();
                    Ok(Expr::Value(self.parse_value()?))
                }
                Keyword::CURRENT_TIMESTAMP | Keyword::CURRENT_TIME | Keyword::CURRENT_DATE => {
                    self.parse_time_functions(ObjectName(vec![w.to_ident()]))
                }
                Keyword::CASE => self.parse_case_expr(),
                Keyword::CAST => self.parse_cast_expr(),
                Keyword::TRY_CAST => self.parse_try_cast_expr(),
                Keyword::EXISTS => self.parse_exists_expr(false),
                Keyword::EXTRACT => self.parse_extract_expr(),
                Keyword::POSITION => self.parse_position_expr(),
                Keyword::SUBSTRING => self.parse_substring_expr(),
                Keyword::TRIM => self.parse_trim_expr(),
                Keyword::INTERVAL => self.parse_literal_interval(),
                Keyword::LISTAGG => self.parse_listagg_expr(),
                // Treat ARRAY[1,2,3] as an array [1,2,3], otherwise try as function call
                Keyword::ARRAY if self.peek_token() == Token::LBracket => {
                    self.expect_token(&Token::LBracket)?;
                    self.parse_array_expr(true)
                }
                Keyword::NOT => self.parse_not(),
                // Here `w` is a word, check if it's a part of a multi-part
                // identifier, a function call, or a simple identifier:
                _ => match self.peek_token() {
                    Token::LParen | Token::Period => {
                        let mut id_parts: Vec<Ident> = vec![w.to_ident()];
                        while self.consume_token(&Token::Period) {
                            match self.next_token() {
                                Token::Word(w) => id_parts.push(w.to_ident()),
                                unexpected => {
                                    return self
                                        .expected("an identifier or a '*' after '.'", unexpected);
                                }
                            }
                        }

                        if self.consume_token(&Token::LParen) {
                            self.prev_token();
                            self.parse_function(ObjectName(id_parts))
                        } else {
                            Ok(Expr::CompoundIdentifier(id_parts))
                        }
                    }
                    _ => Ok(Expr::Identifier(w.to_ident())),
                },
            }, // End of Token::Word
            // array `[1, 2, 3]`
            Token::LBracket => self.parse_array_expr(false),
            tok @ Token::Minus | tok @ Token::Plus => {
                let op = if tok == Token::Plus {
                    UnaryOperator::Plus
                } else {
                    UnaryOperator::Minus
                };
                Ok(Expr::UnaryOp {
                    op,
                    expr: Box::new(self.parse_subexpr(Self::PLUS_MINUS_PREC)?),
                })
            }
            Token::EscapedStringLiteral(_) => {
                self.prev_token();
                Ok(Expr::Value(self.parse_value()?))
            }
            Token::Number(_, _)
            | Token::SingleQuotedString(_)
            | Token::DoubleQuotedString(_)
            | Token::NationalStringLiteral(_)
            | Token::HexStringLiteral(_) => {
                self.prev_token();
                Ok(Expr::Value(self.parse_value()?))
            }

            Token::LParen => {
                let expr =
                    if self.parse_keyword(Keyword::SELECT) || self.parse_keyword(Keyword::WITH) {
                        self.prev_token();
                        Expr::Subquery(Box::new(self.parse_query()?))
                    } else {
                        let exprs = self.parse_comma_separated(Parser::parse_expr)?;
                        match exprs.len() {
                            0 => unreachable!(), // parse_comma_separated ensures 1 or more
                            1 => Expr::Nested(Box::new(exprs.into_iter().next().unwrap())),
                            _ => Expr::Tuple(exprs),
                        }
                    };
                self.expect_token(&Token::RParen)?;
                if !self.consume_token(&Token::Period) {
                    Ok(expr)
                } else {
                    let tok = self.next_token();
                    let key = match tok {
                        Token::Word(word) => word.to_ident(),
                        _ => return parser_err!(format!("Expected identifier, found: {}", tok)),
                    };
                    Ok(Expr::CompositeAccess {
                        expr: Box::new(expr),
                        key,
                    })
                }
            }
            Token::Placeholder(_) => {
                self.prev_token();
                Ok(Expr::Value(self.parse_value()?))
            }
            unexpected => self.expected("an expression:", unexpected),
        }?;

        if self.parse_keyword(Keyword::COLLATE) {
            Ok(Expr::Collate {
                expr: Box::new(expr),
                collation: self.parse_object_name()?,
            })
        } else {
            Ok(expr)
        }
    }

    pub fn parse_function(&mut self, name: ObjectName) -> Result<Expr, ParserError> {
        self.expect_token(&Token::LParen)?;
        let distinct = self.parse_all_or_distinct()?;
        let args = self.parse_optional_args()?;
        let over = if self.parse_keyword(Keyword::OVER) {
            // TBD: support window names (`OVER mywin`) in place of inline specification
            self.expect_token(&Token::LParen)?;
            let partition_by = if self.parse_keywords(&[Keyword::PARTITION, Keyword::BY]) {
                // a list of possibly-qualified column names
                self.parse_comma_separated(Parser::parse_expr)?
            } else {
                vec![]
            };
            let order_by = if self.parse_keywords(&[Keyword::ORDER, Keyword::BY]) {
                self.parse_comma_separated(Parser::parse_order_by_expr)?
            } else {
                vec![]
            };
            let window_frame = if !self.consume_token(&Token::RParen) {
                let window_frame = self.parse_window_frame()?;
                self.expect_token(&Token::RParen)?;
                Some(window_frame)
            } else {
                None
            };

            Some(WindowSpec {
                partition_by,
                order_by,
                window_frame,
            })
        } else {
            None
        };

        Ok(Expr::Function(Function {
            name,
            args,
            over,
            distinct,
        }))
    }

    pub fn parse_time_functions(&mut self, name: ObjectName) -> Result<Expr, ParserError> {
        let args = if self.consume_token(&Token::LParen) {
            self.parse_optional_args()?
        } else {
            vec![]
        };
        Ok(Expr::Function(Function {
            name,
            args,
            over: None,
            distinct: false,
        }))
    }

    pub fn parse_window_frame_units(&mut self) -> Result<WindowFrameUnits, ParserError> {
        match self.next_token() {
            Token::Word(w) => match w.keyword {
                Keyword::ROWS => Ok(WindowFrameUnits::Rows),
                Keyword::RANGE => Ok(WindowFrameUnits::Range),
                Keyword::GROUPS => Ok(WindowFrameUnits::Groups),
                _ => self.expected("ROWS, RANGE, GROUPS", Token::Word(w))?,
            },
            unexpected => self.expected("ROWS, RANGE, GROUPS", unexpected),
        }
    }

    pub fn parse_window_frame(&mut self) -> Result<WindowFrame, ParserError> {
        let units = self.parse_window_frame_units()?;
        let (start_bound, end_bound) = if self.parse_keyword(Keyword::BETWEEN) {
            let start_bound = self.parse_window_frame_bound()?;
            self.expect_keyword(Keyword::AND)?;
            let end_bound = Some(self.parse_window_frame_bound()?);
            (start_bound, end_bound)
        } else {
            (self.parse_window_frame_bound()?, None)
        };
        Ok(WindowFrame {
            units,
            start_bound,
            end_bound,
        })
    }

    /// Parse `CURRENT ROW` or `{ <positive number> | UNBOUNDED } { PRECEDING | FOLLOWING }`
    pub fn parse_window_frame_bound(&mut self) -> Result<WindowFrameBound, ParserError> {
        if self.parse_keywords(&[Keyword::CURRENT, Keyword::ROW]) {
            Ok(WindowFrameBound::CurrentRow)
        } else {
            let rows = if self.parse_keyword(Keyword::UNBOUNDED) {
                None
            } else {
                Some(self.parse_literal_uint()?)
            };
            if self.parse_keyword(Keyword::PRECEDING) {
                Ok(WindowFrameBound::Preceding(rows))
            } else if self.parse_keyword(Keyword::FOLLOWING) {
                Ok(WindowFrameBound::Following(rows))
            } else {
                self.expected("PRECEDING or FOLLOWING", self.peek_token())
            }
        }
    }

    /// parse a group by expr. a group by expr can be one of group sets, roll up, cube, or simple
    /// expr.
    fn parse_group_by_expr(&mut self) -> Result<Expr, ParserError> {
        if self.parse_keywords(&[Keyword::GROUPING, Keyword::SETS]) {
            self.expect_token(&Token::LParen)?;
            let result = self.parse_comma_separated(|p| p.parse_tuple(false, true))?;
            self.expect_token(&Token::RParen)?;
            Ok(Expr::GroupingSets(result))
        } else if self.parse_keyword(Keyword::CUBE) {
            self.expect_token(&Token::LParen)?;
            let result = self.parse_comma_separated(|p| p.parse_tuple(true, true))?;
            self.expect_token(&Token::RParen)?;
            Ok(Expr::Cube(result))
        } else if self.parse_keyword(Keyword::ROLLUP) {
            self.expect_token(&Token::LParen)?;
            let result = self.parse_comma_separated(|p| p.parse_tuple(true, true))?;
            self.expect_token(&Token::RParen)?;
            Ok(Expr::Rollup(result))
        } else {
            self.parse_expr()
        }
    }

    /// parse a tuple with `(` and `)`.
    /// If `lift_singleton` is true, then a singleton tuple is lifted to a tuple of length 1, otherwise it will fail.
    /// If `allow_empty` is true, then an empty tuple is allowed.
    fn parse_tuple(
        &mut self,
        lift_singleton: bool,
        allow_empty: bool,
    ) -> Result<Vec<Expr>, ParserError> {
        if lift_singleton {
            if self.consume_token(&Token::LParen) {
                let result = if allow_empty && self.consume_token(&Token::RParen) {
                    vec![]
                } else {
                    let result = self.parse_comma_separated(Parser::parse_expr)?;
                    self.expect_token(&Token::RParen)?;
                    result
                };
                Ok(result)
            } else {
                Ok(vec![self.parse_expr()?])
            }
        } else {
            self.expect_token(&Token::LParen)?;
            let result = if allow_empty && self.consume_token(&Token::RParen) {
                vec![]
            } else {
                let result = self.parse_comma_separated(Parser::parse_expr)?;
                self.expect_token(&Token::RParen)?;
                result
            };
            Ok(result)
        }
    }

    pub fn parse_case_expr(&mut self) -> Result<Expr, ParserError> {
        let mut operand = None;
        if !self.parse_keyword(Keyword::WHEN) {
            operand = Some(Box::new(self.parse_expr()?));
            self.expect_keyword(Keyword::WHEN)?;
        }
        let mut conditions = vec![];
        let mut results = vec![];
        loop {
            conditions.push(self.parse_expr()?);
            self.expect_keyword(Keyword::THEN)?;
            results.push(self.parse_expr()?);
            if !self.parse_keyword(Keyword::WHEN) {
                break;
            }
        }
        let else_result = if self.parse_keyword(Keyword::ELSE) {
            Some(Box::new(self.parse_expr()?))
        } else {
            None
        };
        self.expect_keyword(Keyword::END)?;
        Ok(Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        })
    }

    /// Parse a SQL CAST function e.g. `CAST(expr AS FLOAT)`
    pub fn parse_cast_expr(&mut self) -> Result<Expr, ParserError> {
        self.expect_token(&Token::LParen)?;
        let expr = self.parse_expr()?;
        self.expect_keyword(Keyword::AS)?;
        let data_type = self.parse_data_type()?;
        self.expect_token(&Token::RParen)?;
        Ok(Expr::Cast {
            expr: Box::new(expr),
            data_type,
        })
    }

    /// Parse a SQL TRY_CAST function e.g. `TRY_CAST(expr AS FLOAT)`
    pub fn parse_try_cast_expr(&mut self) -> Result<Expr, ParserError> {
        self.expect_token(&Token::LParen)?;
        let expr = self.parse_expr()?;
        self.expect_keyword(Keyword::AS)?;
        let data_type = self.parse_data_type()?;
        self.expect_token(&Token::RParen)?;
        Ok(Expr::TryCast {
            expr: Box::new(expr),
            data_type,
        })
    }

    /// Parse a SQL EXISTS expression e.g. `WHERE EXISTS(SELECT ...)`.
    pub fn parse_exists_expr(&mut self, negated: bool) -> Result<Expr, ParserError> {
        self.expect_token(&Token::LParen)?;
        let exists_node = Expr::Exists {
            negated,
            subquery: Box::new(self.parse_query()?),
        };
        self.expect_token(&Token::RParen)?;
        Ok(exists_node)
    }

    pub fn parse_extract_expr(&mut self) -> Result<Expr, ParserError> {
        self.expect_token(&Token::LParen)?;
        let field = self.parse_date_time_field()?;
        self.expect_keyword(Keyword::FROM)?;
        let expr = self.parse_expr()?;
        self.expect_token(&Token::RParen)?;
        Ok(Expr::Extract {
            field,
            expr: Box::new(expr),
        })
    }

    pub fn parse_position_expr(&mut self) -> Result<Expr, ParserError> {
        // PARSE SELECT POSITION('@' in field)
        self.expect_token(&Token::LParen)?;

        // Parse the subexpr till the IN keyword
        let expr = self.parse_subexpr(Self::BETWEEN_PREC)?;
        if self.parse_keyword(Keyword::IN) {
            let from = self.parse_expr()?;
            self.expect_token(&Token::RParen)?;
            Ok(Expr::Position {
                expr: Box::new(expr),
                r#in: Box::new(from),
            })
        } else {
            return parser_err!("Position function must include IN keyword".to_string());
        }
    }

    pub fn parse_substring_expr(&mut self) -> Result<Expr, ParserError> {
        // PARSE SUBSTRING (EXPR [FROM 1] [FOR 3])
        self.expect_token(&Token::LParen)?;
        let expr = self.parse_expr()?;
        let mut from_expr = None;
        if self.parse_keyword(Keyword::FROM) || self.consume_token(&Token::Comma) {
            from_expr = Some(self.parse_expr()?);
        }

        let mut to_expr = None;
        if self.parse_keyword(Keyword::FOR) || self.consume_token(&Token::Comma) {
            to_expr = Some(self.parse_expr()?);
        }
        self.expect_token(&Token::RParen)?;

        Ok(Expr::Substring {
            expr: Box::new(expr),
            substring_from: from_expr.map(Box::new),
            substring_for: to_expr.map(Box::new),
        })
    }

    /// TRIM (WHERE 'text' FROM 'text')\
    /// TRIM ('text')
    pub fn parse_trim_expr(&mut self) -> Result<Expr, ParserError> {
        self.expect_token(&Token::LParen)?;
        let mut where_expr = None;
        if let Token::Word(word) = self.peek_token() {
            if [Keyword::BOTH, Keyword::LEADING, Keyword::TRAILING]
                .iter()
                .any(|d| word.keyword == *d)
            {
                let trim_where = self.parse_trim_where()?;
                let sub_expr = self.parse_expr()?;
                self.expect_keyword(Keyword::FROM)?;
                where_expr = Some((trim_where, Box::new(sub_expr)));
            }
        }
        let expr = self.parse_expr()?;
        self.expect_token(&Token::RParen)?;

        Ok(Expr::Trim {
            expr: Box::new(expr),
            trim_where: where_expr,
        })
    }

    pub fn parse_trim_where(&mut self) -> Result<TrimWhereField, ParserError> {
        match self.next_token() {
            Token::Word(w) => match w.keyword {
                Keyword::BOTH => Ok(TrimWhereField::Both),
                Keyword::LEADING => Ok(TrimWhereField::Leading),
                Keyword::TRAILING => Ok(TrimWhereField::Trailing),
                _ => self.expected("trim_where field", Token::Word(w))?,
            },
            unexpected => self.expected("trim_where field", unexpected),
        }
    }

    /// Parses an array expression `[ex1, ex2, ..]`
    /// if `named` is `true`, came from an expression like  `ARRAY[ex1, ex2]`
    pub fn parse_array_expr(&mut self, named: bool) -> Result<Expr, ParserError> {
        if self.peek_token() == Token::RBracket {
            let _ = self.next_token();
            Ok(Expr::Array(Array {
                elem: vec![],
                named,
            }))
        } else {
            let exprs = self.parse_comma_separated(Parser::parse_expr)?;
            self.expect_token(&Token::RBracket)?;
            Ok(Expr::Array(Array { elem: exprs, named }))
        }
    }

    /// Parse a SQL LISTAGG expression, e.g. `LISTAGG(...) WITHIN GROUP (ORDER BY ...)`.
    pub fn parse_listagg_expr(&mut self) -> Result<Expr, ParserError> {
        self.expect_token(&Token::LParen)?;
        let distinct = self.parse_all_or_distinct()?;
        let expr = Box::new(self.parse_expr()?);
        // While ANSI SQL would would require the separator, Redshift makes this optional. Here we
        // choose to make the separator optional as this provides the more general implementation.
        let separator = if self.consume_token(&Token::Comma) {
            Some(Box::new(self.parse_expr()?))
        } else {
            None
        };
        let on_overflow = if self.parse_keywords(&[Keyword::ON, Keyword::OVERFLOW]) {
            if self.parse_keyword(Keyword::ERROR) {
                Some(ListAggOnOverflow::Error)
            } else {
                self.expect_keyword(Keyword::TRUNCATE)?;
                let filler = match self.peek_token() {
                    Token::Word(w)
                        if w.keyword == Keyword::WITH || w.keyword == Keyword::WITHOUT =>
                    {
                        None
                    }
                    Token::SingleQuotedString(_)
                    | Token::EscapedStringLiteral(_)
                    | Token::NationalStringLiteral(_)
                    | Token::HexStringLiteral(_) => Some(Box::new(self.parse_expr()?)),
                    unexpected => {
                        self.expected("either filler, WITH, or WITHOUT in LISTAGG", unexpected)?
                    }
                };
                let with_count = self.parse_keyword(Keyword::WITH);
                if !with_count && !self.parse_keyword(Keyword::WITHOUT) {
                    self.expected("either WITH or WITHOUT in LISTAGG", self.peek_token())?;
                }
                self.expect_keyword(Keyword::COUNT)?;
                Some(ListAggOnOverflow::Truncate { filler, with_count })
            }
        } else {
            None
        };
        self.expect_token(&Token::RParen)?;
        // Once again ANSI SQL requires WITHIN GROUP, but Redshift does not. Again we choose the
        // more general implementation.
        let within_group = if self.parse_keywords(&[Keyword::WITHIN, Keyword::GROUP]) {
            self.expect_token(&Token::LParen)?;
            self.expect_keywords(&[Keyword::ORDER, Keyword::BY])?;
            let order_by_expr = self.parse_comma_separated(Parser::parse_order_by_expr)?;
            self.expect_token(&Token::RParen)?;
            order_by_expr
        } else {
            vec![]
        };
        Ok(Expr::ListAgg(ListAgg {
            distinct,
            expr,
            separator,
            on_overflow,
            within_group,
        }))
    }

    // This function parses date/time fields for both the EXTRACT function-like
    // operator and interval qualifiers. EXTRACT supports a wider set of
    // date/time fields than interval qualifiers, so this function may need to
    // be split in two.
    pub fn parse_date_time_field(&mut self) -> Result<DateTimeField, ParserError> {
        match self.next_token() {
            Token::Word(w) => match w.keyword {
                Keyword::YEAR => Ok(DateTimeField::Year),
                Keyword::MONTH => Ok(DateTimeField::Month),
                Keyword::WEEK => Ok(DateTimeField::Week),
                Keyword::DAY => Ok(DateTimeField::Day),
                Keyword::HOUR => Ok(DateTimeField::Hour),
                Keyword::MINUTE => Ok(DateTimeField::Minute),
                Keyword::SECOND => Ok(DateTimeField::Second),
                Keyword::CENTURY => Ok(DateTimeField::Century),
                Keyword::DECADE => Ok(DateTimeField::Decade),
                Keyword::DOY => Ok(DateTimeField::Doy),
                Keyword::DOW => Ok(DateTimeField::Dow),
                Keyword::EPOCH => Ok(DateTimeField::Epoch),
                Keyword::ISODOW => Ok(DateTimeField::Isodow),
                Keyword::ISOYEAR => Ok(DateTimeField::Isoyear),
                Keyword::JULIAN => Ok(DateTimeField::Julian),
                Keyword::MICROSECONDS => Ok(DateTimeField::Microseconds),
                Keyword::MILLENIUM => Ok(DateTimeField::Millenium),
                Keyword::MILLISECONDS => Ok(DateTimeField::Milliseconds),
                Keyword::QUARTER => Ok(DateTimeField::Quarter),
                Keyword::TIMEZONE => Ok(DateTimeField::Timezone),
                Keyword::TIMEZONE_HOUR => Ok(DateTimeField::TimezoneHour),
                Keyword::TIMEZONE_MINUTE => Ok(DateTimeField::TimezoneMinute),
                _ => self.expected("date/time field", Token::Word(w))?,
            },
            unexpected => self.expected("date/time field", unexpected),
        }
    }

    pub fn parse_not(&mut self) -> Result<Expr, ParserError> {
        match self.peek_token() {
            Token::Word(w) => match w.keyword {
                Keyword::EXISTS => {
                    let negated = true;
                    let _ = self.parse_keyword(Keyword::EXISTS);
                    self.parse_exists_expr(negated)
                }
                _ => Ok(Expr::UnaryOp {
                    op: UnaryOperator::Not,
                    expr: Box::new(self.parse_subexpr(Self::UNARY_NOT_PREC)?),
                }),
            },
            _ => Ok(Expr::UnaryOp {
                op: UnaryOperator::Not,
                expr: Box::new(self.parse_subexpr(Self::UNARY_NOT_PREC)?),
            }),
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
    ///   7. (MySql and BigQuey only):`INTERVAL 1 DAY`
    ///
    /// Note that we do not currently attempt to parse the quoted value.
    pub fn parse_literal_interval(&mut self) -> Result<Expr, ParserError> {
        // The SQL standard allows an optional sign before the value string, but
        // it is not clear if any implementations support that syntax, so we
        // don't currently try to parse it. (The sign can instead be included
        // inside the value string.)

        // The first token in an interval is a string literal which specifies
        // the duration of the interval.
        let value = self.parse_expr()?;

        // Following the string literal is a qualifier which indicates the units
        // of the duration specified in the string literal.
        //
        // Note that PostgreSQL allows omitting the qualifier, so we provide
        // this more general implementation.
        let leading_field = match self.peek_token() {
            Token::Word(kw)
                if [
                    Keyword::YEAR,
                    Keyword::MONTH,
                    Keyword::WEEK,
                    Keyword::DAY,
                    Keyword::HOUR,
                    Keyword::MINUTE,
                    Keyword::SECOND,
                    Keyword::CENTURY,
                    Keyword::DECADE,
                    Keyword::DOW,
                    Keyword::DOY,
                    Keyword::EPOCH,
                    Keyword::ISODOW,
                    Keyword::ISOYEAR,
                    Keyword::JULIAN,
                    Keyword::MICROSECONDS,
                    Keyword::MILLENIUM,
                    Keyword::MILLISECONDS,
                    Keyword::QUARTER,
                    Keyword::TIMEZONE,
                    Keyword::TIMEZONE_HOUR,
                    Keyword::TIMEZONE_MINUTE,
                ]
                .iter()
                .any(|d| kw.keyword == *d) =>
            {
                Some(self.parse_date_time_field()?)
            }
            _ => None,
        };

        let (leading_precision, last_field, fsec_precision) =
            if leading_field == Some(DateTimeField::Second) {
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
                if self.parse_keyword(Keyword::TO) {
                    let last_field = Some(self.parse_date_time_field()?);
                    let fsec_precision = if last_field == Some(DateTimeField::Second) {
                        self.parse_optional_precision()?
                    } else {
                        None
                    };
                    (leading_precision, last_field, fsec_precision)
                } else {
                    (leading_precision, None, None)
                }
            };

        Ok(Expr::Value(Value::Interval {
            value: Box::new(value),
            leading_field,
            leading_precision,
            last_field,
            fractional_seconds_precision: fsec_precision,
        }))
    }

    /// Parse an operator following an expression
    pub fn parse_infix(&mut self, expr: Expr, precedence: u8) -> Result<Expr, ParserError> {
        let tok = self.next_token();

        let regular_binary_operator = match &tok {
            Token::Spaceship => Some(BinaryOperator::Spaceship),
            Token::DoubleEq => Some(BinaryOperator::Eq),
            Token::Eq => Some(BinaryOperator::Eq),
            Token::Neq => Some(BinaryOperator::NotEq),
            Token::Gt => Some(BinaryOperator::Gt),
            Token::GtEq => Some(BinaryOperator::GtEq),
            Token::Lt => Some(BinaryOperator::Lt),
            Token::LtEq => Some(BinaryOperator::LtEq),
            Token::Plus => Some(BinaryOperator::Plus),
            Token::Minus => Some(BinaryOperator::Minus),
            Token::Mul => Some(BinaryOperator::Multiply),
            Token::Mod => Some(BinaryOperator::Modulo),
            Token::StringConcat => Some(BinaryOperator::StringConcat),
            Token::Pipe => Some(BinaryOperator::BitwiseOr),
            Token::Caret => Some(BinaryOperator::BitwiseXor),
            Token::Ampersand => Some(BinaryOperator::BitwiseAnd),
            Token::Div => Some(BinaryOperator::Divide),
            Token::Tilde => Some(BinaryOperator::PGRegexMatch),
            Token::TildeAsterisk => Some(BinaryOperator::PGRegexIMatch),
            Token::ExclamationMarkTilde => Some(BinaryOperator::PGRegexNotMatch),
            Token::ExclamationMarkTildeAsterisk => Some(BinaryOperator::PGRegexNotIMatch),
            Token::Word(w) => match w.keyword {
                Keyword::AND => Some(BinaryOperator::And),
                Keyword::OR => Some(BinaryOperator::Or),
                Keyword::LIKE => Some(BinaryOperator::Like),
                Keyword::ILIKE => Some(BinaryOperator::ILike),
                Keyword::NOT => {
                    if self.parse_keyword(Keyword::LIKE) {
                        Some(BinaryOperator::NotLike)
                    } else if self.parse_keyword(Keyword::ILIKE) {
                        Some(BinaryOperator::NotILike)
                    } else {
                        None
                    }
                }
                Keyword::XOR => Some(BinaryOperator::Xor),
                _ => None,
            },
            _ => None,
        };

        if let Some(op) = regular_binary_operator {
            if let Some(keyword) = self.parse_one_of_keywords(&[Keyword::ANY, Keyword::ALL]) {
                self.expect_token(&Token::LParen)?;
                let right = self.parse_subexpr(precedence)?;
                self.expect_token(&Token::RParen)?;

                let right = match keyword {
                    Keyword::ALL => Box::new(Expr::AllOp(Box::new(right))),
                    Keyword::ANY => Box::new(Expr::AnyOp(Box::new(right))),
                    _ => unreachable!(),
                };

                Ok(Expr::BinaryOp {
                    left: Box::new(expr),
                    op,
                    right,
                })
            } else {
                Ok(Expr::BinaryOp {
                    left: Box::new(expr),
                    op,
                    right: Box::new(self.parse_subexpr(precedence)?),
                })
            }
        } else if let Token::Word(w) = &tok {
            match w.keyword {
                Keyword::IS => {
                    if self.parse_keyword(Keyword::NULL) {
                        Ok(Expr::IsNull(Box::new(expr)))
                    } else if self.parse_keywords(&[Keyword::NOT, Keyword::NULL]) {
                        Ok(Expr::IsNotNull(Box::new(expr)))
                    } else if self.parse_keywords(&[Keyword::TRUE]) {
                        Ok(Expr::IsTrue(Box::new(expr)))
                    } else if self.parse_keywords(&[Keyword::FALSE]) {
                        Ok(Expr::IsFalse(Box::new(expr)))
                    } else if self.parse_keywords(&[Keyword::DISTINCT, Keyword::FROM]) {
                        let expr2 = self.parse_expr()?;
                        Ok(Expr::IsDistinctFrom(Box::new(expr), Box::new(expr2)))
                    } else if self.parse_keywords(&[Keyword::NOT, Keyword::DISTINCT, Keyword::FROM])
                    {
                        let expr2 = self.parse_expr()?;
                        Ok(Expr::IsNotDistinctFrom(Box::new(expr), Box::new(expr2)))
                    } else {
                        self.expected(
                            "[NOT] NULL or TRUE|FALSE or [NOT] DISTINCT FROM after IS",
                            self.peek_token(),
                        )
                    }
                }
                Keyword::AT => {
                    // if self.parse_keyword(Keyword::TIME) {
                    //     self.expect_keyword(Keyword::ZONE)?;
                    if self.parse_keywords(&[Keyword::TIME, Keyword::ZONE]) {
                        let time_zone = self.next_token();
                        match time_zone {
                            Token::SingleQuotedString(time_zone) => {
                                log::trace!("Peek token: {:?}", self.peek_token());
                                Ok(Expr::AtTimeZone {
                                    timestamp: Box::new(expr),
                                    time_zone,
                                })
                            }
                            tok => self.expected(
                                "Expected Token::SingleQuotedString after AT TIME ZONE",
                                tok,
                            ),
                        }
                    } else {
                        self.expected("Expected Token::Word after AT", tok)
                    }
                }
                Keyword::NOT | Keyword::IN | Keyword::BETWEEN => {
                    self.prev_token();
                    let negated = self.parse_keyword(Keyword::NOT);
                    if self.parse_keyword(Keyword::IN) {
                        self.parse_in(expr, negated)
                    } else if self.parse_keyword(Keyword::BETWEEN) {
                        self.parse_between(expr, negated)
                    } else {
                        self.expected("IN or BETWEEN after NOT", self.peek_token())
                    }
                }
                // Can only happen if `get_next_precedence` got out of sync with this function
                _ => parser_err!(format!("No infix parser for token {:?}", tok)),
            }
        } else if Token::DoubleColon == tok {
            self.parse_pg_cast(expr)
        } else if Token::ExclamationMark == tok {
            // PostgreSQL factorial operation
            Ok(Expr::UnaryOp {
                op: UnaryOperator::PGPostfixFactorial,
                expr: Box::new(expr),
            })
        } else if Token::LBracket == tok {
            self.parse_array_index(expr)
        } else if Token::Arrow == tok
            || Token::LongArrow == tok
            || Token::HashArrow == tok
            || Token::HashLongArrow == tok
        {
            let operator = match tok {
                Token::Arrow => JsonOperator::Arrow,
                Token::LongArrow => JsonOperator::LongArrow,
                Token::HashArrow => JsonOperator::HashArrow,
                Token::HashLongArrow => JsonOperator::HashLongArrow,
                _ => unreachable!(),
            };
            Ok(Expr::JsonAccess {
                left: Box::new(expr),
                operator,
                right: Box::new(self.parse_expr()?),
            })
        } else {
            // Can only happen if `get_next_precedence` got out of sync with this function
            parser_err!(format!("No infix parser for token {:?}", tok))
        }
    }

    pub fn parse_array_index(&mut self, expr: Expr) -> Result<Expr, ParserError> {
        let index = self.parse_expr()?;
        self.expect_token(&Token::RBracket)?;
        let mut indexes: Vec<Expr> = vec![index];
        while self.consume_token(&Token::LBracket) {
            let index = self.parse_expr()?;
            self.expect_token(&Token::RBracket)?;
            indexes.push(index);
        }
        Ok(Expr::ArrayIndex {
            obj: Box::new(expr),
            indexes,
        })
    }

    pub fn parse_map_access(&mut self, expr: Expr) -> Result<Expr, ParserError> {
        let key = self.parse_map_key()?;
        let tok = self.consume_token(&Token::RBracket);
        debug!("Tok: {}", tok);
        let mut key_parts: Vec<Expr> = vec![key];
        while self.consume_token(&Token::LBracket) {
            let key = self.parse_map_key()?;
            let tok = self.consume_token(&Token::RBracket);
            debug!("Tok: {}", tok);
            key_parts.push(key);
        }
        match expr {
            e @ Expr::Identifier(_) | e @ Expr::CompoundIdentifier(_) => Ok(Expr::MapAccess {
                column: Box::new(e),
                keys: key_parts,
            }),
            _ => Ok(expr),
        }
    }

    /// Parses the parens following the `[ NOT ] IN` operator
    pub fn parse_in(&mut self, expr: Expr, negated: bool) -> Result<Expr, ParserError> {
        // BigQuery allows `IN UNNEST(array_expression)`
        // https://cloud.google.com/bigquery/docs/reference/standard-sql/operators#in_operators
        if self.parse_keyword(Keyword::UNNEST) {
            self.expect_token(&Token::LParen)?;
            let array_expr = self.parse_expr()?;
            self.expect_token(&Token::RParen)?;
            return Ok(Expr::InUnnest {
                expr: Box::new(expr),
                array_expr: Box::new(array_expr),
                negated,
            });
        }
        self.expect_token(&Token::LParen)?;
        let in_op = if self.parse_keyword(Keyword::SELECT) || self.parse_keyword(Keyword::WITH) {
            self.prev_token();
            Expr::InSubquery {
                expr: Box::new(expr),
                subquery: Box::new(self.parse_query()?),
                negated,
            }
        } else {
            Expr::InList {
                expr: Box::new(expr),
                list: self.parse_comma_separated(Parser::parse_expr)?,
                negated,
            }
        };
        self.expect_token(&Token::RParen)?;
        Ok(in_op)
    }

    /// Parses `BETWEEN <low> AND <high>`, assuming the `BETWEEN` keyword was already consumed
    pub fn parse_between(&mut self, expr: Expr, negated: bool) -> Result<Expr, ParserError> {
        // Stop parsing subexpressions for <low> and <high> on tokens with
        // precedence lower than that of `BETWEEN`, such as `AND`, `IS`, etc.
        let low = self.parse_subexpr(Self::BETWEEN_PREC)?;
        self.expect_keyword(Keyword::AND)?;
        let high = self.parse_subexpr(Self::BETWEEN_PREC)?;
        Ok(Expr::Between {
            expr: Box::new(expr),
            negated,
            low: Box::new(low),
            high: Box::new(high),
        })
    }

    /// Parse a postgresql casting style which is in the form of `expr::datatype`
    pub fn parse_pg_cast(&mut self, expr: Expr) -> Result<Expr, ParserError> {
        Ok(Expr::Cast {
            expr: Box::new(expr),
            data_type: self.parse_data_type()?,
        })
    }

    const UNARY_NOT_PREC: u8 = 15;
    const BETWEEN_PREC: u8 = 20;
    const PLUS_MINUS_PREC: u8 = 30;
    const TIME_ZONE_PREC: u8 = 20;

    /// Get the precedence of the next token
    pub fn get_next_precedence(&self) -> Result<u8, ParserError> {
        let token = self.peek_token();
        debug!("get_next_precedence() {:?}", token);
        let token_0 = self.peek_nth_token(0);
        let token_1 = self.peek_nth_token(1);
        let token_2 = self.peek_nth_token(2);
        debug!("0: {token_0} 1: {token_1} 2: {token_2}");
        match token {
            Token::Word(w) if w.keyword == Keyword::OR => Ok(5),
            Token::Word(w) if w.keyword == Keyword::AND => Ok(10),
            Token::Word(w) if w.keyword == Keyword::XOR => Ok(24),

            Token::Word(w) if w.keyword == Keyword::AT => {
                match (self.peek_nth_token(1), self.peek_nth_token(2)) {
                    (Token::Word(w), Token::Word(w2))
                        if w.keyword == Keyword::TIME && w2.keyword == Keyword::ZONE =>
                    {
                        Ok(Self::TIME_ZONE_PREC)
                    }
                    _ => Ok(0),
                }
            }

            Token::Word(w) if w.keyword == Keyword::NOT => match self.peek_nth_token(1) {
                // The precedence of NOT varies depending on keyword that
                // follows it. If it is followed by IN, BETWEEN, or LIKE,
                // it takes on the precedence of those tokens. Otherwise it
                // is not an infix operator, and therefore has zero
                // precedence.
                Token::Word(w) if w.keyword == Keyword::IN => Ok(Self::BETWEEN_PREC),
                Token::Word(w) if w.keyword == Keyword::BETWEEN => Ok(Self::BETWEEN_PREC),
                Token::Word(w) if w.keyword == Keyword::LIKE => Ok(Self::BETWEEN_PREC),
                Token::Word(w) if w.keyword == Keyword::ILIKE => Ok(Self::BETWEEN_PREC),
                _ => Ok(0),
            },
            Token::Word(w) if w.keyword == Keyword::IS => Ok(17),
            Token::Word(w) if w.keyword == Keyword::IN => Ok(Self::BETWEEN_PREC),
            Token::Word(w) if w.keyword == Keyword::BETWEEN => Ok(Self::BETWEEN_PREC),
            Token::Word(w) if w.keyword == Keyword::LIKE => Ok(Self::BETWEEN_PREC),
            Token::Word(w) if w.keyword == Keyword::ILIKE => Ok(Self::BETWEEN_PREC),
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
            | Token::Spaceship => Ok(20),
            Token::Pipe => Ok(21),
            Token::Caret | Token::Sharp | Token::ShiftRight | Token::ShiftLeft => Ok(22),
            Token::Ampersand => Ok(23),
            Token::Plus | Token::Minus => Ok(Self::PLUS_MINUS_PREC),
            Token::Mul | Token::Div | Token::Mod | Token::StringConcat => Ok(40),
            Token::DoubleColon => Ok(50),
            Token::ExclamationMark => Ok(50),
            Token::LBracket
            | Token::LongArrow
            | Token::Arrow
            | Token::HashArrow
            | Token::HashLongArrow => Ok(50),
            _ => Ok(0),
        }
    }

    /// Return the first non-whitespace token that has not yet been processed
    /// (or None if reached end-of-file)
    pub fn peek_token(&self) -> Token {
        self.peek_nth_token(0)
    }

    /// Return nth non-whitespace token that has not yet been processed
    pub fn peek_nth_token(&self, mut n: usize) -> Token {
        let mut index = self.index;
        loop {
            index += 1;
            match self.tokens.get(index - 1) {
                Some(Token::Whitespace(_)) => continue,
                non_whitespace => {
                    if n == 0 {
                        return non_whitespace.cloned().unwrap_or(Token::EOF);
                    }
                    n -= 1;
                }
            }
        }
    }

    /// Return the first non-whitespace token that has not yet been processed
    /// (or None if reached end-of-file) and mark it as processed. OK to call
    /// repeatedly after reaching EOF.
    pub fn next_token(&mut self) -> Token {
        loop {
            self.index += 1;
            match self.tokens.get(self.index - 1) {
                Some(Token::Whitespace(_)) => continue,
                token => return token.cloned().unwrap_or(Token::EOF),
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
    fn expected<T>(&self, expected: &str, found: Token) -> Result<T, ParserError> {
        parser_err!(format!("Expected {}, found: {}", expected, found))
    }

    /// Look for an expected keyword and consume it if it exists
    #[must_use]
    pub fn parse_keyword(&mut self, expected: Keyword) -> bool {
        match self.peek_token() {
            Token::Word(w) if expected == w.keyword => {
                self.next_token();
                true
            }
            _ => false,
        }
    }

    /// Look for an expected sequence of keywords and consume them if they exist
    #[must_use]
    pub fn parse_keywords(&mut self, keywords: &[Keyword]) -> bool {
        let index = self.index;
        for &keyword in keywords {
            if !self.parse_keyword(keyword) {
                // println!("parse_keywords aborting .. did not find {:?}", keyword);
                // reset index and return immediately
                self.index = index;
                return false;
            }
        }
        true
    }

    /// Look for one of the given keywords and return the one that matches.
    #[must_use]
    pub fn parse_one_of_keywords(&mut self, keywords: &[Keyword]) -> Option<Keyword> {
        match self.peek_token() {
            Token::Word(w) => {
                keywords
                    .iter()
                    .find(|keyword| **keyword == w.keyword)
                    .map(|keyword| {
                        self.next_token();
                        *keyword
                    })
            }
            _ => None,
        }
    }

    /// Bail out if the current token is not one of the expected keywords, or consume it if it is
    pub fn expect_one_of_keywords(&mut self, keywords: &[Keyword]) -> Result<Keyword, ParserError> {
        if let Some(keyword) = self.parse_one_of_keywords(keywords) {
            Ok(keyword)
        } else {
            let keywords: Vec<String> = keywords.iter().map(|x| format!("{:?}", x)).collect();
            self.expected(
                &format!("one of {}", keywords.join(" or ")),
                self.peek_token(),
            )
        }
    }

    /// Bail out if the current token is not an expected keyword, or consume it if it is
    pub fn expect_keyword(&mut self, expected: Keyword) -> Result<(), ParserError> {
        if self.parse_keyword(expected) {
            Ok(())
        } else {
            self.expected(format!("{:?}", &expected).as_str(), self.peek_token())
        }
    }

    /// Bail out if the following tokens are not the expected sequence of
    /// keywords, or consume them if they are.
    pub fn expect_keywords(&mut self, expected: &[Keyword]) -> Result<(), ParserError> {
        for &kw in expected {
            self.expect_keyword(kw)?;
        }
        Ok(())
    }

    /// Consume the next token if it matches the expected token, otherwise return false
    #[must_use]
    pub fn consume_token(&mut self, expected: &Token) -> bool {
        if self.peek_token() == *expected {
            self.next_token();
            true
        } else {
            false
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

    /// Parse a comma-separated list of 1+ items accepted by `F`
    pub fn parse_comma_separated<T, F>(&mut self, mut f: F) -> Result<Vec<T>, ParserError>
    where
        F: FnMut(&mut Parser) -> Result<T, ParserError>,
    {
        let mut values = vec![];
        loop {
            values.push(f(self)?);
            if !self.consume_token(&Token::Comma) {
                break;
            }
        }
        Ok(values)
    }

    /// Run a parser method `f`, reverting back to the current position
    /// if unsuccessful.
    #[must_use]
    fn maybe_parse<T, F>(&mut self, mut f: F) -> Option<T>
    where
        F: FnMut(&mut Parser) -> Result<T, ParserError>,
    {
        let index = self.index;
        if let Ok(t) = f(self) {
            Some(t)
        } else {
            self.index = index;
            None
        }
    }

    /// Parse either `ALL` or `DISTINCT`. Returns `true` if `DISTINCT` is parsed and results in a
    /// `ParserError` if both `ALL` and `DISTINCT` are fround.
    pub fn parse_all_or_distinct(&mut self) -> Result<bool, ParserError> {
        let all = self.parse_keyword(Keyword::ALL);
        let distinct = self.parse_keyword(Keyword::DISTINCT);
        if all && distinct {
            return parser_err!("Cannot specify both ALL and DISTINCT".to_string());
        } else {
            Ok(distinct)
        }
    }

    pub fn parse_file_format(&mut self) -> Result<FileFormat, ParserError> {
        match self.next_token() {
            Token::Word(w) => match w.keyword {
                Keyword::AVRO => Ok(FileFormat::AVRO),
                Keyword::JSONFILE => Ok(FileFormat::JSONFILE),
                Keyword::ORC => Ok(FileFormat::ORC),
                Keyword::PARQUET => Ok(FileFormat::PARQUET),
                Keyword::RCFILE => Ok(FileFormat::RCFILE),
                Keyword::SEQUENCEFILE => Ok(FileFormat::SEQUENCEFILE),
                Keyword::TEXTFILE => Ok(FileFormat::TEXTFILE),
                _ => self.expected("fileformat", Token::Word(w)),
            },
            unexpected => self.expected("fileformat", unexpected),
        }
    }

    pub fn parse_options(&mut self, keyword: Keyword) -> Result<Vec<SqlOption>, ParserError> {
        if self.parse_keyword(keyword) {
            self.expect_token(&Token::LParen)?;
            let options = self.parse_comma_separated(Parser::parse_sql_option)?;
            self.expect_token(&Token::RParen)?;
            Ok(options)
        } else {
            Ok(vec![])
        }
    }

    pub fn parse_sql_option(&mut self) -> Result<SqlOption, ParserError> {
        let name = self.parse_identifier()?;
        self.expect_token(&Token::Eq)?;
        let value = self.parse_value()?;
        Ok(SqlOption { name, value })
    }

    /// Parse a tab separated values in
    /// COPY payload
    pub fn parse_tsv(&mut self) -> Vec<Option<String>> {
        self.parse_tab_value()
    }

    pub fn parse_tab_value(&mut self) -> Vec<Option<String>> {
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
                        return values;
                    }
                    if let Token::Word(w) = self.next_token() {
                        if w.value == "N" {
                            values.push(None);
                        }
                    }
                }
                _ => {
                    content.push_str(&t.to_string());
                }
            }
        }
        values
    }

    /// Parse a literal value (numbers, strings, date/time, booleans)
    pub fn parse_value(&mut self) -> Result<Value, ParserError> {
        match self.next_token() {
            Token::Word(w) => match w.keyword {
                Keyword::TRUE => Ok(Value::Boolean(true)),
                Keyword::FALSE => Ok(Value::Boolean(false)),
                Keyword::NULL => Ok(Value::Null),
                Keyword::NoKeyword if w.quote_style.is_some() => match w.quote_style {
                    Some('"') => Ok(Value::DoubleQuotedString(w.value)),
                    Some('\'') => Ok(Value::SingleQuotedString(w.value)),
                    _ => self.expected("A value?", Token::Word(w))?,
                },
                _ => self.expected("a concrete value", Token::Word(w)),
            },
            // The call to n.parse() returns a bigdecimal when the
            // bigdecimal feature is enabled, and is otherwise a no-op
            // (i.e., it returns the input string).
            Token::Number(ref n, l) => match n.parse() {
                Ok(n) => Ok(Value::Number(n, l)),
                Err(e) => parser_err!(format!("Could not parse '{}' as number: {}", n, e)),
            },
            Token::SingleQuotedString(ref s) => Ok(Value::SingleQuotedString(s.to_string())),
            Token::DoubleQuotedString(ref s) => Ok(Value::DoubleQuotedString(s.to_string())),
            Token::NationalStringLiteral(ref s) => Ok(Value::NationalStringLiteral(s.to_string())),
            Token::EscapedStringLiteral(ref s) => Ok(Value::EscapedStringLiteral(s.to_string())),
            Token::HexStringLiteral(ref s) => Ok(Value::HexStringLiteral(s.to_string())),
            Token::Placeholder(ref s) => Ok(Value::Placeholder(s.to_string())),
            unexpected => self.expected("a value", unexpected),
        }
    }

    pub fn parse_number_value(&mut self) -> Result<Value, ParserError> {
        match self.parse_value()? {
            v @ Value::Number(_, _) => Ok(v),
            v @ Value::Placeholder(_) => Ok(v),
            _ => {
                self.prev_token();
                self.expected("literal number", self.peek_token())
            }
        }
    }

    /// Parse an unsigned literal integer/long
    pub fn parse_literal_uint(&mut self) -> Result<u64, ParserError> {
        match self.next_token() {
            Token::Number(s, _) => s.parse::<u64>().map_err(|e| {
                ParserError::ParserError(format!("Could not parse '{}' as u64: {}", s, e))
            }),
            unexpected => self.expected("literal int", unexpected),
        }
    }

    /// Parse a literal string
    pub fn parse_literal_string(&mut self) -> Result<String, ParserError> {
        match self.next_token() {
            Token::Word(Word { value, keyword, .. }) if keyword == Keyword::NoKeyword => Ok(value),
            Token::SingleQuotedString(s) => Ok(s),
            Token::DoubleQuotedString(s) => Ok(s),
            Token::EscapedStringLiteral(s) => Ok(s),
            unexpected => self.expected("literal string", unexpected),
        }
    }

    /// Parse a map key string
    pub fn parse_map_key(&mut self) -> Result<Expr, ParserError> {
        match self.next_token() {
            Token::Word(Word { value, keyword, .. }) if keyword == Keyword::NoKeyword => {
                if self.peek_token() == Token::LParen {
                    return self.parse_function(ObjectName(vec![Ident::new(value)]));
                }
                Ok(Expr::Value(Value::SingleQuotedString(value)))
            }
            Token::SingleQuotedString(s) => Ok(Expr::Value(Value::SingleQuotedString(s))),
            #[cfg(not(feature = "bigdecimal"))]
            Token::Number(s, _) => Ok(Expr::Value(Value::Number(s, false))),
            #[cfg(feature = "bigdecimal")]
            Token::Number(s, _) => Ok(Expr::Value(Value::Number(s.parse().unwrap(), false))),
            unexpected => self.expected("literal string, number or function", unexpected),
        }
    }

    /// Parse a SQL datatype (in the context of a CREATE TABLE statement for example)
    pub fn parse_data_type(&mut self) -> Result<DataType, ParserError> {
        let mut data = match self.next_token() {
            Token::Word(w) => match w.keyword {
                Keyword::BOOLEAN => Ok(DataType::Boolean),
                Keyword::FLOAT => Ok(DataType::Float(self.parse_optional_precision()?)),
                Keyword::REAL => Ok(DataType::Real),
                Keyword::DOUBLE => {
                    let _ = self.parse_keyword(Keyword::PRECISION);
                    Ok(DataType::Double)
                }
                Keyword::TINYINT => {
                    let optional_precision = self.parse_optional_precision();
                    if self.parse_keyword(Keyword::UNSIGNED) {
                        Ok(DataType::UnsignedTinyInt(optional_precision?))
                    } else {
                        Ok(DataType::TinyInt(optional_precision?))
                    }
                }
                Keyword::SMALLINT => {
                    let optional_precision = self.parse_optional_precision();
                    if self.parse_keyword(Keyword::UNSIGNED) {
                        Ok(DataType::UnsignedSmallInt(optional_precision?))
                    } else {
                        Ok(DataType::SmallInt(optional_precision?))
                    }
                }
                Keyword::INT => {
                    let optional_precision = self.parse_optional_precision();
                    if self.parse_keyword(Keyword::UNSIGNED) {
                        Ok(DataType::UnsignedInt(optional_precision?))
                    } else {
                        Ok(DataType::Int(optional_precision?))
                    }
                }
                Keyword::INTEGER => {
                    let optional_precision = self.parse_optional_precision();
                    if self.parse_keyword(Keyword::UNSIGNED) {
                        Ok(DataType::UnsignedInteger(optional_precision?))
                    } else {
                        Ok(DataType::Integer(optional_precision?))
                    }
                }
                Keyword::BIGINT => {
                    let optional_precision = self.parse_optional_precision();
                    if self.parse_keyword(Keyword::UNSIGNED) {
                        Ok(DataType::UnsignedBigInt(optional_precision?))
                    } else {
                        Ok(DataType::BigInt(optional_precision?))
                    }
                }
                Keyword::VARCHAR => Ok(DataType::Varchar(self.parse_optional_precision()?)),
                Keyword::NVARCHAR => Ok(DataType::Nvarchar(self.parse_optional_precision()?)),
                Keyword::CHAR | Keyword::CHARACTER => {
                    if self.parse_keyword(Keyword::VARYING) {
                        Ok(DataType::Varchar(self.parse_optional_precision()?))
                    } else {
                        Ok(DataType::Char(self.parse_optional_precision()?))
                    }
                }
                Keyword::UUID => Ok(DataType::Uuid),
                Keyword::DATE => Ok(DataType::Date),
                Keyword::DATETIME => Ok(DataType::Datetime),
                Keyword::TIMESTAMP => {
                    // TBD: we throw away "with/without timezone" information
                    if self.parse_keyword(Keyword::WITH) || self.parse_keyword(Keyword::WITHOUT) {
                        self.expect_keywords(&[Keyword::TIME, Keyword::ZONE])?;
                    }
                    Ok(DataType::Timestamp)
                }
                Keyword::TIME => {
                    // TBD: we throw away "with/without timezone" information
                    if self.parse_keyword(Keyword::WITH) || self.parse_keyword(Keyword::WITHOUT) {
                        self.expect_keywords(&[Keyword::TIME, Keyword::ZONE])?;
                    }
                    Ok(DataType::Time)
                }
                // Interval types can be followed by a complicated interval
                // qualifier that we don't currently support. See
                // parse_interval_literal for a taste.
                Keyword::INTERVAL => Ok(DataType::Interval),
                Keyword::REGCLASS => Ok(DataType::Regclass),
                Keyword::STRING => Ok(DataType::String),
                Keyword::TEXT => Ok(DataType::Text),
                Keyword::BYTEA => Ok(DataType::Bytea),
                Keyword::NUMERIC | Keyword::DECIMAL | Keyword::DEC => {
                    let (precision, scale) = self.parse_optional_precision_scale()?;
                    Ok(DataType::Decimal(precision, scale))
                }
                Keyword::ENUM => Ok(DataType::Enum(self.parse_string_values()?)),
                Keyword::SET => Ok(DataType::Set(self.parse_string_values()?)),
                Keyword::ARRAY => {
                    // Hive array syntax. Note that nesting arrays - or other Hive syntax
                    // that ends with > will fail due to "C++" problem - >> is parsed as
                    // Token::ShiftRight
                    self.expect_token(&Token::Lt)?;
                    let inside_type = self.parse_data_type()?;
                    self.expect_token(&Token::Gt)?;
                    Ok(DataType::Array(Box::new(inside_type)))
                }
                _ => {
                    self.prev_token();
                    let type_name = self.parse_object_name()?;
                    Ok(DataType::Custom(type_name))
                }
            },
            unexpected => self.expected("a data type name", unexpected),
        }?;

        // Parse array data types. Note: this is postgresql-specific and different from
        // Keyword::ARRAY syntax from above
        while self.consume_token(&Token::LBracket) {
            self.expect_token(&Token::RBracket)?;
            data = DataType::Array(Box::new(data))
        }
        Ok(data)
    }

    pub fn parse_string_values(&mut self) -> Result<Vec<String>, ParserError> {
        self.expect_token(&Token::LParen)?;
        let mut values = Vec::new();
        loop {
            match self.next_token() {
                Token::SingleQuotedString(value) => values.push(value),
                unexpected => self.expected("a string", unexpected)?,
            }
            match self.next_token() {
                Token::Comma => (),
                Token::RParen => break,
                unexpected => self.expected(", or }", unexpected)?,
            }
        }
        Ok(values)
    }

    /// Parse `AS identifier` (or simply `identifier` if it's not a reserved keyword)
    /// Some examples with aliases: `SELECT 1 foo`, `SELECT COUNT(*) AS cnt`,
    /// `SELECT ... FROM t1 foo, t2 bar`, `SELECT ... FROM (...) AS bar`
    pub fn parse_optional_alias(
        &mut self,
        reserved_kwds: &[Keyword],
    ) -> Result<Option<Ident>, ParserError> {
        let after_as = self.parse_keyword(Keyword::AS);
        match self.next_token() {
            // Accept any identifier after `AS` (though many dialects have restrictions on
            // keywords that may appear here). If there's no `AS`: don't parse keywords,
            // which may start a construct allowed in this position, to be parsed as aliases.
            // (For example, in `FROM t1 JOIN` the `JOIN` will always be parsed as a keyword,
            // not an alias.)
            Token::Word(w) if after_as || !reserved_kwds.contains(&w.keyword) => {
                Ok(Some(w.to_ident()))
            }
            // MSSQL supports single-quoted strings as aliases for columns
            // We accept them as table aliases too, although MSSQL does not.
            //
            // Note, that this conflicts with an obscure rule from the SQL
            // standard, which we don't implement:
            // https://crate.io/docs/sql-99/en/latest/chapters/07.html#character-string-literal-s
            //    "[Obscure Rule] SQL allows you to break a long <character
            //    string literal> up into two or more smaller <character string
            //    literal>s, split by a <separator> that includes a newline
            //    character. When it sees such a <literal>, your DBMS will
            //    ignore the <separator> and treat the multiple strings as
            //    a single <literal>."
            Token::SingleQuotedString(s) => Ok(Some(Ident::with_quote('\'', s))),
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
        reserved_kwds: &[Keyword],
    ) -> Result<Option<TableAlias>, ParserError> {
        match self.parse_optional_alias(reserved_kwds)? {
            Some(name) => {
                let columns = self.parse_parenthesized_column_list(Optional)?;
                Ok(Some(TableAlias { name, columns }))
            }
            None => Ok(None),
        }
    }

    /// Parse a possibly qualified, possibly quoted identifier, e.g.
    /// `foo` or `myschema."table"
    pub fn parse_object_name(&mut self) -> Result<ObjectName, ParserError> {
        let mut idents = vec![];
        loop {
            idents.push(self.parse_identifier()?);
            if !self.consume_token(&Token::Period) {
                break;
            }
        }
        Ok(ObjectName(idents))
    }

    /// Parse identifiers strictly i.e. don't parse keywords
    pub fn parse_identifiers_non_keywords(&mut self) -> Result<Vec<Ident>, ParserError> {
        let mut idents = vec![];
        loop {
            match self.peek_token() {
                Token::Word(w) => {
                    if w.keyword != Keyword::NoKeyword {
                        break;
                    }

                    idents.push(w.to_ident());
                }
                Token::EOF | Token::Eq => break,
                _ => {}
            }

            self.next_token();
        }

        Ok(idents)
    }

    /// Parse identifiers
    pub fn parse_identifiers(&mut self) -> Result<Vec<Ident>, ParserError> {
        let mut idents = vec![];
        loop {
            match self.next_token() {
                Token::Word(w) => {
                    idents.push(w.to_ident());
                }
                Token::EOF => break,
                _ => {}
            }
        }

        Ok(idents)
    }

    /// Parse a simple one-word identifier (possibly quoted, possibly a keyword)
    pub fn parse_identifier(&mut self) -> Result<Ident, ParserError> {
        match self.next_token() {
            Token::Word(w) => Ok(w.to_ident()),
            Token::SingleQuotedString(s) => Ok(Ident::with_quote('\'', s)),
            unexpected => self.expected("identifier", unexpected),
        }
    }

    /// Parse a parenthesized comma-separated list of unqualified, possibly quoted identifiers
    pub fn parse_parenthesized_column_list(
        &mut self,
        optional: IsOptional,
    ) -> Result<Vec<Ident>, ParserError> {
        if self.consume_token(&Token::LParen) {
            let cols = self.parse_comma_separated(Parser::parse_identifier)?;
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

    /// Parse a query expression, i.e. a `SELECT` statement optionally
    /// preceeded with some `WITH` CTE declarations and optionally followed
    /// by `ORDER BY`. Unlike some other parse_... methods, this one doesn't
    /// expect the initial keyword to be already consumed
    pub fn parse_query(&mut self) -> Result<Query, ParserError> {
        let with = if self.parse_keyword(Keyword::WITH) {
            Some(With {
                recursive: self.parse_keyword(Keyword::RECURSIVE),
                cte_tables: self.parse_comma_separated(Parser::parse_cte)?,
            })
        } else {
            None
        };

        let body = Box::new(self.parse_query_body(0)?);

        let order_by = if self.parse_keywords(&[Keyword::ORDER, Keyword::BY]) {
            self.parse_comma_separated(Parser::parse_order_by_expr)?
        } else {
            vec![]
        };

        let mut limit = None;
        let mut offset = None;

        for _x in 0..2 {
            if limit.is_none() && self.parse_keyword(Keyword::LIMIT) {
                limit = self.parse_limit()?
            }

            if offset.is_none() && self.parse_keyword(Keyword::OFFSET) {
                offset = Some(self.parse_offset()?)
            }

            if offset.is_none() && self.consume_token(&Token::Comma) {
                // mysql style LIMIT 10, offset 5
                offset = Some(self.parse_offset()?)
            }
        }

        let fetch = if self.parse_keyword(Keyword::FETCH) {
            Some(self.parse_fetch()?)
        } else {
            None
        };

        let lock = if self.parse_keyword(Keyword::FOR) {
            Some(self.parse_lock()?)
        } else {
            None
        };

        Ok(Query {
            with,
            body,
            order_by,
            limit,
            offset,
            fetch,
            lock,
        })
    }

    /// Parse a CTE (`alias [( col1, col2, ... )] AS (subquery)`)
    pub fn parse_cte(&mut self) -> Result<Cte, ParserError> {
        let name = self.parse_identifier()?;

        let mut cte = if self.parse_keyword(Keyword::AS) {
            self.expect_token(&Token::LParen)?;
            let query = self.parse_query()?;
            self.expect_token(&Token::RParen)?;
            let alias = TableAlias {
                name,
                columns: vec![],
            };
            Cte {
                alias,
                query,
                from: None,
            }
        } else {
            let columns = self.parse_parenthesized_column_list(Optional)?;
            self.expect_keyword(Keyword::AS)?;
            self.expect_token(&Token::LParen)?;
            let query = self.parse_query()?;
            self.expect_token(&Token::RParen)?;
            let alias = TableAlias { name, columns };
            Cte {
                alias,
                query,
                from: None,
            }
        };
        if self.parse_keyword(Keyword::FROM) {
            cte.from = Some(self.parse_identifier()?);
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
    pub fn parse_query_body(&mut self, precedence: u8) -> Result<SetExpr, ParserError> {
        // We parse the expression using a Pratt parser, as in `parse_expr()`.
        // Start by parsing a restricted SELECT or a `(subquery)`:
        let mut expr = if self.parse_keyword(Keyword::SELECT) {
            SetExpr::Select(Box::new(self.parse_select()?))
        } else if self.consume_token(&Token::LParen) {
            // CTEs are not allowed here, but the parser currently accepts them
            let subquery = self.parse_query()?;
            self.expect_token(&Token::RParen)?;
            SetExpr::Query(Box::new(subquery))
        } else if self.parse_keyword(Keyword::VALUES) {
            SetExpr::Values(self.parse_values()?)
        } else {
            return self.expected(
                "SELECT, VALUES, or a subquery in the query body",
                self.peek_token(),
            );
        };

        loop {
            // The query can be optionally followed by a set operator:
            let op = self.parse_set_operator(&self.peek_token());
            let next_precedence = match op {
                // UNION and EXCEPT have the same binding power and evaluate left-to-right
                Some(SetOperator::Union) | Some(SetOperator::Except) => 10,
                // INTERSECT has higher precedence than UNION/EXCEPT
                Some(SetOperator::Intersect) => 20,
                // Unexpected token or EOF => stop parsing the query body
                None => break,
            };
            if precedence >= next_precedence {
                break;
            }
            self.next_token(); // skip past the set operator
            expr = SetExpr::SetOperation {
                left: Box::new(expr),
                op: op.unwrap(),
                all: self.parse_keyword(Keyword::ALL),
                right: Box::new(self.parse_query_body(next_precedence)?),
            };
        }

        Ok(expr)
    }

    pub fn parse_set_operator(&mut self, token: &Token) -> Option<SetOperator> {
        match token {
            Token::Word(w) if w.keyword == Keyword::UNION => Some(SetOperator::Union),
            Token::Word(w) if w.keyword == Keyword::EXCEPT => Some(SetOperator::Except),
            Token::Word(w) if w.keyword == Keyword::INTERSECT => Some(SetOperator::Intersect),
            _ => None,
        }
    }

    /// Parse a restricted `SELECT` statement (no CTEs / `UNION` / `ORDER BY`),
    /// assuming the initial `SELECT` was already consumed
    pub fn parse_select(&mut self) -> Result<Select, ParserError> {
        let distinct = self.parse_all_or_distinct()?;

        let top = if self.parse_keyword(Keyword::TOP) {
            Some(self.parse_top()?)
        } else {
            None
        };

        let projection = self.parse_comma_separated(Parser::parse_select_item)?;

        let into = if self.parse_keyword(Keyword::INTO) {
            let temporary = self
                .parse_one_of_keywords(&[Keyword::TEMP, Keyword::TEMPORARY])
                .is_some();
            let unlogged = self.parse_keyword(Keyword::UNLOGGED);
            let table = self.parse_keyword(Keyword::TABLE);
            let name = self.parse_object_name()?;
            Some(SelectInto {
                temporary,
                unlogged,
                table,
                name,
            })
        } else {
            None
        };

        // Note that for keywords to be properly handled here, they need to be
        // added to `RESERVED_FOR_COLUMN_ALIAS` / `RESERVED_FOR_TABLE_ALIAS`,
        // otherwise they may be parsed as an alias as part of the `projection`
        // or `from`.

        let from = if self.parse_keyword(Keyword::FROM) {
            self.parse_comma_separated(Parser::parse_table_and_joins)?
        } else {
            vec![]
        };

        let mut lateral_views = vec![];
        loop {
            if self.parse_keywords(&[Keyword::LATERAL, Keyword::VIEW]) {
                let outer = self.parse_keyword(Keyword::OUTER);
                let lateral_view = self.parse_expr()?;
                let lateral_view_name = self.parse_object_name()?;
                let lateral_col_alias = self
                    .parse_comma_separated(|parser| {
                        parser.parse_optional_alias(&[
                            Keyword::WHERE,
                            Keyword::GROUP,
                            Keyword::CLUSTER,
                            Keyword::HAVING,
                            Keyword::LATERAL,
                        ]) // This couldn't possibly be a bad idea
                    })?
                    .into_iter()
                    .flatten()
                    .collect();

                lateral_views.push(LateralView {
                    lateral_view,
                    lateral_view_name,
                    lateral_col_alias,
                    outer,
                });
            } else {
                break;
            }
        }

        let selection = if self.parse_keyword(Keyword::WHERE) {
            Some(self.parse_expr()?)
        } else {
            None
        };

        let group_by = if self.parse_keywords(&[Keyword::GROUP, Keyword::BY]) {
            self.parse_comma_separated(Parser::parse_group_by_expr)?
        } else {
            vec![]
        };

        let cluster_by = if self.parse_keywords(&[Keyword::CLUSTER, Keyword::BY]) {
            self.parse_comma_separated(Parser::parse_expr)?
        } else {
            vec![]
        };

        let distribute_by = if self.parse_keywords(&[Keyword::DISTRIBUTE, Keyword::BY]) {
            self.parse_comma_separated(Parser::parse_expr)?
        } else {
            vec![]
        };

        let sort_by = if self.parse_keywords(&[Keyword::SORT, Keyword::BY]) {
            self.parse_comma_separated(Parser::parse_expr)?
        } else {
            vec![]
        };

        let having = if self.parse_keyword(Keyword::HAVING) {
            Some(self.parse_expr()?)
        } else {
            None
        };

        let qualify = if self.parse_keyword(Keyword::QUALIFY) {
            Some(self.parse_expr()?)
        } else {
            None
        };

        Ok(Select {
            distinct,
            top,
            projection,
            into,
            from,
            lateral_views,
            selection,
            group_by,
            cluster_by,
            distribute_by,
            sort_by,
            having,
            qualify,
        })
    }

    pub fn parse_table_and_joins(&mut self) -> Result<TableWithJoins, ParserError> {
        let relation = self.parse_table_factor()?;
        // Note that for keywords to be properly handled here, they need to be
        // added to `RESERVED_FOR_TABLE_ALIAS`, otherwise they may be parsed as
        // a table alias.
        let mut joins = vec![];
        loop {
            let join = if self.parse_keyword(Keyword::CROSS) {
                let join_operator = if self.parse_keyword(Keyword::JOIN) {
                    JoinOperator::CrossJoin
                } else if self.parse_keyword(Keyword::APPLY) {
                    // MSSQL extension, similar to CROSS JOIN LATERAL
                    JoinOperator::CrossApply
                } else {
                    return self.expected("JOIN or APPLY after CROSS", self.peek_token());
                };
                Join {
                    relation: self.parse_table_factor()?,
                    join_operator,
                }
            } else if self.parse_keyword(Keyword::OUTER) {
                // MSSQL extension, similar to LEFT JOIN LATERAL .. ON 1=1
                self.expect_keyword(Keyword::APPLY)?;
                Join {
                    relation: self.parse_table_factor()?,
                    join_operator: JoinOperator::OuterApply,
                }
            } else {
                let natural = self.parse_keyword(Keyword::NATURAL);
                let peek_keyword = if let Token::Word(w) = self.peek_token() {
                    w.keyword
                } else {
                    Keyword::NoKeyword
                };

                let join_operator_type = match peek_keyword {
                    Keyword::INNER | Keyword::JOIN => {
                        let _ = self.parse_keyword(Keyword::INNER);
                        self.expect_keyword(Keyword::JOIN)?;
                        JoinOperator::Inner
                    }
                    kw @ Keyword::LEFT | kw @ Keyword::RIGHT | kw @ Keyword::FULL => {
                        let _ = self.next_token();
                        let _ = self.parse_keyword(Keyword::OUTER);
                        self.expect_keyword(Keyword::JOIN)?;
                        match kw {
                            Keyword::LEFT => JoinOperator::LeftOuter,
                            Keyword::RIGHT => JoinOperator::RightOuter,
                            Keyword::FULL => JoinOperator::FullOuter,
                            _ => unreachable!(),
                        }
                    }
                    Keyword::OUTER => {
                        return self.expected("LEFT, RIGHT, or FULL", self.peek_token());
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
        if self.parse_keyword(Keyword::LATERAL) {
            // LATERAL must always be followed by a subquery.
            if !self.consume_token(&Token::LParen) {
                self.expected("subquery after LATERAL", self.peek_token())?;
            }
            self.parse_derived_table_factor(Lateral)
        } else if self.parse_keyword(Keyword::TABLE) {
            // parse table function (SELECT * FROM TABLE (<expr>) [ AS <alias> ])
            self.expect_token(&Token::LParen)?;
            let expr = self.parse_expr()?;
            self.expect_token(&Token::RParen)?;
            let alias = self.parse_optional_table_alias(keywords::RESERVED_FOR_TABLE_ALIAS)?;
            Ok(TableFactor::TableFunction { expr, alias })
        } else if self.consume_token(&Token::LParen) {
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
            //                   | | | (4) belongs to a SetExpr::Query inside the subquery
            //                   | | (3) starts a derived table (subquery)
            //                   | (2) starts a nested join
            //                   (1) an additional set of parens around a nested join
            //

            // If the recently consumed '(' starts a derived table, the call to
            // `parse_derived_table_factor` below will return success after parsing the
            // subquery, followed by the closing ')', and the alias of the derived table.
            // In the example above this is case (3).
            return_ok_if_some!(
                self.maybe_parse(|parser| parser.parse_derived_table_factor(NotLateral))
            );
            // A parsing error from `parse_derived_table_factor` indicates that the '(' we've
            // recently consumed does not start a derived table (cases 1, 2, or 4).
            // `maybe_parse` will ignore such an error and rewind to be after the opening '('.

            // Inside the parentheses we expect to find an (A) table factor
            // followed by some joins or (B) another level of nesting.
            let mut table_and_joins = self.parse_table_and_joins()?;

            #[allow(clippy::if_same_then_else)]
            if !table_and_joins.joins.is_empty() {
                self.expect_token(&Token::RParen)?;
                Ok(TableFactor::NestedJoin(Box::new(table_and_joins))) // (A)
            } else if let TableFactor::NestedJoin(_) = &table_and_joins.relation {
                // (B): `table_and_joins` (what we found inside the parentheses)
                // is a nested join `(foo JOIN bar)`, not followed by other joins.
                self.expect_token(&Token::RParen)?;
                Ok(TableFactor::NestedJoin(Box::new(table_and_joins)))
            } else {
                // Dialect-specific behavior: Snowflake diverges from the
                // standard and from most of the other implementations by
                // allowing extra parentheses not only around a join (B), but
                // around lone table names (e.g. `FROM (mytable [AS alias])`)
                // and around derived tables (e.g. `FROM ((SELECT ...)
                // [AS alias])`) as well.
                self.expect_token(&Token::RParen)?;

                if let Some(outer_alias) =
                    self.parse_optional_table_alias(keywords::RESERVED_FOR_TABLE_ALIAS)?
                {
                    // Snowflake also allows specifying an alias *after* parens
                    // e.g. `FROM (mytable) AS alias`
                    match &mut table_and_joins.relation {
                        TableFactor::Derived { alias, .. }
                        | TableFactor::Table { alias, .. }
                        | TableFactor::UNNEST { alias, .. }
                        | TableFactor::TableFunction { alias, .. } => {
                            // but not `FROM (mytable AS alias1) AS alias2`.
                            if let Some(inner_alias) = alias {
                                return Err(ParserError::ParserError(format!(
                                    "duplicate alias {}",
                                    inner_alias.sql(&Default::default()).unwrap()
                                )));
                            }
                            // Act as if the alias was specified normally next
                            // to the table name: `(mytable) AS alias` ->
                            // `(mytable AS alias)`
                            alias.replace(outer_alias);
                        }
                        TableFactor::NestedJoin(_) => unreachable!(),
                    };
                }
                // Do not store the extra set of parens in the AST
                Ok(table_and_joins.relation)
            }
        } else if self.parse_keyword(Keyword::UNNEST) {
            self.expect_token(&Token::LParen)?;
            let expr = self.parse_expr()?;
            self.expect_token(&Token::RParen)?;

            let alias = match self.parse_optional_table_alias(keywords::RESERVED_FOR_TABLE_ALIAS) {
                Ok(Some(alias)) => Some(alias),
                Ok(None) => None,
                Err(e) => return Err(e),
            };

            let with_offset = match self.expect_keywords(&[Keyword::WITH, Keyword::OFFSET]) {
                Ok(()) => true,
                Err(_) => false,
            };

            let with_offset_alias =
                match self.parse_optional_alias(keywords::RESERVED_FOR_COLUMN_ALIAS) {
                    Ok(Some(alias)) => Some(alias),
                    Ok(None) => None,
                    Err(e) => return Err(e),
                };

            Ok(TableFactor::UNNEST {
                alias,
                array_expr: Box::new(expr),
                with_offset,
                with_offset_alias,
            })
        } else {
            let name = self.parse_object_name()?;
            // Postgres, MSSQL: table-valued functions:
            let args = if self.consume_token(&Token::LParen) {
                Some(self.parse_optional_args()?)
            } else {
                None
            };
            let alias = self.parse_optional_table_alias(keywords::RESERVED_FOR_TABLE_ALIAS)?;
            // MSSQL-specific table hints:
            let mut with_hints = vec![];
            if self.parse_keyword(Keyword::WITH) {
                if self.consume_token(&Token::LParen) {
                    with_hints = self.parse_comma_separated(Parser::parse_expr)?;
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

    pub fn parse_join_constraint(&mut self, natural: bool) -> Result<JoinConstraint, ParserError> {
        if natural {
            Ok(JoinConstraint::Natural)
        } else if self.parse_keyword(Keyword::ON) {
            let constraint = self.parse_expr()?;
            Ok(JoinConstraint::On(constraint))
        } else if self.parse_keyword(Keyword::USING) {
            let columns = self.parse_parenthesized_column_list(Mandatory)?;
            Ok(JoinConstraint::Using(columns))
        } else {
            Ok(JoinConstraint::None)
            //self.expected("ON, or USING after JOIN", self.peek_token())
        }
    }

    /// Parse a `var = expr` assignment, used in an UPDATE statement
    pub fn parse_assignment(&mut self) -> Result<Assignment, ParserError> {
        let id = self.parse_identifiers_non_keywords()?;
        self.expect_token(&Token::Eq)?;
        let value = self.parse_expr()?;
        Ok(Assignment { id, value })
    }

    pub fn parse_function_args(&mut self) -> Result<FunctionArg, ParserError> {
        if self.peek_nth_token(1) == Token::RArrow {
            let name = self.parse_identifier()?;

            self.expect_token(&Token::RArrow)?;
            let arg = self.parse_wildcard_expr()?.into();

            Ok(FunctionArg::Named { name, arg })
        } else {
            Ok(FunctionArg::Unnamed(self.parse_wildcard_expr()?.into()))
        }
    }

    pub fn parse_optional_args(&mut self) -> Result<Vec<FunctionArg>, ParserError> {
        if self.consume_token(&Token::RParen) {
            Ok(vec![])
        } else {
            let args = self.parse_comma_separated(Parser::parse_function_args)?;
            self.expect_token(&Token::RParen)?;
            Ok(args)
        }
    }

    /// Parse a comma-delimited list of projections after SELECT
    pub fn parse_select_item(&mut self) -> Result<SelectItem, ParserError> {
        match self.parse_wildcard_expr()? {
            WildcardExpr::Expr(expr) => self
                .parse_optional_alias(keywords::RESERVED_FOR_COLUMN_ALIAS)
                .map(|alias| match alias {
                    Some(alias) => SelectItem::ExprWithAlias { expr, alias },
                    None => SelectItem::UnnamedExpr(expr),
                }),
            WildcardExpr::QualifiedWildcard(prefix) => Ok(SelectItem::QualifiedWildcard(prefix)),
            WildcardExpr::Wildcard => Ok(SelectItem::Wildcard),
        }
    }

    /// Parse an expression, optionally followed by ASC or DESC (used in ORDER BY)
    pub fn parse_order_by_expr(&mut self) -> Result<OrderByExpr, ParserError> {
        let expr = self.parse_expr()?;

        let asc = if self.parse_keyword(Keyword::ASC) {
            Some(true)
        } else if self.parse_keyword(Keyword::DESC) {
            Some(false)
        } else {
            None
        };

        let nulls_first = if self.parse_keywords(&[Keyword::NULLS, Keyword::FIRST]) {
            Some(true)
        } else if self.parse_keywords(&[Keyword::NULLS, Keyword::LAST]) {
            Some(false)
        } else {
            None
        };

        Ok(OrderByExpr {
            expr,
            asc,
            nulls_first,
        })
    }

    /// Parse a TOP clause, MSSQL equivalent of LIMIT,
    /// that follows after SELECT [DISTINCT].
    pub fn parse_top(&mut self) -> Result<Top, ParserError> {
        let quantity = if self.consume_token(&Token::LParen) {
            let quantity = self.parse_expr()?;
            self.expect_token(&Token::RParen)?;
            Some(quantity)
        } else {
            Some(Expr::Value(self.parse_number_value()?))
        };

        let percent = self.parse_keyword(Keyword::PERCENT);

        let with_ties = self.parse_keywords(&[Keyword::WITH, Keyword::TIES]);

        Ok(Top {
            with_ties,
            percent,
            quantity,
        })
    }

    /// Parse a LIMIT clause
    pub fn parse_limit(&mut self) -> Result<Option<Expr>, ParserError> {
        if self.parse_keyword(Keyword::ALL) {
            Ok(None)
        } else {
            Ok(Some(Expr::Value(self.parse_number_value()?)))
        }
    }

    /// Parse an OFFSET clause
    pub fn parse_offset(&mut self) -> Result<Offset, ParserError> {
        let value = Expr::Value(self.parse_number_value()?);
        let rows = if self.parse_keyword(Keyword::ROW) {
            OffsetRows::Row
        } else if self.parse_keyword(Keyword::ROWS) {
            OffsetRows::Rows
        } else {
            OffsetRows::None
        };
        Ok(Offset { value, rows })
    }

    /// Parse a FETCH clause
    pub fn parse_fetch(&mut self) -> Result<Fetch, ParserError> {
        self.expect_one_of_keywords(&[Keyword::FIRST, Keyword::NEXT])?;
        let (quantity, percent) = if self
            .parse_one_of_keywords(&[Keyword::ROW, Keyword::ROWS])
            .is_some()
        {
            (None, false)
        } else {
            let quantity = Expr::Value(self.parse_value()?);
            let percent = self.parse_keyword(Keyword::PERCENT);
            self.expect_one_of_keywords(&[Keyword::ROW, Keyword::ROWS])?;
            (Some(quantity), percent)
        };
        let with_ties = if self.parse_keyword(Keyword::ONLY) {
            false
        } else if self.parse_keywords(&[Keyword::WITH, Keyword::TIES]) {
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

    /// Parse a FOR UPDATE/FOR SHARE clause
    pub fn parse_lock(&mut self) -> Result<LockType, ParserError> {
        match self.expect_one_of_keywords(&[Keyword::UPDATE, Keyword::SHARE])? {
            Keyword::UPDATE => Ok(LockType::Update),
            Keyword::SHARE => Ok(LockType::Share),
            _ => unreachable!(),
        }
    }

    pub fn parse_values(&mut self) -> Result<Values, ParserError> {
        let values = self.parse_comma_separated(|parser| {
            parser.expect_token(&Token::LParen)?;
            let exprs = parser.parse_comma_separated(Parser::parse_expr)?;
            parser.expect_token(&Token::RParen)?;
            Ok(exprs)
        })?;
        Ok(Values(values))
    }

    pub fn parse_merge_clauses(&mut self) -> Result<Vec<MergeClause>, ParserError> {
        let mut clauses: Vec<MergeClause> = vec![];
        loop {
            if self.peek_token() == Token::EOF || self.peek_token() == Token::SemiColon {
                break;
            }
            self.expect_keyword(Keyword::WHEN)?;

            let is_not_matched = self.parse_keyword(Keyword::NOT);
            self.expect_keyword(Keyword::MATCHED)?;

            let predicate = if self.parse_keyword(Keyword::AND) {
                Some(self.parse_expr()?)
            } else {
                None
            };

            self.expect_keyword(Keyword::THEN)?;

            clauses.push(
                match self.parse_one_of_keywords(&[
                    Keyword::UPDATE,
                    Keyword::INSERT,
                    Keyword::DELETE,
                ]) {
                    Some(Keyword::UPDATE) => {
                        if is_not_matched {
                            return Err(ParserError::ParserError(
                                "UPDATE in NOT MATCHED merge clause".to_string(),
                            ));
                        }
                        self.expect_keyword(Keyword::SET)?;
                        let assignments = self.parse_comma_separated(Parser::parse_assignment)?;
                        MergeClause::MatchedUpdate {
                            predicate,
                            assignments,
                        }
                    }
                    Some(Keyword::DELETE) => {
                        if is_not_matched {
                            return Err(ParserError::ParserError(
                                "DELETE in NOT MATCHED merge clause".to_string(),
                            ));
                        }
                        MergeClause::MatchedDelete(predicate)
                    }
                    Some(Keyword::INSERT) => {
                        if !is_not_matched {
                            return Err(ParserError::ParserError(
                                "INSERT in MATCHED merge clause".to_string(),
                            ));
                        }
                        let columns = self.parse_parenthesized_column_list(Optional)?;
                        self.expect_keyword(Keyword::VALUES)?;
                        let values = self.parse_values()?;
                        MergeClause::NotMatched {
                            predicate,
                            columns,
                            values,
                        }
                    }
                    Some(_) => {
                        return Err(ParserError::ParserError(
                            "expected UPDATE, DELETE or INSERT in merge clause".to_string(),
                        ))
                    }
                    None => {
                        return Err(ParserError::ParserError(
                            "expected UPDATE, DELETE or INSERT in merge clause".to_string(),
                        ))
                    }
                },
            );
        }
        Ok(clauses)
    }
}

impl Word {
    pub fn to_ident(&self) -> Ident {
        Ident {
            value: self.value.clone(),
            quote_style: self.quote_style,
        }
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::test_utils::{all_dialects, TestedDialects};
//
//     #[test]
//     fn test_prev_index() {
//         let sql = "SELECT version";
//         all_dialects().run_parser_method(sql, |parser| {
//             assert_eq!(parser.peek_token(), Token::make_keyword("SELECT"));
//             assert_eq!(parser.next_token(), Token::make_keyword("SELECT"));
//             parser.prev_token();
//             assert_eq!(parser.next_token(), Token::make_keyword("SELECT"));
//             assert_eq!(parser.next_token(), Token::make_word("version", None));
//             parser.prev_token();
//             assert_eq!(parser.peek_token(), Token::make_word("version", None));
//             assert_eq!(parser.next_token(), Token::make_word("version", None));
//             assert_eq!(parser.peek_token(), Token::EOF);
//             parser.prev_token();
//             assert_eq!(parser.next_token(), Token::make_word("version", None));
//             assert_eq!(parser.next_token(), Token::EOF);
//             assert_eq!(parser.next_token(), Token::EOF);
//             parser.prev_token();
//         });
//     }
//
//     #[test]
//     fn test_parse_limit() {
//         let sql = "SELECT * FROM user LIMIT 1";
//         all_dialects().run_parser_method(sql, |parser| {
//             let ast = parser.parse_query().unwrap();
//             assert_eq!(ast.to_string(), sql.to_string());
//         });
//
//         let sql = "SELECT * FROM user LIMIT $1 OFFSET $2";
//         let dialects = TestedDialects {
//             dialects: vec![
//                 Box::new(PostgreSqlDialect {}),
//                 Box::new(ClickHouseDialect {}),
//                 Box::new(GenericDialect {}),
//                 Box::new(MsSqlDialect {}),
//                 Box::new(SnowflakeDialect {}),
//             ],
//         };
//
//         dialects.run_parser_method(sql, |parser| {
//             let ast = parser.parse_query().unwrap();
//             assert_eq!(ast.to_string(), sql.to_string());
//         });
//
//         let sql = "SELECT * FROM user LIMIT ? OFFSET ?";
//         let dialects = TestedDialects {
//             dialects: vec![Box::new(MySqlDialect {})],
//         };
//         dialects.run_parser_method(sql, |parser| {
//             let ast = parser.parse_query().unwrap();
//             assert_eq!(ast.to_string(), sql.to_string());
//         });
//     }
// }

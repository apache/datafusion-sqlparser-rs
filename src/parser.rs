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

use IsLateral::*;
use IsOptional::*;

use crate::ast::helpers::stmt_create_table::CreateTableBuilder;
use crate::ast::*;
use crate::dialect::*;
use crate::keywords::{self, Keyword};
use crate::tokenizer::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParserError {
    TokenizerError(String),
    ParserError(String),
    RecursionLimitExceeded,
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

#[cfg(feature = "std")]
/// Implemenation [`RecursionCounter`] if std is available
mod recursion {
    use core::sync::atomic::{AtomicUsize, Ordering};
    use std::rc::Rc;

    use super::ParserError;

    /// Tracks remaining recursion depth. This value is decremented on
    /// each call to `try_decrease()`, when it reaches 0 an error will
    /// be returned.
    ///
    /// Note: Uses an Rc and AtomicUsize in order to satisfy the Rust
    /// borrow checker so the automatic DepthGuard decrement a
    /// reference to the counter. The actual value is not modified
    /// concurrently
    pub(crate) struct RecursionCounter {
        remaining_depth: Rc<AtomicUsize>,
    }

    impl RecursionCounter {
        /// Creates a [`RecursionCounter`] with the specified maximum
        /// depth
        pub fn new(remaining_depth: usize) -> Self {
            Self {
                remaining_depth: Rc::new(remaining_depth.into()),
            }
        }

        /// Decreases the remaining depth by 1.
        ///
        /// Returns `Err` if the remaining depth falls to 0.
        ///
        /// Returns a [`DepthGuard`] which will adds 1 to the
        /// remaining depth upon drop;
        pub fn try_decrease(&self) -> Result<DepthGuard, ParserError> {
            let old_value = self.remaining_depth.fetch_sub(1, Ordering::SeqCst);
            // ran out of space
            if old_value == 0 {
                Err(ParserError::RecursionLimitExceeded)
            } else {
                Ok(DepthGuard::new(Rc::clone(&self.remaining_depth)))
            }
        }
    }

    /// Guard that increass the remaining depth by 1 on drop
    pub struct DepthGuard {
        remaining_depth: Rc<AtomicUsize>,
    }

    impl DepthGuard {
        fn new(remaining_depth: Rc<AtomicUsize>) -> Self {
            Self { remaining_depth }
        }
    }
    impl Drop for DepthGuard {
        fn drop(&mut self) {
            self.remaining_depth.fetch_add(1, Ordering::SeqCst);
        }
    }
}

#[cfg(not(feature = "std"))]
mod recursion {
    /// Implemenation [`RecursionCounter`] if std is NOT available (and does not
    /// guard against stack overflow).
    ///
    /// Has the same API as the std RecursionCounter implementation
    /// but does not actually limit stack depth.
    pub(crate) struct RecursionCounter {}

    impl RecursionCounter {
        pub fn new(_remaining_depth: usize) -> Self {
            Self {}
        }
        pub fn try_decrease(&self) -> Result<DepthGuard, super::ParserError> {
            Ok(DepthGuard {})
        }
    }

    pub struct DepthGuard {}
}

use recursion::RecursionCounter;

#[derive(PartialEq, Eq)]
pub enum IsOptional {
    Optional,
    Mandatory,
}

pub enum IsLateral {
    Lateral,
    NotLateral,
}

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
                ParserError::RecursionLimitExceeded => "recursion limit exceeded",
            }
        )
    }
}

#[cfg(feature = "std")]
impl std::error::Error for ParserError {}

// By default, allow expressions up to this deep before erroring
const DEFAULT_REMAINING_DEPTH: usize = 50;

pub struct Parser<'a> {
    tokens: Vec<TokenWithLocation>,
    /// The index of the first unprocessed token in `self.tokens`
    index: usize,
    /// The current dialect to use
    dialect: &'a dyn Dialect,
    /// ensure the stack does not overflow by limiting recusion depth
    recursion_counter: RecursionCounter,
}

impl<'a> Parser<'a> {
    /// Create a parser for a [`Dialect`]
    ///
    /// See also [`Parser::parse_sql`]
    ///
    /// Example:
    /// ```
    /// # use sqlparser::{parser::{Parser, ParserError}, dialect::GenericDialect};
    /// # fn main() -> Result<(), ParserError> {
    /// let dialect = GenericDialect{};
    /// let statements = Parser::new(&dialect)
    ///   .try_with_sql("SELECT * FROM foo")?
    ///   .parse_statements()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn new(dialect: &'a dyn Dialect) -> Self {
        Self {
            tokens: vec![],
            index: 0,
            dialect,
            recursion_counter: RecursionCounter::new(DEFAULT_REMAINING_DEPTH),
        }
    }

    /// Specify the maximum recursion limit while parsing.
    ///
    ///
    /// [`Parser`] prevents stack overflows by returning
    /// [`ParserError::RecursionLimitExceeded`] if the parser exceeds
    /// this depth while processing the query.
    ///
    /// Example:
    /// ```
    /// # use sqlparser::{parser::{Parser, ParserError}, dialect::GenericDialect};
    /// # fn main() -> Result<(), ParserError> {
    /// let dialect = GenericDialect{};
    /// let result = Parser::new(&dialect)
    ///   .with_recursion_limit(1)
    ///   .try_with_sql("SELECT * FROM foo WHERE (a OR (b OR (c OR d)))")?
    ///   .parse_statements();
    ///   assert_eq!(result, Err(ParserError::RecursionLimitExceeded));
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_recursion_limit(mut self, recursion_limit: usize) -> Self {
        self.recursion_counter = RecursionCounter::new(recursion_limit);
        self
    }

    /// Reset this parser to parse the specified token stream
    pub fn with_tokens_with_locations(mut self, tokens: Vec<TokenWithLocation>) -> Self {
        self.tokens = tokens;
        self.index = 0;
        self
    }

    /// Reset this parser state to parse the specified tokens
    pub fn with_tokens(self, tokens: Vec<Token>) -> Self {
        // Put in dummy locations
        let tokens_with_locations: Vec<TokenWithLocation> = tokens
            .into_iter()
            .map(|token| TokenWithLocation {
                token,
                location: Location { line: 0, column: 0 },
            })
            .collect();
        self.with_tokens_with_locations(tokens_with_locations)
    }

    /// Tokenize the sql string and sets this [`Parser`]'s state to
    /// parse the resulting tokens
    ///
    /// Returns an error if there was an error tokenizing the SQL string.
    ///
    /// See example on [`Parser::new()`] for an example
    pub fn try_with_sql(self, sql: &str) -> Result<Self, ParserError> {
        debug!("Parsing sql '{}'...", sql);
        let mut tokenizer = Tokenizer::new(self.dialect, sql);
        let tokens = tokenizer.tokenize()?;
        Ok(self.with_tokens(tokens))
    }

    /// Parse potentially multiple statements
    ///
    /// Example
    /// ```
    /// # use sqlparser::{parser::{Parser, ParserError}, dialect::GenericDialect};
    /// # fn main() -> Result<(), ParserError> {
    /// let dialect = GenericDialect{};
    /// let statements = Parser::new(&dialect)
    ///   // Parse a SQL string with 2 separate statements
    ///   .try_with_sql("SELECT * FROM foo; SELECT * FROM bar;")?
    ///   .parse_statements()?;
    /// assert_eq!(statements.len(), 2);
    /// # Ok(())
    /// # }
    /// ```
    pub fn parse_statements(&mut self) -> Result<Vec<Statement>, ParserError> {
        let mut stmts = Vec::new();
        let mut expecting_statement_delimiter = false;
        loop {
            // ignore empty statements (between successive statement delimiters)
            while self.consume_token(&Token::SemiColon) {
                expecting_statement_delimiter = false;
            }

            if self.peek_token() == Token::EOF {
                break;
            }
            if expecting_statement_delimiter {
                return self.expected("end of statement", self.peek_token());
            }

            let statement = self.parse_statement()?;
            stmts.push(statement);
            expecting_statement_delimiter = true;
        }
        Ok(stmts)
    }

    /// Convience method to parse a string with one or more SQL
    /// statements into produce an Abstract Syntax Tree (AST).
    ///
    /// Example
    /// ```
    /// # use sqlparser::{parser::{Parser, ParserError}, dialect::GenericDialect};
    /// # fn main() -> Result<(), ParserError> {
    /// let dialect = GenericDialect{};
    /// let statements = Parser::parse_sql(
    ///   &dialect, "SELECT * FROM foo"
    /// )?;
    /// assert_eq!(statements.len(), 1);
    /// # Ok(())
    /// # }
    /// ```
    pub fn parse_sql(dialect: &dyn Dialect, sql: &str) -> Result<Vec<Statement>, ParserError> {
        Parser::new(dialect).try_with_sql(sql)?.parse_statements()
    }

    /// Parse a single top-level statement (such as SELECT, INSERT, CREATE, etc.),
    /// stopping before the statement separator, if any.
    pub fn parse_statement(&mut self) -> Result<Statement, ParserError> {
        let _guard = self.recursion_counter.try_decrease()?;

        // allow the dialect to override statement parsing
        if let Some(statement) = self.dialect.parse_statement(self) {
            return statement;
        }

        let next_token = self.next_token();
        match &next_token.token {
            Token::Word(w) => match w.keyword {
                Keyword::KILL => Ok(self.parse_kill()?),
                Keyword::DESCRIBE => Ok(self.parse_explain(true)?),
                Keyword::EXPLAIN => Ok(self.parse_explain(false)?),
                Keyword::ANALYZE => Ok(self.parse_analyze()?),
                Keyword::SELECT | Keyword::WITH | Keyword::VALUES => {
                    self.prev_token();
                    Ok(Statement::Query(Box::new(self.parse_query()?)))
                }
                Keyword::TRUNCATE => Ok(self.parse_truncate()?),
                Keyword::MSCK => Ok(self.parse_msck()?),
                Keyword::CREATE => Ok(self.parse_create()?),
                Keyword::CACHE => Ok(self.parse_cache_table()?),
                Keyword::DROP => Ok(self.parse_drop()?),
                Keyword::DISCARD => Ok(self.parse_discard()?),
                Keyword::DECLARE => Ok(self.parse_declare()?),
                Keyword::FETCH => Ok(self.parse_fetch_statement()?),
                Keyword::DELETE => Ok(self.parse_delete()?),
                Keyword::INSERT => Ok(self.parse_insert()?),
                Keyword::UNCACHE => Ok(self.parse_uncache_table()?),
                Keyword::UPDATE => Ok(self.parse_update()?),
                Keyword::ALTER => Ok(self.parse_alter()?),
                Keyword::COPY => Ok(self.parse_copy()?),
                Keyword::CLOSE => Ok(self.parse_close()?),
                Keyword::SET => Ok(self.parse_set()?),
                Keyword::SHOW => Ok(self.parse_show()?),
                Keyword::USE => Ok(self.parse_use()?),
                Keyword::GRANT => Ok(self.parse_grant()?),
                Keyword::REVOKE => Ok(self.parse_revoke()?),
                Keyword::START => Ok(self.parse_start_transaction()?),
                // `BEGIN` is a nonstandard but common alias for the
                // standard `START TRANSACTION` statement. It is supported
                // by at least PostgreSQL and MySQL.
                Keyword::BEGIN => Ok(self.parse_begin()?),
                Keyword::SAVEPOINT => Ok(self.parse_savepoint()?),
                Keyword::COMMIT => Ok(self.parse_commit()?),
                Keyword::ROLLBACK => Ok(self.parse_rollback()?),
                Keyword::ASSERT => Ok(self.parse_assert()?),
                // `PREPARE`, `EXECUTE` and `DEALLOCATE` are Postgres-specific
                // syntaxes. They are used for Postgres prepared statement.
                Keyword::DEALLOCATE => Ok(self.parse_deallocate()?),
                Keyword::EXECUTE => Ok(self.parse_execute()?),
                Keyword::PREPARE => Ok(self.parse_prepare()?),
                Keyword::MERGE => Ok(self.parse_merge()?),
                _ => self.expected("an SQL statement", next_token),
            },
            Token::LParen => {
                self.prev_token();
                Ok(Statement::Query(Box::new(self.parse_query()?)))
            }
            _ => self.expected("an SQL statement", next_token),
        }
    }

    pub fn parse_msck(&mut self) -> Result<Statement, ParserError> {
        let repair = self.parse_keyword(Keyword::REPAIR);
        self.expect_keyword(Keyword::TABLE)?;
        let table_name = self.parse_object_name()?;
        let partition_action = self
            .maybe_parse(|parser| {
                let pa = match parser.parse_one_of_keywords(&[
                    Keyword::ADD,
                    Keyword::DROP,
                    Keyword::SYNC,
                ]) {
                    Some(Keyword::ADD) => Some(AddDropSync::ADD),
                    Some(Keyword::DROP) => Some(AddDropSync::DROP),
                    Some(Keyword::SYNC) => Some(AddDropSync::SYNC),
                    _ => None,
                };
                parser.expect_keyword(Keyword::PARTITIONS)?;
                Ok(pa)
            })
            .unwrap_or_default();
        Ok(Statement::Msck {
            repair,
            table_name,
            partition_action,
        })
    }

    pub fn parse_truncate(&mut self) -> Result<Statement, ParserError> {
        self.expect_keyword(Keyword::TABLE)?;
        let table_name = self.parse_object_name()?;
        let mut partitions = None;
        if self.parse_keyword(Keyword::PARTITION) {
            self.expect_token(&Token::LParen)?;
            partitions = Some(self.parse_comma_separated(Parser::parse_expr)?);
            self.expect_token(&Token::RParen)?;
        }
        Ok(Statement::Truncate {
            table_name,
            partitions,
        })
    }

    pub fn parse_analyze(&mut self) -> Result<Statement, ParserError> {
        self.expect_keyword(Keyword::TABLE)?;
        let table_name = self.parse_object_name()?;
        let mut for_columns = false;
        let mut cache_metadata = false;
        let mut noscan = false;
        let mut partitions = None;
        let mut compute_statistics = false;
        let mut columns = vec![];
        loop {
            match self.parse_one_of_keywords(&[
                Keyword::PARTITION,
                Keyword::FOR,
                Keyword::CACHE,
                Keyword::NOSCAN,
                Keyword::COMPUTE,
            ]) {
                Some(Keyword::PARTITION) => {
                    self.expect_token(&Token::LParen)?;
                    partitions = Some(self.parse_comma_separated(Parser::parse_expr)?);
                    self.expect_token(&Token::RParen)?;
                }
                Some(Keyword::NOSCAN) => noscan = true,
                Some(Keyword::FOR) => {
                    self.expect_keyword(Keyword::COLUMNS)?;

                    columns = self
                        .maybe_parse(|parser| {
                            parser.parse_comma_separated(Parser::parse_identifier)
                        })
                        .unwrap_or_default();
                    for_columns = true
                }
                Some(Keyword::CACHE) => {
                    self.expect_keyword(Keyword::METADATA)?;
                    cache_metadata = true
                }
                Some(Keyword::COMPUTE) => {
                    self.expect_keyword(Keyword::STATISTICS)?;
                    compute_statistics = true
                }
                _ => break,
            }
        }

        Ok(Statement::Analyze {
            table_name,
            for_columns,
            columns,
            partitions,
            cache_metadata,
            noscan,
            compute_statistics,
        })
    }

    /// Parse a new expression including wildcard & qualified wildcard
    pub fn parse_wildcard_expr(&mut self) -> Result<WildcardExpr, ParserError> {
        let index = self.index;

        let next_token = self.next_token();
        match next_token.token {
            Token::Word(w) if self.peek_token().token == Token::Period => {
                let mut id_parts: Vec<Ident> = vec![w.to_ident()];

                while self.consume_token(&Token::Period) {
                    let next_token = self.next_token();
                    match next_token.token {
                        Token::Word(w) => id_parts.push(w.to_ident()),
                        Token::Mul => {
                            return Ok(WildcardExpr::QualifiedWildcard(ObjectName(id_parts)));
                        }
                        _ => {
                            return self.expected("an identifier or a '*' after '.'", next_token);
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
        let _guard = self.recursion_counter.try_decrease()?;
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

    pub fn parse_interval_expr(&mut self) -> Result<Expr, ParserError> {
        let precedence = 0;
        let mut expr = self.parse_prefix()?;

        loop {
            let next_precedence = self.get_next_interval_precedence()?;

            if precedence >= next_precedence {
                break;
            }

            expr = self.parse_infix(expr, next_precedence)?;
        }

        Ok(expr)
    }

    /// Get the precedence of the next token
    /// With AND, OR, and XOR
    pub fn get_next_interval_precedence(&self) -> Result<u8, ParserError> {
        let token = self.peek_token();

        match token.token {
            Token::Word(w) if w.keyword == Keyword::AND => Ok(0),
            Token::Word(w) if w.keyword == Keyword::OR => Ok(0),
            Token::Word(w) if w.keyword == Keyword::XOR => Ok(0),
            _ => self.get_next_precedence(),
        }
    }

    pub fn parse_assert(&mut self) -> Result<Statement, ParserError> {
        let condition = self.parse_expr()?;
        let message = if self.parse_keyword(Keyword::AS) {
            Some(self.parse_expr()?)
        } else {
            None
        };

        Ok(Statement::Assert { condition, message })
    }

    pub fn parse_savepoint(&mut self) -> Result<Statement, ParserError> {
        let name = self.parse_identifier()?;
        Ok(Statement::Savepoint { name })
    }

    /// Parse an expression prefix
    pub fn parse_prefix(&mut self) -> Result<Expr, ParserError> {
        // allow the dialect to override prefix parsing
        if let Some(prefix) = self.dialect.parse_prefix(self) {
            return prefix;
        }

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
                DataType::Interval => parser.parse_interval(),
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

        let next_token = self.next_token();
        let expr = match next_token.token {
            Token::Word(w) => match w.keyword {
                Keyword::TRUE | Keyword::FALSE | Keyword::NULL => {
                    self.prev_token();
                    Ok(Expr::Value(self.parse_value()?))
                }
                Keyword::CURRENT_CATALOG
                | Keyword::CURRENT_USER
                | Keyword::SESSION_USER
                | Keyword::USER
                    if dialect_of!(self is PostgreSqlDialect | GenericDialect) =>
                {
                    Ok(Expr::Function(Function {
                        name: ObjectName(vec![w.to_ident()]),
                        args: vec![],
                        over: None,
                        distinct: false,
                        special: true,
                    }))
                }
                Keyword::CURRENT_TIMESTAMP
                | Keyword::CURRENT_TIME
                | Keyword::CURRENT_DATE
                | Keyword::LOCALTIME
                | Keyword::LOCALTIMESTAMP => {
                    self.parse_time_functions(ObjectName(vec![w.to_ident()]))
                }
                Keyword::CASE => self.parse_case_expr(),
                Keyword::CAST => self.parse_cast_expr(),
                Keyword::TRY_CAST => self.parse_try_cast_expr(),
                Keyword::SAFE_CAST => self.parse_safe_cast_expr(),
                Keyword::EXISTS => self.parse_exists_expr(false),
                Keyword::EXTRACT => self.parse_extract_expr(),
                Keyword::CEIL => self.parse_ceil_floor_expr(true),
                Keyword::FLOOR => self.parse_ceil_floor_expr(false),
                Keyword::POSITION => self.parse_position_expr(),
                Keyword::SUBSTRING => self.parse_substring_expr(),
                Keyword::OVERLAY => self.parse_overlay_expr(),
                Keyword::TRIM => self.parse_trim_expr(),
                Keyword::INTERVAL => self.parse_interval(),
                Keyword::LISTAGG => self.parse_listagg_expr(),
                // Treat ARRAY[1,2,3] as an array [1,2,3], otherwise try as subquery or a function call
                Keyword::ARRAY if self.peek_token() == Token::LBracket => {
                    self.expect_token(&Token::LBracket)?;
                    self.parse_array_expr(true)
                }
                Keyword::ARRAY
                    if self.peek_token() == Token::LParen
                        && !dialect_of!(self is ClickHouseDialect) =>
                {
                    self.expect_token(&Token::LParen)?;
                    self.parse_array_subquery()
                }
                Keyword::ARRAY_AGG => self.parse_array_agg_expr(),
                Keyword::NOT => self.parse_not(),
                Keyword::MATCH if dialect_of!(self is MySqlDialect | GenericDialect) => {
                    self.parse_match_against()
                }
                // Here `w` is a word, check if it's a part of a multi-part
                // identifier, a function call, or a simple identifier:
                _ => match self.peek_token().token {
                    Token::LParen | Token::Period => {
                        let mut id_parts: Vec<Ident> = vec![w.to_ident()];
                        while self.consume_token(&Token::Period) {
                            let next_token = self.next_token();
                            match next_token.token {
                                Token::Word(w) => id_parts.push(w.to_ident()),
                                _ => {
                                    return self
                                        .expected("an identifier or a '*' after '.'", next_token);
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
            tok @ Token::DoubleExclamationMark
            | tok @ Token::PGSquareRoot
            | tok @ Token::PGCubeRoot
            | tok @ Token::AtSign
            | tok @ Token::Tilde
                if dialect_of!(self is PostgreSqlDialect) =>
            {
                let op = match tok {
                    Token::DoubleExclamationMark => UnaryOperator::PGPrefixFactorial,
                    Token::PGSquareRoot => UnaryOperator::PGSquareRoot,
                    Token::PGCubeRoot => UnaryOperator::PGCubeRoot,
                    Token::AtSign => UnaryOperator::PGAbs,
                    Token::Tilde => UnaryOperator::PGBitwiseNot,
                    _ => unreachable!(),
                };
                Ok(Expr::UnaryOp {
                    op,
                    expr: Box::new(self.parse_subexpr(Self::PLUS_MINUS_PREC)?),
                })
            }
            Token::EscapedStringLiteral(_) if dialect_of!(self is PostgreSqlDialect | GenericDialect) =>
            {
                self.prev_token();
                Ok(Expr::Value(self.parse_value()?))
            }
            Token::Number(_, _)
            | Token::SingleQuotedString(_)
            | Token::DoubleQuotedString(_)
            | Token::DollarQuotedString(_)
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
                    let key = match tok.token {
                        Token::Word(word) => word.to_ident(),
                        _ => return parser_err!(format!("Expected identifier, found: {}", tok)),
                    };
                    Ok(Expr::CompositeAccess {
                        expr: Box::new(expr),
                        key,
                    })
                }
            }
            Token::Placeholder(_) | Token::Colon | Token::AtSign => {
                self.prev_token();
                Ok(Expr::Value(self.parse_value()?))
            }
            _ => self.expected("an expression:", next_token),
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
            special: false,
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
            special: false,
        }))
    }

    pub fn parse_window_frame_units(&mut self) -> Result<WindowFrameUnits, ParserError> {
        let next_token = self.next_token();
        match &next_token.token {
            Token::Word(w) => match w.keyword {
                Keyword::ROWS => Ok(WindowFrameUnits::Rows),
                Keyword::RANGE => Ok(WindowFrameUnits::Range),
                Keyword::GROUPS => Ok(WindowFrameUnits::Groups),
                _ => self.expected("ROWS, RANGE, GROUPS", next_token)?,
            },
            _ => self.expected("ROWS, RANGE, GROUPS", next_token),
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
                Some(Box::new(match self.peek_token().token {
                    Token::SingleQuotedString(_) => self.parse_interval()?,
                    _ => self.parse_expr()?,
                }))
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
        if dialect_of!(self is PostgreSqlDialect | GenericDialect) {
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
        } else {
            // TODO parse rollup for other dialects
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

    /// Parse a BigQuery SAFE_CAST function e.g. `SAFE_CAST(expr AS FLOAT64)`
    pub fn parse_safe_cast_expr(&mut self) -> Result<Expr, ParserError> {
        self.expect_token(&Token::LParen)?;
        let expr = self.parse_expr()?;
        self.expect_keyword(Keyword::AS)?;
        let data_type = self.parse_data_type()?;
        self.expect_token(&Token::RParen)?;
        Ok(Expr::SafeCast {
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

    pub fn parse_ceil_floor_expr(&mut self, is_ceil: bool) -> Result<Expr, ParserError> {
        self.expect_token(&Token::LParen)?;
        let expr = self.parse_expr()?;
        // Parse `CEIL/FLOOR(expr)`
        let mut field = DateTimeField::NoDateTime;
        let keyword_to = self.parse_keyword(Keyword::TO);
        if keyword_to {
            // Parse `CEIL/FLOOR(expr TO DateTimeField)`
            field = self.parse_date_time_field()?;
        }
        self.expect_token(&Token::RParen)?;
        if is_ceil {
            Ok(Expr::Ceil {
                expr: Box::new(expr),
                field,
            })
        } else {
            Ok(Expr::Floor {
                expr: Box::new(expr),
                field,
            })
        }
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
            parser_err!("Position function must include IN keyword".to_string())
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

    pub fn parse_overlay_expr(&mut self) -> Result<Expr, ParserError> {
        // PARSE OVERLAY (EXPR PLACING EXPR FROM 1 [FOR 3])
        self.expect_token(&Token::LParen)?;
        let expr = self.parse_expr()?;
        self.expect_keyword(Keyword::PLACING)?;
        let what_expr = self.parse_expr()?;
        self.expect_keyword(Keyword::FROM)?;
        let from_expr = self.parse_expr()?;
        let mut for_expr = None;
        if self.parse_keyword(Keyword::FOR) {
            for_expr = Some(self.parse_expr()?);
        }
        self.expect_token(&Token::RParen)?;

        Ok(Expr::Overlay {
            expr: Box::new(expr),
            overlay_what: Box::new(what_expr),
            overlay_from: Box::new(from_expr),
            overlay_for: for_expr.map(Box::new),
        })
    }

    /// ```sql
    /// TRIM ([WHERE] ['text' FROM] 'text')
    /// TRIM ('text')
    /// ```
    pub fn parse_trim_expr(&mut self) -> Result<Expr, ParserError> {
        self.expect_token(&Token::LParen)?;
        let mut trim_where = None;
        if let Token::Word(word) = self.peek_token().token {
            if [Keyword::BOTH, Keyword::LEADING, Keyword::TRAILING]
                .iter()
                .any(|d| word.keyword == *d)
            {
                trim_where = Some(self.parse_trim_where()?);
            }
        }
        let expr = self.parse_expr()?;
        if self.parse_keyword(Keyword::FROM) {
            let trim_what = Box::new(expr);
            let expr = self.parse_expr()?;
            self.expect_token(&Token::RParen)?;
            Ok(Expr::Trim {
                expr: Box::new(expr),
                trim_where,
                trim_what: Some(trim_what),
            })
        } else {
            self.expect_token(&Token::RParen)?;
            Ok(Expr::Trim {
                expr: Box::new(expr),
                trim_where,
                trim_what: None,
            })
        }
    }

    pub fn parse_trim_where(&mut self) -> Result<TrimWhereField, ParserError> {
        let next_token = self.next_token();
        match &next_token.token {
            Token::Word(w) => match w.keyword {
                Keyword::BOTH => Ok(TrimWhereField::Both),
                Keyword::LEADING => Ok(TrimWhereField::Leading),
                Keyword::TRAILING => Ok(TrimWhereField::Trailing),
                _ => self.expected("trim_where field", next_token)?,
            },
            _ => self.expected("trim_where field", next_token),
        }
    }

    /// Parses an array expression `[ex1, ex2, ..]`
    /// if `named` is `true`, came from an expression like  `ARRAY[ex1, ex2]`
    pub fn parse_array_expr(&mut self, named: bool) -> Result<Expr, ParserError> {
        if self.peek_token().token == Token::RBracket {
            let _ = self.next_token(); // consume ]
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

    // Parses an array constructed from a subquery
    pub fn parse_array_subquery(&mut self) -> Result<Expr, ParserError> {
        let query = self.parse_query()?;
        self.expect_token(&Token::RParen)?;
        Ok(Expr::ArraySubquery(Box::new(query)))
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
                let filler = match self.peek_token().token {
                    Token::Word(w)
                        if w.keyword == Keyword::WITH || w.keyword == Keyword::WITHOUT =>
                    {
                        None
                    }
                    Token::SingleQuotedString(_)
                    | Token::EscapedStringLiteral(_)
                    | Token::NationalStringLiteral(_)
                    | Token::HexStringLiteral(_) => Some(Box::new(self.parse_expr()?)),
                    _ => self.expected(
                        "either filler, WITH, or WITHOUT in LISTAGG",
                        self.peek_token(),
                    )?,
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

    pub fn parse_array_agg_expr(&mut self) -> Result<Expr, ParserError> {
        self.expect_token(&Token::LParen)?;
        let distinct = self.parse_keyword(Keyword::DISTINCT);
        let expr = Box::new(self.parse_expr()?);
        // ANSI SQL and BigQuery define ORDER BY inside function.
        if !self.dialect.supports_within_after_array_aggregation() {
            let order_by = if self.parse_keywords(&[Keyword::ORDER, Keyword::BY]) {
                let order_by_expr = self.parse_order_by_expr()?;
                Some(Box::new(order_by_expr))
            } else {
                None
            };
            let limit = if self.parse_keyword(Keyword::LIMIT) {
                self.parse_limit()?.map(Box::new)
            } else {
                None
            };
            self.expect_token(&Token::RParen)?;
            return Ok(Expr::ArrayAgg(ArrayAgg {
                distinct,
                expr,
                order_by,
                limit,
                within_group: false,
            }));
        }
        // Snowflake defines ORDERY BY in within group instead of inside the function like
        // ANSI SQL.
        self.expect_token(&Token::RParen)?;
        let within_group = if self.parse_keywords(&[Keyword::WITHIN, Keyword::GROUP]) {
            self.expect_token(&Token::LParen)?;
            self.expect_keywords(&[Keyword::ORDER, Keyword::BY])?;
            let order_by_expr = self.parse_order_by_expr()?;
            self.expect_token(&Token::RParen)?;
            Some(Box::new(order_by_expr))
        } else {
            None
        };

        Ok(Expr::ArrayAgg(ArrayAgg {
            distinct,
            expr,
            order_by: within_group,
            limit: None,
            within_group: true,
        }))
    }

    // This function parses date/time fields for the EXTRACT function-like
    // operator, interval qualifiers, and the ceil/floor operations.
    // EXTRACT supports a wider set of date/time fields than interval qualifiers,
    // so this function may need to be split in two.
    pub fn parse_date_time_field(&mut self) -> Result<DateTimeField, ParserError> {
        let next_token = self.next_token();
        match &next_token.token {
            Token::Word(w) => match w.keyword {
                Keyword::YEAR => Ok(DateTimeField::Year),
                Keyword::MONTH => Ok(DateTimeField::Month),
                Keyword::WEEK => Ok(DateTimeField::Week),
                Keyword::DAY => Ok(DateTimeField::Day),
                Keyword::DATE => Ok(DateTimeField::Date),
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
                Keyword::MICROSECOND => Ok(DateTimeField::Microsecond),
                Keyword::MICROSECONDS => Ok(DateTimeField::Microseconds),
                Keyword::MILLENIUM => Ok(DateTimeField::Millenium),
                Keyword::MILLENNIUM => Ok(DateTimeField::Millennium),
                Keyword::MILLISECOND => Ok(DateTimeField::Millisecond),
                Keyword::MILLISECONDS => Ok(DateTimeField::Milliseconds),
                Keyword::NANOSECOND => Ok(DateTimeField::Nanosecond),
                Keyword::NANOSECONDS => Ok(DateTimeField::Nanoseconds),
                Keyword::QUARTER => Ok(DateTimeField::Quarter),
                Keyword::TIMEZONE => Ok(DateTimeField::Timezone),
                Keyword::TIMEZONE_HOUR => Ok(DateTimeField::TimezoneHour),
                Keyword::TIMEZONE_MINUTE => Ok(DateTimeField::TimezoneMinute),
                _ => self.expected("date/time field", next_token),
            },
            _ => self.expected("date/time field", next_token),
        }
    }

    pub fn parse_not(&mut self) -> Result<Expr, ParserError> {
        match self.peek_token().token {
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

    /// Parses fulltext expressions [(1)]
    ///
    /// # Errors
    /// This method will raise an error if the column list is empty or with invalid identifiers,
    /// the match expression is not a literal string, or if the search modifier is not valid.
    ///
    /// [(1)]: Expr::MatchAgainst
    pub fn parse_match_against(&mut self) -> Result<Expr, ParserError> {
        let columns = self.parse_parenthesized_column_list(Mandatory)?;

        self.expect_keyword(Keyword::AGAINST)?;

        self.expect_token(&Token::LParen)?;

        // MySQL is too permissive about the value, IMO we can't validate it perfectly on syntax level.
        let match_value = self.parse_value()?;

        let in_natural_language_mode_keywords = &[
            Keyword::IN,
            Keyword::NATURAL,
            Keyword::LANGUAGE,
            Keyword::MODE,
        ];

        let with_query_expansion_keywords = &[Keyword::WITH, Keyword::QUERY, Keyword::EXPANSION];

        let in_boolean_mode_keywords = &[Keyword::IN, Keyword::BOOLEAN, Keyword::MODE];

        let opt_search_modifier = if self.parse_keywords(in_natural_language_mode_keywords) {
            if self.parse_keywords(with_query_expansion_keywords) {
                Some(SearchModifier::InNaturalLanguageModeWithQueryExpansion)
            } else {
                Some(SearchModifier::InNaturalLanguageMode)
            }
        } else if self.parse_keywords(in_boolean_mode_keywords) {
            Some(SearchModifier::InBooleanMode)
        } else if self.parse_keywords(with_query_expansion_keywords) {
            Some(SearchModifier::WithQueryExpansion)
        } else {
            None
        };

        self.expect_token(&Token::RParen)?;

        Ok(Expr::MatchAgainst {
            columns,
            match_value,
            opt_search_modifier,
        })
    }

    /// Parse an INTERVAL expression.
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
    pub fn parse_interval(&mut self) -> Result<Expr, ParserError> {
        // The SQL standard allows an optional sign before the value string, but
        // it is not clear if any implementations support that syntax, so we
        // don't currently try to parse it. (The sign can instead be included
        // inside the value string.)

        // The first token in an interval is a string literal which specifies
        // the duration of the interval.
        let value = self.parse_interval_expr()?;

        // Following the string literal is a qualifier which indicates the units
        // of the duration specified in the string literal.
        //
        // Note that PostgreSQL allows omitting the qualifier, so we provide
        // this more general implementation.
        let leading_field = match self.peek_token().token {
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
                    Keyword::MICROSECOND,
                    Keyword::MICROSECONDS,
                    Keyword::MILLENIUM,
                    Keyword::MILLENNIUM,
                    Keyword::MILLISECOND,
                    Keyword::MILLISECONDS,
                    Keyword::NANOSECOND,
                    Keyword::NANOSECONDS,
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

        Ok(Expr::Interval {
            value: Box::new(value),
            leading_field,
            leading_precision,
            last_field,
            fractional_seconds_precision: fsec_precision,
        })
    }

    /// Parse an operator following an expression
    pub fn parse_infix(&mut self, expr: Expr, precedence: u8) -> Result<Expr, ParserError> {
        // allow the dialect to override infix parsing
        if let Some(infix) = self.dialect.parse_infix(self, &expr, precedence) {
            return infix;
        }

        let tok = self.next_token();

        let regular_binary_operator = match &tok.token {
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
            Token::ShiftLeft if dialect_of!(self is PostgreSqlDialect | GenericDialect) => {
                Some(BinaryOperator::PGBitwiseShiftLeft)
            }
            Token::ShiftRight if dialect_of!(self is PostgreSqlDialect | GenericDialect) => {
                Some(BinaryOperator::PGBitwiseShiftRight)
            }
            Token::Sharp if dialect_of!(self is PostgreSqlDialect) => {
                Some(BinaryOperator::PGBitwiseXor)
            }
            Token::Tilde => Some(BinaryOperator::PGRegexMatch),
            Token::TildeAsterisk => Some(BinaryOperator::PGRegexIMatch),
            Token::ExclamationMarkTilde => Some(BinaryOperator::PGRegexNotMatch),
            Token::ExclamationMarkTildeAsterisk => Some(BinaryOperator::PGRegexNotIMatch),
            Token::Word(w) => match w.keyword {
                Keyword::AND => Some(BinaryOperator::And),
                Keyword::OR => Some(BinaryOperator::Or),
                Keyword::XOR => Some(BinaryOperator::Xor),
                Keyword::OPERATOR if dialect_of!(self is PostgreSqlDialect | GenericDialect) => {
                    self.expect_token(&Token::LParen)?;
                    // there are special rules for operator names in
                    // postgres so we can not use 'parse_object'
                    // or similar.
                    // See https://www.postgresql.org/docs/current/sql-createoperator.html
                    let mut idents = vec![];
                    loop {
                        idents.push(self.next_token().to_string());
                        if !self.consume_token(&Token::Period) {
                            break;
                        }
                    }
                    self.expect_token(&Token::RParen)?;
                    Some(BinaryOperator::PGCustomBinaryOperator(idents))
                }
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
        } else if let Token::Word(w) = &tok.token {
            match w.keyword {
                Keyword::IS => {
                    if self.parse_keyword(Keyword::NULL) {
                        Ok(Expr::IsNull(Box::new(expr)))
                    } else if self.parse_keywords(&[Keyword::NOT, Keyword::NULL]) {
                        Ok(Expr::IsNotNull(Box::new(expr)))
                    } else if self.parse_keywords(&[Keyword::TRUE]) {
                        Ok(Expr::IsTrue(Box::new(expr)))
                    } else if self.parse_keywords(&[Keyword::NOT, Keyword::TRUE]) {
                        Ok(Expr::IsNotTrue(Box::new(expr)))
                    } else if self.parse_keywords(&[Keyword::FALSE]) {
                        Ok(Expr::IsFalse(Box::new(expr)))
                    } else if self.parse_keywords(&[Keyword::NOT, Keyword::FALSE]) {
                        Ok(Expr::IsNotFalse(Box::new(expr)))
                    } else if self.parse_keywords(&[Keyword::UNKNOWN]) {
                        Ok(Expr::IsUnknown(Box::new(expr)))
                    } else if self.parse_keywords(&[Keyword::NOT, Keyword::UNKNOWN]) {
                        Ok(Expr::IsNotUnknown(Box::new(expr)))
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
                        match time_zone.token {
                            Token::SingleQuotedString(time_zone) => {
                                log::trace!("Peek token: {:?}", self.peek_token());
                                Ok(Expr::AtTimeZone {
                                    timestamp: Box::new(expr),
                                    time_zone,
                                })
                            }
                            _ => self.expected(
                                "Expected Token::SingleQuotedString after AT TIME ZONE",
                                time_zone,
                            ),
                        }
                    } else {
                        self.expected("Expected Token::Word after AT", tok)
                    }
                }
                Keyword::NOT
                | Keyword::IN
                | Keyword::BETWEEN
                | Keyword::LIKE
                | Keyword::ILIKE
                | Keyword::SIMILAR => {
                    self.prev_token();
                    let negated = self.parse_keyword(Keyword::NOT);
                    if self.parse_keyword(Keyword::IN) {
                        self.parse_in(expr, negated)
                    } else if self.parse_keyword(Keyword::BETWEEN) {
                        self.parse_between(expr, negated)
                    } else if self.parse_keyword(Keyword::LIKE) {
                        Ok(Expr::Like {
                            negated,
                            expr: Box::new(expr),
                            pattern: Box::new(self.parse_subexpr(Self::LIKE_PREC)?),
                            escape_char: self.parse_escape_char()?,
                        })
                    } else if self.parse_keyword(Keyword::ILIKE) {
                        Ok(Expr::ILike {
                            negated,
                            expr: Box::new(expr),
                            pattern: Box::new(self.parse_subexpr(Self::LIKE_PREC)?),
                            escape_char: self.parse_escape_char()?,
                        })
                    } else if self.parse_keywords(&[Keyword::SIMILAR, Keyword::TO]) {
                        Ok(Expr::SimilarTo {
                            negated,
                            expr: Box::new(expr),
                            pattern: Box::new(self.parse_subexpr(Self::LIKE_PREC)?),
                            escape_char: self.parse_escape_char()?,
                        })
                    } else {
                        self.expected("IN or BETWEEN after NOT", self.peek_token())
                    }
                }
                // Can only happen if `get_next_precedence` got out of sync with this function
                _ => parser_err!(format!("No infix parser for token {:?}", tok.token)),
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
            if dialect_of!(self is PostgreSqlDialect | GenericDialect) {
                // parse index
                return self.parse_array_index(expr);
            }
            self.parse_map_access(expr)
        } else if Token::Colon == tok {
            Ok(Expr::JsonAccess {
                left: Box::new(expr),
                operator: JsonOperator::Colon,
                right: Box::new(Expr::Value(self.parse_value()?)),
            })
        } else if Token::Arrow == tok
            || Token::LongArrow == tok
            || Token::HashArrow == tok
            || Token::HashLongArrow == tok
            || Token::AtArrow == tok
            || Token::ArrowAt == tok
            || Token::HashMinus == tok
            || Token::AtQuestion == tok
            || Token::AtAt == tok
        {
            let operator = match tok.token {
                Token::Arrow => JsonOperator::Arrow,
                Token::LongArrow => JsonOperator::LongArrow,
                Token::HashArrow => JsonOperator::HashArrow,
                Token::HashLongArrow => JsonOperator::HashLongArrow,
                Token::AtArrow => JsonOperator::AtArrow,
                Token::ArrowAt => JsonOperator::ArrowAt,
                Token::HashMinus => JsonOperator::HashMinus,
                Token::AtQuestion => JsonOperator::AtQuestion,
                Token::AtAt => JsonOperator::AtAt,
                _ => unreachable!(),
            };
            Ok(Expr::JsonAccess {
                left: Box::new(expr),
                operator,
                right: Box::new(self.parse_expr()?),
            })
        } else {
            // Can only happen if `get_next_precedence` got out of sync with this function
            parser_err!(format!("No infix parser for token {:?}", tok.token))
        }
    }

    /// parse the ESCAPE CHAR portion of LIKE, ILIKE, and SIMILAR TO
    pub fn parse_escape_char(&mut self) -> Result<Option<char>, ParserError> {
        if self.parse_keyword(Keyword::ESCAPE) {
            Ok(Some(self.parse_literal_char()?))
        } else {
            Ok(None)
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

    // use https://www.postgresql.org/docs/7.0/operators.htm#AEN2026 as a reference
    const PLUS_MINUS_PREC: u8 = 30;
    const XOR_PREC: u8 = 24;
    const TIME_ZONE_PREC: u8 = 20;
    const BETWEEN_PREC: u8 = 20;
    const LIKE_PREC: u8 = 19;
    const IS_PREC: u8 = 17;
    const UNARY_NOT_PREC: u8 = 15;
    const AND_PREC: u8 = 10;
    const OR_PREC: u8 = 5;

    /// Get the precedence of the next token
    pub fn get_next_precedence(&self) -> Result<u8, ParserError> {
        // allow the dialect to override precedence logic
        if let Some(precedence) = self.dialect.get_next_precedence(self) {
            return precedence;
        }

        let token = self.peek_token();
        debug!("get_next_precedence() {:?}", token);
        let token_0 = self.peek_nth_token(0);
        let token_1 = self.peek_nth_token(1);
        let token_2 = self.peek_nth_token(2);
        debug!("0: {token_0} 1: {token_1} 2: {token_2}");
        match token.token {
            Token::Word(w) if w.keyword == Keyword::OR => Ok(Self::OR_PREC),
            Token::Word(w) if w.keyword == Keyword::AND => Ok(Self::AND_PREC),
            Token::Word(w) if w.keyword == Keyword::XOR => Ok(Self::XOR_PREC),

            Token::Word(w) if w.keyword == Keyword::AT => {
                match (self.peek_nth_token(1).token, self.peek_nth_token(2).token) {
                    (Token::Word(w), Token::Word(w2))
                        if w.keyword == Keyword::TIME && w2.keyword == Keyword::ZONE =>
                    {
                        Ok(Self::TIME_ZONE_PREC)
                    }
                    _ => Ok(0),
                }
            }

            Token::Word(w) if w.keyword == Keyword::NOT => match self.peek_nth_token(1).token {
                // The precedence of NOT varies depending on keyword that
                // follows it. If it is followed by IN, BETWEEN, or LIKE,
                // it takes on the precedence of those tokens. Otherwise it
                // is not an infix operator, and therefore has zero
                // precedence.
                Token::Word(w) if w.keyword == Keyword::IN => Ok(Self::BETWEEN_PREC),
                Token::Word(w) if w.keyword == Keyword::BETWEEN => Ok(Self::BETWEEN_PREC),
                Token::Word(w) if w.keyword == Keyword::LIKE => Ok(Self::LIKE_PREC),
                Token::Word(w) if w.keyword == Keyword::ILIKE => Ok(Self::LIKE_PREC),
                Token::Word(w) if w.keyword == Keyword::SIMILAR => Ok(Self::LIKE_PREC),
                _ => Ok(0),
            },
            Token::Word(w) if w.keyword == Keyword::IS => Ok(Self::IS_PREC),
            Token::Word(w) if w.keyword == Keyword::IN => Ok(Self::BETWEEN_PREC),
            Token::Word(w) if w.keyword == Keyword::BETWEEN => Ok(Self::BETWEEN_PREC),
            Token::Word(w) if w.keyword == Keyword::LIKE => Ok(Self::LIKE_PREC),
            Token::Word(w) if w.keyword == Keyword::ILIKE => Ok(Self::LIKE_PREC),
            Token::Word(w) if w.keyword == Keyword::SIMILAR => Ok(Self::LIKE_PREC),
            Token::Word(w) if w.keyword == Keyword::OPERATOR => Ok(Self::BETWEEN_PREC),
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
            Token::Colon => Ok(50),
            Token::ExclamationMark => Ok(50),
            Token::LBracket
            | Token::LongArrow
            | Token::Arrow
            | Token::HashArrow
            | Token::HashLongArrow
            | Token::AtArrow
            | Token::ArrowAt
            | Token::HashMinus
            | Token::AtQuestion
            | Token::AtAt => Ok(50),
            _ => Ok(0),
        }
    }

    /// Return the first non-whitespace token that has not yet been processed
    /// (or None if reached end-of-file)
    pub fn peek_token(&self) -> TokenWithLocation {
        self.peek_nth_token(0)
    }

    /// Return nth non-whitespace token that has not yet been processed
    pub fn peek_nth_token(&self, mut n: usize) -> TokenWithLocation {
        let mut index = self.index;
        loop {
            index += 1;
            match self.tokens.get(index - 1) {
                Some(TokenWithLocation {
                    token: Token::Whitespace(_),
                    location: _,
                }) => continue,
                non_whitespace => {
                    if n == 0 {
                        return non_whitespace.cloned().unwrap_or(TokenWithLocation {
                            token: Token::EOF,
                            location: Location { line: 0, column: 0 },
                        });
                    }
                    n -= 1;
                }
            }
        }
    }

    /// Return the first non-whitespace token that has not yet been processed
    /// (or None if reached end-of-file) and mark it as processed. OK to call
    /// repeatedly after reaching EOF.
    pub fn next_token(&mut self) -> TokenWithLocation {
        loop {
            self.index += 1;
            match self.tokens.get(self.index - 1) {
                Some(TokenWithLocation {
                    token: Token::Whitespace(_),
                    location: _,
                }) => continue,
                token => {
                    return token
                        .cloned()
                        .unwrap_or_else(|| TokenWithLocation::wrap(Token::EOF))
                }
            }
        }
    }

    /// Return the first unprocessed token, possibly whitespace.
    pub fn next_token_no_skip(&mut self) -> Option<&TokenWithLocation> {
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
            if let Some(TokenWithLocation {
                token: Token::Whitespace(_),
                location: _,
            }) = self.tokens.get(self.index)
            {
                continue;
            }
            return;
        }
    }

    /// Report unexpected token
    pub fn expected<T>(&self, expected: &str, found: TokenWithLocation) -> Result<T, ParserError> {
        parser_err!(format!("Expected {}, found: {}", expected, found))
    }

    /// Look for an expected keyword and consume it if it exists
    #[must_use]
    pub fn parse_keyword(&mut self, expected: Keyword) -> bool {
        match self.peek_token().token {
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
        match self.peek_token().token {
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

    /// Parse a comma-separated list of 1+ SelectItem
    pub fn parse_projection(&mut self) -> Result<Vec<SelectItem>, ParserError> {
        let mut values = vec![];
        loop {
            values.push(self.parse_select_item()?);
            if !self.consume_token(&Token::Comma) {
                break;
            } else if dialect_of!(self is BigQueryDialect) {
                // BigQuery allows trailing commas.
                // e.g. `SELECT 1, 2, FROM t`
                // https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#trailing_commas
                match self.peek_token().token {
                    Token::Word(kw)
                        if keywords::RESERVED_FOR_COLUMN_ALIAS
                            .iter()
                            .any(|d| kw.keyword == *d) =>
                    {
                        break;
                    }
                    Token::RParen | Token::EOF => break,
                    _ => continue,
                }
            }
        }
        Ok(values)
    }

    /// Parse a comma-separated list of 1+ items accepted by `F`
    pub fn parse_comma_separated<T, F>(&mut self, mut f: F) -> Result<Vec<T>, ParserError>
    where
        F: FnMut(&mut Parser<'a>) -> Result<T, ParserError>,
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
            parser_err!("Cannot specify both ALL and DISTINCT".to_string())
        } else {
            Ok(distinct)
        }
    }

    /// Parse a SQL CREATE statement
    pub fn parse_create(&mut self) -> Result<Statement, ParserError> {
        let or_replace = self.parse_keywords(&[Keyword::OR, Keyword::REPLACE]);
        let local = self.parse_one_of_keywords(&[Keyword::LOCAL]).is_some();
        let global = self.parse_one_of_keywords(&[Keyword::GLOBAL]).is_some();
        let global: Option<bool> = if global {
            Some(true)
        } else if local {
            Some(false)
        } else {
            None
        };
        let temporary = self
            .parse_one_of_keywords(&[Keyword::TEMP, Keyword::TEMPORARY])
            .is_some();
        if self.parse_keyword(Keyword::TABLE) {
            self.parse_create_table(or_replace, temporary, global)
        } else if self.parse_keyword(Keyword::MATERIALIZED) || self.parse_keyword(Keyword::VIEW) {
            self.prev_token();
            self.parse_create_view(or_replace)
        } else if self.parse_keyword(Keyword::EXTERNAL) {
            self.parse_create_external_table(or_replace)
        } else if self.parse_keyword(Keyword::FUNCTION) {
            self.parse_create_function(or_replace, temporary)
        } else if or_replace {
            self.expected(
                "[EXTERNAL] TABLE or [MATERIALIZED] VIEW or FUNCTION after CREATE OR REPLACE",
                self.peek_token(),
            )
        } else if self.parse_keyword(Keyword::INDEX) {
            self.parse_create_index(false)
        } else if self.parse_keywords(&[Keyword::UNIQUE, Keyword::INDEX]) {
            self.parse_create_index(true)
        } else if self.parse_keyword(Keyword::VIRTUAL) {
            self.parse_create_virtual_table()
        } else if self.parse_keyword(Keyword::SCHEMA) {
            self.parse_create_schema()
        } else if self.parse_keyword(Keyword::DATABASE) {
            self.parse_create_database()
        } else if self.parse_keyword(Keyword::ROLE) {
            self.parse_create_role()
        } else if self.parse_keyword(Keyword::SEQUENCE) {
            self.parse_create_sequence(temporary)
        } else {
            self.expected("an object type after CREATE", self.peek_token())
        }
    }

    /// Parse a CACHE TABLE statement
    pub fn parse_cache_table(&mut self) -> Result<Statement, ParserError> {
        let (mut table_flag, mut options, mut has_as, mut query) = (None, vec![], false, None);
        if self.parse_keyword(Keyword::TABLE) {
            let table_name = self.parse_object_name()?;
            if self.peek_token().token != Token::EOF {
                if let Token::Word(word) = self.peek_token().token {
                    if word.keyword == Keyword::OPTIONS {
                        options = self.parse_options(Keyword::OPTIONS)?
                    }
                };

                if self.peek_token().token != Token::EOF {
                    let (a, q) = self.parse_as_query()?;
                    has_as = a;
                    query = Some(q);
                }

                Ok(Statement::Cache {
                    table_flag,
                    table_name,
                    has_as,
                    options,
                    query,
                })
            } else {
                Ok(Statement::Cache {
                    table_flag,
                    table_name,
                    has_as,
                    options,
                    query,
                })
            }
        } else {
            table_flag = Some(self.parse_object_name()?);
            if self.parse_keyword(Keyword::TABLE) {
                let table_name = self.parse_object_name()?;
                if self.peek_token() != Token::EOF {
                    if let Token::Word(word) = self.peek_token().token {
                        if word.keyword == Keyword::OPTIONS {
                            options = self.parse_options(Keyword::OPTIONS)?
                        }
                    };

                    if self.peek_token() != Token::EOF {
                        let (a, q) = self.parse_as_query()?;
                        has_as = a;
                        query = Some(q);
                    }

                    Ok(Statement::Cache {
                        table_flag,
                        table_name,
                        has_as,
                        options,
                        query,
                    })
                } else {
                    Ok(Statement::Cache {
                        table_flag,
                        table_name,
                        has_as,
                        options,
                        query,
                    })
                }
            } else {
                if self.peek_token() == Token::EOF {
                    self.prev_token();
                }
                self.expected("a `TABLE` keyword", self.peek_token())
            }
        }
    }

    /// Parse 'AS' before as query,such as `WITH XXX AS SELECT XXX` oer `CACHE TABLE AS SELECT XXX`
    pub fn parse_as_query(&mut self) -> Result<(bool, Query), ParserError> {
        match self.peek_token().token {
            Token::Word(word) => match word.keyword {
                Keyword::AS => {
                    self.next_token();
                    Ok((true, self.parse_query()?))
                }
                _ => Ok((false, self.parse_query()?)),
            },
            _ => self.expected("a QUERY statement", self.peek_token()),
        }
    }

    /// Parse a UNCACHE TABLE statement
    pub fn parse_uncache_table(&mut self) -> Result<Statement, ParserError> {
        let has_table = self.parse_keyword(Keyword::TABLE);
        if has_table {
            let if_exists = self.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
            let table_name = self.parse_object_name()?;
            if self.peek_token().token == Token::EOF {
                Ok(Statement::UNCache {
                    table_name,
                    if_exists,
                })
            } else {
                self.expected("an `EOF`", self.peek_token())
            }
        } else {
            self.expected("a `TABLE` keyword", self.peek_token())
        }
    }

    /// SQLite-specific `CREATE VIRTUAL TABLE`
    pub fn parse_create_virtual_table(&mut self) -> Result<Statement, ParserError> {
        self.expect_keyword(Keyword::TABLE)?;
        let if_not_exists = self.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let table_name = self.parse_object_name()?;
        self.expect_keyword(Keyword::USING)?;
        let module_name = self.parse_identifier()?;
        // SQLite docs note that module "arguments syntax is sufficiently
        // general that the arguments can be made to appear as column
        // definitions in a traditional CREATE TABLE statement", but
        // we don't implement that.
        let module_args = self.parse_parenthesized_column_list(Optional)?;
        Ok(Statement::CreateVirtualTable {
            name: table_name,
            if_not_exists,
            module_name,
            module_args,
        })
    }

    pub fn parse_create_schema(&mut self) -> Result<Statement, ParserError> {
        let if_not_exists = self.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);

        let schema_name = self.parse_schema_name()?;

        Ok(Statement::CreateSchema {
            schema_name,
            if_not_exists,
        })
    }

    fn parse_schema_name(&mut self) -> Result<SchemaName, ParserError> {
        if self.parse_keyword(Keyword::AUTHORIZATION) {
            Ok(SchemaName::UnnamedAuthorization(self.parse_identifier()?))
        } else {
            let name = self.parse_object_name()?;

            if self.parse_keyword(Keyword::AUTHORIZATION) {
                Ok(SchemaName::NamedAuthorization(
                    name,
                    self.parse_identifier()?,
                ))
            } else {
                Ok(SchemaName::Simple(name))
            }
        }
    }

    pub fn parse_create_database(&mut self) -> Result<Statement, ParserError> {
        let ine = self.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let db_name = self.parse_object_name()?;
        let mut location = None;
        let mut managed_location = None;
        loop {
            match self.parse_one_of_keywords(&[Keyword::LOCATION, Keyword::MANAGEDLOCATION]) {
                Some(Keyword::LOCATION) => location = Some(self.parse_literal_string()?),
                Some(Keyword::MANAGEDLOCATION) => {
                    managed_location = Some(self.parse_literal_string()?)
                }
                _ => break,
            }
        }
        Ok(Statement::CreateDatabase {
            db_name,
            if_not_exists: ine,
            location,
            managed_location,
        })
    }

    pub fn parse_optional_create_function_using(
        &mut self,
    ) -> Result<Option<CreateFunctionUsing>, ParserError> {
        if !self.parse_keyword(Keyword::USING) {
            return Ok(None);
        };
        let keyword =
            self.expect_one_of_keywords(&[Keyword::JAR, Keyword::FILE, Keyword::ARCHIVE])?;

        let uri = self.parse_literal_string()?;

        match keyword {
            Keyword::JAR => Ok(Some(CreateFunctionUsing::Jar(uri))),
            Keyword::FILE => Ok(Some(CreateFunctionUsing::File(uri))),
            Keyword::ARCHIVE => Ok(Some(CreateFunctionUsing::Archive(uri))),
            _ => self.expected(
                "JAR, FILE or ARCHIVE, got {:?}",
                TokenWithLocation::wrap(Token::make_keyword(format!("{:?}", keyword).as_str())),
            ),
        }
    }

    pub fn parse_create_function(
        &mut self,
        or_replace: bool,
        temporary: bool,
    ) -> Result<Statement, ParserError> {
        if dialect_of!(self is HiveDialect) {
            let name = self.parse_object_name()?;
            self.expect_keyword(Keyword::AS)?;
            let class_name = self.parse_function_definition()?;
            let params = CreateFunctionBody {
                as_: Some(class_name),
                using: self.parse_optional_create_function_using()?,
                ..Default::default()
            };

            Ok(Statement::CreateFunction {
                or_replace,
                temporary,
                name,
                args: None,
                return_type: None,
                params,
            })
        } else if dialect_of!(self is PostgreSqlDialect) {
            let name = self.parse_object_name()?;
            self.expect_token(&Token::LParen)?;
            let args = if self.consume_token(&Token::RParen) {
                self.prev_token();
                None
            } else {
                Some(self.parse_comma_separated(Parser::parse_function_arg)?)
            };

            self.expect_token(&Token::RParen)?;

            let return_type = if self.parse_keyword(Keyword::RETURNS) {
                Some(self.parse_data_type()?)
            } else {
                None
            };

            let params = self.parse_create_function_body()?;

            Ok(Statement::CreateFunction {
                or_replace,
                temporary,
                name,
                args,
                return_type,
                params,
            })
        } else {
            self.prev_token();
            self.expected("an object type after CREATE", self.peek_token())
        }
    }

    fn parse_function_arg(&mut self) -> Result<OperateFunctionArg, ParserError> {
        let mode = if self.parse_keyword(Keyword::IN) {
            Some(ArgMode::In)
        } else if self.parse_keyword(Keyword::OUT) {
            Some(ArgMode::Out)
        } else if self.parse_keyword(Keyword::INOUT) {
            Some(ArgMode::InOut)
        } else {
            None
        };

        // parse: [ argname ] argtype
        let mut name = None;
        let mut data_type = self.parse_data_type()?;
        if let DataType::Custom(n, _) = &data_type {
            // the first token is actually a name
            name = Some(n.0[0].clone());
            data_type = self.parse_data_type()?;
        }

        let default_expr = if self.parse_keyword(Keyword::DEFAULT) || self.consume_token(&Token::Eq)
        {
            Some(self.parse_expr()?)
        } else {
            None
        };
        Ok(OperateFunctionArg {
            mode,
            name,
            data_type,
            default_expr,
        })
    }

    fn parse_create_function_body(&mut self) -> Result<CreateFunctionBody, ParserError> {
        let mut body = CreateFunctionBody::default();
        loop {
            fn ensure_not_set<T>(field: &Option<T>, name: &str) -> Result<(), ParserError> {
                if field.is_some() {
                    return Err(ParserError::ParserError(format!(
                        "{name} specified more than once",
                    )));
                }
                Ok(())
            }
            if self.parse_keyword(Keyword::AS) {
                ensure_not_set(&body.as_, "AS")?;
                body.as_ = Some(self.parse_function_definition()?);
            } else if self.parse_keyword(Keyword::LANGUAGE) {
                ensure_not_set(&body.language, "LANGUAGE")?;
                body.language = Some(self.parse_identifier()?);
            } else if self.parse_keyword(Keyword::IMMUTABLE) {
                ensure_not_set(&body.behavior, "IMMUTABLE | STABLE | VOLATILE")?;
                body.behavior = Some(FunctionBehavior::Immutable);
            } else if self.parse_keyword(Keyword::STABLE) {
                ensure_not_set(&body.behavior, "IMMUTABLE | STABLE | VOLATILE")?;
                body.behavior = Some(FunctionBehavior::Stable);
            } else if self.parse_keyword(Keyword::VOLATILE) {
                ensure_not_set(&body.behavior, "IMMUTABLE | STABLE | VOLATILE")?;
                body.behavior = Some(FunctionBehavior::Volatile);
            } else if self.parse_keyword(Keyword::RETURN) {
                ensure_not_set(&body.return_, "RETURN")?;
                body.return_ = Some(self.parse_expr()?);
            } else {
                return Ok(body);
            }
        }
    }

    pub fn parse_create_external_table(
        &mut self,
        or_replace: bool,
    ) -> Result<Statement, ParserError> {
        self.expect_keyword(Keyword::TABLE)?;
        let if_not_exists = self.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let table_name = self.parse_object_name()?;
        let (columns, constraints) = self.parse_columns()?;

        let hive_distribution = self.parse_hive_distribution()?;
        let hive_formats = self.parse_hive_formats()?;

        let file_format = if let Some(ff) = &hive_formats.storage {
            match ff {
                HiveIOFormat::FileFormat { format } => Some(*format),
                _ => None,
            }
        } else {
            None
        };
        let location = hive_formats.location.clone();
        let table_properties = self.parse_options(Keyword::TBLPROPERTIES)?;
        Ok(CreateTableBuilder::new(table_name)
            .columns(columns)
            .constraints(constraints)
            .hive_distribution(hive_distribution)
            .hive_formats(Some(hive_formats))
            .table_properties(table_properties)
            .or_replace(or_replace)
            .if_not_exists(if_not_exists)
            .external(true)
            .file_format(file_format)
            .location(location)
            .build())
    }

    pub fn parse_file_format(&mut self) -> Result<FileFormat, ParserError> {
        let next_token = self.next_token();
        match &next_token.token {
            Token::Word(w) => match w.keyword {
                Keyword::AVRO => Ok(FileFormat::AVRO),
                Keyword::JSONFILE => Ok(FileFormat::JSONFILE),
                Keyword::ORC => Ok(FileFormat::ORC),
                Keyword::PARQUET => Ok(FileFormat::PARQUET),
                Keyword::RCFILE => Ok(FileFormat::RCFILE),
                Keyword::SEQUENCEFILE => Ok(FileFormat::SEQUENCEFILE),
                Keyword::TEXTFILE => Ok(FileFormat::TEXTFILE),
                _ => self.expected("fileformat", next_token),
            },
            _ => self.expected("fileformat", next_token),
        }
    }

    pub fn parse_analyze_format(&mut self) -> Result<AnalyzeFormat, ParserError> {
        let next_token = self.next_token();
        match &next_token.token {
            Token::Word(w) => match w.keyword {
                Keyword::TEXT => Ok(AnalyzeFormat::TEXT),
                Keyword::GRAPHVIZ => Ok(AnalyzeFormat::GRAPHVIZ),
                Keyword::JSON => Ok(AnalyzeFormat::JSON),
                _ => self.expected("fileformat", next_token),
            },
            _ => self.expected("fileformat", next_token),
        }
    }

    pub fn parse_create_view(&mut self, or_replace: bool) -> Result<Statement, ParserError> {
        let materialized = self.parse_keyword(Keyword::MATERIALIZED);
        self.expect_keyword(Keyword::VIEW)?;
        // Many dialects support `OR ALTER` right after `CREATE`, but we don't (yet).
        // ANSI SQL and Postgres support RECURSIVE here, but we don't support it either.
        let name = self.parse_object_name()?;
        let columns = self.parse_parenthesized_column_list(Optional)?;
        let with_options = self.parse_options(Keyword::WITH)?;

        let cluster_by = if self.parse_keyword(Keyword::CLUSTER) {
            self.expect_keyword(Keyword::BY)?;
            self.parse_parenthesized_column_list(Optional)?
        } else {
            vec![]
        };

        self.expect_keyword(Keyword::AS)?;
        let query = Box::new(self.parse_query()?);
        // Optional `WITH [ CASCADED | LOCAL ] CHECK OPTION` is widely supported here.
        Ok(Statement::CreateView {
            name,
            columns,
            query,
            materialized,
            or_replace,
            with_options,
            cluster_by,
        })
    }

    pub fn parse_create_role(&mut self) -> Result<Statement, ParserError> {
        let if_not_exists = self.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let names = self.parse_comma_separated(Parser::parse_object_name)?;

        let _ = self.parse_keyword(Keyword::WITH); // [ WITH ]

        let optional_keywords = if dialect_of!(self is MsSqlDialect) {
            vec![Keyword::AUTHORIZATION]
        } else if dialect_of!(self is PostgreSqlDialect) {
            vec![
                Keyword::LOGIN,
                Keyword::NOLOGIN,
                Keyword::INHERIT,
                Keyword::NOINHERIT,
                Keyword::BYPASSRLS,
                Keyword::NOBYPASSRLS,
                Keyword::PASSWORD,
                Keyword::CREATEDB,
                Keyword::NOCREATEDB,
                Keyword::CREATEROLE,
                Keyword::NOCREATEROLE,
                Keyword::SUPERUSER,
                Keyword::NOSUPERUSER,
                Keyword::REPLICATION,
                Keyword::NOREPLICATION,
                Keyword::CONNECTION,
                Keyword::VALID,
                Keyword::IN,
                Keyword::ROLE,
                Keyword::ADMIN,
                Keyword::USER,
            ]
        } else {
            vec![]
        };

        // MSSQL
        let mut authorization_owner = None;
        // Postgres
        let mut login = None;
        let mut inherit = None;
        let mut bypassrls = None;
        let mut password = None;
        let mut create_db = None;
        let mut create_role = None;
        let mut superuser = None;
        let mut replication = None;
        let mut connection_limit = None;
        let mut valid_until = None;
        let mut in_role = vec![];
        let mut in_group = vec![];
        let mut role = vec![];
        let mut user = vec![];
        let mut admin = vec![];

        while let Some(keyword) = self.parse_one_of_keywords(&optional_keywords) {
            match keyword {
                Keyword::AUTHORIZATION => {
                    if authorization_owner.is_some() {
                        parser_err!("Found multiple AUTHORIZATION")
                    } else {
                        authorization_owner = Some(self.parse_object_name()?);
                        Ok(())
                    }
                }
                Keyword::LOGIN | Keyword::NOLOGIN => {
                    if login.is_some() {
                        parser_err!("Found multiple LOGIN or NOLOGIN")
                    } else {
                        login = Some(keyword == Keyword::LOGIN);
                        Ok(())
                    }
                }
                Keyword::INHERIT | Keyword::NOINHERIT => {
                    if inherit.is_some() {
                        parser_err!("Found multiple INHERIT or NOINHERIT")
                    } else {
                        inherit = Some(keyword == Keyword::INHERIT);
                        Ok(())
                    }
                }
                Keyword::BYPASSRLS | Keyword::NOBYPASSRLS => {
                    if bypassrls.is_some() {
                        parser_err!("Found multiple BYPASSRLS or NOBYPASSRLS")
                    } else {
                        bypassrls = Some(keyword == Keyword::BYPASSRLS);
                        Ok(())
                    }
                }
                Keyword::CREATEDB | Keyword::NOCREATEDB => {
                    if create_db.is_some() {
                        parser_err!("Found multiple CREATEDB or NOCREATEDB")
                    } else {
                        create_db = Some(keyword == Keyword::CREATEDB);
                        Ok(())
                    }
                }
                Keyword::CREATEROLE | Keyword::NOCREATEROLE => {
                    if create_role.is_some() {
                        parser_err!("Found multiple CREATEROLE or NOCREATEROLE")
                    } else {
                        create_role = Some(keyword == Keyword::CREATEROLE);
                        Ok(())
                    }
                }
                Keyword::SUPERUSER | Keyword::NOSUPERUSER => {
                    if superuser.is_some() {
                        parser_err!("Found multiple SUPERUSER or NOSUPERUSER")
                    } else {
                        superuser = Some(keyword == Keyword::SUPERUSER);
                        Ok(())
                    }
                }
                Keyword::REPLICATION | Keyword::NOREPLICATION => {
                    if replication.is_some() {
                        parser_err!("Found multiple REPLICATION or NOREPLICATION")
                    } else {
                        replication = Some(keyword == Keyword::REPLICATION);
                        Ok(())
                    }
                }
                Keyword::PASSWORD => {
                    if password.is_some() {
                        parser_err!("Found multiple PASSWORD")
                    } else {
                        password = if self.parse_keyword(Keyword::NULL) {
                            Some(Password::NullPassword)
                        } else {
                            Some(Password::Password(Expr::Value(self.parse_value()?)))
                        };
                        Ok(())
                    }
                }
                Keyword::CONNECTION => {
                    self.expect_keyword(Keyword::LIMIT)?;
                    if connection_limit.is_some() {
                        parser_err!("Found multiple CONNECTION LIMIT")
                    } else {
                        connection_limit = Some(Expr::Value(self.parse_number_value()?));
                        Ok(())
                    }
                }
                Keyword::VALID => {
                    self.expect_keyword(Keyword::UNTIL)?;
                    if valid_until.is_some() {
                        parser_err!("Found multiple VALID UNTIL")
                    } else {
                        valid_until = Some(Expr::Value(self.parse_value()?));
                        Ok(())
                    }
                }
                Keyword::IN => {
                    if self.parse_keyword(Keyword::ROLE) {
                        if !in_role.is_empty() {
                            parser_err!("Found multiple IN ROLE")
                        } else {
                            in_role = self.parse_comma_separated(Parser::parse_identifier)?;
                            Ok(())
                        }
                    } else if self.parse_keyword(Keyword::GROUP) {
                        if !in_group.is_empty() {
                            parser_err!("Found multiple IN GROUP")
                        } else {
                            in_group = self.parse_comma_separated(Parser::parse_identifier)?;
                            Ok(())
                        }
                    } else {
                        self.expected("ROLE or GROUP after IN", self.peek_token())
                    }
                }
                Keyword::ROLE => {
                    if !role.is_empty() {
                        parser_err!("Found multiple ROLE")
                    } else {
                        role = self.parse_comma_separated(Parser::parse_identifier)?;
                        Ok(())
                    }
                }
                Keyword::USER => {
                    if !user.is_empty() {
                        parser_err!("Found multiple USER")
                    } else {
                        user = self.parse_comma_separated(Parser::parse_identifier)?;
                        Ok(())
                    }
                }
                Keyword::ADMIN => {
                    if !admin.is_empty() {
                        parser_err!("Found multiple ADMIN")
                    } else {
                        admin = self.parse_comma_separated(Parser::parse_identifier)?;
                        Ok(())
                    }
                }
                _ => break,
            }?
        }

        Ok(Statement::CreateRole {
            names,
            if_not_exists,
            login,
            inherit,
            bypassrls,
            password,
            create_db,
            create_role,
            replication,
            superuser,
            connection_limit,
            valid_until,
            in_role,
            in_group,
            role,
            user,
            admin,
            authorization_owner,
        })
    }

    pub fn parse_drop(&mut self) -> Result<Statement, ParserError> {
        let object_type = if self.parse_keyword(Keyword::TABLE) {
            ObjectType::Table
        } else if self.parse_keyword(Keyword::VIEW) {
            ObjectType::View
        } else if self.parse_keyword(Keyword::INDEX) {
            ObjectType::Index
        } else if self.parse_keyword(Keyword::ROLE) {
            ObjectType::Role
        } else if self.parse_keyword(Keyword::SCHEMA) {
            ObjectType::Schema
        } else if self.parse_keyword(Keyword::SEQUENCE) {
            ObjectType::Sequence
        } else if self.parse_keyword(Keyword::FUNCTION) {
            return self.parse_drop_function();
        } else {
            return self.expected(
                "TABLE, VIEW, INDEX, ROLE, SCHEMA, FUNCTION or SEQUENCE after DROP",
                self.peek_token(),
            );
        };
        // Many dialects support the non standard `IF EXISTS` clause and allow
        // specifying multiple objects to delete in a single statement
        let if_exists = self.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
        let names = self.parse_comma_separated(Parser::parse_object_name)?;
        let cascade = self.parse_keyword(Keyword::CASCADE);
        let restrict = self.parse_keyword(Keyword::RESTRICT);
        let purge = self.parse_keyword(Keyword::PURGE);
        if cascade && restrict {
            return parser_err!("Cannot specify both CASCADE and RESTRICT in DROP");
        }
        if object_type == ObjectType::Role && (cascade || restrict || purge) {
            return parser_err!("Cannot specify CASCADE, RESTRICT, or PURGE in DROP ROLE");
        }
        Ok(Statement::Drop {
            object_type,
            if_exists,
            names,
            cascade,
            restrict,
            purge,
        })
    }

    /// ```sql
    /// DROP FUNCTION [ IF EXISTS ] name [ ( [ [ argmode ] [ argname ] argtype [, ...] ] ) ] [, ...]
    /// [ CASCADE | RESTRICT ]
    /// ```
    fn parse_drop_function(&mut self) -> Result<Statement, ParserError> {
        let if_exists = self.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
        let func_desc = self.parse_comma_separated(Parser::parse_drop_function_desc)?;
        let option = match self.parse_one_of_keywords(&[Keyword::CASCADE, Keyword::RESTRICT]) {
            Some(Keyword::CASCADE) => Some(ReferentialAction::Cascade),
            Some(Keyword::RESTRICT) => Some(ReferentialAction::Restrict),
            _ => None,
        };
        Ok(Statement::DropFunction {
            if_exists,
            func_desc,
            option,
        })
    }

    fn parse_drop_function_desc(&mut self) -> Result<DropFunctionDesc, ParserError> {
        let name = self.parse_object_name()?;

        let args = if self.consume_token(&Token::LParen) {
            if self.consume_token(&Token::RParen) {
                None
            } else {
                let args = self.parse_comma_separated(Parser::parse_function_arg)?;
                self.expect_token(&Token::RParen)?;
                Some(args)
            }
        } else {
            None
        };

        Ok(DropFunctionDesc { name, args })
    }

    /// ```sql
    /// DECLARE name [ BINARY ] [ ASENSITIVE | INSENSITIVE ] [ [ NO ] SCROLL ]
    ///     CURSOR [ { WITH | WITHOUT } HOLD ] FOR query
    /// ```
    pub fn parse_declare(&mut self) -> Result<Statement, ParserError> {
        let name = self.parse_identifier()?;

        let binary = self.parse_keyword(Keyword::BINARY);
        let sensitive = if self.parse_keyword(Keyword::INSENSITIVE) {
            Some(true)
        } else if self.parse_keyword(Keyword::ASENSITIVE) {
            Some(false)
        } else {
            None
        };
        let scroll = if self.parse_keyword(Keyword::SCROLL) {
            Some(true)
        } else if self.parse_keywords(&[Keyword::NO, Keyword::SCROLL]) {
            Some(false)
        } else {
            None
        };

        self.expect_keyword(Keyword::CURSOR)?;

        let hold = match self.parse_one_of_keywords(&[Keyword::WITH, Keyword::WITHOUT]) {
            Some(keyword) => {
                self.expect_keyword(Keyword::HOLD)?;

                match keyword {
                    Keyword::WITH => Some(true),
                    Keyword::WITHOUT => Some(false),
                    _ => unreachable!(),
                }
            }
            None => None,
        };

        self.expect_keyword(Keyword::FOR)?;

        let query = self.parse_query()?;

        Ok(Statement::Declare {
            name,
            binary,
            sensitive,
            scroll,
            hold,
            query: Box::new(query),
        })
    }

    // FETCH [ direction { FROM | IN } ] cursor INTO target;
    pub fn parse_fetch_statement(&mut self) -> Result<Statement, ParserError> {
        let direction = if self.parse_keyword(Keyword::NEXT) {
            FetchDirection::Next
        } else if self.parse_keyword(Keyword::PRIOR) {
            FetchDirection::Prior
        } else if self.parse_keyword(Keyword::FIRST) {
            FetchDirection::First
        } else if self.parse_keyword(Keyword::LAST) {
            FetchDirection::Last
        } else if self.parse_keyword(Keyword::ABSOLUTE) {
            FetchDirection::Absolute {
                limit: self.parse_number_value()?,
            }
        } else if self.parse_keyword(Keyword::RELATIVE) {
            FetchDirection::Relative {
                limit: self.parse_number_value()?,
            }
        } else if self.parse_keyword(Keyword::FORWARD) {
            if self.parse_keyword(Keyword::ALL) {
                FetchDirection::ForwardAll
            } else {
                FetchDirection::Forward {
                    // TODO: Support optional
                    limit: Some(self.parse_number_value()?),
                }
            }
        } else if self.parse_keyword(Keyword::BACKWARD) {
            if self.parse_keyword(Keyword::ALL) {
                FetchDirection::BackwardAll
            } else {
                FetchDirection::Backward {
                    // TODO: Support optional
                    limit: Some(self.parse_number_value()?),
                }
            }
        } else if self.parse_keyword(Keyword::ALL) {
            FetchDirection::All
        } else {
            FetchDirection::Count {
                limit: self.parse_number_value()?,
            }
        };

        self.expect_one_of_keywords(&[Keyword::FROM, Keyword::IN])?;

        let name = self.parse_identifier()?;

        let into = if self.parse_keyword(Keyword::INTO) {
            Some(self.parse_object_name()?)
        } else {
            None
        };

        Ok(Statement::Fetch {
            name,
            direction,
            into,
        })
    }

    pub fn parse_discard(&mut self) -> Result<Statement, ParserError> {
        let object_type = if self.parse_keyword(Keyword::ALL) {
            DiscardObject::ALL
        } else if self.parse_keyword(Keyword::PLANS) {
            DiscardObject::PLANS
        } else if self.parse_keyword(Keyword::SEQUENCES) {
            DiscardObject::SEQUENCES
        } else if self.parse_keyword(Keyword::TEMP) || self.parse_keyword(Keyword::TEMPORARY) {
            DiscardObject::TEMP
        } else {
            return self.expected(
                "ALL, PLANS, SEQUENCES, TEMP or TEMPORARY after DISCARD",
                self.peek_token(),
            );
        };
        Ok(Statement::Discard { object_type })
    }

    pub fn parse_create_index(&mut self, unique: bool) -> Result<Statement, ParserError> {
        let if_not_exists = self.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let index_name = self.parse_object_name()?;
        self.expect_keyword(Keyword::ON)?;
        let table_name = self.parse_object_name()?;
        let using = if self.parse_keyword(Keyword::USING) {
            Some(self.parse_identifier()?)
        } else {
            None
        };
        self.expect_token(&Token::LParen)?;
        let columns = self.parse_comma_separated(Parser::parse_order_by_expr)?;
        self.expect_token(&Token::RParen)?;
        Ok(Statement::CreateIndex {
            name: index_name,
            table_name,
            using,
            columns,
            unique,
            if_not_exists,
        })
    }

    //TODO: Implement parsing for Skewed and Clustered
    pub fn parse_hive_distribution(&mut self) -> Result<HiveDistributionStyle, ParserError> {
        if self.parse_keywords(&[Keyword::PARTITIONED, Keyword::BY]) {
            self.expect_token(&Token::LParen)?;
            let columns = self.parse_comma_separated(Parser::parse_column_def)?;
            self.expect_token(&Token::RParen)?;
            Ok(HiveDistributionStyle::PARTITIONED { columns })
        } else {
            Ok(HiveDistributionStyle::NONE)
        }
    }

    pub fn parse_hive_formats(&mut self) -> Result<HiveFormat, ParserError> {
        let mut hive_format = HiveFormat::default();
        loop {
            match self.parse_one_of_keywords(&[Keyword::ROW, Keyword::STORED, Keyword::LOCATION]) {
                Some(Keyword::ROW) => {
                    hive_format.row_format = Some(self.parse_row_format()?);
                }
                Some(Keyword::STORED) => {
                    self.expect_keyword(Keyword::AS)?;
                    if self.parse_keyword(Keyword::INPUTFORMAT) {
                        let input_format = self.parse_expr()?;
                        self.expect_keyword(Keyword::OUTPUTFORMAT)?;
                        let output_format = self.parse_expr()?;
                        hive_format.storage = Some(HiveIOFormat::IOF {
                            input_format,
                            output_format,
                        });
                    } else {
                        let format = self.parse_file_format()?;
                        hive_format.storage = Some(HiveIOFormat::FileFormat { format });
                    }
                }
                Some(Keyword::LOCATION) => {
                    hive_format.location = Some(self.parse_literal_string()?);
                }
                None => break,
                _ => break,
            }
        }

        Ok(hive_format)
    }

    pub fn parse_row_format(&mut self) -> Result<HiveRowFormat, ParserError> {
        self.expect_keyword(Keyword::FORMAT)?;
        match self.parse_one_of_keywords(&[Keyword::SERDE, Keyword::DELIMITED]) {
            Some(Keyword::SERDE) => {
                let class = self.parse_literal_string()?;
                Ok(HiveRowFormat::SERDE { class })
            }
            _ => Ok(HiveRowFormat::DELIMITED),
        }
    }

    pub fn parse_create_table(
        &mut self,
        or_replace: bool,
        temporary: bool,
        global: Option<bool>,
    ) -> Result<Statement, ParserError> {
        let if_not_exists = self.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let table_name = self.parse_object_name()?;

        // Clickhouse has `ON CLUSTER 'cluster'` syntax for DDLs
        let on_cluster = if self.parse_keywords(&[Keyword::ON, Keyword::CLUSTER]) {
            let next_token = self.next_token();
            match next_token.token {
                Token::SingleQuotedString(s) => Some(s),
                Token::Word(s) => Some(s.to_string()),
                _ => self.expected("identifier or cluster literal", next_token)?,
            }
        } else {
            None
        };

        let like = if self.parse_keyword(Keyword::LIKE) || self.parse_keyword(Keyword::ILIKE) {
            self.parse_object_name().ok()
        } else {
            None
        };

        let clone = if self.parse_keyword(Keyword::CLONE) {
            self.parse_object_name().ok()
        } else {
            None
        };

        // parse optional column list (schema)
        let (columns, constraints) = self.parse_columns()?;

        // SQLite supports `WITHOUT ROWID` at the end of `CREATE TABLE`
        let without_rowid = self.parse_keywords(&[Keyword::WITHOUT, Keyword::ROWID]);

        let hive_distribution = self.parse_hive_distribution()?;
        let hive_formats = self.parse_hive_formats()?;
        // PostgreSQL supports `WITH ( options )`, before `AS`
        let with_options = self.parse_options(Keyword::WITH)?;
        let table_properties = self.parse_options(Keyword::TBLPROPERTIES)?;
        // Parse optional `AS ( query )`
        let query = if self.parse_keyword(Keyword::AS) {
            Some(Box::new(self.parse_query()?))
        } else {
            None
        };

        let engine = if self.parse_keyword(Keyword::ENGINE) {
            self.expect_token(&Token::Eq)?;
            let next_token = self.next_token();
            match next_token.token {
                Token::Word(w) => Some(w.value),
                _ => self.expected("identifier", next_token)?,
            }
        } else {
            None
        };

        let default_charset = if self.parse_keywords(&[Keyword::DEFAULT, Keyword::CHARSET]) {
            self.expect_token(&Token::Eq)?;
            let next_token = self.next_token();
            match next_token.token {
                Token::Word(w) => Some(w.value),
                _ => self.expected("identifier", next_token)?,
            }
        } else {
            None
        };

        let collation = if self.parse_keywords(&[Keyword::COLLATE]) {
            self.expect_token(&Token::Eq)?;
            let next_token = self.next_token();
            match next_token.token {
                Token::Word(w) => Some(w.value),
                _ => self.expected("identifier", next_token)?,
            }
        } else {
            None
        };

        let on_commit: Option<OnCommit> =
            if self.parse_keywords(&[Keyword::ON, Keyword::COMMIT, Keyword::DELETE, Keyword::ROWS])
            {
                Some(OnCommit::DeleteRows)
            } else if self.parse_keywords(&[
                Keyword::ON,
                Keyword::COMMIT,
                Keyword::PRESERVE,
                Keyword::ROWS,
            ]) {
                Some(OnCommit::PreserveRows)
            } else if self.parse_keywords(&[Keyword::ON, Keyword::COMMIT, Keyword::DROP]) {
                Some(OnCommit::Drop)
            } else {
                None
            };

        Ok(CreateTableBuilder::new(table_name)
            .temporary(temporary)
            .columns(columns)
            .constraints(constraints)
            .with_options(with_options)
            .table_properties(table_properties)
            .or_replace(or_replace)
            .if_not_exists(if_not_exists)
            .hive_distribution(hive_distribution)
            .hive_formats(Some(hive_formats))
            .global(global)
            .query(query)
            .without_rowid(without_rowid)
            .like(like)
            .clone_clause(clone)
            .engine(engine)
            .default_charset(default_charset)
            .collation(collation)
            .on_commit(on_commit)
            .on_cluster(on_cluster)
            .build())
    }

    pub fn parse_columns(&mut self) -> Result<(Vec<ColumnDef>, Vec<TableConstraint>), ParserError> {
        let mut columns = vec![];
        let mut constraints = vec![];
        if !self.consume_token(&Token::LParen) || self.consume_token(&Token::RParen) {
            return Ok((columns, constraints));
        }

        loop {
            if let Some(constraint) = self.parse_optional_table_constraint()? {
                constraints.push(constraint);
            } else if let Token::Word(_) = self.peek_token().token {
                columns.push(self.parse_column_def()?);
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

    pub fn parse_column_def(&mut self) -> Result<ColumnDef, ParserError> {
        let name = self.parse_identifier()?;
        let data_type = self.parse_data_type()?;
        let collation = if self.parse_keyword(Keyword::COLLATE) {
            Some(self.parse_object_name()?)
        } else {
            None
        };
        let mut options = vec![];
        loop {
            if self.parse_keyword(Keyword::CONSTRAINT) {
                let name = Some(self.parse_identifier()?);
                if let Some(option) = self.parse_optional_column_option()? {
                    options.push(ColumnOptionDef { name, option });
                } else {
                    return self.expected(
                        "constraint details after CONSTRAINT <name>",
                        self.peek_token(),
                    );
                }
            } else if let Some(option) = self.parse_optional_column_option()? {
                options.push(ColumnOptionDef { name: None, option });
            } else {
                break;
            };
        }
        Ok(ColumnDef {
            name,
            data_type,
            collation,
            options,
        })
    }

    pub fn parse_optional_column_option(&mut self) -> Result<Option<ColumnOption>, ParserError> {
        if self.parse_keywords(&[Keyword::CHARACTER, Keyword::SET]) {
            Ok(Some(ColumnOption::CharacterSet(self.parse_object_name()?)))
        } else if self.parse_keywords(&[Keyword::NOT, Keyword::NULL]) {
            Ok(Some(ColumnOption::NotNull))
        } else if self.parse_keywords(&[Keyword::COMMENT]) {
            let next_token = self.next_token();
            match next_token.token {
                Token::SingleQuotedString(value, ..) => Ok(Some(ColumnOption::Comment(value))),
                _ => self.expected("string", next_token),
            }
        } else if self.parse_keyword(Keyword::NULL) {
            Ok(Some(ColumnOption::Null))
        } else if self.parse_keyword(Keyword::DEFAULT) {
            Ok(Some(ColumnOption::Default(self.parse_expr()?)))
        } else if self.parse_keywords(&[Keyword::PRIMARY, Keyword::KEY]) {
            Ok(Some(ColumnOption::Unique { is_primary: true }))
        } else if self.parse_keyword(Keyword::UNIQUE) {
            Ok(Some(ColumnOption::Unique { is_primary: false }))
        } else if self.parse_keyword(Keyword::REFERENCES) {
            let foreign_table = self.parse_object_name()?;
            // PostgreSQL allows omitting the column list and
            // uses the primary key column of the foreign table by default
            let referred_columns = self.parse_parenthesized_column_list(Optional)?;
            let mut on_delete = None;
            let mut on_update = None;
            loop {
                if on_delete.is_none() && self.parse_keywords(&[Keyword::ON, Keyword::DELETE]) {
                    on_delete = Some(self.parse_referential_action()?);
                } else if on_update.is_none()
                    && self.parse_keywords(&[Keyword::ON, Keyword::UPDATE])
                {
                    on_update = Some(self.parse_referential_action()?);
                } else {
                    break;
                }
            }
            Ok(Some(ColumnOption::ForeignKey {
                foreign_table,
                referred_columns,
                on_delete,
                on_update,
            }))
        } else if self.parse_keyword(Keyword::CHECK) {
            self.expect_token(&Token::LParen)?;
            let expr = self.parse_expr()?;
            self.expect_token(&Token::RParen)?;
            Ok(Some(ColumnOption::Check(expr)))
        } else if self.parse_keyword(Keyword::AUTO_INCREMENT)
            && dialect_of!(self is MySqlDialect |  GenericDialect)
        {
            // Support AUTO_INCREMENT for MySQL
            Ok(Some(ColumnOption::DialectSpecific(vec![
                Token::make_keyword("AUTO_INCREMENT"),
            ])))
        } else if self.parse_keyword(Keyword::AUTOINCREMENT)
            && dialect_of!(self is SQLiteDialect |  GenericDialect)
        {
            // Support AUTOINCREMENT for SQLite
            Ok(Some(ColumnOption::DialectSpecific(vec![
                Token::make_keyword("AUTOINCREMENT"),
            ])))
        } else if self.parse_keywords(&[Keyword::ON, Keyword::UPDATE])
            && dialect_of!(self is MySqlDialect | GenericDialect)
        {
            let expr = self.parse_expr()?;
            Ok(Some(ColumnOption::OnUpdate(expr)))
        } else {
            Ok(None)
        }
    }

    pub fn parse_referential_action(&mut self) -> Result<ReferentialAction, ParserError> {
        if self.parse_keyword(Keyword::RESTRICT) {
            Ok(ReferentialAction::Restrict)
        } else if self.parse_keyword(Keyword::CASCADE) {
            Ok(ReferentialAction::Cascade)
        } else if self.parse_keywords(&[Keyword::SET, Keyword::NULL]) {
            Ok(ReferentialAction::SetNull)
        } else if self.parse_keywords(&[Keyword::NO, Keyword::ACTION]) {
            Ok(ReferentialAction::NoAction)
        } else if self.parse_keywords(&[Keyword::SET, Keyword::DEFAULT]) {
            Ok(ReferentialAction::SetDefault)
        } else {
            self.expected(
                "one of RESTRICT, CASCADE, SET NULL, NO ACTION or SET DEFAULT",
                self.peek_token(),
            )
        }
    }

    pub fn parse_optional_table_constraint(
        &mut self,
    ) -> Result<Option<TableConstraint>, ParserError> {
        let name = if self.parse_keyword(Keyword::CONSTRAINT) {
            Some(self.parse_identifier()?)
        } else {
            None
        };

        let next_token = self.next_token();
        match next_token.token {
            Token::Word(w) if w.keyword == Keyword::PRIMARY || w.keyword == Keyword::UNIQUE => {
                let is_primary = w.keyword == Keyword::PRIMARY;
                if is_primary {
                    self.expect_keyword(Keyword::KEY)?;
                }
                let columns = self.parse_parenthesized_column_list(Mandatory)?;
                Ok(Some(TableConstraint::Unique {
                    name,
                    columns,
                    is_primary,
                }))
            }
            Token::Word(w) if w.keyword == Keyword::FOREIGN => {
                self.expect_keyword(Keyword::KEY)?;
                let columns = self.parse_parenthesized_column_list(Mandatory)?;
                self.expect_keyword(Keyword::REFERENCES)?;
                let foreign_table = self.parse_object_name()?;
                let referred_columns = self.parse_parenthesized_column_list(Mandatory)?;
                let mut on_delete = None;
                let mut on_update = None;
                loop {
                    if on_delete.is_none() && self.parse_keywords(&[Keyword::ON, Keyword::DELETE]) {
                        on_delete = Some(self.parse_referential_action()?);
                    } else if on_update.is_none()
                        && self.parse_keywords(&[Keyword::ON, Keyword::UPDATE])
                    {
                        on_update = Some(self.parse_referential_action()?);
                    } else {
                        break;
                    }
                }
                Ok(Some(TableConstraint::ForeignKey {
                    name,
                    columns,
                    foreign_table,
                    referred_columns,
                    on_delete,
                    on_update,
                }))
            }
            Token::Word(w) if w.keyword == Keyword::CHECK => {
                self.expect_token(&Token::LParen)?;
                let expr = Box::new(self.parse_expr()?);
                self.expect_token(&Token::RParen)?;
                Ok(Some(TableConstraint::Check { name, expr }))
            }
            Token::Word(w)
                if (w.keyword == Keyword::INDEX || w.keyword == Keyword::KEY)
                    && dialect_of!(self is GenericDialect | MySqlDialect) =>
            {
                let display_as_key = w.keyword == Keyword::KEY;

                let name = match self.peek_token().token {
                    Token::Word(word) if word.keyword == Keyword::USING => None,
                    _ => self.maybe_parse(|parser| parser.parse_identifier()),
                };

                let index_type = if self.parse_keyword(Keyword::USING) {
                    Some(self.parse_index_type()?)
                } else {
                    None
                };
                let columns = self.parse_parenthesized_column_list(Mandatory)?;

                Ok(Some(TableConstraint::Index {
                    display_as_key,
                    name,
                    index_type,
                    columns,
                }))
            }
            Token::Word(w)
                if (w.keyword == Keyword::FULLTEXT || w.keyword == Keyword::SPATIAL)
                    && dialect_of!(self is GenericDialect | MySqlDialect) =>
            {
                if let Some(name) = name {
                    return self.expected(
                        "FULLTEXT or SPATIAL option without constraint name",
                        TokenWithLocation {
                            token: Token::make_keyword(&name.to_string()),
                            location: next_token.location,
                        },
                    );
                }

                let fulltext = w.keyword == Keyword::FULLTEXT;

                let index_type_display = if self.parse_keyword(Keyword::KEY) {
                    KeyOrIndexDisplay::Key
                } else if self.parse_keyword(Keyword::INDEX) {
                    KeyOrIndexDisplay::Index
                } else {
                    KeyOrIndexDisplay::None
                };

                let opt_index_name = self.maybe_parse(|parser| parser.parse_identifier());

                let columns = self.parse_parenthesized_column_list(Mandatory)?;

                Ok(Some(TableConstraint::FulltextOrSpatial {
                    fulltext,
                    index_type_display,
                    opt_index_name,
                    columns,
                }))
            }
            _ => {
                if name.is_some() {
                    self.expected("PRIMARY, UNIQUE, FOREIGN, or CHECK", next_token)
                } else {
                    self.prev_token();
                    Ok(None)
                }
            }
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

    pub fn parse_index_type(&mut self) -> Result<IndexType, ParserError> {
        if self.parse_keyword(Keyword::BTREE) {
            Ok(IndexType::BTree)
        } else if self.parse_keyword(Keyword::HASH) {
            Ok(IndexType::Hash)
        } else {
            self.expected("index type {BTREE | HASH}", self.peek_token())
        }
    }

    pub fn parse_sql_option(&mut self) -> Result<SqlOption, ParserError> {
        let name = self.parse_identifier()?;
        self.expect_token(&Token::Eq)?;
        let value = self.parse_value()?;
        Ok(SqlOption { name, value })
    }

    pub fn parse_alter(&mut self) -> Result<Statement, ParserError> {
        let object_type = self.expect_one_of_keywords(&[Keyword::TABLE, Keyword::INDEX])?;
        match object_type {
            Keyword::TABLE => {
                let _ = self.parse_keyword(Keyword::ONLY); // [ ONLY ]
                let table_name = self.parse_object_name()?;
                let operation = if self.parse_keyword(Keyword::ADD) {
                    if let Some(constraint) = self.parse_optional_table_constraint()? {
                        AlterTableOperation::AddConstraint(constraint)
                    } else {
                        let if_not_exists =
                            self.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
                        if self.parse_keyword(Keyword::PARTITION) {
                            self.expect_token(&Token::LParen)?;
                            let partitions = self.parse_comma_separated(Parser::parse_expr)?;
                            self.expect_token(&Token::RParen)?;
                            AlterTableOperation::AddPartitions {
                                if_not_exists,
                                new_partitions: partitions,
                            }
                        } else {
                            let column_keyword = self.parse_keyword(Keyword::COLUMN);

                            let if_not_exists = if dialect_of!(self is PostgreSqlDialect | BigQueryDialect | GenericDialect)
                            {
                                self.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS])
                                    || if_not_exists
                            } else {
                                false
                            };

                            let column_def = self.parse_column_def()?;
                            AlterTableOperation::AddColumn {
                                column_keyword,
                                if_not_exists,
                                column_def,
                            }
                        }
                    }
                } else if self.parse_keyword(Keyword::RENAME) {
                    if dialect_of!(self is PostgreSqlDialect)
                        && self.parse_keyword(Keyword::CONSTRAINT)
                    {
                        let old_name = self.parse_identifier()?;
                        self.expect_keyword(Keyword::TO)?;
                        let new_name = self.parse_identifier()?;
                        AlterTableOperation::RenameConstraint { old_name, new_name }
                    } else if self.parse_keyword(Keyword::TO) {
                        let table_name = self.parse_object_name()?;
                        AlterTableOperation::RenameTable { table_name }
                    } else {
                        let _ = self.parse_keyword(Keyword::COLUMN); // [ COLUMN ]
                        let old_column_name = self.parse_identifier()?;
                        self.expect_keyword(Keyword::TO)?;
                        let new_column_name = self.parse_identifier()?;
                        AlterTableOperation::RenameColumn {
                            old_column_name,
                            new_column_name,
                        }
                    }
                } else if self.parse_keyword(Keyword::DROP) {
                    if self.parse_keywords(&[Keyword::IF, Keyword::EXISTS, Keyword::PARTITION]) {
                        self.expect_token(&Token::LParen)?;
                        let partitions = self.parse_comma_separated(Parser::parse_expr)?;
                        self.expect_token(&Token::RParen)?;
                        AlterTableOperation::DropPartitions {
                            partitions,
                            if_exists: true,
                        }
                    } else if self.parse_keyword(Keyword::PARTITION) {
                        self.expect_token(&Token::LParen)?;
                        let partitions = self.parse_comma_separated(Parser::parse_expr)?;
                        self.expect_token(&Token::RParen)?;
                        AlterTableOperation::DropPartitions {
                            partitions,
                            if_exists: false,
                        }
                    } else if self.parse_keyword(Keyword::CONSTRAINT) {
                        let if_exists = self.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
                        let name = self.parse_identifier()?;
                        let cascade = self.parse_keyword(Keyword::CASCADE);
                        AlterTableOperation::DropConstraint {
                            if_exists,
                            name,
                            cascade,
                        }
                    } else if self.parse_keywords(&[Keyword::PRIMARY, Keyword::KEY])
                        && dialect_of!(self is MySqlDialect | GenericDialect)
                    {
                        AlterTableOperation::DropPrimaryKey
                    } else {
                        let _ = self.parse_keyword(Keyword::COLUMN); // [ COLUMN ]
                        let if_exists = self.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
                        let column_name = self.parse_identifier()?;
                        let cascade = self.parse_keyword(Keyword::CASCADE);
                        AlterTableOperation::DropColumn {
                            column_name,
                            if_exists,
                            cascade,
                        }
                    }
                } else if self.parse_keyword(Keyword::PARTITION) {
                    self.expect_token(&Token::LParen)?;
                    let before = self.parse_comma_separated(Parser::parse_expr)?;
                    self.expect_token(&Token::RParen)?;
                    self.expect_keyword(Keyword::RENAME)?;
                    self.expect_keywords(&[Keyword::TO, Keyword::PARTITION])?;
                    self.expect_token(&Token::LParen)?;
                    let renames = self.parse_comma_separated(Parser::parse_expr)?;
                    self.expect_token(&Token::RParen)?;
                    AlterTableOperation::RenamePartitions {
                        old_partitions: before,
                        new_partitions: renames,
                    }
                } else if self.parse_keyword(Keyword::CHANGE) {
                    let _ = self.parse_keyword(Keyword::COLUMN); // [ COLUMN ]
                    let old_name = self.parse_identifier()?;
                    let new_name = self.parse_identifier()?;
                    let data_type = self.parse_data_type()?;
                    let mut options = vec![];
                    while let Some(option) = self.parse_optional_column_option()? {
                        options.push(option);
                    }

                    AlterTableOperation::ChangeColumn {
                        old_name,
                        new_name,
                        data_type,
                        options,
                    }
                } else if self.parse_keyword(Keyword::ALTER) {
                    let _ = self.parse_keyword(Keyword::COLUMN); // [ COLUMN ]
                    let column_name = self.parse_identifier()?;
                    let is_postgresql = dialect_of!(self is PostgreSqlDialect);

                    let op = if self.parse_keywords(&[Keyword::SET, Keyword::NOT, Keyword::NULL]) {
                        AlterColumnOperation::SetNotNull {}
                    } else if self.parse_keywords(&[Keyword::DROP, Keyword::NOT, Keyword::NULL]) {
                        AlterColumnOperation::DropNotNull {}
                    } else if self.parse_keywords(&[Keyword::SET, Keyword::DEFAULT]) {
                        AlterColumnOperation::SetDefault {
                            value: self.parse_expr()?,
                        }
                    } else if self.parse_keywords(&[Keyword::DROP, Keyword::DEFAULT]) {
                        AlterColumnOperation::DropDefault {}
                    } else if self.parse_keywords(&[Keyword::SET, Keyword::DATA, Keyword::TYPE])
                        || (is_postgresql && self.parse_keyword(Keyword::TYPE))
                    {
                        let data_type = self.parse_data_type()?;
                        let using = if is_postgresql && self.parse_keyword(Keyword::USING) {
                            Some(self.parse_expr()?)
                        } else {
                            None
                        };
                        AlterColumnOperation::SetDataType { data_type, using }
                    } else {
                        return self.expected(
                            "SET/DROP NOT NULL, SET DEFAULT, SET DATA TYPE after ALTER COLUMN",
                            self.peek_token(),
                        );
                    };
                    AlterTableOperation::AlterColumn { column_name, op }
                } else {
                    return self.expected(
                        "ADD, RENAME, PARTITION or DROP after ALTER TABLE",
                        self.peek_token(),
                    );
                };
                Ok(Statement::AlterTable {
                    name: table_name,
                    operation,
                })
            }
            Keyword::INDEX => {
                let index_name = self.parse_object_name()?;
                let operation = if self.parse_keyword(Keyword::RENAME) {
                    if self.parse_keyword(Keyword::TO) {
                        let index_name = self.parse_object_name()?;
                        AlterIndexOperation::RenameIndex { index_name }
                    } else {
                        return self.expected("TO after RENAME", self.peek_token());
                    }
                } else {
                    return self.expected("RENAME after ALTER INDEX", self.peek_token());
                };

                Ok(Statement::AlterIndex {
                    name: index_name,
                    operation,
                })
            }
            // unreachable because expect_one_of_keywords used above
            _ => unreachable!(),
        }
    }

    /// Parse a copy statement
    pub fn parse_copy(&mut self) -> Result<Statement, ParserError> {
        let table_name = self.parse_object_name()?;
        let columns = self.parse_parenthesized_column_list(Optional)?;
        let to = match self.parse_one_of_keywords(&[Keyword::FROM, Keyword::TO]) {
            Some(Keyword::FROM) => false,
            Some(Keyword::TO) => true,
            _ => self.expected("FROM or TO", self.peek_token())?,
        };
        let target = if self.parse_keyword(Keyword::STDIN) {
            CopyTarget::Stdin
        } else if self.parse_keyword(Keyword::STDOUT) {
            CopyTarget::Stdout
        } else if self.parse_keyword(Keyword::PROGRAM) {
            CopyTarget::Program {
                command: self.parse_literal_string()?,
            }
        } else {
            CopyTarget::File {
                filename: self.parse_literal_string()?,
            }
        };
        let _ = self.parse_keyword(Keyword::WITH); // [ WITH ]
        let mut options = vec![];
        if self.consume_token(&Token::LParen) {
            options = self.parse_comma_separated(Parser::parse_copy_option)?;
            self.expect_token(&Token::RParen)?;
        }
        let mut legacy_options = vec![];
        while let Some(opt) = self.maybe_parse(|parser| parser.parse_copy_legacy_option()) {
            legacy_options.push(opt);
        }
        let values = if let CopyTarget::Stdin = target {
            self.expect_token(&Token::SemiColon)?;
            self.parse_tsv()
        } else {
            vec![]
        };
        Ok(Statement::Copy {
            table_name,
            columns,
            to,
            target,
            options,
            legacy_options,
            values,
        })
    }

    pub fn parse_close(&mut self) -> Result<Statement, ParserError> {
        let cursor = if self.parse_keyword(Keyword::ALL) {
            CloseCursor::All
        } else {
            let name = self.parse_identifier()?;

            CloseCursor::Specific { name }
        };

        Ok(Statement::Close { cursor })
    }

    fn parse_copy_option(&mut self) -> Result<CopyOption, ParserError> {
        let ret = match self.parse_one_of_keywords(&[
            Keyword::FORMAT,
            Keyword::FREEZE,
            Keyword::DELIMITER,
            Keyword::NULL,
            Keyword::HEADER,
            Keyword::QUOTE,
            Keyword::ESCAPE,
            Keyword::FORCE_QUOTE,
            Keyword::FORCE_NOT_NULL,
            Keyword::FORCE_NULL,
            Keyword::ENCODING,
        ]) {
            Some(Keyword::FORMAT) => CopyOption::Format(self.parse_identifier()?),
            Some(Keyword::FREEZE) => CopyOption::Freeze(!matches!(
                self.parse_one_of_keywords(&[Keyword::TRUE, Keyword::FALSE]),
                Some(Keyword::FALSE)
            )),
            Some(Keyword::DELIMITER) => CopyOption::Delimiter(self.parse_literal_char()?),
            Some(Keyword::NULL) => CopyOption::Null(self.parse_literal_string()?),
            Some(Keyword::HEADER) => CopyOption::Header(!matches!(
                self.parse_one_of_keywords(&[Keyword::TRUE, Keyword::FALSE]),
                Some(Keyword::FALSE)
            )),
            Some(Keyword::QUOTE) => CopyOption::Quote(self.parse_literal_char()?),
            Some(Keyword::ESCAPE) => CopyOption::Escape(self.parse_literal_char()?),
            Some(Keyword::FORCE_QUOTE) => {
                CopyOption::ForceQuote(self.parse_parenthesized_column_list(Mandatory)?)
            }
            Some(Keyword::FORCE_NOT_NULL) => {
                CopyOption::ForceNotNull(self.parse_parenthesized_column_list(Mandatory)?)
            }
            Some(Keyword::FORCE_NULL) => {
                CopyOption::ForceNull(self.parse_parenthesized_column_list(Mandatory)?)
            }
            Some(Keyword::ENCODING) => CopyOption::Encoding(self.parse_literal_string()?),
            _ => self.expected("option", self.peek_token())?,
        };
        Ok(ret)
    }

    fn parse_copy_legacy_option(&mut self) -> Result<CopyLegacyOption, ParserError> {
        let ret = match self.parse_one_of_keywords(&[
            Keyword::BINARY,
            Keyword::DELIMITER,
            Keyword::NULL,
            Keyword::CSV,
        ]) {
            Some(Keyword::BINARY) => CopyLegacyOption::Binary,
            Some(Keyword::DELIMITER) => {
                let _ = self.parse_keyword(Keyword::AS); // [ AS ]
                CopyLegacyOption::Delimiter(self.parse_literal_char()?)
            }
            Some(Keyword::NULL) => {
                let _ = self.parse_keyword(Keyword::AS); // [ AS ]
                CopyLegacyOption::Null(self.parse_literal_string()?)
            }
            Some(Keyword::CSV) => CopyLegacyOption::Csv({
                let mut opts = vec![];
                while let Some(opt) =
                    self.maybe_parse(|parser| parser.parse_copy_legacy_csv_option())
                {
                    opts.push(opt);
                }
                opts
            }),
            _ => self.expected("option", self.peek_token())?,
        };
        Ok(ret)
    }

    fn parse_copy_legacy_csv_option(&mut self) -> Result<CopyLegacyCsvOption, ParserError> {
        let ret = match self.parse_one_of_keywords(&[
            Keyword::HEADER,
            Keyword::QUOTE,
            Keyword::ESCAPE,
            Keyword::FORCE,
        ]) {
            Some(Keyword::HEADER) => CopyLegacyCsvOption::Header,
            Some(Keyword::QUOTE) => {
                let _ = self.parse_keyword(Keyword::AS); // [ AS ]
                CopyLegacyCsvOption::Quote(self.parse_literal_char()?)
            }
            Some(Keyword::ESCAPE) => {
                let _ = self.parse_keyword(Keyword::AS); // [ AS ]
                CopyLegacyCsvOption::Escape(self.parse_literal_char()?)
            }
            Some(Keyword::FORCE) if self.parse_keywords(&[Keyword::NOT, Keyword::NULL]) => {
                CopyLegacyCsvOption::ForceNotNull(
                    self.parse_comma_separated(Parser::parse_identifier)?,
                )
            }
            Some(Keyword::FORCE) if self.parse_keywords(&[Keyword::QUOTE]) => {
                CopyLegacyCsvOption::ForceQuote(
                    self.parse_comma_separated(Parser::parse_identifier)?,
                )
            }
            _ => self.expected("csv option", self.peek_token())?,
        };
        Ok(ret)
    }

    fn parse_literal_char(&mut self) -> Result<char, ParserError> {
        let s = self.parse_literal_string()?;
        if s.len() != 1 {
            return parser_err!(format!("Expect a char, found {:?}", s));
        }
        Ok(s.chars().next().unwrap())
    }

    /// Parse a tab separated values in
    /// COPY payload
    pub fn parse_tsv(&mut self) -> Vec<Option<String>> {
        self.parse_tab_value()
    }

    pub fn parse_tab_value(&mut self) -> Vec<Option<String>> {
        let mut values = vec![];
        let mut content = String::from("");
        while let Some(t) = self.next_token_no_skip().map(|t| &t.token) {
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
                    if let Token::Word(w) = self.next_token().token {
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
        let next_token = self.next_token();
        let location = next_token.location;
        match next_token.token {
            Token::Word(w) => match w.keyword {
                Keyword::TRUE => Ok(Value::Boolean(true)),
                Keyword::FALSE => Ok(Value::Boolean(false)),
                Keyword::NULL => Ok(Value::Null),
                Keyword::NoKeyword if w.quote_style.is_some() => match w.quote_style {
                    Some('"') => Ok(Value::DoubleQuotedString(w.value)),
                    Some('\'') => Ok(Value::SingleQuotedString(w.value)),
                    _ => self.expected(
                        "A value?",
                        TokenWithLocation {
                            token: Token::Word(w),
                            location,
                        },
                    )?,
                },
                // Case when Snowflake Semi-structured data like key:value
                Keyword::NoKeyword | Keyword::LOCATION | Keyword::TYPE if dialect_of!(self is SnowflakeDialect | GenericDialect) => {
                    Ok(Value::UnQuotedString(w.value))
                }
                _ => self.expected(
                    "a concrete value",
                    TokenWithLocation {
                        token: Token::Word(w),
                        location,
                    },
                ),
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
            Token::DollarQuotedString(ref s) => Ok(Value::DollarQuotedString(s.clone())),
            Token::NationalStringLiteral(ref s) => Ok(Value::NationalStringLiteral(s.to_string())),
            Token::EscapedStringLiteral(ref s) => Ok(Value::EscapedStringLiteral(s.to_string())),
            Token::HexStringLiteral(ref s) => Ok(Value::HexStringLiteral(s.to_string())),
            Token::Placeholder(ref s) => Ok(Value::Placeholder(s.to_string())),
            tok @ Token::Colon | tok @ Token::AtSign => {
                let ident = self.parse_identifier()?;
                let placeholder = tok.to_string() + &ident.value;
                Ok(Value::Placeholder(placeholder))
            }
            unexpected => self.expected(
                "a value",
                TokenWithLocation {
                    token: unexpected,
                    location,
                },
            ),
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
        let next_token = self.next_token();
        match next_token.token {
            Token::Number(s, _) => s.parse::<u64>().map_err(|e| {
                ParserError::ParserError(format!("Could not parse '{}' as u64: {}", s, e))
            }),
            _ => self.expected("literal int", next_token),
        }
    }

    pub fn parse_function_definition(&mut self) -> Result<FunctionDefinition, ParserError> {
        let peek_token = self.peek_token();
        match peek_token.token {
            Token::DollarQuotedString(value) if dialect_of!(self is PostgreSqlDialect) => {
                self.next_token();
                Ok(FunctionDefinition::DoubleDollarDef(value.value))
            }
            _ => Ok(FunctionDefinition::SingleQuotedDef(
                self.parse_literal_string()?,
            )),
        }
    }
    /// Parse a literal string
    pub fn parse_literal_string(&mut self) -> Result<String, ParserError> {
        let next_token = self.next_token();
        match next_token.token {
            Token::Word(Word { value, keyword, .. }) if keyword == Keyword::NoKeyword => Ok(value),
            Token::SingleQuotedString(s) => Ok(s),
            Token::DoubleQuotedString(s) => Ok(s),
            Token::EscapedStringLiteral(s) if dialect_of!(self is PostgreSqlDialect | GenericDialect) => {
                Ok(s)
            }
            _ => self.expected("literal string", next_token),
        }
    }

    /// Parse a map key string
    pub fn parse_map_key(&mut self) -> Result<Expr, ParserError> {
        let next_token = self.next_token();
        match next_token.token {
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
            _ => self.expected("literal string, number or function", next_token),
        }
    }

    /// Parse a SQL datatype (in the context of a CREATE TABLE statement for example)
    pub fn parse_data_type(&mut self) -> Result<DataType, ParserError> {
        let next_token = self.next_token();
        let mut data = match next_token.token {
            Token::Word(w) => match w.keyword {
                Keyword::BOOLEAN => Ok(DataType::Boolean),
                Keyword::FLOAT => Ok(DataType::Float(self.parse_optional_precision()?)),
                Keyword::REAL => Ok(DataType::Real),
                Keyword::DOUBLE => {
                    if self.parse_keyword(Keyword::PRECISION) {
                        Ok(DataType::DoublePrecision)
                    } else {
                        Ok(DataType::Double)
                    }
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
                Keyword::MEDIUMINT => {
                    let optional_precision = self.parse_optional_precision();
                    if self.parse_keyword(Keyword::UNSIGNED) {
                        Ok(DataType::UnsignedMediumInt(optional_precision?))
                    } else {
                        Ok(DataType::MediumInt(optional_precision?))
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
                Keyword::VARCHAR => Ok(DataType::Varchar(self.parse_optional_character_length()?)),
                Keyword::NVARCHAR => Ok(DataType::Nvarchar(self.parse_optional_precision()?)),
                Keyword::CHARACTER => {
                    if self.parse_keyword(Keyword::VARYING) {
                        Ok(DataType::CharacterVarying(
                            self.parse_optional_character_length()?,
                        ))
                    } else if self.parse_keywords(&[Keyword::LARGE, Keyword::OBJECT]) {
                        Ok(DataType::CharacterLargeObject(
                            self.parse_optional_precision()?,
                        ))
                    } else {
                        Ok(DataType::Character(self.parse_optional_character_length()?))
                    }
                }
                Keyword::CHAR => {
                    if self.parse_keyword(Keyword::VARYING) {
                        Ok(DataType::CharVarying(
                            self.parse_optional_character_length()?,
                        ))
                    } else if self.parse_keywords(&[Keyword::LARGE, Keyword::OBJECT]) {
                        Ok(DataType::CharLargeObject(self.parse_optional_precision()?))
                    } else {
                        Ok(DataType::Char(self.parse_optional_character_length()?))
                    }
                }
                Keyword::CLOB => Ok(DataType::Clob(self.parse_optional_precision()?)),
                Keyword::BINARY => Ok(DataType::Binary(self.parse_optional_precision()?)),
                Keyword::VARBINARY => Ok(DataType::Varbinary(self.parse_optional_precision()?)),
                Keyword::BLOB => Ok(DataType::Blob(self.parse_optional_precision()?)),
                Keyword::UUID => Ok(DataType::Uuid),
                Keyword::DATE => Ok(DataType::Date),
                Keyword::DATETIME => Ok(DataType::Datetime(self.parse_optional_precision()?)),
                Keyword::TIMESTAMP => {
                    let precision = self.parse_optional_precision()?;
                    let tz = if self.parse_keyword(Keyword::WITH) {
                        self.expect_keywords(&[Keyword::TIME, Keyword::ZONE])?;
                        TimezoneInfo::WithTimeZone
                    } else if self.parse_keyword(Keyword::WITHOUT) {
                        self.expect_keywords(&[Keyword::TIME, Keyword::ZONE])?;
                        TimezoneInfo::WithoutTimeZone
                    } else {
                        TimezoneInfo::None
                    };
                    Ok(DataType::Timestamp(precision, tz))
                }
                Keyword::TIMESTAMPTZ => Ok(DataType::Timestamp(
                    self.parse_optional_precision()?,
                    TimezoneInfo::Tz,
                )),
                Keyword::TIME => {
                    let precision = self.parse_optional_precision()?;
                    let tz = if self.parse_keyword(Keyword::WITH) {
                        self.expect_keywords(&[Keyword::TIME, Keyword::ZONE])?;
                        TimezoneInfo::WithTimeZone
                    } else if self.parse_keyword(Keyword::WITHOUT) {
                        self.expect_keywords(&[Keyword::TIME, Keyword::ZONE])?;
                        TimezoneInfo::WithoutTimeZone
                    } else {
                        TimezoneInfo::None
                    };
                    Ok(DataType::Time(precision, tz))
                }
                Keyword::TIMETZ => Ok(DataType::Time(
                    self.parse_optional_precision()?,
                    TimezoneInfo::Tz,
                )),
                // Interval types can be followed by a complicated interval
                // qualifier that we don't currently support. See
                // parse_interval for a taste.
                Keyword::INTERVAL => Ok(DataType::Interval),
                Keyword::REGCLASS => Ok(DataType::Regclass),
                Keyword::STRING => Ok(DataType::String),
                Keyword::TEXT => Ok(DataType::Text),
                Keyword::BYTEA => Ok(DataType::Bytea),
                Keyword::NUMERIC => Ok(DataType::Numeric(
                    self.parse_exact_number_optional_precision_scale()?,
                )),
                Keyword::DECIMAL => Ok(DataType::Decimal(
                    self.parse_exact_number_optional_precision_scale()?,
                )),
                Keyword::DEC => Ok(DataType::Dec(
                    self.parse_exact_number_optional_precision_scale()?,
                )),
                Keyword::ENUM => Ok(DataType::Enum(self.parse_string_values()?)),
                Keyword::SET => Ok(DataType::Set(self.parse_string_values()?)),
                Keyword::ARRAY => {
                    if dialect_of!(self is SnowflakeDialect) {
                        Ok(DataType::Array(None))
                    } else {
                        // Hive array syntax. Note that nesting arrays - or other Hive syntax
                        // that ends with > will fail due to "C++" problem - >> is parsed as
                        // Token::ShiftRight
                        self.expect_token(&Token::Lt)?;
                        let inside_type = self.parse_data_type()?;
                        self.expect_token(&Token::Gt)?;
                        Ok(DataType::Array(Some(Box::new(inside_type))))
                    }
                }
                _ => {
                    self.prev_token();
                    let type_name = self.parse_object_name()?;
                    if let Some(modifiers) = self.parse_optional_type_modifiers()? {
                        Ok(DataType::Custom(type_name, modifiers))
                    } else {
                        Ok(DataType::Custom(type_name, vec![]))
                    }
                }
            },
            _ => self.expected("a data type name", next_token),
        }?;

        // Parse array data types. Note: this is postgresql-specific and different from
        // Keyword::ARRAY syntax from above
        while self.consume_token(&Token::LBracket) {
            self.expect_token(&Token::RBracket)?;
            data = DataType::Array(Some(Box::new(data)))
        }
        Ok(data)
    }

    pub fn parse_string_values(&mut self) -> Result<Vec<String>, ParserError> {
        self.expect_token(&Token::LParen)?;
        let mut values = Vec::new();
        loop {
            let next_token = self.next_token();
            match next_token.token {
                Token::SingleQuotedString(value) => values.push(value),
                _ => self.expected("a string", next_token)?,
            }
            let next_token = self.next_token();
            match next_token.token {
                Token::Comma => (),
                Token::RParen => break,
                _ => self.expected(", or }", next_token)?,
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
        let next_token = self.next_token();
        match next_token.token {
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
            // Support for MySql dialect double qouted string, `AS "HOUR"` for example
            Token::DoubleQuotedString(s) => Ok(Some(Ident::with_quote('\"', s))),
            _ => {
                if after_as {
                    return self.expected("an identifier after AS", next_token);
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

    /// Parse identifiers
    pub fn parse_identifiers(&mut self) -> Result<Vec<Ident>, ParserError> {
        let mut idents = vec![];
        loop {
            match self.peek_token().token {
                Token::Word(w) => {
                    idents.push(w.to_ident());
                }
                Token::EOF | Token::Eq => break,
                _ => {}
            }
            self.next_token();
        }
        Ok(idents)
    }

    /// Parse a simple one-word identifier (possibly quoted, possibly a keyword)
    pub fn parse_identifier(&mut self) -> Result<Ident, ParserError> {
        let next_token = self.next_token();
        match next_token.token {
            Token::Word(w) => Ok(w.to_ident()),
            Token::SingleQuotedString(s) => Ok(Ident::with_quote('\'', s)),
            Token::DoubleQuotedString(s) => Ok(Ident::with_quote('\"', s)),
            _ => self.expected("identifier", next_token),
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

    pub fn parse_precision(&mut self) -> Result<u64, ParserError> {
        self.expect_token(&Token::LParen)?;
        let n = self.parse_literal_uint()?;
        self.expect_token(&Token::RParen)?;
        Ok(n)
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

    pub fn parse_optional_character_length(
        &mut self,
    ) -> Result<Option<CharacterLength>, ParserError> {
        if self.consume_token(&Token::LParen) {
            let character_length = self.parse_character_length()?;
            self.expect_token(&Token::RParen)?;
            Ok(Some(character_length))
        } else {
            Ok(None)
        }
    }

    pub fn parse_character_length(&mut self) -> Result<CharacterLength, ParserError> {
        let length = self.parse_literal_uint()?;
        let unit = if self.parse_keyword(Keyword::CHARACTERS) {
            Some(CharLengthUnits::Characters)
        } else if self.parse_keyword(Keyword::OCTETS) {
            Some(CharLengthUnits::Octets)
        } else {
            None
        };

        Ok(CharacterLength { length, unit })
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

    pub fn parse_exact_number_optional_precision_scale(
        &mut self,
    ) -> Result<ExactNumberInfo, ParserError> {
        if self.consume_token(&Token::LParen) {
            let precision = self.parse_literal_uint()?;
            let scale = if self.consume_token(&Token::Comma) {
                Some(self.parse_literal_uint()?)
            } else {
                None
            };

            self.expect_token(&Token::RParen)?;

            match scale {
                None => Ok(ExactNumberInfo::Precision(precision)),
                Some(scale) => Ok(ExactNumberInfo::PrecisionAndScale(precision, scale)),
            }
        } else {
            Ok(ExactNumberInfo::None)
        }
    }

    pub fn parse_optional_type_modifiers(&mut self) -> Result<Option<Vec<String>>, ParserError> {
        if self.consume_token(&Token::LParen) {
            let mut modifiers = Vec::new();
            loop {
                let next_token = self.next_token();
                match next_token.token {
                    Token::Word(w) => modifiers.push(w.to_string()),
                    Token::Number(n, _) => modifiers.push(n),
                    Token::SingleQuotedString(s) => modifiers.push(s),

                    Token::Comma => {
                        continue;
                    }
                    Token::RParen => {
                        break;
                    }
                    _ => self.expected("type modifiers", next_token)?,
                }
            }

            Ok(Some(modifiers))
        } else {
            Ok(None)
        }
    }

    pub fn parse_delete(&mut self) -> Result<Statement, ParserError> {
        self.expect_keyword(Keyword::FROM)?;
        let table_name = self.parse_table_factor()?;
        let using = if self.parse_keyword(Keyword::USING) {
            Some(self.parse_table_factor()?)
        } else {
            None
        };
        let selection = if self.parse_keyword(Keyword::WHERE) {
            Some(self.parse_expr()?)
        } else {
            None
        };

        let returning = if self.parse_keyword(Keyword::RETURNING) {
            Some(self.parse_comma_separated(Parser::parse_select_item)?)
        } else {
            None
        };

        Ok(Statement::Delete {
            table_name,
            using,
            selection,
            returning,
        })
    }

    // KILL [CONNECTION | QUERY | MUTATION] processlist_id
    pub fn parse_kill(&mut self) -> Result<Statement, ParserError> {
        let modifier_keyword =
            self.parse_one_of_keywords(&[Keyword::CONNECTION, Keyword::QUERY, Keyword::MUTATION]);

        let id = self.parse_literal_uint()?;

        let modifier = match modifier_keyword {
            Some(Keyword::CONNECTION) => Some(KillType::Connection),
            Some(Keyword::QUERY) => Some(KillType::Query),
            Some(Keyword::MUTATION) => {
                if dialect_of!(self is ClickHouseDialect | GenericDialect) {
                    Some(KillType::Mutation)
                } else {
                    self.expected(
                        "Unsupported type for KILL, allowed: CONNECTION | QUERY",
                        self.peek_token(),
                    )?
                }
            }
            _ => None,
        };

        Ok(Statement::Kill { modifier, id })
    }

    pub fn parse_explain(&mut self, describe_alias: bool) -> Result<Statement, ParserError> {
        let analyze = self.parse_keyword(Keyword::ANALYZE);
        let verbose = self.parse_keyword(Keyword::VERBOSE);
        let mut format = None;
        if self.parse_keyword(Keyword::FORMAT) {
            format = Some(self.parse_analyze_format()?);
        }

        match self.maybe_parse(|parser| parser.parse_statement()) {
            Some(Statement::Explain { .. }) | Some(Statement::ExplainTable { .. }) => Err(
                ParserError::ParserError("Explain must be root of the plan".to_string()),
            ),
            Some(statement) => Ok(Statement::Explain {
                describe_alias,
                analyze,
                verbose,
                statement: Box::new(statement),
                format,
            }),
            _ => {
                let table_name = self.parse_object_name()?;
                Ok(Statement::ExplainTable {
                    describe_alias,
                    table_name,
                })
            }
        }
    }

    /// Parse a query expression, i.e. a `SELECT` statement optionally
    /// preceded with some `WITH` CTE declarations and optionally followed
    /// by `ORDER BY`. Unlike some other parse_... methods, this one doesn't
    /// expect the initial keyword to be already consumed
    pub fn parse_query(&mut self) -> Result<Query, ParserError> {
        let _guard = self.recursion_counter.try_decrease()?;
        let with = if self.parse_keyword(Keyword::WITH) {
            Some(With {
                recursive: self.parse_keyword(Keyword::RECURSIVE),
                cte_tables: self.parse_comma_separated(Parser::parse_cte)?,
            })
        } else {
            None
        };

        if !self.parse_keyword(Keyword::INSERT) {
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

                if dialect_of!(self is GenericDialect | MySqlDialect)
                    && limit.is_some()
                    && offset.is_none()
                    && self.consume_token(&Token::Comma)
                {
                    // MySQL style LIMIT x,y => LIMIT y OFFSET x.
                    // Check <https://dev.mysql.com/doc/refman/8.0/en/select.html> for more details.
                    offset = Some(Offset {
                        value: limit.unwrap(),
                        rows: OffsetRows::None,
                    });
                    limit = Some(self.parse_expr()?);
                }
            }

            let fetch = if self.parse_keyword(Keyword::FETCH) {
                Some(self.parse_fetch()?)
            } else {
                None
            };

            let mut locks = Vec::new();
            while self.parse_keyword(Keyword::FOR) {
                locks.push(self.parse_lock()?);
            }

            Ok(Query {
                with,
                body,
                order_by,
                limit,
                offset,
                fetch,
                locks,
            })
        } else {
            let insert = self.parse_insert()?;

            Ok(Query {
                with,
                body: Box::new(SetExpr::Insert(insert)),
                limit: None,
                order_by: vec![],
                offset: None,
                fetch: None,
                locks: vec![],
            })
        }
    }

    /// Parse a CTE (`alias [( col1, col2, ... )] AS (subquery)`)
    pub fn parse_cte(&mut self) -> Result<Cte, ParserError> {
        let name = self.parse_identifier()?;

        let mut cte = if self.parse_keyword(Keyword::AS) {
            self.expect_token(&Token::LParen)?;
            let query = Box::new(self.parse_query()?);
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
            let query = Box::new(self.parse_query()?);
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
    /// ```sql
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
        } else if self.parse_keyword(Keyword::TABLE) {
            SetExpr::Table(Box::new(self.parse_as_table()?))
        } else {
            return self.expected(
                "SELECT, VALUES, or a subquery in the query body",
                self.peek_token(),
            );
        };

        loop {
            // The query can be optionally followed by a set operator:
            let op = self.parse_set_operator(&self.peek_token().token);
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
            let set_quantifier = self.parse_set_quantifier(&op);
            expr = SetExpr::SetOperation {
                left: Box::new(expr),
                op: op.unwrap(),
                set_quantifier,
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

    pub fn parse_set_quantifier(&mut self, op: &Option<SetOperator>) -> SetQuantifier {
        match op {
            Some(SetOperator::Union) => {
                if self.parse_keyword(Keyword::ALL) {
                    SetQuantifier::All
                } else if self.parse_keyword(Keyword::DISTINCT) {
                    SetQuantifier::Distinct
                } else {
                    SetQuantifier::None
                }
            }
            Some(SetOperator::Except) | Some(SetOperator::Intersect) => {
                if self.parse_keyword(Keyword::ALL) {
                    SetQuantifier::All
                } else if self.parse_keyword(Keyword::DISTINCT) {
                    SetQuantifier::Distinct
                } else {
                    SetQuantifier::None
                }
            }
            _ => SetQuantifier::None,
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

        let projection = self.parse_projection()?;

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

    /// Parse `CREATE TABLE x AS TABLE y`
    pub fn parse_as_table(&mut self) -> Result<Table, ParserError> {
        let token1 = self.next_token();
        let token2 = self.next_token();
        let token3 = self.next_token();

        let table_name;
        let schema_name;
        if token2 == Token::Period {
            match token1.token {
                Token::Word(w) => {
                    schema_name = w.value;
                }
                _ => {
                    return self.expected("Schema name", token1);
                }
            }
            match token3.token {
                Token::Word(w) => {
                    table_name = w.value;
                }
                _ => {
                    return self.expected("Table name", token3);
                }
            }
            Ok(Table {
                table_name: Some(table_name),
                schema_name: Some(schema_name),
            })
        } else {
            match token1.token {
                Token::Word(w) => {
                    table_name = w.value;
                }
                _ => {
                    return self.expected("Table name", token1);
                }
            }
            Ok(Table {
                table_name: Some(table_name),
                schema_name: None,
            })
        }
    }

    pub fn parse_set(&mut self) -> Result<Statement, ParserError> {
        let modifier =
            self.parse_one_of_keywords(&[Keyword::SESSION, Keyword::LOCAL, Keyword::HIVEVAR]);
        if let Some(Keyword::HIVEVAR) = modifier {
            self.expect_token(&Token::Colon)?;
        } else if self.parse_keyword(Keyword::ROLE) {
            let context_modifier = match modifier {
                Some(keyword) if keyword == Keyword::LOCAL => ContextModifier::Local,
                Some(keyword) if keyword == Keyword::SESSION => ContextModifier::Session,
                _ => ContextModifier::None,
            };

            let role_name = if self.parse_keyword(Keyword::NONE) {
                None
            } else {
                Some(self.parse_identifier()?)
            };
            return Ok(Statement::SetRole {
                context_modifier,
                role_name,
            });
        }

        let variable = if self.parse_keywords(&[Keyword::TIME, Keyword::ZONE]) {
            ObjectName(vec!["TIMEZONE".into()])
        } else {
            self.parse_object_name()?
        };

        if variable.to_string().eq_ignore_ascii_case("NAMES")
            && dialect_of!(self is MySqlDialect | GenericDialect)
        {
            if self.parse_keyword(Keyword::DEFAULT) {
                return Ok(Statement::SetNamesDefault {});
            }

            let charset_name = self.parse_literal_string()?;
            let collation_name = if self.parse_one_of_keywords(&[Keyword::COLLATE]).is_some() {
                Some(self.parse_literal_string()?)
            } else {
                None
            };

            Ok(Statement::SetNames {
                charset_name,
                collation_name,
            })
        } else if self.consume_token(&Token::Eq) || self.parse_keyword(Keyword::TO) {
            let mut values = vec![];
            loop {
                let value = if let Ok(expr) = self.parse_expr() {
                    expr
                } else {
                    self.expected("variable value", self.peek_token())?
                };

                values.push(value);
                if self.consume_token(&Token::Comma) {
                    continue;
                }
                return Ok(Statement::SetVariable {
                    local: modifier == Some(Keyword::LOCAL),
                    hivevar: Some(Keyword::HIVEVAR) == modifier,
                    variable,
                    value: values,
                });
            }
        } else if variable.to_string().eq_ignore_ascii_case("TIMEZONE") {
            // for some db (e.g. postgresql), SET TIME ZONE <value> is an alias for SET TIMEZONE [TO|=] <value>
            match self.parse_expr() {
                Ok(expr) => Ok(Statement::SetTimeZone {
                    local: modifier == Some(Keyword::LOCAL),
                    value: expr,
                }),
                _ => self.expected("timezone value", self.peek_token())?,
            }
        } else if variable.to_string() == "CHARACTERISTICS" {
            self.expect_keywords(&[Keyword::AS, Keyword::TRANSACTION])?;
            Ok(Statement::SetTransaction {
                modes: self.parse_transaction_modes()?,
                snapshot: None,
                session: true,
            })
        } else if variable.to_string() == "TRANSACTION" && modifier.is_none() {
            if self.parse_keyword(Keyword::SNAPSHOT) {
                let snaphot_id = self.parse_value()?;
                return Ok(Statement::SetTransaction {
                    modes: vec![],
                    snapshot: Some(snaphot_id),
                    session: false,
                });
            }
            Ok(Statement::SetTransaction {
                modes: self.parse_transaction_modes()?,
                snapshot: None,
                session: false,
            })
        } else {
            self.expected("equals sign or TO", self.peek_token())
        }
    }

    pub fn parse_show(&mut self) -> Result<Statement, ParserError> {
        let extended = self.parse_keyword(Keyword::EXTENDED);
        let full = self.parse_keyword(Keyword::FULL);
        if self
            .parse_one_of_keywords(&[Keyword::COLUMNS, Keyword::FIELDS])
            .is_some()
        {
            Ok(self.parse_show_columns(extended, full)?)
        } else if self.parse_keyword(Keyword::TABLES) {
            Ok(self.parse_show_tables(extended, full)?)
        } else if self.parse_keyword(Keyword::FUNCTIONS) {
            Ok(self.parse_show_functions()?)
        } else if extended || full {
            Err(ParserError::ParserError(
                "EXTENDED/FULL are not supported with this type of SHOW query".to_string(),
            ))
        } else if self.parse_one_of_keywords(&[Keyword::CREATE]).is_some() {
            Ok(self.parse_show_create()?)
        } else if self.parse_keyword(Keyword::COLLATION) {
            Ok(self.parse_show_collation()?)
        } else if self.parse_keyword(Keyword::VARIABLES)
            && dialect_of!(self is MySqlDialect | GenericDialect)
        {
            // TODO: Support GLOBAL|SESSION
            Ok(Statement::ShowVariables {
                filter: self.parse_show_statement_filter()?,
            })
        } else {
            Ok(Statement::ShowVariable {
                variable: self.parse_identifiers()?,
            })
        }
    }

    pub fn parse_show_create(&mut self) -> Result<Statement, ParserError> {
        let obj_type = match self.expect_one_of_keywords(&[
            Keyword::TABLE,
            Keyword::TRIGGER,
            Keyword::FUNCTION,
            Keyword::PROCEDURE,
            Keyword::EVENT,
            Keyword::VIEW,
        ])? {
            Keyword::TABLE => Ok(ShowCreateObject::Table),
            Keyword::TRIGGER => Ok(ShowCreateObject::Trigger),
            Keyword::FUNCTION => Ok(ShowCreateObject::Function),
            Keyword::PROCEDURE => Ok(ShowCreateObject::Procedure),
            Keyword::EVENT => Ok(ShowCreateObject::Event),
            Keyword::VIEW => Ok(ShowCreateObject::View),
            keyword => Err(ParserError::ParserError(format!(
                "Unable to map keyword to ShowCreateObject: {:?}",
                keyword
            ))),
        }?;

        let obj_name = self.parse_object_name()?;

        Ok(Statement::ShowCreate { obj_type, obj_name })
    }

    pub fn parse_show_columns(
        &mut self,
        extended: bool,
        full: bool,
    ) -> Result<Statement, ParserError> {
        self.expect_one_of_keywords(&[Keyword::FROM, Keyword::IN])?;
        let object_name = self.parse_object_name()?;
        let table_name = match self.parse_one_of_keywords(&[Keyword::FROM, Keyword::IN]) {
            Some(_) => {
                let db_name = vec![self.parse_identifier()?];
                let ObjectName(table_name) = object_name;
                let object_name = db_name.into_iter().chain(table_name.into_iter()).collect();
                ObjectName(object_name)
            }
            None => object_name,
        };
        let filter = self.parse_show_statement_filter()?;
        Ok(Statement::ShowColumns {
            extended,
            full,
            table_name,
            filter,
        })
    }

    pub fn parse_show_tables(
        &mut self,
        extended: bool,
        full: bool,
    ) -> Result<Statement, ParserError> {
        let db_name = match self.parse_one_of_keywords(&[Keyword::FROM, Keyword::IN]) {
            Some(_) => Some(self.parse_identifier()?),
            None => None,
        };
        let filter = self.parse_show_statement_filter()?;
        Ok(Statement::ShowTables {
            extended,
            full,
            db_name,
            filter,
        })
    }

    pub fn parse_show_functions(&mut self) -> Result<Statement, ParserError> {
        let filter = self.parse_show_statement_filter()?;
        Ok(Statement::ShowFunctions { filter })
    }

    pub fn parse_show_collation(&mut self) -> Result<Statement, ParserError> {
        let filter = self.parse_show_statement_filter()?;
        Ok(Statement::ShowCollation { filter })
    }

    pub fn parse_show_statement_filter(
        &mut self,
    ) -> Result<Option<ShowStatementFilter>, ParserError> {
        if self.parse_keyword(Keyword::LIKE) {
            Ok(Some(ShowStatementFilter::Like(
                self.parse_literal_string()?,
            )))
        } else if self.parse_keyword(Keyword::ILIKE) {
            Ok(Some(ShowStatementFilter::ILike(
                self.parse_literal_string()?,
            )))
        } else if self.parse_keyword(Keyword::WHERE) {
            Ok(Some(ShowStatementFilter::Where(self.parse_expr()?)))
        } else {
            Ok(None)
        }
    }

    pub fn parse_use(&mut self) -> Result<Statement, ParserError> {
        let db_name = self.parse_identifier()?;
        Ok(Statement::Use { db_name })
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
                let peek_keyword = if let Token::Word(w) = self.peek_token().token {
                    w.keyword
                } else {
                    Keyword::NoKeyword
                };

                let join_operator_type = match peek_keyword {
                    Keyword::INNER | Keyword::JOIN => {
                        let _ = self.parse_keyword(Keyword::INNER); // [ INNER ]
                        self.expect_keyword(Keyword::JOIN)?;
                        JoinOperator::Inner
                    }
                    kw @ Keyword::LEFT | kw @ Keyword::RIGHT => {
                        let _ = self.next_token(); // consume LEFT/RIGHT
                        let is_left = kw == Keyword::LEFT;
                        let join_type = self.parse_one_of_keywords(&[
                            Keyword::OUTER,
                            Keyword::SEMI,
                            Keyword::ANTI,
                            Keyword::JOIN,
                        ]);
                        match join_type {
                            Some(Keyword::OUTER) => {
                                self.expect_keyword(Keyword::JOIN)?;
                                if is_left {
                                    JoinOperator::LeftOuter
                                } else {
                                    JoinOperator::RightOuter
                                }
                            }
                            Some(Keyword::SEMI) => {
                                self.expect_keyword(Keyword::JOIN)?;
                                if is_left {
                                    JoinOperator::LeftSemi
                                } else {
                                    JoinOperator::RightSemi
                                }
                            }
                            Some(Keyword::ANTI) => {
                                self.expect_keyword(Keyword::JOIN)?;
                                if is_left {
                                    JoinOperator::LeftAnti
                                } else {
                                    JoinOperator::RightAnti
                                }
                            }
                            Some(Keyword::JOIN) => {
                                if is_left {
                                    JoinOperator::LeftOuter
                                } else {
                                    JoinOperator::RightOuter
                                }
                            }
                            _ => {
                                return Err(ParserError::ParserError(format!(
                                    "expected OUTER, SEMI, ANTI or JOIN after {:?}",
                                    kw
                                )))
                            }
                        }
                    }
                    Keyword::FULL => {
                        let _ = self.next_token(); // consume FULL
                        let _ = self.parse_keyword(Keyword::OUTER); // [ OUTER ]
                        self.expect_keyword(Keyword::JOIN)?;
                        JoinOperator::FullOuter
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
                let alias = self.parse_optional_table_alias(keywords::RESERVED_FOR_TABLE_ALIAS)?;
                Ok(TableFactor::NestedJoin {
                    table_with_joins: Box::new(table_and_joins),
                    alias,
                }) // (A)
            } else if let TableFactor::NestedJoin {
                table_with_joins: _,
                alias: _,
            } = &table_and_joins.relation
            {
                // (B): `table_and_joins` (what we found inside the parentheses)
                // is a nested join `(foo JOIN bar)`, not followed by other joins.
                self.expect_token(&Token::RParen)?;
                let alias = self.parse_optional_table_alias(keywords::RESERVED_FOR_TABLE_ALIAS)?;
                Ok(TableFactor::NestedJoin {
                    table_with_joins: Box::new(table_and_joins),
                    alias,
                })
            } else if dialect_of!(self is SnowflakeDialect | GenericDialect) {
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
                        | TableFactor::TableFunction { alias, .. }
                        | TableFactor::NestedJoin { alias, .. } => {
                            // but not `FROM (mytable AS alias1) AS alias2`.
                            if let Some(inner_alias) = alias {
                                return Err(ParserError::ParserError(format!(
                                    "duplicate alias {}",
                                    inner_alias
                                )));
                            }
                            // Act as if the alias was specified normally next
                            // to the table name: `(mytable) AS alias` ->
                            // `(mytable AS alias)`
                            alias.replace(outer_alias);
                        }
                    };
                }
                // Do not store the extra set of parens in the AST
                Ok(table_and_joins.relation)
            } else {
                // The SQL spec prohibits derived tables and bare tables from
                // appearing alone in parentheses (e.g. `FROM (mytable)`)
                self.expected("joined table", self.peek_token())
            }
        } else if dialect_of!(self is BigQueryDialect | GenericDialect)
            && self.parse_keyword(Keyword::UNNEST)
        {
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

            let with_offset_alias = if with_offset {
                match self.parse_optional_alias(keywords::RESERVED_FOR_COLUMN_ALIAS) {
                    Ok(Some(alias)) => Some(alias),
                    Ok(None) => None,
                    Err(e) => return Err(e),
                }
            } else {
                None
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

    /// Parse a GRANT statement.
    pub fn parse_grant(&mut self) -> Result<Statement, ParserError> {
        let (privileges, objects) = self.parse_grant_revoke_privileges_objects()?;

        self.expect_keyword(Keyword::TO)?;
        let grantees = self.parse_comma_separated(Parser::parse_identifier)?;

        let with_grant_option =
            self.parse_keywords(&[Keyword::WITH, Keyword::GRANT, Keyword::OPTION]);

        let granted_by = self
            .parse_keywords(&[Keyword::GRANTED, Keyword::BY])
            .then(|| self.parse_identifier().unwrap());

        Ok(Statement::Grant {
            privileges,
            objects,
            grantees,
            with_grant_option,
            granted_by,
        })
    }

    pub fn parse_grant_revoke_privileges_objects(
        &mut self,
    ) -> Result<(Privileges, GrantObjects), ParserError> {
        let privileges = if self.parse_keyword(Keyword::ALL) {
            Privileges::All {
                with_privileges_keyword: self.parse_keyword(Keyword::PRIVILEGES),
            }
        } else {
            let (actions, err): (Vec<_>, Vec<_>) = self
                .parse_comma_separated(Parser::parse_grant_permission)?
                .into_iter()
                .map(|(kw, columns)| match kw {
                    Keyword::DELETE => Ok(Action::Delete),
                    Keyword::INSERT => Ok(Action::Insert { columns }),
                    Keyword::REFERENCES => Ok(Action::References { columns }),
                    Keyword::SELECT => Ok(Action::Select { columns }),
                    Keyword::TRIGGER => Ok(Action::Trigger),
                    Keyword::TRUNCATE => Ok(Action::Truncate),
                    Keyword::UPDATE => Ok(Action::Update { columns }),
                    Keyword::USAGE => Ok(Action::Usage),
                    Keyword::CONNECT => Ok(Action::Connect),
                    Keyword::CREATE => Ok(Action::Create),
                    Keyword::EXECUTE => Ok(Action::Execute),
                    Keyword::TEMPORARY => Ok(Action::Temporary),
                    // This will cover all future added keywords to
                    // parse_grant_permission and unhandled in this
                    // match
                    _ => Err(kw),
                })
                .partition(Result::is_ok);

            if !err.is_empty() {
                let errors: Vec<Keyword> = err.into_iter().filter_map(|x| x.err()).collect();
                return Err(ParserError::ParserError(format!(
                    "INTERNAL ERROR: GRANT/REVOKE unexpected keyword(s) - {:?}",
                    errors
                )));
            }
            let act = actions.into_iter().filter_map(|x| x.ok()).collect();
            Privileges::Actions(act)
        };

        self.expect_keyword(Keyword::ON)?;

        let objects = if self.parse_keywords(&[
            Keyword::ALL,
            Keyword::TABLES,
            Keyword::IN,
            Keyword::SCHEMA,
        ]) {
            GrantObjects::AllTablesInSchema {
                schemas: self.parse_comma_separated(Parser::parse_object_name)?,
            }
        } else if self.parse_keywords(&[
            Keyword::ALL,
            Keyword::SEQUENCES,
            Keyword::IN,
            Keyword::SCHEMA,
        ]) {
            GrantObjects::AllSequencesInSchema {
                schemas: self.parse_comma_separated(Parser::parse_object_name)?,
            }
        } else {
            let object_type =
                self.parse_one_of_keywords(&[Keyword::SEQUENCE, Keyword::SCHEMA, Keyword::TABLE]);
            let objects = self.parse_comma_separated(Parser::parse_object_name);
            match object_type {
                Some(Keyword::SCHEMA) => GrantObjects::Schemas(objects?),
                Some(Keyword::SEQUENCE) => GrantObjects::Sequences(objects?),
                Some(Keyword::TABLE) | None => GrantObjects::Tables(objects?),
                _ => unreachable!(),
            }
        };

        Ok((privileges, objects))
    }

    pub fn parse_grant_permission(&mut self) -> Result<(Keyword, Option<Vec<Ident>>), ParserError> {
        if let Some(kw) = self.parse_one_of_keywords(&[
            Keyword::CONNECT,
            Keyword::CREATE,
            Keyword::DELETE,
            Keyword::EXECUTE,
            Keyword::INSERT,
            Keyword::REFERENCES,
            Keyword::SELECT,
            Keyword::TEMPORARY,
            Keyword::TRIGGER,
            Keyword::TRUNCATE,
            Keyword::UPDATE,
            Keyword::USAGE,
        ]) {
            let columns = match kw {
                Keyword::INSERT | Keyword::REFERENCES | Keyword::SELECT | Keyword::UPDATE => {
                    let columns = self.parse_parenthesized_column_list(Optional)?;
                    if columns.is_empty() {
                        None
                    } else {
                        Some(columns)
                    }
                }
                _ => None,
            };
            Ok((kw, columns))
        } else {
            self.expected("a privilege keyword", self.peek_token())?
        }
    }

    /// Parse a REVOKE statement
    pub fn parse_revoke(&mut self) -> Result<Statement, ParserError> {
        let (privileges, objects) = self.parse_grant_revoke_privileges_objects()?;

        self.expect_keyword(Keyword::FROM)?;
        let grantees = self.parse_comma_separated(Parser::parse_identifier)?;

        let granted_by = self
            .parse_keywords(&[Keyword::GRANTED, Keyword::BY])
            .then(|| self.parse_identifier().unwrap());

        let cascade = self.parse_keyword(Keyword::CASCADE);
        let restrict = self.parse_keyword(Keyword::RESTRICT);
        if cascade && restrict {
            return parser_err!("Cannot specify both CASCADE and RESTRICT in REVOKE");
        }

        Ok(Statement::Revoke {
            privileges,
            objects,
            grantees,
            granted_by,
            cascade,
        })
    }

    /// Parse an INSERT statement
    pub fn parse_insert(&mut self) -> Result<Statement, ParserError> {
        let or = if !dialect_of!(self is SQLiteDialect) {
            None
        } else if self.parse_keywords(&[Keyword::OR, Keyword::REPLACE]) {
            Some(SqliteOnConflict::Replace)
        } else if self.parse_keywords(&[Keyword::OR, Keyword::ROLLBACK]) {
            Some(SqliteOnConflict::Rollback)
        } else if self.parse_keywords(&[Keyword::OR, Keyword::ABORT]) {
            Some(SqliteOnConflict::Abort)
        } else if self.parse_keywords(&[Keyword::OR, Keyword::FAIL]) {
            Some(SqliteOnConflict::Fail)
        } else if self.parse_keywords(&[Keyword::OR, Keyword::IGNORE]) {
            Some(SqliteOnConflict::Ignore)
        } else if self.parse_keyword(Keyword::REPLACE) {
            Some(SqliteOnConflict::Replace)
        } else {
            None
        };

        let action = self.parse_one_of_keywords(&[Keyword::INTO, Keyword::OVERWRITE]);
        let into = action == Some(Keyword::INTO);
        let overwrite = action == Some(Keyword::OVERWRITE);

        let local = self.parse_keyword(Keyword::LOCAL);

        if self.parse_keyword(Keyword::DIRECTORY) {
            let path = self.parse_literal_string()?;
            let file_format = if self.parse_keywords(&[Keyword::STORED, Keyword::AS]) {
                Some(self.parse_file_format()?)
            } else {
                None
            };
            let source = Box::new(self.parse_query()?);
            Ok(Statement::Directory {
                local,
                path,
                overwrite,
                file_format,
                source,
            })
        } else {
            // Hive lets you put table here regardless
            let table = self.parse_keyword(Keyword::TABLE);
            let table_name = self.parse_object_name()?;
            let columns = self.parse_parenthesized_column_list(Optional)?;

            let partitioned = if self.parse_keyword(Keyword::PARTITION) {
                self.expect_token(&Token::LParen)?;
                let r = Some(self.parse_comma_separated(Parser::parse_expr)?);
                self.expect_token(&Token::RParen)?;
                r
            } else {
                None
            };

            // Hive allows you to specify columns after partitions as well if you want.
            let after_columns = self.parse_parenthesized_column_list(Optional)?;

            let source = Box::new(self.parse_query()?);
            let on = if self.parse_keyword(Keyword::ON) {
                if self.parse_keyword(Keyword::CONFLICT) {
                    let conflict_target =
                        if self.parse_keywords(&[Keyword::ON, Keyword::CONSTRAINT]) {
                            Some(ConflictTarget::OnConstraint(self.parse_object_name()?))
                        } else if self.peek_token() == Token::LParen {
                            Some(ConflictTarget::Columns(
                                self.parse_parenthesized_column_list(IsOptional::Mandatory)?,
                            ))
                        } else {
                            None
                        };

                    self.expect_keyword(Keyword::DO)?;
                    let action = if self.parse_keyword(Keyword::NOTHING) {
                        OnConflictAction::DoNothing
                    } else {
                        self.expect_keyword(Keyword::UPDATE)?;
                        self.expect_keyword(Keyword::SET)?;
                        let assignments = self.parse_comma_separated(Parser::parse_assignment)?;
                        let selection = if self.parse_keyword(Keyword::WHERE) {
                            Some(self.parse_expr()?)
                        } else {
                            None
                        };
                        OnConflictAction::DoUpdate(DoUpdate {
                            assignments,
                            selection,
                        })
                    };

                    Some(OnInsert::OnConflict(OnConflict {
                        conflict_target,
                        action,
                    }))
                } else {
                    self.expect_keyword(Keyword::DUPLICATE)?;
                    self.expect_keyword(Keyword::KEY)?;
                    self.expect_keyword(Keyword::UPDATE)?;
                    let l = self.parse_comma_separated(Parser::parse_assignment)?;

                    Some(OnInsert::DuplicateKeyUpdate(l))
                }
            } else {
                None
            };

            let returning = if self.parse_keyword(Keyword::RETURNING) {
                Some(self.parse_comma_separated(Parser::parse_select_item)?)
            } else {
                None
            };

            Ok(Statement::Insert {
                or,
                table_name,
                into,
                overwrite,
                partitioned,
                columns,
                after_columns,
                source,
                table,
                on,
                returning,
            })
        }
    }

    pub fn parse_update(&mut self) -> Result<Statement, ParserError> {
        let table = self.parse_table_and_joins()?;
        self.expect_keyword(Keyword::SET)?;
        let assignments = self.parse_comma_separated(Parser::parse_assignment)?;
        let from = if self.parse_keyword(Keyword::FROM)
            && dialect_of!(self is GenericDialect | PostgreSqlDialect | BigQueryDialect | SnowflakeDialect | RedshiftSqlDialect | MsSqlDialect)
        {
            Some(self.parse_table_and_joins()?)
        } else {
            None
        };
        let selection = if self.parse_keyword(Keyword::WHERE) {
            Some(self.parse_expr()?)
        } else {
            None
        };
        let returning = if self.parse_keyword(Keyword::RETURNING) {
            Some(self.parse_comma_separated(Parser::parse_select_item)?)
        } else {
            None
        };
        Ok(Statement::Update {
            table,
            assignments,
            from,
            selection,
            returning,
        })
    }

    /// Parse a `var = expr` assignment, used in an UPDATE statement
    pub fn parse_assignment(&mut self) -> Result<Assignment, ParserError> {
        let id = self.parse_identifiers()?;
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
            WildcardExpr::Expr(expr) => {
                let expr: Expr = if self.dialect.supports_filter_during_aggregation()
                    && self.parse_keyword(Keyword::FILTER)
                {
                    let i = self.index - 1;
                    if self.consume_token(&Token::LParen) && self.parse_keyword(Keyword::WHERE) {
                        let filter = self.parse_expr()?;
                        self.expect_token(&Token::RParen)?;
                        Expr::AggregateExpressionWithFilter {
                            expr: Box::new(expr),
                            filter: Box::new(filter),
                        }
                    } else {
                        self.index = i;
                        expr
                    }
                } else {
                    expr
                };
                self.parse_optional_alias(keywords::RESERVED_FOR_COLUMN_ALIAS)
                    .map(|alias| match alias {
                        Some(alias) => SelectItem::ExprWithAlias { expr, alias },
                        None => SelectItem::UnnamedExpr(expr),
                    })
            }
            WildcardExpr::QualifiedWildcard(prefix) => Ok(SelectItem::QualifiedWildcard(
                prefix,
                self.parse_wildcard_additional_options()?,
            )),
            WildcardExpr::Wildcard => Ok(SelectItem::Wildcard(
                self.parse_wildcard_additional_options()?,
            )),
        }
    }

    /// Parse an [`WildcardAdditionalOptions`](WildcardAdditionalOptions) information for wildcard select items.
    ///
    /// If it is not possible to parse it, will return an option.
    pub fn parse_wildcard_additional_options(
        &mut self,
    ) -> Result<WildcardAdditionalOptions, ParserError> {
        let opt_exclude = if dialect_of!(self is GenericDialect | SnowflakeDialect) {
            self.parse_optional_select_item_exclude()?
        } else {
            None
        };
        let opt_except = if dialect_of!(self is GenericDialect | BigQueryDialect) {
            self.parse_optional_select_item_except()?
        } else {
            None
        };

        Ok(WildcardAdditionalOptions {
            opt_exclude,
            opt_except,
        })
    }

    /// Parse an [`Exclude`](ExcludeSelectItem) information for wildcard select items.
    ///
    /// If it is not possible to parse it, will return an option.
    pub fn parse_optional_select_item_exclude(
        &mut self,
    ) -> Result<Option<ExcludeSelectItem>, ParserError> {
        let opt_exclude = if self.parse_keyword(Keyword::EXCLUDE) {
            if self.consume_token(&Token::LParen) {
                let columns = self.parse_comma_separated(|parser| parser.parse_identifier())?;
                self.expect_token(&Token::RParen)?;
                Some(ExcludeSelectItem::Multiple(columns))
            } else {
                let column = self.parse_identifier()?;
                Some(ExcludeSelectItem::Single(column))
            }
        } else {
            None
        };

        Ok(opt_exclude)
    }

    /// Parse an [`Except`](ExceptSelectItem) information for wildcard select items.
    ///
    /// If it is not possible to parse it, will return an option.
    pub fn parse_optional_select_item_except(
        &mut self,
    ) -> Result<Option<ExceptSelectItem>, ParserError> {
        let opt_except = if self.parse_keyword(Keyword::EXCEPT) {
            let idents = self.parse_parenthesized_column_list(Mandatory)?;
            match &idents[..] {
                [] => {
                    return self.expected(
                        "at least one column should be parsed by the expect clause",
                        self.peek_token(),
                    )?;
                }
                [first, idents @ ..] => Some(ExceptSelectItem {
                    fist_elemnt: first.clone(),
                    additional_elements: idents.to_vec(),
                }),
            }
        } else {
            None
        };

        Ok(opt_except)
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
    /// that follows after `SELECT [DISTINCT]`.
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
            Ok(Some(self.parse_expr()?))
        }
    }

    /// Parse an OFFSET clause
    pub fn parse_offset(&mut self) -> Result<Offset, ParserError> {
        let value = self.parse_expr()?;
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
    pub fn parse_lock(&mut self) -> Result<LockClause, ParserError> {
        let lock_type = match self.expect_one_of_keywords(&[Keyword::UPDATE, Keyword::SHARE])? {
            Keyword::UPDATE => LockType::Update,
            Keyword::SHARE => LockType::Share,
            _ => unreachable!(),
        };
        let of = if self.parse_keyword(Keyword::OF) {
            Some(self.parse_object_name()?)
        } else {
            None
        };
        let nonblock = if self.parse_keyword(Keyword::NOWAIT) {
            Some(NonBlock::Nowait)
        } else if self.parse_keywords(&[Keyword::SKIP, Keyword::LOCKED]) {
            Some(NonBlock::SkipLocked)
        } else {
            None
        };
        Ok(LockClause {
            lock_type,
            of,
            nonblock,
        })
    }

    pub fn parse_values(&mut self) -> Result<Values, ParserError> {
        let mut explicit_row = false;

        let rows = self.parse_comma_separated(|parser| {
            if parser.parse_keyword(Keyword::ROW) {
                explicit_row = true;
            }

            parser.expect_token(&Token::LParen)?;
            let exprs = parser.parse_comma_separated(Parser::parse_expr)?;
            parser.expect_token(&Token::RParen)?;
            Ok(exprs)
        })?;
        Ok(Values { explicit_row, rows })
    }

    pub fn parse_start_transaction(&mut self) -> Result<Statement, ParserError> {
        self.expect_keyword(Keyword::TRANSACTION)?;
        Ok(Statement::StartTransaction {
            modes: self.parse_transaction_modes()?,
        })
    }

    pub fn parse_begin(&mut self) -> Result<Statement, ParserError> {
        let _ = self.parse_one_of_keywords(&[Keyword::TRANSACTION, Keyword::WORK]);
        Ok(Statement::StartTransaction {
            modes: self.parse_transaction_modes()?,
        })
    }

    pub fn parse_transaction_modes(&mut self) -> Result<Vec<TransactionMode>, ParserError> {
        let mut modes = vec![];
        let mut required = false;
        loop {
            let mode = if self.parse_keywords(&[Keyword::ISOLATION, Keyword::LEVEL]) {
                let iso_level = if self.parse_keywords(&[Keyword::READ, Keyword::UNCOMMITTED]) {
                    TransactionIsolationLevel::ReadUncommitted
                } else if self.parse_keywords(&[Keyword::READ, Keyword::COMMITTED]) {
                    TransactionIsolationLevel::ReadCommitted
                } else if self.parse_keywords(&[Keyword::REPEATABLE, Keyword::READ]) {
                    TransactionIsolationLevel::RepeatableRead
                } else if self.parse_keyword(Keyword::SERIALIZABLE) {
                    TransactionIsolationLevel::Serializable
                } else {
                    self.expected("isolation level", self.peek_token())?
                };
                TransactionMode::IsolationLevel(iso_level)
            } else if self.parse_keywords(&[Keyword::READ, Keyword::ONLY]) {
                TransactionMode::AccessMode(TransactionAccessMode::ReadOnly)
            } else if self.parse_keywords(&[Keyword::READ, Keyword::WRITE]) {
                TransactionMode::AccessMode(TransactionAccessMode::ReadWrite)
            } else if required {
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

    pub fn parse_commit(&mut self) -> Result<Statement, ParserError> {
        Ok(Statement::Commit {
            chain: self.parse_commit_rollback_chain()?,
        })
    }

    pub fn parse_rollback(&mut self) -> Result<Statement, ParserError> {
        Ok(Statement::Rollback {
            chain: self.parse_commit_rollback_chain()?,
        })
    }

    pub fn parse_commit_rollback_chain(&mut self) -> Result<bool, ParserError> {
        let _ = self.parse_one_of_keywords(&[Keyword::TRANSACTION, Keyword::WORK]);
        if self.parse_keyword(Keyword::AND) {
            let chain = !self.parse_keyword(Keyword::NO);
            self.expect_keyword(Keyword::CHAIN)?;
            Ok(chain)
        } else {
            Ok(false)
        }
    }

    pub fn parse_deallocate(&mut self) -> Result<Statement, ParserError> {
        let prepare = self.parse_keyword(Keyword::PREPARE);
        let name = self.parse_identifier()?;
        Ok(Statement::Deallocate { name, prepare })
    }

    pub fn parse_execute(&mut self) -> Result<Statement, ParserError> {
        let name = self.parse_identifier()?;

        let mut parameters = vec![];
        if self.consume_token(&Token::LParen) {
            parameters = self.parse_comma_separated(Parser::parse_expr)?;
            self.expect_token(&Token::RParen)?;
        }

        Ok(Statement::Execute { name, parameters })
    }

    pub fn parse_prepare(&mut self) -> Result<Statement, ParserError> {
        let name = self.parse_identifier()?;

        let mut data_types = vec![];
        if self.consume_token(&Token::LParen) {
            data_types = self.parse_comma_separated(Parser::parse_data_type)?;
            self.expect_token(&Token::RParen)?;
        }

        self.expect_keyword(Keyword::AS)?;
        let statement = Box::new(self.parse_statement()?);
        Ok(Statement::Prepare {
            name,
            data_types,
            statement,
        })
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
                        ));
                    }
                    None => {
                        return Err(ParserError::ParserError(
                            "expected UPDATE, DELETE or INSERT in merge clause".to_string(),
                        ));
                    }
                },
            );
        }
        Ok(clauses)
    }

    pub fn parse_merge(&mut self) -> Result<Statement, ParserError> {
        let into = self.parse_keyword(Keyword::INTO);

        let table = self.parse_table_factor()?;

        self.expect_keyword(Keyword::USING)?;
        let source = self.parse_table_factor()?;
        self.expect_keyword(Keyword::ON)?;
        let on = self.parse_expr()?;
        let clauses = self.parse_merge_clauses()?;

        Ok(Statement::Merge {
            into,
            table,
            source,
            on: Box::new(on),
            clauses,
        })
    }

    /// ```sql
    /// CREATE [ { TEMPORARY | TEMP } ] SEQUENCE [ IF NOT EXISTS ] <sequence_name>
    /// ```
    ///
    /// See [Postgres docs](https://www.postgresql.org/docs/current/sql-createsequence.html) for more details.
    pub fn parse_create_sequence(&mut self, temporary: bool) -> Result<Statement, ParserError> {
        //[ IF NOT EXISTS ]
        let if_not_exists = self.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        //name
        let name = self.parse_object_name()?;
        //[ AS data_type ]
        let mut data_type: Option<DataType> = None;
        if self.parse_keywords(&[Keyword::AS]) {
            data_type = Some(self.parse_data_type()?)
        }
        let sequence_options = self.parse_create_sequence_options()?;
        // [ OWNED BY { table_name.column_name | NONE } ]
        let owned_by = if self.parse_keywords(&[Keyword::OWNED, Keyword::BY]) {
            if self.parse_keywords(&[Keyword::NONE]) {
                Some(ObjectName(vec![Ident::new("NONE")]))
            } else {
                Some(self.parse_object_name()?)
            }
        } else {
            None
        };
        Ok(Statement::CreateSequence {
            temporary,
            if_not_exists,
            name,
            data_type,
            sequence_options,
            owned_by,
        })
    }

    fn parse_create_sequence_options(&mut self) -> Result<Vec<SequenceOptions>, ParserError> {
        let mut sequence_options = vec![];
        //[ INCREMENT [ BY ] increment ]
        if self.parse_keywords(&[Keyword::INCREMENT]) {
            if self.parse_keywords(&[Keyword::BY]) {
                sequence_options.push(SequenceOptions::IncrementBy(
                    Expr::Value(self.parse_number_value()?),
                    true,
                ));
            } else {
                sequence_options.push(SequenceOptions::IncrementBy(
                    Expr::Value(self.parse_number_value()?),
                    false,
                ));
            }
        }
        //[ MINVALUE minvalue | NO MINVALUE ]
        if self.parse_keyword(Keyword::MINVALUE) {
            sequence_options.push(SequenceOptions::MinValue(MinMaxValue::Some(Expr::Value(
                self.parse_number_value()?,
            ))));
        } else if self.parse_keywords(&[Keyword::NO, Keyword::MINVALUE]) {
            sequence_options.push(SequenceOptions::MinValue(MinMaxValue::None));
        } else {
            sequence_options.push(SequenceOptions::MinValue(MinMaxValue::Empty));
        }
        //[ MAXVALUE maxvalue | NO MAXVALUE ]
        if self.parse_keywords(&[Keyword::MAXVALUE]) {
            sequence_options.push(SequenceOptions::MaxValue(MinMaxValue::Some(Expr::Value(
                self.parse_number_value()?,
            ))));
        } else if self.parse_keywords(&[Keyword::NO, Keyword::MAXVALUE]) {
            sequence_options.push(SequenceOptions::MaxValue(MinMaxValue::None));
        } else {
            sequence_options.push(SequenceOptions::MaxValue(MinMaxValue::Empty));
        }
        //[ START [ WITH ] start ]
        if self.parse_keywords(&[Keyword::START]) {
            if self.parse_keywords(&[Keyword::WITH]) {
                sequence_options.push(SequenceOptions::StartWith(
                    Expr::Value(self.parse_number_value()?),
                    true,
                ));
            } else {
                sequence_options.push(SequenceOptions::StartWith(
                    Expr::Value(self.parse_number_value()?),
                    false,
                ));
            }
        }
        //[ CACHE cache ]
        if self.parse_keywords(&[Keyword::CACHE]) {
            sequence_options.push(SequenceOptions::Cache(Expr::Value(
                self.parse_number_value()?,
            )));
        }
        // [ [ NO ] CYCLE ]
        if self.parse_keywords(&[Keyword::NO]) {
            if self.parse_keywords(&[Keyword::CYCLE]) {
                sequence_options.push(SequenceOptions::Cycle(true));
            }
        } else if self.parse_keywords(&[Keyword::CYCLE]) {
            sequence_options.push(SequenceOptions::Cycle(false));
        }
        Ok(sequence_options)
    }

    /// The index of the first unprocessed token.
    pub fn index(&self) -> usize {
        self.index
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

#[cfg(test)]
mod tests {
    use crate::test_utils::{all_dialects, TestedDialects};

    use super::*;

    #[test]
    fn test_prev_index() {
        let sql = "SELECT version";
        all_dialects().run_parser_method(sql, |parser| {
            assert_eq!(parser.peek_token(), Token::make_keyword("SELECT"));
            assert_eq!(parser.next_token(), Token::make_keyword("SELECT"));
            parser.prev_token();
            assert_eq!(parser.next_token(), Token::make_keyword("SELECT"));
            assert_eq!(parser.next_token(), Token::make_word("version", None));
            parser.prev_token();
            assert_eq!(parser.peek_token(), Token::make_word("version", None));
            assert_eq!(parser.next_token(), Token::make_word("version", None));
            assert_eq!(parser.peek_token(), Token::EOF);
            parser.prev_token();
            assert_eq!(parser.next_token(), Token::make_word("version", None));
            assert_eq!(parser.next_token(), Token::EOF);
            assert_eq!(parser.next_token(), Token::EOF);
            parser.prev_token();
        });
    }

    #[test]
    fn test_parse_limit() {
        let sql = "SELECT * FROM user LIMIT 1";
        all_dialects().run_parser_method(sql, |parser| {
            let ast = parser.parse_query().unwrap();
            assert_eq!(ast.to_string(), sql.to_string());
        });

        let sql = "SELECT * FROM user LIMIT $1 OFFSET $2";
        let dialects = TestedDialects {
            dialects: vec![
                Box::new(PostgreSqlDialect {}),
                Box::new(ClickHouseDialect {}),
                Box::new(GenericDialect {}),
                Box::new(MsSqlDialect {}),
                Box::new(SnowflakeDialect {}),
            ],
        };

        dialects.run_parser_method(sql, |parser| {
            let ast = parser.parse_query().unwrap();
            assert_eq!(ast.to_string(), sql.to_string());
        });

        let sql = "SELECT * FROM user LIMIT ? OFFSET ?";
        let dialects = TestedDialects {
            dialects: vec![Box::new(MySqlDialect {})],
        };
        dialects.run_parser_method(sql, |parser| {
            let ast = parser.parse_query().unwrap();
            assert_eq!(ast.to_string(), sql.to_string());
        });
    }

    #[cfg(test)]
    mod test_parse_data_type {
        use crate::ast::{
            CharLengthUnits, CharacterLength, DataType, ExactNumberInfo, ObjectName, TimezoneInfo,
        };
        use crate::dialect::{AnsiDialect, GenericDialect};
        use crate::test_utils::TestedDialects;

        macro_rules! test_parse_data_type {
            ($dialect:expr, $input:expr, $expected_type:expr $(,)?) => {{
                $dialect.run_parser_method(&*$input, |parser| {
                    let data_type = parser.parse_data_type().unwrap();
                    assert_eq!($expected_type, data_type);
                    assert_eq!($input.to_string(), data_type.to_string());
                });
            }};
        }

        #[test]
        fn test_ansii_character_string_types() {
            // Character string types: <https://jakewheat.github.io/sql-overview/sql-2016-foundation-grammar.html#character-string-type>
            let dialect = TestedDialects {
                dialects: vec![Box::new(GenericDialect {}), Box::new(AnsiDialect {})],
            };

            test_parse_data_type!(dialect, "CHARACTER", DataType::Character(None));

            test_parse_data_type!(
                dialect,
                "CHARACTER(20)",
                DataType::Character(Some(CharacterLength {
                    length: 20,
                    unit: None
                }))
            );

            test_parse_data_type!(
                dialect,
                "CHARACTER(20 CHARACTERS)",
                DataType::Character(Some(CharacterLength {
                    length: 20,
                    unit: Some(CharLengthUnits::Characters)
                }))
            );

            test_parse_data_type!(
                dialect,
                "CHARACTER(20 OCTETS)",
                DataType::Character(Some(CharacterLength {
                    length: 20,
                    unit: Some(CharLengthUnits::Octets)
                }))
            );

            test_parse_data_type!(dialect, "CHAR", DataType::Char(None));

            test_parse_data_type!(
                dialect,
                "CHAR(20)",
                DataType::Char(Some(CharacterLength {
                    length: 20,
                    unit: None
                }))
            );

            test_parse_data_type!(
                dialect,
                "CHAR(20 CHARACTERS)",
                DataType::Char(Some(CharacterLength {
                    length: 20,
                    unit: Some(CharLengthUnits::Characters)
                }))
            );

            test_parse_data_type!(
                dialect,
                "CHAR(20 OCTETS)",
                DataType::Char(Some(CharacterLength {
                    length: 20,
                    unit: Some(CharLengthUnits::Octets)
                }))
            );

            test_parse_data_type!(
                dialect,
                "CHARACTER VARYING(20)",
                DataType::CharacterVarying(Some(CharacterLength {
                    length: 20,
                    unit: None
                }))
            );

            test_parse_data_type!(
                dialect,
                "CHARACTER VARYING(20 CHARACTERS)",
                DataType::CharacterVarying(Some(CharacterLength {
                    length: 20,
                    unit: Some(CharLengthUnits::Characters)
                }))
            );

            test_parse_data_type!(
                dialect,
                "CHARACTER VARYING(20 OCTETS)",
                DataType::CharacterVarying(Some(CharacterLength {
                    length: 20,
                    unit: Some(CharLengthUnits::Octets)
                }))
            );

            test_parse_data_type!(
                dialect,
                "CHAR VARYING(20)",
                DataType::CharVarying(Some(CharacterLength {
                    length: 20,
                    unit: None
                }))
            );

            test_parse_data_type!(
                dialect,
                "CHAR VARYING(20 CHARACTERS)",
                DataType::CharVarying(Some(CharacterLength {
                    length: 20,
                    unit: Some(CharLengthUnits::Characters)
                }))
            );

            test_parse_data_type!(
                dialect,
                "CHAR VARYING(20 OCTETS)",
                DataType::CharVarying(Some(CharacterLength {
                    length: 20,
                    unit: Some(CharLengthUnits::Octets)
                }))
            );

            test_parse_data_type!(
                dialect,
                "VARCHAR(20)",
                DataType::Varchar(Some(CharacterLength {
                    length: 20,
                    unit: None
                }))
            );
        }

        #[test]
        fn test_ansii_character_large_object_types() {
            // Character large object types: <https://jakewheat.github.io/sql-overview/sql-2016-foundation-grammar.html#character-large-object-length>
            let dialect = TestedDialects {
                dialects: vec![Box::new(GenericDialect {}), Box::new(AnsiDialect {})],
            };

            test_parse_data_type!(
                dialect,
                "CHARACTER LARGE OBJECT",
                DataType::CharacterLargeObject(None)
            );
            test_parse_data_type!(
                dialect,
                "CHARACTER LARGE OBJECT(20)",
                DataType::CharacterLargeObject(Some(20))
            );

            test_parse_data_type!(
                dialect,
                "CHAR LARGE OBJECT",
                DataType::CharLargeObject(None)
            );
            test_parse_data_type!(
                dialect,
                "CHAR LARGE OBJECT(20)",
                DataType::CharLargeObject(Some(20))
            );

            test_parse_data_type!(dialect, "CLOB", DataType::Clob(None));
            test_parse_data_type!(dialect, "CLOB(20)", DataType::Clob(Some(20)));
        }

        #[test]
        fn test_parse_custom_types() {
            let dialect = TestedDialects {
                dialects: vec![Box::new(GenericDialect {}), Box::new(AnsiDialect {})],
            };
            test_parse_data_type!(
                dialect,
                "GEOMETRY",
                DataType::Custom(ObjectName(vec!["GEOMETRY".into()]), vec![])
            );

            test_parse_data_type!(
                dialect,
                "GEOMETRY(POINT)",
                DataType::Custom(
                    ObjectName(vec!["GEOMETRY".into()]),
                    vec!["POINT".to_string()]
                )
            );

            test_parse_data_type!(
                dialect,
                "GEOMETRY(POINT, 4326)",
                DataType::Custom(
                    ObjectName(vec!["GEOMETRY".into()]),
                    vec!["POINT".to_string(), "4326".to_string()]
                )
            );
        }

        #[test]
        fn test_ansii_exact_numeric_types() {
            // Exact numeric types: <https://jakewheat.github.io/sql-overview/sql-2016-foundation-grammar.html#exact-numeric-type>
            let dialect = TestedDialects {
                dialects: vec![Box::new(GenericDialect {}), Box::new(AnsiDialect {})],
            };

            test_parse_data_type!(dialect, "NUMERIC", DataType::Numeric(ExactNumberInfo::None));

            test_parse_data_type!(
                dialect,
                "NUMERIC(2)",
                DataType::Numeric(ExactNumberInfo::Precision(2))
            );

            test_parse_data_type!(
                dialect,
                "NUMERIC(2,10)",
                DataType::Numeric(ExactNumberInfo::PrecisionAndScale(2, 10))
            );

            test_parse_data_type!(dialect, "DECIMAL", DataType::Decimal(ExactNumberInfo::None));

            test_parse_data_type!(
                dialect,
                "DECIMAL(2)",
                DataType::Decimal(ExactNumberInfo::Precision(2))
            );

            test_parse_data_type!(
                dialect,
                "DECIMAL(2,10)",
                DataType::Decimal(ExactNumberInfo::PrecisionAndScale(2, 10))
            );

            test_parse_data_type!(dialect, "DEC", DataType::Dec(ExactNumberInfo::None));

            test_parse_data_type!(
                dialect,
                "DEC(2)",
                DataType::Dec(ExactNumberInfo::Precision(2))
            );

            test_parse_data_type!(
                dialect,
                "DEC(2,10)",
                DataType::Dec(ExactNumberInfo::PrecisionAndScale(2, 10))
            );
        }

        #[test]
        fn test_ansii_date_type() {
            // Datetime types: <https://jakewheat.github.io/sql-overview/sql-2016-foundation-grammar.html#datetime-type>
            let dialect = TestedDialects {
                dialects: vec![Box::new(GenericDialect {}), Box::new(AnsiDialect {})],
            };

            test_parse_data_type!(dialect, "DATE", DataType::Date);

            test_parse_data_type!(dialect, "TIME", DataType::Time(None, TimezoneInfo::None));

            test_parse_data_type!(
                dialect,
                "TIME(6)",
                DataType::Time(Some(6), TimezoneInfo::None)
            );

            test_parse_data_type!(
                dialect,
                "TIME WITH TIME ZONE",
                DataType::Time(None, TimezoneInfo::WithTimeZone)
            );

            test_parse_data_type!(
                dialect,
                "TIME(6) WITH TIME ZONE",
                DataType::Time(Some(6), TimezoneInfo::WithTimeZone)
            );

            test_parse_data_type!(
                dialect,
                "TIME WITHOUT TIME ZONE",
                DataType::Time(None, TimezoneInfo::WithoutTimeZone)
            );

            test_parse_data_type!(
                dialect,
                "TIME(6) WITHOUT TIME ZONE",
                DataType::Time(Some(6), TimezoneInfo::WithoutTimeZone)
            );

            test_parse_data_type!(
                dialect,
                "TIMESTAMP",
                DataType::Timestamp(None, TimezoneInfo::None)
            );

            test_parse_data_type!(
                dialect,
                "TIMESTAMP(22)",
                DataType::Timestamp(Some(22), TimezoneInfo::None)
            );

            test_parse_data_type!(
                dialect,
                "TIMESTAMP(22) WITH TIME ZONE",
                DataType::Timestamp(Some(22), TimezoneInfo::WithTimeZone)
            );

            test_parse_data_type!(
                dialect,
                "TIMESTAMP(33) WITHOUT TIME ZONE",
                DataType::Timestamp(Some(33), TimezoneInfo::WithoutTimeZone)
            );
        }
    }

    #[test]
    fn test_parse_schema_name() {
        // The expected name should be identical as the input name, that's why I don't receive both
        macro_rules! test_parse_schema_name {
            ($input:expr, $expected_name:expr $(,)?) => {{
                all_dialects().run_parser_method(&*$input, |parser| {
                    let schema_name = parser.parse_schema_name().unwrap();
                    // Validate that the structure is the same as expected
                    assert_eq!(schema_name, $expected_name);
                    // Validate that the input and the expected structure serialization are the same
                    assert_eq!(schema_name.to_string(), $input.to_string());
                });
            }};
        }

        let dummy_name = ObjectName(vec![Ident::new("dummy_name")]);
        let dummy_authorization = Ident::new("dummy_authorization");

        test_parse_schema_name!(
            format!("{dummy_name}"),
            SchemaName::Simple(dummy_name.clone())
        );

        test_parse_schema_name!(
            format!("AUTHORIZATION {dummy_authorization}"),
            SchemaName::UnnamedAuthorization(dummy_authorization.clone()),
        );
        test_parse_schema_name!(
            format!("{dummy_name} AUTHORIZATION {dummy_authorization}"),
            SchemaName::NamedAuthorization(dummy_name.clone(), dummy_authorization.clone()),
        );
    }

    #[test]
    fn mysql_parse_index_table_constraint() {
        macro_rules! test_parse_table_constraint {
            ($dialect:expr, $input:expr, $expected:expr $(,)?) => {{
                $dialect.run_parser_method(&*$input, |parser| {
                    let constraint = parser.parse_optional_table_constraint().unwrap().unwrap();
                    // Validate that the structure is the same as expected
                    assert_eq!(constraint, $expected);
                    // Validate that the input and the expected structure serialization are the same
                    assert_eq!(constraint.to_string(), $input.to_string());
                });
            }};
        }

        let dialect = TestedDialects {
            dialects: vec![Box::new(GenericDialect {}), Box::new(MySqlDialect {})],
        };

        test_parse_table_constraint!(
            dialect,
            "INDEX (c1)",
            TableConstraint::Index {
                display_as_key: false,
                name: None,
                index_type: None,
                columns: vec![Ident::new("c1")],
            }
        );

        test_parse_table_constraint!(
            dialect,
            "KEY (c1)",
            TableConstraint::Index {
                display_as_key: true,
                name: None,
                index_type: None,
                columns: vec![Ident::new("c1")],
            }
        );

        test_parse_table_constraint!(
            dialect,
            "INDEX 'index' (c1, c2)",
            TableConstraint::Index {
                display_as_key: false,
                name: Some(Ident::with_quote('\'', "index")),
                index_type: None,
                columns: vec![Ident::new("c1"), Ident::new("c2")],
            }
        );

        test_parse_table_constraint!(
            dialect,
            "INDEX USING BTREE (c1)",
            TableConstraint::Index {
                display_as_key: false,
                name: None,
                index_type: Some(IndexType::BTree),
                columns: vec![Ident::new("c1")],
            }
        );

        test_parse_table_constraint!(
            dialect,
            "INDEX USING HASH (c1)",
            TableConstraint::Index {
                display_as_key: false,
                name: None,
                index_type: Some(IndexType::Hash),
                columns: vec![Ident::new("c1")],
            }
        );

        test_parse_table_constraint!(
            dialect,
            "INDEX idx_name USING BTREE (c1)",
            TableConstraint::Index {
                display_as_key: false,
                name: Some(Ident::new("idx_name")),
                index_type: Some(IndexType::BTree),
                columns: vec![Ident::new("c1")],
            }
        );

        test_parse_table_constraint!(
            dialect,
            "INDEX idx_name USING HASH (c1)",
            TableConstraint::Index {
                display_as_key: false,
                name: Some(Ident::new("idx_name")),
                index_type: Some(IndexType::Hash),
                columns: vec![Ident::new("c1")],
            }
        );
    }

    #[test]
    fn test_update_has_keyword() {
        let sql = r#"UPDATE test SET name=$1,
                value=$2,
                where=$3,
                create=$4,
                is_default=$5,
                classification=$6,
                sort=$7
                WHERE id=$8"#;
        let pg_dialect = PostgreSqlDialect {};
        let ast = Parser::parse_sql(&pg_dialect, sql).unwrap();
        assert_eq!(
            ast[0].to_string(),
            r#"UPDATE test SET name = $1, value = $2, where = $3, create = $4, is_default = $5, classification = $6, sort = $7 WHERE id = $8"#
        );
    }

    #[test]
    fn test_tokenizer_error_loc() {
        let sql = "foo '";
        let ast = Parser::parse_sql(&GenericDialect, sql);
        assert_eq!(
            ast,
            Err(ParserError::TokenizerError(
                "Unterminated string literal at Line: 1, Column 5".to_string()
            ))
        );
    }

    #[test]
    fn test_parser_error_loc() {
        // TODO: Once we thread token locations through the parser, we should update this
        // test to assert the locations of the referenced token
        let sql = "SELECT this is a syntax error";
        let ast = Parser::parse_sql(&GenericDialect, sql);
        assert_eq!(
            ast,
            Err(ParserError::ParserError(
                "Expected [NOT] NULL or TRUE|FALSE or [NOT] DISTINCT FROM after IS, found: a"
                    .to_string()
            ))
        );
    }

    #[test]
    fn test_nested_explain_error() {
        let sql = "EXPLAIN EXPLAIN SELECT 1";
        let ast = Parser::parse_sql(&GenericDialect, sql);
        assert_eq!(
            ast,
            Err(ParserError::ParserError(
                "Explain must be root of the plan".to_string()
            ))
        );
    }
}

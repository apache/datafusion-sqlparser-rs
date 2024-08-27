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
use core::{
    fmt::{self, Display},
    str::FromStr,
};

use log::debug;

use recursion::RecursionCounter;
use IsLateral::*;
use IsOptional::*;

use crate::ast::helpers::stmt_create_table::{CreateTableBuilder, CreateTableConfiguration};
use crate::ast::*;
use crate::dialect::*;
use crate::keywords::{Keyword, ALL_KEYWORDS};
use crate::tokenizer::*;

mod alter;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParserError {
    TokenizerError(String),
    ParserError(String),
    RecursionLimitExceeded,
}

// avoid clippy type_complexity warnings
type ParsedAction = (Keyword, Option<Vec<Ident>>);

// Use `Parser::expected` instead, if possible
macro_rules! parser_err {
    ($MSG:expr, $loc:expr) => {
        Err(ParserError::ParserError(format!("{}{}", $MSG, $loc)))
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
/// Implementation [`RecursionCounter`] if std is available
mod recursion {
    use std::cell::Cell;
    use std::rc::Rc;

    use super::ParserError;

    /// Tracks remaining recursion depth. This value is decremented on
    /// each call to [`RecursionCounter::try_decrease()`], when it reaches 0 an error will
    /// be returned.
    ///
    /// Note: Uses an [`std::rc::Rc`] and [`std::cell::Cell`] in order to satisfy the Rust
    /// borrow checker so the automatic [`DepthGuard`] decrement a
    /// reference to the counter.
    pub(crate) struct RecursionCounter {
        remaining_depth: Rc<Cell<usize>>,
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
        /// Returns [`Err`] if the remaining depth falls to 0.
        ///
        /// Returns a [`DepthGuard`] which will adds 1 to the
        /// remaining depth upon drop;
        pub fn try_decrease(&self) -> Result<DepthGuard, ParserError> {
            let old_value = self.remaining_depth.get();
            // ran out of space
            if old_value == 0 {
                Err(ParserError::RecursionLimitExceeded)
            } else {
                self.remaining_depth.set(old_value - 1);
                Ok(DepthGuard::new(Rc::clone(&self.remaining_depth)))
            }
        }
    }

    /// Guard that increases the remaining depth by 1 on drop
    pub struct DepthGuard {
        remaining_depth: Rc<Cell<usize>>,
    }

    impl DepthGuard {
        fn new(remaining_depth: Rc<Cell<usize>>) -> Self {
            Self { remaining_depth }
        }
    }
    impl Drop for DepthGuard {
        fn drop(&mut self) {
            let old_value = self.remaining_depth.get();
            self.remaining_depth.set(old_value + 1);
        }
    }
}

#[cfg(not(feature = "std"))]
mod recursion {
    /// Implementation [`RecursionCounter`] if std is NOT available (and does not
    /// guard against stack overflow).
    ///
    /// Has the same API as the std [`RecursionCounter`] implementation
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

/// Composite types declarations using angle brackets syntax can be arbitrary
/// nested such that the following declaration is possible:
///      `ARRAY<ARRAY<INT>>`
/// But the tokenizer recognizes the `>>` as a ShiftRight token.
/// We work around that limitation when parsing a data type by accepting
/// either a `>` or `>>` token in such cases, remembering which variant we
/// matched.
/// In the latter case having matched a `>>`, the parent type will not look to
/// match its closing `>` as a result since that will have taken place at the
/// child type.
///
/// See [Parser::parse_data_type] for details
struct MatchedTrailingBracket(bool);

impl From<bool> for MatchedTrailingBracket {
    fn from(value: bool) -> Self {
        Self(value)
    }
}

/// Options that control how the [`Parser`] parses SQL text
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParserOptions {
    pub trailing_commas: bool,
    /// Controls how literal values are unescaped. See
    /// [`Tokenizer::with_unescape`] for more details.
    pub unescape: bool,
}

impl Default for ParserOptions {
    fn default() -> Self {
        Self {
            trailing_commas: false,
            unescape: true,
        }
    }
}

impl ParserOptions {
    /// Create a new [`ParserOptions`]
    pub fn new() -> Self {
        Default::default()
    }

    /// Set if trailing commas are allowed.
    ///
    /// If this option is `false` (the default), the following SQL will
    /// not parse. If the option is `true`, the SQL will parse.
    ///
    /// ```sql
    ///  SELECT
    ///   foo,
    ///   bar,
    ///  FROM baz
    /// ```
    pub fn with_trailing_commas(mut self, trailing_commas: bool) -> Self {
        self.trailing_commas = trailing_commas;
        self
    }

    /// Set if literal values are unescaped. Defaults to true. See
    /// [`Tokenizer::with_unescape`] for more details.
    pub fn with_unescape(mut self, unescape: bool) -> Self {
        self.unescape = unescape;
        self
    }
}

#[derive(Copy, Clone)]
enum ParserState {
    /// The default state of the parser.
    Normal,
    /// The state when parsing a CONNECT BY expression. This allows parsing
    /// PRIOR expressions while still allowing prior as an identifier name
    /// in other contexts.
    ConnectBy,
}

pub struct Parser<'a> {
    tokens: Vec<TokenWithLocation>,
    /// The index of the first unprocessed token in [`Parser::tokens`].
    index: usize,
    /// The current state of the parser.
    state: ParserState,
    /// The current dialect to use.
    dialect: &'a dyn Dialect,
    /// Additional options that allow you to mix & match behavior
    /// otherwise constrained to certain dialects (e.g. trailing
    /// commas) and/or format of parse (e.g. unescaping).
    options: ParserOptions,
    /// Ensure the stack does not overflow by limiting recursion depth.
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
            state: ParserState::Normal,
            dialect,
            recursion_counter: RecursionCounter::new(DEFAULT_REMAINING_DEPTH),
            options: ParserOptions::new().with_trailing_commas(dialect.supports_trailing_commas()),
        }
    }

    /// Specify the maximum recursion limit while parsing.
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

    /// Specify additional parser options
    ///
    /// [`Parser`] supports additional options ([`ParserOptions`])
    /// that allow you to mix & match behavior otherwise constrained
    /// to certain dialects (e.g. trailing commas).
    ///
    /// Example:
    /// ```
    /// # use sqlparser::{parser::{Parser, ParserError, ParserOptions}, dialect::GenericDialect};
    /// # fn main() -> Result<(), ParserError> {
    /// let dialect = GenericDialect{};
    /// let options = ParserOptions::new()
    ///    .with_trailing_commas(true)
    ///    .with_unescape(false);
    /// let result = Parser::new(&dialect)
    ///   .with_options(options)
    ///   .try_with_sql("SELECT a, b, COUNT(*), FROM foo GROUP BY a, b,")?
    ///   .parse_statements();
    ///   assert!(matches!(result, Ok(_)));
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_options(mut self, options: ParserOptions) -> Self {
        self.options = options;
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
        let tokens = Tokenizer::new(self.dialect, sql)
            .with_unescape(self.options.unescape)
            .tokenize_with_location()?;
        Ok(self.with_tokens_with_locations(tokens))
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

            match self.peek_token().token {
                Token::EOF => break,

                // end of statement
                Token::Word(word) => {
                    if expecting_statement_delimiter && word.keyword == Keyword::END {
                        break;
                    }
                }
                _ => {}
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

    /// Convenience method to parse a string with one or more SQL
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
                Keyword::FLUSH => Ok(self.parse_flush()?),
                Keyword::DESC => Ok(self.parse_explain(DescribeAlias::Desc)?),
                Keyword::DESCRIBE => Ok(self.parse_explain(DescribeAlias::Describe)?),
                Keyword::EXPLAIN => Ok(self.parse_explain(DescribeAlias::Explain)?),
                Keyword::ANALYZE => Ok(self.parse_analyze()?),
                Keyword::SELECT | Keyword::WITH | Keyword::VALUES => {
                    self.prev_token();
                    Ok(Statement::Query(self.parse_boxed_query()?))
                }
                Keyword::TRUNCATE => Ok(self.parse_truncate()?),
                Keyword::ATTACH => {
                    if dialect_of!(self is DuckDbDialect) {
                        Ok(self.parse_attach_duckdb_database()?)
                    } else {
                        Ok(self.parse_attach_database()?)
                    }
                }
                Keyword::DETACH if dialect_of!(self is DuckDbDialect | GenericDialect) => {
                    Ok(self.parse_detach_duckdb_database()?)
                }
                Keyword::MSCK => Ok(self.parse_msck()?),
                Keyword::CREATE => Ok(self.parse_create()?),
                Keyword::CACHE => Ok(self.parse_cache_table()?),
                Keyword::DROP => Ok(self.parse_drop()?),
                Keyword::DISCARD => Ok(self.parse_discard()?),
                Keyword::DECLARE => Ok(self.parse_declare()?),
                Keyword::FETCH => Ok(self.parse_fetch_statement()?),
                Keyword::DELETE => Ok(self.parse_delete()?),
                Keyword::INSERT => Ok(self.parse_insert()?),
                Keyword::REPLACE => Ok(self.parse_replace()?),
                Keyword::UNCACHE => Ok(self.parse_uncache_table()?),
                Keyword::UPDATE => Ok(self.parse_update()?),
                Keyword::ALTER => Ok(self.parse_alter()?),
                Keyword::CALL => Ok(self.parse_call()?),
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
                // `END` is a nonstandard but common alias for the
                // standard `COMMIT TRANSACTION` statement. It is supported
                // by PostgreSQL.
                Keyword::END => Ok(self.parse_end()?),
                Keyword::SAVEPOINT => Ok(self.parse_savepoint()?),
                Keyword::RELEASE => Ok(self.parse_release()?),
                Keyword::COMMIT => Ok(self.parse_commit()?),
                Keyword::ROLLBACK => Ok(self.parse_rollback()?),
                Keyword::ASSERT => Ok(self.parse_assert()?),
                // `PREPARE`, `EXECUTE` and `DEALLOCATE` are Postgres-specific
                // syntaxes. They are used for Postgres prepared statement.
                Keyword::DEALLOCATE => Ok(self.parse_deallocate()?),
                Keyword::EXECUTE => Ok(self.parse_execute()?),
                Keyword::PREPARE => Ok(self.parse_prepare()?),
                Keyword::MERGE => Ok(self.parse_merge()?),
                // `PRAGMA` is sqlite specific https://www.sqlite.org/pragma.html
                Keyword::PRAGMA => Ok(self.parse_pragma()?),
                Keyword::UNLOAD => Ok(self.parse_unload()?),
                // `INSTALL` is duckdb specific https://duckdb.org/docs/extensions/overview
                Keyword::INSTALL if dialect_of!(self is DuckDbDialect | GenericDialect) => {
                    Ok(self.parse_install()?)
                }
                // `LOAD` is duckdb specific https://duckdb.org/docs/extensions/overview
                Keyword::LOAD if dialect_of!(self is DuckDbDialect | GenericDialect) => {
                    Ok(self.parse_load()?)
                }
                // `OPTIMIZE` is clickhouse specific https://clickhouse.tech/docs/en/sql-reference/statements/optimize/
                Keyword::OPTIMIZE if dialect_of!(self is ClickHouseDialect | GenericDialect) => {
                    Ok(self.parse_optimize_table()?)
                }
                _ => self.expected("an SQL statement", next_token),
            },
            Token::LParen => {
                self.prev_token();
                Ok(Statement::Query(self.parse_boxed_query()?))
            }
            _ => self.expected("an SQL statement", next_token),
        }
    }

    pub fn parse_flush(&mut self) -> Result<Statement, ParserError> {
        let mut channel = None;
        let mut tables: Vec<ObjectName> = vec![];
        let mut read_lock = false;
        let mut export = false;

        if !dialect_of!(self is MySqlDialect | GenericDialect) {
            return parser_err!("Unsupported statement FLUSH", self.peek_token().location);
        }

        let location = if self.parse_keyword(Keyword::NO_WRITE_TO_BINLOG) {
            Some(FlushLocation::NoWriteToBinlog)
        } else if self.parse_keyword(Keyword::LOCAL) {
            Some(FlushLocation::Local)
        } else {
            None
        };

        let object_type = if self.parse_keywords(&[Keyword::BINARY, Keyword::LOGS]) {
            FlushType::BinaryLogs
        } else if self.parse_keywords(&[Keyword::ENGINE, Keyword::LOGS]) {
            FlushType::EngineLogs
        } else if self.parse_keywords(&[Keyword::ERROR, Keyword::LOGS]) {
            FlushType::ErrorLogs
        } else if self.parse_keywords(&[Keyword::GENERAL, Keyword::LOGS]) {
            FlushType::GeneralLogs
        } else if self.parse_keywords(&[Keyword::HOSTS]) {
            FlushType::Hosts
        } else if self.parse_keyword(Keyword::PRIVILEGES) {
            FlushType::Privileges
        } else if self.parse_keyword(Keyword::OPTIMIZER_COSTS) {
            FlushType::OptimizerCosts
        } else if self.parse_keywords(&[Keyword::RELAY, Keyword::LOGS]) {
            if self.parse_keywords(&[Keyword::FOR, Keyword::CHANNEL]) {
                channel = Some(self.parse_object_name(false).unwrap().to_string());
            }
            FlushType::RelayLogs
        } else if self.parse_keywords(&[Keyword::SLOW, Keyword::LOGS]) {
            FlushType::SlowLogs
        } else if self.parse_keyword(Keyword::STATUS) {
            FlushType::Status
        } else if self.parse_keyword(Keyword::USER_RESOURCES) {
            FlushType::UserResources
        } else if self.parse_keywords(&[Keyword::LOGS]) {
            FlushType::Logs
        } else if self.parse_keywords(&[Keyword::TABLES]) {
            loop {
                let next_token = self.next_token();
                match &next_token.token {
                    Token::Word(w) => match w.keyword {
                        Keyword::WITH => {
                            read_lock = self.parse_keywords(&[Keyword::READ, Keyword::LOCK]);
                        }
                        Keyword::FOR => {
                            export = self.parse_keyword(Keyword::EXPORT);
                        }
                        Keyword::NoKeyword => {
                            self.prev_token();
                            tables = self.parse_comma_separated(|p| p.parse_object_name(false))?;
                        }
                        _ => {}
                    },
                    _ => {
                        break;
                    }
                }
            }

            FlushType::Tables
        } else {
            return self.expected(
                "BINARY LOGS, ENGINE LOGS, ERROR LOGS, GENERAL LOGS, HOSTS, LOGS, PRIVILEGES, OPTIMIZER_COSTS,\
                 RELAY LOGS [FOR CHANNEL channel], SLOW LOGS, STATUS, USER_RESOURCES",
                self.peek_token(),
            );
        };

        Ok(Statement::Flush {
            object_type,
            location,
            channel,
            read_lock,
            export,
            tables,
        })
    }

    pub fn parse_msck(&mut self) -> Result<Statement, ParserError> {
        let repair = self.parse_keyword(Keyword::REPAIR);
        self.expect_keyword(Keyword::TABLE)?;
        let table_name = self.parse_object_name(false)?;
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
        let table = self.parse_keyword(Keyword::TABLE);
        let only = self.parse_keyword(Keyword::ONLY);

        let table_names = self.parse_comma_separated(|p| p.parse_object_name(false))?;

        // Unwrap is safe - the preceding parse  fails if there is not at least one table name
        let table_name = table_names.first().unwrap().clone();

        let mut partitions = None;
        if self.parse_keyword(Keyword::PARTITION) {
            self.expect_token(&Token::LParen)?;
            partitions = Some(self.parse_comma_separated(Parser::parse_expr)?);
            self.expect_token(&Token::RParen)?;
        }

        let identity = if self.parse_keywords(&[Keyword::RESTART, Keyword::IDENTITY]) {
            Some(TruncateIdentityOption::Restart)
        } else if self.parse_keywords(&[Keyword::CONTINUE, Keyword::IDENTITY]) {
            Some(TruncateIdentityOption::Continue)
        } else {
            None
        };

        let cascade = if self.parse_keyword(Keyword::CASCADE) {
            Some(TruncateCascadeOption::Cascade)
        } else if self.parse_keyword(Keyword::RESTRICT) {
            Some(TruncateCascadeOption::Restrict)
        } else {
            None
        };

        Ok(Statement::Truncate {
            table_name,
            table_names,
            partitions,
            table,
            only,
            identity,
            cascade,
        })
    }

    pub fn parse_attach_duckdb_database_options(
        &mut self,
    ) -> Result<Vec<AttachDuckDBDatabaseOption>, ParserError> {
        if !self.consume_token(&Token::LParen) {
            return Ok(vec![]);
        }

        let mut options = vec![];
        loop {
            if self.parse_keyword(Keyword::READ_ONLY) {
                let boolean = if self.parse_keyword(Keyword::TRUE) {
                    Some(true)
                } else if self.parse_keyword(Keyword::FALSE) {
                    Some(false)
                } else {
                    None
                };
                options.push(AttachDuckDBDatabaseOption::ReadOnly(boolean));
            } else if self.parse_keyword(Keyword::TYPE) {
                let ident = self.parse_identifier(false)?;
                options.push(AttachDuckDBDatabaseOption::Type(ident));
            } else {
                return self.expected("expected one of: ), READ_ONLY, TYPE", self.peek_token());
            };

            if self.consume_token(&Token::RParen) {
                return Ok(options);
            } else if self.consume_token(&Token::Comma) {
                continue;
            } else {
                return self.expected("expected one of: ')', ','", self.peek_token());
            }
        }
    }

    pub fn parse_attach_duckdb_database(&mut self) -> Result<Statement, ParserError> {
        let database = self.parse_keyword(Keyword::DATABASE);
        let if_not_exists = self.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let database_path = self.parse_identifier(false)?;
        let database_alias = if self.parse_keyword(Keyword::AS) {
            Some(self.parse_identifier(false)?)
        } else {
            None
        };

        let attach_options = self.parse_attach_duckdb_database_options()?;
        Ok(Statement::AttachDuckDBDatabase {
            if_not_exists,
            database,
            database_path,
            database_alias,
            attach_options,
        })
    }

    pub fn parse_detach_duckdb_database(&mut self) -> Result<Statement, ParserError> {
        let database = self.parse_keyword(Keyword::DATABASE);
        let if_exists = self.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
        let database_alias = self.parse_identifier(false)?;
        Ok(Statement::DetachDuckDBDatabase {
            if_exists,
            database,
            database_alias,
        })
    }

    pub fn parse_attach_database(&mut self) -> Result<Statement, ParserError> {
        let database = self.parse_keyword(Keyword::DATABASE);
        let database_file_name = self.parse_expr()?;
        self.expect_keyword(Keyword::AS)?;
        let schema_name = self.parse_identifier(false)?;
        Ok(Statement::AttachDatabase {
            database,
            schema_name,
            database_file_name,
        })
    }

    pub fn parse_analyze(&mut self) -> Result<Statement, ParserError> {
        self.expect_keyword(Keyword::TABLE)?;
        let table_name = self.parse_object_name(false)?;
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
                            parser.parse_comma_separated(|p| p.parse_identifier(false))
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

    /// Parse a new expression including wildcard & qualified wildcard.
    pub fn parse_wildcard_expr(&mut self) -> Result<Expr, ParserError> {
        let index = self.index;

        let next_token = self.next_token();
        match next_token.token {
            t @ (Token::Word(_) | Token::SingleQuotedString(_)) => {
                if self.peek_token().token == Token::Period {
                    let mut id_parts: Vec<Ident> = vec![match t {
                        Token::Word(w) => w.to_ident(),
                        Token::SingleQuotedString(s) => Ident::with_quote('\'', s),
                        _ => unreachable!(), // We matched above
                    }];

                    while self.consume_token(&Token::Period) {
                        let next_token = self.next_token();
                        match next_token.token {
                            Token::Word(w) => id_parts.push(w.to_ident()),
                            Token::SingleQuotedString(s) => {
                                // SQLite has single-quoted identifiers
                                id_parts.push(Ident::with_quote('\'', s))
                            }
                            Token::Mul => {
                                return Ok(Expr::QualifiedWildcard(ObjectName(id_parts)));
                            }
                            _ => {
                                return self
                                    .expected("an identifier or a '*' after '.'", next_token);
                            }
                        }
                    }
                }
            }
            Token::Mul => {
                return Ok(Expr::Wildcard);
            }
            _ => (),
        };

        self.index = index;
        self.parse_expr()
    }

    /// Parse a new expression.
    pub fn parse_expr(&mut self) -> Result<Expr, ParserError> {
        let _guard = self.recursion_counter.try_decrease()?;
        self.parse_subexpr(self.dialect.prec_unknown())
    }

    /// Parse tokens until the precedence changes.
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
        let precedence = self.dialect.prec_unknown();
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

    /// Get the precedence of the next token, with AND, OR, and XOR.
    pub fn get_next_interval_precedence(&self) -> Result<u8, ParserError> {
        let token = self.peek_token();

        match token.token {
            Token::Word(w) if w.keyword == Keyword::AND => Ok(self.dialect.prec_unknown()),
            Token::Word(w) if w.keyword == Keyword::OR => Ok(self.dialect.prec_unknown()),
            Token::Word(w) if w.keyword == Keyword::XOR => Ok(self.dialect.prec_unknown()),
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
        let name = self.parse_identifier(false)?;
        Ok(Statement::Savepoint { name })
    }

    pub fn parse_release(&mut self) -> Result<Statement, ParserError> {
        let _ = self.parse_keyword(Keyword::SAVEPOINT);
        let name = self.parse_identifier(false)?;

        Ok(Statement::ReleaseSavepoint { name })
    }

    /// Parse an expression prefix.
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
        let loc = self.peek_token().location;
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
                DataType::Custom(..) => parser_err!("dummy", loc),
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
                        parameters: FunctionArguments::None,
                        args: FunctionArguments::None,
                        null_treatment: None,
                        filter: None,
                        over: None,
                        within_group: vec![],
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
                Keyword::CONVERT => self.parse_convert_expr(),
                Keyword::CAST => self.parse_cast_expr(CastKind::Cast),
                Keyword::TRY_CAST => self.parse_cast_expr(CastKind::TryCast),
                Keyword::SAFE_CAST => self.parse_cast_expr(CastKind::SafeCast),
                Keyword::EXISTS
                    // Support parsing Databricks has a function named `exists`.
                    if !dialect_of!(self is DatabricksDialect)
                        || matches!(
                            self.peek_nth_token(1).token,
                            Token::Word(Word {
                                keyword: Keyword::SELECT | Keyword::WITH,
                                ..
                            })
                        ) =>
                {
                    self.parse_exists_expr(false)
                }
                Keyword::EXTRACT => self.parse_extract_expr(),
                Keyword::CEIL => self.parse_ceil_floor_expr(true),
                Keyword::FLOOR => self.parse_ceil_floor_expr(false),
                Keyword::POSITION if self.peek_token().token == Token::LParen => {
                    self.parse_position_expr(w.to_ident())
                }
                Keyword::SUBSTRING => self.parse_substring_expr(),
                Keyword::OVERLAY => self.parse_overlay_expr(),
                Keyword::TRIM => self.parse_trim_expr(),
                Keyword::INTERVAL => self.parse_interval(),
                // Treat ARRAY[1,2,3] as an array [1,2,3], otherwise try as subquery or a function call
                Keyword::ARRAY if self.peek_token() == Token::LBracket => {
                    self.expect_token(&Token::LBracket)?;
                    self.parse_array_expr(true)
                }
                Keyword::ARRAY
                    if self.peek_token() == Token::LParen
                        && !dialect_of!(self is ClickHouseDialect | DatabricksDialect) =>
                {
                    self.expect_token(&Token::LParen)?;
                    let query = self.parse_boxed_query()?;
                    self.expect_token(&Token::RParen)?;
                    Ok(Expr::Function(Function {
                        name: ObjectName(vec![w.to_ident()]),
                        parameters: FunctionArguments::None,
                        args: FunctionArguments::Subquery(query),
                        filter: None,
                        null_treatment: None,
                        over: None,
                        within_group: vec![],
                    }))
                }
                Keyword::NOT => self.parse_not(),
                Keyword::MATCH if dialect_of!(self is MySqlDialect | GenericDialect) => {
                    self.parse_match_against()
                }
                Keyword::STRUCT if dialect_of!(self is BigQueryDialect | GenericDialect) => {
                    self.prev_token();
                    self.parse_bigquery_struct_literal()
                }
                Keyword::PRIOR if matches!(self.state, ParserState::ConnectBy) => {
                    let expr = self.parse_subexpr(self.dialect.prec_value(Precedence::PlusMinus))?;
                    Ok(Expr::Prior(Box::new(expr)))
                }
                Keyword::MAP if self.peek_token() == Token::LBrace && self.dialect.support_map_literal_syntax() => {
                    self.parse_duckdb_map_literal()
                }
                // Here `w` is a word, check if it's a part of a multipart
                // identifier, a function call, or a simple identifier:
                _ => match self.peek_token().token {
                    Token::LParen | Token::Period => {
                        let mut id_parts: Vec<Ident> = vec![w.to_ident()];
                        let mut ends_with_wildcard = false;
                        while self.consume_token(&Token::Period) {
                            let next_token = self.next_token();
                            match next_token.token {
                                Token::Word(w) => id_parts.push(w.to_ident()),
                                Token::Mul => {
                                    // Postgres explicitly allows funcnm(tablenm.*) and the
                                    // function array_agg traverses this control flow
                                    if dialect_of!(self is PostgreSqlDialect) {
                                        ends_with_wildcard = true;
                                        break;
                                    } else {
                                        return self
                                            .expected("an identifier after '.'", next_token);
                                    }
                                }
                                Token::SingleQuotedString(s) => {
                                    id_parts.push(Ident::with_quote('\'', s))
                                }
                                _ => {
                                    return self
                                        .expected("an identifier or a '*' after '.'", next_token);
                                }
                            }
                        }

                        if ends_with_wildcard {
                            Ok(Expr::QualifiedWildcard(ObjectName(id_parts)))
                        } else if self.consume_token(&Token::LParen) {
                            if dialect_of!(self is SnowflakeDialect | MsSqlDialect)
                                && self.consume_tokens(&[Token::Plus, Token::RParen])
                            {
                                Ok(Expr::OuterJoin(Box::new(
                                    match <[Ident; 1]>::try_from(id_parts) {
                                        Ok([ident]) => Expr::Identifier(ident),
                                        Err(parts) => Expr::CompoundIdentifier(parts),
                                    },
                                )))
                            } else {
                                self.prev_token();
                                self.parse_function(ObjectName(id_parts))
                            }
                        } else {
                            Ok(Expr::CompoundIdentifier(id_parts))
                        }
                    }
                    // string introducer https://dev.mysql.com/doc/refman/8.0/en/charset-introducer.html
                    Token::SingleQuotedString(_)
                    | Token::DoubleQuotedString(_)
                    | Token::HexStringLiteral(_)
                        if w.value.starts_with('_') =>
                    {
                        Ok(Expr::IntroducedString {
                            introducer: w.value,
                            value: self.parse_introduced_string_value()?,
                        })
                    }
                    Token::Arrow if self.dialect.supports_lambda_functions() => {
                        self.expect_token(&Token::Arrow)?;
                        return Ok(Expr::Lambda(LambdaFunction {
                            params: OneOrManyWithParens::One(w.to_ident()),
                            body: Box::new(self.parse_expr()?),
                        }));
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
                    expr: Box::new(
                        self.parse_subexpr(self.dialect.prec_value(Precedence::MulDivModOp))?,
                    ),
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
                    expr: Box::new(
                        self.parse_subexpr(self.dialect.prec_value(Precedence::PlusMinus))?,
                    ),
                })
            }
            Token::EscapedStringLiteral(_) if dialect_of!(self is PostgreSqlDialect | GenericDialect) =>
            {
                self.prev_token();
                Ok(Expr::Value(self.parse_value()?))
            }
            Token::UnicodeStringLiteral(_) => {
                self.prev_token();
                Ok(Expr::Value(self.parse_value()?))
            }
            Token::Number(_, _)
            | Token::SingleQuotedString(_)
            | Token::DoubleQuotedString(_)
            | Token::TripleSingleQuotedString(_)
            | Token::TripleDoubleQuotedString(_)
            | Token::DollarQuotedString(_)
            | Token::SingleQuotedByteStringLiteral(_)
            | Token::DoubleQuotedByteStringLiteral(_)
            | Token::TripleSingleQuotedByteStringLiteral(_)
            | Token::TripleDoubleQuotedByteStringLiteral(_)
            | Token::SingleQuotedRawStringLiteral(_)
            | Token::DoubleQuotedRawStringLiteral(_)
            | Token::TripleSingleQuotedRawStringLiteral(_)
            | Token::TripleDoubleQuotedRawStringLiteral(_)
            | Token::NationalStringLiteral(_)
            | Token::HexStringLiteral(_) => {
                self.prev_token();
                Ok(Expr::Value(self.parse_value()?))
            }
            Token::LParen => {
                let expr = if let Some(expr) = self.try_parse_expr_sub_query()? {
                    expr
                } else if let Some(lambda) = self.try_parse_lambda() {
                    return Ok(lambda);
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
                        _ => {
                            return parser_err!(
                                format!("Expected identifier, found: {tok}"),
                                tok.location
                            )
                        }
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
            Token::LBrace if self.dialect.supports_dictionary_syntax() => {
                self.prev_token();
                self.parse_duckdb_struct_literal()
            }
            _ => self.expected("an expression:", next_token),
        }?;

        if self.parse_keyword(Keyword::COLLATE) {
            Ok(Expr::Collate {
                expr: Box::new(expr),
                collation: self.parse_object_name(false)?,
            })
        } else {
            Ok(expr)
        }
    }

    fn try_parse_expr_sub_query(&mut self) -> Result<Option<Expr>, ParserError> {
        if self
            .parse_one_of_keywords(&[Keyword::SELECT, Keyword::WITH])
            .is_none()
        {
            return Ok(None);
        }
        self.prev_token();

        Ok(Some(Expr::Subquery(self.parse_boxed_query()?)))
    }

    fn try_parse_lambda(&mut self) -> Option<Expr> {
        if !self.dialect.supports_lambda_functions() {
            return None;
        }
        self.maybe_parse(|p| {
            let params = p.parse_comma_separated(|p| p.parse_identifier(false))?;
            p.expect_token(&Token::RParen)?;
            p.expect_token(&Token::Arrow)?;
            let expr = p.parse_expr()?;
            Ok(Expr::Lambda(LambdaFunction {
                params: OneOrManyWithParens::Many(params),
                body: Box::new(expr),
            }))
        })
    }

    pub fn parse_function(&mut self, name: ObjectName) -> Result<Expr, ParserError> {
        self.expect_token(&Token::LParen)?;

        // Snowflake permits a subquery to be passed as an argument without
        // an enclosing set of parens if it's the only argument.
        if dialect_of!(self is SnowflakeDialect)
            && self
                .parse_one_of_keywords(&[Keyword::WITH, Keyword::SELECT])
                .is_some()
        {
            self.prev_token();
            let subquery = self.parse_boxed_query()?;
            self.expect_token(&Token::RParen)?;
            return Ok(Expr::Function(Function {
                name,
                parameters: FunctionArguments::None,
                args: FunctionArguments::Subquery(subquery),
                filter: None,
                null_treatment: None,
                over: None,
                within_group: vec![],
            }));
        }

        let mut args = self.parse_function_argument_list()?;
        let mut parameters = FunctionArguments::None;
        // ClickHouse aggregations support parametric functions like `HISTOGRAM(0.5, 0.6)(x, y)`
        // which (0.5, 0.6) is a parameter to the function.
        if dialect_of!(self is ClickHouseDialect | GenericDialect)
            && self.consume_token(&Token::LParen)
        {
            parameters = FunctionArguments::List(args);
            args = self.parse_function_argument_list()?;
        }

        let within_group = if self.parse_keywords(&[Keyword::WITHIN, Keyword::GROUP]) {
            self.expect_token(&Token::LParen)?;
            self.expect_keywords(&[Keyword::ORDER, Keyword::BY])?;
            let order_by = self.parse_comma_separated(Parser::parse_order_by_expr)?;
            self.expect_token(&Token::RParen)?;
            order_by
        } else {
            vec![]
        };

        let filter = if self.dialect.supports_filter_during_aggregation()
            && self.parse_keyword(Keyword::FILTER)
            && self.consume_token(&Token::LParen)
            && self.parse_keyword(Keyword::WHERE)
        {
            let filter = Some(Box::new(self.parse_expr()?));
            self.expect_token(&Token::RParen)?;
            filter
        } else {
            None
        };

        // Syntax for null treatment shows up either in the args list
        // or after the function call, but not both.
        let null_treatment = if args
            .clauses
            .iter()
            .all(|clause| !matches!(clause, FunctionArgumentClause::IgnoreOrRespectNulls(_)))
        {
            self.parse_null_treatment()?
        } else {
            None
        };

        let over = if self.parse_keyword(Keyword::OVER) {
            if self.consume_token(&Token::LParen) {
                let window_spec = self.parse_window_spec()?;
                Some(WindowType::WindowSpec(window_spec))
            } else {
                Some(WindowType::NamedWindow(self.parse_identifier(false)?))
            }
        } else {
            None
        };

        Ok(Expr::Function(Function {
            name,
            parameters,
            args: FunctionArguments::List(args),
            null_treatment,
            filter,
            over,
            within_group,
        }))
    }

    /// Optionally parses a null treatment clause.
    fn parse_null_treatment(&mut self) -> Result<Option<NullTreatment>, ParserError> {
        match self.parse_one_of_keywords(&[Keyword::RESPECT, Keyword::IGNORE]) {
            Some(keyword) => {
                self.expect_keyword(Keyword::NULLS)?;

                Ok(match keyword {
                    Keyword::RESPECT => Some(NullTreatment::RespectNulls),
                    Keyword::IGNORE => Some(NullTreatment::IgnoreNulls),
                    _ => None,
                })
            }
            None => Ok(None),
        }
    }

    pub fn parse_time_functions(&mut self, name: ObjectName) -> Result<Expr, ParserError> {
        let args = if self.consume_token(&Token::LParen) {
            FunctionArguments::List(self.parse_function_argument_list()?)
        } else {
            FunctionArguments::None
        };
        Ok(Expr::Function(Function {
            name,
            parameters: FunctionArguments::None,
            args,
            filter: None,
            over: None,
            null_treatment: None,
            within_group: vec![],
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

    /// Parse a group by expr. Group by expr can be one of group sets, roll up, cube, or simple expr.
    fn parse_group_by_expr(&mut self) -> Result<Expr, ParserError> {
        if self.dialect.supports_group_by_expr() {
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
            } else if self.consume_tokens(&[Token::LParen, Token::RParen]) {
                // PostgreSQL allow to use empty tuple as a group by expression,
                // e.g. `GROUP BY (), name`. Please refer to GROUP BY Clause section in
                // [PostgreSQL](https://www.postgresql.org/docs/16/sql-select.html)
                Ok(Expr::Tuple(vec![]))
            } else {
                self.parse_expr()
            }
        } else {
            // TODO parse rollup for other dialects
            self.parse_expr()
        }
    }

    /// Parse a tuple with `(` and `)`.
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

    pub fn parse_optional_cast_format(&mut self) -> Result<Option<CastFormat>, ParserError> {
        if self.parse_keyword(Keyword::FORMAT) {
            let value = self.parse_value()?;
            match self.parse_optional_time_zone()? {
                Some(tz) => Ok(Some(CastFormat::ValueAtTimeZone(value, tz))),
                None => Ok(Some(CastFormat::Value(value))),
            }
        } else {
            Ok(None)
        }
    }

    pub fn parse_optional_time_zone(&mut self) -> Result<Option<Value>, ParserError> {
        if self.parse_keywords(&[Keyword::AT, Keyword::TIME, Keyword::ZONE]) {
            self.parse_value().map(Some)
        } else {
            Ok(None)
        }
    }

    /// mssql-like convert function
    fn parse_mssql_convert(&mut self) -> Result<Expr, ParserError> {
        self.expect_token(&Token::LParen)?;
        let data_type = self.parse_data_type()?;
        self.expect_token(&Token::Comma)?;
        let expr = self.parse_expr()?;
        let styles = if self.consume_token(&Token::Comma) {
            self.parse_comma_separated(Parser::parse_expr)?
        } else {
            Default::default()
        };
        self.expect_token(&Token::RParen)?;
        Ok(Expr::Convert {
            expr: Box::new(expr),
            data_type: Some(data_type),
            charset: None,
            target_before_value: true,
            styles,
        })
    }

    /// Parse a SQL CONVERT function:
    ///  - `CONVERT('héhé' USING utf8mb4)` (MySQL)
    ///  - `CONVERT('héhé', CHAR CHARACTER SET utf8mb4)` (MySQL)
    ///  - `CONVERT(DECIMAL(10, 5), 42)` (MSSQL) - the type comes first
    pub fn parse_convert_expr(&mut self) -> Result<Expr, ParserError> {
        if self.dialect.convert_type_before_value() {
            return self.parse_mssql_convert();
        }
        self.expect_token(&Token::LParen)?;
        let expr = self.parse_expr()?;
        if self.parse_keyword(Keyword::USING) {
            let charset = self.parse_object_name(false)?;
            self.expect_token(&Token::RParen)?;
            return Ok(Expr::Convert {
                expr: Box::new(expr),
                data_type: None,
                charset: Some(charset),
                target_before_value: false,
                styles: vec![],
            });
        }
        self.expect_token(&Token::Comma)?;
        let data_type = self.parse_data_type()?;
        let charset = if self.parse_keywords(&[Keyword::CHARACTER, Keyword::SET]) {
            Some(self.parse_object_name(false)?)
        } else {
            None
        };
        self.expect_token(&Token::RParen)?;
        Ok(Expr::Convert {
            expr: Box::new(expr),
            data_type: Some(data_type),
            charset,
            target_before_value: false,
            styles: vec![],
        })
    }

    /// Parse a SQL CAST function e.g. `CAST(expr AS FLOAT)`
    pub fn parse_cast_expr(&mut self, kind: CastKind) -> Result<Expr, ParserError> {
        self.expect_token(&Token::LParen)?;
        let expr = self.parse_expr()?;
        self.expect_keyword(Keyword::AS)?;
        let data_type = self.parse_data_type()?;
        let format = self.parse_optional_cast_format()?;
        self.expect_token(&Token::RParen)?;
        Ok(Expr::Cast {
            kind,
            expr: Box::new(expr),
            data_type,
            format,
        })
    }

    /// Parse a SQL EXISTS expression e.g. `WHERE EXISTS(SELECT ...)`.
    pub fn parse_exists_expr(&mut self, negated: bool) -> Result<Expr, ParserError> {
        self.expect_token(&Token::LParen)?;
        let exists_node = Expr::Exists {
            negated,
            subquery: self.parse_boxed_query()?,
        };
        self.expect_token(&Token::RParen)?;
        Ok(exists_node)
    }

    pub fn parse_extract_expr(&mut self) -> Result<Expr, ParserError> {
        self.expect_token(&Token::LParen)?;
        let field = self.parse_date_time_field()?;

        let syntax = if self.parse_keyword(Keyword::FROM) {
            ExtractSyntax::From
        } else if self.consume_token(&Token::Comma)
            && dialect_of!(self is SnowflakeDialect | GenericDialect)
        {
            ExtractSyntax::Comma
        } else {
            return Err(ParserError::ParserError(
                "Expected 'FROM' or ','".to_string(),
            ));
        };

        let expr = self.parse_expr()?;
        self.expect_token(&Token::RParen)?;
        Ok(Expr::Extract {
            field,
            expr: Box::new(expr),
            syntax,
        })
    }

    pub fn parse_ceil_floor_expr(&mut self, is_ceil: bool) -> Result<Expr, ParserError> {
        self.expect_token(&Token::LParen)?;
        let expr = self.parse_expr()?;
        // Parse `CEIL/FLOOR(expr)`
        let field = if self.parse_keyword(Keyword::TO) {
            // Parse `CEIL/FLOOR(expr TO DateTimeField)`
            CeilFloorKind::DateTimeField(self.parse_date_time_field()?)
        } else if self.consume_token(&Token::Comma) {
            // Parse `CEIL/FLOOR(expr, scale)`
            match self.parse_value()? {
                Value::Number(n, s) => CeilFloorKind::Scale(Value::Number(n, s)),
                _ => {
                    return Err(ParserError::ParserError(
                        "Scale field can only be of number type".to_string(),
                    ))
                }
            }
        } else {
            CeilFloorKind::DateTimeField(DateTimeField::NoDateTime)
        };
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

    pub fn parse_position_expr(&mut self, ident: Ident) -> Result<Expr, ParserError> {
        let between_prec = self.dialect.prec_value(Precedence::Between);
        let position_expr = self.maybe_parse(|p| {
            // PARSE SELECT POSITION('@' in field)
            p.expect_token(&Token::LParen)?;

            // Parse the subexpr till the IN keyword
            let expr = p.parse_subexpr(between_prec)?;
            p.expect_keyword(Keyword::IN)?;
            let from = p.parse_expr()?;
            p.expect_token(&Token::RParen)?;
            Ok(Expr::Position {
                expr: Box::new(expr),
                r#in: Box::new(from),
            })
        });
        match position_expr {
            Some(expr) => Ok(expr),
            // Snowflake supports `position` as an ordinary function call
            // without the special `IN` syntax.
            None => self.parse_function(ObjectName(vec![ident])),
        }
    }

    pub fn parse_substring_expr(&mut self) -> Result<Expr, ParserError> {
        // PARSE SUBSTRING (EXPR [FROM 1] [FOR 3])
        self.expect_token(&Token::LParen)?;
        let expr = self.parse_expr()?;
        let mut from_expr = None;
        let special = self.consume_token(&Token::Comma);
        if special || self.parse_keyword(Keyword::FROM) {
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
            special,
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
    /// TRIM(<expr>, [, characters]) -- only Snowflake or BigQuery
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
                trim_characters: None,
            })
        } else if self.consume_token(&Token::Comma)
            && dialect_of!(self is SnowflakeDialect | BigQueryDialect | GenericDialect)
        {
            let characters = self.parse_comma_separated(Parser::parse_expr)?;
            self.expect_token(&Token::RParen)?;
            Ok(Expr::Trim {
                expr: Box::new(expr),
                trim_where: None,
                trim_what: None,
                trim_characters: Some(characters),
            })
        } else {
            self.expect_token(&Token::RParen)?;
            Ok(Expr::Trim {
                expr: Box::new(expr),
                trim_where,
                trim_what: None,
                trim_characters: None,
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
        let exprs = self.parse_comma_separated0(Parser::parse_expr, Token::RBracket)?;
        self.expect_token(&Token::RBracket)?;
        Ok(Expr::Array(Array { elem: exprs, named }))
    }

    pub fn parse_listagg_on_overflow(&mut self) -> Result<Option<ListAggOnOverflow>, ParserError> {
        if self.parse_keywords(&[Keyword::ON, Keyword::OVERFLOW]) {
            if self.parse_keyword(Keyword::ERROR) {
                Ok(Some(ListAggOnOverflow::Error))
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
                    | Token::UnicodeStringLiteral(_)
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
                Ok(Some(ListAggOnOverflow::Truncate { filler, with_count }))
            }
        } else {
            Ok(None)
        }
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
                Keyword::WEEK => {
                    let week_day = if dialect_of!(self is BigQueryDialect | GenericDialect)
                        && self.consume_token(&Token::LParen)
                    {
                        let week_day = self.parse_identifier(false)?;
                        self.expect_token(&Token::RParen)?;
                        Some(week_day)
                    } else {
                        None
                    };
                    Ok(DateTimeField::Week(week_day))
                }
                Keyword::DAY => Ok(DateTimeField::Day),
                Keyword::DAYOFWEEK => Ok(DateTimeField::DayOfWeek),
                Keyword::DAYOFYEAR => Ok(DateTimeField::DayOfYear),
                Keyword::DATE => Ok(DateTimeField::Date),
                Keyword::DATETIME => Ok(DateTimeField::Datetime),
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
                Keyword::ISOWEEK => Ok(DateTimeField::IsoWeek),
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
                Keyword::TIME => Ok(DateTimeField::Time),
                Keyword::TIMEZONE => Ok(DateTimeField::Timezone),
                Keyword::TIMEZONE_ABBR => Ok(DateTimeField::TimezoneAbbr),
                Keyword::TIMEZONE_HOUR => Ok(DateTimeField::TimezoneHour),
                Keyword::TIMEZONE_MINUTE => Ok(DateTimeField::TimezoneMinute),
                Keyword::TIMEZONE_REGION => Ok(DateTimeField::TimezoneRegion),
                _ if self.dialect.allow_extract_custom() => {
                    self.prev_token();
                    let custom = self.parse_identifier(false)?;
                    Ok(DateTimeField::Custom(custom))
                }
                _ => self.expected("date/time field", next_token),
            },
            Token::SingleQuotedString(_) if self.dialect.allow_extract_single_quotes() => {
                self.prev_token();
                let custom = self.parse_identifier(false)?;
                Ok(DateTimeField::Custom(custom))
            }
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
                    expr: Box::new(
                        self.parse_subexpr(self.dialect.prec_value(Precedence::UnaryNot))?,
                    ),
                }),
            },
            _ => Ok(Expr::UnaryOp {
                op: UnaryOperator::Not,
                expr: Box::new(self.parse_subexpr(self.dialect.prec_value(Precedence::UnaryNot))?),
            }),
        }
    }

    /// Parses fulltext expressions [`sqlparser::ast::Expr::MatchAgainst`]
    ///
    /// # Errors
    /// This method will raise an error if the column list is empty or with invalid identifiers,
    /// the match expression is not a literal string, or if the search modifier is not valid.
    pub fn parse_match_against(&mut self) -> Result<Expr, ParserError> {
        let columns = self.parse_parenthesized_column_list(Mandatory, false)?;

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

    /// Parse an `INTERVAL` expression.
    ///
    /// Some syntactically valid intervals:
    ///
    /// ```sql
    ///   1. INTERVAL '1' DAY
    ///   2. INTERVAL '1-1' YEAR TO MONTH
    ///   3. INTERVAL '1' SECOND
    ///   4. INTERVAL '1:1:1.1' HOUR (5) TO SECOND (5)
    ///   5. INTERVAL '1.1' SECOND (2, 2)
    ///   6. INTERVAL '1:1' HOUR (5) TO MINUTE (5)
    ///   7. (MySql & BigQuery only): INTERVAL 1 DAY
    /// ```
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

        Ok(Expr::Interval(Interval {
            value: Box::new(value),
            leading_field,
            leading_precision,
            last_field,
            fractional_seconds_precision: fsec_precision,
        }))
    }

    /// Bigquery specific: Parse a struct literal
    /// Syntax
    /// ```sql
    /// -- typed
    /// STRUCT<[field_name] field_type, ...>( expr1 [, ... ])
    /// -- typeless
    /// STRUCT( expr1 [AS field_name] [, ... ])
    /// ```
    fn parse_bigquery_struct_literal(&mut self) -> Result<Expr, ParserError> {
        let (fields, trailing_bracket) =
            self.parse_struct_type_def(Self::parse_struct_field_def)?;
        if trailing_bracket.0 {
            return parser_err!("unmatched > in STRUCT literal", self.peek_token().location);
        }

        self.expect_token(&Token::LParen)?;
        let values = self
            .parse_comma_separated(|parser| parser.parse_struct_field_expr(!fields.is_empty()))?;
        self.expect_token(&Token::RParen)?;

        Ok(Expr::Struct { values, fields })
    }

    /// Parse an expression value for a bigquery struct [1]
    /// Syntax
    /// ```sql
    /// expr [AS name]
    /// ```
    ///
    /// Parameter typed_syntax is set to true if the expression
    /// is to be parsed as a field expression declared using typed
    /// struct syntax [2], and false if using typeless struct syntax [3].
    ///
    /// [1]: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#constructing_a_struct
    /// [2]: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#typed_struct_syntax
    /// [3]: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#typeless_struct_syntax
    fn parse_struct_field_expr(&mut self, typed_syntax: bool) -> Result<Expr, ParserError> {
        let expr = self.parse_expr()?;
        if self.parse_keyword(Keyword::AS) {
            if typed_syntax {
                return parser_err!("Typed syntax does not allow AS", {
                    self.prev_token();
                    self.peek_token().location
                });
            }
            let field_name = self.parse_identifier(false)?;
            Ok(Expr::Named {
                expr: expr.into(),
                name: field_name,
            })
        } else {
            Ok(expr)
        }
    }

    /// Parse a Struct type definition as a sequence of field-value pairs.
    /// The syntax of the Struct elem differs by dialect so it is customised
    /// by the `elem_parser` argument.
    ///
    /// Syntax
    /// ```sql
    /// Hive:
    /// STRUCT<field_name: field_type>
    ///
    /// BigQuery:
    /// STRUCT<[field_name] field_type>
    /// ```
    fn parse_struct_type_def<F>(
        &mut self,
        mut elem_parser: F,
    ) -> Result<(Vec<StructField>, MatchedTrailingBracket), ParserError>
    where
        F: FnMut(&mut Parser<'a>) -> Result<(StructField, MatchedTrailingBracket), ParserError>,
    {
        let start_token = self.peek_token();
        self.expect_keyword(Keyword::STRUCT)?;

        // Nothing to do if we have no type information.
        if Token::Lt != self.peek_token() {
            return Ok((Default::default(), false.into()));
        }
        self.next_token();

        let mut field_defs = vec![];
        let trailing_bracket = loop {
            let (def, trailing_bracket) = elem_parser(self)?;
            field_defs.push(def);
            if !self.consume_token(&Token::Comma) {
                break trailing_bracket;
            }

            // Angle brackets are balanced so we only expect the trailing `>>` after
            // we've matched all field types for the current struct.
            // e.g. this is invalid syntax `STRUCT<STRUCT<INT>>>, INT>(NULL)`
            if trailing_bracket.0 {
                return parser_err!("unmatched > in STRUCT definition", start_token.location);
            }
        };

        Ok((
            field_defs,
            self.expect_closing_angle_bracket(trailing_bracket)?,
        ))
    }

    /// Duckdb Struct Data Type <https://duckdb.org/docs/sql/data_types/struct.html#retrieving-from-structs>
    fn parse_duckdb_struct_type_def(&mut self) -> Result<Vec<StructField>, ParserError> {
        self.expect_keyword(Keyword::STRUCT)?;
        self.expect_token(&Token::LParen)?;
        let struct_body = self.parse_comma_separated(|parser| {
            let field_name = parser.parse_identifier(false)?;
            let field_type = parser.parse_data_type()?;

            Ok(StructField {
                field_name: Some(field_name),
                field_type,
            })
        });
        self.expect_token(&Token::RParen)?;
        struct_body
    }

    /// Parse a field definition in a [struct] or [tuple].
    /// Syntax:
    ///
    /// ```sql
    /// [field_name] field_type
    /// ```
    ///
    /// [struct]: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#declaring_a_struct_type
    /// [tuple]: https://clickhouse.com/docs/en/sql-reference/data-types/tuple
    fn parse_struct_field_def(
        &mut self,
    ) -> Result<(StructField, MatchedTrailingBracket), ParserError> {
        // Look beyond the next item to infer whether both field name
        // and type are specified.
        let is_anonymous_field = !matches!(
            (self.peek_nth_token(0).token, self.peek_nth_token(1).token),
            (Token::Word(_), Token::Word(_))
        );

        let field_name = if is_anonymous_field {
            None
        } else {
            Some(self.parse_identifier(false)?)
        };

        let (field_type, trailing_bracket) = self.parse_data_type_helper()?;

        Ok((
            StructField {
                field_name,
                field_type,
            },
            trailing_bracket,
        ))
    }

    /// DuckDB specific: Parse a Union type definition as a sequence of field-value pairs.
    ///
    /// Syntax:
    ///
    /// ```sql
    /// UNION(field_name field_type[,...])
    /// ```
    ///
    /// [1]: https://duckdb.org/docs/sql/data_types/union.html
    fn parse_union_type_def(&mut self) -> Result<Vec<UnionField>, ParserError> {
        self.expect_keyword(Keyword::UNION)?;

        self.expect_token(&Token::LParen)?;

        let fields = self.parse_comma_separated(|p| {
            Ok(UnionField {
                field_name: p.parse_identifier(false)?,
                field_type: p.parse_data_type()?,
            })
        })?;

        self.expect_token(&Token::RParen)?;

        Ok(fields)
    }

    /// DuckDB specific: Parse a duckdb [dictionary]
    ///
    /// Syntax:
    ///
    /// ```sql
    /// {'field_name': expr1[, ... ]}
    /// ```
    ///
    /// [dictionary]: https://duckdb.org/docs/sql/data_types/struct#creating-structs
    fn parse_duckdb_struct_literal(&mut self) -> Result<Expr, ParserError> {
        self.expect_token(&Token::LBrace)?;

        let fields = self.parse_comma_separated(Self::parse_duckdb_dictionary_field)?;

        self.expect_token(&Token::RBrace)?;

        Ok(Expr::Dictionary(fields))
    }

    /// Parse a field for a duckdb [dictionary]
    ///
    /// Syntax
    ///
    /// ```sql
    /// 'name': expr
    /// ```
    ///
    /// [dictionary]: https://duckdb.org/docs/sql/data_types/struct#creating-structs
    fn parse_duckdb_dictionary_field(&mut self) -> Result<DictionaryField, ParserError> {
        let key = self.parse_identifier(false)?;

        self.expect_token(&Token::Colon)?;

        let expr = self.parse_expr()?;

        Ok(DictionaryField {
            key,
            value: Box::new(expr),
        })
    }

    /// DuckDB specific: Parse a duckdb [map]
    ///
    /// Syntax:
    ///
    /// ```sql
    /// Map {key1: value1[, ... ]}
    /// ```
    ///
    /// [map]: https://duckdb.org/docs/sql/data_types/map.html#creating-maps
    fn parse_duckdb_map_literal(&mut self) -> Result<Expr, ParserError> {
        self.expect_token(&Token::LBrace)?;
        let fields = self.parse_comma_separated0(Self::parse_duckdb_map_field, Token::RBrace)?;
        self.expect_token(&Token::RBrace)?;
        Ok(Expr::Map(Map { entries: fields }))
    }

    /// Parse a field for a duckdb [map]
    ///
    /// Syntax
    ///
    /// ```sql
    /// key: value
    /// ```
    ///
    /// [map]: https://duckdb.org/docs/sql/data_types/map.html#creating-maps
    fn parse_duckdb_map_field(&mut self) -> Result<MapEntry, ParserError> {
        let key = self.parse_expr()?;

        self.expect_token(&Token::Colon)?;

        let value = self.parse_expr()?;

        Ok(MapEntry {
            key: Box::new(key),
            value: Box::new(value),
        })
    }

    /// Parse clickhouse [map]
    ///
    /// Syntax
    ///
    /// ```sql
    /// Map(key_data_type, value_data_type)
    /// ```
    ///
    /// [map]: https://clickhouse.com/docs/en/sql-reference/data-types/map
    fn parse_click_house_map_def(&mut self) -> Result<(DataType, DataType), ParserError> {
        self.expect_keyword(Keyword::MAP)?;
        self.expect_token(&Token::LParen)?;
        let key_data_type = self.parse_data_type()?;
        self.expect_token(&Token::Comma)?;
        let value_data_type = self.parse_data_type()?;
        self.expect_token(&Token::RParen)?;

        Ok((key_data_type, value_data_type))
    }

    /// Parse clickhouse [tuple]
    ///
    /// Syntax
    ///
    /// ```sql
    /// Tuple([field_name] field_type, ...)
    /// ```
    ///
    /// [tuple]: https://clickhouse.com/docs/en/sql-reference/data-types/tuple
    fn parse_click_house_tuple_def(&mut self) -> Result<Vec<StructField>, ParserError> {
        self.expect_keyword(Keyword::TUPLE)?;
        self.expect_token(&Token::LParen)?;
        let mut field_defs = vec![];
        loop {
            let (def, _) = self.parse_struct_field_def()?;
            field_defs.push(def);
            if !self.consume_token(&Token::Comma) {
                break;
            }
        }
        self.expect_token(&Token::RParen)?;

        Ok(field_defs)
    }

    /// For nested types that use the angle bracket syntax, this matches either
    /// `>`, `>>` or nothing depending on which variant is expected (specified by the previously
    /// matched `trailing_bracket` argument). It returns whether there is a trailing
    /// left to be matched - (i.e. if '>>' was matched).
    fn expect_closing_angle_bracket(
        &mut self,
        trailing_bracket: MatchedTrailingBracket,
    ) -> Result<MatchedTrailingBracket, ParserError> {
        let trailing_bracket = if !trailing_bracket.0 {
            match self.peek_token().token {
                Token::Gt => {
                    self.next_token();
                    false.into()
                }
                Token::ShiftRight => {
                    self.next_token();
                    true.into()
                }
                _ => return self.expected(">", self.peek_token()),
            }
        } else {
            false.into()
        };

        Ok(trailing_bracket)
    }

    /// Parse an operator following an expression
    pub fn parse_infix(&mut self, expr: Expr, precedence: u8) -> Result<Expr, ParserError> {
        // allow the dialect to override infix parsing
        if let Some(infix) = self.dialect.parse_infix(self, &expr, precedence) {
            return infix;
        }

        let mut tok = self.next_token();
        let regular_binary_operator = match &mut tok.token {
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
            Token::Caret => {
                // In PostgreSQL, ^ stands for the exponentiation operation,
                // and # stands for XOR. See https://www.postgresql.org/docs/current/functions-math.html
                if dialect_of!(self is PostgreSqlDialect) {
                    Some(BinaryOperator::PGExp)
                } else {
                    Some(BinaryOperator::BitwiseXor)
                }
            }
            Token::Ampersand => Some(BinaryOperator::BitwiseAnd),
            Token::Div => Some(BinaryOperator::Divide),
            Token::DuckIntDiv if dialect_of!(self is DuckDbDialect | GenericDialect) => {
                Some(BinaryOperator::DuckIntegerDivide)
            }
            Token::ShiftLeft if dialect_of!(self is PostgreSqlDialect | DuckDbDialect | GenericDialect) => {
                Some(BinaryOperator::PGBitwiseShiftLeft)
            }
            Token::ShiftRight if dialect_of!(self is PostgreSqlDialect | DuckDbDialect | GenericDialect) => {
                Some(BinaryOperator::PGBitwiseShiftRight)
            }
            Token::Sharp if dialect_of!(self is PostgreSqlDialect) => {
                Some(BinaryOperator::PGBitwiseXor)
            }
            Token::Overlap if dialect_of!(self is PostgreSqlDialect | GenericDialect) => {
                Some(BinaryOperator::PGOverlap)
            }
            Token::CaretAt if dialect_of!(self is PostgreSqlDialect | GenericDialect) => {
                Some(BinaryOperator::PGStartsWith)
            }
            Token::Tilde => Some(BinaryOperator::PGRegexMatch),
            Token::TildeAsterisk => Some(BinaryOperator::PGRegexIMatch),
            Token::ExclamationMarkTilde => Some(BinaryOperator::PGRegexNotMatch),
            Token::ExclamationMarkTildeAsterisk => Some(BinaryOperator::PGRegexNotIMatch),
            Token::DoubleTilde => Some(BinaryOperator::PGLikeMatch),
            Token::DoubleTildeAsterisk => Some(BinaryOperator::PGILikeMatch),
            Token::ExclamationMarkDoubleTilde => Some(BinaryOperator::PGNotLikeMatch),
            Token::ExclamationMarkDoubleTildeAsterisk => Some(BinaryOperator::PGNotILikeMatch),
            Token::Arrow => Some(BinaryOperator::Arrow),
            Token::LongArrow => Some(BinaryOperator::LongArrow),
            Token::HashArrow => Some(BinaryOperator::HashArrow),
            Token::HashLongArrow => Some(BinaryOperator::HashLongArrow),
            Token::AtArrow => Some(BinaryOperator::AtArrow),
            Token::ArrowAt => Some(BinaryOperator::ArrowAt),
            Token::HashMinus => Some(BinaryOperator::HashMinus),
            Token::AtQuestion => Some(BinaryOperator::AtQuestion),
            Token::AtAt => Some(BinaryOperator::AtAt),
            Token::Question => Some(BinaryOperator::Question),
            Token::QuestionAnd => Some(BinaryOperator::QuestionAnd),
            Token::QuestionPipe => Some(BinaryOperator::QuestionPipe),
            Token::CustomBinaryOperator(s) => Some(BinaryOperator::Custom(core::mem::take(s))),

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

                if !matches!(
                    op,
                    BinaryOperator::Gt
                        | BinaryOperator::Lt
                        | BinaryOperator::GtEq
                        | BinaryOperator::LtEq
                        | BinaryOperator::Eq
                        | BinaryOperator::NotEq
                ) {
                    return parser_err!(
                        format!(
                        "Expected one of [=, >, <, =>, =<, !=] as comparison operator, found: {op}"
                    ),
                        tok.location
                    );
                };

                Ok(match keyword {
                    Keyword::ALL => Expr::AllOp {
                        left: Box::new(expr),
                        compare_op: op,
                        right: Box::new(right),
                    },
                    Keyword::ANY => Expr::AnyOp {
                        left: Box::new(expr),
                        compare_op: op,
                        right: Box::new(right),
                    },
                    _ => unreachable!(),
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
                    self.expect_keywords(&[Keyword::TIME, Keyword::ZONE])?;
                    Ok(Expr::AtTimeZone {
                        timestamp: Box::new(expr),
                        time_zone: Box::new(self.parse_subexpr(precedence)?),
                    })
                }
                Keyword::NOT
                | Keyword::IN
                | Keyword::BETWEEN
                | Keyword::LIKE
                | Keyword::ILIKE
                | Keyword::SIMILAR
                | Keyword::REGEXP
                | Keyword::RLIKE => {
                    self.prev_token();
                    let negated = self.parse_keyword(Keyword::NOT);
                    let regexp = self.parse_keyword(Keyword::REGEXP);
                    let rlike = self.parse_keyword(Keyword::RLIKE);
                    if regexp || rlike {
                        Ok(Expr::RLike {
                            negated,
                            expr: Box::new(expr),
                            pattern: Box::new(
                                self.parse_subexpr(self.dialect.prec_value(Precedence::Like))?,
                            ),
                            regexp,
                        })
                    } else if self.parse_keyword(Keyword::IN) {
                        self.parse_in(expr, negated)
                    } else if self.parse_keyword(Keyword::BETWEEN) {
                        self.parse_between(expr, negated)
                    } else if self.parse_keyword(Keyword::LIKE) {
                        Ok(Expr::Like {
                            negated,
                            expr: Box::new(expr),
                            pattern: Box::new(
                                self.parse_subexpr(self.dialect.prec_value(Precedence::Like))?,
                            ),
                            escape_char: self.parse_escape_char()?,
                        })
                    } else if self.parse_keyword(Keyword::ILIKE) {
                        Ok(Expr::ILike {
                            negated,
                            expr: Box::new(expr),
                            pattern: Box::new(
                                self.parse_subexpr(self.dialect.prec_value(Precedence::Like))?,
                            ),
                            escape_char: self.parse_escape_char()?,
                        })
                    } else if self.parse_keywords(&[Keyword::SIMILAR, Keyword::TO]) {
                        Ok(Expr::SimilarTo {
                            negated,
                            expr: Box::new(expr),
                            pattern: Box::new(
                                self.parse_subexpr(self.dialect.prec_value(Precedence::Like))?,
                            ),
                            escape_char: self.parse_escape_char()?,
                        })
                    } else {
                        self.expected("IN or BETWEEN after NOT", self.peek_token())
                    }
                }
                // Can only happen if `get_next_precedence` got out of sync with this function
                _ => parser_err!(
                    format!("No infix parser for token {:?}", tok.token),
                    tok.location
                ),
            }
        } else if Token::DoubleColon == tok {
            Ok(Expr::Cast {
                kind: CastKind::DoubleColon,
                expr: Box::new(expr),
                data_type: self.parse_data_type()?,
                format: None,
            })
        } else if Token::ExclamationMark == tok {
            // PostgreSQL factorial operation
            Ok(Expr::UnaryOp {
                op: UnaryOperator::PGPostfixFactorial,
                expr: Box::new(expr),
            })
        } else if Token::LBracket == tok {
            if dialect_of!(self is PostgreSqlDialect | DuckDbDialect | GenericDialect) {
                self.parse_subscript(expr)
            } else if dialect_of!(self is SnowflakeDialect) {
                self.prev_token();
                self.parse_json_access(expr)
            } else {
                self.parse_map_access(expr)
            }
        } else if dialect_of!(self is SnowflakeDialect | GenericDialect) && Token::Colon == tok {
            self.prev_token();
            self.parse_json_access(expr)
        } else {
            // Can only happen if `get_next_precedence` got out of sync with this function
            parser_err!(
                format!("No infix parser for token {:?}", tok.token),
                tok.location
            )
        }
    }

    /// Parse the `ESCAPE CHAR` portion of `LIKE`, `ILIKE`, and `SIMILAR TO`
    pub fn parse_escape_char(&mut self) -> Result<Option<String>, ParserError> {
        if self.parse_keyword(Keyword::ESCAPE) {
            Ok(Some(self.parse_literal_string()?))
        } else {
            Ok(None)
        }
    }

    /// Parses an array subscript like
    /// * `[:]`
    /// * `[l]`
    /// * `[l:]`
    /// * `[:u]`
    /// * `[l:u]`
    /// * `[l:u:s]`
    ///
    /// Parser is right after `[`
    fn parse_subscript_inner(&mut self) -> Result<Subscript, ParserError> {
        // at either `<lower>:(rest)` or `:(rest)]`
        let lower_bound = if self.consume_token(&Token::Colon) {
            None
        } else {
            Some(self.parse_expr()?)
        };

        // check for end
        if self.consume_token(&Token::RBracket) {
            if let Some(lower_bound) = lower_bound {
                return Ok(Subscript::Index { index: lower_bound });
            };
            return Ok(Subscript::Slice {
                lower_bound,
                upper_bound: None,
                stride: None,
            });
        }

        // consume the `:`
        if lower_bound.is_some() {
            self.expect_token(&Token::Colon)?;
        }

        // we are now at either `]`, `<upper>(rest)]`
        let upper_bound = if self.consume_token(&Token::RBracket) {
            return Ok(Subscript::Slice {
                lower_bound,
                upper_bound: None,
                stride: None,
            });
        } else {
            Some(self.parse_expr()?)
        };

        // check for end
        if self.consume_token(&Token::RBracket) {
            return Ok(Subscript::Slice {
                lower_bound,
                upper_bound,
                stride: None,
            });
        }

        // we are now at `:]` or `:stride]`
        self.expect_token(&Token::Colon)?;
        let stride = if self.consume_token(&Token::RBracket) {
            None
        } else {
            Some(self.parse_expr()?)
        };

        if stride.is_some() {
            self.expect_token(&Token::RBracket)?;
        }

        Ok(Subscript::Slice {
            lower_bound,
            upper_bound,
            stride,
        })
    }

    /// Parses an array subscript like `[1:3]`
    ///
    /// Parser is right after `[`
    pub fn parse_subscript(&mut self, expr: Expr) -> Result<Expr, ParserError> {
        let subscript = self.parse_subscript_inner()?;
        Ok(Expr::Subscript {
            expr: Box::new(expr),
            subscript: Box::new(subscript),
        })
    }

    fn parse_json_path_object_key(&mut self) -> Result<JsonPathElem, ParserError> {
        let token = self.next_token();
        match token.token {
            Token::Word(Word {
                value,
                // path segments in SF dot notation can be unquoted or double-quoted
                quote_style: quote_style @ (Some('"') | None),
                // some experimentation suggests that snowflake permits
                // any keyword here unquoted.
                keyword: _,
            }) => Ok(JsonPathElem::Dot {
                key: value,
                quoted: quote_style.is_some(),
            }),

            // This token should never be generated on snowflake or generic
            // dialects, but we handle it just in case this is used on future
            // dialects.
            Token::DoubleQuotedString(key) => Ok(JsonPathElem::Dot { key, quoted: true }),

            _ => self.expected("variant object key name", token),
        }
    }

    fn parse_json_access(&mut self, expr: Expr) -> Result<Expr, ParserError> {
        let mut path = Vec::new();
        loop {
            match self.next_token().token {
                Token::Colon if path.is_empty() => {
                    path.push(self.parse_json_path_object_key()?);
                }
                Token::Period if !path.is_empty() => {
                    path.push(self.parse_json_path_object_key()?);
                }
                Token::LBracket => {
                    let key = self.parse_expr()?;
                    self.expect_token(&Token::RBracket)?;

                    path.push(JsonPathElem::Bracket { key });
                }
                _ => {
                    self.prev_token();
                    break;
                }
            };
        }

        debug_assert!(!path.is_empty());
        Ok(Expr::JsonAccess {
            value: Box::new(expr),
            path: JsonPath { path },
        })
    }

    pub fn parse_map_access(&mut self, expr: Expr) -> Result<Expr, ParserError> {
        let key = self.parse_expr()?;
        self.expect_token(&Token::RBracket)?;

        let mut keys = vec![MapAccessKey {
            key,
            syntax: MapAccessSyntax::Bracket,
        }];
        loop {
            let key = match self.peek_token().token {
                Token::LBracket => {
                    self.next_token(); // consume `[`
                    let key = self.parse_expr()?;
                    self.expect_token(&Token::RBracket)?;
                    MapAccessKey {
                        key,
                        syntax: MapAccessSyntax::Bracket,
                    }
                }
                // Access on BigQuery nested and repeated expressions can
                // mix notations in the same expression.
                // https://cloud.google.com/bigquery/docs/nested-repeated#query_nested_and_repeated_columns
                Token::Period if dialect_of!(self is BigQueryDialect) => {
                    self.next_token(); // consume `.`
                    MapAccessKey {
                        key: self.parse_expr()?,
                        syntax: MapAccessSyntax::Period,
                    }
                }
                _ => break,
            };
            keys.push(key);
        }

        Ok(Expr::MapAccess {
            column: Box::new(expr),
            keys,
        })
    }

    /// Parses the parens following the `[ NOT ] IN` operator.
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
                subquery: self.parse_boxed_query()?,
                negated,
            }
        } else {
            Expr::InList {
                expr: Box::new(expr),
                list: if self.dialect.supports_in_empty_list() {
                    self.parse_comma_separated0(Parser::parse_expr, Token::RParen)?
                } else {
                    self.parse_comma_separated(Parser::parse_expr)?
                },
                negated,
            }
        };
        self.expect_token(&Token::RParen)?;
        Ok(in_op)
    }

    /// Parses `BETWEEN <low> AND <high>`, assuming the `BETWEEN` keyword was already consumed.
    pub fn parse_between(&mut self, expr: Expr, negated: bool) -> Result<Expr, ParserError> {
        // Stop parsing subexpressions for <low> and <high> on tokens with
        // precedence lower than that of `BETWEEN`, such as `AND`, `IS`, etc.
        let low = self.parse_subexpr(self.dialect.prec_value(Precedence::Between))?;
        self.expect_keyword(Keyword::AND)?;
        let high = self.parse_subexpr(self.dialect.prec_value(Precedence::Between))?;
        Ok(Expr::Between {
            expr: Box::new(expr),
            negated,
            low: Box::new(low),
            high: Box::new(high),
        })
    }

    /// Parse a postgresql casting style which is in the form of `expr::datatype`.
    pub fn parse_pg_cast(&mut self, expr: Expr) -> Result<Expr, ParserError> {
        Ok(Expr::Cast {
            kind: CastKind::DoubleColon,
            expr: Box::new(expr),
            data_type: self.parse_data_type()?,
            format: None,
        })
    }

    /// Get the precedence of the next token
    pub fn get_next_precedence(&self) -> Result<u8, ParserError> {
        self.dialect.get_next_precedence_default(self)
    }

    /// Return the first non-whitespace token that has not yet been processed
    /// (or None if reached end-of-file)
    pub fn peek_token(&self) -> TokenWithLocation {
        self.peek_nth_token(0)
    }

    /// Returns the `N` next non-whitespace tokens that have not yet been
    /// processed.
    ///
    /// Example:
    /// ```rust
    /// # use sqlparser::dialect::GenericDialect;
    /// # use sqlparser::parser::Parser;
    /// # use sqlparser::keywords::Keyword;
    /// # use sqlparser::tokenizer::{Token, Word};
    /// let dialect = GenericDialect {};
    /// let mut parser = Parser::new(&dialect).try_with_sql("ORDER BY foo, bar").unwrap();
    ///
    /// // Note that Rust infers the number of tokens to peek based on the
    /// // length of the slice pattern!
    /// assert!(matches!(
    ///     parser.peek_tokens(),
    ///     [
    ///         Token::Word(Word { keyword: Keyword::ORDER, .. }),
    ///         Token::Word(Word { keyword: Keyword::BY, .. }),
    ///     ]
    /// ));
    /// ```
    pub fn peek_tokens<const N: usize>(&self) -> [Token; N] {
        self.peek_tokens_with_location()
            .map(|with_loc| with_loc.token)
    }

    /// Returns the `N` next non-whitespace tokens with locations that have not
    /// yet been processed.
    ///
    /// See [`Self::peek_token`] for an example.
    pub fn peek_tokens_with_location<const N: usize>(&self) -> [TokenWithLocation; N] {
        let mut index = self.index;
        core::array::from_fn(|_| loop {
            let token = self.tokens.get(index);
            index += 1;
            if let Some(TokenWithLocation {
                token: Token::Whitespace(_),
                location: _,
            }) = token
            {
                continue;
            }
            break token.cloned().unwrap_or(TokenWithLocation {
                token: Token::EOF,
                location: Location { line: 0, column: 0 },
            });
        })
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

    /// Return the first token, possibly whitespace, that has not yet been processed
    /// (or None if reached end-of-file).
    pub fn peek_token_no_skip(&self) -> TokenWithLocation {
        self.peek_nth_token_no_skip(0)
    }

    /// Return nth token, possibly whitespace, that has not yet been processed.
    pub fn peek_nth_token_no_skip(&self, n: usize) -> TokenWithLocation {
        self.tokens
            .get(self.index + n)
            .cloned()
            .unwrap_or(TokenWithLocation {
                token: Token::EOF,
                location: Location { line: 0, column: 0 },
            })
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

    /// Report `found` was encountered instead of `expected`
    pub fn expected<T>(&self, expected: &str, found: TokenWithLocation) -> Result<T, ParserError> {
        parser_err!(
            format!("Expected: {expected}, found: {found}"),
            found.location
        )
    }

    /// If the current token is the `expected` keyword, consume it and returns
    /// true. Otherwise, no tokens are consumed and returns false.
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

    /// If the current token is the `expected` keyword followed by
    /// specified tokens, consume them and returns true.
    /// Otherwise, no tokens are consumed and returns false.
    ///
    /// Note that if the length of `tokens` is too long, this function will
    /// not be efficient as it does a loop on the tokens with `peek_nth_token`
    /// each time.
    pub fn parse_keyword_with_tokens(&mut self, expected: Keyword, tokens: &[Token]) -> bool {
        match self.peek_token().token {
            Token::Word(w) if expected == w.keyword => {
                for (idx, token) in tokens.iter().enumerate() {
                    if self.peek_nth_token(idx + 1).token != *token {
                        return false;
                    }
                }
                // consume all tokens
                for _ in 0..(tokens.len() + 1) {
                    self.next_token();
                }
                true
            }
            _ => false,
        }
    }

    /// If the current and subsequent tokens exactly match the `keywords`
    /// sequence, consume them and returns true. Otherwise, no tokens are
    /// consumed and returns false
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

    /// If the current token is one of the given `keywords`, consume the token
    /// and return the keyword that matches. Otherwise, no tokens are consumed
    /// and returns [`None`].
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

    /// If the current token is one of the expected keywords, consume the token
    /// and return the keyword that matches. Otherwise, return an error.
    pub fn expect_one_of_keywords(&mut self, keywords: &[Keyword]) -> Result<Keyword, ParserError> {
        if let Some(keyword) = self.parse_one_of_keywords(keywords) {
            Ok(keyword)
        } else {
            let keywords: Vec<String> = keywords.iter().map(|x| format!("{x:?}")).collect();
            self.expected(
                &format!("one of {}", keywords.join(" or ")),
                self.peek_token(),
            )
        }
    }

    /// If the current token is the `expected` keyword, consume the token.
    /// Otherwise, return an error.
    pub fn expect_keyword(&mut self, expected: Keyword) -> Result<(), ParserError> {
        if self.parse_keyword(expected) {
            Ok(())
        } else {
            self.expected(format!("{:?}", &expected).as_str(), self.peek_token())
        }
    }

    /// If the current and subsequent tokens exactly match the `keywords`
    /// sequence, consume them and returns Ok. Otherwise, return an Error.
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

    /// If the current and subsequent tokens exactly match the `tokens`
    /// sequence, consume them and returns true. Otherwise, no tokens are
    /// consumed and returns false
    #[must_use]
    pub fn consume_tokens(&mut self, tokens: &[Token]) -> bool {
        let index = self.index;
        for token in tokens {
            if !self.consume_token(token) {
                self.index = index;
                return false;
            }
        }
        true
    }

    /// Bail out if the current token is not an expected keyword, or consume it if it is
    pub fn expect_token(&mut self, expected: &Token) -> Result<(), ParserError> {
        if self.consume_token(expected) {
            Ok(())
        } else {
            self.expected(&expected.to_string(), self.peek_token())
        }
    }

    fn parse<T: FromStr>(s: String, loc: Location) -> Result<T, ParserError>
    where
        <T as FromStr>::Err: Display,
    {
        s.parse::<T>().map_err(|e| {
            ParserError::ParserError(format!(
                "Could not parse '{s}' as {}: {e}{loc}",
                core::any::type_name::<T>()
            ))
        })
    }

    /// Parse a comma-separated list of 1+ SelectItem
    pub fn parse_projection(&mut self) -> Result<Vec<SelectItem>, ParserError> {
        // BigQuery and Snowflake allow trailing commas, but only in project lists
        // e.g. `SELECT 1, 2, FROM t`
        // https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#trailing_commas
        // https://docs.snowflake.com/en/release-notes/2024/8_11#select-supports-trailing-commas
        //
        // This pattern could be captured better with RAII type semantics, but it's quite a bit of
        // code to add for just one case, so we'll just do it manually here.
        let old_value = self.options.trailing_commas;
        self.options.trailing_commas |= self.dialect.supports_projection_trailing_commas();

        let ret = self.parse_comma_separated(|p| p.parse_select_item());
        self.options.trailing_commas = old_value;

        ret
    }

    pub fn parse_actions_list(&mut self) -> Result<Vec<ParsedAction>, ParserError> {
        let mut values = vec![];
        loop {
            values.push(self.parse_grant_permission()?);
            if !self.consume_token(&Token::Comma) {
                break;
            } else if self.options.trailing_commas {
                match self.peek_token().token {
                    Token::Word(kw) if kw.keyword == Keyword::ON => {
                        break;
                    }
                    Token::RParen
                    | Token::SemiColon
                    | Token::EOF
                    | Token::RBracket
                    | Token::RBrace => break,
                    _ => continue,
                }
            }
        }
        Ok(values)
    }

    /// Parse the comma of a comma-separated syntax element.
    /// Returns true if there is a next element
    fn is_parse_comma_separated_end(&mut self) -> bool {
        if !self.consume_token(&Token::Comma) {
            true
        } else if self.options.trailing_commas {
            let token = self.peek_token().token;
            match token {
                Token::Word(ref kw)
                    if keywords::RESERVED_FOR_COLUMN_ALIAS.contains(&kw.keyword) =>
                {
                    true
                }
                Token::RParen | Token::SemiColon | Token::EOF | Token::RBracket | Token::RBrace => {
                    true
                }
                _ => false,
            }
        } else {
            false
        }
    }

    /// Parse a comma-separated list of 1+ items accepted by `F`
    pub fn parse_comma_separated<T, F>(&mut self, mut f: F) -> Result<Vec<T>, ParserError>
    where
        F: FnMut(&mut Parser<'a>) -> Result<T, ParserError>,
    {
        let mut values = vec![];
        loop {
            values.push(f(self)?);
            if self.is_parse_comma_separated_end() {
                break;
            }
        }
        Ok(values)
    }

    /// Parse a keyword-separated list of 1+ items accepted by `F`
    pub fn parse_keyword_separated<T, F>(
        &mut self,
        keyword: Keyword,
        mut f: F,
    ) -> Result<Vec<T>, ParserError>
    where
        F: FnMut(&mut Parser<'a>) -> Result<T, ParserError>,
    {
        let mut values = vec![];
        loop {
            values.push(f(self)?);
            if !self.parse_keyword(keyword) {
                break;
            }
        }
        Ok(values)
    }

    pub fn parse_parenthesized<T, F>(&mut self, mut f: F) -> Result<T, ParserError>
    where
        F: FnMut(&mut Parser<'a>) -> Result<T, ParserError>,
    {
        self.expect_token(&Token::LParen)?;
        let res = f(self)?;
        self.expect_token(&Token::RParen)?;
        Ok(res)
    }

    /// Parse a comma-separated list of 0+ items accepted by `F`
    /// * `end_token` - expected end token for the closure (e.g. [Token::RParen], [Token::RBrace] ...)
    pub fn parse_comma_separated0<T, F>(
        &mut self,
        f: F,
        end_token: Token,
    ) -> Result<Vec<T>, ParserError>
    where
        F: FnMut(&mut Parser<'a>) -> Result<T, ParserError>,
    {
        if self.peek_token().token == end_token {
            return Ok(vec![]);
        }

        if self.options.trailing_commas && self.peek_tokens() == [Token::Comma, end_token] {
            let _ = self.consume_token(&Token::Comma);
            return Ok(vec![]);
        }

        self.parse_comma_separated(f)
    }

    /// Run a parser method `f`, reverting back to the current position if unsuccessful.
    #[must_use]
    pub fn maybe_parse<T, F>(&mut self, mut f: F) -> Option<T>
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

    /// Parse either `ALL`, `DISTINCT` or `DISTINCT ON (...)`. Returns [`None`] if `ALL` is parsed
    /// and results in a [`ParserError`] if both `ALL` and `DISTINCT` are found.
    pub fn parse_all_or_distinct(&mut self) -> Result<Option<Distinct>, ParserError> {
        let loc = self.peek_token().location;
        let all = self.parse_keyword(Keyword::ALL);
        let distinct = self.parse_keyword(Keyword::DISTINCT);
        if !distinct {
            return Ok(None);
        }
        if all {
            return parser_err!("Cannot specify both ALL and DISTINCT".to_string(), loc);
        }
        let on = self.parse_keyword(Keyword::ON);
        if !on {
            return Ok(Some(Distinct::Distinct));
        }

        self.expect_token(&Token::LParen)?;
        let col_names = if self.consume_token(&Token::RParen) {
            self.prev_token();
            Vec::new()
        } else {
            self.parse_comma_separated(Parser::parse_expr)?
        };
        self.expect_token(&Token::RParen)?;
        Ok(Some(Distinct::On(col_names)))
    }

    /// Parse a SQL CREATE statement
    pub fn parse_create(&mut self) -> Result<Statement, ParserError> {
        let or_replace = self.parse_keywords(&[Keyword::OR, Keyword::REPLACE]);
        let or_alter = self.parse_keywords(&[Keyword::OR, Keyword::ALTER]);
        let local = self.parse_one_of_keywords(&[Keyword::LOCAL]).is_some();
        let global = self.parse_one_of_keywords(&[Keyword::GLOBAL]).is_some();
        let transient = self.parse_one_of_keywords(&[Keyword::TRANSIENT]).is_some();
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
        let persistent = dialect_of!(self is DuckDbDialect)
            && self.parse_one_of_keywords(&[Keyword::PERSISTENT]).is_some();
        if self.parse_keyword(Keyword::TABLE) {
            self.parse_create_table(or_replace, temporary, global, transient)
        } else if self.parse_keyword(Keyword::MATERIALIZED) || self.parse_keyword(Keyword::VIEW) {
            self.prev_token();
            self.parse_create_view(or_replace, temporary)
        } else if self.parse_keyword(Keyword::EXTERNAL) {
            self.parse_create_external_table(or_replace)
        } else if self.parse_keyword(Keyword::FUNCTION) {
            self.parse_create_function(or_replace, temporary)
        } else if self.parse_keyword(Keyword::TRIGGER) {
            self.parse_create_trigger(or_replace, false)
        } else if self.parse_keywords(&[Keyword::CONSTRAINT, Keyword::TRIGGER]) {
            self.parse_create_trigger(or_replace, true)
        } else if self.parse_keyword(Keyword::MACRO) {
            self.parse_create_macro(or_replace, temporary)
        } else if self.parse_keyword(Keyword::SECRET) {
            self.parse_create_secret(or_replace, temporary, persistent)
        } else if or_replace {
            self.expected(
                "[EXTERNAL] TABLE or [MATERIALIZED] VIEW or FUNCTION after CREATE OR REPLACE",
                self.peek_token(),
            )
        } else if self.parse_keyword(Keyword::EXTENSION) {
            self.parse_create_extension()
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
        } else if self.parse_keyword(Keyword::TYPE) {
            self.parse_create_type()
        } else if self.parse_keyword(Keyword::PROCEDURE) {
            self.parse_create_procedure(or_alter)
        } else {
            self.expected("an object type after CREATE", self.peek_token())
        }
    }

    /// See [DuckDB Docs](https://duckdb.org/docs/sql/statements/create_secret.html) for more details.
    pub fn parse_create_secret(
        &mut self,
        or_replace: bool,
        temporary: bool,
        persistent: bool,
    ) -> Result<Statement, ParserError> {
        let if_not_exists = self.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);

        let mut storage_specifier = None;
        let mut name = None;
        if self.peek_token() != Token::LParen {
            if self.parse_keyword(Keyword::IN) {
                storage_specifier = self.parse_identifier(false).ok()
            } else {
                name = self.parse_identifier(false).ok();
            }

            // Storage specifier may follow the name
            if storage_specifier.is_none()
                && self.peek_token() != Token::LParen
                && self.parse_keyword(Keyword::IN)
            {
                storage_specifier = self.parse_identifier(false).ok();
            }
        }

        self.expect_token(&Token::LParen)?;
        self.expect_keyword(Keyword::TYPE)?;
        let secret_type = self.parse_identifier(false)?;

        let mut options = Vec::new();
        if self.consume_token(&Token::Comma) {
            options.append(&mut self.parse_comma_separated(|p| {
                let key = p.parse_identifier(false)?;
                let value = p.parse_identifier(false)?;
                Ok(SecretOption { key, value })
            })?);
        }
        self.expect_token(&Token::RParen)?;

        let temp = match (temporary, persistent) {
            (true, false) => Some(true),
            (false, true) => Some(false),
            (false, false) => None,
            _ => self.expected("TEMPORARY or PERSISTENT", self.peek_token())?,
        };

        Ok(Statement::CreateSecret {
            or_replace,
            temporary: temp,
            if_not_exists,
            name,
            storage_specifier,
            secret_type,
            options,
        })
    }

    /// Parse a CACHE TABLE statement
    pub fn parse_cache_table(&mut self) -> Result<Statement, ParserError> {
        let (mut table_flag, mut options, mut has_as, mut query) = (None, vec![], false, None);
        if self.parse_keyword(Keyword::TABLE) {
            let table_name = self.parse_object_name(false)?;
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
            table_flag = Some(self.parse_object_name(false)?);
            if self.parse_keyword(Keyword::TABLE) {
                let table_name = self.parse_object_name(false)?;
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
        self.expect_keyword(Keyword::TABLE)?;
        let if_exists = self.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
        let table_name = self.parse_object_name(false)?;
        Ok(Statement::UNCache {
            table_name,
            if_exists,
        })
    }

    /// SQLite-specific `CREATE VIRTUAL TABLE`
    pub fn parse_create_virtual_table(&mut self) -> Result<Statement, ParserError> {
        self.expect_keyword(Keyword::TABLE)?;
        let if_not_exists = self.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let table_name = self.parse_object_name(false)?;
        self.expect_keyword(Keyword::USING)?;
        let module_name = self.parse_identifier(false)?;
        // SQLite docs note that module "arguments syntax is sufficiently
        // general that the arguments can be made to appear as column
        // definitions in a traditional CREATE TABLE statement", but
        // we don't implement that.
        let module_args = self.parse_parenthesized_column_list(Optional, false)?;
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
            Ok(SchemaName::UnnamedAuthorization(
                self.parse_identifier(false)?,
            ))
        } else {
            let name = self.parse_object_name(false)?;

            if self.parse_keyword(Keyword::AUTHORIZATION) {
                Ok(SchemaName::NamedAuthorization(
                    name,
                    self.parse_identifier(false)?,
                ))
            } else {
                Ok(SchemaName::Simple(name))
            }
        }
    }

    pub fn parse_create_database(&mut self) -> Result<Statement, ParserError> {
        let ine = self.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let db_name = self.parse_object_name(false)?;
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
                TokenWithLocation::wrap(Token::make_keyword(format!("{keyword:?}").as_str())),
            ),
        }
    }

    pub fn parse_create_function(
        &mut self,
        or_replace: bool,
        temporary: bool,
    ) -> Result<Statement, ParserError> {
        if dialect_of!(self is HiveDialect) {
            self.parse_hive_create_function(or_replace, temporary)
        } else if dialect_of!(self is PostgreSqlDialect | GenericDialect) {
            self.parse_postgres_create_function(or_replace, temporary)
        } else if dialect_of!(self is DuckDbDialect) {
            self.parse_create_macro(or_replace, temporary)
        } else if dialect_of!(self is BigQueryDialect) {
            self.parse_bigquery_create_function(or_replace, temporary)
        } else {
            self.prev_token();
            self.expected("an object type after CREATE", self.peek_token())
        }
    }

    /// Parse `CREATE FUNCTION` for [Postgres]
    ///
    /// [Postgres]: https://www.postgresql.org/docs/15/sql-createfunction.html
    fn parse_postgres_create_function(
        &mut self,
        or_replace: bool,
        temporary: bool,
    ) -> Result<Statement, ParserError> {
        let name = self.parse_object_name(false)?;
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

        #[derive(Default)]
        struct Body {
            language: Option<Ident>,
            behavior: Option<FunctionBehavior>,
            function_body: Option<CreateFunctionBody>,
            called_on_null: Option<FunctionCalledOnNull>,
            parallel: Option<FunctionParallel>,
        }
        let mut body = Body::default();
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
                ensure_not_set(&body.function_body, "AS")?;
                body.function_body = Some(CreateFunctionBody::AsBeforeOptions(
                    self.parse_create_function_body_string()?,
                ));
            } else if self.parse_keyword(Keyword::LANGUAGE) {
                ensure_not_set(&body.language, "LANGUAGE")?;
                body.language = Some(self.parse_identifier(false)?);
            } else if self.parse_keyword(Keyword::IMMUTABLE) {
                ensure_not_set(&body.behavior, "IMMUTABLE | STABLE | VOLATILE")?;
                body.behavior = Some(FunctionBehavior::Immutable);
            } else if self.parse_keyword(Keyword::STABLE) {
                ensure_not_set(&body.behavior, "IMMUTABLE | STABLE | VOLATILE")?;
                body.behavior = Some(FunctionBehavior::Stable);
            } else if self.parse_keyword(Keyword::VOLATILE) {
                ensure_not_set(&body.behavior, "IMMUTABLE | STABLE | VOLATILE")?;
                body.behavior = Some(FunctionBehavior::Volatile);
            } else if self.parse_keywords(&[
                Keyword::CALLED,
                Keyword::ON,
                Keyword::NULL,
                Keyword::INPUT,
            ]) {
                ensure_not_set(
                    &body.called_on_null,
                    "CALLED ON NULL INPUT | RETURNS NULL ON NULL INPUT | STRICT",
                )?;
                body.called_on_null = Some(FunctionCalledOnNull::CalledOnNullInput);
            } else if self.parse_keywords(&[
                Keyword::RETURNS,
                Keyword::NULL,
                Keyword::ON,
                Keyword::NULL,
                Keyword::INPUT,
            ]) {
                ensure_not_set(
                    &body.called_on_null,
                    "CALLED ON NULL INPUT | RETURNS NULL ON NULL INPUT | STRICT",
                )?;
                body.called_on_null = Some(FunctionCalledOnNull::ReturnsNullOnNullInput);
            } else if self.parse_keyword(Keyword::STRICT) {
                ensure_not_set(
                    &body.called_on_null,
                    "CALLED ON NULL INPUT | RETURNS NULL ON NULL INPUT | STRICT",
                )?;
                body.called_on_null = Some(FunctionCalledOnNull::Strict);
            } else if self.parse_keyword(Keyword::PARALLEL) {
                ensure_not_set(&body.parallel, "PARALLEL { UNSAFE | RESTRICTED | SAFE }")?;
                if self.parse_keyword(Keyword::UNSAFE) {
                    body.parallel = Some(FunctionParallel::Unsafe);
                } else if self.parse_keyword(Keyword::RESTRICTED) {
                    body.parallel = Some(FunctionParallel::Restricted);
                } else if self.parse_keyword(Keyword::SAFE) {
                    body.parallel = Some(FunctionParallel::Safe);
                } else {
                    return self.expected("one of UNSAFE | RESTRICTED | SAFE", self.peek_token());
                }
            } else if self.parse_keyword(Keyword::RETURN) {
                ensure_not_set(&body.function_body, "RETURN")?;
                body.function_body = Some(CreateFunctionBody::Return(self.parse_expr()?));
            } else {
                break;
            }
        }

        Ok(Statement::CreateFunction {
            or_replace,
            temporary,
            name,
            args,
            return_type,
            behavior: body.behavior,
            called_on_null: body.called_on_null,
            parallel: body.parallel,
            language: body.language,
            function_body: body.function_body,
            if_not_exists: false,
            using: None,
            determinism_specifier: None,
            options: None,
            remote_connection: None,
        })
    }

    /// Parse `CREATE FUNCTION` for [Hive]
    ///
    /// [Hive]: https://cwiki.apache.org/confluence/display/hive/languagemanual+ddl#LanguageManualDDL-Create/Drop/ReloadFunction
    fn parse_hive_create_function(
        &mut self,
        or_replace: bool,
        temporary: bool,
    ) -> Result<Statement, ParserError> {
        let name = self.parse_object_name(false)?;
        self.expect_keyword(Keyword::AS)?;

        let as_ = self.parse_create_function_body_string()?;
        let using = self.parse_optional_create_function_using()?;

        Ok(Statement::CreateFunction {
            or_replace,
            temporary,
            name,
            function_body: Some(CreateFunctionBody::AsBeforeOptions(as_)),
            using,
            if_not_exists: false,
            args: None,
            return_type: None,
            behavior: None,
            called_on_null: None,
            parallel: None,
            language: None,
            determinism_specifier: None,
            options: None,
            remote_connection: None,
        })
    }

    /// Parse `CREATE FUNCTION` for [BigQuery]
    ///
    /// [BigQuery]: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#create_function_statement
    fn parse_bigquery_create_function(
        &mut self,
        or_replace: bool,
        temporary: bool,
    ) -> Result<Statement, ParserError> {
        let if_not_exists = self.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let name = self.parse_object_name(false)?;

        let parse_function_param =
            |parser: &mut Parser| -> Result<OperateFunctionArg, ParserError> {
                let name = parser.parse_identifier(false)?;
                let data_type = parser.parse_data_type()?;
                Ok(OperateFunctionArg {
                    mode: None,
                    name: Some(name),
                    data_type,
                    default_expr: None,
                })
            };
        self.expect_token(&Token::LParen)?;
        let args = self.parse_comma_separated0(parse_function_param, Token::RParen)?;
        self.expect_token(&Token::RParen)?;

        let return_type = if self.parse_keyword(Keyword::RETURNS) {
            Some(self.parse_data_type()?)
        } else {
            None
        };

        let determinism_specifier = if self.parse_keyword(Keyword::DETERMINISTIC) {
            Some(FunctionDeterminismSpecifier::Deterministic)
        } else if self.parse_keywords(&[Keyword::NOT, Keyword::DETERMINISTIC]) {
            Some(FunctionDeterminismSpecifier::NotDeterministic)
        } else {
            None
        };

        let language = if self.parse_keyword(Keyword::LANGUAGE) {
            Some(self.parse_identifier(false)?)
        } else {
            None
        };

        let remote_connection =
            if self.parse_keywords(&[Keyword::REMOTE, Keyword::WITH, Keyword::CONNECTION]) {
                Some(self.parse_object_name(false)?)
            } else {
                None
            };

        // `OPTIONS` may come before of after the function body but
        // may be specified at most once.
        let mut options = self.maybe_parse_options(Keyword::OPTIONS)?;

        let function_body = if remote_connection.is_none() {
            self.expect_keyword(Keyword::AS)?;
            let expr = self.parse_expr()?;
            if options.is_none() {
                options = self.maybe_parse_options(Keyword::OPTIONS)?;
                Some(CreateFunctionBody::AsBeforeOptions(expr))
            } else {
                Some(CreateFunctionBody::AsAfterOptions(expr))
            }
        } else {
            None
        };

        Ok(Statement::CreateFunction {
            or_replace,
            temporary,
            if_not_exists,
            name,
            args: Some(args),
            return_type,
            function_body,
            language,
            determinism_specifier,
            options,
            remote_connection,
            using: None,
            behavior: None,
            called_on_null: None,
            parallel: None,
        })
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

    /// Parse statements of the DropTrigger type such as:
    ///
    /// ```sql
    /// DROP TRIGGER [ IF EXISTS ] name ON table_name [ CASCADE | RESTRICT ]
    /// ```
    pub fn parse_drop_trigger(&mut self) -> Result<Statement, ParserError> {
        if !dialect_of!(self is PostgreSqlDialect | GenericDialect) {
            self.prev_token();
            return self.expected("an object type after DROP", self.peek_token());
        }
        let if_exists = self.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
        let trigger_name = self.parse_object_name(false)?;
        self.expect_keyword(Keyword::ON)?;
        let table_name = self.parse_object_name(false)?;
        let option = self
            .parse_one_of_keywords(&[Keyword::CASCADE, Keyword::RESTRICT])
            .map(|keyword| match keyword {
                Keyword::CASCADE => ReferentialAction::Cascade,
                Keyword::RESTRICT => ReferentialAction::Restrict,
                _ => unreachable!(),
            });
        Ok(Statement::DropTrigger {
            if_exists,
            trigger_name,
            table_name,
            option,
        })
    }

    pub fn parse_create_trigger(
        &mut self,
        or_replace: bool,
        is_constraint: bool,
    ) -> Result<Statement, ParserError> {
        if !dialect_of!(self is PostgreSqlDialect | GenericDialect) {
            self.prev_token();
            return self.expected("an object type after CREATE", self.peek_token());
        }

        let name = self.parse_object_name(false)?;
        let period = self.parse_trigger_period()?;

        let events = self.parse_keyword_separated(Keyword::OR, Parser::parse_trigger_event)?;
        self.expect_keyword(Keyword::ON)?;
        let table_name = self.parse_object_name(false)?;

        let referenced_table_name = if self.parse_keyword(Keyword::FROM) {
            self.parse_object_name(true).ok()
        } else {
            None
        };

        let characteristics = self.parse_constraint_characteristics()?;

        let mut referencing = vec![];
        if self.parse_keyword(Keyword::REFERENCING) {
            while let Some(refer) = self.parse_trigger_referencing()? {
                referencing.push(refer);
            }
        }

        self.expect_keyword(Keyword::FOR)?;
        let include_each = self.parse_keyword(Keyword::EACH);
        let trigger_object =
            match self.expect_one_of_keywords(&[Keyword::ROW, Keyword::STATEMENT])? {
                Keyword::ROW => TriggerObject::Row,
                Keyword::STATEMENT => TriggerObject::Statement,
                _ => unreachable!(),
            };

        let condition = self
            .parse_keyword(Keyword::WHEN)
            .then(|| self.parse_expr())
            .transpose()?;

        self.expect_keyword(Keyword::EXECUTE)?;

        let exec_body = self.parse_trigger_exec_body()?;

        Ok(Statement::CreateTrigger {
            or_replace,
            is_constraint,
            name,
            period,
            events,
            table_name,
            referenced_table_name,
            referencing,
            trigger_object,
            include_each,
            condition,
            exec_body,
            characteristics,
        })
    }

    pub fn parse_trigger_period(&mut self) -> Result<TriggerPeriod, ParserError> {
        Ok(
            match self.expect_one_of_keywords(&[
                Keyword::BEFORE,
                Keyword::AFTER,
                Keyword::INSTEAD,
            ])? {
                Keyword::BEFORE => TriggerPeriod::Before,
                Keyword::AFTER => TriggerPeriod::After,
                Keyword::INSTEAD => self
                    .expect_keyword(Keyword::OF)
                    .map(|_| TriggerPeriod::InsteadOf)?,
                _ => unreachable!(),
            },
        )
    }

    pub fn parse_trigger_event(&mut self) -> Result<TriggerEvent, ParserError> {
        Ok(
            match self.expect_one_of_keywords(&[
                Keyword::INSERT,
                Keyword::UPDATE,
                Keyword::DELETE,
                Keyword::TRUNCATE,
            ])? {
                Keyword::INSERT => TriggerEvent::Insert,
                Keyword::UPDATE => {
                    if self.parse_keyword(Keyword::OF) {
                        let cols = self.parse_comma_separated(|ident| {
                            Parser::parse_identifier(ident, false)
                        })?;
                        TriggerEvent::Update(cols)
                    } else {
                        TriggerEvent::Update(vec![])
                    }
                }
                Keyword::DELETE => TriggerEvent::Delete,
                Keyword::TRUNCATE => TriggerEvent::Truncate,
                _ => unreachable!(),
            },
        )
    }

    pub fn parse_trigger_referencing(&mut self) -> Result<Option<TriggerReferencing>, ParserError> {
        let refer_type = match self.parse_one_of_keywords(&[Keyword::OLD, Keyword::NEW]) {
            Some(Keyword::OLD) if self.parse_keyword(Keyword::TABLE) => {
                TriggerReferencingType::OldTable
            }
            Some(Keyword::NEW) if self.parse_keyword(Keyword::TABLE) => {
                TriggerReferencingType::NewTable
            }
            _ => {
                return Ok(None);
            }
        };

        let is_as = self.parse_keyword(Keyword::AS);
        let transition_relation_name = self.parse_object_name(false)?;
        Ok(Some(TriggerReferencing {
            refer_type,
            is_as,
            transition_relation_name,
        }))
    }

    pub fn parse_trigger_exec_body(&mut self) -> Result<TriggerExecBody, ParserError> {
        Ok(TriggerExecBody {
            exec_type: match self
                .expect_one_of_keywords(&[Keyword::FUNCTION, Keyword::PROCEDURE])?
            {
                Keyword::FUNCTION => TriggerExecBodyType::Function,
                Keyword::PROCEDURE => TriggerExecBodyType::Procedure,
                _ => unreachable!(),
            },
            func_desc: self.parse_function_desc()?,
        })
    }

    pub fn parse_create_macro(
        &mut self,
        or_replace: bool,
        temporary: bool,
    ) -> Result<Statement, ParserError> {
        if dialect_of!(self is DuckDbDialect |  GenericDialect) {
            let name = self.parse_object_name(false)?;
            self.expect_token(&Token::LParen)?;
            let args = if self.consume_token(&Token::RParen) {
                self.prev_token();
                None
            } else {
                Some(self.parse_comma_separated(Parser::parse_macro_arg)?)
            };

            self.expect_token(&Token::RParen)?;
            self.expect_keyword(Keyword::AS)?;

            Ok(Statement::CreateMacro {
                or_replace,
                temporary,
                name,
                args,
                definition: if self.parse_keyword(Keyword::TABLE) {
                    MacroDefinition::Table(self.parse_query()?)
                } else {
                    MacroDefinition::Expr(self.parse_expr()?)
                },
            })
        } else {
            self.prev_token();
            self.expected("an object type after CREATE", self.peek_token())
        }
    }

    fn parse_macro_arg(&mut self) -> Result<MacroArg, ParserError> {
        let name = self.parse_identifier(false)?;

        let default_expr =
            if self.consume_token(&Token::Assignment) || self.consume_token(&Token::RArrow) {
                Some(self.parse_expr()?)
            } else {
                None
            };
        Ok(MacroArg { name, default_expr })
    }

    pub fn parse_create_external_table(
        &mut self,
        or_replace: bool,
    ) -> Result<Statement, ParserError> {
        self.expect_keyword(Keyword::TABLE)?;
        let if_not_exists = self.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let table_name = self.parse_object_name(false)?;
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

    pub fn parse_create_view(
        &mut self,
        or_replace: bool,
        temporary: bool,
    ) -> Result<Statement, ParserError> {
        let materialized = self.parse_keyword(Keyword::MATERIALIZED);
        self.expect_keyword(Keyword::VIEW)?;
        let if_not_exists = dialect_of!(self is BigQueryDialect|SQLiteDialect|GenericDialect)
            && self.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        // Many dialects support `OR ALTER` right after `CREATE`, but we don't (yet).
        // ANSI SQL and Postgres support RECURSIVE here, but we don't support it either.
        let allow_unquoted_hyphen = dialect_of!(self is BigQueryDialect);
        let name = self.parse_object_name(allow_unquoted_hyphen)?;
        let columns = self.parse_view_columns()?;
        let mut options = CreateTableOptions::None;
        let with_options = self.parse_options(Keyword::WITH)?;
        if !with_options.is_empty() {
            options = CreateTableOptions::With(with_options);
        }

        let cluster_by = if self.parse_keyword(Keyword::CLUSTER) {
            self.expect_keyword(Keyword::BY)?;
            self.parse_parenthesized_column_list(Optional, false)?
        } else {
            vec![]
        };

        if dialect_of!(self is BigQueryDialect | GenericDialect) {
            if let Some(opts) = self.maybe_parse_options(Keyword::OPTIONS)? {
                if !opts.is_empty() {
                    options = CreateTableOptions::Options(opts);
                }
            };
        }

        let to = if dialect_of!(self is ClickHouseDialect | GenericDialect)
            && self.parse_keyword(Keyword::TO)
        {
            Some(self.parse_object_name(false)?)
        } else {
            None
        };

        let comment = if dialect_of!(self is SnowflakeDialect | GenericDialect)
            && self.parse_keyword(Keyword::COMMENT)
        {
            self.expect_token(&Token::Eq)?;
            let next_token = self.next_token();
            match next_token.token {
                Token::SingleQuotedString(str) => Some(str),
                _ => self.expected("string literal", next_token)?,
            }
        } else {
            None
        };

        self.expect_keyword(Keyword::AS)?;
        let query = self.parse_boxed_query()?;
        // Optional `WITH [ CASCADED | LOCAL ] CHECK OPTION` is widely supported here.

        let with_no_schema_binding = dialect_of!(self is RedshiftSqlDialect | GenericDialect)
            && self.parse_keywords(&[
                Keyword::WITH,
                Keyword::NO,
                Keyword::SCHEMA,
                Keyword::BINDING,
            ]);

        Ok(Statement::CreateView {
            name,
            columns,
            query,
            materialized,
            or_replace,
            options,
            cluster_by,
            comment,
            with_no_schema_binding,
            if_not_exists,
            temporary,
            to,
        })
    }

    pub fn parse_create_role(&mut self) -> Result<Statement, ParserError> {
        let if_not_exists = self.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let names = self.parse_comma_separated(|p| p.parse_object_name(false))?;

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
            let loc = self
                .tokens
                .get(self.index - 1)
                .map_or(Location { line: 0, column: 0 }, |t| t.location);
            match keyword {
                Keyword::AUTHORIZATION => {
                    if authorization_owner.is_some() {
                        parser_err!("Found multiple AUTHORIZATION", loc)
                    } else {
                        authorization_owner = Some(self.parse_object_name(false)?);
                        Ok(())
                    }
                }
                Keyword::LOGIN | Keyword::NOLOGIN => {
                    if login.is_some() {
                        parser_err!("Found multiple LOGIN or NOLOGIN", loc)
                    } else {
                        login = Some(keyword == Keyword::LOGIN);
                        Ok(())
                    }
                }
                Keyword::INHERIT | Keyword::NOINHERIT => {
                    if inherit.is_some() {
                        parser_err!("Found multiple INHERIT or NOINHERIT", loc)
                    } else {
                        inherit = Some(keyword == Keyword::INHERIT);
                        Ok(())
                    }
                }
                Keyword::BYPASSRLS | Keyword::NOBYPASSRLS => {
                    if bypassrls.is_some() {
                        parser_err!("Found multiple BYPASSRLS or NOBYPASSRLS", loc)
                    } else {
                        bypassrls = Some(keyword == Keyword::BYPASSRLS);
                        Ok(())
                    }
                }
                Keyword::CREATEDB | Keyword::NOCREATEDB => {
                    if create_db.is_some() {
                        parser_err!("Found multiple CREATEDB or NOCREATEDB", loc)
                    } else {
                        create_db = Some(keyword == Keyword::CREATEDB);
                        Ok(())
                    }
                }
                Keyword::CREATEROLE | Keyword::NOCREATEROLE => {
                    if create_role.is_some() {
                        parser_err!("Found multiple CREATEROLE or NOCREATEROLE", loc)
                    } else {
                        create_role = Some(keyword == Keyword::CREATEROLE);
                        Ok(())
                    }
                }
                Keyword::SUPERUSER | Keyword::NOSUPERUSER => {
                    if superuser.is_some() {
                        parser_err!("Found multiple SUPERUSER or NOSUPERUSER", loc)
                    } else {
                        superuser = Some(keyword == Keyword::SUPERUSER);
                        Ok(())
                    }
                }
                Keyword::REPLICATION | Keyword::NOREPLICATION => {
                    if replication.is_some() {
                        parser_err!("Found multiple REPLICATION or NOREPLICATION", loc)
                    } else {
                        replication = Some(keyword == Keyword::REPLICATION);
                        Ok(())
                    }
                }
                Keyword::PASSWORD => {
                    if password.is_some() {
                        parser_err!("Found multiple PASSWORD", loc)
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
                        parser_err!("Found multiple CONNECTION LIMIT", loc)
                    } else {
                        connection_limit = Some(Expr::Value(self.parse_number_value()?));
                        Ok(())
                    }
                }
                Keyword::VALID => {
                    self.expect_keyword(Keyword::UNTIL)?;
                    if valid_until.is_some() {
                        parser_err!("Found multiple VALID UNTIL", loc)
                    } else {
                        valid_until = Some(Expr::Value(self.parse_value()?));
                        Ok(())
                    }
                }
                Keyword::IN => {
                    if self.parse_keyword(Keyword::ROLE) {
                        if !in_role.is_empty() {
                            parser_err!("Found multiple IN ROLE", loc)
                        } else {
                            in_role = self.parse_comma_separated(|p| p.parse_identifier(false))?;
                            Ok(())
                        }
                    } else if self.parse_keyword(Keyword::GROUP) {
                        if !in_group.is_empty() {
                            parser_err!("Found multiple IN GROUP", loc)
                        } else {
                            in_group = self.parse_comma_separated(|p| p.parse_identifier(false))?;
                            Ok(())
                        }
                    } else {
                        self.expected("ROLE or GROUP after IN", self.peek_token())
                    }
                }
                Keyword::ROLE => {
                    if !role.is_empty() {
                        parser_err!("Found multiple ROLE", loc)
                    } else {
                        role = self.parse_comma_separated(|p| p.parse_identifier(false))?;
                        Ok(())
                    }
                }
                Keyword::USER => {
                    if !user.is_empty() {
                        parser_err!("Found multiple USER", loc)
                    } else {
                        user = self.parse_comma_separated(|p| p.parse_identifier(false))?;
                        Ok(())
                    }
                }
                Keyword::ADMIN => {
                    if !admin.is_empty() {
                        parser_err!("Found multiple ADMIN", loc)
                    } else {
                        admin = self.parse_comma_separated(|p| p.parse_identifier(false))?;
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
        // MySQL dialect supports `TEMPORARY`
        let temporary = dialect_of!(self is MySqlDialect | GenericDialect | DuckDbDialect)
            && self.parse_keyword(Keyword::TEMPORARY);
        let persistent = dialect_of!(self is DuckDbDialect)
            && self.parse_one_of_keywords(&[Keyword::PERSISTENT]).is_some();

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
        } else if self.parse_keyword(Keyword::STAGE) {
            ObjectType::Stage
        } else if self.parse_keyword(Keyword::FUNCTION) {
            return self.parse_drop_function();
        } else if self.parse_keyword(Keyword::PROCEDURE) {
            return self.parse_drop_procedure();
        } else if self.parse_keyword(Keyword::SECRET) {
            return self.parse_drop_secret(temporary, persistent);
        } else if self.parse_keyword(Keyword::TRIGGER) {
            return self.parse_drop_trigger();
        } else {
            return self.expected(
                "TABLE, VIEW, INDEX, ROLE, SCHEMA, FUNCTION, PROCEDURE, STAGE, TRIGGER, SECRET or SEQUENCE after DROP",
                self.peek_token(),
            );
        };
        // Many dialects support the non-standard `IF EXISTS` clause and allow
        // specifying multiple objects to delete in a single statement
        let if_exists = self.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
        let names = self.parse_comma_separated(|p| p.parse_object_name(false))?;

        let loc = self.peek_token().location;
        let cascade = self.parse_keyword(Keyword::CASCADE);
        let restrict = self.parse_keyword(Keyword::RESTRICT);
        let purge = self.parse_keyword(Keyword::PURGE);
        if cascade && restrict {
            return parser_err!("Cannot specify both CASCADE and RESTRICT in DROP", loc);
        }
        if object_type == ObjectType::Role && (cascade || restrict || purge) {
            return parser_err!(
                "Cannot specify CASCADE, RESTRICT, or PURGE in DROP ROLE",
                loc
            );
        }
        Ok(Statement::Drop {
            object_type,
            if_exists,
            names,
            cascade,
            restrict,
            purge,
            temporary,
        })
    }

    /// ```sql
    /// DROP FUNCTION [ IF EXISTS ] name [ ( [ [ argmode ] [ argname ] argtype [, ...] ] ) ] [, ...]
    /// [ CASCADE | RESTRICT ]
    /// ```
    fn parse_drop_function(&mut self) -> Result<Statement, ParserError> {
        let if_exists = self.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
        let func_desc = self.parse_comma_separated(Parser::parse_function_desc)?;
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

    /// ```sql
    /// DROP PROCEDURE [ IF EXISTS ] name [ ( [ [ argmode ] [ argname ] argtype [, ...] ] ) ] [, ...]
    /// [ CASCADE | RESTRICT ]
    /// ```
    fn parse_drop_procedure(&mut self) -> Result<Statement, ParserError> {
        let if_exists = self.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
        let proc_desc = self.parse_comma_separated(Parser::parse_function_desc)?;
        let option = match self.parse_one_of_keywords(&[Keyword::CASCADE, Keyword::RESTRICT]) {
            Some(Keyword::CASCADE) => Some(ReferentialAction::Cascade),
            Some(Keyword::RESTRICT) => Some(ReferentialAction::Restrict),
            Some(_) => unreachable!(), // parse_one_of_keywords does not return other keywords
            None => None,
        };
        Ok(Statement::DropProcedure {
            if_exists,
            proc_desc,
            option,
        })
    }

    fn parse_function_desc(&mut self) -> Result<FunctionDesc, ParserError> {
        let name = self.parse_object_name(false)?;

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

        Ok(FunctionDesc { name, args })
    }

    /// See [DuckDB Docs](https://duckdb.org/docs/sql/statements/create_secret.html) for more details.
    fn parse_drop_secret(
        &mut self,
        temporary: bool,
        persistent: bool,
    ) -> Result<Statement, ParserError> {
        let if_exists = self.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
        let name = self.parse_identifier(false)?;
        let storage_specifier = if self.parse_keyword(Keyword::FROM) {
            self.parse_identifier(false).ok()
        } else {
            None
        };
        let temp = match (temporary, persistent) {
            (true, false) => Some(true),
            (false, true) => Some(false),
            (false, false) => None,
            _ => self.expected("TEMPORARY or PERSISTENT", self.peek_token())?,
        };

        Ok(Statement::DropSecret {
            if_exists,
            temporary: temp,
            name,
            storage_specifier,
        })
    }

    /// Parse a `DECLARE` statement.
    ///
    /// ```sql
    /// DECLARE name [ BINARY ] [ ASENSITIVE | INSENSITIVE ] [ [ NO ] SCROLL ]
    ///     CURSOR [ { WITH | WITHOUT } HOLD ] FOR query
    /// ```
    ///
    /// The syntax can vary significantly between warehouses. See the grammar
    /// on the warehouse specific function in such cases.
    pub fn parse_declare(&mut self) -> Result<Statement, ParserError> {
        if dialect_of!(self is BigQueryDialect) {
            return self.parse_big_query_declare();
        }
        if dialect_of!(self is SnowflakeDialect) {
            return self.parse_snowflake_declare();
        }
        if dialect_of!(self is MsSqlDialect) {
            return self.parse_mssql_declare();
        }

        let name = self.parse_identifier(false)?;

        let binary = Some(self.parse_keyword(Keyword::BINARY));
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
        let declare_type = Some(DeclareType::Cursor);

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

        let query = Some(self.parse_boxed_query()?);

        Ok(Statement::Declare {
            stmts: vec![Declare {
                names: vec![name],
                data_type: None,
                assignment: None,
                declare_type,
                binary,
                sensitive,
                scroll,
                hold,
                for_query: query,
            }],
        })
    }

    /// Parse a [BigQuery] `DECLARE` statement.
    ///
    /// Syntax:
    /// ```text
    /// DECLARE variable_name[, ...] [{ <variable_type> | <DEFAULT expression> }];
    /// ```
    /// [BigQuery]: https://cloud.google.com/bigquery/docs/reference/standard-sql/procedural-language#declare
    pub fn parse_big_query_declare(&mut self) -> Result<Statement, ParserError> {
        let names = self.parse_comma_separated(|parser| Parser::parse_identifier(parser, false))?;

        let data_type = match self.peek_token().token {
            Token::Word(w) if w.keyword == Keyword::DEFAULT => None,
            _ => Some(self.parse_data_type()?),
        };

        let expr = if data_type.is_some() {
            if self.parse_keyword(Keyword::DEFAULT) {
                Some(self.parse_expr()?)
            } else {
                None
            }
        } else {
            // If no variable type - default expression must be specified, per BQ docs.
            // i.e `DECLARE foo;` is invalid.
            self.expect_keyword(Keyword::DEFAULT)?;
            Some(self.parse_expr()?)
        };

        Ok(Statement::Declare {
            stmts: vec![Declare {
                names,
                data_type,
                assignment: expr.map(|expr| DeclareAssignment::Default(Box::new(expr))),
                declare_type: None,
                binary: None,
                sensitive: None,
                scroll: None,
                hold: None,
                for_query: None,
            }],
        })
    }

    /// Parse a [Snowflake] `DECLARE` statement.
    ///
    /// Syntax:
    /// ```text
    /// DECLARE
    ///   [{ <variable_declaration>
    ///      | <cursor_declaration>
    ///      | <resultset_declaration>
    ///      | <exception_declaration> }; ... ]
    ///
    /// <variable_declaration>
    /// <variable_name> [<type>] [ { DEFAULT | := } <expression>]
    ///
    /// <cursor_declaration>
    /// <cursor_name> CURSOR FOR <query>
    ///
    /// <resultset_declaration>
    /// <resultset_name> RESULTSET [ { DEFAULT | := } ( <query> ) ] ;
    ///
    /// <exception_declaration>
    /// <exception_name> EXCEPTION [ ( <exception_number> , '<exception_message>' ) ] ;
    /// ```
    ///
    /// [Snowflake]: https://docs.snowflake.com/en/sql-reference/snowflake-scripting/declare
    pub fn parse_snowflake_declare(&mut self) -> Result<Statement, ParserError> {
        let mut stmts = vec![];
        loop {
            let name = self.parse_identifier(false)?;
            let (declare_type, for_query, assigned_expr, data_type) =
                if self.parse_keyword(Keyword::CURSOR) {
                    self.expect_keyword(Keyword::FOR)?;
                    match self.peek_token().token {
                        Token::Word(w) if w.keyword == Keyword::SELECT => (
                            Some(DeclareType::Cursor),
                            Some(self.parse_boxed_query()?),
                            None,
                            None,
                        ),
                        _ => (
                            Some(DeclareType::Cursor),
                            None,
                            Some(DeclareAssignment::For(Box::new(self.parse_expr()?))),
                            None,
                        ),
                    }
                } else if self.parse_keyword(Keyword::RESULTSET) {
                    let assigned_expr = if self.peek_token().token != Token::SemiColon {
                        self.parse_snowflake_variable_declaration_expression()?
                    } else {
                        // Nothing more to do. The statement has no further parameters.
                        None
                    };

                    (Some(DeclareType::ResultSet), None, assigned_expr, None)
                } else if self.parse_keyword(Keyword::EXCEPTION) {
                    let assigned_expr = if self.peek_token().token == Token::LParen {
                        Some(DeclareAssignment::Expr(Box::new(self.parse_expr()?)))
                    } else {
                        // Nothing more to do. The statement has no further parameters.
                        None
                    };

                    (Some(DeclareType::Exception), None, assigned_expr, None)
                } else {
                    // Without an explicit keyword, the only valid option is variable declaration.
                    let (assigned_expr, data_type) = if let Some(assigned_expr) =
                        self.parse_snowflake_variable_declaration_expression()?
                    {
                        (Some(assigned_expr), None)
                    } else if let Token::Word(_) = self.peek_token().token {
                        let data_type = self.parse_data_type()?;
                        (
                            self.parse_snowflake_variable_declaration_expression()?,
                            Some(data_type),
                        )
                    } else {
                        (None, None)
                    };
                    (None, None, assigned_expr, data_type)
                };
            let stmt = Declare {
                names: vec![name],
                data_type,
                assignment: assigned_expr,
                declare_type,
                binary: None,
                sensitive: None,
                scroll: None,
                hold: None,
                for_query,
            };

            stmts.push(stmt);
            if self.consume_token(&Token::SemiColon) {
                match self.peek_token().token {
                    Token::Word(w)
                        if ALL_KEYWORDS
                            .binary_search(&w.value.to_uppercase().as_str())
                            .is_err() =>
                    {
                        // Not a keyword - start of a new declaration.
                        continue;
                    }
                    _ => {
                        // Put back the semicolon, this is the end of the DECLARE statement.
                        self.prev_token();
                    }
                }
            }

            break;
        }

        Ok(Statement::Declare { stmts })
    }

    /// Parse a [MsSql] `DECLARE` statement.
    ///
    /// Syntax:
    /// ```text
    /// DECLARE
    // {
    //   { @local_variable [AS] data_type [ = value ] }
    //   | { @cursor_variable_name CURSOR }
    // } [ ,...n ]
    /// ```
    /// [MsSql]: https://learn.microsoft.com/en-us/sql/t-sql/language-elements/declare-local-variable-transact-sql?view=sql-server-ver16
    pub fn parse_mssql_declare(&mut self) -> Result<Statement, ParserError> {
        let mut stmts = vec![];

        loop {
            let name = {
                let ident = self.parse_identifier(false)?;
                if !ident.value.starts_with('@') {
                    Err(ParserError::TokenizerError(
                        "Invalid MsSql variable declaration.".to_string(),
                    ))
                } else {
                    Ok(ident)
                }
            }?;

            let (declare_type, data_type) = match self.peek_token().token {
                Token::Word(w) => match w.keyword {
                    Keyword::CURSOR => {
                        self.next_token();
                        (Some(DeclareType::Cursor), None)
                    }
                    Keyword::AS => {
                        self.next_token();
                        (None, Some(self.parse_data_type()?))
                    }
                    _ => (None, Some(self.parse_data_type()?)),
                },
                _ => (None, Some(self.parse_data_type()?)),
            };

            let assignment = self.parse_mssql_variable_declaration_expression()?;

            stmts.push(Declare {
                names: vec![name],
                data_type,
                assignment,
                declare_type,
                binary: None,
                sensitive: None,
                scroll: None,
                hold: None,
                for_query: None,
            });

            if self.next_token() != Token::Comma {
                break;
            }
        }

        Ok(Statement::Declare { stmts })
    }

    /// Parses the assigned expression in a variable declaration.
    ///
    /// Syntax:
    /// ```text
    /// [ { DEFAULT | := } <expression>]
    /// ```
    /// <https://docs.snowflake.com/en/sql-reference/snowflake-scripting/declare#variable-declaration-syntax>
    pub fn parse_snowflake_variable_declaration_expression(
        &mut self,
    ) -> Result<Option<DeclareAssignment>, ParserError> {
        Ok(match self.peek_token().token {
            Token::Word(w) if w.keyword == Keyword::DEFAULT => {
                self.next_token(); // Skip `DEFAULT`
                Some(DeclareAssignment::Default(Box::new(self.parse_expr()?)))
            }
            Token::Assignment => {
                self.next_token(); // Skip `:=`
                Some(DeclareAssignment::DuckAssignment(Box::new(
                    self.parse_expr()?,
                )))
            }
            _ => None,
        })
    }

    /// Parses the assigned expression in a variable declaration.
    ///
    /// Syntax:
    /// ```text
    /// [ = <expression>]
    /// ```
    pub fn parse_mssql_variable_declaration_expression(
        &mut self,
    ) -> Result<Option<DeclareAssignment>, ParserError> {
        Ok(match self.peek_token().token {
            Token::Eq => {
                self.next_token(); // Skip `=`
                Some(DeclareAssignment::MsSqlAssignment(Box::new(
                    self.parse_expr()?,
                )))
            }
            _ => None,
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

        let name = self.parse_identifier(false)?;

        let into = if self.parse_keyword(Keyword::INTO) {
            Some(self.parse_object_name(false)?)
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
        let concurrently = self.parse_keyword(Keyword::CONCURRENTLY);
        let if_not_exists = self.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let index_name = if if_not_exists || !self.parse_keyword(Keyword::ON) {
            let index_name = self.parse_object_name(false)?;
            self.expect_keyword(Keyword::ON)?;
            Some(index_name)
        } else {
            None
        };
        let table_name = self.parse_object_name(false)?;
        let using = if self.parse_keyword(Keyword::USING) {
            Some(self.parse_identifier(false)?)
        } else {
            None
        };
        self.expect_token(&Token::LParen)?;
        let columns = self.parse_comma_separated(Parser::parse_order_by_expr)?;
        self.expect_token(&Token::RParen)?;

        let include = if self.parse_keyword(Keyword::INCLUDE) {
            self.expect_token(&Token::LParen)?;
            let columns = self.parse_comma_separated(|p| p.parse_identifier(false))?;
            self.expect_token(&Token::RParen)?;
            columns
        } else {
            vec![]
        };

        let nulls_distinct = if self.parse_keyword(Keyword::NULLS) {
            let not = self.parse_keyword(Keyword::NOT);
            self.expect_keyword(Keyword::DISTINCT)?;
            Some(!not)
        } else {
            None
        };

        let predicate = if self.parse_keyword(Keyword::WHERE) {
            Some(self.parse_expr()?)
        } else {
            None
        };

        Ok(Statement::CreateIndex(CreateIndex {
            name: index_name,
            table_name,
            using,
            columns,
            unique,
            concurrently,
            if_not_exists,
            include,
            nulls_distinct,
            predicate,
        }))
    }

    pub fn parse_create_extension(&mut self) -> Result<Statement, ParserError> {
        let if_not_exists = self.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let name = self.parse_identifier(false)?;

        let (schema, version, cascade) = if self.parse_keyword(Keyword::WITH) {
            let schema = if self.parse_keyword(Keyword::SCHEMA) {
                Some(self.parse_identifier(false)?)
            } else {
                None
            };

            let version = if self.parse_keyword(Keyword::VERSION) {
                Some(self.parse_identifier(false)?)
            } else {
                None
            };

            let cascade = self.parse_keyword(Keyword::CASCADE);

            (schema, version, cascade)
        } else {
            (None, None, false)
        };

        Ok(Statement::CreateExtension {
            name,
            if_not_exists,
            schema,
            version,
            cascade,
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
            match self.parse_one_of_keywords(&[
                Keyword::ROW,
                Keyword::STORED,
                Keyword::LOCATION,
                Keyword::WITH,
            ]) {
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
                Some(Keyword::WITH) => {
                    self.prev_token();
                    let properties = self
                        .parse_options_with_keywords(&[Keyword::WITH, Keyword::SERDEPROPERTIES])?;
                    if !properties.is_empty() {
                        hive_format.serde_properties = Some(properties);
                    } else {
                        break;
                    }
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
            _ => {
                let mut row_delimiters = vec![];

                loop {
                    match self.parse_one_of_keywords(&[
                        Keyword::FIELDS,
                        Keyword::COLLECTION,
                        Keyword::MAP,
                        Keyword::LINES,
                        Keyword::NULL,
                    ]) {
                        Some(Keyword::FIELDS) => {
                            if self.parse_keywords(&[Keyword::TERMINATED, Keyword::BY]) {
                                row_delimiters.push(HiveRowDelimiter {
                                    delimiter: HiveDelimiter::FieldsTerminatedBy,
                                    char: self.parse_identifier(false)?,
                                });

                                if self.parse_keywords(&[Keyword::ESCAPED, Keyword::BY]) {
                                    row_delimiters.push(HiveRowDelimiter {
                                        delimiter: HiveDelimiter::FieldsEscapedBy,
                                        char: self.parse_identifier(false)?,
                                    });
                                }
                            } else {
                                break;
                            }
                        }
                        Some(Keyword::COLLECTION) => {
                            if self.parse_keywords(&[
                                Keyword::ITEMS,
                                Keyword::TERMINATED,
                                Keyword::BY,
                            ]) {
                                row_delimiters.push(HiveRowDelimiter {
                                    delimiter: HiveDelimiter::CollectionItemsTerminatedBy,
                                    char: self.parse_identifier(false)?,
                                });
                            } else {
                                break;
                            }
                        }
                        Some(Keyword::MAP) => {
                            if self.parse_keywords(&[
                                Keyword::KEYS,
                                Keyword::TERMINATED,
                                Keyword::BY,
                            ]) {
                                row_delimiters.push(HiveRowDelimiter {
                                    delimiter: HiveDelimiter::MapKeysTerminatedBy,
                                    char: self.parse_identifier(false)?,
                                });
                            } else {
                                break;
                            }
                        }
                        Some(Keyword::LINES) => {
                            if self.parse_keywords(&[Keyword::TERMINATED, Keyword::BY]) {
                                row_delimiters.push(HiveRowDelimiter {
                                    delimiter: HiveDelimiter::LinesTerminatedBy,
                                    char: self.parse_identifier(false)?,
                                });
                            } else {
                                break;
                            }
                        }
                        Some(Keyword::NULL) => {
                            if self.parse_keywords(&[Keyword::DEFINED, Keyword::AS]) {
                                row_delimiters.push(HiveRowDelimiter {
                                    delimiter: HiveDelimiter::NullDefinedAs,
                                    char: self.parse_identifier(false)?,
                                });
                            } else {
                                break;
                            }
                        }
                        _ => {
                            break;
                        }
                    }
                }

                Ok(HiveRowFormat::DELIMITED {
                    delimiters: row_delimiters,
                })
            }
        }
    }

    fn parse_optional_on_cluster(&mut self) -> Result<Option<Ident>, ParserError> {
        if self.parse_keywords(&[Keyword::ON, Keyword::CLUSTER]) {
            Ok(Some(self.parse_identifier(false)?))
        } else {
            Ok(None)
        }
    }

    pub fn parse_create_table(
        &mut self,
        or_replace: bool,
        temporary: bool,
        global: Option<bool>,
        transient: bool,
    ) -> Result<Statement, ParserError> {
        let allow_unquoted_hyphen = dialect_of!(self is BigQueryDialect);
        let if_not_exists = self.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let table_name = self.parse_object_name(allow_unquoted_hyphen)?;

        // Clickhouse has `ON CLUSTER 'cluster'` syntax for DDLs
        let on_cluster = self.parse_optional_on_cluster()?;

        let like = if self.parse_keyword(Keyword::LIKE) || self.parse_keyword(Keyword::ILIKE) {
            self.parse_object_name(allow_unquoted_hyphen).ok()
        } else {
            None
        };

        let clone = if self.parse_keyword(Keyword::CLONE) {
            self.parse_object_name(allow_unquoted_hyphen).ok()
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

        let engine = if self.parse_keyword(Keyword::ENGINE) {
            self.expect_token(&Token::Eq)?;
            let next_token = self.next_token();
            match next_token.token {
                Token::Word(w) => {
                    let name = w.value;
                    let parameters = if self.peek_token() == Token::LParen {
                        Some(self.parse_parenthesized_identifiers()?)
                    } else {
                        None
                    };
                    Some(TableEngine { name, parameters })
                }
                _ => self.expected("identifier", next_token)?,
            }
        } else {
            None
        };

        let auto_increment_offset = if self.parse_keyword(Keyword::AUTO_INCREMENT) {
            let _ = self.consume_token(&Token::Eq);
            let next_token = self.next_token();
            match next_token.token {
                Token::Number(s, _) => Some(Self::parse::<u32>(s, next_token.location)?),
                _ => self.expected("literal int", next_token)?,
            }
        } else {
            None
        };

        // ClickHouse supports `PRIMARY KEY`, before `ORDER BY`
        // https://clickhouse.com/docs/en/sql-reference/statements/create/table#primary-key
        let primary_key = if dialect_of!(self is ClickHouseDialect | GenericDialect)
            && self.parse_keywords(&[Keyword::PRIMARY, Keyword::KEY])
        {
            Some(Box::new(self.parse_expr()?))
        } else {
            None
        };

        let order_by = if self.parse_keywords(&[Keyword::ORDER, Keyword::BY]) {
            if self.consume_token(&Token::LParen) {
                let columns = if self.peek_token() != Token::RParen {
                    self.parse_comma_separated(|p| p.parse_expr())?
                } else {
                    vec![]
                };
                self.expect_token(&Token::RParen)?;
                Some(OneOrManyWithParens::Many(columns))
            } else {
                Some(OneOrManyWithParens::One(self.parse_expr()?))
            }
        } else {
            None
        };

        let create_table_config = self.parse_optional_create_table_config()?;

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

        let strict = self.parse_keyword(Keyword::STRICT);

        let comment = if self.parse_keyword(Keyword::COMMENT) {
            let _ = self.consume_token(&Token::Eq);
            let next_token = self.next_token();
            match next_token.token {
                Token::SingleQuotedString(str) => Some(CommentDef::WithoutEq(str)),
                _ => self.expected("comment", next_token)?,
            }
        } else {
            None
        };

        // Parse optional `AS ( query )`
        let query = if self.parse_keyword(Keyword::AS) {
            Some(self.parse_boxed_query()?)
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
            .transient(transient)
            .hive_distribution(hive_distribution)
            .hive_formats(Some(hive_formats))
            .global(global)
            .query(query)
            .without_rowid(without_rowid)
            .like(like)
            .clone_clause(clone)
            .engine(engine)
            .comment(comment)
            .auto_increment_offset(auto_increment_offset)
            .order_by(order_by)
            .default_charset(default_charset)
            .collation(collation)
            .on_commit(on_commit)
            .on_cluster(on_cluster)
            .partition_by(create_table_config.partition_by)
            .cluster_by(create_table_config.cluster_by)
            .options(create_table_config.options)
            .primary_key(primary_key)
            .strict(strict)
            .build())
    }

    /// Parse configuration like partitioning, clustering information during the table creation.
    ///
    /// [BigQuery](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#syntax_2)
    /// [PostgreSQL](https://www.postgresql.org/docs/current/ddl-partitioning.html)
    fn parse_optional_create_table_config(
        &mut self,
    ) -> Result<CreateTableConfiguration, ParserError> {
        let partition_by = if dialect_of!(self is BigQueryDialect | PostgreSqlDialect | GenericDialect)
            && self.parse_keywords(&[Keyword::PARTITION, Keyword::BY])
        {
            Some(Box::new(self.parse_expr()?))
        } else {
            None
        };

        let mut cluster_by = None;
        let mut options = None;
        if dialect_of!(self is BigQueryDialect | GenericDialect) {
            if self.parse_keywords(&[Keyword::CLUSTER, Keyword::BY]) {
                cluster_by = Some(WrappedCollection::NoWrapping(
                    self.parse_comma_separated(|p| p.parse_identifier(false))?,
                ));
            };

            if let Token::Word(word) = self.peek_token().token {
                if word.keyword == Keyword::OPTIONS {
                    options = Some(self.parse_options(Keyword::OPTIONS)?);
                }
            };
        }

        Ok(CreateTableConfiguration {
            partition_by,
            cluster_by,
            options,
        })
    }

    pub fn parse_optional_procedure_parameters(
        &mut self,
    ) -> Result<Option<Vec<ProcedureParam>>, ParserError> {
        let mut params = vec![];
        if !self.consume_token(&Token::LParen) || self.consume_token(&Token::RParen) {
            return Ok(Some(params));
        }
        loop {
            if let Token::Word(_) = self.peek_token().token {
                params.push(self.parse_procedure_param()?)
            }
            let comma = self.consume_token(&Token::Comma);
            if self.consume_token(&Token::RParen) {
                // allow a trailing comma, even though it's not in standard
                break;
            } else if !comma {
                return self.expected("',' or ')' after parameter definition", self.peek_token());
            }
        }
        Ok(Some(params))
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
            let rparen = self.peek_token().token == Token::RParen;

            if !comma && !rparen {
                return self.expected("',' or ')' after column definition", self.peek_token());
            };

            if rparen && (!comma || self.options.trailing_commas) {
                let _ = self.consume_token(&Token::RParen);
                break;
            }
        }

        Ok((columns, constraints))
    }

    pub fn parse_procedure_param(&mut self) -> Result<ProcedureParam, ParserError> {
        let name = self.parse_identifier(false)?;
        let data_type = self.parse_data_type()?;
        Ok(ProcedureParam { name, data_type })
    }

    pub fn parse_column_def(&mut self) -> Result<ColumnDef, ParserError> {
        let name = self.parse_identifier(false)?;
        let data_type = if self.is_column_type_sqlite_unspecified() {
            DataType::Unspecified
        } else {
            self.parse_data_type()?
        };
        let mut collation = if self.parse_keyword(Keyword::COLLATE) {
            Some(self.parse_object_name(false)?)
        } else {
            None
        };
        let mut options = vec![];
        loop {
            if self.parse_keyword(Keyword::CONSTRAINT) {
                let name = Some(self.parse_identifier(false)?);
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
            } else if dialect_of!(self is MySqlDialect | GenericDialect)
                && self.parse_keyword(Keyword::COLLATE)
            {
                collation = Some(self.parse_object_name(false)?);
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

    fn is_column_type_sqlite_unspecified(&mut self) -> bool {
        if dialect_of!(self is SQLiteDialect) {
            match self.peek_token().token {
                Token::Word(word) => matches!(
                    word.keyword,
                    Keyword::CONSTRAINT
                        | Keyword::PRIMARY
                        | Keyword::NOT
                        | Keyword::UNIQUE
                        | Keyword::CHECK
                        | Keyword::DEFAULT
                        | Keyword::COLLATE
                        | Keyword::REFERENCES
                        | Keyword::GENERATED
                        | Keyword::AS
                ),
                _ => true, // e.g. comma immediately after column name
            }
        } else {
            false
        }
    }

    pub fn parse_optional_column_option(&mut self) -> Result<Option<ColumnOption>, ParserError> {
        if self.parse_keywords(&[Keyword::CHARACTER, Keyword::SET]) {
            Ok(Some(ColumnOption::CharacterSet(
                self.parse_object_name(false)?,
            )))
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
        } else if dialect_of!(self is ClickHouseDialect| GenericDialect)
            && self.parse_keyword(Keyword::MATERIALIZED)
        {
            Ok(Some(ColumnOption::Materialized(self.parse_expr()?)))
        } else if dialect_of!(self is ClickHouseDialect| GenericDialect)
            && self.parse_keyword(Keyword::ALIAS)
        {
            Ok(Some(ColumnOption::Alias(self.parse_expr()?)))
        } else if dialect_of!(self is ClickHouseDialect| GenericDialect)
            && self.parse_keyword(Keyword::EPHEMERAL)
        {
            // The expression is optional for the EPHEMERAL syntax, so we need to check
            // if the column definition has remaining tokens before parsing the expression.
            if matches!(self.peek_token().token, Token::Comma | Token::RParen) {
                Ok(Some(ColumnOption::Ephemeral(None)))
            } else {
                Ok(Some(ColumnOption::Ephemeral(Some(self.parse_expr()?))))
            }
        } else if self.parse_keywords(&[Keyword::PRIMARY, Keyword::KEY]) {
            let characteristics = self.parse_constraint_characteristics()?;
            Ok(Some(ColumnOption::Unique {
                is_primary: true,
                characteristics,
            }))
        } else if self.parse_keyword(Keyword::UNIQUE) {
            let characteristics = self.parse_constraint_characteristics()?;
            Ok(Some(ColumnOption::Unique {
                is_primary: false,
                characteristics,
            }))
        } else if self.parse_keyword(Keyword::REFERENCES) {
            let foreign_table = self.parse_object_name(false)?;
            // PostgreSQL allows omitting the column list and
            // uses the primary key column of the foreign table by default
            let referred_columns = self.parse_parenthesized_column_list(Optional, false)?;
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
            let characteristics = self.parse_constraint_characteristics()?;

            Ok(Some(ColumnOption::ForeignKey {
                foreign_table,
                referred_columns,
                on_delete,
                on_update,
                characteristics,
            }))
        } else if self.parse_keyword(Keyword::CHECK) {
            self.expect_token(&Token::LParen)?;
            let expr = self.parse_expr()?;
            self.expect_token(&Token::RParen)?;
            Ok(Some(ColumnOption::Check(expr)))
        } else if self.parse_keyword(Keyword::AUTO_INCREMENT)
            && dialect_of!(self is MySqlDialect | GenericDialect)
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
        } else if self.parse_keyword(Keyword::GENERATED) {
            self.parse_optional_column_option_generated()
        } else if dialect_of!(self is BigQueryDialect | GenericDialect)
            && self.parse_keyword(Keyword::OPTIONS)
        {
            self.prev_token();
            Ok(Some(ColumnOption::Options(
                self.parse_options(Keyword::OPTIONS)?,
            )))
        } else if self.parse_keyword(Keyword::AS)
            && dialect_of!(self is MySqlDialect | SQLiteDialect | DuckDbDialect | GenericDialect)
        {
            self.parse_optional_column_option_as()
        } else {
            Ok(None)
        }
    }
    fn parse_optional_column_option_generated(
        &mut self,
    ) -> Result<Option<ColumnOption>, ParserError> {
        if self.parse_keywords(&[Keyword::ALWAYS, Keyword::AS, Keyword::IDENTITY]) {
            let mut sequence_options = vec![];
            if self.expect_token(&Token::LParen).is_ok() {
                sequence_options = self.parse_create_sequence_options()?;
                self.expect_token(&Token::RParen)?;
            }
            Ok(Some(ColumnOption::Generated {
                generated_as: GeneratedAs::Always,
                sequence_options: Some(sequence_options),
                generation_expr: None,
                generation_expr_mode: None,
                generated_keyword: true,
            }))
        } else if self.parse_keywords(&[
            Keyword::BY,
            Keyword::DEFAULT,
            Keyword::AS,
            Keyword::IDENTITY,
        ]) {
            let mut sequence_options = vec![];
            if self.expect_token(&Token::LParen).is_ok() {
                sequence_options = self.parse_create_sequence_options()?;
                self.expect_token(&Token::RParen)?;
            }
            Ok(Some(ColumnOption::Generated {
                generated_as: GeneratedAs::ByDefault,
                sequence_options: Some(sequence_options),
                generation_expr: None,
                generation_expr_mode: None,
                generated_keyword: true,
            }))
        } else if self.parse_keywords(&[Keyword::ALWAYS, Keyword::AS]) {
            if self.expect_token(&Token::LParen).is_ok() {
                let expr = self.parse_expr()?;
                self.expect_token(&Token::RParen)?;
                let (gen_as, expr_mode) = if self.parse_keywords(&[Keyword::STORED]) {
                    Ok((
                        GeneratedAs::ExpStored,
                        Some(GeneratedExpressionMode::Stored),
                    ))
                } else if dialect_of!(self is PostgreSqlDialect) {
                    // Postgres' AS IDENTITY branches are above, this one needs STORED
                    self.expected("STORED", self.peek_token())
                } else if self.parse_keywords(&[Keyword::VIRTUAL]) {
                    Ok((GeneratedAs::Always, Some(GeneratedExpressionMode::Virtual)))
                } else {
                    Ok((GeneratedAs::Always, None))
                }?;

                Ok(Some(ColumnOption::Generated {
                    generated_as: gen_as,
                    sequence_options: None,
                    generation_expr: Some(expr),
                    generation_expr_mode: expr_mode,
                    generated_keyword: true,
                }))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    fn parse_optional_column_option_as(&mut self) -> Result<Option<ColumnOption>, ParserError> {
        // Some DBs allow 'AS (expr)', shorthand for GENERATED ALWAYS AS
        self.expect_token(&Token::LParen)?;
        let expr = self.parse_expr()?;
        self.expect_token(&Token::RParen)?;

        let (gen_as, expr_mode) = if self.parse_keywords(&[Keyword::STORED]) {
            (
                GeneratedAs::ExpStored,
                Some(GeneratedExpressionMode::Stored),
            )
        } else if self.parse_keywords(&[Keyword::VIRTUAL]) {
            (GeneratedAs::Always, Some(GeneratedExpressionMode::Virtual))
        } else {
            (GeneratedAs::Always, None)
        };

        Ok(Some(ColumnOption::Generated {
            generated_as: gen_as,
            sequence_options: None,
            generation_expr: Some(expr),
            generation_expr_mode: expr_mode,
            generated_keyword: false,
        }))
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

    pub fn parse_constraint_characteristics(
        &mut self,
    ) -> Result<Option<ConstraintCharacteristics>, ParserError> {
        let mut cc = ConstraintCharacteristics::default();

        loop {
            if cc.deferrable.is_none() && self.parse_keywords(&[Keyword::NOT, Keyword::DEFERRABLE])
            {
                cc.deferrable = Some(false);
            } else if cc.deferrable.is_none() && self.parse_keyword(Keyword::DEFERRABLE) {
                cc.deferrable = Some(true);
            } else if cc.initially.is_none() && self.parse_keyword(Keyword::INITIALLY) {
                if self.parse_keyword(Keyword::DEFERRED) {
                    cc.initially = Some(DeferrableInitial::Deferred);
                } else if self.parse_keyword(Keyword::IMMEDIATE) {
                    cc.initially = Some(DeferrableInitial::Immediate);
                } else {
                    self.expected("one of DEFERRED or IMMEDIATE", self.peek_token())?;
                }
            } else if cc.enforced.is_none() && self.parse_keyword(Keyword::ENFORCED) {
                cc.enforced = Some(true);
            } else if cc.enforced.is_none()
                && self.parse_keywords(&[Keyword::NOT, Keyword::ENFORCED])
            {
                cc.enforced = Some(false);
            } else {
                break;
            }
        }

        if cc.deferrable.is_some() || cc.initially.is_some() || cc.enforced.is_some() {
            Ok(Some(cc))
        } else {
            Ok(None)
        }
    }

    pub fn parse_optional_table_constraint(
        &mut self,
    ) -> Result<Option<TableConstraint>, ParserError> {
        let name = if self.parse_keyword(Keyword::CONSTRAINT) {
            Some(self.parse_identifier(false)?)
        } else {
            None
        };

        let next_token = self.next_token();
        match next_token.token {
            Token::Word(w) if w.keyword == Keyword::UNIQUE => {
                let index_type_display = self.parse_index_type_display();
                if !dialect_of!(self is GenericDialect | MySqlDialect)
                    && !index_type_display.is_none()
                {
                    return self
                        .expected("`index_name` or `(column_name [, ...])`", self.peek_token());
                }

                // optional index name
                let index_name = self.parse_optional_indent();
                let index_type = self.parse_optional_using_then_index_type()?;

                let columns = self.parse_parenthesized_column_list(Mandatory, false)?;
                let index_options = self.parse_index_options()?;
                let characteristics = self.parse_constraint_characteristics()?;
                Ok(Some(TableConstraint::Unique {
                    name,
                    index_name,
                    index_type_display,
                    index_type,
                    columns,
                    index_options,
                    characteristics,
                }))
            }
            Token::Word(w) if w.keyword == Keyword::PRIMARY => {
                // after `PRIMARY` always stay `KEY`
                self.expect_keyword(Keyword::KEY)?;

                // optional index name
                let index_name = self.parse_optional_indent();
                let index_type = self.parse_optional_using_then_index_type()?;

                let columns = self.parse_parenthesized_column_list(Mandatory, false)?;
                let index_options = self.parse_index_options()?;
                let characteristics = self.parse_constraint_characteristics()?;
                Ok(Some(TableConstraint::PrimaryKey {
                    name,
                    index_name,
                    index_type,
                    columns,
                    index_options,
                    characteristics,
                }))
            }
            Token::Word(w) if w.keyword == Keyword::FOREIGN => {
                self.expect_keyword(Keyword::KEY)?;
                let columns = self.parse_parenthesized_column_list(Mandatory, false)?;
                self.expect_keyword(Keyword::REFERENCES)?;
                let foreign_table = self.parse_object_name(false)?;
                let referred_columns = self.parse_parenthesized_column_list(Mandatory, false)?;
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

                let characteristics = self.parse_constraint_characteristics()?;

                Ok(Some(TableConstraint::ForeignKey {
                    name,
                    columns,
                    foreign_table,
                    referred_columns,
                    on_delete,
                    on_update,
                    characteristics,
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
                    && dialect_of!(self is GenericDialect | MySqlDialect)
                    && name.is_none() =>
            {
                let display_as_key = w.keyword == Keyword::KEY;

                let name = match self.peek_token().token {
                    Token::Word(word) if word.keyword == Keyword::USING => None,
                    _ => self.parse_optional_indent(),
                };

                let index_type = self.parse_optional_using_then_index_type()?;
                let columns = self.parse_parenthesized_column_list(Mandatory, false)?;

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

                let index_type_display = self.parse_index_type_display();

                let opt_index_name = self.parse_optional_indent();

                let columns = self.parse_parenthesized_column_list(Mandatory, false)?;

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

    pub fn maybe_parse_options(
        &mut self,
        keyword: Keyword,
    ) -> Result<Option<Vec<SqlOption>>, ParserError> {
        if let Token::Word(word) = self.peek_token().token {
            if word.keyword == keyword {
                return Ok(Some(self.parse_options(keyword)?));
            }
        };
        Ok(None)
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

    pub fn parse_options_with_keywords(
        &mut self,
        keywords: &[Keyword],
    ) -> Result<Vec<SqlOption>, ParserError> {
        if self.parse_keywords(keywords) {
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

    /// Parse [USING {BTREE | HASH}]
    pub fn parse_optional_using_then_index_type(
        &mut self,
    ) -> Result<Option<IndexType>, ParserError> {
        if self.parse_keyword(Keyword::USING) {
            Ok(Some(self.parse_index_type()?))
        } else {
            Ok(None)
        }
    }

    /// Parse `[ident]`, mostly `ident` is name, like:
    /// `window_name`, `index_name`, ...
    pub fn parse_optional_indent(&mut self) -> Option<Ident> {
        self.maybe_parse(|parser| parser.parse_identifier(false))
    }

    #[must_use]
    pub fn parse_index_type_display(&mut self) -> KeyOrIndexDisplay {
        if self.parse_keyword(Keyword::KEY) {
            KeyOrIndexDisplay::Key
        } else if self.parse_keyword(Keyword::INDEX) {
            KeyOrIndexDisplay::Index
        } else {
            KeyOrIndexDisplay::None
        }
    }

    pub fn parse_optional_index_option(&mut self) -> Result<Option<IndexOption>, ParserError> {
        if let Some(index_type) = self.parse_optional_using_then_index_type()? {
            Ok(Some(IndexOption::Using(index_type)))
        } else if self.parse_keyword(Keyword::COMMENT) {
            let s = self.parse_literal_string()?;
            Ok(Some(IndexOption::Comment(s)))
        } else {
            Ok(None)
        }
    }

    pub fn parse_index_options(&mut self) -> Result<Vec<IndexOption>, ParserError> {
        let mut options = Vec::new();

        loop {
            match self.parse_optional_index_option()? {
                Some(index_option) => options.push(index_option),
                None => return Ok(options),
            }
        }
    }

    pub fn parse_sql_option(&mut self) -> Result<SqlOption, ParserError> {
        let name = self.parse_identifier(false)?;
        self.expect_token(&Token::Eq)?;
        let value = self.parse_expr()?;
        Ok(SqlOption { name, value })
    }

    pub fn parse_partition(&mut self) -> Result<Partition, ParserError> {
        self.expect_token(&Token::LParen)?;
        let partitions = self.parse_comma_separated(Parser::parse_expr)?;
        self.expect_token(&Token::RParen)?;
        Ok(Partition::Partitions(partitions))
    }

    pub fn parse_projection_select(&mut self) -> Result<ProjectionSelect, ParserError> {
        self.expect_token(&Token::LParen)?;
        self.expect_keyword(Keyword::SELECT)?;
        let projection = self.parse_projection()?;
        let group_by = self.parse_optional_group_by()?;
        let order_by = self.parse_optional_order_by()?;
        self.expect_token(&Token::RParen)?;
        Ok(ProjectionSelect {
            projection,
            group_by,
            order_by,
        })
    }
    pub fn parse_alter_table_add_projection(&mut self) -> Result<AlterTableOperation, ParserError> {
        let if_not_exists = self.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let name = self.parse_identifier(false)?;
        let query = self.parse_projection_select()?;
        Ok(AlterTableOperation::AddProjection {
            if_not_exists,
            name,
            select: query,
        })
    }

    pub fn parse_alter_table_operation(&mut self) -> Result<AlterTableOperation, ParserError> {
        let operation = if self.parse_keyword(Keyword::ADD) {
            if let Some(constraint) = self.parse_optional_table_constraint()? {
                AlterTableOperation::AddConstraint(constraint)
            } else if dialect_of!(self is ClickHouseDialect|GenericDialect)
                && self.parse_keyword(Keyword::PROJECTION)
            {
                return self.parse_alter_table_add_projection();
            } else {
                let if_not_exists =
                    self.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
                let mut new_partitions = vec![];
                loop {
                    if self.parse_keyword(Keyword::PARTITION) {
                        new_partitions.push(self.parse_partition()?);
                    } else {
                        break;
                    }
                }
                if !new_partitions.is_empty() {
                    AlterTableOperation::AddPartitions {
                        if_not_exists,
                        new_partitions,
                    }
                } else {
                    let column_keyword = self.parse_keyword(Keyword::COLUMN);

                    let if_not_exists = if dialect_of!(self is PostgreSqlDialect | BigQueryDialect | DuckDbDialect | GenericDialect)
                    {
                        self.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS])
                            || if_not_exists
                    } else {
                        false
                    };

                    let column_def = self.parse_column_def()?;

                    let column_position = self.parse_column_position()?;

                    AlterTableOperation::AddColumn {
                        column_keyword,
                        if_not_exists,
                        column_def,
                        column_position,
                    }
                }
            }
        } else if self.parse_keyword(Keyword::RENAME) {
            if dialect_of!(self is PostgreSqlDialect) && self.parse_keyword(Keyword::CONSTRAINT) {
                let old_name = self.parse_identifier(false)?;
                self.expect_keyword(Keyword::TO)?;
                let new_name = self.parse_identifier(false)?;
                AlterTableOperation::RenameConstraint { old_name, new_name }
            } else if self.parse_keyword(Keyword::TO) {
                let table_name = self.parse_object_name(false)?;
                AlterTableOperation::RenameTable { table_name }
            } else {
                let _ = self.parse_keyword(Keyword::COLUMN); // [ COLUMN ]
                let old_column_name = self.parse_identifier(false)?;
                self.expect_keyword(Keyword::TO)?;
                let new_column_name = self.parse_identifier(false)?;
                AlterTableOperation::RenameColumn {
                    old_column_name,
                    new_column_name,
                }
            }
        } else if self.parse_keyword(Keyword::DISABLE) {
            if self.parse_keywords(&[Keyword::ROW, Keyword::LEVEL, Keyword::SECURITY]) {
                AlterTableOperation::DisableRowLevelSecurity {}
            } else if self.parse_keyword(Keyword::RULE) {
                let name = self.parse_identifier(false)?;
                AlterTableOperation::DisableRule { name }
            } else if self.parse_keyword(Keyword::TRIGGER) {
                let name = self.parse_identifier(false)?;
                AlterTableOperation::DisableTrigger { name }
            } else {
                return self.expected(
                    "ROW LEVEL SECURITY, RULE, or TRIGGER after DISABLE",
                    self.peek_token(),
                );
            }
        } else if self.parse_keyword(Keyword::ENABLE) {
            if self.parse_keywords(&[Keyword::ALWAYS, Keyword::RULE]) {
                let name = self.parse_identifier(false)?;
                AlterTableOperation::EnableAlwaysRule { name }
            } else if self.parse_keywords(&[Keyword::ALWAYS, Keyword::TRIGGER]) {
                let name = self.parse_identifier(false)?;
                AlterTableOperation::EnableAlwaysTrigger { name }
            } else if self.parse_keywords(&[Keyword::ROW, Keyword::LEVEL, Keyword::SECURITY]) {
                AlterTableOperation::EnableRowLevelSecurity {}
            } else if self.parse_keywords(&[Keyword::REPLICA, Keyword::RULE]) {
                let name = self.parse_identifier(false)?;
                AlterTableOperation::EnableReplicaRule { name }
            } else if self.parse_keywords(&[Keyword::REPLICA, Keyword::TRIGGER]) {
                let name = self.parse_identifier(false)?;
                AlterTableOperation::EnableReplicaTrigger { name }
            } else if self.parse_keyword(Keyword::RULE) {
                let name = self.parse_identifier(false)?;
                AlterTableOperation::EnableRule { name }
            } else if self.parse_keyword(Keyword::TRIGGER) {
                let name = self.parse_identifier(false)?;
                AlterTableOperation::EnableTrigger { name }
            } else {
                return self.expected(
                    "ALWAYS, REPLICA, ROW LEVEL SECURITY, RULE, or TRIGGER after ENABLE",
                    self.peek_token(),
                );
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
                let name = self.parse_identifier(false)?;
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
                let column_name = self.parse_identifier(false)?;
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
            let old_name = self.parse_identifier(false)?;
            let new_name = self.parse_identifier(false)?;
            let data_type = self.parse_data_type()?;
            let mut options = vec![];
            while let Some(option) = self.parse_optional_column_option()? {
                options.push(option);
            }

            let column_position = self.parse_column_position()?;

            AlterTableOperation::ChangeColumn {
                old_name,
                new_name,
                data_type,
                options,
                column_position,
            }
        } else if self.parse_keyword(Keyword::MODIFY) {
            let _ = self.parse_keyword(Keyword::COLUMN); // [ COLUMN ]
            let col_name = self.parse_identifier(false)?;
            let data_type = self.parse_data_type()?;
            let mut options = vec![];
            while let Some(option) = self.parse_optional_column_option()? {
                options.push(option);
            }

            let column_position = self.parse_column_position()?;

            AlterTableOperation::ModifyColumn {
                col_name,
                data_type,
                options,
                column_position,
            }
        } else if self.parse_keyword(Keyword::ALTER) {
            let _ = self.parse_keyword(Keyword::COLUMN); // [ COLUMN ]
            let column_name = self.parse_identifier(false)?;
            let is_postgresql = dialect_of!(self is PostgreSqlDialect);

            let op: AlterColumnOperation = if self.parse_keywords(&[
                Keyword::SET,
                Keyword::NOT,
                Keyword::NULL,
            ]) {
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
            } else if self.parse_keywords(&[Keyword::ADD, Keyword::GENERATED]) {
                let generated_as = if self.parse_keyword(Keyword::ALWAYS) {
                    Some(GeneratedAs::Always)
                } else if self.parse_keywords(&[Keyword::BY, Keyword::DEFAULT]) {
                    Some(GeneratedAs::ByDefault)
                } else {
                    None
                };

                self.expect_keywords(&[Keyword::AS, Keyword::IDENTITY])?;

                let mut sequence_options: Option<Vec<SequenceOptions>> = None;

                if self.peek_token().token == Token::LParen {
                    self.expect_token(&Token::LParen)?;
                    sequence_options = Some(self.parse_create_sequence_options()?);
                    self.expect_token(&Token::RParen)?;
                }

                AlterColumnOperation::AddGenerated {
                    generated_as,
                    sequence_options,
                }
            } else {
                let message = if is_postgresql {
                    "SET/DROP NOT NULL, SET DEFAULT, SET DATA TYPE, or ADD GENERATED after ALTER COLUMN"
                } else {
                    "SET/DROP NOT NULL, SET DEFAULT, or SET DATA TYPE after ALTER COLUMN"
                };

                return self.expected(message, self.peek_token());
            };
            AlterTableOperation::AlterColumn { column_name, op }
        } else if self.parse_keyword(Keyword::SWAP) {
            self.expect_keyword(Keyword::WITH)?;
            let table_name = self.parse_object_name(false)?;
            AlterTableOperation::SwapWith { table_name }
        } else if dialect_of!(self is PostgreSqlDialect | GenericDialect)
            && self.parse_keywords(&[Keyword::OWNER, Keyword::TO])
        {
            let new_owner = match self.parse_one_of_keywords(&[Keyword::CURRENT_USER, Keyword::CURRENT_ROLE, Keyword::SESSION_USER]) {
                Some(Keyword::CURRENT_USER) => Owner::CurrentUser,
                Some(Keyword::CURRENT_ROLE) => Owner::CurrentRole,
                Some(Keyword::SESSION_USER) => Owner::SessionUser,
                Some(_) => unreachable!(),
                None => {
                    match self.parse_identifier(false) {
                        Ok(ident) => Owner::Ident(ident),
                        Err(e) => {
                            return Err(ParserError::ParserError(format!("Expected: CURRENT_USER, CURRENT_ROLE, SESSION_USER or identifier after OWNER TO. {e}")))
                        }
                    }
                },
            };

            AlterTableOperation::OwnerTo { new_owner }
        } else if dialect_of!(self is ClickHouseDialect|GenericDialect)
            && self.parse_keyword(Keyword::ATTACH)
        {
            AlterTableOperation::AttachPartition {
                partition: self.parse_part_or_partition()?,
            }
        } else if dialect_of!(self is ClickHouseDialect|GenericDialect)
            && self.parse_keyword(Keyword::DETACH)
        {
            AlterTableOperation::DetachPartition {
                partition: self.parse_part_or_partition()?,
            }
        } else if dialect_of!(self is ClickHouseDialect|GenericDialect)
            && self.parse_keyword(Keyword::FREEZE)
        {
            let partition = self.parse_part_or_partition()?;
            let with_name = if self.parse_keyword(Keyword::WITH) {
                self.expect_keyword(Keyword::NAME)?;
                Some(self.parse_identifier(false)?)
            } else {
                None
            };
            AlterTableOperation::FreezePartition {
                partition,
                with_name,
            }
        } else if dialect_of!(self is ClickHouseDialect|GenericDialect)
            && self.parse_keyword(Keyword::UNFREEZE)
        {
            let partition = self.parse_part_or_partition()?;
            let with_name = if self.parse_keyword(Keyword::WITH) {
                self.expect_keyword(Keyword::NAME)?;
                Some(self.parse_identifier(false)?)
            } else {
                None
            };
            AlterTableOperation::UnfreezePartition {
                partition,
                with_name,
            }
        } else {
            let options: Vec<SqlOption> =
                self.parse_options_with_keywords(&[Keyword::SET, Keyword::TBLPROPERTIES])?;
            if !options.is_empty() {
                AlterTableOperation::SetTblProperties {
                    table_properties: options,
                }
            } else {
                return self.expected(
                    "ADD, RENAME, PARTITION, SWAP, DROP, or SET TBLPROPERTIES after ALTER TABLE",
                    self.peek_token(),
                );
            }
        };
        Ok(operation)
    }

    fn parse_part_or_partition(&mut self) -> Result<Partition, ParserError> {
        let keyword = self.expect_one_of_keywords(&[Keyword::PART, Keyword::PARTITION])?;
        match keyword {
            Keyword::PART => Ok(Partition::Part(self.parse_expr()?)),
            Keyword::PARTITION => Ok(Partition::Expr(self.parse_expr()?)),
            // unreachable because expect_one_of_keywords used above
            _ => unreachable!(),
        }
    }

    pub fn parse_alter(&mut self) -> Result<Statement, ParserError> {
        let object_type = self.expect_one_of_keywords(&[
            Keyword::VIEW,
            Keyword::TABLE,
            Keyword::INDEX,
            Keyword::ROLE,
        ])?;
        match object_type {
            Keyword::VIEW => self.parse_alter_view(),
            Keyword::TABLE => {
                let if_exists = self.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
                let only = self.parse_keyword(Keyword::ONLY); // [ ONLY ]
                let table_name = self.parse_object_name(false)?;
                let on_cluster = self.parse_optional_on_cluster()?;
                let operations = self.parse_comma_separated(Parser::parse_alter_table_operation)?;

                let mut location = None;
                if self.parse_keyword(Keyword::LOCATION) {
                    location = Some(HiveSetLocation {
                        has_set: false,
                        location: self.parse_identifier(false)?,
                    });
                } else if self.parse_keywords(&[Keyword::SET, Keyword::LOCATION]) {
                    location = Some(HiveSetLocation {
                        has_set: true,
                        location: self.parse_identifier(false)?,
                    });
                }

                Ok(Statement::AlterTable {
                    name: table_name,
                    if_exists,
                    only,
                    operations,
                    location,
                    on_cluster,
                })
            }
            Keyword::INDEX => {
                let index_name = self.parse_object_name(false)?;
                let operation = if self.parse_keyword(Keyword::RENAME) {
                    if self.parse_keyword(Keyword::TO) {
                        let index_name = self.parse_object_name(false)?;
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
            Keyword::ROLE => self.parse_alter_role(),
            // unreachable because expect_one_of_keywords used above
            _ => unreachable!(),
        }
    }

    pub fn parse_alter_view(&mut self) -> Result<Statement, ParserError> {
        let name = self.parse_object_name(false)?;
        let columns = self.parse_parenthesized_column_list(Optional, false)?;

        let with_options = self.parse_options(Keyword::WITH)?;

        self.expect_keyword(Keyword::AS)?;
        let query = self.parse_boxed_query()?;

        Ok(Statement::AlterView {
            name,
            columns,
            query,
            with_options,
        })
    }

    /// Parse a `CALL procedure_name(arg1, arg2, ...)`
    /// or `CALL procedure_name` statement
    pub fn parse_call(&mut self) -> Result<Statement, ParserError> {
        let object_name = self.parse_object_name(false)?;
        if self.peek_token().token == Token::LParen {
            match self.parse_function(object_name)? {
                Expr::Function(f) => Ok(Statement::Call(f)),
                other => parser_err!(
                    format!("Expected a simple procedure call but found: {other}"),
                    self.peek_token().location
                ),
            }
        } else {
            Ok(Statement::Call(Function {
                name: object_name,
                parameters: FunctionArguments::None,
                args: FunctionArguments::None,
                over: None,
                filter: None,
                null_treatment: None,
                within_group: vec![],
            }))
        }
    }

    /// Parse a copy statement
    pub fn parse_copy(&mut self) -> Result<Statement, ParserError> {
        let source;
        if self.consume_token(&Token::LParen) {
            source = CopySource::Query(self.parse_boxed_query()?);
            self.expect_token(&Token::RParen)?;
        } else {
            let table_name = self.parse_object_name(false)?;
            let columns = self.parse_parenthesized_column_list(Optional, false)?;
            source = CopySource::Table {
                table_name,
                columns,
            };
        }
        let to = match self.parse_one_of_keywords(&[Keyword::FROM, Keyword::TO]) {
            Some(Keyword::FROM) => false,
            Some(Keyword::TO) => true,
            _ => self.expected("FROM or TO", self.peek_token())?,
        };
        if !to {
            // Use a separate if statement to prevent Rust compiler from complaining about
            // "if statement in this position is unstable: https://github.com/rust-lang/rust/issues/53667"
            if let CopySource::Query(_) = source {
                return Err(ParserError::ParserError(
                    "COPY ... FROM does not support query as a source".to_string(),
                ));
            }
        }
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
            source,
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
            let name = self.parse_identifier(false)?;

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
            Some(Keyword::FORMAT) => CopyOption::Format(self.parse_identifier(false)?),
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
                CopyOption::ForceQuote(self.parse_parenthesized_column_list(Mandatory, false)?)
            }
            Some(Keyword::FORCE_NOT_NULL) => {
                CopyOption::ForceNotNull(self.parse_parenthesized_column_list(Mandatory, false)?)
            }
            Some(Keyword::FORCE_NULL) => {
                CopyOption::ForceNull(self.parse_parenthesized_column_list(Mandatory, false)?)
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
                    self.parse_comma_separated(|p| p.parse_identifier(false))?,
                )
            }
            Some(Keyword::FORCE) if self.parse_keywords(&[Keyword::QUOTE]) => {
                CopyLegacyCsvOption::ForceQuote(
                    self.parse_comma_separated(|p| p.parse_identifier(false))?,
                )
            }
            _ => self.expected("csv option", self.peek_token())?,
        };
        Ok(ret)
    }

    fn parse_literal_char(&mut self) -> Result<char, ParserError> {
        let s = self.parse_literal_string()?;
        if s.len() != 1 {
            let loc = self
                .tokens
                .get(self.index - 1)
                .map_or(Location { line: 0, column: 0 }, |t| t.location);
            return parser_err!(format!("Expect a char, found {s:?}"), loc);
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
            Token::Number(n, l) => Ok(Value::Number(Self::parse(n, location)?, l)),
            Token::SingleQuotedString(ref s) => Ok(Value::SingleQuotedString(s.to_string())),
            Token::DoubleQuotedString(ref s) => Ok(Value::DoubleQuotedString(s.to_string())),
            Token::TripleSingleQuotedString(ref s) => {
                Ok(Value::TripleSingleQuotedString(s.to_string()))
            }
            Token::TripleDoubleQuotedString(ref s) => {
                Ok(Value::TripleDoubleQuotedString(s.to_string()))
            }
            Token::DollarQuotedString(ref s) => Ok(Value::DollarQuotedString(s.clone())),
            Token::SingleQuotedByteStringLiteral(ref s) => {
                Ok(Value::SingleQuotedByteStringLiteral(s.clone()))
            }
            Token::DoubleQuotedByteStringLiteral(ref s) => {
                Ok(Value::DoubleQuotedByteStringLiteral(s.clone()))
            }
            Token::TripleSingleQuotedByteStringLiteral(ref s) => {
                Ok(Value::TripleSingleQuotedByteStringLiteral(s.clone()))
            }
            Token::TripleDoubleQuotedByteStringLiteral(ref s) => {
                Ok(Value::TripleDoubleQuotedByteStringLiteral(s.clone()))
            }
            Token::SingleQuotedRawStringLiteral(ref s) => {
                Ok(Value::SingleQuotedRawStringLiteral(s.clone()))
            }
            Token::DoubleQuotedRawStringLiteral(ref s) => {
                Ok(Value::DoubleQuotedRawStringLiteral(s.clone()))
            }
            Token::TripleSingleQuotedRawStringLiteral(ref s) => {
                Ok(Value::TripleSingleQuotedRawStringLiteral(s.clone()))
            }
            Token::TripleDoubleQuotedRawStringLiteral(ref s) => {
                Ok(Value::TripleDoubleQuotedRawStringLiteral(s.clone()))
            }
            Token::NationalStringLiteral(ref s) => Ok(Value::NationalStringLiteral(s.to_string())),
            Token::EscapedStringLiteral(ref s) => Ok(Value::EscapedStringLiteral(s.to_string())),
            Token::UnicodeStringLiteral(ref s) => Ok(Value::UnicodeStringLiteral(s.to_string())),
            Token::HexStringLiteral(ref s) => Ok(Value::HexStringLiteral(s.to_string())),
            Token::Placeholder(ref s) => Ok(Value::Placeholder(s.to_string())),
            tok @ Token::Colon | tok @ Token::AtSign => {
                // Not calling self.parse_identifier(false)? because only in placeholder we want to check numbers as idfentifies
                // This because snowflake allows numbers as placeholders
                let next_token = self.next_token();
                let ident = match next_token.token {
                    Token::Word(w) => Ok(w.to_ident()),
                    Token::Number(w, false) => Ok(Ident::new(w)),
                    _ => self.expected("placeholder", next_token),
                }?;
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

    fn parse_introduced_string_value(&mut self) -> Result<Value, ParserError> {
        let next_token = self.next_token();
        let location = next_token.location;
        match next_token.token {
            Token::SingleQuotedString(ref s) => Ok(Value::SingleQuotedString(s.to_string())),
            Token::DoubleQuotedString(ref s) => Ok(Value::DoubleQuotedString(s.to_string())),
            Token::HexStringLiteral(ref s) => Ok(Value::HexStringLiteral(s.to_string())),
            unexpected => self.expected(
                "a string value",
                TokenWithLocation {
                    token: unexpected,
                    location,
                },
            ),
        }
    }

    /// Parse an unsigned literal integer/long
    pub fn parse_literal_uint(&mut self) -> Result<u64, ParserError> {
        let next_token = self.next_token();
        match next_token.token {
            Token::Number(s, _) => Self::parse::<u64>(s, next_token.location),
            _ => self.expected("literal int", next_token),
        }
    }

    /// Parse the body of a `CREATE FUNCTION` specified as a string.
    /// e.g. `CREATE FUNCTION ... AS $$ body $$`.
    fn parse_create_function_body_string(&mut self) -> Result<Expr, ParserError> {
        let peek_token = self.peek_token();
        match peek_token.token {
            Token::DollarQuotedString(s) if dialect_of!(self is PostgreSqlDialect | GenericDialect) =>
            {
                self.next_token();
                Ok(Expr::Value(Value::DollarQuotedString(s)))
            }
            _ => Ok(Expr::Value(Value::SingleQuotedString(
                self.parse_literal_string()?,
            ))),
        }
    }

    /// Parse a literal string
    pub fn parse_literal_string(&mut self) -> Result<String, ParserError> {
        let next_token = self.next_token();
        match next_token.token {
            Token::Word(Word {
                value,
                keyword: Keyword::NoKeyword,
                ..
            }) => Ok(value),
            Token::SingleQuotedString(s) => Ok(s),
            Token::DoubleQuotedString(s) => Ok(s),
            Token::EscapedStringLiteral(s) if dialect_of!(self is PostgreSqlDialect | GenericDialect) => {
                Ok(s)
            }
            Token::UnicodeStringLiteral(s) => Ok(s),
            _ => self.expected("literal string", next_token),
        }
    }

    /// Parse a SQL datatype (in the context of a CREATE TABLE statement for example)
    pub fn parse_data_type(&mut self) -> Result<DataType, ParserError> {
        let (ty, trailing_bracket) = self.parse_data_type_helper()?;
        if trailing_bracket.0 {
            return parser_err!(
                format!("unmatched > after parsing data type {ty}"),
                self.peek_token()
            );
        }

        Ok(ty)
    }

    fn parse_data_type_helper(
        &mut self,
    ) -> Result<(DataType, MatchedTrailingBracket), ParserError> {
        let next_token = self.next_token();
        let mut trailing_bracket: MatchedTrailingBracket = false.into();
        let mut data = match next_token.token {
            Token::Word(w) => match w.keyword {
                Keyword::BOOLEAN => Ok(DataType::Boolean),
                Keyword::BOOL => Ok(DataType::Bool),
                Keyword::FLOAT => Ok(DataType::Float(self.parse_optional_precision()?)),
                Keyword::REAL => Ok(DataType::Real),
                Keyword::FLOAT4 => Ok(DataType::Float4),
                Keyword::FLOAT32 => Ok(DataType::Float32),
                Keyword::FLOAT64 => Ok(DataType::Float64),
                Keyword::FLOAT8 => Ok(DataType::Float8),
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
                Keyword::INT2 => {
                    let optional_precision = self.parse_optional_precision();
                    if self.parse_keyword(Keyword::UNSIGNED) {
                        Ok(DataType::UnsignedInt2(optional_precision?))
                    } else {
                        Ok(DataType::Int2(optional_precision?))
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
                Keyword::INT4 => {
                    let optional_precision = self.parse_optional_precision();
                    if self.parse_keyword(Keyword::UNSIGNED) {
                        Ok(DataType::UnsignedInt4(optional_precision?))
                    } else {
                        Ok(DataType::Int4(optional_precision?))
                    }
                }
                Keyword::INT8 => {
                    let optional_precision = self.parse_optional_precision();
                    if self.parse_keyword(Keyword::UNSIGNED) {
                        Ok(DataType::UnsignedInt8(optional_precision?))
                    } else {
                        Ok(DataType::Int8(optional_precision?))
                    }
                }
                Keyword::INT16 => Ok(DataType::Int16),
                Keyword::INT32 => Ok(DataType::Int32),
                Keyword::INT64 => Ok(DataType::Int64),
                Keyword::INT128 => Ok(DataType::Int128),
                Keyword::INT256 => Ok(DataType::Int256),
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
                Keyword::UINT8 => Ok(DataType::UInt8),
                Keyword::UINT16 => Ok(DataType::UInt16),
                Keyword::UINT32 => Ok(DataType::UInt32),
                Keyword::UINT64 => Ok(DataType::UInt64),
                Keyword::UINT128 => Ok(DataType::UInt128),
                Keyword::UINT256 => Ok(DataType::UInt256),
                Keyword::VARCHAR => Ok(DataType::Varchar(self.parse_optional_character_length()?)),
                Keyword::NVARCHAR => {
                    Ok(DataType::Nvarchar(self.parse_optional_character_length()?))
                }
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
                Keyword::BYTES => Ok(DataType::Bytes(self.parse_optional_precision()?)),
                Keyword::UUID => Ok(DataType::Uuid),
                Keyword::DATE => Ok(DataType::Date),
                Keyword::DATE32 => Ok(DataType::Date32),
                Keyword::DATETIME => Ok(DataType::Datetime(self.parse_optional_precision()?)),
                Keyword::DATETIME64 => {
                    self.prev_token();
                    let (precision, time_zone) = self.parse_datetime_64()?;
                    Ok(DataType::Datetime64(precision, time_zone))
                }
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
                Keyword::JSON => Ok(DataType::JSON),
                Keyword::JSONB => Ok(DataType::JSONB),
                Keyword::REGCLASS => Ok(DataType::Regclass),
                Keyword::STRING => Ok(DataType::String(self.parse_optional_precision()?)),
                Keyword::FIXEDSTRING => {
                    self.expect_token(&Token::LParen)?;
                    let character_length = self.parse_literal_uint()?;
                    self.expect_token(&Token::RParen)?;
                    Ok(DataType::FixedString(character_length))
                }
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
                Keyword::BIGNUMERIC => Ok(DataType::BigNumeric(
                    self.parse_exact_number_optional_precision_scale()?,
                )),
                Keyword::BIGDECIMAL => Ok(DataType::BigDecimal(
                    self.parse_exact_number_optional_precision_scale()?,
                )),
                Keyword::ENUM => Ok(DataType::Enum(self.parse_string_values()?)),
                Keyword::SET => Ok(DataType::Set(self.parse_string_values()?)),
                Keyword::ARRAY => {
                    if dialect_of!(self is SnowflakeDialect) {
                        Ok(DataType::Array(ArrayElemTypeDef::None))
                    } else if dialect_of!(self is ClickHouseDialect) {
                        Ok(self.parse_sub_type(|internal_type| {
                            DataType::Array(ArrayElemTypeDef::Parenthesis(internal_type))
                        })?)
                    } else {
                        self.expect_token(&Token::Lt)?;
                        let (inside_type, _trailing_bracket) = self.parse_data_type_helper()?;
                        trailing_bracket = self.expect_closing_angle_bracket(_trailing_bracket)?;
                        Ok(DataType::Array(ArrayElemTypeDef::AngleBracket(Box::new(
                            inside_type,
                        ))))
                    }
                }
                Keyword::STRUCT if dialect_of!(self is DuckDbDialect) => {
                    self.prev_token();
                    let field_defs = self.parse_duckdb_struct_type_def()?;
                    Ok(DataType::Struct(field_defs, StructBracketKind::Parentheses))
                }
                Keyword::STRUCT if dialect_of!(self is BigQueryDialect | GenericDialect) => {
                    self.prev_token();
                    let (field_defs, _trailing_bracket) =
                        self.parse_struct_type_def(Self::parse_struct_field_def)?;
                    trailing_bracket = _trailing_bracket;
                    Ok(DataType::Struct(
                        field_defs,
                        StructBracketKind::AngleBrackets,
                    ))
                }
                Keyword::UNION if dialect_of!(self is DuckDbDialect | GenericDialect) => {
                    self.prev_token();
                    let fields = self.parse_union_type_def()?;
                    Ok(DataType::Union(fields))
                }
                Keyword::NULLABLE if dialect_of!(self is ClickHouseDialect | GenericDialect) => {
                    Ok(self.parse_sub_type(DataType::Nullable)?)
                }
                Keyword::LOWCARDINALITY if dialect_of!(self is ClickHouseDialect | GenericDialect) => {
                    Ok(self.parse_sub_type(DataType::LowCardinality)?)
                }
                Keyword::MAP if dialect_of!(self is ClickHouseDialect | GenericDialect) => {
                    self.prev_token();
                    let (key_data_type, value_data_type) = self.parse_click_house_map_def()?;
                    Ok(DataType::Map(
                        Box::new(key_data_type),
                        Box::new(value_data_type),
                    ))
                }
                Keyword::NESTED if dialect_of!(self is ClickHouseDialect | GenericDialect) => {
                    self.expect_token(&Token::LParen)?;
                    let field_defs = self.parse_comma_separated(Parser::parse_column_def)?;
                    self.expect_token(&Token::RParen)?;
                    Ok(DataType::Nested(field_defs))
                }
                Keyword::TUPLE if dialect_of!(self is ClickHouseDialect | GenericDialect) => {
                    self.prev_token();
                    let field_defs = self.parse_click_house_tuple_def()?;
                    Ok(DataType::Tuple(field_defs))
                }
                Keyword::TRIGGER => Ok(DataType::Trigger),
                _ => {
                    self.prev_token();
                    let type_name = self.parse_object_name(false)?;
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
            let size = if dialect_of!(self is GenericDialect | DuckDbDialect | PostgreSqlDialect) {
                self.maybe_parse(|p| p.parse_literal_uint())
            } else {
                None
            };
            self.expect_token(&Token::RBracket)?;
            data = DataType::Array(ArrayElemTypeDef::SquareBracket(Box::new(data), size))
        }
        Ok((data, trailing_bracket))
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

    /// Strictly parse `identifier AS identifier`
    pub fn parse_identifier_with_alias(&mut self) -> Result<IdentWithAlias, ParserError> {
        let ident = self.parse_identifier(false)?;
        self.expect_keyword(Keyword::AS)?;
        let alias = self.parse_identifier(false)?;
        Ok(IdentWithAlias { ident, alias })
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
            // Support for MySql dialect double-quoted string, `AS "HOUR"` for example
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
                let columns = self.parse_parenthesized_column_list(Optional, false)?;
                Ok(Some(TableAlias { name, columns }))
            }
            None => Ok(None),
        }
    }

    pub fn parse_optional_group_by(&mut self) -> Result<Option<GroupByExpr>, ParserError> {
        if self.parse_keywords(&[Keyword::GROUP, Keyword::BY]) {
            let expressions = if self.parse_keyword(Keyword::ALL) {
                None
            } else {
                Some(self.parse_comma_separated(Parser::parse_group_by_expr)?)
            };

            let mut modifiers = vec![];
            if dialect_of!(self is ClickHouseDialect | GenericDialect) {
                loop {
                    if !self.parse_keyword(Keyword::WITH) {
                        break;
                    }
                    let keyword = self.expect_one_of_keywords(&[
                        Keyword::ROLLUP,
                        Keyword::CUBE,
                        Keyword::TOTALS,
                    ])?;
                    modifiers.push(match keyword {
                        Keyword::ROLLUP => GroupByWithModifier::Rollup,
                        Keyword::CUBE => GroupByWithModifier::Cube,
                        Keyword::TOTALS => GroupByWithModifier::Totals,
                        _ => {
                            return parser_err!(
                                "BUG: expected to match GroupBy modifier keyword",
                                self.peek_token().location
                            )
                        }
                    });
                }
            }
            let group_by = match expressions {
                None => GroupByExpr::All(modifiers),
                Some(exprs) => GroupByExpr::Expressions(exprs, modifiers),
            };
            Ok(Some(group_by))
        } else {
            Ok(None)
        }
    }

    pub fn parse_optional_order_by(&mut self) -> Result<Option<OrderBy>, ParserError> {
        if self.parse_keywords(&[Keyword::ORDER, Keyword::BY]) {
            let order_by_exprs = self.parse_comma_separated(Parser::parse_order_by_expr)?;
            let interpolate = if dialect_of!(self is ClickHouseDialect | GenericDialect) {
                self.parse_interpolations()?
            } else {
                None
            };

            Ok(Some(OrderBy {
                exprs: order_by_exprs,
                interpolate,
            }))
        } else {
            Ok(None)
        }
    }

    /// Parse a possibly qualified, possibly quoted identifier, e.g.
    /// `foo` or `myschema."table"
    ///
    /// The `in_table_clause` parameter indicates whether the object name is a table in a FROM, JOIN,
    /// or similar table clause. Currently, this is used only to support unquoted hyphenated identifiers
    /// in this context on BigQuery.
    pub fn parse_object_name(&mut self, in_table_clause: bool) -> Result<ObjectName, ParserError> {
        let mut idents = vec![];
        loop {
            idents.push(self.parse_identifier(in_table_clause)?);
            if !self.consume_token(&Token::Period) {
                break;
            }
        }

        // BigQuery accepts any number of quoted identifiers of a table name.
        // https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#quoted_identifiers
        if dialect_of!(self is BigQueryDialect)
            && idents.iter().any(|ident| ident.value.contains('.'))
        {
            idents = idents
                .into_iter()
                .flat_map(|ident| {
                    ident
                        .value
                        .split('.')
                        .map(|value| Ident {
                            value: value.into(),
                            quote_style: ident.quote_style,
                        })
                        .collect::<Vec<_>>()
                })
                .collect()
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

    /// Parse identifiers of form ident1[.identN]*
    ///
    /// Similar in functionality to [parse_identifiers], with difference
    /// being this function is much more strict about parsing a valid multipart identifier, not
    /// allowing extraneous tokens to be parsed, otherwise it fails.
    ///
    /// For example:
    ///
    /// ```rust
    /// use sqlparser::ast::Ident;
    /// use sqlparser::dialect::GenericDialect;
    /// use sqlparser::parser::Parser;
    ///
    /// let dialect = GenericDialect {};
    /// let expected = vec![Ident::new("one"), Ident::new("two")];
    ///
    /// // expected usage
    /// let sql = "one.two";
    /// let mut parser = Parser::new(&dialect).try_with_sql(sql).unwrap();
    /// let actual = parser.parse_multipart_identifier().unwrap();
    /// assert_eq!(&actual, &expected);
    ///
    /// // parse_identifiers is more loose on what it allows, parsing successfully
    /// let sql = "one + two";
    /// let mut parser = Parser::new(&dialect).try_with_sql(sql).unwrap();
    /// let actual = parser.parse_identifiers().unwrap();
    /// assert_eq!(&actual, &expected);
    ///
    /// // expected to strictly fail due to + separator
    /// let sql = "one + two";
    /// let mut parser = Parser::new(&dialect).try_with_sql(sql).unwrap();
    /// let actual = parser.parse_multipart_identifier().unwrap_err();
    /// assert_eq!(
    ///     actual.to_string(),
    ///     "sql parser error: Unexpected token in identifier: +"
    /// );
    /// ```
    ///
    /// [parse_identifiers]: Parser::parse_identifiers
    pub fn parse_multipart_identifier(&mut self) -> Result<Vec<Ident>, ParserError> {
        let mut idents = vec![];

        // expecting at least one word for identifier
        match self.next_token().token {
            Token::Word(w) => idents.push(w.to_ident()),
            Token::EOF => {
                return Err(ParserError::ParserError(
                    "Empty input when parsing identifier".to_string(),
                ))?
            }
            token => {
                return Err(ParserError::ParserError(format!(
                    "Unexpected token in identifier: {token}"
                )))?
            }
        };

        // parse optional next parts if exist
        loop {
            match self.next_token().token {
                // ensure that optional period is succeeded by another identifier
                Token::Period => match self.next_token().token {
                    Token::Word(w) => idents.push(w.to_ident()),
                    Token::EOF => {
                        return Err(ParserError::ParserError(
                            "Trailing period in identifier".to_string(),
                        ))?
                    }
                    token => {
                        return Err(ParserError::ParserError(format!(
                            "Unexpected token following period in identifier: {token}"
                        )))?
                    }
                },
                Token::EOF => break,
                token => {
                    return Err(ParserError::ParserError(format!(
                        "Unexpected token in identifier: {token}"
                    )))?
                }
            }
        }

        Ok(idents)
    }

    /// Parse a simple one-word identifier (possibly quoted, possibly a keyword)
    ///
    /// The `in_table_clause` parameter indicates whether the identifier is a table in a FROM, JOIN, or
    /// similar table clause. Currently, this is used only to support unquoted hyphenated identifiers in
    //  this context on BigQuery.
    pub fn parse_identifier(&mut self, in_table_clause: bool) -> Result<Ident, ParserError> {
        let next_token = self.next_token();
        match next_token.token {
            Token::Word(w) => {
                let mut ident = w.to_ident();

                // On BigQuery, hyphens are permitted in unquoted identifiers inside of a FROM or
                // TABLE clause [0].
                //
                // The first segment must be an ordinary unquoted identifier, e.g. it must not start
                // with a digit. Subsequent segments are either must either be valid identifiers or
                // integers, e.g. foo-123 is allowed, but foo-123a is not.
                //
                // [0] https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical
                if dialect_of!(self is BigQueryDialect)
                    && w.quote_style.is_none()
                    && in_table_clause
                {
                    let mut requires_whitespace = false;
                    while matches!(self.peek_token_no_skip().token, Token::Minus) {
                        self.next_token();
                        ident.value.push('-');

                        let token = self
                            .next_token_no_skip()
                            .cloned()
                            .unwrap_or(TokenWithLocation::wrap(Token::EOF));
                        requires_whitespace = match token.token {
                            Token::Word(next_word) if next_word.quote_style.is_none() => {
                                ident.value.push_str(&next_word.value);
                                false
                            }
                            Token::Number(s, false) if s.chars().all(|c| c.is_ascii_digit()) => {
                                ident.value.push_str(&s);
                                true
                            }
                            _ => {
                                return self
                                    .expected("continuation of hyphenated identifier", token);
                            }
                        }
                    }

                    // If the last segment was a number, we must check that it's followed by whitespace,
                    // otherwise foo-123a will be parsed as `foo-123` with the alias `a`.
                    if requires_whitespace {
                        let token = self.next_token();
                        if !matches!(token.token, Token::EOF | Token::Whitespace(_)) {
                            return self
                                .expected("whitespace following hyphenated identifier", token);
                        }
                    }
                }
                Ok(ident)
            }
            Token::SingleQuotedString(s) => Ok(Ident::with_quote('\'', s)),
            Token::DoubleQuotedString(s) => Ok(Ident::with_quote('\"', s)),
            _ => self.expected("identifier", next_token),
        }
    }

    /// Parses a parenthesized, comma-separated list of column definitions within a view.
    fn parse_view_columns(&mut self) -> Result<Vec<ViewColumnDef>, ParserError> {
        if self.consume_token(&Token::LParen) {
            if self.peek_token().token == Token::RParen {
                self.next_token();
                Ok(vec![])
            } else {
                let cols = self.parse_comma_separated(Parser::parse_view_column)?;
                self.expect_token(&Token::RParen)?;
                Ok(cols)
            }
        } else {
            Ok(vec![])
        }
    }

    /// Parses a column definition within a view.
    fn parse_view_column(&mut self) -> Result<ViewColumnDef, ParserError> {
        let name = self.parse_identifier(false)?;
        let options = if dialect_of!(self is BigQueryDialect | GenericDialect)
            && self.parse_keyword(Keyword::OPTIONS)
        {
            self.prev_token();
            Some(self.parse_options(Keyword::OPTIONS)?)
        } else {
            None
        };
        let data_type = if dialect_of!(self is ClickHouseDialect) {
            Some(self.parse_data_type()?)
        } else {
            None
        };
        Ok(ViewColumnDef {
            name,
            data_type,
            options,
        })
    }

    /// Parse a parenthesized comma-separated list of unqualified, possibly quoted identifiers
    pub fn parse_parenthesized_column_list(
        &mut self,
        optional: IsOptional,
        allow_empty: bool,
    ) -> Result<Vec<Ident>, ParserError> {
        if self.consume_token(&Token::LParen) {
            if allow_empty && self.peek_token().token == Token::RParen {
                self.next_token();
                Ok(vec![])
            } else {
                let cols = self.parse_comma_separated(|p| p.parse_identifier(false))?;
                self.expect_token(&Token::RParen)?;
                Ok(cols)
            }
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

    /// Parse datetime64 [1]
    /// Syntax
    /// ```sql
    /// DateTime64(precision[, timezone])
    /// ```
    ///
    /// [1]: https://clickhouse.com/docs/en/sql-reference/data-types/datetime64
    pub fn parse_datetime_64(&mut self) -> Result<(u64, Option<String>), ParserError> {
        self.expect_keyword(Keyword::DATETIME64)?;
        self.expect_token(&Token::LParen)?;
        let precision = self.parse_literal_uint()?;
        let time_zone = if self.consume_token(&Token::Comma) {
            Some(self.parse_literal_string()?)
        } else {
            None
        };
        self.expect_token(&Token::RParen)?;
        Ok((precision, time_zone))
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
        if self.parse_keyword(Keyword::MAX) {
            return Ok(CharacterLength::Max);
        }
        let length = self.parse_literal_uint()?;
        let unit = if self.parse_keyword(Keyword::CHARACTERS) {
            Some(CharLengthUnits::Characters)
        } else if self.parse_keyword(Keyword::OCTETS) {
            Some(CharLengthUnits::Octets)
        } else {
            None
        };
        Ok(CharacterLength::IntegerLength { length, unit })
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

    /// Parse a parenthesized sub data type
    fn parse_sub_type<F>(&mut self, parent_type: F) -> Result<DataType, ParserError>
    where
        F: FnOnce(Box<DataType>) -> DataType,
    {
        self.expect_token(&Token::LParen)?;
        let inside_type = self.parse_data_type()?;
        self.expect_token(&Token::RParen)?;
        Ok(parent_type(inside_type.into()))
    }

    pub fn parse_delete(&mut self) -> Result<Statement, ParserError> {
        let (tables, with_from_keyword) = if !self.parse_keyword(Keyword::FROM) {
            // `FROM` keyword is optional in BigQuery SQL.
            // https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#delete_statement
            if dialect_of!(self is BigQueryDialect | GenericDialect) {
                (vec![], false)
            } else {
                let tables = self.parse_comma_separated(|p| p.parse_object_name(false))?;
                self.expect_keyword(Keyword::FROM)?;
                (tables, true)
            }
        } else {
            (vec![], true)
        };

        let from = self.parse_comma_separated(Parser::parse_table_and_joins)?;
        let using = if self.parse_keyword(Keyword::USING) {
            Some(self.parse_comma_separated(Parser::parse_table_and_joins)?)
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
        let order_by = if self.parse_keywords(&[Keyword::ORDER, Keyword::BY]) {
            self.parse_comma_separated(Parser::parse_order_by_expr)?
        } else {
            vec![]
        };
        let limit = if self.parse_keyword(Keyword::LIMIT) {
            self.parse_limit()?
        } else {
            None
        };

        Ok(Statement::Delete(Delete {
            tables,
            from: if with_from_keyword {
                FromTable::WithFromKeyword(from)
            } else {
                FromTable::WithoutKeyword(from)
            },
            using,
            selection,
            returning,
            order_by,
            limit,
        }))
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

    pub fn parse_explain(
        &mut self,
        describe_alias: DescribeAlias,
    ) -> Result<Statement, ParserError> {
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
                let hive_format =
                    match self.parse_one_of_keywords(&[Keyword::EXTENDED, Keyword::FORMATTED]) {
                        Some(Keyword::EXTENDED) => Some(HiveDescribeFormat::Extended),
                        Some(Keyword::FORMATTED) => Some(HiveDescribeFormat::Formatted),
                        _ => None,
                    };

                let has_table_keyword = if self.dialect.describe_requires_table_keyword() {
                    // only allow to use TABLE keyword for DESC|DESCRIBE statement
                    self.parse_keyword(Keyword::TABLE)
                } else {
                    false
                };

                let table_name = self.parse_object_name(false)?;
                Ok(Statement::ExplainTable {
                    describe_alias,
                    hive_format,
                    has_table_keyword,
                    table_name,
                })
            }
        }
    }

    /// Call's [`Self::parse_query`] returning a `Box`'ed  result.
    ///
    /// This function can be used to reduce the stack size required in debug
    /// builds. Instead of `sizeof(Query)` only a pointer (`Box<Query>`)
    /// is used.
    pub fn parse_boxed_query(&mut self) -> Result<Box<Query>, ParserError> {
        self.parse_query().map(Box::new)
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
        if self.parse_keyword(Keyword::INSERT) {
            Ok(Query {
                with,
                body: self.parse_insert_setexpr_boxed()?,
                limit: None,
                limit_by: vec![],
                order_by: None,
                offset: None,
                fetch: None,
                locks: vec![],
                for_clause: None,
                settings: None,
                format_clause: None,
            })
        } else if self.parse_keyword(Keyword::UPDATE) {
            Ok(Query {
                with,
                body: self.parse_update_setexpr_boxed()?,
                limit: None,
                limit_by: vec![],
                order_by: None,
                offset: None,
                fetch: None,
                locks: vec![],
                for_clause: None,
                settings: None,
                format_clause: None,
            })
        } else {
            let body = self.parse_boxed_query_body(self.dialect.prec_unknown())?;

            let order_by = self.parse_optional_order_by()?;

            let mut limit = None;
            let mut offset = None;

            for _x in 0..2 {
                if limit.is_none() && self.parse_keyword(Keyword::LIMIT) {
                    limit = self.parse_limit()?
                }

                if offset.is_none() && self.parse_keyword(Keyword::OFFSET) {
                    offset = Some(self.parse_offset()?)
                }

                if dialect_of!(self is GenericDialect | MySqlDialect | ClickHouseDialect)
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

            let limit_by = if dialect_of!(self is ClickHouseDialect | GenericDialect)
                && self.parse_keyword(Keyword::BY)
            {
                self.parse_comma_separated(Parser::parse_expr)?
            } else {
                vec![]
            };

            let settings = self.parse_settings()?;

            let fetch = if self.parse_keyword(Keyword::FETCH) {
                Some(self.parse_fetch()?)
            } else {
                None
            };

            let mut for_clause = None;
            let mut locks = Vec::new();
            while self.parse_keyword(Keyword::FOR) {
                if let Some(parsed_for_clause) = self.parse_for_clause()? {
                    for_clause = Some(parsed_for_clause);
                    break;
                } else {
                    locks.push(self.parse_lock()?);
                }
            }
            let format_clause = if dialect_of!(self is ClickHouseDialect | GenericDialect)
                && self.parse_keyword(Keyword::FORMAT)
            {
                if self.parse_keyword(Keyword::NULL) {
                    Some(FormatClause::Null)
                } else {
                    let ident = self.parse_identifier(false)?;
                    Some(FormatClause::Identifier(ident))
                }
            } else {
                None
            };

            Ok(Query {
                with,
                body,
                order_by,
                limit,
                limit_by,
                offset,
                fetch,
                locks,
                for_clause,
                settings,
                format_clause,
            })
        }
    }

    fn parse_settings(&mut self) -> Result<Option<Vec<Setting>>, ParserError> {
        let settings = if dialect_of!(self is ClickHouseDialect|GenericDialect)
            && self.parse_keyword(Keyword::SETTINGS)
        {
            let key_values = self.parse_comma_separated(|p| {
                let key = p.parse_identifier(false)?;
                p.expect_token(&Token::Eq)?;
                let value = p.parse_value()?;
                Ok(Setting { key, value })
            })?;
            Some(key_values)
        } else {
            None
        };
        Ok(settings)
    }

    /// Parse a mssql `FOR [XML | JSON | BROWSE]` clause
    pub fn parse_for_clause(&mut self) -> Result<Option<ForClause>, ParserError> {
        if self.parse_keyword(Keyword::XML) {
            Ok(Some(self.parse_for_xml()?))
        } else if self.parse_keyword(Keyword::JSON) {
            Ok(Some(self.parse_for_json()?))
        } else if self.parse_keyword(Keyword::BROWSE) {
            Ok(Some(ForClause::Browse))
        } else {
            Ok(None)
        }
    }

    /// Parse a mssql `FOR XML` clause
    pub fn parse_for_xml(&mut self) -> Result<ForClause, ParserError> {
        let for_xml = if self.parse_keyword(Keyword::RAW) {
            let mut element_name = None;
            if self.peek_token().token == Token::LParen {
                self.expect_token(&Token::LParen)?;
                element_name = Some(self.parse_literal_string()?);
                self.expect_token(&Token::RParen)?;
            }
            ForXml::Raw(element_name)
        } else if self.parse_keyword(Keyword::AUTO) {
            ForXml::Auto
        } else if self.parse_keyword(Keyword::EXPLICIT) {
            ForXml::Explicit
        } else if self.parse_keyword(Keyword::PATH) {
            let mut element_name = None;
            if self.peek_token().token == Token::LParen {
                self.expect_token(&Token::LParen)?;
                element_name = Some(self.parse_literal_string()?);
                self.expect_token(&Token::RParen)?;
            }
            ForXml::Path(element_name)
        } else {
            return Err(ParserError::ParserError(
                "Expected FOR XML [RAW | AUTO | EXPLICIT | PATH ]".to_string(),
            ));
        };
        let mut elements = false;
        let mut binary_base64 = false;
        let mut root = None;
        let mut r#type = false;
        while self.peek_token().token == Token::Comma {
            self.next_token();
            if self.parse_keyword(Keyword::ELEMENTS) {
                elements = true;
            } else if self.parse_keyword(Keyword::BINARY) {
                self.expect_keyword(Keyword::BASE64)?;
                binary_base64 = true;
            } else if self.parse_keyword(Keyword::ROOT) {
                self.expect_token(&Token::LParen)?;
                root = Some(self.parse_literal_string()?);
                self.expect_token(&Token::RParen)?;
            } else if self.parse_keyword(Keyword::TYPE) {
                r#type = true;
            }
        }
        Ok(ForClause::Xml {
            for_xml,
            elements,
            binary_base64,
            root,
            r#type,
        })
    }

    /// Parse a mssql `FOR JSON` clause
    pub fn parse_for_json(&mut self) -> Result<ForClause, ParserError> {
        let for_json = if self.parse_keyword(Keyword::AUTO) {
            ForJson::Auto
        } else if self.parse_keyword(Keyword::PATH) {
            ForJson::Path
        } else {
            return Err(ParserError::ParserError(
                "Expected FOR JSON [AUTO | PATH ]".to_string(),
            ));
        };
        let mut root = None;
        let mut include_null_values = false;
        let mut without_array_wrapper = false;
        while self.peek_token().token == Token::Comma {
            self.next_token();
            if self.parse_keyword(Keyword::ROOT) {
                self.expect_token(&Token::LParen)?;
                root = Some(self.parse_literal_string()?);
                self.expect_token(&Token::RParen)?;
            } else if self.parse_keyword(Keyword::INCLUDE_NULL_VALUES) {
                include_null_values = true;
            } else if self.parse_keyword(Keyword::WITHOUT_ARRAY_WRAPPER) {
                without_array_wrapper = true;
            }
        }
        Ok(ForClause::Json {
            for_json,
            root,
            include_null_values,
            without_array_wrapper,
        })
    }

    /// Parse a CTE (`alias [( col1, col2, ... )] AS (subquery)`)
    pub fn parse_cte(&mut self) -> Result<Cte, ParserError> {
        let name = self.parse_identifier(false)?;

        let mut cte = if self.parse_keyword(Keyword::AS) {
            let mut is_materialized = None;
            if dialect_of!(self is PostgreSqlDialect) {
                if self.parse_keyword(Keyword::MATERIALIZED) {
                    is_materialized = Some(CteAsMaterialized::Materialized);
                } else if self.parse_keywords(&[Keyword::NOT, Keyword::MATERIALIZED]) {
                    is_materialized = Some(CteAsMaterialized::NotMaterialized);
                }
            }
            self.expect_token(&Token::LParen)?;
            let query = self.parse_boxed_query()?;
            self.expect_token(&Token::RParen)?;
            let alias = TableAlias {
                name,
                columns: vec![],
            };
            Cte {
                alias,
                query,
                from: None,
                materialized: is_materialized,
            }
        } else {
            let columns = self.parse_parenthesized_column_list(Optional, false)?;
            self.expect_keyword(Keyword::AS)?;
            let mut is_materialized = None;
            if dialect_of!(self is PostgreSqlDialect) {
                if self.parse_keyword(Keyword::MATERIALIZED) {
                    is_materialized = Some(CteAsMaterialized::Materialized);
                } else if self.parse_keywords(&[Keyword::NOT, Keyword::MATERIALIZED]) {
                    is_materialized = Some(CteAsMaterialized::NotMaterialized);
                }
            }
            self.expect_token(&Token::LParen)?;
            let query = self.parse_boxed_query()?;
            self.expect_token(&Token::RParen)?;
            let alias = TableAlias { name, columns };
            Cte {
                alias,
                query,
                from: None,
                materialized: is_materialized,
            }
        };
        if self.parse_keyword(Keyword::FROM) {
            cte.from = Some(self.parse_identifier(false)?);
        }
        Ok(cte)
    }

    /// Call's [`Self::parse_query_body`] returning a `Box`'ed  result.
    ///
    /// This function can be used to reduce the stack size required in debug
    /// builds. Instead of `sizeof(QueryBody)` only a pointer (`Box<QueryBody>`)
    /// is used.
    fn parse_boxed_query_body(&mut self, precedence: u8) -> Result<Box<SetExpr>, ParserError> {
        self.parse_query_body(precedence).map(Box::new)
    }

    /// Parse a "query body", which is an expression with roughly the
    /// following grammar:
    /// ```sql
    ///   query_body ::= restricted_select | '(' subquery ')' | set_operation
    ///   restricted_select ::= 'SELECT' [expr_list] [ from ] [ where ] [ groupby_having ]
    ///   subquery ::= query_body [ order_by_limit ]
    ///   set_operation ::= query_body { 'UNION' | 'EXCEPT' | 'INTERSECT' } [ 'ALL' ] query_body
    /// ```
    ///
    /// If you need `Box<SetExpr>` then maybe there is sense to use `parse_boxed_query_body`
    /// due to prevent stack overflow in debug building(to reserve less memory on stack).
    pub fn parse_query_body(&mut self, precedence: u8) -> Result<SetExpr, ParserError> {
        // We parse the expression using a Pratt parser, as in `parse_expr()`.
        // Start by parsing a restricted SELECT or a `(subquery)`:
        let expr = if self.parse_keyword(Keyword::SELECT) {
            SetExpr::Select(self.parse_select().map(Box::new)?)
        } else if self.consume_token(&Token::LParen) {
            // CTEs are not allowed here, but the parser currently accepts them
            let subquery = self.parse_boxed_query()?;
            self.expect_token(&Token::RParen)?;
            SetExpr::Query(subquery)
        } else if self.parse_keyword(Keyword::VALUES) {
            let is_mysql = dialect_of!(self is MySqlDialect);
            SetExpr::Values(self.parse_values(is_mysql)?)
        } else if self.parse_keyword(Keyword::TABLE) {
            SetExpr::Table(Box::new(self.parse_as_table()?))
        } else {
            return self.expected(
                "SELECT, VALUES, or a subquery in the query body",
                self.peek_token(),
            );
        };

        self.parse_remaining_set_exprs(expr, precedence)
    }

    /// Parse any extra set expressions that may be present in a query body
    ///
    /// (this is its own function to reduce required stack size in debug builds)
    fn parse_remaining_set_exprs(
        &mut self,
        mut expr: SetExpr,
        precedence: u8,
    ) -> Result<SetExpr, ParserError> {
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
                right: self.parse_boxed_query_body(next_precedence)?,
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
            Some(SetOperator::Except | SetOperator::Intersect | SetOperator::Union) => {
                if self.parse_keywords(&[Keyword::DISTINCT, Keyword::BY, Keyword::NAME]) {
                    SetQuantifier::DistinctByName
                } else if self.parse_keywords(&[Keyword::BY, Keyword::NAME]) {
                    SetQuantifier::ByName
                } else if self.parse_keyword(Keyword::ALL) {
                    if self.parse_keywords(&[Keyword::BY, Keyword::NAME]) {
                        SetQuantifier::AllByName
                    } else {
                        SetQuantifier::All
                    }
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
        let value_table_mode =
            if dialect_of!(self is BigQueryDialect) && self.parse_keyword(Keyword::AS) {
                if self.parse_keyword(Keyword::VALUE) {
                    Some(ValueTableMode::AsValue)
                } else if self.parse_keyword(Keyword::STRUCT) {
                    Some(ValueTableMode::AsStruct)
                } else {
                    self.expected("VALUE or STRUCT", self.peek_token())?
                }
            } else {
                None
            };

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
            let name = self.parse_object_name(false)?;
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
                let lateral_view_name = self.parse_object_name(false)?;
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

        let prewhere = if dialect_of!(self is ClickHouseDialect|GenericDialect)
            && self.parse_keyword(Keyword::PREWHERE)
        {
            Some(self.parse_expr()?)
        } else {
            None
        };

        let selection = if self.parse_keyword(Keyword::WHERE) {
            Some(self.parse_expr()?)
        } else {
            None
        };

        let group_by = self
            .parse_optional_group_by()?
            .unwrap_or_else(|| GroupByExpr::Expressions(vec![], vec![]));

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

        // Accept QUALIFY and WINDOW in any order and flag accordingly.
        let (named_windows, qualify, window_before_qualify) = if self.parse_keyword(Keyword::WINDOW)
        {
            let named_windows = self.parse_comma_separated(Parser::parse_named_window)?;
            if self.parse_keyword(Keyword::QUALIFY) {
                (named_windows, Some(self.parse_expr()?), true)
            } else {
                (named_windows, None, true)
            }
        } else if self.parse_keyword(Keyword::QUALIFY) {
            let qualify = Some(self.parse_expr()?);
            if self.parse_keyword(Keyword::WINDOW) {
                (
                    self.parse_comma_separated(Parser::parse_named_window)?,
                    qualify,
                    false,
                )
            } else {
                (Default::default(), qualify, false)
            }
        } else {
            Default::default()
        };

        let connect_by = if self.dialect.supports_connect_by()
            && self
                .parse_one_of_keywords(&[Keyword::START, Keyword::CONNECT])
                .is_some()
        {
            self.prev_token();
            Some(self.parse_connect_by()?)
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
            prewhere,
            selection,
            group_by,
            cluster_by,
            distribute_by,
            sort_by,
            having,
            named_window: named_windows,
            window_before_qualify,
            qualify,
            value_table_mode,
            connect_by,
        })
    }

    /// Invoke `f` after first setting the parser's `ParserState` to `state`.
    ///
    /// Upon return, restores the parser's state to what it started at.
    fn with_state<T, F>(&mut self, state: ParserState, mut f: F) -> Result<T, ParserError>
    where
        F: FnMut(&mut Parser) -> Result<T, ParserError>,
    {
        let current_state = self.state;
        self.state = state;
        let res = f(self);
        self.state = current_state;
        res
    }

    pub fn parse_connect_by(&mut self) -> Result<ConnectBy, ParserError> {
        let (condition, relationships) = if self.parse_keywords(&[Keyword::CONNECT, Keyword::BY]) {
            let relationships = self.with_state(ParserState::ConnectBy, |parser| {
                parser.parse_comma_separated(Parser::parse_expr)
            })?;
            self.expect_keywords(&[Keyword::START, Keyword::WITH])?;
            let condition = self.parse_expr()?;
            (condition, relationships)
        } else {
            self.expect_keywords(&[Keyword::START, Keyword::WITH])?;
            let condition = self.parse_expr()?;
            self.expect_keywords(&[Keyword::CONNECT, Keyword::BY])?;
            let relationships = self.with_state(ParserState::ConnectBy, |parser| {
                parser.parse_comma_separated(Parser::parse_expr)
            })?;
            (condition, relationships)
        };
        Ok(ConnectBy {
            condition,
            relationships,
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
                Some(Keyword::LOCAL) => ContextModifier::Local,
                Some(Keyword::SESSION) => ContextModifier::Session,
                _ => ContextModifier::None,
            };

            let role_name = if self.parse_keyword(Keyword::NONE) {
                None
            } else {
                Some(self.parse_identifier(false)?)
            };
            return Ok(Statement::SetRole {
                context_modifier,
                role_name,
            });
        }

        let variables = if self.parse_keywords(&[Keyword::TIME, Keyword::ZONE]) {
            OneOrManyWithParens::One(ObjectName(vec!["TIMEZONE".into()]))
        } else if self.dialect.supports_parenthesized_set_variables()
            && self.consume_token(&Token::LParen)
        {
            let variables = OneOrManyWithParens::Many(
                self.parse_comma_separated(|parser: &mut Parser<'a>| {
                    parser.parse_identifier(false)
                })?
                .into_iter()
                .map(|ident| ObjectName(vec![ident]))
                .collect(),
            );
            self.expect_token(&Token::RParen)?;
            variables
        } else {
            OneOrManyWithParens::One(self.parse_object_name(false)?)
        };

        if matches!(&variables, OneOrManyWithParens::One(variable) if variable.to_string().eq_ignore_ascii_case("NAMES")
            && dialect_of!(self is MySqlDialect | GenericDialect))
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

            return Ok(Statement::SetNames {
                charset_name,
                collation_name,
            });
        }

        let parenthesized_assignment = matches!(&variables, OneOrManyWithParens::Many(_));

        if self.consume_token(&Token::Eq) || self.parse_keyword(Keyword::TO) {
            if parenthesized_assignment {
                self.expect_token(&Token::LParen)?;
            }

            let mut values = vec![];
            loop {
                let value = if let Some(expr) = self.try_parse_expr_sub_query()? {
                    expr
                } else if let Ok(expr) = self.parse_expr() {
                    expr
                } else {
                    self.expected("variable value", self.peek_token())?
                };

                values.push(value);
                if self.consume_token(&Token::Comma) {
                    continue;
                }

                if parenthesized_assignment {
                    self.expect_token(&Token::RParen)?;
                }
                return Ok(Statement::SetVariable {
                    local: modifier == Some(Keyword::LOCAL),
                    hivevar: Some(Keyword::HIVEVAR) == modifier,
                    variables,
                    value: values,
                });
            }
        }

        let OneOrManyWithParens::One(variable) = variables else {
            return self.expected("set variable", self.peek_token());
        };

        if variable.to_string().eq_ignore_ascii_case("TIMEZONE") {
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
                let snapshot_id = self.parse_value()?;
                return Ok(Statement::SetTransaction {
                    modes: vec![],
                    snapshot: Some(snapshot_id),
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
        let session = self.parse_keyword(Keyword::SESSION);
        let global = self.parse_keyword(Keyword::GLOBAL);
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
            Ok(Statement::ShowVariables {
                filter: self.parse_show_statement_filter()?,
                session,
                global,
            })
        } else if self.parse_keyword(Keyword::STATUS)
            && dialect_of!(self is MySqlDialect | GenericDialect)
        {
            Ok(Statement::ShowStatus {
                filter: self.parse_show_statement_filter()?,
                session,
                global,
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
                "Unable to map keyword to ShowCreateObject: {keyword:?}"
            ))),
        }?;

        let obj_name = self.parse_object_name(false)?;

        Ok(Statement::ShowCreate { obj_type, obj_name })
    }

    pub fn parse_show_columns(
        &mut self,
        extended: bool,
        full: bool,
    ) -> Result<Statement, ParserError> {
        self.expect_one_of_keywords(&[Keyword::FROM, Keyword::IN])?;
        let object_name = self.parse_object_name(false)?;
        let table_name = match self.parse_one_of_keywords(&[Keyword::FROM, Keyword::IN]) {
            Some(_) => {
                let db_name = vec![self.parse_identifier(false)?];
                let ObjectName(table_name) = object_name;
                let object_name = db_name.into_iter().chain(table_name).collect();
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
            Some(_) => Some(self.parse_identifier(false)?),
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
        // Determine which keywords are recognized by the current dialect
        let parsed_keyword = if dialect_of!(self is HiveDialect) {
            // HiveDialect accepts USE DEFAULT; statement without any db specified
            if self.parse_keyword(Keyword::DEFAULT) {
                return Ok(Statement::Use(Use::Default));
            }
            None // HiveDialect doesn't expect any other specific keyword after `USE`
        } else if dialect_of!(self is DatabricksDialect) {
            self.parse_one_of_keywords(&[Keyword::CATALOG, Keyword::DATABASE, Keyword::SCHEMA])
        } else if dialect_of!(self is SnowflakeDialect) {
            self.parse_one_of_keywords(&[Keyword::DATABASE, Keyword::SCHEMA, Keyword::WAREHOUSE])
        } else {
            None // No specific keywords for other dialects, including GenericDialect
        };

        let obj_name = self.parse_object_name(false)?;
        let result = match parsed_keyword {
            Some(Keyword::CATALOG) => Use::Catalog(obj_name),
            Some(Keyword::DATABASE) => Use::Database(obj_name),
            Some(Keyword::SCHEMA) => Use::Schema(obj_name),
            Some(Keyword::WAREHOUSE) => Use::Warehouse(obj_name),
            _ => Use::Object(obj_name),
        };

        Ok(Statement::Use(result))
    }

    pub fn parse_table_and_joins(&mut self) -> Result<TableWithJoins, ParserError> {
        let relation = self.parse_table_factor()?;
        // Note that for keywords to be properly handled here, they need to be
        // added to `RESERVED_FOR_TABLE_ALIAS`, otherwise they may be parsed as
        // a table alias.
        let mut joins = vec![];
        loop {
            let global = self.parse_keyword(Keyword::GLOBAL);
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
                    global,
                    join_operator,
                }
            } else if self.parse_keyword(Keyword::OUTER) {
                // MSSQL extension, similar to LEFT JOIN LATERAL .. ON 1=1
                self.expect_keyword(Keyword::APPLY)?;
                Join {
                    relation: self.parse_table_factor()?,
                    global,
                    join_operator: JoinOperator::OuterApply,
                }
            } else if self.parse_keyword(Keyword::ASOF) {
                self.expect_keyword(Keyword::JOIN)?;
                let relation = self.parse_table_factor()?;
                self.expect_keyword(Keyword::MATCH_CONDITION)?;
                let match_condition = self.parse_parenthesized(Self::parse_expr)?;
                Join {
                    relation,
                    global,
                    join_operator: JoinOperator::AsOf {
                        match_condition,
                        constraint: self.parse_join_constraint(false)?,
                    },
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
                                    "expected OUTER, SEMI, ANTI or JOIN after {kw:?}"
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
                    global,
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
            // LATERAL must always be followed by a subquery or table function.
            if self.consume_token(&Token::LParen) {
                self.parse_derived_table_factor(Lateral)
            } else {
                let name = self.parse_object_name(false)?;
                self.expect_token(&Token::LParen)?;
                let args = self.parse_optional_args()?;
                let alias = self.parse_optional_table_alias(keywords::RESERVED_FOR_TABLE_ALIAS)?;
                Ok(TableFactor::Function {
                    lateral: true,
                    name,
                    args,
                    alias,
                })
            }
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
            if let Some(mut table) =
                self.maybe_parse(|parser| parser.parse_derived_table_factor(NotLateral))
            {
                while let Some(kw) = self.parse_one_of_keywords(&[Keyword::PIVOT, Keyword::UNPIVOT])
                {
                    table = match kw {
                        Keyword::PIVOT => self.parse_pivot_table_factor(table)?,
                        Keyword::UNPIVOT => self.parse_unpivot_table_factor(table)?,
                        _ => unreachable!(),
                    }
                }
                return Ok(table);
            }

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
                        | TableFactor::Function { alias, .. }
                        | TableFactor::UNNEST { alias, .. }
                        | TableFactor::JsonTable { alias, .. }
                        | TableFactor::TableFunction { alias, .. }
                        | TableFactor::Pivot { alias, .. }
                        | TableFactor::Unpivot { alias, .. }
                        | TableFactor::MatchRecognize { alias, .. }
                        | TableFactor::NestedJoin { alias, .. } => {
                            // but not `FROM (mytable AS alias1) AS alias2`.
                            if let Some(inner_alias) = alias {
                                return Err(ParserError::ParserError(format!(
                                    "duplicate alias {inner_alias}"
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
        } else if dialect_of!(self is SnowflakeDialect | DatabricksDialect | GenericDialect)
            && matches!(
                self.peek_tokens(),
                [
                    Token::Word(Word {
                        keyword: Keyword::VALUES,
                        ..
                    }),
                    Token::LParen
                ]
            )
        {
            self.expect_keyword(Keyword::VALUES)?;

            // Snowflake and Databricks allow syntax like below:
            // SELECT * FROM VALUES (1, 'a'), (2, 'b') AS t (col1, col2)
            // where there are no parentheses around the VALUES clause.
            let values = SetExpr::Values(self.parse_values(false)?);
            let alias = self.parse_optional_table_alias(keywords::RESERVED_FOR_TABLE_ALIAS)?;
            Ok(TableFactor::Derived {
                lateral: false,
                subquery: Box::new(Query {
                    with: None,
                    body: Box::new(values),
                    order_by: None,
                    limit: None,
                    limit_by: vec![],
                    offset: None,
                    fetch: None,
                    locks: vec![],
                    for_clause: None,
                    settings: None,
                    format_clause: None,
                }),
                alias,
            })
        } else if dialect_of!(self is BigQueryDialect | PostgreSqlDialect | GenericDialect)
            && self.parse_keyword(Keyword::UNNEST)
        {
            self.expect_token(&Token::LParen)?;
            let array_exprs = self.parse_comma_separated(Parser::parse_expr)?;
            self.expect_token(&Token::RParen)?;

            let with_ordinality = self.parse_keywords(&[Keyword::WITH, Keyword::ORDINALITY]);
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
                array_exprs,
                with_offset,
                with_offset_alias,
                with_ordinality,
            })
        } else if self.parse_keyword_with_tokens(Keyword::JSON_TABLE, &[Token::LParen]) {
            let json_expr = self.parse_expr()?;
            self.expect_token(&Token::Comma)?;
            let json_path = self.parse_value()?;
            self.expect_keyword(Keyword::COLUMNS)?;
            self.expect_token(&Token::LParen)?;
            let columns = self.parse_comma_separated(Parser::parse_json_table_column_def)?;
            self.expect_token(&Token::RParen)?;
            self.expect_token(&Token::RParen)?;
            let alias = self.parse_optional_table_alias(keywords::RESERVED_FOR_TABLE_ALIAS)?;
            Ok(TableFactor::JsonTable {
                json_expr,
                json_path,
                columns,
                alias,
            })
        } else {
            let name = self.parse_object_name(true)?;

            let partitions: Vec<Ident> = if dialect_of!(self is MySqlDialect | GenericDialect)
                && self.parse_keyword(Keyword::PARTITION)
            {
                self.parse_parenthesized_identifiers()?
            } else {
                vec![]
            };

            // Parse potential version qualifier
            let version = self.parse_table_version()?;

            // Postgres, MSSQL, ClickHouse: table-valued functions:
            let args = if self.consume_token(&Token::LParen) {
                Some(self.parse_table_function_args()?)
            } else {
                None
            };

            let with_ordinality = self.parse_keywords(&[Keyword::WITH, Keyword::ORDINALITY]);

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

            let mut table = TableFactor::Table {
                name,
                alias,
                args,
                with_hints,
                version,
                partitions,
                with_ordinality,
            };

            while let Some(kw) = self.parse_one_of_keywords(&[Keyword::PIVOT, Keyword::UNPIVOT]) {
                table = match kw {
                    Keyword::PIVOT => self.parse_pivot_table_factor(table)?,
                    Keyword::UNPIVOT => self.parse_unpivot_table_factor(table)?,
                    _ => unreachable!(),
                }
            }

            if self.dialect.supports_match_recognize()
                && self.parse_keyword(Keyword::MATCH_RECOGNIZE)
            {
                table = self.parse_match_recognize(table)?;
            }

            Ok(table)
        }
    }

    fn parse_match_recognize(&mut self, table: TableFactor) -> Result<TableFactor, ParserError> {
        self.expect_token(&Token::LParen)?;

        let partition_by = if self.parse_keywords(&[Keyword::PARTITION, Keyword::BY]) {
            self.parse_comma_separated(Parser::parse_expr)?
        } else {
            vec![]
        };

        let order_by = if self.parse_keywords(&[Keyword::ORDER, Keyword::BY]) {
            self.parse_comma_separated(Parser::parse_order_by_expr)?
        } else {
            vec![]
        };

        let measures = if self.parse_keyword(Keyword::MEASURES) {
            self.parse_comma_separated(|p| {
                let expr = p.parse_expr()?;
                let _ = p.parse_keyword(Keyword::AS);
                let alias = p.parse_identifier(false)?;
                Ok(Measure { expr, alias })
            })?
        } else {
            vec![]
        };

        let rows_per_match =
            if self.parse_keywords(&[Keyword::ONE, Keyword::ROW, Keyword::PER, Keyword::MATCH]) {
                Some(RowsPerMatch::OneRow)
            } else if self.parse_keywords(&[
                Keyword::ALL,
                Keyword::ROWS,
                Keyword::PER,
                Keyword::MATCH,
            ]) {
                Some(RowsPerMatch::AllRows(
                    if self.parse_keywords(&[Keyword::SHOW, Keyword::EMPTY, Keyword::MATCHES]) {
                        Some(EmptyMatchesMode::Show)
                    } else if self.parse_keywords(&[
                        Keyword::OMIT,
                        Keyword::EMPTY,
                        Keyword::MATCHES,
                    ]) {
                        Some(EmptyMatchesMode::Omit)
                    } else if self.parse_keywords(&[
                        Keyword::WITH,
                        Keyword::UNMATCHED,
                        Keyword::ROWS,
                    ]) {
                        Some(EmptyMatchesMode::WithUnmatched)
                    } else {
                        None
                    },
                ))
            } else {
                None
            };

        let after_match_skip =
            if self.parse_keywords(&[Keyword::AFTER, Keyword::MATCH, Keyword::SKIP]) {
                if self.parse_keywords(&[Keyword::PAST, Keyword::LAST, Keyword::ROW]) {
                    Some(AfterMatchSkip::PastLastRow)
                } else if self.parse_keywords(&[Keyword::TO, Keyword::NEXT, Keyword::ROW]) {
                    Some(AfterMatchSkip::ToNextRow)
                } else if self.parse_keywords(&[Keyword::TO, Keyword::FIRST]) {
                    Some(AfterMatchSkip::ToFirst(self.parse_identifier(false)?))
                } else if self.parse_keywords(&[Keyword::TO, Keyword::LAST]) {
                    Some(AfterMatchSkip::ToLast(self.parse_identifier(false)?))
                } else {
                    let found = self.next_token();
                    return self.expected("after match skip option", found);
                }
            } else {
                None
            };

        self.expect_keyword(Keyword::PATTERN)?;
        let pattern = self.parse_parenthesized(Self::parse_pattern)?;

        self.expect_keyword(Keyword::DEFINE)?;

        let symbols = self.parse_comma_separated(|p| {
            let symbol = p.parse_identifier(false)?;
            p.expect_keyword(Keyword::AS)?;
            let definition = p.parse_expr()?;
            Ok(SymbolDefinition { symbol, definition })
        })?;

        self.expect_token(&Token::RParen)?;

        let alias = self.parse_optional_table_alias(keywords::RESERVED_FOR_TABLE_ALIAS)?;

        Ok(TableFactor::MatchRecognize {
            table: Box::new(table),
            partition_by,
            order_by,
            measures,
            rows_per_match,
            after_match_skip,
            pattern,
            symbols,
            alias,
        })
    }

    fn parse_base_pattern(&mut self) -> Result<MatchRecognizePattern, ParserError> {
        match self.next_token().token {
            Token::Caret => Ok(MatchRecognizePattern::Symbol(MatchRecognizeSymbol::Start)),
            Token::Placeholder(s) if s == "$" => {
                Ok(MatchRecognizePattern::Symbol(MatchRecognizeSymbol::End))
            }
            Token::LBrace => {
                self.expect_token(&Token::Minus)?;
                let symbol = self
                    .parse_identifier(false)
                    .map(MatchRecognizeSymbol::Named)?;
                self.expect_token(&Token::Minus)?;
                self.expect_token(&Token::RBrace)?;
                Ok(MatchRecognizePattern::Exclude(symbol))
            }
            Token::Word(Word {
                value,
                quote_style: None,
                ..
            }) if value == "PERMUTE" => {
                self.expect_token(&Token::LParen)?;
                let symbols = self.parse_comma_separated(|p| {
                    p.parse_identifier(false).map(MatchRecognizeSymbol::Named)
                })?;
                self.expect_token(&Token::RParen)?;
                Ok(MatchRecognizePattern::Permute(symbols))
            }
            Token::LParen => {
                let pattern = self.parse_pattern()?;
                self.expect_token(&Token::RParen)?;
                Ok(MatchRecognizePattern::Group(Box::new(pattern)))
            }
            _ => {
                self.prev_token();
                self.parse_identifier(false)
                    .map(MatchRecognizeSymbol::Named)
                    .map(MatchRecognizePattern::Symbol)
            }
        }
    }

    fn parse_repetition_pattern(&mut self) -> Result<MatchRecognizePattern, ParserError> {
        let mut pattern = self.parse_base_pattern()?;
        loop {
            let token = self.next_token();
            let quantifier = match token.token {
                Token::Mul => RepetitionQuantifier::ZeroOrMore,
                Token::Plus => RepetitionQuantifier::OneOrMore,
                Token::Placeholder(s) if s == "?" => RepetitionQuantifier::AtMostOne,
                Token::LBrace => {
                    // quantifier is a range like {n} or {n,} or {,m} or {n,m}
                    let token = self.next_token();
                    match token.token {
                        Token::Comma => {
                            let next_token = self.next_token();
                            let Token::Number(n, _) = next_token.token else {
                                return self.expected("literal number", next_token);
                            };
                            self.expect_token(&Token::RBrace)?;
                            RepetitionQuantifier::AtMost(Self::parse(n, token.location)?)
                        }
                        Token::Number(n, _) if self.consume_token(&Token::Comma) => {
                            let next_token = self.next_token();
                            match next_token.token {
                                Token::Number(m, _) => {
                                    self.expect_token(&Token::RBrace)?;
                                    RepetitionQuantifier::Range(
                                        Self::parse(n, token.location)?,
                                        Self::parse(m, token.location)?,
                                    )
                                }
                                Token::RBrace => {
                                    RepetitionQuantifier::AtLeast(Self::parse(n, token.location)?)
                                }
                                _ => {
                                    return self.expected("} or upper bound", next_token);
                                }
                            }
                        }
                        Token::Number(n, _) => {
                            self.expect_token(&Token::RBrace)?;
                            RepetitionQuantifier::Exactly(Self::parse(n, token.location)?)
                        }
                        _ => return self.expected("quantifier range", token),
                    }
                }
                _ => {
                    self.prev_token();
                    break;
                }
            };
            pattern = MatchRecognizePattern::Repetition(Box::new(pattern), quantifier);
        }
        Ok(pattern)
    }

    fn parse_concat_pattern(&mut self) -> Result<MatchRecognizePattern, ParserError> {
        let mut patterns = vec![self.parse_repetition_pattern()?];
        while !matches!(self.peek_token().token, Token::RParen | Token::Pipe) {
            patterns.push(self.parse_repetition_pattern()?);
        }
        match <[MatchRecognizePattern; 1]>::try_from(patterns) {
            Ok([pattern]) => Ok(pattern),
            Err(patterns) => Ok(MatchRecognizePattern::Concat(patterns)),
        }
    }

    fn parse_pattern(&mut self) -> Result<MatchRecognizePattern, ParserError> {
        let pattern = self.parse_concat_pattern()?;
        if self.consume_token(&Token::Pipe) {
            match self.parse_pattern()? {
                // flatten nested alternations
                MatchRecognizePattern::Alternation(mut patterns) => {
                    patterns.insert(0, pattern);
                    Ok(MatchRecognizePattern::Alternation(patterns))
                }
                next => Ok(MatchRecognizePattern::Alternation(vec![pattern, next])),
            }
        } else {
            Ok(pattern)
        }
    }

    /// Parse a given table version specifier.
    ///
    /// For now it only supports timestamp versioning for BigQuery and MSSQL dialects.
    pub fn parse_table_version(&mut self) -> Result<Option<TableVersion>, ParserError> {
        if dialect_of!(self is BigQueryDialect | MsSqlDialect)
            && self.parse_keywords(&[Keyword::FOR, Keyword::SYSTEM_TIME, Keyword::AS, Keyword::OF])
        {
            let expr = self.parse_expr()?;
            Ok(Some(TableVersion::ForSystemTimeAsOf(expr)))
        } else {
            Ok(None)
        }
    }

    /// Parses MySQL's JSON_TABLE column definition.
    /// For example: `id INT EXISTS PATH '$' DEFAULT '0' ON EMPTY ERROR ON ERROR`
    pub fn parse_json_table_column_def(&mut self) -> Result<JsonTableColumn, ParserError> {
        let name = self.parse_identifier(false)?;
        let r#type = self.parse_data_type()?;
        let exists = self.parse_keyword(Keyword::EXISTS);
        self.expect_keyword(Keyword::PATH)?;
        let path = self.parse_value()?;
        let mut on_empty = None;
        let mut on_error = None;
        while let Some(error_handling) = self.parse_json_table_column_error_handling()? {
            if self.parse_keyword(Keyword::EMPTY) {
                on_empty = Some(error_handling);
            } else {
                self.expect_keyword(Keyword::ERROR)?;
                on_error = Some(error_handling);
            }
        }
        Ok(JsonTableColumn {
            name,
            r#type,
            path,
            exists,
            on_empty,
            on_error,
        })
    }

    fn parse_json_table_column_error_handling(
        &mut self,
    ) -> Result<Option<JsonTableColumnErrorHandling>, ParserError> {
        let res = if self.parse_keyword(Keyword::NULL) {
            JsonTableColumnErrorHandling::Null
        } else if self.parse_keyword(Keyword::ERROR) {
            JsonTableColumnErrorHandling::Error
        } else if self.parse_keyword(Keyword::DEFAULT) {
            JsonTableColumnErrorHandling::Default(self.parse_value()?)
        } else {
            return Ok(None);
        };
        self.expect_keyword(Keyword::ON)?;
        Ok(Some(res))
    }

    pub fn parse_derived_table_factor(
        &mut self,
        lateral: IsLateral,
    ) -> Result<TableFactor, ParserError> {
        let subquery = self.parse_boxed_query()?;
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

    fn parse_aliased_function_call(&mut self) -> Result<ExprWithAlias, ParserError> {
        let function_name = match self.next_token().token {
            Token::Word(w) => Ok(w.value),
            _ => self.expected("a function identifier", self.peek_token()),
        }?;
        let expr = self.parse_function(ObjectName(vec![Ident::new(function_name)]))?;
        let alias = if self.parse_keyword(Keyword::AS) {
            Some(self.parse_identifier(false)?)
        } else {
            None
        };

        Ok(ExprWithAlias { expr, alias })
    }

    fn parse_expr_with_alias(&mut self) -> Result<ExprWithAlias, ParserError> {
        let expr = self.parse_expr()?;
        let alias = if self.parse_keyword(Keyword::AS) {
            Some(self.parse_identifier(false)?)
        } else {
            None
        };

        Ok(ExprWithAlias { expr, alias })
    }

    pub fn parse_pivot_table_factor(
        &mut self,
        table: TableFactor,
    ) -> Result<TableFactor, ParserError> {
        self.expect_token(&Token::LParen)?;
        let aggregate_functions = self.parse_comma_separated(Self::parse_aliased_function_call)?;
        self.expect_keyword(Keyword::FOR)?;
        let value_column = self.parse_object_name(false)?.0;
        self.expect_keyword(Keyword::IN)?;

        self.expect_token(&Token::LParen)?;
        let value_source = if self.parse_keyword(Keyword::ANY) {
            let order_by = if self.parse_keywords(&[Keyword::ORDER, Keyword::BY]) {
                self.parse_comma_separated(Parser::parse_order_by_expr)?
            } else {
                vec![]
            };
            PivotValueSource::Any(order_by)
        } else if self
            .parse_one_of_keywords(&[Keyword::SELECT, Keyword::WITH])
            .is_some()
        {
            self.prev_token();
            PivotValueSource::Subquery(self.parse_query()?)
        } else {
            PivotValueSource::List(self.parse_comma_separated(Self::parse_expr_with_alias)?)
        };
        self.expect_token(&Token::RParen)?;

        let default_on_null =
            if self.parse_keywords(&[Keyword::DEFAULT, Keyword::ON, Keyword::NULL]) {
                self.expect_token(&Token::LParen)?;
                let expr = self.parse_expr()?;
                self.expect_token(&Token::RParen)?;
                Some(expr)
            } else {
                None
            };

        self.expect_token(&Token::RParen)?;
        let alias = self.parse_optional_table_alias(keywords::RESERVED_FOR_TABLE_ALIAS)?;
        Ok(TableFactor::Pivot {
            table: Box::new(table),
            aggregate_functions,
            value_column,
            value_source,
            default_on_null,
            alias,
        })
    }

    pub fn parse_unpivot_table_factor(
        &mut self,
        table: TableFactor,
    ) -> Result<TableFactor, ParserError> {
        self.expect_token(&Token::LParen)?;
        let value = self.parse_identifier(false)?;
        self.expect_keyword(Keyword::FOR)?;
        let name = self.parse_identifier(false)?;
        self.expect_keyword(Keyword::IN)?;
        let columns = self.parse_parenthesized_column_list(Mandatory, false)?;
        self.expect_token(&Token::RParen)?;
        let alias = self.parse_optional_table_alias(keywords::RESERVED_FOR_TABLE_ALIAS)?;
        Ok(TableFactor::Unpivot {
            table: Box::new(table),
            value,
            name,
            columns,
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
            let columns = self.parse_parenthesized_column_list(Mandatory, false)?;
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
        let grantees = self.parse_comma_separated(|p| p.parse_identifier(false))?;

        let with_grant_option =
            self.parse_keywords(&[Keyword::WITH, Keyword::GRANT, Keyword::OPTION]);

        let granted_by = self
            .parse_keywords(&[Keyword::GRANTED, Keyword::BY])
            .then(|| self.parse_identifier(false).unwrap());

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
                .parse_actions_list()?
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
                    "INTERNAL ERROR: GRANT/REVOKE unexpected keyword(s) - {errors:?}"
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
                schemas: self.parse_comma_separated(|p| p.parse_object_name(false))?,
            }
        } else if self.parse_keywords(&[
            Keyword::ALL,
            Keyword::SEQUENCES,
            Keyword::IN,
            Keyword::SCHEMA,
        ]) {
            GrantObjects::AllSequencesInSchema {
                schemas: self.parse_comma_separated(|p| p.parse_object_name(false))?,
            }
        } else {
            let object_type =
                self.parse_one_of_keywords(&[Keyword::SEQUENCE, Keyword::SCHEMA, Keyword::TABLE]);
            let objects = self.parse_comma_separated(|p| p.parse_object_name(false));
            match object_type {
                Some(Keyword::SCHEMA) => GrantObjects::Schemas(objects?),
                Some(Keyword::SEQUENCE) => GrantObjects::Sequences(objects?),
                Some(Keyword::TABLE) | None => GrantObjects::Tables(objects?),
                _ => unreachable!(),
            }
        };

        Ok((privileges, objects))
    }

    pub fn parse_grant_permission(&mut self) -> Result<ParsedAction, ParserError> {
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
                    let columns = self.parse_parenthesized_column_list(Optional, false)?;
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
        let grantees = self.parse_comma_separated(|p| p.parse_identifier(false))?;

        let granted_by = self
            .parse_keywords(&[Keyword::GRANTED, Keyword::BY])
            .then(|| self.parse_identifier(false).unwrap());

        let loc = self.peek_token().location;
        let cascade = self.parse_keyword(Keyword::CASCADE);
        let restrict = self.parse_keyword(Keyword::RESTRICT);
        if cascade && restrict {
            return parser_err!("Cannot specify both CASCADE and RESTRICT in REVOKE", loc);
        }

        Ok(Statement::Revoke {
            privileges,
            objects,
            grantees,
            granted_by,
            cascade,
        })
    }

    /// Parse an REPLACE statement
    pub fn parse_replace(&mut self) -> Result<Statement, ParserError> {
        if !dialect_of!(self is MySqlDialect | GenericDialect) {
            return parser_err!("Unsupported statement REPLACE", self.peek_token().location);
        }

        let insert = &mut self.parse_insert()?;
        if let Statement::Insert(Insert { replace_into, .. }) = insert {
            *replace_into = true;
        }

        Ok(insert.clone())
    }

    /// Parse an INSERT statement, returning a `Box`ed SetExpr
    ///
    /// This is used to reduce the size of the stack frames in debug builds
    fn parse_insert_setexpr_boxed(&mut self) -> Result<Box<SetExpr>, ParserError> {
        Ok(Box::new(SetExpr::Insert(self.parse_insert()?)))
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

        let priority = if !dialect_of!(self is MySqlDialect | GenericDialect) {
            None
        } else if self.parse_keyword(Keyword::LOW_PRIORITY) {
            Some(MysqlInsertPriority::LowPriority)
        } else if self.parse_keyword(Keyword::DELAYED) {
            Some(MysqlInsertPriority::Delayed)
        } else if self.parse_keyword(Keyword::HIGH_PRIORITY) {
            Some(MysqlInsertPriority::HighPriority)
        } else {
            None
        };

        let ignore = dialect_of!(self is MySqlDialect | GenericDialect)
            && self.parse_keyword(Keyword::IGNORE);

        let replace_into = false;

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
            let source = self.parse_boxed_query()?;
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
            let table_name = self.parse_object_name(false)?;

            let table_alias =
                if dialect_of!(self is PostgreSqlDialect) && self.parse_keyword(Keyword::AS) {
                    Some(self.parse_identifier(false)?)
                } else {
                    None
                };

            let is_mysql = dialect_of!(self is MySqlDialect);

            let (columns, partitioned, after_columns, source) =
                if self.parse_keywords(&[Keyword::DEFAULT, Keyword::VALUES]) {
                    (vec![], None, vec![], None)
                } else {
                    let columns = self.parse_parenthesized_column_list(Optional, is_mysql)?;

                    let partitioned = self.parse_insert_partition()?;
                    // Hive allows you to specify columns after partitions as well if you want.
                    let after_columns = if dialect_of!(self is HiveDialect) {
                        self.parse_parenthesized_column_list(Optional, false)?
                    } else {
                        vec![]
                    };

                    let source = Some(self.parse_boxed_query()?);

                    (columns, partitioned, after_columns, source)
                };

            let insert_alias = if dialect_of!(self is MySqlDialect | GenericDialect)
                && self.parse_keyword(Keyword::AS)
            {
                let row_alias = self.parse_object_name(false)?;
                let col_aliases = Some(self.parse_parenthesized_column_list(Optional, false)?);
                Some(InsertAliases {
                    row_alias,
                    col_aliases,
                })
            } else {
                None
            };

            let on = if self.parse_keyword(Keyword::ON) {
                if self.parse_keyword(Keyword::CONFLICT) {
                    let conflict_target =
                        if self.parse_keywords(&[Keyword::ON, Keyword::CONSTRAINT]) {
                            Some(ConflictTarget::OnConstraint(self.parse_object_name(false)?))
                        } else if self.peek_token() == Token::LParen {
                            Some(ConflictTarget::Columns(
                                self.parse_parenthesized_column_list(IsOptional::Mandatory, false)?,
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

            Ok(Statement::Insert(Insert {
                or,
                table_name,
                table_alias,
                ignore,
                into,
                overwrite,
                partitioned,
                columns,
                after_columns,
                source,
                table,
                on,
                returning,
                replace_into,
                priority,
                insert_alias,
            }))
        }
    }

    pub fn parse_insert_partition(&mut self) -> Result<Option<Vec<Expr>>, ParserError> {
        if self.parse_keyword(Keyword::PARTITION) {
            self.expect_token(&Token::LParen)?;
            let partition_cols = Some(self.parse_comma_separated(Parser::parse_expr)?);
            self.expect_token(&Token::RParen)?;
            Ok(partition_cols)
        } else {
            Ok(None)
        }
    }

    /// Parse an UPDATE statement, returning a `Box`ed SetExpr
    ///
    /// This is used to reduce the size of the stack frames in debug builds
    fn parse_update_setexpr_boxed(&mut self) -> Result<Box<SetExpr>, ParserError> {
        Ok(Box::new(SetExpr::Update(self.parse_update()?)))
    }

    pub fn parse_update(&mut self) -> Result<Statement, ParserError> {
        let table = self.parse_table_and_joins()?;
        self.expect_keyword(Keyword::SET)?;
        let assignments = self.parse_comma_separated(Parser::parse_assignment)?;
        let from = if self.parse_keyword(Keyword::FROM)
            && dialect_of!(self is GenericDialect | PostgreSqlDialect | DuckDbDialect | BigQueryDialect | SnowflakeDialect | RedshiftSqlDialect | MsSqlDialect | SQLiteDialect )
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
        let target = self.parse_assignment_target()?;
        self.expect_token(&Token::Eq)?;
        let value = self.parse_expr()?;
        Ok(Assignment { target, value })
    }

    /// Parse the left-hand side of an assignment, used in an UPDATE statement
    pub fn parse_assignment_target(&mut self) -> Result<AssignmentTarget, ParserError> {
        if self.consume_token(&Token::LParen) {
            let columns = self.parse_comma_separated(|p| p.parse_object_name(false))?;
            self.expect_token(&Token::RParen)?;
            Ok(AssignmentTarget::Tuple(columns))
        } else {
            let column = self.parse_object_name(false)?;
            Ok(AssignmentTarget::ColumnName(column))
        }
    }

    pub fn parse_function_args(&mut self) -> Result<FunctionArg, ParserError> {
        if self.peek_nth_token(1) == Token::RArrow {
            let name = self.parse_identifier(false)?;

            self.expect_token(&Token::RArrow)?;
            let arg = self.parse_wildcard_expr()?.into();

            Ok(FunctionArg::Named {
                name,
                arg,
                operator: FunctionArgOperator::RightArrow,
            })
        } else if self.dialect.supports_named_fn_args_with_eq_operator()
            && self.peek_nth_token(1) == Token::Eq
        {
            let name = self.parse_identifier(false)?;

            self.expect_token(&Token::Eq)?;
            let arg = self.parse_wildcard_expr()?.into();

            Ok(FunctionArg::Named {
                name,
                arg,
                operator: FunctionArgOperator::Equals,
            })
        } else if dialect_of!(self is DuckDbDialect | GenericDialect)
            && self.peek_nth_token(1) == Token::Assignment
        {
            let name = self.parse_identifier(false)?;

            self.expect_token(&Token::Assignment)?;
            let arg = self.parse_expr()?.into();

            Ok(FunctionArg::Named {
                name,
                arg,
                operator: FunctionArgOperator::Assignment,
            })
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

    fn parse_table_function_args(&mut self) -> Result<TableFunctionArgs, ParserError> {
        if self.consume_token(&Token::RParen) {
            return Ok(TableFunctionArgs {
                args: vec![],
                settings: None,
            });
        }
        let mut args = vec![];
        let settings = loop {
            if let Some(settings) = self.parse_settings()? {
                break Some(settings);
            }
            args.push(self.parse_function_args()?);
            if self.is_parse_comma_separated_end() {
                break None;
            }
        };
        self.expect_token(&Token::RParen)?;
        Ok(TableFunctionArgs { args, settings })
    }

    /// Parses a potentially empty list of arguments to a window function
    /// (including the closing parenthesis).
    ///
    /// Examples:
    /// ```sql
    /// FIRST_VALUE(x ORDER BY 1,2,3);
    /// FIRST_VALUE(x IGNORE NULL);
    /// ```
    fn parse_function_argument_list(&mut self) -> Result<FunctionArgumentList, ParserError> {
        if self.consume_token(&Token::RParen) {
            return Ok(FunctionArgumentList {
                duplicate_treatment: None,
                args: vec![],
                clauses: vec![],
            });
        }

        let duplicate_treatment = self.parse_duplicate_treatment()?;
        let args = self.parse_comma_separated(Parser::parse_function_args)?;

        let mut clauses = vec![];

        if self.dialect.supports_window_function_null_treatment_arg() {
            if let Some(null_treatment) = self.parse_null_treatment()? {
                clauses.push(FunctionArgumentClause::IgnoreOrRespectNulls(null_treatment));
            }
        }

        if self.parse_keywords(&[Keyword::ORDER, Keyword::BY]) {
            clauses.push(FunctionArgumentClause::OrderBy(
                self.parse_comma_separated(Parser::parse_order_by_expr)?,
            ));
        }

        if self.parse_keyword(Keyword::LIMIT) {
            clauses.push(FunctionArgumentClause::Limit(self.parse_expr()?));
        }

        if dialect_of!(self is GenericDialect | BigQueryDialect)
            && self.parse_keyword(Keyword::HAVING)
        {
            let kind = match self.expect_one_of_keywords(&[Keyword::MIN, Keyword::MAX])? {
                Keyword::MIN => HavingBoundKind::Min,
                Keyword::MAX => HavingBoundKind::Max,
                _ => unreachable!(),
            };
            clauses.push(FunctionArgumentClause::Having(HavingBound(
                kind,
                self.parse_expr()?,
            )))
        }

        if dialect_of!(self is GenericDialect | MySqlDialect)
            && self.parse_keyword(Keyword::SEPARATOR)
        {
            clauses.push(FunctionArgumentClause::Separator(self.parse_value()?));
        }

        if let Some(on_overflow) = self.parse_listagg_on_overflow()? {
            clauses.push(FunctionArgumentClause::OnOverflow(on_overflow));
        }

        self.expect_token(&Token::RParen)?;
        Ok(FunctionArgumentList {
            duplicate_treatment,
            args,
            clauses,
        })
    }

    fn parse_duplicate_treatment(&mut self) -> Result<Option<DuplicateTreatment>, ParserError> {
        let loc = self.peek_token().location;
        match (
            self.parse_keyword(Keyword::ALL),
            self.parse_keyword(Keyword::DISTINCT),
        ) {
            (true, false) => Ok(Some(DuplicateTreatment::All)),
            (false, true) => Ok(Some(DuplicateTreatment::Distinct)),
            (false, false) => Ok(None),
            (true, true) => parser_err!("Cannot specify both ALL and DISTINCT".to_string(), loc),
        }
    }

    /// Parse a comma-delimited list of projections after SELECT
    pub fn parse_select_item(&mut self) -> Result<SelectItem, ParserError> {
        match self.parse_wildcard_expr()? {
            Expr::QualifiedWildcard(prefix) => Ok(SelectItem::QualifiedWildcard(
                prefix,
                self.parse_wildcard_additional_options()?,
            )),
            Expr::Wildcard => Ok(SelectItem::Wildcard(
                self.parse_wildcard_additional_options()?,
            )),
            Expr::Identifier(v) if v.value.to_lowercase() == "from" && v.quote_style.is_none() => {
                parser_err!(
                    format!("Expected an expression, found: {}", v),
                    self.peek_token().location
                )
            }
            expr => self
                .parse_optional_alias(keywords::RESERVED_FOR_COLUMN_ALIAS)
                .map(|alias| match alias {
                    Some(alias) => SelectItem::ExprWithAlias { expr, alias },
                    None => SelectItem::UnnamedExpr(expr),
                }),
        }
    }

    /// Parse an [`WildcardAdditionalOptions`] information for wildcard select items.
    ///
    /// If it is not possible to parse it, will return an option.
    pub fn parse_wildcard_additional_options(
        &mut self,
    ) -> Result<WildcardAdditionalOptions, ParserError> {
        let opt_ilike = if dialect_of!(self is GenericDialect | SnowflakeDialect) {
            self.parse_optional_select_item_ilike()?
        } else {
            None
        };
        let opt_exclude = if opt_ilike.is_none()
            && dialect_of!(self is GenericDialect | DuckDbDialect | SnowflakeDialect)
        {
            self.parse_optional_select_item_exclude()?
        } else {
            None
        };
        let opt_except = if self.dialect.supports_select_wildcard_except() {
            self.parse_optional_select_item_except()?
        } else {
            None
        };
        let opt_replace = if dialect_of!(self is GenericDialect | BigQueryDialect | ClickHouseDialect | DuckDbDialect | SnowflakeDialect)
        {
            self.parse_optional_select_item_replace()?
        } else {
            None
        };
        let opt_rename = if dialect_of!(self is GenericDialect | SnowflakeDialect) {
            self.parse_optional_select_item_rename()?
        } else {
            None
        };

        Ok(WildcardAdditionalOptions {
            opt_ilike,
            opt_exclude,
            opt_except,
            opt_rename,
            opt_replace,
        })
    }

    /// Parse an [`Ilike`](IlikeSelectItem) information for wildcard select items.
    ///
    /// If it is not possible to parse it, will return an option.
    pub fn parse_optional_select_item_ilike(
        &mut self,
    ) -> Result<Option<IlikeSelectItem>, ParserError> {
        let opt_ilike = if self.parse_keyword(Keyword::ILIKE) {
            let next_token = self.next_token();
            let pattern = match next_token.token {
                Token::SingleQuotedString(s) => s,
                _ => return self.expected("ilike pattern", next_token),
            };
            Some(IlikeSelectItem { pattern })
        } else {
            None
        };
        Ok(opt_ilike)
    }

    /// Parse an [`Exclude`](ExcludeSelectItem) information for wildcard select items.
    ///
    /// If it is not possible to parse it, will return an option.
    pub fn parse_optional_select_item_exclude(
        &mut self,
    ) -> Result<Option<ExcludeSelectItem>, ParserError> {
        let opt_exclude = if self.parse_keyword(Keyword::EXCLUDE) {
            if self.consume_token(&Token::LParen) {
                let columns =
                    self.parse_comma_separated(|parser| parser.parse_identifier(false))?;
                self.expect_token(&Token::RParen)?;
                Some(ExcludeSelectItem::Multiple(columns))
            } else {
                let column = self.parse_identifier(false)?;
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
            if self.peek_token().token == Token::LParen {
                let idents = self.parse_parenthesized_column_list(Mandatory, false)?;
                match &idents[..] {
                    [] => {
                        return self.expected(
                            "at least one column should be parsed by the expect clause",
                            self.peek_token(),
                        )?;
                    }
                    [first, idents @ ..] => Some(ExceptSelectItem {
                        first_element: first.clone(),
                        additional_elements: idents.to_vec(),
                    }),
                }
            } else {
                // Clickhouse allows EXCEPT column_name
                let ident = self.parse_identifier(false)?;
                Some(ExceptSelectItem {
                    first_element: ident,
                    additional_elements: vec![],
                })
            }
        } else {
            None
        };

        Ok(opt_except)
    }

    /// Parse a [`Rename`](RenameSelectItem) information for wildcard select items.
    pub fn parse_optional_select_item_rename(
        &mut self,
    ) -> Result<Option<RenameSelectItem>, ParserError> {
        let opt_rename = if self.parse_keyword(Keyword::RENAME) {
            if self.consume_token(&Token::LParen) {
                let idents =
                    self.parse_comma_separated(|parser| parser.parse_identifier_with_alias())?;
                self.expect_token(&Token::RParen)?;
                Some(RenameSelectItem::Multiple(idents))
            } else {
                let ident = self.parse_identifier_with_alias()?;
                Some(RenameSelectItem::Single(ident))
            }
        } else {
            None
        };

        Ok(opt_rename)
    }

    /// Parse a [`Replace`](ReplaceSelectItem) information for wildcard select items.
    pub fn parse_optional_select_item_replace(
        &mut self,
    ) -> Result<Option<ReplaceSelectItem>, ParserError> {
        let opt_replace = if self.parse_keyword(Keyword::REPLACE) {
            if self.consume_token(&Token::LParen) {
                let items = self.parse_comma_separated(|parser| {
                    Ok(Box::new(parser.parse_replace_elements()?))
                })?;
                self.expect_token(&Token::RParen)?;
                Some(ReplaceSelectItem { items })
            } else {
                let tok = self.next_token();
                return self.expected("( after REPLACE but", tok);
            }
        } else {
            None
        };

        Ok(opt_replace)
    }
    pub fn parse_replace_elements(&mut self) -> Result<ReplaceSelectElement, ParserError> {
        let expr = self.parse_expr()?;
        let as_keyword = self.parse_keyword(Keyword::AS);
        let ident = self.parse_identifier(false)?;
        Ok(ReplaceSelectElement {
            expr,
            column_name: ident,
            as_keyword,
        })
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

        let with_fill = if dialect_of!(self is ClickHouseDialect | GenericDialect)
            && self.parse_keywords(&[Keyword::WITH, Keyword::FILL])
        {
            Some(self.parse_with_fill()?)
        } else {
            None
        };

        Ok(OrderByExpr {
            expr,
            asc,
            nulls_first,
            with_fill,
        })
    }

    // Parse a WITH FILL clause (ClickHouse dialect)
    // that follow the WITH FILL keywords in a ORDER BY clause
    pub fn parse_with_fill(&mut self) -> Result<WithFill, ParserError> {
        let from = if self.parse_keyword(Keyword::FROM) {
            Some(self.parse_expr()?)
        } else {
            None
        };

        let to = if self.parse_keyword(Keyword::TO) {
            Some(self.parse_expr()?)
        } else {
            None
        };

        let step = if self.parse_keyword(Keyword::STEP) {
            Some(self.parse_expr()?)
        } else {
            None
        };

        Ok(WithFill { from, to, step })
    }

    // Parse a set of comma seperated INTERPOLATE expressions (ClickHouse dialect)
    // that follow the INTERPOLATE keyword in an ORDER BY clause with the WITH FILL modifier
    pub fn parse_interpolations(&mut self) -> Result<Option<Interpolate>, ParserError> {
        if !self.parse_keyword(Keyword::INTERPOLATE) {
            return Ok(None);
        }

        if self.consume_token(&Token::LParen) {
            let interpolations =
                self.parse_comma_separated0(|p| p.parse_interpolation(), Token::RParen)?;
            self.expect_token(&Token::RParen)?;
            // INTERPOLATE () and INTERPOLATE ( ... ) variants
            return Ok(Some(Interpolate {
                exprs: Some(interpolations),
            }));
        }

        // INTERPOLATE
        Ok(Some(Interpolate { exprs: None }))
    }

    // Parse a INTERPOLATE expression (ClickHouse dialect)
    pub fn parse_interpolation(&mut self) -> Result<InterpolateExpr, ParserError> {
        let column = self.parse_identifier(false)?;
        let expr = if self.parse_keyword(Keyword::AS) {
            Some(self.parse_expr()?)
        } else {
            None
        };
        Ok(InterpolateExpr { column, expr })
    }

    /// Parse a TOP clause, MSSQL equivalent of LIMIT,
    /// that follows after `SELECT [DISTINCT]`.
    pub fn parse_top(&mut self) -> Result<Top, ParserError> {
        let quantity = if self.consume_token(&Token::LParen) {
            let quantity = self.parse_expr()?;
            self.expect_token(&Token::RParen)?;
            Some(TopQuantity::Expr(quantity))
        } else {
            let next_token = self.next_token();
            let quantity = match next_token.token {
                Token::Number(s, _) => Self::parse::<u64>(s, next_token.location)?,
                _ => self.expected("literal int", next_token)?,
            };
            Some(TopQuantity::Constant(quantity))
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
            Some(self.parse_object_name(false)?)
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

    pub fn parse_values(&mut self, allow_empty: bool) -> Result<Values, ParserError> {
        let mut explicit_row = false;

        let rows = self.parse_comma_separated(|parser| {
            if parser.parse_keyword(Keyword::ROW) {
                explicit_row = true;
            }

            parser.expect_token(&Token::LParen)?;
            if allow_empty && parser.peek_token().token == Token::RParen {
                parser.next_token();
                Ok(vec![])
            } else {
                let exprs = parser.parse_comma_separated(Parser::parse_expr)?;
                parser.expect_token(&Token::RParen)?;
                Ok(exprs)
            }
        })?;
        Ok(Values { explicit_row, rows })
    }

    pub fn parse_start_transaction(&mut self) -> Result<Statement, ParserError> {
        self.expect_keyword(Keyword::TRANSACTION)?;
        Ok(Statement::StartTransaction {
            modes: self.parse_transaction_modes()?,
            begin: false,
            modifier: None,
        })
    }

    pub fn parse_begin(&mut self) -> Result<Statement, ParserError> {
        let modifier = if !self.dialect.supports_start_transaction_modifier() {
            None
        } else if self.parse_keyword(Keyword::DEFERRED) {
            Some(TransactionModifier::Deferred)
        } else if self.parse_keyword(Keyword::IMMEDIATE) {
            Some(TransactionModifier::Immediate)
        } else if self.parse_keyword(Keyword::EXCLUSIVE) {
            Some(TransactionModifier::Exclusive)
        } else {
            None
        };
        let _ = self.parse_one_of_keywords(&[Keyword::TRANSACTION, Keyword::WORK]);
        Ok(Statement::StartTransaction {
            modes: self.parse_transaction_modes()?,
            begin: true,
            modifier,
        })
    }

    pub fn parse_end(&mut self) -> Result<Statement, ParserError> {
        Ok(Statement::Commit {
            chain: self.parse_commit_rollback_chain()?,
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
        let chain = self.parse_commit_rollback_chain()?;
        let savepoint = self.parse_rollback_savepoint()?;

        Ok(Statement::Rollback { chain, savepoint })
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

    pub fn parse_rollback_savepoint(&mut self) -> Result<Option<Ident>, ParserError> {
        if self.parse_keyword(Keyword::TO) {
            let _ = self.parse_keyword(Keyword::SAVEPOINT);
            let savepoint = self.parse_identifier(false)?;

            Ok(Some(savepoint))
        } else {
            Ok(None)
        }
    }

    pub fn parse_deallocate(&mut self) -> Result<Statement, ParserError> {
        let prepare = self.parse_keyword(Keyword::PREPARE);
        let name = self.parse_identifier(false)?;
        Ok(Statement::Deallocate { name, prepare })
    }

    pub fn parse_execute(&mut self) -> Result<Statement, ParserError> {
        let name = self.parse_identifier(false)?;

        let mut parameters = vec![];
        if self.consume_token(&Token::LParen) {
            parameters = self.parse_comma_separated(Parser::parse_expr)?;
            self.expect_token(&Token::RParen)?;
        }

        let mut using = vec![];
        if self.parse_keyword(Keyword::USING) {
            using.push(self.parse_expr()?);

            while self.consume_token(&Token::Comma) {
                using.push(self.parse_expr()?);
            }
        };

        Ok(Statement::Execute {
            name,
            parameters,
            using,
        })
    }

    pub fn parse_prepare(&mut self) -> Result<Statement, ParserError> {
        let name = self.parse_identifier(false)?;

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

    pub fn parse_unload(&mut self) -> Result<Statement, ParserError> {
        self.expect_token(&Token::LParen)?;
        let query = self.parse_boxed_query()?;
        self.expect_token(&Token::RParen)?;

        self.expect_keyword(Keyword::TO)?;
        let to = self.parse_identifier(false)?;

        let with_options = self.parse_options(Keyword::WITH)?;

        Ok(Statement::Unload {
            query,
            to,
            with: with_options,
        })
    }

    pub fn parse_merge_clauses(&mut self) -> Result<Vec<MergeClause>, ParserError> {
        let mut clauses = vec![];
        loop {
            if self.peek_token() == Token::EOF || self.peek_token() == Token::SemiColon {
                break;
            }
            self.expect_keyword(Keyword::WHEN)?;

            let mut clause_kind = MergeClauseKind::Matched;
            if self.parse_keyword(Keyword::NOT) {
                clause_kind = MergeClauseKind::NotMatched;
            }
            self.expect_keyword(Keyword::MATCHED)?;

            if matches!(clause_kind, MergeClauseKind::NotMatched)
                && self.parse_keywords(&[Keyword::BY, Keyword::SOURCE])
            {
                clause_kind = MergeClauseKind::NotMatchedBySource;
            } else if matches!(clause_kind, MergeClauseKind::NotMatched)
                && self.parse_keywords(&[Keyword::BY, Keyword::TARGET])
            {
                clause_kind = MergeClauseKind::NotMatchedByTarget;
            }

            let predicate = if self.parse_keyword(Keyword::AND) {
                Some(self.parse_expr()?)
            } else {
                None
            };

            self.expect_keyword(Keyword::THEN)?;

            let merge_clause = match self.parse_one_of_keywords(&[
                Keyword::UPDATE,
                Keyword::INSERT,
                Keyword::DELETE,
            ]) {
                Some(Keyword::UPDATE) => {
                    if matches!(
                        clause_kind,
                        MergeClauseKind::NotMatched | MergeClauseKind::NotMatchedByTarget
                    ) {
                        return Err(ParserError::ParserError(format!(
                            "UPDATE is not allowed in a {clause_kind} merge clause"
                        )));
                    }
                    self.expect_keyword(Keyword::SET)?;
                    MergeAction::Update {
                        assignments: self.parse_comma_separated(Parser::parse_assignment)?,
                    }
                }
                Some(Keyword::DELETE) => {
                    if matches!(
                        clause_kind,
                        MergeClauseKind::NotMatched | MergeClauseKind::NotMatchedByTarget
                    ) {
                        return Err(ParserError::ParserError(format!(
                            "DELETE is not allowed in a {clause_kind} merge clause"
                        )));
                    }
                    MergeAction::Delete
                }
                Some(Keyword::INSERT) => {
                    if !matches!(
                        clause_kind,
                        MergeClauseKind::NotMatched | MergeClauseKind::NotMatchedByTarget
                    ) {
                        return Err(ParserError::ParserError(format!(
                            "INSERT is not allowed in a {clause_kind} merge clause"
                        )));
                    }
                    let is_mysql = dialect_of!(self is MySqlDialect);

                    let columns = self.parse_parenthesized_column_list(Optional, is_mysql)?;
                    let kind = if dialect_of!(self is BigQueryDialect | GenericDialect)
                        && self.parse_keyword(Keyword::ROW)
                    {
                        MergeInsertKind::Row
                    } else {
                        self.expect_keyword(Keyword::VALUES)?;
                        let values = self.parse_values(is_mysql)?;
                        MergeInsertKind::Values(values)
                    };
                    MergeAction::Insert(MergeInsertExpr { columns, kind })
                }
                _ => {
                    return Err(ParserError::ParserError(
                        "expected UPDATE, DELETE or INSERT in merge clause".to_string(),
                    ));
                }
            };
            clauses.push(MergeClause {
                clause_kind,
                predicate,
                action: merge_clause,
            });
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

    fn parse_pragma_value(&mut self) -> Result<Value, ParserError> {
        match self.parse_value()? {
            v @ Value::SingleQuotedString(_) => Ok(v),
            v @ Value::DoubleQuotedString(_) => Ok(v),
            v @ Value::Number(_, _) => Ok(v),
            v @ Value::Placeholder(_) => Ok(v),
            _ => {
                self.prev_token();
                self.expected("number or string or ? placeholder", self.peek_token())
            }
        }
    }

    // PRAGMA [schema-name '.'] pragma-name [('=' pragma-value) | '(' pragma-value ')']
    pub fn parse_pragma(&mut self) -> Result<Statement, ParserError> {
        let name = self.parse_object_name(false)?;
        if self.consume_token(&Token::LParen) {
            let value = self.parse_pragma_value()?;
            self.expect_token(&Token::RParen)?;
            Ok(Statement::Pragma {
                name,
                value: Some(value),
                is_eq: false,
            })
        } else if self.consume_token(&Token::Eq) {
            Ok(Statement::Pragma {
                name,
                value: Some(self.parse_pragma_value()?),
                is_eq: true,
            })
        } else {
            Ok(Statement::Pragma {
                name,
                value: None,
                is_eq: false,
            })
        }
    }

    /// `INSTALL [extension_name]`
    pub fn parse_install(&mut self) -> Result<Statement, ParserError> {
        let extension_name = self.parse_identifier(false)?;

        Ok(Statement::Install { extension_name })
    }

    /// `LOAD [extension_name]`
    pub fn parse_load(&mut self) -> Result<Statement, ParserError> {
        let extension_name = self.parse_identifier(false)?;
        Ok(Statement::Load { extension_name })
    }

    /// ```sql
    /// OPTIMIZE TABLE [db.]name [ON CLUSTER cluster] [PARTITION partition | PARTITION ID 'partition_id'] [FINAL] [DEDUPLICATE [BY expression]]
    /// ```
    /// [ClickHouse](https://clickhouse.com/docs/en/sql-reference/statements/optimize)
    pub fn parse_optimize_table(&mut self) -> Result<Statement, ParserError> {
        self.expect_keyword(Keyword::TABLE)?;
        let name = self.parse_object_name(false)?;
        let on_cluster = self.parse_optional_on_cluster()?;

        let partition = if self.parse_keyword(Keyword::PARTITION) {
            if self.parse_keyword(Keyword::ID) {
                Some(Partition::Identifier(self.parse_identifier(false)?))
            } else {
                Some(Partition::Expr(self.parse_expr()?))
            }
        } else {
            None
        };

        let include_final = self.parse_keyword(Keyword::FINAL);
        let deduplicate = if self.parse_keyword(Keyword::DEDUPLICATE) {
            if self.parse_keyword(Keyword::BY) {
                Some(Deduplicate::ByExpression(self.parse_expr()?))
            } else {
                Some(Deduplicate::All)
            }
        } else {
            None
        };

        Ok(Statement::OptimizeTable {
            name,
            on_cluster,
            partition,
            include_final,
            deduplicate,
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
        let name = self.parse_object_name(false)?;
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
                Some(self.parse_object_name(false)?)
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
            sequence_options.push(SequenceOptions::MinValue(Some(Expr::Value(
                self.parse_number_value()?,
            ))));
        } else if self.parse_keywords(&[Keyword::NO, Keyword::MINVALUE]) {
            sequence_options.push(SequenceOptions::MinValue(None));
        }
        //[ MAXVALUE maxvalue | NO MAXVALUE ]
        if self.parse_keywords(&[Keyword::MAXVALUE]) {
            sequence_options.push(SequenceOptions::MaxValue(Some(Expr::Value(
                self.parse_number_value()?,
            ))));
        } else if self.parse_keywords(&[Keyword::NO, Keyword::MAXVALUE]) {
            sequence_options.push(SequenceOptions::MaxValue(None));
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
        if self.parse_keywords(&[Keyword::NO, Keyword::CYCLE]) {
            sequence_options.push(SequenceOptions::Cycle(true));
        } else if self.parse_keywords(&[Keyword::CYCLE]) {
            sequence_options.push(SequenceOptions::Cycle(false));
        }

        Ok(sequence_options)
    }

    /// The index of the first unprocessed token.
    pub fn index(&self) -> usize {
        self.index
    }

    pub fn parse_named_window(&mut self) -> Result<NamedWindowDefinition, ParserError> {
        let ident = self.parse_identifier(false)?;
        self.expect_keyword(Keyword::AS)?;

        let window_expr = if self.consume_token(&Token::LParen) {
            NamedWindowExpr::WindowSpec(self.parse_window_spec()?)
        } else if self.dialect.supports_window_clause_named_window_reference() {
            NamedWindowExpr::NamedWindow(self.parse_identifier(false)?)
        } else {
            return self.expected("(", self.peek_token());
        };

        Ok(NamedWindowDefinition(ident, window_expr))
    }

    pub fn parse_create_procedure(&mut self, or_alter: bool) -> Result<Statement, ParserError> {
        let name = self.parse_object_name(false)?;
        let params = self.parse_optional_procedure_parameters()?;
        self.expect_keyword(Keyword::AS)?;
        self.expect_keyword(Keyword::BEGIN)?;
        let statements = self.parse_statements()?;
        self.expect_keyword(Keyword::END)?;
        Ok(Statement::CreateProcedure {
            name,
            or_alter,
            params,
            body: statements,
        })
    }

    pub fn parse_window_spec(&mut self) -> Result<WindowSpec, ParserError> {
        let window_name = match self.peek_token().token {
            Token::Word(word) if word.keyword == Keyword::NoKeyword => self.parse_optional_indent(),
            _ => None,
        };

        let partition_by = if self.parse_keywords(&[Keyword::PARTITION, Keyword::BY]) {
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
        Ok(WindowSpec {
            window_name,
            partition_by,
            order_by,
            window_frame,
        })
    }

    pub fn parse_create_type(&mut self) -> Result<Statement, ParserError> {
        let name = self.parse_object_name(false)?;
        self.expect_keyword(Keyword::AS)?;

        let mut attributes = vec![];
        if !self.consume_token(&Token::LParen) || self.consume_token(&Token::RParen) {
            return Ok(Statement::CreateType {
                name,
                representation: UserDefinedTypeRepresentation::Composite { attributes },
            });
        }

        loop {
            let attr_name = self.parse_identifier(false)?;
            let attr_data_type = self.parse_data_type()?;
            let attr_collation = if self.parse_keyword(Keyword::COLLATE) {
                Some(self.parse_object_name(false)?)
            } else {
                None
            };
            attributes.push(UserDefinedTypeCompositeAttributeDef {
                name: attr_name,
                data_type: attr_data_type,
                collation: attr_collation,
            });
            let comma = self.consume_token(&Token::Comma);
            if self.consume_token(&Token::RParen) {
                // allow a trailing comma
                break;
            } else if !comma {
                return self.expected("',' or ')' after attribute definition", self.peek_token());
            }
        }

        Ok(Statement::CreateType {
            name,
            representation: UserDefinedTypeRepresentation::Composite { attributes },
        })
    }

    fn parse_parenthesized_identifiers(&mut self) -> Result<Vec<Ident>, ParserError> {
        self.expect_token(&Token::LParen)?;
        let partitions = self.parse_comma_separated(|p| p.parse_identifier(false))?;
        self.expect_token(&Token::RParen)?;
        Ok(partitions)
    }

    fn parse_column_position(&mut self) -> Result<Option<MySQLColumnPosition>, ParserError> {
        if dialect_of!(self is MySqlDialect | GenericDialect) {
            if self.parse_keyword(Keyword::FIRST) {
                Ok(Some(MySQLColumnPosition::First))
            } else if self.parse_keyword(Keyword::AFTER) {
                let ident = self.parse_identifier(false)?;
                Ok(Some(MySQLColumnPosition::After(ident)))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    /// Consume the parser and return its underlying token buffer
    pub fn into_tokens(self) -> Vec<TokenWithLocation> {
        self.tokens
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
    fn test_peek_tokens() {
        all_dialects().run_parser_method("SELECT foo AS bar FROM baz", |parser| {
            assert!(matches!(
                parser.peek_tokens(),
                [Token::Word(Word {
                    keyword: Keyword::SELECT,
                    ..
                })]
            ));

            assert!(matches!(
                parser.peek_tokens(),
                [
                    Token::Word(Word {
                        keyword: Keyword::SELECT,
                        ..
                    }),
                    Token::Word(_),
                    Token::Word(Word {
                        keyword: Keyword::AS,
                        ..
                    }),
                ]
            ));

            for _ in 0..4 {
                parser.next_token();
            }

            assert!(matches!(
                parser.peek_tokens(),
                [
                    Token::Word(Word {
                        keyword: Keyword::FROM,
                        ..
                    }),
                    Token::Word(_),
                    Token::EOF,
                    Token::EOF,
                ]
            ))
        })
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
                options: None,
            };

            test_parse_data_type!(dialect, "CHARACTER", DataType::Character(None));

            test_parse_data_type!(
                dialect,
                "CHARACTER(20)",
                DataType::Character(Some(CharacterLength::IntegerLength {
                    length: 20,
                    unit: None
                }))
            );

            test_parse_data_type!(
                dialect,
                "CHARACTER(20 CHARACTERS)",
                DataType::Character(Some(CharacterLength::IntegerLength {
                    length: 20,
                    unit: Some(CharLengthUnits::Characters)
                }))
            );

            test_parse_data_type!(
                dialect,
                "CHARACTER(20 OCTETS)",
                DataType::Character(Some(CharacterLength::IntegerLength {
                    length: 20,
                    unit: Some(CharLengthUnits::Octets)
                }))
            );

            test_parse_data_type!(dialect, "CHAR", DataType::Char(None));

            test_parse_data_type!(
                dialect,
                "CHAR(20)",
                DataType::Char(Some(CharacterLength::IntegerLength {
                    length: 20,
                    unit: None
                }))
            );

            test_parse_data_type!(
                dialect,
                "CHAR(20 CHARACTERS)",
                DataType::Char(Some(CharacterLength::IntegerLength {
                    length: 20,
                    unit: Some(CharLengthUnits::Characters)
                }))
            );

            test_parse_data_type!(
                dialect,
                "CHAR(20 OCTETS)",
                DataType::Char(Some(CharacterLength::IntegerLength {
                    length: 20,
                    unit: Some(CharLengthUnits::Octets)
                }))
            );

            test_parse_data_type!(
                dialect,
                "CHARACTER VARYING(20)",
                DataType::CharacterVarying(Some(CharacterLength::IntegerLength {
                    length: 20,
                    unit: None
                }))
            );

            test_parse_data_type!(
                dialect,
                "CHARACTER VARYING(20 CHARACTERS)",
                DataType::CharacterVarying(Some(CharacterLength::IntegerLength {
                    length: 20,
                    unit: Some(CharLengthUnits::Characters)
                }))
            );

            test_parse_data_type!(
                dialect,
                "CHARACTER VARYING(20 OCTETS)",
                DataType::CharacterVarying(Some(CharacterLength::IntegerLength {
                    length: 20,
                    unit: Some(CharLengthUnits::Octets)
                }))
            );

            test_parse_data_type!(
                dialect,
                "CHAR VARYING(20)",
                DataType::CharVarying(Some(CharacterLength::IntegerLength {
                    length: 20,
                    unit: None
                }))
            );

            test_parse_data_type!(
                dialect,
                "CHAR VARYING(20 CHARACTERS)",
                DataType::CharVarying(Some(CharacterLength::IntegerLength {
                    length: 20,
                    unit: Some(CharLengthUnits::Characters)
                }))
            );

            test_parse_data_type!(
                dialect,
                "CHAR VARYING(20 OCTETS)",
                DataType::CharVarying(Some(CharacterLength::IntegerLength {
                    length: 20,
                    unit: Some(CharLengthUnits::Octets)
                }))
            );

            test_parse_data_type!(
                dialect,
                "VARCHAR(20)",
                DataType::Varchar(Some(CharacterLength::IntegerLength {
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
                options: None,
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
                options: None,
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
                options: None,
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
                options: None,
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
            options: None,
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
    fn test_tokenizer_error_loc() {
        let sql = "foo '";
        let ast = Parser::parse_sql(&GenericDialect, sql);
        assert_eq!(
            ast,
            Err(ParserError::TokenizerError(
                "Unterminated string literal at Line: 1, Column: 5".to_string()
            ))
        );
    }

    #[test]
    fn test_parser_error_loc() {
        let sql = "SELECT this is a syntax error";
        let ast = Parser::parse_sql(&GenericDialect, sql);
        assert_eq!(
            ast,
            Err(ParserError::ParserError(
                "Expected: [NOT] NULL or TRUE|FALSE or [NOT] DISTINCT FROM after IS, found: a at Line: 1, Column: 16"
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

    #[test]
    fn test_parse_multipart_identifier_positive() {
        let dialect = TestedDialects {
            dialects: vec![Box::new(GenericDialect {})],
            options: None,
        };

        // parse multipart with quotes
        let expected = vec![
            Ident {
                value: "CATALOG".to_string(),
                quote_style: None,
            },
            Ident {
                value: "F(o)o. \"bar".to_string(),
                quote_style: Some('"'),
            },
            Ident {
                value: "table".to_string(),
                quote_style: None,
            },
        ];
        dialect.run_parser_method(r#"CATALOG."F(o)o. ""bar".table"#, |parser| {
            let actual = parser.parse_multipart_identifier().unwrap();
            assert_eq!(expected, actual);
        });

        // allow whitespace between ident parts
        let expected = vec![
            Ident {
                value: "CATALOG".to_string(),
                quote_style: None,
            },
            Ident {
                value: "table".to_string(),
                quote_style: None,
            },
        ];
        dialect.run_parser_method("CATALOG . table", |parser| {
            let actual = parser.parse_multipart_identifier().unwrap();
            assert_eq!(expected, actual);
        });
    }

    #[test]
    fn test_parse_multipart_identifier_negative() {
        macro_rules! test_parse_multipart_identifier_error {
            ($input:expr, $expected_err:expr $(,)?) => {{
                all_dialects().run_parser_method(&*$input, |parser| {
                    let actual_err = parser.parse_multipart_identifier().unwrap_err();
                    assert_eq!(actual_err.to_string(), $expected_err);
                });
            }};
        }

        test_parse_multipart_identifier_error!(
            "",
            "sql parser error: Empty input when parsing identifier",
        );

        test_parse_multipart_identifier_error!(
            "*schema.table",
            "sql parser error: Unexpected token in identifier: *",
        );

        test_parse_multipart_identifier_error!(
            "schema.table*",
            "sql parser error: Unexpected token in identifier: *",
        );

        test_parse_multipart_identifier_error!(
            "schema.table.",
            "sql parser error: Trailing period in identifier",
        );

        test_parse_multipart_identifier_error!(
            "schema.*",
            "sql parser error: Unexpected token following period in identifier: *",
        );
    }

    #[test]
    fn test_mysql_partition_selection() {
        let sql = "SELECT * FROM employees PARTITION (p0, p2)";
        let expected = vec!["p0", "p2"];

        let ast: Vec<Statement> = Parser::parse_sql(&MySqlDialect {}, sql).unwrap();
        assert_eq!(ast.len(), 1);
        if let Statement::Query(v) = &ast[0] {
            if let SetExpr::Select(select) = &*v.body {
                assert_eq!(select.from.len(), 1);
                let from: &TableWithJoins = &select.from[0];
                let table_factor = &from.relation;
                if let TableFactor::Table { partitions, .. } = table_factor {
                    let actual: Vec<&str> = partitions
                        .iter()
                        .map(|ident| ident.value.as_str())
                        .collect();
                    assert_eq!(expected, actual);
                }
            }
        } else {
            panic!("fail to parse mysql partition selection");
        }
    }

    #[test]
    fn test_replace_into_placeholders() {
        let sql = "REPLACE INTO t (a) VALUES (&a)";

        assert!(Parser::parse_sql(&GenericDialect {}, sql).is_err());
    }

    #[test]
    fn test_replace_into_set() {
        // NOTE: This is actually valid MySQL syntax, REPLACE and INSERT,
        // but the parser does not yet support it.
        // https://dev.mysql.com/doc/refman/8.3/en/insert.html
        let sql = "REPLACE INTO t SET a='1'";

        assert!(Parser::parse_sql(&MySqlDialect {}, sql).is_err());
    }

    #[test]
    fn test_replace_into_set_placeholder() {
        let sql = "REPLACE INTO t SET ?";

        assert!(Parser::parse_sql(&GenericDialect {}, sql).is_err());
    }

    #[test]
    fn test_replace_incomplete() {
        let sql = r#"REPLACE"#;

        assert!(Parser::parse_sql(&MySqlDialect {}, sql).is_err());
    }
}

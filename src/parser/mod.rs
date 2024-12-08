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
use helpers::attached_token::AttachedToken;

use log::debug;

use recursion::RecursionCounter;
use IsLateral::*;
use IsOptional::*;

use crate::ast::helpers::stmt_create_table::{CreateTableBuilder, CreateTableConfiguration};
use crate::ast::Statement::CreatePolicy;
use crate::ast::*;
use crate::dialect::*;
use crate::keywords::Keyword;
use crate::tokenizer::*;

mod alter;
mod analyze;
mod assert;
mod assignment;
mod attach;
mod cache;
mod call;
mod close;
mod columns;
mod comment;
mod commit;
mod copy;
mod create;
mod deallocate;
mod declare;
mod delete;
mod dialects;
mod discard;
mod drop;
mod end;
mod execute;
mod explain;
mod expr;
mod fetch;
mod flush;
mod grant;
mod identifier;
mod insert;
mod install;
mod keyword;
mod kill;
mod listen;
mod lists;
mod load;
mod merge;
mod msck;
mod notify;
mod optimize;
mod options;
mod pragma;
mod prepare;
mod release;
mod replace;
mod revoke;
mod rollback;
mod savepoint;
mod select;
mod set;
mod show;
mod start;
mod tokens;
mod truncate;
mod uncache;
mod unlisten;
mod unload;
mod update;
mod r#use;
mod value;
mod window;

#[cfg(test)]
mod tests;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParserError {
    TokenizerError(String),
    ParserError(String),
    RecursionLimitExceeded,
}

// avoid clippy type_complexity warnings
type ParsedAction = (Keyword, Option<Vec<Ident>>);

// Use `Parser::expected` instead, if possible
#[macro_export]
macro_rules! parser_err {
    ($MSG:expr, $loc:expr) => {
        Err(ParserError::ParserError(format!("{}{}", $MSG, $loc)))
    };
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
pub(crate) struct MatchedTrailingBracket(bool);

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
    tokens: Vec<TokenWithSpan>,
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
    pub fn with_tokens_with_locations(mut self, tokens: Vec<TokenWithSpan>) -> Self {
        self.tokens = tokens;
        self.index = 0;
        self
    }

    /// Reset this parser state to parse the specified tokens
    pub fn with_tokens(self, tokens: Vec<Token>) -> Self {
        // Put in dummy locations
        let tokens_with_locations: Vec<TokenWithSpan> = tokens
            .into_iter()
            .map(|token| TokenWithSpan {
                token,
                span: Span::empty(),
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

    /// The index of the first unprocessed token.
    pub fn index(&self) -> usize {
        self.index
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
                Keyword::KILL => self.parse_kill(),
                Keyword::FLUSH => self.parse_flush(),
                Keyword::DESC => self.parse_explain(DescribeAlias::Desc),
                Keyword::DESCRIBE => self.parse_explain(DescribeAlias::Describe),
                Keyword::EXPLAIN => self.parse_explain(DescribeAlias::Explain),
                Keyword::ANALYZE => self.parse_analyze(),
                Keyword::SELECT | Keyword::WITH | Keyword::VALUES => {
                    self.prev_token();
                    self.parse_query().map(Statement::Query)
                }
                Keyword::TRUNCATE => self.parse_truncate(),
                Keyword::ATTACH => {
                    if dialect_of!(self is DuckDbDialect) {
                        self.parse_attach_duckdb_database()
                    } else {
                        self.parse_attach_database()
                    }
                }
                Keyword::DETACH if dialect_of!(self is DuckDbDialect | GenericDialect) => {
                    self.parse_detach_duckdb_database()
                }
                Keyword::MSCK => self.parse_msck(),
                Keyword::CREATE => self.parse_create(),
                Keyword::CACHE => self.parse_cache_table(),
                Keyword::DROP => self.parse_drop(),
                Keyword::DISCARD => self.parse_discard(),
                Keyword::DECLARE => self.parse_declare(),
                Keyword::FETCH => self.parse_fetch_statement(),
                Keyword::DELETE => self.parse_delete(),
                Keyword::INSERT => self.parse_insert(),
                Keyword::REPLACE => self.parse_replace(),
                Keyword::UNCACHE => self.parse_uncache_table(),
                Keyword::UPDATE => self.parse_update(),
                Keyword::ALTER => self.parse_alter(),
                Keyword::CALL => self.parse_call(),
                Keyword::COPY => self.parse_copy(),
                Keyword::CLOSE => self.parse_close(),
                Keyword::SET => self.parse_set(),
                Keyword::SHOW => self.parse_show(),
                Keyword::USE => self.parse_use(),
                Keyword::GRANT => self.parse_grant(),
                Keyword::REVOKE => self.parse_revoke(),
                Keyword::START => self.parse_start_transaction(),
                // `BEGIN` is a nonstandard but common alias for the
                // standard `START TRANSACTION` statement. It is supported
                // by at least PostgreSQL and MySQL.
                Keyword::BEGIN => self.parse_begin(),
                // `END` is a nonstandard but common alias for the
                // standard `COMMIT TRANSACTION` statement. It is supported
                // by PostgreSQL.
                Keyword::END => self.parse_end(),
                Keyword::SAVEPOINT => self.parse_savepoint(),
                Keyword::RELEASE => self.parse_release(),
                Keyword::COMMIT => self.parse_commit(),
                Keyword::ROLLBACK => self.parse_rollback(),
                Keyword::ASSERT => self.parse_assert(),
                // `PREPARE`, `EXECUTE` and `DEALLOCATE` are Postgres-specific
                // syntaxes. They are used for Postgres prepared statement.
                Keyword::DEALLOCATE => self.parse_deallocate(),
                Keyword::EXECUTE | Keyword::EXEC => self.parse_execute(),
                Keyword::PREPARE => self.parse_prepare(),
                Keyword::MERGE => self.parse_merge(),
                // `LISTEN`, `UNLISTEN` and `NOTIFY` are Postgres-specific
                // syntaxes. They are used for Postgres statement.
                Keyword::LISTEN if self.dialect.supports_listen_notify() => self.parse_listen(),
                Keyword::UNLISTEN if self.dialect.supports_listen_notify() => self.parse_unlisten(),
                Keyword::NOTIFY if self.dialect.supports_listen_notify() => self.parse_notify(),
                // `PRAGMA` is sqlite specific https://www.sqlite.org/pragma.html
                Keyword::PRAGMA => self.parse_pragma(),
                Keyword::UNLOAD => self.parse_unload(),
                // `INSTALL` is duckdb specific https://duckdb.org/docs/extensions/overview
                Keyword::INSTALL if dialect_of!(self is DuckDbDialect | GenericDialect) => {
                    self.parse_install()
                }
                Keyword::LOAD => self.parse_load(),
                // `OPTIMIZE` is clickhouse specific https://clickhouse.tech/docs/en/sql-reference/statements/optimize/
                Keyword::OPTIMIZE if dialect_of!(self is ClickHouseDialect | GenericDialect) => {
                    self.parse_optimize_table()
                }
                // `COMMENT` is snowflake specific https://docs.snowflake.com/en/sql-reference/sql/comment
                Keyword::COMMENT if self.dialect.supports_comment_on() => self.parse_comment(),
                _ => self.expected("an SQL statement", next_token),
            },
            Token::LParen => {
                self.prev_token();
                self.parse_query().map(Statement::Query)
            }
            _ => self.expected("an SQL statement", next_token),
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

    /// Run a parser method `f`, reverting back to the current position if unsuccessful.
    /// Returns `None` if `f` returns an error
    pub fn maybe_parse<T, F>(&mut self, f: F) -> Result<Option<T>, ParserError>
    where
        F: FnMut(&mut Parser) -> Result<T, ParserError>,
    {
        match self.try_parse(f) {
            Ok(t) => Ok(Some(t)),
            Err(ParserError::RecursionLimitExceeded) => Err(ParserError::RecursionLimitExceeded),
            _ => Ok(None),
        }
    }

    /// Run a parser method `f`, reverting back to the current position if unsuccessful.
    pub fn try_parse<T, F>(&mut self, mut f: F) -> Result<T, ParserError>
    where
        F: FnMut(&mut Parser) -> Result<T, ParserError>,
    {
        let index = self.index;
        match f(self) {
            Ok(t) => Ok(t),
            Err(e) => {
                // Unwind stack if limit exceeded
                self.index = index;
                Err(e)
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

    // Apparently this is dead code? I assume its retained for public API
    // compatibility or something of that nature.
    pub fn parse_precision(&mut self) -> Result<u64, ParserError> {
        self.expect_token(&Token::LParen)?;
        let n = self.parse_literal_uint()?;
        self.expect_token(&Token::RParen)?;
        Ok(n)
    }

    /// Returns true if the next keyword indicates a sub query, i.e. SELECT or WITH
    fn peek_sub_query(&mut self) -> bool {
        if self
            .parse_one_of_keywords(&[Keyword::SELECT, Keyword::WITH])
            .is_some()
        {
            self.prev_token();
            return true;
        }
        false
    }
}

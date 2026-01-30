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

use crate::ast::Statement::CreatePolicy;
use crate::ast::*;
use crate::ast::{
    comments,
    helpers::{
        key_value_options::{
            KeyValueOption, KeyValueOptionKind, KeyValueOptions, KeyValueOptionsDelimiter,
        },
        stmt_create_table::{CreateTableBuilder, CreateTableConfiguration},
    },
};
use crate::dialect::*;
use crate::keywords::{Keyword, ALL_KEYWORDS};
use crate::tokenizer::*;
use sqlparser::parser::ParserState::ColumnDefinition;

/// Errors produced by the SQL parser.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParserError {
    /// Error originating from the tokenizer with a message.
    TokenizerError(String),
    /// Generic parser error with a message.
    ParserError(String),
    /// Raised when a recursion depth limit is exceeded.
    RecursionLimitExceeded,
}

// Use `Parser::expected` instead, if possible
macro_rules! parser_err {
    ($MSG:expr, $loc:expr) => {
        Err(ParserError::ParserError(format!("{}{}", $MSG, $loc)))
    };
}

mod alter;
mod merge;

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
    ///
    /// Note: when "recursive-protection" feature is enabled, this crate uses additional stack overflow protection
    /// for some of its recursive methods. See [`recursive::recursive`] for more information.
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
/// Indicates whether a parser element is optional or mandatory.
pub enum IsOptional {
    /// The element is optional.
    Optional,
    /// The element is mandatory.
    Mandatory,
}

/// Indicates if a table expression is lateral.
pub enum IsLateral {
    /// The expression is lateral.
    Lateral,
    /// The expression is not lateral.
    NotLateral,
}

/// Represents a wildcard expression used in SELECT lists.
pub enum WildcardExpr {
    /// A specific expression used instead of a wildcard.
    Expr(Expr),
    /// A qualified wildcard like `table.*`.
    QualifiedWildcard(ObjectName),
    /// An unqualified `*` wildcard.
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

// A constant EOF token that can be referenced.
const EOF_TOKEN: TokenWithSpan = TokenWithSpan {
    token: Token::EOF,
    span: Span {
        start: Location { line: 0, column: 0 },
        end: Location { line: 0, column: 0 },
    },
};

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
    /// Allow trailing commas in lists (e.g. `a, b,`).
    pub trailing_commas: bool,
    /// Controls how literal values are unescaped. See
    /// [`Tokenizer::with_unescape`] for more details.
    pub unescape: bool,
    /// Controls if the parser expects a semi-colon token
    /// between statements. Default is `true`.
    pub require_semicolon_stmt_delimiter: bool,
}

impl Default for ParserOptions {
    fn default() -> Self {
        Self {
            trailing_commas: false,
            unescape: true,
            require_semicolon_stmt_delimiter: true,
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
    /// The state when parsing column definitions.  This state prohibits
    /// NOT NULL as an alias for IS NOT NULL.  For example:
    /// ```sql
    /// CREATE TABLE foo (abc BIGINT NOT NULL);
    /// ```
    ColumnDefinition,
}

/// A SQL Parser
///
/// This struct is the main entry point for parsing SQL queries.
///
/// # Functionality:
/// * Parsing SQL: see examples on [`Parser::new`] and [`Parser::parse_sql`]
/// * Controlling recursion: See [`Parser::with_recursion_limit`]
/// * Controlling parser options: See [`Parser::with_options`]
/// * Providing your own tokens: See [`Parser::with_tokens`]
///
/// # Internals
///
/// The parser uses a [`Tokenizer`] to tokenize the input SQL string into a
/// `Vec` of [`TokenWithSpan`]s and maintains an `index` to the current token
/// being processed. The token vec may contain multiple SQL statements.
///
/// * The "current" token is the token at `index - 1`
/// * The "next" token is the token at `index`
/// * The "previous" token is the token at `index - 2`
///
/// If `index` is equal to the length of the token stream, the 'next' token is
/// [`Token::EOF`].
///
/// For example, the SQL string "SELECT * FROM foo" will be tokenized into
/// following tokens:
/// ```text
///  [
///    "SELECT", // token index 0
///    " ",      // whitespace
///    "*",
///    " ",
///    "FROM",
///    " ",
///    "foo"
///   ]
/// ```
///
///
pub struct Parser<'a> {
    /// The tokens
    tokens: Vec<TokenWithSpan>,
    /// The index of the first unprocessed token in [`Parser::tokens`].
    index: usize,
    /// The current state of the parser.
    state: ParserState,
    /// The SQL dialect to use.
    dialect: &'a dyn Dialect,
    /// Additional options that allow you to mix & match behavior
    /// otherwise constrained to certain dialects (e.g. trailing
    /// commas) and/or format of parse (e.g. unescaping).
    options: ParserOptions,
    /// Ensures the stack does not overflow by limiting recursion depth.
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
    ///
    /// Note: when "recursive-protection" feature is enabled, this crate uses additional stack overflow protection
    //  for some of its recursive methods. See [`recursive::recursive`] for more information.
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
        debug!("Parsing sql '{sql}'...");
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

            if !self.options.require_semicolon_stmt_delimiter {
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

    /// Parses the given `sql` into an Abstract Syntax Tree (AST), returning
    /// also encountered source code comments.
    ///
    /// See [Parser::parse_sql].
    pub fn parse_sql_with_comments(
        dialect: &'a dyn Dialect,
        sql: &str,
    ) -> Result<(Vec<Statement>, comments::Comments), ParserError> {
        let mut p = Parser::new(dialect).try_with_sql(sql)?;
        p.parse_statements().map(|stmts| (stmts, p.into_comments()))
    }

    /// Consumes this parser returning comments from the parsed token stream.
    fn into_comments(self) -> comments::Comments {
        let mut comments = comments::Comments::default();
        for t in self.tokens.into_iter() {
            match t.token {
                Token::Whitespace(Whitespace::SingleLineComment { comment, prefix }) => {
                    comments.offer(comments::CommentWithSpan {
                        comment: comments::Comment::SingleLine {
                            content: comment,
                            prefix,
                        },
                        span: t.span,
                    });
                }
                Token::Whitespace(Whitespace::MultiLineComment(comment)) => {
                    comments.offer(comments::CommentWithSpan {
                        comment: comments::Comment::MultiLine(comment),
                        span: t.span,
                    });
                }
                _ => {}
            }
        }
        comments
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
                Keyword::ANALYZE => self.parse_analyze().map(Into::into),
                Keyword::CASE => {
                    self.prev_token();
                    self.parse_case_stmt().map(Into::into)
                }
                Keyword::IF => {
                    self.prev_token();
                    self.parse_if_stmt().map(Into::into)
                }
                Keyword::WHILE => {
                    self.prev_token();
                    self.parse_while().map(Into::into)
                }
                Keyword::RAISE => {
                    self.prev_token();
                    self.parse_raise_stmt().map(Into::into)
                }
                Keyword::SELECT | Keyword::WITH | Keyword::VALUES | Keyword::FROM => {
                    self.prev_token();
                    self.parse_query().map(Into::into)
                }
                Keyword::TRUNCATE => self.parse_truncate().map(Into::into),
                Keyword::ATTACH => {
                    if dialect_of!(self is DuckDbDialect) {
                        self.parse_attach_duckdb_database()
                    } else {
                        self.parse_attach_database()
                    }
                }
                Keyword::DETACH if self.dialect.supports_detach() => {
                    self.parse_detach_duckdb_database()
                }
                Keyword::MSCK => self.parse_msck().map(Into::into),
                Keyword::CREATE => self.parse_create(),
                Keyword::CACHE => self.parse_cache_table(),
                Keyword::DROP => self.parse_drop(),
                Keyword::DISCARD => self.parse_discard(),
                Keyword::DECLARE => self.parse_declare(),
                Keyword::FETCH => self.parse_fetch_statement(),
                Keyword::DELETE => self.parse_delete(next_token),
                Keyword::INSERT => self.parse_insert(next_token),
                Keyword::REPLACE => self.parse_replace(next_token),
                Keyword::UNCACHE => self.parse_uncache_table(),
                Keyword::UPDATE => self.parse_update(next_token),
                Keyword::ALTER => self.parse_alter(),
                Keyword::CALL => self.parse_call(),
                Keyword::COPY => self.parse_copy(),
                Keyword::OPEN => {
                    self.prev_token();
                    self.parse_open()
                }
                Keyword::CLOSE => self.parse_close(),
                Keyword::SET => self.parse_set(),
                Keyword::SHOW => self.parse_show(),
                Keyword::USE => self.parse_use(),
                Keyword::GRANT => self.parse_grant(),
                Keyword::DENY => {
                    self.prev_token();
                    self.parse_deny()
                }
                Keyword::REVOKE => self.parse_revoke(),
                Keyword::START => self.parse_start_transaction(),
                Keyword::BEGIN => self.parse_begin(),
                Keyword::END => self.parse_end(),
                Keyword::SAVEPOINT => self.parse_savepoint(),
                Keyword::RELEASE => self.parse_release(),
                Keyword::COMMIT => self.parse_commit(),
                Keyword::RAISERROR => Ok(self.parse_raiserror()?),
                Keyword::ROLLBACK => self.parse_rollback(),
                Keyword::ASSERT => self.parse_assert(),
                // `PREPARE`, `EXECUTE` and `DEALLOCATE` are Postgres-specific
                // syntaxes. They are used for Postgres prepared statement.
                Keyword::DEALLOCATE => self.parse_deallocate(),
                Keyword::EXECUTE | Keyword::EXEC => self.parse_execute(),
                Keyword::PREPARE => self.parse_prepare(),
                Keyword::MERGE => self.parse_merge(next_token).map(Into::into),
                // `LISTEN`, `UNLISTEN` and `NOTIFY` are Postgres-specific
                // syntaxes. They are used for Postgres statement.
                Keyword::LISTEN if self.dialect.supports_listen_notify() => self.parse_listen(),
                Keyword::UNLISTEN if self.dialect.supports_listen_notify() => self.parse_unlisten(),
                Keyword::NOTIFY if self.dialect.supports_listen_notify() => self.parse_notify(),
                // `PRAGMA` is sqlite specific https://www.sqlite.org/pragma.html
                Keyword::PRAGMA => self.parse_pragma(),
                Keyword::UNLOAD => {
                    self.prev_token();
                    self.parse_unload()
                }
                Keyword::RENAME => self.parse_rename(),
                // `INSTALL` is duckdb specific https://duckdb.org/docs/extensions/overview
                Keyword::INSTALL if self.dialect.supports_install() => self.parse_install(),
                Keyword::LOAD => self.parse_load(),
                // `OPTIMIZE` is clickhouse specific https://clickhouse.tech/docs/en/sql-reference/statements/optimize/
                Keyword::OPTIMIZE if self.dialect.supports_optimize_table() => {
                    self.parse_optimize_table()
                }
                // `COMMENT` is snowflake specific https://docs.snowflake.com/en/sql-reference/sql/comment
                Keyword::COMMENT if self.dialect.supports_comment_on() => self.parse_comment(),
                Keyword::PRINT => self.parse_print(),
                Keyword::RETURN => self.parse_return(),
                Keyword::EXPORT => {
                    self.prev_token();
                    self.parse_export_data()
                }
                Keyword::VACUUM => {
                    self.prev_token();
                    self.parse_vacuum()
                }
                Keyword::RESET => self.parse_reset().map(Into::into),
                _ => self.expected("an SQL statement", next_token),
            },
            Token::LParen => {
                self.prev_token();
                self.parse_query().map(Into::into)
            }
            _ => self.expected("an SQL statement", next_token),
        }
    }

    /// Parse a `CASE` statement.
    ///
    /// See [Statement::Case]
    pub fn parse_case_stmt(&mut self) -> Result<CaseStatement, ParserError> {
        let case_token = self.expect_keyword(Keyword::CASE)?;

        let match_expr = if self.peek_keyword(Keyword::WHEN) {
            None
        } else {
            Some(self.parse_expr()?)
        };

        self.expect_keyword_is(Keyword::WHEN)?;
        let when_blocks = self.parse_keyword_separated(Keyword::WHEN, |parser| {
            parser.parse_conditional_statement_block(&[Keyword::WHEN, Keyword::ELSE, Keyword::END])
        })?;

        let else_block = if self.parse_keyword(Keyword::ELSE) {
            Some(self.parse_conditional_statement_block(&[Keyword::END])?)
        } else {
            None
        };

        let mut end_case_token = self.expect_keyword(Keyword::END)?;
        if self.peek_keyword(Keyword::CASE) {
            end_case_token = self.expect_keyword(Keyword::CASE)?;
        }

        Ok(CaseStatement {
            case_token: AttachedToken(case_token),
            match_expr,
            when_blocks,
            else_block,
            end_case_token: AttachedToken(end_case_token),
        })
    }

    /// Parse an `IF` statement.
    ///
    /// See [Statement::If]
    pub fn parse_if_stmt(&mut self) -> Result<IfStatement, ParserError> {
        self.expect_keyword_is(Keyword::IF)?;
        let if_block = self.parse_conditional_statement_block(&[
            Keyword::ELSE,
            Keyword::ELSEIF,
            Keyword::END,
        ])?;

        let elseif_blocks = if self.parse_keyword(Keyword::ELSEIF) {
            self.parse_keyword_separated(Keyword::ELSEIF, |parser| {
                parser.parse_conditional_statement_block(&[
                    Keyword::ELSEIF,
                    Keyword::ELSE,
                    Keyword::END,
                ])
            })?
        } else {
            vec![]
        };

        let else_block = if self.parse_keyword(Keyword::ELSE) {
            Some(self.parse_conditional_statement_block(&[Keyword::END])?)
        } else {
            None
        };

        self.expect_keyword_is(Keyword::END)?;
        let end_token = self.expect_keyword(Keyword::IF)?;

        Ok(IfStatement {
            if_block,
            elseif_blocks,
            else_block,
            end_token: Some(AttachedToken(end_token)),
        })
    }

    /// Parse a `WHILE` statement.
    ///
    /// See [Statement::While]
    fn parse_while(&mut self) -> Result<WhileStatement, ParserError> {
        self.expect_keyword_is(Keyword::WHILE)?;
        let while_block = self.parse_conditional_statement_block(&[Keyword::END])?;

        Ok(WhileStatement { while_block })
    }

    /// Parses an expression and associated list of statements
    /// belonging to a conditional statement like `IF` or `WHEN` or `WHILE`.
    ///
    /// Example:
    /// ```sql
    /// IF condition THEN statement1; statement2;
    /// ```
    fn parse_conditional_statement_block(
        &mut self,
        terminal_keywords: &[Keyword],
    ) -> Result<ConditionalStatementBlock, ParserError> {
        let start_token = self.get_current_token().clone(); // self.expect_keyword(keyword)?;
        let mut then_token = None;

        let condition = match &start_token.token {
            Token::Word(w) if w.keyword == Keyword::ELSE => None,
            Token::Word(w) if w.keyword == Keyword::WHILE => {
                let expr = self.parse_expr()?;
                Some(expr)
            }
            _ => {
                let expr = self.parse_expr()?;
                then_token = Some(AttachedToken(self.expect_keyword(Keyword::THEN)?));
                Some(expr)
            }
        };

        let conditional_statements = self.parse_conditional_statements(terminal_keywords)?;

        Ok(ConditionalStatementBlock {
            start_token: AttachedToken(start_token),
            condition,
            then_token,
            conditional_statements,
        })
    }

    /// Parse a BEGIN/END block or a sequence of statements
    /// This could be inside of a conditional (IF, CASE, WHILE etc.) or an object body defined optionally BEGIN/END and one or more statements.
    pub(crate) fn parse_conditional_statements(
        &mut self,
        terminal_keywords: &[Keyword],
    ) -> Result<ConditionalStatements, ParserError> {
        let conditional_statements = if self.peek_keyword(Keyword::BEGIN) {
            let begin_token = self.expect_keyword(Keyword::BEGIN)?;
            let statements = self.parse_statement_list(terminal_keywords)?;
            let end_token = self.expect_keyword(Keyword::END)?;

            ConditionalStatements::BeginEnd(BeginEndStatements {
                begin_token: AttachedToken(begin_token),
                statements,
                end_token: AttachedToken(end_token),
            })
        } else {
            ConditionalStatements::Sequence {
                statements: self.parse_statement_list(terminal_keywords)?,
            }
        };
        Ok(conditional_statements)
    }

    /// Parse a `RAISE` statement.
    ///
    /// See [Statement::Raise]
    pub fn parse_raise_stmt(&mut self) -> Result<RaiseStatement, ParserError> {
        self.expect_keyword_is(Keyword::RAISE)?;

        let value = if self.parse_keywords(&[Keyword::USING, Keyword::MESSAGE]) {
            self.expect_token(&Token::Eq)?;
            Some(RaiseStatementValue::UsingMessage(self.parse_expr()?))
        } else {
            self.maybe_parse(|parser| parser.parse_expr().map(RaiseStatementValue::Expr))?
        };

        Ok(RaiseStatement { value })
    }
    /// Parse a COMMENT statement.
    ///
    /// See [Statement::Comment]
    pub fn parse_comment(&mut self) -> Result<Statement, ParserError> {
        let if_exists = self.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);

        self.expect_keyword_is(Keyword::ON)?;
        let token = self.next_token();

        let (object_type, object_name) = match token.token {
            Token::Word(w) if w.keyword == Keyword::COLUMN => {
                (CommentObject::Column, self.parse_object_name(false)?)
            }
            Token::Word(w) if w.keyword == Keyword::TABLE => {
                (CommentObject::Table, self.parse_object_name(false)?)
            }
            Token::Word(w) if w.keyword == Keyword::EXTENSION => {
                (CommentObject::Extension, self.parse_object_name(false)?)
            }
            Token::Word(w) if w.keyword == Keyword::SCHEMA => {
                (CommentObject::Schema, self.parse_object_name(false)?)
            }
            Token::Word(w) if w.keyword == Keyword::DATABASE => {
                (CommentObject::Database, self.parse_object_name(false)?)
            }
            Token::Word(w) if w.keyword == Keyword::USER => {
                (CommentObject::User, self.parse_object_name(false)?)
            }
            Token::Word(w) if w.keyword == Keyword::ROLE => {
                (CommentObject::Role, self.parse_object_name(false)?)
            }
            _ => self.expected("comment object_type", token)?,
        };

        self.expect_keyword_is(Keyword::IS)?;
        let comment = if self.parse_keyword(Keyword::NULL) {
            None
        } else {
            Some(self.parse_literal_string()?)
        };
        Ok(Statement::Comment {
            object_type,
            object_name,
            comment,
            if_exists,
        })
    }

    /// Parse `FLUSH` statement.
    pub fn parse_flush(&mut self) -> Result<Statement, ParserError> {
        let mut channel = None;
        let mut tables: Vec<ObjectName> = vec![];
        let mut read_lock = false;
        let mut export = false;

        if !dialect_of!(self is MySqlDialect | GenericDialect) {
            return parser_err!("Unsupported statement FLUSH", self.peek_token().span.start);
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

    /// Parse `MSCK` statement.
    pub fn parse_msck(&mut self) -> Result<Msck, ParserError> {
        let repair = self.parse_keyword(Keyword::REPAIR);
        self.expect_keyword_is(Keyword::TABLE)?;
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
                parser.expect_keyword_is(Keyword::PARTITIONS)?;
                Ok(pa)
            })?
            .unwrap_or_default();
        Ok(Msck {
            repair,
            table_name,
            partition_action,
        })
    }

    /// Parse `TRUNCATE` statement.
    pub fn parse_truncate(&mut self) -> Result<Truncate, ParserError> {
        let table = self.parse_keyword(Keyword::TABLE);
        let if_exists = self.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);

        let table_names = self.parse_comma_separated(|p| {
            let only = p.parse_keyword(Keyword::ONLY);
            let name = p.parse_object_name(false)?;
            let has_asterisk = p.consume_token(&Token::Mul);
            Ok(TruncateTableTarget {
                name,
                only,
                has_asterisk,
            })
        })?;

        let mut partitions = None;
        if self.parse_keyword(Keyword::PARTITION) {
            self.expect_token(&Token::LParen)?;
            partitions = Some(self.parse_comma_separated(Parser::parse_expr)?);
            self.expect_token(&Token::RParen)?;
        }

        let mut identity = None;
        let mut cascade = None;

        if dialect_of!(self is PostgreSqlDialect | GenericDialect) {
            identity = if self.parse_keywords(&[Keyword::RESTART, Keyword::IDENTITY]) {
                Some(TruncateIdentityOption::Restart)
            } else if self.parse_keywords(&[Keyword::CONTINUE, Keyword::IDENTITY]) {
                Some(TruncateIdentityOption::Continue)
            } else {
                None
            };

            cascade = self.parse_cascade_option();
        };

        let on_cluster = self.parse_optional_on_cluster()?;

        Ok(Truncate {
            table_names,
            partitions,
            table,
            if_exists,
            identity,
            cascade,
            on_cluster,
        })
    }

    fn parse_cascade_option(&mut self) -> Option<CascadeOption> {
        if self.parse_keyword(Keyword::CASCADE) {
            Some(CascadeOption::Cascade)
        } else if self.parse_keyword(Keyword::RESTRICT) {
            Some(CascadeOption::Restrict)
        } else {
            None
        }
    }

    /// Parse options for `ATTACH DUCKDB DATABASE` statement.
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
                let ident = self.parse_identifier()?;
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

    /// Parse `ATTACH DUCKDB DATABASE` statement.
    pub fn parse_attach_duckdb_database(&mut self) -> Result<Statement, ParserError> {
        let database = self.parse_keyword(Keyword::DATABASE);
        let if_not_exists = self.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let database_path = self.parse_identifier()?;
        let database_alias = if self.parse_keyword(Keyword::AS) {
            Some(self.parse_identifier()?)
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

    /// Parse `DETACH DUCKDB DATABASE` statement.
    pub fn parse_detach_duckdb_database(&mut self) -> Result<Statement, ParserError> {
        let database = self.parse_keyword(Keyword::DATABASE);
        let if_exists = self.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
        let database_alias = self.parse_identifier()?;
        Ok(Statement::DetachDuckDBDatabase {
            if_exists,
            database,
            database_alias,
        })
    }

    /// Parse `ATTACH DATABASE` statement.
    pub fn parse_attach_database(&mut self) -> Result<Statement, ParserError> {
        let database = self.parse_keyword(Keyword::DATABASE);
        let database_file_name = self.parse_expr()?;
        self.expect_keyword_is(Keyword::AS)?;
        let schema_name = self.parse_identifier()?;
        Ok(Statement::AttachDatabase {
            database,
            schema_name,
            database_file_name,
        })
    }

    /// Parse `ANALYZE` statement.
    pub fn parse_analyze(&mut self) -> Result<Analyze, ParserError> {
        let has_table_keyword = self.parse_keyword(Keyword::TABLE);
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
                    self.expect_keyword_is(Keyword::COLUMNS)?;

                    columns = self
                        .maybe_parse(|parser| {
                            parser.parse_comma_separated(|p| p.parse_identifier())
                        })?
                        .unwrap_or_default();
                    for_columns = true
                }
                Some(Keyword::CACHE) => {
                    self.expect_keyword_is(Keyword::METADATA)?;
                    cache_metadata = true
                }
                Some(Keyword::COMPUTE) => {
                    self.expect_keyword_is(Keyword::STATISTICS)?;
                    compute_statistics = true
                }
                _ => break,
            }
        }

        Ok(Analyze {
            has_table_keyword,
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
                        Token::Word(w) => w.into_ident(next_token.span),
                        Token::SingleQuotedString(s) => Ident::with_quote('\'', s),
                        _ => {
                            return Err(ParserError::ParserError(
                                "Internal parser error: unexpected token type".to_string(),
                            ))
                        }
                    }];

                    while self.consume_token(&Token::Period) {
                        let next_token = self.next_token();
                        match next_token.token {
                            Token::Word(w) => id_parts.push(w.into_ident(next_token.span)),
                            Token::SingleQuotedString(s) => {
                                // SQLite has single-quoted identifiers
                                id_parts.push(Ident::with_quote('\'', s))
                            }
                            Token::Mul => {
                                return Ok(Expr::QualifiedWildcard(
                                    ObjectName::from(id_parts),
                                    AttachedToken(next_token),
                                ));
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
                return Ok(Expr::Wildcard(AttachedToken(next_token)));
            }
            // Handle parenthesized wildcard: (*)
            Token::LParen => {
                let [maybe_mul, maybe_rparen] = self.peek_tokens_ref();
                if maybe_mul.token == Token::Mul && maybe_rparen.token == Token::RParen {
                    let mul_token = self.next_token(); // consume Mul
                    self.next_token(); // consume RParen
                    return Ok(Expr::Wildcard(AttachedToken(mul_token)));
                }
            }
            _ => (),
        };

        self.index = index;
        self.parse_expr()
    }

    /// Parse a new expression.
    pub fn parse_expr(&mut self) -> Result<Expr, ParserError> {
        self.parse_subexpr(self.dialect.prec_unknown())
    }

    /// Parse expression with optional alias and order by.
    pub fn parse_expr_with_alias_and_order_by(
        &mut self,
    ) -> Result<ExprWithAliasAndOrderBy, ParserError> {
        let expr = self.parse_expr()?;

        fn validator(explicit: bool, kw: &Keyword, _parser: &mut Parser) -> bool {
            explicit || !&[Keyword::ASC, Keyword::DESC, Keyword::GROUP].contains(kw)
        }
        let alias = self.parse_optional_alias_inner(None, validator)?;
        let order_by = OrderByOptions {
            asc: self.parse_asc_desc(),
            nulls_first: None,
        };
        Ok(ExprWithAliasAndOrderBy {
            expr: ExprWithAlias { expr, alias },
            order_by,
        })
    }

    /// Parse tokens until the precedence changes.
    pub fn parse_subexpr(&mut self, precedence: u8) -> Result<Expr, ParserError> {
        let _guard = self.recursion_counter.try_decrease()?;
        debug!("parsing expr");
        let mut expr = self.parse_prefix()?;

        expr = self.parse_compound_expr(expr, vec![])?;

        debug!("prefix: {expr:?}");
        loop {
            let next_precedence = self.get_next_precedence()?;
            debug!("next precedence: {next_precedence:?}");

            if precedence >= next_precedence {
                break;
            }

            // The period operator is handled exclusively by the
            // compound field access parsing.
            if Token::Period == self.peek_token_ref().token {
                break;
            }

            expr = self.parse_infix(expr, next_precedence)?;
        }
        Ok(expr)
    }

    /// Parse `ASSERT` statement.
    pub fn parse_assert(&mut self) -> Result<Statement, ParserError> {
        let condition = self.parse_expr()?;
        let message = if self.parse_keyword(Keyword::AS) {
            Some(self.parse_expr()?)
        } else {
            None
        };

        Ok(Statement::Assert { condition, message })
    }

    /// Parse `SAVEPOINT` statement.
    pub fn parse_savepoint(&mut self) -> Result<Statement, ParserError> {
        let name = self.parse_identifier()?;
        Ok(Statement::Savepoint { name })
    }

    /// Parse `RELEASE` statement.
    pub fn parse_release(&mut self) -> Result<Statement, ParserError> {
        let _ = self.parse_keyword(Keyword::SAVEPOINT);
        let name = self.parse_identifier()?;

        Ok(Statement::ReleaseSavepoint { name })
    }

    /// Parse `LISTEN` statement.
    pub fn parse_listen(&mut self) -> Result<Statement, ParserError> {
        let channel = self.parse_identifier()?;
        Ok(Statement::LISTEN { channel })
    }

    /// Parse `UNLISTEN` statement.
    pub fn parse_unlisten(&mut self) -> Result<Statement, ParserError> {
        let channel = if self.consume_token(&Token::Mul) {
            Ident::new(Expr::Wildcard(AttachedToken::empty()).to_string())
        } else {
            match self.parse_identifier() {
                Ok(expr) => expr,
                _ => {
                    self.prev_token();
                    return self.expected("wildcard or identifier", self.peek_token());
                }
            }
        };
        Ok(Statement::UNLISTEN { channel })
    }

    /// Parse `NOTIFY` statement.
    pub fn parse_notify(&mut self) -> Result<Statement, ParserError> {
        let channel = self.parse_identifier()?;
        let payload = if self.consume_token(&Token::Comma) {
            Some(self.parse_literal_string()?)
        } else {
            None
        };
        Ok(Statement::NOTIFY { channel, payload })
    }

    /// Parses a `RENAME TABLE` statement. See [Statement::RenameTable]
    pub fn parse_rename(&mut self) -> Result<Statement, ParserError> {
        if self.peek_keyword(Keyword::TABLE) {
            self.expect_keyword(Keyword::TABLE)?;
            let rename_tables = self.parse_comma_separated(|parser| {
                let old_name = parser.parse_object_name(false)?;
                parser.expect_keyword(Keyword::TO)?;
                let new_name = parser.parse_object_name(false)?;

                Ok(RenameTable { old_name, new_name })
            })?;
            Ok(rename_tables.into())
        } else {
            self.expected("KEYWORD `TABLE` after RENAME", self.peek_token())
        }
    }

    /// Tries to parse an expression by matching the specified word to known keywords that have a special meaning in the dialect.
    /// Returns `None if no match is found.
    fn parse_expr_prefix_by_reserved_word(
        &mut self,
        w: &Word,
        w_span: Span,
    ) -> Result<Option<Expr>, ParserError> {
        match w.keyword {
            Keyword::TRUE | Keyword::FALSE if self.dialect.supports_boolean_literals() => {
                self.prev_token();
                Ok(Some(Expr::Value(self.parse_value()?)))
            }
            Keyword::NULL => {
                self.prev_token();
                Ok(Some(Expr::Value(self.parse_value()?)))
            }
            Keyword::CURRENT_CATALOG
            | Keyword::CURRENT_USER
            | Keyword::SESSION_USER
            | Keyword::USER
            if dialect_of!(self is PostgreSqlDialect | GenericDialect) =>
                {
                    Ok(Some(Expr::Function(Function {
                        name: ObjectName::from(vec![w.to_ident(w_span)]),
                        uses_odbc_syntax: false,
                        parameters: FunctionArguments::None,
                        args: FunctionArguments::None,
                        null_treatment: None,
                        filter: None,
                        over: None,
                        within_group: vec![],
                    })))
                }
            Keyword::CURRENT_TIMESTAMP
            | Keyword::CURRENT_TIME
            | Keyword::CURRENT_DATE
            | Keyword::LOCALTIME
            | Keyword::LOCALTIMESTAMP => {
                Ok(Some(self.parse_time_functions(ObjectName::from(vec![w.to_ident(w_span)]))?))
            }
            Keyword::CASE => Ok(Some(self.parse_case_expr()?)),
            Keyword::CONVERT => Ok(Some(self.parse_convert_expr(false)?)),
            Keyword::TRY_CONVERT if self.dialect.supports_try_convert() => Ok(Some(self.parse_convert_expr(true)?)),
            Keyword::CAST => Ok(Some(self.parse_cast_expr(CastKind::Cast)?)),
            Keyword::TRY_CAST => Ok(Some(self.parse_cast_expr(CastKind::TryCast)?)),
            Keyword::SAFE_CAST => Ok(Some(self.parse_cast_expr(CastKind::SafeCast)?)),
            Keyword::EXISTS
            // Support parsing Databricks has a function named `exists`.
            if !dialect_of!(self is DatabricksDialect)
                || matches!(
                        self.peek_nth_token_ref(1).token,
                        Token::Word(Word {
                            keyword: Keyword::SELECT | Keyword::WITH,
                            ..
                        })
                    ) =>
                {
                    Ok(Some(self.parse_exists_expr(false)?))
                }
            Keyword::EXTRACT => Ok(Some(self.parse_extract_expr()?)),
            Keyword::CEIL => Ok(Some(self.parse_ceil_floor_expr(true)?)),
            Keyword::FLOOR => Ok(Some(self.parse_ceil_floor_expr(false)?)),
            Keyword::POSITION if self.peek_token_ref().token == Token::LParen => {
                Ok(Some(self.parse_position_expr(w.to_ident(w_span))?))
            }
            Keyword::SUBSTR | Keyword::SUBSTRING => {
                self.prev_token();
                Ok(Some(self.parse_substring()?))
            }
            Keyword::OVERLAY => Ok(Some(self.parse_overlay_expr()?)),
            Keyword::TRIM => Ok(Some(self.parse_trim_expr()?)),
            Keyword::INTERVAL => Ok(Some(self.parse_interval()?)),
            // Treat ARRAY[1,2,3] as an array [1,2,3], otherwise try as subquery or a function call
            Keyword::ARRAY if *self.peek_token_ref() == Token::LBracket => {
                self.expect_token(&Token::LBracket)?;
                Ok(Some(self.parse_array_expr(true)?))
            }
            Keyword::ARRAY
            if self.peek_token() == Token::LParen
                && !dialect_of!(self is ClickHouseDialect | DatabricksDialect) =>
                {
                    self.expect_token(&Token::LParen)?;
                    let query = self.parse_query()?;
                    self.expect_token(&Token::RParen)?;
                    Ok(Some(Expr::Function(Function {
                        name: ObjectName::from(vec![w.to_ident(w_span)]),
                        uses_odbc_syntax: false,
                        parameters: FunctionArguments::None,
                        args: FunctionArguments::Subquery(query),
                        filter: None,
                        null_treatment: None,
                        over: None,
                        within_group: vec![],
                    })))
                }
            Keyword::NOT => Ok(Some(self.parse_not()?)),
            Keyword::MATCH if self.dialect.supports_match_against() => {
                Ok(Some(self.parse_match_against()?))
            }
            Keyword::STRUCT if self.dialect.supports_struct_literal() => {
                let struct_expr = self.parse_struct_literal()?;
                Ok(Some(struct_expr))
            }
            Keyword::PRIOR if matches!(self.state, ParserState::ConnectBy) => {
                let expr = self.parse_subexpr(self.dialect.prec_value(Precedence::PlusMinus))?;
                Ok(Some(Expr::Prior(Box::new(expr))))
            }
            Keyword::MAP if *self.peek_token_ref() == Token::LBrace && self.dialect.support_map_literal_syntax() => {
                Ok(Some(self.parse_duckdb_map_literal()?))
            }
            Keyword::LAMBDA if self.dialect.supports_lambda_functions() => {
                Ok(Some(self.parse_lambda_expr()?))
            }
            _ if self.dialect.supports_geometric_types() => match w.keyword {
                Keyword::CIRCLE => Ok(Some(self.parse_geometric_type(GeometricTypeKind::Circle)?)),
                Keyword::BOX => Ok(Some(self.parse_geometric_type(GeometricTypeKind::GeometricBox)?)),
                Keyword::PATH => Ok(Some(self.parse_geometric_type(GeometricTypeKind::GeometricPath)?)),
                Keyword::LINE => Ok(Some(self.parse_geometric_type(GeometricTypeKind::Line)?)),
                Keyword::LSEG => Ok(Some(self.parse_geometric_type(GeometricTypeKind::LineSegment)?)),
                Keyword::POINT => Ok(Some(self.parse_geometric_type(GeometricTypeKind::Point)?)),
                Keyword::POLYGON => Ok(Some(self.parse_geometric_type(GeometricTypeKind::Polygon)?)),
                _ => Ok(None),
            },
            _ => Ok(None),
        }
    }

    /// Tries to parse an expression by a word that is not known to have a special meaning in the dialect.
    fn parse_expr_prefix_by_unreserved_word(
        &mut self,
        w: &Word,
        w_span: Span,
    ) -> Result<Expr, ParserError> {
        match self.peek_token().token {
            Token::LParen if !self.peek_outer_join_operator() => {
                let id_parts = vec![w.to_ident(w_span)];
                self.parse_function(ObjectName::from(id_parts))
            }
            // string introducer https://dev.mysql.com/doc/refman/8.0/en/charset-introducer.html
            Token::SingleQuotedString(_)
            | Token::DoubleQuotedString(_)
            | Token::HexStringLiteral(_)
                if w.value.starts_with('_') =>
            {
                Ok(Expr::Prefixed {
                    prefix: w.to_ident(w_span),
                    value: self.parse_introduced_string_expr()?.into(),
                })
            }
            // string introducer https://dev.mysql.com/doc/refman/8.0/en/charset-introducer.html
            Token::SingleQuotedString(_)
            | Token::DoubleQuotedString(_)
            | Token::HexStringLiteral(_)
                if w.value.starts_with('_') =>
            {
                Ok(Expr::Prefixed {
                    prefix: w.to_ident(w_span),
                    value: self.parse_introduced_string_expr()?.into(),
                })
            }
            Token::Arrow if self.dialect.supports_lambda_functions() => {
                self.expect_token(&Token::Arrow)?;
                Ok(Expr::Lambda(LambdaFunction {
                    params: OneOrManyWithParens::One(w.to_ident(w_span)),
                    body: Box::new(self.parse_expr()?),
                    syntax: LambdaSyntax::Arrow,
                }))
            }
            _ => Ok(Expr::Identifier(w.to_ident(w_span))),
        }
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
        let loc = self.peek_token_ref().span.start;
        let opt_expr = self.maybe_parse(|parser| {
            match parser.parse_data_type()? {
                DataType::Interval { .. } => parser.parse_interval(),
                // PostgreSQL allows almost any identifier to be used as custom data type name,
                // and we support that in `parse_data_type()`. But unlike Postgres we don't
                // have a list of globally reserved keywords (since they vary across dialects),
                // so given `NOT 'a' LIKE 'b'`, we'd accept `NOT` as a possible custom data type
                // name, resulting in `NOT 'a'` being recognized as a `TypedString` instead of
                // an unary negation `NOT ('a' LIKE 'b')`. To solve this, we don't accept the
                // `type 'string'` syntax for the custom data types at all.
                DataType::Custom(..) => parser_err!("dummy", loc),
                // MySQL supports using the `BINARY` keyword as a cast to binary type.
                DataType::Binary(..) if self.dialect.supports_binary_kw_as_cast() => {
                    Ok(Expr::Cast {
                        kind: CastKind::Cast,
                        expr: Box::new(parser.parse_expr()?),
                        data_type: DataType::Binary(None),
                        array: false,
                        format: None,
                    })
                }
                data_type => Ok(Expr::TypedString(TypedString {
                    data_type,
                    value: parser.parse_value()?,
                    uses_odbc_syntax: false,
                })),
            }
        })?;

        if let Some(expr) = opt_expr {
            return Ok(expr);
        }

        // Cache some dialect properties to avoid lifetime issues with the
        // next_token reference.

        let dialect = self.dialect;

        self.advance_token();
        let next_token_index = self.get_current_index();
        let next_token = self.get_current_token();
        let span = next_token.span;
        let expr = match &next_token.token {
            Token::Word(w) => {
                // The word we consumed may fall into one of two cases: it has a special meaning, or not.
                // For example, in Snowflake, the word `interval` may have two meanings depending on the context:
                // `SELECT CURRENT_DATE() + INTERVAL '1 DAY', MAX(interval) FROM tbl;`
                //                          ^^^^^^^^^^^^^^^^      ^^^^^^^^
                //                         interval expression   identifier
                //
                // We first try to parse the word and following tokens as a special expression, and if that fails,
                // we rollback and try to parse it as an identifier.
                let w = w.clone();
                match self.try_parse(|parser| parser.parse_expr_prefix_by_reserved_word(&w, span)) {
                    // This word indicated an expression prefix and parsing was successful
                    Ok(Some(expr)) => Ok(expr),

                    // No expression prefix associated with this word
                    Ok(None) => Ok(self.parse_expr_prefix_by_unreserved_word(&w, span)?),

                    // If parsing of the word as a special expression failed, we are facing two options:
                    // 1. The statement is malformed, e.g. `SELECT INTERVAL '1 DAI` (`DAI` instead of `DAY`)
                    // 2. The word is used as an identifier, e.g. `SELECT MAX(interval) FROM tbl`
                    // We first try to parse the word as an identifier and if that fails
                    // we rollback and return the parsing error we got from trying to parse a
                    // special expression (to maintain backwards compatibility of parsing errors).
                    Err(e) => {
                        if !self.dialect.is_reserved_for_identifier(w.keyword) {
                            if let Ok(Some(expr)) = self.maybe_parse(|parser| {
                                parser.parse_expr_prefix_by_unreserved_word(&w, span)
                            }) {
                                return Ok(expr);
                            }
                        }
                        return Err(e);
                    }
                }
            } // End of Token::Word
            // array `[1, 2, 3]`
            Token::LBracket => self.parse_array_expr(false),
            tok @ Token::Minus | tok @ Token::Plus => {
                let op = if *tok == Token::Plus {
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
            Token::ExclamationMark if dialect.supports_bang_not_operator() => Ok(Expr::UnaryOp {
                op: UnaryOperator::BangNot,
                expr: Box::new(self.parse_subexpr(self.dialect.prec_value(Precedence::UnaryNot))?),
            }),
            tok @ Token::DoubleExclamationMark
            | tok @ Token::PGSquareRoot
            | tok @ Token::PGCubeRoot
            | tok @ Token::AtSign
                if dialect_is!(dialect is PostgreSqlDialect) =>
            {
                let op = match tok {
                    Token::DoubleExclamationMark => UnaryOperator::PGPrefixFactorial,
                    Token::PGSquareRoot => UnaryOperator::PGSquareRoot,
                    Token::PGCubeRoot => UnaryOperator::PGCubeRoot,
                    Token::AtSign => UnaryOperator::PGAbs,
                    _ => {
                        return Err(ParserError::ParserError(
                            "Internal parser error: unexpected unary operator token".to_string(),
                        ))
                    }
                };
                Ok(Expr::UnaryOp {
                    op,
                    expr: Box::new(
                        self.parse_subexpr(self.dialect.prec_value(Precedence::PlusMinus))?,
                    ),
                })
            }
            Token::Tilde => Ok(Expr::UnaryOp {
                op: UnaryOperator::BitwiseNot,
                expr: Box::new(self.parse_subexpr(self.dialect.prec_value(Precedence::PlusMinus))?),
            }),
            tok @ Token::Sharp
            | tok @ Token::AtDashAt
            | tok @ Token::AtAt
            | tok @ Token::QuestionMarkDash
            | tok @ Token::QuestionPipe
                if self.dialect.supports_geometric_types() =>
            {
                let op = match tok {
                    Token::Sharp => UnaryOperator::Hash,
                    Token::AtDashAt => UnaryOperator::AtDashAt,
                    Token::AtAt => UnaryOperator::DoubleAt,
                    Token::QuestionMarkDash => UnaryOperator::QuestionDash,
                    Token::QuestionPipe => UnaryOperator::QuestionPipe,
                    _ => {
                        return Err(ParserError::ParserError(format!(
                            "Unexpected token in unary operator parsing: {tok:?}"
                        )))
                    }
                };
                Ok(Expr::UnaryOp {
                    op,
                    expr: Box::new(
                        self.parse_subexpr(self.dialect.prec_value(Precedence::PlusMinus))?,
                    ),
                })
            }
            Token::EscapedStringLiteral(_) if dialect_is!(dialect is PostgreSqlDialect | GenericDialect) =>
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
            | Token::QuoteDelimitedStringLiteral(_)
            | Token::NationalQuoteDelimitedStringLiteral(_)
            | Token::HexStringLiteral(_) => {
                self.prev_token();
                Ok(Expr::Value(self.parse_value()?))
            }
            Token::LParen => {
                let expr =
                    if let Some(expr) = self.try_parse_expr_sub_query()? {
                        expr
                    } else if let Some(lambda) = self.try_parse_lambda()? {
                        return Ok(lambda);
                    } else {
                        let exprs = self.parse_comma_separated(Parser::parse_expr)?;
                        match exprs.len() {
                            0 => return Err(ParserError::ParserError(
                                "Internal parser error: parse_comma_separated returned empty list"
                                    .to_string(),
                            )),
                            1 => Expr::Nested(Box::new(exprs.into_iter().next().unwrap())),
                            _ => Expr::Tuple(exprs),
                        }
                    };
                self.expect_token(&Token::RParen)?;
                Ok(expr)
            }
            Token::Placeholder(_) | Token::Colon | Token::AtSign => {
                self.prev_token();
                Ok(Expr::Value(self.parse_value()?))
            }
            Token::LBrace => {
                self.prev_token();
                self.parse_lbrace_expr()
            }
            _ => self.expected_at("an expression", next_token_index),
        }?;

        if !self.in_column_definition_state() && self.parse_keyword(Keyword::COLLATE) {
            Ok(Expr::Collate {
                expr: Box::new(expr),
                collation: self.parse_object_name(false)?,
            })
        } else {
            Ok(expr)
        }
    }

    fn parse_geometric_type(&mut self, kind: GeometricTypeKind) -> Result<Expr, ParserError> {
        Ok(Expr::TypedString(TypedString {
            data_type: DataType::GeometricType(kind),
            value: self.parse_value()?,
            uses_odbc_syntax: false,
        }))
    }

    /// Try to parse an [Expr::CompoundFieldAccess] like `a.b.c` or `a.b[1].c`.
    /// If all the fields are `Expr::Identifier`s, return an [Expr::CompoundIdentifier] instead.
    /// If only the root exists, return the root.
    /// Parses compound expressions which may be delimited by period
    /// or bracket notation.
    /// For example: `a.b.c`, `a.b[1]`.
    pub fn parse_compound_expr(
        &mut self,
        root: Expr,
        mut chain: Vec<AccessExpr>,
    ) -> Result<Expr, ParserError> {
        let mut ending_wildcard: Option<TokenWithSpan> = None;
        loop {
            if self.consume_token(&Token::Period) {
                let next_token = self.peek_token_ref();
                match &next_token.token {
                    Token::Mul => {
                        // Postgres explicitly allows funcnm(tablenm.*) and the
                        // function array_agg traverses this control flow
                        if dialect_of!(self is PostgreSqlDialect) {
                            ending_wildcard = Some(self.next_token());
                        } else {
                            // Put back the consumed `.` tokens before exiting.
                            // If this expression is being parsed in the
                            // context of a projection, then the `.*` could imply
                            // a wildcard expansion. For example:
                            // `SELECT STRUCT('foo').* FROM T`
                            self.prev_token(); // .
                        }

                        break;
                    }
                    Token::SingleQuotedString(s) => {
                        let expr =
                            Expr::Identifier(Ident::with_quote_and_span('\'', next_token.span, s));
                        chain.push(AccessExpr::Dot(expr));
                        self.advance_token(); // The consumed string
                    }
                    // Fallback to parsing an arbitrary expression, but restrict to expression
                    // types that are valid after the dot operator. This ensures that e.g.
                    // `T.interval` is parsed as a compound identifier, not as an interval
                    // expression.
                    _ => {
                        let expr = self.maybe_parse(|parser| {
                            let expr = parser
                                .parse_subexpr(parser.dialect.prec_value(Precedence::Period))?;
                            match &expr {
                                Expr::CompoundFieldAccess { .. }
                                | Expr::CompoundIdentifier(_)
                                | Expr::Identifier(_)
                                | Expr::Value(_)
                                | Expr::Function(_) => Ok(expr),
                                _ => parser.expected("an identifier or value", parser.peek_token()),
                            }
                        })?;

                        match expr {
                            // If we get back a compound field access or identifier,
                            // we flatten the nested expression.
                            // For example if the current root is `foo`
                            // and we get back a compound identifier expression `bar.baz`
                            // The full expression should be `foo.bar.baz` (i.e.
                            // a root with an access chain with 2 entries) and not
                            // `foo.(bar.baz)` (i.e. a root with an access chain with
                            // 1 entry`).
                            Some(Expr::CompoundFieldAccess { root, access_chain }) => {
                                chain.push(AccessExpr::Dot(*root));
                                chain.extend(access_chain);
                            }
                            Some(Expr::CompoundIdentifier(parts)) => chain.extend(
                                parts.into_iter().map(Expr::Identifier).map(AccessExpr::Dot),
                            ),
                            Some(expr) => {
                                chain.push(AccessExpr::Dot(expr));
                            }
                            // If the expression is not a valid suffix, fall back to
                            // parsing as an identifier. This handles cases like `T.interval`
                            // where `interval` is a keyword but should be treated as an identifier.
                            None => {
                                chain.push(AccessExpr::Dot(Expr::Identifier(
                                    self.parse_identifier()?,
                                )));
                            }
                        }
                    }
                }
            } else if !self.dialect.supports_partiql()
                && self.peek_token_ref().token == Token::LBracket
            {
                self.parse_multi_dim_subscript(&mut chain)?;
            } else {
                break;
            }
        }

        let tok_index = self.get_current_index();
        if let Some(wildcard_token) = ending_wildcard {
            if !Self::is_all_ident(&root, &chain) {
                return self.expected("an identifier or a '*' after '.'", self.peek_token());
            };
            Ok(Expr::QualifiedWildcard(
                ObjectName::from(Self::exprs_to_idents(root, chain)?),
                AttachedToken(wildcard_token),
            ))
        } else if self.maybe_parse_outer_join_operator() {
            if !Self::is_all_ident(&root, &chain) {
                return self.expected_at("column identifier before (+)", tok_index);
            };
            let expr = if chain.is_empty() {
                root
            } else {
                Expr::CompoundIdentifier(Self::exprs_to_idents(root, chain)?)
            };
            Ok(Expr::OuterJoin(expr.into()))
        } else {
            Self::build_compound_expr(root, chain)
        }
    }

    /// Combines a root expression and access chain to form
    /// a compound expression. Which may be a [Expr::CompoundFieldAccess]
    /// or other special cased expressions like [Expr::CompoundIdentifier],
    /// [Expr::OuterJoin].
    fn build_compound_expr(
        root: Expr,
        mut access_chain: Vec<AccessExpr>,
    ) -> Result<Expr, ParserError> {
        if access_chain.is_empty() {
            return Ok(root);
        }

        if Self::is_all_ident(&root, &access_chain) {
            return Ok(Expr::CompoundIdentifier(Self::exprs_to_idents(
                root,
                access_chain,
            )?));
        }

        // Flatten qualified function calls.
        // For example, the expression `a.b.c.foo(1,2,3)` should
        // represent a function called `a.b.c.foo`, rather than
        // a composite expression.
        if matches!(root, Expr::Identifier(_))
            && matches!(
                access_chain.last(),
                Some(AccessExpr::Dot(Expr::Function(_)))
            )
            && access_chain
                .iter()
                .rev()
                .skip(1) // All except the Function
                .all(|access| matches!(access, AccessExpr::Dot(Expr::Identifier(_))))
        {
            let Some(AccessExpr::Dot(Expr::Function(mut func))) = access_chain.pop() else {
                return parser_err!("expected function expression", root.span().start);
            };

            let compound_func_name = [root]
                .into_iter()
                .chain(access_chain.into_iter().flat_map(|access| match access {
                    AccessExpr::Dot(expr) => Some(expr),
                    _ => None,
                }))
                .flat_map(|expr| match expr {
                    Expr::Identifier(ident) => Some(ident),
                    _ => None,
                })
                .map(ObjectNamePart::Identifier)
                .chain(func.name.0)
                .collect::<Vec<_>>();
            func.name = ObjectName(compound_func_name);

            return Ok(Expr::Function(func));
        }

        // Flatten qualified outer join expressions.
        // For example, the expression `T.foo(+)` should
        // represent an outer join on the column name `T.foo`
        // rather than a composite expression.
        if access_chain.len() == 1
            && matches!(
                access_chain.last(),
                Some(AccessExpr::Dot(Expr::OuterJoin(_)))
            )
        {
            let Some(AccessExpr::Dot(Expr::OuterJoin(inner_expr))) = access_chain.pop() else {
                return parser_err!("expected (+) expression", root.span().start);
            };

            if !Self::is_all_ident(&root, &[]) {
                return parser_err!("column identifier before (+)", root.span().start);
            };

            let token_start = root.span().start;
            let mut idents = Self::exprs_to_idents(root, vec![])?;
            match *inner_expr {
                Expr::CompoundIdentifier(suffix) => idents.extend(suffix),
                Expr::Identifier(suffix) => idents.push(suffix),
                _ => {
                    return parser_err!("column identifier before (+)", token_start);
                }
            }

            return Ok(Expr::OuterJoin(Expr::CompoundIdentifier(idents).into()));
        }

        Ok(Expr::CompoundFieldAccess {
            root: Box::new(root),
            access_chain,
        })
    }

    fn keyword_to_modifier(k: Keyword) -> Option<ContextModifier> {
        match k {
            Keyword::LOCAL => Some(ContextModifier::Local),
            Keyword::GLOBAL => Some(ContextModifier::Global),
            Keyword::SESSION => Some(ContextModifier::Session),
            _ => None,
        }
    }

    /// Check if the root is an identifier and all fields are identifiers.
    fn is_all_ident(root: &Expr, fields: &[AccessExpr]) -> bool {
        if !matches!(root, Expr::Identifier(_)) {
            return false;
        }
        fields
            .iter()
            .all(|x| matches!(x, AccessExpr::Dot(Expr::Identifier(_))))
    }

    /// Convert a root and a list of fields to a list of identifiers.
    fn exprs_to_idents(root: Expr, fields: Vec<AccessExpr>) -> Result<Vec<Ident>, ParserError> {
        let mut idents = vec![];
        if let Expr::Identifier(root) = root {
            idents.push(root);
            for x in fields {
                if let AccessExpr::Dot(Expr::Identifier(ident)) = x {
                    idents.push(ident);
                } else {
                    return parser_err!(
                        format!("Expected identifier, found: {}", x),
                        x.span().start
                    );
                }
            }
            Ok(idents)
        } else {
            parser_err!(
                format!("Expected identifier, found: {}", root),
                root.span().start
            )
        }
    }

    /// Returns true if the next tokens indicate the outer join operator `(+)`.
    fn peek_outer_join_operator(&mut self) -> bool {
        if !self.dialect.supports_outer_join_operator() {
            return false;
        }

        let [maybe_lparen, maybe_plus, maybe_rparen] = self.peek_tokens_ref();
        Token::LParen == maybe_lparen.token
            && Token::Plus == maybe_plus.token
            && Token::RParen == maybe_rparen.token
    }

    /// If the next tokens indicates the outer join operator `(+)`, consume
    /// the tokens and return true.
    fn maybe_parse_outer_join_operator(&mut self) -> bool {
        self.dialect.supports_outer_join_operator()
            && self.consume_tokens(&[Token::LParen, Token::Plus, Token::RParen])
    }

    /// Parse utility options in the form of `(option1, option2 arg2, option3 arg3, ...)`
    pub fn parse_utility_options(&mut self) -> Result<Vec<UtilityOption>, ParserError> {
        self.expect_token(&Token::LParen)?;
        let options = self.parse_comma_separated(Self::parse_utility_option)?;
        self.expect_token(&Token::RParen)?;

        Ok(options)
    }

    fn parse_utility_option(&mut self) -> Result<UtilityOption, ParserError> {
        let name = self.parse_identifier()?;

        let next_token = self.peek_token();
        if next_token == Token::Comma || next_token == Token::RParen {
            return Ok(UtilityOption { name, arg: None });
        }
        let arg = self.parse_expr()?;

        Ok(UtilityOption {
            name,
            arg: Some(arg),
        })
    }

    fn try_parse_expr_sub_query(&mut self) -> Result<Option<Expr>, ParserError> {
        if !self.peek_sub_query() {
            return Ok(None);
        }

        Ok(Some(Expr::Subquery(self.parse_query()?)))
    }

    fn try_parse_lambda(&mut self) -> Result<Option<Expr>, ParserError> {
        if !self.dialect.supports_lambda_functions() {
            return Ok(None);
        }
        self.maybe_parse(|p| {
            let params = p.parse_comma_separated(|p| p.parse_identifier())?;
            p.expect_token(&Token::RParen)?;
            p.expect_token(&Token::Arrow)?;
            let expr = p.parse_expr()?;
            Ok(Expr::Lambda(LambdaFunction {
                params: OneOrManyWithParens::Many(params),
                body: Box::new(expr),
                syntax: LambdaSyntax::Arrow,
            }))
        })
    }

    /// Parses a lambda expression using the `LAMBDA` keyword syntax.
    ///
    /// Syntax: `LAMBDA <params> : <expr>`
    ///
    /// Examples:
    /// - `LAMBDA x : x + 1`
    /// - `LAMBDA x, i : x > i`
    ///
    /// See <https://duckdb.org/docs/stable/sql/functions/lambda>
    fn parse_lambda_expr(&mut self) -> Result<Expr, ParserError> {
        // Parse the parameters: either a single identifier or comma-separated identifiers
        let params = if self.consume_token(&Token::LParen) {
            // Parenthesized parameters: (x, y)
            let params = self.parse_comma_separated(|p| p.parse_identifier())?;
            self.expect_token(&Token::RParen)?;
            OneOrManyWithParens::Many(params)
        } else {
            // Unparenthesized parameters: x or x, y
            let params = self.parse_comma_separated(|p| p.parse_identifier())?;
            if params.len() == 1 {
                OneOrManyWithParens::One(params.into_iter().next().unwrap())
            } else {
                OneOrManyWithParens::Many(params)
            }
        };
        // Expect the colon separator
        self.expect_token(&Token::Colon)?;
        // Parse the body expression
        let body = self.parse_expr()?;
        Ok(Expr::Lambda(LambdaFunction {
            params,
            body: Box::new(body),
            syntax: LambdaSyntax::LambdaKeyword,
        }))
    }

    /// Tries to parse the body of an [ODBC escaping sequence]
    /// i.e. without the enclosing braces
    /// Currently implemented:
    /// Scalar Function Calls
    /// Date, Time, and Timestamp Literals
    /// See <https://learn.microsoft.com/en-us/sql/odbc/reference/develop-app/escape-sequences-in-odbc?view=sql-server-2017>
    fn maybe_parse_odbc_body(&mut self) -> Result<Option<Expr>, ParserError> {
        // Attempt 1: Try to parse it as a function.
        if let Some(expr) = self.maybe_parse_odbc_fn_body()? {
            return Ok(Some(expr));
        }
        // Attempt 2: Try to parse it as a Date, Time or Timestamp Literal
        self.maybe_parse_odbc_body_datetime()
    }

    /// Tries to parse the body of an [ODBC Date, Time, and Timestamp Literals] call.
    ///
    /// ```sql
    /// {d '2025-07-17'}
    /// {t '14:12:01'}
    /// {ts '2025-07-17 14:12:01'}
    /// ```
    ///
    /// [ODBC Date, Time, and Timestamp Literals]:
    /// https://learn.microsoft.com/en-us/sql/odbc/reference/develop-app/date-time-and-timestamp-literals?view=sql-server-2017
    fn maybe_parse_odbc_body_datetime(&mut self) -> Result<Option<Expr>, ParserError> {
        self.maybe_parse(|p| {
            let token = p.next_token().clone();
            let word_string = token.token.to_string();
            let data_type = match word_string.as_str() {
                "t" => DataType::Time(None, TimezoneInfo::None),
                "d" => DataType::Date,
                "ts" => DataType::Timestamp(None, TimezoneInfo::None),
                _ => return p.expected("ODBC datetime keyword (t, d, or ts)", token),
            };
            let value = p.parse_value()?;
            Ok(Expr::TypedString(TypedString {
                data_type,
                value,
                uses_odbc_syntax: true,
            }))
        })
    }

    /// Tries to parse the body of an [ODBC function] call.
    /// i.e. without the enclosing braces
    ///
    /// ```sql
    /// fn myfunc(1,2,3)
    /// ```
    ///
    /// [ODBC function]: https://learn.microsoft.com/en-us/sql/odbc/reference/develop-app/scalar-function-calls?view=sql-server-2017
    fn maybe_parse_odbc_fn_body(&mut self) -> Result<Option<Expr>, ParserError> {
        self.maybe_parse(|p| {
            p.expect_keyword(Keyword::FN)?;
            let fn_name = p.parse_object_name(false)?;
            let mut fn_call = p.parse_function_call(fn_name)?;
            fn_call.uses_odbc_syntax = true;
            Ok(Expr::Function(fn_call))
        })
    }

    /// Parse a function call expression named by `name` and return it as an `Expr`.
    pub fn parse_function(&mut self, name: ObjectName) -> Result<Expr, ParserError> {
        self.parse_function_call(name).map(Expr::Function)
    }

    fn parse_function_call(&mut self, name: ObjectName) -> Result<Function, ParserError> {
        self.expect_token(&Token::LParen)?;

        // Snowflake permits a subquery to be passed as an argument without
        // an enclosing set of parens if it's the only argument.
        if self.dialect.supports_subquery_as_function_arg() && self.peek_sub_query() {
            let subquery = self.parse_query()?;
            self.expect_token(&Token::RParen)?;
            return Ok(Function {
                name,
                uses_odbc_syntax: false,
                parameters: FunctionArguments::None,
                args: FunctionArguments::Subquery(subquery),
                filter: None,
                null_treatment: None,
                over: None,
                within_group: vec![],
            });
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
                Some(WindowType::NamedWindow(self.parse_identifier()?))
            }
        } else {
            None
        };

        Ok(Function {
            name,
            uses_odbc_syntax: false,
            parameters,
            args: FunctionArguments::List(args),
            null_treatment,
            filter,
            over,
            within_group,
        })
    }

    /// Optionally parses a null treatment clause.
    fn parse_null_treatment(&mut self) -> Result<Option<NullTreatment>, ParserError> {
        match self.parse_one_of_keywords(&[Keyword::RESPECT, Keyword::IGNORE]) {
            Some(keyword) => {
                self.expect_keyword_is(Keyword::NULLS)?;

                Ok(match keyword {
                    Keyword::RESPECT => Some(NullTreatment::RespectNulls),
                    Keyword::IGNORE => Some(NullTreatment::IgnoreNulls),
                    _ => None,
                })
            }
            None => Ok(None),
        }
    }

    /// Parse time-related function `name` possibly followed by `(...)` arguments.
    pub fn parse_time_functions(&mut self, name: ObjectName) -> Result<Expr, ParserError> {
        let args = if self.consume_token(&Token::LParen) {
            FunctionArguments::List(self.parse_function_argument_list()?)
        } else {
            FunctionArguments::None
        };
        Ok(Expr::Function(Function {
            name,
            uses_odbc_syntax: false,
            parameters: FunctionArguments::None,
            args,
            filter: None,
            over: None,
            null_treatment: None,
            within_group: vec![],
        }))
    }

    /// Parse window frame `UNITS` clause: `ROWS`, `RANGE`, or `GROUPS`.
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

    /// Parse a `WINDOW` frame definition (units and bounds).
    pub fn parse_window_frame(&mut self) -> Result<WindowFrame, ParserError> {
        let units = self.parse_window_frame_units()?;
        let (start_bound, end_bound) = if self.parse_keyword(Keyword::BETWEEN) {
            let start_bound = self.parse_window_frame_bound()?;
            self.expect_keyword_is(Keyword::AND)?;
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

    /// Parse a window frame bound: `CURRENT ROW` or `<n> PRECEDING|FOLLOWING`.
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

    /// Parse a `CASE` expression and return an [`Expr::Case`].
    pub fn parse_case_expr(&mut self) -> Result<Expr, ParserError> {
        let case_token = AttachedToken(self.get_current_token().clone());
        let mut operand = None;
        if !self.parse_keyword(Keyword::WHEN) {
            operand = Some(Box::new(self.parse_expr()?));
            self.expect_keyword_is(Keyword::WHEN)?;
        }
        let mut conditions = vec![];
        loop {
            let condition = self.parse_expr()?;
            self.expect_keyword_is(Keyword::THEN)?;
            let result = self.parse_expr()?;
            conditions.push(CaseWhen { condition, result });
            if !self.parse_keyword(Keyword::WHEN) {
                break;
            }
        }
        let else_result = if self.parse_keyword(Keyword::ELSE) {
            Some(Box::new(self.parse_expr()?))
        } else {
            None
        };
        let end_token = AttachedToken(self.expect_keyword(Keyword::END)?);
        Ok(Expr::Case {
            case_token,
            end_token,
            operand,
            conditions,
            else_result,
        })
    }

    /// Parse an optional `FORMAT` clause for `CAST` expressions.
    pub fn parse_optional_cast_format(&mut self) -> Result<Option<CastFormat>, ParserError> {
        if self.parse_keyword(Keyword::FORMAT) {
            let value = self.parse_value()?.value;
            match self.parse_optional_time_zone()? {
                Some(tz) => Ok(Some(CastFormat::ValueAtTimeZone(value, tz))),
                None => Ok(Some(CastFormat::Value(value))),
            }
        } else {
            Ok(None)
        }
    }

    /// Parse an optional `AT TIME ZONE` clause.
    pub fn parse_optional_time_zone(&mut self) -> Result<Option<Value>, ParserError> {
        if self.parse_keywords(&[Keyword::AT, Keyword::TIME, Keyword::ZONE]) {
            self.parse_value().map(|v| Some(v.value))
        } else {
            Ok(None)
        }
    }

    /// mssql-like convert function
    fn parse_mssql_convert(&mut self, is_try: bool) -> Result<Expr, ParserError> {
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
            is_try,
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
    pub fn parse_convert_expr(&mut self, is_try: bool) -> Result<Expr, ParserError> {
        if self.dialect.convert_type_before_value() {
            return self.parse_mssql_convert(is_try);
        }
        self.expect_token(&Token::LParen)?;
        let expr = self.parse_expr()?;
        if self.parse_keyword(Keyword::USING) {
            let charset = self.parse_object_name(false)?;
            self.expect_token(&Token::RParen)?;
            return Ok(Expr::Convert {
                is_try,
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
            is_try,
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
        self.expect_keyword_is(Keyword::AS)?;
        let data_type = self.parse_data_type()?;
        let array = self.parse_keyword(Keyword::ARRAY);
        let format = self.parse_optional_cast_format()?;
        self.expect_token(&Token::RParen)?;
        Ok(Expr::Cast {
            kind,
            expr: Box::new(expr),
            data_type,
            array,
            format,
        })
    }

    /// Parse a SQL EXISTS expression e.g. `WHERE EXISTS(SELECT ...)`.
    pub fn parse_exists_expr(&mut self, negated: bool) -> Result<Expr, ParserError> {
        self.expect_token(&Token::LParen)?;
        let exists_node = Expr::Exists {
            negated,
            subquery: self.parse_query()?,
        };
        self.expect_token(&Token::RParen)?;
        Ok(exists_node)
    }

    /// Parse a SQL `EXTRACT` expression e.g. `EXTRACT(YEAR FROM date)`.
    pub fn parse_extract_expr(&mut self) -> Result<Expr, ParserError> {
        self.expect_token(&Token::LParen)?;
        let field = self.parse_date_time_field()?;

        let syntax = if self.parse_keyword(Keyword::FROM) {
            ExtractSyntax::From
        } else if self.dialect.supports_extract_comma_syntax() && self.consume_token(&Token::Comma)
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

    /// Parse a `CEIL` or `FLOOR` expression.
    pub fn parse_ceil_floor_expr(&mut self, is_ceil: bool) -> Result<Expr, ParserError> {
        self.expect_token(&Token::LParen)?;
        let expr = self.parse_expr()?;
        // Parse `CEIL/FLOOR(expr)`
        let field = if self.parse_keyword(Keyword::TO) {
            // Parse `CEIL/FLOOR(expr TO DateTimeField)`
            CeilFloorKind::DateTimeField(self.parse_date_time_field()?)
        } else if self.consume_token(&Token::Comma) {
            // Parse `CEIL/FLOOR(expr, scale)`
            match self.parse_value()?.value {
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

    /// Parse a `POSITION` expression.
    pub fn parse_position_expr(&mut self, ident: Ident) -> Result<Expr, ParserError> {
        let between_prec = self.dialect.prec_value(Precedence::Between);
        let position_expr = self.maybe_parse(|p| {
            // PARSE SELECT POSITION('@' in field)
            p.expect_token(&Token::LParen)?;

            // Parse the subexpr till the IN keyword
            let expr = p.parse_subexpr(between_prec)?;
            p.expect_keyword_is(Keyword::IN)?;
            let from = p.parse_expr()?;
            p.expect_token(&Token::RParen)?;
            Ok(Expr::Position {
                expr: Box::new(expr),
                r#in: Box::new(from),
            })
        })?;
        match position_expr {
            Some(expr) => Ok(expr),
            // Snowflake supports `position` as an ordinary function call
            // without the special `IN` syntax.
            None => self.parse_function(ObjectName::from(vec![ident])),
        }
    }

    /// Parse `SUBSTRING`/`SUBSTR` expressions: `SUBSTRING(expr FROM start FOR length)` or `SUBSTR(expr, start, length)`.
    pub fn parse_substring(&mut self) -> Result<Expr, ParserError> {
        let shorthand = match self.expect_one_of_keywords(&[Keyword::SUBSTR, Keyword::SUBSTRING])? {
            Keyword::SUBSTR => true,
            Keyword::SUBSTRING => false,
            _ => {
                self.prev_token();
                return self.expected("SUBSTR or SUBSTRING", self.peek_token());
            }
        };
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
            shorthand,
        })
    }

    /// Parse an OVERLAY expression.
    ///
    /// See [Expr::Overlay]
    pub fn parse_overlay_expr(&mut self) -> Result<Expr, ParserError> {
        // PARSE OVERLAY (EXPR PLACING EXPR FROM 1 [FOR 3])
        self.expect_token(&Token::LParen)?;
        let expr = self.parse_expr()?;
        self.expect_keyword_is(Keyword::PLACING)?;
        let what_expr = self.parse_expr()?;
        self.expect_keyword_is(Keyword::FROM)?;
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
            if [Keyword::BOTH, Keyword::LEADING, Keyword::TRAILING].contains(&word.keyword) {
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
            && dialect_of!(self is DuckDbDialect | SnowflakeDialect | BigQueryDialect | GenericDialect)
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

    /// Parse the `WHERE` field for a `TRIM` expression.
    ///
    /// See [TrimWhereField]
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

    /// Parse the `ON OVERFLOW` clause for `LISTAGG`.
    ///
    /// See [`ListAggOnOverflow`]
    pub fn parse_listagg_on_overflow(&mut self) -> Result<Option<ListAggOnOverflow>, ParserError> {
        if self.parse_keywords(&[Keyword::ON, Keyword::OVERFLOW]) {
            if self.parse_keyword(Keyword::ERROR) {
                Ok(Some(ListAggOnOverflow::Error))
            } else {
                self.expect_keyword_is(Keyword::TRUNCATE)?;
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
                    | Token::QuoteDelimitedStringLiteral(_)
                    | Token::NationalQuoteDelimitedStringLiteral(_)
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
                self.expect_keyword_is(Keyword::COUNT)?;
                Ok(Some(ListAggOnOverflow::Truncate { filler, with_count }))
            }
        } else {
            Ok(None)
        }
    }

    /// Parse a date/time field for `EXTRACT`, interval qualifiers, and ceil/floor operations.
    ///
    /// `EXTRACT` supports a wider set of date/time fields than interval qualifiers,
    /// so this function may need to be split in two.
    ///
    /// See [`DateTimeField`]
    pub fn parse_date_time_field(&mut self) -> Result<DateTimeField, ParserError> {
        let next_token = self.next_token();
        match &next_token.token {
            Token::Word(w) => match w.keyword {
                Keyword::YEAR => Ok(DateTimeField::Year),
                Keyword::YEARS => Ok(DateTimeField::Years),
                Keyword::MONTH => Ok(DateTimeField::Month),
                Keyword::MONTHS => Ok(DateTimeField::Months),
                Keyword::WEEK => {
                    let week_day = if dialect_of!(self is BigQueryDialect | GenericDialect)
                        && self.consume_token(&Token::LParen)
                    {
                        let week_day = self.parse_identifier()?;
                        self.expect_token(&Token::RParen)?;
                        Some(week_day)
                    } else {
                        None
                    };
                    Ok(DateTimeField::Week(week_day))
                }
                Keyword::WEEKS => Ok(DateTimeField::Weeks),
                Keyword::DAY => Ok(DateTimeField::Day),
                Keyword::DAYOFWEEK => Ok(DateTimeField::DayOfWeek),
                Keyword::DAYOFYEAR => Ok(DateTimeField::DayOfYear),
                Keyword::DAYS => Ok(DateTimeField::Days),
                Keyword::DATE => Ok(DateTimeField::Date),
                Keyword::DATETIME => Ok(DateTimeField::Datetime),
                Keyword::HOUR => Ok(DateTimeField::Hour),
                Keyword::HOURS => Ok(DateTimeField::Hours),
                Keyword::MINUTE => Ok(DateTimeField::Minute),
                Keyword::MINUTES => Ok(DateTimeField::Minutes),
                Keyword::SECOND => Ok(DateTimeField::Second),
                Keyword::SECONDS => Ok(DateTimeField::Seconds),
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
                    let custom = self.parse_identifier()?;
                    Ok(DateTimeField::Custom(custom))
                }
                _ => self.expected("date/time field", next_token),
            },
            Token::SingleQuotedString(_) if self.dialect.allow_extract_single_quotes() => {
                self.prev_token();
                let custom = self.parse_identifier()?;
                Ok(DateTimeField::Custom(custom))
            }
            _ => self.expected("date/time field", next_token),
        }
    }

    /// Parse a `NOT` expression.
    ///
    /// Represented in the AST as `Expr::UnaryOp` with `UnaryOperator::Not`.
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

    /// Parse expression types that start with a left brace '{'.
    /// Examples:
    /// ```sql
    /// -- Dictionary expr.
    /// {'key1': 'value1', 'key2': 'value2'}
    ///
    /// -- Function call using the ODBC syntax.
    /// { fn CONCAT('foo', 'bar') }
    /// ```
    fn parse_lbrace_expr(&mut self) -> Result<Expr, ParserError> {
        let token = self.expect_token(&Token::LBrace)?;

        if let Some(fn_expr) = self.maybe_parse_odbc_body()? {
            self.expect_token(&Token::RBrace)?;
            return Ok(fn_expr);
        }

        if self.dialect.supports_dictionary_syntax() {
            self.prev_token(); // Put back the '{'
            return self.parse_dictionary();
        }

        self.expected("an expression", token)
    }

    /// Parses fulltext expressions [`sqlparser::ast::Expr::MatchAgainst`]
    ///
    /// # Errors
    /// This method will raise an error if the column list is empty or with invalid identifiers,
    /// the match expression is not a literal string, or if the search modifier is not valid.
    pub fn parse_match_against(&mut self) -> Result<Expr, ParserError> {
        let columns = self.parse_parenthesized_qualified_column_list(Mandatory, false)?;

        self.expect_keyword_is(Keyword::AGAINST)?;

        self.expect_token(&Token::LParen)?;

        // MySQL is too permissive about the value, IMO we can't validate it perfectly on syntax level.
        let match_value = self.parse_value()?.value;

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

        // to match the different flavours of INTERVAL syntax, we only allow expressions
        // if the dialect requires an interval qualifier,
        // see https://github.com/sqlparser-rs/sqlparser-rs/pull/1398 for more details
        let value = if self.dialect.require_interval_qualifier() {
            // parse a whole expression so `INTERVAL 1 + 1 DAY` is valid
            self.parse_expr()?
        } else {
            // parse a prefix expression so `INTERVAL 1 DAY` is valid, but `INTERVAL 1 + 1 DAY` is not
            // this also means that `INTERVAL '5 days' > INTERVAL '1 day'` treated properly
            self.parse_prefix()?
        };

        // Following the string literal is a qualifier which indicates the units
        // of the duration specified in the string literal.
        //
        // Note that PostgreSQL allows omitting the qualifier, so we provide
        // this more general implementation.
        let leading_field = if self.next_token_is_temporal_unit() {
            Some(self.parse_date_time_field()?)
        } else if self.dialect.require_interval_qualifier() {
            return parser_err!(
                "INTERVAL requires a unit after the literal value",
                self.peek_token().span.start
            );
        } else {
            None
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

    /// Peek at the next token and determine if it is a temporal unit
    /// like `second`.
    pub fn next_token_is_temporal_unit(&mut self) -> bool {
        if let Token::Word(word) = self.peek_token().token {
            matches!(
                word.keyword,
                Keyword::YEAR
                    | Keyword::YEARS
                    | Keyword::MONTH
                    | Keyword::MONTHS
                    | Keyword::WEEK
                    | Keyword::WEEKS
                    | Keyword::DAY
                    | Keyword::DAYS
                    | Keyword::HOUR
                    | Keyword::HOURS
                    | Keyword::MINUTE
                    | Keyword::MINUTES
                    | Keyword::SECOND
                    | Keyword::SECONDS
                    | Keyword::CENTURY
                    | Keyword::DECADE
                    | Keyword::DOW
                    | Keyword::DOY
                    | Keyword::EPOCH
                    | Keyword::ISODOW
                    | Keyword::ISOYEAR
                    | Keyword::JULIAN
                    | Keyword::MICROSECOND
                    | Keyword::MICROSECONDS
                    | Keyword::MILLENIUM
                    | Keyword::MILLENNIUM
                    | Keyword::MILLISECOND
                    | Keyword::MILLISECONDS
                    | Keyword::NANOSECOND
                    | Keyword::NANOSECONDS
                    | Keyword::QUARTER
                    | Keyword::TIMEZONE
                    | Keyword::TIMEZONE_HOUR
                    | Keyword::TIMEZONE_MINUTE
            )
        } else {
            false
        }
    }

    /// Syntax
    /// ```sql
    /// -- typed
    /// STRUCT<[field_name] field_type, ...>( expr1 [, ... ])
    /// -- typeless
    /// STRUCT( expr1 [AS field_name] [, ... ])
    /// ```
    fn parse_struct_literal(&mut self) -> Result<Expr, ParserError> {
        // Parse the fields definition if exist `<[field_name] field_type, ...>`
        self.prev_token();
        let (fields, trailing_bracket) =
            self.parse_struct_type_def(Self::parse_struct_field_def)?;
        if trailing_bracket.0 {
            return parser_err!(
                "unmatched > in STRUCT literal",
                self.peek_token().span.start
            );
        }

        // Parse the struct values `(expr1 [, ... ])`
        self.expect_token(&Token::LParen)?;
        let values = self
            .parse_comma_separated(|parser| parser.parse_struct_field_expr(!fields.is_empty()))?;
        self.expect_token(&Token::RParen)?;

        Ok(Expr::Struct { values, fields })
    }

    /// Parse an expression value for a struct literal
    /// Syntax
    /// ```sql
    /// expr [AS name]
    /// ```
    ///
    /// For biquery [1], Parameter typed_syntax is set to true if the expression
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
                    self.peek_token().span.start
                });
            }
            let field_name = self.parse_identifier()?;
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
        self.expect_keyword_is(Keyword::STRUCT)?;

        // Nothing to do if we have no type information.
        if Token::Lt != self.peek_token() {
            return Ok((Default::default(), false.into()));
        }
        self.next_token();

        let mut field_defs = vec![];
        let trailing_bracket = loop {
            let (def, trailing_bracket) = elem_parser(self)?;
            field_defs.push(def);
            // The struct field definition is finished if it occurs `>>` or comma.
            if trailing_bracket.0 || !self.consume_token(&Token::Comma) {
                break trailing_bracket;
            }
        };

        Ok((
            field_defs,
            self.expect_closing_angle_bracket(trailing_bracket)?,
        ))
    }

    /// Duckdb Struct Data Type <https://duckdb.org/docs/sql/data_types/struct.html#retrieving-from-structs>
    fn parse_duckdb_struct_type_def(&mut self) -> Result<Vec<StructField>, ParserError> {
        self.expect_keyword_is(Keyword::STRUCT)?;
        self.expect_token(&Token::LParen)?;
        let struct_body = self.parse_comma_separated(|parser| {
            let field_name = parser.parse_identifier()?;
            let field_type = parser.parse_data_type()?;

            Ok(StructField {
                field_name: Some(field_name),
                field_type,
                options: None,
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
            Some(self.parse_identifier()?)
        };

        let (field_type, trailing_bracket) = self.parse_data_type_helper()?;

        let options = self.maybe_parse_options(Keyword::OPTIONS)?;
        Ok((
            StructField {
                field_name,
                field_type,
                options,
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
        self.expect_keyword_is(Keyword::UNION)?;

        self.expect_token(&Token::LParen)?;

        let fields = self.parse_comma_separated(|p| {
            Ok(UnionField {
                field_name: p.parse_identifier()?,
                field_type: p.parse_data_type()?,
            })
        })?;

        self.expect_token(&Token::RParen)?;

        Ok(fields)
    }

    /// DuckDB and ClickHouse specific: Parse a duckdb [dictionary] or a clickhouse [map] setting
    ///
    /// Syntax:
    ///
    /// ```sql
    /// {'field_name': expr1[, ... ]}
    /// ```
    ///
    /// [dictionary]: https://duckdb.org/docs/sql/data_types/struct#creating-structs
    /// [map]: https://clickhouse.com/docs/operations/settings/settings#additional_table_filters
    fn parse_dictionary(&mut self) -> Result<Expr, ParserError> {
        self.expect_token(&Token::LBrace)?;

        let fields = self.parse_comma_separated0(Self::parse_dictionary_field, Token::RBrace)?;

        self.expect_token(&Token::RBrace)?;

        Ok(Expr::Dictionary(fields))
    }

    /// Parse a field for a duckdb [dictionary] or a clickhouse [map] setting
    ///
    /// Syntax
    ///
    /// ```sql
    /// 'name': expr
    /// ```
    ///
    /// [dictionary]: https://duckdb.org/docs/sql/data_types/struct#creating-structs
    /// [map]: https://clickhouse.com/docs/operations/settings/settings#additional_table_filters
    fn parse_dictionary_field(&mut self) -> Result<DictionaryField, ParserError> {
        let key = self.parse_identifier()?;

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
        self.expect_keyword_is(Keyword::MAP)?;
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
        self.expect_keyword_is(Keyword::TUPLE)?;
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

        let dialect = self.dialect;

        self.advance_token();
        let tok = self.get_current_token();
        debug!("infix: {tok:?}");
        let tok_index = self.get_current_index();
        let span = tok.span;
        let regular_binary_operator = match &tok.token {
            Token::Spaceship => Some(BinaryOperator::Spaceship),
            Token::DoubleEq => Some(BinaryOperator::Eq),
            Token::Assignment => Some(BinaryOperator::Assignment),
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
                if dialect_is!(dialect is PostgreSqlDialect) {
                    Some(BinaryOperator::PGExp)
                } else {
                    Some(BinaryOperator::BitwiseXor)
                }
            }
            Token::Ampersand => Some(BinaryOperator::BitwiseAnd),
            Token::Div => Some(BinaryOperator::Divide),
            Token::DuckIntDiv if dialect_is!(dialect is DuckDbDialect | GenericDialect) => {
                Some(BinaryOperator::DuckIntegerDivide)
            }
            Token::ShiftLeft if dialect.supports_bitwise_shift_operators() => {
                Some(BinaryOperator::PGBitwiseShiftLeft)
            }
            Token::ShiftRight if dialect.supports_bitwise_shift_operators() => {
                Some(BinaryOperator::PGBitwiseShiftRight)
            }
            Token::Sharp if dialect_is!(dialect is PostgreSqlDialect | RedshiftSqlDialect) => {
                Some(BinaryOperator::PGBitwiseXor)
            }
            Token::Overlap if dialect_is!(dialect is PostgreSqlDialect | RedshiftSqlDialect) => {
                Some(BinaryOperator::PGOverlap)
            }
            Token::Overlap if dialect_is!(dialect is PostgreSqlDialect | GenericDialect) => {
                Some(BinaryOperator::PGOverlap)
            }
            Token::Overlap if dialect.supports_double_ampersand_operator() => {
                Some(BinaryOperator::And)
            }
            Token::CaretAt if dialect_is!(dialect is PostgreSqlDialect | GenericDialect) => {
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
            Token::CustomBinaryOperator(s) => Some(BinaryOperator::Custom(s.clone())),
            Token::DoubleSharp if self.dialect.supports_geometric_types() => {
                Some(BinaryOperator::DoubleHash)
            }

            Token::AmpersandLeftAngleBracket if self.dialect.supports_geometric_types() => {
                Some(BinaryOperator::AndLt)
            }
            Token::AmpersandRightAngleBracket if self.dialect.supports_geometric_types() => {
                Some(BinaryOperator::AndGt)
            }
            Token::QuestionMarkDash if self.dialect.supports_geometric_types() => {
                Some(BinaryOperator::QuestionDash)
            }
            Token::AmpersandLeftAngleBracketVerticalBar
                if self.dialect.supports_geometric_types() =>
            {
                Some(BinaryOperator::AndLtPipe)
            }
            Token::VerticalBarAmpersandRightAngleBracket
                if self.dialect.supports_geometric_types() =>
            {
                Some(BinaryOperator::PipeAndGt)
            }
            Token::TwoWayArrow if self.dialect.supports_geometric_types() => {
                Some(BinaryOperator::LtDashGt)
            }
            Token::LeftAngleBracketCaret if self.dialect.supports_geometric_types() => {
                Some(BinaryOperator::LtCaret)
            }
            Token::RightAngleBracketCaret if self.dialect.supports_geometric_types() => {
                Some(BinaryOperator::GtCaret)
            }
            Token::QuestionMarkSharp if self.dialect.supports_geometric_types() => {
                Some(BinaryOperator::QuestionHash)
            }
            Token::QuestionMarkDoubleVerticalBar if self.dialect.supports_geometric_types() => {
                Some(BinaryOperator::QuestionDoublePipe)
            }
            Token::QuestionMarkDashVerticalBar if self.dialect.supports_geometric_types() => {
                Some(BinaryOperator::QuestionDashPipe)
            }
            Token::TildeEqual if self.dialect.supports_geometric_types() => {
                Some(BinaryOperator::TildeEq)
            }
            Token::ShiftLeftVerticalBar if self.dialect.supports_geometric_types() => {
                Some(BinaryOperator::LtLtPipe)
            }
            Token::VerticalBarShiftRight if self.dialect.supports_geometric_types() => {
                Some(BinaryOperator::PipeGtGt)
            }
            Token::AtSign if self.dialect.supports_geometric_types() => Some(BinaryOperator::At),

            Token::Word(w) => match w.keyword {
                Keyword::AND => Some(BinaryOperator::And),
                Keyword::OR => Some(BinaryOperator::Or),
                Keyword::XOR => Some(BinaryOperator::Xor),
                Keyword::OVERLAPS => Some(BinaryOperator::Overlaps),
                Keyword::OPERATOR if dialect_is!(dialect is PostgreSqlDialect | GenericDialect) => {
                    self.expect_token(&Token::LParen)?;
                    // there are special rules for operator names in
                    // postgres so we can not use 'parse_object'
                    // or similar.
                    // See https://www.postgresql.org/docs/current/sql-createoperator.html
                    let mut idents = vec![];
                    loop {
                        self.advance_token();
                        idents.push(self.get_current_token().to_string());
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

        let tok = self.token_at(tok_index);
        if let Some(op) = regular_binary_operator {
            if let Some(keyword) =
                self.parse_one_of_keywords(&[Keyword::ANY, Keyword::ALL, Keyword::SOME])
            {
                self.expect_token(&Token::LParen)?;
                let right = if self.peek_sub_query() {
                    // We have a subquery ahead (SELECT\WITH ...) need to rewind and
                    // use the parenthesis for parsing the subquery as an expression.
                    self.prev_token(); // LParen
                    self.parse_subexpr(precedence)?
                } else {
                    // Non-subquery expression
                    let right = self.parse_subexpr(precedence)?;
                    self.expect_token(&Token::RParen)?;
                    right
                };

                if !matches!(
                    op,
                    BinaryOperator::Gt
                        | BinaryOperator::Lt
                        | BinaryOperator::GtEq
                        | BinaryOperator::LtEq
                        | BinaryOperator::Eq
                        | BinaryOperator::NotEq
                        | BinaryOperator::PGRegexMatch
                        | BinaryOperator::PGRegexIMatch
                        | BinaryOperator::PGRegexNotMatch
                        | BinaryOperator::PGRegexNotIMatch
                        | BinaryOperator::PGLikeMatch
                        | BinaryOperator::PGILikeMatch
                        | BinaryOperator::PGNotLikeMatch
                        | BinaryOperator::PGNotILikeMatch
                ) {
                    return parser_err!(
                        format!(
                        "Expected one of [=, >, <, =>, =<, !=, ~, ~*, !~, !~*, ~~, ~~*, !~~, !~~*] as comparison operator, found: {op}"
                    ),
                        span.start
                    );
                };

                Ok(match keyword {
                    Keyword::ALL => Expr::AllOp {
                        left: Box::new(expr),
                        compare_op: op,
                        right: Box::new(right),
                    },
                    Keyword::ANY | Keyword::SOME => Expr::AnyOp {
                        left: Box::new(expr),
                        compare_op: op,
                        right: Box::new(right),
                        is_some: keyword == Keyword::SOME,
                    },
                    unexpected_keyword => return Err(ParserError::ParserError(
                        format!("Internal parser error: expected any of {{ALL, ANY, SOME}}, got {unexpected_keyword:?}"),
                    )),
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
                    } else if let Ok(is_normalized) = self.parse_unicode_is_normalized(expr) {
                        Ok(is_normalized)
                    } else {
                        self.expected(
                            "[NOT] NULL | TRUE | FALSE | DISTINCT | [form] NORMALIZED FROM after IS",
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
                    let null = if !self.in_column_definition_state() {
                        self.parse_keyword(Keyword::NULL)
                    } else {
                        false
                    };
                    if regexp || rlike {
                        Ok(Expr::RLike {
                            negated,
                            expr: Box::new(expr),
                            pattern: Box::new(
                                self.parse_subexpr(self.dialect.prec_value(Precedence::Like))?,
                            ),
                            regexp,
                        })
                    } else if negated && null {
                        Ok(Expr::IsNotNull(Box::new(expr)))
                    } else if self.parse_keyword(Keyword::IN) {
                        self.parse_in(expr, negated)
                    } else if self.parse_keyword(Keyword::BETWEEN) {
                        self.parse_between(expr, negated)
                    } else if self.parse_keyword(Keyword::LIKE) {
                        Ok(Expr::Like {
                            negated,
                            any: self.parse_keyword(Keyword::ANY),
                            expr: Box::new(expr),
                            pattern: Box::new(
                                self.parse_subexpr(self.dialect.prec_value(Precedence::Like))?,
                            ),
                            escape_char: self.parse_escape_char()?,
                        })
                    } else if self.parse_keyword(Keyword::ILIKE) {
                        Ok(Expr::ILike {
                            negated,
                            any: self.parse_keyword(Keyword::ANY),
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
                Keyword::NOTNULL if dialect.supports_notnull_operator() => {
                    Ok(Expr::IsNotNull(Box::new(expr)))
                }
                Keyword::MEMBER => {
                    if self.parse_keyword(Keyword::OF) {
                        self.expect_token(&Token::LParen)?;
                        let array = self.parse_expr()?;
                        self.expect_token(&Token::RParen)?;
                        Ok(Expr::MemberOf(MemberOf {
                            value: Box::new(expr),
                            array: Box::new(array),
                        }))
                    } else {
                        self.expected("OF after MEMBER", self.peek_token())
                    }
                }
                // Can only happen if `get_next_precedence` got out of sync with this function
                _ => parser_err!(
                    format!("No infix parser for token {:?}", tok.token),
                    tok.span.start
                ),
            }
        } else if Token::DoubleColon == *tok {
            Ok(Expr::Cast {
                kind: CastKind::DoubleColon,
                expr: Box::new(expr),
                data_type: self.parse_data_type()?,
                array: false,
                format: None,
            })
        } else if Token::ExclamationMark == *tok && self.dialect.supports_factorial_operator() {
            Ok(Expr::UnaryOp {
                op: UnaryOperator::PGPostfixFactorial,
                expr: Box::new(expr),
            })
        } else if Token::LBracket == *tok && self.dialect.supports_partiql()
            || (Token::Colon == *tok)
        {
            self.prev_token();
            self.parse_json_access(expr)
        } else {
            // Can only happen if `get_next_precedence` got out of sync with this function
            parser_err!(
                format!("No infix parser for token {:?}", tok.token),
                tok.span.start
            )
        }
    }

    /// Parse the `ESCAPE CHAR` portion of `LIKE`, `ILIKE`, and `SIMILAR TO`
    pub fn parse_escape_char(&mut self) -> Result<Option<Value>, ParserError> {
        if self.parse_keyword(Keyword::ESCAPE) {
            Ok(Some(self.parse_value()?.into()))
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
            // parse expr until we hit a colon (or any token with lower precedence)
            Some(self.parse_subexpr(self.dialect.prec_value(Precedence::Colon))?)
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
            // parse expr until we hit a colon (or any token with lower precedence)
            Some(self.parse_subexpr(self.dialect.prec_value(Precedence::Colon))?)
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

    /// Parse a multi-dimension array accessing like `[1:3][1][1]`
    pub fn parse_multi_dim_subscript(
        &mut self,
        chain: &mut Vec<AccessExpr>,
    ) -> Result<(), ParserError> {
        while self.consume_token(&Token::LBracket) {
            self.parse_subscript(chain)?;
        }
        Ok(())
    }

    /// Parses an array subscript like `[1:3]`
    ///
    /// Parser is right after `[`
    fn parse_subscript(&mut self, chain: &mut Vec<AccessExpr>) -> Result<(), ParserError> {
        let subscript = self.parse_subscript_inner()?;
        chain.push(AccessExpr::Subscript(subscript));
        Ok(())
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
        let path = self.parse_json_path()?;
        Ok(Expr::JsonAccess {
            value: Box::new(expr),
            path,
        })
    }

    fn parse_json_path(&mut self) -> Result<JsonPath, ParserError> {
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
        Ok(JsonPath { path })
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
        let in_op = match self.maybe_parse(|p| p.parse_query())? {
            Some(subquery) => Expr::InSubquery {
                expr: Box::new(expr),
                subquery,
                negated,
            },
            None => Expr::InList {
                expr: Box::new(expr),
                list: if self.dialect.supports_in_empty_list() {
                    self.parse_comma_separated0(Parser::parse_expr, Token::RParen)?
                } else {
                    self.parse_comma_separated(Parser::parse_expr)?
                },
                negated,
            },
        };
        self.expect_token(&Token::RParen)?;
        Ok(in_op)
    }

    /// Parses `BETWEEN <low> AND <high>`, assuming the `BETWEEN` keyword was already consumed.
    pub fn parse_between(&mut self, expr: Expr, negated: bool) -> Result<Expr, ParserError> {
        // Stop parsing subexpressions for <low> and <high> on tokens with
        // precedence lower than that of `BETWEEN`, such as `AND`, `IS`, etc.
        let low = self.parse_subexpr(self.dialect.prec_value(Precedence::Between))?;
        self.expect_keyword_is(Keyword::AND)?;
        let high = self.parse_subexpr(self.dialect.prec_value(Precedence::Between))?;
        Ok(Expr::Between {
            expr: Box::new(expr),
            negated,
            low: Box::new(low),
            high: Box::new(high),
        })
    }

    /// Parse a PostgreSQL casting style which is in the form of `expr::datatype`.
    pub fn parse_pg_cast(&mut self, expr: Expr) -> Result<Expr, ParserError> {
        Ok(Expr::Cast {
            kind: CastKind::DoubleColon,
            expr: Box::new(expr),
            data_type: self.parse_data_type()?,
            array: false,
            format: None,
        })
    }

    /// Get the precedence of the next token
    pub fn get_next_precedence(&self) -> Result<u8, ParserError> {
        self.dialect.get_next_precedence_default(self)
    }

    /// Return the token at the given location, or EOF if the index is beyond
    /// the length of the current set of tokens.
    pub fn token_at(&self, index: usize) -> &TokenWithSpan {
        self.tokens.get(index).unwrap_or(&EOF_TOKEN)
    }

    /// Return the first non-whitespace token that has not yet been processed
    /// or Token::EOF
    ///
    /// See [`Self::peek_token_ref`] to avoid the copy.
    pub fn peek_token(&self) -> TokenWithSpan {
        self.peek_nth_token(0)
    }

    /// Return a reference to the first non-whitespace token that has not yet
    /// been processed or Token::EOF
    pub fn peek_token_ref(&self) -> &TokenWithSpan {
        self.peek_nth_token_ref(0)
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
    pub fn peek_tokens_with_location<const N: usize>(&self) -> [TokenWithSpan; N] {
        let mut index = self.index;
        core::array::from_fn(|_| loop {
            let token = self.tokens.get(index);
            index += 1;
            if let Some(TokenWithSpan {
                token: Token::Whitespace(_),
                span: _,
            }) = token
            {
                continue;
            }
            break token.cloned().unwrap_or(TokenWithSpan {
                token: Token::EOF,
                span: Span::empty(),
            });
        })
    }

    /// Returns references to the `N` next non-whitespace tokens
    /// that have not yet been processed.
    ///
    /// See [`Self::peek_tokens`] for an example.
    pub fn peek_tokens_ref<const N: usize>(&self) -> [&TokenWithSpan; N] {
        let mut index = self.index;
        core::array::from_fn(|_| loop {
            let token = self.tokens.get(index);
            index += 1;
            if let Some(TokenWithSpan {
                token: Token::Whitespace(_),
                span: _,
            }) = token
            {
                continue;
            }
            break token.unwrap_or(&EOF_TOKEN);
        })
    }

    /// Return nth non-whitespace token that has not yet been processed
    pub fn peek_nth_token(&self, n: usize) -> TokenWithSpan {
        self.peek_nth_token_ref(n).clone()
    }

    /// Return nth non-whitespace token that has not yet been processed
    pub fn peek_nth_token_ref(&self, mut n: usize) -> &TokenWithSpan {
        let mut index = self.index;
        loop {
            index += 1;
            match self.tokens.get(index - 1) {
                Some(TokenWithSpan {
                    token: Token::Whitespace(_),
                    span: _,
                }) => continue,
                non_whitespace => {
                    if n == 0 {
                        return non_whitespace.unwrap_or(&EOF_TOKEN);
                    }
                    n -= 1;
                }
            }
        }
    }

    /// Return the first token, possibly whitespace, that has not yet been processed
    /// (or None if reached end-of-file).
    pub fn peek_token_no_skip(&self) -> TokenWithSpan {
        self.peek_nth_token_no_skip(0)
    }

    /// Return nth token, possibly whitespace, that has not yet been processed.
    pub fn peek_nth_token_no_skip(&self, n: usize) -> TokenWithSpan {
        self.tokens
            .get(self.index + n)
            .cloned()
            .unwrap_or(TokenWithSpan {
                token: Token::EOF,
                span: Span::empty(),
            })
    }

    /// Return nth token, possibly whitespace, that has not yet been processed.
    fn peek_nth_token_no_skip_ref(&self, n: usize) -> &TokenWithSpan {
        self.tokens.get(self.index + n).unwrap_or(&EOF_TOKEN)
    }

    /// Return true if the next tokens exactly `expected`
    ///
    /// Does not advance the current token.
    fn peek_keywords(&mut self, expected: &[Keyword]) -> bool {
        let index = self.index;
        let matched = self.parse_keywords(expected);
        self.index = index;
        matched
    }

    /// Advances to the next non-whitespace token and returns a copy.
    ///
    /// Please use [`Self::advance_token`] and [`Self::get_current_token`] to
    /// avoid the copy.
    pub fn next_token(&mut self) -> TokenWithSpan {
        self.advance_token();
        self.get_current_token().clone()
    }

    /// Returns the index of the current token
    ///
    /// This can be used with APIs that expect an index, such as
    /// [`Self::token_at`]
    pub fn get_current_index(&self) -> usize {
        self.index.saturating_sub(1)
    }

    /// Return the next unprocessed token, possibly whitespace.
    pub fn next_token_no_skip(&mut self) -> Option<&TokenWithSpan> {
        self.index += 1;
        self.tokens.get(self.index - 1)
    }

    /// Advances the current token to the next non-whitespace token
    ///
    /// See [`Self::get_current_token`] to get the current token after advancing
    pub fn advance_token(&mut self) {
        loop {
            self.index += 1;
            match self.tokens.get(self.index - 1) {
                Some(TokenWithSpan {
                    token: Token::Whitespace(_),
                    span: _,
                }) => continue,
                _ => break,
            }
        }
    }

    /// Returns a reference to the current token
    ///
    /// Does not advance the current token.
    pub fn get_current_token(&self) -> &TokenWithSpan {
        self.token_at(self.index.saturating_sub(1))
    }

    /// Returns a reference to the previous token
    ///
    /// Does not advance the current token.
    pub fn get_previous_token(&self) -> &TokenWithSpan {
        self.token_at(self.index.saturating_sub(2))
    }

    /// Returns a reference to the next token
    ///
    /// Does not advance the current token.
    pub fn get_next_token(&self) -> &TokenWithSpan {
        self.token_at(self.index)
    }

    /// Seek back the last one non-whitespace token.
    ///
    /// Must be called after `next_token()`, otherwise might panic. OK to call
    /// after `next_token()` indicates an EOF.
    ///
    // TODO rename to backup_token and deprecate prev_token?
    pub fn prev_token(&mut self) {
        loop {
            assert!(self.index > 0);
            self.index -= 1;
            if let Some(TokenWithSpan {
                token: Token::Whitespace(_),
                span: _,
            }) = self.tokens.get(self.index)
            {
                continue;
            }
            return;
        }
    }

    /// Report `found` was encountered instead of `expected`
    pub fn expected<T>(&self, expected: &str, found: TokenWithSpan) -> Result<T, ParserError> {
        parser_err!(
            format!("Expected: {expected}, found: {found}"),
            found.span.start
        )
    }

    /// report `found` was encountered instead of `expected`
    pub fn expected_ref<T>(&self, expected: &str, found: &TokenWithSpan) -> Result<T, ParserError> {
        parser_err!(
            format!("Expected: {expected}, found: {found}"),
            found.span.start
        )
    }

    /// Report that the token at `index` was found instead of `expected`.
    pub fn expected_at<T>(&self, expected: &str, index: usize) -> Result<T, ParserError> {
        let found = self.tokens.get(index).unwrap_or(&EOF_TOKEN);
        parser_err!(
            format!("Expected: {expected}, found: {found}"),
            found.span.start
        )
    }

    /// If the current token is the `expected` keyword, consume it and returns
    /// true. Otherwise, no tokens are consumed and returns false.
    #[must_use]
    pub fn parse_keyword(&mut self, expected: Keyword) -> bool {
        if self.peek_keyword(expected) {
            self.advance_token();
            true
        } else {
            false
        }
    }

    #[must_use]
    /// Check if the current token is the expected keyword without consuming it.
    ///
    /// Returns true if the current token matches the expected keyword.
    pub fn peek_keyword(&self, expected: Keyword) -> bool {
        matches!(&self.peek_token_ref().token, Token::Word(w) if expected == w.keyword)
    }

    /// If the current token is the `expected` keyword followed by
    /// specified tokens, consume them and returns true.
    /// Otherwise, no tokens are consumed and returns false.
    ///
    /// Note that if the length of `tokens` is too long, this function will
    /// not be efficient as it does a loop on the tokens with `peek_nth_token`
    /// each time.
    pub fn parse_keyword_with_tokens(&mut self, expected: Keyword, tokens: &[Token]) -> bool {
        self.keyword_with_tokens(expected, tokens, true)
    }

    /// Peeks to see if the current token is the `expected` keyword followed by specified tokens
    /// without consuming them.
    ///
    /// See [Self::parse_keyword_with_tokens] for details.
    pub(crate) fn peek_keyword_with_tokens(&mut self, expected: Keyword, tokens: &[Token]) -> bool {
        self.keyword_with_tokens(expected, tokens, false)
    }

    fn keyword_with_tokens(&mut self, expected: Keyword, tokens: &[Token], consume: bool) -> bool {
        match &self.peek_token_ref().token {
            Token::Word(w) if expected == w.keyword => {
                for (idx, token) in tokens.iter().enumerate() {
                    if self.peek_nth_token_ref(idx + 1).token != *token {
                        return false;
                    }
                }

                if consume {
                    for _ in 0..(tokens.len() + 1) {
                        self.advance_token();
                    }
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

    /// If the current token is one of the given `keywords`, returns the keyword
    /// that matches, without consuming the token. Otherwise, returns [`None`].
    #[must_use]
    pub fn peek_one_of_keywords(&self, keywords: &[Keyword]) -> Option<Keyword> {
        for keyword in keywords {
            if self.peek_keyword(*keyword) {
                return Some(*keyword);
            }
        }
        None
    }

    /// If the current token is one of the given `keywords`, consume the token
    /// and return the keyword that matches. Otherwise, no tokens are consumed
    /// and returns [`None`].
    #[must_use]
    pub fn parse_one_of_keywords(&mut self, keywords: &[Keyword]) -> Option<Keyword> {
        match &self.peek_token_ref().token {
            Token::Word(w) => {
                keywords
                    .iter()
                    .find(|keyword| **keyword == w.keyword)
                    .map(|keyword| {
                        self.advance_token();
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
            self.expected_ref(
                &format!("one of {}", keywords.join(" or ")),
                self.peek_token_ref(),
            )
        }
    }

    /// If the current token is the `expected` keyword, consume the token.
    /// Otherwise, return an error.
    ///
    // todo deprecate in favor of expected_keyword_is
    pub fn expect_keyword(&mut self, expected: Keyword) -> Result<TokenWithSpan, ParserError> {
        if self.parse_keyword(expected) {
            Ok(self.get_current_token().clone())
        } else {
            self.expected_ref(format!("{:?}", &expected).as_str(), self.peek_token_ref())
        }
    }

    /// If the current token is the `expected` keyword, consume the token.
    /// Otherwise, return an error.
    ///
    /// This differs from expect_keyword only in that the matched keyword
    /// token is not returned.
    pub fn expect_keyword_is(&mut self, expected: Keyword) -> Result<(), ParserError> {
        if self.parse_keyword(expected) {
            Ok(())
        } else {
            self.expected_ref(format!("{:?}", &expected).as_str(), self.peek_token_ref())
        }
    }

    /// If the current and subsequent tokens exactly match the `keywords`
    /// sequence, consume them and returns Ok. Otherwise, return an Error.
    pub fn expect_keywords(&mut self, expected: &[Keyword]) -> Result<(), ParserError> {
        for &kw in expected {
            self.expect_keyword_is(kw)?;
        }
        Ok(())
    }

    /// Consume the next token if it matches the expected token, otherwise return false
    ///
    /// See [Self::advance_token] to consume the token unconditionally
    #[must_use]
    pub fn consume_token(&mut self, expected: &Token) -> bool {
        if self.peek_token_ref() == expected {
            self.advance_token();
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
    pub fn expect_token(&mut self, expected: &Token) -> Result<TokenWithSpan, ParserError> {
        if self.peek_token_ref() == expected {
            Ok(self.next_token())
        } else {
            self.expected_ref(&expected.to_string(), self.peek_token_ref())
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

        let trailing_commas =
            self.options.trailing_commas | self.dialect.supports_projection_trailing_commas();

        self.parse_comma_separated_with_trailing_commas(
            |p| p.parse_select_item(),
            trailing_commas,
            Self::is_reserved_for_column_alias,
        )
    }

    /// Parse a list of actions for `GRANT` statements.
    pub fn parse_actions_list(&mut self) -> Result<Vec<Action>, ParserError> {
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

    /// Parse a list of [TableWithJoins]
    fn parse_table_with_joins(&mut self) -> Result<Vec<TableWithJoins>, ParserError> {
        let trailing_commas = self.dialect.supports_from_trailing_commas();

        self.parse_comma_separated_with_trailing_commas(
            Parser::parse_table_and_joins,
            trailing_commas,
            |kw, parser| !self.dialect.is_table_factor(kw, parser),
        )
    }

    /// Parse the comma of a comma-separated syntax element.
    /// `R` is a predicate that should return true if the next
    /// keyword is a reserved keyword.
    /// Allows for control over trailing commas
    ///
    /// Returns true if there is a next element
    fn is_parse_comma_separated_end_with_trailing_commas<R>(
        &mut self,
        trailing_commas: bool,
        is_reserved_keyword: &R,
    ) -> bool
    where
        R: Fn(&Keyword, &mut Parser) -> bool,
    {
        if !self.consume_token(&Token::Comma) {
            true
        } else if trailing_commas {
            let token = self.next_token().token;
            let is_end = match token {
                Token::Word(ref kw) if is_reserved_keyword(&kw.keyword, self) => true,
                Token::RParen | Token::SemiColon | Token::EOF | Token::RBracket | Token::RBrace => {
                    true
                }
                _ => false,
            };
            self.prev_token();

            is_end
        } else {
            false
        }
    }

    /// Parse the comma of a comma-separated syntax element.
    /// Returns true if there is a next element
    fn is_parse_comma_separated_end(&mut self) -> bool {
        self.is_parse_comma_separated_end_with_trailing_commas(
            self.options.trailing_commas,
            &Self::is_reserved_for_column_alias,
        )
    }

    /// Parse a comma-separated list of 1+ items accepted by `F`
    pub fn parse_comma_separated<T, F>(&mut self, f: F) -> Result<Vec<T>, ParserError>
    where
        F: FnMut(&mut Parser<'a>) -> Result<T, ParserError>,
    {
        self.parse_comma_separated_with_trailing_commas(
            f,
            self.options.trailing_commas,
            Self::is_reserved_for_column_alias,
        )
    }

    /// Parse a comma-separated list of 1+ items accepted by `F`.
    /// `R` is a predicate that should return true if the next
    /// keyword is a reserved keyword.
    /// Allows for control over trailing commas.
    fn parse_comma_separated_with_trailing_commas<T, F, R>(
        &mut self,
        mut f: F,
        trailing_commas: bool,
        is_reserved_keyword: R,
    ) -> Result<Vec<T>, ParserError>
    where
        F: FnMut(&mut Parser<'a>) -> Result<T, ParserError>,
        R: Fn(&Keyword, &mut Parser) -> bool,
    {
        let mut values = vec![];
        loop {
            values.push(f(self)?);
            if self.is_parse_comma_separated_end_with_trailing_commas(
                trailing_commas,
                &is_reserved_keyword,
            ) {
                break;
            }
        }
        Ok(values)
    }

    /// Parse a period-separated list of 1+ items accepted by `F`
    fn parse_period_separated<T, F>(&mut self, mut f: F) -> Result<Vec<T>, ParserError>
    where
        F: FnMut(&mut Parser<'a>) -> Result<T, ParserError>,
    {
        let mut values = vec![];
        loop {
            values.push(f(self)?);
            if !self.consume_token(&Token::Period) {
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

    /// Parse an expression enclosed in parentheses.
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

    /// Parses 0 or more statements, each followed by a semicolon.
    /// If the next token is any of `terminal_keywords` then no more
    /// statements will be parsed.
    pub(crate) fn parse_statement_list(
        &mut self,
        terminal_keywords: &[Keyword],
    ) -> Result<Vec<Statement>, ParserError> {
        let mut values = vec![];
        loop {
            match &self.peek_nth_token_ref(0).token {
                Token::EOF => break,
                Token::Word(w) => {
                    if w.quote_style.is_none() && terminal_keywords.contains(&w.keyword) {
                        break;
                    }
                }
                _ => {}
            }

            values.push(self.parse_statement()?);
            self.expect_token(&Token::SemiColon)?;
        }
        Ok(values)
    }

    /// Default implementation of a predicate that returns true if
    /// the specified keyword is reserved for column alias.
    /// See [Dialect::is_column_alias]
    fn is_reserved_for_column_alias(kw: &Keyword, parser: &mut Parser) -> bool {
        !parser.dialect.is_column_alias(kw, parser)
    }

    /// Run a parser method `f`, reverting back to the current position if unsuccessful.
    /// Returns `ParserError::RecursionLimitExceeded` if `f` returns a `RecursionLimitExceeded`.
    /// Returns `Ok(None)` if `f` returns any other error.
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

    /// Parse either `ALL`, `DISTINCT` or `DISTINCT ON (...)`. Returns [`None`] if `ALL` is parsed
    /// and results in a [`ParserError`] if both `ALL` and `DISTINCT` are found.
    pub fn parse_all_or_distinct(&mut self) -> Result<Option<Distinct>, ParserError> {
        let loc = self.peek_token().span.start;
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
        let create_view_params = self.parse_create_view_params()?;
        if self.parse_keyword(Keyword::TABLE) {
            self.parse_create_table(or_replace, temporary, global, transient)
                .map(Into::into)
        } else if self.peek_keyword(Keyword::MATERIALIZED)
            || self.peek_keyword(Keyword::VIEW)
            || self.peek_keywords(&[Keyword::SECURE, Keyword::MATERIALIZED, Keyword::VIEW])
            || self.peek_keywords(&[Keyword::SECURE, Keyword::VIEW])
        {
            self.parse_create_view(or_alter, or_replace, temporary, create_view_params)
                .map(Into::into)
        } else if self.parse_keyword(Keyword::POLICY) {
            self.parse_create_policy()
        } else if self.parse_keyword(Keyword::EXTERNAL) {
            self.parse_create_external_table(or_replace).map(Into::into)
        } else if self.parse_keyword(Keyword::FUNCTION) {
            self.parse_create_function(or_alter, or_replace, temporary)
        } else if self.parse_keyword(Keyword::DOMAIN) {
            self.parse_create_domain().map(Into::into)
        } else if self.parse_keyword(Keyword::TRIGGER) {
            self.parse_create_trigger(temporary, or_alter, or_replace, false)
                .map(Into::into)
        } else if self.parse_keywords(&[Keyword::CONSTRAINT, Keyword::TRIGGER]) {
            self.parse_create_trigger(temporary, or_alter, or_replace, true)
                .map(Into::into)
        } else if self.parse_keyword(Keyword::MACRO) {
            self.parse_create_macro(or_replace, temporary)
        } else if self.parse_keyword(Keyword::SECRET) {
            self.parse_create_secret(or_replace, temporary, persistent)
        } else if self.parse_keyword(Keyword::USER) {
            self.parse_create_user(or_replace).map(Into::into)
        } else if or_replace {
            self.expected(
                "[EXTERNAL] TABLE or [MATERIALIZED] VIEW or FUNCTION after CREATE OR REPLACE",
                self.peek_token(),
            )
        } else if self.parse_keyword(Keyword::EXTENSION) {
            self.parse_create_extension().map(Into::into)
        } else if self.parse_keyword(Keyword::INDEX) {
            self.parse_create_index(false).map(Into::into)
        } else if self.parse_keywords(&[Keyword::UNIQUE, Keyword::INDEX]) {
            self.parse_create_index(true).map(Into::into)
        } else if self.parse_keyword(Keyword::VIRTUAL) {
            self.parse_create_virtual_table()
        } else if self.parse_keyword(Keyword::SCHEMA) {
            self.parse_create_schema()
        } else if self.parse_keyword(Keyword::DATABASE) {
            self.parse_create_database()
        } else if self.parse_keyword(Keyword::ROLE) {
            self.parse_create_role().map(Into::into)
        } else if self.parse_keyword(Keyword::SEQUENCE) {
            self.parse_create_sequence(temporary)
        } else if self.parse_keyword(Keyword::TYPE) {
            self.parse_create_type()
        } else if self.parse_keyword(Keyword::PROCEDURE) {
            self.parse_create_procedure(or_alter)
        } else if self.parse_keyword(Keyword::CONNECTOR) {
            self.parse_create_connector().map(Into::into)
        } else if self.parse_keyword(Keyword::OPERATOR) {
            // Check if this is CREATE OPERATOR FAMILY or CREATE OPERATOR CLASS
            if self.parse_keyword(Keyword::FAMILY) {
                self.parse_create_operator_family().map(Into::into)
            } else if self.parse_keyword(Keyword::CLASS) {
                self.parse_create_operator_class().map(Into::into)
            } else {
                self.parse_create_operator().map(Into::into)
            }
        } else if self.parse_keyword(Keyword::SERVER) {
            self.parse_pg_create_server()
        } else {
            self.expected("an object type after CREATE", self.peek_token())
        }
    }

    fn parse_create_user(&mut self, or_replace: bool) -> Result<CreateUser, ParserError> {
        let if_not_exists = self.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let name = self.parse_identifier()?;
        let options = self
            .parse_key_value_options(false, &[Keyword::WITH, Keyword::TAG])?
            .options;
        let with_tags = self.parse_keyword(Keyword::WITH);
        let tags = if self.parse_keyword(Keyword::TAG) {
            self.parse_key_value_options(true, &[])?.options
        } else {
            vec![]
        };
        Ok(CreateUser {
            or_replace,
            if_not_exists,
            name,
            options: KeyValueOptions {
                options,
                delimiter: KeyValueOptionsDelimiter::Space,
            },
            with_tags,
            tags: KeyValueOptions {
                options: tags,
                delimiter: KeyValueOptionsDelimiter::Comma,
            },
        })
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
                storage_specifier = self.parse_identifier().ok()
            } else {
                name = self.parse_identifier().ok();
            }

            // Storage specifier may follow the name
            if storage_specifier.is_none()
                && self.peek_token() != Token::LParen
                && self.parse_keyword(Keyword::IN)
            {
                storage_specifier = self.parse_identifier().ok();
            }
        }

        self.expect_token(&Token::LParen)?;
        self.expect_keyword_is(Keyword::TYPE)?;
        let secret_type = self.parse_identifier()?;

        let mut options = Vec::new();
        if self.consume_token(&Token::Comma) {
            options.append(&mut self.parse_comma_separated(|p| {
                let key = p.parse_identifier()?;
                let value = p.parse_identifier()?;
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
    pub fn parse_as_query(&mut self) -> Result<(bool, Box<Query>), ParserError> {
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
        self.expect_keyword_is(Keyword::TABLE)?;
        let if_exists = self.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
        let table_name = self.parse_object_name(false)?;
        Ok(Statement::UNCache {
            table_name,
            if_exists,
        })
    }

    /// SQLite-specific `CREATE VIRTUAL TABLE`
    pub fn parse_create_virtual_table(&mut self) -> Result<Statement, ParserError> {
        self.expect_keyword_is(Keyword::TABLE)?;
        let if_not_exists = self.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let table_name = self.parse_object_name(false)?;
        self.expect_keyword_is(Keyword::USING)?;
        let module_name = self.parse_identifier()?;
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

    /// Parse a `CREATE SCHEMA` statement.
    pub fn parse_create_schema(&mut self) -> Result<Statement, ParserError> {
        let if_not_exists = self.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);

        let schema_name = self.parse_schema_name()?;

        let default_collate_spec = if self.parse_keywords(&[Keyword::DEFAULT, Keyword::COLLATE]) {
            Some(self.parse_expr()?)
        } else {
            None
        };

        let with = if self.peek_keyword(Keyword::WITH) {
            Some(self.parse_options(Keyword::WITH)?)
        } else {
            None
        };

        let options = if self.peek_keyword(Keyword::OPTIONS) {
            Some(self.parse_options(Keyword::OPTIONS)?)
        } else {
            None
        };

        let clone = if self.parse_keyword(Keyword::CLONE) {
            Some(self.parse_object_name(false)?)
        } else {
            None
        };

        Ok(Statement::CreateSchema {
            schema_name,
            if_not_exists,
            with,
            options,
            default_collate_spec,
            clone,
        })
    }

    fn parse_schema_name(&mut self) -> Result<SchemaName, ParserError> {
        if self.parse_keyword(Keyword::AUTHORIZATION) {
            Ok(SchemaName::UnnamedAuthorization(self.parse_identifier()?))
        } else {
            let name = self.parse_object_name(false)?;

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

    /// Parse a `CREATE DATABASE` statement.
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
        let clone = if self.parse_keyword(Keyword::CLONE) {
            Some(self.parse_object_name(false)?)
        } else {
            None
        };

        Ok(Statement::CreateDatabase {
            db_name,
            if_not_exists: ine,
            location,
            managed_location,
            or_replace: false,
            transient: false,
            clone,
            data_retention_time_in_days: None,
            max_data_extension_time_in_days: None,
            external_volume: None,
            catalog: None,
            replace_invalid_characters: None,
            default_ddl_collation: None,
            storage_serialization_policy: None,
            comment: None,
            catalog_sync: None,
            catalog_sync_namespace_mode: None,
            catalog_sync_namespace_flatten_delimiter: None,
            with_tags: None,
            with_contacts: None,
        })
    }

    /// Parse an optional `USING` clause for `CREATE FUNCTION`.
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
                TokenWithSpan::wrap(Token::make_keyword(format!("{keyword:?}").as_str())),
            ),
        }
    }

    /// Parse a `CREATE FUNCTION` statement.
    pub fn parse_create_function(
        &mut self,
        or_alter: bool,
        or_replace: bool,
        temporary: bool,
    ) -> Result<Statement, ParserError> {
        if dialect_of!(self is HiveDialect) {
            self.parse_hive_create_function(or_replace, temporary)
                .map(Into::into)
        } else if dialect_of!(self is PostgreSqlDialect | GenericDialect) {
            self.parse_postgres_create_function(or_replace, temporary)
                .map(Into::into)
        } else if dialect_of!(self is DuckDbDialect) {
            self.parse_create_macro(or_replace, temporary)
        } else if dialect_of!(self is BigQueryDialect) {
            self.parse_bigquery_create_function(or_replace, temporary)
                .map(Into::into)
        } else if dialect_of!(self is MsSqlDialect) {
            self.parse_mssql_create_function(or_alter, or_replace, temporary)
                .map(Into::into)
        } else {
            self.prev_token();
            self.expected("an object type after CREATE", self.peek_token())
        }
    }

    /// Parse `CREATE FUNCTION` for [PostgreSQL]
    ///
    /// [PostgreSQL]: https://www.postgresql.org/docs/15/sql-createfunction.html
    fn parse_postgres_create_function(
        &mut self,
        or_replace: bool,
        temporary: bool,
    ) -> Result<CreateFunction, ParserError> {
        let name = self.parse_object_name(false)?;

        self.expect_token(&Token::LParen)?;
        let args = if Token::RParen != self.peek_token_ref().token {
            self.parse_comma_separated(Parser::parse_function_arg)?
        } else {
            vec![]
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
            security: Option<FunctionSecurity>,
        }
        let mut body = Body::default();
        let mut set_params: Vec<FunctionDefinitionSetParam> = Vec::new();
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
                body.function_body = Some(self.parse_create_function_body_string()?);
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
            } else if self.parse_keyword(Keyword::SECURITY) {
                ensure_not_set(&body.security, "SECURITY { DEFINER | INVOKER }")?;
                if self.parse_keyword(Keyword::DEFINER) {
                    body.security = Some(FunctionSecurity::Definer);
                } else if self.parse_keyword(Keyword::INVOKER) {
                    body.security = Some(FunctionSecurity::Invoker);
                } else {
                    return self.expected("DEFINER or INVOKER", self.peek_token());
                }
            } else if self.parse_keyword(Keyword::SET) {
                let name = self.parse_identifier()?;
                let value = if self.parse_keywords(&[Keyword::FROM, Keyword::CURRENT]) {
                    FunctionSetValue::FromCurrent
                } else {
                    if !self.consume_token(&Token::Eq) && !self.parse_keyword(Keyword::TO) {
                        return self.expected("= or TO", self.peek_token());
                    }
                    let values = self.parse_comma_separated(Parser::parse_expr)?;
                    FunctionSetValue::Values(values)
                };
                set_params.push(FunctionDefinitionSetParam { name, value });
            } else if self.parse_keyword(Keyword::RETURN) {
                ensure_not_set(&body.function_body, "RETURN")?;
                body.function_body = Some(CreateFunctionBody::Return(self.parse_expr()?));
            } else {
                break;
            }
        }

        Ok(CreateFunction {
            or_alter: false,
            or_replace,
            temporary,
            name,
            args: Some(args),
            return_type,
            behavior: body.behavior,
            called_on_null: body.called_on_null,
            parallel: body.parallel,
            security: body.security,
            set_params,
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
    ) -> Result<CreateFunction, ParserError> {
        let name = self.parse_object_name(false)?;
        self.expect_keyword_is(Keyword::AS)?;

        let body = self.parse_create_function_body_string()?;
        let using = self.parse_optional_create_function_using()?;

        Ok(CreateFunction {
            or_alter: false,
            or_replace,
            temporary,
            name,
            function_body: Some(body),
            using,
            if_not_exists: false,
            args: None,
            return_type: None,
            behavior: None,
            called_on_null: None,
            parallel: None,
            security: None,
            set_params: vec![],
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
    ) -> Result<CreateFunction, ParserError> {
        let if_not_exists = self.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let (name, args) = self.parse_create_function_name_and_params()?;

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
            Some(self.parse_identifier()?)
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
            self.expect_keyword_is(Keyword::AS)?;
            let expr = self.parse_expr()?;
            if options.is_none() {
                options = self.maybe_parse_options(Keyword::OPTIONS)?;
                Some(CreateFunctionBody::AsBeforeOptions {
                    body: expr,
                    link_symbol: None,
                })
            } else {
                Some(CreateFunctionBody::AsAfterOptions(expr))
            }
        } else {
            None
        };

        Ok(CreateFunction {
            or_alter: false,
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
            security: None,
            set_params: vec![],
        })
    }

    /// Parse `CREATE FUNCTION` for [MsSql]
    ///
    /// [MsSql]: https://learn.microsoft.com/en-us/sql/t-sql/statements/create-function-transact-sql
    fn parse_mssql_create_function(
        &mut self,
        or_alter: bool,
        or_replace: bool,
        temporary: bool,
    ) -> Result<CreateFunction, ParserError> {
        let (name, args) = self.parse_create_function_name_and_params()?;

        self.expect_keyword(Keyword::RETURNS)?;

        let return_table = self.maybe_parse(|p| {
            let return_table_name = p.parse_identifier()?;

            p.expect_keyword_is(Keyword::TABLE)?;
            p.prev_token();

            let table_column_defs = match p.parse_data_type()? {
                DataType::Table(Some(table_column_defs)) if !table_column_defs.is_empty() => {
                    table_column_defs
                }
                _ => parser_err!(
                    "Expected table column definitions after TABLE keyword",
                    p.peek_token().span.start
                )?,
            };

            Ok(DataType::NamedTable {
                name: ObjectName(vec![ObjectNamePart::Identifier(return_table_name)]),
                columns: table_column_defs,
            })
        })?;

        let return_type = if return_table.is_some() {
            return_table
        } else {
            Some(self.parse_data_type()?)
        };

        let _ = self.parse_keyword(Keyword::AS);

        let function_body = if self.peek_keyword(Keyword::BEGIN) {
            let begin_token = self.expect_keyword(Keyword::BEGIN)?;
            let statements = self.parse_statement_list(&[Keyword::END])?;
            let end_token = self.expect_keyword(Keyword::END)?;

            Some(CreateFunctionBody::AsBeginEnd(BeginEndStatements {
                begin_token: AttachedToken(begin_token),
                statements,
                end_token: AttachedToken(end_token),
            }))
        } else if self.parse_keyword(Keyword::RETURN) {
            if self.peek_token() == Token::LParen {
                Some(CreateFunctionBody::AsReturnExpr(self.parse_expr()?))
            } else if self.peek_keyword(Keyword::SELECT) {
                let select = self.parse_select()?;
                Some(CreateFunctionBody::AsReturnSelect(select))
            } else {
                parser_err!(
                    "Expected a subquery (or bare SELECT statement) after RETURN",
                    self.peek_token().span.start
                )?
            }
        } else {
            parser_err!("Unparsable function body", self.peek_token().span.start)?
        };

        Ok(CreateFunction {
            or_alter,
            or_replace,
            temporary,
            if_not_exists: false,
            name,
            args: Some(args),
            return_type,
            function_body,
            language: None,
            determinism_specifier: None,
            options: None,
            remote_connection: None,
            using: None,
            behavior: None,
            called_on_null: None,
            parallel: None,
            security: None,
            set_params: vec![],
        })
    }

    fn parse_create_function_name_and_params(
        &mut self,
    ) -> Result<(ObjectName, Vec<OperateFunctionArg>), ParserError> {
        let name = self.parse_object_name(false)?;
        let parse_function_param =
            |parser: &mut Parser| -> Result<OperateFunctionArg, ParserError> {
                let name = parser.parse_identifier()?;
                let data_type = parser.parse_data_type()?;
                let default_expr = if parser.consume_token(&Token::Eq) {
                    Some(parser.parse_expr()?)
                } else {
                    None
                };

                Ok(OperateFunctionArg {
                    mode: None,
                    name: Some(name),
                    data_type,
                    default_expr,
                })
            };
        self.expect_token(&Token::LParen)?;
        let args = self.parse_comma_separated0(parse_function_param, Token::RParen)?;
        self.expect_token(&Token::RParen)?;
        Ok((name, args))
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

        // To check whether the first token is a name or a type, we need to
        // peek the next token, which if it is another type keyword, then the
        // first token is a name and not a type in itself.
        let data_type_idx = self.get_current_index();

        // DEFAULT will be parsed as `DataType::Custom`, which is undesirable in this context
        fn parse_data_type_no_default(parser: &mut Parser) -> Result<DataType, ParserError> {
            if parser.peek_keyword(Keyword::DEFAULT) {
                // This dummy error is ignored in `maybe_parse`
                parser_err!(
                    "The DEFAULT keyword is not a type",
                    parser.peek_token().span.start
                )
            } else {
                parser.parse_data_type()
            }
        }

        if let Some(next_data_type) = self.maybe_parse(parse_data_type_no_default)? {
            let token = self.token_at(data_type_idx);

            // We ensure that the token is a `Word` token, and not other special tokens.
            if !matches!(token.token, Token::Word(_)) {
                return self.expected("a name or type", token.clone());
            }

            name = Some(Ident::new(token.to_string()));
            data_type = next_data_type;
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
    pub fn parse_drop_trigger(&mut self) -> Result<DropTrigger, ParserError> {
        if !dialect_of!(self is PostgreSqlDialect | SQLiteDialect | GenericDialect | MySqlDialect | MsSqlDialect)
        {
            self.prev_token();
            return self.expected("an object type after DROP", self.peek_token());
        }
        let if_exists = self.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
        let trigger_name = self.parse_object_name(false)?;
        let table_name = if self.parse_keyword(Keyword::ON) {
            Some(self.parse_object_name(false)?)
        } else {
            None
        };
        let option = match self.parse_one_of_keywords(&[Keyword::CASCADE, Keyword::RESTRICT]) {
            Some(Keyword::CASCADE) => Some(ReferentialAction::Cascade),
            Some(Keyword::RESTRICT) => Some(ReferentialAction::Restrict),
            Some(unexpected_keyword) => return Err(ParserError::ParserError(
                format!("Internal parser error: expected any of {{CASCADE, RESTRICT}}, got {unexpected_keyword:?}"),
            )),
            None => None,
        };
        Ok(DropTrigger {
            if_exists,
            trigger_name,
            table_name,
            option,
        })
    }

    /// Parse a `CREATE TRIGGER` statement.
    pub fn parse_create_trigger(
        &mut self,
        temporary: bool,
        or_alter: bool,
        or_replace: bool,
        is_constraint: bool,
    ) -> Result<CreateTrigger, ParserError> {
        if !dialect_of!(self is PostgreSqlDialect | SQLiteDialect | GenericDialect | MySqlDialect | MsSqlDialect)
        {
            self.prev_token();
            return self.expected("an object type after CREATE", self.peek_token());
        }

        let name = self.parse_object_name(false)?;
        let period = self.maybe_parse(|parser| parser.parse_trigger_period())?;

        let events = self.parse_keyword_separated(Keyword::OR, Parser::parse_trigger_event)?;
        self.expect_keyword_is(Keyword::ON)?;
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

        let trigger_object = if self.parse_keyword(Keyword::FOR) {
            let include_each = self.parse_keyword(Keyword::EACH);
            let trigger_object =
                match self.expect_one_of_keywords(&[Keyword::ROW, Keyword::STATEMENT])? {
                    Keyword::ROW => TriggerObject::Row,
                    Keyword::STATEMENT => TriggerObject::Statement,
                    unexpected_keyword => return Err(ParserError::ParserError(
                        format!("Internal parser error: unexpected keyword `{unexpected_keyword}` in ROW/STATEMENT"),
                    )),
                };

            Some(if include_each {
                TriggerObjectKind::ForEach(trigger_object)
            } else {
                TriggerObjectKind::For(trigger_object)
            })
        } else {
            let _ = self.parse_keyword(Keyword::FOR);

            None
        };

        let condition = self
            .parse_keyword(Keyword::WHEN)
            .then(|| self.parse_expr())
            .transpose()?;

        let mut exec_body = None;
        let mut statements = None;
        if self.parse_keyword(Keyword::EXECUTE) {
            exec_body = Some(self.parse_trigger_exec_body()?);
        } else {
            statements = Some(self.parse_conditional_statements(&[Keyword::END])?);
        }

        Ok(CreateTrigger {
            or_alter,
            temporary,
            or_replace,
            is_constraint,
            name,
            period,
            period_before_table: true,
            events,
            table_name,
            referenced_table_name,
            referencing,
            trigger_object,
            condition,
            exec_body,
            statements_as: false,
            statements,
            characteristics,
        })
    }

    /// Parse the period part of a trigger (`BEFORE`, `AFTER`, etc.).
    pub fn parse_trigger_period(&mut self) -> Result<TriggerPeriod, ParserError> {
        Ok(
            match self.expect_one_of_keywords(&[
                Keyword::FOR,
                Keyword::BEFORE,
                Keyword::AFTER,
                Keyword::INSTEAD,
            ])? {
                Keyword::FOR => TriggerPeriod::For,
                Keyword::BEFORE => TriggerPeriod::Before,
                Keyword::AFTER => TriggerPeriod::After,
                Keyword::INSTEAD => self
                    .expect_keyword_is(Keyword::OF)
                    .map(|_| TriggerPeriod::InsteadOf)?,
                unexpected_keyword => return Err(ParserError::ParserError(
                    format!("Internal parser error: unexpected keyword `{unexpected_keyword}` in trigger period"),
                )),
            },
        )
    }

    /// Parse the event part of a trigger (`INSERT`, `UPDATE`, etc.).
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
                        let cols = self.parse_comma_separated(Parser::parse_identifier)?;
                        TriggerEvent::Update(cols)
                    } else {
                        TriggerEvent::Update(vec![])
                    }
                }
                Keyword::DELETE => TriggerEvent::Delete,
                Keyword::TRUNCATE => TriggerEvent::Truncate,
                unexpected_keyword => return Err(ParserError::ParserError(
                    format!("Internal parser error: unexpected keyword `{unexpected_keyword}` in trigger event"),
                )),
            },
        )
    }

    /// Parse the `REFERENCING` clause of a trigger.
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

    /// Parse the execution body of a trigger (`FUNCTION` or `PROCEDURE`).
    pub fn parse_trigger_exec_body(&mut self) -> Result<TriggerExecBody, ParserError> {
        Ok(TriggerExecBody {
            exec_type: match self
                .expect_one_of_keywords(&[Keyword::FUNCTION, Keyword::PROCEDURE])?
            {
                Keyword::FUNCTION => TriggerExecBodyType::Function,
                Keyword::PROCEDURE => TriggerExecBodyType::Procedure,
                unexpected_keyword => return Err(ParserError::ParserError(
                    format!("Internal parser error: unexpected keyword `{unexpected_keyword}` in trigger exec body"),
                )),
            },
            func_desc: self.parse_function_desc()?,
        })
    }

    /// Parse a `CREATE MACRO` statement.
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
            self.expect_keyword_is(Keyword::AS)?;

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
        let name = self.parse_identifier()?;

        let default_expr =
            if self.consume_token(&Token::Assignment) || self.consume_token(&Token::RArrow) {
                Some(self.parse_expr()?)
            } else {
                None
            };
        Ok(MacroArg { name, default_expr })
    }

    /// Parse a `CREATE EXTERNAL TABLE` statement.
    pub fn parse_create_external_table(
        &mut self,
        or_replace: bool,
    ) -> Result<CreateTable, ParserError> {
        self.expect_keyword_is(Keyword::TABLE)?;
        let if_not_exists = self.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let table_name = self.parse_object_name(false)?;
        let (columns, constraints) = self.parse_columns()?;

        let hive_distribution = self.parse_hive_distribution()?;
        let hive_formats = self.parse_hive_formats()?;

        let file_format = if let Some(ref hf) = hive_formats {
            if let Some(ref ff) = hf.storage {
                match ff {
                    HiveIOFormat::FileFormat { format } => Some(*format),
                    _ => None,
                }
            } else {
                None
            }
        } else {
            None
        };
        let location = hive_formats.as_ref().and_then(|hf| hf.location.clone());
        let table_properties = self.parse_options(Keyword::TBLPROPERTIES)?;
        let table_options = if !table_properties.is_empty() {
            CreateTableOptions::TableProperties(table_properties)
        } else {
            CreateTableOptions::None
        };
        Ok(CreateTableBuilder::new(table_name)
            .columns(columns)
            .constraints(constraints)
            .hive_distribution(hive_distribution)
            .hive_formats(hive_formats)
            .table_options(table_options)
            .or_replace(or_replace)
            .if_not_exists(if_not_exists)
            .external(true)
            .file_format(file_format)
            .location(location)
            .build())
    }

    /// Parse a file format for external tables.
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

    fn parse_analyze_format_kind(&mut self) -> Result<AnalyzeFormatKind, ParserError> {
        if self.consume_token(&Token::Eq) {
            Ok(AnalyzeFormatKind::Assignment(self.parse_analyze_format()?))
        } else {
            Ok(AnalyzeFormatKind::Keyword(self.parse_analyze_format()?))
        }
    }

    /// Parse an `ANALYZE FORMAT`.
    pub fn parse_analyze_format(&mut self) -> Result<AnalyzeFormat, ParserError> {
        let next_token = self.next_token();
        match &next_token.token {
            Token::Word(w) => match w.keyword {
                Keyword::TEXT => Ok(AnalyzeFormat::TEXT),
                Keyword::GRAPHVIZ => Ok(AnalyzeFormat::GRAPHVIZ),
                Keyword::JSON => Ok(AnalyzeFormat::JSON),
                Keyword::TREE => Ok(AnalyzeFormat::TREE),
                _ => self.expected("fileformat", next_token),
            },
            _ => self.expected("fileformat", next_token),
        }
    }

    /// Parse a `CREATE VIEW` statement.
    pub fn parse_create_view(
        &mut self,
        or_alter: bool,
        or_replace: bool,
        temporary: bool,
        create_view_params: Option<CreateViewParams>,
    ) -> Result<CreateView, ParserError> {
        let secure = self.parse_keyword(Keyword::SECURE);
        let materialized = self.parse_keyword(Keyword::MATERIALIZED);
        self.expect_keyword_is(Keyword::VIEW)?;
        let allow_unquoted_hyphen = dialect_of!(self is BigQueryDialect);
        // Tries to parse IF NOT EXISTS either before name or after name
        // Name before IF NOT EXISTS is supported by snowflake but undocumented
        let if_not_exists_first =
            self.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let name = self.parse_object_name(allow_unquoted_hyphen)?;
        let name_before_not_exists = !if_not_exists_first
            && self.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let if_not_exists = if_not_exists_first || name_before_not_exists;
        // Many dialects support `OR ALTER` right after `CREATE`, but we don't (yet).
        // ANSI SQL and Postgres support RECURSIVE here, but we don't support it either.
        let columns = self.parse_view_columns()?;
        let mut options = CreateTableOptions::None;
        let with_options = self.parse_options(Keyword::WITH)?;
        if !with_options.is_empty() {
            options = CreateTableOptions::With(with_options);
        }

        let cluster_by = if self.parse_keyword(Keyword::CLUSTER) {
            self.expect_keyword_is(Keyword::BY)?;
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

        let comment = if self.dialect.supports_create_view_comment_syntax()
            && self.parse_keyword(Keyword::COMMENT)
        {
            self.expect_token(&Token::Eq)?;
            Some(self.parse_comment_value()?)
        } else {
            None
        };

        self.expect_keyword_is(Keyword::AS)?;
        let query = self.parse_query()?;
        // Optional `WITH [ CASCADED | LOCAL ] CHECK OPTION` is widely supported here.

        let with_no_schema_binding = dialect_of!(self is RedshiftSqlDialect | GenericDialect)
            && self.parse_keywords(&[
                Keyword::WITH,
                Keyword::NO,
                Keyword::SCHEMA,
                Keyword::BINDING,
            ]);

        Ok(CreateView {
            or_alter,
            name,
            columns,
            query,
            materialized,
            secure,
            or_replace,
            options,
            cluster_by,
            comment,
            with_no_schema_binding,
            if_not_exists,
            temporary,
            to,
            params: create_view_params,
            name_before_not_exists,
        })
    }

    /// Parse optional parameters for the `CREATE VIEW` statement supported by [MySQL].
    ///
    /// [MySQL]: https://dev.mysql.com/doc/refman/9.1/en/create-view.html
    fn parse_create_view_params(&mut self) -> Result<Option<CreateViewParams>, ParserError> {
        let algorithm = if self.parse_keyword(Keyword::ALGORITHM) {
            self.expect_token(&Token::Eq)?;
            Some(
                match self.expect_one_of_keywords(&[
                    Keyword::UNDEFINED,
                    Keyword::MERGE,
                    Keyword::TEMPTABLE,
                ])? {
                    Keyword::UNDEFINED => CreateViewAlgorithm::Undefined,
                    Keyword::MERGE => CreateViewAlgorithm::Merge,
                    Keyword::TEMPTABLE => CreateViewAlgorithm::TempTable,
                    _ => {
                        self.prev_token();
                        let found = self.next_token();
                        return self
                            .expected("UNDEFINED or MERGE or TEMPTABLE after ALGORITHM =", found);
                    }
                },
            )
        } else {
            None
        };
        let definer = if self.parse_keyword(Keyword::DEFINER) {
            self.expect_token(&Token::Eq)?;
            Some(self.parse_grantee_name()?)
        } else {
            None
        };
        let security = if self.parse_keywords(&[Keyword::SQL, Keyword::SECURITY]) {
            Some(
                match self.expect_one_of_keywords(&[Keyword::DEFINER, Keyword::INVOKER])? {
                    Keyword::DEFINER => CreateViewSecurity::Definer,
                    Keyword::INVOKER => CreateViewSecurity::Invoker,
                    _ => {
                        self.prev_token();
                        let found = self.next_token();
                        return self.expected("DEFINER or INVOKER after SQL SECURITY", found);
                    }
                },
            )
        } else {
            None
        };
        if algorithm.is_some() || definer.is_some() || security.is_some() {
            Ok(Some(CreateViewParams {
                algorithm,
                definer,
                security,
            }))
        } else {
            Ok(None)
        }
    }

    /// Parse a `CREATE ROLE` statement.
    pub fn parse_create_role(&mut self) -> Result<CreateRole, ParserError> {
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
                .map_or(Location { line: 0, column: 0 }, |t| t.span.start);
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
                    self.expect_keyword_is(Keyword::LIMIT)?;
                    if connection_limit.is_some() {
                        parser_err!("Found multiple CONNECTION LIMIT", loc)
                    } else {
                        connection_limit = Some(Expr::Value(self.parse_number_value()?));
                        Ok(())
                    }
                }
                Keyword::VALID => {
                    self.expect_keyword_is(Keyword::UNTIL)?;
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
                            in_role = self.parse_comma_separated(|p| p.parse_identifier())?;
                            Ok(())
                        }
                    } else if self.parse_keyword(Keyword::GROUP) {
                        if !in_group.is_empty() {
                            parser_err!("Found multiple IN GROUP", loc)
                        } else {
                            in_group = self.parse_comma_separated(|p| p.parse_identifier())?;
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
                        role = self.parse_comma_separated(|p| p.parse_identifier())?;
                        Ok(())
                    }
                }
                Keyword::USER => {
                    if !user.is_empty() {
                        parser_err!("Found multiple USER", loc)
                    } else {
                        user = self.parse_comma_separated(|p| p.parse_identifier())?;
                        Ok(())
                    }
                }
                Keyword::ADMIN => {
                    if !admin.is_empty() {
                        parser_err!("Found multiple ADMIN", loc)
                    } else {
                        admin = self.parse_comma_separated(|p| p.parse_identifier())?;
                        Ok(())
                    }
                }
                _ => break,
            }?
        }

        Ok(CreateRole {
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

    /// Parse an `OWNER` clause.
    pub fn parse_owner(&mut self) -> Result<Owner, ParserError> {
        let owner = match self.parse_one_of_keywords(&[Keyword::CURRENT_USER, Keyword::CURRENT_ROLE, Keyword::SESSION_USER]) {
            Some(Keyword::CURRENT_USER) => Owner::CurrentUser,
            Some(Keyword::CURRENT_ROLE) => Owner::CurrentRole,
            Some(Keyword::SESSION_USER) => Owner::SessionUser,
            Some(unexpected_keyword) => return Err(ParserError::ParserError(
                format!("Internal parser error: unexpected keyword `{unexpected_keyword}` in owner"),
            )),
            None => {
                match self.parse_identifier() {
                    Ok(ident) => Owner::Ident(ident),
                    Err(e) => {
                        return Err(ParserError::ParserError(format!("Expected: CURRENT_USER, CURRENT_ROLE, SESSION_USER or identifier after OWNER TO. {e}")))
                    }
                }
            }
        };
        Ok(owner)
    }

    /// Parses a [Statement::CreateDomain] statement.
    fn parse_create_domain(&mut self) -> Result<CreateDomain, ParserError> {
        let name = self.parse_object_name(false)?;
        self.expect_keyword_is(Keyword::AS)?;
        let data_type = self.parse_data_type()?;
        let collation = if self.parse_keyword(Keyword::COLLATE) {
            Some(self.parse_identifier()?)
        } else {
            None
        };
        let default = if self.parse_keyword(Keyword::DEFAULT) {
            Some(self.parse_expr()?)
        } else {
            None
        };
        let mut constraints = Vec::new();
        while let Some(constraint) = self.parse_optional_table_constraint()? {
            constraints.push(constraint);
        }

        Ok(CreateDomain {
            name,
            data_type,
            collation,
            default,
            constraints,
        })
    }

    /// ```sql
    ///     CREATE POLICY name ON table_name [ AS { PERMISSIVE | RESTRICTIVE } ]
    ///     [ FOR { ALL | SELECT | INSERT | UPDATE | DELETE } ]
    ///     [ TO { role_name | PUBLIC | CURRENT_USER | CURRENT_ROLE | SESSION_USER } [, ...] ]
    ///     [ USING ( using_expression ) ]
    ///     [ WITH CHECK ( with_check_expression ) ]
    /// ```
    ///
    /// [PostgreSQL Documentation](https://www.postgresql.org/docs/current/sql-createpolicy.html)
    pub fn parse_create_policy(&mut self) -> Result<Statement, ParserError> {
        let name = self.parse_identifier()?;
        self.expect_keyword_is(Keyword::ON)?;
        let table_name = self.parse_object_name(false)?;

        let policy_type = if self.parse_keyword(Keyword::AS) {
            let keyword =
                self.expect_one_of_keywords(&[Keyword::PERMISSIVE, Keyword::RESTRICTIVE])?;
            Some(match keyword {
                Keyword::PERMISSIVE => CreatePolicyType::Permissive,
                Keyword::RESTRICTIVE => CreatePolicyType::Restrictive,
                unexpected_keyword => return Err(ParserError::ParserError(
                    format!("Internal parser error: unexpected keyword `{unexpected_keyword}` in policy type"),
                )),
            })
        } else {
            None
        };

        let command = if self.parse_keyword(Keyword::FOR) {
            let keyword = self.expect_one_of_keywords(&[
                Keyword::ALL,
                Keyword::SELECT,
                Keyword::INSERT,
                Keyword::UPDATE,
                Keyword::DELETE,
            ])?;
            Some(match keyword {
                Keyword::ALL => CreatePolicyCommand::All,
                Keyword::SELECT => CreatePolicyCommand::Select,
                Keyword::INSERT => CreatePolicyCommand::Insert,
                Keyword::UPDATE => CreatePolicyCommand::Update,
                Keyword::DELETE => CreatePolicyCommand::Delete,
                unexpected_keyword => return Err(ParserError::ParserError(
                    format!("Internal parser error: unexpected keyword `{unexpected_keyword}` in policy command"),
                )),
            })
        } else {
            None
        };

        let to = if self.parse_keyword(Keyword::TO) {
            Some(self.parse_comma_separated(|p| p.parse_owner())?)
        } else {
            None
        };

        let using = if self.parse_keyword(Keyword::USING) {
            self.expect_token(&Token::LParen)?;
            let expr = self.parse_expr()?;
            self.expect_token(&Token::RParen)?;
            Some(expr)
        } else {
            None
        };

        let with_check = if self.parse_keywords(&[Keyword::WITH, Keyword::CHECK]) {
            self.expect_token(&Token::LParen)?;
            let expr = self.parse_expr()?;
            self.expect_token(&Token::RParen)?;
            Some(expr)
        } else {
            None
        };

        Ok(CreatePolicy {
            name,
            table_name,
            policy_type,
            command,
            to,
            using,
            with_check,
        })
    }

    /// ```sql
    /// CREATE CONNECTOR [IF NOT EXISTS] connector_name
    /// [TYPE datasource_type]
    /// [URL datasource_url]
    /// [COMMENT connector_comment]
    /// [WITH DCPROPERTIES(property_name=property_value, ...)]
    /// ```
    ///
    /// [Hive Documentation](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=27362034#LanguageManualDDL-CreateDataConnectorCreateConnector)
    pub fn parse_create_connector(&mut self) -> Result<CreateConnector, ParserError> {
        let if_not_exists = self.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let name = self.parse_identifier()?;

        let connector_type = if self.parse_keyword(Keyword::TYPE) {
            Some(self.parse_literal_string()?)
        } else {
            None
        };

        let url = if self.parse_keyword(Keyword::URL) {
            Some(self.parse_literal_string()?)
        } else {
            None
        };

        let comment = self.parse_optional_inline_comment()?;

        let with_dcproperties =
            match self.parse_options_with_keywords(&[Keyword::WITH, Keyword::DCPROPERTIES])? {
                properties if !properties.is_empty() => Some(properties),
                _ => None,
            };

        Ok(CreateConnector {
            name,
            if_not_exists,
            connector_type,
            url,
            comment,
            with_dcproperties,
        })
    }

    /// Parse an operator name, which can contain special characters like +, -, <, >, =
    /// that are tokenized as operator tokens rather than identifiers.
    /// This is used for PostgreSQL CREATE OPERATOR statements.
    ///
    /// Examples: `+`, `myschema.+`, `pg_catalog.<=`
    fn parse_operator_name(&mut self) -> Result<ObjectName, ParserError> {
        let mut parts = vec![];
        loop {
            parts.push(ObjectNamePart::Identifier(Ident::new(
                self.next_token().to_string(),
            )));
            if !self.consume_token(&Token::Period) {
                break;
            }
        }
        Ok(ObjectName(parts))
    }

    /// Parse a [Statement::CreateOperator]
    ///
    /// [PostgreSQL Documentation](https://www.postgresql.org/docs/current/sql-createoperator.html)
    pub fn parse_create_operator(&mut self) -> Result<CreateOperator, ParserError> {
        let name = self.parse_operator_name()?;
        self.expect_token(&Token::LParen)?;

        let mut function: Option<ObjectName> = None;
        let mut is_procedure = false;
        let mut left_arg: Option<DataType> = None;
        let mut right_arg: Option<DataType> = None;
        let mut options: Vec<OperatorOption> = Vec::new();

        loop {
            let keyword = self.expect_one_of_keywords(&[
                Keyword::FUNCTION,
                Keyword::PROCEDURE,
                Keyword::LEFTARG,
                Keyword::RIGHTARG,
                Keyword::COMMUTATOR,
                Keyword::NEGATOR,
                Keyword::RESTRICT,
                Keyword::JOIN,
                Keyword::HASHES,
                Keyword::MERGES,
            ])?;

            match keyword {
                Keyword::HASHES if !options.iter().any(|o| matches!(o, OperatorOption::Hashes)) => {
                    options.push(OperatorOption::Hashes);
                }
                Keyword::MERGES if !options.iter().any(|o| matches!(o, OperatorOption::Merges)) => {
                    options.push(OperatorOption::Merges);
                }
                Keyword::FUNCTION | Keyword::PROCEDURE if function.is_none() => {
                    self.expect_token(&Token::Eq)?;
                    function = Some(self.parse_object_name(false)?);
                    is_procedure = keyword == Keyword::PROCEDURE;
                }
                Keyword::LEFTARG if left_arg.is_none() => {
                    self.expect_token(&Token::Eq)?;
                    left_arg = Some(self.parse_data_type()?);
                }
                Keyword::RIGHTARG if right_arg.is_none() => {
                    self.expect_token(&Token::Eq)?;
                    right_arg = Some(self.parse_data_type()?);
                }
                Keyword::COMMUTATOR
                    if !options
                        .iter()
                        .any(|o| matches!(o, OperatorOption::Commutator(_))) =>
                {
                    self.expect_token(&Token::Eq)?;
                    if self.parse_keyword(Keyword::OPERATOR) {
                        self.expect_token(&Token::LParen)?;
                        let op = self.parse_operator_name()?;
                        self.expect_token(&Token::RParen)?;
                        options.push(OperatorOption::Commutator(op));
                    } else {
                        options.push(OperatorOption::Commutator(self.parse_operator_name()?));
                    }
                }
                Keyword::NEGATOR
                    if !options
                        .iter()
                        .any(|o| matches!(o, OperatorOption::Negator(_))) =>
                {
                    self.expect_token(&Token::Eq)?;
                    if self.parse_keyword(Keyword::OPERATOR) {
                        self.expect_token(&Token::LParen)?;
                        let op = self.parse_operator_name()?;
                        self.expect_token(&Token::RParen)?;
                        options.push(OperatorOption::Negator(op));
                    } else {
                        options.push(OperatorOption::Negator(self.parse_operator_name()?));
                    }
                }
                Keyword::RESTRICT
                    if !options
                        .iter()
                        .any(|o| matches!(o, OperatorOption::Restrict(_))) =>
                {
                    self.expect_token(&Token::Eq)?;
                    options.push(OperatorOption::Restrict(Some(
                        self.parse_object_name(false)?,
                    )));
                }
                Keyword::JOIN if !options.iter().any(|o| matches!(o, OperatorOption::Join(_))) => {
                    self.expect_token(&Token::Eq)?;
                    options.push(OperatorOption::Join(Some(self.parse_object_name(false)?)));
                }
                _ => {
                    return Err(ParserError::ParserError(format!(
                        "Duplicate or unexpected keyword {:?} in CREATE OPERATOR",
                        keyword
                    )))
                }
            }

            if !self.consume_token(&Token::Comma) {
                break;
            }
        }

        // Expect closing parenthesis
        self.expect_token(&Token::RParen)?;

        // FUNCTION is required
        let function = function.ok_or_else(|| {
            ParserError::ParserError("CREATE OPERATOR requires FUNCTION parameter".to_string())
        })?;

        Ok(CreateOperator {
            name,
            function,
            is_procedure,
            left_arg,
            right_arg,
            options,
        })
    }

    /// Parse a [Statement::CreateOperatorFamily]
    ///
    /// [PostgreSQL Documentation](https://www.postgresql.org/docs/current/sql-createopfamily.html)
    pub fn parse_create_operator_family(&mut self) -> Result<CreateOperatorFamily, ParserError> {
        let name = self.parse_object_name(false)?;
        self.expect_keyword(Keyword::USING)?;
        let using = self.parse_identifier()?;

        Ok(CreateOperatorFamily { name, using })
    }

    /// Parse a [Statement::CreateOperatorClass]
    ///
    /// [PostgreSQL Documentation](https://www.postgresql.org/docs/current/sql-createopclass.html)
    pub fn parse_create_operator_class(&mut self) -> Result<CreateOperatorClass, ParserError> {
        let name = self.parse_object_name(false)?;
        let default = self.parse_keyword(Keyword::DEFAULT);
        self.expect_keywords(&[Keyword::FOR, Keyword::TYPE])?;
        let for_type = self.parse_data_type()?;
        self.expect_keyword(Keyword::USING)?;
        let using = self.parse_identifier()?;

        let family = if self.parse_keyword(Keyword::FAMILY) {
            Some(self.parse_object_name(false)?)
        } else {
            None
        };

        self.expect_keyword(Keyword::AS)?;

        let mut items = vec![];
        loop {
            if self.parse_keyword(Keyword::OPERATOR) {
                let strategy_number = self.parse_literal_uint()?;
                let operator_name = self.parse_operator_name()?;

                // Optional operator argument types
                let op_types = if self.consume_token(&Token::LParen) {
                    let left = self.parse_data_type()?;
                    self.expect_token(&Token::Comma)?;
                    let right = self.parse_data_type()?;
                    self.expect_token(&Token::RParen)?;
                    Some(OperatorArgTypes { left, right })
                } else {
                    None
                };

                // Optional purpose
                let purpose = if self.parse_keyword(Keyword::FOR) {
                    if self.parse_keyword(Keyword::SEARCH) {
                        Some(OperatorPurpose::ForSearch)
                    } else if self.parse_keywords(&[Keyword::ORDER, Keyword::BY]) {
                        let sort_family = self.parse_object_name(false)?;
                        Some(OperatorPurpose::ForOrderBy { sort_family })
                    } else {
                        return self.expected("SEARCH or ORDER BY after FOR", self.peek_token());
                    }
                } else {
                    None
                };

                items.push(OperatorClassItem::Operator {
                    strategy_number,
                    operator_name,
                    op_types,
                    purpose,
                });
            } else if self.parse_keyword(Keyword::FUNCTION) {
                let support_number = self.parse_literal_uint()?;

                // Optional operator types
                let op_types =
                    if self.consume_token(&Token::LParen) && self.peek_token() != Token::RParen {
                        let mut types = vec![];
                        loop {
                            types.push(self.parse_data_type()?);
                            if !self.consume_token(&Token::Comma) {
                                break;
                            }
                        }
                        self.expect_token(&Token::RParen)?;
                        Some(types)
                    } else if self.consume_token(&Token::LParen) {
                        self.expect_token(&Token::RParen)?;
                        Some(vec![])
                    } else {
                        None
                    };

                let function_name = self.parse_object_name(false)?;

                // Function argument types
                let argument_types = if self.consume_token(&Token::LParen) {
                    let mut types = vec![];
                    loop {
                        if self.peek_token() == Token::RParen {
                            break;
                        }
                        types.push(self.parse_data_type()?);
                        if !self.consume_token(&Token::Comma) {
                            break;
                        }
                    }
                    self.expect_token(&Token::RParen)?;
                    types
                } else {
                    vec![]
                };

                items.push(OperatorClassItem::Function {
                    support_number,
                    op_types,
                    function_name,
                    argument_types,
                });
            } else if self.parse_keyword(Keyword::STORAGE) {
                let storage_type = self.parse_data_type()?;
                items.push(OperatorClassItem::Storage { storage_type });
            } else {
                break;
            }

            // Check for comma separator
            if !self.consume_token(&Token::Comma) {
                break;
            }
        }

        Ok(CreateOperatorClass {
            name,
            default,
            for_type,
            using,
            family,
            items,
        })
    }

    /// Parse a `DROP` statement.
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
        } else if self.parse_keywords(&[Keyword::MATERIALIZED, Keyword::VIEW]) {
            ObjectType::MaterializedView
        } else if self.parse_keyword(Keyword::INDEX) {
            ObjectType::Index
        } else if self.parse_keyword(Keyword::ROLE) {
            ObjectType::Role
        } else if self.parse_keyword(Keyword::SCHEMA) {
            ObjectType::Schema
        } else if self.parse_keyword(Keyword::DATABASE) {
            ObjectType::Database
        } else if self.parse_keyword(Keyword::SEQUENCE) {
            ObjectType::Sequence
        } else if self.parse_keyword(Keyword::STAGE) {
            ObjectType::Stage
        } else if self.parse_keyword(Keyword::TYPE) {
            ObjectType::Type
        } else if self.parse_keyword(Keyword::USER) {
            ObjectType::User
        } else if self.parse_keyword(Keyword::STREAM) {
            ObjectType::Stream
        } else if self.parse_keyword(Keyword::FUNCTION) {
            return self.parse_drop_function().map(Into::into);
        } else if self.parse_keyword(Keyword::POLICY) {
            return self.parse_drop_policy();
        } else if self.parse_keyword(Keyword::CONNECTOR) {
            return self.parse_drop_connector();
        } else if self.parse_keyword(Keyword::DOMAIN) {
            return self.parse_drop_domain().map(Into::into);
        } else if self.parse_keyword(Keyword::PROCEDURE) {
            return self.parse_drop_procedure();
        } else if self.parse_keyword(Keyword::SECRET) {
            return self.parse_drop_secret(temporary, persistent);
        } else if self.parse_keyword(Keyword::TRIGGER) {
            return self.parse_drop_trigger().map(Into::into);
        } else if self.parse_keyword(Keyword::EXTENSION) {
            return self.parse_drop_extension();
        } else if self.parse_keyword(Keyword::OPERATOR) {
            // Check if this is DROP OPERATOR FAMILY or DROP OPERATOR CLASS
            return if self.parse_keyword(Keyword::FAMILY) {
                self.parse_drop_operator_family()
            } else if self.parse_keyword(Keyword::CLASS) {
                self.parse_drop_operator_class()
            } else {
                self.parse_drop_operator()
            };
        } else {
            return self.expected(
                "CONNECTOR, DATABASE, EXTENSION, FUNCTION, INDEX, OPERATOR, POLICY, PROCEDURE, ROLE, SCHEMA, SECRET, SEQUENCE, STAGE, TABLE, TRIGGER, TYPE, VIEW, MATERIALIZED VIEW or USER after DROP",
                self.peek_token(),
            );
        };
        // Many dialects support the non-standard `IF EXISTS` clause and allow
        // specifying multiple objects to delete in a single statement
        let if_exists = self.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
        let names = self.parse_comma_separated(|p| p.parse_object_name(false))?;

        let loc = self.peek_token().span.start;
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
        let table = if self.parse_keyword(Keyword::ON) {
            Some(self.parse_object_name(false)?)
        } else {
            None
        };
        Ok(Statement::Drop {
            object_type,
            if_exists,
            names,
            cascade,
            restrict,
            purge,
            temporary,
            table,
        })
    }

    fn parse_optional_drop_behavior(&mut self) -> Option<DropBehavior> {
        match self.parse_one_of_keywords(&[Keyword::CASCADE, Keyword::RESTRICT]) {
            Some(Keyword::CASCADE) => Some(DropBehavior::Cascade),
            Some(Keyword::RESTRICT) => Some(DropBehavior::Restrict),
            _ => None,
        }
    }

    /// ```sql
    /// DROP FUNCTION [ IF EXISTS ] name [ ( [ [ argmode ] [ argname ] argtype [, ...] ] ) ] [, ...]
    /// [ CASCADE | RESTRICT ]
    /// ```
    fn parse_drop_function(&mut self) -> Result<DropFunction, ParserError> {
        let if_exists = self.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
        let func_desc = self.parse_comma_separated(Parser::parse_function_desc)?;
        let drop_behavior = self.parse_optional_drop_behavior();
        Ok(DropFunction {
            if_exists,
            func_desc,
            drop_behavior,
        })
    }

    /// ```sql
    /// DROP POLICY [ IF EXISTS ] name ON table_name [ CASCADE | RESTRICT ]
    /// ```
    ///
    /// [PostgreSQL Documentation](https://www.postgresql.org/docs/current/sql-droppolicy.html)
    fn parse_drop_policy(&mut self) -> Result<Statement, ParserError> {
        let if_exists = self.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
        let name = self.parse_identifier()?;
        self.expect_keyword_is(Keyword::ON)?;
        let table_name = self.parse_object_name(false)?;
        let drop_behavior = self.parse_optional_drop_behavior();
        Ok(Statement::DropPolicy {
            if_exists,
            name,
            table_name,
            drop_behavior,
        })
    }
    /// ```sql
    /// DROP CONNECTOR [IF EXISTS] name
    /// ```
    ///
    /// See [Hive](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=27362034#LanguageManualDDL-DropConnector)
    fn parse_drop_connector(&mut self) -> Result<Statement, ParserError> {
        let if_exists = self.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
        let name = self.parse_identifier()?;
        Ok(Statement::DropConnector { if_exists, name })
    }

    /// ```sql
    /// DROP DOMAIN [ IF EXISTS ] name [ CASCADE | RESTRICT ]
    /// ```
    fn parse_drop_domain(&mut self) -> Result<DropDomain, ParserError> {
        let if_exists = self.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
        let name = self.parse_object_name(false)?;
        let drop_behavior = self.parse_optional_drop_behavior();
        Ok(DropDomain {
            if_exists,
            name,
            drop_behavior,
        })
    }

    /// ```sql
    /// DROP PROCEDURE [ IF EXISTS ] name [ ( [ [ argmode ] [ argname ] argtype [, ...] ] ) ] [, ...]
    /// [ CASCADE | RESTRICT ]
    /// ```
    fn parse_drop_procedure(&mut self) -> Result<Statement, ParserError> {
        let if_exists = self.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
        let proc_desc = self.parse_comma_separated(Parser::parse_function_desc)?;
        let drop_behavior = self.parse_optional_drop_behavior();
        Ok(Statement::DropProcedure {
            if_exists,
            proc_desc,
            drop_behavior,
        })
    }

    fn parse_function_desc(&mut self) -> Result<FunctionDesc, ParserError> {
        let name = self.parse_object_name(false)?;

        let args = if self.consume_token(&Token::LParen) {
            if self.consume_token(&Token::RParen) {
                Some(vec![])
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
        let name = self.parse_identifier()?;
        let storage_specifier = if self.parse_keyword(Keyword::FROM) {
            self.parse_identifier().ok()
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

        let name = self.parse_identifier()?;

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

        self.expect_keyword_is(Keyword::CURSOR)?;
        let declare_type = Some(DeclareType::Cursor);

        let hold = match self.parse_one_of_keywords(&[Keyword::WITH, Keyword::WITHOUT]) {
            Some(keyword) => {
                self.expect_keyword_is(Keyword::HOLD)?;

                match keyword {
                    Keyword::WITH => Some(true),
                    Keyword::WITHOUT => Some(false),
                    unexpected_keyword => return Err(ParserError::ParserError(
                        format!("Internal parser error: unexpected keyword `{unexpected_keyword}` in cursor hold"),
                    )),
                }
            }
            None => None,
        };

        self.expect_keyword_is(Keyword::FOR)?;

        let query = Some(self.parse_query()?);

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
        let names = self.parse_comma_separated(Parser::parse_identifier)?;

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
            self.expect_keyword_is(Keyword::DEFAULT)?;
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
            let name = self.parse_identifier()?;
            let (declare_type, for_query, assigned_expr, data_type) =
                if self.parse_keyword(Keyword::CURSOR) {
                    self.expect_keyword_is(Keyword::FOR)?;
                    match self.peek_token().token {
                        Token::Word(w) if w.keyword == Keyword::SELECT => (
                            Some(DeclareType::Cursor),
                            Some(self.parse_query()?),
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
    //   | { @cursor_variable_name CURSOR [ FOR ] }
    // } [ ,...n ]
    /// ```
    /// [MsSql]: https://learn.microsoft.com/en-us/sql/t-sql/language-elements/declare-local-variable-transact-sql?view=sql-server-ver16
    pub fn parse_mssql_declare(&mut self) -> Result<Statement, ParserError> {
        let stmts = self.parse_comma_separated(Parser::parse_mssql_declare_stmt)?;

        Ok(Statement::Declare { stmts })
    }

    /// Parse the body of a [MsSql] `DECLARE`statement.
    ///
    /// Syntax:
    /// ```text
    // {
    //   { @local_variable [AS] data_type [ = value ] }
    //   | { @cursor_variable_name CURSOR [ FOR ]}
    // } [ ,...n ]
    /// ```
    /// [MsSql]: https://learn.microsoft.com/en-us/sql/t-sql/language-elements/declare-local-variable-transact-sql?view=sql-server-ver16
    pub fn parse_mssql_declare_stmt(&mut self) -> Result<Declare, ParserError> {
        let name = {
            let ident = self.parse_identifier()?;
            if !ident.value.starts_with('@')
                && !matches!(
                    self.peek_token().token,
                    Token::Word(w) if w.keyword == Keyword::CURSOR
                )
            {
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

        let (for_query, assignment) = if self.peek_keyword(Keyword::FOR) {
            self.next_token();
            let query = Some(self.parse_query()?);
            (query, None)
        } else {
            let assignment = self.parse_mssql_variable_declaration_expression()?;
            (None, assignment)
        };

        Ok(Declare {
            names: vec![name],
            data_type,
            assignment,
            declare_type,
            binary: None,
            sensitive: None,
            scroll: None,
            hold: None,
            for_query,
        })
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

    /// Parse `FETCH [direction] { FROM | IN } cursor INTO target;` statement.
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
                limit: self.parse_number_value()?.value,
            }
        } else if self.parse_keyword(Keyword::RELATIVE) {
            FetchDirection::Relative {
                limit: self.parse_number_value()?.value,
            }
        } else if self.parse_keyword(Keyword::FORWARD) {
            if self.parse_keyword(Keyword::ALL) {
                FetchDirection::ForwardAll
            } else {
                FetchDirection::Forward {
                    // TODO: Support optional
                    limit: Some(self.parse_number_value()?.value),
                }
            }
        } else if self.parse_keyword(Keyword::BACKWARD) {
            if self.parse_keyword(Keyword::ALL) {
                FetchDirection::BackwardAll
            } else {
                FetchDirection::Backward {
                    // TODO: Support optional
                    limit: Some(self.parse_number_value()?.value),
                }
            }
        } else if self.parse_keyword(Keyword::ALL) {
            FetchDirection::All
        } else {
            FetchDirection::Count {
                limit: self.parse_number_value()?.value,
            }
        };

        let position = if self.peek_keyword(Keyword::FROM) {
            self.expect_keyword(Keyword::FROM)?;
            FetchPosition::From
        } else if self.peek_keyword(Keyword::IN) {
            self.expect_keyword(Keyword::IN)?;
            FetchPosition::In
        } else {
            return parser_err!("Expected FROM or IN", self.peek_token().span.start);
        };

        let name = self.parse_identifier()?;

        let into = if self.parse_keyword(Keyword::INTO) {
            Some(self.parse_object_name(false)?)
        } else {
            None
        };

        Ok(Statement::Fetch {
            name,
            direction,
            position,
            into,
        })
    }

    /// Parse a `DISCARD` statement.
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

    /// Parse a `CREATE INDEX` statement.
    pub fn parse_create_index(&mut self, unique: bool) -> Result<CreateIndex, ParserError> {
        let concurrently = self.parse_keyword(Keyword::CONCURRENTLY);
        let if_not_exists = self.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);

        let mut using = None;

        let index_name = if if_not_exists || !self.parse_keyword(Keyword::ON) {
            let index_name = self.parse_object_name(false)?;
            // MySQL allows `USING index_type` either before or after `ON table_name`
            using = self.parse_optional_using_then_index_type()?;
            self.expect_keyword_is(Keyword::ON)?;
            Some(index_name)
        } else {
            None
        };

        let table_name = self.parse_object_name(false)?;

        // MySQL allows having two `USING` clauses.
        // In that case, the second clause overwrites the first.
        using = self.parse_optional_using_then_index_type()?.or(using);

        let columns = self.parse_parenthesized_index_column_list()?;

        let include = if self.parse_keyword(Keyword::INCLUDE) {
            self.expect_token(&Token::LParen)?;
            let columns = self.parse_comma_separated(|p| p.parse_identifier())?;
            self.expect_token(&Token::RParen)?;
            columns
        } else {
            vec![]
        };

        let nulls_distinct = if self.parse_keyword(Keyword::NULLS) {
            let not = self.parse_keyword(Keyword::NOT);
            self.expect_keyword_is(Keyword::DISTINCT)?;
            Some(!not)
        } else {
            None
        };

        let with = if self.dialect.supports_create_index_with_clause()
            && self.parse_keyword(Keyword::WITH)
        {
            self.expect_token(&Token::LParen)?;
            let with_params = self.parse_comma_separated(Parser::parse_expr)?;
            self.expect_token(&Token::RParen)?;
            with_params
        } else {
            Vec::new()
        };

        let predicate = if self.parse_keyword(Keyword::WHERE) {
            Some(self.parse_expr()?)
        } else {
            None
        };

        // MySQL options (including the modern style of `USING` after the column list instead of
        // before, which is deprecated) shouldn't conflict with other preceding options (e.g. `WITH
        // PARSER` won't be caught by the above `WITH` clause parsing because MySQL doesn't set that
        // support flag). This is probably invalid syntax for other dialects, but it is simpler to
        // parse it anyway (as we do inside `ALTER TABLE` and `CREATE TABLE` parsing).
        let index_options = self.parse_index_options()?;

        // MySQL allows `ALGORITHM` and `LOCK` options. Unlike in `ALTER TABLE`, they need not be comma separated.
        let mut alter_options = Vec::new();
        while self
            .peek_one_of_keywords(&[Keyword::ALGORITHM, Keyword::LOCK])
            .is_some()
        {
            alter_options.push(self.parse_alter_table_operation()?)
        }

        Ok(CreateIndex {
            name: index_name,
            table_name,
            using,
            columns,
            unique,
            concurrently,
            if_not_exists,
            include,
            nulls_distinct,
            with,
            predicate,
            index_options,
            alter_options,
        })
    }

    /// Parse a `CREATE EXTENSION` statement.
    pub fn parse_create_extension(&mut self) -> Result<CreateExtension, ParserError> {
        let if_not_exists = self.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let name = self.parse_identifier()?;

        let (schema, version, cascade) = if self.parse_keyword(Keyword::WITH) {
            let schema = if self.parse_keyword(Keyword::SCHEMA) {
                Some(self.parse_identifier()?)
            } else {
                None
            };

            let version = if self.parse_keyword(Keyword::VERSION) {
                Some(self.parse_identifier()?)
            } else {
                None
            };

            let cascade = self.parse_keyword(Keyword::CASCADE);

            (schema, version, cascade)
        } else {
            (None, None, false)
        };

        Ok(CreateExtension {
            name,
            if_not_exists,
            schema,
            version,
            cascade,
        })
    }

    /// Parse a PostgreSQL-specific [Statement::DropExtension] statement.
    pub fn parse_drop_extension(&mut self) -> Result<Statement, ParserError> {
        let if_exists = self.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
        let names = self.parse_comma_separated(|p| p.parse_identifier())?;
        let cascade_or_restrict =
            self.parse_one_of_keywords(&[Keyword::CASCADE, Keyword::RESTRICT]);
        Ok(Statement::DropExtension(DropExtension {
            names,
            if_exists,
            cascade_or_restrict: cascade_or_restrict
                .map(|k| match k {
                    Keyword::CASCADE => Ok(ReferentialAction::Cascade),
                    Keyword::RESTRICT => Ok(ReferentialAction::Restrict),
                    _ => self.expected("CASCADE or RESTRICT", self.peek_token()),
                })
                .transpose()?,
        }))
    }

    /// Parse a[Statement::DropOperator] statement.
    ///
    pub fn parse_drop_operator(&mut self) -> Result<Statement, ParserError> {
        let if_exists = self.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
        let operators = self.parse_comma_separated(|p| p.parse_drop_operator_signature())?;
        let drop_behavior = self.parse_optional_drop_behavior();
        Ok(Statement::DropOperator(DropOperator {
            if_exists,
            operators,
            drop_behavior,
        }))
    }

    /// Parse an operator signature for a [Statement::DropOperator]
    /// Format: `name ( { left_type | NONE } , right_type )`
    fn parse_drop_operator_signature(&mut self) -> Result<DropOperatorSignature, ParserError> {
        let name = self.parse_operator_name()?;
        self.expect_token(&Token::LParen)?;

        // Parse left operand type (or NONE for prefix operators)
        let left_type = if self.parse_keyword(Keyword::NONE) {
            None
        } else {
            Some(self.parse_data_type()?)
        };

        self.expect_token(&Token::Comma)?;

        // Parse right operand type (always required)
        let right_type = self.parse_data_type()?;

        self.expect_token(&Token::RParen)?;

        Ok(DropOperatorSignature {
            name,
            left_type,
            right_type,
        })
    }

    /// Parse a [Statement::DropOperatorFamily]
    ///
    /// [PostgreSQL Documentation](https://www.postgresql.org/docs/current/sql-dropopfamily.html)
    pub fn parse_drop_operator_family(&mut self) -> Result<Statement, ParserError> {
        let if_exists = self.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
        let names = self.parse_comma_separated(|p| p.parse_object_name(false))?;
        self.expect_keyword(Keyword::USING)?;
        let using = self.parse_identifier()?;
        let drop_behavior = self.parse_optional_drop_behavior();
        Ok(Statement::DropOperatorFamily(DropOperatorFamily {
            if_exists,
            names,
            using,
            drop_behavior,
        }))
    }

    /// Parse a [Statement::DropOperatorClass]
    ///
    /// [PostgreSQL Documentation](https://www.postgresql.org/docs/current/sql-dropopclass.html)
    pub fn parse_drop_operator_class(&mut self) -> Result<Statement, ParserError> {
        let if_exists = self.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
        let names = self.parse_comma_separated(|p| p.parse_object_name(false))?;
        self.expect_keyword(Keyword::USING)?;
        let using = self.parse_identifier()?;
        let drop_behavior = self.parse_optional_drop_behavior();
        Ok(Statement::DropOperatorClass(DropOperatorClass {
            if_exists,
            names,
            using,
            drop_behavior,
        }))
    }

    /// Parse Hive distribution style.
    ///
    /// TODO: Support parsing for `SKEWED` distribution style.
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

    /// Parse Hive formats.
    pub fn parse_hive_formats(&mut self) -> Result<Option<HiveFormat>, ParserError> {
        let mut hive_format: Option<HiveFormat> = None;
        loop {
            match self.parse_one_of_keywords(&[
                Keyword::ROW,
                Keyword::STORED,
                Keyword::LOCATION,
                Keyword::WITH,
            ]) {
                Some(Keyword::ROW) => {
                    hive_format
                        .get_or_insert_with(HiveFormat::default)
                        .row_format = Some(self.parse_row_format()?);
                }
                Some(Keyword::STORED) => {
                    self.expect_keyword_is(Keyword::AS)?;
                    if self.parse_keyword(Keyword::INPUTFORMAT) {
                        let input_format = self.parse_expr()?;
                        self.expect_keyword_is(Keyword::OUTPUTFORMAT)?;
                        let output_format = self.parse_expr()?;
                        hive_format.get_or_insert_with(HiveFormat::default).storage =
                            Some(HiveIOFormat::IOF {
                                input_format,
                                output_format,
                            });
                    } else {
                        let format = self.parse_file_format()?;
                        hive_format.get_or_insert_with(HiveFormat::default).storage =
                            Some(HiveIOFormat::FileFormat { format });
                    }
                }
                Some(Keyword::LOCATION) => {
                    hive_format.get_or_insert_with(HiveFormat::default).location =
                        Some(self.parse_literal_string()?);
                }
                Some(Keyword::WITH) => {
                    self.prev_token();
                    let properties = self
                        .parse_options_with_keywords(&[Keyword::WITH, Keyword::SERDEPROPERTIES])?;
                    if !properties.is_empty() {
                        hive_format
                            .get_or_insert_with(HiveFormat::default)
                            .serde_properties = Some(properties);
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

    /// Parse Hive row format.
    pub fn parse_row_format(&mut self) -> Result<HiveRowFormat, ParserError> {
        self.expect_keyword_is(Keyword::FORMAT)?;
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
                                    char: self.parse_identifier()?,
                                });

                                if self.parse_keywords(&[Keyword::ESCAPED, Keyword::BY]) {
                                    row_delimiters.push(HiveRowDelimiter {
                                        delimiter: HiveDelimiter::FieldsEscapedBy,
                                        char: self.parse_identifier()?,
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
                                    char: self.parse_identifier()?,
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
                                    char: self.parse_identifier()?,
                                });
                            } else {
                                break;
                            }
                        }
                        Some(Keyword::LINES) => {
                            if self.parse_keywords(&[Keyword::TERMINATED, Keyword::BY]) {
                                row_delimiters.push(HiveRowDelimiter {
                                    delimiter: HiveDelimiter::LinesTerminatedBy,
                                    char: self.parse_identifier()?,
                                });
                            } else {
                                break;
                            }
                        }
                        Some(Keyword::NULL) => {
                            if self.parse_keywords(&[Keyword::DEFINED, Keyword::AS]) {
                                row_delimiters.push(HiveRowDelimiter {
                                    delimiter: HiveDelimiter::NullDefinedAs,
                                    char: self.parse_identifier()?,
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
            Ok(Some(self.parse_identifier()?))
        } else {
            Ok(None)
        }
    }

    /// Parse `CREATE TABLE` statement.
    pub fn parse_create_table(
        &mut self,
        or_replace: bool,
        temporary: bool,
        global: Option<bool>,
        transient: bool,
    ) -> Result<CreateTable, ParserError> {
        let allow_unquoted_hyphen = dialect_of!(self is BigQueryDialect);
        let if_not_exists = self.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let table_name = self.parse_object_name(allow_unquoted_hyphen)?;

        // PostgreSQL PARTITION OF for child partition tables
        // Note: This is a PostgreSQL-specific feature, but the dialect check was intentionally
        // removed to allow GenericDialect and other dialects to parse this syntax. This enables
        // multi-dialect SQL tools to work with PostgreSQL-specific DDL statements.
        //
        // PARTITION OF can be combined with other table definition clauses in the AST,
        // though PostgreSQL itself prohibits PARTITION OF with AS SELECT or LIKE clauses.
        // The parser accepts these combinations for flexibility; semantic validation
        // is left to downstream tools.
        // Child partitions can have their own constraints and indexes.
        let partition_of = if self.parse_keywords(&[Keyword::PARTITION, Keyword::OF]) {
            Some(self.parse_object_name(allow_unquoted_hyphen)?)
        } else {
            None
        };

        // Clickhouse has `ON CLUSTER 'cluster'` syntax for DDLs
        let on_cluster = self.parse_optional_on_cluster()?;

        let like = self.maybe_parse_create_table_like(allow_unquoted_hyphen)?;

        let clone = if self.parse_keyword(Keyword::CLONE) {
            self.parse_object_name(allow_unquoted_hyphen).ok()
        } else {
            None
        };

        // parse optional column list (schema)
        let (columns, constraints) = self.parse_columns()?;
        let comment_after_column_def =
            if dialect_of!(self is HiveDialect) && self.parse_keyword(Keyword::COMMENT) {
                let next_token = self.next_token();
                match next_token.token {
                    Token::SingleQuotedString(str) => Some(CommentDef::WithoutEq(str)),
                    _ => self.expected("comment", next_token)?,
                }
            } else {
                None
            };

        // PostgreSQL PARTITION OF: partition bound specification
        let for_values = if partition_of.is_some() {
            if self.peek_keyword(Keyword::FOR) || self.peek_keyword(Keyword::DEFAULT) {
                Some(self.parse_partition_for_values()?)
            } else {
                return self.expected(
                    "FOR VALUES or DEFAULT after PARTITION OF",
                    self.peek_token(),
                );
            }
        } else {
            None
        };

        // SQLite supports `WITHOUT ROWID` at the end of `CREATE TABLE`
        let without_rowid = self.parse_keywords(&[Keyword::WITHOUT, Keyword::ROWID]);

        let hive_distribution = self.parse_hive_distribution()?;
        let clustered_by = self.parse_optional_clustered_by()?;
        let hive_formats = self.parse_hive_formats()?;

        let create_table_config = self.parse_optional_create_table_config()?;

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

        let on_commit = if self.parse_keywords(&[Keyword::ON, Keyword::COMMIT]) {
            Some(self.parse_create_table_on_commit()?)
        } else {
            None
        };

        let strict = self.parse_keyword(Keyword::STRICT);

        // Parse optional `AS ( query )`
        let query = if self.parse_keyword(Keyword::AS) {
            Some(self.parse_query()?)
        } else if self.dialect.supports_create_table_select() && self.parse_keyword(Keyword::SELECT)
        {
            // rewind the SELECT keyword
            self.prev_token();
            Some(self.parse_query()?)
        } else {
            None
        };

        Ok(CreateTableBuilder::new(table_name)
            .temporary(temporary)
            .columns(columns)
            .constraints(constraints)
            .or_replace(or_replace)
            .if_not_exists(if_not_exists)
            .transient(transient)
            .hive_distribution(hive_distribution)
            .hive_formats(hive_formats)
            .global(global)
            .query(query)
            .without_rowid(without_rowid)
            .like(like)
            .clone_clause(clone)
            .comment_after_column_def(comment_after_column_def)
            .order_by(order_by)
            .on_commit(on_commit)
            .on_cluster(on_cluster)
            .clustered_by(clustered_by)
            .partition_by(create_table_config.partition_by)
            .cluster_by(create_table_config.cluster_by)
            .inherits(create_table_config.inherits)
            .partition_of(partition_of)
            .for_values(for_values)
            .table_options(create_table_config.table_options)
            .primary_key(primary_key)
            .strict(strict)
            .build())
    }

    fn maybe_parse_create_table_like(
        &mut self,
        allow_unquoted_hyphen: bool,
    ) -> Result<Option<CreateTableLikeKind>, ParserError> {
        let like = if self.dialect.supports_create_table_like_parenthesized()
            && self.consume_token(&Token::LParen)
        {
            if self.parse_keyword(Keyword::LIKE) {
                let name = self.parse_object_name(allow_unquoted_hyphen)?;
                let defaults = if self.parse_keywords(&[Keyword::INCLUDING, Keyword::DEFAULTS]) {
                    Some(CreateTableLikeDefaults::Including)
                } else if self.parse_keywords(&[Keyword::EXCLUDING, Keyword::DEFAULTS]) {
                    Some(CreateTableLikeDefaults::Excluding)
                } else {
                    None
                };
                self.expect_token(&Token::RParen)?;
                Some(CreateTableLikeKind::Parenthesized(CreateTableLike {
                    name,
                    defaults,
                }))
            } else {
                // Rollback the '(' it's probably the columns list
                self.prev_token();
                None
            }
        } else if self.parse_keyword(Keyword::LIKE) || self.parse_keyword(Keyword::ILIKE) {
            let name = self.parse_object_name(allow_unquoted_hyphen)?;
            Some(CreateTableLikeKind::Plain(CreateTableLike {
                name,
                defaults: None,
            }))
        } else {
            None
        };
        Ok(like)
    }

    pub(crate) fn parse_create_table_on_commit(&mut self) -> Result<OnCommit, ParserError> {
        if self.parse_keywords(&[Keyword::DELETE, Keyword::ROWS]) {
            Ok(OnCommit::DeleteRows)
        } else if self.parse_keywords(&[Keyword::PRESERVE, Keyword::ROWS]) {
            Ok(OnCommit::PreserveRows)
        } else if self.parse_keywords(&[Keyword::DROP]) {
            Ok(OnCommit::Drop)
        } else {
            parser_err!(
                "Expecting DELETE ROWS, PRESERVE ROWS or DROP",
                self.peek_token()
            )
        }
    }

    /// Parse [ForValues] of a `PARTITION OF` clause.
    ///
    /// Parses: `FOR VALUES partition_bound_spec | DEFAULT`
    ///
    /// [PostgreSQL](https://www.postgresql.org/docs/current/sql-createtable.html)
    fn parse_partition_for_values(&mut self) -> Result<ForValues, ParserError> {
        if self.parse_keyword(Keyword::DEFAULT) {
            return Ok(ForValues::Default);
        }

        self.expect_keywords(&[Keyword::FOR, Keyword::VALUES])?;

        if self.parse_keyword(Keyword::IN) {
            // FOR VALUES IN (expr, ...)
            self.expect_token(&Token::LParen)?;
            if self.peek_token() == Token::RParen {
                return self.expected("at least one value", self.peek_token());
            }
            let values = self.parse_comma_separated(Parser::parse_expr)?;
            self.expect_token(&Token::RParen)?;
            Ok(ForValues::In(values))
        } else if self.parse_keyword(Keyword::FROM) {
            // FOR VALUES FROM (...) TO (...)
            self.expect_token(&Token::LParen)?;
            if self.peek_token() == Token::RParen {
                return self.expected("at least one value", self.peek_token());
            }
            let from = self.parse_comma_separated(Parser::parse_partition_bound_value)?;
            self.expect_token(&Token::RParen)?;
            self.expect_keyword(Keyword::TO)?;
            self.expect_token(&Token::LParen)?;
            if self.peek_token() == Token::RParen {
                return self.expected("at least one value", self.peek_token());
            }
            let to = self.parse_comma_separated(Parser::parse_partition_bound_value)?;
            self.expect_token(&Token::RParen)?;
            Ok(ForValues::From { from, to })
        } else if self.parse_keyword(Keyword::WITH) {
            // FOR VALUES WITH (MODULUS n, REMAINDER r)
            self.expect_token(&Token::LParen)?;
            self.expect_keyword(Keyword::MODULUS)?;
            let modulus = self.parse_literal_uint()?;
            self.expect_token(&Token::Comma)?;
            self.expect_keyword(Keyword::REMAINDER)?;
            let remainder = self.parse_literal_uint()?;
            self.expect_token(&Token::RParen)?;
            Ok(ForValues::With { modulus, remainder })
        } else {
            self.expected("IN, FROM, or WITH after FOR VALUES", self.peek_token())
        }
    }

    /// Parse a single [PartitionBoundValue].
    fn parse_partition_bound_value(&mut self) -> Result<PartitionBoundValue, ParserError> {
        if self.parse_keyword(Keyword::MINVALUE) {
            Ok(PartitionBoundValue::MinValue)
        } else if self.parse_keyword(Keyword::MAXVALUE) {
            Ok(PartitionBoundValue::MaxValue)
        } else {
            Ok(PartitionBoundValue::Expr(self.parse_expr()?))
        }
    }

    /// Parse configuration like inheritance, partitioning, clustering information during the table creation.
    ///
    /// [BigQuery](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#syntax_2)
    /// [PostgreSQL](https://www.postgresql.org/docs/current/ddl-partitioning.html)
    /// [MySql](https://dev.mysql.com/doc/refman/8.4/en/create-table.html)
    fn parse_optional_create_table_config(
        &mut self,
    ) -> Result<CreateTableConfiguration, ParserError> {
        let mut table_options = CreateTableOptions::None;

        let inherits = if self.parse_keyword(Keyword::INHERITS) {
            Some(self.parse_parenthesized_qualified_column_list(IsOptional::Mandatory, false)?)
        } else {
            None
        };

        // PostgreSQL supports `WITH ( options )`, before `AS`
        let with_options = self.parse_options(Keyword::WITH)?;
        if !with_options.is_empty() {
            table_options = CreateTableOptions::With(with_options)
        }

        let table_properties = self.parse_options(Keyword::TBLPROPERTIES)?;
        if !table_properties.is_empty() {
            table_options = CreateTableOptions::TableProperties(table_properties);
        }
        let partition_by = if dialect_of!(self is BigQueryDialect | PostgreSqlDialect | GenericDialect)
            && self.parse_keywords(&[Keyword::PARTITION, Keyword::BY])
        {
            Some(Box::new(self.parse_expr()?))
        } else {
            None
        };

        let mut cluster_by = None;
        if dialect_of!(self is BigQueryDialect | GenericDialect) {
            if self.parse_keywords(&[Keyword::CLUSTER, Keyword::BY]) {
                cluster_by = Some(WrappedCollection::NoWrapping(
                    self.parse_comma_separated(|p| p.parse_expr())?,
                ));
            };

            if let Token::Word(word) = self.peek_token().token {
                if word.keyword == Keyword::OPTIONS {
                    table_options =
                        CreateTableOptions::Options(self.parse_options(Keyword::OPTIONS)?)
                }
            };
        }

        if !dialect_of!(self is HiveDialect) && table_options == CreateTableOptions::None {
            let plain_options = self.parse_plain_options()?;
            if !plain_options.is_empty() {
                table_options = CreateTableOptions::Plain(plain_options)
            }
        };

        Ok(CreateTableConfiguration {
            partition_by,
            cluster_by,
            inherits,
            table_options,
        })
    }

    fn parse_plain_option(&mut self) -> Result<Option<SqlOption>, ParserError> {
        // Single parameter option
        // <https://dev.mysql.com/doc/refman/8.4/en/create-table.html>
        if self.parse_keywords(&[Keyword::START, Keyword::TRANSACTION]) {
            return Ok(Some(SqlOption::Ident(Ident::new("START TRANSACTION"))));
        }

        // Custom option
        // <https://dev.mysql.com/doc/refman/8.4/en/create-table.html>
        if self.parse_keywords(&[Keyword::COMMENT]) {
            let has_eq = self.consume_token(&Token::Eq);
            let value = self.next_token();

            let comment = match (has_eq, value.token) {
                (true, Token::SingleQuotedString(s)) => {
                    Ok(Some(SqlOption::Comment(CommentDef::WithEq(s))))
                }
                (false, Token::SingleQuotedString(s)) => {
                    Ok(Some(SqlOption::Comment(CommentDef::WithoutEq(s))))
                }
                (_, token) => {
                    self.expected("Token::SingleQuotedString", TokenWithSpan::wrap(token))
                }
            };
            return comment;
        }

        // <https://dev.mysql.com/doc/refman/8.4/en/create-table.html>
        // <https://clickhouse.com/docs/sql-reference/statements/create/table>
        if self.parse_keywords(&[Keyword::ENGINE]) {
            let _ = self.consume_token(&Token::Eq);
            let value = self.next_token();

            let engine = match value.token {
                Token::Word(w) => {
                    let parameters = if self.peek_token() == Token::LParen {
                        self.parse_parenthesized_identifiers()?
                    } else {
                        vec![]
                    };

                    Ok(Some(SqlOption::NamedParenthesizedList(
                        NamedParenthesizedList {
                            key: Ident::new("ENGINE"),
                            name: Some(Ident::new(w.value)),
                            values: parameters,
                        },
                    )))
                }
                _ => {
                    return self.expected("Token::Word", value)?;
                }
            };

            return engine;
        }

        // <https://dev.mysql.com/doc/refman/8.4/en/create-table.html>
        if self.parse_keywords(&[Keyword::TABLESPACE]) {
            let _ = self.consume_token(&Token::Eq);
            let value = self.next_token();

            let tablespace = match value.token {
                Token::Word(Word { value: name, .. }) | Token::SingleQuotedString(name) => {
                    let storage = match self.parse_keyword(Keyword::STORAGE) {
                        true => {
                            let _ = self.consume_token(&Token::Eq);
                            let storage_token = self.next_token();
                            match &storage_token.token {
                                Token::Word(w) => match w.value.to_uppercase().as_str() {
                                    "DISK" => Some(StorageType::Disk),
                                    "MEMORY" => Some(StorageType::Memory),
                                    _ => self
                                        .expected("Storage type (DISK or MEMORY)", storage_token)?,
                                },
                                _ => self.expected("Token::Word", storage_token)?,
                            }
                        }
                        false => None,
                    };

                    Ok(Some(SqlOption::TableSpace(TablespaceOption {
                        name,
                        storage,
                    })))
                }
                _ => {
                    return self.expected("Token::Word", value)?;
                }
            };

            return tablespace;
        }

        // <https://dev.mysql.com/doc/refman/8.4/en/create-table.html>
        if self.parse_keyword(Keyword::UNION) {
            let _ = self.consume_token(&Token::Eq);
            let value = self.next_token();

            match value.token {
                Token::LParen => {
                    let tables: Vec<Ident> =
                        self.parse_comma_separated0(Parser::parse_identifier, Token::RParen)?;
                    self.expect_token(&Token::RParen)?;

                    return Ok(Some(SqlOption::NamedParenthesizedList(
                        NamedParenthesizedList {
                            key: Ident::new("UNION"),
                            name: None,
                            values: tables,
                        },
                    )));
                }
                _ => {
                    return self.expected("Token::LParen", value)?;
                }
            }
        }

        // Key/Value parameter option
        let key = if self.parse_keywords(&[Keyword::DEFAULT, Keyword::CHARSET]) {
            Ident::new("DEFAULT CHARSET")
        } else if self.parse_keyword(Keyword::CHARSET) {
            Ident::new("CHARSET")
        } else if self.parse_keywords(&[Keyword::DEFAULT, Keyword::CHARACTER, Keyword::SET]) {
            Ident::new("DEFAULT CHARACTER SET")
        } else if self.parse_keywords(&[Keyword::CHARACTER, Keyword::SET]) {
            Ident::new("CHARACTER SET")
        } else if self.parse_keywords(&[Keyword::DEFAULT, Keyword::COLLATE]) {
            Ident::new("DEFAULT COLLATE")
        } else if self.parse_keyword(Keyword::COLLATE) {
            Ident::new("COLLATE")
        } else if self.parse_keywords(&[Keyword::DATA, Keyword::DIRECTORY]) {
            Ident::new("DATA DIRECTORY")
        } else if self.parse_keywords(&[Keyword::INDEX, Keyword::DIRECTORY]) {
            Ident::new("INDEX DIRECTORY")
        } else if self.parse_keyword(Keyword::KEY_BLOCK_SIZE) {
            Ident::new("KEY_BLOCK_SIZE")
        } else if self.parse_keyword(Keyword::ROW_FORMAT) {
            Ident::new("ROW_FORMAT")
        } else if self.parse_keyword(Keyword::PACK_KEYS) {
            Ident::new("PACK_KEYS")
        } else if self.parse_keyword(Keyword::STATS_AUTO_RECALC) {
            Ident::new("STATS_AUTO_RECALC")
        } else if self.parse_keyword(Keyword::STATS_PERSISTENT) {
            Ident::new("STATS_PERSISTENT")
        } else if self.parse_keyword(Keyword::STATS_SAMPLE_PAGES) {
            Ident::new("STATS_SAMPLE_PAGES")
        } else if self.parse_keyword(Keyword::DELAY_KEY_WRITE) {
            Ident::new("DELAY_KEY_WRITE")
        } else if self.parse_keyword(Keyword::COMPRESSION) {
            Ident::new("COMPRESSION")
        } else if self.parse_keyword(Keyword::ENCRYPTION) {
            Ident::new("ENCRYPTION")
        } else if self.parse_keyword(Keyword::MAX_ROWS) {
            Ident::new("MAX_ROWS")
        } else if self.parse_keyword(Keyword::MIN_ROWS) {
            Ident::new("MIN_ROWS")
        } else if self.parse_keyword(Keyword::AUTOEXTEND_SIZE) {
            Ident::new("AUTOEXTEND_SIZE")
        } else if self.parse_keyword(Keyword::AVG_ROW_LENGTH) {
            Ident::new("AVG_ROW_LENGTH")
        } else if self.parse_keyword(Keyword::CHECKSUM) {
            Ident::new("CHECKSUM")
        } else if self.parse_keyword(Keyword::CONNECTION) {
            Ident::new("CONNECTION")
        } else if self.parse_keyword(Keyword::ENGINE_ATTRIBUTE) {
            Ident::new("ENGINE_ATTRIBUTE")
        } else if self.parse_keyword(Keyword::PASSWORD) {
            Ident::new("PASSWORD")
        } else if self.parse_keyword(Keyword::SECONDARY_ENGINE_ATTRIBUTE) {
            Ident::new("SECONDARY_ENGINE_ATTRIBUTE")
        } else if self.parse_keyword(Keyword::INSERT_METHOD) {
            Ident::new("INSERT_METHOD")
        } else if self.parse_keyword(Keyword::AUTO_INCREMENT) {
            Ident::new("AUTO_INCREMENT")
        } else {
            return Ok(None);
        };

        let _ = self.consume_token(&Token::Eq);

        let value = match self
            .maybe_parse(|parser| parser.parse_value())?
            .map(Expr::Value)
        {
            Some(expr) => expr,
            None => Expr::Identifier(self.parse_identifier()?),
        };

        Ok(Some(SqlOption::KeyValue { key, value }))
    }

    /// Parse plain options.
    pub fn parse_plain_options(&mut self) -> Result<Vec<SqlOption>, ParserError> {
        let mut options = Vec::new();

        while let Some(option) = self.parse_plain_option()? {
            options.push(option);
            // Some dialects support comma-separated options; it shouldn't introduce ambiguity to
            // consume it for all dialects.
            let _ = self.consume_token(&Token::Comma);
        }

        Ok(options)
    }

    /// Parse optional inline comment.
    pub fn parse_optional_inline_comment(&mut self) -> Result<Option<CommentDef>, ParserError> {
        let comment = if self.parse_keyword(Keyword::COMMENT) {
            let has_eq = self.consume_token(&Token::Eq);
            let comment = self.parse_comment_value()?;
            Some(if has_eq {
                CommentDef::WithEq(comment)
            } else {
                CommentDef::WithoutEq(comment)
            })
        } else {
            None
        };
        Ok(comment)
    }

    /// Parse comment value.
    pub fn parse_comment_value(&mut self) -> Result<String, ParserError> {
        let next_token = self.next_token();
        let value = match next_token.token {
            Token::SingleQuotedString(str) => str,
            Token::DollarQuotedString(str) => str.value,
            _ => self.expected("string literal", next_token)?,
        };
        Ok(value)
    }

    /// Parse optional procedure parameters.
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

    /// Parse columns and constraints.
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

            if rparen
                && (!comma
                    || self.dialect.supports_column_definition_trailing_commas()
                    || self.options.trailing_commas)
            {
                let _ = self.consume_token(&Token::RParen);
                break;
            }
        }

        Ok((columns, constraints))
    }

    /// Parse procedure parameter.
    pub fn parse_procedure_param(&mut self) -> Result<ProcedureParam, ParserError> {
        let mode = if self.parse_keyword(Keyword::IN) {
            Some(ArgMode::In)
        } else if self.parse_keyword(Keyword::OUT) {
            Some(ArgMode::Out)
        } else if self.parse_keyword(Keyword::INOUT) {
            Some(ArgMode::InOut)
        } else {
            None
        };
        let name = self.parse_identifier()?;
        let data_type = self.parse_data_type()?;
        let default = if self.consume_token(&Token::Eq) {
            Some(self.parse_expr()?)
        } else {
            None
        };

        Ok(ProcedureParam {
            name,
            data_type,
            mode,
            default,
        })
    }

    /// Parse column definition.
    pub fn parse_column_def(&mut self) -> Result<ColumnDef, ParserError> {
        let col_name = self.parse_identifier()?;
        let data_type = if self.is_column_type_sqlite_unspecified() {
            DataType::Unspecified
        } else {
            self.parse_data_type()?
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
            name: col_name,
            data_type,
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

    /// Parse optional column option.
    pub fn parse_optional_column_option(&mut self) -> Result<Option<ColumnOption>, ParserError> {
        if let Some(option) = self.dialect.parse_column_option(self)? {
            return option;
        }

        self.with_state(
            ColumnDefinition,
            |parser| -> Result<Option<ColumnOption>, ParserError> {
                parser.parse_optional_column_option_inner()
            },
        )
    }

    fn parse_optional_column_option_inner(&mut self) -> Result<Option<ColumnOption>, ParserError> {
        if self.parse_keywords(&[Keyword::CHARACTER, Keyword::SET]) {
            Ok(Some(ColumnOption::CharacterSet(
                self.parse_object_name(false)?,
            )))
        } else if self.parse_keywords(&[Keyword::COLLATE]) {
            Ok(Some(ColumnOption::Collation(
                self.parse_object_name(false)?,
            )))
        } else if self.parse_keywords(&[Keyword::NOT, Keyword::NULL]) {
            Ok(Some(ColumnOption::NotNull))
        } else if self.parse_keywords(&[Keyword::COMMENT]) {
            Ok(Some(ColumnOption::Comment(self.parse_comment_value()?)))
        } else if self.parse_keyword(Keyword::NULL) {
            Ok(Some(ColumnOption::Null))
        } else if self.parse_keyword(Keyword::DEFAULT) {
            Ok(Some(ColumnOption::Default(
                self.parse_column_option_expr()?,
            )))
        } else if dialect_of!(self is ClickHouseDialect| GenericDialect)
            && self.parse_keyword(Keyword::MATERIALIZED)
        {
            Ok(Some(ColumnOption::Materialized(
                self.parse_column_option_expr()?,
            )))
        } else if dialect_of!(self is ClickHouseDialect| GenericDialect)
            && self.parse_keyword(Keyword::ALIAS)
        {
            Ok(Some(ColumnOption::Alias(self.parse_column_option_expr()?)))
        } else if dialect_of!(self is ClickHouseDialect| GenericDialect)
            && self.parse_keyword(Keyword::EPHEMERAL)
        {
            // The expression is optional for the EPHEMERAL syntax, so we need to check
            // if the column definition has remaining tokens before parsing the expression.
            if matches!(self.peek_token().token, Token::Comma | Token::RParen) {
                Ok(Some(ColumnOption::Ephemeral(None)))
            } else {
                Ok(Some(ColumnOption::Ephemeral(Some(
                    self.parse_column_option_expr()?,
                ))))
            }
        } else if self.parse_keywords(&[Keyword::PRIMARY, Keyword::KEY]) {
            let characteristics = self.parse_constraint_characteristics()?;
            Ok(Some(
                PrimaryKeyConstraint {
                    name: None,
                    index_name: None,
                    index_type: None,
                    columns: vec![],
                    index_options: vec![],
                    characteristics,
                }
                .into(),
            ))
        } else if self.parse_keyword(Keyword::UNIQUE) {
            let characteristics = self.parse_constraint_characteristics()?;
            Ok(Some(
                UniqueConstraint {
                    name: None,
                    index_name: None,
                    index_type_display: KeyOrIndexDisplay::None,
                    index_type: None,
                    columns: vec![],
                    index_options: vec![],
                    characteristics,
                    nulls_distinct: NullsDistinctOption::None,
                }
                .into(),
            ))
        } else if self.parse_keyword(Keyword::REFERENCES) {
            let foreign_table = self.parse_object_name(false)?;
            // PostgreSQL allows omitting the column list and
            // uses the primary key column of the foreign table by default
            let referred_columns = self.parse_parenthesized_column_list(Optional, false)?;
            let mut match_kind = None;
            let mut on_delete = None;
            let mut on_update = None;
            loop {
                if match_kind.is_none() && self.parse_keyword(Keyword::MATCH) {
                    match_kind = Some(self.parse_match_kind()?);
                } else if on_delete.is_none()
                    && self.parse_keywords(&[Keyword::ON, Keyword::DELETE])
                {
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

            Ok(Some(
                ForeignKeyConstraint {
                    name: None,       // Column-level constraints don't have names
                    index_name: None, // Not applicable for column-level constraints
                    columns: vec![],  // Not applicable for column-level constraints
                    foreign_table,
                    referred_columns,
                    on_delete,
                    on_update,
                    match_kind,
                    characteristics,
                }
                .into(),
            ))
        } else if self.parse_keyword(Keyword::CHECK) {
            self.expect_token(&Token::LParen)?;
            // since `CHECK` requires parentheses, we can parse the inner expression in ParserState::Normal
            let expr: Expr = self.with_state(ParserState::Normal, |p| p.parse_expr())?;
            self.expect_token(&Token::RParen)?;
            Ok(Some(
                CheckConstraint {
                    name: None, // Column-level check constraints don't have names
                    expr: Box::new(expr),
                    enforced: None, // Could be extended later to support MySQL ENFORCED/NOT ENFORCED
                }
                .into(),
            ))
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
        } else if self.parse_keyword(Keyword::ASC)
            && self.dialect.supports_asc_desc_in_column_definition()
        {
            // Support ASC for SQLite
            Ok(Some(ColumnOption::DialectSpecific(vec![
                Token::make_keyword("ASC"),
            ])))
        } else if self.parse_keyword(Keyword::DESC)
            && self.dialect.supports_asc_desc_in_column_definition()
        {
            // Support DESC for SQLite
            Ok(Some(ColumnOption::DialectSpecific(vec![
                Token::make_keyword("DESC"),
            ])))
        } else if self.parse_keywords(&[Keyword::ON, Keyword::UPDATE])
            && dialect_of!(self is MySqlDialect | GenericDialect)
        {
            let expr = self.parse_column_option_expr()?;
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
        } else if self.parse_keyword(Keyword::SRID)
            && dialect_of!(self is MySqlDialect | GenericDialect)
        {
            Ok(Some(ColumnOption::Srid(Box::new(
                self.parse_column_option_expr()?,
            ))))
        } else if self.parse_keyword(Keyword::IDENTITY)
            && dialect_of!(self is MsSqlDialect | GenericDialect)
        {
            let parameters = if self.consume_token(&Token::LParen) {
                let seed = self.parse_number()?;
                self.expect_token(&Token::Comma)?;
                let increment = self.parse_number()?;
                self.expect_token(&Token::RParen)?;

                Some(IdentityPropertyFormatKind::FunctionCall(
                    IdentityParameters { seed, increment },
                ))
            } else {
                None
            };
            Ok(Some(ColumnOption::Identity(
                IdentityPropertyKind::Identity(IdentityProperty {
                    parameters,
                    order: None,
                }),
            )))
        } else if dialect_of!(self is SQLiteDialect | GenericDialect)
            && self.parse_keywords(&[Keyword::ON, Keyword::CONFLICT])
        {
            // Support ON CONFLICT for SQLite
            Ok(Some(ColumnOption::OnConflict(
                self.expect_one_of_keywords(&[
                    Keyword::ROLLBACK,
                    Keyword::ABORT,
                    Keyword::FAIL,
                    Keyword::IGNORE,
                    Keyword::REPLACE,
                ])?,
            )))
        } else if self.parse_keyword(Keyword::INVISIBLE) {
            Ok(Some(ColumnOption::Invisible))
        } else {
            Ok(None)
        }
    }

    /// When parsing some column option expressions we need to revert to [ParserState::Normal] since
    /// `NOT NULL` is allowed as an alias for `IS NOT NULL`.
    /// In those cases we use this helper instead of calling [Parser::parse_expr] directly.
    ///
    /// For example, consider these `CREATE TABLE` statements:
    /// ```sql
    /// CREATE TABLE foo (abc BOOL DEFAULT (42 NOT NULL) NOT NULL);
    /// ```
    /// vs
    /// ```sql
    /// CREATE TABLE foo (abc BOOL NOT NULL);
    /// ```
    ///
    /// In the first we should parse the inner portion of `(42 NOT NULL)` as [Expr::IsNotNull],
    /// whereas is both statements that trailing `NOT NULL` should only be parsed as a
    /// [ColumnOption::NotNull].
    fn parse_column_option_expr(&mut self) -> Result<Expr, ParserError> {
        if self.peek_token_ref().token == Token::LParen {
            let expr: Expr = self.with_state(ParserState::Normal, |p| p.parse_prefix())?;
            Ok(expr)
        } else {
            Ok(self.parse_expr()?)
        }
    }

    pub(crate) fn parse_tag(&mut self) -> Result<Tag, ParserError> {
        let name = self.parse_object_name(false)?;
        self.expect_token(&Token::Eq)?;
        let value = self.parse_literal_string()?;

        Ok(Tag::new(name, value))
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
                let expr: Expr = self.with_state(ParserState::Normal, |p| p.parse_expr())?;
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

    /// Parse optional `CLUSTERED BY` clause for Hive/Generic dialects.
    pub fn parse_optional_clustered_by(&mut self) -> Result<Option<ClusteredBy>, ParserError> {
        let clustered_by = if dialect_of!(self is HiveDialect|GenericDialect)
            && self.parse_keywords(&[Keyword::CLUSTERED, Keyword::BY])
        {
            let columns = self.parse_parenthesized_column_list(Mandatory, false)?;

            let sorted_by = if self.parse_keywords(&[Keyword::SORTED, Keyword::BY]) {
                self.expect_token(&Token::LParen)?;
                let sorted_by_columns = self.parse_comma_separated(|p| p.parse_order_by_expr())?;
                self.expect_token(&Token::RParen)?;
                Some(sorted_by_columns)
            } else {
                None
            };

            self.expect_keyword_is(Keyword::INTO)?;
            let num_buckets = self.parse_number_value()?.value;
            self.expect_keyword_is(Keyword::BUCKETS)?;
            Some(ClusteredBy {
                columns,
                sorted_by,
                num_buckets,
            })
        } else {
            None
        };
        Ok(clustered_by)
    }

    /// Parse a referential action used in foreign key clauses.
    ///
    /// Recognized forms: `RESTRICT`, `CASCADE`, `SET NULL`, `NO ACTION`, `SET DEFAULT`.
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

    /// Parse a `MATCH` kind for constraint references: `FULL`, `PARTIAL`, or `SIMPLE`.
    pub fn parse_match_kind(&mut self) -> Result<ConstraintReferenceMatchKind, ParserError> {
        if self.parse_keyword(Keyword::FULL) {
            Ok(ConstraintReferenceMatchKind::Full)
        } else if self.parse_keyword(Keyword::PARTIAL) {
            Ok(ConstraintReferenceMatchKind::Partial)
        } else if self.parse_keyword(Keyword::SIMPLE) {
            Ok(ConstraintReferenceMatchKind::Simple)
        } else {
            self.expected("one of FULL, PARTIAL or SIMPLE", self.peek_token())
        }
    }

    /// Parse optional constraint characteristics such as `DEFERRABLE`, `INITIALLY` and `ENFORCED`.
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

    /// Parse an optional table constraint (e.g. `PRIMARY KEY`, `UNIQUE`, `FOREIGN KEY`, `CHECK`).
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
            Token::Word(w) if w.keyword == Keyword::UNIQUE => {
                let index_type_display = self.parse_index_type_display();
                if !dialect_of!(self is GenericDialect | MySqlDialect)
                    && !index_type_display.is_none()
                {
                    return self
                        .expected("`index_name` or `(column_name [, ...])`", self.peek_token());
                }

                let nulls_distinct = self.parse_optional_nulls_distinct()?;

                // optional index name
                let index_name = self.parse_optional_ident()?;
                let index_type = self.parse_optional_using_then_index_type()?;

                let columns = self.parse_parenthesized_index_column_list()?;
                let index_options = self.parse_index_options()?;
                let characteristics = self.parse_constraint_characteristics()?;
                Ok(Some(
                    UniqueConstraint {
                        name,
                        index_name,
                        index_type_display,
                        index_type,
                        columns,
                        index_options,
                        characteristics,
                        nulls_distinct,
                    }
                    .into(),
                ))
            }
            Token::Word(w) if w.keyword == Keyword::PRIMARY => {
                // after `PRIMARY` always stay `KEY`
                self.expect_keyword_is(Keyword::KEY)?;

                // optional index name
                let index_name = self.parse_optional_ident()?;
                let index_type = self.parse_optional_using_then_index_type()?;

                let columns = self.parse_parenthesized_index_column_list()?;
                let index_options = self.parse_index_options()?;
                let characteristics = self.parse_constraint_characteristics()?;
                Ok(Some(
                    PrimaryKeyConstraint {
                        name,
                        index_name,
                        index_type,
                        columns,
                        index_options,
                        characteristics,
                    }
                    .into(),
                ))
            }
            Token::Word(w) if w.keyword == Keyword::FOREIGN => {
                self.expect_keyword_is(Keyword::KEY)?;
                let index_name = self.parse_optional_ident()?;
                let columns = self.parse_parenthesized_column_list(Mandatory, false)?;
                self.expect_keyword_is(Keyword::REFERENCES)?;
                let foreign_table = self.parse_object_name(false)?;
                let referred_columns = self.parse_parenthesized_column_list(Optional, false)?;
                let mut match_kind = None;
                let mut on_delete = None;
                let mut on_update = None;
                loop {
                    if match_kind.is_none() && self.parse_keyword(Keyword::MATCH) {
                        match_kind = Some(self.parse_match_kind()?);
                    } else if on_delete.is_none()
                        && self.parse_keywords(&[Keyword::ON, Keyword::DELETE])
                    {
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

                Ok(Some(
                    ForeignKeyConstraint {
                        name,
                        index_name,
                        columns,
                        foreign_table,
                        referred_columns,
                        on_delete,
                        on_update,
                        match_kind,
                        characteristics,
                    }
                    .into(),
                ))
            }
            Token::Word(w) if w.keyword == Keyword::CHECK => {
                self.expect_token(&Token::LParen)?;
                let expr = Box::new(self.parse_expr()?);
                self.expect_token(&Token::RParen)?;

                let enforced = if self.parse_keyword(Keyword::ENFORCED) {
                    Some(true)
                } else if self.parse_keywords(&[Keyword::NOT, Keyword::ENFORCED]) {
                    Some(false)
                } else {
                    None
                };

                Ok(Some(
                    CheckConstraint {
                        name,
                        expr,
                        enforced,
                    }
                    .into(),
                ))
            }
            Token::Word(w)
                if (w.keyword == Keyword::INDEX || w.keyword == Keyword::KEY)
                    && dialect_of!(self is GenericDialect | MySqlDialect)
                    && name.is_none() =>
            {
                let display_as_key = w.keyword == Keyword::KEY;

                let name = match self.peek_token().token {
                    Token::Word(word) if word.keyword == Keyword::USING => None,
                    _ => self.parse_optional_ident()?,
                };

                let index_type = self.parse_optional_using_then_index_type()?;
                let columns = self.parse_parenthesized_index_column_list()?;
                let index_options = self.parse_index_options()?;

                Ok(Some(
                    IndexConstraint {
                        display_as_key,
                        name,
                        index_type,
                        columns,
                        index_options,
                    }
                    .into(),
                ))
            }
            Token::Word(w)
                if (w.keyword == Keyword::FULLTEXT || w.keyword == Keyword::SPATIAL)
                    && dialect_of!(self is GenericDialect | MySqlDialect) =>
            {
                if let Some(name) = name {
                    return self.expected(
                        "FULLTEXT or SPATIAL option without constraint name",
                        TokenWithSpan {
                            token: Token::make_keyword(&name.to_string()),
                            span: next_token.span,
                        },
                    );
                }

                let fulltext = w.keyword == Keyword::FULLTEXT;

                let index_type_display = self.parse_index_type_display();

                let opt_index_name = self.parse_optional_ident()?;

                let columns = self.parse_parenthesized_index_column_list()?;

                Ok(Some(
                    FullTextOrSpatialConstraint {
                        fulltext,
                        index_type_display,
                        opt_index_name,
                        columns,
                    }
                    .into(),
                ))
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

    fn parse_optional_nulls_distinct(&mut self) -> Result<NullsDistinctOption, ParserError> {
        Ok(if self.parse_keyword(Keyword::NULLS) {
            let not = self.parse_keyword(Keyword::NOT);
            self.expect_keyword_is(Keyword::DISTINCT)?;
            if not {
                NullsDistinctOption::NotDistinct
            } else {
                NullsDistinctOption::Distinct
            }
        } else {
            NullsDistinctOption::None
        })
    }

    /// Optionally parse a parenthesized list of `SqlOption`s introduced by `keyword`.
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

    /// Parse a parenthesized list of `SqlOption`s following `keyword`, or return an empty vec.
    pub fn parse_options(&mut self, keyword: Keyword) -> Result<Vec<SqlOption>, ParserError> {
        if self.parse_keyword(keyword) {
            self.expect_token(&Token::LParen)?;
            let options = self.parse_comma_separated0(Parser::parse_sql_option, Token::RParen)?;
            self.expect_token(&Token::RParen)?;
            Ok(options)
        } else {
            Ok(vec![])
        }
    }

    /// Parse options introduced by one of `keywords` followed by a parenthesized list.
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

    /// Parse an index type token (e.g. `BTREE`, `HASH`, or a custom identifier).
    pub fn parse_index_type(&mut self) -> Result<IndexType, ParserError> {
        Ok(if self.parse_keyword(Keyword::BTREE) {
            IndexType::BTree
        } else if self.parse_keyword(Keyword::HASH) {
            IndexType::Hash
        } else if self.parse_keyword(Keyword::GIN) {
            IndexType::GIN
        } else if self.parse_keyword(Keyword::GIST) {
            IndexType::GiST
        } else if self.parse_keyword(Keyword::SPGIST) {
            IndexType::SPGiST
        } else if self.parse_keyword(Keyword::BRIN) {
            IndexType::BRIN
        } else if self.parse_keyword(Keyword::BLOOM) {
            IndexType::Bloom
        } else {
            IndexType::Custom(self.parse_identifier()?)
        })
    }

    /// Optionally parse the `USING` keyword, followed by an [IndexType]
    /// Example:
    /// ```sql
    //// USING BTREE (name, age DESC)
    /// ```
    /// Optionally parse `USING <index_type>` and return the parsed `IndexType` if present.
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
    /// Parse an optional identifier, returning `Some(Ident)` if present.
    pub fn parse_optional_ident(&mut self) -> Result<Option<Ident>, ParserError> {
        self.maybe_parse(|parser| parser.parse_identifier())
    }

    #[must_use]
    /// Parse optional `KEY` or `INDEX` display tokens used in index/constraint declarations.
    pub fn parse_index_type_display(&mut self) -> KeyOrIndexDisplay {
        if self.parse_keyword(Keyword::KEY) {
            KeyOrIndexDisplay::Key
        } else if self.parse_keyword(Keyword::INDEX) {
            KeyOrIndexDisplay::Index
        } else {
            KeyOrIndexDisplay::None
        }
    }

    /// Parse an optional index option such as `USING <type>` or `COMMENT <string>`.
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

    /// Parse zero or more index options and return them as a vector.
    pub fn parse_index_options(&mut self) -> Result<Vec<IndexOption>, ParserError> {
        let mut options = Vec::new();

        loop {
            match self.parse_optional_index_option()? {
                Some(index_option) => options.push(index_option),
                None => return Ok(options),
            }
        }
    }

    /// Parse a single `SqlOption` used by various dialect-specific DDL statements.
    pub fn parse_sql_option(&mut self) -> Result<SqlOption, ParserError> {
        let is_mssql = dialect_of!(self is MsSqlDialect|GenericDialect);

        match self.peek_token().token {
            Token::Word(w) if w.keyword == Keyword::HEAP && is_mssql => {
                Ok(SqlOption::Ident(self.parse_identifier()?))
            }
            Token::Word(w) if w.keyword == Keyword::PARTITION && is_mssql => {
                self.parse_option_partition()
            }
            Token::Word(w) if w.keyword == Keyword::CLUSTERED && is_mssql => {
                self.parse_option_clustered()
            }
            _ => {
                let name = self.parse_identifier()?;
                self.expect_token(&Token::Eq)?;
                let value = self.parse_expr()?;

                Ok(SqlOption::KeyValue { key: name, value })
            }
        }
    }

    /// Parse a `CLUSTERED` table option (MSSQL-specific syntaxes supported).
    pub fn parse_option_clustered(&mut self) -> Result<SqlOption, ParserError> {
        if self.parse_keywords(&[
            Keyword::CLUSTERED,
            Keyword::COLUMNSTORE,
            Keyword::INDEX,
            Keyword::ORDER,
        ]) {
            Ok(SqlOption::Clustered(
                TableOptionsClustered::ColumnstoreIndexOrder(
                    self.parse_parenthesized_column_list(IsOptional::Mandatory, false)?,
                ),
            ))
        } else if self.parse_keywords(&[Keyword::CLUSTERED, Keyword::COLUMNSTORE, Keyword::INDEX]) {
            Ok(SqlOption::Clustered(
                TableOptionsClustered::ColumnstoreIndex,
            ))
        } else if self.parse_keywords(&[Keyword::CLUSTERED, Keyword::INDEX]) {
            self.expect_token(&Token::LParen)?;

            let columns = self.parse_comma_separated(|p| {
                let name = p.parse_identifier()?;
                let asc = p.parse_asc_desc();

                Ok(ClusteredIndex { name, asc })
            })?;

            self.expect_token(&Token::RParen)?;

            Ok(SqlOption::Clustered(TableOptionsClustered::Index(columns)))
        } else {
            Err(ParserError::ParserError(
                "invalid CLUSTERED sequence".to_string(),
            ))
        }
    }

    /// Parse a `PARTITION(...) FOR VALUES(...)` table option.
    pub fn parse_option_partition(&mut self) -> Result<SqlOption, ParserError> {
        self.expect_keyword_is(Keyword::PARTITION)?;
        self.expect_token(&Token::LParen)?;
        let column_name = self.parse_identifier()?;

        self.expect_keyword_is(Keyword::RANGE)?;
        let range_direction = if self.parse_keyword(Keyword::LEFT) {
            Some(PartitionRangeDirection::Left)
        } else if self.parse_keyword(Keyword::RIGHT) {
            Some(PartitionRangeDirection::Right)
        } else {
            None
        };

        self.expect_keywords(&[Keyword::FOR, Keyword::VALUES])?;
        self.expect_token(&Token::LParen)?;

        let for_values = self.parse_comma_separated(Parser::parse_expr)?;

        self.expect_token(&Token::RParen)?;
        self.expect_token(&Token::RParen)?;

        Ok(SqlOption::Partition {
            column_name,
            range_direction,
            for_values,
        })
    }

    /// Parse a parenthesized list of partition expressions and return a `Partition` value.
    pub fn parse_partition(&mut self) -> Result<Partition, ParserError> {
        self.expect_token(&Token::LParen)?;
        let partitions = self.parse_comma_separated(Parser::parse_expr)?;
        self.expect_token(&Token::RParen)?;
        Ok(Partition::Partitions(partitions))
    }

    /// Parse a parenthesized `SELECT` projection used for projection-based operations.
    pub fn parse_projection_select(&mut self) -> Result<ProjectionSelect, ParserError> {
        self.expect_token(&Token::LParen)?;
        self.expect_keyword_is(Keyword::SELECT)?;
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
    /// Parse `ALTER TABLE ... ADD PROJECTION ...` operation.
    pub fn parse_alter_table_add_projection(&mut self) -> Result<AlterTableOperation, ParserError> {
        let if_not_exists = self.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let name = self.parse_identifier()?;
        let query = self.parse_projection_select()?;
        Ok(AlterTableOperation::AddProjection {
            if_not_exists,
            name,
            select: query,
        })
    }

    /// Parse a single `ALTER TABLE` operation and return an `AlterTableOperation`.
    pub fn parse_alter_table_operation(&mut self) -> Result<AlterTableOperation, ParserError> {
        let operation = if self.parse_keyword(Keyword::ADD) {
            if let Some(constraint) = self.parse_optional_table_constraint()? {
                let not_valid = self.parse_keywords(&[Keyword::NOT, Keyword::VALID]);
                AlterTableOperation::AddConstraint {
                    constraint,
                    not_valid,
                }
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
                let old_name = self.parse_identifier()?;
                self.expect_keyword_is(Keyword::TO)?;
                let new_name = self.parse_identifier()?;
                AlterTableOperation::RenameConstraint { old_name, new_name }
            } else if self.parse_keyword(Keyword::TO) {
                let table_name = self.parse_object_name(false)?;
                AlterTableOperation::RenameTable {
                    table_name: RenameTableNameKind::To(table_name),
                }
            } else if self.parse_keyword(Keyword::AS) {
                let table_name = self.parse_object_name(false)?;
                AlterTableOperation::RenameTable {
                    table_name: RenameTableNameKind::As(table_name),
                }
            } else {
                let _ = self.parse_keyword(Keyword::COLUMN); // [ COLUMN ]
                let old_column_name = self.parse_identifier()?;
                self.expect_keyword_is(Keyword::TO)?;
                let new_column_name = self.parse_identifier()?;
                AlterTableOperation::RenameColumn {
                    old_column_name,
                    new_column_name,
                }
            }
        } else if self.parse_keyword(Keyword::DISABLE) {
            if self.parse_keywords(&[Keyword::ROW, Keyword::LEVEL, Keyword::SECURITY]) {
                AlterTableOperation::DisableRowLevelSecurity {}
            } else if self.parse_keyword(Keyword::RULE) {
                let name = self.parse_identifier()?;
                AlterTableOperation::DisableRule { name }
            } else if self.parse_keyword(Keyword::TRIGGER) {
                let name = self.parse_identifier()?;
                AlterTableOperation::DisableTrigger { name }
            } else {
                return self.expected(
                    "ROW LEVEL SECURITY, RULE, or TRIGGER after DISABLE",
                    self.peek_token(),
                );
            }
        } else if self.parse_keyword(Keyword::ENABLE) {
            if self.parse_keywords(&[Keyword::ALWAYS, Keyword::RULE]) {
                let name = self.parse_identifier()?;
                AlterTableOperation::EnableAlwaysRule { name }
            } else if self.parse_keywords(&[Keyword::ALWAYS, Keyword::TRIGGER]) {
                let name = self.parse_identifier()?;
                AlterTableOperation::EnableAlwaysTrigger { name }
            } else if self.parse_keywords(&[Keyword::ROW, Keyword::LEVEL, Keyword::SECURITY]) {
                AlterTableOperation::EnableRowLevelSecurity {}
            } else if self.parse_keywords(&[Keyword::REPLICA, Keyword::RULE]) {
                let name = self.parse_identifier()?;
                AlterTableOperation::EnableReplicaRule { name }
            } else if self.parse_keywords(&[Keyword::REPLICA, Keyword::TRIGGER]) {
                let name = self.parse_identifier()?;
                AlterTableOperation::EnableReplicaTrigger { name }
            } else if self.parse_keyword(Keyword::RULE) {
                let name = self.parse_identifier()?;
                AlterTableOperation::EnableRule { name }
            } else if self.parse_keyword(Keyword::TRIGGER) {
                let name = self.parse_identifier()?;
                AlterTableOperation::EnableTrigger { name }
            } else {
                return self.expected(
                    "ALWAYS, REPLICA, ROW LEVEL SECURITY, RULE, or TRIGGER after ENABLE",
                    self.peek_token(),
                );
            }
        } else if self.parse_keywords(&[
            Keyword::FORCE,
            Keyword::ROW,
            Keyword::LEVEL,
            Keyword::SECURITY,
        ]) {
            AlterTableOperation::ForceRowLevelSecurity
        } else if self.parse_keywords(&[
            Keyword::NO,
            Keyword::FORCE,
            Keyword::ROW,
            Keyword::LEVEL,
            Keyword::SECURITY,
        ]) {
            AlterTableOperation::NoForceRowLevelSecurity
        } else if self.parse_keywords(&[Keyword::CLEAR, Keyword::PROJECTION])
            && dialect_of!(self is ClickHouseDialect|GenericDialect)
        {
            let if_exists = self.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
            let name = self.parse_identifier()?;
            let partition = if self.parse_keywords(&[Keyword::IN, Keyword::PARTITION]) {
                Some(self.parse_identifier()?)
            } else {
                None
            };
            AlterTableOperation::ClearProjection {
                if_exists,
                name,
                partition,
            }
        } else if self.parse_keywords(&[Keyword::MATERIALIZE, Keyword::PROJECTION])
            && dialect_of!(self is ClickHouseDialect|GenericDialect)
        {
            let if_exists = self.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
            let name = self.parse_identifier()?;
            let partition = if self.parse_keywords(&[Keyword::IN, Keyword::PARTITION]) {
                Some(self.parse_identifier()?)
            } else {
                None
            };
            AlterTableOperation::MaterializeProjection {
                if_exists,
                name,
                partition,
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
                let drop_behavior = self.parse_optional_drop_behavior();
                AlterTableOperation::DropConstraint {
                    if_exists,
                    name,
                    drop_behavior,
                }
            } else if self.parse_keywords(&[Keyword::PRIMARY, Keyword::KEY]) {
                let drop_behavior = self.parse_optional_drop_behavior();
                AlterTableOperation::DropPrimaryKey { drop_behavior }
            } else if self.parse_keywords(&[Keyword::FOREIGN, Keyword::KEY]) {
                let name = self.parse_identifier()?;
                let drop_behavior = self.parse_optional_drop_behavior();
                AlterTableOperation::DropForeignKey {
                    name,
                    drop_behavior,
                }
            } else if self.parse_keyword(Keyword::INDEX) {
                let name = self.parse_identifier()?;
                AlterTableOperation::DropIndex { name }
            } else if self.parse_keyword(Keyword::PROJECTION)
                && dialect_of!(self is ClickHouseDialect|GenericDialect)
            {
                let if_exists = self.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
                let name = self.parse_identifier()?;
                AlterTableOperation::DropProjection { if_exists, name }
            } else if self.parse_keywords(&[Keyword::CLUSTERING, Keyword::KEY]) {
                AlterTableOperation::DropClusteringKey
            } else {
                let has_column_keyword = self.parse_keyword(Keyword::COLUMN); // [ COLUMN ]
                let if_exists = self.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
                let column_names = if self.dialect.supports_comma_separated_drop_column_list() {
                    self.parse_comma_separated(Parser::parse_identifier)?
                } else {
                    vec![self.parse_identifier()?]
                };
                let drop_behavior = self.parse_optional_drop_behavior();
                AlterTableOperation::DropColumn {
                    has_column_keyword,
                    column_names,
                    if_exists,
                    drop_behavior,
                }
            }
        } else if self.parse_keyword(Keyword::PARTITION) {
            self.expect_token(&Token::LParen)?;
            let before = self.parse_comma_separated(Parser::parse_expr)?;
            self.expect_token(&Token::RParen)?;
            self.expect_keyword_is(Keyword::RENAME)?;
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
            let col_name = self.parse_identifier()?;
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
            let column_name = self.parse_identifier()?;
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
            } else if self.parse_keywords(&[Keyword::SET, Keyword::DATA, Keyword::TYPE]) {
                self.parse_set_data_type(true)?
            } else if self.parse_keyword(Keyword::TYPE) {
                self.parse_set_data_type(false)?
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
            self.expect_keyword_is(Keyword::WITH)?;
            let table_name = self.parse_object_name(false)?;
            AlterTableOperation::SwapWith { table_name }
        } else if dialect_of!(self is PostgreSqlDialect | GenericDialect)
            && self.parse_keywords(&[Keyword::OWNER, Keyword::TO])
        {
            let new_owner = self.parse_owner()?;
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
                self.expect_keyword_is(Keyword::NAME)?;
                Some(self.parse_identifier()?)
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
                self.expect_keyword_is(Keyword::NAME)?;
                Some(self.parse_identifier()?)
            } else {
                None
            };
            AlterTableOperation::UnfreezePartition {
                partition,
                with_name,
            }
        } else if self.parse_keywords(&[Keyword::CLUSTER, Keyword::BY]) {
            self.expect_token(&Token::LParen)?;
            let exprs = self.parse_comma_separated(|parser| parser.parse_expr())?;
            self.expect_token(&Token::RParen)?;
            AlterTableOperation::ClusterBy { exprs }
        } else if self.parse_keywords(&[Keyword::SUSPEND, Keyword::RECLUSTER]) {
            AlterTableOperation::SuspendRecluster
        } else if self.parse_keywords(&[Keyword::RESUME, Keyword::RECLUSTER]) {
            AlterTableOperation::ResumeRecluster
        } else if self.parse_keyword(Keyword::LOCK) {
            let equals = self.consume_token(&Token::Eq);
            let lock = match self.parse_one_of_keywords(&[
                Keyword::DEFAULT,
                Keyword::EXCLUSIVE,
                Keyword::NONE,
                Keyword::SHARED,
            ]) {
                Some(Keyword::DEFAULT) => AlterTableLock::Default,
                Some(Keyword::EXCLUSIVE) => AlterTableLock::Exclusive,
                Some(Keyword::NONE) => AlterTableLock::None,
                Some(Keyword::SHARED) => AlterTableLock::Shared,
                _ => self.expected(
                    "DEFAULT, EXCLUSIVE, NONE or SHARED after LOCK [=]",
                    self.peek_token(),
                )?,
            };
            AlterTableOperation::Lock { equals, lock }
        } else if self.parse_keyword(Keyword::ALGORITHM) {
            let equals = self.consume_token(&Token::Eq);
            let algorithm = match self.parse_one_of_keywords(&[
                Keyword::DEFAULT,
                Keyword::INSTANT,
                Keyword::INPLACE,
                Keyword::COPY,
            ]) {
                Some(Keyword::DEFAULT) => AlterTableAlgorithm::Default,
                Some(Keyword::INSTANT) => AlterTableAlgorithm::Instant,
                Some(Keyword::INPLACE) => AlterTableAlgorithm::Inplace,
                Some(Keyword::COPY) => AlterTableAlgorithm::Copy,
                _ => self.expected(
                    "DEFAULT, INSTANT, INPLACE, or COPY after ALGORITHM [=]",
                    self.peek_token(),
                )?,
            };
            AlterTableOperation::Algorithm { equals, algorithm }
        } else if self.parse_keyword(Keyword::AUTO_INCREMENT) {
            let equals = self.consume_token(&Token::Eq);
            let value = self.parse_number_value()?;
            AlterTableOperation::AutoIncrement { equals, value }
        } else if self.parse_keywords(&[Keyword::REPLICA, Keyword::IDENTITY]) {
            let identity = if self.parse_keyword(Keyword::NOTHING) {
                ReplicaIdentity::Nothing
            } else if self.parse_keyword(Keyword::FULL) {
                ReplicaIdentity::Full
            } else if self.parse_keyword(Keyword::DEFAULT) {
                ReplicaIdentity::Default
            } else if self.parse_keywords(&[Keyword::USING, Keyword::INDEX]) {
                ReplicaIdentity::Index(self.parse_identifier()?)
            } else {
                return self.expected(
                    "NOTHING, FULL, DEFAULT, or USING INDEX index_name after REPLICA IDENTITY",
                    self.peek_token(),
                );
            };

            AlterTableOperation::ReplicaIdentity { identity }
        } else if self.parse_keywords(&[Keyword::VALIDATE, Keyword::CONSTRAINT]) {
            let name = self.parse_identifier()?;
            AlterTableOperation::ValidateConstraint { name }
        } else {
            let mut options =
                self.parse_options_with_keywords(&[Keyword::SET, Keyword::TBLPROPERTIES])?;
            if !options.is_empty() {
                AlterTableOperation::SetTblProperties {
                    table_properties: options,
                }
            } else {
                options = self.parse_options(Keyword::SET)?;
                if !options.is_empty() {
                    AlterTableOperation::SetOptionsParens { options }
                } else {
                    return self.expected(
                    "ADD, RENAME, PARTITION, SWAP, DROP, REPLICA IDENTITY, SET, or SET TBLPROPERTIES after ALTER TABLE",
                    self.peek_token(),
                  );
                }
            }
        };
        Ok(operation)
    }

    fn parse_set_data_type(&mut self, had_set: bool) -> Result<AlterColumnOperation, ParserError> {
        let data_type = self.parse_data_type()?;
        let using = if self.dialect.supports_alter_column_type_using()
            && self.parse_keyword(Keyword::USING)
        {
            Some(self.parse_expr()?)
        } else {
            None
        };
        Ok(AlterColumnOperation::SetDataType {
            data_type,
            using,
            had_set,
        })
    }

    fn parse_part_or_partition(&mut self) -> Result<Partition, ParserError> {
        let keyword = self.expect_one_of_keywords(&[Keyword::PART, Keyword::PARTITION])?;
        match keyword {
            Keyword::PART => Ok(Partition::Part(self.parse_expr()?)),
            Keyword::PARTITION => Ok(Partition::Expr(self.parse_expr()?)),
            // unreachable because expect_one_of_keywords used above
            unexpected_keyword => Err(ParserError::ParserError(
                format!("Internal parser error: expected any of {{PART, PARTITION}}, got {unexpected_keyword:?}"),
            )),
        }
    }

    /// Parse an `ALTER <object>` statement and dispatch to the appropriate alter handler.
    pub fn parse_alter(&mut self) -> Result<Statement, ParserError> {
        let object_type = self.expect_one_of_keywords(&[
            Keyword::VIEW,
            Keyword::TYPE,
            Keyword::TABLE,
            Keyword::INDEX,
            Keyword::ROLE,
            Keyword::POLICY,
            Keyword::CONNECTOR,
            Keyword::ICEBERG,
            Keyword::SCHEMA,
            Keyword::USER,
            Keyword::OPERATOR,
        ])?;
        match object_type {
            Keyword::SCHEMA => {
                self.prev_token();
                self.prev_token();
                self.parse_alter_schema()
            }
            Keyword::VIEW => self.parse_alter_view(),
            Keyword::TYPE => self.parse_alter_type(),
            Keyword::TABLE => self.parse_alter_table(false),
            Keyword::ICEBERG => {
                self.expect_keyword(Keyword::TABLE)?;
                self.parse_alter_table(true)
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
            Keyword::OPERATOR => {
                if self.parse_keyword(Keyword::FAMILY) {
                    self.parse_alter_operator_family().map(Into::into)
                } else if self.parse_keyword(Keyword::CLASS) {
                    self.parse_alter_operator_class().map(Into::into)
                } else {
                    self.parse_alter_operator().map(Into::into)
                }
            }
            Keyword::ROLE => self.parse_alter_role(),
            Keyword::POLICY => self.parse_alter_policy(),
            Keyword::CONNECTOR => self.parse_alter_connector(),
            Keyword::USER => self.parse_alter_user().map(Into::into),
            // unreachable because expect_one_of_keywords used above
            unexpected_keyword => Err(ParserError::ParserError(
                format!("Internal parser error: expected any of {{VIEW, TYPE, TABLE, INDEX, ROLE, POLICY, CONNECTOR, ICEBERG, SCHEMA, USER, OPERATOR}}, got {unexpected_keyword:?}"),
            )),
        }
    }

    /// Parse a [Statement::AlterTable]
    pub fn parse_alter_table(&mut self, iceberg: bool) -> Result<Statement, ParserError> {
        let if_exists = self.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
        let only = self.parse_keyword(Keyword::ONLY); // [ ONLY ]
        let table_name = self.parse_object_name(false)?;
        let on_cluster = self.parse_optional_on_cluster()?;
        let operations = self.parse_comma_separated(Parser::parse_alter_table_operation)?;

        let mut location = None;
        if self.parse_keyword(Keyword::LOCATION) {
            location = Some(HiveSetLocation {
                has_set: false,
                location: self.parse_identifier()?,
            });
        } else if self.parse_keywords(&[Keyword::SET, Keyword::LOCATION]) {
            location = Some(HiveSetLocation {
                has_set: true,
                location: self.parse_identifier()?,
            });
        }

        let end_token = if self.peek_token_ref().token == Token::SemiColon {
            self.peek_token_ref().clone()
        } else {
            self.get_current_token().clone()
        };

        Ok(AlterTable {
            name: table_name,
            if_exists,
            only,
            operations,
            location,
            on_cluster,
            table_type: if iceberg {
                Some(AlterTableType::Iceberg)
            } else {
                None
            },
            end_token: AttachedToken(end_token),
        }
        .into())
    }

    /// Parse an `ALTER VIEW` statement.
    pub fn parse_alter_view(&mut self) -> Result<Statement, ParserError> {
        let name = self.parse_object_name(false)?;
        let columns = self.parse_parenthesized_column_list(Optional, false)?;

        let with_options = self.parse_options(Keyword::WITH)?;

        self.expect_keyword_is(Keyword::AS)?;
        let query = self.parse_query()?;

        Ok(Statement::AlterView {
            name,
            columns,
            query,
            with_options,
        })
    }

    /// Parse a [Statement::AlterType]
    pub fn parse_alter_type(&mut self) -> Result<Statement, ParserError> {
        let name = self.parse_object_name(false)?;

        if self.parse_keywords(&[Keyword::RENAME, Keyword::TO]) {
            let new_name = self.parse_identifier()?;
            Ok(Statement::AlterType(AlterType {
                name,
                operation: AlterTypeOperation::Rename(AlterTypeRename { new_name }),
            }))
        } else if self.parse_keywords(&[Keyword::ADD, Keyword::VALUE]) {
            let if_not_exists = self.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
            let new_enum_value = self.parse_identifier()?;
            let position = if self.parse_keyword(Keyword::BEFORE) {
                Some(AlterTypeAddValuePosition::Before(self.parse_identifier()?))
            } else if self.parse_keyword(Keyword::AFTER) {
                Some(AlterTypeAddValuePosition::After(self.parse_identifier()?))
            } else {
                None
            };

            Ok(Statement::AlterType(AlterType {
                name,
                operation: AlterTypeOperation::AddValue(AlterTypeAddValue {
                    if_not_exists,
                    value: new_enum_value,
                    position,
                }),
            }))
        } else if self.parse_keywords(&[Keyword::RENAME, Keyword::VALUE]) {
            let existing_enum_value = self.parse_identifier()?;
            self.expect_keyword(Keyword::TO)?;
            let new_enum_value = self.parse_identifier()?;

            Ok(Statement::AlterType(AlterType {
                name,
                operation: AlterTypeOperation::RenameValue(AlterTypeRenameValue {
                    from: existing_enum_value,
                    to: new_enum_value,
                }),
            }))
        } else {
            self.expected_ref(
                "{RENAME TO | { RENAME | ADD } VALUE}",
                self.peek_token_ref(),
            )
        }
    }

    /// Parse a [Statement::AlterOperator]
    ///
    /// [PostgreSQL Documentation](https://www.postgresql.org/docs/current/sql-alteroperator.html)
    pub fn parse_alter_operator(&mut self) -> Result<AlterOperator, ParserError> {
        let name = self.parse_operator_name()?;

        // Parse (left_type, right_type)
        self.expect_token(&Token::LParen)?;

        let left_type = if self.parse_keyword(Keyword::NONE) {
            None
        } else {
            Some(self.parse_data_type()?)
        };

        self.expect_token(&Token::Comma)?;
        let right_type = self.parse_data_type()?;
        self.expect_token(&Token::RParen)?;

        // Parse the operation
        let operation = if self.parse_keywords(&[Keyword::OWNER, Keyword::TO]) {
            let owner = if self.parse_keyword(Keyword::CURRENT_ROLE) {
                Owner::CurrentRole
            } else if self.parse_keyword(Keyword::CURRENT_USER) {
                Owner::CurrentUser
            } else if self.parse_keyword(Keyword::SESSION_USER) {
                Owner::SessionUser
            } else {
                Owner::Ident(self.parse_identifier()?)
            };
            AlterOperatorOperation::OwnerTo(owner)
        } else if self.parse_keywords(&[Keyword::SET, Keyword::SCHEMA]) {
            let schema_name = self.parse_object_name(false)?;
            AlterOperatorOperation::SetSchema { schema_name }
        } else if self.parse_keyword(Keyword::SET) {
            self.expect_token(&Token::LParen)?;

            let mut options = Vec::new();
            loop {
                let keyword = self.expect_one_of_keywords(&[
                    Keyword::RESTRICT,
                    Keyword::JOIN,
                    Keyword::COMMUTATOR,
                    Keyword::NEGATOR,
                    Keyword::HASHES,
                    Keyword::MERGES,
                ])?;

                match keyword {
                    Keyword::RESTRICT => {
                        self.expect_token(&Token::Eq)?;
                        let proc_name = if self.parse_keyword(Keyword::NONE) {
                            None
                        } else {
                            Some(self.parse_object_name(false)?)
                        };
                        options.push(OperatorOption::Restrict(proc_name));
                    }
                    Keyword::JOIN => {
                        self.expect_token(&Token::Eq)?;
                        let proc_name = if self.parse_keyword(Keyword::NONE) {
                            None
                        } else {
                            Some(self.parse_object_name(false)?)
                        };
                        options.push(OperatorOption::Join(proc_name));
                    }
                    Keyword::COMMUTATOR => {
                        self.expect_token(&Token::Eq)?;
                        let op_name = self.parse_operator_name()?;
                        options.push(OperatorOption::Commutator(op_name));
                    }
                    Keyword::NEGATOR => {
                        self.expect_token(&Token::Eq)?;
                        let op_name = self.parse_operator_name()?;
                        options.push(OperatorOption::Negator(op_name));
                    }
                    Keyword::HASHES => {
                        options.push(OperatorOption::Hashes);
                    }
                    Keyword::MERGES => {
                        options.push(OperatorOption::Merges);
                    }
                    unexpected_keyword => return Err(ParserError::ParserError(
                        format!("Internal parser error: unexpected keyword `{unexpected_keyword}` in operator option"),
                    )),
                }

                if !self.consume_token(&Token::Comma) {
                    break;
                }
            }

            self.expect_token(&Token::RParen)?;
            AlterOperatorOperation::Set { options }
        } else {
            return self.expected_ref(
                "OWNER TO, SET SCHEMA, or SET after ALTER OPERATOR",
                self.peek_token_ref(),
            );
        };

        Ok(AlterOperator {
            name,
            left_type,
            right_type,
            operation,
        })
    }

    /// Parse an operator item for ALTER OPERATOR FAMILY ADD operations
    fn parse_operator_family_add_operator(&mut self) -> Result<OperatorFamilyItem, ParserError> {
        let strategy_number = self.parse_literal_uint()?;
        let operator_name = self.parse_operator_name()?;

        // Operator argument types (required for ALTER OPERATOR FAMILY)
        self.expect_token(&Token::LParen)?;
        let op_types = self.parse_comma_separated(Parser::parse_data_type)?;
        self.expect_token(&Token::RParen)?;

        // Optional purpose
        let purpose = if self.parse_keyword(Keyword::FOR) {
            if self.parse_keyword(Keyword::SEARCH) {
                Some(OperatorPurpose::ForSearch)
            } else if self.parse_keywords(&[Keyword::ORDER, Keyword::BY]) {
                let sort_family = self.parse_object_name(false)?;
                Some(OperatorPurpose::ForOrderBy { sort_family })
            } else {
                return self.expected("SEARCH or ORDER BY after FOR", self.peek_token());
            }
        } else {
            None
        };

        Ok(OperatorFamilyItem::Operator {
            strategy_number,
            operator_name,
            op_types,
            purpose,
        })
    }

    /// Parse a function item for ALTER OPERATOR FAMILY ADD operations
    fn parse_operator_family_add_function(&mut self) -> Result<OperatorFamilyItem, ParserError> {
        let support_number = self.parse_literal_uint()?;

        // Optional operator types
        let op_types = if self.consume_token(&Token::LParen) && self.peek_token() != Token::RParen {
            let types = self.parse_comma_separated(Parser::parse_data_type)?;
            self.expect_token(&Token::RParen)?;
            Some(types)
        } else if self.consume_token(&Token::LParen) {
            self.expect_token(&Token::RParen)?;
            Some(vec![])
        } else {
            None
        };

        let function_name = self.parse_object_name(false)?;

        // Function argument types
        let argument_types = if self.consume_token(&Token::LParen) {
            if self.peek_token() == Token::RParen {
                self.expect_token(&Token::RParen)?;
                vec![]
            } else {
                let types = self.parse_comma_separated(Parser::parse_data_type)?;
                self.expect_token(&Token::RParen)?;
                types
            }
        } else {
            vec![]
        };

        Ok(OperatorFamilyItem::Function {
            support_number,
            op_types,
            function_name,
            argument_types,
        })
    }

    /// Parse an operator item for ALTER OPERATOR FAMILY DROP operations
    fn parse_operator_family_drop_operator(
        &mut self,
    ) -> Result<OperatorFamilyDropItem, ParserError> {
        let strategy_number = self.parse_literal_uint()?;

        // Operator argument types (required for DROP)
        self.expect_token(&Token::LParen)?;
        let op_types = self.parse_comma_separated(Parser::parse_data_type)?;
        self.expect_token(&Token::RParen)?;

        Ok(OperatorFamilyDropItem::Operator {
            strategy_number,
            op_types,
        })
    }

    /// Parse a function item for ALTER OPERATOR FAMILY DROP operations
    fn parse_operator_family_drop_function(
        &mut self,
    ) -> Result<OperatorFamilyDropItem, ParserError> {
        let support_number = self.parse_literal_uint()?;

        // Operator types (required for DROP)
        self.expect_token(&Token::LParen)?;
        let op_types = self.parse_comma_separated(Parser::parse_data_type)?;
        self.expect_token(&Token::RParen)?;

        Ok(OperatorFamilyDropItem::Function {
            support_number,
            op_types,
        })
    }

    /// Parse an operator family item for ADD operations (dispatches to operator or function parsing)
    fn parse_operator_family_add_item(&mut self) -> Result<OperatorFamilyItem, ParserError> {
        if self.parse_keyword(Keyword::OPERATOR) {
            self.parse_operator_family_add_operator()
        } else if self.parse_keyword(Keyword::FUNCTION) {
            self.parse_operator_family_add_function()
        } else {
            self.expected("OPERATOR or FUNCTION", self.peek_token())
        }
    }

    /// Parse an operator family item for DROP operations (dispatches to operator or function parsing)
    fn parse_operator_family_drop_item(&mut self) -> Result<OperatorFamilyDropItem, ParserError> {
        if self.parse_keyword(Keyword::OPERATOR) {
            self.parse_operator_family_drop_operator()
        } else if self.parse_keyword(Keyword::FUNCTION) {
            self.parse_operator_family_drop_function()
        } else {
            self.expected("OPERATOR or FUNCTION", self.peek_token())
        }
    }

    /// Parse a [Statement::AlterOperatorFamily]
    /// See <https://www.postgresql.org/docs/current/sql-alteropfamily.html>
    pub fn parse_alter_operator_family(&mut self) -> Result<AlterOperatorFamily, ParserError> {
        let name = self.parse_object_name(false)?;
        self.expect_keyword(Keyword::USING)?;
        let using = self.parse_identifier()?;

        let operation = if self.parse_keyword(Keyword::ADD) {
            let items = self.parse_comma_separated(Parser::parse_operator_family_add_item)?;
            AlterOperatorFamilyOperation::Add { items }
        } else if self.parse_keyword(Keyword::DROP) {
            let items = self.parse_comma_separated(Parser::parse_operator_family_drop_item)?;
            AlterOperatorFamilyOperation::Drop { items }
        } else if self.parse_keywords(&[Keyword::RENAME, Keyword::TO]) {
            let new_name = self.parse_object_name(false)?;
            AlterOperatorFamilyOperation::RenameTo { new_name }
        } else if self.parse_keywords(&[Keyword::OWNER, Keyword::TO]) {
            let owner = self.parse_owner()?;
            AlterOperatorFamilyOperation::OwnerTo(owner)
        } else if self.parse_keywords(&[Keyword::SET, Keyword::SCHEMA]) {
            let schema_name = self.parse_object_name(false)?;
            AlterOperatorFamilyOperation::SetSchema { schema_name }
        } else {
            return self.expected_ref(
                "ADD, DROP, RENAME TO, OWNER TO, or SET SCHEMA after ALTER OPERATOR FAMILY",
                self.peek_token_ref(),
            );
        };

        Ok(AlterOperatorFamily {
            name,
            using,
            operation,
        })
    }

    /// Parse an `ALTER OPERATOR CLASS` statement.
    ///
    /// Handles operations like `RENAME TO`, `OWNER TO`, and `SET SCHEMA`.
    pub fn parse_alter_operator_class(&mut self) -> Result<AlterOperatorClass, ParserError> {
        let name = self.parse_object_name(false)?;
        self.expect_keyword(Keyword::USING)?;
        let using = self.parse_identifier()?;

        let operation = if self.parse_keywords(&[Keyword::RENAME, Keyword::TO]) {
            let new_name = self.parse_object_name(false)?;
            AlterOperatorClassOperation::RenameTo { new_name }
        } else if self.parse_keywords(&[Keyword::OWNER, Keyword::TO]) {
            let owner = self.parse_owner()?;
            AlterOperatorClassOperation::OwnerTo(owner)
        } else if self.parse_keywords(&[Keyword::SET, Keyword::SCHEMA]) {
            let schema_name = self.parse_object_name(false)?;
            AlterOperatorClassOperation::SetSchema { schema_name }
        } else {
            return self.expected_ref(
                "RENAME TO, OWNER TO, or SET SCHEMA after ALTER OPERATOR CLASS",
                self.peek_token_ref(),
            );
        };

        Ok(AlterOperatorClass {
            name,
            using,
            operation,
        })
    }

    /// Parse an `ALTER SCHEMA` statement.
    ///
    /// Supports operations such as setting options, renaming, adding/dropping replicas, and changing owner.
    pub fn parse_alter_schema(&mut self) -> Result<Statement, ParserError> {
        self.expect_keywords(&[Keyword::ALTER, Keyword::SCHEMA])?;
        let if_exists = self.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
        let name = self.parse_object_name(false)?;
        let operation = if self.parse_keywords(&[Keyword::SET, Keyword::OPTIONS]) {
            self.prev_token();
            let options = self.parse_options(Keyword::OPTIONS)?;
            AlterSchemaOperation::SetOptionsParens { options }
        } else if self.parse_keywords(&[Keyword::SET, Keyword::DEFAULT, Keyword::COLLATE]) {
            let collate = self.parse_expr()?;
            AlterSchemaOperation::SetDefaultCollate { collate }
        } else if self.parse_keywords(&[Keyword::ADD, Keyword::REPLICA]) {
            let replica = self.parse_identifier()?;
            let options = if self.peek_keyword(Keyword::OPTIONS) {
                Some(self.parse_options(Keyword::OPTIONS)?)
            } else {
                None
            };
            AlterSchemaOperation::AddReplica { replica, options }
        } else if self.parse_keywords(&[Keyword::DROP, Keyword::REPLICA]) {
            let replica = self.parse_identifier()?;
            AlterSchemaOperation::DropReplica { replica }
        } else if self.parse_keywords(&[Keyword::RENAME, Keyword::TO]) {
            let new_name = self.parse_object_name(false)?;
            AlterSchemaOperation::Rename { name: new_name }
        } else if self.parse_keywords(&[Keyword::OWNER, Keyword::TO]) {
            let owner = self.parse_owner()?;
            AlterSchemaOperation::OwnerTo { owner }
        } else {
            return self.expected_ref("ALTER SCHEMA operation", self.peek_token_ref());
        };
        Ok(Statement::AlterSchema(AlterSchema {
            name,
            if_exists,
            operations: vec![operation],
        }))
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
                    self.peek_token().span.start
                ),
            }
        } else {
            Ok(Statement::Call(Function {
                name: object_name,
                uses_odbc_syntax: false,
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
            source = CopySource::Query(self.parse_query()?);
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
        while let Some(opt) = self.maybe_parse(|parser| parser.parse_copy_legacy_option())? {
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

    /// Parse [Statement::Open]
    fn parse_open(&mut self) -> Result<Statement, ParserError> {
        self.expect_keyword(Keyword::OPEN)?;
        Ok(Statement::Open(OpenStatement {
            cursor_name: self.parse_identifier()?,
        }))
    }

    /// Parse a `CLOSE` cursor statement.
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
        // FORMAT \[ AS \] is optional
        if self.parse_keyword(Keyword::FORMAT) {
            let _ = self.parse_keyword(Keyword::AS);
        }

        let ret = match self.parse_one_of_keywords(&[
            Keyword::ACCEPTANYDATE,
            Keyword::ACCEPTINVCHARS,
            Keyword::ADDQUOTES,
            Keyword::ALLOWOVERWRITE,
            Keyword::BINARY,
            Keyword::BLANKSASNULL,
            Keyword::BZIP2,
            Keyword::CLEANPATH,
            Keyword::COMPUPDATE,
            Keyword::CSV,
            Keyword::DATEFORMAT,
            Keyword::DELIMITER,
            Keyword::EMPTYASNULL,
            Keyword::ENCRYPTED,
            Keyword::ESCAPE,
            Keyword::EXTENSION,
            Keyword::FIXEDWIDTH,
            Keyword::GZIP,
            Keyword::HEADER,
            Keyword::IAM_ROLE,
            Keyword::IGNOREHEADER,
            Keyword::JSON,
            Keyword::MANIFEST,
            Keyword::MAXFILESIZE,
            Keyword::NULL,
            Keyword::PARALLEL,
            Keyword::PARQUET,
            Keyword::PARTITION,
            Keyword::REGION,
            Keyword::REMOVEQUOTES,
            Keyword::ROWGROUPSIZE,
            Keyword::STATUPDATE,
            Keyword::TIMEFORMAT,
            Keyword::TRUNCATECOLUMNS,
            Keyword::ZSTD,
        ]) {
            Some(Keyword::ACCEPTANYDATE) => CopyLegacyOption::AcceptAnyDate,
            Some(Keyword::ACCEPTINVCHARS) => {
                let _ = self.parse_keyword(Keyword::AS); // [ AS ]
                let ch = if matches!(self.peek_token().token, Token::SingleQuotedString(_)) {
                    Some(self.parse_literal_string()?)
                } else {
                    None
                };
                CopyLegacyOption::AcceptInvChars(ch)
            }
            Some(Keyword::ADDQUOTES) => CopyLegacyOption::AddQuotes,
            Some(Keyword::ALLOWOVERWRITE) => CopyLegacyOption::AllowOverwrite,
            Some(Keyword::BINARY) => CopyLegacyOption::Binary,
            Some(Keyword::BLANKSASNULL) => CopyLegacyOption::BlankAsNull,
            Some(Keyword::BZIP2) => CopyLegacyOption::Bzip2,
            Some(Keyword::CLEANPATH) => CopyLegacyOption::CleanPath,
            Some(Keyword::COMPUPDATE) => {
                let preset = self.parse_keyword(Keyword::PRESET);
                let enabled = match self.parse_one_of_keywords(&[
                    Keyword::TRUE,
                    Keyword::FALSE,
                    Keyword::ON,
                    Keyword::OFF,
                ]) {
                    Some(Keyword::TRUE) | Some(Keyword::ON) => Some(true),
                    Some(Keyword::FALSE) | Some(Keyword::OFF) => Some(false),
                    _ => None,
                };
                CopyLegacyOption::CompUpdate { preset, enabled }
            }
            Some(Keyword::CSV) => CopyLegacyOption::Csv({
                let mut opts = vec![];
                while let Some(opt) =
                    self.maybe_parse(|parser| parser.parse_copy_legacy_csv_option())?
                {
                    opts.push(opt);
                }
                opts
            }),
            Some(Keyword::DATEFORMAT) => {
                let _ = self.parse_keyword(Keyword::AS);
                let fmt = if matches!(self.peek_token().token, Token::SingleQuotedString(_)) {
                    Some(self.parse_literal_string()?)
                } else {
                    None
                };
                CopyLegacyOption::DateFormat(fmt)
            }
            Some(Keyword::DELIMITER) => {
                let _ = self.parse_keyword(Keyword::AS);
                CopyLegacyOption::Delimiter(self.parse_literal_char()?)
            }
            Some(Keyword::EMPTYASNULL) => CopyLegacyOption::EmptyAsNull,
            Some(Keyword::ENCRYPTED) => {
                let auto = self.parse_keyword(Keyword::AUTO);
                CopyLegacyOption::Encrypted { auto }
            }
            Some(Keyword::ESCAPE) => CopyLegacyOption::Escape,
            Some(Keyword::EXTENSION) => {
                let ext = self.parse_literal_string()?;
                CopyLegacyOption::Extension(ext)
            }
            Some(Keyword::FIXEDWIDTH) => {
                let spec = self.parse_literal_string()?;
                CopyLegacyOption::FixedWidth(spec)
            }
            Some(Keyword::GZIP) => CopyLegacyOption::Gzip,
            Some(Keyword::HEADER) => CopyLegacyOption::Header,
            Some(Keyword::IAM_ROLE) => CopyLegacyOption::IamRole(self.parse_iam_role_kind()?),
            Some(Keyword::IGNOREHEADER) => {
                let _ = self.parse_keyword(Keyword::AS);
                let num_rows = self.parse_literal_uint()?;
                CopyLegacyOption::IgnoreHeader(num_rows)
            }
            Some(Keyword::JSON) => {
                let _ = self.parse_keyword(Keyword::AS);
                let fmt = if matches!(self.peek_token().token, Token::SingleQuotedString(_)) {
                    Some(self.parse_literal_string()?)
                } else {
                    None
                };
                CopyLegacyOption::Json(fmt)
            }
            Some(Keyword::MANIFEST) => {
                let verbose = self.parse_keyword(Keyword::VERBOSE);
                CopyLegacyOption::Manifest { verbose }
            }
            Some(Keyword::MAXFILESIZE) => {
                let _ = self.parse_keyword(Keyword::AS);
                let size = self.parse_number_value()?.value;
                let unit = match self.parse_one_of_keywords(&[Keyword::MB, Keyword::GB]) {
                    Some(Keyword::MB) => Some(FileSizeUnit::MB),
                    Some(Keyword::GB) => Some(FileSizeUnit::GB),
                    _ => None,
                };
                CopyLegacyOption::MaxFileSize(FileSize { size, unit })
            }
            Some(Keyword::NULL) => {
                let _ = self.parse_keyword(Keyword::AS);
                CopyLegacyOption::Null(self.parse_literal_string()?)
            }
            Some(Keyword::PARALLEL) => {
                let enabled = match self.parse_one_of_keywords(&[
                    Keyword::TRUE,
                    Keyword::FALSE,
                    Keyword::ON,
                    Keyword::OFF,
                ]) {
                    Some(Keyword::TRUE) | Some(Keyword::ON) => Some(true),
                    Some(Keyword::FALSE) | Some(Keyword::OFF) => Some(false),
                    _ => None,
                };
                CopyLegacyOption::Parallel(enabled)
            }
            Some(Keyword::PARQUET) => CopyLegacyOption::Parquet,
            Some(Keyword::PARTITION) => {
                self.expect_keyword(Keyword::BY)?;
                let columns = self.parse_parenthesized_column_list(IsOptional::Mandatory, false)?;
                let include = self.parse_keyword(Keyword::INCLUDE);
                CopyLegacyOption::PartitionBy(UnloadPartitionBy { columns, include })
            }
            Some(Keyword::REGION) => {
                let _ = self.parse_keyword(Keyword::AS);
                let region = self.parse_literal_string()?;
                CopyLegacyOption::Region(region)
            }
            Some(Keyword::REMOVEQUOTES) => CopyLegacyOption::RemoveQuotes,
            Some(Keyword::ROWGROUPSIZE) => {
                let _ = self.parse_keyword(Keyword::AS);
                let file_size = self.parse_file_size()?;
                CopyLegacyOption::RowGroupSize(file_size)
            }
            Some(Keyword::STATUPDATE) => {
                let enabled = match self.parse_one_of_keywords(&[
                    Keyword::TRUE,
                    Keyword::FALSE,
                    Keyword::ON,
                    Keyword::OFF,
                ]) {
                    Some(Keyword::TRUE) | Some(Keyword::ON) => Some(true),
                    Some(Keyword::FALSE) | Some(Keyword::OFF) => Some(false),
                    _ => None,
                };
                CopyLegacyOption::StatUpdate(enabled)
            }
            Some(Keyword::TIMEFORMAT) => {
                let _ = self.parse_keyword(Keyword::AS);
                let fmt = if matches!(self.peek_token().token, Token::SingleQuotedString(_)) {
                    Some(self.parse_literal_string()?)
                } else {
                    None
                };
                CopyLegacyOption::TimeFormat(fmt)
            }
            Some(Keyword::TRUNCATECOLUMNS) => CopyLegacyOption::TruncateColumns,
            Some(Keyword::ZSTD) => CopyLegacyOption::Zstd,
            _ => self.expected("option", self.peek_token())?,
        };
        Ok(ret)
    }

    fn parse_file_size(&mut self) -> Result<FileSize, ParserError> {
        let size = self.parse_number_value()?.value;
        let unit = self.maybe_parse_file_size_unit();
        Ok(FileSize { size, unit })
    }

    fn maybe_parse_file_size_unit(&mut self) -> Option<FileSizeUnit> {
        match self.parse_one_of_keywords(&[Keyword::MB, Keyword::GB]) {
            Some(Keyword::MB) => Some(FileSizeUnit::MB),
            Some(Keyword::GB) => Some(FileSizeUnit::GB),
            _ => None,
        }
    }

    fn parse_iam_role_kind(&mut self) -> Result<IamRoleKind, ParserError> {
        if self.parse_keyword(Keyword::DEFAULT) {
            Ok(IamRoleKind::Default)
        } else {
            let arn = self.parse_literal_string()?;
            Ok(IamRoleKind::Arn(arn))
        }
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
                    self.parse_comma_separated(|p| p.parse_identifier())?,
                )
            }
            Some(Keyword::FORCE) if self.parse_keywords(&[Keyword::QUOTE]) => {
                CopyLegacyCsvOption::ForceQuote(
                    self.parse_comma_separated(|p| p.parse_identifier())?,
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
                .map_or(Location { line: 0, column: 0 }, |t| t.span.start);
            return parser_err!(format!("Expect a char, found {s:?}"), loc);
        }
        Ok(s.chars().next().unwrap())
    }

    /// Parse a tab separated values in
    /// COPY payload
    pub fn parse_tsv(&mut self) -> Vec<Option<String>> {
        self.parse_tab_value()
    }

    /// Parse a single tab-separated value row used by `COPY` payload parsing.
    pub fn parse_tab_value(&mut self) -> Vec<Option<String>> {
        let mut values = vec![];
        let mut content = String::new();
        while let Some(t) = self.next_token_no_skip().map(|t| &t.token) {
            match t {
                Token::Whitespace(Whitespace::Tab) => {
                    values.push(Some(core::mem::take(&mut content)));
                }
                Token::Whitespace(Whitespace::Newline) => {
                    values.push(Some(core::mem::take(&mut content)));
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
    pub fn parse_value(&mut self) -> Result<ValueWithSpan, ParserError> {
        let next_token = self.next_token();
        let span = next_token.span;
        let ok_value = |value: Value| Ok(value.with_span(span));
        match next_token.token {
            Token::Word(w) => match w.keyword {
                Keyword::TRUE if self.dialect.supports_boolean_literals() => {
                    ok_value(Value::Boolean(true))
                }
                Keyword::FALSE if self.dialect.supports_boolean_literals() => {
                    ok_value(Value::Boolean(false))
                }
                Keyword::NULL => ok_value(Value::Null),
                Keyword::NoKeyword if w.quote_style.is_some() => match w.quote_style {
                    Some('"') => ok_value(Value::DoubleQuotedString(w.value)),
                    Some('\'') => ok_value(Value::SingleQuotedString(w.value)),
                    _ => self.expected(
                        "A value?",
                        TokenWithSpan {
                            token: Token::Word(w),
                            span,
                        },
                    )?,
                },
                _ => self.expected(
                    "a concrete value",
                    TokenWithSpan {
                        token: Token::Word(w),
                        span,
                    },
                ),
            },
            // The call to n.parse() returns a bigdecimal when the
            // bigdecimal feature is enabled, and is otherwise a no-op
            // (i.e., it returns the input string).
            Token::Number(n, l) => ok_value(Value::Number(Self::parse(n, span.start)?, l)),
            Token::SingleQuotedString(ref s) => ok_value(Value::SingleQuotedString(
                self.maybe_concat_string_literal(s.to_string()),
            )),
            Token::DoubleQuotedString(ref s) => ok_value(Value::DoubleQuotedString(
                self.maybe_concat_string_literal(s.to_string()),
            )),
            Token::TripleSingleQuotedString(ref s) => {
                ok_value(Value::TripleSingleQuotedString(s.to_string()))
            }
            Token::TripleDoubleQuotedString(ref s) => {
                ok_value(Value::TripleDoubleQuotedString(s.to_string()))
            }
            Token::DollarQuotedString(ref s) => ok_value(Value::DollarQuotedString(s.clone())),
            Token::SingleQuotedByteStringLiteral(ref s) => {
                ok_value(Value::SingleQuotedByteStringLiteral(s.clone()))
            }
            Token::DoubleQuotedByteStringLiteral(ref s) => {
                ok_value(Value::DoubleQuotedByteStringLiteral(s.clone()))
            }
            Token::TripleSingleQuotedByteStringLiteral(ref s) => {
                ok_value(Value::TripleSingleQuotedByteStringLiteral(s.clone()))
            }
            Token::TripleDoubleQuotedByteStringLiteral(ref s) => {
                ok_value(Value::TripleDoubleQuotedByteStringLiteral(s.clone()))
            }
            Token::SingleQuotedRawStringLiteral(ref s) => {
                ok_value(Value::SingleQuotedRawStringLiteral(s.clone()))
            }
            Token::DoubleQuotedRawStringLiteral(ref s) => {
                ok_value(Value::DoubleQuotedRawStringLiteral(s.clone()))
            }
            Token::TripleSingleQuotedRawStringLiteral(ref s) => {
                ok_value(Value::TripleSingleQuotedRawStringLiteral(s.clone()))
            }
            Token::TripleDoubleQuotedRawStringLiteral(ref s) => {
                ok_value(Value::TripleDoubleQuotedRawStringLiteral(s.clone()))
            }
            Token::NationalStringLiteral(ref s) => {
                ok_value(Value::NationalStringLiteral(s.to_string()))
            }
            Token::QuoteDelimitedStringLiteral(v) => {
                ok_value(Value::QuoteDelimitedStringLiteral(v))
            }
            Token::NationalQuoteDelimitedStringLiteral(v) => {
                ok_value(Value::NationalQuoteDelimitedStringLiteral(v))
            }
            Token::EscapedStringLiteral(ref s) => {
                ok_value(Value::EscapedStringLiteral(s.to_string()))
            }
            Token::UnicodeStringLiteral(ref s) => {
                ok_value(Value::UnicodeStringLiteral(s.to_string()))
            }
            Token::HexStringLiteral(ref s) => ok_value(Value::HexStringLiteral(s.to_string())),
            Token::Placeholder(ref s) => ok_value(Value::Placeholder(s.to_string())),
            tok @ Token::Colon | tok @ Token::AtSign => {
                // 1. Not calling self.parse_identifier(false)?
                //    because only in placeholder we want to check
                //    numbers as idfentifies.  This because snowflake
                //    allows numbers as placeholders
                // 2. Not calling self.next_token() to enforce `tok`
                //    be followed immediately by a word/number, ie.
                //    without any whitespace in between
                let next_token = self.next_token_no_skip().unwrap_or(&EOF_TOKEN).clone();
                let ident = match next_token.token {
                    Token::Word(w) => Ok(w.into_ident(next_token.span)),
                    Token::Number(w, false) => Ok(Ident::with_span(next_token.span, w)),
                    _ => self.expected("placeholder", next_token),
                }?;
                Ok(Value::Placeholder(format!("{tok}{}", ident.value))
                    .with_span(Span::new(span.start, ident.span.end)))
            }
            unexpected => self.expected(
                "a value",
                TokenWithSpan {
                    token: unexpected,
                    span,
                },
            ),
        }
    }

    fn maybe_concat_string_literal(&mut self, mut str: String) -> String {
        if self.dialect.supports_string_literal_concatenation() {
            while let Token::SingleQuotedString(ref s) | Token::DoubleQuotedString(ref s) =
                self.peek_token_ref().token
            {
                str.push_str(s);
                self.advance_token();
            }
        } else if self
            .dialect
            .supports_string_literal_concatenation_with_newline()
        {
            // We are iterating over tokens including whitespaces, to identify
            // string literals separated by newlines so we can concatenate them.
            let mut after_newline = false;
            loop {
                match self.peek_token_no_skip().token {
                    Token::Whitespace(Whitespace::Newline) => {
                        after_newline = true;
                        self.next_token_no_skip();
                    }
                    Token::Whitespace(_) => {
                        self.next_token_no_skip();
                    }
                    Token::SingleQuotedString(ref s) | Token::DoubleQuotedString(ref s)
                        if after_newline =>
                    {
                        str.push_str(s.clone().as_str());
                        self.next_token_no_skip();
                        after_newline = false;
                    }
                    _ => break,
                }
            }
        }

        str
    }

    /// Parse an unsigned numeric literal
    pub fn parse_number_value(&mut self) -> Result<ValueWithSpan, ParserError> {
        let value_wrapper = self.parse_value()?;
        match &value_wrapper.value {
            Value::Number(_, _) => Ok(value_wrapper),
            Value::Placeholder(_) => Ok(value_wrapper),
            _ => {
                self.prev_token();
                self.expected("literal number", self.peek_token())
            }
        }
    }

    /// Parse a numeric literal as an expression. Returns a [`Expr::UnaryOp`] if the number is signed,
    /// otherwise returns a [`Expr::Value`]
    pub fn parse_number(&mut self) -> Result<Expr, ParserError> {
        let next_token = self.next_token();
        match next_token.token {
            Token::Plus => Ok(Expr::UnaryOp {
                op: UnaryOperator::Plus,
                expr: Box::new(Expr::Value(self.parse_number_value()?)),
            }),
            Token::Minus => Ok(Expr::UnaryOp {
                op: UnaryOperator::Minus,
                expr: Box::new(Expr::Value(self.parse_number_value()?)),
            }),
            _ => {
                self.prev_token();
                Ok(Expr::Value(self.parse_number_value()?))
            }
        }
    }

    fn parse_introduced_string_expr(&mut self) -> Result<Expr, ParserError> {
        let next_token = self.next_token();
        let span = next_token.span;
        match next_token.token {
            Token::SingleQuotedString(ref s) => Ok(Expr::Value(
                Value::SingleQuotedString(s.to_string()).with_span(span),
            )),
            Token::DoubleQuotedString(ref s) => Ok(Expr::Value(
                Value::DoubleQuotedString(s.to_string()).with_span(span),
            )),
            Token::HexStringLiteral(ref s) => Ok(Expr::Value(
                Value::HexStringLiteral(s.to_string()).with_span(span),
            )),
            unexpected => self.expected(
                "a string value",
                TokenWithSpan {
                    token: unexpected,
                    span,
                },
            ),
        }
    }

    /// Parse an unsigned literal integer/long
    pub fn parse_literal_uint(&mut self) -> Result<u64, ParserError> {
        let next_token = self.next_token();
        match next_token.token {
            Token::Number(s, _) => Self::parse::<u64>(s, next_token.span.start),
            _ => self.expected("literal int", next_token),
        }
    }

    /// Parse the body of a `CREATE FUNCTION` specified as a string.
    /// e.g. `CREATE FUNCTION ... AS $$ body $$`.
    fn parse_create_function_body_string(&mut self) -> Result<CreateFunctionBody, ParserError> {
        let parse_string_expr = |parser: &mut Parser| -> Result<Expr, ParserError> {
            let peek_token = parser.peek_token();
            let span = peek_token.span;
            match peek_token.token {
                Token::DollarQuotedString(s) if dialect_of!(parser is PostgreSqlDialect | GenericDialect) =>
                {
                    parser.next_token();
                    Ok(Expr::Value(Value::DollarQuotedString(s).with_span(span)))
                }
                _ => Ok(Expr::Value(
                    Value::SingleQuotedString(parser.parse_literal_string()?).with_span(span),
                )),
            }
        };

        Ok(CreateFunctionBody::AsBeforeOptions {
            body: parse_string_expr(self)?,
            link_symbol: if self.consume_token(&Token::Comma) {
                Some(parse_string_expr(self)?)
            } else {
                None
            },
        })
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

    /// Parse a boolean string
    pub(crate) fn parse_boolean_string(&mut self) -> Result<bool, ParserError> {
        match self.parse_one_of_keywords(&[Keyword::TRUE, Keyword::FALSE]) {
            Some(Keyword::TRUE) => Ok(true),
            Some(Keyword::FALSE) => Ok(false),
            _ => self.expected("TRUE or FALSE", self.peek_token()),
        }
    }

    /// Parse a literal unicode normalization clause
    pub fn parse_unicode_is_normalized(&mut self, expr: Expr) -> Result<Expr, ParserError> {
        let neg = self.parse_keyword(Keyword::NOT);
        let normalized_form = self.maybe_parse(|parser| {
            match parser.parse_one_of_keywords(&[
                Keyword::NFC,
                Keyword::NFD,
                Keyword::NFKC,
                Keyword::NFKD,
            ]) {
                Some(Keyword::NFC) => Ok(NormalizationForm::NFC),
                Some(Keyword::NFD) => Ok(NormalizationForm::NFD),
                Some(Keyword::NFKC) => Ok(NormalizationForm::NFKC),
                Some(Keyword::NFKD) => Ok(NormalizationForm::NFKD),
                _ => parser.expected("unicode normalization form", parser.peek_token()),
            }
        })?;
        if self.parse_keyword(Keyword::NORMALIZED) {
            return Ok(Expr::IsNormalized {
                expr: Box::new(expr),
                form: normalized_form,
                negated: neg,
            });
        }
        self.expected("unicode normalization form", self.peek_token())
    }

    /// Parse parenthesized enum members, used with `ENUM(...)` type definitions.
    pub fn parse_enum_values(&mut self) -> Result<Vec<EnumMember>, ParserError> {
        self.expect_token(&Token::LParen)?;
        let values = self.parse_comma_separated(|parser| {
            let name = parser.parse_literal_string()?;
            let e = if parser.consume_token(&Token::Eq) {
                let value = parser.parse_number()?;
                EnumMember::NamedValue(name, value)
            } else {
                EnumMember::Name(name)
            };
            Ok(e)
        })?;
        self.expect_token(&Token::RParen)?;

        Ok(values)
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
        let dialect = self.dialect;
        self.advance_token();
        let next_token = self.get_current_token();
        let next_token_index = self.get_current_index();

        let mut trailing_bracket: MatchedTrailingBracket = false.into();
        let mut data = match &next_token.token {
            Token::Word(w) => match w.keyword {
                Keyword::BOOLEAN => Ok(DataType::Boolean),
                Keyword::BOOL => Ok(DataType::Bool),
                Keyword::FLOAT => {
                    let precision = self.parse_exact_number_optional_precision_scale()?;

                    if self.parse_keyword(Keyword::UNSIGNED) {
                        Ok(DataType::FloatUnsigned(precision))
                    } else {
                        Ok(DataType::Float(precision))
                    }
                }
                Keyword::REAL => {
                    if self.parse_keyword(Keyword::UNSIGNED) {
                        Ok(DataType::RealUnsigned)
                    } else {
                        Ok(DataType::Real)
                    }
                }
                Keyword::FLOAT4 => Ok(DataType::Float4),
                Keyword::FLOAT32 => Ok(DataType::Float32),
                Keyword::FLOAT64 => Ok(DataType::Float64),
                Keyword::FLOAT8 => Ok(DataType::Float8),
                Keyword::DOUBLE => {
                    if self.parse_keyword(Keyword::PRECISION) {
                        if self.parse_keyword(Keyword::UNSIGNED) {
                            Ok(DataType::DoublePrecisionUnsigned)
                        } else {
                            Ok(DataType::DoublePrecision)
                        }
                    } else {
                        let precision = self.parse_exact_number_optional_precision_scale()?;

                        if self.parse_keyword(Keyword::UNSIGNED) {
                            Ok(DataType::DoubleUnsigned(precision))
                        } else {
                            Ok(DataType::Double(precision))
                        }
                    }
                }
                Keyword::TINYINT => {
                    let optional_precision = self.parse_optional_precision();
                    if self.parse_keyword(Keyword::UNSIGNED) {
                        Ok(DataType::TinyIntUnsigned(optional_precision?))
                    } else {
                        if dialect.supports_data_type_signed_suffix() {
                            let _ = self.parse_keyword(Keyword::SIGNED);
                        }
                        Ok(DataType::TinyInt(optional_precision?))
                    }
                }
                Keyword::INT2 => {
                    let optional_precision = self.parse_optional_precision();
                    if self.parse_keyword(Keyword::UNSIGNED) {
                        Ok(DataType::Int2Unsigned(optional_precision?))
                    } else {
                        Ok(DataType::Int2(optional_precision?))
                    }
                }
                Keyword::SMALLINT => {
                    let optional_precision = self.parse_optional_precision();
                    if self.parse_keyword(Keyword::UNSIGNED) {
                        Ok(DataType::SmallIntUnsigned(optional_precision?))
                    } else {
                        if dialect.supports_data_type_signed_suffix() {
                            let _ = self.parse_keyword(Keyword::SIGNED);
                        }
                        Ok(DataType::SmallInt(optional_precision?))
                    }
                }
                Keyword::MEDIUMINT => {
                    let optional_precision = self.parse_optional_precision();
                    if self.parse_keyword(Keyword::UNSIGNED) {
                        Ok(DataType::MediumIntUnsigned(optional_precision?))
                    } else {
                        if dialect.supports_data_type_signed_suffix() {
                            let _ = self.parse_keyword(Keyword::SIGNED);
                        }
                        Ok(DataType::MediumInt(optional_precision?))
                    }
                }
                Keyword::INT => {
                    let optional_precision = self.parse_optional_precision();
                    if self.parse_keyword(Keyword::UNSIGNED) {
                        Ok(DataType::IntUnsigned(optional_precision?))
                    } else {
                        if dialect.supports_data_type_signed_suffix() {
                            let _ = self.parse_keyword(Keyword::SIGNED);
                        }
                        Ok(DataType::Int(optional_precision?))
                    }
                }
                Keyword::INT4 => {
                    let optional_precision = self.parse_optional_precision();
                    if self.parse_keyword(Keyword::UNSIGNED) {
                        Ok(DataType::Int4Unsigned(optional_precision?))
                    } else {
                        Ok(DataType::Int4(optional_precision?))
                    }
                }
                Keyword::INT8 => {
                    let optional_precision = self.parse_optional_precision();
                    if self.parse_keyword(Keyword::UNSIGNED) {
                        Ok(DataType::Int8Unsigned(optional_precision?))
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
                        Ok(DataType::IntegerUnsigned(optional_precision?))
                    } else {
                        if dialect.supports_data_type_signed_suffix() {
                            let _ = self.parse_keyword(Keyword::SIGNED);
                        }
                        Ok(DataType::Integer(optional_precision?))
                    }
                }
                Keyword::BIGINT => {
                    let optional_precision = self.parse_optional_precision();
                    if self.parse_keyword(Keyword::UNSIGNED) {
                        Ok(DataType::BigIntUnsigned(optional_precision?))
                    } else {
                        if dialect.supports_data_type_signed_suffix() {
                            let _ = self.parse_keyword(Keyword::SIGNED);
                        }
                        Ok(DataType::BigInt(optional_precision?))
                    }
                }
                Keyword::HUGEINT => Ok(DataType::HugeInt),
                Keyword::UBIGINT => Ok(DataType::UBigInt),
                Keyword::UHUGEINT => Ok(DataType::UHugeInt),
                Keyword::USMALLINT => Ok(DataType::USmallInt),
                Keyword::UTINYINT => Ok(DataType::UTinyInt),
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
                Keyword::VARBINARY => Ok(DataType::Varbinary(self.parse_optional_binary_length()?)),
                Keyword::BLOB => Ok(DataType::Blob(self.parse_optional_precision()?)),
                Keyword::TINYBLOB => Ok(DataType::TinyBlob),
                Keyword::MEDIUMBLOB => Ok(DataType::MediumBlob),
                Keyword::LONGBLOB => Ok(DataType::LongBlob),
                Keyword::BYTES => Ok(DataType::Bytes(self.parse_optional_precision()?)),
                Keyword::BIT => {
                    if self.parse_keyword(Keyword::VARYING) {
                        Ok(DataType::BitVarying(self.parse_optional_precision()?))
                    } else {
                        Ok(DataType::Bit(self.parse_optional_precision()?))
                    }
                }
                Keyword::VARBIT => Ok(DataType::VarBit(self.parse_optional_precision()?)),
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
                Keyword::TIMESTAMP_NTZ => {
                    Ok(DataType::TimestampNtz(self.parse_optional_precision()?))
                }
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
                Keyword::INTERVAL => {
                    if self.dialect.supports_interval_options() {
                        let fields = self.maybe_parse_optional_interval_fields()?;
                        let precision = self.parse_optional_precision()?;
                        Ok(DataType::Interval { fields, precision })
                    } else {
                        Ok(DataType::Interval {
                            fields: None,
                            precision: None,
                        })
                    }
                }
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
                Keyword::TINYTEXT => Ok(DataType::TinyText),
                Keyword::MEDIUMTEXT => Ok(DataType::MediumText),
                Keyword::LONGTEXT => Ok(DataType::LongText),
                Keyword::BYTEA => Ok(DataType::Bytea),
                Keyword::NUMERIC => Ok(DataType::Numeric(
                    self.parse_exact_number_optional_precision_scale()?,
                )),
                Keyword::DECIMAL => {
                    let precision = self.parse_exact_number_optional_precision_scale()?;

                    if self.parse_keyword(Keyword::UNSIGNED) {
                        Ok(DataType::DecimalUnsigned(precision))
                    } else {
                        Ok(DataType::Decimal(precision))
                    }
                }
                Keyword::DEC => {
                    let precision = self.parse_exact_number_optional_precision_scale()?;

                    if self.parse_keyword(Keyword::UNSIGNED) {
                        Ok(DataType::DecUnsigned(precision))
                    } else {
                        Ok(DataType::Dec(precision))
                    }
                }
                Keyword::BIGNUMERIC => Ok(DataType::BigNumeric(
                    self.parse_exact_number_optional_precision_scale()?,
                )),
                Keyword::BIGDECIMAL => Ok(DataType::BigDecimal(
                    self.parse_exact_number_optional_precision_scale()?,
                )),
                Keyword::ENUM => Ok(DataType::Enum(self.parse_enum_values()?, None)),
                Keyword::ENUM8 => Ok(DataType::Enum(self.parse_enum_values()?, Some(8))),
                Keyword::ENUM16 => Ok(DataType::Enum(self.parse_enum_values()?, Some(16))),
                Keyword::SET => Ok(DataType::Set(self.parse_string_values()?)),
                Keyword::ARRAY => {
                    if self.dialect.supports_array_typedef_without_element_type() {
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
                Keyword::STRUCT if dialect_is!(dialect is DuckDbDialect) => {
                    self.prev_token();
                    let field_defs = self.parse_duckdb_struct_type_def()?;
                    Ok(DataType::Struct(field_defs, StructBracketKind::Parentheses))
                }
                Keyword::STRUCT if dialect_is!(dialect is BigQueryDialect | GenericDialect) => {
                    self.prev_token();
                    let (field_defs, _trailing_bracket) =
                        self.parse_struct_type_def(Self::parse_struct_field_def)?;
                    trailing_bracket = _trailing_bracket;
                    Ok(DataType::Struct(
                        field_defs,
                        StructBracketKind::AngleBrackets,
                    ))
                }
                Keyword::UNION if dialect_is!(dialect is DuckDbDialect | GenericDialect) => {
                    self.prev_token();
                    let fields = self.parse_union_type_def()?;
                    Ok(DataType::Union(fields))
                }
                Keyword::NULLABLE if dialect_is!(dialect is ClickHouseDialect | GenericDialect) => {
                    Ok(self.parse_sub_type(DataType::Nullable)?)
                }
                Keyword::LOWCARDINALITY if dialect_is!(dialect is ClickHouseDialect | GenericDialect) => {
                    Ok(self.parse_sub_type(DataType::LowCardinality)?)
                }
                Keyword::MAP if dialect_is!(dialect is ClickHouseDialect | GenericDialect) => {
                    self.prev_token();
                    let (key_data_type, value_data_type) = self.parse_click_house_map_def()?;
                    Ok(DataType::Map(
                        Box::new(key_data_type),
                        Box::new(value_data_type),
                    ))
                }
                Keyword::NESTED if dialect_is!(dialect is ClickHouseDialect | GenericDialect) => {
                    self.expect_token(&Token::LParen)?;
                    let field_defs = self.parse_comma_separated(Parser::parse_column_def)?;
                    self.expect_token(&Token::RParen)?;
                    Ok(DataType::Nested(field_defs))
                }
                Keyword::TUPLE if dialect_is!(dialect is ClickHouseDialect | GenericDialect) => {
                    self.prev_token();
                    let field_defs = self.parse_click_house_tuple_def()?;
                    Ok(DataType::Tuple(field_defs))
                }
                Keyword::TRIGGER => Ok(DataType::Trigger),
                Keyword::ANY if self.peek_keyword(Keyword::TYPE) => {
                    let _ = self.parse_keyword(Keyword::TYPE);
                    Ok(DataType::AnyType)
                }
                Keyword::TABLE => {
                    // an LParen after the TABLE keyword indicates that table columns are being defined
                    // whereas no LParen indicates an anonymous table expression will be returned
                    if self.peek_token() == Token::LParen {
                        let columns = self.parse_returns_table_columns()?;
                        Ok(DataType::Table(Some(columns)))
                    } else {
                        Ok(DataType::Table(None))
                    }
                }
                Keyword::SIGNED => {
                    if self.parse_keyword(Keyword::INTEGER) {
                        Ok(DataType::SignedInteger)
                    } else {
                        Ok(DataType::Signed)
                    }
                }
                Keyword::UNSIGNED => {
                    if self.parse_keyword(Keyword::INTEGER) {
                        Ok(DataType::UnsignedInteger)
                    } else {
                        Ok(DataType::Unsigned)
                    }
                }
                Keyword::TSVECTOR if dialect_is!(dialect is PostgreSqlDialect | GenericDialect) => {
                    Ok(DataType::TsVector)
                }
                Keyword::TSQUERY if dialect_is!(dialect is PostgreSqlDialect | GenericDialect) => {
                    Ok(DataType::TsQuery)
                }
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
            _ => self.expected_at("a data type name", next_token_index),
        }?;

        if self.dialect.supports_array_typedef_with_brackets() {
            while self.consume_token(&Token::LBracket) {
                // Parse optional array data type size
                let size = self.maybe_parse(|p| p.parse_literal_uint())?;
                self.expect_token(&Token::RBracket)?;
                data = DataType::Array(ArrayElemTypeDef::SquareBracket(Box::new(data), size))
            }
        }
        Ok((data, trailing_bracket))
    }

    fn parse_returns_table_column(&mut self) -> Result<ColumnDef, ParserError> {
        self.parse_column_def()
    }

    fn parse_returns_table_columns(&mut self) -> Result<Vec<ColumnDef>, ParserError> {
        self.expect_token(&Token::LParen)?;
        let columns = self.parse_comma_separated(Parser::parse_returns_table_column)?;
        self.expect_token(&Token::RParen)?;
        Ok(columns)
    }

    /// Parse a parenthesized, comma-separated list of single-quoted strings.
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
        let ident = self.parse_identifier()?;
        self.expect_keyword_is(Keyword::AS)?;
        let alias = self.parse_identifier()?;
        Ok(IdentWithAlias { ident, alias })
    }

    /// Parse `identifier [AS] identifier` where the AS keyword is optional
    fn parse_identifier_with_optional_alias(&mut self) -> Result<IdentWithAlias, ParserError> {
        let ident = self.parse_identifier()?;
        let _after_as = self.parse_keyword(Keyword::AS);
        let alias = self.parse_identifier()?;
        Ok(IdentWithAlias { ident, alias })
    }

    /// Parse comma-separated list of parenthesized queries for pipe operators
    fn parse_pipe_operator_queries(&mut self) -> Result<Vec<Query>, ParserError> {
        self.parse_comma_separated(|parser| {
            parser.expect_token(&Token::LParen)?;
            let query = parser.parse_query()?;
            parser.expect_token(&Token::RParen)?;
            Ok(*query)
        })
    }

    /// Parse set quantifier for pipe operators that require DISTINCT. E.g. INTERSECT and EXCEPT
    fn parse_distinct_required_set_quantifier(
        &mut self,
        operator_name: &str,
    ) -> Result<SetQuantifier, ParserError> {
        let quantifier = self.parse_set_quantifier(&Some(SetOperator::Intersect));
        match quantifier {
            SetQuantifier::Distinct | SetQuantifier::DistinctByName => Ok(quantifier),
            _ => Err(ParserError::ParserError(format!(
                "{operator_name} pipe operator requires DISTINCT modifier",
            ))),
        }
    }

    /// Parse optional identifier alias (with or without AS keyword)
    fn parse_identifier_optional_alias(&mut self) -> Result<Option<Ident>, ParserError> {
        if self.parse_keyword(Keyword::AS) {
            Ok(Some(self.parse_identifier()?))
        } else {
            // Check if the next token is an identifier (implicit alias)
            self.maybe_parse(|parser| parser.parse_identifier())
        }
    }

    /// Optionally parses an alias for a select list item
    fn maybe_parse_select_item_alias(&mut self) -> Result<Option<Ident>, ParserError> {
        fn validator(explicit: bool, kw: &Keyword, parser: &mut Parser) -> bool {
            parser.dialect.is_select_item_alias(explicit, kw, parser)
        }
        self.parse_optional_alias_inner(None, validator)
    }

    /// Optionally parses an alias for a table like in `... FROM generate_series(1, 10) AS t (col)`.
    /// In this case, the alias is allowed to optionally name the columns in the table, in
    /// addition to the table itself.
    pub fn maybe_parse_table_alias(&mut self) -> Result<Option<TableAlias>, ParserError> {
        fn validator(explicit: bool, kw: &Keyword, parser: &mut Parser) -> bool {
            parser.dialect.is_table_factor_alias(explicit, kw, parser)
        }
        let explicit = self.peek_keyword(Keyword::AS);
        match self.parse_optional_alias_inner(None, validator)? {
            Some(name) => {
                let columns = self.parse_table_alias_column_defs()?;
                Ok(Some(TableAlias {
                    explicit,
                    name,
                    columns,
                }))
            }
            None => Ok(None),
        }
    }

    fn parse_table_index_hints(&mut self) -> Result<Vec<TableIndexHints>, ParserError> {
        let mut hints = vec![];
        while let Some(hint_type) =
            self.parse_one_of_keywords(&[Keyword::USE, Keyword::IGNORE, Keyword::FORCE])
        {
            let hint_type = match hint_type {
                Keyword::USE => TableIndexHintType::Use,
                Keyword::IGNORE => TableIndexHintType::Ignore,
                Keyword::FORCE => TableIndexHintType::Force,
                _ => {
                    return self.expected(
                        "expected to match USE/IGNORE/FORCE keyword",
                        self.peek_token(),
                    )
                }
            };
            let index_type = match self.parse_one_of_keywords(&[Keyword::INDEX, Keyword::KEY]) {
                Some(Keyword::INDEX) => TableIndexType::Index,
                Some(Keyword::KEY) => TableIndexType::Key,
                _ => {
                    return self.expected("expected to match INDEX/KEY keyword", self.peek_token())
                }
            };
            let for_clause = if self.parse_keyword(Keyword::FOR) {
                let clause = if self.parse_keyword(Keyword::JOIN) {
                    TableIndexHintForClause::Join
                } else if self.parse_keywords(&[Keyword::ORDER, Keyword::BY]) {
                    TableIndexHintForClause::OrderBy
                } else if self.parse_keywords(&[Keyword::GROUP, Keyword::BY]) {
                    TableIndexHintForClause::GroupBy
                } else {
                    return self.expected(
                        "expected to match FOR/ORDER BY/GROUP BY table hint in for clause",
                        self.peek_token(),
                    );
                };
                Some(clause)
            } else {
                None
            };

            self.expect_token(&Token::LParen)?;
            let index_names = if self.peek_token().token != Token::RParen {
                self.parse_comma_separated(Parser::parse_identifier)?
            } else {
                vec![]
            };
            self.expect_token(&Token::RParen)?;
            hints.push(TableIndexHints {
                hint_type,
                index_type,
                for_clause,
                index_names,
            });
        }
        Ok(hints)
    }

    /// Wrapper for parse_optional_alias_inner, left for backwards-compatibility
    /// but new flows should use the context-specific methods such as `maybe_parse_select_item_alias`
    /// and `maybe_parse_table_alias`.
    pub fn parse_optional_alias(
        &mut self,
        reserved_kwds: &[Keyword],
    ) -> Result<Option<Ident>, ParserError> {
        fn validator(_explicit: bool, _kw: &Keyword, _parser: &mut Parser) -> bool {
            false
        }
        self.parse_optional_alias_inner(Some(reserved_kwds), validator)
    }

    /// Parses an optional alias after a SQL element such as a select list item
    /// or a table name.
    ///
    /// This method accepts an optional list of reserved keywords or a function
    /// to call to validate if a keyword should be parsed as an alias, to allow
    /// callers to customize the parsing logic based on their context.
    fn parse_optional_alias_inner<F>(
        &mut self,
        reserved_kwds: Option<&[Keyword]>,
        validator: F,
    ) -> Result<Option<Ident>, ParserError>
    where
        F: Fn(bool, &Keyword, &mut Parser) -> bool,
    {
        let after_as = self.parse_keyword(Keyword::AS);

        let next_token = self.next_token();
        match next_token.token {
            // Accepts a keyword as an alias if the AS keyword explicitly indicate an alias or if the
            // caller provided a list of reserved keywords and the keyword is not on that list.
            Token::Word(w)
                if reserved_kwds.is_some()
                    && (after_as || reserved_kwds.is_some_and(|x| !x.contains(&w.keyword))) =>
            {
                Ok(Some(w.into_ident(next_token.span)))
            }
            // Accepts a keyword as alias based on the caller's context, such as to what SQL element
            // this word is a potential alias of using the validator call-back. This allows for
            // dialect-specific logic.
            Token::Word(w) if validator(after_as, &w.keyword, self) => {
                Ok(Some(w.into_ident(next_token.span)))
            }
            // For backwards-compatibility, we accept quoted strings as aliases regardless of the context.
            Token::SingleQuotedString(s) => Ok(Some(Ident::with_quote('\'', s))),
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

    /// Parse an optional `GROUP BY` clause, returning `Some(GroupByExpr)` when present.
    pub fn parse_optional_group_by(&mut self) -> Result<Option<GroupByExpr>, ParserError> {
        if self.parse_keywords(&[Keyword::GROUP, Keyword::BY]) {
            let expressions = if self.parse_keyword(Keyword::ALL) {
                None
            } else {
                Some(self.parse_comma_separated(Parser::parse_group_by_expr)?)
            };

            let mut modifiers = vec![];
            if self.dialect.supports_group_by_with_modifier() {
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
                                self.peek_token().span.start
                            )
                        }
                    });
                }
            }
            if self.parse_keywords(&[Keyword::GROUPING, Keyword::SETS]) {
                self.expect_token(&Token::LParen)?;
                let result = self.parse_comma_separated(|p| {
                    if p.peek_token_ref().token == Token::LParen {
                        p.parse_tuple(true, true)
                    } else {
                        Ok(vec![p.parse_expr()?])
                    }
                })?;
                self.expect_token(&Token::RParen)?;
                modifiers.push(GroupByWithModifier::GroupingSets(Expr::GroupingSets(
                    result,
                )));
            };
            let group_by = match expressions {
                None => GroupByExpr::All(modifiers),
                Some(exprs) => GroupByExpr::Expressions(exprs, modifiers),
            };
            Ok(Some(group_by))
        } else {
            Ok(None)
        }
    }

    /// Parse an optional `ORDER BY` clause, returning `Some(OrderBy)` when present.
    pub fn parse_optional_order_by(&mut self) -> Result<Option<OrderBy>, ParserError> {
        if self.parse_keywords(&[Keyword::ORDER, Keyword::BY]) {
            let order_by =
                if self.dialect.supports_order_by_all() && self.parse_keyword(Keyword::ALL) {
                    let order_by_options = self.parse_order_by_options()?;
                    OrderBy {
                        kind: OrderByKind::All(order_by_options),
                        interpolate: None,
                    }
                } else {
                    let exprs = self.parse_comma_separated(Parser::parse_order_by_expr)?;
                    let interpolate = if self.dialect.supports_interpolate() {
                        self.parse_interpolations()?
                    } else {
                        None
                    };
                    OrderBy {
                        kind: OrderByKind::Expressions(exprs),
                        interpolate,
                    }
                };
            Ok(Some(order_by))
        } else {
            Ok(None)
        }
    }

    fn parse_optional_limit_clause(&mut self) -> Result<Option<LimitClause>, ParserError> {
        let mut offset = if self.parse_keyword(Keyword::OFFSET) {
            Some(self.parse_offset()?)
        } else {
            None
        };

        let (limit, limit_by) = if self.parse_keyword(Keyword::LIMIT) {
            let expr = self.parse_limit()?;

            if self.dialect.supports_limit_comma()
                && offset.is_none()
                && expr.is_some() // ALL not supported with comma
                && self.consume_token(&Token::Comma)
            {
                let offset = expr.ok_or_else(|| {
                    ParserError::ParserError(
                        "Missing offset for LIMIT <offset>, <limit>".to_string(),
                    )
                })?;
                return Ok(Some(LimitClause::OffsetCommaLimit {
                    offset,
                    limit: self.parse_expr()?,
                }));
            }

            let limit_by = if self.dialect.supports_limit_by() && self.parse_keyword(Keyword::BY) {
                Some(self.parse_comma_separated(Parser::parse_expr)?)
            } else {
                None
            };

            (Some(expr), limit_by)
        } else {
            (None, None)
        };

        if offset.is_none() && limit.is_some() && self.parse_keyword(Keyword::OFFSET) {
            offset = Some(self.parse_offset()?);
        }

        if offset.is_some() || (limit.is_some() && limit != Some(None)) || limit_by.is_some() {
            Ok(Some(LimitClause::LimitOffset {
                limit: limit.unwrap_or_default(),
                offset,
                limit_by: limit_by.unwrap_or_default(),
            }))
        } else {
            Ok(None)
        }
    }

    /// Parse a table object for insertion
    /// e.g. `some_database.some_table` or `FUNCTION some_table_func(...)`
    pub fn parse_table_object(&mut self) -> Result<TableObject, ParserError> {
        if self.dialect.supports_insert_table_function() && self.parse_keyword(Keyword::FUNCTION) {
            let fn_name = self.parse_object_name(false)?;
            self.parse_function_call(fn_name)
                .map(TableObject::TableFunction)
        } else {
            self.parse_object_name(false).map(TableObject::TableName)
        }
    }

    /// Parse a possibly qualified, possibly quoted identifier, e.g.
    /// `foo` or `myschema."table"
    ///
    /// The `in_table_clause` parameter indicates whether the object name is a table in a FROM, JOIN,
    /// or similar table clause. Currently, this is used only to support unquoted hyphenated identifiers
    /// in this context on BigQuery.
    pub fn parse_object_name(&mut self, in_table_clause: bool) -> Result<ObjectName, ParserError> {
        self.parse_object_name_inner(in_table_clause, false)
    }

    /// Parse a possibly qualified, possibly quoted identifier, e.g.
    /// `foo` or `myschema."table"
    ///
    /// The `in_table_clause` parameter indicates whether the object name is a table in a FROM, JOIN,
    /// or similar table clause. Currently, this is used only to support unquoted hyphenated identifiers
    /// in this context on BigQuery.
    ///
    /// The `allow_wildcards` parameter indicates whether to allow for wildcards in the object name
    /// e.g. *, *.*, `foo`.*, or "foo"."bar"
    fn parse_object_name_inner(
        &mut self,
        in_table_clause: bool,
        allow_wildcards: bool,
    ) -> Result<ObjectName, ParserError> {
        let mut parts = vec![];
        if dialect_of!(self is BigQueryDialect) && in_table_clause {
            loop {
                let (ident, end_with_period) = self.parse_unquoted_hyphenated_identifier()?;
                parts.push(ObjectNamePart::Identifier(ident));
                if !self.consume_token(&Token::Period) && !end_with_period {
                    break;
                }
            }
        } else {
            loop {
                if allow_wildcards && self.peek_token().token == Token::Mul {
                    let span = self.next_token().span;
                    parts.push(ObjectNamePart::Identifier(Ident {
                        value: Token::Mul.to_string(),
                        quote_style: None,
                        span,
                    }));
                } else if dialect_of!(self is BigQueryDialect) && in_table_clause {
                    let (ident, end_with_period) = self.parse_unquoted_hyphenated_identifier()?;
                    parts.push(ObjectNamePart::Identifier(ident));
                    if !self.consume_token(&Token::Period) && !end_with_period {
                        break;
                    }
                } else if self.dialect.supports_object_name_double_dot_notation()
                    && parts.len() == 1
                    && matches!(self.peek_token().token, Token::Period)
                {
                    // Empty string here means default schema
                    parts.push(ObjectNamePart::Identifier(Ident::new("")));
                } else {
                    let ident = self.parse_identifier()?;
                    let part = if self
                        .dialect
                        .is_identifier_generating_function_name(&ident, &parts)
                    {
                        self.expect_token(&Token::LParen)?;
                        let args: Vec<FunctionArg> =
                            self.parse_comma_separated0(Self::parse_function_args, Token::RParen)?;
                        self.expect_token(&Token::RParen)?;
                        ObjectNamePart::Function(ObjectNamePartFunction { name: ident, args })
                    } else {
                        ObjectNamePart::Identifier(ident)
                    };
                    parts.push(part);
                }

                if !self.consume_token(&Token::Period) {
                    break;
                }
            }
        }

        // BigQuery accepts any number of quoted identifiers of a table name.
        // https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#quoted_identifiers
        if dialect_of!(self is BigQueryDialect)
            && parts.iter().any(|part| {
                part.as_ident()
                    .is_some_and(|ident| ident.value.contains('.'))
            })
        {
            parts = parts
                .into_iter()
                .flat_map(|part| match part.as_ident() {
                    Some(ident) => ident
                        .value
                        .split('.')
                        .map(|value| {
                            ObjectNamePart::Identifier(Ident {
                                value: value.into(),
                                quote_style: ident.quote_style,
                                span: ident.span,
                            })
                        })
                        .collect::<Vec<_>>(),
                    None => vec![part],
                })
                .collect()
        }

        Ok(ObjectName(parts))
    }

    /// Parse identifiers
    pub fn parse_identifiers(&mut self) -> Result<Vec<Ident>, ParserError> {
        let mut idents = vec![];
        loop {
            let token = self.peek_token_ref();
            match &token.token {
                Token::Word(w) => {
                    idents.push(w.to_ident(token.span));
                }
                Token::EOF | Token::Eq | Token::SemiColon | Token::VerticalBarRightAngleBracket => {
                    break
                }
                _ => {}
            }
            self.advance_token();
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
        let next_token = self.next_token();
        match next_token.token {
            Token::Word(w) => idents.push(w.into_ident(next_token.span)),
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
                Token::Period => {
                    let next_token = self.next_token();
                    match next_token.token {
                        Token::Word(w) => idents.push(w.into_ident(next_token.span)),
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
                    }
                }
                Token::EOF => break,
                token => {
                    return Err(ParserError::ParserError(format!(
                        "Unexpected token in identifier: {token}"
                    )))?;
                }
            }
        }

        Ok(idents)
    }

    /// Parse a simple one-word identifier (possibly quoted, possibly a keyword)
    pub fn parse_identifier(&mut self) -> Result<Ident, ParserError> {
        let next_token = self.next_token();
        match next_token.token {
            Token::Word(w) => Ok(w.into_ident(next_token.span)),
            Token::SingleQuotedString(s) => Ok(Ident::with_quote('\'', s)),
            Token::DoubleQuotedString(s) => Ok(Ident::with_quote('\"', s)),
            _ => self.expected("identifier", next_token),
        }
    }

    /// On BigQuery, hyphens are permitted in unquoted identifiers inside of a FROM or
    /// TABLE clause.
    ///
    /// The first segment must be an ordinary unquoted identifier, e.g. it must not start
    /// with a digit. Subsequent segments are either must either be valid identifiers or
    /// integers, e.g. foo-123 is allowed, but foo-123a is not.
    ///
    /// [BigQuery-lexical](https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical)
    ///
    /// Return a tuple of the identifier and a boolean indicating it ends with a period.
    fn parse_unquoted_hyphenated_identifier(&mut self) -> Result<(Ident, bool), ParserError> {
        match self.peek_token().token {
            Token::Word(w) => {
                let quote_style_is_none = w.quote_style.is_none();
                let mut requires_whitespace = false;
                let mut ident = w.into_ident(self.next_token().span);
                if quote_style_is_none {
                    while matches!(self.peek_token_no_skip().token, Token::Minus) {
                        self.next_token();
                        ident.value.push('-');

                        let token = self
                            .next_token_no_skip()
                            .cloned()
                            .unwrap_or(TokenWithSpan::wrap(Token::EOF));
                        requires_whitespace = match token.token {
                            Token::Word(next_word) if next_word.quote_style.is_none() => {
                                ident.value.push_str(&next_word.value);
                                false
                            }
                            Token::Number(s, false) => {
                                // A number token can represent a decimal value ending with a period, e.g., `Number('123.')`.
                                // However, for an [ObjectName], it is part of a hyphenated identifier, e.g., `foo-123.bar`.
                                //
                                // If a number token is followed by a period, it is part of an [ObjectName].
                                // Return the identifier with `true` if the number token is followed by a period, indicating that
                                // parsing should continue for the next part of the hyphenated identifier.
                                if s.ends_with('.') {
                                    let Some(s) = s.split('.').next().filter(|s| {
                                        !s.is_empty() && s.chars().all(|c| c.is_ascii_digit())
                                    }) else {
                                        return self.expected(
                                            "continuation of hyphenated identifier",
                                            TokenWithSpan::new(Token::Number(s, false), token.span),
                                        );
                                    };
                                    ident.value.push_str(s);
                                    return Ok((ident, true));
                                } else {
                                    ident.value.push_str(&s);
                                }
                                // If next token is period, then it is part of an ObjectName and we don't expect whitespace
                                // after the number.
                                !matches!(self.peek_token().token, Token::Period)
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
                Ok((ident, false))
            }
            _ => Ok((self.parse_identifier()?, false)),
        }
    }

    /// Parses a parenthesized, comma-separated list of column definitions within a view.
    fn parse_view_columns(&mut self) -> Result<Vec<ViewColumnDef>, ParserError> {
        if self.consume_token(&Token::LParen) {
            if self.peek_token().token == Token::RParen {
                self.next_token();
                Ok(vec![])
            } else {
                let cols = self.parse_comma_separated_with_trailing_commas(
                    Parser::parse_view_column,
                    self.dialect.supports_column_definition_trailing_commas(),
                    Self::is_reserved_for_column_alias,
                )?;
                self.expect_token(&Token::RParen)?;
                Ok(cols)
            }
        } else {
            Ok(vec![])
        }
    }

    /// Parses a column definition within a view.
    fn parse_view_column(&mut self) -> Result<ViewColumnDef, ParserError> {
        let name = self.parse_identifier()?;
        let options = self.parse_view_column_options()?;
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

    fn parse_view_column_options(&mut self) -> Result<Option<ColumnOptions>, ParserError> {
        let mut options = Vec::new();
        loop {
            let option = self.parse_optional_column_option()?;
            if let Some(option) = option {
                options.push(option);
            } else {
                break;
            }
        }
        if options.is_empty() {
            Ok(None)
        } else if self.dialect.supports_space_separated_column_options() {
            Ok(Some(ColumnOptions::SpaceSeparated(options)))
        } else {
            Ok(Some(ColumnOptions::CommaSeparated(options)))
        }
    }

    /// Parses a parenthesized comma-separated list of unqualified, possibly quoted identifiers.
    /// For example: `(col1, "col 2", ...)`
    pub fn parse_parenthesized_column_list(
        &mut self,
        optional: IsOptional,
        allow_empty: bool,
    ) -> Result<Vec<Ident>, ParserError> {
        self.parse_parenthesized_column_list_inner(optional, allow_empty, |p| p.parse_identifier())
    }

    /// Parse a parenthesized list of compound identifiers as expressions.
    pub fn parse_parenthesized_compound_identifier_list(
        &mut self,
        optional: IsOptional,
        allow_empty: bool,
    ) -> Result<Vec<Expr>, ParserError> {
        self.parse_parenthesized_column_list_inner(optional, allow_empty, |p| {
            Ok(Expr::CompoundIdentifier(
                p.parse_period_separated(|p| p.parse_identifier())?,
            ))
        })
    }

    /// Parses a parenthesized comma-separated list of index columns, which can be arbitrary
    /// expressions with ordering information (and an opclass in some dialects).
    fn parse_parenthesized_index_column_list(&mut self) -> Result<Vec<IndexColumn>, ParserError> {
        self.parse_parenthesized_column_list_inner(Mandatory, false, |p| {
            p.parse_create_index_expr()
        })
    }

    /// Parses a parenthesized comma-separated list of qualified, possibly quoted identifiers.
    /// For example: `(db1.sc1.tbl1.col1, db1.sc1.tbl1."col 2", ...)`
    pub fn parse_parenthesized_qualified_column_list(
        &mut self,
        optional: IsOptional,
        allow_empty: bool,
    ) -> Result<Vec<ObjectName>, ParserError> {
        self.parse_parenthesized_column_list_inner(optional, allow_empty, |p| {
            p.parse_object_name(true)
        })
    }

    /// Parses a parenthesized comma-separated list of columns using
    /// the provided function to parse each element.
    fn parse_parenthesized_column_list_inner<F, T>(
        &mut self,
        optional: IsOptional,
        allow_empty: bool,
        mut f: F,
    ) -> Result<Vec<T>, ParserError>
    where
        F: FnMut(&mut Parser) -> Result<T, ParserError>,
    {
        if self.consume_token(&Token::LParen) {
            if allow_empty && self.peek_token().token == Token::RParen {
                self.next_token();
                Ok(vec![])
            } else {
                let cols = self.parse_comma_separated(|p| f(p))?;
                self.expect_token(&Token::RParen)?;
                Ok(cols)
            }
        } else if optional == Optional {
            Ok(vec![])
        } else {
            self.expected("a list of columns in parentheses", self.peek_token())
        }
    }

    /// Parses a parenthesized comma-separated list of table alias column definitions.
    fn parse_table_alias_column_defs(&mut self) -> Result<Vec<TableAliasColumnDef>, ParserError> {
        if self.consume_token(&Token::LParen) {
            let cols = self.parse_comma_separated(|p| {
                let name = p.parse_identifier()?;
                let data_type = p.maybe_parse(|p| p.parse_data_type())?;
                Ok(TableAliasColumnDef { name, data_type })
            })?;
            self.expect_token(&Token::RParen)?;
            Ok(cols)
        } else {
            Ok(vec![])
        }
    }

    /// Parse an unsigned precision value enclosed in parentheses, e.g. `(10)`.
    pub fn parse_precision(&mut self) -> Result<u64, ParserError> {
        self.expect_token(&Token::LParen)?;
        let n = self.parse_literal_uint()?;
        self.expect_token(&Token::RParen)?;
        Ok(n)
    }

    /// Parse an optional precision `(n)` and return it as `Some(n)` when present.
    pub fn parse_optional_precision(&mut self) -> Result<Option<u64>, ParserError> {
        if self.consume_token(&Token::LParen) {
            let n = self.parse_literal_uint()?;
            self.expect_token(&Token::RParen)?;
            Ok(Some(n))
        } else {
            Ok(None)
        }
    }

    fn maybe_parse_optional_interval_fields(
        &mut self,
    ) -> Result<Option<IntervalFields>, ParserError> {
        match self.parse_one_of_keywords(&[
            // Can be followed by `TO` option
            Keyword::YEAR,
            Keyword::DAY,
            Keyword::HOUR,
            Keyword::MINUTE,
            // No `TO` option
            Keyword::MONTH,
            Keyword::SECOND,
        ]) {
            Some(Keyword::YEAR) => {
                if self.peek_keyword(Keyword::TO) {
                    self.expect_keyword(Keyword::TO)?;
                    self.expect_keyword(Keyword::MONTH)?;
                    Ok(Some(IntervalFields::YearToMonth))
                } else {
                    Ok(Some(IntervalFields::Year))
                }
            }
            Some(Keyword::DAY) => {
                if self.peek_keyword(Keyword::TO) {
                    self.expect_keyword(Keyword::TO)?;
                    match self.expect_one_of_keywords(&[
                        Keyword::HOUR,
                        Keyword::MINUTE,
                        Keyword::SECOND,
                    ])? {
                        Keyword::HOUR => Ok(Some(IntervalFields::DayToHour)),
                        Keyword::MINUTE => Ok(Some(IntervalFields::DayToMinute)),
                        Keyword::SECOND => Ok(Some(IntervalFields::DayToSecond)),
                        _ => {
                            self.prev_token();
                            self.expected("HOUR, MINUTE, or SECOND", self.peek_token())
                        }
                    }
                } else {
                    Ok(Some(IntervalFields::Day))
                }
            }
            Some(Keyword::HOUR) => {
                if self.peek_keyword(Keyword::TO) {
                    self.expect_keyword(Keyword::TO)?;
                    match self.expect_one_of_keywords(&[Keyword::MINUTE, Keyword::SECOND])? {
                        Keyword::MINUTE => Ok(Some(IntervalFields::HourToMinute)),
                        Keyword::SECOND => Ok(Some(IntervalFields::HourToSecond)),
                        _ => {
                            self.prev_token();
                            self.expected("MINUTE or SECOND", self.peek_token())
                        }
                    }
                } else {
                    Ok(Some(IntervalFields::Hour))
                }
            }
            Some(Keyword::MINUTE) => {
                if self.peek_keyword(Keyword::TO) {
                    self.expect_keyword(Keyword::TO)?;
                    self.expect_keyword(Keyword::SECOND)?;
                    Ok(Some(IntervalFields::MinuteToSecond))
                } else {
                    Ok(Some(IntervalFields::Minute))
                }
            }
            Some(Keyword::MONTH) => Ok(Some(IntervalFields::Month)),
            Some(Keyword::SECOND) => Ok(Some(IntervalFields::Second)),
            Some(_) => {
                self.prev_token();
                self.expected(
                    "YEAR, MONTH, DAY, HOUR, MINUTE, or SECOND",
                    self.peek_token(),
                )
            }
            None => Ok(None),
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
        self.expect_keyword_is(Keyword::DATETIME64)?;
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

    /// Parse an optional character length specification `(n | MAX [CHARACTERS|OCTETS])`.
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

    /// Parse an optional binary length specification like `(n)`.
    pub fn parse_optional_binary_length(&mut self) -> Result<Option<BinaryLength>, ParserError> {
        if self.consume_token(&Token::LParen) {
            let binary_length = self.parse_binary_length()?;
            self.expect_token(&Token::RParen)?;
            Ok(Some(binary_length))
        } else {
            Ok(None)
        }
    }

    /// Parse a character length, handling `MAX` or integer lengths with optional units.
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

    /// Parse a binary length specification, returning `BinaryLength`.
    pub fn parse_binary_length(&mut self) -> Result<BinaryLength, ParserError> {
        if self.parse_keyword(Keyword::MAX) {
            return Ok(BinaryLength::Max);
        }
        let length = self.parse_literal_uint()?;
        Ok(BinaryLength::IntegerLength { length })
    }

    /// Parse an optional `(precision[, scale])` and return `(Option<precision>, Option<scale>)`.
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

    /// Parse exact-number precision/scale info like `(precision[, scale])` for decimal types.
    pub fn parse_exact_number_optional_precision_scale(
        &mut self,
    ) -> Result<ExactNumberInfo, ParserError> {
        if self.consume_token(&Token::LParen) {
            let precision = self.parse_literal_uint()?;
            let scale = if self.consume_token(&Token::Comma) {
                Some(self.parse_signed_integer()?)
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

    /// Parse an optionally signed integer literal.
    fn parse_signed_integer(&mut self) -> Result<i64, ParserError> {
        let is_negative = self.consume_token(&Token::Minus);

        if !is_negative {
            let _ = self.consume_token(&Token::Plus);
        }

        let current_token = self.peek_token_ref();
        match &current_token.token {
            Token::Number(s, _) => {
                let s = s.clone();
                let span_start = current_token.span.start;
                self.advance_token();
                let value = Self::parse::<i64>(s, span_start)?;
                Ok(if is_negative { -value } else { value })
            }
            _ => self.expected_ref("number", current_token),
        }
    }

    /// Parse optional type modifiers appearing in parentheses e.g. `(UNSIGNED, ZEROFILL)`.
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

    /// Parse a DELETE statement, returning a `Box`ed SetExpr
    ///
    /// This is used to reduce the size of the stack frames in debug builds
    fn parse_delete_setexpr_boxed(
        &mut self,
        delete_token: TokenWithSpan,
    ) -> Result<Box<SetExpr>, ParserError> {
        Ok(Box::new(SetExpr::Delete(self.parse_delete(delete_token)?)))
    }

    /// Parse a `DELETE` statement and return `Statement::Delete`.
    pub fn parse_delete(&mut self, delete_token: TokenWithSpan) -> Result<Statement, ParserError> {
        let optimizer_hint = self.maybe_parse_optimizer_hint()?;
        let (tables, with_from_keyword) = if !self.parse_keyword(Keyword::FROM) {
            // `FROM` keyword is optional in BigQuery SQL.
            // https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#delete_statement
            if dialect_of!(self is BigQueryDialect | OracleDialect | GenericDialect) {
                (vec![], false)
            } else {
                let tables = self.parse_comma_separated(|p| p.parse_object_name(false))?;
                self.expect_keyword_is(Keyword::FROM)?;
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
            delete_token: delete_token.into(),
            optimizer_hint,
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

    /// Parse a `KILL` statement, optionally specifying `CONNECTION`, `QUERY`, or `MUTATION`.
    /// KILL [CONNECTION | QUERY | MUTATION] processlist_id
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

    /// Parse an `EXPLAIN` statement, handling dialect-specific options and modifiers.
    pub fn parse_explain(
        &mut self,
        describe_alias: DescribeAlias,
    ) -> Result<Statement, ParserError> {
        let mut analyze = false;
        let mut verbose = false;
        let mut query_plan = false;
        let mut estimate = false;
        let mut format = None;
        let mut options = None;

        // Note: DuckDB is compatible with PostgreSQL syntax for this statement,
        // although not all features may be implemented.
        if describe_alias == DescribeAlias::Explain
            && self.dialect.supports_explain_with_utility_options()
            && self.peek_token().token == Token::LParen
        {
            options = Some(self.parse_utility_options()?)
        } else if self.parse_keywords(&[Keyword::QUERY, Keyword::PLAN]) {
            query_plan = true;
        } else if self.parse_keyword(Keyword::ESTIMATE) {
            estimate = true;
        } else {
            analyze = self.parse_keyword(Keyword::ANALYZE);
            verbose = self.parse_keyword(Keyword::VERBOSE);
            if self.parse_keyword(Keyword::FORMAT) {
                format = Some(self.parse_analyze_format_kind()?);
            }
        }

        match self.maybe_parse(|parser| parser.parse_statement())? {
            Some(Statement::Explain { .. }) | Some(Statement::ExplainTable { .. }) => Err(
                ParserError::ParserError("Explain must be root of the plan".to_string()),
            ),
            Some(statement) => Ok(Statement::Explain {
                describe_alias,
                analyze,
                verbose,
                query_plan,
                estimate,
                statement: Box::new(statement),
                format,
                options,
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

    /// Parse a query expression, i.e. a `SELECT` statement optionally
    /// preceded with some `WITH` CTE declarations and optionally followed
    /// by `ORDER BY`. Unlike some other parse_... methods, this one doesn't
    /// expect the initial keyword to be already consumed
    pub fn parse_query(&mut self) -> Result<Box<Query>, ParserError> {
        let _guard = self.recursion_counter.try_decrease()?;
        let with = if self.parse_keyword(Keyword::WITH) {
            let with_token = self.get_current_token();
            Some(With {
                with_token: with_token.clone().into(),
                recursive: self.parse_keyword(Keyword::RECURSIVE),
                cte_tables: self.parse_comma_separated(Parser::parse_cte)?,
            })
        } else {
            None
        };
        if self.parse_keyword(Keyword::INSERT) {
            Ok(Query {
                with,
                body: self.parse_insert_setexpr_boxed(self.get_current_token().clone())?,
                order_by: None,
                limit_clause: None,
                fetch: None,
                locks: vec![],
                for_clause: None,
                settings: None,
                format_clause: None,
                pipe_operators: vec![],
            }
            .into())
        } else if self.parse_keyword(Keyword::UPDATE) {
            Ok(Query {
                with,
                body: self.parse_update_setexpr_boxed(self.get_current_token().clone())?,
                order_by: None,
                limit_clause: None,
                fetch: None,
                locks: vec![],
                for_clause: None,
                settings: None,
                format_clause: None,
                pipe_operators: vec![],
            }
            .into())
        } else if self.parse_keyword(Keyword::DELETE) {
            Ok(Query {
                with,
                body: self.parse_delete_setexpr_boxed(self.get_current_token().clone())?,
                limit_clause: None,
                order_by: None,
                fetch: None,
                locks: vec![],
                for_clause: None,
                settings: None,
                format_clause: None,
                pipe_operators: vec![],
            }
            .into())
        } else if self.parse_keyword(Keyword::MERGE) {
            Ok(Query {
                with,
                body: self.parse_merge_setexpr_boxed(self.get_current_token().clone())?,
                limit_clause: None,
                order_by: None,
                fetch: None,
                locks: vec![],
                for_clause: None,
                settings: None,
                format_clause: None,
                pipe_operators: vec![],
            }
            .into())
        } else {
            let body = self.parse_query_body(self.dialect.prec_unknown())?;

            let order_by = self.parse_optional_order_by()?;

            let limit_clause = self.parse_optional_limit_clause()?;

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
            let format_clause =
                if self.dialect.supports_select_format() && self.parse_keyword(Keyword::FORMAT) {
                    if self.parse_keyword(Keyword::NULL) {
                        Some(FormatClause::Null)
                    } else {
                        let ident = self.parse_identifier()?;
                        Some(FormatClause::Identifier(ident))
                    }
                } else {
                    None
                };

            let pipe_operators = if self.dialect.supports_pipe_operator() {
                self.parse_pipe_operators()?
            } else {
                Vec::new()
            };

            Ok(Query {
                with,
                body,
                order_by,
                limit_clause,
                fetch,
                locks,
                for_clause,
                settings,
                format_clause,
                pipe_operators,
            }
            .into())
        }
    }

    fn parse_pipe_operators(&mut self) -> Result<Vec<PipeOperator>, ParserError> {
        let mut pipe_operators = Vec::new();

        while self.consume_token(&Token::VerticalBarRightAngleBracket) {
            let kw = self.expect_one_of_keywords(&[
                Keyword::SELECT,
                Keyword::EXTEND,
                Keyword::SET,
                Keyword::DROP,
                Keyword::AS,
                Keyword::WHERE,
                Keyword::LIMIT,
                Keyword::AGGREGATE,
                Keyword::ORDER,
                Keyword::TABLESAMPLE,
                Keyword::RENAME,
                Keyword::UNION,
                Keyword::INTERSECT,
                Keyword::EXCEPT,
                Keyword::CALL,
                Keyword::PIVOT,
                Keyword::UNPIVOT,
                Keyword::JOIN,
                Keyword::INNER,
                Keyword::LEFT,
                Keyword::RIGHT,
                Keyword::FULL,
                Keyword::CROSS,
            ])?;
            match kw {
                Keyword::SELECT => {
                    let exprs = self.parse_comma_separated(Parser::parse_select_item)?;
                    pipe_operators.push(PipeOperator::Select { exprs })
                }
                Keyword::EXTEND => {
                    let exprs = self.parse_comma_separated(Parser::parse_select_item)?;
                    pipe_operators.push(PipeOperator::Extend { exprs })
                }
                Keyword::SET => {
                    let assignments = self.parse_comma_separated(Parser::parse_assignment)?;
                    pipe_operators.push(PipeOperator::Set { assignments })
                }
                Keyword::DROP => {
                    let columns = self.parse_identifiers()?;
                    pipe_operators.push(PipeOperator::Drop { columns })
                }
                Keyword::AS => {
                    let alias = self.parse_identifier()?;
                    pipe_operators.push(PipeOperator::As { alias })
                }
                Keyword::WHERE => {
                    let expr = self.parse_expr()?;
                    pipe_operators.push(PipeOperator::Where { expr })
                }
                Keyword::LIMIT => {
                    let expr = self.parse_expr()?;
                    let offset = if self.parse_keyword(Keyword::OFFSET) {
                        Some(self.parse_expr()?)
                    } else {
                        None
                    };
                    pipe_operators.push(PipeOperator::Limit { expr, offset })
                }
                Keyword::AGGREGATE => {
                    let full_table_exprs = if self.peek_keyword(Keyword::GROUP) {
                        vec![]
                    } else {
                        self.parse_comma_separated(|parser| {
                            parser.parse_expr_with_alias_and_order_by()
                        })?
                    };

                    let group_by_expr = if self.parse_keywords(&[Keyword::GROUP, Keyword::BY]) {
                        self.parse_comma_separated(|parser| {
                            parser.parse_expr_with_alias_and_order_by()
                        })?
                    } else {
                        vec![]
                    };

                    pipe_operators.push(PipeOperator::Aggregate {
                        full_table_exprs,
                        group_by_expr,
                    })
                }
                Keyword::ORDER => {
                    self.expect_one_of_keywords(&[Keyword::BY])?;
                    let exprs = self.parse_comma_separated(Parser::parse_order_by_expr)?;
                    pipe_operators.push(PipeOperator::OrderBy { exprs })
                }
                Keyword::TABLESAMPLE => {
                    let sample = self.parse_table_sample(TableSampleModifier::TableSample)?;
                    pipe_operators.push(PipeOperator::TableSample { sample });
                }
                Keyword::RENAME => {
                    let mappings =
                        self.parse_comma_separated(Parser::parse_identifier_with_optional_alias)?;
                    pipe_operators.push(PipeOperator::Rename { mappings });
                }
                Keyword::UNION => {
                    let set_quantifier = self.parse_set_quantifier(&Some(SetOperator::Union));
                    let queries = self.parse_pipe_operator_queries()?;
                    pipe_operators.push(PipeOperator::Union {
                        set_quantifier,
                        queries,
                    });
                }
                Keyword::INTERSECT => {
                    let set_quantifier =
                        self.parse_distinct_required_set_quantifier("INTERSECT")?;
                    let queries = self.parse_pipe_operator_queries()?;
                    pipe_operators.push(PipeOperator::Intersect {
                        set_quantifier,
                        queries,
                    });
                }
                Keyword::EXCEPT => {
                    let set_quantifier = self.parse_distinct_required_set_quantifier("EXCEPT")?;
                    let queries = self.parse_pipe_operator_queries()?;
                    pipe_operators.push(PipeOperator::Except {
                        set_quantifier,
                        queries,
                    });
                }
                Keyword::CALL => {
                    let function_name = self.parse_object_name(false)?;
                    let function_expr = self.parse_function(function_name)?;
                    if let Expr::Function(function) = function_expr {
                        let alias = self.parse_identifier_optional_alias()?;
                        pipe_operators.push(PipeOperator::Call { function, alias });
                    } else {
                        return Err(ParserError::ParserError(
                            "Expected function call after CALL".to_string(),
                        ));
                    }
                }
                Keyword::PIVOT => {
                    self.expect_token(&Token::LParen)?;
                    let aggregate_functions =
                        self.parse_comma_separated(Self::parse_aliased_function_call)?;
                    self.expect_keyword_is(Keyword::FOR)?;
                    let value_column = self.parse_period_separated(|p| p.parse_identifier())?;
                    self.expect_keyword_is(Keyword::IN)?;

                    self.expect_token(&Token::LParen)?;
                    let value_source = if self.parse_keyword(Keyword::ANY) {
                        let order_by = if self.parse_keywords(&[Keyword::ORDER, Keyword::BY]) {
                            self.parse_comma_separated(Parser::parse_order_by_expr)?
                        } else {
                            vec![]
                        };
                        PivotValueSource::Any(order_by)
                    } else if self.peek_sub_query() {
                        PivotValueSource::Subquery(self.parse_query()?)
                    } else {
                        PivotValueSource::List(
                            self.parse_comma_separated(Self::parse_expr_with_alias)?,
                        )
                    };
                    self.expect_token(&Token::RParen)?;
                    self.expect_token(&Token::RParen)?;

                    let alias = self.parse_identifier_optional_alias()?;

                    pipe_operators.push(PipeOperator::Pivot {
                        aggregate_functions,
                        value_column,
                        value_source,
                        alias,
                    });
                }
                Keyword::UNPIVOT => {
                    self.expect_token(&Token::LParen)?;
                    let value_column = self.parse_identifier()?;
                    self.expect_keyword(Keyword::FOR)?;
                    let name_column = self.parse_identifier()?;
                    self.expect_keyword(Keyword::IN)?;

                    self.expect_token(&Token::LParen)?;
                    let unpivot_columns = self.parse_comma_separated(Parser::parse_identifier)?;
                    self.expect_token(&Token::RParen)?;

                    self.expect_token(&Token::RParen)?;

                    let alias = self.parse_identifier_optional_alias()?;

                    pipe_operators.push(PipeOperator::Unpivot {
                        value_column,
                        name_column,
                        unpivot_columns,
                        alias,
                    });
                }
                Keyword::JOIN
                | Keyword::INNER
                | Keyword::LEFT
                | Keyword::RIGHT
                | Keyword::FULL
                | Keyword::CROSS => {
                    self.prev_token();
                    let mut joins = self.parse_joins()?;
                    if joins.len() != 1 {
                        return Err(ParserError::ParserError(
                            "Join pipe operator must have a single join".to_string(),
                        ));
                    }
                    let join = joins.swap_remove(0);
                    pipe_operators.push(PipeOperator::Join(join))
                }
                unhandled => {
                    return Err(ParserError::ParserError(format!(
                    "`expect_one_of_keywords` further up allowed unhandled keyword: {unhandled:?}"
                )))
                }
            }
        }
        Ok(pipe_operators)
    }

    fn parse_settings(&mut self) -> Result<Option<Vec<Setting>>, ParserError> {
        let settings = if self.dialect.supports_settings() && self.parse_keyword(Keyword::SETTINGS)
        {
            let key_values = self.parse_comma_separated(|p| {
                let key = p.parse_identifier()?;
                p.expect_token(&Token::Eq)?;
                let value = p.parse_expr()?;
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
                self.expect_keyword_is(Keyword::BASE64)?;
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
        let name = self.parse_identifier()?;

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

            let query = self.parse_query()?;
            let closing_paren_token = self.expect_token(&Token::RParen)?;

            let alias = TableAlias {
                explicit: false,
                name,
                columns: vec![],
            };
            Cte {
                alias,
                query,
                from: None,
                materialized: is_materialized,
                closing_paren_token: closing_paren_token.into(),
            }
        } else {
            let columns = self.parse_table_alias_column_defs()?;
            self.expect_keyword_is(Keyword::AS)?;
            let mut is_materialized = None;
            if dialect_of!(self is PostgreSqlDialect) {
                if self.parse_keyword(Keyword::MATERIALIZED) {
                    is_materialized = Some(CteAsMaterialized::Materialized);
                } else if self.parse_keywords(&[Keyword::NOT, Keyword::MATERIALIZED]) {
                    is_materialized = Some(CteAsMaterialized::NotMaterialized);
                }
            }
            self.expect_token(&Token::LParen)?;

            let query = self.parse_query()?;
            let closing_paren_token = self.expect_token(&Token::RParen)?;

            let alias = TableAlias {
                explicit: false,
                name,
                columns,
            };
            Cte {
                alias,
                query,
                from: None,
                materialized: is_materialized,
                closing_paren_token: closing_paren_token.into(),
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
    pub fn parse_query_body(&mut self, precedence: u8) -> Result<Box<SetExpr>, ParserError> {
        // We parse the expression using a Pratt parser, as in `parse_expr()`.
        // Start by parsing a restricted SELECT or a `(subquery)`:
        let expr = if self.peek_keyword(Keyword::SELECT)
            || (self.peek_keyword(Keyword::FROM) && self.dialect.supports_from_first_select())
        {
            SetExpr::Select(self.parse_select().map(Box::new)?)
        } else if self.consume_token(&Token::LParen) {
            // CTEs are not allowed here, but the parser currently accepts them
            let subquery = self.parse_query()?;
            self.expect_token(&Token::RParen)?;
            SetExpr::Query(subquery)
        } else if self.parse_keyword(Keyword::VALUES) {
            let is_mysql = dialect_of!(self is MySqlDialect);
            SetExpr::Values(self.parse_values(is_mysql, false)?)
        } else if self.parse_keyword(Keyword::VALUE) {
            let is_mysql = dialect_of!(self is MySqlDialect);
            SetExpr::Values(self.parse_values(is_mysql, true)?)
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
    ) -> Result<Box<SetExpr>, ParserError> {
        loop {
            // The query can be optionally followed by a set operator:
            let op = self.parse_set_operator(&self.peek_token().token);
            let next_precedence = match op {
                // UNION and EXCEPT have the same binding power and evaluate left-to-right
                Some(SetOperator::Union) | Some(SetOperator::Except) | Some(SetOperator::Minus) => {
                    10
                }
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
                right: self.parse_query_body(next_precedence)?,
            };
        }

        Ok(expr.into())
    }

    /// Parse a set operator token into its `SetOperator` variant.
    pub fn parse_set_operator(&mut self, token: &Token) -> Option<SetOperator> {
        match token {
            Token::Word(w) if w.keyword == Keyword::UNION => Some(SetOperator::Union),
            Token::Word(w) if w.keyword == Keyword::EXCEPT => Some(SetOperator::Except),
            Token::Word(w) if w.keyword == Keyword::INTERSECT => Some(SetOperator::Intersect),
            Token::Word(w) if w.keyword == Keyword::MINUS => Some(SetOperator::Minus),
            _ => None,
        }
    }

    /// Parse a set quantifier (e.g., `ALL`, `DISTINCT BY NAME`) for the given set operator.
    pub fn parse_set_quantifier(&mut self, op: &Option<SetOperator>) -> SetQuantifier {
        match op {
            Some(
                SetOperator::Except
                | SetOperator::Intersect
                | SetOperator::Union
                | SetOperator::Minus,
            ) => {
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

    /// Parse a restricted `SELECT` statement (no CTEs / `UNION` / `ORDER BY`)
    pub fn parse_select(&mut self) -> Result<Select, ParserError> {
        let mut from_first = None;

        if self.dialect.supports_from_first_select() && self.peek_keyword(Keyword::FROM) {
            let from_token = self.expect_keyword(Keyword::FROM)?;
            let from = self.parse_table_with_joins()?;
            if !self.peek_keyword(Keyword::SELECT) {
                return Ok(Select {
                    select_token: AttachedToken(from_token),
                    optimizer_hint: None,
                    distinct: None,
                    top: None,
                    top_before_distinct: false,
                    projection: vec![],
                    exclude: None,
                    into: None,
                    from,
                    lateral_views: vec![],
                    prewhere: None,
                    selection: None,
                    group_by: GroupByExpr::Expressions(vec![], vec![]),
                    cluster_by: vec![],
                    distribute_by: vec![],
                    sort_by: vec![],
                    having: None,
                    named_window: vec![],
                    window_before_qualify: false,
                    qualify: None,
                    value_table_mode: None,
                    connect_by: None,
                    flavor: SelectFlavor::FromFirstNoSelect,
                });
            }
            from_first = Some(from);
        }

        let select_token = self.expect_keyword(Keyword::SELECT)?;
        let optimizer_hint = self.maybe_parse_optimizer_hint()?;
        let value_table_mode = self.parse_value_table_mode()?;

        let mut top_before_distinct = false;
        let mut top = None;
        if self.dialect.supports_top_before_distinct() && self.parse_keyword(Keyword::TOP) {
            top = Some(self.parse_top()?);
            top_before_distinct = true;
        }
        let distinct = self.parse_all_or_distinct()?;
        if !self.dialect.supports_top_before_distinct() && self.parse_keyword(Keyword::TOP) {
            top = Some(self.parse_top()?);
        }

        let projection =
            if self.dialect.supports_empty_projections() && self.peek_keyword(Keyword::FROM) {
                vec![]
            } else {
                self.parse_projection()?
            };

        let exclude = if self.dialect.supports_select_exclude() {
            self.parse_optional_select_item_exclude()?
        } else {
            None
        };

        let into = if self.parse_keyword(Keyword::INTO) {
            Some(self.parse_select_into()?)
        } else {
            None
        };

        // Note that for keywords to be properly handled here, they need to be
        // added to `RESERVED_FOR_COLUMN_ALIAS` / `RESERVED_FOR_TABLE_ALIAS`,
        // otherwise they may be parsed as an alias as part of the `projection`
        // or `from`.

        let (from, from_first) = if let Some(from) = from_first.take() {
            (from, true)
        } else if self.parse_keyword(Keyword::FROM) {
            (self.parse_table_with_joins()?, false)
        } else {
            (vec![], false)
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

        let prewhere = if self.dialect.supports_prewhere() && self.parse_keyword(Keyword::PREWHERE)
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
            self.parse_comma_separated(Parser::parse_order_by_expr)?
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
            select_token: AttachedToken(select_token),
            optimizer_hint,
            distinct,
            top,
            top_before_distinct,
            projection,
            exclude,
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
            flavor: if from_first {
                SelectFlavor::FromFirst
            } else {
                SelectFlavor::Standard
            },
        })
    }

    /// Parses an optional optimizer hint at the current token position
    ///
    /// [MySQL](https://dev.mysql.com/doc/refman/8.4/en/optimizer-hints.html#optimizer-hints-overview)
    /// [Oracle](https://docs.oracle.com/en/database/oracle/oracle-database/21/sqlrf/Comments.html#GUID-D316D545-89E2-4D54-977F-FC97815CD62E)
    fn maybe_parse_optimizer_hint(&mut self) -> Result<Option<OptimizerHint>, ParserError> {
        let supports_hints = self.dialect.supports_comment_optimizer_hint();
        if !supports_hints {
            return Ok(None);
        }
        loop {
            let t = self.peek_nth_token_no_skip_ref(0);
            match &t.token {
                Token::Whitespace(ws) => {
                    match ws {
                        Whitespace::SingleLineComment { comment, .. }
                        | Whitespace::MultiLineComment(comment) => {
                            return Ok(match comment.strip_prefix("+") {
                                None => None,
                                Some(text) => {
                                    let hint = OptimizerHint {
                                        text: text.into(),
                                        style: if let Whitespace::SingleLineComment {
                                            prefix, ..
                                        } = ws
                                        {
                                            OptimizerHintStyle::SingleLine {
                                                prefix: prefix.clone(),
                                            }
                                        } else {
                                            OptimizerHintStyle::MultiLine
                                        },
                                    };
                                    // Consume the comment token
                                    self.next_token_no_skip();
                                    Some(hint)
                                }
                            });
                        }
                        Whitespace::Space | Whitespace::Tab | Whitespace::Newline => {
                            // Consume the token and try with the next whitespace or comment
                            self.next_token_no_skip();
                        }
                    }
                }
                _ => return Ok(None),
            }
        }
    }

    fn parse_value_table_mode(&mut self) -> Result<Option<ValueTableMode>, ParserError> {
        if !dialect_of!(self is BigQueryDialect) {
            return Ok(None);
        }

        let mode = if self.parse_keywords(&[Keyword::DISTINCT, Keyword::AS, Keyword::VALUE]) {
            Some(ValueTableMode::DistinctAsValue)
        } else if self.parse_keywords(&[Keyword::DISTINCT, Keyword::AS, Keyword::STRUCT]) {
            Some(ValueTableMode::DistinctAsStruct)
        } else if self.parse_keywords(&[Keyword::AS, Keyword::VALUE])
            || self.parse_keywords(&[Keyword::ALL, Keyword::AS, Keyword::VALUE])
        {
            Some(ValueTableMode::AsValue)
        } else if self.parse_keywords(&[Keyword::AS, Keyword::STRUCT])
            || self.parse_keywords(&[Keyword::ALL, Keyword::AS, Keyword::STRUCT])
        {
            Some(ValueTableMode::AsStruct)
        } else if self.parse_keyword(Keyword::AS) {
            self.expected("VALUE or STRUCT", self.peek_token())?
        } else {
            None
        };

        Ok(mode)
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

    /// Parse a `CONNECT BY` clause (Oracle-style hierarchical query support).
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

    /// Parse a `SET ROLE` statement. Expects SET to be consumed already.
    fn parse_set_role(
        &mut self,
        modifier: Option<ContextModifier>,
    ) -> Result<Statement, ParserError> {
        self.expect_keyword_is(Keyword::ROLE)?;

        let role_name = if self.parse_keyword(Keyword::NONE) {
            None
        } else {
            Some(self.parse_identifier()?)
        };
        Ok(Statement::Set(Set::SetRole {
            context_modifier: modifier,
            role_name,
        }))
    }

    fn parse_set_values(
        &mut self,
        parenthesized_assignment: bool,
    ) -> Result<Vec<Expr>, ParserError> {
        let mut values = vec![];

        if parenthesized_assignment {
            self.expect_token(&Token::LParen)?;
        }

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
            return Ok(values);
        }
    }

    fn parse_context_modifier(&mut self) -> Option<ContextModifier> {
        let modifier =
            self.parse_one_of_keywords(&[Keyword::SESSION, Keyword::LOCAL, Keyword::GLOBAL])?;

        Self::keyword_to_modifier(modifier)
    }

    /// Parse a single SET statement assignment `var = expr`.
    fn parse_set_assignment(&mut self) -> Result<SetAssignment, ParserError> {
        let scope = self.parse_context_modifier();

        let name = if self.dialect.supports_parenthesized_set_variables()
            && self.consume_token(&Token::LParen)
        {
            // Parenthesized assignments are handled in the `parse_set` function after
            // trying to parse list of assignments using this function.
            // If a dialect supports both, and we find a LParen, we early exit from this function.
            self.expected("Unparenthesized assignment", self.peek_token())?
        } else {
            self.parse_object_name(false)?
        };

        if !(self.consume_token(&Token::Eq) || self.parse_keyword(Keyword::TO)) {
            return self.expected("assignment operator", self.peek_token());
        }

        let value = self.parse_expr()?;

        Ok(SetAssignment { scope, name, value })
    }

    fn parse_set(&mut self) -> Result<Statement, ParserError> {
        let hivevar = self.parse_keyword(Keyword::HIVEVAR);

        // Modifier is either HIVEVAR: or a ContextModifier (LOCAL, SESSION, etc), not both
        let scope = if !hivevar {
            self.parse_context_modifier()
        } else {
            None
        };

        if hivevar {
            self.expect_token(&Token::Colon)?;
        }

        if let Some(set_role_stmt) = self.maybe_parse(|parser| parser.parse_set_role(scope))? {
            return Ok(set_role_stmt);
        }

        // Handle special cases first
        if self.parse_keywords(&[Keyword::TIME, Keyword::ZONE])
            || self.parse_keyword(Keyword::TIMEZONE)
        {
            if self.consume_token(&Token::Eq) || self.parse_keyword(Keyword::TO) {
                return Ok(Set::SingleAssignment {
                    scope,
                    hivevar,
                    variable: ObjectName::from(vec!["TIMEZONE".into()]),
                    values: self.parse_set_values(false)?,
                }
                .into());
            } else {
                // A shorthand alias for SET TIME ZONE that doesn't require
                // the assignment operator. It's originally PostgreSQL specific,
                // but we allow it for all the dialects
                return Ok(Set::SetTimeZone {
                    local: scope == Some(ContextModifier::Local),
                    value: self.parse_expr()?,
                }
                .into());
            }
        } else if self.dialect.supports_set_names() && self.parse_keyword(Keyword::NAMES) {
            if self.parse_keyword(Keyword::DEFAULT) {
                return Ok(Set::SetNamesDefault {}.into());
            }
            let charset_name = self.parse_identifier()?;
            let collation_name = if self.parse_one_of_keywords(&[Keyword::COLLATE]).is_some() {
                Some(self.parse_literal_string()?)
            } else {
                None
            };

            return Ok(Set::SetNames {
                charset_name,
                collation_name,
            }
            .into());
        } else if self.parse_keyword(Keyword::CHARACTERISTICS) {
            self.expect_keywords(&[Keyword::AS, Keyword::TRANSACTION])?;
            return Ok(Set::SetTransaction {
                modes: self.parse_transaction_modes()?,
                snapshot: None,
                session: true,
            }
            .into());
        } else if self.parse_keyword(Keyword::TRANSACTION) {
            if self.parse_keyword(Keyword::SNAPSHOT) {
                let snapshot_id = self.parse_value()?.value;
                return Ok(Set::SetTransaction {
                    modes: vec![],
                    snapshot: Some(snapshot_id),
                    session: false,
                }
                .into());
            }
            return Ok(Set::SetTransaction {
                modes: self.parse_transaction_modes()?,
                snapshot: None,
                session: false,
            }
            .into());
        } else if self.parse_keyword(Keyword::AUTHORIZATION) {
            let auth_value = if self.parse_keyword(Keyword::DEFAULT) {
                SetSessionAuthorizationParamKind::Default
            } else {
                let value = self.parse_identifier()?;
                SetSessionAuthorizationParamKind::User(value)
            };
            return Ok(Set::SetSessionAuthorization(SetSessionAuthorizationParam {
                scope: scope.expect("SET ... AUTHORIZATION must have a scope"),
                kind: auth_value,
            })
            .into());
        }

        if self.dialect.supports_comma_separated_set_assignments() {
            if scope.is_some() {
                self.prev_token();
            }

            if let Some(assignments) = self
                .maybe_parse(|parser| parser.parse_comma_separated(Parser::parse_set_assignment))?
            {
                return if assignments.len() > 1 {
                    Ok(Set::MultipleAssignments { assignments }.into())
                } else {
                    let SetAssignment { scope, name, value } =
                        assignments.into_iter().next().ok_or_else(|| {
                            ParserError::ParserError("Expected at least one assignment".to_string())
                        })?;

                    Ok(Set::SingleAssignment {
                        scope,
                        hivevar,
                        variable: name,
                        values: vec![value],
                    }
                    .into())
                };
            }
        }

        let variables = if self.dialect.supports_parenthesized_set_variables()
            && self.consume_token(&Token::LParen)
        {
            let vars = OneOrManyWithParens::Many(
                self.parse_comma_separated(|parser: &mut Parser<'a>| parser.parse_identifier())?
                    .into_iter()
                    .map(|ident| ObjectName::from(vec![ident]))
                    .collect(),
            );
            self.expect_token(&Token::RParen)?;
            vars
        } else {
            OneOrManyWithParens::One(self.parse_object_name(false)?)
        };

        if self.consume_token(&Token::Eq) || self.parse_keyword(Keyword::TO) {
            let stmt = match variables {
                OneOrManyWithParens::One(var) => Set::SingleAssignment {
                    scope,
                    hivevar,
                    variable: var,
                    values: self.parse_set_values(false)?,
                },
                OneOrManyWithParens::Many(vars) => Set::ParenthesizedAssignments {
                    variables: vars,
                    values: self.parse_set_values(true)?,
                },
            };

            return Ok(stmt.into());
        }

        if self.dialect.supports_set_stmt_without_operator() {
            self.prev_token();
            return self.parse_set_session_params();
        };

        self.expected("equals sign or TO", self.peek_token())
    }

    /// Parse session parameter assignments after `SET` when no `=` or `TO` is present.
    pub fn parse_set_session_params(&mut self) -> Result<Statement, ParserError> {
        if self.parse_keyword(Keyword::STATISTICS) {
            let topic = match self.parse_one_of_keywords(&[
                Keyword::IO,
                Keyword::PROFILE,
                Keyword::TIME,
                Keyword::XML,
            ]) {
                Some(Keyword::IO) => SessionParamStatsTopic::IO,
                Some(Keyword::PROFILE) => SessionParamStatsTopic::Profile,
                Some(Keyword::TIME) => SessionParamStatsTopic::Time,
                Some(Keyword::XML) => SessionParamStatsTopic::Xml,
                _ => return self.expected("IO, PROFILE, TIME or XML", self.peek_token()),
            };
            let value = self.parse_session_param_value()?;
            Ok(
                Set::SetSessionParam(SetSessionParamKind::Statistics(SetSessionParamStatistics {
                    topic,
                    value,
                }))
                .into(),
            )
        } else if self.parse_keyword(Keyword::IDENTITY_INSERT) {
            let obj = self.parse_object_name(false)?;
            let value = self.parse_session_param_value()?;
            Ok(Set::SetSessionParam(SetSessionParamKind::IdentityInsert(
                SetSessionParamIdentityInsert { obj, value },
            ))
            .into())
        } else if self.parse_keyword(Keyword::OFFSETS) {
            let keywords = self.parse_comma_separated(|parser| {
                let next_token = parser.next_token();
                match &next_token.token {
                    Token::Word(w) => Ok(w.to_string()),
                    _ => parser.expected("SQL keyword", next_token),
                }
            })?;
            let value = self.parse_session_param_value()?;
            Ok(
                Set::SetSessionParam(SetSessionParamKind::Offsets(SetSessionParamOffsets {
                    keywords,
                    value,
                }))
                .into(),
            )
        } else {
            let names = self.parse_comma_separated(|parser| {
                let next_token = parser.next_token();
                match next_token.token {
                    Token::Word(w) => Ok(w.to_string()),
                    _ => parser.expected("Session param name", next_token),
                }
            })?;
            let value = self.parse_expr()?.to_string();
            Ok(
                Set::SetSessionParam(SetSessionParamKind::Generic(SetSessionParamGeneric {
                    names,
                    value,
                }))
                .into(),
            )
        }
    }

    fn parse_session_param_value(&mut self) -> Result<SessionParamValue, ParserError> {
        if self.parse_keyword(Keyword::ON) {
            Ok(SessionParamValue::On)
        } else if self.parse_keyword(Keyword::OFF) {
            Ok(SessionParamValue::Off)
        } else {
            self.expected("ON or OFF", self.peek_token())
        }
    }

    /// Parse a `SHOW` statement and dispatch to specific SHOW handlers.
    pub fn parse_show(&mut self) -> Result<Statement, ParserError> {
        let terse = self.parse_keyword(Keyword::TERSE);
        let extended = self.parse_keyword(Keyword::EXTENDED);
        let full = self.parse_keyword(Keyword::FULL);
        let session = self.parse_keyword(Keyword::SESSION);
        let global = self.parse_keyword(Keyword::GLOBAL);
        let external = self.parse_keyword(Keyword::EXTERNAL);
        if self
            .parse_one_of_keywords(&[Keyword::COLUMNS, Keyword::FIELDS])
            .is_some()
        {
            Ok(self.parse_show_columns(extended, full)?)
        } else if self.parse_keyword(Keyword::TABLES) {
            Ok(self.parse_show_tables(terse, extended, full, external)?)
        } else if self.parse_keywords(&[Keyword::MATERIALIZED, Keyword::VIEWS]) {
            Ok(self.parse_show_views(terse, true)?)
        } else if self.parse_keyword(Keyword::VIEWS) {
            Ok(self.parse_show_views(terse, false)?)
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
        } else if self.parse_keyword(Keyword::DATABASES) {
            self.parse_show_databases(terse)
        } else if self.parse_keyword(Keyword::SCHEMAS) {
            self.parse_show_schemas(terse)
        } else if self.parse_keywords(&[Keyword::CHARACTER, Keyword::SET]) {
            self.parse_show_charset(false)
        } else if self.parse_keyword(Keyword::CHARSET) {
            self.parse_show_charset(true)
        } else {
            Ok(Statement::ShowVariable {
                variable: self.parse_identifiers()?,
            })
        }
    }

    fn parse_show_charset(&mut self, is_shorthand: bool) -> Result<Statement, ParserError> {
        // parse one of keywords
        Ok(Statement::ShowCharset(ShowCharset {
            is_shorthand,
            filter: self.parse_show_statement_filter()?,
        }))
    }

    fn parse_show_databases(&mut self, terse: bool) -> Result<Statement, ParserError> {
        let history = self.parse_keyword(Keyword::HISTORY);
        let show_options = self.parse_show_stmt_options()?;
        Ok(Statement::ShowDatabases {
            terse,
            history,
            show_options,
        })
    }

    fn parse_show_schemas(&mut self, terse: bool) -> Result<Statement, ParserError> {
        let history = self.parse_keyword(Keyword::HISTORY);
        let show_options = self.parse_show_stmt_options()?;
        Ok(Statement::ShowSchemas {
            terse,
            history,
            show_options,
        })
    }

    /// Parse `SHOW CREATE <object>` returning the corresponding `ShowCreate` statement.
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

    /// Parse `SHOW COLUMNS`/`SHOW FIELDS` and return a `ShowColumns` statement.
    pub fn parse_show_columns(
        &mut self,
        extended: bool,
        full: bool,
    ) -> Result<Statement, ParserError> {
        let show_options = self.parse_show_stmt_options()?;
        Ok(Statement::ShowColumns {
            extended,
            full,
            show_options,
        })
    }

    fn parse_show_tables(
        &mut self,
        terse: bool,
        extended: bool,
        full: bool,
        external: bool,
    ) -> Result<Statement, ParserError> {
        let history = !external && self.parse_keyword(Keyword::HISTORY);
        let show_options = self.parse_show_stmt_options()?;
        Ok(Statement::ShowTables {
            terse,
            history,
            extended,
            full,
            external,
            show_options,
        })
    }

    fn parse_show_views(
        &mut self,
        terse: bool,
        materialized: bool,
    ) -> Result<Statement, ParserError> {
        let show_options = self.parse_show_stmt_options()?;
        Ok(Statement::ShowViews {
            materialized,
            terse,
            show_options,
        })
    }

    /// Parse `SHOW FUNCTIONS` and optional filter.
    pub fn parse_show_functions(&mut self) -> Result<Statement, ParserError> {
        let filter = self.parse_show_statement_filter()?;
        Ok(Statement::ShowFunctions { filter })
    }

    /// Parse `SHOW COLLATION` and optional filter.
    pub fn parse_show_collation(&mut self) -> Result<Statement, ParserError> {
        let filter = self.parse_show_statement_filter()?;
        Ok(Statement::ShowCollation { filter })
    }

    /// Parse an optional filter used by `SHOW` statements (LIKE, ILIKE, WHERE, or literal).
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
            self.maybe_parse(|parser| -> Result<String, ParserError> {
                parser.parse_literal_string()
            })?
            .map_or(Ok(None), |filter| {
                Ok(Some(ShowStatementFilter::NoKeyword(filter)))
            })
        }
    }

    /// Parse a `USE` statement (database/catalog/schema/warehouse/role selection).
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
            self.parse_one_of_keywords(&[
                Keyword::DATABASE,
                Keyword::SCHEMA,
                Keyword::WAREHOUSE,
                Keyword::ROLE,
                Keyword::SECONDARY,
            ])
        } else {
            None // No specific keywords for other dialects, including GenericDialect
        };

        let result = if matches!(parsed_keyword, Some(Keyword::SECONDARY)) {
            self.parse_secondary_roles()?
        } else {
            let obj_name = self.parse_object_name(false)?;
            match parsed_keyword {
                Some(Keyword::CATALOG) => Use::Catalog(obj_name),
                Some(Keyword::DATABASE) => Use::Database(obj_name),
                Some(Keyword::SCHEMA) => Use::Schema(obj_name),
                Some(Keyword::WAREHOUSE) => Use::Warehouse(obj_name),
                Some(Keyword::ROLE) => Use::Role(obj_name),
                _ => Use::Object(obj_name),
            }
        };

        Ok(Statement::Use(result))
    }

    fn parse_secondary_roles(&mut self) -> Result<Use, ParserError> {
        self.expect_one_of_keywords(&[Keyword::ROLES, Keyword::ROLE])?;
        if self.parse_keyword(Keyword::NONE) {
            Ok(Use::SecondaryRoles(SecondaryRoles::None))
        } else if self.parse_keyword(Keyword::ALL) {
            Ok(Use::SecondaryRoles(SecondaryRoles::All))
        } else {
            let roles = self.parse_comma_separated(|parser| parser.parse_identifier())?;
            Ok(Use::SecondaryRoles(SecondaryRoles::List(roles)))
        }
    }

    /// Parse a table factor followed by any join clauses, returning `TableWithJoins`.
    pub fn parse_table_and_joins(&mut self) -> Result<TableWithJoins, ParserError> {
        let relation = self.parse_table_factor()?;
        // Note that for keywords to be properly handled here, they need to be
        // added to `RESERVED_FOR_TABLE_ALIAS`, otherwise they may be parsed as
        // a table alias.
        let joins = self.parse_joins()?;
        Ok(TableWithJoins { relation, joins })
    }

    fn parse_joins(&mut self) -> Result<Vec<Join>, ParserError> {
        let mut joins = vec![];
        loop {
            let global = self.parse_keyword(Keyword::GLOBAL);
            let join = if self.parse_keyword(Keyword::CROSS) {
                let join_operator = if self.parse_keyword(Keyword::JOIN) {
                    JoinOperator::CrossJoin(JoinConstraint::None)
                } else if self.parse_keyword(Keyword::APPLY) {
                    // MSSQL extension, similar to CROSS JOIN LATERAL
                    JoinOperator::CrossApply
                } else {
                    return self.expected("JOIN or APPLY after CROSS", self.peek_token());
                };
                let relation = self.parse_table_factor()?;
                let join_operator = if matches!(join_operator, JoinOperator::CrossJoin(_))
                    && self.dialect.supports_cross_join_constraint()
                {
                    let constraint = self.parse_join_constraint(false)?;
                    JoinOperator::CrossJoin(constraint)
                } else {
                    join_operator
                };
                Join {
                    relation,
                    global,
                    join_operator,
                }
            } else if self.parse_keyword(Keyword::OUTER) {
                // MSSQL extension, similar to LEFT JOIN LATERAL .. ON 1=1
                self.expect_keyword_is(Keyword::APPLY)?;
                Join {
                    relation: self.parse_table_factor()?,
                    global,
                    join_operator: JoinOperator::OuterApply,
                }
            } else if self.parse_keyword(Keyword::ASOF) {
                self.expect_keyword_is(Keyword::JOIN)?;
                let relation = self.parse_table_factor()?;
                self.expect_keyword_is(Keyword::MATCH_CONDITION)?;
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
                        let inner = self.parse_keyword(Keyword::INNER); // [ INNER ]
                        self.expect_keyword_is(Keyword::JOIN)?;
                        if inner {
                            JoinOperator::Inner
                        } else {
                            JoinOperator::Join
                        }
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
                                self.expect_keyword_is(Keyword::JOIN)?;
                                if is_left {
                                    JoinOperator::LeftOuter
                                } else {
                                    JoinOperator::RightOuter
                                }
                            }
                            Some(Keyword::SEMI) => {
                                self.expect_keyword_is(Keyword::JOIN)?;
                                if is_left {
                                    JoinOperator::LeftSemi
                                } else {
                                    JoinOperator::RightSemi
                                }
                            }
                            Some(Keyword::ANTI) => {
                                self.expect_keyword_is(Keyword::JOIN)?;
                                if is_left {
                                    JoinOperator::LeftAnti
                                } else {
                                    JoinOperator::RightAnti
                                }
                            }
                            Some(Keyword::JOIN) => {
                                if is_left {
                                    JoinOperator::Left
                                } else {
                                    JoinOperator::Right
                                }
                            }
                            _ => {
                                return Err(ParserError::ParserError(format!(
                                    "expected OUTER, SEMI, ANTI or JOIN after {kw:?}"
                                )))
                            }
                        }
                    }
                    Keyword::ANTI => {
                        let _ = self.next_token(); // consume ANTI
                        self.expect_keyword_is(Keyword::JOIN)?;
                        JoinOperator::Anti
                    }
                    Keyword::SEMI => {
                        let _ = self.next_token(); // consume SEMI
                        self.expect_keyword_is(Keyword::JOIN)?;
                        JoinOperator::Semi
                    }
                    Keyword::FULL => {
                        let _ = self.next_token(); // consume FULL
                        let _ = self.parse_keyword(Keyword::OUTER); // [ OUTER ]
                        self.expect_keyword_is(Keyword::JOIN)?;
                        JoinOperator::FullOuter
                    }
                    Keyword::OUTER => {
                        return self.expected("LEFT, RIGHT, or FULL", self.peek_token());
                    }
                    Keyword::STRAIGHT_JOIN => {
                        let _ = self.next_token(); // consume STRAIGHT_JOIN
                        JoinOperator::StraightJoin
                    }
                    _ if natural => {
                        return self.expected("a join type after NATURAL", self.peek_token());
                    }
                    _ => break,
                };
                let mut relation = self.parse_table_factor()?;

                if !self
                    .dialect
                    .supports_left_associative_joins_without_parens()
                    && self.peek_parens_less_nested_join()
                {
                    let joins = self.parse_joins()?;
                    relation = TableFactor::NestedJoin {
                        table_with_joins: Box::new(TableWithJoins { relation, joins }),
                        alias: None,
                    };
                }

                let join_constraint = self.parse_join_constraint(natural)?;
                Join {
                    relation,
                    global,
                    join_operator: join_operator_type(join_constraint),
                }
            };
            joins.push(join);
        }
        Ok(joins)
    }

    fn peek_parens_less_nested_join(&self) -> bool {
        matches!(
            self.peek_token_ref().token,
            Token::Word(Word {
                keyword: Keyword::JOIN
                    | Keyword::INNER
                    | Keyword::LEFT
                    | Keyword::RIGHT
                    | Keyword::FULL,
                ..
            })
        )
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
                let alias = self.maybe_parse_table_alias()?;
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
            let alias = self.maybe_parse_table_alias()?;
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
                self.maybe_parse(|parser| parser.parse_derived_table_factor(NotLateral))?
            {
                while let Some(kw) = self.parse_one_of_keywords(&[Keyword::PIVOT, Keyword::UNPIVOT])
                {
                    table = match kw {
                        Keyword::PIVOT => self.parse_pivot_table_factor(table)?,
                        Keyword::UNPIVOT => self.parse_unpivot_table_factor(table)?,
                        unexpected_keyword => return Err(ParserError::ParserError(
                            format!("Internal parser error: unexpected keyword `{unexpected_keyword}` in pivot/unpivot"),
                        )),
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
                let alias = self.maybe_parse_table_alias()?;
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
                let alias = self.maybe_parse_table_alias()?;
                Ok(TableFactor::NestedJoin {
                    table_with_joins: Box::new(table_and_joins),
                    alias,
                })
            } else if self.dialect.supports_parens_around_table_factor() {
                // Dialect-specific behavior: Snowflake diverges from the
                // standard and from most of the other implementations by
                // allowing extra parentheses not only around a join (B), but
                // around lone table names (e.g. `FROM (mytable [AS alias])`)
                // and around derived tables (e.g. `FROM ((SELECT ...)
                // [AS alias])`) as well.
                self.expect_token(&Token::RParen)?;

                if let Some(outer_alias) = self.maybe_parse_table_alias()? {
                    // Snowflake also allows specifying an alias *after* parens
                    // e.g. `FROM (mytable) AS alias`
                    match &mut table_and_joins.relation {
                        TableFactor::Derived { alias, .. }
                        | TableFactor::Table { alias, .. }
                        | TableFactor::Function { alias, .. }
                        | TableFactor::UNNEST { alias, .. }
                        | TableFactor::JsonTable { alias, .. }
                        | TableFactor::XmlTable { alias, .. }
                        | TableFactor::OpenJsonTable { alias, .. }
                        | TableFactor::TableFunction { alias, .. }
                        | TableFactor::Pivot { alias, .. }
                        | TableFactor::Unpivot { alias, .. }
                        | TableFactor::MatchRecognize { alias, .. }
                        | TableFactor::SemanticView { alias, .. }
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
        } else if self.dialect.supports_values_as_table_factor()
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
            self.expect_keyword_is(Keyword::VALUES)?;

            // Snowflake and Databricks allow syntax like below:
            // SELECT * FROM VALUES (1, 'a'), (2, 'b') AS t (col1, col2)
            // where there are no parentheses around the VALUES clause.
            let values = SetExpr::Values(self.parse_values(false, false)?);
            let alias = self.maybe_parse_table_alias()?;
            Ok(TableFactor::Derived {
                lateral: false,
                subquery: Box::new(Query {
                    with: None,
                    body: Box::new(values),
                    order_by: None,
                    limit_clause: None,
                    fetch: None,
                    locks: vec![],
                    for_clause: None,
                    settings: None,
                    format_clause: None,
                    pipe_operators: vec![],
                }),
                alias,
                sample: None,
            })
        } else if dialect_of!(self is BigQueryDialect | PostgreSqlDialect | GenericDialect)
            && self.parse_keyword(Keyword::UNNEST)
        {
            self.expect_token(&Token::LParen)?;
            let array_exprs = self.parse_comma_separated(Parser::parse_expr)?;
            self.expect_token(&Token::RParen)?;

            let with_ordinality = self.parse_keywords(&[Keyword::WITH, Keyword::ORDINALITY]);
            let alias = match self.maybe_parse_table_alias() {
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
            let json_path = self.parse_value()?.value;
            self.expect_keyword_is(Keyword::COLUMNS)?;
            self.expect_token(&Token::LParen)?;
            let columns = self.parse_comma_separated(Parser::parse_json_table_column_def)?;
            self.expect_token(&Token::RParen)?;
            self.expect_token(&Token::RParen)?;
            let alias = self.maybe_parse_table_alias()?;
            Ok(TableFactor::JsonTable {
                json_expr,
                json_path,
                columns,
                alias,
            })
        } else if self.parse_keyword_with_tokens(Keyword::OPENJSON, &[Token::LParen]) {
            self.prev_token();
            self.parse_open_json_table_factor()
        } else if self.parse_keyword_with_tokens(Keyword::XMLTABLE, &[Token::LParen]) {
            self.prev_token();
            self.parse_xml_table_factor()
        } else if self.dialect.supports_semantic_view_table_factor()
            && self.peek_keyword_with_tokens(Keyword::SEMANTIC_VIEW, &[Token::LParen])
        {
            self.parse_semantic_view_table_factor()
        } else {
            let name = self.parse_object_name(true)?;

            let json_path = match self.peek_token().token {
                Token::LBracket if self.dialect.supports_partiql() => Some(self.parse_json_path()?),
                _ => None,
            };

            let partitions: Vec<Ident> = if dialect_of!(self is MySqlDialect | GenericDialect)
                && self.parse_keyword(Keyword::PARTITION)
            {
                self.parse_parenthesized_identifiers()?
            } else {
                vec![]
            };

            // Parse potential version qualifier
            let version = self.maybe_parse_table_version()?;

            // Postgres, MSSQL, ClickHouse: table-valued functions:
            let args = if self.consume_token(&Token::LParen) {
                Some(self.parse_table_function_args()?)
            } else {
                None
            };

            let with_ordinality = self.parse_keywords(&[Keyword::WITH, Keyword::ORDINALITY]);

            let mut sample = None;
            if self.dialect.supports_table_sample_before_alias() {
                if let Some(parsed_sample) = self.maybe_parse_table_sample()? {
                    sample = Some(TableSampleKind::BeforeTableAlias(parsed_sample));
                }
            }

            let alias = self.maybe_parse_table_alias()?;

            // MYSQL-specific table hints:
            let index_hints = if self.dialect.supports_table_hints() {
                self.maybe_parse(|p| p.parse_table_index_hints())?
                    .unwrap_or(vec![])
            } else {
                vec![]
            };

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

            if !self.dialect.supports_table_sample_before_alias() {
                if let Some(parsed_sample) = self.maybe_parse_table_sample()? {
                    sample = Some(TableSampleKind::AfterTableAlias(parsed_sample));
                }
            }

            let mut table = TableFactor::Table {
                name,
                alias,
                args,
                with_hints,
                version,
                partitions,
                with_ordinality,
                json_path,
                sample,
                index_hints,
            };

            while let Some(kw) = self.parse_one_of_keywords(&[Keyword::PIVOT, Keyword::UNPIVOT]) {
                table = match kw {
                    Keyword::PIVOT => self.parse_pivot_table_factor(table)?,
                    Keyword::UNPIVOT => self.parse_unpivot_table_factor(table)?,
                    unexpected_keyword => return Err(ParserError::ParserError(
                        format!("Internal parser error: unexpected keyword `{unexpected_keyword}` in pivot/unpivot"),
                    )),
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

    fn maybe_parse_table_sample(&mut self) -> Result<Option<Box<TableSample>>, ParserError> {
        let modifier = if self.parse_keyword(Keyword::TABLESAMPLE) {
            TableSampleModifier::TableSample
        } else if self.parse_keyword(Keyword::SAMPLE) {
            TableSampleModifier::Sample
        } else {
            return Ok(None);
        };
        self.parse_table_sample(modifier).map(Some)
    }

    fn parse_table_sample(
        &mut self,
        modifier: TableSampleModifier,
    ) -> Result<Box<TableSample>, ParserError> {
        let name = match self.parse_one_of_keywords(&[
            Keyword::BERNOULLI,
            Keyword::ROW,
            Keyword::SYSTEM,
            Keyword::BLOCK,
        ]) {
            Some(Keyword::BERNOULLI) => Some(TableSampleMethod::Bernoulli),
            Some(Keyword::ROW) => Some(TableSampleMethod::Row),
            Some(Keyword::SYSTEM) => Some(TableSampleMethod::System),
            Some(Keyword::BLOCK) => Some(TableSampleMethod::Block),
            _ => None,
        };

        let parenthesized = self.consume_token(&Token::LParen);

        let (quantity, bucket) = if parenthesized && self.parse_keyword(Keyword::BUCKET) {
            let selected_bucket = self.parse_number_value()?.value;
            self.expect_keywords(&[Keyword::OUT, Keyword::OF])?;
            let total = self.parse_number_value()?.value;
            let on = if self.parse_keyword(Keyword::ON) {
                Some(self.parse_expr()?)
            } else {
                None
            };
            (
                None,
                Some(TableSampleBucket {
                    bucket: selected_bucket,
                    total,
                    on,
                }),
            )
        } else {
            let value = match self.maybe_parse(|p| p.parse_expr())? {
                Some(num) => num,
                None => {
                    let next_token = self.next_token();
                    if let Token::Word(w) = next_token.token {
                        Expr::Value(Value::Placeholder(w.value).with_span(next_token.span))
                    } else {
                        return parser_err!(
                            "Expecting number or byte length e.g. 100M",
                            self.peek_token().span.start
                        );
                    }
                }
            };
            let unit = if self.parse_keyword(Keyword::ROWS) {
                Some(TableSampleUnit::Rows)
            } else if self.parse_keyword(Keyword::PERCENT) {
                Some(TableSampleUnit::Percent)
            } else {
                None
            };
            (
                Some(TableSampleQuantity {
                    parenthesized,
                    value,
                    unit,
                }),
                None,
            )
        };
        if parenthesized {
            self.expect_token(&Token::RParen)?;
        }

        let seed = if self.parse_keyword(Keyword::REPEATABLE) {
            Some(self.parse_table_sample_seed(TableSampleSeedModifier::Repeatable)?)
        } else if self.parse_keyword(Keyword::SEED) {
            Some(self.parse_table_sample_seed(TableSampleSeedModifier::Seed)?)
        } else {
            None
        };

        let offset = if self.parse_keyword(Keyword::OFFSET) {
            Some(self.parse_expr()?)
        } else {
            None
        };

        Ok(Box::new(TableSample {
            modifier,
            name,
            quantity,
            seed,
            bucket,
            offset,
        }))
    }

    fn parse_table_sample_seed(
        &mut self,
        modifier: TableSampleSeedModifier,
    ) -> Result<TableSampleSeed, ParserError> {
        self.expect_token(&Token::LParen)?;
        let value = self.parse_number_value()?.value;
        self.expect_token(&Token::RParen)?;
        Ok(TableSampleSeed { modifier, value })
    }

    /// Parses `OPENJSON( jsonExpression [ , path ] )  [ <with_clause> ]` clause,
    /// assuming the `OPENJSON` keyword was already consumed.
    fn parse_open_json_table_factor(&mut self) -> Result<TableFactor, ParserError> {
        self.expect_token(&Token::LParen)?;
        let json_expr = self.parse_expr()?;
        let json_path = if self.consume_token(&Token::Comma) {
            Some(self.parse_value()?.value)
        } else {
            None
        };
        self.expect_token(&Token::RParen)?;
        let columns = if self.parse_keyword(Keyword::WITH) {
            self.expect_token(&Token::LParen)?;
            let columns = self.parse_comma_separated(Parser::parse_openjson_table_column_def)?;
            self.expect_token(&Token::RParen)?;
            columns
        } else {
            Vec::new()
        };
        let alias = self.maybe_parse_table_alias()?;
        Ok(TableFactor::OpenJsonTable {
            json_expr,
            json_path,
            columns,
            alias,
        })
    }

    fn parse_xml_table_factor(&mut self) -> Result<TableFactor, ParserError> {
        self.expect_token(&Token::LParen)?;
        let namespaces = if self.parse_keyword(Keyword::XMLNAMESPACES) {
            self.expect_token(&Token::LParen)?;
            let namespaces = self.parse_comma_separated(Parser::parse_xml_namespace_definition)?;
            self.expect_token(&Token::RParen)?;
            self.expect_token(&Token::Comma)?;
            namespaces
        } else {
            vec![]
        };
        let row_expression = self.parse_expr()?;
        let passing = self.parse_xml_passing_clause()?;
        self.expect_keyword_is(Keyword::COLUMNS)?;
        let columns = self.parse_comma_separated(Parser::parse_xml_table_column)?;
        self.expect_token(&Token::RParen)?;
        let alias = self.maybe_parse_table_alias()?;
        Ok(TableFactor::XmlTable {
            namespaces,
            row_expression,
            passing,
            columns,
            alias,
        })
    }

    fn parse_xml_namespace_definition(&mut self) -> Result<XmlNamespaceDefinition, ParserError> {
        let uri = self.parse_expr()?;
        self.expect_keyword_is(Keyword::AS)?;
        let name = self.parse_identifier()?;
        Ok(XmlNamespaceDefinition { uri, name })
    }

    fn parse_xml_table_column(&mut self) -> Result<XmlTableColumn, ParserError> {
        let name = self.parse_identifier()?;

        let option = if self.parse_keyword(Keyword::FOR) {
            self.expect_keyword(Keyword::ORDINALITY)?;
            XmlTableColumnOption::ForOrdinality
        } else {
            let r#type = self.parse_data_type()?;
            let mut path = None;
            let mut default = None;

            if self.parse_keyword(Keyword::PATH) {
                path = Some(self.parse_expr()?);
            }

            if self.parse_keyword(Keyword::DEFAULT) {
                default = Some(self.parse_expr()?);
            }

            let not_null = self.parse_keywords(&[Keyword::NOT, Keyword::NULL]);
            if !not_null {
                // NULL is the default but can be specified explicitly
                let _ = self.parse_keyword(Keyword::NULL);
            }

            XmlTableColumnOption::NamedInfo {
                r#type,
                path,
                default,
                nullable: !not_null,
            }
        };
        Ok(XmlTableColumn { name, option })
    }

    fn parse_xml_passing_clause(&mut self) -> Result<XmlPassingClause, ParserError> {
        let mut arguments = vec![];
        if self.parse_keyword(Keyword::PASSING) {
            loop {
                let by_value =
                    self.parse_keyword(Keyword::BY) && self.expect_keyword(Keyword::VALUE).is_ok();
                let expr = self.parse_expr()?;
                let alias = if self.parse_keyword(Keyword::AS) {
                    Some(self.parse_identifier()?)
                } else {
                    None
                };
                arguments.push(XmlPassingArgument {
                    expr,
                    alias,
                    by_value,
                });
                if !self.consume_token(&Token::Comma) {
                    break;
                }
            }
        }
        Ok(XmlPassingClause { arguments })
    }

    /// Parse a [TableFactor::SemanticView]
    fn parse_semantic_view_table_factor(&mut self) -> Result<TableFactor, ParserError> {
        self.expect_keyword(Keyword::SEMANTIC_VIEW)?;
        self.expect_token(&Token::LParen)?;

        let name = self.parse_object_name(true)?;

        // Parse DIMENSIONS, METRICS, FACTS and WHERE clauses in flexible order
        let mut dimensions = Vec::new();
        let mut metrics = Vec::new();
        let mut facts = Vec::new();
        let mut where_clause = None;

        while self.peek_token().token != Token::RParen {
            if self.parse_keyword(Keyword::DIMENSIONS) {
                if !dimensions.is_empty() {
                    return Err(ParserError::ParserError(
                        "DIMENSIONS clause can only be specified once".to_string(),
                    ));
                }
                dimensions = self.parse_comma_separated(Parser::parse_wildcard_expr)?;
            } else if self.parse_keyword(Keyword::METRICS) {
                if !metrics.is_empty() {
                    return Err(ParserError::ParserError(
                        "METRICS clause can only be specified once".to_string(),
                    ));
                }
                metrics = self.parse_comma_separated(Parser::parse_wildcard_expr)?;
            } else if self.parse_keyword(Keyword::FACTS) {
                if !facts.is_empty() {
                    return Err(ParserError::ParserError(
                        "FACTS clause can only be specified once".to_string(),
                    ));
                }
                facts = self.parse_comma_separated(Parser::parse_wildcard_expr)?;
            } else if self.parse_keyword(Keyword::WHERE) {
                if where_clause.is_some() {
                    return Err(ParserError::ParserError(
                        "WHERE clause can only be specified once".to_string(),
                    ));
                }
                where_clause = Some(self.parse_expr()?);
            } else {
                return parser_err!(
                    format!(
                        "Expected one of DIMENSIONS, METRICS, FACTS or WHERE, got {}",
                        self.peek_token().token
                    ),
                    self.peek_token().span.start
                )?;
            }
        }

        self.expect_token(&Token::RParen)?;

        let alias = self.maybe_parse_table_alias()?;

        Ok(TableFactor::SemanticView {
            name,
            dimensions,
            metrics,
            facts,
            where_clause,
            alias,
        })
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
                let alias = p.parse_identifier()?;
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
                    Some(AfterMatchSkip::ToFirst(self.parse_identifier()?))
                } else if self.parse_keywords(&[Keyword::TO, Keyword::LAST]) {
                    Some(AfterMatchSkip::ToLast(self.parse_identifier()?))
                } else {
                    let found = self.next_token();
                    return self.expected("after match skip option", found);
                }
            } else {
                None
            };

        self.expect_keyword_is(Keyword::PATTERN)?;
        let pattern = self.parse_parenthesized(Self::parse_pattern)?;

        self.expect_keyword_is(Keyword::DEFINE)?;

        let symbols = self.parse_comma_separated(|p| {
            let symbol = p.parse_identifier()?;
            p.expect_keyword_is(Keyword::AS)?;
            let definition = p.parse_expr()?;
            Ok(SymbolDefinition { symbol, definition })
        })?;

        self.expect_token(&Token::RParen)?;

        let alias = self.maybe_parse_table_alias()?;

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
                let symbol = self.parse_identifier().map(MatchRecognizeSymbol::Named)?;
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
                    p.parse_identifier().map(MatchRecognizeSymbol::Named)
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
                self.parse_identifier()
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
                            RepetitionQuantifier::AtMost(Self::parse(n, token.span.start)?)
                        }
                        Token::Number(n, _) if self.consume_token(&Token::Comma) => {
                            let next_token = self.next_token();
                            match next_token.token {
                                Token::Number(m, _) => {
                                    self.expect_token(&Token::RBrace)?;
                                    RepetitionQuantifier::Range(
                                        Self::parse(n, token.span.start)?,
                                        Self::parse(m, token.span.start)?,
                                    )
                                }
                                Token::RBrace => {
                                    RepetitionQuantifier::AtLeast(Self::parse(n, token.span.start)?)
                                }
                                _ => {
                                    return self.expected("} or upper bound", next_token);
                                }
                            }
                        }
                        Token::Number(n, _) => {
                            self.expect_token(&Token::RBrace)?;
                            RepetitionQuantifier::Exactly(Self::parse(n, token.span.start)?)
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

    /// Parses a the timestamp version specifier (i.e. query historical data)
    pub fn maybe_parse_table_version(&mut self) -> Result<Option<TableVersion>, ParserError> {
        if self.dialect.supports_table_versioning() {
            if self.parse_keywords(&[Keyword::FOR, Keyword::SYSTEM_TIME, Keyword::AS, Keyword::OF])
            {
                let expr = self.parse_expr()?;
                return Ok(Some(TableVersion::ForSystemTimeAsOf(expr)));
            } else if self.peek_keyword(Keyword::AT) || self.peek_keyword(Keyword::BEFORE) {
                let func_name = self.parse_object_name(true)?;
                let func = self.parse_function(func_name)?;
                return Ok(Some(TableVersion::Function(func)));
            } else if self.parse_keywords(&[Keyword::TIMESTAMP, Keyword::AS, Keyword::OF]) {
                let expr = self.parse_expr()?;
                return Ok(Some(TableVersion::TimestampAsOf(expr)));
            } else if self.parse_keywords(&[Keyword::VERSION, Keyword::AS, Keyword::OF]) {
                let expr = Expr::Value(self.parse_number_value()?);
                return Ok(Some(TableVersion::VersionAsOf(expr)));
            }
        }
        Ok(None)
    }

    /// Parses MySQL's JSON_TABLE column definition.
    /// For example: `id INT EXISTS PATH '$' DEFAULT '0' ON EMPTY ERROR ON ERROR`
    pub fn parse_json_table_column_def(&mut self) -> Result<JsonTableColumn, ParserError> {
        if self.parse_keyword(Keyword::NESTED) {
            let _has_path_keyword = self.parse_keyword(Keyword::PATH);
            let path = self.parse_value()?.value;
            self.expect_keyword_is(Keyword::COLUMNS)?;
            let columns = self.parse_parenthesized(|p| {
                p.parse_comma_separated(Self::parse_json_table_column_def)
            })?;
            return Ok(JsonTableColumn::Nested(JsonTableNestedColumn {
                path,
                columns,
            }));
        }
        let name = self.parse_identifier()?;
        if self.parse_keyword(Keyword::FOR) {
            self.expect_keyword_is(Keyword::ORDINALITY)?;
            return Ok(JsonTableColumn::ForOrdinality(name));
        }
        let r#type = self.parse_data_type()?;
        let exists = self.parse_keyword(Keyword::EXISTS);
        self.expect_keyword_is(Keyword::PATH)?;
        let path = self.parse_value()?.value;
        let mut on_empty = None;
        let mut on_error = None;
        while let Some(error_handling) = self.parse_json_table_column_error_handling()? {
            if self.parse_keyword(Keyword::EMPTY) {
                on_empty = Some(error_handling);
            } else {
                self.expect_keyword_is(Keyword::ERROR)?;
                on_error = Some(error_handling);
            }
        }
        Ok(JsonTableColumn::Named(JsonTableNamedColumn {
            name,
            r#type,
            path,
            exists,
            on_empty,
            on_error,
        }))
    }

    /// Parses MSSQL's `OPENJSON WITH` column definition.
    ///
    /// ```sql
    /// colName type [ column_path ] [ AS JSON ]
    /// ```
    ///
    /// Reference: <https://learn.microsoft.com/en-us/sql/t-sql/functions/openjson-transact-sql?view=sql-server-ver16#syntax>
    pub fn parse_openjson_table_column_def(&mut self) -> Result<OpenJsonTableColumn, ParserError> {
        let name = self.parse_identifier()?;
        let r#type = self.parse_data_type()?;
        let path = if let Token::SingleQuotedString(path) = self.peek_token().token {
            self.next_token();
            Some(path)
        } else {
            None
        };
        let as_json = self.parse_keyword(Keyword::AS);
        if as_json {
            self.expect_keyword_is(Keyword::JSON)?;
        }
        Ok(OpenJsonTableColumn {
            name,
            r#type,
            path,
            as_json,
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
            JsonTableColumnErrorHandling::Default(self.parse_value()?.value)
        } else {
            return Ok(None);
        };
        self.expect_keyword_is(Keyword::ON)?;
        Ok(Some(res))
    }

    /// Parse a derived table factor (a parenthesized subquery), handling optional LATERAL.
    pub fn parse_derived_table_factor(
        &mut self,
        lateral: IsLateral,
    ) -> Result<TableFactor, ParserError> {
        let subquery = self.parse_query()?;
        self.expect_token(&Token::RParen)?;
        let alias = self.maybe_parse_table_alias()?;

        // Parse optional SAMPLE clause after alias
        let sample = self
            .maybe_parse_table_sample()?
            .map(TableSampleKind::AfterTableAlias);

        Ok(TableFactor::Derived {
            lateral: match lateral {
                Lateral => true,
                NotLateral => false,
            },
            subquery,
            alias,
            sample,
        })
    }

    fn parse_aliased_function_call(&mut self) -> Result<ExprWithAlias, ParserError> {
        let function_name = match self.next_token().token {
            Token::Word(w) => Ok(w.value),
            _ => self.expected("a function identifier", self.peek_token()),
        }?;
        let expr = self.parse_function(ObjectName::from(vec![Ident::new(function_name)]))?;
        let alias = if self.parse_keyword(Keyword::AS) {
            Some(self.parse_identifier()?)
        } else {
            None
        };

        Ok(ExprWithAlias { expr, alias })
    }
    /// Parses an expression with an optional alias
    ///
    /// Examples:
    ///
    /// ```sql
    /// SUM(price) AS total_price
    /// ```
    /// ```sql
    /// SUM(price)
    /// ```
    ///
    /// Example
    /// ```
    /// # use sqlparser::parser::{Parser, ParserError};
    /// # use sqlparser::dialect::GenericDialect;
    /// # fn main() ->Result<(), ParserError> {
    /// let sql = r#"SUM("a") as "b""#;
    /// let mut parser = Parser::new(&GenericDialect).try_with_sql(sql)?;
    /// let expr_with_alias = parser.parse_expr_with_alias()?;
    /// assert_eq!(Some("b".to_string()), expr_with_alias.alias.map(|x|x.value));
    /// # Ok(())
    /// # }
    pub fn parse_expr_with_alias(&mut self) -> Result<ExprWithAlias, ParserError> {
        let expr = self.parse_expr()?;
        let alias = if self.parse_keyword(Keyword::AS) {
            Some(self.parse_identifier()?)
        } else {
            None
        };

        Ok(ExprWithAlias { expr, alias })
    }

    /// Parse a PIVOT table factor (ClickHouse/Oracle style pivot), returning a TableFactor.
    pub fn parse_pivot_table_factor(
        &mut self,
        table: TableFactor,
    ) -> Result<TableFactor, ParserError> {
        self.expect_token(&Token::LParen)?;
        let aggregate_functions = self.parse_comma_separated(Self::parse_aliased_function_call)?;
        self.expect_keyword_is(Keyword::FOR)?;
        let value_column = if self.peek_token_ref().token == Token::LParen {
            self.parse_parenthesized_column_list_inner(Mandatory, false, |p| {
                p.parse_subexpr(self.dialect.prec_value(Precedence::Between))
            })?
        } else {
            vec![self.parse_subexpr(self.dialect.prec_value(Precedence::Between))?]
        };
        self.expect_keyword_is(Keyword::IN)?;

        self.expect_token(&Token::LParen)?;
        let value_source = if self.parse_keyword(Keyword::ANY) {
            let order_by = if self.parse_keywords(&[Keyword::ORDER, Keyword::BY]) {
                self.parse_comma_separated(Parser::parse_order_by_expr)?
            } else {
                vec![]
            };
            PivotValueSource::Any(order_by)
        } else if self.peek_sub_query() {
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
        let alias = self.maybe_parse_table_alias()?;
        Ok(TableFactor::Pivot {
            table: Box::new(table),
            aggregate_functions,
            value_column,
            value_source,
            default_on_null,
            alias,
        })
    }

    /// Parse an UNPIVOT table factor, returning a TableFactor.
    pub fn parse_unpivot_table_factor(
        &mut self,
        table: TableFactor,
    ) -> Result<TableFactor, ParserError> {
        let null_inclusion = if self.parse_keyword(Keyword::INCLUDE) {
            self.expect_keyword_is(Keyword::NULLS)?;
            Some(NullInclusion::IncludeNulls)
        } else if self.parse_keyword(Keyword::EXCLUDE) {
            self.expect_keyword_is(Keyword::NULLS)?;
            Some(NullInclusion::ExcludeNulls)
        } else {
            None
        };
        self.expect_token(&Token::LParen)?;
        let value = self.parse_expr()?;
        self.expect_keyword_is(Keyword::FOR)?;
        let name = self.parse_identifier()?;
        self.expect_keyword_is(Keyword::IN)?;
        let columns = self.parse_parenthesized_column_list_inner(Mandatory, false, |p| {
            p.parse_expr_with_alias()
        })?;
        self.expect_token(&Token::RParen)?;
        let alias = self.maybe_parse_table_alias()?;
        Ok(TableFactor::Unpivot {
            table: Box::new(table),
            value,
            null_inclusion,
            name,
            columns,
            alias,
        })
    }

    /// Parse a JOIN constraint (`NATURAL`, `ON <expr>`, `USING (...)`, or no constraint).
    pub fn parse_join_constraint(&mut self, natural: bool) -> Result<JoinConstraint, ParserError> {
        if natural {
            Ok(JoinConstraint::Natural)
        } else if self.parse_keyword(Keyword::ON) {
            let constraint = self.parse_expr()?;
            Ok(JoinConstraint::On(constraint))
        } else if self.parse_keyword(Keyword::USING) {
            let columns = self.parse_parenthesized_qualified_column_list(Mandatory, false)?;
            Ok(JoinConstraint::Using(columns))
        } else {
            Ok(JoinConstraint::None)
            //self.expected("ON, or USING after JOIN", self.peek_token())
        }
    }

    /// Parse a GRANT statement.
    pub fn parse_grant(&mut self) -> Result<Statement, ParserError> {
        let (privileges, objects) = self.parse_grant_deny_revoke_privileges_objects()?;

        self.expect_keyword_is(Keyword::TO)?;
        let grantees = self.parse_grantees()?;

        let with_grant_option =
            self.parse_keywords(&[Keyword::WITH, Keyword::GRANT, Keyword::OPTION]);

        let current_grants =
            if self.parse_keywords(&[Keyword::COPY, Keyword::CURRENT, Keyword::GRANTS]) {
                Some(CurrentGrantsKind::CopyCurrentGrants)
            } else if self.parse_keywords(&[Keyword::REVOKE, Keyword::CURRENT, Keyword::GRANTS]) {
                Some(CurrentGrantsKind::RevokeCurrentGrants)
            } else {
                None
            };

        let as_grantor = if self.parse_keywords(&[Keyword::AS]) {
            Some(self.parse_identifier()?)
        } else {
            None
        };

        let granted_by = if self.parse_keywords(&[Keyword::GRANTED, Keyword::BY]) {
            Some(self.parse_identifier()?)
        } else {
            None
        };

        Ok(Statement::Grant {
            privileges,
            objects,
            grantees,
            with_grant_option,
            as_grantor,
            granted_by,
            current_grants,
        })
    }

    fn parse_grantees(&mut self) -> Result<Vec<Grantee>, ParserError> {
        let mut values = vec![];
        let mut grantee_type = GranteesType::None;
        loop {
            let new_grantee_type = if self.parse_keyword(Keyword::ROLE) {
                GranteesType::Role
            } else if self.parse_keyword(Keyword::USER) {
                GranteesType::User
            } else if self.parse_keyword(Keyword::SHARE) {
                GranteesType::Share
            } else if self.parse_keyword(Keyword::GROUP) {
                GranteesType::Group
            } else if self.parse_keyword(Keyword::PUBLIC) {
                GranteesType::Public
            } else if self.parse_keywords(&[Keyword::DATABASE, Keyword::ROLE]) {
                GranteesType::DatabaseRole
            } else if self.parse_keywords(&[Keyword::APPLICATION, Keyword::ROLE]) {
                GranteesType::ApplicationRole
            } else if self.parse_keyword(Keyword::APPLICATION) {
                GranteesType::Application
            } else {
                grantee_type.clone() // keep from previous iteraton, if not specified
            };

            if self
                .dialect
                .get_reserved_grantees_types()
                .contains(&new_grantee_type)
            {
                self.prev_token();
            } else {
                grantee_type = new_grantee_type;
            }

            let grantee = if grantee_type == GranteesType::Public {
                Grantee {
                    grantee_type: grantee_type.clone(),
                    name: None,
                }
            } else {
                let mut name = self.parse_grantee_name()?;
                if self.consume_token(&Token::Colon) {
                    // Redshift supports namespace prefix for external users and groups:
                    // <Namespace>:<GroupName> or <Namespace>:<UserName>
                    // https://docs.aws.amazon.com/redshift/latest/mgmt/redshift-iam-access-control-native-idp.html
                    let ident = self.parse_identifier()?;
                    if let GranteeName::ObjectName(namespace) = name {
                        name = GranteeName::ObjectName(ObjectName::from(vec![Ident::new(
                            format!("{namespace}:{ident}"),
                        )]));
                    };
                }
                Grantee {
                    grantee_type: grantee_type.clone(),
                    name: Some(name),
                }
            };

            values.push(grantee);

            if !self.consume_token(&Token::Comma) {
                break;
            }
        }

        Ok(values)
    }

    /// Parse privileges and optional target objects for GRANT/DENY/REVOKE statements.
    pub fn parse_grant_deny_revoke_privileges_objects(
        &mut self,
    ) -> Result<(Privileges, Option<GrantObjects>), ParserError> {
        let privileges = if self.parse_keyword(Keyword::ALL) {
            Privileges::All {
                with_privileges_keyword: self.parse_keyword(Keyword::PRIVILEGES),
            }
        } else {
            let actions = self.parse_actions_list()?;
            Privileges::Actions(actions)
        };

        let objects = if self.parse_keyword(Keyword::ON) {
            if self.parse_keywords(&[Keyword::ALL, Keyword::TABLES, Keyword::IN, Keyword::SCHEMA]) {
                Some(GrantObjects::AllTablesInSchema {
                    schemas: self.parse_comma_separated(|p| p.parse_object_name(false))?,
                })
            } else if self.parse_keywords(&[
                Keyword::ALL,
                Keyword::EXTERNAL,
                Keyword::TABLES,
                Keyword::IN,
                Keyword::SCHEMA,
            ]) {
                Some(GrantObjects::AllExternalTablesInSchema {
                    schemas: self.parse_comma_separated(|p| p.parse_object_name(false))?,
                })
            } else if self.parse_keywords(&[
                Keyword::ALL,
                Keyword::VIEWS,
                Keyword::IN,
                Keyword::SCHEMA,
            ]) {
                Some(GrantObjects::AllViewsInSchema {
                    schemas: self.parse_comma_separated(|p| p.parse_object_name(false))?,
                })
            } else if self.parse_keywords(&[
                Keyword::ALL,
                Keyword::MATERIALIZED,
                Keyword::VIEWS,
                Keyword::IN,
                Keyword::SCHEMA,
            ]) {
                Some(GrantObjects::AllMaterializedViewsInSchema {
                    schemas: self.parse_comma_separated(|p| p.parse_object_name(false))?,
                })
            } else if self.parse_keywords(&[
                Keyword::ALL,
                Keyword::FUNCTIONS,
                Keyword::IN,
                Keyword::SCHEMA,
            ]) {
                Some(GrantObjects::AllFunctionsInSchema {
                    schemas: self.parse_comma_separated(|p| p.parse_object_name(false))?,
                })
            } else if self.parse_keywords(&[
                Keyword::FUTURE,
                Keyword::SCHEMAS,
                Keyword::IN,
                Keyword::DATABASE,
            ]) {
                Some(GrantObjects::FutureSchemasInDatabase {
                    databases: self.parse_comma_separated(|p| p.parse_object_name(false))?,
                })
            } else if self.parse_keywords(&[
                Keyword::FUTURE,
                Keyword::TABLES,
                Keyword::IN,
                Keyword::SCHEMA,
            ]) {
                Some(GrantObjects::FutureTablesInSchema {
                    schemas: self.parse_comma_separated(|p| p.parse_object_name(false))?,
                })
            } else if self.parse_keywords(&[
                Keyword::FUTURE,
                Keyword::EXTERNAL,
                Keyword::TABLES,
                Keyword::IN,
                Keyword::SCHEMA,
            ]) {
                Some(GrantObjects::FutureExternalTablesInSchema {
                    schemas: self.parse_comma_separated(|p| p.parse_object_name(false))?,
                })
            } else if self.parse_keywords(&[
                Keyword::FUTURE,
                Keyword::VIEWS,
                Keyword::IN,
                Keyword::SCHEMA,
            ]) {
                Some(GrantObjects::FutureViewsInSchema {
                    schemas: self.parse_comma_separated(|p| p.parse_object_name(false))?,
                })
            } else if self.parse_keywords(&[
                Keyword::FUTURE,
                Keyword::MATERIALIZED,
                Keyword::VIEWS,
                Keyword::IN,
                Keyword::SCHEMA,
            ]) {
                Some(GrantObjects::FutureMaterializedViewsInSchema {
                    schemas: self.parse_comma_separated(|p| p.parse_object_name(false))?,
                })
            } else if self.parse_keywords(&[
                Keyword::ALL,
                Keyword::SEQUENCES,
                Keyword::IN,
                Keyword::SCHEMA,
            ]) {
                Some(GrantObjects::AllSequencesInSchema {
                    schemas: self.parse_comma_separated(|p| p.parse_object_name(false))?,
                })
            } else if self.parse_keywords(&[
                Keyword::FUTURE,
                Keyword::SEQUENCES,
                Keyword::IN,
                Keyword::SCHEMA,
            ]) {
                Some(GrantObjects::FutureSequencesInSchema {
                    schemas: self.parse_comma_separated(|p| p.parse_object_name(false))?,
                })
            } else if self.parse_keywords(&[Keyword::RESOURCE, Keyword::MONITOR]) {
                Some(GrantObjects::ResourceMonitors(
                    self.parse_comma_separated(|p| p.parse_object_name(false))?,
                ))
            } else if self.parse_keywords(&[Keyword::COMPUTE, Keyword::POOL]) {
                Some(GrantObjects::ComputePools(
                    self.parse_comma_separated(|p| p.parse_object_name(false))?,
                ))
            } else if self.parse_keywords(&[Keyword::FAILOVER, Keyword::GROUP]) {
                Some(GrantObjects::FailoverGroup(
                    self.parse_comma_separated(|p| p.parse_object_name(false))?,
                ))
            } else if self.parse_keywords(&[Keyword::REPLICATION, Keyword::GROUP]) {
                Some(GrantObjects::ReplicationGroup(
                    self.parse_comma_separated(|p| p.parse_object_name(false))?,
                ))
            } else if self.parse_keywords(&[Keyword::EXTERNAL, Keyword::VOLUME]) {
                Some(GrantObjects::ExternalVolumes(
                    self.parse_comma_separated(|p| p.parse_object_name(false))?,
                ))
            } else {
                let object_type = self.parse_one_of_keywords(&[
                    Keyword::SEQUENCE,
                    Keyword::DATABASE,
                    Keyword::SCHEMA,
                    Keyword::TABLE,
                    Keyword::VIEW,
                    Keyword::WAREHOUSE,
                    Keyword::INTEGRATION,
                    Keyword::VIEW,
                    Keyword::WAREHOUSE,
                    Keyword::INTEGRATION,
                    Keyword::USER,
                    Keyword::CONNECTION,
                    Keyword::PROCEDURE,
                    Keyword::FUNCTION,
                ]);
                let objects =
                    self.parse_comma_separated(|p| p.parse_object_name_inner(false, true));
                match object_type {
                    Some(Keyword::DATABASE) => Some(GrantObjects::Databases(objects?)),
                    Some(Keyword::SCHEMA) => Some(GrantObjects::Schemas(objects?)),
                    Some(Keyword::SEQUENCE) => Some(GrantObjects::Sequences(objects?)),
                    Some(Keyword::WAREHOUSE) => Some(GrantObjects::Warehouses(objects?)),
                    Some(Keyword::INTEGRATION) => Some(GrantObjects::Integrations(objects?)),
                    Some(Keyword::VIEW) => Some(GrantObjects::Views(objects?)),
                    Some(Keyword::USER) => Some(GrantObjects::Users(objects?)),
                    Some(Keyword::CONNECTION) => Some(GrantObjects::Connections(objects?)),
                    kw @ (Some(Keyword::PROCEDURE) | Some(Keyword::FUNCTION)) => {
                        if let Some(name) = objects?.first() {
                            self.parse_grant_procedure_or_function(name, &kw)?
                        } else {
                            self.expected("procedure or function name", self.peek_token())?
                        }
                    }
                    Some(Keyword::TABLE) | None => Some(GrantObjects::Tables(objects?)),
                    Some(unexpected_keyword) => return Err(ParserError::ParserError(
                        format!("Internal parser error: unexpected keyword `{unexpected_keyword}` in grant objects"),
                    )),
                }
            }
        } else {
            None
        };

        Ok((privileges, objects))
    }

    fn parse_grant_procedure_or_function(
        &mut self,
        name: &ObjectName,
        kw: &Option<Keyword>,
    ) -> Result<Option<GrantObjects>, ParserError> {
        let arg_types = if self.consume_token(&Token::LParen) {
            let list = self.parse_comma_separated0(Self::parse_data_type, Token::RParen)?;
            self.expect_token(&Token::RParen)?;
            list
        } else {
            vec![]
        };
        match kw {
            Some(Keyword::PROCEDURE) => Ok(Some(GrantObjects::Procedure {
                name: name.clone(),
                arg_types,
            })),
            Some(Keyword::FUNCTION) => Ok(Some(GrantObjects::Function {
                name: name.clone(),
                arg_types,
            })),
            _ => self.expected("procedure or function keywords", self.peek_token())?,
        }
    }

    /// Parse a single grantable permission/action (used within GRANT statements).
    pub fn parse_grant_permission(&mut self) -> Result<Action, ParserError> {
        fn parse_columns(parser: &mut Parser) -> Result<Option<Vec<Ident>>, ParserError> {
            let columns = parser.parse_parenthesized_column_list(Optional, false)?;
            if columns.is_empty() {
                Ok(None)
            } else {
                Ok(Some(columns))
            }
        }

        // Multi-word privileges
        if self.parse_keywords(&[Keyword::IMPORTED, Keyword::PRIVILEGES]) {
            Ok(Action::ImportedPrivileges)
        } else if self.parse_keywords(&[Keyword::ADD, Keyword::SEARCH, Keyword::OPTIMIZATION]) {
            Ok(Action::AddSearchOptimization)
        } else if self.parse_keywords(&[Keyword::ATTACH, Keyword::LISTING]) {
            Ok(Action::AttachListing)
        } else if self.parse_keywords(&[Keyword::ATTACH, Keyword::POLICY]) {
            Ok(Action::AttachPolicy)
        } else if self.parse_keywords(&[Keyword::BIND, Keyword::SERVICE, Keyword::ENDPOINT]) {
            Ok(Action::BindServiceEndpoint)
        } else if self.parse_keywords(&[Keyword::DATABASE, Keyword::ROLE]) {
            let role = self.parse_object_name(false)?;
            Ok(Action::DatabaseRole { role })
        } else if self.parse_keywords(&[Keyword::EVOLVE, Keyword::SCHEMA]) {
            Ok(Action::EvolveSchema)
        } else if self.parse_keywords(&[Keyword::IMPORT, Keyword::SHARE]) {
            Ok(Action::ImportShare)
        } else if self.parse_keywords(&[Keyword::MANAGE, Keyword::VERSIONS]) {
            Ok(Action::ManageVersions)
        } else if self.parse_keywords(&[Keyword::MANAGE, Keyword::RELEASES]) {
            Ok(Action::ManageReleases)
        } else if self.parse_keywords(&[Keyword::OVERRIDE, Keyword::SHARE, Keyword::RESTRICTIONS]) {
            Ok(Action::OverrideShareRestrictions)
        } else if self.parse_keywords(&[
            Keyword::PURCHASE,
            Keyword::DATA,
            Keyword::EXCHANGE,
            Keyword::LISTING,
        ]) {
            Ok(Action::PurchaseDataExchangeListing)
        } else if self.parse_keywords(&[Keyword::RESOLVE, Keyword::ALL]) {
            Ok(Action::ResolveAll)
        } else if self.parse_keywords(&[Keyword::READ, Keyword::SESSION]) {
            Ok(Action::ReadSession)

        // Single-word privileges
        } else if self.parse_keyword(Keyword::APPLY) {
            let apply_type = self.parse_action_apply_type()?;
            Ok(Action::Apply { apply_type })
        } else if self.parse_keyword(Keyword::APPLYBUDGET) {
            Ok(Action::ApplyBudget)
        } else if self.parse_keyword(Keyword::AUDIT) {
            Ok(Action::Audit)
        } else if self.parse_keyword(Keyword::CONNECT) {
            Ok(Action::Connect)
        } else if self.parse_keyword(Keyword::CREATE) {
            let obj_type = self.maybe_parse_action_create_object_type();
            Ok(Action::Create { obj_type })
        } else if self.parse_keyword(Keyword::DELETE) {
            Ok(Action::Delete)
        } else if self.parse_keyword(Keyword::EXEC) {
            let obj_type = self.maybe_parse_action_execute_obj_type();
            Ok(Action::Exec { obj_type })
        } else if self.parse_keyword(Keyword::EXECUTE) {
            let obj_type = self.maybe_parse_action_execute_obj_type();
            Ok(Action::Execute { obj_type })
        } else if self.parse_keyword(Keyword::FAILOVER) {
            Ok(Action::Failover)
        } else if self.parse_keyword(Keyword::INSERT) {
            Ok(Action::Insert {
                columns: parse_columns(self)?,
            })
        } else if self.parse_keyword(Keyword::MANAGE) {
            let manage_type = self.parse_action_manage_type()?;
            Ok(Action::Manage { manage_type })
        } else if self.parse_keyword(Keyword::MODIFY) {
            let modify_type = self.parse_action_modify_type();
            Ok(Action::Modify { modify_type })
        } else if self.parse_keyword(Keyword::MONITOR) {
            let monitor_type = self.parse_action_monitor_type();
            Ok(Action::Monitor { monitor_type })
        } else if self.parse_keyword(Keyword::OPERATE) {
            Ok(Action::Operate)
        } else if self.parse_keyword(Keyword::REFERENCES) {
            Ok(Action::References {
                columns: parse_columns(self)?,
            })
        } else if self.parse_keyword(Keyword::READ) {
            Ok(Action::Read)
        } else if self.parse_keyword(Keyword::REPLICATE) {
            Ok(Action::Replicate)
        } else if self.parse_keyword(Keyword::ROLE) {
            let role = self.parse_object_name(false)?;
            Ok(Action::Role { role })
        } else if self.parse_keyword(Keyword::SELECT) {
            Ok(Action::Select {
                columns: parse_columns(self)?,
            })
        } else if self.parse_keyword(Keyword::TEMPORARY) {
            Ok(Action::Temporary)
        } else if self.parse_keyword(Keyword::TRIGGER) {
            Ok(Action::Trigger)
        } else if self.parse_keyword(Keyword::TRUNCATE) {
            Ok(Action::Truncate)
        } else if self.parse_keyword(Keyword::UPDATE) {
            Ok(Action::Update {
                columns: parse_columns(self)?,
            })
        } else if self.parse_keyword(Keyword::USAGE) {
            Ok(Action::Usage)
        } else if self.parse_keyword(Keyword::OWNERSHIP) {
            Ok(Action::Ownership)
        } else if self.parse_keyword(Keyword::DROP) {
            Ok(Action::Drop)
        } else {
            self.expected("a privilege keyword", self.peek_token())?
        }
    }

    fn maybe_parse_action_create_object_type(&mut self) -> Option<ActionCreateObjectType> {
        // Multi-word object types
        if self.parse_keywords(&[Keyword::APPLICATION, Keyword::PACKAGE]) {
            Some(ActionCreateObjectType::ApplicationPackage)
        } else if self.parse_keywords(&[Keyword::COMPUTE, Keyword::POOL]) {
            Some(ActionCreateObjectType::ComputePool)
        } else if self.parse_keywords(&[Keyword::DATA, Keyword::EXCHANGE, Keyword::LISTING]) {
            Some(ActionCreateObjectType::DataExchangeListing)
        } else if self.parse_keywords(&[Keyword::EXTERNAL, Keyword::VOLUME]) {
            Some(ActionCreateObjectType::ExternalVolume)
        } else if self.parse_keywords(&[Keyword::FAILOVER, Keyword::GROUP]) {
            Some(ActionCreateObjectType::FailoverGroup)
        } else if self.parse_keywords(&[Keyword::NETWORK, Keyword::POLICY]) {
            Some(ActionCreateObjectType::NetworkPolicy)
        } else if self.parse_keywords(&[Keyword::ORGANIZATION, Keyword::LISTING]) {
            Some(ActionCreateObjectType::OrganiationListing)
        } else if self.parse_keywords(&[Keyword::REPLICATION, Keyword::GROUP]) {
            Some(ActionCreateObjectType::ReplicationGroup)
        }
        // Single-word object types
        else if self.parse_keyword(Keyword::ACCOUNT) {
            Some(ActionCreateObjectType::Account)
        } else if self.parse_keyword(Keyword::APPLICATION) {
            Some(ActionCreateObjectType::Application)
        } else if self.parse_keyword(Keyword::DATABASE) {
            Some(ActionCreateObjectType::Database)
        } else if self.parse_keyword(Keyword::INTEGRATION) {
            Some(ActionCreateObjectType::Integration)
        } else if self.parse_keyword(Keyword::ROLE) {
            Some(ActionCreateObjectType::Role)
        } else if self.parse_keyword(Keyword::SCHEMA) {
            Some(ActionCreateObjectType::Schema)
        } else if self.parse_keyword(Keyword::SHARE) {
            Some(ActionCreateObjectType::Share)
        } else if self.parse_keyword(Keyword::USER) {
            Some(ActionCreateObjectType::User)
        } else if self.parse_keyword(Keyword::WAREHOUSE) {
            Some(ActionCreateObjectType::Warehouse)
        } else {
            None
        }
    }

    fn parse_action_apply_type(&mut self) -> Result<ActionApplyType, ParserError> {
        if self.parse_keywords(&[Keyword::AGGREGATION, Keyword::POLICY]) {
            Ok(ActionApplyType::AggregationPolicy)
        } else if self.parse_keywords(&[Keyword::AUTHENTICATION, Keyword::POLICY]) {
            Ok(ActionApplyType::AuthenticationPolicy)
        } else if self.parse_keywords(&[Keyword::JOIN, Keyword::POLICY]) {
            Ok(ActionApplyType::JoinPolicy)
        } else if self.parse_keywords(&[Keyword::MASKING, Keyword::POLICY]) {
            Ok(ActionApplyType::MaskingPolicy)
        } else if self.parse_keywords(&[Keyword::PACKAGES, Keyword::POLICY]) {
            Ok(ActionApplyType::PackagesPolicy)
        } else if self.parse_keywords(&[Keyword::PASSWORD, Keyword::POLICY]) {
            Ok(ActionApplyType::PasswordPolicy)
        } else if self.parse_keywords(&[Keyword::PROJECTION, Keyword::POLICY]) {
            Ok(ActionApplyType::ProjectionPolicy)
        } else if self.parse_keywords(&[Keyword::ROW, Keyword::ACCESS, Keyword::POLICY]) {
            Ok(ActionApplyType::RowAccessPolicy)
        } else if self.parse_keywords(&[Keyword::SESSION, Keyword::POLICY]) {
            Ok(ActionApplyType::SessionPolicy)
        } else if self.parse_keyword(Keyword::TAG) {
            Ok(ActionApplyType::Tag)
        } else {
            self.expected("GRANT APPLY type", self.peek_token())
        }
    }

    fn maybe_parse_action_execute_obj_type(&mut self) -> Option<ActionExecuteObjectType> {
        if self.parse_keywords(&[Keyword::DATA, Keyword::METRIC, Keyword::FUNCTION]) {
            Some(ActionExecuteObjectType::DataMetricFunction)
        } else if self.parse_keywords(&[Keyword::MANAGED, Keyword::ALERT]) {
            Some(ActionExecuteObjectType::ManagedAlert)
        } else if self.parse_keywords(&[Keyword::MANAGED, Keyword::TASK]) {
            Some(ActionExecuteObjectType::ManagedTask)
        } else if self.parse_keyword(Keyword::ALERT) {
            Some(ActionExecuteObjectType::Alert)
        } else if self.parse_keyword(Keyword::TASK) {
            Some(ActionExecuteObjectType::Task)
        } else {
            None
        }
    }

    fn parse_action_manage_type(&mut self) -> Result<ActionManageType, ParserError> {
        if self.parse_keywords(&[Keyword::ACCOUNT, Keyword::SUPPORT, Keyword::CASES]) {
            Ok(ActionManageType::AccountSupportCases)
        } else if self.parse_keywords(&[Keyword::EVENT, Keyword::SHARING]) {
            Ok(ActionManageType::EventSharing)
        } else if self.parse_keywords(&[Keyword::LISTING, Keyword::AUTO, Keyword::FULFILLMENT]) {
            Ok(ActionManageType::ListingAutoFulfillment)
        } else if self.parse_keywords(&[Keyword::ORGANIZATION, Keyword::SUPPORT, Keyword::CASES]) {
            Ok(ActionManageType::OrganizationSupportCases)
        } else if self.parse_keywords(&[Keyword::USER, Keyword::SUPPORT, Keyword::CASES]) {
            Ok(ActionManageType::UserSupportCases)
        } else if self.parse_keyword(Keyword::GRANTS) {
            Ok(ActionManageType::Grants)
        } else if self.parse_keyword(Keyword::WAREHOUSES) {
            Ok(ActionManageType::Warehouses)
        } else {
            self.expected("GRANT MANAGE type", self.peek_token())
        }
    }

    fn parse_action_modify_type(&mut self) -> Option<ActionModifyType> {
        if self.parse_keywords(&[Keyword::LOG, Keyword::LEVEL]) {
            Some(ActionModifyType::LogLevel)
        } else if self.parse_keywords(&[Keyword::TRACE, Keyword::LEVEL]) {
            Some(ActionModifyType::TraceLevel)
        } else if self.parse_keywords(&[Keyword::SESSION, Keyword::LOG, Keyword::LEVEL]) {
            Some(ActionModifyType::SessionLogLevel)
        } else if self.parse_keywords(&[Keyword::SESSION, Keyword::TRACE, Keyword::LEVEL]) {
            Some(ActionModifyType::SessionTraceLevel)
        } else {
            None
        }
    }

    fn parse_action_monitor_type(&mut self) -> Option<ActionMonitorType> {
        if self.parse_keyword(Keyword::EXECUTION) {
            Some(ActionMonitorType::Execution)
        } else if self.parse_keyword(Keyword::SECURITY) {
            Some(ActionMonitorType::Security)
        } else if self.parse_keyword(Keyword::USAGE) {
            Some(ActionMonitorType::Usage)
        } else {
            None
        }
    }

    /// Parse a grantee name, possibly with a host qualifier (user@host).
    pub fn parse_grantee_name(&mut self) -> Result<GranteeName, ParserError> {
        let mut name = self.parse_object_name(false)?;
        if self.dialect.supports_user_host_grantee()
            && name.0.len() == 1
            && name.0[0].as_ident().is_some()
            && self.consume_token(&Token::AtSign)
        {
            let user = name.0.pop().unwrap().as_ident().unwrap().clone();
            let host = self.parse_identifier()?;
            Ok(GranteeName::UserHost { user, host })
        } else {
            Ok(GranteeName::ObjectName(name))
        }
    }

    /// Parse [`Statement::Deny`]
    pub fn parse_deny(&mut self) -> Result<Statement, ParserError> {
        self.expect_keyword(Keyword::DENY)?;

        let (privileges, objects) = self.parse_grant_deny_revoke_privileges_objects()?;
        let objects = match objects {
            Some(o) => o,
            None => {
                return parser_err!(
                    "DENY statements must specify an object",
                    self.peek_token().span.start
                )
            }
        };

        self.expect_keyword_is(Keyword::TO)?;
        let grantees = self.parse_grantees()?;
        let cascade = self.parse_cascade_option();
        let granted_by = if self.parse_keywords(&[Keyword::AS]) {
            Some(self.parse_identifier()?)
        } else {
            None
        };

        Ok(Statement::Deny(DenyStatement {
            privileges,
            objects,
            grantees,
            cascade,
            granted_by,
        }))
    }

    /// Parse a REVOKE statement
    pub fn parse_revoke(&mut self) -> Result<Statement, ParserError> {
        let (privileges, objects) = self.parse_grant_deny_revoke_privileges_objects()?;

        self.expect_keyword_is(Keyword::FROM)?;
        let grantees = self.parse_grantees()?;

        let granted_by = if self.parse_keywords(&[Keyword::GRANTED, Keyword::BY]) {
            Some(self.parse_identifier()?)
        } else {
            None
        };

        let cascade = self.parse_cascade_option();

        Ok(Statement::Revoke {
            privileges,
            objects,
            grantees,
            granted_by,
            cascade,
        })
    }

    /// Parse an REPLACE statement
    pub fn parse_replace(
        &mut self,
        replace_token: TokenWithSpan,
    ) -> Result<Statement, ParserError> {
        if !dialect_of!(self is MySqlDialect | GenericDialect) {
            return parser_err!(
                "Unsupported statement REPLACE",
                self.peek_token().span.start
            );
        }

        let mut insert = self.parse_insert(replace_token)?;
        if let Statement::Insert(Insert { replace_into, .. }) = &mut insert {
            *replace_into = true;
        }

        Ok(insert)
    }

    /// Parse an INSERT statement, returning a `Box`ed SetExpr
    ///
    /// This is used to reduce the size of the stack frames in debug builds
    fn parse_insert_setexpr_boxed(
        &mut self,
        insert_token: TokenWithSpan,
    ) -> Result<Box<SetExpr>, ParserError> {
        Ok(Box::new(SetExpr::Insert(self.parse_insert(insert_token)?)))
    }

    /// Parse an INSERT statement
    pub fn parse_insert(&mut self, insert_token: TokenWithSpan) -> Result<Statement, ParserError> {
        let optimizer_hint = self.maybe_parse_optimizer_hint()?;
        let or = self.parse_conflict_clause();
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

        let overwrite = self.parse_keyword(Keyword::OVERWRITE);
        let into = self.parse_keyword(Keyword::INTO);

        let local = self.parse_keyword(Keyword::LOCAL);

        if self.parse_keyword(Keyword::DIRECTORY) {
            let path = self.parse_literal_string()?;
            let file_format = if self.parse_keywords(&[Keyword::STORED, Keyword::AS]) {
                Some(self.parse_file_format()?)
            } else {
                None
            };
            let source = self.parse_query()?;
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
            let table_object = self.parse_table_object()?;

            let table_alias =
                if dialect_of!(self is PostgreSqlDialect) && self.parse_keyword(Keyword::AS) {
                    Some(self.parse_identifier()?)
                } else {
                    None
                };

            let is_mysql = dialect_of!(self is MySqlDialect);

            let (columns, partitioned, after_columns, source, assignments) = if self
                .parse_keywords(&[Keyword::DEFAULT, Keyword::VALUES])
            {
                (vec![], None, vec![], None, vec![])
            } else {
                let (columns, partitioned, after_columns) = if !self.peek_subquery_start() {
                    let columns = self.parse_parenthesized_column_list(Optional, is_mysql)?;

                    let partitioned = self.parse_insert_partition()?;
                    // Hive allows you to specify columns after partitions as well if you want.
                    let after_columns = if dialect_of!(self is HiveDialect) {
                        self.parse_parenthesized_column_list(Optional, false)?
                    } else {
                        vec![]
                    };
                    (columns, partitioned, after_columns)
                } else {
                    Default::default()
                };

                let (source, assignments) = if self.peek_keyword(Keyword::FORMAT)
                    || self.peek_keyword(Keyword::SETTINGS)
                {
                    (None, vec![])
                } else if self.dialect.supports_insert_set() && self.parse_keyword(Keyword::SET) {
                    (None, self.parse_comma_separated(Parser::parse_assignment)?)
                } else {
                    (Some(self.parse_query()?), vec![])
                };

                (columns, partitioned, after_columns, source, assignments)
            };

            let (format_clause, settings) = if self.dialect.supports_insert_format() {
                // Settings always comes before `FORMAT` for ClickHouse:
                // <https://clickhouse.com/docs/en/sql-reference/statements/insert-into>
                let settings = self.parse_settings()?;

                let format = if self.parse_keyword(Keyword::FORMAT) {
                    Some(self.parse_input_format_clause()?)
                } else {
                    None
                };

                (format, settings)
            } else {
                Default::default()
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

                    self.expect_keyword_is(Keyword::DO)?;
                    let action = if self.parse_keyword(Keyword::NOTHING) {
                        OnConflictAction::DoNothing
                    } else {
                        self.expect_keyword_is(Keyword::UPDATE)?;
                        self.expect_keyword_is(Keyword::SET)?;
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
                    self.expect_keyword_is(Keyword::DUPLICATE)?;
                    self.expect_keyword_is(Keyword::KEY)?;
                    self.expect_keyword_is(Keyword::UPDATE)?;
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

            Ok(Insert {
                insert_token: insert_token.into(),
                optimizer_hint,
                or,
                table: table_object,
                table_alias,
                ignore,
                into,
                overwrite,
                partitioned,
                columns,
                after_columns,
                source,
                assignments,
                has_table_keyword: table,
                on,
                returning,
                replace_into,
                priority,
                insert_alias,
                settings,
                format_clause,
            }
            .into())
        }
    }

    /// Parses input format clause used for ClickHouse.
    ///
    /// <https://clickhouse.com/docs/en/interfaces/formats>
    pub fn parse_input_format_clause(&mut self) -> Result<InputFormatClause, ParserError> {
        let ident = self.parse_identifier()?;
        let values = self
            .maybe_parse(|p| p.parse_comma_separated(|p| p.parse_expr()))?
            .unwrap_or_default();

        Ok(InputFormatClause { ident, values })
    }

    /// Returns true if the immediate tokens look like the
    /// beginning of a subquery. `(SELECT ...`
    fn peek_subquery_start(&mut self) -> bool {
        let [maybe_lparen, maybe_select] = self.peek_tokens();
        Token::LParen == maybe_lparen
            && matches!(maybe_select, Token::Word(w) if w.keyword == Keyword::SELECT)
    }

    fn parse_conflict_clause(&mut self) -> Option<SqliteOnConflict> {
        if self.parse_keywords(&[Keyword::OR, Keyword::REPLACE]) {
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
        }
    }

    /// Parse an optional `PARTITION (...)` clause for INSERT statements.
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

    /// Parse optional Hive `INPUTFORMAT ... SERDE ...` clause used by LOAD DATA.
    pub fn parse_load_data_table_format(
        &mut self,
    ) -> Result<Option<HiveLoadDataFormat>, ParserError> {
        if self.parse_keyword(Keyword::INPUTFORMAT) {
            let input_format = self.parse_expr()?;
            self.expect_keyword_is(Keyword::SERDE)?;
            let serde = self.parse_expr()?;
            Ok(Some(HiveLoadDataFormat {
                input_format,
                serde,
            }))
        } else {
            Ok(None)
        }
    }

    /// Parse an UPDATE statement, returning a `Box`ed SetExpr
    ///
    /// This is used to reduce the size of the stack frames in debug builds
    fn parse_update_setexpr_boxed(
        &mut self,
        update_token: TokenWithSpan,
    ) -> Result<Box<SetExpr>, ParserError> {
        Ok(Box::new(SetExpr::Update(self.parse_update(update_token)?)))
    }

    /// Parse an `UPDATE` statement and return `Statement::Update`.
    pub fn parse_update(&mut self, update_token: TokenWithSpan) -> Result<Statement, ParserError> {
        let optimizer_hint = self.maybe_parse_optimizer_hint()?;
        let or = self.parse_conflict_clause();
        let table = self.parse_table_and_joins()?;
        let from_before_set = if self.parse_keyword(Keyword::FROM) {
            Some(UpdateTableFromKind::BeforeSet(
                self.parse_table_with_joins()?,
            ))
        } else {
            None
        };
        self.expect_keyword(Keyword::SET)?;
        let assignments = self.parse_comma_separated(Parser::parse_assignment)?;
        let from = if from_before_set.is_none() && self.parse_keyword(Keyword::FROM) {
            Some(UpdateTableFromKind::AfterSet(
                self.parse_table_with_joins()?,
            ))
        } else {
            from_before_set
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
        let limit = if self.parse_keyword(Keyword::LIMIT) {
            Some(self.parse_expr()?)
        } else {
            None
        };
        Ok(Update {
            update_token: update_token.into(),
            optimizer_hint,
            table,
            assignments,
            from,
            selection,
            returning,
            or,
            limit,
        }
        .into())
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

    /// Parse a single function argument, handling named and unnamed variants.
    pub fn parse_function_args(&mut self) -> Result<FunctionArg, ParserError> {
        let arg = if self.dialect.supports_named_fn_args_with_expr_name() {
            self.maybe_parse(|p| {
                let name = p.parse_expr()?;
                let operator = p.parse_function_named_arg_operator()?;
                let arg = p.parse_wildcard_expr()?.into();
                Ok(FunctionArg::ExprNamed {
                    name,
                    arg,
                    operator,
                })
            })?
        } else {
            self.maybe_parse(|p| {
                let name = p.parse_identifier()?;
                let operator = p.parse_function_named_arg_operator()?;
                let arg = p.parse_wildcard_expr()?.into();
                Ok(FunctionArg::Named {
                    name,
                    arg,
                    operator,
                })
            })?
        };
        if let Some(arg) = arg {
            return Ok(arg);
        }
        Ok(FunctionArg::Unnamed(self.parse_wildcard_expr()?.into()))
    }

    fn parse_function_named_arg_operator(&mut self) -> Result<FunctionArgOperator, ParserError> {
        if self.parse_keyword(Keyword::VALUE) {
            return Ok(FunctionArgOperator::Value);
        }
        let tok = self.next_token();
        match tok.token {
            Token::RArrow if self.dialect.supports_named_fn_args_with_rarrow_operator() => {
                Ok(FunctionArgOperator::RightArrow)
            }
            Token::Eq if self.dialect.supports_named_fn_args_with_eq_operator() => {
                Ok(FunctionArgOperator::Equals)
            }
            Token::Assignment
                if self
                    .dialect
                    .supports_named_fn_args_with_assignment_operator() =>
            {
                Ok(FunctionArgOperator::Assignment)
            }
            Token::Colon if self.dialect.supports_named_fn_args_with_colon_operator() => {
                Ok(FunctionArgOperator::Colon)
            }
            _ => {
                self.prev_token();
                self.expected("argument operator", tok)
            }
        }
    }

    /// Parse an optional, comma-separated list of function arguments (consumes closing paren).
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

    /// Parses a potentially empty list of arguments to a function
    /// (including the closing parenthesis).
    ///
    /// Examples:
    /// ```sql
    /// FIRST_VALUE(x ORDER BY 1,2,3);
    /// FIRST_VALUE(x IGNORE NULL);
    /// ```
    fn parse_function_argument_list(&mut self) -> Result<FunctionArgumentList, ParserError> {
        let mut clauses = vec![];

        // Handle clauses that may exist with an empty argument list

        if let Some(null_clause) = self.parse_json_null_clause() {
            clauses.push(FunctionArgumentClause::JsonNullClause(null_clause));
        }

        if let Some(json_returning_clause) = self.maybe_parse_json_returning_clause()? {
            clauses.push(FunctionArgumentClause::JsonReturningClause(
                json_returning_clause,
            ));
        }

        if self.consume_token(&Token::RParen) {
            return Ok(FunctionArgumentList {
                duplicate_treatment: None,
                args: vec![],
                clauses,
            });
        }

        let duplicate_treatment = self.parse_duplicate_treatment()?;
        let args = self.parse_comma_separated(Parser::parse_function_args)?;

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
                unexpected_keyword => return Err(ParserError::ParserError(
                    format!("Internal parser error: unexpected keyword `{unexpected_keyword}` in having bound"),
                )),
            };
            clauses.push(FunctionArgumentClause::Having(HavingBound(
                kind,
                self.parse_expr()?,
            )))
        }

        if dialect_of!(self is GenericDialect | MySqlDialect)
            && self.parse_keyword(Keyword::SEPARATOR)
        {
            clauses.push(FunctionArgumentClause::Separator(self.parse_value()?.value));
        }

        if let Some(on_overflow) = self.parse_listagg_on_overflow()? {
            clauses.push(FunctionArgumentClause::OnOverflow(on_overflow));
        }

        if let Some(null_clause) = self.parse_json_null_clause() {
            clauses.push(FunctionArgumentClause::JsonNullClause(null_clause));
        }

        if let Some(json_returning_clause) = self.maybe_parse_json_returning_clause()? {
            clauses.push(FunctionArgumentClause::JsonReturningClause(
                json_returning_clause,
            ));
        }

        self.expect_token(&Token::RParen)?;
        Ok(FunctionArgumentList {
            duplicate_treatment,
            args,
            clauses,
        })
    }

    fn parse_json_null_clause(&mut self) -> Option<JsonNullClause> {
        if self.parse_keywords(&[Keyword::ABSENT, Keyword::ON, Keyword::NULL]) {
            Some(JsonNullClause::AbsentOnNull)
        } else if self.parse_keywords(&[Keyword::NULL, Keyword::ON, Keyword::NULL]) {
            Some(JsonNullClause::NullOnNull)
        } else {
            None
        }
    }

    fn maybe_parse_json_returning_clause(
        &mut self,
    ) -> Result<Option<JsonReturningClause>, ParserError> {
        if self.parse_keyword(Keyword::RETURNING) {
            let data_type = self.parse_data_type()?;
            Ok(Some(JsonReturningClause { data_type }))
        } else {
            Ok(None)
        }
    }

    fn parse_duplicate_treatment(&mut self) -> Result<Option<DuplicateTreatment>, ParserError> {
        let loc = self.peek_token().span.start;
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
        let prefix = self
            .parse_one_of_keywords(
                self.dialect
                    .get_reserved_keywords_for_select_item_operator(),
            )
            .map(|keyword| Ident::new(format!("{keyword:?}")));

        match self.parse_wildcard_expr()? {
            Expr::QualifiedWildcard(prefix, token) => Ok(SelectItem::QualifiedWildcard(
                SelectItemQualifiedWildcardKind::ObjectName(prefix),
                self.parse_wildcard_additional_options(token.0)?,
            )),
            Expr::Wildcard(token) => Ok(SelectItem::Wildcard(
                self.parse_wildcard_additional_options(token.0)?,
            )),
            Expr::Identifier(v) if v.value.to_lowercase() == "from" && v.quote_style.is_none() => {
                parser_err!(
                    format!("Expected an expression, found: {}", v),
                    self.peek_token().span.start
                )
            }
            Expr::BinaryOp {
                left,
                op: BinaryOperator::Eq,
                right,
            } if self.dialect.supports_eq_alias_assignment()
                && matches!(left.as_ref(), Expr::Identifier(_)) =>
            {
                let Expr::Identifier(alias) = *left else {
                    return parser_err!(
                        "BUG: expected identifier expression as alias",
                        self.peek_token().span.start
                    );
                };
                Ok(SelectItem::ExprWithAlias {
                    expr: *right,
                    alias,
                })
            }
            expr if self.dialect.supports_select_expr_star()
                && self.consume_tokens(&[Token::Period, Token::Mul]) =>
            {
                let wildcard_token = self.get_previous_token().clone();
                Ok(SelectItem::QualifiedWildcard(
                    SelectItemQualifiedWildcardKind::Expr(expr),
                    self.parse_wildcard_additional_options(wildcard_token)?,
                ))
            }
            expr => self
                .maybe_parse_select_item_alias()
                .map(|alias| match alias {
                    Some(alias) => SelectItem::ExprWithAlias {
                        expr: maybe_prefixed_expr(expr, prefix),
                        alias,
                    },
                    None => SelectItem::UnnamedExpr(maybe_prefixed_expr(expr, prefix)),
                }),
        }
    }

    /// Parse an [`WildcardAdditionalOptions`] information for wildcard select items.
    ///
    /// If it is not possible to parse it, will return an option.
    pub fn parse_wildcard_additional_options(
        &mut self,
        wildcard_token: TokenWithSpan,
    ) -> Result<WildcardAdditionalOptions, ParserError> {
        let opt_ilike = if self.dialect.supports_select_wildcard_ilike() {
            self.parse_optional_select_item_ilike()?
        } else {
            None
        };
        let opt_exclude = if opt_ilike.is_none() && self.dialect.supports_select_wildcard_exclude()
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
        let opt_replace = if self.dialect.supports_select_wildcard_replace() {
            self.parse_optional_select_item_replace()?
        } else {
            None
        };
        let opt_rename = if self.dialect.supports_select_wildcard_rename() {
            self.parse_optional_select_item_rename()?
        } else {
            None
        };

        Ok(WildcardAdditionalOptions {
            wildcard_token: wildcard_token.into(),
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
                let ident = self.parse_identifier()?;
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
    /// Parse a single element of a `REPLACE (...)` select-item clause.
    pub fn parse_replace_elements(&mut self) -> Result<ReplaceSelectElement, ParserError> {
        let expr = self.parse_expr()?;
        let as_keyword = self.parse_keyword(Keyword::AS);
        let ident = self.parse_identifier()?;
        Ok(ReplaceSelectElement {
            expr,
            column_name: ident,
            as_keyword,
        })
    }

    /// Parse ASC or DESC, returns an Option with true if ASC, false of DESC or `None` if none of
    /// them.
    pub fn parse_asc_desc(&mut self) -> Option<bool> {
        if self.parse_keyword(Keyword::ASC) {
            Some(true)
        } else if self.parse_keyword(Keyword::DESC) {
            Some(false)
        } else {
            None
        }
    }

    /// Parse an [OrderByExpr] expression.
    pub fn parse_order_by_expr(&mut self) -> Result<OrderByExpr, ParserError> {
        self.parse_order_by_expr_inner(false)
            .map(|(order_by, _)| order_by)
    }

    /// Parse an [IndexColumn].
    pub fn parse_create_index_expr(&mut self) -> Result<IndexColumn, ParserError> {
        self.parse_order_by_expr_inner(true)
            .map(|(column, operator_class)| IndexColumn {
                column,
                operator_class,
            })
    }

    fn parse_order_by_expr_inner(
        &mut self,
        with_operator_class: bool,
    ) -> Result<(OrderByExpr, Option<ObjectName>), ParserError> {
        let expr = self.parse_expr()?;

        let operator_class: Option<ObjectName> = if with_operator_class {
            // We check that if non of the following keywords are present, then we parse an
            // identifier as operator class.
            if self
                .peek_one_of_keywords(&[Keyword::ASC, Keyword::DESC, Keyword::NULLS, Keyword::WITH])
                .is_some()
            {
                None
            } else {
                self.maybe_parse(|parser| parser.parse_object_name(false))?
            }
        } else {
            None
        };

        let options = self.parse_order_by_options()?;

        let with_fill = if self.dialect.supports_with_fill()
            && self.parse_keywords(&[Keyword::WITH, Keyword::FILL])
        {
            Some(self.parse_with_fill()?)
        } else {
            None
        };

        Ok((
            OrderByExpr {
                expr,
                options,
                with_fill,
            },
            operator_class,
        ))
    }

    fn parse_order_by_options(&mut self) -> Result<OrderByOptions, ParserError> {
        let asc = self.parse_asc_desc();

        let nulls_first = if self.parse_keywords(&[Keyword::NULLS, Keyword::FIRST]) {
            Some(true)
        } else if self.parse_keywords(&[Keyword::NULLS, Keyword::LAST]) {
            Some(false)
        } else {
            None
        };

        Ok(OrderByOptions { asc, nulls_first })
    }

    // Parse a WITH FILL clause (ClickHouse dialect)
    // that follow the WITH FILL keywords in a ORDER BY clause
    /// Parse a `WITH FILL` clause used in ORDER BY (ClickHouse dialect).
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

    /// Parse a set of comma separated INTERPOLATE expressions (ClickHouse dialect)
    /// that follow the INTERPOLATE keyword in an ORDER BY clause with the WITH FILL modifier
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

    /// Parse a INTERPOLATE expression (ClickHouse dialect)
    pub fn parse_interpolation(&mut self) -> Result<InterpolateExpr, ParserError> {
        let column = self.parse_identifier()?;
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
                Token::Number(s, _) => Self::parse::<u64>(s, next_token.span.start)?,
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
        let _ = self.parse_one_of_keywords(&[Keyword::FIRST, Keyword::NEXT]);

        let (quantity, percent) = if self
            .parse_one_of_keywords(&[Keyword::ROW, Keyword::ROWS])
            .is_some()
        {
            (None, false)
        } else {
            let quantity = Expr::Value(self.parse_value()?);
            let percent = self.parse_keyword(Keyword::PERCENT);
            let _ = self.parse_one_of_keywords(&[Keyword::ROW, Keyword::ROWS]);
            (Some(quantity), percent)
        };

        let with_ties = if self.parse_keyword(Keyword::ONLY) {
            false
        } else {
            self.parse_keywords(&[Keyword::WITH, Keyword::TIES])
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
            unexpected_keyword => return Err(ParserError::ParserError(
                format!("Internal parser error: expected any of {{UPDATE, SHARE}}, got {unexpected_keyword:?}"),
            )),
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

    /// Parse a VALUES clause
    pub fn parse_values(
        &mut self,
        allow_empty: bool,
        value_keyword: bool,
    ) -> Result<Values, ParserError> {
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
        Ok(Values {
            explicit_row,
            rows,
            value_keyword,
        })
    }

    /// Parse a 'START TRANSACTION' statement
    pub fn parse_start_transaction(&mut self) -> Result<Statement, ParserError> {
        self.expect_keyword_is(Keyword::TRANSACTION)?;
        Ok(Statement::StartTransaction {
            modes: self.parse_transaction_modes()?,
            begin: false,
            transaction: Some(BeginTransactionKind::Transaction),
            modifier: None,
            statements: vec![],
            exception: None,
            has_end_keyword: false,
        })
    }

    /// Parse a 'BEGIN' statement
    pub fn parse_begin(&mut self) -> Result<Statement, ParserError> {
        let modifier = if !self.dialect.supports_start_transaction_modifier() {
            None
        } else if self.parse_keyword(Keyword::DEFERRED) {
            Some(TransactionModifier::Deferred)
        } else if self.parse_keyword(Keyword::IMMEDIATE) {
            Some(TransactionModifier::Immediate)
        } else if self.parse_keyword(Keyword::EXCLUSIVE) {
            Some(TransactionModifier::Exclusive)
        } else if self.parse_keyword(Keyword::TRY) {
            Some(TransactionModifier::Try)
        } else if self.parse_keyword(Keyword::CATCH) {
            Some(TransactionModifier::Catch)
        } else {
            None
        };
        let transaction = match self.parse_one_of_keywords(&[Keyword::TRANSACTION, Keyword::WORK]) {
            Some(Keyword::TRANSACTION) => Some(BeginTransactionKind::Transaction),
            Some(Keyword::WORK) => Some(BeginTransactionKind::Work),
            _ => None,
        };
        Ok(Statement::StartTransaction {
            modes: self.parse_transaction_modes()?,
            begin: true,
            transaction,
            modifier,
            statements: vec![],
            exception: None,
            has_end_keyword: false,
        })
    }

    /// Parse a 'BEGIN ... EXCEPTION ... END' block
    pub fn parse_begin_exception_end(&mut self) -> Result<Statement, ParserError> {
        let statements = self.parse_statement_list(&[Keyword::EXCEPTION, Keyword::END])?;

        let exception = if self.parse_keyword(Keyword::EXCEPTION) {
            let mut when = Vec::new();

            // We can have multiple `WHEN` arms so we consume all cases until `END`
            while !self.peek_keyword(Keyword::END) {
                self.expect_keyword(Keyword::WHEN)?;

                // Each `WHEN` case can have one or more conditions, e.g.
                // WHEN EXCEPTION_1 [OR EXCEPTION_2] THEN
                // So we parse identifiers until the `THEN` keyword.
                let mut idents = Vec::new();

                while !self.parse_keyword(Keyword::THEN) {
                    let ident = self.parse_identifier()?;
                    idents.push(ident);

                    self.maybe_parse(|p| p.expect_keyword(Keyword::OR))?;
                }

                let statements = self.parse_statement_list(&[Keyword::WHEN, Keyword::END])?;

                when.push(ExceptionWhen { idents, statements });
            }

            Some(when)
        } else {
            None
        };

        self.expect_keyword(Keyword::END)?;

        Ok(Statement::StartTransaction {
            begin: true,
            statements,
            exception,
            has_end_keyword: true,
            transaction: None,
            modifier: None,
            modes: Default::default(),
        })
    }

    /// Parse an 'END' statement
    pub fn parse_end(&mut self) -> Result<Statement, ParserError> {
        let modifier = if !self.dialect.supports_end_transaction_modifier() {
            None
        } else if self.parse_keyword(Keyword::TRY) {
            Some(TransactionModifier::Try)
        } else if self.parse_keyword(Keyword::CATCH) {
            Some(TransactionModifier::Catch)
        } else {
            None
        };
        Ok(Statement::Commit {
            chain: self.parse_commit_rollback_chain()?,
            end: true,
            modifier,
        })
    }

    /// Parse a list of transaction modes
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
                } else if self.parse_keyword(Keyword::SNAPSHOT) {
                    TransactionIsolationLevel::Snapshot
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

    /// Parse a 'COMMIT' statement
    pub fn parse_commit(&mut self) -> Result<Statement, ParserError> {
        Ok(Statement::Commit {
            chain: self.parse_commit_rollback_chain()?,
            end: false,
            modifier: None,
        })
    }

    /// Parse a 'ROLLBACK' statement
    pub fn parse_rollback(&mut self) -> Result<Statement, ParserError> {
        let chain = self.parse_commit_rollback_chain()?;
        let savepoint = self.parse_rollback_savepoint()?;

        Ok(Statement::Rollback { chain, savepoint })
    }

    /// Parse an optional `AND [NO] CHAIN` clause for `COMMIT` and `ROLLBACK` statements
    pub fn parse_commit_rollback_chain(&mut self) -> Result<bool, ParserError> {
        let _ = self.parse_one_of_keywords(&[Keyword::TRANSACTION, Keyword::WORK]);
        if self.parse_keyword(Keyword::AND) {
            let chain = !self.parse_keyword(Keyword::NO);
            self.expect_keyword_is(Keyword::CHAIN)?;
            Ok(chain)
        } else {
            Ok(false)
        }
    }

    /// Parse an optional 'TO SAVEPOINT savepoint_name' clause for ROLLBACK statements
    pub fn parse_rollback_savepoint(&mut self) -> Result<Option<Ident>, ParserError> {
        if self.parse_keyword(Keyword::TO) {
            let _ = self.parse_keyword(Keyword::SAVEPOINT);
            let savepoint = self.parse_identifier()?;

            Ok(Some(savepoint))
        } else {
            Ok(None)
        }
    }

    /// Parse a 'RAISERROR' statement
    pub fn parse_raiserror(&mut self) -> Result<Statement, ParserError> {
        self.expect_token(&Token::LParen)?;
        let message = Box::new(self.parse_expr()?);
        self.expect_token(&Token::Comma)?;
        let severity = Box::new(self.parse_expr()?);
        self.expect_token(&Token::Comma)?;
        let state = Box::new(self.parse_expr()?);
        let arguments = if self.consume_token(&Token::Comma) {
            self.parse_comma_separated(Parser::parse_expr)?
        } else {
            vec![]
        };
        self.expect_token(&Token::RParen)?;
        let options = if self.parse_keyword(Keyword::WITH) {
            self.parse_comma_separated(Parser::parse_raiserror_option)?
        } else {
            vec![]
        };
        Ok(Statement::RaisError {
            message,
            severity,
            state,
            arguments,
            options,
        })
    }

    /// Parse a single `RAISERROR` option
    pub fn parse_raiserror_option(&mut self) -> Result<RaisErrorOption, ParserError> {
        match self.expect_one_of_keywords(&[Keyword::LOG, Keyword::NOWAIT, Keyword::SETERROR])? {
            Keyword::LOG => Ok(RaisErrorOption::Log),
            Keyword::NOWAIT => Ok(RaisErrorOption::NoWait),
            Keyword::SETERROR => Ok(RaisErrorOption::SetError),
            _ => self.expected(
                "LOG, NOWAIT OR SETERROR raiserror option",
                self.peek_token(),
            ),
        }
    }

    /// Parse a SQL `DEALLOCATE` statement
    pub fn parse_deallocate(&mut self) -> Result<Statement, ParserError> {
        let prepare = self.parse_keyword(Keyword::PREPARE);
        let name = self.parse_identifier()?;
        Ok(Statement::Deallocate { name, prepare })
    }

    /// Parse a SQL `EXECUTE` statement
    pub fn parse_execute(&mut self) -> Result<Statement, ParserError> {
        let name = if self.dialect.supports_execute_immediate()
            && self.parse_keyword(Keyword::IMMEDIATE)
        {
            None
        } else {
            let has_parentheses = self.consume_token(&Token::LParen);
            let name = self.parse_object_name(false)?;
            if has_parentheses {
                self.expect_token(&Token::RParen)?;
            }
            Some(name)
        };

        let has_parentheses = self.consume_token(&Token::LParen);

        let end_kws = &[Keyword::USING, Keyword::OUTPUT, Keyword::DEFAULT];
        let end_token = match (has_parentheses, self.peek_token().token) {
            (true, _) => Token::RParen,
            (false, Token::EOF) => Token::EOF,
            (false, Token::Word(w)) if end_kws.contains(&w.keyword) => Token::Word(w),
            (false, _) => Token::SemiColon,
        };

        let parameters = self.parse_comma_separated0(Parser::parse_expr, end_token)?;

        if has_parentheses {
            self.expect_token(&Token::RParen)?;
        }

        let into = if self.parse_keyword(Keyword::INTO) {
            self.parse_comma_separated(Self::parse_identifier)?
        } else {
            vec![]
        };

        let using = if self.parse_keyword(Keyword::USING) {
            self.parse_comma_separated(Self::parse_expr_with_alias)?
        } else {
            vec![]
        };

        let output = self.parse_keyword(Keyword::OUTPUT);

        let default = self.parse_keyword(Keyword::DEFAULT);

        Ok(Statement::Execute {
            immediate: name.is_none(),
            name,
            parameters,
            has_parentheses,
            into,
            using,
            output,
            default,
        })
    }

    /// Parse a SQL `PREPARE` statement
    pub fn parse_prepare(&mut self) -> Result<Statement, ParserError> {
        let name = self.parse_identifier()?;

        let mut data_types = vec![];
        if self.consume_token(&Token::LParen) {
            data_types = self.parse_comma_separated(Parser::parse_data_type)?;
            self.expect_token(&Token::RParen)?;
        }

        self.expect_keyword_is(Keyword::AS)?;
        let statement = Box::new(self.parse_statement()?);
        Ok(Statement::Prepare {
            name,
            data_types,
            statement,
        })
    }

    /// Parse a SQL `UNLOAD` statement
    pub fn parse_unload(&mut self) -> Result<Statement, ParserError> {
        self.expect_keyword(Keyword::UNLOAD)?;
        self.expect_token(&Token::LParen)?;
        let (query, query_text) = if matches!(self.peek_token().token, Token::SingleQuotedString(_))
        {
            (None, Some(self.parse_literal_string()?))
        } else {
            (Some(self.parse_query()?), None)
        };
        self.expect_token(&Token::RParen)?;

        self.expect_keyword_is(Keyword::TO)?;
        let to = self.parse_identifier()?;
        let auth = if self.parse_keyword(Keyword::IAM_ROLE) {
            Some(self.parse_iam_role_kind()?)
        } else {
            None
        };
        let with = self.parse_options(Keyword::WITH)?;
        let mut options = vec![];
        while let Some(opt) = self.maybe_parse(|parser| parser.parse_copy_legacy_option())? {
            options.push(opt);
        }
        Ok(Statement::Unload {
            query,
            query_text,
            to,
            auth,
            with,
            options,
        })
    }

    fn parse_select_into(&mut self) -> Result<SelectInto, ParserError> {
        let temporary = self
            .parse_one_of_keywords(&[Keyword::TEMP, Keyword::TEMPORARY])
            .is_some();
        let unlogged = self.parse_keyword(Keyword::UNLOGGED);
        let table = self.parse_keyword(Keyword::TABLE);
        let name = self.parse_object_name(false)?;

        Ok(SelectInto {
            temporary,
            unlogged,
            table,
            name,
        })
    }

    fn parse_pragma_value(&mut self) -> Result<Value, ParserError> {
        match self.parse_value()?.value {
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

    /// PRAGMA [schema-name '.'] pragma-name [('=' pragma-value) | '(' pragma-value ')']
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
        let extension_name = self.parse_identifier()?;

        Ok(Statement::Install { extension_name })
    }

    /// Parse a SQL LOAD statement
    pub fn parse_load(&mut self) -> Result<Statement, ParserError> {
        if self.dialect.supports_load_extension() {
            let extension_name = self.parse_identifier()?;
            Ok(Statement::Load { extension_name })
        } else if self.parse_keyword(Keyword::DATA) && self.dialect.supports_load_data() {
            let local = self.parse_one_of_keywords(&[Keyword::LOCAL]).is_some();
            self.expect_keyword_is(Keyword::INPATH)?;
            let inpath = self.parse_literal_string()?;
            let overwrite = self.parse_one_of_keywords(&[Keyword::OVERWRITE]).is_some();
            self.expect_keyword_is(Keyword::INTO)?;
            self.expect_keyword_is(Keyword::TABLE)?;
            let table_name = self.parse_object_name(false)?;
            let partitioned = self.parse_insert_partition()?;
            let table_format = self.parse_load_data_table_format()?;
            Ok(Statement::LoadData {
                local,
                inpath,
                overwrite,
                table_name,
                partitioned,
                table_format,
            })
        } else {
            self.expected(
                "`DATA` or an extension name after `LOAD`",
                self.peek_token(),
            )
        }
    }

    /// ```sql
    /// OPTIMIZE TABLE [db.]name [ON CLUSTER cluster] [PARTITION partition | PARTITION ID 'partition_id'] [FINAL] [DEDUPLICATE [BY expression]]
    /// ```
    /// [ClickHouse](https://clickhouse.com/docs/en/sql-reference/statements/optimize)
    pub fn parse_optimize_table(&mut self) -> Result<Statement, ParserError> {
        self.expect_keyword_is(Keyword::TABLE)?;
        let name = self.parse_object_name(false)?;
        let on_cluster = self.parse_optional_on_cluster()?;

        let partition = if self.parse_keyword(Keyword::PARTITION) {
            if self.parse_keyword(Keyword::ID) {
                Some(Partition::Identifier(self.parse_identifier()?))
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
                Some(ObjectName::from(vec![Ident::new("NONE")]))
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
                sequence_options.push(SequenceOptions::IncrementBy(self.parse_number()?, true));
            } else {
                sequence_options.push(SequenceOptions::IncrementBy(self.parse_number()?, false));
            }
        }
        //[ MINVALUE minvalue | NO MINVALUE ]
        if self.parse_keyword(Keyword::MINVALUE) {
            sequence_options.push(SequenceOptions::MinValue(Some(self.parse_number()?)));
        } else if self.parse_keywords(&[Keyword::NO, Keyword::MINVALUE]) {
            sequence_options.push(SequenceOptions::MinValue(None));
        }
        //[ MAXVALUE maxvalue | NO MAXVALUE ]
        if self.parse_keywords(&[Keyword::MAXVALUE]) {
            sequence_options.push(SequenceOptions::MaxValue(Some(self.parse_number()?)));
        } else if self.parse_keywords(&[Keyword::NO, Keyword::MAXVALUE]) {
            sequence_options.push(SequenceOptions::MaxValue(None));
        }

        //[ START [ WITH ] start ]
        if self.parse_keywords(&[Keyword::START]) {
            if self.parse_keywords(&[Keyword::WITH]) {
                sequence_options.push(SequenceOptions::StartWith(self.parse_number()?, true));
            } else {
                sequence_options.push(SequenceOptions::StartWith(self.parse_number()?, false));
            }
        }
        //[ CACHE cache ]
        if self.parse_keywords(&[Keyword::CACHE]) {
            sequence_options.push(SequenceOptions::Cache(self.parse_number()?));
        }
        // [ [ NO ] CYCLE ]
        if self.parse_keywords(&[Keyword::NO, Keyword::CYCLE]) {
            sequence_options.push(SequenceOptions::Cycle(true));
        } else if self.parse_keywords(&[Keyword::CYCLE]) {
            sequence_options.push(SequenceOptions::Cycle(false));
        }

        Ok(sequence_options)
    }

    ///   Parse a `CREATE SERVER` statement.
    ///
    ///  See [Statement::CreateServer]
    pub fn parse_pg_create_server(&mut self) -> Result<Statement, ParserError> {
        let ine = self.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let name = self.parse_object_name(false)?;

        let server_type = if self.parse_keyword(Keyword::TYPE) {
            Some(self.parse_identifier()?)
        } else {
            None
        };

        let version = if self.parse_keyword(Keyword::VERSION) {
            Some(self.parse_identifier()?)
        } else {
            None
        };

        self.expect_keywords(&[Keyword::FOREIGN, Keyword::DATA, Keyword::WRAPPER])?;
        let foreign_data_wrapper = self.parse_object_name(false)?;

        let mut options = None;
        if self.parse_keyword(Keyword::OPTIONS) {
            self.expect_token(&Token::LParen)?;
            options = Some(self.parse_comma_separated(|p| {
                let key = p.parse_identifier()?;
                let value = p.parse_identifier()?;
                Ok(CreateServerOption { key, value })
            })?);
            self.expect_token(&Token::RParen)?;
        }

        Ok(Statement::CreateServer(CreateServerStatement {
            name,
            if_not_exists: ine,
            server_type,
            version,
            foreign_data_wrapper,
            options,
        }))
    }

    /// The index of the first unprocessed token.
    pub fn index(&self) -> usize {
        self.index
    }

    /// Parse a named window definition.
    pub fn parse_named_window(&mut self) -> Result<NamedWindowDefinition, ParserError> {
        let ident = self.parse_identifier()?;
        self.expect_keyword_is(Keyword::AS)?;

        let window_expr = if self.consume_token(&Token::LParen) {
            NamedWindowExpr::WindowSpec(self.parse_window_spec()?)
        } else if self.dialect.supports_window_clause_named_window_reference() {
            NamedWindowExpr::NamedWindow(self.parse_identifier()?)
        } else {
            return self.expected("(", self.peek_token());
        };

        Ok(NamedWindowDefinition(ident, window_expr))
    }

    /// Parse `CREATE PROCEDURE` statement.
    pub fn parse_create_procedure(&mut self, or_alter: bool) -> Result<Statement, ParserError> {
        let name = self.parse_object_name(false)?;
        let params = self.parse_optional_procedure_parameters()?;

        let language = if self.parse_keyword(Keyword::LANGUAGE) {
            Some(self.parse_identifier()?)
        } else {
            None
        };

        self.expect_keyword_is(Keyword::AS)?;

        let body = self.parse_conditional_statements(&[Keyword::END])?;

        Ok(Statement::CreateProcedure {
            name,
            or_alter,
            params,
            language,
            body,
        })
    }

    /// Parse a window specification.
    pub fn parse_window_spec(&mut self) -> Result<WindowSpec, ParserError> {
        let window_name = match self.peek_token().token {
            Token::Word(word) if word.keyword == Keyword::NoKeyword => {
                self.parse_optional_ident()?
            }
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

    /// Parse `CREATE TYPE` statement.
    pub fn parse_create_type(&mut self) -> Result<Statement, ParserError> {
        let name = self.parse_object_name(false)?;

        // Check if we have AS keyword
        let has_as = self.parse_keyword(Keyword::AS);

        if !has_as {
            // Two cases: CREATE TYPE name; or CREATE TYPE name (options);
            if self.consume_token(&Token::LParen) {
                // CREATE TYPE name (options) - SQL definition without AS
                let options = self.parse_create_type_sql_definition_options()?;
                self.expect_token(&Token::RParen)?;
                return Ok(Statement::CreateType {
                    name,
                    representation: Some(UserDefinedTypeRepresentation::SqlDefinition { options }),
                });
            }

            // CREATE TYPE name; - no representation
            return Ok(Statement::CreateType {
                name,
                representation: None,
            });
        }

        // We have AS keyword
        if self.parse_keyword(Keyword::ENUM) {
            // CREATE TYPE name AS ENUM (labels)
            self.parse_create_type_enum(name)
        } else if self.parse_keyword(Keyword::RANGE) {
            // CREATE TYPE name AS RANGE (options)
            self.parse_create_type_range(name)
        } else if self.consume_token(&Token::LParen) {
            // CREATE TYPE name AS (attributes) - Composite
            self.parse_create_type_composite(name)
        } else {
            self.expected("ENUM, RANGE, or '(' after AS", self.peek_token())
        }
    }

    /// Parse remainder of `CREATE TYPE AS (attributes)` statement (composite type)
    ///
    /// See [PostgreSQL](https://www.postgresql.org/docs/current/sql-createtype.html)
    fn parse_create_type_composite(&mut self, name: ObjectName) -> Result<Statement, ParserError> {
        if self.consume_token(&Token::RParen) {
            // Empty composite type
            return Ok(Statement::CreateType {
                name,
                representation: Some(UserDefinedTypeRepresentation::Composite {
                    attributes: vec![],
                }),
            });
        }

        let mut attributes = vec![];
        loop {
            let attr_name = self.parse_identifier()?;
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

            if !self.consume_token(&Token::Comma) {
                break;
            }
        }
        self.expect_token(&Token::RParen)?;

        Ok(Statement::CreateType {
            name,
            representation: Some(UserDefinedTypeRepresentation::Composite { attributes }),
        })
    }

    /// Parse remainder of `CREATE TYPE AS ENUM` statement (see [Statement::CreateType] and [Self::parse_create_type])
    ///
    /// See [PostgreSQL](https://www.postgresql.org/docs/current/sql-createtype.html)
    pub fn parse_create_type_enum(&mut self, name: ObjectName) -> Result<Statement, ParserError> {
        self.expect_token(&Token::LParen)?;
        let labels = self.parse_comma_separated0(|p| p.parse_identifier(), Token::RParen)?;
        self.expect_token(&Token::RParen)?;

        Ok(Statement::CreateType {
            name,
            representation: Some(UserDefinedTypeRepresentation::Enum { labels }),
        })
    }

    /// Parse remainder of `CREATE TYPE AS RANGE` statement
    ///
    /// See [PostgreSQL](https://www.postgresql.org/docs/current/sql-createtype.html)
    fn parse_create_type_range(&mut self, name: ObjectName) -> Result<Statement, ParserError> {
        self.expect_token(&Token::LParen)?;
        let options = self.parse_comma_separated0(|p| p.parse_range_option(), Token::RParen)?;
        self.expect_token(&Token::RParen)?;

        Ok(Statement::CreateType {
            name,
            representation: Some(UserDefinedTypeRepresentation::Range { options }),
        })
    }

    /// Parse a single range option for a `CREATE TYPE AS RANGE` statement
    fn parse_range_option(&mut self) -> Result<UserDefinedTypeRangeOption, ParserError> {
        let keyword = self.parse_one_of_keywords(&[
            Keyword::SUBTYPE,
            Keyword::SUBTYPE_OPCLASS,
            Keyword::COLLATION,
            Keyword::CANONICAL,
            Keyword::SUBTYPE_DIFF,
            Keyword::MULTIRANGE_TYPE_NAME,
        ]);

        match keyword {
            Some(Keyword::SUBTYPE) => {
                self.expect_token(&Token::Eq)?;
                let data_type = self.parse_data_type()?;
                Ok(UserDefinedTypeRangeOption::Subtype(data_type))
            }
            Some(Keyword::SUBTYPE_OPCLASS) => {
                self.expect_token(&Token::Eq)?;
                let name = self.parse_object_name(false)?;
                Ok(UserDefinedTypeRangeOption::SubtypeOpClass(name))
            }
            Some(Keyword::COLLATION) => {
                self.expect_token(&Token::Eq)?;
                let name = self.parse_object_name(false)?;
                Ok(UserDefinedTypeRangeOption::Collation(name))
            }
            Some(Keyword::CANONICAL) => {
                self.expect_token(&Token::Eq)?;
                let name = self.parse_object_name(false)?;
                Ok(UserDefinedTypeRangeOption::Canonical(name))
            }
            Some(Keyword::SUBTYPE_DIFF) => {
                self.expect_token(&Token::Eq)?;
                let name = self.parse_object_name(false)?;
                Ok(UserDefinedTypeRangeOption::SubtypeDiff(name))
            }
            Some(Keyword::MULTIRANGE_TYPE_NAME) => {
                self.expect_token(&Token::Eq)?;
                let name = self.parse_object_name(false)?;
                Ok(UserDefinedTypeRangeOption::MultirangeTypeName(name))
            }
            _ => self.expected("range option keyword", self.peek_token()),
        }
    }

    /// Parse SQL definition options for CREATE TYPE (options)
    fn parse_create_type_sql_definition_options(
        &mut self,
    ) -> Result<Vec<UserDefinedTypeSqlDefinitionOption>, ParserError> {
        self.parse_comma_separated0(|p| p.parse_sql_definition_option(), Token::RParen)
    }

    /// Parse a single SQL definition option for CREATE TYPE (options)
    fn parse_sql_definition_option(
        &mut self,
    ) -> Result<UserDefinedTypeSqlDefinitionOption, ParserError> {
        let keyword = self.parse_one_of_keywords(&[
            Keyword::INPUT,
            Keyword::OUTPUT,
            Keyword::RECEIVE,
            Keyword::SEND,
            Keyword::TYPMOD_IN,
            Keyword::TYPMOD_OUT,
            Keyword::ANALYZE,
            Keyword::SUBSCRIPT,
            Keyword::INTERNALLENGTH,
            Keyword::PASSEDBYVALUE,
            Keyword::ALIGNMENT,
            Keyword::STORAGE,
            Keyword::LIKE,
            Keyword::CATEGORY,
            Keyword::PREFERRED,
            Keyword::DEFAULT,
            Keyword::ELEMENT,
            Keyword::DELIMITER,
            Keyword::COLLATABLE,
        ]);

        match keyword {
            Some(Keyword::INPUT) => {
                self.expect_token(&Token::Eq)?;
                let name = self.parse_object_name(false)?;
                Ok(UserDefinedTypeSqlDefinitionOption::Input(name))
            }
            Some(Keyword::OUTPUT) => {
                self.expect_token(&Token::Eq)?;
                let name = self.parse_object_name(false)?;
                Ok(UserDefinedTypeSqlDefinitionOption::Output(name))
            }
            Some(Keyword::RECEIVE) => {
                self.expect_token(&Token::Eq)?;
                let name = self.parse_object_name(false)?;
                Ok(UserDefinedTypeSqlDefinitionOption::Receive(name))
            }
            Some(Keyword::SEND) => {
                self.expect_token(&Token::Eq)?;
                let name = self.parse_object_name(false)?;
                Ok(UserDefinedTypeSqlDefinitionOption::Send(name))
            }
            Some(Keyword::TYPMOD_IN) => {
                self.expect_token(&Token::Eq)?;
                let name = self.parse_object_name(false)?;
                Ok(UserDefinedTypeSqlDefinitionOption::TypmodIn(name))
            }
            Some(Keyword::TYPMOD_OUT) => {
                self.expect_token(&Token::Eq)?;
                let name = self.parse_object_name(false)?;
                Ok(UserDefinedTypeSqlDefinitionOption::TypmodOut(name))
            }
            Some(Keyword::ANALYZE) => {
                self.expect_token(&Token::Eq)?;
                let name = self.parse_object_name(false)?;
                Ok(UserDefinedTypeSqlDefinitionOption::Analyze(name))
            }
            Some(Keyword::SUBSCRIPT) => {
                self.expect_token(&Token::Eq)?;
                let name = self.parse_object_name(false)?;
                Ok(UserDefinedTypeSqlDefinitionOption::Subscript(name))
            }
            Some(Keyword::INTERNALLENGTH) => {
                self.expect_token(&Token::Eq)?;
                if self.parse_keyword(Keyword::VARIABLE) {
                    Ok(UserDefinedTypeSqlDefinitionOption::InternalLength(
                        UserDefinedTypeInternalLength::Variable,
                    ))
                } else {
                    let value = self.parse_literal_uint()?;
                    Ok(UserDefinedTypeSqlDefinitionOption::InternalLength(
                        UserDefinedTypeInternalLength::Fixed(value),
                    ))
                }
            }
            Some(Keyword::PASSEDBYVALUE) => Ok(UserDefinedTypeSqlDefinitionOption::PassedByValue),
            Some(Keyword::ALIGNMENT) => {
                self.expect_token(&Token::Eq)?;
                let align_keyword = self.parse_one_of_keywords(&[
                    Keyword::CHAR,
                    Keyword::INT2,
                    Keyword::INT4,
                    Keyword::DOUBLE,
                ]);
                match align_keyword {
                    Some(Keyword::CHAR) => Ok(UserDefinedTypeSqlDefinitionOption::Alignment(
                        Alignment::Char,
                    )),
                    Some(Keyword::INT2) => Ok(UserDefinedTypeSqlDefinitionOption::Alignment(
                        Alignment::Int2,
                    )),
                    Some(Keyword::INT4) => Ok(UserDefinedTypeSqlDefinitionOption::Alignment(
                        Alignment::Int4,
                    )),
                    Some(Keyword::DOUBLE) => Ok(UserDefinedTypeSqlDefinitionOption::Alignment(
                        Alignment::Double,
                    )),
                    _ => self.expected(
                        "alignment value (char, int2, int4, or double)",
                        self.peek_token(),
                    ),
                }
            }
            Some(Keyword::STORAGE) => {
                self.expect_token(&Token::Eq)?;
                let storage_keyword = self.parse_one_of_keywords(&[
                    Keyword::PLAIN,
                    Keyword::EXTERNAL,
                    Keyword::EXTENDED,
                    Keyword::MAIN,
                ]);
                match storage_keyword {
                    Some(Keyword::PLAIN) => Ok(UserDefinedTypeSqlDefinitionOption::Storage(
                        UserDefinedTypeStorage::Plain,
                    )),
                    Some(Keyword::EXTERNAL) => Ok(UserDefinedTypeSqlDefinitionOption::Storage(
                        UserDefinedTypeStorage::External,
                    )),
                    Some(Keyword::EXTENDED) => Ok(UserDefinedTypeSqlDefinitionOption::Storage(
                        UserDefinedTypeStorage::Extended,
                    )),
                    Some(Keyword::MAIN) => Ok(UserDefinedTypeSqlDefinitionOption::Storage(
                        UserDefinedTypeStorage::Main,
                    )),
                    _ => self.expected(
                        "storage value (plain, external, extended, or main)",
                        self.peek_token(),
                    ),
                }
            }
            Some(Keyword::LIKE) => {
                self.expect_token(&Token::Eq)?;
                let name = self.parse_object_name(false)?;
                Ok(UserDefinedTypeSqlDefinitionOption::Like(name))
            }
            Some(Keyword::CATEGORY) => {
                self.expect_token(&Token::Eq)?;
                let category_str = self.parse_literal_string()?;
                let category_char = category_str.chars().next().ok_or_else(|| {
                    ParserError::ParserError(
                        "CATEGORY value must be a single character".to_string(),
                    )
                })?;
                Ok(UserDefinedTypeSqlDefinitionOption::Category(category_char))
            }
            Some(Keyword::PREFERRED) => {
                self.expect_token(&Token::Eq)?;
                let value =
                    self.parse_keyword(Keyword::TRUE) || !self.parse_keyword(Keyword::FALSE);
                Ok(UserDefinedTypeSqlDefinitionOption::Preferred(value))
            }
            Some(Keyword::DEFAULT) => {
                self.expect_token(&Token::Eq)?;
                let expr = self.parse_expr()?;
                Ok(UserDefinedTypeSqlDefinitionOption::Default(expr))
            }
            Some(Keyword::ELEMENT) => {
                self.expect_token(&Token::Eq)?;
                let data_type = self.parse_data_type()?;
                Ok(UserDefinedTypeSqlDefinitionOption::Element(data_type))
            }
            Some(Keyword::DELIMITER) => {
                self.expect_token(&Token::Eq)?;
                let delimiter = self.parse_literal_string()?;
                Ok(UserDefinedTypeSqlDefinitionOption::Delimiter(delimiter))
            }
            Some(Keyword::COLLATABLE) => {
                self.expect_token(&Token::Eq)?;
                let value =
                    self.parse_keyword(Keyword::TRUE) || !self.parse_keyword(Keyword::FALSE);
                Ok(UserDefinedTypeSqlDefinitionOption::Collatable(value))
            }
            _ => self.expected("SQL definition option keyword", self.peek_token()),
        }
    }

    fn parse_parenthesized_identifiers(&mut self) -> Result<Vec<Ident>, ParserError> {
        self.expect_token(&Token::LParen)?;
        let idents = self.parse_comma_separated0(|p| p.parse_identifier(), Token::RParen)?;
        self.expect_token(&Token::RParen)?;
        Ok(idents)
    }

    fn parse_column_position(&mut self) -> Result<Option<MySQLColumnPosition>, ParserError> {
        if dialect_of!(self is MySqlDialect | GenericDialect) {
            if self.parse_keyword(Keyword::FIRST) {
                Ok(Some(MySQLColumnPosition::First))
            } else if self.parse_keyword(Keyword::AFTER) {
                let ident = self.parse_identifier()?;
                Ok(Some(MySQLColumnPosition::After(ident)))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    /// Parse [Statement::Print]
    fn parse_print(&mut self) -> Result<Statement, ParserError> {
        Ok(Statement::Print(PrintStatement {
            message: Box::new(self.parse_expr()?),
        }))
    }

    /// Parse [Statement::Return]
    fn parse_return(&mut self) -> Result<Statement, ParserError> {
        match self.maybe_parse(|p| p.parse_expr())? {
            Some(expr) => Ok(Statement::Return(ReturnStatement {
                value: Some(ReturnStatementValue::Expr(expr)),
            })),
            None => Ok(Statement::Return(ReturnStatement { value: None })),
        }
    }

    /// /// Parse a `EXPORT DATA` statement.
    ///
    /// See [Statement::ExportData]
    fn parse_export_data(&mut self) -> Result<Statement, ParserError> {
        self.expect_keywords(&[Keyword::EXPORT, Keyword::DATA])?;

        let connection = if self.parse_keywords(&[Keyword::WITH, Keyword::CONNECTION]) {
            Some(self.parse_object_name(false)?)
        } else {
            None
        };
        self.expect_keyword(Keyword::OPTIONS)?;
        self.expect_token(&Token::LParen)?;
        let options = self.parse_comma_separated(|p| p.parse_sql_option())?;
        self.expect_token(&Token::RParen)?;
        self.expect_keyword(Keyword::AS)?;
        let query = self.parse_query()?;
        Ok(Statement::ExportData(ExportData {
            options,
            query,
            connection,
        }))
    }

    fn parse_vacuum(&mut self) -> Result<Statement, ParserError> {
        self.expect_keyword(Keyword::VACUUM)?;
        let full = self.parse_keyword(Keyword::FULL);
        let sort_only = self.parse_keywords(&[Keyword::SORT, Keyword::ONLY]);
        let delete_only = self.parse_keywords(&[Keyword::DELETE, Keyword::ONLY]);
        let reindex = self.parse_keyword(Keyword::REINDEX);
        let recluster = self.parse_keyword(Keyword::RECLUSTER);
        let (table_name, threshold, boost) =
            match self.maybe_parse(|p| p.parse_object_name(false))? {
                Some(table_name) => {
                    let threshold = if self.parse_keyword(Keyword::TO) {
                        let value = self.parse_value()?;
                        self.expect_keyword(Keyword::PERCENT)?;
                        Some(value.value)
                    } else {
                        None
                    };
                    let boost = self.parse_keyword(Keyword::BOOST);
                    (Some(table_name), threshold, boost)
                }
                _ => (None, None, false),
            };
        Ok(Statement::Vacuum(VacuumStatement {
            full,
            sort_only,
            delete_only,
            reindex,
            recluster,
            table_name,
            threshold,
            boost,
        }))
    }

    /// Consume the parser and return its underlying token buffer
    pub fn into_tokens(self) -> Vec<TokenWithSpan> {
        self.tokens
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

    pub(crate) fn parse_show_stmt_options(&mut self) -> Result<ShowStatementOptions, ParserError> {
        let show_in;
        let mut filter_position = None;
        if self.dialect.supports_show_like_before_in() {
            if let Some(filter) = self.parse_show_statement_filter()? {
                filter_position = Some(ShowStatementFilterPosition::Infix(filter));
            }
            show_in = self.maybe_parse_show_stmt_in()?;
        } else {
            show_in = self.maybe_parse_show_stmt_in()?;
            if let Some(filter) = self.parse_show_statement_filter()? {
                filter_position = Some(ShowStatementFilterPosition::Suffix(filter));
            }
        }
        let starts_with = self.maybe_parse_show_stmt_starts_with()?;
        let limit = self.maybe_parse_show_stmt_limit()?;
        let from = self.maybe_parse_show_stmt_from()?;
        Ok(ShowStatementOptions {
            filter_position,
            show_in,
            starts_with,
            limit,
            limit_from: from,
        })
    }

    fn maybe_parse_show_stmt_in(&mut self) -> Result<Option<ShowStatementIn>, ParserError> {
        let clause = match self.parse_one_of_keywords(&[Keyword::FROM, Keyword::IN]) {
            Some(Keyword::FROM) => ShowStatementInClause::FROM,
            Some(Keyword::IN) => ShowStatementInClause::IN,
            None => return Ok(None),
            _ => return self.expected("FROM or IN", self.peek_token()),
        };

        let (parent_type, parent_name) = match self.parse_one_of_keywords(&[
            Keyword::ACCOUNT,
            Keyword::DATABASE,
            Keyword::SCHEMA,
            Keyword::TABLE,
            Keyword::VIEW,
        ]) {
            // If we see these next keywords it means we don't have a parent name
            Some(Keyword::DATABASE)
                if self.peek_keywords(&[Keyword::STARTS, Keyword::WITH])
                    | self.peek_keyword(Keyword::LIMIT) =>
            {
                (Some(ShowStatementInParentType::Database), None)
            }
            Some(Keyword::SCHEMA)
                if self.peek_keywords(&[Keyword::STARTS, Keyword::WITH])
                    | self.peek_keyword(Keyword::LIMIT) =>
            {
                (Some(ShowStatementInParentType::Schema), None)
            }
            Some(parent_kw) => {
                // The parent name here is still optional, for example:
                // SHOW TABLES IN ACCOUNT, so parsing the object name
                // may fail because the statement ends.
                let parent_name = self.maybe_parse(|p| p.parse_object_name(false))?;
                match parent_kw {
                    Keyword::ACCOUNT => (Some(ShowStatementInParentType::Account), parent_name),
                    Keyword::DATABASE => (Some(ShowStatementInParentType::Database), parent_name),
                    Keyword::SCHEMA => (Some(ShowStatementInParentType::Schema), parent_name),
                    Keyword::TABLE => (Some(ShowStatementInParentType::Table), parent_name),
                    Keyword::VIEW => (Some(ShowStatementInParentType::View), parent_name),
                    _ => {
                        return self.expected(
                            "one of ACCOUNT, DATABASE, SCHEMA, TABLE or VIEW",
                            self.peek_token(),
                        )
                    }
                }
            }
            None => {
                // Parsing MySQL style FROM tbl_name FROM db_name
                // which is equivalent to FROM tbl_name.db_name
                let mut parent_name = self.parse_object_name(false)?;
                if self
                    .parse_one_of_keywords(&[Keyword::FROM, Keyword::IN])
                    .is_some()
                {
                    parent_name
                        .0
                        .insert(0, ObjectNamePart::Identifier(self.parse_identifier()?));
                }
                (None, Some(parent_name))
            }
        };

        Ok(Some(ShowStatementIn {
            clause,
            parent_type,
            parent_name,
        }))
    }

    fn maybe_parse_show_stmt_starts_with(&mut self) -> Result<Option<Value>, ParserError> {
        if self.parse_keywords(&[Keyword::STARTS, Keyword::WITH]) {
            Ok(Some(self.parse_value()?.value))
        } else {
            Ok(None)
        }
    }

    fn maybe_parse_show_stmt_limit(&mut self) -> Result<Option<Expr>, ParserError> {
        if self.parse_keyword(Keyword::LIMIT) {
            Ok(self.parse_limit()?)
        } else {
            Ok(None)
        }
    }

    fn maybe_parse_show_stmt_from(&mut self) -> Result<Option<Value>, ParserError> {
        if self.parse_keyword(Keyword::FROM) {
            Ok(Some(self.parse_value()?.value))
        } else {
            Ok(None)
        }
    }

    pub(crate) fn in_column_definition_state(&self) -> bool {
        matches!(self.state, ColumnDefinition)
    }

    /// Parses options provided in key-value format.
    ///
    /// * `parenthesized` - true if the options are enclosed in parenthesis
    /// * `end_words` - a list of keywords that any of them indicates the end of the options section
    pub(crate) fn parse_key_value_options(
        &mut self,
        parenthesized: bool,
        end_words: &[Keyword],
    ) -> Result<KeyValueOptions, ParserError> {
        let mut options: Vec<KeyValueOption> = Vec::new();
        let mut delimiter = KeyValueOptionsDelimiter::Space;
        if parenthesized {
            self.expect_token(&Token::LParen)?;
        }
        loop {
            match self.next_token().token {
                Token::RParen => {
                    if parenthesized {
                        break;
                    } else {
                        return self.expected(" another option or EOF", self.peek_token());
                    }
                }
                Token::EOF | Token::SemiColon => break,
                Token::Comma => {
                    delimiter = KeyValueOptionsDelimiter::Comma;
                    continue;
                }
                Token::Word(w) if !end_words.contains(&w.keyword) => {
                    options.push(self.parse_key_value_option(&w)?)
                }
                Token::Word(w) if end_words.contains(&w.keyword) => {
                    self.prev_token();
                    break;
                }
                _ => {
                    return self.expected(
                        "another option, EOF, SemiColon, Comma or ')'",
                        self.peek_token(),
                    )
                }
            };
        }

        Ok(KeyValueOptions { delimiter, options })
    }

    /// Parses a `KEY = VALUE` construct based on the specified key
    pub(crate) fn parse_key_value_option(
        &mut self,
        key: &Word,
    ) -> Result<KeyValueOption, ParserError> {
        self.expect_token(&Token::Eq)?;
        match self.peek_token().token {
            Token::SingleQuotedString(_) => Ok(KeyValueOption {
                option_name: key.value.clone(),
                option_value: KeyValueOptionKind::Single(self.parse_value()?.into()),
            }),
            Token::Word(word)
                if word.keyword == Keyword::TRUE || word.keyword == Keyword::FALSE =>
            {
                Ok(KeyValueOption {
                    option_name: key.value.clone(),
                    option_value: KeyValueOptionKind::Single(self.parse_value()?.into()),
                })
            }
            Token::Number(..) => Ok(KeyValueOption {
                option_name: key.value.clone(),
                option_value: KeyValueOptionKind::Single(self.parse_value()?.into()),
            }),
            Token::Word(word) => {
                self.next_token();
                Ok(KeyValueOption {
                    option_name: key.value.clone(),
                    option_value: KeyValueOptionKind::Single(Value::Placeholder(
                        word.value.clone(),
                    )),
                })
            }
            Token::LParen => {
                // Can be a list of values or a list of key value properties.
                // Try to parse a list of values and if that fails, try to parse
                // a list of key-value properties.
                match self.maybe_parse(|parser| {
                    parser.expect_token(&Token::LParen)?;
                    let values = parser.parse_comma_separated0(|p| p.parse_value(), Token::RParen);
                    parser.expect_token(&Token::RParen)?;
                    values
                })? {
                    Some(values) => {
                        let values = values.into_iter().map(|v| v.value).collect();
                        Ok(KeyValueOption {
                            option_name: key.value.clone(),
                            option_value: KeyValueOptionKind::Multi(values),
                        })
                    }
                    None => Ok(KeyValueOption {
                        option_name: key.value.clone(),
                        option_value: KeyValueOptionKind::KeyValueOptions(Box::new(
                            self.parse_key_value_options(true, &[])?,
                        )),
                    }),
                }
            }
            _ => self.expected("expected option value", self.peek_token()),
        }
    }

    /// Parses a RESET statement
    fn parse_reset(&mut self) -> Result<ResetStatement, ParserError> {
        if self.parse_keyword(Keyword::ALL) {
            return Ok(ResetStatement { reset: Reset::ALL });
        }

        let obj = self.parse_object_name(false)?;
        Ok(ResetStatement {
            reset: Reset::ConfigurationParameter(obj),
        })
    }
}

fn maybe_prefixed_expr(expr: Expr, prefix: Option<Ident>) -> Expr {
    if let Some(prefix) = prefix {
        Expr::Prefixed {
            prefix,
            value: Box::new(expr),
        }
    } else {
        expr
    }
}

impl Word {
    /// Convert a reference to this word into an [`Ident`] by cloning the value.
    ///
    /// Use this method when you need to keep the original `Word` around.
    /// If you can consume the `Word`, prefer [`into_ident`](Self::into_ident) instead
    /// to avoid cloning.
    pub fn to_ident(&self, span: Span) -> Ident {
        Ident {
            value: self.value.clone(),
            quote_style: self.quote_style,
            span,
        }
    }

    /// Convert this word into an [`Ident`] identifier, consuming the `Word`.
    ///
    /// This avoids cloning the string value. If you need to keep the original
    /// `Word`, use [`to_ident`](Self::to_ident) instead.
    pub fn into_ident(self, span: Span) -> Ident {
        Ident {
            value: self.value,
            quote_style: self.quote_style,
            span,
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
        use crate::dialect::{AnsiDialect, GenericDialect, PostgreSqlDialect};
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
            let dialect =
                TestedDialects::new(vec![Box::new(GenericDialect {}), Box::new(AnsiDialect {})]);

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
            let dialect =
                TestedDialects::new(vec![Box::new(GenericDialect {}), Box::new(AnsiDialect {})]);

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
            let dialect =
                TestedDialects::new(vec![Box::new(GenericDialect {}), Box::new(AnsiDialect {})]);

            test_parse_data_type!(
                dialect,
                "GEOMETRY",
                DataType::Custom(ObjectName::from(vec!["GEOMETRY".into()]), vec![])
            );

            test_parse_data_type!(
                dialect,
                "GEOMETRY(POINT)",
                DataType::Custom(
                    ObjectName::from(vec!["GEOMETRY".into()]),
                    vec!["POINT".to_string()]
                )
            );

            test_parse_data_type!(
                dialect,
                "GEOMETRY(POINT, 4326)",
                DataType::Custom(
                    ObjectName::from(vec!["GEOMETRY".into()]),
                    vec!["POINT".to_string(), "4326".to_string()]
                )
            );
        }

        #[test]
        fn test_ansii_exact_numeric_types() {
            // Exact numeric types: <https://jakewheat.github.io/sql-overview/sql-2016-foundation-grammar.html#exact-numeric-type>
            let dialect = TestedDialects::new(vec![
                Box::new(GenericDialect {}),
                Box::new(AnsiDialect {}),
                Box::new(PostgreSqlDialect {}),
            ]);

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

            // Test negative scale values.
            test_parse_data_type!(
                dialect,
                "NUMERIC(10,-2)",
                DataType::Numeric(ExactNumberInfo::PrecisionAndScale(10, -2))
            );

            test_parse_data_type!(
                dialect,
                "DECIMAL(1000,-10)",
                DataType::Decimal(ExactNumberInfo::PrecisionAndScale(1000, -10))
            );

            test_parse_data_type!(
                dialect,
                "DEC(5,-1000)",
                DataType::Dec(ExactNumberInfo::PrecisionAndScale(5, -1000))
            );

            test_parse_data_type!(
                dialect,
                "NUMERIC(10,-5)",
                DataType::Numeric(ExactNumberInfo::PrecisionAndScale(10, -5))
            );

            test_parse_data_type!(
                dialect,
                "DECIMAL(20,-10)",
                DataType::Decimal(ExactNumberInfo::PrecisionAndScale(20, -10))
            );

            test_parse_data_type!(
                dialect,
                "DEC(5,-2)",
                DataType::Dec(ExactNumberInfo::PrecisionAndScale(5, -2))
            );

            dialect.run_parser_method("NUMERIC(10,+5)", |parser| {
                let data_type = parser.parse_data_type().unwrap();
                assert_eq!(
                    DataType::Numeric(ExactNumberInfo::PrecisionAndScale(10, 5)),
                    data_type
                );
                // Note: Explicit '+' sign is not preserved in output, which is correct
                assert_eq!("NUMERIC(10,5)", data_type.to_string());
            });
        }

        #[test]
        fn test_ansii_date_type() {
            // Datetime types: <https://jakewheat.github.io/sql-overview/sql-2016-foundation-grammar.html#datetime-type>
            let dialect =
                TestedDialects::new(vec![Box::new(GenericDialect {}), Box::new(AnsiDialect {})]);

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

        let dummy_name = ObjectName::from(vec![Ident::new("dummy_name")]);
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

        fn mk_expected_col(name: &str) -> IndexColumn {
            IndexColumn {
                column: OrderByExpr {
                    expr: Expr::Identifier(name.into()),
                    options: OrderByOptions {
                        asc: None,
                        nulls_first: None,
                    },
                    with_fill: None,
                },
                operator_class: None,
            }
        }

        let dialect =
            TestedDialects::new(vec![Box::new(GenericDialect {}), Box::new(MySqlDialect {})]);

        test_parse_table_constraint!(
            dialect,
            "INDEX (c1)",
            IndexConstraint {
                display_as_key: false,
                name: None,
                index_type: None,
                columns: vec![mk_expected_col("c1")],
                index_options: vec![],
            }
            .into()
        );

        test_parse_table_constraint!(
            dialect,
            "KEY (c1)",
            IndexConstraint {
                display_as_key: true,
                name: None,
                index_type: None,
                columns: vec![mk_expected_col("c1")],
                index_options: vec![],
            }
            .into()
        );

        test_parse_table_constraint!(
            dialect,
            "INDEX 'index' (c1, c2)",
            TableConstraint::Index(IndexConstraint {
                display_as_key: false,
                name: Some(Ident::with_quote('\'', "index")),
                index_type: None,
                columns: vec![mk_expected_col("c1"), mk_expected_col("c2")],
                index_options: vec![],
            })
        );

        test_parse_table_constraint!(
            dialect,
            "INDEX USING BTREE (c1)",
            IndexConstraint {
                display_as_key: false,
                name: None,
                index_type: Some(IndexType::BTree),
                columns: vec![mk_expected_col("c1")],
                index_options: vec![],
            }
            .into()
        );

        test_parse_table_constraint!(
            dialect,
            "INDEX USING HASH (c1)",
            IndexConstraint {
                display_as_key: false,
                name: None,
                index_type: Some(IndexType::Hash),
                columns: vec![mk_expected_col("c1")],
                index_options: vec![],
            }
            .into()
        );

        test_parse_table_constraint!(
            dialect,
            "INDEX idx_name USING BTREE (c1)",
            IndexConstraint {
                display_as_key: false,
                name: Some(Ident::new("idx_name")),
                index_type: Some(IndexType::BTree),
                columns: vec![mk_expected_col("c1")],
                index_options: vec![],
            }
            .into()
        );

        test_parse_table_constraint!(
            dialect,
            "INDEX idx_name USING HASH (c1)",
            IndexConstraint {
                display_as_key: false,
                name: Some(Ident::new("idx_name")),
                index_type: Some(IndexType::Hash),
                columns: vec![mk_expected_col("c1")],
                index_options: vec![],
            }
            .into()
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
                "Expected: [NOT] NULL | TRUE | FALSE | DISTINCT | [form] NORMALIZED FROM after IS, found: a at Line: 1, Column: 16"
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
        let dialect = TestedDialects::new(vec![Box::new(GenericDialect {})]);

        // parse multipart with quotes
        let expected = vec![
            Ident {
                value: "CATALOG".to_string(),
                quote_style: None,
                span: Span::empty(),
            },
            Ident {
                value: "F(o)o. \"bar".to_string(),
                quote_style: Some('"'),
                span: Span::empty(),
            },
            Ident {
                value: "table".to_string(),
                quote_style: None,
                span: Span::empty(),
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
                span: Span::empty(),
            },
            Ident {
                value: "table".to_string(),
                quote_style: None,
                span: Span::empty(),
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
    fn test_replace_into_set_placeholder() {
        let sql = "REPLACE INTO t SET ?";

        assert!(Parser::parse_sql(&GenericDialect {}, sql).is_err());
    }

    #[test]
    fn test_replace_incomplete() {
        let sql = r#"REPLACE"#;

        assert!(Parser::parse_sql(&MySqlDialect {}, sql).is_err());
    }

    #[test]
    fn test_placeholder_invalid_whitespace() {
        for w in ["  ", "/*invalid*/"] {
            let sql = format!("\nSELECT\n  :{w}fooBar");
            assert!(Parser::parse_sql(&GenericDialect, &sql).is_err());
        }
    }
}

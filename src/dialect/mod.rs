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

mod ansi;
mod bigquery;
mod clickhouse;
mod databricks;
mod duckdb;
mod generic;
mod hive;
mod mssql;
mod mysql;
mod postgresql;
mod redshift;
mod snowflake;
mod sqlite;

use core::any::{Any, TypeId};
use core::fmt::Debug;
use core::iter::Peekable;
use core::str::Chars;

use log::debug;

pub use self::ansi::AnsiDialect;
pub use self::bigquery::BigQueryDialect;
pub use self::clickhouse::ClickHouseDialect;
pub use self::databricks::DatabricksDialect;
pub use self::duckdb::DuckDbDialect;
pub use self::generic::GenericDialect;
pub use self::hive::HiveDialect;
pub use self::mssql::MsSqlDialect;
pub use self::mysql::MySqlDialect;
pub use self::postgresql::PostgreSqlDialect;
pub use self::redshift::RedshiftSqlDialect;
pub use self::snowflake::SnowflakeDialect;
pub use self::sqlite::SQLiteDialect;
pub use crate::keywords;
use crate::ast::{Expr, Statement};
use crate::parser::{Parser, ParserError};
use crate::keywords::Keyword;
use crate::tokenizer::Token;

#[cfg(not(feature = "std"))]
use alloc::boxed::Box;

/// Convenience check if a [`Parser`] uses a certain dialect.
///
/// `dialect_of!(parser Is SQLiteDialect |  GenericDialect)` evaluates
/// to `true` if `parser.dialect` is one of the [`Dialect`]s specified.
macro_rules! dialect_of {
    ( $parsed_dialect: ident is $($dialect_type: ty)|+ ) => {
        ($($parsed_dialect.dialect.is::<$dialect_type>())||+)
    };
}

/// Encapsulates the differences between SQL implementations.
///
/// # SQL Dialects
/// SQL implementations deviatiate from one another, either due to
/// custom extensions or various historical reasons. This trait
/// encapsulates the parsing differences between dialects.
///
/// [`GenericDialect`] is the most permissive dialect, and parses the union of
/// all the other dialects, when there is no ambiguity. However, it does not
/// currently allow `CREATE TABLE` statements without types specified for all
/// columns; use [`SQLiteDialect`] if you require that.
///
/// # Examples
/// Most users create a [`Dialect`] directly, as shown on the [module
/// level documentation]:
///
/// ```
/// # use sqlparser::dialect::AnsiDialect;
/// let dialect = AnsiDialect {};
/// ```
///
/// It is also possible to dynamically create a [`Dialect`] from its
/// name. For example:
///
/// ```
/// # use sqlparser::dialect::{AnsiDialect, dialect_from_str};
/// let dialect = dialect_from_str("ansi").unwrap();
///
/// // Parsed dialect is an instance of `AnsiDialect`:
/// assert!(dialect.is::<AnsiDialect>());
/// ```
///
/// [module level documentation]: crate
pub trait Dialect: Debug + Any {
    /// Determine the [`TypeId`] of this dialect.
    ///
    /// By default, return the same [`TypeId`] as [`Any::type_id`]. Can be overridden
    /// by dialects that behave like other dialects
    /// (for example when wrapping a dialect).
    fn dialect(&self) -> TypeId {
        self.type_id()
    }

    /// Determine if a character starts a quoted identifier. The default
    /// implementation, accepting "double quoted" ids is both ANSI-compliant
    /// and appropriate for most dialects (with the notable exception of
    /// MySQL, MS SQL, and sqlite). You can accept one of characters listed
    /// in `Word::matching_end_quote` here
    fn is_delimited_identifier_start(&self, ch: char) -> bool {
        ch == '"' || ch == '`'
    }
    /// Return the character used to quote identifiers.
    fn identifier_quote_style(&self, _identifier: &str) -> Option<char> {
        None
    }
    /// Determine if quoted characters are proper for identifier
    fn is_proper_identifier_inside_quotes(&self, mut _chars: Peekable<Chars<'_>>) -> bool {
        true
    }
    /// Determine if a character is a valid start character for an unquoted identifier
    fn is_identifier_start(&self, ch: char) -> bool;
    /// Determine if a character is a valid unquoted identifier character
    fn is_identifier_part(&self, ch: char) -> bool;

    /// Most dialects do not have custom operators. Override this method to provide custom operators.
    fn is_custom_operator_part(&self, _ch: char) -> bool {
        false
    }

    /// Determine if the dialect supports escaping characters via '\' in string literals.
    ///
    /// Some dialects like BigQuery and Snowflake support this while others like
    /// Postgres do not. Such that the following is accepted by the former but
    /// rejected by the latter.
    /// ```sql
    /// SELECT 'ab\'cd';
    /// ```
    ///
    /// Conversely, such dialects reject the following statement which
    /// otherwise would be valid in the other dialects.
    /// ```sql
    /// SELECT '\';
    /// ```
    fn supports_string_literal_backslash_escape(&self) -> bool {
        false
    }

    /// Determine if the dialect supports string literals with `U&` prefix.
    /// This is used to specify Unicode code points in string literals.
    /// For example, in PostgreSQL, the following is a valid string literal:
    /// ```sql
    /// SELECT U&'\0061\0062\0063';
    /// ```
    /// This is equivalent to the string literal `'abc'`.
    /// See
    ///  - [Postgres docs](https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-STRINGS-UESCAPE)
    ///  - [H2 docs](http://www.h2database.com/html/grammar.html#string)
    fn supports_unicode_string_literal(&self) -> bool {
        false
    }

    /// Does the dialect support `FILTER (WHERE expr)` for aggregate queries?
    fn supports_filter_during_aggregation(&self) -> bool {
        false
    }
    /// Returns true if the dialect supports referencing another named window
    /// within a window clause declaration.
    ///
    /// Example
    /// ```sql
    /// SELECT * FROM mytable
    /// WINDOW mynamed_window AS another_named_window
    /// ```
    fn supports_window_clause_named_window_reference(&self) -> bool {
        false
    }
    /// Returns true if the dialect supports `ARRAY_AGG() [WITHIN GROUP (ORDER BY)]` expressions.
    /// Otherwise, the dialect should expect an `ORDER BY` without the `WITHIN GROUP` clause, e.g. [`ANSI`]
    ///
    /// [`ANSI`]: https://jakewheat.github.io/sql-overview/sql-2016-foundation-grammar.html#array-aggregate-function
    fn supports_within_after_array_aggregation(&self) -> bool {
        false
    }
    /// Returns true if the dialects supports `group sets, roll up, or cube` expressions.
    fn supports_group_by_expr(&self) -> bool {
        false
    }
    /// Returns true if the dialect supports CONNECT BY.
    fn supports_connect_by(&self) -> bool {
        false
    }
    /// Returns true if the dialect supports the MATCH_RECOGNIZE operation.
    fn supports_match_recognize(&self) -> bool {
        false
    }
    /// Returns true if the dialect supports `(NOT) IN ()` expressions
    fn supports_in_empty_list(&self) -> bool {
        false
    }
    /// Returns true if the dialect supports `BEGIN {DEFERRED | IMMEDIATE | EXCLUSIVE} [TRANSACTION]` statements
    fn supports_start_transaction_modifier(&self) -> bool {
        false
    }
    /// Returns true if the dialect supports named arguments of the form FUN(a = '1', b = '2').
    fn supports_named_fn_args_with_eq_operator(&self) -> bool {
        false
    }
    /// Returns true if the dialect supports identifiers starting with a numeric
    /// prefix such as tables named: `59901_user_login`
    fn supports_numeric_prefix(&self) -> bool {
        false
    }
    /// Returns true if the dialects supports specifying null treatment
    /// as part of a window function's parameter list. As opposed
    /// to after the parameter list.
    /// i.e The following syntax returns true
    /// ```sql
    /// FIRST_VALUE(a IGNORE NULLS) OVER ()
    /// ```
    /// while the following syntax returns false
    /// ```sql
    /// FIRST_VALUE(a) IGNORE NULLS OVER ()
    /// ```
    fn supports_window_function_null_treatment_arg(&self) -> bool {
        false
    }
    /// Returns true if the dialect supports defining structs or objects using a
    /// syntax like `{'x': 1, 'y': 2, 'z': 3}`.
    fn supports_dictionary_syntax(&self) -> bool {
        false
    }
    /// Returns true if the dialect supports defining object using the
    /// syntax like `Map {1: 10, 2: 20}`.
    fn support_map_literal_syntax(&self) -> bool {
        false
    }
    /// Returns true if the dialect supports lambda functions, for example:
    ///
    /// ```sql
    /// SELECT transform(array(1, 2, 3), x -> x + 1); -- returns [2,3,4]
    /// ```
    fn supports_lambda_functions(&self) -> bool {
        false
    }
    /// Returns true if the dialect supports multiple variable assignment
    /// using parentheses in a `SET` variable declaration.
    ///
    /// ```sql
    /// SET (variable[, ...]) = (expression[, ...]);
    /// ```
    fn supports_parenthesized_set_variables(&self) -> bool {
        false
    }
    /// Returns true if the dialect supports an `EXCEPT` clause following a
    /// wildcard in a select list.
    ///
    /// For example
    /// ```sql
    /// SELECT * EXCEPT order_id FROM orders;
    /// ```
    fn supports_select_wildcard_except(&self) -> bool {
        false
    }
    /// Returns true if the dialect has a CONVERT function which accepts a type first
    /// and an expression second, e.g. `CONVERT(varchar, 1)`
    fn convert_type_before_value(&self) -> bool {
        false
    }
    /// Returns true if the dialect supports triple quoted string
    /// e.g. `"""abc"""`
    fn supports_triple_quoted_string(&self) -> bool {
        false
    }
    /// Dialect-specific prefix parser override
    fn parse_prefix(&self, _parser: &mut Parser) -> Option<Result<Expr, ParserError>> {
        // return None to fall back to the default behavior
        None
    }
    /// Does the dialect support trailing commas around the query?
    fn supports_trailing_commas(&self) -> bool {
        false
    }
    /// Does the dialect support trailing commas in the projection list?
    fn supports_projection_trailing_commas(&self) -> bool {
        self.supports_trailing_commas()
    }
    /// Dialect-specific infix parser override
    fn parse_infix(
        &self,
        _parser: &mut Parser,
        _expr: &Expr,
        _precedence: u8,
    ) -> Option<Result<Expr, ParserError>> {
        // return None to fall back to the default behavior
        None
    }
    /// Dialect-specific precedence override
    fn get_next_precedence(&self, _parser: &Parser) -> Option<Result<u8, ParserError>> {
        // return None to fall back to the default behavior
        None
    }

    /// Get the precedence of the next token. This "full" method means all precedence logic and remain
    /// in the dialect. while still allowing overriding the `get_next_precedence` method with the option to
    /// fallback to the default behavior.
    ///
    /// Higher number => higher precedence
    fn get_next_precedence_full(&self, parser: &Parser) -> Result<u8, ParserError> {
        if let Some(precedence) = self.get_next_precedence(parser) {
            return precedence;
        }

        let token = parser.peek_token();
        debug!("get_next_precedence() {:?}", token);
        match token.token {
            Token::Word(w) if w.keyword == Keyword::OR => Ok(OR_PREC),
            Token::Word(w) if w.keyword == Keyword::AND => Ok(AND_PREC),
            Token::Word(w) if w.keyword == Keyword::XOR => Ok(XOR_PREC),

            Token::Word(w) if w.keyword == Keyword::AT => {
                match (
                    parser.peek_nth_token(1).token,
                    parser.peek_nth_token(2).token,
                ) {
                    (Token::Word(w), Token::Word(w2))
                        if w.keyword == Keyword::TIME && w2.keyword == Keyword::ZONE =>
                    {
                        Ok(AT_TZ_PREC)
                    }
                    _ => Ok(UNKNOWN_PREC),
                }
            }

            Token::Word(w) if w.keyword == Keyword::NOT => match parser.peek_nth_token(1).token {
                // The precedence of NOT varies depending on keyword that
                // follows it. If it is followed by IN, BETWEEN, or LIKE,
                // it takes on the precedence of those tokens. Otherwise, it
                // is not an infix operator, and therefore has zero
                // precedence.
                Token::Word(w) if w.keyword == Keyword::IN => Ok(BETWEEN_PREC),
                Token::Word(w) if w.keyword == Keyword::BETWEEN => Ok(BETWEEN_PREC),
                Token::Word(w) if w.keyword == Keyword::LIKE => Ok(LIKE_PREC),
                Token::Word(w) if w.keyword == Keyword::ILIKE => Ok(LIKE_PREC),
                Token::Word(w) if w.keyword == Keyword::RLIKE => Ok(LIKE_PREC),
                Token::Word(w) if w.keyword == Keyword::REGEXP => Ok(LIKE_PREC),
                Token::Word(w) if w.keyword == Keyword::SIMILAR => Ok(LIKE_PREC),
                _ => Ok(UNKNOWN_PREC),
            },
            Token::Word(w) if w.keyword == Keyword::IS => Ok(IS_PREC),
            Token::Word(w) if w.keyword == Keyword::IN => Ok(BETWEEN_PREC),
            Token::Word(w) if w.keyword == Keyword::BETWEEN => Ok(BETWEEN_PREC),
            Token::Word(w) if w.keyword == Keyword::LIKE => Ok(LIKE_PREC),
            Token::Word(w) if w.keyword == Keyword::ILIKE => Ok(LIKE_PREC),
            Token::Word(w) if w.keyword == Keyword::RLIKE => Ok(LIKE_PREC),
            Token::Word(w) if w.keyword == Keyword::REGEXP => Ok(LIKE_PREC),
            Token::Word(w) if w.keyword == Keyword::SIMILAR => Ok(LIKE_PREC),
            Token::Word(w) if w.keyword == Keyword::OPERATOR => Ok(BETWEEN_PREC),
            Token::Word(w) if w.keyword == Keyword::DIV => Ok(MUL_DIV_MOD_OP_PREC),
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
            | Token::DoubleTilde
            | Token::DoubleTildeAsterisk
            | Token::ExclamationMarkDoubleTilde
            | Token::ExclamationMarkDoubleTildeAsterisk
            | Token::Spaceship => Ok(EQ_PREC),
            Token::Pipe => Ok(PIPE_PREC),
            Token::Caret | Token::Sharp | Token::ShiftRight | Token::ShiftLeft => Ok(CARET_PREC),
            Token::Ampersand => Ok(AMPERSAND_PREC),
            Token::Plus | Token::Minus => Ok(PLUS_MINUS_PREC),
            Token::Mul | Token::Div | Token::DuckIntDiv | Token::Mod | Token::StringConcat => {
                Ok(MUL_DIV_MOD_OP_PREC)
            }
            Token::DoubleColon
            | Token::ExclamationMark
            | Token::LBracket
            | Token::Overlap
            | Token::CaretAt => Ok(DOUBLE_COLON_PREC),
            // Token::Colon if (self as dyn Dialect).is::<SnowflakeDialect>() => Ok(DOUBLE_COLON_PREC),
            Token::Arrow
            | Token::LongArrow
            | Token::HashArrow
            | Token::HashLongArrow
            | Token::AtArrow
            | Token::ArrowAt
            | Token::HashMinus
            | Token::AtQuestion
            | Token::AtAt
            | Token::Question
            | Token::QuestionAnd
            | Token::QuestionPipe
            | Token::CustomBinaryOperator(_) => Ok(PG_OTHER_PREC),
            _ => Ok(UNKNOWN_PREC),
        }
    }

    /// Dialect-specific statement parser override
    fn parse_statement(&self, _parser: &mut Parser) -> Option<Result<Statement, ParserError>> {
        // return None to fall back to the default behavior
        None
    }

    /// The following precedence values are used directly by `Parse` or in dialects,
    /// so have to be made public by the dialect.
    fn prec_double_colon(&self) -> u8 {
        DOUBLE_COLON_PREC
    }

    fn prec_mul_div_mod_op(&self) -> u8 {
        MUL_DIV_MOD_OP_PREC
    }

    fn prec_plus_minus(&self) -> u8 {
        PLUS_MINUS_PREC
    }

    fn prec_between(&self) -> u8 {
        BETWEEN_PREC
    }

    fn prec_like(&self) -> u8 {
        LIKE_PREC
    }

    fn prec_unary_not(&self) -> u8 {
        UNARY_NOT_PREC
    }

    fn prec_unknown(&self) -> u8 {
        UNKNOWN_PREC
    }
}

// Define the lexical Precedence of operators.
//
// Uses (APPROXIMATELY) <https://www.postgresql.org/docs/7.0/operators.htm#AEN2026> as a reference
// higher number = higher precedence
//
// NOTE: The pg documentation is incomplete, e.g. the AT TIME ZONE operator
//       actually has higher precedence than addition.
//       See <https://postgrespro.com/list/thread-id/2673331>.
const DOUBLE_COLON_PREC: u8 = 50;
const AT_TZ_PREC: u8 = 41;
const MUL_DIV_MOD_OP_PREC: u8 = 40;
const PLUS_MINUS_PREC: u8 = 30;
const XOR_PREC: u8 = 24;
const AMPERSAND_PREC: u8 = 23;
const CARET_PREC: u8 = 22;
const PIPE_PREC: u8 = 21;
const BETWEEN_PREC: u8 = 20;
const EQ_PREC: u8 = 20;
const LIKE_PREC: u8 = 19;
const IS_PREC: u8 = 17;
const PG_OTHER_PREC: u8 = 16;
const UNARY_NOT_PREC: u8 = 15;
const AND_PREC: u8 = 10;
const OR_PREC: u8 = 5;
const UNKNOWN_PREC: u8 = 0;

impl dyn Dialect {
    #[inline]
    pub fn is<T: Dialect>(&self) -> bool {
        // borrowed from `Any` implementation
        TypeId::of::<T>() == self.dialect()
    }
}

/// Returns the built in [`Dialect`] corresponding to `dialect_name`.
///
/// See [`Dialect`] documentation for an example.
pub fn dialect_from_str(dialect_name: impl AsRef<str>) -> Option<Box<dyn Dialect>> {
    let dialect_name = dialect_name.as_ref();
    match dialect_name.to_lowercase().as_str() {
        "generic" => Some(Box::new(GenericDialect)),
        "mysql" => Some(Box::new(MySqlDialect {})),
        "postgresql" | "postgres" => Some(Box::new(PostgreSqlDialect {})),
        "hive" => Some(Box::new(HiveDialect {})),
        "sqlite" => Some(Box::new(SQLiteDialect {})),
        "snowflake" => Some(Box::new(SnowflakeDialect)),
        "redshift" => Some(Box::new(RedshiftSqlDialect {})),
        "mssql" => Some(Box::new(MsSqlDialect {})),
        "clickhouse" => Some(Box::new(ClickHouseDialect {})),
        "bigquery" => Some(Box::new(BigQueryDialect)),
        "ansi" => Some(Box::new(AnsiDialect {})),
        "duckdb" => Some(Box::new(DuckDbDialect {})),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct DialectHolder<'a> {
        dialect: &'a dyn Dialect,
    }

    #[test]
    fn test_is_dialect() {
        let generic_dialect: &dyn Dialect = &GenericDialect {};
        let ansi_dialect: &dyn Dialect = &AnsiDialect {};

        let generic_holder = DialectHolder {
            dialect: generic_dialect,
        };
        let ansi_holder = DialectHolder {
            dialect: ansi_dialect,
        };

        assert!(dialect_of!(generic_holder is GenericDialect |  AnsiDialect),);
        assert!(!dialect_of!(generic_holder is  AnsiDialect));
        assert!(dialect_of!(ansi_holder is AnsiDialect));
        assert!(dialect_of!(ansi_holder is GenericDialect | AnsiDialect));
        assert!(!dialect_of!(ansi_holder is GenericDialect | MsSqlDialect));
    }

    #[test]
    fn test_dialect_from_str() {
        assert!(parse_dialect("generic").is::<GenericDialect>());
        assert!(parse_dialect("mysql").is::<MySqlDialect>());
        assert!(parse_dialect("MySql").is::<MySqlDialect>());
        assert!(parse_dialect("postgresql").is::<PostgreSqlDialect>());
        assert!(parse_dialect("postgres").is::<PostgreSqlDialect>());
        assert!(parse_dialect("hive").is::<HiveDialect>());
        assert!(parse_dialect("sqlite").is::<SQLiteDialect>());
        assert!(parse_dialect("snowflake").is::<SnowflakeDialect>());
        assert!(parse_dialect("SnowFlake").is::<SnowflakeDialect>());
        assert!(parse_dialect("MsSql").is::<MsSqlDialect>());
        assert!(parse_dialect("clickhouse").is::<ClickHouseDialect>());
        assert!(parse_dialect("ClickHouse").is::<ClickHouseDialect>());
        assert!(parse_dialect("bigquery").is::<BigQueryDialect>());
        assert!(parse_dialect("BigQuery").is::<BigQueryDialect>());
        assert!(parse_dialect("ansi").is::<AnsiDialect>());
        assert!(parse_dialect("ANSI").is::<AnsiDialect>());
        assert!(parse_dialect("duckdb").is::<DuckDbDialect>());
        assert!(parse_dialect("DuckDb").is::<DuckDbDialect>());

        // error cases
        assert!(dialect_from_str("Unknown").is_none());
        assert!(dialect_from_str("").is_none());
    }

    fn parse_dialect(v: &str) -> Box<dyn Dialect> {
        dialect_from_str(v).unwrap()
    }

    #[test]
    fn identifier_quote_style() {
        let tests: Vec<(&dyn Dialect, &str, Option<char>)> = vec![
            (&GenericDialect {}, "id", None),
            (&SQLiteDialect {}, "id", Some('`')),
            (&PostgreSqlDialect {}, "id", Some('"')),
        ];

        for (dialect, ident, expected) in tests {
            let actual = dialect.identifier_quote_style(ident);

            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn parse_with_wrapped_dialect() {
        /// Wrapper for a dialect. In a real-world example, this wrapper
        /// would tweak the behavior of the dialect. For the test case,
        /// it wraps all methods unaltered.
        #[derive(Debug)]
        struct WrappedDialect(MySqlDialect);

        impl Dialect for WrappedDialect {
            fn dialect(&self) -> std::any::TypeId {
                self.0.dialect()
            }

            fn is_identifier_start(&self, ch: char) -> bool {
                self.0.is_identifier_start(ch)
            }

            fn is_delimited_identifier_start(&self, ch: char) -> bool {
                self.0.is_delimited_identifier_start(ch)
            }

            fn identifier_quote_style(&self, identifier: &str) -> Option<char> {
                self.0.identifier_quote_style(identifier)
            }

            fn supports_string_literal_backslash_escape(&self) -> bool {
                self.0.supports_string_literal_backslash_escape()
            }

            fn is_proper_identifier_inside_quotes(
                &self,
                chars: std::iter::Peekable<std::str::Chars<'_>>,
            ) -> bool {
                self.0.is_proper_identifier_inside_quotes(chars)
            }

            fn supports_filter_during_aggregation(&self) -> bool {
                self.0.supports_filter_during_aggregation()
            }

            fn supports_within_after_array_aggregation(&self) -> bool {
                self.0.supports_within_after_array_aggregation()
            }

            fn supports_group_by_expr(&self) -> bool {
                self.0.supports_group_by_expr()
            }

            fn supports_in_empty_list(&self) -> bool {
                self.0.supports_in_empty_list()
            }

            fn convert_type_before_value(&self) -> bool {
                self.0.convert_type_before_value()
            }

            fn parse_prefix(
                &self,
                parser: &mut sqlparser::parser::Parser,
            ) -> Option<Result<Expr, sqlparser::parser::ParserError>> {
                self.0.parse_prefix(parser)
            }

            fn parse_infix(
                &self,
                parser: &mut sqlparser::parser::Parser,
                expr: &Expr,
                precedence: u8,
            ) -> Option<Result<Expr, sqlparser::parser::ParserError>> {
                self.0.parse_infix(parser, expr, precedence)
            }

            fn get_next_precedence(
                &self,
                parser: &sqlparser::parser::Parser,
            ) -> Option<Result<u8, sqlparser::parser::ParserError>> {
                self.0.get_next_precedence(parser)
            }

            fn parse_statement(
                &self,
                parser: &mut sqlparser::parser::Parser,
            ) -> Option<Result<Statement, sqlparser::parser::ParserError>> {
                self.0.parse_statement(parser)
            }

            fn is_identifier_part(&self, ch: char) -> bool {
                self.0.is_identifier_part(ch)
            }
        }

        #[allow(clippy::needless_raw_string_hashes)]
        let statement = r#"SELECT 'Wayne\'s World'"#;
        let res1 = Parser::parse_sql(&MySqlDialect {}, statement);
        let res2 = Parser::parse_sql(&WrappedDialect(MySqlDialect {}), statement);
        assert!(res1.is_ok());
        assert_eq!(res1, res2);
    }
}

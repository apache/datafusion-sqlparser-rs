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
use crate::ast::{ColumnOption, Expr, GranteesType, Ident, ObjectNamePart, Statement};
pub use crate::keywords;
use crate::keywords::Keyword;
use crate::parser::{Parser, ParserError};
use crate::tokenizer::Token;

#[cfg(not(feature = "std"))]
use alloc::boxed::Box;

/// Convenience check if a [`Parser`] uses a certain dialect.
///
/// Note: when possible please the new style, adding a method to the [`Dialect`]
/// trait rather than using this macro.
///
/// The benefits of adding a method on `Dialect` over this macro are:
/// 1. user defined [`Dialect`]s can customize the parsing behavior
/// 2. The differences between dialects can be clearly documented in the trait
///
/// `dialect_of!(parser is SQLiteDialect |  GenericDialect)` evaluates
/// to `true` if `parser.dialect` is one of the [`Dialect`]s specified.
macro_rules! dialect_of {
    ( $parsed_dialect: ident is $($dialect_type: ty)|+ ) => {
        ($($parsed_dialect.dialect.is::<$dialect_type>())||+)
    };
}

// Similar to above, but for applying directly against an instance of dialect
// instead of a struct member named dialect. This avoids lifetime issues when
// mixing match guards and token references.
macro_rules! dialect_is {
    ($dialect:ident is $($dialect_type:ty)|+) => {
        ($($dialect.is::<$dialect_type>())||+)
    }
}

/// Encapsulates the differences between SQL implementations.
///
/// # SQL Dialects
///
/// SQL implementations deviate from one another, either due to
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

    /// Determine if a character starts a potential nested quoted identifier.
    /// Example: RedShift supports the following quote styles to all mean the same thing:
    /// ```sql
    /// SELECT 1 AS foo;
    /// SELECT 1 AS "foo";
    /// SELECT 1 AS [foo];
    /// SELECT 1 AS ["foo"];
    /// ```
    fn is_nested_delimited_identifier_start(&self, _ch: char) -> bool {
        false
    }

    /// Only applicable whenever [`Self::is_nested_delimited_identifier_start`] returns true
    /// If the next sequence of tokens potentially represent a nested identifier, then this method
    /// returns a tuple containing the outer quote style, and if present, the inner (nested) quote style.
    ///
    /// Example (Redshift):
    /// ```text
    /// `["foo"]` => Some(`[`, Some(`"`))
    /// `[foo]` => Some(`[`, None)
    /// `[0]` => None
    /// `"foo"` => None
    /// ```
    fn peek_nested_delimited_identifier_quotes(
        &self,
        mut _chars: Peekable<Chars<'_>>,
    ) -> Option<(char, Option<char>)> {
        None
    }

    /// Return the character used to quote identifiers.
    fn identifier_quote_style(&self, _identifier: &str) -> Option<char> {
        None
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

    /// Determine whether the dialect strips the backslash when escaping LIKE wildcards (%, _).
    ///
    /// [MySQL] has a special case when escaping single quoted strings which leaves these unescaped
    /// so they can be used in LIKE patterns without double-escaping (as is necessary in other
    /// escaping dialects, such as [Snowflake]). Generally, special characters have escaping rules
    /// causing them to be replaced with a different byte sequences (e.g. `'\0'` becoming the zero
    /// byte), and the default if an escaped character does not have a specific escaping rule is to
    /// strip the backslash (e.g. there is no rule for `h`, so `'\h' = 'h'`). MySQL's special case
    /// for ignoring LIKE wildcard escapes is to *not* strip the backslash, so that `'\%' = '\\%'`.
    /// This applies to all string literals though, not just those used in LIKE patterns.
    ///
    /// ```text
    /// mysql> select '\_', hex('\\'), hex('_'), hex('\_');
    /// +----+-----------+----------+-----------+
    /// | \_ | hex('\\') | hex('_') | hex('\_') |
    /// +----+-----------+----------+-----------+
    /// | \_ | 5C        | 5F       | 5C5F      |
    /// +----+-----------+----------+-----------+
    /// 1 row in set (0.00 sec)
    /// ```
    ///
    /// [MySQL]: https://dev.mysql.com/doc/refman/8.4/en/string-literals.html
    /// [Snowflake]: https://docs.snowflake.com/en/sql-reference/functions/like#usage-notes
    fn ignores_wildcard_escapes(&self) -> bool {
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

    /// Returns true if the dialects supports `GROUP BY` modifiers prefixed by a `WITH` keyword.
    /// Example: `GROUP BY value WITH ROLLUP`.
    fn supports_group_by_with_modifier(&self) -> bool {
        false
    }

    /// Indicates whether the dialect supports left-associative join parsing
    /// by default when parentheses are omitted in nested joins.
    ///
    /// Most dialects (like MySQL or Postgres) assume **left-associative** precedence,
    /// so a query like:
    ///
    /// ```sql
    /// SELECT * FROM t1 NATURAL JOIN t5 INNER JOIN t0 ON ...
    /// ```
    /// is interpreted as:
    /// ```sql
    /// ((t1 NATURAL JOIN t5) INNER JOIN t0 ON ...)
    /// ```
    /// and internally represented as a **flat list** of joins.
    ///
    /// In contrast, some dialects (e.g. **Snowflake**) assume **right-associative**
    /// precedence and interpret the same query as:
    /// ```sql
    /// (t1 NATURAL JOIN (t5 INNER JOIN t0 ON ...))
    /// ```
    /// which results in a **nested join** structure in the AST.
    ///
    /// If this method returns `false`, the parser must build nested join trees
    /// even in the absence of parentheses to reflect the correct associativity
    fn supports_left_associative_joins_without_parens(&self) -> bool {
        true
    }

    /// Returns true if the dialect supports the `(+)` syntax for OUTER JOIN.
    fn supports_outer_join_operator(&self) -> bool {
        false
    }

    /// Returns true if the dialect supports CONNECT BY.
    fn supports_connect_by(&self) -> bool {
        false
    }

    /// Returns true if the dialect supports `EXECUTE IMMEDIATE` statements.
    fn supports_execute_immediate(&self) -> bool {
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

    /// Returns true if the dialect supports `BEGIN {DEFERRED | IMMEDIATE | EXCLUSIVE | TRY | CATCH} [TRANSACTION]` statements
    fn supports_start_transaction_modifier(&self) -> bool {
        false
    }

    /// Returns true if the dialect supports `END {TRY | CATCH}` statements
    fn supports_end_transaction_modifier(&self) -> bool {
        false
    }

    /// Returns true if the dialect supports named arguments of the form `FUN(a = '1', b = '2')`.
    fn supports_named_fn_args_with_eq_operator(&self) -> bool {
        false
    }

    /// Returns true if the dialect supports named arguments of the form `FUN(a : '1', b : '2')`.
    fn supports_named_fn_args_with_colon_operator(&self) -> bool {
        false
    }

    /// Returns true if the dialect supports named arguments of the form `FUN(a := '1', b := '2')`.
    fn supports_named_fn_args_with_assignment_operator(&self) -> bool {
        false
    }

    /// Returns true if the dialect supports named arguments of the form `FUN(a => '1', b => '2')`.
    fn supports_named_fn_args_with_rarrow_operator(&self) -> bool {
        true
    }

    /// Returns true if dialect supports argument name as arbitrary expression.
    /// e.g. `FUN(LOWER('a'):'1',  b:'2')`
    /// Such function arguments are represented in the AST by the `FunctionArg::ExprNamed` variant,
    /// otherwise use the `FunctionArg::Named` variant (compatible reason).
    fn supports_named_fn_args_with_expr_name(&self) -> bool {
        false
    }

    /// Returns true if the dialect supports identifiers starting with a numeric
    /// prefix such as tables named `59901_user_login`
    fn supports_numeric_prefix(&self) -> bool {
        false
    }

    /// Returns true if the dialect supports numbers containing underscores, e.g. `10_000_000`
    fn supports_numeric_literal_underscores(&self) -> bool {
        false
    }

    /// Returns true if the dialects supports specifying null treatment
    /// as part of a window function's parameter list as opposed
    /// to after the parameter list.
    ///
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

    /// Returns true if the dialect supports multiple `SET` statements
    /// in a single statement.
    ///
    /// ```sql
    /// SET variable = expression [, variable = expression];
    /// ```
    fn supports_comma_separated_set_assignments(&self) -> bool {
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

    /// Does the dialect support parsing `LIMIT 1, 2` as `LIMIT 2 OFFSET 1`?
    fn supports_limit_comma(&self) -> bool {
        false
    }

    // Does the Dialect support concatenating of string literal
    // Example: SELECT 'Hello ' "world" => SELECT 'Hello world'
    fn supports_concat_quoted_identifiers(&self) -> bool {
        false
    }

    /// Does the dialect support trailing commas in the projection list?
    fn supports_projection_trailing_commas(&self) -> bool {
        self.supports_trailing_commas()
    }

    /// Returns true if the dialect supports trailing commas in the `FROM` clause of a `SELECT` statement.
    /// Example: `SELECT 1 FROM T, U, LIMIT 1`
    fn supports_from_trailing_commas(&self) -> bool {
        false
    }

    /// Returns true if the dialect supports trailing commas in the
    /// column definitions list of a `CREATE` statement.
    /// Example: `CREATE TABLE T (x INT, y TEXT,)`
    fn supports_column_definition_trailing_commas(&self) -> bool {
        false
    }

    /// Returns true if the dialect supports double dot notation for object names
    ///
    /// Example
    /// ```sql
    /// SELECT * FROM db_name..table_name
    /// ```
    fn supports_object_name_double_dot_notation(&self) -> bool {
        false
    }

    /// Return true if the dialect supports the STRUCT literal
    ///
    /// Example
    /// ```sql
    /// SELECT STRUCT(1 as one, 'foo' as foo, false)
    /// ```
    fn supports_struct_literal(&self) -> bool {
        false
    }

    /// Return true if the dialect supports empty projections in SELECT statements
    ///
    /// Example
    /// ```sql
    /// SELECT from table_name
    /// ```
    fn supports_empty_projections(&self) -> bool {
        false
    }

    /// Return true if the dialect supports wildcard expansion on
    /// arbitrary expressions in projections.
    ///
    /// Example:
    /// ```sql
    /// SELECT STRUCT<STRING>('foo').* FROM T
    /// ```
    fn supports_select_expr_star(&self) -> bool {
        false
    }

    /// Return true if the dialect supports "FROM-first" selects.
    ///
    /// Example:
    /// ```sql
    /// FROM table
    /// SELECT *
    /// ```
    fn supports_from_first_select(&self) -> bool {
        false
    }

    /// Return true if the dialect supports pipe operator.
    ///
    /// Example:
    /// ```sql
    /// SELECT *
    /// FROM table
    /// |> limit 1
    /// ```
    ///
    /// See <https://cloud.google.com/bigquery/docs/pipe-syntax-guide#basic_syntax>
    fn supports_pipe_operator(&self) -> bool {
        false
    }

    /// Does the dialect support MySQL-style `'user'@'host'` grantee syntax?
    fn supports_user_host_grantee(&self) -> bool {
        false
    }

    /// Does the dialect support the `MATCH() AGAINST()` syntax?
    fn supports_match_against(&self) -> bool {
        false
    }

    /// Returns true if the dialect supports an exclude option
    /// following a wildcard in the projection section. For example:
    /// `SELECT * EXCLUDE col1 FROM tbl`.
    ///
    /// [Redshift](https://docs.aws.amazon.com/redshift/latest/dg/r_EXCLUDE_list.html)
    /// [Snowflake](https://docs.snowflake.com/en/sql-reference/sql/select)
    fn supports_select_wildcard_exclude(&self) -> bool {
        false
    }

    /// Returns true if the dialect supports an exclude option
    /// as the last item in the projection section, not necessarily
    /// after a wildcard. For example:
    /// `SELECT *, c1, c2 EXCLUDE c3 FROM tbl`
    ///
    /// [Redshift](https://docs.aws.amazon.com/redshift/latest/dg/r_EXCLUDE_list.html)
    fn supports_select_exclude(&self) -> bool {
        false
    }

    /// Return true if the dialect supports specifying multiple options
    /// in a `CREATE TABLE` statement for the structure of the new table. For example:
    /// `CREATE TABLE t (a INT, b INT) AS SELECT 1 AS b, 2 AS a`
    fn supports_create_table_multi_schema_info_sources(&self) -> bool {
        false
    }

    /// Dialect-specific infix parser override
    ///
    /// This method is called to parse the next infix expression.
    ///
    /// If `None` is returned, falls back to the default behavior.
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
    ///
    /// This method is called to get the precedence of the next token.
    ///
    /// If `None` is returned, falls back to the default behavior.
    fn get_next_precedence(&self, _parser: &Parser) -> Option<Result<u8, ParserError>> {
        // return None to fall back to the default behavior
        None
    }

    /// Get the precedence of the next token, looking at the full token stream.
    ///
    /// A higher number => higher precedence
    ///
    /// See [`Self::get_next_precedence`] to override the behavior for just the
    /// next token.
    ///
    /// The default implementation is used for many dialects, but can be
    /// overridden to provide dialect-specific behavior.
    fn get_next_precedence_default(&self, parser: &Parser) -> Result<u8, ParserError> {
        if let Some(precedence) = self.get_next_precedence(parser) {
            return precedence;
        }
        macro_rules! p {
            ($precedence:ident) => {
                self.prec_value(Precedence::$precedence)
            };
        }

        let token = parser.peek_token();
        debug!("get_next_precedence_full() {token:?}");
        match token.token {
            Token::Word(w) if w.keyword == Keyword::OR => Ok(p!(Or)),
            Token::Word(w) if w.keyword == Keyword::AND => Ok(p!(And)),
            Token::Word(w) if w.keyword == Keyword::XOR => Ok(p!(Xor)),

            Token::Word(w) if w.keyword == Keyword::AT => {
                match (
                    parser.peek_nth_token(1).token,
                    parser.peek_nth_token(2).token,
                ) {
                    (Token::Word(w), Token::Word(w2))
                        if w.keyword == Keyword::TIME && w2.keyword == Keyword::ZONE =>
                    {
                        Ok(p!(AtTz))
                    }
                    _ => Ok(self.prec_unknown()),
                }
            }

            Token::Word(w) if w.keyword == Keyword::NOT => match parser.peek_nth_token(1).token {
                // The precedence of NOT varies depending on keyword that
                // follows it. If it is followed by IN, BETWEEN, or LIKE,
                // it takes on the precedence of those tokens. Otherwise, it
                // is not an infix operator, and therefore has zero
                // precedence.
                Token::Word(w) if w.keyword == Keyword::IN => Ok(p!(Between)),
                Token::Word(w) if w.keyword == Keyword::BETWEEN => Ok(p!(Between)),
                Token::Word(w) if w.keyword == Keyword::LIKE => Ok(p!(Like)),
                Token::Word(w) if w.keyword == Keyword::ILIKE => Ok(p!(Like)),
                Token::Word(w) if w.keyword == Keyword::RLIKE => Ok(p!(Like)),
                Token::Word(w) if w.keyword == Keyword::REGEXP => Ok(p!(Like)),
                Token::Word(w) if w.keyword == Keyword::MATCH => Ok(p!(Like)),
                Token::Word(w) if w.keyword == Keyword::SIMILAR => Ok(p!(Like)),
                Token::Word(w) if w.keyword == Keyword::MEMBER => Ok(p!(Like)),
                Token::Word(w)
                    if w.keyword == Keyword::NULL && !parser.in_column_definition_state() =>
                {
                    Ok(p!(Is))
                }
                _ => Ok(self.prec_unknown()),
            },
            Token::Word(w) if w.keyword == Keyword::NOTNULL && self.supports_notnull_operator() => {
                Ok(p!(Is))
            }
            Token::Word(w) if w.keyword == Keyword::IS => Ok(p!(Is)),
            Token::Word(w) if w.keyword == Keyword::IN => Ok(p!(Between)),
            Token::Word(w) if w.keyword == Keyword::BETWEEN => Ok(p!(Between)),
            Token::Word(w) if w.keyword == Keyword::OVERLAPS => Ok(p!(Between)),
            Token::Word(w) if w.keyword == Keyword::LIKE => Ok(p!(Like)),
            Token::Word(w) if w.keyword == Keyword::ILIKE => Ok(p!(Like)),
            Token::Word(w) if w.keyword == Keyword::RLIKE => Ok(p!(Like)),
            Token::Word(w) if w.keyword == Keyword::REGEXP => Ok(p!(Like)),
            Token::Word(w) if w.keyword == Keyword::MATCH => Ok(p!(Like)),
            Token::Word(w) if w.keyword == Keyword::SIMILAR => Ok(p!(Like)),
            Token::Word(w) if w.keyword == Keyword::MEMBER => Ok(p!(Like)),
            Token::Word(w) if w.keyword == Keyword::OPERATOR => Ok(p!(Between)),
            Token::Word(w) if w.keyword == Keyword::DIV => Ok(p!(MulDivModOp)),
            Token::Period => Ok(p!(Period)),
            Token::Assignment
            | Token::Eq
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
            | Token::Spaceship => Ok(p!(Eq)),
            Token::Pipe
            | Token::QuestionMarkDash
            | Token::DoubleSharp
            | Token::Overlap
            | Token::AmpersandLeftAngleBracket
            | Token::AmpersandRightAngleBracket
            | Token::QuestionMarkDashVerticalBar
            | Token::AmpersandLeftAngleBracketVerticalBar
            | Token::VerticalBarAmpersandRightAngleBracket
            | Token::TwoWayArrow
            | Token::LeftAngleBracketCaret
            | Token::RightAngleBracketCaret
            | Token::QuestionMarkSharp
            | Token::QuestionMarkDoubleVerticalBar
            | Token::QuestionPipe
            | Token::TildeEqual
            | Token::AtSign
            | Token::ShiftLeftVerticalBar
            | Token::VerticalBarShiftRight => Ok(p!(Pipe)),
            Token::Caret | Token::Sharp | Token::ShiftRight | Token::ShiftLeft => Ok(p!(Caret)),
            Token::Ampersand => Ok(p!(Ampersand)),
            Token::Plus | Token::Minus => Ok(p!(PlusMinus)),
            Token::Mul | Token::Div | Token::DuckIntDiv | Token::Mod | Token::StringConcat => {
                Ok(p!(MulDivModOp))
            }
            Token::DoubleColon | Token::ExclamationMark | Token::LBracket | Token::CaretAt => {
                Ok(p!(DoubleColon))
            }
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
            | Token::CustomBinaryOperator(_) => Ok(p!(PgOther)),
            _ => Ok(self.prec_unknown()),
        }
    }

    /// Dialect-specific statement parser override
    ///
    /// This method is called to parse the next statement.
    ///
    /// If `None` is returned, falls back to the default behavior.
    fn parse_statement(&self, _parser: &mut Parser) -> Option<Result<Statement, ParserError>> {
        // return None to fall back to the default behavior
        None
    }

    /// Dialect-specific column option parser override
    ///
    /// This method is called to parse the next column option.
    ///
    /// If `None` is returned, falls back to the default behavior.
    fn parse_column_option(
        &self,
        _parser: &mut Parser,
    ) -> Result<Option<Result<Option<ColumnOption>, ParserError>>, ParserError> {
        // return None to fall back to the default behavior
        Ok(None)
    }

    /// Decide the lexical Precedence of operators.
    ///
    /// Uses (APPROXIMATELY) <https://www.postgresql.org/docs/7.0/operators.htm#AEN2026> as a reference
    fn prec_value(&self, prec: Precedence) -> u8 {
        match prec {
            Precedence::Period => 100,
            Precedence::DoubleColon => 50,
            Precedence::AtTz => 41,
            Precedence::MulDivModOp => 40,
            Precedence::PlusMinus => 30,
            Precedence::Xor => 24,
            Precedence::Ampersand => 23,
            Precedence::Caret => 22,
            Precedence::Pipe => 21,
            Precedence::Between => 20,
            Precedence::Eq => 20,
            Precedence::Like => 19,
            Precedence::Is => 17,
            Precedence::PgOther => 16,
            Precedence::UnaryNot => 15,
            Precedence::And => 10,
            Precedence::Or => 5,
        }
    }

    /// Returns the precedence when the precedence is otherwise unknown
    fn prec_unknown(&self) -> u8 {
        0
    }

    /// Returns true if this dialect requires the `TABLE` keyword after `DESCRIBE`
    ///
    /// Defaults to false.
    ///
    /// If true, the following statement is valid: `DESCRIBE TABLE my_table`
    /// If false, the following statements are valid: `DESCRIBE my_table` and `DESCRIBE table`
    fn describe_requires_table_keyword(&self) -> bool {
        false
    }

    /// Returns true if this dialect allows the `EXTRACT` function to words other than [`Keyword`].
    fn allow_extract_custom(&self) -> bool {
        false
    }

    /// Returns true if this dialect allows the `EXTRACT` function to use single quotes in the part being extracted.
    fn allow_extract_single_quotes(&self) -> bool {
        false
    }

    /// Returns true if this dialect allows dollar placeholders
    /// e.g. `SELECT $var` (SQLite)
    fn supports_dollar_placeholder(&self) -> bool {
        false
    }

    /// Does the dialect support with clause in create index statement?
    /// e.g. `CREATE INDEX idx ON t WITH (key = value, key2)`
    fn supports_create_index_with_clause(&self) -> bool {
        false
    }

    /// Whether `INTERVAL` expressions require units (called "qualifiers" in the ANSI SQL spec) to be specified,
    /// e.g. `INTERVAL 1 DAY` vs `INTERVAL 1`.
    ///
    /// Expressions within intervals (e.g. `INTERVAL '1' + '1' DAY`) are only allowed when units are required.
    ///
    /// See <https://github.com/sqlparser-rs/sqlparser-rs/pull/1398> for more information.
    ///
    /// When `true`:
    /// * `INTERVAL '1' DAY` is VALID
    /// * `INTERVAL 1 + 1 DAY` is VALID
    /// * `INTERVAL '1' + '1' DAY` is VALID
    /// * `INTERVAL '1'` is INVALID
    ///
    /// When `false`:
    /// * `INTERVAL '1'` is VALID
    /// * `INTERVAL '1' DAY` is VALID — unit is not required, but still allowed
    /// * `INTERVAL 1 + 1 DAY` is INVALID
    fn require_interval_qualifier(&self) -> bool {
        false
    }

    fn supports_explain_with_utility_options(&self) -> bool {
        false
    }

    fn supports_asc_desc_in_column_definition(&self) -> bool {
        false
    }

    /// Returns true if the dialect supports `a!` expressions
    fn supports_factorial_operator(&self) -> bool {
        false
    }

    /// Returns true if the dialect supports nested comments
    /// e.g. `/* /* nested */ */`
    fn supports_nested_comments(&self) -> bool {
        false
    }

    /// Returns true if this dialect supports treating the equals operator `=` within a `SelectItem`
    /// as an alias assignment operator, rather than a boolean expression.
    /// For example: the following statements are equivalent for such a dialect:
    /// ```sql
    ///  SELECT col_alias = col FROM tbl;
    ///  SELECT col_alias AS col FROM tbl;
    /// ```
    fn supports_eq_alias_assignment(&self) -> bool {
        false
    }

    /// Returns true if this dialect supports the `TRY_CONVERT` function
    fn supports_try_convert(&self) -> bool {
        false
    }

    /// Returns true if the dialect supports `!a` syntax for boolean `NOT` expressions.
    fn supports_bang_not_operator(&self) -> bool {
        false
    }

    /// Returns true if the dialect supports the `LISTEN`, `UNLISTEN` and `NOTIFY` statements
    fn supports_listen_notify(&self) -> bool {
        false
    }

    /// Returns true if the dialect supports the `LOAD DATA` statement
    fn supports_load_data(&self) -> bool {
        false
    }

    /// Returns true if the dialect supports the `LOAD extension` statement
    fn supports_load_extension(&self) -> bool {
        false
    }

    /// Returns true if this dialect expects the `TOP` option
    /// before the `ALL`/`DISTINCT` options in a `SELECT` statement.
    fn supports_top_before_distinct(&self) -> bool {
        false
    }

    /// Returns true if the dialect supports boolean literals (`true` and `false`).
    /// For example, in MSSQL these are treated as identifiers rather than boolean literals.
    fn supports_boolean_literals(&self) -> bool {
        true
    }

    /// Returns true if this dialect supports the `LIKE 'pattern'` option in
    /// a `SHOW` statement before the `IN` option
    fn supports_show_like_before_in(&self) -> bool {
        false
    }

    /// Returns true if this dialect supports the `COMMENT` statement
    fn supports_comment_on(&self) -> bool {
        false
    }

    /// Returns true if the dialect supports the `CREATE TABLE SELECT` statement
    fn supports_create_table_select(&self) -> bool {
        false
    }

    /// Returns true if the dialect supports PartiQL for querying semi-structured data
    /// <https://partiql.org/index.html>
    fn supports_partiql(&self) -> bool {
        false
    }

    /// Returns true if the specified keyword is reserved and cannot be
    /// used as an identifier without special handling like quoting.
    fn is_reserved_for_identifier(&self, kw: Keyword) -> bool {
        keywords::RESERVED_FOR_IDENTIFIER.contains(&kw)
    }

    /// Returns reserved keywords that may prefix a select item expression
    /// e.g. `SELECT CONNECT_BY_ROOT name FROM Tbl2` (Snowflake)
    fn get_reserved_keywords_for_select_item_operator(&self) -> &[Keyword] {
        &[]
    }

    /// Returns grantee types that should be treated as identifiers
    fn get_reserved_grantees_types(&self) -> &[GranteesType] {
        &[]
    }

    /// Returns true if this dialect supports the `TABLESAMPLE` option
    /// before the table alias option. For example:
    ///
    /// Table sample before alias: `SELECT * FROM tbl AS t TABLESAMPLE (10)`
    /// Table sample after alias: `SELECT * FROM tbl TABLESAMPLE (10) AS t`
    ///
    /// <https://jakewheat.github.io/sql-overview/sql-2016-foundation-grammar.html#_7_6_table_reference>
    fn supports_table_sample_before_alias(&self) -> bool {
        false
    }

    /// Returns true if this dialect supports the `INSERT INTO ... SET col1 = 1, ...` syntax.
    ///
    /// MySQL: <https://dev.mysql.com/doc/refman/8.4/en/insert.html>
    fn supports_insert_set(&self) -> bool {
        false
    }

    /// Does the dialect support table function in insertion?
    fn supports_insert_table_function(&self) -> bool {
        false
    }

    /// Does the dialect support insert formats, e.g. `INSERT INTO ... FORMAT <format>`
    fn supports_insert_format(&self) -> bool {
        false
    }

    /// Returns true if this dialect supports `SET` statements without an explicit
    /// assignment operator such as `=`. For example: `SET SHOWPLAN_XML ON`.
    fn supports_set_stmt_without_operator(&self) -> bool {
        false
    }

    /// Returns true if the specified keyword should be parsed as a column identifier.
    /// See [keywords::RESERVED_FOR_COLUMN_ALIAS]
    fn is_column_alias(&self, kw: &Keyword, _parser: &mut Parser) -> bool {
        !keywords::RESERVED_FOR_COLUMN_ALIAS.contains(kw)
    }

    /// Returns true if the specified keyword should be parsed as a select item alias.
    /// When explicit is true, the keyword is preceded by an `AS` word. Parser is provided
    /// to enable looking ahead if needed.
    fn is_select_item_alias(&self, explicit: bool, kw: &Keyword, parser: &mut Parser) -> bool {
        explicit || self.is_column_alias(kw, parser)
    }

    /// Returns true if the specified keyword should be parsed as a table factor identifier.
    /// See [keywords::RESERVED_FOR_TABLE_FACTOR]
    fn is_table_factor(&self, kw: &Keyword, _parser: &mut Parser) -> bool {
        !keywords::RESERVED_FOR_TABLE_FACTOR.contains(kw)
    }

    /// Returns true if the specified keyword should be parsed as a table factor alias.
    /// See [keywords::RESERVED_FOR_TABLE_ALIAS]
    fn is_table_alias(&self, kw: &Keyword, _parser: &mut Parser) -> bool {
        !keywords::RESERVED_FOR_TABLE_ALIAS.contains(kw)
    }

    /// Returns true if the specified keyword should be parsed as a table factor alias.
    /// When explicit is true, the keyword is preceded by an `AS` word. Parser is provided
    /// to enable looking ahead if needed.
    fn is_table_factor_alias(&self, explicit: bool, kw: &Keyword, parser: &mut Parser) -> bool {
        explicit || self.is_table_alias(kw, parser)
    }

    /// Returns true if this dialect supports querying historical table data
    /// by specifying which version of the data to query.
    fn supports_timestamp_versioning(&self) -> bool {
        false
    }

    /// Returns true if this dialect supports the E'...' syntax for string literals
    ///
    /// Postgres: <https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-STRINGS-ESCAPE>
    fn supports_string_escape_constant(&self) -> bool {
        false
    }

    /// Returns true if the dialect supports the table hints in the `FROM` clause.
    fn supports_table_hints(&self) -> bool {
        false
    }

    /// Returns true if this dialect requires a whitespace character after `--` to start a single line comment.
    ///
    /// MySQL: <https://dev.mysql.com/doc/refman/8.4/en/ansi-diff-comments.html>
    /// e.g. UPDATE account SET balance=balance--1
    //       WHERE account_id=5752             ^^^ will be interpreted as two minus signs instead of a comment
    fn requires_single_line_comment_whitespace(&self) -> bool {
        false
    }

    /// Returns true if the dialect supports array type definition with brackets with
    /// an optional size. For example:
    /// ```CREATE TABLE my_table (arr1 INT[], arr2 INT[3])```
    /// ```SELECT x::INT[]```
    fn supports_array_typedef_with_brackets(&self) -> bool {
        false
    }
    /// Returns true if the dialect supports geometric types.
    ///
    /// Postgres: <https://www.postgresql.org/docs/9.5/functions-geometry.html>
    /// e.g. @@ circle '((0,0),10)'
    fn supports_geometric_types(&self) -> bool {
        false
    }

    /// Returns true if the dialect supports `ORDER BY ALL`.
    /// `ALL` which means all columns of the SELECT clause.
    ///
    /// For example: ```SELECT * FROM addresses ORDER BY ALL;```.
    fn supports_order_by_all(&self) -> bool {
        false
    }

    /// Returns true if the dialect supports `SET NAMES <charset_name> [COLLATE <collation_name>]`.
    ///
    /// - [MySQL](https://dev.mysql.com/doc/refman/8.4/en/set-names.html)
    /// - [PostgreSQL](https://www.postgresql.org/docs/17/sql-set.html)
    ///
    /// Note: Postgres doesn't support the `COLLATE` clause, but we permissively parse it anyway.
    fn supports_set_names(&self) -> bool {
        false
    }

    fn supports_space_separated_column_options(&self) -> bool {
        false
    }

    /// Returns true if the dialect supports the `USING` clause in an `ALTER COLUMN` statement.
    /// Example:
    ///  ```sql
    ///  ALTER TABLE tbl ALTER COLUMN col SET DATA TYPE <type> USING <exp>`
    /// ```
    fn supports_alter_column_type_using(&self) -> bool {
        false
    }

    /// Returns true if the dialect supports `ALTER TABLE tbl DROP COLUMN c1, ..., cn`
    fn supports_comma_separated_drop_column_list(&self) -> bool {
        false
    }

    /// Returns true if the dialect considers the specified ident as a function
    /// that returns an identifier. Typically used to generate identifiers
    /// programmatically.
    ///
    /// - [Snowflake](https://docs.snowflake.com/en/sql-reference/identifier-literal)
    fn is_identifier_generating_function_name(
        &self,
        _ident: &Ident,
        _name_parts: &[ObjectNamePart],
    ) -> bool {
        false
    }

    /// Returns true if the dialect supports the `x NOTNULL`
    /// operator expression.
    fn supports_notnull_operator(&self) -> bool {
        false
    }

    /// Returns true if this dialect allows an optional `SIGNED` suffix after integer data types.
    ///
    /// Example:
    /// ```sql
    /// CREATE TABLE t (i INT(20) SIGNED);
    /// ```
    ///
    /// Note that this is canonicalized to `INT(20)`.
    fn supports_data_type_signed_suffix(&self) -> bool {
        false
    }

    /// Returns true if the dialect supports the `INTERVAL` data type with [Postgres]-style options.
    ///
    /// Examples:
    /// ```sql
    /// CREATE TABLE t (i INTERVAL YEAR TO MONTH);
    /// SELECT '1 second'::INTERVAL HOUR TO SECOND(3);
    /// ```
    ///
    /// See [`crate::ast::DataType::Interval`] and [`crate::ast::IntervalFields`].
    ///
    /// [Postgres]: https://www.postgresql.org/docs/17/datatype-datetime.html
    fn supports_interval_options(&self) -> bool {
        false
    }

    /// Returns true if the dialect supports specifying which table to copy
    /// the schema from inside parenthesis.
    ///
    /// Not parenthesized:
    /// '''sql
    /// CREATE TABLE new LIKE old ...
    /// '''
    /// [Snowflake](https://docs.snowflake.com/en/sql-reference/sql/create-table#label-create-table-like)
    /// [BigQuery](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#create_table_like)
    ///
    /// Parenthesized:
    /// '''sql
    /// CREATE TABLE new (LIKE old ...)
    /// '''
    /// [Redshift](https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_TABLE_NEW.html)
    fn supports_create_table_like_parenthesized(&self) -> bool {
        false
    }

    /// Returns true if the dialect supports `SEMANTIC_VIEW()` table functions.
    ///
    /// ```sql
    /// SELECT * FROM SEMANTIC_VIEW(
    ///     model_name
    ///     DIMENSIONS customer.name, customer.region
    ///     METRICS orders.revenue, orders.count
    ///     WHERE customer.active = true
    /// )
    /// ```
    fn supports_semantic_view_table_factor(&self) -> bool {
        false
    }
}

/// This represents the operators for which precedence must be defined
///
/// higher number -> higher precedence
#[derive(Debug, Clone, Copy)]
pub enum Precedence {
    Period,
    DoubleColon,
    AtTz,
    MulDivModOp,
    PlusMinus,
    Xor,
    Ampersand,
    Caret,
    Pipe,
    Between,
    Eq,
    Like,
    Is,
    PgOther,
    UnaryNot,
    And,
    Or,
}

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
        "databricks" => Some(Box::new(DatabricksDialect {})),
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
        assert!(parse_dialect("DataBricks").is::<DatabricksDialect>());
        assert!(parse_dialect("databricks").is::<DatabricksDialect>());

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

            fn is_nested_delimited_identifier_start(&self, ch: char) -> bool {
                self.0.is_nested_delimited_identifier_start(ch)
            }

            fn peek_nested_delimited_identifier_quotes(
                &self,
                chars: std::iter::Peekable<std::str::Chars<'_>>,
            ) -> Option<(char, Option<char>)> {
                self.0.peek_nested_delimited_identifier_quotes(chars)
            }

            fn identifier_quote_style(&self, identifier: &str) -> Option<char> {
                self.0.identifier_quote_style(identifier)
            }

            fn supports_string_literal_backslash_escape(&self) -> bool {
                self.0.supports_string_literal_backslash_escape()
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

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
mod duckdb;
mod generic;
mod hive;
mod mssql;
mod mysql;
mod postgresql;
mod redshift;
mod snowflake;
mod sqlite;

use crate::ast::{Expr, Statement};
use core::any::{Any, TypeId};
use core::fmt::Debug;
use core::iter::Peekable;
use core::str::Chars;

pub use self::ansi::AnsiDialect;
pub use self::bigquery::BigQueryDialect;
pub use self::clickhouse::ClickHouseDialect;
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
use crate::parser::{Parser, ParserError};

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
    /// Determine if a character starts a quoted identifier. The default
    /// implementation, accepting "double quoted" ids is both ANSI-compliant
    /// and appropriate for most dialects (with the notable exception of
    /// MySQL, MS SQL, and sqlite). You can accept one of characters listed
    /// in `Word::matching_end_quote` here
    fn is_delimited_identifier_start(&self, ch: char) -> bool {
        ch == '"'
    }
    /// Determine if quoted characters are proper for identifier
    fn is_proper_identifier_inside_quotes(&self, mut _chars: Peekable<Chars<'_>>) -> bool {
        true
    }
    /// Determine if a character is a valid start character for an unquoted identifier
    fn is_identifier_start(&self, ch: char) -> bool;
    /// Determine if a character is a valid unquoted identifier character
    fn is_identifier_part(&self, ch: char) -> bool;
    /// Does the dialect support `FILTER (WHERE expr)` for aggregate queries?
    fn supports_filter_during_aggregation(&self) -> bool {
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
    /// Dialect-specific prefix parser override
    fn parse_prefix(&self, _parser: &mut Parser) -> Option<Result<Expr, ParserError>> {
        // return None to fall back to the default behavior
        None
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
    /// Dialect-specific statement parser override
    fn parse_statement(&self, _parser: &mut Parser) -> Option<Result<Statement, ParserError>> {
        // return None to fall back to the default behavior
        None
    }
}

impl dyn Dialect {
    #[inline]
    pub fn is<T: Dialect>(&self) -> bool {
        // borrowed from `Any` implementation
        TypeId::of::<T>() == self.type_id()
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
    use super::ansi::AnsiDialect;
    use super::generic::GenericDialect;
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
}

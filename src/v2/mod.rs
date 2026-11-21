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

//! Parsing with the [`sqlparser_v2`] parser, returning this crate's AST.
//!
//! The v2 parser is strict: each dialect accepts only its database's syntax.
//! There is no v2 equivalent of [`GenericDialect`] or [`AnsiDialect`], and
//! custom dialects cannot be used here.
//!
//! The conversion does not yet cover the whole v2 AST. SQL that v2 parses
//! into a node with no conversion fails with [`Error::Unconverted`].
//!
//! [`GenericDialect`]: crate::dialect::GenericDialect
//! [`AnsiDialect`]: crate::dialect::AnsiDialect

mod convert;

use core::fmt;

use sqlparser_v2::dialect::DialectKind;

use crate::ast::Statement;
use crate::dialect::{
    BigQueryDialect, ClickHouseDialect, DatabricksDialect, Dialect, DuckDbDialect, HiveDialect,
    MsSqlDialect, MySqlDialect, OracleDialect, PostgreSqlDialect, RedshiftSqlDialect,
    SQLiteDialect, SnowflakeDialect, SparkSqlDialect, TeradataDialect,
};
use crate::parser::{Parser, ParserError};

/// Why the v2 parser produced no AST.
#[derive(Debug, Clone, PartialEq)]
pub enum Error {
    /// The dialect has no v2 equivalent.
    UnsupportedDialect,
    /// The v2 parser rejected the SQL.
    Parse(ParserError),
    /// The v2 parser accepted the SQL, but part of its AST has no conversion yet.
    Unconverted(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::UnsupportedDialect => f.write_str("dialect is not supported by the v2 parser"),
            Error::Parse(error) => error.fmt(f),
            Error::Unconverted(what) => write!(f, "no conversion from the v2 AST for {what}"),
        }
    }
}

impl std::error::Error for Error {}

/// The v2 dialect matching `dialect`, if there is one.
pub fn dialect_kind(dialect: &dyn Dialect) -> Option<DialectKind> {
    macro_rules! kinds {
        ($($dialect:ty => $kind:ident),*) => {
            $(if dialect.is::<$dialect>() {
                return Some(DialectKind::$kind);
            })*
        };
    }
    kinds!(
        PostgreSqlDialect => Postgres,
        MySqlDialect => MySql,
        RedshiftSqlDialect => Redshift,
        SnowflakeDialect => Snowflake,
        SparkSqlDialect => Spark,
        DatabricksDialect => Databricks,
        HiveDialect => Hive,
        BigQueryDialect => BigQuery,
        ClickHouseDialect => ClickHouse,
        DuckDbDialect => DuckDb,
        MsSqlDialect => MsSql,
        OracleDialect => Oracle,
        SQLiteDialect => Sqlite,
        TeradataDialect => Teradata
    );
    None
}

/// Parses `sql` with the v2 parser and converts the result to this crate's AST.
pub fn parse_sql(dialect: &dyn Dialect, sql: &str) -> Result<Vec<Statement>, Error> {
    let kind = dialect_kind(dialect).ok_or(Error::UnsupportedDialect)?;
    let statements = sqlparser_v2::dialect::builtin::parse_sql_as(kind, sql)
        .map_err(|error| Error::Parse(convert::parse_error(sql, &error)))?;
    convert::Converter::new(dialect, sql).statements(&statements)
}

/// Like [`parse_sql`], but parses with this crate's parser if the v2 parser
/// gives no result.
pub fn parse_sql_with_fallback(
    dialect: &dyn Dialect,
    sql: &str,
) -> Result<Vec<Statement>, ParserError> {
    parse_sql(dialect, sql).or_else(|_| Parser::parse_sql(dialect, sql))
}

/// The results of both parsers on the same SQL.
#[derive(Debug, Clone, PartialEq)]
pub struct Comparison {
    /// This crate's parser.
    pub v1: Result<Vec<Statement>, ParserError>,
    /// The v2 parser, converted.
    pub v2: Result<Vec<Statement>, Error>,
}

impl Comparison {
    /// True if both parsers produced the same AST, ignoring spans.
    pub fn same_ast(&self) -> bool {
        matches!((&self.v1, &self.v2), (Ok(v1), Ok(v2)) if v1 == v2)
    }
}

/// Parses `sql` with both parsers, to find where they disagree.
pub fn compare(dialect: &dyn Dialect, sql: &str) -> Comparison {
    Comparison {
        v1: Parser::parse_sql(dialect, sql),
        v2: parse_sql(dialect, sql),
    }
}

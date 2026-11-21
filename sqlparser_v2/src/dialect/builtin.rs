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

//! The AST shared by all built-in dialects, and choosing one at runtime.

use std::borrow::Cow;
use std::fmt;

use crate::ast::{Expr, Query, Statement};
use crate::dialect::bigquery::{BigQueryExpr, BigQueryStatement, BigQueryTableFactor};
use crate::dialect::clickhouse::{
    ClickHouseExpr, ClickHouseOrderByItem, ClickHouseQueryClauses, ClickHouseStatement,
};
use crate::dialect::databricks::DatabricksStatement;
use crate::dialect::duckdb::{DuckDbExpr, DuckDbStatement};
use crate::dialect::hive::HiveStatement;
use crate::dialect::mssql::{
    MsSqlExpr, MsSqlForClause, MsSqlStatement, MsSqlTableFactor, MsSqlTop,
};
use crate::dialect::mysql::{MySqlExpr, MySqlStatement, MySqlTableFactor};
use crate::dialect::oracle::{self, OptimizerHint, OracleExpr, OracleStatement};
use crate::dialect::postgres::{
    PostgresExpr, PostgresStatement, PostgresTableFactor, PostgresUsingConstraint,
};
use crate::dialect::redshift::{RedshiftExpr, RedshiftStatement, RedshiftTableFactor};
use crate::dialect::snowflake::{SnowflakeExpr, SnowflakeStatement};
use crate::dialect::spark::SparkStatement;
use crate::dialect::sqlite::{SqliteExpr, SqliteIndexedTable, SqliteStatement};
use crate::dialect::teradata::{TeradataExpr, TeradataStatement, TeradataTop};
use crate::dialect::{
    AstExt, BigQuery, ClickHouse, Core, Databricks, DuckDb, Hive, MsSql, MySql, Oracle, Postgres,
    Redshift, Snowflake, Spark, Sqlite, Teradata,
};
use crate::{PResult, parse_query, parse_sql};

/// The extensions used by every built-in dialect, so that they all produce
/// the same AST type.
#[derive(Debug, Clone, PartialEq)]
pub struct Builtin;

impl AstExt for Builtin {
    type TableFactor<'a> = BuiltinTableFactor<'a>;
    type JoinConstraint<'a> = BuiltinJoinConstraint<'a>;
    type QueryTail<'a> = BuiltinQueryTail<'a>;
    type Expr<'a> = BuiltinExpr<'a>;
    type Select<'a> = BuiltinSelect<'a>;
    type Statement<'a> = BuiltinStatement<'a>;
    type Insert<'a> = BuiltinInsert<'a>;
    type OrderByItem<'a> = ClickHouseOrderByItem<'a>;

    fn modifiers_follow_quantifier(modifiers: &BuiltinSelect<'_>) -> bool {
        modifiers.mysql != MySqlSelect::default()
            || modifiers.mssql_top.is_some()
            || modifiers.bigquery_as.is_some()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum BuiltinExpr<'a> {
    MySql(MySqlExpr<'a, Builtin>),
    Snowflake(SnowflakeExpr<'a>),
    Teradata(TeradataExpr<'a>),
    Oracle(OracleExpr<'a>),
    Sqlite(SqliteExpr<'a>),
    MsSql(MsSqlExpr<'a>),
    BigQuery(BigQueryExpr<'a>),
    ClickHouse(ClickHouseExpr<'a>),
    DuckDb(DuckDbExpr<'a>),
    Postgres(PostgresExpr<'a>),
    Redshift(RedshiftExpr<'a>),
}

/// Join conditions that only some built-in dialects have.
#[derive(Debug, Clone, PartialEq)]
pub enum BuiltinJoinConstraint<'a> {
    Postgres(PostgresUsingConstraint<'a>),
}

impl fmt::Display for BuiltinJoinConstraint<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Postgres(constraint) => constraint.fmt(f),
        }
    }
}

/// Table factors that only some built-in dialects have.
#[derive(Debug, Clone, PartialEq)]
pub enum BuiltinTableFactor<'a> {
    MySql(Box<MySqlTableFactor<'a>>),
    Sqlite(Box<SqliteIndexedTable<'a>>),
    MsSql(MsSqlTableFactor<'a>),
    BigQuery(BigQueryTableFactor<'a>),
    Postgres(Box<PostgresTableFactor<'a>>),
    Redshift(Box<RedshiftTableFactor<'a>>),
}

impl fmt::Display for BuiltinTableFactor<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MySql(factor) => factor.fmt(f),
            Self::Sqlite(table) => table.fmt(f),
            Self::MsSql(factor) => factor.fmt(f),
            Self::BigQuery(factor) => factor.fmt(f),
            Self::Postgres(factor) => factor.fmt(f),
            Self::Redshift(factor) => factor.fmt(f),
        }
    }
}

impl<'a> From<MySqlExpr<'a, Builtin>> for BuiltinExpr<'a> {
    fn from(expr: MySqlExpr<'a, Builtin>) -> Self {
        Self::MySql(expr)
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct BuiltinSelect<'a> {
    /// The number in `SELECT TOP n`.
    pub top: Option<Cow<'a, str>>,
    /// The expressions in PostgreSQL's `SELECT DISTINCT ON (...)`.
    pub distinct_on: Option<Vec<Expr<'a, Builtin>>>,
    /// Teradata's `TOP` when it is more than a plain number.
    pub teradata_top: Option<Box<TeradataTop<'a>>>,
    pub mysql: MySqlSelect,
    /// Optimizer hints written as comments after `SELECT`.
    pub hints: Vec<OptimizerHint<'a>>,
    /// `TOP` in Microsoft SQL Server, after any `DISTINCT`.
    pub mssql_top: Option<Box<MsSqlTop<'a>>>,
    /// BigQuery's `AS STRUCT` or `AS VALUE`, holding the word after `AS`.
    pub bigquery_as: Option<Cow<'a, str>>,
}

/// MySQL's keywords between `SELECT` and the select list.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct MySqlSelect {
    pub high_priority: bool,
    pub straight_join: bool,
    pub small_result: bool,
    pub big_result: bool,
    pub buffer_result: bool,
    pub no_cache: bool,
    pub calc_found_rows: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum BuiltinStatement<'a> {
    MySql(MySqlStatement<'a>),
    Snowflake(SnowflakeStatement<'a>),
    Postgres(PostgresStatement<'a>),
    Redshift(RedshiftStatement<'a>),
    Spark(SparkStatement<'a>),
    Databricks(DatabricksStatement<'a>),
    Hive(HiveStatement<'a>),
    BigQuery(BigQueryStatement<'a>),
    ClickHouse(ClickHouseStatement<'a>),
    DuckDb(DuckDbStatement<'a>),
    MsSql(MsSqlStatement<'a>),
    Oracle(OracleStatement<'a>),
    Sqlite(SqliteStatement<'a>),
    Teradata(TeradataStatement<'a>),
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct BuiltinInsert<'a> {
    /// MySQL's `INSERT IGNORE`.
    pub ignore: bool,
    /// Snowflake's `INSERT OVERWRITE INTO`.
    pub overwrite: bool,
    /// MySQL's `LOW_PRIORITY`, `DELAYED` or `HIGH_PRIORITY`.
    pub priority: Option<Cow<'a, str>>,
}

/// Clauses at the end of a query that only a built-in dialect has.
///
/// One pointer, to keep every query small.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct BuiltinQueryTail<'a>(pub Option<Box<BuiltinQueryClauses<'a>>>);

#[derive(Debug, Clone, PartialEq)]
pub enum BuiltinQueryClauses<'a> {
    /// Microsoft SQL Server's `FOR XML`, `FOR JSON` or `FOR BROWSE`.
    MsSql(MsSqlForClause<'a>),
    /// ClickHouse's `SETTINGS` and `FORMAT`.
    ClickHouse(ClickHouseQueryClauses<'a>),
}

impl fmt::Display for BuiltinQueryTail<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0.as_deref() {
            None => Ok(()),
            Some(BuiltinQueryClauses::MsSql(clause)) => write!(f, " {clause}"),
            Some(BuiltinQueryClauses::ClickHouse(clauses)) => clauses.fmt(f),
        }
    }
}

/// A built-in dialect chosen at runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DialectKind {
    Core,
    MySql,
    Postgres,
    Redshift,
    Snowflake,
    Spark,
    Databricks,
    Hive,
    BigQuery,
    ClickHouse,
    DuckDb,
    MsSql,
    Oracle,
    Sqlite,
    Teradata,
}

impl DialectKind {
    /// Finds a dialect by name, ignoring case.
    pub fn from_name(name: &str) -> Option<Self> {
        const NAMES: [(&str, DialectKind); 18] = [
            ("core", DialectKind::Core),
            ("mysql", DialectKind::MySql),
            ("postgres", DialectKind::Postgres),
            ("postgresql", DialectKind::Postgres),
            ("redshift", DialectKind::Redshift),
            ("snowflake", DialectKind::Snowflake),
            ("spark", DialectKind::Spark),
            ("databricks", DialectKind::Databricks),
            ("hive", DialectKind::Hive),
            ("bigquery", DialectKind::BigQuery),
            ("clickhouse", DialectKind::ClickHouse),
            ("duckdb", DialectKind::DuckDb),
            ("mssql", DialectKind::MsSql),
            ("sqlserver", DialectKind::MsSql),
            ("oracle", DialectKind::Oracle),
            ("sqlite", DialectKind::Sqlite),
            ("teradata", DialectKind::Teradata),
            ("tsql", DialectKind::MsSql),
        ];
        NAMES
            .iter()
            .find(|(candidate, _)| candidate.eq_ignore_ascii_case(name))
            .map(|&(_, kind)| kind)
    }
}

/// Calls a generic function with the dialect type that `kind` names.
macro_rules! dispatch {
    ($kind:expr, $function:ident($sql:expr)) => {
        match $kind {
            DialectKind::Core => $function::<Core>($sql),
            DialectKind::MySql => $function::<MySql>($sql),
            DialectKind::Postgres => $function::<Postgres>($sql),
            DialectKind::Redshift => $function::<Redshift>($sql),
            DialectKind::Snowflake => $function::<Snowflake>($sql),
            DialectKind::Spark => $function::<Spark>($sql),
            DialectKind::Databricks => $function::<Databricks>($sql),
            DialectKind::Hive => $function::<Hive>($sql),
            DialectKind::BigQuery => $function::<BigQuery>($sql),
            DialectKind::ClickHouse => $function::<ClickHouse>($sql),
            DialectKind::DuckDb => $function::<DuckDb>($sql),
            DialectKind::MsSql => $function::<MsSql>($sql),
            DialectKind::Oracle => $function::<Oracle>($sql),
            DialectKind::Sqlite => $function::<Sqlite>($sql),
            DialectKind::Teradata => $function::<Teradata>($sql),
        }
    };
}

/// Parses statements using a dialect chosen at runtime.
///
/// The choice is made once here. Parsing itself is statically dispatched.
pub fn parse_sql_as(kind: DialectKind, sql: &str) -> PResult<Vec<Statement<'_, Builtin>>> {
    dispatch!(kind, parse_sql(sql))
}

/// Parses a single query using a dialect chosen at runtime.
pub fn parse_query_as(kind: DialectKind, sql: &str) -> PResult<Query<'_, Builtin>> {
    dispatch!(kind, parse_query(sql))
}

impl fmt::Display for BuiltinStatement<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MySql(statement) => statement.fmt(f),
            Self::Snowflake(statement) => statement.fmt(f),
            Self::Postgres(statement) => statement.fmt(f),
            Self::Redshift(statement) => statement.fmt(f),
            Self::Spark(statement) => statement.fmt(f),
            Self::Databricks(statement) => statement.fmt(f),
            Self::Hive(statement) => statement.fmt(f),
            Self::BigQuery(statement) => statement.fmt(f),
            Self::ClickHouse(statement) => statement.fmt(f),
            Self::DuckDb(statement) => statement.fmt(f),
            Self::MsSql(statement) => statement.fmt(f),
            Self::Oracle(statement) => statement.fmt(f),
            Self::Sqlite(statement) => statement.fmt(f),
            Self::Teradata(statement) => statement.fmt(f),
        }
    }
}

impl fmt::Display for BuiltinExpr<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BuiltinExpr::MySql(expr) => expr.fmt(f),
            BuiltinExpr::Snowflake(expr) => expr.fmt(f),
            BuiltinExpr::Teradata(expr) => expr.fmt(f),
            BuiltinExpr::Oracle(expr) => expr.fmt(f),
            BuiltinExpr::Sqlite(expr) => expr.fmt(f),
            BuiltinExpr::MsSql(expr) => expr.fmt(f),
            BuiltinExpr::BigQuery(expr) => expr.fmt(f),
            BuiltinExpr::ClickHouse(expr) => expr.fmt(f),
            BuiltinExpr::DuckDb(expr) => expr.fmt(f),
            BuiltinExpr::Postgres(expr) => expr.fmt(f),
            BuiltinExpr::Redshift(expr) => expr.fmt(f),
        }
    }
}

impl fmt::Display for BuiltinSelect<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        oracle::write_hints(f, &self.hints)?;
        if let Some(ref top) = self.top {
            write!(f, "TOP {top} ")?;
        }
        if let Some(top) = &self.teradata_top {
            write!(f, "{top} ")?;
        }
        if let Some(exprs) = &self.distinct_on {
            f.write_str("DISTINCT ON (")?;
            for (i, expr) in exprs.iter().enumerate() {
                if i > 0 {
                    f.write_str(", ")?;
                }
                write!(f, "{expr}")?;
            }
            f.write_str(") ")?;
        }
        if let Some(top) = &self.mssql_top {
            write!(f, "{top} ")?;
        }
        if let Some(ref word) = self.bigquery_as {
            write!(f, "AS {word} ")?;
        }
        let flags = [
            (self.mysql.high_priority, "HIGH_PRIORITY "),
            (self.mysql.straight_join, "STRAIGHT_JOIN "),
            (self.mysql.small_result, "SQL_SMALL_RESULT "),
            (self.mysql.big_result, "SQL_BIG_RESULT "),
            (self.mysql.buffer_result, "SQL_BUFFER_RESULT "),
            (self.mysql.no_cache, "SQL_NO_CACHE "),
            (self.mysql.calc_found_rows, "SQL_CALC_FOUND_ROWS "),
        ];
        for (set, keyword) in flags {
            if set {
                f.write_str(keyword)?;
            }
        }
        Ok(())
    }
}

impl fmt::Display for BuiltinInsert<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(ref priority) = self.priority {
            write!(f, "{priority} ")?;
        }
        if self.ignore {
            f.write_str("IGNORE ")?;
        }
        if self.overwrite {
            f.write_str("OVERWRITE ")?;
        }
        Ok(())
    }
}

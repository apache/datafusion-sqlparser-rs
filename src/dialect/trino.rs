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

use crate::dialect::Dialect;

/// A [`Dialect`] for [Trino](https://trino.io/docs/current/language.html),
/// the distributed SQL engine that descends from Presto.
///
/// Trino follows the SQL standard closely: identifiers are delimited with
/// double quotes only, aggregates take `FILTER (WHERE ...)`, higher-order
/// functions take `x -> ...` lambdas, and row pattern recognition, table
/// sampling, table versioning (time travel) and parenthesized `EXPLAIN`
/// options are all part of the grammar.
///
/// See <https://trino.io/docs/current/sql.html>.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct TrinoDialect;

impl Dialect for TrinoDialect {
    /// Trino delimits identifiers with double quotes only; backquotes are a
    /// syntax error, the most common mistake of users arriving from
    /// MySQL, ClickHouse or Spark.
    ///
    /// See <https://trino.io/docs/current/language/reserved.html>
    fn is_delimited_identifier_start(&self, ch: char) -> bool {
        ch == '"'
    }

    fn is_identifier_start(&self, ch: char) -> bool {
        ch.is_ascii_alphabetic() || ch == '_'
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        ch.is_ascii_alphanumeric() || ch == '_'
    }

    /// See <https://trino.io/docs/current/sql/select.html#filter>
    fn supports_filter_during_aggregation(&self) -> bool {
        true
    }

    /// `GROUP BY` accepts arbitrary expressions, `GROUPING SETS`, `CUBE`
    /// and `ROLLUP`.
    ///
    /// See <https://trino.io/docs/current/sql/select.html#group-by-clause>
    fn supports_group_by_expr(&self) -> bool {
        true
    }

    /// See <https://trino.io/docs/current/functions/lambda.html>
    fn supports_lambda_functions(&self) -> bool {
        true
    }

    /// See <https://trino.io/docs/current/sql/match-recognize.html>
    fn supports_match_recognize(&self) -> bool {
        true
    }

    /// Trino reads Iceberg and Delta tables at a point in time with
    /// `FOR TIMESTAMP AS OF` and `FOR VERSION AS OF`.
    ///
    /// See <https://trino.io/docs/current/connector/iceberg.html#time-travel-queries>
    fn supports_table_versioning(&self) -> bool {
        true
    }

    /// `EXPLAIN (TYPE IO, FORMAT JSON) ...` and friends.
    ///
    /// See <https://trino.io/docs/current/sql/explain.html>
    fn supports_explain_with_utility_options(&self) -> bool {
        true
    }

    /// Named arguments to table functions use `=>`, e.g.
    /// `TABLE(sequence(start => 1, stop => 10))`.
    ///
    /// See <https://trino.io/docs/current/functions/table.html>
    fn supports_named_fn_args_with_rarrow_operator(&self) -> bool {
        true
    }

    /// See <https://trino.io/docs/current/sql/comment.html>
    fn supports_comment_on(&self) -> bool {
        true
    }

    /// `SHOW TABLES FROM schema LIKE '%x%'`: the source comes before the
    /// pattern.
    ///
    /// See <https://trino.io/docs/current/sql/show-tables.html>
    fn supports_show_like_before_in(&self) -> bool {
        false
    }
}

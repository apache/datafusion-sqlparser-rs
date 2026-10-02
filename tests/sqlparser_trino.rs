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

#![warn(clippy::all)]
//! Test SQL syntax specific to Trino.

use sqlparser::ast::*;
use sqlparser::dialect::TrinoDialect;
use sqlparser::parser::ParserError;
use test_utils::*;

#[macro_use]
mod test_utils;

fn trino() -> TestedDialects {
    TestedDialects::new(vec![Box::new(TrinoDialect {})])
}

// --------------------------------
// Identifiers
// --------------------------------

#[test]
fn double_quoted_identifiers() {
    let select = trino().verified_only_select(r#"SELECT "order_id" FROM iceberg."demo"."orders""#);
    match &select.projection[0] {
        SelectItem::UnnamedExpr(Expr::Identifier(ident)) => {
            assert_eq!(ident.value, "order_id");
            assert_eq!(ident.quote_style, Some('"'));
        }
        other => panic!("expected a quoted identifier, got {other:?}"),
    }
}

#[test]
fn backquoted_identifiers_are_rejected() {
    // Trino has no backquote quoting; the parser must not accept it either,
    // or a MySQL/ClickHouse habit would be reported far from its cause.
    let err = trino()
        .parse_sql_statements("SELECT * FROM `demo`.`orders`")
        .unwrap_err();
    assert!(matches!(
        err,
        ParserError::ParserError(_) | ParserError::TokenizerError(_)
    ));
}

#[test]
fn hidden_columns_are_quoted_identifiers() {
    trino().verified_stmt(r#"SELECT "$path" FROM iceberg.demo.orders"#);
}

// --------------------------------
// Aggregates, grouping and lambdas
// --------------------------------

#[test]
fn filter_during_aggregation() {
    trino().verified_stmt("SELECT count(*) FILTER (WHERE status = 'DELIVERED') FROM orders");
}

#[test]
fn approx_percentile_and_array_agg_with_order() {
    trino().verified_stmt("SELECT approx_percentile(total, 0.5) FROM orders");
    trino().verified_stmt("SELECT array_agg(status ORDER BY status) FROM orders");
}

#[test]
fn grouping_sets_cube_rollup() {
    trino().verified_stmt(
        "SELECT status, region, count(*) FROM orders GROUP BY GROUPING SETS ((status), (region), ())",
    );
    trino().verified_stmt("SELECT status, count(*) FROM orders GROUP BY ROLLUP (status)");
    trino().verified_stmt("SELECT status, count(*) FROM orders GROUP BY CUBE (status, region)");
}

#[test]
fn lambda_functions() {
    trino().verified_stmt("SELECT filter(ARRAY[1, 2, 3], x -> x > 1)");
    trino().verified_stmt("SELECT transform(ARRAY[1, 2], x -> x * 2)");
    trino().verified_stmt("SELECT reduce(ARRAY[1, 2, 3], 0, (s, x) -> s + x, s -> s)");
}

// --------------------------------
// Relations
// --------------------------------

#[test]
fn unnest_with_ordinality() {
    trino().verified_stmt("SELECT t.x, t.i FROM UNNEST(ARRAY[10, 20]) WITH ORDINALITY AS t (x, i)");
}

#[test]
fn cross_join_unnest() {
    trino().verified_stmt("SELECT o.id, e FROM orders AS o CROSS JOIN UNNEST(o.items) AS t (e)");
}

#[test]
fn values_with_column_aliases() {
    trino().verified_stmt("SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS t (id, name)");
}

#[test]
fn tablesample() {
    trino().verified_stmt("SELECT count(*) FROM orders TABLESAMPLE BERNOULLI (50)");
    trino().verified_stmt("SELECT count(*) FROM orders AS o TABLESAMPLE SYSTEM (10)");
}

#[test]
fn table_functions_with_named_arguments() {
    trino().verified_stmt("SELECT * FROM TABLE(sequence(start => 1, stop => 10))");
}

// --------------------------------
// Expressions and literals
// --------------------------------

#[test]
fn typed_literals() {
    trino().verified_stmt("SELECT DATE '2026-01-01'");
    trino().verified_stmt("SELECT TIMESTAMP '2026-01-01 00:00:00 UTC'");
    trino().verified_stmt("SELECT DECIMAL '1.5'");
}

#[test]
fn interval_literal() {
    trino().verified_stmt(
        "SELECT count(*) FROM orders WHERE order_date > current_date - INTERVAL '365' DAY",
    );
}

#[test]
fn cast_and_try_cast() {
    trino().verified_stmt("SELECT CAST(order_id AS VARCHAR) FROM orders");
    trino().verified_stmt("SELECT TRY_CAST(order_id AS VARCHAR) FROM orders");
}

#[test]
fn at_time_zone() {
    trino().verified_stmt("SELECT created_at AT TIME ZONE 'UTC' FROM orders");
}

#[test]
fn is_distinct_from() {
    trino().verified_stmt("SELECT count(*) FROM orders WHERE status IS DISTINCT FROM 'x'");
}

#[test]
fn json_path_functions() {
    trino().verified_stmt(r#"SELECT JSON_VALUE('{"a":1}', 'lax $.a')"#);
    trino().verified_stmt(r#"SELECT JSON_QUERY('{"a":[1,2]}', 'lax $.a')"#);
}

#[test]
fn listagg_within_group() {
    trino().verified_stmt("SELECT LISTAGG(status, ',') WITHIN GROUP (ORDER BY status) FROM orders");
}

#[test]
fn map_subscript() {
    trino().verified_stmt("SELECT MAP(ARRAY['k'], ARRAY['v'])['k']");
}

#[test]
fn like_with_escape() {
    trino().verified_stmt(r"SELECT count(*) FROM orders WHERE status LIKE 'a\_%' ESCAPE '\'");
}

// --------------------------------
// Query shape
// --------------------------------

#[test]
fn fetch_first_rows_only() {
    trino().verified_stmt("SELECT order_id FROM orders ORDER BY order_id FETCH FIRST 2 ROWS ONLY");
}

#[test]
fn with_cte() {
    trino().verified_stmt(
        "WITH x AS (SELECT status FROM orders) SELECT status, count(*) FROM x GROUP BY status",
    );
}

#[test]
fn match_recognize() {
    trino().verified_stmt(
        "SELECT * FROM orders MATCH_RECOGNIZE(PARTITION BY customer_id ORDER BY order_date MEASURES A.order_date AS start_date ONE ROW PER MATCH PATTERN (A B+) DEFINE B AS B.total > PREV(B.total))",
    );
}

// --------------------------------
// Statements around queries
// --------------------------------

#[test]
fn explain_with_options() {
    trino().verified_stmt("EXPLAIN (TYPE IO, FORMAT JSON) SELECT * FROM orders");
    trino().verified_stmt("EXPLAIN (TYPE VALIDATE) SELECT * FROM orders");
}

#[test]
fn show_and_describe() {
    trino().verified_stmt("SHOW SCHEMAS FROM iceberg");
    trino().verified_stmt("SHOW TABLES FROM demo");
    trino().verified_stmt("SHOW COLUMNS FROM demo.orders");
    trino().verified_stmt("DESCRIBE demo.orders");
}

#[test]
fn comment_on() {
    trino().verified_stmt("COMMENT ON TABLE demo.orders IS 'orders'");
    trino().verified_stmt("COMMENT ON COLUMN demo.orders.status IS 'lifecycle'");
}

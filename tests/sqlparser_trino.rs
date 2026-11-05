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

// --------------------------------
// Agreement with Trino
// --------------------------------
//
// Every statement below was given to Trino 483 as `EXPLAIN (TYPE VALIDATE) …`
// on 2026-10-07. A `SYNTAX_ERROR` answer is a reject; any other answer (a
// plan, or a semantic error such as a missing table) means Trino parsed it.
// The dialect must agree on each.

const TRINO_PARSES: &[&str] = &[
    r#"SELECT * FROM orders MATCH_RECOGNIZE(PATTERN (A) DEFINE A AS true) AS m"#,
    r#"SELECT "order_id" FROM iceberg."demo"."orders""#,
    r#"SELECT "$path" FROM iceberg.demo.orders"#,
    r#"SELECT count(*) FILTER (WHERE status = 'DELIVERED') FROM orders"#,
    r#"SELECT approx_percentile(total, 0.5) FROM orders"#,
    r#"SELECT array_agg(status ORDER BY status) FROM orders"#,
    r#"SELECT status, region, count(*) FROM orders GROUP BY GROUPING SETS ((status), (region), ())"#,
    r#"SELECT status, count(*) FROM orders GROUP BY ROLLUP (status)"#,
    r#"SELECT status, count(*) FROM orders GROUP BY CUBE (status, region)"#,
    r#"SELECT filter(ARRAY[1, 2, 3], x -> x > 1)"#,
    r#"SELECT transform(ARRAY[1, 2], x -> x * 2)"#,
    r#"SELECT reduce(ARRAY[1, 2, 3], 0, (s, x) -> s + x, s -> s)"#,
    r#"SELECT t.x, t.i FROM UNNEST(ARRAY[10, 20]) WITH ORDINALITY AS t (x, i)"#,
    r#"SELECT o.id, e FROM orders AS o CROSS JOIN UNNEST(o.items) AS t (e)"#,
    r#"SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS t (id, name)"#,
    r#"SELECT count(*) FROM orders TABLESAMPLE BERNOULLI (50)"#,
    r#"SELECT count(*) FROM orders AS o TABLESAMPLE SYSTEM (10)"#,
    r#"SELECT * FROM TABLE(sequence(start => 1, stop => 10))"#,
    r#"SELECT DATE '2026-01-01'"#,
    r#"SELECT TIMESTAMP '2026-01-01 00:00:00 UTC'"#,
    r#"SELECT DECIMAL '1.5'"#,
    r#"SELECT count(*) FROM orders WHERE order_date > current_date - INTERVAL '365' DAY"#,
    r#"SELECT CAST(order_id AS VARCHAR) FROM orders"#,
    r#"SELECT TRY_CAST(order_id AS VARCHAR) FROM orders"#,
    r#"SELECT created_at AT TIME ZONE 'UTC' FROM orders"#,
    r#"SELECT count(*) FROM orders WHERE status IS DISTINCT FROM 'x'"#,
    r#"SELECT JSON_VALUE('{"a":1}', 'lax $.a')"#,
    r#"SELECT JSON_QUERY('{"a":[1,2]}', 'lax $.a')"#,
    r#"SELECT LISTAGG(status, ',') WITHIN GROUP (ORDER BY status) FROM orders"#,
    r#"SELECT MAP(ARRAY['k'], ARRAY['v'])['k']"#,
    r#"SELECT order_id FROM orders ORDER BY order_id FETCH FIRST 2 ROWS ONLY"#,
    r#"WITH x AS (SELECT status FROM orders) SELECT status, count(*) FROM x GROUP BY status"#,
    r#"SELECT * FROM orders MATCH_RECOGNIZE(PARTITION BY customer_id ORDER BY order_date MEASURES A.order_date AS start_date ONE ROW PER MATCH PATTERN (A B+) DEFINE B AS B.total > PREV(B.total))"#,
    r#"EXPLAIN (TYPE IO, FORMAT JSON) SELECT * FROM orders"#,
    r#"SHOW SCHEMAS FROM iceberg"#,
    r#"SHOW TABLES FROM demo"#,
    r#"SHOW COLUMNS FROM demo.orders"#,
    r#"DESCRIBE demo.orders"#,
    r#"COMMENT ON TABLE demo.orders IS 'orders'"#,
    r#"COMMENT ON COLUMN demo.orders.status IS 'lifecycle'"#,
    r#"SELECT status, count(*) FROM orders GROUP BY ALL"#,
    r#"SELECT transform(items, x -> x + 1) FROM orders"#,
    r#"SELECT transform(items, x => x + 1) FROM orders"#,
    r#"EXPLAIN ANALYZE SELECT * FROM orders"#,
    r#"SELECT json_extract(payload, path => '$.a') FROM orders"#,
    r#"SELECT json_extract(payload => '$.a') FROM orders"#,
    r#"SELECT substr(name, 1, 3) FROM orders"#,
    r#"COMMENT ON TABLE orders IS 'customer orders'"#,
    r#"COMMENT ON COLUMN orders.status IS 'state'"#,
    r#"COMMENT ON VIEW v IS 'x'"#,
    r#"SHOW TABLES IN demo LIKE 'ord%'"#,
    r#"SHOW TABLES FROM demo LIKE 'ord%'"#,
    r#"SELECT * FROM orders OFFSET 5 LIMIT 10"#,
    r#"SELECT 'a' || 'b'"#,
    r#"SELECT "a" FROM orders"#,
    r#"SELECT CAST(x AS INT64) FROM orders"#,
    r#"DELETE FROM orders WHERE 1 = 1"#,
    r#"INSERT INTO orders VALUES (1, 2)"#,
    r#"MERGE INTO orders t USING updates u ON t.id = u.id WHEN MATCHED THEN UPDATE SET status = u.status"#,
    r#"SELECT * FROM orders QUALIFY"#,
    r#"SELECT * FROM orders LATERAL"#,
    r#"SELECT * FROM orders TOP"#,
    r#"SELECT * FROM orders VIEW"#,
    r#"SELECT * FROM orders PIVOT"#,
    r#"SELECT * FROM orders SETTINGS"#,
    r#"SELECT * FROM orders FORMAT"#,
    r#"SELECT * FROM orders SAMPLE"#,
    r#"SELECT * FROM orders OPEN"#,
    r#"SELECT 1 EXCLUDE FROM orders"#,
    r#"SELECT 1 TOP FROM orders"#,
    r#"SELECT 1 VIEW FROM orders"#,
    r#"SELECT 1 RETURNING FROM orders"#,
];

const TRINO_REJECTS: &[&str] = &[
    r#"COMMENT ON TABLE orders 'x'"#,
    r#"COMMENT ON orders IS 'x'"#,
    r#"SELECT count(*) FILTER (status = 'x') FROM orders"#,
    r#"SELECT status, count(*) FROM orders GROUP BY GROUPING SETS status"#,
    r#"SELECT transform(items, x -> ) FROM orders"#,
    r#"EXPLAIN (TYPE IO FORMAT JSON) SELECT 1"#,
    r#"EXPLAIN () SELECT 1"#,
    r#"SELECT * FROM `demo`.`orders`"#,
    r#"SELECT count(*) FILTER status = 'DELIVERED' FROM orders"#,
    r#"SELECT transform(items, (x) x + 1) FROM orders"#,
    r#"SELECT * FROM orders MATCH_RECOGNIZE(PARTITION BY customer_id ORDER BY order_date MEASURES A.id AS a_id) AS m"#,
    r#"SELECT * FROM orders FOR SYSTEM_TIME AS OF TIMESTAMP '2026-01-01 00:00:00'"#,
    r#"SELECT * FROM orders AS o FOR VERSION AS OF 123"#,
    r#"SELECT * FROM orders TIMESTAMP AS OF '2026-01-01'"#,
    r#"SELECT * FROM orders VERSION AS OF 123"#,
    r#"SHOW TABLES LIKE 'ord%' IN demo"#,
    r#"SELECT * FROM orders USING SAMPLE 10"#,
    r#"SELECT * FROM TABLE(sequence(start => , stop => 10))"#,
    r#"SELECT json_extract(payload, => '$.a') FROM orders"#,
];

/// Trino rejects these too, but the parser accepts them for every dialect
/// and offers no hook to refuse them. Listed so the gap is stated, not
/// discovered; the assertion flips the day a hook exists.
const TRINO_REJECTS_PARSER_ACCEPTS: &[&str] = &[
    r#"SHOW TABLES demo"#,
    r#"COMMENT ON TABLE orders IS x"#,
    r#"EXPLAIN (TYPE BOGUS) SELECT * FROM orders"#,
    r#"EXPLAIN FORMAT JSON SELECT * FROM orders"#,
    r#"SELECT * FROM orders LIMIT 10 OFFSET 5"#,
    r#"SELECT TOP 10 * FROM orders"#,
    r#"SELECT * FROM orders WHERE status ILIKE 'd%'"#,
    r#"SELECT a:b FROM orders"#,
    r#"SELECT * FROM orders TABLESAMPLE BERNOULLI 50"#,
    r#"SELECT * FROM orders FOR UPDATE"#,
    r#"SELECT $path FROM orders"#,
];

/// Trino parses these and the parser rejects them for every dialect: a
/// keyword that may follow a table reference or select item is kept
/// reserved as an alias because the parser does not backtrack, Trino reads
/// any `identifier 'string'` as a typed literal, and `FINAL` / `RUNNING`
/// are admitted outside `MATCH_RECOGNIZE`. Listed so the gap is stated;
/// the assertion flips the day one is fixed.
const TRINO_PARSES_PARSER_REJECTS: &[&str] = &[
    r#"SELECT * FROM orders LIMIT"#,
    r#"SELECT * FROM orders OFFSET"#,
    r#"SELECT * FROM orders FETCH"#,
    r#"SELECT * FROM orders WINDOW"#,
    r#"SELECT * FROM orders SET"#,
    r#"SELECT count(*) FROM orders TABLESAMPLE TABLESAMPLE BERNOULLI (50)"#,
    r#"SELECT * FROM orders MATCH_RECOGNIZE"#,
    r#"SELECT 1 LIMIT FROM orders"#,
    r#"SELECT json_extract(payload '$.a') FROM orders"#,
    r#"SELECT ILIKE 'a%' "a" FROM orders"#,
    r#"SELECT FINAL transform(ARRAY[1, 2], x -> x * 2)"#,
];

#[test]
fn agrees_with_trino_on_what_parses() {
    for sql in TRINO_PARSES {
        assert!(
            trino().parse_sql_statements(sql).is_ok(),
            "Trino parses this and the dialect must too: {sql}"
        );
    }
}

#[test]
fn agrees_with_trino_on_what_does_not_parse() {
    for sql in TRINO_REJECTS {
        assert!(
            trino().parse_sql_statements(sql).is_err(),
            "Trino rejects this and the dialect must too: {sql}"
        );
    }
}

#[test]
fn known_leniencies_are_the_listed_ones() {
    for sql in TRINO_REJECTS_PARSER_ACCEPTS {
        assert!(
            trino().parse_sql_statements(sql).is_ok(),
            "no longer accepted; move it to TRINO_REJECTS: {sql}"
        );
    }
}

#[test]
fn known_misses_are_the_listed_ones() {
    for sql in TRINO_PARSES_PARSER_REJECTS {
        assert!(
            trino().parse_sql_statements(sql).is_err(),
            "now accepted; move it to TRINO_PARSES: {sql}"
        );
    }
}

/// Differential check against a file of verdicts a running Trino produced
/// (one JSON line per statement: `{"sql": …, "verdict": "accept"|"reject"}`).
/// Generated by a mutation fuzzer outside this repository; run with
/// `TRINO_VERDICTS=<file> cargo test --test sqlparser_trino -- --ignored`.
///
/// Fails on any statement Trino parses and the dialect rejects: that is
/// the dialect's to fix. Statements Trino rejects and the parser accepts
/// are counted and printed, not failed: the parser is lenient for every
/// dialect there, and a dialect has no hook to tighten most of them.
#[test]
#[ignore]
fn differential_verdicts_from_file() {
    let Ok(path) = std::env::var("TRINO_VERDICTS") else {
        eprintln!("TRINO_VERDICTS not set; nothing checked");
        return;
    };
    let text = std::fs::read_to_string(&path).expect("verdict file");
    let (mut checked, mut false_rejects, mut lenient) = (0usize, Vec::new(), Vec::new());
    for line in text.lines().filter(|l| !l.trim().is_empty()) {
        let (sql, trino_parses) = verdict_line(line);
        checked += 1;
        let dialect_parses = trino().parse_sql_statements(&sql).is_ok();
        match (trino_parses, dialect_parses) {
            (true, false) => false_rejects.push(sql),
            (false, true) => lenient.push(sql),
            _ => {}
        }
    }
    eprintln!(
        "{checked} statements checked: {} Trino parses and the dialect rejects; {} Trino rejects and the parser accepts",
        false_rejects.len(),
        lenient.len()
    );
    for sql in &lenient {
        eprintln!("  parser accepts, Trino rejects: {sql}");
    }
    for sql in &false_rejects {
        eprintln!("  TRINO PARSES, DIALECT REJECTS: {sql}");
    }
    assert!(
        false_rejects.is_empty(),
        "{} statements Trino parses are rejected by the dialect",
        false_rejects.len()
    );
}

/// The two fields this file needs, read without a JSON dependency: the
/// `sql` string (JSON escapes decoded) and whether the verdict is `accept`.
fn verdict_line(line: &str) -> (String, bool) {
    let after = |key: &str| -> Option<&str> {
        let i = line.find(key)? + key.len();
        Some(&line[i..])
    };
    let raw = after("\"sql\": \"")
        .or_else(|| after("\"sql\":\""))
        .expect("sql field");
    let mut sql = String::new();
    let mut chars = raw.chars();
    while let Some(c) = chars.next() {
        match c {
            '"' => break,
            '\\' => match chars.next() {
                Some('n') => sql.push('\n'),
                Some('t') => sql.push('\t'),
                Some('u') => {
                    let hex: String = chars.by_ref().take(4).collect();
                    if let Some(ch) = u32::from_str_radix(&hex, 16).ok().and_then(char::from_u32) {
                        sql.push(ch);
                    }
                }
                Some(other) => sql.push(other),
                None => break,
            },
            c => sql.push(c),
        }
    }
    let accept =
        line.contains("\"verdict\": \"accept\"") || line.contains("\"verdict\":\"accept\"");
    (sql, accept)
}

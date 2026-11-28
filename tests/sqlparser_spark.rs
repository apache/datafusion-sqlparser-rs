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
//! Test SQL syntax specific to Apache Spark SQL.

use sqlparser::ast::*;
use sqlparser::dialect::SparkSqlDialect;
use sqlparser::parser::{Parser, ParserError};
use sqlparser::tokenizer::{Location, Span};
use test_utils::*;

#[macro_use]
mod test_utils;

fn spark() -> TestedDialects {
    TestedDialects::new(vec![Box::new(SparkSqlDialect {})])
}

// --------------------------------
// CREATE TABLE USING
// --------------------------------

#[test]
fn test_create_table_using() {
    let stmt = spark().verified_stmt("CREATE TABLE t (i INT, s STRING) USING parquet");
    match stmt {
        Statement::CreateTable(ct) => {
            assert_eq!(ct.name.to_string(), "t");
            assert_eq!(ct.columns.len(), 2);
            assert_eq!(
                ct.hive_formats.unwrap().storage,
                Some(HiveIOFormat::Using {
                    format: Ident::new("parquet")
                })
            );
        }
        _ => panic!("Expected CreateTable"),
    }
}

#[test]
fn test_create_table_using_if_not_exists() {
    spark().verified_stmt("CREATE TABLE IF NOT EXISTS t (i INT) USING delta");
}

#[test]
fn test_create_table_using_with_location() {
    spark().verified_stmt("CREATE TABLE t (i INT) USING parquet LOCATION '/data/t'");
}

#[test]
fn test_create_table_multi_column() {
    spark().verified_stmt(
        "CREATE TABLE t (i INT, l BIGINT, f FLOAT, d DOUBLE, s STRING, b BOOLEAN) USING parquet",
    );
}

#[test]
fn test_create_table_long_type() {
    // LONG is an alias for BIGINT; round-trips as BIGINT
    spark().one_statement_parses_to(
        "CREATE TABLE t (id LONG, val LONG) USING parquet",
        "CREATE TABLE t (id BIGINT, val BIGINT) USING parquet",
    );
}

#[test]
fn test_create_table_array_type() {
    spark().verified_stmt("CREATE TABLE t (arr ARRAY<INT>) USING parquet");
}

#[test]
fn test_create_table_map_type() {
    // MAP<K, V> parses and stores as DataType::Map (which displays as Map(K, V))
    spark()
        .parse_sql_statements("CREATE TABLE t (m MAP<STRING, INT>) USING parquet")
        .unwrap();
}

#[test]
fn test_create_table_struct_type() {
    // STRUCT field definitions drop the colon separator on round-trip
    spark().one_statement_parses_to(
        "CREATE TABLE t (s STRUCT<name: STRING, age: INT, score: DOUBLE>) USING parquet",
        "CREATE TABLE t (s STRUCT<name STRING, age INT, score DOUBLE>) USING parquet",
    );
}

#[test]
fn test_create_table_nested_types() {
    // Nested types parse successfully
    spark()
        .parse_sql_statements(
            "CREATE TABLE t (arr ARRAY<STRUCT<name: STRING, value: INT>>) USING parquet",
        )
        .unwrap();
    spark()
        .parse_sql_statements("CREATE TABLE t (m MAP<STRING, INT>, arr ARRAY<INT>) USING parquet")
        .unwrap();
}

#[test]
fn test_create_table_decimal_type() {
    spark()
        .verified_stmt("CREATE TABLE t (grp STRING, d DECIMAL(10,2), flag BOOLEAN) USING parquet");
}

// --------------------------------
// INSERT INTO
// --------------------------------

#[test]
fn test_insert_values() {
    spark().verified_stmt(
        "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c'), (NULL, 'd'), (1, NULL), (NULL, NULL)",
    );
}

#[test]
fn test_insert_values_multiline() {
    // Multi-line whitespace is normalized to single-line on round-trip
    spark().one_statement_parses_to(
        "INSERT INTO t VALUES\n  (1, 10, 'a'),\n  (2, 20, 'a'),\n  (3, 30, 'b')",
        "INSERT INTO t VALUES (1, 10, 'a'), (2, 20, 'a'), (3, 30, 'b')",
    );
}

// --------------------------------
// Lambda expressions
// --------------------------------

#[test]
fn test_lambda_single_param() {
    spark().verified_stmt("SELECT filter(arr, x -> x > 2) FROM t");
}

#[test]
fn test_lambda_two_params() {
    spark().verified_stmt("SELECT filter(arr, (x, i) -> i > 0) FROM t");
}

#[test]
fn test_lambda_transform() {
    spark().verified_stmt("SELECT transform(arr, x -> x * 2) FROM t");
}

// --------------------------------
// DIV integer division
// --------------------------------

#[test]
fn test_div_operator() {
    spark().one_statement_parses_to("SELECT c1 div c2 FROM t", "SELECT c1 DIV c2 FROM t");
}

#[test]
fn test_div_literal() {
    spark().one_statement_parses_to("SELECT 10 div 3", "SELECT 10 DIV 3");
}

#[test]
fn test_div_precedence() {
    let div = |left: Expr, right: Expr| Expr::BinaryOp {
        left: Box::new(left),
        op: BinaryOperator::MyIntegerDivide,
        right: Box::new(right),
    };
    let num = |n: &str| Expr::value(number(n));

    // `DIV` shares the precedence of `*` and `/`, so `+` must end up at the root.
    assert_eq!(
        Expr::BinaryOp {
            left: Box::new(div(num("7"), num("2"))),
            op: BinaryOperator::Plus,
            right: Box::new(num("1")),
        },
        spark().verified_expr("7 DIV 2 + 1")
    );

    // Equal precedence resolves left-associatively.
    assert_eq!(
        Expr::BinaryOp {
            left: Box::new(div(num("9"), num("3"))),
            op: BinaryOperator::Multiply,
            right: Box::new(num("3")),
        },
        spark().verified_expr("9 DIV 3 * 3")
    );
}

// Spark is not part of all_dialects, so the between low bound comparison is pinned here.

#[test]
fn parse_comparison_in_between_low_bound() {
    match spark().verified_expr("a BETWEEN b > 1 AND c") {
        Expr::Between { low, .. } => assert!(
            matches!(
                &*low,
                Expr::BinaryOp {
                    op: BinaryOperator::Gt,
                    ..
                }
            ),
            "{low:?}"
        ),
        other => panic!("expected BETWEEN, got {other:?}"),
    }
}

// --------------------------------
// Struct support
// --------------------------------

#[test]
fn test_named_struct() {
    spark().verified_stmt("SELECT named_struct('x', a, 'y', b, 'z', c) FROM t");
}

#[test]
fn test_struct_function() {
    // Parses as a STRUCT literal; round-trips with uppercase STRUCT keyword
    spark().one_statement_parses_to(
        "SELECT struct(a, b, c) FROM t",
        "SELECT STRUCT(a, b, c) FROM t",
    );
}

// --------------------------------
// Aggregate FILTER
// --------------------------------

#[test]
fn test_aggregate_filter() {
    spark().verified_stmt(
        "SELECT COUNT(*) FILTER (WHERE i > 0), SUM(val) FILTER (WHERE val IS NOT NULL) FROM t",
    );
}

#[test]
fn test_aggregate_filter_with_group_by() {
    spark().verified_stmt(
        "SELECT grp, SUM(i) FILTER (WHERE flag = true) FROM t GROUP BY grp ORDER BY grp",
    );
}

// --------------------------------
// Window functions with IGNORE NULLS
// --------------------------------

#[test]
fn test_lag_ignore_nulls() {
    spark().verified_stmt("SELECT LAG(val) IGNORE NULLS OVER (ORDER BY id) AS lag_val FROM t");
}

#[test]
fn test_lead_ignore_nulls() {
    spark().verified_stmt(
        "SELECT LEAD(val) IGNORE NULLS OVER (PARTITION BY grp ORDER BY id) AS lead_val FROM t",
    );
}

#[test]
fn test_lag_with_offset_and_default() {
    spark().verified_stmt("SELECT LAG(val, 2, -1) OVER (ORDER BY id) AS lag_val FROM t");
}

// --------------------------------
// CASE WHEN
// --------------------------------

#[test]
fn test_case_when() {
    spark().verified_stmt(
        "SELECT CASE WHEN i = 1 THEN 'one' WHEN i = 2 THEN 'two' ELSE 'other' END FROM t",
    );
}

#[test]
fn test_case_value() {
    spark().verified_stmt("SELECT CASE i WHEN 1 THEN 'one' WHEN 2 THEN 'two' END FROM t");
}

// --------------------------------
// CAST expressions
// --------------------------------

#[test]
fn test_cast_basic_types() {
    // cast() lower-case round-trips as CAST() upper-case
    spark().one_statement_parses_to(
        "SELECT cast(i AS BIGINT), cast(i AS DOUBLE), cast(i AS STRING) FROM t",
        "SELECT CAST(i AS BIGINT), CAST(i AS DOUBLE), CAST(i AS STRING) FROM t",
    );
}

#[test]
fn test_cast_to_timestamp() {
    spark().one_statement_parses_to(
        "SELECT cast('2020-01-01' AS TIMESTAMP)",
        "SELECT CAST('2020-01-01' AS TIMESTAMP)",
    );
    spark().one_statement_parses_to(
        "SELECT cast('2020-01-01T12:34:56' AS TIMESTAMP)",
        "SELECT CAST('2020-01-01T12:34:56' AS TIMESTAMP)",
    );
}

#[test]
fn test_cast_special_float_values() {
    spark().one_statement_parses_to(
        "SELECT cast('NaN' AS FLOAT), cast('Infinity' AS DOUBLE)",
        "SELECT CAST('NaN' AS FLOAT), CAST('Infinity' AS DOUBLE)",
    );
}

// --------------------------------
// Aggregate functions
// --------------------------------

#[test]
fn test_count_aggregate() {
    spark().verified_stmt("SELECT count(*), count(i), count(s) FROM t");
    spark().verified_stmt("SELECT grp, count(*), count(i) FROM t GROUP BY grp ORDER BY grp");
}

#[test]
fn test_sum_avg() {
    spark().verified_stmt("SELECT avg(i), avg(l), avg(f), avg(d) FROM t");
}

#[test]
fn test_bit_aggregates() {
    spark().verified_stmt("SELECT bit_and(i), bit_or(i), bit_xor(i) FROM t");
}

// --------------------------------
// Arithmetic
// --------------------------------

#[test]
fn test_arithmetic_operators() {
    spark().verified_stmt("SELECT a + b, a - b, a * b, a / b, a % b FROM t");
}

#[test]
fn test_unary_negative() {
    spark().verified_stmt("SELECT negative(col1), -(col1) FROM t");
}

// --------------------------------
// String operations
// --------------------------------

#[test]
fn test_like_pattern() {
    spark().verified_stmt("SELECT s FROM t WHERE s LIKE 'foo%'");
}

#[test]
fn test_substring() {
    spark().one_statement_parses_to(
        "SELECT substring(s, 1, 3) FROM t",
        "SELECT SUBSTRING(s, 1, 3) FROM t",
    );
}

#[test]
fn test_pipe_operator() {
    spark().verified_stmt("SELECT * FROM t |> WHERE x > 1 |> SELECT x AS y |> ORDER BY y");
}

#[test]
fn parse_string_literal_backslash_escape() {
    let Statement::Query(query) =
        spark().one_statement_parses_to(r"SELECT 'a\tb\nc\'d\\e\%f\_g'", "")
    else {
        unreachable!()
    };
    let SetExpr::Select(select) = *query.body else {
        unreachable!()
    };
    assert_eq!(
        expr_from_projection(only(&select.projection)),
        &Expr::Value(
            Value::SingleQuotedString("a\tb\nc'd\\e\\%f\\_g".to_string()).with_empty_span()
        )
    );
}

#[test]
fn parse_negative_numeric_literal_cast() {
    for (literal, data_type) in [
        ("-128", DataType::TinyInt(None)),
        ("-32768", DataType::SmallInt(None)),
        ("-2147483648", DataType::Int(None)),
        ("-9223372036854775808", DataType::BigInt(None)),
    ] {
        let select = spark().verified_only_select(&format!("SELECT {literal}::{data_type}"));
        assert_eq!(
            expr_from_projection(only(&select.projection)),
            &Expr::Cast {
                kind: CastKind::DoubleColon,
                expr: Box::new(Expr::value(number(literal))),
                data_type,
                format: None,
            }
        );
    }
}

#[test]
fn parse_negative_numeric_literals() {
    for (sql, literal, long) in [
        ("-0", "-0", false),
        ("-1.25", "-1.25", false),
        ("-.5", "-.5", false),
        ("-1e-3", "-1e-3", false),
        ("-1L", "-1", true),
        ("- /* sign */ 128", "-128", false),
    ] {
        let expr = spark().run_parser_method(sql, |p| p.parse_expr()).unwrap();
        assert_eq!(
            expr,
            Expr::value(Value::Number(literal.parse().unwrap(), long))
        );
        let Expr::Value(value) = &expr else {
            unreachable!()
        };
        assert_eq!(
            value.span,
            Span::new(Location::new(1, 1), Location::new(1, sql.len() as u64 + 1))
        );
        assert_eq!(
            expr,
            spark()
                .run_parser_method(&expr.to_string(), |p| p.parse_expr())
                .unwrap()
        );
    }
}

#[test]
fn parse_negative_literal_preserves_operators() {
    for (sql, op, inner) in [
        (
            "-a::INT",
            UnaryOperator::Minus,
            Expr::Identifier(Ident::new("a")),
        ),
        (
            "-(1)::INT",
            UnaryOperator::Minus,
            Expr::Nested(Box::new(Expr::value(number("1")))),
        ),
        ("+1::INT", UnaryOperator::Plus, Expr::value(number("1"))),
        ("- -1::INT", UnaryOperator::Minus, Expr::value(number("-1"))),
    ] {
        assert_eq!(
            spark().verified_expr(sql),
            Expr::UnaryOp {
                op,
                expr: Box::new(Expr::Cast {
                    kind: CastKind::DoubleColon,
                    expr: Box::new(inner),
                    data_type: DataType::Int(None),
                    format: None,
                }),
            }
        );
    }
    assert_eq!(
        spark().verified_expr("1 - 2::INT"),
        Expr::BinaryOp {
            left: Box::new(Expr::value(number("1"))),
            op: BinaryOperator::Minus,
            right: Box::new(Expr::Cast {
                kind: CastKind::DoubleColon,
                expr: Box::new(Expr::value(number("2"))),
                data_type: DataType::Int(None),
                format: None,
            }),
        }
    );
    for sql in [
        "SELECT -1::INT::BIGINT",
        "SELECT -1.25::DECIMAL(3,2)",
        "SELECT - -1, 1 - -2, -1 * 2 + 3, CAST(-128 AS TINYINT)",
    ] {
        let statements = Parser::parse_sql(&SparkSqlDialect {}, sql).unwrap();
        assert_eq!(
            statements,
            Parser::parse_sql(&SparkSqlDialect {}, &statements[0].to_string()).unwrap()
        );
    }
}

#[test]
fn parse_negative_numeric_literal_errors() {
    for (sql, expected) in [
        (
            "SELECT -::INT",
            "Expected: an expression, found: :: at Line: 1, Column: 9",
        ),
        ("SELECT -1::", "Expected: a data type name, found: EOF"),
    ] {
        assert_eq!(
            Parser::parse_sql(&SparkSqlDialect {}, sql).unwrap_err(),
            ParserError::ParserError(expected.to_string())
        );
    }
}

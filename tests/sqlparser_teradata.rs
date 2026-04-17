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

//! Test SQL syntax, specific to [sqlparser::dialect::TeradataDialect].

use sqlparser::dialect::{Dialect, GenericDialect, TeradataDialect};
use sqlparser::test_utils::all_dialects_where;
use test_utils::TestedDialects;

mod test_utils;

fn teradata() -> TestedDialects {
    TestedDialects::new(vec![Box::new(TeradataDialect)])
}

fn teradata_and_generic() -> TestedDialects {
    TestedDialects::new(vec![Box::new(TeradataDialect), Box::new(GenericDialect {})])
}

#[test]
fn dialect_methods() {
    let d: &dyn Dialect = &TeradataDialect;
    assert_eq!(d.identifier_quote_style("x"), Some('"'));
    assert!(d.is_delimited_identifier_start('"'));
    assert!(!d.is_delimited_identifier_start('`'));
    assert!(d.is_identifier_start('$'));
    assert!(d.is_identifier_start('#'));
    assert!(d.is_identifier_part('$'));
    assert!(d.is_identifier_part('#'));
    assert!(d.supports_group_by_expr());
    assert!(!d.supports_boolean_literals());
    assert!(d.require_interval_qualifier());
    assert!(d.supports_comment_on());
    assert!(d.supports_create_table_select());
    assert!(d.supports_execute_immediate());
    assert!(d.supports_top_before_distinct());
    assert!(d.supports_window_function_null_treatment_arg());
    assert!(d.supports_string_literal_concatenation());
    assert!(d.supports_leading_comma_before_table_options());
}

#[test]
fn parse_identifier() {
    teradata().verified_stmt(concat!(
        "SELECT ",
        "NULL AS foo, ",
        "NULL AS _bar, ",
        "NULL AS #baz, ",
        "NULL AS $qux, ",
        "NULL AS a$1, ",
        "NULL AS a#1, ",
        "NULL AS a_1, ",
        r#"NULL AS "quoted id""#
    ));
}

#[test]
fn parse_create_table_multiset() {
    teradata_and_generic().verified_stmt("CREATE MULTISET TABLE foo (id INT)");
    teradata_and_generic().verified_stmt("CREATE SET TABLE foo (id INT)");
}

#[test]
fn parse_create_table_volatile() {
    teradata_and_generic().verified_stmt("CREATE VOLATILE TABLE foo (id INT)");
    teradata_and_generic().verified_stmt("CREATE MULTISET VOLATILE TABLE foo (id INT)");
}

#[test]
fn parse_create_table_fallback() {
    teradata().verified_stmt("CREATE TABLE foo, FALLBACK (id INT)");
    teradata().verified_stmt("CREATE TABLE foo, NO FALLBACK (id INT)");
    teradata().verified_stmt("CREATE MULTISET TABLE foo, NO FALLBACK (id INT)");
}

#[test]
fn parse_create_table_as_with_data() {
    teradata_and_generic().verified_stmt("CREATE TABLE foo AS (SELECT 1 AS a) WITH DATA");
    teradata_and_generic().verified_stmt("CREATE TABLE foo AS (SELECT 1 AS a) WITH NO DATA");
    teradata_and_generic()
        .verified_stmt("CREATE TABLE foo AS (SELECT 1 AS a) WITH DATA AND STATISTICS");
    teradata_and_generic()
        .verified_stmt("CREATE TABLE foo AS (SELECT 1 AS a) WITH DATA AND NO STATISTICS");
    teradata_and_generic()
        .verified_stmt("CREATE TABLE foo AS (SELECT 1 AS a) WITH NO DATA AND STATISTICS");
    teradata_and_generic()
        .verified_stmt("CREATE TABLE foo AS (SELECT 1 AS a) WITH NO DATA AND NO STATISTICS");
}

#[test]
fn parse_create_table_options() {
    teradata().verified_stmt(concat!(
        "CREATE MULTISET VOLATILE TABLE foo, NO FALLBACK ",
        "(id INT, name VARCHAR(100)) ",
        "ON COMMIT PRESERVE ROWS"
    ));
}

#[test]
fn parse_leading_comma_before_table_options() {
    let dialect = all_dialects_where(|d| d.supports_leading_comma_before_table_options());
    dialect.verified_stmt("CREATE TABLE foo, FALLBACK (id INT)");

    let unsupported_dialects =
        all_dialects_where(|d| !d.supports_leading_comma_before_table_options());
    assert!(unsupported_dialects
        .parse_sql_statements("CREATE TABLE foo, FALLBACK (id INT)")
        .is_err());
}

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
//! Test SQL syntax specific to Apache Doris.

#[macro_use]
mod test_utils;

use sqlparser::ast::*;
use sqlparser::dialect::{Dialect, DorisDialect, GenericDialect};
use sqlparser::tokenizer::Token;
use test_utils::*;

fn doris() -> TestedDialects {
    TestedDialects::new(vec![Box::new(DorisDialect {})])
}

fn doris_and_generic() -> TestedDialects {
    TestedDialects::new(vec![Box::new(DorisDialect {}), Box::new(GenericDialect {})])
}

#[test]
fn doris_identifier_and_string_literal_gates() {
    let dialect = DorisDialect {};
    assert_eq!(dialect.identifier_quote_style("identifier"), Some('`'));
    assert!(dialect.is_delimited_identifier_start('`'));
    assert!(dialect.supports_string_literal_backslash_escape());
    assert!(dialect.ignores_wildcard_escapes());
    assert!(dialect.supports_numeric_prefix());
    assert!(dialect.supports_parenthesized_auto_increment_column_option());
    assert!(dialect.supports_column_aggregation_function_option());
}

#[test]
fn generic_supports_doris_aggregate_column_options_only() {
    let dialect = GenericDialect {};
    assert!(!dialect.supports_parenthesized_auto_increment_column_option());
    assert!(dialect.supports_column_aggregation_function_option());
}

#[test]
fn parse_doris_strings_and_identifiers() {
    doris().verified_stmt(
        r#"SELECT "double quoted string", 'single quoted string', `select` FROM `db`.`table`"#,
    );
}

#[test]
fn doris_and_generic_parse_common_sql_identically() {
    doris_and_generic().verified_stmt("SELECT 1 AS properties FROM t");
}

#[test]
fn parse_doris_auto_increment_column() {
    doris().verified_stmt("CREATE TABLE t (id BIGINT AUTO_INCREMENT(100), name STRING)");
}

#[test]
fn parse_doris_auto_increment_no_start_value() {
    doris().verified_stmt("CREATE TABLE t (id BIGINT AUTO_INCREMENT, name STRING)");
}

#[test]
fn parse_generic_auto_increment_uses_unified_ast() {
    let generic = TestedDialects::new(vec![Box::new(GenericDialect {})]);
    let sql = "CREATE TABLE t (id BIGINT AUTO_INCREMENT)";
    let stmt = generic.verified_stmt(sql);
    match stmt {
        Statement::CreateTable(CreateTable { columns, .. }) => {
            assert_eq!(
                columns[0].options[0].option,
                ColumnOption::AutoIncrement(None)
            );
        }
        _ => panic!("Expected CreateTable"),
    }
}

#[test]
fn ast_doris_auto_increment_with_start() {
    let sql = "CREATE TABLE t (id BIGINT AUTO_INCREMENT(100), name STRING)";
    let stmt = doris().verified_stmt(sql);
    match stmt {
        Statement::CreateTable(CreateTable { columns, .. }) => {
            let id_col = &columns[0];
            assert_eq!(id_col.name, Ident::new("id"));
            let auto_inc = id_col
                .options
                .iter()
                .find(|o| matches!(o.option, ColumnOption::AutoIncrement(_)));
            assert!(auto_inc.is_some());
            match &auto_inc.unwrap().option {
                ColumnOption::AutoIncrement(Some(100)) => {}
                other => panic!("Expected AutoIncrement(Some(100)), got {:?}", other),
            }
        }
        _ => panic!("Expected CreateTable"),
    }
}

#[test]
fn ast_doris_auto_increment_without_start() {
    let sql = "CREATE TABLE t (id BIGINT AUTO_INCREMENT, name STRING)";
    let stmt = doris().verified_stmt(sql);
    match stmt {
        Statement::CreateTable(CreateTable { columns, .. }) => {
            let id_col = &columns[0];
            let auto_inc = id_col
                .options
                .iter()
                .find(|o| matches!(o.option, ColumnOption::AutoIncrement(_)));
            assert!(auto_inc.is_some());
            match &auto_inc.unwrap().option {
                ColumnOption::AutoIncrement(None) => {}
                other => panic!("Expected AutoIncrement(None), got {:?}", other),
            }
        }
        _ => panic!("Expected CreateTable"),
    }
}

#[test]
fn parse_doris_aggregate_column_options() {
    doris_and_generic()
        .verified_stmt("CREATE TABLE t (k BIGINT, v BIGINT SUM, bitmap_col BITMAP BITMAP_UNION)");
}

#[test]
fn parse_doris_all_aggregate_column_options() {
    doris_and_generic().verified_stmt(
        "CREATE TABLE t (k LARGEINT, v1 BIGINT SUM, v2 BIGINT MAX, v3 BIGINT MIN, v4 BIGINT REPLACE, v5 HLL HLL_UNION, v6 BITMAP BITMAP_UNION, v7 QUANTILESTATE QUANTILE_UNION)",
    );
}

#[test]
fn ast_doris_aggregate_column_option_is_dialect_specific() {
    let sql = "CREATE TABLE t (k BIGINT, v BIGINT SUM)";
    let stmt = doris().verified_stmt(sql);
    match stmt {
        Statement::CreateTable(CreateTable { columns, .. }) => {
            let v_col = &columns[1];
            assert_eq!(v_col.name, Ident::new("v"));
            let agg_opt = v_col
                .options
                .iter()
                .find(|o| matches!(o.option, ColumnOption::DialectSpecific(_)));
            assert!(agg_opt.is_some());
            match &agg_opt.unwrap().option {
                ColumnOption::DialectSpecific(tokens) => {
                    assert_eq!(tokens.len(), 1);
                    assert_eq!(tokens[0], Token::make_keyword("SUM"));
                }
                other => panic!("Expected DialectSpecific, got {:?}", other),
            }
        }
        _ => panic!("Expected CreateTable"),
    }
}

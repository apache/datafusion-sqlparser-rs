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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![cfg(not(feature = "std"))]

use sqlparser::dialect::GenericDialect;
use sqlparser::parser::{Parser, ParserError};

#[test]
fn with_recursion_limit_applies_without_default_features() {
    let dialect = GenericDialect {};
    let result = Parser::new(&dialect)
        .with_recursion_limit(1)
        .try_with_sql("SELECT * FROM foo WHERE (a OR (b OR (c OR d)))")
        .unwrap()
        .parse_statements();

    assert_eq!(result, Err(ParserError::RecursionLimitExceeded));
}

#[test]
fn default_recursion_limit_applies_without_default_features() {
    let dialect = GenericDialect {};
    let sql = format!(
        "SELECT * FROM t WHERE {}a = 1{}",
        "(".repeat(200),
        ")".repeat(200)
    );

    let result = Parser::parse_sql(&dialect, &sql);

    assert_eq!(result, Err(ParserError::RecursionLimitExceeded));
}

#[test]
fn deeply_nested_not_returns_error_without_default_features() {
    let dialect = GenericDialect {};
    let sql = format!("SELECT * FROM t WHERE {}a", "NOT ".repeat(1024));

    let result = Parser::parse_sql(&dialect, &sql);

    assert!(result.is_err());
}

#[test]
fn valid_nested_queries_parse_without_default_features() {
    let dialect = GenericDialect {};

    let result = Parser::parse_sql(&dialect, "SELECT 1 + (2 + 3)");

    assert!(result.is_ok());
}

#[test]
fn recursion_budget_restores_between_statements_without_default_features() {
    let dialect = GenericDialect {};
    let statements = Parser::new(&dialect)
        .with_recursion_limit(4)
        .try_with_sql("SELECT 1; SELECT 2; SELECT 3")
        .unwrap()
        .parse_statements()
        .unwrap();

    assert_eq!(statements.len(), 3);
}

#[test]
fn deeply_nested_intervals_hit_recursion_limit_without_default_features() {
    let dialect = GenericDialect {};
    let sql = format!("SELECT {}1", "INTERVAL ".repeat(1000));

    let result = Parser::parse_sql(&dialect, &sql);

    assert_eq!(result, Err(ParserError::RecursionLimitExceeded));
}

#[test]
fn nested_queries_hit_recursion_limit_without_default_features() {
    let dialect = GenericDialect {};
    let sql = format!(
        "{}SELECT 1{}",
        "SELECT 1 WHERE 1 IN (".repeat(100),
        ")".repeat(100)
    );

    let result = Parser::parse_sql(&dialect, &sql);

    assert_eq!(result, Err(ParserError::RecursionLimitExceeded));
}

#[test]
fn nested_table_factors_hit_recursion_limit_without_default_features() {
    let dialect = GenericDialect {};
    let sql = format!("SELECT * FROM {}t{}", "(".repeat(100), ")".repeat(100));

    let result = Parser::parse_sql(&dialect, &sql);

    assert_eq!(result, Err(ParserError::RecursionLimitExceeded));
}

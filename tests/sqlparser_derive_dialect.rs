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

//! Tests for the `derive_dialect!` macro.

use sqlparser::derive_dialect;
use sqlparser::dialect::{Dialect, GenericDialect, MySqlDialect, PostgreSqlDialect};
use sqlparser::parser::Parser;

#[test]
fn test_method_overrides() {
    derive_dialect!(EnhancedGenericDialect, GenericDialect, overrides = {
        supports_order_by_all = true,
        supports_triple_quoted_string = true,
    });
    let dialect = EnhancedGenericDialect::new();

    // Overridden methods
    assert!(dialect.supports_order_by_all());
    assert!(dialect.supports_triple_quoted_string());

    // Non-overridden retains base behavior
    assert!(!dialect.supports_factorial_operator());

    // Parsing works with the overrides
    let result = Parser::new(&dialect)
        .try_with_sql("SELECT '''value''' FROM t ORDER BY ALL")
        .unwrap()
        .parse_statements();

    assert!(result.is_ok());
}

#[test]
fn test_preserve_type_id() {
    // Check the override works and the parser recognizes it as the base type
    derive_dialect!(
        PreservedTypeDialect,
        GenericDialect,
        preserve_type_id = true,
        overrides = { supports_order_by_all = true }
    );
    let dialect = PreservedTypeDialect::new();
    let d: &dyn Dialect = &dialect;

    assert!(dialect.supports_order_by_all());
    assert!(d.is::<GenericDialect>());
}

#[test]
fn test_different_base_dialects() {
    derive_dialect!(
        EnhancedMySqlDialect,
        MySqlDialect,
        overrides = { supports_order_by_all = true }
    );
    derive_dialect!(UniquePostgreSqlDialect, PostgreSqlDialect);

    let pg = UniquePostgreSqlDialect::new();
    let mysql = EnhancedMySqlDialect::new();

    // Inherit different base behaviors
    assert!(pg.supports_filter_during_aggregation()); // PostgreSQL feature
    assert!(mysql.supports_string_literal_backslash_escape()); // MySQL feature
    assert!(mysql.supports_order_by_all()); // Override

    // Each has unique TypeId
    let pg_ref: &dyn Dialect = &pg;
    let mysql_ref: &dyn Dialect = &mysql;
    assert!(pg_ref.is::<UniquePostgreSqlDialect>());
    assert!(!pg_ref.is::<PostgreSqlDialect>());
    assert!(mysql_ref.is::<EnhancedMySqlDialect>());
}

#[test]
fn test_identifier_quote_style_overrides() {
    derive_dialect!(
        BacktickGenericDialect,
        GenericDialect,
        overrides = { identifier_quote_style = '`' }
    );
    derive_dialect!(
        AnotherBacktickDialect,
        GenericDialect,
        overrides = { identifier_quote_style = '[' }
    );
    derive_dialect!(
        QuotelessPostgreSqlDialect,
        PostgreSqlDialect,
        preserve_type_id = true,
        overrides = { identifier_quote_style = None }
    );

    // Char literal (auto-wrapped in Some)
    assert_eq!(
        BacktickGenericDialect::new().identifier_quote_style("x"),
        Some('`')
    );
    // Another char literal
    assert_eq!(
        AnotherBacktickDialect::new().identifier_quote_style("x"),
        Some('[')
    );
    // None (overrides PostgreSQL's default '"')
    assert_eq!(
        QuotelessPostgreSqlDialect::new().identifier_quote_style("x"),
        None
    );
}

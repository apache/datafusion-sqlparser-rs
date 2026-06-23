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

use sqlparser::dialect::{Dialect, DorisDialect, GenericDialect};
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

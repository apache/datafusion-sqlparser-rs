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

use sqlparser::dialect::{Dialect, TeradataDialect};
use test_utils::TestedDialects;

mod test_utils;

fn teradata() -> TestedDialects {
    TestedDialects::new(vec![Box::new(TeradataDialect)])
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

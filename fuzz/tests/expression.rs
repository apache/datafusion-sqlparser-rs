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

#[path = "../fuzz_targets/expression/mod.rs"]
#[expect(
    dead_code,
    reason = "the chain generator is only used by the fuzz targets"
)]
mod expression;

#[path = "../fuzz_targets/expression/postgres.rs"]
mod postgres;
#[path = "../fuzz_targets/expression/sqlite.rs"]
mod sqlite;

use expression::parenthesize;
use postgres::spell_custom_operators;
use sqlite::fold_booleans;
use sqlparser::ast::{visit_expressions_mut, Expr, Statement};
use sqlparser::dialect::{Dialect, GenericDialect, PostgreSqlDialect, SQLiteDialect};
use sqlparser::parser::Parser;
use std::ops::ControlFlow;

fn statement(dialect: &dyn Dialect, sql: &str) -> Statement {
    let [statement] = <[Statement; 1]>::try_from(Parser::parse_sql(dialect, sql).unwrap())
        .expect("one statement");
    statement
}

fn parenthesized(dialect: &dyn Dialect, sql: &str) -> String {
    let mut statement = statement(dialect, sql);
    parenthesize(&mut statement);
    statement.to_string()
}

fn without_nested(mut statement: Statement) -> Statement {
    let _ = visit_expressions_mut(&mut statement, |expr| {
        while let Expr::Nested(inner) = expr {
            *expr = std::mem::replace(inner, Expr::value(sqlparser::ast::Value::Null));
        }
        ControlFlow::<()>::Continue(())
    });
    statement
}

#[test]
fn parenthesize_wraps_each_compound_expression_once() {
    assert_eq!(
        parenthesized(
            &GenericDialect {},
            "SELECT a + b * c FROM t WHERE NOT a = 1"
        ),
        "SELECT (a + (b * c)) FROM t WHERE (NOT (a = 1))"
    );
}

#[test]
fn parenthesize_leaves_atoms_and_parenthesized_forms_alone() {
    let sql = "SELECT (a), (SELECT 1), (1, 2), 'x', t.b FROM t";
    assert_eq!(parenthesized(&GenericDialect {}, sql), sql);
}

#[test]
fn parenthesize_only_adds_parentheses() {
    let cases: [(&dyn Dialect, &str); 6] = [
        (&GenericDialect {}, "SELECT a + b * c - d / e FROM t"),
        (
            &PostgreSqlDialect {},
            "SELECT CASE WHEN a THEN b ELSE c END::int, -a ^ 2 FROM t",
        ),
        (
            &PostgreSqlDialect {},
            "SELECT a = ANY(ARRAY[1, 2]) AND b IS NOT NULL FROM t",
        ),
        (
            &PostgreSqlDialect {},
            "UPDATE t SET a = CAST(b AS int) + abs(c) WHERE a BETWEEN 1 AND 2",
        ),
        (
            &SQLiteDialect {},
            "SELECT a COLLATE NOCASE || b, a IN (1, 2), x ->> '$.n' FROM t",
        ),
        (
            &SQLiteDialect {},
            "SELECT 1 FROM t ORDER BY a LIKE 'x%' ESCAPE '\\', NOT b",
        ),
    ];
    for (dialect, sql) in cases {
        let grouped = parenthesized(dialect, sql);
        assert_eq!(
            without_nested(statement(dialect, &grouped)),
            without_nested(statement(dialect, sql)),
            "{sql} became {grouped}"
        );
    }
}

#[test]
fn spell_custom_operators_names_only_custom_operators() {
    let mut statement = statement(&PostgreSqlDialect {}, "SELECT a<>~1, a + 1, a <-> b FROM t");
    spell_custom_operators(&mut statement);
    assert_eq!(
        statement.to_string(),
        "SELECT a OPERATOR(<>~) 1, a + 1, a <-> b FROM t"
    );
}

#[test]
fn fold_booleans_lowercases_only_unquoted_keywords() {
    assert_eq!(
        fold_booleans(
            "SELECT TRUE, False, 'TRUE', \"TRUE\", [TRUE], `TRUE`, trueish, x_TRUE FROM t"
        ),
        "SELECT true, false, 'TRUE', \"TRUE\", [TRUE], `TRUE`, trueish, x_TRUE FROM t"
    );
}

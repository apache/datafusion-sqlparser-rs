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

//! Checks that sqlparser groups PostgreSQL operators the way PostgreSQL does.
//!
//! sqlparser's tree is printed with every compound expression parenthesized, and PostgreSQL must
//! build the same parse tree from that text as from the original.

#![no_main]

mod expression;

use expression::{parenthesize, Chain, Grammar};
use libfuzzer_sys::fuzz_target;
use serde_json::Value;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

const POSTGRES: Grammar = Grammar {
    atoms: &[
        "1",
        "2",
        "3",
        "0",
        "2.5",
        "1e3",
        "'a'",
        "'{1,2}'",
        "NULL",
        "TRUE",
        "FALSE",
        "a",
        "t.b",
        "$1",
        "B'101'",
        "E'\\n'",
        "DATE '2020-01-01'",
        "INTERVAL '1 day'",
    ],
    prefix: &[
        "-",
        "+",
        "~",
        "NOT",
        "@",
        "|/",
        "||/",
        "OPERATOR(pg_catalog.-)",
    ],
    infix: &[
        "+",
        "-",
        "*",
        "/",
        "%",
        "^",
        "||",
        "&",
        "|",
        "#",
        "<<",
        ">>",
        "=",
        "<>",
        "!=",
        "<",
        ">",
        "<=",
        ">=",
        "AND",
        "OR",
        "IS DISTINCT FROM",
        "IS NOT DISTINCT FROM",
        "LIKE",
        "NOT LIKE",
        "ILIKE",
        "NOT ILIKE",
        "SIMILAR TO",
        "NOT SIMILAR TO",
        "ESCAPE",
        "BETWEEN",
        "NOT BETWEEN",
        "BETWEEN SYMMETRIC",
        "IN",
        "NOT IN",
        "~",
        "!~",
        "~*",
        "!~*",
        "~~",
        "!~~",
        "@>",
        "<@",
        "&&",
        "->",
        "->>",
        "#>",
        "#>>",
        "?",
        "?|",
        "@@",
        "AT TIME ZONE",
        "OVERLAPS",
        "IS",
        "IS NOT",
        "= ANY",
        "< ALL",
        "OPERATOR(pg_catalog.+)",
        "<->",
        "-|-",
    ],
    postfix: &[
        "IS NULL",
        "IS NOT NULL",
        "ISNULL",
        "NOTNULL",
        "IS TRUE",
        "IS NOT TRUE",
        "IS FALSE",
        "IS UNKNOWN",
        "::int",
        "::text",
        "::numeric",
        "::int[]",
        "COLLATE \"C\"",
        "[1]",
        "[1:2]",
        "AT LOCAL",
        "IS NORMALIZED",
        "IS JSON",
    ],
    functions: &["abs", "coalesce", "greatest", "length", "now", "row"],
    types: &[
        "int",
        "text",
        "numeric(10, 2)",
        "bool",
        "int[]",
        "timestamptz",
    ],
    array: ("ARRAY[", "]"),
};

/// Drops source positions, which differ between the two texts by construction.
fn strip_locations(value: &mut Value) {
    match value {
        Value::Object(map) => {
            map.retain(|key, _| {
                !(key.ends_with("location") || key == "stmt_len" || key.starts_with("list_"))
            });
            map.values_mut().for_each(strip_locations);
        }
        Value::Array(items) => items.iter_mut().for_each(strip_locations),
        _ => {}
    }
}

fn tree(parsed: &pg_query::ParseResult) -> Value {
    let mut tree = serde_json::to_value(&parsed.protobuf.stmts).expect("parse trees serialize");
    strip_locations(&mut tree);
    tree
}

fuzz_target!(|chain: Chain| {
    let sql = format!("SELECT {} FROM t", chain.render(&POSTGRES));
    let Ok(original) = pg_query::parse(&sql) else {
        return;
    };
    let Ok(mut statements) = Parser::parse_sql(&PostgreSqlDialect {}, &sql) else {
        return;
    };
    let [statement] = statements.as_mut_slice() else {
        return;
    };
    parenthesize(statement);
    let grouped = statement.to_string();
    let regrouped = pg_query::parse(&grouped);
    if regrouped
        .as_ref()
        .is_ok_and(|regrouped| tree(&original) == tree(regrouped))
    {
        return;
    }
    let version = original.protobuf.version;
    let reading = original.deparse().unwrap_or_default();
    match regrouped {
        Err(err) => panic!(
            "PostgreSQL {version} rejects sqlparser's grouping of SQL it accepts\n  sql:      {sql}\n  grouped:  {grouped}\n  postgres: {reading}\n  error:    {err}"
        ),
        Ok(regrouped) => panic!(
            "sqlparser groups the expression differently from PostgreSQL {version}\n  sql:       {sql}\n  postgres:  {reading}\n  sqlparser: {}",
            regrouped.deparse().unwrap_or(grouped),
        ),
    }
});

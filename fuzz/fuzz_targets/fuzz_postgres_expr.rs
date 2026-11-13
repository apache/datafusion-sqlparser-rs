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
#[path = "expression/postgres.rs"]
mod spelling;

use expression::{parenthesize, Case, Grammar};
use libfuzzer_sys::fuzz_target;
use serde_json::Value;
use spelling::spell_custom_operators;
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
        ".5",
        "1.5e-3",
        "0x1F",
        "0o17",
        "0b101",
        "1_000",
        "U&'a'",
        "$$a$$",
        "X'1F'",
        "CURRENT_TIMESTAMP",
        "CURRENT_USER",
        "ROW(1, 2)",
        "'(1,2)'::point",
        "'{\"a\":1}'::jsonb",
        "(SELECT 1)",
        "EXISTS (SELECT 1)",
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
        "> SOME",
        "<> ALL",
        "LIKE ANY",
        "OPERATOR(pg_catalog.+)",
        "<->",
        "-|-",
        "#-",
        "?&",
        "@?",
        "##",
        "<<|",
        "|>>",
        "&<",
        "&>",
        "<^",
        ">^",
        "?#",
        "~=",
        "<<=",
        ">>=",
        "AND NOT",
        "OR NOT",
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
        "::regclass",
        "::jsonb",
        "::bit(3)",
        "::varchar(5)",
        "::timestamp(3) with time zone",
        "::double precision",
        "::\"char\"",
        "IS NOT UNKNOWN",
        "IS NOT NORMALIZED",
        "IS NFC NORMALIZED",
        "IS JSON OBJECT",
        "IS NOT JSON",
        "[1][2]",
        "[:2]",
        "[1:]",
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
    contexts: &[
        "SELECT {} FROM t",
        "SELECT 1 FROM t WHERE {}",
        "SELECT 1 FROM t ORDER BY {}",
        "SELECT 1 FROM t GROUP BY 1 HAVING {}",
        "SELECT 1 FROM t JOIN u ON {}",
        "UPDATE t SET a = {}",
    ],
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

fuzz_target!(|case: Case| {
    let sql = case.render(&POSTGRES);
    let Ok(original) = pg_query::parse(&sql) else {
        return;
    };
    let Ok(mut statements) = Parser::parse_sql(&PostgreSqlDialect {}, &sql) else {
        return;
    };
    let [statement] = statements.as_mut_slice() else {
        return;
    };
    spell_custom_operators(statement);
    let printed = statement.to_string();
    let version = original.protobuf.version;
    let reading = || original.deparse().unwrap_or_default();
    let expected = tree(&original);
    match pg_query::parse(&printed) {
        Ok(baseline) if tree(&baseline) == expected => {}
        Ok(baseline) => panic!(
            "sqlparser reads or prints the statement differently from PostgreSQL {version}\n  sql:       {sql}\n  printed:   {printed}\n  postgres:  {}\n  sqlparser: {}",
            reading(),
            baseline.deparse().unwrap_or_default(),
        ),
        Err(err) => panic!(
            "PostgreSQL {version} rejects sqlparser's printing of SQL it accepts\n  sql:      {sql}\n  printed:  {printed}\n  postgres: {}\n  error:    {err}",
            reading(),
        ),
    }
    // `printed` and `grouped` differ only in parentheses, so any difference left is grouping.
    parenthesize(statement);
    let grouped = statement.to_string();
    match pg_query::parse(&grouped) {
        Ok(regrouped) if tree(&regrouped) == expected => {}
        Ok(regrouped) => panic!(
            "sqlparser groups the expression differently from PostgreSQL {version}\n  sql:       {sql}\n  postgres:  {}\n  sqlparser: {}",
            reading(),
            regrouped.deparse().unwrap_or(grouped),
        ),
        Err(err) => panic!(
            "PostgreSQL {version} rejects sqlparser's grouping of SQL it accepts\n  sql:      {sql}\n  grouped:  {grouped}\n  postgres: {}\n  error:    {err}",
            reading(),
        ),
    }
});

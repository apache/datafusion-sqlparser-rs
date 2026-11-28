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

//! Tests that the v2 parser, converted to this crate's AST, agrees with this
//! crate's parser.

#![cfg(feature = "v2")]

use std::collections::{BTreeMap, BTreeSet};

use sqlparser::dialect::{
    dialect_from_str, AnsiDialect, BigQueryDialect, DuckDbDialect, GenericDialect, MySqlDialect,
    PostgreSqlDialect, SnowflakeDialect,
};
use sqlparser::parser::ParserError;
use sqlparser::v2::{self, Error};

#[track_caller]
fn assert_same_ast(dialect: &dyn sqlparser::dialect::Dialect, sql: &str) {
    let comparison = v2::compare(dialect, sql);
    assert!(
        comparison.same_ast(),
        "{sql}\nv1: {:?}\nv2: {:?}",
        comparison.v1,
        comparison.v2
    );
}

#[test]
fn queries_match_v1() {
    let postgres = PostgreSqlDialect {};
    for sql in [
        "SELECT a, b + 1 AS c FROM t WHERE x = 'it''s' AND y <> 2",
        "SELECT DISTINCT count(*), max(t.a) FROM s.t AS u WHERE (a OR b)",
        "SELECT \"Quoted\", 1.5, NULL, TRUE FROM t x",
        "WITH c (a) AS (SELECT 1) SELECT a FROM c ORDER BY a DESC NULLS LAST LIMIT 10 OFFSET 5",
        "SELECT * FROM a LEFT JOIN b ON a.id = b.id JOIN c USING (id) CROSS JOIN d",
        "SELECT a FROM t GROUP BY a HAVING count(*) > 1 UNION ALL SELECT 1",
        "SELECT CASE WHEN a IS NULL THEN 0 ELSE a END, a BETWEEN 1 AND 2, a NOT IN (1, 2) FROM t",
        "SELECT CAST(a AS INT), a::TEXT, b LIKE 'x%' ESCAPE '\\' FROM t",
        "SELECT row_number() OVER (PARTITION BY a ORDER BY b ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t",
        "SELECT * FROM (SELECT 1) AS s (x) WHERE EXISTS (SELECT 1 FROM u WHERE u.x = s.x)",
        "SELECT EXTRACT(YEAR FROM d), INTERVAL '1' DAY, DATE '2020-01-01', CURRENT_DATE FROM t",
    ] {
        assert_same_ast(&postgres, sql);
    }
    assert_same_ast(&MySqlDialect {}, "SELECT `a` FROM t LIMIT 1, 2");
    assert_same_ast(&BigQueryDialect, "SELECT \"double quoted\" FROM `p.d.t`");
    assert_same_ast(
        &SnowflakeDialect,
        "SELECT a FROM t QUALIFY row_number() OVER (ORDER BY a) = 1",
    );
    assert_same_ast(&DuckDbDialect {}, "SELECT a[1], a[1:2] FROM t");
}

#[test]
fn spans_have_line_and_column() {
    let statements = v2::parse_sql(&PostgreSqlDialect {}, "SELECT\n  abc FROM t").unwrap();
    let sqlparser::ast::Statement::Query(query) = &statements[0] else {
        panic!("expected a query");
    };
    let sqlparser::ast::SetExpr::Select(select) = &*query.body else {
        panic!("expected a select");
    };
    let sqlparser::ast::SelectItem::UnnamedExpr(sqlparser::ast::Expr::Identifier(ident)) =
        &select.projection[0]
    else {
        panic!("expected an identifier");
    };
    assert_eq!(ident.span.start, sqlparser::tokenizer::Location::new(2, 3));
    assert_eq!(ident.span.end, sqlparser::tokenizer::Location::new(2, 6));
}

#[test]
fn generic_and_ansi_are_unsupported() {
    assert_eq!(
        v2::parse_sql(&GenericDialect {}, "SELECT 1"),
        Err(Error::UnsupportedDialect)
    );
    assert_eq!(
        v2::parse_sql(&AnsiDialect {}, "SELECT 1"),
        Err(Error::UnsupportedDialect)
    );
}

#[test]
fn errors_report_v2_rejections_and_gaps() {
    let postgres = PostgreSqlDialect {};
    assert_eq!(
        v2::parse_sql(&postgres, "SELECT 1,\nFROM"),
        Err(Error::Parse(ParserError::ParserError(
            "expected identifier, found FROM at Line: 2, Column: 1".to_string()
        )))
    );
    assert!(matches!(
        v2::parse_sql(&postgres, "INSERT INTO t VALUES (1)"),
        Err(Error::Unconverted(_))
    ));
}

#[test]
fn fallback_uses_v1_when_v2_gives_no_result() {
    let sql = "INSERT INTO t VALUES (1)";
    assert_eq!(
        v2::parse_sql_with_fallback(&PostgreSqlDialect {}, sql),
        sqlparser::parser::Parser::parse_sql(&PostgreSqlDialect {}, sql)
    );
}

/// Each SQL statement in the corpus generated from this crate's tests.
fn corpus_statements() -> Vec<String> {
    let text = include_str!("v2_corpus.txt");
    let mut statements = Vec::new();
    let mut lines = text
        .strip_suffix('\n')
        .unwrap_or(text)
        .split('\n')
        .peekable();
    while let Some(header) = lines.next() {
        let mut body = Vec::new();
        while let Some(line) = lines.next_if(|line| !line.starts_with("%%%% ")) {
            body.push(line);
        }
        if header == "%%%% stmt" {
            statements.push(body.join("\n"));
        }
    }
    statements
}

/// The number of corpus cases whose conversion matches this crate's AST.
/// `bigdecimal` makes both parsers reject some numbers.
const MIN_SAME: usize = if cfg!(feature = "bigdecimal") {
    14635
} else {
    14640
};

/// Converted ASTs must equal this crate's, apart from the differences listed
/// in `v2_known_differences.txt`, and coverage must not fall below [`MIN_SAME`].
#[test]
fn corpus_parity() {
    // This crate's parser needs more than the default test stack for some
    // corpus cases in a debug build.
    std::thread::Builder::new()
        .stack_size(256 << 20)
        .spawn(check_corpus_parity)
        .unwrap()
        .join()
        .unwrap();
}

fn check_corpus_parity() {
    const DIALECTS: [&str; 14] = [
        "mysql",
        "postgres",
        "redshift",
        "snowflake",
        "spark",
        "databricks",
        "hive",
        "bigquery",
        "clickhouse",
        "duckdb",
        "mssql",
        "oracle",
        "sqlite",
        "teradata",
    ];
    let statements = corpus_statements();
    let mut counts: BTreeMap<&str, usize> = BTreeMap::new();
    let mut different = Vec::new();
    for name in DIALECTS {
        let dialect = dialect_from_str(name).unwrap();
        for sql in &statements {
            let comparison = v2::compare(dialect.as_ref(), sql);
            let outcome = match (&comparison.v1, &comparison.v2) {
                (Ok(_), Ok(_)) if comparison.same_ast() => "same",
                (Ok(v1), Ok(v2)) => {
                    different.push((difference_key(name, sql), first_difference(v1, v2)));
                    "different"
                }
                (Ok(_), Err(Error::Unconverted(_))) => "unconverted",
                (Ok(_), Err(_)) => "rejected by v2 only",
                (Err(_), Ok(_)) => "rejected by v1 only",
                (Err(_), Err(_)) => "rejected by both",
            };
            *counts.entry(outcome).or_default() += 1;
        }
    }
    if let Some(path) = std::env::var_os("V2_DIFFERENCES") {
        let text: Vec<String> = different.iter().map(|(k, d)| format!("{k}\n{d}")).collect();
        std::fs::write(path, text.join("\n\n")).unwrap();
    }
    println!("{counts:#?}");

    let found: BTreeSet<String> = different.into_iter().map(|(key, _)| key).collect();
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/v2_known_differences.txt"
    );
    if std::env::var_os("V2_BLESS").is_some() {
        let text: String = found.iter().map(|key| format!("{key}\n")).collect();
        std::fs::write(path, text).unwrap();
        return;
    }
    let known: BTreeSet<String> = std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(str::to_string)
        .collect();
    let new: Vec<_> = found.difference(&known).collect();
    let fixed: Vec<_> = known.difference(&found).collect();
    assert!(
        new.is_empty(),
        "conversions that newly differ from v1: {new:#?}"
    );
    assert!(
        fixed.is_empty(),
        "remove these from {path}, or rerun with V2_BLESS=1: {fixed:#?}"
    );
    let same = counts.get("same").copied().unwrap_or_default();
    assert!(
        same >= MIN_SAME,
        "conversions matching v1 fell from {MIN_SAME} to {same}"
    );
}

/// Both ASTs' debug output, for finding where they differ.
fn first_difference<T: std::fmt::Debug>(v1: &T, v2: &T) -> String {
    format!("  v1: {v1:?}\n  v2: {v2:?}")
}

/// One line naming a case, with the SQL escaped.
fn difference_key(dialect: &str, sql: &str) -> String {
    let sql = sql
        .replace('\\', "\\\\")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
        .replace('\t', "\\t");
    format!("{dialect}\t{sql}")
}

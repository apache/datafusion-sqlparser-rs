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

use criterion::{criterion_group, criterion_main, Criterion};
use sqlparser::dialect::{GenericDialect, PostgreSqlDialect, SQLiteDialect};
use sqlparser::keywords::Keyword;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::{Span, Word};

fn basic_queries(c: &mut Criterion) {
    let mut group = c.benchmark_group("sqlparser-rs parsing benchmark");
    let dialect = GenericDialect {};

    let string = "SELECT * FROM my_table WHERE 1 = 1";
    group.bench_function("sqlparser::select", |b| {
        b.iter(|| Parser::parse_sql(&dialect, string).unwrap());
    });

    let with_query = "
        WITH derived AS (
            SELECT MAX(a) AS max_a,
                   COUNT(b) AS b_num,
                   user_id
            FROM MY_TABLE
            GROUP BY user_id
        )
        SELECT * FROM my_table
        LEFT JOIN derived USING (user_id)
    ";
    group.bench_function("sqlparser::with_select", |b| {
        b.iter(|| Parser::parse_sql(&dialect, with_query).unwrap());
    });

    let large_statement = {
        let expressions = (0..1000)
            .map(|n| format!("FN_{n}(COL_{n})"))
            .collect::<Vec<_>>()
            .join(", ");
        let tables = (0..1000)
            .map(|n| format!("TABLE_{n}"))
            .collect::<Vec<_>>()
            .join(" CROSS JOIN ");
        let where_condition = (0..1000)
            .map(|n| format!("COL_{n} = {n}"))
            .collect::<Vec<_>>()
            .join(" OR ");
        let order_condition = (0..1000)
            .map(|n| format!("COL_{n} DESC"))
            .collect::<Vec<_>>()
            .join(", ");

        format!(
            "SELECT {expressions} FROM {tables} WHERE {where_condition} ORDER BY {order_condition}"
        )
    };

    group.bench_function("parse_large_statement", |b| {
        b.iter(|| Parser::parse_sql(&dialect, std::hint::black_box(large_statement.as_str())));
    });

    let large_statement = Parser::parse_sql(&dialect, large_statement.as_str())
        .unwrap()
        .pop()
        .unwrap();

    group.bench_function("format_large_statement", |b| {
        b.iter(|| {
            let _formatted_query = large_statement.to_string();
        });
    });
}

/// Benchmark comparing `to_ident(&self)` vs `clone().into_ident(self)`.
///
/// Both approaches have equivalent performance since the String clone dominates.
/// `to_ident()` is preferred for clearer code (one method call vs two).
fn word_to_ident(c: &mut Criterion) {
    let mut group = c.benchmark_group("word_to_ident");

    // Create Word instances with varying identifier lengths
    let words: Vec<Word> = (0..100)
        .map(|i| Word {
            value: format!("identifier_name_with_number_{i}"),
            quote_style: None,
            keyword: Keyword::NoKeyword,
        })
        .collect();
    let span = Span::empty();

    // clone().into_ident(): clones entire Word struct, then moves the String value
    group.bench_function("clone_into_ident_100x", |b| {
        b.iter(|| {
            for w in &words {
                std::hint::black_box(w.clone().into_ident(span));
            }
        });
    });

    // to_ident(): clones only the String value directly into the Ident
    group.bench_function("to_ident_100x", |b| {
        b.iter(|| {
            for w in &words {
                std::hint::black_box(w.to_ident(span));
            }
        });
    });

    group.finish();
}

/// Benchmark parsing queries with many identifiers to show real-world impact
fn parse_many_identifiers(c: &mut Criterion) {
    let mut group = c.benchmark_group("parse_identifiers");
    let dialect = GenericDialect {};

    // Query with many column references (identifiers)
    let many_columns = (0..100)
        .map(|n| format!("column_{n}"))
        .collect::<Vec<_>>()
        .join(", ");
    let query = format!("SELECT {many_columns} FROM my_table");

    group.bench_function("select_100_columns", |b| {
        b.iter(|| Parser::parse_sql(&dialect, std::hint::black_box(&query)));
    });

    // Query with many table.column references
    let qualified_columns = (0..100)
        .map(|n| format!("t{}.column_{n}", n % 5))
        .collect::<Vec<_>>()
        .join(", ");
    let query_qualified = format!("SELECT {qualified_columns} FROM t0, t1, t2, t3, t4");

    group.bench_function("select_100_qualified_columns", |b| {
        b.iter(|| Parser::parse_sql(&dialect, std::hint::black_box(&query_qualified)));
    });

    group.finish();
}

/// Benchmark parsing pathological compound chains that previously caused 2^N
/// work in `parse_compound_expr`. The input `IF a0.a1...aN.#` rejects at the
/// trailing `#`, which used to force quadratic-or-worse backtracking through
/// the chain.
fn parse_compound_chain(c: &mut Criterion) {
    let mut group = c.benchmark_group("parse_compound_chain");
    let dialect = GenericDialect {};

    for &n in &[10usize, 20, 30] {
        let chain = (0..n)
            .map(|i| format!("a{i}"))
            .collect::<Vec<_>>()
            .join(".");
        let sql = format!("IF {chain}.#");

        group.bench_function(format!("chain_{n}"), |b| {
            b.iter(|| {
                let _ = Parser::parse_sql(&dialect, std::hint::black_box(&sql));
            });
        });
    }

    group.finish();
}

/// Benchmark parsing pathological compound chains with a reserved keyword in
/// field position, like `SELECT x.not-b.not-b...`. The `.not-b` shape used to
/// cause 2^N work in `parse_compound_expr` because `parse_prefix` descended
/// into `parse_not` -> `parse_subexpr`, re-walking the remaining chain at
/// every segment.
fn parse_compound_keyword_chain(c: &mut Criterion) {
    let mut group = c.benchmark_group("parse_compound_keyword_chain");
    let dialect = GenericDialect {};

    for &n in &[5usize, 10, 15] {
        let body = std::iter::repeat_n(".not-b", n).collect::<String>();
        let sql = format!("SELECT x{body}");

        group.bench_function(format!("chain_{n}"), |b| {
            b.iter(|| {
                let _ = Parser::parse_sql(&dialect, std::hint::black_box(&sql));
            });
        });
    }

    group.finish();
}

/// Benchmark parsing pathological `IF(<keyword-fn>(<keyword-fn>(...x` chains
/// that previously caused 2^N work in `parse_prefix`. Each nested
/// `current_time(` segment used to be explored twice at every level (once via
/// the speculative reserved-word arm, once via the unreserved-word fallback),
/// doubling work per level. Post-fix the cost is linear in chain length.
fn parse_prefix_keyword_call_chain(c: &mut Criterion) {
    let mut group = c.benchmark_group("parse_prefix_keyword_call_chain");
    let dialect = PostgreSqlDialect {};

    for &n in &[10usize, 20, 30] {
        let sql = String::from("if(") + &"current_time(".repeat(n) + "x";

        group.bench_function(format!("chain_{n}"), |b| {
            b.iter(|| {
                let _ = Parser::parse_sql(&dialect, std::hint::black_box(&sql));
            });
        });
    }

    group.finish();
}

/// Benchmark parsing pathological `case-case-case-...c` chains that
/// previously caused 2^N work in `parse_prefix`. Each `case` token used to
/// trigger a speculative `parse_case_expr` that recursively descends the
/// chain, but the unreserved-word fallback returns `Identifier(case)` so the
/// overall `parse_prefix` succeeds and the failure cache never fires.
/// Post-fix the per-arm cache short-circuits the speculative descent.
fn parse_prefix_case_chain(c: &mut Criterion) {
    let mut group = c.benchmark_group("parse_prefix_case_chain");
    let dialect = SQLiteDialect {};

    for &n in &[10usize, 20, 30] {
        let sql = "case\t-".repeat(n) + "c";

        group.bench_function(format!("chain_{n}"), |b| {
            b.iter(|| {
                let _ = Parser::parse_sql(&dialect, std::hint::black_box(&sql));
            });
        });
    }

    group.finish();
}

/// Benchmark parsing pathological paren chains that previously caused 2^N
/// work in `parse_table_factor`. The input `SELECT 1 FROM ((((...` rejects
/// at EOF, which used to force exponential backtracking through the chain.
fn parse_table_factor_paren_chain(c: &mut Criterion) {
    let mut group = c.benchmark_group("parse_table_factor_paren_chain");
    let dialect = GenericDialect {};

    for &n in &[10usize, 20, 30] {
        let mut sql = String::from("SELECT 1 ");
        for _ in 0..5 {
            sql.push_str("FROM ");
            sql.push_str(&"(".repeat(n));
            sql.push(' ');
        }

        group.bench_function(format!("chain_{n}"), |b| {
            b.iter(|| {
                let _ = Parser::new(&dialect)
                    .with_recursion_limit(256)
                    .try_with_sql(std::hint::black_box(&sql))
                    .and_then(|mut p| p.parse_statements());
            });
        });
    }

    group.finish();
}

/// Benchmark parsing pathological `CAST(CASE (CAST(CASE (...` chains that
/// previously caused 2^N work in `parse_function_args` on dialects with
/// expression-named function arguments (the argument expression was parsed
/// once to detect the named form, then re-parsed on the unnamed path).
fn parse_function_arg_call_chain(c: &mut Criterion) {
    let mut group = c.benchmark_group("parse_function_arg_call_chain");
    let dialect = PostgreSqlDialect {};

    for &n in &[10usize, 20, 30] {
        let sql = String::from("SELECT ") + &"CAST(CASE (".repeat(n) + &")".repeat(n);

        group.bench_function(format!("chain_{n}"), |b| {
            b.iter(|| {
                let _ = Parser::new(&dialect)
                    .with_recursion_limit(256)
                    .try_with_sql(std::hint::black_box(&sql))
                    .and_then(|mut p| p.parse_statements());
            });
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    basic_queries,
    word_to_ident,
    parse_many_identifiers,
    parse_compound_chain,
    parse_compound_keyword_chain,
    parse_prefix_keyword_call_chain,
    parse_prefix_case_chain,
    parse_table_factor_paren_chain,
    parse_function_arg_call_chain
);
criterion_main!(benches);

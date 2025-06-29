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
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

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
            .join(" JOIN ");
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

criterion_group!(benches, basic_queries);
criterion_main!(benches);

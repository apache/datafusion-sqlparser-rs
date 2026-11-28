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

//! Compares this crate's parser with the v2 parser converted to this crate's AST.

use criterion::{criterion_group, criterion_main, Criterion};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use std::hint::black_box;

fn large_statement() -> String {
    let list =
        |f: &dyn Fn(usize) -> String, sep: &str| (0..1000).map(f).collect::<Vec<_>>().join(sep);
    format!(
        "SELECT {} FROM {} WHERE {} ORDER BY {}",
        list(&|n| format!("FN_{n}(COL_{n})"), ", "),
        list(&|n| format!("TABLE_{n}"), " CROSS JOIN "),
        list(&|n| format!("COL_{n} = {n}"), " OR "),
        list(&|n| format!("COL_{n} DESC"), ", "),
    )
}

fn v2_bridge(c: &mut Criterion) {
    let dialect = PostgreSqlDialect {};
    let queries = [
        ("select", "SELECT * FROM my_table WHERE 1 = 1".to_string()),
        (
            "with_select",
            "WITH derived AS (SELECT MAX(a) AS max_a, COUNT(b) AS b_num, user_id \
             FROM MY_TABLE GROUP BY user_id) \
             SELECT * FROM my_table LEFT JOIN derived USING (user_id)"
                .to_string(),
        ),
        (
            "tpch_q5",
            "SELECT n_name, sum(l_extendedprice * (1 - l_discount)) AS revenue \
             FROM customer, orders, lineitem, supplier, nation, region \
             WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND l_suppkey = s_suppkey \
             AND c_nationkey = s_nationkey AND s_nationkey = n_nationkey \
             AND n_regionkey = r_regionkey AND r_name = 'ASIA' \
             AND o_orderdate >= DATE '1994-01-01' AND o_orderdate < DATE '1995-01-01' \
             GROUP BY n_name ORDER BY revenue DESC"
                .to_string(),
        ),
        ("large_statement", large_statement()),
    ];
    let mut group = c.benchmark_group("v2_bridge");
    for (name, sql) in &queries {
        assert_eq!(
            sqlparser::v2::parse_sql(&dialect, sql).unwrap(),
            Parser::parse_sql(&dialect, sql).unwrap()
        );
        group.bench_function(format!("{name}/v1"), |b| {
            b.iter(|| Parser::parse_sql(&dialect, black_box(sql)).unwrap())
        });
        group.bench_function(format!("{name}/v2"), |b| {
            b.iter(|| sqlparser::v2::parse_sql(&dialect, black_box(sql)).unwrap())
        });
    }
    group.finish();
}

criterion_group!(benches, v2_bridge);
criterion_main!(benches);

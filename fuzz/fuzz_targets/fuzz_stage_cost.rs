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

#![no_main]

use libfuzzer_sys::fuzz_target;
use sqlparser::dialect::{
    AnsiDialect, BigQueryDialect, ClickHouseDialect, DatabricksDialect, Dialect, DuckDbDialect,
    GenericDialect, HiveDialect, MsSqlDialect, MySqlDialect, OracleDialect, PostgreSqlDialect,
    RedshiftSqlDialect, SQLiteDialect, SnowflakeDialect, SparkSqlDialect, TeradataDialect,
};
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Tokenizer;
use std::hint::black_box;
use std::time::{Duration, Instant};

/// Stage times below this are noise, never a finding.
const FLOOR: Duration = Duration::from_millis(10);
/// How many times the previous stage's time a stage may take.
const MAX_RATIO: u32 = 50;
/// Runs whose fastest time per stage must still breach before it is reported.
const CONFIRM_RUNS: usize = 3;

#[derive(Clone, Copy, Debug)]
struct Costs {
    tokenize: Duration,
    parse: Duration,
    print: Duration,
}

impl Costs {
    fn fastest(self, other: Self) -> Self {
        Self {
            tokenize: self.tokenize.min(other.tokenize),
            parse: self.parse.min(other.parse),
            print: self.print.min(other.print),
        }
    }

    fn breach(&self) -> Option<&'static str> {
        let over =
            |stage: Duration, baseline: Duration| stage > FLOOR && stage > baseline * MAX_RATIO;
        if over(self.parse, self.tokenize) {
            Some("parse against tokenize")
        } else if over(self.print, self.parse) {
            Some("print against parse")
        } else {
            None
        }
    }
}

fn measure(dialect: &dyn Dialect, sql: &str) -> Option<Costs> {
    let started = Instant::now();
    let tokens = Tokenizer::new(dialect, sql).tokenize_with_location().ok()?;
    let tokenize = started.elapsed();

    let started = Instant::now();
    let statements = Parser::new(dialect)
        .with_tokens_with_locations(tokens)
        .parse_statements();
    let parse = started.elapsed();

    let started = Instant::now();
    for statement in statements.iter().flatten() {
        black_box(statement.to_string());
    }
    let print = started.elapsed();

    Some(Costs {
        tokenize,
        parse,
        print,
    })
}

fuzz_target!(|sql: &str| {
    let dialects: [(&str, &dyn Dialect); 16] = [
        ("ansi", &AnsiDialect {}),
        ("bigquery", &BigQueryDialect {}),
        ("clickhouse", &ClickHouseDialect {}),
        ("databricks", &DatabricksDialect {}),
        ("duckdb", &DuckDbDialect {}),
        ("generic", &GenericDialect {}),
        ("hive", &HiveDialect {}),
        ("mssql", &MsSqlDialect {}),
        ("mysql", &MySqlDialect {}),
        ("oracle", &OracleDialect {}),
        ("postgres", &PostgreSqlDialect {}),
        ("redshift", &RedshiftSqlDialect {}),
        ("sqlite", &SQLiteDialect {}),
        ("snowflake", &SnowflakeDialect {}),
        ("spark", &SparkSqlDialect {}),
        ("teradata", &TeradataDialect {}),
    ];
    for (name, dialect) in dialects {
        let Some(costs) = measure(dialect, sql) else {
            continue;
        };
        if costs.breach().is_none() {
            continue;
        }
        let confirmed = (0..CONFIRM_RUNS)
            .filter_map(|_| measure(dialect, sql))
            .fold(costs, Costs::fastest);
        if let Some(stage) = confirmed.breach() {
            panic!("{name}: {stage} exceeds {MAX_RATIO}x above {FLOOR:?}\n  costs: {confirmed:?}");
        }
    }
});

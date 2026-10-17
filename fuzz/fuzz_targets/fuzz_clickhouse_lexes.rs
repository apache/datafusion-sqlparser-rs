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

use clickhouse_lexer_sys::{Lexer, TokenKind, UPSTREAM_COMMIT};
use libfuzzer_sys::fuzz_target;
use sqlparser::dialect::ClickHouseDialect;
use sqlparser::tokenizer::Tokenizer;

/// Whether ClickHouse's lexer reads `sql` to the end without an error token or a lone `$`.
///
/// A lone `$` lexes as `DollarSign`, which only the JSONPath parser consumes, so no
/// statement holding one parses.
fn clickhouse_lexes(sql: &str) -> bool {
    Lexer::new(sql).all(|token| !token.kind.is_error() && token.kind != TokenKind::DollarSign)
}

fuzz_target!(|sql: &str| {
    if !clickhouse_lexes(sql) {
        return;
    }
    if let Err(err) = Tokenizer::new(&ClickHouseDialect {}, sql).tokenize() {
        panic!(
            "ClickHouse lexer at {UPSTREAM_COMMIT} accepts SQL the ClickHouse dialect tokenizer rejects\n  sql: {sql:?}\n  error: {err}"
        )
    }
});

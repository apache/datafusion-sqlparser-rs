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
use sqlparser::dialect::ClickHouseDialect;
use sqlparser::tokenizer::Tokenizer;
use std::ffi::{c_char, c_int, c_uchar, c_void};

const COMMIT: &str = include_str!("../vendor/clickhouse-lexer/COMMIT");

// C ABI of the vendored lexer, see fuzz/vendor/clickhouse-lexer/Parsers/clickhouse_lexer.h.
extern "C" {
    static clickhouse_lexer_size: usize;
    fn clickhouse_lexer_create(
        ptr: *mut c_void,
        begin: *const c_char,
        end: *const c_char,
        max_query_size: usize,
    );
    fn clickhouse_lexer_next_token(
        ptr: *mut c_void,
        out_token_begin: *mut *const c_char,
        out_token_end: *mut *const c_char,
    ) -> c_uchar;
    fn clickhouse_lexer_token_is_error(token: c_uchar) -> c_int;
    fn clickhouse_lexer_token_is_end(token: c_uchar) -> c_int;
}

/// Whether ClickHouse's lexer reads `sql` to the end without an error token.
fn clickhouse_lexes(sql: &str) -> bool {
    // SAFETY: the symbol is a plain `size_t` the C++ side initializes statically.
    let size = unsafe { clickhouse_lexer_size };
    // u64 slots keep the pointer fields of the C++ Lexer aligned.
    let mut lexer = vec![0u64; size.div_ceil(8)];
    let ptr = lexer.as_mut_ptr().cast::<c_void>();
    let begin = sql.as_ptr().cast::<c_char>();
    // SAFETY: `lexer` holds `clickhouse_lexer_size` bytes and `sql` outlives every call below.
    unsafe {
        clickhouse_lexer_create(ptr, begin, begin.add(sql.len()), 0);
        loop {
            let mut token_begin = std::ptr::null();
            let mut token_end = std::ptr::null();
            let token = clickhouse_lexer_next_token(ptr, &mut token_begin, &mut token_end);
            if clickhouse_lexer_token_is_error(token) != 0 {
                return false;
            }
            if clickhouse_lexer_token_is_end(token) != 0 {
                return true;
            }
        }
    }
}

fuzz_target!(|sql: &str| {
    if !clickhouse_lexes(sql) {
        return;
    }
    if let Err(err) = Tokenizer::new(&ClickHouseDialect {}, sql).tokenize() {
        panic!(
            "ClickHouse lexer at {} accepts SQL the ClickHouse dialect tokenizer rejects\n  sql: {sql:?}\n  error: {err}",
            COMMIT.trim()
        )
    }
});

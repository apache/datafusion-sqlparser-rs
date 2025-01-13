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

#![warn(clippy::all)]
//! Test SQL syntax, which all sqlparser dialects must parse in the same way.
//!
//! Note that it does not mean all SQL here is valid in all the dialects, only
//! that 1) it's either standard or widely supported and 2) it can be parsed by
//! sqlparser regardless of the chosen dialect (i.e. it doesn't conflict with
//! dialect-specific parsing rules).

extern crate core;

use sqlparser::test_utils::all_dialects;

#[test]
fn format_update_tuple_row_values() {
    all_dialects().verified_stmt(
        "\
UPDATE x
SET (a, b) = (1, 2)
WHERE c = 3\
        ",
    );
}

#[test]
fn format_update_multiple_sets_newlines() {
    all_dialects().verified_stmt(
        "\
UPDATE x
SET a = 1,
    b = 2,
    c = 3
WHERE d = 4\
        ",
    );
}

#[test]
fn format_update_newline_before_where() {
    all_dialects().verified_stmt(
        "\
UPDATE x SET x = 1
WHERE c = 3\
        ",
    );
}

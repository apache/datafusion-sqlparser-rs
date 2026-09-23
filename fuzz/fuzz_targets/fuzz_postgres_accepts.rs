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
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::{Parser, ParserError};

fuzz_target!(|sql: &str| {
    if sql.contains('\0') {
        return;
    }
    let Ok(parsed) = pg_query::parse(sql) else {
        return;
    };
    match Parser::parse_sql(&PostgreSqlDialect {}, sql) {
        Ok(_) | Err(ParserError::RecursionLimitExceeded) => {}
        Err(err) => panic!(
            "PostgreSQL {} accepts SQL the PostgreSQL dialect rejects\n  sql: {sql:?}\n  error: {err}",
            parsed.protobuf.version
        ),
    }
});

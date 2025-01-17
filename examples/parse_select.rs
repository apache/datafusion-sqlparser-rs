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

use sqlparser::dialect::GenericDialect;
use sqlparser::parser::*;

fn main() {
    let sql = "SELECT a, b, 123, myfunc(b) \
               FROM table_1 \
               WHERE a > b AND b < 100 \
               ORDER BY a DESC, b";

    let dialect = GenericDialect::default();

    let ast = Parser::parse_sql(&dialect, sql).unwrap();

    println!("AST: {ast:?}");
}

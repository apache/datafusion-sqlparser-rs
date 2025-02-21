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
use sqlparser::parser::Parser;

macro_rules! tpch_tests {
    ($($name:ident: $value:expr_2021,)*) => {
        const QUERIES: &[&str] = &[
            $(include_str!(concat!("queries/tpch/", $value, ".sql"))),*
        ];
    $(

        #[test]
        fn $name() {
            let dialect = GenericDialect {};
            let res = Parser::parse_sql(&dialect, QUERIES[$value -1]);
                assert!(res.is_ok());
        }
    )*
    }
}

tpch_tests! {
    tpch_1: 1,
    tpch_2: 2,
    tpch_3: 3,
    tpch_4: 4,
    tpch_5: 5,
    tpch_6: 6,
    tpch_7: 7,
    tpch_8: 8,
    tpch_9: 9,
    tpch_10: 10,
    tpch_11: 11,
    tpch_12: 12,
    tpch_13: 13,
    tpch_14: 14,
    tpch_15: 15,
    tpch_16: 16,
    tpch_17: 17,
    tpch_18: 18,
    tpch_19: 19,
    tpch_20: 20,
    tpch_21: 21,
    tpch_22: 22,
}

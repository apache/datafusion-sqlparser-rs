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

use crate::dialect::Dialect;

/// A [`Dialect`] for [ClickHouse](https://clickhouse.com/).
#[derive(Debug)]
pub struct ClickHouseDialect {}

impl Dialect for ClickHouseDialect {
    fn is_identifier_start(&self, ch: char) -> bool {
        // See https://clickhouse.com/docs/en/sql-reference/syntax/#syntax-identifiers
        ch.is_ascii_lowercase() || ch.is_ascii_uppercase() || ch == '_'
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        self.is_identifier_start(ch) || ch.is_ascii_digit()
    }

    fn supports_string_literal_backslash_escape(&self) -> bool {
        true
    }

    fn supports_select_wildcard_except(&self) -> bool {
        true
    }

    fn describe_requires_table_keyword(&self) -> bool {
        true
    }

    fn require_interval_qualifier(&self) -> bool {
        true
    }

    fn supports_limit_comma(&self) -> bool {
        true
    }

    fn supports_insert_table_function(&self) -> bool {
        true
    }

    fn supports_insert_format(&self) -> bool {
        true
    }

    fn supports_numeric_literal_underscores(&self) -> bool {
        true
    }

    // ClickHouse uses this for some FORMAT expressions in `INSERT` context, e.g. when inserting
    // with FORMAT JSONEachRow a raw JSON key-value expression is valid and expected.
    //
    // [ClickHouse formats](https://clickhouse.com/docs/en/interfaces/formats)
    fn supports_dictionary_syntax(&self) -> bool {
        true
    }

    /// See <https://clickhouse.com/docs/en/sql-reference/functions#higher-order-functions---operator-and-lambdaparams-expr-function>
    fn supports_lambda_functions(&self) -> bool {
        true
    }

    fn supports_from_first_select(&self) -> bool {
        true
    }

    /// See <https://clickhouse.com/docs/en/sql-reference/statements/select/order-by>
    fn supports_order_by_all(&self) -> bool {
        true
    }

    // See <https://clickhouse.com/docs/en/sql-reference/aggregate-functions/grouping_function#grouping-sets>
    fn supports_group_by_expr(&self) -> bool {
        true
    }

    /// See <https://clickhouse.com/docs/en/sql-reference/statements/select/group-by#rollup-modifier>
    fn supports_group_by_with_modifier(&self) -> bool {
        true
    }

    /// Supported since 2020.
    /// See <https://clickhouse.com/docs/whats-new/changelog/2020#backward-incompatible-change-2>
    fn supports_nested_comments(&self) -> bool {
        true
    }
}

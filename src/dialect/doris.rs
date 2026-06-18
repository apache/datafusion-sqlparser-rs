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

/// A [`Dialect`] for [Apache Doris](https://doris.apache.org/).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct DorisDialect {}

impl Dialect for DorisDialect {
    fn is_delimited_identifier_start(&self, ch: char) -> bool {
        ch == '`'
    }

    fn identifier_quote_style(&self, _identifier: &str) -> Option<char> {
        Some('`')
    }

    fn is_identifier_start(&self, ch: char) -> bool {
        ch.is_ascii_alphabetic() || ch == '_' || !ch.is_ascii()
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        self.is_identifier_start(ch) || ch.is_ascii_digit()
    }

    fn supports_string_literal_backslash_escape(&self) -> bool {
        true
    }

    fn ignores_wildcard_escapes(&self) -> bool {
        true
    }

    fn supports_numeric_prefix(&self) -> bool {
        true
    }

    fn supports_create_table_key_model_clause(&self) -> bool {
        true
    }

    fn supports_create_table_range_list_partitioning_clause(&self) -> bool {
        true
    }

    fn supports_create_table_distribution_clause(&self) -> bool {
        true
    }

    fn supports_create_table_properties_clause(&self) -> bool {
        true
    }

    fn supports_create_table_model_clause_without_marker(&self) -> bool {
        true
    }

    fn supports_parenthesized_auto_increment_column_option(&self) -> bool {
        true
    }

    fn supports_column_aggregation_function_option(&self) -> bool {
        true
    }
}

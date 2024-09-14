// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::dialect::{Dialect, DialectFlags};

/// A [`Dialect`] for [DuckDB](https://duckdb.org/)
#[derive(Debug, Default)]
pub struct DuckDbDialect;

// In most cases the redshift dialect is identical to [`PostgresSqlDialect`].
impl Dialect for DuckDbDialect {
    fn flags(&self) -> DialectFlags {
        DialectFlags {
            supports_trailing_commas: true,
            supports_filter_during_aggregation: true,
            supports_group_by_expr: true,
            supports_named_fn_args_with_eq_operator: true,
            // DuckDB uses this syntax for `STRUCT`s.
            //
            // https://duckdb.org/docs/sql/data_types/struct.html#creating-structs
            supports_dictionary_syntax: true,
            // DuckDB uses this syntax for `MAP`s.
            //
            // https://duckdb.org/docs/sql/data_types/map.html#creating-maps
            support_map_literal_syntax: true,
            ..Default::default()
        }
    }

    fn is_identifier_start(&self, ch: char) -> bool {
        ch.is_alphabetic() || ch == '_'
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        ch.is_alphabetic() || ch.is_ascii_digit() || ch == '$' || ch == '_'
    }
}

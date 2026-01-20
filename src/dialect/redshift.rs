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
use core::iter::Peekable;
use core::str::Chars;

use super::PostgreSqlDialect;

/// A [`Dialect`] for [RedShift](https://aws.amazon.com/redshift/)
#[derive(Debug)]
pub struct RedshiftSqlDialect {}

// In most cases the redshift dialect is identical to [`PostgresSqlDialect`].
//
// Notable differences:
// 1. Redshift treats brackets `[` and `]` differently. For example, `SQL SELECT a[1][2] FROM b`
// in the Postgres dialect, the query will be parsed as an array, while in the Redshift dialect it will
// be a json path
impl Dialect for RedshiftSqlDialect {
    /// Determine if a character starts a potential nested quoted identifier.
    /// Example: RedShift supports the following quote styles to all mean the same thing:
    /// ```sql
    /// SELECT 1 AS foo;
    /// SELECT 1 AS "foo";
    /// SELECT 1 AS [foo];
    /// SELECT 1 AS ["foo"];
    /// ```
    fn is_nested_delimited_identifier_start(&self, ch: char) -> bool {
        ch == '['
    }

    /// Only applicable whenever [`Self::is_nested_delimited_identifier_start`] returns true
    /// If the next sequence of tokens potentially represent a nested identifier, then this method
    /// returns a tuple containing the outer quote style, and if present, the inner (nested) quote style.
    ///
    /// Example (Redshift):
    /// ```text
    /// `["foo"]` => Some(`[`, Some(`"`))
    /// `[foo]` => Some(`[`, None)
    /// `[0]` => None
    /// `"foo"` => None
    /// ```
    fn peek_nested_delimited_identifier_quotes(
        &self,
        mut chars: Peekable<Chars<'_>>,
    ) -> Option<(char, Option<char>)> {
        if chars.peek() != Some(&'[') {
            return None;
        }

        chars.next();

        let mut not_white_chars = chars.skip_while(|ch| ch.is_whitespace()).peekable();

        if let Some(&ch) = not_white_chars.peek() {
            if ch == '"' {
                return Some(('[', Some('"')));
            }
            if self.is_identifier_start(ch) {
                return Some(('[', None));
            }
        }

        None
    }

    fn is_identifier_start(&self, ch: char) -> bool {
        // UTF-8 multibyte characters are supported in identifiers via the PostgreSqlDialect.
        // https://docs.aws.amazon.com/redshift/latest/dg/r_names.html
        PostgreSqlDialect {}.is_identifier_start(ch) || ch == '#'
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        // UTF-8 multibyte characters are supported in identifiers via the PostgreSqlDialect.
        // https://docs.aws.amazon.com/redshift/latest/dg/r_names.html
        PostgreSqlDialect {}.is_identifier_part(ch) || ch == '#'
    }

    /// redshift has `CONVERT(type, value)` instead of `CONVERT(value, type)`
    /// <https://docs.aws.amazon.com/redshift/latest/dg/r_CONVERT_function.html>
    fn convert_type_before_value(&self) -> bool {
        true
    }

    fn supports_connect_by(&self) -> bool {
        true
    }

    /// Redshift expects the `TOP` option before the `ALL/DISTINCT` option:
    /// <https://docs.aws.amazon.com/redshift/latest/dg/r_SELECT_list.html#r_SELECT_list-parameters>
    fn supports_top_before_distinct(&self) -> bool {
        true
    }

    /// Redshift supports PartiQL: <https://docs.aws.amazon.com/redshift/latest/dg/super-overview.html>
    fn supports_partiql(&self) -> bool {
        true
    }

    fn supports_string_escape_constant(&self) -> bool {
        true
    }

    fn supports_geometric_types(&self) -> bool {
        true
    }

    fn supports_bitwise_shift_operators(&self) -> bool {
        true
    }

    fn supports_array_typedef_with_brackets(&self) -> bool {
        true
    }

    fn allow_extract_single_quotes(&self) -> bool {
        true
    }

    fn supports_string_literal_backslash_escape(&self) -> bool {
        true
    }

    fn supports_select_wildcard_exclude(&self) -> bool {
        true
    }

    fn supports_select_exclude(&self) -> bool {
        true
    }

    fn supports_create_table_like_parenthesized(&self) -> bool {
        true
    }

    fn supports_string_literal_concatenation_with_newline(&self) -> bool {
        true
    }
}

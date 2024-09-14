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

/// A permissive, general purpose [`Dialect`], which parses a wide variety of SQL
/// statements, from many different dialects.
#[derive(Debug)]
pub struct GenericDialect(DialectFlags);

impl Default for GenericDialect {
    fn default() -> Self {
        Self(DialectFlags {
            supports_unicode_string_literal: true,
            supports_group_by_expr: true,
            supports_connect_by: true,
            supports_match_recognize: true,
            supports_start_transaction_modifier: true,
            supports_window_function_null_treatment_arg: true,
            supports_dictionary_syntax: true,
            supports_window_clause_named_window_reference: true,
            supports_parenthesized_set_variables: true,
            supports_select_wildcard_except: true,
            support_map_literal_syntax: true,
            allow_extract_custom: true,
            allow_extract_single_quotes: true,
            supports_create_index_with_clause: true,
            ..Default::default()
        })
    }
}

impl Dialect for GenericDialect {
    fn flags(&self) -> &DialectFlags {
        &self.0
    }

    fn is_delimited_identifier_start(&self, ch: char) -> bool {
        ch == '"' || ch == '`'
    }

    fn is_identifier_start(&self, ch: char) -> bool {
        ch.is_alphabetic() || ch == '_' || ch == '#' || ch == '@'
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        ch.is_alphabetic()
            || ch.is_ascii_digit()
            || ch == '@'
            || ch == '$'
            || ch == '#'
            || ch == '_'
    }
}

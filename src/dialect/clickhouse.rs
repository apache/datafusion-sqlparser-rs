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

use alloc::string::String;
use core::iter::Peekable;
use core::str::Chars;

use crate::dialect::Dialect;
use crate::tokenizer::Token;

/// A [`Dialect`] for [ClickHouse](https://clickhouse.com/).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ClickHouseDialect {}

impl Dialect for ClickHouseDialect {
    fn is_identifier_start(&self, ch: char) -> bool {
        // See https://clickhouse.com/docs/en/sql-reference/syntax/#syntax-identifiers
        ch.is_ascii_lowercase() || ch.is_ascii_uppercase() || ch == '_'
    }

    /// Aliases are identifiers, quoted with `"` or a backtick, see
    /// <https://clickhouse.com/docs/reference/syntax#identifiers>
    fn supports_string_literal_column_alias(&self, _explicit: bool) -> bool {
        false
    }

    /// See <https://clickhouse.com/docs/reference/syntax#identifiers>
    fn supports_string_literal_table_alias(&self, _explicit: bool) -> bool {
        false
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        // Undocumented, but the server accepts `$` in identifiers for MySQL compatibility
        // since https://github.com/ClickHouse/ClickHouse/pull/14232
        self.is_identifier_start(ch) || ch.is_ascii_digit() || ch == '$'
    }

    /// ClickHouse accepts both backticks and double quotes, but its own
    /// formatter emits backticks by default.
    /// See <https://clickhouse.com/docs/en/sql-reference/syntax#identifiers>
    fn identifier_quote_style(&self, _identifier: &str) -> Option<char> {
        Some('`')
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

    fn supports_numeric_prefix(&self) -> bool {
        true
    }

    /// Undocumented; the lexer reads digits followed by word characters as one bare word
    /// unless the run is a number, see "Try to parse it to a identifier" in `Lexer.cpp`.
    fn is_digit_leading_identifier(
        &self,
        chars: &Peekable<Chars<'_>>,
        prev_token: Option<&Token>,
    ) -> bool {
        let run: String = chars
            .clone()
            .take_while(|&ch| self.is_identifier_part(ch))
            .collect();
        // After a period the lexer reads only a plain integer as a number, so `t.12e34` is a word.
        if prev_token == Some(&Token::Period) {
            !is_digit_run(&run, |ch| ch.is_ascii_digit())
        } else {
            !is_number_run(&run)
        }
    }

    fn supports_partition_by_after_order_by(&self) -> bool {
        true
    }

    fn supports_array_join_syntax(&self) -> bool {
        true
    }

    fn supports_array_typedef_with_parentheses(&self) -> bool {
        true
    }

    fn supports_map_typedef_with_parentheses(&self) -> bool {
        true
    }

    // ClickHouse uses this for some FORMAT expressions in `INSERT` context, e.g. when inserting
    // with FORMAT JSONEachRow a raw JSON key-value expression is valid and expected.
    //
    // [ClickHouse formats](https://clickhouse.com/docs/en/interfaces/formats)
    fn supports_dictionary_syntax(&self) -> bool {
        true
    }

    // See <https://clickhouse.com/docs/sql-reference/operators/in>
    fn supports_in_unparenthesized_expr(&self) -> bool {
        true
    }

    // See <https://clickhouse.com/docs/sql-reference/operators/in#distributed-subqueries>
    fn supports_global_in(&self) -> bool {
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

    /// See <https://clickhouse.com/docs/sql-reference/syntax#comments>
    fn supports_hash_comment(&self) -> bool {
        true
    }

    /// See <https://github.com/ClickHouse/ClickHouse/blob/master/src/Parsers/Lexer.cpp>
    fn requires_hash_comment_space(&self) -> bool {
        true
    }

    /// See <https://clickhouse.com/docs/en/sql-reference/statements/optimize>
    fn supports_optimize_table(&self) -> bool {
        true
    }

    /// See <https://clickhouse.com/docs/en/sql-reference/statements/select/prewhere>
    fn supports_prewhere(&self) -> bool {
        true
    }

    /// See <https://clickhouse.com/docs/en/sql-reference/statements/select/order-by#order-by-expr-with-fill-modifier>
    fn supports_with_fill(&self) -> bool {
        true
    }

    /// See <https://clickhouse.com/docs/en/sql-reference/statements/select/limit-by>
    fn supports_limit_by(&self) -> bool {
        true
    }

    /// See <https://clickhouse.com/docs/en/sql-reference/statements/select/order-by#order-by-expr-with-fill-modifier>
    fn supports_interpolate(&self) -> bool {
        true
    }

    /// See <https://clickhouse.com/docs/en/sql-reference/statements/select#settings-in-select-query>
    fn supports_settings(&self) -> bool {
        true
    }

    /// See <https://clickhouse.com/docs/en/sql-reference/statements/select/format>
    fn supports_select_format(&self) -> bool {
        true
    }

    /// See <https://clickhouse.com/docs/sql-reference/statements/select#replace>
    fn supports_select_wildcard_replace(&self) -> bool {
        true
    }

    fn supports_comma_separated_trim(&self) -> bool {
        true
    }

    /// See <https://clickhouse.com/docs/sql-reference/statements/alter/column>
    fn supports_alter_column_position(&self) -> bool {
        true
    }

    /// See <https://clickhouse.com/docs/sql-reference/data-types/tuple#addressing-tuple-elements>
    fn supports_tuple_element_access(&self) -> bool {
        true
    }
}

/// Whether a run of identifier characters is a number: digit groups joined by single
/// underscores, with an optional `0x` or `0b` prefix or decimal exponent. A bare exponent
/// marker counts (`1e`), so that `1e+5` is still read by the number path.
fn is_number_run(run: &str) -> bool {
    if let Some(hex) = run.strip_prefix("0x").or_else(|| run.strip_prefix("0X")) {
        return is_digit_run(hex, |ch| ch.is_ascii_hexdigit());
    }
    if let Some(bin) = run.strip_prefix("0b").or_else(|| run.strip_prefix("0B")) {
        return is_digit_run(bin, |ch| ch == '0' || ch == '1');
    }
    let (mantissa, exponent) = run.split_once(['e', 'E']).unwrap_or((run, ""));
    is_digit_run(mantissa, |ch| ch.is_ascii_digit())
        && (exponent.is_empty() || is_digit_run(exponent, |ch| ch.is_ascii_digit()))
}

/// Digit groups joined by single underscores, e.g. `1_000`.
fn is_digit_run(s: &str, is_digit: impl Fn(char) -> bool) -> bool {
    !s.is_empty()
        && s.split('_')
            .all(|group| !group.is_empty() && group.chars().all(&is_digit))
}

#[cfg(test)]
mod tests {
    use super::is_number_run;

    #[test]
    fn number_runs() {
        for run in [
            "0", "8", "1_000", "1_0_0", "1e5", "1E5", "1e1_0", "1e", "0x1F", "0Xff_ff", "0b101",
            "0B1_0",
        ] {
            assert!(is_number_run(run), "{run}");
        }
        for run in [
            "", "_", "_1", "1_", "1__0", "8_e", "8large", "1_a", "1L", "1e_1", "1e1_", "1e3a",
            "1ea", "1e5e3", "0x", "0x_1", "0x1_", "0xg", "0b", "0b2", "0b1_", "1$",
        ] {
            assert!(!is_number_run(run), "{run}");
        }
    }
}

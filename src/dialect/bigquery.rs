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

use crate::ast::Statement;
use crate::dialect::Dialect;
use crate::keywords::Keyword;
use crate::parser::{Parser, ParserError};
use crate::tokenizer::Token;

/// These keywords are disallowed as column identifiers. Such that
/// `SELECT 5 AS <col> FROM T` is rejected by BigQuery.
const RESERVED_FOR_COLUMN_ALIAS: &[Keyword] = &[
    Keyword::WITH,
    Keyword::SELECT,
    Keyword::WHERE,
    Keyword::GROUP,
    Keyword::HAVING,
    Keyword::ORDER,
    Keyword::LATERAL,
    Keyword::LIMIT,
    Keyword::FETCH,
    Keyword::UNION,
    Keyword::EXCEPT,
    Keyword::INTERSECT,
    Keyword::FROM,
    Keyword::INTO,
    Keyword::END,
];

/// A [`Dialect`] for [Google Bigquery](https://cloud.google.com/bigquery/)
#[derive(Debug, Default)]
pub struct BigQueryDialect;

impl Dialect for BigQueryDialect {
    fn parse_statement(&self, parser: &mut Parser) -> Option<Result<Statement, ParserError>> {
        if parser.parse_keyword(Keyword::BEGIN) {
            if parser.peek_keyword(Keyword::TRANSACTION)
                || parser.peek_token_ref().token == Token::SemiColon
                || parser.peek_token_ref().token == Token::EOF
            {
                parser.prev_token();
                return None;
            }
            return Some(parser.parse_begin_exception_end());
        }

        None
    }

    /// See <https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#identifiers>
    fn is_delimited_identifier_start(&self, ch: char) -> bool {
        ch == '`'
    }

    fn supports_projection_trailing_commas(&self) -> bool {
        true
    }

    /// See <https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#create_table_statement>
    fn supports_column_definition_trailing_commas(&self) -> bool {
        true
    }

    fn is_identifier_start(&self, ch: char) -> bool {
        ch.is_ascii_lowercase() || ch.is_ascii_uppercase() || ch == '_'
            // BigQuery supports `@@foo.bar` variable syntax in its procedural language.
            // https://cloud.google.com/bigquery/docs/reference/standard-sql/procedural-language#beginexceptionend
            || ch == '@'
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        ch.is_ascii_lowercase() || ch.is_ascii_uppercase() || ch.is_ascii_digit() || ch == '_'
    }

    /// See [doc](https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#quoted_literals)
    fn supports_triple_quoted_string(&self) -> bool {
        true
    }

    /// See [doc](https://cloud.google.com/bigquery/docs/reference/standard-sql/navigation_functions#first_value)
    fn supports_window_function_null_treatment_arg(&self) -> bool {
        true
    }

    // See https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#escape_sequences
    fn supports_string_literal_backslash_escape(&self) -> bool {
        true
    }

    /// See [doc](https://cloud.google.com/bigquery/docs/reference/standard-sql/window-function-calls#ref_named_window)
    fn supports_window_clause_named_window_reference(&self) -> bool {
        true
    }

    /// See [doc](https://cloud.google.com/bigquery/docs/reference/standard-sql/procedural-language#set)
    fn supports_parenthesized_set_variables(&self) -> bool {
        true
    }

    // See https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#select_except
    fn supports_select_wildcard_except(&self) -> bool {
        true
    }

    fn require_interval_qualifier(&self) -> bool {
        true
    }

    // See https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#constructing_a_struct
    fn supports_struct_literal(&self) -> bool {
        true
    }

    /// See <https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#select_expression_star>
    fn supports_select_expr_star(&self) -> bool {
        true
    }

    /// See <https://cloud.google.com/bigquery/docs/reference/standard-sql/procedural-language#execute_immediate>
    fn supports_execute_immediate(&self) -> bool {
        true
    }

    // See <https://cloud.google.com/bigquery/docs/access-historical-data>
    fn supports_table_versioning(&self) -> bool {
        true
    }

    // See <https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#group_by_clause>
    fn supports_group_by_expr(&self) -> bool {
        true
    }

    fn is_column_alias(&self, kw: &Keyword, _parser: &mut Parser) -> bool {
        !RESERVED_FOR_COLUMN_ALIAS.contains(kw)
    }

    fn supports_pipe_operator(&self) -> bool {
        true
    }

    fn supports_create_table_multi_schema_info_sources(&self) -> bool {
        true
    }
}

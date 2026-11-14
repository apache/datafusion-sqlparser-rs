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

#[cfg(not(feature = "std"))]
use alloc::vec;

use crate::ast::{CreateFunction, Statement, Use};
use crate::dialect::Dialect;
use crate::keywords::{Keyword, RESERVED_FOR_TABLE_FACTOR};
use crate::parser::{Parser, ParserError};

/// A [`Dialect`] for [Hive](https://hive.apache.org/).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct HiveDialect {}

impl Dialect for HiveDialect {
    /// Hive accepts `USE DEFAULT` with no database named.
    fn parse_use(&self, parser: &mut Parser) -> Option<Result<Statement, ParserError>> {
        if parser.parse_keyword(Keyword::DEFAULT) {
            return Some(Ok(Statement::Use(Use::Default)));
        }
        None
    }

    fn parse_create_function(
        &self,
        parser: &mut Parser,
        _or_alter: bool,
        or_replace: bool,
        temporary: bool,
    ) -> Option<Result<Statement, ParserError>> {
        Some(
            parser
                .parse_hive_create_function(or_replace, temporary)
                .map(Into::into),
        )
    }

    fn is_delimited_identifier_start(&self, ch: char) -> bool {
        (ch == '"') || (ch == '`')
    }

    fn supports_hash_comment(&self) -> bool {
        true
    }

    /// The alias is an `identifier`, see `selectItem` in
    /// <https://github.com/apache/hive/blob/master/parser/src/java/org/apache/hadoop/hive/ql/parse/SelectClauseParser.g>
    fn supports_string_literal_column_alias(&self, _explicit: bool) -> bool {
        false
    }

    /// The alias is an `identifier`, see `tableSource` in
    /// <https://github.com/apache/hive/blob/master/parser/src/java/org/apache/hadoop/hive/ql/parse/FromClauseParser.g>
    fn supports_string_literal_table_alias(&self, _explicit: bool) -> bool {
        false
    }

    /// See <https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-RulesforColumnNames>
    fn identifier_quote_style(&self, _identifier: &str) -> Option<char> {
        Some('`')
    }

    fn is_identifier_start(&self, ch: char) -> bool {
        ch.is_ascii_lowercase() || ch.is_ascii_uppercase() || ch.is_ascii_digit() || ch == '$'
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        ch.is_ascii_lowercase()
            || ch.is_ascii_uppercase()
            || ch.is_ascii_digit()
            || ch == '_'
            || ch == '$'
            || ch == '{'
            || ch == '}'
    }

    /// LIMIT is not reserved in Hive; identifiers may be nonReserved keywords.
    /// See <https://github.com/apache/hive/blob/4e9caba7c02a947a39c60de427acb7189c8c1fe8/parser/src/java/org/apache/hadoop/hive/ql/parse/IdentifiersParser.g#L945>
    fn is_table_factor(&self, kw: &Keyword, _parser: &mut Parser) -> bool {
        kw == &Keyword::LIMIT || !RESERVED_FOR_TABLE_FACTOR.contains(kw)
    }

    fn supports_filter_during_aggregation(&self) -> bool {
        true
    }

    fn supports_numeric_prefix(&self) -> bool {
        true
    }

    fn require_interval_qualifier(&self) -> bool {
        true
    }

    /// See <https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=27362061#Tutorial-BuiltInOperators>
    fn supports_bang_not_operator(&self) -> bool {
        true
    }

    /// See <https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=27362036#LanguageManualDML-Loadingfilesintotables>
    fn supports_load_data(&self) -> bool {
        true
    }

    /// See <https://cwiki.apache.org/confluence/display/hive/languagemanual+sampling>
    fn supports_table_sample_before_alias(&self) -> bool {
        true
    }

    /// See <https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=30151323#EnhancedAggregation,Cube,GroupingandRollup-CubesandRollupsr>
    fn supports_group_by_with_modifier(&self) -> bool {
        true
    }

    // TODO: The parsing of the FROM keyword seems wrong, as it happens within the CTE.
    // See https://github.com/apache/datafusion-sqlparser-rs/issues/2236 for more details.
    /// See <https://hive.apache.org/docs/latest/language/common-table-expression/>
    fn supports_from_first_insert(&self) -> bool {
        true
    }

    /// See <https://hive.apache.org/docs/latest/language/languagemanual-types/>
    fn supports_map_literal_with_angle_brackets(&self) -> bool {
        true
    }
}

impl Parser<'_> {
    /// Parse `CREATE FUNCTION` for [Hive]
    ///
    /// [Hive]: https://cwiki.apache.org/confluence/display/hive/languagemanual+ddl#LanguageManualDDL-Create/Drop/ReloadFunction
    fn parse_hive_create_function(
        &mut self,
        or_replace: bool,
        temporary: bool,
    ) -> Result<CreateFunction, ParserError> {
        let name = self.parse_object_name(false)?;
        self.expect_keyword_is(Keyword::AS)?;

        let body = self.parse_create_function_body_string()?;
        let using = self.parse_optional_create_function_using()?;

        Ok(CreateFunction {
            or_alter: false,
            or_replace,
            temporary,
            name,
            function_body: Some(body),
            using,
            if_not_exists: false,
            args: None,
            return_type: None,
            behavior: None,
            called_on_null: None,
            parallel: None,
            security: None,
            leakproof: None,
            window: false,
            cost: None,
            rows: None,
            support: None,
            set_params: vec![],
            language: None,
            determinism_specifier: None,
            options: None,
            remote_connection: None,
        })
    }
}

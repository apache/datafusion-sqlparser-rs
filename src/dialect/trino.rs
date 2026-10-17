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
use crate::keywords::{self, Keyword};
use crate::parser::Parser;

/// Keywords in [`keywords::RESERVED_FOR_TABLE_ALIAS`] because of other dialects
/// that Trino accepts as a table alias and that cannot follow a table reference
/// in Trino's grammar. Words that can (`LIMIT`, `WINDOW`, `TABLESAMPLE`, ...)
/// stay reserved.
/// See <https://trino.io/docs/current/language/reserved.html>.
const RESERVED_EXCLUSIONS_FOR_TABLE_ALIAS: &[Keyword] = &[
    Keyword::ANALYZE,
    Keyword::ANTI,
    Keyword::ASOF,
    Keyword::CLUSTER,
    Keyword::CONNECT,
    Keyword::DISTRIBUTE,
    Keyword::EXPLAIN,
    Keyword::FORMAT,
    Keyword::GLOBAL,
    Keyword::LATERAL,
    Keyword::MATCH_CONDITION,
    Keyword::MINUS,
    Keyword::OPEN,
    Keyword::OUTPUT,
    Keyword::PIVOT,
    Keyword::PREWHERE,
    Keyword::QUALIFY,
    Keyword::RETURNING,
    Keyword::SAMPLE,
    Keyword::SEMI,
    Keyword::SETTINGS,
    Keyword::SORT,
    Keyword::START,
    Keyword::TOP,
    Keyword::UNPIVOT,
    Keyword::VIEW,
];

/// The same for [`keywords::RESERVED_FOR_COLUMN_ALIAS`].
const RESERVED_EXCLUSIONS_FOR_COLUMN_ALIAS: &[Keyword] = &[
    Keyword::ANALYZE,
    Keyword::CLUSTER,
    Keyword::DISTRIBUTE,
    Keyword::EXCLUDE,
    Keyword::EXPLAIN,
    Keyword::LATERAL,
    Keyword::MINUS,
    Keyword::RETURNING,
    Keyword::SORT,
    Keyword::TOP,
    Keyword::VIEW,
];

/// A [`Dialect`] for [Trino](https://trino.io/docs/current/language.html).
///
/// Each enabled feature below is covered by `tests/sqlparser_trino.rs`,
/// whose statements carry the verdict of a running Trino.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct TrinoDialect;

impl Dialect for TrinoDialect {
    fn is_table_alias(&self, kw: &Keyword, _parser: &mut Parser) -> bool {
        !keywords::RESERVED_FOR_TABLE_ALIAS.contains(kw)
            || RESERVED_EXCLUSIONS_FOR_TABLE_ALIAS.contains(kw)
    }

    fn is_column_alias(&self, kw: &Keyword, _parser: &mut Parser) -> bool {
        !keywords::RESERVED_FOR_COLUMN_ALIAS.contains(kw)
            || RESERVED_EXCLUSIONS_FOR_COLUMN_ALIAS.contains(kw)
    }

    /// Only double quotes delimit identifiers; backquotes are a syntax error.
    fn is_delimited_identifier_start(&self, ch: char) -> bool {
        ch == '"'
    }

    fn is_identifier_start(&self, ch: char) -> bool {
        ch.is_ascii_alphabetic() || ch == '_'
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        ch.is_ascii_alphanumeric() || ch == '_'
    }

    fn supports_filter_during_aggregation(&self) -> bool {
        true
    }

    fn supports_group_by_expr(&self) -> bool {
        true
    }

    fn supports_lambda_functions(&self) -> bool {
        true
    }

    fn supports_match_recognize(&self) -> bool {
        true
    }

    fn supports_explain_with_utility_options(&self) -> bool {
        true
    }

    fn supports_named_fn_args_with_rarrow_operator(&self) -> bool {
        true
    }

    fn supports_comment_on(&self) -> bool {
        true
    }

    /// `SHOW TABLES FROM s LIKE 'x'`: the source comes before the pattern.
    fn supports_show_like_before_in(&self) -> bool {
        false
    }
}

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
use alloc::boxed::Box;

use crate::ast::BinaryOperator;
use crate::ast::{Expr, Statement};
use crate::dialect::{Dialect, IsNotNullAlias};
use crate::keywords::Keyword;
use crate::parser::{Parser, ParserError};

/// A [`Dialect`] for [SQLite](https://www.sqlite.org)
///
/// This dialect allows columns in a
/// [`CREATE TABLE`](https://sqlite.org/lang_createtable.html) statement with no
/// type specified, as in `CREATE TABLE t1 (a)`. In the AST, these columns will
/// have the data type [`Unspecified`](crate::ast::DataType::Unspecified).
#[derive(Debug)]
pub struct SQLiteDialect {}

impl Dialect for SQLiteDialect {
    // see https://www.sqlite.org/lang_keywords.html
    // parse `...`, [...] and "..." as identifier
    // TODO: support depending on the context tread '...' as identifier too.
    fn is_delimited_identifier_start(&self, ch: char) -> bool {
        ch == '`' || ch == '"' || ch == '['
    }

    fn identifier_quote_style(&self, _identifier: &str) -> Option<char> {
        Some('`')
    }

    fn is_identifier_start(&self, ch: char) -> bool {
        // See https://www.sqlite.org/draft/tokenreq.html
        ch.is_ascii_lowercase()
            || ch.is_ascii_uppercase()
            || ch == '_'
            || ('\u{007f}'..='\u{ffff}').contains(&ch)
    }

    fn supports_filter_during_aggregation(&self) -> bool {
        true
    }

    fn supports_start_transaction_modifier(&self) -> bool {
        true
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        self.is_identifier_start(ch) || ch.is_ascii_digit()
    }

    fn parse_statement(&self, parser: &mut Parser) -> Option<Result<Statement, ParserError>> {
        if parser.parse_keyword(Keyword::REPLACE) {
            parser.prev_token();
            Some(parser.parse_insert())
        } else {
            None
        }
    }

    fn parse_infix(
        &self,
        parser: &mut crate::parser::Parser,
        expr: &crate::ast::Expr,
        _precedence: u8,
    ) -> Option<Result<crate::ast::Expr, ParserError>> {
        // Parse MATCH and REGEXP as operators
        // See <https://www.sqlite.org/lang_expr.html#the_like_glob_regexp_match_and_extract_operators>
        for (keyword, op) in [
            (Keyword::REGEXP, BinaryOperator::Regexp),
            (Keyword::MATCH, BinaryOperator::Match),
        ] {
            if parser.parse_keyword(keyword) {
                let left = Box::new(expr.clone());
                let right = Box::new(parser.parse_expr().unwrap());
                return Some(Ok(Expr::BinaryOp { left, op, right }));
            }
        }
        None
    }

    fn supports_in_empty_list(&self) -> bool {
        true
    }

    fn supports_limit_comma(&self) -> bool {
        true
    }

    fn supports_asc_desc_in_column_definition(&self) -> bool {
        true
    }

    fn supports_dollar_placeholder(&self) -> bool {
        true
    }

    /// SQLite supports ``NOT NULL` and `NOTNULL` as
    /// aliases for `IS NOT NULL`, see: <https://sqlite.org/syntax/expr.html>
    fn supports_is_not_null_alias(&self, alias: IsNotNullAlias) -> bool {
        match alias {
            IsNotNullAlias::NotNull => true,
            IsNotNullAlias::NotSpaceNull => true,
        }
    }
}

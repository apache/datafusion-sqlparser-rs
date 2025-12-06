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

use log::debug;

use crate::{
    parser::{Parser, ParserError},
    tokenizer::Token,
};

use super::{Dialect, Precedence};

/// A [`Dialect`] for [Oracle Databases](https://docs.oracle.com/en/database/oracle/oracle-database/21/sqlrf/index.html)
#[derive(Debug)]
pub struct OracleDialect;

impl Dialect for OracleDialect {
    // ~ appears not to be called anywhere
    fn identifier_quote_style(&self, _identifier: &str) -> Option<char> {
        Some('"')
    }

    fn is_delimited_identifier_start(&self, ch: char) -> bool {
        ch == '"'
    }

    fn is_identifier_start(&self, ch: char) -> bool {
        ch.is_alphabetic()
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        ch.is_alphanumeric() || ch == '_' || ch == '$' || ch == '#' || ch == '@'
    }

    fn supports_outer_join_operator(&self) -> bool {
        true
    }

    fn supports_connect_by(&self) -> bool {
        true
    }

    fn supports_execute_immediate(&self) -> bool {
        true
    }

    fn supports_match_recognize(&self) -> bool {
        true
    }

    fn supports_window_function_null_treatment_arg(&self) -> bool {
        true
    }

    fn supports_boolean_literals(&self) -> bool {
        false
    }

    fn supports_comment_on(&self) -> bool {
        true
    }

    fn supports_create_table_select(&self) -> bool {
        true
    }

    fn supports_set_stmt_without_operator(&self) -> bool {
        true
    }

    fn get_next_precedence(&self, _parser: &Parser) -> Option<Result<u8, ParserError>> {
        let t = _parser.peek_token();
        debug!("get_next_precedence() {t:?}");

        match t.token {
            Token::StringConcat => Some(Ok(self.prec_value(Precedence::PlusMinus))),
            _ => None,
        }
    }

    fn prec_value(&self, prec: Precedence) -> u8 {
        match prec {
            Precedence::Period => 100,
            Precedence::DoubleColon => 50,
            Precedence::AtTz => 41,
            Precedence::MulDivModOp => 40,
            Precedence::PlusMinus => 30,
            Precedence::Xor => 24,
            Precedence::Ampersand => 23,
            Precedence::Caret => 22,
            Precedence::Pipe => 21,
            Precedence::Between | Precedence::Eq | Precedence::Like | Precedence::Is => 20,
            Precedence::PgOther => 16,
            Precedence::UnaryNot => 15,
            Precedence::And => 10,
            Precedence::Or => 5,
        }
    }

    fn supports_group_by_expr(&self) -> bool {
        true
    }
}

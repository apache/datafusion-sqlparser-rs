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

use crate::{
    ast::{JoinConstraint, JoinOperator},
    dialect::Dialect,
};

/// A [`Dialect`] for [Hive](https://hive.apache.org/).
#[derive(Debug)]
pub struct HiveDialect {}

impl Dialect for HiveDialect {
    fn is_delimited_identifier_start(&self, ch: char) -> bool {
        (ch == '"') || (ch == '`')
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

    fn supports_filter_during_aggregation(&self) -> bool {
        true
    }

    fn supports_numeric_prefix(&self) -> bool {
        true
    }

    fn require_interval_qualifier(&self) -> bool {
        true
    }

    /// See Hive <https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=27362061#Tutorial-BuiltInOperators>
    fn supports_bang_not_operator(&self) -> bool {
        true
    }

    /// See Hive <https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=27362036#LanguageManualDML-Loadingfilesintotables>
    fn supports_load_data(&self) -> bool {
        true
    }

    // https://cwiki.apache.org/confluence/display/hive/languagemanual+joins
    fn verify_join_operator(&self, join_operator: &JoinOperator) -> bool {
        match join_operator {
            JoinOperator::Inner(_)
            | JoinOperator::LeftOuter(_)
            | JoinOperator::RightOuter(_)
            | JoinOperator::FullOuter(_)
            | JoinOperator::CrossJoin
            | JoinOperator::Semi(_)
            | JoinOperator::LeftSemi(_) => true,
            _ => false,
        }
    }

    fn verify_join_constraint(&self, join_operator: &JoinOperator) -> bool {
        match join_operator.constraint() {
            JoinConstraint::Natural => false,
            JoinConstraint::On(_) | JoinConstraint::Using(_) => matches!(
                join_operator,
                JoinOperator::Inner(_)
                    | JoinOperator::LeftOuter(_)
                    | JoinOperator::RightOuter(_)
                    | JoinOperator::FullOuter(_)
                    | JoinOperator::Semi(_)
                    | JoinOperator::LeftSemi(_)
            ),
            JoinConstraint::None => matches!(
                join_operator,
                JoinOperator::Inner(_) | JoinOperator::CrossJoin
            ),
        }
    }
}

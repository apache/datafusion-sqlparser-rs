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

/// A [`Dialect`] for [Teradata](https://docs.teradata.com/).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct TeradataDialect;

impl Dialect for TeradataDialect {
    /// See <https://docs.teradata.com/r/Enterprise_IntelliFlex_VMware/SQL-Fundamentals/Basic-SQL-Syntax/Object-Names>
    fn identifier_quote_style(&self, _identifier: &str) -> Option<char> {
        Some('"')
    }

    /// See <https://docs.teradata.com/r/Enterprise_IntelliFlex_VMware/SQL-Fundamentals/Basic-SQL-Syntax/Working-with-Unicode-Delimited-Identifiers>
    fn is_delimited_identifier_start(&self, ch: char) -> bool {
        ch == '"'
    }

    /// See <https://docs.teradata.com/r/Enterprise_IntelliFlex_VMware/International-Character-Set-Support/Managing-International-Language-Support/Object-Names/Rules-for-Object-Naming>
    fn is_identifier_start(&self, ch: char) -> bool {
        ch.is_alphabetic() || ch == '_' || ch == '#' || ch == '$'
    }

    // See <https://docs.teradata.com/r/Enterprise_IntelliFlex_VMware/SQL-Fundamentals/Basic-SQL-Syntax/Object-Names>
    fn is_identifier_part(&self, ch: char) -> bool {
        ch.is_alphanumeric() || self.is_identifier_start(ch)
    }

    /// See <https://docs.teradata.com/r/Enterprise_IntelliFlex_VMware/SQL-Data-Manipulation-Language/SELECT-Statements/GROUP-BY-Clause/GROUP-BY-Clause-Syntax>
    fn supports_group_by_expr(&self) -> bool {
        true
    }

    /// Teradata has no native `BOOLEAN` data type.
    ///
    /// See <https://docs.teradata.com/r/Enterprise_IntelliFlex_VMware/SQL-Data-Types-and-Literals>
    fn supports_boolean_literals(&self) -> bool {
        false
    }

    /// See <https://docs.teradata.com/r/Enterprise_IntelliFlex_VMware/SQL-Data-Types-and-Literals/Data-Literals/Interval-Literals>
    fn require_interval_qualifier(&self) -> bool {
        true
    }

    /// See <https://docs.teradata.com/r/Enterprise_IntelliFlex_VMware/SQL-Data-Definition-Language-Syntax-and-Examples/Comment-Help-and-Show-Statements/COMMENT-Comment-Placing-Form>
    fn supports_comment_on(&self) -> bool {
        true
    }

    /// See <https://docs.teradata.com/r/Enterprise_IntelliFlex_VMware/SQL-Data-Definition-Language-Syntax-and-Examples/Table-Statements/CREATE-TABLE-and-CREATE-TABLE-AS>
    fn supports_create_table_select(&self) -> bool {
        true
    }

    /// See <https://docs.teradata.com/r/Enterprise_IntelliFlex_VMware/SQL-Stored-Procedures-and-Embedded-SQL/Dynamic-Embedded-SQL-Statements/Dynamic-SQL-Statement-Syntax/EXECUTE-IMMEDIATE>
    fn supports_execute_immediate(&self) -> bool {
        true
    }

    /// See <https://docs.teradata.com/r/Enterprise_IntelliFlex_VMware/SQL-Data-Manipulation-Language/SELECT-Statements/Select-List-Syntax/TOP-Clause>
    fn supports_top_before_distinct(&self) -> bool {
        true
    }

    /// See <https://docs.teradata.com/r/Enterprise_IntelliFlex_VMware/SQL-Functions-Expressions-and-Predicates/Ordered-Analytical/Window-Aggregate-Functions>
    fn supports_window_function_null_treatment_arg(&self) -> bool {
        true
    }

    /// See <https://docs.teradata.com/r/Enterprise_IntelliFlex_VMware/SQL-Data-Types-and-Literals/Data-Literals/Character-String-Literals>
    fn supports_string_literal_concatenation(&self) -> bool {
        true
    }

    /// See <https://docs.teradata.com/r/Enterprise_IntelliFlex_VMware/SQL-Data-Definition-Language-Syntax-and-Examples/Table-Statements/CREATE-TABLE-and-CREATE-TABLE-AS>
    fn supports_leading_comma_before_table_options(&self) -> bool {
        true
    }
}

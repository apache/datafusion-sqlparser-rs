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

#[derive(Debug)]
pub struct CypherDialect {}

impl Dialect for CypherDialect {
    fn is_identifier_start(&self, ch: char) -> bool {
        // Cypher identifiers can start with a letter or underscore
        ch.is_ascii_alphabetic() || ch == '_'
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        // Cypher identifiers can contain letters, numbers, and underscores
        ch.is_ascii_alphanumeric() || ch == '_'
    }

    fn is_delimited_identifier_start(&self, ch: char) -> bool {
        // Cypher uses backticks for quoted identifiers
        ch == '`'
    }

    // fn is_delimited_identifier_part(&self, ch: char) -> bool {
    //     ch != '`'
    // }

    // fn is_string_literal_start(&self, ch: char) -> bool {
    //     // Cypher supports both single and double quotes for strings
    //     ch == '\'' || ch == '"'
    // }

    // fn supports_nested_table_aliases(&self) -> bool {
    //     false
    // }

    fn supports_numeric_prefix(&self) -> bool {
        true
    }

    fn supports_range_notation(&self) -> bool {
        true
    }
}
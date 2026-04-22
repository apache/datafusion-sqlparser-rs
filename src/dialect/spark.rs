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

use crate::ast::{BinaryOperator, Expr};
use crate::dialect::Dialect;
use crate::keywords::Keyword;
use crate::parser::{Parser, ParserError};

/// A [`Dialect`] for [Apache Spark SQL](https://spark.apache.org/docs/latest/sql-ref.html).
///
/// See <https://spark.apache.org/docs/latest/sql-ref-syntax.html>.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SparkSqlDialect;

impl Dialect for SparkSqlDialect {
    // See https://spark.apache.org/docs/latest/sql-ref-identifier.html
    fn is_delimited_identifier_start(&self, ch: char) -> bool {
        matches!(ch, '`')
    }

    fn is_identifier_start(&self, ch: char) -> bool {
        matches!(ch, 'a'..='z' | 'A'..='Z' | '_')
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        matches!(ch, 'a'..='z' | 'A'..='Z' | '0'..='9' | '_')
    }

    /// See <https://spark.apache.org/docs/latest/sql-ref-functions-builtin-agg.html>
    fn supports_filter_during_aggregation(&self) -> bool {
        true
    }

    /// See <https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-groupby.html>
    fn supports_group_by_expr(&self) -> bool {
        true
    }

    /// See <https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-groupby.html>
    fn supports_group_by_with_modifier(&self) -> bool {
        true
    }

    /// See <https://spark.apache.org/docs/latest/sql-ref-functions-builtin-higher-order-func.html>
    fn supports_lambda_functions(&self) -> bool {
        true
    }

    /// See <https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select.html>
    fn supports_select_wildcard_except(&self) -> bool {
        true
    }

    /// See <https://spark.apache.org/docs/latest/sql-ref-datatypes.html>
    fn supports_struct_literal(&self) -> bool {
        true
    }

    fn supports_nested_comments(&self) -> bool {
        true
    }

    /// See <https://spark.apache.org/docs/latest/sql-ref-syntax-ddl-create-table-datasource.html>
    fn supports_create_table_using(&self) -> bool {
        true
    }

    /// `LONG` is an alias for `BIGINT` in Spark SQL.
    ///
    /// See <https://spark.apache.org/docs/latest/sql-ref-datatypes.html>
    fn supports_long_type_as_bigint(&self) -> bool {
        true
    }

    /// See <https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select.html>
    fn supports_values_as_table_factor(&self) -> bool {
        true
    }

    fn require_interval_qualifier(&self) -> bool {
        true
    }

    fn supports_bang_not_operator(&self) -> bool {
        true
    }

    fn supports_select_item_multi_column_alias(&self) -> bool {
        true
    }

    fn supports_cte_without_as(&self) -> bool {
        true
    }

    /// See <https://spark.apache.org/docs/latest/sql-ref-datatypes.html>
    fn supports_map_literal_with_angle_brackets(&self) -> bool {
        true
    }

    /// Parse the `DIV` keyword as integer division.
    ///
    /// Example: `SELECT 10 DIV 3` returns `3`.
    ///
    /// See <https://spark.apache.org/docs/latest/sql-ref-functions-builtin-math.html>
    fn parse_infix(
        &self,
        parser: &mut Parser,
        expr: &Expr,
        _precedence: u8,
    ) -> Option<Result<Expr, ParserError>> {
        if parser.parse_keyword(Keyword::DIV) {
            let left = Box::new(expr.clone());
            let right = Box::new(match parser.parse_expr() {
                Ok(expr) => expr,
                Err(e) => return Some(Err(e)),
            });
            Some(Ok(Expr::BinaryOp {
                left,
                op: BinaryOperator::MyIntegerDivide,
                right,
            }))
        } else {
            None
        }
    }
}

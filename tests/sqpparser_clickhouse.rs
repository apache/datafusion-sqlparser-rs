// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![warn(clippy::all)]
//! Test SQL syntax specific to ClickHouse.

#[macro_use]
mod test_utils;

use test_utils::*;

use sqlparser::ast::Expr::{BinaryOp, Identifier, MapAccess};
use sqlparser::ast::Ident;
use sqlparser::ast::SelectItem::UnnamedExpr;
use sqlparser::ast::TableFactor::Table;
use sqlparser::ast::*;

use sqlparser::dialect::ClickHouseDialect;

#[test]
fn parse_map_access_expr() {
    let sql = r#"SELECT string_values[indexOf(string_names, 'endpoint')] FROM foos WHERE id = 'test' AND string_value[indexOf(string_name, 'app')] <> 'foo'"#;
    let select = clickhouse().verified_only_select(sql);
    assert_eq!(
        Select {
            distinct: false,
            top: None,
            projection: vec![UnnamedExpr(MapAccess {
                column: Box::new(Identifier(Ident {
                    value: "string_values".to_string(),
                    quote_style: None,
                })),
                keys: vec![Expr::Function(Function {
                    name: ObjectName(vec!["indexOf".into()]),
                    args: vec![
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Identifier(Ident::new(
                            "string_names"
                        )))),
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
                            Value::SingleQuotedString("endpoint".to_string())
                        ))),
                    ],
                    over: None,
                    distinct: false,
                })],
            })],
            from: vec![TableWithJoins {
                relation: Table {
                    name: ObjectName(vec![Ident::new("foos")]),
                    alias: None,
                    args: vec![],
                    with_hints: vec![],
                },
                joins: vec![]
            }],
            lateral_views: vec![],
            selection: Some(BinaryOp {
                left: Box::new(BinaryOp {
                    left: Box::new(Identifier(Ident::new("id"))),
                    op: BinaryOperator::Eq,
                    right: Box::new(Expr::Value(Value::SingleQuotedString("test".to_string())))
                }),
                op: BinaryOperator::And,
                right: Box::new(BinaryOp {
                    left: Box::new(MapAccess {
                        column: Box::new(Identifier(Ident::new("string_value"))),
                        keys: vec![Expr::Function(Function {
                            name: ObjectName(vec![Ident::new("indexOf")]),
                            args: vec![
                                FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Identifier(
                                    Ident::new("string_name")
                                ))),
                                FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
                                    Value::SingleQuotedString("app".to_string())
                                ))),
                            ],
                            over: None,
                            distinct: false
                        })]
                    }),
                    op: BinaryOperator::NotEq,
                    right: Box::new(Expr::Value(Value::SingleQuotedString("foo".to_string())))
                })
            }),
            group_by: vec![],
            cluster_by: vec![],
            distribute_by: vec![],
            sort_by: vec![],
            having: None
        },
        select
    );
}

#[test]
fn parse_array_expr() {
    let sql = "SELECT ['1', '2'] FROM test";
    let select = clickhouse().verified_only_select(sql);
    assert_eq!(
        &Expr::Array(Array {
            elem: vec![
                Expr::Value(Value::SingleQuotedString("1".to_string())),
                Expr::Value(Value::SingleQuotedString("2".to_string()))
            ],
            named: false,
        }),
        expr_from_projection(only(&select.projection))
    )
}

fn clickhouse() -> TestedDialects {
    TestedDialects {
        dialects: vec![Box::new(ClickHouseDialect {})],
    }
}

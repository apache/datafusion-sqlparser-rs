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

use sqlparser::ast::Expr::{Identifier, MapAccess};
use sqlparser::ast::*;

use sqlparser::dialect::ClickHouseDialect;

#[test]
fn parse_map_access_expr() {
    let sql = r#"SELECT string_values[indexOf(string_names, 'endpoint')] FROM foos"#;
    let select = clickhouse().verified_only_select(sql);
    assert_eq!(
        &MapAccess {
            column: Box::new(Identifier(Ident {
                value: "string_values".to_string(),
                quote_style: None
            })),
            keys: vec![Expr::Function(Function {
                name: ObjectName(vec!["indexOf".into()]),
                args: vec![
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Identifier(Ident::new(
                        "string_names"
                    )))),
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
                        Value::SingleQuotedString("endpoint".to_string())
                    )))
                ],
                over: None,
                distinct: false
            })]
        },
        expr_from_projection(only(&select.projection)),
    );
}

fn clickhouse() -> TestedDialects {
    TestedDialects {
        dialects: vec![Box::new(ClickHouseDialect {})],
    }
}

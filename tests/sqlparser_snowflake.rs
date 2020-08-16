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
use sqlparser::ast::*;
use sqlparser::dialect::{GenericDialect, SnowflakeDialect};
use sqlparser::test_utils::*;

#[test]
fn test_snowflake_create_table() {
    let sql = "CREATE TABLE _my_$table (am00unt number)";
    match snowflake_and_generic().verified_stmt(sql) {
        Statement::CreateTable { name, .. } => {
            assert_eq!("_my_$table", name.to_string());
        }
        _ => unreachable!(),
    }
}

#[test]
fn test_query_with_variable_name() {
    let sql = "SELECT $var1";
    let select = snowflake().verified_only_select(sql);

    assert_eq!(
        only(select.projection),
        SelectItem::UnnamedExpr(Expr::SqlVariable {
            prefix: '$',
            name: Ident::new("var1")
        },)
    );

    let sql = "SELECT c1 FROM t1 WHERE num BETWEEN $min AND $max";
    let select = snowflake().verified_only_select(sql);

    assert_eq!(
        select.selection.unwrap(),
        Expr::Between {
            expr: Box::new(Expr::Identifier("num".into())),
            low: Box::new(Expr::SqlVariable {
                prefix: '$',
                name: Ident::new("min")
            }),
            high: Box::new(Expr::SqlVariable {
                prefix: '$',
                name: Ident::new("max")
            }),
            negated: false,
        }
    );
}

fn snowflake() -> TestedDialects {
    TestedDialects {
        dialects: vec![Box::new(SnowflakeDialect {})],
    }
}

fn snowflake_and_generic() -> TestedDialects {
    TestedDialects {
        dialects: vec![Box::new(SnowflakeDialect {}), Box::new(GenericDialect {})],
    }
}

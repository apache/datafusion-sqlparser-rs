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

#[macro_use]
mod test_utils;

use test_utils::*;

use sqlparser::ast::*;
use sqlparser::dialect::BigQueryDialect;

#[test]
fn parse_literal_string() {
    let sql = r#"SELECT 'single', "double""#;
    let select = bigquery().verified_only_select(sql);
    assert_eq!(2, select.projection.len());
    assert_eq!(
        &Expr::Value(Value::SingleQuotedString("single".to_string())),
        expr_from_projection(&select.projection[0])
    );
    assert_eq!(
        &Expr::Value(Value::DoubleQuotedString("double".to_string())),
        expr_from_projection(&select.projection[1])
    );
}

#[test]
fn parse_table_identifiers() {
    fn test_table_ident(ident: &str, expected: Vec<Ident>) {
        let sql = format!("SELECT 1 FROM {}", ident);
        let select = bigquery().verified_only_select(&sql);
        assert_eq!(
            select.from,
            vec![TableWithJoins {
                relation: TableFactor::Table {
                    name: ObjectName(expected),
                    alias: None,
                    args: None,
                    with_hints: vec![],
                },
                joins: vec![]
            },]
        );
    }
    fn test_table_ident_err(ident: &str) {
        let sql = format!("SELECT 1 FROM {}", ident);
        assert!(bigquery().parse_sql_statements(&sql).is_err());
    }

    test_table_ident("da-sh-es", vec![Ident::new("da-sh-es")]);

    test_table_ident("`spa ce`", vec![Ident::with_quote('`', "spa ce")]);

    test_table_ident(
        "`!@#$%^&*()-=_+`",
        vec![Ident::with_quote('`', "!@#$%^&*()-=_+")],
    );

    test_table_ident(
        "_5abc.dataField",
        vec![Ident::new("_5abc"), Ident::new("dataField")],
    );
    test_table_ident(
        "`5abc`.dataField",
        vec![Ident::with_quote('`', "5abc"), Ident::new("dataField")],
    );

    test_table_ident_err("5abc.dataField");

    test_table_ident(
        "abc5.dataField",
        vec![Ident::new("abc5"), Ident::new("dataField")],
    );

    test_table_ident_err("abc5!.dataField");

    test_table_ident(
        "`GROUP`.dataField",
        vec![Ident::with_quote('`', "GROUP"), Ident::new("dataField")],
    );

    // TODO: this should be error
    // test_table_ident_err("GROUP.dataField");

    test_table_ident("abc5.GROUP", vec![Ident::new("abc5"), Ident::new("GROUP")]);
}

fn bigquery() -> TestedDialects {
    TestedDialects {
        dialects: vec![Box::new(BigQueryDialect {})],
    }
}

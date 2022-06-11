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

use sqlparser::ast::Expr::Identifier;
use sqlparser::ast::SelectItem::UnnamedExpr;
use sqlparser::ast::*;
use sqlparser::dialect::BigQueryDialect;

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

#[test]
fn parse_column_identifiers() {
    fn test_table_ident(column: &str, expected: Vec<SelectItem>) {
        let sql = format!("SELECT {} FROM a", column);
        let select = bigquery().verified_only_select(&sql);
        assert_eq!(select.projection, expected);
    }

    test_table_ident("1", vec![UnnamedExpr(Expr::Value(number("1")))]);
    test_table_ident(
        "`a`",
        vec![UnnamedExpr(Identifier(Ident::with_quote('`', "a")))],
    );
    test_table_ident(
        "'a'",
        vec![UnnamedExpr(Expr::Value(Value::SingleQuotedString(
            "a".to_string(),
        )))],
    );
    test_table_ident("a", vec![UnnamedExpr(Identifier(Ident::new("a")))]);
    test_table_ident(
        "\"a\"",
        vec![UnnamedExpr(Identifier(Ident::with_quote('"', "a")))],
    );
    test_table_ident("it's", vec![UnnamedExpr(Identifier(Ident::new("it's")))]);
}

fn bigquery() -> TestedDialects {
    TestedDialects {
        dialects: vec![Box::new(BigQueryDialect {})],
    }
}

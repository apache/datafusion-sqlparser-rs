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
//! Test SQL syntax specific to SQLite. The parser based on the
//! generic dialect is also tested (on the inputs it can handle).

#[macro_use]
mod test_utils;

use test_utils::*;

use sqlparser::ast::SelectItem::UnnamedExpr;
use sqlparser::ast::*;
use sqlparser::dialect::{GenericDialect, SQLiteDialect};
use sqlparser::tokenizer::Token;

#[test]
fn parse_create_table_without_rowid() {
    let sql = "CREATE TABLE t (a INT) WITHOUT ROWID";
    match sqlite_and_generic().verified_stmt(sql) {
        Statement::CreateTable {
            name,
            without_rowid: true,
            ..
        } => {
            assert_eq!("t", name.to_string());
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_virtual_table() {
    let sql = "CREATE VIRTUAL TABLE IF NOT EXISTS t USING module_name (arg1, arg2)";
    match sqlite_and_generic().verified_stmt(sql) {
        Statement::CreateVirtualTable {
            name,
            if_not_exists: true,
            module_name,
            module_args,
        } => {
            let args = vec![Ident::new("arg1"), Ident::new("arg2")];
            assert_eq!("t", name.to_string());
            assert_eq!("module_name", module_name.to_string());
            assert_eq!(args, module_args);
        }
        _ => unreachable!(),
    }

    let sql = "CREATE VIRTUAL TABLE t USING module_name";
    sqlite_and_generic().verified_stmt(sql);
}

#[test]
fn parse_create_table_auto_increment() {
    let sql = "CREATE TABLE foo (bar INT PRIMARY KEY AUTOINCREMENT)";
    match sqlite_and_generic().verified_stmt(sql) {
        Statement::CreateTable { name, columns, .. } => {
            assert_eq!(name.to_string(), "foo");
            assert_eq!(
                vec![ColumnDef {
                    name: "bar".into(),
                    data_type: DataType::Int(None),
                    collation: None,
                    options: vec![
                        ColumnOptionDef {
                            name: None,
                            option: ColumnOption::Unique { is_primary: true },
                        },
                        ColumnOptionDef {
                            name: None,
                            option: ColumnOption::DialectSpecific(vec![Token::make_keyword(
                                "AUTOINCREMENT"
                            )]),
                        },
                    ],
                }],
                columns
            );
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_sqlite_quote() {
    let sql = "CREATE TABLE `PRIMARY` (\"KEY\" INT, [INDEX] INT)";
    match sqlite().verified_stmt(sql) {
        Statement::CreateTable { name, columns, .. } => {
            assert_eq!(name.to_string(), "`PRIMARY`");
            assert_eq!(
                vec![
                    ColumnDef {
                        name: Ident::with_quote('"', "KEY"),
                        data_type: DataType::Int(None),
                        collation: None,
                        options: vec![],
                    },
                    ColumnDef {
                        name: Ident::with_quote('[', "INDEX"),
                        data_type: DataType::Int(None),
                        collation: None,
                        options: vec![],
                    },
                ],
                columns
            );
        }
        _ => unreachable!(),
    }
}

#[test]
fn test_placeholder() {
    // In postgres, this would be the absolute value operator '@' applied to the column 'xxx'
    // But in sqlite, this is a named parameter.
    // see https://www.sqlite.org/lang_expr.html#varparam
    let sql = "SELECT @xxx";
    let ast = sqlite().verified_only_select(sql);
    assert_eq!(
        ast.projection[0],
        UnnamedExpr(Expr::Value(Value::Placeholder("@xxx".into()))),
    );
}

#[test]
fn parse_like() {
    fn chk(negated: bool) {
        let sql = &format!(
            "SELECT * FROM customers WHERE name {}LIKE '%a'",
            if negated { "NOT " } else { "" }
        );
        let select = sqlite().verified_only_select(sql);
        assert_eq!(
            Expr::Like {
                expr: Box::new(Expr::Identifier(Ident::new("name"))),
                negated,
                pattern: Box::new(Expr::Value(Value::SingleQuotedString("%a".to_string()))),
                escape_char: None,
            },
            select.selection.unwrap()
        );

        // Test with escape char
        let sql = &format!(
            "SELECT * FROM customers WHERE name {}LIKE '%a' ESCAPE '\\'",
            if negated { "NOT " } else { "" }
        );
        let select = sqlite().verified_only_select(sql);
        assert_eq!(
            Expr::Like {
                expr: Box::new(Expr::Identifier(Ident::new("name"))),
                negated,
                pattern: Box::new(Expr::Value(Value::SingleQuotedString("%a".to_string()))),
                escape_char: Some('\\'),
            },
            select.selection.unwrap()
        );

        // This statement tests that LIKE and NOT LIKE have the same precedence.
        // This was previously mishandled (#81).
        let sql = &format!(
            "SELECT * FROM customers WHERE name {}LIKE '%a' IS NULL",
            if negated { "NOT " } else { "" }
        );
        let select = sqlite().verified_only_select(sql);
        assert_eq!(
            Expr::IsNull(Box::new(Expr::Like {
                expr: Box::new(Expr::Identifier(Ident::new("name"))),
                negated,
                pattern: Box::new(Expr::Value(Value::SingleQuotedString("%a".to_string()))),
                escape_char: None,
            })),
            select.selection.unwrap()
        );
    }
    chk(false);
    chk(true);
}

#[test]
fn parse_similar_to() {
    fn chk(negated: bool) {
        let sql = &format!(
            "SELECT * FROM customers WHERE name {}SIMILAR TO '%a'",
            if negated { "NOT " } else { "" }
        );
        let select = sqlite().verified_only_select(sql);
        assert_eq!(
            Expr::SimilarTo {
                expr: Box::new(Expr::Identifier(Ident::new("name"))),
                negated,
                pattern: Box::new(Expr::Value(Value::SingleQuotedString("%a".to_string()))),
                escape_char: None,
            },
            select.selection.unwrap()
        );

        // Test with escape char
        let sql = &format!(
            "SELECT * FROM customers WHERE name {}SIMILAR TO '%a' ESCAPE '\\'",
            if negated { "NOT " } else { "" }
        );
        let select = sqlite().verified_only_select(sql);
        assert_eq!(
            Expr::SimilarTo {
                expr: Box::new(Expr::Identifier(Ident::new("name"))),
                negated,
                pattern: Box::new(Expr::Value(Value::SingleQuotedString("%a".to_string()))),
                escape_char: Some('\\'),
            },
            select.selection.unwrap()
        );

        // This statement tests that SIMILAR TO and NOT SIMILAR TO have the same precedence.
        let sql = &format!(
            "SELECT * FROM customers WHERE name {}SIMILAR TO '%a' ESCAPE '\\' IS NULL",
            if negated { "NOT " } else { "" }
        );
        let select = sqlite().verified_only_select(sql);
        assert_eq!(
            Expr::IsNull(Box::new(Expr::SimilarTo {
                expr: Box::new(Expr::Identifier(Ident::new("name"))),
                negated,
                pattern: Box::new(Expr::Value(Value::SingleQuotedString("%a".to_string()))),
                escape_char: Some('\\'),
            })),
            select.selection.unwrap()
        );
    }
    chk(false);
    chk(true);
}

fn sqlite() -> TestedDialects {
    TestedDialects {
        dialects: vec![Box::new(SQLiteDialect {})],
    }
}

fn sqlite_and_generic() -> TestedDialects {
    TestedDialects {
        // we don't have a separate SQLite dialect, so test only the generic dialect for now
        dialects: vec![Box::new(SQLiteDialect {}), Box::new(GenericDialect {})],
    }
}

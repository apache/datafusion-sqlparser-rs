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
//! Test SQL syntax specific to MySQL. The parser based on the generic dialect
//! is also tested (on the inputs it can handle).

#[macro_use]
mod test_utils;

use test_utils::*;

use sqlparser::ast::*;
use sqlparser::dialect::{GenericDialect, MySqlDialect};
use sqlparser::tokenizer::Token;

#[test]
fn parse_identifiers() {
    mysql().verified_stmt("SELECT $a$, àà");
}

#[test]
fn parse_show_columns() {
    let table_name = ObjectName(vec![Ident::new("mytable")]);
    assert_eq!(
        mysql_and_generic().verified_stmt("SHOW COLUMNS FROM mytable"),
        Statement::ShowColumns {
            extended: false,
            full: false,
            table_name: table_name.clone(),
            filter: None,
        }
    );
    assert_eq!(
        mysql_and_generic().verified_stmt("SHOW COLUMNS FROM mydb.mytable"),
        Statement::ShowColumns {
            extended: false,
            full: false,
            table_name: ObjectName(vec![Ident::new("mydb"), Ident::new("mytable")]),
            filter: None,
        }
    );
    assert_eq!(
        mysql_and_generic().verified_stmt("SHOW EXTENDED COLUMNS FROM mytable"),
        Statement::ShowColumns {
            extended: true,
            full: false,
            table_name: table_name.clone(),
            filter: None,
        }
    );
    assert_eq!(
        mysql_and_generic().verified_stmt("SHOW FULL COLUMNS FROM mytable"),
        Statement::ShowColumns {
            extended: false,
            full: true,
            table_name: table_name.clone(),
            filter: None,
        }
    );
    assert_eq!(
        mysql_and_generic().verified_stmt("SHOW COLUMNS FROM mytable LIKE 'pattern'"),
        Statement::ShowColumns {
            extended: false,
            full: false,
            table_name: table_name.clone(),
            filter: Some(ShowStatementFilter::Like("pattern".into())),
        }
    );
    assert_eq!(
        mysql_and_generic().verified_stmt("SHOW COLUMNS FROM mytable WHERE 1 = 2"),
        Statement::ShowColumns {
            extended: false,
            full: false,
            table_name,
            filter: Some(ShowStatementFilter::Where(
                mysql_and_generic().verified_expr("1 = 2")
            )),
        }
    );
    mysql_and_generic()
        .one_statement_parses_to("SHOW FIELDS FROM mytable", "SHOW COLUMNS FROM mytable");
    mysql_and_generic()
        .one_statement_parses_to("SHOW COLUMNS IN mytable", "SHOW COLUMNS FROM mytable");
    mysql_and_generic()
        .one_statement_parses_to("SHOW FIELDS IN mytable", "SHOW COLUMNS FROM mytable");

    // unhandled things are truly unhandled
    match mysql_and_generic().parse_sql_statements("SHOW COLUMNS FROM mytable FROM mydb") {
        Err(_) => {}
        Ok(val) => panic!("unexpected successful parse: {:?}", val),
    }
}

#[test]
fn parse_show_create() {
    let obj_name = ObjectName(vec![Ident::new("myident")]);

    for obj_type in &vec![
        ShowCreateObject::Table,
        ShowCreateObject::Trigger,
        ShowCreateObject::Event,
        ShowCreateObject::Function,
        ShowCreateObject::Procedure,
    ] {
        assert_eq!(
            mysql_and_generic().verified_stmt(format!("SHOW CREATE {} myident", obj_type).as_str()),
            Statement::ShowCreate {
                obj_type: obj_type.clone(),
                obj_name: obj_name.clone(),
            }
        );
    }
}

#[test]
fn parse_create_table_auto_increment() {
    let sql = "CREATE TABLE foo (bar INT PRIMARY KEY AUTO_INCREMENT)";
    match mysql().verified_stmt(sql) {
        Statement::CreateTable { name, columns, .. } => {
            assert_eq!(name.to_string(), "foo");
            assert_eq!(
                vec![ColumnDef {
                    name: Ident::new("bar"),
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
                                "AUTO_INCREMENT"
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
fn parse_quote_identifiers() {
    let sql = "CREATE TABLE `PRIMARY` (`BEGIN` INT PRIMARY KEY)";
    match mysql().verified_stmt(sql) {
        Statement::CreateTable { name, columns, .. } => {
            assert_eq!(name.to_string(), "`PRIMARY`");
            assert_eq!(
                vec![ColumnDef {
                    name: Ident::with_quote('`', "BEGIN"),
                    data_type: DataType::Int(None),
                    collation: None,
                    options: vec![ColumnOptionDef {
                        name: None,
                        option: ColumnOption::Unique { is_primary: true },
                    }],
                }],
                columns
            );
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_table_with_minimum_display_width() {
    let sql = "CREATE TABLE foo (bar_tinyint TINYINT(3), bar_smallint SMALLINT(5), bar_int INT(11), bar_bigint BIGINT(20))";
    match mysql().verified_stmt(sql) {
        Statement::CreateTable { name, columns, .. } => {
            assert_eq!(name.to_string(), "foo");
            assert_eq!(
                vec![
                    ColumnDef {
                        name: Ident::new("bar_tinyint"),
                        data_type: DataType::TinyInt(Some(3)),
                        collation: None,
                        options: vec![],
                    },
                    ColumnDef {
                        name: Ident::new("bar_smallint"),
                        data_type: DataType::SmallInt(Some(5)),
                        collation: None,
                        options: vec![],
                    },
                    ColumnDef {
                        name: Ident::new("bar_int"),
                        data_type: DataType::Int(Some(11)),
                        collation: None,
                        options: vec![],
                    },
                    ColumnDef {
                        name: Ident::new("bar_bigint"),
                        data_type: DataType::BigInt(Some(20)),
                        collation: None,
                        options: vec![],
                    }
                ],
                columns
            );
        }
        _ => unreachable!(),
    }
}

fn mysql() -> TestedDialects {
    TestedDialects {
        dialects: vec![Box::new(MySqlDialect {})],
    }
}

fn mysql_and_generic() -> TestedDialects {
    TestedDialects {
        dialects: vec![Box::new(MySqlDialect {}), Box::new(GenericDialect {})],
    }
}

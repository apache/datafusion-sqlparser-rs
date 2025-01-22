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

#![warn(clippy::all)]
//! Test SQL syntax specific to MySQL. The parser based on the generic dialect
//! is also tested (on the inputs it can handle).

use helpers::attached_token::AttachedToken;
use matches::assert_matches;

use sqlparser::ast::MysqlInsertPriority::{Delayed, HighPriority, LowPriority};
use sqlparser::ast::*;
use sqlparser::dialect::{GenericDialect, MySqlDialect};
use sqlparser::parser::{ParserError, ParserOptions};
use sqlparser::tokenizer::Span;
use sqlparser::tokenizer::Token;
use test_utils::*;

#[macro_use]
mod test_utils;

fn mysql() -> TestedDialects {
    TestedDialects::new(vec![Box::new(MySqlDialect {})])
}

fn mysql_and_generic() -> TestedDialects {
    TestedDialects::new(vec![Box::new(MySqlDialect {}), Box::new(GenericDialect {})])
}

#[test]
fn parse_identifiers() {
    mysql().verified_stmt("SELECT $a$, àà");
}

#[test]
fn parse_literal_string() {
    let sql = r#"SELECT 'single', "double""#;
    let select = mysql().verified_only_select(sql);
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
fn parse_flush() {
    assert_eq!(
        mysql_and_generic().verified_stmt("FLUSH OPTIMIZER_COSTS"),
        Statement::Flush {
            location: None,
            object_type: FlushType::OptimizerCosts,
            channel: None,
            read_lock: false,
            export: false,
            tables: vec![]
        }
    );
    assert_eq!(
        mysql_and_generic().verified_stmt("FLUSH BINARY LOGS"),
        Statement::Flush {
            location: None,
            object_type: FlushType::BinaryLogs,
            channel: None,
            read_lock: false,
            export: false,
            tables: vec![]
        }
    );
    assert_eq!(
        mysql_and_generic().verified_stmt("FLUSH ENGINE LOGS"),
        Statement::Flush {
            location: None,
            object_type: FlushType::EngineLogs,
            channel: None,
            read_lock: false,
            export: false,
            tables: vec![]
        }
    );
    assert_eq!(
        mysql_and_generic().verified_stmt("FLUSH ERROR LOGS"),
        Statement::Flush {
            location: None,
            object_type: FlushType::ErrorLogs,
            channel: None,
            read_lock: false,
            export: false,
            tables: vec![]
        }
    );
    assert_eq!(
        mysql_and_generic().verified_stmt("FLUSH NO_WRITE_TO_BINLOG GENERAL LOGS"),
        Statement::Flush {
            location: Some(FlushLocation::NoWriteToBinlog),
            object_type: FlushType::GeneralLogs,
            channel: None,
            read_lock: false,
            export: false,
            tables: vec![]
        }
    );
    assert_eq!(
        mysql_and_generic().verified_stmt("FLUSH RELAY LOGS FOR CHANNEL test"),
        Statement::Flush {
            location: None,
            object_type: FlushType::RelayLogs,
            channel: Some("test".to_string()),
            read_lock: false,
            export: false,
            tables: vec![]
        }
    );
    assert_eq!(
        mysql_and_generic().verified_stmt("FLUSH LOCAL SLOW LOGS"),
        Statement::Flush {
            location: Some(FlushLocation::Local),
            object_type: FlushType::SlowLogs,
            channel: None,
            read_lock: false,
            export: false,
            tables: vec![]
        }
    );
    assert_eq!(
        mysql_and_generic().verified_stmt("FLUSH TABLES `mek`.`table1`, table2"),
        Statement::Flush {
            location: None,
            object_type: FlushType::Tables,
            channel: None,
            read_lock: false,
            export: false,
            tables: vec![
                ObjectName::from(vec![
                    Ident {
                        value: "mek".to_string(),
                        quote_style: Some('`'),
                        span: Span::empty(),
                    },
                    Ident {
                        value: "table1".to_string(),
                        quote_style: Some('`'),
                        span: Span::empty(),
                    }
                ]),
                ObjectName::from(vec![Ident {
                    value: "table2".to_string(),
                    quote_style: None,
                    span: Span::empty(),
                }])
            ]
        }
    );
    assert_eq!(
        mysql_and_generic().verified_stmt("FLUSH TABLES WITH READ LOCK"),
        Statement::Flush {
            location: None,
            object_type: FlushType::Tables,
            channel: None,
            read_lock: true,
            export: false,
            tables: vec![]
        }
    );
    assert_eq!(
        mysql_and_generic().verified_stmt("FLUSH TABLES `mek`.`table1`, table2 WITH READ LOCK"),
        Statement::Flush {
            location: None,
            object_type: FlushType::Tables,
            channel: None,
            read_lock: true,
            export: false,
            tables: vec![
                ObjectName::from(vec![
                    Ident {
                        value: "mek".to_string(),
                        quote_style: Some('`'),
                        span: Span::empty(),
                    },
                    Ident {
                        value: "table1".to_string(),
                        quote_style: Some('`'),
                        span: Span::empty(),
                    }
                ]),
                ObjectName::from(vec![Ident {
                    value: "table2".to_string(),
                    quote_style: None,
                    span: Span::empty(),
                }])
            ]
        }
    );
    assert_eq!(
        mysql_and_generic().verified_stmt("FLUSH TABLES `mek`.`table1`, table2 FOR EXPORT"),
        Statement::Flush {
            location: None,
            object_type: FlushType::Tables,
            channel: None,
            read_lock: false,
            export: true,
            tables: vec![
                ObjectName::from(vec![
                    Ident {
                        value: "mek".to_string(),
                        quote_style: Some('`'),
                        span: Span::empty(),
                    },
                    Ident {
                        value: "table1".to_string(),
                        quote_style: Some('`'),
                        span: Span::empty(),
                    }
                ]),
                ObjectName::from(vec![Ident {
                    value: "table2".to_string(),
                    quote_style: None,
                    span: Span::empty(),
                }])
            ]
        }
    );
}

#[test]
fn parse_show_columns() {
    assert_eq!(
        mysql_and_generic().verified_stmt("SHOW COLUMNS FROM mytable"),
        Statement::ShowColumns {
            extended: false,
            full: false,
            show_options: ShowStatementOptions {
                show_in: Some(ShowStatementIn {
                    clause: ShowStatementInClause::FROM,
                    parent_type: None,
                    parent_name: Some(ObjectName::from(vec![Ident::new("mytable")])),
                }),
                filter_position: None,
                limit_from: None,
                limit: None,
                starts_with: None,
            }
        }
    );
    assert_eq!(
        mysql_and_generic().verified_stmt("SHOW COLUMNS FROM mydb.mytable"),
        Statement::ShowColumns {
            extended: false,
            full: false,
            show_options: ShowStatementOptions {
                show_in: Some(ShowStatementIn {
                    clause: ShowStatementInClause::FROM,
                    parent_type: None,
                    parent_name: Some(ObjectName::from(vec![
                        Ident::new("mydb"),
                        Ident::new("mytable")
                    ])),
                }),
                filter_position: None,
                limit_from: None,
                limit: None,
                starts_with: None,
            }
        }
    );
    assert_eq!(
        mysql_and_generic().verified_stmt("SHOW EXTENDED COLUMNS FROM mytable"),
        Statement::ShowColumns {
            extended: true,
            full: false,
            show_options: ShowStatementOptions {
                show_in: Some(ShowStatementIn {
                    clause: ShowStatementInClause::FROM,
                    parent_type: None,
                    parent_name: Some(ObjectName::from(vec![Ident::new("mytable")])),
                }),
                filter_position: None,
                limit_from: None,
                limit: None,
                starts_with: None,
            }
        }
    );
    assert_eq!(
        mysql_and_generic().verified_stmt("SHOW FULL COLUMNS FROM mytable"),
        Statement::ShowColumns {
            extended: false,
            full: true,
            show_options: ShowStatementOptions {
                show_in: Some(ShowStatementIn {
                    clause: ShowStatementInClause::FROM,
                    parent_type: None,
                    parent_name: Some(ObjectName::from(vec![Ident::new("mytable")])),
                }),
                filter_position: None,
                limit_from: None,
                limit: None,
                starts_with: None,
            }
        }
    );
    assert_eq!(
        mysql_and_generic().verified_stmt("SHOW COLUMNS FROM mytable LIKE 'pattern'"),
        Statement::ShowColumns {
            extended: false,
            full: false,
            show_options: ShowStatementOptions {
                show_in: Some(ShowStatementIn {
                    clause: ShowStatementInClause::FROM,
                    parent_type: None,
                    parent_name: Some(ObjectName::from(vec![Ident::new("mytable")])),
                }),
                filter_position: Some(ShowStatementFilterPosition::Suffix(
                    ShowStatementFilter::Like("pattern".into())
                )),
                limit_from: None,
                limit: None,
                starts_with: None,
            }
        }
    );
    assert_eq!(
        mysql_and_generic().verified_stmt("SHOW COLUMNS FROM mytable WHERE 1 = 2"),
        Statement::ShowColumns {
            extended: false,
            full: false,
            show_options: ShowStatementOptions {
                show_in: Some(ShowStatementIn {
                    clause: ShowStatementInClause::FROM,
                    parent_type: None,
                    parent_name: Some(ObjectName::from(vec![Ident::new("mytable")])),
                }),
                filter_position: Some(ShowStatementFilterPosition::Suffix(
                    ShowStatementFilter::Where(mysql_and_generic().verified_expr("1 = 2"))
                )),
                limit_from: None,
                limit: None,
                starts_with: None,
            }
        }
    );
    mysql_and_generic()
        .one_statement_parses_to("SHOW FIELDS FROM mytable", "SHOW COLUMNS FROM mytable");
    mysql_and_generic()
        .one_statement_parses_to("SHOW COLUMNS IN mytable", "SHOW COLUMNS IN mytable");
    mysql_and_generic()
        .one_statement_parses_to("SHOW FIELDS IN mytable", "SHOW COLUMNS IN mytable");
    mysql_and_generic().one_statement_parses_to(
        "SHOW COLUMNS FROM mytable FROM mydb",
        "SHOW COLUMNS FROM mydb.mytable",
    );
}

#[test]
fn parse_show_status() {
    assert_eq!(
        mysql_and_generic().verified_stmt("SHOW SESSION STATUS LIKE 'ssl_cipher'"),
        Statement::ShowStatus {
            filter: Some(ShowStatementFilter::Like("ssl_cipher".into())),
            session: true,
            global: false
        }
    );
    assert_eq!(
        mysql_and_generic().verified_stmt("SHOW GLOBAL STATUS LIKE 'ssl_cipher'"),
        Statement::ShowStatus {
            filter: Some(ShowStatementFilter::Like("ssl_cipher".into())),
            session: false,
            global: true
        }
    );
    assert_eq!(
        mysql_and_generic().verified_stmt("SHOW STATUS WHERE value = 2"),
        Statement::ShowStatus {
            filter: Some(ShowStatementFilter::Where(
                mysql_and_generic().verified_expr("value = 2")
            )),
            session: false,
            global: false
        }
    );
}

#[test]
fn parse_show_tables() {
    assert_eq!(
        mysql_and_generic().verified_stmt("SHOW TABLES"),
        Statement::ShowTables {
            terse: false,
            history: false,
            extended: false,
            full: false,
            external: false,
            show_options: ShowStatementOptions {
                starts_with: None,
                limit: None,
                limit_from: None,
                show_in: None,
                filter_position: None
            }
        }
    );
    assert_eq!(
        mysql_and_generic().verified_stmt("SHOW TABLES FROM mydb"),
        Statement::ShowTables {
            terse: false,
            history: false,
            extended: false,
            full: false,
            external: false,
            show_options: ShowStatementOptions {
                starts_with: None,
                limit: None,
                limit_from: None,
                show_in: Some(ShowStatementIn {
                    clause: ShowStatementInClause::FROM,
                    parent_type: None,
                    parent_name: Some(ObjectName::from(vec![Ident::new("mydb")])),
                }),
                filter_position: None
            }
        }
    );
    assert_eq!(
        mysql_and_generic().verified_stmt("SHOW EXTENDED TABLES"),
        Statement::ShowTables {
            terse: false,
            history: false,
            extended: true,
            full: false,
            external: false,
            show_options: ShowStatementOptions {
                starts_with: None,
                limit: None,
                limit_from: None,
                show_in: None,
                filter_position: None
            }
        }
    );
    assert_eq!(
        mysql_and_generic().verified_stmt("SHOW FULL TABLES"),
        Statement::ShowTables {
            terse: false,
            history: false,
            extended: false,
            full: true,
            external: false,
            show_options: ShowStatementOptions {
                starts_with: None,
                limit: None,
                limit_from: None,
                show_in: None,
                filter_position: None
            }
        }
    );
    assert_eq!(
        mysql_and_generic().verified_stmt("SHOW TABLES LIKE 'pattern'"),
        Statement::ShowTables {
            terse: false,
            history: false,
            extended: false,
            full: false,
            external: false,
            show_options: ShowStatementOptions {
                starts_with: None,
                limit: None,
                limit_from: None,
                show_in: None,
                filter_position: Some(ShowStatementFilterPosition::Suffix(
                    ShowStatementFilter::Like("pattern".into())
                ))
            }
        }
    );
    assert_eq!(
        mysql_and_generic().verified_stmt("SHOW TABLES WHERE 1 = 2"),
        Statement::ShowTables {
            terse: false,
            history: false,
            extended: false,
            full: false,
            external: false,
            show_options: ShowStatementOptions {
                starts_with: None,
                limit: None,
                limit_from: None,
                show_in: None,
                filter_position: Some(ShowStatementFilterPosition::Suffix(
                    ShowStatementFilter::Where(mysql_and_generic().verified_expr("1 = 2"))
                ))
            }
        }
    );
    mysql_and_generic().verified_stmt("SHOW TABLES IN mydb");
    mysql_and_generic().verified_stmt("SHOW TABLES FROM mydb");
}

#[test]
fn parse_show_extended_full() {
    assert!(mysql_and_generic()
        .parse_sql_statements("SHOW EXTENDED FULL TABLES")
        .is_ok());
    assert!(mysql_and_generic()
        .parse_sql_statements("SHOW EXTENDED FULL COLUMNS FROM mytable")
        .is_ok());
    // SHOW EXTENDED/FULL can only be used with COLUMNS and TABLES
    assert!(mysql_and_generic()
        .parse_sql_statements("SHOW EXTENDED FULL CREATE TABLE mytable")
        .is_err());
    assert!(mysql_and_generic()
        .parse_sql_statements("SHOW EXTENDED FULL COLLATION")
        .is_err());
    assert!(mysql_and_generic()
        .parse_sql_statements("SHOW EXTENDED FULL VARIABLES")
        .is_err());
}

#[test]
fn parse_show_create() {
    let obj_name = ObjectName::from(vec![Ident::new("myident")]);

    for obj_type in &[
        ShowCreateObject::Table,
        ShowCreateObject::Trigger,
        ShowCreateObject::Event,
        ShowCreateObject::Function,
        ShowCreateObject::Procedure,
        ShowCreateObject::View,
    ] {
        assert_eq!(
            mysql_and_generic().verified_stmt(format!("SHOW CREATE {obj_type} myident").as_str()),
            Statement::ShowCreate {
                obj_type: *obj_type,
                obj_name: obj_name.clone(),
            }
        );
    }
}

#[test]
fn parse_show_collation() {
    assert_eq!(
        mysql_and_generic().verified_stmt("SHOW COLLATION"),
        Statement::ShowCollation { filter: None }
    );
    assert_eq!(
        mysql_and_generic().verified_stmt("SHOW COLLATION LIKE 'pattern'"),
        Statement::ShowCollation {
            filter: Some(ShowStatementFilter::Like("pattern".into())),
        }
    );
    assert_eq!(
        mysql_and_generic().verified_stmt("SHOW COLLATION WHERE 1 = 2"),
        Statement::ShowCollation {
            filter: Some(ShowStatementFilter::Where(
                mysql_and_generic().verified_expr("1 = 2")
            )),
        }
    );
}

#[test]
fn parse_use() {
    let valid_object_names = [
        "mydb",
        "SCHEMA",
        "DATABASE",
        "CATALOG",
        "WAREHOUSE",
        "DEFAULT",
    ];
    let quote_styles = ['\'', '"', '`'];
    for object_name in &valid_object_names {
        // Test single identifier without quotes
        assert_eq!(
            mysql_and_generic().verified_stmt(&format!("USE {}", object_name)),
            Statement::Use(Use::Object(ObjectName::from(vec![Ident::new(
                object_name.to_string()
            )])))
        );
        for &quote in &quote_styles {
            // Test single identifier with different type of quotes
            assert_eq!(
                mysql_and_generic()
                    .verified_stmt(&format!("USE {}{}{}", quote, object_name, quote)),
                Statement::Use(Use::Object(ObjectName::from(vec![Ident::with_quote(
                    quote,
                    object_name.to_string(),
                )])))
            );
        }
    }
}

#[test]
fn parse_set_variables() {
    mysql_and_generic().verified_stmt("SET sql_mode = CONCAT(@@sql_mode, ',STRICT_TRANS_TABLES')");
    assert_eq!(
        mysql_and_generic().verified_stmt("SET LOCAL autocommit = 1"),
        Statement::SetVariable {
            local: true,
            hivevar: false,
            variables: OneOrManyWithParens::One(ObjectName::from(vec!["autocommit".into()])),
            value: vec![Expr::Value(number("1"))],
        }
    );
}

#[test]
fn parse_create_table_auto_increment() {
    let sql = "CREATE TABLE foo (bar INT PRIMARY KEY AUTO_INCREMENT)";
    match mysql().verified_stmt(sql) {
        Statement::CreateTable(CreateTable { name, columns, .. }) => {
            assert_eq!(name.to_string(), "foo");
            assert_eq!(
                vec![ColumnDef {
                    name: Ident::new("bar"),
                    data_type: DataType::Int(None),
                    collation: None,
                    options: vec![
                        ColumnOptionDef {
                            name: None,
                            option: ColumnOption::Unique {
                                is_primary: true,
                                characteristics: None
                            },
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

/// if `unique_index_type_display` is `Some` create `TableConstraint::Unique`
///  otherwise create `TableConstraint::Primary`
fn table_constraint_unique_primary_ctor(
    name: Option<Ident>,
    index_name: Option<Ident>,
    index_type: Option<IndexType>,
    columns: Vec<Ident>,
    index_options: Vec<IndexOption>,
    characteristics: Option<ConstraintCharacteristics>,
    unique_index_type_display: Option<KeyOrIndexDisplay>,
) -> TableConstraint {
    match unique_index_type_display {
        Some(index_type_display) => TableConstraint::Unique {
            name,
            index_name,
            index_type_display,
            index_type,
            columns,
            index_options,
            characteristics,
            nulls_distinct: NullsDistinctOption::None,
        },
        None => TableConstraint::PrimaryKey {
            name,
            index_name,
            index_type,
            columns,
            index_options,
            characteristics,
        },
    }
}

#[test]
fn parse_create_table_primary_and_unique_key() {
    let sqls = ["UNIQUE KEY", "PRIMARY KEY"]
        .map(|key_ty| format!("CREATE TABLE foo (id INT PRIMARY KEY AUTO_INCREMENT, bar INT NOT NULL, CONSTRAINT bar_key {key_ty} (bar))"));

    let index_type_display = [Some(KeyOrIndexDisplay::Key), None];

    for (sql, index_type_display) in sqls.iter().zip(index_type_display) {
        match mysql().one_statement_parses_to(sql, "") {
            Statement::CreateTable(CreateTable {
                name,
                columns,
                constraints,
                ..
            }) => {
                assert_eq!(name.to_string(), "foo");

                let expected_constraint = table_constraint_unique_primary_ctor(
                    Some(Ident::new("bar_key")),
                    None,
                    None,
                    vec![Ident::new("bar")],
                    vec![],
                    None,
                    index_type_display,
                );
                assert_eq!(vec![expected_constraint], constraints);

                assert_eq!(
                    vec![
                        ColumnDef {
                            name: Ident::new("id"),
                            data_type: DataType::Int(None),
                            collation: None,
                            options: vec![
                                ColumnOptionDef {
                                    name: None,
                                    option: ColumnOption::Unique {
                                        is_primary: true,
                                        characteristics: None
                                    },
                                },
                                ColumnOptionDef {
                                    name: None,
                                    option: ColumnOption::DialectSpecific(vec![
                                        Token::make_keyword("AUTO_INCREMENT")
                                    ]),
                                },
                            ],
                        },
                        ColumnDef {
                            name: Ident::new("bar"),
                            data_type: DataType::Int(None),
                            collation: None,
                            options: vec![ColumnOptionDef {
                                name: None,
                                option: ColumnOption::NotNull,
                            },],
                        },
                    ],
                    columns
                );
            }
            _ => unreachable!(),
        }
    }
}

#[test]
fn parse_create_table_primary_and_unique_key_with_index_options() {
    let sqls = ["UNIQUE INDEX", "PRIMARY KEY"]
        .map(|key_ty| format!("CREATE TABLE foo (bar INT, var INT, CONSTRAINT constr {key_ty} index_name (bar, var) USING HASH COMMENT 'yes, ' USING BTREE COMMENT 'MySQL allows')"));

    let index_type_display = [Some(KeyOrIndexDisplay::Index), None];

    for (sql, index_type_display) in sqls.iter().zip(index_type_display) {
        match mysql_and_generic().one_statement_parses_to(sql, "") {
            Statement::CreateTable(CreateTable {
                name, constraints, ..
            }) => {
                assert_eq!(name.to_string(), "foo");

                let expected_constraint = table_constraint_unique_primary_ctor(
                    Some(Ident::new("constr")),
                    Some(Ident::new("index_name")),
                    None,
                    vec![Ident::new("bar"), Ident::new("var")],
                    vec![
                        IndexOption::Using(IndexType::Hash),
                        IndexOption::Comment("yes, ".into()),
                        IndexOption::Using(IndexType::BTree),
                        IndexOption::Comment("MySQL allows".into()),
                    ],
                    None,
                    index_type_display,
                );
                assert_eq!(vec![expected_constraint], constraints);
            }
            _ => unreachable!(),
        }

        mysql_and_generic().verified_stmt(sql);
    }
}

#[test]
fn parse_create_table_primary_and_unique_key_with_index_type() {
    let sqls = ["UNIQUE", "PRIMARY KEY"].map(|key_ty| {
        format!("CREATE TABLE foo (bar INT, {key_ty} index_name USING BTREE (bar) USING HASH)")
    });

    let index_type_display = [Some(KeyOrIndexDisplay::None), None];

    for (sql, index_type_display) in sqls.iter().zip(index_type_display) {
        match mysql_and_generic().one_statement_parses_to(sql, "") {
            Statement::CreateTable(CreateTable {
                name, constraints, ..
            }) => {
                assert_eq!(name.to_string(), "foo");

                let expected_constraint = table_constraint_unique_primary_ctor(
                    None,
                    Some(Ident::new("index_name")),
                    Some(IndexType::BTree),
                    vec![Ident::new("bar")],
                    vec![IndexOption::Using(IndexType::Hash)],
                    None,
                    index_type_display,
                );
                assert_eq!(vec![expected_constraint], constraints);
            }
            _ => unreachable!(),
        }
        mysql_and_generic().verified_stmt(sql);
    }

    let sql = "CREATE TABLE foo (bar INT, UNIQUE INDEX index_name USING BTREE (bar) USING HASH)";
    mysql_and_generic().verified_stmt(sql);
    let sql = "CREATE TABLE foo (bar INT, PRIMARY KEY index_name USING BTREE (bar) USING HASH)";
    mysql_and_generic().verified_stmt(sql);
}

#[test]
fn parse_create_table_primary_and_unique_key_characteristic_test() {
    let sqls = ["UNIQUE INDEX", "PRIMARY KEY"]
        .map(|key_ty| format!("CREATE TABLE x (y INT, CONSTRAINT constr {key_ty} (y) NOT DEFERRABLE INITIALLY IMMEDIATE)"));
    for sql in &sqls {
        mysql_and_generic().verified_stmt(sql);
    }
}

#[test]
fn parse_create_table_comment() {
    let without_equal = "CREATE TABLE foo (bar INT) COMMENT 'baz'";
    let with_equal = "CREATE TABLE foo (bar INT) COMMENT = 'baz'";

    for sql in [without_equal, with_equal] {
        match mysql().verified_stmt(sql) {
            Statement::CreateTable(CreateTable { name, comment, .. }) => {
                assert_eq!(name.to_string(), "foo");
                assert_eq!(comment.expect("Should exist").to_string(), "baz");
            }
            _ => unreachable!(),
        }
    }
}

#[test]
fn parse_create_table_auto_increment_offset() {
    let canonical =
        "CREATE TABLE foo (bar INT NOT NULL AUTO_INCREMENT) ENGINE=InnoDB AUTO_INCREMENT 123";
    let with_equal =
        "CREATE TABLE foo (bar INT NOT NULL AUTO_INCREMENT) ENGINE=InnoDB AUTO_INCREMENT=123";

    for sql in [canonical, with_equal] {
        match mysql().one_statement_parses_to(sql, canonical) {
            Statement::CreateTable(CreateTable {
                name,
                auto_increment_offset,
                ..
            }) => {
                assert_eq!(name.to_string(), "foo");
                assert_eq!(
                    auto_increment_offset.expect("Should exist").to_string(),
                    "123"
                );
            }
            _ => unreachable!(),
        }
    }
}

#[test]
fn parse_create_table_set_enum() {
    let sql = "CREATE TABLE foo (bar SET('a', 'b'), baz ENUM('a', 'b'))";
    match mysql().verified_stmt(sql) {
        Statement::CreateTable(CreateTable { name, columns, .. }) => {
            assert_eq!(name.to_string(), "foo");
            assert_eq!(
                vec![
                    ColumnDef {
                        name: Ident::new("bar"),
                        data_type: DataType::Set(vec!["a".to_string(), "b".to_string()]),
                        collation: None,
                        options: vec![],
                    },
                    ColumnDef {
                        name: Ident::new("baz"),
                        data_type: DataType::Enum(
                            vec![
                                EnumMember::Name("a".to_string()),
                                EnumMember::Name("b".to_string())
                            ],
                            None
                        ),
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

#[test]
fn parse_create_table_engine_default_charset() {
    let sql = "CREATE TABLE foo (id INT(11)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3";
    match mysql().verified_stmt(sql) {
        Statement::CreateTable(CreateTable {
            name,
            columns,
            engine,
            default_charset,
            ..
        }) => {
            assert_eq!(name.to_string(), "foo");
            assert_eq!(
                vec![ColumnDef {
                    name: Ident::new("id"),
                    data_type: DataType::Int(Some(11)),
                    collation: None,
                    options: vec![],
                },],
                columns
            );
            assert_eq!(
                engine,
                Some(TableEngine {
                    name: "InnoDB".to_string(),
                    parameters: None
                })
            );
            assert_eq!(default_charset, Some("utf8mb3".to_string()));
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_table_collate() {
    let sql = "CREATE TABLE foo (id INT(11)) COLLATE=utf8mb4_0900_ai_ci";
    match mysql().verified_stmt(sql) {
        Statement::CreateTable(CreateTable {
            name,
            columns,
            collation,
            ..
        }) => {
            assert_eq!(name.to_string(), "foo");
            assert_eq!(
                vec![ColumnDef {
                    name: Ident::new("id"),
                    data_type: DataType::Int(Some(11)),
                    collation: None,
                    options: vec![],
                },],
                columns
            );
            assert_eq!(collation, Some("utf8mb4_0900_ai_ci".to_string()));
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_table_both_options_and_as_query() {
    let sql = "CREATE TABLE foo (id INT(11)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb4_0900_ai_ci AS SELECT 1";
    match mysql_and_generic().verified_stmt(sql) {
        Statement::CreateTable(CreateTable {
            name,
            collation,
            query,
            ..
        }) => {
            assert_eq!(name.to_string(), "foo");
            assert_eq!(collation, Some("utf8mb4_0900_ai_ci".to_string()));
            assert_eq!(
                query.unwrap().body.as_select().unwrap().projection,
                vec![SelectItem::UnnamedExpr(Expr::Value(number("1")))]
            );
        }
        _ => unreachable!(),
    }

    let sql = r"CREATE TABLE foo (id INT(11)) ENGINE=InnoDB AS SELECT 1 DEFAULT CHARSET=utf8mb3";
    assert!(matches!(
        mysql_and_generic().parse_sql_statements(sql),
        Err(ParserError::ParserError(_))
    ));
}

#[test]
fn parse_create_table_comment_character_set() {
    let sql = "CREATE TABLE foo (s TEXT CHARACTER SET utf8mb4 COMMENT 'comment')";
    match mysql().verified_stmt(sql) {
        Statement::CreateTable(CreateTable { name, columns, .. }) => {
            assert_eq!(name.to_string(), "foo");
            assert_eq!(
                vec![ColumnDef {
                    name: Ident::new("s"),
                    data_type: DataType::Text,
                    collation: None,
                    options: vec![
                        ColumnOptionDef {
                            name: None,
                            option: ColumnOption::CharacterSet(ObjectName::from(vec![Ident::new(
                                "utf8mb4"
                            )]))
                        },
                        ColumnOptionDef {
                            name: None,
                            option: ColumnOption::Comment("comment".to_string())
                        }
                    ],
                },],
                columns
            );
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_table_gencol() {
    let sql_default = "CREATE TABLE t1 (a INT, b INT GENERATED ALWAYS AS (a * 2))";
    mysql_and_generic().verified_stmt(sql_default);

    let sql_virt = "CREATE TABLE t1 (a INT, b INT GENERATED ALWAYS AS (a * 2) VIRTUAL)";
    mysql_and_generic().verified_stmt(sql_virt);

    let sql_stored = "CREATE TABLE t1 (a INT, b INT GENERATED ALWAYS AS (a * 2) STORED)";
    mysql_and_generic().verified_stmt(sql_stored);

    mysql_and_generic().verified_stmt("CREATE TABLE t1 (a INT, b INT AS (a * 2))");
    mysql_and_generic().verified_stmt("CREATE TABLE t1 (a INT, b INT AS (a * 2) VIRTUAL)");
    mysql_and_generic().verified_stmt("CREATE TABLE t1 (a INT, b INT AS (a * 2) STORED)");
}

#[test]
fn parse_quote_identifiers() {
    let sql = "CREATE TABLE `PRIMARY` (`BEGIN` INT PRIMARY KEY)";
    match mysql().verified_stmt(sql) {
        Statement::CreateTable(CreateTable { name, columns, .. }) => {
            assert_eq!(name.to_string(), "`PRIMARY`");
            assert_eq!(
                vec![ColumnDef {
                    name: Ident::with_quote('`', "BEGIN"),
                    data_type: DataType::Int(None),
                    collation: None,
                    options: vec![ColumnOptionDef {
                        name: None,
                        option: ColumnOption::Unique {
                            is_primary: true,
                            characteristics: None
                        },
                    }],
                }],
                columns
            );
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_escaped_quote_identifiers_with_escape() {
    let sql = "SELECT `quoted `` identifier`";
    assert_eq!(
        TestedDialects::new(vec![Box::new(MySqlDialect {})]).verified_stmt(sql),
        Statement::Query(Box::new(Query {
            with: None,
            body: Box::new(SetExpr::Select(Box::new(Select {
                select_token: AttachedToken::empty(),
                distinct: None,
                top: None,
                top_before_distinct: false,
                projection: vec![SelectItem::UnnamedExpr(Expr::Identifier(Ident {
                    value: "quoted ` identifier".into(),
                    quote_style: Some('`'),
                    span: Span::empty(),
                }))],
                into: None,
                from: vec![],
                lateral_views: vec![],
                prewhere: None,
                selection: None,
                group_by: GroupByExpr::Expressions(vec![], vec![]),
                cluster_by: vec![],
                distribute_by: vec![],
                sort_by: vec![],
                having: None,
                named_window: vec![],
                qualify: None,
                window_before_qualify: false,
                value_table_mode: None,
                connect_by: None,
            }))),
            order_by: None,
            limit: None,
            limit_by: vec![],
            offset: None,
            fetch: None,
            locks: vec![],
            for_clause: None,
            settings: None,
            format_clause: None,
        }))
    );
}

#[test]
fn parse_escaped_quote_identifiers_with_no_escape() {
    let sql = "SELECT `quoted `` identifier`";
    assert_eq!(
        TestedDialects::new_with_options(
            vec![Box::new(MySqlDialect {})],
            ParserOptions {
                trailing_commas: false,
                unescape: false,
            }
        )
        .verified_stmt(sql),
        Statement::Query(Box::new(Query {
            with: None,
            body: Box::new(SetExpr::Select(Box::new(Select {
                select_token: AttachedToken::empty(),
                distinct: None,
                top: None,
                top_before_distinct: false,
                projection: vec![SelectItem::UnnamedExpr(Expr::Identifier(Ident {
                    value: "quoted `` identifier".into(),
                    quote_style: Some('`'),
                    span: Span::empty(),
                }))],
                into: None,
                from: vec![],
                lateral_views: vec![],
                prewhere: None,
                selection: None,
                group_by: GroupByExpr::Expressions(vec![], vec![]),
                cluster_by: vec![],
                distribute_by: vec![],
                sort_by: vec![],
                having: None,
                named_window: vec![],
                qualify: None,
                window_before_qualify: false,
                value_table_mode: None,
                connect_by: None,
            }))),
            order_by: None,
            limit: None,
            limit_by: vec![],
            offset: None,
            fetch: None,
            locks: vec![],
            for_clause: None,
            settings: None,
            format_clause: None,
        }))
    );
}

#[test]
fn parse_escaped_backticks_with_escape() {
    let sql = "SELECT ```quoted identifier```";
    assert_eq!(
        TestedDialects::new(vec![Box::new(MySqlDialect {})]).verified_stmt(sql),
        Statement::Query(Box::new(Query {
            with: None,
            body: Box::new(SetExpr::Select(Box::new(Select {
                select_token: AttachedToken::empty(),

                distinct: None,
                top: None,
                top_before_distinct: false,
                projection: vec![SelectItem::UnnamedExpr(Expr::Identifier(Ident {
                    value: "`quoted identifier`".into(),
                    quote_style: Some('`'),
                    span: Span::empty(),
                }))],
                into: None,
                from: vec![],
                lateral_views: vec![],
                prewhere: None,
                selection: None,
                group_by: GroupByExpr::Expressions(vec![], vec![]),
                cluster_by: vec![],
                distribute_by: vec![],
                sort_by: vec![],
                having: None,
                named_window: vec![],
                qualify: None,
                window_before_qualify: false,
                value_table_mode: None,
                connect_by: None,
            }))),
            order_by: None,
            limit: None,
            limit_by: vec![],
            offset: None,
            fetch: None,
            locks: vec![],
            for_clause: None,
            settings: None,
            format_clause: None,
        }))
    );
}

#[test]
fn parse_escaped_backticks_with_no_escape() {
    let sql = "SELECT ```quoted identifier```";
    assert_eq!(
        TestedDialects::new_with_options(
            vec![Box::new(MySqlDialect {})],
            ParserOptions::new().with_unescape(false)
        )
        .verified_stmt(sql),
        Statement::Query(Box::new(Query {
            with: None,
            body: Box::new(SetExpr::Select(Box::new(Select {
                select_token: AttachedToken::empty(),

                distinct: None,
                top: None,
                top_before_distinct: false,
                projection: vec![SelectItem::UnnamedExpr(Expr::Identifier(Ident {
                    value: "``quoted identifier``".into(),
                    quote_style: Some('`'),
                    span: Span::empty(),
                }))],
                into: None,
                from: vec![],
                lateral_views: vec![],
                prewhere: None,
                selection: None,
                group_by: GroupByExpr::Expressions(vec![], vec![]),
                cluster_by: vec![],
                distribute_by: vec![],
                sort_by: vec![],
                having: None,
                named_window: vec![],
                qualify: None,
                window_before_qualify: false,
                value_table_mode: None,
                connect_by: None,
            }))),
            order_by: None,
            limit: None,
            limit_by: vec![],
            offset: None,
            fetch: None,
            locks: vec![],
            for_clause: None,
            settings: None,
            format_clause: None,
        }))
    );
}

#[test]
fn parse_unterminated_escape() {
    let sql = r"SELECT 'I\'m not fine\'";
    let result = std::panic::catch_unwind(|| mysql().one_statement_parses_to(sql, ""));
    assert!(result.is_err());

    let sql = r"SELECT 'I\\'m not fine'";
    let result = std::panic::catch_unwind(|| mysql().one_statement_parses_to(sql, ""));
    assert!(result.is_err());
}

#[test]
fn check_roundtrip_of_escaped_string() {
    let options = ParserOptions::new().with_unescape(false);

    TestedDialects::new_with_options(vec![Box::new(MySqlDialect {})], options.clone())
        .verified_stmt(r"SELECT 'I\'m fine'");
    TestedDialects::new_with_options(vec![Box::new(MySqlDialect {})], options.clone())
        .verified_stmt(r#"SELECT 'I''m fine'"#);
    TestedDialects::new_with_options(vec![Box::new(MySqlDialect {})], options.clone())
        .verified_stmt(r"SELECT 'I\\\'m fine'");
    TestedDialects::new_with_options(vec![Box::new(MySqlDialect {})], options.clone())
        .verified_stmt(r"SELECT 'I\\\'m fine'");
    TestedDialects::new_with_options(vec![Box::new(MySqlDialect {})], options.clone())
        .verified_stmt(r#"SELECT "I\"m fine""#);
    TestedDialects::new_with_options(vec![Box::new(MySqlDialect {})], options.clone())
        .verified_stmt(r#"SELECT "I""m fine""#);
    TestedDialects::new_with_options(vec![Box::new(MySqlDialect {})], options.clone())
        .verified_stmt(r#"SELECT "I\\\"m fine""#);
    TestedDialects::new_with_options(vec![Box::new(MySqlDialect {})], options.clone())
        .verified_stmt(r#"SELECT "I\\\"m fine""#);
    TestedDialects::new_with_options(vec![Box::new(MySqlDialect {})], options.clone())
        .verified_stmt(r#"SELECT "I'm ''fine''""#);
}

#[test]
fn parse_create_table_with_minimum_display_width() {
    let sql = "CREATE TABLE foo (bar_tinyint TINYINT(3), bar_smallint SMALLINT(5), bar_mediumint MEDIUMINT(6), bar_int INT(11), bar_bigint BIGINT(20))";
    match mysql().verified_stmt(sql) {
        Statement::CreateTable(CreateTable { name, columns, .. }) => {
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
                        name: Ident::new("bar_mediumint"),
                        data_type: DataType::MediumInt(Some(6)),
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

#[test]
fn parse_create_table_unsigned() {
    let sql = "CREATE TABLE foo (bar_tinyint TINYINT(3) UNSIGNED, bar_smallint SMALLINT(5) UNSIGNED, bar_mediumint MEDIUMINT(13) UNSIGNED, bar_int INT(11) UNSIGNED, bar_bigint BIGINT(20) UNSIGNED)";
    match mysql().verified_stmt(sql) {
        Statement::CreateTable(CreateTable { name, columns, .. }) => {
            assert_eq!(name.to_string(), "foo");
            assert_eq!(
                vec![
                    ColumnDef {
                        name: Ident::new("bar_tinyint"),
                        data_type: DataType::UnsignedTinyInt(Some(3)),
                        collation: None,
                        options: vec![],
                    },
                    ColumnDef {
                        name: Ident::new("bar_smallint"),
                        data_type: DataType::UnsignedSmallInt(Some(5)),
                        collation: None,
                        options: vec![],
                    },
                    ColumnDef {
                        name: Ident::new("bar_mediumint"),
                        data_type: DataType::UnsignedMediumInt(Some(13)),
                        collation: None,
                        options: vec![],
                    },
                    ColumnDef {
                        name: Ident::new("bar_int"),
                        data_type: DataType::UnsignedInt(Some(11)),
                        collation: None,
                        options: vec![],
                    },
                    ColumnDef {
                        name: Ident::new("bar_bigint"),
                        data_type: DataType::UnsignedBigInt(Some(20)),
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
fn parse_simple_insert() {
    let sql = r"INSERT INTO tasks (title, priority) VALUES ('Test Some Inserts', 1), ('Test Entry 2', 2), ('Test Entry 3', 3)";

    match mysql().verified_stmt(sql) {
        Statement::Insert(Insert {
            table: table_name,
            columns,
            source,
            on,
            ..
        }) => {
            assert_eq!(
                TableObject::TableName(ObjectName::from(vec![Ident::new("tasks")])),
                table_name
            );
            assert_eq!(vec![Ident::new("title"), Ident::new("priority")], columns);
            assert!(on.is_none());
            assert_eq!(
                Some(Box::new(Query {
                    with: None,
                    body: Box::new(SetExpr::Values(Values {
                        explicit_row: false,
                        rows: vec![
                            vec![
                                Expr::Value(Value::SingleQuotedString(
                                    "Test Some Inserts".to_string()
                                )),
                                Expr::Value(number("1"))
                            ],
                            vec![
                                Expr::Value(Value::SingleQuotedString("Test Entry 2".to_string())),
                                Expr::Value(number("2"))
                            ],
                            vec![
                                Expr::Value(Value::SingleQuotedString("Test Entry 3".to_string())),
                                Expr::Value(number("3"))
                            ]
                        ]
                    })),
                    order_by: None,
                    limit: None,
                    limit_by: vec![],
                    offset: None,
                    fetch: None,
                    locks: vec![],
                    for_clause: None,
                    settings: None,
                    format_clause: None,
                })),
                source
            );
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_ignore_insert() {
    let sql = r"INSERT IGNORE INTO tasks (title, priority) VALUES ('Test Some Inserts', 1)";

    match mysql_and_generic().verified_stmt(sql) {
        Statement::Insert(Insert {
            table: table_name,
            columns,
            source,
            on,
            ignore,
            ..
        }) => {
            assert_eq!(
                TableObject::TableName(ObjectName::from(vec![Ident::new("tasks")])),
                table_name
            );
            assert_eq!(vec![Ident::new("title"), Ident::new("priority")], columns);
            assert!(on.is_none());
            assert!(ignore);
            assert_eq!(
                Some(Box::new(Query {
                    with: None,
                    body: Box::new(SetExpr::Values(Values {
                        explicit_row: false,
                        rows: vec![vec![
                            Expr::Value(Value::SingleQuotedString("Test Some Inserts".to_string())),
                            Expr::Value(number("1"))
                        ]]
                    })),
                    order_by: None,
                    limit: None,
                    limit_by: vec![],
                    offset: None,
                    fetch: None,
                    locks: vec![],
                    for_clause: None,
                    settings: None,
                    format_clause: None,
                })),
                source
            );
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_priority_insert() {
    let sql = r"INSERT HIGH_PRIORITY INTO tasks (title, priority) VALUES ('Test Some Inserts', 1)";

    match mysql_and_generic().verified_stmt(sql) {
        Statement::Insert(Insert {
            table: table_name,
            columns,
            source,
            on,
            priority,
            ..
        }) => {
            assert_eq!(
                TableObject::TableName(ObjectName::from(vec![Ident::new("tasks")])),
                table_name
            );
            assert_eq!(vec![Ident::new("title"), Ident::new("priority")], columns);
            assert!(on.is_none());
            assert_eq!(priority, Some(HighPriority));
            assert_eq!(
                Some(Box::new(Query {
                    with: None,
                    body: Box::new(SetExpr::Values(Values {
                        explicit_row: false,
                        rows: vec![vec![
                            Expr::Value(Value::SingleQuotedString("Test Some Inserts".to_string())),
                            Expr::Value(number("1"))
                        ]]
                    })),
                    order_by: None,
                    limit: None,
                    limit_by: vec![],
                    offset: None,
                    fetch: None,
                    locks: vec![],
                    for_clause: None,
                    settings: None,
                    format_clause: None,
                })),
                source
            );
        }
        _ => unreachable!(),
    }

    let sql2 = r"INSERT LOW_PRIORITY INTO tasks (title, priority) VALUES ('Test Some Inserts', 1)";

    match mysql().verified_stmt(sql2) {
        Statement::Insert(Insert {
            table: table_name,
            columns,
            source,
            on,
            priority,
            ..
        }) => {
            assert_eq!(
                TableObject::TableName(ObjectName::from(vec![Ident::new("tasks")])),
                table_name
            );
            assert_eq!(vec![Ident::new("title"), Ident::new("priority")], columns);
            assert!(on.is_none());
            assert_eq!(priority, Some(LowPriority));
            assert_eq!(
                Some(Box::new(Query {
                    with: None,
                    body: Box::new(SetExpr::Values(Values {
                        explicit_row: false,
                        rows: vec![vec![
                            Expr::Value(Value::SingleQuotedString("Test Some Inserts".to_string())),
                            Expr::Value(number("1"))
                        ]]
                    })),
                    order_by: None,
                    limit: None,
                    limit_by: vec![],
                    offset: None,
                    fetch: None,
                    locks: vec![],
                    for_clause: None,
                    settings: None,
                    format_clause: None,
                })),
                source
            );
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_insert_as() {
    let sql = r"INSERT INTO `table` (`date`) VALUES ('2024-01-01') AS `alias`";
    match mysql_and_generic().verified_stmt(sql) {
        Statement::Insert(Insert {
            table: table_name,
            columns,
            source,
            insert_alias,
            ..
        }) => {
            assert_eq!(
                TableObject::TableName(ObjectName::from(vec![Ident::with_quote('`', "table")])),
                table_name
            );
            assert_eq!(vec![Ident::with_quote('`', "date")], columns);
            let insert_alias = insert_alias.unwrap();

            assert_eq!(
                ObjectName::from(vec![Ident::with_quote('`', "alias")]),
                insert_alias.row_alias
            );
            assert_eq!(Some(vec![]), insert_alias.col_aliases);
            assert_eq!(
                Some(Box::new(Query {
                    with: None,
                    body: Box::new(SetExpr::Values(Values {
                        explicit_row: false,
                        rows: vec![vec![Expr::Value(Value::SingleQuotedString(
                            "2024-01-01".to_string()
                        ))]]
                    })),
                    order_by: None,
                    limit: None,
                    limit_by: vec![],
                    offset: None,
                    fetch: None,
                    locks: vec![],
                    for_clause: None,
                    settings: None,
                    format_clause: None,
                })),
                source
            );
        }
        _ => unreachable!(),
    }

    let sql = r"INSERT INTO `table` (`date`) VALUES ('2024-01-01') AS `alias` ()";
    assert!(matches!(
        mysql_and_generic().parse_sql_statements(sql),
        Err(ParserError::ParserError(_))
    ));

    let sql = r"INSERT INTO `table` (`id`, `date`) VALUES (1, '2024-01-01') AS `alias` (`mek_id`, `mek_date`)";
    match mysql_and_generic().verified_stmt(sql) {
        Statement::Insert(Insert {
            table: table_name,
            columns,
            source,
            insert_alias,
            ..
        }) => {
            assert_eq!(
                TableObject::TableName(ObjectName::from(vec![Ident::with_quote('`', "table")])),
                table_name
            );
            assert_eq!(
                vec![Ident::with_quote('`', "id"), Ident::with_quote('`', "date")],
                columns
            );
            let insert_alias = insert_alias.unwrap();
            assert_eq!(
                ObjectName::from(vec![Ident::with_quote('`', "alias")]),
                insert_alias.row_alias
            );
            assert_eq!(
                Some(vec![
                    Ident::with_quote('`', "mek_id"),
                    Ident::with_quote('`', "mek_date")
                ]),
                insert_alias.col_aliases
            );
            assert_eq!(
                Some(Box::new(Query {
                    with: None,
                    body: Box::new(SetExpr::Values(Values {
                        explicit_row: false,
                        rows: vec![vec![
                            Expr::Value(number("1")),
                            Expr::Value(Value::SingleQuotedString("2024-01-01".to_string()))
                        ]]
                    })),
                    order_by: None,
                    limit: None,
                    limit_by: vec![],
                    offset: None,
                    fetch: None,
                    locks: vec![],
                    for_clause: None,
                    settings: None,
                    format_clause: None,
                })),
                source
            );
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_replace_insert() {
    let sql = r"REPLACE DELAYED INTO tasks (title, priority) VALUES ('Test Some Inserts', 1)";
    match mysql().verified_stmt(sql) {
        Statement::Insert(Insert {
            table: table_name,
            columns,
            source,
            on,
            replace_into,
            priority,
            ..
        }) => {
            assert_eq!(
                TableObject::TableName(ObjectName::from(vec![Ident::new("tasks")])),
                table_name
            );
            assert_eq!(vec![Ident::new("title"), Ident::new("priority")], columns);
            assert!(on.is_none());
            assert!(replace_into);
            assert_eq!(priority, Some(Delayed));
            assert_eq!(
                Some(Box::new(Query {
                    with: None,
                    body: Box::new(SetExpr::Values(Values {
                        explicit_row: false,
                        rows: vec![vec![
                            Expr::Value(Value::SingleQuotedString("Test Some Inserts".to_string())),
                            Expr::Value(number("1"))
                        ]]
                    })),
                    order_by: None,
                    limit: None,
                    limit_by: vec![],
                    offset: None,
                    fetch: None,
                    locks: vec![],
                    for_clause: None,
                    settings: None,
                    format_clause: None,
                })),
                source
            );
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_empty_row_insert() {
    let sql = "INSERT INTO tb () VALUES (), ()";

    match mysql().one_statement_parses_to(sql, "INSERT INTO tb VALUES (), ()") {
        Statement::Insert(Insert {
            table: table_name,
            columns,
            source,
            on,
            ..
        }) => {
            assert_eq!(
                TableObject::TableName(ObjectName::from(vec![Ident::new("tb")])),
                table_name
            );
            assert!(columns.is_empty());
            assert!(on.is_none());
            assert_eq!(
                Some(Box::new(Query {
                    with: None,
                    body: Box::new(SetExpr::Values(Values {
                        explicit_row: false,
                        rows: vec![vec![], vec![]]
                    })),
                    order_by: None,
                    limit: None,
                    limit_by: vec![],
                    offset: None,
                    fetch: None,
                    locks: vec![],
                    for_clause: None,
                    settings: None,
                    format_clause: None,
                })),
                source
            );
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_insert_with_on_duplicate_update() {
    let sql = "INSERT INTO permission_groups (name, description, perm_create, perm_read, perm_update, perm_delete) VALUES ('accounting_manager', 'Some description about the group', true, true, true, true) ON DUPLICATE KEY UPDATE description = VALUES(description), perm_create = VALUES(perm_create), perm_read = VALUES(perm_read), perm_update = VALUES(perm_update), perm_delete = VALUES(perm_delete)";

    match mysql().verified_stmt(sql) {
        Statement::Insert(Insert {
            table: table_name,
            columns,
            source,
            on,
            ..
        }) => {
            assert_eq!(
                TableObject::TableName(ObjectName::from(vec![Ident::new("permission_groups")])),
                table_name
            );
            assert_eq!(
                vec![
                    Ident::new("name"),
                    Ident::new("description"),
                    Ident::new("perm_create"),
                    Ident::new("perm_read"),
                    Ident::new("perm_update"),
                    Ident::new("perm_delete")
                ],
                columns
            );
            assert_eq!(
                Some(Box::new(Query {
                    with: None,
                    body: Box::new(SetExpr::Values(Values {
                        explicit_row: false,
                        rows: vec![vec![
                            Expr::Value(Value::SingleQuotedString(
                                "accounting_manager".to_string()
                            )),
                            Expr::Value(Value::SingleQuotedString(
                                "Some description about the group".to_string()
                            )),
                            Expr::Value(Value::Boolean(true)),
                            Expr::Value(Value::Boolean(true)),
                            Expr::Value(Value::Boolean(true)),
                            Expr::Value(Value::Boolean(true)),
                        ]]
                    })),
                    order_by: None,
                    limit: None,
                    limit_by: vec![],
                    offset: None,
                    fetch: None,
                    locks: vec![],
                    for_clause: None,
                    settings: None,
                    format_clause: None,
                })),
                source
            );
            assert_eq!(
                Some(OnInsert::DuplicateKeyUpdate(vec![
                    Assignment {
                        target: AssignmentTarget::ColumnName(ObjectName::from(vec![Ident::new(
                            "description".to_string()
                        )])),
                        value: call("VALUES", [Expr::Identifier(Ident::new("description"))]),
                    },
                    Assignment {
                        target: AssignmentTarget::ColumnName(ObjectName::from(vec![Ident::new(
                            "perm_create".to_string()
                        )])),
                        value: call("VALUES", [Expr::Identifier(Ident::new("perm_create"))]),
                    },
                    Assignment {
                        target: AssignmentTarget::ColumnName(ObjectName::from(vec![Ident::new(
                            "perm_read".to_string()
                        )])),
                        value: call("VALUES", [Expr::Identifier(Ident::new("perm_read"))]),
                    },
                    Assignment {
                        target: AssignmentTarget::ColumnName(ObjectName::from(vec![Ident::new(
                            "perm_update".to_string()
                        )])),
                        value: call("VALUES", [Expr::Identifier(Ident::new("perm_update"))]),
                    },
                    Assignment {
                        target: AssignmentTarget::ColumnName(ObjectName::from(vec![Ident::new(
                            "perm_delete".to_string()
                        )])),
                        value: call("VALUES", [Expr::Identifier(Ident::new("perm_delete"))]),
                    },
                ])),
                on
            );
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_select_with_numeric_prefix_column_name() {
    let sql = "SELECT 123col_$@123abc FROM \"table\"";
    match mysql().verified_stmt(sql) {
        Statement::Query(q) => {
            assert_eq!(
                q.body,
                Box::new(SetExpr::Select(Box::new(Select {
                    select_token: AttachedToken::empty(),

                    distinct: None,
                    top: None,
                    top_before_distinct: false,
                    projection: vec![SelectItem::UnnamedExpr(Expr::Identifier(Ident::new(
                        "123col_$@123abc"
                    )))],
                    into: None,
                    from: vec![TableWithJoins {
                        relation: table_from_name(ObjectName::from(vec![Ident::with_quote(
                            '"', "table"
                        )])),
                        joins: vec![]
                    }],
                    lateral_views: vec![],
                    prewhere: None,
                    selection: None,
                    group_by: GroupByExpr::Expressions(vec![], vec![]),
                    cluster_by: vec![],
                    distribute_by: vec![],
                    sort_by: vec![],
                    having: None,
                    named_window: vec![],
                    qualify: None,
                    window_before_qualify: false,
                    value_table_mode: None,
                    connect_by: None,
                })))
            );
        }
        _ => unreachable!(),
    }
}

// Don't run with bigdecimal as it fails like this on rust beta:
//
// 'parse_select_with_concatenation_of_exp_number_and_numeric_prefix_column'
// panicked at 'assertion failed: `(left == right)`
//
//  left: `"SELECT 123e4, 123col_$@123abc FROM \"table\""`,
//  right: `"SELECT 1230000, 123col_$@123abc FROM \"table\""`', src/test_utils.rs:114:13
#[cfg(not(feature = "bigdecimal"))]
#[test]
fn parse_select_with_concatenation_of_exp_number_and_numeric_prefix_column() {
    let sql = "SELECT 123e4, 123col_$@123abc FROM \"table\"";
    match mysql().verified_stmt(sql) {
        Statement::Query(q) => {
            assert_eq!(
                q.body,
                Box::new(SetExpr::Select(Box::new(Select {
                    select_token: AttachedToken::empty(),

                    distinct: None,
                    top: None,
                    top_before_distinct: false,
                    projection: vec![
                        SelectItem::UnnamedExpr(Expr::Value(number("123e4"))),
                        SelectItem::UnnamedExpr(Expr::Identifier(Ident::new("123col_$@123abc")))
                    ],
                    into: None,
                    from: vec![TableWithJoins {
                        relation: table_from_name(ObjectName::from(vec![Ident::with_quote(
                            '"', "table"
                        )])),
                        joins: vec![]
                    }],
                    lateral_views: vec![],
                    prewhere: None,
                    selection: None,
                    group_by: GroupByExpr::Expressions(vec![], vec![]),
                    cluster_by: vec![],
                    distribute_by: vec![],
                    sort_by: vec![],
                    having: None,
                    named_window: vec![],
                    qualify: None,
                    window_before_qualify: false,
                    value_table_mode: None,
                    connect_by: None,
                })))
            );
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_insert_with_numeric_prefix_column_name() {
    let sql = "INSERT INTO s1.t1 (123col_$@length123) VALUES (67.654)";
    match mysql().verified_stmt(sql) {
        Statement::Insert(Insert {
            table: table_name,
            columns,
            ..
        }) => {
            assert_eq!(
                TableObject::TableName(ObjectName::from(vec![Ident::new("s1"), Ident::new("t1")])),
                table_name
            );
            assert_eq!(vec![Ident::new("123col_$@length123")], columns);
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_update_with_joins() {
    let sql = "UPDATE orders AS o JOIN customers AS c ON o.customer_id = c.id SET o.completed = true WHERE c.firstname = 'Peter'";
    match mysql().verified_stmt(sql) {
        Statement::Update {
            table,
            assignments,
            from: _from,
            selection,
            returning,
            or: None,
        } => {
            assert_eq!(
                TableWithJoins {
                    relation: TableFactor::Table {
                        name: ObjectName::from(vec![Ident::new("orders")]),
                        alias: Some(TableAlias {
                            name: Ident::new("o"),
                            columns: vec![]
                        }),
                        args: None,
                        with_hints: vec![],
                        version: None,
                        partitions: vec![],
                        with_ordinality: false,
                        json_path: None,
                        sample: None,
                        index_hints: vec![],
                    },
                    joins: vec![Join {
                        relation: TableFactor::Table {
                            name: ObjectName::from(vec![Ident::new("customers")]),
                            alias: Some(TableAlias {
                                name: Ident::new("c"),
                                columns: vec![]
                            }),
                            args: None,
                            with_hints: vec![],
                            version: None,
                            partitions: vec![],
                            with_ordinality: false,
                            json_path: None,
                            sample: None,
                            index_hints: vec![],
                        },
                        global: false,
                        join_operator: JoinOperator::Inner(JoinConstraint::On(Expr::BinaryOp {
                            left: Box::new(Expr::CompoundIdentifier(vec![
                                Ident::new("o"),
                                Ident::new("customer_id")
                            ])),
                            op: BinaryOperator::Eq,
                            right: Box::new(Expr::CompoundIdentifier(vec![
                                Ident::new("c"),
                                Ident::new("id")
                            ]))
                        })),
                    }]
                },
                table
            );
            assert_eq!(
                vec![Assignment {
                    target: AssignmentTarget::ColumnName(ObjectName::from(vec![
                        Ident::new("o"),
                        Ident::new("completed")
                    ])),
                    value: Expr::Value(Value::Boolean(true))
                }],
                assignments
            );
            assert_eq!(
                Some(Expr::BinaryOp {
                    left: Box::new(Expr::CompoundIdentifier(vec![
                        Ident::new("c"),
                        Ident::new("firstname")
                    ])),
                    op: BinaryOperator::Eq,
                    right: Box::new(Expr::Value(Value::SingleQuotedString("Peter".to_string())))
                }),
                selection
            );
            assert_eq!(None, returning);
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_delete_with_order_by() {
    let sql = "DELETE FROM customers ORDER BY id DESC";
    match mysql().verified_stmt(sql) {
        Statement::Delete(Delete { order_by, .. }) => {
            assert_eq!(
                vec![OrderByExpr {
                    expr: Expr::Identifier(Ident {
                        value: "id".to_owned(),
                        quote_style: None,
                        span: Span::empty(),
                    }),
                    asc: Some(false),
                    nulls_first: None,
                    with_fill: None,
                }],
                order_by
            );
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_delete_with_limit() {
    let sql = "DELETE FROM customers LIMIT 100";
    match mysql().verified_stmt(sql) {
        Statement::Delete(Delete { limit, .. }) => {
            assert_eq!(Some(Expr::Value(number("100"))), limit);
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_alter_table_add_column() {
    match mysql().verified_stmt("ALTER TABLE tab ADD COLUMN b INT FIRST") {
        Statement::AlterTable {
            name,
            if_exists,
            only,
            operations,
            location: _,
            on_cluster: _,
        } => {
            assert_eq!(name.to_string(), "tab");
            assert!(!if_exists);
            assert!(!only);
            assert_eq!(
                operations,
                vec![AlterTableOperation::AddColumn {
                    column_keyword: true,
                    if_not_exists: false,
                    column_def: ColumnDef {
                        name: "b".into(),
                        data_type: DataType::Int(None),
                        collation: None,
                        options: vec![],
                    },
                    column_position: Some(MySQLColumnPosition::First),
                },]
            );
        }
        _ => unreachable!(),
    }

    match mysql().verified_stmt("ALTER TABLE tab ADD COLUMN b INT AFTER foo") {
        Statement::AlterTable {
            name,
            if_exists,
            only,
            operations,
            location: _,
            on_cluster: _,
        } => {
            assert_eq!(name.to_string(), "tab");
            assert!(!if_exists);
            assert!(!only);
            assert_eq!(
                operations,
                vec![AlterTableOperation::AddColumn {
                    column_keyword: true,
                    if_not_exists: false,
                    column_def: ColumnDef {
                        name: "b".into(),
                        data_type: DataType::Int(None),
                        collation: None,
                        options: vec![],
                    },
                    column_position: Some(MySQLColumnPosition::After(Ident {
                        value: String::from("foo"),
                        quote_style: None,
                        span: Span::empty(),
                    })),
                },]
            );
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_alter_table_add_columns() {
    match mysql()
        .verified_stmt("ALTER TABLE tab ADD COLUMN a TEXT FIRST, ADD COLUMN b INT AFTER foo")
    {
        Statement::AlterTable {
            name,
            if_exists,
            only,
            operations,
            location: _,
            on_cluster: _,
        } => {
            assert_eq!(name.to_string(), "tab");
            assert!(!if_exists);
            assert!(!only);
            assert_eq!(
                operations,
                vec![
                    AlterTableOperation::AddColumn {
                        column_keyword: true,
                        if_not_exists: false,
                        column_def: ColumnDef {
                            name: "a".into(),
                            data_type: DataType::Text,
                            collation: None,
                            options: vec![],
                        },
                        column_position: Some(MySQLColumnPosition::First),
                    },
                    AlterTableOperation::AddColumn {
                        column_keyword: true,
                        if_not_exists: false,
                        column_def: ColumnDef {
                            name: "b".into(),
                            data_type: DataType::Int(None),
                            collation: None,
                            options: vec![],
                        },
                        column_position: Some(MySQLColumnPosition::After(Ident {
                            value: String::from("foo"),
                            quote_style: None,
                            span: Span::empty(),
                        })),
                    },
                ]
            );
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_alter_table_drop_primary_key() {
    assert_matches!(
        alter_table_op(mysql_and_generic().verified_stmt("ALTER TABLE tab DROP PRIMARY KEY")),
        AlterTableOperation::DropPrimaryKey
    );
}

#[test]
fn parse_alter_table_change_column() {
    let expected_name = ObjectName::from(vec![Ident::new("orders")]);
    let expected_operation = AlterTableOperation::ChangeColumn {
        old_name: Ident::new("description"),
        new_name: Ident::new("desc"),
        data_type: DataType::Text,
        options: vec![ColumnOption::NotNull],
        column_position: None,
    };

    let sql1 = "ALTER TABLE orders CHANGE COLUMN description desc TEXT NOT NULL";
    let operation =
        alter_table_op_with_name(mysql().verified_stmt(sql1), &expected_name.to_string());
    assert_eq!(expected_operation, operation);

    let sql2 = "ALTER TABLE orders CHANGE description desc TEXT NOT NULL";
    let operation = alter_table_op_with_name(
        mysql().one_statement_parses_to(sql2, sql1),
        &expected_name.to_string(),
    );
    assert_eq!(expected_operation, operation);

    let expected_operation = AlterTableOperation::ChangeColumn {
        old_name: Ident::new("description"),
        new_name: Ident::new("desc"),
        data_type: DataType::Text,
        options: vec![ColumnOption::NotNull],
        column_position: Some(MySQLColumnPosition::First),
    };
    let sql3 = "ALTER TABLE orders CHANGE COLUMN description desc TEXT NOT NULL FIRST";
    let operation =
        alter_table_op_with_name(mysql().verified_stmt(sql3), &expected_name.to_string());
    assert_eq!(expected_operation, operation);

    let expected_operation = AlterTableOperation::ChangeColumn {
        old_name: Ident::new("description"),
        new_name: Ident::new("desc"),
        data_type: DataType::Text,
        options: vec![ColumnOption::NotNull],
        column_position: Some(MySQLColumnPosition::After(Ident {
            value: String::from("foo"),
            quote_style: None,
            span: Span::empty(),
        })),
    };
    let sql4 = "ALTER TABLE orders CHANGE COLUMN description desc TEXT NOT NULL AFTER foo";
    let operation =
        alter_table_op_with_name(mysql().verified_stmt(sql4), &expected_name.to_string());
    assert_eq!(expected_operation, operation);
}

#[test]
fn parse_alter_table_change_column_with_column_position() {
    let expected_name = ObjectName::from(vec![Ident::new("orders")]);
    let expected_operation_first = AlterTableOperation::ChangeColumn {
        old_name: Ident::new("description"),
        new_name: Ident::new("desc"),
        data_type: DataType::Text,
        options: vec![ColumnOption::NotNull],
        column_position: Some(MySQLColumnPosition::First),
    };

    let sql1 = "ALTER TABLE orders CHANGE COLUMN description desc TEXT NOT NULL FIRST";
    let operation =
        alter_table_op_with_name(mysql().verified_stmt(sql1), &expected_name.to_string());
    assert_eq!(expected_operation_first, operation);

    let sql2 = "ALTER TABLE orders CHANGE description desc TEXT NOT NULL FIRST";
    let operation = alter_table_op_with_name(
        mysql().one_statement_parses_to(sql2, sql1),
        &expected_name.to_string(),
    );
    assert_eq!(expected_operation_first, operation);

    let expected_operation_after = AlterTableOperation::ChangeColumn {
        old_name: Ident::new("description"),
        new_name: Ident::new("desc"),
        data_type: DataType::Text,
        options: vec![ColumnOption::NotNull],
        column_position: Some(MySQLColumnPosition::After(Ident {
            value: String::from("total_count"),
            quote_style: None,
            span: Span::empty(),
        })),
    };

    let sql1 = "ALTER TABLE orders CHANGE COLUMN description desc TEXT NOT NULL AFTER total_count";
    let operation =
        alter_table_op_with_name(mysql().verified_stmt(sql1), &expected_name.to_string());
    assert_eq!(expected_operation_after, operation);

    let sql2 = "ALTER TABLE orders CHANGE description desc TEXT NOT NULL AFTER total_count";
    let operation = alter_table_op_with_name(
        mysql().one_statement_parses_to(sql2, sql1),
        &expected_name.to_string(),
    );
    assert_eq!(expected_operation_after, operation);
}

#[test]
fn parse_alter_table_modify_column() {
    let expected_name = ObjectName::from(vec![Ident::new("orders")]);
    let expected_operation = AlterTableOperation::ModifyColumn {
        col_name: Ident::new("description"),
        data_type: DataType::Text,
        options: vec![ColumnOption::NotNull],
        column_position: None,
    };

    let sql1 = "ALTER TABLE orders MODIFY COLUMN description TEXT NOT NULL";
    let operation =
        alter_table_op_with_name(mysql().verified_stmt(sql1), &expected_name.to_string());
    assert_eq!(expected_operation, operation);

    let sql2 = "ALTER TABLE orders MODIFY description TEXT NOT NULL";
    let operation = alter_table_op_with_name(
        mysql().one_statement_parses_to(sql2, sql1),
        &expected_name.to_string(),
    );
    assert_eq!(expected_operation, operation);

    let expected_operation = AlterTableOperation::ModifyColumn {
        col_name: Ident::new("description"),
        data_type: DataType::Text,
        options: vec![ColumnOption::NotNull],
        column_position: Some(MySQLColumnPosition::First),
    };
    let sql3 = "ALTER TABLE orders MODIFY COLUMN description TEXT NOT NULL FIRST";
    let operation =
        alter_table_op_with_name(mysql().verified_stmt(sql3), &expected_name.to_string());
    assert_eq!(expected_operation, operation);

    let expected_operation = AlterTableOperation::ModifyColumn {
        col_name: Ident::new("description"),
        data_type: DataType::Text,
        options: vec![ColumnOption::NotNull],
        column_position: Some(MySQLColumnPosition::After(Ident {
            value: String::from("foo"),
            quote_style: None,
            span: Span::empty(),
        })),
    };
    let sql4 = "ALTER TABLE orders MODIFY COLUMN description TEXT NOT NULL AFTER foo";
    let operation =
        alter_table_op_with_name(mysql().verified_stmt(sql4), &expected_name.to_string());
    assert_eq!(expected_operation, operation);
}

#[test]
fn parse_alter_table_modify_column_with_column_position() {
    let expected_name = ObjectName::from(vec![Ident::new("orders")]);
    let expected_operation_first = AlterTableOperation::ModifyColumn {
        col_name: Ident::new("description"),
        data_type: DataType::Text,
        options: vec![ColumnOption::NotNull],
        column_position: Some(MySQLColumnPosition::First),
    };

    let sql1 = "ALTER TABLE orders MODIFY COLUMN description TEXT NOT NULL FIRST";
    let operation =
        alter_table_op_with_name(mysql().verified_stmt(sql1), &expected_name.to_string());
    assert_eq!(expected_operation_first, operation);

    let sql2 = "ALTER TABLE orders MODIFY description TEXT NOT NULL FIRST";
    let operation = alter_table_op_with_name(
        mysql().one_statement_parses_to(sql2, sql1),
        &expected_name.to_string(),
    );
    assert_eq!(expected_operation_first, operation);

    let expected_operation_after = AlterTableOperation::ModifyColumn {
        col_name: Ident::new("description"),
        data_type: DataType::Text,
        options: vec![ColumnOption::NotNull],
        column_position: Some(MySQLColumnPosition::After(Ident {
            value: String::from("total_count"),
            quote_style: None,
            span: Span::empty(),
        })),
    };

    let sql1 = "ALTER TABLE orders MODIFY COLUMN description TEXT NOT NULL AFTER total_count";
    let operation =
        alter_table_op_with_name(mysql().verified_stmt(sql1), &expected_name.to_string());
    assert_eq!(expected_operation_after, operation);

    let sql2 = "ALTER TABLE orders MODIFY description TEXT NOT NULL AFTER total_count";
    let operation = alter_table_op_with_name(
        mysql().one_statement_parses_to(sql2, sql1),
        &expected_name.to_string(),
    );
    assert_eq!(expected_operation_after, operation);
}

#[test]
fn parse_substring_in_select() {
    use sqlparser::tokenizer::Span;

    let sql = "SELECT DISTINCT SUBSTRING(description, 0, 1) FROM test";
    match mysql().one_statement_parses_to(
        sql,
        "SELECT DISTINCT SUBSTRING(description, 0, 1) FROM test",
    ) {
        Statement::Query(query) => {
            assert_eq!(
                Box::new(Query {
                    with: None,
                    body: Box::new(SetExpr::Select(Box::new(Select {
                        select_token: AttachedToken::empty(),
                        distinct: Some(Distinct::Distinct),
                        top: None,
                        top_before_distinct: false,
                        projection: vec![SelectItem::UnnamedExpr(Expr::Substring {
                            expr: Box::new(Expr::Identifier(Ident {
                                value: "description".to_string(),
                                quote_style: None,
                                span: Span::empty(),
                            })),
                            substring_from: Some(Box::new(Expr::Value(number("0")))),
                            substring_for: Some(Box::new(Expr::Value(number("1")))),
                            special: true,
                        })],
                        into: None,
                        from: vec![TableWithJoins {
                            relation: table_from_name(ObjectName::from(vec![Ident {
                                value: "test".to_string(),
                                quote_style: None,
                                span: Span::empty(),
                            }])),
                            joins: vec![]
                        }],
                        lateral_views: vec![],
                        prewhere: None,
                        selection: None,
                        group_by: GroupByExpr::Expressions(vec![], vec![]),
                        cluster_by: vec![],
                        distribute_by: vec![],
                        sort_by: vec![],
                        having: None,
                        named_window: vec![],
                        window_before_qualify: false,
                        qualify: None,
                        value_table_mode: None,
                        connect_by: None,
                    }))),
                    order_by: None,
                    limit: None,
                    limit_by: vec![],
                    offset: None,
                    fetch: None,
                    locks: vec![],
                    for_clause: None,
                    settings: None,
                    format_clause: None,
                }),
                query
            );
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_show_variables() {
    mysql_and_generic().verified_stmt("SHOW VARIABLES");
    mysql_and_generic().verified_stmt("SHOW VARIABLES LIKE 'admin%'");
    mysql_and_generic().verified_stmt("SHOW VARIABLES WHERE value = '3306'");
    mysql_and_generic().verified_stmt("SHOW GLOBAL VARIABLES");
    mysql_and_generic().verified_stmt("SHOW GLOBAL VARIABLES LIKE 'admin%'");
    mysql_and_generic().verified_stmt("SHOW GLOBAL VARIABLES WHERE value = '3306'");
    mysql_and_generic().verified_stmt("SHOW SESSION VARIABLES");
    mysql_and_generic().verified_stmt("SHOW SESSION VARIABLES LIKE 'admin%'");
    mysql_and_generic().verified_stmt("SHOW GLOBAL VARIABLES WHERE value = '3306'");
}

#[test]
fn parse_rlike_and_regexp() {
    for s in &[
        "SELECT 1 WHERE 'a' RLIKE '^a$'",
        "SELECT 1 WHERE 'a' REGEXP '^a$'",
        "SELECT 1 WHERE 'a' NOT RLIKE '^a$'",
        "SELECT 1 WHERE 'a' NOT REGEXP '^a$'",
    ] {
        mysql_and_generic().verified_only_select(s);
    }
}

#[test]
fn parse_kill() {
    let stmt = mysql_and_generic().verified_stmt("KILL CONNECTION 5");
    assert_eq!(
        stmt,
        Statement::Kill {
            modifier: Some(KillType::Connection),
            id: 5,
        }
    );

    let stmt = mysql_and_generic().verified_stmt("KILL QUERY 5");
    assert_eq!(
        stmt,
        Statement::Kill {
            modifier: Some(KillType::Query),
            id: 5,
        }
    );

    let stmt = mysql_and_generic().verified_stmt("KILL 5");
    assert_eq!(
        stmt,
        Statement::Kill {
            modifier: None,
            id: 5,
        }
    );
}

#[test]
fn parse_table_column_option_on_update() {
    let sql1 = "CREATE TABLE foo (`modification_time` DATETIME ON UPDATE CURRENT_TIMESTAMP())";
    match mysql().verified_stmt(sql1) {
        Statement::CreateTable(CreateTable { name, columns, .. }) => {
            assert_eq!(name.to_string(), "foo");
            assert_eq!(
                vec![ColumnDef {
                    name: Ident::with_quote('`', "modification_time"),
                    data_type: DataType::Datetime(None),
                    collation: None,
                    options: vec![ColumnOptionDef {
                        name: None,
                        option: ColumnOption::OnUpdate(call("CURRENT_TIMESTAMP", [])),
                    },],
                }],
                columns
            );
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_set_names() {
    let stmt = mysql_and_generic().verified_stmt("SET NAMES utf8mb4");
    assert_eq!(
        stmt,
        Statement::SetNames {
            charset_name: "utf8mb4".to_string(),
            collation_name: None,
        }
    );

    let stmt = mysql_and_generic().verified_stmt("SET NAMES utf8mb4 COLLATE bogus");
    assert_eq!(
        stmt,
        Statement::SetNames {
            charset_name: "utf8mb4".to_string(),
            collation_name: Some("bogus".to_string()),
        }
    );

    let stmt = mysql_and_generic()
        .parse_sql_statements("set names utf8mb4 collate bogus")
        .unwrap();
    assert_eq!(
        stmt,
        vec![Statement::SetNames {
            charset_name: "utf8mb4".to_string(),
            collation_name: Some("bogus".to_string()),
        }]
    );

    let stmt = mysql_and_generic().verified_stmt("SET NAMES DEFAULT");
    assert_eq!(stmt, Statement::SetNamesDefault {});
}

#[test]
fn parse_limit_my_sql_syntax() {
    mysql_and_generic().one_statement_parses_to(
        "SELECT id, fname, lname FROM customer LIMIT 5, 10",
        "SELECT id, fname, lname FROM customer LIMIT 10 OFFSET 5",
    );
    mysql_and_generic().verified_stmt("SELECT * FROM user LIMIT ? OFFSET ?");
}

#[test]
fn parse_create_table_with_index_definition() {
    mysql_and_generic().one_statement_parses_to(
        "CREATE TABLE tb (id INT, INDEX (id))",
        "CREATE TABLE tb (id INT, INDEX (id))",
    );

    mysql_and_generic().one_statement_parses_to(
        "CREATE TABLE tb (id INT, index USING BTREE (id))",
        "CREATE TABLE tb (id INT, INDEX USING BTREE (id))",
    );

    mysql_and_generic().one_statement_parses_to(
        "CREATE TABLE tb (id INT, KEY USING HASH (id))",
        "CREATE TABLE tb (id INT, KEY USING HASH (id))",
    );

    mysql_and_generic().one_statement_parses_to(
        "CREATE TABLE tb (id INT, key index (id))",
        "CREATE TABLE tb (id INT, KEY index (id))",
    );

    mysql_and_generic().one_statement_parses_to(
        "CREATE TABLE tb (id INT, INDEX 'index' (id))",
        "CREATE TABLE tb (id INT, INDEX 'index' (id))",
    );

    mysql_and_generic().one_statement_parses_to(
        "CREATE TABLE tb (id INT, INDEX index USING BTREE (id))",
        "CREATE TABLE tb (id INT, INDEX index USING BTREE (id))",
    );

    mysql_and_generic().one_statement_parses_to(
        "CREATE TABLE tb (id INT, INDEX index USING HASH (id))",
        "CREATE TABLE tb (id INT, INDEX index USING HASH (id))",
    );

    mysql_and_generic().one_statement_parses_to(
        "CREATE TABLE tb (id INT, INDEX (c1, c2, c3, c4,c5))",
        "CREATE TABLE tb (id INT, INDEX (c1, c2, c3, c4, c5))",
    );
}

#[test]
fn parse_create_table_unallow_constraint_then_index() {
    let sql = "CREATE TABLE foo (bar INT, CONSTRAINT constr INDEX index (bar))";
    assert!(mysql_and_generic().parse_sql_statements(sql).is_err());

    let sql = "CREATE TABLE foo (bar INT, INDEX index (bar))";
    assert!(mysql_and_generic().parse_sql_statements(sql).is_ok());
}

#[test]
fn parse_create_table_with_fulltext_definition() {
    mysql_and_generic().verified_stmt("CREATE TABLE tb (id INT, FULLTEXT (id))");

    mysql_and_generic().verified_stmt("CREATE TABLE tb (id INT, FULLTEXT INDEX (id))");

    mysql_and_generic().verified_stmt("CREATE TABLE tb (id INT, FULLTEXT KEY (id))");

    mysql_and_generic().verified_stmt("CREATE TABLE tb (id INT, FULLTEXT potato (id))");

    mysql_and_generic().verified_stmt("CREATE TABLE tb (id INT, FULLTEXT INDEX potato (id))");

    mysql_and_generic().verified_stmt("CREATE TABLE tb (id INT, FULLTEXT KEY potato (id))");

    mysql_and_generic()
        .verified_stmt("CREATE TABLE tb (c1 INT, c2 INT, FULLTEXT KEY potato (c1, c2))");
}

#[test]
fn parse_create_table_with_spatial_definition() {
    mysql_and_generic().verified_stmt("CREATE TABLE tb (id INT, SPATIAL (id))");

    mysql_and_generic().verified_stmt("CREATE TABLE tb (id INT, SPATIAL INDEX (id))");

    mysql_and_generic().verified_stmt("CREATE TABLE tb (id INT, SPATIAL KEY (id))");

    mysql_and_generic().verified_stmt("CREATE TABLE tb (id INT, SPATIAL potato (id))");

    mysql_and_generic().verified_stmt("CREATE TABLE tb (id INT, SPATIAL INDEX potato (id))");

    mysql_and_generic().verified_stmt("CREATE TABLE tb (id INT, SPATIAL KEY potato (id))");

    mysql_and_generic()
        .verified_stmt("CREATE TABLE tb (c1 INT, c2 INT, SPATIAL KEY potato (c1, c2))");
}

#[test]
fn parse_fulltext_expression() {
    mysql_and_generic().verified_stmt("SELECT * FROM tb WHERE MATCH (c1) AGAINST ('string')");

    mysql_and_generic().verified_stmt(
        "SELECT * FROM tb WHERE MATCH (c1) AGAINST ('string' IN NATURAL LANGUAGE MODE)",
    );

    mysql_and_generic().verified_stmt("SELECT * FROM tb WHERE MATCH (c1) AGAINST ('string' IN NATURAL LANGUAGE MODE WITH QUERY EXPANSION)");

    mysql_and_generic()
        .verified_stmt("SELECT * FROM tb WHERE MATCH (c1) AGAINST ('string' IN BOOLEAN MODE)");

    mysql_and_generic()
        .verified_stmt("SELECT * FROM tb WHERE MATCH (c1) AGAINST ('string' WITH QUERY EXPANSION)");

    mysql_and_generic()
        .verified_stmt("SELECT * FROM tb WHERE MATCH (c1, c2, c3) AGAINST ('string')");

    mysql_and_generic().verified_stmt("SELECT * FROM tb WHERE MATCH (c1) AGAINST (123)");

    mysql_and_generic().verified_stmt("SELECT * FROM tb WHERE MATCH (c1) AGAINST (NULL)");

    mysql_and_generic().verified_stmt("SELECT COUNT(IF(MATCH (title, body) AGAINST ('database' IN NATURAL LANGUAGE MODE), 1, NULL)) AS count FROM articles");
}

#[test]
#[should_panic = "Expected: FULLTEXT or SPATIAL option without constraint name, found: cons"]
fn parse_create_table_with_fulltext_definition_should_not_accept_constraint_name() {
    mysql_and_generic().verified_stmt("CREATE TABLE tb (c1 INT, CONSTRAINT cons FULLTEXT (c1))");
}

#[test]
fn parse_values() {
    mysql().verified_stmt("VALUES ROW(1, true, 'a')");
    mysql().verified_stmt("SELECT a, c FROM (VALUES ROW(1, true, 'a'), ROW(2, false, 'b'), ROW(3, false, 'c')) AS t (a, b, c)");
}

#[test]
fn parse_hex_string_introducer() {
    assert_eq!(
        mysql().verified_stmt("SELECT _latin1 X'4D7953514C'"),
        Statement::Query(Box::new(Query {
            with: None,
            body: Box::new(SetExpr::Select(Box::new(Select {
                select_token: AttachedToken::empty(),
                distinct: None,
                top: None,
                top_before_distinct: false,
                projection: vec![SelectItem::UnnamedExpr(Expr::IntroducedString {
                    introducer: "_latin1".to_string(),
                    value: Value::HexStringLiteral("4D7953514C".to_string())
                })],
                from: vec![],
                lateral_views: vec![],
                prewhere: None,
                selection: None,
                group_by: GroupByExpr::Expressions(vec![], vec![]),
                cluster_by: vec![],
                distribute_by: vec![],
                sort_by: vec![],
                having: None,
                named_window: vec![],
                window_before_qualify: false,
                qualify: None,
                value_table_mode: None,
                into: None,
                connect_by: None,
            }))),
            order_by: None,
            limit: None,
            limit_by: vec![],
            offset: None,
            fetch: None,
            locks: vec![],
            for_clause: None,
            settings: None,
            format_clause: None,
        }))
    )
}

#[test]
fn parse_string_introducers() {
    mysql().verified_stmt("SELECT _binary 'abc'");
    mysql().one_statement_parses_to("SELECT _utf8'abc'", "SELECT _utf8 'abc'");
    mysql().one_statement_parses_to("SELECT _utf8mb4'abc'", "SELECT _utf8mb4 'abc'");
    mysql().verified_stmt("SELECT _binary 'abc', _utf8mb4 'abc'");
}

#[test]
fn parse_div_infix() {
    mysql().verified_stmt(r#"SELECT 5 DIV 2"#);
}

#[test]
fn parse_drop_temporary_table() {
    let sql = "DROP TEMPORARY TABLE foo";
    match mysql().verified_stmt(sql) {
        Statement::Drop {
            object_type,
            if_exists,
            names,
            cascade,
            purge: _,
            temporary,
            ..
        } => {
            assert!(!if_exists);
            assert_eq!(ObjectType::Table, object_type);
            assert_eq!(
                vec!["foo"],
                names.iter().map(ToString::to_string).collect::<Vec<_>>()
            );
            assert!(!cascade);
            assert!(temporary);
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_convert_using() {
    // https://dev.mysql.com/doc/refman/8.0/en/cast-functions.html#function_convert

    // CONVERT(expr USING transcoding_name)
    mysql().verified_only_select("SELECT CONVERT('x' USING latin1)");
    mysql().verified_only_select("SELECT CONVERT(my_column USING utf8mb4) FROM my_table");

    // CONVERT(expr, type)
    mysql().verified_only_select("SELECT CONVERT('abc', CHAR(60))");
    mysql().verified_only_select("SELECT CONVERT(123.456, DECIMAL(5,2))");
    // with a type + a charset
    mysql().verified_only_select("SELECT CONVERT('test', CHAR CHARACTER SET utf8mb4)");
}

#[test]
fn parse_create_table_with_column_collate() {
    let sql = "CREATE TABLE tb (id TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci)";
    let canonical = "CREATE TABLE tb (id TEXT COLLATE utf8mb4_0900_ai_ci CHARACTER SET utf8mb4)";
    match mysql().one_statement_parses_to(sql, canonical) {
        Statement::CreateTable(CreateTable { name, columns, .. }) => {
            assert_eq!(name.to_string(), "tb");
            assert_eq!(
                vec![ColumnDef {
                    name: Ident::new("id"),
                    data_type: DataType::Text,
                    collation: Some(ObjectName::from(vec![Ident::new("utf8mb4_0900_ai_ci")])),
                    options: vec![ColumnOptionDef {
                        name: None,
                        option: ColumnOption::CharacterSet(ObjectName::from(vec![Ident::new(
                            "utf8mb4"
                        )]))
                    }],
                },],
                columns
            );
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_lock_tables() {
    mysql().one_statement_parses_to(
        "LOCK TABLES trans t READ, customer WRITE",
        "LOCK TABLES trans AS t READ, customer WRITE",
    );
    mysql().verified_stmt("LOCK TABLES trans AS t READ, customer WRITE");
    mysql().verified_stmt("LOCK TABLES trans AS t READ LOCAL, customer WRITE");
    mysql().verified_stmt("LOCK TABLES trans AS t READ, customer LOW_PRIORITY WRITE");
    mysql().verified_stmt("UNLOCK TABLES");
}

#[test]
fn parse_select_table_with_index_hints() {
    mysql()
        .verified_stmt("SELECT * FROM t1 USE INDEX (i1) IGNORE INDEX FOR ORDER BY (i2) ORDER BY a");
    mysql().verified_stmt("SELECT * FROM t1 USE INDEX (i1) USE INDEX (i1, i1)");
    mysql().verified_stmt(
        "SELECT * FROM t1 USE INDEX () IGNORE INDEX (i2) USE INDEX (i1) USE INDEX (i2)",
    );
    mysql().verified_stmt("SELECT * FROM t1 FORCE INDEX FOR JOIN (i2)");
    mysql().verified_stmt("SELECT * FROM t1 IGNORE INDEX FOR JOIN (i2)");
    mysql().verified_stmt(
        "SELECT * FROM t USE INDEX (index1) IGNORE INDEX FOR ORDER BY (index1) IGNORE INDEX FOR GROUP BY (index1) WHERE A = B",
    );
}

#[test]
fn parse_json_table() {
    mysql().verified_only_select("SELECT * FROM JSON_TABLE('[[1, 2], [3, 4]]', '$[*]' COLUMNS(a INT PATH '$[0]', b INT PATH '$[1]')) AS t");
    mysql().verified_only_select(
        r#"SELECT * FROM JSON_TABLE('["x", "y"]', '$[*]' COLUMNS(a VARCHAR(20) PATH '$')) AS t"#,
    );
    // with a bound parameter
    mysql().verified_only_select(
        r#"SELECT * FROM JSON_TABLE(?, '$[*]' COLUMNS(a VARCHAR(20) PATH '$')) AS t"#,
    );
    // quote escaping
    mysql().verified_only_select(r#"SELECT * FROM JSON_TABLE('{"''": [1,2,3]}', '$."''"[*]' COLUMNS(a VARCHAR(20) PATH '$')) AS t"#);
    // double quotes
    mysql().verified_only_select(
        r#"SELECT * FROM JSON_TABLE("[]", "$[*]" COLUMNS(a VARCHAR(20) PATH "$")) AS t"#,
    );
    // exists
    mysql().verified_only_select(r#"SELECT * FROM JSON_TABLE('[{}, {"x":1}]', '$[*]' COLUMNS(x INT EXISTS PATH '$.x')) AS t"#);
    // error handling
    mysql().verified_only_select(
        r#"SELECT * FROM JSON_TABLE('[1,2]', '$[*]' COLUMNS(x INT PATH '$' ERROR ON ERROR)) AS t"#,
    );
    mysql().verified_only_select(
        r#"SELECT * FROM JSON_TABLE('[1,2]', '$[*]' COLUMNS(x INT PATH '$' ERROR ON EMPTY)) AS t"#,
    );
    mysql().verified_only_select(r#"SELECT * FROM JSON_TABLE('[1,2]', '$[*]' COLUMNS(x INT PATH '$' ERROR ON EMPTY DEFAULT '0' ON ERROR)) AS t"#);
    mysql().verified_only_select(
        r#"SELECT jt.* FROM JSON_TABLE('["Alice", "Bob", "Charlie"]', '$[*]' COLUMNS(row_num FOR ORDINALITY, name VARCHAR(50) PATH '$')) AS jt"#,
    );
    mysql().verified_only_select(
        r#"SELECT * FROM JSON_TABLE('[ {"a": 1, "b": [11,111]}, {"a": 2, "b": [22,222]}, {"a":3}]', '$[*]' COLUMNS(a INT PATH '$.a', NESTED PATH '$.b[*]' COLUMNS (b INT PATH '$'))) AS jt"#,
    );
    assert_eq!(
        mysql()
            .verified_only_select(
                r#"SELECT * FROM JSON_TABLE('[1,2]', '$[*]' COLUMNS(x INT PATH '$' DEFAULT '0' ON EMPTY NULL ON ERROR)) AS t"#
            )
            .from[0]
            .relation,
        TableFactor::JsonTable {
            json_expr: Expr::Value(Value::SingleQuotedString("[1,2]".to_string())),
            json_path: Value::SingleQuotedString("$[*]".to_string()),
            columns: vec![
                JsonTableColumn::Named(JsonTableNamedColumn {
                    name: Ident::new("x"),
                    r#type: DataType::Int(None),
                    path: Value::SingleQuotedString("$".to_string()),
                    exists: false,
                    on_empty: Some(JsonTableColumnErrorHandling::Default(Value::SingleQuotedString("0".to_string()))),
                    on_error: Some(JsonTableColumnErrorHandling::Null),
                }),
            ],
            alias: Some(TableAlias {
                name: Ident::new("t"),
                columns: vec![],
            }),
        }
    );
}

#[test]
fn test_group_concat() {
    // examples taken from mysql docs
    // https://dev.mysql.com/doc/refman/8.0/en/aggregate-functions.html#function_group-concat
    mysql_and_generic().verified_expr("GROUP_CONCAT(DISTINCT test_score)");
    mysql_and_generic().verified_expr("GROUP_CONCAT(test_score ORDER BY test_score)");
    mysql_and_generic().verified_expr("GROUP_CONCAT(test_score SEPARATOR ' ')");
    mysql_and_generic()
        .verified_expr("GROUP_CONCAT(DISTINCT test_score ORDER BY test_score DESC SEPARATOR ' ')");
}

/// The XOR binary operator is only supported in MySQL
#[test]
fn parse_logical_xor() {
    let sql = "SELECT true XOR true, false XOR false, true XOR false, false XOR true";
    let select = mysql_and_generic().verified_only_select(sql);
    assert_eq!(
        SelectItem::UnnamedExpr(Expr::BinaryOp {
            left: Box::new(Expr::Value(Value::Boolean(true))),
            op: BinaryOperator::Xor,
            right: Box::new(Expr::Value(Value::Boolean(true))),
        }),
        select.projection[0]
    );
    assert_eq!(
        SelectItem::UnnamedExpr(Expr::BinaryOp {
            left: Box::new(Expr::Value(Value::Boolean(false))),
            op: BinaryOperator::Xor,
            right: Box::new(Expr::Value(Value::Boolean(false))),
        }),
        select.projection[1]
    );
    assert_eq!(
        SelectItem::UnnamedExpr(Expr::BinaryOp {
            left: Box::new(Expr::Value(Value::Boolean(true))),
            op: BinaryOperator::Xor,
            right: Box::new(Expr::Value(Value::Boolean(false))),
        }),
        select.projection[2]
    );
    assert_eq!(
        SelectItem::UnnamedExpr(Expr::BinaryOp {
            left: Box::new(Expr::Value(Value::Boolean(false))),
            op: BinaryOperator::Xor,
            right: Box::new(Expr::Value(Value::Boolean(true))),
        }),
        select.projection[3]
    );
}

#[test]
fn parse_bitstring_literal() {
    let select = mysql_and_generic().verified_only_select("SELECT B'111'");
    assert_eq!(
        select.projection,
        vec![SelectItem::UnnamedExpr(Expr::Value(
            Value::SingleQuotedByteStringLiteral("111".to_string())
        ))]
    );
}

#[test]
fn parse_grant() {
    let sql = "GRANT ALL ON *.* TO 'jeffrey'@'%'";
    let stmt = mysql().verified_stmt(sql);
    if let Statement::Grant {
        privileges,
        objects,
        grantees,
        with_grant_option,
        granted_by,
    } = stmt
    {
        assert_eq!(
            privileges,
            Privileges::All {
                with_privileges_keyword: false
            }
        );
        assert_eq!(
            objects,
            GrantObjects::Tables(vec![ObjectName::from(vec!["*".into(), "*".into()])])
        );
        assert!(!with_grant_option);
        assert!(granted_by.is_none());
        if let [Grantee {
            grantee_type: GranteesType::None,
            name: Some(GranteeName::UserHost { user, host }),
        }] = grantees.as_slice()
        {
            assert_eq!(user.value, "jeffrey");
            assert_eq!(user.quote_style, Some('\''));
            assert_eq!(host.value, "%");
            assert_eq!(host.quote_style, Some('\''));
        } else {
            unreachable!()
        }
    } else {
        unreachable!()
    }
}

#[test]
fn parse_revoke() {
    let sql = "REVOKE ALL ON db1.* FROM 'jeffrey'@'%'";
    let stmt = mysql_and_generic().verified_stmt(sql);
    if let Statement::Revoke {
        privileges,
        objects,
        grantees,
        granted_by,
        cascade,
    } = stmt
    {
        assert_eq!(
            privileges,
            Privileges::All {
                with_privileges_keyword: false
            }
        );
        assert_eq!(
            objects,
            GrantObjects::Tables(vec![ObjectName::from(vec!["db1".into(), "*".into()])])
        );
        if let [Grantee {
            grantee_type: GranteesType::None,
            name: Some(GranteeName::UserHost { user, host }),
        }] = grantees.as_slice()
        {
            assert_eq!(user.value, "jeffrey");
            assert_eq!(user.quote_style, Some('\''));
            assert_eq!(host.value, "%");
            assert_eq!(host.quote_style, Some('\''));
        } else {
            unreachable!()
        }
        assert!(granted_by.is_none());
        assert!(cascade.is_none());
    } else {
        unreachable!()
    }
}

#[test]
fn parse_create_view_algorithm_param() {
    let sql = "CREATE ALGORITHM = MERGE VIEW foo AS SELECT 1";
    let stmt = mysql().verified_stmt(sql);
    if let Statement::CreateView {
        params:
            Some(CreateViewParams {
                algorithm,
                definer,
                security,
            }),
        ..
    } = stmt
    {
        assert_eq!(algorithm, Some(CreateViewAlgorithm::Merge));
        assert!(definer.is_none());
        assert!(security.is_none());
    } else {
        unreachable!()
    }
    mysql().verified_stmt("CREATE ALGORITHM = UNDEFINED VIEW foo AS SELECT 1");
    mysql().verified_stmt("CREATE ALGORITHM = TEMPTABLE VIEW foo AS SELECT 1");
}

#[test]
fn parse_create_view_definer_param() {
    let sql = "CREATE DEFINER = 'jeffrey'@'localhost' VIEW foo AS SELECT 1";
    let stmt = mysql().verified_stmt(sql);
    if let Statement::CreateView {
        params:
            Some(CreateViewParams {
                algorithm,
                definer,
                security,
            }),
        ..
    } = stmt
    {
        assert!(algorithm.is_none());
        if let Some(GranteeName::UserHost { user, host }) = definer {
            assert_eq!(user.value, "jeffrey");
            assert_eq!(user.quote_style, Some('\''));
            assert_eq!(host.value, "localhost");
            assert_eq!(host.quote_style, Some('\''));
        } else {
            unreachable!()
        }
        assert!(security.is_none());
    } else {
        unreachable!()
    }
}

#[test]
fn parse_create_view_security_param() {
    let sql = "CREATE SQL SECURITY DEFINER VIEW foo AS SELECT 1";
    let stmt = mysql().verified_stmt(sql);
    if let Statement::CreateView {
        params:
            Some(CreateViewParams {
                algorithm,
                definer,
                security,
            }),
        ..
    } = stmt
    {
        assert!(algorithm.is_none());
        assert!(definer.is_none());
        assert_eq!(security, Some(CreateViewSecurity::Definer));
    } else {
        unreachable!()
    }
    mysql().verified_stmt("CREATE SQL SECURITY INVOKER VIEW foo AS SELECT 1");
}

#[test]
fn parse_create_view_multiple_params() {
    let sql = "CREATE ALGORITHM = UNDEFINED DEFINER = `root`@`%` SQL SECURITY INVOKER VIEW foo AS SELECT 1";
    let stmt = mysql().verified_stmt(sql);
    if let Statement::CreateView {
        params:
            Some(CreateViewParams {
                algorithm,
                definer,
                security,
            }),
        ..
    } = stmt
    {
        assert_eq!(algorithm, Some(CreateViewAlgorithm::Undefined));
        if let Some(GranteeName::UserHost { user, host }) = definer {
            assert_eq!(user.value, "root");
            assert_eq!(user.quote_style, Some('`'));
            assert_eq!(host.value, "%");
            assert_eq!(host.quote_style, Some('`'));
        } else {
            unreachable!()
        }
        assert_eq!(security, Some(CreateViewSecurity::Invoker));
    } else {
        unreachable!()
    }
}

#[test]
fn parse_longblob_type() {
    let sql = "CREATE TABLE foo (bar LONGBLOB)";
    let stmt = mysql_and_generic().verified_stmt(sql);
    if let Statement::CreateTable(CreateTable { columns, .. }) = stmt {
        assert_eq!(columns.len(), 1);
        assert_eq!(columns[0].data_type, DataType::LongBlob);
    } else {
        unreachable!()
    }
    mysql_and_generic().verified_stmt("CREATE TABLE foo (bar TINYBLOB)");
    mysql_and_generic().verified_stmt("CREATE TABLE foo (bar MEDIUMBLOB)");
    mysql_and_generic().verified_stmt("CREATE TABLE foo (bar TINYTEXT)");
    mysql_and_generic().verified_stmt("CREATE TABLE foo (bar MEDIUMTEXT)");
    mysql_and_generic().verified_stmt("CREATE TABLE foo (bar LONGTEXT)");
}

#[test]
fn parse_begin_without_transaction() {
    mysql().verified_stmt("BEGIN");
}

#[test]
fn parse_double_precision() {
    mysql().verified_stmt("CREATE TABLE foo (bar DOUBLE)");
    mysql().verified_stmt("CREATE TABLE foo (bar DOUBLE(11,0))");
    mysql().one_statement_parses_to(
        "CREATE TABLE foo (bar DOUBLE(11, 0))",
        "CREATE TABLE foo (bar DOUBLE(11,0))",
    );
}

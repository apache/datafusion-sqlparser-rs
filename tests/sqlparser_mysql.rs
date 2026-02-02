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
        &Expr::Value((Value::SingleQuotedString("single".to_string())).with_empty_span()),
        expr_from_projection(&select.projection[0])
    );
    assert_eq!(
        &Expr::Value((Value::DoubleQuotedString("double".to_string())).with_empty_span()),
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
            mysql_and_generic().verified_stmt(&format!("USE {object_name}")),
            Statement::Use(Use::Object(ObjectName::from(vec![Ident::new(
                object_name.to_string()
            )])))
        );
        for &quote in &quote_styles {
            // Test single identifier with different type of quotes
            assert_eq!(
                mysql_and_generic().verified_stmt(&format!("USE {quote}{object_name}{quote}")),
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
        Statement::Set(Set::SingleAssignment {
            scope: Some(ContextModifier::Local),
            hivevar: false,
            variable: ObjectName::from(vec!["autocommit".into()]),
            values: vec![Expr::value(number("1"))],
        })
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
                    options: vec![
                        ColumnOptionDef {
                            name: None,
                            option: ColumnOption::PrimaryKey(PrimaryKeyConstraint {
                                name: None,
                                index_name: None,
                                index_type: None,
                                columns: vec![],
                                index_options: vec![],
                                characteristics: None,
                            }),
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
    let columns = columns
        .into_iter()
        .map(|ident| IndexColumn {
            column: OrderByExpr {
                expr: Expr::Identifier(ident),
                options: OrderByOptions {
                    asc: None,
                    nulls_first: None,
                },
                with_fill: None,
            },
            operator_class: None,
        })
        .collect();
    match unique_index_type_display {
        Some(index_type_display) => UniqueConstraint {
            name,
            index_name,
            index_type_display,
            index_type,
            columns,
            index_options,
            characteristics,
            nulls_distinct: NullsDistinctOption::None,
        }
        .into(),
        None => PrimaryKeyConstraint {
            name,
            index_name,
            index_type,
            columns,
            index_options,
            characteristics,
        }
        .into(),
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
                            options: vec![
                                ColumnOptionDef {
                                    name: None,
                                    option: ColumnOption::PrimaryKey(PrimaryKeyConstraint {
                                        name: None,
                                        index_name: None,
                                        index_type: None,
                                        columns: vec![],
                                        index_options: vec![],
                                        characteristics: None,
                                    }),
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
fn parse_prefix_key_part() {
    let expected = vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::value(
        number("10"),
    )))];
    for sql in [
        "CREATE INDEX idx_index ON t(textcol(10))",
        "ALTER TABLE tab ADD INDEX idx_index (textcol(10))",
        "ALTER TABLE tab ADD PRIMARY KEY (textcol(10))",
        "ALTER TABLE tab ADD UNIQUE KEY (textcol(10))",
        "ALTER TABLE tab ADD UNIQUE KEY (textcol(10))",
        "ALTER TABLE tab ADD FULLTEXT INDEX (textcol(10))",
        "CREATE TABLE t (textcol TEXT, INDEX idx_index (textcol(10)))",
    ] {
        match index_column(mysql_and_generic().verified_stmt(sql)) {
            Expr::Function(Function {
                name,
                args: FunctionArguments::List(FunctionArgumentList { args, .. }),
                ..
            }) => {
                assert_eq!(name.to_string(), "textcol");
                assert_eq!(args, expected);
            }
            expr => panic!("unexpected expression {expr} for {sql}"),
        }
    }
}

#[test]
fn test_functional_key_part() {
    assert_eq!(
        index_column(
            mysql_and_generic()
                .verified_stmt("CREATE INDEX idx_index ON t((col COLLATE utf8mb4_bin) DESC)")
        ),
        Expr::Nested(Box::new(Expr::Collate {
            expr: Box::new(Expr::Identifier("col".into())),
            collation: ObjectName(vec![sqlparser::ast::ObjectNamePart::Identifier(
                Ident::new("utf8mb4_bin")
            )]),
        }))
    );
    assert_eq!(
        index_column(mysql_and_generic().verified_stmt(
            r#"CREATE TABLE t (jsoncol JSON, PRIMARY KEY ((CAST(col ->> '$.id' AS UNSIGNED)) ASC))"#
        )),
        Expr::Nested(Box::new(Expr::Cast {
            kind: CastKind::Cast,
            expr: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Identifier(Ident::new("col"))),
                op: BinaryOperator::LongArrow,
                right: Box::new(Expr::Value(
                    Value::SingleQuotedString("$.id".to_string()).with_empty_span()
                )),
            }),
            data_type: DataType::Unsigned,
            array: false,
            format: None,
        })),
    );
    assert_eq!(
        index_column(mysql_and_generic().verified_stmt(
            r#"CREATE TABLE t (jsoncol JSON, PRIMARY KEY ((CAST(col ->> '$.fields' AS UNSIGNED ARRAY)) ASC))"#
        )),
        Expr::Nested(Box::new(Expr::Cast {
            kind: CastKind::Cast,
            expr: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Identifier(Ident::new("col"))),
                op: BinaryOperator::LongArrow,
                right: Box::new(Expr::Value(
                    Value::SingleQuotedString("$.fields".to_string()).with_empty_span()
                )),
            }),
            data_type: DataType::Unsigned,
            array: true,
            format: None,
        })),
    );
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
            Statement::CreateTable(CreateTable {
                name,
                table_options,
                ..
            }) => {
                assert_eq!(name.to_string(), "foo");

                let plain_options = match table_options {
                    CreateTableOptions::Plain(options) => options,
                    _ => unreachable!(),
                };
                let comment = match plain_options.first().unwrap() {
                    SqlOption::Comment(CommentDef::WithEq(c))
                    | SqlOption::Comment(CommentDef::WithoutEq(c)) => c,
                    _ => unreachable!(),
                };
                assert_eq!(comment, "baz");
            }
            _ => unreachable!(),
        }
    }
}

#[test]
fn parse_create_table_auto_increment_offset() {
    let sql =
        "CREATE TABLE foo (bar INT NOT NULL AUTO_INCREMENT) ENGINE = InnoDB AUTO_INCREMENT = 123";

    match mysql().verified_stmt(sql) {
        Statement::CreateTable(CreateTable {
            name,
            table_options,
            ..
        }) => {
            assert_eq!(name.to_string(), "foo");

            let plain_options = match table_options {
                CreateTableOptions::Plain(options) => options,
                _ => unreachable!(),
            };

            assert!(plain_options.contains(&SqlOption::KeyValue {
                key: Ident::new("AUTO_INCREMENT"),
                value: Expr::Value(test_utils::number("123").with_empty_span())
            }));
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_table_multiple_options_order_independent() {
    let sql1 = "CREATE TABLE mytable (id INT) ENGINE=InnoDB ROW_FORMAT=DYNAMIC KEY_BLOCK_SIZE=8 COMMENT='abc'";
    let sql2 = "CREATE TABLE mytable (id INT) KEY_BLOCK_SIZE=8 COMMENT='abc' ENGINE=InnoDB ROW_FORMAT=DYNAMIC";
    let sql3 = "CREATE TABLE mytable (id INT) ROW_FORMAT=DYNAMIC KEY_BLOCK_SIZE=8 COMMENT='abc' ENGINE=InnoDB";

    for sql in [sql1, sql2, sql3] {
        match mysql().parse_sql_statements(sql).unwrap().pop().unwrap() {
            Statement::CreateTable(CreateTable {
                name,
                table_options,
                ..
            }) => {
                assert_eq!(name.to_string(), "mytable");

                let plain_options = match table_options {
                    CreateTableOptions::Plain(options) => options,
                    _ => unreachable!(),
                };

                assert!(plain_options.contains(&SqlOption::NamedParenthesizedList(
                    NamedParenthesizedList {
                        key: Ident::new("ENGINE"),
                        name: Some(Ident::new("InnoDB")),
                        values: vec![]
                    }
                )));

                assert!(plain_options.contains(&SqlOption::KeyValue {
                    key: Ident::new("KEY_BLOCK_SIZE"),
                    value: Expr::Value(test_utils::number("8").with_empty_span())
                }));

                assert!(plain_options
                    .contains(&SqlOption::Comment(CommentDef::WithEq("abc".to_owned()))));

                assert!(plain_options.contains(&SqlOption::KeyValue {
                    key: Ident::new("ROW_FORMAT"),
                    value: Expr::Identifier(Ident::new("DYNAMIC".to_owned()))
                }));
            }
            _ => unreachable!(),
        }
    }
}

#[test]
fn parse_create_table_with_all_table_options() {
    let sql =
        "CREATE TABLE foo (bar INT NOT NULL AUTO_INCREMENT) ENGINE = InnoDB AUTO_INCREMENT = 123 DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci INSERT_METHOD = FIRST KEY_BLOCK_SIZE = 8 ROW_FORMAT = DYNAMIC DATA DIRECTORY = '/var/lib/mysql/data' INDEX DIRECTORY = '/var/lib/mysql/index' PACK_KEYS = 1 STATS_AUTO_RECALC = 1 STATS_PERSISTENT = 0 STATS_SAMPLE_PAGES = 128 DELAY_KEY_WRITE = 1 COMPRESSION = 'ZLIB' ENCRYPTION = 'Y' MAX_ROWS = 10000 MIN_ROWS = 10 AUTOEXTEND_SIZE = 64 AVG_ROW_LENGTH = 128 CHECKSUM = 1 CONNECTION = 'mysql://localhost' ENGINE_ATTRIBUTE = 'primary' PASSWORD = 'secure_password' SECONDARY_ENGINE_ATTRIBUTE = 'secondary_attr' START TRANSACTION TABLESPACE my_tablespace STORAGE DISK UNION = (table1, table2, table3)";

    match mysql().verified_stmt(sql) {
        Statement::CreateTable(CreateTable {
            name,
            table_options,
            ..
        }) => {
            assert_eq!(name, vec![Ident::new("foo".to_owned())].into());

            let plain_options = match table_options {
                CreateTableOptions::Plain(options) => options,
                _ => unreachable!(),
            };

            assert!(plain_options.contains(&SqlOption::NamedParenthesizedList(
                NamedParenthesizedList {
                    key: Ident::new("ENGINE"),
                    name: Some(Ident::new("InnoDB")),
                    values: vec![]
                }
            )));

            assert!(plain_options.contains(&SqlOption::KeyValue {
                key: Ident::new("COLLATE"),
                value: Expr::Identifier(Ident::new("utf8mb4_0900_ai_ci".to_owned()))
            }));
            assert!(plain_options.contains(&SqlOption::KeyValue {
                key: Ident::new("DEFAULT CHARSET"),
                value: Expr::Identifier(Ident::new("utf8mb4".to_owned()))
            }));
            assert!(plain_options.contains(&SqlOption::KeyValue {
                key: Ident::new("AUTO_INCREMENT"),
                value: Expr::value(test_utils::number("123"))
            }));
            assert!(plain_options.contains(&SqlOption::KeyValue {
                key: Ident::new("KEY_BLOCK_SIZE"),
                value: Expr::value(test_utils::number("8"))
            }));
            assert!(plain_options.contains(&SqlOption::KeyValue {
                key: Ident::new("ROW_FORMAT"),
                value: Expr::Identifier(Ident::new("DYNAMIC".to_owned()))
            }));
            assert!(plain_options.contains(&SqlOption::KeyValue {
                key: Ident::new("PACK_KEYS"),
                value: Expr::value(test_utils::number("1"))
            }));
            assert!(plain_options.contains(&SqlOption::KeyValue {
                key: Ident::new("STATS_AUTO_RECALC"),
                value: Expr::value(test_utils::number("1"))
            }));
            assert!(plain_options.contains(&SqlOption::KeyValue {
                key: Ident::new("STATS_PERSISTENT"),
                value: Expr::value(test_utils::number("0"))
            }));
            assert!(plain_options.contains(&SqlOption::KeyValue {
                key: Ident::new("STATS_SAMPLE_PAGES"),
                value: Expr::value(test_utils::number("128"))
            }));
            assert!(plain_options.contains(&SqlOption::KeyValue {
                key: Ident::new("STATS_SAMPLE_PAGES"),
                value: Expr::value(test_utils::number("128"))
            }));
            assert!(plain_options.contains(&SqlOption::KeyValue {
                key: Ident::new("INSERT_METHOD"),
                value: Expr::Identifier(Ident::new("FIRST".to_owned()))
            }));
            assert!(plain_options.contains(&SqlOption::KeyValue {
                key: Ident::new("COMPRESSION"),
                value: Expr::value(Value::SingleQuotedString("ZLIB".to_owned()))
            }));
            assert!(plain_options.contains(&SqlOption::KeyValue {
                key: Ident::new("ENCRYPTION"),
                value: Expr::value(Value::SingleQuotedString("Y".to_owned()))
            }));
            assert!(plain_options.contains(&SqlOption::KeyValue {
                key: Ident::new("MAX_ROWS"),
                value: Expr::value(test_utils::number("10000"))
            }));
            assert!(plain_options.contains(&SqlOption::KeyValue {
                key: Ident::new("MIN_ROWS"),
                value: Expr::value(test_utils::number("10"))
            }));
            assert!(plain_options.contains(&SqlOption::KeyValue {
                key: Ident::new("AUTOEXTEND_SIZE"),
                value: Expr::value(test_utils::number("64"))
            }));
            assert!(plain_options.contains(&SqlOption::KeyValue {
                key: Ident::new("AVG_ROW_LENGTH"),
                value: Expr::value(test_utils::number("128"))
            }));
            assert!(plain_options.contains(&SqlOption::KeyValue {
                key: Ident::new("CHECKSUM"),
                value: Expr::value(test_utils::number("1"))
            }));
            assert!(plain_options.contains(&SqlOption::KeyValue {
                key: Ident::new("CONNECTION"),
                value: Expr::value(Value::SingleQuotedString("mysql://localhost".to_owned()))
            }));
            assert!(plain_options.contains(&SqlOption::KeyValue {
                key: Ident::new("ENGINE_ATTRIBUTE"),
                value: Expr::value(Value::SingleQuotedString("primary".to_owned()))
            }));
            assert!(plain_options.contains(&SqlOption::KeyValue {
                key: Ident::new("PASSWORD"),
                value: Expr::value(Value::SingleQuotedString("secure_password".to_owned()))
            }));
            assert!(plain_options.contains(&SqlOption::KeyValue {
                key: Ident::new("SECONDARY_ENGINE_ATTRIBUTE"),
                value: Expr::value(Value::SingleQuotedString("secondary_attr".to_owned()))
            }));
            assert!(plain_options.contains(&SqlOption::Ident(Ident::new(
                "START TRANSACTION".to_owned()
            ))));
            assert!(
                plain_options.contains(&SqlOption::TableSpace(TablespaceOption {
                    name: "my_tablespace".to_string(),
                    storage: Some(StorageType::Disk),
                }))
            );

            assert!(plain_options.contains(&SqlOption::NamedParenthesizedList(
                NamedParenthesizedList {
                    key: Ident::new("UNION"),
                    name: None,
                    values: vec![
                        Ident::new("table1".to_string()),
                        Ident::new("table2".to_string()),
                        Ident::new("table3".to_string())
                    ]
                }
            )));

            assert!(plain_options.contains(&SqlOption::KeyValue {
                key: Ident::new("DATA DIRECTORY"),
                value: Expr::value(Value::SingleQuotedString("/var/lib/mysql/data".to_owned()))
            }));
            assert!(plain_options.contains(&SqlOption::KeyValue {
                key: Ident::new("INDEX DIRECTORY"),
                value: Expr::value(Value::SingleQuotedString("/var/lib/mysql/index".to_owned()))
            }));
        }
        _ => unreachable!(),
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
    let sql = "CREATE TABLE foo (id INT(11)) ENGINE = InnoDB DEFAULT CHARSET = utf8mb3";
    match mysql().verified_stmt(sql) {
        Statement::CreateTable(CreateTable {
            name,
            columns,
            table_options,
            ..
        }) => {
            assert_eq!(name.to_string(), "foo");
            assert_eq!(
                vec![ColumnDef {
                    name: Ident::new("id"),
                    data_type: DataType::Int(Some(11)),
                    options: vec![],
                },],
                columns
            );

            let plain_options = match table_options {
                CreateTableOptions::Plain(options) => options,
                _ => unreachable!(),
            };

            assert!(plain_options.contains(&SqlOption::KeyValue {
                key: Ident::new("DEFAULT CHARSET"),
                value: Expr::Identifier(Ident::new("utf8mb3".to_owned()))
            }));

            assert!(plain_options.contains(&SqlOption::NamedParenthesizedList(
                NamedParenthesizedList {
                    key: Ident::new("ENGINE"),
                    name: Some(Ident::new("InnoDB")),
                    values: vec![]
                }
            )));
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_table_collate() {
    let sql = "CREATE TABLE foo (id INT(11)) COLLATE = utf8mb4_0900_ai_ci";
    match mysql().verified_stmt(sql) {
        Statement::CreateTable(CreateTable {
            name,
            columns,
            table_options,
            ..
        }) => {
            assert_eq!(name.to_string(), "foo");
            assert_eq!(
                vec![ColumnDef {
                    name: Ident::new("id"),
                    data_type: DataType::Int(Some(11)),
                    options: vec![],
                },],
                columns
            );

            let plain_options = match table_options {
                CreateTableOptions::Plain(options) => options,
                _ => unreachable!(),
            };

            assert!(plain_options.contains(&SqlOption::KeyValue {
                key: Ident::new("COLLATE"),
                value: Expr::Identifier(Ident::new("utf8mb4_0900_ai_ci".to_owned()))
            }));
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_table_both_options_and_as_query() {
    let sql = "CREATE TABLE foo (id INT(11)) ENGINE = InnoDB DEFAULT CHARSET = utf8mb3 COLLATE = utf8mb4_0900_ai_ci AS SELECT 1";
    match mysql_and_generic().verified_stmt(sql) {
        Statement::CreateTable(CreateTable {
            name,
            query,
            table_options,
            ..
        }) => {
            assert_eq!(name.to_string(), "foo");

            let plain_options = match table_options {
                CreateTableOptions::Plain(options) => options,
                _ => unreachable!(),
            };

            assert!(plain_options.contains(&SqlOption::KeyValue {
                key: Ident::new("COLLATE"),
                value: Expr::Identifier(Ident::new("utf8mb4_0900_ai_ci".to_owned()))
            }));

            assert_eq!(
                query.unwrap().body.as_select().unwrap().projection,
                vec![SelectItem::UnnamedExpr(Expr::Value(
                    (number("1")).with_empty_span()
                ))]
            );
        }
        _ => unreachable!(),
    }

    let sql =
        r"CREATE TABLE foo (id INT(11)) ENGINE = InnoDB AS SELECT 1 DEFAULT CHARSET = utf8mb3";
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
fn parse_create_table_options_comma_separated() {
    let sql = "CREATE TABLE t (x INT) DEFAULT CHARSET = utf8mb4, ENGINE = InnoDB , AUTO_INCREMENT 1 DATA DIRECTORY '/var/lib/mysql/data'";
    let canonical = "CREATE TABLE t (x INT) DEFAULT CHARSET = utf8mb4 ENGINE = InnoDB AUTO_INCREMENT = 1 DATA DIRECTORY = '/var/lib/mysql/data'";
    mysql_and_generic().one_statement_parses_to(sql, canonical);
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
                    options: vec![ColumnOptionDef {
                        name: None,
                        option: ColumnOption::PrimaryKey(PrimaryKeyConstraint {
                            name: None,
                            index_name: None,
                            index_type: None,
                            columns: vec![],
                            index_options: vec![],
                            characteristics: None,
                        }),
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
                optimizer_hint: None,
                distinct: None,
                top: None,
                top_before_distinct: false,
                projection: vec![SelectItem::UnnamedExpr(Expr::Identifier(Ident {
                    value: "quoted ` identifier".into(),
                    quote_style: Some('`'),
                    span: Span::empty(),
                }))],
                exclude: None,
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
                flavor: SelectFlavor::Standard,
            }))),
            order_by: None,
            limit_clause: None,
            fetch: None,
            locks: vec![],
            for_clause: None,
            settings: None,
            format_clause: None,
            pipe_operators: vec![],
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
                require_semicolon_stmt_delimiter: true,
            }
        )
        .verified_stmt(sql),
        Statement::Query(Box::new(Query {
            with: None,
            body: Box::new(SetExpr::Select(Box::new(Select {
                select_token: AttachedToken::empty(),
                optimizer_hint: None,
                distinct: None,
                top: None,
                top_before_distinct: false,
                projection: vec![SelectItem::UnnamedExpr(Expr::Identifier(Ident {
                    value: "quoted `` identifier".into(),
                    quote_style: Some('`'),
                    span: Span::empty(),
                }))],
                exclude: None,
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
                flavor: SelectFlavor::Standard,
            }))),
            order_by: None,
            limit_clause: None,
            fetch: None,
            locks: vec![],
            for_clause: None,
            settings: None,
            format_clause: None,
            pipe_operators: vec![],
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
                optimizer_hint: None,
                distinct: None,
                top: None,
                top_before_distinct: false,
                projection: vec![SelectItem::UnnamedExpr(Expr::Identifier(Ident {
                    value: "`quoted identifier`".into(),
                    quote_style: Some('`'),
                    span: Span::empty(),
                }))],
                exclude: None,
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
                flavor: SelectFlavor::Standard,
            }))),
            order_by: None,
            limit_clause: None,
            fetch: None,
            locks: vec![],
            for_clause: None,
            settings: None,
            format_clause: None,
            pipe_operators: vec![],
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
                optimizer_hint: None,
                distinct: None,
                top: None,
                top_before_distinct: false,
                projection: vec![SelectItem::UnnamedExpr(Expr::Identifier(Ident {
                    value: "``quoted identifier``".into(),
                    quote_style: Some('`'),
                    span: Span::empty(),
                }))],
                exclude: None,
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
                flavor: SelectFlavor::Standard,
            }))),
            order_by: None,
            limit_clause: None,
            fetch: None,
            locks: vec![],
            for_clause: None,
            settings: None,
            format_clause: None,
            pipe_operators: vec![],
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
                        options: vec![],
                    },
                    ColumnDef {
                        name: Ident::new("bar_smallint"),
                        data_type: DataType::SmallInt(Some(5)),
                        options: vec![],
                    },
                    ColumnDef {
                        name: Ident::new("bar_mediumint"),
                        data_type: DataType::MediumInt(Some(6)),
                        options: vec![],
                    },
                    ColumnDef {
                        name: Ident::new("bar_int"),
                        data_type: DataType::Int(Some(11)),
                        options: vec![],
                    },
                    ColumnDef {
                        name: Ident::new("bar_bigint"),
                        data_type: DataType::BigInt(Some(20)),
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
                        data_type: DataType::TinyIntUnsigned(Some(3)),
                        options: vec![],
                    },
                    ColumnDef {
                        name: Ident::new("bar_smallint"),
                        data_type: DataType::SmallIntUnsigned(Some(5)),
                        options: vec![],
                    },
                    ColumnDef {
                        name: Ident::new("bar_mediumint"),
                        data_type: DataType::MediumIntUnsigned(Some(13)),
                        options: vec![],
                    },
                    ColumnDef {
                        name: Ident::new("bar_int"),
                        data_type: DataType::IntUnsigned(Some(11)),
                        options: vec![],
                    },
                    ColumnDef {
                        name: Ident::new("bar_bigint"),
                        data_type: DataType::BigIntUnsigned(Some(20)),
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
fn parse_signed_data_types() {
    let sql = "CREATE TABLE foo (bar_tinyint TINYINT(3) SIGNED, bar_smallint SMALLINT(5) SIGNED, bar_mediumint MEDIUMINT(13) SIGNED, bar_int INT(11) SIGNED, bar_bigint BIGINT(20) SIGNED)";
    let canonical = "CREATE TABLE foo (bar_tinyint TINYINT(3), bar_smallint SMALLINT(5), bar_mediumint MEDIUMINT(13), bar_int INT(11), bar_bigint BIGINT(20))";
    match mysql().one_statement_parses_to(sql, canonical) {
        Statement::CreateTable(CreateTable { name, columns, .. }) => {
            assert_eq!(name.to_string(), "foo");
            assert_eq!(
                vec![
                    ColumnDef {
                        name: Ident::new("bar_tinyint"),
                        data_type: DataType::TinyInt(Some(3)),
                        options: vec![],
                    },
                    ColumnDef {
                        name: Ident::new("bar_smallint"),
                        data_type: DataType::SmallInt(Some(5)),
                        options: vec![],
                    },
                    ColumnDef {
                        name: Ident::new("bar_mediumint"),
                        data_type: DataType::MediumInt(Some(13)),
                        options: vec![],
                    },
                    ColumnDef {
                        name: Ident::new("bar_int"),
                        data_type: DataType::Int(Some(11)),
                        options: vec![],
                    },
                    ColumnDef {
                        name: Ident::new("bar_bigint"),
                        data_type: DataType::BigInt(Some(20)),
                        options: vec![],
                    },
                ],
                columns
            );
        }
        _ => unreachable!(),
    }
    all_dialects_except(|d| d.supports_data_type_signed_suffix())
        .run_parser_method(sql, |p| p.parse_statement())
        .expect_err("SIGNED suffix should not be allowed");
}

#[test]
fn parse_deprecated_mysql_unsigned_data_types() {
    let sql = "CREATE TABLE foo (bar_decimal DECIMAL UNSIGNED, bar_decimal_prec DECIMAL(10) UNSIGNED, bar_decimal_scale DECIMAL(10,2) UNSIGNED, bar_dec DEC UNSIGNED, bar_dec_prec DEC(10) UNSIGNED, bar_dec_scale DEC(10,2) UNSIGNED, bar_float FLOAT UNSIGNED, bar_float_prec FLOAT(10) UNSIGNED, bar_float_scale FLOAT(10,2) UNSIGNED, bar_double DOUBLE UNSIGNED, bar_double_prec DOUBLE(10) UNSIGNED, bar_double_scale DOUBLE(10,2) UNSIGNED, bar_real REAL UNSIGNED, bar_double_precision DOUBLE PRECISION UNSIGNED)";
    match mysql().verified_stmt(sql) {
        Statement::CreateTable(CreateTable { name, columns, .. }) => {
            assert_eq!(name.to_string(), "foo");
            assert_eq!(
                vec![
                    ColumnDef {
                        name: Ident::new("bar_decimal"),
                        data_type: DataType::DecimalUnsigned(ExactNumberInfo::None),
                        options: vec![],
                    },
                    ColumnDef {
                        name: Ident::new("bar_decimal_prec"),
                        data_type: DataType::DecimalUnsigned(ExactNumberInfo::Precision(10)),
                        options: vec![],
                    },
                    ColumnDef {
                        name: Ident::new("bar_decimal_scale"),
                        data_type: DataType::DecimalUnsigned(ExactNumberInfo::PrecisionAndScale(
                            10, 2
                        )),
                        options: vec![],
                    },
                    ColumnDef {
                        name: Ident::new("bar_dec"),
                        data_type: DataType::DecUnsigned(ExactNumberInfo::None),
                        options: vec![],
                    },
                    ColumnDef {
                        name: Ident::new("bar_dec_prec"),
                        data_type: DataType::DecUnsigned(ExactNumberInfo::Precision(10)),
                        options: vec![],
                    },
                    ColumnDef {
                        name: Ident::new("bar_dec_scale"),
                        data_type: DataType::DecUnsigned(ExactNumberInfo::PrecisionAndScale(10, 2)),
                        options: vec![],
                    },
                    ColumnDef {
                        name: Ident::new("bar_float"),
                        data_type: DataType::FloatUnsigned(ExactNumberInfo::None),
                        options: vec![],
                    },
                    ColumnDef {
                        name: Ident::new("bar_float_prec"),
                        data_type: DataType::FloatUnsigned(ExactNumberInfo::Precision(10)),
                        options: vec![],
                    },
                    ColumnDef {
                        name: Ident::new("bar_float_scale"),
                        data_type: DataType::FloatUnsigned(ExactNumberInfo::PrecisionAndScale(
                            10, 2
                        )),
                        options: vec![],
                    },
                    ColumnDef {
                        name: Ident::new("bar_double"),
                        data_type: DataType::DoubleUnsigned(ExactNumberInfo::None),
                        options: vec![],
                    },
                    ColumnDef {
                        name: Ident::new("bar_double_prec"),
                        data_type: DataType::DoubleUnsigned(ExactNumberInfo::Precision(10)),
                        options: vec![],
                    },
                    ColumnDef {
                        name: Ident::new("bar_double_scale"),
                        data_type: DataType::DoubleUnsigned(ExactNumberInfo::PrecisionAndScale(
                            10, 2
                        )),
                        options: vec![],
                    },
                    ColumnDef {
                        name: Ident::new("bar_real"),
                        data_type: DataType::RealUnsigned,
                        options: vec![],
                    },
                    ColumnDef {
                        name: Ident::new("bar_double_precision"),
                        data_type: DataType::DoublePrecisionUnsigned,
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
                        value_keyword: false,
                        explicit_row: false,
                        rows: vec![
                            vec![
                                Expr::Value(
                                    (Value::SingleQuotedString("Test Some Inserts".to_string()))
                                        .with_empty_span()
                                ),
                                Expr::value(number("1"))
                            ],
                            vec![
                                Expr::Value(
                                    (Value::SingleQuotedString("Test Entry 2".to_string()))
                                        .with_empty_span()
                                ),
                                Expr::value(number("2"))
                            ],
                            vec![
                                Expr::Value(
                                    (Value::SingleQuotedString("Test Entry 3".to_string()))
                                        .with_empty_span()
                                ),
                                Expr::value(number("3"))
                            ]
                        ]
                    })),
                    order_by: None,
                    limit_clause: None,
                    fetch: None,
                    locks: vec![],
                    for_clause: None,
                    settings: None,
                    format_clause: None,
                    pipe_operators: vec![],
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
                        value_keyword: false,
                        explicit_row: false,
                        rows: vec![vec![
                            Expr::Value(
                                (Value::SingleQuotedString("Test Some Inserts".to_string()))
                                    .with_empty_span()
                            ),
                            Expr::value(number("1"))
                        ]]
                    })),
                    order_by: None,
                    limit_clause: None,
                    fetch: None,
                    locks: vec![],
                    for_clause: None,
                    settings: None,
                    format_clause: None,
                    pipe_operators: vec![],
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
                        value_keyword: false,
                        explicit_row: false,
                        rows: vec![vec![
                            Expr::Value(
                                (Value::SingleQuotedString("Test Some Inserts".to_string()))
                                    .with_empty_span()
                            ),
                            Expr::value(number("1"))
                        ]]
                    })),
                    order_by: None,
                    limit_clause: None,
                    fetch: None,
                    locks: vec![],
                    for_clause: None,
                    settings: None,
                    format_clause: None,
                    pipe_operators: vec![],
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
                        value_keyword: false,
                        explicit_row: false,
                        rows: vec![vec![
                            Expr::Value(
                                (Value::SingleQuotedString("Test Some Inserts".to_string()))
                                    .with_empty_span()
                            ),
                            Expr::value(number("1"))
                        ]]
                    })),
                    order_by: None,
                    limit_clause: None,
                    fetch: None,
                    locks: vec![],
                    for_clause: None,
                    settings: None,
                    format_clause: None,
                    pipe_operators: vec![],
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
                        value_keyword: false,
                        explicit_row: false,
                        rows: vec![vec![Expr::Value(
                            (Value::SingleQuotedString("2024-01-01".to_string())).with_empty_span()
                        )]]
                    })),
                    order_by: None,
                    limit_clause: None,
                    fetch: None,
                    locks: vec![],
                    for_clause: None,
                    settings: None,
                    format_clause: None,
                    pipe_operators: vec![],
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
                        value_keyword: false,
                        explicit_row: false,
                        rows: vec![vec![
                            Expr::value(number("1")),
                            Expr::Value(
                                (Value::SingleQuotedString("2024-01-01".to_string()))
                                    .with_empty_span()
                            )
                        ]]
                    })),
                    order_by: None,
                    limit_clause: None,
                    fetch: None,
                    locks: vec![],
                    for_clause: None,
                    settings: None,
                    format_clause: None,
                    pipe_operators: vec![],
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
                        value_keyword: false,
                        explicit_row: false,
                        rows: vec![vec![
                            Expr::Value(
                                (Value::SingleQuotedString("Test Some Inserts".to_string()))
                                    .with_empty_span()
                            ),
                            Expr::value(number("1"))
                        ]]
                    })),
                    order_by: None,
                    limit_clause: None,
                    fetch: None,
                    locks: vec![],
                    for_clause: None,
                    settings: None,
                    format_clause: None,
                    pipe_operators: vec![],
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
                        value_keyword: false,
                        explicit_row: false,
                        rows: vec![vec![], vec![]]
                    })),
                    order_by: None,
                    limit_clause: None,
                    fetch: None,
                    locks: vec![],
                    for_clause: None,
                    settings: None,
                    format_clause: None,
                    pipe_operators: vec![],
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
                        value_keyword: false,
                        explicit_row: false,
                        rows: vec![vec![
                            Expr::Value(
                                (Value::SingleQuotedString("accounting_manager".to_string()))
                                    .with_empty_span()
                            ),
                            Expr::Value(
                                (Value::SingleQuotedString(
                                    "Some description about the group".to_string()
                                ))
                                .with_empty_span()
                            ),
                            Expr::Value((Value::Boolean(true)).with_empty_span()),
                            Expr::Value((Value::Boolean(true)).with_empty_span()),
                            Expr::Value((Value::Boolean(true)).with_empty_span()),
                            Expr::Value((Value::Boolean(true)).with_empty_span()),
                        ]]
                    })),
                    order_by: None,
                    limit_clause: None,
                    fetch: None,
                    locks: vec![],
                    for_clause: None,
                    settings: None,
                    format_clause: None,
                    pipe_operators: vec![],
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
                    optimizer_hint: None,
                    distinct: None,
                    top: None,
                    top_before_distinct: false,
                    projection: vec![SelectItem::UnnamedExpr(Expr::Identifier(Ident::new(
                        "123col_$@123abc"
                    )))],
                    exclude: None,
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
                    flavor: SelectFlavor::Standard,
                })))
            );
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_qualified_identifiers_with_numeric_prefix() {
    // Case 1: Qualified column name that starts with digits.
    match mysql().verified_stmt("SELECT t.15to29 FROM my_table AS t") {
        Statement::Query(q) => match *q.body {
            SetExpr::Select(s) => match s.projection.last() {
                Some(SelectItem::UnnamedExpr(Expr::CompoundIdentifier(parts))) => {
                    assert_eq!(&[Ident::new("t"), Ident::new("15to29")], &parts[..]);
                }
                proj => panic!("Unexpected projection: {proj:?}"),
            },
            body => panic!("Unexpected statement body: {body:?}"),
        },
        stmt => panic!("Unexpected statement: {stmt:?}"),
    }

    // Case 2: Qualified column name that starts with digits and on its own represents a number.
    match mysql().verified_stmt("SELECT t.15e29 FROM my_table AS t") {
        Statement::Query(q) => match *q.body {
            SetExpr::Select(s) => match s.projection.last() {
                Some(SelectItem::UnnamedExpr(Expr::CompoundIdentifier(parts))) => {
                    assert_eq!(&[Ident::new("t"), Ident::new("15e29")], &parts[..]);
                }
                proj => panic!("Unexpected projection: {proj:?}"),
            },
            body => panic!("Unexpected statement body: {body:?}"),
        },
        stmt => panic!("Unexpected statement: {stmt:?}"),
    }

    // Case 3: Unqualified, the same token is parsed as a number.
    match mysql()
        .parse_sql_statements("SELECT 15e29 FROM my_table")
        .unwrap()
        .pop()
    {
        Some(Statement::Query(q)) => match *q.body {
            SetExpr::Select(s) => match s.projection.last() {
                Some(SelectItem::UnnamedExpr(Expr::Value(ValueWithSpan { value, .. }))) => {
                    assert_eq!(&number("15e29"), value);
                }
                proj => panic!("Unexpected projection: {proj:?}"),
            },
            body => panic!("Unexpected statement body: {body:?}"),
        },
        stmt => panic!("Unexpected statement: {stmt:?}"),
    }

    // Case 4: Quoted simple identifier.
    match mysql().verified_stmt("SELECT `15e29` FROM my_table") {
        Statement::Query(q) => match *q.body {
            SetExpr::Select(s) => match s.projection.last() {
                Some(SelectItem::UnnamedExpr(Expr::Identifier(name))) => {
                    assert_eq!(&Ident::with_quote('`', "15e29"), name);
                }
                proj => panic!("Unexpected projection: {proj:?}"),
            },
            body => panic!("Unexpected statement body: {body:?}"),
        },
        stmt => panic!("Unexpected statement: {stmt:?}"),
    }

    // Case 5: Quoted compound identifier.
    match mysql().verified_stmt("SELECT t.`15e29` FROM my_table AS t") {
        Statement::Query(q) => match *q.body {
            SetExpr::Select(s) => match s.projection.last() {
                Some(SelectItem::UnnamedExpr(Expr::CompoundIdentifier(parts))) => {
                    assert_eq!(
                        &[Ident::new("t"), Ident::with_quote('`', "15e29")],
                        &parts[..]
                    );
                }
                proj => panic!("Unexpected projection: {proj:?}"),
            },
            body => panic!("Unexpected statement body: {body:?}"),
        },
        stmt => panic!("Unexpected statement: {stmt:?}"),
    }

    // Case 6: Multi-level compound identifiers.
    match mysql().verified_stmt("SELECT 1db.1table.1column") {
        Statement::Query(q) => match *q.body {
            SetExpr::Select(s) => match s.projection.last() {
                Some(SelectItem::UnnamedExpr(Expr::CompoundIdentifier(parts))) => {
                    assert_eq!(
                        &[
                            Ident::new("1db"),
                            Ident::new("1table"),
                            Ident::new("1column")
                        ],
                        &parts[..]
                    );
                }
                proj => panic!("Unexpected projection: {proj:?}"),
            },
            body => panic!("Unexpected statement body: {body:?}"),
        },
        stmt => panic!("Unexpected statement: {stmt:?}"),
    }

    // Case 7: Multi-level compound quoted identifiers.
    match mysql().verified_stmt("SELECT `1`.`2`.`3`") {
        Statement::Query(q) => match *q.body {
            SetExpr::Select(s) => match s.projection.last() {
                Some(SelectItem::UnnamedExpr(Expr::CompoundIdentifier(parts))) => {
                    assert_eq!(
                        &[
                            Ident::with_quote('`', "1"),
                            Ident::with_quote('`', "2"),
                            Ident::with_quote('`', "3")
                        ],
                        &parts[..]
                    );
                }
                proj => panic!("Unexpected projection: {proj:?}"),
            },
            body => panic!("Unexpected statement body: {body:?}"),
        },
        stmt => panic!("Unexpected statement: {stmt:?}"),
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
                    optimizer_hint: None,
                    distinct: None,
                    top: None,
                    top_before_distinct: false,
                    projection: vec![
                        SelectItem::UnnamedExpr(Expr::value(number("123e4"))),
                        SelectItem::UnnamedExpr(Expr::Identifier(Ident::new("123col_$@123abc")))
                    ],
                    exclude: None,
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
                    flavor: SelectFlavor::Standard,
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
        Statement::Update(Update {
            table,
            assignments,
            from: _from,
            selection,
            returning,
            or: None,
            limit: None,
            optimizer_hint: None,
            update_token: _,
        }) => {
            assert_eq!(
                TableWithJoins {
                    relation: TableFactor::Table {
                        name: ObjectName::from(vec![Ident::new("orders")]),
                        alias: table_alias(true, "o"),
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
                            alias: table_alias(true, "c"),
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
                        join_operator: JoinOperator::Join(JoinConstraint::On(Expr::BinaryOp {
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
                    value: Expr::Value((Value::Boolean(true)).with_empty_span())
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
                    right: Box::new(Expr::Value(
                        (Value::SingleQuotedString("Peter".to_string())).with_empty_span()
                    ))
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
                    options: OrderByOptions {
                        asc: Some(false),
                        nulls_first: None,
                    },
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
            assert_eq!(Some(Expr::value(number("100"))), limit);
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_alter_table_add_column() {
    match mysql().verified_stmt("ALTER TABLE tab ADD COLUMN b INT FIRST") {
        Statement::AlterTable(AlterTable {
            name,
            if_exists,
            only,
            operations,
            table_type,
            location: _,
            on_cluster: _,
            end_token: _,
        }) => {
            assert_eq!(name.to_string(), "tab");
            assert!(!if_exists);
            assert_eq!(table_type, None);
            assert!(!only);
            assert_eq!(
                operations,
                vec![AlterTableOperation::AddColumn {
                    column_keyword: true,
                    if_not_exists: false,
                    column_def: ColumnDef {
                        name: "b".into(),
                        data_type: DataType::Int(None),
                        options: vec![],
                    },
                    column_position: Some(MySQLColumnPosition::First),
                },]
            );
        }
        _ => unreachable!(),
    }

    match mysql().verified_stmt("ALTER TABLE tab ADD COLUMN b INT AFTER foo") {
        Statement::AlterTable(AlterTable {
            name,
            if_exists,
            only,
            operations,
            ..
        }) => {
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
        Statement::AlterTable(AlterTable {
            name,
            if_exists,
            only,
            operations,
            ..
        }) => {
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
        AlterTableOperation::DropPrimaryKey {
            drop_behavior: None
        }
    );
}

#[test]
fn parse_alter_table_drop_foreign_key() {
    assert_matches!(
        alter_table_op(
            mysql_and_generic().verified_stmt("ALTER TABLE tab DROP FOREIGN KEY foo_ibfk_1")
        ),
        AlterTableOperation::DropForeignKey { name, .. } if name.value == "foo_ibfk_1"
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
fn parse_alter_table_with_algorithm() {
    let sql = "ALTER TABLE tab ALGORITHM = COPY";
    let expected_operation = AlterTableOperation::Algorithm {
        equals: true,
        algorithm: AlterTableAlgorithm::Copy,
    };
    let operation = alter_table_op(mysql_and_generic().verified_stmt(sql));
    assert_eq!(expected_operation, operation);

    //  Check order doesn't matter
    let sql =
        "ALTER TABLE users DROP COLUMN password_digest, ALGORITHM = COPY, RENAME COLUMN name TO username";
    let stmt = mysql_and_generic().verified_stmt(sql);
    match stmt {
        Statement::AlterTable(AlterTable { operations, .. }) => {
            assert_eq!(
                operations,
                vec![
                    AlterTableOperation::DropColumn {
                        has_column_keyword: true,
                        column_names: vec![Ident::new("password_digest")],
                        if_exists: false,
                        drop_behavior: None,
                    },
                    AlterTableOperation::Algorithm {
                        equals: true,
                        algorithm: AlterTableAlgorithm::Copy,
                    },
                    AlterTableOperation::RenameColumn {
                        old_column_name: Ident::new("name"),
                        new_column_name: Ident::new("username")
                    },
                ]
            )
        }
        _ => panic!("Unexpected statement {stmt}"),
    }

    mysql_and_generic().verified_stmt("ALTER TABLE `users` ALGORITHM DEFAULT");
    mysql_and_generic().verified_stmt("ALTER TABLE `users` ALGORITHM INSTANT");
    mysql_and_generic().verified_stmt("ALTER TABLE `users` ALGORITHM INPLACE");
    mysql_and_generic().verified_stmt("ALTER TABLE `users` ALGORITHM COPY");
    mysql_and_generic().verified_stmt("ALTER TABLE `users` ALGORITHM = DEFAULT");
    mysql_and_generic().verified_stmt("ALTER TABLE `users` ALGORITHM = INSTANT");
    mysql_and_generic().verified_stmt("ALTER TABLE `users` ALGORITHM = INPLACE");
    mysql_and_generic().verified_stmt("ALTER TABLE `users` ALGORITHM = COPY");
}

#[test]
fn parse_alter_table_with_lock() {
    let sql = "ALTER TABLE tab LOCK = SHARED";
    let expected_operation = AlterTableOperation::Lock {
        equals: true,
        lock: AlterTableLock::Shared,
    };
    let operation = alter_table_op(mysql_and_generic().verified_stmt(sql));
    assert_eq!(expected_operation, operation);

    let sql =
        "ALTER TABLE users DROP COLUMN password_digest, LOCK = EXCLUSIVE, RENAME COLUMN name TO username";
    let stmt = mysql_and_generic().verified_stmt(sql);
    match stmt {
        Statement::AlterTable(AlterTable { operations, .. }) => {
            assert_eq!(
                operations,
                vec![
                    AlterTableOperation::DropColumn {
                        has_column_keyword: true,
                        column_names: vec![Ident::new("password_digest")],
                        if_exists: false,
                        drop_behavior: None,
                    },
                    AlterTableOperation::Lock {
                        equals: true,
                        lock: AlterTableLock::Exclusive,
                    },
                    AlterTableOperation::RenameColumn {
                        old_column_name: Ident::new("name"),
                        new_column_name: Ident::new("username")
                    },
                ]
            )
        }
        _ => panic!("Unexpected statement {stmt}"),
    }
    mysql_and_generic().verified_stmt("ALTER TABLE `users` LOCK DEFAULT");
    mysql_and_generic().verified_stmt("ALTER TABLE `users` LOCK SHARED");
    mysql_and_generic().verified_stmt("ALTER TABLE `users` LOCK NONE");
    mysql_and_generic().verified_stmt("ALTER TABLE `users` LOCK EXCLUSIVE");
    mysql_and_generic().verified_stmt("ALTER TABLE `users` LOCK = DEFAULT");
    mysql_and_generic().verified_stmt("ALTER TABLE `users` LOCK = SHARED");
    mysql_and_generic().verified_stmt("ALTER TABLE `users` LOCK = NONE");
    mysql_and_generic().verified_stmt("ALTER TABLE `users` LOCK = EXCLUSIVE");
}

#[test]
fn parse_alter_table_auto_increment() {
    let sql = "ALTER TABLE tab AUTO_INCREMENT = 42";
    let expected_operation = AlterTableOperation::AutoIncrement {
        equals: true,
        value: number("42").with_empty_span(),
    };
    let operation = alter_table_op(mysql().verified_stmt(sql));
    assert_eq!(expected_operation, operation);

    mysql_and_generic().verified_stmt("ALTER TABLE `users` AUTO_INCREMENT 42");
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
                        optimizer_hint: None,
                        distinct: Some(Distinct::Distinct),
                        top: None,
                        top_before_distinct: false,
                        projection: vec![SelectItem::UnnamedExpr(Expr::Substring {
                            expr: Box::new(Expr::Identifier(Ident {
                                value: "description".to_string(),
                                quote_style: None,
                                span: Span::empty(),
                            })),
                            substring_from: Some(Box::new(Expr::Value(
                                (number("0")).with_empty_span()
                            ))),
                            substring_for: Some(Box::new(Expr::Value(
                                (number("1")).with_empty_span()
                            ))),
                            special: true,
                            shorthand: false,
                        })],
                        exclude: None,
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
                        flavor: SelectFlavor::Standard,
                    }))),
                    order_by: None,
                    limit_clause: None,
                    fetch: None,
                    locks: vec![],
                    for_clause: None,
                    settings: None,
                    format_clause: None,
                    pipe_operators: vec![],
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
fn parse_like_with_escape() {
    // verify backslash is not stripped for escaped wildcards
    mysql().verified_only_select(r#"SELECT 'a\%c' LIKE 'a\%c'"#);
    mysql().verified_only_select(r#"SELECT 'a\_c' LIKE 'a\_c'"#);
    mysql().verified_only_select(r#"SELECT '%\_\%' LIKE '%\_\%'"#);
    mysql().verified_only_select(r#"SELECT '\_\%' LIKE CONCAT('\_', '\%')"#);
    mysql().verified_only_select(r#"SELECT 'a%c' LIKE 'a$%c' ESCAPE '$'"#);
    mysql().verified_only_select(r#"SELECT 'a_c' LIKE 'a#_c' ESCAPE '#'"#);
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
        Statement::Set(Set::SetNames {
            charset_name: "utf8mb4".into(),
            collation_name: None,
        })
    );

    let stmt = mysql_and_generic().verified_stmt("SET NAMES utf8mb4 COLLATE bogus");
    assert_eq!(
        stmt,
        Statement::Set(Set::SetNames {
            charset_name: "utf8mb4".into(),
            collation_name: Some("bogus".to_string()),
        })
    );

    let stmt = mysql_and_generic()
        .parse_sql_statements("set names utf8mb4 collate bogus")
        .unwrap();
    assert_eq!(
        stmt,
        vec![Statement::Set(Set::SetNames {
            charset_name: "utf8mb4".into(),
            collation_name: Some("bogus".to_string()),
        })]
    );

    let stmt = mysql_and_generic().verified_stmt("SET NAMES DEFAULT");
    assert_eq!(stmt, Statement::Set(Set::SetNamesDefault {}));
}

#[test]
fn parse_limit_my_sql_syntax() {
    mysql_and_generic().verified_stmt("SELECT id, fname, lname FROM customer LIMIT 10 OFFSET 5");
    mysql_and_generic().verified_stmt("SELECT id, fname, lname FROM customer LIMIT 5, 10");
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
                optimizer_hint: None,
                distinct: None,
                top: None,
                top_before_distinct: false,
                projection: vec![SelectItem::UnnamedExpr(Expr::Prefixed {
                    prefix: Ident::from("_latin1"),
                    value: Expr::Value(
                        Value::HexStringLiteral("4D7953514C".to_string()).with_empty_span()
                    )
                    .into(),
                })],
                exclude: None,
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
                flavor: SelectFlavor::Standard,
            }))),
            order_by: None,
            limit_clause: None,
            fetch: None,
            locks: vec![],
            for_clause: None,
            settings: None,
            format_clause: None,
            pipe_operators: vec![],
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
    match mysql().verified_stmt(sql) {
        Statement::CreateTable(CreateTable { name, columns, .. }) => {
            assert_eq!(name.to_string(), "tb");
            assert_eq!(
                vec![ColumnDef {
                    name: Ident::new("id"),
                    data_type: DataType::Text,
                    options: vec![
                        ColumnOptionDef {
                            name: None,
                            option: ColumnOption::CharacterSet(ObjectName::from(vec![Ident::new(
                                "utf8mb4"
                            )]))
                        },
                        ColumnOptionDef {
                            name: None,
                            option: ColumnOption::Collation(ObjectName::from(vec![Ident::new(
                                "utf8mb4_0900_ai_ci"
                            )]))
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
            json_expr: Expr::Value((Value::SingleQuotedString("[1,2]".to_string())).with_empty_span()),
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
            alias: table_alias(true, "t"),
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
            left: Box::new(Expr::Value((Value::Boolean(true)).with_empty_span())),
            op: BinaryOperator::Xor,
            right: Box::new(Expr::Value((Value::Boolean(true)).with_empty_span())),
        }),
        select.projection[0]
    );
    assert_eq!(
        SelectItem::UnnamedExpr(Expr::BinaryOp {
            left: Box::new(Expr::Value((Value::Boolean(false)).with_empty_span())),
            op: BinaryOperator::Xor,
            right: Box::new(Expr::Value((Value::Boolean(false)).with_empty_span())),
        }),
        select.projection[1]
    );
    assert_eq!(
        SelectItem::UnnamedExpr(Expr::BinaryOp {
            left: Box::new(Expr::Value((Value::Boolean(true)).with_empty_span())),
            op: BinaryOperator::Xor,
            right: Box::new(Expr::Value((Value::Boolean(false)).with_empty_span())),
        }),
        select.projection[2]
    );
    assert_eq!(
        SelectItem::UnnamedExpr(Expr::BinaryOp {
            left: Box::new(Expr::Value((Value::Boolean(false)).with_empty_span())),
            op: BinaryOperator::Xor,
            right: Box::new(Expr::Value((Value::Boolean(true)).with_empty_span())),
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
            (Value::SingleQuotedByteStringLiteral("111".to_string())).with_empty_span()
        ))]
    );
}

#[test]
fn parse_grant() {
    let sql = "GRANT ALL ON *.* TO 'jeffrey'@'%'";
    let stmt = mysql().verified_stmt(sql);
    if let Statement::Grant(Grant {
        privileges,
        objects,
        grantees,
        with_grant_option,
        as_grantor: _,
        granted_by,
        current_grants: _,
    }) = stmt
    {
        assert_eq!(
            privileges,
            Privileges::All {
                with_privileges_keyword: false
            }
        );
        assert_eq!(
            objects,
            Some(GrantObjects::Tables(vec![ObjectName::from(vec![
                "*".into(),
                "*".into()
            ])]))
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
    if let Statement::Revoke(Revoke {
        privileges,
        objects,
        grantees,
        granted_by,
        cascade,
    }) = stmt
    {
        assert_eq!(
            privileges,
            Privileges::All {
                with_privileges_keyword: false
            }
        );
        assert_eq!(
            objects,
            Some(GrantObjects::Tables(vec![ObjectName::from(vec![
                "db1".into(),
                "*".into()
            ])]))
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
    if let Statement::CreateView(CreateView {
        params:
            Some(CreateViewParams {
                algorithm,
                definer,
                security,
            }),
        ..
    }) = stmt
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
    if let Statement::CreateView(CreateView {
        params:
            Some(CreateViewParams {
                algorithm,
                definer,
                security,
            }),
        ..
    }) = stmt
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
    if let Statement::CreateView(CreateView {
        params:
            Some(CreateViewParams {
                algorithm,
                definer,
                security,
            }),
        ..
    }) = stmt
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
    if let Statement::CreateView(CreateView {
        params:
            Some(CreateViewParams {
                algorithm,
                definer,
                security,
            }),
        ..
    }) = stmt
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
fn parse_geometric_types_srid_option() {
    mysql_and_generic().verified_stmt("CREATE TABLE t (a geometry SRID 4326)");
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

#[test]
fn parse_looks_like_single_line_comment() {
    mysql().one_statement_parses_to(
        "UPDATE account SET balance=balance--1 WHERE account_id=5752",
        "UPDATE account SET balance = balance - -1 WHERE account_id = 5752",
    );
    mysql().one_statement_parses_to(
        r#"
            UPDATE account SET balance=balance-- 1
            WHERE account_id=5752
        "#,
        "UPDATE account SET balance = balance WHERE account_id = 5752",
    );
}

#[test]
fn parse_create_trigger() {
    let sql_create_trigger = r#"CREATE TRIGGER emp_stamp BEFORE INSERT ON emp FOR EACH ROW EXECUTE FUNCTION emp_stamp()"#;
    let create_stmt = mysql().verified_stmt(sql_create_trigger);
    assert_eq!(
        create_stmt,
        Statement::CreateTrigger(CreateTrigger {
            or_alter: false,
            temporary: false,
            or_replace: false,
            is_constraint: false,
            name: ObjectName::from(vec![Ident::new("emp_stamp")]),
            period: Some(TriggerPeriod::Before),
            period_before_table: true,
            events: vec![TriggerEvent::Insert],
            table_name: ObjectName::from(vec![Ident::new("emp")]),
            referenced_table_name: None,
            referencing: vec![],
            trigger_object: Some(TriggerObjectKind::ForEach(TriggerObject::Row)),
            condition: None,
            exec_body: Some(TriggerExecBody {
                exec_type: TriggerExecBodyType::Function,
                func_desc: FunctionDesc {
                    name: ObjectName::from(vec![Ident::new("emp_stamp")]),
                    args: Some(vec![]),
                }
            }),
            statements_as: false,
            statements: None,
            characteristics: None,
        })
    );
}

#[test]
fn parse_create_trigger_compound_statement() {
    mysql_and_generic().verified_stmt("CREATE TRIGGER mytrigger BEFORE INSERT ON mytable FOR EACH ROW BEGIN SET NEW.a = 1; SET NEW.b = 2; END");
    mysql_and_generic().verified_stmt("CREATE TRIGGER tr AFTER INSERT ON t1 FOR EACH ROW BEGIN INSERT INTO t2 VALUES (NEW.id); END");
}

#[test]
fn parse_drop_trigger() {
    let sql_drop_trigger = "DROP TRIGGER emp_stamp;";
    let drop_stmt = mysql().one_statement_parses_to(sql_drop_trigger, "");
    assert_eq!(
        drop_stmt,
        Statement::DropTrigger(DropTrigger {
            if_exists: false,
            trigger_name: ObjectName::from(vec![Ident::new("emp_stamp")]),
            table_name: None,
            option: None,
        })
    );
}

#[test]
fn parse_cast_integers() {
    mysql().verified_expr("CAST(foo AS UNSIGNED)");
    mysql().verified_expr("CAST(foo AS SIGNED)");
    mysql().verified_expr("CAST(foo AS UNSIGNED INTEGER)");
    mysql().verified_expr("CAST(foo AS SIGNED INTEGER)");

    mysql()
        .run_parser_method("CAST(foo AS UNSIGNED(3))", |p| p.parse_expr())
        .expect_err("CAST doesn't allow display width");
    mysql()
        .run_parser_method("CAST(foo AS UNSIGNED(3) INTEGER)", |p| p.parse_expr())
        .expect_err("CAST doesn't allow display width");
    mysql()
        .run_parser_method("CAST(foo AS UNSIGNED INTEGER(3))", |p| p.parse_expr())
        .expect_err("CAST doesn't allow display width");
}

#[test]
fn parse_cast_array() {
    mysql().verified_expr("CAST(foo AS SIGNED ARRAY)");
    mysql()
        .run_parser_method("CAST(foo AS ARRAY)", |p| p.parse_expr())
        .expect_err("ARRAY alone is not a type");
}

#[test]
fn parse_match_against_with_alias() {
    let sql = "SELECT tbl.ProjectID FROM surveys.tbl1 AS tbl WHERE MATCH (tbl.ReferenceID) AGAINST ('AAA' IN BOOLEAN MODE)";
    match mysql().verified_stmt(sql) {
        Statement::Query(query) => match *query.body {
            SetExpr::Select(select) => match select.selection {
                Some(Expr::MatchAgainst {
                    columns,
                    match_value,
                    opt_search_modifier,
                }) => {
                    assert_eq!(
                        columns,
                        vec![ObjectName::from(vec![
                            Ident::new("tbl"),
                            Ident::new("ReferenceID")
                        ])]
                    );
                    assert_eq!(match_value, Value::SingleQuotedString("AAA".to_owned()));
                    assert_eq!(opt_search_modifier, Some(SearchModifier::InBooleanMode));
                }
                _ => unreachable!(),
            },
            _ => unreachable!(),
        },
        _ => unreachable!(),
    }
}

#[test]
fn test_variable_assignment_using_colon_equal() {
    let sql_select = "SELECT @price := price, @tax := price * 0.1 FROM products WHERE id = 1";
    let stmt = mysql().verified_stmt(sql_select);
    match stmt {
        Statement::Query(query) => {
            let select = query.body.as_select().unwrap();

            assert_eq!(
                select.projection,
                vec![
                    SelectItem::UnnamedExpr(Expr::BinaryOp {
                        left: Box::new(Expr::Identifier(Ident {
                            value: "@price".to_string(),
                            quote_style: None,
                            span: Span::empty(),
                        })),
                        op: BinaryOperator::Assignment,
                        right: Box::new(Expr::Identifier(Ident {
                            value: "price".to_string(),
                            quote_style: None,
                            span: Span::empty(),
                        })),
                    }),
                    SelectItem::UnnamedExpr(Expr::BinaryOp {
                        left: Box::new(Expr::Identifier(Ident {
                            value: "@tax".to_string(),
                            quote_style: None,
                            span: Span::empty(),
                        })),
                        op: BinaryOperator::Assignment,
                        right: Box::new(Expr::BinaryOp {
                            left: Box::new(Expr::Identifier(Ident {
                                value: "price".to_string(),
                                quote_style: None,
                                span: Span::empty(),
                            })),
                            op: BinaryOperator::Multiply,
                            right: Box::new(Expr::Value(
                                (test_utils::number("0.1")).with_empty_span()
                            )),
                        }),
                    }),
                ]
            );

            assert_eq!(
                select.selection,
                Some(Expr::BinaryOp {
                    left: Box::new(Expr::Identifier(Ident {
                        value: "id".to_string(),
                        quote_style: None,
                        span: Span::empty(),
                    })),
                    op: BinaryOperator::Eq,
                    right: Box::new(Expr::Value((test_utils::number("1")).with_empty_span())),
                })
            );
        }
        _ => panic!("Unexpected statement {stmt}"),
    }

    let sql_update =
        "UPDATE products SET price = @new_price := price * 1.1 WHERE category = 'Books'";
    let stmt = mysql().verified_stmt(sql_update);

    match stmt {
        Statement::Update(Update { assignments, .. }) => {
            assert_eq!(
                assignments,
                vec![Assignment {
                    target: AssignmentTarget::ColumnName(ObjectName(vec![
                        ObjectNamePart::Identifier(Ident {
                            value: "price".to_string(),
                            quote_style: None,
                            span: Span::empty(),
                        })
                    ])),
                    value: Expr::BinaryOp {
                        left: Box::new(Expr::Identifier(Ident {
                            value: "@new_price".to_string(),
                            quote_style: None,
                            span: Span::empty(),
                        })),
                        op: BinaryOperator::Assignment,
                        right: Box::new(Expr::BinaryOp {
                            left: Box::new(Expr::Identifier(Ident {
                                value: "price".to_string(),
                                quote_style: None,
                                span: Span::empty(),
                            })),
                            op: BinaryOperator::Multiply,
                            right: Box::new(Expr::Value(
                                (test_utils::number("1.1")).with_empty_span()
                            )),
                        }),
                    },
                }]
            )
        }
        _ => panic!("Unexpected statement {stmt}"),
    }
}

#[test]
fn parse_straight_join() {
    mysql().verified_stmt(
        "SELECT a.*, b.* FROM table_a AS a STRAIGHT_JOIN table_b AS b ON a.b_id = b.id",
    );
    // Without table alias
    mysql()
        .verified_stmt("SELECT a.*, b.* FROM table_a STRAIGHT_JOIN table_b AS b ON a.b_id = b.id");
}

#[test]
fn mysql_foreign_key_with_index_name() {
    mysql().verified_stmt(
        "CREATE TABLE orders (customer_id INT, INDEX idx_customer (customer_id), CONSTRAINT fk_customer FOREIGN KEY idx_customer (customer_id) REFERENCES customers(id))",
    );
}

#[test]
fn parse_drop_index() {
    let sql = "DROP INDEX idx_name ON table_name";
    match mysql().verified_stmt(sql) {
        Statement::Drop {
            object_type,
            if_exists,
            names,
            cascade,
            restrict,
            purge,
            temporary,
            table,
        } => {
            assert!(!if_exists);
            assert_eq!(ObjectType::Index, object_type);
            assert_eq!(
                vec!["idx_name"],
                names.iter().map(ToString::to_string).collect::<Vec<_>>()
            );
            assert!(!cascade);
            assert!(!restrict);
            assert!(!purge);
            assert!(!temporary);
            assert!(table.is_some());
            assert_eq!("table_name", table.unwrap().to_string());
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_alter_table_drop_index() {
    assert_matches!(
        alter_table_op(
            mysql_and_generic().verified_stmt("ALTER TABLE tab DROP INDEX idx_index")
        ),
        AlterTableOperation::DropIndex { name } if name.value == "idx_index"
    );
}

#[test]
fn parse_json_member_of() {
    mysql().verified_stmt(r#"SELECT 17 MEMBER OF('[23, "abc", 17, "ab", 10]')"#);
    let sql = r#"SELECT 'ab' MEMBER OF('[23, "abc", 17, "ab", 10]')"#;
    let stmt = mysql().verified_stmt(sql);
    match stmt {
        Statement::Query(query) => {
            let select = query.body.as_select().unwrap();
            assert_eq!(
                select.projection,
                vec![SelectItem::UnnamedExpr(Expr::MemberOf(MemberOf {
                    value: Box::new(Expr::Value(
                        Value::SingleQuotedString("ab".to_string()).into()
                    )),
                    array: Box::new(Expr::Value(
                        Value::SingleQuotedString(r#"[23, "abc", 17, "ab", 10]"#.to_string())
                            .into()
                    )),
                }))]
            );
        }
        _ => panic!("Unexpected statement {stmt}"),
    }
}

#[test]
fn parse_show_charset() {
    let res = mysql().verified_stmt("SHOW CHARACTER SET");
    assert_eq!(
        res,
        Statement::ShowCharset(ShowCharset {
            is_shorthand: false,
            filter: None
        })
    );
    mysql().verified_stmt("SHOW CHARACTER SET LIKE 'utf8mb4%'");
    mysql().verified_stmt("SHOW CHARSET WHERE charset = 'utf8mb4%'");
    mysql().verified_stmt("SHOW CHARSET LIKE 'utf8mb4%'");
}

#[test]
fn test_ddl_with_index_using() {
    let columns = "(name, age DESC)";
    let using = "USING BTREE";

    for sql in [
        format!("CREATE INDEX idx_name ON test {using} {columns}"),
        format!("CREATE TABLE foo (name VARCHAR(255), age INT, KEY idx_name {using} {columns})"),
        format!("ALTER TABLE foo ADD KEY idx_name {using} {columns}"),
        format!("CREATE INDEX idx_name ON test{columns} {using}"),
        format!("CREATE TABLE foo (name VARCHAR(255), age INT, KEY idx_name {columns} {using})"),
        format!("ALTER TABLE foo ADD KEY idx_name {columns} {using}"),
    ] {
        mysql_and_generic().verified_stmt(&sql);
    }
}

#[test]
fn test_create_index_options() {
    mysql_and_generic()
        .verified_stmt("CREATE INDEX idx_name ON t(c1, c2) USING HASH LOCK = SHARED");
    mysql_and_generic()
        .verified_stmt("CREATE INDEX idx_name ON t(c1, c2) USING BTREE ALGORITHM = INPLACE");
    mysql_and_generic().verified_stmt(
        "CREATE INDEX idx_name ON t(c1, c2) USING BTREE LOCK = EXCLUSIVE ALGORITHM = DEFAULT",
    );
}

#[test]
fn test_optimizer_hints() {
    let mysql_dialect = mysql_and_generic();

    // ~ selects
    mysql_dialect.verified_stmt(
        "\
       SELECT /*+ SET_VAR(optimizer_switch = 'mrr_cost_based=off') \
                  SET_VAR(max_heap_table_size = 1G) */ 1",
    );

    mysql_dialect.verified_stmt(
        "\
       SELECT /*+ SET_VAR(target_partitions=1) */ * FROM \
           (SELECT /*+ SET_VAR(target_partitions=8) */ * FROM t1 LIMIT 1) AS dt",
    );

    // ~ inserts / replace
    mysql_dialect.verified_stmt(
        "\
       INSERT /*+ RESOURCE_GROUP(Batch) */ \
       INTO t2 VALUES (2)",
    );

    mysql_dialect.verified_stmt(
        "\
       REPLACE /*+ foobar */ INTO test \
       VALUES (1, 'Old', '2014-08-20 18:47:00')",
    );

    // ~ updates
    mysql_dialect.verified_stmt(
        "\
       UPDATE /*+ quux */ table_name \
       SET column1 = 1 \
       WHERE 1 = 1",
    );

    // ~ deletes
    mysql_dialect.verified_stmt(
        "\
       DELETE /*+ foobar */ FROM table_name",
    );
}

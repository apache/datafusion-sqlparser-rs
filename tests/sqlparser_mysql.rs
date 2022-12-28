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

use sqlparser::ast::Expr;
use sqlparser::ast::Value;
use sqlparser::ast::*;
use sqlparser::dialect::{GenericDialect, MySqlDialect};
use sqlparser::tokenizer::Token;
use test_utils::*;

#[macro_use]
mod test_utils;

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
    mysql_and_generic().one_statement_parses_to(
        "SHOW COLUMNS FROM mytable FROM mydb",
        "SHOW COLUMNS FROM mydb.mytable",
    );
}

#[test]
fn parse_show_tables() {
    assert_eq!(
        mysql_and_generic().verified_stmt("SHOW TABLES"),
        Statement::ShowTables {
            extended: false,
            full: false,
            db_name: None,
            filter: None,
        }
    );
    assert_eq!(
        mysql_and_generic().verified_stmt("SHOW TABLES FROM mydb"),
        Statement::ShowTables {
            extended: false,
            full: false,
            db_name: Some(Ident::new("mydb")),
            filter: None,
        }
    );
    assert_eq!(
        mysql_and_generic().verified_stmt("SHOW EXTENDED TABLES"),
        Statement::ShowTables {
            extended: true,
            full: false,
            db_name: None,
            filter: None,
        }
    );
    assert_eq!(
        mysql_and_generic().verified_stmt("SHOW FULL TABLES"),
        Statement::ShowTables {
            extended: false,
            full: true,
            db_name: None,
            filter: None,
        }
    );
    assert_eq!(
        mysql_and_generic().verified_stmt("SHOW TABLES LIKE 'pattern'"),
        Statement::ShowTables {
            extended: false,
            full: false,
            db_name: None,
            filter: Some(ShowStatementFilter::Like("pattern".into())),
        }
    );
    assert_eq!(
        mysql_and_generic().verified_stmt("SHOW TABLES WHERE 1 = 2"),
        Statement::ShowTables {
            extended: false,
            full: false,
            db_name: None,
            filter: Some(ShowStatementFilter::Where(
                mysql_and_generic().verified_expr("1 = 2")
            )),
        }
    );
    mysql_and_generic().one_statement_parses_to("SHOW TABLES IN mydb", "SHOW TABLES FROM mydb");
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
    let obj_name = ObjectName(vec![Ident::new("myident")]);

    for obj_type in &vec![
        ShowCreateObject::Table,
        ShowCreateObject::Trigger,
        ShowCreateObject::Event,
        ShowCreateObject::Function,
        ShowCreateObject::Procedure,
        ShowCreateObject::View,
    ] {
        assert_eq!(
            mysql_and_generic().verified_stmt(format!("SHOW CREATE {} myident", obj_type).as_str()),
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
    assert_eq!(
        mysql_and_generic().verified_stmt("USE mydb"),
        Statement::Use {
            db_name: Ident::new("mydb")
        }
    );
}

#[test]
fn parse_set_variables() {
    mysql_and_generic().verified_stmt("SET sql_mode = CONCAT(@@sql_mode, ',STRICT_TRANS_TABLES')");
    assert_eq!(
        mysql_and_generic().verified_stmt("SET LOCAL autocommit = 1"),
        Statement::SetVariable {
            local: true,
            hivevar: false,
            variable: ObjectName(vec!["autocommit".into()]),
            value: vec![Expr::Value(Value::Number(
                #[cfg(not(feature = "bigdecimal"))]
                "1".to_string(),
                #[cfg(feature = "bigdecimal")]
                bigdecimal::BigDecimal::from(1),
                false
            ))],
        }
    );
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
fn parse_create_table_set_enum() {
    let sql = "CREATE TABLE foo (bar SET('a', 'b'), baz ENUM('a', 'b'))";
    match mysql().verified_stmt(sql) {
        Statement::CreateTable { name, columns, .. } => {
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
                        data_type: DataType::Enum(vec!["a".to_string(), "b".to_string()]),
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
        Statement::CreateTable {
            name,
            columns,
            engine,
            default_charset,
            ..
        } => {
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
            assert_eq!(engine, Some("InnoDB".to_string()));
            assert_eq!(default_charset, Some("utf8mb3".to_string()));
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_table_collate() {
    let sql = "CREATE TABLE foo (id INT(11)) COLLATE=utf8mb4_0900_ai_ci";
    match mysql().verified_stmt(sql) {
        Statement::CreateTable {
            name,
            columns,
            collation,
            ..
        } => {
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
fn parse_create_table_comment_character_set() {
    let sql = "CREATE TABLE foo (s TEXT CHARACTER SET utf8mb4 COMMENT 'comment')";
    match mysql().verified_stmt(sql) {
        Statement::CreateTable { name, columns, .. } => {
            assert_eq!(name.to_string(), "foo");
            assert_eq!(
                vec![ColumnDef {
                    name: Ident::new("s"),
                    data_type: DataType::Text,
                    collation: None,
                    options: vec![
                        ColumnOptionDef {
                            name: None,
                            option: ColumnOption::CharacterSet(ObjectName(vec![Ident::new(
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
fn parse_quote_identifiers_2() {
    let sql = "SELECT `quoted `` identifier`";
    assert_eq!(
        mysql().verified_stmt(sql),
        Statement::Query(Box::new(Query {
            with: None,
            body: Box::new(SetExpr::Select(Box::new(Select {
                distinct: false,
                top: None,
                projection: vec![SelectItem::UnnamedExpr(Expr::Identifier(Ident {
                    value: "quoted ` identifier".into(),
                    quote_style: Some('`'),
                }))],
                into: None,
                from: vec![],
                lateral_views: vec![],
                selection: None,
                group_by: vec![],
                cluster_by: vec![],
                distribute_by: vec![],
                sort_by: vec![],
                having: None,
                qualify: None
            }))),
            order_by: vec![],
            limit: None,
            offset: None,
            fetch: None,
            locks: vec![],
        }))
    );
}

#[test]
fn parse_quote_identifiers_3() {
    let sql = "SELECT ```quoted identifier```";
    assert_eq!(
        mysql().verified_stmt(sql),
        Statement::Query(Box::new(Query {
            with: None,
            body: Box::new(SetExpr::Select(Box::new(Select {
                distinct: false,
                top: None,
                projection: vec![SelectItem::UnnamedExpr(Expr::Identifier(Ident {
                    value: "`quoted identifier`".into(),
                    quote_style: Some('`'),
                }))],
                into: None,
                from: vec![],
                lateral_views: vec![],
                selection: None,
                group_by: vec![],
                cluster_by: vec![],
                distribute_by: vec![],
                sort_by: vec![],
                having: None,
                qualify: None
            }))),
            order_by: vec![],
            limit: None,
            offset: None,
            fetch: None,
            locks: vec![],
        }))
    );
}

#[test]
fn parse_unterminated_escape() {
    let sql = r#"SELECT 'I\'m not fine\'"#;
    let result = std::panic::catch_unwind(|| mysql().one_statement_parses_to(sql, ""));
    assert!(result.is_err());

    let sql = r#"SELECT 'I\\'m not fine'"#;
    let result = std::panic::catch_unwind(|| mysql().one_statement_parses_to(sql, ""));
    assert!(result.is_err());
}

#[test]
fn parse_escaped_string() {
    let sql = r#"SELECT 'I\'m fine'"#;

    let stmt = mysql().one_statement_parses_to(sql, "");

    match stmt {
        Statement::Query(query) => match *query.body {
            SetExpr::Select(value) => {
                let expr = expr_from_projection(only(&value.projection));
                assert_eq!(
                    *expr,
                    Expr::Value(Value::SingleQuotedString("I'm fine".to_string()))
                );
            }
            _ => unreachable!(),
        },
        _ => unreachable!(),
    };

    let sql = r#"SELECT 'I''m fine'"#;

    let projection = mysql().verified_only_select(sql).projection;
    let item = projection.get(0).unwrap();

    match &item {
        SelectItem::UnnamedExpr(Expr::Value(value)) => {
            assert_eq!(*value, Value::SingleQuotedString("I'm fine".to_string()));
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_table_with_minimum_display_width() {
    let sql = "CREATE TABLE foo (bar_tinyint TINYINT(3), bar_smallint SMALLINT(5), bar_mediumint MEDIUMINT(6), bar_int INT(11), bar_bigint BIGINT(20))";
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
        Statement::CreateTable { name, columns, .. } => {
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
#[cfg(not(feature = "bigdecimal"))]
fn parse_simple_insert() {
    let sql = r"INSERT INTO tasks (title, priority) VALUES ('Test Some Inserts', 1), ('Test Entry 2', 2), ('Test Entry 3', 3)";

    match mysql().verified_stmt(sql) {
        Statement::Insert {
            table_name,
            columns,
            source,
            on,
            ..
        } => {
            assert_eq!(ObjectName(vec![Ident::new("tasks")]), table_name);
            assert_eq!(vec![Ident::new("title"), Ident::new("priority")], columns);
            assert!(on.is_none());
            assert_eq!(
                Box::new(Query {
                    with: None,
                    body: Box::new(SetExpr::Values(Values {
                        explicit_row: false,
                        rows: vec![
                            vec![
                                Expr::Value(Value::SingleQuotedString(
                                    "Test Some Inserts".to_string()
                                )),
                                Expr::Value(Value::Number("1".to_string(), false))
                            ],
                            vec![
                                Expr::Value(Value::SingleQuotedString("Test Entry 2".to_string())),
                                Expr::Value(Value::Number("2".to_string(), false))
                            ],
                            vec![
                                Expr::Value(Value::SingleQuotedString("Test Entry 3".to_string())),
                                Expr::Value(Value::Number("3".to_string(), false))
                            ]
                        ]
                    })),
                    order_by: vec![],
                    limit: None,
                    offset: None,
                    fetch: None,
                    locks: vec![],
                }),
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
        Statement::Insert {
            table_name,
            columns,
            source,
            on,
            ..
        } => {
            assert_eq!(
                ObjectName(vec![Ident::new("permission_groups")]),
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
                Box::new(Query {
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
                    order_by: vec![],
                    limit: None,
                    offset: None,
                    fetch: None,
                    locks: vec![],
                }),
                source
            );
            assert_eq!(
                Some(OnInsert::DuplicateKeyUpdate(vec![
                    Assignment {
                        id: vec![Ident::new("description".to_string())],
                        value: Expr::Function(Function {
                            name: ObjectName(vec![Ident::new("VALUES".to_string()),]),
                            args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(
                                Expr::Identifier(Ident::new("description"))
                            ))],
                            over: None,
                            distinct: false,
                            special: false,
                        })
                    },
                    Assignment {
                        id: vec![Ident::new("perm_create".to_string())],
                        value: Expr::Function(Function {
                            name: ObjectName(vec![Ident::new("VALUES".to_string()),]),
                            args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(
                                Expr::Identifier(Ident::new("perm_create"))
                            ))],
                            over: None,
                            distinct: false,
                            special: false,
                        })
                    },
                    Assignment {
                        id: vec![Ident::new("perm_read".to_string())],
                        value: Expr::Function(Function {
                            name: ObjectName(vec![Ident::new("VALUES".to_string()),]),
                            args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(
                                Expr::Identifier(Ident::new("perm_read"))
                            ))],
                            over: None,
                            distinct: false,
                            special: false,
                        })
                    },
                    Assignment {
                        id: vec![Ident::new("perm_update".to_string())],
                        value: Expr::Function(Function {
                            name: ObjectName(vec![Ident::new("VALUES".to_string()),]),
                            args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(
                                Expr::Identifier(Ident::new("perm_update"))
                            ))],
                            over: None,
                            distinct: false,
                            special: false,
                        })
                    },
                    Assignment {
                        id: vec![Ident::new("perm_delete".to_string())],
                        value: Expr::Function(Function {
                            name: ObjectName(vec![Ident::new("VALUES".to_string()),]),
                            args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(
                                Expr::Identifier(Ident::new("perm_delete"))
                            ))],
                            over: None,
                            distinct: false,
                            special: false,
                        })
                    },
                ])),
                on
            );
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
        } => {
            assert_eq!(
                TableWithJoins {
                    relation: TableFactor::Table {
                        name: ObjectName(vec![Ident::new("orders")]),
                        alias: Some(TableAlias {
                            name: Ident::new("o"),
                            columns: vec![]
                        }),
                        args: None,
                        with_hints: vec![],
                    },
                    joins: vec![Join {
                        relation: TableFactor::Table {
                            name: ObjectName(vec![Ident::new("customers")]),
                            alias: Some(TableAlias {
                                name: Ident::new("c"),
                                columns: vec![]
                            }),
                            args: None,
                            with_hints: vec![],
                        },
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
                    id: vec![Ident::new("o"), Ident::new("completed")],
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
fn parse_alter_table_drop_primary_key() {
    match mysql_and_generic().verified_stmt("ALTER TABLE tab DROP PRIMARY KEY") {
        Statement::AlterTable {
            name,
            operation: AlterTableOperation::DropPrimaryKey,
        } => {
            assert_eq!("tab", name.to_string());
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_alter_table_change_column() {
    let expected_name = ObjectName(vec![Ident::new("orders")]);
    let expected_operation = AlterTableOperation::ChangeColumn {
        old_name: Ident::new("description"),
        new_name: Ident::new("desc"),
        data_type: DataType::Text,
        options: vec![ColumnOption::NotNull],
    };

    let sql1 = "ALTER TABLE orders CHANGE COLUMN description desc TEXT NOT NULL";
    match mysql().verified_stmt(sql1) {
        Statement::AlterTable { name, operation } => {
            assert_eq!(expected_name, name);
            assert_eq!(expected_operation, operation);
        }
        _ => unreachable!(),
    }

    let sql2 = "ALTER TABLE orders CHANGE description desc TEXT NOT NULL";
    match mysql().one_statement_parses_to(sql2, sql1) {
        Statement::AlterTable { name, operation } => {
            assert_eq!(expected_name, name);
            assert_eq!(expected_operation, operation);
        }
        _ => unreachable!(),
    }
}

#[test]
#[cfg(not(feature = "bigdecimal"))]
fn parse_substring_in_select() {
    let sql = "SELECT DISTINCT SUBSTRING(description, 0, 1) FROM test";
    match mysql().one_statement_parses_to(
        sql,
        "SELECT DISTINCT SUBSTRING(description FROM 0 FOR 1) FROM test",
    ) {
        Statement::Query(query) => {
            assert_eq!(
                Box::new(Query {
                    with: None,
                    body: Box::new(SetExpr::Select(Box::new(Select {
                        distinct: true,
                        top: None,
                        projection: vec![SelectItem::UnnamedExpr(Expr::Substring {
                            expr: Box::new(Expr::Identifier(Ident {
                                value: "description".to_string(),
                                quote_style: None
                            })),
                            substring_from: Some(Box::new(Expr::Value(Value::Number(
                                "0".to_string(),
                                false
                            )))),
                            substring_for: Some(Box::new(Expr::Value(Value::Number(
                                "1".to_string(),
                                false
                            ))))
                        })],
                        into: None,
                        from: vec![TableWithJoins {
                            relation: TableFactor::Table {
                                name: ObjectName(vec![Ident {
                                    value: "test".to_string(),
                                    quote_style: None
                                }]),
                                alias: None,
                                args: None,
                                with_hints: vec![]
                            },
                            joins: vec![]
                        }],
                        lateral_views: vec![],
                        selection: None,
                        group_by: vec![],
                        cluster_by: vec![],
                        distribute_by: vec![],
                        sort_by: vec![],
                        having: None,
                        qualify: None
                    }))),
                    order_by: vec![],
                    limit: None,
                    offset: None,
                    fetch: None,
                    locks: vec![],
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
fn parse_table_colum_option_on_update() {
    let sql1 = "CREATE TABLE foo (`modification_time` DATETIME ON UPDATE CURRENT_TIMESTAMP())";
    match mysql().verified_stmt(sql1) {
        Statement::CreateTable { name, columns, .. } => {
            assert_eq!(name.to_string(), "foo");
            assert_eq!(
                vec![ColumnDef {
                    name: Ident::with_quote('`', "modification_time"),
                    data_type: DataType::Datetime(None),
                    collation: None,
                    options: vec![ColumnOptionDef {
                        name: None,
                        option: ColumnOption::OnUpdate(Expr::Function(Function {
                            name: ObjectName(vec![Ident::new("CURRENT_TIMESTAMP")]),
                            args: vec![],
                            over: None,
                            distinct: false,
                            special: false,
                        })),
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
#[should_panic = "Expected FULLTEXT or SPATIAL option without constraint name, found: cons"]
fn parse_create_table_with_fulltext_definition_should_not_accept_constraint_name() {
    mysql_and_generic().verified_stmt("CREATE TABLE tb (c1 INT, CONSTRAINT cons FULLTEXT (c1))");
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

#[test]
fn parse_values() {
    mysql().verified_stmt("VALUES ROW(1, true, 'a')");
    mysql().verified_stmt("SELECT a, c FROM (VALUES ROW(1, true, 'a'), ROW(2, false, 'b'), ROW(3, false, 'c')) AS t (a, b, c)");
}

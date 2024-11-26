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

#[macro_use]
mod test_utils;

use helpers::attached_token::AttachedToken;
use sqlparser::tokenizer::Span;
use test_utils::*;

use sqlparser::ast::*;
use sqlparser::dialect::{DuckDbDialect, GenericDialect};

fn duckdb() -> TestedDialects {
    TestedDialects::new(vec![Box::new(DuckDbDialect {})])
}

fn duckdb_and_generic() -> TestedDialects {
    TestedDialects::new(vec![
        Box::new(DuckDbDialect {}),
        Box::new(GenericDialect {}),
    ])
}

#[test]
fn test_struct() {
    // s STRUCT(v VARCHAR, i INTEGER)
    let struct_type1 = DataType::Struct(
        vec![
            StructField {
                field_name: Some(Ident::new("v")),
                field_type: DataType::Varchar(None),
            },
            StructField {
                field_name: Some(Ident::new("i")),
                field_type: DataType::Integer(None),
            },
        ],
        StructBracketKind::Parentheses,
    );

    // basic struct
    let statement = duckdb().verified_stmt(r#"CREATE TABLE t1 (s STRUCT(v VARCHAR, i INTEGER))"#);
    assert_eq!(
        column_defs(statement),
        vec![ColumnDef {
            name: "s".into(),
            data_type: struct_type1.clone(),
            collation: None,
            options: vec![],
        }]
    );

    // struct array
    let statement = duckdb().verified_stmt(r#"CREATE TABLE t1 (s STRUCT(v VARCHAR, i INTEGER)[])"#);
    assert_eq!(
        column_defs(statement),
        vec![ColumnDef {
            name: "s".into(),
            data_type: DataType::Array(ArrayElemTypeDef::SquareBracket(
                Box::new(struct_type1),
                None
            )),
            collation: None,
            options: vec![],
        }]
    );

    // s STRUCT(v VARCHAR, s STRUCT(a1 INTEGER, a2 VARCHAR))
    let struct_type2 = DataType::Struct(
        vec![
            StructField {
                field_name: Some(Ident::new("v")),
                field_type: DataType::Varchar(None),
            },
            StructField {
                field_name: Some(Ident::new("s")),
                field_type: DataType::Struct(
                    vec![
                        StructField {
                            field_name: Some(Ident::new("a1")),
                            field_type: DataType::Integer(None),
                        },
                        StructField {
                            field_name: Some(Ident::new("a2")),
                            field_type: DataType::Varchar(None),
                        },
                    ],
                    StructBracketKind::Parentheses,
                ),
            },
        ],
        StructBracketKind::Parentheses,
    );

    // nested struct
    let statement = duckdb().verified_stmt(
        r#"CREATE TABLE t1 (s STRUCT(v VARCHAR, s STRUCT(a1 INTEGER, a2 VARCHAR))[])"#,
    );

    assert_eq!(
        column_defs(statement),
        vec![ColumnDef {
            name: "s".into(),
            data_type: DataType::Array(ArrayElemTypeDef::SquareBracket(
                Box::new(struct_type2),
                None
            )),
            collation: None,
            options: vec![],
        }]
    );

    // failing test (duckdb does not support bracket syntax)
    let sql_list = vec![
        r#"CREATE TABLE t1 (s STRUCT(v VARCHAR, i INTEGER)))"#,
        r#"CREATE TABLE t1 (s STRUCT(v VARCHAR, i INTEGER>)"#,
        r#"CREATE TABLE t1 (s STRUCT<v VARCHAR, i INTEGER>)"#,
        r#"CREATE TABLE t1 (s STRUCT v VARCHAR, i INTEGER )"#,
        r#"CREATE TABLE t1 (s STRUCT VARCHAR, i INTEGER )"#,
        r#"CREATE TABLE t1 (s STRUCT (VARCHAR, INTEGER))"#,
    ];

    for sql in sql_list {
        duckdb().parse_sql_statements(sql).unwrap_err();
    }
}

/// Returns the ColumnDefinitions from a CreateTable statement
fn column_defs(statement: Statement) -> Vec<ColumnDef> {
    match statement {
        Statement::CreateTable(CreateTable { columns, .. }) => columns,
        _ => panic!("Expected CreateTable"),
    }
}

#[test]
fn test_select_wildcard_with_exclude() {
    let select = duckdb().verified_only_select("SELECT * EXCLUDE (col_a) FROM data");
    let expected = SelectItem::Wildcard(WildcardAdditionalOptions {
        opt_exclude: Some(ExcludeSelectItem::Multiple(vec![Ident::new("col_a")])),
        ..Default::default()
    });
    assert_eq!(expected, select.projection[0]);

    let select =
        duckdb().verified_only_select("SELECT name.* EXCLUDE department_id FROM employee_table");
    let expected = SelectItem::QualifiedWildcard(
        ObjectName(vec![Ident::new("name")]),
        WildcardAdditionalOptions {
            opt_exclude: Some(ExcludeSelectItem::Single(Ident::new("department_id"))),
            ..Default::default()
        },
    );
    assert_eq!(expected, select.projection[0]);

    let select = duckdb()
        .verified_only_select("SELECT * EXCLUDE (department_id, employee_id) FROM employee_table");
    let expected = SelectItem::Wildcard(WildcardAdditionalOptions {
        opt_exclude: Some(ExcludeSelectItem::Multiple(vec![
            Ident::new("department_id"),
            Ident::new("employee_id"),
        ])),
        ..Default::default()
    });
    assert_eq!(expected, select.projection[0]);
}

#[test]
fn parse_div_infix() {
    duckdb_and_generic().verified_stmt(r#"SELECT 5 // 2"#);
}

#[test]
fn test_create_macro() {
    let macro_ = duckdb().verified_stmt("CREATE MACRO schema.add(a, b) AS a + b");
    let expected = Statement::CreateMacro {
        or_replace: false,
        temporary: false,
        name: ObjectName(vec![Ident::new("schema"), Ident::new("add")]),
        args: Some(vec![MacroArg::new("a"), MacroArg::new("b")]),
        definition: MacroDefinition::Expr(Expr::BinaryOp {
            left: Box::new(Expr::Identifier(Ident::new("a"))),
            op: BinaryOperator::Plus,
            right: Box::new(Expr::Identifier(Ident::new("b"))),
        }),
    };
    assert_eq!(expected, macro_);
}

#[test]
fn test_create_macro_default_args() {
    let macro_ = duckdb().verified_stmt("CREATE MACRO add_default(a, b := 5) AS a + b");
    let expected = Statement::CreateMacro {
        or_replace: false,
        temporary: false,
        name: ObjectName(vec![Ident::new("add_default")]),
        args: Some(vec![
            MacroArg::new("a"),
            MacroArg {
                name: Ident::new("b"),
                default_expr: Some(Expr::Value(number("5"))),
            },
        ]),
        definition: MacroDefinition::Expr(Expr::BinaryOp {
            left: Box::new(Expr::Identifier(Ident::new("a"))),
            op: BinaryOperator::Plus,
            right: Box::new(Expr::Identifier(Ident::new("b"))),
        }),
    };
    assert_eq!(expected, macro_);
}

#[test]
fn test_create_table_macro() {
    let query = "SELECT col1_value AS column1, col2_value AS column2 UNION ALL SELECT 'Hello' AS col1_value, 456 AS col2_value";
    let macro_ = duckdb().verified_stmt(
        &("CREATE OR REPLACE TEMPORARY MACRO dynamic_table(col1_value, col2_value) AS TABLE "
            .to_string()
            + query),
    );
    let expected = Statement::CreateMacro {
        or_replace: true,
        temporary: true,
        name: ObjectName(vec![Ident::new("dynamic_table")]),
        args: Some(vec![
            MacroArg::new("col1_value"),
            MacroArg::new("col2_value"),
        ]),
        definition: MacroDefinition::Table(duckdb().verified_query(query).into()),
    };
    assert_eq!(expected, macro_);
}

#[test]
fn test_select_union_by_name() {
    let q1 = "SELECT * FROM capitals UNION BY NAME SELECT * FROM weather";
    let q2 = "SELECT * FROM capitals UNION ALL BY NAME SELECT * FROM weather";
    let q3 = "SELECT * FROM capitals UNION DISTINCT BY NAME SELECT * FROM weather";

    for (ast, expected_quantifier) in &[
        (duckdb().verified_query(q1), SetQuantifier::ByName),
        (duckdb().verified_query(q2), SetQuantifier::AllByName),
        (duckdb().verified_query(q3), SetQuantifier::DistinctByName),
    ] {
        let expected = Box::<SetExpr>::new(SetExpr::SetOperation {
            op: SetOperator::Union,
            set_quantifier: *expected_quantifier,
            left: Box::<SetExpr>::new(SetExpr::Select(Box::new(Select {
                select_token: AttachedToken::empty(),
                distinct: None,
                top: None,
                projection: vec![SelectItem::Wildcard(WildcardAdditionalOptions::default())],
                top_before_distinct: false,
                into: None,
                from: vec![TableWithJoins {
                    relation: TableFactor::Table {
                        name: ObjectName(vec![Ident {
                            value: "capitals".to_string(),
                            quote_style: None,
                            span: Span::empty(),
                        }]),
                        alias: None,
                        args: None,
                        with_hints: vec![],
                        version: None,
                        partitions: vec![],
                        with_ordinality: false,
                        json_path: None,
                    },
                    joins: vec![],
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
            right: Box::<SetExpr>::new(SetExpr::Select(Box::new(Select {
                select_token: AttachedToken::empty(),
                distinct: None,
                top: None,
                projection: vec![SelectItem::Wildcard(WildcardAdditionalOptions::default())],
                top_before_distinct: false,
                into: None,
                from: vec![TableWithJoins {
                    relation: TableFactor::Table {
                        name: ObjectName(vec![Ident {
                            value: "weather".to_string(),
                            quote_style: None,
                            span: Span::empty(),
                        }]),
                        alias: None,
                        args: None,
                        with_hints: vec![],
                        version: None,
                        partitions: vec![],
                        with_ordinality: false,
                        json_path: None,
                    },
                    joins: vec![],
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
        });
        assert_eq!(ast.body, expected);
    }
}

#[test]
fn test_duckdb_install() {
    let stmt = duckdb().verified_stmt("INSTALL tpch");
    assert_eq!(
        stmt,
        Statement::Install {
            extension_name: Ident {
                value: "tpch".to_string(),
                quote_style: None,
                span: Span::empty()
            }
        }
    );
}

#[test]
fn test_duckdb_load_extension() {
    let stmt = duckdb().verified_stmt("LOAD my_extension");
    assert_eq!(
        Statement::Load {
            extension_name: Ident {
                value: "my_extension".to_string(),
                quote_style: None,
                span: Span::empty()
            }
        },
        stmt
    );
}

#[test]
fn test_duckdb_struct_literal() {
    //struct literal syntax https://duckdb.org/docs/sql/data_types/struct#creating-structs
    //syntax: {'field_name': expr1[, ... ]}
    let sql = "SELECT {'a': 1, 'b': 2, 'c': 3}, [{'a': 'abc'}], {'a': 1, 'b': [t.str_col]}, {'a': 1, 'b': 'abc'}, {'abc': str_col}, {'a': {'aa': 1}}";
    let select = duckdb_and_generic().verified_only_select(sql);
    assert_eq!(6, select.projection.len());
    assert_eq!(
        &Expr::Dictionary(vec![
            DictionaryField {
                key: Ident::with_quote('\'', "a"),
                value: Box::new(Expr::Value(number("1"))),
            },
            DictionaryField {
                key: Ident::with_quote('\'', "b"),
                value: Box::new(Expr::Value(number("2"))),
            },
            DictionaryField {
                key: Ident::with_quote('\'', "c"),
                value: Box::new(Expr::Value(number("3"))),
            },
        ],),
        expr_from_projection(&select.projection[0])
    );

    assert_eq!(
        &Expr::Array(Array {
            elem: vec![Expr::Dictionary(vec![DictionaryField {
                key: Ident::with_quote('\'', "a"),
                value: Box::new(Expr::Value(Value::SingleQuotedString("abc".to_string()))),
            },],)],
            named: false
        }),
        expr_from_projection(&select.projection[1])
    );
    assert_eq!(
        &Expr::Dictionary(vec![
            DictionaryField {
                key: Ident::with_quote('\'', "a"),
                value: Box::new(Expr::Value(number("1"))),
            },
            DictionaryField {
                key: Ident::with_quote('\'', "b"),
                value: Box::new(Expr::Array(Array {
                    elem: vec![Expr::CompoundIdentifier(vec![
                        Ident::from("t"),
                        Ident::from("str_col")
                    ])],
                    named: false
                })),
            },
        ],),
        expr_from_projection(&select.projection[2])
    );
    assert_eq!(
        &Expr::Dictionary(vec![
            DictionaryField {
                key: Ident::with_quote('\'', "a"),
                value: Expr::Value(number("1")).into(),
            },
            DictionaryField {
                key: Ident::with_quote('\'', "b"),
                value: Expr::Value(Value::SingleQuotedString("abc".to_string())).into(),
            },
        ],),
        expr_from_projection(&select.projection[3])
    );
    assert_eq!(
        &Expr::Dictionary(vec![DictionaryField {
            key: Ident::with_quote('\'', "abc"),
            value: Expr::Identifier(Ident::from("str_col")).into(),
        }],),
        expr_from_projection(&select.projection[4])
    );
    assert_eq!(
        &Expr::Dictionary(vec![DictionaryField {
            key: Ident::with_quote('\'', "a"),
            value: Expr::Dictionary(vec![DictionaryField {
                key: Ident::with_quote('\'', "aa"),
                value: Expr::Value(number("1")).into(),
            }],)
            .into(),
        }],),
        expr_from_projection(&select.projection[5])
    );
}

#[test]
fn test_create_secret() {
    let sql = r#"CREATE OR REPLACE PERSISTENT SECRET IF NOT EXISTS name IN storage ( TYPE type, key1 value1, key2 value2 )"#;
    let stmt = duckdb().verified_stmt(sql);
    assert_eq!(
        Statement::CreateSecret {
            or_replace: true,
            temporary: Some(false),
            if_not_exists: true,
            name: Some(Ident::new("name")),
            storage_specifier: Some(Ident::new("storage")),
            secret_type: Ident::new("type"),
            options: vec![
                SecretOption {
                    key: Ident::new("key1"),
                    value: Ident::new("value1"),
                },
                SecretOption {
                    key: Ident::new("key2"),
                    value: Ident::new("value2"),
                }
            ]
        },
        stmt
    );
}

#[test]
fn test_create_secret_simple() {
    let sql = r#"CREATE SECRET ( TYPE type )"#;
    let stmt = duckdb().verified_stmt(sql);
    assert_eq!(
        Statement::CreateSecret {
            or_replace: false,
            temporary: None,
            if_not_exists: false,
            name: None,
            storage_specifier: None,
            secret_type: Ident::new("type"),
            options: vec![]
        },
        stmt
    );
}

#[test]
fn test_drop_secret() {
    let sql = r#"DROP PERSISTENT SECRET IF EXISTS secret FROM storage"#;
    let stmt = duckdb().verified_stmt(sql);
    assert_eq!(
        Statement::DropSecret {
            if_exists: true,
            temporary: Some(false),
            name: Ident::new("secret"),
            storage_specifier: Some(Ident::new("storage"))
        },
        stmt
    );
}

#[test]
fn test_drop_secret_simple() {
    let sql = r#"DROP SECRET secret"#;
    let stmt = duckdb().verified_stmt(sql);
    assert_eq!(
        Statement::DropSecret {
            if_exists: false,
            temporary: None,
            name: Ident::new("secret"),
            storage_specifier: None
        },
        stmt
    );
}

#[test]
fn test_attach_database() {
    let sql = r#"ATTACH DATABASE IF NOT EXISTS 'sqlite_file.db' AS sqlite_db (READ_ONLY false, TYPE SQLITE)"#;
    let stmt = duckdb().verified_stmt(sql);
    assert_eq!(
        Statement::AttachDuckDBDatabase {
            if_not_exists: true,
            database: true,
            database_path: Ident::with_quote('\'', "sqlite_file.db"),
            database_alias: Some(Ident::new("sqlite_db")),
            attach_options: vec![
                AttachDuckDBDatabaseOption::ReadOnly(Some(false)),
                AttachDuckDBDatabaseOption::Type(Ident::new("SQLITE")),
            ]
        },
        stmt
    );
}

#[test]
fn test_attach_database_simple() {
    let sql = r#"ATTACH 'postgres://user.name:pass-word@some.url.com:5432/postgres'"#;
    let stmt = duckdb().verified_stmt(sql);
    assert_eq!(
        Statement::AttachDuckDBDatabase {
            if_not_exists: false,
            database: false,
            database_path: Ident::with_quote(
                '\'',
                "postgres://user.name:pass-word@some.url.com:5432/postgres"
            ),
            database_alias: None,
            attach_options: vec![]
        },
        stmt
    );
}

#[test]
fn test_detach_database() {
    let sql = r#"DETACH DATABASE IF EXISTS db_name"#;
    let stmt = duckdb().verified_stmt(sql);
    assert_eq!(
        Statement::DetachDuckDBDatabase {
            if_exists: true,
            database: true,
            database_alias: Ident::new("db_name"),
        },
        stmt
    );
}

#[test]
fn test_detach_database_simple() {
    let sql = r#"DETACH db_name"#;
    let stmt = duckdb().verified_stmt(sql);
    assert_eq!(
        Statement::DetachDuckDBDatabase {
            if_exists: false,
            database: false,
            database_alias: Ident::new("db_name"),
        },
        stmt
    );
}

#[test]
fn test_duckdb_named_argument_function_with_assignment_operator() {
    let sql = "SELECT FUN(a := '1', b := '2') FROM foo";
    let select = duckdb_and_generic().verified_only_select(sql);
    assert_eq!(
        &Expr::Function(Function {
            name: ObjectName(vec![Ident::new("FUN")]),
            parameters: FunctionArguments::None,
            args: FunctionArguments::List(FunctionArgumentList {
                duplicate_treatment: None,
                args: vec![
                    FunctionArg::Named {
                        name: Ident::new("a"),
                        arg: FunctionArgExpr::Expr(Expr::Value(Value::SingleQuotedString(
                            "1".to_owned()
                        ))),
                        operator: FunctionArgOperator::Assignment
                    },
                    FunctionArg::Named {
                        name: Ident::new("b"),
                        arg: FunctionArgExpr::Expr(Expr::Value(Value::SingleQuotedString(
                            "2".to_owned()
                        ))),
                        operator: FunctionArgOperator::Assignment
                    },
                ],
                clauses: vec![],
            }),
            null_treatment: None,
            filter: None,
            over: None,
            within_group: vec![],
        }),
        expr_from_projection(only(&select.projection))
    );
}

#[test]
fn test_array_index() {
    let sql = r#"SELECT ['a', 'b', 'c'][3] AS three"#;
    let select = duckdb().verified_only_select(sql);
    let projection = &select.projection;
    assert_eq!(1, projection.len());
    let expr = match &projection[0] {
        SelectItem::ExprWithAlias { expr, .. } => expr,
        _ => panic!("Expected an expression with alias"),
    };
    assert_eq!(
        &Expr::Subscript {
            expr: Box::new(Expr::Array(Array {
                elem: vec![
                    Expr::Value(Value::SingleQuotedString("a".to_owned())),
                    Expr::Value(Value::SingleQuotedString("b".to_owned())),
                    Expr::Value(Value::SingleQuotedString("c".to_owned()))
                ],
                named: false
            })),
            subscript: Box::new(Subscript::Index {
                index: Expr::Value(number("3"))
            })
        },
        expr
    );
}

#[test]
fn test_duckdb_union_datatype() {
    let sql = "CREATE TABLE tbl1 (one UNION(a INT), two UNION(a INT, b INT), nested UNION(a UNION(b INT)))";
    let stmt = duckdb_and_generic().verified_stmt(sql);
    assert_eq!(
        Statement::CreateTable(CreateTable {
            or_replace: Default::default(),
            temporary: Default::default(),
            external: Default::default(),
            global: Default::default(),
            if_not_exists: Default::default(),
            transient: Default::default(),
            volatile: Default::default(),
            name: ObjectName(vec!["tbl1".into()]),
            columns: vec![
                ColumnDef {
                    name: "one".into(),
                    data_type: DataType::Union(vec![UnionField {
                        field_name: "a".into(),
                        field_type: DataType::Int(None)
                    }]),
                    collation: Default::default(),
                    options: Default::default()
                },
                ColumnDef {
                    name: "two".into(),
                    data_type: DataType::Union(vec![
                        UnionField {
                            field_name: "a".into(),
                            field_type: DataType::Int(None)
                        },
                        UnionField {
                            field_name: "b".into(),
                            field_type: DataType::Int(None)
                        }
                    ]),
                    collation: Default::default(),
                    options: Default::default()
                },
                ColumnDef {
                    name: "nested".into(),
                    data_type: DataType::Union(vec![UnionField {
                        field_name: "a".into(),
                        field_type: DataType::Union(vec![UnionField {
                            field_name: "b".into(),
                            field_type: DataType::Int(None)
                        }])
                    }]),
                    collation: Default::default(),
                    options: Default::default()
                }
            ],
            constraints: Default::default(),
            hive_distribution: HiveDistributionStyle::NONE,
            hive_formats: Some(HiveFormat {
                row_format: Default::default(),
                serde_properties: Default::default(),
                storage: Default::default(),
                location: Default::default()
            }),
            table_properties: Default::default(),
            with_options: Default::default(),
            file_format: Default::default(),
            location: Default::default(),
            query: Default::default(),
            without_rowid: Default::default(),
            like: Default::default(),
            clone: Default::default(),
            engine: Default::default(),
            comment: Default::default(),
            auto_increment_offset: Default::default(),
            default_charset: Default::default(),
            collation: Default::default(),
            on_commit: Default::default(),
            on_cluster: Default::default(),
            primary_key: Default::default(),
            order_by: Default::default(),
            partition_by: Default::default(),
            cluster_by: Default::default(),
            clustered_by: Default::default(),
            options: Default::default(),
            strict: Default::default(),
            copy_grants: Default::default(),
            enable_schema_evolution: Default::default(),
            change_tracking: Default::default(),
            data_retention_time_in_days: Default::default(),
            max_data_extension_time_in_days: Default::default(),
            default_ddl_collation: Default::default(),
            with_aggregation_policy: Default::default(),
            with_row_access_policy: Default::default(),
            with_tags: Default::default()
        }),
        stmt
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
    let quote_styles = ['"', '\''];

    for object_name in &valid_object_names {
        // Test single identifier without quotes
        assert_eq!(
            duckdb().verified_stmt(&format!("USE {}", object_name)),
            Statement::Use(Use::Object(ObjectName(vec![Ident::new(
                object_name.to_string()
            )])))
        );
        for &quote in &quote_styles {
            // Test single identifier with different type of quotes
            assert_eq!(
                duckdb().verified_stmt(&format!("USE {0}{1}{0}", quote, object_name)),
                Statement::Use(Use::Object(ObjectName(vec![Ident::with_quote(
                    quote,
                    object_name.to_string(),
                )])))
            );
        }
    }

    for &quote in &quote_styles {
        // Test double identifier with different type of quotes
        assert_eq!(
            duckdb().verified_stmt(&format!("USE {0}CATALOG{0}.{0}my_schema{0}", quote)),
            Statement::Use(Use::Object(ObjectName(vec![
                Ident::with_quote(quote, "CATALOG"),
                Ident::with_quote(quote, "my_schema")
            ])))
        );
    }
    // Test double identifier without quotes
    assert_eq!(
        duckdb().verified_stmt("USE mydb.my_schema"),
        Statement::Use(Use::Object(ObjectName(vec![
            Ident::new("mydb"),
            Ident::new("my_schema")
        ])))
    );
}

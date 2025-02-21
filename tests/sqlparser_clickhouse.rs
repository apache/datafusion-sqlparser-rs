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
//! Test SQL syntax specific to ClickHouse.

#[macro_use]
mod test_utils;

use helpers::attached_token::AttachedToken;
use sqlparser::tokenizer::Span;
use test_utils::*;

use sqlparser::ast::Expr::{BinaryOp, Identifier};
use sqlparser::ast::SelectItem::UnnamedExpr;
use sqlparser::ast::TableFactor::Table;
use sqlparser::ast::Value::Number;
use sqlparser::ast::*;
use sqlparser::dialect::ClickHouseDialect;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::ParserError::ParserError;

#[test]
fn parse_map_access_expr() {
    let sql = r#"SELECT string_values[indexOf(string_names, 'endpoint')] FROM foos WHERE id = 'test' AND string_value[indexOf(string_name, 'app')] <> 'foo'"#;
    let select = clickhouse().verified_only_select(sql);
    assert_eq!(
        Select {
            distinct: None,
            select_token: AttachedToken::empty(),
            top: None,
            top_before_distinct: false,
            projection: vec![UnnamedExpr(Expr::CompoundFieldAccess {
                root: Box::new(Identifier(Ident {
                    value: "string_values".to_string(),
                    quote_style: None,
                    span: Span::empty(),
                })),
                access_chain: vec![AccessExpr::Subscript(Subscript::Index {
                    index: call(
                        "indexOf",
                        [
                            Expr::Identifier(Ident::new("string_names")),
                            Expr::Value(Value::SingleQuotedString("endpoint".to_string()))
                        ]
                    ),
                })],
            })],
            into: None,
            from: vec![TableWithJoins {
                relation: table_from_name(ObjectName::from(vec![Ident::new("foos")])),
                joins: vec![],
            }],
            lateral_views: vec![],
            prewhere: None,
            selection: Some(BinaryOp {
                left: Box::new(BinaryOp {
                    left: Box::new(Identifier(Ident::new("id"))),
                    op: BinaryOperator::Eq,
                    right: Box::new(Expr::Value(Value::SingleQuotedString("test".to_string()))),
                }),
                op: BinaryOperator::And,
                right: Box::new(BinaryOp {
                    left: Box::new(Expr::CompoundFieldAccess {
                        root: Box::new(Identifier(Ident::new("string_value"))),
                        access_chain: vec![AccessExpr::Subscript(Subscript::Index {
                            index: call(
                                "indexOf",
                                [
                                    Expr::Identifier(Ident::new("string_name")),
                                    Expr::Value(Value::SingleQuotedString("app".to_string()))
                                ]
                            ),
                        })],
                    }),
                    op: BinaryOperator::NotEq,
                    right: Box::new(Expr::Value(Value::SingleQuotedString("foo".to_string()))),
                }),
            }),
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
        },
        select
    );
}

#[test]
fn parse_array_expr() {
    let sql = "SELECT ['1', '2'] FROM test";
    let select = clickhouse().verified_only_select(sql);
    assert_eq!(
        &Expr::Array(Array {
            elem: vec![
                Expr::Value(Value::SingleQuotedString("1".to_string())),
                Expr::Value(Value::SingleQuotedString("2".to_string())),
            ],
            named: false,
        }),
        expr_from_projection(only(&select.projection))
    )
}

#[test]
fn parse_array_fn() {
    let sql = "SELECT array(x1, x2) FROM foo";
    let select = clickhouse().verified_only_select(sql);
    assert_eq!(
        &call(
            "array",
            [
                Expr::Identifier(Ident::new("x1")),
                Expr::Identifier(Ident::new("x2"))
            ]
        ),
        expr_from_projection(only(&select.projection))
    );
}

#[test]
fn parse_kill() {
    let stmt = clickhouse().verified_stmt("KILL MUTATION 5");
    assert_eq!(
        stmt,
        Statement::Kill {
            modifier: Some(KillType::Mutation),
            id: 5,
        }
    );
}

#[test]
fn parse_delimited_identifiers() {
    // check that quoted identifiers in any position remain quoted after serialization
    let select = clickhouse().verified_only_select(
        r#"SELECT "alias"."bar baz", "myfun"(), "simple id" AS "column alias" FROM "a table" AS "alias""#,
    );
    // check FROM
    match only(select.from).relation {
        TableFactor::Table {
            name,
            alias,
            args,
            with_hints,
            version,
            ..
        } => {
            assert_eq!(
                ObjectName::from(vec![Ident::with_quote('"', "a table")]),
                name
            );
            assert_eq!(Ident::with_quote('"', "alias"), alias.unwrap().name);
            assert!(args.is_none());
            assert!(with_hints.is_empty());
            assert!(version.is_none());
        }
        _ => panic!("Expecting TableFactor::Table"),
    }
    // check SELECT
    assert_eq!(3, select.projection.len());
    assert_eq!(
        &Expr::CompoundIdentifier(vec![
            Ident::with_quote('"', "alias"),
            Ident::with_quote('"', "bar baz"),
        ]),
        expr_from_projection(&select.projection[0]),
    );
    assert_eq!(
        &Expr::Function(Function {
            name: ObjectName::from(vec![Ident::with_quote('"', "myfun")]),
            uses_odbc_syntax: false,
            parameters: FunctionArguments::None,
            args: FunctionArguments::List(FunctionArgumentList {
                duplicate_treatment: None,
                args: vec![],
                clauses: vec![],
            }),
            null_treatment: None,
            filter: None,
            over: None,
            within_group: vec![],
        }),
        expr_from_projection(&select.projection[1]),
    );
    match &select.projection[2] {
        SelectItem::ExprWithAlias { expr, alias } => {
            assert_eq!(&Expr::Identifier(Ident::with_quote('"', "simple id")), expr);
            assert_eq!(&Ident::with_quote('"', "column alias"), alias);
        }
        _ => panic!("Expected ExprWithAlias"),
    }

    clickhouse().verified_stmt(r#"CREATE TABLE "foo" ("bar" "int")"#);
    clickhouse().verified_stmt(r#"ALTER TABLE foo ADD CONSTRAINT "bar" PRIMARY KEY (baz)"#);
    //TODO verified_stmt(r#"UPDATE foo SET "bar" = 5"#);
}

#[test]
fn parse_create_table() {
    clickhouse().verified_stmt(r#"CREATE TABLE "x" ("a" "int") ENGINE=MergeTree ORDER BY ("x")"#);
    clickhouse().verified_stmt(r#"CREATE TABLE "x" ("a" "int") ENGINE=MergeTree ORDER BY "x""#);
    clickhouse().verified_stmt(
        r#"CREATE TABLE "x" ("a" "int") ENGINE=MergeTree ORDER BY "x" AS SELECT * FROM "t" WHERE true"#,
    );
}

#[test]
fn parse_insert_into_function() {
    clickhouse().verified_stmt(r#"INSERT INTO TABLE FUNCTION remote('localhost', default.simple_table) VALUES (100, 'inserted via remote()')"#);
    clickhouse().verified_stmt(r#"INSERT INTO FUNCTION remote('localhost', default.simple_table) VALUES (100, 'inserted via remote()')"#);
}

#[test]
fn parse_alter_table_attach_and_detach_partition() {
    for operation in &["ATTACH", "DETACH"] {
        match clickhouse_and_generic()
            .verified_stmt(format!("ALTER TABLE t0 {operation} PARTITION part").as_str())
        {
            Statement::AlterTable {
                name, operations, ..
            } => {
                pretty_assertions::assert_eq!("t0", name.to_string());
                pretty_assertions::assert_eq!(
                    operations[0],
                    if operation == &"ATTACH" {
                        AlterTableOperation::AttachPartition {
                            partition: Partition::Expr(Identifier(Ident::new("part"))),
                        }
                    } else {
                        AlterTableOperation::DetachPartition {
                            partition: Partition::Expr(Identifier(Ident::new("part"))),
                        }
                    }
                );
            }
            _ => unreachable!(),
        }

        match clickhouse_and_generic()
            .verified_stmt(format!("ALTER TABLE t1 {operation} PART part").as_str())
        {
            Statement::AlterTable {
                name, operations, ..
            } => {
                pretty_assertions::assert_eq!("t1", name.to_string());
                pretty_assertions::assert_eq!(
                    operations[0],
                    if operation == &"ATTACH" {
                        AlterTableOperation::AttachPartition {
                            partition: Partition::Part(Identifier(Ident::new("part"))),
                        }
                    } else {
                        AlterTableOperation::DetachPartition {
                            partition: Partition::Part(Identifier(Ident::new("part"))),
                        }
                    }
                );
            }
            _ => unreachable!(),
        }

        // negative cases
        assert_eq!(
            clickhouse_and_generic()
                .parse_sql_statements(format!("ALTER TABLE t0 {operation} PARTITION").as_str())
                .unwrap_err(),
            ParserError("Expected: an expression, found: EOF".to_string())
        );
        assert_eq!(
            clickhouse_and_generic()
                .parse_sql_statements(format!("ALTER TABLE t0 {operation} PART").as_str())
                .unwrap_err(),
            ParserError("Expected: an expression, found: EOF".to_string())
        );
    }
}

#[test]
fn parse_alter_table_add_projection() {
    match clickhouse_and_generic().verified_stmt(concat!(
        "ALTER TABLE t0 ADD PROJECTION IF NOT EXISTS my_name",
        " (SELECT a, b GROUP BY a ORDER BY b)",
    )) {
        Statement::AlterTable {
            name, operations, ..
        } => {
            assert_eq!(name, ObjectName::from(vec!["t0".into()]));
            assert_eq!(1, operations.len());
            assert_eq!(
                operations[0],
                AlterTableOperation::AddProjection {
                    if_not_exists: true,
                    name: "my_name".into(),
                    select: ProjectionSelect {
                        projection: vec![
                            UnnamedExpr(Identifier(Ident::new("a"))),
                            UnnamedExpr(Identifier(Ident::new("b"))),
                        ],
                        group_by: Some(GroupByExpr::Expressions(
                            vec![Identifier(Ident::new("a"))],
                            vec![]
                        )),
                        order_by: Some(OrderBy {
                            exprs: vec![OrderByExpr {
                                expr: Identifier(Ident::new("b")),
                                asc: None,
                                nulls_first: None,
                                with_fill: None,
                            }],
                            interpolate: None,
                        }),
                    }
                }
            )
        }
        _ => unreachable!(),
    }

    // leave out IF NOT EXISTS is allowed
    clickhouse_and_generic()
        .verified_stmt("ALTER TABLE t0 ADD PROJECTION my_name (SELECT a, b GROUP BY a ORDER BY b)");
    // leave out GROUP BY is allowed
    clickhouse_and_generic()
        .verified_stmt("ALTER TABLE t0 ADD PROJECTION my_name (SELECT a, b ORDER BY b)");
    // leave out ORDER BY is allowed
    clickhouse_and_generic()
        .verified_stmt("ALTER TABLE t0 ADD PROJECTION my_name (SELECT a, b GROUP BY a)");

    // missing select query is not allowed
    assert_eq!(
        clickhouse_and_generic()
            .parse_sql_statements("ALTER TABLE t0 ADD PROJECTION my_name")
            .unwrap_err(),
        ParserError("Expected: (, found: EOF".to_string())
    );
    assert_eq!(
        clickhouse_and_generic()
            .parse_sql_statements("ALTER TABLE t0 ADD PROJECTION my_name ()")
            .unwrap_err(),
        ParserError("Expected: SELECT, found: )".to_string())
    );
    assert_eq!(
        clickhouse_and_generic()
            .parse_sql_statements("ALTER TABLE t0 ADD PROJECTION my_name (SELECT)")
            .unwrap_err(),
        ParserError("Expected: an expression, found: )".to_string())
    );
}

#[test]
fn parse_alter_table_drop_projection() {
    match clickhouse_and_generic().verified_stmt("ALTER TABLE t0 DROP PROJECTION IF EXISTS my_name")
    {
        Statement::AlterTable {
            name, operations, ..
        } => {
            assert_eq!(name, ObjectName::from(vec!["t0".into()]));
            assert_eq!(1, operations.len());
            assert_eq!(
                operations[0],
                AlterTableOperation::DropProjection {
                    if_exists: true,
                    name: "my_name".into(),
                }
            )
        }
        _ => unreachable!(),
    }
    // allow to skip `IF EXISTS`
    clickhouse_and_generic().verified_stmt("ALTER TABLE t0 DROP PROJECTION my_name");

    assert_eq!(
        clickhouse_and_generic()
            .parse_sql_statements("ALTER TABLE t0 DROP PROJECTION")
            .unwrap_err(),
        ParserError("Expected: identifier, found: EOF".to_string())
    );
}

#[test]
fn parse_alter_table_clear_and_materialize_projection() {
    for keyword in ["CLEAR", "MATERIALIZE"] {
        match clickhouse_and_generic().verified_stmt(
            format!("ALTER TABLE t0 {keyword} PROJECTION IF EXISTS my_name IN PARTITION p0",)
                .as_str(),
        ) {
            Statement::AlterTable {
                name, operations, ..
            } => {
                assert_eq!(name, ObjectName::from(vec!["t0".into()]));
                assert_eq!(1, operations.len());
                assert_eq!(
                    operations[0],
                    if keyword == "CLEAR" {
                        AlterTableOperation::ClearProjection {
                            if_exists: true,
                            name: "my_name".into(),
                            partition: Some(Ident::new("p0")),
                        }
                    } else {
                        AlterTableOperation::MaterializeProjection {
                            if_exists: true,
                            name: "my_name".into(),
                            partition: Some(Ident::new("p0")),
                        }
                    }
                )
            }
            _ => unreachable!(),
        }
        // allow to skip `IF EXISTS`
        clickhouse_and_generic().verified_stmt(
            format!("ALTER TABLE t0 {keyword} PROJECTION my_name IN PARTITION p0",).as_str(),
        );
        // allow to skip `IN PARTITION partition_name`
        clickhouse_and_generic()
            .verified_stmt(format!("ALTER TABLE t0 {keyword} PROJECTION my_name",).as_str());

        assert_eq!(
            clickhouse_and_generic()
                .parse_sql_statements(format!("ALTER TABLE t0 {keyword} PROJECTION",).as_str())
                .unwrap_err(),
            ParserError("Expected: identifier, found: EOF".to_string())
        );

        assert_eq!(
            clickhouse_and_generic()
                .parse_sql_statements(
                    format!("ALTER TABLE t0 {keyword} PROJECTION my_name IN PARTITION",).as_str()
                )
                .unwrap_err(),
            ParserError("Expected: identifier, found: EOF".to_string())
        );

        assert_eq!(
            clickhouse_and_generic()
                .parse_sql_statements(
                    format!("ALTER TABLE t0 {keyword} PROJECTION my_name IN",).as_str()
                )
                .unwrap_err(),
            ParserError("Expected: end of statement, found: IN".to_string())
        );
    }
}

#[test]
fn parse_optimize_table() {
    clickhouse_and_generic().verified_stmt("OPTIMIZE TABLE t0");
    clickhouse_and_generic().verified_stmt("OPTIMIZE TABLE db.t0");
    clickhouse_and_generic().verified_stmt("OPTIMIZE TABLE t0 ON CLUSTER 'cluster'");
    clickhouse_and_generic().verified_stmt("OPTIMIZE TABLE t0 ON CLUSTER 'cluster' FINAL");
    clickhouse_and_generic().verified_stmt("OPTIMIZE TABLE t0 FINAL DEDUPLICATE");
    clickhouse_and_generic().verified_stmt("OPTIMIZE TABLE t0 DEDUPLICATE");
    clickhouse_and_generic().verified_stmt("OPTIMIZE TABLE t0 DEDUPLICATE BY id");
    clickhouse_and_generic().verified_stmt("OPTIMIZE TABLE t0 FINAL DEDUPLICATE BY id");
    clickhouse_and_generic()
        .verified_stmt("OPTIMIZE TABLE t0 PARTITION tuple('2023-04-22') DEDUPLICATE BY id");
    match clickhouse_and_generic().verified_stmt(
        "OPTIMIZE TABLE t0 ON CLUSTER cluster PARTITION ID '2024-07' FINAL DEDUPLICATE BY id",
    ) {
        Statement::OptimizeTable {
            name,
            on_cluster,
            partition,
            include_final,
            deduplicate,
            ..
        } => {
            assert_eq!(name.to_string(), "t0");
            assert_eq!(on_cluster, Some(Ident::new("cluster")));
            assert_eq!(
                partition,
                Some(Partition::Identifier(Ident::with_quote('\'', "2024-07")))
            );
            assert!(include_final);
            assert_eq!(
                deduplicate,
                Some(Deduplicate::ByExpression(Identifier(Ident::new("id"))))
            );
        }
        _ => unreachable!(),
    }

    // negative cases
    assert_eq!(
        clickhouse_and_generic()
            .parse_sql_statements("OPTIMIZE TABLE t0 DEDUPLICATE BY")
            .unwrap_err(),
        ParserError("Expected: an expression, found: EOF".to_string())
    );
    assert_eq!(
        clickhouse_and_generic()
            .parse_sql_statements("OPTIMIZE TABLE t0 PARTITION")
            .unwrap_err(),
        ParserError("Expected: an expression, found: EOF".to_string())
    );
    assert_eq!(
        clickhouse_and_generic()
            .parse_sql_statements("OPTIMIZE TABLE t0 PARTITION ID")
            .unwrap_err(),
        ParserError("Expected: identifier, found: EOF".to_string())
    );
}

fn column_def(name: Ident, data_type: DataType) -> ColumnDef {
    ColumnDef {
        name,
        data_type,
        options: vec![],
    }
}

#[test]
fn parse_clickhouse_data_types() {
    let sql = concat!(
        "CREATE TABLE table (",
        "a1 UInt8, a2 UInt16, a3 UInt32, a4 UInt64, a5 UInt128, a6 UInt256,",
        " b1 Int8, b2 Int16, b3 Int32, b4 Int64, b5 Int128, b6 Int256,",
        " c1 Float32, c2 Float64,",
        " d1 Date32, d2 DateTime64(3), d3 DateTime64(3, 'UTC'),",
        " e1 FixedString(255),",
        " f1 LowCardinality(Int32)",
        ") ORDER BY (a1)",
    );
    // ClickHouse has a case-sensitive definition of data type, but canonical representation is not
    let canonical_sql = sql
        .replace(" Int8", " INT8")
        .replace(" Int64", " INT64")
        .replace(" Float64", " FLOAT64");

    match clickhouse_and_generic().one_statement_parses_to(sql, &canonical_sql) {
        Statement::CreateTable(CreateTable { name, columns, .. }) => {
            assert_eq!(name, ObjectName::from(vec!["table".into()]));
            assert_eq!(
                columns,
                vec![
                    column_def("a1".into(), DataType::UInt8),
                    column_def("a2".into(), DataType::UInt16),
                    column_def("a3".into(), DataType::UInt32),
                    column_def("a4".into(), DataType::UInt64),
                    column_def("a5".into(), DataType::UInt128),
                    column_def("a6".into(), DataType::UInt256),
                    column_def("b1".into(), DataType::Int8(None)),
                    column_def("b2".into(), DataType::Int16),
                    column_def("b3".into(), DataType::Int32),
                    column_def("b4".into(), DataType::Int64),
                    column_def("b5".into(), DataType::Int128),
                    column_def("b6".into(), DataType::Int256),
                    column_def("c1".into(), DataType::Float32),
                    column_def("c2".into(), DataType::Float64),
                    column_def("d1".into(), DataType::Date32),
                    column_def("d2".into(), DataType::Datetime64(3, None)),
                    column_def("d3".into(), DataType::Datetime64(3, Some("UTC".into()))),
                    column_def("e1".into(), DataType::FixedString(255)),
                    column_def(
                        "f1".into(),
                        DataType::LowCardinality(Box::new(DataType::Int32))
                    ),
                ]
            );
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_table_with_nullable() {
    let sql = r#"CREATE TABLE table (k UInt8, `a` Nullable(String), `b` Nullable(DateTime64(9, 'UTC')), c Nullable(DateTime64(9)), d Date32 NULL) ENGINE=MergeTree ORDER BY (`k`)"#;
    // ClickHouse has a case-sensitive definition of data type, but canonical representation is not
    let canonical_sql = sql.replace("String", "STRING");

    match clickhouse_and_generic().one_statement_parses_to(sql, &canonical_sql) {
        Statement::CreateTable(CreateTable { name, columns, .. }) => {
            assert_eq!(name, ObjectName::from(vec!["table".into()]));
            assert_eq!(
                columns,
                vec![
                    column_def("k".into(), DataType::UInt8),
                    column_def(
                        Ident::with_quote('`', "a"),
                        DataType::Nullable(Box::new(DataType::String(None)))
                    ),
                    column_def(
                        Ident::with_quote('`', "b"),
                        DataType::Nullable(Box::new(DataType::Datetime64(
                            9,
                            Some("UTC".to_string())
                        )))
                    ),
                    column_def(
                        "c".into(),
                        DataType::Nullable(Box::new(DataType::Datetime64(9, None)))
                    ),
                    ColumnDef {
                        name: "d".into(),
                        data_type: DataType::Date32,
                        options: vec![ColumnOptionDef {
                            name: None,
                            option: ColumnOption::Null
                        }],
                    }
                ]
            );
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_table_with_nested_data_types() {
    let sql = concat!(
        "CREATE TABLE table (",
        " i Nested(a Array(Int16), b LowCardinality(String)),",
        " k Array(Tuple(FixedString(128), Int128)),",
        " l Tuple(a DateTime64(9), b Array(UUID)),",
        " m Map(String, UInt16)",
        ") ENGINE=MergeTree ORDER BY (k)"
    );

    match clickhouse().one_statement_parses_to(sql, "") {
        Statement::CreateTable(CreateTable { name, columns, .. }) => {
            assert_eq!(name, ObjectName::from(vec!["table".into()]));
            assert_eq!(
                columns,
                vec![
                    ColumnDef {
                        name: Ident::new("i"),
                        data_type: DataType::Nested(vec![
                            column_def(
                                "a".into(),
                                DataType::Array(ArrayElemTypeDef::Parenthesis(Box::new(
                                    DataType::Int16
                                ),))
                            ),
                            column_def(
                                "b".into(),
                                DataType::LowCardinality(Box::new(DataType::String(None)))
                            )
                        ]),
                        options: vec![],
                    },
                    ColumnDef {
                        name: Ident::new("k"),
                        data_type: DataType::Array(ArrayElemTypeDef::Parenthesis(Box::new(
                            DataType::Tuple(vec![
                                StructField {
                                    field_name: None,
                                    field_type: DataType::FixedString(128)
                                },
                                StructField {
                                    field_name: None,
                                    field_type: DataType::Int128
                                }
                            ])
                        ))),
                        options: vec![],
                    },
                    ColumnDef {
                        name: Ident::new("l"),
                        data_type: DataType::Tuple(vec![
                            StructField {
                                field_name: Some("a".into()),
                                field_type: DataType::Datetime64(9, None),
                            },
                            StructField {
                                field_name: Some("b".into()),
                                field_type: DataType::Array(ArrayElemTypeDef::Parenthesis(
                                    Box::new(DataType::Uuid)
                                ))
                            },
                        ]),
                        options: vec![],
                    },
                    ColumnDef {
                        name: Ident::new("m"),
                        data_type: DataType::Map(
                            Box::new(DataType::String(None)),
                            Box::new(DataType::UInt16)
                        ),
                        options: vec![],
                    },
                ]
            );
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_table_with_primary_key() {
    match clickhouse_and_generic().verified_stmt(concat!(
        r#"CREATE TABLE db.table (`i` INT, `k` INT)"#,
        " ENGINE=SharedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')",
        " PRIMARY KEY tuple(i)",
        " ORDER BY tuple(i)",
    )) {
        Statement::CreateTable(CreateTable {
            name,
            columns,
            engine,
            primary_key,
            order_by,
            ..
        }) => {
            assert_eq!(name.to_string(), "db.table");
            assert_eq!(
                vec![
                    ColumnDef {
                        name: Ident::with_quote('`', "i"),
                        data_type: DataType::Int(None),
                        options: vec![],
                    },
                    ColumnDef {
                        name: Ident::with_quote('`', "k"),
                        data_type: DataType::Int(None),
                        options: vec![],
                    },
                ],
                columns
            );
            assert_eq!(
                engine,
                Some(TableEngine {
                    name: "SharedMergeTree".to_string(),
                    parameters: Some(vec![
                        Ident::with_quote('\'', "/clickhouse/tables/{uuid}/{shard}"),
                        Ident::with_quote('\'', "{replica}"),
                    ]),
                })
            );
            fn assert_function(actual: &Function, name: &str, arg: &str) -> bool {
                assert_eq!(actual.name, ObjectName::from(vec![Ident::new(name)]));
                assert_eq!(
                    actual.args,
                    FunctionArguments::List(FunctionArgumentList {
                        args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(Identifier(
                            Ident::new(arg)
                        )),)],
                        duplicate_treatment: None,
                        clauses: vec![],
                    })
                );
                true
            }
            match primary_key.unwrap().as_ref() {
                Expr::Function(primary_key) => {
                    assert!(assert_function(primary_key, "tuple", "i"));
                }
                _ => panic!("unexpected primary key type"),
            }
            match order_by {
                Some(OneOrManyWithParens::One(Expr::Function(order_by))) => {
                    assert!(assert_function(&order_by, "tuple", "i"));
                }
                _ => panic!("unexpected order by type"),
            };
        }
        _ => unreachable!(),
    }

    clickhouse_and_generic()
        .parse_sql_statements(concat!(
            r#"CREATE TABLE db.table (`i` Int, `k` Int)"#,
            " ORDER BY tuple(i), tuple(k)",
        ))
        .expect_err("ORDER BY supports one expression with tuple");
}

#[test]
fn parse_create_table_with_variant_default_expressions() {
    let sql = concat!(
        "CREATE TABLE table (",
        "a DATETIME MATERIALIZED now(),",
        " b DATETIME EPHEMERAL now(),",
        " c DATETIME EPHEMERAL,",
        " d STRING ALIAS toString(c)",
        ") ENGINE=MergeTree"
    );
    match clickhouse_and_generic().verified_stmt(sql) {
        Statement::CreateTable(CreateTable { columns, .. }) => {
            assert_eq!(
                columns,
                vec![
                    ColumnDef {
                        name: Ident::new("a"),
                        data_type: DataType::Datetime(None),
                        options: vec![ColumnOptionDef {
                            name: None,
                            option: ColumnOption::Materialized(Expr::Function(Function {
                                name: ObjectName::from(vec![Ident::new("now")]),
                                uses_odbc_syntax: false,
                                args: FunctionArguments::List(FunctionArgumentList {
                                    args: vec![],
                                    duplicate_treatment: None,
                                    clauses: vec![],
                                }),
                                parameters: FunctionArguments::None,
                                null_treatment: None,
                                filter: None,
                                over: None,
                                within_group: vec![],
                            }))
                        }],
                    },
                    ColumnDef {
                        name: Ident::new("b"),
                        data_type: DataType::Datetime(None),
                        options: vec![ColumnOptionDef {
                            name: None,
                            option: ColumnOption::Ephemeral(Some(Expr::Function(Function {
                                name: ObjectName::from(vec![Ident::new("now")]),
                                uses_odbc_syntax: false,
                                args: FunctionArguments::List(FunctionArgumentList {
                                    args: vec![],
                                    duplicate_treatment: None,
                                    clauses: vec![],
                                }),
                                parameters: FunctionArguments::None,
                                null_treatment: None,
                                filter: None,
                                over: None,
                                within_group: vec![],
                            })))
                        }],
                    },
                    ColumnDef {
                        name: Ident::new("c"),
                        data_type: DataType::Datetime(None),
                        options: vec![ColumnOptionDef {
                            name: None,
                            option: ColumnOption::Ephemeral(None)
                        }],
                    },
                    ColumnDef {
                        name: Ident::new("d"),
                        data_type: DataType::String(None),
                        options: vec![ColumnOptionDef {
                            name: None,
                            option: ColumnOption::Alias(Expr::Function(Function {
                                name: ObjectName::from(vec![Ident::new("toString")]),
                                uses_odbc_syntax: false,
                                args: FunctionArguments::List(FunctionArgumentList {
                                    args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(
                                        Identifier(Ident::new("c"))
                                    ))],
                                    duplicate_treatment: None,
                                    clauses: vec![],
                                }),
                                parameters: FunctionArguments::None,
                                null_treatment: None,
                                filter: None,
                                over: None,
                                within_group: vec![],
                            }))
                        }],
                    }
                ]
            )
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_view_with_fields_data_types() {
    match clickhouse().verified_stmt(r#"CREATE VIEW v (i "int", f "String") AS SELECT * FROM t"#) {
        Statement::CreateView { name, columns, .. } => {
            assert_eq!(name, ObjectName::from(vec!["v".into()]));
            assert_eq!(
                columns,
                vec![
                    ViewColumnDef {
                        name: "i".into(),
                        data_type: Some(DataType::Custom(
                            ObjectName::from(vec![Ident {
                                value: "int".into(),
                                quote_style: Some('"'),
                                span: Span::empty(),
                            }]),
                            vec![]
                        )),
                        options: None
                    },
                    ViewColumnDef {
                        name: "f".into(),
                        data_type: Some(DataType::Custom(
                            ObjectName::from(vec![Ident {
                                value: "String".into(),
                                quote_style: Some('"'),
                                span: Span::empty(),
                            }]),
                            vec![]
                        )),
                        options: None
                    },
                ]
            );
        }
        _ => unreachable!(),
    }

    clickhouse()
        .parse_sql_statements(r#"CREATE VIEW v (i, f) AS SELECT * FROM t"#)
        .expect_err("CREATE VIEW with fields and without data types should be invalid");
}

#[test]
fn parse_double_equal() {
    clickhouse().one_statement_parses_to(
        r#"SELECT foo FROM bar WHERE buz == 'buz'"#,
        r#"SELECT foo FROM bar WHERE buz = 'buz'"#,
    );
}

#[test]
fn parse_limit_by() {
    clickhouse_and_generic().verified_stmt(
        r#"SELECT * FROM default.last_asset_runs_mv ORDER BY created_at DESC LIMIT 1 BY asset"#,
    );
    clickhouse_and_generic().verified_stmt(
        r#"SELECT * FROM default.last_asset_runs_mv ORDER BY created_at DESC LIMIT 1 BY asset, toStartOfDay(created_at)"#,
    );
}

#[test]
fn parse_settings_in_query() {
    match clickhouse_and_generic()
        .verified_stmt(r#"SELECT * FROM t SETTINGS max_threads = 1, max_block_size = 10000"#)
    {
        Statement::Query(query) => {
            assert_eq!(
                query.settings,
                Some(vec![
                    Setting {
                        key: Ident::new("max_threads"),
                        value: Number("1".parse().unwrap(), false)
                    },
                    Setting {
                        key: Ident::new("max_block_size"),
                        value: Number("10000".parse().unwrap(), false)
                    },
                ])
            );
        }
        _ => unreachable!(),
    }

    let invalid_cases = vec![
        "SELECT * FROM t SETTINGS a",
        "SELECT * FROM t SETTINGS a=",
        "SELECT * FROM t SETTINGS a=1, b",
        "SELECT * FROM t SETTINGS a=1, b=",
        "SELECT * FROM t SETTINGS a=1, b=c",
    ];
    for sql in invalid_cases {
        clickhouse_and_generic()
            .parse_sql_statements(sql)
            .expect_err("Expected: SETTINGS key = value, found: ");
    }
}
#[test]
fn parse_select_star_except() {
    clickhouse().verified_stmt("SELECT * EXCEPT (prev_status) FROM anomalies");
}

#[test]
fn parse_select_parametric_function() {
    match clickhouse_and_generic().verified_stmt("SELECT HISTOGRAM(0.5, 0.6)(x, y) FROM t") {
        Statement::Query(query) => {
            let projection: &Vec<SelectItem> = query.body.as_select().unwrap().projection.as_ref();
            assert_eq!(projection.len(), 1);
            match &projection[0] {
                UnnamedExpr(Expr::Function(f)) => {
                    let args = match &f.args {
                        FunctionArguments::List(args) => args,
                        _ => unreachable!(),
                    };
                    assert_eq!(args.args.len(), 2);
                    assert_eq!(
                        args.args[0],
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(Identifier(Ident::from("x"))))
                    );
                    assert_eq!(
                        args.args[1],
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(Identifier(Ident::from("y"))))
                    );

                    let parameters = match f.parameters {
                        FunctionArguments::List(ref args) => args,
                        _ => unreachable!(),
                    };
                    assert_eq!(parameters.args.len(), 2);
                    assert_eq!(
                        parameters.args[0],
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(Value::Number(
                            "0.5".parse().unwrap(),
                            false
                        ))))
                    );
                    assert_eq!(
                        parameters.args[1],
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(Value::Number(
                            "0.6".parse().unwrap(),
                            false
                        ))))
                    );
                }
                _ => unreachable!(),
            }
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_select_star_except_no_parens() {
    clickhouse().one_statement_parses_to(
        "SELECT * EXCEPT prev_status FROM anomalies",
        "SELECT * EXCEPT (prev_status) FROM anomalies",
    );
}

#[test]
fn parse_create_materialized_view() {
    // example sql
    // https://clickhouse.com/docs/en/guides/developer/cascading-materialized-views
    let sql = concat!(
        "CREATE MATERIALIZED VIEW analytics.monthly_aggregated_data_mv ",
        "TO analytics.monthly_aggregated_data ",
        "AS SELECT toDate(toStartOfMonth(event_time)) ",
        "AS month, domain_name, sumState(count_views) ",
        "AS sumCountViews FROM analytics.hourly_data ",
        "GROUP BY domain_name, month"
    );
    clickhouse_and_generic().verified_stmt(sql);
}

#[test]
fn parse_select_order_by_with_fill_interpolate() {
    let sql = "SELECT id, fname, lname FROM customer WHERE id < 5 \
        ORDER BY \
            fname ASC NULLS FIRST WITH FILL FROM 10 TO 20 STEP 2, \
            lname DESC NULLS LAST WITH FILL FROM 30 TO 40 STEP 3 \
            INTERPOLATE (col1 AS col1 + 1) \
        LIMIT 2";
    let select = clickhouse().verified_query(sql);
    assert_eq!(
        OrderBy {
            exprs: vec![
                OrderByExpr {
                    expr: Expr::Identifier(Ident::new("fname")),
                    asc: Some(true),
                    nulls_first: Some(true),
                    with_fill: Some(WithFill {
                        from: Some(Expr::Value(number("10"))),
                        to: Some(Expr::Value(number("20"))),
                        step: Some(Expr::Value(number("2"))),
                    }),
                },
                OrderByExpr {
                    expr: Expr::Identifier(Ident::new("lname")),
                    asc: Some(false),
                    nulls_first: Some(false),
                    with_fill: Some(WithFill {
                        from: Some(Expr::Value(number("30"))),
                        to: Some(Expr::Value(number("40"))),
                        step: Some(Expr::Value(number("3"))),
                    }),
                },
            ],
            interpolate: Some(Interpolate {
                exprs: Some(vec![InterpolateExpr {
                    column: Ident::new("col1"),
                    expr: Some(Expr::BinaryOp {
                        left: Box::new(Expr::Identifier(Ident::new("col1"))),
                        op: BinaryOperator::Plus,
                        right: Box::new(Expr::Value(number("1"))),
                    }),
                }])
            })
        },
        select.order_by.expect("ORDER BY expected")
    );
    assert_eq!(Some(Expr::Value(number("2"))), select.limit);
}

#[test]
fn parse_select_order_by_with_fill_interpolate_multi_interpolates() {
    let sql = "SELECT id, fname, lname FROM customer ORDER BY fname WITH FILL \
        INTERPOLATE (col1 AS col1 + 1) INTERPOLATE (col2 AS col2 + 2)";
    clickhouse_and_generic()
        .parse_sql_statements(sql)
        .expect_err("ORDER BY only accepts a single INTERPOLATE clause");
}

#[test]
fn parse_select_order_by_with_fill_interpolate_multi_with_fill_interpolates() {
    let sql = "SELECT id, fname, lname FROM customer \
        ORDER BY \
            fname WITH FILL INTERPOLATE (col1 AS col1 + 1), \
            lname WITH FILL INTERPOLATE (col2 AS col2 + 2)";
    clickhouse_and_generic()
        .parse_sql_statements(sql)
        .expect_err("ORDER BY only accepts a single INTERPOLATE clause");
}

#[test]
fn parse_select_order_by_interpolate_not_last() {
    let sql = "SELECT id, fname, lname FROM customer \
        ORDER BY \
            fname INTERPOLATE (col2 AS col2 + 2),
            lname";
    clickhouse_and_generic()
        .parse_sql_statements(sql)
        .expect_err("ORDER BY INTERPOLATE must be in the last position");
}

#[test]
fn parse_with_fill() {
    let sql = "SELECT fname FROM customer ORDER BY fname \
        WITH FILL FROM 10 TO 20 STEP 2";
    let select = clickhouse().verified_query(sql);
    assert_eq!(
        Some(WithFill {
            from: Some(Expr::Value(number("10"))),
            to: Some(Expr::Value(number("20"))),
            step: Some(Expr::Value(number("2"))),
        }),
        select.order_by.expect("ORDER BY expected").exprs[0].with_fill
    );
}

#[test]
fn parse_with_fill_missing_single_argument() {
    let sql = "SELECT id, fname, lname FROM customer ORDER BY \
            fname WITH FILL FROM TO 20";
    clickhouse_and_generic()
        .parse_sql_statements(sql)
        .expect_err("WITH FILL requires expressions for all arguments");
}

#[test]
fn parse_with_fill_multiple_incomplete_arguments() {
    let sql = "SELECT id, fname, lname FROM customer ORDER BY \
            fname WITH FILL FROM TO 20, lname WITH FILL FROM TO STEP 1";
    clickhouse_and_generic()
        .parse_sql_statements(sql)
        .expect_err("WITH FILL requires expressions for all arguments");
}

#[test]
fn parse_interpolate_body_with_columns() {
    let sql = "SELECT fname FROM customer ORDER BY fname WITH FILL \
        INTERPOLATE (col1 AS col1 + 1, col2 AS col3, col4 AS col4 + 4)";
    let select = clickhouse().verified_query(sql);
    assert_eq!(
        Some(Interpolate {
            exprs: Some(vec![
                InterpolateExpr {
                    column: Ident::new("col1"),
                    expr: Some(Expr::BinaryOp {
                        left: Box::new(Expr::Identifier(Ident::new("col1"))),
                        op: BinaryOperator::Plus,
                        right: Box::new(Expr::Value(number("1"))),
                    }),
                },
                InterpolateExpr {
                    column: Ident::new("col2"),
                    expr: Some(Expr::Identifier(Ident::new("col3"))),
                },
                InterpolateExpr {
                    column: Ident::new("col4"),
                    expr: Some(Expr::BinaryOp {
                        left: Box::new(Expr::Identifier(Ident::new("col4"))),
                        op: BinaryOperator::Plus,
                        right: Box::new(Expr::Value(number("4"))),
                    }),
                },
            ])
        }),
        select.order_by.expect("ORDER BY expected").interpolate
    );
}

#[test]
fn parse_interpolate_without_body() {
    let sql = "SELECT fname FROM customer ORDER BY fname WITH FILL INTERPOLATE";
    let select = clickhouse().verified_query(sql);
    assert_eq!(
        Some(Interpolate { exprs: None }),
        select.order_by.expect("ORDER BY expected").interpolate
    );
}

#[test]
fn parse_interpolate_with_empty_body() {
    let sql = "SELECT fname FROM customer ORDER BY fname WITH FILL INTERPOLATE ()";
    let select = clickhouse().verified_query(sql);
    assert_eq!(
        Some(Interpolate {
            exprs: Some(vec![])
        }),
        select.order_by.expect("ORDER BY expected").interpolate
    );
}

#[test]
fn test_prewhere() {
    match clickhouse_and_generic().verified_stmt("SELECT * FROM t PREWHERE x = 1 WHERE y = 2") {
        Statement::Query(query) => {
            let prewhere = query.body.as_select().unwrap().prewhere.as_ref();
            assert_eq!(
                prewhere,
                Some(&BinaryOp {
                    left: Box::new(Identifier(Ident::new("x"))),
                    op: BinaryOperator::Eq,
                    right: Box::new(Expr::Value(Value::Number("1".parse().unwrap(), false))),
                })
            );
            let selection = query.as_ref().body.as_select().unwrap().selection.as_ref();
            assert_eq!(
                selection,
                Some(&BinaryOp {
                    left: Box::new(Identifier(Ident::new("y"))),
                    op: BinaryOperator::Eq,
                    right: Box::new(Expr::Value(Value::Number("2".parse().unwrap(), false))),
                })
            );
        }
        _ => unreachable!(),
    }

    match clickhouse_and_generic().verified_stmt("SELECT * FROM t PREWHERE x = 1 AND y = 2") {
        Statement::Query(query) => {
            let prewhere = query.body.as_select().unwrap().prewhere.as_ref();
            assert_eq!(
                prewhere,
                Some(&BinaryOp {
                    left: Box::new(BinaryOp {
                        left: Box::new(Identifier(Ident::new("x"))),
                        op: BinaryOperator::Eq,
                        right: Box::new(Expr::Value(Value::Number("1".parse().unwrap(), false))),
                    }),
                    op: BinaryOperator::And,
                    right: Box::new(BinaryOp {
                        left: Box::new(Identifier(Ident::new("y"))),
                        op: BinaryOperator::Eq,
                        right: Box::new(Expr::Value(Value::Number("2".parse().unwrap(), false))),
                    }),
                })
            );
        }
        _ => unreachable!(),
    }
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
    let quote_styles = ['"', '`'];

    for object_name in &valid_object_names {
        // Test single identifier without quotes
        assert_eq!(
            clickhouse().verified_stmt(&format!("USE {}", object_name)),
            Statement::Use(Use::Object(ObjectName::from(vec![Ident::new(
                object_name.to_string()
            )])))
        );
        for &quote in &quote_styles {
            // Test single identifier with different type of quotes
            assert_eq!(
                clickhouse().verified_stmt(&format!("USE {0}{1}{0}", quote, object_name)),
                Statement::Use(Use::Object(ObjectName::from(vec![Ident::with_quote(
                    quote,
                    object_name.to_string(),
                )])))
            );
        }
    }
}

#[test]
fn test_query_with_format_clause() {
    let format_options = vec!["TabSeparated", "JSONCompact", "NULL"];
    for format in &format_options {
        let sql = format!("SELECT * FROM t FORMAT {}", format);
        match clickhouse_and_generic().verified_stmt(&sql) {
            Statement::Query(query) => {
                if *format == "NULL" {
                    assert_eq!(query.format_clause, Some(FormatClause::Null));
                } else {
                    assert_eq!(
                        query.format_clause,
                        Some(FormatClause::Identifier(Ident::new(*format)))
                    );
                }
            }
            _ => unreachable!(),
        }
    }

    let invalid_cases = [
        "SELECT * FROM t FORMAT",
        "SELECT * FROM t FORMAT TabSeparated JSONCompact",
        "SELECT * FROM t FORMAT TabSeparated TabSeparated",
    ];
    for sql in &invalid_cases {
        clickhouse_and_generic()
            .parse_sql_statements(sql)
            .expect_err("Expected: FORMAT {identifier}, found: ");
    }
}

#[test]
fn test_insert_query_with_format_clause() {
    let cases = [
        r#"INSERT INTO tbl FORMAT JSONEachRow {"id": 1, "value": "foo"}, {"id": 2, "value": "bar"}"#,
        r#"INSERT INTO tbl FORMAT JSONEachRow ["first", "second", "third"]"#,
        r#"INSERT INTO tbl FORMAT JSONEachRow [{"first": 1}]"#,
        r#"INSERT INTO tbl (foo) FORMAT JSONAsObject {"foo": {"bar": {"x": "y"}, "baz": 1}}"#,
        r#"INSERT INTO tbl (foo, bar) FORMAT JSON {"foo": 1, "bar": 2}"#,
        r#"INSERT INTO tbl FORMAT CSV col1, col2, col3"#,
        r#"INSERT INTO tbl FORMAT LineAsString "I love apple", "I love banana", "I love orange""#,
        r#"INSERT INTO tbl (foo) SETTINGS input_format_json_read_bools_as_numbers = true FORMAT JSONEachRow {"id": 1, "value": "foo"}"#,
        r#"INSERT INTO tbl SETTINGS format_template_resultset = '/some/path/resultset.format', format_template_row = '/some/path/row.format' FORMAT Template"#,
        r#"INSERT INTO tbl SETTINGS input_format_json_read_bools_as_numbers = true FORMAT JSONEachRow {"id": 1, "value": "foo"}"#,
    ];

    for sql in &cases {
        clickhouse().verified_stmt(sql);
    }
}

#[test]
fn parse_create_table_on_commit_and_as_query() {
    let sql = r#"CREATE LOCAL TEMPORARY TABLE test ON COMMIT PRESERVE ROWS AS SELECT 1"#;
    match clickhouse_and_generic().verified_stmt(sql) {
        Statement::CreateTable(CreateTable {
            name,
            on_commit,
            query,
            ..
        }) => {
            assert_eq!(name.to_string(), "test");
            assert_eq!(on_commit, Some(OnCommit::PreserveRows));
            assert_eq!(
                query.unwrap().body.as_select().unwrap().projection,
                vec![UnnamedExpr(Expr::Value(Value::Number(
                    "1".parse().unwrap(),
                    false
                )))]
            );
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_freeze_and_unfreeze_partition() {
    // test cases without `WITH NAME`
    for operation_name in &["FREEZE", "UNFREEZE"] {
        let sql = format!("ALTER TABLE t {operation_name} PARTITION '2024-08-14'");

        let expected_partition = Partition::Expr(Expr::Value(Value::SingleQuotedString(
            "2024-08-14".to_string(),
        )));
        match clickhouse_and_generic().verified_stmt(&sql) {
            Statement::AlterTable { operations, .. } => {
                assert_eq!(operations.len(), 1);
                let expected_operation = if operation_name == &"FREEZE" {
                    AlterTableOperation::FreezePartition {
                        partition: expected_partition,
                        with_name: None,
                    }
                } else {
                    AlterTableOperation::UnfreezePartition {
                        partition: expected_partition,
                        with_name: None,
                    }
                };
                assert_eq!(operations[0], expected_operation);
            }
            _ => unreachable!(),
        }
    }

    // test case with `WITH NAME`
    for operation_name in &["FREEZE", "UNFREEZE"] {
        let sql =
            format!("ALTER TABLE t {operation_name} PARTITION '2024-08-14' WITH NAME 'hello'");
        match clickhouse_and_generic().verified_stmt(&sql) {
            Statement::AlterTable { operations, .. } => {
                assert_eq!(operations.len(), 1);
                let expected_partition = Partition::Expr(Expr::Value(Value::SingleQuotedString(
                    "2024-08-14".to_string(),
                )));
                let expected_operation = if operation_name == &"FREEZE" {
                    AlterTableOperation::FreezePartition {
                        partition: expected_partition,
                        with_name: Some(Ident::with_quote('\'', "hello")),
                    }
                } else {
                    AlterTableOperation::UnfreezePartition {
                        partition: expected_partition,
                        with_name: Some(Ident::with_quote('\'', "hello")),
                    }
                };
                assert_eq!(operations[0], expected_operation);
            }
            _ => unreachable!(),
        }
    }

    // negative cases
    for operation_name in &["FREEZE", "UNFREEZE"] {
        assert_eq!(
            clickhouse_and_generic()
                .parse_sql_statements(format!("ALTER TABLE t0 {operation_name} PARTITION").as_str())
                .unwrap_err(),
            ParserError("Expected: an expression, found: EOF".to_string())
        );
        assert_eq!(
            clickhouse_and_generic()
                .parse_sql_statements(
                    format!("ALTER TABLE t0 {operation_name} PARTITION p0 WITH").as_str()
                )
                .unwrap_err(),
            ParserError("Expected: NAME, found: EOF".to_string())
        );
        assert_eq!(
            clickhouse_and_generic()
                .parse_sql_statements(
                    format!("ALTER TABLE t0 {operation_name} PARTITION p0 WITH NAME").as_str()
                )
                .unwrap_err(),
            ParserError("Expected: identifier, found: EOF".to_string())
        );
    }
}

#[test]
fn parse_select_table_function_settings() {
    fn check_settings(sql: &str, expected: &TableFunctionArgs) {
        match clickhouse_and_generic().verified_stmt(sql) {
            Statement::Query(q) => {
                let from = &q.body.as_select().unwrap().from;
                assert_eq!(from.len(), 1);
                assert_eq!(from[0].joins, vec![]);
                match &from[0].relation {
                    Table { args, .. } => {
                        let args = args.as_ref().unwrap();
                        assert_eq!(args, expected);
                    }
                    _ => unreachable!(),
                }
            }
            _ => unreachable!(),
        }
    }
    check_settings(
        "SELECT * FROM table_function(arg, SETTINGS s0 = 3, s1 = 's')",
        &TableFunctionArgs {
            args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(
                Expr::Identifier("arg".into()),
            ))],

            settings: Some(vec![
                Setting {
                    key: "s0".into(),
                    value: Value::Number("3".parse().unwrap(), false),
                },
                Setting {
                    key: "s1".into(),
                    value: Value::SingleQuotedString("s".into()),
                },
            ]),
        },
    );
    check_settings(
        r#"SELECT * FROM table_function(arg)"#,
        &TableFunctionArgs {
            args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(
                Expr::Identifier("arg".into()),
            ))],
            settings: None,
        },
    );
    check_settings(
        "SELECT * FROM table_function(SETTINGS s0 = 3, s1 = 's')",
        &TableFunctionArgs {
            args: vec![],
            settings: Some(vec![
                Setting {
                    key: "s0".into(),
                    value: Value::Number("3".parse().unwrap(), false),
                },
                Setting {
                    key: "s1".into(),
                    value: Value::SingleQuotedString("s".into()),
                },
            ]),
        },
    );
    let invalid_cases = vec![
        "SELECT * FROM t(SETTINGS a)",
        "SELECT * FROM t(SETTINGS a=)",
        "SELECT * FROM t(SETTINGS a=1, b)",
        "SELECT * FROM t(SETTINGS a=1, b=)",
        "SELECT * FROM t(SETTINGS a=1, b=c)",
    ];
    for sql in invalid_cases {
        clickhouse_and_generic()
            .parse_sql_statements(sql)
            .expect_err("Expected: SETTINGS key = value, found: ");
    }
}

#[test]
fn explain_describe() {
    clickhouse().verified_stmt("DESCRIBE test.table");
    clickhouse().verified_stmt("DESCRIBE TABLE test.table");
}

#[test]
fn explain_desc() {
    clickhouse().verified_stmt("DESC test.table");
    clickhouse().verified_stmt("DESC TABLE test.table");
}

#[test]
fn parse_explain_table() {
    match clickhouse().verified_stmt("EXPLAIN TABLE test_identifier") {
        Statement::ExplainTable {
            describe_alias,
            hive_format,
            has_table_keyword,
            table_name,
        } => {
            pretty_assertions::assert_eq!(describe_alias, DescribeAlias::Explain);
            pretty_assertions::assert_eq!(hive_format, None);
            pretty_assertions::assert_eq!(has_table_keyword, true);
            pretty_assertions::assert_eq!("test_identifier", table_name.to_string());
        }
        _ => panic!("Unexpected Statement, must be ExplainTable"),
    }
}

#[test]
fn parse_table_sample() {
    clickhouse().verified_stmt("SELECT * FROM tbl SAMPLE 0.1");
    clickhouse().verified_stmt("SELECT * FROM tbl SAMPLE 1000");
    clickhouse().verified_stmt("SELECT * FROM tbl SAMPLE 1 / 10");
    clickhouse().verified_stmt("SELECT * FROM tbl SAMPLE 1 / 10 OFFSET 1 / 2");
}

fn clickhouse() -> TestedDialects {
    TestedDialects::new(vec![Box::new(ClickHouseDialect {})])
}

fn clickhouse_and_generic() -> TestedDialects {
    TestedDialects::new(vec![
        Box::new(ClickHouseDialect {}),
        Box::new(GenericDialect {}),
    ])
}

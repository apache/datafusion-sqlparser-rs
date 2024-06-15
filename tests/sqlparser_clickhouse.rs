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
//! Test SQL syntax specific to ClickHouse.

#[macro_use]
mod test_utils;

use test_utils::*;

use sqlparser::ast::Expr::{BinaryOp, Identifier, MapAccess};
use sqlparser::ast::SelectItem::UnnamedExpr;
use sqlparser::ast::TableFactor::Table;
use sqlparser::ast::*;

use sqlparser::dialect::ClickHouseDialect;
use sqlparser::dialect::GenericDialect;

#[test]
fn parse_map_access_expr() {
    let sql = r#"SELECT string_values[indexOf(string_names, 'endpoint')] FROM foos WHERE id = 'test' AND string_value[indexOf(string_name, 'app')] <> 'foo'"#;
    let select = clickhouse().verified_only_select(sql);
    assert_eq!(
        Select {
            distinct: None,
            top: None,
            projection: vec![UnnamedExpr(MapAccess {
                column: Box::new(Identifier(Ident {
                    value: "string_values".to_string(),
                    quote_style: None,
                })),
                keys: vec![MapAccessKey {
                    key: call(
                        "indexOf",
                        [
                            Expr::Identifier(Ident::new("string_names")),
                            Expr::Value(Value::SingleQuotedString("endpoint".to_string()))
                        ]
                    ),
                    syntax: MapAccessSyntax::Bracket
                }],
            })],
            into: None,
            from: vec![TableWithJoins {
                relation: Table {
                    name: ObjectName(vec![Ident::new("foos")]),
                    alias: None,
                    args: None,
                    with_hints: vec![],
                    version: None,
                    partitions: vec![],
                },
                joins: vec![],
            }],
            lateral_views: vec![],
            selection: Some(BinaryOp {
                left: Box::new(BinaryOp {
                    left: Box::new(Identifier(Ident::new("id"))),
                    op: BinaryOperator::Eq,
                    right: Box::new(Expr::Value(Value::SingleQuotedString("test".to_string()))),
                }),
                op: BinaryOperator::And,
                right: Box::new(BinaryOp {
                    left: Box::new(MapAccess {
                        column: Box::new(Identifier(Ident::new("string_value"))),
                        keys: vec![MapAccessKey {
                            key: call(
                                "indexOf",
                                [
                                    Expr::Identifier(Ident::new("string_name")),
                                    Expr::Value(Value::SingleQuotedString("app".to_string()))
                                ]
                            ),
                            syntax: MapAccessSyntax::Bracket
                        }],
                    }),
                    op: BinaryOperator::NotEq,
                    right: Box::new(Expr::Value(Value::SingleQuotedString("foo".to_string()))),
                }),
            }),
            group_by: GroupByExpr::Expressions(vec![]),
            cluster_by: vec![],
            distribute_by: vec![],
            sort_by: vec![],
            having: None,
            named_window: vec![],
            window_before_qualify: false,
            qualify: None,
            value_table_mode: None,
            connect_by: None,
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
            partitions: _,
        } => {
            assert_eq!(vec![Ident::with_quote('"', "a table")], name.0);
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
            name: ObjectName(vec![Ident::with_quote('"', "myfun")]),
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

fn column_def(name: Ident, data_type: DataType) -> ColumnDef {
    ColumnDef {
        name,
        data_type,
        collation: None,
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
            assert_eq!(name, ObjectName(vec!["table".into()]));
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
            assert_eq!(name, ObjectName(vec!["table".into()]));
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
                        collation: None,
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
            assert_eq!(name, ObjectName(vec!["table".into()]));
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
                        collation: None,
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
                        collation: None,
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
                        collation: None,
                        options: vec![],
                    },
                    ColumnDef {
                        name: Ident::new("m"),
                        data_type: DataType::Map(
                            Box::new(DataType::String(None)),
                            Box::new(DataType::UInt16)
                        ),
                        collation: None,
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
                        collation: None,
                        options: vec![],
                    },
                    ColumnDef {
                        name: Ident::with_quote('`', "k"),
                        data_type: DataType::Int(None),
                        collation: None,
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
                assert_eq!(actual.name, ObjectName(vec![Ident::new(name)]));
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
fn parse_create_view_with_fields_data_types() {
    match clickhouse().verified_stmt(r#"CREATE VIEW v (i "int", f "String") AS SELECT * FROM t"#) {
        Statement::CreateView { name, columns, .. } => {
            assert_eq!(name, ObjectName(vec!["v".into()]));
            assert_eq!(
                columns,
                vec![
                    ViewColumnDef {
                        name: "i".into(),
                        data_type: Some(DataType::Custom(
                            ObjectName(vec![Ident {
                                value: "int".into(),
                                quote_style: Some('"')
                            }]),
                            vec![]
                        )),
                        options: None
                    },
                    ViewColumnDef {
                        name: "f".into(),
                        data_type: Some(DataType::Custom(
                            ObjectName(vec![Ident {
                                value: "String".into(),
                                quote_style: Some('"')
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
fn parse_select_star_except() {
    clickhouse().verified_stmt("SELECT * EXCEPT (prev_status) FROM anomalies");
}

#[test]
fn parse_select_star_except_no_parens() {
    clickhouse().one_statement_parses_to(
        "SELECT * EXCEPT prev_status FROM anomalies",
        "SELECT * EXCEPT (prev_status) FROM anomalies",
    );
}

#[test]
fn test_create_matireal_view_test() {
    // example sql
    // https://clickhouse.com/docs/en/guides/developer/cascading-materialized-views
    let sql = r#"CREATE MATERIALIZED VIEW analytics.monthly_aggregated_data_mv TO analytics.monthly_aggregated_data AS SELECT toDate(toStartOfMonth(event_time)) AS month, domain_name, sumState(count_views) AS sumCountViews FROM analytics.hourly_data GROUP BY domain_name, month"#;
    clickhouse().verified_stmt(sql);
}

fn clickhouse() -> TestedDialects {
    TestedDialects {
        dialects: vec![Box::new(ClickHouseDialect {})],
        options: None,
    }
}

fn clickhouse_and_generic() -> TestedDialects {
    TestedDialects {
        dialects: vec![Box::new(ClickHouseDialect {}), Box::new(GenericDialect {})],
        options: None,
    }
}

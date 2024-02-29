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

use sqlparser::ast;
use std::ops::Deref;

use sqlparser::ast::*;
use sqlparser::dialect::{BigQueryDialect, GenericDialect};
use sqlparser::parser::ParserError;
use test_utils::*;

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
fn parse_byte_literal() {
    let sql = r#"SELECT B'abc', B"abc""#;
    let select = bigquery().verified_only_select(sql);
    assert_eq!(2, select.projection.len());
    assert_eq!(
        &Expr::Value(Value::SingleQuotedByteStringLiteral("abc".to_string())),
        expr_from_projection(&select.projection[0])
    );
    assert_eq!(
        &Expr::Value(Value::DoubleQuotedByteStringLiteral("abc".to_string())),
        expr_from_projection(&select.projection[1])
    );

    let sql = r#"SELECT b'abc', b"abc""#;
    bigquery().one_statement_parses_to(sql, r#"SELECT B'abc', B"abc""#);
}

#[test]
fn parse_raw_literal() {
    let sql = r#"SELECT R'abc', R"abc", R'f\(abc,(.*),def\)', R"f\(abc,(.*),def\)""#;
    let stmt = bigquery().one_statement_parses_to(
        sql,
        r"SELECT R'abc', R'abc', R'f\(abc,(.*),def\)', R'f\(abc,(.*),def\)'",
    );
    if let Statement::Query(query) = stmt {
        if let SetExpr::Select(select) = *query.body {
            assert_eq!(4, select.projection.len());
            assert_eq!(
                &Expr::Value(Value::RawStringLiteral("abc".to_string())),
                expr_from_projection(&select.projection[0])
            );
            assert_eq!(
                &Expr::Value(Value::RawStringLiteral("abc".to_string())),
                expr_from_projection(&select.projection[1])
            );
            assert_eq!(
                &Expr::Value(Value::RawStringLiteral(r"f\(abc,(.*),def\)".to_string())),
                expr_from_projection(&select.projection[2])
            );
            assert_eq!(
                &Expr::Value(Value::RawStringLiteral(r"f\(abc,(.*),def\)".to_string())),
                expr_from_projection(&select.projection[3])
            );
            return;
        }
    }
    panic!("invalid query")
}

#[test]
fn parse_delete_statement() {
    let sql = "DELETE \"table\" WHERE 1";
    match bigquery_and_generic().verified_stmt(sql) {
        Statement::Delete {
            from: FromTable::WithoutKeyword(from),
            ..
        } => {
            assert_eq!(
                TableFactor::Table {
                    name: ObjectName(vec![Ident::with_quote('"', "table")]),
                    alias: None,
                    args: None,
                    with_hints: vec![],
                    version: None,
                    partitions: vec![],
                },
                from[0].relation
            );
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_view_with_options() {
    let sql = concat!(
        "CREATE VIEW myproject.mydataset.newview ",
        r#"(name, age OPTIONS(description = "field age")) "#,
        r#"OPTIONS(expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 48 HOUR), "#,
        r#"friendly_name = "newview", description = "a view that expires in 2 days", labels = [("org_unit", "development")]) "#,
        "AS SELECT column_1, column_2, column_3 FROM myproject.mydataset.mytable",
    );
    match bigquery().verified_stmt(sql) {
        Statement::CreateView {
            name,
            query,
            options,
            columns,
            ..
        } => {
            assert_eq!(
                name,
                ObjectName(vec![
                    "myproject".into(),
                    "mydataset".into(),
                    "newview".into()
                ])
            );
            assert_eq!(
                vec![
                    ViewColumnDef {
                        name: Ident::new("name"),
                        options: None,
                    },
                    ViewColumnDef {
                        name: Ident::new("age"),
                        options: Some(vec![SqlOption {
                            name: Ident::new("description"),
                            value: Expr::Value(Value::DoubleQuotedString("field age".to_string())),
                        }])
                    },
                ],
                columns
            );
            assert_eq!(
                "SELECT column_1, column_2, column_3 FROM myproject.mydataset.mytable",
                query.to_string()
            );
            assert_eq!(
                r#"OPTIONS(expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 48 HOUR), friendly_name = "newview", description = "a view that expires in 2 days", labels = [("org_unit", "development")])"#,
                options.to_string()
            );
            let CreateTableOptions::Options(options) = options else {
                unreachable!()
            };
            assert_eq!(
                &SqlOption {
                    name: Ident::new("description"),
                    value: Expr::Value(Value::DoubleQuotedString(
                        "a view that expires in 2 days".to_string()
                    )),
                },
                &options[2],
            );
        }
        _ => unreachable!(),
    }
}
#[test]
fn parse_create_view_if_not_exists() {
    let sql = "CREATE VIEW IF NOT EXISTS mydataset.newview AS SELECT foo FROM bar";
    match bigquery().verified_stmt(sql) {
        Statement::CreateView {
            name,
            columns,
            query,
            or_replace,
            materialized,
            options,
            cluster_by,
            with_no_schema_binding: late_binding,
            if_not_exists,
            temporary,
        } => {
            assert_eq!("mydataset.newview", name.to_string());
            assert_eq!(Vec::<ViewColumnDef>::new(), columns);
            assert_eq!("SELECT foo FROM bar", query.to_string());
            assert!(!materialized);
            assert!(!or_replace);
            assert_eq!(options, CreateTableOptions::None);
            assert_eq!(cluster_by, vec![]);
            assert!(!late_binding);
            assert!(if_not_exists);
            assert!(!temporary);
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_table_with_options() {
    let sql = concat!(
        "CREATE TABLE mydataset.newtable ",
        r#"(x INT64 NOT NULL OPTIONS(description = "field x"), "#,
        r#"y BOOL OPTIONS(description = "field y")) "#,
        "PARTITION BY _PARTITIONDATE ",
        "CLUSTER BY userid, age ",
        r#"OPTIONS(partition_expiration_days = 1, description = "table option description")"#
    );
    match bigquery().verified_stmt(sql) {
        Statement::CreateTable {
            name,
            columns,
            partition_by,
            cluster_by,
            options,
            ..
        } => {
            assert_eq!(
                name,
                ObjectName(vec!["mydataset".into(), "newtable".into()])
            );
            assert_eq!(
                vec![
                    ColumnDef {
                        name: Ident::new("x"),
                        data_type: DataType::Int64,
                        collation: None,
                        options: vec![
                            ColumnOptionDef {
                                name: None,
                                option: ColumnOption::NotNull,
                            },
                            ColumnOptionDef {
                                name: None,
                                option: ColumnOption::Options(vec![SqlOption {
                                    name: Ident::new("description"),
                                    value: Expr::Value(Value::DoubleQuotedString(
                                        "field x".to_string()
                                    )),
                                },])
                            },
                        ]
                    },
                    ColumnDef {
                        name: Ident::new("y"),
                        data_type: DataType::Bool,
                        collation: None,
                        options: vec![ColumnOptionDef {
                            name: None,
                            option: ColumnOption::Options(vec![SqlOption {
                                name: Ident::new("description"),
                                value: Expr::Value(Value::DoubleQuotedString(
                                    "field y".to_string()
                                )),
                            },])
                        }]
                    },
                ],
                columns
            );
            assert_eq!(
                (
                    Some(Box::new(Expr::Identifier(Ident::new("_PARTITIONDATE")))),
                    Some(vec![Ident::new("userid"), Ident::new("age"),]),
                    Some(vec![
                        SqlOption {
                            name: Ident::new("partition_expiration_days"),
                            value: Expr::Value(number("1")),
                        },
                        SqlOption {
                            name: Ident::new("description"),
                            value: Expr::Value(Value::DoubleQuotedString(
                                "table option description".to_string()
                            )),
                        },
                    ])
                ),
                (partition_by, cluster_by, options)
            )
        }
        _ => unreachable!(),
    }

    let sql = concat!(
        "CREATE TABLE mydataset.newtable ",
        r#"(x INT64 NOT NULL OPTIONS(description = "field x"), "#,
        r#"y BOOL OPTIONS(description = "field y")) "#,
        "CLUSTER BY userid ",
        r#"OPTIONS(partition_expiration_days = 1, "#,
        r#"description = "table option description")"#
    );
    bigquery().verified_stmt(sql);
}

#[test]
fn parse_nested_data_types() {
    let sql = "CREATE TABLE table (x STRUCT<a ARRAY<INT64>, b BYTES(42)>, y ARRAY<STRUCT<INT64>>)";
    match bigquery().one_statement_parses_to(sql, sql) {
        Statement::CreateTable { name, columns, .. } => {
            assert_eq!(name, ObjectName(vec!["table".into()]));
            assert_eq!(
                columns,
                vec![
                    ColumnDef {
                        name: Ident::new("x"),
                        data_type: DataType::Struct(vec![
                            StructField {
                                field_name: Some("a".into()),
                                field_type: DataType::Array(ArrayElemTypeDef::AngleBracket(
                                    Box::new(DataType::Int64,)
                                ))
                            },
                            StructField {
                                field_name: Some("b".into()),
                                field_type: DataType::Bytes(Some(42))
                            },
                        ]),
                        collation: None,
                        options: vec![],
                    },
                    ColumnDef {
                        name: Ident::new("y"),
                        data_type: DataType::Array(ArrayElemTypeDef::AngleBracket(Box::new(
                            DataType::Struct(vec![StructField {
                                field_name: None,
                                field_type: DataType::Int64,
                            }]),
                        ))),
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
fn parse_invalid_brackets() {
    let sql = "SELECT STRUCT<INT64>>(NULL)";
    assert_eq!(
        bigquery().parse_sql_statements(sql).unwrap_err(),
        ParserError::ParserError("unmatched > in STRUCT literal".to_string())
    );

    let sql = "SELECT STRUCT<STRUCT<INT64>>>(NULL)";
    assert_eq!(
        bigquery().parse_sql_statements(sql).unwrap_err(),
        ParserError::ParserError("Expected (, found: >".to_string())
    );

    let sql = "CREATE TABLE table (x STRUCT<STRUCT<INT64>>>)";
    assert_eq!(
        bigquery().parse_sql_statements(sql).unwrap_err(),
        ParserError::ParserError(
            "Expected ',' or ')' after column definition, found: >".to_string()
        )
    );
}

#[test]
fn parse_tuple_struct_literal() {
    // tuple syntax: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#tuple_syntax
    // syntax: (expr1, expr2 [, ... ])
    let sql = "SELECT (1, 2, 3), (1, 1.0, '123', true)";
    let select = bigquery().verified_only_select(sql);
    assert_eq!(2, select.projection.len());
    assert_eq!(
        &Expr::Tuple(vec![
            Expr::Value(number("1")),
            Expr::Value(number("2")),
            Expr::Value(number("3")),
        ]),
        expr_from_projection(&select.projection[0])
    );
    assert_eq!(
        &Expr::Tuple(vec![
            Expr::Value(number("1")),
            Expr::Value(number("1.0")),
            Expr::Value(Value::SingleQuotedString("123".to_string())),
            Expr::Value(Value::Boolean(true))
        ]),
        expr_from_projection(&select.projection[1])
    );
}

#[test]
fn parse_typeless_struct_syntax() {
    // typeless struct syntax https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#typeless_struct_syntax
    // syntax: STRUCT( expr1 [AS field_name] [, ... ])
    let sql = "SELECT STRUCT(1, 2, 3), STRUCT('abc'), STRUCT(1, t.str_col), STRUCT(1 AS a, 'abc' AS b), STRUCT(str_col AS abc)";
    let select = bigquery().verified_only_select(sql);
    assert_eq!(5, select.projection.len());
    assert_eq!(
        &Expr::Struct {
            values: vec![
                Expr::Value(number("1")),
                Expr::Value(number("2")),
                Expr::Value(number("3")),
            ],
            fields: Default::default()
        },
        expr_from_projection(&select.projection[0])
    );

    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::Value(Value::SingleQuotedString("abc".to_string())),],
            fields: Default::default()
        },
        expr_from_projection(&select.projection[1])
    );
    assert_eq!(
        &Expr::Struct {
            values: vec![
                Expr::Value(number("1")),
                Expr::CompoundIdentifier(vec![Ident::from("t"), Ident::from("str_col")]),
            ],
            fields: Default::default()
        },
        expr_from_projection(&select.projection[2])
    );
    assert_eq!(
        &Expr::Struct {
            values: vec![
                Expr::Named {
                    expr: Expr::Value(number("1")).into(),
                    name: Ident::from("a")
                },
                Expr::Named {
                    expr: Expr::Value(Value::SingleQuotedString("abc".to_string())).into(),
                    name: Ident::from("b")
                },
            ],
            fields: Default::default()
        },
        expr_from_projection(&select.projection[3])
    );
    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::Named {
                expr: Expr::Identifier(Ident::from("str_col")).into(),
                name: Ident::from("abc")
            }],
            fields: Default::default()
        },
        expr_from_projection(&select.projection[4])
    );
}

#[test]
fn parse_typed_struct_syntax() {
    // typed struct syntax https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#typed_struct_syntax
    // syntax: STRUCT<[field_name] field_type, ...>( expr1 [, ... ])

    let sql = r#"SELECT STRUCT<INT64>(5), STRUCT<x INT64, y STRING>(1, t.str_col), STRUCT<arr ARRAY<FLOAT64>, str STRUCT<BOOL>>(nested_col)"#;
    let select = bigquery().verified_only_select(sql);
    assert_eq!(3, select.projection.len());
    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::Value(number("5")),],
            fields: vec![StructField {
                field_name: None,
                field_type: DataType::Int64,
            }]
        },
        expr_from_projection(&select.projection[0])
    );
    assert_eq!(
        &Expr::Struct {
            values: vec![
                Expr::Value(number("1")),
                Expr::CompoundIdentifier(vec![
                    Ident {
                        value: "t".into(),
                        quote_style: None,
                    },
                    Ident {
                        value: "str_col".into(),
                        quote_style: None,
                    },
                ]),
            ],
            fields: vec![
                StructField {
                    field_name: Some(Ident {
                        value: "x".into(),
                        quote_style: None,
                    }),
                    field_type: DataType::Int64
                },
                StructField {
                    field_name: Some(Ident {
                        value: "y".into(),
                        quote_style: None,
                    }),
                    field_type: DataType::String(None)
                },
            ]
        },
        expr_from_projection(&select.projection[1])
    );
    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::Identifier(Ident {
                value: "nested_col".into(),
                quote_style: None,
            }),],
            fields: vec![
                StructField {
                    field_name: Some("arr".into()),
                    field_type: DataType::Array(ArrayElemTypeDef::AngleBracket(Box::new(
                        DataType::Float64
                    )))
                },
                StructField {
                    field_name: Some("str".into()),
                    field_type: DataType::Struct(vec![StructField {
                        field_name: None,
                        field_type: DataType::Bool
                    }])
                },
            ]
        },
        expr_from_projection(&select.projection[2])
    );

    let sql = r#"SELECT STRUCT<x STRUCT, y ARRAY<STRUCT>>(nested_col)"#;
    let select = bigquery().verified_only_select(sql);
    assert_eq!(1, select.projection.len());
    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::Identifier(Ident {
                value: "nested_col".into(),
                quote_style: None,
            }),],
            fields: vec![
                StructField {
                    field_name: Some("x".into()),
                    field_type: DataType::Struct(Default::default())
                },
                StructField {
                    field_name: Some("y".into()),
                    field_type: DataType::Array(ArrayElemTypeDef::AngleBracket(Box::new(
                        DataType::Struct(Default::default())
                    )))
                },
            ]
        },
        expr_from_projection(&select.projection[0])
    );

    let sql = r#"SELECT STRUCT<BOOL>(true), STRUCT<BYTES(42)>(B'abc')"#;
    let select = bigquery().verified_only_select(sql);
    assert_eq!(2, select.projection.len());
    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::Value(Value::Boolean(true)),],
            fields: vec![StructField {
                field_name: None,
                field_type: DataType::Bool
            }]
        },
        expr_from_projection(&select.projection[0])
    );
    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::Value(Value::SingleQuotedByteStringLiteral(
                "abc".into()
            )),],
            fields: vec![StructField {
                field_name: None,
                field_type: DataType::Bytes(Some(42))
            }]
        },
        expr_from_projection(&select.projection[1])
    );

    let sql = r#"SELECT STRUCT<DATE>("2011-05-05"), STRUCT<DATETIME>(DATETIME '1999-01-01 01:23:34.45'), STRUCT<FLOAT64>(5.0), STRUCT<INT64>(1)"#;
    let select = bigquery().verified_only_select(sql);
    assert_eq!(4, select.projection.len());
    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::Value(Value::DoubleQuotedString(
                "2011-05-05".to_string()
            )),],
            fields: vec![StructField {
                field_name: None,
                field_type: DataType::Date
            }]
        },
        expr_from_projection(&select.projection[0])
    );
    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::TypedString {
                data_type: DataType::Datetime(None),
                value: "1999-01-01 01:23:34.45".to_string()
            },],
            fields: vec![StructField {
                field_name: None,
                field_type: DataType::Datetime(None)
            }]
        },
        expr_from_projection(&select.projection[1])
    );
    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::Value(number("5.0")),],
            fields: vec![StructField {
                field_name: None,
                field_type: DataType::Float64
            }]
        },
        expr_from_projection(&select.projection[2])
    );
    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::Value(number("1")),],
            fields: vec![StructField {
                field_name: None,
                field_type: DataType::Int64
            }]
        },
        expr_from_projection(&select.projection[3])
    );

    let sql = r#"SELECT STRUCT<INTERVAL>(INTERVAL '1-2 3 4:5:6.789999'), STRUCT<JSON>(JSON '{"class" : {"students" : [{"name" : "Jane"}]}}')"#;
    let select = bigquery().verified_only_select(sql);
    assert_eq!(2, select.projection.len());
    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::Interval(ast::Interval {
                value: Box::new(Expr::Value(Value::SingleQuotedString(
                    "1-2 3 4:5:6.789999".to_string()
                ))),
                leading_field: None,
                leading_precision: None,
                last_field: None,
                fractional_seconds_precision: None
            }),],
            fields: vec![StructField {
                field_name: None,
                field_type: DataType::Interval
            }]
        },
        expr_from_projection(&select.projection[0])
    );
    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::TypedString {
                data_type: DataType::JSON,
                value: r#"{"class" : {"students" : [{"name" : "Jane"}]}}"#.to_string()
            },],
            fields: vec![StructField {
                field_name: None,
                field_type: DataType::JSON
            }]
        },
        expr_from_projection(&select.projection[1])
    );

    let sql = r#"SELECT STRUCT<STRING(42)>("foo"), STRUCT<TIMESTAMP>(TIMESTAMP '2008-12-25 15:30:00 America/Los_Angeles'), STRUCT<TIME>(TIME '15:30:00')"#;
    let select = bigquery().verified_only_select(sql);
    assert_eq!(3, select.projection.len());
    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::Value(Value::DoubleQuotedString("foo".to_string())),],
            fields: vec![StructField {
                field_name: None,
                field_type: DataType::String(Some(42))
            }]
        },
        expr_from_projection(&select.projection[0])
    );
    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::TypedString {
                data_type: DataType::Timestamp(None, TimezoneInfo::None),
                value: "2008-12-25 15:30:00 America/Los_Angeles".to_string()
            },],
            fields: vec![StructField {
                field_name: None,
                field_type: DataType::Timestamp(None, TimezoneInfo::None)
            }]
        },
        expr_from_projection(&select.projection[1])
    );

    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::TypedString {
                data_type: DataType::Time(None, TimezoneInfo::None),
                value: "15:30:00".to_string()
            },],
            fields: vec![StructField {
                field_name: None,
                field_type: DataType::Time(None, TimezoneInfo::None)
            }]
        },
        expr_from_projection(&select.projection[2])
    );

    let sql = r#"SELECT STRUCT<NUMERIC>(NUMERIC '1'), STRUCT<BIGNUMERIC>(BIGNUMERIC '1')"#;
    let select = bigquery().verified_only_select(sql);
    assert_eq!(2, select.projection.len());
    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::TypedString {
                data_type: DataType::Numeric(ExactNumberInfo::None),
                value: "1".to_string()
            },],
            fields: vec![StructField {
                field_name: None,
                field_type: DataType::Numeric(ExactNumberInfo::None)
            }]
        },
        expr_from_projection(&select.projection[0])
    );
    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::TypedString {
                data_type: DataType::BigNumeric(ExactNumberInfo::None),
                value: "1".to_string()
            },],
            fields: vec![StructField {
                field_name: None,
                field_type: DataType::BigNumeric(ExactNumberInfo::None)
            }]
        },
        expr_from_projection(&select.projection[1])
    );
}

#[test]
fn parse_typed_struct_with_field_name() {
    let sql = r#"SELECT STRUCT<x INT64>(5), STRUCT<y STRING>("foo")"#;
    let select = bigquery().verified_only_select(sql);
    assert_eq!(2, select.projection.len());
    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::Value(number("5")),],
            fields: vec![StructField {
                field_name: Some(Ident::from("x")),
                field_type: DataType::Int64
            }]
        },
        expr_from_projection(&select.projection[0])
    );
    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::Value(Value::DoubleQuotedString("foo".to_string())),],
            fields: vec![StructField {
                field_name: Some(Ident::from("y")),
                field_type: DataType::String(None)
            }]
        },
        expr_from_projection(&select.projection[1])
    );

    let sql = r#"SELECT STRUCT<x INT64, y INT64>(5, 5)"#;
    let select = bigquery().verified_only_select(sql);
    assert_eq!(1, select.projection.len());
    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::Value(number("5")), Expr::Value(number("5")),],
            fields: vec![
                StructField {
                    field_name: Some(Ident::from("x")),
                    field_type: DataType::Int64
                },
                StructField {
                    field_name: Some(Ident::from("y")),
                    field_type: DataType::Int64
                }
            ]
        },
        expr_from_projection(&select.projection[0])
    );
}

#[test]
fn parse_table_identifiers() {
    /// Parses a table identifier ident and verifies that re-serializing the
    /// parsed identifier produces the original ident string.
    ///
    /// In some cases, re-serializing the result of the parsed ident is not
    /// expected to produce the original ident string. canonical is provided
    /// instead as the canonical representation of the identifier for comparison.
    /// For example, re-serializing the result of ident `foo.bar` produces
    /// the equivalent canonical representation `foo`.`bar`
    fn test_table_ident(ident: &str, canonical: Option<&str>, expected: Vec<Ident>) {
        let sql = format!("SELECT 1 FROM {ident}");
        let canonical = canonical.map(|ident| format!("SELECT 1 FROM {ident}"));

        let select = if let Some(canonical) = canonical {
            bigquery().verified_only_select_with_canonical(&sql, canonical.deref())
        } else {
            bigquery().verified_only_select(&sql)
        };

        assert_eq!(
            select.from,
            vec![TableWithJoins {
                relation: TableFactor::Table {
                    name: ObjectName(expected),
                    alias: None,
                    args: None,
                    with_hints: vec![],
                    version: None,
                    partitions: vec![],
                },
                joins: vec![]
            },]
        );
    }

    fn test_table_ident_err(ident: &str) {
        let sql = format!("SELECT 1 FROM {ident}");
        assert!(bigquery().parse_sql_statements(&sql).is_err());
    }

    test_table_ident("`spa ce`", None, vec![Ident::with_quote('`', "spa ce")]);

    test_table_ident(
        "`!@#$%^&*()-=_+`",
        None,
        vec![Ident::with_quote('`', "!@#$%^&*()-=_+")],
    );

    test_table_ident(
        "_5abc.dataField",
        None,
        vec![Ident::new("_5abc"), Ident::new("dataField")],
    );
    test_table_ident(
        "`5abc`.dataField",
        None,
        vec![Ident::with_quote('`', "5abc"), Ident::new("dataField")],
    );

    test_table_ident_err("5abc.dataField");

    test_table_ident(
        "abc5.dataField",
        None,
        vec![Ident::new("abc5"), Ident::new("dataField")],
    );

    test_table_ident_err("abc5!.dataField");

    test_table_ident(
        "`GROUP`.dataField",
        None,
        vec![Ident::with_quote('`', "GROUP"), Ident::new("dataField")],
    );

    // TODO: this should be error
    // test_table_ident_err("GROUP.dataField");

    test_table_ident(
        "abc5.GROUP",
        None,
        vec![Ident::new("abc5"), Ident::new("GROUP")],
    );

    test_table_ident(
        "`foo.bar.baz`",
        Some("`foo`.`bar`.`baz`"),
        vec![
            Ident::with_quote('`', "foo"),
            Ident::with_quote('`', "bar"),
            Ident::with_quote('`', "baz"),
        ],
    );

    test_table_ident(
        "`foo.bar`.`baz`",
        Some("`foo`.`bar`.`baz`"),
        vec![
            Ident::with_quote('`', "foo"),
            Ident::with_quote('`', "bar"),
            Ident::with_quote('`', "baz"),
        ],
    );

    test_table_ident(
        "`foo`.`bar.baz`",
        Some("`foo`.`bar`.`baz`"),
        vec![
            Ident::with_quote('`', "foo"),
            Ident::with_quote('`', "bar"),
            Ident::with_quote('`', "baz"),
        ],
    );

    test_table_ident(
        "`foo`.`bar`.`baz`",
        Some("`foo`.`bar`.`baz`"),
        vec![
            Ident::with_quote('`', "foo"),
            Ident::with_quote('`', "bar"),
            Ident::with_quote('`', "baz"),
        ],
    );

    test_table_ident(
        "`5abc.dataField`",
        Some("`5abc`.`dataField`"),
        vec![
            Ident::with_quote('`', "5abc"),
            Ident::with_quote('`', "dataField"),
        ],
    );

    test_table_ident(
        "`_5abc.da-sh-es`",
        Some("`_5abc`.`da-sh-es`"),
        vec![
            Ident::with_quote('`', "_5abc"),
            Ident::with_quote('`', "da-sh-es"),
        ],
    );

    test_table_ident(
        "foo-bar.baz-123",
        Some("foo-bar.baz-123"),
        vec![Ident::new("foo-bar"), Ident::new("baz-123")],
    );

    test_table_ident_err("foo-`bar`");
    test_table_ident_err("`foo`-bar");
    test_table_ident_err("foo-123a");
    test_table_ident_err("foo - bar");
    test_table_ident_err("123-bar");
    test_table_ident_err("bar-");
}

#[test]
fn parse_hyphenated_table_identifiers() {
    bigquery().one_statement_parses_to(
        "select * from foo-bar f join baz-qux b on f.id = b.id",
        "SELECT * FROM foo-bar AS f JOIN baz-qux AS b ON f.id = b.id",
    );

    assert_eq!(
        bigquery()
            .verified_only_select_with_canonical(
                "SELECT foo-bar.x FROM t",
                "SELECT foo - bar.x FROM t"
            )
            .projection[0],
        SelectItem::UnnamedExpr(Expr::BinaryOp {
            left: Box::new(Expr::Identifier(Ident::new("foo"))),
            op: BinaryOperator::Minus,
            right: Box::new(Expr::CompoundIdentifier(vec![
                Ident::new("bar"),
                Ident::new("x")
            ]))
        })
    );

    let error_sql = "select foo-bar.* from foo-bar";
    assert!(bigquery().parse_sql_statements(error_sql).is_err());
}

#[test]
fn parse_table_time_travel() {
    let version = "2023-08-18 23:08:18".to_string();
    let sql = format!("SELECT 1 FROM t1 FOR SYSTEM_TIME AS OF '{version}'");
    let select = bigquery().verified_only_select(&sql);
    assert_eq!(
        select.from,
        vec![TableWithJoins {
            relation: TableFactor::Table {
                name: ObjectName(vec![Ident::new("t1")]),
                alias: None,
                args: None,
                with_hints: vec![],
                version: Some(TableVersion::ForSystemTimeAsOf(Expr::Value(
                    Value::SingleQuotedString(version)
                ))),
                partitions: vec![],
            },
            joins: vec![]
        },]
    );

    let sql = "SELECT 1 FROM t1 FOR SYSTEM TIME AS OF 'some_timestamp'".to_string();
    assert!(bigquery().parse_sql_statements(&sql).is_err());
}

#[test]
fn parse_join_constraint_unnest_alias() {
    assert_eq!(
        only(
            bigquery()
                .verified_only_select("SELECT * FROM t1 JOIN UNNEST(t1.a) AS f ON c1 = c2")
                .from
        )
        .joins,
        vec![Join {
            relation: TableFactor::UNNEST {
                alias: table_alias("f"),
                array_exprs: vec![Expr::CompoundIdentifier(vec![
                    Ident::new("t1"),
                    Ident::new("a")
                ])],
                with_offset: false,
                with_offset_alias: None
            },
            join_operator: JoinOperator::Inner(JoinConstraint::On(Expr::BinaryOp {
                left: Box::new(Expr::Identifier("c1".into())),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::Identifier("c2".into())),
            })),
        }]
    );
}

#[test]
fn parse_trailing_comma() {
    for (sql, canonical) in [
        ("SELECT a,", "SELECT a"),
        ("SELECT 1,", "SELECT 1"),
        ("SELECT 1,2,", "SELECT 1, 2"),
        ("SELECT a, b,", "SELECT a, b"),
        ("SELECT a, b AS c,", "SELECT a, b AS c"),
        ("SELECT a, b AS c, FROM t", "SELECT a, b AS c FROM t"),
        ("SELECT a, b, FROM t", "SELECT a, b FROM t"),
        ("SELECT a, b, LIMIT 1", "SELECT a, b LIMIT 1"),
        ("SELECT a, (SELECT 1, )", "SELECT a, (SELECT 1)"),
    ] {
        bigquery().one_statement_parses_to(sql, canonical);
    }
}

#[test]
fn parse_cast_type() {
    let sql = r"SELECT SAFE_CAST(1 AS INT64)";
    bigquery_and_generic().verified_only_select(sql);
}

#[test]
fn parse_cast_date_format() {
    let sql =
        r"SELECT CAST(date_valid_from AS DATE FORMAT 'YYYY-MM-DD') AS date_valid_from FROM foo";
    bigquery_and_generic().verified_only_select(sql);
}

#[test]
fn parse_cast_time_format() {
    let sql = r"SELECT CAST(TIME '21:30:00' AS STRING FORMAT 'PM') AS date_time_to_string";
    bigquery_and_generic().verified_only_select(sql);
}

#[test]
fn parse_cast_timestamp_format_tz() {
    let sql = r"SELECT CAST(TIMESTAMP '2008-12-25 00:00:00+00:00' AS STRING FORMAT 'TZH' AT TIME ZONE 'Asia/Kolkata') AS date_time_to_string";
    bigquery_and_generic().verified_only_select(sql);
}

#[test]
fn parse_cast_string_to_bytes_format() {
    let sql = r"SELECT CAST('Hello' AS BYTES FORMAT 'ASCII') AS string_to_bytes";
    bigquery_and_generic().verified_only_select(sql);
}

#[test]
fn parse_cast_bytes_to_string_format() {
    let sql = r"SELECT CAST(B'\x48\x65\x6c\x6c\x6f' AS STRING FORMAT 'ASCII') AS bytes_to_string";
    bigquery_and_generic().verified_only_select(sql);
}

#[test]
fn parse_like() {
    fn chk(negated: bool) {
        let sql = &format!(
            "SELECT * FROM customers WHERE name {}LIKE '%a'",
            if negated { "NOT " } else { "" }
        );
        let select = bigquery().verified_only_select(sql);
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
        let select = bigquery().verified_only_select(sql);
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
        let select = bigquery().verified_only_select(sql);
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
        let select = bigquery().verified_only_select(sql);
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
        let select = bigquery().verified_only_select(sql);
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
        let select = bigquery().verified_only_select(sql);
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

#[test]
fn parse_array_agg_func() {
    for sql in [
        "SELECT ARRAY_AGG(x ORDER BY x) AS a FROM T",
        "SELECT ARRAY_AGG(x ORDER BY x LIMIT 2) FROM tbl",
        "SELECT ARRAY_AGG(DISTINCT x ORDER BY x LIMIT 2) FROM tbl",
    ] {
        bigquery().verified_stmt(sql);
    }
}

#[test]
fn test_select_wildcard_with_except() {
    let select = bigquery_and_generic().verified_only_select("SELECT * EXCEPT (col_a) FROM data");
    let expected = SelectItem::Wildcard(WildcardAdditionalOptions {
        opt_except: Some(ExceptSelectItem {
            first_element: Ident::new("col_a"),
            additional_elements: vec![],
        }),
        ..Default::default()
    });
    assert_eq!(expected, select.projection[0]);

    let select = bigquery_and_generic()
        .verified_only_select("SELECT * EXCEPT (department_id, employee_id) FROM employee_table");
    let expected = SelectItem::Wildcard(WildcardAdditionalOptions {
        opt_except: Some(ExceptSelectItem {
            first_element: Ident::new("department_id"),
            additional_elements: vec![Ident::new("employee_id")],
        }),
        ..Default::default()
    });
    assert_eq!(expected, select.projection[0]);

    assert_eq!(
        bigquery_and_generic()
            .parse_sql_statements("SELECT * EXCEPT () FROM employee_table")
            .unwrap_err()
            .to_string(),
        "sql parser error: Expected identifier, found: )"
    );
}

#[test]
fn test_select_wildcard_with_replace() {
    let select = bigquery_and_generic()
        .verified_only_select(r#"SELECT * REPLACE ('widget' AS item_name) FROM orders"#);
    let expected = SelectItem::Wildcard(WildcardAdditionalOptions {
        opt_replace: Some(ReplaceSelectItem {
            items: vec![Box::new(ReplaceSelectElement {
                expr: Expr::Value(Value::SingleQuotedString("widget".to_owned())),
                column_name: Ident::new("item_name"),
                as_keyword: true,
            })],
        }),
        ..Default::default()
    });
    assert_eq!(expected, select.projection[0]);

    let select = bigquery_and_generic().verified_only_select(
        r#"SELECT * REPLACE (quantity / 2 AS quantity, 3 AS order_id) FROM orders"#,
    );
    let expected = SelectItem::Wildcard(WildcardAdditionalOptions {
        opt_replace: Some(ReplaceSelectItem {
            items: vec![
                Box::new(ReplaceSelectElement {
                    expr: Expr::BinaryOp {
                        left: Box::new(Expr::Identifier(Ident::new("quantity"))),
                        op: BinaryOperator::Divide,
                        right: Box::new(Expr::Value(number("2"))),
                    },
                    column_name: Ident::new("quantity"),
                    as_keyword: true,
                }),
                Box::new(ReplaceSelectElement {
                    expr: Expr::Value(number("3")),
                    column_name: Ident::new("order_id"),
                    as_keyword: true,
                }),
            ],
        }),
        ..Default::default()
    });
    assert_eq!(expected, select.projection[0]);
}

#[test]
fn parse_big_query_declare() {
    for (sql, expected_names, expected_data_type, expected_assigned_expr) in [
        (
            "DECLARE x INT64",
            vec![Ident::new("x")],
            Some(DataType::Int64),
            None,
        ),
        (
            "DECLARE x INT64 DEFAULT 42",
            vec![Ident::new("x")],
            Some(DataType::Int64),
            Some(DeclareAssignment::Default(Box::new(Expr::Value(number(
                "42",
            ))))),
        ),
        (
            "DECLARE x, y, z INT64 DEFAULT 42",
            vec![Ident::new("x"), Ident::new("y"), Ident::new("z")],
            Some(DataType::Int64),
            Some(DeclareAssignment::Default(Box::new(Expr::Value(number(
                "42",
            ))))),
        ),
        (
            "DECLARE x DEFAULT 42",
            vec![Ident::new("x")],
            None,
            Some(DeclareAssignment::Default(Box::new(Expr::Value(number(
                "42",
            ))))),
        ),
    ] {
        match bigquery().verified_stmt(sql) {
            Statement::Declare { mut stmts } => {
                assert_eq!(1, stmts.len());
                let Declare {
                    names,
                    data_type,
                    assignment: assigned_expr,
                    ..
                } = stmts.swap_remove(0);
                assert_eq!(expected_names, names);
                assert_eq!(expected_data_type, data_type);
                assert_eq!(expected_assigned_expr, assigned_expr);
            }
            _ => unreachable!(),
        }
    }

    let error_sql = "DECLARE x";
    assert_eq!(
        ParserError::ParserError("Expected a data type name, found: EOF".to_owned()),
        bigquery().parse_sql_statements(error_sql).unwrap_err()
    );

    let error_sql = "DECLARE x 42";
    assert_eq!(
        ParserError::ParserError("Expected a data type name, found: 42".to_owned()),
        bigquery().parse_sql_statements(error_sql).unwrap_err()
    );
}

fn bigquery() -> TestedDialects {
    TestedDialects {
        dialects: vec![Box::new(BigQueryDialect {})],
        options: None,
    }
}

fn bigquery_and_generic() -> TestedDialects {
    TestedDialects {
        dialects: vec![Box::new(BigQueryDialect {}), Box::new(GenericDialect {})],
        options: None,
    }
}

#[test]
fn parse_map_access_offset() {
    let sql = "SELECT d[offset(0)]";
    let _select = bigquery().verified_only_select(sql);
    assert_eq!(
        _select.projection[0],
        SelectItem::UnnamedExpr(Expr::MapAccess {
            column: Box::new(Expr::Identifier(Ident {
                value: "d".to_string(),
                quote_style: None,
            })),
            keys: vec![Expr::Function(Function {
                name: ObjectName(vec!["offset".into()]),
                args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
                    number("0")
                ))),],
                null_treatment: None,
                filter: None,
                over: None,
                distinct: false,
                special: false,
                order_by: vec![],
            })],
        })
    );

    // test other operators
    for sql in [
        "SELECT d[SAFE_OFFSET(0)]",
        "SELECT d[ORDINAL(0)]",
        "SELECT d[SAFE_ORDINAL(0)]",
    ] {
        bigquery().verified_only_select(sql);
    }
}

#[test]
fn test_bigquery_trim() {
    let real_sql = r#"SELECT customer_id, TRIM(item_price_id, '"', "a") AS item_price_id FROM models_staging.subscriptions"#;
    assert_eq!(bigquery().verified_stmt(real_sql).to_string(), real_sql);

    let sql_only_select = "SELECT TRIM('xyz', 'a')";
    let select = bigquery().verified_only_select(sql_only_select);
    assert_eq!(
        &Expr::Trim {
            expr: Box::new(Expr::Value(Value::SingleQuotedString("xyz".to_owned()))),
            trim_where: None,
            trim_what: None,
            trim_characters: Some(vec![Expr::Value(Value::SingleQuotedString("a".to_owned()))]),
        },
        expr_from_projection(only(&select.projection))
    );

    // missing comma separation
    let error_sql = "SELECT TRIM('xyz' 'a')";
    assert_eq!(
        ParserError::ParserError("Expected ), found: 'a'".to_owned()),
        bigquery().parse_sql_statements(error_sql).unwrap_err()
    );
}

#[test]
fn test_select_as_struct() {
    bigquery().verified_only_select("SELECT * FROM (SELECT AS VALUE STRUCT(123 AS a, false AS b))");
    let select = bigquery().verified_only_select("SELECT AS STRUCT 1 AS a, 2 AS b");
    assert_eq!(Some(ValueTableMode::AsStruct), select.value_table_mode);
}

#[test]
fn test_select_as_value() {
    bigquery().verified_only_select(
        "SELECT * FROM (SELECT AS VALUE STRUCT(5 AS star_rating, false AS up_down_rating))",
    );
    let select = bigquery().verified_only_select("SELECT AS VALUE STRUCT(1 AS a, 2 AS b) AS xyz");
    assert_eq!(Some(ValueTableMode::AsValue), select.value_table_mode);
}

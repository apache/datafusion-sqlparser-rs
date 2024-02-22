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
use sqlparser::tokenizer::*;
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
fn parse_nested_data_types() {
    let sql = "CREATE TABLE table (x STRUCT<a ARRAY<INT64>, b BYTES(42)>, y ARRAY<STRUCT<INT64>>)";
    match bigquery().one_statement_parses_to(sql, sql) {
        Statement::CreateTable { name, columns, .. } => {
            assert_eq!(name, ObjectName(vec!["table".into()]));
            assert_eq!(
                columns,
                vec![
                    ColumnDef {
                        name: Ident::new("x").empty_span(),
                        data_type: DataType::Struct(vec![
                            StructField {
                                field_name: Some(Ident::new("a").empty_span()),
                                field_type: DataType::Array(ArrayElemTypeDef::AngleBracket(
                                    Box::new(DataType::Int64)
                                )),
                            },
                            StructField {
                                field_name: Some(Ident::new("b").empty_span()),
                                field_type: DataType::Bytes(Some(42)),
                            },
                        ]),
                        collation: None,
                        codec: None,
                        options: vec![],
                    },
                    ColumnDef {
                        name: Ident::new("y").empty_span(),
                        data_type: DataType::Array(ArrayElemTypeDef::AngleBracket(Box::new(
                            DataType::Struct(vec![StructField {
                                field_name: None,
                                field_type: DataType::Int64,
                            }]),
                        ))),
                        collation: None,
                        codec: None,
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
        ParserError::ParserError(
            "Expected (, found: >\nNear `SELECT STRUCT<STRUCT<INT64>>`".to_string()
        )
    );

    let sql = "CREATE TABLE table (x STRUCT<STRUCT<INT64>>>)";
    assert_eq!(
        bigquery().parse_sql_statements(sql).unwrap_err(),
        ParserError::ParserError(
            "Expected ',' or ')' after column definition, found: >\nNear ` (x STRUCT<STRUCT<INT64>>`".to_string()
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
            Expr::Value(Value::Boolean(true)),
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
            fields: Default::default(),
        },
        expr_from_projection(&select.projection[0])
    );

    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::Value(Value::SingleQuotedString("abc".to_string()))],
            fields: Default::default(),
        },
        expr_from_projection(&select.projection[1])
    );
    assert_eq!(
        &Expr::Struct {
            values: vec![
                Expr::Value(number("1")),
                Expr::CompoundIdentifier(
                    vec![Ident::from("t"), Ident::from("str_col")].empty_span()
                ),
            ],
            fields: Default::default(),
        },
        expr_from_projection(&select.projection[2])
    );
    assert_eq!(
        &Expr::Struct {
            values: vec![
                Expr::Named {
                    expr: Expr::Value(number("1")).into(),
                    name: Ident::from("a").empty_span(),
                },
                Expr::Named {
                    expr: Expr::Value(Value::SingleQuotedString("abc".to_string())).into(),
                    name: Ident::from("b").empty_span(),
                },
            ],
            fields: Default::default(),
        },
        expr_from_projection(&select.projection[3])
    );
    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::Named {
                expr: Expr::Identifier(Ident::from("str_col").empty_span()).into(),
                name: Ident::from("abc").empty_span(),
            }],
            fields: Default::default(),
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
            values: vec![Expr::Value(number("5"))],
            fields: vec![StructField {
                field_name: None,
                field_type: DataType::Int64,
            }],
        },
        expr_from_projection(&select.projection[0])
    );
    assert_eq!(
        &Expr::Struct {
            values: vec![
                Expr::Value(number("1")),
                Expr::CompoundIdentifier(
                    vec![
                        Ident {
                            value: "t".into(),
                            quote_style: None,
                        },
                        Ident {
                            value: "str_col".into(),
                            quote_style: None,
                        },
                    ]
                    .empty_span()
                ),
            ],
            fields: vec![
                StructField {
                    field_name: Some(Ident::new("x").empty_span()),
                    field_type: DataType::Int64,
                },
                StructField {
                    field_name: Some(Ident::new("y").empty_span()),
                    field_type: DataType::String(None),
                },
            ],
        },
        expr_from_projection(&select.projection[1])
    );
    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::Identifier(Ident::new("nested_col").empty_span())],
            fields: vec![
                StructField {
                    field_name: Some(Ident::new("arr").empty_span()),
                    field_type: DataType::Array(ArrayElemTypeDef::AngleBracket(Box::new(
                        DataType::Float64
                    ))),
                },
                StructField {
                    field_name: Some(Ident::new("str").empty_span()),
                    field_type: DataType::Struct(vec![StructField {
                        field_name: None,
                        field_type: DataType::Bool,
                    }]),
                },
            ],
        },
        expr_from_projection(&select.projection[2])
    );

    let sql = r#"SELECT STRUCT<x STRUCT, y ARRAY<STRUCT>>(nested_col)"#;
    let select = bigquery().verified_only_select(sql);
    assert_eq!(1, select.projection.len());
    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::Identifier(Ident::new("nested_col").empty_span())],
            fields: vec![
                StructField {
                    field_name: Some(Ident::new("x").empty_span()),
                    field_type: DataType::Struct(Default::default()),
                },
                StructField {
                    field_name: Some(Ident::new("y").empty_span()),
                    field_type: DataType::Array(ArrayElemTypeDef::AngleBracket(Box::new(
                        DataType::Struct(Default::default())
                    ))),
                },
            ],
        },
        expr_from_projection(&select.projection[0])
    );

    let sql = r#"SELECT STRUCT<BOOL>(true), STRUCT<BYTES(42)>(B'abc')"#;
    let select = bigquery().verified_only_select(sql);
    assert_eq!(2, select.projection.len());
    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::Value(Value::Boolean(true))],
            fields: vec![StructField {
                field_name: None,
                field_type: DataType::Bool,
            }],
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
                field_type: DataType::Bytes(Some(42)),
            }],
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
                field_type: DataType::Date,
            }],
        },
        expr_from_projection(&select.projection[0])
    );
    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::TypedString {
                data_type: DataType::Datetime(None),
                value: "1999-01-01 01:23:34.45".to_string(),
            },],
            fields: vec![StructField {
                field_name: None,
                field_type: DataType::Datetime(None),
            }],
        },
        expr_from_projection(&select.projection[1])
    );
    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::Value(number("5.0"))],
            fields: vec![StructField {
                field_name: None,
                field_type: DataType::Float64,
            }],
        },
        expr_from_projection(&select.projection[2])
    );
    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::Value(number("1"))],
            fields: vec![StructField {
                field_name: None,
                field_type: DataType::Int64,
            }],
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
                fractional_seconds_precision: None,
            }),],
            fields: vec![StructField {
                field_name: None,
                field_type: DataType::Interval,
            }],
        },
        expr_from_projection(&select.projection[0])
    );
    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::TypedString {
                data_type: DataType::JSON,
                value: r#"{"class" : {"students" : [{"name" : "Jane"}]}}"#.to_string(),
            },],
            fields: vec![StructField {
                field_name: None,
                field_type: DataType::JSON,
            }],
        },
        expr_from_projection(&select.projection[1])
    );

    let sql = r#"SELECT STRUCT<STRING(42)>("foo"), STRUCT<TIMESTAMP>(TIMESTAMP '2008-12-25 15:30:00 America/Los_Angeles'), STRUCT<TIME>(TIME '15:30:00')"#;
    let select = bigquery().verified_only_select(sql);
    assert_eq!(3, select.projection.len());
    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::Value(Value::DoubleQuotedString("foo".to_string()))],
            fields: vec![StructField {
                field_name: None,
                field_type: DataType::String(Some(42)),
            }],
        },
        expr_from_projection(&select.projection[0])
    );
    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::TypedString {
                data_type: DataType::Timestamp(None, TimezoneInfo::None),
                value: "2008-12-25 15:30:00 America/Los_Angeles".to_string(),
            },],
            fields: vec![StructField {
                field_name: None,
                field_type: DataType::Timestamp(None, TimezoneInfo::None),
            }],
        },
        expr_from_projection(&select.projection[1])
    );

    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::TypedString {
                data_type: DataType::Time(None, TimezoneInfo::None),
                value: "15:30:00".to_string(),
            },],
            fields: vec![StructField {
                field_name: None,
                field_type: DataType::Time(None, TimezoneInfo::None),
            }],
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
                value: "1".to_string(),
            },],
            fields: vec![StructField {
                field_name: None,
                field_type: DataType::Numeric(ExactNumberInfo::None),
            }],
        },
        expr_from_projection(&select.projection[0])
    );
    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::TypedString {
                data_type: DataType::BigNumeric(ExactNumberInfo::None),
                value: "1".to_string(),
            },],
            fields: vec![StructField {
                field_name: None,
                field_type: DataType::BigNumeric(ExactNumberInfo::None),
            }],
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
            values: vec![Expr::Value(number("5"))],
            fields: vec![StructField {
                field_name: Some(Ident::from("x").empty_span()),
                field_type: DataType::Int64,
            }],
        },
        expr_from_projection(&select.projection[0])
    );
    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::Value(Value::DoubleQuotedString("foo".to_string()))],
            fields: vec![StructField {
                field_name: Some(Ident::from("y").empty_span()),
                field_type: DataType::String(None),
            }],
        },
        expr_from_projection(&select.projection[1])
    );

    let sql = r#"SELECT STRUCT<x INT64, y INT64>(5, 5)"#;
    let select = bigquery().verified_only_select(sql);
    assert_eq!(1, select.projection.len());
    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::Value(number("5")), Expr::Value(number("5"))],
            fields: vec![
                StructField {
                    field_name: Some(Ident::from("x").empty_span()),
                    field_type: DataType::Int64,
                },
                StructField {
                    field_name: Some(Ident::from("y").empty_span()),
                    field_type: DataType::Int64,
                },
            ],
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
                joins: vec![],
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
            joins: vec![],
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
                array_exprs: vec![Expr::CompoundIdentifier(
                    vec![Ident::new("t1"), Ident::new("a")].empty_span()
                )],
                with_offset: false,
                with_offset_alias: None,
            },
            join_operator: JoinOperator::Inner(JoinConstraint::On(Expr::BinaryOp {
                left: Box::new(Expr::Identifier(Ident::new("c1").empty_span())),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::Identifier(Ident::new("c2").empty_span())),
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
    let sql = r#"SELECT SAFE_CAST(1 AS INT64)"#;
    bigquery().verified_only_select(sql);
}

#[test]
fn parse_cast_date_format() {
    let sql =
        r#"SELECT CAST(date_valid_from AS DATE FORMAT 'YYYY-MM-DD') AS date_valid_from FROM foo"#;
    bigquery().verified_only_select(sql);
}

#[test]
fn parse_cast_time_format() {
    let sql = r#"SELECT CAST(TIME '21:30:00' AS STRING FORMAT 'PM') AS date_time_to_string"#;
    bigquery().verified_only_select(sql);
}

#[test]
#[ignore] // TODO: fix
fn parse_cast_timestamp_format_tz() {
    let sql = r#"SELECT CAST(TIMESTAMP '2008-12-25 00:00:00+00:00' AS STRING FORMAT 'TZH' AT TIME ZONE 'Asia/Kolkata') AS date_time_to_string"#;
    bigquery().verified_only_select(sql);
}

#[test]
fn parse_cast_string_to_bytes_format() {
    let sql = r#"SELECT CAST('Hello' AS BYTES FORMAT 'ASCII') AS string_to_bytes"#;
    bigquery().verified_only_select(sql);
}

#[test]
fn parse_cast_bytes_to_string_format() {
    let sql = r#"SELECT CAST(B'\x48\x65\x6c\x6c\x6f' AS STRING FORMAT 'ASCII') AS bytes_to_string"#;
    bigquery().verified_only_select(sql);
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
                expr: Box::new(Expr::Identifier(Ident::new("name").empty_span())),
                negated,
                pattern: Box::new(Expr::Value(Value::SingleQuotedString("%a".to_string()))),
                escape_char: None,
            }
            .empty_span(),
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
                expr: Box::new(Expr::Identifier(Ident::new("name").empty_span())),
                negated,
                pattern: Box::new(Expr::Value(Value::SingleQuotedString("%a".to_string()))),
                escape_char: Some('\\'),
            }
            .empty_span(),
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
                expr: Box::new(Expr::Identifier(Ident::new("name").empty_span())),
                negated,
                pattern: Box::new(Expr::Value(Value::SingleQuotedString("%a".to_string()))),
                escape_char: None,
            }))
            .empty_span(),
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
                expr: Box::new(Expr::Identifier(Ident::new("name").empty_span())),
                negated,
                pattern: Box::new(Expr::Value(Value::SingleQuotedString("%a".to_string()))),
                escape_char: None,
            }
            .empty_span(),
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
                expr: Box::new(Expr::Identifier(Ident::new("name").empty_span())),
                negated,
                pattern: Box::new(Expr::Value(Value::SingleQuotedString("%a".to_string()))),
                escape_char: Some('\\'),
            }
            .empty_span(),
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
                expr: Box::new(Expr::Identifier(Ident::new("name").empty_span())),
                negated,
                pattern: Box::new(Expr::Value(Value::SingleQuotedString("%a".to_string()))),
                escape_char: Some('\\'),
            }))
            .empty_span(),
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
            first_element: Ident::new("col_a").empty_span(),
            additional_elements: vec![],
        }),
        ..Default::default()
    })
    .empty_span();
    assert_eq!(expected, select.projection[0]);

    let select = bigquery_and_generic()
        .verified_only_select("SELECT * EXCEPT (department_id, employee_id) FROM employee_table");
    let expected = SelectItem::Wildcard(WildcardAdditionalOptions {
        opt_except: Some(ExceptSelectItem {
            first_element: Ident::new("department_id").empty_span(),
            additional_elements: vec![Ident::new("employee_id").empty_span()],
        }),
        ..Default::default()
    })
    .empty_span();
    assert_eq!(expected, select.projection[0]);

    assert_eq!(
        bigquery_and_generic()
            .parse_sql_statements("SELECT * EXCEPT () FROM employee_table")
            .unwrap_err()
            .to_string(),
        "sql parser error: Expected identifier, found: )\nNear `SELECT * EXCEPT ()`"
    );
}

#[test]
fn test_select_agg_ignore_nulls() {
    bigquery().one_statement_parses_to(
        "SELECT last_value(user_id IGNORE NULLS) OVER (PARTITION BY anonymous_id ORDER BY tstamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS user_id FROM table1",
        "SELECT last_value(user_id) IGNORE NULLS OVER (PARTITION BY anonymous_id ORDER BY tstamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS user_id FROM table1",
    );
}

#[test]
fn test_select_agg_order_by() {
    bigquery().verified_only_select(
        "SELECT last_value(user_id ORDER BY user_id) OVER (PARTITION BY anonymous_id ORDER BY tstamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS user_id FROM table1",
    );
}

#[test]
fn test_select_agg_ignore_nulls_order_by() {
    bigquery().one_statement_parses_to(
        "SELECT last_value(user_id IGNORE NULLS ORDER BY user_id) OVER (PARTITION BY anonymous_id ORDER BY tstamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS user_id FROM table1",
        "SELECT last_value(user_id ORDER BY user_id) IGNORE NULLS OVER (PARTITION BY anonymous_id ORDER BY tstamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS user_id FROM table1",
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
    })
    .empty_span();
    assert_eq!(expected, select.projection[0]);

    let select = bigquery_and_generic().verified_only_select(
        r#"SELECT * REPLACE (quantity / 2 AS quantity, 3 AS order_id) FROM orders"#,
    );
    let expected = SelectItem::Wildcard(WildcardAdditionalOptions {
        opt_replace: Some(ReplaceSelectItem {
            items: vec![
                Box::new(ReplaceSelectElement {
                    expr: Expr::BinaryOp {
                        left: Box::new(Expr::Identifier(Ident::new("quantity").empty_span())),
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
    })
    .empty_span();
    assert_eq!(expected, select.projection[0]);
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
        SelectItem::UnnamedExpr(
            Expr::MapAccess {
                column: Box::new(Expr::Identifier(
                    Ident {
                        value: "d".to_string(),
                        quote_style: None,
                    }
                    .empty_span()
                )),
                keys: vec![Expr::Function(Function {
                    name: ObjectName(vec!["offset".into()]),
                    args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
                        number("0")
                    ))),],
                    over: None,
                    distinct: false,
                    special: false,
                    order_by: vec![],
                    limit: None,
                    on_overflow: None,
                    null_treatment: None,
                    within_group: None,
                })],
            }
            .empty_span()
        )
        .empty_span()
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
fn test_array_agg_over() {
    let sql = r"SELECT array_agg(account_combined_id) OVER (PARTITION BY shareholder_id ORDER BY date_from ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS previous_combined_id FROM foo";
    bigquery().verified_only_select(sql);
}

#[test]
fn test_trim() {
    bigquery().verified_only_select(r#"SELECT CAST(TRIM(NULLIF(TRIM(JSON_QUERY(json_dump, "$.email_verified")), ''), '\"') AS BOOL) AS is_email_verified FROM foo"#);
    bigquery().verified_only_select(r#"SELECT CAST(LTRIM(NULLIF(TRIM(JSON_QUERY(json_dump, "$.email_verified")), ''), '\"') AS BOOL) AS is_email_verified FROM foo"#);
    bigquery().verified_only_select(r#"SELECT CAST(RTRIM(NULLIF(TRIM(JSON_QUERY(json_dump, "$.email_verified")), ''), '\"') AS BOOL) AS is_email_verified FROM foo"#);
}

#[test]
fn test_external_query() {
    bigquery().verified_only_select("SELECT * FROM EXTERNAL_QUERY(\"projects/bq-proj/locations/EU/connections/connection_name\",\"SELECT * FROM public.auth0_user \")");
    bigquery().verified_only_select("SELECT * FROM EXTERNAL_QUERY(\"projects/bq-proj/locations/EU/connections/connection_name\",\"SELECT * FROM public.auth0_user \", '{\"default_type_for_decimal_columns\":\"numeric\"}')");
    bigquery().verified_only_select("SELECT * FROM EXTERNAL_QUERY('connection_id','''SELECT * FROM customers AS c ORDER BY c.customer_id''')");
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

#[test]
fn test_select_array_item_field() {
    bigquery().verified_only_select(
        "SELECT arr[SAFE_OFFSET(0)].id AS arr_id FROM `proj`.`dataset`.`table`",
    );
}

#[test]
fn test_select_array_item_field_in_function() {
    bigquery().verified_only_select(
        "SELECT LOWER(arr[SAFE_OFFSET(0)].id) AS arr_id FROM `proj`.`dataset`.`table`",
    );
}

#[test]
fn test_select_json_field() {
    let _select = bigquery().verified_only_select(
        "SELECT JSON_VALUE(PARSE_JSON(response_json).user.username) AS arr_id FROM `proj`.`dataset`.`table`",
    );

    assert_eq!(
        SelectItem::ExprWithAlias {
            expr: Expr::Function(Function {
                name: ObjectName(vec!["JSON_VALUE".into()]),
                args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(
                    Expr::JsonAccess {
                        left: Box::new(Expr::Function(Function {
                            name: ObjectName(vec!["PARSE_JSON".into()]),
                            args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(
                                Expr::Identifier(
                                    Ident::new("response_json".to_string()).empty_span()
                                )
                            ))],
                            over: None,
                            distinct: false,
                            special: false,
                            order_by: vec![],
                            limit: None,
                            on_overflow: None,
                            null_treatment: None,
                            within_group: None,
                        })),
                        operator: JsonOperator::Period,
                        right: Box::new(Expr::Value(Value::UnQuotedString(
                            "user.username".to_string()
                        ))),
                    }
                ))],
                over: None,
                distinct: false,
                special: false,
                order_by: vec![],
                limit: None,
                on_overflow: None,
                null_treatment: None,
                within_group: None,
            })
            .empty_span(),
            alias: Ident::new("arr_id").empty_span(),
        }
        .empty_span(),
        _select.projection[0]
    );
}

#[test]
fn test_bigquery_single_line_comment_tokenize() {
    let sql = "CREATE TABLE# this is a comment \ntable_1";
    let dialect = BigQueryDialect {};
    let tokens = Tokenizer::new(&dialect, sql).tokenize().unwrap();

    let expected = vec![
        Token::make_keyword("CREATE"),
        Token::Whitespace(Whitespace::Space),
        Token::make_keyword("TABLE"),
        Token::Whitespace(Whitespace::SingleLineComment {
            prefix: "#".to_string(),
            comment: " this is a comment \n".to_string(),
        }),
        Token::make_word("table_1", None),
    ];

    assert_eq!(expected, tokens);
}

#[test]
fn test_bigquery_single_line_comment_parsing() {
    bigquery().verified_only_select_with_canonical(
        "SELECT book# this is a comment \n FROM library",
        "SELECT book FROM library",
    );
}

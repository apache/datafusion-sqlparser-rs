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

use std::ops::Deref;

use sqlparser::ast::helpers::attached_token::AttachedToken;
use sqlparser::ast::*;
use sqlparser::dialect::{BigQueryDialect, GenericDialect};
use sqlparser::keywords::Keyword;
use sqlparser::parser::{ParserError, ParserOptions};
use sqlparser::tokenizer::{Location, Span, Token, TokenWithSpan, Word};
use test_utils::*;

#[test]
fn parse_literal_string() {
    let sql = concat!(
        "SELECT ",                            // line 1, column 1
        "'single', ",                         // line 1, column 7
        r#""double", "#,                      // line 1, column 14
        "'''triple-single''', ",              // line 1, column 22
        r#""""triple-double""", "#,           // line 1, column 33
        r#"'single\'escaped', "#,             // line 1, column 43
        r#"'''triple-single\'escaped''', "#,  // line 1, column 55
        r#"'''triple-single'unescaped''', "#, // line 1, column 68
        r#""double\"escaped", "#,             // line 1, column 83
        r#""""triple-double\"escaped""", "#,  // line 1, column 92
        r#""""triple-double"unescaped""", "#, // line 1, column 105
        r#""""triple-double'unescaped""", "#, // line 1, column 118
        r#"'''triple-single"unescaped'''"#,   // line 1, column 131
    );
    let dialect = TestedDialects::new_with_options(
        vec![Box::new(BigQueryDialect {})],
        ParserOptions::new().with_unescape(false),
    );
    let select = dialect.verified_only_select(sql);
    assert_eq!(12, select.projection.len());
    assert_eq!(
        &Expr::Value(Value::SingleQuotedString("single".into()).with_empty_span()),
        expr_from_projection(&select.projection[0])
    );
    assert_eq!(
        &Expr::Value(Value::DoubleQuotedString("double".into()).with_empty_span()),
        expr_from_projection(&select.projection[1])
    );
    assert_eq!(
        &Expr::Value(Value::TripleSingleQuotedString("triple-single".into()).with_empty_span()),
        expr_from_projection(&select.projection[2])
    );
    assert_eq!(
        &Expr::Value(Value::TripleDoubleQuotedString("triple-double".into()).with_empty_span()),
        expr_from_projection(&select.projection[3])
    );
    assert_eq!(
        &Expr::Value(Value::SingleQuotedString(r#"single\'escaped"#.into()).with_empty_span()),
        expr_from_projection(&select.projection[4])
    );
    assert_eq!(
        &Expr::Value(
            Value::TripleSingleQuotedString(r#"triple-single\'escaped"#.into()).with_empty_span()
        ),
        expr_from_projection(&select.projection[5])
    );
    assert_eq!(
        &Expr::Value(
            Value::TripleSingleQuotedString(r#"triple-single'unescaped"#.into()).with_empty_span()
        ),
        expr_from_projection(&select.projection[6])
    );
    assert_eq!(
        &Expr::Value(Value::DoubleQuotedString(r#"double\"escaped"#.to_string()).with_empty_span()),
        expr_from_projection(&select.projection[7])
    );
    assert_eq!(
        &Expr::Value(
            Value::TripleDoubleQuotedString(r#"triple-double\"escaped"#.to_string())
                .with_empty_span()
        ),
        expr_from_projection(&select.projection[8])
    );
    assert_eq!(
        &Expr::Value(
            Value::TripleDoubleQuotedString(r#"triple-double"unescaped"#.to_string())
                .with_empty_span()
        ),
        expr_from_projection(&select.projection[9])
    );
    assert_eq!(
        &Expr::Value(
            Value::TripleDoubleQuotedString(r#"triple-double'unescaped"#.to_string())
                .with_empty_span()
        ),
        expr_from_projection(&select.projection[10])
    );
    assert_eq!(
        &Expr::Value(
            Value::TripleSingleQuotedString(r#"triple-single"unescaped"#.to_string())
                .with_empty_span()
        ),
        expr_from_projection(&select.projection[11])
    );
}

#[test]
fn parse_byte_literal() {
    let sql = concat!(
        "SELECT ",                   // line 1, column 1
        "B'abc', ",                  // line 1, column 8
        r#"B"abc", "#,               // line 1, column 15
        r#"B'f\(abc,(.*),def\)', "#, // line 1, column 22
        r#"B"f\(abc,(.*),def\)", "#, // line 1, column 42
        r#"B'''abc''', "#,           // line 1, column 62
        r#"B"""abc""""#,             // line 1, column 74
    );
    let stmt = bigquery().verified_stmt(sql);
    if let Statement::Query(query) = stmt {
        if let SetExpr::Select(select) = *query.body {
            assert_eq!(6, select.projection.len());
            assert_eq!(
                &Expr::Value(
                    Value::SingleQuotedByteStringLiteral("abc".to_string()).with_empty_span()
                ),
                expr_from_projection(&select.projection[0])
            );
            assert_eq!(
                &Expr::Value(
                    Value::DoubleQuotedByteStringLiteral("abc".to_string()).with_empty_span()
                ),
                expr_from_projection(&select.projection[1])
            );
            assert_eq!(
                &Expr::Value(
                    Value::SingleQuotedByteStringLiteral(r"f\(abc,(.*),def\)".to_string())
                        .with_empty_span()
                ),
                expr_from_projection(&select.projection[2])
            );
            assert_eq!(
                &Expr::Value(
                    Value::DoubleQuotedByteStringLiteral(r"f\(abc,(.*),def\)".to_string())
                        .with_empty_span()
                ),
                expr_from_projection(&select.projection[3])
            );
            assert_eq!(
                &Expr::Value(
                    Value::TripleSingleQuotedByteStringLiteral(r"abc".to_string())
                        .with_empty_span()
                ),
                expr_from_projection(&select.projection[4])
            );
            assert_eq!(
                &Expr::Value(
                    Value::TripleDoubleQuotedByteStringLiteral(r"abc".to_string())
                        .with_empty_span()
                ),
                expr_from_projection(&select.projection[5])
            );
        }
    } else {
        panic!("invalid query");
    }

    bigquery().one_statement_parses_to(
        r#"SELECT b'123', b"123", b'''123''', b"""123""""#,
        r#"SELECT B'123', B"123", B'''123''', B"""123""""#,
    );
}

#[test]
fn parse_raw_literal() {
    let sql = concat!(
        "SELECT ",                   // line 1, column 1
        "R'abc', ",                  // line 1, column 8
        r#"R"abc", "#,               // line 1, column 15
        r#"R'f\(abc,(.*),def\)', "#, // line 1, column 22
        r#"R"f\(abc,(.*),def\)", "#, // line 1, column 42
        r#"R'''abc''', "#,           // line 1, column 62
        r#"R"""abc""""#,             // line 1, column 74
    );
    let stmt = bigquery().verified_stmt(sql);
    if let Statement::Query(query) = stmt {
        if let SetExpr::Select(select) = *query.body {
            assert_eq!(6, select.projection.len());
            assert_eq!(
                &Expr::Value(
                    Value::SingleQuotedRawStringLiteral("abc".to_string()).with_empty_span()
                ),
                expr_from_projection(&select.projection[0])
            );
            assert_eq!(
                &Expr::Value(
                    Value::DoubleQuotedRawStringLiteral("abc".to_string()).with_empty_span()
                ),
                expr_from_projection(&select.projection[1])
            );
            assert_eq!(
                &Expr::Value(
                    Value::SingleQuotedRawStringLiteral(r"f\(abc,(.*),def\)".to_string())
                        .with_empty_span()
                ),
                expr_from_projection(&select.projection[2])
            );
            assert_eq!(
                &Expr::Value(
                    Value::DoubleQuotedRawStringLiteral(r"f\(abc,(.*),def\)".to_string())
                        .with_empty_span()
                ),
                expr_from_projection(&select.projection[3])
            );
            assert_eq!(
                &Expr::Value(
                    Value::TripleSingleQuotedRawStringLiteral(r"abc".to_string()).with_empty_span()
                ),
                expr_from_projection(&select.projection[4])
            );
            assert_eq!(
                &Expr::Value(
                    Value::TripleDoubleQuotedRawStringLiteral(r"abc".to_string()).with_empty_span()
                ),
                expr_from_projection(&select.projection[5])
            );
        }
    } else {
        panic!("invalid query");
    }

    bigquery().one_statement_parses_to(
        r#"SELECT r'123', r"123", r'''123''', r"""123""""#,
        r#"SELECT R'123', R"123", R'''123''', R"""123""""#,
    );
}

#[test]
fn parse_big_query_non_reserved_column_alias() {
    let sql = r#"SELECT OFFSET, EXPLAIN, ANALYZE, SORT, TOP, VIEW FROM T"#;
    bigquery().verified_stmt(sql);

    let sql = r#"SELECT 1 AS OFFSET, 2 AS EXPLAIN, 3 AS ANALYZE FROM T"#;
    bigquery().verified_stmt(sql);
}

#[test]
fn parse_at_at_identifier() {
    bigquery().verified_stmt("SELECT @@error.stack_trace, @@error.message");
}

#[test]
fn parse_begin() {
    let sql = r#"BEGIN SELECT 1; EXCEPTION WHEN ERROR THEN SELECT 2; RAISE USING MESSAGE = FORMAT('ERR: %s', 'Bad'); END"#;
    let Statement::StartTransaction {
        statements,
        exception,
        has_end_keyword,
        ..
    } = bigquery().verified_stmt(sql)
    else {
        unreachable!();
    };
    assert_eq!(1, statements.len());
    assert!(exception.is_some());

    let exception = exception.unwrap();
    assert_eq!(1, exception.len());
    assert!(has_end_keyword);

    bigquery().verified_stmt(
        "BEGIN SELECT 1; SELECT 2; EXCEPTION WHEN ERROR THEN SELECT 2; SELECT 4; END",
    );
    bigquery()
        .verified_stmt("BEGIN SELECT 1; EXCEPTION WHEN ERROR THEN SELECT @@error.stack_trace; END");
    bigquery().verified_stmt("BEGIN EXCEPTION WHEN ERROR THEN SELECT 2; END");
    bigquery().verified_stmt("BEGIN SELECT 1; SELECT 2; EXCEPTION WHEN ERROR THEN END");
    bigquery().verified_stmt("BEGIN EXCEPTION WHEN ERROR THEN END");
    bigquery().verified_stmt("BEGIN SELECT 1; SELECT 2; END");
    bigquery().verified_stmt("BEGIN END");

    assert_eq!(
        bigquery()
            .parse_sql_statements("BEGIN SELECT 1; SELECT 2 END")
            .unwrap_err(),
        ParserError::ParserError("Expected: ;, found: END".to_string())
    );
    assert_eq!(
        bigquery()
            .parse_sql_statements("BEGIN SELECT 1; EXCEPTION WHEN ERROR THEN SELECT 2 END")
            .unwrap_err(),
        ParserError::ParserError("Expected: ;, found: END".to_string())
    );
}

#[test]
fn parse_delete_statement() {
    let sql = "DELETE \"table\" WHERE 1";
    match bigquery_and_generic().verified_stmt(sql) {
        Statement::Delete(Delete {
            from: FromTable::WithoutKeyword(from),
            ..
        }) => {
            assert_eq!(
                table_from_name(ObjectName::from(vec![Ident::with_quote('"', "table")])),
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
        Statement::CreateView(CreateView {
            name,
            query,
            options,
            columns,
            ..
        }) => {
            assert_eq!(
                name,
                ObjectName::from(vec![
                    "myproject".into(),
                    "mydataset".into(),
                    "newview".into()
                ])
            );
            assert_eq!(
                vec![
                    ViewColumnDef {
                        name: Ident::new("name"),
                        data_type: None,
                        options: None,
                    },
                    ViewColumnDef {
                        name: Ident::new("age"),
                        data_type: None,
                        options: Some(ColumnOptions::CommaSeparated(vec![ColumnOption::Options(
                            vec![SqlOption::KeyValue {
                                key: Ident::new("description"),
                                value: Expr::Value(
                                    Value::DoubleQuotedString("field age".to_string()).with_span(
                                        Span::new(Location::new(1, 42), Location::new(1, 52))
                                    )
                                ),
                            }]
                        )])),
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
                &SqlOption::KeyValue {
                    key: Ident::new("description"),
                    value: Expr::Value(
                        Value::DoubleQuotedString("a view that expires in 2 days".to_string())
                            .with_empty_span()
                    ),
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
        Statement::CreateView(CreateView {
            name,
            columns,
            query,
            or_replace,
            materialized,
            options,
            cluster_by,
            comment,
            with_no_schema_binding: late_binding,
            if_not_exists,
            temporary,
            ..
        }) => {
            assert_eq!("mydataset.newview", name.to_string());
            assert_eq!(Vec::<ViewColumnDef>::new(), columns);
            assert_eq!("SELECT foo FROM bar", query.to_string());
            assert!(!materialized);
            assert!(!or_replace);
            assert_eq!(options, CreateTableOptions::None);
            assert_eq!(cluster_by, vec![]);
            assert!(comment.is_none());
            assert!(!late_binding);
            assert!(if_not_exists);
            assert!(!temporary);
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_view_with_unquoted_hyphen() {
    let sql = "CREATE VIEW IF NOT EXISTS my-pro-ject.mydataset.myview AS SELECT 1";
    match bigquery().verified_stmt(sql) {
        Statement::CreateView(CreateView {
            name,
            query,
            if_not_exists,
            ..
        }) => {
            assert_eq!("my-pro-ject.mydataset.myview", name.to_string());
            assert_eq!("SELECT 1", query.to_string());
            assert!(if_not_exists);
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_table_with_unquoted_hyphen() {
    let sql = "CREATE TABLE my-pro-ject.mydataset.mytable (x INT64)";
    match bigquery().verified_stmt(sql) {
        Statement::CreateTable(CreateTable { name, columns, .. }) => {
            assert_eq!(
                name,
                ObjectName::from(vec![
                    "my-pro-ject".into(),
                    "mydataset".into(),
                    "mytable".into()
                ])
            );
            assert_eq!(
                vec![ColumnDef {
                    name: Ident::new("x"),
                    data_type: DataType::Int64,
                    options: vec![]
                },],
                columns
            );
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
        Statement::CreateTable(CreateTable {
            name,
            columns,
            partition_by,
            cluster_by,
            table_options,
            ..
        }) => {
            assert_eq!(
                name,
                ObjectName::from(vec!["mydataset".into(), "newtable".into()])
            );
            assert_eq!(
                vec![
                    ColumnDef {
                        name: Ident::new("x"),
                        data_type: DataType::Int64,
                        options: vec![
                            ColumnOptionDef {
                                name: None,
                                option: ColumnOption::NotNull,
                            },
                            ColumnOptionDef {
                                name: None,
                                option: ColumnOption::Options(vec![SqlOption::KeyValue {
                                    key: Ident::new("description"),
                                    value: Expr::Value(
                                        Value::DoubleQuotedString("field x".to_string()).with_span(
                                            Span::new(Location::new(1, 42), Location::new(1, 52))
                                        )
                                    ),
                                },])
                            },
                        ]
                    },
                    ColumnDef {
                        name: Ident::new("y"),
                        data_type: DataType::Bool,
                        options: vec![ColumnOptionDef {
                            name: None,
                            option: ColumnOption::Options(vec![SqlOption::KeyValue {
                                key: Ident::new("description"),
                                value: Expr::Value(
                                    Value::DoubleQuotedString("field y".to_string()).with_span(
                                        Span::new(Location::new(1, 42), Location::new(1, 52))
                                    )
                                ),
                            },])
                        }]
                    },
                ],
                columns
            );
            assert_eq!(
                (
                    Some(Box::new(Expr::Identifier(Ident::new("_PARTITIONDATE")))),
                    Some(WrappedCollection::NoWrapping(vec![
                        Expr::Identifier(Ident::new("userid")),
                        Expr::Identifier(Ident::new("age")),
                    ])),
                    CreateTableOptions::Options(vec![
                        SqlOption::KeyValue {
                            key: Ident::new("partition_expiration_days"),
                            value: Expr::Value(
                                number("1").with_span(Span::new(
                                    Location::new(1, 42),
                                    Location::new(1, 43)
                                ))
                            ),
                        },
                        SqlOption::KeyValue {
                            key: Ident::new("description"),
                            value: Expr::Value(
                                Value::DoubleQuotedString("table option description".to_string())
                                    .with_span(Span::new(
                                        Location::new(1, 42),
                                        Location::new(1, 52)
                                    ))
                            ),
                        },
                    ])
                ),
                (partition_by, cluster_by, table_options)
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

    let sql = "CREATE TABLE foo (x INT64) OPTIONS()";
    bigquery().verified_stmt(sql);

    let sql = "CREATE TABLE db.schema.test (x INT64 OPTIONS(description = 'An optional INTEGER field')) OPTIONS()";
    bigquery().verified_stmt(sql);
}

#[test]
fn parse_nested_data_types() {
    let sql = "CREATE TABLE table (x STRUCT<a ARRAY<INT64>, b BYTES(42)>, y ARRAY<STRUCT<INT64>>)";
    match bigquery_and_generic().one_statement_parses_to(sql, sql) {
        Statement::CreateTable(CreateTable { name, columns, .. }) => {
            assert_eq!(name, ObjectName::from(vec!["table".into()]));
            assert_eq!(
                columns,
                vec![
                    ColumnDef {
                        name: Ident::new("x"),
                        data_type: DataType::Struct(
                            vec![
                                StructField {
                                    field_name: Some("a".into()),
                                    field_type: DataType::Array(ArrayElemTypeDef::AngleBracket(
                                        Box::new(DataType::Int64,)
                                    )),
                                    options: None,
                                },
                                StructField {
                                    field_name: Some("b".into()),
                                    field_type: DataType::Bytes(Some(42)),
                                    options: None,
                                },
                            ],
                            StructBracketKind::AngleBrackets
                        ),
                        options: vec![],
                    },
                    ColumnDef {
                        name: Ident::new("y"),
                        data_type: DataType::Array(ArrayElemTypeDef::AngleBracket(Box::new(
                            DataType::Struct(
                                vec![StructField {
                                    field_name: None,
                                    field_type: DataType::Int64,
                                    options: None,
                                }],
                                StructBracketKind::AngleBrackets
                            ),
                        ))),
                        options: vec![],
                    },
                ]
            );
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_tuple_struct_literal() {
    // tuple syntax: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#tuple_syntax
    // syntax: (expr1, expr2 [, ... ])
    let sql = "SELECT (1, 2, 3), (1, 1.0, '123', true)"; // line 1, column 1
    let select = bigquery().verified_only_select(sql);
    assert_eq!(2, select.projection.len());
    assert_eq!(
        &Expr::Tuple(vec![
            Expr::value(number("1")),
            Expr::value(number("2")),
            Expr::value(number("3")),
        ]),
        expr_from_projection(&select.projection[0])
    );
    assert_eq!(
        &Expr::Tuple(vec![
            Expr::value(number("1")),
            Expr::value(number("1.0")),
            Expr::Value(Value::SingleQuotedString("123".into()).with_empty_span()),
            Expr::Value(Value::Boolean(true).with_empty_span())
        ]),
        expr_from_projection(&select.projection[1])
    );
}

#[test]
fn parse_typeless_struct_syntax() {
    // typeless struct syntax https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#typeless_struct_syntax
    // syntax: STRUCT( expr1 [AS field_name] [, ... ])
    let sql = "SELECT STRUCT(1, 2, 3), STRUCT('abc'), STRUCT(1, t.str_col), STRUCT(1 AS a, 'abc' AS b), STRUCT(str_col AS abc)";
    let select = bigquery_and_generic().verified_only_select(sql);
    assert_eq!(5, select.projection.len());
    assert_eq!(
        &Expr::Struct {
            values: vec![
                Expr::value(number("1")),
                Expr::value(number("2")),
                Expr::value(number("3")),
            ],
            fields: Default::default()
        },
        expr_from_projection(&select.projection[0])
    );

    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::Value(
                Value::SingleQuotedString("abc".into()).with_empty_span()
            )],
            fields: Default::default()
        },
        expr_from_projection(&select.projection[1])
    );

    assert_eq!(
        &Expr::Struct {
            values: vec![
                Expr::value(number("1")),
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
                    expr: Expr::value(number("1")).into(),
                    name: Ident::from("a")
                },
                Expr::Named {
                    expr: Expr::Value(Value::SingleQuotedString("abc".into()).with_empty_span())
                        .into(),
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
fn parse_typed_struct_syntax_bigquery() {
    // typed struct syntax https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#typed_struct_syntax
    // syntax: STRUCT<[field_name] field_type, ...>( expr1 [, ... ])

    let sql = r#"SELECT STRUCT<INT64>(5), STRUCT<x INT64, y STRING>(1, t.str_col), STRUCT<arr ARRAY<FLOAT64>, str STRUCT<BOOL>>(nested_col)"#;
    let select = bigquery().verified_only_select(sql);
    assert_eq!(3, select.projection.len());
    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::value(number("5"))],
            fields: vec![StructField {
                field_name: None,
                field_type: DataType::Int64,
                options: None,
            }]
        },
        expr_from_projection(&select.projection[0])
    );
    assert_eq!(
        &Expr::Struct {
            values: vec![
                Expr::value(number("1")),
                Expr::CompoundIdentifier(vec![
                    Ident {
                        value: "t".into(),
                        quote_style: None,
                        span: Span::empty(),
                    },
                    Ident {
                        value: "str_col".into(),
                        quote_style: None,
                        span: Span::empty(),
                    },
                ]),
            ],
            fields: vec![
                StructField {
                    field_name: Some(Ident {
                        value: "x".into(),
                        quote_style: None,
                        span: Span::empty(),
                    }),
                    field_type: DataType::Int64,
                    options: None,
                },
                StructField {
                    field_name: Some(Ident {
                        value: "y".into(),
                        quote_style: None,
                        span: Span::empty(),
                    }),
                    field_type: DataType::String(None),
                    options: None,
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
                span: Span::empty(),
            })],
            fields: vec![
                StructField {
                    field_name: Some("arr".into()),
                    field_type: DataType::Array(ArrayElemTypeDef::AngleBracket(Box::new(
                        DataType::Float64
                    ))),
                    options: None,
                },
                StructField {
                    field_name: Some("str".into()),
                    field_type: DataType::Struct(
                        vec![StructField {
                            field_name: None,
                            field_type: DataType::Bool,
                            options: None,
                        }],
                        StructBracketKind::AngleBrackets
                    ),
                    options: None,
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
                span: Span::empty(),
            })],
            fields: vec![
                StructField {
                    field_name: Some("x".into()),
                    field_type: DataType::Struct(
                        Default::default(),
                        StructBracketKind::AngleBrackets
                    ),
                    options: None,
                },
                StructField {
                    field_name: Some("y".into()),
                    field_type: DataType::Array(ArrayElemTypeDef::AngleBracket(Box::new(
                        DataType::Struct(Default::default(), StructBracketKind::AngleBrackets)
                    ))),
                    options: None,
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
            values: vec![Expr::Value(Value::Boolean(true).with_empty_span())],
            fields: vec![StructField {
                field_name: None,
                field_type: DataType::Bool,
                options: None,
            }]
        },
        expr_from_projection(&select.projection[0])
    );
    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::Value(
                Value::SingleQuotedByteStringLiteral("abc".into()).with_empty_span()
            )],
            fields: vec![StructField {
                field_name: None,
                field_type: DataType::Bytes(Some(42)),
                options: None,
            }]
        },
        expr_from_projection(&select.projection[1])
    );

    let sql = r#"SELECT STRUCT<DATE>("2011-05-05"), STRUCT<DATETIME>(DATETIME '1999-01-01 01:23:34.45'), STRUCT<FLOAT64>(5.0), STRUCT<INT64>(1)"#;
    let select = bigquery().verified_only_select(sql);
    assert_eq!(4, select.projection.len());
    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::Value(
                Value::DoubleQuotedString("2011-05-05".into()).with_empty_span()
            )],
            fields: vec![StructField {
                field_name: None,
                field_type: DataType::Date,
                options: None,
            }]
        },
        expr_from_projection(&select.projection[0])
    );
    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::TypedString(TypedString {
                data_type: DataType::Datetime(None),
                value: ValueWithSpan {
                    value: Value::SingleQuotedString("1999-01-01 01:23:34.45".into()),
                    span: Span::empty(),
                },
                uses_odbc_syntax: false
            })],
            fields: vec![StructField {
                field_name: None,
                field_type: DataType::Datetime(None),
                options: None,
            }]
        },
        expr_from_projection(&select.projection[1])
    );
    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::value(number("5.0"))],
            fields: vec![StructField {
                field_name: None,
                field_type: DataType::Float64,
                options: None,
            }]
        },
        expr_from_projection(&select.projection[2])
    );
    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::value(number("1"))],
            fields: vec![StructField {
                field_name: None,
                field_type: DataType::Int64,
                options: None,
            }]
        },
        expr_from_projection(&select.projection[3])
    );

    let sql = r#"SELECT STRUCT<INTERVAL>(INTERVAL '2' HOUR), STRUCT<JSON>(JSON '{"class" : {"students" : [{"name" : "Jane"}]}}')"#;
    let select = bigquery().verified_only_select(sql);
    assert_eq!(2, select.projection.len());
    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::Interval(Interval {
                value: Box::new(Expr::Value(
                    Value::SingleQuotedString("2".into()).with_empty_span()
                )),
                leading_field: Some(DateTimeField::Hour),
                leading_precision: None,
                last_field: None,
                fractional_seconds_precision: None
            })],
            fields: vec![StructField {
                field_name: None,
                field_type: DataType::Interval {
                    fields: None,
                    precision: None
                },
                options: None,
            }]
        },
        expr_from_projection(&select.projection[0])
    );
    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::TypedString(TypedString {
                data_type: DataType::JSON,
                value: ValueWithSpan {
                    value: Value::SingleQuotedString(
                        r#"{"class" : {"students" : [{"name" : "Jane"}]}}"#.into()
                    ),
                    span: Span::empty(),
                },
                uses_odbc_syntax: false
            })],
            fields: vec![StructField {
                field_name: None,
                field_type: DataType::JSON,
                options: None,
            }]
        },
        expr_from_projection(&select.projection[1])
    );

    let sql = r#"SELECT STRUCT<STRING(42)>("foo"), STRUCT<TIMESTAMP>(TIMESTAMP '2008-12-25 15:30:00 America/Los_Angeles'), STRUCT<TIME>(TIME '15:30:00')"#;
    let select = bigquery().verified_only_select(sql);
    assert_eq!(3, select.projection.len());
    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::Value(
                Value::DoubleQuotedString("foo".into()).with_empty_span()
            )],
            fields: vec![StructField {
                field_name: None,
                field_type: DataType::String(Some(42)),
                options: None,
            }]
        },
        expr_from_projection(&select.projection[0])
    );
    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::TypedString(TypedString {
                data_type: DataType::Timestamp(None, TimezoneInfo::None),
                value: ValueWithSpan {
                    value: Value::SingleQuotedString(
                        "2008-12-25 15:30:00 America/Los_Angeles".into()
                    ),
                    span: Span::empty(),
                },
                uses_odbc_syntax: false
            })],
            fields: vec![StructField {
                field_name: None,
                field_type: DataType::Timestamp(None, TimezoneInfo::None),
                options: None,
            }]
        },
        expr_from_projection(&select.projection[1])
    );

    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::TypedString(TypedString {
                data_type: DataType::Time(None, TimezoneInfo::None),
                value: ValueWithSpan {
                    value: Value::SingleQuotedString("15:30:00".into()),
                    span: Span::empty(),
                },
                uses_odbc_syntax: false
            })],
            fields: vec![StructField {
                field_name: None,
                field_type: DataType::Time(None, TimezoneInfo::None),
                options: None,
            }]
        },
        expr_from_projection(&select.projection[2])
    );

    let sql = r#"SELECT STRUCT<NUMERIC>(NUMERIC '1'), STRUCT<BIGNUMERIC>(BIGNUMERIC '1')"#;
    let select = bigquery().verified_only_select(sql);
    assert_eq!(2, select.projection.len());
    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::TypedString(TypedString {
                data_type: DataType::Numeric(ExactNumberInfo::None),
                value: ValueWithSpan {
                    value: Value::SingleQuotedString("1".into()),
                    span: Span::empty(),
                },
                uses_odbc_syntax: false
            })],
            fields: vec![StructField {
                field_name: None,
                field_type: DataType::Numeric(ExactNumberInfo::None),
                options: None,
            }]
        },
        expr_from_projection(&select.projection[0])
    );
    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::TypedString(TypedString {
                data_type: DataType::BigNumeric(ExactNumberInfo::None),
                value: ValueWithSpan {
                    value: Value::SingleQuotedString("1".into()),
                    span: Span::empty(),
                },
                uses_odbc_syntax: false
            })],
            fields: vec![StructField {
                field_name: None,
                field_type: DataType::BigNumeric(ExactNumberInfo::None),
                options: None,
            }]
        },
        expr_from_projection(&select.projection[1])
    );

    // Keywords in the parser may be used as field names.
    let sql = r#"SELECT STRUCT<key INT64, value INT64>(1, 2)"#;
    let select = bigquery().verified_only_select(sql);
    assert_eq!(1, select.projection.len());
    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::value(number("1")), Expr::value(number("2")),],
            fields: vec![
                StructField {
                    field_name: Some("key".into()),
                    field_type: DataType::Int64,
                    options: None,
                },
                StructField {
                    field_name: Some("value".into()),
                    field_type: DataType::Int64,
                    options: None,
                },
            ]
        },
        expr_from_projection(&select.projection[0])
    );
}

#[test]
fn parse_typed_struct_syntax_bigquery_and_generic() {
    // typed struct syntax https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#typed_struct_syntax
    // syntax: STRUCT<[field_name] field_type, ...>( expr1 [, ... ])

    let sql = r#"SELECT STRUCT<INT64>(5), STRUCT<x INT64, y STRING>(1, t.str_col), STRUCT<arr ARRAY<FLOAT64>, str STRUCT<BOOL>>(nested_col)"#;
    let select = bigquery_and_generic().verified_only_select(sql);
    assert_eq!(3, select.projection.len());
    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::value(number("5"))],
            fields: vec![StructField {
                field_name: None,
                field_type: DataType::Int64,
                options: None,
            }]
        },
        expr_from_projection(&select.projection[0])
    );
    assert_eq!(
        &Expr::Struct {
            values: vec![
                Expr::value(number("1")),
                Expr::CompoundIdentifier(vec![
                    Ident {
                        value: "t".into(),
                        quote_style: None,
                        span: Span::empty(),
                    },
                    Ident {
                        value: "str_col".into(),
                        quote_style: None,
                        span: Span::empty(),
                    },
                ]),
            ],
            fields: vec![
                StructField {
                    field_name: Some(Ident {
                        value: "x".into(),
                        quote_style: None,
                        span: Span::empty(),
                    }),
                    field_type: DataType::Int64,
                    options: None,
                },
                StructField {
                    field_name: Some(Ident {
                        value: "y".into(),
                        quote_style: None,
                        span: Span::empty(),
                    }),
                    field_type: DataType::String(None),
                    options: None,
                },
            ]
        },
        expr_from_projection(&select.projection[1])
    );

    let sql = r#"SELECT STRUCT<x STRUCT, y ARRAY<STRUCT>>(nested_col)"#;
    let select = bigquery_and_generic().verified_only_select(sql);
    assert_eq!(1, select.projection.len());
    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::Identifier(Ident {
                value: "nested_col".into(),
                quote_style: None,
                span: Span::empty(),
            })],
            fields: vec![
                StructField {
                    field_name: Some("x".into()),
                    field_type: DataType::Struct(
                        Default::default(),
                        StructBracketKind::AngleBrackets
                    ),
                    options: None,
                },
                StructField {
                    field_name: Some("y".into()),
                    field_type: DataType::Array(ArrayElemTypeDef::AngleBracket(Box::new(
                        DataType::Struct(Default::default(), StructBracketKind::AngleBrackets)
                    ))),
                    options: None,
                },
            ]
        },
        expr_from_projection(&select.projection[0])
    );

    let sql = r#"SELECT STRUCT<BOOL>(true), STRUCT<BYTES(42)>(B'abc')"#;
    let select = bigquery_and_generic().verified_only_select(sql);
    assert_eq!(2, select.projection.len());
    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::Value(Value::Boolean(true).with_empty_span())],
            fields: vec![StructField {
                field_name: None,
                field_type: DataType::Bool,
                options: None,
            }]
        },
        expr_from_projection(&select.projection[0])
    );
    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::Value(
                Value::SingleQuotedByteStringLiteral("abc".into()).with_empty_span()
            )],
            fields: vec![StructField {
                field_name: None,
                field_type: DataType::Bytes(Some(42)),
                options: None,
            }]
        },
        expr_from_projection(&select.projection[1])
    );

    let sql = r#"SELECT STRUCT<DATE>('2011-05-05'), STRUCT<DATETIME>(DATETIME '1999-01-01 01:23:34.45'), STRUCT<FLOAT64>(5.0), STRUCT<INT64>(1)"#;
    let select = bigquery_and_generic().verified_only_select(sql);
    assert_eq!(4, select.projection.len());
    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::Value(
                Value::SingleQuotedString("2011-05-05".into()).with_empty_span()
            )],
            fields: vec![StructField {
                field_name: None,
                field_type: DataType::Date,
                options: None,
            }]
        },
        expr_from_projection(&select.projection[0])
    );
    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::TypedString(TypedString {
                data_type: DataType::Datetime(None),
                value: ValueWithSpan {
                    value: Value::SingleQuotedString("1999-01-01 01:23:34.45".into()),
                    span: Span::empty(),
                },
                uses_odbc_syntax: false
            })],
            fields: vec![StructField {
                field_name: None,
                field_type: DataType::Datetime(None),
                options: None,
            }]
        },
        expr_from_projection(&select.projection[1])
    );
    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::value(number("5.0"))],
            fields: vec![StructField {
                field_name: None,
                field_type: DataType::Float64,
                options: None,
            }]
        },
        expr_from_projection(&select.projection[2])
    );
    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::value(number("1"))],
            fields: vec![StructField {
                field_name: None,
                field_type: DataType::Int64,
                options: None,
            }]
        },
        expr_from_projection(&select.projection[3])
    );

    let sql = r#"SELECT STRUCT<INTERVAL>(INTERVAL '1' MONTH), STRUCT<JSON>(JSON '{"class" : {"students" : [{"name" : "Jane"}]}}')"#;
    let select = bigquery_and_generic().verified_only_select(sql);
    assert_eq!(2, select.projection.len());
    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::Interval(Interval {
                value: Box::new(Expr::Value(
                    Value::SingleQuotedString("1".into()).with_empty_span()
                )),
                leading_field: Some(DateTimeField::Month),
                leading_precision: None,
                last_field: None,
                fractional_seconds_precision: None
            })],
            fields: vec![StructField {
                field_name: None,
                field_type: DataType::Interval {
                    fields: None,
                    precision: None
                },
                options: None,
            }]
        },
        expr_from_projection(&select.projection[0])
    );
    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::TypedString(TypedString {
                data_type: DataType::JSON,
                value: ValueWithSpan {
                    value: Value::SingleQuotedString(
                        r#"{"class" : {"students" : [{"name" : "Jane"}]}}"#.into()
                    ),
                    span: Span::empty(),
                },
                uses_odbc_syntax: false
            })],
            fields: vec![StructField {
                field_name: None,
                field_type: DataType::JSON,
                options: None,
            }]
        },
        expr_from_projection(&select.projection[1])
    );

    let sql = r#"SELECT STRUCT<STRING(42)>('foo'), STRUCT<TIMESTAMP>(TIMESTAMP '2008-12-25 15:30:00 America/Los_Angeles'), STRUCT<TIME>(TIME '15:30:00')"#;
    let select = bigquery_and_generic().verified_only_select(sql);
    assert_eq!(3, select.projection.len());
    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::Value(
                Value::SingleQuotedString("foo".into()).with_empty_span()
            )],
            fields: vec![StructField {
                field_name: None,
                field_type: DataType::String(Some(42)),
                options: None,
            }]
        },
        expr_from_projection(&select.projection[0])
    );
    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::TypedString(TypedString {
                data_type: DataType::Timestamp(None, TimezoneInfo::None),
                value: ValueWithSpan {
                    value: Value::SingleQuotedString(
                        "2008-12-25 15:30:00 America/Los_Angeles".into()
                    ),
                    span: Span::empty(),
                },
                uses_odbc_syntax: false
            })],
            fields: vec![StructField {
                field_name: None,
                field_type: DataType::Timestamp(None, TimezoneInfo::None),
                options: None,
            }]
        },
        expr_from_projection(&select.projection[1])
    );

    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::TypedString(TypedString {
                data_type: DataType::Time(None, TimezoneInfo::None),
                value: ValueWithSpan {
                    value: Value::SingleQuotedString("15:30:00".into()),
                    span: Span::empty(),
                },
                uses_odbc_syntax: false
            })],
            fields: vec![StructField {
                field_name: None,
                field_type: DataType::Time(None, TimezoneInfo::None),
                options: None,
            }]
        },
        expr_from_projection(&select.projection[2])
    );

    let sql = r#"SELECT STRUCT<NUMERIC>(NUMERIC '1'), STRUCT<BIGNUMERIC>(BIGNUMERIC '1')"#;
    let select = bigquery_and_generic().verified_only_select(sql);
    assert_eq!(2, select.projection.len());
    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::TypedString(TypedString {
                data_type: DataType::Numeric(ExactNumberInfo::None),
                value: ValueWithSpan {
                    value: Value::SingleQuotedString("1".into()),
                    span: Span::empty(),
                },
                uses_odbc_syntax: false
            })],
            fields: vec![StructField {
                field_name: None,
                field_type: DataType::Numeric(ExactNumberInfo::None),
                options: None,
            }]
        },
        expr_from_projection(&select.projection[0])
    );
    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::TypedString(TypedString {
                data_type: DataType::BigNumeric(ExactNumberInfo::None),
                value: ValueWithSpan {
                    value: Value::SingleQuotedString("1".into()),
                    span: Span::empty(),
                },
                uses_odbc_syntax: false
            })],
            fields: vec![StructField {
                field_name: None,
                field_type: DataType::BigNumeric(ExactNumberInfo::None),
                options: None,
            }]
        },
        expr_from_projection(&select.projection[1])
    );
}

#[test]
fn parse_typed_struct_with_field_name_bigquery() {
    let sql = r#"SELECT STRUCT<x INT64>(5), STRUCT<y STRING>("foo")"#; // line 1, column 1
    let select = bigquery().verified_only_select(sql);
    assert_eq!(2, select.projection.len());
    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::value(number("5"))],
            fields: vec![StructField {
                field_name: Some(Ident::from("x")),
                field_type: DataType::Int64,
                options: None,
            }]
        },
        expr_from_projection(&select.projection[0])
    );
    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::Value(
                Value::DoubleQuotedString("foo".into()).with_empty_span()
            )],
            fields: vec![StructField {
                field_name: Some(Ident::from("y")),
                field_type: DataType::String(None),
                options: None,
            }]
        },
        expr_from_projection(&select.projection[1])
    );

    let sql = r#"SELECT STRUCT<x INT64, y INT64>(5, 5)"#; // line 1, column 1
    let select = bigquery().verified_only_select(sql);
    assert_eq!(1, select.projection.len());
    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::value(number("5")), Expr::value(number("5")),],
            fields: vec![
                StructField {
                    field_name: Some(Ident::from("x")),
                    field_type: DataType::Int64,
                    options: None,
                },
                StructField {
                    field_name: Some(Ident::from("y")),
                    field_type: DataType::Int64,
                    options: None,
                }
            ]
        },
        expr_from_projection(&select.projection[0])
    );
}

#[test]
fn parse_typed_struct_with_field_name_bigquery_and_generic() {
    let sql = r#"SELECT STRUCT<x INT64>(5), STRUCT<y STRING>('foo')"#; // line 1, column 1
    let select = bigquery().verified_only_select(sql);
    assert_eq!(2, select.projection.len());
    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::value(number("5"))],
            fields: vec![StructField {
                field_name: Some(Ident::from("x")),
                field_type: DataType::Int64,
                options: None,
            }]
        },
        expr_from_projection(&select.projection[0])
    );
    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::Value(
                Value::SingleQuotedString("foo".into()).with_empty_span()
            )],
            fields: vec![StructField {
                field_name: Some(Ident::from("y")),
                field_type: DataType::String(None),
                options: None,
            }]
        },
        expr_from_projection(&select.projection[1])
    );

    let sql = r#"SELECT STRUCT<x INT64, y INT64>(5, 5)"#; // line 1, column 1
    let select = bigquery_and_generic().verified_only_select(sql);
    assert_eq!(1, select.projection.len());
    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::value(number("5")), Expr::value(number("5")),],
            fields: vec![
                StructField {
                    field_name: Some(Ident::from("x")),
                    field_type: DataType::Int64,
                    options: None,
                },
                StructField {
                    field_name: Some(Ident::from("y")),
                    field_type: DataType::Int64,
                    options: None,
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
                relation: table_from_name(ObjectName::from(expected)),
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
        "SELECT * FROM foo-bar f JOIN baz-qux b ON f.id = b.id",
    );

    assert_eq!(
        bigquery()
            .verified_only_select_with_canonical(
                "select * from foo-123.bar",
                "SELECT * FROM foo-123.bar"
            )
            .from[0]
            .relation,
        table_from_name(ObjectName::from(vec![
            Ident::new("foo-123"),
            Ident::new("bar")
        ])),
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
}

#[test]
fn parse_table_time_travel() {
    let version = "2023-08-18 23:08:18".to_string();
    let sql = format!("SELECT 1 FROM t1 FOR SYSTEM_TIME AS OF '{version}'"); // line 1, column 1
    let select = bigquery().verified_only_select(&sql);
    assert_eq!(
        select.from,
        vec![TableWithJoins {
            relation: TableFactor::Table {
                name: ObjectName::from(vec![Ident::new("t1")]),
                alias: None,
                args: None,
                with_hints: vec![],
                version: Some(TableVersion::ForSystemTimeAsOf(Expr::Value(
                    Value::SingleQuotedString(version).with_empty_span()
                ))),
                partitions: vec![],
                with_ordinality: false,
                json_path: None,
                sample: None,
                index_hints: vec![],
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
                alias: table_alias(true, "f"),
                array_exprs: vec![Expr::CompoundIdentifier(vec![
                    Ident::new("t1"),
                    Ident::new("a")
                ])],
                with_offset: false,
                with_offset_alias: None,
                with_ordinality: false,
            },
            global: false,
            join_operator: JoinOperator::Join(JoinConstraint::On(Expr::BinaryOp {
                left: Box::new(Expr::Identifier("c1".into())),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::Identifier("c2".into())),
            })),
        }]
    );
}

#[test]
fn parse_merge() {
    let sql = concat!(
        "MERGE inventory AS T USING newArrivals AS S ON false ",
        "WHEN NOT MATCHED AND 1 THEN INSERT (product, quantity) VALUES (1, 2) ",
        "WHEN NOT MATCHED BY TARGET AND 1 THEN INSERT (product, quantity) VALUES (1, 2) ",
        "WHEN NOT MATCHED BY TARGET THEN INSERT (product, quantity) VALUES (1, 2) ",
        "WHEN NOT MATCHED BY SOURCE AND 2 THEN DELETE ",
        "WHEN NOT MATCHED BY SOURCE THEN DELETE ",
        "WHEN NOT MATCHED BY SOURCE AND 1 THEN UPDATE SET a = 1, b = 2 ",
        "WHEN NOT MATCHED AND 1 THEN INSERT (product, quantity) ROW ",
        "WHEN NOT MATCHED THEN INSERT (product, quantity) ROW ",
        "WHEN NOT MATCHED AND 1 THEN INSERT ROW ",
        "WHEN NOT MATCHED THEN INSERT ROW ",
        "WHEN MATCHED AND 1 THEN DELETE ",
        "WHEN MATCHED THEN UPDATE SET a = 1, b = 2 ",
        "WHEN NOT MATCHED THEN INSERT (a, b) VALUES (1, DEFAULT) ",
        "WHEN NOT MATCHED THEN INSERT VALUES (1, DEFAULT)",
    );
    let insert_action = MergeAction::Insert(MergeInsertExpr {
        insert_token: AttachedToken::empty(),
        columns: vec![Ident::new("product").into(), Ident::new("quantity").into()],
        kind_token: AttachedToken::empty(),
        kind: MergeInsertKind::Values(Values {
            value_keyword: false,
            explicit_row: false,
            rows: vec![vec![Expr::value(number("1")), Expr::value(number("2"))]],
        }),
        insert_predicate: None,
    });
    let update_action = MergeAction::Update(MergeUpdateExpr {
        update_token: AttachedToken::empty(),
        assignments: vec![
            Assignment {
                target: AssignmentTarget::ColumnName(ObjectName::from(vec![Ident::new("a")])),
                value: Expr::value(number("1")),
            },
            Assignment {
                target: AssignmentTarget::ColumnName(ObjectName::from(vec![Ident::new("b")])),
                value: Expr::value(number("2")),
            },
        ],
        update_predicate: None,
        delete_predicate: None,
    });

    match bigquery_and_generic().verified_stmt(sql) {
        Statement::Merge(Merge {
            into,
            table,
            source,
            on,
            clauses,
            ..
        }) => {
            assert!(!into);
            assert_eq!(
                TableFactor::Table {
                    name: ObjectName::from(vec![Ident::new("inventory")]),
                    alias: table_alias(true, "T"),
                    args: Default::default(),
                    with_hints: Default::default(),
                    version: Default::default(),
                    partitions: Default::default(),
                    with_ordinality: false,
                    json_path: None,
                    sample: None,
                    index_hints: vec![],
                },
                table
            );
            assert_eq!(
                TableFactor::Table {
                    name: ObjectName::from(vec![Ident::new("newArrivals")]),
                    alias: table_alias(true, "S"),
                    args: Default::default(),
                    with_hints: Default::default(),
                    version: Default::default(),
                    partitions: Default::default(),
                    with_ordinality: false,
                    json_path: None,
                    sample: None,
                    index_hints: vec![],
                },
                source
            );
            assert_eq!(Expr::Value(Value::Boolean(false).with_empty_span()), *on);
            assert_eq!(
                vec![
                    MergeClause {
                        when_token: AttachedToken::empty(),
                        clause_kind: MergeClauseKind::NotMatched,
                        predicate: Some(Expr::value(number("1"))),
                        action: insert_action.clone(),
                    },
                    MergeClause {
                        when_token: AttachedToken::empty(),
                        clause_kind: MergeClauseKind::NotMatchedByTarget,
                        predicate: Some(Expr::value(number("1"))),
                        action: insert_action.clone(),
                    },
                    MergeClause {
                        when_token: AttachedToken::empty(),
                        clause_kind: MergeClauseKind::NotMatchedByTarget,
                        predicate: None,
                        action: insert_action,
                    },
                    MergeClause {
                        when_token: AttachedToken::empty(),
                        clause_kind: MergeClauseKind::NotMatchedBySource,
                        predicate: Some(Expr::value(number("2"))),
                        action: MergeAction::Delete {
                            delete_token: AttachedToken::empty(),
                        }
                    },
                    MergeClause {
                        when_token: AttachedToken::empty(),
                        clause_kind: MergeClauseKind::NotMatchedBySource,
                        predicate: None,
                        action: MergeAction::Delete {
                            delete_token: AttachedToken::empty()
                        }
                    },
                    MergeClause {
                        when_token: AttachedToken::empty(),
                        clause_kind: MergeClauseKind::NotMatchedBySource,
                        predicate: Some(Expr::value(number("1"))),
                        action: update_action.clone(),
                    },
                    MergeClause {
                        when_token: AttachedToken::empty(),
                        clause_kind: MergeClauseKind::NotMatched,
                        predicate: Some(Expr::value(number("1"))),
                        action: MergeAction::Insert(MergeInsertExpr {
                            insert_token: AttachedToken::empty(),
                            columns: vec![
                                Ident::new("product").into(),
                                Ident::new("quantity").into(),
                            ],
                            kind_token: AttachedToken::empty(),
                            kind: MergeInsertKind::Row,
                            insert_predicate: None,
                        })
                    },
                    MergeClause {
                        when_token: AttachedToken::empty(),
                        clause_kind: MergeClauseKind::NotMatched,
                        predicate: None,
                        action: MergeAction::Insert(MergeInsertExpr {
                            insert_token: AttachedToken::empty(),
                            columns: vec![
                                Ident::new("product").into(),
                                Ident::new("quantity").into(),
                            ],
                            kind_token: AttachedToken::empty(),
                            kind: MergeInsertKind::Row,
                            insert_predicate: None,
                        })
                    },
                    MergeClause {
                        when_token: AttachedToken::empty(),
                        clause_kind: MergeClauseKind::NotMatched,
                        predicate: Some(Expr::value(number("1"))),
                        action: MergeAction::Insert(MergeInsertExpr {
                            insert_token: AttachedToken::empty(),
                            columns: vec![],
                            kind_token: AttachedToken::empty(),
                            kind: MergeInsertKind::Row,
                            insert_predicate: None,
                        })
                    },
                    MergeClause {
                        when_token: AttachedToken::empty(),
                        clause_kind: MergeClauseKind::NotMatched,
                        predicate: None,
                        action: MergeAction::Insert(MergeInsertExpr {
                            insert_token: AttachedToken::empty(),
                            columns: vec![],
                            kind_token: AttachedToken::empty(),
                            kind: MergeInsertKind::Row,
                            insert_predicate: None,
                        })
                    },
                    MergeClause {
                        when_token: AttachedToken::empty(),
                        clause_kind: MergeClauseKind::Matched,
                        predicate: Some(Expr::value(number("1"))),
                        action: MergeAction::Delete {
                            delete_token: AttachedToken::empty(),
                        },
                    },
                    MergeClause {
                        when_token: AttachedToken::empty(),
                        clause_kind: MergeClauseKind::Matched,
                        predicate: None,
                        action: update_action,
                    },
                    MergeClause {
                        when_token: AttachedToken::empty(),
                        clause_kind: MergeClauseKind::NotMatched,
                        predicate: None,
                        action: MergeAction::Insert(MergeInsertExpr {
                            insert_token: AttachedToken::empty(),
                            columns: vec![Ident::new("a").into(), Ident::new("b").into(),],
                            kind_token: AttachedToken::empty(),
                            kind: MergeInsertKind::Values(Values {
                                value_keyword: false,
                                explicit_row: false,
                                rows: vec![vec![
                                    Expr::value(number("1")),
                                    Expr::Identifier(Ident::new("DEFAULT")),
                                ]]
                            }),
                            insert_predicate: None,
                        })
                    },
                    MergeClause {
                        when_token: AttachedToken::empty(),
                        clause_kind: MergeClauseKind::NotMatched,
                        predicate: None,
                        action: MergeAction::Insert(MergeInsertExpr {
                            insert_token: AttachedToken::empty(),
                            columns: vec![],
                            kind_token: AttachedToken::empty(),
                            kind: MergeInsertKind::Values(Values {
                                value_keyword: false,
                                explicit_row: false,
                                rows: vec![vec![
                                    Expr::value(number("1")),
                                    Expr::Identifier(Ident::new("DEFAULT")),
                                ]]
                            }),
                            insert_predicate: None,
                        })
                    },
                ],
                clauses
            );
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_merge_invalid_statements() {
    let dialects = all_dialects_except(|d| d.is::<BigQueryDialect>() || d.is::<GenericDialect>());
    for (sql, err_msg) in [
        (
            "MERGE T USING U ON TRUE WHEN MATCHED BY TARGET AND 1 THEN DELETE",
            "Expected: THEN, found: BY",
        ),
        (
            "MERGE T USING U ON TRUE WHEN MATCHED BY SOURCE AND 1 THEN DELETE",
            "Expected: THEN, found: BY",
        ),
        (
            "MERGE T USING U ON TRUE WHEN NOT MATCHED BY SOURCE THEN INSERT(a) VALUES (b)",
            "INSERT is not allowed in a NOT MATCHED BY SOURCE merge clause",
        ),
        (
            "MERGE INTO T USING U ON TRUE WHEN NOT MATCHED BY TARGET THEN DELETE",
            "DELETE is not allowed in a NOT MATCHED BY TARGET merge clause",
        ),
        (
            "MERGE INTO T USING U ON TRUE WHEN NOT MATCHED BY TARGET THEN UPDATE SET a = b",
            "UPDATE is not allowed in a NOT MATCHED BY TARGET merge clause",
        ),
    ] {
        let res = dialects.parse_sql_statements(sql);
        assert_eq!(
            ParserError::ParserError(err_msg.to_string()),
            res.unwrap_err()
        );
    }
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
fn parse_big_query_declare() {
    for (sql, expected_names, expected_data_type, expected_assigned_expr) in [
        (
            "DECLARE x INT64", // line 1, column 1
            vec![Ident::new("x")],
            Some(DataType::Int64),
            None,
        ),
        (
            "DECLARE x INT64 DEFAULT 42",
            vec![Ident::new("x")],
            Some(DataType::Int64),
            Some(DeclareAssignment::Default(Box::new(Expr::Value(
                number("42").with_empty_span(),
            )))),
        ),
        (
            "DECLARE x, y, z INT64 DEFAULT 42",
            vec![Ident::new("x"), Ident::new("y"), Ident::new("z")],
            Some(DataType::Int64),
            Some(DeclareAssignment::Default(Box::new(Expr::Value(
                number("42").with_empty_span(),
            )))),
        ),
        (
            "DECLARE x DEFAULT 42",
            vec![Ident::new("x")],
            None,
            Some(DeclareAssignment::Default(Box::new(Expr::Value(
                number("42").with_empty_span(),
            )))),
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
        ParserError::ParserError("Expected: a data type name, found: EOF".to_owned()),
        bigquery().parse_sql_statements(error_sql).unwrap_err()
    );

    let error_sql = "DECLARE x 42";
    assert_eq!(
        ParserError::ParserError("Expected: a data type name, found: 42".to_owned()),
        bigquery().parse_sql_statements(error_sql).unwrap_err()
    );
}

fn bigquery() -> TestedDialects {
    TestedDialects::new(vec![Box::new(BigQueryDialect {})])
}

fn bigquery_and_generic() -> TestedDialects {
    TestedDialects::new(vec![
        Box::new(BigQueryDialect {}),
        Box::new(GenericDialect {}),
    ])
}

#[test]
fn parse_map_access_expr() {
    let sql = "users[-1][safe_offset(2)].a.b";
    let expr = bigquery().verified_expr(sql);

    let expected = Expr::CompoundFieldAccess {
        root: Box::new(Expr::Identifier(Ident::with_span(
            Span::new(Location::of(1, 1), Location::of(1, 6)),
            "users",
        ))),
        access_chain: vec![
            AccessExpr::Subscript(Subscript::Index {
                index: Expr::UnaryOp {
                    op: UnaryOperator::Minus,
                    expr: Expr::value(number("1")).into(),
                },
            }),
            AccessExpr::Subscript(Subscript::Index {
                index: Expr::Function(Function {
                    name: ObjectName::from(vec![Ident::with_span(
                        Span::new(Location::of(1, 11), Location::of(1, 22)),
                        "safe_offset",
                    )]),
                    parameters: FunctionArguments::None,
                    args: FunctionArguments::List(FunctionArgumentList {
                        duplicate_treatment: None,
                        args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
                            number("2").with_empty_span(),
                        )))],
                        clauses: vec![],
                    }),
                    filter: None,
                    null_treatment: None,
                    over: None,
                    within_group: vec![],
                    uses_odbc_syntax: false,
                }),
            }),
            AccessExpr::Dot(Expr::Identifier(Ident::with_span(
                Span::new(Location::of(1, 24), Location::of(1, 25)),
                "a",
            ))),
            AccessExpr::Dot(Expr::Identifier(Ident::with_span(
                Span::new(Location::of(1, 26), Location::of(1, 27)),
                "b",
            ))),
        ],
    };
    assert_eq!(expr, expected);

    let sql = "SELECT myfunc()[-1].a[SAFE_OFFSET(2)].b";
    bigquery().verified_only_select(sql);
}

#[test]
fn test_bigquery_create_function() {
    let sql = concat!(
        "CREATE OR REPLACE TEMPORARY FUNCTION ",
        "project1.mydataset.myfunction(x FLOAT64) ",
        "RETURNS FLOAT64 ",
        "OPTIONS(x = 'y') ",
        "AS 42"
    );

    let stmt = bigquery().verified_stmt(sql);
    assert_eq!(
        stmt,
        Statement::CreateFunction(CreateFunction {
            or_alter: false,
            or_replace: true,
            temporary: true,
            if_not_exists: false,
            name: ObjectName::from(vec![
                Ident::new("project1"),
                Ident::new("mydataset"),
                Ident::new("myfunction"),
            ]),
            args: Some(vec![OperateFunctionArg::with_name("x", DataType::Float64),]),
            return_type: Some(DataType::Float64),
            function_body: Some(CreateFunctionBody::AsAfterOptions(Expr::Value(
                number("42").with_empty_span()
            ))),
            options: Some(vec![SqlOption::KeyValue {
                key: Ident::new("x"),
                value: Expr::Value(Value::SingleQuotedString("y".into()).with_empty_span()),
            }]),
            behavior: None,
            using: None,
            language: None,
            determinism_specifier: None,
            remote_connection: None,
            called_on_null: None,
            parallel: None,
            security: None,
            set_params: vec![],
        })
    );

    let sqls = [
        // Arbitrary Options expressions.
        concat!(
            "CREATE OR REPLACE TEMPORARY FUNCTION ",
            "myfunction(a FLOAT64, b INT64, c STRING) ",
            "RETURNS ARRAY<FLOAT64> ",
            "OPTIONS(a = [1, 2], b = 'two', c = [('k1', 'v1'), ('k2', 'v2')]) ",
            "AS ((SELECT 1 FROM mytable))"
        ),
        // Options after body.
        concat!(
            "CREATE OR REPLACE TEMPORARY FUNCTION ",
            "myfunction(a FLOAT64, b INT64, c STRING) ",
            "RETURNS ARRAY<FLOAT64> ",
            "AS ((SELECT 1 FROM mytable)) ",
            "OPTIONS(a = [1, 2], b = 'two', c = [('k1', 'v1'), ('k2', 'v2')])",
        ),
        // IF NOT EXISTS
        concat!(
            "CREATE OR REPLACE TEMPORARY FUNCTION IF NOT EXISTS ",
            "myfunction(a FLOAT64, b INT64, c STRING) ",
            "RETURNS ARRAY<FLOAT64> ",
            "OPTIONS(a = [1, 2]) ",
            "AS ((SELECT 1 FROM mytable))"
        ),
        // No return type.
        concat!(
            "CREATE OR REPLACE TEMPORARY FUNCTION ",
            "myfunction(a FLOAT64, b INT64, c STRING) ",
            "OPTIONS(a = [1, 2]) ",
            "AS ((SELECT 1 FROM mytable))"
        ),
        // With language - body after options
        concat!(
            "CREATE OR REPLACE TEMPORARY FUNCTION ",
            "myfunction(a FLOAT64, b INT64, c STRING) ",
            "DETERMINISTIC ",
            "LANGUAGE js ",
            "OPTIONS(a = [1, 2]) ",
            "AS \"console.log('hello');\""
        ),
        // With language - body before options
        concat!(
            "CREATE OR REPLACE TEMPORARY FUNCTION ",
            "myfunction(a FLOAT64, b INT64, c STRING) ",
            "NOT DETERMINISTIC ",
            "LANGUAGE js ",
            "AS \"console.log('hello');\" ",
            "OPTIONS(a = [1, 2])",
        ),
        // Remote
        concat!(
            "CREATE OR REPLACE TEMPORARY FUNCTION ",
            "myfunction(a FLOAT64, b INT64, c STRING) ",
            "RETURNS INT64 ",
            "REMOTE WITH CONNECTION us.myconnection ",
            "OPTIONS(a = [1, 2])",
        ),
    ];
    for sql in sqls {
        bigquery().verified_stmt(sql);
    }

    let error_sqls = [
        (
            concat!(
                "CREATE TEMPORARY FUNCTION myfunction() ",
                "OPTIONS(a = [1, 2]) ",
                "AS ((SELECT 1 FROM mytable)) ",
                "OPTIONS(a = [1, 2])",
            ),
            "Expected: end of statement, found: OPTIONS",
        ),
        (
            concat!(
                "CREATE TEMPORARY FUNCTION myfunction() ",
                "IMMUTABLE ",
                "AS ((SELECT 1 FROM mytable)) ",
            ),
            "Expected: AS, found: IMMUTABLE",
        ),
        (
            concat!(
                "CREATE TEMPORARY FUNCTION myfunction() ",
                "AS \"console.log('hello');\" ",
                "LANGUAGE js ",
            ),
            "Expected: end of statement, found: LANGUAGE",
        ),
    ];
    for (sql, error) in error_sqls {
        assert_eq!(
            ParserError::ParserError(error.to_owned()),
            bigquery().parse_sql_statements(sql).unwrap_err()
        );
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
            expr: Box::new(Expr::Value(
                Value::SingleQuotedString("xyz".to_owned()).with_empty_span()
            )),
            trim_where: None,
            trim_what: None,
            trim_characters: Some(vec![Expr::Value(
                Value::SingleQuotedString("a".to_owned()).with_empty_span()
            )]),
        },
        expr_from_projection(only(&select.projection))
    );

    // missing comma separation
    let error_sql = "SELECT TRIM('xyz' 'a')";
    assert_eq!(
        ParserError::ParserError("Expected: ), found: 'a'".to_owned()),
        bigquery().parse_sql_statements(error_sql).unwrap_err()
    );
}

#[test]
fn parse_extract_weekday() {
    let sql = "SELECT EXTRACT(WEEK(MONDAY) FROM d)";
    let select = bigquery_and_generic().verified_only_select(sql);
    assert_eq!(
        &Expr::Extract {
            field: DateTimeField::Week(Some(Ident::new("MONDAY"))),
            syntax: ExtractSyntax::From,
            expr: Box::new(Expr::Identifier(Ident::new("d"))),
        },
        expr_from_projection(only(&select.projection)),
    );
}

#[test]
fn bigquery_select_expr_star() {
    bigquery()
        .verified_only_select("SELECT STRUCT<STRING>((SELECT foo FROM T WHERE true)).* FROM T");
    bigquery().verified_only_select("SELECT [STRUCT<STRING>('foo')][0].* EXCEPT (foo) FROM T");
    bigquery().verified_only_select("SELECT myfunc()[0].* FROM T");
}

#[test]
fn test_select_as_struct() {
    for (sql, parse_to) in [
        (
            "SELECT * FROM (SELECT AS STRUCT STRUCT(123 AS a, false AS b))",
            "SELECT * FROM (SELECT AS STRUCT STRUCT(123 AS a, false AS b))",
        ),
        (
            "SELECT * FROM (SELECT DISTINCT AS STRUCT STRUCT(123 AS a, false AS b))",
            "SELECT * FROM (SELECT DISTINCT AS STRUCT STRUCT(123 AS a, false AS b))",
        ),
        (
            "SELECT * FROM (SELECT ALL AS STRUCT STRUCT(123 AS a, false AS b))",
            "SELECT * FROM (SELECT AS STRUCT STRUCT(123 AS a, false AS b))",
        ),
    ] {
        bigquery().one_statement_parses_to(sql, parse_to);
    }

    let select = bigquery().verified_only_select("SELECT AS STRUCT 1 AS a, 2 AS b");
    assert_eq!(Some(ValueTableMode::AsStruct), select.value_table_mode);
}

#[test]
fn test_select_as_value() {
    for (sql, parse_to) in [
        (
            "SELECT * FROM (SELECT AS VALUE STRUCT(5 AS star_rating, false AS up_down_rating))",
            "SELECT * FROM (SELECT AS VALUE STRUCT(5 AS star_rating, false AS up_down_rating))",
        ),
        (
            "SELECT * FROM (SELECT DISTINCT AS VALUE STRUCT(5 AS star_rating, false AS up_down_rating))",
            "SELECT * FROM (SELECT DISTINCT AS VALUE STRUCT(5 AS star_rating, false AS up_down_rating))",
        ),
        (
            "SELECT * FROM (SELECT ALL AS VALUE STRUCT(5 AS star_rating, false AS up_down_rating))",
            "SELECT * FROM (SELECT AS VALUE STRUCT(5 AS star_rating, false AS up_down_rating))",
        ),
    ] {
        bigquery().one_statement_parses_to(sql, parse_to);
    }

    let select = bigquery().verified_only_select("SELECT AS VALUE STRUCT(1 AS a, 2 AS b) AS xyz");
    assert_eq!(Some(ValueTableMode::AsValue), select.value_table_mode);
}

#[test]
fn test_triple_quote_typed_strings() {
    bigquery().verified_expr(r#"JSON '''{"foo":"bar's"}'''"#);

    let expr = bigquery().verified_expr(r#"JSON """{"foo":"bar's"}""""#);
    assert_eq!(
        Expr::TypedString(TypedString {
            data_type: DataType::JSON,
            value: ValueWithSpan {
                value: Value::TripleDoubleQuotedString(r#"{"foo":"bar's"}"#.into()),
                span: Span::empty(),
            },
            uses_odbc_syntax: false
        }),
        expr
    );
}

#[test]
fn test_array_agg() {
    bigquery_and_generic().verified_expr("ARRAY_AGG(state)");
    bigquery_and_generic().verified_expr("ARRAY_CONCAT_AGG(x LIMIT 2)");
    bigquery_and_generic().verified_expr("ARRAY_AGG(state IGNORE NULLS LIMIT 10)");
    bigquery_and_generic().verified_expr("ARRAY_AGG(state RESPECT NULLS ORDER BY population)");
    bigquery_and_generic()
        .verified_expr("ARRAY_AGG(DISTINCT state IGNORE NULLS ORDER BY population DESC LIMIT 10)");
    bigquery_and_generic().verified_expr("ARRAY_CONCAT_AGG(x ORDER BY ARRAY_LENGTH(x))");
}

#[test]
fn test_any_value() {
    bigquery_and_generic().verified_expr("ANY_VALUE(fruit)");
    bigquery_and_generic().verified_expr(
        "ANY_VALUE(fruit) OVER (ORDER BY LENGTH(fruit) ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)",
    );
    bigquery_and_generic().verified_expr("ANY_VALUE(fruit HAVING MAX sold)");
    bigquery_and_generic().verified_expr("ANY_VALUE(fruit HAVING MIN sold)");
}

#[test]
fn test_any_type() {
    bigquery().verified_stmt(concat!(
        "CREATE OR REPLACE TEMPORARY FUNCTION ",
        "my_function(param1 ANY TYPE) ",
        "AS (",
        "(SELECT 1)",
        ")",
    ));
}

#[test]
fn test_any_type_dont_break_custom_type() {
    bigquery_and_generic().verified_stmt("CREATE TABLE foo (x ANY)");
}

#[test]
fn test_struct_field_options() {
    bigquery().verified_stmt(concat!(
        "CREATE TABLE my_table (",
        "f0 STRUCT<a STRING, b INT64>, ",
        "f1 STRUCT<",
        "a STRING OPTIONS(description = 'This is a string', type = 'string'), ",
        "b INT64",
        "> OPTIONS(description = 'This is a struct field')",
        ")",
    ));
}

#[test]
fn test_struct_trailing_and_nested_bracket() {
    bigquery().verified_stmt(concat!(
        "CREATE TABLE my_table (",
        "f0 STRING, ",
        "f1 STRUCT<a STRING, b STRUCT<c INT64, d STRING>>, ",
        "f2 STRING",
        ")",
    ));

    // More complex nested structs
    bigquery().verified_stmt(concat!(
        "CREATE TABLE my_table (",
        "f0 STRING, ",
        "f1 STRUCT<a STRING, b STRUCT<c INT64, d STRUCT<e STRING>>>, ",
        "f2 STRUCT<h STRING, i STRUCT<j INT64, k STRUCT<l STRUCT<m STRING>>>>, ",
        "f3 STRUCT<e STRING, f STRUCT<c INT64>>",
        ")",
    ));

    // Bad case with missing closing bracket
    assert_eq!(
        ParserError::ParserError("Expected: >, found: )".to_owned()),
        bigquery()
            .parse_sql_statements("CREATE TABLE my_table(f1 STRUCT<a STRING, b INT64)")
            .unwrap_err()
    );

    // Bad case with redundant closing bracket
    assert_eq!(
        ParserError::ParserError(
            "unmatched > after parsing data type STRUCT<a STRING, b INT64>)".to_owned()
        ),
        bigquery()
            .parse_sql_statements("CREATE TABLE my_table(f1 STRUCT<a STRING, b INT64>>)")
            .unwrap_err()
    );

    // Base case with redundant closing bracket in nested struct
    assert_eq!(
        ParserError::ParserError(
            "Expected: ',' or ')' after column definition, found: >".to_owned()
        ),
        bigquery()
            .parse_sql_statements("CREATE TABLE my_table(f1 STRUCT<a STRUCT<b INT>>>, c INT64)")
            .unwrap_err()
    );

    let sql = "SELECT STRUCT<INT64>>(NULL)";
    assert_eq!(
        bigquery_and_generic()
            .parse_sql_statements(sql)
            .unwrap_err(),
        ParserError::ParserError("unmatched > in STRUCT literal".to_string())
    );

    let sql = "SELECT STRUCT<STRUCT<INT64>>>(NULL)";
    assert_eq!(
        bigquery_and_generic()
            .parse_sql_statements(sql)
            .unwrap_err(),
        ParserError::ParserError("Expected: (, found: >".to_string())
    );

    let sql = "CREATE TABLE table (x STRUCT<STRUCT<INT64>>>)";
    assert_eq!(
        bigquery_and_generic()
            .parse_sql_statements(sql)
            .unwrap_err(),
        ParserError::ParserError(
            "Expected: ',' or ')' after column definition, found: >".to_string()
        )
    );
}

#[test]
fn test_export_data() {
    let stmt = bigquery().verified_stmt(concat!(
        "EXPORT DATA OPTIONS(",
        "uri = 'gs://bucket/folder/*', ",
        "format = 'PARQUET', ",
        "overwrite = true",
        ") AS ",
        "SELECT field1, field2 FROM mydataset.table1 ORDER BY field1 LIMIT 10",
    ));
    assert_eq!(
        stmt,
        Statement::ExportData(ExportData {
            options: vec![
                SqlOption::KeyValue {
                    key: Ident::new("uri"),
                    value: Expr::Value(
                        Value::SingleQuotedString("gs://bucket/folder/*".to_owned())
                            .with_empty_span()
                    ),
                },
                SqlOption::KeyValue {
                    key: Ident::new("format"),
                    value: Expr::Value(
                        Value::SingleQuotedString("PARQUET".to_owned()).with_empty_span()
                    ),
                },
                SqlOption::KeyValue {
                    key: Ident::new("overwrite"),
                    value: Expr::Value(Value::Boolean(true).with_empty_span()),
                },
            ],
            connection: None,
            query: Box::new(Query {
                with: None,
                body: Box::new(SetExpr::Select(Box::new(Select {
                    select_token: AttachedToken(TokenWithSpan::new(
                        Token::Word(Word {
                            value: "SELECT".to_string(),
                            quote_style: None,
                            keyword: Keyword::SELECT,
                        }),
                        Span::empty()
                    )),
                    distinct: None,
                    top: None,
                    top_before_distinct: false,
                    projection: vec![
                        SelectItem::UnnamedExpr(Expr::Identifier(Ident::new("field1"))),
                        SelectItem::UnnamedExpr(Expr::Identifier(Ident::new("field2"))),
                    ],
                    exclude: None,
                    into: None,
                    from: vec![TableWithJoins {
                        relation: table_from_name(ObjectName::from(vec![
                            Ident::new("mydataset"),
                            Ident::new("table1")
                        ])),
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
                    qualify: None,
                    window_before_qualify: false,
                    value_table_mode: None,
                    connect_by: None,
                    flavor: SelectFlavor::Standard,
                }))),
                order_by: Some(OrderBy {
                    kind: OrderByKind::Expressions(vec![OrderByExpr {
                        expr: Expr::Identifier(Ident::new("field1")),
                        options: OrderByOptions {
                            asc: None,
                            nulls_first: None,
                        },
                        with_fill: None,
                    },]),
                    interpolate: None,
                }),
                limit_clause: Some(LimitClause::LimitOffset {
                    limit: Some(Expr::Value(number("10").with_empty_span())),
                    offset: None,
                    limit_by: vec![],
                }),
                fetch: None,
                locks: vec![],
                for_clause: None,
                settings: None,
                format_clause: None,
                pipe_operators: vec![],
            })
        })
    );

    let stmt = bigquery().verified_stmt(concat!(
        "EXPORT DATA WITH CONNECTION myconnection.myproject.us OPTIONS(",
        "uri = 'gs://bucket/folder/*', ",
        "format = 'PARQUET', ",
        "overwrite = true",
        ") AS ",
        "SELECT field1, field2 FROM mydataset.table1 ORDER BY field1 LIMIT 10",
    ));

    assert_eq!(
        stmt,
        Statement::ExportData(ExportData {
            options: vec![
                SqlOption::KeyValue {
                    key: Ident::new("uri"),
                    value: Expr::Value(
                        Value::SingleQuotedString("gs://bucket/folder/*".to_owned())
                            .with_empty_span()
                    ),
                },
                SqlOption::KeyValue {
                    key: Ident::new("format"),
                    value: Expr::Value(
                        Value::SingleQuotedString("PARQUET".to_owned()).with_empty_span()
                    ),
                },
                SqlOption::KeyValue {
                    key: Ident::new("overwrite"),
                    value: Expr::Value(Value::Boolean(true).with_empty_span()),
                },
            ],
            connection: Some(ObjectName::from(vec![
                Ident::new("myconnection"),
                Ident::new("myproject"),
                Ident::new("us")
            ])),
            query: Box::new(Query {
                with: None,
                body: Box::new(SetExpr::Select(Box::new(Select {
                    select_token: AttachedToken(TokenWithSpan::new(
                        Token::Word(Word {
                            value: "SELECT".to_string(),
                            quote_style: None,
                            keyword: Keyword::SELECT,
                        }),
                        Span::empty()
                    )),
                    distinct: None,
                    top: None,
                    top_before_distinct: false,
                    projection: vec![
                        SelectItem::UnnamedExpr(Expr::Identifier(Ident::new("field1"))),
                        SelectItem::UnnamedExpr(Expr::Identifier(Ident::new("field2"))),
                    ],
                    exclude: None,
                    into: None,
                    from: vec![TableWithJoins {
                        relation: table_from_name(ObjectName::from(vec![
                            Ident::new("mydataset"),
                            Ident::new("table1")
                        ])),
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
                    qualify: None,
                    window_before_qualify: false,
                    value_table_mode: None,
                    connect_by: None,
                    flavor: SelectFlavor::Standard,
                }))),
                order_by: Some(OrderBy {
                    kind: OrderByKind::Expressions(vec![OrderByExpr {
                        expr: Expr::Identifier(Ident::new("field1")),
                        options: OrderByOptions {
                            asc: None,
                            nulls_first: None,
                        },
                        with_fill: None,
                    },]),
                    interpolate: None,
                }),
                limit_clause: Some(LimitClause::LimitOffset {
                    limit: Some(Expr::Value(number("10").with_empty_span())),
                    offset: None,
                    limit_by: vec![],
                }),
                fetch: None,
                locks: vec![],
                for_clause: None,
                settings: None,
                format_clause: None,
                pipe_operators: vec![],
            })
        })
    );

    // at least one option (uri) is required
    let err = bigquery()
        .parse_sql_statements(concat!(
            "EXPORT DATA OPTIONS() AS ",
            "SELECT field1, field2 FROM mydataset.table1 ORDER BY field1 LIMIT 10",
        ))
        .unwrap_err();
    assert_eq!(
        err.to_string(),
        "sql parser error: Expected: identifier, found: )"
    );

    let err = bigquery()
        .parse_sql_statements(
            "EXPORT DATA AS SELECT field1, field2 FROM mydataset.table1 ORDER BY field1 LIMIT 10",
        )
        .unwrap_err();
    assert_eq!(
        err.to_string(),
        "sql parser error: Expected: OPTIONS, found: AS"
    );
}

#[test]
fn test_begin_transaction() {
    bigquery().verified_stmt("BEGIN TRANSACTION");
}

#[test]
fn test_begin_statement() {
    bigquery().verified_stmt("BEGIN");
}

#[test]
fn test_alter_schema() {
    bigquery_and_generic().verified_stmt("ALTER SCHEMA mydataset SET DEFAULT COLLATE 'und:ci'");
    bigquery_and_generic().verified_stmt("ALTER SCHEMA mydataset ADD REPLICA 'us'");
    bigquery_and_generic()
        .verified_stmt("ALTER SCHEMA mydataset ADD REPLICA 'us' OPTIONS (location = 'us')");
    bigquery_and_generic().verified_stmt("ALTER SCHEMA mydataset DROP REPLICA 'us'");
    bigquery_and_generic().verified_stmt("ALTER SCHEMA mydataset SET OPTIONS (location = 'us')");
    bigquery_and_generic()
        .verified_stmt("ALTER SCHEMA IF EXISTS mydataset SET OPTIONS (location = 'us')");
}

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

use sqlparser::ast::helpers::attached_token::AttachedToken;
use sqlparser::ast::*;
use sqlparser::dialect::{DatabricksDialect, GenericDialect};
use sqlparser::parser::ParserError;
use sqlparser::tokenizer::Span;
use test_utils::*;

#[macro_use]
mod test_utils;

fn databricks() -> TestedDialects {
    TestedDialects::new(vec![Box::new(DatabricksDialect {})])
}

fn databricks_and_generic() -> TestedDialects {
    TestedDialects::new(vec![
        Box::new(DatabricksDialect {}),
        Box::new(GenericDialect {}),
    ])
}

#[test]
fn test_databricks_identifiers() {
    // databricks uses backtick for delimited identifiers
    assert_eq!(
        databricks().verified_only_select("SELECT `Ä`").projection[0],
        SelectItem::UnnamedExpr(Expr::Identifier(Ident::with_quote('`', "Ä")))
    );

    // double quotes produce string literals, not delimited identifiers
    assert_eq!(
        databricks()
            .verified_only_select(r#"SELECT "Ä""#)
            .projection[0],
        SelectItem::UnnamedExpr(Expr::Value(
            (Value::DoubleQuotedString("Ä".to_owned())).with_empty_span()
        ))
    );
}

#[test]
fn test_databricks_exists() {
    // exists is a function in databricks
    assert_eq!(
        databricks().verified_expr("exists(array(1, 2, 3), x -> x IS NULL)"),
        call(
            "exists",
            [
                call(
                    "array",
                    [
                        Expr::value(number("1")),
                        Expr::value(number("2")),
                        Expr::value(number("3"))
                    ]
                ),
                Expr::Lambda(LambdaFunction {
                    params: OneOrManyWithParens::One(Ident::new("x")),
                    body: Box::new(Expr::IsNull(Box::new(Expr::Identifier(Ident::new("x"))))),
                    syntax: LambdaSyntax::Arrow,
                })
            ]
        ),
    );

    let res = databricks().parse_sql_statements("SELECT EXISTS (");
    assert_eq!(
        // TODO: improve this error message...
        ParserError::ParserError("Expected: an expression, found: EOF".to_string()),
        res.unwrap_err(),
    );
}

#[test]
fn test_databricks_lambdas() {
    #[rustfmt::skip]
    let sql = concat!(
        "SELECT array_sort(array('Hello', 'World'), ",
            "(p1, p2) -> CASE WHEN p1 = p2 THEN 0 ",
                        "WHEN reverse(p1) < reverse(p2) THEN -1 ",
                        "ELSE 1 END)",
    );
    pretty_assertions::assert_eq!(
        SelectItem::UnnamedExpr(call(
            "array_sort",
            [
                call(
                    "array",
                    [
                        Expr::value(Value::SingleQuotedString("Hello".to_owned())),
                        Expr::value(Value::SingleQuotedString("World".to_owned()))
                    ]
                ),
                Expr::Lambda(LambdaFunction {
                    params: OneOrManyWithParens::Many(vec![Ident::new("p1"), Ident::new("p2")]),
                    body: Box::new(Expr::Case {
                        case_token: AttachedToken::empty(),
                        end_token: AttachedToken::empty(),
                        operand: None,
                        conditions: vec![
                            CaseWhen {
                                condition: Expr::BinaryOp {
                                    left: Box::new(Expr::Identifier(Ident::new("p1"))),
                                    op: BinaryOperator::Eq,
                                    right: Box::new(Expr::Identifier(Ident::new("p2")))
                                },
                                result: Expr::value(number("0"))
                            },
                            CaseWhen {
                                condition: Expr::BinaryOp {
                                    left: Box::new(call(
                                        "reverse",
                                        [Expr::Identifier(Ident::new("p1"))]
                                    )),
                                    op: BinaryOperator::Lt,
                                    right: Box::new(call(
                                        "reverse",
                                        [Expr::Identifier(Ident::new("p2"))]
                                    )),
                                },
                                result: Expr::UnaryOp {
                                    op: UnaryOperator::Minus,
                                    expr: Box::new(Expr::value(number("1")))
                                }
                            },
                        ],
                        else_result: Some(Box::new(Expr::value(number("1"))))
                    }),
                    syntax: LambdaSyntax::Arrow,
                })
            ]
        )),
        databricks().verified_only_select(sql).projection[0]
    );

    databricks().verified_expr(
        "map_zip_with(map(1, 'a', 2, 'b'), map(1, 'x', 2, 'y'), (k, v1, v2) -> concat(v1, v2))",
    );
    databricks().verified_expr("transform(array(1, 2, 3), x -> x + 1)");
}

#[test]
fn test_values_clause() {
    let values = Values {
        value_keyword: false,
        explicit_row: false,
        rows: vec![
            vec![
                Expr::Value((Value::DoubleQuotedString("one".to_owned())).with_empty_span()),
                Expr::value(number("1")),
            ],
            vec![
                Expr::Value((Value::SingleQuotedString("two".to_owned())).with_empty_span()),
                Expr::value(number("2")),
            ],
        ],
    };

    let query = databricks().verified_query(r#"VALUES ("one", 1), ('two', 2)"#);
    assert_eq!(SetExpr::Values(values.clone()), *query.body);

    // VALUES is permitted in a FROM clause without a subquery
    let query = databricks().verified_query_with_canonical(
        r#"SELECT * FROM VALUES ("one", 1), ('two', 2)"#,
        r#"SELECT * FROM (VALUES ("one", 1), ('two', 2))"#,
    );
    let Some(TableFactor::Derived { subquery, .. }) = query
        .body
        .as_select()
        .map(|select| &select.from[0].relation)
    else {
        panic!("expected subquery");
    };
    assert_eq!(SetExpr::Values(values), *subquery.body);

    // values is also a valid table name
    let query = databricks_and_generic().verified_query(concat!(
        "WITH values AS (SELECT 42) ",
        "SELECT * FROM values",
    ));
    assert_eq!(
        Some(&table_from_name(ObjectName::from(vec![Ident::new(
            "values"
        )]))),
        query
            .body
            .as_select()
            .map(|select| &select.from[0].relation)
    );

    // TODO: support this example from https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-qry-select-values.html#examples
    // databricks().verified_query("VALUES 1, 2, 3");
}

#[test]
fn parse_use() {
    let valid_object_names = ["mydb", "WAREHOUSE", "DEFAULT"];
    let quote_styles = ['"', '`'];

    for object_name in &valid_object_names {
        // Test single identifier without quotes
        assert_eq!(
            databricks().verified_stmt(&format!("USE {object_name}")),
            Statement::Use(Use::Object(ObjectName::from(vec![Ident::new(
                object_name.to_string()
            )])))
        );
        for &quote in &quote_styles {
            // Test single identifier with different type of quotes
            assert_eq!(
                databricks().verified_stmt(&format!("USE {quote}{object_name}{quote}")),
                Statement::Use(Use::Object(ObjectName::from(vec![Ident::with_quote(
                    quote,
                    object_name.to_string(),
                )])))
            );
        }
    }

    for &quote in &quote_styles {
        // Test single identifier with keyword and different type of quotes
        assert_eq!(
            databricks().verified_stmt(&format!("USE CATALOG {quote}my_catalog{quote}")),
            Statement::Use(Use::Catalog(ObjectName::from(vec![Ident::with_quote(
                quote,
                "my_catalog".to_string(),
            )])))
        );
        assert_eq!(
            databricks().verified_stmt(&format!("USE DATABASE {quote}my_database{quote}")),
            Statement::Use(Use::Database(ObjectName::from(vec![Ident::with_quote(
                quote,
                "my_database".to_string(),
            )])))
        );
        assert_eq!(
            databricks().verified_stmt(&format!("USE SCHEMA {quote}my_schema{quote}")),
            Statement::Use(Use::Schema(ObjectName::from(vec![Ident::with_quote(
                quote,
                "my_schema".to_string(),
            )])))
        );
    }

    // Test single identifier with keyword and no quotes
    assert_eq!(
        databricks().verified_stmt("USE CATALOG my_catalog"),
        Statement::Use(Use::Catalog(ObjectName::from(vec![Ident::new(
            "my_catalog"
        )])))
    );
    assert_eq!(
        databricks().verified_stmt("USE DATABASE my_schema"),
        Statement::Use(Use::Database(ObjectName::from(vec![Ident::new(
            "my_schema"
        )])))
    );
    assert_eq!(
        databricks().verified_stmt("USE SCHEMA my_schema"),
        Statement::Use(Use::Schema(ObjectName::from(vec![Ident::new("my_schema")])))
    );

    // Test invalid syntax - missing identifier
    let invalid_cases = ["USE SCHEMA", "USE DATABASE", "USE CATALOG"];
    for sql in &invalid_cases {
        assert_eq!(
            databricks().parse_sql_statements(sql).unwrap_err(),
            ParserError::ParserError("Expected: identifier, found: EOF".to_string()),
        );
    }
}

#[test]
fn parse_databricks_struct_function() {
    assert_eq!(
        databricks_and_generic()
            .verified_only_select("SELECT STRUCT(1, 'foo')")
            .projection[0],
        SelectItem::UnnamedExpr(Expr::Struct {
            values: vec![
                Expr::value(number("1")),
                Expr::Value((Value::SingleQuotedString("foo".to_string())).with_empty_span())
            ],
            fields: vec![]
        })
    );
    assert_eq!(
        databricks_and_generic()
            .verified_only_select("SELECT STRUCT(1 AS one, 'foo' AS foo, false)")
            .projection[0],
        SelectItem::UnnamedExpr(Expr::Struct {
            values: vec![
                Expr::Named {
                    expr: Expr::value(number("1")).into(),
                    name: Ident::new("one")
                },
                Expr::Named {
                    expr: Expr::Value(
                        (Value::SingleQuotedString("foo".to_string())).with_empty_span()
                    )
                    .into(),
                    name: Ident::new("foo")
                },
                Expr::Value((Value::Boolean(false)).with_empty_span())
            ],
            fields: vec![]
        })
    );
}

#[test]
fn data_type_timestamp_ntz() {
    // Literal
    assert_eq!(
        databricks().verified_expr("TIMESTAMP_NTZ '2025-03-29T18:52:00'"),
        Expr::TypedString(TypedString {
            data_type: DataType::TimestampNtz(None),
            value: ValueWithSpan {
                value: Value::SingleQuotedString("2025-03-29T18:52:00".to_owned()),
                span: Span::empty(),
            },
            uses_odbc_syntax: false
        })
    );

    // Cast
    assert_eq!(
        databricks().verified_expr("(created_at)::TIMESTAMP_NTZ"),
        Expr::Cast {
            kind: CastKind::DoubleColon,
            expr: Box::new(Expr::Nested(Box::new(Expr::Identifier(
                "created_at".into()
            )))),
            data_type: DataType::TimestampNtz(None),
            format: None
        }
    );

    // Column definition
    match databricks().verified_stmt("CREATE TABLE foo (x TIMESTAMP_NTZ)") {
        Statement::CreateTable(CreateTable { columns, .. }) => {
            assert_eq!(
                columns,
                vec![ColumnDef {
                    name: "x".into(),
                    data_type: DataType::TimestampNtz(None),
                    options: vec![],
                }]
            );
        }
        s => panic!("Unexpected statement: {s:?}"),
    }
}

#[test]
fn parse_table_time_travel() {
    all_dialects_where(|d| d.supports_table_versioning())
        .verified_only_select("SELECT 1 FROM t1 TIMESTAMP AS OF '2018-10-18T22:15:12.013Z'");

    all_dialects_where(|d| d.supports_table_versioning()).verified_only_select(
        "SELECT 1 FROM t1 TIMESTAMP AS OF CURRENT_TIMESTAMP() - INTERVAL 12 HOURS",
    );

    all_dialects_where(|d| d.supports_table_versioning())
        .verified_only_select("SELECT 1 FROM t1 VERSION AS OF 1");

    assert!(databricks()
        .parse_sql_statements("SELECT 1 FROM t1 FOR TIMESTAMP AS OF 'some_timestamp'")
        .is_err());

    assert!(all_dialects_where(|d| d.supports_table_versioning())
        .parse_sql_statements("SELECT 1 FROM t1 VERSION AS OF 1 - 2",)
        .is_err())
}

#[test]
fn parse_optimize_table() {
    // Basic OPTIMIZE (Databricks style - no TABLE keyword)
    databricks().verified_stmt("OPTIMIZE my_table");
    databricks().verified_stmt("OPTIMIZE db.my_table");
    databricks().verified_stmt("OPTIMIZE catalog.db.my_table");

    // With WHERE clause
    databricks().verified_stmt("OPTIMIZE my_table WHERE date = '2023-01-01'");
    databricks().verified_stmt("OPTIMIZE my_table WHERE date >= '2023-01-01' AND date < '2023-02-01'");

    // With ZORDER BY clause
    databricks().verified_stmt("OPTIMIZE my_table ZORDER BY (col1)");
    databricks().verified_stmt("OPTIMIZE my_table ZORDER BY (col1, col2)");
    databricks().verified_stmt("OPTIMIZE my_table ZORDER BY (col1, col2, col3)");

    // Combined WHERE and ZORDER BY
    databricks().verified_stmt("OPTIMIZE my_table WHERE date = '2023-01-01' ZORDER BY (col1)");
    databricks().verified_stmt("OPTIMIZE my_table WHERE date >= '2023-01-01' ZORDER BY (col1, col2)");

    // Verify AST structure
    match databricks()
        .verified_stmt("OPTIMIZE my_table WHERE date = '2023-01-01' ZORDER BY (col1, col2)")
    {
        Statement::OptimizeTable {
            name,
            has_table_keyword,
            on_cluster,
            partition,
            include_final,
            deduplicate,
            predicate,
            zorder,
        } => {
            assert_eq!(name.to_string(), "my_table");
            assert!(!has_table_keyword);
            assert!(on_cluster.is_none());
            assert!(partition.is_none());
            assert!(!include_final);
            assert!(deduplicate.is_none());
            assert!(predicate.is_some());
            assert_eq!(
                zorder,
                Some(vec![
                    Expr::Identifier(Ident::new("col1")),
                    Expr::Identifier(Ident::new("col2")),
                ])
            );
        }
        _ => unreachable!(),
    }

    // Negative cases
    assert_eq!(
        databricks()
            .parse_sql_statements("OPTIMIZE my_table ZORDER BY")
            .unwrap_err(),
        ParserError::ParserError("Expected: (, found: EOF".to_string())
    );
    assert_eq!(
        databricks()
            .parse_sql_statements("OPTIMIZE my_table ZORDER BY ()")
            .unwrap_err(),
        ParserError::ParserError("Expected: an expression, found: )".to_string())
    );
}

#[test]
fn parse_create_table_partitioned_by() {
    // Databricks allows PARTITIONED BY with just column names (referencing existing columns)
    // https://docs.databricks.com/en/sql/language-manual/sql-ref-partition.html

    // Single partition column without type
    databricks().verified_stmt("CREATE TABLE t (col1 STRING, col2 INT) PARTITIONED BY (col1)");

    // Multiple partition columns without types
    databricks()
        .verified_stmt("CREATE TABLE t (col1 STRING, col2 INT, col3 DATE) PARTITIONED BY (col1, col2)");

    // Partition columns with types (new columns not in table spec)
    databricks().verified_stmt("CREATE TABLE t (name STRING) PARTITIONED BY (year INT, month INT)");

    // Mixed: some with types, some without
    databricks().verified_stmt(
        "CREATE TABLE t (id INT, name STRING) PARTITIONED BY (region, year INT)",
    );

    // Verify AST structure for column without type
    match databricks()
        .verified_stmt("CREATE TABLE t (col1 STRING) PARTITIONED BY (col1)")
    {
        Statement::CreateTable(CreateTable {
            name,
            columns,
            hive_distribution,
            ..
        }) => {
            assert_eq!(name.to_string(), "t");
            assert_eq!(columns.len(), 1);
            assert_eq!(columns[0].name.to_string(), "col1");
            match hive_distribution {
                HiveDistributionStyle::PARTITIONED { columns: partition_cols } => {
                    assert_eq!(partition_cols.len(), 1);
                    assert_eq!(partition_cols[0].name.to_string(), "col1");
                    assert_eq!(partition_cols[0].data_type, DataType::Unspecified);
                }
                _ => unreachable!(),
            }
        }
        _ => unreachable!(),
    }

    // Verify AST structure for column with type
    match databricks()
        .verified_stmt("CREATE TABLE t (name STRING) PARTITIONED BY (year INT)")
    {
        Statement::CreateTable(CreateTable {
            hive_distribution,
            ..
        }) => {
            match hive_distribution {
                HiveDistributionStyle::PARTITIONED { columns: partition_cols } => {
                    assert_eq!(partition_cols.len(), 1);
                    assert_eq!(partition_cols[0].name.to_string(), "year");
                    assert_eq!(partition_cols[0].data_type, DataType::Int(None));
                }
                _ => unreachable!(),
            }
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_databricks_struct_type() {
    // Databricks uses colon-separated struct field syntax (colon is optional)
    // https://docs.databricks.com/en/sql/language-manual/data-types/struct-type.html

    // Basic struct with colon syntax - parses to canonical form without colons
    databricks().one_statement_parses_to(
        "CREATE TABLE t (col1 STRUCT<field1: STRING, field2: INT>)",
        "CREATE TABLE t (col1 STRUCT<field1 STRING, field2 INT>)",
    );

    // Nested array of struct (the original issue case)
    databricks().one_statement_parses_to(
        "CREATE TABLE t (col1 ARRAY<STRUCT<finish_flag: STRING, survive_flag: STRING, score: INT>>)",
        "CREATE TABLE t (col1 ARRAY<STRUCT<finish_flag STRING, survive_flag STRING, score INT>>)",
    );

    // Multiple struct columns
    databricks().one_statement_parses_to(
        "CREATE TABLE t (col1 STRUCT<a: INT, b: STRING>, col2 STRUCT<x: DOUBLE>)",
        "CREATE TABLE t (col1 STRUCT<a INT, b STRING>, col2 STRUCT<x DOUBLE>)",
    );

    // Deeply nested structs
    databricks().one_statement_parses_to(
        "CREATE TABLE t (col1 STRUCT<outer: STRUCT<inner: STRING>>)",
        "CREATE TABLE t (col1 STRUCT<outer STRUCT<inner STRING>>)",
    );

    // Struct with array field
    databricks().one_statement_parses_to(
        "CREATE TABLE t (col1 STRUCT<items: ARRAY<INT>, name: STRING>)",
        "CREATE TABLE t (col1 STRUCT<items ARRAY<INT>, name STRING>)",
    );

    // Syntax without colons should also work (BigQuery compatible)
    databricks().verified_stmt("CREATE TABLE t (col1 STRUCT<field1 STRING, field2 INT>)");

    // Verify AST structure
    match databricks().one_statement_parses_to(
        "CREATE TABLE t (col1 STRUCT<field1: STRING, field2: INT>)",
        "CREATE TABLE t (col1 STRUCT<field1 STRING, field2 INT>)",
    ) {
        Statement::CreateTable(CreateTable { columns, .. }) => {
            assert_eq!(columns.len(), 1);
            assert_eq!(columns[0].name.to_string(), "col1");
            match &columns[0].data_type {
                DataType::Struct(fields, StructBracketKind::AngleBrackets) => {
                    assert_eq!(fields.len(), 2);
                    assert_eq!(
                        fields[0].field_name.as_ref().map(|i| i.to_string()),
                        Some("field1".to_string())
                    );
                    assert_eq!(fields[0].field_type, DataType::String(None));
                    assert_eq!(
                        fields[1].field_name.as_ref().map(|i| i.to_string()),
                        Some("field2".to_string())
                    );
                    assert_eq!(fields[1].field_type, DataType::Int(None));
                }
                _ => unreachable!(),
            }
        }
        _ => unreachable!(),
    }
}

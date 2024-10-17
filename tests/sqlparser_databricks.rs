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

use sqlparser::ast::*;
use sqlparser::dialect::{DatabricksDialect, GenericDialect};
use sqlparser::parser::ParserError;
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
        SelectItem::UnnamedExpr(Expr::Value(Value::DoubleQuotedString("Ä".to_owned())))
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
                        Expr::Value(number("1")),
                        Expr::Value(number("2")),
                        Expr::Value(number("3"))
                    ]
                ),
                Expr::Lambda(LambdaFunction {
                    params: OneOrManyWithParens::One(Ident::new("x")),
                    body: Box::new(Expr::IsNull(Box::new(Expr::Identifier(Ident::new("x")))))
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
                        Expr::Value(Value::SingleQuotedString("Hello".to_owned())),
                        Expr::Value(Value::SingleQuotedString("World".to_owned()))
                    ]
                ),
                Expr::Lambda(LambdaFunction {
                    params: OneOrManyWithParens::Many(vec![Ident::new("p1"), Ident::new("p2")]),
                    body: Box::new(Expr::Case {
                        operand: None,
                        conditions: vec![
                            Expr::BinaryOp {
                                left: Box::new(Expr::Identifier(Ident::new("p1"))),
                                op: BinaryOperator::Eq,
                                right: Box::new(Expr::Identifier(Ident::new("p2")))
                            },
                            Expr::BinaryOp {
                                left: Box::new(call(
                                    "reverse",
                                    [Expr::Identifier(Ident::new("p1"))]
                                )),
                                op: BinaryOperator::Lt,
                                right: Box::new(call(
                                    "reverse",
                                    [Expr::Identifier(Ident::new("p2"))]
                                ))
                            }
                        ],
                        results: vec![
                            Expr::Value(number("0")),
                            Expr::UnaryOp {
                                op: UnaryOperator::Minus,
                                expr: Box::new(Expr::Value(number("1")))
                            }
                        ],
                        else_result: Some(Box::new(Expr::Value(number("1"))))
                    })
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
        explicit_row: false,
        rows: vec![
            vec![
                Expr::Value(Value::DoubleQuotedString("one".to_owned())),
                Expr::Value(number("1")),
            ],
            vec![
                Expr::Value(Value::SingleQuotedString("two".to_owned())),
                Expr::Value(number("2")),
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
        Some(&TableFactor::Table {
            name: ObjectName(vec![Ident::new("values")]),
            alias: None,
            args: None,
            with_hints: vec![],
            version: None,
            partitions: vec![],
            with_ordinality: false,
        }),
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
            databricks().verified_stmt(&format!("USE {}", object_name)),
            Statement::Use(Use::Object(ObjectName(vec![Ident::new(
                object_name.to_string()
            )])))
        );
        for &quote in &quote_styles {
            // Test single identifier with different type of quotes
            assert_eq!(
                databricks().verified_stmt(&format!("USE {0}{1}{0}", quote, object_name)),
                Statement::Use(Use::Object(ObjectName(vec![Ident::with_quote(
                    quote,
                    object_name.to_string(),
                )])))
            );
        }
    }

    for &quote in &quote_styles {
        // Test single identifier with keyword and different type of quotes
        assert_eq!(
            databricks().verified_stmt(&format!("USE CATALOG {0}my_catalog{0}", quote)),
            Statement::Use(Use::Catalog(ObjectName(vec![Ident::with_quote(
                quote,
                "my_catalog".to_string(),
            )])))
        );
        assert_eq!(
            databricks().verified_stmt(&format!("USE DATABASE {0}my_database{0}", quote)),
            Statement::Use(Use::Database(ObjectName(vec![Ident::with_quote(
                quote,
                "my_database".to_string(),
            )])))
        );
        assert_eq!(
            databricks().verified_stmt(&format!("USE SCHEMA {0}my_schema{0}", quote)),
            Statement::Use(Use::Schema(ObjectName(vec![Ident::with_quote(
                quote,
                "my_schema".to_string(),
            )])))
        );
    }

    // Test single identifier with keyword and no quotes
    assert_eq!(
        databricks().verified_stmt("USE CATALOG my_catalog"),
        Statement::Use(Use::Catalog(ObjectName(vec![Ident::new("my_catalog")])))
    );
    assert_eq!(
        databricks().verified_stmt("USE DATABASE my_schema"),
        Statement::Use(Use::Database(ObjectName(vec![Ident::new("my_schema")])))
    );
    assert_eq!(
        databricks().verified_stmt("USE SCHEMA my_schema"),
        Statement::Use(Use::Schema(ObjectName(vec![Ident::new("my_schema")])))
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

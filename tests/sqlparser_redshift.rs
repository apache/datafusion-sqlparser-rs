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

use sqlparser::tokenizer::Span;
use test_utils::*;

use sqlparser::ast::*;
use sqlparser::dialect::GenericDialect;
use sqlparser::dialect::RedshiftSqlDialect;

#[test]
fn test_square_brackets_over_db_schema_table_name() {
    let select = redshift().verified_only_select("SELECT [col1] FROM [test_schema].[test_table]");
    assert_eq!(
        select.projection[0],
        SelectItem::UnnamedExpr(Expr::Identifier(Ident {
            value: "col1".to_string(),
            quote_style: Some('['),
            span: Span::empty(),
        })),
    );
    assert_eq!(
        select.from[0],
        TableWithJoins {
            relation: table_from_name(ObjectName::from(vec![
                Ident {
                    value: "test_schema".to_string(),
                    quote_style: Some('['),
                    span: Span::empty(),
                },
                Ident {
                    value: "test_table".to_string(),
                    quote_style: Some('['),
                    span: Span::empty(),
                }
            ])),
            joins: vec![],
        }
    );
}

#[test]
fn brackets_over_db_schema_table_name_with_whites_paces() {
    match redshift().parse_sql_statements("SELECT [   col1  ] FROM [  test_schema].[ test_table]") {
        Ok(statements) => {
            assert_eq!(statements.len(), 1);
        }
        _ => unreachable!(),
    }
}

#[test]
fn test_double_quotes_over_db_schema_table_name() {
    let select =
        redshift().verified_only_select("SELECT \"col1\" FROM \"test_schema\".\"test_table\"");
    assert_eq!(
        select.projection[0],
        SelectItem::UnnamedExpr(Expr::Identifier(Ident {
            value: "col1".to_string(),
            quote_style: Some('"'),
            span: Span::empty(),
        })),
    );
    assert_eq!(
        select.from[0],
        TableWithJoins {
            relation: table_from_name(ObjectName::from(vec![
                Ident {
                    value: "test_schema".to_string(),
                    quote_style: Some('"'),
                    span: Span::empty(),
                },
                Ident {
                    value: "test_table".to_string(),
                    quote_style: Some('"'),
                    span: Span::empty(),
                }
            ])),
            joins: vec![],
        }
    );
}

#[test]
fn parse_delimited_identifiers() {
    // check that quoted identifiers in any position remain quoted after serialization
    let select = redshift().verified_only_select(
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

    redshift().verified_stmt(r#"CREATE TABLE "foo" ("bar" "int")"#);
    // An alias starting with a number
    redshift().verified_stmt(r#"CREATE TABLE "foo" ("1" INT)"#);
    redshift().verified_stmt(r#"ALTER TABLE foo ADD CONSTRAINT "bar" PRIMARY KEY (baz)"#);
    //TODO verified_stmt(r#"UPDATE foo SET "bar" = 5"#);
}

fn redshift() -> TestedDialects {
    TestedDialects::new(vec![Box::new(RedshiftSqlDialect {})])
}

fn redshift_and_generic() -> TestedDialects {
    TestedDialects::new(vec![
        Box::new(RedshiftSqlDialect {}),
        Box::new(GenericDialect {}),
    ])
}

#[test]
fn test_sharp() {
    let sql = "SELECT #_of_values";
    let select = redshift().verified_only_select(sql);
    assert_eq!(
        SelectItem::UnnamedExpr(Expr::Identifier(Ident::new("#_of_values"))),
        select.projection[0]
    );
}

#[test]
fn test_create_view_with_no_schema_binding() {
    redshift_and_generic()
        .verified_stmt("CREATE VIEW myevent AS SELECT eventname FROM event WITH NO SCHEMA BINDING");
}

#[test]
fn test_redshift_json_path() {
    let dialects = all_dialects_where(|d| d.supports_partiql());
    let sql = "SELECT cust.c_orders[0].o_orderkey FROM customer_orders_lineitem";
    let select = dialects.verified_only_select(sql);

    assert_eq!(
        &Expr::JsonAccess {
            value: Box::new(Expr::CompoundIdentifier(vec![
                Ident::new("cust"),
                Ident::new("c_orders")
            ])),
            path: JsonPath {
                path: vec![
                    JsonPathElem::Bracket {
                        key: Expr::value(number("0"))
                    },
                    JsonPathElem::Dot {
                        key: "o_orderkey".to_string(),
                        quoted: false
                    }
                ]
            }
        },
        expr_from_projection(only(&select.projection))
    );

    let sql = "SELECT cust.c_orders[0]['id'] FROM customer_orders_lineitem";
    let select = dialects.verified_only_select(sql);
    assert_eq!(
        &Expr::JsonAccess {
            value: Box::new(Expr::CompoundIdentifier(vec![
                Ident::new("cust"),
                Ident::new("c_orders")
            ])),
            path: JsonPath {
                path: vec![
                    JsonPathElem::Bracket {
                        key: Expr::value(number("0"))
                    },
                    JsonPathElem::Bracket {
                        key: Expr::Value(
                            (Value::SingleQuotedString("id".to_owned())).with_empty_span()
                        )
                    }
                ]
            }
        },
        expr_from_projection(only(&select.projection))
    );

    let sql = "SELECT db1.sc1.tbl1.col1[0]['id'] FROM customer_orders_lineitem";
    let select = dialects.verified_only_select(sql);
    assert_eq!(
        &Expr::JsonAccess {
            value: Box::new(Expr::CompoundIdentifier(vec![
                Ident::new("db1"),
                Ident::new("sc1"),
                Ident::new("tbl1"),
                Ident::new("col1")
            ])),
            path: JsonPath {
                path: vec![
                    JsonPathElem::Bracket {
                        key: Expr::value(number("0"))
                    },
                    JsonPathElem::Bracket {
                        key: Expr::Value(
                            (Value::SingleQuotedString("id".to_owned())).with_empty_span()
                        )
                    }
                ]
            }
        },
        expr_from_projection(only(&select.projection))
    );

    let sql = r#"SELECT db1.sc1.tbl1.col1[0]."id" FROM customer_orders_lineitem"#;
    let select = dialects.verified_only_select(sql);
    assert_eq!(
        &Expr::JsonAccess {
            value: Box::new(Expr::CompoundIdentifier(vec![
                Ident::new("db1"),
                Ident::new("sc1"),
                Ident::new("tbl1"),
                Ident::new("col1")
            ])),
            path: JsonPath {
                path: vec![
                    JsonPathElem::Bracket {
                        key: Expr::value(number("0"))
                    },
                    JsonPathElem::Dot {
                        key: "id".to_string(),
                        quoted: true,
                    }
                ]
            }
        },
        expr_from_projection(only(&select.projection))
    );
}

#[test]
fn test_parse_json_path_from() {
    let dialects = all_dialects_where(|d| d.supports_partiql());
    let select = dialects.verified_only_select("SELECT * FROM src[0].a AS a");
    match &select.from[0].relation {
        TableFactor::Table {
            name, json_path, ..
        } => {
            assert_eq!(name, &ObjectName::from(vec![Ident::new("src")]));
            assert_eq!(
                json_path,
                &Some(JsonPath {
                    path: vec![
                        JsonPathElem::Bracket {
                            key: Expr::value(number("0"))
                        },
                        JsonPathElem::Dot {
                            key: "a".to_string(),
                            quoted: false
                        }
                    ]
                })
            );
        }
        _ => panic!(),
    }

    let select = dialects.verified_only_select("SELECT * FROM src[0].a[1].b AS a");
    match &select.from[0].relation {
        TableFactor::Table {
            name, json_path, ..
        } => {
            assert_eq!(name, &ObjectName::from(vec![Ident::new("src")]));
            assert_eq!(
                json_path,
                &Some(JsonPath {
                    path: vec![
                        JsonPathElem::Bracket {
                            key: Expr::value(number("0"))
                        },
                        JsonPathElem::Dot {
                            key: "a".to_string(),
                            quoted: false
                        },
                        JsonPathElem::Bracket {
                            key: Expr::Value(
                                (Value::Number("1".parse().unwrap(), false)).with_empty_span()
                            )
                        },
                        JsonPathElem::Dot {
                            key: "b".to_string(),
                            quoted: false
                        },
                    ]
                })
            );
        }
        _ => panic!(),
    }

    let select = dialects.verified_only_select("SELECT * FROM src.a.b");
    match &select.from[0].relation {
        TableFactor::Table {
            name, json_path, ..
        } => {
            assert_eq!(
                name,
                &ObjectName::from(vec![Ident::new("src"), Ident::new("a"), Ident::new("b")])
            );
            assert_eq!(json_path, &None);
        }
        _ => panic!(),
    }
}

#[test]
fn test_parse_select_numbered_columns() {
    // An alias starting with a number
    redshift_and_generic().verified_stmt(r#"SELECT 1 AS "1" FROM a"#);
    redshift_and_generic().verified_stmt(r#"SELECT 1 AS "1abc" FROM a"#);
}

#[test]
fn test_parse_nested_quoted_identifier() {
    redshift().verified_stmt(r#"SELECT 1 AS ["1"] FROM a"#);
    redshift().verified_stmt(r#"SELECT 1 AS ["[="] FROM a"#);
    redshift().verified_stmt(r#"SELECT 1 AS ["=]"] FROM a"#);
    redshift().verified_stmt(r#"SELECT 1 AS ["a[b]"] FROM a"#);
    // trim spaces
    redshift().one_statement_parses_to(r#"SELECT 1 AS [ " 1 " ]"#, r#"SELECT 1 AS [" 1 "]"#);
    // invalid query
    assert!(redshift()
        .parse_sql_statements(r#"SELECT 1 AS ["1]"#)
        .is_err());
}

#[test]
fn parse_extract_single_quotes() {
    let sql = "SELECT EXTRACT('month' FROM my_timestamp) FROM my_table";
    redshift().verified_stmt(sql);
}

#[test]
fn parse_string_literal_backslash_escape() {
    redshift().one_statement_parses_to(r#"SELECT 'l\'auto'"#, "SELECT 'l''auto'");
}

#[test]
fn parse_utf8_multibyte_idents() {
    redshift().verified_stmt("SELECT 🚀.city AS 🎸 FROM customers AS 🚀");
}

#[test]
fn parse_vacuum() {
    redshift().verified_stmt("VACUUM db1.sc1.tbl1");
    let stmt = redshift().verified_stmt(
        "VACUUM FULL SORT ONLY DELETE ONLY REINDEX RECLUSTER db1.sc1.tbl1 TO 20 PERCENT BOOST",
    );
    match stmt {
        Statement::Vacuum(v) => {
            assert!(v.full);
            assert!(v.sort_only);
            assert!(v.delete_only);
            assert!(v.reindex);
            assert!(v.recluster);
            assert_eq!(
                v.table_name,
                Some(ObjectName::from(vec![
                    Ident::new("db1"),
                    Ident::new("sc1"),
                    Ident::new("tbl1"),
                ]))
            );
            assert_eq!(v.threshold, Some(number("20")));
            assert!(v.boost);
        }
        _ => unreachable!(),
    }
}

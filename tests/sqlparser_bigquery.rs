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

use sqlparser::ast::*;
use sqlparser::dialect::{BigQueryDialect, GenericDialect};
use test_utils::*;

#[cfg(feature = "bigdecimal")]
use bigdecimal::*;
#[cfg(feature = "bigdecimal")]
use std::str::FromStr;

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
        r#"SELECT R'abc', R'abc', R'f\(abc,(.*),def\)', R'f\(abc,(.*),def\)'"#,
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
                &Expr::Value(Value::RawStringLiteral(r#"f\(abc,(.*),def\)"#.to_string())),
                expr_from_projection(&select.projection[2])
            );
            assert_eq!(
                &Expr::Value(Value::RawStringLiteral(r#"f\(abc,(.*),def\)"#.to_string())),
                expr_from_projection(&select.projection[3])
            );
            return;
        }
    }
    panic!("invalid query")
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
            expr: Box::new(StructExpr(vec![
                ExprWithFieldName {
                    expr: Expr::Value(number("1")),
                    field_name: None
                },
                ExprWithFieldName {
                    expr: Expr::Value(number("2")),
                    field_name: None
                },
                ExprWithFieldName {
                    expr: Expr::Value(number("3")),
                    field_name: None
                }
            ])),
            type_ann: None
        },
        expr_from_projection(&select.projection[0])
    );

    assert_eq!(
        &Expr::Struct {
            expr: Box::new(StructExpr(vec![ExprWithFieldName {
                expr: Expr::Value(Value::SingleQuotedString("abc".to_string())),
                field_name: None
            },])),
            type_ann: None
        },
        expr_from_projection(&select.projection[1])
    );

    assert_eq!(
        &Expr::Struct {
            expr: Box::new(StructExpr(vec![
                ExprWithFieldName {
                    expr: Expr::Value(number("1")),
                    field_name: None
                },
                ExprWithFieldName {
                    expr: Expr::CompoundIdentifier(vec![Ident::from("t"), Ident::from("str_col")]),
                    field_name: None
                },
            ])),
            type_ann: None
        },
        expr_from_projection(&select.projection[2])
    );

    assert_eq!(
        &Expr::Struct {
            expr: Box::new(StructExpr(vec![
                ExprWithFieldName {
                    expr: Expr::Value(number("1")),
                    field_name: Some(Ident::from("a"))
                },
                ExprWithFieldName {
                    expr: Expr::Value(Value::SingleQuotedString("abc".to_string())),
                    field_name: Some(Ident::from("b"))
                },
            ])),
            type_ann: None
        },
        expr_from_projection(&select.projection[3])
    );

    assert_eq!(
        &Expr::Struct {
            expr: Box::new(StructExpr(vec![ExprWithFieldName {
                expr: Expr::Identifier(Ident::from("str_col")),
                field_name: Some(Ident::from("abc"))
            }])),
            type_ann: None
        },
        expr_from_projection(&select.projection[4])
    );
}

#[test]
fn parse_typed_struct_syntax() {
    // typed struct syntax https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#typed_struct_syntax
    // syntax: STRUCT<[field_name] field_type, ...>( expr1 [, ... ])
    // let sql = r#"SELECT STRUCT<int64>(5), STRUCT<x int64, y string>(1, t.str_col), STRUCT<int64>(int_col)"#;
    let sql = r#"SELECT STRUCT<BOOL>(true), STRUCT<BYTES>(B'abc')"#;
    let select = bigquery().verified_only_select(sql);
    assert_eq!(2, select.projection.len());
    assert_eq!(
        &Expr::Struct {
            expr: Box::new(StructExpr(vec![ExprWithFieldName {
                expr: Expr::Value(Value::Boolean(true)),
                field_name: None
            }])),
            type_ann: Some(vec![StructTypeAnn {
                field_name: None,
                field_type: StructFieldType::Bool
            }])
        },
        expr_from_projection(&select.projection[0])
    );

    assert_eq!(
        &Expr::Struct {
            expr: Box::new(StructExpr(vec![ExprWithFieldName {
                expr: Expr::Value(Value::SingleQuotedByteStringLiteral("abc".to_string())),
                field_name: None
            }])),
            type_ann: Some(vec![StructTypeAnn {
                field_name: None,
                field_type: StructFieldType::Bytes
            }])
        },
        expr_from_projection(&select.projection[1])
    );

    let sql = r#"SELECT STRUCT<DATE>("2011-05-05"), STRUCT<DATETIME>(DATETIME '1999-01-01 01:23:34.45'), STRUCT<FLOAT64>(5.0), STRUCT<INT64>(1)"#;
    let select = bigquery().verified_only_select(sql);
    assert_eq!(4, select.projection.len());

    let sql = r#"SELECT STRUCT<INTERVAL>(INTERVAL '1-2 3 4:5:6.789999'), STRUCT<JSON>(JSON '{"class" : {"students" : [{"name" : "Jane"}]}}')"#;
    let select = bigquery().verified_only_select(sql);
    assert_eq!(2, select.projection.len());

    let sql = r#"SELECT STRUCT<STRING>("foo"), STRUCT<TIMESTAMP>(TIMESTAMP '2008-12-25 15:30:00 America/Los_Angeles'), STRUCT<TIME>(TIME '15:30:00')"#;
    let select = bigquery().verified_only_select(sql);
    assert_eq!(3, select.projection.len());

    // TODO: support
    // let sql = r#"SELECT STRUCT<NUMERIC>(NUMERIC '1'), STRUCT<BIGNUMERIC>(BIGNUMERIC '1')"#;
    // let select = bigquery().verified_only_select(sql);
    // assert_eq!(1, select.projection.len());

    // let sql = r#"SELECT STRUCT<STRUCT<STRING>>(STRUCT("foo"))"#;
    // let select = bigquery().verified_only_select(sql);
    // assert_eq!(1, select.projection.len());

    // let sql = r#"SELECT STRUCT<ARRAY<STRING>>(["foo"])"#;
    // let select = bigquery().verified_only_select(sql);
    // assert_eq!(1, select.projection.len());

    // // should return error
    // let sql = r#"SELECT STRUCT<x int64>(5 AS x)"#;
    // let sql = r#"SELECT STRUCT()"#;
}

#[test]
fn parse_table_identifiers() {
    fn test_table_ident(ident: &str, expected: Vec<Ident>) {
        let sql = format!("SELECT 1 FROM {ident}");
        let select = bigquery().verified_only_select(&sql);
        assert_eq!(
            select.from,
            vec![TableWithJoins {
                relation: TableFactor::Table {
                    name: ObjectName(expected),
                    alias: None,
                    args: None,
                    columns_definition: None,
                    with_hints: vec![],
                },
                joins: vec![]
            },]
        );
    }
    fn test_table_ident_err(ident: &str) {
        let sql = format!("SELECT 1 FROM {ident}");
        assert!(bigquery().parse_sql_statements(&sql).is_err());
    }

    test_table_ident("da-sh-es", vec![Ident::new("da-sh-es")]);

    test_table_ident("`spa ce`", vec![Ident::with_quote('`', "spa ce")]);

    test_table_ident(
        "`!@#$%^&*()-=_+`",
        vec![Ident::with_quote('`', "!@#$%^&*()-=_+")],
    );

    test_table_ident(
        "_5abc.dataField",
        vec![Ident::new("_5abc"), Ident::new("dataField")],
    );
    test_table_ident(
        "`5abc`.dataField",
        vec![Ident::with_quote('`', "5abc"), Ident::new("dataField")],
    );

    test_table_ident_err("5abc.dataField");

    test_table_ident(
        "abc5.dataField",
        vec![Ident::new("abc5"), Ident::new("dataField")],
    );

    test_table_ident_err("abc5!.dataField");

    test_table_ident(
        "`GROUP`.dataField",
        vec![Ident::with_quote('`', "GROUP"), Ident::new("dataField")],
    );

    // TODO: this should be error
    // test_table_ident_err("GROUP.dataField");

    test_table_ident("abc5.GROUP", vec![Ident::new("abc5"), Ident::new("GROUP")]);
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
                array_expr: Box::new(Expr::CompoundIdentifier(vec![
                    Ident::new("t1"),
                    Ident::new("a")
                ])),
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

fn bigquery() -> TestedDialects {
    TestedDialects {
        dialects: vec![Box::new(BigQueryDialect {})],
    }
}

fn bigquery_and_generic() -> TestedDialects {
    TestedDialects {
        dialects: vec![Box::new(BigQueryDialect {}), Box::new(GenericDialect {})],
    }
}

#[test]
fn parse_map_access_offset() {
    let sql = "SELECT d[offset(0)]";
    let _select = bigquery().verified_only_select(sql);
    #[cfg(not(feature = "bigdecimal"))]
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
                    Value::Number("0".into(), false)
                ))),],
                over: None,
                distinct: false,
                special: false,
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

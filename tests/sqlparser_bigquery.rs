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

    test_table_ident("da-sh-es", None, vec![Ident::new("da-sh-es")]);

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

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

use test_utils::*;

use sqlparser::ast::*;
use sqlparser::dialect::RedshiftSqlDialect;

#[test]
fn test_square_brackets_over_db_schema_table_name() {
    let select = redshift().verified_only_select("SELECT [col1] FROM [test_schema].[test_table]");
    assert_eq!(
        select.projection[0],
        SelectItem::UnnamedExpr(Expr::Identifier(Ident {
            value: "col1".to_string(),
            quote_style: Some('[')
        })),
    );
    assert_eq!(
        select.from[0],
        TableWithJoins {
            relation: TableFactor::Table {
                name: ObjectName(vec![
                    Ident {
                        value: "test_schema".to_string(),
                        quote_style: Some('[')
                    },
                    Ident {
                        value: "test_table".to_string(),
                        quote_style: Some('[')
                    }
                ]),
                alias: None,
                args: None,
                columns_definition: None,
                with_hints: vec![],
            },
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
            quote_style: Some('"')
        })),
    );
    assert_eq!(
        select.from[0],
        TableWithJoins {
            relation: TableFactor::Table {
                name: ObjectName(vec![
                    Ident {
                        value: "test_schema".to_string(),
                        quote_style: Some('"')
                    },
                    Ident {
                        value: "test_table".to_string(),
                        quote_style: Some('"')
                    }
                ]),
                alias: None,
                args: None,
                columns_definition: None,
                with_hints: vec![],
            },
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
            columns_definition,
            with_hints,
        } => {
            assert_eq!(vec![Ident::with_quote('"', "a table")], name.0);
            assert_eq!(Ident::with_quote('"', "alias"), alias.unwrap().name);
            assert!(args.is_none());
            assert!(columns_definition.is_none());
            assert!(with_hints.is_empty());
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
            args: vec![],
            over: None,
            distinct: false,
            special: false,
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
    redshift().verified_stmt(r#"ALTER TABLE foo ADD CONSTRAINT "bar" PRIMARY KEY (baz)"#);
    //TODO verified_stmt(r#"UPDATE foo SET "bar" = 5"#);
}

#[test]
fn parse_like() {
    fn chk(negated: bool) {
        let sql = &format!(
            "SELECT * FROM customers WHERE name {}LIKE '%a'",
            if negated { "NOT " } else { "" }
        );
        let select = redshift().verified_only_select(sql);
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
        let select = redshift().verified_only_select(sql);
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
        let select = redshift().verified_only_select(sql);
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
        let select = redshift().verified_only_select(sql);
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
        let select = redshift().verified_only_select(sql);
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
        let select = redshift().verified_only_select(sql);
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

fn redshift() -> TestedDialects {
    TestedDialects {
        dialects: vec![Box::new(RedshiftSqlDialect {})],
    }
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
fn test_parse_pg_get_late_binding_view_cols() {
    let sql = "select * from pg_get_late_binding_view_cols() some_name_cols(view_schema name, view_name name, col_name name)";
    let expected = "SELECT * FROM pg_get_late_binding_view_cols() some_name_cols(view_schema name, view_name name, col_name name)";
    redshift().one_statement_parses_to(sql, expected);

    let select = redshift().verified_only_select(expected);
    assert_eq!(
        TableFactor::Table {
            name: ObjectName(vec![Ident::new("pg_get_late_binding_view_cols")],),
            args: Some(vec![]),
            alias: None,
            columns_definition: Some(ColsDefinition {
                name: Ident::new("some_name_cols"),
                args: vec![
                    IdentPair(Ident::new("view_schema"), Ident::new("name")),
                    IdentPair(Ident::new("view_name"), Ident::new("name")),
                    IdentPair(Ident::new("col_name"), Ident::new("name"))
                ]
            }),
            with_hints: vec![]
        },
        select.from[0].relation
    );
}

#[test]
fn test_parse_pg_get_cols() {
    let sql =
        "SELECT * FROM pg_get_cols() some_name(view_schema name, view_name name, col_name name)";
    redshift().verified_stmt(sql);
}

#[test]
fn test_parse_pg_get_grantee_by_iam_role() {
    let sql = "SELECT grantee, grantee_type, cmd_type FROM pg_get_grantee_by_iam_role('arn:aws:iam::123456789012:role/Redshift-S3-Write') res_grantee(grantee text, grantee_type text, cmd_type text)";
    redshift().verified_stmt(sql);
}

#[test]
fn test_parse_pg_get_iam_role_by_user() {
    let sql = "SELECT username, iam_role, cmd FROM pg_get_iam_role_by_user('reg_user1') res_iam_role(username text, iam_role text, cmd text)";
    redshift().verified_stmt(sql);
}

#[test]
fn test_parse_pg_get_late_binding_view_cols_in_select() {
    let sql = "SELECT pg_get_late_binding_view_cols()";
    redshift().verified_stmt(sql);
}

#[test]
fn test_parse_pg_get_cols_in_select() {
    let sql = "SELECT pg_get_cols()";
    redshift().verified_stmt(sql);
}

#[test]
fn test_parse_pg_get_grantee_by_iam_role_in_select() {
    let sql = "SELECT pg_get_grantee_by_iam_role()";
    redshift().verified_stmt(sql);
}

#[test]
fn test_parse_pg_get_iam_role_by_user_in_select() {
    let sql = "SELECT pg_get_iam_role_by_user()";
    redshift().verified_stmt(sql);
}

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
//! Test SQL syntax specific to Microsoft's T-SQL. The parser based on the
//! generic dialect is also tested (on the inputs it can handle).

#[macro_use]
mod test_utils;
use test_utils::*;

use sqlparser::ast::*;
use sqlparser::dialect::{GenericDialect, MsSqlDialect};

#[test]
fn parse_mssql_identifiers() {
    let sql = "SELECT @@version, _foo$123 FROM ##temp";
    let select = ms_and_generic().verified_only_select(sql);
    assert_eq!(
        &Expr::Identifier(Ident::new("@@version")),
        expr_from_projection(&select.projection[0]),
    );
    assert_eq!(
        &Expr::Identifier(Ident::new("_foo$123")),
        expr_from_projection(&select.projection[1]),
    );
    assert_eq!(2, select.projection.len());
    match &only(&select.from).relation {
        TableFactor::Table { name, .. } => {
            assert_eq!("##temp".to_string(), name.to_string());
        }
        _ => unreachable!(),
    };
}

#[test]
fn parse_table_time_travel() {
    let version = "2023-08-18 23:08:18".to_string();
    let sql = format!("SELECT 1 FROM t1 FOR SYSTEM_TIME AS OF '{version}'");
    let select = ms().verified_only_select(&sql);
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
    assert!(ms().parse_sql_statements(&sql).is_err());
}

#[test]
fn parse_mssql_single_quoted_aliases() {
    let _ = ms_and_generic().one_statement_parses_to("SELECT foo 'alias'", "SELECT foo AS 'alias'");
}

#[test]
fn parse_mssql_delimited_identifiers() {
    let _ = ms().one_statement_parses_to(
        "SELECT [a.b!] [FROM] FROM foo [WHERE]",
        "SELECT [a.b!] AS [FROM] FROM foo AS [WHERE]",
    );
}

#[test]
fn parse_create_procedure() {
    let sql = "CREATE OR ALTER PROCEDURE test (@foo INT, @bar VARCHAR(256)) AS BEGIN SELECT 1 END";

    assert_eq!(
        ms().verified_stmt(sql),
        Statement::CreateProcedure {
            or_alter: true,
            body: vec![Statement::Query(Box::new(Query {
                with: None,
                limit: None,
                limit_by: vec![],
                offset: None,
                fetch: None,
                locks: vec![],
                for_clause: None,
                order_by: vec![],
                body: Box::new(SetExpr::Select(Box::new(Select {
                    distinct: None,
                    top: None,
                    projection: vec![SelectItem::UnnamedExpr(Expr::Value(number("1")))],
                    into: None,
                    from: vec![],
                    lateral_views: vec![],
                    selection: None,
                    group_by: GroupByExpr::Expressions(vec![]),
                    cluster_by: vec![],
                    distribute_by: vec![],
                    sort_by: vec![],
                    having: None,
                    named_window: vec![],
                    qualify: None
                })))
            }))],
            params: Some(vec![
                ProcedureParam {
                    name: Ident {
                        value: "@foo".into(),
                        quote_style: None
                    },
                    data_type: DataType::Int(None)
                },
                ProcedureParam {
                    name: Ident {
                        value: "@bar".into(),
                        quote_style: None
                    },
                    data_type: DataType::Varchar(Some(CharacterLength::IntegerLength {
                        length: 256,
                        unit: None
                    }))
                }
            ]),
            name: ObjectName(vec![Ident {
                value: "test".into(),
                quote_style: None
            }])
        }
    )
}

#[test]
fn parse_mssql_create_procedure() {
    let _ = ms_and_generic().verified_stmt("CREATE OR ALTER PROCEDURE foo AS BEGIN SELECT 1 END");
    let _ = ms_and_generic().verified_stmt("CREATE PROCEDURE foo AS BEGIN SELECT 1 END");
    let _ = ms().verified_stmt(
        "CREATE PROCEDURE foo AS BEGIN SELECT [myColumn] FROM [myschema].[mytable] END",
    );
    let _ = ms_and_generic().verified_stmt(
        "CREATE PROCEDURE foo (@CustomerName NVARCHAR(50)) AS BEGIN SELECT * FROM DEV END",
    );
    let _ = ms().verified_stmt("CREATE PROCEDURE [foo] AS BEGIN UPDATE bar SET col = 'test' END");
    // Test a statement with END in it
    let _ = ms().verified_stmt("CREATE PROCEDURE [foo] AS BEGIN SELECT [foo], CASE WHEN [foo] IS NULL THEN 'empty' ELSE 'notempty' END AS [foo] END");
    // Multiple statements
    let _ = ms().verified_stmt("CREATE PROCEDURE [foo] AS BEGIN UPDATE bar SET col = 'test'; SELECT [foo] FROM BAR WHERE [FOO] > 10 END");
}

#[test]
fn parse_mssql_apply_join() {
    let _ = ms_and_generic().verified_only_select(
        "SELECT * FROM sys.dm_exec_query_stats AS deqs \
         CROSS APPLY sys.dm_exec_query_plan(deqs.plan_handle)",
    );
    let _ = ms_and_generic().verified_only_select(
        "SELECT * FROM sys.dm_exec_query_stats AS deqs \
         OUTER APPLY sys.dm_exec_query_plan(deqs.plan_handle)",
    );
    let _ = ms_and_generic().verified_only_select(
        "SELECT * FROM foo \
         OUTER APPLY (SELECT foo.x + 1) AS bar",
    );
}

#[test]
fn parse_mssql_top_paren() {
    let sql = "SELECT TOP (5) * FROM foo";
    let select = ms_and_generic().verified_only_select(sql);
    let top = select.top.unwrap();
    assert_eq!(
        Some(TopQuantity::Expr(Expr::Value(number("5")))),
        top.quantity
    );
    assert!(!top.percent);
}

#[test]
fn parse_mssql_top_percent() {
    let sql = "SELECT TOP (5) PERCENT * FROM foo";
    let select = ms_and_generic().verified_only_select(sql);
    let top = select.top.unwrap();
    assert_eq!(
        Some(TopQuantity::Expr(Expr::Value(number("5")))),
        top.quantity
    );
    assert!(top.percent);
}

#[test]
fn parse_mssql_top_with_ties() {
    let sql = "SELECT TOP (5) WITH TIES * FROM foo";
    let select = ms_and_generic().verified_only_select(sql);
    let top = select.top.unwrap();
    assert_eq!(
        Some(TopQuantity::Expr(Expr::Value(number("5")))),
        top.quantity
    );
    assert!(top.with_ties);
}

#[test]
fn parse_mssql_top_percent_with_ties() {
    let sql = "SELECT TOP (10) PERCENT WITH TIES * FROM foo";
    let select = ms_and_generic().verified_only_select(sql);
    let top = select.top.unwrap();
    assert_eq!(
        Some(TopQuantity::Expr(Expr::Value(number("10")))),
        top.quantity
    );
    assert!(top.percent);
}

#[test]
fn parse_mssql_top() {
    let sql = "SELECT TOP 5 bar, baz FROM foo";
    let _ = ms_and_generic().one_statement_parses_to(sql, "SELECT TOP 5 bar, baz FROM foo");
}

#[test]
fn parse_mssql_bin_literal() {
    let _ = ms_and_generic().one_statement_parses_to("SELECT 0xdeadBEEF", "SELECT X'deadBEEF'");
}

#[test]
fn parse_mssql_create_role() {
    let sql = "CREATE ROLE mssql AUTHORIZATION helena";
    match ms().verified_stmt(sql) {
        Statement::CreateRole {
            names,
            authorization_owner,
            ..
        } => {
            assert_eq_vec(&["mssql"], &names);
            assert_eq!(
                authorization_owner,
                Some(ObjectName(vec![Ident {
                    value: "helena".into(),
                    quote_style: None
                }]))
            );
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_alter_role() {
    let sql = "ALTER ROLE old_name WITH NAME = new_name";
    assert_eq!(
        ms().parse_sql_statements(sql).unwrap(),
        [Statement::AlterRole {
            name: Ident {
                value: "old_name".into(),
                quote_style: None
            },
            operation: AlterRoleOperation::RenameRole {
                role_name: Ident {
                    value: "new_name".into(),
                    quote_style: None
                }
            },
        }]
    );

    let sql = "ALTER ROLE role_name ADD MEMBER new_member";
    assert_eq!(
        ms().verified_stmt(sql),
        Statement::AlterRole {
            name: Ident {
                value: "role_name".into(),
                quote_style: None
            },
            operation: AlterRoleOperation::AddMember {
                member_name: Ident {
                    value: "new_member".into(),
                    quote_style: None
                }
            },
        }
    );

    let sql = "ALTER ROLE role_name DROP MEMBER old_member";
    assert_eq!(
        ms().verified_stmt(sql),
        Statement::AlterRole {
            name: Ident {
                value: "role_name".into(),
                quote_style: None
            },
            operation: AlterRoleOperation::DropMember {
                member_name: Ident {
                    value: "old_member".into(),
                    quote_style: None
                }
            },
        }
    );
}

#[test]
fn parse_delimited_identifiers() {
    // check that quoted identifiers in any position remain quoted after serialization
    let select = ms_and_generic().verified_only_select(
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
            args: vec![],
            null_treatment: None,
            filter: None,
            over: None,
            distinct: false,
            special: false,
            order_by: vec![],
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

    ms_and_generic().verified_stmt(r#"CREATE TABLE "foo" ("bar" "int")"#);
    ms_and_generic().verified_stmt(r#"ALTER TABLE foo ADD CONSTRAINT "bar" PRIMARY KEY (baz)"#);
    //TODO verified_stmt(r#"UPDATE foo SET "bar" = 5"#);
}

#[test]
fn parse_table_name_in_square_brackets() {
    let select = ms().verified_only_select(r#"SELECT [a column] FROM [a schema].[a table]"#);
    if let TableFactor::Table { name, .. } = only(select.from).relation {
        assert_eq!(
            vec![
                Ident::with_quote('[', "a schema"),
                Ident::with_quote('[', "a table")
            ],
            name.0
        );
    } else {
        panic!("Expecting TableFactor::Table");
    }
    assert_eq!(
        &Expr::Identifier(Ident::with_quote('[', "a column")),
        expr_from_projection(&select.projection[0]),
    );
}

#[test]
fn parse_like() {
    fn chk(negated: bool) {
        let sql = &format!(
            "SELECT * FROM customers WHERE name {}LIKE '%a'",
            if negated { "NOT " } else { "" }
        );
        let select = ms_and_generic().verified_only_select(sql);
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
        let select = ms_and_generic().verified_only_select(sql);
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
        let select = ms_and_generic().verified_only_select(sql);
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
fn parse_for_clause() {
    ms_and_generic().verified_stmt("SELECT a FROM t FOR JSON PATH");
    ms_and_generic().verified_stmt("SELECT b FROM t FOR JSON AUTO");
    ms_and_generic().verified_stmt("SELECT c FROM t FOR JSON AUTO, WITHOUT_ARRAY_WRAPPER");
    ms_and_generic().verified_stmt("SELECT 1 FROM t FOR JSON PATH, ROOT('x'), INCLUDE_NULL_VALUES");
    ms_and_generic().verified_stmt("SELECT 2 FROM t FOR XML AUTO");
    ms_and_generic().verified_stmt("SELECT 3 FROM t FOR XML AUTO, TYPE, ELEMENTS");
    ms_and_generic().verified_stmt("SELECT * FROM t WHERE x FOR XML AUTO, ELEMENTS");
    ms_and_generic().verified_stmt("SELECT x FROM t ORDER BY y FOR XML AUTO, ELEMENTS");
    ms_and_generic().verified_stmt("SELECT y FROM t FOR XML PATH('x'), ROOT('y'), ELEMENTS");
    ms_and_generic().verified_stmt("SELECT z FROM t FOR XML EXPLICIT, BINARY BASE64");
    ms_and_generic().verified_stmt("SELECT * FROM t FOR XML RAW('x')");
    ms_and_generic().verified_stmt("SELECT * FROM t FOR BROWSE");
}

#[test]
fn dont_parse_trailing_for() {
    assert!(ms()
        .run_parser_method("SELECT * FROM foo FOR", |p| p.parse_query())
        .is_err());
}

#[test]
fn parse_for_json_expect_ast() {
    assert_eq!(
        ms().verified_query("SELECT * FROM t FOR JSON PATH, ROOT('root')")
            .for_clause
            .unwrap(),
        ForClause::Json {
            for_json: ForJson::Path,
            root: Some("root".into()),
            without_array_wrapper: false,
            include_null_values: false,
        }
    );
}

#[test]
fn parse_cast_varchar_max() {
    ms_and_generic().verified_expr("CAST('foo' AS VARCHAR(MAX))");
}

#[test]
fn parse_convert() {
    ms().verified_expr("CONVERT(VARCHAR(MAX), 'foo')");
    ms().verified_expr("CONVERT(VARCHAR(10), 'foo')");
    ms().verified_expr("CONVERT(DECIMAL(10,5), 12.55)");
}

#[test]
fn parse_similar_to() {
    fn chk(negated: bool) {
        let sql = &format!(
            "SELECT * FROM customers WHERE name {}SIMILAR TO '%a'",
            if negated { "NOT " } else { "" }
        );
        let select = ms_and_generic().verified_only_select(sql);
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
        let select = ms_and_generic().verified_only_select(sql);
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
        let select = ms_and_generic().verified_only_select(sql);
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
fn parse_substring_in_select() {
    let sql = "SELECT DISTINCT SUBSTRING(description, 0, 1) FROM test";
    match ms().one_statement_parses_to(
        sql,
        "SELECT DISTINCT SUBSTRING(description, 0, 1) FROM test",
    ) {
        Statement::Query(query) => {
            assert_eq!(
                Box::new(Query {
                    with: None,

                    body: Box::new(SetExpr::Select(Box::new(Select {
                        distinct: Some(Distinct::Distinct),
                        top: None,
                        projection: vec![SelectItem::UnnamedExpr(Expr::Substring {
                            expr: Box::new(Expr::Identifier(Ident {
                                value: "description".to_string(),
                                quote_style: None
                            })),
                            substring_from: Some(Box::new(Expr::Value(number("0")))),
                            substring_for: Some(Box::new(Expr::Value(number("1")))),
                            special: true,
                        })],
                        into: None,
                        from: vec![TableWithJoins {
                            relation: TableFactor::Table {
                                name: ObjectName(vec![Ident {
                                    value: "test".to_string(),
                                    quote_style: None
                                }]),
                                alias: None,
                                args: None,
                                with_hints: vec![],
                                version: None,
                                partitions: vec![],
                            },
                            joins: vec![]
                        }],
                        lateral_views: vec![],
                        selection: None,
                        group_by: GroupByExpr::Expressions(vec![]),
                        cluster_by: vec![],
                        distribute_by: vec![],
                        sort_by: vec![],
                        having: None,
                        named_window: vec![],
                        qualify: None
                    }))),
                    order_by: vec![],
                    limit: None,
                    limit_by: vec![],
                    offset: None,
                    fetch: None,
                    locks: vec![],
                    for_clause: None,
                }),
                query
            );
        }
        _ => unreachable!(),
    }
}

fn ms() -> TestedDialects {
    TestedDialects {
        dialects: vec![Box::new(MsSqlDialect {})],
        options: None,
    }
}
fn ms_and_generic() -> TestedDialects {
    TestedDialects {
        dialects: vec![Box::new(MsSqlDialect {}), Box::new(GenericDialect {})],
        options: None,
    }
}

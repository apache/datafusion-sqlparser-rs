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
//! Test SQL syntax specific to Snowflake. The parser based on the
//! generic dialect is also tested (on the inputs it can handle).

use sqlparser::ast::helpers::stmt_data_loading::{
    DataLoadingOption, DataLoadingOptionType, StageLoadSelectItem,
};
use sqlparser::ast::*;
use sqlparser::dialect::{GenericDialect, SnowflakeDialect};
use sqlparser::parser::{ParserError, ParserOptions};
use sqlparser::tokenizer::*;
use test_utils::*;

#[macro_use]
mod test_utils;

#[cfg(test)]
use pretty_assertions::assert_eq;

#[test]
fn test_snowflake_create_table() {
    let sql = "CREATE TABLE _my_$table (am00unt number)";
    match snowflake_and_generic().verified_stmt(sql) {
        Statement::CreateTable { name, .. } => {
            assert_eq!("_my_$table", name.to_string());
        }
        _ => unreachable!(),
    }
}

#[test]
fn test_snowflake_create_transient_table() {
    let sql = "CREATE TRANSIENT TABLE CUSTOMER (id INT, name VARCHAR(255))";
    match snowflake_and_generic().verified_stmt(sql) {
        Statement::CreateTable {
            name, transient, ..
        } => {
            assert_eq!("CUSTOMER", name.to_string());
            assert!(transient)
        }
        _ => unreachable!(),
    }
}

#[test]
fn test_snowflake_single_line_tokenize() {
    let sql = "CREATE TABLE# this is a comment \ntable_1";
    let dialect = SnowflakeDialect {};
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

    let sql = "CREATE TABLE // this is a comment \ntable_1";
    let tokens = Tokenizer::new(&dialect, sql).tokenize().unwrap();

    let expected = vec![
        Token::make_keyword("CREATE"),
        Token::Whitespace(Whitespace::Space),
        Token::make_keyword("TABLE"),
        Token::Whitespace(Whitespace::Space),
        Token::Whitespace(Whitespace::SingleLineComment {
            prefix: "//".to_string(),
            comment: " this is a comment \n".to_string(),
        }),
        Token::make_word("table_1", None),
    ];

    assert_eq!(expected, tokens);
}

#[test]
fn test_sf_derived_table_in_parenthesis() {
    // Nesting a subquery in an extra set of parentheses is non-standard,
    // but supported in Snowflake SQL
    snowflake_and_generic().one_statement_parses_to(
        "SELECT * FROM ((SELECT 1) AS t)",
        "SELECT * FROM (SELECT 1) AS t",
    );
    snowflake_and_generic().one_statement_parses_to(
        "SELECT * FROM (((SELECT 1) AS t))",
        "SELECT * FROM (SELECT 1) AS t",
    );
}

#[test]
fn test_single_table_in_parenthesis() {
    // Parenthesized table names are non-standard, but supported in Snowflake SQL
    snowflake_and_generic().one_statement_parses_to(
        "SELECT * FROM (a NATURAL JOIN (b))",
        "SELECT * FROM (a NATURAL JOIN b)",
    );
    snowflake_and_generic().one_statement_parses_to(
        "SELECT * FROM (a NATURAL JOIN ((b)))",
        "SELECT * FROM (a NATURAL JOIN b)",
    );
}

#[test]
fn test_single_table_in_parenthesis_with_alias() {
    snowflake_and_generic().one_statement_parses_to(
        "SELECT * FROM (a NATURAL JOIN (b) c )",
        "SELECT * FROM (a NATURAL JOIN b AS c)",
    );

    snowflake_and_generic().one_statement_parses_to(
        "SELECT * FROM (a NATURAL JOIN ((b)) c )",
        "SELECT * FROM (a NATURAL JOIN b AS c)",
    );

    snowflake_and_generic().one_statement_parses_to(
        "SELECT * FROM (a NATURAL JOIN ( (b) c ) )",
        "SELECT * FROM (a NATURAL JOIN b AS c)",
    );

    snowflake_and_generic().one_statement_parses_to(
        "SELECT * FROM (a NATURAL JOIN ( (b) as c ) )",
        "SELECT * FROM (a NATURAL JOIN b AS c)",
    );

    snowflake_and_generic().one_statement_parses_to(
        "SELECT * FROM (a alias1 NATURAL JOIN ( (b) c ) )",
        "SELECT * FROM (a AS alias1 NATURAL JOIN b AS c)",
    );

    snowflake_and_generic().one_statement_parses_to(
        "SELECT * FROM (a as alias1 NATURAL JOIN ( (b) as c ) )",
        "SELECT * FROM (a AS alias1 NATURAL JOIN b AS c)",
    );

    snowflake_and_generic().one_statement_parses_to(
        "SELECT * FROM (a NATURAL JOIN b) c",
        "SELECT * FROM (a NATURAL JOIN b) AS c",
    );

    let res = snowflake().parse_sql_statements("SELECT * FROM (a b) c");
    assert_eq!(
        ParserError::ParserError("duplicate alias b".to_string()),
        res.unwrap_err()
    );
}

#[test]
fn parse_array() {
    let sql = "SELECT CAST(a AS ARRAY) FROM customer";
    let select = snowflake().verified_only_select(sql);
    assert_eq!(
        &Expr::Cast {
            kind: CastKind::Cast,
            expr: Box::new(Expr::Identifier(Ident::new("a"))),
            data_type: DataType::Array(ArrayElemTypeDef::None),
            format: None,
        },
        expr_from_projection(only(&select.projection))
    );
}

#[test]
fn parse_lateral_flatten() {
    snowflake().verified_only_select(r#"SELECT * FROM TABLE(FLATTEN(input => parse_json('{"a":1, "b":[77,88]}'), outer => true)) AS f"#);
    snowflake().verified_only_select(r#"SELECT emp.employee_ID, emp.last_name, index, value AS project_name FROM employees AS emp, LATERAL FLATTEN(INPUT => emp.project_names) AS proj_names"#);
}

// https://docs.snowflake.com/en/user-guide/querying-semistructured
#[test]
fn parse_semi_structured_data_traversal() {
    // most basic case
    let sql = "SELECT a:b FROM t";
    let select = snowflake().verified_only_select(sql);
    assert_eq!(
        SelectItem::UnnamedExpr(Expr::JsonAccess {
            value: Box::new(Expr::Identifier(Ident::new("a"))),
            path: JsonPath {
                path: vec![JsonPathElem::Dot {
                    key: "b".to_owned(),
                    quoted: false
                }]
            },
        }),
        select.projection[0]
    );

    // identifier can be quoted
    let sql = r#"SELECT a:"my long object key name" FROM t"#;
    let select = snowflake().verified_only_select(sql);
    assert_eq!(
        SelectItem::UnnamedExpr(Expr::JsonAccess {
            value: Box::new(Expr::Identifier(Ident::new("a"))),
            path: JsonPath {
                path: vec![JsonPathElem::Dot {
                    key: "my long object key name".to_owned(),
                    quoted: true
                }]
            },
        }),
        select.projection[0]
    );

    // expressions are allowed in bracket notation
    let sql = r#"SELECT a[2 + 2] FROM t"#;
    let select = snowflake().verified_only_select(sql);
    assert_eq!(
        SelectItem::UnnamedExpr(Expr::JsonAccess {
            value: Box::new(Expr::Identifier(Ident::new("a"))),
            path: JsonPath {
                path: vec![JsonPathElem::Bracket {
                    key: Expr::BinaryOp {
                        left: Box::new(Expr::Value(number("2"))),
                        op: BinaryOperator::Plus,
                        right: Box::new(Expr::Value(number("2")))
                    },
                }]
            },
        }),
        select.projection[0]
    );

    snowflake().verified_stmt("SELECT a:b::INT FROM t");

    // unquoted keywords are permitted in the object key
    let sql = "SELECT a:select, a:from FROM t";
    let select = snowflake().verified_only_select(sql);
    assert_eq!(
        vec![
            SelectItem::UnnamedExpr(Expr::JsonAccess {
                value: Box::new(Expr::Identifier(Ident::new("a"))),
                path: JsonPath {
                    path: vec![JsonPathElem::Dot {
                        key: "select".to_owned(),
                        quoted: false
                    }]
                },
            }),
            SelectItem::UnnamedExpr(Expr::JsonAccess {
                value: Box::new(Expr::Identifier(Ident::new("a"))),
                path: JsonPath {
                    path: vec![JsonPathElem::Dot {
                        key: "from".to_owned(),
                        quoted: false
                    }]
                },
            })
        ],
        select.projection
    );

    // multiple levels can be traversed
    // https://docs.snowflake.com/en/user-guide/querying-semistructured#dot-notation
    let sql = r#"SELECT a:foo."bar".baz"#;
    let select = snowflake().verified_only_select(sql);
    assert_eq!(
        vec![SelectItem::UnnamedExpr(Expr::JsonAccess {
            value: Box::new(Expr::Identifier(Ident::new("a"))),
            path: JsonPath {
                path: vec![
                    JsonPathElem::Dot {
                        key: "foo".to_owned(),
                        quoted: false,
                    },
                    JsonPathElem::Dot {
                        key: "bar".to_owned(),
                        quoted: true,
                    },
                    JsonPathElem::Dot {
                        key: "baz".to_owned(),
                        quoted: false,
                    }
                ]
            },
        })],
        select.projection
    );

    // dot and bracket notation can be mixed (starting with : case)
    // https://docs.snowflake.com/en/user-guide/querying-semistructured#dot-notation
    let sql = r#"SELECT a:foo[0].bar"#;
    let select = snowflake().verified_only_select(sql);
    assert_eq!(
        vec![SelectItem::UnnamedExpr(Expr::JsonAccess {
            value: Box::new(Expr::Identifier(Ident::new("a"))),
            path: JsonPath {
                path: vec![
                    JsonPathElem::Dot {
                        key: "foo".to_owned(),
                        quoted: false,
                    },
                    JsonPathElem::Bracket {
                        key: Expr::Value(number("0")),
                    },
                    JsonPathElem::Dot {
                        key: "bar".to_owned(),
                        quoted: false,
                    }
                ]
            },
        })],
        select.projection
    );

    // dot and bracket notation can be mixed (starting with bracket case)
    // https://docs.snowflake.com/en/user-guide/querying-semistructured#dot-notation
    let sql = r#"SELECT a[0].foo.bar"#;
    let select = snowflake().verified_only_select(sql);
    assert_eq!(
        vec![SelectItem::UnnamedExpr(Expr::JsonAccess {
            value: Box::new(Expr::Identifier(Ident::new("a"))),
            path: JsonPath {
                path: vec![
                    JsonPathElem::Bracket {
                        key: Expr::Value(number("0")),
                    },
                    JsonPathElem::Dot {
                        key: "foo".to_owned(),
                        quoted: false,
                    },
                    JsonPathElem::Dot {
                        key: "bar".to_owned(),
                        quoted: false,
                    }
                ]
            },
        })],
        select.projection
    );
}

#[test]
fn parse_delimited_identifiers() {
    // check that quoted identifiers in any position remain quoted after serialization
    let select = snowflake().verified_only_select(
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
            args: FunctionArguments::List(FunctionArgumentList {
                duplicate_treatment: None,
                args: vec![],
                clauses: vec![],
            }),
            filter: None,
            null_treatment: None,
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

    snowflake().verified_stmt(r#"CREATE TABLE "foo" ("bar" "int")"#);
    snowflake().verified_stmt(r#"ALTER TABLE foo ADD CONSTRAINT "bar" PRIMARY KEY (baz)"#);
    //TODO verified_stmt(r#"UPDATE foo SET "bar" = 5"#);
}

#[test]
fn test_array_agg_func() {
    for sql in [
        "SELECT ARRAY_AGG(x) WITHIN GROUP (ORDER BY x) AS a FROM T",
        "SELECT ARRAY_AGG(DISTINCT x) WITHIN GROUP (ORDER BY x ASC) FROM tbl",
    ] {
        snowflake().verified_stmt(sql);
    }
}

fn snowflake() -> TestedDialects {
    TestedDialects {
        dialects: vec![Box::new(SnowflakeDialect {})],
        options: None,
    }
}

fn snowflake_without_unescape() -> TestedDialects {
    TestedDialects {
        dialects: vec![Box::new(SnowflakeDialect {})],
        options: Some(ParserOptions::new().with_unescape(false)),
    }
}

fn snowflake_and_generic() -> TestedDialects {
    TestedDialects {
        dialects: vec![Box::new(SnowflakeDialect {}), Box::new(GenericDialect {})],
        options: None,
    }
}

#[test]
fn test_select_wildcard_with_exclude() {
    let select = snowflake_and_generic().verified_only_select("SELECT * EXCLUDE (col_a) FROM data");
    let expected = SelectItem::Wildcard(WildcardAdditionalOptions {
        opt_exclude: Some(ExcludeSelectItem::Multiple(vec![Ident::new("col_a")])),
        ..Default::default()
    });
    assert_eq!(expected, select.projection[0]);

    let select = snowflake_and_generic()
        .verified_only_select("SELECT name.* EXCLUDE department_id FROM employee_table");
    let expected = SelectItem::QualifiedWildcard(
        ObjectName(vec![Ident::new("name")]),
        WildcardAdditionalOptions {
            opt_exclude: Some(ExcludeSelectItem::Single(Ident::new("department_id"))),
            ..Default::default()
        },
    );
    assert_eq!(expected, select.projection[0]);

    let select = snowflake_and_generic()
        .verified_only_select("SELECT * EXCLUDE (department_id, employee_id) FROM employee_table");
    let expected = SelectItem::Wildcard(WildcardAdditionalOptions {
        opt_exclude: Some(ExcludeSelectItem::Multiple(vec![
            Ident::new("department_id"),
            Ident::new("employee_id"),
        ])),
        ..Default::default()
    });
    assert_eq!(expected, select.projection[0]);
}

#[test]
fn test_select_wildcard_with_rename() {
    let select =
        snowflake_and_generic().verified_only_select("SELECT * RENAME col_a AS col_b FROM data");
    let expected = SelectItem::Wildcard(WildcardAdditionalOptions {
        opt_rename: Some(RenameSelectItem::Single(IdentWithAlias {
            ident: Ident::new("col_a"),
            alias: Ident::new("col_b"),
        })),
        ..Default::default()
    });
    assert_eq!(expected, select.projection[0]);

    let select = snowflake_and_generic().verified_only_select(
        "SELECT name.* RENAME (department_id AS new_dep, employee_id AS new_emp) FROM employee_table",
    );
    let expected = SelectItem::QualifiedWildcard(
        ObjectName(vec![Ident::new("name")]),
        WildcardAdditionalOptions {
            opt_rename: Some(RenameSelectItem::Multiple(vec![
                IdentWithAlias {
                    ident: Ident::new("department_id"),
                    alias: Ident::new("new_dep"),
                },
                IdentWithAlias {
                    ident: Ident::new("employee_id"),
                    alias: Ident::new("new_emp"),
                },
            ])),
            ..Default::default()
        },
    );
    assert_eq!(expected, select.projection[0]);
}

#[test]
fn test_select_wildcard_with_exclude_and_rename() {
    let select = snowflake_and_generic()
        .verified_only_select("SELECT * EXCLUDE col_z RENAME col_a AS col_b FROM data");
    let expected = SelectItem::Wildcard(WildcardAdditionalOptions {
        opt_exclude: Some(ExcludeSelectItem::Single(Ident::new("col_z"))),
        opt_rename: Some(RenameSelectItem::Single(IdentWithAlias {
            ident: Ident::new("col_a"),
            alias: Ident::new("col_b"),
        })),
        ..Default::default()
    });
    assert_eq!(expected, select.projection[0]);

    // rename cannot precede exclude
    assert_eq!(
        snowflake_and_generic()
            .parse_sql_statements("SELECT * RENAME col_a AS col_b EXCLUDE col_z FROM data")
            .unwrap_err()
            .to_string(),
        "sql parser error: Expected end of statement, found: EXCLUDE"
    );
}

#[test]
fn test_alter_table_swap_with() {
    let sql = "ALTER TABLE tab1 SWAP WITH tab2";
    match alter_table_op_with_name(snowflake_and_generic().verified_stmt(sql), "tab1") {
        AlterTableOperation::SwapWith { table_name } => {
            assert_eq!("tab2", table_name.to_string());
        }
        _ => unreachable!(),
    };
}

#[test]
fn test_drop_stage() {
    match snowflake_and_generic().verified_stmt("DROP STAGE s1") {
        Statement::Drop {
            names, if_exists, ..
        } => {
            assert!(!if_exists);
            assert_eq!("s1", names[0].to_string());
        }
        _ => unreachable!(),
    };
    match snowflake_and_generic().verified_stmt("DROP STAGE IF EXISTS s1") {
        Statement::Drop {
            names, if_exists, ..
        } => {
            assert!(if_exists);
            assert_eq!("s1", names[0].to_string());
        }
        _ => unreachable!(),
    };

    snowflake_and_generic().one_statement_parses_to("DROP STAGE s1", "DROP STAGE s1");

    snowflake_and_generic()
        .one_statement_parses_to("DROP STAGE IF EXISTS s1", "DROP STAGE IF EXISTS s1");
}

#[test]
fn parse_snowflake_declare_cursor() {
    for (sql, expected_name, expected_assigned_expr, expected_query_projections) in [
        (
            "DECLARE c1 CURSOR FOR SELECT id, price FROM invoices",
            "c1",
            None,
            Some(vec!["id", "price"]),
        ),
        (
            "DECLARE c1 CURSOR FOR res",
            "c1",
            Some(DeclareAssignment::For(
                Expr::Identifier(Ident::new("res")).into(),
            )),
            None,
        ),
    ] {
        match snowflake().verified_stmt(sql) {
            Statement::Declare { mut stmts } => {
                assert_eq!(1, stmts.len());
                let Declare {
                    names,
                    data_type,
                    declare_type,
                    assignment: assigned_expr,
                    for_query,
                    ..
                } = stmts.swap_remove(0);
                assert_eq!(vec![Ident::new(expected_name)], names);
                assert!(data_type.is_none());
                assert_eq!(Some(DeclareType::Cursor), declare_type);
                assert_eq!(expected_assigned_expr, assigned_expr);
                assert_eq!(
                    expected_query_projections,
                    for_query.as_ref().map(|q| {
                        match q.body.as_ref() {
                            SetExpr::Select(q) => q
                                .projection
                                .iter()
                                .map(|item| match item {
                                    SelectItem::UnnamedExpr(Expr::Identifier(ident)) => {
                                        ident.value.as_str()
                                    }
                                    _ => unreachable!(),
                                })
                                .collect::<Vec<_>>(),
                            _ => unreachable!(),
                        }
                    })
                )
            }
            _ => unreachable!(),
        }
    }

    let error_sql = "DECLARE c1 CURSOR SELECT id FROM invoices";
    assert_eq!(
        ParserError::ParserError("Expected FOR, found: SELECT".to_owned()),
        snowflake().parse_sql_statements(error_sql).unwrap_err()
    );

    let error_sql = "DECLARE c1 CURSOR res";
    assert_eq!(
        ParserError::ParserError("Expected FOR, found: res".to_owned()),
        snowflake().parse_sql_statements(error_sql).unwrap_err()
    );
}

#[test]
fn parse_snowflake_declare_result_set() {
    for (sql, expected_name, expected_assigned_expr) in [
        (
            "DECLARE res RESULTSET DEFAULT 42",
            "res",
            Some(DeclareAssignment::Default(Expr::Value(number("42")).into())),
        ),
        (
            "DECLARE res RESULTSET := 42",
            "res",
            Some(DeclareAssignment::DuckAssignment(
                Expr::Value(number("42")).into(),
            )),
        ),
        ("DECLARE res RESULTSET", "res", None),
    ] {
        match snowflake().verified_stmt(sql) {
            Statement::Declare { mut stmts } => {
                assert_eq!(1, stmts.len());
                let Declare {
                    names,
                    data_type,
                    declare_type,
                    assignment: assigned_expr,
                    for_query,
                    ..
                } = stmts.swap_remove(0);
                assert_eq!(vec![Ident::new(expected_name)], names);
                assert!(data_type.is_none());
                assert!(for_query.is_none());
                assert_eq!(Some(DeclareType::ResultSet), declare_type);
                assert_eq!(expected_assigned_expr, assigned_expr);
            }
            _ => unreachable!(),
        }
    }

    let sql = "DECLARE res RESULTSET DEFAULT (SELECT price FROM invoices)";
    assert_eq!(snowflake().verified_stmt(sql).to_string(), sql);

    let error_sql = "DECLARE res RESULTSET DEFAULT";
    assert_eq!(
        ParserError::ParserError("Expected an expression:, found: EOF".to_owned()),
        snowflake().parse_sql_statements(error_sql).unwrap_err()
    );

    let error_sql = "DECLARE res RESULTSET :=";
    assert_eq!(
        ParserError::ParserError("Expected an expression:, found: EOF".to_owned()),
        snowflake().parse_sql_statements(error_sql).unwrap_err()
    );
}

#[test]
fn parse_snowflake_declare_exception() {
    for (sql, expected_name, expected_assigned_expr) in [
        (
            "DECLARE ex EXCEPTION (42, 'ERROR')",
            "ex",
            Some(DeclareAssignment::Expr(
                Expr::Tuple(vec![
                    Expr::Value(number("42")),
                    Expr::Value(Value::SingleQuotedString("ERROR".to_string())),
                ])
                .into(),
            )),
        ),
        ("DECLARE ex EXCEPTION", "ex", None),
    ] {
        match snowflake().verified_stmt(sql) {
            Statement::Declare { mut stmts } => {
                assert_eq!(1, stmts.len());
                let Declare {
                    names,
                    data_type,
                    declare_type,
                    assignment: assigned_expr,
                    for_query,
                    ..
                } = stmts.swap_remove(0);
                assert_eq!(vec![Ident::new(expected_name)], names);
                assert!(data_type.is_none());
                assert!(for_query.is_none());
                assert_eq!(Some(DeclareType::Exception), declare_type);
                assert_eq!(expected_assigned_expr, assigned_expr);
            }
            _ => unreachable!(),
        }
    }
}

#[test]
fn parse_snowflake_declare_variable() {
    for (sql, expected_name, expected_data_type, expected_assigned_expr) in [
        (
            "DECLARE profit TEXT DEFAULT 42",
            "profit",
            Some(DataType::Text),
            Some(DeclareAssignment::Default(Expr::Value(number("42")).into())),
        ),
        (
            "DECLARE profit DEFAULT 42",
            "profit",
            None,
            Some(DeclareAssignment::Default(Expr::Value(number("42")).into())),
        ),
        ("DECLARE profit TEXT", "profit", Some(DataType::Text), None),
        ("DECLARE profit", "profit", None, None),
    ] {
        match snowflake().verified_stmt(sql) {
            Statement::Declare { mut stmts } => {
                assert_eq!(1, stmts.len());
                let Declare {
                    names,
                    data_type,
                    declare_type,
                    assignment: assigned_expr,
                    for_query,
                    ..
                } = stmts.swap_remove(0);
                assert_eq!(vec![Ident::new(expected_name)], names);
                assert!(for_query.is_none());
                assert_eq!(expected_data_type, data_type);
                assert_eq!(None, declare_type);
                assert_eq!(expected_assigned_expr, assigned_expr);
            }
            _ => unreachable!(),
        }
    }

    snowflake().one_statement_parses_to("DECLARE profit;", "DECLARE profit");

    let error_sql = "DECLARE profit INT 2";
    assert_eq!(
        ParserError::ParserError("Expected end of statement, found: 2".to_owned()),
        snowflake().parse_sql_statements(error_sql).unwrap_err()
    );

    let error_sql = "DECLARE profit INT DEFAULT";
    assert_eq!(
        ParserError::ParserError("Expected an expression:, found: EOF".to_owned()),
        snowflake().parse_sql_statements(error_sql).unwrap_err()
    );

    let error_sql = "DECLARE profit DEFAULT";
    assert_eq!(
        ParserError::ParserError("Expected an expression:, found: EOF".to_owned()),
        snowflake().parse_sql_statements(error_sql).unwrap_err()
    );
}

#[test]
fn parse_snowflake_declare_multi_statements() {
    let sql = concat!(
        "DECLARE profit DEFAULT 42; ",
        "res RESULTSET DEFAULT (SELECT price FROM invoices); ",
        "c1 CURSOR FOR res; ",
        "ex EXCEPTION (-20003, 'ERROR: Could not create table.')"
    );
    match snowflake().verified_stmt(sql) {
        Statement::Declare { stmts } => {
            let actual = stmts
                .iter()
                .map(|stmt| (stmt.names[0].value.as_str(), stmt.declare_type.clone()))
                .collect::<Vec<_>>();

            assert_eq!(
                vec![
                    ("profit", None),
                    ("res", Some(DeclareType::ResultSet)),
                    ("c1", Some(DeclareType::Cursor)),
                    ("ex", Some(DeclareType::Exception)),
                ],
                actual
            );
        }
        _ => unreachable!(),
    }

    let error_sql = "DECLARE profit DEFAULT 42 c1 CURSOR FOR res;";
    assert_eq!(
        ParserError::ParserError("Expected end of statement, found: c1".to_owned()),
        snowflake().parse_sql_statements(error_sql).unwrap_err()
    );
}

#[test]
fn test_create_stage() {
    let sql = "CREATE STAGE s1.s2";
    match snowflake().verified_stmt(sql) {
        Statement::CreateStage {
            or_replace,
            temporary,
            if_not_exists,
            name,
            comment,
            ..
        } => {
            assert!(!or_replace);
            assert!(!temporary);
            assert!(!if_not_exists);
            assert_eq!("s1.s2", name.to_string());
            assert!(comment.is_none());
        }
        _ => unreachable!(),
    };
    assert_eq!(snowflake().verified_stmt(sql).to_string(), sql);

    let extended_sql = concat!(
        "CREATE OR REPLACE TEMPORARY STAGE IF NOT EXISTS s1.s2 ",
        "COMMENT='some-comment'"
    );
    match snowflake().verified_stmt(extended_sql) {
        Statement::CreateStage {
            or_replace,
            temporary,
            if_not_exists,
            name,
            stage_params,
            comment,
            ..
        } => {
            assert!(or_replace);
            assert!(temporary);
            assert!(if_not_exists);
            assert!(stage_params.url.is_none());
            assert!(stage_params.endpoint.is_none());
            assert_eq!("s1.s2", name.to_string());
            assert_eq!("some-comment", comment.unwrap());
        }
        _ => unreachable!(),
    };
    assert_eq!(
        snowflake().verified_stmt(extended_sql).to_string(),
        extended_sql
    );
}

#[test]
fn test_create_stage_with_stage_params() {
    let sql = concat!(
        "CREATE OR REPLACE STAGE my_ext_stage ",
        "URL='s3://load/files/' ",
        "STORAGE_INTEGRATION=myint ",
        "ENDPOINT='<s3_api_compatible_endpoint>' ",
        "CREDENTIALS=(AWS_KEY_ID='1a2b3c' AWS_SECRET_KEY='4x5y6z') ",
        "ENCRYPTION=(MASTER_KEY='key' TYPE='AWS_SSE_KMS')"
    );

    match snowflake().verified_stmt(sql) {
        Statement::CreateStage { stage_params, .. } => {
            assert_eq!("s3://load/files/", stage_params.url.unwrap());
            assert_eq!("myint", stage_params.storage_integration.unwrap());
            assert_eq!(
                "<s3_api_compatible_endpoint>",
                stage_params.endpoint.unwrap()
            );
            assert!(stage_params
                .credentials
                .options
                .contains(&DataLoadingOption {
                    option_name: "AWS_KEY_ID".to_string(),
                    option_type: DataLoadingOptionType::STRING,
                    value: "1a2b3c".to_string()
                }));
            assert!(stage_params
                .credentials
                .options
                .contains(&DataLoadingOption {
                    option_name: "AWS_SECRET_KEY".to_string(),
                    option_type: DataLoadingOptionType::STRING,
                    value: "4x5y6z".to_string()
                }));
            assert!(stage_params
                .encryption
                .options
                .contains(&DataLoadingOption {
                    option_name: "MASTER_KEY".to_string(),
                    option_type: DataLoadingOptionType::STRING,
                    value: "key".to_string()
                }));
            assert!(stage_params
                .encryption
                .options
                .contains(&DataLoadingOption {
                    option_name: "TYPE".to_string(),
                    option_type: DataLoadingOptionType::STRING,
                    value: "AWS_SSE_KMS".to_string()
                }));
        }
        _ => unreachable!(),
    };

    assert_eq!(snowflake().verified_stmt(sql).to_string(), sql);
}

#[test]
fn test_create_stage_with_directory_table_params() {
    let sql = concat!(
        "CREATE OR REPLACE STAGE my_ext_stage ",
        "URL='s3://load/files/' ",
        "DIRECTORY=(ENABLE=TRUE REFRESH_ON_CREATE=FALSE NOTIFICATION_INTEGRATION='some-string')"
    );

    match snowflake().verified_stmt(sql) {
        Statement::CreateStage {
            directory_table_params,
            ..
        } => {
            assert!(directory_table_params.options.contains(&DataLoadingOption {
                option_name: "ENABLE".to_string(),
                option_type: DataLoadingOptionType::BOOLEAN,
                value: "TRUE".to_string()
            }));
            assert!(directory_table_params.options.contains(&DataLoadingOption {
                option_name: "REFRESH_ON_CREATE".to_string(),
                option_type: DataLoadingOptionType::BOOLEAN,
                value: "FALSE".to_string()
            }));
            assert!(directory_table_params.options.contains(&DataLoadingOption {
                option_name: "NOTIFICATION_INTEGRATION".to_string(),
                option_type: DataLoadingOptionType::STRING,
                value: "some-string".to_string()
            }));
        }
        _ => unreachable!(),
    };
    assert_eq!(snowflake().verified_stmt(sql).to_string(), sql);
}

#[test]
fn test_create_stage_with_file_format() {
    let sql = concat!(
        "CREATE OR REPLACE STAGE my_ext_stage ",
        "URL='s3://load/files/' ",
        r#"FILE_FORMAT=(COMPRESSION=AUTO BINARY_FORMAT=HEX ESCAPE='\\')"#
    );

    match snowflake_without_unescape().verified_stmt(sql) {
        Statement::CreateStage { file_format, .. } => {
            assert!(file_format.options.contains(&DataLoadingOption {
                option_name: "COMPRESSION".to_string(),
                option_type: DataLoadingOptionType::ENUM,
                value: "AUTO".to_string()
            }));
            assert!(file_format.options.contains(&DataLoadingOption {
                option_name: "BINARY_FORMAT".to_string(),
                option_type: DataLoadingOptionType::ENUM,
                value: "HEX".to_string()
            }));
            assert!(file_format.options.contains(&DataLoadingOption {
                option_name: "ESCAPE".to_string(),
                option_type: DataLoadingOptionType::STRING,
                value: r#"\\"#.to_string()
            }));
        }
        _ => unreachable!(),
    };
    assert_eq!(
        snowflake_without_unescape().verified_stmt(sql).to_string(),
        sql
    );
}

#[test]
fn test_create_stage_with_copy_options() {
    let sql = concat!(
        "CREATE OR REPLACE STAGE my_ext_stage ",
        "URL='s3://load/files/' ",
        "COPY_OPTIONS=(ON_ERROR=CONTINUE FORCE=TRUE)"
    );
    match snowflake().verified_stmt(sql) {
        Statement::CreateStage { copy_options, .. } => {
            assert!(copy_options.options.contains(&DataLoadingOption {
                option_name: "ON_ERROR".to_string(),
                option_type: DataLoadingOptionType::ENUM,
                value: "CONTINUE".to_string()
            }));
            assert!(copy_options.options.contains(&DataLoadingOption {
                option_name: "FORCE".to_string(),
                option_type: DataLoadingOptionType::BOOLEAN,
                value: "TRUE".to_string()
            }));
        }
        _ => unreachable!(),
    };
    assert_eq!(snowflake().verified_stmt(sql).to_string(), sql);
}

#[test]
fn test_copy_into() {
    let sql = concat!(
        "COPY INTO my_company.emp_basic ",
        "FROM 'gcs://mybucket/./../a.csv'"
    );
    match snowflake().verified_stmt(sql) {
        Statement::CopyIntoSnowflake {
            into,
            from_stage,
            files,
            pattern,
            validation_mode,
            ..
        } => {
            assert_eq!(
                into,
                ObjectName(vec![Ident::new("my_company"), Ident::new("emp_basic")])
            );
            assert_eq!(
                from_stage,
                ObjectName(vec![Ident::with_quote('\'', "gcs://mybucket/./../a.csv")])
            );
            assert!(files.is_none());
            assert!(pattern.is_none());
            assert!(validation_mode.is_none());
        }
        _ => unreachable!(),
    };
    assert_eq!(snowflake().verified_stmt(sql).to_string(), sql);
}

#[test]
fn test_copy_into_with_stage_params() {
    let sql = concat!(
        "COPY INTO my_company.emp_basic ",
        "FROM 's3://load/files/' ",
        "STORAGE_INTEGRATION=myint ",
        "ENDPOINT='<s3_api_compatible_endpoint>' ",
        "CREDENTIALS=(AWS_KEY_ID='1a2b3c' AWS_SECRET_KEY='4x5y6z') ",
        "ENCRYPTION=(MASTER_KEY='key' TYPE='AWS_SSE_KMS')"
    );

    match snowflake().verified_stmt(sql) {
        Statement::CopyIntoSnowflake {
            from_stage,
            stage_params,
            ..
        } => {
            //assert_eq!("s3://load/files/", stage_params.url.unwrap());
            assert_eq!(
                from_stage,
                ObjectName(vec![Ident::with_quote('\'', "s3://load/files/")])
            );
            assert_eq!("myint", stage_params.storage_integration.unwrap());
            assert_eq!(
                "<s3_api_compatible_endpoint>",
                stage_params.endpoint.unwrap()
            );
            assert!(stage_params
                .credentials
                .options
                .contains(&DataLoadingOption {
                    option_name: "AWS_KEY_ID".to_string(),
                    option_type: DataLoadingOptionType::STRING,
                    value: "1a2b3c".to_string()
                }));
            assert!(stage_params
                .credentials
                .options
                .contains(&DataLoadingOption {
                    option_name: "AWS_SECRET_KEY".to_string(),
                    option_type: DataLoadingOptionType::STRING,
                    value: "4x5y6z".to_string()
                }));
            assert!(stage_params
                .encryption
                .options
                .contains(&DataLoadingOption {
                    option_name: "MASTER_KEY".to_string(),
                    option_type: DataLoadingOptionType::STRING,
                    value: "key".to_string()
                }));
            assert!(stage_params
                .encryption
                .options
                .contains(&DataLoadingOption {
                    option_name: "TYPE".to_string(),
                    option_type: DataLoadingOptionType::STRING,
                    value: "AWS_SSE_KMS".to_string()
                }));
        }
        _ => unreachable!(),
    };

    assert_eq!(snowflake().verified_stmt(sql).to_string(), sql);

    // stage params within copy into with transformations
    let sql = concat!(
        "COPY INTO my_company.emp_basic FROM ",
        "(SELECT t1.$1 FROM 's3://load/files/' STORAGE_INTEGRATION=myint)",
    );

    match snowflake().verified_stmt(sql) {
        Statement::CopyIntoSnowflake {
            from_stage,
            stage_params,
            ..
        } => {
            assert_eq!(
                from_stage,
                ObjectName(vec![Ident::with_quote('\'', "s3://load/files/")])
            );
            assert_eq!("myint", stage_params.storage_integration.unwrap());
        }
        _ => unreachable!(),
    }
}

#[test]
fn test_copy_into_with_files_and_pattern_and_verification() {
    let sql = concat!(
        "COPY INTO my_company.emp_basic ",
        "FROM 'gcs://mybucket/./../a.csv' AS some_alias ",
        "FILES = ('file1.json', 'file2.json') ",
        "PATTERN = '.*employees0[1-5].csv.gz' ",
        "VALIDATION_MODE = RETURN_7_ROWS"
    );

    match snowflake().verified_stmt(sql) {
        Statement::CopyIntoSnowflake {
            files,
            pattern,
            validation_mode,
            from_stage_alias,
            ..
        } => {
            assert_eq!(files.unwrap(), vec!["file1.json", "file2.json"]);
            assert_eq!(pattern.unwrap(), ".*employees0[1-5].csv.gz");
            assert_eq!(validation_mode.unwrap(), "RETURN_7_ROWS");
            assert_eq!(from_stage_alias.unwrap(), Ident::new("some_alias"));
        }
        _ => unreachable!(),
    }
    assert_eq!(snowflake().verified_stmt(sql).to_string(), sql);
}

#[test]
fn test_copy_into_with_transformations() {
    let sql = concat!(
        "COPY INTO my_company.emp_basic FROM ",
        "(SELECT t1.$1:st AS st, $1:index, t2.$1 FROM @schema.general_finished AS T) ",
        "FILES = ('file1.json', 'file2.json') ",
        "PATTERN = '.*employees0[1-5].csv.gz' ",
        "VALIDATION_MODE = RETURN_7_ROWS"
    );

    match snowflake().verified_stmt(sql) {
        Statement::CopyIntoSnowflake {
            from_stage,
            from_transformations,
            ..
        } => {
            assert_eq!(
                from_stage,
                ObjectName(vec![Ident::new("@schema"), Ident::new("general_finished")])
            );
            assert_eq!(
                from_transformations.as_ref().unwrap()[0],
                StageLoadSelectItem {
                    alias: Some(Ident::new("t1")),
                    file_col_num: 1,
                    element: Some(Ident::new("st")),
                    item_as: Some(Ident::new("st"))
                }
            );
            assert_eq!(
                from_transformations.as_ref().unwrap()[1],
                StageLoadSelectItem {
                    alias: None,
                    file_col_num: 1,
                    element: Some(Ident::new("index")),
                    item_as: None
                }
            );
            assert_eq!(
                from_transformations.as_ref().unwrap()[2],
                StageLoadSelectItem {
                    alias: Some(Ident::new("t2")),
                    file_col_num: 1,
                    element: None,
                    item_as: None
                }
            );
        }
        _ => unreachable!(),
    }
    assert_eq!(snowflake().verified_stmt(sql).to_string(), sql);
}

#[test]
fn test_copy_into_file_format() {
    let sql = concat!(
        "COPY INTO my_company.emp_basic ",
        "FROM 'gcs://mybucket/./../a.csv' ",
        "FILES = ('file1.json', 'file2.json') ",
        "PATTERN = '.*employees0[1-5].csv.gz' ",
        r#"FILE_FORMAT=(COMPRESSION=AUTO BINARY_FORMAT=HEX ESCAPE='\\')"#
    );

    match snowflake_without_unescape().verified_stmt(sql) {
        Statement::CopyIntoSnowflake { file_format, .. } => {
            assert!(file_format.options.contains(&DataLoadingOption {
                option_name: "COMPRESSION".to_string(),
                option_type: DataLoadingOptionType::ENUM,
                value: "AUTO".to_string()
            }));
            assert!(file_format.options.contains(&DataLoadingOption {
                option_name: "BINARY_FORMAT".to_string(),
                option_type: DataLoadingOptionType::ENUM,
                value: "HEX".to_string()
            }));
            assert!(file_format.options.contains(&DataLoadingOption {
                option_name: "ESCAPE".to_string(),
                option_type: DataLoadingOptionType::STRING,
                value: r#"\\"#.to_string()
            }));
        }
        _ => unreachable!(),
    }
    assert_eq!(
        snowflake_without_unescape().verified_stmt(sql).to_string(),
        sql
    );
}

#[test]
fn test_copy_into_copy_options() {
    let sql = concat!(
        "COPY INTO my_company.emp_basic ",
        "FROM 'gcs://mybucket/./../a.csv' ",
        "FILES = ('file1.json', 'file2.json') ",
        "PATTERN = '.*employees0[1-5].csv.gz' ",
        "COPY_OPTIONS=(ON_ERROR=CONTINUE FORCE=TRUE)"
    );

    match snowflake().verified_stmt(sql) {
        Statement::CopyIntoSnowflake { copy_options, .. } => {
            assert!(copy_options.options.contains(&DataLoadingOption {
                option_name: "ON_ERROR".to_string(),
                option_type: DataLoadingOptionType::ENUM,
                value: "CONTINUE".to_string()
            }));
            assert!(copy_options.options.contains(&DataLoadingOption {
                option_name: "FORCE".to_string(),
                option_type: DataLoadingOptionType::BOOLEAN,
                value: "TRUE".to_string()
            }));
        }
        _ => unreachable!(),
    };
    assert_eq!(snowflake().verified_stmt(sql).to_string(), sql);
}

#[test]
fn test_snowflake_stage_object_names() {
    let allowed_formatted_names = [
        "my_company.emp_basic",
        "@namespace.%table_name",
        "@namespace.%table_name/path",
        "@namespace.stage_name/path",
        "@~/path",
    ];
    let mut allowed_object_names = [
        ObjectName(vec![Ident::new("my_company"), Ident::new("emp_basic")]),
        ObjectName(vec![Ident::new("@namespace"), Ident::new("%table_name")]),
        ObjectName(vec![
            Ident::new("@namespace"),
            Ident::new("%table_name/path"),
        ]),
        ObjectName(vec![
            Ident::new("@namespace"),
            Ident::new("stage_name/path"),
        ]),
        ObjectName(vec![Ident::new("@~/path")]),
    ];

    for it in allowed_formatted_names
        .iter()
        .zip(allowed_object_names.iter_mut())
    {
        let (formatted_name, object_name) = it;
        let sql = format!(
            "COPY INTO {} FROM 'gcs://mybucket/./../a.csv'",
            formatted_name
        );
        match snowflake().verified_stmt(&sql) {
            Statement::CopyIntoSnowflake { into, .. } => {
                assert_eq!(into.0, object_name.0)
            }
            _ => unreachable!(),
        }
    }
}

#[test]
fn test_snowflake_copy_into() {
    let sql = "COPY INTO a.b FROM @namespace.stage_name";
    assert_eq!(snowflake().verified_stmt(sql).to_string(), sql);
    match snowflake().verified_stmt(sql) {
        Statement::CopyIntoSnowflake {
            into, from_stage, ..
        } => {
            assert_eq!(into, ObjectName(vec![Ident::new("a"), Ident::new("b")]));
            assert_eq!(
                from_stage,
                ObjectName(vec![Ident::new("@namespace"), Ident::new("stage_name")])
            )
        }
        _ => unreachable!(),
    }
}

#[test]
fn test_snowflake_copy_into_stage_name_ends_with_parens() {
    let sql = "COPY INTO SCHEMA.SOME_MONITORING_SYSTEM FROM (SELECT t.$1:st AS st FROM @schema.general_finished)";
    assert_eq!(snowflake().verified_stmt(sql).to_string(), sql);
    match snowflake().verified_stmt(sql) {
        Statement::CopyIntoSnowflake {
            into, from_stage, ..
        } => {
            assert_eq!(
                into,
                ObjectName(vec![
                    Ident::new("SCHEMA"),
                    Ident::new("SOME_MONITORING_SYSTEM")
                ])
            );
            assert_eq!(
                from_stage,
                ObjectName(vec![Ident::new("@schema"), Ident::new("general_finished")])
            )
        }
        _ => unreachable!(),
    }
}

#[test]
fn test_snowflake_trim() {
    let real_sql = r#"SELECT customer_id, TRIM(sub_items.value:item_price_id, '"', "a") AS item_price_id FROM models_staging.subscriptions"#;
    assert_eq!(snowflake().verified_stmt(real_sql).to_string(), real_sql);

    let sql_only_select = "SELECT TRIM('xyz', 'a')";
    let select = snowflake().verified_only_select(sql_only_select);
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
        snowflake().parse_sql_statements(error_sql).unwrap_err()
    );
}

#[test]
fn test_number_placeholder() {
    let sql_only_select = "SELECT :1";
    let select = snowflake().verified_only_select(sql_only_select);
    assert_eq!(
        &Expr::Value(Value::Placeholder(":1".into())),
        expr_from_projection(only(&select.projection))
    );

    snowflake()
        .parse_sql_statements("alter role 1 with name = 'foo'")
        .expect_err("should have failed");
}

#[test]
fn parse_position_not_function_columns() {
    snowflake_and_generic()
        .verified_stmt("SELECT position FROM tbl1 WHERE position NOT IN ('first', 'last')");
}

#[test]
fn parse_subquery_function_argument() {
    // Snowflake allows passing an unparenthesized subquery as the single
    // argument to a function.
    snowflake().verified_stmt("SELECT parse_json(SELECT '{}')");

    // Subqueries that begin with WITH work too.
    snowflake()
        .verified_stmt("SELECT parse_json(WITH q AS (SELECT '{}' AS foo) SELECT foo FROM q)");

    // Commas are parsed as part of the subquery, not additional arguments to
    // the function.
    snowflake().verified_stmt("SELECT func(SELECT 1, 2)");
}

#[test]
fn parse_division_correctly() {
    snowflake_and_generic().one_statement_parses_to(
        "SELECT field/1000 FROM tbl1",
        "SELECT field / 1000 FROM tbl1",
    );

    snowflake_and_generic().one_statement_parses_to(
        "SELECT tbl1.field/tbl2.field FROM tbl1 JOIN tbl2 ON tbl1.id = tbl2.entity_id",
        "SELECT tbl1.field / tbl2.field FROM tbl1 JOIN tbl2 ON tbl1.id = tbl2.entity_id",
    );
}

#[test]
fn parse_pivot_of_table_factor_derived() {
    snowflake().verified_stmt(
        "SELECT * FROM (SELECT place_id, weekday, open FROM times AS p) PIVOT(max(open) FOR weekday IN (0, 1, 2, 3, 4, 5, 6)) AS p (place_id, open_sun, open_mon, open_tue, open_wed, open_thu, open_fri, open_sat)"
    );
}

#[test]
fn parse_top() {
    snowflake().one_statement_parses_to(
        "SELECT TOP 4 c1 FROM testtable",
        "SELECT TOP 4 c1 FROM testtable",
    );
}

#[test]
fn parse_extract_custom_part() {
    let sql = "SELECT EXTRACT(eod FROM d)";
    let select = snowflake_and_generic().verified_only_select(sql);
    assert_eq!(
        &Expr::Extract {
            field: DateTimeField::Custom(Ident::new("eod")),
            expr: Box::new(Expr::Identifier(Ident::new("d"))),
        },
        expr_from_projection(only(&select.projection)),
    );
}

#[test]
fn parse_comma_outer_join() {
    // compound identifiers
    let case1 =
        snowflake().verified_only_select("SELECT t1.c1, t2.c2 FROM t1, t2 WHERE t1.c1 = t2.c2 (+)");
    assert_eq!(
        case1.selection,
        Some(Expr::BinaryOp {
            left: Box::new(Expr::CompoundIdentifier(vec![
                Ident::new("t1"),
                Ident::new("c1")
            ])),
            op: BinaryOperator::Eq,
            right: Box::new(Expr::OuterJoin(Box::new(Expr::CompoundIdentifier(vec![
                Ident::new("t2"),
                Ident::new("c2")
            ]))))
        })
    );

    // regular identifiers
    let case2 =
        snowflake().verified_only_select("SELECT t1.c1, t2.c2 FROM t1, t2 WHERE c1 = c2 (+)");
    assert_eq!(
        case2.selection,
        Some(Expr::BinaryOp {
            left: Box::new(Expr::Identifier(Ident::new("c1"))),
            op: BinaryOperator::Eq,
            right: Box::new(Expr::OuterJoin(Box::new(Expr::Identifier(Ident::new(
                "c2"
            )))))
        })
    );

    // ensure we can still parse function calls with a unary plus arg
    let case3 =
        snowflake().verified_only_select("SELECT t1.c1, t2.c2 FROM t1, t2 WHERE c1 = myudf(+42)");
    assert_eq!(
        case3.selection,
        Some(Expr::BinaryOp {
            left: Box::new(Expr::Identifier(Ident::new("c1"))),
            op: BinaryOperator::Eq,
            right: Box::new(call(
                "myudf",
                [Expr::UnaryOp {
                    op: UnaryOperator::Plus,
                    expr: Box::new(Expr::Value(number("42")))
                }]
            )),
        })
    );

    // permissive with whitespace
    snowflake().verified_only_select_with_canonical(
        "SELECT t1.c1, t2.c2 FROM t1, t2 WHERE t1.c1 = t2.c2(   +     )",
        "SELECT t1.c1, t2.c2 FROM t1, t2 WHERE t1.c1 = t2.c2 (+)",
    );
}

#[test]
fn test_sf_trailing_commas() {
    snowflake().verified_only_select_with_canonical("SELECT 1, 2, FROM t", "SELECT 1, 2 FROM t");
}

#[test]
fn test_select_wildcard_with_ilike() {
    let select = snowflake_and_generic().verified_only_select(r#"SELECT * ILIKE '%id%' FROM tbl"#);
    let expected = SelectItem::Wildcard(WildcardAdditionalOptions {
        opt_ilike: Some(IlikeSelectItem {
            pattern: "%id%".to_owned(),
        }),
        ..Default::default()
    });
    assert_eq!(expected, select.projection[0]);
}

#[test]
fn test_select_wildcard_with_ilike_double_quote() {
    let res = snowflake().parse_sql_statements(r#"SELECT * ILIKE "%id" FROM tbl"#);
    assert_eq!(
        res.unwrap_err().to_string(),
        "sql parser error: Expected ilike pattern, found: \"%id\""
    );
}

#[test]
fn test_select_wildcard_with_ilike_number() {
    let res = snowflake().parse_sql_statements(r#"SELECT * ILIKE 42 FROM tbl"#);
    assert_eq!(
        res.unwrap_err().to_string(),
        "sql parser error: Expected ilike pattern, found: 42"
    );
}

#[test]
fn test_select_wildcard_with_ilike_replace() {
    let res = snowflake().parse_sql_statements(r#"SELECT * ILIKE '%id%' EXCLUDE col FROM tbl"#);
    assert_eq!(
        res.unwrap_err().to_string(),
        "sql parser error: Expected end of statement, found: EXCLUDE"
    );
}

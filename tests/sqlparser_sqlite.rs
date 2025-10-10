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

#![warn(clippy::all)]
//! Test SQL syntax specific to SQLite. The parser based on the
//! generic dialect is also tested (on the inputs it can handle).

#[macro_use]
mod test_utils;

use sqlparser::keywords::Keyword;
use test_utils::*;

use sqlparser::ast::SelectItem::UnnamedExpr;
use sqlparser::ast::Value::Placeholder;
use sqlparser::ast::*;
use sqlparser::dialect::{GenericDialect, SQLiteDialect};
use sqlparser::parser::{ParserError, ParserOptions};
use sqlparser::tokenizer::Token;

#[test]
fn pragma_no_value() {
    let sql = "PRAGMA cache_size";
    match sqlite_and_generic().verified_stmt(sql) {
        Statement::Pragma {
            name,
            value: None,
            is_eq: false,
        } => {
            assert_eq!("cache_size", name.to_string());
        }
        _ => unreachable!(),
    }
}
#[test]
fn pragma_eq_style() {
    let sql = "PRAGMA cache_size = 10";
    match sqlite_and_generic().verified_stmt(sql) {
        Statement::Pragma {
            name,
            value: Some(val),
            is_eq: true,
        } => {
            assert_eq!("cache_size", name.to_string());
            assert_eq!("10", val.to_string());
        }
        _ => unreachable!(),
    }
}
#[test]
fn pragma_function_style() {
    let sql = "PRAGMA cache_size(10)";
    match sqlite_and_generic().verified_stmt(sql) {
        Statement::Pragma {
            name,
            value: Some(val),
            is_eq: false,
        } => {
            assert_eq!("cache_size", name.to_string());
            assert_eq!("10", val.to_string());
        }
        _ => unreachable!(),
    }
}

#[test]
fn pragma_eq_string_style() {
    let sql = "PRAGMA table_info = 'sqlite_master'";
    match sqlite_and_generic().verified_stmt(sql) {
        Statement::Pragma {
            name,
            value: Some(val),
            is_eq: true,
        } => {
            assert_eq!("table_info", name.to_string());
            assert_eq!("'sqlite_master'", val.to_string());
        }
        _ => unreachable!(),
    }
}

#[test]
fn pragma_function_string_style() {
    let sql = "PRAGMA table_info(\"sqlite_master\")";
    match sqlite_and_generic().verified_stmt(sql) {
        Statement::Pragma {
            name,
            value: Some(val),
            is_eq: false,
        } => {
            assert_eq!("table_info", name.to_string());
            assert_eq!("\"sqlite_master\"", val.to_string());
        }
        _ => unreachable!(),
    }
}

#[test]
fn pragma_eq_placeholder_style() {
    let sql = "PRAGMA table_info = ?";
    match sqlite_and_generic().verified_stmt(sql) {
        Statement::Pragma {
            name,
            value: Some(val),
            is_eq: true,
        } => {
            assert_eq!("table_info", name.to_string());
            assert_eq!("?", val.to_string());
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_table_without_rowid() {
    let sql = "CREATE TABLE t (a INT) WITHOUT ROWID";
    match sqlite_and_generic().verified_stmt(sql) {
        Statement::CreateTable(CreateTable {
            name,
            without_rowid: true,
            ..
        }) => {
            assert_eq!("t", name.to_string());
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_virtual_table() {
    let sql = "CREATE VIRTUAL TABLE IF NOT EXISTS t USING module_name (arg1, arg2)";
    match sqlite_and_generic().verified_stmt(sql) {
        Statement::CreateVirtualTable {
            name,
            if_not_exists: true,
            module_name,
            module_args,
        } => {
            let args = vec![Ident::new("arg1"), Ident::new("arg2")];
            assert_eq!("t", name.to_string());
            assert_eq!("module_name", module_name.to_string());
            assert_eq!(args, module_args);
        }
        _ => unreachable!(),
    }

    let sql = "CREATE VIRTUAL TABLE t USING module_name";
    sqlite_and_generic().verified_stmt(sql);
}

#[test]
fn parse_create_view_temporary_if_not_exists() {
    let sql = "CREATE TEMPORARY VIEW IF NOT EXISTS myschema.myview AS SELECT foo FROM bar";
    match sqlite_and_generic().verified_stmt(sql) {
        Statement::CreateView {
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
        } => {
            assert_eq!("myschema.myview", name.to_string());
            assert_eq!(Vec::<ViewColumnDef>::new(), columns);
            assert_eq!("SELECT foo FROM bar", query.to_string());
            assert!(!materialized);
            assert!(!or_replace);
            assert_eq!(options, CreateTableOptions::None);
            assert_eq!(cluster_by, vec![]);
            assert!(comment.is_none());
            assert!(!late_binding);
            assert!(if_not_exists);
            assert!(temporary);
        }
        _ => unreachable!(),
    }
}

#[test]
fn double_equality_operator() {
    // Sqlite supports this operator: https://www.sqlite.org/lang_expr.html#binaryops
    let input = "SELECT a==b FROM t";
    let expected = "SELECT a = b FROM t";
    let _ = sqlite_and_generic().one_statement_parses_to(input, expected);
}

#[test]
fn parse_create_table_auto_increment() {
    let sql = "CREATE TABLE foo (bar INT PRIMARY KEY AUTOINCREMENT)";
    match sqlite_and_generic().verified_stmt(sql) {
        Statement::CreateTable(CreateTable { name, columns, .. }) => {
            assert_eq!(name.to_string(), "foo");
            assert_eq!(
                vec![ColumnDef {
                    name: "bar".into(),
                    data_type: DataType::Int(None),
                    options: vec![
                        ColumnOptionDef {
                            name: None,
                            option: ColumnOption::PrimaryKey(PrimaryKeyConstraint {
                                name: None,
                                index_name: None,
                                index_type: None,
                                columns: vec!["bar".into()],
                                index_options: vec![],
                                characteristics: None,
                            }),
                        },
                        ColumnOptionDef {
                            name: None,
                            option: ColumnOption::DialectSpecific(vec![Token::make_keyword(
                                "AUTOINCREMENT"
                            )]),
                        },
                    ],
                }],
                columns
            );
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_table_primary_key_asc_desc() {
    let expected_column_def = |kind| ColumnDef {
        name: "bar".into(),
        data_type: DataType::Int(None),
        options: vec![
            ColumnOptionDef {
                name: None,
                option: ColumnOption::PrimaryKey(PrimaryKeyConstraint {
                    name: None,
                    index_name: None,
                    index_type: None,
                    columns: vec!["bar".into()],
                    index_options: vec![],
                    characteristics: None,
                }),
            },
            ColumnOptionDef {
                name: None,
                option: ColumnOption::DialectSpecific(vec![Token::make_keyword(kind)]),
            },
        ],
    };

    let sql = "CREATE TABLE foo (bar INT PRIMARY KEY ASC)";
    match sqlite_and_generic().verified_stmt(sql) {
        Statement::CreateTable(CreateTable { columns, .. }) => {
            assert_eq!(vec![expected_column_def("ASC")], columns);
        }
        _ => unreachable!(),
    }
    let sql = "CREATE TABLE foo (bar INT PRIMARY KEY DESC)";
    match sqlite_and_generic().verified_stmt(sql) {
        Statement::CreateTable(CreateTable { columns, .. }) => {
            assert_eq!(vec![expected_column_def("DESC")], columns);
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_sqlite_quote() {
    let sql = "CREATE TABLE `PRIMARY` (\"KEY\" INT, [INDEX] INT)";
    match sqlite().verified_stmt(sql) {
        Statement::CreateTable(CreateTable { name, columns, .. }) => {
            assert_eq!(name.to_string(), "`PRIMARY`");
            assert_eq!(
                vec![
                    ColumnDef {
                        name: Ident::with_quote('"', "KEY"),
                        data_type: DataType::Int(None),
                        options: vec![],
                    },
                    ColumnDef {
                        name: Ident::with_quote('[', "INDEX"),
                        data_type: DataType::Int(None),
                        options: vec![],
                    },
                ],
                columns
            );
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_table_gencol() {
    let sql_default = "CREATE TABLE t1 (a INT, b INT GENERATED ALWAYS AS (a * 2))";
    sqlite_and_generic().verified_stmt(sql_default);

    let sql_virt = "CREATE TABLE t1 (a INT, b INT GENERATED ALWAYS AS (a * 2) VIRTUAL)";
    sqlite_and_generic().verified_stmt(sql_virt);

    let sql_stored = "CREATE TABLE t1 (a INT, b INT GENERATED ALWAYS AS (a * 2) STORED)";
    sqlite_and_generic().verified_stmt(sql_stored);

    sqlite_and_generic().verified_stmt("CREATE TABLE t1 (a INT, b INT AS (a * 2))");
    sqlite_and_generic().verified_stmt("CREATE TABLE t1 (a INT, b INT AS (a * 2) VIRTUAL)");
    sqlite_and_generic().verified_stmt("CREATE TABLE t1 (a INT, b INT AS (a * 2) STORED)");
}

#[test]
fn parse_create_table_on_conflict_col() {
    for keyword in [
        Keyword::ROLLBACK,
        Keyword::ABORT,
        Keyword::FAIL,
        Keyword::IGNORE,
        Keyword::REPLACE,
    ] {
        let sql = format!("CREATE TABLE t1 (a INT, b INT ON CONFLICT {keyword:?})");
        match sqlite_and_generic().verified_stmt(&sql) {
            Statement::CreateTable(CreateTable { columns, .. }) => {
                assert_eq!(
                    vec![ColumnOptionDef {
                        name: None,
                        option: ColumnOption::OnConflict(keyword),
                    }],
                    columns[1].options
                );
            }
            _ => unreachable!(),
        }
    }
}

#[test]
fn test_parse_create_table_on_conflict_col_err() {
    let sql_err = "CREATE TABLE t1 (a INT, b INT ON CONFLICT BOH)";
    let err = sqlite_and_generic()
        .parse_sql_statements(sql_err)
        .unwrap_err();
    assert_eq!(
        err,
        ParserError::ParserError(
            "Expected: one of ROLLBACK or ABORT or FAIL or IGNORE or REPLACE, found: BOH"
                .to_string()
        )
    );
}

#[test]
fn parse_create_table_untyped() {
    sqlite().verified_stmt("CREATE TABLE t1 (a, b AS (a * 2), c NOT NULL)");
}

#[test]
fn test_placeholder() {
    // In postgres, this would be the absolute value operator '@' applied to the column 'xxx'
    // But in sqlite, this is a named parameter.
    // see https://www.sqlite.org/lang_expr.html#varparam
    let sql = "SELECT @xxx";
    let ast = sqlite().verified_only_select(sql);
    assert_eq!(
        ast.projection[0],
        UnnamedExpr(Expr::Value(
            (Value::Placeholder("@xxx".into())).with_empty_span()
        )),
    );
}

#[test]
fn parse_create_table_with_strict() {
    let sql = "CREATE TABLE Fruits (id TEXT NOT NULL PRIMARY KEY) STRICT";
    if let Statement::CreateTable(CreateTable { name, strict, .. }) = sqlite().verified_stmt(sql) {
        assert_eq!(name.to_string(), "Fruits");
        assert!(strict);
    }
}

#[test]
fn parse_single_quoted_identified() {
    sqlite().verified_only_select("SELECT 't'.*, t.'x' FROM 't'");
    // TODO: add support for select 't'.x
}

#[test]
fn parse_substring() {
    // SQLite supports the SUBSTRING function since v3.34, but does not support the SQL standard
    // SUBSTRING(expr FROM start FOR length) syntax.
    // https://www.sqlite.org/lang_corefunc.html#substr
    sqlite().verified_only_select("SELECT SUBSTRING('SQLITE', 3, 4)");
    sqlite().verified_only_select("SELECT SUBSTR('SQLITE', 3, 4)");
    sqlite().verified_only_select("SELECT SUBSTRING('SQLITE', 3)");
    sqlite().verified_only_select("SELECT SUBSTR('SQLITE', 3)");
}

#[test]
fn parse_window_function_with_filter() {
    for func_name in [
        "row_number",
        "rank",
        "max",
        "count",
        "user_defined_function",
    ] {
        let sql = format!("SELECT {func_name}(x) FILTER (WHERE y) OVER () FROM t");
        let select = sqlite().verified_only_select(&sql);
        assert_eq!(select.to_string(), sql);
        assert_eq!(
            select.projection,
            vec![SelectItem::UnnamedExpr(Expr::Function(Function {
                name: ObjectName::from(vec![Ident::new(func_name)]),
                uses_odbc_syntax: false,
                parameters: FunctionArguments::None,
                args: FunctionArguments::List(FunctionArgumentList {
                    duplicate_treatment: None,
                    args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(
                        Expr::Identifier(Ident::new("x"))
                    ))],
                    clauses: vec![],
                }),
                null_treatment: None,
                over: Some(WindowType::WindowSpec(WindowSpec {
                    window_name: None,
                    partition_by: vec![],
                    order_by: vec![],
                    window_frame: None,
                })),
                filter: Some(Box::new(Expr::Identifier(Ident::new("y")))),
                within_group: vec![],
            }))]
        );
    }
}

#[test]
fn parse_attach_database() {
    let sql = "ATTACH DATABASE 'test.db' AS test";
    let verified_stmt = sqlite().verified_stmt(sql);
    assert_eq!(sql, format!("{verified_stmt}"));
    match verified_stmt {
        Statement::AttachDatabase {
            schema_name,
            database_file_name:
                Expr::Value(ValueWithSpan {
                    value: Value::SingleQuotedString(literal_name),
                    span: _,
                }),
            database: true,
        } => {
            assert_eq!(schema_name.value, "test");
            assert_eq!(literal_name, "test.db");
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_update_tuple_row_values() {
    // See https://github.com/sqlparser-rs/sqlparser-rs/issues/1311
    assert_eq!(
        sqlite().verified_stmt("UPDATE x SET (a, b) = (1, 2)"),
        Statement::Update {
            or: None,
            assignments: vec![Assignment {
                target: AssignmentTarget::Tuple(vec![
                    ObjectName::from(vec![Ident::new("a"),]),
                    ObjectName::from(vec![Ident::new("b"),]),
                ]),
                value: Expr::Tuple(vec![
                    Expr::Value((Value::Number("1".parse().unwrap(), false)).with_empty_span()),
                    Expr::Value((Value::Number("2".parse().unwrap(), false)).with_empty_span())
                ])
            }],
            selection: None,
            table: TableWithJoins {
                relation: table_from_name(ObjectName::from(vec![Ident::new("x")])),
                joins: vec![],
            },
            from: None,
            returning: None,
            limit: None
        }
    );
}

#[test]
fn parse_where_in_empty_list() {
    let sql = "SELECT * FROM t1 WHERE a IN ()";
    let select = sqlite().verified_only_select(sql);
    if let Expr::InList { list, .. } = select.selection.as_ref().unwrap() {
        assert_eq!(list.len(), 0);
    } else {
        unreachable!()
    }

    sqlite_with_options(ParserOptions::new().with_trailing_commas(true)).one_statement_parses_to(
        "SELECT * FROM t1 WHERE a IN (,)",
        "SELECT * FROM t1 WHERE a IN ()",
    );
}

#[test]
fn invalid_empty_list() {
    let sql = "SELECT * FROM t1 WHERE a IN (,,)";
    let sqlite = sqlite_with_options(ParserOptions::new().with_trailing_commas(true));
    assert_eq!(
        "sql parser error: Expected: an expression, found: ,",
        sqlite.parse_sql_statements(sql).unwrap_err().to_string()
    );
}

#[test]
fn parse_start_transaction_with_modifier() {
    sqlite_and_generic().verified_stmt("BEGIN DEFERRED TRANSACTION");
    sqlite_and_generic().verified_stmt("BEGIN IMMEDIATE TRANSACTION");
    sqlite_and_generic().verified_stmt("BEGIN EXCLUSIVE TRANSACTION");
    sqlite_and_generic().verified_stmt("BEGIN DEFERRED");
    sqlite_and_generic().verified_stmt("BEGIN IMMEDIATE");
    sqlite_and_generic().verified_stmt("BEGIN EXCLUSIVE");
}

#[test]
fn test_dollar_identifier_as_placeholder() {
    // This relates to the discussion in issue #291. The `$id` should be treated as a placeholder,
    // not as an identifier in SQLite dialect.
    //
    // Reference: https://www.sqlite.org/lang_expr.html#varparam
    match sqlite().verified_expr("id = $id") {
        Expr::BinaryOp { op, left, right } => {
            assert_eq!(op, BinaryOperator::Eq);
            assert_eq!(left, Box::new(Expr::Identifier(Ident::new("id"))));
            assert_eq!(
                right,
                Box::new(Expr::Value(
                    (Placeholder("$id".to_string())).with_empty_span()
                ))
            );
        }
        _ => unreachable!(),
    }

    // $$ is a valid placeholder in SQLite
    match sqlite().verified_expr("id = $$") {
        Expr::BinaryOp { op, left, right } => {
            assert_eq!(op, BinaryOperator::Eq);
            assert_eq!(left, Box::new(Expr::Identifier(Ident::new("id"))));
            assert_eq!(
                right,
                Box::new(Expr::Value(
                    (Placeholder("$$".to_string())).with_empty_span()
                ))
            );
        }
        _ => unreachable!(),
    }
}

#[test]
fn test_match_operator() {
    assert_eq!(
        sqlite().verified_expr("col MATCH 'pattern'"),
        Expr::BinaryOp {
            op: BinaryOperator::Match,
            left: Box::new(Expr::Identifier(Ident::new("col"))),
            right: Box::new(Expr::Value(
                (Value::SingleQuotedString("pattern".to_string())).with_empty_span()
            ))
        }
    );
    sqlite().verified_only_select("SELECT * FROM email WHERE email MATCH 'fts5'");
}

#[test]
fn test_regexp_operator() {
    assert_eq!(
        sqlite().verified_expr("col REGEXP 'pattern'"),
        Expr::BinaryOp {
            op: BinaryOperator::Regexp,
            left: Box::new(Expr::Identifier(Ident::new("col"))),
            right: Box::new(Expr::Value(
                (Value::SingleQuotedString("pattern".to_string())).with_empty_span()
            ))
        }
    );
    sqlite().verified_only_select(r#"SELECT count(*) FROM messages WHERE msg_text REGEXP '\d+'"#);
}

#[test]
fn test_update_delete_limit() {
    match sqlite().verified_stmt("UPDATE foo SET bar = 1 LIMIT 99") {
        Statement::Update { limit, .. } => {
            assert_eq!(limit, Some(Expr::value(number("99"))));
        }
        _ => unreachable!(),
    }

    match sqlite().verified_stmt("DELETE FROM foo LIMIT 99") {
        Statement::Delete(Delete { limit, .. }) => {
            assert_eq!(limit, Some(Expr::value(number("99"))));
        }
        _ => unreachable!(),
    }
}

fn sqlite() -> TestedDialects {
    TestedDialects::new(vec![Box::new(SQLiteDialect {})])
}

fn sqlite_with_options(options: ParserOptions) -> TestedDialects {
    TestedDialects::new_with_options(vec![Box::new(SQLiteDialect {})], options)
}

fn sqlite_and_generic() -> TestedDialects {
    TestedDialects::new(vec![
        Box::new(SQLiteDialect {}),
        Box::new(GenericDialect {}),
    ])
}

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
//! Test SQL syntax, which all sqlparser dialects must parse in the same way.
//!
//! Note that it does not mean all SQL here is valid in all the dialects, only
//! that 1) it's either standard or widely supported and 2) it can be parsed by
//! sqlparser regardless of the chosen dialect (i.e. it doesn't conflict with
//! dialect-specific parsing rules).

extern crate core;

use helpers::attached_token::AttachedToken;
use matches::assert_matches;
use sqlparser::ast::helpers::key_value_options::*;
use sqlparser::ast::helpers::key_value_options::{KeyValueOptions, KeyValueOptionsDelimiter};
use sqlparser::ast::SelectItem::UnnamedExpr;
use sqlparser::ast::TableFactor::{Pivot, Unpivot};
use sqlparser::ast::*;
use sqlparser::dialect::{
    AnsiDialect, BigQueryDialect, ClickHouseDialect, DatabricksDialect, Dialect, DuckDbDialect,
    GenericDialect, HiveDialect, MsSqlDialect, MySqlDialect, PostgreSqlDialect, RedshiftSqlDialect,
    SQLiteDialect, SnowflakeDialect,
};
use sqlparser::keywords::{Keyword, ALL_KEYWORDS};
use sqlparser::parser::{Parser, ParserError, ParserOptions};
use sqlparser::tokenizer::Tokenizer;
use sqlparser::tokenizer::{Location, Span};
use test_utils::{
    all_dialects, all_dialects_where, all_dialects_with_options, alter_table_op, assert_eq_vec,
    call, expr_from_projection, join, number, only, table, table_alias, table_from_name,
    TestedDialects,
};

#[macro_use]
mod test_utils;

#[cfg(test)]
use pretty_assertions::assert_eq;
use sqlparser::ast::ColumnOption::Comment;
use sqlparser::ast::DateTimeField::Seconds;
use sqlparser::ast::Expr::{Identifier, UnaryOp};
use sqlparser::ast::Value::Number;
use sqlparser::test_utils::all_dialects_except;

#[test]
fn parse_numeric_literal_underscore() {
    let dialects = all_dialects_where(|d| d.supports_numeric_literal_underscores());

    let canonical = if cfg!(feature = "bigdecimal") {
        "SELECT 10000"
    } else {
        "SELECT 10_000"
    };

    let select = dialects.verified_only_select_with_canonical("SELECT 10_000", canonical);

    assert_eq!(
        select.projection,
        vec![UnnamedExpr(Expr::Value(
            (number("10_000")).with_empty_span()
        ))]
    );
}

#[test]
fn parse_function_object_name() {
    let select = verified_only_select("SELECT a.b.c.d(1, 2, 3) FROM T");
    let Expr::Function(func) = expr_from_projection(&select.projection[0]) else {
        unreachable!()
    };
    assert_eq!(
        ObjectName::from(
            ["a", "b", "c", "d"]
                .into_iter()
                .map(Ident::new)
                .collect::<Vec<_>>()
        ),
        func.name,
    );
}

#[test]
fn parse_insert_values() {
    let row = vec![
        Expr::value(number("1")),
        Expr::value(number("2")),
        Expr::value(number("3")),
    ];
    let rows1 = vec![row.clone()];
    let rows2 = vec![row.clone(), row];

    let sql = "INSERT customer VALUES (1, 2, 3)";
    check_one(sql, "customer", &[], &rows1);

    let sql = "INSERT INTO customer VALUES (1, 2, 3)";
    check_one(sql, "customer", &[], &rows1);

    let sql = "INSERT INTO customer VALUES (1, 2, 3), (1, 2, 3)";
    check_one(sql, "customer", &[], &rows2);

    let sql = "INSERT INTO public.customer VALUES (1, 2, 3)";
    check_one(sql, "public.customer", &[], &rows1);

    let sql = "INSERT INTO db.public.customer VALUES (1, 2, 3)";
    check_one(sql, "db.public.customer", &[], &rows1);

    let sql = "INSERT INTO public.customer (id, name, active) VALUES (1, 2, 3)";
    check_one(
        sql,
        "public.customer",
        &["id".to_string(), "name".to_string(), "active".to_string()],
        &rows1,
    );

    fn check_one(
        sql: &str,
        expected_table_name: &str,
        expected_columns: &[String],
        expected_rows: &[Vec<Expr>],
    ) {
        match verified_stmt(sql) {
            Statement::Insert(Insert {
                table: table_name,
                columns,
                source: Some(source),
                ..
            }) => {
                assert_eq!(table_name.to_string(), expected_table_name);
                assert_eq!(columns.len(), expected_columns.len());
                for (index, column) in columns.iter().enumerate() {
                    assert_eq!(column, &Ident::new(expected_columns[index].clone()));
                }
                match *source.body {
                    SetExpr::Values(Values { rows, .. }) => {
                        assert_eq!(rows.as_slice(), expected_rows)
                    }
                    _ => unreachable!(),
                }
            }
            _ => unreachable!(),
        }
    }

    verified_stmt("INSERT INTO customer WITH foo AS (SELECT 1) SELECT * FROM foo UNION VALUES (1)");
}

#[test]
fn parse_insert_set() {
    let dialects = all_dialects_where(|d| d.supports_insert_set());
    dialects.verified_stmt("INSERT INTO tbl1 SET col1 = 1, col2 = 'abc', col3 = current_date()");
}

#[test]
fn parse_replace_into() {
    let dialect = PostgreSqlDialect {};
    let sql = "REPLACE INTO public.customer (id, name, active) VALUES (1, 2, 3)";

    assert_eq!(
        ParserError::ParserError("Unsupported statement REPLACE at Line: 1, Column: 9".to_string()),
        Parser::parse_sql(&dialect, sql,).unwrap_err(),
    )
}

#[test]
fn parse_insert_default_values() {
    let insert_with_default_values = verified_stmt("INSERT INTO test_table DEFAULT VALUES");

    match insert_with_default_values {
        Statement::Insert(Insert {
            after_columns,
            columns,
            on,
            partitioned,
            returning,
            source,
            table: table_name,
            ..
        }) => {
            assert_eq!(columns, vec![]);
            assert_eq!(after_columns, vec![]);
            assert_eq!(on, None);
            assert_eq!(partitioned, None);
            assert_eq!(returning, None);
            assert_eq!(source, None);
            assert_eq!(
                table_name,
                TableObject::TableName(ObjectName::from(vec!["test_table".into()]))
            );
        }
        _ => unreachable!(),
    }

    let insert_with_default_values_and_returning =
        verified_stmt("INSERT INTO test_table DEFAULT VALUES RETURNING test_column");

    match insert_with_default_values_and_returning {
        Statement::Insert(Insert {
            after_columns,
            columns,
            on,
            partitioned,
            returning,
            source,
            table: table_name,
            ..
        }) => {
            assert_eq!(after_columns, vec![]);
            assert_eq!(columns, vec![]);
            assert_eq!(on, None);
            assert_eq!(partitioned, None);
            assert!(returning.is_some());
            assert_eq!(source, None);
            assert_eq!(
                table_name,
                TableObject::TableName(ObjectName::from(vec!["test_table".into()]))
            );
        }
        _ => unreachable!(),
    }

    let insert_with_default_values_and_on_conflict =
        verified_stmt("INSERT INTO test_table DEFAULT VALUES ON CONFLICT DO NOTHING");

    match insert_with_default_values_and_on_conflict {
        Statement::Insert(Insert {
            after_columns,
            columns,
            on,
            partitioned,
            returning,
            source,
            table: table_name,
            ..
        }) => {
            assert_eq!(after_columns, vec![]);
            assert_eq!(columns, vec![]);
            assert!(on.is_some());
            assert_eq!(partitioned, None);
            assert_eq!(returning, None);
            assert_eq!(source, None);
            assert_eq!(
                table_name,
                TableObject::TableName(ObjectName::from(vec!["test_table".into()]))
            );
        }
        _ => unreachable!(),
    }

    let insert_with_columns_and_default_values = "INSERT INTO test_table (test_col) DEFAULT VALUES";
    assert_eq!(
        ParserError::ParserError(
            "Expected: SELECT, VALUES, or a subquery in the query body, found: DEFAULT".to_string()
        ),
        parse_sql_statements(insert_with_columns_and_default_values).unwrap_err()
    );

    let insert_with_default_values_and_hive_after_columns =
        "INSERT INTO test_table DEFAULT VALUES (some_column)";
    assert_eq!(
        ParserError::ParserError("Expected: end of statement, found: (".to_string()),
        parse_sql_statements(insert_with_default_values_and_hive_after_columns).unwrap_err()
    );

    let insert_with_default_values_and_hive_partition =
        "INSERT INTO test_table DEFAULT VALUES PARTITION (some_column)";
    assert_eq!(
        ParserError::ParserError("Expected: end of statement, found: PARTITION".to_string()),
        parse_sql_statements(insert_with_default_values_and_hive_partition).unwrap_err()
    );

    let insert_with_default_values_and_values_list = "INSERT INTO test_table DEFAULT VALUES (1)";
    assert_eq!(
        ParserError::ParserError("Expected: end of statement, found: (".to_string()),
        parse_sql_statements(insert_with_default_values_and_values_list).unwrap_err()
    );
}

#[test]
fn parse_insert_select_returning() {
    // Dialects that support `RETURNING` as a column identifier do
    // not support this syntax.
    let dialects =
        all_dialects_where(|d| !d.is_column_alias(&Keyword::RETURNING, &mut Parser::new(d)));

    dialects.verified_stmt("INSERT INTO t SELECT 1 RETURNING 2");
    let stmt = dialects.verified_stmt("INSERT INTO t SELECT x RETURNING x AS y");
    match stmt {
        Statement::Insert(Insert {
            returning: Some(ret),
            source: Some(_),
            ..
        }) => assert_eq!(ret.len(), 1),
        _ => unreachable!(),
    }
}

#[test]
fn parse_insert_select_from_returning() {
    let sql = "INSERT INTO table1 SELECT * FROM table2 RETURNING id";
    match verified_stmt(sql) {
        Statement::Insert(Insert {
            table: TableObject::TableName(table_name),
            source: Some(source),
            returning: Some(returning),
            ..
        }) => {
            assert_eq!("table1", table_name.to_string());
            assert!(matches!(*source.body, SetExpr::Select(_)));
            assert_eq!(
                returning,
                vec![SelectItem::UnnamedExpr(Expr::Identifier(Ident::new("id"))),]
            );
        }
        bad_stmt => unreachable!("Expected valid insert, got {:?}", bad_stmt),
    }
}

#[test]
fn parse_returning_as_column_alias() {
    verified_stmt("SELECT 1 AS RETURNING");
}

#[test]
fn parse_insert_sqlite() {
    let dialect = SQLiteDialect {};

    let check = |sql: &str, expected_action: Option<SqliteOnConflict>| match Parser::parse_sql(
        &dialect, sql,
    )
    .unwrap()
    .pop()
    .unwrap()
    {
        Statement::Insert(Insert { or, .. }) => assert_eq!(or, expected_action),
        _ => panic!("{}", sql),
    };

    let sql = "INSERT INTO test_table(id) VALUES(1)";
    check(sql, None);

    let sql = "REPLACE INTO test_table(id) VALUES(1)";
    check(sql, Some(SqliteOnConflict::Replace));

    let sql = "INSERT OR REPLACE INTO test_table(id) VALUES(1)";
    check(sql, Some(SqliteOnConflict::Replace));

    let sql = "INSERT OR ROLLBACK INTO test_table(id) VALUES(1)";
    check(sql, Some(SqliteOnConflict::Rollback));

    let sql = "INSERT OR ABORT INTO test_table(id) VALUES(1)";
    check(sql, Some(SqliteOnConflict::Abort));

    let sql = "INSERT OR FAIL INTO test_table(id) VALUES(1)";
    check(sql, Some(SqliteOnConflict::Fail));

    let sql = "INSERT OR IGNORE INTO test_table(id) VALUES(1)";
    check(sql, Some(SqliteOnConflict::Ignore));
}

#[test]
fn parse_update() {
    let sql = "UPDATE t SET a = 1, b = 2, c = 3 WHERE d";
    match verified_stmt(sql) {
        Statement::Update(Update {
            table,
            assignments,
            selection,
            ..
        }) => {
            assert_eq!(table.to_string(), "t".to_string());
            assert_eq!(
                assignments,
                vec![
                    Assignment {
                        target: AssignmentTarget::ColumnName(ObjectName::from(vec!["a".into()])),
                        value: Expr::value(number("1")),
                    },
                    Assignment {
                        target: AssignmentTarget::ColumnName(ObjectName::from(vec!["b".into()])),
                        value: Expr::value(number("2")),
                    },
                    Assignment {
                        target: AssignmentTarget::ColumnName(ObjectName::from(vec!["c".into()])),
                        value: Expr::value(number("3")),
                    },
                ]
            );
            assert_eq!(selection.unwrap(), Expr::Identifier("d".into()));
        }
        _ => unreachable!(),
    }

    verified_stmt("UPDATE t SET a = 1, a = 2, a = 3");

    let sql = "UPDATE t WHERE 1";
    let res = parse_sql_statements(sql);
    assert_eq!(
        ParserError::ParserError("Expected: SET, found: WHERE".to_string()),
        res.unwrap_err()
    );

    let sql = "UPDATE t SET a = 1 extrabadstuff";
    let res = parse_sql_statements(sql);
    assert_eq!(
        ParserError::ParserError("Expected: end of statement, found: extrabadstuff".to_string()),
        res.unwrap_err()
    );
}

#[test]
fn parse_update_set_from() {
    let sql = "UPDATE t1 SET name = t2.name FROM (SELECT name, id FROM t1 GROUP BY id) AS t2 WHERE t1.id = t2.id";
    let dialects = TestedDialects::new(vec![
        Box::new(GenericDialect {}),
        Box::new(DuckDbDialect {}),
        Box::new(PostgreSqlDialect {}),
        Box::new(BigQueryDialect {}),
        Box::new(SnowflakeDialect {}),
        Box::new(RedshiftSqlDialect {}),
        Box::new(MsSqlDialect {}),
        Box::new(SQLiteDialect {}),
    ]);
    let stmt = dialects.verified_stmt(sql);
    assert_eq!(
        stmt,
        Statement::Update(Update {
            table: TableWithJoins {
                relation: table_from_name(ObjectName::from(vec![Ident::new("t1")])),
                joins: vec![],
            },
            assignments: vec![Assignment {
                target: AssignmentTarget::ColumnName(ObjectName::from(vec![Ident::new("name")])),
                value: Expr::CompoundIdentifier(vec![Ident::new("t2"), Ident::new("name")])
            }],
            from: Some(UpdateTableFromKind::AfterSet(vec![TableWithJoins {
                relation: TableFactor::Derived {
                    lateral: false,
                    subquery: Box::new(Query {
                        with: None,
                        body: Box::new(SetExpr::Select(Box::new(Select {
                            select_token: AttachedToken::empty(),
                            distinct: None,
                            top: None,
                            top_before_distinct: false,
                            projection: vec![
                                SelectItem::UnnamedExpr(Expr::Identifier(Ident::new("name"))),
                                SelectItem::UnnamedExpr(Expr::Identifier(Ident::new("id"))),
                            ],
                            exclude: None,
                            into: None,
                            from: vec![TableWithJoins {
                                relation: table_from_name(ObjectName::from(vec![Ident::new("t1")])),
                                joins: vec![],
                            }],
                            lateral_views: vec![],
                            prewhere: None,
                            selection: None,
                            group_by: GroupByExpr::Expressions(
                                vec![Expr::Identifier(Ident::new("id"))],
                                vec![]
                            ),
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
                        order_by: None,
                        limit_clause: None,
                        fetch: None,
                        locks: vec![],
                        for_clause: None,
                        settings: None,
                        format_clause: None,
                        pipe_operators: vec![],
                    }),
                    alias: Some(TableAlias {
                        name: Ident::new("t2"),
                        columns: vec![],
                    })
                },
                joins: vec![]
            }])),
            selection: Some(Expr::BinaryOp {
                left: Box::new(Expr::CompoundIdentifier(vec![
                    Ident::new("t1"),
                    Ident::new("id")
                ])),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::CompoundIdentifier(vec![
                    Ident::new("t2"),
                    Ident::new("id")
                ])),
            }),
            returning: None,
            or: None,
            limit: None
        })
    );

    let sql = "UPDATE T SET a = b FROM U, (SELECT foo FROM V) AS W WHERE 1 = 1";
    dialects.verified_stmt(sql);
}

#[test]
fn parse_update_with_table_alias() {
    let sql = "UPDATE users AS u SET u.username = 'new_user' WHERE u.username = 'old_user'";
    match verified_stmt(sql) {
        Statement::Update(Update {
            table,
            assignments,
            from: _from,
            selection,
            returning,
            or: None,
            limit: None,
        }) => {
            assert_eq!(
                TableWithJoins {
                    relation: TableFactor::Table {
                        name: ObjectName::from(vec![Ident::new("users")]),
                        alias: Some(TableAlias {
                            name: Ident::new("u"),
                            columns: vec![],
                        }),
                        args: None,
                        with_hints: vec![],
                        version: None,
                        partitions: vec![],
                        with_ordinality: false,
                        json_path: None,
                        sample: None,
                        index_hints: vec![],
                    },
                    joins: vec![],
                },
                table
            );
            assert_eq!(
                vec![Assignment {
                    target: AssignmentTarget::ColumnName(ObjectName::from(vec![
                        Ident::new("u"),
                        Ident::new("username")
                    ])),
                    value: Expr::Value(
                        (Value::SingleQuotedString("new_user".to_string())).with_empty_span()
                    ),
                }],
                assignments
            );
            assert_eq!(
                Some(Expr::BinaryOp {
                    left: Box::new(Expr::CompoundIdentifier(vec![
                        Ident::new("u"),
                        Ident::new("username"),
                    ])),
                    op: BinaryOperator::Eq,
                    right: Box::new(Expr::Value(
                        (Value::SingleQuotedString("old_user".to_string())).with_empty_span()
                    )),
                }),
                selection
            );
            assert_eq!(None, returning);
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_update_or() {
    let expect_or_clause = |sql: &str, expected_action: SqliteOnConflict| match verified_stmt(sql) {
        Statement::Update(Update { or, .. }) => assert_eq!(or, Some(expected_action)),
        other => unreachable!("Expected update with or, got {:?}", other),
    };
    expect_or_clause(
        "UPDATE OR REPLACE t SET n = n + 1",
        SqliteOnConflict::Replace,
    );
    expect_or_clause(
        "UPDATE OR ROLLBACK t SET n = n + 1",
        SqliteOnConflict::Rollback,
    );
    expect_or_clause("UPDATE OR ABORT t SET n = n + 1", SqliteOnConflict::Abort);
    expect_or_clause("UPDATE OR FAIL t SET n = n + 1", SqliteOnConflict::Fail);
    expect_or_clause("UPDATE OR IGNORE t SET n = n + 1", SqliteOnConflict::Ignore);
}

#[test]
fn parse_select_with_table_alias_as() {
    // AS is optional
    one_statement_parses_to(
        "SELECT a, b, c FROM lineitem l (A, B, C)",
        "SELECT a, b, c FROM lineitem AS l (A, B, C)",
    );
}

#[test]
fn parse_select_with_table_alias() {
    let select = verified_only_select("SELECT a, b, c FROM lineitem AS l (A, B, C)");
    assert_eq!(
        select.projection,
        vec![
            SelectItem::UnnamedExpr(Expr::Identifier(Ident::new("a")),),
            SelectItem::UnnamedExpr(Expr::Identifier(Ident::new("b")),),
            SelectItem::UnnamedExpr(Expr::Identifier(Ident::new("c")),),
        ]
    );
    assert_eq!(
        select.from,
        vec![TableWithJoins {
            relation: TableFactor::Table {
                name: ObjectName::from(vec![Ident::new("lineitem")]),
                alias: Some(TableAlias {
                    name: Ident::new("l"),
                    columns: vec![
                        TableAliasColumnDef::from_name("A"),
                        TableAliasColumnDef::from_name("B"),
                        TableAliasColumnDef::from_name("C"),
                    ],
                }),
                args: None,
                with_hints: vec![],
                version: None,
                partitions: vec![],
                with_ordinality: false,
                json_path: None,
                sample: None,
                index_hints: vec![],
            },
            joins: vec![],
        }]
    );
}

#[test]
fn parse_analyze() {
    verified_stmt("ANALYZE TABLE test_table");
    verified_stmt("ANALYZE test_table");
}

#[test]
fn parse_invalid_table_name() {
    let ast = all_dialects().run_parser_method("db.public..customer", |parser| {
        parser.parse_object_name(false)
    });
    assert!(ast.is_err());
}

#[test]
fn parse_no_table_name() {
    let ast = all_dialects().run_parser_method("", |parser| parser.parse_object_name(false));
    assert!(ast.is_err());
}

#[test]
fn parse_delete_statement() {
    let sql = "DELETE FROM \"table\"";
    match verified_stmt(sql) {
        Statement::Delete(Delete {
            from: FromTable::WithFromKeyword(from),
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
fn parse_delete_without_from_error() {
    let sql = "DELETE \"table\" WHERE 1";

    let dialects = all_dialects_except(|d| d.is::<BigQueryDialect>() || d.is::<GenericDialect>());
    let res = dialects.parse_sql_statements(sql);
    assert_eq!(
        ParserError::ParserError("Expected: FROM, found: WHERE".to_string()),
        res.unwrap_err()
    );
}

#[test]
fn parse_delete_statement_for_multi_tables() {
    let sql = "DELETE schema1.table1, schema2.table2 FROM schema1.table1 JOIN schema2.table2 ON schema2.table2.col1 = schema1.table1.col1 WHERE schema2.table2.col2 = 1";
    let dialects = all_dialects_except(|d| d.is::<BigQueryDialect>() || d.is::<GenericDialect>());
    match dialects.verified_stmt(sql) {
        Statement::Delete(Delete {
            tables,
            from: FromTable::WithFromKeyword(from),
            ..
        }) => {
            assert_eq!(
                ObjectName::from(vec![Ident::new("schema1"), Ident::new("table1")]),
                tables[0]
            );
            assert_eq!(
                ObjectName::from(vec![Ident::new("schema2"), Ident::new("table2")]),
                tables[1]
            );
            assert_eq!(
                table_from_name(ObjectName::from(vec![
                    Ident::new("schema1"),
                    Ident::new("table1")
                ])),
                from[0].relation
            );
            assert_eq!(
                table_from_name(ObjectName::from(vec![
                    Ident::new("schema2"),
                    Ident::new("table2")
                ])),
                from[0].joins[0].relation
            );
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_delete_statement_for_multi_tables_with_using() {
    let sql = "DELETE FROM schema1.table1, schema2.table2 USING schema1.table1 JOIN schema2.table2 ON schema2.table2.pk = schema1.table1.col1 WHERE schema2.table2.col2 = 1";
    match verified_stmt(sql) {
        Statement::Delete(Delete {
            from: FromTable::WithFromKeyword(from),
            using: Some(using),
            ..
        }) => {
            assert_eq!(
                table_from_name(ObjectName::from(vec![
                    Ident::new("schema1"),
                    Ident::new("table1")
                ])),
                from[0].relation
            );
            assert_eq!(
                table_from_name(ObjectName::from(vec![
                    Ident::new("schema2"),
                    Ident::new("table2")
                ])),
                from[1].relation
            );
            assert_eq!(
                table_from_name(ObjectName::from(vec![
                    Ident::new("schema1"),
                    Ident::new("table1")
                ])),
                using[0].relation
            );
            assert_eq!(
                table_from_name(ObjectName::from(vec![
                    Ident::new("schema2"),
                    Ident::new("table2")
                ])),
                using[0].joins[0].relation
            );
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_where_delete_statement() {
    use self::BinaryOperator::*;

    let sql = "DELETE FROM foo WHERE name = 5";
    match verified_stmt(sql) {
        Statement::Delete(Delete {
            tables: _,
            from: FromTable::WithFromKeyword(from),
            using,
            selection,
            returning,
            ..
        }) => {
            assert_eq!(
                table_from_name(ObjectName::from(vec![Ident::new("foo")])),
                from[0].relation,
            );

            assert_eq!(None, using);
            assert_eq!(
                Expr::BinaryOp {
                    left: Box::new(Expr::Identifier(Ident::new("name"))),
                    op: Eq,
                    right: Box::new(Expr::value(number("5"))),
                },
                selection.unwrap(),
            );
            assert_eq!(None, returning);
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_where_delete_with_alias_statement() {
    use self::BinaryOperator::*;

    let sql = "DELETE FROM basket AS a USING basket AS b WHERE a.id < b.id";
    match verified_stmt(sql) {
        Statement::Delete(Delete {
            tables: _,
            from: FromTable::WithFromKeyword(from),
            using,
            selection,
            returning,
            ..
        }) => {
            assert_eq!(
                TableFactor::Table {
                    name: ObjectName::from(vec![Ident::new("basket")]),
                    alias: Some(TableAlias {
                        name: Ident::new("a"),
                        columns: vec![],
                    }),
                    args: None,
                    with_hints: vec![],
                    version: None,
                    partitions: vec![],
                    with_ordinality: false,
                    json_path: None,
                    sample: None,
                    index_hints: vec![],
                },
                from[0].relation,
            );
            assert_eq!(
                Some(vec![TableWithJoins {
                    relation: TableFactor::Table {
                        name: ObjectName::from(vec![Ident::new("basket")]),
                        alias: Some(TableAlias {
                            name: Ident::new("b"),
                            columns: vec![],
                        }),
                        args: None,
                        with_hints: vec![],
                        version: None,
                        partitions: vec![],
                        with_ordinality: false,
                        json_path: None,
                        sample: None,
                        index_hints: vec![],
                    },
                    joins: vec![],
                }]),
                using
            );
            assert_eq!(
                Expr::BinaryOp {
                    left: Box::new(Expr::CompoundIdentifier(vec![
                        Ident::new("a"),
                        Ident::new("id"),
                    ])),
                    op: Lt,
                    right: Box::new(Expr::CompoundIdentifier(vec![
                        Ident::new("b"),
                        Ident::new("id"),
                    ])),
                },
                selection.unwrap(),
            );
            assert_eq!(None, returning);
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_top_level() {
    verified_stmt("SELECT 1");
    verified_stmt("(SELECT 1)");
    verified_stmt("((SELECT 1))");
    verified_stmt("VALUES (1)");
    verified_stmt("VALUES ROW(1, NULL, 'a'), ROW(2, NULL, 'b')");
}

#[test]
fn parse_simple_select() {
    let sql = "SELECT id, fname, lname FROM customer WHERE id = 1 LIMIT 5";
    let select = verified_only_select(sql);
    assert!(select.distinct.is_none());
    assert_eq!(3, select.projection.len());
    let select = verified_query(sql);
    let expected_limit_clause = LimitClause::LimitOffset {
        limit: Some(Expr::value(number("5"))),
        offset: None,
        limit_by: vec![],
    };
    assert_eq!(Some(expected_limit_clause), select.limit_clause);
}

#[test]
fn parse_limit() {
    verified_stmt("SELECT * FROM user LIMIT 1");
}

#[test]
fn parse_invalid_limit_by() {
    all_dialects()
        .parse_sql_statements("SELECT * FROM user BY name")
        .expect_err("BY without LIMIT");
}

#[test]
fn parse_limit_is_not_an_alias() {
    // In dialects supporting LIMIT it shouldn't be parsed as a table alias
    let ast = verified_query("SELECT id FROM customer LIMIT 1");
    let expected_limit_clause = LimitClause::LimitOffset {
        limit: Some(Expr::value(number("1"))),
        offset: None,
        limit_by: vec![],
    };
    assert_eq!(Some(expected_limit_clause), ast.limit_clause);

    let ast = verified_query("SELECT 1 LIMIT 5");
    let expected_limit_clause = LimitClause::LimitOffset {
        limit: Some(Expr::value(number("5"))),
        offset: None,
        limit_by: vec![],
    };
    assert_eq!(Some(expected_limit_clause), ast.limit_clause);
}

#[test]
fn parse_select_distinct() {
    let sql = "SELECT DISTINCT name FROM customer";
    let select = verified_only_select(sql);
    assert!(select.distinct.is_some());
    assert_eq!(
        &SelectItem::UnnamedExpr(Expr::Identifier(Ident::new("name"))),
        only(&select.projection)
    );
}

#[test]
fn parse_select_distinct_two_fields() {
    let sql = "SELECT DISTINCT name, id FROM customer";
    let select = verified_only_select(sql);
    assert!(select.distinct.is_some());
    assert_eq!(
        &SelectItem::UnnamedExpr(Expr::Identifier(Ident::new("name"))),
        &select.projection[0]
    );
    assert_eq!(
        &SelectItem::UnnamedExpr(Expr::Identifier(Ident::new("id"))),
        &select.projection[1]
    );
}

#[test]
fn parse_select_distinct_tuple() {
    let sql = "SELECT DISTINCT (name, id) FROM customer";
    let select = verified_only_select(sql);
    assert_eq!(
        &vec![SelectItem::UnnamedExpr(Expr::Tuple(vec![
            Expr::Identifier(Ident::new("name")),
            Expr::Identifier(Ident::new("id")),
        ]))],
        &select.projection
    );
}

#[test]
fn parse_outer_join_operator() {
    let dialects = all_dialects_where(|d| d.supports_outer_join_operator());

    let select = dialects.verified_only_select("SELECT 1 FROM T WHERE a = b (+)");
    assert_eq!(
        select.selection,
        Some(Expr::BinaryOp {
            left: Box::new(Expr::Identifier(Ident::new("a"))),
            op: BinaryOperator::Eq,
            right: Box::new(Expr::OuterJoin(Box::new(Expr::Identifier(Ident::new("b")))))
        })
    );

    let select = dialects.verified_only_select("SELECT 1 FROM T WHERE t1.c1 = t2.c2.d3 (+)");
    assert_eq!(
        select.selection,
        Some(Expr::BinaryOp {
            left: Box::new(Expr::CompoundIdentifier(vec![
                Ident::new("t1"),
                Ident::new("c1")
            ])),
            op: BinaryOperator::Eq,
            right: Box::new(Expr::OuterJoin(Box::new(Expr::CompoundIdentifier(vec![
                Ident::new("t2"),
                Ident::new("c2"),
                Ident::new("d3"),
            ]))))
        })
    );

    let res = dialects.parse_sql_statements("SELECT 1 FROM T WHERE 1 = 2 (+)");
    assert_eq!(
        ParserError::ParserError("Expected: column identifier before (+), found: 2".to_string()),
        res.unwrap_err()
    );
}

#[test]
fn parse_select_distinct_on() {
    let sql = "SELECT DISTINCT ON (album_id) name FROM track ORDER BY album_id, milliseconds";
    let select = verified_only_select(sql);
    assert_eq!(
        &Some(Distinct::On(vec![Expr::Identifier(Ident::new("album_id"))])),
        &select.distinct
    );

    let sql = "SELECT DISTINCT ON () name FROM track ORDER BY milliseconds";
    let select = verified_only_select(sql);
    assert_eq!(&Some(Distinct::On(vec![])), &select.distinct);

    let sql = "SELECT DISTINCT ON (album_id, milliseconds) name FROM track";
    let select = verified_only_select(sql);
    assert_eq!(
        &Some(Distinct::On(vec![
            Expr::Identifier(Ident::new("album_id")),
            Expr::Identifier(Ident::new("milliseconds")),
        ])),
        &select.distinct
    );
}

#[test]
fn parse_select_distinct_missing_paren() {
    let result = parse_sql_statements("SELECT DISTINCT (name, id FROM customer");
    assert_eq!(
        ParserError::ParserError("Expected: ), found: FROM".to_string()),
        result.unwrap_err(),
    );
}

#[test]
fn parse_select_all() {
    one_statement_parses_to("SELECT ALL name FROM customer", "SELECT name FROM customer");
}

#[test]
fn parse_select_all_distinct() {
    let result = parse_sql_statements("SELECT ALL DISTINCT name FROM customer");
    assert_eq!(
        ParserError::ParserError("Cannot specify both ALL and DISTINCT".to_string()),
        result.unwrap_err(),
    );
}

#[test]
fn parse_select_into() {
    let sql = "SELECT * INTO table0 FROM table1";
    one_statement_parses_to(sql, "SELECT * INTO table0 FROM table1");
    let select = verified_only_select(sql);
    assert_eq!(
        &SelectInto {
            temporary: false,
            unlogged: false,
            table: false,
            name: ObjectName::from(vec![Ident::new("table0")]),
        },
        only(&select.into)
    );

    let sql = "SELECT * INTO TEMPORARY UNLOGGED TABLE table0 FROM table1";
    one_statement_parses_to(
        sql,
        "SELECT * INTO TEMPORARY UNLOGGED TABLE table0 FROM table1",
    );

    // Do not allow aliases here
    let sql = "SELECT * INTO table0 asdf FROM table1";
    let result = parse_sql_statements(sql);
    assert_eq!(
        ParserError::ParserError("Expected: end of statement, found: asdf".to_string()),
        result.unwrap_err()
    )
}

#[test]
fn parse_select_wildcard() {
    let sql = "SELECT * FROM foo";
    let select = verified_only_select(sql);
    assert_eq!(
        &SelectItem::Wildcard(WildcardAdditionalOptions::default()),
        only(&select.projection)
    );

    let sql = "SELECT foo.* FROM foo";
    let select = verified_only_select(sql);
    assert_eq!(
        &SelectItem::QualifiedWildcard(
            SelectItemQualifiedWildcardKind::ObjectName(ObjectName::from(vec![Ident::new("foo")])),
            WildcardAdditionalOptions::default()
        ),
        only(&select.projection)
    );

    let sql = "SELECT myschema.mytable.* FROM myschema.mytable";
    let select = verified_only_select(sql);
    assert_eq!(
        &SelectItem::QualifiedWildcard(
            SelectItemQualifiedWildcardKind::ObjectName(ObjectName::from(vec![
                Ident::new("myschema"),
                Ident::new("mytable"),
            ])),
            WildcardAdditionalOptions::default(),
        ),
        only(&select.projection)
    );

    let sql = "SELECT * + * FROM foo;";
    let result = parse_sql_statements(sql);
    assert_eq!(
        ParserError::ParserError("Expected: end of statement, found: +".to_string()),
        result.unwrap_err(),
    );
}

#[test]
fn parse_count_wildcard() {
    verified_only_select("SELECT COUNT(*) FROM Order WHERE id = 10");

    verified_only_select(
        "SELECT COUNT(Employee.*) FROM Order JOIN Employee ON Order.employee = Employee.id",
    );
}

#[test]
fn parse_column_aliases() {
    let sql = "SELECT a.col + 1 AS newname FROM foo AS a";
    let select = verified_only_select(sql);
    if let SelectItem::ExprWithAlias {
        expr: Expr::BinaryOp {
            ref op, ref right, ..
        },
        ref alias,
    } = only(&select.projection)
    {
        assert_eq!(&BinaryOperator::Plus, op);
        assert_eq!(&Expr::value(number("1")), right.as_ref());
        assert_eq!(&Ident::new("newname"), alias);
    } else {
        panic!("Expected: ExprWithAlias")
    }

    // alias without AS is parsed correctly:
    one_statement_parses_to("SELECT a.col + 1 newname FROM foo AS a", sql);
}

#[test]
fn parse_select_expr_star() {
    let dialects = all_dialects_where(|d| d.supports_select_expr_star());

    // Identifier wildcard expansion.
    let select = dialects.verified_only_select("SELECT foo.bar.* FROM T");
    let SelectItem::QualifiedWildcard(SelectItemQualifiedWildcardKind::ObjectName(object_name), _) =
        only(&select.projection)
    else {
        unreachable!(
            "expected wildcard select item: got {:?}",
            &select.projection[0]
        )
    };
    assert_eq!(
        &ObjectName::from(
            ["foo", "bar"]
                .into_iter()
                .map(Ident::new)
                .collect::<Vec<_>>()
        ),
        object_name
    );

    // Arbitrary compound expression with wildcard expansion.
    let select = dialects.verified_only_select("SELECT foo - bar.* FROM T");
    let SelectItem::QualifiedWildcard(
        SelectItemQualifiedWildcardKind::Expr(Expr::BinaryOp { left, op, right }),
        _,
    ) = only(&select.projection)
    else {
        unreachable!(
            "expected wildcard select item: got {:?}",
            &select.projection[0]
        )
    };
    let (Expr::Identifier(left), BinaryOperator::Minus, Expr::Identifier(right)) =
        (left.as_ref(), op, right.as_ref())
    else {
        unreachable!("expected binary op expr: got {:?}", &select.projection[0])
    };
    assert_eq!(&Ident::new("foo"), left);
    assert_eq!(&Ident::new("bar"), right);

    // Arbitrary expression wildcard expansion.
    let select = dialects.verified_only_select("SELECT myfunc().foo.* FROM T");
    let SelectItem::QualifiedWildcard(
        SelectItemQualifiedWildcardKind::Expr(Expr::CompoundFieldAccess { root, access_chain }),
        _,
    ) = only(&select.projection)
    else {
        unreachable!("expected wildcard expr: got {:?}", &select.projection[0])
    };
    assert!(matches!(root.as_ref(), Expr::Function(_)));
    assert_eq!(1, access_chain.len());
    assert!(matches!(
        &access_chain[0],
        AccessExpr::Dot(Expr::Identifier(_))
    ));

    dialects.one_statement_parses_to(
        "SELECT 2. * 3 FROM T",
        #[cfg(feature = "bigdecimal")]
        "SELECT 2 * 3 FROM T",
        #[cfg(not(feature = "bigdecimal"))]
        "SELECT 2. * 3 FROM T",
    );
    dialects.verified_only_select("SELECT myfunc().* FROM T");

    // Invalid
    let res = dialects.parse_sql_statements("SELECT foo.*.* FROM T");
    assert_eq!(
        ParserError::ParserError("Expected: end of statement, found: .".to_string()),
        res.unwrap_err()
    );

    let dialects = all_dialects_where(|d| {
        d.supports_select_expr_star() && d.supports_select_wildcard_except()
    });
    dialects.verified_only_select("SELECT myfunc().* EXCEPT (foo) FROM T");
}

#[test]
fn test_eof_after_as() {
    let res = parse_sql_statements("SELECT foo AS");
    assert_eq!(
        ParserError::ParserError("Expected: an identifier after AS, found: EOF".to_string()),
        res.unwrap_err()
    );

    let res = parse_sql_statements("SELECT 1 FROM foo AS");
    assert_eq!(
        ParserError::ParserError("Expected: an identifier after AS, found: EOF".to_string()),
        res.unwrap_err()
    );
}

#[test]
fn test_no_infix_error() {
    let dialects = TestedDialects::new(vec![Box::new(ClickHouseDialect {})]);

    let res = dialects.parse_sql_statements("ASSERT-URA<<");
    assert_eq!(
        ParserError::ParserError("No infix parser for token ShiftLeft".to_string()),
        res.unwrap_err()
    );
}

#[test]
fn parse_select_count_wildcard() {
    let sql = "SELECT COUNT(*) FROM customer";
    let select = verified_only_select(sql);
    assert_eq!(
        &Expr::Function(Function {
            name: ObjectName::from(vec![Ident::new("COUNT")]),
            uses_odbc_syntax: false,
            parameters: FunctionArguments::None,
            args: FunctionArguments::List(FunctionArgumentList {
                duplicate_treatment: None,
                args: vec![FunctionArg::Unnamed(FunctionArgExpr::Wildcard)],
                clauses: vec![],
            }),
            null_treatment: None,
            filter: None,
            over: None,
            within_group: vec![]
        }),
        expr_from_projection(only(&select.projection))
    );
}

#[test]
fn parse_select_count_distinct() {
    let sql = "SELECT COUNT(DISTINCT +x) FROM customer";
    let select = verified_only_select(sql);
    assert_eq!(
        &Expr::Function(Function {
            name: ObjectName::from(vec![Ident::new("COUNT")]),
            uses_odbc_syntax: false,
            parameters: FunctionArguments::None,
            args: FunctionArguments::List(FunctionArgumentList {
                duplicate_treatment: Some(DuplicateTreatment::Distinct),
                args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::UnaryOp {
                    op: UnaryOperator::Plus,
                    expr: Box::new(Expr::Identifier(Ident::new("x"))),
                }))],
                clauses: vec![],
            }),
            null_treatment: None,
            within_group: vec![],
            filter: None,
            over: None
        }),
        expr_from_projection(only(&select.projection))
    );

    verified_stmt("SELECT COUNT(ALL +x) FROM customer");
    verified_stmt("SELECT COUNT(+x) FROM customer");

    let sql = "SELECT COUNT(ALL DISTINCT + x) FROM customer";
    let res = parse_sql_statements(sql);
    assert_eq!(
        ParserError::ParserError("Cannot specify both ALL and DISTINCT".to_string()),
        res.unwrap_err()
    );
}

#[test]
fn parse_not() {
    let sql = "SELECT id FROM customer WHERE NOT salary = ''";
    let _ast = verified_only_select(sql);
    //TODO: add assertions
}

#[test]
fn parse_invalid_infix_not() {
    let res = parse_sql_statements("SELECT c FROM t WHERE c NOT (");
    assert_eq!(
        ParserError::ParserError("Expected: end of statement, found: NOT".to_string()),
        res.unwrap_err(),
    );
}

#[test]
fn parse_collate() {
    let sql = "SELECT name COLLATE \"de_DE\" FROM customer";
    assert_matches!(
        only(&all_dialects().verified_only_select(sql).projection),
        SelectItem::UnnamedExpr(Expr::Collate { .. })
    );
}

#[test]
fn parse_collate_after_parens() {
    let sql = "SELECT (name) COLLATE \"de_DE\" FROM customer";
    assert_matches!(
        only(&all_dialects().verified_only_select(sql).projection),
        SelectItem::UnnamedExpr(Expr::Collate { .. })
    );
}

#[test]
fn parse_select_string_predicate() {
    let sql = "SELECT id, fname, lname FROM customer \
               WHERE salary <> 'Not Provided' AND salary <> ''";
    let _ast = verified_only_select(sql);
    //TODO: add assertions
}

#[test]
fn parse_projection_nested_type() {
    let sql = "SELECT customer.address.state FROM foo";
    let _ast = verified_only_select(sql);
    //TODO: add assertions
}

#[test]
fn parse_null_in_select() {
    let sql = "SELECT NULL";
    let select = verified_only_select(sql);
    assert_eq!(
        &Expr::Value((Value::Null).with_empty_span()),
        expr_from_projection(only(&select.projection)),
    );
}

#[test]
fn parse_exponent_in_select() -> Result<(), ParserError> {
    // all except Hive, as it allows numbers to start an identifier
    let dialects = TestedDialects::new(vec![
        Box::new(AnsiDialect {}),
        Box::new(BigQueryDialect {}),
        Box::new(ClickHouseDialect {}),
        Box::new(DuckDbDialect {}),
        Box::new(GenericDialect {}),
        // Box::new(HiveDialect {}),
        Box::new(MsSqlDialect {}),
        Box::new(MySqlDialect {}),
        Box::new(PostgreSqlDialect {}),
        Box::new(RedshiftSqlDialect {}),
        Box::new(SnowflakeDialect {}),
        Box::new(SQLiteDialect {}),
    ]);
    let sql = "SELECT 10e-20, 1e3, 1e+3, 1e3a, 1e, 0.5e2";
    let mut select = dialects.parse_sql_statements(sql)?;

    let select = match select.pop().unwrap() {
        Statement::Query(inner) => *inner,
        _ => panic!("Expected: Query"),
    };
    let select = match *select.body {
        SetExpr::Select(inner) => *inner,
        _ => panic!("Expected: SetExpr::Select"),
    };

    assert_eq!(
        &vec![
            SelectItem::UnnamedExpr(Expr::Value((number("10e-20")).with_empty_span())),
            SelectItem::UnnamedExpr(Expr::value(number("1e3"))),
            SelectItem::UnnamedExpr(Expr::Value((number("1e+3")).with_empty_span())),
            SelectItem::ExprWithAlias {
                expr: Expr::value(number("1e3")),
                alias: Ident::new("a")
            },
            SelectItem::ExprWithAlias {
                expr: Expr::value(number("1")),
                alias: Ident::new("e")
            },
            SelectItem::UnnamedExpr(Expr::value(number("0.5e2"))),
        ],
        &select.projection
    );

    Ok(())
}

#[test]
fn parse_select_with_date_column_name() {
    let sql = "SELECT date";
    let select = verified_only_select(sql);
    assert_eq!(
        &Expr::Identifier(Ident {
            value: "date".into(),
            quote_style: None,
            span: Span::empty(),
        }),
        expr_from_projection(only(&select.projection)),
    );
}

#[test]
fn parse_escaped_single_quote_string_predicate_with_escape() {
    use self::BinaryOperator::*;
    let sql = "SELECT id, fname, lname FROM customer \
               WHERE salary <> 'Jim''s salary'";

    let ast = verified_only_select(sql);

    assert_eq!(
        Some(Expr::BinaryOp {
            left: Box::new(Expr::Identifier(Ident::new("salary"))),
            op: NotEq,
            right: Box::new(Expr::Value(
                (Value::SingleQuotedString("Jim's salary".to_string())).with_empty_span()
            )),
        }),
        ast.selection,
    );
}

#[test]
fn parse_escaped_single_quote_string_predicate_with_no_escape() {
    use self::BinaryOperator::*;
    let sql = "SELECT id, fname, lname FROM customer \
               WHERE salary <> 'Jim''s salary'";

    let ast = TestedDialects::new_with_options(
        vec![Box::new(MySqlDialect {})],
        ParserOptions::new()
            .with_trailing_commas(true)
            .with_unescape(false),
    )
    .verified_only_select(sql);

    assert_eq!(
        Some(Expr::BinaryOp {
            left: Box::new(Expr::Identifier(Ident::new("salary"))),
            op: NotEq,
            right: Box::new(Expr::Value(
                (Value::SingleQuotedString("Jim''s salary".to_string())).with_empty_span()
            )),
        }),
        ast.selection,
    );
}

#[test]
fn parse_number() {
    let expr = verified_expr("1.0");

    #[cfg(feature = "bigdecimal")]
    assert_eq!(
        expr,
        Expr::Value((Value::Number(bigdecimal::BigDecimal::from(1), false)).with_empty_span())
    );

    #[cfg(not(feature = "bigdecimal"))]
    assert_eq!(
        expr,
        Expr::Value((Value::Number("1.0".into(), false)).with_empty_span())
    );
}

#[test]
fn parse_compound_expr_1() {
    use self::BinaryOperator::*;
    use self::Expr::*;
    let sql = "a + b * c";
    assert_eq!(
        BinaryOp {
            left: Box::new(Identifier(Ident::new("a"))),
            op: Plus,
            right: Box::new(BinaryOp {
                left: Box::new(Identifier(Ident::new("b"))),
                op: Multiply,
                right: Box::new(Identifier(Ident::new("c"))),
            }),
        },
        verified_expr(sql)
    );
}

#[test]
fn parse_compound_expr_2() {
    use self::BinaryOperator::*;
    use self::Expr::*;
    let sql = "a * b + c";
    assert_eq!(
        BinaryOp {
            left: Box::new(BinaryOp {
                left: Box::new(Identifier(Ident::new("a"))),
                op: Multiply,
                right: Box::new(Identifier(Ident::new("b"))),
            }),
            op: Plus,
            right: Box::new(Identifier(Ident::new("c"))),
        },
        verified_expr(sql)
    );
}

#[test]
fn parse_unary_math_with_plus() {
    use self::Expr::*;
    let sql = "-a + -b";
    assert_eq!(
        BinaryOp {
            left: Box::new(UnaryOp {
                op: UnaryOperator::Minus,
                expr: Box::new(Identifier(Ident::new("a"))),
            }),
            op: BinaryOperator::Plus,
            right: Box::new(UnaryOp {
                op: UnaryOperator::Minus,
                expr: Box::new(Identifier(Ident::new("b"))),
            }),
        },
        verified_expr(sql)
    );
}

#[test]
fn parse_unary_math_with_multiply() {
    use self::Expr::*;
    let sql = "-a * -b";
    assert_eq!(
        BinaryOp {
            left: Box::new(UnaryOp {
                op: UnaryOperator::Minus,
                expr: Box::new(Identifier(Ident::new("a"))),
            }),
            op: BinaryOperator::Multiply,
            right: Box::new(UnaryOp {
                op: UnaryOperator::Minus,
                expr: Box::new(Identifier(Ident::new("b"))),
            }),
        },
        verified_expr(sql)
    );
}

#[test]
fn parse_mod() {
    use self::Expr::*;
    let sql = "a % b";
    assert_eq!(
        BinaryOp {
            left: Box::new(Identifier(Ident::new("a"))),
            op: BinaryOperator::Modulo,
            right: Box::new(Identifier(Ident::new("b"))),
        },
        verified_expr(sql)
    );
}

fn pg_and_generic() -> TestedDialects {
    TestedDialects::new(vec![
        Box::new(PostgreSqlDialect {}),
        Box::new(GenericDialect {}),
    ])
}

fn ms_and_generic() -> TestedDialects {
    TestedDialects::new(vec![Box::new(MsSqlDialect {}), Box::new(GenericDialect {})])
}

#[test]
fn parse_json_ops_without_colon() {
    use self::BinaryOperator::*;
    let binary_ops = [
        (
            "->",
            Arrow,
            all_dialects_except(|d| d.supports_lambda_functions()),
        ),
        ("->>", LongArrow, all_dialects()),
        ("#>", HashArrow, pg_and_generic()),
        ("#>>", HashLongArrow, pg_and_generic()),
        ("@>", AtArrow, all_dialects()),
        ("<@", ArrowAt, all_dialects()),
        ("#-", HashMinus, pg_and_generic()),
        ("@?", AtQuestion, all_dialects()),
        ("@@", AtAt, all_dialects()),
    ];

    for (str_op, op, dialects) in binary_ops {
        let select = dialects.verified_only_select(&format!("SELECT a {} b", &str_op));
        assert_eq!(
            SelectItem::UnnamedExpr(Expr::BinaryOp {
                left: Box::new(Expr::Identifier(Ident::new("a"))),
                op,
                right: Box::new(Expr::Identifier(Ident::new("b"))),
            }),
            select.projection[0]
        );
    }
}

#[test]
fn parse_json_object() {
    let dialects = TestedDialects::new(vec![
        Box::new(MsSqlDialect {}),
        Box::new(PostgreSqlDialect {}),
    ]);
    let select = dialects.verified_only_select("SELECT JSON_OBJECT('name' : 'value', 'type' : 1)");
    match expr_from_projection(&select.projection[0]) {
        Expr::Function(Function {
            args: FunctionArguments::List(FunctionArgumentList { args, .. }),
            ..
        }) => assert_eq!(
            &[
                FunctionArg::ExprNamed {
                    name: Expr::Value((Value::SingleQuotedString("name".into())).with_empty_span()),
                    arg: FunctionArgExpr::Expr(Expr::Value(
                        (Value::SingleQuotedString("value".into())).with_empty_span()
                    )),
                    operator: FunctionArgOperator::Colon
                },
                FunctionArg::ExprNamed {
                    name: Expr::Value((Value::SingleQuotedString("type".into())).with_empty_span()),
                    arg: FunctionArgExpr::Expr(Expr::value(number("1"))),
                    operator: FunctionArgOperator::Colon
                }
            ],
            &args[..]
        ),
        _ => unreachable!(),
    }
    let select = dialects
        .verified_only_select("SELECT JSON_OBJECT('name' : 'value', 'type' : NULL ABSENT ON NULL)");
    match expr_from_projection(&select.projection[0]) {
        Expr::Function(Function {
            args: FunctionArguments::List(FunctionArgumentList { args, clauses, .. }),
            ..
        }) => {
            assert_eq!(
                &[
                    FunctionArg::ExprNamed {
                        name: Expr::Value(
                            (Value::SingleQuotedString("name".into())).with_empty_span()
                        ),
                        arg: FunctionArgExpr::Expr(Expr::Value(
                            (Value::SingleQuotedString("value".into())).with_empty_span()
                        )),
                        operator: FunctionArgOperator::Colon
                    },
                    FunctionArg::ExprNamed {
                        name: Expr::Value(
                            (Value::SingleQuotedString("type".into())).with_empty_span()
                        ),
                        arg: FunctionArgExpr::Expr(Expr::Value((Value::Null).with_empty_span())),
                        operator: FunctionArgOperator::Colon
                    }
                ],
                &args[..]
            );
            assert_eq!(
                &[FunctionArgumentClause::JsonNullClause(
                    JsonNullClause::AbsentOnNull
                )],
                &clauses[..]
            );
        }
        _ => unreachable!(),
    }
    let select = dialects.verified_only_select("SELECT JSON_OBJECT(NULL ON NULL)");
    match expr_from_projection(&select.projection[0]) {
        Expr::Function(Function {
            args: FunctionArguments::List(FunctionArgumentList { args, clauses, .. }),
            ..
        }) => {
            assert!(args.is_empty());
            assert_eq!(
                &[FunctionArgumentClause::JsonNullClause(
                    JsonNullClause::NullOnNull
                )],
                &clauses[..]
            );
        }
        _ => unreachable!(),
    }
    let select = dialects.verified_only_select("SELECT JSON_OBJECT(ABSENT ON NULL)");
    match expr_from_projection(&select.projection[0]) {
        Expr::Function(Function {
            args: FunctionArguments::List(FunctionArgumentList { args, clauses, .. }),
            ..
        }) => {
            assert!(args.is_empty());
            assert_eq!(
                &[FunctionArgumentClause::JsonNullClause(
                    JsonNullClause::AbsentOnNull
                )],
                &clauses[..]
            );
        }
        _ => unreachable!(),
    }
    let select = dialects.verified_only_select(
        "SELECT JSON_OBJECT('name' : 'value', 'type' : JSON_ARRAY(1, 2) ABSENT ON NULL)",
    );
    match expr_from_projection(&select.projection[0]) {
        Expr::Function(Function {
            args: FunctionArguments::List(FunctionArgumentList { args, clauses, .. }),
            ..
        }) => {
            assert_eq!(
                &FunctionArg::ExprNamed {
                    name: Expr::Value((Value::SingleQuotedString("name".into())).with_empty_span()),
                    arg: FunctionArgExpr::Expr(Expr::Value(
                        (Value::SingleQuotedString("value".into())).with_empty_span()
                    )),
                    operator: FunctionArgOperator::Colon
                },
                &args[0]
            );
            assert!(matches!(
                args[1],
                FunctionArg::ExprNamed {
                    name: Expr::Value(ValueWithSpan {
                        value: Value::SingleQuotedString(_),
                        span: _
                    }),
                    arg: FunctionArgExpr::Expr(Expr::Function(_)),
                    operator: FunctionArgOperator::Colon
                }
            ));
            assert_eq!(
                &[FunctionArgumentClause::JsonNullClause(
                    JsonNullClause::AbsentOnNull
                )],
                &clauses[..]
            );
        }
        _ => unreachable!(),
    }
    let select = dialects.verified_only_select(
        "SELECT JSON_OBJECT('name' : 'value', 'type' : JSON_OBJECT('type_id' : 1, 'name' : 'a') NULL ON NULL)",
    );
    match expr_from_projection(&select.projection[0]) {
        Expr::Function(Function {
            args: FunctionArguments::List(FunctionArgumentList { args, clauses, .. }),
            ..
        }) => {
            assert_eq!(
                &FunctionArg::ExprNamed {
                    name: Expr::Value((Value::SingleQuotedString("name".into())).with_empty_span()),
                    arg: FunctionArgExpr::Expr(Expr::Value(
                        (Value::SingleQuotedString("value".into())).with_empty_span()
                    )),
                    operator: FunctionArgOperator::Colon
                },
                &args[0]
            );
            assert!(matches!(
                args[1],
                FunctionArg::ExprNamed {
                    name: Expr::Value(ValueWithSpan {
                        value: Value::SingleQuotedString(_),
                        span: _
                    }),
                    arg: FunctionArgExpr::Expr(Expr::Function(_)),
                    operator: FunctionArgOperator::Colon
                }
            ));
            assert_eq!(
                &[FunctionArgumentClause::JsonNullClause(
                    JsonNullClause::NullOnNull
                )],
                &clauses[..]
            );
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_mod_no_spaces() {
    use self::Expr::*;
    let canonical = "a1 % b1";
    let sqls = ["a1 % b1", "a1% b1", "a1 %b1", "a1%b1"];
    for sql in sqls {
        println!("Parsing {sql}");
        assert_eq!(
            BinaryOp {
                left: Box::new(Identifier(Ident::new("a1"))),
                op: BinaryOperator::Modulo,
                right: Box::new(Identifier(Ident::new("b1"))),
            },
            pg_and_generic().expr_parses_to(sql, canonical)
        );
    }
}

#[test]
fn parse_is_null() {
    use self::Expr::*;
    let sql = "a IS NULL";
    assert_eq!(
        IsNull(Box::new(Identifier(Ident::new("a")))),
        verified_expr(sql)
    );
}

#[test]
fn parse_is_not_null() {
    use self::Expr::*;
    let sql = "a IS NOT NULL";
    assert_eq!(
        IsNotNull(Box::new(Identifier(Ident::new("a")))),
        verified_expr(sql)
    );
}

#[test]
fn parse_is_distinct_from() {
    use self::Expr::*;
    let sql = "a IS DISTINCT FROM b";
    assert_eq!(
        IsDistinctFrom(
            Box::new(Identifier(Ident::new("a"))),
            Box::new(Identifier(Ident::new("b"))),
        ),
        verified_expr(sql)
    );
}

#[test]
fn parse_is_not_distinct_from() {
    use self::Expr::*;
    let sql = "a IS NOT DISTINCT FROM b";
    assert_eq!(
        IsNotDistinctFrom(
            Box::new(Identifier(Ident::new("a"))),
            Box::new(Identifier(Ident::new("b"))),
        ),
        verified_expr(sql)
    );
}

#[test]
fn parse_not_precedence() {
    // NOT has higher precedence than OR/AND, so the following must parse as (NOT true) OR true
    let sql = "NOT 1 OR 1";
    assert_matches!(
        verified_expr(sql),
        Expr::BinaryOp {
            op: BinaryOperator::Or,
            ..
        }
    );

    // But NOT has lower precedence than comparison operators, so the following parses as NOT (a IS NULL)
    let sql = "NOT a IS NULL";
    assert_matches!(
        verified_expr(sql),
        Expr::UnaryOp {
            op: UnaryOperator::Not,
            ..
        }
    );

    // NOT has lower precedence than BETWEEN, so the following parses as NOT (1 NOT BETWEEN 1 AND 2)
    let sql = "NOT 1 NOT BETWEEN 1 AND 2";
    assert_eq!(
        verified_expr(sql),
        Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr: Box::new(Expr::Between {
                expr: Box::new(Expr::value(number("1"))),
                low: Box::new(Expr::value(number("1"))),
                high: Box::new(Expr::value(number("2"))),
                negated: true,
            }),
        },
    );

    // NOT has lower precedence than LIKE, so the following parses as NOT ('a' NOT LIKE 'b')
    let sql = "NOT 'a' NOT LIKE 'b'";
    assert_eq!(
        verified_expr(sql),
        Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr: Box::new(Expr::Like {
                expr: Box::new(Expr::Value(
                    (Value::SingleQuotedString("a".into())).with_empty_span()
                )),
                negated: true,
                pattern: Box::new(Expr::Value(
                    (Value::SingleQuotedString("b".into())).with_empty_span()
                )),
                escape_char: None,
                any: false,
            }),
        },
    );

    // NOT has lower precedence than IN, so the following parses as NOT (a NOT IN 'a')
    let sql = "NOT a NOT IN ('a')";
    assert_eq!(
        verified_expr(sql),
        Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr: Box::new(Expr::InList {
                expr: Box::new(Expr::Identifier("a".into())),
                list: vec![Expr::Value(
                    (Value::SingleQuotedString("a".into())).with_empty_span()
                )],
                negated: true,
            }),
        },
    );
}

#[test]
fn parse_null_like() {
    let sql = "SELECT \
            column1 LIKE NULL AS col_null, \
            NULL LIKE column1 AS null_col \
        FROM customers";
    let select = verified_only_select(sql);
    assert_eq!(
        SelectItem::ExprWithAlias {
            expr: Expr::Like {
                expr: Box::new(Expr::Identifier(Ident::new("column1"))),
                any: false,
                negated: false,
                pattern: Box::new(Expr::Value((Value::Null).with_empty_span())),
                escape_char: None,
            },
            alias: Ident {
                value: "col_null".to_owned(),
                quote_style: None,
                span: Span::empty(),
            },
        },
        select.projection[0]
    );
    assert_eq!(
        SelectItem::ExprWithAlias {
            expr: Expr::Like {
                expr: Box::new(Expr::Value((Value::Null).with_empty_span())),
                any: false,
                negated: false,
                pattern: Box::new(Expr::Identifier(Ident::new("column1"))),
                escape_char: None,
            },
            alias: Ident {
                value: "null_col".to_owned(),
                quote_style: None,
                span: Span::empty(),
            },
        },
        select.projection[1]
    );
}

#[test]
fn parse_ilike() {
    fn chk(negated: bool) {
        let sql = &format!(
            "SELECT * FROM customers WHERE name {}ILIKE '%a'",
            if negated { "NOT " } else { "" }
        );
        let select = verified_only_select(sql);
        assert_eq!(
            Expr::ILike {
                expr: Box::new(Expr::Identifier(Ident::new("name"))),
                negated,
                pattern: Box::new(Expr::Value(
                    (Value::SingleQuotedString("%a".to_string())).with_empty_span()
                )),
                escape_char: None,
                any: false,
            },
            select.selection.unwrap()
        );

        // Test with escape char
        let sql = &format!(
            "SELECT * FROM customers WHERE name {}ILIKE '%a' ESCAPE '^'",
            if negated { "NOT " } else { "" }
        );
        let select = verified_only_select(sql);
        assert_eq!(
            Expr::ILike {
                expr: Box::new(Expr::Identifier(Ident::new("name"))),
                negated,
                pattern: Box::new(Expr::Value(
                    (Value::SingleQuotedString("%a".to_string())).with_empty_span()
                )),
                escape_char: Some(Value::SingleQuotedString('^'.to_string())),
                any: false,
            },
            select.selection.unwrap()
        );

        // This statement tests that ILIKE and NOT ILIKE have the same precedence.
        // This was previously mishandled (#81).
        let sql = &format!(
            "SELECT * FROM customers WHERE name {}ILIKE '%a' IS NULL",
            if negated { "NOT " } else { "" }
        );
        let select = verified_only_select(sql);
        assert_eq!(
            Expr::IsNull(Box::new(Expr::ILike {
                expr: Box::new(Expr::Identifier(Ident::new("name"))),
                negated,
                pattern: Box::new(Expr::Value(
                    (Value::SingleQuotedString("%a".to_string())).with_empty_span()
                )),
                escape_char: None,
                any: false,
            })),
            select.selection.unwrap()
        );
    }
    chk(false);
    chk(true);
}

#[test]
fn parse_like() {
    fn chk(negated: bool) {
        let sql = &format!(
            "SELECT * FROM customers WHERE name {}LIKE '%a'",
            if negated { "NOT " } else { "" }
        );
        let select = verified_only_select(sql);
        assert_eq!(
            Expr::Like {
                expr: Box::new(Expr::Identifier(Ident::new("name"))),
                negated,
                pattern: Box::new(Expr::Value(
                    (Value::SingleQuotedString("%a".to_string())).with_empty_span()
                )),
                escape_char: None,
                any: false,
            },
            select.selection.unwrap()
        );

        // Test with escape char
        let sql = &format!(
            "SELECT * FROM customers WHERE name {}LIKE '%a' ESCAPE '^'",
            if negated { "NOT " } else { "" }
        );
        let select = verified_only_select(sql);
        assert_eq!(
            Expr::Like {
                expr: Box::new(Expr::Identifier(Ident::new("name"))),
                negated,
                pattern: Box::new(Expr::Value(
                    (Value::SingleQuotedString("%a".to_string())).with_empty_span()
                )),
                escape_char: Some(Value::SingleQuotedString('^'.to_string())),
                any: false,
            },
            select.selection.unwrap()
        );

        // This statement tests that LIKE and NOT LIKE have the same precedence.
        // This was previously mishandled (#81).
        let sql = &format!(
            "SELECT * FROM customers WHERE name {}LIKE '%a' IS NULL",
            if negated { "NOT " } else { "" }
        );
        let select = verified_only_select(sql);
        assert_eq!(
            Expr::IsNull(Box::new(Expr::Like {
                expr: Box::new(Expr::Identifier(Ident::new("name"))),
                negated,
                pattern: Box::new(Expr::Value(
                    (Value::SingleQuotedString("%a".to_string())).with_empty_span()
                )),
                escape_char: None,
                any: false,
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
        let select = verified_only_select(sql);
        assert_eq!(
            Expr::SimilarTo {
                expr: Box::new(Expr::Identifier(Ident::new("name"))),
                negated,
                pattern: Box::new(Expr::Value(
                    (Value::SingleQuotedString("%a".to_string())).with_empty_span()
                )),
                escape_char: None,
            },
            select.selection.unwrap()
        );

        // Test with escape char
        let sql = &format!(
            "SELECT * FROM customers WHERE name {}SIMILAR TO '%a' ESCAPE '^'",
            if negated { "NOT " } else { "" }
        );
        let select = verified_only_select(sql);
        assert_eq!(
            Expr::SimilarTo {
                expr: Box::new(Expr::Identifier(Ident::new("name"))),
                negated,
                pattern: Box::new(Expr::Value(
                    (Value::SingleQuotedString("%a".to_string())).with_empty_span()
                )),
                escape_char: Some(Value::SingleQuotedString('^'.to_string())),
            },
            select.selection.unwrap()
        );

        let sql = &format!(
            "SELECT * FROM customers WHERE name {}SIMILAR TO '%a' ESCAPE NULL",
            if negated { "NOT " } else { "" }
        );
        let select = verified_only_select(sql);
        assert_eq!(
            Expr::SimilarTo {
                expr: Box::new(Expr::Identifier(Ident::new("name"))),
                negated,
                pattern: Box::new(Expr::Value(
                    (Value::SingleQuotedString("%a".to_string())).with_empty_span()
                )),
                escape_char: Some(Value::Null),
            },
            select.selection.unwrap()
        );

        // This statement tests that SIMILAR TO and NOT SIMILAR TO have the same precedence.
        let sql = &format!(
            "SELECT * FROM customers WHERE name {}SIMILAR TO '%a' ESCAPE '^' IS NULL",
            if negated { "NOT " } else { "" }
        );
        let select = verified_only_select(sql);
        assert_eq!(
            Expr::IsNull(Box::new(Expr::SimilarTo {
                expr: Box::new(Expr::Identifier(Ident::new("name"))),
                negated,
                pattern: Box::new(Expr::Value(
                    (Value::SingleQuotedString("%a".to_string())).with_empty_span()
                )),
                escape_char: Some(Value::SingleQuotedString('^'.to_string())),
            })),
            select.selection.unwrap()
        );
    }
    chk(false);
    chk(true);
}

#[test]
fn parse_in_list() {
    fn chk(negated: bool) {
        let sql = &format!(
            "SELECT * FROM customers WHERE segment {}IN ('HIGH', 'MED')",
            if negated { "NOT " } else { "" }
        );
        let select = verified_only_select(sql);
        assert_eq!(
            Expr::InList {
                expr: Box::new(Expr::Identifier(Ident::new("segment"))),
                list: vec![
                    Expr::Value((Value::SingleQuotedString("HIGH".to_string())).with_empty_span()),
                    Expr::Value((Value::SingleQuotedString("MED".to_string())).with_empty_span()),
                ],
                negated,
            },
            select.selection.unwrap()
        );
    }
    chk(false);
    chk(true);
}

#[test]
fn parse_in_subquery() {
    let sql = "SELECT * FROM customers WHERE segment IN (SELECT segm FROM bar)";
    let select = verified_only_select(sql);
    assert_eq!(
        Expr::InSubquery {
            expr: Box::new(Expr::Identifier(Ident::new("segment"))),
            subquery: Box::new(verified_query("SELECT segm FROM bar")),
            negated: false,
        },
        select.selection.unwrap()
    );
}

#[test]
fn parse_in_union() {
    let sql = "SELECT * FROM customers WHERE segment IN ((SELECT segm FROM bar) UNION (SELECT segm FROM bar2))";
    let select = verified_only_select(sql);
    assert_eq!(
        Expr::InSubquery {
            expr: Box::new(Expr::Identifier(Ident::new("segment"))),
            subquery: Box::new(verified_query(
                "(SELECT segm FROM bar) UNION (SELECT segm FROM bar2)"
            )),
            negated: false,
        },
        select.selection.unwrap()
    );
}

#[test]
fn parse_in_unnest() {
    fn chk(negated: bool) {
        let sql = &format!(
            "SELECT * FROM customers WHERE segment {}IN UNNEST(expr)",
            if negated { "NOT " } else { "" }
        );
        let select = verified_only_select(sql);
        assert_eq!(
            Expr::InUnnest {
                expr: Box::new(Expr::Identifier(Ident::new("segment"))),
                array_expr: Box::new(verified_expr("expr")),
                negated,
            },
            select.selection.unwrap()
        );
    }
    chk(false);
    chk(true);
}

#[test]
fn parse_in_error() {
    // <expr> IN <expr> is no valid
    let sql = "SELECT * FROM customers WHERE segment in segment";
    let res = parse_sql_statements(sql);
    assert_eq!(
        ParserError::ParserError("Expected: (, found: segment".to_string()),
        res.unwrap_err()
    );
}

#[test]
fn parse_string_agg() {
    let sql = "SELECT a || b";

    let select = verified_only_select(sql);
    assert_eq!(
        SelectItem::UnnamedExpr(Expr::BinaryOp {
            left: Box::new(Expr::Identifier(Ident::new("a"))),
            op: BinaryOperator::StringConcat,
            right: Box::new(Expr::Identifier(Ident::new("b"))),
        }),
        select.projection[0]
    );
}

/// selects all dialects but PostgreSQL
pub fn all_dialects_but_pg() -> TestedDialects {
    TestedDialects::new(
        all_dialects()
            .dialects
            .into_iter()
            .filter(|x| !x.is::<PostgreSqlDialect>())
            .collect(),
    )
}

#[test]
fn parse_bitwise_ops() {
    let bitwise_ops = &[
        ("^", BinaryOperator::BitwiseXor, all_dialects_but_pg()),
        ("|", BinaryOperator::BitwiseOr, all_dialects()),
        ("&", BinaryOperator::BitwiseAnd, all_dialects()),
    ];

    for (str_op, op, dialects) in bitwise_ops {
        let select = dialects.verified_only_select(&format!("SELECT a {} b", &str_op));
        assert_eq!(
            SelectItem::UnnamedExpr(Expr::BinaryOp {
                left: Box::new(Expr::Identifier(Ident::new("a"))),
                op: op.clone(),
                right: Box::new(Expr::Identifier(Ident::new("b"))),
            }),
            select.projection[0]
        );
    }
}

#[test]
fn parse_binary_any() {
    let select = verified_only_select("SELECT a = ANY(b)");
    assert_eq!(
        SelectItem::UnnamedExpr(Expr::AnyOp {
            left: Box::new(Expr::Identifier(Ident::new("a"))),
            compare_op: BinaryOperator::Eq,
            right: Box::new(Expr::Identifier(Ident::new("b"))),
            is_some: false,
        }),
        select.projection[0]
    );
}

#[test]
fn parse_binary_all() {
    let select = verified_only_select("SELECT a = ALL(b)");
    assert_eq!(
        SelectItem::UnnamedExpr(Expr::AllOp {
            left: Box::new(Expr::Identifier(Ident::new("a"))),
            compare_op: BinaryOperator::Eq,
            right: Box::new(Expr::Identifier(Ident::new("b"))),
        }),
        select.projection[0]
    );
}

#[test]
fn parse_between() {
    fn chk(negated: bool) {
        let sql = &format!(
            "SELECT * FROM customers WHERE age {}BETWEEN 25 AND 32",
            if negated { "NOT " } else { "" }
        );
        let select = verified_only_select(sql);
        assert_eq!(
            Expr::Between {
                expr: Box::new(Expr::Identifier(Ident::new("age"))),
                low: Box::new(Expr::value(number("25"))),
                high: Box::new(Expr::value(number("32"))),
                negated,
            },
            select.selection.unwrap()
        );
    }
    chk(false);
    chk(true);
}

#[test]
fn parse_between_with_expr() {
    use self::BinaryOperator::*;
    let sql = "SELECT * FROM t WHERE 1 BETWEEN 1 + 2 AND 3 + 4 IS NULL";
    let select = verified_only_select(sql);
    assert_eq!(
        Expr::IsNull(Box::new(Expr::Between {
            expr: Box::new(Expr::value(number("1"))),
            low: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::value(number("1"))),
                op: Plus,
                right: Box::new(Expr::value(number("2"))),
            }),
            high: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::value(number("3"))),
                op: Plus,
                right: Box::new(Expr::value(number("4"))),
            }),
            negated: false,
        })),
        select.selection.unwrap()
    );

    let sql = "SELECT * FROM t WHERE 1 = 1 AND 1 + x BETWEEN 1 AND 2";
    let select = verified_only_select(sql);
    assert_eq!(
        Expr::BinaryOp {
            left: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::value(number("1"))),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::value(number("1"))),
            }),
            op: BinaryOperator::And,
            right: Box::new(Expr::Between {
                expr: Box::new(Expr::BinaryOp {
                    left: Box::new(Expr::value(number("1"))),
                    op: BinaryOperator::Plus,
                    right: Box::new(Expr::Identifier(Ident::new("x"))),
                }),
                low: Box::new(Expr::value(number("1"))),
                high: Box::new(Expr::value(number("2"))),
                negated: false,
            }),
        },
        select.selection.unwrap(),
    )
}

#[test]
fn parse_tuples() {
    let sql = "SELECT (1, 2), (1), ('foo', 3, baz)";
    let select = verified_only_select(sql);
    assert_eq!(
        vec![
            SelectItem::UnnamedExpr(Expr::Tuple(vec![
                Expr::value(number("1")),
                Expr::value(number("2")),
            ])),
            SelectItem::UnnamedExpr(Expr::Nested(Box::new(Expr::Value(
                (number("1")).with_empty_span()
            )))),
            SelectItem::UnnamedExpr(Expr::Tuple(vec![
                Expr::Value((Value::SingleQuotedString("foo".into())).with_empty_span()),
                Expr::value(number("3")),
                Expr::Identifier(Ident::new("baz")),
            ])),
        ],
        select.projection
    );
}

#[test]
fn parse_tuple_invalid() {
    let sql = "select (1";
    let res = parse_sql_statements(sql);
    assert_eq!(
        ParserError::ParserError("Expected: ), found: EOF".to_string()),
        res.unwrap_err()
    );

    let sql = "select (), 2";
    let res = parse_sql_statements(sql);
    assert_eq!(
        ParserError::ParserError("Expected: an expression, found: )".to_string()),
        res.unwrap_err()
    );
}

#[test]
fn parse_select_order_by() {
    fn chk(sql: &str) {
        let select = verified_query(sql);
        assert_eq!(
            OrderByKind::Expressions(vec![
                OrderByExpr {
                    expr: Expr::Identifier(Ident::new("lname")),
                    options: OrderByOptions {
                        asc: Some(true),
                        nulls_first: None,
                    },
                    with_fill: None,
                },
                OrderByExpr {
                    expr: Expr::Identifier(Ident::new("fname")),
                    options: OrderByOptions {
                        asc: Some(false),
                        nulls_first: None,
                    },
                    with_fill: None,
                },
                OrderByExpr {
                    expr: Expr::Identifier(Ident::new("id")),
                    options: OrderByOptions {
                        asc: None,
                        nulls_first: None,
                    },
                    with_fill: None,
                },
            ]),
            select.order_by.expect("ORDER BY expected").kind
        );
    }
    chk("SELECT id, fname, lname FROM customer WHERE id < 5 ORDER BY lname ASC, fname DESC, id");
    // make sure ORDER is not treated as an alias
    chk("SELECT id, fname, lname FROM customer ORDER BY lname ASC, fname DESC, id");
    chk("SELECT 1 AS lname, 2 AS fname, 3 AS id, 4 ORDER BY lname ASC, fname DESC, id");
}

#[test]
fn parse_select_order_by_limit() {
    let sql = "SELECT id, fname, lname FROM customer WHERE id < 5 \
               ORDER BY lname ASC, fname DESC LIMIT 2";
    let select = verified_query(sql);
    assert_eq!(
        OrderByKind::Expressions(vec![
            OrderByExpr {
                expr: Expr::Identifier(Ident::new("lname")),
                options: OrderByOptions {
                    asc: Some(true),
                    nulls_first: None,
                },
                with_fill: None,
            },
            OrderByExpr {
                expr: Expr::Identifier(Ident::new("fname")),
                options: OrderByOptions {
                    asc: Some(false),
                    nulls_first: None,
                },
                with_fill: None,
            },
        ]),
        select.order_by.expect("ORDER BY expected").kind
    );
    let expected_limit_clause = LimitClause::LimitOffset {
        limit: Some(Expr::value(number("2"))),
        offset: None,
        limit_by: vec![],
    };
    assert_eq!(Some(expected_limit_clause), select.limit_clause);
}

#[test]
fn parse_select_order_by_all() {
    fn chk(sql: &str, except_order_by: OrderByKind) {
        let dialects = all_dialects_where(|d| d.supports_order_by_all());
        let select = dialects.verified_query(sql);
        assert_eq!(
            except_order_by,
            select.order_by.expect("ORDER BY expected").kind
        );
    }
    let test_cases = [
        (
            "SELECT id, fname, lname FROM customer WHERE id < 5 ORDER BY ALL",
            OrderByKind::All(OrderByOptions {
                asc: None,
                nulls_first: None,
            }),
        ),
        (
            "SELECT id, fname, lname FROM customer WHERE id < 5 ORDER BY ALL NULLS FIRST",
            OrderByKind::All(OrderByOptions {
                asc: None,
                nulls_first: Some(true),
            }),
        ),
        (
            "SELECT id, fname, lname FROM customer WHERE id < 5 ORDER BY ALL NULLS LAST",
            OrderByKind::All(OrderByOptions {
                asc: None,
                nulls_first: Some(false),
            }),
        ),
        (
            "SELECT id, fname, lname FROM customer ORDER BY ALL ASC",
            OrderByKind::All(OrderByOptions {
                asc: Some(true),
                nulls_first: None,
            }),
        ),
        (
            "SELECT id, fname, lname FROM customer ORDER BY ALL ASC NULLS FIRST",
            OrderByKind::All(OrderByOptions {
                asc: Some(true),
                nulls_first: Some(true),
            }),
        ),
        (
            "SELECT id, fname, lname FROM customer ORDER BY ALL ASC NULLS LAST",
            OrderByKind::All(OrderByOptions {
                asc: Some(true),
                nulls_first: Some(false),
            }),
        ),
        (
            "SELECT id, fname, lname FROM customer WHERE id < 5 ORDER BY ALL DESC",
            OrderByKind::All(OrderByOptions {
                asc: Some(false),
                nulls_first: None,
            }),
        ),
        (
            "SELECT id, fname, lname FROM customer WHERE id < 5 ORDER BY ALL DESC NULLS FIRST",
            OrderByKind::All(OrderByOptions {
                asc: Some(false),
                nulls_first: Some(true),
            }),
        ),
        (
            "SELECT id, fname, lname FROM customer WHERE id < 5 ORDER BY ALL DESC NULLS LAST",
            OrderByKind::All(OrderByOptions {
                asc: Some(false),
                nulls_first: Some(false),
            }),
        ),
    ];

    for (sql, expected_order_by) in test_cases {
        chk(sql, expected_order_by);
    }
}

#[test]
fn parse_select_order_by_not_support_all() {
    fn chk(sql: &str, except_order_by: OrderByKind) {
        let dialects = all_dialects_where(|d| !d.supports_order_by_all());
        let select = dialects.verified_query(sql);
        assert_eq!(
            except_order_by,
            select.order_by.expect("ORDER BY expected").kind
        );
    }
    let test_cases = [
        (
            "SELECT id, ALL FROM customer WHERE id < 5 ORDER BY ALL",
            OrderByKind::Expressions(vec![OrderByExpr {
                expr: Expr::Identifier(Ident::new("ALL")),
                options: OrderByOptions {
                    asc: None,
                    nulls_first: None,
                },
                with_fill: None,
            }]),
        ),
        (
            "SELECT id, ALL FROM customer ORDER BY ALL ASC NULLS FIRST",
            OrderByKind::Expressions(vec![OrderByExpr {
                expr: Expr::Identifier(Ident::new("ALL")),
                options: OrderByOptions {
                    asc: Some(true),
                    nulls_first: Some(true),
                },
                with_fill: None,
            }]),
        ),
        (
            "SELECT id, ALL FROM customer ORDER BY ALL DESC NULLS LAST",
            OrderByKind::Expressions(vec![OrderByExpr {
                expr: Expr::Identifier(Ident::new("ALL")),
                options: OrderByOptions {
                    asc: Some(false),
                    nulls_first: Some(false),
                },
                with_fill: None,
            }]),
        ),
    ];

    for (sql, expected_order_by) in test_cases {
        chk(sql, expected_order_by);
    }
}

#[test]
fn parse_select_order_by_nulls_order() {
    let sql = "SELECT id, fname, lname FROM customer WHERE id < 5 \
               ORDER BY lname ASC NULLS FIRST, fname DESC NULLS LAST LIMIT 2";
    let select = verified_query(sql);
    assert_eq!(
        OrderByKind::Expressions(vec![
            OrderByExpr {
                expr: Expr::Identifier(Ident::new("lname")),
                options: OrderByOptions {
                    asc: Some(true),
                    nulls_first: Some(true),
                },
                with_fill: None,
            },
            OrderByExpr {
                expr: Expr::Identifier(Ident::new("fname")),
                options: OrderByOptions {
                    asc: Some(false),
                    nulls_first: Some(false),
                },
                with_fill: None,
            },
        ]),
        select.order_by.expect("ORDER BY expeccted").kind
    );
    let expected_limit_clause = LimitClause::LimitOffset {
        limit: Some(Expr::value(number("2"))),
        offset: None,
        limit_by: vec![],
    };
    assert_eq!(Some(expected_limit_clause), select.limit_clause);
}

#[test]
fn parse_select_group_by() {
    let sql = "SELECT id, fname, lname FROM customer GROUP BY lname, fname";
    let select = verified_only_select(sql);
    assert_eq!(
        GroupByExpr::Expressions(
            vec![
                Expr::Identifier(Ident::new("lname")),
                Expr::Identifier(Ident::new("fname")),
            ],
            vec![]
        ),
        select.group_by
    );

    // Tuples can also be in the set
    one_statement_parses_to(
        "SELECT id, fname, lname FROM customer GROUP BY (lname, fname)",
        "SELECT id, fname, lname FROM customer GROUP BY (lname, fname)",
    );
}

#[test]
fn parse_select_group_by_all() {
    let sql = "SELECT id, fname, lname, SUM(order) FROM customer GROUP BY ALL";
    let select = verified_only_select(sql);
    assert_eq!(GroupByExpr::All(vec![]), select.group_by);

    one_statement_parses_to(
        "SELECT id, fname, lname, SUM(order) FROM customer GROUP BY ALL",
        "SELECT id, fname, lname, SUM(order) FROM customer GROUP BY ALL",
    );
}

#[test]
fn parse_group_by_with_modifier() {
    let clauses = ["x", "a, b", "ALL"];
    let modifiers = [
        "WITH ROLLUP",
        "WITH CUBE",
        "WITH TOTALS",
        "WITH ROLLUP WITH CUBE",
    ];
    let expected_modifiers = [
        vec![GroupByWithModifier::Rollup],
        vec![GroupByWithModifier::Cube],
        vec![GroupByWithModifier::Totals],
        vec![GroupByWithModifier::Rollup, GroupByWithModifier::Cube],
    ];
    let dialects = all_dialects_where(|d| d.supports_group_by_with_modifier());

    for clause in &clauses {
        for (modifier, expected_modifier) in modifiers.iter().zip(expected_modifiers.iter()) {
            let sql = format!("SELECT * FROM t GROUP BY {clause} {modifier}");
            match dialects.verified_stmt(&sql) {
                Statement::Query(query) => {
                    let group_by = &query.body.as_select().unwrap().group_by;
                    if clause == &"ALL" {
                        assert_eq!(group_by, &GroupByExpr::All(expected_modifier.to_vec()));
                    } else {
                        assert_eq!(
                            group_by,
                            &GroupByExpr::Expressions(
                                clause
                                    .split(", ")
                                    .map(|c| Identifier(Ident::new(c)))
                                    .collect(),
                                expected_modifier.to_vec()
                            )
                        );
                    }
                }
                _ => unreachable!(),
            }
        }
    }

    // invalid cases
    let invalid_cases = [
        "SELECT * FROM t GROUP BY x WITH",
        "SELECT * FROM t GROUP BY x WITH ROLLUP CUBE",
        "SELECT * FROM t GROUP BY x WITH WITH ROLLUP",
        "SELECT * FROM t GROUP BY WITH ROLLUP",
    ];
    for sql in invalid_cases {
        dialects
            .parse_sql_statements(sql)
            .expect_err("Expected: one of ROLLUP or CUBE or TOTALS, found: WITH");
    }
}

#[test]
fn parse_group_by_special_grouping_sets() {
    let sql = "SELECT a, b, SUM(c) FROM tab1 GROUP BY a, b GROUPING SETS ((a, b), (a), (b), ())";
    match all_dialects().verified_stmt(sql) {
        Statement::Query(query) => {
            let group_by = &query.body.as_select().unwrap().group_by;
            assert_eq!(
                group_by,
                &GroupByExpr::Expressions(
                    vec![
                        Expr::Identifier(Ident::new("a")),
                        Expr::Identifier(Ident::new("b"))
                    ],
                    vec![GroupByWithModifier::GroupingSets(Expr::GroupingSets(vec![
                        vec![
                            Expr::Identifier(Ident::new("a")),
                            Expr::Identifier(Ident::new("b"))
                        ],
                        vec![Expr::Identifier(Ident::new("a")),],
                        vec![Expr::Identifier(Ident::new("b"))],
                        vec![]
                    ]))]
                )
            );
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_group_by_grouping_sets_single_values() {
    let sql = "SELECT a, b, SUM(c) FROM tab1 GROUP BY a, b GROUPING SETS ((a, b), a, (b), c, ())";
    let canonical =
        "SELECT a, b, SUM(c) FROM tab1 GROUP BY a, b GROUPING SETS ((a, b), (a), (b), (c), ())";
    match all_dialects().one_statement_parses_to(sql, canonical) {
        Statement::Query(query) => {
            let group_by = &query.body.as_select().unwrap().group_by;
            assert_eq!(
                group_by,
                &GroupByExpr::Expressions(
                    vec![
                        Expr::Identifier(Ident::new("a")),
                        Expr::Identifier(Ident::new("b"))
                    ],
                    vec![GroupByWithModifier::GroupingSets(Expr::GroupingSets(vec![
                        vec![
                            Expr::Identifier(Ident::new("a")),
                            Expr::Identifier(Ident::new("b"))
                        ],
                        vec![Expr::Identifier(Ident::new("a"))],
                        vec![Expr::Identifier(Ident::new("b"))],
                        vec![Expr::Identifier(Ident::new("c"))],
                        vec![]
                    ]))]
                )
            );
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_select_having() {
    let sql = "SELECT foo FROM bar GROUP BY foo HAVING COUNT(*) > 1";
    let select = verified_only_select(sql);
    assert_eq!(
        Some(Expr::BinaryOp {
            left: Box::new(Expr::Function(Function {
                name: ObjectName::from(vec![Ident::new("COUNT")]),
                uses_odbc_syntax: false,
                parameters: FunctionArguments::None,
                args: FunctionArguments::List(FunctionArgumentList {
                    duplicate_treatment: None,
                    args: vec![FunctionArg::Unnamed(FunctionArgExpr::Wildcard)],
                    clauses: vec![],
                }),
                null_treatment: None,
                filter: None,
                over: None,
                within_group: vec![]
            })),
            op: BinaryOperator::Gt,
            right: Box::new(Expr::value(number("1"))),
        }),
        select.having
    );

    let sql = "SELECT 'foo' HAVING 1 = 1";
    let select = verified_only_select(sql);
    assert!(select.having.is_some());
}

#[test]
fn parse_select_qualify() {
    let sql = "SELECT i, p, o FROM qt QUALIFY ROW_NUMBER() OVER (PARTITION BY p ORDER BY o) = 1";
    let select = verified_only_select(sql);
    assert_eq!(
        Some(Expr::BinaryOp {
            left: Box::new(Expr::Function(Function {
                name: ObjectName::from(vec![Ident::new("ROW_NUMBER")]),
                uses_odbc_syntax: false,
                parameters: FunctionArguments::None,
                args: FunctionArguments::List(FunctionArgumentList {
                    duplicate_treatment: None,
                    args: vec![],
                    clauses: vec![],
                }),
                null_treatment: None,
                filter: None,
                over: Some(WindowType::WindowSpec(WindowSpec {
                    window_name: None,
                    partition_by: vec![Expr::Identifier(Ident::new("p"))],
                    order_by: vec![OrderByExpr {
                        expr: Expr::Identifier(Ident::new("o")),
                        options: OrderByOptions {
                            asc: None,
                            nulls_first: None,
                        },
                        with_fill: None,
                    }],
                    window_frame: None,
                })),
                within_group: vec![]
            })),
            op: BinaryOperator::Eq,
            right: Box::new(Expr::value(number("1"))),
        }),
        select.qualify
    );

    let sql = "SELECT i, p, o, ROW_NUMBER() OVER (PARTITION BY p ORDER BY o) AS row_num FROM qt QUALIFY row_num = 1";
    let select = verified_only_select(sql);
    assert_eq!(
        Some(Expr::BinaryOp {
            left: Box::new(Expr::Identifier(Ident::new("row_num"))),
            op: BinaryOperator::Eq,
            right: Box::new(Expr::value(number("1"))),
        }),
        select.qualify
    );
}

#[test]
fn parse_limit_accepts_all() {
    one_statement_parses_to(
        "SELECT id, fname, lname FROM customer WHERE id = 1 LIMIT ALL",
        "SELECT id, fname, lname FROM customer WHERE id = 1",
    );
    one_statement_parses_to(
        "SELECT id, fname, lname FROM customer WHERE id = 1 LIMIT ALL OFFSET 1",
        "SELECT id, fname, lname FROM customer WHERE id = 1 OFFSET 1",
    );
    one_statement_parses_to(
        "SELECT id, fname, lname FROM customer WHERE id = 1 OFFSET 1 LIMIT ALL",
        "SELECT id, fname, lname FROM customer WHERE id = 1 OFFSET 1",
    );
}

#[test]
fn parse_cast() {
    let sql = "SELECT CAST(id AS BIGINT) FROM customer";
    let select = verified_only_select(sql);
    assert_eq!(
        &Expr::Cast {
            kind: CastKind::Cast,
            expr: Box::new(Expr::Identifier(Ident::new("id"))),
            data_type: DataType::BigInt(None),
            format: None,
        },
        expr_from_projection(only(&select.projection))
    );

    let sql = "SELECT CAST(id AS TINYINT) FROM customer";
    let select = verified_only_select(sql);
    assert_eq!(
        &Expr::Cast {
            kind: CastKind::Cast,
            expr: Box::new(Expr::Identifier(Ident::new("id"))),
            data_type: DataType::TinyInt(None),
            format: None,
        },
        expr_from_projection(only(&select.projection))
    );

    one_statement_parses_to(
        "SELECT CAST(id AS MEDIUMINT) FROM customer",
        "SELECT CAST(id AS MEDIUMINT) FROM customer",
    );

    one_statement_parses_to(
        "SELECT CAST(id AS BIGINT) FROM customer",
        "SELECT CAST(id AS BIGINT) FROM customer",
    );

    verified_stmt("SELECT CAST(id AS NUMERIC) FROM customer");

    verified_stmt("SELECT CAST(id AS DEC) FROM customer");

    verified_stmt("SELECT CAST(id AS DECIMAL) FROM customer");

    let sql = "SELECT CAST(id AS NVARCHAR(50)) FROM customer";
    let select = verified_only_select(sql);
    assert_eq!(
        &Expr::Cast {
            kind: CastKind::Cast,
            expr: Box::new(Expr::Identifier(Ident::new("id"))),
            data_type: DataType::Nvarchar(Some(CharacterLength::IntegerLength {
                length: 50,
                unit: None,
            })),
            format: None,
        },
        expr_from_projection(only(&select.projection))
    );

    let sql = "SELECT CAST(id AS CLOB) FROM customer";
    let select = verified_only_select(sql);
    assert_eq!(
        &Expr::Cast {
            kind: CastKind::Cast,
            expr: Box::new(Expr::Identifier(Ident::new("id"))),
            data_type: DataType::Clob(None),
            format: None,
        },
        expr_from_projection(only(&select.projection))
    );

    let sql = "SELECT CAST(id AS CLOB(50)) FROM customer";
    let select = verified_only_select(sql);
    assert_eq!(
        &Expr::Cast {
            kind: CastKind::Cast,
            expr: Box::new(Expr::Identifier(Ident::new("id"))),
            data_type: DataType::Clob(Some(50)),
            format: None,
        },
        expr_from_projection(only(&select.projection))
    );

    let sql = "SELECT CAST(id AS BINARY(50)) FROM customer";
    let select = verified_only_select(sql);
    assert_eq!(
        &Expr::Cast {
            kind: CastKind::Cast,
            expr: Box::new(Expr::Identifier(Ident::new("id"))),
            data_type: DataType::Binary(Some(50)),
            format: None,
        },
        expr_from_projection(only(&select.projection))
    );

    let sql = "SELECT CAST(id AS VARBINARY(50)) FROM customer";
    let select = verified_only_select(sql);
    assert_eq!(
        &Expr::Cast {
            kind: CastKind::Cast,
            expr: Box::new(Expr::Identifier(Ident::new("id"))),
            data_type: DataType::Varbinary(Some(BinaryLength::IntegerLength { length: 50 })),
            format: None,
        },
        expr_from_projection(only(&select.projection))
    );

    let sql = "SELECT CAST(id AS BLOB) FROM customer";
    let select = verified_only_select(sql);
    assert_eq!(
        &Expr::Cast {
            kind: CastKind::Cast,
            expr: Box::new(Expr::Identifier(Ident::new("id"))),
            data_type: DataType::Blob(None),
            format: None,
        },
        expr_from_projection(only(&select.projection))
    );

    let sql = "SELECT CAST(id AS BLOB(50)) FROM customer";
    let select = verified_only_select(sql);
    assert_eq!(
        &Expr::Cast {
            kind: CastKind::Cast,
            expr: Box::new(Expr::Identifier(Ident::new("id"))),
            data_type: DataType::Blob(Some(50)),
            format: None,
        },
        expr_from_projection(only(&select.projection))
    );

    let sql = "SELECT CAST(details AS JSONB) FROM customer";
    let select = verified_only_select(sql);
    assert_eq!(
        &Expr::Cast {
            kind: CastKind::Cast,
            expr: Box::new(Expr::Identifier(Ident::new("details"))),
            data_type: DataType::JSONB,
            format: None,
        },
        expr_from_projection(only(&select.projection))
    );
}

#[test]
fn parse_try_cast() {
    let sql = "SELECT TRY_CAST(id AS BIGINT) FROM customer";
    let select = verified_only_select(sql);
    assert_eq!(
        &Expr::Cast {
            kind: CastKind::TryCast,
            expr: Box::new(Expr::Identifier(Ident::new("id"))),
            data_type: DataType::BigInt(None),
            format: None,
        },
        expr_from_projection(only(&select.projection))
    );
    verified_stmt("SELECT TRY_CAST(id AS BIGINT) FROM customer");

    verified_stmt("SELECT TRY_CAST(id AS NUMERIC) FROM customer");

    verified_stmt("SELECT TRY_CAST(id AS DEC) FROM customer");

    verified_stmt("SELECT TRY_CAST(id AS DECIMAL) FROM customer");
}

#[test]
fn parse_extract() {
    let sql = "SELECT EXTRACT(YEAR FROM d)";
    let select = verified_only_select(sql);
    assert_eq!(
        &Expr::Extract {
            field: DateTimeField::Year,
            syntax: ExtractSyntax::From,
            expr: Box::new(Expr::Identifier(Ident::new("d"))),
        },
        expr_from_projection(only(&select.projection)),
    );

    one_statement_parses_to("SELECT EXTRACT(year from d)", "SELECT EXTRACT(YEAR FROM d)");

    verified_stmt("SELECT EXTRACT(MONTH FROM d)");
    verified_stmt("SELECT EXTRACT(WEEK FROM d)");
    verified_stmt("SELECT EXTRACT(DAY FROM d)");
    verified_stmt("SELECT EXTRACT(DAYOFWEEK FROM d)");
    verified_stmt("SELECT EXTRACT(DAYOFYEAR FROM d)");
    verified_stmt("SELECT EXTRACT(DATE FROM d)");
    verified_stmt("SELECT EXTRACT(DATETIME FROM d)");
    verified_stmt("SELECT EXTRACT(HOUR FROM d)");
    verified_stmt("SELECT EXTRACT(MINUTE FROM d)");
    verified_stmt("SELECT EXTRACT(SECOND FROM d)");
    verified_stmt("SELECT EXTRACT(MILLISECOND FROM d)");
    verified_stmt("SELECT EXTRACT(MICROSECOND FROM d)");
    verified_stmt("SELECT EXTRACT(NANOSECOND FROM d)");
    verified_stmt("SELECT EXTRACT(CENTURY FROM d)");
    verified_stmt("SELECT EXTRACT(DECADE FROM d)");
    verified_stmt("SELECT EXTRACT(DOW FROM d)");
    verified_stmt("SELECT EXTRACT(DOY FROM d)");
    verified_stmt("SELECT EXTRACT(EPOCH FROM d)");
    verified_stmt("SELECT EXTRACT(ISODOW FROM d)");
    verified_stmt("SELECT EXTRACT(ISOWEEK FROM d)");
    verified_stmt("SELECT EXTRACT(ISOYEAR FROM d)");
    verified_stmt("SELECT EXTRACT(JULIAN FROM d)");
    verified_stmt("SELECT EXTRACT(MICROSECOND FROM d)");
    verified_stmt("SELECT EXTRACT(MICROSECONDS FROM d)");
    verified_stmt("SELECT EXTRACT(MILLENIUM FROM d)");
    verified_stmt("SELECT EXTRACT(MILLENNIUM FROM d)");
    verified_stmt("SELECT EXTRACT(MILLISECOND FROM d)");
    verified_stmt("SELECT EXTRACT(MILLISECONDS FROM d)");
    verified_stmt("SELECT EXTRACT(QUARTER FROM d)");
    verified_stmt("SELECT EXTRACT(TIMEZONE FROM d)");
    verified_stmt("SELECT EXTRACT(TIMEZONE_ABBR FROM d)");
    verified_stmt("SELECT EXTRACT(TIMEZONE_HOUR FROM d)");
    verified_stmt("SELECT EXTRACT(TIMEZONE_MINUTE FROM d)");
    verified_stmt("SELECT EXTRACT(TIMEZONE_REGION FROM d)");
    verified_stmt("SELECT EXTRACT(TIME FROM d)");

    let dialects = all_dialects_except(|d| d.allow_extract_custom());
    let res = dialects.parse_sql_statements("SELECT EXTRACT(JIFFY FROM d)");
    assert_eq!(
        ParserError::ParserError("Expected: date/time field, found: JIFFY".to_string()),
        res.unwrap_err()
    );
}

#[test]
fn parse_ceil_number() {
    verified_stmt("SELECT CEIL(1.5)");
    verified_stmt("SELECT CEIL(float_column) FROM my_table");
}

#[test]
fn parse_floor_number() {
    verified_stmt("SELECT FLOOR(1.5)");
    verified_stmt("SELECT FLOOR(float_column) FROM my_table");
}

#[test]
fn parse_ceil_number_scale() {
    verified_stmt("SELECT CEIL(1.5, 1)");
    verified_stmt("SELECT CEIL(float_column, 3) FROM my_table");
}

#[test]
fn parse_floor_number_scale() {
    verified_stmt("SELECT FLOOR(1.5, 1)");
    verified_stmt("SELECT FLOOR(float_column, 3) FROM my_table");
}

#[test]
fn parse_ceil_scale() {
    let sql = "SELECT CEIL(d, 2)";
    let select = verified_only_select(sql);

    #[cfg(feature = "bigdecimal")]
    assert_eq!(
        &Expr::Ceil {
            expr: Box::new(Expr::Identifier(Ident::new("d"))),
            field: CeilFloorKind::Scale(Value::Number(bigdecimal::BigDecimal::from(2), false)),
        },
        expr_from_projection(only(&select.projection)),
    );

    #[cfg(not(feature = "bigdecimal"))]
    assert_eq!(
        &Expr::Ceil {
            expr: Box::new(Expr::Identifier(Ident::new("d"))),
            field: CeilFloorKind::Scale(Value::Number(2.to_string(), false)),
        },
        expr_from_projection(only(&select.projection)),
    );
}

#[test]
fn parse_floor_scale() {
    let sql = "SELECT FLOOR(d, 2)";
    let select = verified_only_select(sql);

    #[cfg(feature = "bigdecimal")]
    assert_eq!(
        &Expr::Floor {
            expr: Box::new(Expr::Identifier(Ident::new("d"))),
            field: CeilFloorKind::Scale(Value::Number(bigdecimal::BigDecimal::from(2), false)),
        },
        expr_from_projection(only(&select.projection)),
    );

    #[cfg(not(feature = "bigdecimal"))]
    assert_eq!(
        &Expr::Floor {
            expr: Box::new(Expr::Identifier(Ident::new("d"))),
            field: CeilFloorKind::Scale(Value::Number(2.to_string(), false)),
        },
        expr_from_projection(only(&select.projection)),
    );
}

#[test]
fn parse_ceil_datetime() {
    let sql = "SELECT CEIL(d TO DAY)";
    let select = verified_only_select(sql);
    assert_eq!(
        &Expr::Ceil {
            expr: Box::new(Expr::Identifier(Ident::new("d"))),
            field: CeilFloorKind::DateTimeField(DateTimeField::Day),
        },
        expr_from_projection(only(&select.projection)),
    );

    one_statement_parses_to("SELECT CEIL(d to day)", "SELECT CEIL(d TO DAY)");

    verified_stmt("SELECT CEIL(d TO HOUR) FROM df");
    verified_stmt("SELECT CEIL(d TO MINUTE) FROM df");
    verified_stmt("SELECT CEIL(d TO SECOND) FROM df");
    verified_stmt("SELECT CEIL(d TO MILLISECOND) FROM df");

    let dialects = all_dialects_except(|d| d.allow_extract_custom());
    let res = dialects.parse_sql_statements("SELECT CEIL(d TO JIFFY) FROM df");
    assert_eq!(
        ParserError::ParserError("Expected: date/time field, found: JIFFY".to_string()),
        res.unwrap_err()
    );
}

#[test]
fn parse_floor_datetime() {
    let sql = "SELECT FLOOR(d TO DAY)";
    let select = verified_only_select(sql);
    assert_eq!(
        &Expr::Floor {
            expr: Box::new(Expr::Identifier(Ident::new("d"))),
            field: CeilFloorKind::DateTimeField(DateTimeField::Day),
        },
        expr_from_projection(only(&select.projection)),
    );

    one_statement_parses_to("SELECT FLOOR(d to day)", "SELECT FLOOR(d TO DAY)");

    verified_stmt("SELECT FLOOR(d TO HOUR) FROM df");
    verified_stmt("SELECT FLOOR(d TO MINUTE) FROM df");
    verified_stmt("SELECT FLOOR(d TO SECOND) FROM df");
    verified_stmt("SELECT FLOOR(d TO MILLISECOND) FROM df");

    let dialects = all_dialects_except(|d| d.allow_extract_custom());
    let res = dialects.parse_sql_statements("SELECT FLOOR(d TO JIFFY) FROM df");
    assert_eq!(
        ParserError::ParserError("Expected: date/time field, found: JIFFY".to_string()),
        res.unwrap_err()
    );
}

#[test]
fn parse_listagg() {
    let select = verified_only_select(concat!(
        "SELECT LISTAGG(DISTINCT dateid, ', ' ON OVERFLOW TRUNCATE '%' WITHOUT COUNT) ",
        "WITHIN GROUP (ORDER BY id, username)",
    ));

    assert_eq!(
        &Expr::Function(Function {
            name: ObjectName::from(vec![Ident::new("LISTAGG")]),
            uses_odbc_syntax: false,
            parameters: FunctionArguments::None,
            args: FunctionArguments::List(FunctionArgumentList {
                duplicate_treatment: Some(DuplicateTreatment::Distinct),
                args: vec![
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Identifier(Ident::new(
                        "dateid"
                    )))),
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
                        (Value::SingleQuotedString(", ".to_owned())).with_empty_span()
                    )))
                ],
                clauses: vec![FunctionArgumentClause::OnOverflow(
                    ListAggOnOverflow::Truncate {
                        filler: Some(Box::new(Expr::Value(
                            (Value::SingleQuotedString("%".to_string(),)).with_empty_span()
                        ))),
                        with_count: false,
                    }
                )],
            }),
            filter: None,
            null_treatment: None,
            over: None,
            within_group: vec![
                OrderByExpr {
                    expr: Expr::Identifier(Ident {
                        value: "id".to_string(),
                        quote_style: None,
                        span: Span::empty(),
                    }),
                    options: OrderByOptions {
                        asc: None,
                        nulls_first: None,
                    },
                    with_fill: None,
                },
                OrderByExpr {
                    expr: Expr::Identifier(Ident {
                        value: "username".to_string(),
                        quote_style: None,
                        span: Span::empty(),
                    }),
                    options: OrderByOptions {
                        asc: None,
                        nulls_first: None,
                    },
                    with_fill: None,
                },
            ]
        }),
        expr_from_projection(only(&select.projection))
    );

    verified_stmt("SELECT LISTAGG(sellerid) WITHIN GROUP (ORDER BY dateid)");
    verified_stmt("SELECT LISTAGG(dateid)");
    verified_stmt("SELECT LISTAGG(DISTINCT dateid)");
    verified_stmt("SELECT LISTAGG(dateid ON OVERFLOW ERROR)");
    verified_stmt("SELECT LISTAGG(dateid ON OVERFLOW TRUNCATE N'...' WITH COUNT)");
    verified_stmt("SELECT LISTAGG(dateid ON OVERFLOW TRUNCATE X'deadbeef' WITH COUNT)");
}

#[test]
fn parse_array_agg_func() {
    let supported_dialects = TestedDialects::new(vec![
        Box::new(GenericDialect {}),
        Box::new(DuckDbDialect {}),
        Box::new(PostgreSqlDialect {}),
        Box::new(MsSqlDialect {}),
        Box::new(AnsiDialect {}),
        Box::new(HiveDialect {}),
    ]);

    for sql in [
        "SELECT ARRAY_AGG(x ORDER BY x) AS a FROM T",
        "SELECT ARRAY_AGG(x ORDER BY x LIMIT 2) FROM tbl",
        "SELECT ARRAY_AGG(DISTINCT x ORDER BY x LIMIT 2) FROM tbl",
        "SELECT ARRAY_AGG(x ORDER BY x, y) AS a FROM T",
        "SELECT ARRAY_AGG(x ORDER BY x ASC, y DESC) AS a FROM T",
    ] {
        supported_dialects.verified_stmt(sql);
    }
}

#[test]
fn parse_agg_with_order_by() {
    let supported_dialects = TestedDialects::new(vec![
        Box::new(GenericDialect {}),
        Box::new(PostgreSqlDialect {}),
        Box::new(MsSqlDialect {}),
        Box::new(AnsiDialect {}),
        Box::new(HiveDialect {}),
    ]);

    for sql in [
        "SELECT FIRST_VALUE(x ORDER BY x) AS a FROM T",
        "SELECT FIRST_VALUE(x ORDER BY x) FROM tbl",
        "SELECT LAST_VALUE(x ORDER BY x, y) AS a FROM T",
        "SELECT LAST_VALUE(x ORDER BY x ASC, y DESC) AS a FROM T",
    ] {
        supported_dialects.verified_stmt(sql);
    }
}

#[test]
fn parse_window_rank_function() {
    let supported_dialects = TestedDialects::new(vec![
        Box::new(GenericDialect {}),
        Box::new(PostgreSqlDialect {}),
        Box::new(MsSqlDialect {}),
        Box::new(AnsiDialect {}),
        Box::new(HiveDialect {}),
        Box::new(SnowflakeDialect {}),
    ]);

    for sql in [
        "SELECT column1, column2, FIRST_VALUE(column2) OVER (PARTITION BY column1 ORDER BY column2 NULLS LAST) AS column2_first FROM t1",
        "SELECT column1, column2, FIRST_VALUE(column2) OVER (ORDER BY column2 NULLS LAST) AS column2_first FROM t1",
        "SELECT col_1, col_2, LAG(col_2) OVER (ORDER BY col_1) FROM t1",
        "SELECT LAG(col_2, 1, 0) OVER (ORDER BY col_1) FROM t1",
        "SELECT LAG(col_2, 1, 0) OVER (PARTITION BY col_3 ORDER BY col_1)",
    ] {
        supported_dialects.verified_stmt(sql);
    }

    let supported_dialects_nulls = TestedDialects::new(vec![
        Box::new(MsSqlDialect {}),
        Box::new(SnowflakeDialect {}),
    ]);

    for sql in [
        "SELECT column1, column2, FIRST_VALUE(column2) IGNORE NULLS OVER (PARTITION BY column1 ORDER BY column2 NULLS LAST) AS column2_first FROM t1",
        "SELECT column1, column2, FIRST_VALUE(column2) RESPECT NULLS OVER (PARTITION BY column1 ORDER BY column2 NULLS LAST) AS column2_first FROM t1",
        "SELECT LAG(col_2, 1, 0) IGNORE NULLS OVER (ORDER BY col_1) FROM t1",
        "SELECT LAG(col_2, 1, 0) RESPECT NULLS OVER (ORDER BY col_1) FROM t1",
    ] {
        supported_dialects_nulls.verified_stmt(sql);
    }
}

#[test]
fn parse_window_function_null_treatment_arg() {
    let dialects = all_dialects_where(|d| d.supports_window_function_null_treatment_arg());
    let sql = "SELECT \
        FIRST_VALUE(a IGNORE NULLS) OVER (), \
        FIRST_VALUE(b RESPECT NULLS) OVER () \
    FROM mytable";
    let Select { projection, .. } = dialects.verified_only_select(sql);
    for (i, (expected_expr, expected_null_treatment)) in [
        ("a", NullTreatment::IgnoreNulls),
        ("b", NullTreatment::RespectNulls),
    ]
    .into_iter()
    .enumerate()
    {
        let SelectItem::UnnamedExpr(Expr::Function(actual)) = &projection[i] else {
            unreachable!()
        };
        assert_eq!(
            ObjectName::from(vec![Ident::new("FIRST_VALUE")]),
            actual.name
        );
        let FunctionArguments::List(arg_list) = &actual.args else {
            panic!("expected argument list")
        };
        assert!({
            arg_list
                .clauses
                .iter()
                .all(|clause| !matches!(clause, FunctionArgumentClause::OrderBy(_)))
        });
        assert_eq!(1, arg_list.args.len());
        let FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Identifier(actual_expr))) =
            &arg_list.args[0]
        else {
            unreachable!()
        };
        assert_eq!(&Ident::new(expected_expr), actual_expr);
        assert_eq!(
            Some(expected_null_treatment),
            arg_list.clauses.iter().find_map(|clause| match clause {
                FunctionArgumentClause::IgnoreOrRespectNulls(nt) => Some(*nt),
                _ => None,
            })
        );
    }

    let sql = "SELECT LAG(1 IGNORE NULLS) IGNORE NULLS OVER () FROM t1";
    assert_eq!(
        dialects.parse_sql_statements(sql).unwrap_err(),
        ParserError::ParserError("Expected: end of statement, found: NULLS".to_string())
    );

    let sql = "SELECT LAG(1 IGNORE NULLS) IGNORE NULLS OVER () FROM t1";
    assert_eq!(
        all_dialects_where(|d| !d.supports_window_function_null_treatment_arg())
            .parse_sql_statements(sql)
            .unwrap_err(),
        ParserError::ParserError("Expected: ), found: IGNORE".to_string())
    );
}

#[test]
fn test_compound_expr() {
    let supported_dialects = TestedDialects::new(vec![
        Box::new(GenericDialect {}),
        Box::new(DuckDbDialect {}),
        Box::new(BigQueryDialect {}),
    ]);
    let sqls = [
        "SELECT abc[1].f1 FROM t",
        "SELECT abc[1].f1.f2 FROM t",
        "SELECT f1.abc[1] FROM t",
        "SELECT f1.f2.abc[1] FROM t",
        "SELECT f1.abc[1].f2 FROM t",
        "SELECT named_struct('a', 1, 'b', 2).a",
        "SELECT named_struct('a', 1, 'b', 2).a",
        "SELECT make_array(1, 2, 3)[1]",
        "SELECT make_array(named_struct('a', 1))[1].a",
        "SELECT abc[1][-1].a.b FROM t",
        "SELECT abc[1][-1].a.b[1] FROM t",
    ];
    for sql in sqls {
        supported_dialects.verified_stmt(sql);
    }
}

#[test]
fn test_double_value() {
    let dialects = all_dialects();
    let test_cases = vec![
        gen_number_case_with_sign("0."),
        gen_number_case_with_sign("0.0"),
        gen_number_case_with_sign("0000."),
        gen_number_case_with_sign("0000.00"),
        gen_number_case_with_sign(".0"),
        gen_number_case_with_sign(".00"),
        gen_number_case_with_sign("0e0"),
        gen_number_case_with_sign("0e+0"),
        gen_number_case_with_sign("0e-0"),
        gen_number_case_with_sign("0.e-0"),
        gen_number_case_with_sign("0.e+0"),
        gen_number_case_with_sign(".0e-0"),
        gen_number_case_with_sign(".0e+0"),
        gen_number_case_with_sign("00.0e+0"),
        gen_number_case_with_sign("00.0e-0"),
    ];

    for (input, expected) in test_cases {
        for (i, expr) in input.iter().enumerate() {
            if let Statement::Query(query) =
                dialects.one_statement_parses_to(&format!("SELECT {expr}"), "")
            {
                if let SetExpr::Select(select) = *query.body {
                    assert_eq!(expected[i], select.projection[0]);
                } else {
                    panic!("Expected a SELECT statement");
                }
            } else {
                panic!("Expected a SELECT statement");
            }
        }
    }
}

fn gen_number_case(value: &str) -> (Vec<String>, Vec<SelectItem>) {
    let input = vec![
        value.to_string(),
        format!("{} col_alias", value),
        format!("{} AS col_alias", value),
    ];
    let expected = vec![
        SelectItem::UnnamedExpr(Expr::value(number(value))),
        SelectItem::ExprWithAlias {
            expr: Expr::value(number(value)),
            alias: Ident::new("col_alias"),
        },
        SelectItem::ExprWithAlias {
            expr: Expr::value(number(value)),
            alias: Ident::new("col_alias"),
        },
    ];
    (input, expected)
}

fn gen_sign_number_case(value: &str, op: UnaryOperator) -> (Vec<String>, Vec<SelectItem>) {
    match op {
        UnaryOperator::Plus | UnaryOperator::Minus => {}
        _ => panic!("Invalid sign"),
    }

    let input = vec![
        format!("{}{}", op, value),
        format!("{}{} col_alias", op, value),
        format!("{}{} AS col_alias", op, value),
    ];
    let expected = vec![
        SelectItem::UnnamedExpr(Expr::UnaryOp {
            op,
            expr: Box::new(Expr::value(number(value))),
        }),
        SelectItem::ExprWithAlias {
            expr: Expr::UnaryOp {
                op,
                expr: Box::new(Expr::value(number(value))),
            },
            alias: Ident::new("col_alias"),
        },
        SelectItem::ExprWithAlias {
            expr: Expr::UnaryOp {
                op,
                expr: Box::new(Expr::value(number(value))),
            },
            alias: Ident::new("col_alias"),
        },
    ];
    (input, expected)
}

/// generate the test cases for signed and unsigned numbers
/// For example, given "0.0", the test cases will be:
/// - "0.0"
/// - "+0.0"
/// - "-0.0"
fn gen_number_case_with_sign(number: &str) -> (Vec<String>, Vec<SelectItem>) {
    let (mut input, mut expected) = gen_number_case(number);
    for op in [UnaryOperator::Plus, UnaryOperator::Minus] {
        let (input_sign, expected_sign) = gen_sign_number_case(number, op);
        input.extend(input_sign);
        expected.extend(expected_sign);
    }
    (input, expected)
}

#[test]
fn parse_negative_value() {
    let sql1 = "SELECT -1";
    one_statement_parses_to(sql1, "SELECT -1");

    let sql2 = "CREATE SEQUENCE name INCREMENT -10 MINVALUE -1000 MAXVALUE 15 START -100;";
    one_statement_parses_to(
        sql2,
        "CREATE SEQUENCE name INCREMENT -10 MINVALUE -1000 MAXVALUE 15 START -100",
    );
}

#[test]
fn parse_create_table() {
    let sql = "CREATE TABLE uk_cities (\
               name VARCHAR(100) NOT NULL,\
               lat DOUBLE NULL,\
               lng DOUBLE,
               constrained INT NULL CONSTRAINT pkey PRIMARY KEY NOT NULL UNIQUE CHECK (constrained > 0),
               ref INT REFERENCES othertable (a, b),\
               ref2 INT references othertable2 on delete cascade on update no action,\
               constraint fkey foreign key (lat) references othertable3 (lat) on delete restrict,\
               constraint fkey2 foreign key (lat) references othertable4(lat) on delete no action on update restrict, \
               foreign key (lat) references othertable4(lat) on update set default on delete cascade, \
               FOREIGN KEY (lng) REFERENCES othertable4 (longitude) ON UPDATE SET NULL
               )";
    let ast = one_statement_parses_to(
        sql,
        "CREATE TABLE uk_cities (\
         name VARCHAR(100) NOT NULL, \
         lat DOUBLE NULL, \
         lng DOUBLE, \
         constrained INT NULL CONSTRAINT pkey PRIMARY KEY NOT NULL UNIQUE CHECK (constrained > 0), \
         ref INT REFERENCES othertable (a, b), \
         ref2 INT REFERENCES othertable2 ON DELETE CASCADE ON UPDATE NO ACTION, \
         CONSTRAINT fkey FOREIGN KEY (lat) REFERENCES othertable3(lat) ON DELETE RESTRICT, \
         CONSTRAINT fkey2 FOREIGN KEY (lat) REFERENCES othertable4(lat) ON DELETE NO ACTION ON UPDATE RESTRICT, \
         FOREIGN KEY (lat) REFERENCES othertable4(lat) ON DELETE CASCADE ON UPDATE SET DEFAULT, \
         FOREIGN KEY (lng) REFERENCES othertable4(longitude) ON UPDATE SET NULL)",
    );
    match ast {
        Statement::CreateTable(CreateTable {
            name,
            columns,
            constraints,
            table_options,
            if_not_exists: false,
            external: false,
            file_format: None,
            location: None,
            ..
        }) => {
            assert_eq!("uk_cities", name.to_string());
            assert_eq!(
                columns,
                vec![
                    ColumnDef {
                        name: "name".into(),
                        data_type: DataType::Varchar(Some(CharacterLength::IntegerLength {
                            length: 100,
                            unit: None,
                        })),
                        options: vec![ColumnOptionDef {
                            name: None,
                            option: ColumnOption::NotNull,
                        }],
                    },
                    ColumnDef {
                        name: "lat".into(),
                        data_type: DataType::Double(ExactNumberInfo::None),
                        options: vec![ColumnOptionDef {
                            name: None,
                            option: ColumnOption::Null,
                        }],
                    },
                    ColumnDef {
                        name: "lng".into(),
                        data_type: DataType::Double(ExactNumberInfo::None),
                        options: vec![],
                    },
                    ColumnDef {
                        name: "constrained".into(),
                        data_type: DataType::Int(None),
                        options: vec![
                            ColumnOptionDef {
                                name: None,
                                option: ColumnOption::Null,
                            },
                            ColumnOptionDef {
                                name: Some("pkey".into()),
                                option: ColumnOption::Unique {
                                    is_primary: true,
                                    characteristics: None
                                },
                            },
                            ColumnOptionDef {
                                name: None,
                                option: ColumnOption::NotNull,
                            },
                            ColumnOptionDef {
                                name: None,
                                option: ColumnOption::Unique {
                                    is_primary: false,
                                    characteristics: None
                                },
                            },
                            ColumnOptionDef {
                                name: None,
                                option: ColumnOption::Check(verified_expr("constrained > 0")),
                            },
                        ],
                    },
                    ColumnDef {
                        name: "ref".into(),
                        data_type: DataType::Int(None),
                        options: vec![ColumnOptionDef {
                            name: None,
                            option: ColumnOption::ForeignKey(ForeignKeyConstraint {
                                name: None,
                                index_name: None,
                                columns: vec!["ref".into()],
                                foreign_table: ObjectName::from(vec!["othertable".into()]),
                                referred_columns: vec!["a".into(), "b".into()],
                                on_delete: None,
                                on_update: None,
                                match_kind: None,
                                characteristics: None,
                            }),
                        }],
                    },
                    ColumnDef {
                        name: "ref2".into(),
                        data_type: DataType::Int(None),
                        options: vec![ColumnOptionDef {
                            name: None,
                            option: ColumnOption::ForeignKey(ForeignKeyConstraint {
                                name: None,
                                index_name: None,
                                columns: vec!["ref2".into()],
                                foreign_table: ObjectName::from(vec!["othertable2".into()]),
                                referred_columns: vec![],
                                on_delete: Some(ReferentialAction::Cascade),
                                on_update: Some(ReferentialAction::NoAction),
                                match_kind: None,
                                characteristics: None,
                            }),
                        },],
                    },
                ]
            );
            assert_eq!(
                constraints,
                vec![
                    ForeignKeyConstraint {
                        name: Some("fkey".into()),
                        index_name: None,
                        columns: vec!["lat".into()],
                        foreign_table: ObjectName::from(vec!["othertable3".into()]),
                        referred_columns: vec!["lat".into()],
                        on_delete: Some(ReferentialAction::Restrict),
                        on_update: None,
                        match_kind: None,
                        characteristics: None,
                    }
                    .into(),
                    ForeignKeyConstraint {
                        name: Some("fkey2".into()),
                        index_name: None,
                        columns: vec!["lat".into()],
                        foreign_table: ObjectName::from(vec!["othertable4".into()]),
                        referred_columns: vec!["lat".into()],
                        on_delete: Some(ReferentialAction::NoAction),
                        on_update: Some(ReferentialAction::Restrict),
                        match_kind: None,
                        characteristics: None,
                    }
                    .into(),
                    ForeignKeyConstraint {
                        name: None,
                        index_name: None,
                        columns: vec!["lat".into()],
                        foreign_table: ObjectName::from(vec!["othertable4".into()]),
                        referred_columns: vec!["lat".into()],
                        on_delete: Some(ReferentialAction::Cascade),
                        on_update: Some(ReferentialAction::SetDefault),
                        match_kind: None,
                        characteristics: None,
                    }
                    .into(),
                    ForeignKeyConstraint {
                        name: None,
                        index_name: None,
                        columns: vec!["lng".into()],
                        foreign_table: ObjectName::from(vec!["othertable4".into()]),
                        referred_columns: vec!["longitude".into()],
                        on_delete: None,
                        on_update: Some(ReferentialAction::SetNull),
                        match_kind: None,
                        characteristics: None,
                    }
                    .into(),
                ]
            );
            assert_eq!(table_options, CreateTableOptions::None);
        }
        _ => unreachable!(),
    }

    let res = parse_sql_statements("CREATE TABLE t (a int NOT NULL GARBAGE)");
    assert!(res
        .unwrap_err()
        .to_string()
        .contains("Expected: \',\' or \')\' after column definition, found: GARBAGE"));

    let res = parse_sql_statements("CREATE TABLE t (a int NOT NULL CONSTRAINT foo)");
    assert!(res
        .unwrap_err()
        .to_string()
        .contains("Expected: constraint details after CONSTRAINT <name>"));
}

#[test]
fn parse_create_table_with_constraint_characteristics() {
    let sql = "CREATE TABLE uk_cities (\
               name VARCHAR(100) NOT NULL,\
               lat DOUBLE NULL,\
               lng DOUBLE,
               constraint fkey foreign key (lat) references othertable3 (lat) on delete restrict deferrable initially deferred,\
               constraint fkey2 foreign key (lat) references othertable4(lat) on delete no action on update restrict deferrable initially immediate, \
               foreign key (lat) references othertable4(lat) on update set default on delete cascade not deferrable initially deferred not enforced, \
               FOREIGN KEY (lng) REFERENCES othertable4 (longitude) ON UPDATE SET NULL enforced not deferrable initially immediate
               )";
    let ast = one_statement_parses_to(
        sql,
        "CREATE TABLE uk_cities (\
         name VARCHAR(100) NOT NULL, \
         lat DOUBLE NULL, \
         lng DOUBLE, \
         CONSTRAINT fkey FOREIGN KEY (lat) REFERENCES othertable3(lat) ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED, \
         CONSTRAINT fkey2 FOREIGN KEY (lat) REFERENCES othertable4(lat) ON DELETE NO ACTION ON UPDATE RESTRICT DEFERRABLE INITIALLY IMMEDIATE, \
         FOREIGN KEY (lat) REFERENCES othertable4(lat) ON DELETE CASCADE ON UPDATE SET DEFAULT NOT DEFERRABLE INITIALLY DEFERRED NOT ENFORCED, \
         FOREIGN KEY (lng) REFERENCES othertable4(longitude) ON UPDATE SET NULL NOT DEFERRABLE INITIALLY IMMEDIATE ENFORCED)",
    );
    match ast {
        Statement::CreateTable(CreateTable {
            name,
            columns,
            constraints,
            table_options,
            if_not_exists: false,
            external: false,
            file_format: None,
            location: None,
            ..
        }) => {
            assert_eq!("uk_cities", name.to_string());
            assert_eq!(
                columns,
                vec![
                    ColumnDef {
                        name: "name".into(),
                        data_type: DataType::Varchar(Some(CharacterLength::IntegerLength {
                            length: 100,
                            unit: None,
                        })),
                        options: vec![ColumnOptionDef {
                            name: None,
                            option: ColumnOption::NotNull,
                        }],
                    },
                    ColumnDef {
                        name: "lat".into(),
                        data_type: DataType::Double(ExactNumberInfo::None),
                        options: vec![ColumnOptionDef {
                            name: None,
                            option: ColumnOption::Null,
                        }],
                    },
                    ColumnDef {
                        name: "lng".into(),
                        data_type: DataType::Double(ExactNumberInfo::None),
                        options: vec![],
                    },
                ]
            );
            assert_eq!(
                constraints,
                vec![
                    ForeignKeyConstraint {
                        name: Some("fkey".into()),
                        index_name: None,
                        columns: vec!["lat".into()],
                        foreign_table: ObjectName::from(vec!["othertable3".into()]),
                        referred_columns: vec!["lat".into()],
                        on_delete: Some(ReferentialAction::Restrict),
                        on_update: None,
                        match_kind: None,
                        characteristics: Some(ConstraintCharacteristics {
                            deferrable: Some(true),
                            initially: Some(DeferrableInitial::Deferred),
                            enforced: None
                        }),
                    }
                    .into(),
                    ForeignKeyConstraint {
                        name: Some("fkey2".into()),
                        index_name: None,
                        columns: vec!["lat".into()],
                        foreign_table: ObjectName::from(vec!["othertable4".into()]),
                        referred_columns: vec!["lat".into()],
                        on_delete: Some(ReferentialAction::NoAction),
                        on_update: Some(ReferentialAction::Restrict),
                        match_kind: None,
                        characteristics: Some(ConstraintCharacteristics {
                            deferrable: Some(true),
                            initially: Some(DeferrableInitial::Immediate),
                            enforced: None,
                        }),
                    }
                    .into(),
                    ForeignKeyConstraint {
                        name: None,
                        index_name: None,
                        columns: vec!["lat".into()],
                        foreign_table: ObjectName::from(vec!["othertable4".into()]),
                        referred_columns: vec!["lat".into()],
                        on_delete: Some(ReferentialAction::Cascade),
                        on_update: Some(ReferentialAction::SetDefault),
                        match_kind: None,
                        characteristics: Some(ConstraintCharacteristics {
                            deferrable: Some(false),
                            initially: Some(DeferrableInitial::Deferred),
                            enforced: Some(false),
                        }),
                    }
                    .into(),
                    ForeignKeyConstraint {
                        name: None,
                        index_name: None,
                        columns: vec!["lng".into()],
                        foreign_table: ObjectName::from(vec!["othertable4".into()]),
                        referred_columns: vec!["longitude".into()],
                        on_delete: None,
                        on_update: Some(ReferentialAction::SetNull),
                        match_kind: None,
                        characteristics: Some(ConstraintCharacteristics {
                            deferrable: Some(false),
                            initially: Some(DeferrableInitial::Immediate),
                            enforced: Some(true),
                        }),
                    }
                    .into(),
                ]
            );
            assert_eq!(table_options, CreateTableOptions::None);
        }
        _ => unreachable!(),
    }

    let res = parse_sql_statements("CREATE TABLE t (
        a int NOT NULL,
         FOREIGN KEY (a) REFERENCES othertable4(a) ON DELETE CASCADE ON UPDATE SET DEFAULT DEFERRABLE INITIALLY IMMEDIATE NOT DEFERRABLE, \
        )");
    assert!(res
        .unwrap_err()
        .to_string()
        .contains("Expected: \',\' or \')\' after column definition, found: NOT"));

    let res = parse_sql_statements("CREATE TABLE t (
        a int NOT NULL,
         FOREIGN KEY (a) REFERENCES othertable4(a) ON DELETE CASCADE ON UPDATE SET DEFAULT NOT ENFORCED INITIALLY DEFERRED ENFORCED, \
        )");
    assert!(res
        .unwrap_err()
        .to_string()
        .contains("Expected: \',\' or \')\' after column definition, found: ENFORCED"));

    let res = parse_sql_statements("CREATE TABLE t (
        a int NOT NULL,
         FOREIGN KEY (lat) REFERENCES othertable4(lat) ON DELETE CASCADE ON UPDATE SET DEFAULT INITIALLY DEFERRED INITIALLY IMMEDIATE, \
        )");
    assert!(res
        .unwrap_err()
        .to_string()
        .contains("Expected: \',\' or \')\' after column definition, found: INITIALLY"));
}

#[test]
fn parse_create_table_column_constraint_characteristics() {
    fn test_combo(
        syntax: &str,
        deferrable: Option<bool>,
        initially: Option<DeferrableInitial>,
        enforced: Option<bool>,
    ) {
        let message = if syntax.is_empty() {
            "No clause"
        } else {
            syntax
        };

        let sql = format!("CREATE TABLE t (a int UNIQUE {syntax})");
        let expected_clause = if syntax.is_empty() {
            String::new()
        } else {
            format!(" {syntax}")
        };
        let expected = format!("CREATE TABLE t (a INT UNIQUE{expected_clause})");
        let ast = one_statement_parses_to(&sql, &expected);

        let expected_value = if deferrable.is_some() || initially.is_some() || enforced.is_some() {
            Some(ConstraintCharacteristics {
                deferrable,
                initially,
                enforced,
            })
        } else {
            None
        };

        match ast {
            Statement::CreateTable(CreateTable { columns, .. }) => {
                assert_eq!(
                    columns,
                    vec![ColumnDef {
                        name: "a".into(),
                        data_type: DataType::Int(None),
                        options: vec![ColumnOptionDef {
                            name: None,
                            option: ColumnOption::Unique {
                                is_primary: false,
                                characteristics: expected_value
                            }
                        }]
                    }],
                    "{message}"
                )
            }
            _ => unreachable!(),
        }
    }

    for deferrable in [None, Some(true), Some(false)] {
        for initially in [
            None,
            Some(DeferrableInitial::Immediate),
            Some(DeferrableInitial::Deferred),
        ] {
            for enforced in [None, Some(true), Some(false)] {
                let deferrable_text =
                    deferrable.map(|d| if d { "DEFERRABLE" } else { "NOT DEFERRABLE" });
                let initially_text = initially.map(|i| match i {
                    DeferrableInitial::Immediate => "INITIALLY IMMEDIATE",
                    DeferrableInitial::Deferred => "INITIALLY DEFERRED",
                });
                let enforced_text = enforced.map(|e| if e { "ENFORCED" } else { "NOT ENFORCED" });

                let syntax = [deferrable_text, initially_text, enforced_text]
                    .into_iter()
                    .flatten()
                    .collect::<Vec<_>>()
                    .join(" ");

                test_combo(&syntax, deferrable, initially, enforced);
            }
        }
    }

    let res = parse_sql_statements(
        "CREATE TABLE t (a int NOT NULL UNIQUE DEFERRABLE INITIALLY BADVALUE)",
    );
    assert!(res
        .unwrap_err()
        .to_string()
        .contains("Expected: one of DEFERRED or IMMEDIATE, found: BADVALUE"));

    let res = parse_sql_statements(
        "CREATE TABLE t (a int NOT NULL UNIQUE INITIALLY IMMEDIATE DEFERRABLE INITIALLY DEFERRED)",
    );
    res.expect_err("INITIALLY {IMMEDIATE|DEFERRED} setting should only be allowed once");

    let res = parse_sql_statements(
        "CREATE TABLE t (a int NOT NULL UNIQUE DEFERRABLE INITIALLY DEFERRED NOT DEFERRABLE)",
    );
    res.expect_err("[NOT] DEFERRABLE setting should only be allowed once");

    let res = parse_sql_statements(
        "CREATE TABLE t (a int NOT NULL UNIQUE DEFERRABLE INITIALLY DEFERRED ENFORCED NOT ENFORCED)",
    );
    res.expect_err("[NOT] ENFORCED setting should only be allowed once");
}

#[test]
fn parse_create_table_hive_array() {
    // Parsing [] type arrays does not work in MsSql since [ is used in is_delimited_identifier_start
    for (dialects, angle_bracket_syntax) in [
        (
            vec![Box::new(PostgreSqlDialect {}) as Box<dyn Dialect>],
            false,
        ),
        (
            vec![
                Box::new(HiveDialect {}) as Box<dyn Dialect>,
                Box::new(BigQueryDialect {}) as Box<dyn Dialect>,
            ],
            true,
        ),
    ] {
        let dialects = TestedDialects::new(dialects);

        let sql = format!(
            "CREATE TABLE IF NOT EXISTS something (name INT, val {})",
            if angle_bracket_syntax {
                "ARRAY<INT>"
            } else {
                "INT[]"
            }
        );

        let expected = Box::new(DataType::Int(None));
        let expected = if angle_bracket_syntax {
            ArrayElemTypeDef::AngleBracket(expected)
        } else {
            ArrayElemTypeDef::SquareBracket(expected, None)
        };

        match dialects.one_statement_parses_to(sql.as_str(), sql.as_str()) {
            Statement::CreateTable(CreateTable {
                if_not_exists,
                name,
                columns,
                ..
            }) => {
                assert!(if_not_exists);
                assert_eq!(name, ObjectName::from(vec!["something".into()]));
                assert_eq!(
                    columns,
                    vec![
                        ColumnDef {
                            name: Ident::new("name"),
                            data_type: DataType::Int(None),
                            options: vec![],
                        },
                        ColumnDef {
                            name: Ident::new("val"),
                            data_type: DataType::Array(expected),
                            options: vec![],
                        },
                    ],
                )
            }
            _ => unreachable!(),
        }
    }

    // SnowflakeDialect using array different
    let dialects = TestedDialects::new(vec![
        Box::new(PostgreSqlDialect {}),
        Box::new(HiveDialect {}),
        Box::new(MySqlDialect {}),
    ]);
    let sql = "CREATE TABLE IF NOT EXISTS something (name int, val array<int)";

    assert_eq!(
        dialects.parse_sql_statements(sql).unwrap_err(),
        ParserError::ParserError("Expected: >, found: )".to_string())
    );
}

#[test]
fn parse_create_table_with_multiple_on_delete_in_constraint_fails() {
    parse_sql_statements(
        "\
        create table X (\
            y_id int, \
            foreign key (y_id) references Y (id) on delete cascade on update cascade on delete no action\
        )",
    )
        .expect_err("should have failed");
}

#[test]
fn parse_create_table_with_multiple_on_delete_fails() {
    parse_sql_statements(
        "\
        create table X (\
            y_id int references Y (id) \
            on delete cascade on update cascade on delete no action\
        )",
    )
    .expect_err("should have failed");
}

#[test]
fn parse_assert() {
    let sql = "ASSERT (SELECT COUNT(*) FROM my_table) > 0";
    let ast = one_statement_parses_to(sql, "ASSERT (SELECT COUNT(*) FROM my_table) > 0");
    match ast {
        Statement::Assert {
            condition: _condition,
            message,
        } => {
            assert_eq!(message, None);
        }
        _ => unreachable!(),
    }
}

#[test]
#[allow(clippy::collapsible_match)]
fn parse_assert_message() {
    let sql = "ASSERT (SELECT COUNT(*) FROM my_table) > 0 AS 'No rows in my_table'";
    let ast = one_statement_parses_to(
        sql,
        "ASSERT (SELECT COUNT(*) FROM my_table) > 0 AS 'No rows in my_table'",
    );
    match ast {
        Statement::Assert {
            condition: _condition,
            message: Some(message),
        } => {
            match message {
                Expr::Value(ValueWithSpan {
                    value: Value::SingleQuotedString(s),
                    span: _,
                }) => assert_eq!(s, "No rows in my_table"),
                _ => unreachable!(),
            };
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_schema() {
    let sql = "CREATE SCHEMA X";

    match verified_stmt(sql) {
        Statement::CreateSchema { schema_name, .. } => {
            assert_eq!(schema_name.to_string(), "X".to_owned())
        }
        _ => unreachable!(),
    }

    verified_stmt(r#"CREATE SCHEMA a.b.c OPTIONS(key1 = 'value1', key2 = 'value2')"#);
    verified_stmt(r#"CREATE SCHEMA IF NOT EXISTS a OPTIONS(key1 = 'value1')"#);
    verified_stmt(r#"CREATE SCHEMA IF NOT EXISTS a OPTIONS()"#);
    verified_stmt(r#"CREATE SCHEMA IF NOT EXISTS a DEFAULT COLLATE 'und:ci' OPTIONS()"#);
    verified_stmt(r#"CREATE SCHEMA a.b.c WITH (key1 = 'value1', key2 = 'value2')"#);
    verified_stmt(r#"CREATE SCHEMA IF NOT EXISTS a WITH (key1 = 'value1')"#);
    verified_stmt(r#"CREATE SCHEMA IF NOT EXISTS a WITH ()"#);
    verified_stmt(r#"CREATE SCHEMA a CLONE b"#);
}

#[test]
fn parse_create_schema_with_authorization() {
    let sql = "CREATE SCHEMA AUTHORIZATION Y";

    match verified_stmt(sql) {
        Statement::CreateSchema { schema_name, .. } => {
            assert_eq!(schema_name.to_string(), "AUTHORIZATION Y".to_owned())
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_schema_with_name_and_authorization() {
    let sql = "CREATE SCHEMA X AUTHORIZATION Y";

    match verified_stmt(sql) {
        Statement::CreateSchema { schema_name, .. } => {
            assert_eq!(schema_name.to_string(), "X AUTHORIZATION Y".to_owned())
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_drop_schema() {
    let sql = "DROP SCHEMA X";

    match verified_stmt(sql) {
        Statement::Drop { object_type, .. } => assert_eq!(object_type, ObjectType::Schema),
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_table_as() {
    let sql = "CREATE TABLE t AS SELECT * FROM a";

    match verified_stmt(sql) {
        Statement::CreateTable(CreateTable { name, query, .. }) => {
            assert_eq!(name.to_string(), "t".to_string());
            assert_eq!(query, Some(Box::new(verified_query("SELECT * FROM a"))));
        }
        _ => unreachable!(),
    }

    // BigQuery allows specifying table schema in CTAS
    // ANSI SQL and PostgreSQL let you only specify the list of columns
    // (without data types) in a CTAS, but we have yet to support that.
    let dialects = all_dialects_where(|d| d.supports_create_table_multi_schema_info_sources());
    let sql = "CREATE TABLE t (a INT, b INT) AS SELECT 1 AS b, 2 AS a";
    match dialects.verified_stmt(sql) {
        Statement::CreateTable(CreateTable { columns, query, .. }) => {
            assert_eq!(columns.len(), 2);
            assert_eq!(columns[0].to_string(), "a INT".to_string());
            assert_eq!(columns[1].to_string(), "b INT".to_string());
            assert_eq!(
                query,
                Some(Box::new(verified_query("SELECT 1 AS b, 2 AS a")))
            );
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_table_as_table() {
    let sql1 = "CREATE TABLE new_table AS TABLE old_table";

    let expected_query1 = Box::new(Query {
        with: None,
        body: Box::new(SetExpr::Table(Box::new(Table {
            table_name: Some("old_table".to_string()),
            schema_name: None,
        }))),
        order_by: None,
        limit_clause: None,
        fetch: None,
        locks: vec![],
        for_clause: None,
        settings: None,
        format_clause: None,
        pipe_operators: vec![],
    });

    match verified_stmt(sql1) {
        Statement::CreateTable(CreateTable { query, name, .. }) => {
            assert_eq!(name, ObjectName::from(vec![Ident::new("new_table")]));
            assert_eq!(query.unwrap(), expected_query1);
        }
        _ => unreachable!(),
    }

    let sql2 = "CREATE TABLE new_table AS TABLE schema_name.old_table";

    let expected_query2 = Box::new(Query {
        with: None,
        body: Box::new(SetExpr::Table(Box::new(Table {
            table_name: Some("old_table".to_string()),
            schema_name: Some("schema_name".to_string()),
        }))),
        order_by: None,
        limit_clause: None,
        fetch: None,
        locks: vec![],
        for_clause: None,
        settings: None,
        format_clause: None,
        pipe_operators: vec![],
    });

    match verified_stmt(sql2) {
        Statement::CreateTable(CreateTable { query, name, .. }) => {
            assert_eq!(name, ObjectName::from(vec![Ident::new("new_table")]));
            assert_eq!(query.unwrap(), expected_query2);
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_table_on_cluster() {
    let generic = TestedDialects::new(vec![Box::new(GenericDialect {})]);

    // Using single-quote literal to define current cluster
    let sql = "CREATE TABLE t ON CLUSTER '{cluster}' (a INT, b INT)";
    match generic.verified_stmt(sql) {
        Statement::CreateTable(CreateTable { on_cluster, .. }) => {
            assert_eq!(on_cluster.unwrap().to_string(), "'{cluster}'".to_string());
        }
        _ => unreachable!(),
    }

    // Using explicitly declared cluster name
    let sql = "CREATE TABLE t ON CLUSTER my_cluster (a INT, b INT)";
    match generic.verified_stmt(sql) {
        Statement::CreateTable(CreateTable { on_cluster, .. }) => {
            assert_eq!(on_cluster.unwrap().to_string(), "my_cluster".to_string());
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_or_replace_table() {
    let sql = "CREATE OR REPLACE TABLE t (a INT)";

    match verified_stmt(sql) {
        Statement::CreateTable(CreateTable {
            name, or_replace, ..
        }) => {
            assert_eq!(name.to_string(), "t".to_string());
            assert!(or_replace);
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_table_with_on_delete_on_update_2in_any_order() -> Result<(), ParserError> {
    let sql = |options: &str| -> String {
        format!("create table X (y_id int references Y (id) {options})")
    };

    parse_sql_statements(&sql("on update cascade on delete no action"))?;
    parse_sql_statements(&sql("on delete cascade on update cascade"))?;
    parse_sql_statements(&sql("on update no action"))?;
    parse_sql_statements(&sql("on delete restrict"))?;

    Ok(())
}

#[test]
fn parse_create_table_with_options() {
    let generic = TestedDialects::new(vec![Box::new(GenericDialect {})]);

    let sql = "CREATE TABLE t (c INT) WITH (foo = 'bar', a = 123)";
    match generic.verified_stmt(sql) {
        Statement::CreateTable(CreateTable { table_options, .. }) => {
            let with_options = match table_options {
                CreateTableOptions::With(options) => options,
                _ => unreachable!(),
            };
            assert_eq!(
                vec![
                    SqlOption::KeyValue {
                        key: "foo".into(),
                        value: Expr::Value(
                            (Value::SingleQuotedString("bar".into())).with_empty_span()
                        ),
                    },
                    SqlOption::KeyValue {
                        key: "a".into(),
                        value: Expr::value(number("123")),
                    },
                ],
                with_options
            );
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_table_clone() {
    let sql = "CREATE OR REPLACE TABLE a CLONE a_tmp";
    match verified_stmt(sql) {
        Statement::CreateTable(CreateTable { name, clone, .. }) => {
            assert_eq!(ObjectName::from(vec![Ident::new("a")]), name);
            assert_eq!(Some(ObjectName::from(vec![(Ident::new("a_tmp"))])), clone)
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_table_trailing_comma() {
    let dialect = TestedDialects::new(vec![Box::new(DuckDbDialect {})]);

    let sql = "CREATE TABLE foo (bar int,);";
    dialect.one_statement_parses_to(sql, "CREATE TABLE foo (bar INT)");
}

#[test]
fn parse_create_external_table() {
    let sql = "CREATE EXTERNAL TABLE uk_cities (\
               name VARCHAR(100) NOT NULL,\
               lat DOUBLE NULL,\
               lng DOUBLE)\
               STORED AS TEXTFILE LOCATION '/tmp/example.csv'";
    let ast = one_statement_parses_to(
        sql,
        "CREATE EXTERNAL TABLE uk_cities (\
         name VARCHAR(100) NOT NULL, \
         lat DOUBLE NULL, \
         lng DOUBLE) \
         STORED AS TEXTFILE LOCATION '/tmp/example.csv'",
    );
    match ast {
        Statement::CreateTable(CreateTable {
            name,
            columns,
            constraints,
            table_options,
            if_not_exists,
            external,
            file_format,
            location,
            ..
        }) => {
            assert_eq!("uk_cities", name.to_string());
            assert_eq!(
                columns,
                vec![
                    ColumnDef {
                        name: "name".into(),
                        data_type: DataType::Varchar(Some(CharacterLength::IntegerLength {
                            length: 100,
                            unit: None,
                        })),
                        options: vec![ColumnOptionDef {
                            name: None,
                            option: ColumnOption::NotNull,
                        }],
                    },
                    ColumnDef {
                        name: "lat".into(),
                        data_type: DataType::Double(ExactNumberInfo::None),
                        options: vec![ColumnOptionDef {
                            name: None,
                            option: ColumnOption::Null,
                        }],
                    },
                    ColumnDef {
                        name: "lng".into(),
                        data_type: DataType::Double(ExactNumberInfo::None),
                        options: vec![],
                    },
                ]
            );
            assert!(constraints.is_empty());

            assert!(external);
            assert_eq!(FileFormat::TEXTFILE, file_format.unwrap());
            assert_eq!("/tmp/example.csv", location.unwrap());

            assert_eq!(table_options, CreateTableOptions::None);
            assert!(!if_not_exists);
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_or_replace_external_table() {
    // Supported by at least Snowflake
    // https://docs.snowflake.com/en/sql-reference/sql/create-external-table.html
    let sql = "CREATE OR REPLACE EXTERNAL TABLE uk_cities (\
               name VARCHAR(100) NOT NULL)\
               STORED AS TEXTFILE LOCATION '/tmp/example.csv'";
    let ast = one_statement_parses_to(
        sql,
        "CREATE OR REPLACE EXTERNAL TABLE uk_cities (\
         name VARCHAR(100) NOT NULL) \
         STORED AS TEXTFILE LOCATION '/tmp/example.csv'",
    );
    match ast {
        Statement::CreateTable(CreateTable {
            name,
            columns,
            constraints,
            table_options,
            if_not_exists,
            external,
            file_format,
            location,
            or_replace,
            ..
        }) => {
            assert_eq!("uk_cities", name.to_string());
            assert_eq!(
                columns,
                vec![ColumnDef {
                    name: "name".into(),
                    data_type: DataType::Varchar(Some(CharacterLength::IntegerLength {
                        length: 100,
                        unit: None,
                    })),
                    options: vec![ColumnOptionDef {
                        name: None,
                        option: ColumnOption::NotNull,
                    }],
                },]
            );
            assert!(constraints.is_empty());

            assert!(external);
            assert_eq!(FileFormat::TEXTFILE, file_format.unwrap());
            assert_eq!("/tmp/example.csv", location.unwrap());

            assert_eq!(table_options, CreateTableOptions::None);
            assert!(!if_not_exists);
            assert!(or_replace);
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_external_table_lowercase() {
    let sql = "create external table uk_cities (\
               name varchar(100) not null,\
               lat double null,\
               lng double)\
               stored as parquet location '/tmp/example.csv'";
    let ast = one_statement_parses_to(
        sql,
        "CREATE EXTERNAL TABLE uk_cities (\
         name VARCHAR(100) NOT NULL, \
         lat DOUBLE NULL, \
         lng DOUBLE) \
         STORED AS PARQUET LOCATION '/tmp/example.csv'",
    );
    assert_matches!(ast, Statement::CreateTable(CreateTable { .. }));
}

#[test]
fn parse_alter_table() {
    let add_column = "ALTER TABLE tab ADD COLUMN foo TEXT;";
    match alter_table_op(one_statement_parses_to(
        add_column,
        "ALTER TABLE tab ADD COLUMN foo TEXT",
    )) {
        AlterTableOperation::AddColumn {
            column_keyword,
            if_not_exists,
            column_def,
            column_position,
        } => {
            assert!(column_keyword);
            assert!(!if_not_exists);
            assert_eq!("foo", column_def.name.to_string());
            assert_eq!("TEXT", column_def.data_type.to_string());
            assert_eq!(None, column_position);
        }
        _ => unreachable!(),
    };

    let rename_table = "ALTER TABLE tab RENAME TO new_tab";
    match alter_table_op(verified_stmt(rename_table)) {
        AlterTableOperation::RenameTable { table_name } => {
            assert_eq!(
                RenameTableNameKind::To(ObjectName::from(vec![Ident::new("new_tab")])),
                table_name
            );
        }
        _ => unreachable!(),
    };

    let rename_table_as = "ALTER TABLE tab RENAME AS new_tab";
    match alter_table_op(verified_stmt(rename_table_as)) {
        AlterTableOperation::RenameTable { table_name } => {
            assert_eq!(
                RenameTableNameKind::As(ObjectName::from(vec![Ident::new("new_tab")])),
                table_name
            );
        }
        _ => unreachable!(),
    };

    let rename_column = "ALTER TABLE tab RENAME COLUMN foo TO new_foo";
    match alter_table_op(verified_stmt(rename_column)) {
        AlterTableOperation::RenameColumn {
            old_column_name,
            new_column_name,
        } => {
            assert_eq!(old_column_name.to_string(), "foo");
            assert_eq!(new_column_name.to_string(), "new_foo");
        }
        _ => unreachable!(),
    }

    let set_table_properties = "ALTER TABLE tab SET TBLPROPERTIES('classification' = 'parquet')";
    match alter_table_op(verified_stmt(set_table_properties)) {
        AlterTableOperation::SetTblProperties { table_properties } => {
            assert_eq!(
                table_properties,
                [SqlOption::KeyValue {
                    key: Ident {
                        value: "classification".to_string(),
                        quote_style: Some('\''),
                        span: Span::empty(),
                    },
                    value: Expr::Value(
                        (Value::SingleQuotedString("parquet".to_string())).with_empty_span()
                    ),
                }],
            );
        }
        _ => unreachable!(),
    }

    let set_storage_parameters = "ALTER TABLE tab SET (autovacuum_vacuum_scale_factor = 0.01, autovacuum_vacuum_threshold = 500)";
    match alter_table_op(verified_stmt(set_storage_parameters)) {
        AlterTableOperation::SetOptionsParens { options } => {
            assert_eq!(
                options,
                [
                    SqlOption::KeyValue {
                        key: Ident {
                            value: "autovacuum_vacuum_scale_factor".to_string(),
                            quote_style: None,
                            span: Span::empty(),
                        },
                        value: Expr::Value(test_utils::number("0.01").with_empty_span()),
                    },
                    SqlOption::KeyValue {
                        key: Ident {
                            value: "autovacuum_vacuum_threshold".to_string(),
                            quote_style: None,
                            span: Span::empty(),
                        },
                        value: Expr::Value(test_utils::number("500").with_empty_span()),
                    }
                ],
            );
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_rename_table() {
    match verified_stmt("RENAME TABLE test.test1 TO test_db.test2") {
        Statement::RenameTable(rename_tables) => {
            assert_eq!(
                vec![RenameTable {
                    old_name: ObjectName::from(vec![
                        Ident::new("test".to_string()),
                        Ident::new("test1".to_string()),
                    ]),
                    new_name: ObjectName::from(vec![
                        Ident::new("test_db".to_string()),
                        Ident::new("test2".to_string()),
                    ]),
                }],
                rename_tables
            );
        }
        _ => unreachable!(),
    };

    match verified_stmt(
        "RENAME TABLE old_table1 TO new_table1, old_table2 TO new_table2, old_table3 TO new_table3",
    ) {
        Statement::RenameTable(rename_tables) => {
            assert_eq!(
                vec![
                    RenameTable {
                        old_name: ObjectName::from(vec![Ident::new("old_table1".to_string())]),
                        new_name: ObjectName::from(vec![Ident::new("new_table1".to_string())]),
                    },
                    RenameTable {
                        old_name: ObjectName::from(vec![Ident::new("old_table2".to_string())]),
                        new_name: ObjectName::from(vec![Ident::new("new_table2".to_string())]),
                    },
                    RenameTable {
                        old_name: ObjectName::from(vec![Ident::new("old_table3".to_string())]),
                        new_name: ObjectName::from(vec![Ident::new("new_table3".to_string())]),
                    }
                ],
                rename_tables
            );
        }
        _ => unreachable!(),
    };

    assert_eq!(
        parse_sql_statements("RENAME TABLE old_table TO new_table a").unwrap_err(),
        ParserError::ParserError("Expected: end of statement, found: a".to_string())
    );

    assert_eq!(
        parse_sql_statements("RENAME TABLE1 old_table TO new_table a").unwrap_err(),
        ParserError::ParserError(
            "Expected: KEYWORD `TABLE` after RENAME, found: TABLE1".to_string()
        )
    );
}

#[test]
fn test_alter_table_with_on_cluster() {
    match all_dialects()
        .verified_stmt("ALTER TABLE t ON CLUSTER 'cluster' ADD CONSTRAINT bar PRIMARY KEY (baz)")
    {
        Statement::AlterTable(AlterTable {
            name, on_cluster, ..
        }) => {
            assert_eq!(name.to_string(), "t");
            assert_eq!(on_cluster, Some(Ident::with_quote('\'', "cluster")));
        }
        _ => unreachable!(),
    }

    match all_dialects()
        .verified_stmt("ALTER TABLE t ON CLUSTER cluster_name ADD CONSTRAINT bar PRIMARY KEY (baz)")
    {
        Statement::AlterTable(AlterTable {
            name, on_cluster, ..
        }) => {
            assert_eq!(name.to_string(), "t");
            assert_eq!(on_cluster, Some(Ident::new("cluster_name")));
        }
        _ => unreachable!(),
    }

    let res = all_dialects()
        .parse_sql_statements("ALTER TABLE t ON CLUSTER 123 ADD CONSTRAINT bar PRIMARY KEY (baz)");
    assert_eq!(
        res.unwrap_err(),
        ParserError::ParserError("Expected: identifier, found: 123".to_string())
    )
}

#[test]
fn parse_alter_index() {
    let rename_index = "ALTER INDEX idx RENAME TO new_idx";
    match verified_stmt(rename_index) {
        Statement::AlterIndex {
            name,
            operation: AlterIndexOperation::RenameIndex { index_name },
        } => {
            assert_eq!("idx", name.to_string());
            assert_eq!("new_idx", index_name.to_string())
        }
        _ => unreachable!(),
    };
}

#[test]
fn parse_alter_view() {
    let sql = "ALTER VIEW myschema.myview AS SELECT foo FROM bar";
    match verified_stmt(sql) {
        Statement::AlterView {
            name,
            columns,
            query,
            with_options,
        } => {
            assert_eq!("myschema.myview", name.to_string());
            assert_eq!(Vec::<Ident>::new(), columns);
            assert_eq!("SELECT foo FROM bar", query.to_string());
            assert_eq!(with_options, vec![]);
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_alter_view_with_options() {
    let sql = "ALTER VIEW v WITH (foo = 'bar', a = 123) AS SELECT 1";
    match verified_stmt(sql) {
        Statement::AlterView { with_options, .. } => {
            assert_eq!(
                vec![
                    SqlOption::KeyValue {
                        key: "foo".into(),
                        value: Expr::Value(
                            (Value::SingleQuotedString("bar".into())).with_empty_span()
                        ),
                    },
                    SqlOption::KeyValue {
                        key: "a".into(),
                        value: Expr::value(number("123")),
                    },
                ],
                with_options
            );
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_alter_view_with_columns() {
    let sql = "ALTER VIEW v (has, cols) AS SELECT 1, 2";
    match verified_stmt(sql) {
        Statement::AlterView {
            name,
            columns,
            query,
            with_options,
        } => {
            assert_eq!("v", name.to_string());
            assert_eq!(columns, vec![Ident::new("has"), Ident::new("cols")]);
            assert_eq!("SELECT 1, 2", query.to_string());
            assert_eq!(with_options, vec![]);
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_alter_table_add_column() {
    match alter_table_op(verified_stmt("ALTER TABLE tab ADD foo TEXT")) {
        AlterTableOperation::AddColumn { column_keyword, .. } => {
            assert!(!column_keyword);
        }
        _ => unreachable!(),
    };

    match alter_table_op(verified_stmt("ALTER TABLE tab ADD COLUMN foo TEXT")) {
        AlterTableOperation::AddColumn { column_keyword, .. } => {
            assert!(column_keyword);
        }
        _ => unreachable!(),
    };
}

#[test]
fn parse_alter_table_add_column_if_not_exists() {
    let dialects = TestedDialects::new(vec![
        Box::new(PostgreSqlDialect {}),
        Box::new(BigQueryDialect {}),
        Box::new(GenericDialect {}),
        Box::new(DuckDbDialect {}),
    ]);

    match alter_table_op(dialects.verified_stmt("ALTER TABLE tab ADD IF NOT EXISTS foo TEXT")) {
        AlterTableOperation::AddColumn { if_not_exists, .. } => {
            assert!(if_not_exists);
        }
        _ => unreachable!(),
    };

    match alter_table_op(
        dialects.verified_stmt("ALTER TABLE tab ADD COLUMN IF NOT EXISTS foo TEXT"),
    ) {
        AlterTableOperation::AddColumn {
            column_keyword,
            if_not_exists,
            ..
        } => {
            assert!(column_keyword);
            assert!(if_not_exists);
        }
        _ => unreachable!(),
    };
}

#[test]
fn parse_alter_table_constraints() {
    check_one("CONSTRAINT address_pkey PRIMARY KEY (address_id)");
    check_one("CONSTRAINT uk_task UNIQUE (report_date, task_id)");
    check_one(
        "CONSTRAINT customer_address_id_fkey FOREIGN KEY (address_id) \
         REFERENCES public.address(address_id)",
    );
    check_one("CONSTRAINT ck CHECK (rtrim(ltrim(REF_CODE)) <> '')");

    check_one("PRIMARY KEY (foo, bar)");
    check_one("UNIQUE (id)");
    check_one("FOREIGN KEY (foo, bar) REFERENCES AnotherTable(foo, bar)");
    check_one("CHECK (end_date > start_date OR end_date IS NULL)");
    check_one("CONSTRAINT fk FOREIGN KEY (lng) REFERENCES othertable4");

    fn check_one(constraint_text: &str) {
        match alter_table_op(verified_stmt(&format!(
            "ALTER TABLE tab ADD {constraint_text}"
        ))) {
            AlterTableOperation::AddConstraint { constraint, .. } => {
                assert_eq!(constraint_text, constraint.to_string());
            }
            _ => unreachable!(),
        }
        verified_stmt(&format!("CREATE TABLE foo (id INT, {constraint_text})"));
    }
}

#[test]
fn parse_alter_table_drop_column() {
    check_one("DROP COLUMN IF EXISTS is_active");
    check_one("DROP COLUMN IF EXISTS is_active CASCADE");
    check_one("DROP COLUMN IF EXISTS is_active RESTRICT");
    one_statement_parses_to(
        "ALTER TABLE tab DROP COLUMN IF EXISTS is_active CASCADE",
        "ALTER TABLE tab DROP COLUMN IF EXISTS is_active CASCADE",
    );
    one_statement_parses_to(
        "ALTER TABLE tab DROP is_active CASCADE",
        "ALTER TABLE tab DROP is_active CASCADE",
    );

    let dialects = all_dialects_where(|d| d.supports_comma_separated_drop_column_list());
    dialects.verified_stmt("ALTER TABLE tbl DROP COLUMN c1, c2, c3");

    fn check_one(constraint_text: &str) {
        match alter_table_op(verified_stmt(&format!("ALTER TABLE tab {constraint_text}"))) {
            AlterTableOperation::DropColumn {
                has_column_keyword: true,
                column_names,
                if_exists,
                drop_behavior,
            } => {
                assert_eq!("is_active", column_names.first().unwrap().to_string());
                assert!(if_exists);
                match drop_behavior {
                    None => assert!(constraint_text.ends_with(" is_active")),
                    Some(DropBehavior::Restrict) => assert!(constraint_text.ends_with(" RESTRICT")),
                    Some(DropBehavior::Cascade) => assert!(constraint_text.ends_with(" CASCADE")),
                }
            }
            _ => unreachable!(),
        }
    }
}

#[test]
fn parse_alter_table_alter_column() {
    let alter_stmt = "ALTER TABLE tab";
    match alter_table_op(verified_stmt(&format!(
        "{alter_stmt} ALTER COLUMN is_active SET NOT NULL"
    ))) {
        AlterTableOperation::AlterColumn { column_name, op } => {
            assert_eq!("is_active", column_name.to_string());
            assert_eq!(op, AlterColumnOperation::SetNotNull {});
        }
        _ => unreachable!(),
    }

    one_statement_parses_to(
        "ALTER TABLE tab ALTER is_active DROP NOT NULL",
        "ALTER TABLE tab ALTER COLUMN is_active DROP NOT NULL",
    );

    match alter_table_op(verified_stmt(&format!(
        "{alter_stmt} ALTER COLUMN is_active SET DEFAULT 0"
    ))) {
        AlterTableOperation::AlterColumn { column_name, op } => {
            assert_eq!("is_active", column_name.to_string());
            assert_eq!(
                op,
                AlterColumnOperation::SetDefault {
                    value: Expr::Value((test_utils::number("0")).with_empty_span())
                }
            );
        }
        _ => unreachable!(),
    }

    match alter_table_op(verified_stmt(&format!(
        "{alter_stmt} ALTER COLUMN is_active DROP DEFAULT"
    ))) {
        AlterTableOperation::AlterColumn { column_name, op } => {
            assert_eq!("is_active", column_name.to_string());
            assert_eq!(op, AlterColumnOperation::DropDefault {});
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_alter_table_alter_column_type() {
    let alter_stmt = "ALTER TABLE tab";
    match alter_table_op(verified_stmt(
        "ALTER TABLE tab ALTER COLUMN is_active SET DATA TYPE TEXT",
    )) {
        AlterTableOperation::AlterColumn { column_name, op } => {
            assert_eq!("is_active", column_name.to_string());
            assert_eq!(
                op,
                AlterColumnOperation::SetDataType {
                    data_type: DataType::Text,
                    using: None,
                    had_set: true,
                }
            );
        }
        _ => unreachable!(),
    }
    verified_stmt(&format!("{alter_stmt} ALTER COLUMN is_active TYPE TEXT"));

    let dialects = all_dialects_where(|d| d.supports_alter_column_type_using());
    dialects.verified_stmt(&format!(
        "{alter_stmt} ALTER COLUMN is_active SET DATA TYPE TEXT USING 'text'"
    ));

    let dialects = all_dialects_except(|d| d.supports_alter_column_type_using());
    let res = dialects.parse_sql_statements(&format!(
        "{alter_stmt} ALTER COLUMN is_active SET DATA TYPE TEXT USING 'text'"
    ));
    assert_eq!(
        ParserError::ParserError("Expected: end of statement, found: USING".to_string()),
        res.unwrap_err()
    );
}

#[test]
fn parse_alter_table_drop_constraint() {
    check_one("DROP CONSTRAINT IF EXISTS constraint_name");
    check_one("DROP CONSTRAINT IF EXISTS constraint_name RESTRICT");
    check_one("DROP CONSTRAINT IF EXISTS constraint_name CASCADE");
    fn check_one(constraint_text: &str) {
        match alter_table_op(verified_stmt(&format!("ALTER TABLE tab {constraint_text}"))) {
            AlterTableOperation::DropConstraint {
                name: constr_name,
                if_exists,
                drop_behavior,
            } => {
                assert_eq!("constraint_name", constr_name.to_string());
                assert!(if_exists);
                match drop_behavior {
                    None => assert!(constraint_text.ends_with(" constraint_name")),
                    Some(DropBehavior::Restrict) => assert!(constraint_text.ends_with(" RESTRICT")),
                    Some(DropBehavior::Cascade) => assert!(constraint_text.ends_with(" CASCADE")),
                }
            }
            _ => unreachable!(),
        }
    }

    let res = parse_sql_statements("ALTER TABLE tab DROP CONSTRAINT is_active TEXT");
    assert_eq!(
        ParserError::ParserError("Expected: end of statement, found: TEXT".to_string()),
        res.unwrap_err()
    );
}

#[test]
fn parse_bad_constraint() {
    let res = parse_sql_statements("ALTER TABLE tab ADD");
    assert_eq!(
        ParserError::ParserError("Expected: identifier, found: EOF".to_string()),
        res.unwrap_err()
    );

    let res = parse_sql_statements("CREATE TABLE tab (foo int,");
    assert_eq!(
        ParserError::ParserError(
            "Expected: column name or constraint definition, found: EOF".to_string()
        ),
        res.unwrap_err()
    );
}

#[test]
fn parse_scalar_function_in_projection() {
    let names = vec!["sqrt", "foo"];

    for function_name in names {
        // like SELECT sqrt(id) FROM foo
        let sql = format!("SELECT {function_name}(id) FROM foo");
        let select = verified_only_select(&sql);
        assert_eq!(
            &call(function_name, [Expr::Identifier(Ident::new("id"))]),
            expr_from_projection(only(&select.projection))
        );
    }
}

fn run_explain_analyze(
    dialect: TestedDialects,
    query: &str,
    expected_verbose: bool,
    expected_analyze: bool,
    expected_format: Option<AnalyzeFormatKind>,
    expected_options: Option<Vec<UtilityOption>>,
) {
    match dialect.verified_stmt(query) {
        Statement::Explain {
            describe_alias: _,
            analyze,
            verbose,
            query_plan,
            estimate,
            statement,
            format,
            options,
        } => {
            assert_eq!(verbose, expected_verbose);
            assert_eq!(analyze, expected_analyze);
            assert_eq!(format, expected_format);
            assert_eq!(options, expected_options);
            assert!(!query_plan);
            assert!(!estimate);
            assert_eq!("SELECT sqrt(id) FROM foo", statement.to_string());
        }
        _ => panic!("Unexpected Statement, must be Explain"),
    }
}

#[test]
fn parse_explain_table() {
    let validate_explain =
        |query: &str, expected_describe_alias: DescribeAlias, expected_table_keyword| {
            match verified_stmt(query) {
                Statement::ExplainTable {
                    describe_alias,
                    hive_format,
                    has_table_keyword,
                    table_name,
                } => {
                    assert_eq!(describe_alias, expected_describe_alias);
                    assert_eq!(hive_format, None);
                    assert_eq!(has_table_keyword, expected_table_keyword);
                    assert_eq!("test_identifier", table_name.to_string());
                }
                _ => panic!("Unexpected Statement, must be ExplainTable"),
            }
        };

    validate_explain("EXPLAIN test_identifier", DescribeAlias::Explain, false);
    validate_explain("DESCRIBE test_identifier", DescribeAlias::Describe, false);
    validate_explain("DESC test_identifier", DescribeAlias::Desc, false);
}

#[test]
fn explain_describe() {
    verified_stmt("DESCRIBE test.table");
}

#[test]
fn explain_desc() {
    verified_stmt("DESC test.table");
}

#[test]
fn parse_explain_analyze_with_simple_select() {
    // Describe is an alias for EXPLAIN
    run_explain_analyze(
        all_dialects(),
        "DESCRIBE SELECT sqrt(id) FROM foo",
        false,
        false,
        None,
        None,
    );

    run_explain_analyze(
        all_dialects(),
        "EXPLAIN SELECT sqrt(id) FROM foo",
        false,
        false,
        None,
        None,
    );
    run_explain_analyze(
        all_dialects(),
        "EXPLAIN VERBOSE SELECT sqrt(id) FROM foo",
        true,
        false,
        None,
        None,
    );
    run_explain_analyze(
        all_dialects(),
        "EXPLAIN ANALYZE SELECT sqrt(id) FROM foo",
        false,
        true,
        None,
        None,
    );
    run_explain_analyze(
        all_dialects(),
        "EXPLAIN ANALYZE VERBOSE SELECT sqrt(id) FROM foo",
        true,
        true,
        None,
        None,
    );

    run_explain_analyze(
        all_dialects(),
        "EXPLAIN ANALYZE FORMAT GRAPHVIZ SELECT sqrt(id) FROM foo",
        false,
        true,
        Some(AnalyzeFormatKind::Keyword(AnalyzeFormat::GRAPHVIZ)),
        None,
    );

    run_explain_analyze(
        all_dialects(),
        "EXPLAIN ANALYZE VERBOSE FORMAT JSON SELECT sqrt(id) FROM foo",
        true,
        true,
        Some(AnalyzeFormatKind::Keyword(AnalyzeFormat::JSON)),
        None,
    );

    run_explain_analyze(
        all_dialects(),
        "EXPLAIN ANALYZE VERBOSE FORMAT=JSON SELECT sqrt(id) FROM foo",
        true,
        true,
        Some(AnalyzeFormatKind::Assignment(AnalyzeFormat::JSON)),
        None,
    );

    run_explain_analyze(
        all_dialects(),
        "EXPLAIN VERBOSE FORMAT TEXT SELECT sqrt(id) FROM foo",
        true,
        false,
        Some(AnalyzeFormatKind::Keyword(AnalyzeFormat::TEXT)),
        None,
    );
}

#[test]
fn parse_explain_query_plan() {
    match all_dialects().verified_stmt("EXPLAIN QUERY PLAN SELECT sqrt(id) FROM foo") {
        Statement::Explain {
            query_plan,
            analyze,
            verbose,
            statement,
            ..
        } => {
            assert!(query_plan);
            assert!(!analyze);
            assert!(!verbose);
            assert_eq!("SELECT sqrt(id) FROM foo", statement.to_string());
        }
        _ => unreachable!(),
    }

    // omit QUERY PLAN should be good
    all_dialects().verified_stmt("EXPLAIN SELECT sqrt(id) FROM foo");

    // missing PLAN keyword should return error
    assert_eq!(
        ParserError::ParserError("Expected: end of statement, found: SELECT".to_string()),
        all_dialects()
            .parse_sql_statements("EXPLAIN QUERY SELECT sqrt(id) FROM foo")
            .unwrap_err()
    );
}

#[test]
fn parse_explain_estimate() {
    let statement = all_dialects().verified_stmt("EXPLAIN ESTIMATE SELECT sqrt(id) FROM foo");

    match &statement {
        Statement::Explain {
            query_plan,
            estimate,
            analyze,
            verbose,
            statement,
            ..
        } => {
            assert!(estimate);
            assert!(!query_plan);
            assert!(!analyze);
            assert!(!verbose);
            assert_eq!("SELECT sqrt(id) FROM foo", statement.to_string());
        }
        _ => unreachable!(),
    }

    assert_eq!(
        "EXPLAIN ESTIMATE SELECT sqrt(id) FROM foo",
        statement.to_string()
    );
}

#[test]
fn parse_named_argument_function() {
    let dialects = all_dialects_where(|d| {
        d.supports_named_fn_args_with_rarrow_operator()
            && !d.supports_named_fn_args_with_expr_name()
    });
    let sql = "SELECT FUN(a => '1', b => '2') FROM foo";
    let select = dialects.verified_only_select(sql);

    assert_eq!(
        &Expr::Function(Function {
            name: ObjectName::from(vec![Ident::new("FUN")]),
            uses_odbc_syntax: false,
            parameters: FunctionArguments::None,
            args: FunctionArguments::List(FunctionArgumentList {
                duplicate_treatment: None,
                args: vec![
                    FunctionArg::Named {
                        name: Ident::new("a"),
                        arg: FunctionArgExpr::Expr(Expr::Value(
                            (Value::SingleQuotedString("1".to_owned())).with_empty_span()
                        )),
                        operator: FunctionArgOperator::RightArrow
                    },
                    FunctionArg::Named {
                        name: Ident::new("b"),
                        arg: FunctionArgExpr::Expr(Expr::Value(
                            (Value::SingleQuotedString("2".to_owned())).with_empty_span()
                        )),
                        operator: FunctionArgOperator::RightArrow
                    },
                ],
                clauses: vec![],
            }),
            null_treatment: None,
            filter: None,
            over: None,
            within_group: vec![]
        }),
        expr_from_projection(only(&select.projection))
    );
}

#[test]
fn parse_named_argument_function_with_eq_operator() {
    let sql = "SELECT FUN(a = '1', b = '2') FROM foo";

    let select = all_dialects_where(|d| d.supports_named_fn_args_with_eq_operator())
        .verified_only_select(sql);
    assert_eq!(
        &Expr::Function(Function {
            name: ObjectName::from(vec![Ident::new("FUN")]),
            uses_odbc_syntax: false,
            parameters: FunctionArguments::None,
            args: FunctionArguments::List(FunctionArgumentList {
                duplicate_treatment: None,
                args: vec![
                    FunctionArg::Named {
                        name: Ident::new("a"),
                        arg: FunctionArgExpr::Expr(Expr::Value(
                            (Value::SingleQuotedString("1".to_owned())).with_empty_span()
                        )),
                        operator: FunctionArgOperator::Equals
                    },
                    FunctionArg::Named {
                        name: Ident::new("b"),
                        arg: FunctionArgExpr::Expr(Expr::Value(
                            (Value::SingleQuotedString("2".to_owned())).with_empty_span()
                        )),
                        operator: FunctionArgOperator::Equals
                    },
                ],
                clauses: vec![],
            }),
            null_treatment: None,
            filter: None,
            over: None,
            within_group: vec![],
        }),
        expr_from_projection(only(&select.projection))
    );

    // Ensure that bar = 42 in a function argument parses as an equality binop
    // rather than a named function argument.
    assert_eq!(
        all_dialects_except(|d| d.supports_named_fn_args_with_eq_operator())
            .verified_expr("foo(bar = 42)"),
        call(
            "foo",
            [Expr::BinaryOp {
                left: Box::new(Expr::Identifier(Ident::new("bar"))),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::value(number("42"))),
            }]
        ),
    );

    // TODO: should this parse for all dialects?
    all_dialects_except(|d| d.supports_named_fn_args_with_eq_operator())
        .verified_expr("iff(1 = 1, 1, 0)");
}

#[test]
fn parse_window_functions() {
    let sql = "SELECT row_number() OVER (ORDER BY dt DESC), \
               sum(foo) OVER (PARTITION BY a, b ORDER BY c, d \
               ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), \
               avg(bar) OVER (ORDER BY a \
               RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING), \
               sum(bar) OVER (ORDER BY a \
               RANGE BETWEEN INTERVAL '1' DAY PRECEDING AND INTERVAL '1 MONTH' FOLLOWING), \
               COUNT(*) OVER (ORDER BY a \
               RANGE BETWEEN INTERVAL '1 DAY' PRECEDING AND INTERVAL '1 DAY' FOLLOWING), \
               max(baz) OVER (ORDER BY a \
               ROWS UNBOUNDED PRECEDING), \
               sum(qux) OVER (ORDER BY a \
               GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING) \
               FROM foo";
    let dialects = all_dialects_except(|d| d.require_interval_qualifier());
    let select = dialects.verified_only_select(sql);

    const EXPECTED_PROJ_QTY: usize = 7;
    assert_eq!(EXPECTED_PROJ_QTY, select.projection.len());

    assert_eq!(
        &Expr::Function(Function {
            name: ObjectName::from(vec![Ident::new("row_number")]),
            uses_odbc_syntax: false,
            parameters: FunctionArguments::None,
            args: FunctionArguments::List(FunctionArgumentList {
                duplicate_treatment: None,
                args: vec![],
                clauses: vec![],
            }),
            null_treatment: None,
            filter: None,
            over: Some(WindowType::WindowSpec(WindowSpec {
                window_name: None,
                partition_by: vec![],
                order_by: vec![OrderByExpr {
                    expr: Expr::Identifier(Ident::new("dt")),
                    options: OrderByOptions {
                        asc: Some(false),
                        nulls_first: None,
                    },
                    with_fill: None,
                }],
                window_frame: None,
            })),
            within_group: vec![],
        }),
        expr_from_projection(&select.projection[0])
    );

    for i in 0..EXPECTED_PROJ_QTY {
        assert!(matches!(
            expr_from_projection(&select.projection[i]),
            Expr::Function(Function {
                over: Some(WindowType::WindowSpec(WindowSpec {
                    window_name: None,
                    ..
                })),
                ..
            })
        ));
    }
}

#[test]
fn parse_named_window_functions() {
    let supported_dialects = TestedDialects::new(vec![
        Box::new(GenericDialect {}),
        Box::new(PostgreSqlDialect {}),
        Box::new(MySqlDialect {}),
        Box::new(BigQueryDialect {}),
    ]);

    let sql = "SELECT row_number() OVER (w ORDER BY dt DESC), \
               sum(foo) OVER (win PARTITION BY a, b ORDER BY c, d \
               ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) \
               FROM foo \
               WINDOW w AS (PARTITION BY x), win AS (ORDER BY y)";
    supported_dialects.verified_stmt(sql);

    let select = all_dialects_except(|d| d.is_table_alias(&Keyword::WINDOW, &mut Parser::new(d)))
        .verified_only_select(sql);

    const EXPECTED_PROJ_QTY: usize = 2;
    assert_eq!(EXPECTED_PROJ_QTY, select.projection.len());

    const EXPECTED_WIN_NAMES: [&str; 2] = ["w", "win"];
    for (i, win_name) in EXPECTED_WIN_NAMES.iter().enumerate() {
        assert!(matches!(
            expr_from_projection(&select.projection[i]),
            Expr::Function(Function {
                over: Some(WindowType::WindowSpec(WindowSpec {
                    window_name: Some(Ident { value, .. }),
                    ..
                })),
                ..
            }) if value == win_name
        ));
    }

    let sql = "SELECT \
        FIRST_VALUE(x) OVER (w ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first, \
        FIRST_VALUE(x) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last, \
        SUM(y) OVER (win PARTITION BY x) AS last \
        FROM EMPLOYEE \
        WINDOW w AS (PARTITION BY x), win AS (w ORDER BY y)";
    supported_dialects.verified_stmt(sql);
}

#[test]
fn parse_window_clause() {
    let dialects = all_dialects_except(|d| d.is_table_alias(&Keyword::WINDOW, &mut Parser::new(d)));
    let sql = "SELECT * \
    FROM mytable \
    WINDOW \
        window1 AS (ORDER BY 1 ASC, 2 DESC, 3 NULLS FIRST), \
        window2 AS (window1), \
        window3 AS (PARTITION BY a, b, c), \
        window4 AS (ROWS UNBOUNDED PRECEDING), \
        window5 AS (window1 PARTITION BY a), \
        window6 AS (window1 ORDER BY a), \
        window7 AS (window1 ROWS UNBOUNDED PRECEDING), \
        window8 AS (window1 PARTITION BY a ORDER BY b ROWS UNBOUNDED PRECEDING) \
    ORDER BY C3";
    dialects.verified_only_select(sql);

    let sql = "SELECT * from mytable WINDOW window1 AS window2";
    let dialects = all_dialects_except(|d| {
        d.is::<BigQueryDialect>()
            || d.is::<GenericDialect>()
            || d.is_table_alias(&Keyword::WINDOW, &mut Parser::new(d))
    });
    let res = dialects.parse_sql_statements(sql);
    assert_eq!(
        ParserError::ParserError("Expected: (, found: window2".to_string()),
        res.unwrap_err()
    );
}

#[test]
fn test_parse_named_window() {
    let dialects = all_dialects_except(|d| d.is_table_alias(&Keyword::WINDOW, &mut Parser::new(d)));
    let sql = "SELECT \
    MIN(c12) OVER window1 AS min1, \
    MAX(c12) OVER window2 AS max1 \
    FROM aggregate_test_100 \
    WINDOW window1 AS (ORDER BY C12), \
    window2 AS (PARTITION BY C11) \
    ORDER BY C3";
    let actual_select_only = dialects.verified_only_select(sql);
    let expected = Select {
        select_token: AttachedToken::empty(),
        distinct: None,
        top: None,
        top_before_distinct: false,
        projection: vec![
            SelectItem::ExprWithAlias {
                expr: Expr::Function(Function {
                    name: ObjectName::from(vec![Ident {
                        value: "MIN".to_string(),
                        quote_style: None,
                        span: Span::empty(),
                    }]),
                    uses_odbc_syntax: false,
                    parameters: FunctionArguments::None,
                    args: FunctionArguments::List(FunctionArgumentList {
                        duplicate_treatment: None,
                        args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(
                            Expr::Identifier(Ident {
                                value: "c12".to_string(),
                                quote_style: None,
                                span: Span::empty(),
                            }),
                        ))],
                        clauses: vec![],
                    }),
                    null_treatment: None,
                    filter: None,
                    over: Some(WindowType::NamedWindow(Ident {
                        value: "window1".to_string(),
                        quote_style: None,
                        span: Span::empty(),
                    })),
                    within_group: vec![],
                }),
                alias: Ident {
                    value: "min1".to_string(),
                    quote_style: None,
                    span: Span::empty(),
                },
            },
            SelectItem::ExprWithAlias {
                expr: Expr::Function(Function {
                    name: ObjectName::from(vec![Ident {
                        value: "MAX".to_string(),
                        quote_style: None,
                        span: Span::empty(),
                    }]),
                    uses_odbc_syntax: false,
                    parameters: FunctionArguments::None,
                    args: FunctionArguments::List(FunctionArgumentList {
                        duplicate_treatment: None,
                        args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(
                            Expr::Identifier(Ident {
                                value: "c12".to_string(),
                                quote_style: None,
                                span: Span::empty(),
                            }),
                        ))],
                        clauses: vec![],
                    }),
                    null_treatment: None,
                    filter: None,
                    over: Some(WindowType::NamedWindow(Ident {
                        value: "window2".to_string(),
                        quote_style: None,
                        span: Span::empty(),
                    })),
                    within_group: vec![],
                }),
                alias: Ident {
                    value: "max1".to_string(),
                    quote_style: None,
                    span: Span::empty(),
                },
            },
        ],
        exclude: None,
        into: None,
        from: vec![TableWithJoins {
            relation: table_from_name(ObjectName::from(vec![Ident {
                value: "aggregate_test_100".to_string(),
                quote_style: None,
                span: Span::empty(),
            }])),
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
        named_window: vec![
            NamedWindowDefinition(
                Ident {
                    value: "window1".to_string(),
                    quote_style: None,
                    span: Span::empty(),
                },
                NamedWindowExpr::WindowSpec(WindowSpec {
                    window_name: None,
                    partition_by: vec![],
                    order_by: vec![OrderByExpr {
                        expr: Expr::Identifier(Ident {
                            value: "C12".to_string(),
                            quote_style: None,
                            span: Span::empty(),
                        }),
                        options: OrderByOptions {
                            asc: None,
                            nulls_first: None,
                        },
                        with_fill: None,
                    }],
                    window_frame: None,
                }),
            ),
            NamedWindowDefinition(
                Ident {
                    value: "window2".to_string(),
                    quote_style: None,
                    span: Span::empty(),
                },
                NamedWindowExpr::WindowSpec(WindowSpec {
                    window_name: None,
                    partition_by: vec![Expr::Identifier(Ident {
                        value: "C11".to_string(),
                        quote_style: None,
                        span: Span::empty(),
                    })],
                    order_by: vec![],
                    window_frame: None,
                }),
            ),
        ],
        qualify: None,
        window_before_qualify: true,
        value_table_mode: None,
        connect_by: None,
        flavor: SelectFlavor::Standard,
    };
    assert_eq!(actual_select_only, expected);
}

#[test]
fn parse_window_and_qualify_clause() {
    let dialects = all_dialects_except(|d| {
        d.is_table_alias(&Keyword::WINDOW, &mut Parser::new(d))
            || d.is_table_alias(&Keyword::QUALIFY, &mut Parser::new(d))
    });
    let sql = "SELECT \
    MIN(c12) OVER window1 AS min1 \
    FROM aggregate_test_100 \
    QUALIFY ROW_NUMBER() OVER my_window \
    WINDOW window1 AS (ORDER BY C12), \
    window2 AS (PARTITION BY C11) \
    ORDER BY C3";
    dialects.verified_only_select(sql);

    let sql = "SELECT \
    MIN(c12) OVER window1 AS min1 \
    FROM aggregate_test_100 \
    WINDOW window1 AS (ORDER BY C12), \
    window2 AS (PARTITION BY C11) \
    QUALIFY ROW_NUMBER() OVER my_window \
    ORDER BY C3";
    dialects.verified_only_select(sql);
}

#[test]
fn parse_window_clause_named_window() {
    let sql = "SELECT * FROM mytable WINDOW window1 AS window2";
    let Select { named_window, .. } =
        all_dialects_where(|d| d.supports_window_clause_named_window_reference())
            .verified_only_select(sql);
    assert_eq!(
        vec![NamedWindowDefinition(
            Ident::new("window1"),
            NamedWindowExpr::NamedWindow(Ident::new("window2"))
        )],
        named_window
    );
}

#[test]
fn parse_aggregate_with_group_by() {
    let sql = "SELECT a, COUNT(1), MIN(b), MAX(b) FROM foo GROUP BY a";
    let _ast = verified_only_select(sql);
    //TODO: assertions
}

#[test]
fn parse_literal_integer() {
    let sql = "SELECT 1, -10, +20";
    let select = verified_only_select(sql);
    assert_eq!(3, select.projection.len());
    assert_eq!(
        &Expr::value(number("1")),
        expr_from_projection(&select.projection[0]),
    );
    // negative literal is parsed as a - and expr
    assert_eq!(
        &UnaryOp {
            op: UnaryOperator::Minus,
            expr: Box::new(Expr::value(number("10")))
        },
        expr_from_projection(&select.projection[1]),
    );
    // positive literal is parsed as a + and expr
    assert_eq!(
        &UnaryOp {
            op: UnaryOperator::Plus,
            expr: Box::new(Expr::value(number("20")))
        },
        expr_from_projection(&select.projection[2]),
    )
}

#[test]
fn parse_literal_decimal() {
    // These numbers were explicitly chosen to not roundtrip if represented as
    // f64s (i.e., as 64-bit binary floating point numbers).
    let sql = "SELECT 0.300000000000000004, 9007199254740993.0";
    let select = verified_only_select(sql);
    assert_eq!(2, select.projection.len());
    assert_eq!(
        &Expr::value(number("0.300000000000000004")),
        expr_from_projection(&select.projection[0]),
    );
    assert_eq!(
        &Expr::value(number("9007199254740993.0")),
        expr_from_projection(&select.projection[1]),
    )
}

#[test]
fn parse_literal_string() {
    let sql = "SELECT 'one', N'national string', X'deadBEEF'";
    let select = verified_only_select(sql);
    assert_eq!(3, select.projection.len());
    assert_eq!(
        &Expr::Value((Value::SingleQuotedString("one".to_string())).with_empty_span()),
        expr_from_projection(&select.projection[0])
    );
    assert_eq!(
        &Expr::Value(
            (Value::NationalStringLiteral("national string".to_string())).with_empty_span()
        ),
        expr_from_projection(&select.projection[1])
    );
    assert_eq!(
        &Expr::Value((Value::HexStringLiteral("deadBEEF".to_string())).with_empty_span()),
        expr_from_projection(&select.projection[2])
    );

    one_statement_parses_to("SELECT x'deadBEEF'", "SELECT X'deadBEEF'");
    one_statement_parses_to("SELECT n'national string'", "SELECT N'national string'");
}

#[test]
fn parse_literal_date() {
    let sql = "SELECT DATE '1999-01-01'";
    let select = verified_only_select(sql);
    assert_eq!(
        &Expr::TypedString(TypedString {
            data_type: DataType::Date,
            value: ValueWithSpan {
                value: Value::SingleQuotedString("1999-01-01".into()),
                span: Span::empty(),
            },
            uses_odbc_syntax: false
        }),
        expr_from_projection(only(&select.projection)),
    );
}

#[test]
fn parse_literal_time() {
    let sql = "SELECT TIME '01:23:34'";
    let select = verified_only_select(sql);
    assert_eq!(
        &Expr::TypedString(TypedString {
            data_type: DataType::Time(None, TimezoneInfo::None),
            value: ValueWithSpan {
                value: Value::SingleQuotedString("01:23:34".into()),
                span: Span::empty(),
            },
            uses_odbc_syntax: false
        }),
        expr_from_projection(only(&select.projection)),
    );
}

#[test]
fn parse_literal_datetime() {
    let sql = "SELECT DATETIME '1999-01-01 01:23:34.45'";
    let select = verified_only_select(sql);
    assert_eq!(
        &Expr::TypedString(TypedString {
            data_type: DataType::Datetime(None),
            value: ValueWithSpan {
                value: Value::SingleQuotedString("1999-01-01 01:23:34.45".into()),
                span: Span::empty(),
            },
            uses_odbc_syntax: false
        }),
        expr_from_projection(only(&select.projection)),
    );
}

#[test]
fn parse_literal_timestamp_without_time_zone() {
    let sql = "SELECT TIMESTAMP '1999-01-01 01:23:34'";
    let select = verified_only_select(sql);
    assert_eq!(
        &Expr::TypedString(TypedString {
            data_type: DataType::Timestamp(None, TimezoneInfo::None),
            value: ValueWithSpan {
                value: Value::SingleQuotedString("1999-01-01 01:23:34".into()),
                span: Span::empty(),
            },
            uses_odbc_syntax: false
        }),
        expr_from_projection(only(&select.projection)),
    );

    one_statement_parses_to("SELECT TIMESTAMP '1999-01-01 01:23:34'", sql);
}

#[test]
fn parse_literal_timestamp_with_time_zone() {
    let sql = "SELECT TIMESTAMPTZ '1999-01-01 01:23:34Z'";
    let select = verified_only_select(sql);
    assert_eq!(
        &Expr::TypedString(TypedString {
            data_type: DataType::Timestamp(None, TimezoneInfo::Tz),
            value: ValueWithSpan {
                value: Value::SingleQuotedString("1999-01-01 01:23:34Z".into()),
                span: Span::empty(),
            },
            uses_odbc_syntax: false
        }),
        expr_from_projection(only(&select.projection)),
    );

    one_statement_parses_to("SELECT TIMESTAMPTZ '1999-01-01 01:23:34Z'", sql);
}

#[test]
fn parse_interval_all() {
    // these intervals expressions all work with both variants of INTERVAL

    let sql = "SELECT INTERVAL '1-1' YEAR TO MONTH";
    let select = verified_only_select(sql);
    assert_eq!(
        &Expr::Interval(Interval {
            value: Box::new(Expr::Value(
                (Value::SingleQuotedString(String::from("1-1"))).with_empty_span()
            )),
            leading_field: Some(DateTimeField::Year),
            leading_precision: None,
            last_field: Some(DateTimeField::Month),
            fractional_seconds_precision: None,
        }),
        expr_from_projection(only(&select.projection)),
    );

    let sql = "SELECT INTERVAL '01:01.01' MINUTE (5) TO SECOND (5)";
    let select = verified_only_select(sql);
    assert_eq!(
        &Expr::Interval(Interval {
            value: Box::new(Expr::Value(
                (Value::SingleQuotedString(String::from("01:01.01"))).with_empty_span()
            )),
            leading_field: Some(DateTimeField::Minute),
            leading_precision: Some(5),
            last_field: Some(DateTimeField::Second),
            fractional_seconds_precision: Some(5),
        }),
        expr_from_projection(only(&select.projection)),
    );

    let sql = "SELECT INTERVAL '1' SECOND (5, 4)";
    let select = verified_only_select(sql);
    assert_eq!(
        &Expr::Interval(Interval {
            value: Box::new(Expr::Value(
                (Value::SingleQuotedString(String::from("1"))).with_empty_span()
            )),
            leading_field: Some(DateTimeField::Second),
            leading_precision: Some(5),
            last_field: None,
            fractional_seconds_precision: Some(4),
        }),
        expr_from_projection(only(&select.projection)),
    );

    let sql = "SELECT INTERVAL '10' HOUR";
    let select = verified_only_select(sql);
    assert_eq!(
        &Expr::Interval(Interval {
            value: Box::new(Expr::Value(
                (Value::SingleQuotedString(String::from("10"))).with_empty_span()
            )),
            leading_field: Some(DateTimeField::Hour),
            leading_precision: None,
            last_field: None,
            fractional_seconds_precision: None,
        }),
        expr_from_projection(only(&select.projection)),
    );

    let sql = "SELECT INTERVAL 5 DAY";
    let select = verified_only_select(sql);
    assert_eq!(
        &Expr::Interval(Interval {
            value: Box::new(Expr::value(number("5"))),
            leading_field: Some(DateTimeField::Day),
            leading_precision: None,
            last_field: None,
            fractional_seconds_precision: None,
        }),
        expr_from_projection(only(&select.projection)),
    );

    let sql = "SELECT INTERVAL 5 DAYS";
    let select = verified_only_select(sql);
    assert_eq!(
        &Expr::Interval(Interval {
            value: Box::new(Expr::value(number("5"))),
            leading_field: Some(DateTimeField::Days),
            leading_precision: None,
            last_field: None,
            fractional_seconds_precision: None,
        }),
        expr_from_projection(only(&select.projection)),
    );

    let sql = "SELECT INTERVAL '10' HOUR (1)";
    let select = verified_only_select(sql);
    assert_eq!(
        &Expr::Interval(Interval {
            value: Box::new(Expr::Value(
                (Value::SingleQuotedString(String::from("10"))).with_empty_span()
            )),
            leading_field: Some(DateTimeField::Hour),
            leading_precision: Some(1),
            last_field: None,
            fractional_seconds_precision: None,
        }),
        expr_from_projection(only(&select.projection)),
    );

    let result = parse_sql_statements("SELECT INTERVAL '1' SECOND TO SECOND");
    assert_eq!(
        ParserError::ParserError("Expected: end of statement, found: SECOND".to_string()),
        result.unwrap_err(),
    );

    let result = parse_sql_statements("SELECT INTERVAL '10' HOUR (1) TO HOUR (2)");
    assert_eq!(
        ParserError::ParserError("Expected: end of statement, found: (".to_string()),
        result.unwrap_err(),
    );

    verified_only_select("SELECT INTERVAL '1' YEAR");
    verified_only_select("SELECT INTERVAL '1' MONTH");
    verified_only_select("SELECT INTERVAL '1' WEEK");
    verified_only_select("SELECT INTERVAL '1' DAY");
    verified_only_select("SELECT INTERVAL '1' HOUR");
    verified_only_select("SELECT INTERVAL '1' MINUTE");
    verified_only_select("SELECT INTERVAL '1' SECOND");
    verified_only_select("SELECT INTERVAL '1' YEARS");
    verified_only_select("SELECT INTERVAL '1' MONTHS");
    verified_only_select("SELECT INTERVAL '1' WEEKS");
    verified_only_select("SELECT INTERVAL '1' DAYS");
    verified_only_select("SELECT INTERVAL '1' HOURS");
    verified_only_select("SELECT INTERVAL '1' MINUTES");
    verified_only_select("SELECT INTERVAL '1' SECONDS");
    verified_only_select("SELECT INTERVAL '1' YEAR TO MONTH");
    verified_only_select("SELECT INTERVAL '1' DAY TO HOUR");
    verified_only_select("SELECT INTERVAL '1' DAY TO MINUTE");
    verified_only_select("SELECT INTERVAL '1' DAY TO SECOND");
    verified_only_select("SELECT INTERVAL '1' HOUR TO MINUTE");
    verified_only_select("SELECT INTERVAL '1' HOUR TO SECOND");
    verified_only_select("SELECT INTERVAL '1' MINUTE TO SECOND");
    verified_only_select("SELECT INTERVAL 1 YEAR");
    verified_only_select("SELECT INTERVAL 1 MONTH");
    verified_only_select("SELECT INTERVAL 1 WEEK");
    verified_only_select("SELECT INTERVAL 1 DAY");
    verified_only_select("SELECT INTERVAL 1 HOUR");
    verified_only_select("SELECT INTERVAL 1 MINUTE");
    verified_only_select("SELECT INTERVAL 1 SECOND");
    verified_only_select("SELECT INTERVAL 1 YEARS");
    verified_only_select("SELECT INTERVAL 1 MONTHS");
    verified_only_select("SELECT INTERVAL 1 WEEKS");
    verified_only_select("SELECT INTERVAL 1 DAYS");
    verified_only_select("SELECT INTERVAL 1 HOURS");
    verified_only_select("SELECT INTERVAL 1 MINUTES");
    verified_only_select("SELECT INTERVAL 1 SECONDS");
    verified_only_select(
        "SELECT '2 years 15 months 100 weeks 99 hours 123456789 milliseconds'::INTERVAL",
    );
}

#[test]
fn parse_interval_dont_require_unit() {
    let dialects = all_dialects_except(|d| d.require_interval_qualifier());

    let sql = "SELECT INTERVAL '1 DAY'";
    let select = dialects.verified_only_select(sql);
    assert_eq!(
        &Expr::Interval(Interval {
            value: Box::new(Expr::Value(
                (Value::SingleQuotedString(String::from("1 DAY"))).with_empty_span()
            )),
            leading_field: None,
            leading_precision: None,
            last_field: None,
            fractional_seconds_precision: None,
        }),
        expr_from_projection(only(&select.projection)),
    );
    dialects.verified_only_select("SELECT INTERVAL '1 YEAR'");
    dialects.verified_only_select("SELECT INTERVAL '1 MONTH'");
    dialects.verified_only_select("SELECT INTERVAL '1 DAY'");
    dialects.verified_only_select("SELECT INTERVAL '1 HOUR'");
    dialects.verified_only_select("SELECT INTERVAL '1 MINUTE'");
    dialects.verified_only_select("SELECT INTERVAL '1 SECOND'");
}

#[test]
fn parse_interval_require_unit() {
    let dialects = all_dialects_where(|d| d.require_interval_qualifier());
    let sql = "SELECT INTERVAL '1 DAY'";
    let err = dialects.parse_sql_statements(sql).unwrap_err();
    assert_eq!(
        err.to_string(),
        "sql parser error: INTERVAL requires a unit after the literal value"
    )
}

#[test]
fn parse_interval_require_qualifier() {
    let dialects = all_dialects_where(|d| d.require_interval_qualifier());

    let sql = "SELECT INTERVAL 1 + 1 DAY";
    let select = dialects.verified_only_select(sql);
    assert_eq!(
        expr_from_projection(only(&select.projection)),
        &Expr::Interval(Interval {
            value: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::value(number("1"))),
                op: BinaryOperator::Plus,
                right: Box::new(Expr::value(number("1"))),
            }),
            leading_field: Some(DateTimeField::Day),
            leading_precision: None,
            last_field: None,
            fractional_seconds_precision: None,
        }),
    );

    let sql = "SELECT INTERVAL '1' + '1' DAY";
    let select = dialects.verified_only_select(sql);
    assert_eq!(
        expr_from_projection(only(&select.projection)),
        &Expr::Interval(Interval {
            value: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Value(
                    (Value::SingleQuotedString("1".to_string())).with_empty_span()
                )),
                op: BinaryOperator::Plus,
                right: Box::new(Expr::Value(
                    (Value::SingleQuotedString("1".to_string())).with_empty_span()
                )),
            }),
            leading_field: Some(DateTimeField::Day),
            leading_precision: None,
            last_field: None,
            fractional_seconds_precision: None,
        }),
    );

    let sql = "SELECT INTERVAL '1' + '2' - '3' DAY";
    let select = dialects.verified_only_select(sql);
    assert_eq!(
        expr_from_projection(only(&select.projection)),
        &Expr::Interval(Interval {
            value: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::BinaryOp {
                    left: Box::new(Expr::Value(
                        (Value::SingleQuotedString("1".to_string())).with_empty_span()
                    )),
                    op: BinaryOperator::Plus,
                    right: Box::new(Expr::Value(
                        (Value::SingleQuotedString("2".to_string())).with_empty_span()
                    )),
                }),
                op: BinaryOperator::Minus,
                right: Box::new(Expr::Value(
                    (Value::SingleQuotedString("3".to_string())).with_empty_span()
                )),
            }),
            leading_field: Some(DateTimeField::Day),
            leading_precision: None,
            last_field: None,
            fractional_seconds_precision: None,
        }),
    );
}

#[test]
fn parse_interval_disallow_interval_expr() {
    let dialects = all_dialects_except(|d| d.require_interval_qualifier());

    let sql = "SELECT INTERVAL '1 DAY'";
    let select = dialects.verified_only_select(sql);
    assert_eq!(
        expr_from_projection(only(&select.projection)),
        &Expr::Interval(Interval {
            value: Box::new(Expr::Value(
                (Value::SingleQuotedString(String::from("1 DAY"))).with_empty_span()
            )),
            leading_field: None,
            leading_precision: None,
            last_field: None,
            fractional_seconds_precision: None,
        }),
    );

    dialects.verified_only_select("SELECT INTERVAL '1 YEAR'");
    dialects.verified_only_select("SELECT INTERVAL '1 YEAR' AS one_year");
    dialects.one_statement_parses_to(
        "SELECT INTERVAL '1 YEAR' one_year",
        "SELECT INTERVAL '1 YEAR' AS one_year",
    );

    let sql = "SELECT INTERVAL '1 DAY' > INTERVAL '1 SECOND'";
    let select = dialects.verified_only_select(sql);
    assert_eq!(
        expr_from_projection(only(&select.projection)),
        &Expr::BinaryOp {
            left: Box::new(Expr::Interval(Interval {
                value: Box::new(Expr::Value(
                    (Value::SingleQuotedString(String::from("1 DAY"))).with_empty_span()
                )),
                leading_field: None,
                leading_precision: None,
                last_field: None,
                fractional_seconds_precision: None,
            })),
            op: BinaryOperator::Gt,
            right: Box::new(Expr::Interval(Interval {
                value: Box::new(Expr::Value(
                    (Value::SingleQuotedString(String::from("1 SECOND"))).with_empty_span()
                )),
                leading_field: None,
                leading_precision: None,
                last_field: None,
                fractional_seconds_precision: None,
            }))
        }
    );
}

#[test]
fn interval_disallow_interval_expr_gt() {
    let dialects = all_dialects_except(|d| d.require_interval_qualifier());
    let expr = dialects.verified_expr("INTERVAL '1 second' > x");
    assert_eq!(
        expr,
        Expr::BinaryOp {
            left: Box::new(Expr::Interval(Interval {
                value: Box::new(Expr::Value(
                    (Value::SingleQuotedString("1 second".to_string())).with_empty_span()
                )),
                leading_field: None,
                leading_precision: None,
                last_field: None,
                fractional_seconds_precision: None,
            },)),
            op: BinaryOperator::Gt,
            right: Box::new(Expr::Identifier(Ident {
                value: "x".to_string(),
                quote_style: None,
                span: Span::empty(),
            })),
        }
    )
}

#[test]
fn interval_disallow_interval_expr_double_colon() {
    let dialects = all_dialects_except(|d| d.require_interval_qualifier());
    let expr = dialects.verified_expr("INTERVAL '1 second'::TEXT");
    assert_eq!(
        expr,
        Expr::Cast {
            kind: CastKind::DoubleColon,
            expr: Box::new(Expr::Interval(Interval {
                value: Box::new(Expr::Value(
                    (Value::SingleQuotedString("1 second".to_string())).with_empty_span()
                )),
                leading_field: None,
                leading_precision: None,
                last_field: None,
                fractional_seconds_precision: None,
            })),
            data_type: DataType::Text,
            format: None,
        }
    )
}

#[test]
fn parse_interval_and_or_xor() {
    let sql = "SELECT col FROM test \
        WHERE d3_date > d1_date + INTERVAL '5 days' \
        AND d2_date > d1_date + INTERVAL '3 days'";

    let dialects = all_dialects_except(|d| d.require_interval_qualifier());
    let actual_ast = dialects.parse_sql_statements(sql).unwrap();

    let expected_ast = vec![Statement::Query(Box::new(Query {
        with: None,
        body: Box::new(SetExpr::Select(Box::new(Select {
            select_token: AttachedToken::empty(),
            distinct: None,
            top: None,
            top_before_distinct: false,
            projection: vec![UnnamedExpr(Expr::Identifier(Ident {
                value: "col".to_string(),
                quote_style: None,
                span: Span::empty(),
            }))],
            exclude: None,
            into: None,
            from: vec![TableWithJoins {
                relation: table_from_name(ObjectName::from(vec![Ident {
                    value: "test".to_string(),
                    quote_style: None,
                    span: Span::empty(),
                }])),
                joins: vec![],
            }],
            lateral_views: vec![],
            prewhere: None,
            selection: Some(Expr::BinaryOp {
                left: Box::new(Expr::BinaryOp {
                    left: Box::new(Expr::Identifier(Ident {
                        value: "d3_date".to_string(),
                        quote_style: None,
                        span: Span::empty(),
                    })),
                    op: BinaryOperator::Gt,
                    right: Box::new(Expr::BinaryOp {
                        left: Box::new(Expr::Identifier(Ident {
                            value: "d1_date".to_string(),
                            quote_style: None,
                            span: Span::empty(),
                        })),
                        op: BinaryOperator::Plus,
                        right: Box::new(Expr::Interval(Interval {
                            value: Box::new(Expr::Value(
                                (Value::SingleQuotedString("5 days".to_string())).with_empty_span(),
                            )),
                            leading_field: None,
                            leading_precision: None,
                            last_field: None,
                            fractional_seconds_precision: None,
                        })),
                    }),
                }),
                op: BinaryOperator::And,
                right: Box::new(Expr::BinaryOp {
                    left: Box::new(Expr::Identifier(Ident {
                        value: "d2_date".to_string(),
                        quote_style: None,
                        span: Span::empty(),
                    })),
                    op: BinaryOperator::Gt,
                    right: Box::new(Expr::BinaryOp {
                        left: Box::new(Expr::Identifier(Ident {
                            value: "d1_date".to_string(),
                            quote_style: None,
                            span: Span::empty(),
                        })),
                        op: BinaryOperator::Plus,
                        right: Box::new(Expr::Interval(Interval {
                            value: Box::new(Expr::Value(
                                (Value::SingleQuotedString("3 days".to_string())).with_empty_span(),
                            )),
                            leading_field: None,
                            leading_precision: None,
                            last_field: None,
                            fractional_seconds_precision: None,
                        })),
                    }),
                }),
            }),
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
        order_by: None,
        limit_clause: None,
        fetch: None,
        locks: vec![],
        for_clause: None,
        settings: None,
        format_clause: None,
        pipe_operators: vec![],
    }))];

    assert_eq!(actual_ast, expected_ast);

    dialects.verified_stmt(
        "SELECT col FROM test \
        WHERE d3_date > d1_date + INTERVAL '5 days' \
        AND d2_date > d1_date + INTERVAL '3 days'",
    );

    dialects.verified_stmt(
        "SELECT col FROM test \
        WHERE d3_date > d1_date + INTERVAL '5 days' \
        OR d2_date > d1_date + INTERVAL '3 days'",
    );

    dialects.verified_stmt(
        "SELECT col FROM test \
        WHERE d3_date > d1_date + INTERVAL '5 days' \
        XOR d2_date > d1_date + INTERVAL '3 days'",
    );
}

#[test]
fn parse_at_timezone() {
    let zero = Expr::value(number("0"));
    let sql = "SELECT FROM_UNIXTIME(0) AT TIME ZONE 'UTC-06:00' FROM t";
    let select = verified_only_select(sql);
    assert_eq!(
        &Expr::AtTimeZone {
            timestamp: Box::new(call("FROM_UNIXTIME", [zero.clone()])),
            time_zone: Box::new(Expr::Value(
                (Value::SingleQuotedString("UTC-06:00".to_string())).with_empty_span()
            )),
        },
        expr_from_projection(only(&select.projection)),
    );

    let sql = r#"SELECT DATE_FORMAT(FROM_UNIXTIME(0) AT TIME ZONE 'UTC-06:00', '%Y-%m-%dT%H') AS "hour" FROM t"#;
    let select = verified_only_select(sql);
    assert_eq!(
        &SelectItem::ExprWithAlias {
            expr: call(
                "DATE_FORMAT",
                [
                    Expr::AtTimeZone {
                        timestamp: Box::new(call("FROM_UNIXTIME", [zero])),
                        time_zone: Box::new(Expr::Value(
                            (Value::SingleQuotedString("UTC-06:00".to_string())).with_empty_span()
                        )),
                    },
                    Expr::Value(
                        (Value::SingleQuotedString("%Y-%m-%dT%H".to_string())).with_empty_span()
                    )
                ]
            ),
            alias: Ident {
                value: "hour".to_string(),
                quote_style: Some('"'),
                span: Span::empty(),
            },
        },
        only(&select.projection),
    );
}

#[test]
fn parse_json_keyword() {
    let sql = r#"SELECT JSON '{
  "id": 10,
  "type": "fruit",
  "name": "apple",
  "on_menu": true,
  "recipes":
    {
      "salads":
      [
        { "id": 2001, "type": "Walnut Apple Salad" },
        { "id": 2002, "type": "Apple Spinach Salad" }
      ],
      "desserts":
      [
        { "id": 3001, "type": "Apple Pie" },
        { "id": 3002, "type": "Apple Scones" },
        { "id": 3003, "type": "Apple Crumble" }
      ]
    }
}'"#;
    let select = verified_only_select(sql);
    assert_eq!(
        &Expr::TypedString(TypedString {
            data_type: DataType::JSON,
            value: ValueWithSpan {
                value: Value::SingleQuotedString(
                    r#"{
  "id": 10,
  "type": "fruit",
  "name": "apple",
  "on_menu": true,
  "recipes":
    {
      "salads":
      [
        { "id": 2001, "type": "Walnut Apple Salad" },
        { "id": 2002, "type": "Apple Spinach Salad" }
      ],
      "desserts":
      [
        { "id": 3001, "type": "Apple Pie" },
        { "id": 3002, "type": "Apple Scones" },
        { "id": 3003, "type": "Apple Crumble" }
      ]
    }
}"#
                    .to_string()
                ),
                span: Span::empty(),
            },
            uses_odbc_syntax: false,
        }),
        expr_from_projection(only(&select.projection)),
    );
}

#[test]
fn parse_typed_strings() {
    let expr = verified_expr(r#"JSON '{"foo":"bar"}'"#);
    assert_eq!(
        Expr::TypedString(TypedString {
            data_type: DataType::JSON,
            value: ValueWithSpan {
                value: Value::SingleQuotedString(r#"{"foo":"bar"}"#.into()),
                span: Span::empty(),
            },
            uses_odbc_syntax: false
        }),
        expr
    );

    if let Expr::TypedString(TypedString {
        data_type,
        value,
        uses_odbc_syntax: false,
    }) = expr
    {
        assert_eq!(DataType::JSON, data_type);
        assert_eq!(r#"{"foo":"bar"}"#, value.into_string().unwrap());
    }
}

#[test]
fn parse_bignumeric_keyword() {
    let sql = r#"SELECT BIGNUMERIC '0'"#;
    let select = verified_only_select(sql);
    assert_eq!(
        &Expr::TypedString(TypedString {
            data_type: DataType::BigNumeric(ExactNumberInfo::None),
            value: ValueWithSpan {
                value: Value::SingleQuotedString(r#"0"#.into()),
                span: Span::empty(),
            },
            uses_odbc_syntax: false
        }),
        expr_from_projection(only(&select.projection)),
    );
    verified_stmt("SELECT BIGNUMERIC '0'");

    let sql = r#"SELECT BIGNUMERIC '123456'"#;
    let select = verified_only_select(sql);
    assert_eq!(
        &Expr::TypedString(TypedString {
            data_type: DataType::BigNumeric(ExactNumberInfo::None),
            value: ValueWithSpan {
                value: Value::SingleQuotedString(r#"123456"#.into()),
                span: Span::empty(),
            },
            uses_odbc_syntax: false
        }),
        expr_from_projection(only(&select.projection)),
    );
    verified_stmt("SELECT BIGNUMERIC '123456'");

    let sql = r#"SELECT BIGNUMERIC '-3.14'"#;
    let select = verified_only_select(sql);
    assert_eq!(
        &Expr::TypedString(TypedString {
            data_type: DataType::BigNumeric(ExactNumberInfo::None),
            value: ValueWithSpan {
                value: Value::SingleQuotedString(r#"-3.14"#.into()),
                span: Span::empty(),
            },
            uses_odbc_syntax: false
        }),
        expr_from_projection(only(&select.projection)),
    );
    verified_stmt("SELECT BIGNUMERIC '-3.14'");

    let sql = r#"SELECT BIGNUMERIC '-0.54321'"#;
    let select = verified_only_select(sql);
    assert_eq!(
        &Expr::TypedString(TypedString {
            data_type: DataType::BigNumeric(ExactNumberInfo::None),
            value: ValueWithSpan {
                value: Value::SingleQuotedString(r#"-0.54321"#.into()),
                span: Span::empty(),
            },
            uses_odbc_syntax: false
        }),
        expr_from_projection(only(&select.projection)),
    );
    verified_stmt("SELECT BIGNUMERIC '-0.54321'");

    let sql = r#"SELECT BIGNUMERIC '1.23456e05'"#;
    let select = verified_only_select(sql);
    assert_eq!(
        &Expr::TypedString(TypedString {
            data_type: DataType::BigNumeric(ExactNumberInfo::None),
            value: ValueWithSpan {
                value: Value::SingleQuotedString(r#"1.23456e05"#.into()),
                span: Span::empty(),
            },
            uses_odbc_syntax: false
        }),
        expr_from_projection(only(&select.projection)),
    );
    verified_stmt("SELECT BIGNUMERIC '1.23456e05'");

    let sql = r#"SELECT BIGNUMERIC '-9.876e-3'"#;
    let select = verified_only_select(sql);
    assert_eq!(
        &Expr::TypedString(TypedString {
            data_type: DataType::BigNumeric(ExactNumberInfo::None),
            value: ValueWithSpan {
                value: Value::SingleQuotedString(r#"-9.876e-3"#.into()),
                span: Span::empty(),
            },
            uses_odbc_syntax: false
        }),
        expr_from_projection(only(&select.projection)),
    );
    verified_stmt("SELECT BIGNUMERIC '-9.876e-3'");
}

#[test]
fn parse_simple_math_expr_plus() {
    let sql = "SELECT a + b, 2 + a, 2.5 + a, a_f + b_f, 2 + a_f, 2.5 + a_f FROM c";
    verified_only_select(sql);
}

#[test]
fn parse_simple_math_expr_minus() {
    let sql = "SELECT a - b, 2 - a, 2.5 - a, a_f - b_f, 2 - a_f, 2.5 - a_f FROM c";
    verified_only_select(sql);
}

#[test]
fn parse_table_function() {
    let select = verified_only_select("SELECT * FROM TABLE(FUN('1')) AS a");

    match only(select.from).relation {
        TableFactor::TableFunction { expr, alias } => {
            assert_eq!(
                call(
                    "FUN",
                    [Expr::Value(
                        (Value::SingleQuotedString("1".to_owned())).with_empty_span()
                    )],
                ),
                expr
            );
            assert_eq!(alias, table_alias("a"))
        }
        _ => panic!("Expecting TableFactor::TableFunction"),
    }

    let res = parse_sql_statements("SELECT * FROM TABLE '1' AS a");
    assert_eq!(
        ParserError::ParserError("Expected: (, found: \'1\'".to_string()),
        res.unwrap_err()
    );

    let res = parse_sql_statements("SELECT * FROM TABLE (FUN(a) AS a");
    assert_eq!(
        ParserError::ParserError("Expected: ), found: AS".to_string()),
        res.unwrap_err()
    );
}

#[test]
fn parse_select_with_alias_and_column_defs() {
    let sql = r#"SELECT * FROM jsonb_to_record('{"a": "x", "b": 2}'::JSONB) AS x (a TEXT, b INT)"#;
    let select = verified_only_select(sql);

    match only(&select.from) {
        TableWithJoins {
            relation: TableFactor::Table {
                alias: Some(alias), ..
            },
            ..
        } => {
            assert_eq!(alias.name.value, "x");
            assert_eq!(
                alias.columns,
                vec![
                    TableAliasColumnDef {
                        name: Ident::new("a"),
                        data_type: Some(DataType::Text),
                    },
                    TableAliasColumnDef {
                        name: Ident::new("b"),
                        data_type: Some(DataType::Int(None)),
                    },
                ]
            );
        }
        _ => unreachable!(
            "Expecting only TableWithJoins with TableFactor::Table, got {:#?}",
            select.from
        ),
    }
}

#[test]
fn parse_unnest() {
    let sql = "SELECT UNNEST(make_array(1, 2, 3))";
    one_statement_parses_to(sql, sql);
    let sql = "SELECT UNNEST(make_array(1, 2, 3), make_array(4, 5))";
    one_statement_parses_to(sql, sql);
}

#[test]
fn parse_unnest_in_from_clause() {
    fn chk(
        array_exprs: &str,
        alias: bool,
        with_offset: bool,
        with_offset_alias: bool,
        dialects: &TestedDialects,
        want: Vec<TableWithJoins>,
    ) {
        let sql = &format!(
            "SELECT * FROM UNNEST({}){}{}{}",
            array_exprs,
            if alias { " AS numbers" } else { "" },
            if with_offset { " WITH OFFSET" } else { "" },
            if with_offset_alias {
                " AS with_offset_alias"
            } else {
                ""
            },
        );
        let select = dialects.verified_only_select(sql);
        assert_eq!(select.from, want);
    }
    let dialects = TestedDialects::new(vec![
        Box::new(BigQueryDialect {}),
        Box::new(GenericDialect {}),
    ]);
    // 1. both Alias and WITH OFFSET clauses.
    chk(
        "expr",
        true,
        true,
        false,
        &dialects,
        vec![TableWithJoins {
            relation: TableFactor::UNNEST {
                alias: Some(TableAlias {
                    name: Ident::new("numbers"),
                    columns: vec![],
                }),
                array_exprs: vec![Expr::Identifier(Ident::new("expr"))],
                with_offset: true,
                with_offset_alias: None,
                with_ordinality: false,
            },
            joins: vec![],
        }],
    );
    // 2. neither Alias nor WITH OFFSET clause.
    chk(
        "expr",
        false,
        false,
        false,
        &dialects,
        vec![TableWithJoins {
            relation: TableFactor::UNNEST {
                alias: None,
                array_exprs: vec![Expr::Identifier(Ident::new("expr"))],
                with_offset: false,
                with_offset_alias: None,
                with_ordinality: false,
            },
            joins: vec![],
        }],
    );
    // 3. Alias but no WITH OFFSET clause.
    chk(
        "expr",
        false,
        true,
        false,
        &dialects,
        vec![TableWithJoins {
            relation: TableFactor::UNNEST {
                alias: None,
                array_exprs: vec![Expr::Identifier(Ident::new("expr"))],
                with_offset: true,
                with_offset_alias: None,
                with_ordinality: false,
            },
            joins: vec![],
        }],
    );
    // 4. WITH OFFSET but no Alias.
    chk(
        "expr",
        true,
        false,
        false,
        &dialects,
        vec![TableWithJoins {
            relation: TableFactor::UNNEST {
                alias: Some(TableAlias {
                    name: Ident::new("numbers"),
                    columns: vec![],
                }),
                array_exprs: vec![Expr::Identifier(Ident::new("expr"))],
                with_offset: false,
                with_offset_alias: None,
                with_ordinality: false,
            },
            joins: vec![],
        }],
    );
    // 5. Simple array
    chk(
        "make_array(1, 2, 3)",
        false,
        false,
        false,
        &dialects,
        vec![TableWithJoins {
            relation: TableFactor::UNNEST {
                alias: None,
                array_exprs: vec![call(
                    "make_array",
                    [
                        Expr::value(number("1")),
                        Expr::value(number("2")),
                        Expr::value(number("3")),
                    ],
                )],
                with_offset: false,
                with_offset_alias: None,
                with_ordinality: false,
            },
            joins: vec![],
        }],
    );
    // 6. Multiple arrays
    chk(
        "make_array(1, 2, 3), make_array(5, 6)",
        false,
        false,
        false,
        &dialects,
        vec![TableWithJoins {
            relation: TableFactor::UNNEST {
                alias: None,
                array_exprs: vec![
                    call(
                        "make_array",
                        [
                            Expr::value(number("1")),
                            Expr::value(number("2")),
                            Expr::value(number("3")),
                        ],
                    ),
                    call(
                        "make_array",
                        [Expr::value(number("5")), Expr::value(number("6"))],
                    ),
                ],
                with_offset: false,
                with_offset_alias: None,
                with_ordinality: false,
            },
            joins: vec![],
        }],
    )
}

#[test]
fn parse_parens() {
    use self::BinaryOperator::*;
    use self::Expr::*;
    let sql = "(a + b) - (c + d)";
    assert_eq!(
        BinaryOp {
            left: Box::new(Nested(Box::new(BinaryOp {
                left: Box::new(Identifier(Ident::new("a"))),
                op: Plus,
                right: Box::new(Identifier(Ident::new("b"))),
            }))),
            op: Minus,
            right: Box::new(Nested(Box::new(BinaryOp {
                left: Box::new(Identifier(Ident::new("c"))),
                op: Plus,
                right: Box::new(Identifier(Ident::new("d"))),
            }))),
        },
        verified_expr(sql)
    );
}

#[test]
fn parse_searched_case_expr() {
    let sql = "SELECT CASE WHEN bar IS NULL THEN 'null' WHEN bar = 0 THEN '=0' WHEN bar >= 0 THEN '>=0' ELSE '<0' END FROM foo";
    use self::BinaryOperator::*;
    use self::Expr::{BinaryOp, Case, Identifier, IsNull};
    let select = verified_only_select(sql);
    assert_eq!(
        &Case {
            case_token: AttachedToken::empty(),
            end_token: AttachedToken::empty(),
            operand: None,
            conditions: vec![
                CaseWhen {
                    condition: IsNull(Box::new(Identifier(Ident::new("bar")))),
                    result: Expr::value(Value::SingleQuotedString("null".to_string())),
                },
                CaseWhen {
                    condition: BinaryOp {
                        left: Box::new(Identifier(Ident::new("bar"))),
                        op: Eq,
                        right: Box::new(Expr::value(number("0"))),
                    },
                    result: Expr::value(Value::SingleQuotedString("=0".to_string())),
                },
                CaseWhen {
                    condition: BinaryOp {
                        left: Box::new(Identifier(Ident::new("bar"))),
                        op: GtEq,
                        right: Box::new(Expr::value(number("0"))),
                    },
                    result: Expr::value(Value::SingleQuotedString(">=0".to_string())),
                },
            ],
            else_result: Some(Box::new(Expr::value(Value::SingleQuotedString(
                "<0".to_string()
            )))),
        },
        expr_from_projection(only(&select.projection)),
    );
}

#[test]
fn parse_simple_case_expr() {
    // ANSI calls a CASE expression with an operand "<simple case>"
    let sql = "SELECT CASE foo WHEN 1 THEN 'Y' ELSE 'N' END";
    let select = verified_only_select(sql);
    use self::Expr::{Case, Identifier};
    assert_eq!(
        &Case {
            case_token: AttachedToken::empty(),
            end_token: AttachedToken::empty(),
            operand: Some(Box::new(Identifier(Ident::new("foo")))),
            conditions: vec![CaseWhen {
                condition: Expr::value(number("1")),
                result: Expr::value(Value::SingleQuotedString("Y".to_string())),
            }],
            else_result: Some(Box::new(Expr::value(Value::SingleQuotedString(
                "N".to_string()
            )))),
        },
        expr_from_projection(only(&select.projection)),
    );
}

#[test]
fn parse_from_advanced() {
    let sql = "SELECT * FROM fn(1, 2) AS foo, schema.bar AS bar WITH (NOLOCK)";
    let _select = verified_only_select(sql);
}

#[test]
fn parse_nullary_table_valued_function() {
    let sql = "SELECT * FROM fn()";
    let _select = verified_only_select(sql);
}

#[test]
fn parse_implicit_join() {
    let sql = "SELECT * FROM t1, t2";
    let select = verified_only_select(sql);
    assert_eq!(
        vec![
            TableWithJoins {
                relation: table_from_name(ObjectName::from(vec!["t1".into()])),
                joins: vec![],
            },
            TableWithJoins {
                relation: table_from_name(ObjectName::from(vec!["t2".into()])),
                joins: vec![],
            },
        ],
        select.from,
    );

    let sql = "SELECT * FROM t1a NATURAL JOIN t1b, t2a NATURAL JOIN t2b";
    let select = verified_only_select(sql);
    assert_eq!(
        vec![
            TableWithJoins {
                relation: table_from_name(ObjectName::from(vec!["t1a".into()])),
                joins: vec![Join {
                    relation: table_from_name(ObjectName::from(vec!["t1b".into()])),
                    global: false,
                    join_operator: JoinOperator::Join(JoinConstraint::Natural),
                }],
            },
            TableWithJoins {
                relation: table_from_name(ObjectName::from(vec!["t2a".into()])),
                joins: vec![Join {
                    relation: table_from_name(ObjectName::from(vec!["t2b".into()])),
                    global: false,
                    join_operator: JoinOperator::Join(JoinConstraint::Natural),
                }],
            },
        ],
        select.from,
    );
}

#[test]
fn parse_cross_join() {
    let sql = "SELECT * FROM t1 CROSS JOIN t2";
    let select = verified_only_select(sql);
    assert_eq!(
        Join {
            relation: table_from_name(ObjectName::from(vec![Ident::new("t2")])),
            global: false,
            join_operator: JoinOperator::CrossJoin(JoinConstraint::None),
        },
        only(only(select.from).joins),
    );
}

#[test]
fn parse_cross_join_constraint() {
    fn join_with_constraint(constraint: JoinConstraint) -> Join {
        Join {
            relation: table_from_name(ObjectName::from(vec![Ident::new("t2")])),
            global: false,
            join_operator: JoinOperator::CrossJoin(constraint),
        }
    }

    fn test_constraint(sql: &str, constraint: JoinConstraint) {
        let dialect = all_dialects_where(|d| d.supports_cross_join_constraint());
        let select = dialect.verified_only_select(sql);
        assert_eq!(
            join_with_constraint(constraint),
            only(only(select.from).joins),
        );
    }

    test_constraint(
        "SELECT * FROM t1 CROSS JOIN t2 ON a = b",
        JoinConstraint::On(Expr::BinaryOp {
            left: Box::new(Expr::Identifier(Ident::new("a"))),
            op: BinaryOperator::Eq,
            right: Box::new(Expr::Identifier(Ident::new("b"))),
        }),
    );
    test_constraint(
        "SELECT * FROM t1 CROSS JOIN t2 USING(a)",
        JoinConstraint::Using(vec![ObjectName::from(vec![Ident::new("a")])]),
    );
}

#[test]
fn parse_joins_on() {
    fn join_with_constraint(
        relation: impl Into<String>,
        alias: Option<TableAlias>,
        global: bool,
        f: impl Fn(JoinConstraint) -> JoinOperator,
    ) -> Join {
        Join {
            relation: TableFactor::Table {
                name: ObjectName::from(vec![Ident::new(relation.into())]),
                alias,
                args: None,
                with_hints: vec![],
                version: None,
                partitions: vec![],
                with_ordinality: false,
                json_path: None,
                sample: None,
                index_hints: vec![],
            },
            global,
            join_operator: f(JoinConstraint::On(Expr::BinaryOp {
                left: Box::new(Expr::Identifier("c1".into())),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::Identifier("c2".into())),
            })),
        }
    }
    // Test parsing of aliases
    assert_eq!(
        only(&verified_only_select("SELECT * FROM t1 JOIN t2 AS foo ON c1 = c2").from).joins,
        vec![join_with_constraint(
            "t2",
            table_alias("foo"),
            false,
            JoinOperator::Join,
        )]
    );
    one_statement_parses_to(
        "SELECT * FROM t1 JOIN t2 foo ON c1 = c2",
        "SELECT * FROM t1 JOIN t2 AS foo ON c1 = c2",
    );
    // Test parsing of different join operators
    assert_eq!(
        only(&verified_only_select("SELECT * FROM t1 JOIN t2 ON c1 = c2").from).joins,
        vec![join_with_constraint("t2", None, false, JoinOperator::Join)]
    );
    assert_eq!(
        only(&verified_only_select("SELECT * FROM t1 INNER JOIN t2 ON c1 = c2").from).joins,
        vec![join_with_constraint("t2", None, false, JoinOperator::Inner)]
    );
    assert_eq!(
        only(&verified_only_select("SELECT * FROM t1 LEFT JOIN t2 ON c1 = c2").from).joins,
        vec![join_with_constraint("t2", None, false, JoinOperator::Left)]
    );
    assert_eq!(
        only(&verified_only_select("SELECT * FROM t1 LEFT OUTER JOIN t2 ON c1 = c2").from).joins,
        vec![join_with_constraint(
            "t2",
            None,
            false,
            JoinOperator::LeftOuter
        )]
    );
    assert_eq!(
        only(&verified_only_select("SELECT * FROM t1 RIGHT JOIN t2 ON c1 = c2").from).joins,
        vec![join_with_constraint("t2", None, false, JoinOperator::Right)]
    );
    assert_eq!(
        only(&verified_only_select("SELECT * FROM t1 RIGHT OUTER JOIN t2 ON c1 = c2").from).joins,
        vec![join_with_constraint(
            "t2",
            None,
            false,
            JoinOperator::RightOuter
        )]
    );
    assert_eq!(
        only(&verified_only_select("SELECT * FROM t1 SEMI JOIN t2 ON c1 = c2").from).joins,
        vec![join_with_constraint("t2", None, false, JoinOperator::Semi)]
    );
    assert_eq!(
        only(&verified_only_select("SELECT * FROM t1 LEFT SEMI JOIN t2 ON c1 = c2").from).joins,
        vec![join_with_constraint(
            "t2",
            None,
            false,
            JoinOperator::LeftSemi
        )]
    );
    assert_eq!(
        only(&verified_only_select("SELECT * FROM t1 RIGHT SEMI JOIN t2 ON c1 = c2").from).joins,
        vec![join_with_constraint(
            "t2",
            None,
            false,
            JoinOperator::RightSemi
        )]
    );
    assert_eq!(
        only(&verified_only_select("SELECT * FROM t1 ANTI JOIN t2 ON c1 = c2").from).joins,
        vec![join_with_constraint("t2", None, false, JoinOperator::Anti)]
    );
    assert_eq!(
        only(&verified_only_select("SELECT * FROM t1 LEFT ANTI JOIN t2 ON c1 = c2").from).joins,
        vec![join_with_constraint(
            "t2",
            None,
            false,
            JoinOperator::LeftAnti
        )]
    );
    assert_eq!(
        only(&verified_only_select("SELECT * FROM t1 RIGHT ANTI JOIN t2 ON c1 = c2").from).joins,
        vec![join_with_constraint(
            "t2",
            None,
            false,
            JoinOperator::RightAnti
        )]
    );
    assert_eq!(
        only(&verified_only_select("SELECT * FROM t1 FULL JOIN t2 ON c1 = c2").from).joins,
        vec![join_with_constraint(
            "t2",
            None,
            false,
            JoinOperator::FullOuter
        )]
    );

    assert_eq!(
        only(&verified_only_select("SELECT * FROM t1 GLOBAL FULL JOIN t2 ON c1 = c2").from).joins,
        vec![join_with_constraint(
            "t2",
            None,
            true,
            JoinOperator::FullOuter
        )]
    );
}

#[test]
fn parse_joins_using() {
    fn join_with_constraint(
        relation: impl Into<String>,
        alias: Option<TableAlias>,
        f: impl Fn(JoinConstraint) -> JoinOperator,
    ) -> Join {
        Join {
            relation: TableFactor::Table {
                name: ObjectName::from(vec![Ident::new(relation.into())]),
                alias,
                args: None,
                with_hints: vec![],
                version: None,
                partitions: vec![],
                with_ordinality: false,
                json_path: None,
                sample: None,
                index_hints: vec![],
            },
            global: false,
            join_operator: f(JoinConstraint::Using(vec![ObjectName::from(vec![
                "c1".into()
            ])])),
        }
    }
    // Test parsing of aliases
    assert_eq!(
        only(&verified_only_select("SELECT * FROM t1 JOIN t2 AS foo USING(c1)").from).joins,
        vec![join_with_constraint(
            "t2",
            table_alias("foo"),
            JoinOperator::Join,
        )]
    );
    one_statement_parses_to(
        "SELECT * FROM t1 JOIN t2 foo USING(c1)",
        "SELECT * FROM t1 JOIN t2 AS foo USING(c1)",
    );
    // Test parsing of different join operators
    assert_eq!(
        only(&verified_only_select("SELECT * FROM t1 JOIN t2 USING(c1)").from).joins,
        vec![join_with_constraint("t2", None, JoinOperator::Join)]
    );
    assert_eq!(
        only(&verified_only_select("SELECT * FROM t1 INNER JOIN t2 USING(c1)").from).joins,
        vec![join_with_constraint("t2", None, JoinOperator::Inner)]
    );
    assert_eq!(
        only(&verified_only_select("SELECT * FROM t1 LEFT JOIN t2 USING(c1)").from).joins,
        vec![join_with_constraint("t2", None, JoinOperator::Left)]
    );
    assert_eq!(
        only(&verified_only_select("SELECT * FROM t1 LEFT OUTER JOIN t2 USING(c1)").from).joins,
        vec![join_with_constraint("t2", None, JoinOperator::LeftOuter)]
    );
    assert_eq!(
        only(&verified_only_select("SELECT * FROM t1 RIGHT JOIN t2 USING(c1)").from).joins,
        vec![join_with_constraint("t2", None, JoinOperator::Right)]
    );
    assert_eq!(
        only(&verified_only_select("SELECT * FROM t1 RIGHT OUTER JOIN t2 USING(c1)").from).joins,
        vec![join_with_constraint("t2", None, JoinOperator::RightOuter)]
    );
    assert_eq!(
        only(&verified_only_select("SELECT * FROM t1 SEMI JOIN t2 USING(c1)").from).joins,
        vec![join_with_constraint("t2", None, JoinOperator::Semi)]
    );
    assert_eq!(
        only(&verified_only_select("SELECT * FROM t1 LEFT SEMI JOIN t2 USING(c1)").from).joins,
        vec![join_with_constraint("t2", None, JoinOperator::LeftSemi)]
    );
    assert_eq!(
        only(&verified_only_select("SELECT * FROM t1 RIGHT SEMI JOIN t2 USING(c1)").from).joins,
        vec![join_with_constraint("t2", None, JoinOperator::RightSemi)]
    );
    assert_eq!(
        only(&verified_only_select("SELECT * FROM t1 ANTI JOIN t2 USING(c1)").from).joins,
        vec![join_with_constraint("t2", None, JoinOperator::Anti)]
    );
    assert_eq!(
        only(&verified_only_select("SELECT * FROM t1 LEFT ANTI JOIN t2 USING(c1)").from).joins,
        vec![join_with_constraint("t2", None, JoinOperator::LeftAnti)]
    );
    assert_eq!(
        only(&verified_only_select("SELECT * FROM t1 RIGHT ANTI JOIN t2 USING(c1)").from).joins,
        vec![join_with_constraint("t2", None, JoinOperator::RightAnti)]
    );
    assert_eq!(
        only(&verified_only_select("SELECT * FROM t1 FULL JOIN t2 USING(c1)").from).joins,
        vec![join_with_constraint("t2", None, JoinOperator::FullOuter)]
    );
    verified_stmt("SELECT * FROM tbl1 AS t1 JOIN tbl2 AS t2 USING(t2.col1)");
}

#[test]
fn parse_natural_join() {
    fn natural_join(f: impl Fn(JoinConstraint) -> JoinOperator, alias: Option<TableAlias>) -> Join {
        Join {
            relation: TableFactor::Table {
                name: ObjectName::from(vec![Ident::new("t2")]),
                alias,
                args: None,
                with_hints: vec![],
                version: None,
                partitions: vec![],
                with_ordinality: false,
                json_path: None,
                sample: None,
                index_hints: vec![],
            },
            global: false,
            join_operator: f(JoinConstraint::Natural),
        }
    }

    // unspecified join
    assert_eq!(
        only(&verified_only_select("SELECT * FROM t1 NATURAL JOIN t2").from).joins,
        vec![natural_join(JoinOperator::Join, None)]
    );

    // inner join explicitly
    assert_eq!(
        only(&verified_only_select("SELECT * FROM t1 NATURAL INNER JOIN t2").from).joins,
        vec![natural_join(JoinOperator::Inner, None)]
    );

    // left join explicitly
    assert_eq!(
        only(&verified_only_select("SELECT * FROM t1 NATURAL LEFT JOIN t2").from).joins,
        vec![natural_join(JoinOperator::Left, None)]
    );

    // left outer join explicitly
    assert_eq!(
        only(&verified_only_select("SELECT * FROM t1 NATURAL LEFT OUTER JOIN t2").from).joins,
        vec![natural_join(JoinOperator::LeftOuter, None)]
    );

    // right join explicitly
    assert_eq!(
        only(&verified_only_select("SELECT * FROM t1 NATURAL RIGHT JOIN t2").from).joins,
        vec![natural_join(JoinOperator::Right, None)]
    );

    // right outer join explicitly
    assert_eq!(
        only(&verified_only_select("SELECT * FROM t1 NATURAL RIGHT OUTER JOIN t2").from).joins,
        vec![natural_join(JoinOperator::RightOuter, None)]
    );

    // full join explicitly
    assert_eq!(
        only(&verified_only_select("SELECT * FROM t1 NATURAL FULL JOIN t2").from).joins,
        vec![natural_join(JoinOperator::FullOuter, None)]
    );

    // natural join another table with alias
    assert_eq!(
        only(&verified_only_select("SELECT * FROM t1 NATURAL JOIN t2 AS t3").from).joins,
        vec![natural_join(JoinOperator::Join, table_alias("t3"))]
    );

    let sql = "SELECT * FROM t1 natural";
    assert_eq!(
        ParserError::ParserError("Expected: a join type after NATURAL, found: EOF".to_string()),
        parse_sql_statements(sql).unwrap_err(),
    );
}

#[test]
fn parse_complex_join() {
    let sql = "SELECT c1, c2 FROM t1, t4 JOIN t2 ON t2.c = t1.c LEFT JOIN t3 USING(q, c) WHERE t4.c = t1.c";
    verified_only_select(sql);
}

#[test]
fn parse_join_nesting() {
    let sql = "SELECT * FROM a NATURAL JOIN (b NATURAL JOIN (c NATURAL JOIN d NATURAL JOIN e)) \
               NATURAL JOIN (f NATURAL JOIN (g NATURAL JOIN h))";
    assert_eq!(
        only(&verified_only_select(sql).from).joins,
        vec![
            join(nest!(table("b"), nest!(table("c"), table("d"), table("e")))),
            join(nest!(table("f"), nest!(table("g"), table("h")))),
        ],
    );

    let sql = "SELECT * FROM (a NATURAL JOIN b) NATURAL JOIN c";
    let select = verified_only_select(sql);
    let from = only(select.from);
    assert_eq!(from.relation, nest!(table("a"), table("b")));
    assert_eq!(from.joins, vec![join(table("c"))]);

    let sql = "SELECT * FROM (((a NATURAL JOIN b)))";
    let select = verified_only_select(sql);
    let from = only(select.from);
    assert_eq!(from.relation, nest!(nest!(nest!(table("a"), table("b")))));
    assert_eq!(from.joins, vec![]);

    let sql = "SELECT * FROM a NATURAL JOIN (((b NATURAL JOIN c)))";
    let select = verified_only_select(sql);
    let from = only(select.from);
    assert_eq!(from.relation, table("a"));
    assert_eq!(
        from.joins,
        vec![join(nest!(nest!(nest!(table("b"), table("c")))))]
    );

    let sql = "SELECT * FROM (a NATURAL JOIN b) AS c";
    let select = verified_only_select(sql);
    let from = only(select.from);
    assert_eq!(
        from.relation,
        TableFactor::NestedJoin {
            table_with_joins: Box::new(TableWithJoins {
                relation: table("a"),
                joins: vec![join(table("b"))],
            }),
            alias: table_alias("c"),
        }
    );
    assert_eq!(from.joins, vec![]);
}

#[test]
fn parse_join_syntax_variants() {
    verified_stmt("SELECT c1 FROM t1 JOIN t2 USING(c1)");
    verified_stmt("SELECT c1 FROM t1 INNER JOIN t2 USING(c1)");
    verified_stmt("SELECT c1 FROM t1 LEFT JOIN t2 USING(c1)");
    verified_stmt("SELECT c1 FROM t1 LEFT OUTER JOIN t2 USING(c1)");
    verified_stmt("SELECT c1 FROM t1 RIGHT JOIN t2 USING(c1)");
    verified_stmt("SELECT c1 FROM t1 RIGHT OUTER JOIN t2 USING(c1)");
    one_statement_parses_to(
        "SELECT c1 FROM t1 FULL OUTER JOIN t2 USING(c1)",
        "SELECT c1 FROM t1 FULL JOIN t2 USING(c1)",
    );

    let dialects = all_dialects_except(|d| d.is_table_alias(&Keyword::OUTER, &mut Parser::new(d)));
    let res = dialects.parse_sql_statements("SELECT * FROM a OUTER JOIN b ON 1");
    assert_eq!(
        ParserError::ParserError("Expected: APPLY, found: JOIN".to_string()),
        res.unwrap_err()
    );
}

#[test]
fn parse_ctes() {
    let cte_sqls = vec!["SELECT 1 AS foo", "SELECT 2 AS bar"];
    let with = &format!(
        "WITH a AS ({}), b AS ({}) SELECT foo + bar FROM a, b",
        cte_sqls[0], cte_sqls[1]
    );

    fn assert_ctes_in_select(expected: &[&str], sel: &Query) {
        for (i, exp) in expected.iter().enumerate() {
            let Cte { alias, query, .. } = &sel.with.as_ref().unwrap().cte_tables[i];
            assert_eq!(*exp, query.to_string());
            assert_eq!(
                if i == 0 {
                    Ident::new("a")
                } else {
                    Ident::new("b")
                },
                alias.name
            );
            assert!(alias.columns.is_empty());
        }
    }

    // Top-level CTE
    assert_ctes_in_select(&cte_sqls, &verified_query(with));
    // CTE in a subquery
    let sql = &format!("SELECT ({with})");
    let select = verified_only_select(sql);
    match expr_from_projection(only(&select.projection)) {
        Expr::Subquery(ref subquery) => {
            assert_ctes_in_select(&cte_sqls, subquery.as_ref());
        }
        _ => panic!("Expected: subquery"),
    }
    // CTE in a derived table
    let sql = &format!("SELECT * FROM ({with})");
    let select = verified_only_select(sql);
    match only(select.from).relation {
        TableFactor::Derived { subquery, .. } => {
            assert_ctes_in_select(&cte_sqls, subquery.as_ref())
        }
        _ => panic!("Expected: derived table"),
    }
    // CTE in a view
    let sql = &format!("CREATE VIEW v AS {with}");
    match verified_stmt(sql) {
        Statement::CreateView(create_view) => assert_ctes_in_select(&cte_sqls, &create_view.query),
        _ => panic!("Expected: CREATE VIEW"),
    }
    // CTE in a CTE...
    let sql = &format!("WITH outer_cte AS ({with}) SELECT * FROM outer_cte");
    let select = verified_query(sql);
    assert_ctes_in_select(&cte_sqls, &only(&select.with.unwrap().cte_tables).query);
}

#[test]
fn parse_cte_renamed_columns() {
    let sql = "WITH cte (col1, col2) AS (SELECT foo, bar FROM baz) SELECT * FROM cte";
    let query = all_dialects().verified_query(sql);
    assert_eq!(
        vec![
            TableAliasColumnDef::from_name("col1"),
            TableAliasColumnDef::from_name("col2")
        ],
        query
            .with
            .unwrap()
            .cte_tables
            .first()
            .unwrap()
            .alias
            .columns
    );
}

#[test]
fn parse_recursive_cte() {
    let cte_query = "SELECT 1 UNION ALL SELECT val + 1 FROM nums WHERE val < 10".to_owned();
    let sql = &format!("WITH RECURSIVE nums (val) AS ({cte_query}) SELECT * FROM nums");

    let cte_query = verified_query(&cte_query);
    let query = verified_query(sql);

    let with = query.with.as_ref().unwrap();
    assert!(with.recursive);
    assert_eq!(with.cte_tables.len(), 1);
    let expected = Cte {
        alias: TableAlias {
            name: Ident {
                value: "nums".to_string(),
                quote_style: None,
                span: Span::empty(),
            },
            columns: vec![TableAliasColumnDef::from_name("val")],
        },
        query: Box::new(cte_query),
        from: None,
        materialized: None,
        closing_paren_token: AttachedToken::empty(),
    };
    assert_eq!(with.cte_tables.first().unwrap(), &expected);
}

#[test]
fn parse_cte_in_data_modification_statements() {
    match verified_stmt("WITH x AS (SELECT 1) UPDATE t SET bar = (SELECT * FROM x)") {
        Statement::Query(query) => {
            assert_eq!(query.with.unwrap().to_string(), "WITH x AS (SELECT 1)");
            assert!(matches!(*query.body, SetExpr::Update(_)));
        }
        other => panic!("Expected: UPDATE, got: {other:?}"),
    }

    match verified_stmt("WITH t (x) AS (SELECT 9) DELETE FROM q WHERE id IN (SELECT x FROM t)") {
        Statement::Query(query) => {
            assert_eq!(query.with.unwrap().to_string(), "WITH t (x) AS (SELECT 9)");
            assert!(matches!(*query.body, SetExpr::Delete(_)));
        }
        other => panic!("Expected: DELETE, got: {other:?}"),
    }

    match verified_stmt("WITH x AS (SELECT 42) INSERT INTO t SELECT foo FROM x") {
        Statement::Query(query) => {
            assert_eq!(query.with.unwrap().to_string(), "WITH x AS (SELECT 42)");
            assert!(matches!(*query.body, SetExpr::Insert(_)));
        }
        other => panic!("Expected: INSERT, got: {other:?}"),
    }
}

#[test]
fn parse_derived_tables() {
    let sql = "SELECT a.x, b.y FROM (SELECT x FROM foo) AS a CROSS JOIN (SELECT y FROM bar) AS b";
    let _ = verified_only_select(sql);
    //TODO: add assertions

    let sql = "SELECT a.x, b.y \
               FROM (SELECT x FROM foo) AS a (x) \
               CROSS JOIN (SELECT y FROM bar) AS b (y)";
    let _ = verified_only_select(sql);
    //TODO: add assertions

    let sql = "SELECT * FROM (((SELECT 1)))";
    let _ = verified_only_select(sql);
    // TODO: add assertions

    let sql = "SELECT * FROM t NATURAL JOIN (((SELECT 1)))";
    let _ = verified_only_select(sql);
    // TODO: add assertions

    let sql = "SELECT * FROM (((SELECT 1) UNION (SELECT 2)) AS t1 NATURAL JOIN t2)";
    let select = verified_only_select(sql);
    let from = only(select.from);
    assert_eq!(
        from.relation,
        TableFactor::NestedJoin {
            table_with_joins: Box::new(TableWithJoins {
                relation: TableFactor::Derived {
                    lateral: false,
                    subquery: Box::new(verified_query("(SELECT 1) UNION (SELECT 2)")),
                    alias: Some(TableAlias {
                        name: "t1".into(),
                        columns: vec![],
                    }),
                },
                joins: vec![Join {
                    relation: table_from_name(ObjectName::from(vec!["t2".into()])),
                    global: false,
                    join_operator: JoinOperator::Join(JoinConstraint::Natural),
                }],
            }),
            alias: None,
        }
    );
}

#[test]
fn parse_union_except_intersect_minus() {
    // TODO: add assertions
    verified_stmt("SELECT 1 UNION SELECT 2");
    verified_stmt("SELECT 1 UNION ALL SELECT 2");
    verified_stmt("SELECT 1 UNION DISTINCT SELECT 1");
    verified_stmt("SELECT 1 EXCEPT SELECT 2");
    verified_stmt("SELECT 1 EXCEPT ALL SELECT 2");
    verified_stmt("SELECT 1 EXCEPT DISTINCT SELECT 1");
    verified_stmt("SELECT 1 INTERSECT SELECT 2");
    verified_stmt("SELECT 1 INTERSECT ALL SELECT 2");
    verified_stmt("SELECT 1 INTERSECT DISTINCT SELECT 1");
    verified_stmt("SELECT 1 UNION SELECT 2 UNION SELECT 3");
    verified_stmt("SELECT 1 EXCEPT SELECT 2 UNION SELECT 3"); // Union[Except[1,2], 3]
    verified_stmt("SELECT 1 INTERSECT (SELECT 2 EXCEPT SELECT 3)");
    verified_stmt("WITH cte AS (SELECT 1 AS foo) (SELECT foo FROM cte ORDER BY 1 LIMIT 1)");
    verified_stmt("SELECT 1 UNION (SELECT 2 ORDER BY 1 LIMIT 1)");
    verified_stmt("SELECT 1 UNION SELECT 2 INTERSECT SELECT 3"); // Union[1, Intersect[2,3]]
    verified_stmt("SELECT foo FROM tab UNION SELECT bar FROM TAB");
    verified_stmt("(SELECT * FROM new EXCEPT SELECT * FROM old) UNION ALL (SELECT * FROM old EXCEPT SELECT * FROM new) ORDER BY 1");
    verified_stmt("(SELECT * FROM new EXCEPT DISTINCT SELECT * FROM old) UNION DISTINCT (SELECT * FROM old EXCEPT DISTINCT SELECT * FROM new) ORDER BY 1");
    verified_stmt("SELECT 1 AS x, 2 AS y EXCEPT BY NAME SELECT 9 AS y, 8 AS x");
    verified_stmt("SELECT 1 AS x, 2 AS y EXCEPT ALL BY NAME SELECT 9 AS y, 8 AS x");
    verified_stmt("SELECT 1 AS x, 2 AS y EXCEPT DISTINCT BY NAME SELECT 9 AS y, 8 AS x");
    verified_stmt("SELECT 1 AS x, 2 AS y INTERSECT BY NAME SELECT 9 AS y, 8 AS x");
    verified_stmt("SELECT 1 AS x, 2 AS y INTERSECT ALL BY NAME SELECT 9 AS y, 8 AS x");
    verified_stmt("SELECT 1 AS x, 2 AS y INTERSECT DISTINCT BY NAME SELECT 9 AS y, 8 AS x");

    // Dialects that support `MINUS` as column identifier
    // do not support `MINUS` as a set operator.
    let dialects = all_dialects_where(|d| !d.is_column_alias(&Keyword::MINUS, &mut Parser::new(d)));
    dialects.verified_stmt("SELECT 1 MINUS SELECT 2");
    dialects.verified_stmt("SELECT 1 MINUS ALL SELECT 2");
    dialects.verified_stmt("SELECT 1 MINUS DISTINCT SELECT 1");
}

#[test]
fn parse_values() {
    verified_stmt("SELECT * FROM (VALUES (1), (2), (3))");
    verified_stmt("SELECT * FROM (VALUES (1), (2), (3)), (VALUES (1, 2, 3))");
    verified_stmt("SELECT * FROM (VALUES (1)) UNION VALUES (1)");
    verified_stmt("SELECT * FROM (VALUES ROW(1, NULL, 'a'), ROW(2, NULL, 'b')) AS t (a, b, c)");
}

#[test]
fn parse_multiple_statements() {
    fn test_with(sql1: &str, sql2_kw: &str, sql2_rest: &str) {
        // Check that a string consisting of two statements delimited by a semicolon
        // parses the same as both statements individually:
        let res = parse_sql_statements(&(sql1.to_owned() + ";" + sql2_kw + sql2_rest));
        assert_eq!(
            vec![
                one_statement_parses_to(sql1, ""),
                one_statement_parses_to(&(sql2_kw.to_owned() + sql2_rest), ""),
            ],
            res.unwrap()
        );
        // Check that extra semicolon at the end is stripped by normalization:
        one_statement_parses_to(&(sql1.to_owned() + ";"), sql1);
        // Check that forgetting the semicolon results in an error:
        let res = parse_sql_statements(&(sql1.to_owned() + " " + sql2_kw + sql2_rest));
        assert_eq!(
            ParserError::ParserError("Expected: end of statement, found: ".to_string() + sql2_kw),
            res.unwrap_err()
        );
    }
    test_with("SELECT foo", "SELECT", " bar");
    // ensure that SELECT/WITH is not parsed as a table or column alias if ';'
    // separating the statements is omitted:
    test_with("SELECT foo FROM baz", "SELECT", " bar");
    test_with("SELECT foo", "WITH", " cte AS (SELECT 1 AS s) SELECT bar");
    test_with(
        "SELECT foo FROM baz",
        "WITH",
        " cte AS (SELECT 1 AS s) SELECT bar",
    );
    test_with("DELETE FROM foo", "SELECT", " bar");
    test_with("INSERT INTO foo VALUES (1)", "SELECT", " bar");
    // Since MySQL supports the `CREATE TABLE SELECT` syntax, this needs to be handled separately
    let res = parse_sql_statements("CREATE TABLE foo (baz INT); SELECT bar");
    assert_eq!(
        vec![
            one_statement_parses_to("CREATE TABLE foo (baz INT)", ""),
            one_statement_parses_to("SELECT bar", ""),
        ],
        res.unwrap()
    );
    // Check that extra semicolon at the end is stripped by normalization:
    one_statement_parses_to("CREATE TABLE foo (baz INT);", "CREATE TABLE foo (baz INT)");
    // Make sure that empty statements do not cause an error:
    let res = parse_sql_statements(";;");
    assert_eq!(0, res.unwrap().len());
}

#[test]
fn parse_scalar_subqueries() {
    let sql = "(SELECT 1) + (SELECT 2)";
    assert_matches!(
        verified_expr(sql),
        Expr::BinaryOp {
            op: BinaryOperator::Plus,
            ..
        }
    );
}

#[test]
fn parse_substring() {
    verified_stmt("SELECT SUBSTRING('1')");
    verified_stmt("SELECT SUBSTRING('1' FROM 1)");
    verified_stmt("SELECT SUBSTRING('1' FROM 1 FOR 3)");
    verified_stmt("SELECT SUBSTRING('1', 1, 3)");
    verified_stmt("SELECT SUBSTRING('1', 1)");
    verified_stmt("SELECT SUBSTRING('1' FOR 3)");
    verified_stmt("SELECT SUBSTRING('foo' FROM 1 FOR 2) FROM t");
    verified_stmt("SELECT SUBSTR('foo' FROM 1 FOR 2) FROM t");
    verified_stmt("SELECT SUBSTR('foo', 1, 2) FROM t");
}

#[test]
fn parse_overlay() {
    one_statement_parses_to(
        "SELECT OVERLAY('abccccde' PLACING 'abc' FROM 3)",
        "SELECT OVERLAY('abccccde' PLACING 'abc' FROM 3)",
    );
    one_statement_parses_to(
        "SELECT OVERLAY('abccccde' PLACING 'abc' FROM 3 FOR 12)",
        "SELECT OVERLAY('abccccde' PLACING 'abc' FROM 3 FOR 12)",
    );
    assert_eq!(
        ParserError::ParserError("Expected: PLACING, found: FROM".to_owned()),
        parse_sql_statements("SELECT OVERLAY('abccccde' FROM 3)").unwrap_err(),
    );

    let sql = "SELECT OVERLAY('abcdef' PLACING name FROM 3 FOR id + 1) FROM CUSTOMERS";
    let select = verified_only_select(sql);
    assert_eq!(
        &Expr::Overlay {
            expr: Box::new(Expr::Value(
                (Value::SingleQuotedString("abcdef".to_string())).with_empty_span()
            )),
            overlay_what: Box::new(Expr::Identifier(Ident::new("name"))),
            overlay_from: Box::new(Expr::value(number("3"))),
            overlay_for: Some(Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Identifier(Ident::new("id"))),
                op: BinaryOperator::Plus,
                right: Box::new(Expr::value(number("1"))),
            })),
        },
        expr_from_projection(only(&select.projection))
    );
}

#[test]
fn parse_trim() {
    one_statement_parses_to(
        "SELECT TRIM(BOTH 'xyz' FROM 'xyzfooxyz')",
        "SELECT TRIM(BOTH 'xyz' FROM 'xyzfooxyz')",
    );

    one_statement_parses_to(
        "SELECT TRIM(LEADING 'xyz' FROM 'xyzfooxyz')",
        "SELECT TRIM(LEADING 'xyz' FROM 'xyzfooxyz')",
    );

    one_statement_parses_to(
        "SELECT TRIM(TRAILING 'xyz' FROM 'xyzfooxyz')",
        "SELECT TRIM(TRAILING 'xyz' FROM 'xyzfooxyz')",
    );

    one_statement_parses_to(
        "SELECT TRIM('xyz' FROM 'xyzfooxyz')",
        "SELECT TRIM('xyz' FROM 'xyzfooxyz')",
    );
    one_statement_parses_to("SELECT TRIM('   foo   ')", "SELECT TRIM('   foo   ')");
    one_statement_parses_to(
        "SELECT TRIM(LEADING '   foo   ')",
        "SELECT TRIM(LEADING '   foo   ')",
    );

    assert_eq!(
        ParserError::ParserError("Expected: ), found: 'xyz'".to_owned()),
        parse_sql_statements("SELECT TRIM(FOO 'xyz' FROM 'xyzfooxyz')").unwrap_err()
    );

    //keep Snowflake/BigQuery TRIM syntax failing
    let all_expected_snowflake = TestedDialects::new(vec![
        //Box::new(GenericDialect {}),
        Box::new(PostgreSqlDialect {}),
        Box::new(MsSqlDialect {}),
        Box::new(AnsiDialect {}),
        //Box::new(SnowflakeDialect {}),
        Box::new(HiveDialect {}),
        Box::new(RedshiftSqlDialect {}),
        Box::new(MySqlDialect {}),
        //Box::new(BigQueryDialect {}),
        Box::new(SQLiteDialect {}),
    ]);

    assert_eq!(
        ParserError::ParserError("Expected: ), found: 'a'".to_owned()),
        all_expected_snowflake
            .parse_sql_statements("SELECT TRIM('xyz', 'a')")
            .unwrap_err()
    );
}

#[test]
fn parse_exists_subquery() {
    let expected_inner = verified_query("SELECT 1");
    let sql = "SELECT * FROM t WHERE EXISTS (SELECT 1)";
    let select = verified_only_select(sql);
    assert_eq!(
        Expr::Exists {
            negated: false,
            subquery: Box::new(expected_inner.clone()),
        },
        select.selection.unwrap(),
    );

    let sql = "SELECT * FROM t WHERE NOT EXISTS (SELECT 1)";
    let select = verified_only_select(sql);
    assert_eq!(
        Expr::Exists {
            negated: true,
            subquery: Box::new(expected_inner),
        },
        select.selection.unwrap(),
    );

    verified_stmt("SELECT * FROM t WHERE EXISTS (WITH u AS (SELECT 1) SELECT * FROM u)");
    verified_stmt("SELECT EXISTS (SELECT 1)");

    let res = all_dialects_except(|d| d.is::<DatabricksDialect>())
        .parse_sql_statements("SELECT EXISTS (");
    assert_eq!(
        ParserError::ParserError(
            "Expected: SELECT, VALUES, or a subquery in the query body, found: EOF".to_string()
        ),
        res.unwrap_err(),
    );

    let res = all_dialects_except(|d| d.is::<DatabricksDialect>())
        .parse_sql_statements("SELECT EXISTS (NULL)");
    assert_eq!(
        ParserError::ParserError(
            "Expected: SELECT, VALUES, or a subquery in the query body, found: NULL".to_string()
        ),
        res.unwrap_err(),
    );
}

#[test]
fn parse_create_database() {
    let sql = "CREATE DATABASE mydb";
    match verified_stmt(sql) {
        Statement::CreateDatabase {
            db_name,
            if_not_exists,
            location,
            managed_location,
            clone,
            ..
        } => {
            assert_eq!("mydb", db_name.to_string());
            assert!(!if_not_exists);
            assert_eq!(None, location);
            assert_eq!(None, managed_location);
            assert_eq!(None, clone);
        }
        _ => unreachable!(),
    }
    let sql = "CREATE DATABASE mydb CLONE otherdb";
    match verified_stmt(sql) {
        Statement::CreateDatabase {
            db_name,
            if_not_exists,
            location,
            managed_location,
            clone,
            ..
        } => {
            assert_eq!("mydb", db_name.to_string());
            assert!(!if_not_exists);
            assert_eq!(None, location);
            assert_eq!(None, managed_location);
            assert_eq!(
                Some(ObjectName::from(vec![Ident::new("otherdb".to_string())])),
                clone
            );
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_database_ine() {
    let sql = "CREATE DATABASE IF NOT EXISTS mydb";
    match verified_stmt(sql) {
        Statement::CreateDatabase {
            db_name,
            if_not_exists,
            location,
            managed_location,
            clone,
            ..
        } => {
            assert_eq!("mydb", db_name.to_string());
            assert!(if_not_exists);
            assert_eq!(None, location);
            assert_eq!(None, managed_location);
            assert_eq!(None, clone);
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_drop_database() {
    let sql = "DROP DATABASE mycatalog.mydb";
    match verified_stmt(sql) {
        Statement::Drop {
            names,
            object_type,
            if_exists,
            ..
        } => {
            assert_eq!(
                vec!["mycatalog.mydb"],
                names.iter().map(ToString::to_string).collect::<Vec<_>>()
            );
            assert_eq!(ObjectType::Database, object_type);
            assert!(!if_exists);
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_drop_database_if_exists() {
    let sql = "DROP DATABASE IF EXISTS mydb";
    match verified_stmt(sql) {
        Statement::Drop {
            object_type,
            if_exists,
            ..
        } => {
            assert_eq!(ObjectType::Database, object_type);
            assert!(if_exists);
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_view() {
    let sql = "CREATE VIEW myschema.myview AS SELECT foo FROM bar";
    match verified_stmt(sql) {
        Statement::CreateView(CreateView {
            or_alter,
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
            to,
            params,
            name_before_not_exists: _,
            secure: _,
        }) => {
            assert_eq!(or_alter, false);
            assert_eq!("myschema.myview", name.to_string());
            assert_eq!(Vec::<ViewColumnDef>::new(), columns);
            assert_eq!("SELECT foo FROM bar", query.to_string());
            assert!(!materialized);
            assert!(!or_replace);
            assert_eq!(options, CreateTableOptions::None);
            assert_eq!(cluster_by, vec![]);
            assert!(comment.is_none());
            assert!(!late_binding);
            assert!(!if_not_exists);
            assert!(!temporary);
            assert!(to.is_none());
            assert!(params.is_none());
        }
        _ => unreachable!(),
    }

    let _ = verified_stmt("CREATE OR ALTER VIEW v AS SELECT 1");
}

#[test]
fn parse_create_view_with_options() {
    let sql = "CREATE VIEW v WITH (foo = 'bar', a = 123) AS SELECT 1";
    match verified_stmt(sql) {
        Statement::CreateView(create_view) => {
            assert_eq!(
                CreateTableOptions::With(vec![
                    SqlOption::KeyValue {
                        key: "foo".into(),
                        value: Expr::Value(
                            (Value::SingleQuotedString("bar".into())).with_empty_span()
                        ),
                    },
                    SqlOption::KeyValue {
                        key: "a".into(),
                        value: Expr::value(number("123")),
                    },
                ]),
                create_view.options
            );
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_view_with_columns() {
    let sql = "CREATE VIEW v (has, cols) AS SELECT 1, 2";
    // TODO: why does this fail for ClickHouseDialect? (#1449)
    // match all_dialects().verified_stmt(sql) {
    match all_dialects_except(|d| d.is::<ClickHouseDialect>()).verified_stmt(sql) {
        Statement::CreateView(create_view) => {
            let or_alter = create_view.or_alter;
            let name = create_view.name;
            let columns = create_view.columns;
            let or_replace = create_view.or_replace;
            let options = create_view.options;
            let query = create_view.query;
            let materialized = create_view.materialized;
            let cluster_by = create_view.cluster_by;
            let comment = create_view.comment;
            let late_binding = create_view.with_no_schema_binding;
            let if_not_exists = create_view.if_not_exists;
            let temporary = create_view.temporary;
            let to = create_view.to;
            let params = create_view.params;
            assert_eq!(or_alter, false);
            assert_eq!("v", name.to_string());
            assert_eq!(
                columns,
                vec![Ident::new("has"), Ident::new("cols"),]
                    .into_iter()
                    .map(|name| ViewColumnDef {
                        name,
                        data_type: None,
                        options: None,
                    })
                    .collect::<Vec<_>>()
            );
            assert_eq!(options, CreateTableOptions::None);
            assert_eq!("SELECT 1, 2", query.to_string());
            assert!(!materialized);
            assert!(!or_replace);
            assert_eq!(cluster_by, vec![]);
            assert!(comment.is_none());
            assert!(!late_binding);
            assert!(!if_not_exists);
            assert!(!temporary);
            assert!(to.is_none());
            assert!(params.is_none());
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_view_temporary() {
    let sql = "CREATE TEMPORARY VIEW myschema.myview AS SELECT foo FROM bar";
    match verified_stmt(sql) {
        Statement::CreateView(CreateView {
            or_alter,
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
            to,
            params,
            name_before_not_exists: _,
            secure: _,
        }) => {
            assert_eq!(or_alter, false);
            assert_eq!("myschema.myview", name.to_string());
            assert_eq!(Vec::<ViewColumnDef>::new(), columns);
            assert_eq!("SELECT foo FROM bar", query.to_string());
            assert!(!materialized);
            assert!(!or_replace);
            assert_eq!(options, CreateTableOptions::None);
            assert_eq!(cluster_by, vec![]);
            assert!(comment.is_none());
            assert!(!late_binding);
            assert!(!if_not_exists);
            assert!(temporary);
            assert!(to.is_none());
            assert!(params.is_none());
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_or_replace_view() {
    let sql = "CREATE OR REPLACE VIEW v AS SELECT 1";
    match verified_stmt(sql) {
        Statement::CreateView(CreateView {
            or_alter,
            name,
            columns,
            or_replace,
            options,
            query,
            materialized,
            cluster_by,
            comment,
            with_no_schema_binding: late_binding,
            if_not_exists,
            temporary,
            to,
            params,
            name_before_not_exists: _,
            secure: _,
        }) => {
            assert_eq!(or_alter, false);
            assert_eq!("v", name.to_string());
            assert_eq!(columns, vec![]);
            assert_eq!(options, CreateTableOptions::None);
            assert_eq!("SELECT 1", query.to_string());
            assert!(!materialized);
            assert!(or_replace);
            assert_eq!(cluster_by, vec![]);
            assert!(comment.is_none());
            assert!(!late_binding);
            assert!(!if_not_exists);
            assert!(!temporary);
            assert!(to.is_none());
            assert!(params.is_none());
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_or_replace_materialized_view() {
    // Supported in BigQuery (Beta)
    // https://cloud.google.com/bigquery/docs/materialized-views-intro
    // and Snowflake:
    // https://docs.snowflake.com/en/sql-reference/sql/create-materialized-view.html
    let sql = "CREATE OR REPLACE MATERIALIZED VIEW v AS SELECT 1";
    match verified_stmt(sql) {
        Statement::CreateView(CreateView {
            or_alter,
            name,
            columns,
            or_replace,
            options,
            query,
            materialized,
            cluster_by,
            comment,
            with_no_schema_binding: late_binding,
            if_not_exists,
            temporary,
            to,
            params,
            name_before_not_exists: _,
            secure: _,
        }) => {
            assert_eq!(or_alter, false);
            assert_eq!("v", name.to_string());
            assert_eq!(columns, vec![]);
            assert_eq!(options, CreateTableOptions::None);
            assert_eq!("SELECT 1", query.to_string());
            assert!(materialized);
            assert!(or_replace);
            assert_eq!(cluster_by, vec![]);
            assert!(comment.is_none());
            assert!(!late_binding);
            assert!(!if_not_exists);
            assert!(!temporary);
            assert!(to.is_none());
            assert!(params.is_none());
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_materialized_view() {
    let sql = "CREATE MATERIALIZED VIEW myschema.myview AS SELECT foo FROM bar";
    match verified_stmt(sql) {
        Statement::CreateView(CreateView {
            or_alter,
            name,
            or_replace,
            columns,
            query,
            materialized,
            options,
            cluster_by,
            comment,
            with_no_schema_binding: late_binding,
            if_not_exists,
            temporary,
            to,
            params,
            name_before_not_exists: _,
            secure: _,
        }) => {
            assert_eq!(or_alter, false);
            assert_eq!("myschema.myview", name.to_string());
            assert_eq!(Vec::<ViewColumnDef>::new(), columns);
            assert_eq!("SELECT foo FROM bar", query.to_string());
            assert!(materialized);
            assert_eq!(options, CreateTableOptions::None);
            assert!(!or_replace);
            assert_eq!(cluster_by, vec![]);
            assert!(comment.is_none());
            assert!(!late_binding);
            assert!(!if_not_exists);
            assert!(!temporary);
            assert!(to.is_none());
            assert!(params.is_none());
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_materialized_view_with_cluster_by() {
    let sql = "CREATE MATERIALIZED VIEW myschema.myview CLUSTER BY (foo) AS SELECT foo FROM bar";
    match verified_stmt(sql) {
        Statement::CreateView(CreateView {
            or_alter,
            name,
            or_replace,
            columns,
            query,
            materialized,
            options,
            cluster_by,
            comment,
            with_no_schema_binding: late_binding,
            if_not_exists,
            temporary,
            to,
            params,
            name_before_not_exists: _,
            secure: _,
        }) => {
            assert_eq!(or_alter, false);
            assert_eq!("myschema.myview", name.to_string());
            assert_eq!(Vec::<ViewColumnDef>::new(), columns);
            assert_eq!("SELECT foo FROM bar", query.to_string());
            assert!(materialized);
            assert_eq!(options, CreateTableOptions::None);
            assert!(!or_replace);
            assert_eq!(cluster_by, vec![Ident::new("foo")]);
            assert!(comment.is_none());
            assert!(!late_binding);
            assert!(!if_not_exists);
            assert!(!temporary);
            assert!(to.is_none());
            assert!(params.is_none());
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_drop_table() {
    let sql = "DROP TABLE foo";
    match verified_stmt(sql) {
        Statement::Drop {
            object_type,
            if_exists,
            names,
            cascade,
            purge: _,
            temporary,
            ..
        } => {
            assert!(!if_exists);
            assert_eq!(ObjectType::Table, object_type);
            assert_eq!(
                vec!["foo"],
                names.iter().map(ToString::to_string).collect::<Vec<_>>()
            );
            assert!(!cascade);
            assert!(!temporary);
        }
        _ => unreachable!(),
    }

    let sql = "DROP TABLE IF EXISTS foo, bar CASCADE";
    match verified_stmt(sql) {
        Statement::Drop {
            object_type,
            if_exists,
            names,
            cascade,
            purge: _,
            temporary,
            ..
        } => {
            assert!(if_exists);
            assert_eq!(ObjectType::Table, object_type);
            assert_eq!(
                vec!["foo", "bar"],
                names.iter().map(ToString::to_string).collect::<Vec<_>>()
            );
            assert!(cascade);
            assert!(!temporary);
        }
        _ => unreachable!(),
    }

    let sql = "DROP TABLE";
    assert_eq!(
        ParserError::ParserError("Expected: identifier, found: EOF".to_string()),
        parse_sql_statements(sql).unwrap_err(),
    );

    let sql = "DROP TABLE IF EXISTS foo, bar CASCADE RESTRICT";
    assert_eq!(
        ParserError::ParserError("Cannot specify both CASCADE and RESTRICT in DROP".to_string()),
        parse_sql_statements(sql).unwrap_err(),
    );
}

#[test]
fn parse_drop_view() {
    let sql = "DROP VIEW myschema.myview";
    match verified_stmt(sql) {
        Statement::Drop {
            names, object_type, ..
        } => {
            assert_eq!(
                vec!["myschema.myview"],
                names.iter().map(ToString::to_string).collect::<Vec<_>>()
            );
            assert_eq!(ObjectType::View, object_type);
        }
        _ => unreachable!(),
    }

    verified_stmt("DROP MATERIALIZED VIEW a.b.c");
    verified_stmt("DROP MATERIALIZED VIEW IF EXISTS a.b.c");
}

#[test]
fn parse_drop_user() {
    let sql = "DROP USER u1";
    match verified_stmt(sql) {
        Statement::Drop {
            names, object_type, ..
        } => {
            assert_eq!(
                vec!["u1"],
                names.iter().map(ToString::to_string).collect::<Vec<_>>()
            );
            assert_eq!(ObjectType::User, object_type);
        }
        _ => unreachable!(),
    }
    verified_stmt("DROP USER IF EXISTS u1");
}

#[test]
fn parse_invalid_subquery_without_parens() {
    let res = parse_sql_statements("SELECT SELECT 1 FROM bar WHERE 1=1 FROM baz");
    assert_eq!(
        ParserError::ParserError("Expected: end of statement, found: 1".to_string()),
        res.unwrap_err()
    );
}

#[test]
fn parse_offset() {
    // Dialects that support `OFFSET` as column identifiers
    // don't support this syntax.
    let dialects =
        all_dialects_where(|d| !d.is_column_alias(&Keyword::OFFSET, &mut Parser::new(d)));

    let expected_limit_clause = &Some(LimitClause::LimitOffset {
        limit: None,
        offset: Some(Offset {
            value: Expr::value(number("2")),
            rows: OffsetRows::Rows,
        }),
        limit_by: vec![],
    });
    let ast = dialects.verified_query("SELECT foo FROM bar OFFSET 2 ROWS");
    assert_eq!(&ast.limit_clause, expected_limit_clause);
    let ast = dialects.verified_query("SELECT foo FROM bar WHERE foo = 4 OFFSET 2 ROWS");
    assert_eq!(&ast.limit_clause, expected_limit_clause);
    let ast = dialects.verified_query("SELECT foo FROM bar ORDER BY baz OFFSET 2 ROWS");
    assert_eq!(&ast.limit_clause, expected_limit_clause);
    let ast =
        dialects.verified_query("SELECT foo FROM bar WHERE foo = 4 ORDER BY baz OFFSET 2 ROWS");
    assert_eq!(&ast.limit_clause, expected_limit_clause);
    let ast =
        dialects.verified_query("SELECT foo FROM (SELECT * FROM bar OFFSET 2 ROWS) OFFSET 2 ROWS");
    assert_eq!(&ast.limit_clause, expected_limit_clause);
    match *ast.body {
        SetExpr::Select(s) => match only(s.from).relation {
            TableFactor::Derived { subquery, .. } => {
                assert_eq!(&subquery.limit_clause, expected_limit_clause);
            }
            _ => panic!("Test broke"),
        },
        _ => panic!("Test broke"),
    }
    let expected_limit_clause = LimitClause::LimitOffset {
        limit: None,
        offset: Some(Offset {
            value: Expr::value(number("0")),
            rows: OffsetRows::Rows,
        }),
        limit_by: vec![],
    };
    let ast = dialects.verified_query("SELECT 'foo' OFFSET 0 ROWS");
    assert_eq!(ast.limit_clause, Some(expected_limit_clause));
    let expected_limit_clause = LimitClause::LimitOffset {
        limit: None,
        offset: Some(Offset {
            value: Expr::value(number("1")),
            rows: OffsetRows::Row,
        }),
        limit_by: vec![],
    };
    let ast = dialects.verified_query("SELECT 'foo' OFFSET 1 ROW");
    assert_eq!(ast.limit_clause, Some(expected_limit_clause));
    let expected_limit_clause = LimitClause::LimitOffset {
        limit: None,
        offset: Some(Offset {
            value: Expr::value(number("2")),
            rows: OffsetRows::None,
        }),
        limit_by: vec![],
    };
    let ast = dialects.verified_query("SELECT 'foo' OFFSET 2");
    assert_eq!(ast.limit_clause, Some(expected_limit_clause));
}

#[test]
fn parse_fetch() {
    let fetch_first_two_rows_only = Some(Fetch {
        with_ties: false,
        percent: false,
        quantity: Some(Expr::value(number("2"))),
    });
    let ast = verified_query("SELECT foo FROM bar FETCH FIRST 2 ROWS ONLY");
    assert_eq!(ast.fetch, fetch_first_two_rows_only);
    let ast = verified_query("SELECT 'foo' FETCH FIRST 2 ROWS ONLY");
    assert_eq!(ast.fetch, fetch_first_two_rows_only);
    let ast = verified_query("SELECT foo FROM bar FETCH FIRST ROWS ONLY");
    assert_eq!(
        ast.fetch,
        Some(Fetch {
            with_ties: false,
            percent: false,
            quantity: None,
        })
    );
    let ast = verified_query("SELECT foo FROM bar WHERE foo = 4 FETCH FIRST 2 ROWS ONLY");
    assert_eq!(ast.fetch, fetch_first_two_rows_only);
    let ast = verified_query("SELECT foo FROM bar ORDER BY baz FETCH FIRST 2 ROWS ONLY");
    assert_eq!(ast.fetch, fetch_first_two_rows_only);
    let ast = verified_query(
        "SELECT foo FROM bar WHERE foo = 4 ORDER BY baz FETCH FIRST 2 ROWS WITH TIES",
    );
    assert_eq!(
        ast.fetch,
        Some(Fetch {
            with_ties: true,
            percent: false,
            quantity: Some(Expr::value(number("2"))),
        })
    );
    let ast = verified_query("SELECT foo FROM bar FETCH FIRST 50 PERCENT ROWS ONLY");
    assert_eq!(
        ast.fetch,
        Some(Fetch {
            with_ties: false,
            percent: true,
            quantity: Some(Expr::value(number("50"))),
        })
    );
    let ast = verified_query(
        "SELECT foo FROM bar WHERE foo = 4 ORDER BY baz OFFSET 2 ROWS FETCH FIRST 2 ROWS ONLY",
    );
    let expected_limit_clause = Some(LimitClause::LimitOffset {
        limit: None,
        offset: Some(Offset {
            value: Expr::value(number("2")),
            rows: OffsetRows::Rows,
        }),
        limit_by: vec![],
    });
    assert_eq!(ast.limit_clause, expected_limit_clause);
    assert_eq!(ast.fetch, fetch_first_two_rows_only);
    let ast = verified_query(
        "SELECT foo FROM (SELECT * FROM bar FETCH FIRST 2 ROWS ONLY) FETCH FIRST 2 ROWS ONLY",
    );
    assert_eq!(ast.fetch, fetch_first_two_rows_only);
    match *ast.body {
        SetExpr::Select(s) => match only(s.from).relation {
            TableFactor::Derived { subquery, .. } => {
                assert_eq!(subquery.fetch, fetch_first_two_rows_only);
            }
            _ => panic!("Test broke"),
        },
        _ => panic!("Test broke"),
    }
    let ast = verified_query("SELECT foo FROM (SELECT * FROM bar OFFSET 2 ROWS FETCH FIRST 2 ROWS ONLY) OFFSET 2 ROWS FETCH FIRST 2 ROWS ONLY");
    let expected_limit_clause = &Some(LimitClause::LimitOffset {
        limit: None,
        offset: Some(Offset {
            value: Expr::value(number("2")),
            rows: OffsetRows::Rows,
        }),
        limit_by: vec![],
    });
    assert_eq!(&ast.limit_clause, expected_limit_clause);
    assert_eq!(ast.fetch, fetch_first_two_rows_only);
    match *ast.body {
        SetExpr::Select(s) => match only(s.from).relation {
            TableFactor::Derived { subquery, .. } => {
                assert_eq!(&subquery.limit_clause, expected_limit_clause);
                assert_eq!(subquery.fetch, fetch_first_two_rows_only);
            }
            _ => panic!("Test broke"),
        },
        _ => panic!("Test broke"),
    }
}

#[test]
fn parse_fetch_variations() {
    one_statement_parses_to(
        "SELECT foo FROM bar FETCH FIRST 10 ROW ONLY",
        "SELECT foo FROM bar FETCH FIRST 10 ROWS ONLY",
    );
    one_statement_parses_to(
        "SELECT foo FROM bar FETCH NEXT 10 ROW ONLY",
        "SELECT foo FROM bar FETCH FIRST 10 ROWS ONLY",
    );
    one_statement_parses_to(
        "SELECT foo FROM bar FETCH NEXT 10 ROWS WITH TIES",
        "SELECT foo FROM bar FETCH FIRST 10 ROWS WITH TIES",
    );
    one_statement_parses_to(
        "SELECT foo FROM bar FETCH NEXT ROWS WITH TIES",
        "SELECT foo FROM bar FETCH FIRST ROWS WITH TIES",
    );
    one_statement_parses_to(
        "SELECT foo FROM bar FETCH FIRST ROWS ONLY",
        "SELECT foo FROM bar FETCH FIRST ROWS ONLY",
    );
}

#[test]
fn lateral_derived() {
    fn chk(lateral_in: bool) {
        let lateral_str = if lateral_in { "LATERAL " } else { "" };
        let sql = format!(
            "SELECT * FROM customer LEFT JOIN {lateral_str}\
             (SELECT * FROM orders WHERE orders.customer = customer.id LIMIT 3) AS orders ON 1"
        );
        let select = verified_only_select(&sql);
        let from = only(select.from);
        assert_eq!(from.joins.len(), 1);
        let join = &from.joins[0];
        assert_eq!(
            join.join_operator,
            JoinOperator::Left(JoinConstraint::On(Expr::Value(
                (test_utils::number("1")).with_empty_span()
            )))
        );
        if let TableFactor::Derived {
            lateral,
            ref subquery,
            alias: Some(ref alias),
        } = join.relation
        {
            assert_eq!(lateral_in, lateral);
            assert_eq!(Ident::new("orders"), alias.name);
            assert_eq!(
                subquery.to_string(),
                "SELECT * FROM orders WHERE orders.customer = customer.id LIMIT 3"
            );
        } else {
            unreachable!()
        }
    }
    chk(false);
    chk(true);

    let sql = "SELECT * FROM LATERAL UNNEST ([10,20,30]) as numbers WITH OFFSET;";
    let res = parse_sql_statements(sql);
    assert_eq!(
        ParserError::ParserError("Expected: end of statement, found: WITH".to_string()),
        res.unwrap_err()
    );

    let sql = "SELECT * FROM a LEFT JOIN LATERAL (b CROSS JOIN c)";
    let res = parse_sql_statements(sql);
    assert_eq!(
        ParserError::ParserError(
            "Expected: SELECT, VALUES, or a subquery in the query body, found: b".to_string()
        ),
        res.unwrap_err()
    );
}

#[test]
fn lateral_function() {
    let sql = "SELECT * FROM customer LEFT JOIN LATERAL generate_series(1, customer.id)";
    let actual_select_only = verified_only_select(sql);
    let expected = Select {
        select_token: AttachedToken::empty(),
        distinct: None,
        top: None,
        projection: vec![SelectItem::Wildcard(WildcardAdditionalOptions::default())],
        exclude: None,
        top_before_distinct: false,
        into: None,
        from: vec![TableWithJoins {
            relation: table_from_name(ObjectName::from(vec![Ident {
                value: "customer".to_string(),
                quote_style: None,
                span: Span::empty(),
            }])),
            joins: vec![Join {
                relation: TableFactor::Function {
                    lateral: true,
                    name: ObjectName::from(vec!["generate_series".into()]),
                    args: vec![
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
                            (number("1")).with_empty_span(),
                        ))),
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::CompoundIdentifier(
                            vec![Ident::new("customer"), Ident::new("id")],
                        ))),
                    ],
                    alias: None,
                },
                global: false,
                join_operator: JoinOperator::Left(JoinConstraint::None),
            }],
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
    };
    assert_eq!(actual_select_only, expected);
}

#[test]
fn parse_start_transaction() {
    let dialects = all_dialects_except(|d|
        // BigQuery and Snowflake does not support this syntax
        //
        // BigQuery: <https://cloud.google.com/bigquery/docs/reference/standard-sql/procedural-language#begin_transaction>
        // Snowflake: <https://docs.snowflake.com/en/sql-reference/sql/begin>
        d.is::<BigQueryDialect>() || d.is::<SnowflakeDialect>());
    match dialects
        .verified_stmt("START TRANSACTION READ ONLY, READ WRITE, ISOLATION LEVEL SERIALIZABLE")
    {
        Statement::StartTransaction { modes, .. } => assert_eq!(
            modes,
            vec![
                TransactionMode::AccessMode(TransactionAccessMode::ReadOnly),
                TransactionMode::AccessMode(TransactionAccessMode::ReadWrite),
                TransactionMode::IsolationLevel(TransactionIsolationLevel::Serializable),
            ]
        ),
        _ => unreachable!(),
    }

    // For historical reasons, PostgreSQL allows the commas between the modes to
    // be omitted.
    match dialects.one_statement_parses_to(
        "START TRANSACTION READ ONLY READ WRITE ISOLATION LEVEL SERIALIZABLE",
        "START TRANSACTION READ ONLY, READ WRITE, ISOLATION LEVEL SERIALIZABLE",
    ) {
        Statement::StartTransaction { modes, .. } => assert_eq!(
            modes,
            vec![
                TransactionMode::AccessMode(TransactionAccessMode::ReadOnly),
                TransactionMode::AccessMode(TransactionAccessMode::ReadWrite),
                TransactionMode::IsolationLevel(TransactionIsolationLevel::Serializable),
            ]
        ),
        _ => unreachable!(),
    }

    dialects.verified_stmt("START TRANSACTION");
    dialects.verified_stmt("BEGIN");
    dialects.verified_stmt("BEGIN WORK");
    dialects.verified_stmt("BEGIN TRANSACTION");

    dialects.verified_stmt("START TRANSACTION ISOLATION LEVEL READ UNCOMMITTED");
    dialects.verified_stmt("START TRANSACTION ISOLATION LEVEL READ COMMITTED");
    dialects.verified_stmt("START TRANSACTION ISOLATION LEVEL REPEATABLE READ");
    dialects.verified_stmt("START TRANSACTION ISOLATION LEVEL SERIALIZABLE");

    // Regression test for https://github.com/sqlparser-rs/sqlparser-rs/pull/139,
    // in which START TRANSACTION would fail to parse if followed by a statement
    // terminator.
    assert_eq!(
        dialects.parse_sql_statements("START TRANSACTION; SELECT 1"),
        Ok(vec![
            verified_stmt("START TRANSACTION"),
            verified_stmt("SELECT 1"),
        ])
    );

    let res = dialects.parse_sql_statements("START TRANSACTION ISOLATION LEVEL BAD");
    assert_eq!(
        ParserError::ParserError("Expected: isolation level, found: BAD".to_string()),
        res.unwrap_err()
    );

    let res = dialects.parse_sql_statements("START TRANSACTION BAD");
    assert_eq!(
        ParserError::ParserError("Expected: end of statement, found: BAD".to_string()),
        res.unwrap_err()
    );

    let res = dialects.parse_sql_statements("START TRANSACTION READ ONLY,");
    assert_eq!(
        ParserError::ParserError("Expected: transaction mode, found: EOF".to_string()),
        res.unwrap_err()
    );

    // MS-SQL syntax
    let dialects = all_dialects_where(|d| d.supports_start_transaction_modifier());
    dialects.verified_stmt("BEGIN TRY");
    dialects.verified_stmt("BEGIN CATCH");

    let dialects = all_dialects_where(|d| {
        d.supports_start_transaction_modifier() && d.supports_end_transaction_modifier()
    });
    dialects
        .parse_sql_statements(
            r#"
        BEGIN TRY;
            SELECT 1/0;
        END TRY;
        BEGIN CATCH;
            EXECUTE foo;
        END CATCH;
    "#,
        )
        .unwrap();
}

#[test]
fn parse_set_transaction() {
    // SET TRANSACTION shares transaction mode parsing code with START
    // TRANSACTION, so no need to duplicate the tests here. We just do a quick
    // sanity check.
    match verified_stmt("SET TRANSACTION READ ONLY, READ WRITE, ISOLATION LEVEL SERIALIZABLE") {
        Statement::Set(Set::SetTransaction {
            modes,
            session,
            snapshot,
        }) => {
            assert_eq!(
                modes,
                vec![
                    TransactionMode::AccessMode(TransactionAccessMode::ReadOnly),
                    TransactionMode::AccessMode(TransactionAccessMode::ReadWrite),
                    TransactionMode::IsolationLevel(TransactionIsolationLevel::Serializable),
                ]
            );
            assert!(!session);
            assert_eq!(snapshot, None);
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_set_variable() {
    match verified_stmt("SET SOMETHING = '1'") {
        Statement::Set(Set::SingleAssignment {
            scope,
            hivevar,
            variable,
            values,
        }) => {
            assert_eq!(scope, None);
            assert!(!hivevar);
            assert_eq!(variable, ObjectName::from(vec!["SOMETHING".into()]));
            assert_eq!(
                values,
                vec![Expr::Value(
                    (Value::SingleQuotedString("1".into())).with_empty_span()
                )]
            );
        }
        _ => unreachable!(),
    }

    match verified_stmt("SET GLOBAL VARIABLE = 'Value'") {
        Statement::Set(Set::SingleAssignment {
            scope,
            hivevar,
            variable,
            values,
        }) => {
            assert_eq!(scope, Some(ContextModifier::Global));
            assert!(!hivevar);
            assert_eq!(variable, ObjectName::from(vec!["VARIABLE".into()]));
            assert_eq!(
                values,
                vec![Expr::Value(
                    (Value::SingleQuotedString("Value".into())).with_empty_span()
                )]
            );
        }
        _ => unreachable!(),
    }

    let multi_variable_dialects = all_dialects_where(|d| d.supports_parenthesized_set_variables());
    let sql = r#"SET (a, b, c) = (1, 2, 3)"#;
    match multi_variable_dialects.verified_stmt(sql) {
        Statement::Set(Set::ParenthesizedAssignments { variables, values }) => {
            assert_eq!(
                variables,
                vec![
                    ObjectName::from(vec!["a".into()]),
                    ObjectName::from(vec!["b".into()]),
                    ObjectName::from(vec!["c".into()]),
                ]
            );
            assert_eq!(
                values,
                vec![
                    Expr::value(number("1")),
                    Expr::value(number("2")),
                    Expr::value(number("3")),
                ]
            );
        }
        _ => unreachable!(),
    }

    // Subquery expression
    for (sql, canonical) in [
        (
            "SET (a) = (SELECT 22 FROM tbl1)",
            "SET (a) = ((SELECT 22 FROM tbl1))",
        ),
        (
            "SET (a) = (SELECT 22 FROM tbl1, (SELECT 1 FROM tbl2))",
            "SET (a) = ((SELECT 22 FROM tbl1, (SELECT 1 FROM tbl2)))",
        ),
        (
            "SET (a) = ((SELECT 22 FROM tbl1, (SELECT 1 FROM tbl2)))",
            "SET (a) = ((SELECT 22 FROM tbl1, (SELECT 1 FROM tbl2)))",
        ),
        (
            "SET (a, b) = ((SELECT 22 FROM tbl1, (SELECT 1 FROM tbl2)), SELECT 33 FROM tbl3)",
            "SET (a, b) = ((SELECT 22 FROM tbl1, (SELECT 1 FROM tbl2)), (SELECT 33 FROM tbl3))",
        ),
    ] {
        multi_variable_dialects.one_statement_parses_to(sql, canonical);
    }

    let error_sqls = [
        ("SET (a, b, c) = (1, 2, 3", "Expected: ), found: EOF"),
        ("SET (a, b, c) = 1, 2, 3", "Expected: (, found: 1"),
        (
            "SET (a) = ((SELECT 22 FROM tbl1)",
            "Expected: ), found: EOF",
        ),
        (
            "SET (a) = ((SELECT 22 FROM tbl1) (SELECT 22 FROM tbl1))",
            "Expected: ), found: (",
        ),
    ];
    for (sql, error) in error_sqls {
        assert_eq!(
            ParserError::ParserError(error.to_string()),
            multi_variable_dialects
                .parse_sql_statements(sql)
                .unwrap_err()
        );
    }

    one_statement_parses_to("SET SOMETHING TO '1'", "SET SOMETHING = '1'");
}

#[test]
fn parse_set_role_as_variable() {
    match verified_stmt("SET role = 'foobar'") {
        Statement::Set(Set::SingleAssignment {
            scope,
            hivevar,
            variable,
            values,
        }) => {
            assert_eq!(scope, None);
            assert!(!hivevar);
            assert_eq!(variable, ObjectName::from(vec!["role".into()]));
            assert_eq!(
                values,
                vec![Expr::Value(
                    (Value::SingleQuotedString("foobar".into())).with_empty_span()
                )]
            );
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_double_colon_cast_at_timezone() {
    let sql = "SELECT '2001-01-01T00:00:00.000Z'::TIMESTAMP AT TIME ZONE 'Europe/Brussels' FROM t";
    let select = verified_only_select(sql);

    assert_eq!(
        &Expr::AtTimeZone {
            timestamp: Box::new(Expr::Cast {
                kind: CastKind::DoubleColon,
                expr: Box::new(Expr::Value(
                    (Value::SingleQuotedString("2001-01-01T00:00:00.000Z".to_string()))
                        .with_empty_span()
                )),
                data_type: DataType::Timestamp(None, TimezoneInfo::None),
                format: None
            }),
            time_zone: Box::new(Expr::Value(
                (Value::SingleQuotedString("Europe/Brussels".to_string())).with_empty_span()
            )),
        },
        expr_from_projection(only(&select.projection)),
    );
}

#[test]
fn parse_set_time_zone() {
    match verified_stmt("SET TIMEZONE = 'UTC'") {
        Statement::Set(Set::SingleAssignment {
            scope,
            hivevar,
            variable,
            values,
        }) => {
            assert_eq!(scope, None);
            assert!(!hivevar);
            assert_eq!(variable, ObjectName::from(vec!["TIMEZONE".into()]));
            assert_eq!(
                values,
                vec![Expr::Value(
                    (Value::SingleQuotedString("UTC".into())).with_empty_span()
                )]
            );
        }
        _ => unreachable!(),
    }

    one_statement_parses_to("SET TIME ZONE TO 'UTC'", "SET TIMEZONE = 'UTC'");
}

#[test]
fn parse_commit() {
    match verified_stmt("COMMIT") {
        Statement::Commit { chain: false, .. } => (),
        _ => unreachable!(),
    }

    match verified_stmt("COMMIT AND CHAIN") {
        Statement::Commit { chain: true, .. } => (),
        _ => unreachable!(),
    }

    one_statement_parses_to("COMMIT AND NO CHAIN", "COMMIT");
    one_statement_parses_to("COMMIT WORK AND NO CHAIN", "COMMIT");
    one_statement_parses_to("COMMIT TRANSACTION AND NO CHAIN", "COMMIT");
    one_statement_parses_to("COMMIT WORK AND CHAIN", "COMMIT AND CHAIN");
    one_statement_parses_to("COMMIT TRANSACTION AND CHAIN", "COMMIT AND CHAIN");
    one_statement_parses_to("COMMIT WORK", "COMMIT");
    one_statement_parses_to("COMMIT TRANSACTION", "COMMIT");
}

#[test]
fn parse_end() {
    one_statement_parses_to("END AND NO CHAIN", "END");
    one_statement_parses_to("END WORK AND NO CHAIN", "END");
    one_statement_parses_to("END TRANSACTION AND NO CHAIN", "END");
    one_statement_parses_to("END WORK AND CHAIN", "END AND CHAIN");
    one_statement_parses_to("END TRANSACTION AND CHAIN", "END AND CHAIN");
    one_statement_parses_to("END WORK", "END");
    one_statement_parses_to("END TRANSACTION", "END");
    // MS-SQL syntax
    let dialects = all_dialects_where(|d| d.supports_end_transaction_modifier());
    dialects.verified_stmt("END TRY");
    dialects.verified_stmt("END CATCH");
}

#[test]
fn parse_rollback() {
    match verified_stmt("ROLLBACK") {
        Statement::Rollback {
            chain: false,
            savepoint: None,
        } => (),
        _ => unreachable!(),
    }

    match verified_stmt("ROLLBACK AND CHAIN") {
        Statement::Rollback {
            chain: true,
            savepoint: None,
        } => (),
        _ => unreachable!(),
    }

    match verified_stmt("ROLLBACK TO SAVEPOINT test1") {
        Statement::Rollback {
            chain: false,
            savepoint,
        } => {
            assert_eq!(savepoint, Some(Ident::new("test1")));
        }
        _ => unreachable!(),
    }

    match verified_stmt("ROLLBACK AND CHAIN TO SAVEPOINT test1") {
        Statement::Rollback {
            chain: true,
            savepoint,
        } => {
            assert_eq!(savepoint, Some(Ident::new("test1")));
        }
        _ => unreachable!(),
    }

    one_statement_parses_to("ROLLBACK AND NO CHAIN", "ROLLBACK");
    one_statement_parses_to("ROLLBACK WORK AND NO CHAIN", "ROLLBACK");
    one_statement_parses_to("ROLLBACK TRANSACTION AND NO CHAIN", "ROLLBACK");
    one_statement_parses_to("ROLLBACK WORK AND CHAIN", "ROLLBACK AND CHAIN");
    one_statement_parses_to("ROLLBACK TRANSACTION AND CHAIN", "ROLLBACK AND CHAIN");
    one_statement_parses_to("ROLLBACK WORK", "ROLLBACK");
    one_statement_parses_to("ROLLBACK TRANSACTION", "ROLLBACK");
    one_statement_parses_to("ROLLBACK TO test1", "ROLLBACK TO SAVEPOINT test1");
    one_statement_parses_to(
        "ROLLBACK AND CHAIN TO test1",
        "ROLLBACK AND CHAIN TO SAVEPOINT test1",
    );
}

#[test]
#[should_panic(expected = "Parse results with GenericDialect are different from PostgreSqlDialect")]
fn ensure_multiple_dialects_are_tested() {
    // The SQL here must be parsed differently by different dialects.
    // At the time of writing, `@foo` is accepted as a valid identifier
    // by the Generic and the MSSQL dialect, but not by Postgres and ANSI.
    let _ = parse_sql_statements("SELECT @foo");
}

#[test]
fn parse_create_index() {
    let sql = "CREATE UNIQUE INDEX IF NOT EXISTS idx_name ON test(name, age DESC)";
    let indexed_columns: Vec<IndexColumn> = vec![
        IndexColumn {
            operator_class: None,
            column: OrderByExpr {
                expr: Expr::Identifier(Ident::new("name")),
                with_fill: None,
                options: OrderByOptions {
                    asc: None,
                    nulls_first: None,
                },
            },
        },
        IndexColumn {
            operator_class: None,
            column: OrderByExpr {
                expr: Expr::Identifier(Ident::new("age")),
                with_fill: None,
                options: OrderByOptions {
                    asc: Some(false),
                    nulls_first: None,
                },
            },
        },
    ];
    match verified_stmt(sql) {
        Statement::CreateIndex(CreateIndex {
            name: Some(name),
            table_name,
            columns,
            unique,
            if_not_exists,
            ..
        }) => {
            assert_eq!("idx_name", name.to_string());
            assert_eq!("test", table_name.to_string());
            assert_eq!(indexed_columns, columns);
            assert!(unique);
            assert!(if_not_exists)
        }
        _ => unreachable!(),
    }
}

#[test]
fn test_create_index_with_using_function() {
    let sql = "CREATE UNIQUE INDEX IF NOT EXISTS idx_name ON test USING BTREE (name, age DESC)";
    let indexed_columns: Vec<IndexColumn> = vec![
        IndexColumn {
            operator_class: None,
            column: OrderByExpr {
                expr: Expr::Identifier(Ident::new("name")),
                with_fill: None,
                options: OrderByOptions {
                    asc: None,
                    nulls_first: None,
                },
            },
        },
        IndexColumn {
            operator_class: None,
            column: OrderByExpr {
                expr: Expr::Identifier(Ident::new("age")),
                with_fill: None,
                options: OrderByOptions {
                    asc: Some(false),
                    nulls_first: None,
                },
            },
        },
    ];
    match verified_stmt(sql) {
        Statement::CreateIndex(CreateIndex {
            name: Some(name),
            table_name,
            using,
            columns,
            unique,
            concurrently,
            if_not_exists,
            include,
            nulls_distinct: None,
            with,
            predicate: None,
            index_options,
            alter_options,
        }) => {
            assert_eq!("idx_name", name.to_string());
            assert_eq!("test", table_name.to_string());
            assert_eq!("BTREE", using.unwrap().to_string());
            assert_eq!(indexed_columns, columns);
            assert!(unique);
            assert!(!concurrently);
            assert!(if_not_exists);
            assert!(include.is_empty());
            assert!(with.is_empty());
            assert!(index_options.is_empty());
            assert!(alter_options.is_empty());
        }
        _ => unreachable!(),
    }
}

#[test]
fn test_create_index_with_with_clause() {
    let sql = "CREATE UNIQUE INDEX title_idx ON films(title) WITH (fillfactor = 70, single_param)";
    let indexed_columns: Vec<IndexColumn> = vec![IndexColumn {
        column: OrderByExpr {
            expr: Expr::Identifier(Ident::new("title")),
            options: OrderByOptions {
                asc: None,
                nulls_first: None,
            },
            with_fill: None,
        },
        operator_class: None,
    }];
    let with_parameters = vec![
        Expr::BinaryOp {
            left: Box::new(Expr::Identifier(Ident::new("fillfactor"))),
            op: BinaryOperator::Eq,
            right: Box::new(Expr::value(number("70"))),
        },
        Expr::Identifier(Ident::new("single_param")),
    ];
    let dialects = all_dialects_where(|d| d.supports_create_index_with_clause());
    match dialects.verified_stmt(sql) {
        Statement::CreateIndex(CreateIndex {
            name: Some(name),
            table_name,
            using: None,
            columns,
            unique,
            concurrently,
            if_not_exists,
            include,
            nulls_distinct: None,
            with,
            predicate: None,
            index_options,
            alter_options,
        }) => {
            pretty_assertions::assert_eq!("title_idx", name.to_string());
            pretty_assertions::assert_eq!("films", table_name.to_string());
            pretty_assertions::assert_eq!(indexed_columns, columns);
            assert!(unique);
            assert!(!concurrently);
            assert!(!if_not_exists);
            assert!(include.is_empty());
            pretty_assertions::assert_eq!(with_parameters, with);
            assert!(index_options.is_empty());
            assert!(alter_options.is_empty());
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_drop_index() {
    let sql = "DROP INDEX idx_a";
    match verified_stmt(sql) {
        Statement::Drop {
            names, object_type, ..
        } => {
            assert_eq!(
                vec!["idx_a"],
                names.iter().map(ToString::to_string).collect::<Vec<_>>()
            );
            assert_eq!(ObjectType::Index, object_type);
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_role() {
    let sql = "CREATE ROLE consultant";
    match verified_stmt(sql) {
        Statement::CreateRole(create_role) => {
            assert_eq_vec(&["consultant"], &create_role.names);
        }
        _ => unreachable!(),
    }

    let sql = "CREATE ROLE IF NOT EXISTS mysql_a, mysql_b";
    match verified_stmt(sql) {
        Statement::CreateRole(create_role) => {
            assert_eq_vec(&["mysql_a", "mysql_b"], &create_role.names);
            assert!(create_role.if_not_exists);
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_drop_role() {
    let sql = "DROP ROLE abc";
    match verified_stmt(sql) {
        Statement::Drop {
            names,
            object_type,
            if_exists,
            ..
        } => {
            assert_eq_vec(&["abc"], &names);
            assert_eq!(ObjectType::Role, object_type);
            assert!(!if_exists);
        }
        _ => unreachable!(),
    };

    let sql = "DROP ROLE IF EXISTS def, magician, quaternion";
    match verified_stmt(sql) {
        Statement::Drop {
            names,
            object_type,
            if_exists,
            ..
        } => {
            assert_eq_vec(&["def", "magician", "quaternion"], &names);
            assert_eq!(ObjectType::Role, object_type);
            assert!(if_exists);
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_grant() {
    let sql = "GRANT SELECT, INSERT, UPDATE (shape, size), USAGE, DELETE, TRUNCATE, REFERENCES, TRIGGER, CONNECT, CREATE, EXECUTE, TEMPORARY, DROP ON abc, def TO xyz, m WITH GRANT OPTION GRANTED BY jj";
    match verified_stmt(sql) {
        Statement::Grant {
            privileges,
            objects,
            grantees,
            with_grant_option,
            granted_by,
            ..
        } => match (privileges, objects) {
            (Privileges::Actions(actions), Some(GrantObjects::Tables(objects))) => {
                assert_eq!(
                    vec![
                        Action::Select { columns: None },
                        Action::Insert { columns: None },
                        Action::Update {
                            columns: Some(vec![
                                Ident {
                                    value: "shape".into(),
                                    quote_style: None,
                                    span: Span::empty(),
                                },
                                Ident {
                                    value: "size".into(),
                                    quote_style: None,
                                    span: Span::empty(),
                                },
                            ])
                        },
                        Action::Usage,
                        Action::Delete,
                        Action::Truncate,
                        Action::References { columns: None },
                        Action::Trigger,
                        Action::Connect,
                        Action::Create { obj_type: None },
                        Action::Execute { obj_type: None },
                        Action::Temporary,
                        Action::Drop,
                    ],
                    actions
                );
                assert_eq_vec(&["abc", "def"], &objects);
                assert_eq_vec(&["xyz", "m"], &grantees);
                assert!(with_grant_option);
                assert_eq!("jj", granted_by.unwrap().to_string());
            }
            _ => unreachable!(),
        },
        _ => unreachable!(),
    }

    let sql2 = "GRANT INSERT ON ALL TABLES IN SCHEMA public TO browser";
    match verified_stmt(sql2) {
        Statement::Grant {
            privileges,
            objects,
            grantees,
            with_grant_option,
            ..
        } => match (privileges, objects) {
            (Privileges::Actions(actions), Some(GrantObjects::AllTablesInSchema { schemas })) => {
                assert_eq!(vec![Action::Insert { columns: None }], actions);
                assert_eq_vec(&["public"], &schemas);
                assert_eq_vec(&["browser"], &grantees);
                assert!(!with_grant_option);
            }
            _ => unreachable!(),
        },
        _ => unreachable!(),
    }

    let sql3 = "GRANT USAGE, SELECT ON SEQUENCE p TO u";
    match verified_stmt(sql3) {
        Statement::Grant {
            privileges,
            objects,
            grantees,
            granted_by,
            ..
        } => match (privileges, objects, granted_by) {
            (Privileges::Actions(actions), Some(GrantObjects::Sequences(objects)), None) => {
                assert_eq!(
                    vec![Action::Usage, Action::Select { columns: None }],
                    actions
                );
                assert_eq_vec(&["p"], &objects);
                assert_eq_vec(&["u"], &grantees);
            }
            _ => unreachable!(),
        },
        _ => unreachable!(),
    }

    let sql4 = "GRANT ALL PRIVILEGES ON aa, b TO z";
    match verified_stmt(sql4) {
        Statement::Grant { privileges, .. } => {
            assert_eq!(
                Privileges::All {
                    with_privileges_keyword: true
                },
                privileges
            );
        }
        _ => unreachable!(),
    }

    let sql5 = "GRANT ALL ON SCHEMA aa, b TO z";
    match verified_stmt(sql5) {
        Statement::Grant {
            privileges,
            objects,
            ..
        } => match (privileges, objects) {
            (
                Privileges::All {
                    with_privileges_keyword,
                },
                Some(GrantObjects::Schemas(schemas)),
            ) => {
                assert!(!with_privileges_keyword);
                assert_eq_vec(&["aa", "b"], &schemas);
            }
            _ => unreachable!(),
        },
        _ => unreachable!(),
    }

    let sql6 = "GRANT USAGE ON ALL SEQUENCES IN SCHEMA bus TO a, beta WITH GRANT OPTION";
    match verified_stmt(sql6) {
        Statement::Grant {
            privileges,
            objects,
            ..
        } => match (privileges, objects) {
            (
                Privileges::Actions(actions),
                Some(GrantObjects::AllSequencesInSchema { schemas }),
            ) => {
                assert_eq!(vec![Action::Usage], actions);
                assert_eq_vec(&["bus"], &schemas);
            }
            _ => unreachable!(),
        },
        _ => unreachable!(),
    }

    verified_stmt("GRANT SELECT ON ALL TABLES IN SCHEMA db1.sc1 TO ROLE role1");
    verified_stmt("GRANT SELECT ON ALL TABLES IN SCHEMA db1.sc1 TO ROLE role1 WITH GRANT OPTION");
    verified_stmt("GRANT SELECT ON ALL TABLES IN SCHEMA db1.sc1 TO DATABASE ROLE role1");
    verified_stmt("GRANT SELECT ON ALL TABLES IN SCHEMA db1.sc1 TO APPLICATION role1");
    verified_stmt("GRANT SELECT ON ALL TABLES IN SCHEMA db1.sc1 TO APPLICATION ROLE role1");
    verified_stmt("GRANT SELECT ON ALL TABLES IN SCHEMA db1.sc1 TO SHARE share1");
    verified_stmt("GRANT SELECT ON ALL VIEWS IN SCHEMA db1.sc1 TO ROLE role1");
    verified_stmt("GRANT SELECT ON ALL MATERIALIZED VIEWS IN SCHEMA db1.sc1 TO ROLE role1");
    verified_stmt("GRANT SELECT ON ALL EXTERNAL TABLES IN SCHEMA db1.sc1 TO ROLE role1");
    verified_stmt("GRANT USAGE ON ALL FUNCTIONS IN SCHEMA db1.sc1 TO ROLE role1");
    verified_stmt("GRANT USAGE ON SCHEMA sc1 TO a:b");
    verified_stmt("GRANT USAGE ON SCHEMA sc1 TO GROUP group1");
    verified_stmt("GRANT OWNERSHIP ON ALL TABLES IN SCHEMA DEV_STAS_ROGOZHIN TO ROLE ANALYST");
    verified_stmt("GRANT OWNERSHIP ON ALL TABLES IN SCHEMA DEV_STAS_ROGOZHIN TO ROLE ANALYST COPY CURRENT GRANTS");
    verified_stmt("GRANT OWNERSHIP ON ALL TABLES IN SCHEMA DEV_STAS_ROGOZHIN TO ROLE ANALYST REVOKE CURRENT GRANTS");
    verified_stmt("GRANT USAGE ON DATABASE db1 TO ROLE role1");
    verified_stmt("GRANT USAGE ON WAREHOUSE wh1 TO ROLE role1");
    verified_stmt("GRANT OWNERSHIP ON INTEGRATION int1 TO ROLE role1");
    verified_stmt("GRANT SELECT ON VIEW view1 TO ROLE role1");
    verified_stmt("GRANT EXEC ON my_sp TO runner");
    verified_stmt("GRANT UPDATE ON my_table TO updater_role AS dbo");
    all_dialects_where(|d| d.identifier_quote_style("none") == Some('['))
        .verified_stmt("GRANT SELECT ON [my_table] TO [public]");
    verified_stmt("GRANT SELECT ON FUTURE SCHEMAS IN DATABASE db1 TO ROLE role1");
    verified_stmt("GRANT SELECT ON FUTURE TABLES IN SCHEMA db1.sc1 TO ROLE role1");
    verified_stmt("GRANT SELECT ON FUTURE EXTERNAL TABLES IN SCHEMA db1.sc1 TO ROLE role1");
    verified_stmt("GRANT SELECT ON FUTURE VIEWS IN SCHEMA db1.sc1 TO ROLE role1");
    verified_stmt("GRANT SELECT ON FUTURE MATERIALIZED VIEWS IN SCHEMA db1.sc1 TO ROLE role1");
    verified_stmt("GRANT SELECT ON FUTURE SEQUENCES IN SCHEMA db1.sc1 TO ROLE role1");
    verified_stmt("GRANT USAGE ON PROCEDURE db1.sc1.foo(INT) TO ROLE role1");
    verified_stmt("GRANT USAGE ON FUNCTION db1.sc1.foo(INT) TO ROLE role1");
    verified_stmt("GRANT ROLE role1 TO ROLE role2");
    verified_stmt("GRANT ROLE role1 TO USER user");
    verified_stmt("GRANT CREATE SCHEMA ON DATABASE db1 TO ROLE role1");
}

#[test]
fn parse_deny() {
    let sql = "DENY INSERT, DELETE ON users TO analyst CASCADE AS admin";
    match verified_stmt(sql) {
        Statement::Deny(deny) => {
            assert_eq!(
                Privileges::Actions(vec![Action::Insert { columns: None }, Action::Delete]),
                deny.privileges
            );
            assert_eq!(
                &GrantObjects::Tables(vec![ObjectName::from(vec![Ident::new("users")])]),
                &deny.objects
            );
            assert_eq_vec(&["analyst"], &deny.grantees);
            assert_eq!(Some(CascadeOption::Cascade), deny.cascade);
            assert_eq!(Some(Ident::from("admin")), deny.granted_by);
        }
        _ => unreachable!(),
    }

    verified_stmt("DENY SELECT, INSERT, UPDATE, DELETE ON db1.sc1 TO role1, role2");
    verified_stmt("DENY ALL ON db1.sc1 TO role1");
    verified_stmt("DENY EXEC ON my_sp TO runner");

    all_dialects_where(|d| d.identifier_quote_style("none") == Some('['))
        .verified_stmt("DENY SELECT ON [my_table] TO [public]");
}

#[test]
fn test_revoke() {
    let sql = "REVOKE ALL PRIVILEGES ON users, auth FROM analyst";
    match verified_stmt(sql) {
        Statement::Revoke {
            privileges,
            objects: Some(GrantObjects::Tables(tables)),
            grantees,
            granted_by,
            cascade,
        } => {
            assert_eq!(
                Privileges::All {
                    with_privileges_keyword: true
                },
                privileges
            );
            assert_eq_vec(&["users", "auth"], &tables);
            assert_eq_vec(&["analyst"], &grantees);
            assert_eq!(cascade, None);
            assert_eq!(None, granted_by);
        }
        _ => unreachable!(),
    }
}

#[test]
fn test_revoke_with_cascade() {
    let sql = "REVOKE ALL PRIVILEGES ON users, auth FROM analyst CASCADE";
    match all_dialects_except(|d| d.is::<MySqlDialect>()).verified_stmt(sql) {
        Statement::Revoke {
            privileges,
            objects: Some(GrantObjects::Tables(tables)),
            grantees,
            granted_by,
            cascade,
        } => {
            assert_eq!(
                Privileges::All {
                    with_privileges_keyword: true
                },
                privileges
            );
            assert_eq_vec(&["users", "auth"], &tables);
            assert_eq_vec(&["analyst"], &grantees);
            assert_eq!(cascade, Some(CascadeOption::Cascade));
            assert_eq!(None, granted_by);
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_merge() {
    let sql = "MERGE INTO s.bar AS dest USING (SELECT * FROM s.foo) AS stg ON dest.D = stg.D AND dest.E = stg.E WHEN NOT MATCHED THEN INSERT (A, B, C) VALUES (stg.A, stg.B, stg.C) WHEN MATCHED AND dest.A = 'a' THEN UPDATE SET dest.F = stg.F, dest.G = stg.G WHEN MATCHED THEN DELETE";
    let sql_no_into = "MERGE s.bar AS dest USING (SELECT * FROM s.foo) AS stg ON dest.D = stg.D AND dest.E = stg.E WHEN NOT MATCHED THEN INSERT (A, B, C) VALUES (stg.A, stg.B, stg.C) WHEN MATCHED AND dest.A = 'a' THEN UPDATE SET dest.F = stg.F, dest.G = stg.G WHEN MATCHED THEN DELETE";
    match (verified_stmt(sql), verified_stmt(sql_no_into)) {
        (
            Statement::Merge {
                into,
                table,
                source,
                on,
                clauses,
                ..
            },
            Statement::Merge {
                into: no_into,
                table: table_no_into,
                source: source_no_into,
                on: on_no_into,
                clauses: clauses_no_into,
                ..
            },
        ) => {
            assert!(into);
            assert!(!no_into);

            assert_eq!(
                table,
                TableFactor::Table {
                    name: ObjectName::from(vec![Ident::new("s"), Ident::new("bar")]),
                    alias: Some(TableAlias {
                        name: Ident::new("dest"),
                        columns: vec![],
                    }),
                    args: None,
                    with_hints: vec![],
                    version: None,
                    partitions: vec![],
                    with_ordinality: false,
                    json_path: None,
                    sample: None,
                    index_hints: vec![],
                }
            );
            assert_eq!(table, table_no_into);

            assert_eq!(
                source,
                TableFactor::Derived {
                    lateral: false,
                    subquery: Box::new(Query {
                        with: None,
                        body: Box::new(SetExpr::Select(Box::new(Select {
                            select_token: AttachedToken::empty(),
                            distinct: None,
                            top: None,
                            top_before_distinct: false,
                            projection: vec![SelectItem::Wildcard(
                                WildcardAdditionalOptions::default()
                            )],
                            exclude: None,
                            into: None,
                            from: vec![TableWithJoins {
                                relation: table_from_name(ObjectName::from(vec![
                                    Ident::new("s"),
                                    Ident::new("foo")
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
                            window_before_qualify: false,
                            qualify: None,
                            value_table_mode: None,
                            connect_by: None,
                            flavor: SelectFlavor::Standard,
                        }))),
                        order_by: None,
                        limit_clause: None,
                        fetch: None,
                        locks: vec![],
                        for_clause: None,
                        settings: None,
                        format_clause: None,
                        pipe_operators: vec![],
                    }),
                    alias: Some(TableAlias {
                        name: Ident {
                            value: "stg".to_string(),
                            quote_style: None,
                            span: Span::empty(),
                        },
                        columns: vec![],
                    }),
                }
            );
            assert_eq!(source, source_no_into);

            assert_eq!(
                on,
                Box::new(Expr::BinaryOp {
                    left: Box::new(Expr::BinaryOp {
                        left: Box::new(Expr::CompoundIdentifier(vec![
                            Ident::new("dest"),
                            Ident::new("D"),
                        ])),
                        op: BinaryOperator::Eq,
                        right: Box::new(Expr::CompoundIdentifier(vec![
                            Ident::new("stg"),
                            Ident::new("D"),
                        ])),
                    }),
                    op: BinaryOperator::And,
                    right: Box::new(Expr::BinaryOp {
                        left: Box::new(Expr::CompoundIdentifier(vec![
                            Ident::new("dest"),
                            Ident::new("E"),
                        ])),
                        op: BinaryOperator::Eq,
                        right: Box::new(Expr::CompoundIdentifier(vec![
                            Ident::new("stg"),
                            Ident::new("E"),
                        ])),
                    }),
                })
            );
            assert_eq!(on, on_no_into);

            assert_eq!(
                clauses,
                vec![
                    MergeClause {
                        clause_kind: MergeClauseKind::NotMatched,
                        predicate: None,
                        action: MergeAction::Insert(MergeInsertExpr {
                            columns: vec![Ident::new("A"), Ident::new("B"), Ident::new("C")],
                            kind: MergeInsertKind::Values(Values {
                                explicit_row: false,
                                rows: vec![vec![
                                    Expr::CompoundIdentifier(vec![
                                        Ident::new("stg"),
                                        Ident::new("A")
                                    ]),
                                    Expr::CompoundIdentifier(vec![
                                        Ident::new("stg"),
                                        Ident::new("B")
                                    ]),
                                    Expr::CompoundIdentifier(vec![
                                        Ident::new("stg"),
                                        Ident::new("C")
                                    ]),
                                ]]
                            }),
                        }),
                    },
                    MergeClause {
                        clause_kind: MergeClauseKind::Matched,
                        predicate: Some(Expr::BinaryOp {
                            left: Box::new(Expr::CompoundIdentifier(vec![
                                Ident::new("dest"),
                                Ident::new("A"),
                            ])),
                            op: BinaryOperator::Eq,
                            right: Box::new(Expr::Value(
                                (Value::SingleQuotedString("a".to_string())).with_empty_span()
                            )),
                        }),
                        action: MergeAction::Update {
                            assignments: vec![
                                Assignment {
                                    target: AssignmentTarget::ColumnName(ObjectName::from(vec![
                                        Ident::new("dest"),
                                        Ident::new("F")
                                    ])),
                                    value: Expr::CompoundIdentifier(vec![
                                        Ident::new("stg"),
                                        Ident::new("F"),
                                    ]),
                                },
                                Assignment {
                                    target: AssignmentTarget::ColumnName(ObjectName::from(vec![
                                        Ident::new("dest"),
                                        Ident::new("G")
                                    ])),
                                    value: Expr::CompoundIdentifier(vec![
                                        Ident::new("stg"),
                                        Ident::new("G"),
                                    ]),
                                },
                            ],
                        },
                    },
                    MergeClause {
                        clause_kind: MergeClauseKind::Matched,
                        predicate: None,
                        action: MergeAction::Delete,
                    },
                ]
            );
            assert_eq!(clauses, clauses_no_into);
        }
        _ => unreachable!(),
    };

    let sql = "MERGE INTO s.bar AS dest USING newArrivals AS S ON (1 > 1) WHEN NOT MATCHED THEN INSERT VALUES (stg.A, stg.B, stg.C)";
    verified_stmt(sql);
}

#[test]
fn test_merge_in_cte() {
    verified_only_select(
        "WITH x AS (\
            MERGE INTO t USING (VALUES (1)) ON 1 = 1 \
            WHEN MATCHED THEN DELETE \
            RETURNING *\
        ) SELECT * FROM x",
    );
}

#[test]
fn test_merge_with_returning() {
    let sql = "MERGE INTO wines AS w \
    USING wine_stock_changes AS s \
        ON s.winename = w.winename \
    WHEN NOT MATCHED AND s.stock_delta > 0 THEN INSERT VALUES (s.winename, s.stock_delta) \
    WHEN MATCHED AND w.stock + s.stock_delta > 0 THEN UPDATE SET stock = w.stock + s.stock_delta \
    WHEN MATCHED THEN DELETE \
    RETURNING merge_action(), w.*";
    verified_stmt(sql);
}

#[test]
fn test_merge_with_output() {
    let sql = "MERGE INTO target_table USING source_table \
        ON target_table.id = source_table.oooid \
        WHEN MATCHED THEN \
            UPDATE SET target_table.description = source_table.description \
        WHEN NOT MATCHED THEN \
            INSERT (ID, description) VALUES (source_table.id, source_table.description) \
        OUTPUT inserted.* INTO log_target";

    verified_stmt(sql);
}

#[test]
fn test_merge_with_output_without_into() {
    let sql = "MERGE INTO a USING b ON a.id = b.id \
        WHEN MATCHED THEN DELETE \
        OUTPUT inserted.*";
    verified_stmt(sql);
}

#[test]
fn test_merge_into_using_table() {
    let sql = "MERGE INTO target_table USING source_table \
        ON target_table.id = source_table.oooid \
        WHEN MATCHED THEN \
            UPDATE SET target_table.description = source_table.description \
        WHEN NOT MATCHED THEN \
            INSERT (ID, description) VALUES (source_table.id, source_table.description)";

    verified_stmt(sql);
}

#[test]
fn test_merge_with_delimiter() {
    let sql = "MERGE INTO target_table USING source_table \
    ON target_table.id = source_table.oooid \
    WHEN MATCHED THEN \
        UPDATE SET target_table.description = source_table.description \
    WHEN NOT MATCHED THEN \
        INSERT (ID, description) VALUES (source_table.id, source_table.description);";

    match parse_sql_statements(sql) {
        Ok(_) => {}
        _ => unreachable!(),
    }
}

#[test]
fn test_merge_invalid_statements() {
    let dialects = all_dialects();
    for (sql, err_msg) in [
        (
            "MERGE INTO T USING U ON TRUE WHEN NOT MATCHED THEN UPDATE SET a = b",
            "UPDATE is not allowed in a NOT MATCHED merge clause",
        ),
        (
            "MERGE INTO T USING U ON TRUE WHEN NOT MATCHED THEN DELETE",
            "DELETE is not allowed in a NOT MATCHED merge clause",
        ),
        (
            "MERGE INTO T USING U ON TRUE WHEN MATCHED THEN INSERT(a) VALUES(b)",
            "INSERT is not allowed in a MATCHED merge clause",
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
fn test_lock() {
    let sql = "SELECT * FROM student WHERE id = '1' FOR UPDATE";
    let mut ast = verified_query(sql);
    assert_eq!(ast.locks.len(), 1);
    let lock = ast.locks.pop().unwrap();
    assert_eq!(lock.lock_type, LockType::Update);
    assert!(lock.of.is_none());
    assert!(lock.nonblock.is_none());

    let sql = "SELECT * FROM student WHERE id = '1' FOR SHARE";
    let mut ast = verified_query(sql);
    assert_eq!(ast.locks.len(), 1);
    let lock = ast.locks.pop().unwrap();
    assert_eq!(lock.lock_type, LockType::Share);
    assert!(lock.of.is_none());
    assert!(lock.nonblock.is_none());
}

#[test]
fn test_lock_table() {
    let sql = "SELECT * FROM student WHERE id = '1' FOR UPDATE OF school";
    let mut ast = verified_query(sql);
    assert_eq!(ast.locks.len(), 1);
    let lock = ast.locks.pop().unwrap();
    assert_eq!(lock.lock_type, LockType::Update);
    assert_eq!(
        lock.of.unwrap(),
        ObjectName::from(vec![Ident {
            value: "school".to_string(),
            quote_style: None,
            span: Span::empty(),
        }])
    );
    assert!(lock.nonblock.is_none());

    let sql = "SELECT * FROM student WHERE id = '1' FOR SHARE OF school";
    let mut ast = verified_query(sql);
    assert_eq!(ast.locks.len(), 1);
    let lock = ast.locks.pop().unwrap();
    assert_eq!(lock.lock_type, LockType::Share);
    assert_eq!(
        lock.of.unwrap(),
        ObjectName::from(vec![Ident {
            value: "school".to_string(),
            quote_style: None,
            span: Span::empty(),
        }])
    );
    assert!(lock.nonblock.is_none());

    let sql = "SELECT * FROM student WHERE id = '1' FOR SHARE OF school FOR UPDATE OF student";
    let mut ast = verified_query(sql);
    assert_eq!(ast.locks.len(), 2);
    let lock = ast.locks.remove(0);
    assert_eq!(lock.lock_type, LockType::Share);
    assert_eq!(
        lock.of.unwrap(),
        ObjectName::from(vec![Ident {
            value: "school".to_string(),
            quote_style: None,
            span: Span::empty(),
        }])
    );
    assert!(lock.nonblock.is_none());
    let lock = ast.locks.remove(0);
    assert_eq!(lock.lock_type, LockType::Update);
    assert_eq!(
        lock.of.unwrap(),
        ObjectName::from(vec![Ident {
            value: "student".to_string(),
            quote_style: None,
            span: Span::empty(),
        }])
    );
    assert!(lock.nonblock.is_none());
}

#[test]
fn test_lock_nonblock() {
    let sql = "SELECT * FROM student WHERE id = '1' FOR UPDATE OF school SKIP LOCKED";
    let mut ast = verified_query(sql);
    assert_eq!(ast.locks.len(), 1);
    let lock = ast.locks.pop().unwrap();
    assert_eq!(lock.lock_type, LockType::Update);
    assert_eq!(
        lock.of.unwrap(),
        ObjectName::from(vec![Ident {
            value: "school".to_string(),
            quote_style: None,
            span: Span::empty(),
        }])
    );
    assert_eq!(lock.nonblock.unwrap(), NonBlock::SkipLocked);

    let sql = "SELECT * FROM student WHERE id = '1' FOR SHARE OF school NOWAIT";
    let mut ast = verified_query(sql);
    assert_eq!(ast.locks.len(), 1);
    let lock = ast.locks.pop().unwrap();
    assert_eq!(lock.lock_type, LockType::Share);
    assert_eq!(
        lock.of.unwrap(),
        ObjectName::from(vec![Ident {
            value: "school".to_string(),
            quote_style: None,
            span: Span::empty(),
        }])
    );
    assert_eq!(lock.nonblock.unwrap(), NonBlock::Nowait);
}

#[test]
fn test_placeholder() {
    let dialects = TestedDialects::new(vec![
        Box::new(GenericDialect {}),
        Box::new(DuckDbDialect {}),
        Box::new(PostgreSqlDialect {}),
        Box::new(MsSqlDialect {}),
        Box::new(AnsiDialect {}),
        Box::new(BigQueryDialect {}),
        Box::new(SnowflakeDialect {}),
        // Note: `$` is the starting word for the HiveDialect identifier
        // Box::new(sqlparser::dialect::HiveDialect {}),
    ]);
    let sql = "SELECT * FROM student WHERE id = $Id1";
    let ast = dialects.verified_only_select(sql);
    assert_eq!(
        ast.selection,
        Some(Expr::BinaryOp {
            left: Box::new(Expr::Identifier(Ident::new("id"))),
            op: BinaryOperator::Eq,
            right: Box::new(Expr::Value(
                (Value::Placeholder("$Id1".into())).with_empty_span()
            )),
        })
    );

    let ast = dialects.verified_query("SELECT * FROM student LIMIT $1 OFFSET $2");
    let expected_limit_clause = LimitClause::LimitOffset {
        limit: Some(Expr::Value(
            (Value::Placeholder("$1".into())).with_empty_span(),
        )),
        offset: Some(Offset {
            value: Expr::Value((Value::Placeholder("$2".into())).with_empty_span()),
            rows: OffsetRows::None,
        }),
        limit_by: vec![],
    };
    assert_eq!(ast.limit_clause, Some(expected_limit_clause));

    let dialects = TestedDialects::new(vec![
        Box::new(GenericDialect {}),
        Box::new(DuckDbDialect {}),
        // Note: `?` is for jsonb operators in PostgreSqlDialect
        // Box::new(PostgreSqlDialect {}),
        Box::new(MsSqlDialect {}),
        Box::new(AnsiDialect {}),
        Box::new(BigQueryDialect {}),
        Box::new(SnowflakeDialect {}),
        // Note: `$` is the starting word for the HiveDialect identifier
        // Box::new(sqlparser::dialect::HiveDialect {}),
    ]);
    let sql = "SELECT * FROM student WHERE id = ?";
    let ast = dialects.verified_only_select(sql);
    assert_eq!(
        ast.selection,
        Some(Expr::BinaryOp {
            left: Box::new(Expr::Identifier(Ident::new("id"))),
            op: BinaryOperator::Eq,
            right: Box::new(Expr::Value(
                (Value::Placeholder("?".into())).with_empty_span()
            )),
        })
    );

    let sql = "SELECT $fromage_français, :x, ?123";
    let ast = dialects.verified_only_select(sql);
    assert_eq!(
        ast.projection,
        vec![
            UnnamedExpr(Expr::Value(
                (Value::Placeholder("$fromage_français".into())).with_empty_span()
            )),
            UnnamedExpr(Expr::Value(
                (Value::Placeholder(":x".into())).with_empty_span()
            )),
            UnnamedExpr(Expr::Value(
                (Value::Placeholder("?123".into())).with_empty_span()
            )),
        ]
    );
}

#[test]
fn all_keywords_sorted() {
    // assert!(ALL_KEYWORDS.is_sorted())
    let mut copy = Vec::from(ALL_KEYWORDS);
    copy.sort_unstable();
    assert_eq!(copy, ALL_KEYWORDS)
}

fn parse_sql_statements(sql: &str) -> Result<Vec<Statement>, ParserError> {
    all_dialects().parse_sql_statements(sql)
}

fn one_statement_parses_to(sql: &str, canonical: &str) -> Statement {
    all_dialects().one_statement_parses_to(sql, canonical)
}

fn verified_stmt(query: &str) -> Statement {
    all_dialects().verified_stmt(query)
}

fn verified_query(query: &str) -> Query {
    all_dialects().verified_query(query)
}

fn verified_only_select(query: &str) -> Select {
    all_dialects().verified_only_select(query)
}

fn verified_expr(query: &str) -> Expr {
    all_dialects().verified_expr(query)
}

#[test]
fn parse_offset_and_limit() {
    let sql = "SELECT foo FROM bar LIMIT 1 OFFSET 2";
    let expected_limit_clause = Some(LimitClause::LimitOffset {
        limit: Some(Expr::value(number("1"))),
        offset: Some(Offset {
            value: Expr::value(number("2")),
            rows: OffsetRows::None,
        }),
        limit_by: vec![],
    });
    let ast = verified_query(sql);
    assert_eq!(ast.limit_clause, expected_limit_clause);

    // different order is OK
    one_statement_parses_to("SELECT foo FROM bar OFFSET 2 LIMIT 1", sql);

    // mysql syntax is ok for some dialects
    all_dialects_where(|d| d.supports_limit_comma())
        .verified_query("SELECT foo FROM bar LIMIT 2, 1");

    // expressions are allowed
    let sql = "SELECT foo FROM bar LIMIT 1 + 2 OFFSET 3 * 4";
    let ast = verified_query(sql);
    let expected_limit_clause = LimitClause::LimitOffset {
        limit: Some(Expr::BinaryOp {
            left: Box::new(Expr::value(number("1"))),
            op: BinaryOperator::Plus,
            right: Box::new(Expr::value(number("2"))),
        }),
        offset: Some(Offset {
            value: Expr::BinaryOp {
                left: Box::new(Expr::value(number("3"))),
                op: BinaryOperator::Multiply,
                right: Box::new(Expr::value(number("4"))),
            },
            rows: OffsetRows::None,
        }),
        limit_by: vec![],
    };
    assert_eq!(ast.limit_clause, Some(expected_limit_clause),);

    // OFFSET without LIMIT
    verified_stmt("SELECT foo FROM bar OFFSET 2");

    // Can't repeat OFFSET / LIMIT
    let res = parse_sql_statements("SELECT foo FROM bar OFFSET 2 OFFSET 2");
    assert_eq!(
        ParserError::ParserError("Expected: end of statement, found: OFFSET".to_string()),
        res.unwrap_err()
    );

    let res = parse_sql_statements("SELECT foo FROM bar LIMIT 2 LIMIT 2");
    assert_eq!(
        ParserError::ParserError("Expected: end of statement, found: LIMIT".to_string()),
        res.unwrap_err()
    );

    let res = parse_sql_statements("SELECT foo FROM bar OFFSET 2 LIMIT 2 OFFSET 2");
    assert_eq!(
        ParserError::ParserError("Expected: end of statement, found: OFFSET".to_string()),
        res.unwrap_err()
    );
}

#[test]
fn parse_time_functions() {
    fn test_time_function(func_name: &'static str) {
        let sql = format!("SELECT {func_name}()");
        let select = verified_only_select(&sql);
        let select_localtime_func_call_ast = Function {
            name: ObjectName::from(vec![Ident::new(func_name)]),
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
        };
        assert_eq!(
            &Expr::Function(select_localtime_func_call_ast.clone()),
            expr_from_projection(&select.projection[0])
        );

        // Validating Parenthesis
        let sql_without_parens = format!("SELECT {func_name}");
        let mut ast_without_parens = select_localtime_func_call_ast;
        ast_without_parens.args = FunctionArguments::None;
        assert_eq!(
            &Expr::Function(ast_without_parens),
            expr_from_projection(&verified_only_select(&sql_without_parens).projection[0])
        );
    }

    test_time_function("CURRENT_TIMESTAMP");
    test_time_function("CURRENT_TIME");
    test_time_function("CURRENT_DATE");
    test_time_function("LOCALTIME");
    test_time_function("LOCALTIMESTAMP");
}

#[test]
fn parse_position() {
    assert_eq!(
        Expr::Position {
            expr: Box::new(Expr::Value(
                (Value::SingleQuotedString("@".to_string())).with_empty_span()
            )),
            r#in: Box::new(Expr::Identifier(Ident::new("field"))),
        },
        verified_expr("POSITION('@' IN field)"),
    );

    // some dialects (e.g. snowflake) support position as a function call (i.e. without IN)
    assert_eq!(
        call(
            "position",
            [
                Expr::Value((Value::SingleQuotedString("an".to_owned())).with_empty_span()),
                Expr::Value((Value::SingleQuotedString("banana".to_owned())).with_empty_span()),
                Expr::value(number("1")),
            ]
        ),
        verified_expr("position('an', 'banana', 1)")
    );
}

#[test]
fn parse_position_negative() {
    let sql = "SELECT POSITION(foo IN) from bar";
    let res = parse_sql_statements(sql);
    assert_eq!(
        ParserError::ParserError("Expected: (, found: )".to_string()),
        res.unwrap_err()
    );
}

#[test]
fn parse_is_boolean() {
    use self::Expr::*;

    let sql = "a IS TRUE";
    assert_eq!(
        IsTrue(Box::new(Identifier(Ident::new("a")))),
        verified_expr(sql)
    );

    let sql = "a IS NOT TRUE";
    assert_eq!(
        IsNotTrue(Box::new(Identifier(Ident::new("a")))),
        verified_expr(sql)
    );

    let sql = "a IS FALSE";
    assert_eq!(
        IsFalse(Box::new(Identifier(Ident::new("a")))),
        verified_expr(sql)
    );

    let sql = "a IS NOT FALSE";
    assert_eq!(
        IsNotFalse(Box::new(Identifier(Ident::new("a")))),
        verified_expr(sql)
    );

    let sql = "a IS NORMALIZED";
    assert_eq!(
        IsNormalized {
            expr: Box::new(Identifier(Ident::new("a"))),
            form: None,
            negated: false,
        },
        verified_expr(sql)
    );

    let sql = "a IS NOT NORMALIZED";
    assert_eq!(
        IsNormalized {
            expr: Box::new(Identifier(Ident::new("a"))),
            form: None,
            negated: true,
        },
        verified_expr(sql)
    );

    let sql = "a IS NFKC NORMALIZED";
    assert_eq!(
        IsNormalized {
            expr: Box::new(Identifier(Ident::new("a"))),
            form: Some(NormalizationForm::NFKC),
            negated: false,
        },
        verified_expr(sql)
    );

    let sql = "a IS NOT NFKD NORMALIZED";
    assert_eq!(
        IsNormalized {
            expr: Box::new(Identifier(Ident::new("a"))),
            form: Some(NormalizationForm::NFKD),
            negated: true,
        },
        verified_expr(sql)
    );

    let sql = "a IS UNKNOWN";
    assert_eq!(
        IsUnknown(Box::new(Identifier(Ident::new("a")))),
        verified_expr(sql)
    );

    let sql = "a IS NOT UNKNOWN";
    assert_eq!(
        IsNotUnknown(Box::new(Identifier(Ident::new("a")))),
        verified_expr(sql)
    );

    verified_stmt("SELECT f FROM foo WHERE field IS TRUE");
    verified_stmt("SELECT f FROM foo WHERE field IS NOT TRUE");

    verified_stmt("SELECT f FROM foo WHERE field IS FALSE");
    verified_stmt("SELECT f FROM foo WHERE field IS NOT FALSE");

    verified_stmt("SELECT f FROM foo WHERE field IS NORMALIZED");
    verified_stmt("SELECT f FROM foo WHERE field IS NFC NORMALIZED");
    verified_stmt("SELECT f FROM foo WHERE field IS NFD NORMALIZED");
    verified_stmt("SELECT f FROM foo WHERE field IS NOT NORMALIZED");
    verified_stmt("SELECT f FROM foo WHERE field IS NOT NFKC NORMALIZED");

    verified_stmt("SELECT f FROM foo WHERE field IS UNKNOWN");
    verified_stmt("SELECT f FROM foo WHERE field IS NOT UNKNOWN");

    let sql = "SELECT f from foo where field is 0";
    let res = parse_sql_statements(sql);
    assert_eq!(
        ParserError::ParserError(
            "Expected: [NOT] NULL | TRUE | FALSE | DISTINCT | [form] NORMALIZED FROM after IS, found: 0"
                .to_string()
        ),
        res.unwrap_err()
    );

    let sql = "SELECT s, s IS XYZ NORMALIZED FROM foo";
    let res = parse_sql_statements(sql);
    assert_eq!(
        ParserError::ParserError(
            "Expected: [NOT] NULL | TRUE | FALSE | DISTINCT | [form] NORMALIZED FROM after IS, found: XYZ"
                .to_string()
        ),
        res.unwrap_err()
    );

    let sql = "SELECT s, s IS NFKC FROM foo";
    let res = parse_sql_statements(sql);
    assert_eq!(
        ParserError::ParserError(
            "Expected: [NOT] NULL | TRUE | FALSE | DISTINCT | [form] NORMALIZED FROM after IS, found: FROM"
                .to_string()
        ),
        res.unwrap_err()
    );

    let sql = "SELECT s, s IS TRIM(' NFKC ') FROM foo";
    let res = parse_sql_statements(sql);
    assert_eq!(
        ParserError::ParserError(
            "Expected: [NOT] NULL | TRUE | FALSE | DISTINCT | [form] NORMALIZED FROM after IS, found: TRIM"
                .to_string()
        ),
        res.unwrap_err()
    );
}

#[test]
fn parse_discard() {
    let sql = "DISCARD ALL";
    match verified_stmt(sql) {
        Statement::Discard { object_type, .. } => assert_eq!(object_type, DiscardObject::ALL),
        _ => unreachable!(),
    }

    let sql = "DISCARD PLANS";
    match verified_stmt(sql) {
        Statement::Discard { object_type, .. } => assert_eq!(object_type, DiscardObject::PLANS),
        _ => unreachable!(),
    }

    let sql = "DISCARD SEQUENCES";
    match verified_stmt(sql) {
        Statement::Discard { object_type, .. } => assert_eq!(object_type, DiscardObject::SEQUENCES),
        _ => unreachable!(),
    }

    let sql = "DISCARD TEMP";
    match verified_stmt(sql) {
        Statement::Discard { object_type, .. } => assert_eq!(object_type, DiscardObject::TEMP),
        _ => unreachable!(),
    }
}

#[test]
fn parse_cursor() {
    let sql = r#"CLOSE my_cursor"#;
    match verified_stmt(sql) {
        Statement::Close { cursor } => assert_eq!(
            cursor,
            CloseCursor::Specific {
                name: Ident::new("my_cursor"),
            }
        ),
        _ => unreachable!(),
    }

    let sql = r#"CLOSE ALL"#;
    match verified_stmt(sql) {
        Statement::Close { cursor } => assert_eq!(cursor, CloseCursor::All),
        _ => unreachable!(),
    }
}

#[test]
fn parse_show_functions() {
    assert_eq!(
        verified_stmt("SHOW FUNCTIONS LIKE 'pattern'"),
        Statement::ShowFunctions {
            filter: Some(ShowStatementFilter::Like("pattern".into())),
        }
    );
}

#[test]
fn parse_cache_table() {
    let sql = "SELECT a, b, c FROM foo";
    let cache_table_name = "cache_table_name";
    let table_flag = "flag";
    let query = all_dialects().verified_query(sql);

    assert_eq!(
        verified_stmt(format!("CACHE TABLE '{cache_table_name}'").as_str()),
        Statement::Cache {
            table_flag: None,
            table_name: ObjectName::from(vec![Ident::with_quote('\'', cache_table_name)]),
            has_as: false,
            options: vec![],
            query: None,
        }
    );

    assert_eq!(
        verified_stmt(format!("CACHE {table_flag} TABLE '{cache_table_name}'").as_str()),
        Statement::Cache {
            table_flag: Some(ObjectName::from(vec![Ident::new(table_flag)])),
            table_name: ObjectName::from(vec![Ident::with_quote('\'', cache_table_name)]),
            has_as: false,
            options: vec![],
            query: None,
        }
    );

    assert_eq!(
        verified_stmt(
            format!(
                "CACHE {table_flag} TABLE '{cache_table_name}' OPTIONS('K1' = 'V1', 'K2' = 0.88)",
            )
            .as_str()
        ),
        Statement::Cache {
            table_flag: Some(ObjectName::from(vec![Ident::new(table_flag)])),
            table_name: ObjectName::from(vec![Ident::with_quote('\'', cache_table_name)]),
            has_as: false,
            options: vec![
                SqlOption::KeyValue {
                    key: Ident::with_quote('\'', "K1"),
                    value: Expr::Value((Value::SingleQuotedString("V1".into())).with_empty_span()),
                },
                SqlOption::KeyValue {
                    key: Ident::with_quote('\'', "K2"),
                    value: Expr::value(number("0.88")),
                },
            ],
            query: None,
        }
    );

    assert_eq!(
        verified_stmt(
            format!(
                "CACHE {table_flag} TABLE '{cache_table_name}' OPTIONS('K1' = 'V1', 'K2' = 0.88) {sql}",
            )
                .as_str()
        ),
        Statement::Cache {
            table_flag: Some(ObjectName::from(vec![Ident::new(table_flag)])),
            table_name: ObjectName::from(vec![Ident::with_quote('\'', cache_table_name)]),
            has_as: false,
            options: vec![
                SqlOption::KeyValue {
                    key: Ident::with_quote('\'', "K1"),
                    value: Expr::Value((Value::SingleQuotedString("V1".into())).with_empty_span()),
                },
                SqlOption::KeyValue {
                    key: Ident::with_quote('\'', "K2"),
                    value: Expr::value(number("0.88")),
                },
            ],
            query: Some(query.clone().into()),
        }
    );

    assert_eq!(
        verified_stmt(
            format!(
                "CACHE {table_flag} TABLE '{cache_table_name}' OPTIONS('K1' = 'V1', 'K2' = 0.88) AS {sql}",
            )
                .as_str()
        ),
        Statement::Cache {
            table_flag: Some(ObjectName::from(vec![Ident::new(table_flag)])),
            table_name: ObjectName::from(vec![Ident::with_quote('\'', cache_table_name)]),
            has_as: true,
            options: vec![
                SqlOption::KeyValue {
                    key: Ident::with_quote('\'', "K1"),
                    value: Expr::Value((Value::SingleQuotedString("V1".into())).with_empty_span()),
                },
                SqlOption::KeyValue {
                    key: Ident::with_quote('\'', "K2"),
                    value: Expr::value(number("0.88")),
                },
            ],
            query: Some(query.clone().into()),
        }
    );

    assert_eq!(
        verified_stmt(format!("CACHE {table_flag} TABLE '{cache_table_name}' {sql}").as_str()),
        Statement::Cache {
            table_flag: Some(ObjectName::from(vec![Ident::new(table_flag)])),
            table_name: ObjectName::from(vec![Ident::with_quote('\'', cache_table_name)]),
            has_as: false,
            options: vec![],
            query: Some(query.clone().into()),
        }
    );

    assert_eq!(
        verified_stmt(format!("CACHE {table_flag} TABLE '{cache_table_name}' AS {sql}").as_str()),
        Statement::Cache {
            table_flag: Some(ObjectName::from(vec![Ident::new(table_flag)])),
            table_name: ObjectName::from(vec![Ident::with_quote('\'', cache_table_name)]),
            has_as: true,
            options: vec![],
            query: Some(query.into()),
        }
    );

    let res = parse_sql_statements("CACHE TABLE 'table_name' foo");
    assert_eq!(
        ParserError::ParserError(
            "Expected: SELECT, VALUES, or a subquery in the query body, found: foo".to_string()
        ),
        res.unwrap_err()
    );

    let res = parse_sql_statements("CACHE flag TABLE 'table_name' OPTIONS('K1'='V1') foo");
    assert_eq!(
        ParserError::ParserError(
            "Expected: SELECT, VALUES, or a subquery in the query body, found: foo".to_string()
        ),
        res.unwrap_err()
    );

    let res = parse_sql_statements("CACHE TABLE 'table_name' AS foo");
    assert_eq!(
        ParserError::ParserError(
            "Expected: SELECT, VALUES, or a subquery in the query body, found: foo".to_string()
        ),
        res.unwrap_err()
    );

    let res = parse_sql_statements("CACHE flag TABLE 'table_name' OPTIONS('K1'='V1') AS foo");
    assert_eq!(
        ParserError::ParserError(
            "Expected: SELECT, VALUES, or a subquery in the query body, found: foo".to_string()
        ),
        res.unwrap_err()
    );

    let res = parse_sql_statements("CACHE 'table_name'");
    assert_eq!(
        ParserError::ParserError("Expected: a `TABLE` keyword, found: 'table_name'".to_string()),
        res.unwrap_err()
    );

    let res = parse_sql_statements("CACHE 'table_name' OPTIONS('K1'='V1')");
    assert_eq!(
        ParserError::ParserError("Expected: a `TABLE` keyword, found: OPTIONS".to_string()),
        res.unwrap_err()
    );

    let res = parse_sql_statements("CACHE flag 'table_name' OPTIONS('K1'='V1')");
    assert_eq!(
        ParserError::ParserError("Expected: a `TABLE` keyword, found: 'table_name'".to_string()),
        res.unwrap_err()
    );
}

#[test]
fn parse_uncache_table() {
    assert_eq!(
        verified_stmt("UNCACHE TABLE 'table_name'"),
        Statement::UNCache {
            table_name: ObjectName::from(vec![Ident::with_quote('\'', "table_name")]),
            if_exists: false,
        }
    );

    assert_eq!(
        verified_stmt("UNCACHE TABLE IF EXISTS 'table_name'"),
        Statement::UNCache {
            table_name: ObjectName::from(vec![Ident::with_quote('\'', "table_name")]),
            if_exists: true,
        }
    );

    let res = parse_sql_statements("UNCACHE TABLE 'table_name' foo");
    assert_eq!(
        ParserError::ParserError("Expected: end of statement, found: foo".to_string()),
        res.unwrap_err()
    );

    let res = parse_sql_statements("UNCACHE 'table_name' foo");
    assert_eq!(
        ParserError::ParserError("Expected: TABLE, found: 'table_name'".to_string()),
        res.unwrap_err()
    );

    let res = parse_sql_statements("UNCACHE IF EXISTS 'table_name' foo");
    assert_eq!(
        ParserError::ParserError("Expected: TABLE, found: IF".to_string()),
        res.unwrap_err()
    );
}

#[test]
fn parse_deeply_nested_parens_hits_recursion_limits() {
    let sql = "(".repeat(1000);
    let res = parse_sql_statements(&sql);
    assert_eq!(ParserError::RecursionLimitExceeded, res.unwrap_err());
}

#[test]
fn parse_deeply_nested_unary_op_hits_recursion_limits() {
    let sql = format!("SELECT {}", "+".repeat(1000));
    let res = parse_sql_statements(&sql);
    assert_eq!(ParserError::RecursionLimitExceeded, res.unwrap_err());
}

#[test]
fn parse_deeply_nested_expr_hits_recursion_limits() {
    let dialect = GenericDialect {};

    let where_clause = make_where_clause(100);
    let sql = format!("SELECT id, user_id FROM test WHERE {where_clause}");

    let res = Parser::new(&dialect)
        .try_with_sql(&sql)
        .expect("tokenize to work")
        .parse_statements();

    assert_eq!(res, Err(ParserError::RecursionLimitExceeded));
}

#[test]
fn parse_deeply_nested_subquery_expr_hits_recursion_limits() {
    let dialect = GenericDialect {};

    let where_clause = make_where_clause(100);
    let sql = format!("SELECT id, user_id where id IN (select id from t WHERE {where_clause})");

    let res = Parser::new(&dialect)
        .try_with_sql(&sql)
        .expect("tokenize to work")
        .parse_statements();

    assert_eq!(res, Err(ParserError::RecursionLimitExceeded));
}

#[test]
fn parse_with_recursion_limit() {
    let dialect = GenericDialect {};

    let where_clause = make_where_clause(20);
    let sql = format!("SELECT id, user_id FROM test WHERE {where_clause}");

    // Expect the statement to parse with default limit
    let res = Parser::new(&dialect)
        .try_with_sql(&sql)
        .expect("tokenize to work")
        .parse_statements();

    assert!(res.is_ok(), "{res:?}");

    // limit recursion to something smaller, expect parsing to fail
    let res = Parser::new(&dialect)
        .try_with_sql(&sql)
        .expect("tokenize to work")
        .with_recursion_limit(20)
        .parse_statements();

    assert_eq!(res, Err(ParserError::RecursionLimitExceeded));

    // limit recursion to 50, expect it to succeed
    let res = Parser::new(&dialect)
        .try_with_sql(&sql)
        .expect("tokenize to work")
        .with_recursion_limit(50)
        .parse_statements();

    assert!(res.is_ok(), "{res:?}");
}

#[test]
fn parse_escaped_string_with_unescape() {
    fn assert_mysql_query_value(dialects: &TestedDialects, sql: &str, quoted: &str) {
        match dialects.one_statement_parses_to(sql, "") {
            Statement::Query(query) => match *query.body {
                SetExpr::Select(value) => {
                    let expr = expr_from_projection(only(&value.projection));
                    assert_eq!(
                        *expr,
                        Expr::Value(
                            (Value::SingleQuotedString(quoted.to_string())).with_empty_span()
                        )
                    );
                }
                _ => unreachable!(),
            },
            _ => unreachable!(),
        };
    }

    let escaping_dialects =
        &all_dialects_where(|dialect| dialect.supports_string_literal_backslash_escape());
    let no_wildcard_exception = &all_dialects_where(|dialect| {
        dialect.supports_string_literal_backslash_escape() && !dialect.ignores_wildcard_escapes()
    });
    let with_wildcard_exception = &all_dialects_where(|dialect| {
        dialect.supports_string_literal_backslash_escape() && dialect.ignores_wildcard_escapes()
    });

    let sql = r"SELECT 'I\'m fine'";
    assert_mysql_query_value(escaping_dialects, sql, "I'm fine");

    let sql = r#"SELECT 'I''m fine'"#;
    assert_mysql_query_value(escaping_dialects, sql, "I'm fine");

    let sql = r#"SELECT 'I\"m fine'"#;
    assert_mysql_query_value(escaping_dialects, sql, "I\"m fine");

    let sql = r"SELECT 'Testing: \0 \\ \% \_ \b \n \r \t \Z \a \h \ '";
    assert_mysql_query_value(
        no_wildcard_exception,
        sql,
        "Testing: \0 \\ % _ \u{8} \n \r \t \u{1a} \u{7} h  ",
    );

    // check MySQL doesn't remove backslash from escaped LIKE wildcards
    assert_mysql_query_value(
        with_wildcard_exception,
        sql,
        "Testing: \0 \\ \\% \\_ \u{8} \n \r \t \u{1a} \u{7} h  ",
    );
}

#[test]
fn parse_escaped_string_without_unescape() {
    fn assert_mysql_query_value(sql: &str, quoted: &str) {
        let stmt = TestedDialects::new_with_options(
            vec![
                Box::new(MySqlDialect {}),
                Box::new(BigQueryDialect {}),
                Box::new(SnowflakeDialect {}),
            ],
            ParserOptions::new().with_unescape(false),
        )
        .one_statement_parses_to(sql, "");

        match stmt {
            Statement::Query(query) => match *query.body {
                SetExpr::Select(value) => {
                    let expr = expr_from_projection(only(&value.projection));
                    assert_eq!(
                        *expr,
                        Expr::Value(
                            (Value::SingleQuotedString(quoted.to_string())).with_empty_span()
                        )
                    );
                }
                _ => unreachable!(),
            },
            _ => unreachable!(),
        };
    }
    let sql = r"SELECT 'I\'m fine'";
    assert_mysql_query_value(sql, r"I\'m fine");

    let sql = r#"SELECT 'I''m fine'"#;
    assert_mysql_query_value(sql, r#"I''m fine"#);

    let sql = r#"SELECT 'I\"m fine'"#;
    assert_mysql_query_value(sql, r#"I\"m fine"#);

    let sql = r"SELECT 'Testing: \0 \\ \% \_ \b \n \r \t \Z \a \ '";
    assert_mysql_query_value(sql, r"Testing: \0 \\ \% \_ \b \n \r \t \Z \a \ ");
}

#[test]
fn parse_pivot_table() {
    let sql = concat!(
        "SELECT * FROM monthly_sales AS a PIVOT(",
        "SUM(a.amount), ",
        "SUM(b.amount) AS t, ",
        "SUM(c.amount) AS u ",
        "FOR a.MONTH IN (1 AS x, 'two', three AS y)) AS p (c, d) ",
        "ORDER BY EMPID"
    );

    fn expected_function(table: &'static str, alias: Option<&'static str>) -> ExprWithAlias {
        ExprWithAlias {
            expr: call(
                "SUM",
                [Expr::CompoundIdentifier(vec![
                    Ident::new(table),
                    Ident::new("amount"),
                ])],
            ),
            alias: alias.map(Ident::new),
        }
    }

    assert_eq!(
        verified_only_select(sql).from[0].relation,
        Pivot {
            table: Box::new(TableFactor::Table {
                name: ObjectName::from(vec![Ident::new("monthly_sales")]),
                alias: Some(TableAlias {
                    name: Ident::new("a"),
                    columns: vec![]
                }),
                args: None,
                with_hints: vec![],
                version: None,
                partitions: vec![],
                with_ordinality: false,
                json_path: None,
                sample: None,
                index_hints: vec![],
            }),
            aggregate_functions: vec![
                expected_function("a", None),
                expected_function("b", Some("t")),
                expected_function("c", Some("u")),
            ],
            value_column: vec![Expr::CompoundIdentifier(vec![
                Ident::new("a"),
                Ident::new("MONTH")
            ])],
            value_source: PivotValueSource::List(vec![
                ExprWithAlias {
                    expr: Expr::value(number("1")),
                    alias: Some(Ident::new("x"))
                },
                ExprWithAlias {
                    expr: Expr::Value(
                        (Value::SingleQuotedString("two".to_string())).with_empty_span()
                    ),
                    alias: None
                },
                ExprWithAlias {
                    expr: Expr::Identifier(Ident::new("three")),
                    alias: Some(Ident::new("y"))
                },
            ]),
            default_on_null: None,
            alias: Some(TableAlias {
                name: Ident {
                    value: "p".to_string(),
                    quote_style: None,
                    span: Span::empty(),
                },
                columns: vec![
                    TableAliasColumnDef::from_name("c"),
                    TableAliasColumnDef::from_name("d"),
                ],
            }),
        }
    );
    assert_eq!(verified_stmt(sql).to_string(), sql);

    // parsing should succeed with empty alias
    let sql_without_table_alias = concat!(
        "SELECT * FROM monthly_sales ",
        "PIVOT(SUM(a.amount) FOR a.MONTH IN ('JAN', 'FEB', 'MAR', 'APR')) AS p (c, d) ",
        "ORDER BY EMPID"
    );
    assert_matches!(
        &verified_only_select(sql_without_table_alias).from[0].relation,
        Pivot { table, .. } if matches!(&**table, TableFactor::Table { alias: None, .. })
    );
    assert_eq!(
        verified_stmt(sql_without_table_alias).to_string(),
        sql_without_table_alias
    );

    let multiple_value_columns_sql = concat!(
        "SELECT * FROM person ",
        "PIVOT(",
        "SUM(age) AS a, AVG(class) AS c ",
        "FOR (name, age) IN (('John', 30) AS c1, ('Mike', 40) AS c2))",
    );

    assert_eq!(
        verified_only_select(multiple_value_columns_sql).from[0].relation,
        Pivot {
            table: Box::new(TableFactor::Table {
                name: ObjectName::from(vec![Ident::new("person")]),
                alias: None,
                args: None,
                with_hints: vec![],
                version: None,
                partitions: vec![],
                with_ordinality: false,
                json_path: None,
                sample: None,
                index_hints: vec![],
            }),
            aggregate_functions: vec![
                ExprWithAlias {
                    expr: call("SUM", [Expr::Identifier(Ident::new("age"))]),
                    alias: Some(Ident::new("a"))
                },
                ExprWithAlias {
                    expr: call("AVG", [Expr::Identifier(Ident::new("class"))]),
                    alias: Some(Ident::new("c"))
                },
            ],
            value_column: vec![
                Expr::Identifier(Ident::new("name")),
                Expr::Identifier(Ident::new("age")),
            ],
            value_source: PivotValueSource::List(vec![
                ExprWithAlias {
                    expr: Expr::Tuple(vec![
                        Expr::Value(
                            (Value::SingleQuotedString("John".to_string())).with_empty_span()
                        ),
                        Expr::Value(
                            (Value::Number("30".parse().unwrap(), false)).with_empty_span()
                        ),
                    ]),
                    alias: Some(Ident::new("c1"))
                },
                ExprWithAlias {
                    expr: Expr::Tuple(vec![
                        Expr::Value(
                            (Value::SingleQuotedString("Mike".to_string())).with_empty_span()
                        ),
                        Expr::Value(
                            (Value::Number("40".parse().unwrap(), false)).with_empty_span()
                        ),
                    ]),
                    alias: Some(Ident::new("c2"))
                },
            ]),
            default_on_null: None,
            alias: None,
        }
    );
    assert_eq!(
        verified_stmt(multiple_value_columns_sql).to_string(),
        multiple_value_columns_sql
    );
}

#[test]
fn parse_unpivot_table() {
    let sql = concat!(
        "SELECT * FROM sales AS s ",
        "UNPIVOT(quantity FOR quarter IN (Q1, Q2, Q3, Q4)) AS u (product, quarter, quantity)"
    );
    let base_unpivot = Unpivot {
        table: Box::new(TableFactor::Table {
            name: ObjectName::from(vec![Ident::new("sales")]),
            alias: Some(TableAlias {
                name: Ident::new("s"),
                columns: vec![],
            }),
            args: None,
            with_hints: vec![],
            version: None,
            partitions: vec![],
            with_ordinality: false,
            json_path: None,
            sample: None,
            index_hints: vec![],
        }),
        null_inclusion: None,
        value: Expr::Identifier(Ident::new("quantity")),
        name: Ident::new("quarter"),
        columns: ["Q1", "Q2", "Q3", "Q4"]
            .into_iter()
            .map(|col| ExprWithAlias {
                expr: Expr::Identifier(Ident::new(col)),
                alias: None,
            })
            .collect(),
        alias: Some(TableAlias {
            name: Ident::new("u"),
            columns: ["product", "quarter", "quantity"]
                .into_iter()
                .map(TableAliasColumnDef::from_name)
                .collect(),
        }),
    };
    pretty_assertions::assert_eq!(verified_only_select(sql).from[0].relation, base_unpivot);
    assert_eq!(verified_stmt(sql).to_string(), sql);

    let sql_without_aliases = concat!(
        "SELECT * FROM sales ",
        "UNPIVOT(quantity FOR quarter IN (Q1, Q2, Q3, Q4))"
    );

    assert_matches!(
        &verified_only_select(sql_without_aliases).from[0].relation,
        Unpivot {
            table,
            alias: None,
            ..
        } if matches!(&**table, TableFactor::Table { alias: None, .. })
    );
    assert_eq!(
        verified_stmt(sql_without_aliases).to_string(),
        sql_without_aliases
    );

    let sql_unpivot_exclude_nulls = concat!(
    "SELECT * FROM sales AS s ",
    "UNPIVOT EXCLUDE NULLS (quantity FOR quarter IN (Q1, Q2, Q3, Q4)) AS u (product, quarter, quantity)"
    );

    if let Unpivot { null_inclusion, .. } =
        &verified_only_select(sql_unpivot_exclude_nulls).from[0].relation
    {
        assert_eq!(*null_inclusion, Some(NullInclusion::ExcludeNulls));
    }

    assert_eq!(
        verified_stmt(sql_unpivot_exclude_nulls).to_string(),
        sql_unpivot_exclude_nulls
    );

    let sql_unpivot_include_nulls = concat!(
        "SELECT * FROM sales AS s ",
        "UNPIVOT INCLUDE NULLS (quantity FOR quarter IN (Q1, Q2, Q3, Q4)) AS u (product, quarter, quantity)"
    );

    if let Unpivot { null_inclusion, .. } =
        &verified_only_select(sql_unpivot_include_nulls).from[0].relation
    {
        assert_eq!(*null_inclusion, Some(NullInclusion::IncludeNulls));
    }

    assert_eq!(
        verified_stmt(sql_unpivot_include_nulls).to_string(),
        sql_unpivot_include_nulls
    );

    let sql_unpivot_with_alias = concat!(
        "SELECT * FROM sales AS s ",
        "UNPIVOT INCLUDE NULLS ",
        "(quantity FOR quarter IN ",
        "(Q1 AS Quater1, Q2 AS Quater2, Q3 AS Quater3, Q4 AS Quater4)) ",
        "AS u (product, quarter, quantity)"
    );

    if let Unpivot { value, columns, .. } =
        &verified_only_select(sql_unpivot_with_alias).from[0].relation
    {
        assert_eq!(
            *columns,
            vec![
                ExprWithAlias {
                    expr: Expr::Identifier(Ident::new("Q1")),
                    alias: Some(Ident::new("Quater1")),
                },
                ExprWithAlias {
                    expr: Expr::Identifier(Ident::new("Q2")),
                    alias: Some(Ident::new("Quater2")),
                },
                ExprWithAlias {
                    expr: Expr::Identifier(Ident::new("Q3")),
                    alias: Some(Ident::new("Quater3")),
                },
                ExprWithAlias {
                    expr: Expr::Identifier(Ident::new("Q4")),
                    alias: Some(Ident::new("Quater4")),
                },
            ]
        );
        assert_eq!(*value, Expr::Identifier(Ident::new("quantity")));
    }

    assert_eq!(
        verified_stmt(sql_unpivot_with_alias).to_string(),
        sql_unpivot_with_alias
    );

    let sql_unpivot_with_alias_and_multi_value = concat!(
        "SELECT * FROM sales AS s ",
        "UNPIVOT INCLUDE NULLS ((first_quarter, second_quarter) ",
        "FOR half_of_the_year IN (",
        "(Q1, Q2) AS H1, ",
        "(Q3, Q4) AS H2",
        "))"
    );

    if let Unpivot { value, columns, .. } =
        &verified_only_select(sql_unpivot_with_alias_and_multi_value).from[0].relation
    {
        assert_eq!(
            *columns,
            vec![
                ExprWithAlias {
                    expr: Expr::Tuple(vec![
                        Expr::Identifier(Ident::new("Q1")),
                        Expr::Identifier(Ident::new("Q2")),
                    ]),
                    alias: Some(Ident::new("H1")),
                },
                ExprWithAlias {
                    expr: Expr::Tuple(vec![
                        Expr::Identifier(Ident::new("Q3")),
                        Expr::Identifier(Ident::new("Q4")),
                    ]),
                    alias: Some(Ident::new("H2")),
                },
            ]
        );
        assert_eq!(
            *value,
            Expr::Tuple(vec![
                Expr::Identifier(Ident::new("first_quarter")),
                Expr::Identifier(Ident::new("second_quarter")),
            ])
        );
    }

    assert_eq!(
        verified_stmt(sql_unpivot_with_alias_and_multi_value).to_string(),
        sql_unpivot_with_alias_and_multi_value
    );

    let sql_unpivot_with_alias_and_multi_value_and_qualifier = concat!(
        "SELECT * FROM sales AS s ",
        "UNPIVOT INCLUDE NULLS ((first_quarter, second_quarter) ",
        "FOR half_of_the_year IN (",
        "(sales.Q1, sales.Q2) AS H1, ",
        "(sales.Q3, sales.Q4) AS H2",
        "))"
    );

    if let Unpivot { columns, .. } =
        &verified_only_select(sql_unpivot_with_alias_and_multi_value_and_qualifier).from[0].relation
    {
        assert_eq!(
            *columns,
            vec![
                ExprWithAlias {
                    expr: Expr::Tuple(vec![
                        Expr::CompoundIdentifier(vec![Ident::new("sales"), Ident::new("Q1"),]),
                        Expr::CompoundIdentifier(vec![Ident::new("sales"), Ident::new("Q2"),]),
                    ]),
                    alias: Some(Ident::new("H1")),
                },
                ExprWithAlias {
                    expr: Expr::Tuple(vec![
                        Expr::CompoundIdentifier(vec![Ident::new("sales"), Ident::new("Q3"),]),
                        Expr::CompoundIdentifier(vec![Ident::new("sales"), Ident::new("Q4"),]),
                    ]),
                    alias: Some(Ident::new("H2")),
                },
            ]
        );
    }

    assert_eq!(
        verified_stmt(sql_unpivot_with_alias_and_multi_value_and_qualifier).to_string(),
        sql_unpivot_with_alias_and_multi_value_and_qualifier
    );
}

#[test]
fn parse_select_table_with_index_hints() {
    let supported_dialects = all_dialects_where(|d| d.supports_table_hints());
    let s = supported_dialects.verified_only_select(
        "SELECT * FROM t1 USE INDEX (i1) IGNORE INDEX FOR ORDER BY (i2) ORDER BY a",
    );
    if let TableFactor::Table { index_hints, .. } = &s.from[0].relation {
        assert_eq!(
            vec![
                TableIndexHints {
                    hint_type: TableIndexHintType::Use,
                    index_names: vec!["i1".into()],
                    index_type: TableIndexType::Index,
                    for_clause: None,
                },
                TableIndexHints {
                    hint_type: TableIndexHintType::Ignore,
                    index_names: vec!["i2".into()],
                    index_type: TableIndexType::Index,
                    for_clause: Some(TableIndexHintForClause::OrderBy),
                },
            ],
            *index_hints
        );
    } else {
        panic!("Expected TableFactor::Table");
    }
    supported_dialects.verified_stmt("SELECT * FROM t1 USE INDEX (i1) USE INDEX (i1, i1)");
    supported_dialects.verified_stmt(
        "SELECT * FROM t1 USE INDEX () IGNORE INDEX (i2) USE INDEX (i1) USE INDEX (i2)",
    );
    supported_dialects.verified_stmt("SELECT * FROM t1 FORCE INDEX FOR JOIN (i2)");
    supported_dialects.verified_stmt("SELECT * FROM t1 IGNORE INDEX FOR JOIN (i2)");
    supported_dialects.verified_stmt(
        "SELECT * FROM t USE INDEX (index1) IGNORE INDEX FOR ORDER BY (index1) IGNORE INDEX FOR GROUP BY (index1) WHERE A = B",
    );

    // Test that dialects that don't support table hints will keep parsing the USE as table alias
    let sql = "SELECT * FROM T USE LIMIT 1";
    let unsupported_dialects = all_dialects_where(|d| !d.supports_table_hints());
    let select = unsupported_dialects
        .verified_only_select_with_canonical(sql, "SELECT * FROM T AS USE LIMIT 1");
    assert_eq!(
        select.from,
        vec![TableWithJoins {
            relation: TableFactor::Table {
                name: ObjectName(vec![sqlparser::ast::ObjectNamePart::Identifier(
                    Ident::new("T")
                )]),
                alias: Some(TableAlias {
                    name: Ident::new("USE"),
                    columns: vec![],
                }),
                args: None,
                with_hints: vec![],
                version: None,
                partitions: vec![],
                with_ordinality: false,
                json_path: None,
                sample: None,
                index_hints: vec![],
            },
            joins: vec![],
        }]
    );
}

#[test]
fn parse_pivot_unpivot_table() {
    let sql = concat!(
        "SELECT * FROM census AS c ",
        "UNPIVOT(population FOR year IN (population_2000, population_2010)) AS u ",
        "PIVOT(sum(population) FOR year IN ('population_2000', 'population_2010')) AS p"
    );

    pretty_assertions::assert_eq!(
        verified_only_select(sql).from[0].relation,
        Pivot {
            table: Box::new(Unpivot {
                table: Box::new(TableFactor::Table {
                    name: ObjectName::from(vec![Ident::new("census")]),
                    alias: Some(TableAlias {
                        name: Ident::new("c"),
                        columns: vec![]
                    }),
                    args: None,
                    with_hints: vec![],
                    version: None,
                    partitions: vec![],
                    with_ordinality: false,
                    json_path: None,
                    sample: None,
                    index_hints: vec![],
                }),
                null_inclusion: None,
                value: Expr::Identifier(Ident::new("population")),
                name: Ident::new("year"),
                columns: ["population_2000", "population_2010"]
                    .into_iter()
                    .map(|col| ExprWithAlias {
                        expr: Expr::Identifier(Ident::new(col)),
                        alias: None,
                    })
                    .collect(),
                alias: Some(TableAlias {
                    name: Ident::new("u"),
                    columns: vec![]
                }),
            }),
            aggregate_functions: vec![ExprWithAlias {
                expr: call("sum", [Expr::Identifier(Ident::new("population"))]),
                alias: None
            }],
            value_column: vec![Expr::Identifier(Ident::new("year"))],
            value_source: PivotValueSource::List(vec![
                ExprWithAlias {
                    expr: Expr::Value(
                        (Value::SingleQuotedString("population_2000".to_string()))
                            .with_empty_span()
                    ),
                    alias: None
                },
                ExprWithAlias {
                    expr: Expr::Value(
                        (Value::SingleQuotedString("population_2010".to_string()))
                            .with_empty_span()
                    ),
                    alias: None
                },
            ]),
            default_on_null: None,
            alias: Some(TableAlias {
                name: Ident::new("p"),
                columns: vec![]
            }),
        }
    );
    assert_eq!(verified_stmt(sql).to_string(), sql);
}

/// Makes a predicate that looks like ((user_id = $id) OR user_id = $2...)
fn make_where_clause(num: usize) -> String {
    use std::fmt::Write;
    let mut output = "(".repeat(num - 1);

    for i in 0..num {
        if i > 0 {
            write!(&mut output, " OR ").unwrap();
        }
        write!(&mut output, "user_id = {i}").unwrap();
        if i < num - 1 {
            write!(&mut output, ")").unwrap();
        }
    }
    output
}

#[test]
fn parse_non_latin_identifiers() {
    let supported_dialects = TestedDialects::new(vec![
        Box::new(GenericDialect {}),
        Box::new(DuckDbDialect {}),
        Box::new(PostgreSqlDialect {}),
        Box::new(MsSqlDialect {}),
        Box::new(RedshiftSqlDialect {}),
        Box::new(MySqlDialect {}),
    ]);
    supported_dialects.verified_stmt("SELECT a.説明 FROM test.public.inter01 AS a");
    supported_dialects.verified_stmt("SELECT a.説明 FROM inter01 AS a, inter01_transactions AS b WHERE a.説明 = b.取引 GROUP BY a.説明");
    supported_dialects.verified_stmt("SELECT 説明, hühnervögel, garçon, Москва, 東京 FROM inter01");

    let supported_dialects = TestedDialects::new(vec![
        Box::new(GenericDialect {}),
        Box::new(DuckDbDialect {}),
        Box::new(MsSqlDialect {}),
    ]);
    assert!(supported_dialects
        .parse_sql_statements("SELECT 💝 FROM table1")
        .is_err());
}

#[test]
fn parse_trailing_comma() {
    // At the moment, DuckDB is the only dialect that allows
    // trailing commas anywhere in the query
    let trailing_commas = TestedDialects::new(vec![Box::new(DuckDbDialect {})]);

    trailing_commas.one_statement_parses_to(
        "SELECT album_id, name, FROM track",
        "SELECT album_id, name FROM track",
    );

    trailing_commas.one_statement_parses_to(
        "SELECT * FROM track ORDER BY milliseconds,",
        "SELECT * FROM track ORDER BY milliseconds",
    );

    trailing_commas.one_statement_parses_to(
        "SELECT DISTINCT ON (album_id,) name FROM track",
        "SELECT DISTINCT ON (album_id) name FROM track",
    );

    trailing_commas.one_statement_parses_to(
        "CREATE TABLE employees (name text, age int,)",
        "CREATE TABLE employees (name TEXT, age INT)",
    );

    trailing_commas.one_statement_parses_to(
        "GRANT USAGE, SELECT, INSERT, ON p TO u",
        "GRANT USAGE, SELECT, INSERT ON p TO u",
    );

    trailing_commas.verified_stmt("SELECT album_id, name FROM track");
    trailing_commas.verified_stmt("SELECT * FROM track ORDER BY milliseconds");
    trailing_commas.verified_stmt("SELECT DISTINCT ON (album_id) name FROM track");

    // check quoted "from" identifier edge-case
    trailing_commas.one_statement_parses_to(
        r#"SELECT "from", FROM "from""#,
        r#"SELECT "from" FROM "from""#,
    );
    trailing_commas.verified_stmt(r#"SELECT "from" FROM "from""#);

    // doesn't allow any trailing commas
    let trailing_commas = TestedDialects::new(vec![Box::new(PostgreSqlDialect {})]);

    assert_eq!(
        trailing_commas
            .parse_sql_statements("SELECT name, age, from employees;")
            .unwrap_err(),
        ParserError::ParserError("Expected an expression, found: from".to_string())
    );

    assert_eq!(
        trailing_commas
            .parse_sql_statements("REVOKE USAGE, SELECT, ON p TO u")
            .unwrap_err(),
        ParserError::ParserError("Expected: a privilege keyword, found: ON".to_string())
    );

    assert_eq!(
        trailing_commas
            .parse_sql_statements("CREATE TABLE employees (name text, age int,)")
            .unwrap_err(),
        ParserError::ParserError(
            "Expected: column name or constraint definition, found: )".to_string()
        )
    );

    let unsupported_dialects = all_dialects_where(|d| !d.supports_trailing_commas());
    assert_eq!(
        unsupported_dialects
            .parse_sql_statements("SELECT * FROM track ORDER BY milliseconds,")
            .unwrap_err(),
        ParserError::ParserError("Expected: an expression, found: EOF".to_string())
    );
}

#[test]
fn parse_projection_trailing_comma() {
    let trailing_commas = all_dialects_where(|d| d.supports_projection_trailing_commas());

    trailing_commas.one_statement_parses_to(
        "SELECT album_id, name, FROM track",
        "SELECT album_id, name FROM track",
    );

    trailing_commas.verified_stmt("SELECT album_id, name FROM track");

    trailing_commas.verified_stmt("SELECT * FROM track ORDER BY milliseconds");

    trailing_commas.verified_stmt("SELECT DISTINCT ON (album_id) name FROM track");

    let unsupported_dialects = all_dialects_where(|d| {
        !d.supports_projection_trailing_commas() && !d.supports_trailing_commas()
    });
    assert_eq!(
        unsupported_dialects
            .parse_sql_statements("SELECT album_id, name, FROM track")
            .unwrap_err(),
        ParserError::ParserError("Expected an expression, found: FROM".to_string())
    );
}

#[test]
fn parse_create_type() {
    let create_type =
        verified_stmt("CREATE TYPE db.type_name AS (foo INT, bar TEXT COLLATE \"de_DE\")");
    assert_eq!(
        Statement::CreateType {
            name: ObjectName::from(vec![Ident::new("db"), Ident::new("type_name")]),
            representation: UserDefinedTypeRepresentation::Composite {
                attributes: vec![
                    UserDefinedTypeCompositeAttributeDef {
                        name: Ident::new("foo"),
                        data_type: DataType::Int(None),
                        collation: None,
                    },
                    UserDefinedTypeCompositeAttributeDef {
                        name: Ident::new("bar"),
                        data_type: DataType::Text,
                        collation: Some(ObjectName::from(vec![Ident::with_quote('\"', "de_DE")])),
                    }
                ]
            }
        },
        create_type
    );
}

#[test]
fn parse_drop_type() {
    let sql = "DROP TYPE abc";
    match verified_stmt(sql) {
        Statement::Drop {
            names,
            object_type,
            if_exists,
            cascade,
            ..
        } => {
            assert_eq_vec(&["abc"], &names);
            assert_eq!(ObjectType::Type, object_type);
            assert!(!if_exists);
            assert!(!cascade);
        }
        _ => unreachable!(),
    };

    let sql = "DROP TYPE IF EXISTS def, magician, quaternion";
    match verified_stmt(sql) {
        Statement::Drop {
            names,
            object_type,
            if_exists,
            cascade,
            ..
        } => {
            assert_eq_vec(&["def", "magician", "quaternion"], &names);
            assert_eq!(ObjectType::Type, object_type);
            assert!(if_exists);
            assert!(!cascade);
        }
        _ => unreachable!(),
    }

    let sql = "DROP TYPE IF EXISTS my_type CASCADE";
    match verified_stmt(sql) {
        Statement::Drop {
            names,
            object_type,
            if_exists,
            cascade,
            ..
        } => {
            assert_eq_vec(&["my_type"], &names);
            assert_eq!(ObjectType::Type, object_type);
            assert!(if_exists);
            assert!(cascade);
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_call() {
    all_dialects().verified_stmt("CALL my_procedure()");
    all_dialects().verified_stmt("CALL my_procedure(1, 'a')");
    pg_and_generic().verified_stmt("CALL my_procedure(1, 'a', $1)");
    all_dialects().verified_stmt("CALL my_procedure");
    assert_eq!(
        verified_stmt("CALL my_procedure('a')"),
        Statement::Call(Function {
            uses_odbc_syntax: false,
            parameters: FunctionArguments::None,
            args: FunctionArguments::List(FunctionArgumentList {
                duplicate_treatment: None,
                args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
                    (Value::SingleQuotedString("a".to_string())).with_empty_span()
                )))],
                clauses: vec![],
            }),
            name: ObjectName::from(vec![Ident::new("my_procedure")]),
            filter: None,
            null_treatment: None,
            over: None,
            within_group: vec![],
        })
    );
}

#[test]
fn parse_execute_stored_procedure() {
    let expected = Statement::Execute {
        name: Some(ObjectName::from(vec![
            Ident {
                value: "my_schema".to_string(),
                quote_style: None,
                span: Span::empty(),
            },
            Ident {
                value: "my_stored_procedure".to_string(),
                quote_style: None,
                span: Span::empty(),
            },
        ])),
        parameters: vec![
            Expr::Value((Value::NationalStringLiteral("param1".to_string())).with_empty_span()),
            Expr::Value((Value::NationalStringLiteral("param2".to_string())).with_empty_span()),
        ],
        has_parentheses: false,
        immediate: false,
        using: vec![],
        into: vec![],
        output: false,
        default: false,
    };
    assert_eq!(
        // Microsoft SQL Server does not use parentheses around arguments for EXECUTE
        ms_and_generic()
            .verified_stmt("EXECUTE my_schema.my_stored_procedure N'param1', N'param2'"),
        expected
    );
    assert_eq!(
        ms_and_generic().one_statement_parses_to(
            "EXEC my_schema.my_stored_procedure N'param1', N'param2';",
            "EXECUTE my_schema.my_stored_procedure N'param1', N'param2'",
        ),
        expected
    );
    match ms_and_generic().verified_stmt("EXECUTE dbo.proc1 @ReturnVal = @X OUTPUT") {
        Statement::Execute { output, .. } => {
            assert!(output);
        }
        _ => unreachable!(),
    }
    match ms_and_generic().verified_stmt("EXECUTE dbo.proc1 DEFAULT") {
        Statement::Execute { default, .. } => {
            assert!(default);
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_execute_immediate() {
    let dialects = all_dialects_where(|d| d.supports_execute_immediate());

    let expected = Statement::Execute {
        parameters: vec![Expr::Value(
            (Value::SingleQuotedString("SELECT 1".to_string())).with_empty_span(),
        )],
        immediate: true,
        using: vec![ExprWithAlias {
            expr: Expr::value(number("1")),
            alias: Some(Ident::new("b")),
        }],
        into: vec![Ident::new("a")],
        name: None,
        has_parentheses: false,
        output: false,
        default: false,
    };

    let stmt = dialects.verified_stmt("EXECUTE IMMEDIATE 'SELECT 1' INTO a USING 1 AS b");
    assert_eq!(expected, stmt);

    dialects.verified_stmt("EXECUTE IMMEDIATE 'SELECT 1' INTO a, b USING 1 AS x, y");
    dialects.verified_stmt("EXECUTE IMMEDIATE 'SELECT 1' USING 1 AS x, y");
    dialects.verified_stmt("EXECUTE IMMEDIATE 'SELECT 1' INTO a, b");
    dialects.verified_stmt("EXECUTE IMMEDIATE 'SELECT 1'");
    dialects.verified_stmt("EXECUTE 'SELECT 1'");

    assert_eq!(
        ParserError::ParserError("Expected: identifier, found: ,".to_string()),
        dialects
            .parse_sql_statements("EXECUTE IMMEDIATE 'SELECT 1' USING 1 AS, y")
            .unwrap_err()
    );
}

#[test]
fn parse_create_table_collate() {
    all_dialects().verified_stmt("CREATE TABLE tbl (foo INT, bar TEXT COLLATE \"de_DE\")");
    // check ordering is preserved
    all_dialects().verified_stmt(
        "CREATE TABLE tbl (foo INT, bar TEXT CHARACTER SET utf8mb4 COLLATE \"de_DE\")",
    );
    all_dialects().verified_stmt(
        "CREATE TABLE tbl (foo INT, bar TEXT COLLATE \"de_DE\" CHARACTER SET utf8mb4)",
    );
}

#[test]
fn parse_binary_operators_without_whitespace() {
    // x + y
    all_dialects().one_statement_parses_to(
        "SELECT field+1000 FROM tbl1",
        "SELECT field + 1000 FROM tbl1",
    );

    all_dialects().one_statement_parses_to(
        "SELECT tbl1.field+tbl2.field FROM tbl1 JOIN tbl2 ON tbl1.id = tbl2.entity_id",
        "SELECT tbl1.field + tbl2.field FROM tbl1 JOIN tbl2 ON tbl1.id = tbl2.entity_id",
    );

    // x - y
    all_dialects().one_statement_parses_to(
        "SELECT field-1000 FROM tbl1",
        "SELECT field - 1000 FROM tbl1",
    );

    all_dialects().one_statement_parses_to(
        "SELECT tbl1.field-tbl2.field FROM tbl1 JOIN tbl2 ON tbl1.id = tbl2.entity_id",
        "SELECT tbl1.field - tbl2.field FROM tbl1 JOIN tbl2 ON tbl1.id = tbl2.entity_id",
    );

    // x * y
    all_dialects().one_statement_parses_to(
        "SELECT field*1000 FROM tbl1",
        "SELECT field * 1000 FROM tbl1",
    );

    all_dialects().one_statement_parses_to(
        "SELECT tbl1.field*tbl2.field FROM tbl1 JOIN tbl2 ON tbl1.id = tbl2.entity_id",
        "SELECT tbl1.field * tbl2.field FROM tbl1 JOIN tbl2 ON tbl1.id = tbl2.entity_id",
    );

    // x / y
    all_dialects().one_statement_parses_to(
        "SELECT field/1000 FROM tbl1",
        "SELECT field / 1000 FROM tbl1",
    );

    all_dialects().one_statement_parses_to(
        "SELECT tbl1.field/tbl2.field FROM tbl1 JOIN tbl2 ON tbl1.id = tbl2.entity_id",
        "SELECT tbl1.field / tbl2.field FROM tbl1 JOIN tbl2 ON tbl1.id = tbl2.entity_id",
    );

    // x % y
    all_dialects().one_statement_parses_to(
        "SELECT field%1000 FROM tbl1",
        "SELECT field % 1000 FROM tbl1",
    );

    all_dialects().one_statement_parses_to(
        "SELECT tbl1.field%tbl2.field FROM tbl1 JOIN tbl2 ON tbl1.id = tbl2.entity_id",
        "SELECT tbl1.field % tbl2.field FROM tbl1 JOIN tbl2 ON tbl1.id = tbl2.entity_id",
    );
}

#[test]
fn parse_unload() {
    let unload = verified_stmt("UNLOAD(SELECT cola FROM tab) TO 's3://...' WITH (format = 'AVRO')");
    assert_eq!(
        unload,
        Statement::Unload {
            query: Some(Box::new(Query {
                body: Box::new(SetExpr::Select(Box::new(Select {
                    select_token: AttachedToken::empty(),
                    distinct: None,
                    top: None,
                    top_before_distinct: false,
                    projection: vec![UnnamedExpr(Expr::Identifier(Ident::new("cola"))),],
                    exclude: None,
                    into: None,
                    from: vec![TableWithJoins {
                        relation: table_from_name(ObjectName::from(vec![Ident::new("tab")])),
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
                    window_before_qualify: false,
                    qualify: None,
                    value_table_mode: None,
                    connect_by: None,
                    flavor: SelectFlavor::Standard,
                }))),
                with: None,
                limit_clause: None,
                fetch: None,
                locks: vec![],
                for_clause: None,
                order_by: None,
                settings: None,
                format_clause: None,
                pipe_operators: vec![],
            })),
            to: Ident {
                value: "s3://...".to_string(),
                quote_style: Some('\''),
                span: Span::empty(),
            },
            with: vec![SqlOption::KeyValue {
                key: Ident {
                    value: "format".to_string(),
                    quote_style: None,
                    span: Span::empty(),
                },
                value: Expr::Value(
                    (Value::SingleQuotedString("AVRO".to_string())).with_empty_span()
                )
            }],
            query_text: None,
            auth: None,
            options: vec![],
        }
    );

    one_statement_parses_to(
        concat!(
            "UNLOAD('SELECT 1') ",
            "TO 's3://...' ",
            "IAM_ROLE 'arn:aws:iam::123456789:role/role1' ",
            "FORMAT AS CSV ",
            "FORMAT AS PARQUET ",
            "FORMAT AS JSON ",
            "MAXFILESIZE AS 10 MB ",
            "ROWGROUPSIZE AS 10 MB ",
            "PARALLEL ON ",
            "PARALLEL OFF ",
            "REGION AS 'us-east-1'"
        ),
        concat!(
            "UNLOAD('SELECT 1') ",
            "TO 's3://...' ",
            "IAM_ROLE 'arn:aws:iam::123456789:role/role1' ",
            "CSV ",
            "PARQUET ",
            "JSON ",
            "MAXFILESIZE 10 MB ",
            "ROWGROUPSIZE 10 MB ",
            "PARALLEL TRUE ",
            "PARALLEL FALSE ",
            "REGION 'us-east-1'"
        ),
    );

    verified_stmt(concat!(
        "UNLOAD('SELECT 1') ",
        "TO 's3://...' ",
        "IAM_ROLE 'arn:aws:iam::123456789:role/role1' ",
        "PARTITION BY (c1, c2, c3)",
    ));
    verified_stmt(concat!(
        "UNLOAD('SELECT 1') ",
        "TO 's3://...' ",
        "IAM_ROLE 'arn:aws:iam::123456789:role/role1' ",
        "PARTITION BY (c1, c2, c3) INCLUDE",
    ));

    verified_stmt(concat!(
        "UNLOAD('SELECT 1') ",
        "TO 's3://...' ",
        "IAM_ROLE 'arn:aws:iam::123456789:role/role1' ",
        "PARTITION BY (c1, c2, c3) INCLUDE ",
        "MANIFEST"
    ));
    verified_stmt(concat!(
        "UNLOAD('SELECT 1') ",
        "TO 's3://...' ",
        "IAM_ROLE 'arn:aws:iam::123456789:role/role1' ",
        "PARTITION BY (c1, c2, c3) INCLUDE ",
        "MANIFEST VERBOSE"
    ));

    verified_stmt(concat!(
        "UNLOAD('SELECT 1') ",
        "TO 's3://...' ",
        "IAM_ROLE 'arn:aws:iam::123456789:role/role1' ",
        "PARTITION BY (c1, c2, c3) INCLUDE ",
        "MANIFEST VERBOSE ",
        "HEADER ",
        "FIXEDWIDTH 'col1:1,col2:2' ",
        "ENCRYPTED"
    ));
    verified_stmt(concat!(
        "UNLOAD('SELECT 1') ",
        "TO 's3://...' ",
        "IAM_ROLE 'arn:aws:iam::123456789:role/role1' ",
        "PARTITION BY (c1, c2, c3) INCLUDE ",
        "MANIFEST VERBOSE ",
        "HEADER ",
        "FIXEDWIDTH 'col1:1,col2:2' ",
        "ENCRYPTED AUTO"
    ));

    verified_stmt(concat!(
        "UNLOAD('SELECT 1') ",
        "TO 's3://...' ",
        "IAM_ROLE 'arn:aws:iam::123456789:role/role1' ",
        "PARTITION BY (c1, c2, c3) INCLUDE ",
        "MANIFEST VERBOSE ",
        "HEADER ",
        "FIXEDWIDTH 'col1:1,col2:2' ",
        "ENCRYPTED AUTO ",
        "BZIP2 ",
        "GZIP ",
        "ZSTD ",
        "ADDQUOTES ",
        "NULL 'nil' ",
        "ESCAPE ",
        "ALLOWOVERWRITE ",
        "CLEANPATH ",
        "PARALLEL ",
        "PARALLEL TRUE ",
        "PARALLEL FALSE ",
        "MAXFILESIZE 10 ",
        "MAXFILESIZE 10 MB ",
        "MAXFILESIZE 10 GB ",
        "ROWGROUPSIZE 10 ",
        "ROWGROUPSIZE 10 MB ",
        "ROWGROUPSIZE 10 GB ",
        "REGION 'us-east-1' ",
        "EXTENSION 'ext1'"
    ));
}

#[test]
fn test_savepoint() {
    match verified_stmt("SAVEPOINT test1") {
        Statement::Savepoint { name } => {
            assert_eq!(Ident::new("test1"), name);
        }
        _ => unreachable!(),
    }
}

#[test]
fn test_release_savepoint() {
    match verified_stmt("RELEASE SAVEPOINT test1") {
        Statement::ReleaseSavepoint { name } => {
            assert_eq!(Ident::new("test1"), name);
        }
        _ => unreachable!(),
    }

    one_statement_parses_to("RELEASE test1", "RELEASE SAVEPOINT test1");
}

#[test]
fn test_comment_hash_syntax() {
    let dialects = TestedDialects::new(vec![
        Box::new(BigQueryDialect {}),
        Box::new(SnowflakeDialect {}),
        Box::new(MySqlDialect {}),
        Box::new(HiveDialect {}),
    ]);
    let sql = r#"
    # comment
    SELECT a, b, c # , d, e
    FROM T
    ####### comment #################
    WHERE true
    # comment
    "#;
    let canonical = "SELECT a, b, c FROM T WHERE true";
    dialects.verified_only_select_with_canonical(sql, canonical);
}

#[test]
fn test_parse_inline_comment() {
    let sql =
        "CREATE TABLE t0 (id INT COMMENT 'comment without equal') COMMENT = 'comment with equal'";
    // Hive dialect doesn't support `=` in table comment, please refer:
    // [Hive](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-CreateTable)
    match all_dialects_except(|d| d.is::<HiveDialect>()).verified_stmt(sql) {
        Statement::CreateTable(CreateTable {
            columns,
            table_options,
            ..
        }) => {
            assert_eq!(
                columns,
                vec![ColumnDef {
                    name: Ident::new("id".to_string()),
                    data_type: DataType::Int(None),
                    options: vec![ColumnOptionDef {
                        name: None,
                        option: Comment("comment without equal".to_string()),
                    }]
                }]
            );
            assert_eq!(
                table_options,
                CreateTableOptions::Plain(vec![SqlOption::Comment(CommentDef::WithEq(
                    "comment with equal".to_string()
                ))])
            );
        }
        _ => unreachable!(),
    }
}

#[test]
fn test_buffer_reuse() {
    let d = GenericDialect {};
    let q = "INSERT INTO customer WITH foo AS (SELECT 1) SELECT * FROM foo UNION VALUES (1)";
    let mut buf = Vec::new();
    Tokenizer::new(&d, q)
        .tokenize_with_location_into_buf(&mut buf)
        .unwrap();
    let mut p = Parser::new(&d).with_tokens_with_locations(buf);
    p.parse_statements().unwrap();
    let _ = p.into_tokens();
}

#[test]
fn parse_map_access_expr() {
    let sql = "users[-1][safe_offset(2)]";
    let dialects = TestedDialects::new(vec![
        Box::new(BigQueryDialect {}),
        Box::new(ClickHouseDialect {}),
    ]);
    let expr = dialects.verified_expr(sql);
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
                            (number("2")).with_empty_span(),
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
        ],
    };
    assert_eq!(expr, expected);

    for sql in ["users[1]", "a[array_length(b) - 1 + 2][c + 3][d * 4]"] {
        let _ = dialects.verified_expr(sql);
    }
}

#[test]
fn parse_connect_by() {
    let expect_query = Select {
        select_token: AttachedToken::empty(),
        distinct: None,
        top: None,
        top_before_distinct: false,
        projection: vec![
            SelectItem::UnnamedExpr(Expr::Identifier(Ident::new("employee_id"))),
            SelectItem::UnnamedExpr(Expr::Identifier(Ident::new("manager_id"))),
            SelectItem::UnnamedExpr(Expr::Identifier(Ident::new("title"))),
        ],
        exclude: None,
        from: vec![TableWithJoins {
            relation: table_from_name(ObjectName::from(vec![Ident::new("employees")])),
            joins: vec![],
        }],
        into: None,
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
        connect_by: Some(ConnectBy {
            condition: Expr::BinaryOp {
                left: Box::new(Expr::Identifier(Ident::new("title"))),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::Value(
                    Value::SingleQuotedString("president".to_owned()).with_empty_span(),
                )),
            },
            relationships: vec![Expr::BinaryOp {
                left: Box::new(Expr::Identifier(Ident::new("manager_id"))),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::Prior(Box::new(Expr::Identifier(Ident::new(
                    "employee_id",
                ))))),
            }],
        }),
        flavor: SelectFlavor::Standard,
    };

    let connect_by_1 = concat!(
        "SELECT employee_id, manager_id, title FROM employees ",
        "START WITH title = 'president' ",
        "CONNECT BY manager_id = PRIOR employee_id ",
        "ORDER BY employee_id"
    );

    assert_eq!(
        all_dialects_where(|d| d.supports_connect_by()).verified_only_select(connect_by_1),
        expect_query
    );

    // CONNECT BY can come before START WITH
    let connect_by_2 = concat!(
        "SELECT employee_id, manager_id, title FROM employees ",
        "CONNECT BY manager_id = PRIOR employee_id ",
        "START WITH title = 'president' ",
        "ORDER BY employee_id"
    );
    assert_eq!(
        all_dialects_where(|d| d.supports_connect_by())
            .verified_only_select_with_canonical(connect_by_2, connect_by_1),
        expect_query
    );

    // WHERE must come before CONNECT BY
    let connect_by_3 = concat!(
        "SELECT employee_id, manager_id, title FROM employees ",
        "WHERE employee_id <> 42 ",
        "START WITH title = 'president' ",
        "CONNECT BY manager_id = PRIOR employee_id ",
        "ORDER BY employee_id"
    );
    assert_eq!(
        all_dialects_where(|d| d.supports_connect_by()).verified_only_select(connect_by_3),
        Select {
            select_token: AttachedToken::empty(),
            distinct: None,
            top: None,
            top_before_distinct: false,
            projection: vec![
                SelectItem::UnnamedExpr(Expr::Identifier(Ident::new("employee_id"))),
                SelectItem::UnnamedExpr(Expr::Identifier(Ident::new("manager_id"))),
                SelectItem::UnnamedExpr(Expr::Identifier(Ident::new("title"))),
            ],
            exclude: None,
            from: vec![TableWithJoins {
                relation: table_from_name(ObjectName::from(vec![Ident::new("employees")])),
                joins: vec![],
            }],
            into: None,
            lateral_views: vec![],
            prewhere: None,
            selection: Some(Expr::BinaryOp {
                left: Box::new(Expr::Identifier(Ident::new("employee_id"))),
                op: BinaryOperator::NotEq,
                right: Box::new(Expr::value(number("42"))),
            }),
            group_by: GroupByExpr::Expressions(vec![], vec![]),
            cluster_by: vec![],
            distribute_by: vec![],
            sort_by: vec![],
            having: None,
            named_window: vec![],
            qualify: None,
            window_before_qualify: false,
            value_table_mode: None,
            connect_by: Some(ConnectBy {
                condition: Expr::BinaryOp {
                    left: Box::new(Expr::Identifier(Ident::new("title"))),
                    op: BinaryOperator::Eq,
                    right: Box::new(Expr::Value(
                        (Value::SingleQuotedString("president".to_owned(),)).with_empty_span()
                    )),
                },
                relationships: vec![Expr::BinaryOp {
                    left: Box::new(Expr::Identifier(Ident::new("manager_id"))),
                    op: BinaryOperator::Eq,
                    right: Box::new(Expr::Prior(Box::new(Expr::Identifier(Ident::new(
                        "employee_id",
                    ))))),
                }],
            }),
            flavor: SelectFlavor::Standard,
        }
    );

    let connect_by_4 = concat!(
        "SELECT employee_id, manager_id, title FROM employees ",
        "START WITH title = 'president' ",
        "CONNECT BY manager_id = PRIOR employee_id ",
        "WHERE employee_id <> 42 ",
        "ORDER BY employee_id"
    );
    all_dialects_where(|d| d.supports_connect_by())
        .parse_sql_statements(connect_by_4)
        .expect_err("should have failed");

    // PRIOR expressions are only valid within a CONNECT BY, and the the token
    // `prior` is valid as an identifier anywhere else.
    assert_eq!(
        all_dialects()
            .verified_only_select("SELECT prior FROM some_table")
            .projection,
        vec![SelectItem::UnnamedExpr(Expr::Identifier(Ident::new(
            "prior"
        )))]
    );
}

#[test]
fn test_selective_aggregation() {
    let testing_dialects = all_dialects_where(|d| d.supports_filter_during_aggregation());
    let expected_dialects: Vec<Box<dyn Dialect>> = vec![
        Box::new(PostgreSqlDialect {}),
        Box::new(DatabricksDialect {}),
        Box::new(HiveDialect {}),
        Box::new(SQLiteDialect {}),
        Box::new(DuckDbDialect {}),
        Box::new(GenericDialect {}),
    ];
    assert_eq!(testing_dialects.dialects.len(), expected_dialects.len());
    expected_dialects
        .into_iter()
        .for_each(|d| assert!(d.supports_filter_during_aggregation()));

    let sql = concat!(
        "SELECT ",
        "ARRAY_AGG(name) FILTER (WHERE name IS NOT NULL), ",
        "ARRAY_AGG(name) FILTER (WHERE name LIKE 'a%') AS agg2 ",
        "FROM region"
    );
    assert_eq!(
        testing_dialects.verified_only_select(sql).projection,
        vec![
            SelectItem::UnnamedExpr(Expr::Function(Function {
                name: ObjectName::from(vec![Ident::new("ARRAY_AGG")]),
                uses_odbc_syntax: false,
                parameters: FunctionArguments::None,
                args: FunctionArguments::List(FunctionArgumentList {
                    duplicate_treatment: None,
                    args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(
                        Expr::Identifier(Ident::new("name"))
                    ))],
                    clauses: vec![],
                }),
                filter: Some(Box::new(Expr::IsNotNull(Box::new(Expr::Identifier(
                    Ident::new("name")
                ))))),
                over: None,
                within_group: vec![],
                null_treatment: None
            })),
            SelectItem::ExprWithAlias {
                expr: Expr::Function(Function {
                    name: ObjectName::from(vec![Ident::new("ARRAY_AGG")]),
                    uses_odbc_syntax: false,
                    parameters: FunctionArguments::None,
                    args: FunctionArguments::List(FunctionArgumentList {
                        duplicate_treatment: None,
                        args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(
                            Expr::Identifier(Ident::new("name"))
                        ))],
                        clauses: vec![],
                    }),
                    filter: Some(Box::new(Expr::Like {
                        negated: false,
                        expr: Box::new(Expr::Identifier(Ident::new("name"))),
                        pattern: Box::new(Expr::Value(
                            (Value::SingleQuotedString("a%".to_owned())).with_empty_span()
                        )),
                        escape_char: None,
                        any: false,
                    })),
                    null_treatment: None,
                    over: None,
                    within_group: vec![]
                }),
                alias: Ident::new("agg2")
            },
        ]
    )
}

#[test]
fn test_group_by_grouping_sets() {
    let sql = concat!(
        "SELECT city, car_model, sum(quantity) AS sum ",
        "FROM dealer ",
        "GROUP BY GROUPING SETS ((city, car_model), (city), (car_model), ()) ",
        "ORDER BY city",
    );
    assert_eq!(
        all_dialects_where(|d| d.supports_group_by_expr())
            .verified_only_select(sql)
            .group_by,
        GroupByExpr::Expressions(
            vec![Expr::GroupingSets(vec![
                vec![
                    Expr::Identifier(Ident::new("city")),
                    Expr::Identifier(Ident::new("car_model"))
                ],
                vec![Expr::Identifier(Ident::new("city")),],
                vec![Expr::Identifier(Ident::new("car_model"))],
                vec![]
            ])],
            vec![]
        )
    );
}

#[test]
fn test_xmltable() {
    all_dialects()
        .verified_only_select("SELECT * FROM XMLTABLE('/root' PASSING data COLUMNS element TEXT)");

    // Minimal meaningful working example: returns a single row with a single column named y containing the value z
    all_dialects().verified_only_select(
        "SELECT y FROM XMLTABLE('/X' PASSING '<X><y>z</y></X>' COLUMNS y TEXT)",
    );

    // Test using subqueries
    all_dialects().verified_only_select("SELECT y FROM XMLTABLE((SELECT '/X') PASSING (SELECT CAST('<X><y>z</y></X>' AS xml)) COLUMNS y TEXT PATH (SELECT 'y'))");

    // NOT NULL
    all_dialects().verified_only_select(
        "SELECT y FROM XMLTABLE('/X' PASSING '<X></X>' COLUMNS y TEXT NOT NULL)",
    );

    all_dialects().verified_only_select("SELECT * FROM XMLTABLE('/root/row' PASSING xmldata COLUMNS id INT PATH '@id', name TEXT PATH 'name/text()', value FLOAT PATH 'value')");

    all_dialects().verified_only_select("SELECT * FROM XMLTABLE('//ROWS/ROW' PASSING data COLUMNS row_num FOR ORDINALITY, id INT PATH '@id', name TEXT PATH 'NAME' DEFAULT 'unnamed')");

    // Example from https://www.postgresql.org/docs/15/functions-xml.html#FUNCTIONS-XML-PROCESSING
    all_dialects().verified_only_select(
        "SELECT xmltable.* FROM xmldata, XMLTABLE('//ROWS/ROW' PASSING data COLUMNS id INT PATH '@id', ordinality FOR ORDINALITY, \"COUNTRY_NAME\" TEXT, country_id TEXT PATH 'COUNTRY_ID', size_sq_km FLOAT PATH 'SIZE[@unit = \"sq_km\"]', size_other TEXT PATH 'concat(SIZE[@unit!=\"sq_km\"], \" \", SIZE[@unit!=\"sq_km\"]/@unit)', premier_name TEXT PATH 'PREMIER_NAME' DEFAULT 'not specified')"
    );

    // Example from DB2 docs without explicit PASSING clause: https://www.ibm.com/docs/en/db2/12.1.0?topic=xquery-simple-column-name-passing-xmlexists-xmlquery-xmltable
    all_dialects().verified_only_select(
        "SELECT X.* FROM T1, XMLTABLE('$CUSTLIST/customers/customerinfo' COLUMNS \"Cid\" BIGINT PATH '@Cid', \"Info\" XML PATH 'document{.}', \"History\" XML PATH 'NULL') AS X"
    );

    // Example from PostgreSQL with XMLNAMESPACES
    all_dialects().verified_only_select(
        "SELECT xmltable.* FROM XMLTABLE(XMLNAMESPACES('http://example.com/myns' AS x, 'http://example.com/b' AS \"B\"), '/x:example/x:item' PASSING (SELECT data FROM xmldata) COLUMNS foo INT PATH '@foo', bar INT PATH '@B:bar')"
    );
}

#[test]
fn test_match_recognize() {
    use MatchRecognizePattern::*;
    use MatchRecognizeSymbol::*;
    use RepetitionQuantifier::*;

    let table = table_from_name(ObjectName::from(vec![Ident::new("my_table")]));

    fn check(options: &str, expect: TableFactor) {
        let select = all_dialects_where(|d| d.supports_match_recognize()).verified_only_select(
            &format!("SELECT * FROM my_table MATCH_RECOGNIZE({options})"),
        );
        assert_eq!(&select.from[0].relation, &expect);
    }

    check(
        concat!(
            "PARTITION BY company ",
            "ORDER BY price_date ",
            "MEASURES ",
            "MATCH_NUMBER() AS match_number, ",
            "FIRST(price_date) AS start_date, ",
            "LAST(price_date) AS end_date ",
            "ONE ROW PER MATCH ",
            "AFTER MATCH SKIP TO LAST row_with_price_increase ",
            "PATTERN (row_before_decrease row_with_price_decrease+ row_with_price_increase+) ",
            "DEFINE ",
            "row_with_price_decrease AS price < LAG(price), ",
            "row_with_price_increase AS price > LAG(price)"
        ),
        TableFactor::MatchRecognize {
            table: Box::new(table),
            partition_by: vec![Expr::Identifier(Ident::new("company"))],
            order_by: vec![OrderByExpr {
                expr: Expr::Identifier(Ident::new("price_date")),
                options: OrderByOptions {
                    asc: None,
                    nulls_first: None,
                },
                with_fill: None,
            }],
            measures: vec![
                Measure {
                    expr: call("MATCH_NUMBER", []),
                    alias: Ident::new("match_number"),
                },
                Measure {
                    expr: call("FIRST", [Expr::Identifier(Ident::new("price_date"))]),
                    alias: Ident::new("start_date"),
                },
                Measure {
                    expr: call("LAST", [Expr::Identifier(Ident::new("price_date"))]),
                    alias: Ident::new("end_date"),
                },
            ],
            rows_per_match: Some(RowsPerMatch::OneRow),
            after_match_skip: Some(AfterMatchSkip::ToLast(Ident::new(
                "row_with_price_increase",
            ))),
            pattern: Concat(vec![
                Symbol(Named(Ident::new("row_before_decrease"))),
                Repetition(
                    Box::new(Symbol(Named(Ident::new("row_with_price_decrease")))),
                    OneOrMore,
                ),
                Repetition(
                    Box::new(Symbol(Named(Ident::new("row_with_price_increase")))),
                    OneOrMore,
                ),
            ]),
            symbols: vec![
                SymbolDefinition {
                    symbol: Ident::new("row_with_price_decrease"),
                    definition: Expr::BinaryOp {
                        left: Box::new(Expr::Identifier(Ident::new("price"))),
                        op: BinaryOperator::Lt,
                        right: Box::new(call("LAG", [Expr::Identifier(Ident::new("price"))])),
                    },
                },
                SymbolDefinition {
                    symbol: Ident::new("row_with_price_increase"),
                    definition: Expr::BinaryOp {
                        left: Box::new(Expr::Identifier(Ident::new("price"))),
                        op: BinaryOperator::Gt,
                        right: Box::new(call("LAG", [Expr::Identifier(Ident::new("price"))])),
                    },
                },
            ],
            alias: None,
        },
    );

    #[rustfmt::skip]
    let examples = [
        concat!(
            "SELECT * ",
            "FROM login_attempts ",
            "MATCH_RECOGNIZE(",
                "PARTITION BY user_id ",
                "ORDER BY timestamp ",
                "PATTERN (failed_attempt{3,}) ",
                "DEFINE ",
                    "failed_attempt AS status = 'failure'",
            ")",
        ),
        concat!(
            "SELECT * ",
            "FROM stock_transactions ",
            "MATCH_RECOGNIZE(",
                "PARTITION BY symbol ",
                "ORDER BY timestamp ",
                "MEASURES ",
                    "FIRST(price) AS start_price, ",
                    "LAST(price) AS end_price, ",
                    "MATCH_NUMBER() AS match_num ",
                "ALL ROWS PER MATCH ",
                "PATTERN (STRT UP+) ",
                "DEFINE ",
                    "UP AS price > PREV(price)",
            ")",
        ),
        concat!(
            "SELECT * ",
            "FROM event_log ",
            "MATCH_RECOGNIZE(",
                "MEASURES ",
                    "FIRST(event_type) AS start_event, ",
                    "LAST(event_type) AS end_event, ",
                    "COUNT(*) AS error_count ",
                "ALL ROWS PER MATCH ",
                "PATTERN (STRT ERROR+ END) ",
                "DEFINE ",
                    "STRT AS event_type = 'START', ",
                    "ERROR AS event_type = 'ERROR', ",
                    "END AS event_type = 'END'",
            ")",
        )
    ];

    for sql in examples {
        all_dialects_where(|d| d.supports_match_recognize()).verified_query(sql);
    }
}

#[test]
fn test_match_recognize_patterns() {
    use MatchRecognizePattern::*;
    use MatchRecognizeSymbol::*;
    use RepetitionQuantifier::*;

    fn check(pattern: &str, expect: MatchRecognizePattern) {
        let select =
            all_dialects_where(|d| d.supports_match_recognize()).verified_only_select(&format!(
                "SELECT * FROM my_table MATCH_RECOGNIZE(PATTERN ({pattern}) DEFINE DUMMY AS true)" // "select * from my_table match_recognize ("
            ));
        let TableFactor::MatchRecognize {
            pattern: actual, ..
        } = &select.from[0].relation
        else {
            panic!("expected match_recognize table factor");
        };
        assert_eq!(actual, &expect);
    }

    // just a symbol
    check("FOO", Symbol(Named(Ident::new("FOO"))));

    // just a symbol
    check(
        "^ FOO $",
        Concat(vec![
            Symbol(Start),
            Symbol(Named(Ident::new("FOO"))),
            Symbol(End),
        ]),
    );

    // exclusion
    check("{- FOO -}", Exclude(Named(Ident::new("FOO"))));

    check(
        "PERMUTE(A, B, C)",
        Permute(vec![
            Named(Ident::new("A")),
            Named(Ident::new("B")),
            Named(Ident::new("C")),
        ]),
    );

    // various identifiers
    check(
        "FOO | \"BAR\" | baz42",
        Alternation(vec![
            Symbol(Named(Ident::new("FOO"))),
            Symbol(Named(Ident::with_quote('"', "BAR"))),
            Symbol(Named(Ident::new("baz42"))),
        ]),
    );

    // concatenated basic quantifiers
    check(
        "S1* S2+ S3?",
        Concat(vec![
            Repetition(Box::new(Symbol(Named(Ident::new("S1")))), ZeroOrMore),
            Repetition(Box::new(Symbol(Named(Ident::new("S2")))), OneOrMore),
            Repetition(Box::new(Symbol(Named(Ident::new("S3")))), AtMostOne),
        ]),
    );

    // double repetition
    check(
        "S2*?",
        Repetition(
            Box::new(Repetition(
                Box::new(Symbol(Named(Ident::new("S2")))),
                ZeroOrMore,
            )),
            AtMostOne,
        ),
    );

    // range quantifiers in an alternation
    check(
        "S1{1} | S2{2,3} | S3{4,} | S4{,5}",
        Alternation(vec![
            Repetition(Box::new(Symbol(Named(Ident::new("S1")))), Exactly(1)),
            Repetition(Box::new(Symbol(Named(Ident::new("S2")))), Range(2, 3)),
            Repetition(Box::new(Symbol(Named(Ident::new("S3")))), AtLeast(4)),
            Repetition(Box::new(Symbol(Named(Ident::new("S4")))), AtMost(5)),
        ]),
    );

    // grouping case 1
    check(
        "S1 ( S2 )",
        Concat(vec![
            Symbol(Named(Ident::new("S1"))),
            Group(Box::new(Symbol(Named(Ident::new("S2"))))),
        ]),
    );

    // grouping case 2
    check(
        "( {- S3 -} S4 )+",
        Repetition(
            Box::new(Group(Box::new(Concat(vec![
                Exclude(Named(Ident::new("S3"))),
                Symbol(Named(Ident::new("S4"))),
            ])))),
            OneOrMore,
        ),
    );

    // the grand finale (example taken from snowflake docs)
    check(
        "^ S1 S2*? ( {- S3 -} S4 )+ | PERMUTE(S1, S2){1,2} $",
        Alternation(vec![
            Concat(vec![
                Symbol(Start),
                Symbol(Named(Ident::new("S1"))),
                Repetition(
                    Box::new(Repetition(
                        Box::new(Symbol(Named(Ident::new("S2")))),
                        ZeroOrMore,
                    )),
                    AtMostOne,
                ),
                Repetition(
                    Box::new(Group(Box::new(Concat(vec![
                        Exclude(Named(Ident::new("S3"))),
                        Symbol(Named(Ident::new("S4"))),
                    ])))),
                    OneOrMore,
                ),
            ]),
            Concat(vec![
                Repetition(
                    Box::new(Permute(vec![
                        Named(Ident::new("S1")),
                        Named(Ident::new("S2")),
                    ])),
                    Range(1, 2),
                ),
                Symbol(End),
            ]),
        ]),
    );
}

#[test]
fn test_select_wildcard_with_replace() {
    let sql = r#"SELECT * REPLACE (lower(city) AS city) FROM addresses"#;
    let dialects = TestedDialects::new(vec![
        Box::new(GenericDialect {}),
        Box::new(BigQueryDialect {}),
        Box::new(ClickHouseDialect {}),
        Box::new(SnowflakeDialect {}),
        Box::new(DuckDbDialect {}),
    ]);
    let select = dialects.verified_only_select(sql);
    let expected = SelectItem::Wildcard(WildcardAdditionalOptions {
        opt_replace: Some(ReplaceSelectItem {
            items: vec![Box::new(ReplaceSelectElement {
                expr: call("lower", [Expr::Identifier(Ident::new("city"))]),
                column_name: Ident::new("city"),
                as_keyword: true,
            })],
        }),
        ..Default::default()
    });
    assert_eq!(expected, select.projection[0]);

    let select =
        dialects.verified_only_select(r#"SELECT * REPLACE ('widget' AS item_name) FROM orders"#);
    let expected = SelectItem::Wildcard(WildcardAdditionalOptions {
        opt_replace: Some(ReplaceSelectItem {
            items: vec![Box::new(ReplaceSelectElement {
                expr: Expr::Value(
                    (Value::SingleQuotedString("widget".to_owned())).with_empty_span(),
                ),
                column_name: Ident::new("item_name"),
                as_keyword: true,
            })],
        }),
        ..Default::default()
    });
    assert_eq!(expected, select.projection[0]);

    let select = dialects.verified_only_select(
        r#"SELECT * REPLACE (quantity / 2 AS quantity, 3 AS order_id) FROM orders"#,
    );
    let expected = SelectItem::Wildcard(WildcardAdditionalOptions {
        opt_replace: Some(ReplaceSelectItem {
            items: vec![
                Box::new(ReplaceSelectElement {
                    expr: Expr::BinaryOp {
                        left: Box::new(Expr::Identifier(Ident::new("quantity"))),
                        op: BinaryOperator::Divide,
                        right: Box::new(Expr::value(number("2"))),
                    },
                    column_name: Ident::new("quantity"),
                    as_keyword: true,
                }),
                Box::new(ReplaceSelectElement {
                    expr: Expr::value(number("3")),
                    column_name: Ident::new("order_id"),
                    as_keyword: true,
                }),
            ],
        }),
        ..Default::default()
    });
    assert_eq!(expected, select.projection[0]);
}

#[test]
fn parse_sized_list() {
    let dialects = TestedDialects::new(vec![
        Box::new(GenericDialect {}),
        Box::new(PostgreSqlDialect {}),
        Box::new(DuckDbDialect {}),
    ]);
    let sql = r#"CREATE TABLE embeddings (data FLOAT[1536])"#;
    dialects.verified_stmt(sql);
    let sql = r#"CREATE TABLE embeddings (data FLOAT[1536][3])"#;
    dialects.verified_stmt(sql);
    let sql = r#"SELECT data::FLOAT[1536] FROM embeddings"#;
    dialects.verified_stmt(sql);
}

#[test]
fn insert_into_with_parentheses() {
    let dialects = TestedDialects::new(vec![
        Box::new(SnowflakeDialect {}),
        Box::new(RedshiftSqlDialect {}),
        Box::new(GenericDialect {}),
    ]);
    dialects.verified_stmt("INSERT INTO t1 (id, name) (SELECT t2.id, t2.name FROM t2)");
    dialects.verified_stmt("INSERT INTO t1 (SELECT t2.id, t2.name FROM t2)");
    dialects.verified_stmt(r#"INSERT INTO t1 ("select", name) (SELECT t2.name FROM t2)"#);
}

#[test]
fn parse_odbc_scalar_function() {
    let select = verified_only_select("SELECT {fn my_func(1, 2)}");
    let Expr::Function(Function {
        name,
        uses_odbc_syntax,
        args,
        ..
    }) = expr_from_projection(only(&select.projection))
    else {
        unreachable!("expected function")
    };
    assert_eq!(name, &ObjectName::from(vec![Ident::new("my_func")]));
    assert!(uses_odbc_syntax);
    matches!(args, FunctionArguments::List(l) if l.args.len() == 2);

    verified_stmt("SELECT {fn fna()} AS foo, fnb(1)");

    // Testing invalid SQL with any-one dialect is intentional.
    // Depending on dialect flags the error message may be different.
    let pg = TestedDialects::new(vec![Box::new(PostgreSqlDialect {})]);
    assert_eq!(
        pg.parse_sql_statements("SELECT {fn2 my_func()}")
            .unwrap_err()
            .to_string(),
        "sql parser error: Expected: an expression, found: {"
    );
}

#[test]
fn test_dictionary_syntax() {
    fn check(sql: &str, expect: Expr) {
        assert_eq!(
            all_dialects_where(|d| d.supports_dictionary_syntax()).verified_expr(sql),
            expect
        );
    }

    check("{}", Expr::Dictionary(vec![]));

    check(
        "{'Alberta': 'Edmonton', 'Manitoba': 'Winnipeg'}",
        Expr::Dictionary(vec![
            DictionaryField {
                key: Ident::with_quote('\'', "Alberta"),
                value: Box::new(Expr::Value(
                    (Value::SingleQuotedString("Edmonton".to_owned())).with_empty_span(),
                )),
            },
            DictionaryField {
                key: Ident::with_quote('\'', "Manitoba"),
                value: Box::new(Expr::Value(
                    (Value::SingleQuotedString("Winnipeg".to_owned())).with_empty_span(),
                )),
            },
        ]),
    );

    check(
        "{'start': CAST('2023-04-01' AS TIMESTAMP), 'end': CAST('2023-04-05' AS TIMESTAMP)}",
        Expr::Dictionary(vec![
            DictionaryField {
                key: Ident::with_quote('\'', "start"),
                value: Box::new(Expr::Cast {
                    kind: CastKind::Cast,
                    expr: Box::new(Expr::Value(
                        (Value::SingleQuotedString("2023-04-01".to_owned())).with_empty_span(),
                    )),
                    data_type: DataType::Timestamp(None, TimezoneInfo::None),
                    format: None,
                }),
            },
            DictionaryField {
                key: Ident::with_quote('\'', "end"),
                value: Box::new(Expr::Cast {
                    kind: CastKind::Cast,
                    expr: Box::new(Expr::Value(
                        (Value::SingleQuotedString("2023-04-05".to_owned())).with_empty_span(),
                    )),
                    data_type: DataType::Timestamp(None, TimezoneInfo::None),
                    format: None,
                }),
            },
        ]),
    )
}

#[test]
fn test_map_syntax() {
    fn check(sql: &str, expect: Expr) {
        assert_eq!(
            all_dialects_where(|d| d.support_map_literal_syntax()).verified_expr(sql),
            expect
        );
    }

    check(
        "MAP {'Alberta': 'Edmonton', 'Manitoba': 'Winnipeg'}",
        Expr::Map(Map {
            entries: vec![
                MapEntry {
                    key: Box::new(Expr::Value(
                        (Value::SingleQuotedString("Alberta".to_owned())).with_empty_span(),
                    )),
                    value: Box::new(Expr::Value(
                        (Value::SingleQuotedString("Edmonton".to_owned())).with_empty_span(),
                    )),
                },
                MapEntry {
                    key: Box::new(Expr::Value(
                        (Value::SingleQuotedString("Manitoba".to_owned())).with_empty_span(),
                    )),
                    value: Box::new(Expr::Value(
                        (Value::SingleQuotedString("Winnipeg".to_owned())).with_empty_span(),
                    )),
                },
            ],
        }),
    );

    fn number_expr(s: &str) -> Expr {
        Expr::value(number(s))
    }

    check(
        "MAP {1: 10.0, 2: 20.0}",
        Expr::Map(Map {
            entries: vec![
                MapEntry {
                    key: Box::new(number_expr("1")),
                    value: Box::new(number_expr("10.0")),
                },
                MapEntry {
                    key: Box::new(number_expr("2")),
                    value: Box::new(number_expr("20.0")),
                },
            ],
        }),
    );

    check(
        "MAP {[1, 2, 3]: 10.0, [4, 5, 6]: 20.0}",
        Expr::Map(Map {
            entries: vec![
                MapEntry {
                    key: Box::new(Expr::Array(Array {
                        elem: vec![number_expr("1"), number_expr("2"), number_expr("3")],
                        named: false,
                    })),
                    value: Box::new(Expr::value(number("10.0"))),
                },
                MapEntry {
                    key: Box::new(Expr::Array(Array {
                        elem: vec![number_expr("4"), number_expr("5"), number_expr("6")],
                        named: false,
                    })),
                    value: Box::new(Expr::value(number("20.0"))),
                },
            ],
        }),
    );

    check(
        "MAP {'a': 10, 'b': 20}['a']",
        Expr::CompoundFieldAccess {
            root: Box::new(Expr::Map(Map {
                entries: vec![
                    MapEntry {
                        key: Box::new(Expr::Value(
                            (Value::SingleQuotedString("a".to_owned())).with_empty_span(),
                        )),
                        value: Box::new(number_expr("10")),
                    },
                    MapEntry {
                        key: Box::new(Expr::Value(
                            (Value::SingleQuotedString("b".to_owned())).with_empty_span(),
                        )),
                        value: Box::new(number_expr("20")),
                    },
                ],
            })),
            access_chain: vec![AccessExpr::Subscript(Subscript::Index {
                index: Expr::Value((Value::SingleQuotedString("a".to_owned())).with_empty_span()),
            })],
        },
    );

    check("MAP {}", Expr::Map(Map { entries: vec![] }));
}

#[test]
fn parse_within_group() {
    verified_expr("PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY sales_amount)");
    verified_expr(concat!(
        "PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY sales_amount) ",
        "OVER (PARTITION BY department)",
    ));
}

#[test]
fn tests_select_values_without_parens() {
    let dialects = TestedDialects::new(vec![
        Box::new(GenericDialect {}),
        Box::new(SnowflakeDialect {}),
        Box::new(DatabricksDialect {}),
    ]);
    let sql = "SELECT * FROM VALUES (1, 2), (2,3) AS tbl (id, val)";
    let canonical = "SELECT * FROM (VALUES (1, 2), (2, 3)) AS tbl (id, val)";
    dialects.verified_only_select_with_canonical(sql, canonical);
}

#[test]
fn tests_select_values_without_parens_and_set_op() {
    let dialects = TestedDialects::new(vec![
        Box::new(GenericDialect {}),
        Box::new(SnowflakeDialect {}),
        Box::new(DatabricksDialect {}),
    ]);

    let sql = "SELECT id + 1, name FROM VALUES (1, 'Apple'), (2, 'Banana'), (3, 'Orange') AS fruits (id, name) UNION ALL SELECT 5, 'Strawberry'";
    let canonical = "SELECT id + 1, name FROM (VALUES (1, 'Apple'), (2, 'Banana'), (3, 'Orange')) AS fruits (id, name) UNION ALL SELECT 5, 'Strawberry'";
    let query = dialects.verified_query_with_canonical(sql, canonical);
    match *query.body {
        SetExpr::SetOperation {
            op,
            set_quantifier: _,
            left,
            right,
        } => {
            assert_eq!(SetOperator::Union, op);
            match *left {
                SetExpr::Select(_) => {}
                _ => panic!("Expected: a SELECT statement"),
            }
            match *right {
                SetExpr::Select(_) => {}
                _ => panic!("Expected: a SELECT statement"),
            }
        }
        _ => panic!("Expected: a SET OPERATION"),
    }
}

#[test]
fn parse_select_wildcard_with_except() {
    let dialects = all_dialects_where(|d| d.supports_select_wildcard_except());

    let select = dialects.verified_only_select("SELECT * EXCEPT (col_a) FROM data");
    let expected = SelectItem::Wildcard(WildcardAdditionalOptions {
        opt_except: Some(ExceptSelectItem {
            first_element: Ident::new("col_a"),
            additional_elements: vec![],
        }),
        ..Default::default()
    });
    assert_eq!(expected, select.projection[0]);

    let select = dialects
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
        dialects
            .parse_sql_statements("SELECT * EXCEPT () FROM employee_table")
            .unwrap_err()
            .to_string(),
        "sql parser error: Expected: identifier, found: )"
    );
}

#[test]
fn test_group_by_nothing() {
    let Select { group_by, .. } = all_dialects_where(|d| d.supports_group_by_expr())
        .verified_only_select("SELECT count(1) FROM t GROUP BY ()");
    {
        assert_eq!(
            GroupByExpr::Expressions(vec![Expr::Tuple(vec![])], vec![]),
            group_by
        );
    }

    let Select { group_by, .. } = all_dialects_where(|d| d.supports_group_by_expr())
        .verified_only_select("SELECT name, count(1) FROM t GROUP BY name, ()");
    {
        assert_eq!(
            GroupByExpr::Expressions(
                vec![
                    Identifier(Ident::new("name".to_string())),
                    Expr::Tuple(vec![])
                ],
                vec![]
            ),
            group_by
        );
    }
}

#[test]
fn test_extract_seconds_ok() {
    let dialects = all_dialects_where(|d| d.allow_extract_custom());
    let stmt = dialects.verified_expr("EXTRACT(SECONDS FROM '2 seconds'::INTERVAL)");

    assert_eq!(
        stmt,
        Expr::Extract {
            field: Seconds,
            syntax: ExtractSyntax::From,
            expr: Box::new(Expr::Cast {
                kind: CastKind::DoubleColon,
                expr: Box::new(Expr::Value(
                    (Value::SingleQuotedString("2 seconds".to_string())).with_empty_span()
                )),
                data_type: DataType::Interval {
                    fields: None,
                    precision: None
                },
                format: None,
            }),
        }
    );

    let actual_ast = dialects
        .parse_sql_statements("SELECT EXTRACT(seconds FROM '2 seconds'::INTERVAL)")
        .unwrap();

    let expected_ast = vec![Statement::Query(Box::new(Query {
        with: None,
        body: Box::new(SetExpr::Select(Box::new(Select {
            select_token: AttachedToken::empty(),
            distinct: None,
            top: None,
            top_before_distinct: false,
            projection: vec![UnnamedExpr(Expr::Extract {
                field: Seconds,
                syntax: ExtractSyntax::From,
                expr: Box::new(Expr::Cast {
                    kind: CastKind::DoubleColon,
                    expr: Box::new(Expr::Value(
                        (Value::SingleQuotedString("2 seconds".to_string())).with_empty_span(),
                    )),
                    data_type: DataType::Interval {
                        fields: None,
                        precision: None,
                    },
                    format: None,
                }),
            })],
            exclude: None,
            into: None,
            from: vec![],
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
        order_by: None,
        limit_clause: None,
        fetch: None,
        locks: vec![],
        for_clause: None,
        settings: None,
        format_clause: None,
        pipe_operators: vec![],
    }))];

    assert_eq!(actual_ast, expected_ast);
}

#[test]
fn test_extract_seconds_single_quote_ok() {
    let dialects = all_dialects_where(|d| d.allow_extract_custom());
    let stmt = dialects.verified_expr(r#"EXTRACT('seconds' FROM '2 seconds'::INTERVAL)"#);

    assert_eq!(
        stmt,
        Expr::Extract {
            field: DateTimeField::Custom(Ident {
                value: "seconds".to_string(),
                quote_style: Some('\''),
                span: Span::empty(),
            }),
            syntax: ExtractSyntax::From,
            expr: Box::new(Expr::Cast {
                kind: CastKind::DoubleColon,
                expr: Box::new(Expr::Value(
                    (Value::SingleQuotedString("2 seconds".to_string())).with_empty_span()
                )),
                data_type: DataType::Interval {
                    fields: None,
                    precision: None
                },
                format: None,
            }),
        }
    )
}

#[test]
fn test_extract_seconds_single_quote_err() {
    let sql = r#"SELECT EXTRACT('seconds' FROM '2 seconds'::INTERVAL)"#;
    let dialects = all_dialects_except(|d| d.allow_extract_single_quotes());
    let err = dialects.parse_sql_statements(sql).unwrap_err();
    assert_eq!(
        err.to_string(),
        "sql parser error: Expected: date/time field, found: 'seconds'"
    );
}

#[test]
fn test_truncate_table_with_on_cluster() {
    let sql = "TRUNCATE TABLE t ON CLUSTER cluster_name";
    match all_dialects().verified_stmt(sql) {
        Statement::Truncate(truncate) => {
            assert_eq!(truncate.on_cluster, Some(Ident::new("cluster_name")));
        }
        _ => panic!("Expected: TRUNCATE TABLE statement"),
    }

    // Omit ON CLUSTER is allowed
    all_dialects().verified_stmt("TRUNCATE TABLE t");

    assert_eq!(
        ParserError::ParserError("Expected: identifier, found: EOF".to_string()),
        all_dialects()
            .parse_sql_statements("TRUNCATE TABLE t ON CLUSTER")
            .unwrap_err()
    );
}

#[test]
fn parse_explain_with_option_list() {
    run_explain_analyze(
        all_dialects_where(|d| d.supports_explain_with_utility_options()),
        "EXPLAIN (ANALYZE false, VERBOSE true) SELECT sqrt(id) FROM foo",
        false,
        false,
        None,
        Some(vec![
            UtilityOption {
                name: Ident::new("ANALYZE"),
                arg: Some(Expr::Value((Value::Boolean(false)).with_empty_span())),
            },
            UtilityOption {
                name: Ident::new("VERBOSE"),
                arg: Some(Expr::Value((Value::Boolean(true)).with_empty_span())),
            },
        ]),
    );

    run_explain_analyze(
        all_dialects_where(|d| d.supports_explain_with_utility_options()),
        "EXPLAIN (ANALYZE ON, VERBOSE OFF) SELECT sqrt(id) FROM foo",
        false,
        false,
        None,
        Some(vec![
            UtilityOption {
                name: Ident::new("ANALYZE"),
                arg: Some(Expr::Identifier(Ident::new("ON"))),
            },
            UtilityOption {
                name: Ident::new("VERBOSE"),
                arg: Some(Expr::Identifier(Ident::new("OFF"))),
            },
        ]),
    );

    run_explain_analyze(
        all_dialects_where(|d| d.supports_explain_with_utility_options()),
        r#"EXPLAIN (FORMAT1 TEXT, FORMAT2 'JSON', FORMAT3 "XML", FORMAT4 YAML) SELECT sqrt(id) FROM foo"#,
        false,
        false,
        None,
        Some(vec![
            UtilityOption {
                name: Ident::new("FORMAT1"),
                arg: Some(Expr::Identifier(Ident::new("TEXT"))),
            },
            UtilityOption {
                name: Ident::new("FORMAT2"),
                arg: Some(Expr::Value(
                    (Value::SingleQuotedString("JSON".to_string())).with_empty_span(),
                )),
            },
            UtilityOption {
                name: Ident::new("FORMAT3"),
                arg: Some(Expr::Identifier(Ident::with_quote('"', "XML"))),
            },
            UtilityOption {
                name: Ident::new("FORMAT4"),
                arg: Some(Expr::Identifier(Ident::new("YAML"))),
            },
        ]),
    );

    run_explain_analyze(
        all_dialects_where(|d| d.supports_explain_with_utility_options()),
        r#"EXPLAIN (NUM1 10, NUM2 +10.1, NUM3 -10.2) SELECT sqrt(id) FROM foo"#,
        false,
        false,
        None,
        Some(vec![
            UtilityOption {
                name: Ident::new("NUM1"),
                arg: Some(Expr::Value(
                    (Value::Number("10".parse().unwrap(), false)).with_empty_span(),
                )),
            },
            UtilityOption {
                name: Ident::new("NUM2"),
                arg: Some(Expr::UnaryOp {
                    op: UnaryOperator::Plus,
                    expr: Box::new(Expr::Value(
                        (Value::Number("10.1".parse().unwrap(), false)).with_empty_span(),
                    )),
                }),
            },
            UtilityOption {
                name: Ident::new("NUM3"),
                arg: Some(Expr::UnaryOp {
                    op: UnaryOperator::Minus,
                    expr: Box::new(Expr::Value(
                        (Value::Number("10.2".parse().unwrap(), false)).with_empty_span(),
                    )),
                }),
            },
        ]),
    );

    let utility_options = vec![
        UtilityOption {
            name: Ident::new("ANALYZE"),
            arg: None,
        },
        UtilityOption {
            name: Ident::new("VERBOSE"),
            arg: Some(Expr::Value((Value::Boolean(true)).with_empty_span())),
        },
        UtilityOption {
            name: Ident::new("WAL"),
            arg: Some(Expr::Identifier(Ident::new("OFF"))),
        },
        UtilityOption {
            name: Ident::new("FORMAT"),
            arg: Some(Expr::Identifier(Ident::new("YAML"))),
        },
        UtilityOption {
            name: Ident::new("USER_DEF_NUM"),
            arg: Some(Expr::UnaryOp {
                op: UnaryOperator::Minus,
                expr: Box::new(Expr::Value(
                    (Value::Number("100.1".parse().unwrap(), false)).with_empty_span(),
                )),
            }),
        },
    ];
    run_explain_analyze(
        all_dialects_where(|d| d.supports_explain_with_utility_options()),
        "EXPLAIN (ANALYZE, VERBOSE true, WAL OFF, FORMAT YAML, USER_DEF_NUM -100.1) SELECT sqrt(id) FROM foo",
        false,
        false,
        None,
        Some(utility_options),
    );
}

#[test]
fn test_create_policy() {
    let sql: &str = "CREATE POLICY my_policy ON my_table \
               AS PERMISSIVE FOR SELECT \
               TO my_role, CURRENT_USER \
               USING (c0 = 1) \
               WITH CHECK (1 = 1)";

    match all_dialects().verified_stmt(sql) {
        Statement::CreatePolicy {
            name,
            table_name,
            to,
            using,
            with_check,
            ..
        } => {
            assert_eq!(name.to_string(), "my_policy");
            assert_eq!(table_name.to_string(), "my_table");
            assert_eq!(
                to,
                Some(vec![
                    Owner::Ident(Ident::new("my_role")),
                    Owner::CurrentUser
                ])
            );
            assert_eq!(
                using,
                Some(Expr::BinaryOp {
                    left: Box::new(Expr::Identifier(Ident::new("c0"))),
                    op: BinaryOperator::Eq,
                    right: Box::new(Expr::Value(
                        (Value::Number("1".parse().unwrap(), false)).with_empty_span()
                    )),
                })
            );
            assert_eq!(
                with_check,
                Some(Expr::BinaryOp {
                    left: Box::new(Expr::Value(
                        (Value::Number("1".parse().unwrap(), false)).with_empty_span()
                    )),
                    op: BinaryOperator::Eq,
                    right: Box::new(Expr::Value(
                        (Value::Number("1".parse().unwrap(), false)).with_empty_span()
                    )),
                })
            );
        }
        _ => unreachable!(),
    }

    // USING with SELECT query
    all_dialects().verified_stmt(concat!(
        "CREATE POLICY my_policy ON my_table ",
        "AS PERMISSIVE FOR SELECT ",
        "TO my_role, CURRENT_USER ",
        "USING (c0 IN (SELECT column FROM t0)) ",
        "WITH CHECK (1 = 1)"
    ));
    // omit AS / FOR / TO / USING / WITH CHECK clauses is allowed
    all_dialects().verified_stmt("CREATE POLICY my_policy ON my_table");

    // missing table name
    assert_eq!(
        all_dialects()
            .parse_sql_statements("CREATE POLICY my_policy")
            .unwrap_err()
            .to_string(),
        "sql parser error: Expected: ON, found: EOF"
    );
    // missing policy type
    assert_eq!(
        all_dialects()
            .parse_sql_statements("CREATE POLICY my_policy ON my_table AS")
            .unwrap_err()
            .to_string(),
        "sql parser error: Expected: one of PERMISSIVE or RESTRICTIVE, found: EOF"
    );
    // missing FOR command
    assert_eq!(
        all_dialects()
            .parse_sql_statements("CREATE POLICY my_policy ON my_table FOR")
            .unwrap_err()
            .to_string(),
        "sql parser error: Expected: one of ALL or SELECT or INSERT or UPDATE or DELETE, found: EOF"
    );
    // missing TO owners
    assert_eq!(
        all_dialects()
            .parse_sql_statements("CREATE POLICY my_policy ON my_table TO")
            .unwrap_err()
            .to_string(),
        "sql parser error: Expected: CURRENT_USER, CURRENT_ROLE, SESSION_USER or identifier after OWNER TO. sql parser error: Expected: identifier, found: EOF"
    );
    // missing USING expression
    assert_eq!(
        all_dialects()
            .parse_sql_statements("CREATE POLICY my_policy ON my_table USING")
            .unwrap_err()
            .to_string(),
        "sql parser error: Expected: (, found: EOF"
    );
    // missing WITH CHECK expression
    assert_eq!(
        all_dialects()
            .parse_sql_statements("CREATE POLICY my_policy ON my_table WITH CHECK")
            .unwrap_err()
            .to_string(),
        "sql parser error: Expected: (, found: EOF"
    );
}

#[test]
fn test_drop_policy() {
    let sql = "DROP POLICY IF EXISTS my_policy ON my_table RESTRICT";
    match all_dialects().verified_stmt(sql) {
        Statement::DropPolicy {
            if_exists,
            name,
            table_name,
            drop_behavior,
        } => {
            assert_eq!(if_exists, true);
            assert_eq!(name.to_string(), "my_policy");
            assert_eq!(table_name.to_string(), "my_table");
            assert_eq!(drop_behavior, Some(DropBehavior::Restrict));
        }
        _ => unreachable!(),
    }

    // omit IF EXISTS is allowed
    all_dialects().verified_stmt("DROP POLICY my_policy ON my_table CASCADE");
    // omit option is allowed
    all_dialects().verified_stmt("DROP POLICY my_policy ON my_table");

    // missing table name
    assert_eq!(
        all_dialects()
            .parse_sql_statements("DROP POLICY my_policy")
            .unwrap_err()
            .to_string(),
        "sql parser error: Expected: ON, found: EOF"
    );
    // Wrong option name
    assert_eq!(
        all_dialects()
            .parse_sql_statements("DROP POLICY my_policy ON my_table WRONG")
            .unwrap_err()
            .to_string(),
        "sql parser error: Expected: end of statement, found: WRONG"
    );
}

#[test]
fn test_alter_policy() {
    match verified_stmt("ALTER POLICY old_policy ON my_table RENAME TO new_policy") {
        Statement::AlterPolicy {
            name,
            table_name,
            operation,
            ..
        } => {
            assert_eq!(name.to_string(), "old_policy");
            assert_eq!(table_name.to_string(), "my_table");
            assert_eq!(
                operation,
                AlterPolicyOperation::Rename {
                    new_name: Ident::new("new_policy")
                }
            );
        }
        _ => unreachable!(),
    }

    match verified_stmt(concat!(
        "ALTER POLICY my_policy ON my_table TO CURRENT_USER ",
        "USING ((SELECT c0)) WITH CHECK (c0 > 0)"
    )) {
        Statement::AlterPolicy {
            name, table_name, ..
        } => {
            assert_eq!(name.to_string(), "my_policy");
            assert_eq!(table_name.to_string(), "my_table");
        }
        _ => unreachable!(),
    }

    // omit TO / USING / WITH CHECK clauses is allowed
    verified_stmt("ALTER POLICY my_policy ON my_table");

    // mixing RENAME and APPLY expressions
    assert_eq!(
        parse_sql_statements("ALTER POLICY old_policy ON my_table TO public RENAME TO new_policy")
            .unwrap_err()
            .to_string(),
        "sql parser error: Expected: end of statement, found: RENAME"
    );
    assert_eq!(
        parse_sql_statements("ALTER POLICY old_policy ON my_table RENAME TO new_policy TO public")
            .unwrap_err()
            .to_string(),
        "sql parser error: Expected: end of statement, found: TO"
    );
    // missing TO in RENAME TO
    assert_eq!(
        parse_sql_statements("ALTER POLICY old_policy ON my_table RENAME")
            .unwrap_err()
            .to_string(),
        "sql parser error: Expected: TO, found: EOF"
    );
    // missing new name in RENAME TO
    assert_eq!(
        parse_sql_statements("ALTER POLICY old_policy ON my_table RENAME TO")
            .unwrap_err()
            .to_string(),
        "sql parser error: Expected: identifier, found: EOF"
    );

    // missing the expression in USING
    assert_eq!(
        parse_sql_statements("ALTER POLICY my_policy ON my_table USING")
            .unwrap_err()
            .to_string(),
        "sql parser error: Expected: (, found: EOF"
    );
    // missing the expression in WITH CHECK
    assert_eq!(
        parse_sql_statements("ALTER POLICY my_policy ON my_table WITH CHECK")
            .unwrap_err()
            .to_string(),
        "sql parser error: Expected: (, found: EOF"
    );
}

#[test]
fn test_create_connector() {
    let sql = "CREATE CONNECTOR my_connector \
               TYPE 'jdbc' \
               URL 'jdbc:mysql://localhost:3306/mydb' \
               WITH DCPROPERTIES('user' = 'root', 'password' = 'password')";
    let dialects = all_dialects();
    match dialects.verified_stmt(sql) {
        Statement::CreateConnector(CreateConnector {
            name,
            connector_type,
            url,
            with_dcproperties,
            ..
        }) => {
            assert_eq!(name.to_string(), "my_connector");
            assert_eq!(connector_type, Some("jdbc".to_string()));
            assert_eq!(url, Some("jdbc:mysql://localhost:3306/mydb".to_string()));
            assert_eq!(
                with_dcproperties,
                Some(vec![
                    SqlOption::KeyValue {
                        key: Ident::with_quote('\'', "user"),
                        value: Expr::Value(
                            (Value::SingleQuotedString("root".to_string())).with_empty_span()
                        )
                    },
                    SqlOption::KeyValue {
                        key: Ident::with_quote('\'', "password"),
                        value: Expr::Value(
                            (Value::SingleQuotedString("password".to_string())).with_empty_span()
                        )
                    }
                ])
            );
        }
        _ => unreachable!(),
    }

    // omit IF NOT EXISTS/TYPE/URL/COMMENT/WITH DCPROPERTIES clauses is allowed
    dialects.verified_stmt("CREATE CONNECTOR my_connector");

    // missing connector name
    assert_eq!(
        dialects
            .parse_sql_statements("CREATE CONNECTOR")
            .unwrap_err()
            .to_string(),
        "sql parser error: Expected: identifier, found: EOF"
    );
}

#[test]
fn test_drop_connector() {
    let dialects = all_dialects();
    match dialects.verified_stmt("DROP CONNECTOR IF EXISTS my_connector") {
        Statement::DropConnector { if_exists, name } => {
            assert_eq!(if_exists, true);
            assert_eq!(name.to_string(), "my_connector");
        }
        _ => unreachable!(),
    }

    // omit IF EXISTS is allowed
    dialects.verified_stmt("DROP CONNECTOR my_connector");

    // missing connector name
    assert_eq!(
        dialects
            .parse_sql_statements("DROP CONNECTOR")
            .unwrap_err()
            .to_string(),
        "sql parser error: Expected: identifier, found: EOF"
    );
}

#[test]
fn test_alter_connector() {
    let dialects = all_dialects();
    match dialects.verified_stmt(
        "ALTER CONNECTOR my_connector SET DCPROPERTIES('user' = 'root', 'password' = 'password')",
    ) {
        Statement::AlterConnector {
            name,
            properties,
            url,
            owner,
        } => {
            assert_eq!(name.to_string(), "my_connector");
            assert_eq!(
                properties,
                Some(vec![
                    SqlOption::KeyValue {
                        key: Ident::with_quote('\'', "user"),
                        value: Expr::Value(
                            (Value::SingleQuotedString("root".to_string())).with_empty_span()
                        )
                    },
                    SqlOption::KeyValue {
                        key: Ident::with_quote('\'', "password"),
                        value: Expr::Value(
                            (Value::SingleQuotedString("password".to_string())).with_empty_span()
                        )
                    }
                ])
            );
            assert_eq!(url, None);
            assert_eq!(owner, None);
        }
        _ => unreachable!(),
    }

    match dialects
        .verified_stmt("ALTER CONNECTOR my_connector SET URL 'jdbc:mysql://localhost:3306/mydb'")
    {
        Statement::AlterConnector {
            name,
            properties,
            url,
            owner,
        } => {
            assert_eq!(name.to_string(), "my_connector");
            assert_eq!(properties, None);
            assert_eq!(url, Some("jdbc:mysql://localhost:3306/mydb".to_string()));
            assert_eq!(owner, None);
        }
        _ => unreachable!(),
    }

    match dialects.verified_stmt("ALTER CONNECTOR my_connector SET OWNER USER 'root'") {
        Statement::AlterConnector {
            name,
            properties,
            url,
            owner,
        } => {
            assert_eq!(name.to_string(), "my_connector");
            assert_eq!(properties, None);
            assert_eq!(url, None);
            assert_eq!(
                owner,
                Some(AlterConnectorOwner::User(Ident::with_quote('\'', "root")))
            );
        }
        _ => unreachable!(),
    }

    match dialects.verified_stmt("ALTER CONNECTOR my_connector SET OWNER ROLE 'admin'") {
        Statement::AlterConnector {
            name,
            properties,
            url,
            owner,
        } => {
            assert_eq!(name.to_string(), "my_connector");
            assert_eq!(properties, None);
            assert_eq!(url, None);
            assert_eq!(
                owner,
                Some(AlterConnectorOwner::Role(Ident::with_quote('\'', "admin")))
            );
        }
        _ => unreachable!(),
    }

    // Wrong option name
    assert_eq!(
        dialects
            .parse_sql_statements(
                "ALTER CONNECTOR my_connector SET WRONG 'jdbc:mysql://localhost:3306/mydb'"
            )
            .unwrap_err()
            .to_string(),
        "sql parser error: Expected: end of statement, found: WRONG"
    );
}

#[test]
fn test_select_where_with_like_or_ilike_any() {
    verified_stmt(r#"SELECT * FROM x WHERE a ILIKE ANY '%abc%'"#);
    verified_stmt(r#"SELECT * FROM x WHERE a LIKE ANY '%abc%'"#);
    verified_stmt(r#"SELECT * FROM x WHERE a ILIKE ANY ('%Jo%oe%', 'T%e')"#);
    verified_stmt(r#"SELECT * FROM x WHERE a LIKE ANY ('%Jo%oe%', 'T%e')"#);
}

#[test]
fn test_any_some_all_comparison() {
    verified_stmt("SELECT c1 FROM tbl WHERE c1 = ANY(SELECT c2 FROM tbl)");
    verified_stmt("SELECT c1 FROM tbl WHERE c1 >= ALL(SELECT c2 FROM tbl)");
    verified_stmt("SELECT c1 FROM tbl WHERE c1 <> SOME(SELECT c2 FROM tbl)");
    verified_stmt("SELECT 1 = ANY(WITH x AS (SELECT 1) SELECT * FROM x)");
}

#[test]
fn test_alias_equal_expr() {
    let dialects = all_dialects_where(|d| d.supports_eq_alias_assignment());
    let sql = r#"SELECT some_alias = some_column FROM some_table"#;
    let expected = r#"SELECT some_column AS some_alias FROM some_table"#;
    let _ = dialects.one_statement_parses_to(sql, expected);

    let sql = r#"SELECT some_alias = (a*b) FROM some_table"#;
    let expected = r#"SELECT (a * b) AS some_alias FROM some_table"#;
    let _ = dialects.one_statement_parses_to(sql, expected);

    let dialects = all_dialects_where(|d| !d.supports_eq_alias_assignment());
    let sql = r#"SELECT x = (a * b) FROM some_table"#;
    let expected = r#"SELECT x = (a * b) FROM some_table"#;
    let _ = dialects.one_statement_parses_to(sql, expected);
}

#[test]
fn test_try_convert() {
    let dialects =
        all_dialects_where(|d| d.supports_try_convert() && d.convert_type_before_value());
    dialects.verified_expr("TRY_CONVERT(VARCHAR(MAX), 'foo')");

    let dialects =
        all_dialects_where(|d| d.supports_try_convert() && !d.convert_type_before_value());
    dialects.verified_expr("TRY_CONVERT('foo', VARCHAR(MAX))");
}

#[test]
fn parse_method_select() {
    let _ = verified_only_select(
        "SELECT LEFT('abc', 1).value('.', 'NVARCHAR(MAX)').value('.', 'NVARCHAR(MAX)') AS T",
    );
    let _ = verified_only_select("SELECT STUFF((SELECT ',' + name FROM sys.objects FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 1, '') AS T");
    let _ = verified_only_select("SELECT CAST(column AS XML).value('.', 'NVARCHAR(MAX)') AS T");

    // `CONVERT` support
    let dialects =
        all_dialects_where(|d| d.supports_try_convert() && d.convert_type_before_value());
    let _ = dialects.verified_only_select("SELECT CONVERT(XML, '<Book>abc</Book>').value('.', 'NVARCHAR(MAX)').value('.', 'NVARCHAR(MAX)') AS T");
}

#[test]
fn parse_method_expr() {
    let expr =
        verified_expr("LEFT('abc', 1).value('.', 'NVARCHAR(MAX)').value('.', 'NVARCHAR(MAX)')");
    match expr {
        Expr::CompoundFieldAccess { root, access_chain } => {
            assert!(matches!(*root, Expr::Function(_)));
            assert!(matches!(
                access_chain[..],
                [
                    AccessExpr::Dot(Expr::Function(_)),
                    AccessExpr::Dot(Expr::Function(_))
                ]
            ));
        }
        _ => unreachable!(),
    }

    let expr = verified_expr(
        "(SELECT ',' + name FROM sys.objects FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)')",
    );
    match expr {
        Expr::CompoundFieldAccess { root, access_chain } => {
            assert!(matches!(*root, Expr::Subquery(_)));
            assert!(matches!(
                access_chain[..],
                [AccessExpr::Dot(Expr::Function(_))]
            ));
        }
        _ => unreachable!(),
    }
    let expr = verified_expr("CAST(column AS XML).value('.', 'NVARCHAR(MAX)')");
    match expr {
        Expr::CompoundFieldAccess { root, access_chain } => {
            assert!(matches!(*root, Expr::Cast { .. }));
            assert!(matches!(
                access_chain[..],
                [AccessExpr::Dot(Expr::Function(_))]
            ));
        }
        _ => unreachable!(),
    }

    // `CONVERT` support
    let dialects =
        all_dialects_where(|d| d.supports_try_convert() && d.convert_type_before_value());
    let expr = dialects.verified_expr(
        "CONVERT(XML, '<Book>abc</Book>').value('.', 'NVARCHAR(MAX)').value('.', 'NVARCHAR(MAX)')",
    );
    match expr {
        Expr::CompoundFieldAccess { root, access_chain } => {
            assert!(matches!(*root, Expr::Convert { .. }));
            assert!(matches!(
                access_chain[..],
                [
                    AccessExpr::Dot(Expr::Function(_)),
                    AccessExpr::Dot(Expr::Function(_))
                ]
            ));
        }
        _ => unreachable!(),
    }
}

#[test]
fn test_show_dbs_schemas_tables_views() {
    // These statements are parsed the same by all dialects
    let stmts = vec![
        "SHOW DATABASES",
        "SHOW SCHEMAS",
        "SHOW TABLES",
        "SHOW VIEWS",
        "SHOW TABLES IN db1",
        "SHOW VIEWS FROM db1",
        "SHOW MATERIALIZED VIEWS",
        "SHOW MATERIALIZED VIEWS IN db1",
        "SHOW MATERIALIZED VIEWS FROM db1",
    ];
    for stmt in stmts {
        verified_stmt(stmt);
    }

    // These statements are parsed the same by all dialects
    // except for how the parser interprets the location of
    // LIKE option (infix/suffix)
    let stmts = vec!["SHOW DATABASES LIKE '%abc'", "SHOW SCHEMAS LIKE '%abc'"];
    for stmt in stmts {
        all_dialects_where(|d| d.supports_show_like_before_in()).verified_stmt(stmt);
        all_dialects_where(|d| !d.supports_show_like_before_in()).verified_stmt(stmt);
    }

    // These statements are only parsed by dialects that
    // support the LIKE option in the suffix
    let stmts = vec![
        "SHOW TABLES IN db1 'abc'",
        "SHOW VIEWS IN db1 'abc'",
        "SHOW VIEWS FROM db1 'abc'",
        "SHOW MATERIALIZED VIEWS IN db1 'abc'",
        "SHOW MATERIALIZED VIEWS FROM db1 'abc'",
    ];
    for stmt in stmts {
        all_dialects_where(|d| !d.supports_show_like_before_in()).verified_stmt(stmt);
    }
}

#[test]
fn parse_listen_channel() {
    let dialects = all_dialects_where(|d| d.supports_listen_notify());

    match dialects.verified_stmt("LISTEN test1") {
        Statement::LISTEN { channel } => {
            assert_eq!(Ident::new("test1"), channel);
        }
        _ => unreachable!(),
    };

    assert_eq!(
        dialects.parse_sql_statements("LISTEN *").unwrap_err(),
        ParserError::ParserError("Expected: identifier, found: *".to_string())
    );

    let dialects = all_dialects_where(|d| !d.supports_listen_notify());

    assert_eq!(
        dialects.parse_sql_statements("LISTEN test1").unwrap_err(),
        ParserError::ParserError("Expected: an SQL statement, found: LISTEN".to_string())
    );
}

#[test]
fn parse_unlisten_channel() {
    let dialects = all_dialects_where(|d| d.supports_listen_notify());

    match dialects.verified_stmt("UNLISTEN test1") {
        Statement::UNLISTEN { channel } => {
            assert_eq!(Ident::new("test1"), channel);
        }
        _ => unreachable!(),
    };

    match dialects.verified_stmt("UNLISTEN *") {
        Statement::UNLISTEN { channel } => {
            assert_eq!(Ident::new("*"), channel);
        }
        _ => unreachable!(),
    };

    assert_eq!(
        dialects.parse_sql_statements("UNLISTEN +").unwrap_err(),
        ParserError::ParserError("Expected: wildcard or identifier, found: +".to_string())
    );

    let dialects = all_dialects_where(|d| !d.supports_listen_notify());

    assert_eq!(
        dialects.parse_sql_statements("UNLISTEN test1").unwrap_err(),
        ParserError::ParserError("Expected: an SQL statement, found: UNLISTEN".to_string())
    );
}

#[test]
fn parse_notify_channel() {
    let dialects = all_dialects_where(|d| d.supports_listen_notify());

    match dialects.verified_stmt("NOTIFY test1") {
        Statement::NOTIFY { channel, payload } => {
            assert_eq!(Ident::new("test1"), channel);
            assert_eq!(payload, None);
        }
        _ => unreachable!(),
    };

    match dialects.verified_stmt("NOTIFY test1, 'this is a test notification'") {
        Statement::NOTIFY {
            channel,
            payload: Some(payload),
        } => {
            assert_eq!(Ident::new("test1"), channel);
            assert_eq!("this is a test notification", payload);
        }
        _ => unreachable!(),
    };

    assert_eq!(
        dialects.parse_sql_statements("NOTIFY *").unwrap_err(),
        ParserError::ParserError("Expected: identifier, found: *".to_string())
    );
    assert_eq!(
        dialects
            .parse_sql_statements("NOTIFY test1, *")
            .unwrap_err(),
        ParserError::ParserError("Expected: literal string, found: *".to_string())
    );

    let sql_statements = [
        "NOTIFY test1",
        "NOTIFY test1, 'this is a test notification'",
    ];
    let dialects = all_dialects_where(|d| !d.supports_listen_notify());

    for &sql in &sql_statements {
        assert_eq!(
            dialects.parse_sql_statements(sql).unwrap_err(),
            ParserError::ParserError("Expected: an SQL statement, found: NOTIFY".to_string())
        );
    }
}

#[test]
fn parse_load_data() {
    let dialects = all_dialects_where(|d| d.supports_load_data());
    let only_supports_load_extension_dialects =
        all_dialects_where(|d| !d.supports_load_data() && d.supports_load_extension());
    let not_supports_load_dialects =
        all_dialects_where(|d| !d.supports_load_data() && !d.supports_load_extension());

    let sql = "LOAD DATA INPATH '/local/path/to/data.txt' INTO TABLE test.my_table";
    match dialects.verified_stmt(sql) {
        Statement::LoadData {
            local,
            inpath,
            overwrite,
            table_name,
            partitioned,
            table_format,
        } => {
            assert_eq!(false, local);
            assert_eq!("/local/path/to/data.txt", inpath);
            assert_eq!(false, overwrite);
            assert_eq!(
                ObjectName::from(vec![Ident::new("test"), Ident::new("my_table")]),
                table_name
            );
            assert_eq!(None, partitioned);
            assert_eq!(None, table_format);
        }
        _ => unreachable!(),
    };

    // with OVERWRITE keyword
    let sql = "LOAD DATA INPATH '/local/path/to/data.txt' OVERWRITE INTO TABLE my_table";
    match dialects.verified_stmt(sql) {
        Statement::LoadData {
            local,
            inpath,
            overwrite,
            table_name,
            partitioned,
            table_format,
        } => {
            assert_eq!(false, local);
            assert_eq!("/local/path/to/data.txt", inpath);
            assert_eq!(true, overwrite);
            assert_eq!(ObjectName::from(vec![Ident::new("my_table")]), table_name);
            assert_eq!(None, partitioned);
            assert_eq!(None, table_format);
        }
        _ => unreachable!(),
    };

    assert_eq!(
        only_supports_load_extension_dialects
            .parse_sql_statements(sql)
            .unwrap_err(),
        ParserError::ParserError("Expected: end of statement, found: INPATH".to_string())
    );
    assert_eq!(
        not_supports_load_dialects
            .parse_sql_statements(sql)
            .unwrap_err(),
        ParserError::ParserError(
            "Expected: `DATA` or an extension name after `LOAD`, found: INPATH".to_string()
        )
    );

    // with LOCAL keyword
    let sql = "LOAD DATA LOCAL INPATH '/local/path/to/data.txt' INTO TABLE test.my_table";
    match dialects.verified_stmt(sql) {
        Statement::LoadData {
            local,
            inpath,
            overwrite,
            table_name,
            partitioned,
            table_format,
        } => {
            assert_eq!(true, local);
            assert_eq!("/local/path/to/data.txt", inpath);
            assert_eq!(false, overwrite);
            assert_eq!(
                ObjectName::from(vec![Ident::new("test"), Ident::new("my_table")]),
                table_name
            );
            assert_eq!(None, partitioned);
            assert_eq!(None, table_format);
        }
        _ => unreachable!(),
    };

    assert_eq!(
        only_supports_load_extension_dialects
            .parse_sql_statements(sql)
            .unwrap_err(),
        ParserError::ParserError("Expected: end of statement, found: LOCAL".to_string())
    );
    assert_eq!(
        not_supports_load_dialects
            .parse_sql_statements(sql)
            .unwrap_err(),
        ParserError::ParserError(
            "Expected: `DATA` or an extension name after `LOAD`, found: LOCAL".to_string()
        )
    );

    // with PARTITION  clause
    let sql = "LOAD DATA LOCAL INPATH '/local/path/to/data.txt' INTO TABLE my_table PARTITION (year = 2024, month = 11)";
    match dialects.verified_stmt(sql) {
        Statement::LoadData {
            local,
            inpath,
            overwrite,
            table_name,
            partitioned,
            table_format,
        } => {
            assert_eq!(true, local);
            assert_eq!("/local/path/to/data.txt", inpath);
            assert_eq!(false, overwrite);
            assert_eq!(ObjectName::from(vec![Ident::new("my_table")]), table_name);
            assert_eq!(
                Some(vec![
                    Expr::BinaryOp {
                        left: Box::new(Expr::Identifier(Ident::new("year"))),
                        op: BinaryOperator::Eq,
                        right: Box::new(Expr::Value(
                            (Value::Number("2024".parse().unwrap(), false)).with_empty_span()
                        )),
                    },
                    Expr::BinaryOp {
                        left: Box::new(Expr::Identifier(Ident::new("month"))),
                        op: BinaryOperator::Eq,
                        right: Box::new(Expr::Value(
                            (Value::Number("11".parse().unwrap(), false)).with_empty_span()
                        )),
                    }
                ]),
                partitioned
            );
            assert_eq!(None, table_format);
        }
        _ => unreachable!(),
    };

    // with PARTITION  clause
    let sql = "LOAD DATA LOCAL INPATH '/local/path/to/data.txt' OVERWRITE INTO TABLE good.my_table PARTITION (year = 2024, month = 11) INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'";
    match dialects.verified_stmt(sql) {
        Statement::LoadData {
            local,
            inpath,
            overwrite,
            table_name,
            partitioned,
            table_format,
        } => {
            assert_eq!(true, local);
            assert_eq!("/local/path/to/data.txt", inpath);
            assert_eq!(true, overwrite);
            assert_eq!(
                ObjectName::from(vec![Ident::new("good"), Ident::new("my_table")]),
                table_name
            );
            assert_eq!(
                Some(vec![
                    Expr::BinaryOp {
                        left: Box::new(Expr::Identifier(Ident::new("year"))),
                        op: BinaryOperator::Eq,
                        right: Box::new(Expr::Value(
                            (Value::Number("2024".parse().unwrap(), false)).with_empty_span()
                        )),
                    },
                    Expr::BinaryOp {
                        left: Box::new(Expr::Identifier(Ident::new("month"))),
                        op: BinaryOperator::Eq,
                        right: Box::new(Expr::Value(
                            (Value::Number("11".parse().unwrap(), false)).with_empty_span()
                        )),
                    }
                ]),
                partitioned
            );
            assert_eq!(
                Some(HiveLoadDataFormat {
                    serde: Expr::Value(
                        (Value::SingleQuotedString(
                            "org.apache.hadoop.hive.serde2.OpenCSVSerde".to_string()
                        ))
                        .with_empty_span()
                    ),
                    input_format: Expr::Value(
                        (Value::SingleQuotedString(
                            "org.apache.hadoop.mapred.TextInputFormat".to_string()
                        ))
                        .with_empty_span()
                    )
                }),
                table_format
            );
        }
        _ => unreachable!(),
    };

    // negative test case
    let sql = "LOAD DATA2 LOCAL INPATH '/local/path/to/data.txt' INTO TABLE test.my_table";
    assert_eq!(
        dialects.parse_sql_statements(sql).unwrap_err(),
        ParserError::ParserError(
            "Expected: `DATA` or an extension name after `LOAD`, found: DATA2".to_string()
        )
    );
}

#[test]
fn test_load_extension() {
    let dialects = all_dialects_where(|d| d.supports_load_extension());
    let not_supports_load_extension_dialects = all_dialects_where(|d| !d.supports_load_extension());
    let sql = "LOAD my_extension";

    match dialects.verified_stmt(sql) {
        Statement::Load { extension_name } => {
            assert_eq!(Ident::new("my_extension"), extension_name);
        }
        _ => unreachable!(),
    };

    assert_eq!(
        not_supports_load_extension_dialects
            .parse_sql_statements(sql)
            .unwrap_err(),
        ParserError::ParserError(
            "Expected: `DATA` or an extension name after `LOAD`, found: my_extension".to_string()
        )
    );

    let sql = "LOAD 'filename'";

    match dialects.verified_stmt(sql) {
        Statement::Load { extension_name } => {
            assert_eq!(
                Ident {
                    value: "filename".to_string(),
                    quote_style: Some('\''),
                    span: Span::empty(),
                },
                extension_name
            );
        }
        _ => unreachable!(),
    };
}

#[test]
fn test_select_top() {
    let dialects = all_dialects_where(|d| d.supports_top_before_distinct());
    dialects.one_statement_parses_to("SELECT ALL * FROM tbl", "SELECT * FROM tbl");
    dialects.verified_stmt("SELECT TOP 3 * FROM tbl");
    dialects.one_statement_parses_to("SELECT TOP 3 ALL * FROM tbl", "SELECT TOP 3 * FROM tbl");
    dialects.verified_stmt("SELECT TOP 3 DISTINCT * FROM tbl");
    dialects.verified_stmt("SELECT TOP 3 DISTINCT a, b, c FROM tbl");
}

#[test]
fn parse_bang_not() {
    let dialects = all_dialects_where(|d| d.supports_bang_not_operator());
    let sql = "SELECT !a, !(b > 3)";
    let Select { projection, .. } = dialects.verified_only_select(sql);

    for (i, expr) in [
        Box::new(Expr::Identifier(Ident::new("a"))),
        Box::new(Expr::Nested(Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Identifier(Ident::new("b"))),
            op: BinaryOperator::Gt,
            right: Box::new(Expr::Value(
                Value::Number("3".parse().unwrap(), false).with_empty_span(),
            )),
        }))),
    ]
    .into_iter()
    .enumerate()
    {
        assert_eq!(
            SelectItem::UnnamedExpr(Expr::UnaryOp {
                op: UnaryOperator::BangNot,
                expr
            }),
            projection[i]
        )
    }

    let sql_statements = ["SELECT a!", "SELECT a ! b", "SELECT a ! as b"];

    for &sql in &sql_statements {
        assert_eq!(
            dialects.parse_sql_statements(sql).unwrap_err(),
            ParserError::ParserError("No infix parser for token ExclamationMark".to_string())
        );
    }

    let sql_statements = ["SELECT !a", "SELECT !a b", "SELECT !a as b"];
    let dialects = all_dialects_where(|d| !d.supports_bang_not_operator());

    for &sql in &sql_statements {
        assert_eq!(
            dialects.parse_sql_statements(sql).unwrap_err(),
            ParserError::ParserError("Expected: an expression, found: !".to_string())
        );
    }
}

#[test]
fn parse_factorial_operator() {
    let dialects = all_dialects_where(|d| d.supports_factorial_operator());
    let sql = "SELECT a!, (b + c)!";
    let Select { projection, .. } = dialects.verified_only_select(sql);

    for (i, expr) in [
        Box::new(Expr::Identifier(Ident::new("a"))),
        Box::new(Expr::Nested(Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Identifier(Ident::new("b"))),
            op: BinaryOperator::Plus,
            right: Box::new(Expr::Identifier(Ident::new("c"))),
        }))),
    ]
    .into_iter()
    .enumerate()
    {
        assert_eq!(
            SelectItem::UnnamedExpr(Expr::UnaryOp {
                op: UnaryOperator::PGPostfixFactorial,
                expr
            }),
            projection[i]
        )
    }

    let sql_statements = ["SELECT !a", "SELECT !a b", "SELECT !a as b"];

    for &sql in &sql_statements {
        assert_eq!(
            dialects.parse_sql_statements(sql).unwrap_err(),
            ParserError::ParserError("Expected: an expression, found: !".to_string())
        );
    }

    let sql_statements = ["SELECT a!", "SELECT a ! b", "SELECT a ! as b"];

    // Due to the exclamation mark, which is both part of the `bang not` operator
    // and the `factorial` operator,  additional filtering not supports
    // `bang not` operator is required here.
    let dialects =
        all_dialects_where(|d| !d.supports_factorial_operator() && !d.supports_bang_not_operator());

    for &sql in &sql_statements {
        assert_eq!(
            dialects.parse_sql_statements(sql).unwrap_err(),
            ParserError::ParserError("No infix parser for token ExclamationMark".to_string())
        );
    }

    // Due to the exclamation mark, which is both part of the `bang not` operator
    // and the `factorial` operator,  additional filtering supports
    // `bang not` operator is required here.
    let dialects =
        all_dialects_where(|d| !d.supports_factorial_operator() && d.supports_bang_not_operator());

    for &sql in &sql_statements {
        assert_eq!(
            dialects.parse_sql_statements(sql).unwrap_err(),
            ParserError::ParserError("No infix parser for token ExclamationMark".to_string())
        );
    }
}

#[test]
fn parse_comments() {
    match all_dialects_where(|d| d.supports_comment_on())
        .verified_stmt("COMMENT ON COLUMN tab.name IS 'comment'")
    {
        Statement::Comment {
            object_type,
            object_name,
            comment: Some(comment),
            if_exists,
        } => {
            assert_eq!("comment", comment);
            assert_eq!("tab.name", object_name.to_string());
            assert_eq!(CommentObject::Column, object_type);
            assert!(!if_exists);
        }
        _ => unreachable!(),
    }

    let object_types = [
        ("COLUMN", CommentObject::Column),
        ("EXTENSION", CommentObject::Extension),
        ("TABLE", CommentObject::Table),
        ("SCHEMA", CommentObject::Schema),
        ("DATABASE", CommentObject::Database),
        ("USER", CommentObject::User),
        ("ROLE", CommentObject::Role),
    ];
    for (keyword, expected_object_type) in object_types.iter() {
        match all_dialects_where(|d| d.supports_comment_on())
            .verified_stmt(format!("COMMENT IF EXISTS ON {keyword} db.t0 IS 'comment'").as_str())
        {
            Statement::Comment {
                object_type,
                object_name,
                comment: Some(comment),
                if_exists,
            } => {
                assert_eq!("comment", comment);
                assert_eq!("db.t0", object_name.to_string());
                assert_eq!(*expected_object_type, object_type);
                assert!(if_exists);
            }
            _ => unreachable!(),
        }
    }

    match all_dialects_where(|d| d.supports_comment_on())
        .verified_stmt("COMMENT IF EXISTS ON TABLE public.tab IS NULL")
    {
        Statement::Comment {
            object_type,
            object_name,
            comment: None,
            if_exists,
        } => {
            assert_eq!("public.tab", object_name.to_string());
            assert_eq!(CommentObject::Table, object_type);
            assert!(if_exists);
        }
        _ => unreachable!(),
    }

    // missing IS statement
    assert_eq!(
        all_dialects_where(|d| d.supports_comment_on())
            .parse_sql_statements("COMMENT ON TABLE t0")
            .unwrap_err(),
        ParserError::ParserError("Expected: IS, found: EOF".to_string())
    );

    // missing comment literal
    assert_eq!(
        all_dialects_where(|d| d.supports_comment_on())
            .parse_sql_statements("COMMENT ON TABLE t0 IS")
            .unwrap_err(),
        ParserError::ParserError("Expected: literal string, found: EOF".to_string())
    );

    // unknown object type
    assert_eq!(
        all_dialects_where(|d| d.supports_comment_on())
            .parse_sql_statements("COMMENT ON UNKNOWN t0 IS 'comment'")
            .unwrap_err(),
        ParserError::ParserError("Expected: comment object_type, found: UNKNOWN".to_string())
    );
}

#[test]
fn parse_create_table_select() {
    let dialects = all_dialects_where(|d| d.supports_create_table_select());
    let sql_1 = r#"CREATE TABLE foo (baz INT) SELECT bar"#;
    let expected = r#"CREATE TABLE foo (baz INT) AS SELECT bar"#;
    let _ = dialects.one_statement_parses_to(sql_1, expected);

    let sql_2 = r#"CREATE TABLE foo (baz INT, name STRING) SELECT bar, oth_name FROM test.table_a"#;
    let expected =
        r#"CREATE TABLE foo (baz INT, name STRING) AS SELECT bar, oth_name FROM test.table_a"#;
    let _ = dialects.one_statement_parses_to(sql_2, expected);

    let dialects = all_dialects_where(|d| !d.supports_create_table_select());
    for sql in [sql_1, sql_2] {
        assert_eq!(
            dialects.parse_sql_statements(sql).unwrap_err(),
            ParserError::ParserError("Expected: end of statement, found: SELECT".to_string())
        );
    }
}

#[test]
fn test_reserved_keywords_for_identifiers() {
    let dialects = all_dialects_where(|d| d.is_reserved_for_identifier(Keyword::INTERVAL));
    // Dialects that reserve the word INTERVAL will not allow it as an unquoted identifier
    let sql = "SELECT MAX(interval) FROM tbl";
    assert_eq!(
        dialects.parse_sql_statements(sql),
        Err(ParserError::ParserError(
            "Expected: an expression, found: )".to_string()
        ))
    );

    // Dialects that do not reserve the word INTERVAL will allow it
    let dialects = all_dialects_where(|d| !d.is_reserved_for_identifier(Keyword::INTERVAL));
    let sql = "SELECT MAX(interval) FROM tbl";
    dialects.parse_sql_statements(sql).unwrap();
}

#[test]
fn parse_create_table_with_bit_types() {
    let sql = "CREATE TABLE t (a BIT, b BIT VARYING, c BIT(42), d BIT VARYING(43))";
    match verified_stmt(sql) {
        Statement::CreateTable(CreateTable { columns, .. }) => {
            assert_eq!(columns.len(), 4);
            assert_eq!(columns[0].data_type, DataType::Bit(None));
            assert_eq!(columns[0].to_string(), "a BIT");
            assert_eq!(columns[1].data_type, DataType::BitVarying(None));
            assert_eq!(columns[1].to_string(), "b BIT VARYING");
            assert_eq!(columns[2].data_type, DataType::Bit(Some(42)));
            assert_eq!(columns[2].to_string(), "c BIT(42)");
            assert_eq!(columns[3].data_type, DataType::BitVarying(Some(43)));
            assert_eq!(columns[3].to_string(), "d BIT VARYING(43)");
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_composite_access_expr() {
    assert_eq!(
        verified_expr("f(a).b"),
        Expr::CompoundFieldAccess {
            root: Box::new(Expr::Function(Function {
                name: ObjectName::from(vec![Ident::new("f")]),
                uses_odbc_syntax: false,
                parameters: FunctionArguments::None,
                args: FunctionArguments::List(FunctionArgumentList {
                    duplicate_treatment: None,
                    args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(
                        Expr::Identifier(Ident::new("a"))
                    ))],
                    clauses: vec![],
                }),
                null_treatment: None,
                filter: None,
                over: None,
                within_group: vec![]
            })),
            access_chain: vec![AccessExpr::Dot(Expr::Identifier(Ident::new("b")))]
        }
    );

    // Nested Composite Access
    assert_eq!(
        verified_expr("f(a).b.c"),
        Expr::CompoundFieldAccess {
            root: Box::new(Expr::Function(Function {
                name: ObjectName::from(vec![Ident::new("f")]),
                uses_odbc_syntax: false,
                parameters: FunctionArguments::None,
                args: FunctionArguments::List(FunctionArgumentList {
                    duplicate_treatment: None,
                    args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(
                        Expr::Identifier(Ident::new("a"))
                    ))],
                    clauses: vec![],
                }),
                null_treatment: None,
                filter: None,
                over: None,
                within_group: vec![]
            })),
            access_chain: vec![
                AccessExpr::Dot(Expr::Identifier(Ident::new("b"))),
                AccessExpr::Dot(Expr::Identifier(Ident::new("c"))),
            ]
        }
    );

    // Composite Access in Select and Where Clauses
    let stmt = verified_only_select("SELECT f(a).b FROM t WHERE f(a).b IS NOT NULL");
    let expr = Expr::CompoundFieldAccess {
        root: Box::new(Expr::Function(Function {
            name: ObjectName::from(vec![Ident::new("f")]),
            uses_odbc_syntax: false,
            parameters: FunctionArguments::None,
            args: FunctionArguments::List(FunctionArgumentList {
                duplicate_treatment: None,
                args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(
                    Expr::Identifier(Ident::new("a")),
                ))],
                clauses: vec![],
            }),
            null_treatment: None,
            filter: None,
            over: None,
            within_group: vec![],
        })),
        access_chain: vec![AccessExpr::Dot(Expr::Identifier(Ident::new("b")))],
    };

    assert_eq!(stmt.projection[0], SelectItem::UnnamedExpr(expr.clone()));
    assert_eq!(stmt.selection.unwrap(), Expr::IsNotNull(Box::new(expr)));

    // Compound access with quoted identifier.
    all_dialects_where(|d| d.is_delimited_identifier_start('"'))
        .verified_only_select("SELECT f(a).\"an id\"");

    // Composite Access in struct literal
    all_dialects_where(|d| d.supports_struct_literal()).verified_stmt(
        "SELECT * FROM t WHERE STRUCT(STRUCT(1 AS a, NULL AS b) AS c, NULL AS d).c.a IS NOT NULL",
    );
    let support_struct = all_dialects_where(|d| d.supports_struct_literal());
    let stmt = support_struct
        .verified_only_select("SELECT STRUCT(STRUCT(1 AS a, NULL AS b) AS c, NULL AS d).c.a");
    let expected = SelectItem::UnnamedExpr(Expr::CompoundFieldAccess {
        root: Box::new(Expr::Struct {
            values: vec![
                Expr::Named {
                    name: Ident::new("c"),
                    expr: Box::new(Expr::Struct {
                        values: vec![
                            Expr::Named {
                                name: Ident::new("a"),
                                expr: Box::new(Expr::Value(
                                    (Number("1".parse().unwrap(), false)).with_empty_span(),
                                )),
                            },
                            Expr::Named {
                                name: Ident::new("b"),
                                expr: Box::new(Expr::Value((Value::Null).with_empty_span())),
                            },
                        ],
                        fields: vec![],
                    }),
                },
                Expr::Named {
                    name: Ident::new("d"),
                    expr: Box::new(Expr::Value((Value::Null).with_empty_span())),
                },
            ],
            fields: vec![],
        }),
        access_chain: vec![
            AccessExpr::Dot(Expr::Identifier(Ident::new("c"))),
            AccessExpr::Dot(Expr::Identifier(Ident::new("a"))),
        ],
    });
    assert_eq!(stmt.projection[0], expected);
}

#[test]
fn parse_create_table_with_enum_types() {
    let sql = "CREATE TABLE t0 (foo ENUM8('a' = 1, 'b' = 2), bar ENUM16('a' = 1, 'b' = 2), baz ENUM('a', 'b'))";
    match all_dialects().verified_stmt(sql) {
        Statement::CreateTable(CreateTable { name, columns, .. }) => {
            assert_eq!(name.to_string(), "t0");
            assert_eq!(
                vec![
                    ColumnDef {
                        name: Ident::new("foo"),
                        data_type: DataType::Enum(
                            vec![
                                EnumMember::NamedValue(
                                    "a".to_string(),
                                    Expr::Value(
                                        (Number("1".parse().unwrap(), false)).with_empty_span()
                                    )
                                ),
                                EnumMember::NamedValue(
                                    "b".to_string(),
                                    Expr::Value(
                                        (Number("2".parse().unwrap(), false)).with_empty_span()
                                    )
                                )
                            ],
                            Some(8)
                        ),
                        options: vec![],
                    },
                    ColumnDef {
                        name: Ident::new("bar"),
                        data_type: DataType::Enum(
                            vec![
                                EnumMember::NamedValue(
                                    "a".to_string(),
                                    Expr::Value(
                                        (Number("1".parse().unwrap(), false)).with_empty_span()
                                    )
                                ),
                                EnumMember::NamedValue(
                                    "b".to_string(),
                                    Expr::Value(
                                        (Number("2".parse().unwrap(), false)).with_empty_span()
                                    )
                                )
                            ],
                            Some(16)
                        ),
                        options: vec![],
                    },
                    ColumnDef {
                        name: Ident::new("baz"),
                        data_type: DataType::Enum(
                            vec![
                                EnumMember::Name("a".to_string()),
                                EnumMember::Name("b".to_string())
                            ],
                            None
                        ),
                        options: vec![],
                    }
                ],
                columns
            );
        }
        _ => unreachable!(),
    }

    // invalid case missing value for enum pair
    assert_eq!(
        all_dialects()
            .parse_sql_statements("CREATE TABLE t0 (foo ENUM8('a' = 1, 'b' = ))")
            .unwrap_err(),
        ParserError::ParserError("Expected: a value, found: )".to_string())
    );

    // invalid case that name is not a string
    assert_eq!(
        all_dialects()
            .parse_sql_statements("CREATE TABLE t0 (foo ENUM8('a' = 1, 2))")
            .unwrap_err(),
        ParserError::ParserError("Expected: literal string, found: 2".to_string())
    );
}

#[test]
fn test_table_sample() {
    let dialects = all_dialects_where(|d| d.supports_table_sample_before_alias());
    dialects.verified_stmt("SELECT * FROM tbl TABLESAMPLE (50) AS t");
    dialects.verified_stmt("SELECT * FROM tbl TABLESAMPLE (50 ROWS) AS t");
    dialects.verified_stmt("SELECT * FROM tbl TABLESAMPLE (50 PERCENT) AS t");

    let dialects = all_dialects_where(|d| !d.supports_table_sample_before_alias());
    dialects.verified_stmt("SELECT * FROM tbl AS t TABLESAMPLE BERNOULLI (50)");
    dialects.verified_stmt("SELECT * FROM tbl AS t TABLESAMPLE SYSTEM (50)");
    dialects.verified_stmt("SELECT * FROM tbl AS t TABLESAMPLE SYSTEM (50) REPEATABLE (10)");
}

#[test]
fn overflow() {
    let expr = std::iter::repeat_n("1", 1000)
        .collect::<Vec<_>>()
        .join(" + ");
    let sql = format!("SELECT {expr}");

    let mut statements = Parser::parse_sql(&GenericDialect {}, sql.as_str()).unwrap();
    let statement = statements.pop().unwrap();
    assert_eq!(statement.to_string(), sql);
}
#[test]
fn parse_select_without_projection() {
    let dialects = all_dialects_where(|d| d.supports_empty_projections());
    dialects.verified_stmt("SELECT FROM users");
}

#[test]
fn parse_update_from_before_select() {
    verified_stmt("UPDATE t1 FROM (SELECT name, id FROM t1 GROUP BY id) AS t2 SET name = t2.name WHERE t1.id = t2.id");
    verified_stmt("UPDATE t1 FROM U, (SELECT id FROM V) AS W SET a = b WHERE 1 = 1");

    let query =
    "UPDATE t1 FROM (SELECT name, id FROM t1 GROUP BY id) AS t2 SET name = t2.name FROM (SELECT name from t2) AS t2";
    assert_eq!(
        ParserError::ParserError("Expected: end of statement, found: FROM".to_string()),
        parse_sql_statements(query).unwrap_err()
    );
}
#[test]
fn parse_overlaps() {
    verified_stmt("SELECT (DATE '2016-01-10', DATE '2016-02-01') OVERLAPS (DATE '2016-01-20', DATE '2016-02-10')");
}

#[test]
fn parse_column_definition_trailing_commas() {
    let dialects = all_dialects_where(|d| d.supports_column_definition_trailing_commas());

    dialects.one_statement_parses_to("CREATE TABLE T (x INT64,)", "CREATE TABLE T (x INT64)");
    dialects.one_statement_parses_to(
        "CREATE TABLE T (x INT64, y INT64, )",
        "CREATE TABLE T (x INT64, y INT64)",
    );
    dialects.one_statement_parses_to(
        "CREATE VIEW T (x, y, ) AS SELECT 1",
        "CREATE VIEW T (x, y) AS SELECT 1",
    );

    let unsupported_dialects = all_dialects_where(|d| {
        !d.supports_projection_trailing_commas() && !d.supports_trailing_commas()
    });
    assert_eq!(
        unsupported_dialects
            .parse_sql_statements("CREATE TABLE employees (name text, age int,)")
            .unwrap_err(),
        ParserError::ParserError(
            "Expected: column name or constraint definition, found: )".to_string()
        ),
    );
}

#[test]
fn test_trailing_commas_in_from() {
    let dialects = all_dialects_where(|d| d.supports_from_trailing_commas());
    dialects.verified_only_select_with_canonical("SELECT 1, 2 FROM t,", "SELECT 1, 2 FROM t");

    dialects
        .verified_only_select_with_canonical("SELECT 1, 2 FROM t1, t2,", "SELECT 1, 2 FROM t1, t2");

    let sql = "SELECT a, FROM b, LIMIT 1";
    let _ = dialects.parse_sql_statements(sql).unwrap();

    let sql = "INSERT INTO a SELECT b FROM c,";
    let _ = dialects.parse_sql_statements(sql).unwrap();

    let sql = "SELECT a FROM b, HAVING COUNT(*) > 1";
    let _ = dialects.parse_sql_statements(sql).unwrap();

    let sql = "SELECT a FROM b, WHERE c = 1";
    let _ = dialects.parse_sql_statements(sql).unwrap();

    // nested
    let sql = "SELECT 1, 2 FROM (SELECT * FROM t,),";
    let _ = dialects.parse_sql_statements(sql).unwrap();

    // multiple_subqueries
    dialects.verified_only_select_with_canonical(
        "SELECT 1, 2 FROM (SELECT * FROM t1), (SELECT * FROM t2),",
        "SELECT 1, 2 FROM (SELECT * FROM t1), (SELECT * FROM t2)",
    );
}

#[test]
#[cfg(feature = "visitor")]
fn test_visit_order() {
    let sql = "SELECT CASE a WHEN 1 THEN 2 WHEN 3 THEN 4 ELSE 5 END";
    let stmt = verified_stmt(sql);
    let mut visited = vec![];
    let _ = sqlparser::ast::visit_expressions(&stmt, |expr| {
        visited.push(expr.to_string());
        core::ops::ControlFlow::<()>::Continue(())
    });

    assert_eq!(
        visited,
        [
            "CASE a WHEN 1 THEN 2 WHEN 3 THEN 4 ELSE 5 END",
            "a",
            "1",
            "2",
            "3",
            "4",
            "5"
        ]
    );
}

#[test]
fn parse_case_statement() {
    let sql = "CASE 1 WHEN 2 THEN SELECT 1; SELECT 2; ELSE SELECT 3; END CASE";
    let Statement::Case(stmt) = verified_stmt(sql) else {
        unreachable!()
    };

    assert_eq!(Some(Expr::value(number("1"))), stmt.match_expr);
    assert_eq!(
        Some(Expr::value(number("2"))),
        stmt.when_blocks[0].condition
    );
    assert_eq!(2, stmt.when_blocks[0].statements().len());
    assert_eq!(1, stmt.else_block.unwrap().statements().len());

    verified_stmt(concat!(
        "CASE 1",
        " WHEN a THEN",
        " SELECT 1; SELECT 2; SELECT 3;",
        " WHEN b THEN",
        " SELECT 4; SELECT 5;",
        " ELSE",
        " SELECT 7; SELECT 8;",
        " END CASE"
    ));
    verified_stmt(concat!(
        "CASE 1",
        " WHEN a THEN",
        " SELECT 1; SELECT 2; SELECT 3;",
        " WHEN b THEN",
        " SELECT 4; SELECT 5;",
        " END CASE"
    ));
    verified_stmt(concat!(
        "CASE 1",
        " WHEN a THEN",
        " SELECT 1; SELECT 2; SELECT 3;",
        " END CASE"
    ));
    verified_stmt(concat!(
        "CASE 1",
        " WHEN a THEN",
        " SELECT 1; SELECT 2; SELECT 3;",
        " END"
    ));

    assert_eq!(
        ParserError::ParserError("Expected: THEN, found: END".to_string()),
        parse_sql_statements("CASE 1 WHEN a END").unwrap_err()
    );
    assert_eq!(
        ParserError::ParserError("Expected: WHEN, found: ELSE".to_string()),
        parse_sql_statements("CASE 1 ELSE SELECT 1; END").unwrap_err()
    );
}

#[test]
fn test_case_statement_span() {
    let sql = "CASE 1 WHEN 2 THEN SELECT 1; SELECT 2; ELSE SELECT 3; END CASE";
    let mut parser = Parser::new(&GenericDialect {}).try_with_sql(sql).unwrap();
    assert_eq!(
        parser.parse_statement().unwrap().span(),
        Span::new(Location::new(1, 1), Location::new(1, sql.len() as u64 + 1))
    );
}

#[test]
fn parse_if_statement() {
    let dialects = all_dialects_except(|d| d.is::<MsSqlDialect>());

    let sql = "IF 1 THEN SELECT 1; ELSEIF 2 THEN SELECT 2; ELSE SELECT 3; END IF";
    let Statement::If(IfStatement {
        if_block,
        elseif_blocks,
        else_block,
        ..
    }) = dialects.verified_stmt(sql)
    else {
        unreachable!()
    };
    assert_eq!(Some(Expr::value(number("1"))), if_block.condition);
    assert_eq!(Some(Expr::value(number("2"))), elseif_blocks[0].condition);
    assert_eq!(1, else_block.unwrap().statements().len());

    dialects.verified_stmt(concat!(
        "IF 1 THEN",
        " SELECT 1;",
        " SELECT 2;",
        " SELECT 3;",
        " ELSEIF 2 THEN",
        " SELECT 4;",
        " SELECT 5;",
        " ELSEIF 3 THEN",
        " SELECT 6;",
        " SELECT 7;",
        " ELSE",
        " SELECT 8;",
        " SELECT 9;",
        " END IF"
    ));
    dialects.verified_stmt(concat!(
        "IF 1 THEN",
        " SELECT 1;",
        " SELECT 2;",
        " ELSE",
        " SELECT 3;",
        " SELECT 4;",
        " END IF"
    ));
    dialects.verified_stmt(concat!(
        "IF 1 THEN",
        " SELECT 1;",
        " SELECT 2;",
        " SELECT 3;",
        " ELSEIF 2 THEN",
        " SELECT 3;",
        " SELECT 4;",
        " END IF"
    ));
    dialects.verified_stmt(concat!("IF 1 THEN", " SELECT 1;", " SELECT 2;", " END IF"));
    dialects.verified_stmt(concat!(
        "IF (1) THEN",
        " SELECT 1;",
        " SELECT 2;",
        " END IF"
    ));
    dialects.verified_stmt("IF 1 THEN END IF");
    dialects.verified_stmt("IF 1 THEN SELECT 1; ELSEIF 1 THEN END IF");

    assert_eq!(
        ParserError::ParserError("Expected: IF, found: EOF".to_string()),
        dialects
            .parse_sql_statements("IF 1 THEN SELECT 1; ELSEIF 1 THEN SELECT 2; END")
            .unwrap_err()
    );
}

#[test]
fn test_if_statement_span() {
    let sql = "IF 1=1 THEN SELECT 1; ELSEIF 1=2 THEN SELECT 2; ELSE SELECT 3; END IF";
    let mut parser = Parser::new(&GenericDialect {}).try_with_sql(sql).unwrap();
    assert_eq!(
        parser.parse_statement().unwrap().span(),
        Span::new(Location::new(1, 1), Location::new(1, sql.len() as u64 + 1))
    );
}

#[test]
fn test_if_statement_multiline_span() {
    let sql_line1 = "IF 1 = 1 THEN SELECT 1;";
    let sql_line2 = "ELSEIF 1 = 2 THEN SELECT 2;";
    let sql_line3 = "ELSE SELECT 3;";
    let sql_line4 = "END IF";
    let sql = [sql_line1, sql_line2, sql_line3, sql_line4].join("\n");
    let mut parser = Parser::new(&GenericDialect {}).try_with_sql(&sql).unwrap();
    assert_eq!(
        parser.parse_statement().unwrap().span(),
        Span::new(
            Location::new(1, 1),
            Location::new(4, sql_line4.len() as u64 + 1)
        )
    );
}

#[test]
fn test_conditional_statement_span() {
    let sql = "IF 1=1 THEN SELECT 1; ELSEIF 1=2 THEN SELECT 2; ELSE SELECT 3; END IF";
    let mut parser = Parser::new(&GenericDialect {}).try_with_sql(sql).unwrap();
    match parser.parse_statement().unwrap() {
        Statement::If(IfStatement {
            if_block,
            elseif_blocks,
            else_block,
            ..
        }) => {
            assert_eq!(
                Span::new(Location::new(1, 1), Location::new(1, 21)),
                if_block.span()
            );
            assert_eq!(
                Span::new(Location::new(1, 23), Location::new(1, 47)),
                elseif_blocks[0].span()
            );
            assert_eq!(
                Span::new(Location::new(1, 49), Location::new(1, 62)),
                else_block.unwrap().span()
            );
        }
        stmt => panic!("Unexpected statement: {stmt:?}"),
    }
}

#[test]
fn parse_raise_statement() {
    let sql = "RAISE USING MESSAGE = 42";
    let Statement::Raise(stmt) = verified_stmt(sql) else {
        unreachable!()
    };
    assert_eq!(
        Some(RaiseStatementValue::UsingMessage(Expr::value(number("42")))),
        stmt.value
    );

    verified_stmt("RAISE USING MESSAGE = 'error'");
    verified_stmt("RAISE myerror");
    verified_stmt("RAISE 42");
    verified_stmt("RAISE using");
    verified_stmt("RAISE");

    assert_eq!(
        ParserError::ParserError("Expected: =, found: error".to_string()),
        parse_sql_statements("RAISE USING MESSAGE error").unwrap_err()
    );
}

#[test]
fn test_lambdas() {
    let dialects = all_dialects_where(|d| d.supports_lambda_functions());

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
                        Expr::Value(
                            (Value::SingleQuotedString("Hello".to_owned())).with_empty_span()
                        ),
                        Expr::Value(
                            (Value::SingleQuotedString("World".to_owned())).with_empty_span()
                        )
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
                                result: Expr::value(number("0")),
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
                        else_result: Some(Box::new(Expr::value(number("1")))),
                    })
                })
            ]
        )),
        dialects.verified_only_select(sql).projection[0]
    );

    dialects.verified_expr(
        "map_zip_with(map(1, 'a', 2, 'b'), map(1, 'x', 2, 'y'), (k, v1, v2) -> concat(v1, v2))",
    );
    dialects.verified_expr("transform(array(1, 2, 3), x -> x + 1)");
}

#[test]
fn test_select_from_first() {
    let dialects = all_dialects_where(|d| d.supports_from_first_select());
    let q1 = "FROM capitals";
    let q2 = "FROM capitals SELECT *";

    for (q, flavor, projection) in [
        (q1, SelectFlavor::FromFirstNoSelect, vec![]),
        (
            q2,
            SelectFlavor::FromFirst,
            vec![SelectItem::Wildcard(WildcardAdditionalOptions::default())],
        ),
    ] {
        let ast = dialects.verified_query(q);
        let expected = Query {
            with: None,
            body: Box::new(SetExpr::Select(Box::new(Select {
                select_token: AttachedToken::empty(),
                distinct: None,
                top: None,
                projection,
                exclude: None,
                top_before_distinct: false,
                into: None,
                from: vec![TableWithJoins {
                    relation: table_from_name(ObjectName::from(vec![Ident {
                        value: "capitals".to_string(),
                        quote_style: None,
                        span: Span::empty(),
                    }])),
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
                window_before_qualify: false,
                qualify: None,
                value_table_mode: None,
                connect_by: None,
                flavor,
            }))),
            order_by: None,
            limit_clause: None,
            fetch: None,
            locks: vec![],
            for_clause: None,
            settings: None,
            format_clause: None,
            pipe_operators: vec![],
        };
        assert_eq!(expected, ast);
        assert_eq!(ast.to_string(), q);
    }
}

#[test]
fn test_geometric_unary_operators() {
    // Number of points in path or polygon
    let sql = "# path '((1,0),(0,1),(-1,0))'";
    assert!(matches!(
        all_dialects_where(|d| d.supports_geometric_types()).verified_expr(sql),
        Expr::UnaryOp {
            op: UnaryOperator::Hash,
            ..
        }
    ));

    // Length or circumference
    let sql = "@-@ path '((0,0),(1,0))'";
    assert!(matches!(
        all_dialects_where(|d| d.supports_geometric_types()).verified_expr(sql),
        Expr::UnaryOp {
            op: UnaryOperator::AtDashAt,
            ..
        }
    ));

    // Center
    let sql = "@@ circle '((0,0),10)'";
    assert!(matches!(
        all_dialects_where(|d| d.supports_geometric_types()).verified_expr(sql),
        Expr::UnaryOp {
            op: UnaryOperator::DoubleAt,
            ..
        }
    ));
    // Is horizontal?
    let sql = "?- lseg '((-1,0),(1,0))'";
    assert!(matches!(
        all_dialects_where(|d| d.supports_geometric_types()).verified_expr(sql),
        Expr::UnaryOp {
            op: UnaryOperator::QuestionDash,
            ..
        }
    ));

    // Is vertical?
    let sql = "?| lseg '((-1,0),(1,0))'";
    assert!(matches!(
        all_dialects_where(|d| d.supports_geometric_types()).verified_expr(sql),
        Expr::UnaryOp {
            op: UnaryOperator::QuestionPipe,
            ..
        }
    ));
}

#[test]
fn test_geometry_type() {
    let sql = "point '1,2'";
    assert_eq!(
        all_dialects_where(|d| d.supports_geometric_types()).verified_expr(sql),
        Expr::TypedString(TypedString {
            data_type: DataType::GeometricType(GeometricTypeKind::Point),
            value: ValueWithSpan {
                value: Value::SingleQuotedString("1,2".to_string()),
                span: Span::empty(),
            },
            uses_odbc_syntax: false
        })
    );

    let sql = "line '1,2,3,4'";
    assert_eq!(
        all_dialects_where(|d| d.supports_geometric_types()).verified_expr(sql),
        Expr::TypedString(TypedString {
            data_type: DataType::GeometricType(GeometricTypeKind::Line),
            value: ValueWithSpan {
                value: Value::SingleQuotedString("1,2,3,4".to_string()),
                span: Span::empty(),
            },
            uses_odbc_syntax: false
        })
    );

    let sql = "path '1,2,3,4'";
    assert_eq!(
        all_dialects_where(|d| d.supports_geometric_types()).verified_expr(sql),
        Expr::TypedString(TypedString {
            data_type: DataType::GeometricType(GeometricTypeKind::GeometricPath),
            value: ValueWithSpan {
                value: Value::SingleQuotedString("1,2,3,4".to_string()),
                span: Span::empty(),
            },
            uses_odbc_syntax: false
        })
    );
    let sql = "box '1,2,3,4'";
    assert_eq!(
        all_dialects_where(|d| d.supports_geometric_types()).verified_expr(sql),
        Expr::TypedString(TypedString {
            data_type: DataType::GeometricType(GeometricTypeKind::GeometricBox),
            value: ValueWithSpan {
                value: Value::SingleQuotedString("1,2,3,4".to_string()),
                span: Span::empty(),
            },
            uses_odbc_syntax: false
        })
    );

    let sql = "circle '1,2,3'";
    assert_eq!(
        all_dialects_where(|d| d.supports_geometric_types()).verified_expr(sql),
        Expr::TypedString(TypedString {
            data_type: DataType::GeometricType(GeometricTypeKind::Circle),
            value: ValueWithSpan {
                value: Value::SingleQuotedString("1,2,3".to_string()),
                span: Span::empty(),
            },
            uses_odbc_syntax: false
        })
    );

    let sql = "polygon '1,2,3,4'";
    assert_eq!(
        all_dialects_where(|d| d.supports_geometric_types()).verified_expr(sql),
        Expr::TypedString(TypedString {
            data_type: DataType::GeometricType(GeometricTypeKind::Polygon),
            value: ValueWithSpan {
                value: Value::SingleQuotedString("1,2,3,4".to_string()),
                span: Span::empty(),
            },
            uses_odbc_syntax: false
        })
    );
    let sql = "lseg '1,2,3,4'";
    assert_eq!(
        all_dialects_where(|d| d.supports_geometric_types()).verified_expr(sql),
        Expr::TypedString(TypedString {
            data_type: DataType::GeometricType(GeometricTypeKind::LineSegment),
            value: ValueWithSpan {
                value: Value::SingleQuotedString("1,2,3,4".to_string()),
                span: Span::empty(),
            },
            uses_odbc_syntax: false
        })
    );
}
#[test]
fn test_geometric_binary_operators() {
    // Translation plus
    let sql = "box '((0,0),(1,1))' + point '(2.0,0)'";
    assert!(matches!(
        all_dialects_where(|d| d.supports_geometric_types()).verified_expr(sql),
        Expr::BinaryOp {
            op: BinaryOperator::Plus,
            ..
        }
    ));
    // Translation minus
    let sql = "box '((0,0),(1,1))' - point '(2.0,0)'";
    assert!(matches!(
        all_dialects_where(|d| d.supports_geometric_types()).verified_expr(sql),
        Expr::BinaryOp {
            op: BinaryOperator::Minus,
            ..
        }
    ));

    // Scaling multiply
    let sql = "box '((0,0),(1,1))' * point '(2.0,0)'";
    assert!(matches!(
        all_dialects_where(|d| d.supports_geometric_types()).verified_expr(sql),
        Expr::BinaryOp {
            op: BinaryOperator::Multiply,
            ..
        }
    ));

    // Scaling divide
    let sql = "box '((0,0),(1,1))' / point '(2.0,0)'";
    assert!(matches!(
        all_dialects_where(|d| d.supports_geometric_types()).verified_expr(sql),
        Expr::BinaryOp {
            op: BinaryOperator::Divide,
            ..
        }
    ));

    // Intersection
    let sql = "'((1,-1),(-1,1))' # '((1,1),(-1,-1))'";
    assert!(matches!(
        all_dialects_where(|d| d.supports_geometric_types()).verified_expr(sql),
        Expr::BinaryOp {
            op: BinaryOperator::PGBitwiseXor,
            ..
        }
    ));

    //Point of closest proximity
    let sql = "point '(0,0)' ## lseg '((2,0),(0,2))'";
    assert!(matches!(
        all_dialects_where(|d| d.supports_geometric_types()).verified_expr(sql),
        Expr::BinaryOp {
            op: BinaryOperator::DoubleHash,
            ..
        }
    ));

    // Point of closest proximity
    let sql = "box '((0,0),(1,1))' && box '((0,0),(2,2))'";
    assert!(matches!(
        all_dialects_where(|d| d.supports_geometric_types()).verified_expr(sql),
        Expr::BinaryOp {
            op: BinaryOperator::PGOverlap,
            ..
        }
    ));

    // Overlaps to left?
    let sql = "box '((0,0),(1,1))' &< box '((0,0),(2,2))'";
    assert!(matches!(
        all_dialects_where(|d| d.supports_geometric_types()).verified_expr(sql),
        Expr::BinaryOp {
            op: BinaryOperator::AndLt,
            ..
        }
    ));

    // Overlaps to right?
    let sql = "box '((0,0),(3,3))' &> box '((0,0),(2,2))'";
    assert!(matches!(
        all_dialects_where(|d| d.supports_geometric_types()).verified_expr(sql),
        Expr::BinaryOp {
            op: BinaryOperator::AndGt,
            ..
        }
    ));

    // Distance between
    let sql = "circle '((0,0),1)' <-> circle '((5,0),1)'";
    assert!(matches!(
        all_dialects_where(|d| d.supports_geometric_types()).verified_expr(sql),
        Expr::BinaryOp {
            op: BinaryOperator::LtDashGt,
            ..
        }
    ));

    // Is left of?
    let sql = "circle '((0,0),1)' << circle '((5,0),1)'";
    assert!(matches!(
        all_dialects_where(|d| d.supports_geometric_types()).verified_expr(sql),
        Expr::BinaryOp {
            op: BinaryOperator::PGBitwiseShiftLeft,
            ..
        }
    ));

    // Is right of?
    let sql = "circle '((5,0),1)' >> circle '((0,0),1)'";
    assert!(matches!(
        all_dialects_where(|d| d.supports_geometric_types()).verified_expr(sql),
        Expr::BinaryOp {
            op: BinaryOperator::PGBitwiseShiftRight,
            ..
        }
    ));

    // Is below?
    let sql = "circle '((0,0),1)' <^ circle '((0,5),1)'";
    assert!(matches!(
        all_dialects_where(|d| d.supports_geometric_types()).verified_expr(sql),
        Expr::BinaryOp {
            op: BinaryOperator::LtCaret,
            ..
        }
    ));

    // 	Intersects or overlaps
    let sql = "lseg '((-1,0),(1,0))' ?# box '((-2,-2),(2,2))'";
    assert!(matches!(
        all_dialects_where(|d| d.supports_geometric_types()).verified_expr(sql),
        Expr::BinaryOp {
            op: BinaryOperator::QuestionHash,
            ..
        }
    ));

    // Is horizontal?
    let sql = "point '(1,0)' ?- point '(0,0)'";
    assert!(matches!(
        all_dialects_where(|d| d.supports_geometric_types()).verified_expr(sql),
        Expr::BinaryOp {
            op: BinaryOperator::QuestionDash,
            ..
        }
    ));

    // Is perpendicular?
    let sql = "lseg '((0,0),(0,1))' ?-| lseg '((0,0),(1,0))'";
    assert!(matches!(
        all_dialects_where(|d| d.supports_geometric_types()).verified_expr(sql),
        Expr::BinaryOp {
            op: BinaryOperator::QuestionDashPipe,
            ..
        }
    ));

    // Is vertical?
    let sql = "point '(0,1)' ?| point '(0,0)'";
    assert!(matches!(
        all_dialects_where(|d| d.supports_geometric_types()).verified_expr(sql),
        Expr::BinaryOp {
            op: BinaryOperator::QuestionPipe,
            ..
        }
    ));

    // Are parallel?
    let sql = "lseg '((-1,0),(1,0))' ?|| lseg '((-1,2),(1,2))'";
    assert!(matches!(
        all_dialects_where(|d| d.supports_geometric_types()).verified_expr(sql),
        Expr::BinaryOp {
            op: BinaryOperator::QuestionDoublePipe,
            ..
        }
    ));

    // Contained or on?
    let sql = "point '(1,1)' @ circle '((0,0),2)'";
    assert!(matches!(
        all_dialects_where(|d| d.supports_geometric_types()).verified_expr(sql),
        Expr::BinaryOp {
            op: BinaryOperator::At,
            ..
        }
    ));

    //
    // Same as?
    let sql = "polygon '((0,0),(1,1))' ~= polygon '((1,1),(0,0))'";
    assert!(matches!(
        all_dialects_where(|d| d.supports_geometric_types()).verified_expr(sql),
        Expr::BinaryOp {
            op: BinaryOperator::TildeEq,
            ..
        }
    ));

    // Is strictly below?
    let sql = "box '((0,0),(3,3))' <<| box '((3,4),(5,5))'";
    assert!(matches!(
        all_dialects_where(|d| d.supports_geometric_types()).verified_expr(sql),
        Expr::BinaryOp {
            op: BinaryOperator::LtLtPipe,
            ..
        }
    ));

    // Is strictly above?
    let sql = "box '((3,4),(5,5))' |>> box '((0,0),(3,3))'";
    assert!(matches!(
        all_dialects_where(|d| d.supports_geometric_types()).verified_expr(sql),
        Expr::BinaryOp {
            op: BinaryOperator::PipeGtGt,
            ..
        }
    ));

    // Does not extend above?
    let sql = "box '((0,0),(1,1))' &<| box '((0,0),(2,2))'";
    assert!(matches!(
        all_dialects_where(|d| d.supports_geometric_types()).verified_expr(sql),
        Expr::BinaryOp {
            op: BinaryOperator::AndLtPipe,
            ..
        }
    ));

    // Does not extend below?
    let sql = "box '((0,0),(3,3))' |&> box '((0,0),(2,2))'";
    assert!(matches!(
        all_dialects_where(|d| d.supports_geometric_types()).verified_expr(sql),
        Expr::BinaryOp {
            op: BinaryOperator::PipeAndGt,
            ..
        }
    ));
}

#[test]
fn parse_array_type_def_with_brackets() {
    let dialects = all_dialects_where(|d| d.supports_array_typedef_with_brackets());
    dialects.verified_stmt("SELECT x::INT[]");
    dialects.verified_stmt("SELECT STRING_TO_ARRAY('1,2,3', ',')::INT[3]");
}

#[test]
fn parse_set_names() {
    let dialects = all_dialects_where(|d| d.supports_set_names());
    dialects.verified_stmt("SET NAMES 'UTF8'");
    dialects.verified_stmt("SET NAMES 'utf8'");
    dialects.verified_stmt("SET NAMES UTF8 COLLATE bogus");
}

#[test]
fn parse_pipeline_operator() {
    let dialects = all_dialects_where(|d| d.supports_pipe_operator());

    // select pipe operator
    dialects.verified_stmt("SELECT * FROM users |> SELECT id");
    dialects.verified_stmt("SELECT * FROM users |> SELECT id, name");
    dialects.verified_query_with_canonical(
        "SELECT * FROM users |> SELECT id user_id",
        "SELECT * FROM users |> SELECT id AS user_id",
    );
    dialects.verified_stmt("SELECT * FROM users |> SELECT id AS user_id");

    // extend pipe operator
    dialects.verified_stmt("SELECT * FROM users |> EXTEND id + 1 AS new_id");
    dialects.verified_stmt("SELECT * FROM users |> EXTEND id AS new_id, name AS new_name");
    dialects.verified_query_with_canonical(
        "SELECT * FROM users |> EXTEND id user_id",
        "SELECT * FROM users |> EXTEND id AS user_id",
    );

    // set pipe operator
    dialects.verified_stmt("SELECT * FROM users |> SET id = id + 1");
    dialects.verified_stmt("SELECT * FROM users |> SET id = id + 1, name = name + ' Doe'");

    // drop pipe operator
    dialects.verified_stmt("SELECT * FROM users |> DROP id");
    dialects.verified_stmt("SELECT * FROM users |> DROP id, name");

    // as pipe operator
    dialects.verified_stmt("SELECT * FROM users |> AS new_users");

    // limit pipe operator
    dialects.verified_stmt("SELECT * FROM users |> LIMIT 10");
    dialects.verified_stmt("SELECT * FROM users |> LIMIT 10 OFFSET 5");
    dialects.verified_stmt("SELECT * FROM users |> LIMIT 10 |> LIMIT 5");
    dialects.verified_stmt("SELECT * FROM users |> LIMIT 10 |> WHERE true");

    // where pipe operator
    dialects.verified_stmt("SELECT * FROM users |> WHERE id = 1");
    dialects.verified_stmt("SELECT * FROM users |> WHERE id = 1 AND name = 'John'");
    dialects.verified_stmt("SELECT * FROM users |> WHERE id = 1 OR name = 'John'");

    // aggregate pipe operator full table
    dialects.verified_stmt("SELECT * FROM users |> AGGREGATE COUNT(*)");
    dialects.verified_query_with_canonical(
        "SELECT * FROM users |> AGGREGATE COUNT(*) total_users",
        "SELECT * FROM users |> AGGREGATE COUNT(*) AS total_users",
    );
    dialects.verified_stmt("SELECT * FROM users |> AGGREGATE COUNT(*) AS total_users");
    dialects.verified_stmt("SELECT * FROM users |> AGGREGATE COUNT(*), MIN(id)");

    // aggregate pipe opeprator with grouping
    dialects.verified_stmt(
        "SELECT * FROM users |> AGGREGATE SUM(o_totalprice) AS price, COUNT(*) AS cnt GROUP BY EXTRACT(YEAR FROM o_orderdate) AS year",
    );
    dialects.verified_stmt(
        "SELECT * FROM users |> AGGREGATE GROUP BY EXTRACT(YEAR FROM o_orderdate) AS year",
    );
    dialects
        .verified_stmt("SELECT * FROM users |> AGGREGATE GROUP BY EXTRACT(YEAR FROM o_orderdate)");
    dialects.verified_stmt("SELECT * FROM users |> AGGREGATE GROUP BY a, b");
    dialects.verified_stmt("SELECT * FROM users |> AGGREGATE SUM(c) GROUP BY a, b");
    dialects.verified_stmt("SELECT * FROM users |> AGGREGATE SUM(c) ASC");

    // order by pipe operator
    dialects.verified_stmt("SELECT * FROM users |> ORDER BY id ASC");
    dialects.verified_stmt("SELECT * FROM users |> ORDER BY id DESC");
    dialects.verified_stmt("SELECT * FROM users |> ORDER BY id DESC, name ASC");

    // tablesample pipe operator
    dialects.verified_stmt("SELECT * FROM tbl |> TABLESAMPLE BERNOULLI (50)");
    dialects.verified_stmt("SELECT * FROM tbl |> TABLESAMPLE SYSTEM (50 PERCENT)");
    dialects.verified_stmt("SELECT * FROM tbl |> TABLESAMPLE SYSTEM (50) REPEATABLE (10)");

    // rename pipe operator
    dialects.verified_stmt("SELECT * FROM users |> RENAME old_name AS new_name");
    dialects.verified_stmt("SELECT * FROM users |> RENAME id AS user_id, name AS user_name");
    dialects.verified_query_with_canonical(
        "SELECT * FROM users |> RENAME id user_id",
        "SELECT * FROM users |> RENAME id AS user_id",
    );

    // union pipe operator
    dialects.verified_stmt("SELECT * FROM users |> UNION ALL (SELECT * FROM admins)");
    dialects.verified_stmt("SELECT * FROM users |> UNION DISTINCT (SELECT * FROM admins)");
    dialects.verified_stmt("SELECT * FROM users |> UNION (SELECT * FROM admins)");

    // union pipe operator with multiple queries
    dialects.verified_stmt(
        "SELECT * FROM users |> UNION ALL (SELECT * FROM admins), (SELECT * FROM guests)",
    );
    dialects.verified_stmt("SELECT * FROM users |> UNION DISTINCT (SELECT * FROM admins), (SELECT * FROM guests), (SELECT * FROM employees)");
    dialects.verified_stmt(
        "SELECT * FROM users |> UNION (SELECT * FROM admins), (SELECT * FROM guests)",
    );

    // union pipe operator with BY NAME modifier
    dialects.verified_stmt("SELECT * FROM users |> UNION BY NAME (SELECT * FROM admins)");
    dialects.verified_stmt("SELECT * FROM users |> UNION ALL BY NAME (SELECT * FROM admins)");
    dialects.verified_stmt("SELECT * FROM users |> UNION DISTINCT BY NAME (SELECT * FROM admins)");

    // union pipe operator with BY NAME and multiple queries
    dialects.verified_stmt(
        "SELECT * FROM users |> UNION BY NAME (SELECT * FROM admins), (SELECT * FROM guests)",
    );

    // intersect pipe operator (BigQuery requires DISTINCT modifier for INTERSECT)
    dialects.verified_stmt("SELECT * FROM users |> INTERSECT DISTINCT (SELECT * FROM admins)");

    // intersect pipe operator with BY NAME modifier
    dialects
        .verified_stmt("SELECT * FROM users |> INTERSECT DISTINCT BY NAME (SELECT * FROM admins)");

    // intersect pipe operator with multiple queries
    dialects.verified_stmt(
        "SELECT * FROM users |> INTERSECT DISTINCT (SELECT * FROM admins), (SELECT * FROM guests)",
    );

    // intersect pipe operator with BY NAME and multiple queries
    dialects.verified_stmt("SELECT * FROM users |> INTERSECT DISTINCT BY NAME (SELECT * FROM admins), (SELECT * FROM guests)");

    // except pipe operator (BigQuery requires DISTINCT modifier for EXCEPT)
    dialects.verified_stmt("SELECT * FROM users |> EXCEPT DISTINCT (SELECT * FROM admins)");

    // except pipe operator with BY NAME modifier
    dialects.verified_stmt("SELECT * FROM users |> EXCEPT DISTINCT BY NAME (SELECT * FROM admins)");

    // except pipe operator with multiple queries
    dialects.verified_stmt(
        "SELECT * FROM users |> EXCEPT DISTINCT (SELECT * FROM admins), (SELECT * FROM guests)",
    );

    // except pipe operator with BY NAME and multiple queries
    dialects.verified_stmt("SELECT * FROM users |> EXCEPT DISTINCT BY NAME (SELECT * FROM admins), (SELECT * FROM guests)");

    // call pipe operator
    dialects.verified_stmt("SELECT * FROM users |> CALL my_function()");
    dialects.verified_stmt("SELECT * FROM users |> CALL process_data(5, 'test')");
    dialects.verified_stmt(
        "SELECT * FROM users |> CALL namespace.function_name(col1, col2, 'literal')",
    );

    // call pipe operator with complex arguments
    dialects.verified_stmt("SELECT * FROM users |> CALL transform_data(col1 + col2)");
    dialects.verified_stmt("SELECT * FROM users |> CALL analyze_data('param1', 100, true)");

    // call pipe operator with aliases
    dialects.verified_stmt("SELECT * FROM input_table |> CALL tvf1(arg1) AS al");
    dialects.verified_stmt("SELECT * FROM users |> CALL process_data(5) AS result_table");
    dialects.verified_stmt("SELECT * FROM users |> CALL namespace.func() AS my_alias");

    // multiple call pipe operators in sequence
    dialects.verified_stmt("SELECT * FROM input_table |> CALL tvf1(arg1) |> CALL tvf2(arg2, arg3)");
    dialects.verified_stmt(
        "SELECT * FROM data |> CALL transform(col1) |> CALL validate() |> CALL process(param)",
    );

    // multiple call pipe operators with aliases
    dialects.verified_stmt(
        "SELECT * FROM input_table |> CALL tvf1(arg1) AS step1 |> CALL tvf2(arg2) AS step2",
    );
    dialects.verified_stmt(
        "SELECT * FROM data |> CALL preprocess() AS clean_data |> CALL analyze(mode) AS results",
    );

    // call pipe operators mixed with other pipe operators
    dialects.verified_stmt(
        "SELECT * FROM users |> CALL transform() |> WHERE status = 'active' |> CALL process(param)",
    );
    dialects.verified_stmt(
        "SELECT * FROM data |> CALL preprocess() AS clean |> SELECT col1, col2 |> CALL validate()",
    );

    // pivot pipe operator
    dialects.verified_stmt(
        "SELECT * FROM monthly_sales |> PIVOT(SUM(amount) FOR quarter IN ('Q1', 'Q2', 'Q3', 'Q4'))",
    );
    dialects.verified_stmt("SELECT * FROM sales_data |> PIVOT(AVG(revenue) FOR region IN ('North', 'South', 'East', 'West'))");

    // pivot pipe operator with multiple aggregate functions
    dialects.verified_stmt("SELECT * FROM data |> PIVOT(SUM(sales) AS total_sales, COUNT(*) AS num_transactions FOR month IN ('Jan', 'Feb', 'Mar'))");

    // pivot pipe operator with compound column names
    dialects.verified_stmt("SELECT * FROM sales |> PIVOT(SUM(amount) FOR product.category IN ('Electronics', 'Clothing'))");

    // pivot pipe operator mixed with other pipe operators
    dialects.verified_stmt("SELECT * FROM sales_data |> WHERE year = 2023 |> PIVOT(SUM(revenue) FOR quarter IN ('Q1', 'Q2', 'Q3', 'Q4'))");

    // pivot pipe operator with aliases
    dialects.verified_stmt("SELECT * FROM monthly_sales |> PIVOT(SUM(sales) FOR quarter IN ('Q1', 'Q2')) AS quarterly_sales");
    dialects.verified_stmt("SELECT * FROM data |> PIVOT(AVG(price) FOR category IN ('A', 'B', 'C')) AS avg_by_category");
    dialects.verified_stmt("SELECT * FROM sales |> PIVOT(COUNT(*) AS transactions, SUM(amount) AS total FOR region IN ('North', 'South')) AS regional_summary");

    // pivot pipe operator with implicit aliases (without AS keyword)
    dialects.verified_query_with_canonical(
        "SELECT * FROM monthly_sales |> PIVOT(SUM(sales) FOR quarter IN ('Q1', 'Q2')) quarterly_sales",
        "SELECT * FROM monthly_sales |> PIVOT(SUM(sales) FOR quarter IN ('Q1', 'Q2')) AS quarterly_sales",
    );
    dialects.verified_query_with_canonical(
        "SELECT * FROM data |> PIVOT(AVG(price) FOR category IN ('A', 'B', 'C')) avg_by_category",
        "SELECT * FROM data |> PIVOT(AVG(price) FOR category IN ('A', 'B', 'C')) AS avg_by_category",
    );

    // unpivot pipe operator basic usage
    dialects
        .verified_stmt("SELECT * FROM sales |> UNPIVOT(revenue FOR quarter IN (Q1, Q2, Q3, Q4))");
    dialects.verified_stmt("SELECT * FROM data |> UNPIVOT(value FOR category IN (A, B, C))");
    dialects.verified_stmt(
        "SELECT * FROM metrics |> UNPIVOT(measurement FOR metric_type IN (cpu, memory, disk))",
    );

    // unpivot pipe operator with multiple columns
    dialects.verified_stmt("SELECT * FROM quarterly_sales |> UNPIVOT(amount FOR period IN (jan, feb, mar, apr, may, jun))");
    dialects.verified_stmt(
        "SELECT * FROM report |> UNPIVOT(score FOR subject IN (math, science, english, history))",
    );

    // unpivot pipe operator mixed with other pipe operators
    dialects.verified_stmt("SELECT * FROM sales_data |> WHERE year = 2023 |> UNPIVOT(revenue FOR quarter IN (Q1, Q2, Q3, Q4))");

    // unpivot pipe operator with aliases
    dialects.verified_stmt("SELECT * FROM quarterly_sales |> UNPIVOT(amount FOR period IN (Q1, Q2)) AS unpivoted_sales");
    dialects.verified_stmt(
        "SELECT * FROM data |> UNPIVOT(value FOR category IN (A, B, C)) AS transformed_data",
    );
    dialects.verified_stmt("SELECT * FROM metrics |> UNPIVOT(measurement FOR metric_type IN (cpu, memory)) AS metric_measurements");

    // unpivot pipe operator with implicit aliases (without AS keyword)
    dialects.verified_query_with_canonical(
        "SELECT * FROM quarterly_sales |> UNPIVOT(amount FOR period IN (Q1, Q2)) unpivoted_sales",
        "SELECT * FROM quarterly_sales |> UNPIVOT(amount FOR period IN (Q1, Q2)) AS unpivoted_sales",
    );
    dialects.verified_query_with_canonical(
        "SELECT * FROM data |> UNPIVOT(value FOR category IN (A, B, C)) transformed_data",
        "SELECT * FROM data |> UNPIVOT(value FOR category IN (A, B, C)) AS transformed_data",
    );

    // many pipes
    dialects.verified_stmt(
        "SELECT * FROM CustomerOrders |> AGGREGATE SUM(cost) AS total_cost GROUP BY customer_id, state, item_type |> EXTEND COUNT(*) OVER (PARTITION BY customer_id) AS num_orders |> WHERE num_orders > 1 |> AGGREGATE AVG(total_cost) AS average GROUP BY state DESC, item_type ASC",
    );

    // join pipe operator - INNER JOIN
    dialects.verified_stmt("SELECT * FROM users |> JOIN orders ON users.id = orders.user_id");
    dialects.verified_stmt("SELECT * FROM users |> INNER JOIN orders ON users.id = orders.user_id");

    // join pipe operator - LEFT JOIN
    dialects.verified_stmt("SELECT * FROM users |> LEFT JOIN orders ON users.id = orders.user_id");
    dialects.verified_stmt(
        "SELECT * FROM users |> LEFT OUTER JOIN orders ON users.id = orders.user_id",
    );

    // join pipe operator - RIGHT JOIN
    dialects.verified_stmt("SELECT * FROM users |> RIGHT JOIN orders ON users.id = orders.user_id");
    dialects.verified_stmt(
        "SELECT * FROM users |> RIGHT OUTER JOIN orders ON users.id = orders.user_id",
    );

    // join pipe operator - FULL JOIN
    dialects.verified_stmt("SELECT * FROM users |> FULL JOIN orders ON users.id = orders.user_id");
    dialects.verified_query_with_canonical(
        "SELECT * FROM users |> FULL OUTER JOIN orders ON users.id = orders.user_id",
        "SELECT * FROM users |> FULL JOIN orders ON users.id = orders.user_id",
    );

    // join pipe operator - CROSS JOIN
    dialects.verified_stmt("SELECT * FROM users |> CROSS JOIN orders");

    // join pipe operator with USING
    dialects.verified_query_with_canonical(
        "SELECT * FROM users |> JOIN orders USING (user_id)",
        "SELECT * FROM users |> JOIN orders USING(user_id)",
    );
    dialects.verified_query_with_canonical(
        "SELECT * FROM users |> LEFT JOIN orders USING (user_id, order_date)",
        "SELECT * FROM users |> LEFT JOIN orders USING(user_id, order_date)",
    );

    // join pipe operator with alias
    dialects.verified_query_with_canonical(
        "SELECT * FROM users |> JOIN orders o ON users.id = o.user_id",
        "SELECT * FROM users |> JOIN orders AS o ON users.id = o.user_id",
    );
    dialects.verified_stmt("SELECT * FROM users |> LEFT JOIN orders AS o ON users.id = o.user_id");

    // join pipe operator with complex ON condition
    dialects.verified_stmt("SELECT * FROM users |> JOIN orders ON users.id = orders.user_id AND orders.status = 'active'");
    dialects.verified_stmt("SELECT * FROM users |> LEFT JOIN orders ON users.id = orders.user_id AND orders.amount > 100");

    // multiple join pipe operators
    dialects.verified_stmt("SELECT * FROM users |> JOIN orders ON users.id = orders.user_id |> JOIN products ON orders.product_id = products.id");
    dialects.verified_stmt("SELECT * FROM users |> LEFT JOIN orders ON users.id = orders.user_id |> RIGHT JOIN products ON orders.product_id = products.id");

    // join pipe operator with other pipe operators
    dialects.verified_stmt("SELECT * FROM users |> JOIN orders ON users.id = orders.user_id |> WHERE orders.amount > 100");
    dialects.verified_stmt("SELECT * FROM users |> WHERE users.active = true |> LEFT JOIN orders ON users.id = orders.user_id");
    dialects.verified_stmt("SELECT * FROM users |> JOIN orders ON users.id = orders.user_id |> SELECT users.name, orders.amount");
}

#[test]
fn parse_pipeline_operator_negative_tests() {
    let dialects = all_dialects_where(|d| d.supports_pipe_operator());

    // Test that plain EXCEPT without DISTINCT fails
    assert_eq!(
        ParserError::ParserError("EXCEPT pipe operator requires DISTINCT modifier".to_string()),
        dialects
            .parse_sql_statements("SELECT * FROM users |> EXCEPT (SELECT * FROM admins)")
            .unwrap_err()
    );

    // Test that EXCEPT ALL fails
    assert_eq!(
        ParserError::ParserError("EXCEPT pipe operator requires DISTINCT modifier".to_string()),
        dialects
            .parse_sql_statements("SELECT * FROM users |> EXCEPT ALL (SELECT * FROM admins)")
            .unwrap_err()
    );

    // Test that EXCEPT BY NAME without DISTINCT fails
    assert_eq!(
        ParserError::ParserError("EXCEPT pipe operator requires DISTINCT modifier".to_string()),
        dialects
            .parse_sql_statements("SELECT * FROM users |> EXCEPT BY NAME (SELECT * FROM admins)")
            .unwrap_err()
    );

    // Test that EXCEPT ALL BY NAME fails
    assert_eq!(
        ParserError::ParserError("EXCEPT pipe operator requires DISTINCT modifier".to_string()),
        dialects
            .parse_sql_statements(
                "SELECT * FROM users |> EXCEPT ALL BY NAME (SELECT * FROM admins)"
            )
            .unwrap_err()
    );

    // Test that plain INTERSECT without DISTINCT fails
    assert_eq!(
        ParserError::ParserError("INTERSECT pipe operator requires DISTINCT modifier".to_string()),
        dialects
            .parse_sql_statements("SELECT * FROM users |> INTERSECT (SELECT * FROM admins)")
            .unwrap_err()
    );

    // Test that INTERSECT ALL fails
    assert_eq!(
        ParserError::ParserError("INTERSECT pipe operator requires DISTINCT modifier".to_string()),
        dialects
            .parse_sql_statements("SELECT * FROM users |> INTERSECT ALL (SELECT * FROM admins)")
            .unwrap_err()
    );

    // Test that INTERSECT BY NAME without DISTINCT fails
    assert_eq!(
        ParserError::ParserError("INTERSECT pipe operator requires DISTINCT modifier".to_string()),
        dialects
            .parse_sql_statements("SELECT * FROM users |> INTERSECT BY NAME (SELECT * FROM admins)")
            .unwrap_err()
    );

    // Test that INTERSECT ALL BY NAME fails
    assert_eq!(
        ParserError::ParserError("INTERSECT pipe operator requires DISTINCT modifier".to_string()),
        dialects
            .parse_sql_statements(
                "SELECT * FROM users |> INTERSECT ALL BY NAME (SELECT * FROM admins)"
            )
            .unwrap_err()
    );

    // Test that CALL without function name fails
    assert!(dialects
        .parse_sql_statements("SELECT * FROM users |> CALL")
        .is_err());

    // Test that CALL without parentheses fails
    assert!(dialects
        .parse_sql_statements("SELECT * FROM users |> CALL my_function")
        .is_err());

    // Test that CALL with invalid function syntax fails
    assert!(dialects
        .parse_sql_statements("SELECT * FROM users |> CALL 123invalid")
        .is_err());

    // Test that CALL with malformed arguments fails
    assert!(dialects
        .parse_sql_statements("SELECT * FROM users |> CALL my_function(,)")
        .is_err());

    // Test that CALL with invalid alias syntax fails
    assert!(dialects
        .parse_sql_statements("SELECT * FROM users |> CALL my_function() AS")
        .is_err());

    // Test that PIVOT without parentheses fails
    assert!(dialects
        .parse_sql_statements("SELECT * FROM users |> PIVOT SUM(amount) FOR month IN ('Jan')")
        .is_err());

    // Test that PIVOT without FOR keyword fails
    assert!(dialects
        .parse_sql_statements("SELECT * FROM users |> PIVOT(SUM(amount) month IN ('Jan'))")
        .is_err());

    // Test that PIVOT without IN keyword fails
    assert!(dialects
        .parse_sql_statements("SELECT * FROM users |> PIVOT(SUM(amount) FOR month ('Jan'))")
        .is_err());

    // Test that PIVOT with empty IN list fails
    assert!(dialects
        .parse_sql_statements("SELECT * FROM users |> PIVOT(SUM(amount) FOR month IN ())")
        .is_err());

    // Test that PIVOT with invalid alias syntax fails
    assert!(dialects
        .parse_sql_statements("SELECT * FROM users |> PIVOT(SUM(amount) FOR month IN ('Jan')) AS")
        .is_err());

    // Test UNPIVOT negative cases

    // Test that UNPIVOT without parentheses fails
    assert!(dialects
        .parse_sql_statements("SELECT * FROM users |> UNPIVOT value FOR name IN col1, col2")
        .is_err());

    // Test that UNPIVOT without FOR keyword fails
    assert!(dialects
        .parse_sql_statements("SELECT * FROM users |> UNPIVOT(value name IN (col1, col2))")
        .is_err());

    // Test that UNPIVOT without IN keyword fails
    assert!(dialects
        .parse_sql_statements("SELECT * FROM users |> UNPIVOT(value FOR name (col1, col2))")
        .is_err());

    // Test that UNPIVOT with missing value column fails
    assert!(dialects
        .parse_sql_statements("SELECT * FROM users |> UNPIVOT(FOR name IN (col1, col2))")
        .is_err());

    // Test that UNPIVOT with missing name column fails
    assert!(dialects
        .parse_sql_statements("SELECT * FROM users |> UNPIVOT(value FOR IN (col1, col2))")
        .is_err());

    // Test that UNPIVOT with empty IN list fails
    assert!(dialects
        .parse_sql_statements("SELECT * FROM users |> UNPIVOT(value FOR name IN ())")
        .is_err());

    // Test that UNPIVOT with invalid alias syntax fails
    assert!(dialects
        .parse_sql_statements("SELECT * FROM users |> UNPIVOT(value FOR name IN (col1, col2)) AS")
        .is_err());

    // Test that UNPIVOT with missing closing parenthesis fails
    assert!(dialects
        .parse_sql_statements("SELECT * FROM users |> UNPIVOT(value FOR name IN (col1, col2)")
        .is_err());

    // Test that JOIN without table name fails
    assert!(dialects
        .parse_sql_statements("SELECT * FROM users |> JOIN ON users.id = orders.user_id")
        .is_err());

    // Test that CROSS JOIN with ON condition fails
    assert!(dialects
        .parse_sql_statements(
            "SELECT * FROM users |> CROSS JOIN orders ON users.id = orders.user_id"
        )
        .is_err());

    // Test that CROSS JOIN with USING condition fails
    assert!(dialects
        .parse_sql_statements("SELECT * FROM users |> CROSS JOIN orders USING (user_id)")
        .is_err());

    // Test that JOIN with empty USING list fails
    assert!(dialects
        .parse_sql_statements("SELECT * FROM users |> JOIN orders USING ()")
        .is_err());

    // Test that JOIN with malformed ON condition fails
    assert!(dialects
        .parse_sql_statements("SELECT * FROM users |> JOIN orders ON")
        .is_err());

    // Test that JOIN with invalid USING syntax fails
    assert!(dialects
        .parse_sql_statements("SELECT * FROM users |> JOIN orders USING user_id")
        .is_err());
}

#[test]
fn parse_multiple_set_statements() -> Result<(), ParserError> {
    let dialects = all_dialects_where(|d| d.supports_comma_separated_set_assignments());
    let stmt = dialects.verified_stmt("SET @a = 1, b = 2");

    match stmt {
        Statement::Set(Set::MultipleAssignments { assignments }) => {
            assert_eq!(
                assignments,
                vec![
                    SetAssignment {
                        scope: None,
                        name: ObjectName::from(vec!["@a".into()]),
                        value: Expr::value(number("1"))
                    },
                    SetAssignment {
                        scope: None,
                        name: ObjectName::from(vec!["b".into()]),
                        value: Expr::value(number("2"))
                    }
                ]
            );
        }
        _ => panic!("Expected SetVariable with 2 variables and 2 values"),
    };

    let stmt = dialects.verified_stmt("SET GLOBAL @a = 1, SESSION b = 2, LOCAL c = 3, d = 4");

    match stmt {
        Statement::Set(Set::MultipleAssignments { assignments }) => {
            assert_eq!(
                assignments,
                vec![
                    SetAssignment {
                        scope: Some(ContextModifier::Global),
                        name: ObjectName::from(vec!["@a".into()]),
                        value: Expr::value(number("1"))
                    },
                    SetAssignment {
                        scope: Some(ContextModifier::Session),
                        name: ObjectName::from(vec!["b".into()]),
                        value: Expr::value(number("2"))
                    },
                    SetAssignment {
                        scope: Some(ContextModifier::Local),
                        name: ObjectName::from(vec!["c".into()]),
                        value: Expr::value(number("3"))
                    },
                    SetAssignment {
                        scope: None,
                        name: ObjectName::from(vec!["d".into()]),
                        value: Expr::value(number("4"))
                    }
                ]
            );
        }
        _ => panic!("Expected MultipleAssignments with 4 scoped variables and 4 values"),
    };

    Ok(())
}

#[test]
fn parse_set_time_zone_alias() {
    match all_dialects().verified_stmt("SET TIME ZONE 'UTC'") {
        Statement::Set(Set::SetTimeZone { local, value }) => {
            assert!(!local);
            assert_eq!(
                value,
                Expr::Value((Value::SingleQuotedString("UTC".into())).with_empty_span())
            );
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_return() {
    let stmt = all_dialects().verified_stmt("RETURN");
    assert_eq!(stmt, Statement::Return(ReturnStatement { value: None }));

    let _ = all_dialects().verified_stmt("RETURN 1");
}

#[test]
fn parse_subquery_limit() {
    let _ = all_dialects().verified_stmt("SELECT t1_id, t1_name FROM t1 WHERE t1_id IN (SELECT t2_id FROM t2 WHERE t1_name = t2_name LIMIT 10)");
}

#[test]
fn test_open() {
    let open_cursor = "OPEN Employee_Cursor";
    let stmt = all_dialects().verified_stmt(open_cursor);
    assert_eq!(
        stmt,
        Statement::Open(OpenStatement {
            cursor_name: Ident::new("Employee_Cursor"),
        })
    );
}

#[test]
fn parse_truncate_only() {
    let truncate = all_dialects().verified_stmt("TRUNCATE TABLE employee, ONLY dept");

    let table_names = vec![
        TruncateTableTarget {
            name: ObjectName::from(vec![Ident::new("employee")]),
            only: false,
        },
        TruncateTableTarget {
            name: ObjectName::from(vec![Ident::new("dept")]),
            only: true,
        },
    ];

    assert_eq!(
        Statement::Truncate(Truncate {
            table_names,
            partitions: None,
            table: true,
            identity: None,
            cascade: None,
            on_cluster: None,
        }),
        truncate
    );
}

#[test]
fn check_enforced() {
    all_dialects().verified_stmt(
        "CREATE TABLE t (a INT, b INT, c INT, CHECK (a > 0) NOT ENFORCED, CHECK (b > 0) ENFORCED, CHECK (c > 0))",
    );
}

#[test]
fn join_precedence() {
    all_dialects_except(|d| !d.supports_left_associative_joins_without_parens())
        .verified_query_with_canonical(
        "SELECT *
         FROM t1
         NATURAL JOIN t5
         INNER JOIN t0 ON (t0.v1 + t5.v0) > 0
         WHERE t0.v1 = t1.v0",
        // canonical string without parentheses
        "SELECT * FROM t1 NATURAL JOIN t5 INNER JOIN t0 ON (t0.v1 + t5.v0) > 0 WHERE t0.v1 = t1.v0",
    );
    all_dialects_except(|d| d.supports_left_associative_joins_without_parens()).verified_query_with_canonical(
        "SELECT *
         FROM t1
         NATURAL JOIN t5
         INNER JOIN t0 ON (t0.v1 + t5.v0) > 0
         WHERE t0.v1 = t1.v0",
        // canonical string with parentheses
        "SELECT * FROM t1 NATURAL JOIN (t5 INNER JOIN t0 ON (t0.v1 + t5.v0) > 0) WHERE t0.v1 = t1.v0",
    );
}

#[test]
fn parse_create_procedure_with_language() {
    let sql = r#"CREATE PROCEDURE test_proc LANGUAGE sql AS BEGIN SELECT 1; END"#;
    match verified_stmt(sql) {
        Statement::CreateProcedure {
            or_alter,
            name,
            params,
            language,
            ..
        } => {
            assert_eq!(or_alter, false);
            assert_eq!(name.to_string(), "test_proc");
            assert_eq!(params, Some(vec![]));
            assert_eq!(
                language,
                Some(Ident {
                    value: "sql".into(),
                    quote_style: None,
                    span: Span {
                        start: Location::empty(),
                        end: Location::empty()
                    }
                })
            );
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_procedure_with_parameter_modes() {
    let sql = r#"CREATE PROCEDURE test_proc (IN a INTEGER, OUT b TEXT, INOUT c TIMESTAMP, d BOOL) AS BEGIN SELECT 1; END"#;
    match verified_stmt(sql) {
        Statement::CreateProcedure {
            or_alter,
            name,
            params,
            ..
        } => {
            assert_eq!(or_alter, false);
            assert_eq!(name.to_string(), "test_proc");
            let fake_span = Span {
                start: Location { line: 0, column: 0 },
                end: Location { line: 0, column: 0 },
            };
            assert_eq!(
                params,
                Some(vec![
                    ProcedureParam {
                        name: Ident {
                            value: "a".into(),
                            quote_style: None,
                            span: fake_span,
                        },
                        data_type: DataType::Integer(None),
                        mode: Some(ArgMode::In),
                        default: None,
                    },
                    ProcedureParam {
                        name: Ident {
                            value: "b".into(),
                            quote_style: None,
                            span: fake_span,
                        },
                        data_type: DataType::Text,
                        mode: Some(ArgMode::Out),
                        default: None,
                    },
                    ProcedureParam {
                        name: Ident {
                            value: "c".into(),
                            quote_style: None,
                            span: fake_span,
                        },
                        data_type: DataType::Timestamp(None, TimezoneInfo::None),
                        mode: Some(ArgMode::InOut),
                        default: None,
                    },
                    ProcedureParam {
                        name: Ident {
                            value: "d".into(),
                            quote_style: None,
                            span: fake_span,
                        },
                        data_type: DataType::Bool,
                        mode: None,
                        default: None,
                    },
                ])
            );
        }
        _ => unreachable!(),
    }

    // parameters with default values
    let sql = r#"CREATE PROCEDURE test_proc (IN a INTEGER = 1, OUT b TEXT = '2', INOUT c TIMESTAMP = NULL, d BOOL = 0) AS BEGIN SELECT 1; END"#;
    match verified_stmt(sql) {
        Statement::CreateProcedure {
            or_alter,
            name,
            params,
            ..
        } => {
            assert_eq!(or_alter, false);
            assert_eq!(name.to_string(), "test_proc");
            assert_eq!(
                params,
                Some(vec![
                    ProcedureParam {
                        name: Ident::new("a"),
                        data_type: DataType::Integer(None),
                        mode: Some(ArgMode::In),
                        default: Some(Expr::Value((number("1")).with_empty_span())),
                    },
                    ProcedureParam {
                        name: Ident::new("b"),
                        data_type: DataType::Text,
                        mode: Some(ArgMode::Out),
                        default: Some(Expr::Value(
                            Value::SingleQuotedString("2".into()).with_empty_span()
                        )),
                    },
                    ProcedureParam {
                        name: Ident::new("c"),
                        data_type: DataType::Timestamp(None, TimezoneInfo::None),
                        mode: Some(ArgMode::InOut),
                        default: Some(Expr::Value(Value::Null.with_empty_span())),
                    },
                    ProcedureParam {
                        name: Ident::new("d"),
                        data_type: DataType::Bool,
                        mode: None,
                        default: Some(Expr::Value((number("0")).with_empty_span())),
                    }
                ]),
            );
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_not_null() {
    let _ = all_dialects().expr_parses_to("x NOT NULL", "x IS NOT NULL");
    let _ = all_dialects().expr_parses_to("NULL NOT NULL", "NULL IS NOT NULL");

    assert_matches!(
        all_dialects().expr_parses_to("NOT NULL NOT NULL", "NOT NULL IS NOT NULL"),
        Expr::UnaryOp {
            op: UnaryOperator::Not,
            ..
        }
    );
    assert_matches!(
        all_dialects().expr_parses_to("NOT x NOT NULL", "NOT x IS NOT NULL"),
        Expr::UnaryOp {
            op: UnaryOperator::Not,
            ..
        }
    );
}

#[test]
fn test_select_exclude() {
    let dialects = all_dialects_where(|d| d.supports_select_wildcard_exclude());
    match &dialects
        .verified_only_select("SELECT * EXCLUDE c1 FROM test")
        .projection[0]
    {
        SelectItem::Wildcard(WildcardAdditionalOptions { opt_exclude, .. }) => {
            assert_eq!(
                *opt_exclude,
                Some(ExcludeSelectItem::Single(Ident::new("c1")))
            );
        }
        _ => unreachable!(),
    }
    match &dialects
        .verified_only_select("SELECT * EXCLUDE (c1, c2) FROM test")
        .projection[0]
    {
        SelectItem::Wildcard(WildcardAdditionalOptions { opt_exclude, .. }) => {
            assert_eq!(
                *opt_exclude,
                Some(ExcludeSelectItem::Multiple(vec![
                    Ident::new("c1"),
                    Ident::new("c2")
                ]))
            );
        }
        _ => unreachable!(),
    }
    let select = dialects.verified_only_select("SELECT * EXCLUDE c1, c2 FROM test");
    match &select.projection[0] {
        SelectItem::Wildcard(WildcardAdditionalOptions { opt_exclude, .. }) => {
            assert_eq!(
                *opt_exclude,
                Some(ExcludeSelectItem::Single(Ident::new("c1")))
            );
        }
        _ => unreachable!(),
    }
    match &select.projection[1] {
        SelectItem::UnnamedExpr(Expr::Identifier(ident)) => {
            assert_eq!(*ident, Ident::new("c2"));
        }
        _ => unreachable!(),
    }

    let dialects = all_dialects_where(|d| d.supports_select_exclude());
    let select = dialects.verified_only_select("SELECT *, c1 EXCLUDE c1 FROM test");
    match &select.projection[0] {
        SelectItem::Wildcard(additional_options) => {
            assert_eq!(*additional_options, WildcardAdditionalOptions::default());
        }
        _ => unreachable!(),
    }
    assert_eq!(
        select.exclude,
        Some(ExcludeSelectItem::Single(Ident::new("c1")))
    );

    let dialects = all_dialects_where(|d| {
        d.supports_select_wildcard_exclude() && !d.supports_select_exclude()
    });
    let select = dialects.verified_only_select("SELECT * EXCLUDE c1 FROM test");
    match &select.projection[0] {
        SelectItem::Wildcard(WildcardAdditionalOptions { opt_exclude, .. }) => {
            assert_eq!(
                *opt_exclude,
                Some(ExcludeSelectItem::Single(Ident::new("c1")))
            );
        }
        _ => unreachable!(),
    }

    // Dialects that only support the wildcard form and do not accept EXCLUDE as an implicity alias
    // will fail when encountered with the `c2` ident
    let dialects = all_dialects_where(|d| {
        d.supports_select_wildcard_exclude()
            && !d.supports_select_exclude()
            && d.is_column_alias(&Keyword::EXCLUDE, &mut Parser::new(d))
    });
    assert_eq!(
        dialects
            .parse_sql_statements("SELECT *, c1 EXCLUDE c2 FROM test")
            .err()
            .unwrap(),
        ParserError::ParserError("Expected: end of statement, found: c2".to_string())
    );

    // Dialects that only support the wildcard form and accept EXCLUDE as an implicity alias
    // will fail when encountered with the `EXCLUDE` keyword
    let dialects = all_dialects_where(|d| {
        d.supports_select_wildcard_exclude()
            && !d.supports_select_exclude()
            && !d.is_column_alias(&Keyword::EXCLUDE, &mut Parser::new(d))
    });
    assert_eq!(
        dialects
            .parse_sql_statements("SELECT *, c1 EXCLUDE c2 FROM test")
            .err()
            .unwrap(),
        ParserError::ParserError("Expected: end of statement, found: EXCLUDE".to_string())
    );
}

#[test]
fn test_no_semicolon_required_between_statements() {
    let sql = r#"
SELECT * FROM tbl1
SELECT * FROM tbl2
    "#;

    let dialects = all_dialects_with_options(ParserOptions {
        trailing_commas: false,
        unescape: true,
        require_semicolon_stmt_delimiter: false,
    });
    let stmts = dialects.parse_sql_statements(sql).unwrap();
    assert_eq!(stmts.len(), 2);
    assert!(stmts.iter().all(|s| matches!(s, Statement::Query { .. })));
}

#[test]
fn test_identifier_unicode_support() {
    let sql = r#"SELECT phoneǤЖשचᎯ⻩☯♜🦄⚛🀄ᚠ⌛🌀 AS tbl FROM customers"#;
    let dialects = TestedDialects::new(vec![
        Box::new(MySqlDialect {}),
        Box::new(RedshiftSqlDialect {}),
        Box::new(PostgreSqlDialect {}),
    ]);
    let _ = dialects.verified_stmt(sql);
}

#[test]
fn test_identifier_unicode_start() {
    let sql = r#"SELECT 💝phone AS 💝 FROM customers"#;
    let dialects = TestedDialects::new(vec![
        Box::new(MySqlDialect {}),
        Box::new(RedshiftSqlDialect {}),
        Box::new(PostgreSqlDialect {}),
    ]);
    let _ = dialects.verified_stmt(sql);
}

#[test]
fn parse_notnull() {
    // Some dialects support `x NOTNULL` as an expression while others consider
    // `x NOTNULL` like `x AS NOTNULL` and thus consider `NOTNULL` an alias for x.
    let notnull_unsupported_dialects = all_dialects_except(|d| d.supports_notnull_operator());
    let _ = notnull_unsupported_dialects
        .verified_only_select_with_canonical("SELECT NULL NOTNULL", "SELECT NULL AS NOTNULL");

    // Supported dialects consider `x NOTNULL` as an alias for `x IS NOT NULL`
    let notnull_supported_dialects = all_dialects_where(|d| d.supports_notnull_operator());
    let _ = notnull_supported_dialects.expr_parses_to("x NOTNULL", "x IS NOT NULL");

    // For dialects which support it, `NOT NULL NOTNULL` should
    // parse as `(NOT (NULL IS NOT NULL))`
    assert_matches!(
        notnull_supported_dialects.expr_parses_to("NOT NULL NOTNULL", "NOT NULL IS NOT NULL"),
        Expr::UnaryOp {
            op: UnaryOperator::Not,
            ..
        }
    );

    // for unsupported dialects, parsing should stop at `NOT NULL`
    notnull_unsupported_dialects.expr_parses_to("NOT NULL NOTNULL", "NOT NULL");
}

#[test]
fn parse_odbc_time_date_timestamp() {
    // Supported statements
    let sql_d = "SELECT {d '2025-07-17'}, category_name FROM categories";
    let _ = all_dialects().verified_stmt(sql_d);
    let sql_t = "SELECT {t '14:12:01'}, category_name FROM categories";
    let _ = all_dialects().verified_stmt(sql_t);
    let sql_ts = "SELECT {ts '2025-07-17 14:12:01'}, category_name FROM categories";
    let _ = all_dialects().verified_stmt(sql_ts);
    // Unsupported statement
    let supports_dictionary = all_dialects_where(|d| d.supports_dictionary_syntax());
    let dictionary_unsupported = all_dialects_where(|d| !d.supports_dictionary_syntax());
    let sql = "SELECT {tt '14:12:01'} FROM foo";
    let res = supports_dictionary.parse_sql_statements(sql);
    let res_dict = dictionary_unsupported.parse_sql_statements(sql);
    assert_eq!(
        ParserError::ParserError("Expected: :, found: '14:12:01'".to_string()),
        res.unwrap_err()
    );
    assert_eq!(
        ParserError::ParserError("Expected: an expression, found: {".to_string()),
        res_dict.unwrap_err()
    );
}

#[test]
fn parse_create_user() {
    let create = verified_stmt("CREATE USER u1");
    match create {
        Statement::CreateUser(stmt) => {
            assert_eq!(stmt.name, Ident::new("u1"));
        }
        _ => unreachable!(),
    }
    verified_stmt("CREATE OR REPLACE USER u1");
    verified_stmt("CREATE OR REPLACE USER IF NOT EXISTS u1");
    verified_stmt("CREATE OR REPLACE USER IF NOT EXISTS u1 PASSWORD='secret'");
    let dialects = all_dialects_where(|d| d.supports_boolean_literals());
    dialects.one_statement_parses_to(
        "CREATE OR REPLACE USER IF NOT EXISTS u1 PASSWORD='secret' MUST_CHANGE_PASSWORD=TRUE",
        "CREATE OR REPLACE USER IF NOT EXISTS u1 PASSWORD='secret' MUST_CHANGE_PASSWORD=true",
    );
    dialects.verified_stmt("CREATE OR REPLACE USER IF NOT EXISTS u1 PASSWORD='secret' MUST_CHANGE_PASSWORD=true TYPE=SERVICE TAG (t1='v1')");
    let create = dialects.verified_stmt("CREATE OR REPLACE USER IF NOT EXISTS u1 PASSWORD='secret' MUST_CHANGE_PASSWORD=false TYPE=SERVICE WITH TAG (t1='v1', t2='v2')");
    match create {
        Statement::CreateUser(stmt) => {
            assert_eq!(stmt.name, Ident::new("u1"));
            assert_eq!(stmt.or_replace, true);
            assert_eq!(stmt.if_not_exists, true);
            assert_eq!(
                stmt.options,
                KeyValueOptions {
                    delimiter: KeyValueOptionsDelimiter::Space,
                    options: vec![
                        KeyValueOption {
                            option_name: "PASSWORD".to_string(),
                            option_value: KeyValueOptionKind::Single(Value::SingleQuotedString(
                                "secret".to_string()
                            )),
                        },
                        KeyValueOption {
                            option_name: "MUST_CHANGE_PASSWORD".to_string(),
                            option_value: KeyValueOptionKind::Single(Value::Boolean(false)),
                        },
                        KeyValueOption {
                            option_name: "TYPE".to_string(),
                            option_value: KeyValueOptionKind::Single(Value::Placeholder(
                                "SERVICE".to_string()
                            )),
                        },
                    ],
                },
            );
            assert_eq!(stmt.with_tags, true);
            assert_eq!(
                stmt.tags,
                KeyValueOptions {
                    delimiter: KeyValueOptionsDelimiter::Comma,
                    options: vec![
                        KeyValueOption {
                            option_name: "t1".to_string(),
                            option_value: KeyValueOptionKind::Single(Value::SingleQuotedString(
                                "v1".to_string()
                            )),
                        },
                        KeyValueOption {
                            option_name: "t2".to_string(),
                            option_value: KeyValueOptionKind::Single(Value::SingleQuotedString(
                                "v2".to_string()
                            )),
                        },
                    ]
                }
            );
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_drop_stream() {
    let sql = "DROP STREAM s1";
    match verified_stmt(sql) {
        Statement::Drop {
            names, object_type, ..
        } => {
            assert_eq!(
                vec!["s1"],
                names.iter().map(ToString::to_string).collect::<Vec<_>>()
            );
            assert_eq!(ObjectType::Stream, object_type);
        }
        _ => unreachable!(),
    }
    verified_stmt("DROP STREAM IF EXISTS s1");
}

#[test]
fn parse_create_view_if_not_exists() {
    // Name after IF NOT EXISTS
    let sql: &'static str = "CREATE VIEW IF NOT EXISTS v AS SELECT 1";
    let _ = all_dialects().verified_stmt(sql);
    // Name before IF NOT EXISTS
    let sql = "CREATE VIEW v IF NOT EXISTS AS SELECT 1";
    let _ = all_dialects().verified_stmt(sql);
    // Name missing from query
    let sql = "CREATE VIEW IF NOT EXISTS AS SELECT 1";
    let res = all_dialects().parse_sql_statements(sql);
    assert_eq!(
        ParserError::ParserError("Expected: AS, found: SELECT".to_string()),
        res.unwrap_err()
    );
}

#[test]
fn test_parse_not_null_in_column_options() {
    let canonical = concat!(
        "CREATE TABLE foo (",
        "abc INT DEFAULT (42 IS NOT NULL) NOT NULL,",
        " def INT,",
        " def_null BOOL GENERATED ALWAYS AS (def IS NOT NULL) STORED,",
        " CHECK (abc IS NOT NULL)",
        ")"
    );
    all_dialects().verified_stmt(canonical);
    all_dialects().one_statement_parses_to(
        concat!(
            "CREATE TABLE foo (",
            "abc INT DEFAULT (42 NOT NULL) NOT NULL,",
            " def INT,",
            " def_null BOOL GENERATED ALWAYS AS (def NOT NULL) STORED,",
            " CHECK (abc NOT NULL)",
            ")"
        ),
        canonical,
    );
}

#[test]
fn test_parse_default_with_collate_column_option() {
    let sql = "CREATE TABLE foo (abc TEXT DEFAULT 'foo' COLLATE 'en_US')";
    let stmt = all_dialects().verified_stmt(sql);
    if let Statement::CreateTable(CreateTable { mut columns, .. }) = stmt {
        let mut column = columns.pop().unwrap();
        assert_eq!(&column.name.value, "abc");
        assert_eq!(column.data_type, DataType::Text);
        let collate_option = column.options.pop().unwrap();
        if let ColumnOptionDef {
            name: None,
            option: ColumnOption::Collation(collate),
        } = collate_option
        {
            assert_eq!(collate.to_string(), "'en_US'");
        } else {
            panic!("Expected collate column option, got {collate_option}");
        }
        let default_option = column.options.pop().unwrap();
        if let ColumnOptionDef {
            name: None,
            option: ColumnOption::Default(Expr::Value(value)),
        } = default_option
        {
            assert_eq!(value.to_string(), "'foo'");
        } else {
            panic!("Expected default column option, got {default_option}");
        }
    } else {
        panic!("Expected create table statement");
    }
}

#[test]
fn parse_create_table_like() {
    let dialects = all_dialects_except(|d| d.supports_create_table_like_parenthesized());
    let sql = "CREATE TABLE new LIKE old";
    match dialects.verified_stmt(sql) {
        Statement::CreateTable(stmt) => {
            assert_eq!(
                stmt.name,
                ObjectName::from(vec![Ident::new("new".to_string())])
            );
            assert_eq!(
                stmt.like,
                Some(CreateTableLikeKind::Plain(CreateTableLike {
                    name: ObjectName::from(vec![Ident::new("old".to_string())]),
                    defaults: None,
                }))
            )
        }
        _ => unreachable!(),
    }
    let dialects = all_dialects_where(|d| d.supports_create_table_like_parenthesized());
    let sql = "CREATE TABLE new (LIKE old)";
    match dialects.verified_stmt(sql) {
        Statement::CreateTable(stmt) => {
            assert_eq!(
                stmt.name,
                ObjectName::from(vec![Ident::new("new".to_string())])
            );
            assert_eq!(
                stmt.like,
                Some(CreateTableLikeKind::Parenthesized(CreateTableLike {
                    name: ObjectName::from(vec![Ident::new("old".to_string())]),
                    defaults: None,
                }))
            )
        }
        _ => unreachable!(),
    }
    let sql = "CREATE TABLE new (LIKE old INCLUDING DEFAULTS)";
    match dialects.verified_stmt(sql) {
        Statement::CreateTable(stmt) => {
            assert_eq!(
                stmt.name,
                ObjectName::from(vec![Ident::new("new".to_string())])
            );
            assert_eq!(
                stmt.like,
                Some(CreateTableLikeKind::Parenthesized(CreateTableLike {
                    name: ObjectName::from(vec![Ident::new("old".to_string())]),
                    defaults: Some(CreateTableLikeDefaults::Including),
                }))
            )
        }
        _ => unreachable!(),
    }
    let sql = "CREATE TABLE new (LIKE old EXCLUDING DEFAULTS)";
    match dialects.verified_stmt(sql) {
        Statement::CreateTable(stmt) => {
            assert_eq!(
                stmt.name,
                ObjectName::from(vec![Ident::new("new".to_string())])
            );
            assert_eq!(
                stmt.like,
                Some(CreateTableLikeKind::Parenthesized(CreateTableLike {
                    name: ObjectName::from(vec![Ident::new("old".to_string())]),
                    defaults: Some(CreateTableLikeDefaults::Excluding),
                }))
            )
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_copy_options() {
    let copy = verified_stmt(
        r#"COPY dst (c1, c2, c3) FROM 's3://redshift-downloads/tickit/category_pipe.txt' IAM_ROLE 'arn:aws:iam::123456789:role/role1' CSV IGNOREHEADER 1"#,
    );
    match copy {
        Statement::Copy { legacy_options, .. } => {
            assert_eq!(
                legacy_options,
                vec![
                    CopyLegacyOption::IamRole(IamRoleKind::Arn(
                        "arn:aws:iam::123456789:role/role1".to_string()
                    )),
                    CopyLegacyOption::Csv(vec![]),
                    CopyLegacyOption::IgnoreHeader(1),
                ]
            );
        }
        _ => unreachable!(),
    }

    let copy = one_statement_parses_to(
        r#"COPY dst (c1, c2, c3) FROM 's3://redshift-downloads/tickit/category_pipe.txt' IAM_ROLE DEFAULT CSV IGNOREHEADER AS 1"#,
        r#"COPY dst (c1, c2, c3) FROM 's3://redshift-downloads/tickit/category_pipe.txt' IAM_ROLE DEFAULT CSV IGNOREHEADER 1"#,
    );
    match copy {
        Statement::Copy { legacy_options, .. } => {
            assert_eq!(
                legacy_options,
                vec![
                    CopyLegacyOption::IamRole(IamRoleKind::Default),
                    CopyLegacyOption::Csv(vec![]),
                    CopyLegacyOption::IgnoreHeader(1),
                ]
            );
        }
        _ => unreachable!(),
    }
    one_statement_parses_to(
        concat!(
            "COPY dst (c1, c2, c3) FROM 's3://redshift-downloads/tickit/category_pipe.txt' ",
            "ACCEPTANYDATE ",
            "ACCEPTINVCHARS AS '*' ",
            "BLANKSASNULL ",
            "CSV ",
            "DATEFORMAT AS 'DD-MM-YYYY' ",
            "EMPTYASNULL ",
            "IAM_ROLE DEFAULT ",
            "IGNOREHEADER AS 1 ",
            "TIMEFORMAT AS 'auto' ",
            "TRUNCATECOLUMNS",
        ),
        concat!(
            "COPY dst (c1, c2, c3) FROM 's3://redshift-downloads/tickit/category_pipe.txt' ",
            "ACCEPTANYDATE ",
            "ACCEPTINVCHARS '*' ",
            "BLANKSASNULL ",
            "CSV ",
            "DATEFORMAT 'DD-MM-YYYY' ",
            "EMPTYASNULL ",
            "IAM_ROLE DEFAULT ",
            "IGNOREHEADER 1 ",
            "TIMEFORMAT 'auto' ",
            "TRUNCATECOLUMNS",
        ),
    );
    one_statement_parses_to(
        "COPY dst (c1, c2, c3) FROM 's3://redshift-downloads/tickit/category_pipe.txt' FORMAT AS CSV",
        "COPY dst (c1, c2, c3) FROM 's3://redshift-downloads/tickit/category_pipe.txt' CSV",
    );
}

#[test]
fn test_parse_semantic_view_table_factor() {
    let dialects = all_dialects_where(|d| d.supports_semantic_view_table_factor());

    let valid_sqls = [
        ("SELECT * FROM SEMANTIC_VIEW(model)", None),
        (
            "SELECT * FROM SEMANTIC_VIEW(model DIMENSIONS dim1, dim2)",
            None,
        ),
        ("SELECT * FROM SEMANTIC_VIEW(a.b METRICS c.d, c.e)", None),
        (
            "SELECT * FROM SEMANTIC_VIEW(model FACTS fact1, fact2)",
            None,
        ),
        (
            "SELECT * FROM SEMANTIC_VIEW(model FACTS DATE_PART('year', col))",
            None,
        ),
        (
            "SELECT * FROM SEMANTIC_VIEW(model DIMENSIONS dim1 METRICS met1)",
            None,
        ),
        (
            "SELECT * FROM SEMANTIC_VIEW(model DIMENSIONS dim1 WHERE x > 0)",
            None,
        ),
        (
            "SELECT * FROM SEMANTIC_VIEW(model DIMENSIONS dim1) AS sv",
            None,
        ),
        (
            "SELECT * FROM SEMANTIC_VIEW(model DIMENSIONS DATE_PART('year', col))",
            None,
        ),
        (
            "SELECT * FROM SEMANTIC_VIEW(model METRICS orders.col, orders.col2)",
            None,
        ),
        ("SELECT * FROM SEMANTIC_VIEW(model METRICS orders.*)", None),
        ("SELECT * FROM SEMANTIC_VIEW(model FACTS fact.*)", None),
        (
            "SELECT * FROM SEMANTIC_VIEW(model DIMENSIONS dim.* METRICS orders.*)",
            None,
        ),
        // We can parse in any order but will always produce a result in a fixed order.
        (
            "SELECT * FROM SEMANTIC_VIEW(model WHERE x > 0 DIMENSIONS dim1)",
            Some("SELECT * FROM SEMANTIC_VIEW(model DIMENSIONS dim1 WHERE x > 0)"),
        ),
        (
            "SELECT * FROM SEMANTIC_VIEW(model METRICS met1 DIMENSIONS dim1)",
            Some("SELECT * FROM SEMANTIC_VIEW(model DIMENSIONS dim1 METRICS met1)"),
        ),
        (
            "SELECT * FROM SEMANTIC_VIEW(model FACTS fact1 DIMENSIONS dim1)",
            Some("SELECT * FROM SEMANTIC_VIEW(model DIMENSIONS dim1 FACTS fact1)"),
        ),
    ];

    for (input_sql, expected_sql) in valid_sqls {
        if let Some(expected) = expected_sql {
            // Test that non-canonical order gets normalized
            let parsed = dialects.parse_sql_statements(input_sql).unwrap();
            let formatted = parsed[0].to_string();
            assert_eq!(formatted, expected);
        } else {
            dialects.verified_stmt(input_sql);
        }
    }

    let invalid_sqls = [
        "SELECT * FROM SEMANTIC_VIEW(model DIMENSIONS dim1 INVALID inv1)",
        "SELECT * FROM SEMANTIC_VIEW(model DIMENSIONS dim1 DIMENSIONS dim2)",
    ];

    for sql in invalid_sqls {
        let result = dialects.parse_sql_statements(sql);
        assert!(result.is_err(), "Expected error for invalid SQL: {sql}");
    }

    let ast_sql = r#"SELECT * FROM SEMANTIC_VIEW(
        my_model
        DIMENSIONS DATE_PART('year', date_col), region_name
        METRICS orders.revenue, orders.count
        WHERE active = true
    ) AS model_alias"#;

    let stmt = dialects.parse_sql_statements(ast_sql).unwrap();
    match &stmt[0] {
        Statement::Query(q) => {
            if let SetExpr::Select(select) = q.body.as_ref() {
                if let Some(TableWithJoins { relation, .. }) = select.from.first() {
                    match relation {
                        TableFactor::SemanticView {
                            name,
                            dimensions,
                            metrics,
                            facts,
                            where_clause,
                            alias,
                        } => {
                            assert_eq!(name.to_string(), "my_model");
                            assert_eq!(dimensions.len(), 2);
                            assert_eq!(dimensions[0].to_string(), "DATE_PART('year', date_col)");
                            assert_eq!(dimensions[1].to_string(), "region_name");
                            assert_eq!(metrics.len(), 2);
                            assert_eq!(metrics[0].to_string(), "orders.revenue");
                            assert_eq!(metrics[1].to_string(), "orders.count");
                            assert!(facts.is_empty());
                            assert!(where_clause.is_some());
                            assert_eq!(where_clause.as_ref().unwrap().to_string(), "active = true");
                            assert!(alias.is_some());
                            assert_eq!(alias.as_ref().unwrap().name.value, "model_alias");
                        }
                        _ => panic!("Expected SemanticView table factor"),
                    }
                } else {
                    panic!("Expected table in FROM clause");
                }
            } else {
                panic!("Expected SELECT statement");
            }
        }
        _ => panic!("Expected Query statement"),
    }
}

#[test]
fn parse_adjacent_string_literal_concatenation() {
    let sql = r#"SELECT 'M' "y" 'S' "q" 'l'"#;
    let dialects = all_dialects_where(|d| d.supports_string_literal_concatenation());
    dialects.one_statement_parses_to(sql, r"SELECT 'MySql'");

    let sql = "SELECT * FROM t WHERE col = 'Hello' \n ' ' \t 'World!'";
    dialects.one_statement_parses_to(sql, r"SELECT * FROM t WHERE col = 'Hello World!'");
}

#[test]
fn parse_invisible_column() {
    let sql = r#"CREATE TABLE t (foo INT, bar INT INVISIBLE)"#;
    let stmt = verified_stmt(sql);
    match stmt {
        Statement::CreateTable(CreateTable { columns, .. }) => {
            assert_eq!(
                columns,
                vec![
                    ColumnDef {
                        name: "foo".into(),
                        data_type: DataType::Int(None),
                        options: vec![]
                    },
                    ColumnDef {
                        name: "bar".into(),
                        data_type: DataType::Int(None),
                        options: vec![ColumnOptionDef {
                            name: None,
                            option: ColumnOption::Invisible
                        }]
                    }
                ]
            );
        }
        _ => panic!("Unexpected statement {stmt}"),
    }

    let sql = r#"ALTER TABLE t ADD COLUMN bar INT INVISIBLE"#;
    let stmt = verified_stmt(sql);
    match stmt {
        Statement::AlterTable(alter_table) => {
            assert_eq!(
                alter_table.operations,
                vec![AlterTableOperation::AddColumn {
                    column_keyword: true,
                    if_not_exists: false,
                    column_def: ColumnDef {
                        name: "bar".into(),
                        data_type: DataType::Int(None),
                        options: vec![ColumnOptionDef {
                            name: None,
                            option: ColumnOption::Invisible
                        }]
                    },
                    column_position: None
                }]
            );
        }
        _ => panic!("Unexpected statement {stmt}"),
    }
}

#[test]
fn parse_create_index_different_using_positions() {
    let sql = "CREATE INDEX idx_name USING BTREE ON table_name (col1)";
    let expected = "CREATE INDEX idx_name ON table_name USING BTREE (col1)";
    match all_dialects().one_statement_parses_to(sql, expected) {
        Statement::CreateIndex(CreateIndex {
            name,
            table_name,
            using,
            columns,
            unique,
            ..
        }) => {
            assert_eq!(name.unwrap().to_string(), "idx_name");
            assert_eq!(table_name.to_string(), "table_name");
            assert_eq!(using, Some(IndexType::BTree));
            assert_eq!(columns.len(), 1);
            assert!(!unique);
        }
        _ => unreachable!(),
    }

    let sql = "CREATE INDEX idx_name USING BTREE ON table_name (col1) USING HASH";
    let expected = "CREATE INDEX idx_name ON table_name USING BTREE (col1) USING HASH";
    match all_dialects().one_statement_parses_to(sql, expected) {
        Statement::CreateIndex(CreateIndex {
            name,
            table_name,
            columns,
            index_options,
            ..
        }) => {
            assert_eq!(name.unwrap().to_string(), "idx_name");
            assert_eq!(table_name.to_string(), "table_name");
            assert_eq!(columns.len(), 1);
            assert!(index_options
                .iter()
                .any(|o| o == &IndexOption::Using(IndexType::Hash)));
        }
        _ => unreachable!(),
    }
}

#[test]
fn test_parse_alter_user() {
    verified_stmt("ALTER USER u1");
    verified_stmt("ALTER USER IF EXISTS u1");
    let stmt = verified_stmt("ALTER USER IF EXISTS u1 RENAME TO u2");
    match stmt {
        Statement::AlterUser(alter) => {
            assert!(alter.if_exists);
            assert_eq!(alter.name, Ident::new("u1"));
            assert_eq!(alter.rename_to, Some(Ident::new("u2")));
        }
        _ => unreachable!(),
    }
    verified_stmt("ALTER USER IF EXISTS u1 RESET PASSWORD");
    verified_stmt("ALTER USER IF EXISTS u1 ABORT ALL QUERIES");
    verified_stmt(
        "ALTER USER IF EXISTS u1 ADD DELEGATED AUTHORIZATION OF ROLE r1 TO SECURITY INTEGRATION i1",
    );
    verified_stmt("ALTER USER IF EXISTS u1 REMOVE DELEGATED AUTHORIZATION OF ROLE r1 FROM SECURITY INTEGRATION i1");
    verified_stmt(
        "ALTER USER IF EXISTS u1 REMOVE DELEGATED AUTHORIZATIONS FROM SECURITY INTEGRATION i1",
    );
    verified_stmt("ALTER USER IF EXISTS u1 ENROLL MFA");
    let stmt = verified_stmt("ALTER USER u1 SET DEFAULT_MFA_METHOD PASSKEY");
    match stmt {
        Statement::AlterUser(alter) => {
            assert_eq!(alter.set_default_mfa_method, Some(MfaMethodKind::PassKey))
        }
        _ => unreachable!(),
    }
    verified_stmt("ALTER USER u1 SET DEFAULT_MFA_METHOD TOTP");
    verified_stmt("ALTER USER u1 SET DEFAULT_MFA_METHOD DUO");
    let stmt = verified_stmt("ALTER USER u1 REMOVE MFA METHOD PASSKEY");
    match stmt {
        Statement::AlterUser(alter) => {
            assert_eq!(alter.remove_mfa_method, Some(MfaMethodKind::PassKey))
        }
        _ => unreachable!(),
    }
    verified_stmt("ALTER USER u1 REMOVE MFA METHOD TOTP");
    verified_stmt("ALTER USER u1 REMOVE MFA METHOD DUO");
    let stmt = verified_stmt("ALTER USER u1 MODIFY MFA METHOD PASSKEY SET COMMENT 'abc'");
    match stmt {
        Statement::AlterUser(alter) => {
            assert_eq!(
                alter.modify_mfa_method,
                Some(AlterUserModifyMfaMethod {
                    method: MfaMethodKind::PassKey,
                    comment: "abc".to_string()
                })
            );
        }
        _ => unreachable!(),
    }
    verified_stmt("ALTER USER u1 ADD MFA METHOD OTP");
    verified_stmt("ALTER USER u1 ADD MFA METHOD OTP COUNT = 8");

    let stmt = verified_stmt("ALTER USER u1 SET AUTHENTICATION POLICY p1");
    match stmt {
        Statement::AlterUser(alter) => {
            assert_eq!(
                alter.set_policy,
                Some(AlterUserSetPolicy {
                    policy_kind: UserPolicyKind::Authentication,
                    policy: Ident::new("p1")
                })
            );
        }
        _ => unreachable!(),
    }
    verified_stmt("ALTER USER u1 SET PASSWORD POLICY p1");
    verified_stmt("ALTER USER u1 SET SESSION POLICY p1");
    let stmt = verified_stmt("ALTER USER u1 UNSET AUTHENTICATION POLICY");
    match stmt {
        Statement::AlterUser(alter) => {
            assert_eq!(alter.unset_policy, Some(UserPolicyKind::Authentication));
        }
        _ => unreachable!(),
    }
    verified_stmt("ALTER USER u1 UNSET PASSWORD POLICY");
    verified_stmt("ALTER USER u1 UNSET SESSION POLICY");

    let stmt = verified_stmt("ALTER USER u1 SET TAG k1='v1'");
    match stmt {
        Statement::AlterUser(alter) => {
            assert_eq!(
                alter.set_tag.options,
                vec![KeyValueOption {
                    option_name: "k1".to_string(),
                    option_value: KeyValueOptionKind::Single(Value::SingleQuotedString(
                        "v1".to_string()
                    )),
                },]
            );
        }
        _ => unreachable!(),
    }
    verified_stmt("ALTER USER u1 SET TAG k1='v1', k2='v2'");
    let stmt = verified_stmt("ALTER USER u1 UNSET TAG k1");
    match stmt {
        Statement::AlterUser(alter) => {
            assert_eq!(alter.unset_tag, vec!["k1".to_string()]);
        }
        _ => unreachable!(),
    }
    verified_stmt("ALTER USER u1 UNSET TAG k1, k2, k3");

    let dialects = all_dialects_where(|d| d.supports_boolean_literals());
    dialects.one_statement_parses_to(
        "ALTER USER u1 SET PASSWORD='secret', MUST_CHANGE_PASSWORD=TRUE, MINS_TO_UNLOCK=10",
        "ALTER USER u1 SET PASSWORD='secret', MUST_CHANGE_PASSWORD=true, MINS_TO_UNLOCK=10",
    );

    let stmt = dialects.verified_stmt(
        "ALTER USER u1 SET PASSWORD='secret', MUST_CHANGE_PASSWORD=true, MINS_TO_UNLOCK=10",
    );
    match stmt {
        Statement::AlterUser(alter) => {
            assert_eq!(
                alter.set_props,
                KeyValueOptions {
                    delimiter: KeyValueOptionsDelimiter::Comma,
                    options: vec![
                        KeyValueOption {
                            option_name: "PASSWORD".to_string(),
                            option_value: KeyValueOptionKind::Single(Value::SingleQuotedString(
                                "secret".to_string()
                            )),
                        },
                        KeyValueOption {
                            option_name: "MUST_CHANGE_PASSWORD".to_string(),
                            option_value: KeyValueOptionKind::Single(Value::Boolean(true)),
                        },
                        KeyValueOption {
                            option_name: "MINS_TO_UNLOCK".to_string(),
                            option_value: KeyValueOptionKind::Single(number("10")),
                        },
                    ]
                }
            );
        }
        _ => unreachable!(),
    }

    let stmt = verified_stmt("ALTER USER u1 UNSET PASSWORD");
    match stmt {
        Statement::AlterUser(alter) => {
            assert_eq!(alter.unset_props, vec!["PASSWORD".to_string()]);
        }
        _ => unreachable!(),
    }
    verified_stmt("ALTER USER u1 UNSET PASSWORD, MUST_CHANGE_PASSWORD, MINS_TO_UNLOCK");

    let stmt = verified_stmt("ALTER USER u1 SET DEFAULT_SECONDARY_ROLES=('ALL')");
    match stmt {
        Statement::AlterUser(alter) => {
            assert_eq!(
                alter.set_props.options,
                vec![KeyValueOption {
                    option_name: "DEFAULT_SECONDARY_ROLES".to_string(),
                    option_value: KeyValueOptionKind::Multi(vec![Value::SingleQuotedString(
                        "ALL".to_string()
                    )])
                }]
            );
        }
        _ => unreachable!(),
    }
    verified_stmt("ALTER USER u1 SET DEFAULT_SECONDARY_ROLES=()");
    verified_stmt("ALTER USER u1 SET DEFAULT_SECONDARY_ROLES=('R1', 'R2', 'R3')");
    verified_stmt("ALTER USER u1 SET PASSWORD='secret', DEFAULT_SECONDARY_ROLES=('ALL')");
    verified_stmt("ALTER USER u1 SET DEFAULT_SECONDARY_ROLES=('ALL'), PASSWORD='secret'");
    let stmt = verified_stmt(
        "ALTER USER u1 SET WORKLOAD_IDENTITY=(TYPE=AWS, ARN='arn:aws:iam::123456789:r1/')",
    );
    match stmt {
        Statement::AlterUser(alter) => {
            assert_eq!(
                alter.set_props.options,
                vec![KeyValueOption {
                    option_name: "WORKLOAD_IDENTITY".to_string(),
                    option_value: KeyValueOptionKind::KeyValueOptions(Box::new(KeyValueOptions {
                        delimiter: KeyValueOptionsDelimiter::Comma,
                        options: vec![
                            KeyValueOption {
                                option_name: "TYPE".to_string(),
                                option_value: KeyValueOptionKind::Single(Value::Placeholder(
                                    "AWS".to_string()
                                )),
                            },
                            KeyValueOption {
                                option_name: "ARN".to_string(),
                                option_value: KeyValueOptionKind::Single(
                                    Value::SingleQuotedString(
                                        "arn:aws:iam::123456789:r1/".to_string()
                                    )
                                ),
                            },
                        ]
                    }))
                }]
            )
        }
        _ => unreachable!(),
    }
    verified_stmt("ALTER USER u1 SET DEFAULT_SECONDARY_ROLES=('ALL'), PASSWORD='secret', WORKLOAD_IDENTITY=(TYPE=AWS, ARN='arn:aws:iam::123456789:r1/')");
}

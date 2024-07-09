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
//! Test SQL syntax, which all sqlparser dialects must parse in the same way.
//!
//! Note that it does not mean all SQL here is valid in all the dialects, only
//! that 1) it's either standard or widely supported and 2) it can be parsed by
//! sqlparser regardless of the chosen dialect (i.e. it doesn't conflict with
//! dialect-specific parsing rules).

extern crate core;

use matches::assert_matches;
use sqlparser::ast::SelectItem::UnnamedExpr;
use sqlparser::ast::TableFactor::{Pivot, Unpivot};
use sqlparser::ast::*;
use sqlparser::dialect::{
    AnsiDialect, BigQueryDialect, ClickHouseDialect, DatabricksDialect, Dialect, DuckDbDialect,
    GenericDialect, HiveDialect, MsSqlDialect, MySqlDialect, PostgreSqlDialect, RedshiftSqlDialect,
    SQLiteDialect, SnowflakeDialect,
};
use sqlparser::keywords::ALL_KEYWORDS;
use sqlparser::parser::{Parser, ParserError, ParserOptions};
use sqlparser::tokenizer::Tokenizer;
use test_utils::{
    all_dialects, all_dialects_where, alter_table_op, assert_eq_vec, call, expr_from_projection,
    join, number, only, table, table_alias, TestedDialects,
};

#[macro_use]
mod test_utils;

#[cfg(test)]
use pretty_assertions::assert_eq;
use sqlparser::test_utils::all_dialects_except;

#[test]
fn parse_insert_values() {
    let row = vec![
        Expr::Value(number("1")),
        Expr::Value(number("2")),
        Expr::Value(number("3")),
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
                table_name,
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
            table_name,
            ..
        }) => {
            assert_eq!(columns, vec![]);
            assert_eq!(after_columns, vec![]);
            assert_eq!(on, None);
            assert_eq!(partitioned, None);
            assert_eq!(returning, None);
            assert_eq!(source, None);
            assert_eq!(table_name, ObjectName(vec!["test_table".into()]));
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
            table_name,
            ..
        }) => {
            assert_eq!(after_columns, vec![]);
            assert_eq!(columns, vec![]);
            assert_eq!(on, None);
            assert_eq!(partitioned, None);
            assert!(returning.is_some());
            assert_eq!(source, None);
            assert_eq!(table_name, ObjectName(vec!["test_table".into()]));
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
            table_name,
            ..
        }) => {
            assert_eq!(after_columns, vec![]);
            assert_eq!(columns, vec![]);
            assert!(on.is_some());
            assert_eq!(partitioned, None);
            assert_eq!(returning, None);
            assert_eq!(source, None);
            assert_eq!(table_name, ObjectName(vec!["test_table".into()]));
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
    verified_stmt("INSERT INTO t SELECT 1 RETURNING 2");
    let stmt = verified_stmt("INSERT INTO t SELECT x RETURNING x AS y");
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
        Statement::Update {
            table,
            assignments,
            selection,
            ..
        } => {
            assert_eq!(table.to_string(), "t".to_string());
            assert_eq!(
                assignments,
                vec![
                    Assignment {
                        target: AssignmentTarget::ColumnName(ObjectName(vec!["a".into()])),
                        value: Expr::Value(number("1")),
                    },
                    Assignment {
                        target: AssignmentTarget::ColumnName(ObjectName(vec!["b".into()])),
                        value: Expr::Value(number("2")),
                    },
                    Assignment {
                        target: AssignmentTarget::ColumnName(ObjectName(vec!["c".into()])),
                        value: Expr::Value(number("3")),
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
    let dialects = TestedDialects {
        dialects: vec![
            Box::new(GenericDialect {}),
            Box::new(DuckDbDialect {}),
            Box::new(PostgreSqlDialect {}),
            Box::new(BigQueryDialect {}),
            Box::new(SnowflakeDialect {}),
            Box::new(RedshiftSqlDialect {}),
            Box::new(MsSqlDialect {}),
            Box::new(SQLiteDialect {}),
        ],
        options: None,
    };
    let stmt = dialects.verified_stmt(sql);
    assert_eq!(
        stmt,
        Statement::Update {
            table: TableWithJoins {
                relation: TableFactor::Table {
                    name: ObjectName(vec![Ident::new("t1")]),
                    alias: None,
                    args: None,
                    with_hints: vec![],
                    version: None,
                    partitions: vec![],
                    with_ordinality: false,
                },
                joins: vec![],
            },
            assignments: vec![Assignment {
                target: AssignmentTarget::ColumnName(ObjectName(vec![Ident::new("name")])),
                value: Expr::CompoundIdentifier(vec![Ident::new("t2"), Ident::new("name")])
            }],
            from: Some(TableWithJoins {
                relation: TableFactor::Derived {
                    lateral: false,
                    subquery: Box::new(Query {
                        with: None,
                        body: Box::new(SetExpr::Select(Box::new(Select {
                            distinct: None,
                            top: None,
                            projection: vec![
                                SelectItem::UnnamedExpr(Expr::Identifier(Ident::new("name"))),
                                SelectItem::UnnamedExpr(Expr::Identifier(Ident::new("id"))),
                            ],
                            into: None,
                            from: vec![TableWithJoins {
                                relation: TableFactor::Table {
                                    name: ObjectName(vec![Ident::new("t1")]),
                                    alias: None,
                                    args: None,
                                    with_hints: vec![],
                                    version: None,
                                    partitions: vec![],
                                    with_ordinality: false,
                                },
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
                        }))),
                        order_by: vec![],
                        limit: None,
                        limit_by: vec![],
                        offset: None,
                        fetch: None,
                        locks: vec![],
                        for_clause: None,
                        settings: None,
                        format_clause: None,
                    }),
                    alias: Some(TableAlias {
                        name: Ident::new("t2"),
                        columns: vec![],
                    })
                },
                joins: vec![],
            }),
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
        }
    );
}

#[test]
fn parse_update_with_table_alias() {
    let sql = "UPDATE users AS u SET u.username = 'new_user' WHERE u.username = 'old_user'";
    match verified_stmt(sql) {
        Statement::Update {
            table,
            assignments,
            from: _from,
            selection,
            returning,
        } => {
            assert_eq!(
                TableWithJoins {
                    relation: TableFactor::Table {
                        name: ObjectName(vec![Ident::new("users")]),
                        alias: Some(TableAlias {
                            name: Ident::new("u"),
                            columns: vec![],
                        }),
                        args: None,
                        with_hints: vec![],
                        version: None,
                        partitions: vec![],
                        with_ordinality: false,
                    },
                    joins: vec![],
                },
                table
            );
            assert_eq!(
                vec![Assignment {
                    target: AssignmentTarget::ColumnName(ObjectName(vec![
                        Ident::new("u"),
                        Ident::new("username")
                    ])),
                    value: Expr::Value(Value::SingleQuotedString("new_user".to_string())),
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
                    right: Box::new(Expr::Value(Value::SingleQuotedString(
                        "old_user".to_string()
                    ))),
                }),
                selection
            );
            assert_eq!(None, returning);
        }
        _ => unreachable!(),
    }
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
                name: ObjectName(vec![Ident::new("lineitem")]),
                alias: Some(TableAlias {
                    name: Ident::new("l"),
                    columns: vec![Ident::new("A"), Ident::new("B"), Ident::new("C"),],
                }),
                args: None,
                with_hints: vec![],
                version: None,
                partitions: vec![],
                with_ordinality: false,
            },
            joins: vec![],
        }]
    );
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
                TableFactor::Table {
                    name: ObjectName(vec![Ident::with_quote('"', "table")]),
                    alias: None,
                    args: None,
                    with_hints: vec![],
                    version: None,
                    partitions: vec![],
                    with_ordinality: false,
                },
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
                ObjectName(vec![Ident::new("schema1"), Ident::new("table1")]),
                tables[0]
            );
            assert_eq!(
                ObjectName(vec![Ident::new("schema2"), Ident::new("table2")]),
                tables[1]
            );
            assert_eq!(
                TableFactor::Table {
                    name: ObjectName(vec![Ident::new("schema1"), Ident::new("table1")]),
                    alias: None,
                    args: None,
                    with_hints: vec![],
                    version: None,
                    partitions: vec![],
                    with_ordinality: false,
                },
                from[0].relation
            );
            assert_eq!(
                TableFactor::Table {
                    name: ObjectName(vec![Ident::new("schema2"), Ident::new("table2")]),
                    alias: None,
                    args: None,
                    with_hints: vec![],
                    version: None,
                    partitions: vec![],
                    with_ordinality: false,
                },
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
                TableFactor::Table {
                    name: ObjectName(vec![Ident::new("schema1"), Ident::new("table1")]),
                    alias: None,
                    args: None,
                    with_hints: vec![],
                    version: None,
                    partitions: vec![],
                    with_ordinality: false,
                },
                from[0].relation
            );
            assert_eq!(
                TableFactor::Table {
                    name: ObjectName(vec![Ident::new("schema2"), Ident::new("table2")]),
                    alias: None,
                    args: None,
                    with_hints: vec![],
                    version: None,
                    partitions: vec![],
                    with_ordinality: false,
                },
                from[1].relation
            );
            assert_eq!(
                TableFactor::Table {
                    name: ObjectName(vec![Ident::new("schema1"), Ident::new("table1")]),
                    alias: None,
                    args: None,
                    with_hints: vec![],
                    version: None,
                    partitions: vec![],
                    with_ordinality: false,
                },
                using[0].relation
            );
            assert_eq!(
                TableFactor::Table {
                    name: ObjectName(vec![Ident::new("schema2"), Ident::new("table2")]),
                    alias: None,
                    args: None,
                    with_hints: vec![],
                    version: None,
                    partitions: vec![],
                    with_ordinality: false,
                },
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
                TableFactor::Table {
                    name: ObjectName(vec![Ident::new("foo")]),
                    alias: None,
                    args: None,
                    with_hints: vec![],
                    version: None,
                    partitions: vec![],
                    with_ordinality: false,
                },
                from[0].relation,
            );

            assert_eq!(None, using);
            assert_eq!(
                Expr::BinaryOp {
                    left: Box::new(Expr::Identifier(Ident::new("name"))),
                    op: Eq,
                    right: Box::new(Expr::Value(number("5"))),
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
                    name: ObjectName(vec![Ident::new("basket")]),
                    alias: Some(TableAlias {
                        name: Ident::new("a"),
                        columns: vec![],
                    }),
                    args: None,
                    with_hints: vec![],
                    version: None,
                    partitions: vec![],
                    with_ordinality: false,
                },
                from[0].relation,
            );
            assert_eq!(
                Some(vec![TableWithJoins {
                    relation: TableFactor::Table {
                        name: ObjectName(vec![Ident::new("basket")]),
                        alias: Some(TableAlias {
                            name: Ident::new("b"),
                            columns: vec![],
                        }),
                        args: None,
                        with_hints: vec![],
                        version: None,
                        partitions: vec![],
                        with_ordinality: false,
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
    verified_stmt("VALUES ROW(1, true, 'a'), ROW(2, false, 'b')");
}

#[test]
fn parse_simple_select() {
    let sql = "SELECT id, fname, lname FROM customer WHERE id = 1 LIMIT 5";
    let select = verified_only_select(sql);
    assert!(select.distinct.is_none());
    assert_eq!(3, select.projection.len());
    let select = verified_query(sql);
    assert_eq!(Some(Expr::Value(number("5"))), select.limit);
}

#[test]
fn parse_limit() {
    verified_stmt("SELECT * FROM user LIMIT 1");
}

#[test]
fn parse_limit_is_not_an_alias() {
    // In dialects supporting LIMIT it shouldn't be parsed as a table alias
    let ast = verified_query("SELECT id FROM customer LIMIT 1");
    assert_eq!(Some(Expr::Value(number("1"))), ast.limit);

    let ast = verified_query("SELECT 1 LIMIT 5");
    assert_eq!(Some(Expr::Value(number("5"))), ast.limit);
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
            name: ObjectName(vec![Ident::new("table0")]),
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
            ObjectName(vec![Ident::new("foo")]),
            WildcardAdditionalOptions::default()
        ),
        only(&select.projection)
    );

    let sql = "SELECT myschema.mytable.* FROM myschema.mytable";
    let select = verified_only_select(sql);
    assert_eq!(
        &SelectItem::QualifiedWildcard(
            ObjectName(vec![Ident::new("myschema"), Ident::new("mytable"),]),
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
        assert_eq!(&Expr::Value(number("1")), right.as_ref());
        assert_eq!(&Ident::new("newname"), alias);
    } else {
        panic!("Expected: ExprWithAlias")
    }

    // alias without AS is parsed correctly:
    one_statement_parses_to("SELECT a.col + 1 newname FROM foo AS a", sql);
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
    let dialects = TestedDialects {
        dialects: vec![Box::new(ClickHouseDialect {})],
        options: None,
    };

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
            name: ObjectName(vec![Ident::new("COUNT")]),
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
            name: ObjectName(vec![Ident::new("COUNT")]),
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
        &Expr::Value(Value::Null),
        expr_from_projection(only(&select.projection)),
    );
}

#[test]
fn parse_exponent_in_select() -> Result<(), ParserError> {
    // all except Hive, as it allows numbers to start an identifier
    let dialects = TestedDialects {
        dialects: vec![
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
        ],
        options: None,
    };
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
            SelectItem::UnnamedExpr(Expr::Value(number("10e-20"))),
            SelectItem::UnnamedExpr(Expr::Value(number("1e3"))),
            SelectItem::UnnamedExpr(Expr::Value(number("1e+3"))),
            SelectItem::ExprWithAlias {
                expr: Expr::Value(number("1e3")),
                alias: Ident::new("a")
            },
            SelectItem::ExprWithAlias {
                expr: Expr::Value(number("1")),
                alias: Ident::new("e")
            },
            SelectItem::UnnamedExpr(Expr::Value(number("0.5e2"))),
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
            right: Box::new(Expr::Value(Value::SingleQuotedString(
                "Jim's salary".to_string()
            ))),
        }),
        ast.selection,
    );
}

#[test]
fn parse_escaped_single_quote_string_predicate_with_no_escape() {
    use self::BinaryOperator::*;
    let sql = "SELECT id, fname, lname FROM customer \
               WHERE salary <> 'Jim''s salary'";

    let ast = TestedDialects {
        dialects: vec![Box::new(MySqlDialect {})],
        options: Some(
            ParserOptions::new()
                .with_trailing_commas(true)
                .with_unescape(false),
        ),
    }
    .verified_only_select(sql);

    assert_eq!(
        Some(Expr::BinaryOp {
            left: Box::new(Expr::Identifier(Ident::new("salary"))),
            op: NotEq,
            right: Box::new(Expr::Value(Value::SingleQuotedString(
                "Jim''s salary".to_string()
            ))),
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
        Expr::Value(Value::Number(bigdecimal::BigDecimal::from(1), false))
    );

    #[cfg(not(feature = "bigdecimal"))]
    assert_eq!(expr, Expr::Value(Value::Number("1.0".into(), false)));
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
    TestedDialects {
        dialects: vec![Box::new(PostgreSqlDialect {}), Box::new(GenericDialect {})],
        options: None,
    }
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
    let sql = "NOT true OR true";
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
                expr: Box::new(Expr::Value(number("1"))),
                low: Box::new(Expr::Value(number("1"))),
                high: Box::new(Expr::Value(number("2"))),
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
                expr: Box::new(Expr::Value(Value::SingleQuotedString("a".into()))),
                negated: true,
                pattern: Box::new(Expr::Value(Value::SingleQuotedString("b".into()))),
                escape_char: None,
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
                list: vec![Expr::Value(Value::SingleQuotedString("a".into()))],
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
                negated: false,
                pattern: Box::new(Expr::Value(Value::Null)),
                escape_char: None,
            },
            alias: Ident {
                value: "col_null".to_owned(),
                quote_style: None,
            },
        },
        select.projection[0]
    );
    assert_eq!(
        SelectItem::ExprWithAlias {
            expr: Expr::Like {
                expr: Box::new(Expr::Value(Value::Null)),
                negated: false,
                pattern: Box::new(Expr::Identifier(Ident::new("column1"))),
                escape_char: None,
            },
            alias: Ident {
                value: "null_col".to_owned(),
                quote_style: None,
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
                pattern: Box::new(Expr::Value(Value::SingleQuotedString("%a".to_string()))),
                escape_char: None,
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
                pattern: Box::new(Expr::Value(Value::SingleQuotedString("%a".to_string()))),
                escape_char: Some('^'.to_string()),
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
                pattern: Box::new(Expr::Value(Value::SingleQuotedString("%a".to_string()))),
                escape_char: None,
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
                pattern: Box::new(Expr::Value(Value::SingleQuotedString("%a".to_string()))),
                escape_char: Some('^'.to_string()),
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
        let select = verified_only_select(sql);
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
            "SELECT * FROM customers WHERE name {}SIMILAR TO '%a' ESCAPE '^'",
            if negated { "NOT " } else { "" }
        );
        let select = verified_only_select(sql);
        assert_eq!(
            Expr::SimilarTo {
                expr: Box::new(Expr::Identifier(Ident::new("name"))),
                negated,
                pattern: Box::new(Expr::Value(Value::SingleQuotedString("%a".to_string()))),
                escape_char: Some('^'.to_string()),
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
                pattern: Box::new(Expr::Value(Value::SingleQuotedString("%a".to_string()))),
                escape_char: Some('^'.to_string()),
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
                    Expr::Value(Value::SingleQuotedString("HIGH".to_string())),
                    Expr::Value(Value::SingleQuotedString("MED".to_string())),
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
    TestedDialects {
        dialects: all_dialects()
            .dialects
            .into_iter()
            .filter(|x| !x.is::<PostgreSqlDialect>())
            .collect(),
        options: None,
    }
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
fn parse_logical_xor() {
    let sql = "SELECT true XOR true, false XOR false, true XOR false, false XOR true";
    let select = verified_only_select(sql);
    assert_eq!(
        SelectItem::UnnamedExpr(Expr::BinaryOp {
            left: Box::new(Expr::Value(Value::Boolean(true))),
            op: BinaryOperator::Xor,
            right: Box::new(Expr::Value(Value::Boolean(true))),
        }),
        select.projection[0]
    );
    assert_eq!(
        SelectItem::UnnamedExpr(Expr::BinaryOp {
            left: Box::new(Expr::Value(Value::Boolean(false))),
            op: BinaryOperator::Xor,
            right: Box::new(Expr::Value(Value::Boolean(false))),
        }),
        select.projection[1]
    );
    assert_eq!(
        SelectItem::UnnamedExpr(Expr::BinaryOp {
            left: Box::new(Expr::Value(Value::Boolean(true))),
            op: BinaryOperator::Xor,
            right: Box::new(Expr::Value(Value::Boolean(false))),
        }),
        select.projection[2]
    );
    assert_eq!(
        SelectItem::UnnamedExpr(Expr::BinaryOp {
            left: Box::new(Expr::Value(Value::Boolean(false))),
            op: BinaryOperator::Xor,
            right: Box::new(Expr::Value(Value::Boolean(true))),
        }),
        select.projection[3]
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
                low: Box::new(Expr::Value(number("25"))),
                high: Box::new(Expr::Value(number("32"))),
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
            expr: Box::new(Expr::Value(number("1"))),
            low: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Value(number("1"))),
                op: Plus,
                right: Box::new(Expr::Value(number("2"))),
            }),
            high: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Value(number("3"))),
                op: Plus,
                right: Box::new(Expr::Value(number("4"))),
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
                left: Box::new(Expr::Value(number("1"))),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::Value(number("1"))),
            }),
            op: BinaryOperator::And,
            right: Box::new(Expr::Between {
                expr: Box::new(Expr::BinaryOp {
                    left: Box::new(Expr::Value(number("1"))),
                    op: BinaryOperator::Plus,
                    right: Box::new(Expr::Identifier(Ident::new("x"))),
                }),
                low: Box::new(Expr::Value(number("1"))),
                high: Box::new(Expr::Value(number("2"))),
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
                Expr::Value(number("1")),
                Expr::Value(number("2")),
            ])),
            SelectItem::UnnamedExpr(Expr::Nested(Box::new(Expr::Value(number("1"))))),
            SelectItem::UnnamedExpr(Expr::Tuple(vec![
                Expr::Value(Value::SingleQuotedString("foo".into())),
                Expr::Value(number("3")),
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
        ParserError::ParserError("Expected: an expression:, found: )".to_string()),
        res.unwrap_err()
    );
}

#[test]
fn parse_select_order_by() {
    fn chk(sql: &str) {
        let select = verified_query(sql);
        assert_eq!(
            vec![
                OrderByExpr {
                    expr: Expr::Identifier(Ident::new("lname")),
                    asc: Some(true),
                    nulls_first: None,
                },
                OrderByExpr {
                    expr: Expr::Identifier(Ident::new("fname")),
                    asc: Some(false),
                    nulls_first: None,
                },
                OrderByExpr {
                    expr: Expr::Identifier(Ident::new("id")),
                    asc: None,
                    nulls_first: None,
                },
            ],
            select.order_by
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
        vec![
            OrderByExpr {
                expr: Expr::Identifier(Ident::new("lname")),
                asc: Some(true),
                nulls_first: None,
            },
            OrderByExpr {
                expr: Expr::Identifier(Ident::new("fname")),
                asc: Some(false),
                nulls_first: None,
            },
        ],
        select.order_by
    );
    assert_eq!(Some(Expr::Value(number("2"))), select.limit);
}

#[test]
fn parse_select_order_by_nulls_order() {
    let sql = "SELECT id, fname, lname FROM customer WHERE id < 5 \
               ORDER BY lname ASC NULLS FIRST, fname DESC NULLS LAST LIMIT 2";
    let select = verified_query(sql);
    assert_eq!(
        vec![
            OrderByExpr {
                expr: Expr::Identifier(Ident::new("lname")),
                asc: Some(true),
                nulls_first: Some(true),
            },
            OrderByExpr {
                expr: Expr::Identifier(Ident::new("fname")),
                asc: Some(false),
                nulls_first: Some(false),
            },
        ],
        select.order_by
    );
    assert_eq!(Some(Expr::Value(number("2"))), select.limit);
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
fn parse_select_having() {
    let sql = "SELECT foo FROM bar GROUP BY foo HAVING COUNT(*) > 1";
    let select = verified_only_select(sql);
    assert_eq!(
        Some(Expr::BinaryOp {
            left: Box::new(Expr::Function(Function {
                name: ObjectName(vec![Ident::new("COUNT")]),
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
            right: Box::new(Expr::Value(number("1"))),
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
                name: ObjectName(vec![Ident::new("ROW_NUMBER")]),
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
                        asc: None,
                        nulls_first: None,
                    }],
                    window_frame: None,
                })),
                within_group: vec![]
            })),
            op: BinaryOperator::Eq,
            right: Box::new(Expr::Value(number("1"))),
        }),
        select.qualify
    );

    let sql = "SELECT i, p, o, ROW_NUMBER() OVER (PARTITION BY p ORDER BY o) AS row_num FROM qt QUALIFY row_num = 1";
    let select = verified_only_select(sql);
    assert_eq!(
        Some(Expr::BinaryOp {
            left: Box::new(Expr::Identifier(Ident::new("row_num"))),
            op: BinaryOperator::Eq,
            right: Box::new(Expr::Value(number("1"))),
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
            data_type: DataType::Varbinary(Some(50)),
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

    let dialects = all_dialects_except(|d| d.is::<SnowflakeDialect>() || d.is::<GenericDialect>());
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
fn parse_ceil_datetime() {
    let sql = "SELECT CEIL(d TO DAY)";
    let select = verified_only_select(sql);
    assert_eq!(
        &Expr::Ceil {
            expr: Box::new(Expr::Identifier(Ident::new("d"))),
            field: DateTimeField::Day,
        },
        expr_from_projection(only(&select.projection)),
    );

    one_statement_parses_to("SELECT CEIL(d to day)", "SELECT CEIL(d TO DAY)");

    verified_stmt("SELECT CEIL(d TO HOUR) FROM df");
    verified_stmt("SELECT CEIL(d TO MINUTE) FROM df");
    verified_stmt("SELECT CEIL(d TO SECOND) FROM df");
    verified_stmt("SELECT CEIL(d TO MILLISECOND) FROM df");

    let dialects = all_dialects_except(|d| d.is::<SnowflakeDialect>() || d.is::<GenericDialect>());
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
            field: DateTimeField::Day,
        },
        expr_from_projection(only(&select.projection)),
    );

    one_statement_parses_to("SELECT FLOOR(d to day)", "SELECT FLOOR(d TO DAY)");

    verified_stmt("SELECT FLOOR(d TO HOUR) FROM df");
    verified_stmt("SELECT FLOOR(d TO MINUTE) FROM df");
    verified_stmt("SELECT FLOOR(d TO SECOND) FROM df");
    verified_stmt("SELECT FLOOR(d TO MILLISECOND) FROM df");

    let dialects = all_dialects_except(|d| d.is::<SnowflakeDialect>() || d.is::<GenericDialect>());
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
            name: ObjectName(vec![Ident::new("LISTAGG")]),
            parameters: FunctionArguments::None,
            args: FunctionArguments::List(FunctionArgumentList {
                duplicate_treatment: Some(DuplicateTreatment::Distinct),
                args: vec![
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Identifier(Ident::new(
                        "dateid"
                    )))),
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
                        Value::SingleQuotedString(", ".to_owned())
                    )))
                ],
                clauses: vec![FunctionArgumentClause::OnOverflow(
                    ListAggOnOverflow::Truncate {
                        filler: Some(Box::new(Expr::Value(Value::SingleQuotedString(
                            "%".to_string(),
                        )))),
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
                    }),
                    asc: None,
                    nulls_first: None,
                },
                OrderByExpr {
                    expr: Expr::Identifier(Ident {
                        value: "username".to_string(),
                        quote_style: None,
                    }),
                    asc: None,
                    nulls_first: None,
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
    let supported_dialects = TestedDialects {
        dialects: vec![
            Box::new(GenericDialect {}),
            Box::new(DuckDbDialect {}),
            Box::new(PostgreSqlDialect {}),
            Box::new(MsSqlDialect {}),
            Box::new(AnsiDialect {}),
            Box::new(HiveDialect {}),
        ],
        options: None,
    };

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
    let supported_dialects = TestedDialects {
        dialects: vec![
            Box::new(GenericDialect {}),
            Box::new(PostgreSqlDialect {}),
            Box::new(MsSqlDialect {}),
            Box::new(AnsiDialect {}),
            Box::new(HiveDialect {}),
        ],
        options: None,
    };

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
    let supported_dialects = TestedDialects {
        dialects: vec![
            Box::new(GenericDialect {}),
            Box::new(PostgreSqlDialect {}),
            Box::new(MsSqlDialect {}),
            Box::new(AnsiDialect {}),
            Box::new(HiveDialect {}),
            Box::new(SnowflakeDialect {}),
        ],
        options: None,
    };

    for sql in [
        "SELECT column1, column2, FIRST_VALUE(column2) OVER (PARTITION BY column1 ORDER BY column2 NULLS LAST) AS column2_first FROM t1",
        "SELECT column1, column2, FIRST_VALUE(column2) OVER (ORDER BY column2 NULLS LAST) AS column2_first FROM t1",
        "SELECT col_1, col_2, LAG(col_2) OVER (ORDER BY col_1) FROM t1",
        "SELECT LAG(col_2, 1, 0) OVER (ORDER BY col_1) FROM t1",
        "SELECT LAG(col_2, 1, 0) OVER (PARTITION BY col_3 ORDER BY col_1)",
    ] {
        supported_dialects.verified_stmt(sql);
    }

    let supported_dialects_nulls = TestedDialects {
        dialects: vec![Box::new(MsSqlDialect {}), Box::new(SnowflakeDialect {})],
        options: None,
    };

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
        assert_eq!(ObjectName(vec![Ident::new("FIRST_VALUE")]), actual.name);
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
            with_options,
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
                        collation: None,
                        options: vec![ColumnOptionDef {
                            name: None,
                            option: ColumnOption::NotNull,
                        }],
                    },
                    ColumnDef {
                        name: "lat".into(),
                        data_type: DataType::Double,
                        collation: None,
                        options: vec![ColumnOptionDef {
                            name: None,
                            option: ColumnOption::Null,
                        }],
                    },
                    ColumnDef {
                        name: "lng".into(),
                        data_type: DataType::Double,
                        collation: None,
                        options: vec![],
                    },
                    ColumnDef {
                        name: "constrained".into(),
                        data_type: DataType::Int(None),
                        collation: None,
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
                        collation: None,
                        options: vec![ColumnOptionDef {
                            name: None,
                            option: ColumnOption::ForeignKey {
                                foreign_table: ObjectName(vec!["othertable".into()]),
                                referred_columns: vec!["a".into(), "b".into()],
                                on_delete: None,
                                on_update: None,
                                characteristics: None,
                            },
                        }],
                    },
                    ColumnDef {
                        name: "ref2".into(),
                        data_type: DataType::Int(None),
                        collation: None,
                        options: vec![ColumnOptionDef {
                            name: None,
                            option: ColumnOption::ForeignKey {
                                foreign_table: ObjectName(vec!["othertable2".into()]),
                                referred_columns: vec![],
                                on_delete: Some(ReferentialAction::Cascade),
                                on_update: Some(ReferentialAction::NoAction),
                                characteristics: None,
                            },
                        },],
                    },
                ]
            );
            assert_eq!(
                constraints,
                vec![
                    TableConstraint::ForeignKey {
                        name: Some("fkey".into()),
                        columns: vec!["lat".into()],
                        foreign_table: ObjectName(vec!["othertable3".into()]),
                        referred_columns: vec!["lat".into()],
                        on_delete: Some(ReferentialAction::Restrict),
                        on_update: None,
                        characteristics: None,
                    },
                    TableConstraint::ForeignKey {
                        name: Some("fkey2".into()),
                        columns: vec!["lat".into()],
                        foreign_table: ObjectName(vec!["othertable4".into()]),
                        referred_columns: vec!["lat".into()],
                        on_delete: Some(ReferentialAction::NoAction),
                        on_update: Some(ReferentialAction::Restrict),
                        characteristics: None,
                    },
                    TableConstraint::ForeignKey {
                        name: None,
                        columns: vec!["lat".into()],
                        foreign_table: ObjectName(vec!["othertable4".into()]),
                        referred_columns: vec!["lat".into()],
                        on_delete: Some(ReferentialAction::Cascade),
                        on_update: Some(ReferentialAction::SetDefault),
                        characteristics: None,
                    },
                    TableConstraint::ForeignKey {
                        name: None,
                        columns: vec!["lng".into()],
                        foreign_table: ObjectName(vec!["othertable4".into()]),
                        referred_columns: vec!["longitude".into()],
                        on_delete: None,
                        on_update: Some(ReferentialAction::SetNull),
                        characteristics: None,
                    },
                ]
            );
            assert_eq!(with_options, vec![]);
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
            with_options,
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
                        collation: None,
                        options: vec![ColumnOptionDef {
                            name: None,
                            option: ColumnOption::NotNull,
                        }],
                    },
                    ColumnDef {
                        name: "lat".into(),
                        data_type: DataType::Double,
                        collation: None,
                        options: vec![ColumnOptionDef {
                            name: None,
                            option: ColumnOption::Null,
                        }],
                    },
                    ColumnDef {
                        name: "lng".into(),
                        data_type: DataType::Double,
                        collation: None,
                        options: vec![],
                    },
                ]
            );
            assert_eq!(
                constraints,
                vec![
                    TableConstraint::ForeignKey {
                        name: Some("fkey".into()),
                        columns: vec!["lat".into()],
                        foreign_table: ObjectName(vec!["othertable3".into()]),
                        referred_columns: vec!["lat".into()],
                        on_delete: Some(ReferentialAction::Restrict),
                        on_update: None,
                        characteristics: Some(ConstraintCharacteristics {
                            deferrable: Some(true),
                            initially: Some(DeferrableInitial::Deferred),
                            enforced: None
                        }),
                    },
                    TableConstraint::ForeignKey {
                        name: Some("fkey2".into()),
                        columns: vec!["lat".into()],
                        foreign_table: ObjectName(vec!["othertable4".into()]),
                        referred_columns: vec!["lat".into()],
                        on_delete: Some(ReferentialAction::NoAction),
                        on_update: Some(ReferentialAction::Restrict),
                        characteristics: Some(ConstraintCharacteristics {
                            deferrable: Some(true),
                            initially: Some(DeferrableInitial::Immediate),
                            enforced: None,
                        }),
                    },
                    TableConstraint::ForeignKey {
                        name: None,
                        columns: vec!["lat".into()],
                        foreign_table: ObjectName(vec!["othertable4".into()]),
                        referred_columns: vec!["lat".into()],
                        on_delete: Some(ReferentialAction::Cascade),
                        on_update: Some(ReferentialAction::SetDefault),
                        characteristics: Some(ConstraintCharacteristics {
                            deferrable: Some(false),
                            initially: Some(DeferrableInitial::Deferred),
                            enforced: Some(false),
                        }),
                    },
                    TableConstraint::ForeignKey {
                        name: None,
                        columns: vec!["lng".into()],
                        foreign_table: ObjectName(vec!["othertable4".into()]),
                        referred_columns: vec!["longitude".into()],
                        on_delete: None,
                        on_update: Some(ReferentialAction::SetNull),
                        characteristics: Some(ConstraintCharacteristics {
                            deferrable: Some(false),
                            initially: Some(DeferrableInitial::Immediate),
                            enforced: Some(true),
                        }),
                    },
                ]
            );
            assert_eq!(with_options, vec![]);
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

        let sql = format!("CREATE TABLE t (a int UNIQUE {})", syntax);
        let expected_clause = if syntax.is_empty() {
            String::new()
        } else {
            format!(" {syntax}")
        };
        let expected = format!("CREATE TABLE t (a INT UNIQUE{})", expected_clause);
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
                        collation: None,
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
        let dialects = TestedDialects {
            dialects,
            options: None,
        };

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
                assert_eq!(name, ObjectName(vec!["something".into()]));
                assert_eq!(
                    columns,
                    vec![
                        ColumnDef {
                            name: Ident::new("name"),
                            data_type: DataType::Int(None),
                            collation: None,
                            options: vec![],
                        },
                        ColumnDef {
                            name: Ident::new("val"),
                            data_type: DataType::Array(expected),
                            collation: None,
                            options: vec![],
                        },
                    ],
                )
            }
            _ => unreachable!(),
        }
    }

    // SnowflakeDialect using array different
    let dialects = TestedDialects {
        dialects: vec![
            Box::new(PostgreSqlDialect {}),
            Box::new(HiveDialect {}),
            Box::new(MySqlDialect {}),
        ],
        options: None,
    };
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
                Expr::Value(Value::SingleQuotedString(s)) => assert_eq!(s, "No rows in my_table"),
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
    let sql = "CREATE TABLE t (a INT, b INT) AS SELECT 1 AS b, 2 AS a";
    match verified_stmt(sql) {
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
        order_by: vec![],
        limit: None,
        limit_by: vec![],
        offset: None,
        fetch: None,
        locks: vec![],
        for_clause: None,
        settings: None,
        format_clause: None,
    });

    match verified_stmt(sql1) {
        Statement::CreateTable(CreateTable { query, name, .. }) => {
            assert_eq!(name, ObjectName(vec![Ident::new("new_table")]));
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
        order_by: vec![],
        limit: None,
        limit_by: vec![],
        offset: None,
        fetch: None,
        locks: vec![],
        for_clause: None,
        settings: None,
        format_clause: None,
    });

    match verified_stmt(sql2) {
        Statement::CreateTable(CreateTable { query, name, .. }) => {
            assert_eq!(name, ObjectName(vec![Ident::new("new_table")]));
            assert_eq!(query.unwrap(), expected_query2);
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_table_on_cluster() {
    let generic = TestedDialects {
        dialects: vec![Box::new(GenericDialect {})],
        options: None,
    };

    // Using single-quote literal to define current cluster
    let sql = "CREATE TABLE t ON CLUSTER '{cluster}' (a INT, b INT)";
    match generic.verified_stmt(sql) {
        Statement::CreateTable(CreateTable { on_cluster, .. }) => {
            assert_eq!(on_cluster.unwrap(), "{cluster}".to_string());
        }
        _ => unreachable!(),
    }

    // Using explicitly declared cluster name
    let sql = "CREATE TABLE t ON CLUSTER my_cluster (a INT, b INT)";
    match generic.verified_stmt(sql) {
        Statement::CreateTable(CreateTable { on_cluster, .. }) => {
            assert_eq!(on_cluster.unwrap(), "my_cluster".to_string());
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

    let sql = "CREATE TABLE t (a INT, b INT) AS SELECT 1 AS b, 2 AS a";
    match verified_stmt(sql) {
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
    let generic = TestedDialects {
        dialects: vec![Box::new(GenericDialect {})],
        options: None,
    };

    let sql = "CREATE TABLE t (c INT) WITH (foo = 'bar', a = 123)";
    match generic.verified_stmt(sql) {
        Statement::CreateTable(CreateTable { with_options, .. }) => {
            assert_eq!(
                vec![
                    SqlOption {
                        name: "foo".into(),
                        value: Expr::Value(Value::SingleQuotedString("bar".into())),
                    },
                    SqlOption {
                        name: "a".into(),
                        value: Expr::Value(number("123")),
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
            assert_eq!(ObjectName(vec![Ident::new("a")]), name);
            assert_eq!(Some(ObjectName(vec![(Ident::new("a_tmp"))])), clone)
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_table_trailing_comma() {
    let dialect = TestedDialects {
        dialects: vec![Box::new(DuckDbDialect {})],
        options: None,
    };

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
            with_options,
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
                        collation: None,
                        options: vec![ColumnOptionDef {
                            name: None,
                            option: ColumnOption::NotNull,
                        }],
                    },
                    ColumnDef {
                        name: "lat".into(),
                        data_type: DataType::Double,
                        collation: None,
                        options: vec![ColumnOptionDef {
                            name: None,
                            option: ColumnOption::Null,
                        }],
                    },
                    ColumnDef {
                        name: "lng".into(),
                        data_type: DataType::Double,
                        collation: None,
                        options: vec![],
                    },
                ]
            );
            assert!(constraints.is_empty());

            assert!(external);
            assert_eq!(FileFormat::TEXTFILE, file_format.unwrap());
            assert_eq!("/tmp/example.csv", location.unwrap());

            assert_eq!(with_options, vec![]);
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
            with_options,
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
                    collation: None,
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

            assert_eq!(with_options, vec![]);
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
            assert_eq!("new_tab", table_name.to_string());
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
                [SqlOption {
                    name: Ident {
                        value: "classification".to_string(),
                        quote_style: Some('\'')
                    },
                    value: Expr::Value(Value::SingleQuotedString("parquet".to_string())),
                }],
            );
        }
        _ => unreachable!(),
    }
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
                    SqlOption {
                        name: "foo".into(),
                        value: Expr::Value(Value::SingleQuotedString("bar".into())),
                    },
                    SqlOption {
                        name: "a".into(),
                        value: Expr::Value(number("123")),
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
    let dialects = TestedDialects {
        dialects: vec![
            Box::new(PostgreSqlDialect {}),
            Box::new(BigQueryDialect {}),
            Box::new(GenericDialect {}),
            Box::new(DuckDbDialect {}),
        ],
        options: None,
    };

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

    fn check_one(constraint_text: &str) {
        match alter_table_op(verified_stmt(&format!(
            "ALTER TABLE tab ADD {constraint_text}"
        ))) {
            AlterTableOperation::AddConstraint(constraint) => {
                assert_eq!(constraint_text, constraint.to_string());
            }
            _ => unreachable!(),
        }
        verified_stmt(&format!("CREATE TABLE foo (id INT, {constraint_text})"));
    }
}

#[test]
fn parse_alter_table_drop_column() {
    check_one("DROP COLUMN IF EXISTS is_active CASCADE");
    one_statement_parses_to(
        "ALTER TABLE tab DROP IF EXISTS is_active CASCADE",
        "ALTER TABLE tab DROP COLUMN IF EXISTS is_active CASCADE",
    );
    one_statement_parses_to(
        "ALTER TABLE tab DROP is_active CASCADE",
        "ALTER TABLE tab DROP COLUMN is_active CASCADE",
    );

    fn check_one(constraint_text: &str) {
        match alter_table_op(verified_stmt(&format!("ALTER TABLE tab {constraint_text}"))) {
            AlterTableOperation::DropColumn {
                column_name,
                if_exists,
                cascade,
            } => {
                assert_eq!("is_active", column_name.to_string());
                assert!(if_exists);
                assert!(cascade);
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
        "{alter_stmt} ALTER COLUMN is_active SET DEFAULT false"
    ))) {
        AlterTableOperation::AlterColumn { column_name, op } => {
            assert_eq!("is_active", column_name.to_string());
            assert_eq!(
                op,
                AlterColumnOperation::SetDefault {
                    value: Expr::Value(Value::Boolean(false))
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
                }
            );
        }
        _ => unreachable!(),
    }

    let dialect = TestedDialects {
        dialects: vec![Box::new(GenericDialect {})],
        options: None,
    };

    let res =
        dialect.parse_sql_statements(&format!("{alter_stmt} ALTER COLUMN is_active TYPE TEXT"));
    assert_eq!(
        ParserError::ParserError("Expected: SET/DROP NOT NULL, SET DEFAULT, or SET DATA TYPE after ALTER COLUMN, found: TYPE".to_string()),
        res.unwrap_err()
    );

    let res = dialect.parse_sql_statements(&format!(
        "{alter_stmt} ALTER COLUMN is_active SET DATA TYPE TEXT USING 'text'"
    ));
    assert_eq!(
        ParserError::ParserError("Expected: end of statement, found: USING".to_string()),
        res.unwrap_err()
    );
}

#[test]
fn parse_alter_table_drop_constraint() {
    let alter_stmt = "ALTER TABLE tab";
    match alter_table_op(verified_stmt(
        "ALTER TABLE tab DROP CONSTRAINT constraint_name CASCADE",
    )) {
        AlterTableOperation::DropConstraint {
            name: constr_name,
            if_exists,
            cascade,
        } => {
            assert_eq!("constraint_name", constr_name.to_string());
            assert!(!if_exists);
            assert!(cascade);
        }
        _ => unreachable!(),
    }
    match alter_table_op(verified_stmt(
        "ALTER TABLE tab DROP CONSTRAINT IF EXISTS constraint_name",
    )) {
        AlterTableOperation::DropConstraint {
            name: constr_name,
            if_exists,
            cascade,
        } => {
            assert_eq!("constraint_name", constr_name.to_string());
            assert!(if_exists);
            assert!(!cascade);
        }
        _ => unreachable!(),
    }

    let res = parse_sql_statements(&format!("{alter_stmt} DROP CONSTRAINT is_active TEXT"));
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
        let sql = dbg!(format!("SELECT {function_name}(id) FROM foo"));
        let select = verified_only_select(&sql);
        assert_eq!(
            &call(function_name, [Expr::Identifier(Ident::new("id"))]),
            expr_from_projection(only(&select.projection))
        );
    }
}

fn run_explain_analyze(
    query: &str,
    expected_verbose: bool,
    expected_analyze: bool,
    expected_format: Option<AnalyzeFormat>,
) {
    match verified_stmt(query) {
        Statement::Explain {
            describe_alias: _,
            analyze,
            verbose,
            statement,
            format,
        } => {
            assert_eq!(verbose, expected_verbose);
            assert_eq!(analyze, expected_analyze);
            assert_eq!(format, expected_format);
            assert_eq!("SELECT sqrt(id) FROM foo", statement.to_string());
        }
        _ => panic!("Unexpected Statement, must be Explain"),
    }
}

#[test]
fn parse_explain_table() {
    let validate_explain =
        |query: &str, expected_describe_alias: DescribeAlias| match verified_stmt(query) {
            Statement::ExplainTable {
                describe_alias,
                hive_format,
                table_name,
            } => {
                assert_eq!(describe_alias, expected_describe_alias);
                assert_eq!(hive_format, None);
                assert_eq!("test_identifier", table_name.to_string());
            }
            _ => panic!("Unexpected Statement, must be ExplainTable"),
        };

    validate_explain("EXPLAIN test_identifier", DescribeAlias::Explain);
    validate_explain("DESCRIBE test_identifier", DescribeAlias::Describe);
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
    run_explain_analyze("DESCRIBE SELECT sqrt(id) FROM foo", false, false, None);

    run_explain_analyze("EXPLAIN SELECT sqrt(id) FROM foo", false, false, None);
    run_explain_analyze(
        "EXPLAIN VERBOSE SELECT sqrt(id) FROM foo",
        true,
        false,
        None,
    );
    run_explain_analyze(
        "EXPLAIN ANALYZE SELECT sqrt(id) FROM foo",
        false,
        true,
        None,
    );
    run_explain_analyze(
        "EXPLAIN ANALYZE VERBOSE SELECT sqrt(id) FROM foo",
        true,
        true,
        None,
    );

    run_explain_analyze(
        "EXPLAIN ANALYZE FORMAT GRAPHVIZ SELECT sqrt(id) FROM foo",
        false,
        true,
        Some(AnalyzeFormat::GRAPHVIZ),
    );

    run_explain_analyze(
        "EXPLAIN ANALYZE VERBOSE FORMAT JSON SELECT sqrt(id) FROM foo",
        true,
        true,
        Some(AnalyzeFormat::JSON),
    );

    run_explain_analyze(
        "EXPLAIN VERBOSE FORMAT TEXT SELECT sqrt(id) FROM foo",
        true,
        false,
        Some(AnalyzeFormat::TEXT),
    );
}

#[test]
fn parse_named_argument_function() {
    let sql = "SELECT FUN(a => '1', b => '2') FROM foo";
    let select = verified_only_select(sql);

    assert_eq!(
        &Expr::Function(Function {
            name: ObjectName(vec![Ident::new("FUN")]),
            parameters: FunctionArguments::None,
            args: FunctionArguments::List(FunctionArgumentList {
                duplicate_treatment: None,
                args: vec![
                    FunctionArg::Named {
                        name: Ident::new("a"),
                        arg: FunctionArgExpr::Expr(Expr::Value(Value::SingleQuotedString(
                            "1".to_owned()
                        ))),
                        operator: FunctionArgOperator::RightArrow
                    },
                    FunctionArg::Named {
                        name: Ident::new("b"),
                        arg: FunctionArgExpr::Expr(Expr::Value(Value::SingleQuotedString(
                            "2".to_owned()
                        ))),
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
            name: ObjectName(vec![Ident::new("FUN")]),
            parameters: FunctionArguments::None,
            args: FunctionArguments::List(FunctionArgumentList {
                duplicate_treatment: None,
                args: vec![
                    FunctionArg::Named {
                        name: Ident::new("a"),
                        arg: FunctionArgExpr::Expr(Expr::Value(Value::SingleQuotedString(
                            "1".to_owned()
                        ))),
                        operator: FunctionArgOperator::Equals
                    },
                    FunctionArg::Named {
                        name: Ident::new("b"),
                        arg: FunctionArgExpr::Expr(Expr::Value(Value::SingleQuotedString(
                            "2".to_owned()
                        ))),
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
                right: Box::new(Expr::Value(number("42"))),
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
    let select = verified_only_select(sql);

    const EXPECTED_PROJ_QTY: usize = 7;
    assert_eq!(EXPECTED_PROJ_QTY, select.projection.len());

    assert_eq!(
        &Expr::Function(Function {
            name: ObjectName(vec![Ident::new("row_number")]),
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
                    asc: Some(false),
                    nulls_first: None,
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
    let supported_dialects = TestedDialects {
        dialects: vec![
            Box::new(GenericDialect {}),
            Box::new(PostgreSqlDialect {}),
            Box::new(MySqlDialect {}),
            Box::new(BigQueryDialect {}),
        ],
        options: None,
    };

    let sql = "SELECT row_number() OVER (w ORDER BY dt DESC), \
               sum(foo) OVER (win PARTITION BY a, b ORDER BY c, d \
               ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) \
               FROM foo \
               WINDOW w AS (PARTITION BY x), win AS (ORDER BY y)";
    supported_dialects.verified_stmt(sql);

    let select = verified_only_select(sql);

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
    verified_only_select(sql);

    let sql = "SELECT * from mytable WINDOW window1 AS window2";
    let dialects = all_dialects_except(|d| d.is::<BigQueryDialect>() || d.is::<GenericDialect>());
    let res = dialects.parse_sql_statements(sql);
    assert_eq!(
        ParserError::ParserError("Expected: (, found: window2".to_string()),
        res.unwrap_err()
    );
}

#[test]
fn test_parse_named_window() {
    let sql = "SELECT \
    MIN(c12) OVER window1 AS min1, \
    MAX(c12) OVER window2 AS max1 \
    FROM aggregate_test_100 \
    WINDOW window1 AS (ORDER BY C12), \
    window2 AS (PARTITION BY C11) \
    ORDER BY C3";
    let actual_select_only = verified_only_select(sql);
    let expected = Select {
        distinct: None,
        top: None,
        projection: vec![
            SelectItem::ExprWithAlias {
                expr: Expr::Function(Function {
                    name: ObjectName(vec![Ident {
                        value: "MIN".to_string(),
                        quote_style: None,
                    }]),
                    parameters: FunctionArguments::None,
                    args: FunctionArguments::List(FunctionArgumentList {
                        duplicate_treatment: None,
                        args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(
                            Expr::Identifier(Ident {
                                value: "c12".to_string(),
                                quote_style: None,
                            }),
                        ))],
                        clauses: vec![],
                    }),
                    null_treatment: None,
                    filter: None,
                    over: Some(WindowType::NamedWindow(Ident {
                        value: "window1".to_string(),
                        quote_style: None,
                    })),
                    within_group: vec![],
                }),
                alias: Ident {
                    value: "min1".to_string(),
                    quote_style: None,
                },
            },
            SelectItem::ExprWithAlias {
                expr: Expr::Function(Function {
                    name: ObjectName(vec![Ident {
                        value: "MAX".to_string(),
                        quote_style: None,
                    }]),
                    parameters: FunctionArguments::None,
                    args: FunctionArguments::List(FunctionArgumentList {
                        duplicate_treatment: None,
                        args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(
                            Expr::Identifier(Ident {
                                value: "c12".to_string(),
                                quote_style: None,
                            }),
                        ))],
                        clauses: vec![],
                    }),
                    null_treatment: None,
                    filter: None,
                    over: Some(WindowType::NamedWindow(Ident {
                        value: "window2".to_string(),
                        quote_style: None,
                    })),
                    within_group: vec![],
                }),
                alias: Ident {
                    value: "max1".to_string(),
                    quote_style: None,
                },
            },
        ],
        into: None,
        from: vec![TableWithJoins {
            relation: TableFactor::Table {
                name: ObjectName(vec![Ident {
                    value: "aggregate_test_100".to_string(),
                    quote_style: None,
                }]),
                alias: None,
                args: None,
                with_hints: vec![],
                version: None,
                partitions: vec![],
                with_ordinality: false,
            },
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
                },
                NamedWindowExpr::WindowSpec(WindowSpec {
                    window_name: None,
                    partition_by: vec![],
                    order_by: vec![OrderByExpr {
                        expr: Expr::Identifier(Ident {
                            value: "C12".to_string(),
                            quote_style: None,
                        }),
                        asc: None,
                        nulls_first: None,
                    }],
                    window_frame: None,
                }),
            ),
            NamedWindowDefinition(
                Ident {
                    value: "window2".to_string(),
                    quote_style: None,
                },
                NamedWindowExpr::WindowSpec(WindowSpec {
                    window_name: None,
                    partition_by: vec![Expr::Identifier(Ident {
                        value: "C11".to_string(),
                        quote_style: None,
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
    };
    assert_eq!(actual_select_only, expected);
}

#[test]
fn parse_window_and_qualify_clause() {
    let sql = "SELECT \
    MIN(c12) OVER window1 AS min1 \
    FROM aggregate_test_100 \
    QUALIFY ROW_NUMBER() OVER my_window \
    WINDOW window1 AS (ORDER BY C12), \
    window2 AS (PARTITION BY C11) \
    ORDER BY C3";
    verified_only_select(sql);

    let sql = "SELECT \
    MIN(c12) OVER window1 AS min1 \
    FROM aggregate_test_100 \
    WINDOW window1 AS (ORDER BY C12), \
    window2 AS (PARTITION BY C11) \
    QUALIFY ROW_NUMBER() OVER my_window \
    ORDER BY C3";
    verified_only_select(sql);
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
fn parse_literal_decimal() {
    // These numbers were explicitly chosen to not roundtrip if represented as
    // f64s (i.e., as 64-bit binary floating point numbers).
    let sql = "SELECT 0.300000000000000004, 9007199254740993.0";
    let select = verified_only_select(sql);
    assert_eq!(2, select.projection.len());
    assert_eq!(
        &Expr::Value(number("0.300000000000000004")),
        expr_from_projection(&select.projection[0]),
    );
    assert_eq!(
        &Expr::Value(number("9007199254740993.0")),
        expr_from_projection(&select.projection[1]),
    )
}

#[test]
fn parse_literal_string() {
    let sql = "SELECT 'one', N'national string', X'deadBEEF'";
    let select = verified_only_select(sql);
    assert_eq!(3, select.projection.len());
    assert_eq!(
        &Expr::Value(Value::SingleQuotedString("one".to_string())),
        expr_from_projection(&select.projection[0])
    );
    assert_eq!(
        &Expr::Value(Value::NationalStringLiteral("national string".to_string())),
        expr_from_projection(&select.projection[1])
    );
    assert_eq!(
        &Expr::Value(Value::HexStringLiteral("deadBEEF".to_string())),
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
        &Expr::TypedString {
            data_type: DataType::Date,
            value: "1999-01-01".into(),
        },
        expr_from_projection(only(&select.projection)),
    );
}

#[test]
fn parse_literal_time() {
    let sql = "SELECT TIME '01:23:34'";
    let select = verified_only_select(sql);
    assert_eq!(
        &Expr::TypedString {
            data_type: DataType::Time(None, TimezoneInfo::None),
            value: "01:23:34".into(),
        },
        expr_from_projection(only(&select.projection)),
    );
}

#[test]
fn parse_literal_datetime() {
    let sql = "SELECT DATETIME '1999-01-01 01:23:34.45'";
    let select = verified_only_select(sql);
    assert_eq!(
        &Expr::TypedString {
            data_type: DataType::Datetime(None),
            value: "1999-01-01 01:23:34.45".into(),
        },
        expr_from_projection(only(&select.projection)),
    );
}

#[test]
fn parse_literal_timestamp_without_time_zone() {
    let sql = "SELECT TIMESTAMP '1999-01-01 01:23:34'";
    let select = verified_only_select(sql);
    assert_eq!(
        &Expr::TypedString {
            data_type: DataType::Timestamp(None, TimezoneInfo::None),
            value: "1999-01-01 01:23:34".into(),
        },
        expr_from_projection(only(&select.projection)),
    );

    one_statement_parses_to("SELECT TIMESTAMP '1999-01-01 01:23:34'", sql);
}

#[test]
fn parse_literal_timestamp_with_time_zone() {
    let sql = "SELECT TIMESTAMPTZ '1999-01-01 01:23:34Z'";
    let select = verified_only_select(sql);
    assert_eq!(
        &Expr::TypedString {
            data_type: DataType::Timestamp(None, TimezoneInfo::Tz),
            value: "1999-01-01 01:23:34Z".into(),
        },
        expr_from_projection(only(&select.projection)),
    );

    one_statement_parses_to("SELECT TIMESTAMPTZ '1999-01-01 01:23:34Z'", sql);
}

#[test]
fn parse_interval() {
    let sql = "SELECT INTERVAL '1-1' YEAR TO MONTH";
    let select = verified_only_select(sql);
    assert_eq!(
        &Expr::Interval(Interval {
            value: Box::new(Expr::Value(Value::SingleQuotedString(String::from("1-1")))),
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
            value: Box::new(Expr::Value(Value::SingleQuotedString(String::from(
                "01:01.01"
            )))),
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
            value: Box::new(Expr::Value(Value::SingleQuotedString(String::from("1")))),
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
            value: Box::new(Expr::Value(Value::SingleQuotedString(String::from("10")))),
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
            value: Box::new(Expr::Value(number("5"))),
            leading_field: Some(DateTimeField::Day),
            leading_precision: None,
            last_field: None,
            fractional_seconds_precision: None,
        }),
        expr_from_projection(only(&select.projection)),
    );

    let sql = "SELECT INTERVAL 1 + 1 DAY";
    let select = verified_only_select(sql);
    assert_eq!(
        &Expr::Interval(Interval {
            value: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Value(number("1"))),
                op: BinaryOperator::Plus,
                right: Box::new(Expr::Value(number("1"))),
            }),
            leading_field: Some(DateTimeField::Day),
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
            value: Box::new(Expr::Value(Value::SingleQuotedString(String::from("10")))),
            leading_field: Some(DateTimeField::Hour),
            leading_precision: Some(1),
            last_field: None,
            fractional_seconds_precision: None,
        }),
        expr_from_projection(only(&select.projection)),
    );

    let sql = "SELECT INTERVAL '1 DAY'";
    let select = verified_only_select(sql);
    assert_eq!(
        &Expr::Interval(Interval {
            value: Box::new(Expr::Value(Value::SingleQuotedString(String::from(
                "1 DAY"
            )))),
            leading_field: None,
            leading_precision: None,
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
    verified_only_select("SELECT INTERVAL '1' DAY");
    verified_only_select("SELECT INTERVAL '1' HOUR");
    verified_only_select("SELECT INTERVAL '1' MINUTE");
    verified_only_select("SELECT INTERVAL '1' SECOND");
    verified_only_select("SELECT INTERVAL '1' YEAR TO MONTH");
    verified_only_select("SELECT INTERVAL '1' DAY TO HOUR");
    verified_only_select("SELECT INTERVAL '1' DAY TO MINUTE");
    verified_only_select("SELECT INTERVAL '1' DAY TO SECOND");
    verified_only_select("SELECT INTERVAL '1' HOUR TO MINUTE");
    verified_only_select("SELECT INTERVAL '1' HOUR TO SECOND");
    verified_only_select("SELECT INTERVAL '1' MINUTE TO SECOND");
    verified_only_select("SELECT INTERVAL '1 YEAR'");
    verified_only_select("SELECT INTERVAL '1 YEAR' AS one_year");
    one_statement_parses_to(
        "SELECT INTERVAL '1 YEAR' one_year",
        "SELECT INTERVAL '1 YEAR' AS one_year",
    );
}

#[test]
fn parse_interval_and_or_xor() {
    let sql = "SELECT col FROM test \
        WHERE d3_date > d1_date + INTERVAL '5 days' \
        AND d2_date > d1_date + INTERVAL '3 days'";

    let actual_ast = Parser::parse_sql(&GenericDialect {}, sql).unwrap();

    let expected_ast = vec![Statement::Query(Box::new(Query {
        with: None,
        body: Box::new(SetExpr::Select(Box::new(Select {
            distinct: None,
            top: None,
            projection: vec![UnnamedExpr(Expr::Identifier(Ident {
                value: "col".to_string(),
                quote_style: None,
            }))],
            into: None,
            from: vec![TableWithJoins {
                relation: TableFactor::Table {
                    name: ObjectName(vec![Ident {
                        value: "test".to_string(),
                        quote_style: None,
                    }]),
                    alias: None,
                    args: None,
                    with_hints: vec![],
                    version: None,
                    partitions: vec![],
                    with_ordinality: false,
                },
                joins: vec![],
            }],
            lateral_views: vec![],
            prewhere: None,
            selection: Some(Expr::BinaryOp {
                left: Box::new(Expr::BinaryOp {
                    left: Box::new(Expr::Identifier(Ident {
                        value: "d3_date".to_string(),
                        quote_style: None,
                    })),
                    op: BinaryOperator::Gt,
                    right: Box::new(Expr::BinaryOp {
                        left: Box::new(Expr::Identifier(Ident {
                            value: "d1_date".to_string(),
                            quote_style: None,
                        })),
                        op: BinaryOperator::Plus,
                        right: Box::new(Expr::Interval(Interval {
                            value: Box::new(Expr::Value(Value::SingleQuotedString(
                                "5 days".to_string(),
                            ))),
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
                    })),
                    op: BinaryOperator::Gt,
                    right: Box::new(Expr::BinaryOp {
                        left: Box::new(Expr::Identifier(Ident {
                            value: "d1_date".to_string(),
                            quote_style: None,
                        })),
                        op: BinaryOperator::Plus,
                        right: Box::new(Expr::Interval(Interval {
                            value: Box::new(Expr::Value(Value::SingleQuotedString(
                                "3 days".to_string(),
                            ))),
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
        }))),
        order_by: vec![],
        limit: None,
        limit_by: vec![],
        offset: None,
        fetch: None,
        locks: vec![],
        for_clause: None,
        settings: None,
        format_clause: None,
    }))];

    assert_eq!(actual_ast, expected_ast);

    verified_stmt(
        "SELECT col FROM test \
        WHERE d3_date > d1_date + INTERVAL '5 days' \
        AND d2_date > d1_date + INTERVAL '3 days'",
    );

    verified_stmt(
        "SELECT col FROM test \
        WHERE d3_date > d1_date + INTERVAL '5 days' \
        OR d2_date > d1_date + INTERVAL '3 days'",
    );

    verified_stmt(
        "SELECT col FROM test \
        WHERE d3_date > d1_date + INTERVAL '5 days' \
        XOR d2_date > d1_date + INTERVAL '3 days'",
    );
}

#[test]
fn parse_at_timezone() {
    let zero = Expr::Value(number("0"));
    let sql = "SELECT FROM_UNIXTIME(0) AT TIME ZONE 'UTC-06:00' FROM t";
    let select = verified_only_select(sql);
    assert_eq!(
        &Expr::AtTimeZone {
            timestamp: Box::new(call("FROM_UNIXTIME", [zero.clone()])),
            time_zone: Box::new(Expr::Value(Value::SingleQuotedString(
                "UTC-06:00".to_string()
            ))),
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
                        time_zone: Box::new(Expr::Value(Value::SingleQuotedString(
                            "UTC-06:00".to_string()
                        ))),
                    },
                    Expr::Value(Value::SingleQuotedString("%Y-%m-%dT%H".to_string()),)
                ]
            ),
            alias: Ident {
                value: "hour".to_string(),
                quote_style: Some('"'),
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
        &Expr::TypedString {
            data_type: DataType::JSON,
            value: r#"{
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
            .into()
        },
        expr_from_projection(only(&select.projection)),
    );
}

#[test]
fn parse_bignumeric_keyword() {
    let sql = r#"SELECT BIGNUMERIC '0'"#;
    let select = verified_only_select(sql);
    assert_eq!(
        &Expr::TypedString {
            data_type: DataType::BigNumeric(ExactNumberInfo::None),
            value: r#"0"#.into()
        },
        expr_from_projection(only(&select.projection)),
    );
    verified_stmt("SELECT BIGNUMERIC '0'");

    let sql = r#"SELECT BIGNUMERIC '123456'"#;
    let select = verified_only_select(sql);
    assert_eq!(
        &Expr::TypedString {
            data_type: DataType::BigNumeric(ExactNumberInfo::None),
            value: r#"123456"#.into()
        },
        expr_from_projection(only(&select.projection)),
    );
    verified_stmt("SELECT BIGNUMERIC '123456'");

    let sql = r#"SELECT BIGNUMERIC '-3.14'"#;
    let select = verified_only_select(sql);
    assert_eq!(
        &Expr::TypedString {
            data_type: DataType::BigNumeric(ExactNumberInfo::None),
            value: r#"-3.14"#.into()
        },
        expr_from_projection(only(&select.projection)),
    );
    verified_stmt("SELECT BIGNUMERIC '-3.14'");

    let sql = r#"SELECT BIGNUMERIC '-0.54321'"#;
    let select = verified_only_select(sql);
    assert_eq!(
        &Expr::TypedString {
            data_type: DataType::BigNumeric(ExactNumberInfo::None),
            value: r#"-0.54321"#.into()
        },
        expr_from_projection(only(&select.projection)),
    );
    verified_stmt("SELECT BIGNUMERIC '-0.54321'");

    let sql = r#"SELECT BIGNUMERIC '1.23456e05'"#;
    let select = verified_only_select(sql);
    assert_eq!(
        &Expr::TypedString {
            data_type: DataType::BigNumeric(ExactNumberInfo::None),
            value: r#"1.23456e05"#.into()
        },
        expr_from_projection(only(&select.projection)),
    );
    verified_stmt("SELECT BIGNUMERIC '1.23456e05'");

    let sql = r#"SELECT BIGNUMERIC '-9.876e-3'"#;
    let select = verified_only_select(sql);
    assert_eq!(
        &Expr::TypedString {
            data_type: DataType::BigNumeric(ExactNumberInfo::None),
            value: r#"-9.876e-3"#.into()
        },
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
                    [Expr::Value(Value::SingleQuotedString("1".to_owned()))],
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
    let dialects = TestedDialects {
        dialects: vec![Box::new(BigQueryDialect {}), Box::new(GenericDialect {})],
        options: None,
    };
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
                        Expr::Value(number("1")),
                        Expr::Value(number("2")),
                        Expr::Value(number("3")),
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
                            Expr::Value(number("1")),
                            Expr::Value(number("2")),
                            Expr::Value(number("3")),
                        ],
                    ),
                    call(
                        "make_array",
                        [Expr::Value(number("5")), Expr::Value(number("6"))],
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
            operand: None,
            conditions: vec![
                IsNull(Box::new(Identifier(Ident::new("bar")))),
                BinaryOp {
                    left: Box::new(Identifier(Ident::new("bar"))),
                    op: Eq,
                    right: Box::new(Expr::Value(number("0"))),
                },
                BinaryOp {
                    left: Box::new(Identifier(Ident::new("bar"))),
                    op: GtEq,
                    right: Box::new(Expr::Value(number("0"))),
                },
            ],
            results: vec![
                Expr::Value(Value::SingleQuotedString("null".to_string())),
                Expr::Value(Value::SingleQuotedString("=0".to_string())),
                Expr::Value(Value::SingleQuotedString(">=0".to_string())),
            ],
            else_result: Some(Box::new(Expr::Value(Value::SingleQuotedString(
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
            operand: Some(Box::new(Identifier(Ident::new("foo")))),
            conditions: vec![Expr::Value(number("1"))],
            results: vec![Expr::Value(Value::SingleQuotedString("Y".to_string()))],
            else_result: Some(Box::new(Expr::Value(Value::SingleQuotedString(
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
                relation: TableFactor::Table {
                    name: ObjectName(vec!["t1".into()]),
                    alias: None,
                    args: None,
                    with_hints: vec![],
                    version: None,
                    partitions: vec![],
                    with_ordinality: false,
                },
                joins: vec![],
            },
            TableWithJoins {
                relation: TableFactor::Table {
                    name: ObjectName(vec!["t2".into()]),
                    alias: None,
                    args: None,
                    with_hints: vec![],
                    version: None,
                    partitions: vec![],
                    with_ordinality: false,
                },
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
                relation: TableFactor::Table {
                    name: ObjectName(vec!["t1a".into()]),
                    alias: None,
                    args: None,
                    with_hints: vec![],
                    version: None,
                    partitions: vec![],
                    with_ordinality: false,
                },
                joins: vec![Join {
                    relation: TableFactor::Table {
                        name: ObjectName(vec!["t1b".into()]),
                        alias: None,
                        args: None,
                        with_hints: vec![],
                        version: None,
                        partitions: vec![],
                        with_ordinality: false,
                    },
                    join_operator: JoinOperator::Inner(JoinConstraint::Natural),
                }],
            },
            TableWithJoins {
                relation: TableFactor::Table {
                    name: ObjectName(vec!["t2a".into()]),
                    alias: None,
                    args: None,
                    with_hints: vec![],
                    version: None,
                    partitions: vec![],
                    with_ordinality: false,
                },
                joins: vec![Join {
                    relation: TableFactor::Table {
                        name: ObjectName(vec!["t2b".into()]),
                        alias: None,
                        args: None,
                        with_hints: vec![],
                        version: None,
                        partitions: vec![],
                        with_ordinality: false,
                    },
                    join_operator: JoinOperator::Inner(JoinConstraint::Natural),
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
            relation: TableFactor::Table {
                name: ObjectName(vec![Ident::new("t2")]),
                alias: None,
                args: None,
                with_hints: vec![],
                version: None,
                partitions: vec![],
                with_ordinality: false,
            },
            join_operator: JoinOperator::CrossJoin,
        },
        only(only(select.from).joins),
    );
}

#[test]
fn parse_joins_on() {
    fn join_with_constraint(
        relation: impl Into<String>,
        alias: Option<TableAlias>,
        f: impl Fn(JoinConstraint) -> JoinOperator,
    ) -> Join {
        Join {
            relation: TableFactor::Table {
                name: ObjectName(vec![Ident::new(relation.into())]),
                alias,
                args: None,
                with_hints: vec![],
                version: None,
                partitions: vec![],
                with_ordinality: false,
            },
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
            JoinOperator::Inner,
        )]
    );
    one_statement_parses_to(
        "SELECT * FROM t1 JOIN t2 foo ON c1 = c2",
        "SELECT * FROM t1 JOIN t2 AS foo ON c1 = c2",
    );
    // Test parsing of different join operators
    assert_eq!(
        only(&verified_only_select("SELECT * FROM t1 JOIN t2 ON c1 = c2").from).joins,
        vec![join_with_constraint("t2", None, JoinOperator::Inner)]
    );
    assert_eq!(
        only(&verified_only_select("SELECT * FROM t1 LEFT JOIN t2 ON c1 = c2").from).joins,
        vec![join_with_constraint("t2", None, JoinOperator::LeftOuter)]
    );
    assert_eq!(
        only(&verified_only_select("SELECT * FROM t1 RIGHT JOIN t2 ON c1 = c2").from).joins,
        vec![join_with_constraint("t2", None, JoinOperator::RightOuter)]
    );
    assert_eq!(
        only(&verified_only_select("SELECT * FROM t1 LEFT SEMI JOIN t2 ON c1 = c2").from).joins,
        vec![join_with_constraint("t2", None, JoinOperator::LeftSemi)]
    );
    assert_eq!(
        only(&verified_only_select("SELECT * FROM t1 RIGHT SEMI JOIN t2 ON c1 = c2").from).joins,
        vec![join_with_constraint("t2", None, JoinOperator::RightSemi)]
    );
    assert_eq!(
        only(&verified_only_select("SELECT * FROM t1 LEFT ANTI JOIN t2 ON c1 = c2").from).joins,
        vec![join_with_constraint("t2", None, JoinOperator::LeftAnti)]
    );
    assert_eq!(
        only(&verified_only_select("SELECT * FROM t1 RIGHT ANTI JOIN t2 ON c1 = c2").from).joins,
        vec![join_with_constraint("t2", None, JoinOperator::RightAnti)]
    );
    assert_eq!(
        only(&verified_only_select("SELECT * FROM t1 FULL JOIN t2 ON c1 = c2").from).joins,
        vec![join_with_constraint("t2", None, JoinOperator::FullOuter)]
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
                name: ObjectName(vec![Ident::new(relation.into())]),
                alias,
                args: None,
                with_hints: vec![],
                version: None,
                partitions: vec![],
                with_ordinality: false,
            },
            join_operator: f(JoinConstraint::Using(vec!["c1".into()])),
        }
    }
    // Test parsing of aliases
    assert_eq!(
        only(&verified_only_select("SELECT * FROM t1 JOIN t2 AS foo USING(c1)").from).joins,
        vec![join_with_constraint(
            "t2",
            table_alias("foo"),
            JoinOperator::Inner,
        )]
    );
    one_statement_parses_to(
        "SELECT * FROM t1 JOIN t2 foo USING(c1)",
        "SELECT * FROM t1 JOIN t2 AS foo USING(c1)",
    );
    // Test parsing of different join operators
    assert_eq!(
        only(&verified_only_select("SELECT * FROM t1 JOIN t2 USING(c1)").from).joins,
        vec![join_with_constraint("t2", None, JoinOperator::Inner)]
    );
    assert_eq!(
        only(&verified_only_select("SELECT * FROM t1 LEFT JOIN t2 USING(c1)").from).joins,
        vec![join_with_constraint("t2", None, JoinOperator::LeftOuter)]
    );
    assert_eq!(
        only(&verified_only_select("SELECT * FROM t1 RIGHT JOIN t2 USING(c1)").from).joins,
        vec![join_with_constraint("t2", None, JoinOperator::RightOuter)]
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
}

#[test]
fn parse_natural_join() {
    fn natural_join(f: impl Fn(JoinConstraint) -> JoinOperator, alias: Option<TableAlias>) -> Join {
        Join {
            relation: TableFactor::Table {
                name: ObjectName(vec![Ident::new("t2")]),
                alias,
                args: None,
                with_hints: vec![],
                version: None,
                partitions: vec![],
                with_ordinality: false,
            },
            join_operator: f(JoinConstraint::Natural),
        }
    }

    // if not specified, inner join as default
    assert_eq!(
        only(&verified_only_select("SELECT * FROM t1 NATURAL JOIN t2").from).joins,
        vec![natural_join(JoinOperator::Inner, None)]
    );
    // left join explicitly
    assert_eq!(
        only(&verified_only_select("SELECT * FROM t1 NATURAL LEFT JOIN t2").from).joins,
        vec![natural_join(JoinOperator::LeftOuter, None)]
    );

    // right join explicitly
    assert_eq!(
        only(&verified_only_select("SELECT * FROM t1 NATURAL RIGHT JOIN t2").from).joins,
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
        vec![natural_join(JoinOperator::Inner, table_alias("t3"))]
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
    one_statement_parses_to(
        "SELECT c1 FROM t1 INNER JOIN t2 USING(c1)",
        "SELECT c1 FROM t1 JOIN t2 USING(c1)",
    );
    one_statement_parses_to(
        "SELECT c1 FROM t1 LEFT OUTER JOIN t2 USING(c1)",
        "SELECT c1 FROM t1 LEFT JOIN t2 USING(c1)",
    );
    one_statement_parses_to(
        "SELECT c1 FROM t1 RIGHT OUTER JOIN t2 USING(c1)",
        "SELECT c1 FROM t1 RIGHT JOIN t2 USING(c1)",
    );
    one_statement_parses_to(
        "SELECT c1 FROM t1 FULL OUTER JOIN t2 USING(c1)",
        "SELECT c1 FROM t1 FULL JOIN t2 USING(c1)",
    );

    let res = parse_sql_statements("SELECT * FROM a OUTER JOIN b ON 1");
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
        Statement::CreateView { query, .. } => assert_ctes_in_select(&cte_sqls, &query),
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
        vec![Ident::new("col1"), Ident::new("col2")],
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
            },
            columns: vec![Ident {
                value: "val".to_string(),
                quote_style: None,
            }],
        },
        query: Box::new(cte_query),
        from: None,
        materialized: None,
    };
    assert_eq!(with.cte_tables.first().unwrap(), &expected);
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
                    relation: TableFactor::Table {
                        name: ObjectName(vec!["t2".into()]),
                        alias: None,
                        args: None,
                        with_hints: vec![],
                        version: None,
                        partitions: vec![],
                        with_ordinality: false,
                    },
                    join_operator: JoinOperator::Inner(JoinConstraint::Natural),
                }],
            }),
            alias: None,
        }
    );
}

#[test]
fn parse_union_except_intersect() {
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
}

#[test]
fn parse_values() {
    verified_stmt("SELECT * FROM (VALUES (1), (2), (3))");
    verified_stmt("SELECT * FROM (VALUES (1), (2), (3)), (VALUES (1, 2, 3))");
    verified_stmt("SELECT * FROM (VALUES (1)) UNION VALUES (1)");
    verified_stmt("SELECT * FROM (VALUES ROW(1, true, 'a'), ROW(2, false, 'b')) AS t (a, b, c)");
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
    test_with("CREATE TABLE foo (baz INT)", "SELECT", " bar");
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
            expr: Box::new(Expr::Value(Value::SingleQuotedString("abcdef".to_string()))),
            overlay_what: Box::new(Expr::Identifier(Ident::new("name"))),
            overlay_from: Box::new(Expr::Value(number("3"))),
            overlay_for: Some(Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Identifier(Ident::new("id"))),
                op: BinaryOperator::Plus,
                right: Box::new(Expr::Value(number("1"))),
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
    let all_expected_snowflake = TestedDialects {
        dialects: vec![
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
            Box::new(DuckDbDialect {}),
        ],
        options: None,
    };
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
        } => {
            assert_eq!("mydb", db_name.to_string());
            assert!(!if_not_exists);
            assert_eq!(None, location);
            assert_eq!(None, managed_location);
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
        } => {
            assert_eq!("mydb", db_name.to_string());
            assert!(if_not_exists);
            assert_eq!(None, location);
            assert_eq!(None, managed_location);
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_view() {
    let sql = "CREATE VIEW myschema.myview AS SELECT foo FROM bar";
    match verified_stmt(sql) {
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
            to,
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
            assert!(!if_not_exists);
            assert!(!temporary);
            assert!(to.is_none())
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_view_with_options() {
    let sql = "CREATE VIEW v WITH (foo = 'bar', a = 123) AS SELECT 1";
    match verified_stmt(sql) {
        Statement::CreateView { options, .. } => {
            assert_eq!(
                CreateTableOptions::With(vec![
                    SqlOption {
                        name: "foo".into(),
                        value: Expr::Value(Value::SingleQuotedString("bar".into())),
                    },
                    SqlOption {
                        name: "a".into(),
                        value: Expr::Value(number("123")),
                    },
                ]),
                options
            );
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_view_with_columns() {
    let sql = "CREATE VIEW v (has, cols) AS SELECT 1, 2";
    match verified_stmt(sql) {
        Statement::CreateView {
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
        } => {
            assert_eq!("v", name.to_string());
            assert_eq!(
                columns,
                vec![Ident::new("has"), Ident::new("cols"),]
                    .into_iter()
                    .map(|name| ViewColumnDef {
                        name,
                        data_type: None,
                        options: None
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
            assert!(to.is_none())
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_view_temporary() {
    let sql = "CREATE TEMPORARY VIEW myschema.myview AS SELECT foo FROM bar";
    match verified_stmt(sql) {
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
            to,
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
            assert!(!if_not_exists);
            assert!(temporary);
            assert!(to.is_none())
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_or_replace_view() {
    let sql = "CREATE OR REPLACE VIEW v AS SELECT 1";
    match verified_stmt(sql) {
        Statement::CreateView {
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
        } => {
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
            assert!(to.is_none())
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
        Statement::CreateView {
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
        } => {
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
            assert!(to.is_none())
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_materialized_view() {
    let sql = "CREATE MATERIALIZED VIEW myschema.myview AS SELECT foo FROM bar";
    match verified_stmt(sql) {
        Statement::CreateView {
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
        } => {
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
            assert!(to.is_none())
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_materialized_view_with_cluster_by() {
    let sql = "CREATE MATERIALIZED VIEW myschema.myview CLUSTER BY (foo) AS SELECT foo FROM bar";
    match verified_stmt(sql) {
        Statement::CreateView {
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
        } => {
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
            assert!(to.is_none())
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
    let expect = Some(Offset {
        value: Expr::Value(number("2")),
        rows: OffsetRows::Rows,
    });
    let ast = verified_query("SELECT foo FROM bar OFFSET 2 ROWS");
    assert_eq!(ast.offset, expect);
    let ast = verified_query("SELECT foo FROM bar WHERE foo = 4 OFFSET 2 ROWS");
    assert_eq!(ast.offset, expect);
    let ast = verified_query("SELECT foo FROM bar ORDER BY baz OFFSET 2 ROWS");
    assert_eq!(ast.offset, expect);
    let ast = verified_query("SELECT foo FROM bar WHERE foo = 4 ORDER BY baz OFFSET 2 ROWS");
    assert_eq!(ast.offset, expect);
    let ast = verified_query("SELECT foo FROM (SELECT * FROM bar OFFSET 2 ROWS) OFFSET 2 ROWS");
    assert_eq!(ast.offset, expect);
    match *ast.body {
        SetExpr::Select(s) => match only(s.from).relation {
            TableFactor::Derived { subquery, .. } => {
                assert_eq!(subquery.offset, expect);
            }
            _ => panic!("Test broke"),
        },
        _ => panic!("Test broke"),
    }
    let ast = verified_query("SELECT 'foo' OFFSET 0 ROWS");
    assert_eq!(
        ast.offset,
        Some(Offset {
            value: Expr::Value(number("0")),
            rows: OffsetRows::Rows,
        })
    );
    let ast = verified_query("SELECT 'foo' OFFSET 1 ROW");
    assert_eq!(
        ast.offset,
        Some(Offset {
            value: Expr::Value(number("1")),
            rows: OffsetRows::Row,
        })
    );
    let ast = verified_query("SELECT 'foo' OFFSET 1");
    assert_eq!(
        ast.offset,
        Some(Offset {
            value: Expr::Value(number("1")),
            rows: OffsetRows::None,
        })
    );
}

#[test]
fn parse_fetch() {
    let fetch_first_two_rows_only = Some(Fetch {
        with_ties: false,
        percent: false,
        quantity: Some(Expr::Value(number("2"))),
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
            quantity: Some(Expr::Value(number("2"))),
        })
    );
    let ast = verified_query("SELECT foo FROM bar FETCH FIRST 50 PERCENT ROWS ONLY");
    assert_eq!(
        ast.fetch,
        Some(Fetch {
            with_ties: false,
            percent: true,
            quantity: Some(Expr::Value(number("50"))),
        })
    );
    let ast = verified_query(
        "SELECT foo FROM bar WHERE foo = 4 ORDER BY baz OFFSET 2 ROWS FETCH FIRST 2 ROWS ONLY",
    );
    assert_eq!(
        ast.offset,
        Some(Offset {
            value: Expr::Value(number("2")),
            rows: OffsetRows::Rows,
        })
    );
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
    assert_eq!(
        ast.offset,
        Some(Offset {
            value: Expr::Value(number("2")),
            rows: OffsetRows::Rows,
        })
    );
    assert_eq!(ast.fetch, fetch_first_two_rows_only);
    match *ast.body {
        SetExpr::Select(s) => match only(s.from).relation {
            TableFactor::Derived { subquery, .. } => {
                assert_eq!(
                    subquery.offset,
                    Some(Offset {
                        value: Expr::Value(number("2")),
                        rows: OffsetRows::Rows,
                    })
                );
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
             (SELECT * FROM order WHERE order.customer = customer.id LIMIT 3) AS order ON true"
        );
        let select = verified_only_select(&sql);
        let from = only(select.from);
        assert_eq!(from.joins.len(), 1);
        let join = &from.joins[0];
        assert_eq!(
            join.join_operator,
            JoinOperator::LeftOuter(JoinConstraint::On(Expr::Value(Value::Boolean(true))))
        );
        if let TableFactor::Derived {
            lateral,
            ref subquery,
            alias: Some(ref alias),
        } = join.relation
        {
            assert_eq!(lateral_in, lateral);
            assert_eq!(Ident::new("order"), alias.name);
            assert_eq!(
                subquery.to_string(),
                "SELECT * FROM order WHERE order.customer = customer.id LIMIT 3"
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
        distinct: None,
        top: None,
        projection: vec![SelectItem::Wildcard(WildcardAdditionalOptions {
            opt_ilike: None,
            opt_exclude: None,
            opt_except: None,
            opt_rename: None,
            opt_replace: None,
        })],
        into: None,
        from: vec![TableWithJoins {
            relation: TableFactor::Table {
                name: ObjectName(vec![Ident {
                    value: "customer".to_string(),
                    quote_style: None,
                }]),
                alias: None,
                args: None,
                with_hints: vec![],
                version: None,
                partitions: vec![],
                with_ordinality: false,
            },
            joins: vec![Join {
                relation: TableFactor::Function {
                    lateral: true,
                    name: ObjectName(vec!["generate_series".into()]),
                    args: vec![
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(number("1")))),
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::CompoundIdentifier(
                            vec![Ident::new("customer"), Ident::new("id")],
                        ))),
                    ],
                    alias: None,
                },
                join_operator: JoinOperator::LeftOuter(JoinConstraint::None),
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
    };
    assert_eq!(actual_select_only, expected);
}

#[test]
fn parse_start_transaction() {
    match verified_stmt("START TRANSACTION READ ONLY, READ WRITE, ISOLATION LEVEL SERIALIZABLE") {
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
    match one_statement_parses_to(
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

    verified_stmt("START TRANSACTION");
    one_statement_parses_to("BEGIN", "BEGIN TRANSACTION");
    one_statement_parses_to("BEGIN WORK", "BEGIN TRANSACTION");
    one_statement_parses_to("BEGIN TRANSACTION", "BEGIN TRANSACTION");

    verified_stmt("START TRANSACTION ISOLATION LEVEL READ UNCOMMITTED");
    verified_stmt("START TRANSACTION ISOLATION LEVEL READ COMMITTED");
    verified_stmt("START TRANSACTION ISOLATION LEVEL REPEATABLE READ");
    verified_stmt("START TRANSACTION ISOLATION LEVEL SERIALIZABLE");

    // Regression test for https://github.com/sqlparser-rs/sqlparser-rs/pull/139,
    // in which START TRANSACTION would fail to parse if followed by a statement
    // terminator.
    assert_eq!(
        parse_sql_statements("START TRANSACTION; SELECT 1"),
        Ok(vec![
            verified_stmt("START TRANSACTION"),
            verified_stmt("SELECT 1"),
        ])
    );

    let res = parse_sql_statements("START TRANSACTION ISOLATION LEVEL BAD");
    assert_eq!(
        ParserError::ParserError("Expected: isolation level, found: BAD".to_string()),
        res.unwrap_err()
    );

    let res = parse_sql_statements("START TRANSACTION BAD");
    assert_eq!(
        ParserError::ParserError("Expected: end of statement, found: BAD".to_string()),
        res.unwrap_err()
    );

    let res = parse_sql_statements("START TRANSACTION READ ONLY,");
    assert_eq!(
        ParserError::ParserError("Expected: transaction mode, found: EOF".to_string()),
        res.unwrap_err()
    );
}

#[test]
fn parse_set_transaction() {
    // SET TRANSACTION shares transaction mode parsing code with START
    // TRANSACTION, so no need to duplicate the tests here. We just do a quick
    // sanity check.
    match verified_stmt("SET TRANSACTION READ ONLY, READ WRITE, ISOLATION LEVEL SERIALIZABLE") {
        Statement::SetTransaction {
            modes,
            session,
            snapshot,
        } => {
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
        Statement::SetVariable {
            local,
            hivevar,
            variables,
            value,
        } => {
            assert!(!local);
            assert!(!hivevar);
            assert_eq!(
                variables,
                OneOrManyWithParens::One(ObjectName(vec!["SOMETHING".into()]))
            );
            assert_eq!(
                value,
                vec![Expr::Value(Value::SingleQuotedString("1".into()))]
            );
        }
        _ => unreachable!(),
    }

    let multi_variable_dialects = all_dialects_where(|d| d.supports_parenthesized_set_variables());
    let sql = r#"SET (a, b, c) = (1, 2, 3)"#;
    match multi_variable_dialects.verified_stmt(sql) {
        Statement::SetVariable {
            local,
            hivevar,
            variables,
            value,
        } => {
            assert!(!local);
            assert!(!hivevar);
            assert_eq!(
                variables,
                OneOrManyWithParens::Many(vec![
                    ObjectName(vec!["a".into()]),
                    ObjectName(vec!["b".into()]),
                    ObjectName(vec!["c".into()]),
                ])
            );
            assert_eq!(
                value,
                vec![
                    Expr::Value(number("1")),
                    Expr::Value(number("2")),
                    Expr::Value(number("3")),
                ]
            );
        }
        _ => unreachable!(),
    }

    let error_sqls = [
        ("SET (a, b, c) = (1, 2, 3", "Expected: ), found: EOF"),
        ("SET (a, b, c) = 1, 2, 3", "Expected: (, found: 1"),
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
fn parse_double_colon_cast_at_timezone() {
    let sql = "SELECT '2001-01-01T00:00:00.000Z'::TIMESTAMP AT TIME ZONE 'Europe/Brussels' FROM t";
    let select = verified_only_select(sql);

    assert_eq!(
        &Expr::AtTimeZone {
            timestamp: Box::new(Expr::Cast {
                kind: CastKind::DoubleColon,
                expr: Box::new(Expr::Value(Value::SingleQuotedString(
                    "2001-01-01T00:00:00.000Z".to_string()
                ),)),
                data_type: DataType::Timestamp(None, TimezoneInfo::None),
                format: None
            }),
            time_zone: Box::new(Expr::Value(Value::SingleQuotedString(
                "Europe/Brussels".to_string()
            ))),
        },
        expr_from_projection(only(&select.projection)),
    );
}

#[test]
fn parse_set_time_zone() {
    match verified_stmt("SET TIMEZONE = 'UTC'") {
        Statement::SetVariable {
            local,
            hivevar,
            variables: variable,
            value,
        } => {
            assert!(!local);
            assert!(!hivevar);
            assert_eq!(
                variable,
                OneOrManyWithParens::One(ObjectName(vec!["TIMEZONE".into()]))
            );
            assert_eq!(
                value,
                vec![Expr::Value(Value::SingleQuotedString("UTC".into()))]
            );
        }
        _ => unreachable!(),
    }

    one_statement_parses_to("SET TIME ZONE TO 'UTC'", "SET TIMEZONE = 'UTC'");
}

#[test]
fn parse_set_time_zone_alias() {
    match verified_stmt("SET TIME ZONE 'UTC'") {
        Statement::SetTimeZone { local, value } => {
            assert!(!local);
            assert_eq!(value, Expr::Value(Value::SingleQuotedString("UTC".into())));
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_commit() {
    match verified_stmt("COMMIT") {
        Statement::Commit { chain: false } => (),
        _ => unreachable!(),
    }

    match verified_stmt("COMMIT AND CHAIN") {
        Statement::Commit { chain: true } => (),
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
    one_statement_parses_to("END AND NO CHAIN", "COMMIT");
    one_statement_parses_to("END WORK AND NO CHAIN", "COMMIT");
    one_statement_parses_to("END TRANSACTION AND NO CHAIN", "COMMIT");
    one_statement_parses_to("END WORK AND CHAIN", "COMMIT AND CHAIN");
    one_statement_parses_to("END TRANSACTION AND CHAIN", "COMMIT AND CHAIN");
    one_statement_parses_to("END WORK", "COMMIT");
    one_statement_parses_to("END TRANSACTION", "COMMIT");
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
    let sql = "CREATE UNIQUE INDEX IF NOT EXISTS idx_name ON test(name,age DESC)";
    let indexed_columns = vec![
        OrderByExpr {
            expr: Expr::Identifier(Ident::new("name")),
            asc: None,
            nulls_first: None,
        },
        OrderByExpr {
            expr: Expr::Identifier(Ident::new("age")),
            asc: Some(false),
            nulls_first: None,
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
    let sql = "CREATE UNIQUE INDEX IF NOT EXISTS idx_name ON test USING btree (name,age DESC)";
    let indexed_columns = vec![
        OrderByExpr {
            expr: Expr::Identifier(Ident::new("name")),
            asc: None,
            nulls_first: None,
        },
        OrderByExpr {
            expr: Expr::Identifier(Ident::new("age")),
            asc: Some(false),
            nulls_first: None,
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
            predicate: None,
        }) => {
            assert_eq!("idx_name", name.to_string());
            assert_eq!("test", table_name.to_string());
            assert_eq!("btree", using.unwrap().to_string());
            assert_eq!(indexed_columns, columns);
            assert!(unique);
            assert!(!concurrently);
            assert!(if_not_exists);
            assert!(include.is_empty());
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
        Statement::CreateRole { names, .. } => {
            assert_eq_vec(&["consultant"], &names);
        }
        _ => unreachable!(),
    }

    let sql = "CREATE ROLE IF NOT EXISTS mysql_a, mysql_b";
    match verified_stmt(sql) {
        Statement::CreateRole {
            names,
            if_not_exists,
            ..
        } => {
            assert_eq_vec(&["mysql_a", "mysql_b"], &names);
            assert!(if_not_exists);
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
    let sql = "GRANT SELECT, INSERT, UPDATE (shape, size), USAGE, DELETE, TRUNCATE, REFERENCES, TRIGGER, CONNECT, CREATE, EXECUTE, TEMPORARY ON abc, def TO xyz, m WITH GRANT OPTION GRANTED BY jj";
    match verified_stmt(sql) {
        Statement::Grant {
            privileges,
            objects,
            grantees,
            with_grant_option,
            granted_by,
            ..
        } => match (privileges, objects) {
            (Privileges::Actions(actions), GrantObjects::Tables(objects)) => {
                assert_eq!(
                    vec![
                        Action::Select { columns: None },
                        Action::Insert { columns: None },
                        Action::Update {
                            columns: Some(vec![
                                Ident {
                                    value: "shape".into(),
                                    quote_style: None,
                                },
                                Ident {
                                    value: "size".into(),
                                    quote_style: None,
                                },
                            ])
                        },
                        Action::Usage,
                        Action::Delete,
                        Action::Truncate,
                        Action::References { columns: None },
                        Action::Trigger,
                        Action::Connect,
                        Action::Create,
                        Action::Execute,
                        Action::Temporary,
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
            (Privileges::Actions(actions), GrantObjects::AllTablesInSchema { schemas }) => {
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
            (Privileges::Actions(actions), GrantObjects::Sequences(objects), None) => {
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
                GrantObjects::Schemas(schemas),
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
            (Privileges::Actions(actions), GrantObjects::AllSequencesInSchema { schemas }) => {
                assert_eq!(vec![Action::Usage], actions);
                assert_eq_vec(&["bus"], &schemas);
            }
            _ => unreachable!(),
        },
        _ => unreachable!(),
    }
}

#[test]
fn test_revoke() {
    let sql = "REVOKE ALL PRIVILEGES ON users, auth FROM analyst CASCADE";
    match verified_stmt(sql) {
        Statement::Revoke {
            privileges,
            objects: GrantObjects::Tables(tables),
            grantees,
            cascade,
            granted_by,
        } => {
            assert_eq!(
                Privileges::All {
                    with_privileges_keyword: true
                },
                privileges
            );
            assert_eq_vec(&["users", "auth"], &tables);
            assert_eq_vec(&["analyst"], &grantees);
            assert!(cascade);
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
            },
            Statement::Merge {
                into: no_into,
                table: table_no_into,
                source: source_no_into,
                on: on_no_into,
                clauses: clauses_no_into,
            },
        ) => {
            assert!(into);
            assert!(!no_into);

            assert_eq!(
                table,
                TableFactor::Table {
                    name: ObjectName(vec![Ident::new("s"), Ident::new("bar")]),
                    alias: Some(TableAlias {
                        name: Ident::new("dest"),
                        columns: vec![],
                    }),
                    args: None,
                    with_hints: vec![],
                    version: None,
                    partitions: vec![],
                    with_ordinality: false,
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
                            distinct: None,
                            top: None,
                            projection: vec![SelectItem::Wildcard(
                                WildcardAdditionalOptions::default()
                            )],
                            into: None,
                            from: vec![TableWithJoins {
                                relation: TableFactor::Table {
                                    name: ObjectName(vec![Ident::new("s"), Ident::new("foo")]),
                                    alias: None,
                                    args: None,
                                    with_hints: vec![],
                                    version: None,
                                    partitions: vec![],
                                    with_ordinality: false,
                                },
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
                        }))),
                        order_by: vec![],
                        limit: None,
                        limit_by: vec![],
                        offset: None,
                        fetch: None,
                        locks: vec![],
                        for_clause: None,
                        settings: None,
                        format_clause: None,
                    }),
                    alias: Some(TableAlias {
                        name: Ident {
                            value: "stg".to_string(),
                            quote_style: None,
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
                            right: Box::new(Expr::Value(Value::SingleQuotedString(
                                "a".to_string()
                            ))),
                        }),
                        action: MergeAction::Update {
                            assignments: vec![
                                Assignment {
                                    target: AssignmentTarget::ColumnName(ObjectName(vec![
                                        Ident::new("dest"),
                                        Ident::new("F")
                                    ])),
                                    value: Expr::CompoundIdentifier(vec![
                                        Ident::new("stg"),
                                        Ident::new("F"),
                                    ]),
                                },
                                Assignment {
                                    target: AssignmentTarget::ColumnName(ObjectName(vec![
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

    let sql = "MERGE INTO s.bar AS dest USING newArrivals AS S ON false WHEN NOT MATCHED THEN INSERT VALUES (stg.A, stg.B, stg.C)";
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
        lock.of.unwrap().0,
        vec![Ident {
            value: "school".to_string(),
            quote_style: None
        }]
    );
    assert!(lock.nonblock.is_none());

    let sql = "SELECT * FROM student WHERE id = '1' FOR SHARE OF school";
    let mut ast = verified_query(sql);
    assert_eq!(ast.locks.len(), 1);
    let lock = ast.locks.pop().unwrap();
    assert_eq!(lock.lock_type, LockType::Share);
    assert_eq!(
        lock.of.unwrap().0,
        vec![Ident {
            value: "school".to_string(),
            quote_style: None
        }]
    );
    assert!(lock.nonblock.is_none());

    let sql = "SELECT * FROM student WHERE id = '1' FOR SHARE OF school FOR UPDATE OF student";
    let mut ast = verified_query(sql);
    assert_eq!(ast.locks.len(), 2);
    let lock = ast.locks.remove(0);
    assert_eq!(lock.lock_type, LockType::Share);
    assert_eq!(
        lock.of.unwrap().0,
        vec![Ident {
            value: "school".to_string(),
            quote_style: None
        }]
    );
    assert!(lock.nonblock.is_none());
    let lock = ast.locks.remove(0);
    assert_eq!(lock.lock_type, LockType::Update);
    assert_eq!(
        lock.of.unwrap().0,
        vec![Ident {
            value: "student".to_string(),
            quote_style: None
        }]
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
        lock.of.unwrap().0,
        vec![Ident {
            value: "school".to_string(),
            quote_style: None
        }]
    );
    assert_eq!(lock.nonblock.unwrap(), NonBlock::SkipLocked);

    let sql = "SELECT * FROM student WHERE id = '1' FOR SHARE OF school NOWAIT";
    let mut ast = verified_query(sql);
    assert_eq!(ast.locks.len(), 1);
    let lock = ast.locks.pop().unwrap();
    assert_eq!(lock.lock_type, LockType::Share);
    assert_eq!(
        lock.of.unwrap().0,
        vec![Ident {
            value: "school".to_string(),
            quote_style: None
        }]
    );
    assert_eq!(lock.nonblock.unwrap(), NonBlock::Nowait);
}

#[test]
fn test_placeholder() {
    let dialects = TestedDialects {
        dialects: vec![
            Box::new(GenericDialect {}),
            Box::new(DuckDbDialect {}),
            Box::new(PostgreSqlDialect {}),
            Box::new(MsSqlDialect {}),
            Box::new(AnsiDialect {}),
            Box::new(BigQueryDialect {}),
            Box::new(SnowflakeDialect {}),
            // Note: `$` is the starting word for the HiveDialect identifier
            // Box::new(sqlparser::dialect::HiveDialect {}),
        ],
        options: None,
    };
    let sql = "SELECT * FROM student WHERE id = $Id1";
    let ast = dialects.verified_only_select(sql);
    assert_eq!(
        ast.selection,
        Some(Expr::BinaryOp {
            left: Box::new(Expr::Identifier(Ident::new("id"))),
            op: BinaryOperator::Eq,
            right: Box::new(Expr::Value(Value::Placeholder("$Id1".into()))),
        })
    );

    let sql = "SELECT * FROM student LIMIT $1 OFFSET $2";
    let ast = dialects.verified_query(sql);
    assert_eq!(
        ast.limit,
        Some(Expr::Value(Value::Placeholder("$1".into())))
    );
    assert_eq!(
        ast.offset,
        Some(Offset {
            value: Expr::Value(Value::Placeholder("$2".into())),
            rows: OffsetRows::None,
        }),
    );

    let dialects = TestedDialects {
        dialects: vec![
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
        ],
        options: None,
    };
    let sql = "SELECT * FROM student WHERE id = ?";
    let ast = dialects.verified_only_select(sql);
    assert_eq!(
        ast.selection,
        Some(Expr::BinaryOp {
            left: Box::new(Expr::Identifier(Ident::new("id"))),
            op: BinaryOperator::Eq,
            right: Box::new(Expr::Value(Value::Placeholder("?".into()))),
        })
    );

    let sql = "SELECT $fromage_français, :x, ?123";
    let ast = dialects.verified_only_select(sql);
    assert_eq!(
        ast.projection,
        vec![
            UnnamedExpr(Expr::Value(Value::Placeholder("$fromage_français".into()))),
            UnnamedExpr(Expr::Value(Value::Placeholder(":x".into()))),
            UnnamedExpr(Expr::Value(Value::Placeholder("?123".into()))),
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
    let sql = "SELECT foo FROM bar LIMIT 2 OFFSET 2";
    let expect = Some(Offset {
        value: Expr::Value(number("2")),
        rows: OffsetRows::None,
    });
    let ast = verified_query(sql);
    assert_eq!(ast.offset, expect);
    assert_eq!(ast.limit, Some(Expr::Value(number("2"))));

    // different order is OK
    one_statement_parses_to("SELECT foo FROM bar OFFSET 2 LIMIT 2", sql);

    // expressions are allowed
    let sql = "SELECT foo FROM bar LIMIT 1 + 2 OFFSET 3 * 4";
    let ast = verified_query(sql);
    assert_eq!(
        ast.limit,
        Some(Expr::BinaryOp {
            left: Box::new(Expr::Value(number("1"))),
            op: BinaryOperator::Plus,
            right: Box::new(Expr::Value(number("2"))),
        }),
    );
    assert_eq!(
        ast.offset,
        Some(Offset {
            value: Expr::BinaryOp {
                left: Box::new(Expr::Value(number("3"))),
                op: BinaryOperator::Multiply,
                right: Box::new(Expr::Value(number("4"))),
            },
            rows: OffsetRows::None,
        }),
    );

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
        let sql = format!("SELECT {}()", func_name);
        let select = verified_only_select(&sql);
        let select_localtime_func_call_ast = Function {
            name: ObjectName(vec![Ident::new(func_name)]),
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
        let sql_without_parens = format!("SELECT {}", func_name);
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
    let sql = "SELECT POSITION('@' IN field)";
    let select = verified_only_select(sql);
    assert_eq!(
        &Expr::Position {
            expr: Box::new(Expr::Value(Value::SingleQuotedString("@".to_string()))),
            r#in: Box::new(Expr::Identifier(Ident::new("field"))),
        },
        expr_from_projection(only(&select.projection))
    );
}

#[test]
fn parse_position_negative() {
    let sql = "SELECT POSITION(foo) from bar";
    let res = parse_sql_statements(sql);
    assert_eq!(
        ParserError::ParserError("Position function must include IN keyword".to_string()),
        res.unwrap_err()
    );

    let sql = "SELECT POSITION(foo IN) from bar";
    let res = parse_sql_statements(sql);
    assert_eq!(
        ParserError::ParserError("Expected: an expression:, found: )".to_string()),
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

    verified_stmt("SELECT f FROM foo WHERE field IS UNKNOWN");
    verified_stmt("SELECT f FROM foo WHERE field IS NOT UNKNOWN");

    let sql = "SELECT f from foo where field is 0";
    let res = parse_sql_statements(sql);
    assert_eq!(
        ParserError::ParserError(
            "Expected: [NOT] NULL or TRUE|FALSE or [NOT] DISTINCT FROM after IS, found: 0"
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
            table_name: ObjectName(vec![Ident::with_quote('\'', cache_table_name)]),
            has_as: false,
            options: vec![],
            query: None,
        }
    );

    assert_eq!(
        verified_stmt(format!("CACHE {table_flag} TABLE '{cache_table_name}'").as_str()),
        Statement::Cache {
            table_flag: Some(ObjectName(vec![Ident::new(table_flag)])),
            table_name: ObjectName(vec![Ident::with_quote('\'', cache_table_name)]),
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
            table_flag: Some(ObjectName(vec![Ident::new(table_flag)])),
            table_name: ObjectName(vec![Ident::with_quote('\'', cache_table_name)]),
            has_as: false,
            options: vec![
                SqlOption {
                    name: Ident::with_quote('\'', "K1"),
                    value: Expr::Value(Value::SingleQuotedString("V1".into())),
                },
                SqlOption {
                    name: Ident::with_quote('\'', "K2"),
                    value: Expr::Value(number("0.88")),
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
            table_flag: Some(ObjectName(vec![Ident::new(table_flag)])),
            table_name: ObjectName(vec![Ident::with_quote('\'', cache_table_name)]),
            has_as: false,
            options: vec![
                SqlOption {
                    name: Ident::with_quote('\'', "K1"),
                    value: Expr::Value(Value::SingleQuotedString("V1".into())),
                },
                SqlOption {
                    name: Ident::with_quote('\'', "K2"),
                    value: Expr::Value(number("0.88")),
                },
            ],
            query: Some(query.clone()),
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
            table_flag: Some(ObjectName(vec![Ident::new(table_flag)])),
            table_name: ObjectName(vec![Ident::with_quote('\'', cache_table_name)]),
            has_as: true,
            options: vec![
                SqlOption {
                    name: Ident::with_quote('\'', "K1"),
                    value: Expr::Value(Value::SingleQuotedString("V1".into())),
                },
                SqlOption {
                    name: Ident::with_quote('\'', "K2"),
                    value: Expr::Value(number("0.88")),
                },
            ],
            query: Some(query.clone()),
        }
    );

    assert_eq!(
        verified_stmt(format!("CACHE {table_flag} TABLE '{cache_table_name}' {sql}").as_str()),
        Statement::Cache {
            table_flag: Some(ObjectName(vec![Ident::new(table_flag)])),
            table_name: ObjectName(vec![Ident::with_quote('\'', cache_table_name)]),
            has_as: false,
            options: vec![],
            query: Some(query.clone()),
        }
    );

    assert_eq!(
        verified_stmt(format!("CACHE {table_flag} TABLE '{cache_table_name}' AS {sql}").as_str()),
        Statement::Cache {
            table_flag: Some(ObjectName(vec![Ident::new(table_flag)])),
            table_name: ObjectName(vec![Ident::with_quote('\'', cache_table_name)]),
            has_as: true,
            options: vec![],
            query: Some(query),
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
            table_name: ObjectName(vec![Ident::with_quote('\'', "table_name")]),
            if_exists: false,
        }
    );

    assert_eq!(
        verified_stmt("UNCACHE TABLE IF EXISTS 'table_name'"),
        Statement::UNCache {
            table_name: ObjectName(vec![Ident::with_quote('\'', "table_name")]),
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
    fn assert_mysql_query_value(sql: &str, quoted: &str) {
        let stmt = TestedDialects {
            dialects: vec![
                Box::new(MySqlDialect {}),
                Box::new(BigQueryDialect {}),
                Box::new(SnowflakeDialect {}),
            ],
            options: None,
        }
        .one_statement_parses_to(sql, "");

        match stmt {
            Statement::Query(query) => match *query.body {
                SetExpr::Select(value) => {
                    let expr = expr_from_projection(only(&value.projection));
                    assert_eq!(
                        *expr,
                        Expr::Value(Value::SingleQuotedString(quoted.to_string()))
                    );
                }
                _ => unreachable!(),
            },
            _ => unreachable!(),
        };
    }
    let sql = r"SELECT 'I\'m fine'";
    assert_mysql_query_value(sql, "I'm fine");

    let sql = r#"SELECT 'I''m fine'"#;
    assert_mysql_query_value(sql, "I'm fine");

    let sql = r#"SELECT 'I\"m fine'"#;
    assert_mysql_query_value(sql, "I\"m fine");

    let sql = r"SELECT 'Testing: \0 \\ \% \_ \b \n \r \t \Z \a \h \ '";
    assert_mysql_query_value(sql, "Testing: \0 \\ % _ \u{8} \n \r \t \u{1a} \u{7} h  ");
}

#[test]
fn parse_escaped_string_without_unescape() {
    fn assert_mysql_query_value(sql: &str, quoted: &str) {
        let stmt = TestedDialects {
            dialects: vec![
                Box::new(MySqlDialect {}),
                Box::new(BigQueryDialect {}),
                Box::new(SnowflakeDialect {}),
            ],
            options: Some(ParserOptions::new().with_unescape(false)),
        }
        .one_statement_parses_to(sql, "");

        match stmt {
            Statement::Query(query) => match *query.body {
                SetExpr::Select(value) => {
                    let expr = expr_from_projection(only(&value.projection));
                    assert_eq!(
                        *expr,
                        Expr::Value(Value::SingleQuotedString(quoted.to_string()))
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
                name: ObjectName(vec![Ident::new("monthly_sales")]),
                alias: Some(TableAlias {
                    name: Ident::new("a"),
                    columns: vec![]
                }),
                args: None,
                with_hints: vec![],
                version: None,
                partitions: vec![],
                with_ordinality: false,
            }),
            aggregate_functions: vec![
                expected_function("a", None),
                expected_function("b", Some("t")),
                expected_function("c", Some("u")),
            ],
            value_column: vec![Ident::new("a"), Ident::new("MONTH")],
            value_source: PivotValueSource::List(vec![
                ExprWithAlias {
                    expr: Expr::Value(number("1")),
                    alias: Some(Ident::new("x"))
                },
                ExprWithAlias {
                    expr: Expr::Value(Value::SingleQuotedString("two".to_string())),
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
                    quote_style: None
                },
                columns: vec![Ident::new("c"), Ident::new("d")],
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
}

#[test]
fn parse_unpivot_table() {
    let sql = concat!(
        "SELECT * FROM sales AS s ",
        "UNPIVOT(quantity FOR quarter IN (Q1, Q2, Q3, Q4)) AS u (product, quarter, quantity)"
    );

    pretty_assertions::assert_eq!(
        verified_only_select(sql).from[0].relation,
        Unpivot {
            table: Box::new(TableFactor::Table {
                name: ObjectName(vec![Ident::new("sales")]),
                alias: Some(TableAlias {
                    name: Ident::new("s"),
                    columns: vec![]
                }),
                args: None,
                with_hints: vec![],
                version: None,
                partitions: vec![],
                with_ordinality: false,
            }),
            value: Ident {
                value: "quantity".to_string(),
                quote_style: None
            },

            name: Ident {
                value: "quarter".to_string(),
                quote_style: None
            },
            columns: ["Q1", "Q2", "Q3", "Q4"]
                .into_iter()
                .map(Ident::new)
                .collect(),
            alias: Some(TableAlias {
                name: Ident::new("u"),
                columns: ["product", "quarter", "quantity"]
                    .into_iter()
                    .map(Ident::new)
                    .collect()
            }),
        }
    );
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
                    name: ObjectName(vec![Ident::new("census")]),
                    alias: Some(TableAlias {
                        name: Ident::new("c"),
                        columns: vec![]
                    }),
                    args: None,
                    with_hints: vec![],
                    version: None,
                    partitions: vec![],
                    with_ordinality: false,
                }),
                value: Ident {
                    value: "population".to_string(),
                    quote_style: None
                },

                name: Ident {
                    value: "year".to_string(),
                    quote_style: None
                },
                columns: ["population_2000", "population_2010"]
                    .into_iter()
                    .map(Ident::new)
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
            value_column: vec![Ident::new("year")],
            value_source: PivotValueSource::List(vec![
                ExprWithAlias {
                    expr: Expr::Value(Value::SingleQuotedString("population_2000".to_string())),
                    alias: None
                },
                ExprWithAlias {
                    expr: Expr::Value(Value::SingleQuotedString("population_2010".to_string())),
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
    let supported_dialects = TestedDialects {
        dialects: vec![
            Box::new(GenericDialect {}),
            Box::new(DuckDbDialect {}),
            Box::new(PostgreSqlDialect {}),
            Box::new(MsSqlDialect {}),
            Box::new(RedshiftSqlDialect {}),
            Box::new(MySqlDialect {}),
        ],
        options: None,
    };

    supported_dialects.verified_stmt("SELECT a.説明 FROM test.public.inter01 AS a");
    supported_dialects.verified_stmt("SELECT a.説明 FROM inter01 AS a, inter01_transactions AS b WHERE a.説明 = b.取引 GROUP BY a.説明");
    supported_dialects.verified_stmt("SELECT 説明, hühnervögel, garçon, Москва, 東京 FROM inter01");
    assert!(supported_dialects
        .parse_sql_statements("SELECT 💝 FROM table1")
        .is_err());
}

#[test]
fn parse_trailing_comma() {
    // At the moment, Duck DB is the only dialect that allows
    // trailing commas anywhere in the query
    let trailing_commas = TestedDialects {
        dialects: vec![Box::new(DuckDbDialect {})],
        options: None,
    };

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

    // doesn't allow any trailing commas
    let trailing_commas = TestedDialects {
        dialects: vec![Box::new(GenericDialect {})],
        options: None,
    };

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
}

#[test]
fn parse_projection_trailing_comma() {
    // Some dialects allow trailing commas only in the projection
    let trailing_commas = TestedDialects {
        dialects: vec![Box::new(SnowflakeDialect {}), Box::new(BigQueryDialect {})],
        options: None,
    };

    trailing_commas.one_statement_parses_to(
        "SELECT album_id, name, FROM track",
        "SELECT album_id, name FROM track",
    );

    trailing_commas.verified_stmt("SELECT album_id, name FROM track");

    trailing_commas.verified_stmt("SELECT * FROM track ORDER BY milliseconds");

    trailing_commas.verified_stmt("SELECT DISTINCT ON (album_id) name FROM track");

    assert_eq!(
        trailing_commas
            .parse_sql_statements("SELECT * FROM track ORDER BY milliseconds,")
            .unwrap_err(),
        ParserError::ParserError("Expected: an expression:, found: EOF".to_string())
    );

    assert_eq!(
        trailing_commas
            .parse_sql_statements("CREATE TABLE employees (name text, age int,)")
            .unwrap_err(),
        ParserError::ParserError(
            "Expected: column name or constraint definition, found: )".to_string()
        ),
    );
}

#[test]
fn parse_create_type() {
    let create_type =
        verified_stmt("CREATE TYPE db.type_name AS (foo INT, bar TEXT COLLATE \"de_DE\")");
    assert_eq!(
        Statement::CreateType {
            name: ObjectName(vec![Ident::new("db"), Ident::new("type_name")]),
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
                        collation: Some(ObjectName(vec![Ident::with_quote('\"', "de_DE")])),
                    }
                ]
            }
        },
        create_type
    );
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
            parameters: FunctionArguments::None,
            args: FunctionArguments::List(FunctionArgumentList {
                duplicate_treatment: None,
                args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
                    Value::SingleQuotedString("a".to_string())
                )))],
                clauses: vec![],
            }),
            name: ObjectName(vec![Ident::new("my_procedure")]),
            filter: None,
            null_treatment: None,
            over: None,
            within_group: vec![],
        })
    );
}

#[test]
fn parse_create_table_collate() {
    pg_and_generic().verified_stmt("CREATE TABLE tbl (foo INT, bar TEXT COLLATE \"de_DE\")");
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
            query: Box::new(Query {
                body: Box::new(SetExpr::Select(Box::new(Select {
                    distinct: None,
                    top: None,
                    projection: vec![UnnamedExpr(Expr::Identifier(Ident::new("cola"))),],
                    into: None,
                    from: vec![TableWithJoins {
                        relation: TableFactor::Table {
                            name: ObjectName(vec![Ident::new("tab")]),
                            alias: None,
                            args: None,
                            with_hints: vec![],
                            version: None,
                            partitions: vec![],
                            with_ordinality: false,
                        },
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
                }))),
                with: None,
                limit: None,
                limit_by: vec![],
                offset: None,
                fetch: None,
                locks: vec![],
                for_clause: None,
                order_by: vec![],
                settings: None,
                format_clause: None,
            }),
            to: Ident {
                value: "s3://...".to_string(),
                quote_style: Some('\'')
            },
            with: vec![SqlOption {
                name: Ident {
                    value: "format".to_string(),
                    quote_style: None
                },
                value: Expr::Value(Value::SingleQuotedString("AVRO".to_string()))
            }]
        }
    );
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
    let dialects = TestedDialects {
        dialects: vec![Box::new(BigQueryDialect {}), Box::new(SnowflakeDialect {})],
        options: None,
    };
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
    let dialects = TestedDialects {
        dialects: vec![Box::new(BigQueryDialect {}), Box::new(ClickHouseDialect {})],
        options: None,
    };
    let expr = dialects.verified_expr(sql);
    let expected = Expr::MapAccess {
        column: Expr::Identifier(Ident::new("users")).into(),
        keys: vec![
            MapAccessKey {
                key: Expr::UnaryOp {
                    op: UnaryOperator::Minus,
                    expr: Expr::Value(number("1")).into(),
                },
                syntax: MapAccessSyntax::Bracket,
            },
            MapAccessKey {
                key: call("safe_offset", [Expr::Value(number("2"))]),
                syntax: MapAccessSyntax::Bracket,
            },
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
        distinct: None,
        top: None,
        projection: vec![
            SelectItem::UnnamedExpr(Expr::Identifier(Ident::new("employee_id"))),
            SelectItem::UnnamedExpr(Expr::Identifier(Ident::new("manager_id"))),
            SelectItem::UnnamedExpr(Expr::Identifier(Ident::new("title"))),
        ],
        from: vec![TableWithJoins {
            relation: TableFactor::Table {
                name: ObjectName(vec![Ident::new("employees")]),
                alias: None,
                args: None,
                with_hints: vec![],
                version: None,
                partitions: vec![],
                with_ordinality: false,
            },
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
                right: Box::new(Expr::Value(Value::SingleQuotedString(
                    "president".to_owned(),
                ))),
            },
            relationships: vec![Expr::BinaryOp {
                left: Box::new(Expr::Identifier(Ident::new("manager_id"))),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::Prior(Box::new(Expr::Identifier(Ident::new(
                    "employee_id",
                ))))),
            }],
        }),
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
            distinct: None,
            top: None,
            projection: vec![
                SelectItem::UnnamedExpr(Expr::Identifier(Ident::new("employee_id"))),
                SelectItem::UnnamedExpr(Expr::Identifier(Ident::new("manager_id"))),
                SelectItem::UnnamedExpr(Expr::Identifier(Ident::new("title"))),
            ],
            from: vec![TableWithJoins {
                relation: TableFactor::Table {
                    name: ObjectName(vec![Ident::new("employees")]),
                    alias: None,
                    args: None,
                    with_hints: vec![],
                    version: None,
                    partitions: vec![],
                    with_ordinality: false,
                },
                joins: vec![],
            }],
            into: None,
            lateral_views: vec![],
            prewhere: None,
            selection: Some(Expr::BinaryOp {
                left: Box::new(Expr::Identifier(Ident::new("employee_id"))),
                op: BinaryOperator::NotEq,
                right: Box::new(Expr::Value(number("42"))),
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
                    right: Box::new(Expr::Value(Value::SingleQuotedString(
                        "president".to_owned(),
                    ))),
                },
                relationships: vec![Expr::BinaryOp {
                    left: Box::new(Expr::Identifier(Ident::new("manager_id"))),
                    op: BinaryOperator::Eq,
                    right: Box::new(Expr::Prior(Box::new(Expr::Identifier(Ident::new(
                        "employee_id",
                    ))))),
                }],
            }),
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
    let sql = concat!(
        "SELECT ",
        "ARRAY_AGG(name) FILTER (WHERE name IS NOT NULL), ",
        "ARRAY_AGG(name) FILTER (WHERE name LIKE 'a%') AS agg2 ",
        "FROM region"
    );
    assert_eq!(
        all_dialects_where(|d| d.supports_filter_during_aggregation())
            .verified_only_select(sql)
            .projection,
        vec![
            SelectItem::UnnamedExpr(Expr::Function(Function {
                name: ObjectName(vec![Ident::new("ARRAY_AGG")]),
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
                    name: ObjectName(vec![Ident::new("ARRAY_AGG")]),
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
                        pattern: Box::new(Expr::Value(Value::SingleQuotedString("a%".to_owned()))),
                        escape_char: None,
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
fn test_match_recognize() {
    use MatchRecognizePattern::*;
    use MatchRecognizeSymbol::*;
    use RepetitionQuantifier::*;

    let table = TableFactor::Table {
        name: ObjectName(vec![Ident::new("my_table")]),
        alias: None,
        args: None,
        with_hints: vec![],
        version: None,
        partitions: vec![],
        with_ordinality: false,
    };

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
                asc: None,
                nulls_first: None,
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
    let dialects = TestedDialects {
        dialects: vec![
            Box::new(GenericDialect {}),
            Box::new(BigQueryDialect {}),
            Box::new(ClickHouseDialect {}),
            Box::new(SnowflakeDialect {}),
            Box::new(DuckDbDialect {}),
        ],
        options: None,
    };
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
                expr: Expr::Value(Value::SingleQuotedString("widget".to_owned())),
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

#[test]
fn parse_sized_list() {
    let dialects = TestedDialects {
        dialects: vec![
            Box::new(GenericDialect {}),
            Box::new(PostgreSqlDialect {}),
            Box::new(DuckDbDialect {}),
        ],
        options: None,
    };
    let sql = r#"CREATE TABLE embeddings (data FLOAT[1536])"#;
    dialects.verified_stmt(sql);
    let sql = r#"CREATE TABLE embeddings (data FLOAT[1536][3])"#;
    dialects.verified_stmt(sql);
    let sql = r#"SELECT data::FLOAT[1536] FROM embeddings"#;
    dialects.verified_stmt(sql);
}

#[test]
fn insert_into_with_parentheses() {
    let dialects = TestedDialects {
        dialects: vec![
            Box::new(SnowflakeDialect {}),
            Box::new(RedshiftSqlDialect {}),
            Box::new(GenericDialect {}),
        ],
        options: None,
    };
    dialects.verified_stmt("INSERT INTO t1 (id, name) (SELECT t2.id, t2.name FROM t2)");
}

#[test]
fn test_dictionary_syntax() {
    fn check(sql: &str, expect: Expr) {
        assert_eq!(
            all_dialects_where(|d| d.supports_dictionary_syntax()).verified_expr(sql),
            expect
        );
    }

    check(
        "{'Alberta': 'Edmonton', 'Manitoba': 'Winnipeg'}",
        Expr::Dictionary(vec![
            DictionaryField {
                key: Ident::with_quote('\'', "Alberta"),
                value: Box::new(Expr::Value(Value::SingleQuotedString(
                    "Edmonton".to_owned(),
                ))),
            },
            DictionaryField {
                key: Ident::with_quote('\'', "Manitoba"),
                value: Box::new(Expr::Value(Value::SingleQuotedString(
                    "Winnipeg".to_owned(),
                ))),
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
                    expr: Box::new(Expr::Value(Value::SingleQuotedString(
                        "2023-04-01".to_owned(),
                    ))),
                    data_type: DataType::Timestamp(None, TimezoneInfo::None),
                    format: None,
                }),
            },
            DictionaryField {
                key: Ident::with_quote('\'', "end"),
                value: Box::new(Expr::Cast {
                    kind: CastKind::Cast,
                    expr: Box::new(Expr::Value(Value::SingleQuotedString(
                        "2023-04-05".to_owned(),
                    ))),
                    data_type: DataType::Timestamp(None, TimezoneInfo::None),
                    format: None,
                }),
            },
        ]),
    )
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
    let dialects = TestedDialects {
        dialects: vec![
            Box::new(GenericDialect {}),
            Box::new(SnowflakeDialect {}),
            Box::new(DatabricksDialect {}),
        ],
        options: None,
    };
    let sql = "SELECT * FROM VALUES (1, 2), (2,3) AS tbl (id, val)";
    let canonical = "SELECT * FROM (VALUES (1, 2), (2, 3)) AS tbl (id, val)";
    dialects.verified_only_select_with_canonical(sql, canonical);
}

#[test]
fn tests_select_values_without_parens_and_set_op() {
    let dialects = TestedDialects {
        dialects: vec![
            Box::new(GenericDialect {}),
            Box::new(SnowflakeDialect {}),
            Box::new(DatabricksDialect {}),
        ],
        options: None,
    };
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
fn parse_auto_increment_too_large() {
    let dialect = GenericDialect {};
    let u64_max = u64::MAX;
    let sql =
        format!("CREATE TABLE foo (bar INT NOT NULL AUTO_INCREMENT) AUTO_INCREMENT=1{u64_max}");

    let res = Parser::new(&dialect)
        .try_with_sql(&sql)
        .expect("tokenize to work")
        .parse_statements();

    assert!(res.is_err(), "{res:?}");
}

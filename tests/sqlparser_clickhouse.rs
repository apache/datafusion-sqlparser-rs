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
//! Test SQL syntax specific to ClickHouse.

#[macro_use]
mod test_utils;

use test_utils::*;

use sqlparser::ast::Expr::{BinaryOp, Identifier, MapAccess};
use sqlparser::ast::SelectItem::UnnamedExpr;
use sqlparser::ast::TableFactor::Table;
use sqlparser::ast::*;

use sqlparser::dialect::ClickHouseDialect;
use sqlparser::dialect::GenericDialect;

#[test]
fn parse_map_access_expr() {
    let sql = r#"SELECT string_values[indexOf(string_names, 'endpoint')] FROM foos WHERE id = 'test' AND string_value[indexOf(string_name, 'app')] <> 'foo'"#;
    let select = clickhouse().verified_only_select(sql);
    assert_eq!(
        Select {
            distinct: None,
            top: None,
            projection: vec![UnnamedExpr(MapAccess {
                column: Box::new(Identifier(Ident {
                    value: "string_values".to_string(),
                    quote_style: None,
                })),
                keys: vec![MapAccessKey {
                    key: call(
                        "indexOf",
                        [
                            Expr::Identifier(Ident::new("string_names")),
                            Expr::Value(Value::SingleQuotedString("endpoint".to_string()))
                        ]
                    ),
                    syntax: MapAccessSyntax::Bracket
                }],
            })],
            into: None,
            from: vec![TableWithJoins {
                relation: Table {
                    name: ObjectName(vec![Ident::new("foos")]),
                    alias: None,
                    args: None,
                    with_hints: vec![],
                    version: None,
                    partitions: vec![],
                },
                joins: vec![],
            }],
            lateral_views: vec![],
            selection: Some(BinaryOp {
                left: Box::new(BinaryOp {
                    left: Box::new(Identifier(Ident::new("id"))),
                    op: BinaryOperator::Eq,
                    right: Box::new(Expr::Value(Value::SingleQuotedString("test".to_string()))),
                }),
                op: BinaryOperator::And,
                right: Box::new(BinaryOp {
                    left: Box::new(MapAccess {
                        column: Box::new(Identifier(Ident::new("string_value"))),
                        keys: vec![MapAccessKey {
                            key: call(
                                "indexOf",
                                [
                                    Expr::Identifier(Ident::new("string_name")),
                                    Expr::Value(Value::SingleQuotedString("app".to_string()))
                                ]
                            ),
                            syntax: MapAccessSyntax::Bracket
                        }],
                    }),
                    op: BinaryOperator::NotEq,
                    right: Box::new(Expr::Value(Value::SingleQuotedString("foo".to_string()))),
                }),
            }),
            group_by: GroupByExpr::Expressions(vec![]),
            cluster_by: vec![],
            distribute_by: vec![],
            sort_by: vec![],
            having: None,
            named_window: vec![],
            window_before_qualify: false,
            qualify: None,
            value_table_mode: None,
            connect_by: None,
        },
        select
    );
}

#[test]
fn parse_array_expr() {
    let sql = "SELECT ['1', '2'] FROM test";
    let select = clickhouse().verified_only_select(sql);
    assert_eq!(
        &Expr::Array(Array {
            elem: vec![
                Expr::Value(Value::SingleQuotedString("1".to_string())),
                Expr::Value(Value::SingleQuotedString("2".to_string())),
            ],
            named: false,
        }),
        expr_from_projection(only(&select.projection))
    )
}

#[test]
fn parse_array_fn() {
    let sql = "SELECT array(x1, x2) FROM foo";
    let select = clickhouse().verified_only_select(sql);
    assert_eq!(
        &call(
            "array",
            [
                Expr::Identifier(Ident::new("x1")),
                Expr::Identifier(Ident::new("x2"))
            ]
        ),
        expr_from_projection(only(&select.projection))
    );
}

#[test]
fn parse_kill() {
    let stmt = clickhouse().verified_stmt("KILL MUTATION 5");
    assert_eq!(
        stmt,
        Statement::Kill {
            modifier: Some(KillType::Mutation),
            id: 5,
        }
    );
}

#[test]
fn parse_delimited_identifiers() {
    // check that quoted identifiers in any position remain quoted after serialization
    let select = clickhouse().verified_only_select(
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

    clickhouse().verified_stmt(r#"CREATE TABLE "foo" ("bar" "int")"#);
    clickhouse().verified_stmt(r#"ALTER TABLE foo ADD CONSTRAINT "bar" PRIMARY KEY (baz)"#);
    //TODO verified_stmt(r#"UPDATE foo SET "bar" = 5"#);
}

#[test]
fn parse_create_table() {
    clickhouse().verified_stmt(r#"CREATE TABLE "x" ("a" "int") ENGINE=MergeTree ORDER BY ("x")"#);
    clickhouse().one_statement_parses_to(
        r#"CREATE TABLE "x" ("a" "int") ENGINE=MergeTree ORDER BY "x""#,
        r#"CREATE TABLE "x" ("a" "int") ENGINE=MergeTree ORDER BY ("x")"#,
    );
    clickhouse().verified_stmt(
        r#"CREATE TABLE "x" ("a" "int") ENGINE=MergeTree ORDER BY ("x") AS SELECT * FROM "t" WHERE true"#,
    );
}

#[test]
fn parse_double_equal() {
    clickhouse().one_statement_parses_to(
        r#"SELECT foo FROM bar WHERE buz == 'buz'"#,
        r#"SELECT foo FROM bar WHERE buz = 'buz'"#,
    );
}

#[test]
fn parse_limit_by() {
    clickhouse_and_generic().verified_stmt(
        r#"SELECT * FROM default.last_asset_runs_mv ORDER BY created_at DESC LIMIT 1 BY asset"#,
    );
    clickhouse_and_generic().verified_stmt(
        r#"SELECT * FROM default.last_asset_runs_mv ORDER BY created_at DESC LIMIT 1 BY asset, toStartOfDay(created_at)"#,
    );
}

#[test]
fn parse_select_star_except() {
    clickhouse().verified_stmt("SELECT * EXCEPT (prev_status) FROM anomalies");
}

#[test]
fn parse_select_star_except_no_parens() {
    clickhouse().one_statement_parses_to(
        "SELECT * EXCEPT prev_status FROM anomalies",
        "SELECT * EXCEPT (prev_status) FROM anomalies",
    );
}

fn clickhouse() -> TestedDialects {
    TestedDialects {
        dialects: vec![Box::new(ClickHouseDialect {})],
        options: None,
    }
}

fn clickhouse_and_generic() -> TestedDialects {
    TestedDialects {
        dialects: vec![Box::new(ClickHouseDialect {}), Box::new(GenericDialect {})],
        options: None,
    }
}

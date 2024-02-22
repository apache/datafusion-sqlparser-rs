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
use sqlparser::dialect::{DuckDbDialect, GenericDialect};

fn duckdb() -> TestedDialects {
    TestedDialects {
        dialects: vec![Box::new(DuckDbDialect {})],
        options: None,
    }
}

fn duckdb_and_generic() -> TestedDialects {
    TestedDialects {
        dialects: vec![Box::new(DuckDbDialect {}), Box::new(GenericDialect {})],
        options: None,
    }
}

#[test]
fn test_select_wildcard_with_exclude() {
    let select = duckdb().verified_only_select("SELECT * EXCLUDE (col_a) FROM data");
    let expected = SelectItem::Wildcard(WildcardAdditionalOptions {
        opt_exclude: Some(ExcludeSelectItem::Multiple(vec![Ident::new("col_a")])),
        ..Default::default()
    });
    assert_eq!(expected, select.projection[0]);

    let select =
        duckdb().verified_only_select("SELECT name.* EXCLUDE department_id FROM employee_table");
    let expected = SelectItem::QualifiedWildcard(
        ObjectName(vec![Ident::new("name")]),
        WildcardAdditionalOptions {
            opt_exclude: Some(ExcludeSelectItem::Single(Ident::new("department_id"))),
            ..Default::default()
        },
    );
    assert_eq!(expected, select.projection[0]);

    let select = duckdb()
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
fn parse_div_infix() {
    duckdb_and_generic().verified_stmt(r#"SELECT 5 // 2"#);
}

#[test]
fn test_create_macro() {
    let macro_ = duckdb().verified_stmt("CREATE MACRO schema.add(a, b) AS a + b");
    let expected = Statement::CreateMacro {
        or_replace: false,
        temporary: false,
        name: ObjectName(vec![Ident::new("schema"), Ident::new("add")]),
        args: Some(vec![MacroArg::new("a"), MacroArg::new("b")]),
        definition: MacroDefinition::Expr(Expr::BinaryOp {
            left: Box::new(Expr::Identifier(Ident::new("a"))),
            op: BinaryOperator::Plus,
            right: Box::new(Expr::Identifier(Ident::new("b"))),
        }),
    };
    assert_eq!(expected, macro_);
}

#[test]
fn test_create_macro_default_args() {
    let macro_ = duckdb().verified_stmt("CREATE MACRO add_default(a, b := 5) AS a + b");
    let expected = Statement::CreateMacro {
        or_replace: false,
        temporary: false,
        name: ObjectName(vec![Ident::new("add_default")]),
        args: Some(vec![
            MacroArg::new("a"),
            MacroArg {
                name: Ident::new("b"),
                default_expr: Some(Expr::Value(number("5"))),
            },
        ]),
        definition: MacroDefinition::Expr(Expr::BinaryOp {
            left: Box::new(Expr::Identifier(Ident::new("a"))),
            op: BinaryOperator::Plus,
            right: Box::new(Expr::Identifier(Ident::new("b"))),
        }),
    };
    assert_eq!(expected, macro_);
}

#[test]
fn test_create_table_macro() {
    let query = "SELECT col1_value AS column1, col2_value AS column2 UNION ALL SELECT 'Hello' AS col1_value, 456 AS col2_value";
    let macro_ = duckdb().verified_stmt(
        &("CREATE OR REPLACE TEMPORARY MACRO dynamic_table(col1_value, col2_value) AS TABLE "
            .to_string()
            + query),
    );
    let expected = Statement::CreateMacro {
        or_replace: true,
        temporary: true,
        name: ObjectName(vec![Ident::new("dynamic_table")]),
        args: Some(vec![
            MacroArg::new("col1_value"),
            MacroArg::new("col2_value"),
        ]),
        definition: MacroDefinition::Table(duckdb().verified_query(query)),
    };
    assert_eq!(expected, macro_);
}

#[test]
fn test_select_union_by_name() {
    let q1 = "SELECT * FROM capitals UNION BY NAME SELECT * FROM weather";
    let q2 = "SELECT * FROM capitals UNION ALL BY NAME SELECT * FROM weather";
    let q3 = "SELECT * FROM capitals UNION DISTINCT BY NAME SELECT * FROM weather";

    for (ast, expected_quantifier) in &[
        (duckdb().verified_query(q1), SetQuantifier::ByName),
        (duckdb().verified_query(q2), SetQuantifier::AllByName),
        (duckdb().verified_query(q3), SetQuantifier::DistinctByName),
    ] {
        let expected = Box::<SetExpr>::new(SetExpr::SetOperation {
            op: SetOperator::Union,
            set_quantifier: *expected_quantifier,
            left: Box::<SetExpr>::new(SetExpr::Select(Box::new(Select {
                distinct: None,
                top: None,
                projection: vec![SelectItem::Wildcard(WildcardAdditionalOptions {
                    opt_exclude: None,
                    opt_except: None,
                    opt_rename: None,
                    opt_replace: None,
                })],
                into: None,
                from: vec![TableWithJoins {
                    relation: TableFactor::Table {
                        name: ObjectName(vec![Ident {
                            value: "capitals".to_string(),
                            quote_style: None,
                        }]),
                        alias: None,
                        args: None,
                        with_hints: vec![],
                        version: None,
                        partitions: vec![],
                    },
                    joins: vec![],
                }],
                lateral_views: vec![],
                selection: None,
                group_by: GroupByExpr::Expressions(vec![]),
                cluster_by: vec![],
                distribute_by: vec![],
                sort_by: vec![],
                having: None,
                named_window: vec![],
                qualify: None,
                from_before_select: false,
            }))),
            right: Box::<SetExpr>::new(SetExpr::Select(Box::new(Select {
                distinct: None,
                top: None,
                projection: vec![SelectItem::Wildcard(WildcardAdditionalOptions {
                    opt_exclude: None,
                    opt_except: None,
                    opt_rename: None,
                    opt_replace: None,
                })],
                into: None,
                from: vec![TableWithJoins {
                    relation: TableFactor::Table {
                        name: ObjectName(vec![Ident {
                            value: "weather".to_string(),
                            quote_style: None,
                        }]),
                        alias: None,
                        args: None,
                        with_hints: vec![],
                        version: None,
                        partitions: vec![],
                    },
                    joins: vec![],
                }],
                lateral_views: vec![],
                selection: None,
                group_by: GroupByExpr::Expressions(vec![]),
                cluster_by: vec![],
                distribute_by: vec![],
                sort_by: vec![],
                having: None,
                named_window: vec![],
                qualify: None,
                from_before_select: false,
            }))),
        });
        assert_eq!(ast.body, expected);
    }
}

#[test]
fn test_duckdb_install() {
    let stmt = duckdb().verified_stmt("INSTALL tpch");
    assert_eq!(
        stmt,
        Statement::Install {
            extension_name: Ident {
                value: "tpch".to_string(),
                quote_style: None
            }
        }
    );
}

#[test]
fn test_duckdb_load_extension() {
    let stmt = duckdb().verified_stmt("LOAD my_extension");
    assert_eq!(
        Statement::Load {
            extension_name: Ident {
                value: "my_extension".to_string(),
                quote_style: None
            }
        },
        stmt
    );
}

#[test]
fn test_duckdb_from_statement() {
    let stmt = duckdb().verified_only_select("FROM my_table");
    let expected = Select {
        distinct: None,
        top: None,
        projection: vec![],
        into: None,
        from: vec![TableWithJoins {
            relation: TableFactor::Table {
                name: ObjectName(vec![Ident {
                    value: "my_table".to_string(),
                    quote_style: None,
                }]),
                alias: None,
                args: None,
                with_hints: vec![],
                version: None,
                partitions: vec![],
            },
            joins: vec![],
        }],
        lateral_views: vec![],
        selection: None,
        group_by: GroupByExpr::Expressions(vec![]),
        cluster_by: vec![],
        distribute_by: vec![],
        sort_by: vec![],
        having: None,
        named_window: vec![],
        qualify: None,
        from_before_select: true,
    };
    assert_eq!(stmt, expected);
}

#[test]
fn test_duckdb_from_statement_with_filter() {
    let stmt = duckdb().verified_only_select("FROM t1 WHERE a = 1");
    println!("{:?}", stmt);
    let expected = Select {
        distinct: None,
        top: None,
        projection: vec![],
        into: None,
        from: vec![TableWithJoins {
            relation: TableFactor::Table {
                name: ObjectName(vec![Ident {
                    value: "t1".to_string(),
                    quote_style: None,
                }]),
                alias: None,
                args: None,
                with_hints: vec![],
                version: None,
                partitions: vec![],
            },
            joins: vec![],
        }],
        lateral_views: vec![],
        selection: Some(Expr::BinaryOp {
            left: Box::new(Expr::Identifier(Ident {
                value: "a".to_string(),
                quote_style: None,
            })),
            op: BinaryOperator::Eq,
            right: Box::new(Expr::Value(number("1"))),
        }),
        group_by: GroupByExpr::Expressions(vec![]),
        cluster_by: vec![],
        distribute_by: vec![],
        sort_by: vec![],
        having: None,
        named_window: vec![],
        qualify: None,
        from_before_select: true,
    };
    assert_eq!(stmt, expected);
}

#[test]
fn test_duckdb_from_statement_with_filter_and_select() {
    let stmt = duckdb().verified_only_select("FROM t1 SELECT b WHERE a = 1");
    println!("{:?}", stmt);
    let expected = Select {
        distinct: None,
        top: None,
        projection: vec![SelectItem::UnnamedExpr(Expr::Identifier(Ident {
            value: "b".to_string(),
            quote_style: None,
        }))],
        into: None,
        from: vec![TableWithJoins {
            relation: TableFactor::Table {
                name: ObjectName(vec![Ident {
                    value: "t1".to_string(),
                    quote_style: None,
                }]),
                alias: None,
                args: None,
                with_hints: vec![],
                version: None,
                partitions: vec![],
            },
            joins: vec![],
        }],
        lateral_views: vec![],
        selection: Some(Expr::BinaryOp {
            left: Box::new(Expr::Identifier(Ident {
                value: "a".to_string(),
                quote_style: None,
            })),
            op: BinaryOperator::Eq,
            right: Box::new(Expr::Value(number("1"))),
        }),
        group_by: GroupByExpr::Expressions(vec![]),
        cluster_by: vec![],
        distribute_by: vec![],
        sort_by: vec![],
        having: None,
        named_window: vec![],
        qualify: None,
        from_before_select: true,
    };
    assert_eq!(stmt, expected);
}

#[test]
fn test_from_with_copy() {
    let stmt = duckdb().verified_stmt("COPY (FROM trek_facts) TO 'phaser_filled_facts.parquet'");
    let body = Box::new(SetExpr::Select(Box::new(Select {
        distinct: None,
        top: None,
        projection: vec![],
        into: None,
        from: vec![TableWithJoins {
            relation: TableFactor::Table {
                name: ObjectName(vec![Ident {
                    value: "trek_facts".to_string(),
                    quote_style: None,
                }]),
                alias: None,
                args: None,
                with_hints: vec![],
                version: None,
                partitions: vec![],
            },
            joins: vec![],
        }],
        lateral_views: vec![],
        selection: None,
        group_by: GroupByExpr::Expressions(vec![]),
        cluster_by: vec![],
        distribute_by: vec![],
        sort_by: vec![],
        having: None,
        named_window: vec![],
        qualify: None,
        from_before_select: true,
    })));
    let source = CopySource::Query(Box::new(Query {
        with: None,
        body,
        order_by: vec![],
        limit: None,
        limit_by: vec![],
        offset: None,
        fetch: None,
        locks: vec![],
        for_clause: None,
    }));

    let expected = Statement::Copy {
        source,
        to: true,
        target: CopyTarget::File {
            filename: "phaser_filled_facts.parquet".to_string(),
        },
        options: vec![],
        legacy_options: vec![],
        values: vec![],
    };
    assert_eq!(stmt, expected);
}

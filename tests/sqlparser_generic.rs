extern crate log;
extern crate sqlparser;

use sqlparser::dialect::*;
use sqlparser::sqlast::*;
use sqlparser::sqlparser::*;
use sqlparser::sqltokenizer::*;

#[test]
fn parse_delete_statement() {
    let sql = "DELETE FROM \"table\"";
    match verified_stmt(sql) {
        SQLStatement::SQLDelete { table_name, .. } => {
            assert_eq!(SQLObjectName(vec!["\"table\"".to_string()]), table_name);
        }

        _ => assert!(false),
    }
}

#[test]
fn parse_where_delete_statement() {
    use self::ASTNode::*;
    use self::SQLOperator::*;

    let sql = "DELETE FROM foo WHERE name = 5";
    match verified_stmt(sql) {
        SQLStatement::SQLDelete {
            table_name,
            selection,
            ..
        } => {
            assert_eq!(SQLObjectName(vec!["foo".to_string()]), table_name);

            assert_eq!(
                SQLBinaryExpr {
                    left: Box::new(SQLIdentifier("name".to_string())),
                    op: Eq,
                    right: Box::new(SQLValue(Value::Long(5))),
                },
                selection.unwrap(),
            );
        }

        _ => assert!(false),
    }
}

#[test]
fn parse_simple_select() {
    let sql = "SELECT id, fname, lname FROM customer WHERE id = 1 LIMIT 5";
    let select = verified_only_select(sql);
    assert_eq!(false, select.distinct);
    assert_eq!(3, select.projection.len());
    let select = verified_query(sql);
    assert_eq!(Some(ASTNode::SQLValue(Value::Long(5))), select.limit);
}

#[test]
fn parse_select_distinct() {
    let sql = "SELECT DISTINCT name FROM customer";
    let select = verified_only_select(sql);
    assert_eq!(true, select.distinct);
    assert_eq!(
        &SQLSelectItem::UnnamedExpression(ASTNode::SQLIdentifier("name".to_string())),
        only(&select.projection)
    );
}

#[test]
fn parse_select_wildcard() {
    let sql = "SELECT * FROM foo";
    let select = verified_only_select(sql);
    assert_eq!(&SQLSelectItem::Wildcard, only(&select.projection));

    let sql = "SELECT foo.* FROM foo";
    let select = verified_only_select(sql);
    assert_eq!(
        &SQLSelectItem::QualifiedWildcard(SQLObjectName(vec!["foo".to_string()])),
        only(&select.projection)
    );

    let sql = "SELECT myschema.mytable.* FROM myschema.mytable";
    let select = verified_only_select(sql);
    assert_eq!(
        &SQLSelectItem::QualifiedWildcard(SQLObjectName(vec![
            "myschema".to_string(),
            "mytable".to_string(),
        ])),
        only(&select.projection)
    );
}

#[test]
fn parse_count_wildcard() {
    verified_only_select("SELECT COUNT(Employee.*) FROM Order JOIN Employee ON Order.employee = Employee.id");
}

#[test]
fn parse_column_aliases() {
    let sql = "SELECT a.col + 1 AS newname FROM foo AS a";
    let select = verified_only_select(sql);
    if let SQLSelectItem::ExpressionWithAlias(
        ASTNode::SQLBinaryExpr {
            ref op, ref right, ..
        },
        ref alias,
    ) = only(&select.projection)
    {
        assert_eq!(&SQLOperator::Plus, op);
        assert_eq!(&ASTNode::SQLValue(Value::Long(1)), right.as_ref());
        assert_eq!("newname", alias);
    } else {
        panic!("Expected ExpressionWithAlias")
    }

    // alias without AS is parsed correctly:
    one_statement_parses_to("SELECT a.col + 1 newname FROM foo AS a", &sql);
}

#[test]
fn parse_select_count_wildcard() {
    let sql = "SELECT COUNT(*) FROM customer";
    let select = verified_only_select(sql);
    assert_eq!(
        &ASTNode::SQLFunction {
            id: "COUNT".to_string(),
            args: vec![ASTNode::SQLWildcard],
        },
        expr_from_projection(only(&select.projection))
    );
}

#[test]
fn parse_not() {
    let sql = "SELECT id FROM customer WHERE NOT salary = ''";
    let _ast = verified_only_select(sql);
    //TODO: add assertions
}

#[test]
fn parse_select_string_predicate() {
    let sql = "SELECT id, fname, lname FROM customer \
               WHERE salary != 'Not Provided' AND salary != ''";
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
fn parse_escaped_single_quote_string_predicate() {
    use self::ASTNode::*;
    use self::SQLOperator::*;
    let sql = "SELECT id, fname, lname FROM customer \
               WHERE salary != 'Jim''s salary'";
    let ast = verified_only_select(sql);
    assert_eq!(
        Some(SQLBinaryExpr {
            left: Box::new(SQLIdentifier("salary".to_string())),
            op: NotEq,
            right: Box::new(SQLValue(Value::SingleQuotedString(
                "Jim's salary".to_string()
            )))
        }),
        ast.selection,
    );
}

#[test]
fn parse_compound_expr_1() {
    use self::ASTNode::*;
    use self::SQLOperator::*;
    let sql = "a + b * c";
    assert_eq!(
        SQLBinaryExpr {
            left: Box::new(SQLIdentifier("a".to_string())),
            op: Plus,
            right: Box::new(SQLBinaryExpr {
                left: Box::new(SQLIdentifier("b".to_string())),
                op: Multiply,
                right: Box::new(SQLIdentifier("c".to_string()))
            })
        },
        verified_expr(sql)
    );
}

#[test]
fn parse_compound_expr_2() {
    use self::ASTNode::*;
    use self::SQLOperator::*;
    let sql = "a * b + c";
    assert_eq!(
        SQLBinaryExpr {
            left: Box::new(SQLBinaryExpr {
                left: Box::new(SQLIdentifier("a".to_string())),
                op: Multiply,
                right: Box::new(SQLIdentifier("b".to_string()))
            }),
            op: Plus,
            right: Box::new(SQLIdentifier("c".to_string()))
        },
        verified_expr(sql)
    );
}

#[test]
fn parse_unary_math() {
    use self::ASTNode::*;
    use self::SQLOperator::*;
    let sql = "- a + - b";
    assert_eq!(
        SQLBinaryExpr {
            left: Box::new(SQLUnary {
                operator: Minus,
                expr: Box::new(SQLIdentifier("a".to_string())),
            }),
            op: Plus,
            right: Box::new(SQLUnary {
                operator: Minus,
                expr: Box::new(SQLIdentifier("b".to_string())),
            }),
        },
        verified_expr(sql)
    );
}

#[test]
fn parse_is_null() {
    use self::ASTNode::*;
    let sql = "a IS NULL";
    assert_eq!(
        SQLIsNull(Box::new(SQLIdentifier("a".to_string()))),
        verified_expr(sql)
    );
}

#[test]
fn parse_is_not_null() {
    use self::ASTNode::*;
    let sql = "a IS NOT NULL";
    assert_eq!(
        SQLIsNotNull(Box::new(SQLIdentifier("a".to_string()))),
        verified_expr(sql)
    );
}

#[test]
fn parse_not_precedence() {
    use self::ASTNode::*;
    // NOT has higher precedence than OR/AND, so the following must parse as (NOT true) OR true
    let sql = "NOT true OR true";
    match verified_expr(sql) {
        SQLBinaryExpr {
            op: SQLOperator::Or,
            ..
        } => assert!(true),
        _ => assert!(false),
    };

    // But NOT has lower precedence than comparison operators, so the following parses as NOT (a IS NULL)
    let sql = "NOT a IS NULL";
    match verified_expr(sql) {
        SQLUnary {
            operator: SQLOperator::Not,
            ..
        } => assert!(true),
        _ => assert!(false),
    };
}

#[test]
fn parse_like() {
    let sql = "SELECT * FROM customers WHERE name LIKE '%a'";
    let select = verified_only_select(sql);
    assert_eq!(
        ASTNode::SQLBinaryExpr {
            left: Box::new(ASTNode::SQLIdentifier("name".to_string())),
            op: SQLOperator::Like,
            right: Box::new(ASTNode::SQLValue(Value::SingleQuotedString(
                "%a".to_string()
            ))),
        },
        select.selection.unwrap()
    );
}

#[test]
fn parse_not_like() {
    let sql = "SELECT * FROM customers WHERE name NOT LIKE '%a'";
    let select = verified_only_select(sql);
    assert_eq!(
        ASTNode::SQLBinaryExpr {
            left: Box::new(ASTNode::SQLIdentifier("name".to_string())),
            op: SQLOperator::NotLike,
            right: Box::new(ASTNode::SQLValue(Value::SingleQuotedString(
                "%a".to_string()
            ))),
        },
        select.selection.unwrap()
    );
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
            ASTNode::SQLInList {
                expr: Box::new(ASTNode::SQLIdentifier("segment".to_string())),
                list: vec![
                    ASTNode::SQLValue(Value::SingleQuotedString("HIGH".to_string())),
                    ASTNode::SQLValue(Value::SingleQuotedString("MED".to_string())),
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
        ASTNode::SQLInSubquery {
            expr: Box::new(ASTNode::SQLIdentifier("segment".to_string())),
            subquery: Box::new(verified_query("SELECT segm FROM bar")),
            negated: false,
        },
        select.selection.unwrap()
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
            ASTNode::SQLBetween {
                expr: Box::new(ASTNode::SQLIdentifier("age".to_string())),
                low: Box::new(ASTNode::SQLValue(Value::Long(25))),
                high: Box::new(ASTNode::SQLValue(Value::Long(32))),
                negated,
            },
            select.selection.unwrap()
        );
    }
    chk(false);
    chk(true);
}

#[test]
fn parse_select_order_by() {
    fn chk(sql: &str) {
        let select = verified_query(sql);
        assert_eq!(
            Some(vec![
                SQLOrderByExpr {
                    expr: ASTNode::SQLIdentifier("lname".to_string()),
                    asc: Some(true),
                },
                SQLOrderByExpr {
                    expr: ASTNode::SQLIdentifier("fname".to_string()),
                    asc: Some(false),
                },
                SQLOrderByExpr {
                    expr: ASTNode::SQLIdentifier("id".to_string()),
                    asc: None,
                },
            ]),
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
        Some(vec![
            SQLOrderByExpr {
                expr: ASTNode::SQLIdentifier("lname".to_string()),
                asc: Some(true),
            },
            SQLOrderByExpr {
                expr: ASTNode::SQLIdentifier("fname".to_string()),
                asc: Some(false),
            },
        ]),
        select.order_by
    );
    assert_eq!(Some(ASTNode::SQLValue(Value::Long(2))), select.limit);
}

#[test]
fn parse_select_group_by() {
    let sql = "SELECT id, fname, lname FROM customer GROUP BY lname, fname";
    let select = verified_only_select(sql);
    assert_eq!(
        Some(vec![
            ASTNode::SQLIdentifier("lname".to_string()),
            ASTNode::SQLIdentifier("fname".to_string()),
        ]),
        select.group_by
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
    let sql = "SELECT CAST(id AS bigint) FROM customer";
    let select = verified_only_select(sql);
    assert_eq!(
        &ASTNode::SQLCast {
            expr: Box::new(ASTNode::SQLIdentifier("id".to_string())),
            data_type: SQLType::BigInt
        },
        expr_from_projection(only(&select.projection))
    );
    one_statement_parses_to(
        "SELECT CAST(id AS BIGINT) FROM customer",
        "SELECT CAST(id AS bigint) FROM customer",
    );
}

#[test]
fn parse_create_table() {
    let sql = String::from(
        "CREATE TABLE uk_cities (\
         name VARCHAR(100) NOT NULL,\
         lat DOUBLE NULL,\
         lng DOUBLE NULL)",
    );
    let ast = one_statement_parses_to(
        &sql,
        "CREATE TABLE uk_cities (\
         name character varying(100) NOT NULL, \
         lat double, \
         lng double)",
    );
    match ast {
        SQLStatement::SQLCreateTable {
            name,
            columns,
            external: _,
            file_format: _,
            location: _,
        } => {
            assert_eq!("uk_cities", name.to_string());
            assert_eq!(3, columns.len());

            let c_name = &columns[0];
            assert_eq!("name", c_name.name);
            assert_eq!(SQLType::Varchar(Some(100)), c_name.data_type);
            assert_eq!(false, c_name.allow_null);

            let c_lat = &columns[1];
            assert_eq!("lat", c_lat.name);
            assert_eq!(SQLType::Double, c_lat.data_type);
            assert_eq!(true, c_lat.allow_null);

            let c_lng = &columns[2];
            assert_eq!("lng", c_lng.name);
            assert_eq!(SQLType::Double, c_lng.data_type);
            assert_eq!(true, c_lng.allow_null);
        }
        _ => assert!(false),
    }
}

#[test]
fn parse_create_external_table() {
    let sql = String::from(
        "CREATE EXTERNAL TABLE uk_cities (\
         name VARCHAR(100) NOT NULL,\
         lat DOUBLE NULL,\
         lng DOUBLE NULL)\
         STORED AS TEXTFILE LOCATION '/tmp/example.csv",
    );
    let ast = one_statement_parses_to(
        &sql,
        "CREATE EXTERNAL TABLE uk_cities (\
         name character varying(100) NOT NULL, \
         lat double, \
         lng double) \
         STORED AS TEXTFILE LOCATION '/tmp/example.csv'",
    );
    match ast {
        SQLStatement::SQLCreateTable {
            name,
            columns,
            external,
            file_format,
            location,
        } => {
            assert_eq!("uk_cities", name.to_string());
            assert_eq!(3, columns.len());

            let c_name = &columns[0];
            assert_eq!("name", c_name.name);
            assert_eq!(SQLType::Varchar(Some(100)), c_name.data_type);
            assert_eq!(false, c_name.allow_null);

            let c_lat = &columns[1];
            assert_eq!("lat", c_lat.name);
            assert_eq!(SQLType::Double, c_lat.data_type);
            assert_eq!(true, c_lat.allow_null);

            let c_lng = &columns[2];
            assert_eq!("lng", c_lng.name);
            assert_eq!(SQLType::Double, c_lng.data_type);
            assert_eq!(true, c_lng.allow_null);

            assert!(external);
            assert_eq!(FileFormat::TEXTFILE, file_format.unwrap());
            assert_eq!("/tmp/example.csv", location.unwrap());
        }
        _ => assert!(false),
    }
}

#[test]
fn parse_scalar_function_in_projection() {
    let sql = "SELECT sqrt(id) FROM foo";
    let select = verified_only_select(sql);
    assert_eq!(
        &ASTNode::SQLFunction {
            id: String::from("sqrt"),
            args: vec![ASTNode::SQLIdentifier(String::from("id"))],
        },
        expr_from_projection(only(&select.projection))
    );
}

#[test]
fn parse_aggregate_with_group_by() {
    let sql = "SELECT a, COUNT(1), MIN(b), MAX(b) FROM foo GROUP BY a";
    let _ast = verified_only_select(sql);
    //TODO: assertions
}

#[test]
fn parse_literal_string() {
    let sql = "SELECT 'one', N'national string'";
    let select = verified_only_select(sql);
    assert_eq!(2, select.projection.len());
    assert_eq!(
        &ASTNode::SQLValue(Value::SingleQuotedString("one".to_string())),
        expr_from_projection(&select.projection[0])
    );
    assert_eq!(
        &ASTNode::SQLValue(Value::NationalStringLiteral("national string".to_string())),
        expr_from_projection(&select.projection[1])
    );
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
fn parse_select_version() {
    let sql = "SELECT @@version";
    let select = verified_only_select(sql);
    assert_eq!(
        &ASTNode::SQLIdentifier("@@version".to_string()),
        expr_from_projection(only(&select.projection)),
    );
}

#[test]
fn parse_delimited_identifiers() {
    // check that quoted identifiers in any position remain quoted after serialization
    let select = verified_only_select(
        r#"SELECT "alias"."bar baz", "myfun"(), "simple id" AS "column alias" FROM "a table" AS "alias""#
    );
    // check FROM
    match select.relation.unwrap() {
        TableFactor::Table { name, alias } => {
            assert_eq!(vec![r#""a table""#.to_string()], name.0);
            assert_eq!(r#""alias""#, alias.unwrap());
        }
        _ => panic!("Expecting TableFactor::Table"),
    }
    // check SELECT
    assert_eq!(3, select.projection.len());
    assert_eq!(
        &ASTNode::SQLCompoundIdentifier(vec![r#""alias""#.to_string(), r#""bar baz""#.to_string()]),
        expr_from_projection(&select.projection[0]),
    );
    assert_eq!(
        &ASTNode::SQLFunction {
            id: r#""myfun""#.to_string(),
            args: vec![]
        },
        expr_from_projection(&select.projection[1]),
    );
    match &select.projection[2] {
        &SQLSelectItem::ExpressionWithAlias(ref expr, ref alias) => {
            assert_eq!(&ASTNode::SQLIdentifier(r#""simple id""#.to_string()), expr);
            assert_eq!(r#""column alias""#, alias);
        }
        _ => panic!("Expected ExpressionWithAlias"),
    }

    verified_stmt(r#"CREATE TABLE "foo" ("bar" "int")"#);
    verified_stmt(r#"ALTER TABLE foo ADD CONSTRAINT "bar" PRIMARY KEY (baz)"#);
    //TODO verified_stmt(r#"UPDATE foo SET "bar" = 5"#);
}

#[test]
fn parse_parens() {
    use self::ASTNode::*;
    use self::SQLOperator::*;
    let sql = "(a + b) - (c + d)";
    assert_eq!(
        SQLBinaryExpr {
            left: Box::new(SQLNested(Box::new(SQLBinaryExpr {
                left: Box::new(SQLIdentifier("a".to_string())),
                op: Plus,
                right: Box::new(SQLIdentifier("b".to_string()))
            }))),
            op: Minus,
            right: Box::new(SQLNested(Box::new(SQLBinaryExpr {
                left: Box::new(SQLIdentifier("c".to_string())),
                op: Plus,
                right: Box::new(SQLIdentifier("d".to_string()))
            })))
        },
        verified_expr(sql)
    );
}

#[test]
fn parse_case_expression() {
    let sql = "SELECT CASE WHEN bar IS NULL THEN 'null' WHEN bar = 0 THEN '=0' WHEN bar >= 0 THEN '>=0' ELSE '<0' END FROM foo";
    use self::ASTNode::{SQLBinaryExpr, SQLCase, SQLIdentifier, SQLIsNull, SQLValue};
    use self::SQLOperator::*;
    let select = verified_only_select(sql);
    assert_eq!(
        &SQLCase {
            conditions: vec![
                SQLIsNull(Box::new(SQLIdentifier("bar".to_string()))),
                SQLBinaryExpr {
                    left: Box::new(SQLIdentifier("bar".to_string())),
                    op: Eq,
                    right: Box::new(SQLValue(Value::Long(0)))
                },
                SQLBinaryExpr {
                    left: Box::new(SQLIdentifier("bar".to_string())),
                    op: GtEq,
                    right: Box::new(SQLValue(Value::Long(0)))
                }
            ],
            results: vec![
                SQLValue(Value::SingleQuotedString("null".to_string())),
                SQLValue(Value::SingleQuotedString("=0".to_string())),
                SQLValue(Value::SingleQuotedString(">=0".to_string()))
            ],
            else_result: Some(Box::new(SQLValue(Value::SingleQuotedString(
                "<0".to_string()
            ))))
        },
        expr_from_projection(only(&select.projection)),
    );
}

#[test]
fn parse_implicit_join() {
    let sql = "SELECT * FROM t1, t2";
    let select = verified_only_select(sql);
    assert_eq!(
        &Join {
            relation: TableFactor::Table {
                name: SQLObjectName(vec!["t2".to_string()]),
                alias: None,
            },
            join_operator: JoinOperator::Implicit
        },
        only(&select.joins),
    );
}

#[test]
fn parse_cross_join() {
    let sql = "SELECT * FROM t1 CROSS JOIN t2";
    let select = verified_only_select(sql);
    assert_eq!(
        &Join {
            relation: TableFactor::Table {
                name: SQLObjectName(vec!["t2".to_string()]),
                alias: None,
            },
            join_operator: JoinOperator::Cross
        },
        only(&select.joins),
    );
}

#[test]
fn parse_joins_on() {
    fn join_with_constraint(
        relation: impl Into<String>,
        alias: Option<SQLIdent>,
        f: impl Fn(JoinConstraint) -> JoinOperator,
    ) -> Join {
        Join {
            relation: TableFactor::Table {
                name: SQLObjectName(vec![relation.into()]),
                alias,
            },
            join_operator: f(JoinConstraint::On(ASTNode::SQLBinaryExpr {
                left: Box::new(ASTNode::SQLIdentifier("c1".into())),
                op: SQLOperator::Eq,
                right: Box::new(ASTNode::SQLIdentifier("c2".into())),
            })),
        }
    }
    // Test parsing of aliases
    assert_eq!(
        verified_only_select("SELECT * FROM t1 JOIN t2 AS foo ON c1 = c2").joins,
        vec![join_with_constraint(
            "t2",
            Some("foo".to_string()),
            JoinOperator::Inner
        )]
    );
    one_statement_parses_to(
        "SELECT * FROM t1 JOIN t2 foo ON c1 = c2",
        "SELECT * FROM t1 JOIN t2 AS foo ON c1 = c2",
    );
    // Test parsing of different join operators
    assert_eq!(
        verified_only_select("SELECT * FROM t1 JOIN t2 ON c1 = c2").joins,
        vec![join_with_constraint("t2", None, JoinOperator::Inner)]
    );
    assert_eq!(
        verified_only_select("SELECT * FROM t1 LEFT JOIN t2 ON c1 = c2").joins,
        vec![join_with_constraint("t2", None, JoinOperator::LeftOuter)]
    );
    assert_eq!(
        verified_only_select("SELECT * FROM t1 RIGHT JOIN t2 ON c1 = c2").joins,
        vec![join_with_constraint("t2", None, JoinOperator::RightOuter)]
    );
    assert_eq!(
        verified_only_select("SELECT * FROM t1 FULL JOIN t2 ON c1 = c2").joins,
        vec![join_with_constraint("t2", None, JoinOperator::FullOuter)]
    );
}

#[test]
fn parse_joins_using() {
    fn join_with_constraint(
        relation: impl Into<String>,
        alias: Option<SQLIdent>,
        f: impl Fn(JoinConstraint) -> JoinOperator,
    ) -> Join {
        Join {
            relation: TableFactor::Table {
                name: SQLObjectName(vec![relation.into()]),
                alias,
            },
            join_operator: f(JoinConstraint::Using(vec!["c1".into()])),
        }
    }
    // Test parsing of aliases
    assert_eq!(
        verified_only_select("SELECT * FROM t1 JOIN t2 AS foo USING(c1)").joins,
        vec![join_with_constraint(
            "t2",
            Some("foo".to_string()),
            JoinOperator::Inner
        )]
    );
    one_statement_parses_to(
        "SELECT * FROM t1 JOIN t2 foo USING(c1)",
        "SELECT * FROM t1 JOIN t2 AS foo USING(c1)",
    );
    // Test parsing of different join operators
    assert_eq!(
        verified_only_select("SELECT * FROM t1 JOIN t2 USING(c1)").joins,
        vec![join_with_constraint("t2", None, JoinOperator::Inner)]
    );
    assert_eq!(
        verified_only_select("SELECT * FROM t1 LEFT JOIN t2 USING(c1)").joins,
        vec![join_with_constraint("t2", None, JoinOperator::LeftOuter)]
    );
    assert_eq!(
        verified_only_select("SELECT * FROM t1 RIGHT JOIN t2 USING(c1)").joins,
        vec![join_with_constraint("t2", None, JoinOperator::RightOuter)]
    );
    assert_eq!(
        verified_only_select("SELECT * FROM t1 FULL JOIN t2 USING(c1)").joins,
        vec![join_with_constraint("t2", None, JoinOperator::FullOuter)]
    );
}

#[test]
fn parse_complex_join() {
    let sql = "SELECT c1, c2 FROM t1, t4 JOIN t2 ON t2.c = t1.c LEFT JOIN t3 USING(q, c) WHERE t4.c = t1.c";
    verified_only_select(sql);
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
}

#[test]
fn parse_ctes() {
    let cte_sqls = vec!["SELECT 1 AS foo", "SELECT 2 AS bar"];
    let with = &format!(
        "WITH a AS ({}), b AS ({}) SELECT foo + bar FROM a, b",
        cte_sqls[0], cte_sqls[1]
    );

    fn assert_ctes_in_select(expected: &Vec<&str>, sel: &SQLQuery) {
        for i in 0..1 {
            let Cte {
                ref query,
                ref alias,
            } = sel.ctes[i];
            assert_eq!(expected[i], query.to_string());
            assert_eq!(if i == 0 { "a" } else { "b" }, alias);
        }
    }

    // Top-level CTE
    assert_ctes_in_select(&cte_sqls, &verified_query(with));
    // CTE in a subquery
    let sql = &format!("SELECT ({})", with);
    let select = verified_only_select(sql);
    match expr_from_projection(only(&select.projection)) {
        &ASTNode::SQLSubquery(ref subquery) => {
            assert_ctes_in_select(&cte_sqls, subquery.as_ref());
        }
        _ => panic!("Expected subquery"),
    }
    // CTE in a derived table
    let sql = &format!("SELECT * FROM ({})", with);
    let select = verified_only_select(sql);
    match select.relation {
        Some(TableFactor::Derived { subquery, .. }) => {
            assert_ctes_in_select(&cte_sqls, subquery.as_ref())
        }
        _ => panic!("Expected derived table"),
    }
    // CTE in a view
    let sql = &format!("CREATE VIEW v AS {}", with);
    match verified_stmt(sql) {
        SQLStatement::SQLCreateView { query, .. } => assert_ctes_in_select(&cte_sqls, &query),
        _ => panic!("Expected CREATE VIEW"),
    }
    // CTE in a CTE...
    let sql = &format!("WITH outer_cte AS ({}) SELECT * FROM outer_cte", with);
    let select = verified_query(sql);
    assert_ctes_in_select(&cte_sqls, &only(&select.ctes).query);
}

#[test]
fn parse_derived_tables() {
    let sql = "SELECT a.x, b.y FROM (SELECT x FROM foo) AS a CROSS JOIN (SELECT y FROM bar) AS b";
    let _ = verified_only_select(sql);
    //TODO: add assertions
}

#[test]
fn parse_union() {
    // TODO: add assertions
    verified_stmt("SELECT 1 UNION SELECT 2");
    verified_stmt("SELECT 1 UNION ALL SELECT 2");
    verified_stmt("SELECT 1 EXCEPT SELECT 2");
    verified_stmt("SELECT 1 EXCEPT ALL SELECT 2");
    verified_stmt("SELECT 1 INTERSECT SELECT 2");
    verified_stmt("SELECT 1 INTERSECT ALL SELECT 2");
    verified_stmt("SELECT 1 UNION SELECT 2 UNION SELECT 3");
    verified_stmt("SELECT 1 EXCEPT SELECT 2 UNION SELECT 3"); // Union[Except[1,2], 3]
    verified_stmt("SELECT 1 INTERSECT (SELECT 2 EXCEPT SELECT 3)");
    verified_stmt("WITH cte AS (SELECT 1 AS foo) (SELECT foo FROM cte ORDER BY 1 LIMIT 1)");
    verified_stmt("SELECT 1 UNION (SELECT 2 ORDER BY 1 LIMIT 1)");
    verified_stmt("SELECT 1 UNION SELECT 2 INTERSECT SELECT 3"); // Union[1, Intersect[2,3]]
    verified_stmt("SELECT foo FROM tab UNION SELECT bar FROM TAB");
}

#[test]
fn parse_multiple_statements() {
    fn test_with(sql1: &str, sql2_kw: &str, sql2_rest: &str) {
        // Check that a string consisting of two statements delimited by a semicolon
        // parses the same as both statements individually:
        let res = parse_sql_statements(&(sql1.to_owned() + ";" + sql2_kw + sql2_rest));
        assert_eq!(
            vec![
                one_statement_parses_to(&sql1, ""),
                one_statement_parses_to(&(sql2_kw.to_owned() + sql2_rest), ""),
            ],
            res.unwrap()
        );
        // Check that extra semicolon at the end is stripped by normalization:
        one_statement_parses_to(&(sql1.to_owned() + ";"), sql1);
        // Check that forgetting the semicolon results in an error:
        let res = parse_sql_statements(&(sql1.to_owned() + " " + sql2_kw + sql2_rest));
        assert_eq!(
            ParserError::ParserError("Expected end of statement, found: ".to_string() + sql2_kw),
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
    test_with("INSERT INTO foo VALUES(1)", "SELECT", " bar");
    test_with("CREATE TABLE foo (baz int)", "SELECT", " bar");
    // Make sure that empty statements do not cause an error:
    let res = parse_sql_statements(";;");
    assert_eq!(0, res.unwrap().len());
}

#[test]
fn parse_scalar_subqueries() {
    use self::ASTNode::*;
    let sql = "(SELECT 1) + (SELECT 2)";
    match verified_expr(sql) {
        SQLBinaryExpr {
            op: SQLOperator::Plus, ..
            //left: box SQLSubquery { .. },
            //right: box SQLSubquery { .. },
        } => assert!(true),
        _ => assert!(false),
    };
}

#[test]
fn parse_create_view() {
    let sql = "CREATE VIEW myschema.myview AS SELECT foo FROM bar";
    match verified_stmt(sql) {
        SQLStatement::SQLCreateView {
            name,
            query,
            materialized,
        } => {
            assert_eq!("myschema.myview", name.to_string());
            assert_eq!("SELECT foo FROM bar", query.to_string());
            assert!(!materialized);
        }
        _ => assert!(false),
    }
}

#[test]
fn parse_create_materialized_view() {
    let sql = "CREATE MATERIALIZED VIEW myschema.myview AS SELECT foo FROM bar";
    match verified_stmt(sql) {
        SQLStatement::SQLCreateView {
            name,
            query,
            materialized,
        } => {
            assert_eq!("myschema.myview", name.to_string());
            assert_eq!("SELECT foo FROM bar", query.to_string());
            assert!(materialized);
        }
        _ => assert!(false),
    }
}

#[test]
fn parse_invalid_subquery_without_parens() {
    let res = parse_sql_statements("SELECT SELECT 1 FROM bar WHERE 1=1 FROM baz");
    assert_eq!(
        ParserError::ParserError("Expected end of statement, found: 1".to_string()),
        res.unwrap_err()
    );
}

fn only<'a, T>(v: &'a Vec<T>) -> &'a T {
    assert_eq!(1, v.len());
    v.first().unwrap()
}

fn verified_query(query: &str) -> SQLQuery {
    match verified_stmt(query) {
        SQLStatement::SQLSelect(select) => select,
        _ => panic!("Expected SELECT"),
    }
}

fn expr_from_projection(item: &SQLSelectItem) -> &ASTNode {
    match item {
        SQLSelectItem::UnnamedExpression(expr) => expr,
        _ => panic!("Expected UnnamedExpression"),
    }
}

fn verified_only_select(query: &str) -> SQLSelect {
    match verified_query(query).body {
        SQLSetExpr::Select(s) => s,
        _ => panic!("Expected SQLSetExpr::Select"),
    }
}

fn verified_stmt(query: &str) -> SQLStatement {
    one_statement_parses_to(query, query)
}

fn verified_expr(query: &str) -> ASTNode {
    let ast = parse_sql_expr(query);
    assert_eq!(query, &ast.to_string());
    ast
}

/// Ensures that `sql` parses as a single statement, optionally checking that
/// converting AST back to string equals to `canonical` (unless an empty string
/// is provided).
fn one_statement_parses_to(sql: &str, canonical: &str) -> SQLStatement {
    let mut statements = parse_sql_statements(&sql).unwrap();
    assert_eq!(statements.len(), 1);

    let only_statement = statements.pop().unwrap();
    if !canonical.is_empty() {
        assert_eq!(canonical, only_statement.to_string())
    }
    only_statement
}

fn parse_sql_statements(sql: &str) -> Result<Vec<SQLStatement>, ParserError> {
    let generic_ast = Parser::parse_sql(&GenericSqlDialect {}, sql.to_string());
    let pg_ast = Parser::parse_sql(&PostgreSqlDialect {}, sql.to_string());
    assert_eq!(generic_ast, pg_ast);
    generic_ast
}

fn parse_sql_expr(sql: &str) -> ASTNode {
    let generic_ast = parse_sql_expr_with(&GenericSqlDialect {}, &sql.to_string());
    let pg_ast = parse_sql_expr_with(&PostgreSqlDialect {}, &sql.to_string());
    assert_eq!(generic_ast, pg_ast);
    generic_ast
}

fn parse_sql_expr_with(dialect: &Dialect, sql: &str) -> ASTNode {
    let mut tokenizer = Tokenizer::new(dialect, &sql);
    let tokens = tokenizer.tokenize().unwrap();
    let mut parser = Parser::new(tokens);
    let ast = parser.parse_expr().unwrap();
    ast
}

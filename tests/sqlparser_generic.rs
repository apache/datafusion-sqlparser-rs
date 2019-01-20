extern crate log;
extern crate sqlparser;

use sqlparser::dialect::*;
use sqlparser::sqlast::*;
use sqlparser::sqlparser::*;
use sqlparser::sqltokenizer::*;

#[test]
fn parse_delete_statement() {
    let sql: &str = "DELETE FROM 'table'";

    match verified(&sql) {
        ASTNode::SQLDelete { relation, .. } => {
            assert_eq!(
                Some(Box::new(ASTNode::SQLValue(Value::SingleQuotedString(
                    "table".to_string()
                )))),
                relation
            );
        }

        _ => assert!(false),
    }
}

#[test]
fn parse_where_delete_statement() {
    let sql: &str = "DELETE FROM 'table' WHERE name = 5";

    use self::ASTNode::*;
    use self::SQLOperator::*;

    match verified(&sql) {
        ASTNode::SQLDelete {
            relation,
            selection,
            ..
        } => {
            assert_eq!(
                Some(Box::new(ASTNode::SQLValue(Value::SingleQuotedString(
                    "table".to_string()
                )))),
                relation
            );

            assert_eq!(
                SQLBinaryExpr {
                    left: Box::new(SQLIdentifier("name".to_string())),
                    op: Eq,
                    right: Box::new(SQLValue(Value::Long(5))),
                },
                *selection.unwrap(),
            );
        }

        _ => assert!(false),
    }
}

#[test]
fn parse_simple_select() {
    let sql = String::from("SELECT id, fname, lname FROM customer WHERE id = 1 LIMIT 5");
    match verified(&sql) {
        ASTNode::SQLSelect {
            projection, limit, ..
        } => {
            assert_eq!(3, projection.len());
            assert_eq!(Some(Box::new(ASTNode::SQLValue(Value::Long(5)))), limit);
        }
        _ => assert!(false),
    }
}

#[test]
fn parse_select_wildcard() {
    let sql = String::from("SELECT * FROM customer");
    match verified(&sql) {
        ASTNode::SQLSelect { projection, .. } => {
            assert_eq!(1, projection.len());
            assert_eq!(ASTNode::SQLWildcard, projection[0]);
        }
        _ => assert!(false),
    }
}

#[test]
fn parse_select_count_wildcard() {
    let sql = String::from("SELECT COUNT(*) FROM customer");
    match verified(&sql) {
        ASTNode::SQLSelect { projection, .. } => {
            assert_eq!(1, projection.len());
            assert_eq!(
                ASTNode::SQLFunction {
                    id: "COUNT".to_string(),
                    args: vec![ASTNode::SQLWildcard],
                },
                projection[0]
            );
        }
        _ => assert!(false),
    }
}

#[test]
fn parse_not() {
    let sql = String::from(
        "SELECT id FROM customer \
         WHERE NOT salary = ''",
    );
    let _ast = verified(&sql);
    //TODO: add assertions
}

#[test]
fn parse_select_string_predicate() {
    let sql = String::from(
        "SELECT id, fname, lname FROM customer \
         WHERE salary != 'Not Provided' AND salary != ''",
    );
    let _ast = verified(&sql);
    //TODO: add assertions
}

#[test]
fn parse_projection_nested_type() {
    let sql = String::from("SELECT customer.address.state FROM foo");
    let _ast = verified(&sql);
    //TODO: add assertions
}

#[test]
fn parse_compound_expr_1() {
    use self::ASTNode::*;
    use self::SQLOperator::*;
    let sql = String::from("a + b * c");
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
        verified(&sql)
    );
}

#[test]
fn parse_compound_expr_2() {
    use self::ASTNode::*;
    use self::SQLOperator::*;
    let sql = String::from("a * b + c");
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
        verified(&sql)
    );
}

#[test]
fn parse_is_null() {
    use self::ASTNode::*;
    let sql = String::from("a IS NULL");
    assert_eq!(
        SQLIsNull(Box::new(SQLIdentifier("a".to_string()))),
        verified(&sql)
    );
}

#[test]
fn parse_is_not_null() {
    use self::ASTNode::*;
    let sql = String::from("a IS NOT NULL");
    assert_eq!(
        SQLIsNotNull(Box::new(SQLIdentifier("a".to_string()))),
        verified(&sql)
    );
}

#[test]
fn parse_like() {
    let sql = String::from("SELECT * FROM customers WHERE name LIKE '%a'");
    match verified(&sql) {
        ASTNode::SQLSelect { selection, .. } => {
            assert_eq!(
                ASTNode::SQLBinaryExpr {
                    left: Box::new(ASTNode::SQLIdentifier("name".to_string())),
                    op: SQLOperator::Like,
                    right: Box::new(ASTNode::SQLValue(Value::SingleQuotedString(
                        "%a".to_string()
                    ))),
                },
                *selection.unwrap()
            );
        }
        _ => assert!(false),
    }
}

#[test]
fn parse_not_like() {
    let sql = String::from("SELECT * FROM customers WHERE name NOT LIKE '%a'");
    match verified(&sql) {
        ASTNode::SQLSelect { selection, .. } => {
            assert_eq!(
                ASTNode::SQLBinaryExpr {
                    left: Box::new(ASTNode::SQLIdentifier("name".to_string())),
                    op: SQLOperator::NotLike,
                    right: Box::new(ASTNode::SQLValue(Value::SingleQuotedString(
                        "%a".to_string()
                    ))),
                },
                *selection.unwrap()
            );
        }
        _ => assert!(false),
    }
}

#[test]
fn parse_select_order_by() {
    fn chk(sql: &str) {
        match verified(&sql) {
            ASTNode::SQLSelect { order_by, .. } => {
                assert_eq!(
                    Some(vec![
                        SQLOrderByExpr {
                            expr: Box::new(ASTNode::SQLIdentifier("lname".to_string())),
                            asc: Some(true),
                        },
                        SQLOrderByExpr {
                            expr: Box::new(ASTNode::SQLIdentifier("fname".to_string())),
                            asc: Some(false),
                        },
                        SQLOrderByExpr {
                            expr: Box::new(ASTNode::SQLIdentifier("id".to_string())),
                            asc: None,
                        },
                    ]),
                    order_by
                );
            }
            _ => assert!(false),
        }
    }
    chk("SELECT id, fname, lname FROM customer WHERE id < 5 ORDER BY lname ASC, fname DESC, id");
    // make sure ORDER is not treated as an alias
    chk("SELECT id, fname, lname FROM customer ORDER BY lname ASC, fname DESC, id");
}

#[test]
fn parse_select_order_by_limit() {
    let sql = String::from(
        "SELECT id, fname, lname FROM customer WHERE id < 5 ORDER BY lname ASC, fname DESC LIMIT 2",
    );
    let ast = parse_sql(&sql);
    match ast {
        ASTNode::SQLSelect {
            order_by, limit, ..
        } => {
            assert_eq!(
                Some(vec![
                    SQLOrderByExpr {
                        expr: Box::new(ASTNode::SQLIdentifier("lname".to_string())),
                        asc: Some(true),
                    },
                    SQLOrderByExpr {
                        expr: Box::new(ASTNode::SQLIdentifier("fname".to_string())),
                        asc: Some(false),
                    },
                ]),
                order_by
            );
            assert_eq!(Some(Box::new(ASTNode::SQLValue(Value::Long(2)))), limit);
        }
        _ => assert!(false),
    }
}

#[test]
fn parse_select_group_by() {
    let sql = String::from("SELECT id, fname, lname FROM customer GROUP BY lname, fname");
    match verified(&sql) {
        ASTNode::SQLSelect { group_by, .. } => {
            assert_eq!(
                Some(vec![
                    ASTNode::SQLIdentifier("lname".to_string()),
                    ASTNode::SQLIdentifier("fname".to_string()),
                ]),
                group_by
            );
        }
        _ => assert!(false),
    }
}

#[test]
fn parse_limit_accepts_all() {
    parses_to(
        "SELECT id, fname, lname FROM customer WHERE id = 1 LIMIT ALL",
        "SELECT id, fname, lname FROM customer WHERE id = 1",
    );
}

#[test]
fn parse_cast() {
    let sql = String::from("SELECT CAST(id AS bigint) FROM customer");
    match verified(&sql) {
        ASTNode::SQLSelect { projection, .. } => {
            assert_eq!(1, projection.len());
            assert_eq!(
                ASTNode::SQLCast {
                    expr: Box::new(ASTNode::SQLIdentifier("id".to_string())),
                    data_type: SQLType::BigInt
                },
                projection[0]
            );
        }
        _ => assert!(false),
    }
    parses_to(
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
    parses_to(
        &sql,
        "CREATE TABLE uk_cities (\
         name character varying(100) NOT NULL, \
         lat double, \
         lng double)",
    );
    match parse_sql(&sql) {
        ASTNode::SQLCreateTable { name, columns } => {
            assert_eq!("uk_cities", name);
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
fn parse_scalar_function_in_projection() {
    let sql = String::from("SELECT sqrt(id) FROM foo");
    match verified(&sql) {
        ASTNode::SQLSelect { projection, .. } => {
            assert_eq!(
                vec![ASTNode::SQLFunction {
                    id: String::from("sqrt"),
                    args: vec![ASTNode::SQLIdentifier(String::from("id"))],
                }],
                projection
            );
        }
        _ => assert!(false),
    }
}

#[test]
fn parse_aggregate_with_group_by() {
    let sql = String::from("SELECT a, COUNT(1), MIN(b), MAX(b) FROM foo GROUP BY a");
    let _ast = verified(&sql);
    //TODO: assertions
}

#[test]
fn parse_literal_string() {
    let sql = "SELECT 'one'";
    match verified(&sql) {
        ASTNode::SQLSelect { ref projection, .. } => {
            assert_eq!(
                projection[0],
                ASTNode::SQLValue(Value::SingleQuotedString("one".to_string()))
            );
        }
        _ => panic!(),
    }
}

#[test]
fn parse_simple_math_expr_plus() {
    let sql = "SELECT a + b, 2 + a, 2.5 + a, a_f + b_f, 2 + a_f, 2.5 + a_f FROM c";
    parse_sql(&sql);
}

#[test]
fn parse_simple_math_expr_minus() {
    let sql = "SELECT a - b, 2 - a, 2.5 - a, a_f - b_f, 2 - a_f, 2.5 - a_f FROM c";
    parse_sql(&sql);
}

#[test]
fn parse_select_version() {
    let sql = "SELECT @@version";
    match verified(&sql) {
        ASTNode::SQLSelect { ref projection, .. } => {
            assert_eq!(
                projection[0],
                ASTNode::SQLIdentifier("@@version".to_string())
            );
        }
        _ => panic!(),
    }
}

#[test]
fn parse_parens() {
    use self::ASTNode::*;
    use self::SQLOperator::*;
    let sql = "(a + b) - (c + d)";
    let ast = parse_sql(&sql);
    assert_eq!(
        SQLBinaryExpr {
            left: Box::new(SQLBinaryExpr {
                left: Box::new(SQLIdentifier("a".to_string())),
                op: Plus,
                right: Box::new(SQLIdentifier("b".to_string()))
            }),
            op: Minus,
            right: Box::new(SQLBinaryExpr {
                left: Box::new(SQLIdentifier("c".to_string())),
                op: Plus,
                right: Box::new(SQLIdentifier("d".to_string()))
            })
        },
        ast
    );
}

#[test]
fn parse_case_expression() {
    let sql = "SELECT CASE WHEN bar IS NULL THEN 'null' WHEN bar = 0 THEN '=0' WHEN bar >= 0 THEN '>=0' ELSE '<0' END FROM foo";
    let ast = parse_sql(&sql);
    assert_eq!(sql, ast.to_string());
    use self::ASTNode::*;
    use self::SQLOperator::*;
    match ast {
        ASTNode::SQLSelect { projection, .. } => {
            assert_eq!(1, projection.len());
            assert_eq!(
                SQLCase {
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
                projection[0]
            );
        }
        _ => assert!(false),
    }
}

#[test]
fn parse_select_with_semi_colon() {
    let sql = String::from("SELECT id, fname, lname FROM customer WHERE id = 1;");
    let ast = parse_sql(&sql);
    match ast {
        ASTNode::SQLSelect { projection, .. } => {
            assert_eq!(3, projection.len());
        }
        _ => assert!(false),
    }
}

#[test]
fn parse_delete_with_semi_colon() {
    let sql: &str = "DELETE FROM 'table';";

    match parse_sql(&sql) {
        ASTNode::SQLDelete { relation, .. } => {
            assert_eq!(
                Some(Box::new(ASTNode::SQLValue(Value::SingleQuotedString(
                    "table".to_string()
                )))),
                relation
            );
        }
        _ => assert!(false),
    }
}

#[test]
fn parse_implicit_join() {
    let sql = "SELECT * FROM t1, t2";

    match verified(sql) {
        ASTNode::SQLSelect { joins, .. } => {
            assert_eq!(joins.len(), 1);
            assert_eq!(
                joins[0],
                Join {
                    relation: ASTNode::TableFactor {
                        relation: Box::new(ASTNode::SQLCompoundIdentifier(vec!["t2".to_string()])),
                        alias: None,
                    },
                    join_operator: JoinOperator::Implicit
                }
            )
        }
        _ => assert!(false),
    }
}

#[test]
fn parse_cross_join() {
    let sql = "SELECT * FROM t1 CROSS JOIN t2";

    match verified(sql) {
        ASTNode::SQLSelect { joins, .. } => {
            assert_eq!(joins.len(), 1);
            assert_eq!(
                joins[0],
                Join {
                    relation: ASTNode::TableFactor {
                        relation: Box::new(ASTNode::SQLCompoundIdentifier(vec!["t2".to_string()])),
                        alias: None,
                    },
                    join_operator: JoinOperator::Cross
                }
            )
        }
        _ => assert!(false),
    }
}

#[test]
fn parse_joins_on() {
    fn join_with_constraint(
        relation: impl Into<String>,
        alias: Option<SQLIdent>,
        f: impl Fn(JoinConstraint) -> JoinOperator,
    ) -> Join {
        Join {
            relation: ASTNode::TableFactor {
                relation: Box::new(ASTNode::SQLCompoundIdentifier(vec![relation.into()])),
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
        joins_from(verified("SELECT * FROM t1 JOIN t2 AS foo ON c1 = c2")),
        vec![join_with_constraint(
            "t2",
            Some("foo".to_string()),
            JoinOperator::Inner
        )]
    );
    parses_to(
        "SELECT * FROM t1 JOIN t2 foo ON c1 = c2",
        "SELECT * FROM t1 JOIN t2 AS foo ON c1 = c2",
    );
    // Test parsing of different join operators
    assert_eq!(
        joins_from(verified("SELECT * FROM t1 JOIN t2 ON c1 = c2")),
        vec![join_with_constraint("t2", None, JoinOperator::Inner)]
    );
    assert_eq!(
        joins_from(verified("SELECT * FROM t1 LEFT JOIN t2 ON c1 = c2")),
        vec![join_with_constraint("t2", None, JoinOperator::LeftOuter)]
    );
    assert_eq!(
        joins_from(verified("SELECT * FROM t1 RIGHT JOIN t2 ON c1 = c2")),
        vec![join_with_constraint("t2", None, JoinOperator::RightOuter)]
    );
    assert_eq!(
        joins_from(verified("SELECT * FROM t1 FULL JOIN t2 ON c1 = c2")),
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
            relation: ASTNode::TableFactor {
                relation: Box::new(ASTNode::SQLCompoundIdentifier(vec![relation.into()])),
                alias,
            },
            join_operator: f(JoinConstraint::Using(vec!["c1".into()])),
        }
    }
    // Test parsing of aliases
    assert_eq!(
        joins_from(verified("SELECT * FROM t1 JOIN t2 AS foo USING(c1)")),
        vec![join_with_constraint(
            "t2",
            Some("foo".to_string()),
            JoinOperator::Inner
        )]
    );
    parses_to(
        "SELECT * FROM t1 JOIN t2 foo USING(c1)",
        "SELECT * FROM t1 JOIN t2 AS foo USING(c1)",
    );
    // Test parsing of different join operators
    assert_eq!(
        joins_from(verified("SELECT * FROM t1 JOIN t2 USING(c1)")),
        vec![join_with_constraint("t2", None, JoinOperator::Inner)]
    );
    assert_eq!(
        joins_from(verified("SELECT * FROM t1 LEFT JOIN t2 USING(c1)")),
        vec![join_with_constraint("t2", None, JoinOperator::LeftOuter)]
    );
    assert_eq!(
        joins_from(verified("SELECT * FROM t1 RIGHT JOIN t2 USING(c1)")),
        vec![join_with_constraint("t2", None, JoinOperator::RightOuter)]
    );
    assert_eq!(
        joins_from(verified("SELECT * FROM t1 FULL JOIN t2 USING(c1)")),
        vec![join_with_constraint("t2", None, JoinOperator::FullOuter)]
    );
}

#[test]
fn parse_complex_join() {
    let sql = "SELECT c1, c2 FROM t1, t4 JOIN t2 ON t2.c = t1.c LEFT JOIN t3 USING(q, c) WHERE t4.c = t1.c";
    assert_eq!(sql, parse_sql(sql).to_string());
}

#[test]
fn parse_join_syntax_variants() {
    parses_to(
        "SELECT c1 FROM t1 INNER JOIN t2 USING(c1)",
        "SELECT c1 FROM t1 JOIN t2 USING(c1)",
    );
    parses_to(
        "SELECT c1 FROM t1 LEFT OUTER JOIN t2 USING(c1)",
        "SELECT c1 FROM t1 LEFT JOIN t2 USING(c1)",
    );
    parses_to(
        "SELECT c1 FROM t1 RIGHT OUTER JOIN t2 USING(c1)",
        "SELECT c1 FROM t1 RIGHT JOIN t2 USING(c1)",
    );
    parses_to(
        "SELECT c1 FROM t1 FULL OUTER JOIN t2 USING(c1)",
        "SELECT c1 FROM t1 FULL JOIN t2 USING(c1)",
    );
}

fn verified(query: &str) -> ASTNode {
    let ast = parse_sql(query);
    assert_eq!(query, &ast.to_string());
    ast
}

fn parses_to(from: &str, to: &str) {
    assert_eq!(to, &parse_sql(from).to_string())
}

fn joins_from(ast: ASTNode) -> Vec<Join> {
    match ast {
        ASTNode::SQLSelect { joins, .. } => joins,
        _ => panic!("Expected SELECT"),
    }
}

fn parse_sql(sql: &str) -> ASTNode {
    let generic_ast = parse_sql_with(sql, &GenericSqlDialect {});
    let pg_ast = parse_sql_with(sql, &PostgreSqlDialect {});
    assert_eq!(generic_ast, pg_ast);
    generic_ast
}

fn parse_sql_with(sql: &str, dialect: &Dialect) -> ASTNode {
    let mut tokenizer = Tokenizer::new(dialect, &sql);
    let tokens = tokenizer.tokenize().unwrap();
    let mut parser = Parser::new(tokens);
    let ast = parser.parse().unwrap();
    ast
}

extern crate log;
extern crate sqlparser;

use sqlparser::dialect::*;
use sqlparser::sqlast::*;
use sqlparser::sqlparser::*;
use sqlparser::sqltokenizer::*;

#[test]
fn parse_delete_statement() {
    let sql: &str = "DELETE FROM 'table'";

    match verified_stmt(&sql) {
        SQLStatement::SQLDelete { relation, .. } => {
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

    match verified_stmt(&sql) {
        SQLStatement::SQLDelete {
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
    match verified_stmt(&sql) {
        SQLStatement::SQLSelect(SQLSelect {
            projection, limit, ..
        }) => {
            assert_eq!(3, projection.len());
            assert_eq!(Some(Box::new(ASTNode::SQLValue(Value::Long(5)))), limit);
        }
        _ => assert!(false),
    }
}

#[test]
fn parse_select_wildcard() {
    let sql = String::from("SELECT * FROM customer");
    match verified_stmt(&sql) {
        SQLStatement::SQLSelect(SQLSelect { projection, .. }) => {
            assert_eq!(1, projection.len());
            assert_eq!(ASTNode::SQLWildcard, projection[0]);
        }
        _ => assert!(false),
    }
}

#[test]
fn parse_select_count_wildcard() {
    let sql = String::from("SELECT COUNT(*) FROM customer");
    match verified_stmt(&sql) {
        SQLStatement::SQLSelect(SQLSelect { projection, .. }) => {
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
fn parse_like() {
    let sql = String::from("SELECT * FROM customers WHERE name LIKE '%a'");
    match verified_stmt(&sql) {
        SQLStatement::SQLSelect(SQLSelect { selection, .. }) => {
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
    match verified_stmt(&sql) {
        SQLStatement::SQLSelect(SQLSelect { selection, .. }) => {
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
        match verified_stmt(&sql) {
            SQLStatement::SQLSelect(SQLSelect { order_by, .. }) => {
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
    match verified_stmt(&sql) {
        SQLStatement::SQLSelect(SQLSelect {
            order_by, limit, ..
        }) => {
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
    match verified_stmt(&sql) {
        SQLStatement::SQLSelect(SQLSelect { group_by, .. }) => {
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
        SQLStatement::SQLCreateTable { name, columns } => {
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
    let sql = "SELECT 'one'";
    let select = verified_only_select(sql);
    assert_eq!(1, select.projection.len());
    assert_eq!(
        &ASTNode::SQLValue(Value::SingleQuotedString("one".to_string())),
        expr_from_projection(&select.projection[0])
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

    match verified_stmt(sql) {
        SQLStatement::SQLSelect(SQLSelect { joins, .. }) => {
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

    match verified_stmt(sql) {
        SQLStatement::SQLSelect(SQLSelect { joins, .. }) => {
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
            relation: ASTNode::TableFactor {
                relation: Box::new(ASTNode::SQLCompoundIdentifier(vec![relation.into()])),
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
    test_with("DELETE FROM foo", "SELECT", " bar");
    test_with("INSERT INTO foo VALUES(1)", "SELECT", " bar");
    test_with("CREATE TABLE foo (baz int)", "SELECT", " bar");
    // Make sure that empty statements do not cause an error:
    let res = parse_sql_statements(";;");
    assert_eq!(0, res.unwrap().len());
}

fn only<'a, T>(v: &'a Vec<T>) -> &'a T {
    assert_eq!(1, v.len());
    v.first().unwrap()
}

fn verified_query(query: &str) -> SQLSelect {
    match verified_stmt(query) {
        SQLStatement::SQLSelect(select) => select,
        _ => panic!("Expected SELECT"),
    }
}

fn expr_from_projection(item: &ASTNode) -> &ASTNode {
    item // Will be changed later to extract expression from `expr AS alias` struct
}

fn verified_only_select(query: &str) -> SQLSelect {
    verified_query(query)
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

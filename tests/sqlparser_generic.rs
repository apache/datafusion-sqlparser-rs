extern crate log;
extern crate sqlparser;

use sqlparser::dialect::GenericSqlDialect;
use sqlparser::sqlast::*;
use sqlparser::sqlparser::*;
use sqlparser::sqltokenizer::*;

#[test]
fn parse_delete_statement() {
    let sql: &str = "DELETE FROM 'table'";

    match parse_sql(&sql) {
        ASTNode::SQLDelete { relation, .. } => {
            assert_eq!(
                Some(Box::new(ASTNode::SQLValue(Value::SingleQuotedString("table".to_string())))),
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

    match parse_sql(&sql) {
        ASTNode::SQLDelete {
            relation,
            selection,
            ..
        } => {
            assert_eq!(
                Some(Box::new(ASTNode::SQLValue(Value::SingleQuotedString("table".to_string())))),
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
    let ast = parse_sql(&sql);
    match ast {
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
    let ast = parse_sql(&sql);
    match ast {
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
    let ast = parse_sql(&sql);
    match ast {
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
fn parse_select_string_predicate() {
    let sql = String::from(
        "SELECT id, fname, lname FROM customer \
         WHERE salary != 'Not Provided' AND salary != ''",
    );
    let _ast = parse_sql(&sql);
    //TODO: add assertions
}

#[test]
fn parse_projection_nested_type() {
    let sql = String::from("SELECT customer.address.state FROM foo");
    let _ast = parse_sql(&sql);
    //TODO: add assertions
}

#[test]
fn parse_compound_expr_1() {
    use self::ASTNode::*;
    use self::SQLOperator::*;
    let sql = String::from("a + b * c");
    let ast = parse_sql(&sql);
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
        ast
    );
}

#[test]
fn parse_compound_expr_2() {
    use self::ASTNode::*;
    use self::SQLOperator::*;
    let sql = String::from("a * b + c");
    let ast = parse_sql(&sql);
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
        ast
    );
}

#[test]
fn parse_is_null() {
    use self::ASTNode::*;
    let sql = String::from("a IS NULL");
    let ast = parse_sql(&sql);
    assert_eq!(SQLIsNull(Box::new(SQLIdentifier("a".to_string()))), ast);
}

#[test]
fn parse_is_not_null() {
    use self::ASTNode::*;
    let sql = String::from("a IS NOT NULL");
    let ast = parse_sql(&sql);
    assert_eq!(SQLIsNotNull(Box::new(SQLIdentifier("a".to_string()))), ast);
}

#[test]
fn parse_select_order_by() {
    let sql = String::from(
        "SELECT id, fname, lname FROM customer WHERE id < 5 ORDER BY lname ASC, fname DESC",
    );
    let ast = parse_sql(&sql);
    match ast {
        ASTNode::SQLSelect { order_by, .. } => {
            assert_eq!(
                Some(vec![
                    SQLOrderByExpr {
                        expr: Box::new(ASTNode::SQLIdentifier("lname".to_string())),
                        asc: true,
                    },
                    SQLOrderByExpr {
                        expr: Box::new(ASTNode::SQLIdentifier("fname".to_string())),
                        asc: false,
                    },
                ]),
                order_by
            );
        }
        _ => assert!(false),
    }
}

#[test]
fn parse_select_order_by_limit() {
    let sql = String::from(
        "SELECT id, fname, lname FROM customer WHERE id < 5 ORDER BY lname ASC, fname DESC LIMIT 2",
    );
    let ast = parse_sql(&sql);
    match ast {
        ASTNode::SQLSelect { order_by, limit, .. } => {
            assert_eq!(
                Some(vec![
                    SQLOrderByExpr {
                        expr: Box::new(ASTNode::SQLIdentifier("lname".to_string())),
                        asc: true,
                    },
                    SQLOrderByExpr {
                        expr: Box::new(ASTNode::SQLIdentifier("fname".to_string())),
                        asc: false,
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
    let ast = parse_sql(&sql);
    match ast {
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
    let sql = String::from("SELECT id, fname, lname FROM customer WHERE id = 1 LIMIT ALL");
    let ast = parse_sql(&sql);
    match ast {
        ASTNode::SQLSelect {
            projection, limit, ..
        } => {
            assert_eq!(3, projection.len());
            assert_eq!(None, limit);
        }
        _ => assert!(false),
    }
}

#[test]
fn parse_cast() {
    let sql = String::from("SELECT CAST(id AS BIGINT) FROM customer");
    let ast = parse_sql(&sql);
    match ast {
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
}

#[test]
fn parse_create_table() {
    let sql = String::from(
        "CREATE TABLE uk_cities (\
         name VARCHAR(100) NOT NULL,\
         lat DOUBLE NULL,\
         lng DOUBLE NULL)",
    );
    let ast = parse_sql(&sql);
    match ast {
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
    let ast = parse_sql(&sql);
    if let ASTNode::SQLSelect { projection, .. } = ast {
        assert_eq!(
            vec![ASTNode::SQLFunction {
                id: String::from("sqrt"),
                args: vec![ASTNode::SQLIdentifier(String::from("id"))],
            }],
            projection
        );
    } else {
        assert!(false);
    }
}

#[test]
fn parse_aggregate_with_group_by() {
    let sql = String::from("SELECT a, COUNT(1), MIN(b), MAX(b) FROM foo GROUP BY a");
    let _ast = parse_sql(&sql);
    //TODO: assertions
}

#[test]
fn parse_literal_string() {
    let sql = "SELECT 'one'";
    match parse_sql(&sql) {
        ASTNode::SQLSelect { ref projection, .. } => {
            assert_eq!(projection[0], ASTNode::SQLValue(Value::SingleQuotedString("one".to_string())));
        }
        _ => panic!(),
    }
}

#[test]
fn parse_select_version() {
    let sql = "SELECT @@version";
    match parse_sql(&sql) {
        ASTNode::SQLSelect { ref projection, .. } => {
            assert_eq!(
                projection[0],
                ASTNode::SQLIdentifier("@@version".to_string())
            );
        }
        _ => panic!(),
    }
}

fn parse_sql(sql: &str) -> ASTNode {
    let dialect = GenericSqlDialect {};
    let mut tokenizer = Tokenizer::new(&dialect,&sql, );
    let tokens = tokenizer.tokenize().unwrap();
    let mut parser = Parser::new(tokens);
    let ast = parser.parse().unwrap();
    ast
}


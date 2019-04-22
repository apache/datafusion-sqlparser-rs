extern crate log;
extern crate sqlparser;

use sqlparser::dialect::AnsiSqlDialect;
use sqlparser::sqlast::*;
use sqlparser::sqlparser::*;

#[test]
fn parse_simple_select() {
    let sql = String::from("SELECT id, fname, lname FROM customer WHERE id = 1");
    let ast = Parser::parse_sql(&AnsiSqlDialect {}, sql).unwrap();
    assert_eq!(1, ast.len());
    match ast.first().unwrap() {
        SQLStatement::SQLSelect(SQLQuery {
            body: SQLSetExpr::Select(SQLSelect { projection, .. }),
            ..
        }) => {
            assert_eq!(3, projection.len());
        }
        _ => assert!(false),
    }
}

#[test]
fn roundtrip_qualified_wildcard() {
    let sql = String::from("SELECT COUNT(Employee.*) FROM northwind.Order JOIN northwind.Employee ON Order.employee = Employee.id");
    let ast = Parser::parse_sql(&AnsiSqlDialect {}, sql.clone()).unwrap();
    assert_eq!(1, ast.len());
    match ast.first().unwrap() {
        SQLStatement::SQLSelect(ref query) => {
            assert_eq!(sql, query.to_string());
        }
        _ => assert!(false),
    }
}

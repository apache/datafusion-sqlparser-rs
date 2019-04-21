#![warn(clippy::all)]

use sqlparser::dialect::AnsiSqlDialect;
use sqlparser::sqlast::*;
use sqlparser::sqlparser::*;

#[test]
fn parse_simple_select() {
    let sql = String::from("SELECT id, fname, lname FROM customer WHERE id = 1");
    let mut ast = Parser::parse_sql(&AnsiSqlDialect {}, sql).unwrap();
    assert_eq!(1, ast.len());
    match ast.pop().unwrap() {
        SQLStatement::SQLQuery(q) => match *q {
            SQLQuery {
                body: SQLSetExpr::Select(select),
                ..
            } => {
                assert_eq!(3, select.projection.len());
            }
            _ => unreachable!(),
        },
        _ => unreachable!(),
    }
}

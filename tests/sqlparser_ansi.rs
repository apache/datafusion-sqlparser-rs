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
        _ => unreachable!(),
    }
}

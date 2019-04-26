extern crate sqlparser;

use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::sqlparser::*;

fn main() {

    let sql = "INSERT INTO users \
                 (id, type) \
                 VALUES \
                 ($1, $2)";
    let dialect = PostgreSqlDialect {};

    let ast = Parser::parse_sql(&dialect, sql.to_string()).unwrap();

    println!("AST: {:?}", ast);
}

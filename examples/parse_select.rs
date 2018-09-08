extern crate sqlparser;

use sqlparser::dialect::GenericSqlDialect;
use sqlparser::sqlparser::*;

fn main() {
    let sql = "SELECT a, b, 123, myfunc(b) \
               FROM table_1 \
               WHERE a > b AND b < 100 \
               ORDER BY a DESC, b";

    let dialect = GenericSqlDialect{};

    let ast = Parser::parse_sql(&dialect,sql.to_string()).unwrap();

    println!("AST: {:?}", ast);
}

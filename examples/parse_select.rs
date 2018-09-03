extern crate sqlparser;

use sqlparser::sqlparser::*;

fn main() {

    let sql = "SELECT a, b, 123, myfunc(b) \
        FROM table_1 \
        WHERE a > b AND b < 100 \
        ORDER BY a DESC, b";

    let ast = Parser::parse_sql(sql.to_string()).unwrap();

    println!("AST: {:?}", ast);
}
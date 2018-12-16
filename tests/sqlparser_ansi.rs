extern crate log;
extern crate sqlparser;

use sqlparser::dialect::AnsiSqlDialect;
use sqlparser::sqlast::*;
use sqlparser::sqlparser::*;
use sqlparser::sqltokenizer::*;

#[test]
fn parse_simple_select() {
    let sql = String::from("SELECT id, fname, lname FROM customer WHERE id = 1");
    let ast = parse_sql(&sql);
    match ast {
        ASTNode::SQLSelect { projection, .. } => {
            assert_eq!(3, projection.len());
        }
        _ => assert!(false),
    }
}

fn parse_sql(sql: &str) -> ASTNode {
    let dialect = AnsiSqlDialect {};
    let mut tokenizer = Tokenizer::new(&dialect, &sql);
    let tokens = tokenizer.tokenize().unwrap();
    let mut parser = Parser::new(tokens);
    let ast = parser.parse().unwrap();
    ast
}

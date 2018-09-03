use std::sync::{Arc, Mutex};

extern crate datafusion_sql;

use datafusion_sql::ansi::tokenizer::ANSISQLTokenizer;
use datafusion_sql::ansi::parser::ANSISQLParser;
use datafusion_sql::tokenizer::*;
use datafusion_sql::parser::*;


fn main() {

    let sql = "SELECT 1 + 1";

    // Create parsers
    match ANSISQLParser::parse(sql).unwrap() {
        Some(ast) => println!("{:?}", ast),
        _ => {}
    }
}

extern crate simple_logger;
extern crate sqlparser;
///! A small command-line app to run the parser.
/// Run with `cargo run --example cli`
use std::fs;

use sqlparser::dialect::GenericSqlDialect;
use sqlparser::sqlparser::Parser;

fn main() {
    simple_logger::init().unwrap();

    let filename = std::env::args()
        .nth(1)
        .expect("No arguments provided!\n\nUsage: cargo run --example cli FILENAME.sql");

    let contents =
        fs::read_to_string(&filename).expect(&format!("Unable to read the file {}", &filename));
    let without_bom = if contents.chars().nth(0).unwrap() as u64 != 0xfeff {
        contents.as_str()
    } else {
        let mut chars = contents.chars();
        chars.next();
        chars.as_str()
    };
    println!("Input:\n'{}'", &without_bom);
    let parse_result = Parser::parse_sql(&GenericSqlDialect {}, without_bom.to_owned());
    match parse_result {
        Ok(statements) => {
            println!(
                "Round-trip:\n'{}'",
                statements
                    .iter()
                    .map(|s| s.to_string())
                    .collect::<Vec<_>>()
                    .join("\n")
            );
            println!("Parse results:\n{:#?}", statements);
            std::process::exit(0);
        }
        Err(e) => {
            println!("Error during parsing: {:?}", e);
            std::process::exit(1);
        }
    }
}

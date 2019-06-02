#![warn(clippy::all)]

use simple_logger;

///! A small command-line app to run the parser.
/// Run with `cargo run --example cli`
use std::fs;

use sqlparser::dialect::*;
use sqlparser::sqlparser::Parser;

fn main() {
    simple_logger::init().unwrap();

    let filename = std::env::args().nth(1).expect(
        "No arguments provided!\n\n\
         Usage: cargo run --example cli FILENAME.sql [--dialectname]",
    );

    let dialect: Box<dyn Dialect> = match std::env::args().nth(2).unwrap_or_default().as_ref() {
        "--ansi" => Box::new(AnsiSqlDialect {}),
        "--postgres" => Box::new(PostgreSqlDialect {}),
        "--ms" => Box::new(MsSqlDialect {}),
        "--generic" | "" => Box::new(GenericSqlDialect {}),
        s => panic!("Unexpected parameter: {}", s),
    };

    println!("Parsing from file '{}' using {:?}", &filename, dialect);
    let contents = fs::read_to_string(&filename)
        .unwrap_or_else(|_| panic!("Unable to read the file {}", &filename));
    let without_bom = if contents.chars().nth(0).unwrap() as u64 != 0xfeff {
        contents.as_str()
    } else {
        let mut chars = contents.chars();
        chars.next();
        chars.as_str()
    };
    let parse_result = Parser::parse_sql(&*dialect, without_bom.to_owned());
    match parse_result {
        Ok(statements) => {
            println!(
                "Round-trip:\n'{}'",
                statements
                    .iter()
                    .map(std::string::ToString::to_string)
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

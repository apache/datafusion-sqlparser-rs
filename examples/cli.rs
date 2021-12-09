// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![warn(clippy::all)]

/// A small command-line app to run the parser.
/// Run with `cargo run --example cli`
use std::fs;

use simple_logger::SimpleLogger;
use sqlparser::dialect::*;
use sqlparser::parser::Parser;

fn main() {
    let filename = std::env::args().nth(1).expect(
        r#"
No arguments provided!

Usage:
$ cargo run --example cli FILENAME.sql [--dialectname] [--quiet]

To print the parse results as JSON:
$ cargo run --features json_example --example cli FILENAME.sql [--dialectname] [--quiet]

Pass --quiet to emit only the parse results.

"#,
    );

    let mut optional_args = std::env::args().skip(2).collect::<Vec<_>>();

    let quiet = if let Some(pos) = optional_args.iter().position(|a| *a == "--quiet") {
        optional_args.remove(pos);
        true
    } else {
        false
    };

    let dialect: Box<dyn Dialect> = match optional_args.first().unwrap_or(&"".into()).as_ref() {
        "--ansi" => Box::new(AnsiDialect {}),
        "--bigquery" => Box::new(BigQueryDialect {}),
        "--postgres" => Box::new(PostgreSqlDialect {}),
        "--ms" => Box::new(MsSqlDialect {}),
        "--mysql" => Box::new(MySqlDialect {}),
        "--snowflake" => Box::new(SnowflakeDialect {}),
        "--hive" => Box::new(HiveDialect {}),
        "--redshift" => Box::new(RedshiftSqlDialect {}),
        "--generic" | "" => Box::new(GenericDialect {}),
        s => panic!("Unexpected parameter: {}", s),
    };

    if !quiet {
        SimpleLogger::new().init().unwrap();
    }

    if !quiet {
        println!("Parsing from file '{}' using {:?}", &filename, dialect);
    }
    let contents = fs::read_to_string(&filename)
        .unwrap_or_else(|_| panic!("Unable to read the file {}", &filename));
    let without_bom = if contents.chars().next().unwrap() as u64 != 0xfeff {
        contents.as_str()
    } else {
        let mut chars = contents.chars();
        chars.next();
        chars.as_str()
    };
    let parse_result = Parser::parse_sql(&*dialect, without_bom);
    match parse_result {
        Ok(statements) => {
            if !quiet {
                println!(
                    "Round-trip:\n'{}'",
                    statements
                        .iter()
                        .map(std::string::ToString::to_string)
                        .collect::<Vec<_>>()
                        .join("\n")
                );
            }

            if cfg!(feature = "json_example") {
                #[cfg(feature = "json_example")]
                {
                    if !quiet {
                        println!("Serialized as JSON:");
                    }
                    println!("{}", serde_json::to_string_pretty(&statements).unwrap());
                }
            } else {
                if !quiet {
                    println!("Parse results:");
                }
                println!("{:#?}", statements);
            }

            std::process::exit(0);
        }
        Err(e) => {
            println!("Error during parsing: {:?}", e);
            std::process::exit(1);
        }
    }
}

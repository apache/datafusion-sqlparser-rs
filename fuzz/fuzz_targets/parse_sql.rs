#![no_main]
use libfuzzer_sys::fuzz_target;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

fuzz_target!(|data: String| {
    let dialect = GenericDialect {};

    let _ = Parser::parse_sql(&dialect, &data);
});

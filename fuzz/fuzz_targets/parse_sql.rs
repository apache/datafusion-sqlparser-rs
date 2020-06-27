#![no_main]
use libfuzzer_sys::fuzz_target;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

fuzz_target!(|data: &[u8]| {
    let dialect = GenericDialect {};

    if let Ok(s) = std::str::from_utf8(data) {
        let _ = Parser::parse_sql(&dialect, s);
    }
});

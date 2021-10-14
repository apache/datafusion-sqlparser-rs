use honggfuzz::fuzz;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

fn main() {
    loop {
        fuzz!(|data: String| {
            let dialect = GenericDialect {};
            let _ = Parser::parse_sql(&dialect, &data);
        });
    }
}

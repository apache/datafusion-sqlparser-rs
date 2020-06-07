use criterion::{criterion_group, criterion_main, Criterion};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

fn basic_queries(c: &mut Criterion) {
    let mut group = c.benchmark_group("sqlparser-rs parsing benchmark");
    let dialect = GenericDialect {};

    let string = "SELECT * FROM table WHERE 1 = 1";
    group.bench_function("sqlparser::select", |b| {
        b.iter(|| Parser::parse_sql(&dialect, string));
    });

    let with_query = "
        WITH derived AS (
            SELECT MAX(a) AS max_a,
                   COUNT(b) AS b_num,
                   user_id
            FROM TABLE
            GROUP BY user_id
        )
        SELECT * FROM table
        LEFT JOIN derived USING (user_id)
    ";
    group.bench_function("sqlparser::with_select", |b| {
        b.iter(|| Parser::parse_sql(&dialect, with_query));
    });
}

criterion_group!(benches, basic_queries);
criterion_main!(benches);

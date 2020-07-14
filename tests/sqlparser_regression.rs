use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

macro_rules! tpch_tests {
    ($($name:ident: $value:expr,)*) => {
        const QUERIES: &[&str] = &[
            $(include_str!(concat!("queries/tpch/", $value, ".sql"))),*
        ];
    $(

        #[test]
        fn $name() {
            let dialect = GenericDialect {};

            let res = Parser::parse_sql(&dialect, QUERIES[$value -1]);
            // Ignore 6.sql and 22.sql
            if $value != 6 && $value != 22 {
                assert!(res.is_ok());
            }
        }
    )*
    }
}

tpch_tests! {
    tpch_1: 1,
    tpch_2: 2,
    tpch_3: 3,
    tpch_4: 4,
    tpch_5: 5,
    tpch_6: 6,
    tpch_7: 7,
    tpch_8: 8,
    tpch_9: 9,
    tpch_10: 10,
    tpch_11: 11,
    tpch_12: 12,
    tpch_13: 13,
    tpch_14: 14,
    tpch_15: 15,
    tpch_16: 16,
    tpch_17: 17,
    tpch_18: 18,
    tpch_19: 19,
    tpch_20: 20,
    tpch_21: 21,
    tpch_22: 22,
}

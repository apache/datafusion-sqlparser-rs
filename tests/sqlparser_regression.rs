use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

macro_rules! tpch_test {
    ( $( $query:literal ),* ) => {
        const QUERIES: &[&str] = &[
            $(include_str!(concat!("queries/tpch/", $query, ".sql"))),*
        ];
    }
}

macro_rules! tpch_tests {
    ($($name:ident: $value:expr,)*) => {
    $(
        #[test]
        fn $name() {
            let dialect = GenericDialect {};

            let res = Parser::parse_sql(&dialect, QUERIES[$value -1]);
    
            assert!(res.is_ok());
        }
    )*
    }
}

tpch_test!(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22);

tpch_tests! {
    tpch_1: 1,
    tpch_2: 2,
    tpch_3: 3,
    tpch_4: 4,
    tpch_5: 5,
    //tpch_6: 6,
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
    //tpch_22: 22,
}


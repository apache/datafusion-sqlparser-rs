use crate::test_utils::{all_dialects, TestedDialects};

use super::*;

#[test]
fn test_prev_index() {
    let sql = "SELECT version";
    all_dialects().run_parser_method(sql, |parser| {
        assert_eq!(parser.peek_token(), Token::make_keyword("SELECT"));
        assert_eq!(parser.next_token(), Token::make_keyword("SELECT"));
        parser.prev_token();
        assert_eq!(parser.next_token(), Token::make_keyword("SELECT"));
        assert_eq!(parser.next_token(), Token::make_word("version", None));
        parser.prev_token();
        assert_eq!(parser.peek_token(), Token::make_word("version", None));
        assert_eq!(parser.next_token(), Token::make_word("version", None));
        assert_eq!(parser.peek_token(), Token::EOF);
        parser.prev_token();
        assert_eq!(parser.next_token(), Token::make_word("version", None));
        assert_eq!(parser.next_token(), Token::EOF);
        assert_eq!(parser.next_token(), Token::EOF);
        parser.prev_token();
    });
}

#[test]
fn test_peek_tokens() {
    all_dialects().run_parser_method("SELECT foo AS bar FROM baz", |parser| {
        assert!(matches!(
            parser.peek_tokens(),
            [Token::Word(Word {
                keyword: Keyword::SELECT,
                ..
            })]
        ));

        assert!(matches!(
            parser.peek_tokens(),
            [
                Token::Word(Word {
                    keyword: Keyword::SELECT,
                    ..
                }),
                Token::Word(_),
                Token::Word(Word {
                    keyword: Keyword::AS,
                    ..
                }),
            ]
        ));

        for _ in 0..4 {
            parser.next_token();
        }

        assert!(matches!(
            parser.peek_tokens(),
            [
                Token::Word(Word {
                    keyword: Keyword::FROM,
                    ..
                }),
                Token::Word(_),
                Token::EOF,
                Token::EOF,
            ]
        ))
    })
}

#[cfg(test)]
mod test_parse_data_type {
    use crate::ast::{
        CharLengthUnits, CharacterLength, DataType, ExactNumberInfo, ObjectName, TimezoneInfo,
    };
    use crate::dialect::{AnsiDialect, GenericDialect};
    use crate::test_utils::TestedDialects;

    macro_rules! test_parse_data_type {
        ($dialect:expr, $input:expr, $expected_type:expr $(,)?) => {{
            $dialect.run_parser_method(&*$input, |parser| {
                let data_type = parser.parse_data_type().unwrap();
                assert_eq!($expected_type, data_type);
                assert_eq!($input.to_string(), data_type.to_string());
            });
        }};
    }

    #[test]
    fn test_ansii_character_string_types() {
        // Character string types: <https://jakewheat.github.io/sql-overview/sql-2016-foundation-grammar.html#character-string-type>
        let dialect =
            TestedDialects::new(vec![Box::new(GenericDialect {}), Box::new(AnsiDialect {})]);

        test_parse_data_type!(dialect, "CHARACTER", DataType::Character(None));

        test_parse_data_type!(
            dialect,
            "CHARACTER(20)",
            DataType::Character(Some(CharacterLength::IntegerLength {
                length: 20,
                unit: None
            }))
        );

        test_parse_data_type!(
            dialect,
            "CHARACTER(20 CHARACTERS)",
            DataType::Character(Some(CharacterLength::IntegerLength {
                length: 20,
                unit: Some(CharLengthUnits::Characters)
            }))
        );

        test_parse_data_type!(
            dialect,
            "CHARACTER(20 OCTETS)",
            DataType::Character(Some(CharacterLength::IntegerLength {
                length: 20,
                unit: Some(CharLengthUnits::Octets)
            }))
        );

        test_parse_data_type!(dialect, "CHAR", DataType::Char(None));

        test_parse_data_type!(
            dialect,
            "CHAR(20)",
            DataType::Char(Some(CharacterLength::IntegerLength {
                length: 20,
                unit: None
            }))
        );

        test_parse_data_type!(
            dialect,
            "CHAR(20 CHARACTERS)",
            DataType::Char(Some(CharacterLength::IntegerLength {
                length: 20,
                unit: Some(CharLengthUnits::Characters)
            }))
        );

        test_parse_data_type!(
            dialect,
            "CHAR(20 OCTETS)",
            DataType::Char(Some(CharacterLength::IntegerLength {
                length: 20,
                unit: Some(CharLengthUnits::Octets)
            }))
        );

        test_parse_data_type!(
            dialect,
            "CHARACTER VARYING(20)",
            DataType::CharacterVarying(Some(CharacterLength::IntegerLength {
                length: 20,
                unit: None
            }))
        );

        test_parse_data_type!(
            dialect,
            "CHARACTER VARYING(20 CHARACTERS)",
            DataType::CharacterVarying(Some(CharacterLength::IntegerLength {
                length: 20,
                unit: Some(CharLengthUnits::Characters)
            }))
        );

        test_parse_data_type!(
            dialect,
            "CHARACTER VARYING(20 OCTETS)",
            DataType::CharacterVarying(Some(CharacterLength::IntegerLength {
                length: 20,
                unit: Some(CharLengthUnits::Octets)
            }))
        );

        test_parse_data_type!(
            dialect,
            "CHAR VARYING(20)",
            DataType::CharVarying(Some(CharacterLength::IntegerLength {
                length: 20,
                unit: None
            }))
        );

        test_parse_data_type!(
            dialect,
            "CHAR VARYING(20 CHARACTERS)",
            DataType::CharVarying(Some(CharacterLength::IntegerLength {
                length: 20,
                unit: Some(CharLengthUnits::Characters)
            }))
        );

        test_parse_data_type!(
            dialect,
            "CHAR VARYING(20 OCTETS)",
            DataType::CharVarying(Some(CharacterLength::IntegerLength {
                length: 20,
                unit: Some(CharLengthUnits::Octets)
            }))
        );

        test_parse_data_type!(
            dialect,
            "VARCHAR(20)",
            DataType::Varchar(Some(CharacterLength::IntegerLength {
                length: 20,
                unit: None
            }))
        );
    }

    #[test]
    fn test_ansii_character_large_object_types() {
        // Character large object types: <https://jakewheat.github.io/sql-overview/sql-2016-foundation-grammar.html#character-large-object-length>
        let dialect =
            TestedDialects::new(vec![Box::new(GenericDialect {}), Box::new(AnsiDialect {})]);

        test_parse_data_type!(
            dialect,
            "CHARACTER LARGE OBJECT",
            DataType::CharacterLargeObject(None)
        );
        test_parse_data_type!(
            dialect,
            "CHARACTER LARGE OBJECT(20)",
            DataType::CharacterLargeObject(Some(20))
        );

        test_parse_data_type!(
            dialect,
            "CHAR LARGE OBJECT",
            DataType::CharLargeObject(None)
        );
        test_parse_data_type!(
            dialect,
            "CHAR LARGE OBJECT(20)",
            DataType::CharLargeObject(Some(20))
        );

        test_parse_data_type!(dialect, "CLOB", DataType::Clob(None));
        test_parse_data_type!(dialect, "CLOB(20)", DataType::Clob(Some(20)));
    }

    #[test]
    fn test_parse_custom_types() {
        let dialect =
            TestedDialects::new(vec![Box::new(GenericDialect {}), Box::new(AnsiDialect {})]);

        test_parse_data_type!(
            dialect,
            "GEOMETRY",
            DataType::Custom(ObjectName(vec!["GEOMETRY".into()]), vec![])
        );

        test_parse_data_type!(
            dialect,
            "GEOMETRY(POINT)",
            DataType::Custom(
                ObjectName(vec!["GEOMETRY".into()]),
                vec!["POINT".to_string()]
            )
        );

        test_parse_data_type!(
            dialect,
            "GEOMETRY(POINT, 4326)",
            DataType::Custom(
                ObjectName(vec!["GEOMETRY".into()]),
                vec!["POINT".to_string(), "4326".to_string()]
            )
        );
    }

    #[test]
    fn test_ansii_exact_numeric_types() {
        // Exact numeric types: <https://jakewheat.github.io/sql-overview/sql-2016-foundation-grammar.html#exact-numeric-type>
        let dialect =
            TestedDialects::new(vec![Box::new(GenericDialect {}), Box::new(AnsiDialect {})]);

        test_parse_data_type!(dialect, "NUMERIC", DataType::Numeric(ExactNumberInfo::None));

        test_parse_data_type!(
            dialect,
            "NUMERIC(2)",
            DataType::Numeric(ExactNumberInfo::Precision(2))
        );

        test_parse_data_type!(
            dialect,
            "NUMERIC(2,10)",
            DataType::Numeric(ExactNumberInfo::PrecisionAndScale(2, 10))
        );

        test_parse_data_type!(dialect, "DECIMAL", DataType::Decimal(ExactNumberInfo::None));

        test_parse_data_type!(
            dialect,
            "DECIMAL(2)",
            DataType::Decimal(ExactNumberInfo::Precision(2))
        );

        test_parse_data_type!(
            dialect,
            "DECIMAL(2,10)",
            DataType::Decimal(ExactNumberInfo::PrecisionAndScale(2, 10))
        );

        test_parse_data_type!(dialect, "DEC", DataType::Dec(ExactNumberInfo::None));

        test_parse_data_type!(
            dialect,
            "DEC(2)",
            DataType::Dec(ExactNumberInfo::Precision(2))
        );

        test_parse_data_type!(
            dialect,
            "DEC(2,10)",
            DataType::Dec(ExactNumberInfo::PrecisionAndScale(2, 10))
        );
    }

    #[test]
    fn test_ansii_date_type() {
        // Datetime types: <https://jakewheat.github.io/sql-overview/sql-2016-foundation-grammar.html#datetime-type>
        let dialect =
            TestedDialects::new(vec![Box::new(GenericDialect {}), Box::new(AnsiDialect {})]);

        test_parse_data_type!(dialect, "DATE", DataType::Date);

        test_parse_data_type!(dialect, "TIME", DataType::Time(None, TimezoneInfo::None));

        test_parse_data_type!(
            dialect,
            "TIME(6)",
            DataType::Time(Some(6), TimezoneInfo::None)
        );

        test_parse_data_type!(
            dialect,
            "TIME WITH TIME ZONE",
            DataType::Time(None, TimezoneInfo::WithTimeZone)
        );

        test_parse_data_type!(
            dialect,
            "TIME(6) WITH TIME ZONE",
            DataType::Time(Some(6), TimezoneInfo::WithTimeZone)
        );

        test_parse_data_type!(
            dialect,
            "TIME WITHOUT TIME ZONE",
            DataType::Time(None, TimezoneInfo::WithoutTimeZone)
        );

        test_parse_data_type!(
            dialect,
            "TIME(6) WITHOUT TIME ZONE",
            DataType::Time(Some(6), TimezoneInfo::WithoutTimeZone)
        );

        test_parse_data_type!(
            dialect,
            "TIMESTAMP",
            DataType::Timestamp(None, TimezoneInfo::None)
        );

        test_parse_data_type!(
            dialect,
            "TIMESTAMP(22)",
            DataType::Timestamp(Some(22), TimezoneInfo::None)
        );

        test_parse_data_type!(
            dialect,
            "TIMESTAMP(22) WITH TIME ZONE",
            DataType::Timestamp(Some(22), TimezoneInfo::WithTimeZone)
        );

        test_parse_data_type!(
            dialect,
            "TIMESTAMP(33) WITHOUT TIME ZONE",
            DataType::Timestamp(Some(33), TimezoneInfo::WithoutTimeZone)
        );
    }
}

#[test]
fn test_parse_schema_name() {
    // The expected name should be identical as the input name, that's why I don't receive both
    macro_rules! test_parse_schema_name {
        ($input:expr, $expected_name:expr $(,)?) => {{
            all_dialects().run_parser_method(&*$input, |parser| {
                let schema_name = parser.parse_schema_name().unwrap();
                // Validate that the structure is the same as expected
                assert_eq!(schema_name, $expected_name);
                // Validate that the input and the expected structure serialization are the same
                assert_eq!(schema_name.to_string(), $input.to_string());
            });
        }};
    }

    let dummy_name = ObjectName(vec![Ident::new("dummy_name")]);
    let dummy_authorization = Ident::new("dummy_authorization");

    test_parse_schema_name!(
        format!("{dummy_name}"),
        SchemaName::Simple(dummy_name.clone())
    );

    test_parse_schema_name!(
        format!("AUTHORIZATION {dummy_authorization}"),
        SchemaName::UnnamedAuthorization(dummy_authorization.clone()),
    );
    test_parse_schema_name!(
        format!("{dummy_name} AUTHORIZATION {dummy_authorization}"),
        SchemaName::NamedAuthorization(dummy_name.clone(), dummy_authorization.clone()),
    );
}

#[test]
fn mysql_parse_index_table_constraint() {
    macro_rules! test_parse_table_constraint {
        ($dialect:expr, $input:expr, $expected:expr $(,)?) => {{
            $dialect.run_parser_method(&*$input, |parser| {
                let constraint = parser.parse_optional_table_constraint().unwrap().unwrap();
                // Validate that the structure is the same as expected
                assert_eq!(constraint, $expected);
                // Validate that the input and the expected structure serialization are the same
                assert_eq!(constraint.to_string(), $input.to_string());
            });
        }};
    }

    let dialect = TestedDialects::new(vec![Box::new(GenericDialect {}), Box::new(MySqlDialect {})]);

    test_parse_table_constraint!(
        dialect,
        "INDEX (c1)",
        TableConstraint::Index {
            display_as_key: false,
            name: None,
            index_type: None,
            columns: vec![Ident::new("c1")],
        }
    );

    test_parse_table_constraint!(
        dialect,
        "KEY (c1)",
        TableConstraint::Index {
            display_as_key: true,
            name: None,
            index_type: None,
            columns: vec![Ident::new("c1")],
        }
    );

    test_parse_table_constraint!(
        dialect,
        "INDEX 'index' (c1, c2)",
        TableConstraint::Index {
            display_as_key: false,
            name: Some(Ident::with_quote('\'', "index")),
            index_type: None,
            columns: vec![Ident::new("c1"), Ident::new("c2")],
        }
    );

    test_parse_table_constraint!(
        dialect,
        "INDEX USING BTREE (c1)",
        TableConstraint::Index {
            display_as_key: false,
            name: None,
            index_type: Some(IndexType::BTree),
            columns: vec![Ident::new("c1")],
        }
    );

    test_parse_table_constraint!(
        dialect,
        "INDEX USING HASH (c1)",
        TableConstraint::Index {
            display_as_key: false,
            name: None,
            index_type: Some(IndexType::Hash),
            columns: vec![Ident::new("c1")],
        }
    );

    test_parse_table_constraint!(
        dialect,
        "INDEX idx_name USING BTREE (c1)",
        TableConstraint::Index {
            display_as_key: false,
            name: Some(Ident::new("idx_name")),
            index_type: Some(IndexType::BTree),
            columns: vec![Ident::new("c1")],
        }
    );

    test_parse_table_constraint!(
        dialect,
        "INDEX idx_name USING HASH (c1)",
        TableConstraint::Index {
            display_as_key: false,
            name: Some(Ident::new("idx_name")),
            index_type: Some(IndexType::Hash),
            columns: vec![Ident::new("c1")],
        }
    );
}

#[test]
fn test_tokenizer_error_loc() {
    let sql = "foo '";
    let ast = Parser::parse_sql(&GenericDialect, sql);
    assert_eq!(
        ast,
        Err(ParserError::TokenizerError(
            "Unterminated string literal at Line: 1, Column: 5".to_string()
        ))
    );
}

#[test]
fn test_parser_error_loc() {
    let sql = "SELECT this is a syntax error";
    let ast = Parser::parse_sql(&GenericDialect, sql);
    assert_eq!(
        ast,
        Err(ParserError::ParserError(
            "Expected: [NOT] NULL or TRUE|FALSE or [NOT] DISTINCT FROM after IS, found: a at Line: 1, Column: 16"
                .to_string()
        ))
    );
}

#[test]
fn test_nested_explain_error() {
    let sql = "EXPLAIN EXPLAIN SELECT 1";
    let ast = Parser::parse_sql(&GenericDialect, sql);
    assert_eq!(
        ast,
        Err(ParserError::ParserError(
            "Explain must be root of the plan".to_string()
        ))
    );
}

#[test]
fn test_parse_multipart_identifier_positive() {
    let dialect = TestedDialects::new(vec![Box::new(GenericDialect {})]);

    // parse multipart with quotes
    let expected = vec![
        Ident {
            value: "CATALOG".to_string(),
            quote_style: None,
            span: Span::empty(),
        },
        Ident {
            value: "F(o)o. \"bar".to_string(),
            quote_style: Some('"'),
            span: Span::empty(),
        },
        Ident {
            value: "table".to_string(),
            quote_style: None,
            span: Span::empty(),
        },
    ];
    dialect.run_parser_method(r#"CATALOG."F(o)o. ""bar".table"#, |parser| {
        let actual = parser.parse_multipart_identifier().unwrap();
        assert_eq!(expected, actual);
    });

    // allow whitespace between ident parts
    let expected = vec![
        Ident {
            value: "CATALOG".to_string(),
            quote_style: None,
            span: Span::empty(),
        },
        Ident {
            value: "table".to_string(),
            quote_style: None,
            span: Span::empty(),
        },
    ];
    dialect.run_parser_method("CATALOG . table", |parser| {
        let actual = parser.parse_multipart_identifier().unwrap();
        assert_eq!(expected, actual);
    });
}

#[test]
fn test_parse_multipart_identifier_negative() {
    macro_rules! test_parse_multipart_identifier_error {
        ($input:expr, $expected_err:expr $(,)?) => {{
            all_dialects().run_parser_method(&*$input, |parser| {
                let actual_err = parser.parse_multipart_identifier().unwrap_err();
                assert_eq!(actual_err.to_string(), $expected_err);
            });
        }};
    }

    test_parse_multipart_identifier_error!(
        "",
        "sql parser error: Empty input when parsing identifier",
    );

    test_parse_multipart_identifier_error!(
        "*schema.table",
        "sql parser error: Unexpected token in identifier: *",
    );

    test_parse_multipart_identifier_error!(
        "schema.table*",
        "sql parser error: Unexpected token in identifier: *",
    );

    test_parse_multipart_identifier_error!(
        "schema.table.",
        "sql parser error: Trailing period in identifier",
    );

    test_parse_multipart_identifier_error!(
        "schema.*",
        "sql parser error: Unexpected token following period in identifier: *",
    );
}

#[test]
fn test_mysql_partition_selection() {
    let sql = "SELECT * FROM employees PARTITION (p0, p2)";
    let expected = vec!["p0", "p2"];

    let ast: Vec<Statement> = Parser::parse_sql(&MySqlDialect {}, sql).unwrap();
    assert_eq!(ast.len(), 1);
    if let Statement::Query(v) = &ast[0] {
        if let SetExpr::Select(select) = &*v.body {
            assert_eq!(select.from.len(), 1);
            let from: &TableWithJoins = &select.from[0];
            let table_factor = &from.relation;
            if let TableFactor::Table { partitions, .. } = table_factor {
                let actual: Vec<&str> = partitions
                    .iter()
                    .map(|ident| ident.value.as_str())
                    .collect();
                assert_eq!(expected, actual);
            }
        }
    } else {
        panic!("fail to parse mysql partition selection");
    }
}

#[test]
fn test_replace_into_placeholders() {
    let sql = "REPLACE INTO t (a) VALUES (&a)";

    assert!(Parser::parse_sql(&GenericDialect {}, sql).is_err());
}

#[test]
fn test_replace_into_set() {
    // NOTE: This is actually valid MySQL syntax, REPLACE and INSERT,
    // but the parser does not yet support it.
    // https://dev.mysql.com/doc/refman/8.3/en/insert.html
    let sql = "REPLACE INTO t SET a='1'";

    assert!(Parser::parse_sql(&MySqlDialect {}, sql).is_err());
}

#[test]
fn test_replace_into_set_placeholder() {
    let sql = "REPLACE INTO t SET ?";

    assert!(Parser::parse_sql(&GenericDialect {}, sql).is_err());
}

#[test]
fn test_replace_incomplete() {
    let sql = r#"REPLACE"#;

    assert!(Parser::parse_sql(&MySqlDialect {}, sql).is_err());
}

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#![warn(clippy::all)]
//! Test SQL syntax specific to Snowflake. The parser based on the
//! generic dialect is also tested (on the inputs it can handle).

use sqlparser::ast::helpers::stmt_data_loading::{
    DataLoadingOption, DataLoadingOptionType, StageLoadSelectItem,
};
use sqlparser::ast::*;
use sqlparser::dialect::{Dialect, GenericDialect, SnowflakeDialect};
use sqlparser::parser::{ParserError, ParserOptions};
use sqlparser::tokenizer::*;
use test_utils::*;

#[macro_use]
mod test_utils;

#[cfg(test)]
use pretty_assertions::assert_eq;

#[test]
fn test_snowflake_create_table() {
    let sql = "CREATE TABLE _my_$table (am00unt number)";
    match snowflake_and_generic().verified_stmt(sql) {
        Statement::CreateTable(CreateTable { name, .. }) => {
            assert_eq!("_my_$table", name.to_string());
        }
        _ => unreachable!(),
    }
}

#[test]
fn test_snowflake_create_or_replace_table() {
    let sql = "CREATE OR REPLACE TABLE my_table (a number)";
    match snowflake().verified_stmt(sql) {
        Statement::CreateTable(CreateTable {
            name, or_replace, ..
        }) => {
            assert_eq!("my_table", name.to_string());
            assert!(or_replace);
        }
        _ => unreachable!(),
    }
}

#[test]
fn test_snowflake_create_or_replace_table_copy_grants() {
    let sql = "CREATE OR REPLACE TABLE my_table (a number) COPY GRANTS";
    match snowflake().verified_stmt(sql) {
        Statement::CreateTable(CreateTable {
            name,
            or_replace,
            copy_grants,
            ..
        }) => {
            assert_eq!("my_table", name.to_string());
            assert!(or_replace);
            assert!(copy_grants);
        }
        _ => unreachable!(),
    }
}

#[test]
fn test_snowflake_create_or_replace_table_copy_grants_at_end() {
    let sql = "CREATE OR REPLACE TABLE my_table COPY GRANTS (a number) ";
    let parsed = "CREATE OR REPLACE TABLE my_table (a number) COPY GRANTS";
    match snowflake().one_statement_parses_to(sql, parsed) {
        Statement::CreateTable(CreateTable {
            name,
            or_replace,
            copy_grants,
            ..
        }) => {
            assert_eq!("my_table", name.to_string());
            assert!(or_replace);
            assert!(copy_grants);
        }
        _ => unreachable!(),
    }
}

#[test]
fn test_snowflake_create_or_replace_table_copy_grants_cta() {
    let sql = "CREATE OR REPLACE TABLE my_table COPY GRANTS AS SELECT 1 AS a";
    match snowflake().verified_stmt(sql) {
        Statement::CreateTable(CreateTable {
            name,
            or_replace,
            copy_grants,
            ..
        }) => {
            assert_eq!("my_table", name.to_string());
            assert!(or_replace);
            assert!(copy_grants);
        }
        _ => unreachable!(),
    }
}

#[test]
fn test_snowflake_create_table_enable_schema_evolution() {
    let sql = "CREATE TABLE my_table (a number) ENABLE_SCHEMA_EVOLUTION=TRUE";
    match snowflake().verified_stmt(sql) {
        Statement::CreateTable(CreateTable {
            name,
            enable_schema_evolution,
            ..
        }) => {
            assert_eq!("my_table", name.to_string());
            assert_eq!(Some(true), enable_schema_evolution);
        }
        _ => unreachable!(),
    }
}

#[test]
fn test_snowflake_create_table_change_tracking() {
    let sql = "CREATE TABLE my_table (a number) CHANGE_TRACKING=TRUE";
    match snowflake().verified_stmt(sql) {
        Statement::CreateTable(CreateTable {
            name,
            change_tracking,
            ..
        }) => {
            assert_eq!("my_table", name.to_string());
            assert_eq!(Some(true), change_tracking);
        }
        _ => unreachable!(),
    }
}

#[test]
fn test_snowflake_create_table_data_retention_time_in_days() {
    let sql = "CREATE TABLE my_table (a number) DATA_RETENTION_TIME_IN_DAYS=5";
    match snowflake().verified_stmt(sql) {
        Statement::CreateTable(CreateTable {
            name,
            data_retention_time_in_days,
            ..
        }) => {
            assert_eq!("my_table", name.to_string());
            assert_eq!(Some(5), data_retention_time_in_days);
        }
        _ => unreachable!(),
    }
}

#[test]
fn test_snowflake_create_table_max_data_extension_time_in_days() {
    let sql = "CREATE TABLE my_table (a number) MAX_DATA_EXTENSION_TIME_IN_DAYS=5";
    match snowflake().verified_stmt(sql) {
        Statement::CreateTable(CreateTable {
            name,
            max_data_extension_time_in_days,
            ..
        }) => {
            assert_eq!("my_table", name.to_string());
            assert_eq!(Some(5), max_data_extension_time_in_days);
        }
        _ => unreachable!(),
    }
}

#[test]
fn test_snowflake_create_table_with_aggregation_policy() {
    match snowflake()
        .verified_stmt("CREATE TABLE my_table (a number) WITH AGGREGATION POLICY policy_name")
    {
        Statement::CreateTable(CreateTable {
            name,
            with_aggregation_policy,
            ..
        }) => {
            assert_eq!("my_table", name.to_string());
            assert_eq!(
                Some("policy_name".to_string()),
                with_aggregation_policy.map(|name| name.to_string())
            );
        }
        _ => unreachable!(),
    }

    match snowflake()
        .parse_sql_statements("CREATE TABLE my_table (a number)  AGGREGATION POLICY policy_name")
        .unwrap()
        .pop()
        .unwrap()
    {
        Statement::CreateTable(CreateTable {
            name,
            with_aggregation_policy,
            ..
        }) => {
            assert_eq!("my_table", name.to_string());
            assert_eq!(
                Some("policy_name".to_string()),
                with_aggregation_policy.map(|name| name.to_string())
            );
        }
        _ => unreachable!(),
    }
}

#[test]
fn test_snowflake_create_table_with_row_access_policy() {
    match snowflake().verified_stmt(
        "CREATE TABLE my_table (a number, b number) WITH ROW ACCESS POLICY policy_name ON (a)",
    ) {
        Statement::CreateTable(CreateTable {
            name,
            with_row_access_policy,
            ..
        }) => {
            assert_eq!("my_table", name.to_string());
            assert_eq!(
                Some("WITH ROW ACCESS POLICY policy_name ON (a)".to_string()),
                with_row_access_policy.map(|policy| policy.to_string())
            );
        }
        _ => unreachable!(),
    }

    match snowflake()
        .parse_sql_statements(
            "CREATE TABLE my_table (a number, b number) ROW ACCESS POLICY policy_name ON (a)",
        )
        .unwrap()
        .pop()
        .unwrap()
    {
        Statement::CreateTable(CreateTable {
            name,
            with_row_access_policy,
            ..
        }) => {
            assert_eq!("my_table", name.to_string());
            assert_eq!(
                Some("WITH ROW ACCESS POLICY policy_name ON (a)".to_string()),
                with_row_access_policy.map(|policy| policy.to_string())
            );
        }
        _ => unreachable!(),
    }
}

#[test]
fn test_snowflake_create_table_with_tag() {
    match snowflake()
        .verified_stmt("CREATE TABLE my_table (a number) WITH TAG (A='TAG A', B='TAG B')")
    {
        Statement::CreateTable(CreateTable {
            name, with_tags, ..
        }) => {
            assert_eq!("my_table", name.to_string());
            assert_eq!(
                Some(vec![
                    Tag::new("A".into(), "TAG A".to_string()),
                    Tag::new("B".into(), "TAG B".to_string())
                ]),
                with_tags
            );
        }
        _ => unreachable!(),
    }

    match snowflake()
        .parse_sql_statements("CREATE TABLE my_table (a number) TAG (A='TAG A', B='TAG B')")
        .unwrap()
        .pop()
        .unwrap()
    {
        Statement::CreateTable(CreateTable {
            name, with_tags, ..
        }) => {
            assert_eq!("my_table", name.to_string());
            assert_eq!(
                Some(vec![
                    Tag::new("A".into(), "TAG A".to_string()),
                    Tag::new("B".into(), "TAG B".to_string())
                ]),
                with_tags
            );
        }
        _ => unreachable!(),
    }
}

#[test]
fn test_snowflake_create_table_default_ddl_collation() {
    let sql = "CREATE TABLE my_table (a number) DEFAULT_DDL_COLLATION='de'";
    match snowflake().verified_stmt(sql) {
        Statement::CreateTable(CreateTable {
            name,
            default_ddl_collation,
            ..
        }) => {
            assert_eq!("my_table", name.to_string());
            assert_eq!(Some("de".to_string()), default_ddl_collation);
        }
        _ => unreachable!(),
    }
}

#[test]
fn test_snowflake_create_transient_table() {
    let sql = "CREATE TRANSIENT TABLE CUSTOMER (id INT, name VARCHAR(255))";
    match snowflake_and_generic().verified_stmt(sql) {
        Statement::CreateTable(CreateTable {
            name, transient, ..
        }) => {
            assert_eq!("CUSTOMER", name.to_string());
            assert!(transient)
        }
        _ => unreachable!(),
    }
}

#[test]
fn test_snowflake_create_table_column_comment() {
    let sql = "CREATE TABLE my_table (a STRING COMMENT 'some comment')";
    match snowflake().verified_stmt(sql) {
        Statement::CreateTable(CreateTable { name, columns, .. }) => {
            assert_eq!("my_table", name.to_string());
            assert_eq!(
                vec![ColumnDef {
                    name: "a".into(),
                    data_type: DataType::String(None),
                    options: vec![ColumnOptionDef {
                        name: None,
                        option: ColumnOption::Comment("some comment".to_string())
                    }],
                }],
                columns
            )
        }
        _ => unreachable!(),
    }
}

#[test]
fn test_snowflake_create_table_on_commit() {
    snowflake().verified_stmt(
        r#"CREATE LOCAL TEMPORARY TABLE "AAA"."foo" ("bar" INTEGER) ON COMMIT PRESERVE ROWS"#,
    );
    snowflake().verified_stmt(r#"CREATE TABLE "AAA"."foo" ("bar" INTEGER) ON COMMIT DELETE ROWS"#);
    snowflake().verified_stmt(r#"CREATE TABLE "AAA"."foo" ("bar" INTEGER) ON COMMIT DROP"#);
}

#[test]
fn test_snowflake_create_local_table() {
    match snowflake().verified_stmt("CREATE TABLE my_table (a INT)") {
        Statement::CreateTable(CreateTable { name, global, .. }) => {
            assert_eq!("my_table", name.to_string());
            assert!(global.is_none())
        }
        _ => unreachable!(),
    }

    match snowflake().verified_stmt("CREATE LOCAL TABLE my_table (a INT)") {
        Statement::CreateTable(CreateTable { name, global, .. }) => {
            assert_eq!("my_table", name.to_string());
            assert_eq!(Some(false), global)
        }
        _ => unreachable!(),
    }
}

#[test]
fn test_snowflake_create_global_table() {
    match snowflake().verified_stmt("CREATE GLOBAL TABLE my_table (a INT)") {
        Statement::CreateTable(CreateTable { name, global, .. }) => {
            assert_eq!("my_table", name.to_string());
            assert_eq!(Some(true), global)
        }
        _ => unreachable!(),
    }
}

#[test]
fn test_snowflake_create_invalid_local_global_table() {
    assert_eq!(
        snowflake().parse_sql_statements("CREATE LOCAL GLOBAL TABLE my_table (a INT)"),
        Err(ParserError::ParserError(
            "Expected: an SQL statement, found: LOCAL".to_string()
        ))
    );

    assert_eq!(
        snowflake().parse_sql_statements("CREATE GLOBAL LOCAL TABLE my_table (a INT)"),
        Err(ParserError::ParserError(
            "Expected: an SQL statement, found: GLOBAL".to_string()
        ))
    );
}

#[test]
fn test_snowflake_create_invalid_temporal_table() {
    assert_eq!(
        snowflake().parse_sql_statements("CREATE TEMP TEMPORARY TABLE my_table (a INT)"),
        Err(ParserError::ParserError(
            "Expected: an object type after CREATE, found: TEMPORARY".to_string()
        ))
    );

    assert_eq!(
        snowflake().parse_sql_statements("CREATE TEMP VOLATILE TABLE my_table (a INT)"),
        Err(ParserError::ParserError(
            "Expected: an object type after CREATE, found: VOLATILE".to_string()
        ))
    );

    assert_eq!(
        snowflake().parse_sql_statements("CREATE TEMP TRANSIENT TABLE my_table (a INT)"),
        Err(ParserError::ParserError(
            "Expected: an object type after CREATE, found: TRANSIENT".to_string()
        ))
    );
}

#[test]
fn test_snowflake_create_table_if_not_exists() {
    match snowflake().verified_stmt("CREATE TABLE IF NOT EXISTS my_table (a INT)") {
        Statement::CreateTable(CreateTable {
            name,
            if_not_exists,
            ..
        }) => {
            assert_eq!("my_table", name.to_string());
            assert!(if_not_exists)
        }
        _ => unreachable!(),
    }
}

#[test]
fn test_snowflake_create_table_cluster_by() {
    match snowflake().verified_stmt("CREATE TABLE my_table (a INT) CLUSTER BY (a, b)") {
        Statement::CreateTable(CreateTable {
            name, cluster_by, ..
        }) => {
            assert_eq!("my_table", name.to_string());
            assert_eq!(
                Some(WrappedCollection::Parentheses(vec![
                    Ident::new("a"),
                    Ident::new("b"),
                ])),
                cluster_by
            )
        }
        _ => unreachable!(),
    }
}

#[test]
fn test_snowflake_create_table_comment() {
    match snowflake().verified_stmt("CREATE TABLE my_table (a INT) COMMENT = 'some comment'") {
        Statement::CreateTable(CreateTable { name, comment, .. }) => {
            assert_eq!("my_table", name.to_string());
            assert_eq!("some comment", comment.unwrap().to_string());
        }
        _ => unreachable!(),
    }
}

#[test]
fn test_snowflake_create_table_incomplete_statement() {
    assert_eq!(
        snowflake().parse_sql_statements("CREATE TABLE my_table"),
        Err(ParserError::ParserError(
            "unexpected end of input".to_string()
        ))
    );

    assert_eq!(
        snowflake().parse_sql_statements("CREATE TABLE my_table; (c int)"),
        Err(ParserError::ParserError(
            "unexpected end of input".to_string()
        ))
    );
}

#[test]
fn test_snowflake_single_line_tokenize() {
    let sql = "CREATE TABLE# this is a comment \ntable_1";
    let dialect = SnowflakeDialect {};
    let tokens = Tokenizer::new(&dialect, sql).tokenize().unwrap();

    let expected = vec![
        Token::make_keyword("CREATE"),
        Token::Whitespace(Whitespace::Space),
        Token::make_keyword("TABLE"),
        Token::Whitespace(Whitespace::SingleLineComment {
            prefix: "#".to_string(),
            comment: " this is a comment \n".to_string(),
        }),
        Token::make_word("table_1", None),
    ];

    assert_eq!(expected, tokens);

    let sql = "CREATE TABLE // this is a comment \ntable_1";
    let tokens = Tokenizer::new(&dialect, sql).tokenize().unwrap();

    let expected = vec![
        Token::make_keyword("CREATE"),
        Token::Whitespace(Whitespace::Space),
        Token::make_keyword("TABLE"),
        Token::Whitespace(Whitespace::Space),
        Token::Whitespace(Whitespace::SingleLineComment {
            prefix: "//".to_string(),
            comment: " this is a comment \n".to_string(),
        }),
        Token::make_word("table_1", None),
    ];

    assert_eq!(expected, tokens);
}

#[test]
fn test_snowflake_create_table_with_autoincrement_columns() {
    let sql = concat!(
        "CREATE TABLE my_table (",
        "a INT AUTOINCREMENT ORDER, ",
        "b INT AUTOINCREMENT(100, 1) NOORDER, ",
        "c INT IDENTITY, ",
        "d INT IDENTITY START 100 INCREMENT 1 ORDER",
        ")"
    );
    // it is a snowflake specific options (AUTOINCREMENT/IDENTITY)
    match snowflake().verified_stmt(sql) {
        Statement::CreateTable(CreateTable { columns, .. }) => {
            assert_eq!(
                columns,
                vec![
                    ColumnDef {
                        name: "a".into(),
                        data_type: DataType::Int(None),
                        options: vec![ColumnOptionDef {
                            name: None,
                            option: ColumnOption::Identity(IdentityPropertyKind::Autoincrement(
                                IdentityProperty {
                                    parameters: None,
                                    order: Some(IdentityPropertyOrder::Order),
                                }
                            ))
                        }]
                    },
                    ColumnDef {
                        name: "b".into(),
                        data_type: DataType::Int(None),
                        options: vec![ColumnOptionDef {
                            name: None,
                            option: ColumnOption::Identity(IdentityPropertyKind::Autoincrement(
                                IdentityProperty {
                                    parameters: Some(IdentityPropertyFormatKind::FunctionCall(
                                        IdentityParameters {
                                            seed: Expr::Value(number("100")),
                                            increment: Expr::Value(number("1")),
                                        }
                                    )),
                                    order: Some(IdentityPropertyOrder::NoOrder),
                                }
                            ))
                        }]
                    },
                    ColumnDef {
                        name: "c".into(),
                        data_type: DataType::Int(None),
                        options: vec![ColumnOptionDef {
                            name: None,
                            option: ColumnOption::Identity(IdentityPropertyKind::Identity(
                                IdentityProperty {
                                    parameters: None,
                                    order: None,
                                }
                            ))
                        }]
                    },
                    ColumnDef {
                        name: "d".into(),
                        data_type: DataType::Int(None),
                        options: vec![ColumnOptionDef {
                            name: None,
                            option: ColumnOption::Identity(IdentityPropertyKind::Identity(
                                IdentityProperty {
                                    parameters: Some(
                                        IdentityPropertyFormatKind::StartAndIncrement(
                                            IdentityParameters {
                                                seed: Expr::Value(number("100")),
                                                increment: Expr::Value(number("1")),
                                            }
                                        )
                                    ),
                                    order: Some(IdentityPropertyOrder::Order),
                                }
                            ))
                        }]
                    },
                ]
            );
        }
        _ => unreachable!(),
    }
}

#[test]
fn test_snowflake_create_table_with_collated_column() {
    match snowflake_and_generic().verified_stmt("CREATE TABLE my_table (a TEXT COLLATE 'de_DE')") {
        Statement::CreateTable(CreateTable { columns, .. }) => {
            assert_eq!(
                columns,
                vec![ColumnDef {
                    name: "a".into(),
                    data_type: DataType::Text,
                    options: vec![ColumnOptionDef {
                        name: None,
                        option: ColumnOption::Collation(ObjectName::from(vec![Ident::with_quote(
                            '\'', "de_DE"
                        )])),
                    }]
                },]
            );
        }
        _ => unreachable!(),
    }
}

#[test]
fn test_snowflake_create_table_with_columns_masking_policy() {
    for (sql, with, using_columns) in [
        (
            "CREATE TABLE my_table (a INT WITH MASKING POLICY p)",
            true,
            None,
        ),
        (
            "CREATE TABLE my_table (a INT MASKING POLICY p)",
            false,
            None,
        ),
        (
            "CREATE TABLE my_table (a INT WITH MASKING POLICY p USING (a, b))",
            true,
            Some(vec!["a".into(), "b".into()]),
        ),
        (
            "CREATE TABLE my_table (a INT MASKING POLICY p USING (a, b))",
            false,
            Some(vec!["a".into(), "b".into()]),
        ),
    ] {
        match snowflake().verified_stmt(sql) {
            Statement::CreateTable(CreateTable { columns, .. }) => {
                assert_eq!(
                    columns,
                    vec![ColumnDef {
                        name: "a".into(),
                        data_type: DataType::Int(None),
                        options: vec![ColumnOptionDef {
                            name: None,
                            option: ColumnOption::Policy(ColumnPolicy::MaskingPolicy(
                                ColumnPolicyProperty {
                                    with,
                                    policy_name: "p".into(),
                                    using_columns,
                                }
                            ))
                        }],
                    },]
                );
            }
            _ => unreachable!(),
        }
    }
}

#[test]
fn test_snowflake_create_table_with_columns_projection_policy() {
    for (sql, with) in [
        (
            "CREATE TABLE my_table (a INT WITH PROJECTION POLICY p)",
            true,
        ),
        ("CREATE TABLE my_table (a INT PROJECTION POLICY p)", false),
    ] {
        match snowflake().verified_stmt(sql) {
            Statement::CreateTable(CreateTable { columns, .. }) => {
                assert_eq!(
                    columns,
                    vec![ColumnDef {
                        name: "a".into(),
                        data_type: DataType::Int(None),
                        options: vec![ColumnOptionDef {
                            name: None,
                            option: ColumnOption::Policy(ColumnPolicy::ProjectionPolicy(
                                ColumnPolicyProperty {
                                    with,
                                    policy_name: "p".into(),
                                    using_columns: None,
                                }
                            ))
                        }],
                    },]
                );
            }
            _ => unreachable!(),
        }
    }
}

#[test]
fn test_snowflake_create_table_with_columns_tags() {
    for (sql, with) in [
        (
            "CREATE TABLE my_table (a INT WITH TAG (A='TAG A', B='TAG B'))",
            true,
        ),
        (
            "CREATE TABLE my_table (a INT TAG (A='TAG A', B='TAG B'))",
            false,
        ),
    ] {
        match snowflake().verified_stmt(sql) {
            Statement::CreateTable(CreateTable { columns, .. }) => {
                assert_eq!(
                    columns,
                    vec![ColumnDef {
                        name: "a".into(),
                        data_type: DataType::Int(None),
                        options: vec![ColumnOptionDef {
                            name: None,
                            option: ColumnOption::Tags(TagsColumnOption {
                                with,
                                tags: vec![
                                    Tag::new("A".into(), "TAG A".into()),
                                    Tag::new("B".into(), "TAG B".into()),
                                ]
                            }),
                        }],
                    },]
                );
            }
            _ => unreachable!(),
        }
    }
}

#[test]
fn test_snowflake_create_table_with_several_column_options() {
    let sql = concat!(
        "CREATE TABLE my_table (",
        "a INT IDENTITY WITH MASKING POLICY p1 USING (a, b) WITH TAG (A='TAG A', B='TAG B'), ",
        "b TEXT COLLATE 'de_DE' PROJECTION POLICY p2 TAG (C='TAG C', D='TAG D')",
        ")"
    );
    match snowflake().verified_stmt(sql) {
        Statement::CreateTable(CreateTable { columns, .. }) => {
            assert_eq!(
                columns,
                vec![
                    ColumnDef {
                        name: "a".into(),
                        data_type: DataType::Int(None),
                        options: vec![
                            ColumnOptionDef {
                                name: None,
                                option: ColumnOption::Identity(IdentityPropertyKind::Identity(
                                    IdentityProperty {
                                        parameters: None,
                                        order: None
                                    }
                                )),
                            },
                            ColumnOptionDef {
                                name: None,
                                option: ColumnOption::Policy(ColumnPolicy::MaskingPolicy(
                                    ColumnPolicyProperty {
                                        with: true,
                                        policy_name: "p1".into(),
                                        using_columns: Some(vec!["a".into(), "b".into()]),
                                    }
                                )),
                            },
                            ColumnOptionDef {
                                name: None,
                                option: ColumnOption::Tags(TagsColumnOption {
                                    with: true,
                                    tags: vec![
                                        Tag::new("A".into(), "TAG A".into()),
                                        Tag::new("B".into(), "TAG B".into()),
                                    ]
                                }),
                            }
                        ],
                    },
                    ColumnDef {
                        name: "b".into(),
                        data_type: DataType::Text,
                        options: vec![
                            ColumnOptionDef {
                                name: None,
                                option: ColumnOption::Collation(ObjectName::from(vec![
                                    Ident::with_quote('\'', "de_DE")
                                ])),
                            },
                            ColumnOptionDef {
                                name: None,
                                option: ColumnOption::Policy(ColumnPolicy::ProjectionPolicy(
                                    ColumnPolicyProperty {
                                        with: false,
                                        policy_name: "p2".into(),
                                        using_columns: None,
                                    }
                                )),
                            },
                            ColumnOptionDef {
                                name: None,
                                option: ColumnOption::Tags(TagsColumnOption {
                                    with: false,
                                    tags: vec![
                                        Tag::new("C".into(), "TAG C".into()),
                                        Tag::new("D".into(), "TAG D".into()),
                                    ]
                                }),
                            }
                        ],
                    },
                ]
            );
        }
        _ => unreachable!(),
    }
}

#[test]
fn test_snowflake_create_iceberg_table_all_options() {
    match snowflake().verified_stmt("CREATE ICEBERG TABLE my_table (a INT, b INT) \
    CLUSTER BY (a, b) EXTERNAL_VOLUME = 'volume' CATALOG = 'SNOWFLAKE' BASE_LOCATION = 'relative/path' CATALOG_SYNC = 'OPEN_CATALOG' \
    STORAGE_SERIALIZATION_POLICY = COMPATIBLE COPY GRANTS CHANGE_TRACKING=TRUE DATA_RETENTION_TIME_IN_DAYS=5 MAX_DATA_EXTENSION_TIME_IN_DAYS=10 \
    WITH AGGREGATION POLICY policy_name WITH ROW ACCESS POLICY policy_name ON (a) WITH TAG (A='TAG A', B='TAG B')") {
        Statement::CreateTable(CreateTable {
            name, cluster_by, base_location,
            external_volume, catalog, catalog_sync,
            storage_serialization_policy, change_tracking,
            copy_grants, data_retention_time_in_days,
            max_data_extension_time_in_days, with_aggregation_policy,
            with_row_access_policy, with_tags, ..
        }) => {
            assert_eq!("my_table", name.to_string());
            assert_eq!(
                Some(WrappedCollection::Parentheses(vec![
                    Ident::new("a"),
                    Ident::new("b"),
                ])),
                cluster_by
            );
            assert_eq!("relative/path", base_location.unwrap());
            assert_eq!("volume", external_volume.unwrap());
            assert_eq!("SNOWFLAKE", catalog.unwrap());
            assert_eq!("OPEN_CATALOG", catalog_sync.unwrap());
            assert_eq!(StorageSerializationPolicy::Compatible, storage_serialization_policy.unwrap());
            assert!(change_tracking.unwrap());
            assert!(copy_grants);
            assert_eq!(Some(5), data_retention_time_in_days);
            assert_eq!(Some(10), max_data_extension_time_in_days);
            assert_eq!(
                Some("WITH ROW ACCESS POLICY policy_name ON (a)".to_string()),
                with_row_access_policy.map(|policy| policy.to_string())
            );
            assert_eq!(
                Some("policy_name".to_string()),
                with_aggregation_policy.map(|name| name.to_string())
            );
            assert_eq!(Some(vec![
                                        Tag::new("A".into(), "TAG A".into()),
                                        Tag::new("B".into(), "TAG B".into()),
                                    ]), with_tags);

        }
        _ => unreachable!(),
    }
}

#[test]
fn test_snowflake_create_iceberg_table() {
    match snowflake()
        .verified_stmt("CREATE ICEBERG TABLE my_table (a INT) BASE_LOCATION = 'relative_path'")
    {
        Statement::CreateTable(CreateTable {
            name,
            base_location,
            ..
        }) => {
            assert_eq!("my_table", name.to_string());
            assert_eq!("relative_path", base_location.unwrap());
        }
        _ => unreachable!(),
    }
}

#[test]
fn test_snowflake_create_iceberg_table_without_location() {
    let res = snowflake().parse_sql_statements("CREATE ICEBERG TABLE my_table (a INT)");
    assert_eq!(
        ParserError::ParserError("BASE_LOCATION is required for ICEBERG tables".to_string()),
        res.unwrap_err()
    );
}

#[test]
fn parse_sf_create_or_replace_view_with_comment_missing_equal() {
    assert!(snowflake_and_generic()
        .parse_sql_statements("CREATE OR REPLACE VIEW v COMMENT = 'hello, world' AS SELECT 1")
        .is_ok());

    assert!(snowflake_and_generic()
        .parse_sql_statements("CREATE OR REPLACE VIEW v COMMENT 'hello, world' AS SELECT 1")
        .is_err());
}

#[test]
fn parse_sf_create_or_replace_with_comment_for_snowflake() {
    let sql = "CREATE OR REPLACE VIEW v COMMENT = 'hello, world' AS SELECT 1";
    let dialect =
        test_utils::TestedDialects::new(vec![Box::new(SnowflakeDialect {}) as Box<dyn Dialect>]);

    match dialect.verified_stmt(sql) {
        Statement::CreateView {
            name,
            columns,
            or_replace,
            options,
            query,
            materialized,
            cluster_by,
            comment,
            with_no_schema_binding: late_binding,
            if_not_exists,
            temporary,
            ..
        } => {
            assert_eq!("v", name.to_string());
            assert_eq!(columns, vec![]);
            assert_eq!(options, CreateTableOptions::None);
            assert_eq!("SELECT 1", query.to_string());
            assert!(!materialized);
            assert!(or_replace);
            assert_eq!(cluster_by, vec![]);
            assert!(comment.is_some());
            assert_eq!(comment.expect("expected comment"), "hello, world");
            assert!(!late_binding);
            assert!(!if_not_exists);
            assert!(!temporary);
        }
        _ => unreachable!(),
    }
}

#[test]
fn test_sf_derived_table_in_parenthesis() {
    // Nesting a subquery in an extra set of parentheses is non-standard,
    // but supported in Snowflake SQL
    snowflake_and_generic().one_statement_parses_to(
        "SELECT * FROM ((SELECT 1) AS t)",
        "SELECT * FROM (SELECT 1) AS t",
    );
    snowflake_and_generic().one_statement_parses_to(
        "SELECT * FROM (((SELECT 1) AS t))",
        "SELECT * FROM (SELECT 1) AS t",
    );
}

#[test]
fn test_single_table_in_parenthesis() {
    // Parenthesized table names are non-standard, but supported in Snowflake SQL
    snowflake_and_generic().one_statement_parses_to(
        "SELECT * FROM (a NATURAL JOIN (b))",
        "SELECT * FROM (a NATURAL JOIN b)",
    );
    snowflake_and_generic().one_statement_parses_to(
        "SELECT * FROM (a NATURAL JOIN ((b)))",
        "SELECT * FROM (a NATURAL JOIN b)",
    );
}

#[test]
fn test_single_table_in_parenthesis_with_alias() {
    snowflake_and_generic().one_statement_parses_to(
        "SELECT * FROM (a NATURAL JOIN (b) c )",
        "SELECT * FROM (a NATURAL JOIN b AS c)",
    );

    snowflake_and_generic().one_statement_parses_to(
        "SELECT * FROM (a NATURAL JOIN ((b)) c )",
        "SELECT * FROM (a NATURAL JOIN b AS c)",
    );

    snowflake_and_generic().one_statement_parses_to(
        "SELECT * FROM (a NATURAL JOIN ( (b) c ) )",
        "SELECT * FROM (a NATURAL JOIN b AS c)",
    );

    snowflake_and_generic().one_statement_parses_to(
        "SELECT * FROM (a NATURAL JOIN ( (b) as c ) )",
        "SELECT * FROM (a NATURAL JOIN b AS c)",
    );

    snowflake_and_generic().one_statement_parses_to(
        "SELECT * FROM (a alias1 NATURAL JOIN ( (b) c ) )",
        "SELECT * FROM (a AS alias1 NATURAL JOIN b AS c)",
    );

    snowflake_and_generic().one_statement_parses_to(
        "SELECT * FROM (a as alias1 NATURAL JOIN ( (b) as c ) )",
        "SELECT * FROM (a AS alias1 NATURAL JOIN b AS c)",
    );

    snowflake_and_generic().one_statement_parses_to(
        "SELECT * FROM (a NATURAL JOIN b) c",
        "SELECT * FROM (a NATURAL JOIN b) AS c",
    );

    let res = snowflake().parse_sql_statements("SELECT * FROM (a b) c");
    assert_eq!(
        ParserError::ParserError("duplicate alias b".to_string()),
        res.unwrap_err()
    );
}

#[test]
fn parse_array() {
    let sql = "SELECT CAST(a AS ARRAY) FROM customer";
    let select = snowflake().verified_only_select(sql);
    assert_eq!(
        &Expr::Cast {
            kind: CastKind::Cast,
            expr: Box::new(Expr::Identifier(Ident::new("a"))),
            data_type: DataType::Array(ArrayElemTypeDef::None),
            format: None,
        },
        expr_from_projection(only(&select.projection))
    );
}

#[test]
fn parse_lateral_flatten() {
    snowflake().verified_only_select(r#"SELECT * FROM TABLE(FLATTEN(input => parse_json('{"a":1, "b":[77,88]}'), outer => true)) AS f"#);
    snowflake().verified_only_select(r#"SELECT emp.employee_ID, emp.last_name, index, value AS project_name FROM employees AS emp, LATERAL FLATTEN(INPUT => emp.project_names) AS proj_names"#);
}

// https://docs.snowflake.com/en/user-guide/querying-semistructured
#[test]
fn parse_semi_structured_data_traversal() {
    // most basic case
    let sql = "SELECT a:b FROM t";
    let select = snowflake().verified_only_select(sql);
    assert_eq!(
        SelectItem::UnnamedExpr(Expr::JsonAccess {
            value: Box::new(Expr::Identifier(Ident::new("a"))),
            path: JsonPath {
                path: vec![JsonPathElem::Dot {
                    key: "b".to_owned(),
                    quoted: false
                }]
            },
        }),
        select.projection[0]
    );

    // identifier can be quoted
    let sql = r#"SELECT a:"my long object key name" FROM t"#;
    let select = snowflake().verified_only_select(sql);
    assert_eq!(
        SelectItem::UnnamedExpr(Expr::JsonAccess {
            value: Box::new(Expr::Identifier(Ident::new("a"))),
            path: JsonPath {
                path: vec![JsonPathElem::Dot {
                    key: "my long object key name".to_owned(),
                    quoted: true
                }]
            },
        }),
        select.projection[0]
    );

    // expressions are allowed in bracket notation
    let sql = r#"SELECT a[2 + 2] FROM t"#;
    let select = snowflake().verified_only_select(sql);
    assert_eq!(
        SelectItem::UnnamedExpr(Expr::JsonAccess {
            value: Box::new(Expr::Identifier(Ident::new("a"))),
            path: JsonPath {
                path: vec![JsonPathElem::Bracket {
                    key: Expr::BinaryOp {
                        left: Box::new(Expr::Value(number("2"))),
                        op: BinaryOperator::Plus,
                        right: Box::new(Expr::Value(number("2")))
                    },
                }]
            },
        }),
        select.projection[0]
    );

    snowflake().verified_stmt("SELECT a:b::INT FROM t");

    // unquoted keywords are permitted in the object key
    let sql = "SELECT a:select, a:from FROM t";
    let select = snowflake().verified_only_select(sql);
    assert_eq!(
        vec![
            SelectItem::UnnamedExpr(Expr::JsonAccess {
                value: Box::new(Expr::Identifier(Ident::new("a"))),
                path: JsonPath {
                    path: vec![JsonPathElem::Dot {
                        key: "select".to_owned(),
                        quoted: false
                    }]
                },
            }),
            SelectItem::UnnamedExpr(Expr::JsonAccess {
                value: Box::new(Expr::Identifier(Ident::new("a"))),
                path: JsonPath {
                    path: vec![JsonPathElem::Dot {
                        key: "from".to_owned(),
                        quoted: false
                    }]
                },
            })
        ],
        select.projection
    );

    // multiple levels can be traversed
    // https://docs.snowflake.com/en/user-guide/querying-semistructured#dot-notation
    let sql = r#"SELECT a:foo."bar".baz"#;
    let select = snowflake().verified_only_select(sql);
    assert_eq!(
        vec![SelectItem::UnnamedExpr(Expr::JsonAccess {
            value: Box::new(Expr::Identifier(Ident::new("a"))),
            path: JsonPath {
                path: vec![
                    JsonPathElem::Dot {
                        key: "foo".to_owned(),
                        quoted: false,
                    },
                    JsonPathElem::Dot {
                        key: "bar".to_owned(),
                        quoted: true,
                    },
                    JsonPathElem::Dot {
                        key: "baz".to_owned(),
                        quoted: false,
                    }
                ]
            },
        })],
        select.projection
    );

    // dot and bracket notation can be mixed (starting with : case)
    // https://docs.snowflake.com/en/user-guide/querying-semistructured#dot-notation
    let sql = r#"SELECT a:foo[0].bar"#;
    let select = snowflake().verified_only_select(sql);
    assert_eq!(
        vec![SelectItem::UnnamedExpr(Expr::JsonAccess {
            value: Box::new(Expr::Identifier(Ident::new("a"))),
            path: JsonPath {
                path: vec![
                    JsonPathElem::Dot {
                        key: "foo".to_owned(),
                        quoted: false,
                    },
                    JsonPathElem::Bracket {
                        key: Expr::Value(number("0")),
                    },
                    JsonPathElem::Dot {
                        key: "bar".to_owned(),
                        quoted: false,
                    }
                ]
            },
        })],
        select.projection
    );

    // dot and bracket notation can be mixed (starting with bracket case)
    // https://docs.snowflake.com/en/user-guide/querying-semistructured#dot-notation
    let sql = r#"SELECT a[0].foo.bar"#;
    let select = snowflake().verified_only_select(sql);
    assert_eq!(
        vec![SelectItem::UnnamedExpr(Expr::JsonAccess {
            value: Box::new(Expr::Identifier(Ident::new("a"))),
            path: JsonPath {
                path: vec![
                    JsonPathElem::Bracket {
                        key: Expr::Value(number("0")),
                    },
                    JsonPathElem::Dot {
                        key: "foo".to_owned(),
                        quoted: false,
                    },
                    JsonPathElem::Dot {
                        key: "bar".to_owned(),
                        quoted: false,
                    }
                ]
            },
        })],
        select.projection
    );

    // a json access used as a key to another json access
    assert_eq!(
        snowflake().verified_expr("a[b:c]"),
        Expr::JsonAccess {
            value: Box::new(Expr::Identifier(Ident::new("a"))),
            path: JsonPath {
                path: vec![JsonPathElem::Bracket {
                    key: Expr::JsonAccess {
                        value: Box::new(Expr::Identifier(Ident::new("b"))),
                        path: JsonPath {
                            path: vec![JsonPathElem::Dot {
                                key: "c".to_owned(),
                                quoted: false
                            }]
                        }
                    }
                }]
            }
        }
    );

    // unquoted object keys cannot start with a digit
    assert_eq!(
        snowflake()
            .parse_sql_statements("SELECT a:42")
            .unwrap_err()
            .to_string(),
        "sql parser error: Expected: variant object key name, found: 42"
    );

    // casting a json access and accessing an array element
    assert_eq!(
        snowflake().verified_expr("a:b::ARRAY[1]"),
        Expr::JsonAccess {
            value: Box::new(Expr::Cast {
                kind: CastKind::DoubleColon,
                data_type: DataType::Array(ArrayElemTypeDef::None),
                format: None,
                expr: Box::new(Expr::JsonAccess {
                    value: Box::new(Expr::Identifier(Ident::new("a"))),
                    path: JsonPath {
                        path: vec![JsonPathElem::Dot {
                            key: "b".to_string(),
                            quoted: false
                        }]
                    }
                })
            }),
            path: JsonPath {
                path: vec![JsonPathElem::Bracket {
                    key: Expr::Value(number("1"))
                }]
            }
        }
    );
}

#[test]
fn parse_delimited_identifiers() {
    // check that quoted identifiers in any position remain quoted after serialization
    let select = snowflake().verified_only_select(
        r#"SELECT "alias"."bar baz", "myfun"(), "simple id" AS "column alias" FROM "a table" AS "alias""#,
    );
    // check FROM
    match only(select.from).relation {
        TableFactor::Table {
            name,
            alias,
            args,
            with_hints,
            version,
            ..
        } => {
            assert_eq!(
                ObjectName::from(vec![Ident::with_quote('"', "a table")]),
                name
            );
            assert_eq!(Ident::with_quote('"', "alias"), alias.unwrap().name);
            assert!(args.is_none());
            assert!(with_hints.is_empty());
            assert!(version.is_none());
        }
        _ => panic!("Expecting TableFactor::Table"),
    }
    // check SELECT
    assert_eq!(3, select.projection.len());
    assert_eq!(
        &Expr::CompoundIdentifier(vec![
            Ident::with_quote('"', "alias"),
            Ident::with_quote('"', "bar baz"),
        ]),
        expr_from_projection(&select.projection[0]),
    );
    assert_eq!(
        &Expr::Function(Function {
            name: ObjectName::from(vec![Ident::with_quote('"', "myfun")]),
            uses_odbc_syntax: false,
            parameters: FunctionArguments::None,
            args: FunctionArguments::List(FunctionArgumentList {
                duplicate_treatment: None,
                args: vec![],
                clauses: vec![],
            }),
            filter: None,
            null_treatment: None,
            over: None,
            within_group: vec![],
        }),
        expr_from_projection(&select.projection[1]),
    );
    match &select.projection[2] {
        SelectItem::ExprWithAlias { expr, alias } => {
            assert_eq!(&Expr::Identifier(Ident::with_quote('"', "simple id")), expr);
            assert_eq!(&Ident::with_quote('"', "column alias"), alias);
        }
        _ => panic!("Expected: ExprWithAlias"),
    }

    snowflake().verified_stmt(r#"CREATE TABLE "foo" ("bar" "int")"#);
    snowflake().verified_stmt(r#"ALTER TABLE foo ADD CONSTRAINT "bar" PRIMARY KEY (baz)"#);
    //TODO verified_stmt(r#"UPDATE foo SET "bar" = 5"#);
}

#[test]
fn test_array_agg_func() {
    for sql in [
        "SELECT ARRAY_AGG(x) WITHIN GROUP (ORDER BY x) AS a FROM T",
        "SELECT ARRAY_AGG(DISTINCT x) WITHIN GROUP (ORDER BY x ASC) FROM tbl",
    ] {
        snowflake().verified_stmt(sql);
    }
}

fn snowflake() -> TestedDialects {
    TestedDialects::new(vec![Box::new(SnowflakeDialect {})])
}

fn snowflake_with_recursion_limit(recursion_limit: usize) -> TestedDialects {
    TestedDialects::new(vec![Box::new(SnowflakeDialect {})]).with_recursion_limit(recursion_limit)
}

fn snowflake_without_unescape() -> TestedDialects {
    TestedDialects::new_with_options(
        vec![Box::new(SnowflakeDialect {})],
        ParserOptions::new().with_unescape(false),
    )
}

fn snowflake_and_generic() -> TestedDialects {
    TestedDialects::new(vec![
        Box::new(SnowflakeDialect {}),
        Box::new(GenericDialect {}),
    ])
}

#[test]
fn test_select_wildcard_with_exclude() {
    let select = snowflake_and_generic().verified_only_select("SELECT * EXCLUDE (col_a) FROM data");
    let expected = SelectItem::Wildcard(WildcardAdditionalOptions {
        opt_exclude: Some(ExcludeSelectItem::Multiple(vec![Ident::new("col_a")])),
        ..Default::default()
    });
    assert_eq!(expected, select.projection[0]);

    let select = snowflake_and_generic()
        .verified_only_select("SELECT name.* EXCLUDE department_id FROM employee_table");
    let expected = SelectItem::QualifiedWildcard(
        SelectItemQualifiedWildcardKind::ObjectName(ObjectName::from(vec![Ident::new("name")])),
        WildcardAdditionalOptions {
            opt_exclude: Some(ExcludeSelectItem::Single(Ident::new("department_id"))),
            ..Default::default()
        },
    );
    assert_eq!(expected, select.projection[0]);

    let select = snowflake_and_generic()
        .verified_only_select("SELECT * EXCLUDE (department_id, employee_id) FROM employee_table");
    let expected = SelectItem::Wildcard(WildcardAdditionalOptions {
        opt_exclude: Some(ExcludeSelectItem::Multiple(vec![
            Ident::new("department_id"),
            Ident::new("employee_id"),
        ])),
        ..Default::default()
    });
    assert_eq!(expected, select.projection[0]);
}

#[test]
fn test_select_wildcard_with_rename() {
    let select =
        snowflake_and_generic().verified_only_select("SELECT * RENAME col_a AS col_b FROM data");
    let expected = SelectItem::Wildcard(WildcardAdditionalOptions {
        opt_rename: Some(RenameSelectItem::Single(IdentWithAlias {
            ident: Ident::new("col_a"),
            alias: Ident::new("col_b"),
        })),
        ..Default::default()
    });
    assert_eq!(expected, select.projection[0]);

    let select = snowflake_and_generic().verified_only_select(
        "SELECT name.* RENAME (department_id AS new_dep, employee_id AS new_emp) FROM employee_table",
    );
    let expected = SelectItem::QualifiedWildcard(
        SelectItemQualifiedWildcardKind::ObjectName(ObjectName::from(vec![Ident::new("name")])),
        WildcardAdditionalOptions {
            opt_rename: Some(RenameSelectItem::Multiple(vec![
                IdentWithAlias {
                    ident: Ident::new("department_id"),
                    alias: Ident::new("new_dep"),
                },
                IdentWithAlias {
                    ident: Ident::new("employee_id"),
                    alias: Ident::new("new_emp"),
                },
            ])),
            ..Default::default()
        },
    );
    assert_eq!(expected, select.projection[0]);
}

#[test]
fn test_select_wildcard_with_replace_and_rename() {
    let select = snowflake_and_generic().verified_only_select(
        "SELECT * REPLACE (col_z || col_z AS col_z) RENAME (col_z AS col_zz) FROM data",
    );
    let expected = SelectItem::Wildcard(WildcardAdditionalOptions {
        opt_replace: Some(ReplaceSelectItem {
            items: vec![Box::new(ReplaceSelectElement {
                expr: Expr::BinaryOp {
                    left: Box::new(Expr::Identifier(Ident::new("col_z"))),
                    op: BinaryOperator::StringConcat,
                    right: Box::new(Expr::Identifier(Ident::new("col_z"))),
                },
                column_name: Ident::new("col_z"),
                as_keyword: true,
            })],
        }),
        opt_rename: Some(RenameSelectItem::Multiple(vec![IdentWithAlias {
            ident: Ident::new("col_z"),
            alias: Ident::new("col_zz"),
        }])),
        ..Default::default()
    });
    assert_eq!(expected, select.projection[0]);

    // rename cannot precede replace
    // https://docs.snowflake.com/en/sql-reference/sql/select#parameters
    assert_eq!(
        snowflake_and_generic()
            .parse_sql_statements(
                "SELECT * RENAME (col_z AS col_zz) REPLACE (col_z || col_z AS col_z) FROM data"
            )
            .unwrap_err()
            .to_string(),
        "sql parser error: Expected: end of statement, found: REPLACE"
    );
}

#[test]
fn test_select_wildcard_with_exclude_and_rename() {
    let select = snowflake_and_generic()
        .verified_only_select("SELECT * EXCLUDE col_z RENAME col_a AS col_b FROM data");
    let expected = SelectItem::Wildcard(WildcardAdditionalOptions {
        opt_exclude: Some(ExcludeSelectItem::Single(Ident::new("col_z"))),
        opt_rename: Some(RenameSelectItem::Single(IdentWithAlias {
            ident: Ident::new("col_a"),
            alias: Ident::new("col_b"),
        })),
        ..Default::default()
    });
    assert_eq!(expected, select.projection[0]);

    // rename cannot precede exclude
    // https://docs.snowflake.com/en/sql-reference/sql/select#parameters
    assert_eq!(
        snowflake_and_generic()
            .parse_sql_statements("SELECT * RENAME col_a AS col_b EXCLUDE col_z FROM data")
            .unwrap_err()
            .to_string(),
        "sql parser error: Expected: end of statement, found: EXCLUDE"
    );
}

#[test]
fn test_alter_table_swap_with() {
    let sql = "ALTER TABLE tab1 SWAP WITH tab2";
    match alter_table_op_with_name(snowflake_and_generic().verified_stmt(sql), "tab1") {
        AlterTableOperation::SwapWith { table_name } => {
            assert_eq!("tab2", table_name.to_string());
        }
        _ => unreachable!(),
    };
}

#[test]
fn test_alter_table_clustering() {
    let sql = r#"ALTER TABLE tab CLUSTER BY (c1, "c2", TO_DATE(c3))"#;
    match alter_table_op(snowflake_and_generic().verified_stmt(sql)) {
        AlterTableOperation::ClusterBy { exprs } => {
            assert_eq!(
                exprs,
                [
                    Expr::Identifier(Ident::new("c1")),
                    Expr::Identifier(Ident::with_quote('"', "c2")),
                    Expr::Function(Function {
                        name: ObjectName::from(vec![Ident::new("TO_DATE")]),
                        uses_odbc_syntax: false,
                        parameters: FunctionArguments::None,
                        args: FunctionArguments::List(FunctionArgumentList {
                            args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(
                                Expr::Identifier(Ident::new("c3"))
                            ))],
                            duplicate_treatment: None,
                            clauses: vec![],
                        }),
                        filter: None,
                        null_treatment: None,
                        over: None,
                        within_group: vec![]
                    })
                ],
            );
        }
        _ => unreachable!(),
    }

    snowflake_and_generic().verified_stmt("ALTER TABLE tbl DROP CLUSTERING KEY");
    snowflake_and_generic().verified_stmt("ALTER TABLE tbl SUSPEND RECLUSTER");
    snowflake_and_generic().verified_stmt("ALTER TABLE tbl RESUME RECLUSTER");
}

#[test]
fn test_drop_stage() {
    match snowflake_and_generic().verified_stmt("DROP STAGE s1") {
        Statement::Drop {
            names, if_exists, ..
        } => {
            assert!(!if_exists);
            assert_eq!("s1", names[0].to_string());
        }
        _ => unreachable!(),
    };
    match snowflake_and_generic().verified_stmt("DROP STAGE IF EXISTS s1") {
        Statement::Drop {
            names, if_exists, ..
        } => {
            assert!(if_exists);
            assert_eq!("s1", names[0].to_string());
        }
        _ => unreachable!(),
    };

    snowflake_and_generic().one_statement_parses_to("DROP STAGE s1", "DROP STAGE s1");

    snowflake_and_generic()
        .one_statement_parses_to("DROP STAGE IF EXISTS s1", "DROP STAGE IF EXISTS s1");
}

#[test]
fn parse_snowflake_declare_cursor() {
    for (sql, expected_name, expected_assigned_expr, expected_query_projections) in [
        (
            "DECLARE c1 CURSOR FOR SELECT id, price FROM invoices",
            "c1",
            None,
            Some(vec!["id", "price"]),
        ),
        (
            "DECLARE c1 CURSOR FOR res",
            "c1",
            Some(DeclareAssignment::For(
                Expr::Identifier(Ident::new("res")).into(),
            )),
            None,
        ),
    ] {
        match snowflake().verified_stmt(sql) {
            Statement::Declare { mut stmts } => {
                assert_eq!(1, stmts.len());
                let Declare {
                    names,
                    data_type,
                    declare_type,
                    assignment: assigned_expr,
                    for_query,
                    ..
                } = stmts.swap_remove(0);
                assert_eq!(vec![Ident::new(expected_name)], names);
                assert!(data_type.is_none());
                assert_eq!(Some(DeclareType::Cursor), declare_type);
                assert_eq!(expected_assigned_expr, assigned_expr);
                assert_eq!(
                    expected_query_projections,
                    for_query.as_ref().map(|q| {
                        match q.body.as_ref() {
                            SetExpr::Select(q) => q
                                .projection
                                .iter()
                                .map(|item| match item {
                                    SelectItem::UnnamedExpr(Expr::Identifier(ident)) => {
                                        ident.value.as_str()
                                    }
                                    _ => unreachable!(),
                                })
                                .collect::<Vec<_>>(),
                            _ => unreachable!(),
                        }
                    })
                )
            }
            _ => unreachable!(),
        }
    }

    let error_sql = "DECLARE c1 CURSOR SELECT id FROM invoices";
    assert_eq!(
        ParserError::ParserError("Expected: FOR, found: SELECT".to_owned()),
        snowflake().parse_sql_statements(error_sql).unwrap_err()
    );

    let error_sql = "DECLARE c1 CURSOR res";
    assert_eq!(
        ParserError::ParserError("Expected: FOR, found: res".to_owned()),
        snowflake().parse_sql_statements(error_sql).unwrap_err()
    );
}

#[test]
fn parse_snowflake_declare_result_set() {
    for (sql, expected_name, expected_assigned_expr) in [
        (
            "DECLARE res RESULTSET DEFAULT 42",
            "res",
            Some(DeclareAssignment::Default(Expr::Value(number("42")).into())),
        ),
        (
            "DECLARE res RESULTSET := 42",
            "res",
            Some(DeclareAssignment::DuckAssignment(
                Expr::Value(number("42")).into(),
            )),
        ),
        ("DECLARE res RESULTSET", "res", None),
    ] {
        match snowflake().verified_stmt(sql) {
            Statement::Declare { mut stmts } => {
                assert_eq!(1, stmts.len());
                let Declare {
                    names,
                    data_type,
                    declare_type,
                    assignment: assigned_expr,
                    for_query,
                    ..
                } = stmts.swap_remove(0);
                assert_eq!(vec![Ident::new(expected_name)], names);
                assert!(data_type.is_none());
                assert!(for_query.is_none());
                assert_eq!(Some(DeclareType::ResultSet), declare_type);
                assert_eq!(expected_assigned_expr, assigned_expr);
            }
            _ => unreachable!(),
        }
    }

    let sql = "DECLARE res RESULTSET DEFAULT (SELECT price FROM invoices)";
    assert_eq!(snowflake().verified_stmt(sql).to_string(), sql);

    let error_sql = "DECLARE res RESULTSET DEFAULT";
    assert_eq!(
        ParserError::ParserError("Expected: an expression, found: EOF".to_owned()),
        snowflake().parse_sql_statements(error_sql).unwrap_err()
    );

    let error_sql = "DECLARE res RESULTSET :=";
    assert_eq!(
        ParserError::ParserError("Expected: an expression, found: EOF".to_owned()),
        snowflake().parse_sql_statements(error_sql).unwrap_err()
    );
}

#[test]
fn parse_snowflake_declare_exception() {
    for (sql, expected_name, expected_assigned_expr) in [
        (
            "DECLARE ex EXCEPTION (42, 'ERROR')",
            "ex",
            Some(DeclareAssignment::Expr(
                Expr::Tuple(vec![
                    Expr::Value(number("42")),
                    Expr::Value(Value::SingleQuotedString("ERROR".to_string())),
                ])
                .into(),
            )),
        ),
        ("DECLARE ex EXCEPTION", "ex", None),
    ] {
        match snowflake().verified_stmt(sql) {
            Statement::Declare { mut stmts } => {
                assert_eq!(1, stmts.len());
                let Declare {
                    names,
                    data_type,
                    declare_type,
                    assignment: assigned_expr,
                    for_query,
                    ..
                } = stmts.swap_remove(0);
                assert_eq!(vec![Ident::new(expected_name)], names);
                assert!(data_type.is_none());
                assert!(for_query.is_none());
                assert_eq!(Some(DeclareType::Exception), declare_type);
                assert_eq!(expected_assigned_expr, assigned_expr);
            }
            _ => unreachable!(),
        }
    }
}

#[test]
fn parse_snowflake_declare_variable() {
    for (sql, expected_name, expected_data_type, expected_assigned_expr) in [
        (
            "DECLARE profit TEXT DEFAULT 42",
            "profit",
            Some(DataType::Text),
            Some(DeclareAssignment::Default(Expr::Value(number("42")).into())),
        ),
        (
            "DECLARE profit DEFAULT 42",
            "profit",
            None,
            Some(DeclareAssignment::Default(Expr::Value(number("42")).into())),
        ),
        ("DECLARE profit TEXT", "profit", Some(DataType::Text), None),
        ("DECLARE profit", "profit", None, None),
    ] {
        match snowflake().verified_stmt(sql) {
            Statement::Declare { mut stmts } => {
                assert_eq!(1, stmts.len());
                let Declare {
                    names,
                    data_type,
                    declare_type,
                    assignment: assigned_expr,
                    for_query,
                    ..
                } = stmts.swap_remove(0);
                assert_eq!(vec![Ident::new(expected_name)], names);
                assert!(for_query.is_none());
                assert_eq!(expected_data_type, data_type);
                assert_eq!(None, declare_type);
                assert_eq!(expected_assigned_expr, assigned_expr);
            }
            _ => unreachable!(),
        }
    }

    snowflake().one_statement_parses_to("DECLARE profit;", "DECLARE profit");

    let error_sql = "DECLARE profit INT 2";
    assert_eq!(
        ParserError::ParserError("Expected: end of statement, found: 2".to_owned()),
        snowflake().parse_sql_statements(error_sql).unwrap_err()
    );

    let error_sql = "DECLARE profit INT DEFAULT";
    assert_eq!(
        ParserError::ParserError("Expected: an expression, found: EOF".to_owned()),
        snowflake().parse_sql_statements(error_sql).unwrap_err()
    );

    let error_sql = "DECLARE profit DEFAULT";
    assert_eq!(
        ParserError::ParserError("Expected: an expression, found: EOF".to_owned()),
        snowflake().parse_sql_statements(error_sql).unwrap_err()
    );
}

#[test]
fn parse_snowflake_declare_multi_statements() {
    let sql = concat!(
        "DECLARE profit DEFAULT 42; ",
        "res RESULTSET DEFAULT (SELECT price FROM invoices); ",
        "c1 CURSOR FOR res; ",
        "ex EXCEPTION (-20003, 'ERROR: Could not create table.')"
    );
    match snowflake().verified_stmt(sql) {
        Statement::Declare { stmts } => {
            let actual = stmts
                .iter()
                .map(|stmt| (stmt.names[0].value.as_str(), stmt.declare_type.clone()))
                .collect::<Vec<_>>();

            assert_eq!(
                vec![
                    ("profit", None),
                    ("res", Some(DeclareType::ResultSet)),
                    ("c1", Some(DeclareType::Cursor)),
                    ("ex", Some(DeclareType::Exception)),
                ],
                actual
            );
        }
        _ => unreachable!(),
    }

    let error_sql = "DECLARE profit DEFAULT 42 c1 CURSOR FOR res;";
    assert_eq!(
        ParserError::ParserError("Expected: end of statement, found: c1".to_owned()),
        snowflake().parse_sql_statements(error_sql).unwrap_err()
    );
}

#[test]
fn test_create_stage() {
    let sql = "CREATE STAGE s1.s2";
    match snowflake().verified_stmt(sql) {
        Statement::CreateStage {
            or_replace,
            temporary,
            if_not_exists,
            name,
            comment,
            ..
        } => {
            assert!(!or_replace);
            assert!(!temporary);
            assert!(!if_not_exists);
            assert_eq!("s1.s2", name.to_string());
            assert!(comment.is_none());
        }
        _ => unreachable!(),
    };
    assert_eq!(snowflake().verified_stmt(sql).to_string(), sql);

    let extended_sql = concat!(
        "CREATE OR REPLACE TEMPORARY STAGE IF NOT EXISTS s1.s2 ",
        "COMMENT='some-comment'"
    );
    match snowflake().verified_stmt(extended_sql) {
        Statement::CreateStage {
            or_replace,
            temporary,
            if_not_exists,
            name,
            stage_params,
            comment,
            ..
        } => {
            assert!(or_replace);
            assert!(temporary);
            assert!(if_not_exists);
            assert!(stage_params.url.is_none());
            assert!(stage_params.endpoint.is_none());
            assert_eq!("s1.s2", name.to_string());
            assert_eq!("some-comment", comment.unwrap());
        }
        _ => unreachable!(),
    };
    assert_eq!(
        snowflake().verified_stmt(extended_sql).to_string(),
        extended_sql
    );
}

#[test]
fn test_create_stage_with_stage_params() {
    let sql = concat!(
        "CREATE OR REPLACE STAGE my_ext_stage ",
        "URL='s3://load/files/' ",
        "STORAGE_INTEGRATION=myint ",
        "ENDPOINT='<s3_api_compatible_endpoint>' ",
        "CREDENTIALS=(AWS_KEY_ID='1a2b3c' AWS_SECRET_KEY='4x5y6z') ",
        "ENCRYPTION=(MASTER_KEY='key' TYPE='AWS_SSE_KMS')"
    );

    match snowflake().verified_stmt(sql) {
        Statement::CreateStage { stage_params, .. } => {
            assert_eq!("s3://load/files/", stage_params.url.unwrap());
            assert_eq!("myint", stage_params.storage_integration.unwrap());
            assert_eq!(
                "<s3_api_compatible_endpoint>",
                stage_params.endpoint.unwrap()
            );
            assert!(stage_params
                .credentials
                .options
                .contains(&DataLoadingOption {
                    option_name: "AWS_KEY_ID".to_string(),
                    option_type: DataLoadingOptionType::STRING,
                    value: "1a2b3c".to_string()
                }));
            assert!(stage_params
                .credentials
                .options
                .contains(&DataLoadingOption {
                    option_name: "AWS_SECRET_KEY".to_string(),
                    option_type: DataLoadingOptionType::STRING,
                    value: "4x5y6z".to_string()
                }));
            assert!(stage_params
                .encryption
                .options
                .contains(&DataLoadingOption {
                    option_name: "MASTER_KEY".to_string(),
                    option_type: DataLoadingOptionType::STRING,
                    value: "key".to_string()
                }));
            assert!(stage_params
                .encryption
                .options
                .contains(&DataLoadingOption {
                    option_name: "TYPE".to_string(),
                    option_type: DataLoadingOptionType::STRING,
                    value: "AWS_SSE_KMS".to_string()
                }));
        }
        _ => unreachable!(),
    };

    assert_eq!(snowflake().verified_stmt(sql).to_string(), sql);
}

#[test]
fn test_create_stage_with_directory_table_params() {
    let sql = concat!(
        "CREATE OR REPLACE STAGE my_ext_stage ",
        "URL='s3://load/files/' ",
        "DIRECTORY=(ENABLE=TRUE REFRESH_ON_CREATE=FALSE NOTIFICATION_INTEGRATION='some-string')"
    );

    match snowflake().verified_stmt(sql) {
        Statement::CreateStage {
            directory_table_params,
            ..
        } => {
            assert!(directory_table_params.options.contains(&DataLoadingOption {
                option_name: "ENABLE".to_string(),
                option_type: DataLoadingOptionType::BOOLEAN,
                value: "TRUE".to_string()
            }));
            assert!(directory_table_params.options.contains(&DataLoadingOption {
                option_name: "REFRESH_ON_CREATE".to_string(),
                option_type: DataLoadingOptionType::BOOLEAN,
                value: "FALSE".to_string()
            }));
            assert!(directory_table_params.options.contains(&DataLoadingOption {
                option_name: "NOTIFICATION_INTEGRATION".to_string(),
                option_type: DataLoadingOptionType::STRING,
                value: "some-string".to_string()
            }));
        }
        _ => unreachable!(),
    };
    assert_eq!(snowflake().verified_stmt(sql).to_string(), sql);
}

#[test]
fn test_create_stage_with_file_format() {
    let sql = concat!(
        "CREATE OR REPLACE STAGE my_ext_stage ",
        "URL='s3://load/files/' ",
        r#"FILE_FORMAT=(COMPRESSION=AUTO BINARY_FORMAT=HEX ESCAPE='\\')"#
    );

    match snowflake_without_unescape().verified_stmt(sql) {
        Statement::CreateStage { file_format, .. } => {
            assert!(file_format.options.contains(&DataLoadingOption {
                option_name: "COMPRESSION".to_string(),
                option_type: DataLoadingOptionType::ENUM,
                value: "AUTO".to_string()
            }));
            assert!(file_format.options.contains(&DataLoadingOption {
                option_name: "BINARY_FORMAT".to_string(),
                option_type: DataLoadingOptionType::ENUM,
                value: "HEX".to_string()
            }));
            assert!(file_format.options.contains(&DataLoadingOption {
                option_name: "ESCAPE".to_string(),
                option_type: DataLoadingOptionType::STRING,
                value: r#"\\"#.to_string()
            }));
        }
        _ => unreachable!(),
    };
    assert_eq!(
        snowflake_without_unescape().verified_stmt(sql).to_string(),
        sql
    );
}

#[test]
fn test_create_stage_with_copy_options() {
    let sql = concat!(
        "CREATE OR REPLACE STAGE my_ext_stage ",
        "URL='s3://load/files/' ",
        "COPY_OPTIONS=(ON_ERROR=CONTINUE FORCE=TRUE)"
    );
    match snowflake().verified_stmt(sql) {
        Statement::CreateStage { copy_options, .. } => {
            assert!(copy_options.options.contains(&DataLoadingOption {
                option_name: "ON_ERROR".to_string(),
                option_type: DataLoadingOptionType::ENUM,
                value: "CONTINUE".to_string()
            }));
            assert!(copy_options.options.contains(&DataLoadingOption {
                option_name: "FORCE".to_string(),
                option_type: DataLoadingOptionType::BOOLEAN,
                value: "TRUE".to_string()
            }));
        }
        _ => unreachable!(),
    };
    assert_eq!(snowflake().verified_stmt(sql).to_string(), sql);
}

#[test]
fn test_copy_into() {
    let sql = concat!(
        "COPY INTO my_company.emp_basic ",
        "FROM 'gcs://mybucket/./../a.csv'"
    );
    match snowflake().verified_stmt(sql) {
        Statement::CopyIntoSnowflake {
            kind,
            into,
            from_obj,
            files,
            pattern,
            validation_mode,
            ..
        } => {
            assert_eq!(kind, CopyIntoSnowflakeKind::Table);
            assert_eq!(
                into,
                ObjectName::from(vec![Ident::new("my_company"), Ident::new("emp_basic")])
            );
            assert_eq!(
                from_obj,
                Some(ObjectName::from(vec![Ident::with_quote(
                    '\'',
                    "gcs://mybucket/./../a.csv"
                )]))
            );
            assert!(files.is_none());
            assert!(pattern.is_none());
            assert!(validation_mode.is_none());
        }
        _ => unreachable!(),
    };
    assert_eq!(snowflake().verified_stmt(sql).to_string(), sql);

    let sql = concat!("COPY INTO 's3://a/b/c/data.parquet' ", "FROM db.sc.tbl ", "PARTITION BY ('date=' || to_varchar(dt, 'YYYY-MM-DD') || '/hour=' || to_varchar(date_part(hour, ts)))");
    match snowflake().verified_stmt(sql) {
        Statement::CopyIntoSnowflake {
            kind,
            into,
            from_obj,
            from_query,
            partition,
            ..
        } => {
            assert_eq!(kind, CopyIntoSnowflakeKind::Location);
            assert_eq!(
                into,
                ObjectName::from(vec![Ident::with_quote('\'', "s3://a/b/c/data.parquet")])
            );
            assert_eq!(
                from_obj,
                Some(ObjectName::from(vec![
                    Ident::new("db"),
                    Ident::new("sc"),
                    Ident::new("tbl")
                ]))
            );
            assert!(from_query.is_none());
            assert!(partition.is_some());
        }
        _ => unreachable!(),
    };
    assert_eq!(snowflake().verified_stmt(sql).to_string(), sql);

    let sql = concat!(
        "COPY INTO 's3://a/b/c/data.parquet' ",
        "FROM (SELECT * FROM tbl)"
    );
    match snowflake().verified_stmt(sql) {
        Statement::CopyIntoSnowflake {
            kind,
            into,
            from_obj,
            from_query,
            ..
        } => {
            assert_eq!(kind, CopyIntoSnowflakeKind::Location);
            assert_eq!(
                into,
                ObjectName::from(vec![Ident::with_quote('\'', "s3://a/b/c/data.parquet")])
            );
            assert!(from_query.is_some());
            assert!(from_obj.is_none());
        }
        _ => unreachable!(),
    };
    assert_eq!(snowflake().verified_stmt(sql).to_string(), sql);
}

#[test]
fn test_copy_into_with_stage_params() {
    let sql = concat!(
        "COPY INTO my_company.emp_basic ",
        "FROM 's3://load/files/' ",
        "STORAGE_INTEGRATION=myint ",
        "ENDPOINT='<s3_api_compatible_endpoint>' ",
        "CREDENTIALS=(AWS_KEY_ID='1a2b3c' AWS_SECRET_KEY='4x5y6z') ",
        "ENCRYPTION=(MASTER_KEY='key' TYPE='AWS_SSE_KMS')"
    );

    match snowflake().verified_stmt(sql) {
        Statement::CopyIntoSnowflake {
            from_obj,
            stage_params,
            ..
        } => {
            //assert_eq!("s3://load/files/", stage_params.url.unwrap());
            assert_eq!(
                from_obj,
                Some(ObjectName::from(vec![Ident::with_quote(
                    '\'',
                    "s3://load/files/"
                )]))
            );
            assert_eq!("myint", stage_params.storage_integration.unwrap());
            assert_eq!(
                "<s3_api_compatible_endpoint>",
                stage_params.endpoint.unwrap()
            );
            assert!(stage_params
                .credentials
                .options
                .contains(&DataLoadingOption {
                    option_name: "AWS_KEY_ID".to_string(),
                    option_type: DataLoadingOptionType::STRING,
                    value: "1a2b3c".to_string()
                }));
            assert!(stage_params
                .credentials
                .options
                .contains(&DataLoadingOption {
                    option_name: "AWS_SECRET_KEY".to_string(),
                    option_type: DataLoadingOptionType::STRING,
                    value: "4x5y6z".to_string()
                }));
            assert!(stage_params
                .encryption
                .options
                .contains(&DataLoadingOption {
                    option_name: "MASTER_KEY".to_string(),
                    option_type: DataLoadingOptionType::STRING,
                    value: "key".to_string()
                }));
            assert!(stage_params
                .encryption
                .options
                .contains(&DataLoadingOption {
                    option_name: "TYPE".to_string(),
                    option_type: DataLoadingOptionType::STRING,
                    value: "AWS_SSE_KMS".to_string()
                }));
        }
        _ => unreachable!(),
    };

    assert_eq!(snowflake().verified_stmt(sql).to_string(), sql);

    // stage params within copy into with transformations
    let sql = concat!(
        "COPY INTO my_company.emp_basic FROM ",
        "(SELECT t1.$1 FROM 's3://load/files/' STORAGE_INTEGRATION=myint)",
    );

    match snowflake().verified_stmt(sql) {
        Statement::CopyIntoSnowflake {
            from_obj,
            stage_params,
            ..
        } => {
            assert_eq!(
                from_obj,
                Some(ObjectName::from(vec![Ident::with_quote(
                    '\'',
                    "s3://load/files/"
                )]))
            );
            assert_eq!("myint", stage_params.storage_integration.unwrap());
        }
        _ => unreachable!(),
    }
}

#[test]
fn test_copy_into_with_files_and_pattern_and_verification() {
    let sql = concat!(
        "COPY INTO my_company.emp_basic ",
        "FROM 'gcs://mybucket/./../a.csv' AS some_alias ",
        "FILES = ('file1.json', 'file2.json') ",
        "PATTERN = '.*employees0[1-5].csv.gz' ",
        "VALIDATION_MODE = RETURN_7_ROWS"
    );

    match snowflake().verified_stmt(sql) {
        Statement::CopyIntoSnowflake {
            files,
            pattern,
            validation_mode,
            from_obj_alias,
            ..
        } => {
            assert_eq!(files.unwrap(), vec!["file1.json", "file2.json"]);
            assert_eq!(pattern.unwrap(), ".*employees0[1-5].csv.gz");
            assert_eq!(validation_mode.unwrap(), "RETURN_7_ROWS");
            assert_eq!(from_obj_alias.unwrap(), Ident::new("some_alias"));
        }
        _ => unreachable!(),
    }
    assert_eq!(snowflake().verified_stmt(sql).to_string(), sql);
}

#[test]
fn test_copy_into_with_transformations() {
    let sql = concat!(
        "COPY INTO my_company.emp_basic FROM ",
        "(SELECT t1.$1:st AS st, $1:index, t2.$1 FROM @schema.general_finished AS T) ",
        "FILES = ('file1.json', 'file2.json') ",
        "PATTERN = '.*employees0[1-5].csv.gz' ",
        "VALIDATION_MODE = RETURN_7_ROWS"
    );

    match snowflake().verified_stmt(sql) {
        Statement::CopyIntoSnowflake {
            from_obj,
            from_transformations,
            ..
        } => {
            assert_eq!(
                from_obj,
                Some(ObjectName::from(vec![
                    Ident::new("@schema"),
                    Ident::new("general_finished")
                ]))
            );
            assert_eq!(
                from_transformations.as_ref().unwrap()[0],
                StageLoadSelectItem {
                    alias: Some(Ident::new("t1")),
                    file_col_num: 1,
                    element: Some(Ident::new("st")),
                    item_as: Some(Ident::new("st"))
                }
            );
            assert_eq!(
                from_transformations.as_ref().unwrap()[1],
                StageLoadSelectItem {
                    alias: None,
                    file_col_num: 1,
                    element: Some(Ident::new("index")),
                    item_as: None
                }
            );
            assert_eq!(
                from_transformations.as_ref().unwrap()[2],
                StageLoadSelectItem {
                    alias: Some(Ident::new("t2")),
                    file_col_num: 1,
                    element: None,
                    item_as: None
                }
            );
        }
        _ => unreachable!(),
    }
    assert_eq!(snowflake().verified_stmt(sql).to_string(), sql);
}

#[test]
fn test_copy_into_file_format() {
    let sql = concat!(
        "COPY INTO my_company.emp_basic ",
        "FROM 'gcs://mybucket/./../a.csv' ",
        "FILES = ('file1.json', 'file2.json') ",
        "PATTERN = '.*employees0[1-5].csv.gz' ",
        r#"FILE_FORMAT=(COMPRESSION=AUTO BINARY_FORMAT=HEX ESCAPE='\\')"#
    );

    match snowflake_without_unescape().verified_stmt(sql) {
        Statement::CopyIntoSnowflake { file_format, .. } => {
            assert!(file_format.options.contains(&DataLoadingOption {
                option_name: "COMPRESSION".to_string(),
                option_type: DataLoadingOptionType::ENUM,
                value: "AUTO".to_string()
            }));
            assert!(file_format.options.contains(&DataLoadingOption {
                option_name: "BINARY_FORMAT".to_string(),
                option_type: DataLoadingOptionType::ENUM,
                value: "HEX".to_string()
            }));
            assert!(file_format.options.contains(&DataLoadingOption {
                option_name: "ESCAPE".to_string(),
                option_type: DataLoadingOptionType::STRING,
                value: r#"\\"#.to_string()
            }));
        }
        _ => unreachable!(),
    }
    assert_eq!(
        snowflake_without_unescape().verified_stmt(sql).to_string(),
        sql
    );

    // Test commas in file format
    let sql = concat!(
        "COPY INTO my_company.emp_basic ",
        "FROM 'gcs://mybucket/./../a.csv' ",
        "FILES = ('file1.json', 'file2.json') ",
        "PATTERN = '.*employees0[1-5].csv.gz' ",
        r#"FILE_FORMAT=(COMPRESSION=AUTO, BINARY_FORMAT=HEX, ESCAPE='\\')"#
    );

    match snowflake_without_unescape()
        .parse_sql_statements(sql)
        .unwrap()
        .first()
        .unwrap()
    {
        Statement::CopyIntoSnowflake { file_format, .. } => {
            assert!(file_format.options.contains(&DataLoadingOption {
                option_name: "COMPRESSION".to_string(),
                option_type: DataLoadingOptionType::ENUM,
                value: "AUTO".to_string()
            }));
            assert!(file_format.options.contains(&DataLoadingOption {
                option_name: "BINARY_FORMAT".to_string(),
                option_type: DataLoadingOptionType::ENUM,
                value: "HEX".to_string()
            }));
            assert!(file_format.options.contains(&DataLoadingOption {
                option_name: "ESCAPE".to_string(),
                option_type: DataLoadingOptionType::STRING,
                value: r#"\\"#.to_string()
            }));
        }
        _ => unreachable!(),
    }
}

#[test]
fn test_copy_into_copy_options() {
    let sql = concat!(
        "COPY INTO my_company.emp_basic ",
        "FROM 'gcs://mybucket/./../a.csv' ",
        "FILES = ('file1.json', 'file2.json') ",
        "PATTERN = '.*employees0[1-5].csv.gz' ",
        "COPY_OPTIONS=(ON_ERROR=CONTINUE FORCE=TRUE)"
    );

    match snowflake().verified_stmt(sql) {
        Statement::CopyIntoSnowflake { copy_options, .. } => {
            assert!(copy_options.options.contains(&DataLoadingOption {
                option_name: "ON_ERROR".to_string(),
                option_type: DataLoadingOptionType::ENUM,
                value: "CONTINUE".to_string()
            }));
            assert!(copy_options.options.contains(&DataLoadingOption {
                option_name: "FORCE".to_string(),
                option_type: DataLoadingOptionType::BOOLEAN,
                value: "TRUE".to_string()
            }));
        }
        _ => unreachable!(),
    };
    assert_eq!(snowflake().verified_stmt(sql).to_string(), sql);
}

#[test]
fn test_snowflake_stage_object_names_into_location() {
    let mut allowed_object_names = [
        ObjectName::from(vec![Ident::new("@namespace"), Ident::new("%table_name")]),
        ObjectName::from(vec![
            Ident::new("@namespace"),
            Ident::new("%table_name/path"),
        ]),
        ObjectName::from(vec![
            Ident::new("@namespace"),
            Ident::new("stage_name/path"),
        ]),
        ObjectName::from(vec![Ident::new("@~/path")]),
    ];

    let allowed_names_into_location = [
        "@namespace.%table_name",
        "@namespace.%table_name/path",
        "@namespace.stage_name/path",
        "@~/path",
    ];
    for it in allowed_names_into_location
        .iter()
        .zip(allowed_object_names.iter_mut())
    {
        let (formatted_name, object_name) = it;
        let sql = format!(
            "COPY INTO {} FROM 'gcs://mybucket/./../a.csv'",
            formatted_name
        );
        match snowflake().verified_stmt(&sql) {
            Statement::CopyIntoSnowflake { into, .. } => {
                assert_eq!(into.0, object_name.0)
            }
            _ => unreachable!(),
        }
    }
}

#[test]
fn test_snowflake_stage_object_names_into_table() {
    let mut allowed_object_names = [
        ObjectName::from(vec![Ident::new("my_company"), Ident::new("emp_basic")]),
        ObjectName::from(vec![Ident::new("emp_basic")]),
    ];

    let allowed_names_into_table = ["my_company.emp_basic", "emp_basic"];
    for it in allowed_names_into_table
        .iter()
        .zip(allowed_object_names.iter_mut())
    {
        let (formatted_name, object_name) = it;
        let sql = format!(
            "COPY INTO {} FROM 'gcs://mybucket/./../a.csv'",
            formatted_name
        );
        match snowflake().verified_stmt(&sql) {
            Statement::CopyIntoSnowflake { into, .. } => {
                assert_eq!(into.0, object_name.0)
            }
            _ => unreachable!(),
        }
    }
}

#[test]
fn test_snowflake_copy_into() {
    let sql = "COPY INTO a.b FROM @namespace.stage_name";
    assert_eq!(snowflake().verified_stmt(sql).to_string(), sql);
    match snowflake().verified_stmt(sql) {
        Statement::CopyIntoSnowflake { into, from_obj, .. } => {
            assert_eq!(
                into,
                ObjectName::from(vec![Ident::new("a"), Ident::new("b")])
            );
            assert_eq!(
                from_obj,
                Some(ObjectName::from(vec![
                    Ident::new("@namespace"),
                    Ident::new("stage_name")
                ]))
            )
        }
        _ => unreachable!(),
    }
}

#[test]
fn test_snowflake_copy_into_stage_name_ends_with_parens() {
    let sql = "COPY INTO SCHEMA.SOME_MONITORING_SYSTEM FROM (SELECT t.$1:st AS st FROM @schema.general_finished)";
    assert_eq!(snowflake().verified_stmt(sql).to_string(), sql);
    match snowflake().verified_stmt(sql) {
        Statement::CopyIntoSnowflake { into, from_obj, .. } => {
            assert_eq!(
                into,
                ObjectName::from(vec![
                    Ident::new("SCHEMA"),
                    Ident::new("SOME_MONITORING_SYSTEM")
                ])
            );
            assert_eq!(
                from_obj,
                Some(ObjectName::from(vec![
                    Ident::new("@schema"),
                    Ident::new("general_finished")
                ]))
            )
        }
        _ => unreachable!(),
    }
}

#[test]
fn test_snowflake_trim() {
    let real_sql = r#"SELECT customer_id, TRIM(sub_items.value:item_price_id, '"', "a") AS item_price_id FROM models_staging.subscriptions"#;
    assert_eq!(snowflake().verified_stmt(real_sql).to_string(), real_sql);

    let sql_only_select = "SELECT TRIM('xyz', 'a')";
    let select = snowflake().verified_only_select(sql_only_select);
    assert_eq!(
        &Expr::Trim {
            expr: Box::new(Expr::Value(Value::SingleQuotedString("xyz".to_owned()))),
            trim_where: None,
            trim_what: None,
            trim_characters: Some(vec![Expr::Value(Value::SingleQuotedString("a".to_owned()))]),
        },
        expr_from_projection(only(&select.projection))
    );

    // missing comma separation
    let error_sql = "SELECT TRIM('xyz' 'a')";
    assert_eq!(
        ParserError::ParserError("Expected: ), found: 'a'".to_owned()),
        snowflake().parse_sql_statements(error_sql).unwrap_err()
    );
}

#[test]
fn test_number_placeholder() {
    let sql_only_select = "SELECT :1";
    let select = snowflake().verified_only_select(sql_only_select);
    assert_eq!(
        &Expr::Value(Value::Placeholder(":1".into())),
        expr_from_projection(only(&select.projection))
    );

    snowflake()
        .parse_sql_statements("alter role 1 with name = 'foo'")
        .expect_err("should have failed");
}

#[test]
fn parse_position_not_function_columns() {
    snowflake_and_generic()
        .verified_stmt("SELECT position FROM tbl1 WHERE position NOT IN ('first', 'last')");
}

#[test]
fn parse_subquery_function_argument() {
    // Snowflake allows passing an unparenthesized subquery as the single
    // argument to a function.
    snowflake().verified_stmt("SELECT parse_json(SELECT '{}')");

    // Subqueries that begin with WITH work too.
    snowflake()
        .verified_stmt("SELECT parse_json(WITH q AS (SELECT '{}' AS foo) SELECT foo FROM q)");

    // Commas are parsed as part of the subquery, not additional arguments to
    // the function.
    snowflake().verified_stmt("SELECT func(SELECT 1, 2)");
}

#[test]
fn parse_division_correctly() {
    snowflake_and_generic().one_statement_parses_to(
        "SELECT field/1000 FROM tbl1",
        "SELECT field / 1000 FROM tbl1",
    );

    snowflake_and_generic().one_statement_parses_to(
        "SELECT tbl1.field/tbl2.field FROM tbl1 JOIN tbl2 ON tbl1.id = tbl2.entity_id",
        "SELECT tbl1.field / tbl2.field FROM tbl1 JOIN tbl2 ON tbl1.id = tbl2.entity_id",
    );
}

#[test]
fn parse_pivot_of_table_factor_derived() {
    snowflake().verified_stmt(
        "SELECT * FROM (SELECT place_id, weekday, open FROM times AS p) PIVOT(max(open) FOR weekday IN (0, 1, 2, 3, 4, 5, 6)) AS p (place_id, open_sun, open_mon, open_tue, open_wed, open_thu, open_fri, open_sat)"
    );
}

#[test]
fn parse_top() {
    snowflake().one_statement_parses_to(
        "SELECT TOP 4 c1 FROM testtable",
        "SELECT TOP 4 c1 FROM testtable",
    );
}

#[test]
fn parse_extract_custom_part() {
    let sql = "SELECT EXTRACT(eod FROM d)";
    let select = snowflake_and_generic().verified_only_select(sql);
    assert_eq!(
        &Expr::Extract {
            field: DateTimeField::Custom(Ident::new("eod")),
            syntax: ExtractSyntax::From,
            expr: Box::new(Expr::Identifier(Ident::new("d"))),
        },
        expr_from_projection(only(&select.projection)),
    );
}

#[test]
fn parse_extract_comma() {
    let sql = "SELECT EXTRACT(HOUR, d)";
    let select = snowflake_and_generic().verified_only_select(sql);
    assert_eq!(
        &Expr::Extract {
            field: DateTimeField::Hour,
            syntax: ExtractSyntax::Comma,
            expr: Box::new(Expr::Identifier(Ident::new("d"))),
        },
        expr_from_projection(only(&select.projection)),
    );
}

#[test]
fn parse_extract_comma_quoted() {
    let sql = "SELECT EXTRACT('hour', d)";
    let select = snowflake_and_generic().verified_only_select(sql);
    assert_eq!(
        &Expr::Extract {
            field: DateTimeField::Custom(Ident::with_quote('\'', "hour")),
            syntax: ExtractSyntax::Comma,
            expr: Box::new(Expr::Identifier(Ident::new("d"))),
        },
        expr_from_projection(only(&select.projection)),
    );
}

#[test]
fn parse_comma_outer_join() {
    // compound identifiers
    let case1 =
        snowflake().verified_only_select("SELECT t1.c1, t2.c2 FROM t1, t2 WHERE t1.c1 = t2.c2 (+)");
    assert_eq!(
        case1.selection,
        Some(Expr::BinaryOp {
            left: Box::new(Expr::CompoundIdentifier(vec![
                Ident::new("t1"),
                Ident::new("c1")
            ])),
            op: BinaryOperator::Eq,
            right: Box::new(Expr::OuterJoin(Box::new(Expr::CompoundIdentifier(vec![
                Ident::new("t2"),
                Ident::new("c2")
            ]))))
        })
    );

    // regular identifiers
    let case2 =
        snowflake().verified_only_select("SELECT t1.c1, t2.c2 FROM t1, t2 WHERE c1 = c2 (+)");
    assert_eq!(
        case2.selection,
        Some(Expr::BinaryOp {
            left: Box::new(Expr::Identifier(Ident::new("c1"))),
            op: BinaryOperator::Eq,
            right: Box::new(Expr::OuterJoin(Box::new(Expr::Identifier(Ident::new(
                "c2"
            )))))
        })
    );

    // ensure we can still parse function calls with a unary plus arg
    let case3 =
        snowflake().verified_only_select("SELECT t1.c1, t2.c2 FROM t1, t2 WHERE c1 = myudf(+42)");
    assert_eq!(
        case3.selection,
        Some(Expr::BinaryOp {
            left: Box::new(Expr::Identifier(Ident::new("c1"))),
            op: BinaryOperator::Eq,
            right: Box::new(call(
                "myudf",
                [Expr::UnaryOp {
                    op: UnaryOperator::Plus,
                    expr: Box::new(Expr::Value(number("42")))
                }]
            )),
        })
    );

    // permissive with whitespace
    snowflake().verified_only_select_with_canonical(
        "SELECT t1.c1, t2.c2 FROM t1, t2 WHERE t1.c1 = t2.c2(   +     )",
        "SELECT t1.c1, t2.c2 FROM t1, t2 WHERE t1.c1 = t2.c2 (+)",
    );
}

#[test]
fn test_sf_trailing_commas() {
    snowflake().verified_only_select_with_canonical("SELECT 1, 2, FROM t", "SELECT 1, 2 FROM t");
}

#[test]
fn test_select_wildcard_with_ilike() {
    let select = snowflake_and_generic().verified_only_select(r#"SELECT * ILIKE '%id%' FROM tbl"#);
    let expected = SelectItem::Wildcard(WildcardAdditionalOptions {
        opt_ilike: Some(IlikeSelectItem {
            pattern: "%id%".to_owned(),
        }),
        ..Default::default()
    });
    assert_eq!(expected, select.projection[0]);
}

#[test]
fn test_select_wildcard_with_ilike_double_quote() {
    let res = snowflake().parse_sql_statements(r#"SELECT * ILIKE "%id" FROM tbl"#);
    assert_eq!(
        res.unwrap_err().to_string(),
        "sql parser error: Expected: ilike pattern, found: \"%id\""
    );
}

#[test]
fn test_select_wildcard_with_ilike_number() {
    let res = snowflake().parse_sql_statements(r#"SELECT * ILIKE 42 FROM tbl"#);
    assert_eq!(
        res.unwrap_err().to_string(),
        "sql parser error: Expected: ilike pattern, found: 42"
    );
}

#[test]
fn test_select_wildcard_with_ilike_replace() {
    let res = snowflake().parse_sql_statements(r#"SELECT * ILIKE '%id%' EXCLUDE col FROM tbl"#);
    assert_eq!(
        res.unwrap_err().to_string(),
        "sql parser error: Expected: end of statement, found: EXCLUDE"
    );
}

#[test]
fn first_value_ignore_nulls() {
    snowflake().verified_only_select(concat!(
        "SELECT FIRST_VALUE(column2 IGNORE NULLS) ",
        "OVER (PARTITION BY column1 ORDER BY column2) ",
        "FROM some_table"
    ));
}

#[test]
fn test_pivot() {
    // pivot on static list of values with default
    #[rustfmt::skip]
    snowflake().verified_only_select(concat!(
        "SELECT * ",
        "FROM quarterly_sales ",
          "PIVOT(SUM(amount) ",
            "FOR quarter IN (",
              "'2023_Q1', ",
              "'2023_Q2', ",
              "'2023_Q3', ",
              "'2023_Q4', ",
              "'2024_Q1') ",
            "DEFAULT ON NULL (0)",
          ") ",
        "ORDER BY empid",
    ));

    // dynamic pivot from subquery
    #[rustfmt::skip]
    snowflake().verified_only_select(concat!(
        "SELECT * ",
        "FROM quarterly_sales ",
          "PIVOT(SUM(amount) FOR quarter IN (",
            "SELECT DISTINCT quarter ",
              "FROM ad_campaign_types_by_quarter ",
              "WHERE television = true ",
              "ORDER BY quarter)",
          ") ",
        "ORDER BY empid",
    ));

    // dynamic pivot on any value (with order by)
    #[rustfmt::skip]
    snowflake().verified_only_select(concat!(
        "SELECT * ",
        "FROM quarterly_sales ",
          "PIVOT(SUM(amount) FOR quarter IN (ANY ORDER BY quarter)) ",
        "ORDER BY empid",
    ));

    // dynamic pivot on any value (without order by)
    #[rustfmt::skip]
    snowflake().verified_only_select(concat!(
        "SELECT * ",
        "FROM sales_data ",
          "PIVOT(SUM(total_sales) FOR fis_quarter IN (ANY)) ",
        "WHERE fis_year IN (2023) ",
        "ORDER BY region",
    ));
}

#[test]
fn asof_joins() {
    #[rustfmt::skip]
    let query = snowflake_and_generic().verified_only_select(concat!(
        "SELECT * ",
          "FROM trades_unixtime AS tu ",
            "ASOF JOIN quotes_unixtime AS qu ",
            "MATCH_CONDITION (tu.trade_time >= qu.quote_time)",
    ));

    assert_eq!(
        query.from[0],
        TableWithJoins {
            relation: table_with_alias("trades_unixtime", "tu"),
            joins: vec![Join {
                relation: table_with_alias("quotes_unixtime", "qu"),
                global: false,
                join_operator: JoinOperator::AsOf {
                    match_condition: Expr::BinaryOp {
                        left: Box::new(Expr::CompoundIdentifier(vec![
                            Ident::new("tu"),
                            Ident::new("trade_time"),
                        ])),
                        op: BinaryOperator::GtEq,
                        right: Box::new(Expr::CompoundIdentifier(vec![
                            Ident::new("qu"),
                            Ident::new("quote_time"),
                        ])),
                    },
                    constraint: JoinConstraint::None,
                },
            }],
        }
    );

    #[rustfmt::skip]
    snowflake_and_generic().verified_query(concat!(
        "SELECT t.stock_symbol, t.trade_time, t.quantity, q.quote_time, q.price ",
        "FROM trades AS t ASOF JOIN quotes AS q ",
          "MATCH_CONDITION (t.trade_time >= quote_time) ",
          "ON t.stock_symbol = q.stock_symbol ",
        "ORDER BY t.stock_symbol",
    ));

    #[rustfmt::skip]
    snowflake_and_generic().verified_query(concat!(
        "SELECT t.stock_symbol, c.company_name, t.trade_time, t.quantity, q.quote_time, q.price ",
          "FROM trades AS t ASOF JOIN quotes AS q ",
            "MATCH_CONDITION (t.trade_time <= quote_time) ",
            "USING(stock_symbol) ",
            "JOIN companies AS c ON c.stock_symbol = t.stock_symbol ",
          "ORDER BY t.stock_symbol",
    ));

    #[rustfmt::skip]
    snowflake_and_generic().verified_query(concat!(
        "SELECT * ",
          "FROM snowtime AS s ",
            "ASOF JOIN raintime AS r ",
              "MATCH_CONDITION (s.observed >= r.observed) ",
              "ON s.state = r.state ",
            "ASOF JOIN preciptime AS p ",
              "MATCH_CONDITION (s.observed >= p.observed) ",
              "ON s.state = p.state ",
          "ORDER BY s.observed",
    ));

    // Test without explicit aliases
    #[rustfmt::skip]
    snowflake_and_generic().verified_query(concat!(
        "SELECT * ",
          "FROM snowtime ",
            "ASOF JOIN raintime ",
              "MATCH_CONDITION (snowtime.observed >= raintime.observed) ",
              "ON snowtime.state = raintime.state ",
            "ASOF JOIN preciptime ",
              "MATCH_CONDITION (showtime.observed >= preciptime.observed) ",
              "ON showtime.state = preciptime.state ",
          "ORDER BY showtime.observed",
    ));
}

#[test]
fn test_parse_position() {
    snowflake().verified_query("SELECT position('an', 'banana', 1)");
    snowflake().verified_query("SELECT n, h, POSITION(n IN h) FROM pos");
}

#[test]
fn explain_describe() {
    snowflake().verified_stmt("DESCRIBE test.table");
    snowflake().verified_stmt("DESCRIBE TABLE test.table");
}

#[test]
fn explain_desc() {
    snowflake().verified_stmt("DESC test.table");
    snowflake().verified_stmt("DESC TABLE test.table");
}

#[test]
fn parse_explain_table() {
    match snowflake().verified_stmt("EXPLAIN TABLE test_identifier") {
        Statement::ExplainTable {
            describe_alias,
            hive_format,
            has_table_keyword,
            table_name,
        } => {
            assert_eq!(describe_alias, DescribeAlias::Explain);
            assert_eq!(hive_format, None);
            assert_eq!(has_table_keyword, true);
            assert_eq!("test_identifier", table_name.to_string());
        }
        _ => panic!("Unexpected Statement, must be ExplainTable"),
    }
}

#[test]
fn parse_use() {
    let valid_object_names = ["mydb", "CATALOG", "DEFAULT"];
    let quote_styles = ['\'', '"', '`'];
    for object_name in &valid_object_names {
        // Test single identifier without quotes
        assert_eq!(
            snowflake().verified_stmt(&format!("USE {}", object_name)),
            Statement::Use(Use::Object(ObjectName::from(vec![Ident::new(
                object_name.to_string()
            )])))
        );
        for &quote in &quote_styles {
            // Test single identifier with different type of quotes
            assert_eq!(
                snowflake().verified_stmt(&format!("USE {}{}{}", quote, object_name, quote)),
                Statement::Use(Use::Object(ObjectName::from(vec![Ident::with_quote(
                    quote,
                    object_name.to_string(),
                )])))
            );
        }
    }

    for &quote in &quote_styles {
        // Test double identifier with different type of quotes
        assert_eq!(
            snowflake().verified_stmt(&format!("USE {0}CATALOG{0}.{0}my_schema{0}", quote)),
            Statement::Use(Use::Object(ObjectName::from(vec![
                Ident::with_quote(quote, "CATALOG"),
                Ident::with_quote(quote, "my_schema")
            ])))
        );
    }
    // Test double identifier without quotes
    assert_eq!(
        snowflake().verified_stmt("USE mydb.my_schema"),
        Statement::Use(Use::Object(ObjectName::from(vec![
            Ident::new("mydb"),
            Ident::new("my_schema")
        ])))
    );

    for &quote in &quote_styles {
        // Test single and double identifier with keyword and different type of quotes
        assert_eq!(
            snowflake().verified_stmt(&format!("USE DATABASE {0}my_database{0}", quote)),
            Statement::Use(Use::Database(ObjectName::from(vec![Ident::with_quote(
                quote,
                "my_database".to_string(),
            )])))
        );
        assert_eq!(
            snowflake().verified_stmt(&format!("USE SCHEMA {0}my_schema{0}", quote)),
            Statement::Use(Use::Schema(ObjectName::from(vec![Ident::with_quote(
                quote,
                "my_schema".to_string(),
            )])))
        );
        assert_eq!(
            snowflake().verified_stmt(&format!("USE SCHEMA {0}CATALOG{0}.{0}my_schema{0}", quote)),
            Statement::Use(Use::Schema(ObjectName::from(vec![
                Ident::with_quote(quote, "CATALOG"),
                Ident::with_quote(quote, "my_schema")
            ])))
        );
        assert_eq!(
            snowflake().verified_stmt(&format!("USE ROLE {0}my_role{0}", quote)),
            Statement::Use(Use::Role(ObjectName::from(vec![Ident::with_quote(
                quote,
                "my_role".to_string(),
            )])))
        );
        assert_eq!(
            snowflake().verified_stmt(&format!("USE WAREHOUSE {0}my_wh{0}", quote)),
            Statement::Use(Use::Warehouse(ObjectName::from(vec![Ident::with_quote(
                quote,
                "my_wh".to_string(),
            )])))
        );
    }

    // Test invalid syntax - missing identifier
    let invalid_cases = ["USE SCHEMA", "USE DATABASE", "USE WAREHOUSE"];
    for sql in &invalid_cases {
        assert_eq!(
            snowflake().parse_sql_statements(sql).unwrap_err(),
            ParserError::ParserError("Expected: identifier, found: EOF".to_string()),
        );
    }

    snowflake().verified_stmt("USE SECONDARY ROLES ALL");
    snowflake().verified_stmt("USE SECONDARY ROLES NONE");
    snowflake().verified_stmt("USE SECONDARY ROLES r1, r2, r3");

    // The following is not documented by Snowflake but still works:
    snowflake().one_statement_parses_to("USE SECONDARY ROLE ALL", "USE SECONDARY ROLES ALL");
    snowflake().one_statement_parses_to("USE SECONDARY ROLE NONE", "USE SECONDARY ROLES NONE");
    snowflake().one_statement_parses_to(
        "USE SECONDARY ROLE r1, r2, r3",
        "USE SECONDARY ROLES r1, r2, r3",
    );
}

#[test]
fn view_comment_option_should_be_after_column_list() {
    for sql in [
        "CREATE OR REPLACE VIEW v (a) COMMENT = 'Comment' AS SELECT a FROM t",
        "CREATE OR REPLACE VIEW v (a COMMENT 'a comment', b, c COMMENT 'c comment') COMMENT = 'Comment' AS SELECT a FROM t",
        "CREATE OR REPLACE VIEW v (a COMMENT 'a comment', b, c COMMENT 'c comment') WITH (foo = bar) COMMENT = 'Comment' AS SELECT a FROM t",
    ] {
        snowflake_and_generic()
            .verified_stmt(sql);
    }
}

#[test]
fn parse_view_column_descriptions() {
    let sql = "CREATE OR REPLACE VIEW v (a COMMENT 'Comment', b) AS SELECT a, b FROM table1";

    match snowflake_and_generic().verified_stmt(sql) {
        Statement::CreateView { name, columns, .. } => {
            assert_eq!(name.to_string(), "v");
            assert_eq!(
                columns,
                vec![
                    ViewColumnDef {
                        name: Ident::new("a"),
                        data_type: None,
                        options: Some(vec![ColumnOption::Comment("Comment".to_string())]),
                    },
                    ViewColumnDef {
                        name: Ident::new("b"),
                        data_type: None,
                        options: None,
                    }
                ]
            );
        }
        _ => unreachable!(),
    };
}

#[test]
fn test_parentheses_overflow() {
    // TODO: increase / improve after we fix the recursion limit
    // for real (see https://github.com/apache/datafusion-sqlparser-rs/issues/984)
    let max_nesting_level: usize = 25;

    // Verify the recursion check is not too wasteful... (num of parentheses - 2 is acceptable)
    let slack = 2;
    let l_parens = "(".repeat(max_nesting_level - slack);
    let r_parens = ")".repeat(max_nesting_level - slack);
    let sql = format!("SELECT * FROM {l_parens}a.b.c{r_parens}");
    let parsed =
        snowflake_with_recursion_limit(max_nesting_level).parse_sql_statements(sql.as_str());
    assert_eq!(parsed.err(), None);

    // Verify the recursion check triggers... (num of parentheses - 1 is acceptable)
    let slack = 1;
    let l_parens = "(".repeat(max_nesting_level - slack);
    let r_parens = ")".repeat(max_nesting_level - slack);
    let sql = format!("SELECT * FROM {l_parens}a.b.c{r_parens}");
    let parsed =
        snowflake_with_recursion_limit(max_nesting_level).parse_sql_statements(sql.as_str());
    assert_eq!(parsed.err(), Some(ParserError::RecursionLimitExceeded));
}

#[test]
fn test_show_databases() {
    snowflake().verified_stmt("SHOW DATABASES");
    snowflake().verified_stmt("SHOW TERSE DATABASES");
    snowflake().verified_stmt("SHOW DATABASES HISTORY");
    snowflake().verified_stmt("SHOW DATABASES LIKE '%abc%'");
    snowflake().verified_stmt("SHOW DATABASES STARTS WITH 'demo_db'");
    snowflake().verified_stmt("SHOW DATABASES LIMIT 12");
    snowflake()
        .verified_stmt("SHOW DATABASES HISTORY LIKE '%aa' STARTS WITH 'demo' LIMIT 20 FROM 'abc'");
    snowflake().verified_stmt("SHOW DATABASES IN ACCOUNT abc");
}

#[test]
fn test_parse_show_schemas() {
    snowflake().verified_stmt("SHOW SCHEMAS");
    snowflake().verified_stmt("SHOW TERSE SCHEMAS");
    snowflake().verified_stmt("SHOW SCHEMAS IN ACCOUNT");
    snowflake().verified_stmt("SHOW SCHEMAS IN ACCOUNT abc");
    snowflake().verified_stmt("SHOW SCHEMAS IN DATABASE");
    snowflake().verified_stmt("SHOW SCHEMAS IN DATABASE xyz");
    snowflake().verified_stmt("SHOW SCHEMAS HISTORY LIKE '%xa%'");
    snowflake().verified_stmt("SHOW SCHEMAS STARTS WITH 'abc' LIMIT 20");
    snowflake().verified_stmt("SHOW SCHEMAS IN DATABASE STARTS WITH 'abc' LIMIT 20 FROM 'xyz'");
}

#[test]
fn test_parse_show_objects() {
    snowflake().verified_stmt("SHOW OBJECTS");
    snowflake().verified_stmt("SHOW OBJECTS IN abc");
    snowflake().verified_stmt("SHOW OBJECTS LIKE '%test%' IN abc");
    snowflake().verified_stmt("SHOW OBJECTS IN ACCOUNT");
    snowflake().verified_stmt("SHOW OBJECTS IN DATABASE");
    snowflake().verified_stmt("SHOW OBJECTS IN DATABASE abc");
    snowflake().verified_stmt("SHOW OBJECTS IN SCHEMA");
    snowflake().verified_stmt("SHOW OBJECTS IN SCHEMA abc");
    snowflake().verified_stmt("SHOW TERSE OBJECTS");
    snowflake().verified_stmt("SHOW TERSE OBJECTS IN abc");
    snowflake().verified_stmt("SHOW TERSE OBJECTS LIKE '%test%' IN abc");
    snowflake().verified_stmt("SHOW TERSE OBJECTS LIKE '%test%' IN abc STARTS WITH 'b'");
    snowflake().verified_stmt("SHOW TERSE OBJECTS LIKE '%test%' IN abc STARTS WITH 'b' LIMIT 10");
    snowflake()
        .verified_stmt("SHOW TERSE OBJECTS LIKE '%test%' IN abc STARTS WITH 'b' LIMIT 10 FROM 'x'");
    match snowflake().verified_stmt("SHOW TERSE OBJECTS LIKE '%test%' IN abc") {
        Statement::ShowObjects(ShowObjects {
            terse,
            show_options,
        }) => {
            assert!(terse);
            let name = match show_options.show_in {
                Some(ShowStatementIn {
                    parent_name: Some(val),
                    ..
                }) => val.to_string(),
                _ => unreachable!(),
            };
            assert_eq!("abc", name);
            let like = match show_options.filter_position {
                Some(ShowStatementFilterPosition::Infix(ShowStatementFilter::Like(val))) => val,
                _ => unreachable!(),
            };
            assert_eq!("%test%", like);
        }
        _ => unreachable!(),
    }
}

#[test]
fn test_parse_show_tables() {
    snowflake().verified_stmt("SHOW TABLES");
    snowflake().verified_stmt("SHOW TERSE TABLES");
    snowflake().verified_stmt("SHOW TABLES IN ACCOUNT");
    snowflake().verified_stmt("SHOW TABLES IN DATABASE");
    snowflake().verified_stmt("SHOW TABLES IN DATABASE xyz");
    snowflake().verified_stmt("SHOW TABLES IN SCHEMA");
    snowflake().verified_stmt("SHOW TABLES IN SCHEMA xyz");
    snowflake().verified_stmt("SHOW TABLES HISTORY LIKE '%xa%'");
    snowflake().verified_stmt("SHOW TABLES STARTS WITH 'abc' LIMIT 20");
    snowflake().verified_stmt("SHOW TABLES IN SCHEMA STARTS WITH 'abc' LIMIT 20 FROM 'xyz'");
    snowflake().verified_stmt("SHOW EXTERNAL TABLES");
    snowflake().verified_stmt("SHOW EXTERNAL TABLES IN ACCOUNT");
    snowflake().verified_stmt("SHOW EXTERNAL TABLES IN DATABASE");
    snowflake().verified_stmt("SHOW EXTERNAL TABLES IN DATABASE xyz");
    snowflake().verified_stmt("SHOW EXTERNAL TABLES IN SCHEMA");
    snowflake().verified_stmt("SHOW EXTERNAL TABLES IN SCHEMA xyz");
    snowflake().verified_stmt("SHOW EXTERNAL TABLES STARTS WITH 'abc' LIMIT 20");
    snowflake()
        .verified_stmt("SHOW EXTERNAL TABLES IN SCHEMA STARTS WITH 'abc' LIMIT 20 FROM 'xyz'");
}

#[test]
fn test_show_views() {
    snowflake().verified_stmt("SHOW VIEWS");
    snowflake().verified_stmt("SHOW TERSE VIEWS");
    snowflake().verified_stmt("SHOW VIEWS IN ACCOUNT");
    snowflake().verified_stmt("SHOW VIEWS IN DATABASE");
    snowflake().verified_stmt("SHOW VIEWS IN DATABASE xyz");
    snowflake().verified_stmt("SHOW VIEWS IN SCHEMA");
    snowflake().verified_stmt("SHOW VIEWS IN SCHEMA xyz");
    snowflake().verified_stmt("SHOW VIEWS STARTS WITH 'abc' LIMIT 20");
    snowflake().verified_stmt("SHOW VIEWS IN SCHEMA STARTS WITH 'abc' LIMIT 20 FROM 'xyz'");
}

#[test]
fn test_parse_show_columns_sql() {
    snowflake().verified_stmt("SHOW COLUMNS IN TABLE");
    snowflake().verified_stmt("SHOW COLUMNS IN TABLE abc");
    snowflake().verified_stmt("SHOW COLUMNS LIKE '%xyz%' IN TABLE abc");
}

#[test]
fn test_projection_with_nested_trailing_commas() {
    let sql = "SELECT a, FROM b, LATERAL FLATTEN(input => events)";
    let _ = snowflake().parse_sql_statements(sql).unwrap();

    //Single nesting
    let sql = "SELECT (SELECT a, FROM b, LATERAL FLATTEN(input => events))";
    let _ = snowflake().parse_sql_statements(sql).unwrap();

    //Double nesting
    let sql = "SELECT (SELECT (SELECT a, FROM b, LATERAL FLATTEN(input => events)))";
    let _ = snowflake().parse_sql_statements(sql).unwrap();

    let sql = "SELECT a, b, FROM c, (SELECT d, e, FROM f, LATERAL FLATTEN(input => events))";
    let _ = snowflake().parse_sql_statements(sql).unwrap();
}

#[test]
fn test_sf_double_dot_notation() {
    snowflake().verified_stmt("SELECT * FROM db_name..table_name");
    snowflake().verified_stmt("SELECT * FROM x, y..z JOIN a..b AS b ON x.id = b.id");

    assert_eq!(
        snowflake()
            .parse_sql_statements("SELECT * FROM X.Y..")
            .unwrap_err()
            .to_string(),
        "sql parser error: Expected: identifier, found: ."
    );
    assert_eq!(
        snowflake()
            .parse_sql_statements("SELECT * FROM X..Y..Z")
            .unwrap_err()
            .to_string(),
        "sql parser error: Expected: identifier, found: ."
    );
    assert_eq!(
        // Ensure we don't parse leading token
        snowflake()
            .parse_sql_statements("SELECT * FROM .X.Y")
            .unwrap_err()
            .to_string(),
        "sql parser error: Expected: identifier, found: ."
    );
}

#[test]
fn test_parse_double_dot_notation_wrong_position() {}

#[test]
fn parse_insert_overwrite() {
    let insert_overwrite_into = r#"INSERT OVERWRITE INTO schema.table SELECT a FROM b"#;
    snowflake().verified_stmt(insert_overwrite_into);
}

#[test]
fn test_table_sample() {
    snowflake_and_generic().verified_stmt("SELECT * FROM testtable SAMPLE (10)");
    snowflake_and_generic().verified_stmt("SELECT * FROM testtable TABLESAMPLE (10)");
    snowflake_and_generic()
        .verified_stmt("SELECT * FROM testtable AS t TABLESAMPLE BERNOULLI (10)");
    snowflake_and_generic().verified_stmt("SELECT * FROM testtable AS t TABLESAMPLE ROW (10)");
    snowflake_and_generic().verified_stmt("SELECT * FROM testtable AS t TABLESAMPLE ROW (10 ROWS)");
    snowflake_and_generic()
        .verified_stmt("SELECT * FROM testtable TABLESAMPLE BLOCK (3) SEED (82)");
    snowflake_and_generic()
        .verified_stmt("SELECT * FROM testtable TABLESAMPLE SYSTEM (3) REPEATABLE (82)");
    snowflake_and_generic().verified_stmt("SELECT id FROM mytable TABLESAMPLE (10) REPEATABLE (1)");
    snowflake_and_generic().verified_stmt("SELECT id FROM mytable TABLESAMPLE (10) SEED (1)");
}

#[test]
fn parse_ls_and_rm() {
    snowflake().one_statement_parses_to("LS @~", "LIST @~");
    snowflake().one_statement_parses_to("RM @~", "REMOVE @~");

    let statement = snowflake()
        .verified_stmt("LIST @SNOWFLAKE_KAFKA_CONNECTOR_externalDataLakeSnowflakeConnector_STAGE_call_tracker_stream/");
    match statement {
        Statement::List(command) => {
            assert_eq!(command.stage, ObjectName::from(vec!["@SNOWFLAKE_KAFKA_CONNECTOR_externalDataLakeSnowflakeConnector_STAGE_call_tracker_stream/".into()]));
            assert!(command.pattern.is_none());
        }
        _ => unreachable!(),
    };

    let statement =
        snowflake().verified_stmt("REMOVE @my_csv_stage/analysis/ PATTERN='.*data_0.*'");
    match statement {
        Statement::Remove(command) => {
            assert_eq!(
                command.stage,
                ObjectName::from(vec!["@my_csv_stage/analysis/".into()])
            );
            assert_eq!(command.pattern, Some(".*data_0.*".to_string()));
        }
        _ => unreachable!(),
    };

    snowflake().verified_stmt(r#"LIST @"STAGE_WITH_QUOTES""#);
    // Semi-colon after stage name - should terminate the stage name
    snowflake()
        .parse_sql_statements("LIST @db1.schema1.stage1/dir1/;")
        .unwrap();
}

#[test]
fn test_sql_keywords_as_select_item_aliases() {
    // Some keywords that should be parsed as an alias
    let unreserved_kws = vec!["CLUSTER", "FETCH", "RETURNING", "LIMIT", "EXCEPT"];
    for kw in unreserved_kws {
        snowflake()
            .one_statement_parses_to(&format!("SELECT 1 {kw}"), &format!("SELECT 1 AS {kw}"));
    }

    // Some keywords that should not be parsed as an alias
    let reserved_kws = vec![
        "FROM",
        "GROUP",
        "HAVING",
        "INTERSECT",
        "INTO",
        "ORDER",
        "SELECT",
        "UNION",
        "WHERE",
        "WITH",
    ];
    for kw in reserved_kws {
        assert!(snowflake()
            .parse_sql_statements(&format!("SELECT 1 {kw}"))
            .is_err());
    }
}

#[test]
fn test_timetravel_at_before() {
    snowflake().verified_only_select("SELECT * FROM tbl AT(TIMESTAMP => '2024-12-15 00:00:00')");
    snowflake()
        .verified_only_select("SELECT * FROM tbl BEFORE(TIMESTAMP => '2024-12-15 00:00:00')");
}

#[test]
fn test_grant_account_privileges() {
    let privileges = vec![
        "ALL",
        "ALL PRIVILEGES",
        "ATTACH POLICY",
        "AUDIT",
        "BIND SERVICE ENDPOINT",
        "IMPORT SHARE",
        "OVERRIDE SHARE RESTRICTIONS",
        "PURCHASE DATA EXCHANGE LISTING",
        "RESOLVE ALL",
        "READ SESSION",
    ];
    let with_grant_options = vec!["", " WITH GRANT OPTION"];

    for p in &privileges {
        for wgo in &with_grant_options {
            let sql = format!("GRANT {p} ON ACCOUNT TO ROLE role1{wgo}");
            snowflake_and_generic().verified_stmt(&sql);
        }
    }

    let create_object_types = vec![
        "ACCOUNT",
        "APPLICATION",
        "APPLICATION PACKAGE",
        "COMPUTE POOL",
        "DATA EXCHANGE LISTING",
        "DATABASE",
        "EXTERNAL VOLUME",
        "FAILOVER GROUP",
        "INTEGRATION",
        "NETWORK POLICY",
        "ORGANIZATION LISTING",
        "REPLICATION GROUP",
        "ROLE",
        "SHARE",
        "USER",
        "WAREHOUSE",
    ];
    for t in &create_object_types {
        for wgo in &with_grant_options {
            let sql = format!("GRANT CREATE {t} ON ACCOUNT TO ROLE role1{wgo}");
            snowflake_and_generic().verified_stmt(&sql);
        }
    }

    let apply_types = vec![
        "AGGREGATION POLICY",
        "AUTHENTICATION POLICY",
        "JOIN POLICY",
        "MASKING POLICY",
        "PACKAGES POLICY",
        "PASSWORD POLICY",
        "PROJECTION POLICY",
        "ROW ACCESS POLICY",
        "SESSION POLICY",
        "TAG",
    ];
    for t in &apply_types {
        for wgo in &with_grant_options {
            let sql = format!("GRANT APPLY {t} ON ACCOUNT TO ROLE role1{wgo}");
            snowflake_and_generic().verified_stmt(&sql);
        }
    }

    let execute_types = vec![
        "ALERT",
        "DATA METRIC FUNCTION",
        "MANAGED ALERT",
        "MANAGED TASK",
        "TASK",
    ];
    for t in &execute_types {
        for wgo in &with_grant_options {
            let sql = format!("GRANT EXECUTE {t} ON ACCOUNT TO ROLE role1{wgo}");
            snowflake_and_generic().verified_stmt(&sql);
        }
    }

    let manage_types = vec![
        "ACCOUNT SUPPORT CASES",
        "EVENT SHARING",
        "GRANTS",
        "LISTING AUTO FULFILLMENT",
        "ORGANIZATION SUPPORT CASES",
        "USER SUPPORT CASES",
        "WAREHOUSES",
    ];
    for t in &manage_types {
        for wgo in &with_grant_options {
            let sql = format!("GRANT MANAGE {t} ON ACCOUNT TO ROLE role1{wgo}");
            snowflake_and_generic().verified_stmt(&sql);
        }
    }

    let monitor_types = vec!["EXECUTION", "SECURITY", "USAGE"];
    for t in &monitor_types {
        for wgo in &with_grant_options {
            let sql = format!("GRANT MONITOR {t} ON ACCOUNT TO ROLE role1{wgo}");
            snowflake_and_generic().verified_stmt(&sql);
        }
    }
}

#[test]
fn test_grant_role_to() {
    snowflake_and_generic().verified_stmt("GRANT ROLE r1 TO ROLE r2");
    snowflake_and_generic().verified_stmt("GRANT ROLE r1 TO USER u1");
}

#[test]
fn test_grant_database_role_to() {
    snowflake_and_generic().verified_stmt("GRANT DATABASE ROLE r1 TO ROLE r2");
    snowflake_and_generic().verified_stmt("GRANT DATABASE ROLE db1.sc1.r1 TO ROLE db1.sc1.r2");
}

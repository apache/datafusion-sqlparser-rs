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
//! Test SQL syntax specific to Apache Doris.

#[macro_use]
mod test_utils;

use sqlparser::ast::*;
use sqlparser::dialect::{AnsiDialect, Dialect, DorisDialect, GenericDialect};
use sqlparser::tokenizer::Token;
use test_utils::*;

fn doris() -> TestedDialects {
    TestedDialects::new(vec![Box::new(DorisDialect {})])
}

fn doris_and_generic() -> TestedDialects {
    TestedDialects::new(vec![Box::new(DorisDialect {}), Box::new(GenericDialect {})])
}

// ============================================================
// Dialect gate verification
// ============================================================

#[test]
fn doris_and_generic_enable_doris_create_table_gates() {
    let dialects = doris_and_generic();
    for dialect in dialects.dialects {
        assert!(dialect.supports_create_table_key_model_clause());
        assert!(dialect.supports_create_table_range_list_partitioning_clause());
        assert!(dialect.supports_create_table_distribution_clause());
        assert!(dialect.supports_create_table_properties_clause());
        assert!(dialect.supports_column_aggregation_function_option());
    }
}

#[test]
fn doris_only_parenthesized_auto_increment_gate() {
    let doris_dialect = DorisDialect {};
    assert!(doris_dialect.supports_parenthesized_auto_increment_column_option());
    let generic_dialect = GenericDialect {};
    assert!(!generic_dialect.supports_parenthesized_auto_increment_column_option());
}

// ============================================================
// Strings and identifiers
// ============================================================

#[test]
fn parse_doris_strings_and_identifiers() {
    doris().verified_stmt(
        r#"SELECT "double quoted string", 'single quoted string', `select` FROM `db`.`table`"#,
    );
}

// ============================================================
// CREATE TABLE - key model
// ============================================================

#[test]
fn tokenize_doris_create_table_keywords() {
    doris().verified_stmt(
        "CREATE TABLE t (k LARGEINT, v BIGINT SUM) AGGREGATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS AUTO PROPERTIES ('replication_num' = '1')",
    );
}

#[test]
fn parse_doris_duplicate_key_hash_distribution() {
    doris_and_generic().verified_stmt(
        "CREATE TABLE t (k BIGINT, v STRING) DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 8",
    );
}

#[test]
fn parse_doris_unique_key_random_distribution() {
    doris_and_generic()
        .verified_stmt("CREATE TABLE t (k BIGINT, v STRING) UNIQUE KEY(k) DISTRIBUTED BY RANDOM");
}

#[test]
fn parse_doris_buckets_auto() {
    doris_and_generic().verified_stmt(
        "CREATE TABLE t (k BIGINT, v STRING) DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS AUTO",
    );
}

#[test]
fn parse_doris_table_properties() {
    doris_and_generic().verified_stmt(
        "CREATE TABLE t (k BIGINT, v STRING) DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 8 PROPERTIES ('replication_num' = '1')",
    );
}

#[test]
fn parse_doris_table_properties_with_double_quoted_values() {
    doris().verified_stmt(
        r#"CREATE TABLE t (k BIGINT, v STRING) DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 8 PROPERTIES ("storage_medium" = "SSD", "replication_num" = "1")"#,
    );
}

#[test]
fn parse_doris_properties_only() {
    doris_and_generic()
        .verified_stmt("CREATE TABLE t (k BIGINT) PROPERTIES ('replication_num' = '1')");
}

#[test]
fn parse_doris_engine_with_properties_only() {
    doris_and_generic().verified_stmt(
        "CREATE TABLE t (k BIGINT) ENGINE = OLAP PROPERTIES ('replication_num' = '1')",
    );
}

#[test]
fn parse_doris_engine_comment_properties_only() {
    doris_and_generic().verified_stmt(
        "CREATE TABLE t (k BIGINT) ENGINE = OLAP COMMENT 'my table' PROPERTIES ('replication_num' = '1')",
    );
}

// ============================================================
// CREATE TABLE - ENGINE clause
// ============================================================

#[test]
fn parse_doris_engine_before_key_model() {
    doris_and_generic().verified_stmt(
        "CREATE TABLE t (k BIGINT) ENGINE = OLAP DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 8",
    );
}

#[test]
fn parse_doris_engine_with_comment_and_properties() {
    doris_and_generic().verified_stmt(
        "CREATE TABLE t (k BIGINT, v STRING) ENGINE = OLAP DUPLICATE KEY(k) COMMENT 'my table' DISTRIBUTED BY HASH(k) BUCKETS 8 PROPERTIES ('replication_num' = '1')",
    );
}

#[test]
fn ast_doris_engine_is_captured() {
    let sql =
        "CREATE TABLE t (k BIGINT) ENGINE = OLAP DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 8";
    let stmt = doris().verified_stmt(sql);
    match stmt {
        Statement::CreateTable(CreateTable {
            table_model:
                Some(TableModel {
                    engine: Some(engine),
                    key_model: Some(km),
                    ..
                }),
            ..
        }) => {
            assert_eq!(engine, Ident::new("OLAP"));
            assert_eq!(km.kind, TableKeyModelKind::Duplicate);
        }
        _ => panic!("Expected CreateTable with engine"),
    }
}

#[test]
fn ast_doris_engine_only_is_captured() {
    let sql = "CREATE TABLE t (k BIGINT) ENGINE = OLAP";
    let stmt = doris().verified_stmt(sql);
    match stmt {
        Statement::CreateTable(CreateTable {
            table_model:
                Some(TableModel {
                    engine: Some(engine),
                    ..
                }),
            table_options,
            ..
        }) => {
            assert_eq!(engine, Ident::new("OLAP"));
            assert_eq!(table_options, CreateTableOptions::None);
        }
        _ => panic!("Expected CreateTable with structured engine"),
    }
}

#[test]
fn ast_generic_engine_only_stays_plain_table_option() {
    let generic = TestedDialects::new(vec![Box::new(GenericDialect {})]);
    let sql = "CREATE TABLE t (k BIGINT) ENGINE = InnoDB";
    let stmt = generic.verified_stmt(sql);
    match stmt {
        Statement::CreateTable(CreateTable {
            table_model,
            table_options,
            ..
        }) => {
            assert!(table_model.is_none());
            match table_options {
                CreateTableOptions::Plain(options) => assert!(options.contains(
                    &SqlOption::NamedParenthesizedList(NamedParenthesizedList {
                        key: Ident::new("ENGINE"),
                        name: Some(Ident::new("InnoDB")),
                        values: vec![],
                    })
                )),
                other => panic!("Expected Plain table options, got {:?}", other),
            }
        }
        _ => panic!("Expected CreateTable"),
    }
}

// ============================================================
// CREATE TABLE - table COMMENT
// ============================================================

#[test]
fn parse_doris_table_comment_after_key_model() {
    doris_and_generic().verified_stmt(
        "CREATE TABLE t (k BIGINT) DUPLICATE KEY(k) COMMENT 'table comment' DISTRIBUTED BY HASH(k) BUCKETS 8",
    );
}

#[test]
fn ast_doris_table_comment_is_captured() {
    let sql =
        "CREATE TABLE t (k BIGINT) DUPLICATE KEY(k) COMMENT 'table comment' DISTRIBUTED BY HASH(k) BUCKETS 8";
    let stmt = doris().verified_stmt(sql);
    match stmt {
        Statement::CreateTable(CreateTable {
            table_model:
                Some(TableModel {
                    comment: Some(comment),
                    ..
                }),
            ..
        }) => {
            assert_eq!(comment, "table comment");
        }
        _ => panic!("Expected CreateTable with table_comment"),
    }
}

#[test]
fn ast_doris_table_comment_only_is_captured() {
    let sql = "CREATE TABLE t (k BIGINT) COMMENT 'table comment'";
    let stmt = doris().verified_stmt(sql);
    match stmt {
        Statement::CreateTable(CreateTable {
            table_model:
                Some(TableModel {
                    comment: Some(comment),
                    ..
                }),
            table_options,
            ..
        }) => {
            assert_eq!(comment, "table comment");
            assert_eq!(table_options, CreateTableOptions::None);
        }
        _ => panic!("Expected CreateTable with structured table_comment"),
    }
}

#[test]
fn parse_doris_table_comment_escapes_single_quote() {
    doris().one_statement_parses_to(
        "CREATE TABLE t (k BIGINT) COMMENT 'it''s ok'",
        "CREATE TABLE t (k BIGINT) COMMENT 'it''s ok'",
    );
}

#[test]
fn ast_doris_table_comment_with_properties_is_captured() {
    let sql =
        "CREATE TABLE t (k BIGINT) COMMENT 'table comment' PROPERTIES ('replication_num' = '1')";
    let stmt = doris().verified_stmt(sql);
    match stmt {
        Statement::CreateTable(CreateTable {
            table_model:
                Some(TableModel {
                    comment: Some(comment),
                    properties,
                    ..
                }),
            ..
        }) => {
            assert_eq!(comment, "table comment");
            assert_eq!(properties.len(), 1);
        }
        _ => panic!("Expected CreateTable with table_comment and properties"),
    }
}

// ============================================================
// CREATE TABLE - key model ORDER BY
// ============================================================

#[test]
fn parse_doris_unique_key_order_by() {
    doris_and_generic().verified_stmt(
        "CREATE TABLE t (k BIGINT, c BIGINT) UNIQUE KEY(k) ORDER BY(c) DISTRIBUTED BY HASH(k) BUCKETS 8",
    );
}

#[test]
fn parse_doris_key_model_order_by_is_parser_level_loose() {
    for key_model in ["UNIQUE", "DUPLICATE", "AGGREGATE"] {
        let sql = format!(
            "CREATE TABLE t (k BIGINT, c BIGINT) {key_model} KEY(k) ORDER BY(c) DISTRIBUTED BY HASH(k) BUCKETS 8"
        );
        let stmt = doris().verified_stmt(&sql);
        match stmt {
            Statement::CreateTable(CreateTable {
                table_model:
                    Some(TableModel {
                        key_model:
                            Some(TableKeyModel {
                                order_by: Some(order_by),
                                ..
                            }),
                        ..
                    }),
                ..
            }) => assert_eq!(order_by, vec![Ident::new("c")]),
            _ => panic!("Expected structured key model ORDER BY for {key_model}"),
        }
    }
}

#[test]
fn doris_cluster_by_after_key_model_is_out_of_scope() {
    let sql =
        "CREATE TABLE t (k BIGINT) DUPLICATE KEY(k) CLUSTER BY(k) DISTRIBUTED BY HASH(k) BUCKETS 8";
    assert!(doris().parse_sql_statements(sql).is_err());
}

#[test]
fn ast_doris_key_model_order_by() {
    let sql =
        "CREATE TABLE t (k BIGINT, c BIGINT) UNIQUE KEY(k) ORDER BY(c) DISTRIBUTED BY HASH(k) BUCKETS 8";
    let stmt = doris().verified_stmt(sql);
    match stmt {
        Statement::CreateTable(CreateTable {
            table_model:
                Some(TableModel {
                    key_model: Some(km),
                    ..
                }),
            ..
        }) => {
            assert_eq!(km.kind, TableKeyModelKind::Unique);
            assert_eq!(km.columns, vec![Ident::new("k")]);
            assert_eq!(km.order_by, Some(vec![Ident::new("c")]));
        }
        _ => panic!("Expected CreateTable with key_model"),
    }
}

// ============================================================
// CREATE TABLE - key model AST shape assertions
// ============================================================

#[test]
fn ast_doris_key_model_is_structured() {
    let sql =
        "CREATE TABLE t (k BIGINT, v STRING) DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 8";
    let stmt = doris().verified_stmt(sql);
    match stmt {
        Statement::CreateTable(CreateTable {
            table_model:
                Some(TableModel {
                    key_model: Some(km),
                    ..
                }),
            ..
        }) => {
            assert_eq!(km.kind, TableKeyModelKind::Duplicate);
            assert_eq!(km.columns, vec![Ident::new("k")]);
        }
        _ => panic!("Expected CreateTable with key_model"),
    }
}

#[test]
fn ast_doris_aggregate_key_model() {
    let sql =
        "CREATE TABLE t (k BIGINT, v BIGINT SUM) AGGREGATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 8";
    let stmt = doris().verified_stmt(sql);
    match stmt {
        Statement::CreateTable(CreateTable {
            table_model:
                Some(TableModel {
                    key_model: Some(km),
                    ..
                }),
            ..
        }) => {
            assert_eq!(km.kind, TableKeyModelKind::Aggregate);
            assert_eq!(km.columns, vec![Ident::new("k")]);
        }
        _ => panic!("Expected CreateTable with key_model"),
    }
}

#[test]
fn ast_doris_unique_key_model() {
    let sql = "CREATE TABLE t (k BIGINT, v STRING) UNIQUE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 8";
    let stmt = doris().verified_stmt(sql);
    match stmt {
        Statement::CreateTable(CreateTable {
            table_model:
                Some(TableModel {
                    key_model: Some(km),
                    ..
                }),
            ..
        }) => {
            assert_eq!(km.kind, TableKeyModelKind::Unique);
            assert_eq!(km.columns, vec![Ident::new("k")]);
        }
        _ => panic!("Expected CreateTable with key_model"),
    }
}

// ============================================================
// CREATE TABLE - distribution AST shape assertions
// ============================================================

#[test]
fn ast_doris_distribution_hash_is_structured() {
    let sql =
        "CREATE TABLE t (k BIGINT, v STRING) DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 8";
    let stmt = doris().verified_stmt(sql);
    match stmt {
        Statement::CreateTable(CreateTable {
            table_model:
                Some(TableModel {
                    distribution: Some(TableDistribution::Hash { columns, buckets }),
                    ..
                }),
            ..
        }) => {
            assert_eq!(columns, vec![Ident::new("k")]);
            assert_eq!(buckets, Some(BucketCount::Count(8)));
        }
        _ => panic!("Expected CreateTable with Hash distribution"),
    }
}

#[test]
fn ast_doris_distribution_random_is_structured() {
    let sql = "CREATE TABLE t (k BIGINT, v STRING) UNIQUE KEY(k) DISTRIBUTED BY RANDOM";
    let stmt = doris().verified_stmt(sql);
    match stmt {
        Statement::CreateTable(CreateTable {
            table_model:
                Some(TableModel {
                    distribution: Some(TableDistribution::Random { buckets }),
                    ..
                }),
            ..
        }) => {
            assert_eq!(buckets, None);
        }
        _ => panic!("Expected CreateTable with Random distribution"),
    }
}

// ============================================================
// CREATE TABLE - partition
// ============================================================

#[test]
fn parse_doris_range_partition() {
    doris_and_generic().verified_stmt(
        "CREATE TABLE t (k BIGINT, dt DATE) DUPLICATE KEY(k) PARTITION BY RANGE(dt) (PARTITION p1 VALUES LESS THAN ('2024-01-01')) DISTRIBUTED BY HASH(k) BUCKETS 8",
    );
}

#[test]
fn parse_doris_list_partition() {
    doris_and_generic().verified_stmt(
        "CREATE TABLE t (k BIGINT, dt DATE) DUPLICATE KEY(k) PARTITION BY LIST(dt) (PARTITION p1 VALUES IN (('2024-01-01'), ('2024-01-02'))) DISTRIBUTED BY HASH(k) BUCKETS 8",
    );
}

#[test]
fn parse_doris_auto_partition_skeleton() {
    doris_and_generic().verified_stmt(
        "CREATE TABLE t (k BIGINT, dt DATE) DUPLICATE KEY(k) AUTO PARTITION BY RANGE(date_trunc(dt, 'day')) DISTRIBUTED BY RANDOM",
    );
}

#[test]
fn parse_doris_partition_values_less_than_maxvalue() {
    doris_and_generic().verified_stmt(
        "CREATE TABLE t (k BIGINT, dt DATE) DUPLICATE KEY(k) PARTITION BY RANGE(dt) (PARTITION p1 VALUES LESS THAN ('2024-01-01'), PARTITION pmax VALUES LESS THAN MAXVALUE) DISTRIBUTED BY HASH(k) BUCKETS 8",
    );
}

#[test]
fn parse_doris_partition_with_properties() {
    doris_and_generic().verified_stmt(
        "CREATE TABLE t (k BIGINT, dt DATE) DUPLICATE KEY(k) PARTITION BY RANGE(dt) (PARTITION p1 VALUES LESS THAN ('2024-01-01') PROPERTIES ('storage_medium' = 'SSD')) DISTRIBUTED BY HASH(k) BUCKETS 8",
    );
}

#[test]
fn parse_doris_list_partition_single_values() {
    doris_and_generic().one_statement_parses_to(
        "CREATE TABLE t (k BIGINT, city STRING) DUPLICATE KEY(k) PARTITION BY LIST(city) (PARTITION p1 VALUES IN ('Beijing', 'Shanghai')) DISTRIBUTED BY HASH(k) BUCKETS 8",
        "CREATE TABLE t (k BIGINT, city STRING) DUPLICATE KEY(k) PARTITION BY LIST(city) (PARTITION p1 VALUES IN (('Beijing'), ('Shanghai'))) DISTRIBUTED BY HASH(k) BUCKETS 8",
    );
}

#[test]
fn parse_doris_multi_column_range_partition() {
    doris_and_generic().verified_stmt(
        "CREATE TABLE t (k1 INT, k2 INT, v INT) DUPLICATE KEY(k1, k2) PARTITION BY RANGE(k1, k2) (PARTITION p1 VALUES LESS THAN ('100', '200')) DISTRIBUTED BY HASH(k1) BUCKETS 8",
    );
}

#[test]
fn parse_doris_multi_column_list_partition() {
    doris_and_generic().verified_stmt(
        "CREATE TABLE t (k1 INT, k2 INT, v INT) DUPLICATE KEY(k1, k2) PARTITION BY LIST(k1, k2) (PARTITION p1 VALUES IN (('1', '2'))) DISTRIBUTED BY HASH(k1) BUCKETS 8",
    );
}

#[test]
fn parse_doris_auto_partition_by_list_multi_column() {
    doris_and_generic().verified_stmt(
        "CREATE TABLE t (k1 INT, k2 INT, v INT) DUPLICATE KEY(k1, k2) AUTO PARTITION BY LIST(k1, k2) DISTRIBUTED BY HASH(k1) BUCKETS 8",
    );
}

#[test]
fn parse_doris_partition_fixed_range() {
    doris_and_generic().verified_stmt(
        "CREATE TABLE t (k BIGINT, dt DATE) DUPLICATE KEY(k) PARTITION BY RANGE(dt) (PARTITION p1 VALUES [('2024-01-01'), ('2024-02-01'))) DISTRIBUTED BY HASH(k) BUCKETS 8",
    );
}

#[test]
fn parse_doris_partition_batch_range() {
    doris_and_generic().verified_stmt(
        "CREATE TABLE t (k BIGINT, dt DATE) DUPLICATE KEY(k) PARTITION BY RANGE(dt) (FROM ('2024-01-01') TO ('2024-02-01') INTERVAL 1 DAY) DISTRIBUTED BY HASH(k) BUCKETS 8",
    );
}

#[test]
fn parse_doris_partition_batch_range_no_unit() {
    doris_and_generic().verified_stmt(
        "CREATE TABLE t (k BIGINT, dt INT) DUPLICATE KEY(k) PARTITION BY RANGE(dt) (FROM ('1') TO ('100') INTERVAL 10) DISTRIBUTED BY HASH(k) BUCKETS 8",
    );
}

// ============================================================
// CREATE TABLE - partition IF NOT EXISTS
// ============================================================

#[test]
fn parse_doris_partition_if_not_exists() {
    doris_and_generic().verified_stmt(
        "CREATE TABLE t (k BIGINT, dt DATE) DUPLICATE KEY(k) PARTITION BY RANGE(dt) (PARTITION IF NOT EXISTS p1 VALUES LESS THAN ('2024-01-01')) DISTRIBUTED BY HASH(k) BUCKETS 8",
    );
}

#[test]
fn ast_doris_partition_if_not_exists() {
    let sql = "CREATE TABLE t (k BIGINT, dt DATE) DUPLICATE KEY(k) PARTITION BY RANGE(dt) (PARTITION IF NOT EXISTS p1 VALUES LESS THAN ('2024-01-01')) DISTRIBUTED BY HASH(k) BUCKETS 8";
    let stmt = doris().verified_stmt(sql);
    match stmt {
        Statement::CreateTable(CreateTable {
            table_model:
                Some(TableModel {
                    partitioning: Some(dp),
                    ..
                }),
            ..
        }) => {
            assert_eq!(dp.partitions.len(), 1);
            match &dp.partitions[0] {
                TablePartitioningEntry::Definition(def) => {
                    assert!(def.if_not_exists);
                    assert_eq!(def.name, Ident::new("p1"));
                }
                _ => panic!("Expected Definition entry"),
            }
        }
        _ => panic!("Expected CreateTable with doris_partition"),
    }
}

// ============================================================
// CREATE TABLE - MAXVALUE / MAX_VALUE normalization
// ============================================================

#[test]
fn parse_doris_max_value_underscore() {
    doris_and_generic().one_statement_parses_to(
        "CREATE TABLE t (k BIGINT, dt DATE) DUPLICATE KEY(k) PARTITION BY RANGE(dt) (PARTITION pmax VALUES LESS THAN MAX_VALUE) DISTRIBUTED BY HASH(k) BUCKETS 8",
        "CREATE TABLE t (k BIGINT, dt DATE) DUPLICATE KEY(k) PARTITION BY RANGE(dt) (PARTITION pmax VALUES LESS THAN MAXVALUE) DISTRIBUTED BY HASH(k) BUCKETS 8",
    );
}

#[test]
fn parse_doris_maxvalue_parenthesized() {
    doris_and_generic().one_statement_parses_to(
        "CREATE TABLE t (k BIGINT, dt DATE) DUPLICATE KEY(k) PARTITION BY RANGE(dt) (PARTITION pmax VALUES LESS THAN (MAXVALUE)) DISTRIBUTED BY HASH(k) BUCKETS 8",
        "CREATE TABLE t (k BIGINT, dt DATE) DUPLICATE KEY(k) PARTITION BY RANGE(dt) (PARTITION pmax VALUES LESS THAN MAXVALUE) DISTRIBUTED BY HASH(k) BUCKETS 8",
    );
}

#[test]
fn parse_doris_max_value_parenthesized_underscore() {
    doris_and_generic().one_statement_parses_to(
        "CREATE TABLE t (k BIGINT, dt DATE) DUPLICATE KEY(k) PARTITION BY RANGE(dt) (PARTITION pmax VALUES LESS THAN (MAX_VALUE)) DISTRIBUTED BY HASH(k) BUCKETS 8",
        "CREATE TABLE t (k BIGINT, dt DATE) DUPLICATE KEY(k) PARTITION BY RANGE(dt) (PARTITION pmax VALUES LESS THAN MAXVALUE) DISTRIBUTED BY HASH(k) BUCKETS 8",
    );
}

// ============================================================
// CREATE TABLE - partition AST shape assertions
// ============================================================

#[test]
fn ast_doris_partition_range_is_structured() {
    let sql = "CREATE TABLE t (k BIGINT, dt DATE) DUPLICATE KEY(k) PARTITION BY RANGE(dt) (PARTITION p1 VALUES LESS THAN ('2024-01-01')) DISTRIBUTED BY HASH(k) BUCKETS 8";
    let stmt = doris().verified_stmt(sql);
    match stmt {
        Statement::CreateTable(CreateTable {
            table_model:
                Some(TableModel {
                    partitioning: Some(dp),
                    ..
                }),
            ..
        }) => {
            assert!(!dp.auto);
            assert_eq!(dp.kind, TablePartitioningKind::Range);
            assert_eq!(dp.columns.len(), 1);
            assert_eq!(dp.partitions.len(), 1);
            match &dp.partitions[0] {
                TablePartitioningEntry::Definition(def) => {
                    assert_eq!(def.name, Ident::new("p1"));
                    match &def.values {
                        TablePartitioningValues::LessThan(values) => {
                            assert_eq!(values.len(), 1);
                        }
                        _ => panic!("Expected LessThan partition values"),
                    }
                }
                _ => panic!("Expected Definition entry"),
            }
        }
        _ => panic!("Expected CreateTable with doris_partition"),
    }
}

#[test]
fn ast_doris_partition_maxvalue_is_structured() {
    let sql = "CREATE TABLE t (k BIGINT, dt DATE) DUPLICATE KEY(k) PARTITION BY RANGE(dt) (PARTITION pmax VALUES LESS THAN MAXVALUE) DISTRIBUTED BY HASH(k) BUCKETS 8";
    let stmt = doris().verified_stmt(sql);
    match stmt {
        Statement::CreateTable(CreateTable {
            table_model:
                Some(TableModel {
                    partitioning: Some(dp),
                    ..
                }),
            ..
        }) => {
            assert_eq!(dp.partitions.len(), 1);
            match &dp.partitions[0] {
                TablePartitioningEntry::Definition(def) => {
                    assert_eq!(def.name, Ident::new("pmax"));
                    assert_eq!(def.values, TablePartitioningValues::LessThanMaxValue);
                }
                _ => panic!("Expected Definition entry"),
            }
        }
        _ => panic!("Expected CreateTable with doris_partition"),
    }
}

#[test]
fn ast_doris_auto_partition_is_structured() {
    let sql = "CREATE TABLE t (k BIGINT, dt DATE) DUPLICATE KEY(k) AUTO PARTITION BY RANGE(date_trunc(dt, 'day')) DISTRIBUTED BY RANDOM";
    let stmt = doris().verified_stmt(sql);
    match stmt {
        Statement::CreateTable(CreateTable {
            table_model:
                Some(TableModel {
                    partitioning: Some(dp),
                    ..
                }),
            ..
        }) => {
            assert!(dp.auto);
            assert_eq!(dp.kind, TablePartitioningKind::Range);
            assert!(dp.partitions.is_empty());
        }
        _ => panic!("Expected CreateTable with auto doris_partition"),
    }
}

#[test]
fn ast_doris_multi_column_partition() {
    let sql = "CREATE TABLE t (k1 INT, k2 INT, v INT) DUPLICATE KEY(k1, k2) PARTITION BY RANGE(k1, k2) (PARTITION p1 VALUES LESS THAN ('100', '200')) DISTRIBUTED BY HASH(k1) BUCKETS 8";
    let stmt = doris().verified_stmt(sql);
    match stmt {
        Statement::CreateTable(CreateTable {
            table_model:
                Some(TableModel {
                    partitioning: Some(dp),
                    ..
                }),
            ..
        }) => {
            assert!(!dp.auto);
            assert_eq!(dp.kind, TablePartitioningKind::Range);
            assert_eq!(dp.columns.len(), 2);
            assert_eq!(dp.partitions.len(), 1);
        }
        _ => panic!("Expected CreateTable with multi-column doris_partition"),
    }
}

#[test]
fn ast_doris_fixed_range_partition() {
    let sql = "CREATE TABLE t (k BIGINT, dt DATE) DUPLICATE KEY(k) PARTITION BY RANGE(dt) (PARTITION p1 VALUES [('2024-01-01'), ('2024-02-01'))) DISTRIBUTED BY HASH(k) BUCKETS 8";
    let stmt = doris().verified_stmt(sql);
    match stmt {
        Statement::CreateTable(CreateTable {
            table_model:
                Some(TableModel {
                    partitioning: Some(dp),
                    ..
                }),
            ..
        }) => {
            assert_eq!(dp.partitions.len(), 1);
            match &dp.partitions[0] {
                TablePartitioningEntry::Definition(def) => {
                    assert_eq!(def.name, Ident::new("p1"));
                    match &def.values {
                        TablePartitioningValues::FixedRange { start, end } => {
                            assert_eq!(start.len(), 1);
                            assert_eq!(end.len(), 1);
                        }
                        _ => panic!("Expected FixedRange partition values"),
                    }
                }
                _ => panic!("Expected Definition entry"),
            }
        }
        _ => panic!("Expected CreateTable with doris_partition"),
    }
}

#[test]
fn ast_doris_batch_range_partition() {
    let sql = "CREATE TABLE t (k BIGINT, dt DATE) DUPLICATE KEY(k) PARTITION BY RANGE(dt) (FROM ('2024-01-01') TO ('2024-02-01') INTERVAL 1 DAY) DISTRIBUTED BY HASH(k) BUCKETS 8";
    let stmt = doris().verified_stmt(sql);
    match stmt {
        Statement::CreateTable(CreateTable {
            table_model:
                Some(TableModel {
                    partitioning: Some(dp),
                    ..
                }),
            ..
        }) => {
            assert_eq!(dp.partitions.len(), 1);
            match &dp.partitions[0] {
                TablePartitioningEntry::BatchRange {
                    from,
                    to,
                    interval_value,
                    interval_unit,
                } => {
                    assert_eq!(from.len(), 1);
                    assert_eq!(to.len(), 1);
                    assert_eq!(*interval_value, 1);
                    assert_eq!(interval_unit.as_ref().unwrap(), &Ident::new("DAY"));
                }
                _ => panic!("Expected BatchRange entry"),
            }
        }
        _ => panic!("Expected CreateTable with doris_partition"),
    }
}

// ============================================================
// CREATE TABLE - column options: AUTO_INCREMENT
// ============================================================

#[test]
fn parse_doris_auto_increment_column() {
    doris().verified_stmt(
        "CREATE TABLE t (id BIGINT AUTO_INCREMENT(100), name STRING) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 8",
    );
}

#[test]
fn parse_doris_auto_increment_no_start_value() {
    doris().verified_stmt(
        "CREATE TABLE t (id BIGINT AUTO_INCREMENT, name STRING) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 8",
    );
}

#[test]
fn parse_generic_auto_increment_uses_unified_ast() {
    let generic = TestedDialects::new(vec![Box::new(GenericDialect {})]);
    let sql = "CREATE TABLE t (id BIGINT AUTO_INCREMENT)";
    let stmt = generic.verified_stmt(sql);
    match stmt {
        Statement::CreateTable(CreateTable { columns, .. }) => {
            assert_eq!(
                columns[0].options[0].option,
                ColumnOption::AutoIncrement(None)
            );
        }
        _ => panic!("Expected CreateTable"),
    }
}

#[test]
fn ast_doris_auto_increment_with_start() {
    let sql = "CREATE TABLE t (id BIGINT AUTO_INCREMENT(100), name STRING) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 8";
    let stmt = doris().verified_stmt(sql);
    match stmt {
        Statement::CreateTable(CreateTable { columns, .. }) => {
            let id_col = &columns[0];
            assert_eq!(id_col.name, Ident::new("id"));
            let auto_inc = id_col
                .options
                .iter()
                .find(|o| matches!(o.option, ColumnOption::AutoIncrement(_)));
            assert!(auto_inc.is_some());
            match &auto_inc.unwrap().option {
                ColumnOption::AutoIncrement(Some(100)) => {}
                other => panic!("Expected AutoIncrement(Some(100)), got {:?}", other),
            }
        }
        _ => panic!("Expected CreateTable"),
    }
}

#[test]
fn ast_doris_auto_increment_without_start() {
    let sql = "CREATE TABLE t (id BIGINT AUTO_INCREMENT, name STRING) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 8";
    let stmt = doris().verified_stmt(sql);
    match stmt {
        Statement::CreateTable(CreateTable { columns, .. }) => {
            let id_col = &columns[0];
            let auto_inc = id_col
                .options
                .iter()
                .find(|o| matches!(o.option, ColumnOption::AutoIncrement(_)));
            assert!(auto_inc.is_some());
            match &auto_inc.unwrap().option {
                ColumnOption::AutoIncrement(None) => {}
                other => panic!("Expected AutoIncrement(None), got {:?}", other),
            }
        }
        _ => panic!("Expected CreateTable"),
    }
}

// ============================================================
// CREATE TABLE - column options: aggregate functions
// ============================================================

#[test]
fn parse_doris_aggregate_column_options() {
    doris_and_generic().verified_stmt(
        "CREATE TABLE t (k BIGINT, v BIGINT SUM, bitmap_col BITMAP BITMAP_UNION) AGGREGATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 8",
    );
}

#[test]
fn parse_doris_all_aggregate_column_options() {
    doris_and_generic().verified_stmt(
        "CREATE TABLE t (k BIGINT, v1 BIGINT SUM, v2 BIGINT MAX, v3 BIGINT MIN, v4 BIGINT REPLACE, v5 HLL HLL_UNION, v6 BITMAP BITMAP_UNION, v7 QUANTILESTATE QUANTILE_UNION) AGGREGATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 8",
    );
}

#[test]
fn ast_doris_aggregate_column_option_is_dialect_specific() {
    let sql =
        "CREATE TABLE t (k BIGINT, v BIGINT SUM) AGGREGATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 8";
    let stmt = doris().verified_stmt(sql);
    match stmt {
        Statement::CreateTable(CreateTable { columns, .. }) => {
            let v_col = &columns[1];
            assert_eq!(v_col.name, Ident::new("v"));
            let agg_opt = v_col
                .options
                .iter()
                .find(|o| matches!(o.option, ColumnOption::DialectSpecific(_)));
            assert!(agg_opt.is_some());
            match &agg_opt.unwrap().option {
                ColumnOption::DialectSpecific(tokens) => {
                    assert_eq!(tokens.len(), 1);
                    assert_eq!(tokens[0], Token::make_keyword("SUM"));
                }
                other => panic!("Expected DialectSpecific, got {:?}", other),
            }
        }
        _ => panic!("Expected CreateTable"),
    }
}

// ============================================================
// CREATE TABLE - PROPERTIES AST shape
// ============================================================

#[test]
fn ast_doris_properties_is_structured() {
    let sql = "CREATE TABLE t (k BIGINT, v STRING) DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 8 PROPERTIES ('replication_num' = '1', 'storage_medium' = 'SSD')";
    let stmt = doris().verified_stmt(sql);
    match stmt {
        Statement::CreateTable(CreateTable {
            table_model: Some(TableModel { properties, .. }),
            ..
        }) => {
            assert_eq!(properties.len(), 2);
        }
        _ => panic!("Expected CreateTable"),
    }
}

// ============================================================
// CREATE TABLE - full official clause order
// ============================================================

#[test]
fn parse_doris_full_clause_order() {
    doris_and_generic().verified_stmt(
        "CREATE TABLE t (k BIGINT, dt DATE, v STRING) ENGINE = OLAP DUPLICATE KEY(k) COMMENT 'full example' PARTITION BY RANGE(dt) (PARTITION p1 VALUES LESS THAN ('2024-01-01')) DISTRIBUTED BY HASH(k) BUCKETS 8 PROPERTIES ('replication_num' = '1')",
    );
}

#[test]
fn parse_doris_engine_without_key_model() {
    doris_and_generic()
        .verified_stmt("CREATE TABLE t (k BIGINT) ENGINE = OLAP DISTRIBUTED BY HASH(k) BUCKETS 8");
}

#[test]
fn doris_keywords_can_still_be_common_identifiers_and_aliases() {
    let generic = TestedDialects::new(vec![Box::new(GenericDialect {})]);
    generic.verified_stmt(
        "SELECT 1 AS properties, 2 AS less, 3 AS than, 4 AS random, 5 AS largeint, 6 AS hll_union, 7 AS bitmap_union",
    );
    generic.verified_stmt(
        "CREATE TABLE properties (less INT, than INT, random INT, largeint INT, hll_union INT, bitmap_union INT)",
    );
    generic.verified_stmt(
        "SELECT \"PROPERTIES\", \"LESS\", \"THAN\", \"RANDOM\", \"LARGEINT\", \"HLL_UNION\", \"BITMAP_UNION\" FROM \"PROPERTIES\"",
    );
}

// ============================================================
// Negative tests: ANSI dialect rejects Doris-specific syntax
// ============================================================

#[test]
fn ansi_rejects_doris_key_model() {
    let ansi = TestedDialects::new(vec![Box::new(AnsiDialect {})]);
    let sql =
        "CREATE TABLE t (k BIGINT, v STRING) DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 8";
    assert!(ansi.parse_sql_statements(sql).is_err());
}

#[test]
fn generic_engine_without_model_marker_remains_plain_options() {
    let generic = TestedDialects::new(vec![Box::new(GenericDialect {})]);
    let sql = "CREATE TABLE t (k BIGINT) ENGINE = InnoDB";
    match generic.verified_stmt(sql) {
        Statement::CreateTable(CreateTable {
            table_model,
            table_options,
            ..
        }) => {
            assert!(table_model.is_none());
            assert!(matches!(table_options, CreateTableOptions::Plain(_)));
        }
        _ => panic!("Expected CreateTable"),
    }
}

#[test]
fn doris_engine_without_model_marker_creates_table_model() {
    let sql = "CREATE TABLE t (k BIGINT) ENGINE = OLAP";
    match doris().verified_stmt(sql) {
        Statement::CreateTable(CreateTable {
            table_model:
                Some(TableModel {
                    engine: Some(engine),
                    ..
                }),
            table_options,
            ..
        }) => {
            assert_eq!(engine, Ident::new("OLAP"));
            assert_eq!(table_options, CreateTableOptions::None);
        }
        _ => panic!("Expected CreateTable with table_model engine"),
    }
}

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

//! Test SQL syntax specific to Hive. The parser based on the generic dialect
//! is also tested (on the inputs it can handle).

use sqlparser::ast::{
    CreateFunctionBody, CreateFunctionUsing, Expr, Function, FunctionArgumentList,
    FunctionArguments, FunctionDefinition, Ident, ObjectName, OneOrManyWithParens, SelectItem,
    Statement, TableFactor, UnaryOperator,
};
use sqlparser::dialect::{GenericDialect, HiveDialect, MsSqlDialect};
use sqlparser::parser::{ParserError, ParserOptions};
use sqlparser::test_utils::*;

#[test]
fn parse_table_create() {
    let sql = r#"CREATE TABLE IF NOT EXISTS db.table (a BIGINT, b STRING, c TIMESTAMP) PARTITIONED BY (d STRING, e TIMESTAMP) STORED AS ORC LOCATION 's3://...' TBLPROPERTIES ("prop" = "2", "asdf" = '1234', 'asdf' = "1234", "asdf" = 2)"#;
    let iof = r#"CREATE TABLE IF NOT EXISTS db.table (a BIGINT, b STRING, c TIMESTAMP) PARTITIONED BY (d STRING, e TIMESTAMP) STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat' LOCATION 's3://...'"#;
    let serdeproperties = r#"CREATE EXTERNAL TABLE IF NOT EXISTS db.table (a STRING, b STRING, c STRING) PARTITIONED BY (d STRING, e STRING) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde.config' WITH SERDEPROPERTIES ('prop_a' = 'a', 'prop_b' = 'b') STORED AS TEXTFILE LOCATION 's3://...' TBLPROPERTIES ('prop_c' = 'c')"#;

    hive().verified_stmt(sql);
    hive().verified_stmt(iof);
    hive().verified_stmt(serdeproperties);
}

fn generic(options: Option<ParserOptions>) -> TestedDialects {
    TestedDialects {
        dialects: vec![Box::new(GenericDialect {})],
        options,
    }
}

#[test]
fn parse_describe() {
    let describe = r#"DESCRIBE namespace.`table`"#;
    hive().verified_stmt(describe);
    generic(None).verified_stmt(describe);
}

#[test]
fn explain_describe_formatted() {
    hive().verified_stmt("DESCRIBE FORMATTED test.table");
}

#[test]
fn explain_describe_extended() {
    hive().verified_stmt("DESCRIBE EXTENDED test.table");
}

#[test]
fn parse_insert_overwrite() {
    let insert_partitions = r#"INSERT OVERWRITE TABLE db.new_table PARTITION (a = '1', b) SELECT a, b, c FROM db.table"#;
    hive().verified_stmt(insert_partitions);
}

#[test]
fn test_truncate() {
    let truncate = r#"TRUNCATE TABLE db.table"#;
    hive().verified_stmt(truncate);
}

#[test]
fn parse_analyze() {
    let analyze = r#"ANALYZE TABLE db.table_name PARTITION (a = '1234', b) COMPUTE STATISTICS NOSCAN CACHE METADATA"#;
    hive().verified_stmt(analyze);
}

#[test]
fn parse_analyze_for_columns() {
    let analyze =
        r#"ANALYZE TABLE db.table_name PARTITION (a = '1234', b) COMPUTE STATISTICS FOR COLUMNS"#;
    hive().verified_stmt(analyze);
}

#[test]
fn parse_msck() {
    let msck = r#"MSCK REPAIR TABLE db.table_name ADD PARTITIONS"#;
    let msck2 = r#"MSCK REPAIR TABLE db.table_name"#;
    hive().verified_stmt(msck);
    hive().verified_stmt(msck2);
}

#[test]
fn parse_set() {
    let set = "SET HIVEVAR:name = a, b, c_d";
    hive().verified_stmt(set);
}

#[test]
fn test_spaceship() {
    let spaceship = "SELECT * FROM db.table WHERE a <=> b";
    hive().verified_stmt(spaceship);
}

#[test]
fn parse_with_cte() {
    let with = "WITH a AS (SELECT * FROM b) INSERT INTO TABLE db.table_table PARTITION (a) SELECT * FROM b";
    hive().verified_stmt(with);
}

#[test]
fn drop_table_purge() {
    let purge = "DROP TABLE db.table_name PURGE";
    hive().verified_stmt(purge);
}

#[test]
fn create_table_like() {
    let like = "CREATE TABLE db.table_name LIKE db.other_table";
    hive().verified_stmt(like);
}

// Turning off this test until we can parse identifiers starting with numbers :(
#[test]
fn test_identifier() {
    let between = "SELECT a AS 3_barrr_asdf FROM db.table_name";
    hive().verified_stmt(between);
}

#[test]
fn test_alter_partition() {
    let alter = "ALTER TABLE db.table PARTITION (a = 2) RENAME TO PARTITION (a = 1)";
    hive().verified_stmt(alter);
}

#[test]
fn test_alter_with_location() {
    let alter =
        "ALTER TABLE db.table PARTITION (a = 2) RENAME TO PARTITION (a = 1) LOCATION 's3://...'";
    hive().verified_stmt(alter);
}

#[test]
fn test_alter_with_set_location() {
    let alter = "ALTER TABLE db.table PARTITION (a = 2) RENAME TO PARTITION (a = 1) SET LOCATION 's3://...'";
    hive().verified_stmt(alter);
}

#[test]
fn test_add_partition() {
    let add = "ALTER TABLE db.table ADD IF NOT EXISTS PARTITION (a = 'asdf', b = 2)";
    hive().verified_stmt(add);
}

#[test]
fn test_add_multiple_partitions() {
    let add = "ALTER TABLE db.table ADD IF NOT EXISTS PARTITION (`a` = 'asdf', `b` = 2) PARTITION (`a` = 'asdh', `b` = 3)";
    hive().verified_stmt(add);
}

#[test]
fn test_drop_partition() {
    let drop = "ALTER TABLE db.table DROP PARTITION (a = 1)";
    hive().verified_stmt(drop);
}

#[test]
fn test_drop_if_exists() {
    let drop = "ALTER TABLE db.table DROP IF EXISTS PARTITION (a = 'b', c = 'd')";
    hive().verified_stmt(drop);
}

#[test]
fn test_cluster_by() {
    let cluster = "SELECT a FROM db.table CLUSTER BY a, b";
    hive().verified_stmt(cluster);
}

#[test]
fn test_distribute_by() {
    let cluster = "SELECT a FROM db.table DISTRIBUTE BY a, b";
    hive().verified_stmt(cluster);
}

#[test]
fn no_join_condition() {
    let join = "SELECT a, b FROM db.table_name JOIN a";
    hive().verified_stmt(join);
}

#[test]
fn columns_after_partition() {
    let query = "INSERT INTO db.table_name PARTITION (a, b) (c, d) SELECT a, b, c, d FROM db.table";
    hive().verified_stmt(query);
}

#[test]
fn long_numerics() {
    let query = r#"SELECT MIN(MIN(10, 5), 1L) AS a"#;
    hive().verified_stmt(query);
}

#[test]
fn decimal_precision() {
    let query = "SELECT CAST(a AS DECIMAL(18,2)) FROM db.table";
    hive().verified_stmt(query);
}

#[test]
fn create_temp_table() {
    let query = "CREATE TEMPORARY TABLE db.table (a INT NOT NULL)";
    let query2 = "CREATE TEMP TABLE db.table (a INT NOT NULL)";

    hive().verified_stmt(query);
    hive().one_statement_parses_to(query2, query);
}

#[test]
fn create_delimited_table() {
    let query = "CREATE TABLE tab (cola STRING, colb BIGINT) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' ESCAPED BY '\"' MAP KEYS TERMINATED BY '\"'";
    hive().verified_stmt(query);
}

#[test]
fn create_local_directory() {
    let query =
        "INSERT OVERWRITE LOCAL DIRECTORY '/home/blah' STORED AS TEXTFILE SELECT * FROM db.table";
    hive().verified_stmt(query);
}

#[test]
fn lateral_view() {
    let view = "SELECT a FROM db.table LATERAL VIEW explode(a) t AS j, P LATERAL VIEW OUTER explode(a) t AS a, b WHERE a = 1";
    hive().verified_stmt(view);
}

#[test]
fn sort_by() {
    let sort_by = "SELECT * FROM db.table SORT BY a";
    hive().verified_stmt(sort_by);
}

#[test]
fn rename_table() {
    let rename = "ALTER TABLE db.table_name RENAME TO db.table_2";
    hive().verified_stmt(rename);
}

#[test]
fn map_access() {
    let rename = r#"SELECT a.b["asdf"] FROM db.table WHERE a = 2"#;
    hive().verified_stmt(rename);
}

#[test]
fn from_cte() {
    let rename =
        "WITH cte AS (SELECT * FROM a.b) FROM cte INSERT INTO TABLE a.b PARTITION (a) SELECT *";
    println!("{}", hive().verified_stmt(rename));
}

#[test]
fn set_statement_with_minus() {
    assert_eq!(
        hive().verified_stmt("SET hive.tez.java.opts = -Xmx4g"),
        Statement::SetVariable {
            local: false,
            hivevar: false,
            variables: OneOrManyWithParens::One(ObjectName(vec![
                Ident::new("hive"),
                Ident::new("tez"),
                Ident::new("java"),
                Ident::new("opts")
            ])),
            value: vec![Expr::UnaryOp {
                op: UnaryOperator::Minus,
                expr: Box::new(Expr::Identifier(Ident::new("Xmx4g")))
            }],
        }
    );

    assert_eq!(
        hive().parse_sql_statements("SET hive.tez.java.opts = -"),
        Err(ParserError::ParserError(
            "Expected variable value, found: EOF".to_string()
        ))
    )
}

#[test]
fn parse_create_function() {
    let sql = "CREATE TEMPORARY FUNCTION mydb.myfunc AS 'org.random.class.Name' USING JAR 'hdfs://somewhere.com:8020/very/far'";
    match hive().verified_stmt(sql) {
        Statement::CreateFunction {
            temporary,
            name,
            params,
            ..
        } => {
            assert!(temporary);
            assert_eq!(name.to_string(), "mydb.myfunc");
            assert_eq!(
                params,
                CreateFunctionBody {
                    as_: Some(FunctionDefinition::SingleQuotedDef(
                        "org.random.class.Name".to_string()
                    )),
                    using: Some(CreateFunctionUsing::Jar(
                        "hdfs://somewhere.com:8020/very/far".to_string()
                    )),
                    ..Default::default()
                }
            )
        }
        _ => unreachable!(),
    }

    // Test error in dialect that doesn't support parsing CREATE FUNCTION
    let unsupported_dialects = TestedDialects {
        dialects: vec![Box::new(MsSqlDialect {})],
        options: None,
    };

    assert_eq!(
        unsupported_dialects.parse_sql_statements(sql).unwrap_err(),
        ParserError::ParserError(
            "Expected an object type after CREATE, found: FUNCTION".to_string()
        )
    );

    let sql = "CREATE TEMPORARY FUNCTION mydb.myfunc AS 'org.random.class.Name' USING JAR";
    assert_eq!(
        hive().parse_sql_statements(sql).unwrap_err(),
        ParserError::ParserError("Expected literal string, found: EOF".to_string()),
    );
}

#[test]
fn filter_as_alias() {
    let sql = "SELECT name filter FROM region";
    let expected = "SELECT name AS filter FROM region";
    println!("{}", hive().one_statement_parses_to(sql, expected));
}

#[test]
fn parse_delimited_identifiers() {
    // check that quoted identifiers in any position remain quoted after serialization
    let select = hive().verified_only_select(
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
            partitions: _,
        } => {
            assert_eq!(vec![Ident::with_quote('"', "a table")], name.0);
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
            name: ObjectName(vec![Ident::with_quote('"', "myfun")]),
            args: FunctionArguments::List(FunctionArgumentList {
                duplicate_treatment: None,
                args: vec![],
                clauses: vec![],
            }),
            null_treatment: None,
            filter: None,
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
        _ => panic!("Expected ExprWithAlias"),
    }

    hive().verified_stmt(r#"CREATE TABLE "foo" ("bar" "int")"#);
    hive().verified_stmt(r#"ALTER TABLE foo ADD CONSTRAINT "bar" PRIMARY KEY (baz)"#);
    //TODO verified_stmt(r#"UPDATE foo SET "bar" = 5"#);
}

fn hive() -> TestedDialects {
    TestedDialects {
        dialects: vec![Box::new(HiveDialect {})],
        options: None,
    }
}

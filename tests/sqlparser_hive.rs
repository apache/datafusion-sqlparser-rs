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

use sqlparser::dialect::HiveDialect;
use sqlparser::test_utils::*;

#[test]
fn parse_table_create() {
    let sql = r#"CREATE TABLE IF NOT EXISTS db.table (a BIGINT, b STRING, c TIMESTAMP) PARTITIONED BY (d STRING, e TIMESTAMP) STORED AS ORC LOCATION 's3://...' TBLPROPERTIES ("prop" = "2")"#;
    let iof = r#"CREATE TABLE IF NOT EXISTS db.table (a BIGINT, b STRING, c TIMESTAMP) PARTITIONED BY (d STRING, e TIMESTAMP) STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat' LOCATION 's3://...'"#;

    hive().verified_stmt(sql);
    hive().verified_stmt(iof);
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
    let analyze = r#"ANALYZE TABLE db.table_name PARTITION (a = '1234', b) COMPUTE STATISTICS FOR COLUMNS a, b, c"#;
    hive().verified_stmt(analyze);
}

#[test]
fn parse_msck() {
    let msck = r#"MSCK REPAIR TABLE db.table_name ADD PARTITIONS"#;
    hive().verified_stmt(msck);
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
    let with = "WITH a AS (SELECT * FROM table) INSERT INTO TABLE db.table_table PARTITION (a) SELECT * FROM a";
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
fn test_add_partition() {
    let add = "ALTER TABLE db.table ADD IF NOT EXISTS PARTITION (a = 'asdf', b = 2)";
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
    let expected = "SELECT CAST(a AS NUMERIC(18,2)) FROM db.table";
    hive().one_statement_parses_to(query, expected);
}

#[test]
fn create_temp_table() {
    let query = "CREATE TEMPORARY TABLE db.table (a INT NOT NULL)";
    let query2 = "CREATE TEMP TABLE db.table (a INT NOT NULL)";

    hive().verified_stmt(query);
    hive().one_statement_parses_to(query2, query);
}

#[test]
fn create_local_directory() {
    let query =
        "INSERT OVERWRITE LOCAL DIRECTORY '/home/blah' STORED AS TEXTFILE SELECT * FROM db.table";
    hive().verified_stmt(query);
}

#[test]
fn lateral_view() {
    let view = "SELECT a FROM db.table LATERAL VIEW explode(a) t LATERAL VIEW explode(a) t AS a, b WHERE a = 1";
    hive().verified_stmt(view);
}

#[test]
fn test_array_elements() {
    let elements = "SELECT collect_list(a)[0] FROM db.table";
    hive().verified_stmt(elements);
}

fn hive() -> TestedDialects {
    TestedDialects {
        dialects: vec![Box::new(HiveDialect {})],
    }
}

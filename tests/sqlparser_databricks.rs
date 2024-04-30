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
//! Test SQL syntax specific to Snowflake. The parser based on the
//! generic dialect is also tested (on the inputs it can handle).

#[cfg(test)]
use pretty_assertions::assert_eq;
use sqlparser::ast::*;
use sqlparser::dialect::{DatabricksDialect, GenericDialect};
use sqlparser::parser::ParserOptions;
use test_utils::*;

#[macro_use]
mod test_utils;

fn databricks() -> TestedDialects {
    TestedDialects {
        dialects: vec![Box::new(DatabricksDialect {})],
        options: None,
    }
}

fn databricks_and_generic() -> TestedDialects {
    TestedDialects {
        dialects: vec![Box::new(DatabricksDialect {}), Box::new(GenericDialect {})],
        options: None,
    }
}

fn databricks_unescaped() -> TestedDialects {
    TestedDialects {
        dialects: vec![Box::new(DatabricksDialect {})],
        options: Some(ParserOptions::new().with_unescape(false)),
    }
}

#[test]
fn test_databricks_create_table() {
    let sql = "CREATE TABLE main.dbt_lukasz.customers (customer_id BIGINT, customer_lifetime_value DOUBLE) USING delta TBLPROPERTIES ('delta.minReaderVersion' = '3', 'delta.minWriterVersion' = '7')";
    match databricks_and_generic().verified_stmt(sql) {
        Statement::CreateTable { name, .. } => {
            assert_eq!("main.dbt_lukasz.customers", name.to_string());
        }
        _ => unreachable!(),
    }
}

#[test]
fn test_identifiers() {
    let sql = "SELECT * FROM `main`.`dbt_lukasz`.`raw_orders`";
    databricks().verified_stmt(sql);
}

#[test]
fn test_string_escape() {
    databricks().one_statement_parses_to(r#"SELECT 'O\'Connell'"#, r#"SELECT 'O''Connell'"#);
}

#[test]
fn test_string_raw_literal() {
    let sql = r#"SELECT R'Some\nText'"#;
    databricks_unescaped().verified_stmt(sql);
}

#[test]
fn test_rlike() {
    let sql = r#"SELECT R'%SystemDrive%\Users\John' RLIKE R'%SystemDrive%\\Users.*'"#;
    databricks_unescaped().verified_stmt(sql);
}

#[test]
fn test_create_table_comment_tblproperties() {
    let sql = "CREATE TABLE main.dbt_cloud_lukasz.customers (customer_id BIGINT COMMENT 'Customer Unique identifier', first_name STRING, last_name STRING) USING delta TBLPROPERTIES ('delta.checkpoint.writeStatsAsJson' = 'false') COMMENT 'The ''customers'' table.'";

    databricks_unescaped().verified_stmt(sql);
}

#[test]
fn test_select_star_except() {
    let sql = "SELECT * EXCEPT (c2) FROM tbl";

    databricks().verified_stmt(sql);
}

#[test]
fn test_create_table_partitioned_by_as() {
    let sql = "CREATE TABLE logs PARTITIONED BY (datepart) AS SELECT 1";

    databricks().verified_stmt(sql);
}

#[test]
fn test_create_table_map_type() {
    let sql = "CREATE TABLE logs (info MAP<STRING, ARRAY<STRING>>)";

    databricks().verified_stmt(sql);
}

#[test]
fn test_select_placeholder() {
    let sql = "SELECT {{param}}";

    databricks().verified_stmt(sql);
}

#[test]
fn test_underscore_column_name() {
    databricks().verified_stmt("SELECT _column FROM `myproject`.`mydataset`.`mytable`");

    databricks().verified_stmt("SELECT other AS _column FROM `myproject`.`mydataset`.`mytable`");
}

#[test]
fn test_create_table_column_mask() {
    databricks().verified_stmt("CREATE TABLE persons (name STRING, ssn STRING MASK mask_ssn)");
}

#[test]
fn test_cte_columns() {
    databricks()
        .verified_stmt("WITH t (x, y) AS (SELECT 1, 2) SELECT * FROM t WHERE x = 1 AND y = 2");
}

#[test]
fn test_cte_no_as() {
    databricks().one_statement_parses_to(
        "WITH foo (SELECT 'bar' as baz) SELECT * FROM foo",
        "WITH foo AS (SELECT 'bar' AS baz) SELECT * FROM foo",
    );
    databricks().one_statement_parses_to(
        "WITH foo (WITH b (SELECT * FROM bb) SELECT 'bar' as baz FROM b) SELECT * FROM foo",
        "WITH foo AS (WITH b AS (SELECT * FROM bb) SELECT 'bar' AS baz FROM b) SELECT * FROM foo",
    );
}

#[test]
fn test_create_or_replace_temporary_function_returns_expression() {
    databricks().verified_stmt(
        "CREATE OR REPLACE TEMPORARY FUNCTION GG_Account_ID RETURN '0F98682E-005D-43A9-A5EC-464E8AC478C9'",
    );
    databricks()
        .verified_stmt("CREATE FUNCTION area(x DOUBLE, y DOUBLE) RETURNS DOUBLE RETURN x * y");
    databricks().verified_stmt("CREATE FUNCTION square(x DOUBLE) RETURNS DOUBLE RETURN area(x, x)");
}

#[test]
fn test_create_or_replace_temporary_function_returns_select() {
    databricks().verified_stmt(
        "CREATE FUNCTION avg_score(p INT) RETURNS FLOAT COMMENT 'get an average score of the player' RETURN SELECT AVG(score) FROM scores WHERE player = p",
    );
}

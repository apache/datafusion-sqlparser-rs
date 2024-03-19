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

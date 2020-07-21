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

use sqlparser::ast::*;
use sqlparser::dialect::{GenericDialect, HiveDialect};
use sqlparser::test_utils::*;
use sqlparser::parser::ParserError;

#[test]
fn parse_table_create() -> Result<(), ParserError> {
    let sql = r#"CREATE TABLE IF NOT EXISTS db.table (a BIGINT, b STRING, c TIMESTAMP) PARTITIONED BY (d STRING, e TIMESTAMP) STORED AS ORC LOCATION 's3://...'"#;
    let iof = r#"CREATE TABLE IF NOT EXISTS db.table (a BIGINT, b STRING, c TIMESTAMP) PARTITIONED BY (d STRING, e TIMESTAMP) STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat' LOCATION 's3://...'"#;

    hive().verified_stmt(sql);
    hive().verified_stmt(iof);

    Ok(())
}

fn hive() -> TestedDialects {
    TestedDialects {
        dialects: vec![Box::new(HiveDialect {})],
    }
}

fn hive_and_generic() -> TestedDialects {
    TestedDialects {
        dialects: vec![Box::new(HiveDialect {}), Box::new(GenericDialect {})],
    }
}

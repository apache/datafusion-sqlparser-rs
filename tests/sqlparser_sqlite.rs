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
//! Test SQL syntax specific to SQLite. The parser based on the
//! generic dialect is also tested (on the inputs it can handle).

use sqlparser::ast::*;
use sqlparser::dialect::GenericDialect;
use sqlparser::test_utils::*;

#[test]
fn parse_create_table_without_rowid() {
    let sql = "CREATE TABLE t (a INT) WITHOUT ROWID";
    match sqlite_and_generic().verified_stmt(sql) {
        Statement::CreateTable {
            name,
            without_rowid: true,
            ..
        } => {
            assert_eq!("t", name.to_string());
        }
        _ => unreachable!(),
    }
}

fn sqlite_and_generic() -> TestedDialects {
    TestedDialects {
        // we don't have a separate SQLite dialect, so test only the generic dialect for now
        dialects: vec![Box::new(GenericDialect {})],
    }
}

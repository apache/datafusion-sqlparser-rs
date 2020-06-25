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
//! Test SQL syntax specific to PostgreSQL. The parser based on the
//! generic dialect is also tested (on the inputs it can handle).

use sqlparser::ast::*;
use sqlparser::dialect::GenericDialect;
use sqlparser::test_utils::*;

#[test]
fn parse_create_table() {
    let sql = "CREATE TABLE groups (
                       group_id INTEGER NOT NULL,
                       name TEXT NOT NULL
                    ) WITHOUT ROWID";
    match sqlite().one_statement_parses_to(sql, "") {
        Statement::CreateTable {
            name,
            columns,
            constraints,
            with_options: _with_options,
            if_not_exists: false,
            external: false,
            file_format: None,
            location: None,
            query: _query,
            without_rowid: true,
        } => {
            assert_eq!("groups", name.to_string());
            assert_eq!(
                columns,
                vec![
                    ColumnDef {
                        name: "group_id".into(),
                        data_type: DataType::Int,
                        collation: None,
                        options: vec![ColumnOptionDef {
                            name: None,
                            option: ColumnOption::NotNull,
                        }],
                    },
                    ColumnDef {
                        name: "name".into(),
                        data_type: DataType::Text,
                        collation: None,
                        options: vec![ColumnOptionDef {
                            name: None,
                            option: ColumnOption::NotNull,
                        }],
                    },
                ]
            );
            assert!(constraints.is_empty());
        }
        _ => unreachable!(),
    }
}

fn sqlite() -> TestedDialects {
    TestedDialects {
        dialects: vec![Box::new(GenericDialect {})],
    }
}

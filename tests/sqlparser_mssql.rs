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
//! Test SQL syntax specific to Microsoft's T-SQL. The parser based on the
//! generic dialect is also tested (on the inputs it can handle).

use sqlparser::dialect::{GenericSqlDialect, MsSqlDialect};
use sqlparser::sqlast::*;
use sqlparser::test_utils::*;

#[test]
fn parse_mssql_identifiers() {
    let sql = "SELECT @@version, _foo$123 FROM ##temp";
    let select = ms_and_generic().verified_only_select(sql);
    assert_eq!(
        &Expr::SQLIdentifier("@@version".to_string()),
        expr_from_projection(&select.projection[0]),
    );
    assert_eq!(
        &Expr::SQLIdentifier("_foo$123".to_string()),
        expr_from_projection(&select.projection[1]),
    );
    assert_eq!(2, select.projection.len());
    match &only(&select.from).relation {
        TableFactor::Table { name, .. } => {
            assert_eq!("##temp".to_string(), name.to_string());
        }
        _ => unreachable!(),
    };
}

#[test]
fn parse_mssql_single_quoted_aliases() {
    let _ = ms_and_generic().one_statement_parses_to("SELECT foo 'alias'", "SELECT foo AS 'alias'");
}

#[test]
fn parse_mssql_delimited_identifiers() {
    let _ = ms().one_statement_parses_to(
        "SELECT [a.b!] [FROM] FROM foo [WHERE]",
        "SELECT [a.b!] AS [FROM] FROM foo AS [WHERE]",
    );
}

fn ms() -> TestedDialects {
    TestedDialects {
        dialects: vec![Box::new(MsSqlDialect {})],
    }
}
fn ms_and_generic() -> TestedDialects {
    TestedDialects {
        dialects: vec![Box::new(MsSqlDialect {}), Box::new(GenericSqlDialect {})],
    }
}

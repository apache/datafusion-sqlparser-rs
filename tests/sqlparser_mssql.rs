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

#[macro_use]
mod test_utils;
use test_utils::*;

use sqlparser::ast::*;
use sqlparser::dialect::{GenericDialect, MsSqlDialect};

#[test]
fn parse_mssql_identifiers() {
    let sql = "SELECT @@version, _foo$123 FROM ##temp";
    let select = ms_and_generic().verified_only_select(sql);
    assert_eq!(
        &Expr::Identifier(Ident::new("@@version")),
        expr_from_projection(&select.projection[0]),
    );
    assert_eq!(
        &Expr::Identifier(Ident::new("_foo$123")),
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

#[test]
fn parse_mssql_apply_join() {
    let _ = ms_and_generic().verified_only_select(
        "SELECT * FROM sys.dm_exec_query_stats AS deqs \
         CROSS APPLY sys.dm_exec_query_plan(deqs.plan_handle)",
    );
    let _ = ms_and_generic().verified_only_select(
        "SELECT * FROM sys.dm_exec_query_stats AS deqs \
         OUTER APPLY sys.dm_exec_query_plan(deqs.plan_handle)",
    );
    let _ = ms_and_generic().verified_only_select(
        "SELECT * FROM foo \
         OUTER APPLY (SELECT foo.x + 1) AS bar",
    );
}

#[test]
fn parse_mssql_top_paren() {
    let sql = "SELECT TOP (5) * FROM foo";
    let select = ms_and_generic().verified_only_select(sql);
    let top = select.top.unwrap();
    assert_eq!(Some(Expr::Value(number("5"))), top.quantity);
    assert!(!top.percent);
}

#[test]
fn parse_mssql_top_percent() {
    let sql = "SELECT TOP (5) PERCENT * FROM foo";
    let select = ms_and_generic().verified_only_select(sql);
    let top = select.top.unwrap();
    assert_eq!(Some(Expr::Value(number("5"))), top.quantity);
    assert!(top.percent);
}

#[test]
fn parse_mssql_top_with_ties() {
    let sql = "SELECT TOP (5) WITH TIES * FROM foo";
    let select = ms_and_generic().verified_only_select(sql);
    let top = select.top.unwrap();
    assert_eq!(Some(Expr::Value(number("5"))), top.quantity);
    assert!(top.with_ties);
}

#[test]
fn parse_mssql_top_percent_with_ties() {
    let sql = "SELECT TOP (10) PERCENT WITH TIES * FROM foo";
    let select = ms_and_generic().verified_only_select(sql);
    let top = select.top.unwrap();
    assert_eq!(Some(Expr::Value(number("10"))), top.quantity);
    assert!(top.percent);
}

#[test]
fn parse_mssql_top() {
    let sql = "SELECT TOP 5 bar, baz FROM foo";
    let _ = ms_and_generic().one_statement_parses_to(sql, "SELECT TOP (5) bar, baz FROM foo");
}

fn ms() -> TestedDialects {
    TestedDialects {
        dialects: vec![Box::new(MsSqlDialect {})],
    }
}
fn ms_and_generic() -> TestedDialects {
    TestedDialects {
        dialects: vec![Box::new(MsSqlDialect {}), Box::new(GenericDialect {})],
    }
}

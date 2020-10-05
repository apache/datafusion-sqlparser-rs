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

#[macro_use]
mod test_utils;
use test_utils::*;

use sqlparser::ast::*;
use sqlparser::dialect::{GenericDialect, SnowflakeDialect};
use sqlparser::tokenizer::*;

#[test]
fn test_snowflake_create_table() {
    let sql = "CREATE TABLE _my_$table (am00unt number)";
    match snowflake_and_generic().verified_stmt(sql) {
        Statement::CreateTable { name, .. } => {
            assert_eq!("_my_$table", name.to_string());
        }
        _ => unreachable!(),
    }
}

#[test]
fn test_snowflake_single_line_tokenize() {
    let sql = "CREATE TABLE# this is a comment \ntable_1";
    let dialect = SnowflakeDialect {};
    let mut tokenizer = Tokenizer::new(&dialect, &sql);
    let tokens = tokenizer.tokenize().unwrap();

    let expected = vec![
        Token::make_keyword("CREATE"),
        Token::Whitespace(Whitespace::Space),
        Token::make_keyword("TABLE"),
        Token::Whitespace(Whitespace::SingleLineComment {
            prefix: "#".to_string(),
            comment: " this is a comment \n".to_string(),
        }),
        Token::make_word("table_1", None),
    ];

    assert_eq!(expected, tokens);

    let sql = "CREATE TABLE// this is a comment \ntable_1";
    let mut tokenizer = Tokenizer::new(&dialect, &sql);
    let tokens = tokenizer.tokenize().unwrap();

    let expected = vec![
        Token::make_keyword("CREATE"),
        Token::Whitespace(Whitespace::Space),
        Token::make_keyword("TABLE"),
        Token::Whitespace(Whitespace::SingleLineComment {
            prefix: "//".to_string(),
            comment: " this is a comment \n".to_string(),
        }),
        Token::make_word("table_1", None),
    ];

    assert_eq!(expected, tokens);
}

#[test]
fn test_sf_derives_single_table_in_parenthesis() {
    // Nesting a subquery in parentheses is non-standard, but supported in Snowflake SQL
    let sql = "SELECT * FROM ((SELECT 1) AS t)";
    let select = snowflake_and_generic().verified_only_select(sql);
    let from = only(select.from);
    assert_eq!(
        from.relation,
        TableFactor::NestedJoin(Box::new(TableWithJoins {
            relation: TableFactor::Derived {
                lateral: false,
                subquery: Box::new(snowflake_and_generic().verified_query("SELECT 1")),
                alias: Some(TableAlias {
                    name: "t".into(),
                    columns: vec![],
                })
            },
            joins: Vec::new(),
        }))
    );
}

#[test]
fn test_single_table_in_parenthesis() {
    // Parenthesized table names are non-standard, but supported in Snowflake SQL
    let sql = "SELECT * FROM (a NATURAL JOIN (b))";
    let select = snowflake_and_generic().verified_only_select(sql);
    let from = only(select.from);

    assert_eq!(from.relation, nest!(table("a"), nest!(table("b"))));

    // Double parentheses around table names are non-standard, but supported in Snowflake SQL
    let sql = "SELECT * FROM (a NATURAL JOIN ((b)))";
    let select = snowflake_and_generic().verified_only_select(sql);
    let from = only(select.from);

    assert_eq!(from.relation, nest!(table("a"), nest!(nest!(table("b")))));
}

fn snowflake_and_generic() -> TestedDialects {
    TestedDialects {
        dialects: vec![Box::new(SnowflakeDialect {}), Box::new(GenericDialect {})],
    }
}

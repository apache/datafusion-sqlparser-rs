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

/// This module contains internal utilities used for testing the library.
/// While technically public, the library's users are not supposed to rely
/// on this module, as it will change without notice.
//
// Integration tests (i.e. everything under `tests/`) import this
// via `tests/test_utils/mod.rs`.

#[cfg(not(feature = "std"))]
use alloc::{
    boxed::Box,
    string::{String, ToString},
    vec,
    vec::Vec,
};
use core::fmt::Debug;

use crate::ast::*;
use crate::dialect::*;
use crate::parser::{Parser, ParserError};
use crate::tokenizer::Tokenizer;

/// Tests use the methods on this struct to invoke the parser on one or
/// multiple dialects.
pub struct TestedDialects {
    pub dialects: Vec<Box<dyn Dialect>>,
}

impl TestedDialects {
    /// Run the given function for all of `self.dialects`, assert that they
    /// return the same result, and return that result.
    pub fn one_of_identical_results<F, T: Debug + PartialEq>(&self, f: F) -> T
    where
        F: Fn(&dyn Dialect) -> T,
    {
        let parse_results = self.dialects.iter().map(|dialect| (dialect, f(&**dialect)));
        parse_results
            .fold(None, |s, (dialect, parsed)| {
                if let Some((prev_dialect, prev_parsed)) = s {
                    assert_eq!(
                        prev_parsed, parsed,
                        "Parse results with {:?} are different from {:?}",
                        prev_dialect, dialect
                    );
                }
                Some((dialect, parsed))
            })
            .unwrap()
            .1
    }

    pub fn run_parser_method<F, T: Debug + PartialEq>(&self, sql: &str, f: F) -> T
    where
        F: Fn(&mut Parser) -> T,
    {
        self.one_of_identical_results(|dialect| {
            let mut tokenizer = Tokenizer::new(dialect, sql);
            let (tokens, pos_map) = tokenizer.tokenize().unwrap();
            f(&mut Parser::new(tokens, pos_map, dialect))
        })
    }

    pub fn parse_sql_statements(&self, sql: &str) -> Result<Vec<Statement>, ParserError> {
        self.one_of_identical_results(|dialect| Parser::parse_sql(dialect, sql))
        // To fail the `ensure_multiple_dialects_are_tested` test:
        // Parser::parse_sql(&**self.dialects.first().unwrap(), sql)
    }

    /// Ensures that `sql` parses as a single statement and returns it.
    /// If non-empty `canonical` SQL representation is provided,
    /// additionally asserts that parsing `sql` results in the same parse
    /// tree as parsing `canonical`, and that serializing it back to string
    /// results in the `canonical` representation.
    pub fn one_statement_parses_to(&self, sql: &str, canonical: &str) -> Statement {
        let mut statements = self.parse_sql_statements(sql).unwrap();
        assert_eq!(statements.len(), 1);

        if !canonical.is_empty() && sql != canonical {
            assert_eq!(self.parse_sql_statements(canonical).unwrap(), statements);
        }

        let only_statement = statements.pop().unwrap();
        if !canonical.is_empty() {
            assert_eq!(canonical, only_statement.to_string())
        }
        only_statement
    }

    /// Ensures that `sql` parses as a single [Statement], and is not modified
    /// after a serialization round-trip.
    pub fn verified_stmt(&self, query: &str) -> Statement {
        self.one_statement_parses_to(query, query)
    }

    /// Ensures that `sql` parses as a single [Query], and is not modified
    /// after a serialization round-trip.
    pub fn verified_query(&self, sql: &str) -> Query {
        match self.verified_stmt(sql) {
            Statement::Query(query) => *query,
            _ => panic!("Expected Query"),
        }
    }

    /// Ensures that `sql` parses as a single [Select], and is not modified
    /// after a serialization round-trip.
    pub fn verified_only_select(&self, query: &str) -> Select {
        match self.verified_query(query).body {
            SetExpr::Select(s) => *s,
            _ => panic!("Expected SetExpr::Select"),
        }
    }

    /// Ensures that `sql` parses as an expression, and is not modified
    /// after a serialization round-trip.
    pub fn verified_expr(&self, sql: &str) -> Expr {
        let ast = self
            .run_parser_method(sql, |parser| parser.parse_expr())
            .unwrap();
        assert_eq!(sql, &ast.to_string(), "round-tripping without changes");
        ast
    }
}

pub fn all_dialects() -> TestedDialects {
    TestedDialects {
        dialects: vec![
            Box::new(GenericDialect {}),
            Box::new(PostgreSqlDialect {}),
            Box::new(MsSqlDialect {}),
            Box::new(AnsiDialect {}),
            Box::new(SnowflakeDialect {}),
            Box::new(HiveDialect {}),
        ],
    }
}

pub fn only<T>(v: impl IntoIterator<Item = T>) -> T {
    let mut iter = v.into_iter();
    if let (Some(item), None) = (iter.next(), iter.next()) {
        item
    } else {
        panic!("only called on collection without exactly one item")
    }
}

pub fn expr_from_projection(item: &SelectItem) -> &Expr {
    match item {
        SelectItem::UnnamedExpr(expr) => expr,
        _ => panic!("Expected UnnamedExpr"),
    }
}

pub fn number(n: &'static str) -> Value {
    Value::Number(n.parse().unwrap(), false)
}

pub fn table_alias(name: impl Into<String>) -> Option<TableAlias> {
    Some(TableAlias {
        name: Ident::new(name),
        columns: vec![],
    })
}

pub fn table(name: impl Into<String>) -> TableFactor {
    TableFactor::Table {
        name: ObjectName(vec![Ident::new(name.into())]),
        alias: None,
        args: vec![],
        with_hints: vec![],
        instant: None,
    }
}

pub fn join(relation: TableFactor) -> Join {
    Join {
        relation,
        join_operator: JoinOperator::Inner(JoinConstraint::Natural),
    }
}

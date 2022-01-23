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

#![allow(dead_code)]

#[cfg(not(feature = "std"))]
use alloc::{
    boxed::Box,
    string::{String, ToString},
    vec,
    vec::Vec,
};
use core::fmt::Debug;

use sqlparser::dialect::*;
use sqlparser::parser::{Parser, ParserError};
use sqlparser::{ast::*, tokenizer::Token};

#[macro_export]
macro_rules! nest {
    ($base:expr $(, $join:expr)*) => {
        TableFactor::NestedJoin(Box::new(TableWithJoins {
            relation: $base,
            joins: vec![$(join($join)),*]
        }))
    };
}

pub trait Parse {
    fn parse_statements(&mut self) -> Result<Vec<Statement>, ParserError>;
    fn parse_expr(&mut self) -> Result<Expr, ParserError>;
    fn parse_object_name(&mut self) -> Result<ObjectName, ParserError>;
    fn peek_token(&self) -> Token;
    fn next_token(&mut self) -> Token;
    fn prev_token(&mut self);
}

impl<D: Dialect> Parse for Parser<D> {
    fn parse_statements(&mut self) -> Result<Vec<Statement>, ParserError> {
        self.parse_statements()
    }
    fn parse_expr(&mut self) -> Result<Expr, ParserError> {
        self.parse_expr()
    }

    fn parse_object_name(&mut self) -> Result<ObjectName, ParserError> {
        self.parse_object_name()
    }

    fn peek_token(&self) -> Token {
        self.peek_token()
    }

    fn next_token(&mut self) -> Token {
        self.next_token()
    }

    fn prev_token(&mut self) {
        self.prev_token()
    }
}

type ParserConstructor = fn(tokens: &str) -> Box<dyn Parse>;

/// Tests use the methods on this struct to invoke the parser on one or
/// multiple dialects.
pub struct TestedDialects {
    pub dialects: Vec<(&'static str, ParserConstructor)>,
}

impl TestedDialects {
    /// Run the given function for all of `self.dialects`, assert that they
    /// return the same result, and return that result.
    pub fn one_of_identical_results<F, T: Debug + PartialEq>(&self, f: F) -> T
    where
        F: Fn(&ParserConstructor) -> T,
    {
        let parse_results = self
            .dialects
            .iter()
            .map(|(name, dialect)| (name, f(dialect)));
        parse_results
            .fold(None, |s, (dialect, parsed)| {
                if let Some((prev_dialect, prev_parsed)) = s {
                    assert_eq!(
                        prev_parsed, parsed,
                        "Parse results with {} are different from {}",
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
        F: Fn(&mut dyn Parse) -> T,
    {
        self.one_of_identical_results(|constructor| f(constructor(sql).as_mut()))
    }

    pub fn parse_sql_statements(&self, sql: &str) -> Result<Vec<Statement>, ParserError> {
        self.one_of_identical_results(|constructor| constructor(sql).parse_statements())
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

#[macro_export]
macro_rules! tested_dialects {
    ($($dialect:ident),+) => {
        TestedDialects {
            dialects: vec![
                $(
                    (stringify!($dialect), |input| {
                        Box::new(sqlparser::parser::Parser::<$dialect>::new(
                            sqlparser::tokenizer::Tokenizer::<$dialect>::new(input).tokenize().unwrap(),
                        ))
                    })
                ),+
            ],
        }
    };
}

pub fn all_dialects() -> TestedDialects {
    tested_dialects!(
        GenericDialect,
        PostgreSqlDialect,
        MsSqlDialect,
        AnsiDialect,
        SnowflakeDialect,
        HiveDialect
    )
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
    }
}

pub fn join(relation: TableFactor) -> Join {
    Join {
        relation,
        join_operator: JoinOperator::Inner(JoinConstraint::Natural),
    }
}

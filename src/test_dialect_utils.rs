// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

/// This module contains internal utilities used for testing the library.
/// While technically public, the library's users are not supposed to rely
/// on this module, as it will change without notice.
//
// It's re-exported in `src/test_utils.rs` and used in integration tests
// via `tests::test_utils::*`.

use core::fmt::Debug;

use crate::ast::*;
use crate::dialect::*;
use crate::parser::Parser;
use crate::parser::{ParserError, ParserOptions};
use crate::tokenizer::{Token, Tokenizer};

/// Tests use the methods on this struct to invoke the parser on one or
/// multiple dialects.
pub struct TestedDialects {
    pub dialects: Vec<Box<dyn Dialect>>,
    pub options: Option<ParserOptions>,
    pub recursion_limit: Option<usize>,
}

impl TestedDialects {
    /// Create a TestedDialects with default options and the given dialects.
    pub fn new(dialects: Vec<Box<dyn Dialect>>) -> Self {
        Self {
            dialects,
            options: None,
            recursion_limit: None,
        }
    }

    pub fn new_with_options(dialects: Vec<Box<dyn Dialect>>, options: ParserOptions) -> Self {
        Self {
            dialects,
            options: Some(options),
            recursion_limit: None,
        }
    }

    pub fn with_recursion_limit(mut self, recursion_limit: usize) -> Self {
        self.recursion_limit = Some(recursion_limit);
        self
    }

    fn new_parser<'a>(&self, dialect: &'a dyn Dialect) -> Parser<'a> {
        let parser = Parser::new(dialect);
        let parser = if let Some(options) = &self.options {
            parser.with_options(options.clone())
        } else {
            parser
        };

        let parser = if let Some(recursion_limit) = &self.recursion_limit {
            parser.with_recursion_limit(*recursion_limit)
        } else {
            parser
        };

        parser
    }

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
                        "Parse results with {prev_dialect:?} are different from {dialect:?}"
                    );
                }
                Some((dialect, parsed))
            })
            .expect("tested dialects cannot be empty")
            .1
    }

    pub fn run_parser_method<F, T: Debug + PartialEq>(&self, sql: &str, f: F) -> T
    where
        F: Fn(&mut Parser) -> T,
    {
        self.one_of_identical_results(|dialect| {
            let mut parser = self.new_parser(dialect).try_with_sql(sql).unwrap();
            f(&mut parser)
        })
    }

    /// Parses a single SQL string into multiple statements, ensuring
    /// the result is the same for all tested dialects.
    pub fn parse_sql_statements(&self, sql: &str) -> Result<Vec<Statement>, ParserError> {
        self.one_of_identical_results(|dialect| {
            let mut tokenizer = Tokenizer::new(dialect, sql);
            if let Some(options) = &self.options {
                tokenizer = tokenizer.with_unescape(options.unescape);
            }
            let tokens = tokenizer.tokenize()?;
            self.new_parser(dialect)
                .with_tokens(tokens)
                .parse_statements()
        })
        // To fail the `ensure_multiple_dialects_are_tested` test:
        // Parser::parse_sql(&**self.dialects.first().unwrap(), sql)
    }

    /// Ensures that `sql` parses as a single [Statement] for all tested
    /// dialects.
    ///
    /// In general, the canonical SQL should be the same (see crate
    /// documentation for rationale) and you should prefer the `verified_`
    /// variants in testing, such as  [`verified_statement`] or
    /// [`verified_query`].
    ///
    /// If `canonical` is non empty,this function additionally asserts
    /// that:
    ///
    /// 1. parsing `sql` results in the same [`Statement`] as parsing
    ///    `canonical`.
    ///
    /// 2. re-serializing the result of parsing `sql` produces the same
    ///    `canonical` sql string
    pub fn one_statement_parses_to(&self, sql: &str, canonical: &str) -> Statement {
        let mut statements = self.parse_sql_statements(sql).expect(sql);
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

    /// Ensures that `sql` parses as an [`Expr`], and that
    /// re-serializing the parse result produces canonical
    pub fn expr_parses_to(&self, sql: &str, canonical: &str) -> Expr {
        let ast = self
            .run_parser_method(sql, |parser| parser.parse_expr())
            .unwrap();
        assert_eq!(canonical, &ast.to_string());
        ast
    }

    /// Ensures that `sql` parses as a single [Statement], and that
    /// re-serializing the parse result produces the same `sql`
    /// string (is not modified after a serialization round-trip).
    pub fn verified_stmt(&self, sql: &str) -> Statement {
        self.one_statement_parses_to(sql, sql)
    }

    /// Ensures that `sql` parses as a single [Query], and that
    /// re-serializing the parse result produces the same `sql`
    /// string (is not modified after a serialization round-trip).
    pub fn verified_query(&self, sql: &str) -> Query {
        match self.verified_stmt(sql) {
            Statement::Query(query) => *query,
            _ => panic!("Expected Query"),
        }
    }

    /// Ensures that `sql` parses as a single [Query], and that
    /// re-serializing the parse result matches the given canonical
    /// sql string.
    pub fn verified_query_with_canonical(&self, query: &str, canonical: &str) -> Query {
        match self.one_statement_parses_to(query, canonical) {
            Statement::Query(query) => *query,
            _ => panic!("Expected Query"),
        }
    }

    /// Ensures that `sql` parses as a single [Select], and that
    /// re-serializing the parse result produces the same `sql`
    /// string (is not modified after a serialization round-trip).
    pub fn verified_only_select(&self, query: &str) -> Select {
        match *self.verified_query(query).body {
            SetExpr::Select(s) => *s,
            _ => panic!("Expected SetExpr::Select"),
        }
    }

    /// Ensures that `sql` parses as a single [`Select`], and that additionally:
    ///
    /// 1. parsing `sql` results in the same [`Statement`] as parsing
    ///    `canonical`.
    ///
    /// 2. re-serializing the result of parsing `sql` produces the same
    ///    `canonical` sql string
    pub fn verified_only_select_with_canonical(&self, query: &str, canonical: &str) -> Select {
        let q = match self.one_statement_parses_to(query, canonical) {
            Statement::Query(query) => *query,
            _ => panic!("Expected Query"),
        };
        match *q.body {
            SetExpr::Select(s) => *s,
            _ => panic!("Expected SetExpr::Select"),
        }
    }

    /// Ensures that `sql` parses as an [`Expr`], and that
    /// re-serializing the parse result produces the same `sql`
    /// string (is not modified after a serialization round-trip).
    pub fn verified_expr(&self, sql: &str) -> Expr {
        self.expr_parses_to(sql, sql)
    }

    /// Check that the tokenizer returns the expected tokens for the given SQL.
    pub fn tokenizes_to(&self, sql: &str, expected: Vec<Token>) {
        if self.dialects.is_empty() {
            panic!("No dialects to test");
        }

        self.dialects.iter().for_each(|dialect| {
            let mut tokenizer = Tokenizer::new(&**dialect, sql);
            if let Some(options) = &self.options {
                tokenizer = tokenizer.with_unescape(options.unescape);
            }
            let tokens = tokenizer.tokenize().unwrap();
            assert_eq!(expected, tokens, "Tokenized differently for {:?}", dialect);
        });
    }
}

/// Returns all available dialects.
pub fn all_dialects() -> TestedDialects {
    TestedDialects::new(vec![
        Box::new(GenericDialect {}),
        Box::new(PostgreSqlDialect {}),
        Box::new(MsSqlDialect {}),
        Box::new(AnsiDialect {}),
        Box::new(SnowflakeDialect {}),
        Box::new(HiveDialect {}),
        Box::new(RedshiftSqlDialect {}),
        Box::new(MySqlDialect {}),
        Box::new(BigQueryDialect {}),
        Box::new(SQLiteDialect {}),
        Box::new(DuckDbDialect {}),
        Box::new(DatabricksDialect {}),
        Box::new(ClickHouseDialect {}),
    ])
}

/// Returns all dialects matching the given predicate.
pub fn all_dialects_where<F>(predicate: F) -> TestedDialects
where
    F: Fn(&dyn Dialect) -> bool,
{
    let mut dialects = all_dialects();
    dialects.dialects.retain(|d| predicate(&**d));
    dialects
}

/// Returns available dialects. The `except` predicate is used
/// to filter out specific dialects.
pub fn all_dialects_except<F>(except: F) -> TestedDialects
where
    F: Fn(&dyn Dialect) -> bool,
{
    all_dialects_where(|d| !except(d))
}

use std::fmt::Debug;

use super::dialect::*;
use super::sqlast::*;
use super::sqlparser::{Parser, ParserError};
use super::sqltokenizer::Tokenizer;

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
            let tokens = tokenizer.tokenize().unwrap();
            f(&mut Parser::new(tokens))
        })
    }

    pub fn parse_sql_statements(&self, sql: &str) -> Result<Vec<SQLStatement>, ParserError> {
        self.one_of_identical_results(|dialect| Parser::parse_sql(dialect, sql.to_string()))
        // To fail the `ensure_multiple_dialects_are_tested` test:
        // Parser::parse_sql(&**self.dialects.first().unwrap(), sql.to_string())
    }

    /// Ensures that `sql` parses as a single statement, optionally checking
    /// that converting AST back to string equals to `canonical` (unless an
    /// empty canonical string is provided).
    pub fn one_statement_parses_to(&self, sql: &str, canonical: &str) -> SQLStatement {
        let mut statements = self.parse_sql_statements(&sql).unwrap();
        assert_eq!(statements.len(), 1);

        let only_statement = statements.pop().unwrap();
        if !canonical.is_empty() {
            assert_eq!(canonical, only_statement.to_string())
        }
        only_statement
    }

    /// Ensures that `sql` parses as a single SQLStatement, and is not modified
    /// after a serialization round-trip.
    pub fn verified_stmt(&self, query: &str) -> SQLStatement {
        self.one_statement_parses_to(query, query)
    }

    /// Ensures that `sql` parses as a single SQLQuery, and is not modified
    /// after a serialization round-trip.
    pub fn verified_query(&self, sql: &str) -> SQLQuery {
        match self.verified_stmt(sql) {
            SQLStatement::SQLQuery(query) => *query,
            _ => panic!("Expected SQLQuery"),
        }
    }

    /// Ensures that `sql` parses as a single SQLSelect, and is not modified
    /// after a serialization round-trip.
    pub fn verified_only_select(&self, query: &str) -> SQLSelect {
        match self.verified_query(query).body {
            SQLSetExpr::Select(s) => *s,
            _ => panic!("Expected SQLSetExpr::Select"),
        }
    }

    /// Ensures that `sql` parses as an expression, and is not modified
    /// after a serialization round-trip.
    pub fn verified_expr(&self, sql: &str) -> ASTNode {
        let ast = self.run_parser_method(sql, Parser::parse_expr).unwrap();
        assert_eq!(sql, &ast.to_string(), "round-tripping without changes");
        ast
    }
}

pub fn all_dialects() -> TestedDialects {
    TestedDialects {
        dialects: vec![
            Box::new(GenericSqlDialect {}),
            Box::new(PostgreSqlDialect {}),
            Box::new(AnsiSqlDialect {}),
        ],
    }
}

pub fn only<T>(v: &[T]) -> &T {
    assert_eq!(1, v.len());
    v.first().unwrap()
}

pub fn expr_from_projection(item: &SQLSelectItem) -> &ASTNode {
    match item {
        SQLSelectItem::UnnamedExpression(expr) => expr,
        _ => panic!("Expected UnnamedExpression"),
    }
}

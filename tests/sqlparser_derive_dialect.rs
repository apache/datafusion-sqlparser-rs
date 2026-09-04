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

//! Tests for the `derive_dialect!` macro.

use sqlparser::ast::{
    BinaryOperator, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, LambdaSyntax, Statement,
};
use sqlparser::derive_dialect;
use sqlparser::dialect::{Dialect, GenericDialect, MySqlDialect, PostgreSqlDialect};
use sqlparser::parser::Parser;
use sqlparser::test_utils::{expr_from_projection, only};

#[test]
fn test_method_overrides() {
    derive_dialect!(EnhancedGenericDialect, GenericDialect, overrides = {
        supports_order_by_all = true,
        supports_triple_quoted_string = true,
    });
    let dialect = EnhancedGenericDialect::new();

    // Overridden methods
    assert!(dialect.supports_order_by_all());
    assert!(dialect.supports_triple_quoted_string());

    // Non-overridden retains base behavior
    assert!(!dialect.supports_factorial_operator());

    // Parsing works with the overrides
    let result = Parser::new(&dialect)
        .try_with_sql("SELECT '''value''' FROM t ORDER BY ALL")
        .unwrap()
        .parse_statements();

    assert!(result.is_ok());
}

#[test]
fn test_preserve_type_id() {
    // Check the override works and the parser recognizes it as the base type
    derive_dialect!(
        PreservedTypeDialect,
        GenericDialect,
        preserve_type_id = true,
        overrides = { supports_order_by_all = true }
    );
    let dialect = PreservedTypeDialect::new();
    let d: &dyn Dialect = &dialect;

    assert!(dialect.supports_order_by_all());
    assert!(d.is::<GenericDialect>());
}

#[test]
fn test_different_base_dialects() {
    derive_dialect!(
        EnhancedMySqlDialect,
        MySqlDialect,
        overrides = { supports_order_by_all = true }
    );
    derive_dialect!(UniquePostgreSqlDialect, PostgreSqlDialect);

    let pg = UniquePostgreSqlDialect::new();
    let mysql = EnhancedMySqlDialect::new();

    // Inherit different base behaviors
    assert!(pg.supports_filter_during_aggregation()); // PostgreSQL feature
    assert!(mysql.supports_string_literal_backslash_escape()); // MySQL feature
    assert!(mysql.supports_order_by_all()); // Override

    // Each has unique TypeId
    let pg_ref: &dyn Dialect = &pg;
    let mysql_ref: &dyn Dialect = &mysql;
    assert!(pg_ref.is::<UniquePostgreSqlDialect>());
    assert!(!pg_ref.is::<PostgreSqlDialect>());
    assert!(mysql_ref.is::<EnhancedMySqlDialect>());
}

#[test]
fn test_identifier_quote_style_overrides() {
    derive_dialect!(
        BacktickGenericDialect,
        GenericDialect,
        overrides = { identifier_quote_style = '`' }
    );
    derive_dialect!(
        AnotherBacktickDialect,
        GenericDialect,
        overrides = { identifier_quote_style = '[' }
    );
    derive_dialect!(
        QuotelessPostgreSqlDialect,
        PostgreSqlDialect,
        preserve_type_id = true,
        overrides = { identifier_quote_style = None }
    );

    // Char literal (auto-wrapped in Some)
    assert_eq!(
        BacktickGenericDialect::new().identifier_quote_style("x"),
        Some('`')
    );
    // Another char literal
    assert_eq!(
        AnotherBacktickDialect::new().identifier_quote_style("x"),
        Some('[')
    );
    // None (overrides PostgreSQL's default '"')
    assert_eq!(
        QuotelessPostgreSqlDialect::new().identifier_quote_style("x"),
        None
    );
}

#[test]
fn test_lambda_keyword_syntax_on_postgres_derivative() {
    // A PostgreSQL derivative can opt into the `LAMBDA` keyword spelling of
    // lambda functions without giving up `->` as JSON member access. The two
    // meet in a single expression below: a lambda whose body is a JSON access.
    derive_dialect!(
        LambdaPostgreSqlDialect,
        PostgreSqlDialect,
        overrides = { supports_lambda_keyword_syntax = true }
    );
    let dialect = LambdaPostgreSqlDialect::new();

    // Only the keyword spelling is enabled; the arrow spelling stays off.
    assert!(dialect.supports_lambda_keyword_syntax());
    assert!(!dialect.supports_lambda_functions());

    let sql = "SELECT transform(xs, lambda x : (x -> 'a')::INT + 1)";
    let ast = Parser::parse_sql(&dialect, sql).unwrap();
    assert_eq!(sql, ast[0].to_string());

    // Round-tripping alone would not distinguish a JSON access from a nested
    // lambda, since both print as `x -> 'a'`, so check the parsed shape.
    let Statement::Query(query) = &ast[0] else {
        panic!("unexpected statement {}", ast[0]);
    };
    let Expr::Function(func) =
        expr_from_projection(only(&query.body.as_select().unwrap().projection))
    else {
        panic!("expected a function call");
    };
    let FunctionArguments::List(args) = &func.args else {
        panic!("expected an argument list");
    };
    let [_, FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Lambda(lambda)))] = &args.args[..]
    else {
        panic!("expected the second argument to be a lambda");
    };

    // The lambda came from the `LAMBDA` keyword, not from `->`.
    assert_eq!(LambdaSyntax::LambdaKeyword, lambda.syntax);

    // And the `->` in its body is still JSON member access.
    let Expr::BinaryOp {
        left,
        op: BinaryOperator::Plus,
        ..
    } = lambda.body.as_ref()
    else {
        panic!("expected the lambda body to be an addition");
    };
    let Expr::Cast { expr, .. } = left.as_ref() else {
        panic!("expected the left operand to be a cast");
    };
    let Expr::Nested(json_access) = expr.as_ref() else {
        panic!("expected the cast operand to be parenthesized");
    };
    let Expr::BinaryOp { op, .. } = json_access.as_ref() else {
        panic!("expected `->` to stay a binary operator");
    };
    assert_eq!(&BinaryOperator::Arrow, op);
}

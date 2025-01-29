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

//! Test the ability for dialects to override parsing

use sqlparser::{
    ast::{BinaryOperator, Expr, Statement, Value},
    dialect::Dialect,
    keywords::Keyword,
    parser::{Parser, ParserError},
    tokenizer::Token,
};

#[test]
fn custom_prefix_parser() -> Result<(), ParserError> {
    #[derive(Debug)]
    struct MyDialect {}

    impl Dialect for MyDialect {
        fn is_identifier_start(&self, ch: char) -> bool {
            is_identifier_start(ch)
        }

        fn is_identifier_part(&self, ch: char) -> bool {
            is_identifier_part(ch)
        }

        fn parse_prefix(&self, parser: &mut Parser) -> Option<Result<Expr, ParserError>> {
            if parser.consume_token(&Token::Number("1".to_string(), false)) {
                Some(Ok(Expr::Value(Value::Null)))
            } else {
                None
            }
        }
    }

    let dialect = MyDialect {};
    let sql = "SELECT 1 + 2";
    let ast = Parser::parse_sql(&dialect, sql)?;
    let query = &ast[0];
    assert_eq!("SELECT NULL + 2", &format!("{query}"));
    Ok(())
}

#[test]
fn custom_infix_parser() -> Result<(), ParserError> {
    #[derive(Debug)]
    struct MyDialect {}

    impl Dialect for MyDialect {
        fn is_identifier_start(&self, ch: char) -> bool {
            is_identifier_start(ch)
        }

        fn is_identifier_part(&self, ch: char) -> bool {
            is_identifier_part(ch)
        }

        fn parse_infix(
            &self,
            parser: &mut Parser,
            expr: &Expr,
            _precedence: u8,
        ) -> Option<Result<Expr, ParserError>> {
            if parser.consume_token(&Token::Plus) {
                Some(Ok(Expr::BinaryOp {
                    left: Box::new(expr.clone()),
                    op: BinaryOperator::Multiply, // translate Plus to Multiply
                    right: Box::new(parser.parse_expr().unwrap()),
                }))
            } else {
                None
            }
        }
    }

    let dialect = MyDialect {};
    let sql = "SELECT 1 + 2";
    let ast = Parser::parse_sql(&dialect, sql)?;
    let query = &ast[0];
    assert_eq!("SELECT 1 * 2", &format!("{query}"));
    Ok(())
}

#[test]
fn custom_statement_parser() -> Result<(), ParserError> {
    #[derive(Debug)]
    struct MyDialect {}

    impl Dialect for MyDialect {
        fn is_identifier_start(&self, ch: char) -> bool {
            is_identifier_start(ch)
        }

        fn is_identifier_part(&self, ch: char) -> bool {
            is_identifier_part(ch)
        }

        fn parse_statement(&self, parser: &mut Parser) -> Option<Result<Statement, ParserError>> {
            if parser.parse_keyword(Keyword::SELECT) {
                for _ in 0..3 {
                    let _ = parser.next_token();
                }
                Some(Ok(Statement::Commit {
                    chain: false,
                    end: false,
                    modifier: None,
                }))
            } else {
                None
            }
        }
    }

    let dialect = MyDialect {};
    let sql = "SELECT 1 + 2";
    let ast = Parser::parse_sql(&dialect, sql)?;
    let query = &ast[0];
    assert_eq!("COMMIT", &format!("{query}"));
    Ok(())
}

#[test]
fn test_map_syntax_not_support_default() -> Result<(), ParserError> {
    #[derive(Debug)]
    struct MyDialect {}

    impl Dialect for MyDialect {
        fn is_identifier_start(&self, ch: char) -> bool {
            is_identifier_start(ch)
        }

        fn is_identifier_part(&self, ch: char) -> bool {
            is_identifier_part(ch)
        }
    }

    let dialect = MyDialect {};
    let sql = "SELECT MAP {1: 2}";
    let ast = Parser::parse_sql(&dialect, sql);
    assert!(ast.is_err());
    Ok(())
}

fn is_identifier_start(ch: char) -> bool {
    ch.is_ascii_lowercase() || ch.is_ascii_uppercase() || ch == '_'
}

fn is_identifier_part(ch: char) -> bool {
    ch.is_ascii_lowercase()
        || ch.is_ascii_uppercase()
        || ch.is_ascii_digit()
        || ch == '$'
        || ch == '_'
}

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

//! A zero-copy Pratt SQL parser with composable dialects.

// The derive macros name this crate's traits by their public paths.
pub mod ast;
pub mod dialect;
pub mod display;
pub mod grammar;
pub mod lexer;
pub mod parser;

use std::fmt;

use ast::{Expr, Query, Span, Statement};
use dialect::Dialect;
use parser::Parser;

#[derive(Debug, Clone, PartialEq)]
pub struct ParseError {
    pub message: String,
    pub span: Span,
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} at byte {}", self.message, self.span.start)
    }
}

impl std::error::Error for ParseError {}

pub type PResult<T> = Result<T, ParseError>;

/// Parses a list of statements separated by semicolons.
pub fn parse_sql<D: Dialect>(sql: &str) -> PResult<Vec<Statement<'_, D::Ext>>> {
    parse_statements(&mut Parser::<D>::new(sql)?)
}

fn parse_statements<'a, D: Dialect>(
    parser: &mut Parser<'a, D>,
) -> PResult<Vec<Statement<'a, D::Ext>>> {
    let mut statements = Vec::new();
    loop {
        while parser.eat_op(";") {}
        if parser.at_eof() {
            return Ok(statements);
        }
        statements.push(parser.parse_statement()?);
        if !parser.at_op(";") && !parser.at_eof() {
            return Err(parser.expected("end of statement"));
        }
    }
}

/// Parses a single query, with an optional trailing semicolon.
pub fn parse_query<D: Dialect>(sql: &str) -> PResult<Query<'_, D::Ext>> {
    let mut parser = Parser::<D>::new(sql)?;
    let query = D::parse_query(&mut parser)?;
    parser.eat_op(";");
    parser.expect_eof()?;
    Ok(query)
}

/// Parses a single expression.
pub fn parse_expr<D: Dialect>(sql: &str) -> PResult<Expr<'_, D::Ext>> {
    let mut parser = Parser::<D>::new(sql)?;
    let expr = parser.parse_expr()?;
    parser.expect_eof()?;
    Ok(expr)
}

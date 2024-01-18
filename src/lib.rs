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

//! # SQL Parser for Rust
//!
//! This crate provides an ANSI:SQL 2011 lexer and parser that can parse SQL
//! into an Abstract Syntax Tree ([`AST`]). See the [sqlparser crates.io page]
//! for more information.
//!
//! For more information:
//! 1. [`Parser::parse_sql`] and [`Parser::new`] for the Parsing API
//! 2. [`ast`] for the AST structure
//! 3. [`Dialect`] for supported SQL dialects
//!
//! # Example parsing SQL text
//!
//! ```
//! use sqlparser::dialect::GenericDialect;
//! use sqlparser::parser::Parser;
//!
//! let dialect = GenericDialect {}; // or AnsiDialect
//!
//! let sql = "SELECT a, b, 123, myfunc(b) \
//!            FROM table_1 \
//!            WHERE a > b AND b < 100 \
//!            ORDER BY a DESC, b";
//!
//! let ast = Parser::parse_sql(&dialect, sql).unwrap();
//!
//! println!("AST: {:?}", ast);
//! ```
//!
//! # Creating SQL text from AST
//!
//! This crate allows users to recover the original SQL text (with comments
//! removed, normalized whitespace and identifier capitalization), which is
//! useful for tools that analyze and manipulate SQL.
//!
//! ```
//! # use sqlparser::dialect::GenericDialect;
//! # use sqlparser::parser::Parser;
//! let sql = "SELECT a FROM table_1";
//!
//! // parse to a Vec<Statement>
//! let ast = Parser::parse_sql(&GenericDialect, sql).unwrap();
//!
//! // The original SQL text can be generated from the AST
//! assert_eq!(ast[0].to_string(), sql);
//! ```
//!
//! [sqlparser crates.io page]: https://crates.io/crates/sqlparser
//! [`Parser::parse_sql`]: crate::parser::Parser::parse_sql
//! [`Parser::new`]: crate::parser::Parser::new
//! [`AST`]: crate::ast
//! [`ast`]: crate::ast
//! [`Dialect`]: crate::dialect::Dialect

#![cfg_attr(not(feature = "std"), no_std)]
#![allow(clippy::upper_case_acronyms)]

// Allow proc-macros to find this crate
extern crate self as sqlparser;

#[cfg(not(feature = "std"))]
extern crate alloc;

#[macro_use]
#[cfg(test)]
extern crate pretty_assertions;

pub mod ast;
#[macro_use]
pub mod dialect;
pub mod keywords;
pub mod parser;
pub mod tokenizer;

#[doc(hidden)]
// This is required to make utilities accessible by both the crate-internal
// unit-tests and by the integration tests <https://stackoverflow.com/a/44541071/1026>
// External users are not supposed to rely on this module.
pub mod test_utils;

// Copyright 2018 Grove Enterprises LLC
//
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

//! SQL Parser for Rust
//!
//! Example code:
//!
//! This crate provides an ANSI:SQL 2011 lexer and parser that can parsed SQL into an Abstract
//! Syntax Tree (AST).
//!
//! ```
//! use sqlparser::sqlparser::Parser;
//!
//! let sql = "SELECT a, b, 123, myfunc(b) \
//!            FROM table_1 \
//!            WHERE a > b AND b < 100 \
//!            ORDER BY a DESC, b";
//!
//! let ast = Parser::parse_sql(sql.to_string()).unwrap();
//!
//! println!("AST: {:?}", ast);
//! ```

extern crate fnv;

#[macro_use]
extern crate lazy_static;

pub mod sqlast;
pub mod sqlparser;
pub mod sqltokenizer;

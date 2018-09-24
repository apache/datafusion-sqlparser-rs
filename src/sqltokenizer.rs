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

//! SQL Tokenizer
//!
//! The tokenizer (a.k.a. lexer) converts a string into a sequence of tokens.
//!
//! The tokens then form the input for the parser, which outputs an Abstract Syntax Tree (AST).

use std::iter::Peekable;
use std::str::Chars;

use super::dialect::Dialect;

/// SQL Token enumeration
#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    /// SQL identifier e.g. table or column name
    Identifier(String),
    /// SQL keyword  e.g. Keyword("SELECT")
    Keyword(String),
    /// Numeric literal
    Number(String),
    /// String literal
    String(String),
    /// Comma
    Comma,
    /// Whitespace (space, tab, etc)
    Whitespace(char),
    /// Equality operator `=`
    Eq,
    /// Not Equals operator `!=` or `<>`
    Neq,
    /// Less Than operator `<`
    Lt,
    /// Greater han operator `>`
    Gt,
    /// Less Than Or Equals operator `<=`
    LtEq,
    /// Greater Than Or Equals operator `>=`
    GtEq,
    /// Plus operator `+`
    Plus,
    /// Minus operator `-`
    Minus,
    /// Multiplication operator `*`
    Mult,
    /// Division operator `/`
    Div,
    /// Modulo Operator `%`
    Mod,
    /// Left parenthesis `(`
    LParen,
    /// Right parenthesis `)`
    RParen,
    /// Period (used for compound identifiers or projections into nested types)
    Period,
    /// Colon `:` 
    Colon,
    /// DoubleColon `::` (used for casting in postgresql)
    DoubleColon,
    /// SemiColon `;` used as separator for COPY and payload
    SemiColon,
    /// Backslash `\` used in terminating the COPY payload with `\.`
    Backslash,
    /// Left bracket `[`
    LBracket,
    /// Right bracket `]`
    RBracket,
}

/// Tokenizer error
#[derive(Debug, PartialEq)]
pub struct TokenizerError(String);

/// SQL Tokenizer
pub struct Tokenizer<'a> {
    dialect: &'a Dialect,
    pub query: String,
    pub line: u64,
    pub col: u64,
}

impl<'a> Tokenizer<'a> {
    /// Create a new SQL tokenizer for the specified SQL statement
    pub fn new(dialect: &'a Dialect, query: &str) -> Self {
        Self {
            dialect,
            query: query.to_string(),
            line: 1,
            col: 1,
        }
    }

    fn is_keyword(&self, s: &str) -> bool {
        //TODO: need to reintroduce FnvHashSet at some point .. iterating over keywords is
        // not fast but I want the simplicity for now while I experiment with pluggable
        // dialects
        return self.dialect.keywords().contains(&s);
    }

    /// Tokenize the statement and produce a vector of tokens
    pub fn tokenize(&mut self) -> Result<Vec<Token>, TokenizerError> {
        let mut peekable = self.query.chars().peekable();

        let mut tokens: Vec<Token> = vec![];

        while let Some(token) = self.next_token(&mut peekable)? {
            match &token {
                Token::Whitespace('\n') => {
                    self.line += 1;
                    self.col = 1;
                }

                Token::Whitespace('\t') => self.col += 4,
                Token::Identifier(s) => self.col += s.len() as u64,
                Token::Keyword(s) => self.col += s.len() as u64,
                Token::Number(s) => self.col += s.len() as u64,
                Token::String(s) => self.col += s.len() as u64,
                _ => self.col += 1,
            }

            tokens.push(token);
        }

        Ok(tokens
            .into_iter()
            .filter(|t| match t {
                Token::Whitespace(..) => false,
                _ => true,
            }).collect())
    }

    /// Get the next token or return None
    fn next_token(&self, chars: &mut Peekable<Chars>) -> Result<Option<Token>, TokenizerError> {
        //println!("next_token: {:?}", chars.peek());
        match chars.peek() {
            Some(&ch) => match ch {
                // whitespace
                ' ' | '\t' | '\n' => {
                    chars.next(); // consume
                    Ok(Some(Token::Whitespace(ch)))
                }
                // identifier or keyword
                ch if self.dialect.is_identifier_start(ch) => {
                    let mut s = String::new();
                    chars.next(); // consume
                    s.push(ch);
                    while let Some(&ch) = chars.peek() {
                        if self.dialect.is_identifier_part(ch) {
                            chars.next(); // consume
                            s.push(ch);
                        } else {
                            break;
                        }
                    }
                    let upper_str = s.to_uppercase();
                    if self.is_keyword(upper_str.as_str()) {
                        Ok(Some(Token::Keyword(upper_str)))
                    } else {
                        Ok(Some(Token::Identifier(s)))
                    }
                }
                // string
                '\'' => {
                    //TODO: handle escaped quotes in string
                    //TODO: handle EOF before terminating quote
                    let mut s = String::new();
                    chars.next(); // consume
                    while let Some(&ch) = chars.peek() {
                        match ch {
                            '\'' => {
                                chars.next(); // consume
                                break;
                            }
                            _ => {
                                chars.next(); // consume
                                s.push(ch);
                            }
                        }
                    }
                    Ok(Some(Token::String(s)))
                }
                // numbers
                '0'...'9' => {
                    let mut s = String::new();
                    while let Some(&ch) = chars.peek() {
                        match ch {
                            '0'...'9' | '.' => {
                                chars.next(); // consume
                                s.push(ch);
                            }
                            _ => break,
                        }
                    }
                    Ok(Some(Token::Number(s)))
                }
                // punctuation
                '(' => self.consume_and_return(chars, Token::LParen),
                ')' => self.consume_and_return(chars, Token::RParen),
                ',' => self.consume_and_return(chars, Token::Comma),
                // operators
                '+' => self.consume_and_return(chars, Token::Plus),
                '-' => self.consume_and_return(chars, Token::Minus),
                '*' => self.consume_and_return(chars, Token::Mult),
                '/' => self.consume_and_return(chars, Token::Div),
                '%' => self.consume_and_return(chars, Token::Mod),
                '=' => self.consume_and_return(chars, Token::Eq),
                '.' => self.consume_and_return(chars, Token::Period),
                '!' => {
                    chars.next(); // consume
                    match chars.peek() {
                        Some(&ch) => match ch {
                            '=' => self.consume_and_return(chars, Token::Neq),
                            _ => Err(TokenizerError(format!(
                                "Tokenizer Error at Line: {}, Col: {}",
                                self.line, self.col
                            ))),
                        },
                        None => Err(TokenizerError(format!(
                            "Tokenizer Error at Line: {}, Col: {}",
                            self.line, self.col
                        ))),
                    }
                }
                '<' => {
                    chars.next(); // consume
                    match chars.peek() {
                        Some(&ch) => match ch {
                            '=' => self.consume_and_return(chars, Token::LtEq),
                            '>' => self.consume_and_return(chars, Token::Neq),
                            _ => Ok(Some(Token::Lt)),
                        },
                        None => Ok(Some(Token::Lt)),
                    }
                }
                '>' => {
                    chars.next(); // consume
                    match chars.peek() {
                        Some(&ch) => match ch {
                            '=' => self.consume_and_return(chars, Token::GtEq),
                            _ => Ok(Some(Token::Gt)),
                        },
                        None => Ok(Some(Token::Gt)),
                    }
                }
                // colon
                ':' => {
                    chars.next();
                    match chars.peek() {
                        Some(&ch) => match ch {
                            // double colon
                            ':' => {
                                self.consume_and_return(chars, Token::DoubleColon)
                            }
                             _ => Ok(Some(Token::Colon)),
                        },
                        None => Ok(Some(Token::Colon)),
                    }
                }
                ';' => self.consume_and_return(chars, Token::SemiColon),
                '\\' => self.consume_and_return(chars, Token::Backslash),
                // brakets
                '[' => self.consume_and_return(chars, Token::LBracket),
                ']' => self.consume_and_return(chars, Token::RBracket),
                _ => Err(TokenizerError(format!(
                    "Tokenizer Error at Line: {}, Column: {}, unhandled char '{}'",
                    self.line, self.col, ch
                ))),
            },
            None => Ok(None),
        }
    }

    fn consume_and_return(&self, chars: &mut Peekable<Chars>, t: Token) -> Result<Option<Token>, TokenizerError> {
        chars.next();
        Ok(Some(t))
    }
}

#[cfg(test)]
mod tests {
    use super::super::dialect::GenericSqlDialect;
    use super::*;

    #[test]
    fn tokenize_select_1() {
        let sql = String::from("SELECT 1");
        let dialect = GenericSqlDialect {};
        let mut tokenizer = Tokenizer::new(&dialect, &sql);
        let tokens = tokenizer.tokenize().unwrap();

        let expected = vec![
            Token::Keyword(String::from("SELECT")),
            Token::Number(String::from("1")),
        ];

        compare(expected, tokens);
    }

    #[test]
    fn tokenize_scalar_function() {
        let sql = String::from("SELECT sqrt(1)");
        let dialect = GenericSqlDialect {};
        let mut tokenizer = Tokenizer::new(&dialect, &sql);
        let tokens = tokenizer.tokenize().unwrap();

        let expected = vec![
            Token::Keyword(String::from("SELECT")),
            Token::Identifier(String::from("sqrt")),
            Token::LParen,
            Token::Number(String::from("1")),
            Token::RParen,
        ];

        compare(expected, tokens);
    }

    #[test]
    fn tokenize_simple_select() {
        let sql = String::from("SELECT * FROM customer WHERE id = 1 LIMIT 5");
        let dialect = GenericSqlDialect {};
        let mut tokenizer = Tokenizer::new(&dialect, &sql);
        let tokens = tokenizer.tokenize().unwrap();

        let expected = vec![
            Token::Keyword(String::from("SELECT")),
            Token::Mult,
            Token::Keyword(String::from("FROM")),
            Token::Identifier(String::from("customer")),
            Token::Keyword(String::from("WHERE")),
            Token::Identifier(String::from("id")),
            Token::Eq,
            Token::Number(String::from("1")),
            Token::Keyword(String::from("LIMIT")),
            Token::Number(String::from("5")),
        ];

        compare(expected, tokens);
    }

    #[test]
    fn tokenize_string_predicate() {
        let sql = String::from("SELECT * FROM customer WHERE salary != 'Not Provided'");
        let dialect = GenericSqlDialect {};
        let mut tokenizer = Tokenizer::new(&dialect, &sql);
        let tokens = tokenizer.tokenize().unwrap();

        let expected = vec![
            Token::Keyword(String::from("SELECT")),
            Token::Mult,
            Token::Keyword(String::from("FROM")),
            Token::Identifier(String::from("customer")),
            Token::Keyword(String::from("WHERE")),
            Token::Identifier(String::from("salary")),
            Token::Neq,
            Token::String(String::from("Not Provided")),
        ];

        compare(expected, tokens);
    }

    #[test]
    fn tokenize_invalid_string() {
        let sql = String::from("\nمصطفىh");

        let dialect = GenericSqlDialect {};
        let mut tokenizer = Tokenizer::new(&dialect, &sql);
        let tokens = tokenizer.tokenize();

        match tokens {
            Err(e) => assert_eq!(
                TokenizerError(
                    "Tokenizer Error at Line: 2, Column: 1, unhandled char \'م\'".to_string()
                ),
                e
            ),
            _ => panic!("Test Failure in tokenize_invalid_string"),
        }
    }

    #[test]
    fn tokenize_invalid_string_cols() {
        let sql = String::from("\n\nSELECT * FROM table\tمصطفىh");

        let dialect = GenericSqlDialect {};
        let mut tokenizer = Tokenizer::new(&dialect, &sql);
        let tokens = tokenizer.tokenize();
        match tokens {
            Err(e) => assert_eq!(
                TokenizerError(
                    "Tokenizer Error at Line: 3, Column: 24, unhandled char \'م\'".to_string()
                ),
                e
            ),
            _ => panic!("Test Failure in tokenize_invalid_string_cols"),
        }
    }

    #[test]
    fn tokenize_is_null() {
        let sql = String::from("a IS NULL");
        let dialect = GenericSqlDialect {};
        let mut tokenizer = Tokenizer::new(&dialect, &sql);
        let tokens = tokenizer.tokenize().unwrap();

        let expected = vec![
            Token::Identifier(String::from("a")),
            Token::Keyword("IS".to_string()),
            Token::Keyword("NULL".to_string()),
        ];

        compare(expected, tokens);
    }

    fn compare(expected: Vec<Token>, actual: Vec<Token>) {
        //println!("------------------------------");
        //println!("tokens   = {:?}", actual);
        //println!("expected = {:?}", expected);
        //println!("------------------------------");
        assert_eq!(expected, actual);
    }

}

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

use std::iter::Peekable;
use std::str::Chars;

use fnv::FnvHashSet;

/// SQL Token enumeration
#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    /// SQL identifier e.g. table or column name
    Identifier{key: String, line: u64, col: u64},
    /// SQL keyword  e.g. Keyword("SELECT")
    Keyword{key: String, line: u64, col: u64},
    /// Numeric literal
    Number{num: String, line: u64, col: u64},
    /// String literal
    String{key: String, line: u64, col: u64},
    /// Comma
    Comma,
    /// Whitespace (space, tab, etc)
    Whitespace,
    /// NewLine
    NewLine,
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
}

/// Tokenizer error
#[derive(Debug, PartialEq)]
pub struct TokenizerError{key: String, line: u64, col: u64}

lazy_static! {
    static ref KEYWORDS: FnvHashSet<&'static str> = {
        let mut m = FnvHashSet::default();

        m.insert("SELECT");
        m.insert("FROM");
        m.insert("WHERE");
        m.insert("LIMIT");
        m.insert("ORDER");
        m.insert("GROUP");
        m.insert("BY");
        m.insert("HAVING");
        m.insert("UNION");
        m.insert("ALL");
        m.insert("INSERT");
        m.insert("UPDATE");
        m.insert("DELETE");
        m.insert("IN");
        m.insert("IS");
        m.insert("NULL");
        m.insert("SET");
        m.insert("CREATE");
        m.insert("EXTERNAL");
        m.insert("TABLE");
        m.insert("ASC");
        m.insert("DESC");
        m.insert("AND");
        m.insert("OR");
        m.insert("NOT");
        m.insert("AS");
        m.insert("STORED");
        m.insert("CSV");
        m.insert("PARQUET");
        m.insert("LOCATION");
        m.insert("WITH");
        m.insert("WITHOUT");
        m.insert("HEADER");
        m.insert("ROW");

        // SQL types
        m.insert("CHAR");
        m.insert("CHARACTER");
        m.insert("VARYING");
        m.insert("LARGE");
        m.insert("OBJECT");
        m.insert("VARCHAR");
        m.insert("CLOB");
        m.insert("BINARY");
        m.insert("VARBINARY");
        m.insert("BLOB");
        m.insert("FLOAT");
        m.insert("REAL");
        m.insert("DOUBLE");
        m.insert("PRECISION");
        m.insert("INT");
        m.insert("INTEGER");
        m.insert("SMALLINT");
        m.insert("BIGINT");
        m.insert("NUMERIC");
        m.insert("DECIMAL");
        m.insert("DEC");
        m.insert("BOOLEAN");
        m.insert("DATE");
        m.insert("TIME");
        m.insert("TIMESTAMP");

        m
    };
}

/// SQL Tokenizer
pub struct Tokenizer {
    pub query: String,
    pub line: u64,
    pub col: u64,
}

impl Tokenizer {
    /// Create a new SQL tokenizer for the specified SQL statement
    pub fn new(query: &str) -> Self {
        Self {
            query: query.to_string(),
            line: 0,
            col: 0,
        }
    }

    /// Tokenize the statement and produce a vector of tokens
    pub fn tokenize(&mut self) -> Result<Vec<Token>, TokenizerError> {
        let mut peekable = self.query.chars().peekable();

        let mut tokens: Vec<Token> = vec![];

        while let Some(token) = self.next_token(&mut peekable)? {

            if token == Token::NewLine {
                self.line += 1;
            }

            tokens.push(token);
        }

        Ok(tokens
            .into_iter()
            .filter(|t| match t {
                Token::Whitespace => false,
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
                    Ok(Some(Token::Whitespace))
                }
                // identifier or keyword
                'a'...'z' | 'A'...'Z' | '_' | '@' => {
                    let mut s = String::new();
                    while let Some(&ch) = chars.peek() {
                        match ch {
                            'a'...'z' | 'A'...'Z' | '_' | '0'...'9' | '@' => {
                                chars.next(); // consume
                                s.push(ch);
                            }
                            _ => break,
                        }
                    }
                    let upper_str = s.to_uppercase();
                    if KEYWORDS.contains(upper_str.as_str()) {
                        Ok(Some(Token::Keyword{key: upper_str, line: self.line, col: self.col}))
                    } else {
                        Ok(Some(Token::Identifier{key: s, line: self.line, col: self.col}))
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
                    Ok(Some(Token::String{key: s, line: self.line, col: self.col}))
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
                    Ok(Some(Token::Number{num: s, line: self.line, col: self.col}))
                }
                // punctuation
                ',' => {
                    chars.next();
                    Ok(Some(Token::Comma))
                }
                '(' => {
                    chars.next();
                    Ok(Some(Token::LParen))
                }
                ')' => {
                    chars.next();
                    Ok(Some(Token::RParen))
                }
                // operators
                '+' => {
                    chars.next();
                    Ok(Some(Token::Plus))
                }
                '-' => {
                    chars.next();
                    Ok(Some(Token::Minus))
                }
                '*' => {
                    chars.next();
                    Ok(Some(Token::Mult))
                }
                '/' => {
                    chars.next();
                    Ok(Some(Token::Div))
                }
                '%' => {
                    chars.next();
                    Ok(Some(Token::Mod))
                }
                '=' => {
                    chars.next();
                    Ok(Some(Token::Eq))
                }
                '.' => {
                    chars.next();
                    Ok(Some(Token::Period))
                }
                '!' => {
                    chars.next(); // consume
                    match chars.peek() {
                        Some(&ch) => match ch {
                            '=' => {
                                chars.next();
                                Ok(Some(Token::Neq))
                            }
                            _ => Err(TokenizerError{
                                        key: format!("TBD"),
                                        line: self.line,
                                        col: self.col,
                            }),
                        },
                        None => Err(TokenizerError{
                                        key: format!("TBD"),
                                        line: self.line,
                                        col: self.col,
                        }),
                    }
                }
                '<' => {
                    chars.next(); // consume
                    match chars.peek() {
                        Some(&ch) => match ch {
                            '=' => {
                                chars.next();
                                Ok(Some(Token::LtEq))
                            }
                            '>' => {
                                chars.next();
                                Ok(Some(Token::Neq))
                            }
                            _ => Ok(Some(Token::Lt)),
                        },
                        None => Ok(Some(Token::Lt)),
                    }
                }
                '>' => {
                    chars.next(); // consume
                    match chars.peek() {
                        Some(&ch) => match ch {
                            '=' => {
                                chars.next();
                                Ok(Some(Token::GtEq))
                            }
                            _ => Ok(Some(Token::Gt)),
                        },
                        None => Ok(Some(Token::Gt)),
                    }
                }
                _ => Err(TokenizerError{
                        key: format!("unhandled char '{}' in tokenizer",ch),
                        line: self.line,
                        col: self.col,
                        }),
            },
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokenize_select_1() {
        let sql = String::from("SELECT 1");
        let mut tokenizer = Tokenizer::new(&sql);
        let tokens = tokenizer.tokenize().unwrap();

        let expected = vec![
            Token::Keyword{key: String::from("SELECT"), line: 0, col: 0},
            Token::Number{num: String::from("1"), line: 0, col: 0},
        ];
    
        compare(expected, tokens);
    }
    
    #[test]
    fn test_invalid_string() {
        let sql = String::from("فروردین");

        let mut tokenizer = Tokenizer::new(&sql);
        let tokens = tokenizer.tokenize();
        
        let error: TokenizerError = TokenizerError{key: "unhandled char \'ف\' in tokenizer".to_string(), line: 0, col: 0};

        match tokens {
            Err(e) => assert_eq!(e, error),
            Ok(_) => panic!("This Should never be hit"),
        }
    
    }

    #[test]
    fn tokenize_scalar_function() {
        let sql = String::from("SELECT sqrt(1)");
        let mut tokenizer = Tokenizer::new(&sql);
        let tokens = tokenizer.tokenize().unwrap();

        let expected = vec![
            Token::Keyword{key: String::from("SELECT"), line: 0, col: 0},
            Token::Identifier{key: String::from("sqrt"), line: 0, col: 0},
            Token::LParen,
            Token::Number{num: String::from("1"), line: 0, col: 0},
            Token::RParen,
        ];

        compare(expected, tokens);
    }

    #[test]
    fn tokenize_simple_select() {
        let sql = String::from("SELECT * FROM customer WHERE id = 1 LIMIT 5");
        let mut tokenizer = Tokenizer::new(&sql);
        let tokens = tokenizer.tokenize().unwrap();

        let expected = vec![
            Token::Keyword{key: String::from("SELECT"), line: 0, col: 0},
            Token::Mult,
            Token::Keyword{key: String::from("FROM"), line: 0, col: 0},
            Token::Identifier{key: String::from("customer"), line: 0, col: 0},
            Token::Keyword{key: String::from("WHERE"), line: 0, col: 0},
            Token::Identifier{key: String::from("id"), line: 0, col: 0},
            Token::Eq,
            Token::Number{num: String::from("1"), line: 0, col: 0},
            Token::Keyword{key: String::from("LIMIT"), line: 0, col: 0},
            Token::Number{num: String::from("5"), line: 0, col: 0},
        ];

        compare(expected, tokens);
    }

    #[test]
    fn tokenize_string_predicate() {
        let sql = String::from("SELECT * FROM customer WHERE salary != 'Not Provided'");
        let mut tokenizer = Tokenizer::new(&sql);
        let tokens = tokenizer.tokenize().unwrap();

        let expected = vec![
            Token::Keyword{key: String::from("SELECT"), line: 0, col: 0},
            Token::Mult,
            Token::Keyword{key: String::from("FROM"), line: 0, col: 0},
            Token::Identifier{key: String::from("customer"), line: 0, col: 0},
            Token::Keyword{key: String::from("WHERE"), line: 0, col: 0},
            Token::Identifier{key: String::from("salary"), line: 0, col: 0},
            Token::Neq,
            Token::String{key: String::from("Not Provided"), line: 0, col: 0},
        ];

        compare(expected, tokens);
    }

    #[test]
    fn tokenize_is_null() {
        let sql = String::from("a IS NULL");
        let mut tokenizer = Tokenizer::new(&sql);
        let tokens = tokenizer.tokenize().unwrap();

        let expected = vec![
            Token::Identifier{key: String::from("a"), line: 0, col: 0},
            Token::Keyword{key: "IS".to_string(), line: 0, col: 0},
            Token::Keyword{key: "NULL".to_string(), line: 0, col: 0},
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

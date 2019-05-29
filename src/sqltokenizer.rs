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

use super::dialect::keywords::ALL_KEYWORDS;
use super::dialect::Dialect;

/// SQL Token enumeration
#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    /// A keyword (like SELECT) or an optionally quoted SQL identifier
    SQLWord(SQLWord),
    /// An unsigned numeric literal
    Number(String),
    /// A character that could not be tokenized
    Char(char),
    /// Single quoted string: i.e: 'string'
    SingleQuotedString(String),
    /// "National" string literal: i.e: N'string'
    NationalStringLiteral(String),
    /// Hexadecimal string literal: i.e.: X'deadbeef'
    HexStringLiteral(String),
    /// Comma
    Comma,
    /// Whitespace (space, tab, etc)
    Whitespace(Whitespace),
    /// Equality operator `=`
    Eq,
    /// Not Equals operator `<>` (or `!=` in some dialects)
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
    /// Ampersand &
    Ampersand,
    /// Left brace `{`
    LBrace,
    /// Right brace `}`
    RBrace,
}

impl ToString for Token {
    fn to_string(&self) -> String {
        match self {
            Token::SQLWord(ref w) => w.to_string(),
            Token::Number(ref n) => n.to_string(),
            Token::Char(ref c) => c.to_string(),
            Token::SingleQuotedString(ref s) => format!("'{}'", s),
            Token::NationalStringLiteral(ref s) => format!("N'{}'", s),
            Token::HexStringLiteral(ref s) => format!("X'{}'", s),
            Token::Comma => ",".to_string(),
            Token::Whitespace(ws) => ws.to_string(),
            Token::Eq => "=".to_string(),
            Token::Neq => "<>".to_string(),
            Token::Lt => "<".to_string(),
            Token::Gt => ">".to_string(),
            Token::LtEq => "<=".to_string(),
            Token::GtEq => ">=".to_string(),
            Token::Plus => "+".to_string(),
            Token::Minus => "-".to_string(),
            Token::Mult => "*".to_string(),
            Token::Div => "/".to_string(),
            Token::Mod => "%".to_string(),
            Token::LParen => "(".to_string(),
            Token::RParen => ")".to_string(),
            Token::Period => ".".to_string(),
            Token::Colon => ":".to_string(),
            Token::DoubleColon => "::".to_string(),
            Token::SemiColon => ";".to_string(),
            Token::Backslash => "\\".to_string(),
            Token::LBracket => "[".to_string(),
            Token::RBracket => "]".to_string(),
            Token::Ampersand => "&".to_string(),
            Token::LBrace => "{".to_string(),
            Token::RBrace => "}".to_string(),
        }
    }
}

impl Token {
    pub fn make_keyword(keyword: &str) -> Self {
        Token::make_word(keyword, None)
    }
    pub fn make_word(word: &str, quote_style: Option<char>) -> Self {
        let word_uppercase = word.to_uppercase();
        //TODO: need to reintroduce FnvHashSet at some point .. iterating over keywords is
        // not fast but I want the simplicity for now while I experiment with pluggable
        // dialects
        let is_keyword = quote_style == None && ALL_KEYWORDS.contains(&word_uppercase.as_str());
        Token::SQLWord(SQLWord {
            value: word.to_string(),
            quote_style,
            keyword: if is_keyword {
                word_uppercase
            } else {
                "".to_string()
            },
        })
    }
}

/// A keyword (like SELECT) or an optionally quoted SQL identifier
#[derive(Debug, Clone, PartialEq)]
pub struct SQLWord {
    /// The value of the token, without the enclosing quotes, and with the
    /// escape sequences (if any) processed (TODO: escapes are not handled)
    pub value: String,
    /// An identifier can be "quoted" (&lt;delimited identifier> in ANSI parlance).
    /// The standard and most implementations allow using double quotes for this,
    /// but some implementations support other quoting styles as well (e.g. \[MS SQL])
    pub quote_style: Option<char>,
    /// If the word was not quoted and it matched one of the known keywords,
    /// this will have one of the values from dialect::keywords, otherwise empty
    pub keyword: String,
}

impl ToString for SQLWord {
    fn to_string(&self) -> String {
        match self.quote_style {
            Some(s) if s == '"' || s == '[' || s == '`' => {
                format!("{}{}{}", s, self.value, SQLWord::matching_end_quote(s))
            }
            None => self.value.clone(),
            _ => panic!("Unexpected quote_style!"),
        }
    }
}
impl SQLWord {
    fn matching_end_quote(ch: char) -> char {
        match ch {
            '"' => '"', // ANSI and most dialects
            '[' => ']', // MS SQL
            '`' => '`', // MySQL
            _ => panic!("unexpected quoting style!"),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Whitespace {
    Space,
    Newline,
    Tab,
    SingleLineComment(String),
    MultiLineComment(String),
}

impl ToString for Whitespace {
    fn to_string(&self) -> String {
        match self {
            Whitespace::Space => " ".to_string(),
            Whitespace::Newline => "\n".to_string(),
            Whitespace::Tab => "\t".to_string(),
            Whitespace::SingleLineComment(s) => format!("--{}", s),
            Whitespace::MultiLineComment(s) => format!("/*{}*/", s),
        }
    }
}

/// Tokenizer error
#[derive(Debug, PartialEq)]
pub struct TokenizerError(String);

/// SQL Tokenizer
pub struct Tokenizer<'a> {
    dialect: &'a dyn Dialect,
    pub query: String,
    pub line: u64,
    pub col: u64,
}

impl<'a> Tokenizer<'a> {
    /// Create a new SQL tokenizer for the specified SQL statement
    pub fn new(dialect: &'a dyn Dialect, query: &str) -> Self {
        Self {
            dialect,
            query: query.to_string(),
            line: 1,
            col: 1,
        }
    }

    /// Tokenize the statement and produce a vector of tokens
    pub fn tokenize(&mut self) -> Result<Vec<Token>, TokenizerError> {
        let mut peekable = self.query.chars().peekable();

        let mut tokens: Vec<Token> = vec![];

        while let Some(token) = self.next_token(&mut peekable)? {
            match &token {
                Token::Whitespace(Whitespace::Newline) => {
                    self.line += 1;
                    self.col = 1;
                }

                Token::Whitespace(Whitespace::Tab) => self.col += 4,
                Token::SQLWord(w) if w.quote_style == None => self.col += w.value.len() as u64,
                Token::SQLWord(w) if w.quote_style != None => self.col += w.value.len() as u64 + 2,
                Token::Number(s) => self.col += s.len() as u64,
                Token::SingleQuotedString(s) => self.col += s.len() as u64,
                _ => self.col += 1,
            }

            tokens.push(token);
        }
        Ok(tokens)
    }

    /// Get the next token or return None
    fn next_token(&self, chars: &mut Peekable<Chars<'_>>) -> Result<Option<Token>, TokenizerError> {
        //println!("next_token: {:?}", chars.peek());
        match chars.peek() {
            Some(&ch) => match ch {
                ' ' => self.consume_and_return(chars, Token::Whitespace(Whitespace::Space)),
                '\t' => self.consume_and_return(chars, Token::Whitespace(Whitespace::Tab)),
                '\n' => self.consume_and_return(chars, Token::Whitespace(Whitespace::Newline)),
                '\r' => {
                    // Emit a single Whitespace::Newline token for \r and \r\n
                    chars.next();
                    if let Some('\n') = chars.peek() {
                        chars.next();
                    }
                    Ok(Some(Token::Whitespace(Whitespace::Newline)))
                }
                'N' => {
                    chars.next(); // consume, to check the next char
                    match chars.peek() {
                        Some('\'') => {
                            // N'...' - a <national character string literal>
                            let s = self.tokenize_single_quoted_string(chars);
                            Ok(Some(Token::NationalStringLiteral(s)))
                        }
                        _ => {
                            // regular identifier starting with an "N"
                            let s = self.tokenize_word('N', chars);
                            Ok(Some(Token::make_word(&s, None)))
                        }
                    }
                }
                // The spec only allows an uppercase 'X' to introduce a hex
                // string, but PostgreSQL, at least, allows a lowercase 'x' too.
                x @ 'x' | x @ 'X' => {
                    chars.next(); // consume, to check the next char
                    match chars.peek() {
                        Some('\'') => {
                            // X'...' - a <binary string literal>
                            let s = self.tokenize_single_quoted_string(chars);
                            Ok(Some(Token::HexStringLiteral(s)))
                        }
                        _ => {
                            // regular identifier starting with an "X"
                            let s = self.tokenize_word(x, chars);
                            Ok(Some(Token::make_word(&s, None)))
                        }
                    }
                }
                // identifier or keyword
                ch if self.dialect.is_identifier_start(ch) => {
                    chars.next(); // consume the first char
                    let s = self.tokenize_word(ch, chars);
                    Ok(Some(Token::make_word(&s, None)))
                }
                // string
                '\'' => {
                    let s = self.tokenize_single_quoted_string(chars);
                    Ok(Some(Token::SingleQuotedString(s)))
                }
                // delimited (quoted) identifier
                quote_start if self.dialect.is_delimited_identifier_start(quote_start) => {
                    let mut s = String::new();
                    chars.next(); // consume the opening quote
                    let quote_end = SQLWord::matching_end_quote(quote_start);
                    while let Some(ch) = chars.next() {
                        match ch {
                            c if c == quote_end => break,
                            _ => s.push(ch),
                        }
                    }
                    Ok(Some(Token::make_word(&s, Some(quote_start))))
                }
                // numbers
                '0'..='9' => {
                    let mut s = String::new();
                    while let Some(&ch) = chars.peek() {
                        match ch {
                            '0'..='9' | '.' => {
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
                '-' => {
                    chars.next(); // consume the '-'
                    match chars.peek() {
                        Some('-') => {
                            chars.next(); // consume the second '-', starting a single-line comment
                            let mut s = String::new();
                            loop {
                                match chars.next() {
                                    Some(ch) if ch != '\n' => {
                                        s.push(ch);
                                    }
                                    other => {
                                        if other.is_some() {
                                            s.push('\n');
                                        }
                                        break Ok(Some(Token::Whitespace(
                                            Whitespace::SingleLineComment(s),
                                        )));
                                    }
                                }
                            }
                        }
                        // a regular '-' operator
                        _ => Ok(Some(Token::Minus)),
                    }
                }
                '/' => {
                    chars.next(); // consume the '/'
                    match chars.peek() {
                        Some('*') => {
                            chars.next(); // consume the '*', starting a multi-line comment
                            self.tokenize_multiline_comment(chars)
                        }
                        // a regular '/' operator
                        _ => Ok(Some(Token::Div)),
                    }
                }
                '+' => self.consume_and_return(chars, Token::Plus),
                '*' => self.consume_and_return(chars, Token::Mult),
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
                            ':' => self.consume_and_return(chars, Token::DoubleColon),
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
                '&' => self.consume_and_return(chars, Token::Ampersand),
                '{' => self.consume_and_return(chars, Token::LBrace),
                '}' => self.consume_and_return(chars, Token::RBrace),
                other => self.consume_and_return(chars, Token::Char(other)),
            },
            None => Ok(None),
        }
    }

    /// Tokenize an identifier or keyword, after the first char is already consumed.
    fn tokenize_word(&self, first_char: char, chars: &mut Peekable<Chars<'_>>) -> String {
        let mut s = String::new();
        s.push(first_char);
        while let Some(&ch) = chars.peek() {
            if self.dialect.is_identifier_part(ch) {
                chars.next(); // consume
                s.push(ch);
            } else {
                break;
            }
        }
        s
    }

    /// Read a single quoted string, starting with the opening quote.
    fn tokenize_single_quoted_string(&self, chars: &mut Peekable<Chars<'_>>) -> String {
        //TODO: handle escaped quotes in string
        //TODO: handle newlines in string
        //TODO: handle EOF before terminating quote
        //TODO: handle 'string' <white space> 'string continuation'
        let mut s = String::new();
        chars.next(); // consume the opening quote
        while let Some(&ch) = chars.peek() {
            match ch {
                '\'' => {
                    chars.next(); // consume
                    let escaped_quote = chars.peek().map(|c| *c == '\'').unwrap_or(false);
                    if escaped_quote {
                        s.push('\'');
                        chars.next();
                    } else {
                        break;
                    }
                }
                _ => {
                    chars.next(); // consume
                    s.push(ch);
                }
            }
        }
        s
    }

    fn tokenize_multiline_comment(
        &self,
        chars: &mut Peekable<Chars<'_>>,
    ) -> Result<Option<Token>, TokenizerError> {
        let mut s = String::new();
        let mut maybe_closing_comment = false;
        // TODO: deal with nested comments
        loop {
            match chars.next() {
                Some(ch) => {
                    if maybe_closing_comment {
                        if ch == '/' {
                            break Ok(Some(Token::Whitespace(Whitespace::MultiLineComment(s))));
                        } else {
                            s.push('*');
                        }
                    }
                    maybe_closing_comment = ch == '*';
                    if !maybe_closing_comment {
                        s.push(ch);
                    }
                }
                None => {
                    break Err(TokenizerError(
                        "Unexpected EOF while in a multi-line comment".to_string(),
                    ));
                }
            }
        }
    }

    fn consume_and_return(
        &self,
        chars: &mut Peekable<Chars<'_>>,
        t: Token,
    ) -> Result<Option<Token>, TokenizerError> {
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
            Token::make_keyword("SELECT"),
            Token::Whitespace(Whitespace::Space),
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
            Token::make_keyword("SELECT"),
            Token::Whitespace(Whitespace::Space),
            Token::make_word("sqrt", None),
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
            Token::make_keyword("SELECT"),
            Token::Whitespace(Whitespace::Space),
            Token::Mult,
            Token::Whitespace(Whitespace::Space),
            Token::make_keyword("FROM"),
            Token::Whitespace(Whitespace::Space),
            Token::make_word("customer", None),
            Token::Whitespace(Whitespace::Space),
            Token::make_keyword("WHERE"),
            Token::Whitespace(Whitespace::Space),
            Token::make_word("id", None),
            Token::Whitespace(Whitespace::Space),
            Token::Eq,
            Token::Whitespace(Whitespace::Space),
            Token::Number(String::from("1")),
            Token::Whitespace(Whitespace::Space),
            Token::make_keyword("LIMIT"),
            Token::Whitespace(Whitespace::Space),
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
            Token::make_keyword("SELECT"),
            Token::Whitespace(Whitespace::Space),
            Token::Mult,
            Token::Whitespace(Whitespace::Space),
            Token::make_keyword("FROM"),
            Token::Whitespace(Whitespace::Space),
            Token::make_word("customer", None),
            Token::Whitespace(Whitespace::Space),
            Token::make_keyword("WHERE"),
            Token::Whitespace(Whitespace::Space),
            Token::make_word("salary", None),
            Token::Whitespace(Whitespace::Space),
            Token::Neq,
            Token::Whitespace(Whitespace::Space),
            Token::SingleQuotedString(String::from("Not Provided")),
        ];

        compare(expected, tokens);
    }

    #[test]
    fn tokenize_invalid_string() {
        let sql = String::from("\nمصطفىh");

        let dialect = GenericSqlDialect {};
        let mut tokenizer = Tokenizer::new(&dialect, &sql);
        let tokens = tokenizer.tokenize().unwrap();
        println!("tokens: {:#?}", tokens);
        let expected = vec![
            Token::Whitespace(Whitespace::Newline),
            Token::Char('م'),
            Token::Char('ص'),
            Token::Char('ط'),
            Token::Char('ف'),
            Token::Char('ى'),
            Token::make_word("h", None),
        ];
        compare(expected, tokens);
    }

    #[test]
    fn tokenize_invalid_string_cols() {
        let sql = String::from("\n\nSELECT * FROM table\tمصطفىh");

        let dialect = GenericSqlDialect {};
        let mut tokenizer = Tokenizer::new(&dialect, &sql);
        let tokens = tokenizer.tokenize().unwrap();
        println!("tokens: {:#?}", tokens);
        let expected = vec![
            Token::Whitespace(Whitespace::Newline),
            Token::Whitespace(Whitespace::Newline),
            Token::make_keyword("SELECT"),
            Token::Whitespace(Whitespace::Space),
            Token::Mult,
            Token::Whitespace(Whitespace::Space),
            Token::make_keyword("FROM"),
            Token::Whitespace(Whitespace::Space),
            Token::make_keyword("table"),
            Token::Whitespace(Whitespace::Tab),
            Token::Char('م'),
            Token::Char('ص'),
            Token::Char('ط'),
            Token::Char('ف'),
            Token::Char('ى'),
            Token::make_word("h", None),
        ];
        compare(expected, tokens);
    }

    #[test]
    fn tokenize_is_null() {
        let sql = String::from("a IS NULL");
        let dialect = GenericSqlDialect {};
        let mut tokenizer = Tokenizer::new(&dialect, &sql);
        let tokens = tokenizer.tokenize().unwrap();

        let expected = vec![
            Token::make_word("a", None),
            Token::Whitespace(Whitespace::Space),
            Token::make_keyword("IS"),
            Token::Whitespace(Whitespace::Space),
            Token::make_keyword("NULL"),
        ];

        compare(expected, tokens);
    }

    #[test]
    fn tokenize_comment() {
        let sql = String::from("0--this is a comment\n1");

        let dialect = GenericSqlDialect {};
        let mut tokenizer = Tokenizer::new(&dialect, &sql);
        let tokens = tokenizer.tokenize().unwrap();
        let expected = vec![
            Token::Number("0".to_string()),
            Token::Whitespace(Whitespace::SingleLineComment(
                "this is a comment\n".to_string(),
            )),
            Token::Number("1".to_string()),
        ];
        compare(expected, tokens);
    }

    #[test]
    fn tokenize_comment_at_eof() {
        let sql = String::from("--this is a comment");

        let dialect = GenericSqlDialect {};
        let mut tokenizer = Tokenizer::new(&dialect, &sql);
        let tokens = tokenizer.tokenize().unwrap();
        let expected = vec![Token::Whitespace(Whitespace::SingleLineComment(
            "this is a comment".to_string(),
        ))];
        compare(expected, tokens);
    }

    #[test]
    fn tokenize_multiline_comment() {
        let sql = String::from("0/*multi-line\n* /comment*/1");

        let dialect = GenericSqlDialect {};
        let mut tokenizer = Tokenizer::new(&dialect, &sql);
        let tokens = tokenizer.tokenize().unwrap();
        let expected = vec![
            Token::Number("0".to_string()),
            Token::Whitespace(Whitespace::MultiLineComment(
                "multi-line\n* /comment".to_string(),
            )),
            Token::Number("1".to_string()),
        ];
        compare(expected, tokens);
    }

    #[test]
    fn tokenize_multiline_comment_with_even_asterisks() {
        let sql = String::from("\n/** Comment **/\n");

        let dialect = GenericSqlDialect {};
        let mut tokenizer = Tokenizer::new(&dialect, &sql);
        let tokens = tokenizer.tokenize().unwrap();
        let expected = vec![
            Token::Whitespace(Whitespace::Newline),
            Token::Whitespace(Whitespace::MultiLineComment("* Comment *".to_string())),
            Token::Whitespace(Whitespace::Newline),
        ];
        compare(expected, tokens);
    }

    #[test]
    fn tokenize_newlines() {
        let sql = String::from("line1\nline2\rline3\r\nline4\r");

        let dialect = GenericSqlDialect {};
        let mut tokenizer = Tokenizer::new(&dialect, &sql);
        let tokens = tokenizer.tokenize().unwrap();
        let expected = vec![
            Token::make_word("line1", None),
            Token::Whitespace(Whitespace::Newline),
            Token::make_word("line2", None),
            Token::Whitespace(Whitespace::Newline),
            Token::make_word("line3", None),
            Token::Whitespace(Whitespace::Newline),
            Token::make_word("line4", None),
            Token::Whitespace(Whitespace::Newline),
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

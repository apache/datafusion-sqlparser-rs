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

use super::dialect::keywords::{Keyword, ALL_KEYWORDS, ALL_KEYWORDS_INDEX};
use super::dialect::Dialect;
use super::dialect::SnowflakeDialect;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use std::fmt;

/// SQL Token enumeration
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum Token {
    /// An end-of-file marker, not a real token
    EOF,
    /// A keyword (like SELECT) or an optionally quoted SQL identifier
    Word(Word),
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
    /// Greater Than operator `>`
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
    /// String concatenation `||`
    StringConcat,
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
    /// Ampersand `&`
    Ampersand,
    /// Pipe `|`
    Pipe,
    /// Caret `^`
    Caret,
    /// Left brace `{`
    LBrace,
    /// Right brace `}`
    RBrace,
    /// Right Arrow `=>`
    RArrow,
    /// Sharp `#` used for PostgreSQL Bitwise XOR operator
    Sharp,
    /// Tilde `~` used for PostgreSQL Bitwise NOT operator
    Tilde,
    /// `<<`, a bitwise shift left operator in PostgreSQL
    ShiftLeft,
    /// `>>`, a bitwise shift right operator in PostgreSQL
    ShiftRight,
    /// Exclamation Mark `!` used for PostgreSQL factorial operator
    ExclamationMark,
    /// Double Exclamation Mark `!!` used for PostgreSQL prefix factorial operator
    DoubleExclamationMark,
    /// AtSign `@` used for PostgreSQL abs operator
    AtSign,
    /// `|/`, a square root math operator in PostgreSQL
    PGSquareRoot,
    /// `||/` , a cube root math operator in PostgreSQL
    PGCubeRoot,
}

impl fmt::Display for Token {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Token::EOF => f.write_str("EOF"),
            Token::Word(ref w) => write!(f, "{}", w),
            Token::Number(ref n) => f.write_str(n),
            Token::Char(ref c) => write!(f, "{}", c),
            Token::SingleQuotedString(ref s) => write!(f, "'{}'", s),
            Token::NationalStringLiteral(ref s) => write!(f, "N'{}'", s),
            Token::HexStringLiteral(ref s) => write!(f, "X'{}'", s),
            Token::Comma => f.write_str(","),
            Token::Whitespace(ws) => write!(f, "{}", ws),
            Token::Eq => f.write_str("="),
            Token::Neq => f.write_str("<>"),
            Token::Lt => f.write_str("<"),
            Token::Gt => f.write_str(">"),
            Token::LtEq => f.write_str("<="),
            Token::GtEq => f.write_str(">="),
            Token::Plus => f.write_str("+"),
            Token::Minus => f.write_str("-"),
            Token::Mult => f.write_str("*"),
            Token::Div => f.write_str("/"),
            Token::StringConcat => f.write_str("||"),
            Token::Mod => f.write_str("%"),
            Token::LParen => f.write_str("("),
            Token::RParen => f.write_str(")"),
            Token::Period => f.write_str("."),
            Token::Colon => f.write_str(":"),
            Token::DoubleColon => f.write_str("::"),
            Token::SemiColon => f.write_str(";"),
            Token::Backslash => f.write_str("\\"),
            Token::LBracket => f.write_str("["),
            Token::RBracket => f.write_str("]"),
            Token::Ampersand => f.write_str("&"),
            Token::Caret => f.write_str("^"),
            Token::Pipe => f.write_str("|"),
            Token::LBrace => f.write_str("{"),
            Token::RBrace => f.write_str("}"),
            Token::RArrow => f.write_str("=>"),
            Token::Sharp => f.write_str("#"),
            Token::ExclamationMark => f.write_str("!"),
            Token::DoubleExclamationMark => f.write_str("!!"),
            Token::Tilde => f.write_str("~"),
            Token::AtSign => f.write_str("@"),
            Token::ShiftLeft => f.write_str("<<"),
            Token::ShiftRight => f.write_str(">>"),
            Token::PGSquareRoot => f.write_str("|/"),
            Token::PGCubeRoot => f.write_str("||/"),
        }
    }
}

impl Token {
    pub fn make_keyword(keyword: &str) -> Self {
        Token::make_word(keyword, None)
    }
    pub fn make_word(word: &str, quote_style: Option<char>) -> Self {
        let word_uppercase = word.to_uppercase();
        Token::Word(Word {
            value: word.to_string(),
            quote_style,
            keyword: if quote_style == None {
                let keyword = ALL_KEYWORDS.binary_search(&word_uppercase.as_str());
                keyword.map_or(Keyword::NoKeyword, |x| ALL_KEYWORDS_INDEX[x])
            } else {
                Keyword::NoKeyword
            },
        })
    }
}

/// A keyword (like SELECT) or an optionally quoted SQL identifier
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Word {
    /// The value of the token, without the enclosing quotes, and with the
    /// escape sequences (if any) processed (TODO: escapes are not handled)
    pub value: String,
    /// An identifier can be "quoted" (&lt;delimited identifier> in ANSI parlance).
    /// The standard and most implementations allow using double quotes for this,
    /// but some implementations support other quoting styles as well (e.g. \[MS SQL])
    pub quote_style: Option<char>,
    /// If the word was not quoted and it matched one of the known keywords,
    /// this will have one of the values from dialect::keywords, otherwise empty
    pub keyword: Keyword,
}

impl fmt::Display for Word {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.quote_style {
            Some(s) if s == '"' || s == '[' || s == '`' => {
                write!(f, "{}{}{}", s, self.value, Word::matching_end_quote(s))
            }
            None => f.write_str(&self.value),
            _ => panic!("Unexpected quote_style!"),
        }
    }
}
impl Word {
    fn matching_end_quote(ch: char) -> char {
        match ch {
            '"' => '"', // ANSI and most dialects
            '[' => ']', // MS SQL
            '`' => '`', // MySQL
            _ => panic!("unexpected quoting style!"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum Whitespace {
    Space,
    Newline,
    Tab,
    SingleLineComment { comment: String, prefix: String },
    MultiLineComment(String),
}

impl fmt::Display for Whitespace {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Whitespace::Space => f.write_str(" "),
            Whitespace::Newline => f.write_str("\n"),
            Whitespace::Tab => f.write_str("\t"),
            Whitespace::SingleLineComment { prefix, comment } => write!(f, "{}{}", prefix, comment),
            Whitespace::MultiLineComment(s) => write!(f, "/*{}*/", s),
        }
    }
}

/// Location in input string
#[derive(Debug, PartialEq, Clone)]
pub struct Location {
    /// Line number, starting from 1
    pub line: u64,
    /// Line column, starting from 1
    pub column: u64,
}

/// A [Token] with [Location] attached to it
#[derive(Debug, PartialEq, Clone)]
pub struct TokenWithLocation {
    pub token: Token,
    pub location: Location,
}

impl TokenWithLocation {
    pub fn new(token: Token, line: u64, column: u64) -> TokenWithLocation {
        TokenWithLocation {
            token,
            location: Location { line, column },
        }
    }
}

/// Tokenizer error
#[derive(Debug, PartialEq)]
pub struct TokenizerError {
    pub message: String,
    pub line: u64,
    pub col: u64,
}

struct State<'a> {
    peekable: Peekable<Chars<'a>>,
    pub line: u64,
    pub col: u64,
}

impl<'a> State<'a> {
    pub fn next(&mut self) -> Option<char> {
        match self.peekable.next() {
            None => None,
            Some(s) => {
                if s == '\n' {
                    self.line += 1;
                    self.col = 1;
                } else {
                    self.col += 1;
                }
                Some(s)
            }
        }
    }

    pub fn peek(&mut self) -> Option<&char> {
        self.peekable.peek()
    }

    pub fn location(&self) -> Location {
        Location {
            line: self.line,
            column: self.col,
        }
    }
}

/// SQL Tokenizer
pub struct Tokenizer<'a> {
    dialect: &'a dyn Dialect,
    pub query: &'a str,
}

impl<'a> Tokenizer<'a> {
    /// Create a new SQL tokenizer for the specified SQL statement
    pub fn new(dialect: &'a dyn Dialect, query: &'a str) -> Self {
        Self { dialect, query }
    }

    /// Tokenize the statement and produce a vector of tokens
    pub fn tokenize(&mut self) -> Result<Vec<Token>, TokenizerError> {
        let twl = self.tokenize_with_location()?;
        let mut tokens: Vec<Token> = vec![];

        tokens.reserve(twl.len());
        for token_with_location in twl {
            tokens.push(token_with_location.token);
        }
        Ok(tokens)
    }

    /// Tokenize the statement and produce a vector of tokens
    pub fn tokenize_with_location(&mut self) -> Result<Vec<TokenWithLocation>, TokenizerError> {
        let mut state = State {
            peekable: self.query.chars().peekable(),
            line: 1,
            col: 1,
        };

        let mut tokens: Vec<TokenWithLocation> = vec![];

        let mut location = state.location();
        while let Some(token) = self.next_token(&mut state)? {
            tokens.push(TokenWithLocation {
                token,
                location: location.clone(),
            });

            location = state.location();
        }
        Ok(tokens)
    }

    /// Get the next token or return None
    fn next_token(&self, state: &mut State) -> Result<Option<Token>, TokenizerError> {
        //println!("next_token: {:?}", chars.peek());
        match state.peek() {
            Some(&ch) => match ch {
                ' ' => self.consume_and_return(state, Token::Whitespace(Whitespace::Space)),
                '\t' => self.consume_and_return(state, Token::Whitespace(Whitespace::Tab)),
                '\n' => self.consume_and_return(state, Token::Whitespace(Whitespace::Newline)),
                '\r' => {
                    // Emit a single Whitespace::Newline token for \r and \r\n
                    state.next();
                    if let Some('\n') = state.peek() {
                        state.next();
                    }
                    Ok(Some(Token::Whitespace(Whitespace::Newline)))
                }
                'N' => {
                    state.next(); // consume, to check the next char
                    match state.peek() {
                        Some('\'') => {
                            // N'...' - a <national character string literal>
                            let s = self.tokenize_single_quoted_string(state)?;
                            Ok(Some(Token::NationalStringLiteral(s)))
                        }
                        _ => {
                            // regular identifier starting with an "N"
                            let s = self.tokenize_word('N', state);
                            Ok(Some(Token::make_word(&s, None)))
                        }
                    }
                }
                // The spec only allows an uppercase 'X' to introduce a hex
                // string, but PostgreSQL, at least, allows a lowercase 'x' too.
                x @ 'x' | x @ 'X' => {
                    state.next(); // consume, to check the next char
                    match state.peek() {
                        Some('\'') => {
                            // X'...' - a <binary string literal>
                            let s = self.tokenize_single_quoted_string(state)?;
                            Ok(Some(Token::HexStringLiteral(s)))
                        }
                        _ => {
                            // regular identifier starting with an "X"
                            let s = self.tokenize_word(x, state);
                            Ok(Some(Token::make_word(&s, None)))
                        }
                    }
                }
                // identifier or keyword
                ch if self.dialect.is_identifier_start(ch) => {
                    state.next(); // consume the first char
                    let s = self.tokenize_word(ch, state);
                    Ok(Some(Token::make_word(&s, None)))
                }
                // string
                '\'' => {
                    let s = self.tokenize_single_quoted_string(state)?;
                    Ok(Some(Token::SingleQuotedString(s)))
                }
                // delimited (quoted) identifier
                quote_start if self.dialect.is_delimited_identifier_start(quote_start) => {
                    state.next(); // consume the opening quote
                    let quote_end = Word::matching_end_quote(quote_start);
                    let s = peeking_take_while(state, |ch| ch != quote_end);
                    if state.next() == Some(quote_end) {
                        Ok(Some(Token::make_word(&s, Some(quote_start))))
                    } else {
                        self.tokenizer_error(
                            state,
                            format!("Expected close delimiter '{}' before EOF.", quote_end)
                                .as_str(),
                        )
                    }
                }
                // numbers
                '0'..='9' => {
                    // TODO: https://jakewheat.github.io/sql-overview/sql-2011-foundation-grammar.html#unsigned-numeric-literal
                    let s = peeking_take_while(state, |ch| matches!(ch, '0'..='9' | '.'));
                    Ok(Some(Token::Number(s)))
                }
                // punctuation
                '(' => self.consume_and_return(state, Token::LParen),
                ')' => self.consume_and_return(state, Token::RParen),
                ',' => self.consume_and_return(state, Token::Comma),
                // operators
                '-' => {
                    state.next(); // consume the '-'
                    match state.peek() {
                        Some('-') => {
                            state.next(); // consume the second '-', starting a single-line comment
                            let comment = self.tokenize_single_line_comment(state);
                            Ok(Some(Token::Whitespace(Whitespace::SingleLineComment {
                                prefix: "--".to_owned(),
                                comment,
                            })))
                        }
                        // a regular '-' operator
                        _ => Ok(Some(Token::Minus)),
                    }
                }
                '/' => {
                    state.next(); // consume the '/'
                    match state.peek() {
                        Some('*') => {
                            state.next(); // consume the '*', starting a multi-line comment
                            self.tokenize_multiline_comment(state)
                        }
                        Some('/') if dialect_of!(self is SnowflakeDialect) => {
                            state.next(); // consume the second '/', starting a snowflake single-line comment
                            let comment = self.tokenize_single_line_comment(state);
                            Ok(Some(Token::Whitespace(Whitespace::SingleLineComment {
                                prefix: "//".to_owned(),
                                comment,
                            })))
                        }
                        // a regular '/' operator
                        _ => Ok(Some(Token::Div)),
                    }
                }
                '+' => self.consume_and_return(state, Token::Plus),
                '*' => self.consume_and_return(state, Token::Mult),
                '%' => self.consume_and_return(state, Token::Mod),
                '|' => {
                    state.next(); // consume the '|'
                    match state.peek() {
                        Some('/') => self.consume_and_return(state, Token::PGSquareRoot),
                        Some('|') => {
                            state.next(); // consume the second '|'
                            match state.peek() {
                                Some('/') => self.consume_and_return(state, Token::PGCubeRoot),
                                _ => Ok(Some(Token::StringConcat)),
                            }
                        }
                        // Bitshift '|' operator
                        _ => Ok(Some(Token::Pipe)),
                    }
                }
                '=' => {
                    state.next(); // consume
                    match state.peek() {
                        Some('>') => self.consume_and_return(state, Token::RArrow),
                        _ => Ok(Some(Token::Eq)),
                    }
                }
                '.' => self.consume_and_return(state, Token::Period),
                '!' => {
                    state.next(); // consume
                    match state.peek() {
                        Some('=') => self.consume_and_return(state, Token::Neq),
                        Some('!') => self.consume_and_return(state, Token::DoubleExclamationMark),
                        _ => Ok(Some(Token::ExclamationMark)),
                    }
                }
                '<' => {
                    state.next(); // consume
                    match state.peek() {
                        Some('=') => self.consume_and_return(state, Token::LtEq),
                        Some('>') => self.consume_and_return(state, Token::Neq),
                        Some('<') => self.consume_and_return(state, Token::ShiftLeft),
                        _ => Ok(Some(Token::Lt)),
                    }
                }
                '>' => {
                    state.next(); // consume
                    match state.peek() {
                        Some('=') => self.consume_and_return(state, Token::GtEq),
                        Some('>') => self.consume_and_return(state, Token::ShiftRight),
                        _ => Ok(Some(Token::Gt)),
                    }
                }
                ':' => {
                    state.next();
                    match state.peek() {
                        Some(':') => self.consume_and_return(state, Token::DoubleColon),
                        _ => Ok(Some(Token::Colon)),
                    }
                }
                ';' => self.consume_and_return(state, Token::SemiColon),
                '\\' => self.consume_and_return(state, Token::Backslash),
                '[' => self.consume_and_return(state, Token::LBracket),
                ']' => self.consume_and_return(state, Token::RBracket),
                '&' => self.consume_and_return(state, Token::Ampersand),
                '^' => self.consume_and_return(state, Token::Caret),
                '{' => self.consume_and_return(state, Token::LBrace),
                '}' => self.consume_and_return(state, Token::RBrace),
                '#' if dialect_of!(self is SnowflakeDialect) => {
                    state.next(); // consume the '#', starting a snowflake single-line comment
                    let comment = self.tokenize_single_line_comment(state);
                    Ok(Some(Token::Whitespace(Whitespace::SingleLineComment {
                        prefix: "#".to_owned(),
                        comment,
                    })))
                }
                '~' => self.consume_and_return(state, Token::Tilde),
                '#' => self.consume_and_return(state, Token::Sharp),
                '@' => self.consume_and_return(state, Token::AtSign),
                other => self.consume_and_return(state, Token::Char(other)),
            },
            None => Ok(None),
        }
    }

    fn tokenizer_error<R>(&self, state: &State, message: &str) -> Result<R, TokenizerError> {
        let loc = state.location();
        Err(TokenizerError {
            message: message.to_string(),
            col: loc.column,
            line: loc.line,
        })
    }

    // Consume characters until newline
    fn tokenize_single_line_comment(&self, state: &mut State) -> String {
        let mut comment = peeking_take_while(state, |ch| ch != '\n');
        if let Some(ch) = state.next() {
            assert_eq!(ch, '\n');
            comment.push(ch);
        }
        comment
    }

    /// Tokenize an identifier or keyword, after the first char is already consumed.
    fn tokenize_word(&self, first_char: char, state: &mut State) -> String {
        let mut s = first_char.to_string();
        s.push_str(&peeking_take_while(state, |ch| {
            self.dialect.is_identifier_part(ch)
        }));
        s
    }

    /// Read a single quoted string, starting with the opening quote.
    fn tokenize_single_quoted_string(&self, state: &mut State) -> Result<String, TokenizerError> {
        let mut s = String::new();
        state.next(); // consume the opening quote
        while let Some(&ch) = state.peek() {
            match ch {
                '\'' => {
                    state.next(); // consume
                    let escaped_quote = state.peek().map(|c| *c == '\'').unwrap_or(false);
                    if escaped_quote {
                        s.push('\'');
                        state.next();
                    } else {
                        return Ok(s);
                    }
                }
                _ => {
                    state.next(); // consume
                    s.push(ch);
                }
            }
        }
        self.tokenizer_error(state, "Unterminated string literal")
    }

    fn tokenize_multiline_comment(
        &self,
        state: &mut State,
    ) -> Result<Option<Token>, TokenizerError> {
        let mut s = String::new();
        let mut maybe_closing_comment = false;
        // TODO: deal with nested comments
        loop {
            match state.next() {
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
                    break self
                        .tokenizer_error(state, "Unexpected EOF while in a multi-line comment")
                }
            }
        }
    }

    fn consume_and_return(
        &self,
        state: &mut State,
        t: Token,
    ) -> Result<Option<Token>, TokenizerError> {
        state.next();
        Ok(Some(t))
    }
}

/// Read from `chars` until `predicate` returns `false` or EOF is hit.
/// Return the characters read as String, and keep the first non-matching
/// char available as `chars.next()`.
fn peeking_take_while(state: &mut State, mut predicate: impl FnMut(char) -> bool) -> String {
    let mut s = String::new();
    while let Some(&ch) = state.peek() {
        if predicate(ch) {
            state.next(); // consume
            s.push(ch);
        } else {
            break;
        }
    }
    s
}

#[cfg(test)]
mod tests {
    use super::super::dialect::GenericDialect;
    use super::super::dialect::MsSqlDialect;
    use super::*;

    #[test]
    fn tokenize_select_1() {
        let sql = String::from("SELECT 1");
        let dialect = GenericDialect {};
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
        let dialect = GenericDialect {};
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
    fn tokenize_string_string_concat() {
        let sql = String::from("SELECT 'a' || 'b'");
        let dialect = GenericDialect {};
        let mut tokenizer = Tokenizer::new(&dialect, &sql);
        let tokens = tokenizer.tokenize().unwrap();

        let expected = vec![
            Token::make_keyword("SELECT"),
            Token::Whitespace(Whitespace::Space),
            Token::SingleQuotedString(String::from("a")),
            Token::Whitespace(Whitespace::Space),
            Token::StringConcat,
            Token::Whitespace(Whitespace::Space),
            Token::SingleQuotedString(String::from("b")),
        ];

        compare(expected, tokens);
    }
    #[test]
    fn tokenize_bitwise_op() {
        let sql = String::from("SELECT one | two ^ three");
        let dialect = GenericDialect {};
        let mut tokenizer = Tokenizer::new(&dialect, &sql);
        let tokens = tokenizer.tokenize().unwrap();

        let expected = vec![
            Token::make_keyword("SELECT"),
            Token::Whitespace(Whitespace::Space),
            Token::make_word("one", None),
            Token::Whitespace(Whitespace::Space),
            Token::Pipe,
            Token::Whitespace(Whitespace::Space),
            Token::make_word("two", None),
            Token::Whitespace(Whitespace::Space),
            Token::Caret,
            Token::Whitespace(Whitespace::Space),
            Token::make_word("three", None),
        ];

        compare(expected, tokens);
    }

    #[test]
    fn tokenize_simple_select() {
        let sql = String::from("SELECT * FROM customer WHERE id = 1 LIMIT 5");
        let dialect = GenericDialect {};
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
    fn tokenize_explain_select() {
        let sql = String::from("EXPLAIN SELECT * FROM customer WHERE id = 1");
        let dialect = GenericDialect {};
        let mut tokenizer = Tokenizer::new(&dialect, &sql);
        let tokens = tokenizer.tokenize().unwrap();

        let expected = vec![
            Token::make_keyword("EXPLAIN"),
            Token::Whitespace(Whitespace::Space),
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
        ];

        compare(expected, tokens);
    }

    #[test]
    fn tokenize_explain_analyze_select() {
        let sql = String::from("EXPLAIN ANALYZE SELECT * FROM customer WHERE id = 1");
        let dialect = GenericDialect {};
        let mut tokenizer = Tokenizer::new(&dialect, &sql);
        let tokens = tokenizer.tokenize().unwrap();

        let expected = vec![
            Token::make_keyword("EXPLAIN"),
            Token::Whitespace(Whitespace::Space),
            Token::make_keyword("ANALYZE"),
            Token::Whitespace(Whitespace::Space),
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
        ];

        compare(expected, tokens);
    }

    #[test]
    fn tokenize_string_predicate() {
        let sql = String::from("SELECT * FROM customer WHERE salary != 'Not Provided'");
        let dialect = GenericDialect {};
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

        let dialect = GenericDialect {};
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
    fn tokenize_newline_in_string_literal() {
        let sql = String::from("'foo\r\nbar\nbaz'");

        let dialect = GenericDialect {};
        let mut tokenizer = Tokenizer::new(&dialect, &sql);
        let tokens = tokenizer.tokenize().unwrap();
        let expected = vec![Token::SingleQuotedString("foo\r\nbar\nbaz".to_string())];
        compare(expected, tokens);
    }

    #[test]
    fn tokenize_unterminated_string_literal() {
        let sql = String::from("select 'foo");

        let dialect = GenericDialect {};
        let mut tokenizer = Tokenizer::new(&dialect, &sql);
        assert_eq!(
            tokenizer.tokenize(),
            Err(TokenizerError {
                message: "Unterminated string literal".to_string(),
                line: 1,
                col: 12
            })
        );
    }

    #[test]
    fn tokenize_invalid_string_cols() {
        let sql = String::from("\n\nSELECT * FROM table\tمصطفىh");

        let dialect = GenericDialect {};
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
    fn tokenize_right_arrow() {
        let sql = String::from("FUNCTION(key=>value)");
        let dialect = GenericDialect {};
        let mut tokenizer = Tokenizer::new(&dialect, &sql);
        let tokens = tokenizer.tokenize().unwrap();
        let expected = vec![
            Token::make_word("FUNCTION", None),
            Token::LParen,
            Token::make_word("key", None),
            Token::RArrow,
            Token::make_word("value", None),
            Token::RParen,
        ];
        compare(expected, tokens);
    }

    #[test]
    fn tokenize_is_null() {
        let sql = String::from("a IS NULL");
        let dialect = GenericDialect {};
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

        let dialect = GenericDialect {};
        let mut tokenizer = Tokenizer::new(&dialect, &sql);
        let tokens = tokenizer.tokenize().unwrap();
        let expected = vec![
            Token::Number("0".to_string()),
            Token::Whitespace(Whitespace::SingleLineComment {
                prefix: "--".to_string(),
                comment: "this is a comment\n".to_string(),
            }),
            Token::Number("1".to_string()),
        ];
        compare(expected, tokens);
    }

    #[test]
    fn tokenize_comment_at_eof() {
        let sql = String::from("--this is a comment");

        let dialect = GenericDialect {};
        let mut tokenizer = Tokenizer::new(&dialect, &sql);
        let tokens = tokenizer.tokenize().unwrap();
        let expected = vec![Token::Whitespace(Whitespace::SingleLineComment {
            prefix: "--".to_string(),
            comment: "this is a comment".to_string(),
        })];
        compare(expected, tokens);
    }

    #[test]
    fn tokenize_multiline_comment() {
        let sql = String::from("0/*multi-line\n* /comment*/1");

        let dialect = GenericDialect {};
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

        let dialect = GenericDialect {};
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
    fn tokenize_mismatched_quotes() {
        let sql = String::from("\"foo");

        let dialect = GenericDialect {};
        let mut tokenizer = Tokenizer::new(&dialect, &sql);
        assert_eq!(
            tokenizer.tokenize(),
            Err(TokenizerError {
                message: "Expected close delimiter '\"' before EOF.".to_string(),
                line: 1,
                col: 5,
            })
        );
    }

    #[test]
    fn tokenize_newlines() {
        let sql = String::from("line1\nline2\rline3\r\nline4\r");

        let dialect = GenericDialect {};
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

    #[test]
    fn tokenize_mssql_top() {
        let sql = "SELECT TOP 5 [bar] FROM foo";
        let dialect = MsSqlDialect {};
        let mut tokenizer = Tokenizer::new(&dialect, sql);
        let tokens = tokenizer.tokenize().unwrap();
        let expected = vec![
            Token::make_keyword("SELECT"),
            Token::Whitespace(Whitespace::Space),
            Token::make_keyword("TOP"),
            Token::Whitespace(Whitespace::Space),
            Token::Number(String::from("5")),
            Token::Whitespace(Whitespace::Space),
            Token::make_word("bar", Some('[')),
            Token::Whitespace(Whitespace::Space),
            Token::make_keyword("FROM"),
            Token::Whitespace(Whitespace::Space),
            Token::make_word("foo", None),
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

    fn compare_with_location(expected: Vec<TokenWithLocation>, actual: Vec<TokenWithLocation>) {
        //println!("------------------------------");
        //println!("tokens   = {:?}", actual);
        //println!("expected = {:?}", expected);
        //println!("------------------------------");
        assert_eq!(expected, actual);
    }

    #[test]
    fn tokenize_location_multiline_string_literal() {
        let sql = String::from("'some\nthing' foo");

        let dialect = GenericDialect {};
        let mut tokenizer = Tokenizer::new(&dialect, &sql);
        let tokens = tokenizer.tokenize_with_location().unwrap();
        let expected = vec![
            TokenWithLocation::new(Token::SingleQuotedString("some\nthing".to_string()), 1, 1),
            TokenWithLocation::new(Token::Whitespace(Whitespace::Space), 2, 7),
            TokenWithLocation::new(Token::make_word("foo", None), 2, 8),
        ];
        compare_with_location(expected, tokens);
    }
}

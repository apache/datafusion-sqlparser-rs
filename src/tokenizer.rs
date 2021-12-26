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

#[cfg(not(feature = "std"))]
use alloc::{
    borrow::ToOwned,
    format,
    string::{String, ToString},
    vec,
    vec::Vec,
};

use core::fmt;
use core::iter::Peekable;
use core::str::CharIndices;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use crate::dialect::SnowflakeDialect;
use crate::keywords::{Keyword, ALL_KEYWORDS, ALL_KEYWORDS_INDEX};
use crate::{dialect::Dialect, dialect::MySqlDialect, span::Spanned};

use crate::span::Span;

/// SQL Token enumeration
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum Token {
    /// An end-of-file marker, not a real token
    EOF { span: Span },
    /// A keyword (like SELECT) or an optionally quoted SQL identifier
    Word { value: Word, span: Span },
    /// An unsigned numeric literal
    Number {
        value: String,
        long: bool,
        span: Span,
    },
    /// A character that could not be tokenized
    Char { value: char, span: Span },
    /// Single quoted string: i.e: 'string'
    SingleQuotedString { value: String, span: Span },
    /// "National" string literal: i.e: N'string'
    NationalStringLiteral { value: String, span: Span },
    /// Hexadecimal string literal: i.e.: X'deadbeef'
    HexStringLiteral { value: String, span: Span },
    /// Comma
    Comma { span: Span },
    /// Whitespace (space, tab, etc)
    Whitespace { value: Whitespace, span: Span },
    /// Double equals sign `==`
    DoubleEq { span: Span },
    /// Equality operator `=`
    Eq { span: Span },
    /// Not Equals operator `<>` (or `!=` in some dialects)
    Neq { span: Span },
    /// Less Than operator `<`
    Lt { span: Span },
    /// Greater Than operator `>`
    Gt { span: Span },
    /// Less Than Or Equals operator `<=`
    LtEq { span: Span },
    /// Greater Than Or Equals operator `>=`
    GtEq { span: Span },
    /// Spaceship operator <=>
    Spaceship { span: Span },
    /// Plus operator `+`
    Plus { span: Span },
    /// Minus operator `-`
    Minus { span: Span },
    /// Multiplication operator `*`
    Mul { span: Span },
    /// Division operator `/`
    Div { span: Span },
    /// Modulo Operator `%`
    Mod { span: Span },
    /// String concatenation `||`
    StringConcat { span: Span },
    /// Left parenthesis `(`
    LParen { span: Span },
    /// Right parenthesis `)`
    RParen { span: Span },
    /// Period (used for compound identifiers or projections into nested types)
    Period { span: Span },
    /// Colon `:`
    Colon { span: Span },
    /// DoubleColon `::` (used for casting in postgresql)
    DoubleColon { span: Span },
    /// SemiColon `;` used as separator for COPY and payload
    SemiColon { span: Span },
    /// Backslash `\` used in terminating the COPY payload with `\.`
    Backslash { span: Span },
    /// Left bracket `[`
    LBracket { span: Span },
    /// Right bracket `]`
    RBracket { span: Span },
    /// Ampersand `&`
    Ampersand { span: Span },
    /// Pipe `|`
    Pipe { span: Span },
    /// Caret `^`
    Caret { span: Span },
    /// Left brace `{`
    LBrace { span: Span },
    /// Right brace `}`
    RBrace { span: Span },
    /// Right Arrow `=>`
    RArrow { span: Span },
    /// Sharp `#` used for PostgreSQL Bitwise XOR operator
    Sharp { span: Span },
    /// Tilde `~` used for PostgreSQL Bitwise NOT operator or case sensitive match regular expression operator
    Tilde { span: Span },
    /// `~*` , a case insensitive match regular expression operator in PostgreSQL
    TildeAsterisk { span: Span },
    /// `!~` , a case sensitive not match regular expression operator in PostgreSQL
    ExclamationMarkTilde { span: Span },
    /// `!~*` , a case insensitive not match regular expression operator in PostgreSQL
    ExclamationMarkTildeAsterisk { span: Span },
    /// `<<`, a bitwise shift left operator in PostgreSQL
    ShiftLeft { span: Span },
    /// `>>`, a bitwise shift right operator in PostgreSQL
    ShiftRight { span: Span },
    /// Exclamation Mark `!` used for PostgreSQL factorial operator
    ExclamationMark { span: Span },
    /// Double Exclamation Mark `!!` used for PostgreSQL prefix factorial operator
    DoubleExclamationMark { span: Span },
    /// AtSign `@` used for PostgreSQL abs operator
    AtSign { span: Span },
    /// `|/`, a square root math operator in PostgreSQL
    PGSquareRoot { span: Span },
    /// `||/` , a cube root math operator in PostgreSQL
    PGCubeRoot { span: Span },
}

impl Spanned for Token {
    fn span(&self) -> Span {
        match self {
            Token::EOF { span, .. } => *span,
            Token::Word { span, .. } => *span,
            Token::Number { span, .. } => *span,
            Token::Char { span, .. } => *span,
            Token::SingleQuotedString { span, .. } => *span,
            Token::NationalStringLiteral { span, .. } => *span,
            Token::HexStringLiteral { span, .. } => *span,
            Token::Comma { span, .. } => *span,
            Token::Whitespace { span, .. } => *span,
            Token::DoubleEq { span, .. } => *span,
            Token::Eq { span, .. } => *span,
            Token::Neq { span, .. } => *span,
            Token::Lt { span, .. } => *span,
            Token::Gt { span, .. } => *span,
            Token::LtEq { span, .. } => *span,
            Token::GtEq { span, .. } => *span,
            Token::Spaceship { span, .. } => *span,
            Token::Plus { span, .. } => *span,
            Token::Minus { span, .. } => *span,
            Token::Mul { span, .. } => *span,
            Token::Div { span, .. } => *span,
            Token::Mod { span, .. } => *span,
            Token::StringConcat { span, .. } => *span,
            Token::LParen { span, .. } => *span,
            Token::RParen { span, .. } => *span,
            Token::Period { span, .. } => *span,
            Token::Colon { span, .. } => *span,
            Token::DoubleColon { span, .. } => *span,
            Token::SemiColon { span, .. } => *span,
            Token::Backslash { span, .. } => *span,
            Token::LBracket { span, .. } => *span,
            Token::RBracket { span, .. } => *span,
            Token::Ampersand { span, .. } => *span,
            Token::Pipe { span, .. } => *span,
            Token::Caret { span, .. } => *span,
            Token::LBrace { span, .. } => *span,
            Token::RBrace { span, .. } => *span,
            Token::RArrow { span, .. } => *span,
            Token::Sharp { span, .. } => *span,
            Token::Tilde { span, .. } => *span,
            Token::TildeAsterisk { span, .. } => *span,
            Token::ExclamationMarkTilde { span, .. } => *span,
            Token::ExclamationMarkTildeAsterisk { span, .. } => *span,
            Token::ShiftLeft { span, .. } => *span,
            Token::ShiftRight { span, .. } => *span,
            Token::ExclamationMark { span, .. } => *span,
            Token::DoubleExclamationMark { span, .. } => *span,
            Token::AtSign { span, .. } => *span,
            Token::PGSquareRoot { span, .. } => *span,
            Token::PGCubeRoot { span, .. } => *span,
        }
    }
}

impl fmt::Display for Token {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Token::EOF { .. } => f.write_str("EOF"),
            Token::Word { ref value, .. } => write!(f, "{}", value),
            Token::Number {
                ref value,
                ref long,
                ..
            } => write!(f, "{}{long}", value, long = if *long { "L" } else { "" }),
            Token::Char { ref value, .. } => write!(f, "{}", value),
            Token::SingleQuotedString { ref value, .. } => write!(f, "'{}'", value),
            Token::NationalStringLiteral { ref value, .. } => write!(f, "N'{}'", value),
            Token::HexStringLiteral { ref value, .. } => write!(f, "X'{}'", value),
            Token::Comma { .. } => f.write_str(","),
            Token::Whitespace { value, .. } => write!(f, "{}", value),
            Token::DoubleEq { .. } => f.write_str("=="),
            Token::Spaceship { .. } => f.write_str("<=>"),
            Token::Eq { .. } => f.write_str("="),
            Token::Neq { .. } => f.write_str("<>"),
            Token::Lt { .. } => f.write_str("<"),
            Token::Gt { .. } => f.write_str(">"),
            Token::LtEq { .. } => f.write_str("<="),
            Token::GtEq { .. } => f.write_str(">="),
            Token::Plus { .. } => f.write_str("+"),
            Token::Minus { .. } => f.write_str("-"),
            Token::Mul { .. } => f.write_str("*"),
            Token::Div { .. } => f.write_str("/"),
            Token::StringConcat { .. } => f.write_str("||"),
            Token::Mod { .. } => f.write_str("%"),
            Token::LParen { .. } => f.write_str("("),
            Token::RParen { .. } => f.write_str(")"),
            Token::Period { .. } => f.write_str("."),
            Token::Colon { .. } => f.write_str(":"),
            Token::DoubleColon { .. } => f.write_str("::"),
            Token::SemiColon { .. } => f.write_str(";"),
            Token::Backslash { .. } => f.write_str("\\"),
            Token::LBracket { .. } => f.write_str("["),
            Token::RBracket { .. } => f.write_str("]"),
            Token::Ampersand { .. } => f.write_str("&"),
            Token::Caret { .. } => f.write_str("^"),
            Token::Pipe { .. } => f.write_str("|"),
            Token::LBrace { .. } => f.write_str("{"),
            Token::RBrace { .. } => f.write_str("}"),
            Token::RArrow { .. } => f.write_str("=>"),
            Token::Sharp { .. } => f.write_str("#"),
            Token::ExclamationMark { .. } => f.write_str("!"),
            Token::DoubleExclamationMark { .. } => f.write_str("!!"),
            Token::Tilde { .. } => f.write_str("~"),
            Token::TildeAsterisk { .. } => f.write_str("~*"),
            Token::ExclamationMarkTilde { .. } => f.write_str("!~"),
            Token::ExclamationMarkTildeAsterisk { .. } => f.write_str("!~*"),
            Token::AtSign { .. } => f.write_str("@"),
            Token::ShiftLeft { .. } => f.write_str("<<"),
            Token::ShiftRight { .. } => f.write_str(">>"),
            Token::PGSquareRoot { .. } => f.write_str("|/"),
            Token::PGCubeRoot { .. } => f.write_str("||/"),
        }
    }
}

impl Token {
    pub fn make_keyword(keyword: &str) -> Self {
        Token::make_word(keyword, None)
    }

    pub fn make_word(word: &str, quote_style: Option<char>) -> Self {
        let word_uppercase = word.to_uppercase();
        Token::Word {
            value: Word {
                value: word.to_string(),
                quote_style,
                keyword: if quote_style == None {
                    let keyword = ALL_KEYWORDS.binary_search(&word_uppercase.as_str());
                    keyword.map_or(Keyword::NoKeyword, |x| ALL_KEYWORDS_INDEX[x])
                } else {
                    Keyword::NoKeyword
                },
            },
            span: Default::default(),
        }
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

/// Tokenizer error
#[derive(Debug, PartialEq)]
pub struct TokenizerError {
    pub message: String,
    pub line: u64,
    pub col: u64,
    pub span: Span,
}

impl fmt::Display for TokenizerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} at Line: {}, Column {}",
            self.message, self.line, self.col
        )
    }
}

#[cfg(feature = "std")]
impl std::error::Error for TokenizerError {}

/// SQL Tokenizer
pub struct Tokenizer<'a> {
    dialect: &'a dyn Dialect,
    query: &'a str,
    line: u64,
    col: u64,
    start: usize,
}

impl<'a> Tokenizer<'a> {
    /// Create a new SQL tokenizer for the specified SQL statement
    pub fn new(dialect: &'a dyn Dialect, query: &'a str) -> Self {
        Self {
            dialect,
            query,
            line: 1,
            col: 1,
            start: 0,
        }
    }

    /// Tokenize the statement and produce a vector of tokens
    pub fn tokenize(&mut self) -> Result<Vec<Token>, TokenizerError> {
        let mut peekable = self.query.char_indices().peekable();

        let mut tokens = vec![];

        self.start = peekable
            .peek()
            .map(|(start, _)| *start)
            .unwrap_or(self.query.len());
        loop {
            let mut token = match self.next_token(&mut peekable)? {
                None => break,
                Some(token) => token,
            };
            let end = peekable
                .peek()
                .map(|(start, _)| *start)
                .unwrap_or(self.query.len());
            let s: Span = (self.start..end).into();
            match &mut token {
                Token::EOF { span } => {
                    *span = s;
                }
                Token::Word { span, .. } => {
                    *span = s;
                }
                Token::Number { span, .. } => {
                    *span = s;
                }
                Token::Char { span, .. } => {
                    *span = s;
                }
                Token::SingleQuotedString { span, .. } => {
                    *span = s;
                }
                Token::NationalStringLiteral { span, .. } => {
                    *span = s;
                }
                Token::HexStringLiteral { span, .. } => {
                    *span = s;
                }
                Token::Comma { span } => {
                    *span = s;
                }
                Token::Whitespace { span, .. } => {
                    *span = s;
                }
                Token::DoubleEq { span } => {
                    *span = s;
                }
                Token::Eq { span } => {
                    *span = s;
                }
                Token::Neq { span } => {
                    *span = s;
                }
                Token::Lt { span } => {
                    *span = s;
                }
                Token::Gt { span } => {
                    *span = s;
                }
                Token::LtEq { span } => {
                    *span = s;
                }
                Token::GtEq { span } => {
                    *span = s;
                }
                Token::Spaceship { span } => {
                    *span = s;
                }
                Token::Plus { span } => {
                    *span = s;
                }
                Token::Minus { span } => {
                    *span = s;
                }
                Token::Mul { span } => {
                    *span = s;
                }
                Token::Div { span } => {
                    *span = s;
                }
                Token::Mod { span } => {
                    *span = s;
                }
                Token::StringConcat { span } => {
                    *span = s;
                }
                Token::LParen { span } => {
                    *span = s;
                }
                Token::RParen { span } => {
                    *span = s;
                }
                Token::Period { span } => {
                    *span = s;
                }
                Token::Colon { span } => {
                    *span = s;
                }
                Token::DoubleColon { span } => {
                    *span = s;
                }
                Token::SemiColon { span } => {
                    *span = s;
                }
                Token::Backslash { span } => {
                    *span = s;
                }
                Token::LBracket { span } => {
                    *span = s;
                }
                Token::RBracket { span } => {
                    *span = s;
                }
                Token::Ampersand { span } => {
                    *span = s;
                }
                Token::Pipe { span } => {
                    *span = s;
                }
                Token::Caret { span } => {
                    *span = s;
                }
                Token::LBrace { span } => {
                    *span = s;
                }
                Token::RBrace { span } => {
                    *span = s;
                }
                Token::RArrow { span } => {
                    *span = s;
                }
                Token::Sharp { span } => {
                    *span = s;
                }
                Token::Tilde { span } => {
                    *span = s;
                }
                Token::TildeAsterisk { span } => {
                    *span = s;
                }
                Token::ExclamationMarkTilde { span } => {
                    *span = s;
                }
                Token::ExclamationMarkTildeAsterisk { span } => {
                    *span = s;
                }
                Token::ShiftLeft { span } => {
                    *span = s;
                }
                Token::ShiftRight { span } => {
                    *span = s;
                }
                Token::ExclamationMark { span } => {
                    *span = s;
                }
                Token::DoubleExclamationMark { span } => {
                    *span = s;
                }
                Token::AtSign { span } => {
                    *span = s;
                }
                Token::PGSquareRoot { span } => {
                    *span = s;
                }
                Token::PGCubeRoot { span } => {
                    *span = s;
                }
            }
            match &token {
                Token::Whitespace {
                    value: Whitespace::Newline,
                    ..
                } => {
                    self.line += 1;
                    self.col = 1;
                }
                Token::Whitespace {
                    value: Whitespace::Tab,
                    ..
                } => self.col += 4,
                Token::Word { value, .. } if value.quote_style == None => {
                    self.col += value.value.len() as u64
                }
                Token::Word { value, .. } if value.quote_style != None => {
                    self.col += value.value.len() as u64 + 2
                }
                Token::Number { value, .. } => self.col += value.len() as u64,
                Token::SingleQuotedString { value, .. } => self.col += value.len() as u64,
                _ => self.col += 1,
            }

            tokens.push(token);
            self.start = end;
        }
        Ok(tokens)
    }

    /// Get the next token or return None
    fn next_token(
        &self,
        chars: &mut Peekable<CharIndices<'_>>,
    ) -> Result<Option<Token>, TokenizerError> {
        //println!("next_token: {:?}", chars.peek());
        let span = Span::new();
        match chars.peek() {
            Some(&ch) => match ch.1 {
                ' ' => self.consume_and_return(
                    chars,
                    Token::Whitespace {
                        value: Whitespace::Space,
                        span,
                    },
                ),
                '\t' => self.consume_and_return(
                    chars,
                    Token::Whitespace {
                        value: Whitespace::Tab,
                        span,
                    },
                ),
                '\n' => self.consume_and_return(
                    chars,
                    Token::Whitespace {
                        value: Whitespace::Newline,
                        span,
                    },
                ),
                '\r' => {
                    // Emit a single Whitespace::Newline token for \r and \r\n
                    chars.next();
                    if let Some((_, '\n')) = chars.peek() {
                        chars.next();
                    }
                    Ok(Some(Token::Whitespace {
                        value: Whitespace::Newline,
                        span,
                    }))
                }
                'N' => {
                    chars.next(); // consume, to check the next char
                    match chars.peek() {
                        Some((_, '\'')) => {
                            // N'...' - a <national character string literal>
                            let value = self.tokenize_single_quoted_string(chars)?;
                            Ok(Some(Token::NationalStringLiteral { value, span }))
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
                        Some((_, '\'')) => {
                            // X'...' - a <binary string literal>
                            let value = self.tokenize_single_quoted_string(chars)?;
                            Ok(Some(Token::HexStringLiteral { value, span }))
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

                    if s.chars().all(|x| ('0'..='9').contains(&x) || x == '.') {
                        let mut value =
                            peeking_take_while(&mut s.char_indices().peekable(), |ch| {
                                matches!(ch, '0'..='9' | '.')
                            });
                        let s2 = peeking_take_while(chars, |ch| matches!(ch, '0'..='9' | '.'));
                        value += s2.as_str();
                        return Ok(Some(Token::Number {
                            value,
                            long: false,
                            span,
                        }));
                    }
                    Ok(Some(Token::make_word(&s, None)))
                }
                // string
                '\'' => {
                    let value = self.tokenize_single_quoted_string(chars)?;
                    Ok(Some(Token::SingleQuotedString { value, span }))
                }
                // delimited (quoted) identifier
                quote_start if self.dialect.is_delimited_identifier_start(quote_start) => {
                    chars.next(); // consume the opening quote
                    let quote_end = Word::matching_end_quote(quote_start);
                    let s = peeking_take_while(chars, |ch| ch != quote_end);
                    if chars.next().map(|(_, c)| c) == Some(quote_end) {
                        Ok(Some(Token::make_word(&s, Some(quote_start))))
                    } else {
                        self.tokenizer_error(
                            self.start..self.query.len(),
                            format!("Expected close delimiter '{}' before EOF.", quote_end),
                        )
                    }
                }
                // numbers and period
                '0'..='9' | '.' => {
                    let mut value = peeking_take_while(chars, |ch| matches!(ch, '0'..='9'));

                    // match binary literal that starts with 0x
                    if value == "0" && chars.peek().map(|(_, c)| c) == Some(&'x') {
                        chars.next();
                        let value = peeking_take_while(
                            chars,
                            |ch| matches!(ch, '0'..='9' | 'A'..='F' | 'a'..='f'),
                        );
                        return Ok(Some(Token::HexStringLiteral { value, span }));
                    }

                    // match one period
                    if let Some((_, '.')) = chars.peek() {
                        value.push('.');
                        chars.next();
                    }
                    value += &peeking_take_while(chars, |ch| matches!(ch, '0'..='9'));

                    // No number -> Token::Period
                    if value == "." {
                        return Ok(Some(Token::Period { span }));
                    }

                    let long = if let Some((_, 'L')) = chars.peek() {
                        chars.next();
                        true
                    } else {
                        false
                    };
                    Ok(Some(Token::Number { value, long, span }))
                }
                // punctuation
                '(' => self.consume_and_return(chars, Token::LParen { span }),
                ')' => self.consume_and_return(chars, Token::RParen { span }),
                ',' => self.consume_and_return(chars, Token::Comma { span }),
                // operators
                '-' => {
                    chars.next(); // consume the '-'
                    match chars.peek() {
                        Some((_, '-')) => {
                            chars.next(); // consume the second '-', starting a single-line comment
                            let comment = self.tokenize_single_line_comment(chars);
                            Ok(Some(Token::Whitespace {
                                value: Whitespace::SingleLineComment {
                                    prefix: "--".to_owned(),
                                    comment,
                                },
                                span,
                            }))
                        }
                        // a regular '-' operator
                        _ => Ok(Some(Token::Minus { span })),
                    }
                }
                '/' => {
                    chars.next(); // consume the '/'
                    match chars.peek() {
                        Some((_, '*')) => {
                            chars.next(); // consume the '*', starting a multi-line comment
                            self.tokenize_multiline_comment(chars)
                        }
                        Some((_, '/')) if dialect_of!(self is SnowflakeDialect) => {
                            chars.next(); // consume the second '/', starting a snowflake single-line comment
                            let comment = self.tokenize_single_line_comment(chars);
                            Ok(Some(Token::Whitespace {
                                value: Whitespace::SingleLineComment {
                                    prefix: "//".to_owned(),
                                    comment,
                                },
                                span,
                            }))
                        }
                        // a regular '/' operator
                        _ => Ok(Some(Token::Div { span })),
                    }
                }
                '+' => self.consume_and_return(chars, Token::Plus { span }),
                '*' => self.consume_and_return(chars, Token::Mul { span }),
                '%' => self.consume_and_return(chars, Token::Mod { span }),
                '|' => {
                    chars.next(); // consume the '|'
                    match chars.peek() {
                        Some((_, '/')) => {
                            self.consume_and_return(chars, Token::PGSquareRoot { span })
                        }
                        Some((_, '|')) => {
                            chars.next(); // consume the second '|'
                            match chars.peek() {
                                Some((_, '/')) => {
                                    self.consume_and_return(chars, Token::PGCubeRoot { span })
                                }
                                _ => Ok(Some(Token::StringConcat { span })),
                            }
                        }
                        // Bitshift '|' operator
                        _ => Ok(Some(Token::Pipe { span })),
                    }
                }
                '=' => {
                    chars.next(); // consume
                    match chars.peek() {
                        Some((_, '>')) => self.consume_and_return(chars, Token::RArrow { span }),
                        _ => Ok(Some(Token::Eq { span })),
                    }
                }
                '!' => {
                    chars.next(); // consume
                    match chars.peek() {
                        Some((_, '=')) => self.consume_and_return(chars, Token::Neq { span }),
                        Some((_, '!')) => {
                            self.consume_and_return(chars, Token::DoubleExclamationMark { span })
                        }
                        Some((_, '~')) => {
                            chars.next();
                            match chars.peek() {
                                Some((_, '*')) => self.consume_and_return(
                                    chars,
                                    Token::ExclamationMarkTildeAsterisk { span },
                                ),
                                _ => Ok(Some(Token::ExclamationMarkTilde { span })),
                            }
                        }
                        _ => Ok(Some(Token::ExclamationMark { span })),
                    }
                }
                '<' => {
                    chars.next(); // consume
                    match chars.peek() {
                        Some((_, '=')) => {
                            chars.next();
                            match chars.peek() {
                                Some((_, '>')) => {
                                    self.consume_and_return(chars, Token::Spaceship { span })
                                }
                                _ => Ok(Some(Token::LtEq { span })),
                            }
                        }
                        Some((_, '>')) => self.consume_and_return(chars, Token::Neq { span }),
                        Some((_, '<')) => self.consume_and_return(chars, Token::ShiftLeft { span }),
                        _ => Ok(Some(Token::Lt { span })),
                    }
                }
                '>' => {
                    chars.next(); // consume
                    match chars.peek() {
                        Some((_, '=')) => self.consume_and_return(chars, Token::GtEq { span }),
                        Some((_, '>')) => {
                            self.consume_and_return(chars, Token::ShiftRight { span })
                        }
                        _ => Ok(Some(Token::Gt { span })),
                    }
                }
                ':' => {
                    chars.next();
                    match chars.peek() {
                        Some((_, ':')) => {
                            self.consume_and_return(chars, Token::DoubleColon { span })
                        }
                        _ => Ok(Some(Token::Colon { span })),
                    }
                }
                ';' => self.consume_and_return(chars, Token::SemiColon { span }),
                '\\' => self.consume_and_return(chars, Token::Backslash { span }),
                '[' => self.consume_and_return(chars, Token::LBracket { span }),
                ']' => self.consume_and_return(chars, Token::RBracket { span }),
                '&' => self.consume_and_return(chars, Token::Ampersand { span }),
                '^' => self.consume_and_return(chars, Token::Caret { span }),
                '{' => self.consume_and_return(chars, Token::LBrace { span }),
                '}' => self.consume_and_return(chars, Token::RBrace { span }),
                '#' if dialect_of!(self is SnowflakeDialect) => {
                    chars.next(); // consume the '#', starting a snowflake single-line comment
                    let comment = self.tokenize_single_line_comment(chars);
                    Ok(Some(Token::Whitespace {
                        value: Whitespace::SingleLineComment {
                            prefix: "#".to_owned(),
                            comment,
                        },
                        span,
                    }))
                }
                '~' => {
                    chars.next(); // consume
                    match chars.peek() {
                        Some((_, '*')) => {
                            self.consume_and_return(chars, Token::TildeAsterisk { span })
                        }
                        _ => Ok(Some(Token::Tilde { span })),
                    }
                }
                '#' => self.consume_and_return(chars, Token::Sharp { span }),
                '@' => self.consume_and_return(chars, Token::AtSign { span }),
                other => self.consume_and_return(chars, Token::Char { value: other, span }),
            },
            None => Ok(None),
        }
    }

    fn tokenizer_error<R>(
        &self,
        span: impl Into<Span>,
        message: impl Into<String>,
    ) -> Result<R, TokenizerError> {
        Err(TokenizerError {
            message: message.into(),
            span: span.into(),
            col: self.col,
            line: self.line,
        })
    }

    // Consume characters until newline
    fn tokenize_single_line_comment(&self, chars: &mut Peekable<CharIndices<'_>>) -> String {
        let mut comment = peeking_take_while(chars, |ch| ch != '\n');
        if let Some((_, ch)) = chars.next() {
            assert_eq!(ch, '\n');
            comment.push(ch);
        }
        comment
    }

    /// Tokenize an identifier or keyword, after the first char is already consumed.
    fn tokenize_word(&self, first_char: char, chars: &mut Peekable<CharIndices<'_>>) -> String {
        let mut s = first_char.to_string();
        s.push_str(&peeking_take_while(chars, |ch| {
            self.dialect.is_identifier_part(ch)
        }));
        s
    }

    /// Read a single quoted string, starting with the opening quote.
    fn tokenize_single_quoted_string(
        &self,
        chars: &mut Peekable<CharIndices<'_>>,
    ) -> Result<String, TokenizerError> {
        let mut s = String::new();
        chars.next(); // consume the opening quote

        // slash escaping is specific to MySQL dialect
        let mut is_escaped = false;
        while let Some(&ch) = chars.peek() {
            match ch {
                (_, '\'') => {
                    chars.next(); // consume
                    if is_escaped {
                        s.push('\'');
                        is_escaped = false;
                    } else if chars.peek().map(|c| c.1 == '\'').unwrap_or(false) {
                        s.push('\'');
                        chars.next();
                    } else {
                        return Ok(s);
                    }
                }
                (_, '\\') => {
                    if dialect_of!(self is MySqlDialect) {
                        is_escaped = !is_escaped;
                    } else {
                        s.push('\\');
                    }
                    chars.next();
                }
                (_, ch) => {
                    chars.next(); // consume
                    s.push(ch);
                }
            }
        }
        let end = chars.peek().map(|(i, _)| *i).unwrap_or(self.query.len());
        self.tokenizer_error(self.start..end, "Unterminated string literal")
    }

    fn tokenize_multiline_comment(
        &self,
        chars: &mut Peekable<CharIndices<'_>>,
    ) -> Result<Option<Token>, TokenizerError> {
        let mut s = String::new();
        let mut maybe_closing_comment = false;
        // TODO: deal with nested comments
        loop {
            match chars.next() {
                Some((_, ch)) => {
                    if maybe_closing_comment {
                        if ch == '/' {
                            break Ok(Some(Token::Whitespace {
                                value: Whitespace::MultiLineComment(s),
                                span: Span::default(),
                            }));
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
                    break self.tokenizer_error(
                        self.start..self.query.len(),
                        "Unexpected EOF while in a multi-line comment",
                    )
                }
            }
        }
    }

    #[allow(clippy::unnecessary_wraps)]
    fn consume_and_return(
        &self,
        chars: &mut Peekable<CharIndices<'_>>,
        t: Token,
    ) -> Result<Option<Token>, TokenizerError> {
        chars.next();
        Ok(Some(t))
    }
}

/// Read from `chars` until `predicate` returns `false` or EOF is hit.
/// Return the characters read as String, and keep the first non-matching
/// char available as `chars.next()`.
fn peeking_take_while(
    chars: &mut Peekable<CharIndices<'_>>,
    mut predicate: impl FnMut(char) -> bool,
) -> String {
    let mut s = String::new();
    while let Some(ch) = chars.peek().map(|(_, ch)| *ch) {
        if predicate(ch) {
            chars.next(); // consume
            s.push(ch);
        } else {
            break;
        }
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dialect::{GenericDialect, MsSqlDialect};

    #[test]
    fn tokenizer_error_impl() {
        let err = TokenizerError {
            message: "test".into(),
            line: 1,
            col: 1,
            span: Span::new(),
        };
        #[cfg(feature = "std")]
        {
            use std::error::Error;
            assert!(err.source().is_none());
        }
        assert_eq!(err.to_string(), "test at Line: 1, Column 1");
    }

    #[test]
    fn tokenize_select_1() {
        let sql = String::from("SELECT 1");
        let dialect = GenericDialect {};
        let mut tokenizer = Tokenizer::new(&dialect, &sql);
        let tokens = tokenizer.tokenize().unwrap();

        let expected = vec![
            Token::make_keyword("SELECT"),
            Token::Whitespace {
                value: Whitespace::Space,
                span: Span::new(),
            },
            Token::Number {
                value: String::from("1"),
                long: false,
                span: Span::new(),
            },
        ];

        compare(expected, tokens);
    }

    #[test]
    fn tokenize_select_float() {
        let sql = String::from("SELECT .1");
        let dialect = GenericDialect {};
        let mut tokenizer = Tokenizer::new(&dialect, &sql);
        let tokens = tokenizer.tokenize().unwrap();

        let expected = vec![
            Token::make_keyword("SELECT"),
            Token::Whitespace {
                value: Whitespace::Space,
                span: Span::new(),
            },
            Token::Number {
                value: String::from(".1"),
                long: false,
                span: Span::new(),
            },
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
            Token::Whitespace {
                value: Whitespace::Space,
                span: Span::new(),
            },
            Token::make_word("sqrt", None),
            Token::LParen { span: Span::new() },
            Token::Number {
                value: String::from("1"),
                long: false,
                span: Span::new(),
            },
            Token::RParen { span: Span::new() },
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
            Token::Whitespace {
                value: Whitespace::Space,
                span: Span::new(),
            },
            Token::SingleQuotedString {
                value: String::from("a"),
                span: Span::new(),
            },
            Token::Whitespace {
                value: Whitespace::Space,
                span: Span::new(),
            },
            Token::StringConcat { span: Span::new() },
            Token::Whitespace {
                value: Whitespace::Space,
                span: Span::new(),
            },
            Token::SingleQuotedString {
                value: String::from("b"),
                span: Span::new(),
            },
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
            Token::Whitespace {
                value: Whitespace::Space,
                span: Span::new(),
            },
            Token::make_word("one", None),
            Token::Whitespace {
                value: Whitespace::Space,
                span: Span::new(),
            },
            Token::Pipe { span: Span::new() },
            Token::Whitespace {
                value: Whitespace::Space,
                span: Span::new(),
            },
            Token::make_word("two", None),
            Token::Whitespace {
                value: Whitespace::Space,
                span: Span::new(),
            },
            Token::Caret { span: Span::new() },
            Token::Whitespace {
                value: Whitespace::Space,
                span: Span::new(),
            },
            Token::make_word("three", None),
        ];
        compare(expected, tokens);
    }

    #[test]
    fn tokenize_logical_xor() {
        let sql =
            String::from("SELECT true XOR true, false XOR false, true XOR false, false XOR true");
        let dialect = GenericDialect {};
        let mut tokenizer = Tokenizer::new(&dialect, &sql);
        let tokens = tokenizer.tokenize().unwrap();

        let expected = vec![
            Token::make_keyword("SELECT"),
            Token::Whitespace {
                value: Whitespace::Space,
                span: Span::new(),
            },
            Token::make_keyword("true"),
            Token::Whitespace {
                value: Whitespace::Space,
                span: Span::new(),
            },
            Token::make_keyword("XOR"),
            Token::Whitespace {
                value: Whitespace::Space,
                span: Span::new(),
            },
            Token::make_keyword("true"),
            Token::Comma { span: Span::new() },
            Token::Whitespace {
                value: Whitespace::Space,
                span: Span::new(),
            },
            Token::make_keyword("false"),
            Token::Whitespace {
                value: Whitespace::Space,
                span: Span::new(),
            },
            Token::make_keyword("XOR"),
            Token::Whitespace {
                value: Whitespace::Space,
                span: Span::new(),
            },
            Token::make_keyword("false"),
            Token::Comma { span: Span::new() },
            Token::Whitespace {
                value: Whitespace::Space,
                span: Span::new(),
            },
            Token::make_keyword("true"),
            Token::Whitespace {
                value: Whitespace::Space,
                span: Span::new(),
            },
            Token::make_keyword("XOR"),
            Token::Whitespace {
                value: Whitespace::Space,
                span: Span::new(),
            },
            Token::make_keyword("false"),
            Token::Comma { span: Span::new() },
            Token::Whitespace {
                value: Whitespace::Space,
                span: Span::new(),
            },
            Token::make_keyword("false"),
            Token::Whitespace {
                value: Whitespace::Space,
                span: Span::new(),
            },
            Token::make_keyword("XOR"),
            Token::Whitespace {
                value: Whitespace::Space,
                span: Span::new(),
            },
            Token::make_keyword("true"),
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
            Token::Whitespace {
                value: Whitespace::Space,
                span: Span::new(),
            },
            Token::Mul { span: Span::new() },
            Token::Whitespace {
                value: Whitespace::Space,
                span: Span::new(),
            },
            Token::make_keyword("FROM"),
            Token::Whitespace {
                value: Whitespace::Space,
                span: Span::new(),
            },
            Token::make_word("customer", None),
            Token::Whitespace {
                value: Whitespace::Space,
                span: Span::new(),
            },
            Token::make_keyword("WHERE"),
            Token::Whitespace {
                value: Whitespace::Space,
                span: Span::new(),
            },
            Token::make_word("id", None),
            Token::Whitespace {
                value: Whitespace::Space,
                span: Span::new(),
            },
            Token::Eq { span: Span::new() },
            Token::Whitespace {
                value: Whitespace::Space,
                span: Span::new(),
            },
            Token::Number {
                value: String::from("1"),
                long: false,
                span: Span::new(),
            },
            Token::Whitespace {
                value: Whitespace::Space,
                span: Span::new(),
            },
            Token::make_keyword("LIMIT"),
            Token::Whitespace {
                value: Whitespace::Space,
                span: Span::new(),
            },
            Token::Number {
                value: String::from("5"),
                long: false,
                span: Span::new(),
            },
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
            Token::Whitespace {
                value: Whitespace::Space,
                span: Span::new(),
            },
            Token::make_keyword("SELECT"),
            Token::Whitespace {
                value: Whitespace::Space,
                span: Span::new(),
            },
            Token::Mul { span: Span::new() },
            Token::Whitespace {
                value: Whitespace::Space,
                span: Span::new(),
            },
            Token::make_keyword("FROM"),
            Token::Whitespace {
                value: Whitespace::Space,
                span: Span::new(),
            },
            Token::make_word("customer", None),
            Token::Whitespace {
                value: Whitespace::Space,
                span: Span::new(),
            },
            Token::make_keyword("WHERE"),
            Token::Whitespace {
                value: Whitespace::Space,
                span: Span::new(),
            },
            Token::make_word("id", None),
            Token::Whitespace {
                value: Whitespace::Space,
                span: Span::new(),
            },
            Token::Eq { span: Span::new() },
            Token::Whitespace {
                value: Whitespace::Space,
                span: Span::new(),
            },
            Token::Number {
                value: String::from("1"),
                long: false,
                span: Span::new(),
            },
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
            Token::Whitespace {
                value: Whitespace::Space,
                span: Span::new(),
            },
            Token::make_keyword("ANALYZE"),
            Token::Whitespace {
                value: Whitespace::Space,
                span: Span::new(),
            },
            Token::make_keyword("SELECT"),
            Token::Whitespace {
                value: Whitespace::Space,
                span: Span::new(),
            },
            Token::Mul { span: Span::new() },
            Token::Whitespace {
                value: Whitespace::Space,
                span: Span::new(),
            },
            Token::make_keyword("FROM"),
            Token::Whitespace {
                value: Whitespace::Space,
                span: Span::new(),
            },
            Token::make_word("customer", None),
            Token::Whitespace {
                value: Whitespace::Space,
                span: Span::new(),
            },
            Token::make_keyword("WHERE"),
            Token::Whitespace {
                value: Whitespace::Space,
                span: Span::new(),
            },
            Token::make_word("id", None),
            Token::Whitespace {
                value: Whitespace::Space,
                span: Span::new(),
            },
            Token::Eq { span: Span::new() },
            Token::Whitespace {
                value: Whitespace::Space,
                span: Span::new(),
            },
            Token::Number {
                value: String::from("1"),
                long: false,
                span: Span::new(),
            },
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
            Token::Whitespace {
                value: Whitespace::Space,
                span: Span::new(),
            },
            Token::Mul { span: Span::new() },
            Token::Whitespace {
                value: Whitespace::Space,
                span: Span::new(),
            },
            Token::make_keyword("FROM"),
            Token::Whitespace {
                value: Whitespace::Space,
                span: Span::new(),
            },
            Token::make_word("customer", None),
            Token::Whitespace {
                value: Whitespace::Space,
                span: Span::new(),
            },
            Token::make_keyword("WHERE"),
            Token::Whitespace {
                value: Whitespace::Space,
                span: Span::new(),
            },
            Token::make_word("salary", None),
            Token::Whitespace {
                value: Whitespace::Space,
                span: Span::new(),
            },
            Token::Neq { span: Span::new() },
            Token::Whitespace {
                value: Whitespace::Space,
                span: Span::new(),
            },
            Token::SingleQuotedString {
                value: String::from("Not Provided"),
                span: Span::new(),
            },
        ];

        compare(expected, tokens);
    }

    #[test]
    fn tokenize_invalid_string() {
        let sql = String::from("\nمصطفىh");

        let dialect = GenericDialect {};
        let mut tokenizer = Tokenizer::new(&dialect, &sql);
        let tokens = tokenizer.tokenize().unwrap();
        // println!("tokens: {:#?}", tokens);
        let expected = vec![
            Token::Whitespace {
                value: Whitespace::Newline,
                span: Span::new(),
            },
            Token::Char {
                value: 'م',
                span: Span::new(),
            },
            Token::Char {
                value: 'ص',
                span: Span::new(),
            },
            Token::Char {
                value: 'ط',
                span: Span::new(),
            },
            Token::Char {
                value: 'ف',
                span: Span::new(),
            },
            Token::Char {
                value: 'ى',
                span: Span::new(),
            },
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
        let expected = vec![Token::SingleQuotedString {
            value: "foo\r\nbar\nbaz".to_string(),
            span: Span::new(),
        }];
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
                col: 8,
                span: (7..11).into()
            })
        );
    }

    #[test]
    fn tokenize_invalid_string_cols() {
        let sql = String::from("\n\nSELECT * FROM table\tمصطفىh");

        let dialect = GenericDialect {};
        let mut tokenizer = Tokenizer::new(&dialect, &sql);
        let tokens = tokenizer.tokenize().unwrap();
        // println!("tokens: {:#?}", tokens);
        let expected = vec![
            Token::Whitespace {
                value: Whitespace::Newline,
                span: Span::new(),
            },
            Token::Whitespace {
                value: Whitespace::Newline,
                span: Span::new(),
            },
            Token::make_keyword("SELECT"),
            Token::Whitespace {
                value: Whitespace::Space,
                span: Span::new(),
            },
            Token::Mul { span: Span::new() },
            Token::Whitespace {
                value: Whitespace::Space,
                span: Span::new(),
            },
            Token::make_keyword("FROM"),
            Token::Whitespace {
                value: Whitespace::Space,
                span: Span::new(),
            },
            Token::make_keyword("table"),
            Token::Whitespace {
                value: Whitespace::Tab,
                span: Span::new(),
            },
            Token::Char {
                value: 'م',
                span: Span::new(),
            },
            Token::Char {
                value: 'ص',
                span: Span::new(),
            },
            Token::Char {
                value: 'ط',
                span: Span::new(),
            },
            Token::Char {
                value: 'ف',
                span: Span::new(),
            },
            Token::Char {
                value: 'ى',
                span: Span::new(),
            },
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
            Token::LParen { span: Span::new() },
            Token::make_word("key", None),
            Token::RArrow { span: Span::new() },
            Token::make_word("value", None),
            Token::RParen { span: Span::new() },
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
            Token::Whitespace {
                value: Whitespace::Space,
                span: Span::new(),
            },
            Token::make_keyword("IS"),
            Token::Whitespace {
                value: Whitespace::Space,
                span: Span::new(),
            },
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
            Token::Number {
                value: "0".to_string(),
                long: false,
                span: Span::new(),
            },
            Token::Whitespace {
                value: Whitespace::SingleLineComment {
                    prefix: "--".to_string(),
                    comment: "this is a comment\n".to_string(),
                },
                span: Span::new(),
            },
            Token::Number {
                value: "1".to_string(),
                long: false,
                span: Span::new(),
            },
        ];
        compare(expected, tokens);
    }

    #[test]
    fn tokenize_comment_at_eof() {
        let sql = String::from("--this is a comment");

        let dialect = GenericDialect {};
        let mut tokenizer = Tokenizer::new(&dialect, &sql);
        let tokens = tokenizer.tokenize().unwrap();
        let expected = vec![Token::Whitespace {
            value: Whitespace::SingleLineComment {
                prefix: "--".to_string(),
                comment: "this is a comment".to_string(),
            },
            span: Span::new(),
        }];
        compare(expected, tokens);
    }

    #[test]
    fn tokenize_multiline_comment() {
        let sql = String::from("0/*multi-line\n* /comment*/1");

        let dialect = GenericDialect {};
        let mut tokenizer = Tokenizer::new(&dialect, &sql);
        let tokens = tokenizer.tokenize().unwrap();
        let expected = vec![
            Token::Number {
                value: "0".to_string(),
                long: false,
                span: Span::new(),
            },
            Token::Whitespace {
                value: Whitespace::MultiLineComment("multi-line\n* /comment".to_string()),
                span: Span::new(),
            },
            Token::Number {
                value: "1".to_string(),
                long: false,
                span: Span::new(),
            },
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
            Token::Whitespace {
                value: Whitespace::Newline,
                span: Span::new(),
            },
            Token::Whitespace {
                value: Whitespace::MultiLineComment("* Comment *".to_string()),
                span: Span::new(),
            },
            Token::Whitespace {
                value: Whitespace::Newline,
                span: Span::new(),
            },
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
                col: 1,
                span: (0..4).into(),
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
            Token::Whitespace {
                value: Whitespace::Newline,
                span: Span::new(),
            },
            Token::make_word("line2", None),
            Token::Whitespace {
                value: Whitespace::Newline,
                span: Span::new(),
            },
            Token::make_word("line3", None),
            Token::Whitespace {
                value: Whitespace::Newline,
                span: Span::new(),
            },
            Token::make_word("line4", None),
            Token::Whitespace {
                value: Whitespace::Newline,
                span: Span::new(),
            },
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
            Token::Whitespace {
                value: Whitespace::Space,
                span: Span::new(),
            },
            Token::make_keyword("TOP"),
            Token::Whitespace {
                value: Whitespace::Space,
                span: Span::new(),
            },
            Token::Number {
                value: String::from("5"),
                long: false,
                span: Span::new(),
            },
            Token::Whitespace {
                value: Whitespace::Space,
                span: Span::new(),
            },
            Token::make_word("bar", Some('[')),
            Token::Whitespace {
                value: Whitespace::Space,
                span: Span::new(),
            },
            Token::make_keyword("FROM"),
            Token::Whitespace {
                value: Whitespace::Space,
                span: Span::new(),
            },
            Token::make_word("foo", None),
        ];
        compare(expected, tokens);
    }

    #[test]
    fn tokenize_pg_regex_match() {
        let sql = "SELECT col ~ '^a', col ~* '^a', col !~ '^a', col !~* '^a'";
        let dialect = GenericDialect {};
        let mut tokenizer = Tokenizer::new(&dialect, sql);
        let tokens = tokenizer.tokenize().unwrap();
        let expected = vec![
            Token::make_keyword("SELECT"),
            Token::Whitespace {
                value: Whitespace::Space,
                span: Span::new(),
            },
            Token::make_word("col", None),
            Token::Whitespace {
                value: Whitespace::Space,
                span: Span::new(),
            },
            Token::Tilde { span: Span::new() },
            Token::Whitespace {
                value: Whitespace::Space,
                span: Span::new(),
            },
            Token::SingleQuotedString {
                value: "^a".into(),
                span: Span::new(),
            },
            Token::Comma { span: Span::new() },
            Token::Whitespace {
                value: Whitespace::Space,
                span: Span::new(),
            },
            Token::make_word("col", None),
            Token::Whitespace {
                value: Whitespace::Space,
                span: Span::new(),
            },
            Token::TildeAsterisk { span: Span::new() },
            Token::Whitespace {
                value: Whitespace::Space,
                span: Span::new(),
            },
            Token::SingleQuotedString {
                value: "^a".into(),
                span: Span::new(),
            },
            Token::Comma { span: Span::new() },
            Token::Whitespace {
                value: Whitespace::Space,
                span: Span::new(),
            },
            Token::make_word("col", None),
            Token::Whitespace {
                value: Whitespace::Space,
                span: Span::new(),
            },
            Token::ExclamationMarkTilde { span: Span::new() },
            Token::Whitespace {
                value: Whitespace::Space,
                span: Span::new(),
            },
            Token::SingleQuotedString {
                value: "^a".into(),
                span: Span::new(),
            },
            Token::Comma { span: Span::new() },
            Token::Whitespace {
                value: Whitespace::Space,
                span: Span::new(),
            },
            Token::make_word("col", None),
            Token::Whitespace {
                value: Whitespace::Space,
                span: Span::new(),
            },
            Token::ExclamationMarkTildeAsterisk { span: Span::new() },
            Token::Whitespace {
                value: Whitespace::Space,
                span: Span::new(),
            },
            Token::SingleQuotedString {
                value: "^a".into(),
                span: Span::new(),
            },
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

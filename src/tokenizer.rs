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
use super::dialect::BigQueryDialect;
use super::dialect::Dialect;
use super::dialect::PostgreSqlDialect;
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
    /// This should retains the escaped character sequences so that
    /// .to_string() of the value will give the value that was in the input
    SingleQuotedString(String),
    /// Double quoted string: i.e: "string"
    /// This should retains the escaped character sequences so that
    /// .to_string() of the value will give the value that was in the input
    DoubleQuotedString(String),
    /// Single quoted string: i.e: 'string'
    BacktickQuotedString(String),
    BqRegexQuotedString {
        value: String,
        quote: char,
    },
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
    /// https://www.postgresql.org/docs/9.5/functions-json.html
    /// various PostgreSQL JSON index operators:
    /// `->`
    PgJsonGetIndex,
    /// `->>`
    PgJsonGetIndexText,
    /// `#>` Get JSON object at specified path
    PgJsonGetPath,
    /// `#>>` Get JSON object at specified path as text
    PgJsonGetPathText,
    /// `@>` Does the left JSON value contain the right JSON path/value entries
    /// at the top level?
    PgJsonGt,
    /// <@` Are the left JSON path/value entries contained at the top level
    /// within the right JSON value?
    PgJsonLt,
    /// `?` Does the string exist as a top-level key within the JSON value?
    PgJsonKeyExists,
    /// `?|` Do any of these array strings exist as top-level keys?
    PgJsonAnyKeyExists,
    /// `?&` Do all of these array strings exist as top-level keys?
    PgJsonAllKeysExist,
    /// `#-` Delete the field or element with specified path (for JSON arrays,
    /// negative integers count from the end)
    PgJsonMinus,
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
            Token::DoubleQuotedString(ref s) => write!(f, "\"{}\"", s),
            Token::BacktickQuotedString(ref s) => write!(f, "`{}`", s),
            Token::NationalStringLiteral(ref s) => write!(f, "N'{}'", s),
            Token::BqRegexQuotedString { ref value, quote } => {
                write!(f, "r{}{}{}", quote, value, quote)
            }
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
            Token::PgJsonGetIndex => f.write_str("->"),
            Token::PgJsonGetIndexText => f.write_str("->>"),
            Token::PgJsonGetPath => f.write_str("#>"),
            Token::PgJsonGetPathText => f.write_str("#>>"),
            Token::PgJsonGt => f.write_str("@>"),
            Token::PgJsonLt => f.write_str("@<"),
            Token::PgJsonKeyExists => f.write_str("?"),
            Token::PgJsonAnyKeyExists => f.write_str("?|"),
            Token::PgJsonAllKeysExist => f.write_str("?&"),
            Token::PgJsonMinus => f.write_str("#-"),
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
    SingleLineComment {
        comment: String,
        prefix: String,
    },
    MultiLineComment(String),
    /// https://en.wikipedia.org/wiki/Zero-width_space
    Zwsp,
}

impl fmt::Display for Whitespace {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Whitespace::Space => f.write_str(" "),
            Whitespace::Newline => f.write_str("\n"),
            Whitespace::Tab => f.write_str("\t"),
            Whitespace::SingleLineComment { prefix, comment } => write!(f, "{}{}", prefix, comment),
            Whitespace::MultiLineComment(s) => write!(f, "/*{}*/", s),
            Whitespace::Zwsp => write!(f, "\u{feff}"),
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
                Token::Word(w) if w.quote_style == None => self.col += w.value.len() as u64,
                Token::Word(w) if w.quote_style != None => self.col += w.value.len() as u64 + 2,
                Token::Number(s) => self.col += s.len() as u64,
                Token::SingleQuotedString(s) => self.col += s.len() as u64,
                Token::DoubleQuotedString(s) => self.col += s.len() as u64,
                Token::BacktickQuotedString(s) => self.col += s.len() as u64,
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
                '\u{feff}' => self.consume_and_return(chars, Token::Whitespace(Whitespace::Zwsp)),
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
                r @ 'r' | r @ 'R' if dialect_of!(self is BigQueryDialect) => {
                    chars.next(); // consume, to check the next char
                    match chars.peek() {
                        Some('\'') => {
                            // r'...' - a regex literal
                            let value = self.tokenize_single_quoted_string(chars)?;
                            Ok(Some(Token::BqRegexQuotedString { value, quote: '\'' }))
                        }
                        Some('"') => {
                            // r"..." - a regex literal
                            let value = self.tokenize_double_quoted_string(chars)?;
                            Ok(Some(Token::BqRegexQuotedString { value, quote: '"' }))
                        }
                        _ => {
                            // regular identifier starting with an "r" or "R"
                            let s = self.tokenize_word(r, chars);
                            Ok(Some(Token::make_word(&s, None)))
                        }
                    }
                }
                'N' => {
                    chars.next(); // consume, to check the next char
                    match chars.peek() {
                        Some('\'') => {
                            // N'...' - a <national character string literal>
                            let s = self.tokenize_single_quoted_string(chars)?;
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
                            let s = self.tokenize_single_quoted_string(chars)?;
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
                    let s = self.tokenize_single_quoted_string(chars)?;
                    Ok(Some(Token::SingleQuotedString(s)))
                }
                // string
                '"' if dialect_of!(self is BigQueryDialect) => {
                    let s = self.tokenize_double_quoted_string(chars)?;
                    Ok(Some(Token::DoubleQuotedString(s)))
                }
                // string
                '`' if dialect_of!(self is BigQueryDialect) => {
                    chars.next(); // consume opening backtick
                    let s = peeking_take_while(chars, |ch| ch != '`');
                    match chars.peek() {
                        Some('`') => {
                            chars.next(); // consume closing backtick
                            Ok(Some(Token::BacktickQuotedString(s)))
                        }
                        _ => self.tokenizer_error("Unterminated backtick literal"),
                    }
                }
                // delimited (quoted) identifier
                quote_start if self.dialect.is_delimited_identifier_start(quote_start) => {
                    chars.next(); // consume the opening quote
                    let quote_end = Word::matching_end_quote(quote_start);
                    let s = peeking_take_while(chars, |ch| ch != quote_end);
                    if chars.next() == Some(quote_end) {
                        Ok(Some(Token::make_word(&s, Some(quote_start))))
                    } else {
                        self.tokenizer_error(
                            format!("Expected close delimiter '{}' before EOF.", quote_end)
                                .as_str(),
                        )
                    }
                }
                // numbers
                '0'..='9' | '.' => {
                    // TODO: https://jakewheat.github.io/sql-overview/sql-2011-foundation-grammar.html#unsigned-numeric-literal
                    Ok(Some(consume_number_literal_or_dot(chars, ch)))
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
                            let comment = self.tokenize_single_line_comment(chars);
                            Ok(Some(Token::Whitespace(Whitespace::SingleLineComment {
                                prefix: "--".to_owned(),
                                comment,
                            })))
                        }
                        Some('>') if dialect_of!(self is PostgreSqlDialect) => {
                            chars.next(); // consume >
                            if let Some('>') = chars.peek() {
                                chars.next(); // consume >
                                Ok(Some(Token::PgJsonGetIndexText))
                            } else {
                                Ok(Some(Token::PgJsonGetIndex))
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
                        Some('/') if dialect_of!(self is SnowflakeDialect | BigQueryDialect) => {
                            chars.next(); // consume the second '/', starting a snowflake single-line comment
                            let comment = self.tokenize_single_line_comment(chars);
                            Ok(Some(Token::Whitespace(Whitespace::SingleLineComment {
                                prefix: "//".to_owned(),
                                comment,
                            })))
                        }
                        // a regular '/' operator
                        _ => Ok(Some(Token::Div)),
                    }
                }
                '+' => self.consume_and_return(chars, Token::Plus),
                '*' => self.consume_and_return(chars, Token::Mult),
                '%' => self.consume_and_return(chars, Token::Mod),
                '|' => {
                    chars.next(); // consume the '|'
                    match chars.peek() {
                        Some('/') => self.consume_and_return(chars, Token::PGSquareRoot),
                        Some('|') => {
                            chars.next(); // consume the second '|'
                            match chars.peek() {
                                Some('/') => self.consume_and_return(chars, Token::PGCubeRoot),
                                _ => Ok(Some(Token::StringConcat)),
                            }
                        }
                        // Bitshift '|' operator
                        _ => Ok(Some(Token::Pipe)),
                    }
                }
                '=' => {
                    chars.next(); // consume
                    match chars.peek() {
                        Some('>') => self.consume_and_return(chars, Token::RArrow),
                        _ => Ok(Some(Token::Eq)),
                    }
                }
                '!' => {
                    chars.next(); // consume
                    match chars.peek() {
                        Some('=') => self.consume_and_return(chars, Token::Neq),
                        Some('!') => self.consume_and_return(chars, Token::DoubleExclamationMark),
                        _ => Ok(Some(Token::ExclamationMark)),
                    }
                }
                '<' => {
                    chars.next(); // consume
                    match chars.peek() {
                        Some('=') => self.consume_and_return(chars, Token::LtEq),
                        Some('>') => self.consume_and_return(chars, Token::Neq),
                        Some('<') => self.consume_and_return(chars, Token::ShiftLeft),
                        _ => Ok(Some(Token::Lt)),
                    }
                }
                '>' => {
                    chars.next(); // consume
                    match chars.peek() {
                        Some('=') => self.consume_and_return(chars, Token::GtEq),
                        Some('>') => self.consume_and_return(chars, Token::ShiftRight),
                        _ => Ok(Some(Token::Gt)),
                    }
                }
                ':' => {
                    chars.next();
                    match chars.peek() {
                        Some(':') => self.consume_and_return(chars, Token::DoubleColon),
                        _ => Ok(Some(Token::Colon)),
                    }
                }
                ';' => self.consume_and_return(chars, Token::SemiColon),
                '\\' => self.consume_and_return(chars, Token::Backslash),
                '[' => self.consume_and_return(chars, Token::LBracket),
                ']' => self.consume_and_return(chars, Token::RBracket),
                '&' => self.consume_and_return(chars, Token::Ampersand),
                '^' => self.consume_and_return(chars, Token::Caret),
                '{' => self.consume_and_return(chars, Token::LBrace),
                '}' => self.consume_and_return(chars, Token::RBrace),
                '#' if dialect_of!(self is SnowflakeDialect | BigQueryDialect) => {
                    chars.next(); // consume the '#', starting a snowflake single-line comment
                    let comment = self.tokenize_single_line_comment(chars);
                    Ok(Some(Token::Whitespace(Whitespace::SingleLineComment {
                        prefix: "#".to_owned(),
                        comment,
                    })))
                }
                '#' => {
                    chars.next(); // consume #
                    if dialect_of!(self is PostgreSqlDialect) {
                        match chars.peek() {
                            Some('>') => {
                                chars.next(); // consume >
                                if let Some('>') = chars.peek() {
                                    chars.next(); // consume >
                                    Ok(Some(Token::PgJsonGetPathText))
                                } else {
                                    Ok(Some(Token::PgJsonGetPath))
                                }
                            }
                            Some('-') => {
                                chars.next(); // consume -
                                Ok(Some(Token::PgJsonMinus))
                            }
                            _ => Ok(Some(Token::Sharp)),
                        }
                    } else {
                        Ok(Some(Token::Sharp))
                    }
                }
                '~' => self.consume_and_return(chars, Token::Tilde),
                '@' => {
                    chars.next(); // consume @
                    if dialect_of!(self is PostgreSqlDialect) {
                        match chars.peek() {
                            Some('>') => {
                                chars.next(); // consume >
                                Ok(Some(Token::PgJsonGt))
                            }
                            Some('<') => {
                                chars.next(); // consume <
                                Ok(Some(Token::PgJsonLt))
                            }
                            _ => Ok(Some(Token::AtSign)),
                        }
                    } else {
                        Ok(Some(Token::AtSign))
                    }
                }
                '?' => {
                    chars.next(); // consume ?
                    if dialect_of!(self is PostgreSqlDialect) {
                        match chars.peek() {
                            Some('|') => {
                                chars.next(); // consume |
                                Ok(Some(Token::PgJsonAnyKeyExists))
                            }
                            Some('&') => {
                                chars.next(); // consume &
                                Ok(Some(Token::PgJsonAllKeysExist))
                            }
                            _ => Ok(Some(Token::PgJsonKeyExists)),
                        }
                    } else {
                        Ok(Some(Token::Char('?')))
                    }
                }
                other => self.consume_and_return(chars, Token::Char(other)),
            },
            None => Ok(None),
        }
    }

    fn tokenizer_error<R>(&self, message: &str) -> Result<R, TokenizerError> {
        Err(TokenizerError {
            message: message.to_string(),
            col: self.col,
            line: self.line,
        })
    }

    // Consume characters until newline
    fn tokenize_single_line_comment(&self, chars: &mut Peekable<Chars<'_>>) -> String {
        let mut comment = peeking_take_while(chars, |ch| ch != '\n');
        if let Some(ch) = chars.next() {
            assert_eq!(ch, '\n');
            comment.push(ch);
        }
        comment
    }

    /// Tokenize an identifier or keyword, after the first char is already consumed.
    fn tokenize_word(&self, first_char: char, chars: &mut Peekable<Chars<'_>>) -> String {
        let mut s = first_char.to_string();
        s.push_str(&peeking_take_while(chars, |ch| {
            self.dialect.is_identifier_part(ch)
        }));
        s
    }

    /// Read a single quoted string, starting with the opening quote.
    fn tokenize_single_quoted_string(
        &self,
        chars: &mut Peekable<Chars<'_>>,
    ) -> Result<String, TokenizerError> {
        self.tokenize_quoted_string(chars, '\'')
    }

    /// Read a double quoted string, starting with the opening quote.
    fn tokenize_double_quoted_string(
        &self,
        chars: &mut Peekable<Chars<'_>>,
    ) -> Result<String, TokenizerError> {
        self.tokenize_quoted_string(chars, '"')
    }

    /// Read a quoted string (quoted by any character, typically ' or "),
    /// starting with the opening quote.
    fn tokenize_quoted_string(
        &self,
        chars: &mut Peekable<Chars<'_>>,
        quote_ch: char,
    ) -> Result<String, TokenizerError> {
        let mut s = String::new();
        chars.next(); // consume the opening quote
        while let Some(ch) = chars.next() {
            let next_char_is_quote = chars.peek().map(|c| *c == quote_ch).unwrap_or(false);
            match ch {
                // allow backslash to escape the next character, whatever it is
                '\\' => {
                    s.push('\\');
                    if let Some(next_ch) = chars.next() {
                        s.push(next_ch);
                    }
                }
                // bq allows escaping only with backslash; other warehouses
                // allow escaping the quote character by repeating it
                _ if !dialect_of!(self is BigQueryDialect)
                    && ch == quote_ch
                    && next_char_is_quote =>
                {
                    s.push(quote_ch);
                    s.push(quote_ch);
                    chars.next(); // consume quote_ch
                }
                ch if ch == quote_ch => return Ok(s),
                _ => s.push(ch),
            }
        }
        self.tokenizer_error("Unterminated string literal")
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
                None => break self.tokenizer_error("Unexpected EOF while in a multi-line comment"),
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

/// Read from `chars` until `predicate` returns `false` or EOF is hit.
/// Return the characters read as String, and keep the first non-matching
/// char available as `chars.next()`.
fn peeking_take_while(
    chars: &mut Peekable<Chars<'_>>,
    mut predicate: impl FnMut(char) -> bool,
) -> String {
    let mut s = String::new();
    while let Some(&ch) = chars.peek() {
        if predicate(ch) {
            chars.next(); // consume
            s.push(ch);
        } else {
            break;
        }
    }
    s
}

/// handle parsing numbers, including scientific notation
/// https://docs.snowflake.com/en/sql-reference/data-types-numeric.html
fn consume_number_literal_or_dot(chars: &mut Peekable<Chars<'_>>, first: char) -> Token {
    let mut s = String::new();
    chars.next(); // consume
    s.push(first);
    #[derive(PartialEq)]
    enum NumState {
        WholeNum,      // we look for digits or . or e
        Decimal,       // we look for digits or e
        ExponentStart, // we look for either a +- sign or digits
        Exponent,      // we only look for digits
    }
    let mut num_state = if first == '.' {
        NumState::Decimal
    } else {
        NumState::WholeNum
    };
    let mut is_second_char = true;
    while let Some(&ch) = chars.peek() {
        if num_state == NumState::Decimal && is_second_char && !matches!(ch, '0'..='9') {
            return Token::Period;
        }
        let add_to_string = match num_state {
            NumState::WholeNum | NumState::Decimal => match ch {
                '0'..='9' => true,
                '.' if num_state == NumState::WholeNum => {
                    num_state = NumState::Decimal;
                    true
                }
                'e' | 'E' => {
                    num_state = NumState::ExponentStart;
                    true
                }
                _ => false,
            },
            NumState::ExponentStart => {
                num_state = NumState::Exponent;
                matches!(ch, '0'..='9' | '-' | '+')
            }
            NumState::Exponent => matches!(ch, '0'..='9'),
        };
        if add_to_string {
            chars.next(); // consume
            s.push(ch);
        } else {
            break;
        }
        is_second_char = false;
    }
    Token::Number(s)
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
                col: 8
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
                col: 1
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
}

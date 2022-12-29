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
use core::str::Chars;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

#[cfg(feature = "visitor")]
use sqlparser_derive::Visit;

use crate::ast::DollarQuotedString;
use crate::dialect::SnowflakeDialect;
use crate::dialect::{Dialect, MySqlDialect};
use crate::keywords::{Keyword, ALL_KEYWORDS, ALL_KEYWORDS_INDEX};

/// SQL Token enumeration
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit))]
pub enum Token {
    /// An end-of-file marker, not a real token
    EOF,
    /// A keyword (like SELECT) or an optionally quoted SQL identifier
    Word(Word),
    /// An unsigned numeric literal
    Number(String, bool),
    /// A character that could not be tokenized
    Char(char),
    /// Single quoted string: i.e: 'string'
    SingleQuotedString(String),
    /// Double quoted string: i.e: "string"
    DoubleQuotedString(String),
    /// Dollar quoted string: i.e: $$string$$ or $tag_name$string$tag_name$
    DollarQuotedString(DollarQuotedString),
    /// "National" string literal: i.e: N'string'
    NationalStringLiteral(String),
    /// "escaped" string literal, which are an extension to the SQL standard: i.e: e'first \n second' or E 'first \n second'
    EscapedStringLiteral(String),
    /// Hexadecimal string literal: i.e.: X'deadbeef'
    HexStringLiteral(String),
    /// Comma
    Comma,
    /// Whitespace (space, tab, etc)
    Whitespace(Whitespace),
    /// Double equals sign `==`
    DoubleEq,
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
    /// Spaceship operator <=>
    Spaceship,
    /// Plus operator `+`
    Plus,
    /// Minus operator `-`
    Minus,
    /// Multiplication operator `*`
    Mul,
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
    /// Tilde `~` used for PostgreSQL Bitwise NOT operator or case sensitive match regular expression operator
    Tilde,
    /// `~*` , a case insensitive match regular expression operator in PostgreSQL
    TildeAsterisk,
    /// `!~` , a case sensitive not match regular expression operator in PostgreSQL
    ExclamationMarkTilde,
    /// `!~*` , a case insensitive not match regular expression operator in PostgreSQL
    ExclamationMarkTildeAsterisk,
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
    /// `?` or `$` , a prepared statement arg placeholder
    Placeholder(String),
    /// ->, used as a operator to extract json field in PostgreSQL
    Arrow,
    /// ->>, used as a operator to extract json field as text in PostgreSQL
    LongArrow,
    /// #> Extracts JSON sub-object at the specified path
    HashArrow,
    /// #>> Extracts JSON sub-object at the specified path as text
    HashLongArrow,
    /// jsonb @> jsonb -> boolean: Test whether left json contains the right json
    AtArrow,
    /// jsonb <@ jsonb -> boolean: Test whether right json contains the left json
    ArrowAt,
    /// jsonb #- text[] -> jsonb: Deletes the field or array element at the specified
    /// path, where path elements can be either field keys or array indexes.
    HashMinus,
    /// jsonb @? jsonpath -> boolean: Does JSON path return any item for the specified
    /// JSON value?
    AtQuestion,
    /// jsonb @@ jsonpath → boolean: Returns the result of a JSON path predicate check
    /// for the specified JSON value. Only the first item of the result is taken into
    /// account. If the result is not Boolean, then NULL is returned.
    AtAt,
}

impl fmt::Display for Token {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Token::EOF => f.write_str("EOF"),
            Token::Word(ref w) => write!(f, "{}", w),
            Token::Number(ref n, l) => write!(f, "{}{long}", n, long = if *l { "L" } else { "" }),
            Token::Char(ref c) => write!(f, "{}", c),
            Token::SingleQuotedString(ref s) => write!(f, "'{}'", s),
            Token::DoubleQuotedString(ref s) => write!(f, "\"{}\"", s),
            Token::DollarQuotedString(ref s) => write!(f, "{}", s),
            Token::NationalStringLiteral(ref s) => write!(f, "N'{}'", s),
            Token::EscapedStringLiteral(ref s) => write!(f, "E'{}'", s),
            Token::HexStringLiteral(ref s) => write!(f, "X'{}'", s),
            Token::Comma => f.write_str(","),
            Token::Whitespace(ws) => write!(f, "{}", ws),
            Token::DoubleEq => f.write_str("=="),
            Token::Spaceship => f.write_str("<=>"),
            Token::Eq => f.write_str("="),
            Token::Neq => f.write_str("<>"),
            Token::Lt => f.write_str("<"),
            Token::Gt => f.write_str(">"),
            Token::LtEq => f.write_str("<="),
            Token::GtEq => f.write_str(">="),
            Token::Plus => f.write_str("+"),
            Token::Minus => f.write_str("-"),
            Token::Mul => f.write_str("*"),
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
            Token::TildeAsterisk => f.write_str("~*"),
            Token::ExclamationMarkTilde => f.write_str("!~"),
            Token::ExclamationMarkTildeAsterisk => f.write_str("!~*"),
            Token::AtSign => f.write_str("@"),
            Token::ShiftLeft => f.write_str("<<"),
            Token::ShiftRight => f.write_str(">>"),
            Token::PGSquareRoot => f.write_str("|/"),
            Token::PGCubeRoot => f.write_str("||/"),
            Token::Placeholder(ref s) => write!(f, "{}", s),
            Token::Arrow => write!(f, "->"),
            Token::LongArrow => write!(f, "->>"),
            Token::HashArrow => write!(f, "#>"),
            Token::HashLongArrow => write!(f, "#>>"),
            Token::AtArrow => write!(f, "@>"),
            Token::ArrowAt => write!(f, "<@"),
            Token::HashMinus => write!(f, "#-"),
            Token::AtQuestion => write!(f, "@?"),
            Token::AtAt => write!(f, "@@"),
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
            keyword: if quote_style.is_none() {
                let keyword = ALL_KEYWORDS.binary_search(&word_uppercase.as_str());
                keyword.map_or(Keyword::NoKeyword, |x| ALL_KEYWORDS_INDEX[x])
            } else {
                Keyword::NoKeyword
            },
        })
    }
}

/// A keyword (like SELECT) or an optionally quoted SQL identifier
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit))]
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

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit))]
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
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct Location {
    /// Line number, starting from 1
    pub line: u64,
    /// Line column, starting from 1
    pub column: u64,
}

/// A [Token] with [Location] attached to it
#[derive(Debug, Eq, PartialEq, Clone)]
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

    pub fn wrap(token: Token) -> TokenWithLocation {
        TokenWithLocation::new(token, 0, 0)
    }
}

impl PartialEq<Token> for TokenWithLocation {
    fn eq(&self, other: &Token) -> bool {
        &self.token == other
    }
}

impl PartialEq<TokenWithLocation> for Token {
    fn eq(&self, other: &TokenWithLocation) -> bool {
        self == &other.token
    }
}

impl fmt::Display for TokenWithLocation {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.token.fmt(f)
    }
}

/// Tokenizer error
#[derive(Debug, PartialEq, Eq)]
pub struct TokenizerError {
    pub message: String,
    pub line: u64,
    pub col: u64,
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
    query: &'a str,
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

    /// Tokenize the statement and produce a vector of tokens with location information
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
    fn next_token(&self, chars: &mut State) -> Result<Option<Token>, TokenizerError> {
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
                // Redshift uses lower case n for national string literal
                n @ 'N' | n @ 'n' => {
                    chars.next(); // consume, to check the next char
                    match chars.peek() {
                        Some('\'') => {
                            // N'...' - a <national character string literal>
                            let s = self.tokenize_quoted_string(chars, '\'')?;
                            Ok(Some(Token::NationalStringLiteral(s)))
                        }
                        _ => {
                            // regular identifier starting with an "N"
                            let s = self.tokenize_word(n, chars);
                            Ok(Some(Token::make_word(&s, None)))
                        }
                    }
                }
                // PostgreSQL accepts "escape" string constants, which are an extension to the SQL standard.
                x @ 'e' | x @ 'E' => {
                    let starting_loc = chars.location();
                    chars.next(); // consume, to check the next char
                    match chars.peek() {
                        Some('\'') => {
                            let s =
                                self.tokenize_escaped_single_quoted_string(starting_loc, chars)?;
                            Ok(Some(Token::EscapedStringLiteral(s)))
                        }
                        _ => {
                            // regular identifier starting with an "E" or "e"
                            let s = self.tokenize_word(x, chars);
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
                            let s = self.tokenize_quoted_string(chars, '\'')?;
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

                    // TODO: implement parsing of exponent here
                    if s.chars().all(|x| ('0'..='9').contains(&x) || x == '.') {
                        let mut inner_state = State {
                            peekable: s.chars().peekable(),
                            line: 0,
                            col: 0,
                        };
                        let mut s = peeking_take_while(&mut inner_state, |ch| {
                            matches!(ch, '0'..='9' | '.')
                        });
                        let s2 = peeking_take_while(chars, |ch| matches!(ch, '0'..='9' | '.'));
                        s += s2.as_str();
                        return Ok(Some(Token::Number(s, false)));
                    }
                    Ok(Some(Token::make_word(&s, None)))
                }
                // single quoted string
                '\'' => {
                    let s = self.tokenize_quoted_string(chars, '\'')?;

                    Ok(Some(Token::SingleQuotedString(s)))
                }
                // double quoted string
                '\"' if !self.dialect.is_delimited_identifier_start(ch)
                    && !self.dialect.is_identifier_start(ch) =>
                {
                    let s = self.tokenize_quoted_string(chars, '"')?;

                    Ok(Some(Token::DoubleQuotedString(s)))
                }
                // delimited (quoted) identifier
                quote_start
                    if self.dialect.is_delimited_identifier_start(ch)
                        && self
                            .dialect
                            .is_proper_identifier_inside_quotes(chars.peekable.clone()) =>
                {
                    let error_loc = chars.location();
                    chars.next(); // consume the opening quote
                    let quote_end = Word::matching_end_quote(quote_start);
                    let (s, last_char) = parse_quoted_ident(chars, quote_end);

                    if last_char == Some(quote_end) {
                        Ok(Some(Token::make_word(&s, Some(quote_start))))
                    } else {
                        self.tokenizer_error(
                            error_loc,
                            format!("Expected close delimiter '{}' before EOF.", quote_end),
                        )
                    }
                }
                // numbers and period
                '0'..='9' | '.' => {
                    let mut s = peeking_take_while(chars, |ch| matches!(ch, '0'..='9'));

                    // match binary literal that starts with 0x
                    if s == "0" && chars.peek() == Some(&'x') {
                        chars.next();
                        let s2 = peeking_take_while(
                            chars,
                            |ch| matches!(ch, '0'..='9' | 'A'..='F' | 'a'..='f'),
                        );
                        return Ok(Some(Token::HexStringLiteral(s2)));
                    }

                    // match one period
                    if let Some('.') = chars.peek() {
                        s.push('.');
                        chars.next();
                    }
                    s += &peeking_take_while(chars, |ch| matches!(ch, '0'..='9'));

                    // No number -> Token::Period
                    if s == "." {
                        return Ok(Some(Token::Period));
                    }

                    // Parse exponent as number
                    if chars.peek() == Some(&'e') || chars.peek() == Some(&'E') {
                        let mut char_clone = chars.peekable.clone();
                        let mut exponent_part = String::new();
                        exponent_part.push(char_clone.next().unwrap());

                        // Optional sign
                        match char_clone.peek() {
                            Some(&c) if matches!(c, '+' | '-') => {
                                exponent_part.push(c);
                                char_clone.next();
                            }
                            _ => (),
                        }

                        match char_clone.peek() {
                            // Definitely an exponent, get original iterator up to speed and use it
                            Some(&c) if matches!(c, '0'..='9') => {
                                for _ in 0..exponent_part.len() {
                                    chars.next();
                                }
                                exponent_part +=
                                    &peeking_take_while(chars, |ch| matches!(ch, '0'..='9'));
                                s += exponent_part.as_str();
                            }
                            // Not an exponent, discard the work done
                            _ => (),
                        }
                    }

                    let long = if chars.peek() == Some(&'L') {
                        chars.next();
                        true
                    } else {
                        false
                    };
                    Ok(Some(Token::Number(s, long)))
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
                        Some('>') => {
                            chars.next();
                            match chars.peek() {
                                Some('>') => {
                                    chars.next();
                                    Ok(Some(Token::LongArrow))
                                }
                                _ => Ok(Some(Token::Arrow)),
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
                        Some('/') if dialect_of!(self is SnowflakeDialect) => {
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
                '*' => self.consume_and_return(chars, Token::Mul),
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
                        Some('~') => {
                            chars.next();
                            match chars.peek() {
                                Some('*') => self
                                    .consume_and_return(chars, Token::ExclamationMarkTildeAsterisk),
                                _ => Ok(Some(Token::ExclamationMarkTilde)),
                            }
                        }
                        _ => Ok(Some(Token::ExclamationMark)),
                    }
                }
                '<' => {
                    chars.next(); // consume
                    match chars.peek() {
                        Some('=') => {
                            chars.next();
                            match chars.peek() {
                                Some('>') => self.consume_and_return(chars, Token::Spaceship),
                                _ => Ok(Some(Token::LtEq)),
                            }
                        }
                        Some('>') => self.consume_and_return(chars, Token::Neq),
                        Some('<') => self.consume_and_return(chars, Token::ShiftLeft),
                        Some('@') => self.consume_and_return(chars, Token::ArrowAt),
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
                '#' if dialect_of!(self is SnowflakeDialect) => {
                    chars.next(); // consume the '#', starting a snowflake single-line comment
                    let comment = self.tokenize_single_line_comment(chars);
                    Ok(Some(Token::Whitespace(Whitespace::SingleLineComment {
                        prefix: "#".to_owned(),
                        comment,
                    })))
                }
                '~' => {
                    chars.next(); // consume
                    match chars.peek() {
                        Some('*') => self.consume_and_return(chars, Token::TildeAsterisk),
                        _ => Ok(Some(Token::Tilde)),
                    }
                }
                '#' => {
                    chars.next();
                    match chars.peek() {
                        Some('-') => self.consume_and_return(chars, Token::HashMinus),
                        Some('>') => {
                            chars.next();
                            match chars.peek() {
                                Some('>') => {
                                    chars.next();
                                    Ok(Some(Token::HashLongArrow))
                                }
                                _ => Ok(Some(Token::HashArrow)),
                            }
                        }
                        _ => Ok(Some(Token::Sharp)),
                    }
                }
                '@' => {
                    chars.next();
                    match chars.peek() {
                        Some('>') => self.consume_and_return(chars, Token::AtArrow),
                        Some('?') => self.consume_and_return(chars, Token::AtQuestion),
                        Some('@') => self.consume_and_return(chars, Token::AtAt),
                        _ => Ok(Some(Token::AtSign)),
                    }
                }
                '?' => {
                    chars.next();
                    let s = peeking_take_while(chars, |ch| ch.is_numeric());
                    Ok(Some(Token::Placeholder(String::from("?") + &s)))
                }
                '$' => Ok(Some(self.tokenize_dollar_preceded_value(chars)?)),

                //whitespace check (including unicode chars) should be last as it covers some of the chars above
                ch if ch.is_whitespace() => {
                    self.consume_and_return(chars, Token::Whitespace(Whitespace::Space))
                }
                other => self.consume_and_return(chars, Token::Char(other)),
            },
            None => Ok(None),
        }
    }

    /// Tokenize dollar preceded value (i.e: a string/placeholder)
    fn tokenize_dollar_preceded_value(&self, chars: &mut State) -> Result<Token, TokenizerError> {
        let mut s = String::new();
        let mut value = String::new();

        chars.next();

        if let Some('$') = chars.peek() {
            chars.next();

            let mut is_terminated = false;
            let mut prev: Option<char> = None;

            while let Some(&ch) = chars.peek() {
                if prev == Some('$') {
                    if ch == '$' {
                        chars.next();
                        is_terminated = true;
                        break;
                    } else {
                        s.push('$');
                        s.push(ch);
                    }
                } else if ch != '$' {
                    s.push(ch);
                }

                prev = Some(ch);
                chars.next();
            }

            return if chars.peek().is_none() && !is_terminated {
                self.tokenizer_error(chars.location(), "Unterminated dollar-quoted string")
            } else {
                Ok(Token::DollarQuotedString(DollarQuotedString {
                    value: s,
                    tag: None,
                }))
            };
        } else {
            value.push_str(&peeking_take_while(chars, |ch| {
                ch.is_alphanumeric() || ch == '_'
            }));

            if let Some('$') = chars.peek() {
                chars.next();
                s.push_str(&peeking_take_while(chars, |ch| ch != '$'));

                match chars.peek() {
                    Some('$') => {
                        chars.next();
                        for (_, c) in value.chars().enumerate() {
                            let next_char = chars.next();
                            if Some(c) != next_char {
                                return self.tokenizer_error(
                                    chars.location(),
                                    format!(
                                        "Unterminated dollar-quoted string at or near \"{}\"",
                                        value
                                    ),
                                );
                            }
                        }

                        if let Some('$') = chars.peek() {
                            chars.next();
                        } else {
                            return self.tokenizer_error(
                                chars.location(),
                                "Unterminated dollar-quoted string, expected $",
                            );
                        }
                    }
                    _ => {
                        return self.tokenizer_error(
                            chars.location(),
                            "Unterminated dollar-quoted, expected $",
                        );
                    }
                }
            } else {
                return Ok(Token::Placeholder(String::from("$") + &value));
            }
        }

        Ok(Token::DollarQuotedString(DollarQuotedString {
            value: s,
            tag: if value.is_empty() { None } else { Some(value) },
        }))
    }

    fn tokenizer_error<R>(
        &self,
        loc: Location,
        message: impl Into<String>,
    ) -> Result<R, TokenizerError> {
        Err(TokenizerError {
            message: message.into(),
            col: loc.column,
            line: loc.line,
        })
    }

    // Consume characters until newline
    fn tokenize_single_line_comment(&self, chars: &mut State) -> String {
        let mut comment = peeking_take_while(chars, |ch| ch != '\n');
        if let Some(ch) = chars.next() {
            assert_eq!(ch, '\n');
            comment.push(ch);
        }
        comment
    }

    /// Tokenize an identifier or keyword, after the first char is already consumed.
    fn tokenize_word(&self, first_char: char, chars: &mut State) -> String {
        let mut s = first_char.to_string();
        s.push_str(&peeking_take_while(chars, |ch| {
            self.dialect.is_identifier_part(ch)
        }));
        s
    }

    /// Read a single quoted string, starting with the opening quote.
    fn tokenize_escaped_single_quoted_string(
        &self,
        starting_loc: Location,
        chars: &mut State,
    ) -> Result<String, TokenizerError> {
        let mut s = String::new();

        // This case is a bit tricky

        chars.next(); // consume the opening quote

        // slash escaping
        let mut is_escaped = false;
        while let Some(&ch) = chars.peek() {
            macro_rules! escape_control_character {
                ($ESCAPED:expr) => {{
                    if is_escaped {
                        s.push($ESCAPED);
                        is_escaped = false;
                    } else {
                        s.push(ch);
                    }

                    chars.next();
                }};
            }

            match ch {
                '\'' => {
                    chars.next(); // consume
                    if is_escaped {
                        s.push(ch);
                        is_escaped = false;
                    } else if chars.peek().map(|c| *c == '\'').unwrap_or(false) {
                        s.push(ch);
                        chars.next();
                    } else {
                        return Ok(s);
                    }
                }
                '\\' => {
                    if is_escaped {
                        s.push('\\');
                        is_escaped = false;
                    } else {
                        is_escaped = true;
                    }

                    chars.next();
                }
                'r' => escape_control_character!('\r'),
                'n' => escape_control_character!('\n'),
                't' => escape_control_character!('\t'),
                _ => {
                    is_escaped = false;
                    chars.next(); // consume
                    s.push(ch);
                }
            }
        }
        self.tokenizer_error(starting_loc, "Unterminated encoded string literal")
    }

    /// Read a single quoted string, starting with the opening quote.
    fn tokenize_quoted_string(
        &self,
        chars: &mut State,
        quote_style: char,
    ) -> Result<String, TokenizerError> {
        let mut s = String::new();
        let error_loc = chars.location();

        chars.next(); // consume the opening quote

        // slash escaping is specific to MySQL dialect
        let mut is_escaped = false;
        while let Some(&ch) = chars.peek() {
            match ch {
                char if char == quote_style => {
                    chars.next(); // consume
                    if is_escaped {
                        s.push(ch);
                        is_escaped = false;
                    } else if chars.peek().map(|c| *c == quote_style).unwrap_or(false) {
                        s.push(ch);
                        chars.next();
                    } else {
                        return Ok(s);
                    }
                }
                '\\' => {
                    if dialect_of!(self is MySqlDialect) {
                        is_escaped = !is_escaped;
                    } else {
                        s.push(ch);
                    }
                    chars.next();
                }
                _ => {
                    chars.next(); // consume
                    s.push(ch);
                }
            }
        }
        self.tokenizer_error(error_loc, "Unterminated string literal")
    }

    fn tokenize_multiline_comment(
        &self,
        chars: &mut State,
    ) -> Result<Option<Token>, TokenizerError> {
        let mut s = String::new();
        let mut nested = 1;
        let mut last_ch = ' ';

        loop {
            match chars.next() {
                Some(ch) => {
                    if last_ch == '/' && ch == '*' {
                        nested += 1;
                    } else if last_ch == '*' && ch == '/' {
                        nested -= 1;
                        if nested == 0 {
                            s.pop();
                            break Ok(Some(Token::Whitespace(Whitespace::MultiLineComment(s))));
                        }
                    }
                    s.push(ch);
                    last_ch = ch;
                }
                None => {
                    break self.tokenizer_error(
                        chars.location(),
                        "Unexpected EOF while in a multi-line comment",
                    )
                }
            }
        }
    }

    #[allow(clippy::unnecessary_wraps)]
    fn consume_and_return(
        &self,
        chars: &mut State,
        t: Token,
    ) -> Result<Option<Token>, TokenizerError> {
        chars.next();
        Ok(Some(t))
    }
}

/// Read from `chars` until `predicate` returns `false` or EOF is hit.
/// Return the characters read as String, and keep the first non-matching
/// char available as `chars.next()`.
fn peeking_take_while(chars: &mut State, mut predicate: impl FnMut(char) -> bool) -> String {
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

fn parse_quoted_ident(chars: &mut State, quote_end: char) -> (String, Option<char>) {
    let mut last_char = None;
    let mut s = String::new();
    while let Some(ch) = chars.next() {
        if ch == quote_end {
            if chars.peek() == Some(&quote_end) {
                chars.next();
                s.push(ch);
            } else {
                last_char = Some(quote_end);
                break;
            }
        } else {
            s.push(ch);
        }
    }
    (s, last_char)
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
            Token::Whitespace(Whitespace::Space),
            Token::Number(String::from("1"), false),
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
            Token::Whitespace(Whitespace::Space),
            Token::Number(String::from(".1"), false),
        ];

        compare(expected, tokens);
    }

    #[test]
    fn tokenize_select_exponent() {
        let sql = String::from("SELECT 1e10, 1e-10, 1e+10, 1ea, 1e-10a, 1e-10-10");
        let dialect = GenericDialect {};
        let mut tokenizer = Tokenizer::new(&dialect, &sql);
        let tokens = tokenizer.tokenize().unwrap();

        let expected = vec![
            Token::make_keyword("SELECT"),
            Token::Whitespace(Whitespace::Space),
            Token::Number(String::from("1e10"), false),
            Token::Comma,
            Token::Whitespace(Whitespace::Space),
            Token::Number(String::from("1e-10"), false),
            Token::Comma,
            Token::Whitespace(Whitespace::Space),
            Token::Number(String::from("1e+10"), false),
            Token::Comma,
            Token::Whitespace(Whitespace::Space),
            Token::Number(String::from("1"), false),
            Token::make_word("ea", None),
            Token::Comma,
            Token::Whitespace(Whitespace::Space),
            Token::Number(String::from("1e-10"), false),
            Token::make_word("a", None),
            Token::Comma,
            Token::Whitespace(Whitespace::Space),
            Token::Number(String::from("1e-10"), false),
            Token::Minus,
            Token::Number(String::from("10"), false),
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
            Token::Number(String::from("1"), false),
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
    fn tokenize_logical_xor() {
        let sql =
            String::from("SELECT true XOR true, false XOR false, true XOR false, false XOR true");
        let dialect = GenericDialect {};
        let mut tokenizer = Tokenizer::new(&dialect, &sql);
        let tokens = tokenizer.tokenize().unwrap();

        let expected = vec![
            Token::make_keyword("SELECT"),
            Token::Whitespace(Whitespace::Space),
            Token::make_keyword("true"),
            Token::Whitespace(Whitespace::Space),
            Token::make_keyword("XOR"),
            Token::Whitespace(Whitespace::Space),
            Token::make_keyword("true"),
            Token::Comma,
            Token::Whitespace(Whitespace::Space),
            Token::make_keyword("false"),
            Token::Whitespace(Whitespace::Space),
            Token::make_keyword("XOR"),
            Token::Whitespace(Whitespace::Space),
            Token::make_keyword("false"),
            Token::Comma,
            Token::Whitespace(Whitespace::Space),
            Token::make_keyword("true"),
            Token::Whitespace(Whitespace::Space),
            Token::make_keyword("XOR"),
            Token::Whitespace(Whitespace::Space),
            Token::make_keyword("false"),
            Token::Comma,
            Token::Whitespace(Whitespace::Space),
            Token::make_keyword("false"),
            Token::Whitespace(Whitespace::Space),
            Token::make_keyword("XOR"),
            Token::Whitespace(Whitespace::Space),
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
            Token::Whitespace(Whitespace::Space),
            Token::Mul,
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
            Token::Number(String::from("1"), false),
            Token::Whitespace(Whitespace::Space),
            Token::make_keyword("LIMIT"),
            Token::Whitespace(Whitespace::Space),
            Token::Number(String::from("5"), false),
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
            Token::Mul,
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
            Token::Number(String::from("1"), false),
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
            Token::Mul,
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
            Token::Number(String::from("1"), false),
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
            Token::Mul,
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
        // println!("tokens: {:#?}", tokens);
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
    fn tokenize_unterminated_string_literal_utf8() {
        let sql = String::from("SELECT \"なにか\" FROM Y WHERE \"なにか\" = 'test;");

        let dialect = GenericDialect {};
        let mut tokenizer = Tokenizer::new(&dialect, &sql);
        assert_eq!(
            tokenizer.tokenize(),
            Err(TokenizerError {
                message: "Unterminated string literal".to_string(),
                line: 1,
                col: 35
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
            Token::Whitespace(Whitespace::Newline),
            Token::Whitespace(Whitespace::Newline),
            Token::make_keyword("SELECT"),
            Token::Whitespace(Whitespace::Space),
            Token::Mul,
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
            Token::Number("0".to_string(), false),
            Token::Whitespace(Whitespace::SingleLineComment {
                prefix: "--".to_string(),
                comment: "this is a comment\n".to_string(),
            }),
            Token::Number("1".to_string(), false),
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
            Token::Number("0".to_string(), false),
            Token::Whitespace(Whitespace::MultiLineComment(
                "multi-line\n* /comment".to_string(),
            )),
            Token::Number("1".to_string(), false),
        ];
        compare(expected, tokens);
    }

    #[test]
    fn tokenize_nested_multiline_comment() {
        let sql = String::from("0/*multi-line\n* \n/* comment \n /*comment*/*/ */ /comment*/1");

        let dialect = GenericDialect {};
        let mut tokenizer = Tokenizer::new(&dialect, &sql);
        let tokens = tokenizer.tokenize().unwrap();
        let expected = vec![
            Token::Number("0".to_string(), false),
            Token::Whitespace(Whitespace::MultiLineComment(
                "multi-line\n* \n/* comment \n /*comment*/*/ */ /comment".to_string(),
            )),
            Token::Number("1".to_string(), false),
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
    fn tokenize_unicode_whitespace() {
        let sql = String::from(" \u{2003}\n");

        let dialect = GenericDialect {};
        let mut tokenizer = Tokenizer::new(&dialect, &sql);
        let tokens = tokenizer.tokenize().unwrap();
        let expected = vec![
            Token::Whitespace(Whitespace::Space),
            Token::Whitespace(Whitespace::Space),
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
            Token::Number(String::from("5"), false),
            Token::Whitespace(Whitespace::Space),
            Token::make_word("bar", Some('[')),
            Token::Whitespace(Whitespace::Space),
            Token::make_keyword("FROM"),
            Token::Whitespace(Whitespace::Space),
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
            Token::Whitespace(Whitespace::Space),
            Token::make_word("col", None),
            Token::Whitespace(Whitespace::Space),
            Token::Tilde,
            Token::Whitespace(Whitespace::Space),
            Token::SingleQuotedString("^a".into()),
            Token::Comma,
            Token::Whitespace(Whitespace::Space),
            Token::make_word("col", None),
            Token::Whitespace(Whitespace::Space),
            Token::TildeAsterisk,
            Token::Whitespace(Whitespace::Space),
            Token::SingleQuotedString("^a".into()),
            Token::Comma,
            Token::Whitespace(Whitespace::Space),
            Token::make_word("col", None),
            Token::Whitespace(Whitespace::Space),
            Token::ExclamationMarkTilde,
            Token::Whitespace(Whitespace::Space),
            Token::SingleQuotedString("^a".into()),
            Token::Comma,
            Token::Whitespace(Whitespace::Space),
            Token::make_word("col", None),
            Token::Whitespace(Whitespace::Space),
            Token::ExclamationMarkTildeAsterisk,
            Token::Whitespace(Whitespace::Space),
            Token::SingleQuotedString("^a".into()),
        ];
        compare(expected, tokens);
    }

    #[test]
    fn tokenize_quoted_identifier() {
        let sql = r#" "a "" b" "a """ "c """"" "#;
        let dialect = GenericDialect {};
        let mut tokenizer = Tokenizer::new(&dialect, sql);
        let tokens = tokenizer.tokenize().unwrap();
        let expected = vec![
            Token::Whitespace(Whitespace::Space),
            Token::make_word(r#"a " b"#, Some('"')),
            Token::Whitespace(Whitespace::Space),
            Token::make_word(r#"a ""#, Some('"')),
            Token::Whitespace(Whitespace::Space),
            Token::make_word(r#"c """#, Some('"')),
            Token::Whitespace(Whitespace::Space),
        ];
        compare(expected, tokens);
    }

    #[test]
    fn tokenize_with_location() {
        let sql = "SELECT a,\n b";
        let dialect = GenericDialect {};
        let mut tokenizer = Tokenizer::new(&dialect, sql);
        let tokens = tokenizer.tokenize_with_location().unwrap();
        let expected = vec![
            TokenWithLocation::new(Token::make_keyword("SELECT"), 1, 1),
            TokenWithLocation::new(Token::Whitespace(Whitespace::Space), 1, 7),
            TokenWithLocation::new(Token::make_word("a", None), 1, 8),
            TokenWithLocation::new(Token::Comma, 1, 9),
            TokenWithLocation::new(Token::Whitespace(Whitespace::Newline), 1, 10),
            TokenWithLocation::new(Token::Whitespace(Whitespace::Space), 2, 1),
            TokenWithLocation::new(Token::make_word("b", None), 2, 2),
        ];
        compare(expected, tokens);
    }

    fn compare<T: PartialEq + std::fmt::Debug>(expected: Vec<T>, actual: Vec<T>) {
        //println!("------------------------------");
        //println!("tokens   = {:?}", actual);
        //println!("expected = {:?}", expected);
        //println!("------------------------------");
        assert_eq!(expected, actual);
    }
}

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
use sqlparser_derive::{Visit, VisitMut};

use crate::ast::DollarQuotedString;
use crate::dialect::{
    BigQueryDialect, DuckDbDialect, GenericDialect, HiveDialect, PostgreSqlDialect,
    SnowflakeDialect,
};
use crate::dialect::{Dialect, MySqlDialect};
use crate::keywords::{Keyword, ALL_KEYWORDS, ALL_KEYWORDS_INDEX};

/// SQL Token enumeration
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
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
    /// Byte string literal: i.e: b'string' or B'string' (note that some backends, such as
    /// PostgreSQL, may treat this syntax as a bit string literal instead, i.e: b'10010101')
    SingleQuotedByteStringLiteral(String),
    /// Byte string literal: i.e: b"string" or B"string"
    DoubleQuotedByteStringLiteral(String),
    /// Raw string literal: i.e: r'string' or R'string' or r"string" or R"string"
    RawStringLiteral(String),
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
    /// Integer division operator `//` in DuckDB
    DuckIntDiv,
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
    /// DoubleColon `::` (used for casting in PostgreSQL)
    DoubleColon,
    /// Assignment `:=` (used for keyword argument in DuckDB macros and some functions, and for variable declarations in DuckDB and Snowflake)
    Assignment,
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
    /// `~~`, a case sensitive match pattern operator in PostgreSQL
    DoubleTilde,
    /// `~~*`, a case insensitive match pattern operator in PostgreSQL
    DoubleTildeAsterisk,
    /// `!~~`, a case sensitive not match pattern operator in PostgreSQL
    ExclamationMarkDoubleTilde,
    /// `!~~*`, a case insensitive not match pattern operator in PostgreSQL
    ExclamationMarkDoubleTildeAsterisk,
    /// `<<`, a bitwise shift left operator in PostgreSQL
    ShiftLeft,
    /// `>>`, a bitwise shift right operator in PostgreSQL
    ShiftRight,
    /// `&&`, an overlap operator in PostgreSQL
    Overlap,
    /// Exclamation Mark `!` used for PostgreSQL factorial operator
    ExclamationMark,
    /// Double Exclamation Mark `!!` used for PostgreSQL prefix factorial operator
    DoubleExclamationMark,
    /// AtSign `@` used for PostgreSQL abs operator
    AtSign,
    /// `^@`, a "starts with" string operator in PostgreSQL
    CaretAt,
    /// `|/`, a square root math operator in PostgreSQL
    PGSquareRoot,
    /// `||/`, a cube root math operator in PostgreSQL
    PGCubeRoot,
    /// `?` or `$` , a prepared statement arg placeholder
    Placeholder(String),
    /// `->`, used as a operator to extract json field in PostgreSQL
    Arrow,
    /// `->>`, used as a operator to extract json field as text in PostgreSQL
    LongArrow,
    /// `#>`, extracts JSON sub-object at the specified path
    HashArrow,
    /// `#>>`, extracts JSON sub-object at the specified path as text
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
    /// jsonb ? text -> boolean: Checks whether the string exists as a top-level key within the
    /// jsonb object
    Question,
    /// jsonb ?& text[] -> boolean: Check whether all members of the text array exist as top-level
    /// keys within the jsonb object
    QuestionAnd,
    /// jsonb ?| text[] -> boolean: Check whether any member of the text array exists as top-level
    /// keys within the jsonb object
    QuestionPipe,
}

impl fmt::Display for Token {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Token::EOF => f.write_str("EOF"),
            Token::Word(ref w) => write!(f, "{w}"),
            Token::Number(ref n, l) => write!(f, "{}{long}", n, long = if *l { "L" } else { "" }),
            Token::Char(ref c) => write!(f, "{c}"),
            Token::SingleQuotedString(ref s) => write!(f, "'{s}'"),
            Token::DoubleQuotedString(ref s) => write!(f, "\"{s}\""),
            Token::DollarQuotedString(ref s) => write!(f, "{s}"),
            Token::NationalStringLiteral(ref s) => write!(f, "N'{s}'"),
            Token::EscapedStringLiteral(ref s) => write!(f, "E'{s}'"),
            Token::HexStringLiteral(ref s) => write!(f, "X'{s}'"),
            Token::SingleQuotedByteStringLiteral(ref s) => write!(f, "B'{s}'"),
            Token::DoubleQuotedByteStringLiteral(ref s) => write!(f, "B\"{s}\""),
            Token::RawStringLiteral(ref s) => write!(f, "R'{s}'"),
            Token::Comma => f.write_str(","),
            Token::Whitespace(ws) => write!(f, "{ws}"),
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
            Token::DuckIntDiv => f.write_str("//"),
            Token::StringConcat => f.write_str("||"),
            Token::Mod => f.write_str("%"),
            Token::LParen => f.write_str("("),
            Token::RParen => f.write_str(")"),
            Token::Period => f.write_str("."),
            Token::Colon => f.write_str(":"),
            Token::DoubleColon => f.write_str("::"),
            Token::Assignment => f.write_str(":="),
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
            Token::DoubleTilde => f.write_str("~~"),
            Token::DoubleTildeAsterisk => f.write_str("~~*"),
            Token::ExclamationMarkDoubleTilde => f.write_str("!~~"),
            Token::ExclamationMarkDoubleTildeAsterisk => f.write_str("!~~*"),
            Token::AtSign => f.write_str("@"),
            Token::CaretAt => f.write_str("^@"),
            Token::ShiftLeft => f.write_str("<<"),
            Token::ShiftRight => f.write_str(">>"),
            Token::Overlap => f.write_str("&&"),
            Token::PGSquareRoot => f.write_str("|/"),
            Token::PGCubeRoot => f.write_str("||/"),
            Token::Placeholder(ref s) => write!(f, "{s}"),
            Token::Arrow => write!(f, "->"),
            Token::LongArrow => write!(f, "->>"),
            Token::HashArrow => write!(f, "#>"),
            Token::HashLongArrow => write!(f, "#>>"),
            Token::AtArrow => write!(f, "@>"),
            Token::ArrowAt => write!(f, "<@"),
            Token::HashMinus => write!(f, "#-"),
            Token::AtQuestion => write!(f, "@?"),
            Token::AtAt => write!(f, "@@"),
            Token::Question => write!(f, "?"),
            Token::QuestionAnd => write!(f, "?&"),
            Token::QuestionPipe => write!(f, "?|"),
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
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
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
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
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
            Whitespace::SingleLineComment { prefix, comment } => write!(f, "{prefix}{comment}"),
            Whitespace::MultiLineComment(s) => write!(f, "/*{s}*/"),
        }
    }
}

/// Location in input string
#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub struct Location {
    /// Line number, starting from 1
    pub line: u64,
    /// Line column, starting from 1
    pub column: u64,
}

impl fmt::Display for Location {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.line == 0 {
            return Ok(());
        }
        write!(
            f,
            // TODO: use standard compiler location syntax (<path>:<line>:<col>)
            " at Line: {}, Column {}",
            self.line, self.column,
        )
    }
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
    pub location: Location,
}

impl fmt::Display for TokenizerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{}", self.message, self.location,)
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
    /// return the next character and advance the stream
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

    /// return the next character but do not advance the stream
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
    /// If true (the default), the tokenizer will un-escape literal
    /// SQL strings See [`Tokenizer::with_unescape`] for more details.
    unescape: bool,
}

impl<'a> Tokenizer<'a> {
    /// Create a new SQL tokenizer for the specified SQL statement
    ///
    /// ```
    /// # use sqlparser::tokenizer::{Token, Whitespace, Tokenizer};
    /// # use sqlparser::dialect::GenericDialect;
    /// # let dialect = GenericDialect{};
    /// let query = r#"SELECT 'foo'"#;
    ///
    /// // Parsing the query
    /// let tokens = Tokenizer::new(&dialect, &query).tokenize().unwrap();
    ///
    /// assert_eq!(tokens, vec![
    ///   Token::make_word("SELECT", None),
    ///   Token::Whitespace(Whitespace::Space),
    ///   Token::SingleQuotedString("foo".to_string()),
    /// ]);
    pub fn new(dialect: &'a dyn Dialect, query: &'a str) -> Self {
        Self {
            dialect,
            query,
            unescape: true,
        }
    }

    /// Set unescape mode
    ///
    /// When true (default) the tokenizer unescapes literal values
    /// (for example, `""` in SQL is unescaped to the literal `"`).
    ///
    /// When false, the tokenizer provides the raw strings as provided
    /// in the query.  This can be helpful for programs that wish to
    /// recover the *exact* original query text without normalizing
    /// the escaping
    ///
    /// # Example
    ///
    /// ```
    /// # use sqlparser::tokenizer::{Token, Tokenizer};
    /// # use sqlparser::dialect::GenericDialect;
    /// # let dialect = GenericDialect{};
    /// let query = r#""Foo "" Bar""#;
    /// let unescaped = Token::make_word(r#"Foo " Bar"#, Some('"'));
    /// let original  = Token::make_word(r#"Foo "" Bar"#, Some('"'));
    ///
    /// // Parsing with unescaping (default)
    /// let tokens = Tokenizer::new(&dialect, &query).tokenize().unwrap();
    /// assert_eq!(tokens, vec![unescaped]);
    ///
    /// // Parsing with unescape = false
    /// let tokens = Tokenizer::new(&dialect, &query)
    ///    .with_unescape(false)
    ///    .tokenize().unwrap();
    /// assert_eq!(tokens, vec![original]);
    /// ```
    pub fn with_unescape(mut self, unescape: bool) -> Self {
        self.unescape = unescape;
        self
    }

    /// Tokenize the statement and produce a vector of tokens
    pub fn tokenize(&mut self) -> Result<Vec<Token>, TokenizerError> {
        let twl = self.tokenize_with_location()?;
        Ok(twl.into_iter().map(|t| t.token).collect())
    }

    /// Tokenize the statement and produce a vector of tokens with location information
    pub fn tokenize_with_location(&mut self) -> Result<Vec<TokenWithLocation>, TokenizerError> {
        let mut tokens: Vec<TokenWithLocation> = vec![];
        self.tokenize_with_location_into_buf(&mut tokens)
            .map(|_| tokens)
    }

    /// Tokenize the statement and append tokens with location information into the provided buffer.
    /// If an error is thrown, the buffer will contain all tokens that were successfully parsed before the error.
    pub fn tokenize_with_location_into_buf(
        &mut self,
        buf: &mut Vec<TokenWithLocation>,
    ) -> Result<(), TokenizerError> {
        let mut state = State {
            peekable: self.query.chars().peekable(),
            line: 1,
            col: 1,
        };

        let mut location = state.location();
        while let Some(token) = self.next_token(&mut state)? {
            buf.push(TokenWithLocation { token, location });

            location = state.location();
        }
        Ok(())
    }

    // Tokenize the identifer or keywords in `ch`
    fn tokenize_identifier_or_keyword(
        &self,
        ch: impl IntoIterator<Item = char>,
        chars: &mut State,
    ) -> Result<Option<Token>, TokenizerError> {
        chars.next(); // consume the first char
        let ch: String = ch.into_iter().collect();
        let word = self.tokenize_word(ch, chars);

        // TODO: implement parsing of exponent here
        if word.chars().all(|x| x.is_ascii_digit() || x == '.') {
            let mut inner_state = State {
                peekable: word.chars().peekable(),
                line: 0,
                col: 0,
            };
            let mut s = peeking_take_while(&mut inner_state, |ch| matches!(ch, '0'..='9' | '.'));
            let s2 = peeking_take_while(chars, |ch| matches!(ch, '0'..='9' | '.'));
            s += s2.as_str();
            return Ok(Some(Token::Number(s, false)));
        }

        Ok(Some(Token::make_word(&word, None)))
    }

    /// Get the next token or return None
    fn next_token(&self, chars: &mut State) -> Result<Option<Token>, TokenizerError> {
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
                // BigQuery uses b or B for byte string literal
                b @ 'B' | b @ 'b' if dialect_of!(self is BigQueryDialect | GenericDialect) => {
                    chars.next(); // consume
                    match chars.peek() {
                        Some('\'') => {
                            let s = self.tokenize_quoted_string(chars, '\'', false)?;
                            Ok(Some(Token::SingleQuotedByteStringLiteral(s)))
                        }
                        Some('\"') => {
                            let s = self.tokenize_quoted_string(chars, '\"', false)?;
                            Ok(Some(Token::DoubleQuotedByteStringLiteral(s)))
                        }
                        _ => {
                            // regular identifier starting with an "b" or "B"
                            let s = self.tokenize_word(b, chars);
                            Ok(Some(Token::make_word(&s, None)))
                        }
                    }
                }
                // BigQuery uses r or R for raw string literal
                b @ 'R' | b @ 'r' if dialect_of!(self is BigQueryDialect | GenericDialect) => {
                    chars.next(); // consume
                    match chars.peek() {
                        Some('\'') => {
                            let s = self.tokenize_quoted_string(chars, '\'', false)?;
                            Ok(Some(Token::RawStringLiteral(s)))
                        }
                        Some('\"') => {
                            let s = self.tokenize_quoted_string(chars, '\"', false)?;
                            Ok(Some(Token::RawStringLiteral(s)))
                        }
                        _ => {
                            // regular identifier starting with an "r" or "R"
                            let s = self.tokenize_word(b, chars);
                            Ok(Some(Token::make_word(&s, None)))
                        }
                    }
                }
                // Redshift uses lower case n for national string literal
                n @ 'N' | n @ 'n' => {
                    chars.next(); // consume, to check the next char
                    match chars.peek() {
                        Some('\'') => {
                            // N'...' - a <national character string literal>
                            let s = self.tokenize_quoted_string(chars, '\'', true)?;
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
                            let s = self.tokenize_quoted_string(chars, '\'', true)?;
                            Ok(Some(Token::HexStringLiteral(s)))
                        }
                        _ => {
                            // regular identifier starting with an "X"
                            let s = self.tokenize_word(x, chars);
                            Ok(Some(Token::make_word(&s, None)))
                        }
                    }
                }
                // single quoted string
                '\'' => {
                    let s = self.tokenize_quoted_string(
                        chars,
                        '\'',
                        self.dialect.supports_string_literal_backslash_escape(),
                    )?;

                    Ok(Some(Token::SingleQuotedString(s)))
                }
                // double quoted string
                '\"' if !self.dialect.is_delimited_identifier_start(ch)
                    && !self.dialect.is_identifier_start(ch) =>
                {
                    let s = self.tokenize_quoted_string(
                        chars,
                        '"',
                        self.dialect.supports_string_literal_backslash_escape(),
                    )?;

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
                    let (s, last_char) = self.parse_quoted_ident(chars, quote_end);

                    if last_char == Some(quote_end) {
                        Ok(Some(Token::make_word(&s, Some(quote_start))))
                    } else {
                        self.tokenizer_error(
                            error_loc,
                            format!("Expected close delimiter '{quote_end}' before EOF."),
                        )
                    }
                }
                // numbers and period
                '0'..='9' | '.' => {
                    let mut s = peeking_take_while(chars, |ch| ch.is_ascii_digit());

                    // match binary literal that starts with 0x
                    if s == "0" && chars.peek() == Some(&'x') {
                        chars.next();
                        let s2 = peeking_take_while(chars, |ch| ch.is_ascii_hexdigit());
                        return Ok(Some(Token::HexStringLiteral(s2)));
                    }

                    // match one period
                    if let Some('.') = chars.peek() {
                        s.push('.');
                        chars.next();
                    }
                    s += &peeking_take_while(chars, |ch| ch.is_ascii_digit());

                    // No number -> Token::Period
                    if s == "." {
                        return Ok(Some(Token::Period));
                    }

                    let mut exponent_part = String::new();
                    // Parse exponent as number
                    if chars.peek() == Some(&'e') || chars.peek() == Some(&'E') {
                        let mut char_clone = chars.peekable.clone();
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
                            Some(&c) if c.is_ascii_digit() => {
                                for _ in 0..exponent_part.len() {
                                    chars.next();
                                }
                                exponent_part +=
                                    &peeking_take_while(chars, |ch| ch.is_ascii_digit());
                                s += exponent_part.as_str();
                            }
                            // Not an exponent, discard the work done
                            _ => (),
                        }
                    }

                    // mysql dialect supports identifiers that start with a numeric prefix,
                    // as long as they aren't an exponent number.
                    if (dialect_of!(self is MySqlDialect | HiveDialect)
                        || self.dialect.supports_numeric_prefix())
                        && exponent_part.is_empty()
                    {
                        let word =
                            peeking_take_while(chars, |ch| self.dialect.is_identifier_part(ch));

                        if !word.is_empty() {
                            s += word.as_str();
                            return Ok(Some(Token::make_word(s.as_str(), None)));
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
                        Some('/') if dialect_of!(self is DuckDbDialect | GenericDialect) => {
                            self.consume_and_return(chars, Token::DuckIntDiv)
                        }
                        // a regular '/' operator
                        _ => Ok(Some(Token::Div)),
                    }
                }
                '+' => self.consume_and_return(chars, Token::Plus),
                '*' => self.consume_and_return(chars, Token::Mul),
                '%' => {
                    chars.next(); // advance past '%'
                    match chars.peek() {
                        Some(' ') => Ok(Some(Token::Mod)),
                        Some(sch) if self.dialect.is_identifier_start('%') => {
                            self.tokenize_identifier_or_keyword([ch, *sch], chars)
                        }
                        _ => Ok(Some(Token::Mod)),
                    }
                }
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
                        Some('=') => self.consume_and_return(chars, Token::DoubleEq),
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
                                Some('~') => {
                                    chars.next();
                                    match chars.peek() {
                                        Some('*') => self.consume_and_return(
                                            chars,
                                            Token::ExclamationMarkDoubleTildeAsterisk,
                                        ),
                                        _ => Ok(Some(Token::ExclamationMarkDoubleTilde)),
                                    }
                                }
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
                        Some('=') => self.consume_and_return(chars, Token::Assignment),
                        _ => Ok(Some(Token::Colon)),
                    }
                }
                ';' => self.consume_and_return(chars, Token::SemiColon),
                '\\' => self.consume_and_return(chars, Token::Backslash),
                '[' => self.consume_and_return(chars, Token::LBracket),
                ']' => self.consume_and_return(chars, Token::RBracket),
                '&' => {
                    chars.next(); // consume the '&'
                    match chars.peek() {
                        Some('&') => self.consume_and_return(chars, Token::Overlap),
                        // Bitshift '&' operator
                        _ => Ok(Some(Token::Ampersand)),
                    }
                }
                '^' => {
                    chars.next(); // consume the '^'
                    match chars.peek() {
                        Some('@') => self.consume_and_return(chars, Token::CaretAt),
                        _ => Ok(Some(Token::Caret)),
                    }
                }
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
                '~' => {
                    chars.next(); // consume
                    match chars.peek() {
                        Some('*') => self.consume_and_return(chars, Token::TildeAsterisk),
                        Some('~') => {
                            chars.next();
                            match chars.peek() {
                                Some('*') => {
                                    self.consume_and_return(chars, Token::DoubleTildeAsterisk)
                                }
                                _ => Ok(Some(Token::DoubleTilde)),
                            }
                        }
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
                        Some(' ') => Ok(Some(Token::Sharp)),
                        Some(sch) if self.dialect.is_identifier_start('#') => {
                            self.tokenize_identifier_or_keyword([ch, *sch], chars)
                        }
                        _ => Ok(Some(Token::Sharp)),
                    }
                }
                '@' => {
                    chars.next();
                    match chars.peek() {
                        Some('>') => self.consume_and_return(chars, Token::AtArrow),
                        Some('?') => self.consume_and_return(chars, Token::AtQuestion),
                        Some('@') => {
                            chars.next();
                            match chars.peek() {
                                Some(' ') => Ok(Some(Token::AtAt)),
                                Some(tch) if self.dialect.is_identifier_start('@') => {
                                    self.tokenize_identifier_or_keyword([ch, '@', *tch], chars)
                                }
                                _ => Ok(Some(Token::AtAt)),
                            }
                        }
                        Some(' ') => Ok(Some(Token::AtSign)),
                        Some(sch) if self.dialect.is_identifier_start('@') => {
                            self.tokenize_identifier_or_keyword([ch, *sch], chars)
                        }
                        _ => Ok(Some(Token::AtSign)),
                    }
                }
                // Postgres uses ? for jsonb operators, not prepared statements
                '?' if dialect_of!(self is PostgreSqlDialect) => {
                    chars.next();
                    match chars.peek() {
                        Some('|') => self.consume_and_return(chars, Token::QuestionPipe),
                        Some('&') => self.consume_and_return(chars, Token::QuestionAnd),
                        _ => self.consume_and_return(chars, Token::Question),
                    }
                }
                '?' => {
                    chars.next();
                    let s = peeking_take_while(chars, |ch| ch.is_numeric());
                    Ok(Some(Token::Placeholder(String::from("?") + &s)))
                }

                // identifier or keyword
                ch if self.dialect.is_identifier_start(ch) => {
                    self.tokenize_identifier_or_keyword([ch], chars)
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

                'searching_for_end: loop {
                    s.push_str(&peeking_take_while(chars, |ch| ch != '$'));
                    match chars.peek() {
                        Some('$') => {
                            chars.next();
                            let mut maybe_s = String::from("$");
                            for c in value.chars() {
                                if let Some(next_char) = chars.next() {
                                    maybe_s.push(next_char);
                                    if next_char != c {
                                        // This doesn't match the dollar quote delimiter so this
                                        // is not the end of the string.
                                        s.push_str(&maybe_s);
                                        continue 'searching_for_end;
                                    }
                                } else {
                                    return self.tokenizer_error(
                                        chars.location(),
                                        "Unterminated dollar-quoted, expected $",
                                    );
                                }
                            }
                            if chars.peek() == Some(&'$') {
                                chars.next();
                                maybe_s.push('$');
                                // maybe_s matches the end delimiter
                                break 'searching_for_end;
                            } else {
                                // This also doesn't match the dollar quote delimiter as there are
                                // more characters before the second dollar so this is not the end
                                // of the string.
                                s.push_str(&maybe_s);
                                continue 'searching_for_end;
                            }
                        }
                        _ => {
                            return self.tokenizer_error(
                                chars.location(),
                                "Unterminated dollar-quoted, expected $",
                            )
                        }
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
            location: loc,
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
    fn tokenize_word(&self, first_chars: impl Into<String>, chars: &mut State) -> String {
        let mut s = first_chars.into();
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
        if let Some(s) = unescape_single_quoted_string(chars) {
            return Ok(s);
        }

        self.tokenizer_error(starting_loc, "Unterminated encoded string literal")
    }

    /// Read a single quoted string, starting with the opening quote.
    fn tokenize_quoted_string(
        &self,
        chars: &mut State,
        quote_style: char,
        allow_escape: bool,
    ) -> Result<String, TokenizerError> {
        let mut s = String::new();
        let error_loc = chars.location();

        chars.next(); // consume the opening quote

        while let Some(&ch) = chars.peek() {
            match ch {
                char if char == quote_style => {
                    chars.next(); // consume
                    if chars.peek().map(|c| *c == quote_style).unwrap_or(false) {
                        s.push(ch);
                        if !self.unescape {
                            // In no-escape mode, the given query has to be saved completely
                            s.push(ch);
                        }
                        chars.next();
                    } else {
                        return Ok(s);
                    }
                }
                '\\' if allow_escape => {
                    // consume backslash
                    chars.next();

                    if let Some(next) = chars.peek() {
                        if !self.unescape {
                            // In no-escape mode, the given query has to be saved completely including backslashes.
                            s.push(ch);
                            s.push(*next);
                            chars.next(); // consume next
                        } else {
                            let n = match next {
                                '0' => '\0',
                                'a' => '\u{7}',
                                'b' => '\u{8}',
                                'f' => '\u{c}',
                                'n' => '\n',
                                'r' => '\r',
                                't' => '\t',
                                'Z' => '\u{1a}',
                                _ => *next,
                            };
                            s.push(n);
                            chars.next(); // consume next
                        }
                    }
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

    fn parse_quoted_ident(&self, chars: &mut State, quote_end: char) -> (String, Option<char>) {
        let mut last_char = None;
        let mut s = String::new();
        while let Some(ch) = chars.next() {
            if ch == quote_end {
                if chars.peek() == Some(&quote_end) {
                    chars.next();
                    s.push(ch);
                    if !self.unescape {
                        // In no-escape mode, the given query has to be saved completely
                        s.push(ch);
                    }
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

fn unescape_single_quoted_string(chars: &mut State<'_>) -> Option<String> {
    Unescape::new(chars).unescape()
}

struct Unescape<'a: 'b, 'b> {
    chars: &'b mut State<'a>,
}

impl<'a: 'b, 'b> Unescape<'a, 'b> {
    fn new(chars: &'b mut State<'a>) -> Self {
        Self { chars }
    }
    fn unescape(mut self) -> Option<String> {
        let mut unescaped = String::new();

        self.chars.next();

        while let Some(c) = self.chars.next() {
            if c == '\'' {
                // case: ''''
                if self.chars.peek().map(|c| *c == '\'').unwrap_or(false) {
                    self.chars.next();
                    unescaped.push('\'');
                    continue;
                }
                return Some(unescaped);
            }

            if c != '\\' {
                unescaped.push(c);
                continue;
            }

            let c = match self.chars.next()? {
                'b' => '\u{0008}',
                'f' => '\u{000C}',
                'n' => '\n',
                'r' => '\r',
                't' => '\t',
                'u' => self.unescape_unicode_16()?,
                'U' => self.unescape_unicode_32()?,
                'x' => self.unescape_hex()?,
                c if c.is_digit(8) => self.unescape_octal(c)?,
                c => c,
            };

            unescaped.push(Self::check_null(c)?);
        }

        None
    }

    #[inline]
    fn check_null(c: char) -> Option<char> {
        if c == '\0' {
            None
        } else {
            Some(c)
        }
    }

    #[inline]
    fn byte_to_char<const RADIX: u32>(s: &str) -> Option<char> {
        // u32 is used here because Pg has an overflow operation rather than throwing an exception directly.
        match u32::from_str_radix(s, RADIX) {
            Err(_) => None,
            Ok(n) => {
                let n = n & 0xFF;
                if n <= 127 {
                    char::from_u32(n)
                } else {
                    None
                }
            }
        }
    }

    // Hexadecimal byte value. \xh, \xhh (h = 0–9, A–F)
    fn unescape_hex(&mut self) -> Option<char> {
        let mut s = String::new();

        for _ in 0..2 {
            match self.next_hex_digit() {
                Some(c) => s.push(c),
                None => break,
            }
        }

        if s.is_empty() {
            return Some('x');
        }

        Self::byte_to_char::<16>(&s)
    }

    #[inline]
    fn next_hex_digit(&mut self) -> Option<char> {
        match self.chars.peek() {
            Some(c) if c.is_ascii_hexdigit() => self.chars.next(),
            _ => None,
        }
    }

    // Octal byte value. \o, \oo, \ooo (o = 0–7)
    fn unescape_octal(&mut self, c: char) -> Option<char> {
        let mut s = String::new();

        s.push(c);
        for _ in 0..2 {
            match self.next_octal_digest() {
                Some(c) => s.push(c),
                None => break,
            }
        }

        Self::byte_to_char::<8>(&s)
    }

    #[inline]
    fn next_octal_digest(&mut self) -> Option<char> {
        match self.chars.peek() {
            Some(c) if c.is_digit(8) => self.chars.next(),
            _ => None,
        }
    }

    // 16-bit hexadecimal Unicode character value. \uxxxx (x = 0–9, A–F)
    fn unescape_unicode_16(&mut self) -> Option<char> {
        self.unescape_unicode::<4>()
    }

    // 32-bit hexadecimal Unicode character value. \Uxxxxxxxx (x = 0–9, A–F)
    fn unescape_unicode_32(&mut self) -> Option<char> {
        self.unescape_unicode::<8>()
    }

    fn unescape_unicode<const NUM: usize>(&mut self) -> Option<char> {
        let mut s = String::new();
        for _ in 0..NUM {
            s.push(self.chars.next()?);
        }
        match u32::from_str_radix(&s, 16) {
            Err(_) => None,
            Ok(n) => char::from_u32(n),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dialect::{BigQueryDialect, ClickHouseDialect, MsSqlDialect};
    use core::fmt::Debug;

    #[test]
    fn tokenizer_error_impl() {
        let err = TokenizerError {
            message: "test".into(),
            location: Location { line: 1, column: 1 },
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
        let tokens = Tokenizer::new(&dialect, &sql).tokenize().unwrap();

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
        let tokens = Tokenizer::new(&dialect, &sql).tokenize().unwrap();

        let expected = vec![
            Token::make_keyword("SELECT"),
            Token::Whitespace(Whitespace::Space),
            Token::Number(String::from(".1"), false),
        ];

        compare(expected, tokens);
    }

    #[test]
    fn tokenize_clickhouse_double_equal() {
        let sql = String::from("SELECT foo=='1'");
        let dialect = ClickHouseDialect {};
        let mut tokenizer = Tokenizer::new(&dialect, &sql);
        let tokens = tokenizer.tokenize().unwrap();

        let expected = vec![
            Token::make_keyword("SELECT"),
            Token::Whitespace(Whitespace::Space),
            Token::Word(Word {
                value: "foo".to_string(),
                quote_style: None,
                keyword: Keyword::NoKeyword,
            }),
            Token::DoubleEq,
            Token::SingleQuotedString("1".to_string()),
        ];

        compare(expected, tokens);
    }

    #[test]
    fn tokenize_select_exponent() {
        let sql = String::from("SELECT 1e10, 1e-10, 1e+10, 1ea, 1e-10a, 1e-10-10");
        let dialect = GenericDialect {};
        let tokens = Tokenizer::new(&dialect, &sql).tokenize().unwrap();

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
        let tokens = Tokenizer::new(&dialect, &sql).tokenize().unwrap();

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
        let tokens = Tokenizer::new(&dialect, &sql).tokenize().unwrap();

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
        let tokens = Tokenizer::new(&dialect, &sql).tokenize().unwrap();

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
        let tokens = Tokenizer::new(&dialect, &sql).tokenize().unwrap();

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
        let tokens = Tokenizer::new(&dialect, &sql).tokenize().unwrap();

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
        let tokens = Tokenizer::new(&dialect, &sql).tokenize().unwrap();

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
        let tokens = Tokenizer::new(&dialect, &sql).tokenize().unwrap();

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
        let tokens = Tokenizer::new(&dialect, &sql).tokenize().unwrap();

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
        let sql = String::from("\n💝مصطفىh");

        let dialect = GenericDialect {};
        let tokens = Tokenizer::new(&dialect, &sql).tokenize().unwrap();
        // println!("tokens: {:#?}", tokens);
        let expected = vec![
            Token::Whitespace(Whitespace::Newline),
            Token::Char('💝'),
            Token::make_word("مصطفىh", None),
        ];
        compare(expected, tokens);
    }

    #[test]
    fn tokenize_newline_in_string_literal() {
        let sql = String::from("'foo\r\nbar\nbaz'");

        let dialect = GenericDialect {};
        let tokens = Tokenizer::new(&dialect, &sql).tokenize().unwrap();
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
                location: Location { line: 1, column: 8 },
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
                location: Location {
                    line: 1,
                    column: 35
                }
            })
        );
    }

    #[test]
    fn tokenize_invalid_string_cols() {
        let sql = String::from("\n\nSELECT * FROM table\t💝مصطفىh");

        let dialect = GenericDialect {};
        let tokens = Tokenizer::new(&dialect, &sql).tokenize().unwrap();
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
            Token::Char('💝'),
            Token::make_word("مصطفىh", None),
        ];
        compare(expected, tokens);
    }

    #[test]
    fn tokenize_dollar_quoted_string_tagged() {
        let sql = String::from(
            "SELECT $tag$dollar '$' quoted strings have $tags like this$ or like this $$$tag$",
        );
        let dialect = GenericDialect {};
        let tokens = Tokenizer::new(&dialect, &sql).tokenize().unwrap();
        let expected = vec![
            Token::make_keyword("SELECT"),
            Token::Whitespace(Whitespace::Space),
            Token::DollarQuotedString(DollarQuotedString {
                value: "dollar '$' quoted strings have $tags like this$ or like this $$".into(),
                tag: Some("tag".into()),
            }),
        ];
        compare(expected, tokens);
    }

    #[test]
    fn tokenize_dollar_quoted_string_tagged_unterminated() {
        let sql = String::from("SELECT $tag$dollar '$' quoted strings have $tags like this$ or like this $$$different tag$");
        let dialect = GenericDialect {};
        assert_eq!(
            Tokenizer::new(&dialect, &sql).tokenize(),
            Err(TokenizerError {
                message: "Unterminated dollar-quoted, expected $".into(),
                location: Location {
                    line: 1,
                    column: 91
                }
            })
        );
    }

    #[test]
    fn tokenize_dollar_quoted_string_untagged() {
        let sql =
            String::from("SELECT $$within dollar '$' quoted strings have $tags like this$ $$");
        let dialect = GenericDialect {};
        let tokens = Tokenizer::new(&dialect, &sql).tokenize().unwrap();
        let expected = vec![
            Token::make_keyword("SELECT"),
            Token::Whitespace(Whitespace::Space),
            Token::DollarQuotedString(DollarQuotedString {
                value: "within dollar '$' quoted strings have $tags like this$ ".into(),
                tag: None,
            }),
        ];
        compare(expected, tokens);
    }

    #[test]
    fn tokenize_dollar_quoted_string_untagged_unterminated() {
        let sql = String::from(
            "SELECT $$dollar '$' quoted strings have $tags like this$ or like this $different tag$",
        );
        let dialect = GenericDialect {};
        assert_eq!(
            Tokenizer::new(&dialect, &sql).tokenize(),
            Err(TokenizerError {
                message: "Unterminated dollar-quoted string".into(),
                location: Location {
                    line: 1,
                    column: 86
                }
            })
        );
    }

    #[test]
    fn tokenize_right_arrow() {
        let sql = String::from("FUNCTION(key=>value)");
        let dialect = GenericDialect {};
        let tokens = Tokenizer::new(&dialect, &sql).tokenize().unwrap();
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
        let tokens = Tokenizer::new(&dialect, &sql).tokenize().unwrap();

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
        let tokens = Tokenizer::new(&dialect, &sql).tokenize().unwrap();
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
        let tokens = Tokenizer::new(&dialect, &sql).tokenize().unwrap();
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
        let tokens = Tokenizer::new(&dialect, &sql).tokenize().unwrap();
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
        let tokens = Tokenizer::new(&dialect, &sql).tokenize().unwrap();
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
        let tokens = Tokenizer::new(&dialect, &sql).tokenize().unwrap();
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
        let tokens = Tokenizer::new(&dialect, &sql).tokenize().unwrap();
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
                location: Location { line: 1, column: 1 },
            })
        );
    }

    #[test]
    fn tokenize_newlines() {
        let sql = String::from("line1\nline2\rline3\r\nline4\r");

        let dialect = GenericDialect {};
        let tokens = Tokenizer::new(&dialect, &sql).tokenize().unwrap();
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
        let tokens = Tokenizer::new(&dialect, sql).tokenize().unwrap();
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
        let tokens = Tokenizer::new(&dialect, sql).tokenize().unwrap();
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
    fn tokenize_pg_like_match() {
        let sql = "SELECT col ~~ '_a%', col ~~* '_a%', col !~~ '_a%', col !~~* '_a%'";
        let dialect = GenericDialect {};
        let tokens = Tokenizer::new(&dialect, sql).tokenize().unwrap();
        let expected = vec![
            Token::make_keyword("SELECT"),
            Token::Whitespace(Whitespace::Space),
            Token::make_word("col", None),
            Token::Whitespace(Whitespace::Space),
            Token::DoubleTilde,
            Token::Whitespace(Whitespace::Space),
            Token::SingleQuotedString("_a%".into()),
            Token::Comma,
            Token::Whitespace(Whitespace::Space),
            Token::make_word("col", None),
            Token::Whitespace(Whitespace::Space),
            Token::DoubleTildeAsterisk,
            Token::Whitespace(Whitespace::Space),
            Token::SingleQuotedString("_a%".into()),
            Token::Comma,
            Token::Whitespace(Whitespace::Space),
            Token::make_word("col", None),
            Token::Whitespace(Whitespace::Space),
            Token::ExclamationMarkDoubleTilde,
            Token::Whitespace(Whitespace::Space),
            Token::SingleQuotedString("_a%".into()),
            Token::Comma,
            Token::Whitespace(Whitespace::Space),
            Token::make_word("col", None),
            Token::Whitespace(Whitespace::Space),
            Token::ExclamationMarkDoubleTildeAsterisk,
            Token::Whitespace(Whitespace::Space),
            Token::SingleQuotedString("_a%".into()),
        ];
        compare(expected, tokens);
    }

    #[test]
    fn tokenize_quoted_identifier() {
        let sql = r#" "a "" b" "a """ "c """"" "#;
        let dialect = GenericDialect {};
        let tokens = Tokenizer::new(&dialect, sql).tokenize().unwrap();
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
    fn tokenize_snowflake_div() {
        let sql = r#"field/1000"#;
        let dialect = SnowflakeDialect {};
        let tokens = Tokenizer::new(&dialect, sql).tokenize().unwrap();
        let expected = vec![
            Token::make_word(r#"field"#, None),
            Token::Div,
            Token::Number("1000".to_string(), false),
        ];
        compare(expected, tokens);
    }

    #[test]
    fn tokenize_quoted_identifier_with_no_escape() {
        let sql = r#" "a "" b" "a """ "c """"" "#;
        let dialect = GenericDialect {};
        let tokens = Tokenizer::new(&dialect, sql)
            .with_unescape(false)
            .tokenize()
            .unwrap();
        let expected = vec![
            Token::Whitespace(Whitespace::Space),
            Token::make_word(r#"a "" b"#, Some('"')),
            Token::Whitespace(Whitespace::Space),
            Token::make_word(r#"a """#, Some('"')),
            Token::Whitespace(Whitespace::Space),
            Token::make_word(r#"c """""#, Some('"')),
            Token::Whitespace(Whitespace::Space),
        ];
        compare(expected, tokens);
    }

    #[test]
    fn tokenize_with_location() {
        let sql = "SELECT a,\n b";
        let dialect = GenericDialect {};
        let tokens = Tokenizer::new(&dialect, sql)
            .tokenize_with_location()
            .unwrap();
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

    fn check_unescape(s: &str, expected: Option<&str>) {
        let s = format!("'{}'", s);
        let mut state = State {
            peekable: s.chars().peekable(),
            line: 0,
            col: 0,
        };

        assert_eq!(
            unescape_single_quoted_string(&mut state),
            expected.map(|s| s.to_string())
        );
    }

    #[test]
    fn test_unescape() {
        check_unescape(r"\b", Some("\u{0008}"));
        check_unescape(r"\f", Some("\u{000C}"));
        check_unescape(r"\t", Some("\t"));
        check_unescape(r"\r\n", Some("\r\n"));
        check_unescape(r"\/", Some("/"));
        check_unescape(r"/", Some("/"));
        check_unescape(r"\\", Some("\\"));

        // 16 and 32-bit hexadecimal Unicode character value
        check_unescape(r"\u0001", Some("\u{0001}"));
        check_unescape(r"\u4c91", Some("\u{4c91}"));
        check_unescape(r"\u4c916", Some("\u{4c91}6"));
        check_unescape(r"\u4c", None);
        check_unescape(r"\u0000", None);
        check_unescape(r"\U0010FFFF", Some("\u{10FFFF}"));
        check_unescape(r"\U00110000", None);
        check_unescape(r"\U00000000", None);
        check_unescape(r"\u", None);
        check_unescape(r"\U", None);
        check_unescape(r"\U1010FFFF", None);

        // hexadecimal byte value
        check_unescape(r"\x4B", Some("\u{004b}"));
        check_unescape(r"\x4", Some("\u{0004}"));
        check_unescape(r"\x4L", Some("\u{0004}L"));
        check_unescape(r"\x", Some("x"));
        check_unescape(r"\xP", Some("xP"));
        check_unescape(r"\x0", None);
        check_unescape(r"\xCAD", None);
        check_unescape(r"\xA9", None);

        // octal byte value
        check_unescape(r"\1", Some("\u{0001}"));
        check_unescape(r"\12", Some("\u{000a}"));
        check_unescape(r"\123", Some("\u{0053}"));
        check_unescape(r"\1232", Some("\u{0053}2"));
        check_unescape(r"\4", Some("\u{0004}"));
        check_unescape(r"\45", Some("\u{0025}"));
        check_unescape(r"\450", Some("\u{0028}"));
        check_unescape(r"\603", None);
        check_unescape(r"\0", None);
        check_unescape(r"\080", None);

        // others
        check_unescape(r"\9", Some("9"));
        check_unescape(r"''", Some("'"));
        check_unescape(
            r"Hello\r\nRust/\u4c91 SQL Parser\U0010ABCD\1232",
            Some("Hello\r\nRust/\u{4c91} SQL Parser\u{10abcd}\u{0053}2"),
        );
        check_unescape(r"Hello\0", None);
        check_unescape(r"Hello\xCADRust", None);
    }

    #[test]
    fn tokenize_numeric_prefix_trait() {
        #[derive(Debug)]
        struct NumericPrefixDialect;

        impl Dialect for NumericPrefixDialect {
            fn is_identifier_start(&self, ch: char) -> bool {
                ch.is_ascii_lowercase()
                    || ch.is_ascii_uppercase()
                    || ch.is_ascii_digit()
                    || ch == '$'
            }

            fn is_identifier_part(&self, ch: char) -> bool {
                ch.is_ascii_lowercase()
                    || ch.is_ascii_uppercase()
                    || ch.is_ascii_digit()
                    || ch == '_'
                    || ch == '$'
                    || ch == '{'
                    || ch == '}'
            }

            fn supports_numeric_prefix(&self) -> bool {
                true
            }
        }

        tokenize_numeric_prefix_inner(&NumericPrefixDialect {});
        tokenize_numeric_prefix_inner(&HiveDialect {});
        tokenize_numeric_prefix_inner(&MySqlDialect {});
    }

    fn tokenize_numeric_prefix_inner(dialect: &dyn Dialect) {
        let sql = r#"SELECT * FROM 1"#;
        let tokens = Tokenizer::new(dialect, sql).tokenize().unwrap();
        let expected = vec![
            Token::make_keyword("SELECT"),
            Token::Whitespace(Whitespace::Space),
            Token::Mul,
            Token::Whitespace(Whitespace::Space),
            Token::make_keyword("FROM"),
            Token::Whitespace(Whitespace::Space),
            Token::Number(String::from("1"), false),
        ];
        compare(expected, tokens);
    }

    #[test]
    fn tokenize_quoted_string_escape() {
        for (sql, expected, expected_unescaped) in [
            (r#"'%a\'%b'"#, r#"%a\'%b"#, r#"%a'%b"#),
            (r#"'a\'\'b\'c\'d'"#, r#"a\'\'b\'c\'d"#, r#"a''b'c'd"#),
            (r#"'\\'"#, r#"\\"#, r#"\"#),
            (
                r#"'\0\a\b\f\n\r\t\Z'"#,
                r#"\0\a\b\f\n\r\t\Z"#,
                "\0\u{7}\u{8}\u{c}\n\r\t\u{1a}",
            ),
            (r#"'\"'"#, r#"\""#, "\""),
            (r#"'\\a\\b\'c'"#, r#"\\a\\b\'c"#, r#"\a\b'c"#),
            (r#"'\'abcd'"#, r#"\'abcd"#, r#"'abcd"#),
            (r#"'''a''b'"#, r#"''a''b"#, r#"'a'b"#),
        ] {
            let dialect = BigQueryDialect {};

            let tokens = Tokenizer::new(&dialect, sql)
                .with_unescape(false)
                .tokenize()
                .unwrap();
            let expected = vec![Token::SingleQuotedString(expected.to_string())];
            compare(expected, tokens);

            let tokens = Tokenizer::new(&dialect, sql)
                .with_unescape(true)
                .tokenize()
                .unwrap();
            let expected = vec![Token::SingleQuotedString(expected_unescaped.to_string())];
            compare(expected, tokens);
        }

        for sql in [r#"'\'"#, r#"'ab\'"#] {
            let dialect = BigQueryDialect {};
            let mut tokenizer = Tokenizer::new(&dialect, sql);
            assert_eq!(
                "Unterminated string literal",
                tokenizer.tokenize().unwrap_err().message.as_str(),
            );
        }

        // Non-escape dialect
        for (sql, expected) in [(r#"'\'"#, r#"\"#), (r#"'ab\'"#, r#"ab\"#)] {
            let dialect = GenericDialect {};
            let tokens = Tokenizer::new(&dialect, sql).tokenize().unwrap();

            let expected = vec![Token::SingleQuotedString(expected.to_string())];

            compare(expected, tokens);
        }
    }
}

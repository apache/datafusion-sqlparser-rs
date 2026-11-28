// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Splits SQL into tokens that borrow from the input.
//!
//! Keywords and operators are not enums: a token is just a kind and a slice
//! of the source, so a dialect can introduce new ones without changing this
//! module.

use crate::ast::Span;
use crate::dialect::Dialect;
use crate::{PResult, ParseError};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenKind {
    /// An unquoted word: a keyword or an identifier.
    Word,
    QuotedIdent,
    String,
    Number,
    /// A bind parameter such as `?`, `$1` or `:name`.
    Placeholder,
    /// `X'AB'` or `0xAB`.
    HexString,
    /// `$tag$text$tag$`. Only produced by dialects that have this syntax.
    DollarString,
    /// A literal whose syntax only a dialect knows. That dialect reads it in
    /// `parse_prefix`, and the core rejects it wherever it appears.
    DialectLiteral,
    /// An operator or punctuation.
    Op,
    Eof,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Token<'a> {
    pub kind: TokenKind,
    /// The token's full source text, including any quotes.
    pub text: &'a str,
    pub span: Span,
}

impl Token<'_> {
    /// True if this is the unquoted word `word`, in any case.
    pub fn is_word(&self, word: &str) -> bool {
        self.kind == TokenKind::Word && self.text.eq_ignore_ascii_case(word)
    }

    /// True if this is the operator or punctuation `op`.
    pub fn is_op(&self, op: &str) -> bool {
        self.kind == TokenKind::Op && self.text == op
    }
}

/// The kind and byte length of a token found at the start of the input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Lexeme {
    pub kind: TokenKind,
    pub len: usize,
}

pub type LexResult = Result<Lexeme, &'static str>;

/// Tokenizes `sql`. The result always ends with an [`TokenKind::Eof`] token.
pub fn tokenize<D: Dialect>(sql: &str) -> PResult<Vec<Token<'_>>> {
    if u32::try_from(sql.len()).is_err() {
        return Err(lex_error("input is larger than 4GiB", 0));
    }

    let mut tokens = Vec::with_capacity(sql.len() / 4 + 1);
    let mut pos = 0;
    let mut after_line_comment = false;
    loop {
        let rest = &sql[pos..];
        let trimmed = rest.trim_ascii_start();
        let gap = &rest[..rest.len() - trimmed.len()];
        pos += gap.len();
        if trimmed.is_empty() {
            break;
        }

        if (after_line_comment || gap.contains(['\n', '\r']))
            && let Some((index, len)) = D::lex_payload_line(sql, &tokens, pos)
        {
            let text = trimmed
                .get(..len)
                .filter(|text| !text.is_empty())
                .ok_or_else(|| lex_error("dialect produced an invalid token length", pos))?;
            let end = pos + len;
            tokens.insert(
                index.min(tokens.len()),
                Token {
                    kind: TokenKind::DialectLiteral,
                    text,
                    span: span(pos, end),
                },
            );
            pos = end;
            continue;
        }

        if let Some(comment) = D::lex_comment(trimmed) {
            let len = comment.map_err(|message| lex_error(message, pos))?;
            if trimmed.get(..len).is_none_or(str::is_empty) {
                return Err(lex_error("dialect produced an invalid comment length", pos));
            }
            pos += len;
            after_line_comment = trimmed[..len].ends_with(['\n', '\r']);
            continue;
        }
        after_line_comment = false;

        let adjacent = tokens
            .last()
            .filter(|token: &&Token<'_>| token.span.end as usize == pos)
            .copied();
        let lexeme = D::lex_after(adjacent, trimmed)
            .or_else(|| D::lex_token(trimmed))
            .unwrap_or_else(|| lex_core(trimmed))
            .map_err(|message| lex_error(message, pos))?;
        let text = trimmed
            .get(..lexeme.len)
            .filter(|text| !text.is_empty())
            .ok_or_else(|| lex_error("dialect produced an invalid token length", pos))?;

        let end = pos + lexeme.len;
        tokens.push(Token {
            kind: lexeme.kind,
            text,
            span: span(pos, end),
        });
        pos = end;
    }

    tokens.push(Token {
        kind: TokenKind::Eof,
        text: "",
        span: span(pos, pos),
    });
    Ok(tokens)
}

fn span(start: usize, end: usize) -> Span {
    // `tokenize` has checked that all offsets fit in a u32.
    Span {
        start: start as u32,
        end: end as u32,
    }
}

fn lex_error(message: &str, pos: usize) -> ParseError {
    ParseError {
        message: message.to_string(),
        span: span(pos, pos + 1),
    }
}

/// Lexes one token of the core grammar. `rest` must not be empty.
#[inline]
pub fn lex_core(rest: &str) -> LexResult {
    let bytes = rest.as_bytes();
    match bytes[0] {
        b'\'' => quoted(rest, TokenKind::String),
        b'"' => quoted(rest, TokenKind::QuotedIdent),
        b'0' if bytes.get(1) == Some(&b'x') => hex_number(bytes),
        b'x' | b'X' if bytes.get(1) == Some(&b'\'') => {
            let string = quoted(&rest[1..], TokenKind::HexString)?;
            Ok(Lexeme {
                kind: TokenKind::HexString,
                len: string.len + 1,
            })
        }
        b'0'..=b'9' => number(bytes),
        b'.' if bytes.get(1).is_some_and(u8::is_ascii_digit) => number(bytes),
        b'?' | b'$' | b':' => placeholder(bytes).map_or_else(|| operator(bytes), Ok),
        b if is_word_start(b) => Ok(word(bytes)),
        _ => operator(bytes),
    }
}

/// Lexes text quoted by the first byte of `rest`, in which the quote is
/// escaped by doubling it.
#[inline]
pub fn quoted(rest: &str, kind: TokenKind) -> LexResult {
    let bytes = rest.as_bytes();
    let quote = bytes[0];
    let mut i = 1;
    while i < bytes.len() {
        if bytes[i] == quote {
            if bytes.get(i + 1) == Some(&quote) {
                i += 2;
                continue;
            }
            return Ok(Lexeme { kind, len: i + 1 });
        }
        i += 1;
    }
    Err("unterminated quoted text")
}

#[inline]
fn is_word_start(b: u8) -> bool {
    b.is_ascii_alphabetic() || b == b'_' || !b.is_ascii()
}

#[inline]
fn word(bytes: &[u8]) -> Lexeme {
    let len = bytes
        .iter()
        .position(|&b| !is_word_start(b) && !b.is_ascii_digit())
        .unwrap_or(bytes.len());
    Lexeme {
        kind: TokenKind::Word,
        len,
    }
}

#[inline]
fn hex_number(bytes: &[u8]) -> LexResult {
    let digits = bytes[2..]
        .iter()
        .take_while(|b| b.is_ascii_hexdigit())
        .count();
    let len = 2 + digits;
    if bytes.get(len).is_some_and(|&b| is_word_start(b)) {
        return Err("invalid number");
    }
    Ok(Lexeme {
        kind: TokenKind::HexString,
        len,
    })
}

#[inline]
/// True if the point at `at` in `bytes` belongs to the number before it. A
/// point followed by a letter or another point is left for the parser, unless
/// the letter starts an exponent, as in `1.e5`.
pub fn point_continues_number(bytes: &[u8], at: usize) -> bool {
    if bytes.get(at) != Some(&b'.') {
        return false;
    }
    match bytes.get(at + 1) {
        Some(b'e' | b'E') => {
            let sign = usize::from(matches!(bytes.get(at + 2), Some(b'+' | b'-')));
            bytes.get(at + 2 + sign).is_some_and(u8::is_ascii_digit)
        }
        Some(&b) => !(is_word_start(b) || b == b'.'),
        None => true,
    }
}

fn number(bytes: &[u8]) -> LexResult {
    let digits = |from: usize| {
        bytes[from..]
            .iter()
            .take_while(|b| b.is_ascii_digit())
            .count()
    };
    let mut len = digits(0);
    if point_continues_number(bytes, len) {
        len += 1 + digits(len + 1);
    }
    if matches!(bytes.get(len), Some(b'e' | b'E')) {
        let sign = usize::from(matches!(bytes.get(len + 1), Some(b'+' | b'-')));
        let exponent = digits(len + 1 + sign);
        if exponent > 0 {
            len += 1 + sign + exponent;
        }
    }
    // Otherwise `1a` would silently read as `1` with the alias `a`.
    if bytes.get(len).is_some_and(|&b| is_word_start(b)) {
        return Err("invalid number");
    }
    Ok(Lexeme {
        kind: TokenKind::Number,
        len,
    })
}

/// The token for `?`, `?1`, `$1`, `$name` or `:name`, if one starts here.
#[inline]
fn placeholder(bytes: &[u8]) -> Option<Lexeme> {
    let is_digit = |b: &u8| b.is_ascii_digit();
    let is_name = |b: &u8| is_word_start(*b) || b.is_ascii_digit();
    let tail = &bytes[1..];
    let extra = match bytes[0] {
        b'?' => tail.iter().take_while(|b| is_digit(b)).count(),
        b'$' if tail.first().is_some_and(is_name) => tail.iter().take_while(|b| is_name(b)).count(),
        b':' if tail.first().is_some_and(|&b| is_word_start(b)) => {
            tail.iter().take_while(|b| is_name(b)).count()
        }
        _ => return None,
    };
    Some(Lexeme {
        kind: TokenKind::Placeholder,
        len: 1 + extra,
    })
}

#[inline]
fn operator(bytes: &[u8]) -> LexResult {
    let len = match bytes {
        [b'<', b'=' | b'>' | b'<', ..]
        | [b'>', b'=' | b'>', ..]
        | [b'!', b'=', ..]
        | [b'|', b'|', ..]
        | [b'=', b'>', ..] => 2,
        [
            b'=' | b'<' | b'>' | b'+' | b'-' | b'*' | b'/' | b'%' | b'(' | b')' | b',' | b';'
            | b'.' | b'|' | b'&' | b'^' | b'[' | b']',
            ..,
        ] => 1,
        _ => return Err("unexpected character"),
    };
    Ok(Lexeme {
        kind: TokenKind::Op,
        len,
    })
}

/// The core comment syntax: `--` to the end of the line, and nesting `/* */`.
#[inline]
pub fn lex_core_comment(rest: &str) -> Option<Result<usize, &'static str>> {
    match rest.as_bytes() {
        [b'-', b'-', ..] => Some(Ok(line_comment(rest))),
        [b'/', b'*', ..] => Some(block_comment(rest, true)),
        _ => None,
    }
}

/// The length of a comment running to the end of the line.
pub fn line_comment(rest: &str) -> usize {
    rest.find('\n').map_or(rest.len(), |newline| newline + 1)
}

/// The length of the block comment starting at `rest`.
pub fn block_comment(rest: &str, nested: bool) -> Result<usize, &'static str> {
    let bytes = rest.as_bytes();
    let mut depth = 1;
    let mut i = 2;
    while i + 1 < bytes.len() {
        match (bytes[i], bytes[i + 1]) {
            (b'*', b'/') => {
                depth -= 1;
                i += 2;
                if depth == 0 {
                    return Ok(i);
                }
            }
            (b'/', b'*') if nested => {
                depth += 1;
                i += 2;
            }
            _ => i += 1,
        }
    }
    Err("unterminated block comment")
}

/// Like [`quoted`], for dialects where a backslash also escapes the next byte.
#[inline]
pub fn quoted_with_backslash(rest: &str, kind: TokenKind) -> LexResult {
    let bytes = rest.as_bytes();
    let quote = bytes[0];
    let mut i = 1;
    while i < bytes.len() {
        let Some(off) = bytes[i..].iter().position(|&b| b == quote || b == b'\\') else {
            break;
        };
        i += off;
        if bytes[i] == b'\\' || bytes.get(i + 1) == Some(&quote) {
            i += 2;
        } else {
            return Ok(Lexeme { kind, len: i + 1 });
        }
    }
    Err("unterminated quoted text")
}

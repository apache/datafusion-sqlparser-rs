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

//! A lossless syntax tree that preserves the full fidelity of the source text,
//! including whitespace.
//!
//! We refer to it as a CST, short for Concrete Syntax Tree, in order to
//! contrast it with an AST, although it's not a grammar-based parse tree.
//!
//! The design is based on rust-analyzer's
//! https://github.com/rust-analyzer/rust-analyzer/blob/2020-04-27/docs/dev/syntax.md
//! The RA folks generously made their syntax tree implementation available as the
//! `rowan` crate, which we re-use.
use crate::tokenizer::Token;

/// Each node of the CST has a "kind", a variant from this enum representing
/// its type.
///
/// There are separate kinds for leaf nodes (tokens, such as "whitespace"
/// or "number") and non-leafs, such as "expresssion", but they are not
/// distinguished at the type level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[allow(non_camel_case_types)]
#[repr(u16)]
#[rustfmt::skip]
pub enum SyntaxKind {
    // First, the variants from the `Token` enum.

    // Note: we rely on the token "kinds" having the same internal `u16`
    // representation as the `Token` enum, meaning the number and the
    // order of these must match the `Token` enum exactly.

    // TBD: change `Token` to be a `(SyntaxKind, SmolStr)`.

    /// A keyword (like SELECT) or an optionally quoted SQL identifier
    Word = 0,
    /// An unsigned numeric literal
    Number,
    /// A character that could not be tokenized
    Char,
    /// Single quoted string: i.e: 'string'
    SingleQuotedString,
    /// "National" string literal: i.e: N'string'
    NationalStringLiteral,
    /// Hexadecimal string literal: i.e.: X'deadbeef'
    HexStringLiteral,
    /// Comma
    Comma,
    /// Whitespace (space, tab, etc)
    Whitespace,
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

    // Other kinds representing non-leaf nodes in the Syntax tree.
    ROOT,
    ERR,
    KW,

    // Sentinel value
    LAST
}

impl Token {
    pub fn kind(&self) -> SyntaxKind {
        // From https://github.com/rust-lang/rfcs/blob/master/text/2363-arbitrary-enum-discriminant.md
        unsafe { *(self as *const Self as *const SyntaxKind) }
    }
}

impl From<SyntaxKind> for rowan::SyntaxKind {
    fn from(kind: SyntaxKind) -> Self {
        Self(kind as u16)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Lang {}
impl rowan::Language for Lang {
    type Kind = SyntaxKind;
    fn kind_from_raw(raw: rowan::SyntaxKind) -> Self::Kind {
        assert!(raw.0 <= SyntaxKind::LAST as u16);
        unsafe { std::mem::transmute::<u16, SyntaxKind>(raw.0) }
    }
    fn kind_to_raw(kind: Self::Kind) -> rowan::SyntaxKind {
        kind.into()
    }
}

pub type SyntaxNode = rowan::SyntaxNode<Lang>;

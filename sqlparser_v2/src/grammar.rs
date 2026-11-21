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

//! The core grammar.
//!
//! Each rule is the default implementation of a [`Dialect`] method, and a
//! rule always reaches other rules through `D::`, so an override applies
//! everywhere. Dialects call these functions to fall back to core behavior.
//! Nothing here may depend on which dialect is running.

use crate::PResult;
use crate::ast::StatementKind;
use crate::ast::*;
use crate::dialect::Dialect;
use crate::lexer::{Token, TokenKind};
use crate::parser::{InfixParseFn, InfixRule, Parser, SetOperatorRule};
use std::borrow::Cow;

mod admin;
mod ddl;
mod dml;
mod expr;
mod pattern;
mod pipe;
mod pivot;
mod query;
mod session;
mod xml;

pub use admin::*;
pub use ddl::*;
pub use dml::*;
pub use expr::*;
pub use pattern::*;
pub use pipe::*;
pub use pivot::*;
pub use query::*;
pub use session::*;
pub use xml::*;

/// Binding powers of the core operators. Dialects choose theirs relative to
/// these.
pub mod prec {
    pub const OR: u8 = 10;
    pub const AND: u8 = 20;
    pub const NOT: u8 = 30;
    pub const IS: u8 = 35;
    pub const COMPARE: u8 = 40;
    pub const BIT_OR: u8 = 42;
    /// `^`, `<<` and `>>`.
    pub const BIT_XOR: u8 = 44;
    pub const BIT_AND: u8 = 46;
    /// Below `+` and above comparison, for operators without a standard rank.
    pub const OTHER: u8 = 48;
    pub const ADD: u8 = 50;
    pub const MUL: u8 = 60;
    /// Exponentiation, in dialects where `^` means that.
    pub const EXP: u8 = 65;
    pub const AT_TIME_ZONE: u8 = 67;
    pub const UNARY: u8 = 70;
    /// Tighter than every core operator, for postfix-style syntax.
    pub const POSTFIX: u8 = 80;

    /// Binding powers of the set operators, which only compare with each other.
    pub const UNION: u8 = 10;
    pub const INTERSECT: u8 = 20;
}

/// The length of the longest word matched by [`is_reserved`].
const MAX_RESERVED_LEN: usize = 9;

/// Called for most identifiers, so it avoids scanning a list: longer words
/// return at once, and `matches!` compiles to a decision tree on the bytes.
#[inline]
pub fn is_reserved(word: &str) -> bool {
    if word.len() > MAX_RESERVED_LEN {
        return false;
    }
    let mut buffer = [0; MAX_RESERVED_LEN];
    let upper = &mut buffer[..word.len()];
    upper.copy_from_slice(word.as_bytes());
    upper.make_ascii_uppercase();
    matches!(
        &*upper,
        b"SELECT"
            | b"FROM"
            | b"WHERE"
            | b"GROUP"
            | b"ORDER"
            | b"BY"
            | b"LIMIT"
            | b"OFFSET"
            | b"AS"
            | b"AND"
            | b"OR"
            | b"NOT"
            | b"ASC"
            | b"DESC"
            | b"WITH"
            | b"JOIN"
            | b"INNER"
            | b"LEFT"
            | b"RIGHT"
            | b"FULL"
            | b"OUTER"
            | b"CROSS"
            | b"ON"
            | b"USING"
            | b"HAVING"
            | b"UNION"
            | b"INTERSECT"
            | b"EXCEPT"
            | b"IN"
            | b"SET"
            | b"DISTINCT"
            | b"ALL"
            | b"NATURAL"
            | b"NULL"
            | b"TRUE"
            | b"FALSE"
            | b"CASE"
    )
}

/// Words that begin a clause, so cannot be an alias without `AS`, but are
/// otherwise usable as names.
#[inline]
pub fn is_reserved_for_alias(word: &str) -> bool {
    ["FOR", "FETCH", "WINDOW", "INTO", "ARRAY", "PARTITION"]
        .iter()
        .any(|reserved| reserved.eq_ignore_ascii_case(word))
}

/// Builds a string literal from its token, including the quotes.
#[inline]
pub fn string_literal(token: Token<'_>) -> StringLiteral<'_> {
    StringLiteral {
        text: Cow::Borrowed(&token.text[1..token.text.len() - 1]),
        quote: char::from(token.text.as_bytes()[0]),
        backslash_escapes: false,
        national: false,
        prefix: StringPrefix::None,
        triple: false,
    }
}

/// Like [`string_literal`], for dialects that lex `N'text'` as one token.
pub fn national_string_literal(token: Token<'_>) -> StringLiteral<'_> {
    if matches!(token.text.as_bytes()[0], b'N' | b'n') {
        StringLiteral {
            prefix: Default::default(),
            triple: false,
            text: Cow::Borrowed(&token.text[2..token.text.len() - 1]),
            quote: '\'',
            backslash_escapes: false,
            national: true,
        }
    } else {
        string_literal(token)
    }
}

pub fn parse_statement<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Statement<'a, D::Ext>> {
    if at_query_start(p) || p.at_op("(") {
        return D::parse_query(p).map(|query| StatementKind::Query(Box::new(query)).into());
    }
    if p.at_keyword("INSERT") {
        return D::parse_insert_statement(p);
    }
    if p.at_keyword("UPDATE") {
        return D::parse_update(p).map(|update| StatementKind::Update(Box::new(update)).into());
    }
    if p.at_keyword("DELETE") {
        return D::parse_delete(p).map(|delete| StatementKind::Delete(Box::new(delete)).into());
    }
    if p.at_keyword("CREATE") {
        return D::parse_create(p);
    }
    if p.at_keyword("SHOW") {
        return D::parse_show(p).map(|show| StatementKind::Show(Box::new(show)).into());
    }
    if at_transaction(p) {
        return D::parse_transaction(p).map(|t| StatementKind::Transaction(Box::new(t)).into());
    }
    if p.at_keyword("USE") {
        return D::parse_use(p).map(|name| StatementKind::Use(Box::new(name)).into());
    }
    if p.at_keyword("EXPLAIN") || p.at_keyword("DESCRIBE") || p.at_keyword("DESC") {
        return D::parse_explain(p).map(|explain| StatementKind::Explain(Box::new(explain)).into());
    }
    if p.at_keyword("SET") {
        return D::parse_set(p).map(|set| StatementKind::Set(Box::new(set)).into());
    }
    if p.at_keyword("CALL") {
        return D::parse_call(p).map(|call| StatementKind::Call(Box::new(call)).into());
    }
    if p.at_keyword("GRANT") {
        return D::parse_grant(p).map(|grant| StatementKind::Grant(Box::new(grant)).into());
    }
    if p.at_keyword("REVOKE") {
        return D::parse_revoke(p).map(|revoke| StatementKind::Revoke(Box::new(revoke)).into());
    }
    if p.at_keyword("MERGE") {
        return D::parse_merge(p).map(|merge| StatementKind::Merge(Box::new(merge)).into());
    }
    if at_drop_function(p) {
        return D::parse_drop_routine(p)
            .map(|drop| StatementKind::DropRoutine(Box::new(drop)).into());
    }
    if p.at_keyword("DROP") {
        return D::parse_drop(p).map(|drop| StatementKind::Drop(Box::new(drop)).into());
    }
    if p.at_keyword("TRUNCATE") {
        return D::parse_truncate(p)
            .map(|truncate| StatementKind::Truncate(Box::new(truncate)).into());
    }
    if p.at_keyword("ALTER") {
        return D::parse_alter_table(p)
            .map(|alter| StatementKind::AlterTable(Box::new(alter)).into());
    }
    Err(p.expected("a statement"))
}

pub fn parse_optional_where<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<Option<Expr<'a, D::Ext>>> {
    if p.eat_keyword("WHERE") {
        p.parse_expr().map(Some)
    } else {
        Ok(None)
    }
}

/// Parses a name after a dot, where reserved words are allowed.
pub fn parse_name_part<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Ident<'a>> {
    let token = p.peek();
    match token.kind {
        TokenKind::Word => {
            p.advance();
            Ok(Ident {
                text: token.text.into(),
                quote: None,
                span: token.span,
            })
        }
        TokenKind::QuotedIdent => p.parse_ident(),
        _ => Err(p.expected("identifier")),
    }
}

/// Parses `(a, b, ...)`.
pub fn parse_ident_list<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Vec<Ident<'a>>> {
    p.expect_op("(")?;
    let idents = p.parse_comma_separated(Parser::parse_ident)?;
    p.expect_op(")")?;
    Ok(idents)
}

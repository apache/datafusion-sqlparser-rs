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

//! MySQL.

use crate::ast::Span;
use std::borrow::Cow;
use std::fmt::{self, Write as _};

use crate::PResult;
use crate::ast::{
    AlterTableOp, BinaryOp, CastStyle, ColumnDef, ColumnOption, ColumnPosition, CreateFlags,
    DataType, DataTypeSuffix, Delete, Expr, ExprKind, Generated, GeneratedKind, GeneratedStorage,
    GeneratedWhen, GrantName, Grantee, GroupByModifier, Ident, InsertSource, Join, JoinKind,
    LikeKind, Limit, Literal, MultiTableDelete, ObjectName, ObjectType, OffsetRows, OrderByItem,
    Query, SelectInto, SelectItem, SelectItemKind, SetExpr, SetQuantifier, SetStatement,
    SpecialExpr, Statement, StringLiteral, StringPrefix, TableConstraint, TableFactor, TableRef,
    TableWithJoins, Update,
};
use crate::dialect::builtin::{BuiltinInsert, BuiltinSelect, BuiltinTableFactor};
use crate::dialect::{AstExt, Builtin, Dialect, ExprExtOf};
use crate::grammar::{self, prec};
use crate::lexer::{
    LexResult, Lexeme, Token, TokenKind, block_comment, lex_core_comment, line_comment, quoted,
    quoted_with_backslash,
};
use crate::parser::{InfixRule, Parser};

mod statements;

pub use statements::*;

#[derive(Debug, Clone, PartialEq)]
pub struct MySql;

#[derive(Debug, Clone, PartialEq)]
pub enum MySqlExpr<'a, X: AstExt> {
    /// `left DIV right`
    IntDiv {
        left: Box<Expr<'a, X>>,
        right: Box<Expr<'a, X>>,
    },
    /// `MATCH (columns) AGAINST (value [modifier])`
    MatchAgainst {
        columns: Vec<ObjectName<'a>>,
        value: Box<Expr<'a, X>>,
        modifier: Option<SearchModifier>,
    },
    /// `value MEMBER OF(array)`
    MemberOf {
        value: Box<Expr<'a, X>>,
        array: Box<Expr<'a, X>>,
    },
    /// `CONVERT(expr USING charset)` or `CONVERT(expr, type [CHARACTER SET charset])`
    Convert(Box<MySqlConvert<'a, X>>),
    /// A literal with a character set introducer: `_utf8mb4 'text'`.
    Introduced(Box<MySqlIntroduced<'a>>),
    /// Adjacent string literals, which MySQL concatenates: `'a' "b"`.
    ConcatenatedStrings(Vec<StringLiteral<'a>>),
}

/// `CONVERT(expr USING charset)` when `using` is set, otherwise
/// `CONVERT(expr, data_type [CHARACTER SET charset])`.
#[derive(Debug, Clone, PartialEq)]
pub struct MySqlConvert<'a, X: AstExt> {
    pub expr: Expr<'a, X>,
    pub using: Option<ObjectName<'a>>,
    pub data_type: Option<DataType<'a>>,
    pub charset: Option<ObjectName<'a>>,
}

/// A character set introducer and the literal it applies to.
#[derive(Debug, Clone, PartialEq)]
pub struct MySqlIntroduced<'a> {
    pub charset: Ident<'a>,
    pub value: Literal<'a>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchModifier {
    NaturalLanguage,
    NaturalLanguageWithQueryExpansion,
    Boolean,
    WithQueryExpansion,
}

/// Backticks quote identifiers, double quotes delimit strings, and `@name`
/// is a variable.
#[inline]
pub fn lex_token(rest: &str) -> Option<LexResult> {
    let op = |len| {
        Some(Ok(Lexeme {
            kind: TokenKind::Op,
            len,
        }))
    };
    match rest.as_bytes() {
        [b'`', ..] => Some(quoted(rest, TokenKind::QuotedIdent)),
        [b'"' | b'\'', ..] => Some(quoted_with_backslash(rest, TokenKind::String)),
        [b'b' | b'B', b'\'', ..] => Some(bit_string(rest)),
        [b'n' | b'N', b'\'', ..] => Some(national_string(rest)),
        [b'<', b'=', b'>', ..] | [b'-', b'>', b'>', ..] => op(3),
        [b'-', b'>', ..] | [b':', b'=', ..] | [b'&', b'&', ..] => op(2),
        [b'@', b'\'' | b'"' | b'`', ..] => op(1),
        [b'{' | b'}', ..] => op(1),
        [b'@', ..] => user_variable(rest),
        _ => word_or_number(rest),
    }
}

/// Lexes `b'101'` as a string token, keeping the prefix.
fn bit_string(rest: &str) -> LexResult {
    let string = quoted(&rest[1..], TokenKind::String)?;
    Ok(Lexeme {
        kind: TokenKind::String,
        len: string.len + 1,
    })
}

/// Lexes `N'text'` as a string token, keeping the prefix.
fn national_string(rest: &str) -> LexResult {
    let string = quoted_with_backslash(&rest[1..], TokenKind::String)?;
    Ok(Lexeme {
        kind: TokenKind::String,
        len: string.len + 1,
    })
}

#[inline]
fn is_word_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_' || b == b'$' || !b.is_ascii()
}

fn word_run(bytes: &[u8]) -> usize {
    bytes.iter().take_while(|&&b| is_word_byte(b)).count()
}

/// True if a run of word bytes that starts with a digit reads as a number.
fn run_is_number(run: &[u8]) -> bool {
    let digits = run.iter().take_while(|b| b.is_ascii_digit()).count();
    if digits == run.len() {
        return true;
    }
    if let [b'0', b'x' | b'X', hex @ ..] = run {
        return !hex.is_empty() && hex.iter().all(u8::is_ascii_hexdigit);
    }
    if let [b'0', b'b' | b'B', bits @ ..] = run {
        return !bits.is_empty() && bits.iter().all(|b| matches!(b, b'0' | b'1'));
    }
    match run[digits] {
        b'e' | b'E' => {
            let exponent = &run[digits + 1..];
            !exponent.is_empty() && exponent.iter().all(u8::is_ascii_digit)
        }
        _ => false,
    }
}

/// Lexes names containing `$` or starting with a digit, and `0b101`. Returns
/// `None` for anything the core lexes the same way.
#[inline]
fn word_or_number(rest: &str) -> Option<LexResult> {
    let bytes = rest.as_bytes();
    let first = *bytes.first()?;
    if !is_word_byte(first) {
        return None;
    }
    let len = word_run(bytes);
    let run = &bytes[..len];
    let token = |kind| Some(Ok(Lexeme { kind, len }));
    if first.is_ascii_digit() {
        return match run {
            [b'0', b'b' | b'B', bits @ ..]
                if !bits.is_empty() && bits.iter().all(|b| matches!(b, b'0' | b'1')) =>
            {
                token(TokenKind::Number)
            }
            run if run_is_number(run) || matches!(run, [b'0', b'x' | b'X', ..]) => None,
            // `1e+5`: the core reads the sign and exponent.
            [.., b'e' | b'E']
                if run[..len - 1].iter().all(u8::is_ascii_digit)
                    && matches!(bytes.get(len), Some(b'+' | b'-'))
                    && bytes.get(len + 1).is_some_and(u8::is_ascii_digit) =>
            {
                None
            }
            _ => token(TokenKind::Word),
        };
    }
    (first == b'$' && len > 1 || run.contains(&b'$')).then_some(Ok(Lexeme {
        kind: TokenKind::Word,
        len,
    }))
}

/// Lexes the parts of a qualified name that start with a digit, such as the
/// `.15to29` in `t.15to29`, where the core would read a number.
#[inline]
pub fn lex_after(prev: Option<Token<'_>>, rest: &str) -> Option<LexResult> {
    let prev = prev?;
    let bytes = rest.as_bytes();
    if !bytes[0].is_ascii_digit() && bytes[0] != b'.' {
        return None;
    }
    if prev.is_op(".") && bytes[0].is_ascii_digit() {
        let len = word_run(bytes);
        return bytes[..len]
            .iter()
            .any(|b| !b.is_ascii_digit())
            .then_some(Ok(Lexeme {
                kind: TokenKind::Word,
                len,
            }));
    }
    if matches!(prev.kind, TokenKind::Word | TokenKind::QuotedIdent)
        && bytes[0] == b'.'
        && bytes.get(1).is_some_and(u8::is_ascii_digit)
    {
        return Some(Ok(Lexeme {
            kind: TokenKind::Op,
            len: 1,
        }));
    }
    None
}

/// Lexes `@name` or `@@name` as a word.
#[inline]
fn user_variable(rest: &str) -> Option<LexResult> {
    let bytes = rest.as_bytes();
    let sigils = if bytes.get(1) == Some(&b'@') { 2 } else { 1 };
    let name = bytes[sigils..]
        .iter()
        .take_while(|&&b| b.is_ascii_alphanumeric() || b == b'_' || b == b'.' || !b.is_ascii())
        .count();
    (name > 0).then_some(Ok(Lexeme {
        kind: TokenKind::Word,
        len: sigils + name,
    }))
}

/// `#` starts a comment, block comments do not nest, and `--` only starts a
/// comment when followed by whitespace or the end of the input.
#[inline]
pub fn lex_comment(rest: &str) -> Option<Result<usize, &'static str>> {
    match rest.as_bytes() {
        [b'#', ..] => Some(Ok(line_comment(rest))),
        [b'/', b'*', ..] => Some(block_comment(rest, false)),
        [b'-', b'-', next, ..] if !next.is_ascii_whitespace() => None,
        _ => lex_core_comment(rest),
    }
}

/// Words MySQL does not allow as an alias without `AS`.
#[inline]
pub fn is_reserved_for_alias(word: &str) -> bool {
    [
        "USE",
        "FORCE",
        "IGNORE",
        "PARTITION",
        "ANALYZE",
        "EXPLAIN",
        "MATCH",
        "REGEXP",
        "RLIKE",
    ]
    .iter()
    .any(|reserved| reserved.eq_ignore_ascii_case(word))
        || grammar::is_reserved_for_alias(word)
}

#[inline]
pub fn is_reserved(word: &str) -> bool {
    [
        "DIV",
        "HIGH_PRIORITY",
        "STRAIGHT_JOIN",
        "SQL_CALC_FOUND_ROWS",
        "SQL_SMALL_RESULT",
        "SQL_BIG_RESULT",
    ]
    .iter()
    .any(|reserved| reserved.eq_ignore_ascii_case(word))
        || grammar::is_reserved(word)
}

/// Reads a quoted collation name as written, with its quotes.
fn parse_string_collation<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<ObjectName<'a>> {
    let token = p.advance();
    let quote = char::from(token.text.as_bytes()[0]);
    Ok(ObjectName {
        parts: vec![Ident {
            text: Cow::Borrowed(&token.text[1..token.text.len() - 1]),
            quote: Some(quote),
            span: token.span,
        }],
    })
}

/// Parses `expr COLLATE 'name'`, where the name is a string.
fn parse_collate_string<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
    left: Expr<'a, D::Ext>,
    _bp: u8,
) -> PResult<Expr<'a, D::Ext>> {
    p.advance();
    let collation = parse_string_collation(p)?;
    p.deepen_by(2)?;
    Ok(ExprKind::Special(Box::new(SpecialExpr::Collate {
        expr: left,
        collation,
    }))
    .into())
}

pub fn infix_rule<'a, D>(p: &Parser<'a, D>) -> Option<InfixRule<'a, D>>
where
    D: Dialect,
    ExprExtOf<'a, D>: From<MySqlExpr<'a, D::Ext>>,
{
    if p.at_keyword("DIV") {
        Some(InfixRule::custom(prec::MUL, parse_int_div))
    } else if p.at_op(":=") {
        Some(InfixRule::custom(ASSIGNMENT, parse_assignment_expr))
    } else if p.at_op("&&") {
        Some(InfixRule::binary(prec::AND, BinaryOp::And))
    } else if p.at_keyword("MEMBER") && p.peek_nth(1).is_word("OF") && p.peek_nth(2).is_op("(") {
        Some(InfixRule::custom(prec::COMPARE, parse_member_of))
    } else if p.at_keyword("COLLATE") && p.peek_nth(1).kind == TokenKind::String {
        Some(InfixRule::custom(prec::POSTFIX, parse_collate_string))
    } else if p.at_keyword("XOR") {
        Some(InfixRule::binary(XOR, BinaryOp::Named("XOR".into())))
    } else if at_regexp(p, 0) || (p.at_keyword("NOT") && at_regexp(p, 1)) {
        Some(InfixRule::custom(prec::COMPARE, parse_regexp))
    } else if p.at_op("<<") {
        Some(InfixRule::binary(prec::BIT_XOR, BinaryOp::ShiftLeft))
    } else if p.at_op(">>") {
        Some(InfixRule::binary(prec::BIT_XOR, BinaryOp::ShiftRight))
    } else if p.at_op("<=>") {
        Some(InfixRule::binary(
            prec::COMPARE,
            BinaryOp::Named("<=>".into()),
        ))
    } else if p.at_op("->") {
        Some(InfixRule::binary(prec::OTHER, BinaryOp::Named("->".into())))
    } else if p.at_op("->>") {
        Some(InfixRule::binary(
            prec::OTHER,
            BinaryOp::Named("->>".into()),
        ))
    } else {
        grammar::infix_rule(p)
    }
}

/// Between `AND` and `OR`.
const XOR: u8 = 15;

/// Below every other operator.
const ASSIGNMENT: u8 = 5;

/// `@a := expr`, which associates to the right.
fn parse_assignment_expr<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
    left: Expr<'a, D::Ext>,
    bp: u8,
) -> PResult<Expr<'a, D::Ext>> {
    p.advance();
    let right = p.parse_expr_bp(bp - 1)?;
    Ok(ExprKind::Binary {
        left: Box::new(left),
        op: BinaryOp::Named(":=".into()),
        right: Box::new(right),
    }
    .into())
}

fn parse_member_of<'a, D>(
    p: &mut Parser<'a, D>,
    left: Expr<'a, D::Ext>,
    _bp: u8,
) -> PResult<Expr<'a, D::Ext>>
where
    D: Dialect,
    ExprExtOf<'a, D>: From<MySqlExpr<'a, D::Ext>>,
{
    p.advance();
    p.advance();
    p.expect_op("(")?;
    let array = Box::new(p.parse_expr()?);
    p.expect_op(")")?;
    let node = MySqlExpr::MemberOf {
        value: Box::new(left),
        array,
    };
    Ok(ExprKind::Ext(node.into()).into())
}

pub fn at_regexp<'a, D: Dialect>(p: &Parser<'a, D>, n: usize) -> bool {
    let token = p.peek_nth(n);
    token.is_word("REGEXP") || token.is_word("RLIKE")
}

pub fn parse_regexp<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
    left: Expr<'a, D::Ext>,
    _bp: u8,
) -> PResult<Expr<'a, D::Ext>> {
    let negated = p.eat_keyword("NOT");
    let kind = if p.eat_keyword("REGEXP") {
        LikeKind::Named("REGEXP".into())
    } else {
        p.expect_keyword("RLIKE")?;
        LikeKind::Named("RLIKE".into())
    };
    grammar::parse_like_operand(p, left, negated, kind)
}

/// True if `token` can start an expression, for `BINARY expr`, as opposed to
/// a column named `binary` before a comma, `FROM` and the like.
fn starts_expression<'a, D: Dialect>(p: &Parser<'a, D>, token: Token<'a>) -> bool {
    match token.kind {
        TokenKind::Word => !D::is_reserved(token.text) && !D::is_reserved_for_alias(token.text),
        TokenKind::QuotedIdent
        | TokenKind::String
        | TokenKind::Number
        | TokenKind::HexString
        | TokenKind::Placeholder => true,
        TokenKind::Op => matches!(token.text, "-" | "+" | "~" | "!"),
        _ => {
            let _ = p;
            false
        }
    }
}

/// True at a plain string, as opposed to a prefixed or national one.
fn at_plain_string<'a, D: Dialect>(p: &Parser<'a, D>) -> bool {
    let token = p.peek();
    token.kind == TokenKind::String && !token.text.starts_with(['b', 'B', 'n', 'N'])
}

/// True if `CONVERT` at the current token is MySQL's rather than a plain call:
/// its parentheses hold a top-level `USING`, or exactly one comma before a name.
fn at_mysql_convert<'a, D: Dialect>(p: &Parser<'a, D>) -> bool {
    let (mut n, mut depth, mut commas, mut after_comma) = (2, 0_usize, 0, None);
    loop {
        let token = p.peek_nth(n);
        match token.kind {
            TokenKind::Eof => return false,
            TokenKind::Op if token.text == "(" => depth += 1,
            TokenKind::Op if token.text == ")" => {
                if depth == 0 {
                    return commas == 1 && after_comma == Some(TokenKind::Word);
                }
                depth -= 1;
            }
            TokenKind::Op if token.text == "," && depth == 0 => {
                commas += 1;
                after_comma = Some(p.peek_nth(n + 1).kind);
            }
            TokenKind::Word if depth == 0 && token.is_word("USING") => return true,
            _ => {}
        }
        n += 1;
    }
}

/// Parses the arguments of `CONVERT`, with the current token on `(`.
pub fn parse_convert<'a, D>(p: &mut Parser<'a, D>) -> PResult<Expr<'a, D::Ext>>
where
    D: Dialect,
    ExprExtOf<'a, D>: From<MySqlExpr<'a, D::Ext>>,
{
    p.expect_op("(")?;
    let expr = p.parse_expr()?;
    let mut node = MySqlConvert {
        expr,
        using: None,
        data_type: None,
        charset: None,
    };
    if p.eat_keyword("USING") {
        node.using = Some(grammar::parse_object_name(p)?);
    } else {
        p.expect_op(",")?;
        node.data_type = Some(D::parse_data_type(p)?);
        if p.at_keyword("CHARACTER") && p.peek_nth(1).is_word("SET") {
            p.advance();
            p.advance();
            node.charset = Some(grammar::parse_object_name(p)?);
        }
    }
    p.expect_op(")")?;
    Ok(ExprKind::Ext(MySqlExpr::Convert(Box::new(node)).into()).into())
}

/// Parses `MATCH (...) AGAINST (...)`, `CONVERT`, `BINARY expr`, character set
/// introducers and adjacent strings, or else a core prefix expression.
pub fn parse_prefix<'a, D>(p: &mut Parser<'a, D>) -> PResult<Expr<'a, D::Ext>>
where
    D: Dialect,
    ExprExtOf<'a, D>: From<MySqlExpr<'a, D::Ext>>,
{
    if grammar::at_odbc_escape(p) {
        return grammar::parse_odbc_escape(p);
    }
    let next = p.peek_nth(1);
    if p.at_keyword("CONVERT") && next.is_op("(") && at_mysql_convert(p) {
        p.advance();
        return parse_convert(p);
    }
    if p.at_keyword("BINARY") && starts_expression(p, next) {
        let keyword = p.advance();
        let expr = p.parse_expr_bp(prec::UNARY)?;
        return Ok(ExprKind::Cast {
            expr: Box::new(expr),
            ty: Box::new(DataType {
                qualifier: Vec::new(),
                name: Ident {
                    text: "BINARY".into(),
                    quote: None,
                    span: keyword.span,
                },
                angle: None,
                args: Vec::new(),
                suffix: DataTypeSuffix::None,
                array: crate::ast::ArrayType::None,
            }),
            style: CastStyle::Function,
        }
        .into());
    }
    if p.peek().kind == TokenKind::Word
        && p.peek().text.starts_with('_')
        && matches!(next.kind, TokenKind::String | TokenKind::HexString)
    {
        let charset = p.parse_ident()?;
        let value = match grammar::parse_prefix(p)?.kind {
            ExprKind::Literal(value) => value,
            _ => return Err(p.expected("a string after a character set")),
        };
        let node = MySqlIntroduced { charset, value };
        return Ok(ExprKind::Ext(MySqlExpr::Introduced(Box::new(node)).into()).into());
    }
    if !(p.at_keyword("MATCH") && next.kind == TokenKind::Op && next.text == "(") {
        let expr = grammar::parse_prefix(p)?;
        return match expr.kind {
            ExprKind::Literal(Literal::String(first))
                if first.prefix == StringPrefix::None && !first.national && at_plain_string(p) =>
            {
                let mut parts = vec![first];
                while at_plain_string(p) {
                    let token = p.advance();
                    parts.push(D::string_literal(token));
                }
                Ok(ExprKind::Ext(MySqlExpr::ConcatenatedStrings(parts).into()).into())
            }
            other => Ok(other.into()),
        };
    }
    p.advance();
    p.expect_op("(")?;
    let columns = p.parse_comma_separated(grammar::parse_object_name)?;
    p.expect_op(")")?;
    p.expect_keyword("AGAINST")?;
    p.expect_op("(")?;
    let value = Box::new(p.nested(Parser::parse_prefix)?);
    let modifier = parse_search_modifier(p)?;
    p.expect_op(")")?;
    let node = MySqlExpr::MatchAgainst {
        columns,
        value,
        modifier,
    };
    Ok(ExprKind::Ext(node.into()).into())
}

fn parse_search_modifier<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Option<SearchModifier>> {
    let expansion = |p: &mut Parser<'a, D>| -> PResult<bool> {
        if !p.eat_keyword("WITH") {
            return Ok(false);
        }
        p.expect_keyword("QUERY")?;
        p.expect_keyword("EXPANSION")?;
        Ok(true)
    };
    if !p.eat_keyword("IN") {
        return Ok(expansion(p)?.then_some(SearchModifier::WithQueryExpansion));
    }
    if p.eat_keyword("BOOLEAN") {
        p.expect_keyword("MODE")?;
        return Ok(Some(SearchModifier::Boolean));
    }
    p.expect_keyword("NATURAL")?;
    p.expect_keyword("LANGUAGE")?;
    p.expect_keyword("MODE")?;
    Ok(Some(if expansion(p)? {
        SearchModifier::NaturalLanguageWithQueryExpansion
    } else {
        SearchModifier::NaturalLanguage
    }))
}

pub fn parse_column_position<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<Option<ColumnPosition<'a>>> {
    if p.eat_keyword("FIRST") {
        return Ok(Some(ColumnPosition::First));
    }
    if p.eat_keyword("AFTER") {
        return p
            .parse_ident()
            .map(|name| Some(ColumnPosition::After(name)));
    }
    Ok(None)
}

/// Parses a column definition whose name can be a reserved word.
fn parse_changed_column<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<ColumnDef<'a, D::Ext>> {
    let name = p.parse_name_part()?;
    grammar::parse_column_def_after_name(p, name)
}

fn parse_table_setting<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
    name: &'static str,
) -> PResult<AlterTableOp<'a, D::Ext>> {
    p.advance();
    let equals = p.eat_op("=");
    let token = p.peek();
    if !matches!(token.kind, TokenKind::Word | TokenKind::Number) {
        return Err(p.expected("a value"));
    }
    p.advance();
    Ok(AlterTableOp::Setting {
        name: name.into(),
        equals,
        value: token.text.into(),
    })
}

pub fn parse_alter_table_op<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<AlterTableOp<'a, D::Ext>> {
    let next = p.peek_nth(1);
    for name in ["ALGORITHM", "LOCK", "AUTO_INCREMENT"] {
        if p.at_keyword(name) {
            return parse_table_setting(p, name);
        }
    }
    if p.at_keyword("RENAME") && next.is_word("AS") {
        p.advance();
        p.advance();
        return grammar::parse_object_name(p).map(AlterTableOp::RenameAs);
    }
    if p.eat_keyword("CHANGE") {
        p.eat_keyword("COLUMN");
        let old = p.parse_ident()?;
        let column = parse_changed_column(p)?;
        let position = parse_column_position(p)?;
        return Ok(AlterTableOp::ChangeColumn {
            old,
            column,
            position,
        });
    }
    if p.eat_keyword("MODIFY") {
        p.eat_keyword("COLUMN");
        let column = parse_changed_column(p)?;
        let position = parse_column_position(p)?;
        return Ok(AlterTableOp::ModifyColumn { column, position });
    }
    if p.at_keyword("DROP") && next.is_word("PRIMARY") {
        p.advance();
        p.advance();
        p.expect_keyword("KEY")?;
        return Ok(AlterTableOp::Phrase("DROP PRIMARY KEY".into()));
    }
    if p.at_keyword("DROP") && next.is_word("FOREIGN") {
        p.advance();
        p.advance();
        p.expect_keyword("KEY")?;
        let name = p.parse_ident()?;
        return Ok(AlterTableOp::PhraseName {
            phrase: "DROP FOREIGN KEY".into(),
            name,
        });
    }
    if p.at_keyword("DROP") && next.is_word("INDEX") {
        p.advance();
        p.advance();
        let name = p.parse_ident()?;
        return Ok(AlterTableOp::PhraseName {
            phrase: "DROP INDEX".into(),
            name,
        });
    }
    let mut operation = grammar::parse_alter_table_op(p)?;
    if let AlterTableOp::AddColumn { position, .. } = &mut operation {
        *position = parse_column_position(p)?;
    }
    Ok(operation)
}

/// `ALL`, `DISTINCT` and the MySQL modifiers can appear in any order.
pub fn parse_select_head<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<(BuiltinSelect<'a>, SetQuantifier)> {
    let mut select = BuiltinSelect::default();
    let mut quantifier = SetQuantifier::None;
    loop {
        let unset = quantifier == SetQuantifier::None;
        if !unset
            && (p.at_keyword("ALL") || p.at_keyword("DISTINCT") || p.at_keyword("DISTINCTROW"))
        {
            return Err(p.expected("one of ALL and DISTINCT"));
        }
        if unset && p.eat_keyword("ALL") {
            quantifier = SetQuantifier::All;
        } else if unset && (p.eat_keyword("DISTINCT") || p.eat_keyword("DISTINCTROW")) {
            quantifier = SetQuantifier::Distinct;
        } else if p.eat_keyword("HIGH_PRIORITY") {
            select.mysql.high_priority = true;
        } else if p.eat_keyword("STRAIGHT_JOIN") {
            select.mysql.straight_join = true;
        } else if p.eat_keyword("SQL_SMALL_RESULT") {
            select.mysql.small_result = true;
        } else if p.eat_keyword("SQL_BIG_RESULT") {
            select.mysql.big_result = true;
        } else if p.eat_keyword("SQL_BUFFER_RESULT") {
            select.mysql.buffer_result = true;
        } else if p.eat_keyword("SQL_NO_CACHE") {
            select.mysql.no_cache = true;
        } else if p.eat_keyword("SQL_CALC_FOUND_ROWS") {
            select.mysql.calc_found_rows = true;
        } else {
            return Ok((select, quantifier));
        }
    }
}

pub fn parse_int_div<'a, D>(
    p: &mut Parser<'a, D>,
    left: Expr<'a, D::Ext>,
    bp: u8,
) -> PResult<Expr<'a, D::Ext>>
where
    D: Dialect,
    ExprExtOf<'a, D>: From<MySqlExpr<'a, D::Ext>>,
{
    p.advance();
    let right = p.parse_expr_bp(bp)?;
    let node = MySqlExpr::IntDiv {
        left: Box::new(left),
        right: Box::new(right),
    };
    Ok(ExprKind::Ext(node.into()).into())
}

/// Accepts `LIMIT offset, count` as well as the standard form.
pub fn parse_limit<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Limit<'a, D::Ext>> {
    let first = p.parse_expr()?;
    if p.eat_op(",") {
        let count = p.parse_expr()?;
        Ok(Limit {
            count: Some(count),
            offset: Some(first),
            offset_rows: OffsetRows::None,
            offset_comma: true,
            all: false,
            by: Vec::new(),
        })
    } else {
        grammar::parse_limit_offset(p, first)
    }
}

pub fn parse_column_option<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<Option<ColumnOption<'a, D::Ext>>> {
    let next = p.peek_nth(1);
    let keyword = |keyword: &'static str, expr| ColumnOption::Keyword {
        keyword: keyword.into(),
        expr,
    };
    if p.eat_keyword("AUTO_INCREMENT") {
        return Ok(Some(ColumnOption::AutoIncrement));
    }
    if p.at_keyword("CHARACTER") && next.is_word("SET") {
        p.advance();
        p.advance();
        let name = p.parse_name_part()?;
        return Ok(Some(keyword(
            "CHARACTER SET",
            Some(Expr::new(ExprKind::Ident(name.clone()), name.span)),
        )));
    }
    if p.at_keyword("COMMENT") && next.kind == TokenKind::String {
        p.advance();
        let token = p.advance();
        return Ok(Some(ColumnOption::Comment(D::string_literal(token))));
    }
    if p.at_keyword("SRID") {
        p.advance();
        return Ok(Some(keyword("SRID", Some(p.parse_expr()?))));
    }
    if p.at_keyword("ON") && next.is_word("UPDATE") {
        p.advance();
        p.advance();
        return Ok(Some(keyword("ON UPDATE", Some(p.parse_expr()?))));
    }
    if p.at_keyword("KEY") {
        p.advance();
        return Ok(Some(ColumnOption::PrimaryKey));
    }
    if p.at_keyword("COLLATE") && next.kind == TokenKind::String {
        p.advance();
        return parse_string_collation(p).map(|name| Some(ColumnOption::Collate(name)));
    }
    if p.eat_keyword("INVISIBLE") {
        return Ok(Some(keyword("INVISIBLE", None)));
    }
    if p.at_keyword("UNIQUE") && next.is_word("KEY") {
        p.advance();
        p.advance();
        return Ok(Some(keyword("UNIQUE KEY", None)));
    }
    if p.at_keyword("AS") && next.is_op("(") {
        p.advance();
        p.advance();
        let expr = p.parse_expr()?;
        p.expect_op(")")?;
        let storage = if p.eat_keyword("STORED") {
            Some(GeneratedStorage::Stored)
        } else if p.eat_keyword("VIRTUAL") {
            Some(GeneratedStorage::Virtual)
        } else {
            None
        };
        let generated = Generated {
            when: Some(GeneratedWhen::Omitted),
            kind: GeneratedKind::Expr { expr, storage },
        };
        return Ok(Some(ColumnOption::Generated(Box::new(generated))));
    }
    grammar::parse_column_option(p)
}

/// Parses a data type, with `SIGNED`, `UNSIGNED` and `ZEROFILL` after the
/// arguments, and the integer and string-list types.
pub fn parse_data_type<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<DataType<'a>> {
    let token = p.peek();
    let next = p.peek_nth(1);
    let is_list = (token.is_word("ENUM") || token.is_word("SET")) && next.is_op("(");
    if is_list {
        let name = plain_name(p.advance());
        p.expect_op("(")?;
        let args = p.parse_comma_separated(|p| parse_string_text(p).map(Cow::Borrowed))?;
        p.expect_op(")")?;
        return Ok(DataType {
            qualifier: Vec::new(),
            name,
            angle: None,
            args,
            suffix: DataTypeSuffix::SignedArgs([0, 0]),
            array: crate::ast::ArrayType::None,
        });
    }
    if token.is_word("SIGNED") || token.is_word("UNSIGNED") {
        let name = plain_name(p.advance());
        let suffix = if p.eat_keyword("INTEGER") {
            DataTypeSuffix::Integer
        } else {
            DataTypeSuffix::None
        };
        if p.at_op("(") {
            return Err(p.expected("no display width"));
        }
        return Ok(DataType {
            qualifier: Vec::new(),
            name,
            angle: None,
            args: Vec::new(),
            suffix,
            array: grammar::parse_array_suffix(p)?,
        });
    }
    if token.is_word("ARRAY") {
        return Err(p.expected("a data type"));
    }
    let mut ty = grammar::parse_data_type(p)?;
    if !is_numeric_type(&ty.name.text) {
        return Ok(ty);
    }
    let unsigned = if p.eat_keyword("UNSIGNED") {
        true
    } else {
        p.eat_keyword("SIGNED");
        false
    };
    let zerofill = p.eat_keyword("ZEROFILL");
    ty.suffix = match (ty.suffix, unsigned, zerofill) {
        (DataTypeSuffix::None, true, false) => DataTypeSuffix::Unsigned,
        (DataTypeSuffix::None, false, true) => DataTypeSuffix::Zerofill,
        (DataTypeSuffix::None, true, true) => DataTypeSuffix::UnsignedZerofill,
        (DataTypeSuffix::Precision, true, false) => DataTypeSuffix::PrecisionUnsigned,
        (other, false, false) => other,
        _ => return Err(p.expected("a type with one sign suffix")),
    };
    Ok(ty)
}

/// True for the types that can take `SIGNED`, `UNSIGNED` or `ZEROFILL`.
fn is_numeric_type(name: &str) -> bool {
    [
        "TINYINT",
        "SMALLINT",
        "MEDIUMINT",
        "INT",
        "INTEGER",
        "BIGINT",
        "DECIMAL",
        "DEC",
        "NUMERIC",
        "FIXED",
        "FLOAT",
        "DOUBLE",
        "REAL",
    ]
    .iter()
    .any(|numeric| numeric.eq_ignore_ascii_case(name))
}

fn plain_name<'a>(token: Token<'a>) -> Ident<'a> {
    Ident {
        text: token.text.into(),
        quote: None,
        span: token.span,
    }
}

fn parse_string_text<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<&'a str> {
    if p.peek().kind != TokenKind::String {
        return Err(p.expected("a string"));
    }
    Ok(p.advance().text)
}

impl Dialect for MySql {
    type Ext = Builtin;

    fn parse_create_extension<'a>(
        p: &mut Parser<'a, Self>,
        flags: CreateFlags,
    ) -> PResult<Option<Statement<'a, Builtin>>> {
        grammar::parse_create_role(p, flags, true)
    }

    fn parse_select_into<'a>(
        p: &mut Parser<'a, Self>,
    ) -> PResult<Option<Box<SelectInto<'a, Builtin>>>> {
        grammar::parse_select_into_variables(p)
    }

    fn parse_statement_after_with<'a>(
        p: &mut Parser<'a, Self>,
    ) -> PResult<Option<Statement<'a, Builtin>>> {
        grammar::parse_dml_after_with(p, false, true)
    }

    fn parse_create_table_query<'a>(p: &mut Parser<'a, Self>) -> PResult<Query<'a, Builtin>> {
        grammar::parse_table_or_query(p)
    }

    fn parse_insert_into<'a>(p: &mut Parser<'a, Self>) -> PResult<bool> {
        Ok(p.eat_keyword("INTO"))
    }

    fn parse_statement<'a>(p: &mut Parser<'a, Self>) -> PResult<Statement<'a, Builtin>> {
        parse_statement(p)
    }

    fn parse_create<'a>(p: &mut Parser<'a, Self>) -> PResult<Statement<'a, Builtin>> {
        parse_create(p)
    }

    fn parse_insert_statement<'a>(p: &mut Parser<'a, Self>) -> PResult<Statement<'a, Builtin>> {
        parse_insert_statement(p)
    }

    fn parse_insert_columns<'a>(p: &mut Parser<'a, Self>) -> PResult<Vec<Ident<'a>>> {
        parse_insert_columns(p)
    }

    fn parse_insert_source<'a>(p: &mut Parser<'a, Self>) -> PResult<InsertSource<'a, Builtin>> {
        parse_insert_source(p)
    }

    fn parse_update<'a>(p: &mut Parser<'a, Self>) -> PResult<Update<'a, Builtin>> {
        parse_update(p)
    }

    fn parse_delete<'a>(p: &mut Parser<'a, Self>) -> PResult<Delete<'a, Builtin>> {
        parse_delete(p)
    }

    fn parse_table_constraint<'a>(
        p: &mut Parser<'a, Self>,
    ) -> PResult<Option<TableConstraint<'a, Builtin>>> {
        parse_table_constraint(p)
    }

    fn parse_data_type<'a>(p: &mut Parser<'a, Self>) -> PResult<DataType<'a>> {
        parse_data_type(p)
    }

    fn eat_drop_temporary<'a>(p: &mut Parser<'a, Self>) -> bool {
        p.eat_keyword("TEMPORARY")
    }

    fn parse_grant_name<'a>(p: &mut Parser<'a, Self>) -> PResult<GrantName<'a>> {
        parse_grant_name(p)
    }

    fn parse_grantee<'a>(p: &mut Parser<'a, Self>) -> PResult<Grantee<'a>> {
        parse_mysql_grantee(p)
    }

    fn parse_function_separator<'a>(
        p: &mut Parser<'a, Self>,
    ) -> PResult<Option<StringLiteral<'a>>> {
        parse_function_separator(p)
    }

    fn at_query_values<'a>(p: &Parser<'a, Self>, n: usize) -> bool {
        at_row_values(p, n) || grammar::at_query_values(p, n)
    }

    fn parse_query_values<'a>(p: &mut Parser<'a, Self>) -> PResult<SetExpr<'a, Builtin>> {
        parse_query_values(p)
    }

    fn parse_table_ref<'a>(p: &mut Parser<'a, Self>) -> PResult<TableRef<'a, Builtin>> {
        parse_table_ref(p)
    }

    fn parse_table_factor<'a>(p: &mut Parser<'a, Self>) -> PResult<TableFactor<'a, Builtin>> {
        parse_table_factor(p)
    }

    fn parse_join<'a>(p: &mut Parser<'a, Self>) -> PResult<Option<Join<'a, Builtin>>> {
        parse_join(p)
    }

    fn parse_select_item_alias<'a>(
        p: &mut Parser<'a, Self>,
        expr: Expr<'a, Builtin>,
    ) -> PResult<SelectItem<'a, Builtin>> {
        parse_select_item_alias(p, expr)
    }

    fn parse_group_by_modifier<'a>(p: &mut Parser<'a, Self>) -> Option<GroupByModifier> {
        grammar::parse_group_by_with(p, false)
    }

    fn parse_index_settings<'a>(p: &mut Parser<'a, Self>) -> PResult<Vec<(&'static str, &'a str)>> {
        parse_index_settings(p)
    }

    fn parse_prefix<'a>(p: &mut Parser<'a, Self>) -> PResult<Expr<'a, Builtin>> {
        parse_prefix(p)
    }

    fn parse_alter_table_op<'a>(p: &mut Parser<'a, Self>) -> PResult<AlterTableOp<'a, Builtin>> {
        parse_alter_table_op(p)
    }

    fn parse_set<'a>(p: &mut Parser<'a, Self>) -> PResult<SetStatement<'a, Builtin>> {
        if grammar::at_set_names(p) {
            return grammar::parse_set_names(p);
        }
        if grammar::at_special_set(p) {
            return grammar::parse_set(p);
        }
        grammar::parse_set_assignments(p)
    }

    fn parse_drop_object_type<'a>(p: &mut Parser<'a, Self>) -> Option<ObjectType<'a>> {
        grammar::eat_drop_object_type(p, &["TRIGGER"])
    }

    fn parse_select_head<'a>(
        p: &mut Parser<'a, Self>,
    ) -> PResult<(BuiltinSelect<'a>, SetQuantifier)> {
        parse_select_head(p)
    }

    #[inline]
    fn string_literal(token: Token<'_>) -> StringLiteral<'_> {
        string_literal(token)
    }

    /// MySQL has no `FILTER` clause.
    fn parse_function_filter<'a>(_p: &mut Parser<'a, Self>) -> PResult<Option<Expr<'a, Builtin>>> {
        Ok(None)
    }

    #[inline]
    fn lex_comment(rest: &str) -> Option<Result<usize, &'static str>> {
        lex_comment(rest)
    }

    fn parse_insert_modifiers<'a>(p: &mut Parser<'a, Self>) -> PResult<BuiltinInsert<'a>> {
        let priority = ["LOW_PRIORITY", "DELAYED", "HIGH_PRIORITY"]
            .into_iter()
            .find(|word| p.eat_keyword(word));
        Ok(BuiltinInsert {
            ignore: p.eat_keyword("IGNORE"),
            priority: priority.map(Cow::Borrowed),
            ..Default::default()
        })
    }

    fn parse_column_option<'a>(
        p: &mut Parser<'a, Self>,
    ) -> PResult<Option<ColumnOption<'a, Builtin>>> {
        parse_column_option(p)
    }

    #[inline]
    fn lex_token(rest: &str) -> Option<LexResult> {
        lex_token(rest)
    }

    #[inline]
    fn lex_after(prev: Option<Token<'_>>, rest: &str) -> Option<LexResult> {
        lex_after(prev, rest)
    }

    #[inline]
    fn is_reserved(word: &str) -> bool {
        is_reserved(word)
    }

    #[inline]
    fn is_reserved_for_alias(word: &str) -> bool {
        is_reserved_for_alias(word)
    }

    fn infix_rule<'a>(p: &Parser<'a, Self>) -> Option<InfixRule<'a, Self>> {
        infix_rule(p)
    }

    fn parse_limit<'a>(p: &mut Parser<'a, Self>) -> PResult<Limit<'a, Builtin>> {
        parse_limit(p)
    }
}

/// Builds a string literal from its token. `B'101'` keeps its prefix, and a
/// backslash only escapes in the other strings.
#[inline]
pub fn string_literal(token: Token<'_>) -> StringLiteral<'_> {
    let text = token.text;
    let (prefix, national, body) = match text.as_bytes()[0] {
        b'b' | b'B' => (StringPrefix::Byte, false, &text[1..]),
        b'n' | b'N' => (StringPrefix::None, true, &text[1..]),
        _ => (StringPrefix::None, false, text),
    };
    StringLiteral {
        text: Cow::Borrowed(&body[1..body.len() - 1]),
        quote: char::from(body.as_bytes()[0]),
        backslash_escapes: prefix == StringPrefix::None,
        national,
        prefix,
        triple: false,
    }
}

/// Writes adjacent strings as one literal in the quotes of the first, as
/// the original does.
fn write_concatenated(f: &mut fmt::Formatter<'_>, parts: &[StringLiteral<'_>]) -> fmt::Result {
    let quote = parts[0].quote;
    f.write_char(quote)?;
    for part in parts {
        for c in part.value().chars() {
            if c == quote {
                f.write_char(quote)?;
            }
            if c == '\\' {
                f.write_char('\\')?;
            }
            f.write_char(c)?;
        }
    }
    f.write_char(quote)
}

impl<X: AstExt> fmt::Display for MySqlExpr<'_, X> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MySqlExpr::IntDiv { left, right } => {
                fmt::Display::fmt(&**left, f)?;
                f.write_str(" DIV ")?;
                fmt::Display::fmt(&**right, f)
            }
            MySqlExpr::MemberOf { value, array } => {
                fmt::Display::fmt(&**value, f)?;
                f.write_str(" MEMBER OF(")?;
                fmt::Display::fmt(&**array, f)?;
                f.write_str(")")
            }
            MySqlExpr::Convert(convert) => convert.fmt(f),
            MySqlExpr::Introduced(introduced) => {
                write!(f, "{} {}", introduced.charset, introduced.value)
            }
            MySqlExpr::ConcatenatedStrings(parts) => write_concatenated(f, parts),
            MySqlExpr::MatchAgainst {
                columns,
                value,
                modifier,
            } => {
                f.write_str("MATCH (")?;
                for (i, column) in columns.iter().enumerate() {
                    if i > 0 {
                        f.write_str(", ")?;
                    }
                    write!(f, "{column}")?;
                }
                write!(f, ") AGAINST ({value}")?;
                f.write_str(match modifier {
                    Some(SearchModifier::NaturalLanguage) => " IN NATURAL LANGUAGE MODE",
                    Some(SearchModifier::NaturalLanguageWithQueryExpansion) => {
                        " IN NATURAL LANGUAGE MODE WITH QUERY EXPANSION"
                    }
                    Some(SearchModifier::Boolean) => " IN BOOLEAN MODE",
                    Some(SearchModifier::WithQueryExpansion) => " WITH QUERY EXPANSION",
                    None => "",
                })?;
                f.write_str(")")
            }
        }
    }
}

/// Parses `ALGORITHM` and `LOCK` after an index definition.
pub fn parse_index_settings<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<Vec<(&'static str, &'a str)>> {
    let mut settings = Vec::new();
    while let Some(name) = ["ALGORITHM", "LOCK"]
        .into_iter()
        .find(|word| p.at_keyword(word))
    {
        p.advance();
        p.eat_op("=");
        let token = p.expect_kind(TokenKind::Word, "a value")?;
        settings.push((name, token.text));
    }
    Ok(settings)
}

impl<X: AstExt> fmt::Display for MySqlConvert<'_, X> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CONVERT({}", self.expr)?;
        if let Some(using) = &self.using {
            write!(f, " USING {using}")?;
        }
        if let Some(data_type) = &self.data_type {
            write!(f, ", {data_type}")?;
        }
        if let Some(charset) = &self.charset {
            write!(f, " CHARACTER SET {charset}")?;
        }
        f.write_str(")")
    }
}

/// Reads a string as a name, for places that take either.
fn string_ident<'a>(token: Token<'a>) -> Ident<'a> {
    Ident {
        text: Cow::Borrowed(&token.text[1..token.text.len() - 1]),
        quote: token.text.chars().next(),
        span: token.span,
    }
}

/// Parses `SEPARATOR 'text'` if it is here.
pub fn parse_function_separator<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<Option<StringLiteral<'a>>> {
    if !p.eat_keyword("SEPARATOR") {
        return Ok(None);
    }
    if p.peek().kind != TokenKind::String {
        return Err(p.expected("a string after SEPARATOR"));
    }
    let token = p.advance();
    Ok(Some(D::string_literal(token)))
}

/// Parses a select item's alias, which can be a string.
pub fn parse_select_item_alias<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
    expr: Expr<'a, D::Ext>,
) -> PResult<SelectItem<'a, D::Ext>> {
    let with_as = p.at_keyword("AS") && p.peek_nth(1).kind == TokenKind::String;
    if with_as {
        p.advance();
    }
    if with_as || p.peek().kind == TokenKind::String {
        let token = p.advance();
        return Ok(SelectItemKind::Expr {
            expr,
            alias: Some(string_ident(token)),
        }
        .into());
    }
    grammar::parse_select_item_alias(p, expr)
}

/// True if the token `n` ahead starts `VALUES ROW(...)`, which is only a query
/// when `(` follows `ROW`.
pub fn at_row_values<'a, D: Dialect>(p: &Parser<'a, D>, n: usize) -> bool {
    p.peek_nth(n).is_word("VALUES")
        && p.peek_nth(n + 1).is_word("ROW")
        && p.peek_nth(n + 2).is_op("(")
}

/// Parses `(a, b)`, or `()` in `INSERT`.
fn parse_row<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Vec<Expr<'a, D::Ext>>> {
    p.expect_op("(")?;
    let row = if p.at_op(")") {
        Vec::new()
    } else {
        p.parse_comma_separated(Parser::parse_expr)?
    };
    p.expect_op(")")?;
    Ok(row)
}

/// Parses a list of rows that each start with `ROW`.
fn parse_row_list<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Vec<Vec<Expr<'a, D::Ext>>>> {
    p.parse_comma_separated(|p| {
        p.expect_keyword("ROW")?;
        parse_row(p)
    })
}

/// Parses the rows of a `VALUES` query, where `ROW` is optional.
pub fn parse_query_values<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<SetExpr<'a, D::Ext>> {
    if p.at_keyword("ROW") {
        return parse_row_list(p).map(SetExpr::RowValues);
    }
    grammar::parse_query_values(p)
}

/// Parses the column list of an `INSERT`, which can be empty.
pub fn parse_insert_columns<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Vec<Ident<'a>>> {
    if p.at_op("(") && p.peek_nth(1).is_op(")") {
        p.advance();
        p.advance();
        return Ok(Vec::new());
    }
    grammar::parse_insert_columns(p)
}

/// Parses what follows the table of an `INSERT`: rows, which can be empty and
/// can use `VALUE` or `ROW`, `SET` assignments, or a query.
pub fn parse_insert_source<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<InsertSource<'a, D::Ext>> {
    if p.at_keyword("SET") {
        p.advance();
        return p
            .parse_comma_separated(D::parse_assignment)
            .map(InsertSource::Set);
    }
    let value_keyword =
        p.at_keyword("VALUES") || (p.at_keyword("VALUE") && p.peek_nth(1).is_op("("));
    if !value_keyword {
        return grammar::parse_insert_source(p);
    }
    p.advance();
    if p.at_keyword("ROW") {
        return parse_row_list(p).map(InsertSource::RowValues);
    }
    p.parse_comma_separated(parse_row).map(InsertSource::Values)
}

pub fn parse_update<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Update<'a, D::Ext>> {
    p.expect_keyword("UPDATE")?;
    let table = p.parse_table_ref()?;
    let mut joins = Vec::new();
    while let Some(join) = D::parse_join(p)? {
        joins.push(join);
    }
    p.expect_keyword("SET")?;
    let assignments = p.parse_comma_separated(D::parse_assignment)?;
    let output = D::parse_output_clause(p)?;
    let selection = grammar::parse_optional_where(p)?;
    let order_by = parse_order_by(p)?;
    let limit = parse_trailing_limit(p)?;
    let returning = D::parse_returning(p)?;
    Ok(Update {
        table,
        joins,
        assignments,
        from: Vec::new(),
        from_before_set: false,
        output,
        selection,
        order_by,
        limit,
        returning,
    })
}

pub fn parse_delete<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Delete<'a, D::Ext>> {
    p.expect_keyword("DELETE")?;
    if !p.at_keyword("FROM") {
        let targets = p.parse_comma_separated(grammar::parse_object_name)?;
        p.expect_keyword("FROM")?;
        let using = D::parse_from_list(p)?;
        return finish_multi_table_delete(p, targets, using, true);
    }
    p.advance();
    let table = p.parse_table_ref()?;
    if p.at_op(",") {
        let TableFactor::Named(first) = table.factor else {
            return Err(p.expected("a table name"));
        };
        let mut targets = vec![first];
        while p.eat_op(",") {
            targets.push(grammar::parse_object_name(p)?);
        }
        p.expect_keyword("USING")?;
        let using = D::parse_from_list(p)?;
        return finish_multi_table_delete(p, targets, using, false);
    }
    let using = grammar::parse_optional_delete_using(p)?;
    let output = D::parse_output_clause(p)?;
    let selection = grammar::parse_optional_where(p)?;
    let order_by = parse_order_by(p)?;
    let limit = parse_trailing_limit(p)?;
    let returning = D::parse_returning(p)?;
    Ok(Delete {
        table,
        multi: None,
        using,
        output,
        selection,
        order_by,
        limit,
        returning,
    })
}

/// Finishes `DELETE a, b FROM ...` or `DELETE FROM a, b USING ...`.
fn finish_multi_table_delete<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
    targets: Vec<ObjectName<'a>>,
    using: Vec<TableWithJoins<'a, D::Ext>>,
    from_keyword: bool,
) -> PResult<Delete<'a, D::Ext>> {
    let selection = grammar::parse_optional_where(p)?;
    let table = TableRef {
        factor: TableFactor::Named(targets[0].clone()),
        alias: None,
        span: Span::default(),
    };
    Ok(Delete {
        table,
        multi: Some(Box::new(MultiTableDelete {
            targets,
            from_keyword,
        })),
        using,
        output: None,
        selection,
        order_by: Vec::new(),
        limit: None,
        returning: Vec::new(),
    })
}

fn parse_order_by<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Vec<OrderByItem<'a, D::Ext>>> {
    if p.eat_keyword("ORDER") {
        p.expect_keyword("BY")?;
        return p.parse_comma_separated(D::parse_order_by_item);
    }
    Ok(Vec::new())
}

fn parse_trailing_limit<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<Option<Box<Limit<'a, D::Ext>>>> {
    if p.eat_keyword("LIMIT") {
        return Ok(Some(Box::new(D::parse_limit(p)?)));
    }
    Ok(None)
}

/// Parses a name in `GRANT ... ON`, which can be `*`, `*.*` or `db.*`.
pub fn parse_grant_name<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<GrantName<'a>> {
    let star = |span| Ident {
        text: "*".into(),
        quote: None,
        span,
    };
    let mut parts = Vec::with_capacity(2);
    if p.at_op("*") {
        parts.push(star(p.advance().span));
    } else if p.peek_nth(1).is_op(".") && p.peek_nth(2).is_op("*") {
        parts.push(p.parse_ident()?);
    } else {
        return grammar::parse_grant_name(p);
    }
    if p.eat_op(".") {
        p.expect_op("*")?;
        parts.push(star(p.peek().span));
    }
    Ok(GrantName {
        name: ObjectName { parts },
        args: None,
    })
}

/// True if the name just read is followed by `@host`, which the lexer may
/// have read as a variable.
fn at_host<'a, D: Dialect>(p: &Parser<'a, D>, n: usize) -> bool {
    let token = p.peek_nth(n);
    token.is_op("@") || (token.kind == TokenKind::Word && token.text.starts_with('@'))
}

/// Parses a grantee: a role or other named kind as in the core, or a user
/// written as a string or `name@host`.
fn parse_mysql_grantee<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Grantee<'a>> {
    if p.peek().kind == TokenKind::String || at_host(p, 1) {
        parse_user_host(p)
    } else {
        grammar::parse_grantee(p)
    }
}

/// Parses a user as `name`, `'name'` or `name@host`.
pub fn parse_user_host<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Grantee<'a>> {
    let token = p.peek();
    let name = if token.kind == TokenKind::String {
        p.advance();
        ObjectName {
            parts: vec![string_ident(token)],
        }
    } else {
        grammar::parse_object_name(p)?
    };
    let mut grantee = Grantee {
        kind: Vec::new(),
        name,
        host: None,
    };
    if p.eat_op("@") {
        let host = p.peek();
        grantee.host = Some(match host.kind {
            TokenKind::String | TokenKind::QuotedIdent => string_ident(p.advance()),
            TokenKind::Word => plain_name(p.advance()),
            _ => return Err(p.expected("a host name")),
        });
    } else if p.peek().kind == TokenKind::Word && p.peek().text.starts_with('@') {
        // `user@host` lexes `@host` as a variable.
        let token = p.advance();
        grantee.host = Some(Ident {
            text: Cow::Borrowed(&token.text[1..]),
            quote: None,
            span: token.span,
        });
    }
    Ok(grantee)
}

/// Parses `name PARTITION (a, b)`, `JSON_TABLE(...)` or a core table factor.
pub fn parse_table_factor<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<TableFactor<'a, Builtin>> {
    if p.at_keyword("JSON_TABLE") && p.peek_nth(1).is_op("(") {
        p.advance();
        return parse_json_table(p).map(mysql_factor);
    }
    let factor = grammar::parse_table_factor(p)?;
    let TableFactor::Named(name) = factor else {
        return Ok(factor);
    };
    if !p.eat_keyword("PARTITION") {
        return Ok(TableFactor::Named(name));
    }
    let partitions = grammar::parse_ident_list(p)?;
    Ok(mysql_factor(MySqlTableFactor::Partitioned {
        name,
        partitions,
    }))
}

fn mysql_factor(factor: MySqlTableFactor<'_>) -> TableFactor<'_, Builtin> {
    TableFactor::Ext(BuiltinTableFactor::MySql(Box::new(factor)))
}

/// Parses a table and its alias, then any index hints.
pub fn parse_table_ref<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<TableRef<'a, Builtin>> {
    let table = grammar::parse_table_ref(p)?;
    let mut hints = Vec::new();
    while let Some(hint) = parse_index_hint(p)? {
        hints.push(hint);
    }
    if hints.is_empty() {
        return Ok(table);
    }
    Ok(TableRef {
        factor: mysql_factor(MySqlTableFactor::Hinted { table, hints }),
        alias: None,
        span: Span::default(),
    })
}

/// Parses `USE`, `IGNORE` or `FORCE` `INDEX` or `KEY`, if a hint is here.
fn parse_index_hint<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Option<MySqlIndexHint<'a>>> {
    let Some(verb) = ["USE", "IGNORE", "FORCE"]
        .into_iter()
        .find(|word| p.at_keyword(word))
    else {
        return Ok(None);
    };
    let next = p.peek_nth(1);
    if !(next.is_word("INDEX") || next.is_word("KEY")) {
        return Ok(None);
    }
    p.advance();
    let key = p.advance().is_word("KEY");
    let target = if p.eat_keyword("FOR") {
        if p.eat_keyword("JOIN") {
            Some("JOIN")
        } else if p.eat_keyword("ORDER") {
            p.expect_keyword("BY")?;
            Some("ORDER BY")
        } else {
            p.expect_keyword("GROUP")?;
            p.expect_keyword("BY")?;
            Some("GROUP BY")
        }
    } else {
        None
    };
    p.expect_op("(")?;
    let names = if p.at_op(")") {
        Vec::new()
    } else {
        p.parse_comma_separated(Parser::parse_ident)?
    };
    p.expect_op(")")?;
    Ok(Some(MySqlIndexHint {
        verb: verb.into(),
        key,
        target: target.map(Cow::Borrowed),
        names,
    }))
}

/// Parses a join, adding `STRAIGHT_JOIN` and a condition on `CROSS JOIN`.
pub fn parse_join<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Option<Join<'a, D::Ext>>> {
    if p.eat_keyword("STRAIGHT_JOIN") {
        let table = p.parse_table_ref()?;
        let constraint = grammar::parse_join_constraint(p)?;
        return Ok(Some(Join {
            kind: JoinKind::Custom("STRAIGHT_JOIN".into()),
            spelled_out: false,
            table,
            constraint,
        }));
    }
    let Some(mut join) = grammar::parse_join(p)? else {
        return Ok(None);
    };
    if join.kind == JoinKind::Cross {
        join.constraint = grammar::parse_join_constraint(p)?;
    }
    Ok(Some(join))
}

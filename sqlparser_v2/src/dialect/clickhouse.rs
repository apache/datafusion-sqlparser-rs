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

//! ClickHouse: its query clauses, table engines and administrative statements.

use crate::ast::Span;
use std::borrow::Cow;
use std::fmt;

use crate::PResult;
use crate::ast::{
    AlterTable, AlterTableOp, AngleArguments, Assignment, BinaryOp, ColumnDef, ColumnOption,
    CreateFlags, CreateTable, CreateView, DataType, DataTypeSuffix, Delete, ExplainTarget, Expr,
    ExprKind, FunctionOptions, GroupByModifier, Ident, Insert, InsertSource, IsTest, Join,
    JoinConstraint, JoinKind, Limit, ObjectName, OffsetRows, OrderByItem, Query, SampleClause,
    Select, SelectItem, SelectItemKind, SetQuantifier, SpecialExpr, Statement, StatementKind,
    StringLiteral, StructField, TableConstraint, TableFactor, TableRef, TableSample, Truncate,
    Update, WildcardOptions,
};
use crate::dialect::builtin::{
    BuiltinExpr, BuiltinQueryClauses, BuiltinQueryTail, BuiltinStatement,
};
use crate::dialect::snowflake;
use crate::dialect::{Builtin, Dialect, mysql, postgres, spark};
use crate::display::{
    Indent, SpaceOrNewline, comma, write_columns, write_create_flags, write_if_exists,
    write_if_not_exists, write_list_clause, write_where_clause,
};
use crate::grammar::{self, prec};
use crate::lexer::{self, LexResult, Lexeme, Token, TokenKind, quoted, quoted_with_backslash};
use crate::parser::{InfixRule, Parser};

#[derive(Debug, Clone, PartialEq)]
pub struct ClickHouse;

/// Statements only ClickHouse has, and forms of common ones with its clauses.
#[derive(Debug, Clone, PartialEq)]
pub enum ClickHouseStatement<'a> {
    /// `KILL [CONNECTION | QUERY | MUTATION] id`
    Kill {
        kind: Option<Cow<'a, str>>,
        id: Cow<'a, str>,
    },
    Optimize(Box<ClickHouseOptimize<'a>>),
    TruncateOnCluster(Box<ClickHouseTruncate<'a>>),
    AlterTable(Box<ClickHouseAlterTable<'a>>),
    CreateTable(Box<ClickHouseCreateTable<'a>>),
    CreateView(Box<ClickHouseCreateView<'a>>),
    Insert(Box<ClickHouseInsert<'a>>),
    Update(Box<ClickHouseUpdate<'a>>),
    Delete(Box<ClickHouseDelete<'a>>),
}

/// `OPTIMIZE TABLE name ...`
#[derive(Debug, Clone, PartialEq)]
pub struct ClickHouseOptimize<'a> {
    pub name: ObjectName<'a>,
    pub on_cluster: Option<Ident<'a>>,
    pub partition: Option<ClickHouseOptimizePartition<'a>>,
    pub include_final: bool,
    pub deduplicate: Option<Deduplicate<'a>>,
}

/// `PARTITION expr` or `PARTITION ID 'name'`.
#[derive(Debug, Clone, PartialEq)]
pub enum ClickHouseOptimizePartition<'a> {
    Expr(Expr<'a, Builtin>),
    Id(Ident<'a>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Deduplicate<'a> {
    All,
    By(Expr<'a, Builtin>),
}

/// `ALTER TABLE` with clauses the core does not have.
#[derive(Debug, Clone, PartialEq)]
pub struct ClickHouseAlterTable<'a> {
    pub if_exists: bool,
    pub name: ObjectName<'a>,
    pub on_cluster: Option<Ident<'a>>,
    pub operations: Vec<ClickHouseAlterOp<'a>>,
}

/// An operation of `ALTER TABLE`: a core one or one only ClickHouse has.
#[derive(Debug, Clone, PartialEq)]
pub enum ClickHouseAlterOp<'a> {
    Core(AlterTableOp<'a, Builtin>),
    AddProjection(Box<ClickHouseProjection<'a>>),
    DropProjection {
        if_exists: bool,
        name: Ident<'a>,
    },
    ClearProjection {
        if_exists: bool,
        name: Ident<'a>,
        partition: Option<Ident<'a>>,
    },
    MaterializeProjection {
        if_exists: bool,
        name: Ident<'a>,
        partition: Option<Ident<'a>>,
    },
    Attach(ClickHousePartition<'a>),
    Detach(ClickHousePartition<'a>),
    Freeze {
        partition: ClickHousePartition<'a>,
        with_name: Option<Ident<'a>>,
    },
    Unfreeze {
        partition: ClickHousePartition<'a>,
        with_name: Option<Ident<'a>>,
    },
    ModifyOrderBy(ClickHouseSortingKey<'a>),
}

/// `ADD PROJECTION [IF NOT EXISTS] name (SELECT ...)`
#[derive(Debug, Clone, PartialEq)]
pub struct ClickHouseProjection<'a> {
    pub if_not_exists: bool,
    pub name: Ident<'a>,
    pub projection: Vec<SelectItem<'a, Builtin>>,
    pub group_by: Vec<Expr<'a, Builtin>>,
    pub order_by: Vec<OrderByItem<'a, Builtin>>,
}

/// `PARTITION expr` or `PART expr`.
#[derive(Debug, Clone, PartialEq)]
pub struct ClickHousePartition<'a> {
    pub part: bool,
    pub expr: Expr<'a, Builtin>,
}

/// A single sorting key expression, or a list of them in parentheses.
#[derive(Debug, Clone, PartialEq)]
pub enum ClickHouseSortingKey<'a> {
    One(Expr<'a, Builtin>),
    Many(Vec<Expr<'a, Builtin>>),
}

/// `CREATE TABLE` with clauses the core does not have.
#[derive(Debug, Clone, PartialEq)]
pub struct ClickHouseCreateTable<'a> {
    pub table: CreateTable<'a, Builtin>,
    /// `LOCAL` or `GLOBAL`, written before `TEMPORARY`.
    pub scope: Option<Cow<'a, str>>,
    pub on_cluster: Option<Ident<'a>>,
    pub engine: Option<ClickHouseEngine<'a>>,
    pub primary_key: Option<Expr<'a, Builtin>>,
    pub order_by: Option<ClickHouseSortingKey<'a>>,
    pub partition_by: Option<Expr<'a, Builtin>>,
    /// `DELETE ROWS`, `PRESERVE ROWS` or `DROP`.
    pub on_commit: Option<Cow<'a, str>>,
}

/// `ENGINE = name [(args)]`
#[derive(Debug, Clone, PartialEq)]
pub struct ClickHouseEngine<'a> {
    pub name: Ident<'a>,
    /// Names and strings. A string keeps its quote.
    pub args: Vec<Ident<'a>>,
}

/// `CREATE [MATERIALIZED] VIEW` with typed columns or a target table.
#[derive(Debug, Clone, PartialEq)]
pub struct ClickHouseCreateView<'a> {
    pub or_replace: bool,
    pub materialized: bool,
    pub if_not_exists: bool,
    pub name: ObjectName<'a>,
    pub columns: Vec<ColumnDef<'a, Builtin>>,
    pub to: Option<ObjectName<'a>>,
    pub query: Box<Query<'a, Builtin>>,
}

/// `INSERT` into a table function, or with `SETTINGS` or `FORMAT`.
#[derive(Debug, Clone, PartialEq)]
pub struct ClickHouseInsert<'a> {
    pub table_keyword: bool,
    pub target: ClickHouseInsertTarget<'a>,
    pub columns: Vec<Ident<'a>>,
    pub source: Option<InsertSource<'a, Builtin>>,
    pub settings: Vec<ClickHouseSetting<'a>>,
    pub format: Option<ClickHouseInputFormat<'a>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ClickHouseInsertTarget<'a> {
    Table(ObjectName<'a>),
    /// `FUNCTION name(args)`, always an [`ExprKind::Function`].
    Function(Box<Expr<'a, Builtin>>),
}

/// `FORMAT name [values]`, in an `INSERT`.
#[derive(Debug, Clone, PartialEq)]
pub struct ClickHouseInputFormat<'a> {
    pub name: Ident<'a>,
    pub values: Vec<Expr<'a, Builtin>>,
}

/// `UPDATE` with `ON CLUSTER` or `SETTINGS`.
#[derive(Debug, Clone, PartialEq)]
pub struct ClickHouseUpdate<'a> {
    pub table: TableRef<'a, Builtin>,
    pub on_cluster: Option<Ident<'a>>,
    pub assignments: Vec<Assignment<'a, Builtin>>,
    pub selection: Option<Expr<'a, Builtin>>,
    pub settings: Vec<ClickHouseSetting<'a>>,
}

/// `DELETE` with `ON CLUSTER` or `SETTINGS`.
#[derive(Debug, Clone, PartialEq)]
pub struct ClickHouseDelete<'a> {
    pub table: TableRef<'a, Builtin>,
    pub on_cluster: Option<Ident<'a>>,
    pub selection: Option<Expr<'a, Builtin>>,
    pub settings: Vec<ClickHouseSetting<'a>>,
}

/// `name = value`, in a `SETTINGS` clause.
#[derive(Debug, Clone, PartialEq)]
pub struct ClickHouseSetting<'a> {
    pub name: Ident<'a>,
    pub value: Expr<'a, Builtin>,
}

/// The name in a `FORMAT` clause of a query.
#[derive(Debug, Clone, PartialEq)]
pub enum ClickHouseFormat<'a> {
    Name(Ident<'a>),
    Null,
}

/// `SETTINGS` and `FORMAT`, at the end of a query.
#[derive(Debug, Clone, PartialEq)]
pub struct ClickHouseQueryClauses<'a> {
    pub settings: Vec<ClickHouseSetting<'a>>,
    pub format: Option<ClickHouseFormat<'a>>,
}

/// `WITH FILL` and `INTERPOLATE`, after an `ORDER BY` item.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ClickHouseOrderByItem<'a> {
    pub with_fill: Option<Box<ClickHouseWithFill<'a>>>,
    pub interpolate: Option<Box<ClickHouseInterpolate<'a>>>,
}

/// `TRUNCATE ... ON CLUSTER name`
#[derive(Debug, Clone, PartialEq)]
pub struct ClickHouseTruncate<'a> {
    pub truncate: Truncate<'a>,
    pub on_cluster: Ident<'a>,
}

/// `WITH FILL [FROM x] [TO y] [STEP z]`
#[derive(Debug, Clone, PartialEq)]
pub struct ClickHouseWithFill<'a> {
    pub from: Option<Expr<'a, Builtin>>,
    pub to: Option<Expr<'a, Builtin>>,
    pub step: Option<Expr<'a, Builtin>>,
}

/// `INTERPOLATE [(column [AS expr], ...)]`. `exprs` is `None` when the
/// parentheses are left out, and empty for `INTERPOLATE ()`.
#[derive(Debug, Clone, PartialEq)]
pub struct ClickHouseInterpolate<'a> {
    pub exprs: Option<Vec<(Ident<'a>, Option<Expr<'a, Builtin>>)>>,
}

/// Expression syntax only ClickHouse has.
#[derive(Debug, Clone, PartialEq)]
pub enum ClickHouseExpr<'a> {
    /// `expr.1`, written `expr . 1` when rendered.
    TupleElement {
        expr: Box<Expr<'a, Builtin>>,
        index: Cow<'a, str>,
    },
    /// `[a, b]`
    Array(Vec<Expr<'a, Builtin>>),
    /// `{key: value, ...}`. A string key keeps its quote.
    Dictionary(Vec<(Ident<'a>, Expr<'a, Builtin>)>),
    /// `expr GLOBAL [NOT] IN ...`
    GlobalIn(Box<ClickHouseGlobalIn<'a>>),
    /// A lambda with a type on at least one parameter: `x Int8 -> x`.
    TypedLambda(Box<ClickHouseTypedLambda<'a>>),
    /// A table function call with settings: `f(a, SETTINGS b = 1)`.
    SettingsCall(Box<ClickHouseSettingsCall<'a>>),
    /// `ORDER BY ALL`
    All,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ClickHouseGlobalIn<'a> {
    pub expr: Expr<'a, Builtin>,
    pub negated: bool,
    pub target: ClickHouseInTarget<'a>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ClickHouseInTarget<'a> {
    List(Vec<Expr<'a, Builtin>>),
    Subquery(Box<Query<'a, Builtin>>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct ClickHouseTypedLambda<'a> {
    pub params: Vec<(Ident<'a>, Option<DataType<'a>>)>,
    pub parenthesized: bool,
    pub body: Expr<'a, Builtin>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ClickHouseSettingsCall<'a> {
    pub name: ObjectName<'a>,
    pub args: Vec<Expr<'a, Builtin>>,
    pub settings: Vec<ClickHouseSetting<'a>>,
}

fn ext<'a>(expr: ClickHouseExpr<'a>) -> Expr<'a, Builtin> {
    ExprKind::Ext(BuiltinExpr::ClickHouse(expr)).into()
}

fn statement<'a>(statement: ClickHouseStatement<'a>) -> Statement<'a, Builtin> {
    StatementKind::Ext(BuiltinStatement::ClickHouse(statement)).into()
}

/// Set while parsing a column option's expression, where `NOT NULL` belongs to
/// the column unless parentheses enclose it.
const IN_COLUMN_OPTION: u32 = 1 << 8;

#[inline]
fn is_name_start(byte: u8) -> bool {
    byte.is_ascii_alphabetic() || byte == b'_'
}

#[inline]
fn is_name_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'$')
}

fn digits_with_separators(bytes: &[u8], from: usize) -> Result<usize, &'static str> {
    let mut len = 0;
    while let Some(&byte) = bytes.get(from + len) {
        if byte.is_ascii_digit() {
            len += 1;
        } else if byte == b'_' {
            if len == 0 || !bytes.get(from + len + 1).is_some_and(u8::is_ascii_digit) {
                return Err("misplaced underscore in number");
            }
            len += 1;
        } else {
            break;
        }
    }
    Ok(len)
}

/// Lexes a number, in which underscores can separate digits. Leaves hex
/// numbers and text that does not start with a number to the core.
fn lex_number(rest: &str) -> Option<LexResult> {
    let bytes = rest.as_bytes();
    let leading_point = bytes[0] == b'.' && bytes.get(1).is_some_and(u8::is_ascii_digit);
    if !leading_point && !bytes[0].is_ascii_digit() {
        return None;
    }
    if bytes[0] == b'0' && bytes.get(1) == Some(&b'x') {
        return Some(lex_hex_number(bytes));
    }
    Some(lex_number_body(bytes))
}

fn lex_hex_number(bytes: &[u8]) -> LexResult {
    let mut len = 2;
    while let Some(&byte) = bytes.get(len) {
        let separator =
            byte == b'_' && len > 2 && bytes.get(len + 1).is_some_and(u8::is_ascii_hexdigit);
        if !byte.is_ascii_hexdigit() && !separator {
            break;
        }
        len += 1;
    }
    if bytes
        .get(len)
        .is_some_and(|&b| is_name_start(b) || b >= 0x80)
    {
        return Err("invalid number");
    }
    Ok(Lexeme {
        kind: TokenKind::HexString,
        len,
    })
}

fn lex_number_body(bytes: &[u8]) -> LexResult {
    let mut len = digits_with_separators(bytes, 0)?;
    if bytes.get(len) == Some(&b'.')
        && !bytes
            .get(len + 1)
            .is_some_and(|&b| is_name_start(b) || b == b'.')
    {
        len += 1 + digits_with_separators(bytes, len + 1)?;
    }
    if matches!(bytes.get(len), Some(b'e' | b'E')) {
        let sign = usize::from(matches!(bytes.get(len + 1), Some(b'+' | b'-')));
        let exponent = digits_with_separators(bytes, len + 1 + sign)?;
        if exponent > 0 {
            len += 1 + sign + exponent;
        }
    }
    if bytes
        .get(len)
        .is_some_and(|&b| is_name_start(b) || b >= 0x80)
    {
        return Err("invalid number");
    }
    Ok(Lexeme {
        kind: TokenKind::Number,
        len,
    })
}

/// Reads backtick-quoted names, strings with backslash escapes, names holding
/// `$`, numbers with separators, and operators the core does not have.
#[inline]
pub fn lex_token(rest: &str) -> Option<LexResult> {
    let op = |len| {
        Some(Ok(Lexeme {
            kind: TokenKind::Op,
            len,
        }))
    };
    let bytes = rest.as_bytes();
    match bytes {
        [b'`', ..] => Some(quoted(rest, TokenKind::QuotedIdent)),
        [b'\'', ..] => Some(quoted_with_backslash(rest, TokenKind::String)),
        [b'=', b'=', ..] | [b'-', b'>', ..] | [b':', b':', ..] => op(2),
        [b'{' | b'}', ..] => op(1),
        [b'$', ..] => postgres::dollar_string(rest),
        [b'x' | b'X', b'\'', ..] => None,
        [b, ..] if is_name_start(*b) => Some(Ok(Lexeme {
            kind: TokenKind::Word,
            len: bytes.iter().take_while(|b| is_name_byte(**b)).count(),
        })),
        [b, ..] if *b >= 0x80 => Some(Err("unexpected character")),
        _ => lex_number(rest),
    }
}

/// A point directly after a name, `)`, `]` or a tuple index starts an element
/// access such as `t.1`, never a number, and an element is a whole number.
/// A colon directly after a word or a string separates a dictionary key from
/// its value, even when a name follows it.
#[inline]
pub fn lex_after(prev: Option<Token<'_>>, rest: &str) -> Option<LexResult> {
    let prev = prev?;
    let bytes = rest.as_bytes();
    match bytes {
        [b'.', next, ..] if next.is_ascii_digit() => {
            let continues_value = matches!(prev.kind, TokenKind::Word | TokenKind::QuotedIdent)
                || prev.kind == TokenKind::Number
                || prev.is_op(")")
                || prev.is_op("]");
            continues_value.then_some(Ok(Lexeme {
                kind: TokenKind::Op,
                len: 1,
            }))
        }
        [first, ..] if first.is_ascii_digit() && prev.is_op(".") => Some(Ok(Lexeme {
            kind: TokenKind::Number,
            len: bytes.iter().take_while(|b| b.is_ascii_digit()).count(),
        })),
        [b':', next, ..]
            if *next != b':'
                && matches!(
                    prev.kind,
                    TokenKind::Word | TokenKind::QuotedIdent | TokenKind::String
                ) =>
        {
            Some(Ok(Lexeme {
                kind: TokenKind::Op,
                len: 1,
            }))
        }
        _ => None,
    }
}

/// A `#` starts a comment only before a space or `!`.
#[inline]
pub fn lex_comment(rest: &str) -> Option<Result<usize, &'static str>> {
    match rest.as_bytes() {
        [b'#', b' ' | b'!', ..] => Some(Ok(lexer::line_comment(rest))),
        _ => lexer::lex_core_comment(rest),
    }
}

/// Words that start a clause after a table, on top of the core's.
#[inline]
pub fn is_reserved_for_alias(word: &str) -> bool {
    if word.eq_ignore_ascii_case("QUALIFY") {
        return true;
    }
    ["FORMAT", "SETTINGS", "PREWHERE", "SAMPLE", "GLOBAL"]
        .iter()
        .any(|reserved| reserved.eq_ignore_ascii_case(word))
        || grammar::is_reserved_for_alias(word)
}

fn at_dictionary_key_token(token: Token<'_>) -> bool {
    matches!(
        token.kind,
        TokenKind::Word | TokenKind::QuotedIdent | TokenKind::String
    )
}

/// Parses a name that can also be written as a string, keeping its quote.
fn parse_name_or_string<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Ident<'a>> {
    let token = p.peek();
    if token.kind != TokenKind::String {
        return p.parse_name_part();
    }
    p.advance();
    Ok(Ident {
        text: Cow::Borrowed(&token.text[1..token.text.len() - 1]),
        quote: Some('\''),
        span: token.span,
    })
}

pub fn parse_dictionary<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Expr<'a, Builtin>> {
    p.expect_op("{")?;
    if p.eat_op("}") {
        return Ok(ext(ClickHouseExpr::Dictionary(Vec::new())));
    }
    let fields = p.parse_comma_separated(|p| {
        if !at_dictionary_key_token(p.peek()) {
            return Err(p.expected("a dictionary key"));
        }
        let key = parse_name_or_string(p)?;
        p.expect_op(":")?;
        Ok((key, p.parse_expr()?))
    })?;
    p.expect_op("}")?;
    Ok(ext(ClickHouseExpr::Dictionary(fields)))
}

fn parse_array_literal<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Expr<'a, Builtin>> {
    p.expect_op("[")?;
    let items = if p.at_op("]") {
        Vec::new()
    } else {
        p.parse_comma_separated(Parser::parse_expr)?
    };
    p.expect_op("]")?;
    Ok(ext(ClickHouseExpr::Array(items)))
}

fn is_name(token: Token<'_>) -> bool {
    matches!(token.kind, TokenKind::Word | TokenKind::QuotedIdent)
}

/// True at `name ... ) ->` for a parenthesized lambda with a typed parameter.
pub fn at_typed_lambda<'a, D: Dialect>(p: &Parser<'a, D>) -> bool {
    if is_name(p.peek()) {
        return p.peek_nth(1).kind == TokenKind::Word && p.peek_nth(2).is_op("->");
    }
    if !p.at_op("(") || !is_name(p.peek_nth(1)) {
        return false;
    }
    let mut depth = 0usize;
    let mut typed = false;
    let mut n = 0;
    loop {
        let token = p.peek_nth(n);
        match token.kind {
            TokenKind::Eof => return false,
            TokenKind::Op if token.text == "(" => depth += 1,
            TokenKind::Op if token.text == ")" => {
                depth -= 1;
                if depth == 0 {
                    return typed && p.peek_nth(n + 1).is_op("->");
                }
            }
            TokenKind::Word | TokenKind::QuotedIdent if depth == 1 => {
                let previous = p.peek_nth(n.wrapping_sub(1));
                typed |= n > 0 && is_name(previous);
            }
            _ => {}
        }
        n += 1;
    }
}

fn parse_typed_lambda_param<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<(Ident<'a>, Option<DataType<'a>>)> {
    let name = p.parse_ident()?;
    let data_type = if p.at_op(",") || p.at_op(")") {
        None
    } else {
        Some(D::parse_data_type(p)?)
    };
    Ok((name, data_type))
}

pub fn parse_typed_lambda<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Expr<'a, Builtin>> {
    let parenthesized = p.eat_op("(");
    let params = if parenthesized {
        let params = p.parse_comma_separated(parse_typed_lambda_param)?;
        p.expect_op(")")?;
        params
    } else {
        vec![parse_typed_lambda_param(p)?]
    };
    p.expect_op("->")?;
    let body = p.parse_expr()?;
    Ok(ext(ClickHouseExpr::TypedLambda(Box::new(
        ClickHouseTypedLambda {
            params,
            parenthesized,
            body,
        },
    ))))
}

/// True at a name followed by `.` and a tuple index, possibly after more names.
fn at_name_before_tuple_index<'a, D: Dialect>(p: &Parser<'a, D>) -> bool {
    if !is_name(p.peek()) || !p.peek_nth(1).is_op(".") {
        return false;
    }
    let mut n = 1;
    while p.peek_nth(n).is_op(".") {
        let next = p.peek_nth(n + 1);
        if next.kind == TokenKind::Number {
            return true;
        }
        if !is_name(next) {
            return false;
        }
        n += 2;
    }
    false
}

/// Parses the name in `name.1`, leaving the point for the infix rule.
fn parse_name_before_tuple_index<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Expr<'a, Builtin>> {
    let first = p.parse_ident()?;
    let mut parts = vec![first.clone()];
    while p.at_op(".") && p.peek_nth(1).kind != TokenKind::Number {
        p.advance();
        parts.push(p.parse_name_part()?);
    }
    if parts.len() == 1 {
        return Ok(ExprKind::Ident(first).into());
    }
    Ok(ExprKind::Compound(ObjectName { parts }).into())
}

pub fn parse_prefix<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Expr<'a, Builtin>> {
    if p.at_op("[") {
        return p.without_context(IN_COLUMN_OPTION, parse_array_literal);
    }
    if p.at_op("{") {
        return p.without_context(IN_COLUMN_OPTION, parse_dictionary);
    }
    if p.at_op("(") && p.has_context(IN_COLUMN_OPTION) {
        return p.without_context(IN_COLUMN_OPTION, D::parse_prefix);
    }
    if at_typed_lambda(p) {
        return parse_typed_lambda(p);
    }
    if at_name_before_tuple_index(p) {
        return parse_name_before_tuple_index(p);
    }
    spark::parse_prefix(p)
}

/// How deep a dialect node counts for, compared to a core one.
const EXT_LEVELS: usize = 2;

fn parse_tuple_element<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
    left: Expr<'a, Builtin>,
    _bp: u8,
) -> PResult<Expr<'a, Builtin>> {
    p.advance();
    let token = p.expect_kind(TokenKind::Number, "a tuple index")?;
    p.deepen_by(EXT_LEVELS)?;
    Ok(ext(ClickHouseExpr::TupleElement {
        expr: Box::new(left),
        index: token.text.into(),
    }))
}

fn parse_in_target<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<ClickHouseInTarget<'a>> {
    if !p.eat_op("(") {
        return Ok(ClickHouseInTarget::List(vec![
            p.parse_expr_bp(prec::COMPARE)?,
        ]));
    }
    if grammar::query_starts_at(p, 0) {
        let query = p.nested(D::parse_query)?;
        p.expect_op(")")?;
        return Ok(ClickHouseInTarget::Subquery(Box::new(query)));
    }
    let list = p.parse_comma_separated(Parser::parse_expr)?;
    p.expect_op(")")?;
    Ok(ClickHouseInTarget::List(list))
}

fn parse_global_in<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
    left: Expr<'a, Builtin>,
    _bp: u8,
) -> PResult<Expr<'a, Builtin>> {
    p.expect_keyword("GLOBAL")?;
    let negated = p.eat_keyword("NOT");
    p.expect_keyword("IN")?;
    let target = parse_in_target(p)?;
    p.deepen_by(EXT_LEVELS)?;
    Ok(ext(ClickHouseExpr::GlobalIn(Box::new(
        ClickHouseGlobalIn {
            expr: left,
            negated,
            target,
        },
    ))))
}

fn parse_bare_in<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
    left: Expr<'a, Builtin>,
    _bp: u8,
) -> PResult<Expr<'a, Builtin>> {
    let negated = p.eat_keyword("NOT");
    p.expect_keyword("IN")?;
    let item = p.parse_expr_bp(prec::COMPARE)?;
    Ok(ExprKind::InList {
        expr: Box::new(left),
        list: vec![item],
        negated,
    }
    .into())
}

fn parse_not_null<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
    left: Expr<'a, Builtin>,
    _bp: u8,
) -> PResult<Expr<'a, Builtin>> {
    p.expect_keyword("NOT")?;
    p.expect_keyword("NULL")?;
    Ok(ExprKind::Is {
        expr: Box::new(left),
        test: IsTest::Null,
        negated: true,
    }
    .into())
}

pub fn infix_rule<'a, D: Dialect<Ext = Builtin>>(p: &Parser<'a, D>) -> Option<InfixRule<'a, D>> {
    let token = p.peek();
    if token.kind == TokenKind::Op {
        match token.text {
            "==" => return Some(InfixRule::binary(prec::COMPARE, BinaryOp::Eq)),
            "::" => {
                return Some(InfixRule::custom(
                    prec::POSTFIX,
                    postgres::parse_double_colon,
                ));
            }
            "." if p.peek_nth(1).kind == TokenKind::Number => {
                return Some(InfixRule::custom(prec::POSTFIX, parse_tuple_element));
            }
            _ => {}
        }
    } else if token.kind == TokenKind::Word {
        let next = p.peek_nth(1);
        if p.at_keyword("GLOBAL")
            && (next.is_word("IN") || (next.is_word("NOT") && p.peek_nth(2).is_word("IN")))
        {
            return Some(InfixRule::custom(prec::COMPARE, parse_global_in));
        }
        if p.at_keyword("IN") && !next.is_op("(") {
            return Some(InfixRule::custom(prec::COMPARE, parse_bare_in));
        }
        if p.at_keyword("NOT") && next.is_word("IN") && !p.peek_nth(2).is_op("(") {
            return Some(InfixRule::custom(prec::COMPARE, parse_bare_in));
        }
        if p.at_keyword("NOT") && next.is_word("NULL") && !p.has_context(IN_COLUMN_OPTION) {
            return Some(InfixRule::custom(prec::IS, parse_not_null));
        }
    }
    grammar::infix_rule(p)
}

/// Parses `key = value, ...` after `SETTINGS`.
fn parse_settings_list<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Vec<ClickHouseSetting<'a>>> {
    p.parse_comma_separated(|p| {
        let name = p.parse_ident()?;
        p.expect_op("=")?;
        let value = p.parse_expr()?;
        Ok(ClickHouseSetting { name, value })
    })
}

fn parse_optional_settings<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Vec<ClickHouseSetting<'a>>> {
    if p.eat_keyword("SETTINGS") {
        parse_settings_list(p)
    } else {
        Ok(Vec::new())
    }
}

/// Parses `SETTINGS` and `FORMAT` at the end of a query.
pub fn parse_query_tail<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<BuiltinQueryTail<'a>> {
    let settings = parse_optional_settings(p)?;
    let format = if !p.eat_keyword("FORMAT") {
        None
    } else if p.eat_keyword("NULL") {
        Some(ClickHouseFormat::Null)
    } else {
        Some(ClickHouseFormat::Name(p.parse_ident()?))
    };
    if settings.is_empty() && format.is_none() {
        return Ok(BuiltinQueryTail(None));
    }
    Ok(BuiltinQueryTail(Some(Box::new(
        BuiltinQueryClauses::ClickHouse(ClickHouseQueryClauses { settings, format }),
    ))))
}

/// Parses what follows `LIMIT`: `n`, `offset, n`, `n BY columns` or
/// `n OFFSET m [BY columns]`.
pub fn parse_limit<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Limit<'a, Builtin>> {
    let count = p.parse_expr()?;
    if p.eat_op(",") {
        return Ok(Limit {
            count: Some(p.parse_expr()?),
            offset: Some(count),
            offset_rows: OffsetRows::None,
            offset_comma: true,
            all: false,
            by: Vec::new(),
        });
    }
    let mut by = if p.eat_keyword("BY") {
        p.parse_comma_separated(Parser::parse_expr)?
    } else {
        Vec::new()
    };
    let mut limit = grammar::parse_limit_offset(p, count)?;
    if by.is_empty() && limit.offset.is_some() && p.eat_keyword("BY") {
        by = p.parse_comma_separated(Parser::parse_expr)?;
    }
    limit.by = by;
    Ok(limit)
}

fn parse_with_fill<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<ClickHouseWithFill<'a>> {
    let from = if p.eat_keyword("FROM") {
        Some(p.parse_expr()?)
    } else {
        None
    };
    let to = if p.eat_keyword("TO") {
        Some(p.parse_expr()?)
    } else {
        None
    };
    let step = if p.eat_keyword("STEP") {
        Some(p.parse_expr()?)
    } else {
        None
    };
    Ok(ClickHouseWithFill { from, to, step })
}

fn parse_interpolate<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<ClickHouseInterpolate<'a>> {
    if !p.eat_op("(") {
        return Ok(ClickHouseInterpolate { exprs: None });
    }
    if p.eat_op(")") {
        return Ok(ClickHouseInterpolate {
            exprs: Some(Vec::new()),
        });
    }
    let exprs = p.parse_comma_separated(|p| {
        let column = p.parse_ident()?;
        let expr = if p.eat_keyword("AS") {
            Some(p.parse_expr()?)
        } else {
            None
        };
        Ok((column, expr))
    })?;
    p.expect_op(")")?;
    Ok(ClickHouseInterpolate { exprs: Some(exprs) })
}

/// Parses an `ORDER BY` item, which can be `ALL` or end in `WITH FILL` and
/// `INTERPOLATE`.
pub fn parse_order_by_item<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<OrderByItem<'a, Builtin>> {
    let all = p.at_keyword("ALL") && !p.peek_nth(1).is_op("(");
    let expr = if all {
        let span = p.advance().span;
        Expr::new(ext(ClickHouseExpr::All).kind, span)
    } else {
        p.parse_expr()?
    };
    let (asc, nulls_first) = grammar::parse_order_by_modifiers(p)?;
    if all && p.at_op(",") {
        return Err(p.expected("nothing after ORDER BY ALL"));
    }
    let with_fill = if p.at_keyword("WITH") && p.peek_nth(1).is_word("FILL") {
        p.advance();
        p.advance();
        Some(Box::new(parse_with_fill(p)?))
    } else {
        None
    };
    let interpolate = if p.eat_keyword("INTERPOLATE") {
        let interpolate = parse_interpolate(p)?;
        if p.at_op(",") {
            return Err(p.expected("INTERPOLATE after the last item"));
        }
        Some(Box::new(interpolate))
    } else {
        None
    };
    Ok(OrderByItem {
        using: None,
        expr,
        asc,
        nulls_first,
        ext: ClickHouseOrderByItem {
            with_fill,
            interpolate,
        },
    })
}

/// Parses a `GROUP BY` item, which can be an empty `()`.
pub fn parse_group_by_item<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Expr<'a, Builtin>> {
    if p.at_op("(") && p.peek_nth(1).is_op(")") {
        p.advance();
        p.advance();
        return Ok(ExprKind::Special(Box::new(SpecialExpr::Tuple(Vec::new()))).into());
    }
    grammar::parse_grouping_item(p)
}

fn at_group_by_modifier<'a, D: Dialect>(p: &Parser<'a, D>) -> bool {
    let next = p.peek_nth(1);
    p.at_keyword("WITH")
        && (next.is_word("ROLLUP") || next.is_word("CUBE") || next.is_word("TOTALS"))
}

/// Parses `WITH ROLLUP`, `WITH CUBE` and `WITH TOTALS`, any number of them.
pub fn parse_group_by_modifier<'a, D: Dialect>(p: &mut Parser<'a, D>) -> Option<GroupByModifier> {
    let mut modifiers = Vec::new();
    while at_group_by_modifier(p) {
        p.advance();
        let word = p.advance();
        modifiers.push(if word.is_word("ROLLUP") {
            GroupByModifier::Rollup
        } else if word.is_word("CUBE") {
            GroupByModifier::Cube
        } else {
            GroupByModifier::Totals
        });
    }
    match modifiers.len() {
        0 => None,
        1 => modifiers.pop(),
        _ => Some(GroupByModifier::Chain(modifiers)),
    }
}

/// Parses a select item, in which a wildcard can have `EXCEPT` and `REPLACE`.
pub fn parse_select_item<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<SelectItem<'a, Builtin>> {
    let item = grammar::parse_select_item(p)?;
    let qualifier = match &item.kind {
        SelectItemKind::Wildcard => None,
        SelectItemKind::QualifiedWildcard(name) => Some(name.clone()),
        _ => return Ok(item),
    };
    let except_list = p.at_keyword("EXCEPT") && p.peek_nth(1).is_op("(");
    let except_name = p.at_keyword("EXCEPT")
        && !except_list
        && is_name(p.peek_nth(1))
        && !p.peek_nth(1).is_word("SELECT");
    if !except_list && !except_name && !p.at_keyword("REPLACE") {
        return Ok(item);
    }
    let mut options = WildcardOptions {
        ilike: None,
        exclude: Vec::new(),
        exclude_parens: true,
        except_keyword: true,
        replace: Vec::new(),
        rename: Vec::new(),
        rename_parens: false,
    };
    if except_list {
        p.advance();
        options.exclude = grammar::parse_ident_list(p)?;
    } else if except_name {
        p.advance();
        options.exclude = vec![p.parse_ident()?];
    }
    if p.at_keyword("REPLACE") {
        options.replace = grammar::parse_wildcard_replace(p)?;
    }
    Ok(SelectItemKind::WildcardWithOptions {
        qualifier,
        options: Box::new(options),
    }
    .into())
}

/// True if the `(` that is `n` tokens ahead starts an argument list holding
/// `SETTINGS`.
fn call_has_settings<'a, D: Dialect>(p: &Parser<'a, D>, mut n: usize) -> bool {
    let mut depth = 0usize;
    loop {
        let token = p.peek_nth(n);
        match token.kind {
            TokenKind::Eof => return false,
            TokenKind::Op if token.text == "(" => depth += 1,
            TokenKind::Op if token.text == ")" => {
                depth -= 1;
                if depth == 0 {
                    return false;
                }
            }
            TokenKind::Word if depth == 1 && token.is_word("SETTINGS") => return true,
            _ => {}
        }
        n += 1;
    }
}

/// True at a possibly qualified name whose arguments hold `SETTINGS`.
fn at_call_with_settings<'a, D: Dialect>(p: &Parser<'a, D>) -> bool {
    if !is_name(p.peek()) {
        return false;
    }
    let mut n = 1;
    while p.peek_nth(n).is_op(".") {
        n += 2;
    }
    p.peek_nth(n).is_op("(") && call_has_settings(p, n)
}

fn parse_settings_call<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
    name: ObjectName<'a>,
) -> PResult<Expr<'a, Builtin>> {
    p.expect_op("(")?;
    let mut args = Vec::new();
    let mut settings = Vec::new();
    loop {
        if p.eat_keyword("SETTINGS") {
            settings = parse_settings_list(p)?;
            break;
        }
        args.push(p.parse_expr()?);
        if !p.eat_op(",") {
            break;
        }
    }
    p.expect_op(")")?;
    let span = p.span_since(name.parts[0].span.start);
    let call = ext(ClickHouseExpr::SettingsCall(Box::new(
        ClickHouseSettingsCall {
            name,
            args,
            settings,
        },
    )));
    Ok(Expr::new(call.kind, span))
}

/// Parses a table, whose name can be any word, and a call whose arguments can
/// end in `SETTINGS`.
pub fn parse_table_factor<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<TableFactor<'a, Builtin>> {
    let token = p.peek();
    let reserved = token.kind == TokenKind::Word && D::is_reserved(token.text);
    if !reserved && !at_call_with_settings(p) {
        return grammar::parse_table_factor(p);
    }
    let mut parts = vec![p.parse_name_part()?];
    while p.eat_op(".") {
        parts.push(p.parse_name_part()?);
    }
    let name = ObjectName { parts };
    if !p.at_op("(") {
        return Ok(TableFactor::Named(name));
    }
    let call = if call_has_settings(p, 0) {
        parse_settings_call(p, name)?
    } else {
        p.parse_function_call(name)?
    };
    Ok(TableFactor::Function {
        call: Box::new(call),
        with_ordinality: false,
    })
}

fn parse_sample<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
    table: TableRef<'a, Builtin>,
) -> PResult<TableRef<'a, Builtin>> {
    p.expect_keyword("SAMPLE")?;
    let mut sample: SampleClause<'a, Builtin> = D::parse_sample_clause(p, false)?;
    if p.eat_keyword("OFFSET") {
        sample.offset = Some(Box::new(p.parse_expr()?));
    }
    Ok(TableRef {
        factor: TableFactor::Sampled(Box::new(TableSample { table, sample })),
        alias: None,
        span: Span::default(),
    })
}

/// Parses a table with any alias and `SAMPLE` clause.
pub fn parse_table_ref<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<TableRef<'a, Builtin>> {
    let table = grammar::parse_table_ref(p)?;
    if p.at_keyword("SAMPLE") {
        return parse_sample(p, table);
    }
    Ok(table)
}

/// Consumes the words of an array join, if one starts here.
fn parse_array_join_kind<'a, D: Dialect>(p: &mut Parser<'a, D>) -> Option<JoinKind<'a>> {
    let (kind, words) = if p.at_keyword("ARRAY") && p.peek_nth(1).is_word("JOIN") {
        ("ARRAY JOIN", 2)
    } else if p.at_keyword("LEFT")
        && p.peek_nth(1).is_word("ARRAY")
        && p.peek_nth(2).is_word("JOIN")
    {
        ("LEFT ARRAY JOIN", 3)
    } else if p.at_keyword("INNER")
        && p.peek_nth(1).is_word("ARRAY")
        && p.peek_nth(2).is_word("JOIN")
    {
        ("INNER ARRAY JOIN", 3)
    } else {
        return None;
    };
    for _ in 0..words {
        p.advance();
    }
    Some(JoinKind::Custom(kind.into()))
}

/// The keywords of a join after `GLOBAL`, which renders before them.
fn global_join_kind(kind: JoinKind, spelled_out: bool) -> Option<&'static str> {
    Some(match (kind, spelled_out) {
        (JoinKind::Inner, false) => "GLOBAL JOIN",
        (JoinKind::Inner, true) => "GLOBAL INNER JOIN",
        (JoinKind::Left, false) => "GLOBAL LEFT JOIN",
        (JoinKind::Left, true) => "GLOBAL LEFT OUTER JOIN",
        (JoinKind::Right, false) => "GLOBAL RIGHT JOIN",
        (JoinKind::Right, true) => "GLOBAL RIGHT OUTER JOIN",
        (JoinKind::Full, _) => "GLOBAL FULL JOIN",
        (JoinKind::Cross, _) => "GLOBAL CROSS JOIN",
        (JoinKind::Semi, _) => "GLOBAL SEMI JOIN",
        (JoinKind::LeftSemi, _) => "GLOBAL LEFT SEMI JOIN",
        (JoinKind::Anti, _) => "GLOBAL ANTI JOIN",
        (JoinKind::LeftAnti, _) => "GLOBAL LEFT ANTI JOIN",
        _ => return None,
    })
}

/// Parses a join, which can be an `ARRAY JOIN` or start with `GLOBAL`.
pub fn parse_join<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Option<Join<'a, Builtin>>> {
    if p.at_keyword("GLOBAL") {
        p.advance();
        let Some(mut join) = spark::parse_join(p)? else {
            return Err(p.expected("JOIN after GLOBAL"));
        };
        let Some(kind) = global_join_kind(join.kind, join.spelled_out) else {
            return Err(p.expected("a join type that GLOBAL can modify"));
        };
        join.kind = JoinKind::Custom(kind.into());
        join.spelled_out = false;
        return Ok(Some(join));
    }
    if p.at_keyword("RIGHT") && p.peek_nth(2).is_word("JOIN") {
        let kind = if p.peek_nth(1).is_word("SEMI") {
            Some(JoinKind::RightSemi)
        } else if p.peek_nth(1).is_word("ANTI") {
            Some(JoinKind::RightAnti)
        } else {
            None
        };
        if let Some(kind) = kind {
            for _ in 0..3 {
                p.advance();
            }
            let table = p.parse_table_ref()?;
            let constraint = grammar::parse_join_constraint(p)?;
            return Ok(Some(Join {
                kind,
                spelled_out: false,
                table,
                constraint,
            }));
        }
    }
    let Some(kind) = parse_array_join_kind(p) else {
        return spark::parse_join(p);
    };
    let table = p.parse_table_ref()?;
    Ok(Some(Join {
        kind,
        spelled_out: false,
        table,
        constraint: JoinConstraint::None,
    }))
}

/// Parses a call, which can have a second argument list: `quantile(0.5)(x)`.
pub fn parse_function_call<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
    name: ObjectName<'a>,
) -> PResult<Expr<'a, Builtin>> {
    let first = grammar::parse_function_call(p, name)?;
    if !p.at_op("(") {
        return Ok(first);
    }
    let ExprKind::Function {
        name,
        args: parameters,
        options: None,
    } = first.kind
    else {
        return Ok(first);
    };
    let second = grammar::parse_function_call(p, name)?;
    let ExprKind::Function {
        name,
        args,
        options,
    } = second.kind
    else {
        return Err(p.expected("a function call"));
    };
    let mut options = options.unwrap_or_else(|| {
        Box::new(FunctionOptions {
            inner_null_treatment: None,
            parameters: Vec::new(),
            quantifier: SetQuantifier::None,
            order_by: Vec::new(),
            within_group: Vec::new(),
            filter: None,
            null_treatment: None,
            separator: None,
            on_overflow: None,
            over: None,
        })
    });
    options.parameters = parameters;
    Ok(ExprKind::Function {
        name,
        args,
        options: Some(options),
    }
    .into())
}

/// Parses `ON CLUSTER name` if present; the name can be a string.
fn parse_on_cluster<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Option<Ident<'a>>> {
    if !(p.at_keyword("ON") && p.peek_nth(1).is_word("CLUSTER")) {
        return Ok(None);
    }
    p.advance();
    p.advance();
    let token = p.peek();
    if !matches!(
        token.kind,
        TokenKind::Word | TokenKind::QuotedIdent | TokenKind::String
    ) {
        return Err(p.expected("a cluster name"));
    }
    parse_name_or_string(p).map(Some)
}

fn parse_sorting_key<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<ClickHouseSortingKey<'a>> {
    if !p.eat_op("(") {
        return p.parse_expr().map(ClickHouseSortingKey::One);
    }
    if p.eat_op(")") {
        return Ok(ClickHouseSortingKey::Many(Vec::new()));
    }
    let keys = p.parse_comma_separated(Parser::parse_expr)?;
    p.expect_op(")")?;
    Ok(ClickHouseSortingKey::Many(keys))
}

/// Types the original crate spells in a fixed case, whatever case they are
/// written in. Anything else keeps its source text.
const CANONICAL_TYPES: [&str; 53] = [
    "Array",
    "Nullable",
    "LowCardinality",
    "Map",
    "Tuple",
    "Nested",
    "Int16",
    "Int32",
    "Int128",
    "Int256",
    "UInt8",
    "UInt16",
    "UInt32",
    "UInt64",
    "UInt128",
    "UInt256",
    "Float32",
    "Date32",
    "DateTime64",
    "FixedString",
    "BOOL",
    "BOOLEAN",
    "INT",
    "INTEGER",
    "TINYINT",
    "SMALLINT",
    "MEDIUMINT",
    "BIGINT",
    "HUGEINT",
    "INT2",
    "INT4",
    "INT8",
    "INT64",
    "FLOAT",
    "REAL",
    "FLOAT4",
    "FLOAT8",
    "FLOAT64",
    "DOUBLE",
    "UUID",
    "DATE",
    "DATETIME",
    "TIMESTAMP",
    "TIME",
    "JSON",
    "TEXT",
    "STRING",
    "VARCHAR",
    "CHAR",
    "NUMERIC",
    "DECIMAL",
    "BINARY",
    "BLOB",
];

fn canonical_type_name(text: &str) -> Option<&'static str> {
    CANONICAL_TYPES
        .iter()
        .copied()
        .find(|name| name.eq_ignore_ascii_case(text))
}

/// Parses the fields in `Tuple(a Int8)` or `Nested(a Int8)`.
fn parse_type_fields<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Vec<StructField<'a>>> {
    p.parse_comma_separated(|p| {
        let name = p.parse_ident()?;
        let data_type = p.nested(D::parse_data_type)?;
        Ok(StructField {
            options: Vec::new(),
            name: Some(name),
            colon: false,
            data_type,
            not_null: false,
            comment: None,
        })
    })
}

/// True if `(` at the cursor holds anything but numbers separated by commas,
/// which the core reads itself.
fn at_type_arguments<'a, D: Dialect>(p: &Parser<'a, D>) -> bool {
    if p.peek_nth(1).is_op(")") {
        return true;
    }
    let mut depth = 0usize;
    let mut n = 0;
    loop {
        let token = p.peek_nth(n);
        match token.kind {
            TokenKind::Eof => return false,
            TokenKind::Op if token.text == "(" => depth += 1,
            TokenKind::Op if token.text == ")" => {
                depth -= 1;
                if depth == 0 {
                    return false;
                }
            }
            TokenKind::Number => {}
            TokenKind::Op if depth == 1 && token.text == "," => {}
            _ => return true,
        }
        n += 1;
    }
}

/// Parses `(...)` holding type modifiers or struct fields.
fn parse_type_arguments<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
    name: Ident<'a>,
) -> PResult<DataType<'a>> {
    p.expect_op("(")?;
    let mut data_type = bare_type(name);
    if p.eat_op(")") {
        data_type.angle = Some(Box::new(AngleArguments::ParenTypes(Vec::new())));
        return Ok(data_type);
    }
    let token = p.peek();
    let next = p.peek_nth(1);
    if is_name(token) && !next.is_op(",") && !next.is_op(")") {
        let fields = p.parse_comma_separated(|p| {
            let field_name = p.parse_ident()?;
            let field_type = p.nested(D::parse_data_type)?;
            let not_null = p.eat_keyword("NOT");
            if not_null {
                p.expect_keyword("NULL")?;
            }
            Ok(StructField {
                options: Vec::new(),
                name: Some(field_name),
                colon: false,
                data_type: field_type,
                not_null,
                comment: None,
            })
        })?;
        p.expect_op(")")?;
        data_type.angle = Some(Box::new(AngleArguments::ParenFields(fields)));
        return Ok(data_type);
    }
    data_type.args = p
        .parse_comma_separated(|p| {
            let token = p.peek();
            if !matches!(
                token.kind,
                TokenKind::Number | TokenKind::String | TokenKind::Word | TokenKind::QuotedIdent
            ) {
                return Err(p.expected("a type modifier"));
            }
            p.advance();
            Ok(token.text)
        })?
        .into_iter()
        .map(Cow::Borrowed)
        .collect();
    p.expect_op(")")?;
    Ok(data_type)
}

fn bare_type(name: Ident<'_>) -> DataType<'_> {
    DataType {
        qualifier: Vec::new(),
        name,
        angle: None,
        args: Vec::new(),
        suffix: DataTypeSuffix::None,
        array: crate::ast::ArrayType::None,
    }
}

fn parse_container_type<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
    name: Ident<'a>,
    canonical: &str,
) -> PResult<DataType<'a>> {
    p.expect_op("(")?;
    let mut data_type = bare_type(name);
    let arguments = match canonical {
        "Nested" => AngleArguments::ParenFields(parse_type_fields(p)?),
        "Tuple" => {
            let next = p.peek_nth(1);
            if is_name(p.peek()) && !next.is_op(",") && !next.is_op(")") && !next.is_op("(") {
                AngleArguments::ParenFields(parse_type_fields(p)?)
            } else {
                AngleArguments::ParenTypes(
                    p.parse_comma_separated(|p| p.nested(D::parse_data_type))?,
                )
            }
        }
        _ => AngleArguments::ParenTypes(p.parse_comma_separated(|p| p.nested(D::parse_data_type))?),
    };
    p.expect_op(")")?;
    data_type.angle = Some(Box::new(arguments));
    Ok(data_type)
}

/// Parses a data type, including the parenthesized containers and the known
/// types the original crate renders in a fixed case.
pub fn parse_data_type<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<DataType<'a>> {
    let written = p.parse_ident()?;
    let canonical = written
        .quote
        .is_none()
        .then(|| canonical_type_name(&written.text))
        .flatten();
    let name = match canonical {
        Some(text) => Ident {
            text: text.into(),
            ..written
        },
        None => written,
    };
    match canonical {
        Some(
            container @ ("Array" | "Nullable" | "LowCardinality" | "Map" | "Tuple" | "Nested"),
        ) => parse_container_type(p, name, container),
        _ if p.at_op("(") && at_type_arguments(p) => parse_type_arguments(p, name),
        _ => grammar::parse_data_type_after_name(p, name),
    }
}

/// Parses `DEFAULT`, `MATERIALIZED`, `ALIAS` and `EPHEMERAL` here, so that
/// their expressions leave a following `NOT NULL` to the column.
pub fn parse_column_option<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Option<ColumnOption<'a, Builtin>>> {
    let parse_expr = |p: &mut Parser<'a, D>| p.with_context(IN_COLUMN_OPTION, Parser::parse_expr);
    if p.eat_keyword("DEFAULT") {
        return parse_expr(p).map(|expr| Some(ColumnOption::Default(expr)));
    }
    for keyword in ["MATERIALIZED", "ALIAS"] {
        if p.eat_keyword(keyword) {
            let expr = parse_expr(p)?;
            return Ok(Some(ColumnOption::Keyword {
                keyword: keyword.into(),
                expr: Some(expr),
            }));
        }
    }
    if p.eat_keyword("EPHEMERAL") {
        let expr = if p.at_op(",") || p.at_op(")") {
            None
        } else {
            Some(parse_expr(p)?)
        };
        return Ok(Some(ColumnOption::Keyword {
            keyword: "EPHEMERAL".into(),
            expr,
        }));
    }
    spark::parse_column_option(p)
}

fn parse_engine<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<ClickHouseEngine<'a>> {
    p.expect_keyword("ENGINE")?;
    p.eat_op("=");
    let name = p.parse_ident()?;
    let args = if p.eat_op("(") {
        if p.eat_op(")") {
            Vec::new()
        } else {
            let args = p.parse_comma_separated(parse_name_or_string)?;
            p.expect_op(")")?;
            args
        }
    } else {
        Vec::new()
    };
    Ok(ClickHouseEngine { name, args })
}

fn parse_on_commit<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Option<&'static str>> {
    if !(p.at_keyword("ON") && p.peek_nth(1).is_word("COMMIT")) {
        return Ok(None);
    }
    p.advance();
    p.advance();
    if p.eat_keyword("DELETE") {
        p.expect_keyword("ROWS")?;
        return Ok(Some("DELETE ROWS"));
    }
    if p.eat_keyword("PRESERVE") {
        p.expect_keyword("ROWS")?;
        return Ok(Some("PRESERVE ROWS"));
    }
    p.expect_keyword("DROP")?;
    Ok(Some("DROP"))
}

/// Flags written before `TABLE` or `VIEW`.
struct CreateHead {
    or_replace: bool,
    scope: Option<&'static str>,
    temporary: bool,
}

fn parse_create_head<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<CreateHead> {
    p.expect_keyword("CREATE")?;
    let or_replace = grammar::parse_or_replace(p)?;
    let scope = if p.eat_keyword("LOCAL") {
        Some("LOCAL")
    } else if p.eat_keyword("GLOBAL") {
        Some("GLOBAL")
    } else {
        None
    };
    let temporary = p.eat_keyword("TEMPORARY") || p.eat_keyword("TEMP");
    Ok(CreateHead {
        or_replace,
        scope,
        temporary,
    })
}

fn parse_create_table<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
    head: CreateHead,
) -> PResult<Statement<'a, Builtin>> {
    p.expect_keyword("TABLE")?;
    let if_not_exists = grammar::parse_if_not_exists(p)?;
    let name = grammar::parse_object_name(p)?;
    let on_cluster = parse_on_cluster(p)?;
    let (columns, constraints) = if p.at_op("(") {
        grammar::parse_columns(p)?
    } else {
        (Vec::new(), Vec::new())
    };
    let engine = if p.at_keyword("ENGINE") {
        Some(parse_engine(p)?)
    } else {
        None
    };
    let mut partition_by = if p.at_keyword("PARTITION") && p.peek_nth(1).is_word("BY") {
        p.advance();
        p.advance();
        Some(p.parse_expr()?)
    } else {
        None
    };
    let primary_key = if p.at_keyword("PRIMARY") && p.peek_nth(1).is_word("KEY") {
        p.advance();
        p.advance();
        Some(p.parse_expr()?)
    } else {
        None
    };
    let order_by = if p.at_keyword("ORDER") && p.peek_nth(1).is_word("BY") {
        p.advance();
        p.advance();
        Some(parse_sorting_key(p)?)
    } else {
        None
    };
    if partition_by.is_none() && p.at_keyword("PARTITION") && p.peek_nth(1).is_word("BY") {
        p.advance();
        p.advance();
        partition_by = Some(p.parse_expr()?);
    }
    let on_commit = parse_on_commit(p)?;
    let query = if p.eat_keyword("AS") {
        Some(Box::new(D::parse_query(p)?))
    } else {
        None
    };
    if columns.is_empty() && query.is_none() && engine.is_none() {
        return Err(p.expected("column definitions, ENGINE or AS"));
    }
    let table = CreateTable {
        or_replace: head.or_replace,
        temporary: head.temporary,
        name,
        if_not_exists,
        columns,
        constraints,
        query,
    };
    let plain = head.scope.is_none()
        && on_cluster.is_none()
        && engine.is_none()
        && primary_key.is_none()
        && order_by.is_none()
        && partition_by.is_none()
        && on_commit.is_none();
    if plain {
        return Ok(StatementKind::CreateTable(Box::new(table)).into());
    }
    Ok(statement(ClickHouseStatement::CreateTable(Box::new(
        ClickHouseCreateTable {
            table,
            scope: head.scope.map(Cow::Borrowed),
            on_cluster,
            engine,
            primary_key,
            order_by,
            partition_by,
            on_commit: on_commit.map(Cow::Borrowed),
        },
    ))))
}

fn parse_create_view<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
    head: CreateHead,
) -> PResult<Statement<'a, Builtin>> {
    let materialized = p.eat_keyword("MATERIALIZED");
    p.expect_keyword("VIEW")?;
    let if_not_exists = grammar::parse_if_not_exists(p)?;
    let name = grammar::parse_object_name(p)?;
    let columns = if p.at_op("(") {
        p.advance();
        let columns = p.parse_comma_separated(D::parse_column_def)?;
        p.expect_op(")")?;
        columns
    } else {
        Vec::new()
    };
    let to = if p.eat_keyword("TO") {
        Some(grammar::parse_object_name(p)?)
    } else {
        None
    };
    p.expect_keyword("AS")?;
    let query = Box::new(D::parse_query(p)?);
    if columns.is_empty() && to.is_none() {
        return Ok(StatementKind::CreateView(Box::new(CreateView {
            or_replace: head.or_replace,
            temporary: head.temporary,
            materialized,
            if_not_exists,
            name,
            columns: Vec::new(),
            with: Vec::new(),
            query,
            no_schema_binding: false,
        }))
        .into());
    }
    Ok(statement(ClickHouseStatement::CreateView(Box::new(
        ClickHouseCreateView {
            or_replace: head.or_replace,
            materialized,
            if_not_exists,
            name,
            columns,
            to,
            query,
        },
    ))))
}

/// True at `CREATE ... {TABLE | VIEW | MATERIALIZED}`.
fn at_create_table_or_view<'a, D: Dialect>(p: &Parser<'a, D>) -> bool {
    let mut n = 1;
    if p.peek_nth(n).is_word("OR") {
        n += 2;
    }
    if p.peek_nth(n).is_word("LOCAL") || p.peek_nth(n).is_word("GLOBAL") {
        n += 1;
    }
    if p.peek_nth(n).is_word("TEMPORARY") || p.peek_nth(n).is_word("TEMP") {
        n += 1;
    }
    let object = p.peek_nth(n);
    object.is_word("TABLE") || object.is_word("VIEW") || object.is_word("MATERIALIZED")
}

pub fn parse_create<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Statement<'a, Builtin>> {
    if !at_create_table_or_view(p) {
        return grammar::parse_create(p);
    }
    let head = parse_create_head(p)?;
    if p.at_keyword("TABLE") {
        parse_create_table(p, head)
    } else {
        parse_create_view(p, head)
    }
}

fn parse_partition<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<ClickHousePartition<'a>> {
    let part = p.eat_keyword("PART");
    if !part {
        p.expect_keyword("PARTITION")?;
    }
    Ok(ClickHousePartition {
        part,
        expr: p.parse_expr()?,
    })
}

fn parse_projection_partition<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Option<Ident<'a>>> {
    if !p.eat_keyword("IN") {
        return Ok(None);
    }
    p.expect_keyword("PARTITION")?;
    p.parse_ident().map(Some)
}

fn parse_add_projection<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<ClickHouseProjection<'a>> {
    let if_not_exists = grammar::parse_if_not_exists(p)?;
    let name = p.parse_ident()?;
    p.expect_op("(")?;
    p.expect_keyword("SELECT")?;
    let projection = D::parse_projection(p)?;
    let group_by = if p.at_keyword("GROUP") {
        p.advance();
        p.expect_keyword("BY")?;
        p.parse_comma_separated(Parser::parse_expr)?
    } else {
        Vec::new()
    };
    let order_by = if p.at_keyword("ORDER") {
        p.advance();
        p.expect_keyword("BY")?;
        p.parse_comma_separated(D::parse_order_by_item)?
    } else {
        Vec::new()
    };
    p.expect_op(")")?;
    Ok(ClickHouseProjection {
        if_not_exists,
        name,
        projection,
        group_by,
        order_by,
    })
}

fn parse_with_name<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Option<Ident<'a>>> {
    if !p.eat_keyword("WITH") {
        return Ok(None);
    }
    p.expect_keyword("NAME")?;
    let token = p.peek();
    if !matches!(
        token.kind,
        TokenKind::Word | TokenKind::QuotedIdent | TokenKind::String
    ) {
        return Err(p.expected("a name"));
    }
    parse_name_or_string(p).map(Some)
}

/// Parses an operation of `ALTER TABLE`, trying ClickHouse's before the core's.
pub fn parse_alter_op<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<ClickHouseAlterOp<'a>> {
    let next = p.peek_nth(1);
    if p.at_keyword("ADD") && next.is_word("PROJECTION") {
        p.advance();
        p.advance();
        return parse_add_projection(p).map(|add| ClickHouseAlterOp::AddProjection(Box::new(add)));
    }
    if p.at_keyword("DROP") && next.is_word("PROJECTION") {
        p.advance();
        p.advance();
        let if_exists = grammar::parse_if_exists(p);
        let name = p.parse_ident()?;
        return Ok(ClickHouseAlterOp::DropProjection { if_exists, name });
    }
    if p.at_keyword("CLEAR") && next.is_word("PROJECTION") {
        p.advance();
        p.advance();
        let if_exists = grammar::parse_if_exists(p);
        let name = p.parse_ident()?;
        let partition = parse_projection_partition(p)?;
        return Ok(ClickHouseAlterOp::ClearProjection {
            if_exists,
            name,
            partition,
        });
    }
    if p.at_keyword("MATERIALIZE") && next.is_word("PROJECTION") {
        p.advance();
        p.advance();
        let if_exists = grammar::parse_if_exists(p);
        let name = p.parse_ident()?;
        let partition = parse_projection_partition(p)?;
        return Ok(ClickHouseAlterOp::MaterializeProjection {
            if_exists,
            name,
            partition,
        });
    }
    if p.eat_keyword("ATTACH") {
        return parse_partition(p).map(ClickHouseAlterOp::Attach);
    }
    if p.eat_keyword("DETACH") {
        return parse_partition(p).map(ClickHouseAlterOp::Detach);
    }
    if p.eat_keyword("FREEZE") {
        let partition = parse_partition(p)?;
        let with_name = parse_with_name(p)?;
        return Ok(ClickHouseAlterOp::Freeze {
            partition,
            with_name,
        });
    }
    if p.eat_keyword("UNFREEZE") {
        let partition = parse_partition(p)?;
        let with_name = parse_with_name(p)?;
        return Ok(ClickHouseAlterOp::Unfreeze {
            partition,
            with_name,
        });
    }
    if p.at_keyword("MODIFY") && next.is_word("ORDER") {
        p.advance();
        p.advance();
        p.expect_keyword("BY")?;
        return parse_sorting_key(p).map(ClickHouseAlterOp::ModifyOrderBy);
    }
    if p.eat_keyword("MODIFY") {
        p.eat_keyword("COLUMN");
        let column = D::parse_column_def(p)?;
        let position = mysql::parse_column_position(p)?;
        return Ok(ClickHouseAlterOp::Core(AlterTableOp::ModifyColumn {
            column,
            position,
        }));
    }
    let mut operation = D::parse_alter_table_op(p)?;
    if let AlterTableOp::AddColumn { position, .. } = &mut operation {
        *position = mysql::parse_column_position(p)?;
    }
    Ok(ClickHouseAlterOp::Core(operation))
}

fn parse_alter_table<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Statement<'a, Builtin>> {
    p.expect_keyword("ALTER")?;
    p.expect_keyword("TABLE")?;
    let if_exists = grammar::parse_if_exists(p);
    let name = grammar::parse_object_name(p)?;
    let on_cluster = parse_on_cluster(p)?;
    let operations = p.parse_comma_separated(parse_alter_op)?;
    let all_core = operations
        .iter()
        .all(|operation| matches!(operation, ClickHouseAlterOp::Core(_)));
    if on_cluster.is_none() && all_core {
        let operations = operations
            .into_iter()
            .filter_map(|operation| match operation {
                ClickHouseAlterOp::Core(operation) => Some(operation),
                _ => None,
            })
            .collect();
        return Ok(StatementKind::AlterTable(Box::new(AlterTable {
            if_exists,
            only: false,
            name,
            operations,
        }))
        .into());
    }
    Ok(statement(ClickHouseStatement::AlterTable(Box::new(
        ClickHouseAlterTable {
            if_exists,
            name,
            on_cluster,
            operations,
        },
    ))))
}

fn parse_kill<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<ClickHouseStatement<'a>> {
    p.expect_keyword("KILL")?;
    let kind = ["CONNECTION", "QUERY", "MUTATION"]
        .into_iter()
        .find(|kind| p.eat_keyword(kind));
    let id = grammar::parse_number(p)?;
    Ok(ClickHouseStatement::Kill {
        kind: kind.map(Cow::Borrowed),
        id: id.into(),
    })
}

fn parse_optimize<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<ClickHouseStatement<'a>> {
    p.expect_keyword("OPTIMIZE")?;
    p.expect_keyword("TABLE")?;
    let name = grammar::parse_object_name(p)?;
    let on_cluster = parse_on_cluster(p)?;
    let partition = if p.eat_keyword("PARTITION") {
        if p.eat_keyword("ID") {
            let token = p.peek();
            if !at_dictionary_key_token(token) {
                return Err(p.expected("a partition id"));
            }
            Some(ClickHouseOptimizePartition::Id(parse_name_or_string(p)?))
        } else {
            Some(ClickHouseOptimizePartition::Expr(p.parse_expr()?))
        }
    } else {
        None
    };
    let include_final = p.eat_keyword("FINAL");
    let deduplicate = if p.eat_keyword("DEDUPLICATE") {
        if p.eat_keyword("BY") {
            Some(Deduplicate::By(p.parse_expr()?))
        } else {
            Some(Deduplicate::All)
        }
    } else {
        None
    };
    Ok(ClickHouseStatement::Optimize(Box::new(
        ClickHouseOptimize {
            name,
            on_cluster,
            partition,
            include_final,
            deduplicate,
        },
    )))
}

fn parse_delete<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Statement<'a, Builtin>> {
    p.expect_keyword("DELETE")?;
    p.expect_keyword("FROM")?;
    let table = p.parse_table_ref()?;
    let on_cluster = parse_on_cluster(p)?;
    let selection = if p.eat_keyword("WHERE") {
        Some(p.parse_expr()?)
    } else {
        None
    };
    let settings = parse_optional_settings(p)?;
    if on_cluster.is_none() && settings.is_empty() {
        return Ok(StatementKind::Delete(Box::new(Delete {
            output: None,
            multi: None,
            using: Vec::new(),
            table,
            selection,
            order_by: Vec::new(),
            limit: None,
            returning: Vec::new(),
        }))
        .into());
    }
    Ok(statement(ClickHouseStatement::Delete(Box::new(
        ClickHouseDelete {
            table,
            on_cluster,
            selection,
            settings,
        },
    ))))
}

fn parse_update<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Statement<'a, Builtin>> {
    p.expect_keyword("UPDATE")?;
    let table = p.parse_table_ref()?;
    let on_cluster = parse_on_cluster(p)?;
    p.expect_keyword("SET")?;
    let assignments = p.parse_comma_separated(D::parse_assignment)?;
    let selection = if p.eat_keyword("WHERE") {
        Some(p.parse_expr()?)
    } else {
        None
    };
    let settings = parse_optional_settings(p)?;
    if on_cluster.is_none() && settings.is_empty() {
        return Ok(StatementKind::Update(Box::new(Update {
            output: None,
            from: Vec::new(),
            from_before_set: false,
            table,
            joins: Vec::new(),
            assignments,
            selection,
            order_by: Vec::new(),
            limit: None,
            returning: Vec::new(),
        }))
        .into());
    }
    Ok(statement(ClickHouseStatement::Update(Box::new(
        ClickHouseUpdate {
            table,
            on_cluster,
            assignments,
            selection,
            settings,
        },
    ))))
}

fn parse_input_format<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<ClickHouseInputFormat<'a>> {
    let name = p.parse_ident()?;
    let token = p.peek();
    let has_values = !(token.kind == TokenKind::Eof || token.is_op(";"));
    let values = if has_values {
        p.parse_comma_separated(Parser::parse_expr)?
    } else {
        Vec::new()
    };
    Ok(ClickHouseInputFormat { name, values })
}

/// Parses `INSERT`, which can target a function and end in `SETTINGS` and
/// `FORMAT`.
pub fn parse_insert_statement<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Statement<'a, Builtin>> {
    p.expect_keyword("INSERT")?;
    p.expect_keyword("INTO")?;
    let table_keyword = p.eat_keyword("TABLE");
    let target = if p.eat_keyword("FUNCTION") {
        let name = grammar::parse_object_name(p)?;
        ClickHouseInsertTarget::Function(Box::new(p.parse_function_call(name)?))
    } else {
        ClickHouseInsertTarget::Table(grammar::parse_object_name(p)?)
    };
    let columns = if grammar::at_column_list(p) {
        grammar::parse_ident_list(p)?
    } else {
        Vec::new()
    };
    let source = if p.at_keyword("SETTINGS") || p.at_keyword("FORMAT") {
        None
    } else {
        Some(grammar::parse_insert_source(p)?)
    };
    let settings = parse_optional_settings(p)?;
    let format = if p.eat_keyword("FORMAT") {
        Some(parse_input_format(p)?)
    } else {
        None
    };
    if let (false, ClickHouseInsertTarget::Table(table), Some(source), true, None) = (
        table_keyword,
        &target,
        &source,
        settings.is_empty(),
        &format,
    ) {
        return Ok(StatementKind::Insert(Box::new(Insert {
            into: true,
            output: None,
            modifiers: Default::default(),
            table: table.clone(),
            columns,
            source: source.clone(),
            returning: Vec::new(),
        }))
        .into());
    }
    Ok(statement(ClickHouseStatement::Insert(Box::new(
        ClickHouseInsert {
            table_keyword,
            target,
            columns,
            source,
            settings,
            format,
        },
    ))))
}

/// Parses a table constraint. `CONSTRAINT` always starts one, unlike in the
/// core, where it is otherwise a column's name.
pub fn parse_table_constraint<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Option<TableConstraint<'a, Builtin>>> {
    let third = p.peek_nth(2);
    let known = ["PRIMARY", "UNIQUE", "CHECK", "FOREIGN"]
        .iter()
        .any(|word| third.is_word(word));
    if p.at_keyword("CONSTRAINT") && !known {
        return Err(p.expected("a table constraint after CONSTRAINT"));
    }
    grammar::parse_table_constraint(p)
}

pub fn parse_statement<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Statement<'a, Builtin>> {
    if p.at_keyword("KILL") {
        return parse_kill(p).map(statement);
    }
    if p.at_keyword("OPTIMIZE") {
        return parse_optimize(p).map(statement);
    }
    if p.at_keyword("ALTER") && p.peek_nth(1).is_word("TABLE") {
        return parse_alter_table(p);
    }
    if p.at_keyword("DELETE") {
        return parse_delete(p);
    }
    if p.at_keyword("UPDATE") {
        return parse_update(p);
    }
    if p.at_keyword("TRUNCATE") {
        let truncate = grammar::parse_truncate(p)?;
        let Some(on_cluster) = parse_on_cluster(p)? else {
            return Ok(StatementKind::Truncate(Box::new(truncate)).into());
        };
        return Ok(statement(ClickHouseStatement::TruncateOnCluster(Box::new(
            ClickHouseTruncate {
                truncate,
                on_cluster,
            },
        ))));
    }
    grammar::parse_statement(p)
}

/// Parses what `EXPLAIN` or `DESCRIBE` applies to, which can start with `TABLE`.
pub fn parse_explain_target<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<ExplainTarget<'a, Builtin>> {
    if p.eat_keyword("TABLE") {
        return Ok(ExplainTarget::Table {
            table_keyword: true,
            name: grammar::parse_object_name(p)?,
        });
    }
    grammar::parse_explain_target(p)
}

impl Dialect for ClickHouse {
    type Ext = Builtin;

    fn parse_create_extension<'a>(
        p: &mut Parser<'a, Self>,
        flags: CreateFlags,
    ) -> PResult<Option<Statement<'a, Builtin>>> {
        grammar::parse_create_role(p, flags, true)
    }

    fn parse_qualify<'a>(p: &mut Parser<'a, Self>) -> PResult<Option<Expr<'a, Builtin>>> {
        snowflake::parse_qualify(p)
    }

    #[inline]
    fn is_reserved(word: &str) -> bool {
        !word.eq_ignore_ascii_case("ALL") && grammar::is_reserved(word)
    }

    #[inline]
    fn lex_after(prev: Option<Token<'_>>, rest: &str) -> Option<LexResult> {
        lex_after(prev, rest)
    }

    #[inline]
    fn lex_token(rest: &str) -> Option<LexResult> {
        lex_token(rest)
    }

    #[inline]
    fn lex_comment(rest: &str) -> Option<Result<usize, &'static str>> {
        lex_comment(rest)
    }

    #[inline]
    fn is_reserved_for_alias(word: &str) -> bool {
        is_reserved_for_alias(word)
    }

    #[inline]
    fn string_literal(token: Token<'_>) -> StringLiteral<'_> {
        StringLiteral {
            backslash_escapes: true,
            ..grammar::string_literal(token)
        }
    }

    fn parse_statement<'a>(p: &mut Parser<'a, Self>) -> PResult<Statement<'a, Builtin>> {
        parse_statement(p)
    }

    fn parse_insert_statement<'a>(p: &mut Parser<'a, Self>) -> PResult<Statement<'a, Builtin>> {
        parse_insert_statement(p)
    }

    fn parse_create<'a>(p: &mut Parser<'a, Self>) -> PResult<Statement<'a, Builtin>> {
        parse_create(p)
    }

    fn parse_table_constraint<'a>(
        p: &mut Parser<'a, Self>,
    ) -> PResult<Option<TableConstraint<'a, Builtin>>> {
        parse_table_constraint(p)
    }

    fn parse_column_option<'a>(
        p: &mut Parser<'a, Self>,
    ) -> PResult<Option<ColumnOption<'a, Builtin>>> {
        parse_column_option(p)
    }

    fn parse_data_type<'a>(p: &mut Parser<'a, Self>) -> PResult<DataType<'a>> {
        parse_data_type(p)
    }

    fn at_from_first<'a>(p: &Parser<'a, Self>, n: usize) -> bool {
        p.peek_nth(n).is_word("FROM")
    }

    fn parse_select<'a>(p: &mut Parser<'a, Self>) -> PResult<Select<'a, Builtin>> {
        if p.at_keyword("FROM") {
            grammar::parse_from_first_select(p)
        } else {
            grammar::parse_select(p)
        }
    }

    fn parse_select_item<'a>(p: &mut Parser<'a, Self>) -> PResult<SelectItem<'a, Builtin>> {
        parse_select_item(p)
    }

    fn parse_prewhere<'a>(p: &mut Parser<'a, Self>) -> PResult<Option<Box<Expr<'a, Builtin>>>> {
        if p.eat_keyword("PREWHERE") {
            p.parse_expr().map(|expr| Some(Box::new(expr)))
        } else {
            Ok(None)
        }
    }

    fn parse_query_tail<'a>(p: &mut Parser<'a, Self>) -> PResult<BuiltinQueryTail<'a>> {
        parse_query_tail(p)
    }

    fn parse_limit<'a>(p: &mut Parser<'a, Self>) -> PResult<Limit<'a, Builtin>> {
        parse_limit(p)
    }

    fn parse_order_by_item<'a>(p: &mut Parser<'a, Self>) -> PResult<OrderByItem<'a, Builtin>> {
        parse_order_by_item(p)
    }

    fn eat_group_by_all<'a>(p: &mut Parser<'a, Self>) -> bool {
        p.eat_keyword("ALL")
    }

    fn parse_group_by_item<'a>(p: &mut Parser<'a, Self>) -> PResult<Expr<'a, Builtin>> {
        parse_group_by_item(p)
    }

    fn parse_group_by_modifier<'a>(p: &mut Parser<'a, Self>) -> Option<GroupByModifier> {
        parse_group_by_modifier(p)
    }

    fn parse_table_factor<'a>(p: &mut Parser<'a, Self>) -> PResult<TableFactor<'a, Builtin>> {
        parse_table_factor(p)
    }

    fn parse_table_ref<'a>(p: &mut Parser<'a, Self>) -> PResult<TableRef<'a, Builtin>> {
        parse_table_ref(p)
    }

    fn parse_join<'a>(p: &mut Parser<'a, Self>) -> PResult<Option<Join<'a, Builtin>>> {
        parse_join(p)
    }

    fn parse_function_call<'a>(
        p: &mut Parser<'a, Self>,
        name: ObjectName<'a>,
    ) -> PResult<Expr<'a, Builtin>> {
        parse_function_call(p, name)
    }

    fn parse_prefix<'a>(p: &mut Parser<'a, Self>) -> PResult<Expr<'a, Builtin>> {
        parse_prefix(p)
    }

    fn infix_rule<'a>(p: &Parser<'a, Self>) -> Option<InfixRule<'a, Self>> {
        infix_rule(p)
    }

    fn parse_explain_target<'a>(p: &mut Parser<'a, Self>) -> PResult<ExplainTarget<'a, Builtin>> {
        parse_explain_target(p)
    }
}

impl fmt::Display for ClickHouseSetting<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} = {}", self.name, self.value)
    }
}

impl fmt::Display for ClickHouseFormat<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Name(name) => name.fmt(f),
            Self::Null => f.write_str("NULL"),
        }
    }
}

impl fmt::Display for ClickHouseQueryClauses<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if !self.settings.is_empty() {
            write!(f, " SETTINGS {}", comma(&self.settings))?;
        }
        if let Some(format) = &self.format {
            write!(f, " FORMAT {format}")?;
        }
        Ok(())
    }
}

impl fmt::Display for ClickHouseWithFill<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(" WITH FILL")?;
        if let Some(from) = &self.from {
            write!(f, " FROM {from}")?;
        }
        if let Some(to) = &self.to {
            write!(f, " TO {to}")?;
        }
        if let Some(step) = &self.step {
            write!(f, " STEP {step}")?;
        }
        Ok(())
    }
}

impl fmt::Display for ClickHouseInterpolate<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(" INTERPOLATE")?;
        let Some(exprs) = &self.exprs else {
            return Ok(());
        };
        f.write_str(" (")?;
        for (i, (column, expr)) in exprs.iter().enumerate() {
            if i > 0 {
                f.write_str(", ")?;
            }
            column.fmt(f)?;
            if let Some(expr) = expr {
                write!(f, " AS {expr}")?;
            }
        }
        f.write_str(")")
    }
}

impl fmt::Display for ClickHouseOrderByItem<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(with_fill) = &self.with_fill {
            with_fill.fmt(f)?;
        }
        if let Some(interpolate) = &self.interpolate {
            interpolate.fmt(f)?;
        }
        Ok(())
    }
}

impl fmt::Display for ClickHouseSortingKey<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::One(expr) => expr.fmt(f),
            Self::Many(exprs) => write!(f, "({})", comma(exprs)),
        }
    }
}

impl fmt::Display for ClickHousePartition<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let keyword = if self.part { "PART" } else { "PARTITION" };
        write!(f, "{keyword} {}", self.expr)
    }
}

fn write_projection_partition(
    f: &mut fmt::Formatter<'_>,
    partition: &Option<Ident<'_>>,
) -> fmt::Result {
    match partition {
        Some(partition) => write!(f, " IN PARTITION {partition}"),
        None => Ok(()),
    }
}

fn write_with_name(f: &mut fmt::Formatter<'_>, with_name: &Option<Ident<'_>>) -> fmt::Result {
    match with_name {
        Some(name) => write!(f, " WITH NAME {name}"),
        None => Ok(()),
    }
}

impl fmt::Display for ClickHouseProjection<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ADD PROJECTION ")?;
        write_if_not_exists(f, self.if_not_exists)?;
        write!(f, "{} (SELECT {}", self.name, comma(&self.projection))?;
        if !self.group_by.is_empty() {
            write!(f, " GROUP BY {}", comma(&self.group_by))?;
        }
        if !self.order_by.is_empty() {
            write!(f, " ORDER BY {}", comma(&self.order_by))?;
        }
        f.write_str(")")
    }
}

impl fmt::Display for ClickHouseAlterOp<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Core(operation) => operation.fmt(f),
            Self::AddProjection(projection) => projection.fmt(f),
            Self::DropProjection { if_exists, name } => {
                f.write_str("DROP PROJECTION ")?;
                write_if_exists(f, *if_exists)?;
                name.fmt(f)
            }
            Self::ClearProjection {
                if_exists,
                name,
                partition,
            } => {
                f.write_str("CLEAR PROJECTION ")?;
                write_if_exists(f, *if_exists)?;
                name.fmt(f)?;
                write_projection_partition(f, partition)
            }
            Self::MaterializeProjection {
                if_exists,
                name,
                partition,
            } => {
                f.write_str("MATERIALIZE PROJECTION ")?;
                write_if_exists(f, *if_exists)?;
                name.fmt(f)?;
                write_projection_partition(f, partition)
            }
            Self::Attach(partition) => write!(f, "ATTACH {partition}"),
            Self::Detach(partition) => write!(f, "DETACH {partition}"),
            Self::Freeze {
                partition,
                with_name,
            } => {
                write!(f, "FREEZE {partition}")?;
                write_with_name(f, with_name)
            }
            Self::Unfreeze {
                partition,
                with_name,
            } => {
                write!(f, "UNFREEZE {partition}")?;
                write_with_name(f, with_name)
            }
            Self::ModifyOrderBy(key) => write!(f, "MODIFY ORDER BY {key}"),
        }
    }
}

fn write_on_cluster(f: &mut fmt::Formatter<'_>, on_cluster: &Option<Ident<'_>>) -> fmt::Result {
    match on_cluster {
        Some(cluster) => write!(f, " ON CLUSTER {cluster}"),
        None => Ok(()),
    }
}

impl fmt::Display for ClickHouseAlterTable<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ALTER TABLE ")?;
        write_if_exists(f, self.if_exists)?;
        self.name.fmt(f)?;
        write_on_cluster(f, &self.on_cluster)?;
        write!(f, " {}", comma(&self.operations))
    }
}

impl fmt::Display for ClickHouseEngine<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ENGINE = {}", self.name)?;
        if !self.args.is_empty() {
            write!(f, "({})", comma(&self.args))?;
        }
        Ok(())
    }
}

impl fmt::Display for ClickHouseCreateTable<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let table = &self.table;
        f.write_str("CREATE ")?;
        if table.or_replace {
            f.write_str("OR REPLACE ")?;
        }
        if let Some(ref scope) = self.scope {
            write!(f, "{scope} ")?;
        }
        if table.temporary {
            f.write_str("TEMPORARY ")?;
        }
        f.write_str("TABLE ")?;
        write_if_not_exists(f, table.if_not_exists)?;
        table.name.fmt(f)?;
        write_on_cluster(f, &self.on_cluster)?;
        if !table.columns.is_empty() || !table.constraints.is_empty() {
            write_columns(f, &table.columns, &table.constraints)?;
        }
        if let Some(engine) = &self.engine {
            write!(f, " {engine}")?;
        }
        if let Some(primary_key) = &self.primary_key {
            write!(f, " PRIMARY KEY {primary_key}")?;
        }
        if let Some(order_by) = &self.order_by {
            write!(f, " ORDER BY {order_by}")?;
        }
        if let Some(partition_by) = &self.partition_by {
            write!(f, " PARTITION BY {partition_by}")?;
        }
        if let Some(ref on_commit) = self.on_commit {
            write!(f, " ON COMMIT {on_commit}")?;
        }
        if let Some(query) = &table.query {
            write!(f, " AS {query}")?;
        }
        Ok(())
    }
}

impl fmt::Display for ClickHouseCreateView<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_create_flags(f, self.or_replace, false)?;
        f.write_str(if self.materialized {
            "MATERIALIZED VIEW "
        } else {
            "VIEW "
        })?;
        write_if_not_exists(f, self.if_not_exists)?;
        self.name.fmt(f)?;
        if !self.columns.is_empty() {
            write!(f, " ({})", comma(&self.columns))?;
        }
        if let Some(to) = &self.to {
            write!(f, " TO {to}")?;
        }
        crate::display::write_view_query(f, &self.query)
    }
}

impl fmt::Display for ClickHouseInsertTarget<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Table(name) => name.fmt(f),
            Self::Function(call) => write!(f, "FUNCTION {call}"),
        }
    }
}

impl fmt::Display for ClickHouseInsert<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("INSERT INTO ")?;
        if self.table_keyword {
            f.write_str("TABLE ")?;
        }
        self.target.fmt(f)?;
        if !self.columns.is_empty() {
            write!(f, " ({})", comma(&self.columns))?;
        }
        if let Some(source) = &self.source {
            crate::display::write_insert_source(f, source, !self.columns.is_empty())?;
        }
        let settings_on_new_line = !self.columns.is_empty();
        if !self.settings.is_empty() {
            if settings_on_new_line {
                SpaceOrNewline.fmt(f)?;
            } else {
                f.write_str(" ")?;
            }
            write!(f, "SETTINGS {}", comma(&self.settings))?;
        }
        if let Some(format) = &self.format {
            if settings_on_new_line || !self.settings.is_empty() {
                SpaceOrNewline.fmt(f)?;
            } else {
                f.write_str(" ")?;
            }
            write!(f, "FORMAT {}", format.name)?;
            if !format.values.is_empty() {
                write!(f, " {}", comma(&format.values))?;
            }
        }
        Ok(())
    }
}

impl fmt::Display for ClickHouseUpdate<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "UPDATE {}", self.table)?;
        write_on_cluster(f, &self.on_cluster)?;
        write_list_clause(f, "SET", &self.assignments)?;
        write_where_clause(f, &self.selection)?;
        write_list_clause(f, "SETTINGS", &self.settings)
    }
}

impl fmt::Display for ClickHouseDelete<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("DELETE FROM")?;
        SpaceOrNewline.fmt(f)?;
        Indent(&self.table).fmt(f)?;
        write_on_cluster(f, &self.on_cluster)?;
        write_where_clause(f, &self.selection)?;
        write_list_clause(f, "SETTINGS", &self.settings)
    }
}

impl fmt::Display for ClickHouseOptimize<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "OPTIMIZE TABLE {}", self.name)?;
        write_on_cluster(f, &self.on_cluster)?;
        match &self.partition {
            Some(ClickHouseOptimizePartition::Expr(expr)) => write!(f, " PARTITION {expr}")?,
            Some(ClickHouseOptimizePartition::Id(id)) => write!(f, " PARTITION ID {id}")?,
            None => {}
        }
        if self.include_final {
            f.write_str(" FINAL")?;
        }
        match &self.deduplicate {
            Some(Deduplicate::All) => f.write_str(" DEDUPLICATE"),
            Some(Deduplicate::By(expr)) => write!(f, " DEDUPLICATE BY {expr}"),
            None => Ok(()),
        }
    }
}

impl fmt::Display for ClickHouseStatement<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TruncateOnCluster(truncate) => {
                write!(
                    f,
                    "{} ON CLUSTER {}",
                    truncate.truncate, truncate.on_cluster
                )
            }
            Self::Kill { kind, id } => match kind {
                Some(kind) => write!(f, "KILL {kind} {id}"),
                None => write!(f, "KILL {id}"),
            },
            Self::Optimize(optimize) => optimize.fmt(f),
            Self::AlterTable(alter) => alter.fmt(f),
            Self::CreateTable(create) => create.fmt(f),
            Self::CreateView(create) => create.fmt(f),
            Self::Insert(insert) => insert.fmt(f),
            Self::Update(update) => update.fmt(f),
            Self::Delete(delete) => delete.fmt(f),
        }
    }
}

impl fmt::Display for ClickHouseGlobalIn<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let not = if self.negated { "NOT " } else { "" };
        write!(f, "{} GLOBAL {not}IN ", self.expr)?;
        match &self.target {
            ClickHouseInTarget::List(items) => write!(f, "({})", comma(items)),
            ClickHouseInTarget::Subquery(query) => write!(f, "({query})"),
        }
    }
}

impl fmt::Display for ClickHouseTypedLambda<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.parenthesized {
            f.write_str("(")?;
        }
        for (i, (name, data_type)) in self.params.iter().enumerate() {
            if i > 0 {
                f.write_str(", ")?;
            }
            name.fmt(f)?;
            if let Some(data_type) = data_type {
                write!(f, " {data_type}")?;
            }
        }
        if self.parenthesized {
            f.write_str(")")?;
        }
        write!(f, " -> {}", self.body)
    }
}

impl fmt::Display for ClickHouseSettingsCall<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}(", self.name)?;
        if !self.args.is_empty() {
            write!(f, "{}, ", comma(&self.args))?;
        }
        write!(f, "SETTINGS {})", comma(&self.settings))
    }
}

impl fmt::Display for ClickHouseExpr<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TupleElement { expr, index } => {
                expr.fmt(f)?;
                f.write_str(" . ")?;
                f.write_str(index)
            }
            Self::Array(items) => write!(f, "[{}]", comma(items)),
            Self::Dictionary(fields) => {
                f.write_str("{")?;
                for (i, (key, value)) in fields.iter().enumerate() {
                    if i > 0 {
                        f.write_str(", ")?;
                    }
                    write!(f, "{key}: {value}")?;
                }
                f.write_str("}")
            }
            Self::GlobalIn(global_in) => global_in.fmt(f),
            Self::TypedLambda(lambda) => lambda.fmt(f),
            Self::SettingsCall(call) => call.fmt(f),
            Self::All => f.write_str("ALL"),
        }
    }
}

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

//! Snowflake.

use std::borrow::Cow;
use std::fmt::{self, Display as _};

use crate::PResult;
use crate::ast::{
    AlterTable, AlterTableOp, ColumnOption, ConnectBy, CreateFlags, CreateSchema, DataType,
    DropBehavior, ExplainTarget, Expr, ExprKind, FunctionArg, Ident, Join, JoinConstraint,
    JoinKind, Literal, NullTreatment, ObjectName, ObjectType, OrderByItem, Pivot, PivotSource,
    Quantifier, Query, SampleClause, SelectInto, SelectItem, SelectItemKind, SetQuantifier,
    SetStatement, Span, SpecialExpr, Statement, StatementKind, StringLiteral, TableAlias,
    TableFactor, TableRef, TableSample, TableWithJoins, Unpivot,
};
use crate::dialect::builtin::{BuiltinExpr, BuiltinInsert, BuiltinSelect, BuiltinStatement};
use crate::dialect::mssql::MsSqlTop;
use crate::dialect::{Builtin, Dialect, clickhouse, duckdb, mysql, oracle, postgres, spark};
use crate::display::{SpaceOrNewline, comma, write_if_not_exists};
use crate::grammar::{self, prec};
use crate::lexer::{
    LexResult, Lexeme, Token, TokenKind, block_comment, lex_core_comment, line_comment,
    quoted_with_backslash,
};
use crate::parser::{InfixRule, Parser, SetOperatorRule};

pub mod ddl;

pub use ddl::{
    Assignment, Block, DatabaseClause, Declaration, DeclarationKind, ExceptionWhen, RaiseValue,
    SemanticView, SnowflakeCreateDatabase, SnowflakeCreateTable, SnowflakeCreateView, TableClause,
    ViewColumn,
};

#[derive(Debug, Clone, PartialEq)]
pub struct Snowflake;

/// Expressions only Snowflake has.
#[derive(Debug, Clone, PartialEq)]
pub enum SnowflakeExpr<'a> {
    /// A path into a semi-structured value: `a:b.c[0]`.
    Path {
        value: Box<Expr<'a, Builtin>>,
        path: Vec<PathElement<'a>>,
    },
    /// A call's only argument, written without parentheses: `f(SELECT 1)`.
    BareSubquery(Box<Query<'a, Builtin>>),
    /// The arguments of `SEMANTIC_VIEW(...)`.
    SemanticView(Box<SemanticView<'a>>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum PathElement<'a> {
    /// `:key`
    Colon(Ident<'a>),
    /// `.key`
    Dot(Ident<'a>),
    /// `[index]`
    Index(Expr<'a, Builtin>),
}

/// Statements only Snowflake has.
#[derive(Debug, Clone, PartialEq)]
pub enum SnowflakeStatement<'a> {
    CreateUser(Box<CreateUser<'a>>),
    AlterUser(Box<AlterUser<'a>>),
    /// `USE [kind] name`
    Use {
        kind: Option<Cow<'a, str>>,
        name: ObjectName<'a>,
    },
    UseSecondaryRoles(SecondaryRoles<'a>),
    /// `ALTER SESSION SET a=1 b=2`
    AlterSessionSet(Vec<(Ident<'a>, Literal<'a>)>),
    /// `ALTER SESSION UNSET a, b`
    AlterSessionUnset(Vec<Ident<'a>>),
    /// `INSERT ALL ...` or `INSERT FIRST ...`
    MultiTableInsert(Box<MultiTableInsert<'a>>),
    /// `LIST @stage` or `REMOVE @stage`, with any options.
    StageCommand {
        remove: bool,
        stage: Ident<'a>,
        options: Vec<(Ident<'a>, Expr<'a, Builtin>)>,
    },
    CopyInto(Box<CopyInto<'a>>),
    /// `CREATE STAGE` or `CREATE FILE FORMAT`.
    CreateObject(Box<CreateObject<'a>>),
    CreateTable(Box<SnowflakeCreateTable<'a>>),
    CreateDatabase(Box<SnowflakeCreateDatabase<'a>>),
    CreateView(Box<SnowflakeCreateView<'a>>),
    AlterIcebergTable(Box<AlterTable<'a, Builtin>>),
    /// `ALTER {DYNAMIC | EXTERNAL} TABLE name action`
    AlterTableKind {
        dynamic: bool,
        if_exists: bool,
        name: ObjectName<'a>,
        action: Cow<'a, str>,
        subpath: Option<StringLiteral<'a>>,
    },
    Block(Box<Block<'a>>),
    /// `RAISE [value]`
    Raise(Option<RaiseValue<'a>>),
    Declare(Vec<Declaration<'a>>),
    /// `SET (a, b) = (1, 2)`
    SetTuple {
        names: Vec<Ident<'a>>,
        values: Vec<Expr<'a, Builtin>>,
    },
    /// `PUT 'file' @stage`, with any options.
    Put {
        file: StringLiteral<'a>,
        stage: Ident<'a>,
        options: Vec<(Ident<'a>, Expr<'a, Builtin>)>,
    },
}

/// A `KEY=value` option.
#[derive(Debug, Clone, PartialEq)]
pub struct KeyValue<'a> {
    pub key: Ident<'a>,
    pub value: OptionValue<'a>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum OptionValue<'a> {
    /// An unquoted word such as `CSV` or `true`, as written.
    Word(Cow<'a, str>),
    Literal(Literal<'a>),
    /// `('a', 'b')`
    List(Vec<StringLiteral<'a>>),
    /// `(A=1 B=2)`
    Nested(Vec<KeyValue<'a>>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum CopyLocation<'a> {
    Name(ObjectName<'a>),
    Stage(Ident<'a>),
    Url(StringLiteral<'a>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum CopySource<'a> {
    Query(Box<Query<'a, Builtin>>),
    Location(CopyLocation<'a>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum CopyClause<'a> {
    Option(KeyValue<'a>),
    PartitionBy(Expr<'a, Builtin>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct CopyInto<'a> {
    pub into: CopyLocation<'a>,
    pub into_params: Vec<KeyValue<'a>>,
    pub columns: Vec<Ident<'a>>,
    pub from: CopySource<'a>,
    pub from_params: Vec<KeyValue<'a>>,
    pub alias: Option<Ident<'a>>,
    /// In the order written.
    pub clauses: Vec<CopyClause<'a>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateObject<'a> {
    pub or_replace: bool,
    /// `TEMPORARY` or `VOLATILE`.
    pub lifetime: Option<Cow<'a, str>>,
    /// `STAGE` or `FILE FORMAT`.
    pub kind: Cow<'a, str>,
    pub if_not_exists: bool,
    pub name: ObjectName<'a>,
    pub options: Vec<KeyValue<'a>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MultiTableInsert<'a> {
    pub overwrite: bool,
    /// True for `INSERT FIRST`, false for `INSERT ALL`.
    pub first: bool,
    /// The targets of an unconditional insert.
    pub into: Vec<InsertTarget<'a>>,
    pub when: Vec<InsertWhen<'a>>,
    pub otherwise: Vec<InsertTarget<'a>>,
    pub source: Query<'a, Builtin>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct InsertWhen<'a> {
    pub condition: Expr<'a, Builtin>,
    pub into: Vec<InsertTarget<'a>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct InsertTarget<'a> {
    pub table: ObjectName<'a>,
    pub columns: Vec<Ident<'a>>,
    pub values: Option<Vec<Expr<'a, Builtin>>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SecondaryRoles<'a> {
    All,
    None,
    List(Vec<Ident<'a>>),
}

#[inline]
fn is_word_char(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_' || b == b'$'
}

fn op(len: usize) -> Option<LexResult> {
    Some(Ok(Lexeme {
        kind: TokenKind::Op,
        len,
    }))
}

/// Lexes `@name/path` as one word. Quoted parts may contain anything.
#[inline]
fn stage(rest: &str) -> Option<LexResult> {
    let bytes = rest.as_bytes();
    let mut quoted = false;
    let mut len = 1;
    while let Some(&b) = bytes.get(len) {
        let ends = b.is_ascii_whitespace() || matches!(b, b'(' | b')' | b',' | b';');
        if ends && !quoted {
            break;
        }
        quoted ^= b == b'"';
        len += 1;
    }
    (len > 1).then_some(Ok(Lexeme {
        kind: TokenKind::Word,
        len,
    }))
}

/// Identifiers are ASCII and can contain `$`. `@` starts a stage name.
#[inline]
pub fn lex_token(rest: &str) -> Option<LexResult> {
    let bytes = rest.as_bytes();
    match bytes[0] {
        b'\'' => Some(quoted_with_backslash(rest, TokenKind::String)),
        b'$' => postgres::dollar_string(rest).or_else(|| {
            // A lone `$` is the end anchor of a `MATCH_RECOGNIZE` pattern.
            let continues = bytes.get(1).is_some_and(|&b| is_word_char(b) && b != b'$');
            if continues { None } else { op(1) }
        }),
        b'{' | b'}' => op(1),
        b':' => match bytes.get(1) {
            Some(b':' | b'=') => op(2),
            Some(b) if b.is_ascii_digit() => {
                let digits = bytes[1..].iter().take_while(|b| b.is_ascii_digit()).count();
                Some(Ok(Lexeme {
                    kind: TokenKind::Placeholder,
                    len: 1 + digits,
                }))
            }
            Some(b) if b.is_ascii_alphabetic() || *b == b'_' => None,
            _ => op(1),
        },
        b'@' => stage(rest),
        b'-' if bytes.get(1) == Some(&b'>') => op(2),
        b'x' | b'X' if bytes.get(1) == Some(&b'\'') => None,
        b if b.is_ascii_alphabetic() || b == b'_' => Some(Ok(Lexeme {
            kind: TokenKind::Word,
            len: bytes.iter().take_while(|&&b| is_word_char(b)).count(),
        })),
        b if !b.is_ascii() => Some(Err("unexpected character")),
        _ => None,
    }
}

/// A colon directly after a string is a key's separator, as in `{'a':1}`.
#[inline]
pub fn lex_after(prev: Option<Token<'_>>, rest: &str) -> Option<LexResult> {
    let bytes = rest.as_bytes();
    let separator = bytes[0] == b':'
        && !matches!(bytes.get(1), Some(b':' | b'='))
        && prev.is_some_and(|token| token.kind == TokenKind::String);
    if separator { op(1) } else { None }
}

/// `#` and `//` start line comments, and block comments do not nest.
#[inline]
pub fn lex_comment(rest: &str) -> Option<Result<usize, &'static str>> {
    match rest.as_bytes() {
        [b'#', ..] | [b'/', b'/', ..] => Some(Ok(line_comment(rest))),
        [b'/', b'*', ..] => Some(block_comment(rest, false)),
        _ => lex_core_comment(rest),
    }
}

#[inline]
pub fn is_reserved_for_alias(word: &str) -> bool {
    word.starts_with('@')
        || [
            "MINUS",
            "ASOF",
            "MATCH_CONDITION",
            "QUALIFY",
            "SAMPLE",
            "TABLESAMPLE",
            "START",
            "CONNECT",
            "ILIKE",
            "REGEXP",
            "RLIKE",
            "LIMIT",
            "OFFSET",
            "EXCEPT",
            "OUTER",
        ]
        .iter()
        .any(|reserved| reserved.eq_ignore_ascii_case(word))
        || grammar::is_reserved_for_alias(word)
}

/// True for what can follow `LIMIT` or `OFFSET` when it starts a clause.
fn is_limit_value(token: Token<'_>) -> bool {
    matches!(token.kind, TokenKind::Number | TokenKind::Placeholder)
        || token.is_word("NULL")
        || token.text == "''"
        || token.text == "$$$$"
}

/// True at a word that normally starts a clause but is a name here, because
/// what follows it cannot be that clause. `table` selects the rules for a
/// table alias rather than those for a select item.
fn clause_word_is_name<D: Dialect>(p: &Parser<'_, D>, table: bool) -> bool {
    let next = p.peek_nth(1);
    if p.at_keyword("LIMIT") || p.at_keyword("OFFSET") {
        return !is_limit_value(next);
    }
    if p.at_keyword("FETCH") {
        return !(next.is_word("FIRST") || next.is_word("NEXT") || is_limit_value(next));
    }
    if p.at_keyword("EXCEPT") {
        return next.kind == TokenKind::Eof || next.is_op(";") || (!table && next.is_op(","));
    }
    table && (p.at_keyword("OUTER") || p.at_keyword("WINDOW") || p.at_keyword("PARTITION"))
}

/// Parses a list that allows a trailing comma, where a clause word can still
/// be the next item.
fn parse_list_with_trailing_comma<'a, D, T>(
    p: &mut Parser<'a, D>,
    mut item: impl FnMut(&mut Parser<'a, D>) -> PResult<T>,
) -> PResult<Vec<T>>
where
    D: Dialect<Ext = Builtin>,
{
    let mut items = vec![item(p)?];
    while p.eat_op(",") {
        if grammar::at_list_end(p) && !clause_word_is_name(p, false) {
            break;
        }
        items.push(item(p)?);
    }
    Ok(items)
}

/// Adds the select aliases that the clause words can be.
pub fn parse_select_item_alias<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
    expr: Expr<'a, Builtin>,
) -> PResult<SelectItem<'a, Builtin>> {
    if clause_word_is_name(p, false) {
        let alias = Some(p.parse_name_part()?);
        return Ok(SelectItemKind::Expr { expr, alias }.into());
    }
    grammar::parse_select_item_alias(p, expr)
}

/// True at `IDENTIFIER(...)` taking one string or bind variable.
fn at_identifier_call<D: Dialect>(p: &Parser<'_, D>) -> bool {
    let argument = p.peek_nth(2).kind;
    p.at_keyword("IDENTIFIER")
        && p.peek_nth(1).is_op("(")
        && matches!(argument, TokenKind::String | TokenKind::Placeholder)
        && p.peek_nth(3).is_op(")")
}

/// Parses an `IDENTIFIER(...)` call as a name part holding its source text,
/// since the argument is resolved only when the statement runs.
fn parse_identifier_call<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Ident<'a>> {
    let start = p.advance().span.start;
    p.advance();
    p.advance();
    let end = p.advance().span.end;
    let span = Span { start, end };
    Ok(Ident {
        text: p.text(span).into(),
        quote: None,
        span,
    })
}

fn is_identifier_call(part: &Ident<'_>) -> bool {
    part.quote.is_none() && part.text.ends_with(')')
}

/// A part of an object's name can be an `IDENTIFIER` call, and `a..b` leaves
/// out the middle one.
pub fn parse_object_name_part<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
    before: &[Ident<'a>],
) -> PResult<Ident<'a>> {
    if before.len() == 1 && p.at_op(".") {
        return Ok(Ident {
            text: "".into(),
            quote: None,
            span: Span::default(),
        });
    }
    if at_identifier_call(p) && !before.iter().any(is_identifier_call) {
        return parse_identifier_call(p);
    }
    grammar::parse_object_name_part(p, before)
}

/// The key in a `:key` token, in which the lexer saw a named placeholder. It
/// is only a placeholder at the start of an expression.
fn colon_key(token: Token<'_>) -> Option<Ident<'_>> {
    let key = token.text.strip_prefix(':')?;
    let is_name =
        token.kind == TokenKind::Placeholder && !key.starts_with(|c: char| c.is_ascii_digit());
    is_name.then_some(Ident {
        text: key.into(),
        quote: None,
        span: Span {
            start: token.span.start + 1,
            end: token.span.end,
        },
    })
}

pub fn at_path<D: Dialect<Ext = Builtin>>(p: &Parser<'_, D>) -> bool {
    !p.has_context(grammar::CONTEXT_COLON_SEPARATES)
        && (p.at_op(":") || colon_key(p.peek()).is_some())
}

fn parse_colon_key<'a, D: Dialect<Ext = Builtin>>(p: &mut Parser<'a, D>) -> PResult<Ident<'a>> {
    if let Some(key) = colon_key(p.peek()) {
        p.advance();
        return Ok(key);
    }
    p.expect_op(":")?;
    p.parse_name_part()
}

pub fn parse_path<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
    left: Expr<'a, Builtin>,
    _bp: u8,
) -> PResult<Expr<'a, Builtin>> {
    let mut path = vec![PathElement::Colon(parse_colon_key(p)?)];
    loop {
        if at_path(p) {
            path.push(PathElement::Colon(parse_colon_key(p)?));
        } else if p.eat_op(".") {
            path.push(PathElement::Dot(p.parse_name_part()?));
        } else if p.eat_op("[") {
            path.push(PathElement::Index(p.parse_expr()?));
            p.expect_op("]")?;
        } else {
            break;
        }
    }
    Ok(ExprKind::Ext(BuiltinExpr::Snowflake(SnowflakeExpr::Path {
        value: Box::new(left),
        path,
    }))
    .into())
}

/// True at `(+)`, which marks the outer side of a join.
pub fn at_outer_join_mark<D: Dialect<Ext = Builtin>>(p: &Parser<'_, D>) -> bool {
    p.at_op("(") && p.peek_nth(1).is_op("+") && p.peek_nth(2).is_op(")")
}

pub fn parse_outer_join_mark<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
    left: Expr<'a, Builtin>,
    _bp: u8,
) -> PResult<Expr<'a, Builtin>> {
    if !matches!(left.kind, ExprKind::Ident(_) | ExprKind::Compound(_)) {
        return Err(p.expected("a column before (+)"));
    }
    p.advance();
    p.advance();
    p.advance();
    Ok(ExprKind::Special(Box::new(SpecialExpr::OuterJoinMark(left))).into())
}

pub fn infix_rule<'a, D: Dialect<Ext = Builtin>>(p: &Parser<'a, D>) -> Option<InfixRule<'a, D>> {
    if at_outer_join_mark(p) {
        Some(InfixRule::custom(prec::POSTFIX, parse_outer_join_mark))
    } else if at_path(p) {
        Some(InfixRule::custom(prec::POSTFIX, parse_path))
    } else if p.at_op("::") {
        Some(InfixRule::custom(
            prec::POSTFIX,
            postgres::parse_double_colon,
        ))
    } else if p.at_keyword("ILIKE") {
        Some(InfixRule::custom(prec::COMPARE, postgres::parse_ilike))
    } else if p.at_keyword("NOT") && postgres::next_is_ilike(p) {
        Some(InfixRule::custom(prec::COMPARE, postgres::parse_not_ilike))
    } else if mysql::at_regexp(p, 0) || (p.at_keyword("NOT") && mysql::at_regexp(p, 1)) {
        Some(InfixRule::custom(prec::COMPARE, mysql::parse_regexp))
    } else {
        grammar::infix_rule(p)
    }
}

/// A part of a `USE` name, which can be a string.
fn parse_use_part<'a, D: Dialect<Ext = Builtin>>(p: &mut Parser<'a, D>) -> PResult<Ident<'a>> {
    let token = p.peek();
    if token.kind == TokenKind::String {
        p.advance();
        return Ok(Ident {
            text: Cow::Borrowed(&token.text[1..token.text.len() - 1]),
            quote: Some('\''),
            span: token.span,
        });
    }
    p.parse_ident()
}

fn parse_use_name<'a, D: Dialect<Ext = Builtin>>(p: &mut Parser<'a, D>) -> PResult<ObjectName<'a>> {
    if at_identifier_call(p) {
        return Ok(ObjectName {
            parts: vec![parse_identifier_call(p)?],
        });
    }
    let mut parts = vec![parse_use_part(p)?];
    while p.eat_op(".") {
        parts.push(parse_use_part(p)?);
    }
    Ok(ObjectName { parts })
}

fn parse_use<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<SnowflakeStatement<'a>> {
    p.expect_keyword("USE")?;
    if p.eat_keyword("SECONDARY") {
        if !p.eat_keyword("ROLES") {
            p.expect_keyword("ROLE")?;
        }
        return Ok(SnowflakeStatement::UseSecondaryRoles(
            if p.eat_keyword("ALL") {
                SecondaryRoles::All
            } else if p.eat_keyword("NONE") {
                SecondaryRoles::None
            } else {
                SecondaryRoles::List(p.parse_comma_separated(Parser::parse_ident)?)
            },
        ));
    }
    let kind = ["DATABASE", "SCHEMA", "ROLE", "WAREHOUSE"]
        .into_iter()
        .find(|kind| p.eat_keyword(kind));
    let name = parse_use_name(p)?;
    Ok(SnowflakeStatement::Use {
        kind: kind.map(Cow::Borrowed),
        name,
    })
}

fn parse_session_option<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<(Ident<'a>, Literal<'a>)> {
    let key = p.parse_ident()?;
    p.expect_op("=")?;
    match p.parse_prefix()?.kind {
        ExprKind::Literal(value) => Ok((key, value)),
        _ => Err(p.expected("a literal session parameter value")),
    }
}

fn parse_alter_session<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<SnowflakeStatement<'a>> {
    p.advance();
    p.advance();
    if p.eat_keyword("UNSET") {
        let mut names = vec![p.parse_ident()?];
        while p.eat_op(",") || p.at_ident() {
            names.push(p.parse_ident()?);
        }
        return Ok(SnowflakeStatement::AlterSessionUnset(names));
    }
    p.expect_keyword("SET")?;
    let mut options = vec![parse_session_option(p)?];
    loop {
        p.eat_op(",");
        if !p.at_ident() {
            return Ok(SnowflakeStatement::AlterSessionSet(options));
        }
        options.push(parse_session_option(p)?);
    }
}

fn at_multi_table_insert<D: Dialect<Ext = Builtin>>(p: &Parser<'_, D>) -> bool {
    let at_kind = |n| p.peek_nth(n).is_word("ALL") || p.peek_nth(n).is_word("FIRST");
    p.at_keyword("INSERT") && (at_kind(1) || (p.peek_nth(1).is_word("OVERWRITE") && at_kind(2)))
}

fn parse_insert_target<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<InsertTarget<'a>> {
    p.expect_keyword("INTO")?;
    let table = grammar::parse_object_name(p)?;
    let columns = if p.at_op("(") {
        grammar::parse_ident_list(p)?
    } else {
        Vec::new()
    };
    let values = if p.eat_keyword("VALUES") {
        p.expect_op("(")?;
        let values = p.parse_comma_separated(Parser::parse_expr)?;
        p.expect_op(")")?;
        Some(values)
    } else {
        None
    };
    Ok(InsertTarget {
        table,
        columns,
        values,
    })
}

fn parse_insert_targets<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Vec<InsertTarget<'a>>> {
    let mut targets = vec![parse_insert_target(p)?];
    while p.at_keyword("INTO") {
        targets.push(parse_insert_target(p)?);
    }
    Ok(targets)
}

fn parse_multi_table_insert<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<SnowflakeStatement<'a>> {
    p.expect_keyword("INSERT")?;
    let overwrite = p.eat_keyword("OVERWRITE");
    let first = p.eat_keyword("FIRST");
    if !first {
        p.expect_keyword("ALL")?;
    }
    let mut into = Vec::new();
    let mut when = Vec::new();
    let mut otherwise = Vec::new();
    if p.at_keyword("WHEN") {
        while p.eat_keyword("WHEN") {
            let condition = p.parse_expr()?;
            p.expect_keyword("THEN")?;
            let into = parse_insert_targets(p)?;
            when.push(InsertWhen { condition, into });
        }
        if p.eat_keyword("ELSE") {
            otherwise = parse_insert_targets(p)?;
        }
    } else {
        into = parse_insert_targets(p)?;
    }
    let source = D::parse_query(p)?;
    Ok(SnowflakeStatement::MultiTableInsert(Box::new(
        MultiTableInsert {
            overwrite,
            first,
            into,
            when,
            otherwise,
            source,
        },
    )))
}

/// Parses a stage name, which the lexer read as a word starting with `@`.
fn parse_stage<'a, D: Dialect<Ext = Builtin>>(p: &mut Parser<'a, D>) -> PResult<Ident<'a>> {
    if !(p.peek().kind == TokenKind::Word && p.peek().text.starts_with('@')) {
        return Err(p.expected("a stage name"));
    }
    p.parse_ident()
}

/// Parses `key=value` pairs up to the end of the statement.
fn parse_stage_options<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Vec<(Ident<'a>, Expr<'a, Builtin>)>> {
    let mut options = Vec::new();
    while p.at_ident() {
        let key = p.parse_ident()?;
        p.expect_op("=")?;
        options.push((key, p.parse_prefix()?));
    }
    Ok(options)
}

fn at_stage_command<D: Dialect<Ext = Builtin>>(p: &Parser<'_, D>) -> bool {
    ["LIST", "LS", "REMOVE", "RM"]
        .iter()
        .any(|word| p.at_keyword(word))
}

fn parse_stage_command<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<SnowflakeStatement<'a>> {
    let remove = p.at_keyword("REMOVE") || p.at_keyword("RM");
    p.advance();
    let stage = parse_stage(p)?;
    let options = parse_stage_options(p)?;
    Ok(SnowflakeStatement::StageCommand {
        remove,
        stage,
        options,
    })
}

fn parse_put<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<SnowflakeStatement<'a>> {
    p.advance();
    let token = p.expect_kind(TokenKind::String, "a file URL")?;
    let file = D::string_literal(token);
    let stage = parse_stage(p)?;
    let options = parse_stage_options(p)?;
    Ok(SnowflakeStatement::Put {
        file,
        stage,
        options,
    })
}

fn parse_option_value<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<OptionValue<'a>> {
    let token = p.peek();
    match token.kind {
        TokenKind::String => {
            p.advance();
            Ok(OptionValue::Literal(Literal::String(D::string_literal(
                token,
            ))))
        }
        TokenKind::Number => {
            p.advance();
            Ok(OptionValue::Literal(Literal::Number(token.text.into())))
        }
        TokenKind::Word => {
            p.advance();
            Ok(OptionValue::Word(token.text.into()))
        }
        TokenKind::Op if token.text == "(" => {
            p.advance();
            if p.peek().kind == TokenKind::String {
                let strings = p.parse_comma_separated(grammar::parse_string)?;
                p.expect_op(")")?;
                return Ok(OptionValue::List(strings));
            }
            let mut options = Vec::new();
            while !p.eat_op(")") {
                options.push(parse_key_value(p)?);
                p.eat_op(",");
            }
            Ok(OptionValue::Nested(options))
        }
        _ => Err(p.expected("an option value")),
    }
}

fn parse_key_value<'a, D: Dialect<Ext = Builtin>>(p: &mut Parser<'a, D>) -> PResult<KeyValue<'a>> {
    let key = p.parse_name_part()?;
    p.expect_op("=")?;
    let value = p.nested(parse_option_value)?;
    Ok(KeyValue { key, value })
}

/// Parses options up to something that is not a word.
fn parse_key_values<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Vec<KeyValue<'a>>> {
    let mut options = Vec::new();
    while p.peek().kind == TokenKind::Word {
        options.push(parse_key_value(p)?);
        p.eat_op(",");
    }
    Ok(options)
}

fn parse_copy_location<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<CopyLocation<'a>> {
    let token = p.peek();
    if token.kind == TokenKind::String {
        return grammar::parse_string(p).map(CopyLocation::Url);
    }
    if token.kind == TokenKind::Word && token.text.starts_with('@') {
        return parse_stage(p).map(CopyLocation::Stage);
    }
    grammar::parse_object_name(p).map(CopyLocation::Name)
}

/// Parses the storage options that can follow a location.
fn parse_stage_params<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Vec<KeyValue<'a>>> {
    let mut params = Vec::new();
    while [
        "STORAGE_INTEGRATION",
        "ENDPOINT",
        "CREDENTIALS",
        "ENCRYPTION",
    ]
    .iter()
    .any(|word| p.at_keyword(word))
    {
        params.push(parse_key_value(p)?);
    }
    Ok(params)
}

fn parse_copy_clause<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<CopyClause<'a>> {
    if p.at_keyword("PARTITION") && p.peek_nth(1).is_word("BY") {
        p.advance();
        p.advance();
        return p.parse_expr().map(CopyClause::PartitionBy);
    }
    let option = parse_key_value(p)?;
    if option.key.text.eq_ignore_ascii_case("FILES")
        && !matches!(option.value, OptionValue::List(_))
    {
        return Err(p.expected("a list of files"));
    }
    Ok(CopyClause::Option(option))
}

fn parse_copy_into<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<SnowflakeStatement<'a>> {
    p.advance();
    p.advance();
    let into = parse_copy_location(p)?;
    let into_params = parse_stage_params(p)?;
    let columns = if p.at_op("(") {
        grammar::parse_ident_list(p)?
    } else {
        Vec::new()
    };
    p.expect_keyword("FROM")?;
    let from = if p.eat_op("(") {
        let query = p.nested(D::parse_query)?;
        p.expect_op(")")?;
        CopySource::Query(Box::new(query))
    } else {
        CopySource::Location(parse_copy_location(p)?)
    };
    let from_params = parse_stage_params(p)?;
    let alias = if p.eat_keyword("AS") {
        Some(p.parse_ident()?)
    } else {
        None
    };
    let mut clauses = Vec::new();
    while p.peek().kind == TokenKind::Word {
        clauses.push(parse_copy_clause(p)?);
        p.eat_op(",");
    }
    Ok(SnowflakeStatement::CopyInto(Box::new(CopyInto {
        into,
        into_params,
        columns,
        from,
        from_params,
        alias,
        clauses,
    })))
}

/// The offset of the word after `CREATE [OR REPLACE] [TEMPORARY]`.
fn create_kind_offset<D: Dialect<Ext = Builtin>>(p: &Parser<'_, D>) -> usize {
    let mut n = 1;
    if p.peek_nth(n).is_word("OR") {
        n += 2;
    }
    if ["TEMP", "TEMPORARY", "VOLATILE"]
        .iter()
        .any(|word| p.peek_nth(n).is_word(word))
    {
        n += 1;
    }
    n
}

fn at_create_object<D: Dialect<Ext = Builtin>>(p: &Parser<'_, D>) -> bool {
    if !p.at_keyword("CREATE") {
        return false;
    }
    let n = create_kind_offset(p);
    p.peek_nth(n).is_word("STAGE")
        || p.peek_nth(n).is_word("WAREHOUSE")
        || (p.peek_nth(n).is_word("FILE") && p.peek_nth(n + 1).is_word("FORMAT"))
}

fn parse_create_object<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<SnowflakeStatement<'a>> {
    p.expect_keyword("CREATE")?;
    let or_replace = grammar::parse_or_replace(p)?;
    let lifetime = if p.eat_keyword("TEMP") || p.eat_keyword("TEMPORARY") {
        Some("TEMPORARY")
    } else if p.eat_keyword("VOLATILE") {
        Some("VOLATILE")
    } else {
        None
    };
    let kind = if p.eat_keyword("STAGE") {
        "STAGE"
    } else if p.eat_keyword("WAREHOUSE") {
        if lifetime.is_some() {
            return Err(p.expected("a warehouse without TEMPORARY or VOLATILE"));
        }
        "WAREHOUSE"
    } else {
        p.expect_keyword("FILE")?;
        p.expect_keyword("FORMAT")?;
        "FILE FORMAT"
    };
    let if_not_exists = grammar::parse_if_not_exists(p)?;
    let name = grammar::parse_object_name(p)?;
    if kind == "WAREHOUSE" {
        p.eat_keyword("WITH");
    }
    let options = parse_key_values(p)?;
    Ok(SnowflakeStatement::CreateObject(Box::new(CreateObject {
        or_replace,
        lifetime: lifetime.map(Cow::Borrowed),
        kind: kind.into(),
        if_not_exists,
        name,
        options,
    })))
}

pub fn parse_statement<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Statement<'a, Builtin>> {
    if p.at_keyword("COMMENT") {
        return grammar::parse_comment_on(p).map(|c| StatementKind::Comment(Box::new(c)).into());
    }
    if ddl::at_create_table(p) {
        return ddl::parse_create_table(p);
    }
    if ddl::at_create_database(p) {
        return ddl::parse_create_database(p);
    }
    if ddl::at_create_view(p) {
        return ddl::parse_create_view(p);
    }
    if p.at_keyword("EXECUTE") && p.peek_nth(1).is_word("IMMEDIATE") {
        let execute = oracle::parse_execute_immediate(p)?;
        return Ok(StatementKind::Ext(BuiltinStatement::Oracle(execute)).into());
    }
    let statement = if at_create_user(p) {
        parse_create_user(p)?
    } else if at_alter_user(p) {
        parse_alter_user(p)?
    } else if p.at_keyword("USE") {
        parse_use(p)?
    } else if ddl::at_alter_iceberg_table(p) {
        ddl::parse_alter_iceberg_table(p)?
    } else if ddl::at_alter_table_kind(p) {
        ddl::parse_alter_table_kind(p)?
    } else if ddl::at_block(p) {
        ddl::parse_block(p)?
    } else if p.at_keyword("RAISE") {
        ddl::parse_raise(p)?
    } else if p.at_keyword("DECLARE") {
        ddl::parse_declare(p)?
    } else if ddl::at_set_tuple(p) {
        ddl::parse_set_tuple(p)?
    } else if at_multi_table_insert(p) {
        parse_multi_table_insert(p)?
    } else if at_stage_command(p) {
        parse_stage_command(p)?
    } else if p.at_keyword("PUT") {
        parse_put(p)?
    } else if p.at_keyword("COPY") && p.peek_nth(1).is_word("INTO") {
        parse_copy_into(p)?
    } else if at_create_object(p) {
        parse_create_object(p)?
    } else if p.at_keyword("ALTER") && {
        let next = p.peek_nth(1);
        next.kind == TokenKind::Word && next.text.eq_ignore_ascii_case("SESSION")
    } {
        parse_alter_session(p)?
    } else {
        return grammar::parse_statement(p);
    };
    Ok(StatementKind::Ext(BuiltinStatement::Snowflake(statement)).into())
}

/// Adds any options that follow a wildcard.
pub fn parse_select_item<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<SelectItem<'a, Builtin>> {
    let identifier_wildcard =
        at_identifier_call(p) && p.peek_nth(4).is_op(".") && p.peek_nth(5).is_op("*");
    let item = if identifier_wildcard {
        let name = ObjectName {
            parts: vec![parse_identifier_call(p)?],
        };
        p.advance();
        p.advance();
        SelectItemKind::QualifiedWildcard(name).into()
    } else {
        grammar::parse_select_item(p)?
    };
    let qualifier = match item.kind {
        SelectItemKind::Wildcard => None,
        SelectItemKind::QualifiedWildcard(ref name) => Some(name.clone()),
        other => return Ok(other.into()),
    };
    Ok(match grammar::parse_wildcard_options(p)? {
        Some(options) => SelectItemKind::WildcardWithOptions {
            qualifier,
            options: Box::new(options),
        }
        .into(),
        None => item,
    })
}

/// True at a word followed by `(`, as in `AT(...)`.
fn at_call<D: Dialect<Ext = Builtin>>(p: &Parser<'_, D>, word: &str) -> bool {
    p.at_keyword(word) && p.peek_nth(1).is_op("(")
}

fn parse_named_call<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Expr<'a, Builtin>> {
    let name = ObjectName {
        parts: vec![p.parse_ident()?],
    };
    p.parse_function_call(name)
}

/// Parses `AT(...)`, `BEFORE(...)` or `CHANGES(...) AT(...) [END(...)]` if one
/// follows the table's name.
fn parse_table_version<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Vec<Expr<'a, Builtin>>> {
    let mut calls = Vec::new();
    if at_call(p, "CHANGES") {
        calls.push(parse_named_call(p)?);
        if !(at_call(p, "AT") || at_call(p, "BEFORE")) {
            return Err(p.expected("AT or BEFORE"));
        }
        calls.push(parse_named_call(p)?);
        if at_call(p, "END") {
            calls.push(parse_named_call(p)?);
        }
    } else if at_call(p, "AT") || at_call(p, "BEFORE") {
        calls.push(parse_named_call(p)?);
    }
    Ok(calls)
}

fn parse_semantic_view_list<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
    list: &mut Vec<FunctionArg<'a, Builtin>>,
) -> PResult<()> {
    if !list.is_empty() {
        return Err(p.expected("each clause of SEMANTIC_VIEW only once"));
    }
    *list = p.parse_comma_separated(D::parse_function_arg)?;
    Ok(())
}

/// Parses `SEMANTIC_VIEW(...)`, whose clauses can be in any order.
fn parse_semantic_view<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<TableFactor<'a, Builtin>> {
    let start = p.peek().span.start;
    let name = ObjectName {
        parts: vec![p.parse_ident()?],
    };
    p.advance();
    let args_start = p.peek().span.start;
    let mut view = SemanticView {
        name: grammar::parse_object_name(p)?,
        dimensions: Vec::new(),
        metrics: Vec::new(),
        facts: Vec::new(),
        selection: None,
    };
    while !p.at_op(")") {
        if p.eat_keyword("DIMENSIONS") {
            parse_semantic_view_list(p, &mut view.dimensions)?;
        } else if p.eat_keyword("METRICS") {
            parse_semantic_view_list(p, &mut view.metrics)?;
        } else if p.eat_keyword("FACTS") {
            parse_semantic_view_list(p, &mut view.facts)?;
        } else if p.at_keyword("WHERE") && view.selection.is_none() {
            p.advance();
            view.selection = Some(p.parse_expr()?);
        } else {
            return Err(p.expected("DIMENSIONS, METRICS, FACTS or WHERE"));
        }
    }
    let arg = Expr::new(
        ExprKind::Ext(BuiltinExpr::Snowflake(SnowflakeExpr::SemanticView(
            Box::new(view),
        ))),
        p.span_since(args_start),
    );
    p.expect_op(")")?;
    Ok(TableFactor::Function {
        call: Box::new(Expr::new(
            ExprKind::Function {
                name,
                args: vec![FunctionArg::Expr(arg)],
                options: None,
            },
            p.span_since(start),
        )),
        with_ordinality: false,
    })
}

/// Parses a call's arguments, which can be one unparenthesized query.
pub fn parse_function_args<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
    quantifier: SetQuantifier,
) -> PResult<Vec<FunctionArg<'a, Builtin>>> {
    if quantifier == SetQuantifier::None && (p.at_keyword("SELECT") || p.at_keyword("WITH")) {
        let start = p.peek().span.start;
        let query = p.nested(D::parse_query)?;
        return Ok(vec![FunctionArg::Expr(Expr::new(
            ExprKind::Ext(BuiltinExpr::Snowflake(SnowflakeExpr::BareSubquery(
                Box::new(query),
            ))),
            p.span_since(start),
        ))]);
    }
    grammar::parse_function_args(p, quantifier)
}

/// True at a join that follows a table without a condition between them, so
/// that the join nests inside the outer one.
fn at_nested_join<D: Dialect>(p: &Parser<'_, D>) -> bool {
    ["JOIN", "INNER", "LEFT", "RIGHT", "FULL"]
        .iter()
        .any(|word| p.at_keyword(word))
}

/// Parses what follows `JOIN`, where a further `JOIN` nests without parentheses.
pub fn parse_join_table<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
    has_constraint: bool,
) -> PResult<TableRef<'a, Builtin>> {
    let relation = p.parse_table_ref()?;
    if !has_constraint || !at_nested_join(p) {
        return Ok(relation);
    }
    let joins = p.nested(|p| {
        let mut joins = Vec::new();
        while let Some(join) = D::parse_join(p)? {
            joins.push(join);
        }
        Ok(joins)
    })?;
    Ok(TableRef {
        factor: TableFactor::NestedJoin(Box::new(TableWithJoins { relation, joins })),
        alias: None,
        span: Span::default(),
    })
}

/// Parses `TOP n`, which here comes after any `DISTINCT` and only takes a number.
pub fn parse_select_head<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<(BuiltinSelect<'a>, SetQuantifier)> {
    let quantifier = grammar::parse_quantifier(p);
    let number = p.peek_nth(1);
    let mssql_top = if p.at_keyword("TOP") && number.kind == TokenKind::Number {
        p.advance();
        p.advance();
        Some(Box::new(MsSqlTop {
            quantity: Expr::new(
                ExprKind::Literal(Literal::Number(number.text.into())),
                number.span,
            ),
            parenthesized: false,
            percent: false,
            with_ties: false,
        }))
    } else {
        None
    };
    let modifiers = BuiltinSelect {
        mssql_top,
        ..Default::default()
    };
    Ok((modifiers, quantifier))
}

pub fn parse_table_factor<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<TableFactor<'a, Builtin>> {
    if at_call(p, "SEMANTIC_VIEW") {
        return parse_semantic_view(p);
    }
    let factor = spark::parse_table_factor(p)?;
    let TableFactor::Named(name) = factor else {
        return Ok(factor);
    };
    let calls = parse_table_version(p)?;
    Ok(if calls.is_empty() {
        TableFactor::Named(name)
    } else {
        TableFactor::Versioned { name, calls }
    })
}

/// Parses what follows `SAMPLE` or `TABLESAMPLE`, which here can name a method
/// and end in a seed.
pub fn parse_sample_clause<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
    long_form: bool,
) -> PResult<SampleClause<'a, D::Ext>> {
    let method = ["BERNOULLI", "ROW", "SYSTEM", "BLOCK"]
        .into_iter()
        .find(|method| p.eat_keyword(method));
    let mut clause = grammar::parse_sample_clause(p, long_form)?;
    clause.method = method.map(Cow::Borrowed);
    clause.seed = grammar::parse_sample_seed(p)?;
    Ok(clause)
}

/// Parses the values after `IN`, which here can be `ANY` or a query.
pub fn parse_pivot_source<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<PivotSource<'a, D::Ext>> {
    if p.eat_keyword("ANY") {
        let order_by = if p.eat_keyword("ORDER") {
            p.expect_keyword("BY")?;
            p.parse_comma_separated(D::parse_order_by_item)?
        } else {
            Vec::new()
        };
        return Ok(PivotSource::Any(order_by));
    }
    if p.at_keyword("SELECT") || p.at_keyword("WITH") {
        return Ok(PivotSource::Query(Box::new(p.nested(D::parse_query)?)));
    }
    grammar::parse_pivot_source(p)
}

/// Parses what follows `PIVOT`, which here can end in `DEFAULT ON NULL`.
fn parse_pivot<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
    table: TableRef<'a, Builtin>,
) -> PResult<TableFactor<'a, Builtin>> {
    let mut clause = grammar::parse_pivot_head(p)?;
    if p.eat_keyword("DEFAULT") {
        p.expect_keyword("ON")?;
        p.expect_keyword("NULL")?;
        p.expect_op("(")?;
        clause.default_on_null = Some(p.parse_expr()?);
        p.expect_op(")")?;
    }
    p.expect_op(")")?;
    Ok(TableFactor::Pivot(Box::new(Pivot { table, clause })))
}

/// Levels of tree height added for a clause that wraps the table before it,
/// whose nodes take twice the stack of an ordinary level to clone or render.
const TABLE_CLAUSE_LEVELS: usize = 2;

/// Applies any `PIVOT`, `UNPIVOT` and `SAMPLE` clauses, which can be stacked.
pub fn parse_table_ref<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<TableRef<'a, Builtin>> {
    parse_table_ref_with(p, parse_alias_before_pivot)
}

/// Parses a table and its clauses, reading aliases with `parse_alias`.
fn parse_table_ref_with<'a, D, F>(
    p: &mut Parser<'a, D>,
    parse_alias: F,
) -> PResult<TableRef<'a, Builtin>>
where
    D: Dialect<Ext = Builtin>,
    F: Fn(&mut Parser<'a, D>) -> PResult<Option<TableAlias<'a>>>,
{
    let factor = D::parse_table_factor(p)?;
    let alias = parse_alias(p)?;
    let mut table = strip_parentheses(
        p,
        TableRef {
            factor,
            alias,
            span: Span::default(),
        },
    )?;
    loop {
        if p.at_keyword("SAMPLE") || p.at_keyword("TABLESAMPLE") {
            let long_form = p.at_keyword("TABLESAMPLE");
            p.advance();
            let sample = D::parse_sample_clause(p, long_form)?;
            let factor = TableFactor::Sampled(Box::new(TableSample { table, sample }));
            p.deepen_by(TABLE_CLAUSE_LEVELS)?;
            table = TableRef {
                factor,
                alias: None,
                span: Span::default(),
            };
            continue;
        }
        let factor = if at_call(p, "PIVOT") {
            p.advance();
            parse_pivot(p, table)?
        } else if at_call(p, "MATCH_RECOGNIZE") {
            p.advance();
            // Rendering this clause takes more stack than the others.
            p.deepen()?;
            grammar::parse_match_recognize(p, table)?
        } else if at_unpivot(p) {
            p.advance();
            let clause = grammar::parse_unpivot_clause(p)?;
            TableFactor::Unpivot(Box::new(Unpivot { table, clause }))
        } else {
            return Ok(table);
        };
        p.deepen_by(TABLE_CLAUSE_LEVELS)?;
        let alias = parse_alias(p)?;
        table = TableRef {
            factor,
            alias,
            span: Span::default(),
        };
    }
}

/// Removes parentheses that hold a table and no join.
fn strip_parentheses<'a, D: Dialect<Ext = Builtin>>(
    p: &Parser<'a, D>,
    table: TableRef<'a, Builtin>,
) -> PResult<TableRef<'a, Builtin>> {
    match table.factor {
        // Inner parentheses have already been stripped, so one that is left
        // around a nested join is what groups it.
        TableFactor::NestedJoin(inner)
            if inner.joins.is_empty()
                && !matches!(inner.relation.factor, TableFactor::NestedJoin(_))
                // An alias cannot be written after a sample's own clause.
                && !(table.alias.is_some()
                    && matches!(inner.relation.factor, TableFactor::Sampled(_))) =>
        {
            if inner.relation.alias.is_some() && table.alias.is_some() {
                return Err(p.expected("only one alias for the table"));
            }
            Ok(TableRef {
                alias: inner.relation.alias.or(table.alias),
                factor: inner.relation.factor,
                span: Span::default(),
            })
        }
        factor => Ok(TableRef {
            factor,
            alias: table.alias,
            span: Span::default(),
        }),
    }
}

fn at_unpivot<D: Dialect<Ext = Builtin>>(p: &Parser<'_, D>) -> bool {
    let next = p.peek_nth(1);
    p.at_keyword("UNPIVOT")
        && (next.is_op("(") || next.is_word("INCLUDE") || next.is_word("EXCLUDE"))
}

/// A table alias that can also be a clause keyword.
fn parse_table_alias<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Option<TableAlias<'a>>> {
    if clause_word_is_name(p, true) {
        return Ok(Some(TableAlias {
            name: p.parse_name_part()?,
            explicit: false,
            columns: Vec::new(),
            column_types: Vec::new(),
        }));
    }
    parse_alias_before_pivot(p)
}

/// `PIVOT` is an ordinary alias unless the rest of the clause follows it.
fn parse_alias_before_pivot<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Option<TableAlias<'a>>> {
    if at_call(p, "PIVOT") || at_unpivot(p) || at_call(p, "MATCH_RECOGNIZE") {
        return Ok(None);
    }
    grammar::parse_optional_table_alias(p)
}

pub fn parse_join<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Option<Join<'a, Builtin>>> {
    if !(p.at_keyword("ASOF") && p.peek_nth(1).is_word("JOIN")) {
        return grammar::parse_join(p);
    }
    p.advance();
    p.advance();
    let table = p.parse_table_ref()?;
    p.expect_keyword("MATCH_CONDITION")?;
    p.expect_op("(")?;
    let condition = p.parse_expr()?;
    p.expect_op(")")?;
    let constraint = if p.eat_keyword("ON") {
        JoinConstraint::On(p.parse_expr()?)
    } else if p.eat_keyword("USING") {
        JoinConstraint::Using(grammar::parse_ident_list(p)?)
    } else {
        JoinConstraint::None
    };
    Ok(Some(Join {
        kind: JoinKind::AsOf,
        spelled_out: false,
        table,
        constraint: JoinConstraint::AsOf {
            condition,
            constraint: Box::new(constraint),
        },
    }))
}

pub fn parse_set<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<SetStatement<'a, Builtin>> {
    // `@name` is a stage, not a variable.
    if p.peek_nth(1).text.starts_with('@') {
        p.advance();
        return Err(p.expected("a variable name"));
    }
    grammar::parse_set(p)
}

pub fn parse_alter_table_op<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<AlterTableOp<'a, Builtin>> {
    let next = p.peek_nth(1);
    if p.at_keyword("SWAP") && next.is_word("WITH") {
        p.advance();
        p.advance();
        return grammar::parse_object_name(p).map(AlterTableOp::SwapWith);
    }
    if p.at_keyword("CLUSTER") && next.is_word("BY") {
        p.advance();
        p.advance();
        p.expect_op("(")?;
        let exprs = p.parse_comma_separated(Parser::parse_expr)?;
        p.expect_op(")")?;
        return Ok(AlterTableOp::ClusterBy(exprs));
    }
    if p.at_keyword("DROP") && next.is_word("CLUSTERING") && p.peek_nth(2).is_word("KEY") {
        p.advance();
        p.advance();
        p.expect_keyword("KEY")?;
        return Ok(AlterTableOp::Phrase("DROP CLUSTERING KEY".into()));
    }
    if p.at_keyword("SUSPEND") && next.is_word("RECLUSTER") {
        p.advance();
        p.advance();
        return Ok(AlterTableOp::Phrase("SUSPEND RECLUSTER".into()));
    }
    if p.at_keyword("RESUME") && next.is_word("RECLUSTER") {
        p.advance();
        p.advance();
        return Ok(AlterTableOp::Phrase("RESUME RECLUSTER".into()));
    }
    if p.at_keyword("DROP") && next.is_word("PRIMARY") && p.peek_nth(2).is_word("KEY") {
        p.advance();
        p.advance();
        p.advance();
        return Ok(AlterTableOp::Phrase(
            match grammar::parse_drop_behavior(p) {
                Some(DropBehavior::Cascade) => "DROP PRIMARY KEY CASCADE".into(),
                Some(DropBehavior::Restrict) => "DROP PRIMARY KEY RESTRICT".into(),
                None => "DROP PRIMARY KEY".into(),
            },
        ));
    }
    if p.at_keyword("DROP") && next.is_word("FOREIGN") && p.peek_nth(2).is_word("KEY") {
        p.advance();
        p.advance();
        p.advance();
        let name = p.parse_ident()?;
        let behavior = grammar::parse_drop_behavior(p);
        return Ok(AlterTableOp::DropForeignKey { name, behavior });
    }
    match grammar::parse_alter_table_op(p)? {
        AlterTableOp::DropColumn {
            column_keyword,
            if_exists,
            name,
            behavior: None,
        } if at_another_dropped_column(p) => {
            let mut names = vec![name];
            while at_another_dropped_column(p) {
                p.advance();
                names.push(p.parse_ident()?);
            }
            Ok(AlterTableOp::DropColumns {
                column_keyword,
                if_exists,
                names,
                behavior: grammar::parse_drop_behavior(p),
            })
        }
        operation => Ok(operation),
    }
}

/// True at a comma before another column of `DROP COLUMN`, not another operation.
fn at_another_dropped_column<D: Dialect<Ext = Builtin>>(p: &Parser<'_, D>) -> bool {
    const OPERATIONS: [&str; 10] = [
        "ADD", "ALTER", "CLUSTER", "DROP", "MODIFY", "RENAME", "RESUME", "SET", "SUSPEND", "SWAP",
    ];
    let next = p.peek_nth(1);
    p.at_op(",")
        && matches!(next.kind, TokenKind::Word | TokenKind::QuotedIdent)
        && !OPERATIONS.iter().any(|word| next.is_word(word))
}

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

pub fn parse_name_part<'a, D: Dialect<Ext = Builtin>>(p: &mut Parser<'a, D>) -> PResult<Ident<'a>> {
    let token = p.peek();
    if token.kind == TokenKind::Placeholder && token.text.starts_with('$') {
        p.advance();
        return Ok(Ident {
            text: token.text.into(),
            quote: None,
            span: token.span,
        });
    }
    grammar::parse_name_part(p)
}

fn at_typed_lambda<D: Dialect>(p: &Parser<'_, D>) -> bool {
    let typed_parameter = if p.at_op("(") {
        p.peek_nth(2).kind == TokenKind::Word
    } else {
        p.peek_nth(1).kind == TokenKind::Word
    };
    typed_parameter && clickhouse::at_typed_lambda(p)
}

pub fn parse_prefix<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Expr<'a, Builtin>> {
    if spark::at_lambda(p) {
        return spark::parse_lambda(p);
    }
    if at_typed_lambda(p) {
        return clickhouse::parse_typed_lambda(p);
    }
    if p.at_op("{") {
        return clickhouse::parse_dictionary(p);
    }
    let token = p.peek();
    if token.kind != TokenKind::Word {
        return grammar::parse_prefix(p);
    }
    // Stage names are only valid where a table is.
    if token.text.starts_with('@') {
        return Err(p.expected("an expression"));
    }
    if p.has_context(grammar::CONTEXT_CONNECT_BY) && p.eat_keyword("PRIOR") {
        let expr = p.parse_expr_bp(prec::ADD)?;
        return Ok(ExprKind::Special(Box::new(SpecialExpr::Prior(expr))).into());
    }
    if p.eat_keyword("CONNECT_BY_ROOT") {
        let expr = p.parse_expr_bp(prec::UNARY)?;
        return Ok(ExprKind::Special(Box::new(SpecialExpr::ConnectByRoot(expr))).into());
    }
    grammar::parse_prefix(p)
}

pub fn parse_qualify<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Option<Expr<'a, Builtin>>> {
    if p.eat_keyword("QUALIFY") {
        p.parse_expr().map(Some)
    } else {
        Ok(None)
    }
}

pub fn set_operator_rule<'a, D: Dialect<Ext = Builtin>>(
    p: &Parser<'a, D>,
) -> Option<SetOperatorRule> {
    grammar::set_operator_rule_with_minus(p)
}

impl Dialect for Snowflake {
    type Ext = Builtin;

    fn parse_create_extension<'a>(
        p: &mut Parser<'a, Self>,
        flags: CreateFlags,
    ) -> PResult<Option<Statement<'a, Builtin>>> {
        grammar::parse_create_role(p, flags, false)
    }

    fn parse_like_quantifier<'a>(p: &mut Parser<'a, Self>) -> PResult<Option<Quantifier>> {
        Ok(grammar::parse_like_any(p))
    }

    fn parse_update_from_after_set<'a>(
        p: &mut Parser<'a, Self>,
    ) -> PResult<Vec<TableWithJoins<'a, Builtin>>> {
        grammar::parse_optional_update_from(p)
    }

    fn parse_delete_using<'a>(
        p: &mut Parser<'a, Self>,
    ) -> PResult<Vec<TableWithJoins<'a, Builtin>>> {
        grammar::parse_optional_delete_using(p)
    }

    fn parse_select_into<'a>(
        p: &mut Parser<'a, Self>,
    ) -> PResult<Option<Box<SelectInto<'a, Builtin>>>> {
        grammar::parse_select_into_variables(p)
    }

    fn parse_group_by_item<'a>(p: &mut Parser<'a, Self>) -> PResult<Expr<'a, Builtin>> {
        grammar::parse_grouping_item_or_empty_set(p)
    }

    fn parse_create_schema<'a>(
        p: &mut Parser<'a, Self>,
        flags: CreateFlags,
    ) -> PResult<CreateSchema<'a>> {
        if flags.temporary {
            return Err(p.expected("TABLE or VIEW"));
        }
        let mut schema = grammar::parse_create_schema_body(p, flags)?;
        if p.eat_keyword("CLONE") {
            schema.clone = Some(grammar::parse_object_name(p)?);
        }
        Ok(schema)
    }

    fn parse_table_factor<'a>(p: &mut Parser<'a, Self>) -> PResult<TableFactor<'a, Builtin>> {
        parse_table_factor(p)
    }

    fn parse_table_ref<'a>(p: &mut Parser<'a, Self>) -> PResult<TableRef<'a, Builtin>> {
        parse_table_ref_with(p, parse_table_alias)
    }

    fn parse_join<'a>(p: &mut Parser<'a, Self>) -> PResult<Option<Join<'a, Builtin>>> {
        parse_join(p)
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
    fn lex_comment(rest: &str) -> Option<Result<usize, &'static str>> {
        lex_comment(rest)
    }

    #[inline]
    fn string_literal(token: Token<'_>) -> StringLiteral<'_> {
        StringLiteral {
            backslash_escapes: true,
            ..grammar::string_literal(token)
        }
    }

    #[inline]
    fn is_reserved(word: &str) -> bool {
        // These start clauses, but only when what follows says so.
        if ["LIMIT", "OFFSET", "EXCEPT", "OUTER"]
            .iter()
            .any(|name| name.eq_ignore_ascii_case(word))
        {
            return false;
        }
        // A prefix operator rather than a name.
        word.eq_ignore_ascii_case("CONNECT_BY_ROOT") || grammar::is_reserved(word)
    }

    fn parse_interval<'a>(p: &mut Parser<'a, Self>) -> PResult<Expr<'a, Builtin>> {
        grammar::parse_interval(p, false)
    }

    #[inline]
    fn is_reserved_for_alias(word: &str) -> bool {
        is_reserved_for_alias(word)
    }

    fn check_nested_join<'a>(
        _p: &Parser<'a, Self>,
        _table: &TableWithJoins<'a, Builtin>,
    ) -> PResult<()> {
        Ok(())
    }

    fn at_function_call<'a>(p: &Parser<'a, Self>) -> bool {
        p.at_op("(") && !at_outer_join_mark(p)
    }

    fn parse_column_option<'a>(
        p: &mut Parser<'a, Self>,
    ) -> PResult<Option<ColumnOption<'a, Builtin>>> {
        ddl::parse_column_option(p)
    }

    fn parse_set<'a>(p: &mut Parser<'a, Self>) -> PResult<SetStatement<'a, Builtin>> {
        parse_set(p)
    }

    fn parse_alter_table_op<'a>(p: &mut Parser<'a, Self>) -> PResult<AlterTableOp<'a, Builtin>> {
        parse_alter_table_op(p)
    }

    fn parse_explain_target<'a>(p: &mut Parser<'a, Self>) -> PResult<ExplainTarget<'a, Builtin>> {
        parse_explain_target(p)
    }

    fn parse_drop_object_type<'a>(p: &mut Parser<'a, Self>) -> Option<ObjectType<'a>> {
        grammar::eat_drop_object_type(p, &["STAGE", "STREAM", "WAREHOUSE"])
    }

    fn parse_name_part<'a>(p: &mut Parser<'a, Self>) -> PResult<Ident<'a>> {
        parse_name_part(p)
    }

    fn parse_connect_by<'a>(p: &mut Parser<'a, Self>) -> PResult<Vec<ConnectBy<'a, Builtin>>> {
        grammar::parse_connect_by_clauses(p)
    }

    fn parse_sample_clause<'a>(
        p: &mut Parser<'a, Self>,
        long_form: bool,
    ) -> PResult<SampleClause<'a, Builtin>> {
        parse_sample_clause(p, long_form)
    }

    fn parse_pivot_source<'a>(p: &mut Parser<'a, Self>) -> PResult<PivotSource<'a, Builtin>> {
        parse_pivot_source(p)
    }

    fn parse_prefix<'a>(p: &mut Parser<'a, Self>) -> PResult<Expr<'a, Builtin>> {
        parse_prefix(p)
    }

    fn infix_rule<'a>(p: &Parser<'a, Self>) -> Option<InfixRule<'a, Self>> {
        infix_rule(p)
    }

    fn parse_statement<'a>(p: &mut Parser<'a, Self>) -> PResult<Statement<'a, Builtin>> {
        parse_statement(p)
    }

    fn parse_select_item<'a>(p: &mut Parser<'a, Self>) -> PResult<SelectItem<'a, Builtin>> {
        parse_select_item(p)
    }

    fn parse_projection<'a>(p: &mut Parser<'a, Self>) -> PResult<Vec<SelectItem<'a, Builtin>>> {
        parse_list_with_trailing_comma(p, Parser::parse_select_item)
    }

    fn parse_from_list<'a>(p: &mut Parser<'a, Self>) -> PResult<Vec<TableWithJoins<'a, Builtin>>> {
        parse_list_with_trailing_comma(p, Self::parse_table_with_joins)
    }

    fn parse_qualify<'a>(p: &mut Parser<'a, Self>) -> PResult<Option<Expr<'a, Builtin>>> {
        parse_qualify(p)
    }

    fn parse_select_item_alias<'a>(
        p: &mut Parser<'a, Self>,
        expr: Expr<'a, Builtin>,
    ) -> PResult<SelectItem<'a, Builtin>> {
        parse_select_item_alias(p, expr)
    }

    fn parse_object_name_part<'a>(
        p: &mut Parser<'a, Self>,
        before: &[Ident<'a>],
    ) -> PResult<Ident<'a>> {
        parse_object_name_part(p, before)
    }

    fn parse_join_table<'a>(
        p: &mut Parser<'a, Self>,
        has_constraint: bool,
    ) -> PResult<TableRef<'a, Builtin>> {
        parse_join_table(p, has_constraint)
    }

    fn parse_function_args<'a>(
        p: &mut Parser<'a, Self>,
        quantifier: SetQuantifier,
    ) -> PResult<Vec<FunctionArg<'a, Builtin>>> {
        parse_function_args(p, quantifier)
    }

    fn parse_function_arg_null_treatment<'a>(
        p: &mut Parser<'a, Self>,
    ) -> PResult<Option<NullTreatment>> {
        Ok(grammar::parse_null_treatment(p))
    }

    fn parse_select_head<'a>(
        p: &mut Parser<'a, Self>,
    ) -> PResult<(BuiltinSelect<'a>, SetQuantifier)> {
        parse_select_head(p)
    }

    fn parse_extract_field<'a>(p: &mut Parser<'a, Self>) -> PResult<&'a str> {
        let token = p.peek();
        if matches!(token.kind, TokenKind::Word | TokenKind::String) {
            p.advance();
            return Ok(token.text);
        }
        Err(p.expected("a date or time field"))
    }

    fn parse_insert_modifiers<'a>(p: &mut Parser<'a, Self>) -> PResult<BuiltinInsert<'a>> {
        Ok(BuiltinInsert {
            overwrite: p.eat_keyword("OVERWRITE"),
            ..Default::default()
        })
    }

    fn parse_data_type<'a>(p: &mut Parser<'a, Self>) -> PResult<DataType<'a>> {
        ddl::parse_data_type(p)
    }

    fn parse_query_order_by<'a>(
        p: &mut Parser<'a, Self>,
    ) -> PResult<Vec<OrderByItem<'a, Builtin>>> {
        duckdb::parse_query_order_by(p)
    }

    fn eat_group_by_all<'a>(p: &mut Parser<'a, Self>) -> bool {
        p.eat_keyword("ALL")
    }

    fn parse_add_column_if_not_exists<'a>(p: &mut Parser<'a, Self>) -> PResult<bool> {
        grammar::parse_if_not_exists(p)
    }

    fn set_operator_rule<'a>(p: &Parser<'a, Self>) -> Option<SetOperatorRule> {
        set_operator_rule(p)
    }
}

impl fmt::Display for SnowflakeExpr<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SnowflakeExpr::Path { value, path } => {
                fmt::Display::fmt(&**value, f)?;
                // A path always starts with `:`, even before an index.
                if !matches!(path.first(), Some(PathElement::Colon(_))) {
                    f.write_str(":")?;
                }
                for element in path {
                    match element {
                        PathElement::Colon(key) => write!(f, ":{key}")?,
                        PathElement::Dot(key) => write!(f, ".{key}")?,
                        PathElement::Index(index) => write!(f, "[{index}]")?,
                    }
                }
                Ok(())
            }
            SnowflakeExpr::BareSubquery(query) => query.fmt(f),
            SnowflakeExpr::SemanticView(view) => view.fmt(f),
        }
    }
}

impl fmt::Display for KeyValue<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let spaced = ["FILES", "PATTERN", "VALIDATION_MODE"]
            .iter()
            .any(|key| self.key.text.eq_ignore_ascii_case(key));
        write!(f, "{}{}", self.key, if spaced { " = " } else { "=" })?;
        write_value(f, &self.value)
    }
}

fn write_value(f: &mut fmt::Formatter<'_>, value: &OptionValue<'_>) -> fmt::Result {
    match value {
        OptionValue::Word(word) => f.write_str(word),
        OptionValue::Literal(literal) => write!(f, "{literal}"),
        OptionValue::List(strings) => {
            f.write_str("(")?;
            for (i, string) in strings.iter().enumerate() {
                if i > 0 {
                    f.write_str(", ")?;
                }
                write!(f, "{}", string)?;
            }
            f.write_str(")")
        }
        OptionValue::Nested(options) => {
            f.write_str("(")?;
            for (i, option) in options.iter().enumerate() {
                if i > 0 {
                    f.write_str(" ")?;
                }
                write!(f, "{option}")?;
            }
            f.write_str(")")
        }
    }
}

impl fmt::Display for CopyLocation<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CopyLocation::Name(name) => write!(f, "{name}"),
            CopyLocation::Stage(stage) => write!(f, "{stage}"),
            CopyLocation::Url(url) => write!(f, "{}", url),
        }
    }
}

fn write_key_values(f: &mut fmt::Formatter<'_>, options: &[KeyValue<'_>]) -> fmt::Result {
    for option in options {
        write!(f, " {option}")?;
    }
    Ok(())
}

impl fmt::Display for CopyInto<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "COPY INTO {}", self.into)?;
        write_key_values(f, &self.into_params)?;
        if !self.columns.is_empty() {
            f.write_str(" (")?;
            write!(f, "{}", comma(&self.columns))?;
            f.write_str(")")?;
        }
        match &self.from {
            CopySource::Query(query) => write!(f, " FROM ({query})")?,
            CopySource::Location(location) => write!(f, " FROM {location}")?,
        }
        write_key_values(f, &self.from_params)?;
        if let Some(alias) = &self.alias {
            write!(f, " AS {alias}")?;
        }
        for clause in &self.clauses {
            match clause {
                CopyClause::Option(option) => write!(f, " {option}")?,
                CopyClause::PartitionBy(expr) => write!(f, " PARTITION BY {expr}")?,
            }
        }
        Ok(())
    }
}

impl fmt::Display for CreateObject<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CREATE ")?;
        if self.or_replace {
            f.write_str("OR REPLACE ")?;
        }
        if let Some(ref lifetime) = self.lifetime {
            write!(f, "{lifetime} ")?;
        }
        write!(f, "{} ", self.kind)?;
        if self.if_not_exists {
            f.write_str("IF NOT EXISTS ")?;
        }
        write!(f, "{}", self.name)?;
        write_key_values(f, &self.options)
    }
}

fn write_options(
    f: &mut fmt::Formatter<'_>,
    options: &[(Ident<'_>, Expr<'_, Builtin>)],
) -> fmt::Result {
    for (key, value) in options {
        write!(f, " {key}={value}")?;
    }
    Ok(())
}

impl fmt::Display for InsertTarget<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "INTO {}", self.table)?;
        if !self.columns.is_empty() {
            f.write_str(" (")?;
            write!(f, "{}", comma(&self.columns))?;
            f.write_str(")")?;
        }
        if let Some(values) = &self.values {
            f.write_str(" VALUES (")?;
            for (i, value) in values.iter().enumerate() {
                if i > 0 {
                    f.write_str(", ")?;
                }
                write!(f, "{value}")?;
            }
            f.write_str(")")?;
        }
        Ok(())
    }
}

fn write_targets(f: &mut fmt::Formatter<'_>, targets: &[InsertTarget<'_>]) -> fmt::Result {
    for target in targets {
        SpaceOrNewline.fmt(f)?;
        target.fmt(f)?;
    }
    Ok(())
}

impl fmt::Display for MultiTableInsert<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("INSERT")?;
        if self.overwrite {
            f.write_str(" OVERWRITE")?;
        }
        f.write_str(if self.first { " FIRST" } else { " ALL" })?;
        write_targets(f, &self.into)?;
        for when in &self.when {
            SpaceOrNewline.fmt(f)?;
            write!(f, "WHEN {} THEN", when.condition)?;
            for target in &when.into {
                write!(f, " {target}")?;
            }
        }
        if !self.otherwise.is_empty() {
            SpaceOrNewline.fmt(f)?;
            f.write_str("ELSE")?;
            write_targets(f, &self.otherwise)?;
        }
        SpaceOrNewline.fmt(f)?;
        self.source.fmt(f)
    }
}

impl fmt::Display for SnowflakeStatement<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SnowflakeStatement::CreateUser(create) => create.fmt(f),
            SnowflakeStatement::AlterUser(alter) => alter.fmt(f),
            SnowflakeStatement::Use { kind, name } => {
                f.write_str("USE ")?;
                if let Some(kind) = kind {
                    write!(f, "{kind} ")?;
                }
                write!(f, "{name}")
            }
            SnowflakeStatement::UseSecondaryRoles(roles) => {
                f.write_str("USE SECONDARY ROLES ")?;
                match roles {
                    SecondaryRoles::All => f.write_str("ALL"),
                    SecondaryRoles::None => f.write_str("NONE"),
                    SecondaryRoles::List(names) => write!(f, "{}", comma(names)),
                }
            }
            SnowflakeStatement::AlterSessionSet(options) => {
                f.write_str("ALTER SESSION SET")?;
                for (key, value) in options {
                    write!(f, " {key}={value}")?;
                }
                Ok(())
            }
            SnowflakeStatement::AlterSessionUnset(names) => {
                f.write_str("ALTER SESSION UNSET ")?;
                write!(f, "{}", comma(names))
            }
            SnowflakeStatement::MultiTableInsert(insert) => insert.fmt(f),
            SnowflakeStatement::CreateTable(create) => create.fmt(f),
            SnowflakeStatement::CreateDatabase(create) => create.fmt(f),
            SnowflakeStatement::CreateView(create) => create.fmt(f),
            SnowflakeStatement::AlterIcebergTable(alter) => {
                ddl::write_alter_iceberg_table(f, alter)
            }
            SnowflakeStatement::AlterTableKind {
                dynamic,
                if_exists,
                name,
                action,
                subpath,
            } => {
                let kind = if *dynamic { "DYNAMIC" } else { "EXTERNAL" };
                let if_exists = if *if_exists { "IF EXISTS " } else { "" };
                write!(f, "ALTER {kind} TABLE {if_exists}{name} {action}")?;
                match subpath {
                    Some(path) => write!(f, " {}", path),
                    None => Ok(()),
                }
            }
            SnowflakeStatement::Block(block) => block.fmt(f),
            SnowflakeStatement::Raise(None) => f.write_str("RAISE"),
            SnowflakeStatement::Raise(Some(value)) => write!(f, "RAISE {value}"),
            SnowflakeStatement::Declare(declarations) => ddl::write_declare(f, declarations),
            SnowflakeStatement::SetTuple { names, values } => {
                ddl::write_set_tuple(f, names, values)
            }
            SnowflakeStatement::CopyInto(copy) => copy.fmt(f),
            SnowflakeStatement::CreateObject(create) => create.fmt(f),
            SnowflakeStatement::StageCommand {
                remove,
                stage,
                options,
            } => {
                let command = if *remove { "REMOVE" } else { "LIST" };
                write!(f, "{command} {stage}")?;
                write_options(f, options)
            }
            SnowflakeStatement::Put {
                file,
                stage,
                options,
            } => {
                write!(f, "PUT {} {stage}", file)?;
                write_options(f, options)
            }
        }
    }
}

/// `key = value` in a user's properties.
#[derive(Debug, Clone, PartialEq)]
pub struct UserProperty<'a> {
    pub key: Ident<'a>,
    pub value: UserValue<'a>,
}

/// The value of a user property.
#[derive(Debug, Clone, PartialEq)]
pub enum UserValue<'a> {
    Expr(Expr<'a, Builtin>),
    /// `('a', 'b')`
    List(Vec<Expr<'a, Builtin>>),
    /// `(KEY=value, ...)`
    Properties(Vec<UserProperty<'a>>),
}

/// `CREATE [OR REPLACE] USER [IF NOT EXISTS] name [properties] [[WITH] TAG (...)]`
#[derive(Debug, Clone, PartialEq)]
pub struct CreateUser<'a> {
    pub or_replace: bool,
    pub if_not_exists: bool,
    pub name: Ident<'a>,
    pub properties: Vec<UserProperty<'a>>,
    pub with_tag: bool,
    pub tags: Vec<UserProperty<'a>>,
}

/// What `ALTER USER` does.
#[derive(Debug, Clone, PartialEq)]
pub enum AlterUserAction<'a> {
    Rename(Ident<'a>),
    ResetPassword,
    AbortAllQueries,
    AddDelegatedAuthorization {
        role: Ident<'a>,
        integration: Ident<'a>,
    },
    /// With no role, `AUTHORIZATIONS` removes every one.
    RemoveDelegatedAuthorization {
        role: Option<Ident<'a>>,
        integration: Ident<'a>,
    },
    EnrollMfa,
    AddMfaMethodOtp {
        count: Option<Cow<'a, str>>,
    },
    ModifyMfaMethod {
        method: Cow<'a, str>,
        comment: StringLiteral<'a>,
    },
    RemoveMfaMethod(Cow<'a, str>),
    SetDefaultMfaMethod(Cow<'a, str>),
    /// `AUTHENTICATION`, `PASSWORD` or `SESSION`.
    SetPolicy {
        kind: Cow<'a, str>,
        policy: Ident<'a>,
    },
    UnsetPolicy(Cow<'a, str>),
    SetTag(Vec<UserProperty<'a>>),
    UnsetTag(Vec<Ident<'a>>),
    SetProperties(Vec<UserProperty<'a>>),
    UnsetProperties(Vec<Ident<'a>>),
}

/// `ALTER USER [IF EXISTS] name action`
#[derive(Debug, Clone, PartialEq)]
pub struct AlterUser<'a> {
    pub if_exists: bool,
    pub name: Ident<'a>,
    pub action: AlterUserAction<'a>,
}

const USER_POLICY_KINDS: [&str; 3] = ["AUTHENTICATION", "PASSWORD", "SESSION"];
const MFA_METHODS: [&str; 3] = ["PASSKEY", "TOTP", "DUO"];

fn parse_mfa_method<'a, D: Dialect<Ext = Builtin>>(p: &mut Parser<'a, D>) -> PResult<&'a str> {
    let token = p.peek();
    if !MFA_METHODS.iter().any(|method| token.is_word(method)) {
        return Err(p.expected("PASSKEY, TOTP or DUO"));
    }
    p.advance();
    Ok(token.text)
}

fn parse_user_value<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<UserValue<'a>> {
    if !p.eat_op("(") {
        return p.parse_expr().map(UserValue::Expr);
    }
    if p.eat_op(")") {
        return Ok(UserValue::List(Vec::new()));
    }
    if p.at_ident() && p.peek_nth(1).is_op("=") {
        let properties = p.parse_comma_separated(|p| p.nested(parse_user_property))?;
        p.expect_op(")")?;
        return Ok(UserValue::Properties(properties));
    }
    let items = p.parse_comma_separated(Parser::parse_expr)?;
    p.expect_op(")")?;
    Ok(UserValue::List(items))
}

fn parse_user_property<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<UserProperty<'a>> {
    let key = p.parse_ident()?;
    p.expect_op("=")?;
    Ok(UserProperty {
        key,
        value: parse_user_value(p)?,
    })
}

fn at_user_property<D: Dialect<Ext = Builtin>>(p: &Parser<'_, D>) -> bool {
    p.at_ident() && p.peek_nth(1).is_op("=")
}

/// True at `CREATE [OR REPLACE] USER`.
pub fn at_create_user<D: Dialect<Ext = Builtin>>(p: &Parser<'_, D>) -> bool {
    let n = if p.peek_nth(1).is_word("OR") { 3 } else { 1 };
    p.at_keyword("CREATE") && p.peek_nth(n).is_word("USER")
}

pub fn parse_create_user<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<SnowflakeStatement<'a>> {
    p.expect_keyword("CREATE")?;
    let or_replace = grammar::parse_or_replace(p)?;
    p.expect_keyword("USER")?;
    let if_not_exists = grammar::parse_if_not_exists(p)?;
    let name = p.parse_ident()?;
    let mut properties = Vec::new();
    while at_user_property(p) {
        properties.push(parse_user_property(p)?);
    }
    let with_tag = p.at_keyword("WITH") && p.peek_nth(1).is_word("TAG");
    if with_tag {
        p.advance();
    }
    let tags = if p.at_keyword("TAG") && p.peek_nth(1).is_op("(") {
        p.advance();
        p.advance();
        let tags = p.parse_comma_separated(parse_user_property)?;
        p.expect_op(")")?;
        tags
    } else {
        Vec::new()
    };
    Ok(SnowflakeStatement::CreateUser(Box::new(CreateUser {
        or_replace,
        if_not_exists,
        name,
        properties,
        with_tag,
        tags,
    })))
}

/// Parses what follows `ALTER USER name`, or returns `None` if nothing follows.
fn parse_alter_user_action<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<AlterUserAction<'a>> {
    let next = p.peek_nth(1);
    if p.eat_keyword("RENAME") {
        p.expect_keyword("TO")?;
        return p.parse_ident().map(AlterUserAction::Rename);
    }
    if p.at_keyword("RESET") && next.is_word("PASSWORD") {
        p.advance();
        p.advance();
        return Ok(AlterUserAction::ResetPassword);
    }
    if p.eat_keyword("ABORT") {
        p.expect_keyword("ALL")?;
        p.expect_keyword("QUERIES")?;
        return Ok(AlterUserAction::AbortAllQueries);
    }
    if p.at_keyword("ADD") && next.is_word("DELEGATED") {
        p.advance();
        p.advance();
        p.expect_keyword("AUTHORIZATION")?;
        p.expect_keyword("OF")?;
        p.expect_keyword("ROLE")?;
        let role = p.parse_ident()?;
        p.expect_keyword("TO")?;
        p.expect_keyword("SECURITY")?;
        p.expect_keyword("INTEGRATION")?;
        let integration = p.parse_ident()?;
        return Ok(AlterUserAction::AddDelegatedAuthorization { role, integration });
    }
    if p.at_keyword("REMOVE") && next.is_word("DELEGATED") {
        p.advance();
        p.advance();
        let role = if p.eat_keyword("AUTHORIZATIONS") {
            None
        } else {
            p.expect_keyword("AUTHORIZATION")?;
            p.expect_keyword("OF")?;
            p.expect_keyword("ROLE")?;
            Some(p.parse_ident()?)
        };
        p.expect_keyword("FROM")?;
        p.expect_keyword("SECURITY")?;
        p.expect_keyword("INTEGRATION")?;
        let integration = p.parse_ident()?;
        return Ok(AlterUserAction::RemoveDelegatedAuthorization { role, integration });
    }
    if p.at_keyword("ENROLL") && next.is_word("MFA") {
        p.advance();
        p.advance();
        return Ok(AlterUserAction::EnrollMfa);
    }
    if p.at_keyword("ADD") && next.is_word("MFA") {
        p.advance();
        p.advance();
        p.expect_keyword("METHOD")?;
        p.expect_keyword("OTP")?;
        let count = if p.eat_keyword("COUNT") {
            p.expect_op("=")?;
            Some(grammar::parse_number_text(p)?)
        } else {
            None
        };
        return Ok(AlterUserAction::AddMfaMethodOtp {
            count: count.map(Cow::Borrowed),
        });
    }
    if p.at_keyword("MODIFY") && next.is_word("MFA") {
        p.advance();
        p.advance();
        p.expect_keyword("METHOD")?;
        let method = parse_mfa_method(p)?;
        p.expect_keyword("SET")?;
        p.expect_keyword("COMMENT")?;
        let comment = grammar::parse_string(p)?;
        return Ok(AlterUserAction::ModifyMfaMethod {
            method: method.into(),
            comment,
        });
    }
    if p.at_keyword("REMOVE") && next.is_word("MFA") {
        p.advance();
        p.advance();
        p.expect_keyword("METHOD")?;
        return parse_mfa_method(p).map(|v| AlterUserAction::RemoveMfaMethod(v.into()));
    }
    let unset = p.at_keyword("UNSET");
    if !unset && !p.at_keyword("SET") {
        return Err(p.expected("an ALTER USER action"));
    }
    p.advance();
    if let Some(kind) = USER_POLICY_KINDS
        .into_iter()
        .find(|kind| p.at_keyword(kind) && p.peek_nth(1).is_word("POLICY"))
    {
        p.advance();
        p.advance();
        return Ok(if unset {
            AlterUserAction::UnsetPolicy(kind.into())
        } else {
            AlterUserAction::SetPolicy {
                kind: kind.into(),
                policy: p.parse_ident()?,
            }
        });
    }
    if !unset && p.eat_keyword("DEFAULT_MFA_METHOD") {
        return parse_mfa_method(p).map(|v| AlterUserAction::SetDefaultMfaMethod(v.into()));
    }
    if p.eat_keyword("TAG") {
        return Ok(if unset {
            AlterUserAction::UnsetTag(p.parse_comma_separated(Parser::parse_ident)?)
        } else {
            AlterUserAction::SetTag(p.parse_comma_separated(parse_user_property)?)
        });
    }
    if unset {
        return p
            .parse_comma_separated(Parser::parse_ident)
            .map(AlterUserAction::UnsetProperties);
    }
    p.parse_comma_separated(parse_user_property)
        .map(AlterUserAction::SetProperties)
}

pub fn at_alter_user<D: Dialect<Ext = Builtin>>(p: &Parser<'_, D>) -> bool {
    p.at_keyword("ALTER") && p.peek_nth(1).is_word("USER")
}

pub fn parse_alter_user<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<SnowflakeStatement<'a>> {
    p.advance();
    p.advance();
    let if_exists = p.at_keyword("IF") && p.peek_nth(1).is_word("EXISTS");
    if if_exists {
        p.advance();
        p.advance();
    }
    let name = p.parse_ident()?;
    let action = parse_alter_user_action(p)?;
    Ok(SnowflakeStatement::AlterUser(Box::new(AlterUser {
        if_exists,
        name,
        action,
    })))
}

impl fmt::Display for UserValue<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Expr(expr) => expr.fmt(f),
            Self::List(items) => write!(f, "({})", comma(items)),
            Self::Properties(properties) => write!(f, "({})", comma(properties)),
        }
    }
}

impl fmt::Display for UserProperty<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}={}", self.key, self.value)
    }
}

impl fmt::Display for CreateUser<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CREATE ")?;
        if self.or_replace {
            f.write_str("OR REPLACE ")?;
        }
        f.write_str("USER ")?;
        write_if_not_exists(f, self.if_not_exists)?;
        self.name.fmt(f)?;
        for property in &self.properties {
            write!(f, " {property}")?;
        }
        if !self.tags.is_empty() {
            let with = if self.with_tag { "WITH " } else { "" };
            write!(f, " {with}TAG ({})", comma(&self.tags))?;
        }
        Ok(())
    }
}

impl fmt::Display for AlterUser<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ALTER USER ")?;
        if self.if_exists {
            f.write_str("IF EXISTS ")?;
        }
        write!(f, "{} ", self.name)?;
        match &self.action {
            AlterUserAction::Rename(name) => write!(f, "RENAME TO {name}"),
            AlterUserAction::ResetPassword => f.write_str("RESET PASSWORD"),
            AlterUserAction::AbortAllQueries => f.write_str("ABORT ALL QUERIES"),
            AlterUserAction::AddDelegatedAuthorization { role, integration } => write!(
                f,
                "ADD DELEGATED AUTHORIZATION OF ROLE {role} TO SECURITY INTEGRATION {integration}"
            ),
            AlterUserAction::RemoveDelegatedAuthorization {
                role: Some(role),
                integration,
            } => write!(
                f,
                "REMOVE DELEGATED AUTHORIZATION OF ROLE {role} FROM SECURITY INTEGRATION {integration}"
            ),
            AlterUserAction::RemoveDelegatedAuthorization {
                role: None,
                integration,
            } => write!(
                f,
                "REMOVE DELEGATED AUTHORIZATIONS FROM SECURITY INTEGRATION {integration}"
            ),
            AlterUserAction::EnrollMfa => f.write_str("ENROLL MFA"),
            AlterUserAction::AddMfaMethodOtp { count } => {
                f.write_str("ADD MFA METHOD OTP")?;
                match count {
                    Some(count) => write!(f, " COUNT = {count}"),
                    None => Ok(()),
                }
            }
            AlterUserAction::ModifyMfaMethod { method, comment } => {
                write!(f, "MODIFY MFA METHOD {method} SET COMMENT {}", comment)
            }
            AlterUserAction::RemoveMfaMethod(method) => write!(f, "REMOVE MFA METHOD {method}"),
            AlterUserAction::SetDefaultMfaMethod(method) => {
                write!(f, "SET DEFAULT_MFA_METHOD {method}")
            }
            AlterUserAction::SetPolicy { kind, policy } => {
                write!(f, "SET {kind} POLICY {policy}")
            }
            AlterUserAction::UnsetPolicy(kind) => write!(f, "UNSET {kind} POLICY"),
            AlterUserAction::SetTag(tags) => write!(f, "SET TAG {}", comma(tags)),
            AlterUserAction::UnsetTag(tags) => write!(f, "UNSET TAG {}", comma(tags)),
            AlterUserAction::SetProperties(properties) => {
                write!(f, "SET {}", comma(properties))
            }
            AlterUserAction::UnsetProperties(names) => write!(f, "UNSET {}", comma(names)),
        }
    }
}

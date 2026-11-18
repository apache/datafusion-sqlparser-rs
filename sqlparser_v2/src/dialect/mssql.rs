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

//! Microsoft SQL Server (T-SQL).

use std::borrow::Cow;
use std::fmt::{self, Display as _};

use crate::PResult;
use crate::ast::{
    BinaryOp, ColumnDef, ColumnOption, CreateFlags, CreateSchema, CreateTable, CreateView,
    DataType, DataTypeSuffix, DropBehavior, Expr, ExprKind, GrantObjects, Grantee, Ident,
    IdentityParams, InsertSource, IsTest, Join, JoinConstraint, JoinKind, ObjectName, ObjectType,
    OutputClause, Privileges, Query, RoutineArg, Select, SelectInto, SelectItem, SelectItemKind,
    SetQuantifier, Span, Statement, StatementKind, StringLiteral, TableFactor, TableRef,
    TableWithJoins, TimeTravel,
};
use crate::dialect::builtin::{
    BuiltinExpr, BuiltinQueryClauses, BuiltinQueryTail, BuiltinSelect, BuiltinStatement,
    BuiltinTableFactor,
};
use crate::dialect::{Builtin, Dialect};
use crate::display::{Indent, SpaceOrNewline, comma};
use crate::grammar::{self, parse_object_name, prec};
use crate::lexer::{self, LexResult, Lexeme, Token, TokenKind, quoted};
use crate::parser::{InfixRule, Parser};

#[derive(Debug, Clone, PartialEq)]
pub struct MsSql;

/// Statements only Microsoft SQL Server has.
#[derive(Debug, Clone, PartialEq)]
pub enum MsSqlStatement<'a> {
    /// `BEGIN ... END` used as a statement.
    Block(Vec<Statement<'a, Builtin>>),
    /// `BEGIN TRY`, `END TRY`, `BEGIN CATCH` or `END CATCH`.
    TryCatch(Cow<'a, str>),
    If(Box<MsSqlIf<'a>>),
    While(Box<MsSqlWhile<'a>>),
    /// `DECLARE item, ...`
    Declare(Vec<MsSqlDeclare<'a>>),
    Print(Box<Expr<'a, Builtin>>),
    RaisError(Box<MsSqlRaisError<'a>>),
    /// `THROW [number, message, state]`
    Throw(Option<Box<[Expr<'a, Builtin>; 3]>>),
    /// `WAITFOR {DELAY | TIME} expr`
    WaitFor {
        delay: bool,
        expr: Box<Expr<'a, Builtin>>,
    },
    Return(Option<Box<Expr<'a, Builtin>>>),
    Open(Ident<'a>),
    /// `CLOSE name`, or `CLOSE ALL` when there is no name.
    Close(Option<Ident<'a>>),
    Fetch(Box<MsSqlFetch<'a>>),
    Deallocate {
        prepare: bool,
        name: Ident<'a>,
    },
    Execute(Box<MsSqlExecute<'a>>),
    CreateProcedure(Box<MsSqlCreateProcedure<'a>>),
    CreateFunction(Box<MsSqlCreateFunction<'a>>),
    CreateTrigger(Box<MsSqlCreateTrigger<'a>>),
    CreateRole(Box<MsSqlCreateRole<'a>>),
    AlterRole(Box<MsSqlAlterRole<'a>>),
    Deny(Box<MsSqlDeny<'a>>),
    /// `SET` without an assignment operator, such as `SET NOCOUNT ON`.
    SessionParam(Box<MsSqlSessionParam<'a>>),
    /// `CREATE TABLE ... WITH (...)`
    CreateTable(Box<MsSqlCreateTable<'a>>),
    CreateOrAlterView(Box<CreateView<'a, Builtin>>),
}

/// The statements in a `BEGIN ... END` block or a bare sequence.
#[derive(Debug, Clone, PartialEq)]
pub enum MsSqlBody<'a> {
    Statements(Vec<Statement<'a, Builtin>>),
    Block(Vec<Statement<'a, Builtin>>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct MsSqlIf<'a> {
    pub condition: Expr<'a, Builtin>,
    pub then_branch: MsSqlBody<'a>,
    pub else_branch: Option<MsSqlBody<'a>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MsSqlWhile<'a> {
    pub condition: Expr<'a, Builtin>,
    pub body: MsSqlBody<'a>,
}

/// One item of `DECLARE`.
#[derive(Debug, Clone, PartialEq)]
pub struct MsSqlDeclare<'a> {
    pub name: Ident<'a>,
    pub cursor: bool,
    pub for_query: Option<Box<Query<'a, Builtin>>>,
    pub data_type: Option<DataType<'a>>,
    pub assignment: Option<Expr<'a, Builtin>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MsSqlRaisError<'a> {
    pub message: Expr<'a, Builtin>,
    pub severity: Expr<'a, Builtin>,
    pub state: Expr<'a, Builtin>,
    pub arguments: Vec<Expr<'a, Builtin>>,
    pub options: Vec<Cow<'a, str>>,
}

/// `FETCH [direction] {FROM | IN} name [INTO target]`
#[derive(Debug, Clone, PartialEq)]
pub struct MsSqlFetch<'a> {
    /// `NEXT`, `FORWARD ALL` and so on; empty for a bare count.
    pub direction: Cow<'a, str>,
    pub count: Option<Cow<'a, str>>,
    pub from: bool,
    pub name: Ident<'a>,
    pub into: Option<ObjectName<'a>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MsSqlExecute<'a> {
    pub name: Option<ObjectName<'a>>,
    pub parameters: Vec<Expr<'a, Builtin>>,
    pub has_parentheses: bool,
    pub into: Vec<Ident<'a>>,
    pub using: Vec<(Expr<'a, Builtin>, Option<Ident<'a>>)>,
    pub output: bool,
    pub default: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MsSqlCreateProcedure<'a> {
    pub or_alter: bool,
    pub name: ObjectName<'a>,
    /// `None` when the name has no parentheses.
    pub params: Option<Vec<RoutineArg<'a, Builtin>>>,
    pub language: Option<Ident<'a>>,
    pub body: MsSqlBody<'a>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MsSqlCreateFunction<'a> {
    pub or_alter: bool,
    pub name: ObjectName<'a>,
    pub params: Vec<RoutineArg<'a, Builtin>>,
    pub returns: MsSqlReturns<'a>,
    pub body: MsSqlFunctionBody<'a>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum MsSqlReturns<'a> {
    Type(DataType<'a>),
    /// `TABLE (...)`, or `@name TABLE (...)`.
    Table {
        name: Option<Ident<'a>>,
        columns: Vec<ColumnDef<'a, Builtin>>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum MsSqlFunctionBody<'a> {
    Block(Vec<Statement<'a, Builtin>>),
    /// `AS RETURN expr`
    ReturnExpr(Expr<'a, Builtin>),
    /// `AS RETURN SELECT ...`
    ReturnSelect(Box<Select<'a, Builtin>>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct MsSqlCreateTrigger<'a> {
    pub or_alter: bool,
    pub name: ObjectName<'a>,
    pub table: ObjectName<'a>,
    /// `FOR`, `AFTER`, `BEFORE` or `INSTEAD OF`.
    pub period: Cow<'a, str>,
    pub events: Vec<MsSqlTriggerEvent<'a>>,
    pub body: MsSqlBody<'a>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum MsSqlTriggerEvent<'a> {
    Insert,
    Update(Vec<Ident<'a>>),
    Delete,
    Truncate,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MsSqlCreateRole<'a> {
    pub if_not_exists: bool,
    pub names: Vec<ObjectName<'a>>,
    pub authorization: Option<ObjectName<'a>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MsSqlAlterRole<'a> {
    pub name: Ident<'a>,
    pub operation: MsSqlAlterRoleOp<'a>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum MsSqlAlterRoleOp<'a> {
    AddMember(Ident<'a>),
    DropMember(Ident<'a>),
    Rename(Ident<'a>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct MsSqlDeny<'a> {
    pub privileges: Privileges<'a>,
    pub objects: GrantObjects<'a>,
    pub grantees: Vec<Grantee<'a>>,
    pub behavior: Option<DropBehavior>,
    pub as_grantor: Option<Ident<'a>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum MsSqlSessionParam<'a> {
    /// `SET name, ... value`
    Generic {
        names: Vec<Cow<'a, str>>,
        value: Expr<'a, Builtin>,
    },
    /// `SET STATISTICS topic {ON | OFF}`
    Statistics { topic: Cow<'a, str>, on: bool },
    /// `SET IDENTITY_INSERT table {ON | OFF}`
    IdentityInsert { table: ObjectName<'a>, on: bool },
    /// `SET OFFSETS keyword, ... {ON | OFF}`
    Offsets {
        keywords: Vec<Cow<'a, str>>,
        on: bool,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct MsSqlCreateTable<'a> {
    pub table: CreateTable<'a, Builtin>,
    pub options: Vec<MsSqlTableOption<'a>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum MsSqlTableOption<'a> {
    KeyValue {
        key: ObjectName<'a>,
        value: Expr<'a, Builtin>,
    },
    Heap,
    ClusteredColumnstore,
    ClusteredColumnstoreOrder(Vec<Ident<'a>>),
    /// `CLUSTERED INDEX (column [ASC | DESC], ...)`
    ClusteredIndex(Vec<(Ident<'a>, Option<bool>)>),
    /// `PARTITION (column RANGE [LEFT | RIGHT] FOR VALUES (...))`
    Partition {
        column: Ident<'a>,
        direction: Option<Cow<'a, str>>,
        values: Vec<Expr<'a, Builtin>>,
    },
}

/// Expressions only Microsoft SQL Server has.
#[derive(Debug, Clone, PartialEq)]
pub enum MsSqlExpr<'a> {
    /// `[TRY_]CONVERT(type, expr [, style...])`
    Convert(Box<MsSqlConvert<'a>>),
    /// `JSON_OBJECT(...)` or `JSON_ARRAY(...)`
    JsonCall(Box<MsSqlJsonCall<'a>>),
    /// `expr.name(args)`
    Method(Box<MsSqlMethod<'a>>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct MsSqlMethod<'a> {
    pub expr: Expr<'a, Builtin>,
    pub name: Ident<'a>,
    pub args: Vec<Expr<'a, Builtin>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MsSqlConvert<'a> {
    pub is_try: bool,
    pub data_type: DataType<'a>,
    pub expr: Expr<'a, Builtin>,
    pub styles: Vec<Expr<'a, Builtin>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MsSqlJsonCall<'a> {
    pub name: Ident<'a>,
    pub args: Vec<MsSqlJsonArg<'a>>,
    /// `ABSENT ON NULL` or `NULL ON NULL`.
    pub null_clause: Option<Cow<'a, str>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum MsSqlJsonArg<'a> {
    Value(Expr<'a, Builtin>),
    /// `key : value`
    KeyValue(Expr<'a, Builtin>, Expr<'a, Builtin>),
}

/// `TOP n` or `TOP (expr)`.
#[derive(Debug, Clone, PartialEq)]
pub struct MsSqlTop<'a> {
    pub quantity: Expr<'a, Builtin>,
    pub parenthesized: bool,
    pub percent: bool,
    pub with_ties: bool,
}

/// A table factor only Microsoft SQL Server has.
#[derive(Debug, Clone, PartialEq)]
pub enum MsSqlTableFactor<'a> {
    /// A table followed by `WITH (hint, ...)`, including any alias.
    Hinted(Box<MsSqlHintedTable<'a>>),
    OpenJson(Box<MsSqlOpenJson<'a>>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct MsSqlHintedTable<'a> {
    pub table: TableRef<'a, Builtin>,
    pub hints: Vec<Expr<'a, Builtin>>,
}

/// `OPENJSON(expr [, path]) [WITH (column, ...)]`
#[derive(Debug, Clone, PartialEq)]
pub struct MsSqlOpenJson<'a> {
    pub expr: Expr<'a, Builtin>,
    pub path: Option<Expr<'a, Builtin>>,
    pub columns: Vec<MsSqlOpenJsonColumn<'a>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MsSqlOpenJsonColumn<'a> {
    pub name: Ident<'a>,
    pub data_type: DataType<'a>,
    pub path: Option<StringLiteral<'a>>,
    pub as_json: bool,
}

/// `FOR XML`, `FOR JSON` or `FOR BROWSE` at the end of a query.
#[derive(Debug, Clone, PartialEq)]
pub enum MsSqlForClause<'a> {
    Browse,
    Xml(MsSqlForXml<'a>),
    Json(MsSqlForJson<'a>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct MsSqlForXml<'a> {
    /// `RAW`, `AUTO`, `EXPLICIT` or `PATH`.
    pub mode: Cow<'a, str>,
    pub mode_name: Option<StringLiteral<'a>>,
    pub elements: bool,
    pub binary_base64: bool,
    pub root: Option<StringLiteral<'a>>,
    pub type_directive: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MsSqlForJson<'a> {
    /// `AUTO` or `PATH`.
    pub mode: Cow<'a, str>,
    pub root: Option<StringLiteral<'a>>,
    pub include_null_values: bool,
    pub without_array_wrapper: bool,
}

/// Words that can never be an alias, even after `AS`.
const NON_ALIAS_KEYWORDS: [&str; 18] = [
    "IF",
    "ELSE",
    "DECLARE",
    "EXEC",
    "EXECUTE",
    "INSERT",
    "UPDATE",
    "DELETE",
    "DROP",
    "CREATE",
    "ALTER",
    "TRUNCATE",
    "PRINT",
    "WHILE",
    "RETURN",
    "THROW",
    "RAISERROR",
    "MERGE",
];

#[inline]
fn is_name_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'@' | b'$' | b'#') || !byte.is_ascii()
}

/// Lexes a word that can hold `@`, `#` or `$`.
fn lex_word(rest: &str) -> Option<LexResult> {
    let bytes = rest.as_bytes();
    let first = bytes[0];
    if !first.is_ascii_alphabetic() && !matches!(first, b'_' | b'@' | b'#') && first.is_ascii() {
        return None;
    }
    if matches!(first, b'x' | b'X') && bytes.get(1) == Some(&b'\'') {
        return None;
    }
    let len = bytes.iter().take_while(|&&b| is_name_byte(b)).count();
    // A name needs more than `@` signs, or `a <@@> b` would read as columns.
    if first == b'@' && bytes[..len].iter().all(|&b| b == b'@') {
        return Some(Err("expected a name after @"));
    }
    Some(Ok(Lexeme {
        kind: TokenKind::Word,
        len,
    }))
}

/// Lexes `[name]`, in which `]]` escapes a closing bracket.
fn bracketed(rest: &str) -> LexResult {
    let bytes = rest.as_bytes();
    let mut i = 1;
    while i < bytes.len() {
        if bytes[i] == b']' {
            if bytes.get(i + 1) == Some(&b']') {
                i += 2;
                continue;
            }
            return Ok(Lexeme {
                kind: TokenKind::QuotedIdent,
                len: i + 1,
            });
        }
        i += 1;
    }
    Err("unterminated [ identifier")
}

/// Lexes a money literal: `$` followed by digits and a fraction.
fn lex_money(rest: &str) -> Option<LexResult> {
    let bytes = rest.as_bytes();
    let digits = bytes[1..].iter().take_while(|b| b.is_ascii_digit()).count();
    if digits == 0 || bytes.get(1 + digits) != Some(&b'.') {
        return None;
    }
    let fraction = bytes[digits + 2..]
        .iter()
        .take_while(|b| b.is_ascii_digit())
        .count();
    Some(Ok(Lexeme {
        kind: TokenKind::Placeholder,
        len: 1 + digits + 1 + fraction,
    }))
}

/// Treats non-ASCII whitespace, which the core lexer does not skip, as a comment.
#[inline]
fn lex_comment(rest: &str) -> Option<Result<usize, &'static str>> {
    if !rest.as_bytes()[0].is_ascii() {
        let ch = rest.chars().next()?;
        if ch.is_whitespace() {
            return Some(Ok(ch.len_utf8()));
        }
    }
    lexer::lex_core_comment(rest)
}

#[inline]
pub fn lex_token(rest: &str) -> Option<LexResult> {
    match rest.as_bytes() {
        [b'[', ..] => Some(bracketed(rest)),
        [b'N' | b'n', b'\'', ..] => {
            Some(quoted(&rest[1..], TokenKind::String).map(|lexeme| Lexeme {
                kind: TokenKind::String,
                len: lexeme.len + 1,
            }))
        }
        [b'{' | b'}', ..] => Some(Ok(Lexeme {
            kind: TokenKind::Op,
            len: 1,
        })),
        [b'$', ..] => lex_money(rest),
        // A `:` is punctuation here, since JSON functions separate keys and
        // values with it and a key can be written without a space before it.
        [b':', ..] => Some(Ok(Lexeme {
            kind: TokenKind::Op,
            len: 1,
        })),
        // `=>` has no meaning in T-SQL, so it stays two tokens.
        [b'=', b'>', ..] => Some(Ok(Lexeme {
            kind: TokenKind::Op,
            len: 1,
        })),
        _ => lex_word(rest),
    }
}

/// Builds a string literal, removing a leading `N`.
pub fn string_literal(token: Token<'_>) -> StringLiteral<'_> {
    grammar::national_string_literal(token)
}

fn is_any(token: Token<'_>, words: &[&str]) -> bool {
    token.kind == TokenKind::Word && words.iter().any(|word| token.is_word(word))
}

fn at_any<'a, D: Dialect>(p: &Parser<'a, D>, words: &[&str]) -> bool {
    is_any(p.peek(), words)
}

fn mssql_statement<'a>(statement: MsSqlStatement<'a>) -> Statement<'a, Builtin> {
    StatementKind::Ext(BuiltinStatement::MsSql(statement)).into()
}

/// Parses statements until `END` or the end of input, with an optional `;`
/// after each one.
fn parse_loose_list<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Vec<Statement<'a, Builtin>>> {
    let mut statements = Vec::new();
    while !p.at_eof() && !p.at_keyword("END") {
        statements.push(p.nested(Parser::parse_statement)?);
        while p.eat_op(";") {}
    }
    Ok(statements)
}

/// Parses statements until `END` or the end of input, each followed by `;`.
fn parse_strict_list<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Vec<Statement<'a, Builtin>>> {
    let mut statements = Vec::new();
    while !p.at_eof() && !p.at_keyword("END") {
        statements.push(p.nested(Parser::parse_statement)?);
        p.expect_op(";")?;
    }
    Ok(statements)
}

/// Parses a `BEGIN ... END` block, or else a sequence of statements.
fn parse_body<'a, D: Dialect<Ext = Builtin>>(p: &mut Parser<'a, D>) -> PResult<MsSqlBody<'a>> {
    if p.eat_keyword("BEGIN") {
        let statements = parse_strict_list(p)?;
        p.expect_keyword("END")?;
        Ok(MsSqlBody::Block(statements))
    } else {
        parse_strict_list(p).map(MsSqlBody::Statements)
    }
}

/// Parses one branch of `IF`: a loose `BEGIN ... END` block or one statement.
fn parse_if_branch<'a, D: Dialect<Ext = Builtin>>(p: &mut Parser<'a, D>) -> PResult<MsSqlBody<'a>> {
    if p.eat_keyword("BEGIN") {
        let statements = parse_loose_list(p)?;
        p.expect_keyword("END")?;
        Ok(MsSqlBody::Block(statements))
    } else {
        let statement = p.nested(Parser::parse_statement)?;
        Ok(MsSqlBody::Statements(vec![statement]))
    }
}

fn parse_if<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Statement<'a, Builtin>> {
    p.expect_keyword("IF")?;
    let condition = p.parse_expr()?;
    let then_branch = parse_if_branch(p)?;
    let mut semicolons = 0;
    while p.peek_nth(semicolons).is_op(";") {
        semicolons += 1;
    }
    let else_branch = if p.peek_nth(semicolons).is_word("ELSE") {
        for _ in 0..=semicolons {
            p.advance();
        }
        Some(parse_if_branch(p)?)
    } else {
        None
    };
    Ok(mssql_statement(MsSqlStatement::If(Box::new(MsSqlIf {
        condition,
        then_branch,
        else_branch,
    }))))
}

fn parse_while<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Statement<'a, Builtin>> {
    p.expect_keyword("WHILE")?;
    let condition = p.parse_expr()?;
    let body = parse_body(p)?;
    Ok(mssql_statement(MsSqlStatement::While(Box::new(
        MsSqlWhile { condition, body },
    ))))
}

/// True at a `BEGIN` that starts a transaction or a `TRY`/`CATCH` marker
/// rather than a block.
fn at_transaction_begin<'a, D: Dialect>(p: &Parser<'a, D>) -> bool {
    let next = p.peek_nth(1);
    next.is_op(";")
        || next.kind == TokenKind::Eof
        || is_any(
            next,
            &[
                "TRANSACTION",
                "WORK",
                "TRAN",
                "TRY",
                "CATCH",
                "DEFERRED",
                "IMMEDIATE",
                "EXCLUSIVE",
            ],
        )
}

fn parse_declare_item<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<MsSqlDeclare<'a>> {
    let name = p.parse_ident()?;
    if !name.text.starts_with('@') && !p.at_keyword("CURSOR") {
        return Err(p.expected("a variable name starting with @, or CURSOR"));
    }
    let cursor = p.eat_keyword("CURSOR");
    let data_type = if cursor {
        None
    } else {
        p.eat_keyword("AS");
        Some(D::parse_data_type(p)?)
    };
    let for_query = if p.eat_keyword("FOR") {
        Some(Box::new(D::parse_query(p)?))
    } else {
        None
    };
    let assignment = if for_query.is_none() && p.eat_op("=") {
        Some(p.parse_expr()?)
    } else {
        None
    };
    Ok(MsSqlDeclare {
        name,
        cursor,
        for_query,
        data_type,
        assignment,
    })
}

fn parse_raiserror<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<MsSqlRaisError<'a>> {
    p.advance();
    p.expect_op("(")?;
    let message = p.parse_expr()?;
    p.expect_op(",")?;
    let severity = p.parse_expr()?;
    p.expect_op(",")?;
    let state = p.parse_expr()?;
    let mut arguments = Vec::new();
    while p.eat_op(",") {
        arguments.push(p.parse_expr()?);
    }
    p.expect_op(")")?;
    let options = if p.eat_keyword("WITH") {
        p.parse_comma_separated(|p| {
            for option in ["LOG", "NOWAIT", "SETERROR"] {
                if p.eat_keyword(option) {
                    return Ok(option);
                }
            }
            Err(p.expected("LOG, NOWAIT or SETERROR"))
        })?
    } else {
        Vec::new()
    };
    Ok(MsSqlRaisError {
        message,
        severity,
        state,
        arguments,
        options: options.into_iter().map(Cow::Borrowed).collect(),
    })
}

/// True at the end of a statement that takes no operand, where a statement
/// list inside `BEGIN ... END` may continue without a semicolon.
fn at_operand_end<'a, D: Dialect>(p: &Parser<'a, D>) -> bool {
    p.at_eof() || p.at_op(";") || p.at_keyword("END") || p.at_keyword("ELSE")
}

fn parse_throw<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Statement<'a, Builtin>> {
    p.advance();
    let args = if at_operand_end(p) {
        None
    } else {
        let number = p.parse_expr()?;
        p.expect_op(",")?;
        let message = p.parse_expr()?;
        p.expect_op(",")?;
        let state = p.parse_expr()?;
        Some(Box::new([number, message, state]))
    };
    Ok(mssql_statement(MsSqlStatement::Throw(args)))
}

fn parse_waitfor<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Statement<'a, Builtin>> {
    p.advance();
    let delay = if p.eat_keyword("DELAY") {
        true
    } else {
        p.expect_keyword("TIME")?;
        false
    };
    let expr = Box::new(p.parse_expr()?);
    Ok(mssql_statement(MsSqlStatement::WaitFor { delay, expr }))
}

fn parse_return<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Statement<'a, Builtin>> {
    p.advance();
    let value = if at_operand_end(p) {
        None
    } else {
        Some(Box::new(p.parse_expr()?))
    };
    Ok(mssql_statement(MsSqlStatement::Return(value)))
}

fn parse_fetch<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Statement<'a, Builtin>> {
    p.advance();
    let mut count = None;
    let direction = if p.eat_keyword("NEXT") {
        "NEXT"
    } else if p.eat_keyword("PRIOR") {
        "PRIOR"
    } else if p.eat_keyword("FIRST") {
        "FIRST"
    } else if p.eat_keyword("LAST") {
        "LAST"
    } else if p.eat_keyword("ABSOLUTE") {
        count = Some(grammar::parse_number_text(p)?);
        "ABSOLUTE"
    } else if p.eat_keyword("RELATIVE") {
        count = Some(grammar::parse_number_text(p)?);
        "RELATIVE"
    } else if p.eat_keyword("FORWARD") {
        if p.eat_keyword("ALL") {
            "FORWARD ALL"
        } else {
            count = Some(grammar::parse_number_text(p)?);
            "FORWARD"
        }
    } else if p.eat_keyword("BACKWARD") {
        if p.eat_keyword("ALL") {
            "BACKWARD ALL"
        } else {
            count = Some(grammar::parse_number_text(p)?);
            "BACKWARD"
        }
    } else if p.eat_keyword("ALL") {
        "ALL"
    } else {
        count = Some(grammar::parse_number_text(p)?);
        ""
    };
    let from = if p.eat_keyword("FROM") {
        true
    } else {
        p.expect_keyword("IN")?;
        false
    };
    let name = p.parse_ident()?;
    let into = if p.eat_keyword("INTO") {
        Some(parse_object_name(p)?)
    } else {
        None
    };
    Ok(mssql_statement(MsSqlStatement::Fetch(Box::new(
        MsSqlFetch {
            direction: direction.into(),
            count: count.map(Cow::Borrowed),
            from,
            name,
            into,
        },
    ))))
}

fn parse_execute<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<MsSqlExecute<'a>> {
    p.advance();
    let name = if p.at_op("(") || p.peek().kind == TokenKind::String {
        None
    } else {
        Some(parse_object_name(p)?)
    };
    let has_parentheses = p.eat_op("(");
    let parameters = if at_execute_end(p, has_parentheses) {
        Vec::new()
    } else {
        p.parse_comma_separated(Parser::parse_expr)?
    };
    if has_parentheses {
        p.expect_op(")")?;
    }
    let into = if p.eat_keyword("INTO") {
        p.parse_comma_separated(Parser::parse_ident)?
    } else {
        Vec::new()
    };
    let using = if p.eat_keyword("USING") {
        p.parse_comma_separated(|p| {
            let expr = p.parse_expr()?;
            let alias = grammar::parse_optional_alias(p)?;
            Ok((expr, alias))
        })?
    } else {
        Vec::new()
    };
    let output = p.eat_keyword("OUTPUT");
    let default = p.eat_keyword("DEFAULT");
    Ok(MsSqlExecute {
        name,
        parameters,
        has_parentheses,
        into,
        using,
        output,
        default,
    })
}

fn at_execute_end<'a, D: Dialect>(p: &Parser<'a, D>, has_parentheses: bool) -> bool {
    if has_parentheses {
        return p.at_op(")");
    }
    at_operand_end(p) || at_any(p, &["USING", "OUTPUT", "DEFAULT"])
}

fn parse_deny<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Statement<'a, Builtin>> {
    p.advance();
    let privileges = grammar::parse_privileges(p)?;
    p.expect_keyword("ON")?;
    let objects = grammar::parse_grant_objects(p)?;
    p.expect_keyword("TO")?;
    let grantees = D::parse_grantees(p)?;
    let behavior = if p.eat_keyword("CASCADE") {
        Some(DropBehavior::Cascade)
    } else if p.eat_keyword("RESTRICT") {
        Some(DropBehavior::Restrict)
    } else {
        None
    };
    let as_grantor = if p.eat_keyword("AS") {
        Some(p.parse_ident()?)
    } else {
        None
    };
    Ok(mssql_statement(MsSqlStatement::Deny(Box::new(MsSqlDeny {
        privileges,
        objects,
        grantees,
        behavior,
        as_grantor,
    }))))
}

fn parse_alter_role<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Statement<'a, Builtin>> {
    p.advance();
    p.advance();
    let name = p.parse_ident()?;
    let operation = if p.eat_keyword("ADD") {
        p.expect_keyword("MEMBER")?;
        MsSqlAlterRoleOp::AddMember(p.parse_ident()?)
    } else if p.eat_keyword("DROP") {
        p.expect_keyword("MEMBER")?;
        MsSqlAlterRoleOp::DropMember(p.parse_ident()?)
    } else if p.eat_keyword("RENAME") {
        // The form this crate renders, as the original does.
        p.expect_keyword("TO")?;
        MsSqlAlterRoleOp::Rename(p.parse_ident()?)
    } else {
        p.expect_keyword("WITH")?;
        p.expect_keyword("NAME")?;
        p.expect_op("=")?;
        MsSqlAlterRoleOp::Rename(p.parse_ident()?)
    };
    Ok(mssql_statement(MsSqlStatement::AlterRole(Box::new(
        MsSqlAlterRole { name, operation },
    ))))
}

/// True at `SET` when what follows is not a variable assignment or one of the
/// core's special forms.
fn at_session_param<'a, D: Dialect>(p: &Parser<'a, D>) -> bool {
    if grammar::at_special_set(p) || is_any(p.peek_nth(1), &["LOCAL", "SESSION", "GLOBAL"]) {
        return false;
    }
    let mut n = 1;
    if !matches!(p.peek_nth(n).kind, TokenKind::Word | TokenKind::QuotedIdent) {
        return false;
    }
    while p.peek_nth(n + 1).is_op(".") {
        n += 2;
    }
    let after = p.peek_nth(n + 1);
    !(after.is_op("=") || after.is_word("TO"))
}

/// Reads a word, in any form, as text for `SET name, ... value`.
fn parse_session_word<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<&'a str> {
    let token = p.peek();
    if !matches!(token.kind, TokenKind::Word | TokenKind::QuotedIdent) {
        return Err(p.expected("a session parameter name"));
    }
    p.advance();
    Ok(token.text)
}

fn parse_on_off<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<bool> {
    if p.eat_keyword("ON") {
        Ok(true)
    } else {
        p.expect_keyword("OFF")?;
        Ok(false)
    }
}

/// Parses the value of a `SET` session parameter. A word on its own, such as
/// `ON` or `dmy`, is taken as written.
fn parse_session_value<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Expr<'a, Builtin>> {
    let token = p.peek();
    let next = p.peek_nth(1);
    if token.kind == TokenKind::Word && !next.is_op("(") && !next.is_op(".") {
        p.advance();
        return Ok(Expr::new(
            ExprKind::Ident(Ident {
                text: token.text.into(),
                quote: None,
                span: token.span,
            }),
            token.span,
        ));
    }
    p.parse_expr()
}

fn parse_session_param<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Statement<'a, Builtin>> {
    p.expect_keyword("SET")?;
    let param = if p.eat_keyword("STATISTICS") {
        let topic = ["IO", "PROFILE", "TIME", "XML"]
            .into_iter()
            .find(|topic| p.eat_keyword(topic))
            .ok_or_else(|| p.expected("IO, PROFILE, TIME or XML"))?;
        MsSqlSessionParam::Statistics {
            topic: topic.into(),
            on: parse_on_off(p)?,
        }
    } else if p.eat_keyword("IDENTITY_INSERT") {
        let table = parse_object_name(p)?;
        MsSqlSessionParam::IdentityInsert {
            table,
            on: parse_on_off(p)?,
        }
    } else if p.eat_keyword("OFFSETS") {
        let keywords = p.parse_comma_separated(parse_session_word)?;
        MsSqlSessionParam::Offsets {
            keywords: keywords.into_iter().map(Cow::Borrowed).collect(),
            on: parse_on_off(p)?,
        }
    } else {
        let names = p.parse_comma_separated(parse_session_word)?;
        let value = parse_session_value(p)?;
        MsSqlSessionParam::Generic {
            names: names.into_iter().map(Cow::Borrowed).collect(),
            value,
        }
    };
    Ok(mssql_statement(MsSqlStatement::SessionParam(Box::new(
        param,
    ))))
}

fn parse_routine_params<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
    allow_modes: bool,
) -> PResult<Vec<RoutineArg<'a, Builtin>>> {
    p.expect_op("(")?;
    if p.eat_op(")") {
        return Ok(Vec::new());
    }
    let params = p.parse_comma_separated(|p| {
        let mode = if allow_modes {
            ["IN", "OUT", "INOUT"]
                .into_iter()
                .find(|mode| p.eat_keyword(mode))
        } else {
            None
        };
        let name = Some(p.parse_ident()?);
        let data_type = D::parse_data_type(p)?;
        let default = if p.eat_op("=") {
            Some((true, p.parse_expr()?))
        } else {
            None
        };
        Ok(RoutineArg {
            mode: mode.map(Cow::Borrowed),
            name,
            data_type,
            default,
        })
    })?;
    p.expect_op(")")?;
    Ok(params)
}

fn parse_create_procedure<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
    or_alter: bool,
) -> PResult<Statement<'a, Builtin>> {
    let name = parse_object_name(p)?;
    let params = if p.at_op("(") {
        Some(parse_routine_params(p, true)?)
    } else {
        None
    };
    let language = if p.eat_keyword("LANGUAGE") {
        Some(p.parse_ident()?)
    } else {
        None
    };
    p.expect_keyword("AS")?;
    let body = parse_body(p)?;
    Ok(mssql_statement(MsSqlStatement::CreateProcedure(Box::new(
        MsSqlCreateProcedure {
            or_alter,
            name,
            params,
            language,
            body,
        },
    ))))
}

fn parse_table_columns<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Vec<ColumnDef<'a, Builtin>>> {
    p.expect_op("(")?;
    let columns = p.parse_comma_separated(D::parse_column_def)?;
    p.expect_op(")")?;
    Ok(columns)
}

fn parse_returns<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<MsSqlReturns<'a>> {
    if p.at_keyword("TABLE") && p.peek_nth(1).is_op("(") {
        p.advance();
        return Ok(MsSqlReturns::Table {
            name: None,
            columns: parse_table_columns(p)?,
        });
    }
    if p.peek_nth(1).is_word("TABLE") && p.at_ident() {
        let name = Some(p.parse_ident()?);
        p.advance();
        return Ok(MsSqlReturns::Table {
            name,
            columns: parse_table_columns(p)?,
        });
    }
    D::parse_data_type(p).map(MsSqlReturns::Type)
}

fn parse_create_function<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
    or_alter: bool,
) -> PResult<Statement<'a, Builtin>> {
    let name = parse_object_name(p)?;
    let params = parse_routine_params(p, false)?;
    p.expect_keyword("RETURNS")?;
    let returns = parse_returns(p)?;
    p.eat_keyword("AS");
    let body = if p.eat_keyword("BEGIN") {
        let statements = parse_strict_list(p)?;
        p.expect_keyword("END")?;
        MsSqlFunctionBody::Block(statements)
    } else if p.eat_keyword("RETURN") {
        if p.at_op("(") {
            MsSqlFunctionBody::ReturnExpr(p.parse_expr()?)
        } else if p.at_keyword("SELECT") {
            MsSqlFunctionBody::ReturnSelect(Box::new(D::parse_select(p)?))
        } else {
            return Err(p.expected("a subquery or SELECT after RETURN"));
        }
    } else {
        return Err(p.expected("BEGIN or RETURN"));
    };
    Ok(mssql_statement(MsSqlStatement::CreateFunction(Box::new(
        MsSqlCreateFunction {
            or_alter,
            name,
            params,
            returns,
            body,
        },
    ))))
}

fn parse_trigger_event<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<MsSqlTriggerEvent<'a>> {
    if p.eat_keyword("INSERT") {
        return Ok(MsSqlTriggerEvent::Insert);
    }
    if p.eat_keyword("DELETE") {
        return Ok(MsSqlTriggerEvent::Delete);
    }
    if p.eat_keyword("TRUNCATE") {
        return Ok(MsSqlTriggerEvent::Truncate);
    }
    p.expect_keyword("UPDATE")?;
    let columns = if p.eat_keyword("OF") {
        p.parse_comma_separated(Parser::parse_ident)?
    } else {
        Vec::new()
    };
    Ok(MsSqlTriggerEvent::Update(columns))
}

fn parse_create_trigger<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
    or_alter: bool,
) -> PResult<Statement<'a, Builtin>> {
    let name = parse_object_name(p)?;
    p.expect_keyword("ON")?;
    let table = parse_object_name(p)?;
    let period = if p.eat_keyword("FOR") {
        "FOR"
    } else if p.eat_keyword("AFTER") {
        "AFTER"
    } else if p.eat_keyword("BEFORE") {
        "BEFORE"
    } else if p.eat_keyword("INSTEAD") {
        p.expect_keyword("OF")?;
        "INSTEAD OF"
    } else {
        return Err(p.expected("FOR, AFTER or INSTEAD OF"));
    };
    let events = p.parse_comma_separated(parse_trigger_event)?;
    p.expect_keyword("AS")?;
    let body = parse_body(p)?;
    Ok(mssql_statement(MsSqlStatement::CreateTrigger(Box::new(
        MsSqlCreateTrigger {
            or_alter,
            name,
            table,
            period: period.into(),
            events,
            body,
        },
    ))))
}

fn parse_create_role<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Statement<'a, Builtin>> {
    let if_not_exists = grammar::parse_if_not_exists(p)?;
    let names = p.parse_comma_separated(parse_object_name)?;
    p.eat_keyword("WITH");
    let authorization = if p.eat_keyword("AUTHORIZATION") {
        Some(parse_object_name(p)?)
    } else {
        None
    };
    Ok(mssql_statement(MsSqlStatement::CreateRole(Box::new(
        MsSqlCreateRole {
            if_not_exists,
            names,
            authorization,
        },
    ))))
}

fn parse_clustered_option<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<MsSqlTableOption<'a>> {
    p.advance();
    if p.eat_keyword("COLUMNSTORE") {
        p.expect_keyword("INDEX")?;
        if p.eat_keyword("ORDER") {
            return grammar::parse_ident_list(p).map(MsSqlTableOption::ClusteredColumnstoreOrder);
        }
        return Ok(MsSqlTableOption::ClusteredColumnstore);
    }
    p.expect_keyword("INDEX")?;
    p.expect_op("(")?;
    let columns = p.parse_comma_separated(|p| {
        let name = p.parse_ident()?;
        let asc = if p.eat_keyword("ASC") {
            Some(true)
        } else if p.eat_keyword("DESC") {
            Some(false)
        } else {
            None
        };
        Ok((name, asc))
    })?;
    p.expect_op(")")?;
    Ok(MsSqlTableOption::ClusteredIndex(columns))
}

fn parse_partition_option<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<MsSqlTableOption<'a>> {
    p.advance();
    p.expect_op("(")?;
    let column = p.parse_ident()?;
    p.expect_keyword("RANGE")?;
    let direction = if p.eat_keyword("LEFT") {
        Some("LEFT")
    } else if p.eat_keyword("RIGHT") {
        Some("RIGHT")
    } else {
        None
    };
    p.expect_keyword("FOR")?;
    p.expect_keyword("VALUES")?;
    p.expect_op("(")?;
    let values = p.parse_comma_separated(Parser::parse_expr)?;
    p.expect_op(")")?;
    p.expect_op(")")?;
    Ok(MsSqlTableOption::Partition {
        column,
        direction: direction.map(Cow::Borrowed),
        values,
    })
}

fn parse_table_option<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<MsSqlTableOption<'a>> {
    if p.at_keyword("HEAP") {
        p.advance();
        return Ok(MsSqlTableOption::Heap);
    }
    if p.at_keyword("PARTITION") {
        return parse_partition_option(p);
    }
    if p.at_keyword("CLUSTERED") {
        return parse_clustered_option(p);
    }
    let mut parts = vec![p.parse_ident()?];
    while p.eat_op(".") {
        parts.push(p.parse_name_part()?);
    }
    p.expect_op("=")?;
    let value = p.parse_expr()?;
    Ok(MsSqlTableOption::KeyValue {
        key: ObjectName { parts },
        value,
    })
}

pub fn parse_create<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Statement<'a, Builtin>> {
    let or_alter = p.peek_nth(1).is_word("OR") && p.peek_nth(2).is_word("ALTER");
    let object = p.peek_nth(if or_alter { 3 } else { 1 });
    let routine = is_any(object, &["PROCEDURE", "PROC", "FUNCTION", "TRIGGER"]);
    if or_alter || routine || object.is_word("ROLE") {
        p.advance();
        if or_alter {
            p.advance();
            p.advance();
        }
        if p.eat_keyword("PROCEDURE") || p.eat_keyword("PROC") {
            return parse_create_procedure(p, or_alter);
        }
        if p.eat_keyword("FUNCTION") {
            return parse_create_function(p, or_alter);
        }
        if p.eat_keyword("TRIGGER") {
            return parse_create_trigger(p, or_alter);
        }
        if p.eat_keyword("ROLE") {
            return parse_create_role(p);
        }
        return D::parse_create_view(p, CreateFlags::default())
            .map(|view| mssql_statement(MsSqlStatement::CreateOrAlterView(Box::new(view))));
    }
    let statement = grammar::parse_create(p)?;
    if p.at_keyword("WITH")
        && p.peek_nth(1).is_op("(")
        && let StatementKind::CreateTable(table) = statement.kind
    {
        p.advance();
        p.advance();
        let options = p.parse_comma_separated(parse_table_option)?;
        p.expect_op(")")?;
        return Ok(mssql_statement(MsSqlStatement::CreateTable(Box::new(
            MsSqlCreateTable {
                table: *table,
                options,
            },
        ))));
    }
    Ok(statement)
}

pub fn parse_statement<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Statement<'a, Builtin>> {
    if p.peek().kind != TokenKind::Word {
        return grammar::parse_statement(p);
    }
    if p.at_keyword("BEGIN") {
        if !at_transaction_begin(p) {
            p.advance();
            let statements = parse_strict_list(p)?;
            p.expect_keyword("END")?;
            return Ok(mssql_statement(MsSqlStatement::Block(statements)));
        }
        if let Some(marker) = parse_try_catch(p) {
            return Ok(mssql_statement(MsSqlStatement::TryCatch(marker.into())));
        }
        return grammar::parse_statement(p);
    }
    if p.at_keyword("END") {
        if let Some(marker) = parse_try_catch(p) {
            return Ok(mssql_statement(MsSqlStatement::TryCatch(marker.into())));
        }
        return grammar::parse_statement(p);
    }
    if p.at_keyword("IF") {
        return parse_if(p);
    }
    if p.at_keyword("WHILE") {
        return parse_while(p);
    }
    if p.at_keyword("DECLARE") {
        p.advance();
        let items = p.parse_comma_separated(parse_declare_item)?;
        return Ok(mssql_statement(MsSqlStatement::Declare(items)));
    }
    if p.at_keyword("PRINT") {
        p.advance();
        let expr = p.parse_expr()?;
        return Ok(mssql_statement(MsSqlStatement::Print(Box::new(expr))));
    }
    if p.at_keyword("RAISERROR") {
        return parse_raiserror(p)
            .map(|raise| mssql_statement(MsSqlStatement::RaisError(Box::new(raise))));
    }
    if p.at_keyword("THROW") {
        return parse_throw(p);
    }
    if p.at_keyword("WAITFOR") {
        return parse_waitfor(p);
    }
    if p.at_keyword("RETURN") {
        return parse_return(p);
    }
    if p.at_keyword("OPEN") {
        p.advance();
        let name = p.parse_ident()?;
        return Ok(mssql_statement(MsSqlStatement::Open(name)));
    }
    if p.at_keyword("CLOSE") {
        p.advance();
        let name = if p.eat_keyword("ALL") {
            None
        } else {
            Some(p.parse_ident()?)
        };
        return Ok(mssql_statement(MsSqlStatement::Close(name)));
    }
    if p.at_keyword("FETCH") {
        return parse_fetch(p);
    }
    if p.at_keyword("DEALLOCATE") {
        p.advance();
        let prepare = p.eat_keyword("PREPARE");
        let name = p.parse_ident()?;
        return Ok(mssql_statement(MsSqlStatement::Deallocate {
            prepare,
            name,
        }));
    }
    if p.at_keyword("EXEC") || p.at_keyword("EXECUTE") {
        return parse_execute(p)
            .map(|execute| mssql_statement(MsSqlStatement::Execute(Box::new(execute))));
    }
    if p.at_keyword("DENY") {
        return parse_deny(p);
    }
    if p.at_keyword("ALTER") && p.peek_nth(1).is_word("ROLE") {
        return parse_alter_role(p);
    }
    if grammar::at_alter_view(p) {
        return grammar::parse_alter_view(p, true)
            .map(|a| StatementKind::AlterView(Box::new(a)).into());
    }
    if p.at_keyword("SET") && at_session_param(p) {
        return parse_session_param(p);
    }
    grammar::parse_statement(p)
}

/// Consumes `BEGIN TRY`, `END TRY`, `BEGIN CATCH` or `END CATCH`, returning
/// its normalized form.
fn parse_try_catch<'a, D: Dialect>(p: &mut Parser<'a, D>) -> Option<&'static str> {
    let marker = match (p.at_keyword("BEGIN"), p.peek_nth(1)) {
        (true, next) if next.is_word("TRY") => "BEGIN TRY",
        (true, next) if next.is_word("CATCH") => "BEGIN CATCH",
        (false, next) if next.is_word("TRY") => "END TRY",
        (false, next) if next.is_word("CATCH") => "END CATCH",
        _ => return None,
    };
    p.advance();
    p.advance();
    Some(marker)
}

/// Parses `TOP n` or `TOP (expr)`, with the keyword read.
fn parse_top<'a, D: Dialect<Ext = Builtin>>(p: &mut Parser<'a, D>) -> PResult<MsSqlTop<'a>> {
    let parenthesized = p.eat_op("(");
    let quantity = if parenthesized {
        let quantity = p.parse_expr()?;
        p.expect_op(")")?;
        quantity
    } else {
        let token = p.expect_kind(TokenKind::Number, "a number after TOP")?;
        Expr::new(
            ExprKind::Literal(crate::ast::Literal::Number(token.text.into())),
            token.span,
        )
    };
    let percent = p.eat_keyword("PERCENT");
    let with_ties = p.at_keyword("WITH") && p.peek_nth(1).is_word("TIES");
    if with_ties {
        p.advance();
        p.advance();
    }
    Ok(MsSqlTop {
        quantity,
        parenthesized,
        percent,
        with_ties,
    })
}

pub fn parse_select_head<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<(BuiltinSelect<'a>, SetQuantifier)> {
    let quantifier = grammar::parse_quantifier(p);
    let mssql_top = if p.eat_keyword("TOP") {
        Some(Box::new(parse_top(p)?))
    } else {
        None
    };
    Ok((
        BuiltinSelect {
            mssql_top,
            ..Default::default()
        },
        quantifier,
    ))
}

fn parse_string_alias<'a, D: Dialect>(p: &mut Parser<'a, D>) -> Option<Ident<'a>> {
    let token = p.peek();
    if token.kind != TokenKind::String {
        return None;
    }
    p.advance();
    Some(Ident {
        text: Cow::Borrowed(&token.text[1..token.text.len() - 1]),
        quote: Some('\''),
        span: token.span,
    })
}

pub fn parse_select_item_alias<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
    expr: Expr<'a, Builtin>,
) -> PResult<SelectItem<'a, Builtin>> {
    // `alias = expr` names a select item.
    let Expr { kind, span } = expr;
    let expr = match kind {
        ExprKind::Binary {
            left,
            op: BinaryOp::Eq,
            right,
        } if matches!(left.as_ref().kind, ExprKind::Ident(_)) => {
            let ExprKind::Ident(alias) = left.kind else {
                unreachable!()
            };
            return Ok(SelectItemKind::Expr {
                expr: *right,
                alias: Some(alias),
            }
            .into());
        }
        kind => Expr::new(kind, span),
    };
    if p.at_keyword("AS") {
        let next = p.peek_nth(1);
        if is_any(next, &NON_ALIAS_KEYWORDS) {
            return Err(p.expected("an alias"));
        }
        if next.kind == TokenKind::String {
            p.advance();
            let alias = parse_string_alias(p);
            return Ok(SelectItemKind::Expr { expr, alias }.into());
        }
    }
    if let Some(alias) = parse_string_alias(p) {
        return Ok(SelectItemKind::Expr {
            expr,
            alias: Some(alias),
        }
        .into());
    }
    grammar::parse_select_item_alias(p, expr)
}

pub fn parse_data_type<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<DataType<'a>> {
    let name = p.parse_ident()?;
    if p.at_op("(") && p.peek_nth(1).is_word("MAX") && p.peek_nth(2).is_op(")") {
        p.advance();
        let max = p.advance();
        p.advance();
        return Ok(DataType {
            qualifier: Vec::new(),
            name,
            angle: None,
            args: vec![max.text.into()],
            suffix: DataTypeSuffix::None,
            array: crate::ast::ArrayType::None,
        });
    }
    grammar::parse_data_type_after_name(p, name)
}

pub fn parse_column_option<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Option<ColumnOption<'a, Builtin>>> {
    if !p.at_keyword("IDENTITY") {
        return grammar::parse_column_option(p);
    }
    p.advance();
    let params = if p.eat_op("(") {
        let seed = grammar::parse_number_text(p)?;
        p.expect_op(",")?;
        let increment = grammar::parse_number_text(p)?;
        p.expect_op(")")?;
        Some(IdentityParams {
            seed: seed.into(),
            increment: increment.into(),
            start_form: false,
        })
    } else {
        None
    };
    Ok(Some(ColumnOption::Identity {
        autoincrement: false,
        params,
        order: None,
    }))
}

fn parse_convert<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
    is_try: bool,
) -> PResult<Expr<'a, Builtin>> {
    p.expect_op("(")?;
    let data_type = D::parse_data_type(p)?;
    p.expect_op(",")?;
    let expr = p.parse_expr()?;
    let mut styles = Vec::new();
    while p.eat_op(",") {
        styles.push(p.parse_expr()?);
    }
    p.expect_op(")")?;
    Ok(
        ExprKind::Ext(BuiltinExpr::MsSql(MsSqlExpr::Convert(Box::new(
            MsSqlConvert {
                is_try,
                data_type,
                expr,
                styles,
            },
        ))))
        .into(),
    )
}

/// The null clause at the cursor, without consuming it.
fn json_null_clause<'a, D: Dialect>(p: &Parser<'a, D>) -> Option<&'static str> {
    if !p.peek_nth(1).is_word("ON") || !p.peek_nth(2).is_word("NULL") {
        return None;
    }
    if p.at_keyword("ABSENT") {
        Some("ABSENT ON NULL")
    } else if p.at_keyword("NULL") {
        Some("NULL ON NULL")
    } else {
        None
    }
}

fn parse_json_call<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
    name: Ident<'a>,
) -> PResult<Expr<'a, Builtin>> {
    p.expect_op("(")?;
    let mut args = Vec::new();
    let mut null_clause = None;
    loop {
        if let Some(clause) = json_null_clause(p) {
            null_clause = Some(clause);
            p.advance();
            p.advance();
            p.advance();
            break;
        }
        if p.at_op(")") {
            break;
        }
        let key = p.parse_expr()?;
        if p.eat_op(":") {
            args.push(MsSqlJsonArg::KeyValue(key, p.parse_expr()?));
        } else {
            args.push(MsSqlJsonArg::Value(key));
        }
        if !p.eat_op(",") && json_null_clause(p).is_none() {
            break;
        }
    }
    p.expect_op(")")?;
    Ok(
        ExprKind::Ext(BuiltinExpr::MsSql(MsSqlExpr::JsonCall(Box::new(
            MsSqlJsonCall {
                name,
                args,
                null_clause: null_clause.map(Cow::Borrowed),
            },
        ))))
        .into(),
    )
}

pub fn parse_function_call<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
    name: ObjectName<'a>,
) -> PResult<Expr<'a, Builtin>> {
    if let [only] = name.parts.as_slice()
        && only.quote.is_none()
    {
        let text = &only.text;
        let type_first = matches!(p.peek_nth(1).kind, TokenKind::Word | TokenKind::QuotedIdent);
        // A literal first argument is an ordinary call, as in the original.
        if type_first && text.eq_ignore_ascii_case("CONVERT") {
            return parse_convert(p, false);
        }
        if type_first && text.eq_ignore_ascii_case("TRY_CONVERT") {
            return parse_convert(p, true);
        }
        if text.eq_ignore_ascii_case("JSON_OBJECT") || text.eq_ignore_ascii_case("JSON_ARRAY") {
            return parse_json_call(p, only.clone());
        }
    }
    grammar::parse_function_call(p, name)
}

/// Parses `.name(args)` after an expression, a method call on an XML value.
fn parse_method<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
    left: Expr<'a, Builtin>,
    _bp: u8,
) -> PResult<Expr<'a, Builtin>> {
    p.advance();
    let name = p.parse_name_part()?;
    p.expect_op("(")?;
    let args = if p.at_op(")") {
        Vec::new()
    } else {
        p.parse_comma_separated(Parser::parse_expr)?
    };
    p.expect_op(")")?;
    // A method call passes through more wrapper types than an ordinary level.
    p.deepen_by(2)?;
    Ok(ExprKind::Ext(BuiltinExpr::MsSql(MsSqlExpr::Method(Box::new(
        MsSqlMethod {
            expr: left,
            name,
            args,
        },
    ))))
    .into())
}

pub fn infix_rule<'a, D: Dialect<Ext = Builtin>>(p: &Parser<'a, D>) -> Option<InfixRule<'a, D>> {
    if p.at_op(".")
        && matches!(p.peek_nth(1).kind, TokenKind::Word | TokenKind::QuotedIdent)
        && p.peek_nth(2).is_op("(")
    {
        return Some(InfixRule::custom(prec::POSTFIX, parse_method));
    }
    grammar::infix_rule(p)
}

/// Reads a word as a name, in tables where keywords are allowed.
fn parse_keyword_name<'a, D: Dialect>(p: &mut Parser<'a, D>) -> Ident<'a> {
    let token = p.advance();
    Ident {
        text: token.text.into(),
        quote: None,
        span: token.span,
    }
}

/// True at `db..table`, a name whose schema is left out.
fn at_double_dot<'a, D: Dialect>(p: &Parser<'a, D>) -> bool {
    p.at_ident() && p.peek_nth(1).is_op(".") && p.peek_nth(2).is_op(".")
}

/// Parses `db..table [.name ...]`, with the empty schema as an empty part.
fn parse_double_dot_name<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<ObjectName<'a>> {
    let mut parts = vec![p.parse_ident()?];
    p.expect_op(".")?;
    parts.push(Ident {
        text: "".into(),
        quote: None,
        span: Span::default(),
    });
    while p.eat_op(".") {
        parts.push(p.parse_name_part()?);
    }
    Ok(ObjectName { parts })
}

fn parse_openjson<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<TableFactor<'a, Builtin>> {
    p.advance();
    p.expect_op("(")?;
    let expr = p.parse_expr()?;
    let path = if p.eat_op(",") {
        Some(p.parse_expr()?)
    } else {
        None
    };
    p.expect_op(")")?;
    let columns = if p.at_keyword("WITH") && p.peek_nth(1).is_op("(") {
        p.advance();
        p.advance();
        let columns = p.parse_comma_separated(|p| {
            let name = p.parse_ident()?;
            let data_type = D::parse_data_type(p)?;
            let token = p.peek();
            let path = if token.kind == TokenKind::String {
                p.advance();
                Some(D::string_literal(token))
            } else {
                None
            };
            let as_json = p.eat_keyword("AS");
            if as_json {
                p.expect_keyword("JSON")?;
            }
            Ok(MsSqlOpenJsonColumn {
                name,
                data_type,
                path,
                as_json,
            })
        })?;
        p.expect_op(")")?;
        columns
    } else {
        Vec::new()
    };
    Ok(TableFactor::Ext(BuiltinTableFactor::MsSql(
        MsSqlTableFactor::OpenJson(Box::new(MsSqlOpenJson {
            expr,
            path,
            columns,
        })),
    )))
}

pub fn parse_table_factor<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<TableFactor<'a, Builtin>> {
    if p.at_keyword("OPENJSON") && p.peek_nth(1).is_op("(") {
        return parse_openjson(p);
    }
    if p.at_keyword("LIMIT") {
        return Ok(TableFactor::Named(ObjectName {
            parts: vec![parse_keyword_name(p)],
        }));
    }
    let factor = if at_double_dot(p) {
        let name = parse_double_dot_name(p)?;
        if p.at_op("(") {
            let call = Box::new(p.parse_function_call(name)?);
            TableFactor::Function {
                call,
                with_ordinality: false,
            }
        } else {
            TableFactor::Named(name)
        }
    } else {
        grammar::parse_table_factor(p)?
    };
    let time_travel = p.at_keyword("FOR")
        && p.peek_nth(1).is_word("SYSTEM_TIME")
        && p.peek_nth(2).is_word("AS")
        && p.peek_nth(3).is_word("OF");
    if time_travel && let TableFactor::Named(name) = factor {
        for _ in 0..4 {
            p.advance();
        }
        let at = p.parse_expr()?;
        return Ok(TableFactor::TimeTravel(Box::new(TimeTravel {
            name,
            kind: "FOR SYSTEM_TIME".into(),
            at,
        })));
    }
    Ok(factor)
}

pub fn parse_table_ref<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<TableRef<'a, Builtin>> {
    let factor = D::parse_table_factor(p)?;
    if p.at_keyword("AS") && is_any(p.peek_nth(1), &NON_ALIAS_KEYWORDS) {
        return Err(p.expected("an alias"));
    }
    let alias = grammar::parse_optional_table_alias(p)?;
    let table = TableRef {
        factor,
        alias,
        span: Span::default(),
    };
    if !(p.at_keyword("WITH") && p.peek_nth(1).is_op("(")) {
        return Ok(table);
    }
    p.advance();
    p.advance();
    let hints = p.parse_comma_separated(Parser::parse_expr)?;
    p.expect_op(")")?;
    Ok(TableRef {
        factor: TableFactor::Ext(BuiltinTableFactor::MsSql(MsSqlTableFactor::Hinted(
            Box::new(MsSqlHintedTable { table, hints }),
        ))),
        alias: None,
        span: Span::default(),
    })
}

pub fn parse_join<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Option<Join<'a, Builtin>>> {
    let apply = (p.at_keyword("CROSS") || p.at_keyword("OUTER")) && p.peek_nth(1).is_word("APPLY");
    if !apply {
        return grammar::parse_join(p);
    }
    let kind = if p.at_keyword("CROSS") {
        JoinKind::Custom("CROSS APPLY".into())
    } else {
        JoinKind::Custom("OUTER APPLY".into())
    };
    p.advance();
    p.advance();
    let table = p.parse_table_ref()?;
    Ok(Some(Join {
        kind,
        spelled_out: false,
        table,
        constraint: JoinConstraint::None,
    }))
}

pub fn parse_output_clause<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Option<Box<OutputClause<'a, Builtin>>>> {
    if !p.eat_keyword("OUTPUT") {
        return Ok(None);
    }
    let items = D::parse_projection(p)?;
    let into = if p.eat_keyword("INTO") {
        Some(parse_object_name(p)?)
    } else {
        None
    };
    Ok(Some(Box::new(OutputClause { items, into })))
}

/// Parses a string inside parentheses, with the opening one still to read.
fn parse_parenthesized_string<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<StringLiteral<'a>> {
    p.expect_op("(")?;
    let token = p.expect_kind(TokenKind::String, "a string")?;
    p.expect_op(")")?;
    Ok(D::string_literal(token))
}

fn parse_for_xml<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<MsSqlForXml<'a>> {
    let mode = if p.eat_keyword("RAW") {
        "RAW"
    } else if p.eat_keyword("AUTO") {
        "AUTO"
    } else if p.eat_keyword("EXPLICIT") {
        "EXPLICIT"
    } else {
        p.expect_keyword("PATH")?;
        "PATH"
    };
    let mode_name = if p.at_op("(") {
        Some(parse_parenthesized_string(p)?)
    } else {
        None
    };
    let mut clause = MsSqlForXml {
        mode: mode.into(),
        mode_name,
        elements: false,
        binary_base64: false,
        root: None,
        type_directive: false,
    };
    while p.eat_op(",") {
        if p.eat_keyword("ELEMENTS") {
            clause.elements = true;
        } else if p.eat_keyword("BINARY") {
            p.expect_keyword("BASE64")?;
            clause.binary_base64 = true;
        } else if p.eat_keyword("ROOT") {
            clause.root = Some(parse_parenthesized_string(p)?);
        } else if p.eat_keyword("TYPE") {
            clause.type_directive = true;
        } else {
            return Err(p.expected("a FOR XML directive"));
        }
    }
    Ok(clause)
}

fn parse_for_json<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<MsSqlForJson<'a>> {
    let mode = if p.eat_keyword("AUTO") {
        "AUTO"
    } else {
        p.expect_keyword("PATH")?;
        "PATH"
    };
    let mut clause = MsSqlForJson {
        mode: mode.into(),
        root: None,
        include_null_values: false,
        without_array_wrapper: false,
    };
    while p.eat_op(",") {
        if p.eat_keyword("ROOT") {
            clause.root = Some(parse_parenthesized_string(p)?);
        } else if p.eat_keyword("INCLUDE_NULL_VALUES") {
            clause.include_null_values = true;
        } else if p.eat_keyword("WITHOUT_ARRAY_WRAPPER") {
            clause.without_array_wrapper = true;
        } else {
            return Err(p.expected("a FOR JSON directive"));
        }
    }
    Ok(clause)
}

pub fn parse_query_tail<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<BuiltinQueryTail<'a>> {
    if !p.at_keyword("FOR") {
        return Ok(Default::default());
    }
    let next = p.peek_nth(1);
    let clause = if next.is_word("XML") {
        p.advance();
        p.advance();
        MsSqlForClause::Xml(parse_for_xml(p)?)
    } else if next.is_word("JSON") {
        p.advance();
        p.advance();
        MsSqlForClause::Json(parse_for_json(p)?)
    } else if next.is_word("BROWSE") {
        p.advance();
        p.advance();
        MsSqlForClause::Browse
    } else {
        return Ok(Default::default());
    };
    Ok(BuiltinQueryTail(Some(Box::new(
        BuiltinQueryClauses::MsSql(clause),
    ))))
}

pub fn parse_grantees<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Vec<Grantee<'a>>> {
    p.parse_comma_separated(grammar::parse_grantee)
}

pub fn parse_prefix<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Expr<'a, Builtin>> {
    if grammar::at_odbc_escape(p) {
        return grammar::parse_odbc_escape(p);
    }
    let token = p.peek();
    // T-SQL has no boolean literals, so these are ordinary names.
    if is_any(token, &["TRUE", "FALSE"]) {
        return grammar::parse_name_expr(p);
    }
    // A `:` is punctuation to the lexer, so `:name` is only a parameter when
    // the name follows without a space.
    if token.is_op(":") {
        let next = p.peek_nth(1);
        if matches!(next.kind, TokenKind::Word | TokenKind::Number)
            && next.span.start == token.span.end
        {
            p.advance();
            p.advance();
            return Ok(ExprKind::Placeholder(Cow::Borrowed(
                &p.source()[token.span.start as usize..next.span.end as usize],
            ))
            .into());
        }
    }
    grammar::parse_prefix(p)
}

pub fn is_reserved(word: &str) -> bool {
    !word.eq_ignore_ascii_case("TRUE")
        && !word.eq_ignore_ascii_case("FALSE")
        && grammar::is_reserved(word)
}

/// True for words that cannot follow an expression as a bare alias.
pub fn is_reserved_for_alias(word: &str) -> bool {
    grammar::is_reserved_for_alias(word)
        || NON_ALIAS_KEYWORDS
            .iter()
            .chain(&["OUTPUT", "VALUES", "END"])
            .any(|keyword| keyword.eq_ignore_ascii_case(word))
}

impl Dialect for MsSql {
    /// Recursive CTEs need no keyword here, and `RECURSIVE` is rejected.
    fn eat_with_recursive<'a>(_p: &mut Parser<'a, Self>) -> bool {
        false
    }

    type Ext = Builtin;

    fn parse_update_from_after_set<'a>(
        p: &mut Parser<'a, Self>,
    ) -> PResult<Vec<TableWithJoins<'a, Builtin>>> {
        grammar::parse_optional_update_from(p)
    }

    fn parse_select_into<'a>(
        p: &mut Parser<'a, Self>,
    ) -> PResult<Option<Box<SelectInto<'a, Builtin>>>> {
        grammar::parse_select_into_table(p)
    }

    fn parse_is_test<'a>(p: &mut Parser<'a, Self>) -> PResult<Option<IsTest<'a, Builtin>>> {
        grammar::parse_is_json(p)
    }

    fn parse_statement_after_with<'a>(
        p: &mut Parser<'a, Self>,
    ) -> PResult<Option<Statement<'a, Builtin>>> {
        grammar::parse_dml_after_with(p, true, true)
    }

    fn parse_insert_source<'a>(p: &mut Parser<'a, Self>) -> PResult<InsertSource<'a, Builtin>> {
        grammar::parse_insert_source_with_default(p)
    }

    fn parse_insert_into<'a>(p: &mut Parser<'a, Self>) -> PResult<bool> {
        Ok(p.eat_keyword("INTO"))
    }

    fn parse_create_schema<'a>(
        p: &mut Parser<'a, Self>,
        flags: CreateFlags,
    ) -> PResult<CreateSchema<'a>> {
        if flags != CreateFlags::default() {
            return Err(p.expected("TABLE or VIEW"));
        }
        grammar::parse_create_schema_authorization(p, true)
    }

    #[inline]
    fn lex_token(rest: &str) -> Option<LexResult> {
        lex_token(rest)
    }

    fn is_reserved(word: &str) -> bool {
        is_reserved(word)
    }

    fn is_reserved_for_alias(word: &str) -> bool {
        is_reserved_for_alias(word)
    }

    fn string_literal(token: Token<'_>) -> StringLiteral<'_> {
        string_literal(token)
    }

    fn parse_statement<'a>(p: &mut Parser<'a, Self>) -> PResult<Statement<'a, Builtin>> {
        parse_statement(p)
    }

    fn parse_create<'a>(p: &mut Parser<'a, Self>) -> PResult<Statement<'a, Builtin>> {
        parse_create(p)
    }

    fn parse_select_head<'a>(
        p: &mut Parser<'a, Self>,
    ) -> PResult<(BuiltinSelect<'a>, SetQuantifier)> {
        parse_select_head(p)
    }

    fn parse_select_item_alias<'a>(
        p: &mut Parser<'a, Self>,
        expr: Expr<'a, Builtin>,
    ) -> PResult<SelectItem<'a, Builtin>> {
        parse_select_item_alias(p, expr)
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

    /// T-SQL has no `FILTER` clause.
    fn parse_function_filter<'a>(_p: &mut Parser<'a, Self>) -> PResult<Option<Expr<'a, Builtin>>> {
        Ok(None)
    }

    #[inline]
    fn lex_comment(rest: &str) -> Option<Result<usize, &'static str>> {
        lex_comment(rest)
    }

    fn parse_data_type<'a>(p: &mut Parser<'a, Self>) -> PResult<DataType<'a>> {
        parse_data_type(p)
    }

    fn parse_column_option<'a>(
        p: &mut Parser<'a, Self>,
    ) -> PResult<Option<ColumnOption<'a, Builtin>>> {
        parse_column_option(p)
    }

    fn parse_drop_object_type<'a>(p: &mut Parser<'a, Self>) -> Option<ObjectType<'a>> {
        if let Some(object_type) = grammar::eat_drop_object_type(p, &["TYPE"]) {
            return Some(object_type);
        }
        grammar::eat_drop_object_type(p, &["TRIGGER"])
    }

    fn parse_output_clause<'a>(
        p: &mut Parser<'a, Self>,
    ) -> PResult<Option<Box<OutputClause<'a, Builtin>>>> {
        parse_output_clause(p)
    }

    fn parse_query_tail<'a>(p: &mut Parser<'a, Self>) -> PResult<BuiltinQueryTail<'a>> {
        parse_query_tail(p)
    }

    fn parse_grantees<'a>(p: &mut Parser<'a, Self>) -> PResult<Vec<Grantee<'a>>> {
        parse_grantees(p)
    }
}

fn write_statements(
    f: &mut fmt::Formatter<'_>,
    statements: &[Statement<'_, Builtin>],
) -> fmt::Result {
    for (i, statement) in statements.iter().enumerate() {
        if i > 0 {
            SpaceOrNewline.fmt(f)?;
        }
        write!(f, "{statement};")?;
    }
    Ok(())
}

/// Statements, each on its own line in `{:#}`.
struct StatementLines<'s, 'a>(&'s [Statement<'a, Builtin>]);

impl fmt::Display for StatementLines<'_, '_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_statements(f, self.0)
    }
}

/// Writes a body after a keyword, on indented lines if it is not a block.
fn write_body_after_keyword(f: &mut fmt::Formatter<'_>, body: &MsSqlBody<'_>) -> fmt::Result {
    match body {
        MsSqlBody::Statements(statements) => {
            SpaceOrNewline.fmt(f)?;
            Indent(StatementLines(statements)).fmt(f)
        }
        MsSqlBody::Block(statements) => {
            f.write_str(" ")?;
            write_block(f, statements)
        }
    }
}

/// Writes ` AS` and a body, which starts on a new line in `{:#}`.
fn write_as_body(f: &mut fmt::Formatter<'_>, body: &MsSqlBody<'_>) -> fmt::Result {
    f.write_str(" AS")?;
    match body {
        MsSqlBody::Statements(_) => write_body_after_keyword(f, body),
        MsSqlBody::Block(statements) => {
            SpaceOrNewline.fmt(f)?;
            write_block(f, statements)
        }
    }
}

impl fmt::Display for MsSqlBody<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Statements(statements) => write_statements(f, statements),
            Self::Block(statements) => write_block(f, statements),
        }
    }
}

fn write_block(f: &mut fmt::Formatter<'_>, statements: &[Statement<'_, Builtin>]) -> fmt::Result {
    f.write_str("BEGIN")?;
    if !statements.is_empty() {
        SpaceOrNewline.fmt(f)?;
        Indent(StatementLines(statements)).fmt(f)?;
    }
    SpaceOrNewline.fmt(f)?;
    f.write_str("END")
}

impl fmt::Display for MsSqlIf<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "IF {}", self.condition)?;
        write_body_after_keyword(f, &self.then_branch)?;
        if let Some(branch) = &self.else_branch {
            SpaceOrNewline.fmt(f)?;
            f.write_str("ELSE")?;
            write_body_after_keyword(f, branch)?;
        }
        Ok(())
    }
}

impl fmt::Display for MsSqlWhile<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "WHILE {}", self.condition)?;
        write_body_after_keyword(f, &self.body)
    }
}

impl fmt::Display for MsSqlDeclare<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.name.fmt(f)?;
        if self.cursor {
            f.write_str(" CURSOR")?;
        }
        if let Some(query) = &self.for_query {
            write!(f, " FOR {query}")?;
        }
        if let Some(data_type) = &self.data_type {
            write!(f, " {data_type}")?;
        }
        if let Some(value) = &self.assignment {
            write!(f, " = {value}")?;
        }
        Ok(())
    }
}

impl fmt::Display for MsSqlRaisError<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "RAISERROR({}, {}, {}",
            self.message, self.severity, self.state
        )?;
        if !self.arguments.is_empty() {
            write!(f, ", {}", comma(&self.arguments))?;
        }
        f.write_str(")")?;
        if !self.options.is_empty() {
            write!(f, " WITH {}", comma(&self.options))?;
        }
        Ok(())
    }
}

impl fmt::Display for MsSqlFetch<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("FETCH ")?;
        match (self.direction.as_ref(), &self.count) {
            ("", Some(count)) => write!(f, "{count} ")?,
            (direction, Some(count)) => write!(f, "{direction} {count} ")?,
            (direction, None) => write!(f, "{direction} ")?,
        }
        write!(f, "{} {}", if self.from { "FROM" } else { "IN" }, self.name)?;
        if let Some(into) = &self.into {
            write!(f, " INTO {into}")?;
        }
        Ok(())
    }
}

impl fmt::Display for MsSqlExecute<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("EXECUTE")?;
        if let Some(name) = &self.name {
            write!(f, " {name}")?;
        }
        if self.has_parentheses {
            let open = if self.name.is_some() { "(" } else { " (" };
            write!(f, "{open}{})", comma(&self.parameters))?;
        } else if !self.parameters.is_empty() {
            write!(f, " {}", comma(&self.parameters))?;
        }
        if !self.into.is_empty() {
            write!(f, " INTO {}", comma(&self.into))?;
        }
        if !self.using.is_empty() {
            f.write_str(" USING ")?;
            for (i, (expr, alias)) in self.using.iter().enumerate() {
                if i > 0 {
                    f.write_str(", ")?;
                }
                expr.fmt(f)?;
                if let Some(alias) = alias {
                    write!(f, " AS {alias}")?;
                }
            }
        }
        if self.output {
            f.write_str(" OUTPUT")?;
        }
        if self.default {
            f.write_str(" DEFAULT")?;
        }
        Ok(())
    }
}

impl fmt::Display for MsSqlCreateProcedure<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "CREATE {}PROCEDURE {}",
            if self.or_alter { "OR ALTER " } else { "" },
            self.name
        )?;
        if let Some(params) = &self.params {
            write!(f, " ({})", comma(params))?;
        }
        if let Some(language) = &self.language {
            write!(f, " LANGUAGE {language}")?;
        }
        write_as_body(f, &self.body)
    }
}

impl fmt::Display for MsSqlReturns<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Type(data_type) => data_type.fmt(f),
            Self::Table { name, columns } => {
                if let Some(name) = name {
                    write!(f, "{name} ")?;
                }
                write!(f, "TABLE ({})", comma(columns))
            }
        }
    }
}

impl fmt::Display for MsSqlFunctionBody<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Block(statements) => {
                f.write_str("AS")?;
                SpaceOrNewline.fmt(f)?;
                write_block(f, statements)
            }
            Self::ReturnExpr(expr) => write!(f, "AS RETURN {expr}"),
            Self::ReturnSelect(select) => write!(f, "AS RETURN {select}"),
        }
    }
}

impl fmt::Display for MsSqlCreateFunction<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "CREATE {}FUNCTION {}({}) RETURNS {} ",
            if self.or_alter { "OR ALTER " } else { "" },
            self.name,
            comma(&self.params),
            self.returns,
        )?;
        self.body.fmt(f)
    }
}

impl fmt::Display for MsSqlTriggerEvent<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Insert => f.write_str("INSERT"),
            Self::Delete => f.write_str("DELETE"),
            Self::Truncate => f.write_str("TRUNCATE"),
            Self::Update(columns) if columns.is_empty() => f.write_str("UPDATE"),
            Self::Update(columns) => write!(f, "UPDATE OF {}", comma(columns)),
        }
    }
}

impl fmt::Display for MsSqlCreateTrigger<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "CREATE {}TRIGGER {} ON {} {} {}",
            if self.or_alter { "OR ALTER " } else { "" },
            self.name,
            self.table,
            self.period,
            comma(&self.events),
        )?;
        write_as_body(f, &self.body)
    }
}

impl fmt::Display for MsSqlCreateRole<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CREATE ROLE ")?;
        if self.if_not_exists {
            f.write_str("IF NOT EXISTS ")?;
        }
        comma(&self.names).fmt(f)?;
        if let Some(owner) = &self.authorization {
            write!(f, " AUTHORIZATION {owner}")?;
        }
        Ok(())
    }
}

impl fmt::Display for MsSqlAlterRole<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ALTER ROLE {} ", self.name)?;
        match &self.operation {
            MsSqlAlterRoleOp::AddMember(member) => write!(f, "ADD MEMBER {member}"),
            MsSqlAlterRoleOp::DropMember(member) => write!(f, "DROP MEMBER {member}"),
            MsSqlAlterRoleOp::Rename(name) => write!(f, "RENAME TO {name}"),
        }
    }
}

impl fmt::Display for MsSqlDeny<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "DENY {} ON {} TO {}",
            self.privileges,
            self.objects,
            comma(&self.grantees)
        )?;
        match self.behavior {
            Some(DropBehavior::Cascade) => f.write_str(" CASCADE")?,
            Some(DropBehavior::Restrict) => f.write_str(" RESTRICT")?,
            None => {}
        }
        if let Some(grantor) = &self.as_grantor {
            write!(f, " AS {grantor}")?;
        }
        Ok(())
    }
}

fn write_on_off(f: &mut fmt::Formatter<'_>, on: bool) -> fmt::Result {
    f.write_str(if on { "ON" } else { "OFF" })
}

impl fmt::Display for MsSqlSessionParam<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SET ")?;
        match self {
            Self::Generic { names, value } => write!(f, "{} {value}", comma(names)),
            Self::Statistics { topic, on } => {
                write!(f, "STATISTICS {topic} ")?;
                write_on_off(f, *on)
            }
            Self::IdentityInsert { table, on } => {
                write!(f, "IDENTITY_INSERT {table} ")?;
                write_on_off(f, *on)
            }
            Self::Offsets { keywords, on } => {
                write!(f, "OFFSETS {} ", comma(keywords))?;
                write_on_off(f, *on)
            }
        }
    }
}

impl fmt::Display for MsSqlTableOption<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::KeyValue { key, value } => write!(f, "{key} = {value}"),
            Self::Heap => f.write_str("HEAP"),
            Self::ClusteredColumnstore => f.write_str("CLUSTERED COLUMNSTORE INDEX"),
            Self::ClusteredColumnstoreOrder(columns) => {
                write!(f, "CLUSTERED COLUMNSTORE INDEX ORDER ({})", comma(columns))
            }
            Self::ClusteredIndex(columns) => {
                f.write_str("CLUSTERED INDEX (")?;
                for (i, (name, asc)) in columns.iter().enumerate() {
                    if i > 0 {
                        f.write_str(", ")?;
                    }
                    name.fmt(f)?;
                    match asc {
                        Some(true) => f.write_str(" ASC")?,
                        Some(false) => f.write_str(" DESC")?,
                        None => {}
                    }
                }
                f.write_str(")")
            }
            Self::Partition {
                column,
                direction,
                values,
            } => {
                write!(f, "PARTITION ({column} RANGE")?;
                if let Some(direction) = direction {
                    write!(f, " {direction}")?;
                }
                write!(f, " FOR VALUES ({}))", comma(values))
            }
        }
    }
}

impl fmt::Display for MsSqlCreateTable<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} WITH ({})", self.table, comma(&self.options))
    }
}

impl fmt::Display for MsSqlStatement<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Block(statements) => write_block(f, statements),
            Self::TryCatch(marker) => f.write_str(marker),
            Self::If(statement) => statement.fmt(f),
            Self::While(statement) => statement.fmt(f),
            Self::Declare(items) => write!(f, "DECLARE {}", comma(items)),
            Self::Print(expr) => write!(f, "PRINT {expr}"),
            Self::RaisError(statement) => statement.fmt(f),
            Self::Throw(None) => f.write_str("THROW"),
            Self::Throw(Some(args)) => {
                let [number, message, state] = args.as_ref();
                write!(f, "THROW {number}, {message}, {state}")
            }
            Self::WaitFor { delay, expr } => {
                write!(
                    f,
                    "WAITFOR {} {expr}",
                    if *delay { "DELAY" } else { "TIME" }
                )
            }
            Self::Return(None) => f.write_str("RETURN"),
            Self::Return(Some(expr)) => write!(f, "RETURN {expr}"),
            Self::Open(name) => write!(f, "OPEN {name}"),
            Self::Close(None) => f.write_str("CLOSE ALL"),
            Self::Close(Some(name)) => write!(f, "CLOSE {name}"),
            Self::Fetch(statement) => statement.fmt(f),
            Self::Deallocate { prepare, name } => {
                write!(
                    f,
                    "DEALLOCATE {}{name}",
                    if *prepare { "PREPARE " } else { "" }
                )
            }
            Self::Execute(statement) => statement.fmt(f),
            Self::CreateProcedure(statement) => statement.fmt(f),
            Self::CreateFunction(statement) => statement.fmt(f),
            Self::CreateTrigger(statement) => statement.fmt(f),
            Self::CreateRole(statement) => statement.fmt(f),
            Self::AlterRole(statement) => statement.fmt(f),
            Self::Deny(statement) => statement.fmt(f),
            Self::SessionParam(statement) => statement.fmt(f),
            Self::CreateTable(statement) => statement.fmt(f),
            Self::CreateOrAlterView(view) => {
                let rendered = view.to_string();
                let rest = rendered.strip_prefix("CREATE ").unwrap_or(&rendered);
                write!(f, "CREATE OR ALTER {rest}")
            }
        }
    }
}

impl fmt::Display for MsSqlConvert<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}CONVERT({}, {}",
            if self.is_try { "TRY_" } else { "" },
            self.data_type,
            self.expr
        )?;
        if !self.styles.is_empty() {
            write!(f, ", {}", comma(&self.styles))?;
        }
        f.write_str(")")
    }
}

impl fmt::Display for MsSqlJsonArg<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Value(expr) => expr.fmt(f),
            Self::KeyValue(key, value) => write!(f, "{key} : {value}"),
        }
    }
}

impl fmt::Display for MsSqlJsonCall<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}({}", self.name, comma(&self.args))?;
        if let Some(ref clause) = self.null_clause {
            if !self.args.is_empty() {
                f.write_str(" ")?;
            }
            f.write_str(clause)?;
        }
        f.write_str(")")
    }
}

impl fmt::Display for MsSqlExpr<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Convert(convert) => convert.fmt(f),
            Self::JsonCall(call) => call.fmt(f),
            // Written without `write!`, since chains of these can be long.
            Self::Method(method) => {
                method.expr.fmt(f)?;
                f.write_str(".")?;
                method.name.fmt(f)?;
                f.write_str("(")?;
                comma(&method.args).fmt(f)?;
                f.write_str(")")
            }
        }
    }
}

impl fmt::Display for MsSqlTop<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.parenthesized {
            write!(f, "TOP ({})", self.quantity)?;
        } else {
            write!(f, "TOP {}", self.quantity)?;
        }
        if self.percent {
            f.write_str(" PERCENT")?;
        }
        if self.with_ties {
            f.write_str(" WITH TIES")?;
        }
        Ok(())
    }
}

impl fmt::Display for MsSqlTableFactor<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Hinted(hinted) => {
                write!(f, "{} WITH ({})", hinted.table, comma(&hinted.hints))
            }
            Self::OpenJson(openjson) => openjson.fmt(f),
        }
    }
}

impl fmt::Display for MsSqlOpenJsonColumn<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.name, self.data_type)?;
        if let Some(path) = &self.path {
            write!(f, " '{}'", path.text)?;
        }
        if self.as_json {
            f.write_str(" AS JSON")?;
        }
        Ok(())
    }
}

impl fmt::Display for MsSqlOpenJson<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "OPENJSON({}", self.expr)?;
        if let Some(path) = &self.path {
            write!(f, ", {path}")?;
        }
        f.write_str(")")?;
        if !self.columns.is_empty() {
            write!(f, " WITH ({})", comma(&self.columns))?;
        }
        Ok(())
    }
}

impl fmt::Display for MsSqlForClause<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Browse => f.write_str("FOR BROWSE"),
            Self::Xml(xml) => {
                write!(f, "FOR XML {}", xml.mode)?;
                if let Some(name) = &xml.mode_name {
                    write!(f, "('{}')", name.text)?;
                }
                if xml.binary_base64 {
                    f.write_str(", BINARY BASE64")?;
                }
                if xml.type_directive {
                    f.write_str(", TYPE")?;
                }
                if let Some(root) = &xml.root {
                    write!(f, ", ROOT('{}')", root.text)?;
                }
                if xml.elements {
                    f.write_str(", ELEMENTS")?;
                }
                Ok(())
            }
            Self::Json(json) => {
                write!(f, "FOR JSON {}", json.mode)?;
                if let Some(root) = &json.root {
                    write!(f, ", ROOT('{}')", root.text)?;
                }
                if json.include_null_values {
                    f.write_str(", INCLUDE_NULL_VALUES")?;
                }
                if json.without_array_wrapper {
                    f.write_str(", WITHOUT_ARRAY_WRAPPER")?;
                }
                Ok(())
            }
        }
    }
}

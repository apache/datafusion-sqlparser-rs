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

//! Google BigQuery.

use std::borrow::Cow;
use std::fmt;

use crate::PResult;
use crate::ast::{
    AngleArguments, CastStyle, ColumnOption, CreateSchema, CreateTable, CreateView, DataType,
    DataTypeSuffix, Delete, Expr, ExprKind, ExprOption, ExprWithAlias, FunctionArg,
    FunctionOptions, Ident, Insert, Literal, NullTreatment, ObjectName, OrderByItem, PipeOperator,
    Query, SelectItem, SelectItemKind, SetQuantifier, Span, SpecialExpr, Statement, StatementKind,
    StringLiteral, StringPrefix, StructField, StructFieldOption, TableFactor, TableRef,
    TableWithJoins, TimeTravel, TypedString, WildcardOptions, WindowOver,
};
use crate::dialect::builtin::{BuiltinExpr, BuiltinSelect, BuiltinStatement, BuiltinTableFactor};
use crate::dialect::{Builtin, Dialect, databricks, snowflake, spark};
use crate::display::{
    Indent, SpaceOrNewline, comma, write_columns, write_create_flags, write_if_not_exists,
    write_where_clause,
};
use crate::grammar::{self, prec};
use crate::lexer::{self, LexResult, Lexeme, Token, TokenKind, quoted};
use crate::parser::{InfixRule, Parser};

#[derive(Debug, Clone, PartialEq)]
pub struct BigQuery;

/// Statements only Google BigQuery has.
#[derive(Debug, Clone, PartialEq)]
pub enum BigQueryStatement<'a> {
    CreateTable(Box<BigQueryCreateTable<'a>>),
    CreateView(Box<BigQueryCreateView<'a>>),
    CreateFunction(Box<BigQueryCreateFunction<'a>>),
    CreateSnapshot(Box<BigQueryCreateSnapshot<'a>>),
    CreateSchema(Box<BigQueryCreateSchema<'a>>),
    /// `DELETE table [WHERE ...]`, with no `FROM`.
    DeleteWithoutFrom(Box<Delete<'a, Builtin>>),
    /// `INSERT table ...`, with no `INTO`.
    InsertWithoutInto(Box<Insert<'a, Builtin>>),
    ExportData(Box<BigQueryExportData<'a>>),
    AlterSchema(Box<BigQueryAlterSchema<'a>>),
    Block(Box<BigQueryBlock<'a>>),
    Declare(Box<BigQueryDeclare<'a>>),
    /// `RAISE [USING MESSAGE = message]`, or `RAISE value`.
    Raise(Option<BigQueryRaise<'a>>),
    /// `SET (a, b) = (x, y)`
    SetTuple {
        names: Vec<Ident<'a>>,
        values: Vec<Expr<'a, Builtin>>,
    },
    ExecuteImmediate(Box<BigQueryExecuteImmediate<'a>>),
    /// `ASSERT condition [AS message]`
    Assert {
        condition: Expr<'a, Builtin>,
        message: Option<Expr<'a, Builtin>>,
    },
}

/// `CREATE [OR REPLACE] [TEMPORARY] [EXTERNAL] TABLE` with clauses only
/// BigQuery has. Without them, the statement is the core [`CreateTable`].
#[derive(Debug, Clone, PartialEq)]
pub struct BigQueryCreateTable<'a> {
    pub table: CreateTable<'a, Builtin>,
    pub external: bool,
    /// `LIKE` or `CLONE`, with its table.
    pub source: Option<(Cow<'a, str>, ObjectName<'a>)>,
    pub connection: Option<ObjectName<'a>>,
    pub partition_by: Option<Expr<'a, Builtin>>,
    pub cluster_by: Vec<Expr<'a, Builtin>>,
    pub options: Option<Vec<ExprOption<'a, Builtin>>>,
}

/// `CREATE VIEW` with `OPTIONS`.
#[derive(Debug, Clone, PartialEq)]
pub struct BigQueryCreateView<'a> {
    pub or_replace: bool,
    pub temporary: bool,
    pub materialized: bool,
    pub if_not_exists: bool,
    pub name: ObjectName<'a>,
    pub columns: Vec<BigQueryViewColumn<'a>>,
    pub options: Option<Vec<ExprOption<'a, Builtin>>>,
    pub query: Box<Query<'a, Builtin>>,
}

/// A view column and any `OPTIONS` written after it.
#[derive(Debug, Clone, PartialEq)]
pub struct BigQueryViewColumn<'a> {
    pub name: Ident<'a>,
    pub options: Option<Vec<ExprOption<'a, Builtin>>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BigQueryFunctionArg<'a> {
    pub name: Ident<'a>,
    pub data_type: DataType<'a>,
}

/// `CREATE [OR REPLACE] [TEMPORARY] FUNCTION`.
#[derive(Debug, Clone, PartialEq)]
pub struct BigQueryCreateFunction<'a> {
    pub or_replace: bool,
    pub temporary: bool,
    pub if_not_exists: bool,
    pub name: ObjectName<'a>,
    pub args: Vec<BigQueryFunctionArg<'a>>,
    pub returns: Option<DataType<'a>>,
    /// `Some(false)` for `NOT DETERMINISTIC`.
    pub deterministic: Option<bool>,
    pub language: Option<Ident<'a>>,
    pub remote_connection: Option<ObjectName<'a>>,
    pub options: Option<Vec<ExprOption<'a, Builtin>>>,
    /// True if `OPTIONS` was written after the body.
    pub options_after_body: bool,
    pub body: Option<Expr<'a, Builtin>>,
}

/// `CREATE SNAPSHOT TABLE name CLONE source ...`.
#[derive(Debug, Clone, PartialEq)]
pub struct BigQueryCreateSnapshot<'a> {
    pub if_not_exists: bool,
    pub name: ObjectName<'a>,
    pub clone: ObjectName<'a>,
    pub for_system_time: Option<Expr<'a, Builtin>>,
    pub options: Option<Vec<ExprOption<'a, Builtin>>>,
}

/// `CREATE SCHEMA` with `DEFAULT COLLATE` or `OPTIONS`.
#[derive(Debug, Clone, PartialEq)]
pub struct BigQueryCreateSchema<'a> {
    pub if_not_exists: bool,
    pub name: ObjectName<'a>,
    pub default_collate: Option<Expr<'a, Builtin>>,
    pub options: Option<Vec<ExprOption<'a, Builtin>>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BigQueryExportData<'a> {
    pub connection: Option<ObjectName<'a>>,
    pub options: Vec<ExprOption<'a, Builtin>>,
    pub query: Box<Query<'a, Builtin>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BigQueryAlterSchema<'a> {
    pub if_exists: bool,
    pub name: ObjectName<'a>,
    pub operation: BigQueryAlterSchemaOp<'a>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum BigQueryAlterSchemaOp<'a> {
    SetDefaultCollate(Expr<'a, Builtin>),
    AddReplica {
        replica: Ident<'a>,
        options: Option<Vec<ExprOption<'a, Builtin>>>,
    },
    DropReplica(Ident<'a>),
    SetOptions(Vec<ExprOption<'a, Builtin>>),
}

/// `BEGIN ...; [EXCEPTION WHEN name THEN ...;] END`
#[derive(Debug, Clone, PartialEq)]
pub struct BigQueryBlock<'a> {
    pub statements: Vec<Statement<'a, Builtin>>,
    /// True if `EXCEPTION` was written, even with no handlers after it.
    pub has_exception: bool,
    pub handlers: Vec<BigQueryHandler<'a>>,
}

/// `WHEN name [OR name ...] THEN statements`.
#[derive(Debug, Clone, PartialEq)]
pub struct BigQueryHandler<'a> {
    pub conditions: Vec<Ident<'a>>,
    pub statements: Vec<Statement<'a, Builtin>>,
}

/// `DECLARE a, b type [DEFAULT expr]`
#[derive(Debug, Clone, PartialEq)]
pub struct BigQueryDeclare<'a> {
    pub names: Vec<Ident<'a>>,
    pub data_type: Option<DataType<'a>>,
    pub default: Option<Expr<'a, Builtin>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum BigQueryRaise<'a> {
    UsingMessage(Expr<'a, Builtin>),
    Value(Expr<'a, Builtin>),
}

/// `EXECUTE IMMEDIATE expr [INTO a, b] [USING x [AS y], ...]`
#[derive(Debug, Clone, PartialEq)]
pub struct BigQueryExecuteImmediate<'a> {
    pub query: Expr<'a, Builtin>,
    pub into: Vec<Ident<'a>>,
    pub using: Vec<ExprWithAlias<'a, Builtin>>,
}

/// Expressions only Google BigQuery has.
#[derive(Debug, Clone, PartialEq)]
pub enum BigQueryExpr<'a> {
    /// `[a, b]`
    Array(Vec<Expr<'a, Builtin>>),
    Struct(Box<BigQueryStruct<'a>>),
    /// `expr.*`, with what can follow a wildcard.
    Star(Box<BigQueryStar<'a>>),
    InUnnest(Box<BigQueryInUnnest<'a>>),
    /// `CAST(expr AS ty FORMAT 'f' [AT TIME ZONE 'z'])`
    CastFormat(Box<BigQueryCastFormat<'a>>),
    /// `EXTRACT(WEEK(weekday) FROM expr)`
    ExtractWeek(Box<BigQueryExtractWeek<'a>>),
    /// A call with clauses inside the parentheses that the core has no place for.
    Call(Box<BigQueryCall<'a>>),
    /// A call whose single argument is a query: `ARRAY(SELECT ...)`.
    SubqueryCall(Box<BigQuerySubqueryCall<'a>>),
}

/// `STRUCT<...>(values)` or `STRUCT(values)`.
#[derive(Debug, Clone, PartialEq)]
pub struct BigQueryStruct<'a> {
    pub ty: Option<DataType<'a>>,
    /// Field names can only be given with `AS` in the typeless form.
    pub values: Vec<ExprWithAlias<'a, Builtin>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BigQueryStar<'a> {
    pub expr: Expr<'a, Builtin>,
    pub options: Option<Box<WildcardOptions<'a, Builtin>>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BigQueryInUnnest<'a> {
    pub expr: Expr<'a, Builtin>,
    pub array: Expr<'a, Builtin>,
    pub negated: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BigQueryCastFormat<'a> {
    pub expr: Expr<'a, Builtin>,
    pub ty: DataType<'a>,
    pub safe: bool,
    pub format: StringLiteral<'a>,
    pub time_zone: Option<StringLiteral<'a>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BigQueryExtractWeek<'a> {
    pub weekday: Ident<'a>,
    pub expr: Expr<'a, Builtin>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BigQueryCall<'a> {
    pub name: ObjectName<'a>,
    pub quantifier: SetQuantifier,
    pub args: Vec<FunctionArg<'a, Builtin>>,
    /// Written inside the parentheses.
    pub null_treatment: Option<NullTreatment>,
    pub order_by: Vec<OrderByItem<'a, Builtin>>,
    pub limit: Option<Expr<'a, Builtin>>,
    /// `HAVING MAX expr` (true) or `HAVING MIN expr` (false).
    pub having: Option<(bool, Expr<'a, Builtin>)>,
    pub over: Option<WindowOver<'a, Builtin>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BigQuerySubqueryCall<'a> {
    pub name: ObjectName<'a>,
    pub query: Query<'a, Builtin>,
}

/// Table factors only Google BigQuery has.
#[derive(Debug, Clone, PartialEq)]
pub enum BigQueryTableFactor<'a> {
    /// `UNNEST(...) [AS alias] WITH OFFSET [AS alias]`
    WithOffset(Box<BigQueryWithOffset<'a>>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct BigQueryWithOffset<'a> {
    pub table: TableRef<'a, Builtin>,
    pub alias: Option<Ident<'a>>,
}

impl<'a> From<BigQueryExpr<'a>> for BuiltinExpr<'a> {
    fn from(expr: BigQueryExpr<'a>) -> Self {
        Self::BigQuery(expr)
    }
}

fn ext<'a>(expr: BigQueryExpr<'a>) -> Expr<'a, Builtin> {
    ExprKind::Ext(BuiltinExpr::BigQuery(expr)).into()
}

fn statement<'a>(statement: BigQueryStatement<'a>) -> Statement<'a, Builtin> {
    StatementKind::Ext(BuiltinStatement::BigQuery(statement)).into()
}

#[inline]
fn is_name_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

/// Lexes text in tripled quotes, starting at offset `start` of `rest`.
fn triple_quoted(rest: &str, start: usize, quote: u8, backslash: bool) -> LexResult {
    let bytes = rest.as_bytes();
    let mut i = start + 3;
    while i + 2 < bytes.len() {
        if backslash && bytes[i] == b'\\' {
            i += 2;
        } else if bytes[i..i + 3] == [quote, quote, quote] {
            return Ok(Lexeme {
                kind: TokenKind::String,
                len: i + 3,
            });
        } else {
            i += 1;
        }
    }
    Err("unterminated quoted text")
}

/// Lexes a string whose opening quote is at offset `start` of `rest`, leaving
/// any prefix before it in the token.
fn string_after(rest: &str, start: usize, backslash: bool) -> LexResult {
    let bytes = rest.as_bytes();
    let quote = bytes[start];
    if bytes.get(start + 1) == Some(&quote) && bytes.get(start + 2) == Some(&quote) {
        return triple_quoted(rest, start, quote, backslash);
    }
    let lex = if backslash {
        lexer::quoted_with_backslash
    } else {
        quoted
    };
    lex(&rest[start..], TokenKind::String).map(|lexeme| Lexeme {
        kind: TokenKind::String,
        len: start + lexeme.len,
    })
}

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
        [b'\'' | b'"', ..] => Some(string_after(rest, 0, true)),
        [b'b' | b'B', b'\'' | b'"', ..] => Some(string_after(rest, 1, true)),
        [b'r' | b'R', b'\'' | b'"', ..] => Some(string_after(rest, 1, false)),
        // `@name` and `@@name` are variables, read as names.
        [b'@', ..] => {
            let ats = bytes.iter().take_while(|b| **b == b'@').count();
            let len = bytes[ats..]
                .iter()
                .take_while(|b| is_name_byte(**b))
                .count();
            (len > 0).then_some(Ok(Lexeme {
                kind: TokenKind::Word,
                len: ats + len,
            }))
        }
        [b'|', b'>', ..] => op(2),
        // Type arguments close one at a time, as in `ARRAY<ARRAY<INT64>>`.
        [b'>', b'>', ..] => op(1),
        [b':', next, ..] if !next.is_ascii_alphanumeric() && *next != b'_' && *next != b':' => {
            op(1)
        }
        [b, ..] if !b.is_ascii() => Some(Err("unexpected character")),
        [b, ..] if b.is_ascii_alphabetic() || *b == b'_' => {
            let len = bytes.iter().take_while(|b| is_name_byte(**b)).count();
            if bytes.get(len).is_some_and(|b| !b.is_ascii()) {
                return Some(Err("unexpected character"));
            }
            None
        }
        _ => None,
    }
}

/// Parses the tripled or prefixed strings that [`lex_token`] accepts.
pub fn string_literal(token: Token<'_>) -> StringLiteral<'_> {
    let text = token.text;
    let (prefix, body) = match text.as_bytes()[0] {
        b'b' | b'B' => (StringPrefix::Byte, &text[1..]),
        b'r' | b'R' => (StringPrefix::Raw, &text[1..]),
        _ => (StringPrefix::None, text),
    };
    let quote = char::from(body.as_bytes()[0]);
    let triple = body.len() >= 6
        && body.as_bytes()[1] == body.as_bytes()[0]
        && body.as_bytes()[2] == body.as_bytes()[0];
    let len = if triple { 3 } else { 1 };
    StringLiteral {
        national: false,
        text: Cow::Borrowed(&body[len..body.len() - len]),
        quote,
        backslash_escapes: prefix != StringPrefix::Raw,
        prefix,
        triple,
    }
}

/// Words that cannot be a table alias without `AS`.
const RESERVED_FOR_TABLE_ALIAS: [&str; 59] = [
    "WITH",
    "EXPLAIN",
    "ANALYZE",
    "SELECT",
    "WHERE",
    "GROUP",
    "SORT",
    "HAVING",
    "ORDER",
    "PIVOT",
    "UNPIVOT",
    "TOP",
    "LATERAL",
    "VIEW",
    "LIMIT",
    "OFFSET",
    "FETCH",
    "UNION",
    "EXCEPT",
    "INTERSECT",
    "MINUS",
    "ON",
    "JOIN",
    "INNER",
    "CROSS",
    "FULL",
    "LEFT",
    "RIGHT",
    "NATURAL",
    "USING",
    "CLUSTER",
    "DISTRIBUTE",
    "GLOBAL",
    "ANTI",
    "SEMI",
    "RETURNING",
    "OUTPUT",
    "ASOF",
    "MATCH_CONDITION",
    "OUTER",
    "SET",
    "QUALIFY",
    "WINDOW",
    "END",
    "FOR",
    "PARTITION",
    "ARRAY",
    "PREWHERE",
    "SETTINGS",
    "FORMAT",
    "START",
    "CONNECT",
    "MATCH_RECOGNIZE",
    "SAMPLE",
    "TABLESAMPLE",
    "FROM",
    "OPEN",
    "AS",
    "INTO",
];

/// Words that cannot be a column alias without `AS`.
const RESERVED_FOR_COLUMN_ALIAS: [&str; 15] = [
    "WITH",
    "SELECT",
    "WHERE",
    "GROUP",
    "HAVING",
    "ORDER",
    "LATERAL",
    "LIMIT",
    "FETCH",
    "UNION",
    "EXCEPT",
    "INTERSECT",
    "FROM",
    "INTO",
    "END",
];

fn is_reserved_for_column_alias(word: &str) -> bool {
    RESERVED_FOR_COLUMN_ALIAS
        .iter()
        .any(|reserved| reserved.eq_ignore_ascii_case(word))
}

fn is_name(token: Token<'_>) -> bool {
    matches!(token.kind, TokenKind::Word | TokenKind::QuotedIdent)
}

/// An identifier written as a string: `'name'`, in a place a name is expected.
fn ident_from_string(token: Token<'_>) -> Ident<'_> {
    let literal = string_literal(token);
    Ident {
        text: literal.text,
        quote: Some(literal.quote),
        span: token.span,
    }
}

/// Parses a name that can also be written as a string.
fn parse_name_or_string<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Ident<'a>> {
    let token = p.peek();
    if token.kind == TokenKind::String {
        p.advance();
        return Ok(ident_from_string(token));
    }
    p.parse_name_part()
}

/// Parses a part of a name after a dot, which can be written as a string.
fn parse_name_or_string_part<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Ident<'a>> {
    let token = p.peek();
    if token.kind == TokenKind::String {
        p.advance();
        return Ok(ident_from_string(token));
    }
    grammar::parse_name_part(p)
}

fn expect_adjacent<'a, D: Dialect>(p: &Parser<'a, D>, end: u32) -> PResult<()> {
    if p.peek().span.start == end {
        Ok(())
    } else {
        Err(p.expected("continuation of hyphenated identifier"))
    }
}

/// Parses one part of a table name, which can hold hyphens when unquoted:
/// `my-project`.
fn parse_table_name_part<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
    first_part: bool,
) -> PResult<Ident<'a>> {
    if p.peek().kind == TokenKind::String {
        return parse_name_or_string(p);
    }
    if p.peek().kind != TokenKind::Word {
        return if first_part {
            p.parse_ident()
        } else {
            p.parse_name_part()
        };
    }
    let first = if first_part {
        p.parse_ident()?
    } else {
        p.parse_name_part()?
    };
    let (start, mut end) = (first.span.start, first.span.end);
    while p.at_op("-") && p.peek().span.start == end {
        end = p.advance().span.end;
        expect_adjacent(p, end)?;
        let token = p.peek();
        let continues = token.kind == TokenKind::Word
            || (token.kind == TokenKind::Number && token.text.bytes().all(|b| b.is_ascii_digit()));
        if !continues {
            return Err(p.expected("continuation of hyphenated identifier"));
        }
        end = p.advance().span.end;
    }
    if end == first.span.end {
        return Ok(first);
    }
    let span = Span { start, end };
    Ok(Ident {
        text: p.text(span).into(),
        quote: None,
        span,
    })
}

/// Parses a table's name, splitting quoted parts on the dots inside them.
pub fn parse_table_name<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<ObjectName<'a>> {
    let mut parts = vec![parse_table_name_part(p, true)?];
    while p.eat_op(".") {
        parts.push(parse_table_name_part(p, false)?);
    }
    if parts.iter().any(|part| part.text.contains('.')) {
        parts = parts
            .into_iter()
            .flat_map(|part| {
                let pieces: Vec<Cow<'a, str>> = match part.text {
                    Cow::Borrowed(text) => text.split('.').map(Cow::Borrowed).collect(),
                    Cow::Owned(text) => text.split('.').map(|t| Cow::Owned(t.to_owned())).collect(),
                };
                pieces.into_iter().map(move |text| Ident {
                    text,
                    quote: part.quote,
                    span: part.span,
                })
            })
            .collect();
    }
    Ok(ObjectName { parts })
}

fn parse_option<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<ExprOption<'a, Builtin>> {
    let name = p.parse_name_part()?;
    p.expect_op("=")?;
    let value = p.parse_expr()?;
    Ok(ExprOption { name, value })
}

/// Parses `(key = value, ...)`, which can be empty only if `allow_empty`.
fn parse_option_list<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
    allow_empty: bool,
) -> PResult<Vec<ExprOption<'a, Builtin>>> {
    p.expect_op("(")?;
    if allow_empty && p.eat_op(")") {
        return Ok(Vec::new());
    }
    let options = p.parse_comma_separated(parse_option)?;
    p.expect_op(")")?;
    Ok(options)
}

/// Parses `OPTIONS(...)` if it is here.
fn parse_optional_options<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Option<Vec<ExprOption<'a, Builtin>>>> {
    if !(p.at_keyword("OPTIONS") && p.peek_nth(1).is_op("(")) {
        return Ok(None);
    }
    p.advance();
    parse_option_list(p, true).map(Some)
}

/// Parses a column's `OPTIONS(...)`, and otherwise the core's column options.
pub fn parse_column_option<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Option<ColumnOption<'a, Builtin>>> {
    if let Some(options) = parse_optional_options(p)? {
        return Ok(Some(ColumnOption::Options(options)));
    }
    grammar::parse_column_option(p)
}

/// Parses the elements of a column list, which can end with a comma.
fn parse_column_elements<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<grammar::ColumnList<'a, Builtin>> {
    p.expect_op("(")?;
    let mut columns = Vec::new();
    let mut constraints = Vec::new();
    if p.eat_op(")") {
        return Ok((columns, constraints));
    }
    loop {
        if let Some(constraint) = D::parse_table_constraint(p)? {
            constraints.push(constraint);
        } else {
            columns.push(D::parse_column_def(p)?);
        }
        if !p.eat_op(",") || p.at_op(")") {
            break;
        }
    }
    p.expect_op(")")?;
    Ok((columns, constraints))
}

/// Parses a data type, in which `STRUCT<...>` and `ARRAY<...>` nest.
pub fn parse_data_type<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<DataType<'a>> {
    let name = p.parse_ident()?;
    // `ANY TYPE`, the type of a templated function parameter.
    if name.quote.is_none() && name.text.eq_ignore_ascii_case("ANY") && p.at_keyword("TYPE") {
        p.advance();
        return Ok(DataType {
            qualifier: Vec::new(),
            name: Ident {
                text: "ANY TYPE".into(),
                quote: None,
                span: name.span,
            },
            angle: None,
            args: Vec::new(),
            suffix: DataTypeSuffix::None,
            array: crate::ast::ArrayType::None,
        });
    }
    let mut data_type = grammar::parse_data_type_after_name(p, name.clone())?;
    let is_array = name.quote.is_none() && name.text.eq_ignore_ascii_case("ARRAY");
    let is_struct = name.quote.is_none() && name.text.eq_ignore_ascii_case("STRUCT");
    if is_array && !p.at_op("<") {
        return Err(p.expected("< after ARRAY"));
    }
    if (is_array || is_struct) && p.at_op("<") {
        p.advance();
        let arguments = if is_struct {
            AngleArguments::Fields(p.parse_comma_separated(parse_struct_field)?)
        } else {
            AngleArguments::Types(vec![p.nested(D::parse_data_type)?])
        };
        p.expect_angle_close()?;
        data_type.angle = Some(Box::new(arguments));
    }
    Ok(data_type)
}

/// True if a struct field's name comes before its type.
fn at_named_field<'a, D: Dialect>(p: &Parser<'a, D>) -> bool {
    let (first, second) = (p.peek(), p.peek_nth(1));
    if !is_name(first) {
        return false;
    }
    if second.is_word("OPTIONS") && p.peek_nth(2).is_op("(") {
        return false;
    }
    is_name(second)
        || second.is_op(":")
        || (second.kind == TokenKind::Placeholder && second.text.starts_with(':'))
}

fn parse_struct_field<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<StructField<'a>> {
    let named = at_named_field(p);
    let name = if named {
        Some(p.parse_name_part()?)
    } else {
        None
    };
    let token = p.peek();
    // With no space, the lexer reads `:INT` as a parameter.
    // A colon is an accepted spelling, which is not kept: `x: INT` renders as `x INT`.
    if named {
        p.eat_op(":");
    }
    let data_type = if named && token.kind == TokenKind::Placeholder && token.text.starts_with(':')
    {
        p.advance();
        let type_name = Ident {
            text: Cow::Borrowed(&token.text[1..]),
            quote: None,
            span: Span {
                start: token.span.start + 1,
                end: token.span.end,
            },
        };
        p.nested(|p| grammar::parse_data_type_after_name(p, type_name))?
    } else {
        p.nested(D::parse_data_type)?
    };
    let options = parse_struct_field_options(p)?;
    Ok(StructField {
        not_null: false,
        name,
        colon: false,
        data_type,
        comment: None,
        options,
    })
}

/// Parses `OPTIONS(key = literal, ...)` on a struct field.
fn parse_struct_field_options<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Vec<StructFieldOption<'a>>> {
    if !(p.at_keyword("OPTIONS") && p.peek_nth(1).is_op("(")) {
        return Ok(Vec::new());
    }
    p.advance();
    p.advance();
    if p.eat_op(")") {
        return Ok(Vec::new());
    }
    let options = p.parse_comma_separated(|p| {
        let key = p.parse_name_part()?;
        p.expect_op("=")?;
        let value = parse_field_option_value(p)?;
        Ok(StructFieldOption { key, value })
    })?;
    p.expect_op(")")?;
    Ok(options)
}

fn parse_field_option_value<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Literal<'a>> {
    let token = p.peek();
    let literal = match token.kind {
        TokenKind::String => Literal::String(D::string_literal(token)),
        TokenKind::Number => Literal::Number(token.text.into()),
        TokenKind::Word if token.is_word("TRUE") => Literal::Boolean(true),
        TokenKind::Word if token.is_word("FALSE") => Literal::Boolean(false),
        TokenKind::Word if token.is_word("NULL") => Literal::Null,
        _ => return Err(p.expected("a literal option value")),
    };
    p.advance();
    Ok(literal)
}

/// Parses `(arg type, ...)` after a function's name.
fn parse_function_args<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Vec<BigQueryFunctionArg<'a>>> {
    p.expect_op("(")?;
    if p.eat_op(")") {
        return Ok(Vec::new());
    }
    let args = p.parse_comma_separated(|p| {
        let name = parse_name_or_string_part(p)?;
        let data_type = D::parse_data_type(p)?;
        Ok(BigQueryFunctionArg { name, data_type })
    })?;
    p.expect_op(")")?;
    Ok(args)
}

fn parse_create_flags<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<(bool, bool)> {
    p.expect_keyword("CREATE")?;
    let or_replace = grammar::parse_or_replace(p)?;
    let temporary = p.eat_keyword("TEMPORARY") || p.eat_keyword("TEMP");
    Ok((or_replace, temporary))
}

fn parse_create_function<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<BigQueryCreateFunction<'a>> {
    let (or_replace, temporary) = parse_create_flags(p)?;
    p.expect_keyword("FUNCTION")?;
    let if_not_exists = grammar::parse_if_not_exists(p)?;
    let name = grammar::parse_object_name(p)?;
    let args = parse_function_args(p)?;
    let returns = if p.eat_keyword("RETURNS") {
        Some(D::parse_data_type(p)?)
    } else {
        None
    };
    let deterministic = if p.eat_keyword("DETERMINISTIC") {
        Some(true)
    } else if p.at_keyword("NOT") && p.peek_nth(1).is_word("DETERMINISTIC") {
        p.advance();
        p.advance();
        Some(false)
    } else {
        None
    };
    let language = if p.eat_keyword("LANGUAGE") {
        Some(p.parse_ident()?)
    } else {
        None
    };
    let remote_connection = if p.at_keyword("REMOTE") {
        p.advance();
        p.expect_keyword("WITH")?;
        p.expect_keyword("CONNECTION")?;
        Some(grammar::parse_object_name(p)?)
    } else {
        None
    };
    let mut options = parse_optional_options(p)?;
    let mut options_after_body = false;
    let body = if remote_connection.is_none() {
        p.expect_keyword("AS")?;
        let body = p.parse_expr()?;
        if options.is_none() {
            options = parse_optional_options(p)?;
            options_after_body = options.is_some();
        }
        Some(body)
    } else {
        None
    };
    Ok(BigQueryCreateFunction {
        or_replace,
        temporary,
        if_not_exists,
        name,
        args,
        returns,
        deterministic,
        language,
        remote_connection,
        options,
        options_after_body,
        body,
    })
}

fn parse_create_snapshot<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<BigQueryCreateSnapshot<'a>> {
    p.expect_keyword("SNAPSHOT")?;
    p.expect_keyword("TABLE")?;
    let if_not_exists = grammar::parse_if_not_exists(p)?;
    let name = parse_table_name(p)?;
    p.expect_keyword("CLONE")?;
    let clone = parse_table_name(p)?;
    let for_system_time = if at_system_time(p) {
        consume_system_time(p);
        Some(p.parse_expr()?)
    } else {
        None
    };
    let options = parse_optional_options(p)?;
    Ok(BigQueryCreateSnapshot {
        if_not_exists,
        name,
        clone,
        for_system_time,
        options,
    })
}

fn parse_create_table<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Statement<'a, Builtin>> {
    let (or_replace, temporary) = parse_create_flags(p)?;
    if p.at_keyword("SNAPSHOT") {
        let snapshot = parse_create_snapshot(p)?;
        return Ok(statement(BigQueryStatement::CreateSnapshot(Box::new(
            snapshot,
        ))));
    }
    let external = p.eat_keyword("EXTERNAL");
    p.expect_keyword("TABLE")?;
    let if_not_exists = grammar::parse_if_not_exists(p)?;
    let name = parse_table_name(p)?;
    let source = ["LIKE", "CLONE"]
        .into_iter()
        .find(|keyword| p.at_keyword(keyword));
    let source = match source {
        Some(keyword) => {
            p.advance();
            Some((keyword, parse_table_name(p)?))
        }
        None => None,
    };
    let (columns, constraints) = if p.at_op("(") {
        parse_column_elements(p)?
    } else {
        (Vec::new(), Vec::new())
    };
    let connection = if external && p.at_keyword("WITH") && p.peek_nth(1).is_word("CONNECTION") {
        p.advance();
        p.advance();
        Some(grammar::parse_object_name(p)?)
    } else {
        None
    };
    let partition_by = if p.at_keyword("PARTITION") && p.peek_nth(1).is_word("BY") {
        p.advance();
        p.advance();
        Some(p.parse_expr()?)
    } else {
        None
    };
    let cluster_by = if p.at_keyword("CLUSTER") && p.peek_nth(1).is_word("BY") {
        p.advance();
        p.advance();
        p.parse_comma_separated(Parser::parse_expr)?
    } else {
        Vec::new()
    };
    let options = parse_optional_options(p)?;
    let query = if p.eat_keyword("AS") {
        Some(Box::new(D::parse_query(p)?))
    } else {
        None
    };
    let plain = !external
        && source.is_none()
        && connection.is_none()
        && partition_by.is_none()
        && cluster_by.is_empty()
        && options.is_none();
    if plain && columns.is_empty() && query.is_none() {
        return Err(p.expected("column definitions, table clauses or AS"));
    }
    let table = CreateTable {
        or_replace,
        temporary,
        name,
        if_not_exists,
        columns,
        constraints,
        query,
    };
    if plain {
        return Ok(StatementKind::CreateTable(Box::new(table)).into());
    }
    Ok(statement(BigQueryStatement::CreateTable(Box::new(
        BigQueryCreateTable {
            table,
            external,
            source: source.map(|(kind, name)| (kind.into(), name)),
            connection,
            partition_by,
            cluster_by,
            options,
        },
    ))))
}

fn parse_view_column<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<BigQueryViewColumn<'a>> {
    let name = p.parse_ident()?;
    let options = parse_optional_options(p)?;
    Ok(BigQueryViewColumn { name, options })
}

fn parse_create_view<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Statement<'a, Builtin>> {
    let (or_replace, temporary) = parse_create_flags(p)?;
    let materialized = p.eat_keyword("MATERIALIZED");
    p.expect_keyword("VIEW")?;
    let if_not_exists = grammar::parse_if_not_exists(p)?;
    let name = parse_table_name(p)?;
    let columns = if p.eat_op("(") {
        if p.eat_op(")") {
            Vec::new()
        } else {
            let columns = grammar::parse_list_with_trailing_comma(p, parse_view_column)?;
            p.expect_op(")")?;
            columns
        }
    } else {
        Vec::new()
    };
    let options = parse_optional_options(p)?;
    p.expect_keyword("AS")?;
    let query = Box::new(D::parse_query(p)?);
    let plain = options.is_none() && columns.iter().all(|column| column.options.is_none());
    if plain {
        return Ok(StatementKind::CreateView(Box::new(CreateView {
            or_replace,
            temporary,
            materialized,
            if_not_exists,
            name,
            columns: columns.into_iter().map(|column| column.name).collect(),
            with: Vec::new(),
            query,
            no_schema_binding: false,
        }))
        .into());
    }
    Ok(statement(BigQueryStatement::CreateView(Box::new(
        BigQueryCreateView {
            or_replace,
            temporary,
            materialized,
            if_not_exists,
            name,
            columns,
            options,
            query,
        },
    ))))
}

fn parse_create_schema<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Statement<'a, Builtin>> {
    p.expect_keyword("CREATE")?;
    p.expect_keyword("SCHEMA")?;
    let if_not_exists = grammar::parse_if_not_exists(p)?;
    let name = parse_table_name(p)?;
    let default_collate = if p.at_keyword("DEFAULT") {
        p.advance();
        p.expect_keyword("COLLATE")?;
        Some(p.parse_expr()?)
    } else {
        None
    };
    let options = parse_optional_options(p)?;
    if default_collate.is_none() && options.is_none() {
        return Ok(StatementKind::CreateSchema(Box::new(CreateSchema {
            or_replace: false,
            if_not_exists,
            name: Some(name),
            authorization: None,
            clone: None,
        }))
        .into());
    }
    Ok(statement(BigQueryStatement::CreateSchema(Box::new(
        BigQueryCreateSchema {
            if_not_exists,
            name,
            default_collate,
            options,
        },
    ))))
}

/// Parses `CREATE`, handling BigQuery's tables, views and functions.
pub fn parse_create<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Statement<'a, Builtin>> {
    let mut n = 1;
    if p.peek_nth(n).is_word("OR") {
        n += 2;
    }
    if p.peek_nth(n).is_word("TEMPORARY") || p.peek_nth(n).is_word("TEMP") {
        n += 1;
    }
    let object = p.peek_nth(n);
    if object.is_word("FUNCTION") {
        let function = parse_create_function(p)?;
        return Ok(statement(BigQueryStatement::CreateFunction(Box::new(
            function,
        ))));
    }
    if object.is_word("TABLE") || object.is_word("EXTERNAL") || object.is_word("SNAPSHOT") {
        return parse_create_table(p);
    }
    if object.is_word("VIEW") || object.is_word("MATERIALIZED") {
        return parse_create_view(p);
    }
    if n == 1 && object.is_word("SCHEMA") {
        return parse_create_schema(p);
    }
    grammar::parse_create(p)
}

/// Parses `(a, b)` and an optional value list after `SET`.
fn parse_set_tuple<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<BigQueryStatement<'a>> {
    p.expect_keyword("SET")?;
    let names = grammar::parse_ident_list(p)?;
    if !p.eat_op("=") && !p.eat_keyword("TO") {
        return Err(p.expected("= or TO"));
    }
    p.expect_op("(")?;
    let values = p.parse_comma_separated(|p| {
        if grammar::query_starts_at(p, 0) {
            p.spanned_expr(|p| {
                let query = p.nested(D::parse_query)?;
                Ok(ExprKind::Subquery(Box::new(query)).into())
            })
        } else {
            p.parse_expr()
        }
    })?;
    p.expect_op(")")?;
    Ok(BigQueryStatement::SetTuple { names, values })
}

/// Parses statements up to one of `terminators`, each ended by a semicolon.
fn parse_statement_list<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
    terminators: &[&str],
) -> PResult<Vec<Statement<'a, Builtin>>> {
    let mut statements = Vec::new();
    loop {
        if p.at_eof() || terminators.iter().any(|word| p.at_keyword(word)) {
            return Ok(statements);
        }
        statements.push(p.nested(Parser::parse_statement)?);
        p.expect_op(";")?;
    }
}

/// Parses `BEGIN ...; [EXCEPTION WHEN ...] END`.
fn parse_block<'a, D: Dialect<Ext = Builtin>>(p: &mut Parser<'a, D>) -> PResult<BigQueryBlock<'a>> {
    p.expect_keyword("BEGIN")?;
    let statements = parse_statement_list(p, &["EXCEPTION", "END"])?;
    let has_exception = p.eat_keyword("EXCEPTION");
    let mut handlers = Vec::new();
    if has_exception {
        while !p.at_keyword("END") {
            p.expect_keyword("WHEN")?;
            let mut conditions = Vec::new();
            while !p.eat_keyword("THEN") {
                conditions.push(p.parse_ident()?);
                p.eat_keyword("OR");
            }
            let statements = parse_statement_list(p, &["WHEN", "END"])?;
            handlers.push(BigQueryHandler {
                conditions,
                statements,
            });
        }
    }
    p.expect_keyword("END")?;
    Ok(BigQueryBlock {
        statements,
        has_exception,
        handlers,
    })
}

fn parse_declare<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<BigQueryDeclare<'a>> {
    p.expect_keyword("DECLARE")?;
    let names = p.parse_comma_separated(Parser::parse_ident)?;
    let data_type = if p.at_keyword("DEFAULT") {
        None
    } else {
        Some(D::parse_data_type(p)?)
    };
    let default = if data_type.is_none() || p.at_keyword("DEFAULT") {
        p.expect_keyword("DEFAULT")?;
        Some(p.parse_expr()?)
    } else {
        None
    };
    Ok(BigQueryDeclare {
        names,
        data_type,
        default,
    })
}

fn parse_raise<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Option<BigQueryRaise<'a>>> {
    p.expect_keyword("RAISE")?;
    if p.at_eof() || p.at_op(";") {
        return Ok(None);
    }
    if p.at_keyword("USING") && p.peek_nth(1).is_word("MESSAGE") {
        p.advance();
        p.advance();
        p.expect_op("=")?;
        return Ok(Some(BigQueryRaise::UsingMessage(p.parse_expr()?)));
    }
    Ok(Some(BigQueryRaise::Value(p.parse_expr()?)))
}

fn parse_export_data<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<BigQueryExportData<'a>> {
    p.expect_keyword("EXPORT")?;
    p.expect_keyword("DATA")?;
    let connection = if p.at_keyword("WITH") && p.peek_nth(1).is_word("CONNECTION") {
        p.advance();
        p.advance();
        Some(grammar::parse_object_name(p)?)
    } else {
        None
    };
    p.expect_keyword("OPTIONS")?;
    let options = parse_option_list(p, false)?;
    p.expect_keyword("AS")?;
    let query = Box::new(D::parse_query(p)?);
    Ok(BigQueryExportData {
        connection,
        options,
        query,
    })
}

fn parse_alter_schema<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<BigQueryAlterSchema<'a>> {
    p.expect_keyword("ALTER")?;
    p.expect_keyword("SCHEMA")?;
    let if_exists = p.at_keyword("IF") && p.peek_nth(1).is_word("EXISTS");
    if if_exists {
        p.advance();
        p.advance();
    }
    let name = grammar::parse_object_name(p)?;
    let operation = if p.at_keyword("SET") && p.peek_nth(1).is_word("OPTIONS") {
        p.advance();
        p.advance();
        BigQueryAlterSchemaOp::SetOptions(parse_option_list(p, true)?)
    } else if p.at_keyword("SET") && p.peek_nth(1).is_word("DEFAULT") {
        p.advance();
        p.advance();
        p.expect_keyword("COLLATE")?;
        BigQueryAlterSchemaOp::SetDefaultCollate(p.parse_expr()?)
    } else if p.at_keyword("ADD") && p.peek_nth(1).is_word("REPLICA") {
        p.advance();
        p.advance();
        let replica = parse_name_or_string(p)?;
        let options = parse_optional_options(p)?;
        BigQueryAlterSchemaOp::AddReplica { replica, options }
    } else if p.at_keyword("DROP") && p.peek_nth(1).is_word("REPLICA") {
        p.advance();
        p.advance();
        BigQueryAlterSchemaOp::DropReplica(parse_name_or_string(p)?)
    } else {
        return Err(p.expected("an ALTER SCHEMA operation"));
    };
    Ok(BigQueryAlterSchema {
        if_exists,
        name,
        operation,
    })
}

fn parse_execute_immediate<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<BigQueryExecuteImmediate<'a>> {
    p.expect_keyword("EXECUTE")?;
    p.expect_keyword("IMMEDIATE")?;
    let query = p.parse_expr()?;
    let into = if p.eat_keyword("INTO") {
        p.parse_comma_separated(Parser::parse_ident)?
    } else {
        Vec::new()
    };
    let using = if p.eat_keyword("USING") {
        p.parse_comma_separated(|p| {
            let expr = p.parse_expr()?;
            let alias = grammar::parse_optional_alias(p)?;
            Ok(ExprWithAlias { expr, alias })
        })?
    } else {
        Vec::new()
    };
    Ok(BigQueryExecuteImmediate { query, into, using })
}

fn parse_assert<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<BigQueryStatement<'a>> {
    p.expect_keyword("ASSERT")?;
    let condition = p.parse_expr()?;
    let message = if p.eat_keyword("AS") {
        Some(p.parse_expr()?)
    } else {
        None
    };
    Ok(BigQueryStatement::Assert { condition, message })
}

/// Parses `DELETE table [WHERE ...]`, with no `FROM`.
fn parse_delete_without_from<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Delete<'a, Builtin>> {
    p.expect_keyword("DELETE")?;
    let table = p.parse_table_ref()?;
    let selection = if p.eat_keyword("WHERE") {
        Some(p.parse_expr()?)
    } else {
        None
    };
    Ok(Delete {
        output: None,
        multi: None,
        using: Vec::new(),
        table,
        selection,
        order_by: Vec::new(),
        limit: None,
        returning: Vec::new(),
    })
}

/// True at `BEGIN` that opens a block: not `BEGIN`, `BEGIN;` or `BEGIN TRANSACTION`.
fn at_block<'a, D: Dialect>(p: &Parser<'a, D>) -> bool {
    p.at_keyword("BEGIN") && {
        let next = p.peek_nth(1);
        !(next.kind == TokenKind::Eof || next.is_op(";") || next.is_word("TRANSACTION"))
    }
}

pub fn parse_statement<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Statement<'a, Builtin>> {
    let parsed = if at_block(p) {
        Some(BigQueryStatement::Block(Box::new(parse_block(p)?)))
    } else if p.at_keyword("DECLARE") {
        Some(BigQueryStatement::Declare(Box::new(parse_declare(p)?)))
    } else if p.at_keyword("RAISE") {
        Some(BigQueryStatement::Raise(parse_raise(p)?))
    } else if p.at_keyword("EXPORT") {
        Some(BigQueryStatement::ExportData(Box::new(parse_export_data(
            p,
        )?)))
    } else if p.at_keyword("ALTER") && p.peek_nth(1).is_word("SCHEMA") {
        Some(BigQueryStatement::AlterSchema(Box::new(
            parse_alter_schema(p)?,
        )))
    } else if p.at_keyword("EXECUTE") && p.peek_nth(1).is_word("IMMEDIATE") {
        Some(BigQueryStatement::ExecuteImmediate(Box::new(
            parse_execute_immediate(p)?,
        )))
    } else if p.at_keyword("ASSERT") {
        Some(parse_assert(p)?)
    } else if p.at_keyword("SET") && p.peek_nth(1).is_op("(") {
        Some(parse_set_tuple(p)?)
    } else if p.at_keyword("DELETE") && !p.peek_nth(1).is_word("FROM") {
        Some(BigQueryStatement::DeleteWithoutFrom(Box::new(
            parse_delete_without_from(p)?,
        )))
    } else if p.at_keyword("INSERT") && !p.peek_nth(1).is_word("INTO") {
        return parse_insert_without_into(p);
    } else {
        None
    };
    match parsed {
        Some(parsed) => Ok(statement(parsed)),
        None => grammar::parse_statement(p),
    }
}

/// Parses `INSERT` with its table straight after the keyword.
fn parse_insert_without_into<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Statement<'a, Builtin>> {
    p.expect_keyword("INSERT")?;
    let table = grammar::parse_object_name(p)?;
    let columns = if grammar::at_column_list(p) {
        grammar::parse_ident_list(p)?
    } else {
        Vec::new()
    };
    let source = grammar::parse_insert_source(p)?;
    let insert = Insert {
        into: true,
        output: None,
        modifiers: Default::default(),
        table,
        columns,
        source,
        returning: Vec::new(),
    };
    Ok(statement(BigQueryStatement::InsertWithoutInto(Box::new(
        insert,
    ))))
}

/// Parses a `SELECT` head, which can end with `AS STRUCT` or `AS VALUE`.
pub fn parse_select_head<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<(BuiltinSelect<'a>, SetQuantifier)> {
    let mut modifiers = BuiltinSelect::default();
    let mut quantifier = grammar::parse_quantifier(p);
    if p.at_keyword("AS") {
        let next = p.peek_nth(1);
        let word = if next.is_word("STRUCT") {
            Some("STRUCT")
        } else if next.is_word("VALUE") {
            Some("VALUE")
        } else {
            None
        };
        if let Some(word) = word {
            p.advance();
            p.advance();
            modifiers.bigquery_as = Some(word.into());
            // `ALL` is the default and is not written back out.
            if quantifier == SetQuantifier::All {
                quantifier = SetQuantifier::None;
            }
        }
    }
    Ok((modifiers, quantifier))
}

/// Parses a select item, in which a wildcard can also exclude one bare column:
/// `* EXCEPT a`.
pub fn parse_select_item<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<SelectItem<'a, Builtin>> {
    let item = databricks::parse_select_item(p)?;
    let qualifier = match &item.kind {
        SelectItemKind::Wildcard => None,
        SelectItemKind::QualifiedWildcard(name) => Some(name.clone()),
        _ => return Ok(item),
    };
    let next = p.peek_nth(1);
    let bare_exclude = p.at_keyword("EXCEPT")
        && is_name(next)
        && !["SELECT", "WITH", "VALUES", "ALL", "DISTINCT"]
            .iter()
            .any(|word| next.is_word(word));
    if !bare_exclude {
        return Ok(item);
    }
    p.advance();
    let exclude = vec![p.parse_ident()?];
    Ok(SelectItemKind::WildcardWithOptions {
        qualifier,
        options: Box::new(WildcardOptions {
            ilike: None,
            exclude,
            exclude_parens: true,
            except_keyword: true,
            replace: Vec::new(),
            rename: Vec::new(),
            rename_parens: false,
        }),
    }
    .into())
}

/// Finishes a select item: `expr.*`, or an expression with any alias.
pub fn parse_select_item_alias<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
    expr: Expr<'a, Builtin>,
) -> PResult<SelectItem<'a, Builtin>> {
    if p.at_op(".") && p.peek_nth(1).is_op("*") {
        p.advance();
        p.advance();
        let options = parse_star_options(p)?;
        let start = expr.span.start;
        return Ok(SelectItemKind::Expr {
            expr: Expr::new(
                ext(BigQueryExpr::Star(Box::new(BigQueryStar { expr, options }))).kind,
                p.span_since(start),
            ),
            alias: None,
        }
        .into());
    }
    if p.eat_keyword("AS") {
        let alias = parse_name_or_string(p)?;
        return Ok(SelectItemKind::Expr {
            expr,
            alias: Some(alias),
        }
        .into());
    }
    let token = p.peek();
    let implicit = token.kind == TokenKind::String
        || token.kind == TokenKind::QuotedIdent
        || (token.kind == TokenKind::Word && !is_reserved_for_column_alias(token.text));
    let alias = if implicit {
        Some(parse_name_or_string(p)?)
    } else {
        None
    };
    Ok(SelectItemKind::Expr { expr, alias }.into())
}

/// Parses `EXCEPT (...)` and `REPLACE (...)` after `expr.*`.
fn parse_star_options<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Option<Box<WildcardOptions<'a, Builtin>>>> {
    let query_follows = ["SELECT", "WITH", "VALUES"]
        .iter()
        .any(|word| p.peek_nth(2).is_word(word));
    let exclude = if p.at_keyword("EXCEPT") && p.peek_nth(1).is_op("(") && !query_follows {
        p.advance();
        grammar::parse_ident_list(p)?
    } else {
        Vec::new()
    };
    let replace = if p.at_keyword("REPLACE") && p.peek_nth(1).is_op("(") {
        grammar::parse_wildcard_replace(p)?
    } else {
        Vec::new()
    };
    if exclude.is_empty() && replace.is_empty() {
        return Ok(None);
    }
    Ok(Some(Box::new(WildcardOptions {
        ilike: None,
        exclude,
        exclude_parens: true,
        except_keyword: true,
        replace,
        rename: Vec::new(),
        rename_parens: false,
    })))
}

fn at_system_time<'a, D: Dialect>(p: &Parser<'a, D>) -> bool {
    p.at_keyword("FOR")
        && p.peek_nth(1).is_word("SYSTEM_TIME")
        && p.peek_nth(2).is_word("AS")
        && p.peek_nth(3).is_word("OF")
}

fn consume_system_time<'a, D: Dialect>(p: &mut Parser<'a, D>) {
    for _ in 0..4 {
        p.advance();
    }
}

/// Parses a table: a name that can hold hyphens, optionally read as of a time.
pub fn parse_table_factor<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<TableFactor<'a, Builtin>> {
    if p.at_op("(") || p.at_keyword("LATERAL") {
        return grammar::parse_table_factor(p);
    }
    let name = parse_table_name(p)?;
    if p.at_op("(") {
        let call = Box::new(p.parse_function_call(name)?);
        let with_ordinality = p.at_keyword("WITH") && p.peek_nth(1).is_word("ORDINALITY");
        if with_ordinality {
            p.advance();
            p.advance();
        }
        return Ok(TableFactor::Function {
            call,
            with_ordinality,
        });
    }
    if at_system_time(p) {
        consume_system_time(p);
        let at = p.parse_expr()?;
        return Ok(TableFactor::TimeTravel(Box::new(TimeTravel {
            name,
            kind: "FOR SYSTEM_TIME".into(),
            at,
        })));
    }
    Ok(TableFactor::Named(name))
}

/// True if `table` is a call of `UNNEST`, with any alias.
fn is_unnest(table: &TableRef<'_, Builtin>) -> bool {
    let TableFactor::Function { call, .. } = &table.factor else {
        return false;
    };
    let ExprKind::Function { name, .. } = &call.kind else {
        return false;
    };
    name.parts.len() == 1 && name.parts[0].text.eq_ignore_ascii_case("UNNEST")
}

/// Parses a table with its alias, and any `WITH OFFSET` after them.
pub fn parse_table_ref<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<TableRef<'a, Builtin>> {
    let table = spark::parse_table_ref(p)?;
    if !(p.at_keyword("WITH") && p.peek_nth(1).is_word("OFFSET") && is_unnest(&table)) {
        return Ok(table);
    }
    p.advance();
    p.advance();
    let alias = grammar::parse_optional_alias(p)?;
    let factor = BigQueryTableFactor::WithOffset(Box::new(BigQueryWithOffset { table, alias }));
    Ok(TableRef {
        factor: TableFactor::Ext(BuiltinTableFactor::BigQuery(factor)),
        alias: None,
        span: Span::default(),
    })
}

/// Parses a call's arguments, which can hold clauses the core has no place for
/// inside the parentheses, or be a single query.
pub fn parse_function_call<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
    name: ObjectName<'a>,
) -> PResult<Expr<'a, Builtin>> {
    p.expect_op("(")?;
    let array = name.parts.len() == 1 && name.parts[0].text.eq_ignore_ascii_case("ARRAY");
    if array && grammar::query_starts_at(p, 0) {
        let query = p.nested(D::parse_query)?;
        p.expect_op(")")?;
        return Ok(ext(BigQueryExpr::SubqueryCall(Box::new(
            BigQuerySubqueryCall { name, query },
        ))));
    }
    let quantifier = grammar::parse_quantifier(p);
    let args = if quantifier == SetQuantifier::None && p.at_op(")") {
        Vec::new()
    } else {
        p.parse_comma_separated(grammar::parse_function_arg)?
    };
    let inner_nulls = grammar::parse_null_treatment(p);
    let order_by = grammar::parse_optional_order_by(p)?;
    let limit = if p.eat_keyword("LIMIT") {
        Some(p.parse_expr()?)
    } else {
        None
    };
    let having = if p.eat_keyword("HAVING") {
        let max = p.eat_keyword("MAX");
        if !max {
            p.expect_keyword("MIN")?;
        }
        Some((max, p.parse_expr()?))
    } else {
        None
    };
    p.expect_op(")")?;

    let within_group = if p.at_keyword("WITHIN") && p.peek_nth(1).is_word("GROUP") {
        p.advance();
        p.advance();
        p.expect_op("(")?;
        let items = grammar::parse_optional_order_by(p)?;
        p.expect_op(")")?;
        items
    } else {
        Vec::new()
    };
    let filter = D::parse_function_filter(p)?;
    let outer_nulls = grammar::parse_null_treatment(p);
    let over = if p.eat_keyword("OVER") {
        Some(grammar::parse_window_over(p)?)
    } else {
        None
    };

    if inner_nulls.is_some() || limit.is_some() || having.is_some() {
        if !within_group.is_empty() || filter.is_some() || outer_nulls.is_some() {
            return Err(p.expected("OVER, or nothing, after these call clauses"));
        }
        return Ok(ext(BigQueryExpr::Call(Box::new(BigQueryCall {
            name,
            quantifier,
            args,
            null_treatment: inner_nulls,
            order_by,
            limit,
            having,
            over,
        }))));
    }
    let is_plain = quantifier == SetQuantifier::None
        && order_by.is_empty()
        && within_group.is_empty()
        && filter.is_none()
        && outer_nulls.is_none()
        && over.is_none();
    let options = (!is_plain).then(|| {
        Box::new(FunctionOptions {
            parameters: Vec::new(),
            inner_null_treatment: None,
            quantifier,
            order_by,
            within_group,
            filter,
            null_treatment: outer_nulls,
            separator: None,
            on_overflow: None,
            over,
        })
    });
    Ok(ExprKind::Function {
        name,
        args,
        options,
    }
    .into())
}

/// Parses `[a, b]`, with the cursor on the `[`.
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
    Ok(ext(BigQueryExpr::Array(items)))
}

/// Parses `STRUCT<...>(values)` or `STRUCT(values)`, with the cursor on `STRUCT`.
/// In the typeless form, a value can be named with `AS name`.
pub fn parse_struct<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Expr<'a, Builtin>> {
    let ty = if p.peek_nth(1).is_op("<") {
        Some(D::parse_data_type(p)?)
    } else {
        p.advance();
        None
    };
    p.expect_op("(")?;
    let typed = ty.is_some();
    let values = if p.at_op(")") {
        Vec::new()
    } else {
        p.parse_comma_separated(|p| {
            let expr = p.parse_expr()?;
            let alias = if !typed && p.eat_keyword("AS") {
                Some(p.parse_ident()?)
            } else {
                None
            };
            Ok(ExprWithAlias { expr, alias })
        })?
    };
    p.expect_op(")")?;
    Ok(ext(BigQueryExpr::Struct(Box::new(BigQueryStruct {
        ty,
        values,
    }))))
}

/// Parses a `CAST` or `SAFE_CAST`, whose type can be followed by `FORMAT`.
fn parse_cast<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
    safe: bool,
) -> PResult<Expr<'a, Builtin>> {
    p.advance();
    p.expect_op("(")?;
    let expr = p.parse_expr()?;
    p.expect_keyword("AS")?;
    let ty = D::parse_data_type(p)?;
    if !p.eat_keyword("FORMAT") {
        p.expect_op(")")?;
        return Ok(ExprKind::Cast {
            expr: Box::new(expr),
            ty: Box::new(ty),
            style: if safe {
                CastStyle::SafeFunction
            } else {
                CastStyle::Function
            },
        }
        .into());
    }
    let format = spark::parse_string(p)?;
    let time_zone = if p.at_keyword("AT") {
        p.advance();
        p.expect_keyword("TIME")?;
        p.expect_keyword("ZONE")?;
        Some(spark::parse_string(p)?)
    } else {
        None
    };
    p.expect_op(")")?;
    Ok(ext(BigQueryExpr::CastFormat(Box::new(
        BigQueryCastFormat {
            expr,
            ty,
            safe,
            format,
            time_zone,
        },
    ))))
}

/// The words after which a string literal has that type.
fn is_typed_literal_word(word: &str) -> bool {
    [
        "JSON",
        "NUMERIC",
        "BIGNUMERIC",
        "BIGDECIMAL",
        "DECIMAL",
        "DEC",
        "BOOL",
        "BOOLEAN",
        "INT64",
        "INT",
        "INTEGER",
        "SMALLINT",
        "BIGINT",
        "TINYINT",
        "BYTEINT",
        "FLOAT",
        "FLOAT64",
        "DOUBLE",
        "STRING",
        "BYTES",
        "GEOGRAPHY",
    ]
    .iter()
    .any(|name| name.eq_ignore_ascii_case(word))
}

/// Parses a prefix expression, adding BigQuery's literals and special calls.
pub fn parse_prefix<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Expr<'a, Builtin>> {
    let token = p.peek();
    let next = p.peek_nth(1);
    if token.kind == TokenKind::Op && token.text == "[" {
        return parse_array_literal(p);
    }
    if token.kind == TokenKind::Word {
        // `INTERVAL` always introduces an interval here, never a column.
        if token.is_word("INTERVAL") {
            p.advance();
            return D::parse_interval(p);
        }
        if token.is_word("STRUCT") && (next.is_op("<") || next.is_op("(")) {
            return parse_struct(p);
        }
        if next.is_op("(") {
            if token.is_word("CAST") {
                return parse_cast(p, false);
            }
            if token.is_word("SAFE_CAST") {
                return parse_cast(p, true);
            }
            if token.is_word("EXTRACT") && p.peek_nth(2).is_word("WEEK") && p.peek_nth(3).is_op("(")
            {
                return parse_extract_week(p);
            }
            if token.is_word("LEFT") || token.is_word("RIGHT") {
                let name = ObjectName {
                    parts: vec![p.parse_name_part()?],
                };
                return p.parse_function_call(name);
            }
        }
        if next.kind == TokenKind::String && is_typed_literal_word(token.text) {
            p.advance();
            p.advance();
            let ty = DataType {
                qualifier: Vec::new(),
                name: Ident {
                    text: token.text.into(),
                    quote: None,
                    span: token.span,
                },
                angle: None,
                args: Vec::new(),
                suffix: DataTypeSuffix::None,
                array: crate::ast::ArrayType::None,
            };
            return Ok(ExprKind::TypedString(Box::new(TypedString {
                ty,
                value: D::string_literal(next),
            }))
            .into());
        }
    }
    grammar::parse_prefix(p)
}

fn parse_extract_week<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Expr<'a, Builtin>> {
    for _ in 0..4 {
        p.advance();
    }
    let weekday = p.parse_ident()?;
    p.expect_op(")")?;
    p.expect_keyword("FROM")?;
    let expr = p.parse_expr()?;
    p.expect_op(")")?;
    Ok(ext(BigQueryExpr::ExtractWeek(Box::new(
        BigQueryExtractWeek { weekday, expr },
    ))))
}

/// Levels of tree height for `.field` on a dialect node, whose wrappers take
/// more stack to clone than an ordinary level.
const FIELD_ACCESS_LEVELS: usize = 3;

fn parse_field_access<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
    left: Expr<'a, Builtin>,
    _bp: u8,
) -> PResult<Expr<'a, Builtin>> {
    let composite = match &left.kind {
        ExprKind::Nested(_) | ExprKind::Function { .. } | ExprKind::Ext(_) => true,
        ExprKind::Special(special) => matches!(
            **special,
            SpecialExpr::Subscript { .. } | SpecialExpr::FieldAccess { .. }
        ),
        _ => false,
    };
    if !composite {
        return Err(p.expected("a composite value before the field name"));
    }
    p.advance();
    let field = p.parse_name_part()?;
    p.deepen_by(FIELD_ACCESS_LEVELS)?;
    Ok(ExprKind::Special(Box::new(SpecialExpr::FieldAccess { expr: left, field })).into())
}

fn parse_in_unnest<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
    left: Expr<'a, Builtin>,
    negated: bool,
) -> PResult<Expr<'a, Builtin>> {
    p.expect_keyword("UNNEST")?;
    p.expect_op("(")?;
    let array = p.parse_expr()?;
    p.expect_op(")")?;
    Ok(ext(BigQueryExpr::InUnnest(Box::new(BigQueryInUnnest {
        expr: left,
        array,
        negated,
    }))))
}

fn parse_in_unnest_op<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
    left: Expr<'a, Builtin>,
    _bp: u8,
) -> PResult<Expr<'a, Builtin>> {
    p.advance();
    parse_in_unnest(p, left, false)
}

fn parse_not_in_unnest_op<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
    left: Expr<'a, Builtin>,
    _bp: u8,
) -> PResult<Expr<'a, Builtin>> {
    p.advance();
    p.advance();
    parse_in_unnest(p, left, true)
}

pub fn infix_rule<'a, D: Dialect<Ext = Builtin>>(p: &Parser<'a, D>) -> Option<InfixRule<'a, D>> {
    if p.at_op(".") {
        if p.peek_nth(1).is_op("*") {
            return None;
        }
        return Some(InfixRule::custom(prec::POSTFIX, parse_field_access));
    }
    if p.at_keyword("IN") && p.peek_nth(1).is_word("UNNEST") {
        return Some(InfixRule::custom(prec::COMPARE, parse_in_unnest_op));
    }
    if p.at_keyword("NOT") && p.peek_nth(1).is_word("IN") && p.peek_nth(2).is_word("UNNEST") {
        return Some(InfixRule::custom(prec::COMPARE, parse_not_in_unnest_op));
    }
    grammar::infix_rule(p)
}

impl Dialect for BigQuery {
    type Ext = Builtin;

    fn parse_update_from_after_set<'a>(
        p: &mut Parser<'a, Self>,
    ) -> PResult<Vec<TableWithJoins<'a, Builtin>>> {
        grammar::parse_optional_update_from(p)
    }

    fn parse_group_by_item<'a>(p: &mut Parser<'a, Self>) -> PResult<Expr<'a, Builtin>> {
        grammar::parse_grouping_item_or_empty_set(p)
    }

    fn parse_cte_columns<'a>(_p: &mut Parser<'a, Self>) -> PResult<Vec<Ident<'a>>> {
        Ok(Vec::new())
    }

    fn parse_statement<'a>(p: &mut Parser<'a, Self>) -> PResult<Statement<'a, Builtin>> {
        parse_statement(p)
    }

    #[inline]
    fn lex_token(rest: &str) -> Option<LexResult> {
        lex_token(rest)
    }

    #[inline]
    fn lex_comment(rest: &str) -> Option<Result<usize, &'static str>> {
        match rest.as_bytes() {
            [b'#', ..] | [b'-', b'-', ..] => Some(Ok(lexer::line_comment(rest))),
            [b'/', b'*', ..] => Some(lexer::block_comment(rest, false)),
            _ => None,
        }
    }

    #[inline]
    fn is_reserved(word: &str) -> bool {
        grammar::is_reserved(word) && !word.eq_ignore_ascii_case("OFFSET")
    }

    #[inline]
    fn is_reserved_for_alias(word: &str) -> bool {
        RESERVED_FOR_TABLE_ALIAS
            .iter()
            .any(|reserved| reserved.eq_ignore_ascii_case(word))
    }

    #[inline]
    fn string_literal(token: Token<'_>) -> StringLiteral<'_> {
        string_literal(token)
    }

    fn at_from_first<'a>(p: &Parser<'a, Self>, n: usize) -> bool {
        p.peek_nth(n).is_word("FROM")
    }

    fn parse_select<'a>(p: &mut Parser<'a, Self>) -> PResult<crate::ast::Select<'a, Builtin>> {
        databricks::parse_select(p)
    }

    fn parse_select_head<'a>(
        p: &mut Parser<'a, Self>,
    ) -> PResult<(BuiltinSelect<'a>, SetQuantifier)> {
        parse_select_head(p)
    }

    fn parse_projection<'a>(p: &mut Parser<'a, Self>) -> PResult<Vec<SelectItem<'a, Builtin>>> {
        grammar::parse_list_with_trailing_comma(p, Parser::parse_select_item)
    }

    fn parse_select_item<'a>(p: &mut Parser<'a, Self>) -> PResult<SelectItem<'a, Builtin>> {
        parse_select_item(p)
    }

    fn parse_name_part<'a>(p: &mut Parser<'a, Self>) -> PResult<Ident<'a>> {
        parse_name_or_string_part(p)
    }

    fn parse_function_filter<'a>(_p: &mut Parser<'a, Self>) -> PResult<Option<Expr<'a, Builtin>>> {
        Ok(None)
    }

    fn parse_named_window_reference<'a>(p: &mut Parser<'a, Self>) -> PResult<Option<Ident<'a>>> {
        if p.at_op("(") {
            Ok(None)
        } else {
            p.parse_ident().map(Some)
        }
    }

    fn eat_merge_insert_row<'a>(p: &mut Parser<'a, Self>) -> bool {
        p.eat_keyword("ROW")
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

    fn parse_data_type<'a>(p: &mut Parser<'a, Self>) -> PResult<DataType<'a>> {
        parse_data_type(p)
    }

    fn parse_column_option<'a>(
        p: &mut Parser<'a, Self>,
    ) -> PResult<Option<ColumnOption<'a, Builtin>>> {
        parse_column_option(p)
    }

    fn parse_create<'a>(p: &mut Parser<'a, Self>) -> PResult<Statement<'a, Builtin>> {
        parse_create(p)
    }

    fn parse_pipe_operators<'a>(
        p: &mut Parser<'a, Self>,
    ) -> PResult<Vec<PipeOperator<'a, Builtin>>> {
        grammar::parse_pipe_operators(p)
    }

    fn parse_qualify<'a>(p: &mut Parser<'a, Self>) -> PResult<Option<Expr<'a, Builtin>>> {
        snowflake::parse_qualify(p)
    }

    fn eat_group_by_all<'a>(p: &mut Parser<'a, Self>) -> bool {
        p.eat_keyword("ALL")
    }

    fn parse_add_column_if_not_exists<'a>(p: &mut Parser<'a, Self>) -> PResult<bool> {
        grammar::parse_if_not_exists(p)
    }
}

fn write_opt_options(
    f: &mut fmt::Formatter<'_>,
    options: &Option<Vec<ExprOption<'_, Builtin>>>,
) -> fmt::Result {
    match options {
        Some(options) => write!(f, " OPTIONS({})", comma(options)),
        None => Ok(()),
    }
}

impl fmt::Display for BigQueryCreateTable<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let table = &self.table;
        write_create_flags(f, table.or_replace, table.temporary)?;
        f.write_str(if self.external {
            "EXTERNAL TABLE "
        } else {
            "TABLE "
        })?;
        write_if_not_exists(f, table.if_not_exists)?;
        table.name.fmt(f)?;
        if let Some((keyword, source)) = &self.source {
            write!(f, " {keyword} {source}")?;
        }
        if self.external || !table.columns.is_empty() || !table.constraints.is_empty() {
            write_columns(f, &table.columns, &table.constraints)?;
        }
        if let Some(connection) = &self.connection {
            write!(f, " WITH CONNECTION {connection}")?;
        }
        if let Some(partition_by) = &self.partition_by {
            write!(f, " PARTITION BY {partition_by}")?;
        }
        if !self.cluster_by.is_empty() {
            write!(f, " CLUSTER BY {}", comma(&self.cluster_by))?;
        }
        write_opt_options(f, &self.options)?;
        if let Some(query) = &table.query {
            write!(f, " AS {query}")?;
        }
        Ok(())
    }
}

impl fmt::Display for BigQueryViewColumn<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.name.fmt(f)?;
        write_opt_options(f, &self.options)
    }
}

impl fmt::Display for BigQueryCreateView<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_create_flags(f, self.or_replace, self.temporary)?;
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
        write_opt_options(f, &self.options)?;
        crate::display::write_view_query(f, &self.query)
    }
}

impl fmt::Display for BigQueryCreateFunction<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_create_flags(f, self.or_replace, self.temporary)?;
        f.write_str("FUNCTION ")?;
        write_if_not_exists(f, self.if_not_exists)?;
        write!(f, "{}({})", self.name, comma(&self.args))?;
        if let Some(returns) = &self.returns {
            write!(f, " RETURNS {returns}")?;
        }
        match self.deterministic {
            Some(true) => f.write_str(" DETERMINISTIC")?,
            Some(false) => f.write_str(" NOT DETERMINISTIC")?,
            None => {}
        }
        if let Some(language) = &self.language {
            write!(f, " LANGUAGE {language}")?;
        }
        if let Some(connection) = &self.remote_connection {
            write!(f, " REMOTE WITH CONNECTION {connection}")?;
        }
        if !self.options_after_body {
            write_opt_options(f, &self.options)?;
        }
        if let Some(body) = &self.body {
            write!(f, " AS {body}")?;
        }
        if self.options_after_body {
            write_opt_options(f, &self.options)?;
        }
        Ok(())
    }
}

impl fmt::Display for BigQueryFunctionArg<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.name, self.data_type)
    }
}

impl fmt::Display for BigQueryCreateSnapshot<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CREATE SNAPSHOT TABLE ")?;
        write_if_not_exists(f, self.if_not_exists)?;
        write!(f, "{} CLONE {}", self.name, self.clone)?;
        if let Some(at) = &self.for_system_time {
            write!(f, " FOR SYSTEM_TIME AS OF {at}")?;
        }
        write_opt_options(f, &self.options)
    }
}

impl fmt::Display for BigQueryCreateSchema<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CREATE SCHEMA ")?;
        write_if_not_exists(f, self.if_not_exists)?;
        self.name.fmt(f)?;
        if let Some(collate) = &self.default_collate {
            write!(f, " DEFAULT COLLATE {collate}")?;
        }
        write_opt_options(f, &self.options)
    }
}

impl fmt::Display for BigQueryExportData<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("EXPORT DATA")?;
        if let Some(connection) = &self.connection {
            write!(f, " WITH CONNECTION {connection}")?;
        }
        write!(f, " OPTIONS({}) AS {}", comma(&self.options), self.query)
    }
}

impl fmt::Display for BigQueryAlterSchema<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ALTER SCHEMA ")?;
        if self.if_exists {
            f.write_str("IF EXISTS ")?;
        }
        write!(f, "{}", self.name)?;
        match &self.operation {
            BigQueryAlterSchemaOp::SetDefaultCollate(collate) => {
                write!(f, " SET DEFAULT COLLATE {collate}")
            }
            BigQueryAlterSchemaOp::AddReplica { replica, options } => {
                write!(f, " ADD REPLICA {replica}")?;
                if let Some(options) = options {
                    write!(f, " OPTIONS ({})", comma(options))?;
                }
                Ok(())
            }
            BigQueryAlterSchemaOp::DropReplica(replica) => write!(f, " DROP REPLICA {replica}"),
            BigQueryAlterSchemaOp::SetOptions(options) => {
                write!(f, " SET OPTIONS ({})", comma(options))
            }
        }
    }
}

impl fmt::Display for BigQueryHandler<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("WHEN ")?;
        for (i, condition) in self.conditions.iter().enumerate() {
            if i > 0 {
                f.write_str(" OR ")?;
            }
            condition.fmt(f)?;
        }
        f.write_str(" THEN")?;
        for statement in &self.statements {
            write!(f, " {statement};")?;
        }
        Ok(())
    }
}

impl fmt::Display for BigQueryBlock<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("BEGIN")?;
        for statement in &self.statements {
            write!(f, " {statement};")?;
        }
        if self.has_exception {
            f.write_str(" EXCEPTION")?;
            for handler in &self.handlers {
                write!(f, " {handler}")?;
            }
        }
        f.write_str(" END")
    }
}

impl fmt::Display for BigQueryDeclare<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DECLARE {}", comma(&self.names))?;
        if let Some(data_type) = &self.data_type {
            write!(f, " {data_type}")?;
        }
        if let Some(default) = &self.default {
            write!(f, " DEFAULT {default}")?;
        }
        Ok(())
    }
}

impl fmt::Display for BigQueryExecuteImmediate<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "EXECUTE IMMEDIATE {}", self.query)?;
        if !self.into.is_empty() {
            write!(f, " INTO {}", comma(&self.into))?;
        }
        if !self.using.is_empty() {
            write!(f, " USING {}", comma(&self.using))?;
        }
        Ok(())
    }
}

impl fmt::Display for BigQueryStatement<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CreateTable(create) => create.fmt(f),
            Self::CreateView(view) => view.fmt(f),
            Self::CreateFunction(function) => function.fmt(f),
            Self::CreateSnapshot(snapshot) => snapshot.fmt(f),
            Self::CreateSchema(schema) => schema.fmt(f),
            Self::DeleteWithoutFrom(delete) => {
                f.write_str("DELETE")?;
                SpaceOrNewline.fmt(f)?;
                Indent(&delete.table).fmt(f)?;
                write_where_clause(f, &delete.selection)
            }
            Self::InsertWithoutInto(insert) => {
                write!(f, "INSERT {}", insert.table)?;
                if !insert.columns.is_empty() {
                    write!(f, " ({})", comma(&insert.columns))?;
                }
                crate::display::write_insert_source(f, &insert.source, !insert.columns.is_empty())
            }
            Self::ExportData(export) => export.fmt(f),
            Self::AlterSchema(alter) => alter.fmt(f),
            Self::Block(block) => block.fmt(f),
            Self::Declare(declare) => declare.fmt(f),
            Self::Raise(None) => f.write_str("RAISE"),
            Self::Raise(Some(BigQueryRaise::UsingMessage(message))) => {
                write!(f, "RAISE USING MESSAGE = {message}")
            }
            Self::Raise(Some(BigQueryRaise::Value(value))) => write!(f, "RAISE {value}"),
            Self::SetTuple { names, values } => {
                write!(f, "SET ({}) = ({})", comma(names), comma(values))
            }
            Self::ExecuteImmediate(execute) => execute.fmt(f),
            Self::Assert { condition, message } => {
                write!(f, "ASSERT {condition}")?;
                if let Some(message) = message {
                    write!(f, " AS {message}")?;
                }
                Ok(())
            }
        }
    }
}

impl fmt::Display for BigQueryCall<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}(", self.name)?;
        match self.quantifier {
            SetQuantifier::None => {}
            SetQuantifier::All => f.write_str("ALL ")?,
            SetQuantifier::Distinct => f.write_str("DISTINCT ")?,
        }
        comma(&self.args).fmt(f)?;
        match self.null_treatment {
            Some(NullTreatment::Ignore) => f.write_str(" IGNORE NULLS")?,
            Some(NullTreatment::Respect) => f.write_str(" RESPECT NULLS")?,
            None => {}
        }
        if !self.order_by.is_empty() {
            write!(f, " ORDER BY {}", comma(&self.order_by))?;
        }
        if let Some(limit) = &self.limit {
            write!(f, " LIMIT {limit}")?;
        }
        if let Some((max, having)) = &self.having {
            let bound = if *max { "MAX" } else { "MIN" };
            write!(f, " HAVING {bound} {having}")?;
        }
        f.write_str(")")?;
        if let Some(over) = &self.over {
            write!(f, " OVER {over}")?;
        }
        Ok(())
    }
}

impl fmt::Display for BigQueryExpr<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Array(items) => write!(f, "[{}]", comma(items)),
            Self::Struct(literal) => {
                match &literal.ty {
                    Some(ty) => ty.fmt(f)?,
                    None => f.write_str("STRUCT")?,
                }
                write!(f, "({})", comma(&literal.values))
            }
            Self::Star(star) => {
                write!(f, "{}.*", star.expr)?;
                if let Some(options) = &star.options {
                    options.fmt(f)?;
                }
                Ok(())
            }
            Self::InUnnest(in_unnest) => {
                let not = if in_unnest.negated { "NOT " } else { "" };
                write!(f, "{} {not}IN UNNEST({})", in_unnest.expr, in_unnest.array)
            }
            Self::CastFormat(cast) => {
                let keyword = if cast.safe { "SAFE_CAST" } else { "CAST" };
                write!(
                    f,
                    "{keyword}({} AS {} FORMAT {}",
                    cast.expr, cast.ty, cast.format
                )?;
                if let Some(ref zone) = cast.time_zone {
                    write!(f, " AT TIME ZONE {}", zone)?;
                }
                f.write_str(")")
            }
            Self::ExtractWeek(extract) => {
                write!(
                    f,
                    "EXTRACT(WEEK({}) FROM {})",
                    extract.weekday, extract.expr
                )
            }
            Self::Call(call) => call.fmt(f),
            Self::SubqueryCall(call) => write!(f, "{}({})", call.name, call.query),
        }
    }
}

impl fmt::Display for BigQueryTableFactor<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WithOffset(offset) => {
                write!(f, "{} WITH OFFSET", offset.table)?;
                if let Some(alias) = &offset.alias {
                    write!(f, " AS {alias}")?;
                }
                Ok(())
            }
        }
    }
}

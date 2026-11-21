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

//! DuckDB, built on the core grammar and PostgreSQL's operators and types.

use crate::ast::Span;
use std::borrow::Cow;
use std::fmt;

use crate::PResult;
use crate::ast::{
    AngleArguments, ArrayType, BinaryOp, ColumnOption, CreateFlags, CreateSchema, Cte, DataType,
    DataTypeSuffix, ExplainOption, Expr, ExprKind, FunctionArg, Generated, GeneratedKind,
    GeneratedStorage, GeneratedWhen, Ident, InsertSource, IsTest, Join, Literal, ObjectName,
    ObjectType, OrderByItem, Query, Select, SelectItem, SelectItemKind, SetQuantifier, SpecialExpr,
    Statement, StatementKind, StructField, TableAlias, TableRef, TableWithJoins, WildcardOptions,
};
use crate::dialect::builtin::{BuiltinExpr, BuiltinStatement};
use crate::dialect::{Builtin, Dialect, postgres, snowflake, spark};
use crate::display::{comma, write_if_not_exists};
use crate::grammar::{self, prec};
use crate::lexer::{LexResult, Lexeme, Token, TokenKind};
use crate::parser::{InfixRule, Parser};

#[derive(Debug, Clone, PartialEq)]
pub struct DuckDb;

/// Expressions only DuckDB has.
#[derive(Debug, Clone, PartialEq)]
pub enum DuckDbExpr<'a> {
    /// `{'a': 1, b: 2}`
    Struct(Vec<(Ident<'a>, Expr<'a, Builtin>)>),
    /// `MAP {key: value}`
    Map(Vec<(Expr<'a, Builtin>, Expr<'a, Builtin>)>),
    /// `[a, b]`
    List(Vec<Expr<'a, Builtin>>),
    /// `lambda x, y : body`
    Lambda(Box<DuckDbLambda<'a>>),
    /// `receiver.name(args)`
    MethodCall(Box<DuckDbMethodCall<'a>>),
    /// `ALL` in `ORDER BY ALL`.
    AllColumns,
    /// `:name`, which is written as two tokens because `:` also separates keys
    /// from values.
    ColonPlaceholder(Cow<'a, str>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct DuckDbLambda<'a> {
    pub params: Vec<Ident<'a>>,
    pub body: Expr<'a, Builtin>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DuckDbMethodCall<'a> {
    pub receiver: Expr<'a, Builtin>,
    /// Always an [`ExprKind::Function`] with a one-part name.
    pub call: Expr<'a, Builtin>,
}

/// Statements only DuckDB has.
#[derive(Debug, Clone, PartialEq)]
pub enum DuckDbStatement<'a> {
    CreateMacro(Box<DuckDbCreateMacro<'a>>),
    CreateSecret(Box<DuckDbCreateSecret<'a>>),
    /// `DROP [TEMPORARY | PERSISTENT] SECRET [IF EXISTS] name [FROM storage]`
    DropSecret {
        /// `Some(true)` for `TEMPORARY`, `Some(false)` for `PERSISTENT`.
        temporary: Option<bool>,
        if_exists: bool,
        name: Ident<'a>,
        storage: Option<Ident<'a>>,
    },
    Attach(Box<DuckDbAttach<'a>>),
    /// `DETACH [DATABASE] [IF EXISTS] name`
    Detach {
        database: bool,
        if_exists: bool,
        name: Ident<'a>,
    },
    /// `INSTALL name`
    Install(Ident<'a>),
    /// `LOAD name`
    Load(Ident<'a>),
}

/// `CREATE [OR REPLACE] [TEMPORARY] MACRO name(args) AS definition`.
/// `CREATE FUNCTION` means the same and renders as `CREATE MACRO`.
#[derive(Debug, Clone, PartialEq)]
pub struct DuckDbCreateMacro<'a> {
    pub or_replace: bool,
    pub temporary: bool,
    pub name: ObjectName<'a>,
    /// `None` for an empty argument list.
    pub args: Option<Vec<MacroArg<'a>>>,
    pub definition: MacroDefinition<'a>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MacroArg<'a> {
    pub name: Ident<'a>,
    pub default: Option<Expr<'a, Builtin>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum MacroDefinition<'a> {
    Expr(Expr<'a, Builtin>),
    Table(Box<Query<'a, Builtin>>),
}

/// `CREATE [OR REPLACE] [TEMPORARY | PERSISTENT] SECRET [IF NOT EXISTS] [name]
/// [IN storage] ( TYPE type [, key value ...] )`
#[derive(Debug, Clone, PartialEq)]
pub struct DuckDbCreateSecret<'a> {
    pub or_replace: bool,
    /// `Some(true)` for `TEMPORARY`, `Some(false)` for `PERSISTENT`.
    pub temporary: Option<bool>,
    pub if_not_exists: bool,
    pub name: Option<Ident<'a>>,
    pub storage: Option<Ident<'a>>,
    pub secret_type: Ident<'a>,
    pub options: Vec<(Ident<'a>, Ident<'a>)>,
}

/// `ATTACH [DATABASE] [IF NOT EXISTS] path [AS alias] [(options)]`
#[derive(Debug, Clone, PartialEq)]
pub struct DuckDbAttach<'a> {
    pub database: bool,
    pub if_not_exists: bool,
    pub path: Ident<'a>,
    pub alias: Option<Ident<'a>>,
    pub options: Vec<AttachOption<'a>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AttachOption<'a> {
    /// `READ_ONLY [true | false]`
    ReadOnly(Option<bool>),
    /// `TYPE name`
    Type(Ident<'a>),
}

fn duckdb_expr<'a>(expr: DuckDbExpr<'a>) -> Expr<'a, Builtin> {
    ExprKind::Ext(BuiltinExpr::DuckDb(expr)).into()
}

fn duckdb_statement<'a>(statement: DuckDbStatement<'a>) -> Statement<'a, Builtin> {
    StatementKind::Ext(BuiltinStatement::DuckDb(statement)).into()
}

#[inline]
fn is_name(token: Token<'_>) -> bool {
    matches!(token.kind, TokenKind::Word | TokenKind::QuotedIdent)
}

#[inline]
fn is_word_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_' || !byte.is_ascii()
}

/// Lexes a word holding `$`. Other words are left to the core.
#[inline]
fn lex_dollar_word(bytes: &[u8]) -> Option<LexResult> {
    if !(bytes[0].is_ascii_alphabetic() || bytes[0] == b'_' || !bytes[0].is_ascii()) {
        return None;
    }
    let len = bytes
        .iter()
        .take_while(|b| is_word_byte(**b) || **b == b'$')
        .count();
    bytes[..len].contains(&b'$').then_some(Ok(Lexeme {
        kind: TokenKind::Word,
        len,
    }))
}

/// Scans digits from `start`, each `_` between two of them. Returns the end
/// and whether an underscore was seen.
fn digit_run(bytes: &[u8], start: usize) -> Result<(usize, bool), &'static str> {
    let mut i = start;
    let mut underscore = false;
    while let Some(&byte) = bytes.get(i) {
        if byte == b'_' {
            if i == start || !bytes.get(i + 1).is_some_and(u8::is_ascii_digit) {
                return Err("invalid number");
            }
            underscore = true;
        } else if !byte.is_ascii_digit() {
            break;
        }
        i += 1;
    }
    Ok((i, underscore))
}

/// Lexes a number with `_` between its digits. Numbers without one are left to
/// the core.
fn lex_number_with_underscores(bytes: &[u8]) -> Option<LexResult> {
    let starts_number = bytes[0].is_ascii_digit()
        || (bytes[0] == b'.' && bytes.get(1).is_some_and(u8::is_ascii_digit));
    if !starts_number {
        return None;
    }
    let scan = || -> Result<Option<usize>, &'static str> {
        let (mut len, mut seen) = digit_run(bytes, 0)?;
        if crate::lexer::point_continues_number(bytes, len) {
            let (end, underscore) = digit_run(bytes, len + 1)?;
            len = end;
            seen |= underscore;
        }
        if matches!(bytes.get(len), Some(b'e' | b'E')) {
            let sign = usize::from(matches!(bytes.get(len + 1), Some(b'+' | b'-')));
            if bytes.get(len + 1 + sign).is_some_and(u8::is_ascii_digit) {
                let (end, underscore) = digit_run(bytes, len + 1 + sign)?;
                len = end;
                seen |= underscore;
            }
        }
        Ok(seen.then_some(len))
    };
    match scan() {
        Err(message) => Some(Err(message)),
        Ok(None) => None,
        Ok(Some(len)) if bytes.get(len).is_some_and(|b| is_word_byte(*b)) => {
            Some(Err("invalid number"))
        }
        Ok(Some(len)) => Some(Ok(Lexeme {
            kind: TokenKind::Number,
            len,
        })),
    }
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
        [b'{' | b'}', ..] => op(1),
        [b':', b':' | b'=', ..] | [b'/', b'/', ..] | [b'=', b'=', ..] => op(2),
        // Never a placeholder token: `:` also separates keys from values.
        [b':', ..] => op(1),
        // A placeholder, unlike PostgreSQL's operator.
        [b'?', ..] => None,
        _ => postgres::lex_prefixed_string(rest, false)
            .or_else(|| lex_number_with_underscores(bytes))
            .or_else(|| lex_dollar_word(bytes))
            .or_else(|| postgres::lex_token(rest)),
    }
}

/// Parses a name that can be any word or a string, as many of DuckDB's own
/// statements allow.
fn parse_name_or_string<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Ident<'a>> {
    let token = p.peek();
    if token.kind != TokenKind::String {
        return p.parse_name_part();
    }
    p.advance();
    Ok(Ident {
        text: Cow::Borrowed(&token.text[1..token.text.len() - 1]),
        quote: token.text.chars().next(),
        span: token.span,
    })
}

/// Parses items up to `close`, which is consumed. The list can be empty and
/// can end in a comma.
fn parse_list_until<'a, D: Dialect, T>(
    p: &mut Parser<'a, D>,
    close: &str,
    mut item: impl FnMut(&mut Parser<'a, D>) -> PResult<T>,
) -> PResult<Vec<T>> {
    let mut items = Vec::new();
    while !p.at_op(close) {
        items.push(item(p)?);
        if !p.eat_op(",") {
            break;
        }
    }
    p.expect_op(close)?;
    Ok(items)
}

fn parse_struct_literal<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Expr<'a, Builtin>> {
    p.expect_op("{")?;
    let fields = parse_list_until(p, "}", |p| {
        let key = parse_name_or_string(p)?;
        p.expect_op(":")?;
        Ok((key, p.parse_expr()?))
    })?;
    Ok(duckdb_expr(DuckDbExpr::Struct(fields)))
}

fn parse_map_literal<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Expr<'a, Builtin>> {
    p.expect_keyword("MAP")?;
    p.expect_op("{")?;
    let entries = parse_list_until(p, "}", |p| {
        let key = p.with_context(grammar::CONTEXT_COLON_SEPARATES, Parser::parse_expr)?;
        p.expect_op(":")?;
        Ok((key, p.parse_expr()?))
    })?;
    Ok(duckdb_expr(DuckDbExpr::Map(entries)))
}

fn parse_list_literal<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Expr<'a, Builtin>> {
    p.expect_op("[")?;
    let items = parse_list_until(p, "]", Parser::parse_expr)?;
    Ok(duckdb_expr(DuckDbExpr::List(items)))
}

/// True at `lambda x :`, `lambda x, y :` or `lambda (x, y) :`. Otherwise
/// `lambda` is an ordinary name.
fn at_keyword_lambda<'a, D: Dialect>(p: &Parser<'a, D>) -> bool {
    if !p.at_keyword("LAMBDA") {
        return false;
    }
    let parenthesized = p.peek_nth(1).is_op("(");
    let mut n = if parenthesized { 2 } else { 1 };
    loop {
        if !is_name(p.peek_nth(n)) {
            return false;
        }
        let separator = p.peek_nth(n + 1);
        if parenthesized && separator.is_op(")") {
            return p.peek_nth(n + 2).is_op(":");
        }
        if !parenthesized && separator.is_op(":") {
            return true;
        }
        if !separator.is_op(",") {
            return false;
        }
        n += 2;
    }
}

fn parse_keyword_lambda<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Expr<'a, Builtin>> {
    p.expect_keyword("LAMBDA")?;
    let parenthesized = p.eat_op("(");
    let params = p.parse_comma_separated(Parser::parse_name_part)?;
    if parenthesized {
        p.expect_op(")")?;
    }
    p.expect_op(":")?;
    let body = p.parse_expr()?;
    Ok(duckdb_expr(DuckDbExpr::Lambda(Box::new(DuckDbLambda {
        params,
        body,
    }))))
}

/// True at a `:` joined to the word after it, which makes a placeholder.
fn at_colon_placeholder<'a, D: Dialect>(p: &Parser<'a, D>) -> bool {
    let (colon, next) = (p.peek(), p.peek_nth(1));
    colon.is_op(":")
        && colon.span.end == next.span.start
        && matches!(next.kind, TokenKind::Word | TokenKind::Number)
}

pub fn parse_prefix<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Expr<'a, Builtin>> {
    if p.at_op("{") {
        return parse_struct_literal(p);
    }
    if p.at_op("[") {
        return parse_list_literal(p);
    }
    if p.at_keyword("MAP") && p.peek_nth(1).is_op("{") {
        return parse_map_literal(p);
    }
    if at_keyword_lambda(p) {
        return parse_keyword_lambda(p);
    }
    if at_colon_placeholder(p) {
        p.advance();
        let name = p.advance();
        return Ok(duckdb_expr(DuckDbExpr::ColonPlaceholder(name.text.into())));
    }
    if p.at_op("(") && p.has_context(COLUMN_OPTION) {
        return p.without_context(COLUMN_OPTION, grammar::parse_prefix);
    }
    grammar::parse_prefix(p)
}

/// Levels of tree height added for a node that wraps the expression before it
/// and passes through the dialect's wrapper types.
const POSTFIX_LEVELS: usize = 2;

/// Parses `.field` or `.method(args)` after anything but a number, where a
/// point followed by a letter is not a field.
fn parse_member<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
    receiver: Expr<'a, Builtin>,
    _bp: u8,
) -> PResult<Expr<'a, Builtin>> {
    if matches!(receiver.kind, ExprKind::Literal(Literal::Number(_))) {
        return Err(p.expected("end of the number"));
    }
    p.advance();
    let field = p.parse_name_part()?;
    p.deepen_by(POSTFIX_LEVELS)?;
    if !p.at_op("(") {
        return Ok(ExprKind::Special(Box::new(SpecialExpr::FieldAccess {
            expr: receiver,
            field,
        }))
        .into());
    }
    let name = ObjectName { parts: vec![field] };
    let call = p.parse_function_call(name)?;
    Ok(duckdb_expr(DuckDbExpr::MethodCall(Box::new(
        DuckDbMethodCall { receiver, call },
    ))))
}

/// Set while parsing a column option, where `NOT NULL` is the next constraint
/// and not a test on the expression before it. Parentheses end this.
const COLUMN_OPTION: u32 = 1;

/// Parses `NOT NULL` after an expression, which renders as `IS NOT NULL`.
fn parse_postfix_not_null<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
    left: Expr<'a, D::Ext>,
    _bp: u8,
) -> PResult<Expr<'a, D::Ext>> {
    p.advance();
    p.advance();
    Ok(ExprKind::Is {
        expr: Box::new(left),
        test: IsTest::Null,
        negated: true,
    }
    .into())
}

pub fn infix_rule<'a, D: Dialect<Ext = Builtin>>(p: &Parser<'a, D>) -> Option<InfixRule<'a, D>> {
    if p.at_op("//") {
        return Some(InfixRule::binary(prec::MUL, BinaryOp::Named("//".into())));
    }
    if p.at_op("==") {
        return Some(InfixRule::binary(prec::COMPARE, BinaryOp::Eq));
    }
    if p.at_op("[") {
        return Some(InfixRule::custom(
            prec::POSTFIX,
            grammar::parse_subscript_or_slice_with_step,
        ));
    }
    if p.at_op(".") && is_name(p.peek_nth(1)) {
        return Some(InfixRule::custom(prec::POSTFIX, parse_member));
    }
    if p.at_keyword("NOT") && p.peek_nth(1).is_word("NULL") && !p.has_context(COLUMN_OPTION) {
        return Some(InfixRule::custom(prec::IS, parse_postfix_not_null));
    }
    postgres::infix_rule(p)
}

/// Parses `name type, ...)` of a `STRUCT` or `UNION` type, after its `(`.
fn parse_type_fields<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Vec<StructField<'a>>> {
    let fields = p.parse_comma_separated(|p| {
        let name = parse_name_or_string(p)?;
        let data_type = p.nested(D::parse_data_type)?;
        Ok(StructField {
            not_null: false,
            options: Vec::new(),
            name: Some(name),
            colon: false,
            data_type,
            comment: None,
        })
    })?;
    p.expect_op(")")?;
    Ok(fields)
}

/// Parses a type, which can be `STRUCT(a INT)`, `UNION(a INT)` or `MAP(a, b)`.
/// Interval fields are rejected rather than leaving `DAY` in `x::INTERVAL DAY`
/// to be read as an alias.
pub fn parse_data_type<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<DataType<'a>> {
    let is_map = p.at_keyword("MAP") && p.peek_nth(1).is_op("(");
    if !(is_map || p.at_keyword("STRUCT") || p.at_keyword("UNION")) {
        if spark::at_interval_with_fields(p) {
            return Err(p.expected("an INTERVAL type without fields"));
        }
        let mut ty = grammar::parse_data_type(p)?;
        if ty.array == ArrayType::None && p.at_op("[") {
            ty.array = grammar::parse_array_brackets(p)?;
        }
        return Ok(ty);
    }
    let token = p.advance();
    p.expect_op("(")?;
    let arguments = if is_map {
        let types = p.parse_comma_separated(|p| p.nested(D::parse_data_type))?;
        p.expect_op(")")?;
        AngleArguments::ParenTypes(types)
    } else {
        AngleArguments::ParenFields(parse_type_fields(p)?)
    };
    let array = if p.at_op("[") {
        grammar::parse_array_brackets(p)?
    } else {
        ArrayType::None
    };
    Ok(DataType {
        qualifier: Vec::new(),
        name: Ident {
            text: token.text.into(),
            quote: None,
            span: token.span,
        },
        angle: Some(Box::new(arguments)),
        args: Vec::new(),
        suffix: DataTypeSuffix::None,
        array,
    })
}

/// Parses a select item, where a wildcard can end in `EXCLUDE` and `REPLACE`.
pub fn parse_select_item<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<SelectItem<'a, D::Ext>> {
    let item = grammar::parse_select_item(p)?;
    let (SelectItemKind::Wildcard | SelectItemKind::QualifiedWildcard(_)) = item.kind else {
        return Ok(item);
    };
    let exclude_parens = p.at_keyword("EXCLUDE") && p.peek_nth(1).is_op("(");
    let exclude = if !p.eat_keyword("EXCLUDE") {
        Vec::new()
    } else if exclude_parens {
        grammar::parse_ident_list(p)?
    } else {
        vec![p.parse_ident()?]
    };
    let replace = if p.at_keyword("REPLACE") {
        grammar::parse_wildcard_replace(p)?
    } else {
        Vec::new()
    };
    if exclude.is_empty() && replace.is_empty() {
        return Ok(item);
    }
    let qualifier = match item.kind {
        SelectItemKind::QualifiedWildcard(name) => Some(name),
        _ => None,
    };
    Ok(SelectItemKind::WildcardWithOptions {
        qualifier,
        options: Box::new(WildcardOptions {
            ilike: None,
            exclude,
            exclude_parens,
            except_keyword: false,
            replace,
            rename: Vec::new(),
            rename_parens: false,
        }),
    }
    .into())
}

/// True at `AS 'string'`, which an alias can be in place of a name.
fn at_as_string<'a, D: Dialect>(p: &Parser<'a, D>) -> bool {
    p.at_keyword("AS") && p.peek_nth(1).kind == TokenKind::String
}

/// Finishes a select item, whose alias can be a string after `AS`.
pub fn parse_select_item_alias<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
    expr: Expr<'a, D::Ext>,
) -> PResult<SelectItem<'a, D::Ext>> {
    if at_as_string(p) {
        p.advance();
        let alias = Some(parse_name_or_string(p)?);
        return Ok(SelectItemKind::Expr { expr, alias }.into());
    }
    grammar::parse_select_item_alias(p, expr)
}

fn parse_table_alias<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Option<TableAlias<'a>>> {
    let at_join = (p.at_keyword("SEMI") || p.at_keyword("ANTI")) && p.peek_nth(1).is_word("JOIN");
    if at_join {
        return Ok(None);
    }
    if !at_as_string(p) {
        return grammar::parse_optional_table_alias(p);
    }
    p.advance();
    let name = parse_name_or_string(p)?;
    let columns = if p.at_op("(") {
        grammar::parse_ident_list(p)?
    } else {
        Vec::new()
    };
    Ok(Some(TableAlias {
        name,
        explicit: true,
        columns,
        column_types: Vec::new(),
    }))
}

/// Parses a table and its alias, which can be a string after `AS`.
pub fn parse_table_ref<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<TableRef<'a, D::Ext>> {
    let factor = D::parse_table_factor(p)?;
    let alias = parse_table_alias(p)?;
    Ok(TableRef {
        factor,
        alias,
        span: Span::default(),
    })
}

/// Parses a common table expression, which can say whether to materialize.
pub fn parse_cte<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Cte<'a, D::Ext>> {
    let name = p.parse_ident()?;
    let columns = D::parse_cte_columns(p)?;
    p.expect_keyword("AS")?;
    let materialized = if p.eat_keyword("MATERIALIZED") {
        Some(true)
    } else if p.at_keyword("NOT") && p.peek_nth(1).is_word("MATERIALIZED") {
        p.advance();
        p.advance();
        Some(false)
    } else {
        None
    };
    p.expect_op("(")?;
    let query = p.nested(D::parse_query)?;
    p.expect_op(")")?;
    Ok(Cte {
        name,
        columns,
        materialized,
        query,
    })
}

/// Parses a set operator's quantifier and any `BY NAME`.
pub fn parse_set_quantifier<'a, D: Dialect>(p: &mut Parser<'a, D>) -> (SetQuantifier, bool) {
    let quantifier = grammar::parse_quantifier(p);
    let by_name = p.at_keyword("BY") && p.peek_nth(1).is_word("NAME");
    if by_name {
        p.advance();
        p.advance();
    }
    (quantifier, by_name)
}

/// Parses what follows `ORDER BY`, which can be `ALL` with its modifiers.
pub fn parse_query_order_by<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Vec<OrderByItem<'a, Builtin>>> {
    let all = p.peek().span;
    if !p.eat_keyword("ALL") {
        return p.parse_comma_separated(D::parse_order_by_item);
    }
    let (asc, nulls_first) = grammar::parse_order_by_modifiers(p)?;
    Ok(vec![OrderByItem {
        using: None,
        ext: Default::default(),
        expr: Expr::new(duckdb_expr(DuckDbExpr::AllColumns).kind, all),
        asc,
        nulls_first,
    }])
}

/// Parses a call's arguments, which can be empty before `ORDER BY`.
pub fn parse_function_args<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
    quantifier: SetQuantifier,
) -> PResult<Vec<FunctionArg<'a, D::Ext>>> {
    if quantifier != SetQuantifier::Distinct && p.at_keyword("ORDER") && p.peek_nth(1).is_word("BY")
    {
        return Ok(Vec::new());
    }
    if quantifier == SetQuantifier::None && p.at_op(")") {
        return Ok(Vec::new());
    }
    grammar::parse_list_with_trailing_comma(p, D::parse_function_arg)
}

/// Parses an argument, which can be named with `:=` or `=>`. An `=` compares,
/// as in `coalesce(x = 1, false)`.
pub fn parse_function_arg<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<FunctionArg<'a, D::Ext>> {
    if is_name(p.peek()) {
        let next = p.peek_nth(1);
        if let Some(operator) = [":="].into_iter().find(|op| next.is_op(op)) {
            let name = p.parse_name_part()?;
            p.advance();
            let value = p.parse_expr()?;
            return Ok(FunctionArg::NamedWith {
                name,
                operator: operator.into(),
                value,
            });
        }
    }
    // `x -> body` is a lambda only as an argument; elsewhere `->` extracts
    // from JSON, as DuckDB's binder decides.
    if spark::at_lambda(p) {
        return p.spanned_expr(spark::parse_lambda).map(FunctionArg::Expr);
    }
    grammar::parse_function_arg(p)
}

/// Parses a column option, which can be `AS (expr)` without `GENERATED`.
pub fn parse_column_option<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<Option<ColumnOption<'a, D::Ext>>> {
    if !(p.at_keyword("AS") && p.peek_nth(1).is_op("(")) {
        return p.with_context(COLUMN_OPTION, grammar::parse_column_option);
    }
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
    Ok(Some(ColumnOption::Generated(Box::new(Generated {
        when: Some(GeneratedWhen::Omitted),
        kind: GeneratedKind::Expr { expr, storage },
    }))))
}

/// The spellings of the date and time parts DuckDB knows, in lower case.
const DATE_PARTS: [&str; 79] = [
    "c",
    "cent",
    "centuries",
    "century",
    "d",
    "day",
    "dayofmonth",
    "dayofweek",
    "dayofyear",
    "days",
    "dec",
    "decade",
    "decades",
    "decs",
    "dow",
    "doy",
    "epoch",
    "era",
    "h",
    "hour",
    "hours",
    "hr",
    "hrs",
    "isodow",
    "isoyear",
    "julian",
    "m",
    "microsecond",
    "microseconds",
    "mil",
    "millenia",
    "millennia",
    "millenium",
    "millennium",
    "millisecond",
    "milliseconds",
    "mils",
    "min",
    "mins",
    "minute",
    "minutes",
    "mon",
    "mons",
    "month",
    "months",
    "ms",
    "msec",
    "msecond",
    "mseconds",
    "msecs",
    "nanosecond",
    "nanoseconds",
    "ns",
    "quarter",
    "quarters",
    "s",
    "sec",
    "second",
    "seconds",
    "secs",
    "timezone",
    "timezone_hour",
    "timezone_minute",
    "us",
    "usec",
    "usecond",
    "useconds",
    "usecs",
    "w",
    "week",
    "weekday",
    "weekofyear",
    "weeks",
    "y",
    "year",
    "years",
    "yearweek",
    "yr",
    "yrs",
];

/// Parses the part `EXTRACT` takes: a core date or time field, one of DuckDB's
/// other spellings of one, such as `us`, or a string.
pub fn parse_extract_field<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<&'a str> {
    let token = p.peek();
    let known = token.kind == TokenKind::Word
        && DATE_PARTS
            .iter()
            .any(|part| token.text.eq_ignore_ascii_case(part));
    if token.kind == TokenKind::String || known {
        p.advance();
        return Ok(token.text);
    }
    grammar::parse_date_time_field(p)
}

/// Parses `[TEMPORARY] [PERSISTENT]`. `temp` allows the short `TEMP` too.
fn parse_persistence<'a, D: Dialect>(p: &mut Parser<'a, D>, temp: bool) -> PResult<Option<bool>> {
    let temporary = p.eat_keyword("TEMPORARY") || (temp && p.eat_keyword("TEMP"));
    let persistent = p.eat_keyword("PERSISTENT");
    match (temporary, persistent) {
        (true, true) => Err(p.expected("only one of TEMPORARY and PERSISTENT")),
        (true, false) => Ok(Some(true)),
        (false, true) => Ok(Some(false)),
        (false, false) => Ok(None),
    }
}

/// True if a word in `objects` comes after the modifiers of a `CREATE` or `DROP`.
fn at_object_after_modifiers<'a, D: Dialect>(p: &Parser<'a, D>, objects: &[&str]) -> bool {
    let mut n = 1;
    if p.peek_nth(n).is_word("OR") {
        n += 2;
    }
    while ["TEMPORARY", "TEMP", "PERSISTENT"]
        .iter()
        .any(|word| p.peek_nth(n).is_word(word))
    {
        n += 1;
    }
    objects.iter().any(|word| p.peek_nth(n).is_word(word))
}

fn parse_macro_arg<'a, D: Dialect<Ext = Builtin>>(p: &mut Parser<'a, D>) -> PResult<MacroArg<'a>> {
    let name = parse_name_or_string(p)?;
    let default = if p.eat_op(":=") || p.eat_op("=>") {
        Some(p.parse_expr()?)
    } else {
        None
    };
    Ok(MacroArg { name, default })
}

fn parse_create_macro<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<DuckDbCreateMacro<'a>> {
    p.expect_keyword("CREATE")?;
    let or_replace = grammar::parse_or_replace(p)?;
    let temporary = p.eat_keyword("TEMPORARY") || p.eat_keyword("TEMP");
    if !p.eat_keyword("MACRO") {
        p.expect_keyword("FUNCTION")?;
    }
    let name = grammar::parse_object_name(p)?;
    p.expect_op("(")?;
    let args = if p.eat_op(")") {
        None
    } else {
        let args = p.parse_comma_separated(parse_macro_arg)?;
        p.expect_op(")")?;
        Some(args)
    };
    p.expect_keyword("AS")?;
    let definition = if p.eat_keyword("TABLE") {
        MacroDefinition::Table(Box::new(D::parse_query(p)?))
    } else {
        MacroDefinition::Expr(p.parse_expr()?)
    };
    Ok(DuckDbCreateMacro {
        or_replace,
        temporary,
        name,
        args,
        definition,
    })
}

fn parse_create_secret<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<DuckDbCreateSecret<'a>> {
    p.expect_keyword("CREATE")?;
    let or_replace = grammar::parse_or_replace(p)?;
    let temporary = parse_persistence(p, true)?;
    p.expect_keyword("SECRET")?;
    let if_not_exists = grammar::parse_if_not_exists(p)?;
    let mut name = None;
    let mut storage = None;
    if !p.at_op("(") {
        if p.eat_keyword("IN") {
            storage = Some(parse_name_or_string(p)?);
        } else {
            name = Some(parse_name_or_string(p)?);
            if p.eat_keyword("IN") {
                storage = Some(parse_name_or_string(p)?);
            }
        }
    }
    p.expect_op("(")?;
    p.expect_keyword("TYPE")?;
    let secret_type = parse_name_or_string(p)?;
    let options = if p.eat_op(",") {
        p.parse_comma_separated(|p| {
            let key = parse_name_or_string(p)?;
            Ok((key, parse_name_or_string(p)?))
        })?
    } else {
        Vec::new()
    };
    p.expect_op(")")?;
    Ok(DuckDbCreateSecret {
        or_replace,
        temporary,
        if_not_exists,
        name,
        storage,
        secret_type,
        options,
    })
}

/// Parses `CREATE`, handling DuckDB's macros and secrets.
pub fn parse_create<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Statement<'a, Builtin>> {
    if at_object_after_modifiers(p, &["MACRO", "FUNCTION"]) {
        let create = parse_create_macro(p)?;
        return Ok(duckdb_statement(DuckDbStatement::CreateMacro(Box::new(
            create,
        ))));
    }
    if at_object_after_modifiers(p, &["SECRET"]) {
        let create = parse_create_secret(p)?;
        return Ok(duckdb_statement(DuckDbStatement::CreateSecret(Box::new(
            create,
        ))));
    }
    grammar::parse_create(p)
}

/// Parses `CREATE TYPE name AS ENUM (...)`, which is the form DuckDB shares
/// with PostgreSQL.
pub fn parse_create_type<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
    flags: CreateFlags,
) -> PResult<Option<Statement<'a, Builtin>>> {
    if flags != CreateFlags::default() || !p.eat_keyword("TYPE") {
        return Ok(None);
    }
    let parsed = postgres::parse_create_type(p)?;
    let is_enum = matches!(
        &parsed,
        postgres::PostgresStatement::CreateType(create)
            if matches!(**create, postgres::CreateType::Enum { .. })
    );
    if !is_enum {
        return Err(p.expected("AS ENUM"));
    }
    Ok(Some(
        StatementKind::Ext(BuiltinStatement::Postgres(parsed)).into(),
    ))
}

fn parse_drop_secret<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<DuckDbStatement<'a>> {
    p.expect_keyword("DROP")?;
    let temporary = parse_persistence(p, false)?;
    p.expect_keyword("SECRET")?;
    let if_exists = p.at_keyword("IF");
    if if_exists {
        p.advance();
        p.expect_keyword("EXISTS")?;
    }
    let name = parse_name_or_string(p)?;
    let storage = if p.eat_keyword("FROM") {
        Some(parse_name_or_string(p)?)
    } else {
        None
    };
    Ok(DuckDbStatement::DropSecret {
        temporary,
        if_exists,
        name,
        storage,
    })
}

fn parse_attach_options<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Vec<AttachOption<'a>>> {
    if !p.eat_op("(") {
        return Ok(Vec::new());
    }
    let options = p.parse_comma_separated(|p| {
        if p.eat_keyword("READ_ONLY") {
            let value = if p.eat_keyword("TRUE") {
                Some(true)
            } else if p.eat_keyword("FALSE") {
                Some(false)
            } else {
                None
            };
            return Ok(AttachOption::ReadOnly(value));
        }
        p.expect_keyword("TYPE")?;
        parse_name_or_string(p).map(AttachOption::Type)
    })?;
    p.expect_op(")")?;
    Ok(options)
}

fn parse_attach<'a, D: Dialect<Ext = Builtin>>(p: &mut Parser<'a, D>) -> PResult<DuckDbAttach<'a>> {
    p.expect_keyword("ATTACH")?;
    let database = p.eat_keyword("DATABASE");
    let if_not_exists = grammar::parse_if_not_exists(p)?;
    let path = parse_name_or_string(p)?;
    let alias = if p.eat_keyword("AS") {
        Some(parse_name_or_string(p)?)
    } else {
        None
    };
    let options = parse_attach_options(p)?;
    Ok(DuckDbAttach {
        database,
        if_not_exists,
        path,
        alias,
        options,
    })
}

fn parse_detach<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<DuckDbStatement<'a>> {
    p.expect_keyword("DETACH")?;
    let database = p.eat_keyword("DATABASE");
    let if_exists = p.at_keyword("IF");
    if if_exists {
        p.advance();
        p.expect_keyword("EXISTS")?;
    }
    let name = parse_name_or_string(p)?;
    Ok(DuckDbStatement::Detach {
        database,
        if_exists,
        name,
    })
}

pub fn parse_statement<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Statement<'a, Builtin>> {
    if p.at_keyword("ABORT") {
        return grammar::parse_abort(p);
    }
    if p.at_keyword("ANALYZE") {
        return grammar::parse_analyze(p);
    }
    if p.at_keyword("RESET") {
        return postgres::utility::parse_reset(p)
            .map(|s| StatementKind::Ext(BuiltinStatement::Postgres(s)).into());
    }
    if let Some(prepared) = postgres::utility::parse_prepared_statement(p)? {
        return Ok(StatementKind::Ext(BuiltinStatement::Postgres(prepared)).into());
    }
    if p.at_keyword("ATTACH") {
        let attach = parse_attach(p)?;
        return Ok(duckdb_statement(DuckDbStatement::Attach(Box::new(attach))));
    }
    if p.at_keyword("DETACH") {
        return parse_detach(p).map(duckdb_statement);
    }
    if p.eat_keyword("INSTALL") {
        return parse_name_or_string(p)
            .map(DuckDbStatement::Install)
            .map(duckdb_statement);
    }
    if p.eat_keyword("LOAD") {
        return parse_name_or_string(p)
            .map(DuckDbStatement::Load)
            .map(duckdb_statement);
    }
    if p.at_keyword("DROP") && at_object_after_modifiers(p, &["SECRET"]) {
        return parse_drop_secret(p).map(duckdb_statement);
    }
    grammar::parse_statement(p)
}

/// Parses a `SELECT`, which can start with its `FROM` clause.
pub fn parse_select<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Select<'a, D::Ext>> {
    if p.at_keyword("FROM") {
        grammar::parse_from_first_select(p)
    } else {
        grammar::parse_select(p)
    }
}

#[inline]
pub fn is_reserved(word: &str) -> bool {
    ["RETURNING", "NOTNULL", "ISNULL"]
        .iter()
        .any(|reserved| reserved.eq_ignore_ascii_case(word))
        || grammar::is_reserved(word)
}

#[inline]
pub fn is_reserved_for_alias(word: &str) -> bool {
    ["QUALIFY", "ASOF"]
        .iter()
        .any(|reserved| reserved.eq_ignore_ascii_case(word))
        || grammar::is_reserved_for_alias(word)
}

impl Dialect for DuckDb {
    type Ext = Builtin;

    fn parse_create_table_query<'a>(p: &mut Parser<'a, Self>) -> PResult<Query<'a, Builtin>> {
        grammar::parse_table_or_query(p)
    }

    fn parse_create_extension<'a>(
        p: &mut Parser<'a, Self>,
        flags: CreateFlags,
    ) -> PResult<Option<Statement<'a, Builtin>>> {
        parse_create_type(p, flags)
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

    fn eat_limit_all<'a>(p: &mut Parser<'a, Self>) -> bool {
        p.eat_keyword("ALL")
    }

    fn parse_statement_after_with<'a>(
        p: &mut Parser<'a, Self>,
    ) -> PResult<Option<Statement<'a, Builtin>>> {
        grammar::parse_dml_after_with(p, true, true)
    }

    fn parse_insert_source<'a>(p: &mut Parser<'a, Self>) -> PResult<InsertSource<'a, Builtin>> {
        grammar::parse_insert_source_with_default(p)
    }

    fn parse_drop_object_type<'a>(p: &mut Parser<'a, Self>) -> Option<ObjectType<'a>> {
        grammar::eat_drop_object_type(p, &["TYPE"])
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
        grammar::parse_create_schema_body(p, flags)
    }

    #[inline]
    fn lex_token(rest: &str) -> Option<LexResult> {
        lex_token(rest)
    }

    #[inline]
    fn is_reserved(word: &str) -> bool {
        is_reserved(word)
    }

    #[inline]
    fn is_reserved_for_alias(word: &str) -> bool {
        is_reserved_for_alias(word)
    }

    fn parse_statement<'a>(p: &mut Parser<'a, Self>) -> PResult<Statement<'a, Builtin>> {
        parse_statement(p)
    }

    fn parse_create<'a>(p: &mut Parser<'a, Self>) -> PResult<Statement<'a, Builtin>> {
        parse_create(p)
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

    fn parse_select<'a>(p: &mut Parser<'a, Self>) -> PResult<Select<'a, Builtin>> {
        parse_select(p)
    }

    fn at_from_first<'a>(p: &Parser<'a, Self>, n: usize) -> bool {
        p.peek_nth(n).is_word("FROM")
    }

    fn parse_select_item<'a>(p: &mut Parser<'a, Self>) -> PResult<SelectItem<'a, Builtin>> {
        parse_select_item(p)
    }

    fn parse_select_item_alias<'a>(
        p: &mut Parser<'a, Self>,
        expr: Expr<'a, Builtin>,
    ) -> PResult<SelectItem<'a, Builtin>> {
        parse_select_item_alias(p, expr)
    }

    fn parse_projection<'a>(p: &mut Parser<'a, Self>) -> PResult<Vec<SelectItem<'a, Builtin>>> {
        grammar::parse_list_with_trailing_comma(p, Parser::parse_select_item)
    }

    fn parse_table_ref<'a>(p: &mut Parser<'a, Self>) -> PResult<TableRef<'a, Builtin>> {
        parse_table_ref(p)
    }

    fn parse_join<'a>(p: &mut Parser<'a, Self>) -> PResult<Option<Join<'a, Builtin>>> {
        spark::parse_join(p)
    }

    fn parse_cte<'a>(p: &mut Parser<'a, Self>) -> PResult<Cte<'a, Builtin>> {
        parse_cte(p)
    }

    fn string_literal(token: Token<'_>) -> crate::ast::StringLiteral<'_> {
        postgres::string_literal(token, false)
    }

    fn parse_set_quantifier<'a>(p: &mut Parser<'a, Self>) -> (SetQuantifier, bool) {
        parse_set_quantifier(p)
    }

    fn parse_select_modifiers<'a>(
        p: &mut Parser<'a, Self>,
    ) -> PResult<crate::dialect::builtin::BuiltinSelect<'a>> {
        postgres::parse_distinct_on(p)
    }

    fn parse_query_order_by<'a>(
        p: &mut Parser<'a, Self>,
    ) -> PResult<Vec<OrderByItem<'a, Builtin>>> {
        parse_query_order_by(p)
    }

    fn parse_function_args<'a>(
        p: &mut Parser<'a, Self>,
        quantifier: SetQuantifier,
    ) -> PResult<Vec<FunctionArg<'a, Builtin>>> {
        parse_function_args(p, quantifier)
    }

    fn parse_function_arg<'a>(p: &mut Parser<'a, Self>) -> PResult<FunctionArg<'a, Builtin>> {
        parse_function_arg(p)
    }

    fn parse_extract_field<'a>(p: &mut Parser<'a, Self>) -> PResult<&'a str> {
        parse_extract_field(p)
    }

    fn parse_between_low<'a>(p: &mut Parser<'a, Self>) -> PResult<Expr<'a, Builtin>> {
        let low = grammar::parse_between_low(p)?;
        spark::parse_between_bound(p, low)
    }

    fn parse_qualify<'a>(p: &mut Parser<'a, Self>) -> PResult<Option<Expr<'a, Builtin>>> {
        snowflake::parse_qualify(p)
    }

    fn eat_group_by_all<'a>(p: &mut Parser<'a, Self>) -> bool {
        p.eat_keyword("ALL")
    }

    fn parse_returning<'a>(p: &mut Parser<'a, Self>) -> PResult<Vec<SelectItem<'a, Builtin>>> {
        postgres::parse_returning(p)
    }

    fn parse_interval<'a>(p: &mut Parser<'a, Self>) -> PResult<Expr<'a, Builtin>> {
        grammar::parse_interval(p, false)
    }

    fn parse_explain_options<'a>(p: &mut Parser<'a, Self>) -> PResult<Vec<ExplainOption<'a>>> {
        grammar::parse_explain_option_list(p)
    }

    /// A built-in type name can introduce a string: `UUID '...'`.
    fn is_typed_string_type(name: &str) -> bool {
        grammar::is_typed_string_type(name)
            || TYPED_STRING_TYPES
                .iter()
                .any(|type_name| name.eq_ignore_ascii_case(type_name))
    }

    fn parse_add_column_if_not_exists<'a>(p: &mut Parser<'a, Self>) -> PResult<bool> {
        grammar::parse_if_not_exists(p)
    }

    fn eat_drop_temporary<'a>(p: &mut Parser<'a, Self>) -> bool {
        p.eat_keyword("TEMPORARY")
    }

    fn parse_column_option<'a>(
        p: &mut Parser<'a, Self>,
    ) -> PResult<Option<ColumnOption<'a, Builtin>>> {
        parse_column_option(p)
    }
}

/// Renders the expressions that cannot form long left-deep chains.
fn write_non_chain_expr(f: &mut fmt::Formatter<'_>, expr: &DuckDbExpr<'_>) -> fmt::Result {
    match expr {
        DuckDbExpr::Struct(fields) => {
            f.write_str("{")?;
            for (i, (key, value)) in fields.iter().enumerate() {
                if i > 0 {
                    f.write_str(", ")?;
                }
                write!(f, "{key}: {value}")?;
            }
            f.write_str("}")
        }
        DuckDbExpr::Map(entries) => {
            f.write_str("MAP {")?;
            for (i, (key, value)) in entries.iter().enumerate() {
                if i > 0 {
                    f.write_str(", ")?;
                }
                write!(f, "{key}: {value}")?;
            }
            f.write_str("}")
        }
        DuckDbExpr::List(items) => write!(f, "[{}]", comma(items)),
        DuckDbExpr::Lambda(lambda) => {
            write!(f, "lambda {} : {}", comma(&lambda.params), lambda.body)
        }
        DuckDbExpr::AllColumns => f.write_str("ALL"),
        DuckDbExpr::ColonPlaceholder(name) => write!(f, ":{name}"),
        DuckDbExpr::MethodCall(_) => {
            unreachable!("rendered by DuckDbExpr::fmt")
        }
    }
}

impl fmt::Display for DuckDbExpr<'_> {
    /// Method calls can form long left-deep chains, so this avoids
    /// `write!` as [`Expr`]'s rendering does.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MethodCall(call) => {
                call.receiver.fmt(f)?;
                f.write_str(".")?;
                call.call.fmt(f)
            }
            other => write_non_chain_expr(f, other),
        }
    }
}

impl fmt::Display for MacroArg<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.name.fmt(f)?;
        match &self.default {
            Some(default) => write!(f, " := {default}"),
            None => Ok(()),
        }
    }
}

fn persistence(temporary: Option<bool>) -> &'static str {
    match temporary {
        Some(true) => "TEMPORARY ",
        Some(false) => "PERSISTENT ",
        None => "",
    }
}

impl fmt::Display for DuckDbCreateMacro<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let or_replace = if self.or_replace { "OR REPLACE " } else { "" };
        let temporary = if self.temporary { "TEMPORARY " } else { "" };
        write!(f, "CREATE {or_replace}{temporary}MACRO {}(", self.name)?;
        if let Some(args) = &self.args {
            comma(args).fmt(f)?;
        }
        f.write_str(")")?;
        match &self.definition {
            MacroDefinition::Expr(expr) => write!(f, " AS {expr}"),
            MacroDefinition::Table(query) => write!(f, " AS TABLE {query}"),
        }
    }
}

impl fmt::Display for DuckDbCreateSecret<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let or_replace = if self.or_replace { "OR REPLACE " } else { "" };
        write!(
            f,
            "CREATE {or_replace}{}SECRET ",
            persistence(self.temporary)
        )?;
        write_if_not_exists(f, self.if_not_exists)?;
        if let Some(name) = &self.name {
            write!(f, "{name} ")?;
        }
        if let Some(storage) = &self.storage {
            write!(f, "IN {storage} ")?;
        }
        write!(f, "( TYPE {}", self.secret_type)?;
        for (key, value) in &self.options {
            write!(f, ", {key} {value}")?;
        }
        f.write_str(" )")
    }
}

impl fmt::Display for AttachOption<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ReadOnly(None) => f.write_str("READ_ONLY"),
            Self::ReadOnly(Some(value)) => write!(f, "READ_ONLY {value}"),
            Self::Type(kind) => write!(f, "TYPE {kind}"),
        }
    }
}

impl fmt::Display for DuckDbAttach<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ATTACH ")?;
        if self.database {
            f.write_str("DATABASE ")?;
        }
        write_if_not_exists(f, self.if_not_exists)?;
        self.path.fmt(f)?;
        if let Some(alias) = &self.alias {
            write!(f, " AS {alias}")?;
        }
        if !self.options.is_empty() {
            write!(f, " ({})", comma(&self.options))?;
        }
        Ok(())
    }
}

impl fmt::Display for DuckDbStatement<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CreateMacro(create) => create.fmt(f),
            Self::CreateSecret(create) => create.fmt(f),
            Self::DropSecret {
                temporary,
                if_exists,
                name,
                storage,
            } => {
                let if_exists = if *if_exists { "IF EXISTS " } else { "" };
                write!(
                    f,
                    "DROP {}SECRET {if_exists}{name}",
                    persistence(*temporary)
                )?;
                match storage {
                    Some(storage) => write!(f, " FROM {storage}"),
                    None => Ok(()),
                }
            }
            Self::Attach(attach) => attach.fmt(f),
            Self::Detach {
                database,
                if_exists,
                name,
            } => {
                let database = if *database { "DATABASE " } else { "" };
                let if_exists = if *if_exists { "IF EXISTS " } else { "" };
                write!(f, "DETACH {database}{if_exists}{name}")
            }
            Self::Install(name) => write!(f, "INSTALL {name}"),
            Self::Load(name) => write!(f, "LOAD {name}"),
        }
    }
}

/// DuckDB's built-in types that can introduce a string literal, besides the
/// standard date and time types.
const TYPED_STRING_TYPES: &[&str] = &[
    "BIGINT",
    "INT8",
    "LONG",
    "BIT",
    "BITSTRING",
    "BLOB",
    "BYTEA",
    "BINARY",
    "VARBINARY",
    "BOOLEAN",
    "BOOL",
    "LOGICAL",
    "DECIMAL",
    "NUMERIC",
    "DOUBLE",
    "FLOAT8",
    "FLOAT",
    "FLOAT4",
    "REAL",
    "HUGEINT",
    "INTEGER",
    "INT4",
    "INT",
    "SIGNED",
    "SMALLINT",
    "INT2",
    "SHORT",
    "TINYINT",
    "INT1",
    "UBIGINT",
    "UHUGEINT",
    "UINTEGER",
    "USMALLINT",
    "UTINYINT",
    "UUID",
    "VARCHAR",
    "CHAR",
    "BPCHAR",
    "TEXT",
    "STRING",
];

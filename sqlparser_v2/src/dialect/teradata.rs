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

//! Teradata.

use std::borrow::Cow;
use std::fmt;

use crate::PResult;
use crate::ast::{
    CreateFlags, CreateTable, Expr, ExprKind, Ident, NullTreatment, ObjectName, ObjectType,
    Quantifier, SelectInto, Statement, StatementKind, StringLiteral, TableFactor, TableWithJoins,
};
use crate::dialect::builtin::{BuiltinExpr, BuiltinSelect, BuiltinStatement};
use crate::dialect::snowflake;
use crate::dialect::{Builtin, Dialect};
use crate::display::{comma, write_columns, write_if_not_exists};
use crate::grammar;
use crate::lexer::{LexResult, Lexeme, TokenKind};
use crate::parser::{Parser, SetOperatorRule};

#[derive(Debug, Clone, PartialEq)]
pub struct Teradata;

/// Statements only Teradata has.
#[derive(Debug, Clone, PartialEq)]
pub enum TeradataStatement<'a> {
    CreateTable(Box<TeradataCreateTable<'a>>),
    Execute(Box<TeradataExecute<'a>>),
}

/// Expressions only Teradata has.
#[derive(Debug, Clone, PartialEq)]
pub enum TeradataExpr<'a> {
    /// `'a' 'b'`, which is the single string `'ab'`.
    AdjacentStrings(Vec<StringLiteral<'a>>),
}

impl TeradataExpr<'_> {
    /// The value of the joined strings.
    pub fn value(&self) -> String {
        match self {
            Self::AdjacentStrings(parts) => parts.iter().map(|part| part.value()).collect(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TeradataSetKind {
    Set,
    Multiset,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TeradataOnCommit {
    DeleteRows,
    PreserveRows,
}

/// `WITH [NO] DATA [AND [NO] STATISTICS]`
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TeradataWithData {
    pub data: bool,
    pub statistics: Option<bool>,
}

/// `CREATE TABLE` with clauses only Teradata has.
#[derive(Debug, Clone, PartialEq)]
pub struct TeradataCreateTable<'a> {
    pub table: CreateTable<'a, Builtin>,
    pub set_kind: Option<TeradataSetKind>,
    pub global: bool,
    pub volatile: bool,
    /// `, FALLBACK` is true and `, NO FALLBACK` is false.
    pub fallback: Option<bool>,
    pub on_commit: Option<TeradataOnCommit>,
    /// True if `ON COMMIT` was written after the query.
    pub on_commit_last: bool,
    pub with_data: Option<TeradataWithData>,
}

/// One `expr [AS alias]` of `EXECUTE ... USING`.
#[derive(Debug, Clone, PartialEq)]
pub struct TeradataUsing<'a> {
    pub expr: Expr<'a, Builtin>,
    pub alias: Option<Ident<'a>>,
}

/// `EXECUTE [IMMEDIATE] ...`
#[derive(Debug, Clone, PartialEq)]
pub struct TeradataExecute<'a> {
    pub immediate: bool,
    pub name: Option<ObjectName<'a>>,
    pub parameters: Vec<Expr<'a, Builtin>>,
    pub parenthesized: bool,
    pub into: Vec<Ident<'a>>,
    pub using: Vec<TeradataUsing<'a>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TeradataTopQuantity<'a> {
    Number(Cow<'a, str>),
    Expr(Expr<'a, Builtin>),
}

/// `TOP` beyond a plain number: `TOP (expr)`, `PERCENT` and `WITH TIES`.
#[derive(Debug, Clone, PartialEq)]
pub struct TeradataTop<'a> {
    pub quantity: TeradataTopQuantity<'a>,
    pub percent: bool,
    pub with_ties: bool,
}

fn teradata(statement: TeradataStatement<'_>) -> Statement<'_, Builtin> {
    StatementKind::Ext(BuiltinStatement::Teradata(statement)).into()
}

/// The length of the identifier character at the start of `rest`, or `None`
/// if there is none.
#[inline]
fn name_char_len(rest: &str) -> Option<usize> {
    let byte = *rest.as_bytes().first()?;
    if byte.is_ascii() {
        let is_part = byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'#' | b'$');
        return is_part.then_some(1);
    }
    let ch = rest.chars().next()?;
    ch.is_alphanumeric().then(|| ch.len_utf8())
}

#[inline]
fn name_len(rest: &str) -> usize {
    let mut len = 0;
    while let Some(part) = name_char_len(&rest[len..]) {
        len += part;
    }
    len
}

/// Lexes names holding `#`, `$` or letters beyond ASCII. Anything else is left
/// to the core lexer.
#[inline]
pub fn lex_token(rest: &str) -> Option<LexResult> {
    let bytes = rest.as_bytes();
    let word = |len| {
        Some(Ok(Lexeme {
            kind: TokenKind::Word,
            len,
        }))
    };
    match bytes[0] {
        b'$' => word(name_len(rest)),
        // `#-`, `#>` and a lone `#` are operators.
        b'#' => match bytes.get(1) {
            None | Some(b' ' | b'-' | b'>') => None,
            Some(_) if name_char_len(&rest[1..]).is_none() => None,
            Some(_) => word(name_len(rest)),
        },
        first if first.is_ascii_alphabetic() || first == b'_' => {
            let len = name_len(rest);
            let plain = bytes[..len]
                .iter()
                .all(|b| b.is_ascii_alphanumeric() || *b == b'_');
            // A non-ASCII byte here is not part of the name, but the core
            // lexer would take it as part of one.
            if plain && bytes.get(len).is_none_or(u8::is_ascii) {
                None
            } else {
                word(len)
            }
        }
        first if !first.is_ascii() => {
            if rest.chars().next().is_some_and(char::is_alphabetic) {
                word(name_len(rest))
            } else {
                Some(Err("unexpected character"))
            }
        }
        _ => None,
    }
}

/// True if the words after `CREATE` lead to `TABLE`.
fn at_create_table<'a, D: Dialect>(p: &Parser<'a, D>) -> bool {
    let mut n = 1;
    if p.peek_nth(n).is_word("OR") && p.peek_nth(n + 1).is_word("REPLACE") {
        n += 2;
    }
    if p.peek_nth(n).is_word("SET") || p.peek_nth(n).is_word("MULTISET") {
        n += 1;
    }
    if p.peek_nth(n).is_word("GLOBAL") {
        n += 1;
    }
    if p.peek_nth(n).is_word("TEMPORARY") || p.peek_nth(n).is_word("TEMP") {
        n += 1;
    }
    if p.peek_nth(n).is_word("VOLATILE") {
        n += 1;
    }
    p.peek_nth(n).is_word("TABLE")
}

/// Parses `, FALLBACK` or `, NO FALLBACK` after the table's name.
fn parse_fallback<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Option<bool>> {
    if !p.eat_op(",") {
        return Ok(None);
    }
    if p.eat_keyword("FALLBACK") {
        return Ok(Some(true));
    }
    p.expect_keyword("NO")?;
    p.expect_keyword("FALLBACK")?;
    Ok(Some(false))
}

fn parse_on_commit<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Option<TeradataOnCommit>> {
    if !(p.at_keyword("ON") && p.peek_nth(1).is_word("COMMIT")) {
        return Ok(None);
    }
    p.advance();
    p.advance();
    let action = if p.eat_keyword("DELETE") {
        TeradataOnCommit::DeleteRows
    } else {
        p.expect_keyword("PRESERVE")?;
        TeradataOnCommit::PreserveRows
    };
    p.expect_keyword("ROWS")?;
    Ok(Some(action))
}

fn parse_with_data<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Option<TeradataWithData>> {
    if !p.at_keyword("WITH") {
        return Ok(None);
    }
    let data = if p.peek_nth(1).is_word("DATA") {
        true
    } else if p.peek_nth(1).is_word("NO") && p.peek_nth(2).is_word("DATA") {
        false
    } else {
        return Ok(None);
    };
    p.advance();
    if !data {
        p.advance();
    }
    p.advance();

    let statistics = if p.eat_keyword("AND") {
        let with_statistics = !p.eat_keyword("NO");
        p.expect_keyword("STATISTICS")?;
        Some(with_statistics)
    } else {
        None
    };
    Ok(Some(TeradataWithData { data, statistics }))
}

/// Parses `CREATE TABLE` with Teradata's clauses. A table that uses none of
/// them is a core node.
pub fn parse_create_table<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Statement<'a, Builtin>> {
    p.expect_keyword("CREATE")?;
    let or_replace = grammar::parse_or_replace(p)?;
    let set_kind = if p.eat_keyword("SET") {
        Some(TeradataSetKind::Set)
    } else if p.eat_keyword("MULTISET") {
        Some(TeradataSetKind::Multiset)
    } else {
        None
    };
    let global = p.eat_keyword("GLOBAL");
    let temporary = p.eat_keyword("TEMPORARY") || p.eat_keyword("TEMP");
    let volatile = p.eat_keyword("VOLATILE");
    p.expect_keyword("TABLE")?;

    let if_not_exists = grammar::parse_if_not_exists(p)?;
    let name = grammar::parse_object_name(p)?;
    let fallback = parse_fallback(p)?;
    let (columns, constraints) = if p.at_op("(") {
        grammar::parse_columns(p)?
    } else {
        (Vec::new(), Vec::new())
    };
    let mut on_commit = parse_on_commit(p)?;

    let query = if p.eat_keyword("AS") || p.at_keyword("SELECT") {
        Some(Box::new(D::parse_query(p)?))
    } else {
        None
    };
    if columns.is_empty() && query.is_none() {
        return Err(p.expected("column definitions or AS"));
    }
    let with_data = if query.is_some() {
        parse_with_data(p)?
    } else {
        None
    };
    // The form Teradata documents puts this last, after `WITH DATA`.
    let mut on_commit_last = false;
    if on_commit.is_none() && with_data.is_some() {
        on_commit = parse_on_commit(p)?;
        on_commit_last = on_commit.is_some();
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
    let is_core = set_kind.is_none()
        && !global
        && !volatile
        && fallback.is_none()
        && on_commit.is_none()
        && with_data.is_none();
    if is_core {
        return Ok(StatementKind::CreateTable(Box::new(table)).into());
    }
    Ok(teradata(TeradataStatement::CreateTable(Box::new(
        TeradataCreateTable {
            table,
            set_kind,
            global,
            volatile,
            fallback,
            on_commit,
            on_commit_last,
            with_data,
        },
    ))))
}

pub fn parse_create<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Statement<'a, Builtin>> {
    if at_create_table(p) {
        return parse_create_table(p);
    }
    grammar::parse_create(p)
}

fn parse_using<'a, D: Dialect<Ext = Builtin>>(p: &mut Parser<'a, D>) -> PResult<TeradataUsing<'a>> {
    let expr = p.parse_expr()?;
    let alias = if p.eat_keyword("AS") {
        Some(p.parse_ident()?)
    } else {
        None
    };
    Ok(TeradataUsing { expr, alias })
}

fn at_execute_end<'a, D: Dialect>(p: &Parser<'a, D>) -> bool {
    let token = p.peek();
    token.kind == TokenKind::Eof || token.is_op(";") || token.is_word("USING")
}

/// Parses `EXECUTE` or `EXEC`, which renders as `EXECUTE`.
pub fn parse_execute<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<TeradataExecute<'a>> {
    p.advance();
    let immediate = p.eat_keyword("IMMEDIATE");
    let name = if immediate {
        None
    } else {
        Some(grammar::parse_object_name(p)?)
    };
    let parenthesized = name.is_some() && p.eat_op("(");
    let parameters = if parenthesized {
        let parameters = if p.at_op(")") {
            Vec::new()
        } else {
            p.parse_comma_separated(Parser::parse_expr)?
        };
        p.expect_op(")")?;
        parameters
    } else if at_execute_end(p) {
        Vec::new()
    } else {
        p.parse_comma_separated(Parser::parse_expr)?
    };
    let into = if p.eat_keyword("INTO") {
        p.parse_comma_separated(Parser::parse_ident)?
    } else {
        Vec::new()
    };
    let using = if p.eat_keyword("USING") {
        p.parse_comma_separated(parse_using)?
    } else {
        Vec::new()
    };
    Ok(TeradataExecute {
        immediate,
        name,
        parameters,
        parenthesized,
        into,
        using,
    })
}

pub fn parse_statement<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Statement<'a, Builtin>> {
    if p.at_keyword("COMMENT") {
        return grammar::parse_comment_on(p).map(|c| StatementKind::Comment(Box::new(c)).into());
    }
    if p.at_keyword("EXECUTE") || p.at_keyword("EXEC") {
        let execute = parse_execute(p)?;
        return Ok(teradata(TeradataStatement::Execute(Box::new(execute))));
    }
    grammar::parse_statement(p)
}

/// Parses `TOP` after `SELECT`, which comes before any `DISTINCT` or `ALL`.
pub fn parse_select_modifiers<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<BuiltinSelect<'a>> {
    if !p.eat_keyword("TOP") {
        return Ok(BuiltinSelect::default());
    }
    let quantity = if p.eat_op("(") {
        let quantity = p.parse_expr()?;
        p.expect_op(")")?;
        TeradataTopQuantity::Expr(quantity)
    } else {
        let token = p.peek();
        if token.kind != TokenKind::Number || !token.text.bytes().all(|b| b.is_ascii_digit()) {
            return Err(p.expected("an integer after TOP"));
        }
        p.advance();
        TeradataTopQuantity::Number(token.text.into())
    };
    let percent = p.eat_keyword("PERCENT");
    let with_ties = p.at_keyword("WITH") && p.peek_nth(1).is_word("TIES");
    if with_ties {
        p.advance();
        p.advance();
    }
    Ok(match quantity {
        TeradataTopQuantity::Number(number) if !percent && !with_ties => BuiltinSelect {
            top: Some(number),
            ..BuiltinSelect::default()
        },
        quantity => BuiltinSelect {
            teradata_top: Some(Box::new(TeradataTop {
                quantity,
                percent,
                with_ties,
            })),
            ..BuiltinSelect::default()
        },
    })
}

/// Reads `TRUE` and `FALSE` as names, and joins adjacent strings.
pub fn parse_prefix<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Expr<'a, Builtin>> {
    if p.at_keyword("TRUE") || p.at_keyword("FALSE") {
        return grammar::parse_name_expr(p);
    }
    if p.peek().kind == TokenKind::String && p.peek_nth(1).kind == TokenKind::String {
        let mut parts = Vec::with_capacity(2);
        while p.peek().kind == TokenKind::String {
            parts.push(D::string_literal(p.advance()));
        }
        return Ok(
            ExprKind::Ext(BuiltinExpr::Teradata(TeradataExpr::AdjacentStrings(parts))).into(),
        );
    }
    grammar::parse_prefix(p)
}

impl Dialect for Teradata {
    type Ext = Builtin;

    fn parse_create_extension<'a>(
        p: &mut Parser<'a, Self>,
        flags: CreateFlags,
    ) -> PResult<Option<Statement<'a, Builtin>>> {
        grammar::parse_create_role(p, flags, false)
    }

    fn parse_qualify<'a>(p: &mut Parser<'a, Self>) -> PResult<Option<Expr<'a, Builtin>>> {
        snowflake::parse_qualify(p)
    }

    fn parse_like_quantifier<'a>(p: &mut Parser<'a, Self>) -> PResult<Option<Quantifier>> {
        Ok(grammar::parse_like_any(p))
    }

    fn parse_update_from_before_set<'a>(
        p: &mut Parser<'a, Self>,
    ) -> PResult<Vec<TableWithJoins<'a, Builtin>>> {
        grammar::parse_optional_update_from(p)
    }

    fn parse_select_into<'a>(
        p: &mut Parser<'a, Self>,
    ) -> PResult<Option<Box<SelectInto<'a, Builtin>>>> {
        grammar::parse_select_into_variables(p)
    }

    fn parse_drop_object_type<'a>(p: &mut Parser<'a, Self>) -> Option<ObjectType<'a>> {
        grammar::eat_drop_object_type(p, &["TYPE"])
    }

    fn parse_group_by_item<'a>(p: &mut Parser<'a, Self>) -> PResult<Expr<'a, Builtin>> {
        grammar::parse_grouping_item_or_empty_set(p)
    }

    fn set_operator_rule<'a>(p: &Parser<'a, Self>) -> Option<SetOperatorRule> {
        grammar::set_operator_rule_with_minus(p)
    }

    fn parse_insert_into<'a>(p: &mut Parser<'a, Self>) -> PResult<bool> {
        Ok(p.eat_keyword("INTO"))
    }

    #[inline]
    fn is_reserved_for_alias(word: &str) -> bool {
        word.eq_ignore_ascii_case("MINUS")
            || word.eq_ignore_ascii_case("QUALIFY")
            || grammar::is_reserved_for_alias(word)
    }

    fn parse_table_factor<'a>(p: &mut Parser<'a, Self>) -> PResult<TableFactor<'a, Builtin>> {
        if grammar::at_xml_table(p) {
            return grammar::parse_xml_table(p, false);
        }
        grammar::parse_table_factor(p)
    }

    #[inline]
    fn lex_token(rest: &str) -> Option<LexResult> {
        lex_token(rest)
    }

    #[inline]
    fn is_reserved(word: &str) -> bool {
        !word.eq_ignore_ascii_case("TRUE")
            && !word.eq_ignore_ascii_case("FALSE")
            && grammar::is_reserved(word)
    }

    fn parse_statement<'a>(p: &mut Parser<'a, Self>) -> PResult<Statement<'a, Builtin>> {
        parse_statement(p)
    }

    fn parse_create<'a>(p: &mut Parser<'a, Self>) -> PResult<Statement<'a, Builtin>> {
        parse_create(p)
    }

    fn parse_select_modifiers<'a>(p: &mut Parser<'a, Self>) -> PResult<BuiltinSelect<'a>> {
        parse_select_modifiers(p)
    }

    fn parse_prefix<'a>(p: &mut Parser<'a, Self>) -> PResult<Expr<'a, Builtin>> {
        parse_prefix(p)
    }

    fn parse_function_arg_null_treatment<'a>(
        p: &mut Parser<'a, Self>,
    ) -> PResult<Option<NullTreatment>> {
        Ok(grammar::parse_null_treatment(p))
    }
}

impl fmt::Display for TeradataStatement<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CreateTable(create) => create.fmt(f),
            Self::Execute(execute) => execute.fmt(f),
        }
    }
}

impl fmt::Display for TeradataExpr<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AdjacentStrings(parts) => {
                f.write_str("'")?;
                for part in parts {
                    f.write_str(&part.text)?;
                }
                f.write_str("'")
            }
        }
    }
}

impl fmt::Display for TeradataOnCommit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::DeleteRows => "ON COMMIT DELETE ROWS",
            Self::PreserveRows => "ON COMMIT PRESERVE ROWS",
        })
    }
}

impl fmt::Display for TeradataWithData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(if self.data {
            "WITH DATA"
        } else {
            "WITH NO DATA"
        })?;
        match self.statistics {
            Some(true) => f.write_str(" AND STATISTICS"),
            Some(false) => f.write_str(" AND NO STATISTICS"),
            None => Ok(()),
        }
    }
}

impl fmt::Display for TeradataCreateTable<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let table = &self.table;
        f.write_str("CREATE ")?;
        if table.or_replace {
            f.write_str("OR REPLACE ")?;
        }
        match self.set_kind {
            Some(TeradataSetKind::Set) => f.write_str("SET ")?,
            Some(TeradataSetKind::Multiset) => f.write_str("MULTISET ")?,
            None => {}
        }
        if self.global {
            f.write_str("GLOBAL ")?;
        }
        if table.temporary {
            f.write_str("TEMPORARY ")?;
        }
        if self.volatile {
            f.write_str("VOLATILE ")?;
        }
        f.write_str("TABLE ")?;
        write_if_not_exists(f, table.if_not_exists)?;
        table.name.fmt(f)?;
        match self.fallback {
            Some(true) => f.write_str(", FALLBACK")?,
            Some(false) => f.write_str(", NO FALLBACK")?,
            None => {}
        }
        if !table.columns.is_empty() || !table.constraints.is_empty() {
            write_columns(f, &table.columns, &table.constraints)?;
        }
        if let (Some(on_commit), false) = (self.on_commit, self.on_commit_last) {
            write!(f, " {on_commit}")?;
        }
        if let Some(query) = &table.query {
            write!(f, " AS {query}")?;
        }
        if let Some(with_data) = self.with_data {
            write!(f, " {with_data}")?;
        }
        if let (Some(on_commit), true) = (self.on_commit, self.on_commit_last) {
            write!(f, " {on_commit}")?;
        }
        Ok(())
    }
}

impl fmt::Display for TeradataUsing<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.expr.fmt(f)?;
        if let Some(alias) = &self.alias {
            write!(f, " AS {alias}")?;
        }
        Ok(())
    }
}

impl fmt::Display for TeradataExecute<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("EXECUTE")?;
        if self.immediate {
            f.write_str(" IMMEDIATE")?;
        }
        if let Some(name) = &self.name {
            write!(f, " {name}")?;
        }
        if self.parenthesized {
            write!(f, "({})", comma(&self.parameters))?;
        } else if !self.parameters.is_empty() {
            write!(f, " {}", comma(&self.parameters))?;
        }
        if !self.into.is_empty() {
            write!(f, " INTO {}", comma(&self.into))?;
        }
        if !self.using.is_empty() {
            write!(f, " USING {}", comma(&self.using))?;
        }
        Ok(())
    }
}

impl fmt::Display for TeradataTop<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.quantity {
            TeradataTopQuantity::Number(number) => write!(f, "TOP {number}")?,
            TeradataTopQuantity::Expr(expr) => write!(f, "TOP ({expr})")?,
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

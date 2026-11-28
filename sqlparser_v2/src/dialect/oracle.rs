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

//! Oracle Database.

use std::borrow::Cow;
use std::fmt;

use crate::PResult;
use crate::ast::{
    BinaryOp, ColumnOption, ConnectBy, CreateFlags, CreateSchema, CreateTable, Delete, Expr,
    ExprKind, Ident, Insert, InsertSource, IsTest, ListAggOnOverflow, Merge, NullTreatment,
    ObjectName, ObjectType, Query, SelectInto, SelectItem, SpecialExpr, Statement, StatementKind,
    StringLiteral, TableFactor, TableRef, TableWithJoins, Update,
};
use crate::dialect::builtin::{BuiltinExpr, BuiltinSelect, BuiltinStatement};
use crate::dialect::{Builtin, Dialect, hive, postgres, snowflake};
use crate::display::{
    comma, write_delete_body, write_list_clause, write_merge_body, write_update_body,
};
use crate::grammar::{self, prec};
use crate::lexer::{self, LexResult, Lexeme, Token, TokenKind};
use crate::parser::{InfixRule, Parser, SetOperatorRule};

#[derive(Debug, Clone, PartialEq)]
pub struct Oracle;

/// Expressions only Oracle Database has.
#[derive(Debug, Clone, PartialEq)]
pub enum OracleExpr<'a> {
    QuoteDelimited(Box<QuoteDelimitedString<'a>>),
}

/// `Q'[text]'` or `NQ'[text]'`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuoteDelimitedString<'a> {
    pub national: bool,
    pub start: char,
    pub value: Cow<'a, str>,
    pub end: char,
}

/// A comment read as instructions to the optimizer: `/*+ text */` or
/// `--+ text`, with any letters and digits before the `+` in `prefix`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OptimizerHint<'a> {
    pub prefix: Cow<'a, str>,
    pub text: Cow<'a, str>,
    pub style: HintStyle,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HintStyle {
    SingleLine,
    MultiLine,
}

/// Statements only Oracle Database has.
#[derive(Debug, Clone, PartialEq)]
pub enum OracleStatement<'a> {
    Insert(Box<OracleInsert<'a>>),
    /// `UPDATE` with optimizer hints.
    Update {
        hints: Vec<OptimizerHint<'a>>,
        update: Box<Update<'a, Builtin>>,
    },
    /// `DELETE` with optimizer hints or without `FROM`.
    Delete {
        hints: Vec<OptimizerHint<'a>>,
        from_keyword: bool,
        delete: Box<Delete<'a, Builtin>>,
    },
    /// `MERGE` with optimizer hints.
    Merge {
        hints: Vec<OptimizerHint<'a>>,
        merge: Box<Merge<'a, Builtin>>,
    },
    /// `EXECUTE IMMEDIATE sql [INTO a, ...] [USING expr [AS name], ...]`
    ExecuteImmediate {
        sql: Expr<'a, Builtin>,
        into: Vec<Ident<'a>>,
        using: Vec<(Expr<'a, Builtin>, Option<Ident<'a>>)>,
    },
    /// `SET name [, name ...] value`, with no `=` or `TO`.
    SetParameters {
        names: Vec<Cow<'a, str>>,
        value: Expr<'a, Builtin>,
    },
}

/// `INSERT` with syntax the core node cannot hold.
#[derive(Debug, Clone, PartialEq)]
pub struct OracleInsert<'a> {
    pub hints: Vec<OptimizerHint<'a>>,
    pub target: InsertTarget<'a>,
    pub alias: Option<InsertAlias<'a>>,
    pub columns: Vec<ObjectName<'a>>,
    pub source: InsertSource<'a, Builtin>,
    pub returning: Vec<SelectItem<'a, Builtin>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum InsertTarget<'a> {
    Table(ObjectName<'a>),
    /// `INSERT INTO (SELECT ...) ...`
    Query(Box<Query<'a, Builtin>>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct InsertAlias<'a> {
    pub name: Ident<'a>,
    pub explicit: bool,
}

#[inline]
fn is_name_char(c: char) -> bool {
    c.is_alphanumeric() || matches!(c, '_' | '$' | '#' | '@')
}

/// Lexes a name, which must start with a letter.
#[inline]
fn lex_name(rest: &str) -> Option<LexResult> {
    let first = rest.chars().next()?;
    if !first.is_alphabetic() {
        return (first == '_' || !first.is_ascii()).then_some(Err("unexpected character"));
    }
    let len = rest
        .char_indices()
        .find(|&(_, c)| !is_name_char(c))
        .map_or(rest.len(), |(i, _)| i);
    Some(Ok(Lexeme {
        kind: TokenKind::Word,
        len,
    }))
}

/// The length of the prefix of a quote-delimited string starting `rest`.
#[inline]
fn quote_delimited_prefix(bytes: &[u8]) -> Option<usize> {
    match bytes {
        [b'q' | b'Q', b'\'', ..] => Some(1),
        [b'n' | b'N', b'q' | b'Q', b'\'', ..] => Some(2),
        _ => None,
    }
}

fn closing_delimiter(start: char) -> char {
    match start {
        '[' => ']',
        '{' => '}',
        '<' => '>',
        '(' => ')',
        other => other,
    }
}

/// Lexes `Q'<delimiter>text<delimiter>'`, starting at its opening quote.
fn lex_quote_delimited(rest: &str, prefix: usize) -> LexResult {
    let after_quote = &rest[prefix + 1..];
    let start = match after_quote.chars().next() {
        None | Some(' ' | '\t' | '\r' | '\n') => {
            return Err("invalid space, tab, newline or end of input after Q'");
        }
        Some(c) => c,
    };
    let end = closing_delimiter(start);
    let body = &after_quote[start.len_utf8()..];
    let close = body
        .char_indices()
        .find(|&(i, c)| c == end && body[i + c.len_utf8()..].starts_with('\''))
        .ok_or("unterminated quoted text")?;
    Ok(Lexeme {
        kind: TokenKind::DialectLiteral,
        len: prefix + 1 + start.len_utf8() + close.0 + end.len_utf8() + 1,
    })
}

#[inline]
pub fn lex_token(rest: &str) -> Option<LexResult> {
    let bytes = rest.as_bytes();
    if let Some(prefix) = quote_delimited_prefix(bytes) {
        return Some(lex_quote_delimited(rest, prefix));
    }
    if matches!(bytes, [b'n' | b'N', b'\'', ..]) {
        return Some(
            lexer::quoted(&rest[1..], TokenKind::String).map(|string| Lexeme {
                kind: TokenKind::String,
                len: string.len + 1,
            }),
        );
    }
    match bytes[0] {
        // `x'AB'` is a hex string, which the core lexes.
        b'x' | b'X' if bytes.get(1) == Some(&b'\'') => None,
        b'{' | b'}' => Some(Ok(Lexeme {
            kind: TokenKind::Op,
            len: 1,
        })),
        // A lone `$` is the end anchor of a `MATCH_RECOGNIZE` pattern.
        b'$' if !bytes
            .get(1)
            .is_some_and(|b| b.is_ascii_alphanumeric() || *b == b'_') =>
        {
            Some(Ok(Lexeme {
                kind: TokenKind::Op,
                len: 1,
            }))
        }
        _ => lex_name(rest),
    }
}

/// `--` starts a line comment, and block comments do not nest.
#[inline]
pub fn lex_comment(rest: &str) -> Option<Result<usize, &'static str>> {
    match rest.as_bytes() {
        [b'-', b'-', ..] => Some(Ok(lexer::line_comment(rest))),
        [b'/', b'*', ..] => Some(lexer::block_comment(rest, false)),
        _ => None,
    }
}

/// Builds the node for a token from [`lex_quote_delimited`].
fn quote_delimited_string(text: &str) -> QuoteDelimitedString<'_> {
    let (prefix, national) = match quote_delimited_prefix(text.as_bytes()) {
        Some(2) => (2, true),
        _ => (1, false),
    };
    let after_quote = &text[prefix + 1..];
    let start = after_quote.chars().next().unwrap_or_default();
    let end = closing_delimiter(start);
    let value = &after_quote[start.len_utf8()..after_quote.len() - end.len_utf8() - 1];
    QuoteDelimitedString {
        national,
        start,
        value: value.into(),
        end,
    }
}

/// Splits a comment's text at a `+` that only letters and digits come before.
fn hint_parts(comment: &str) -> Option<(&str, &str)> {
    let (prefix, text) = comment.split_once('+')?;
    prefix
        .bytes()
        .all(|b| b.is_ascii_alphanumeric())
        .then_some((prefix, text))
}

/// Finds the optimizer hints in text holding only whitespace and comments.
fn parse_hints(mut gap: &str) -> Vec<OptimizerHint<'_>> {
    let mut hints = Vec::new();
    loop {
        gap = gap.trim_ascii_start();
        let (comment, style, len) = if gap.starts_with("--") {
            let len = lexer::line_comment(gap);
            let comment = gap[2..len].strip_suffix('\n').unwrap_or(&gap[2..len]);
            (comment, HintStyle::SingleLine, len)
        } else if gap.starts_with("/*") {
            let Ok(len) = lexer::block_comment(gap, false) else {
                return hints;
            };
            (&gap[2..len - 2], HintStyle::MultiLine, len)
        } else {
            return hints;
        };
        if let Some((prefix, text)) = hint_parts(comment) {
            hints.push(OptimizerHint {
                prefix: prefix.into(),
                text: text.into(),
                style,
            });
        }
        gap = &gap[len..];
    }
}

/// True at the `(SELECT` or `(WITH` of an `INSERT` into a query.
fn at_insert_query<D: Dialect>(p: &Parser<'_, D>) -> bool {
    let next = p.peek_nth(1);
    p.at_op("(") && (next.is_word("SELECT") || next.is_word("WITH"))
}

fn parse_insert_alias<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Option<InsertAlias<'a>>> {
    let explicit = p.eat_keyword("AS");
    if !explicit && (!p.at_implicit_alias() || p.at_keyword("VALUES") || p.at_keyword("DEFAULT")) {
        return Ok(None);
    }
    Ok(Some(InsertAlias {
        name: p.parse_ident()?,
        explicit,
    }))
}

fn parse_insert_target<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<InsertTarget<'a>> {
    if !at_insert_query(p) {
        return grammar::parse_object_name(p).map(InsertTarget::Table);
    }
    p.advance();
    let query = p.nested(D::parse_query)?;
    p.expect_op(")")?;
    Ok(InsertTarget::Query(Box::new(query)))
}

/// Parses `INSERT`, keeping the core node when nothing needs more than it holds.
pub fn parse_insert<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Statement<'a, Builtin>> {
    p.expect_keyword("INSERT")?;
    let hints = parse_hints(p.gap_before_nth(0));
    p.expect_keyword("INTO")?;
    let target = parse_insert_target(p)?;
    let alias = parse_insert_alias(p)?;
    let columns = if grammar::at_column_list(p) {
        p.advance();
        let columns = p.parse_comma_separated(grammar::parse_object_name)?;
        p.expect_op(")")?;
        columns
    } else {
        Vec::new()
    };
    let source = grammar::parse_insert_source(p)?;
    let returning = D::parse_returning(p)?;
    let plain = hints.is_empty() && alias.is_none() && columns.iter().all(|c| c.parts.len() == 1);
    let target = match target {
        InsertTarget::Table(table) if plain => {
            return Ok(StatementKind::Insert(Box::new(Insert {
                into: true,
                output: None,
                modifiers: Default::default(),
                table,
                columns: columns.into_iter().map(|mut c| c.parts.remove(0)).collect(),
                source,
                returning,
            }))
            .into());
        }
        other => other,
    };
    Ok(oracle(OracleStatement::Insert(Box::new(OracleInsert {
        hints,
        target,
        alias,
        columns,
        source,
        returning,
    }))))
}

fn parse_delete<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Statement<'a, Builtin>> {
    p.expect_keyword("DELETE")?;
    let hints = parse_hints(p.gap_before_nth(0));
    let from_keyword = p.eat_keyword("FROM");
    let table = p.parse_table_ref()?;
    let selection = if p.eat_keyword("WHERE") {
        Some(p.parse_expr()?)
    } else {
        None
    };
    let returning = D::parse_returning(p)?;
    let delete = Box::new(Delete {
        output: None,
        multi: None,
        using: Vec::new(),
        table,
        selection,
        order_by: Vec::new(),
        limit: None,
        returning,
    });
    if from_keyword && hints.is_empty() {
        return Ok(StatementKind::Delete(delete).into());
    }
    Ok(oracle(OracleStatement::Delete {
        hints,
        from_keyword,
        delete,
    }))
}

pub fn parse_execute_immediate<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<OracleStatement<'a>> {
    p.expect_keyword("EXECUTE")?;
    p.expect_keyword("IMMEDIATE")?;
    let sql = p.parse_expr()?;
    let into = if p.eat_keyword("INTO") {
        p.parse_comma_separated(Parser::parse_ident)?
    } else {
        Vec::new()
    };
    let using = if p.eat_keyword("USING") {
        p.parse_comma_separated(|p| {
            let value = p.parse_expr()?;
            let alias = if p.eat_keyword("AS") {
                Some(p.parse_ident()?)
            } else {
                None
            };
            Ok((value, alias))
        })?
    } else {
        Vec::new()
    };
    Ok(OracleStatement::ExecuteImmediate { sql, into, using })
}

/// True at `SET name [, name ...] value`, where no `=` or `TO` comes after the
/// names.
fn at_set_parameters<D: Dialect>(p: &Parser<'_, D>) -> bool {
    if !p.at_keyword("SET") || grammar::at_special_set(p) {
        return false;
    }
    let first = p.peek_nth(1);
    if ["LOCAL", "SESSION", "GLOBAL"]
        .iter()
        .any(|scope| first.is_word(scope))
    {
        return false;
    }
    let mut n = 1;
    loop {
        if p.peek_nth(n).kind != TokenKind::Word {
            return false;
        }
        n += 1;
        if !p.peek_nth(n).is_op(",") {
            break;
        }
        n += 1;
    }
    let next = p.peek_nth(n);
    next.kind != TokenKind::Eof
        && !next.is_op("=")
        && !next.is_op(".")
        && !next.is_op(";")
        && !next.is_word("TO")
}

fn parse_set_parameters<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<OracleStatement<'a>> {
    p.expect_keyword("SET")?;
    let names = p.parse_comma_separated(|p| Ok(p.advance().text))?;
    let token = p.peek();
    // A reserved word such as `ON` is the value's whole text.
    let value = if token.kind == TokenKind::Word && D::is_reserved(token.text) {
        p.advance();
        Expr::new(
            ExprKind::Ident(Ident {
                text: token.text.into(),
                quote: None,
                span: token.span,
            }),
            token.span,
        )
    } else {
        p.parse_expr()?
    };
    Ok(OracleStatement::SetParameters {
        names: names.into_iter().map(Cow::Borrowed).collect(),
        value,
    })
}

fn oracle<'a>(statement: OracleStatement<'a>) -> Statement<'a, Builtin> {
    StatementKind::Ext(BuiltinStatement::Oracle(statement)).into()
}

pub fn parse_statement<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Statement<'a, Builtin>> {
    if p.at_keyword("ALTER") && p.peek_nth(1).is_word("INDEX") {
        return grammar::parse_alter_index_rename(p);
    }
    if p.at_keyword("DELETE") {
        return parse_delete(p);
    }
    if p.at_keyword("UPDATE") {
        let hints = parse_hints(p.gap_before_nth(1));
        if !hints.is_empty() {
            let update = Box::new(D::parse_update(p)?);
            return Ok(oracle(OracleStatement::Update { hints, update }));
        }
    }
    if p.at_keyword("MERGE") {
        let hints = parse_hints(p.gap_before_nth(1));
        if !hints.is_empty() {
            let merge = Box::new(D::parse_merge(p)?);
            return Ok(oracle(OracleStatement::Merge { hints, merge }));
        }
    }
    if p.at_keyword("COMMENT") {
        return grammar::parse_comment_on(p).map(|c| StatementKind::Comment(Box::new(c)).into());
    }
    if p.at_keyword("EXECUTE") && p.peek_nth(1).is_word("IMMEDIATE") {
        return parse_execute_immediate(p).map(oracle);
    }
    if at_set_parameters(p) {
        return parse_set_parameters(p).map(oracle);
    }
    grammar::parse_statement(p)
}

/// Parses what follows `CREATE ... TABLE`, where `AS` before `SELECT` is optional.
pub fn parse_create_table<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
    flags: CreateFlags,
) -> PResult<CreateTable<'a, D::Ext>> {
    let if_not_exists = grammar::parse_if_not_exists(p)?;
    let name = grammar::parse_object_name(p)?;
    let (columns, constraints) = if p.at_op("(") {
        grammar::parse_columns(p)?
    } else {
        (Vec::new(), Vec::new())
    };
    let query = if p.eat_keyword("AS") || p.at_keyword("SELECT") {
        Some(Box::new(D::parse_query(p)?))
    } else {
        None
    };
    if columns.is_empty() && query.is_none() {
        return Err(p.expected("column definitions, AS or SELECT"));
    }
    Ok(CreateTable {
        or_replace: flags.or_replace,
        temporary: flags.temporary,
        name,
        if_not_exists,
        columns,
        constraints,
        query,
    })
}

pub fn parse_prefix<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Expr<'a, Builtin>> {
    let token = p.peek();
    if token.kind == TokenKind::DialectLiteral {
        p.advance();
        let string = quote_delimited_string(token.text);
        return Ok(
            ExprKind::Ext(BuiltinExpr::Oracle(OracleExpr::QuoteDelimited(Box::new(
                string,
            ))))
            .into(),
        );
    }
    if token.kind != TokenKind::Word {
        return grammar::parse_prefix(p);
    }
    if p.has_context(grammar::CONTEXT_CONNECT_BY) && p.eat_keyword("PRIOR") {
        let expr = p.parse_expr_bp(prec::ADD)?;
        return Ok(ExprKind::Special(Box::new(SpecialExpr::Prior(expr))).into());
    }
    if p.eat_keyword("CONNECT_BY_ROOT") {
        let expr = p.parse_expr_bp(prec::UNARY)?;
        return Ok(ExprKind::Special(Box::new(SpecialExpr::ConnectByRoot(expr))).into());
    }
    // Oracle has no boolean literals here, so these are ordinary names.
    if p.at_keyword("TRUE") || p.at_keyword("FALSE") {
        return grammar::parse_name_expr(p);
    }
    grammar::parse_prefix(p)
}

pub fn infix_rule<'a, D: Dialect<Ext = Builtin>>(p: &Parser<'a, D>) -> Option<InfixRule<'a, D>> {
    if snowflake::at_outer_join_mark(p) {
        return Some(InfixRule::custom(
            prec::POSTFIX,
            snowflake::parse_outer_join_mark,
        ));
    }
    if p.at_op("||") {
        return Some(InfixRule::binary(prec::ADD, BinaryOp::StringConcat));
    }
    grammar::infix_rule(p)
}

pub fn parse_select_modifiers<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<BuiltinSelect<'a>> {
    Ok(BuiltinSelect {
        hints: parse_hints(p.gap_before_nth(0)),
        ..Default::default()
    })
}

impl Dialect for Oracle {
    /// Recursive CTEs need no keyword here, and `RECURSIVE` is rejected.
    fn eat_with_recursive<'a>(_p: &mut Parser<'a, Self>) -> bool {
        false
    }

    type Ext = Builtin;

    fn parse_create_extension<'a>(
        p: &mut Parser<'a, Self>,
        flags: CreateFlags,
    ) -> PResult<Option<Statement<'a, Builtin>>> {
        grammar::parse_create_role(p, flags, false)
    }

    fn parse_column_option<'a>(
        p: &mut Parser<'a, Self>,
    ) -> PResult<Option<ColumnOption<'a, Builtin>>> {
        if p.eat_keyword("INVISIBLE") {
            return Ok(Some(ColumnOption::Keyword {
                keyword: "INVISIBLE".into(),
                expr: None,
            }));
        }
        grammar::parse_column_option(p)
    }

    fn parse_create_schema<'a>(
        p: &mut Parser<'a, Self>,
        flags: CreateFlags,
    ) -> PResult<CreateSchema<'a>> {
        if flags != CreateFlags::default() {
            return Err(p.expected("TABLE or VIEW"));
        }
        grammar::parse_create_schema_authorization(p, false)
    }

    fn parse_update_from_after_set<'a>(
        p: &mut Parser<'a, Self>,
    ) -> PResult<Vec<TableWithJoins<'a, Builtin>>> {
        grammar::parse_optional_update_from(p)
    }

    fn parse_select_into<'a>(
        p: &mut Parser<'a, Self>,
    ) -> PResult<Option<Box<SelectInto<'a, Builtin>>>> {
        grammar::parse_select_into_variables(p)
    }

    fn parse_is_test<'a>(p: &mut Parser<'a, Self>) -> PResult<Option<IsTest<'a, Builtin>>> {
        grammar::parse_is_json(p)
    }

    fn parse_function_on_overflow<'a>(
        p: &mut Parser<'a, Self>,
    ) -> PResult<Option<Box<ListAggOnOverflow<'a, Builtin>>>> {
        grammar::parse_on_overflow(p)
    }

    fn parse_drop_object_type<'a>(p: &mut Parser<'a, Self>) -> Option<ObjectType<'a>> {
        grammar::eat_drop_object_type(p, &["TYPE"])
    }

    fn parse_group_by_item<'a>(p: &mut Parser<'a, Self>) -> PResult<Expr<'a, Builtin>> {
        grammar::parse_grouping_item_or_empty_set(p)
    }

    fn string_literal(token: Token<'_>) -> StringLiteral<'_> {
        grammar::national_string_literal(token)
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
    fn is_reserved(word: &str) -> bool {
        if word.eq_ignore_ascii_case("TRUE") || word.eq_ignore_ascii_case("FALSE") {
            return false;
        }
        // A prefix operator rather than a name.
        word.eq_ignore_ascii_case("CONNECT_BY_ROOT") || grammar::is_reserved(word)
    }

    #[inline]
    fn is_reserved_for_alias(word: &str) -> bool {
        [
            "MINUS",
            "START",
            "CONNECT",
            "MATCH_RECOGNIZE",
            "PIVOT",
            "UNPIVOT",
            "SAMPLE",
            "TABLESAMPLE",
            "RETURNING",
        ]
        .iter()
        .any(|reserved| reserved.eq_ignore_ascii_case(word))
            || grammar::is_reserved_for_alias(word)
    }

    fn parse_statement<'a>(p: &mut Parser<'a, Self>) -> PResult<Statement<'a, Builtin>> {
        parse_statement(p)
    }

    fn parse_insert_statement<'a>(p: &mut Parser<'a, Self>) -> PResult<Statement<'a, Builtin>> {
        parse_insert(p)
    }

    fn parse_returning<'a>(p: &mut Parser<'a, Self>) -> PResult<Vec<SelectItem<'a, Builtin>>> {
        postgres::parse_returning(p)
    }

    fn parse_create_table<'a>(
        p: &mut Parser<'a, Self>,
        flags: CreateFlags,
    ) -> PResult<CreateTable<'a, Builtin>> {
        parse_create_table(p, flags)
    }

    fn parse_select_modifiers<'a>(p: &mut Parser<'a, Self>) -> PResult<BuiltinSelect<'a>> {
        parse_select_modifiers(p)
    }

    fn parse_connect_by<'a>(p: &mut Parser<'a, Self>) -> PResult<Vec<ConnectBy<'a, Builtin>>> {
        grammar::parse_connect_by_clauses(p)
    }

    fn parse_table_ref<'a>(p: &mut Parser<'a, Self>) -> PResult<TableRef<'a, Builtin>> {
        snowflake::parse_table_ref(p)
    }

    fn parse_table_factor<'a>(p: &mut Parser<'a, Self>) -> PResult<TableFactor<'a, Builtin>> {
        if grammar::at_xml_table(p) {
            return grammar::parse_xml_table(p, false);
        }
        hive::parse_table_factor(p)
    }

    fn parse_function_arg_null_treatment<'a>(
        p: &mut Parser<'a, Self>,
    ) -> PResult<Option<NullTreatment>> {
        Ok(grammar::parse_null_treatment(p))
    }

    fn parse_function_filter<'a>(_p: &mut Parser<'a, Self>) -> PResult<Option<Expr<'a, Builtin>>> {
        Ok(None)
    }

    fn at_function_call<'a>(p: &Parser<'a, Self>) -> bool {
        p.at_op("(") && !snowflake::at_outer_join_mark(p)
    }

    fn set_operator_rule<'a>(p: &Parser<'a, Self>) -> Option<SetOperatorRule> {
        snowflake::set_operator_rule(p)
    }

    fn parse_prefix<'a>(p: &mut Parser<'a, Self>) -> PResult<Expr<'a, Builtin>> {
        parse_prefix(p)
    }

    fn infix_rule<'a>(p: &Parser<'a, Self>) -> Option<InfixRule<'a, Self>> {
        infix_rule(p)
    }
}

impl fmt::Display for OracleExpr<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::QuoteDelimited(string) => string.fmt(f),
        }
    }
}

impl fmt::Display for QuoteDelimitedString<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let prefix = if self.national { "NQ" } else { "Q" };
        write!(f, "{prefix}'{}{}{}'", self.start, self.value, self.end)
    }
}

impl fmt::Display for OptimizerHint<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.style {
            HintStyle::SingleLine => writeln!(f, "--{}+{}", self.prefix, self.text),
            HintStyle::MultiLine => write!(f, "/*{}+{}*/", self.prefix, self.text),
        }
    }
}

/// Writes hints in the place a statement's modifiers go, each ending in a space.
pub(crate) fn write_hints(f: &mut fmt::Formatter<'_>, hints: &[OptimizerHint<'_>]) -> fmt::Result {
    hints.iter().try_for_each(|hint| write!(f, "{hint} "))
}

impl fmt::Display for InsertTarget<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Table(name) => name.fmt(f),
            Self::Query(query) => write!(f, "({query})"),
        }
    }
}

impl fmt::Display for OracleInsert<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("INSERT ")?;
        write_hints(f, &self.hints)?;
        write!(f, "INTO {}", self.target)?;
        if let Some(alias) = &self.alias {
            let keyword = if alias.explicit { "AS " } else { "" };
            write!(f, " {keyword}{}", alias.name)?;
        }
        if !self.columns.is_empty() {
            write!(f, " ({})", comma(&self.columns))?;
        }
        crate::display::write_insert_source(f, &self.source, !self.columns.is_empty())?;
        write_list_clause(f, "RETURNING", &self.returning)
    }
}

impl fmt::Display for OracleStatement<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Insert(insert) => insert.fmt(f),
            Self::Update { hints, update } => {
                f.write_str("UPDATE ")?;
                write_hints(f, hints)?;
                write_update_body(f, update)
            }
            Self::Delete {
                hints,
                from_keyword,
                delete,
            } => {
                f.write_str("DELETE")?;
                for hint in hints {
                    write!(f, " {hint}")?;
                }
                if *from_keyword {
                    f.write_str(" FROM")?;
                }
                write_delete_body(f, delete)
            }
            Self::Merge { hints, merge } => {
                f.write_str("MERGE ")?;
                write_hints(f, hints)?;
                write_merge_body(f, merge)
            }
            Self::ExecuteImmediate { sql, into, using } => {
                write!(f, "EXECUTE IMMEDIATE {sql}")?;
                if !into.is_empty() {
                    write!(f, " INTO {}", comma(into))?;
                }
                for (i, (value, alias)) in using.iter().enumerate() {
                    f.write_str(if i == 0 { " USING " } else { ", " })?;
                    write!(f, "{value}")?;
                    if let Some(alias) = alias {
                        write!(f, " AS {alias}")?;
                    }
                }
                Ok(())
            }
            Self::SetParameters { names, value } => {
                write!(f, "SET {} {value}", names.join(", "))
            }
        }
    }
}

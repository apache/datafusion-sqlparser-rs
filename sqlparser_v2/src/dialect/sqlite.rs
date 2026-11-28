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

//! SQLite.

use crate::ast::Span;
use std::borrow::Cow;
use std::fmt;

use crate::PResult;
use crate::ast::{
    ArrayType, Assignment, BinaryOp, ColumnDef, ColumnOption, CreateTable, DataType,
    DataTypeSuffix, Delete, Expr, ExprKind, Generated, GeneratedKind, GeneratedStorage,
    GeneratedWhen, Ident, IndexColumn, Insert, InsertSource, IsTest, Join, JoinKind, Limit,
    ObjectName, ObjectType, OrderByItem, SelectItem, SelectItemKind, Statement, StatementKind,
    TableAlias, TableFactor, TableRef, TableWithJoins, UnaryOp, Update, WindowOver,
};
use crate::dialect::builtin::{BuiltinExpr, BuiltinStatement, BuiltinTableFactor};
use crate::dialect::{Builtin, Dialect, mysql, postgres};
use crate::display::{
    Indent, SpaceOrNewline, comma, write_columns, write_create_flags, write_if_not_exists,
    write_indented_clause, write_list_clause, write_where_clause,
};
use crate::grammar::{self, prec};
use crate::lexer::{self, LexResult, Lexeme, Token, TokenKind, quoted};
use crate::parser::{InfixParseFn, InfixRule, Parser};

#[derive(Debug, Clone, PartialEq)]
pub struct Sqlite;

/// Statements only SQLite has.
#[derive(Debug, Clone, PartialEq)]
pub enum SqliteStatement<'a> {
    /// `PRAGMA name`, `PRAGMA name = value` or `PRAGMA name(value)`
    Pragma {
        name: ObjectName<'a>,
        value: Option<(PragmaForm, Expr<'a, Builtin>)>,
    },
    /// `ATTACH [DATABASE] file AS schema`
    Attach {
        database_keyword: bool,
        file: Expr<'a, Builtin>,
        schema: Ident<'a>,
    },
    /// `BEGIN {DEFERRED | IMMEDIATE | EXCLUSIVE} [TRANSACTION]`
    Begin {
        modifier: Cow<'a, str>,
        transaction_keyword: bool,
    },
    CreateTable(Box<SqliteCreateTable<'a>>),
    CreateVirtualTable(Box<SqliteVirtualTable<'a>>),
    CreateTrigger(Box<SqliteTrigger<'a>>),
    Insert(Box<SqliteInsert<'a>>),
    Update(Box<SqliteUpdate<'a>>),
    Delete(Box<SqliteDelete<'a>>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PragmaForm {
    Equals,
    Call,
}

/// `CREATE TABLE` with table options, which come after the column list.
#[derive(Debug, Clone, PartialEq)]
pub struct SqliteCreateTable<'a> {
    pub table: CreateTable<'a, Builtin>,
    /// True if a comma comes before the first option.
    pub leading_comma: bool,
    pub options: Vec<SqliteTableOption>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqliteTableOption {
    WithoutRowid,
    Strict,
}

/// `CREATE VIRTUAL TABLE [IF NOT EXISTS] name USING module [(args)]`
#[derive(Debug, Clone, PartialEq)]
pub struct SqliteVirtualTable<'a> {
    pub if_not_exists: bool,
    pub name: ObjectName<'a>,
    pub module: Ident<'a>,
    pub args: Vec<Ident<'a>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TriggerPeriod {
    Before,
    After,
    InsteadOf,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TriggerEvent<'a> {
    Delete,
    Insert,
    /// `UPDATE [OF columns]`
    Update(Vec<Ident<'a>>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct SqliteTrigger<'a> {
    pub temporary: bool,
    pub if_not_exists: bool,
    pub name: ObjectName<'a>,
    pub period: Option<TriggerPeriod>,
    pub event: TriggerEvent<'a>,
    pub table: ObjectName<'a>,
    pub for_each_row: bool,
    pub when: Option<Expr<'a, Builtin>>,
    /// The statements between `BEGIN` and `END`, each followed by `;`.
    pub statements: Vec<Statement<'a, Builtin>>,
}

/// An assignment in `UPDATE` or an upsert.
#[derive(Debug, Clone, PartialEq)]
pub struct SqliteAssignment<'a> {
    pub target: SqliteAssignmentTarget<'a>,
    pub value: Expr<'a, Builtin>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SqliteAssignmentTarget<'a> {
    Column(ObjectName<'a>),
    /// `(a, b) = ...`
    Tuple(Vec<ObjectName<'a>>),
}

/// `INSERT` with syntax the core has no place for.
#[derive(Debug, Clone, PartialEq)]
pub struct SqliteInsert<'a> {
    /// The word after `OR`, also set for `REPLACE`, which means the same.
    pub or_action: Option<Cow<'a, str>>,
    pub table: ObjectName<'a>,
    pub columns: Vec<Ident<'a>>,
    /// `None` for `DEFAULT VALUES`.
    pub source: Option<InsertSource<'a, Builtin>>,
    pub on_conflict: Option<OnConflict<'a>>,
    pub returning: Vec<SelectItem<'a, Builtin>>,
}

/// `ON CONFLICT [(columns) [WHERE condition]] DO ...`
#[derive(Debug, Clone, PartialEq)]
pub struct OnConflict<'a> {
    pub columns: Vec<Ident<'a>>,
    pub predicate: Option<Expr<'a, Builtin>>,
    pub action: OnConflictAction<'a>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum OnConflictAction<'a> {
    DoNothing,
    DoUpdate {
        assignments: Vec<SqliteAssignment<'a>>,
        selection: Option<Expr<'a, Builtin>>,
    },
}

/// `UPDATE` with syntax the core has no place for.
#[derive(Debug, Clone, PartialEq)]
pub struct SqliteUpdate<'a> {
    pub or_action: Option<Cow<'a, str>>,
    pub table: TableRef<'a, Builtin>,
    pub assignments: Vec<SqliteAssignment<'a>>,
    pub from: Vec<TableWithJoins<'a, Builtin>>,
    pub selection: Option<Expr<'a, Builtin>>,
    pub returning: Vec<SelectItem<'a, Builtin>>,
    pub limit: Option<Expr<'a, Builtin>>,
}

/// `DELETE` with syntax the core has no place for.
#[derive(Debug, Clone, PartialEq)]
pub struct SqliteDelete<'a> {
    pub table: TableRef<'a, Builtin>,
    pub selection: Option<Expr<'a, Builtin>>,
    pub returning: Vec<SelectItem<'a, Builtin>>,
    pub order_by: Vec<OrderByItem<'a, Builtin>>,
    pub limit: Option<Expr<'a, Builtin>>,
}

/// Expressions only SQLite has.
#[derive(Debug, Clone, PartialEq)]
pub enum SqliteExpr<'a> {
    /// `expr [NOT] IN table`, where `target` is a name or a table function.
    InTable {
        expr: Box<Expr<'a, Builtin>>,
        target: Box<Expr<'a, Builtin>>,
        negated: bool,
    },
}

/// A table with `INDEXED BY` or `NOT INDEXED` after it. The alias is on `table`.
#[derive(Debug, Clone, PartialEq)]
pub struct SqliteIndexedTable<'a> {
    pub table: TableRef<'a, Builtin>,
    /// The index name, or `None` for `NOT INDEXED`.
    pub index: Option<Ident<'a>>,
}

/// SQLite's binding powers. `=`, `IN` and the pattern operators share one
/// level, as in the core; the others are placed around it.
const EQUALITY: u8 = prec::COMPARE;
/// `<`, `<=`, `>` and `>=` bind tighter than the equality level.
const RELATIONAL: u8 = prec::COMPARE + 1;
/// `&`, `|`, `<<` and `>>` share one level.
const BITWISE: u8 = prec::BIT_AND;
/// `||`, `->` and `->>` bind tighter than `*`.
const CONCAT: u8 = prec::MUL + 2;

/// The words that cannot name something. Everything else can fall back to an
/// identifier. See <https://www.sqlite.org/lang_keywords.html>.
const MAX_RESERVED_LEN: usize = 13;

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
        b"ADD"
            | b"ALL"
            | b"ALTER"
            | b"AND"
            | b"AS"
            | b"AUTOINCREMENT"
            | b"BETWEEN"
            | b"CASE"
            | b"CHECK"
            | b"COLLATE"
            | b"COMMIT"
            | b"CONSTRAINT"
            | b"CREATE"
            | b"DEFAULT"
            | b"DEFERRABLE"
            | b"DELETE"
            | b"DISTINCT"
            | b"DROP"
            | b"ELSE"
            | b"ESCAPE"
            | b"EXCEPT"
            | b"EXISTS"
            | b"FOREIGN"
            | b"FROM"
            | b"GROUP"
            | b"HAVING"
            | b"IN"
            | b"INDEX"
            | b"INSERT"
            | b"INTERSECT"
            | b"INTO"
            | b"IS"
            | b"JOIN"
            | b"LIMIT"
            | b"NOT"
            | b"NOTHING"
            | b"NOTNULL"
            | b"NULL"
            | b"ON"
            | b"OR"
            | b"ORDER"
            | b"PRIMARY"
            | b"REFERENCES"
            | b"RETURNING"
            | b"SELECT"
            | b"SET"
            | b"TABLE"
            | b"THEN"
            | b"TO"
            | b"TRANSACTION"
            | b"UNION"
            | b"UNIQUE"
            | b"UPDATE"
            | b"USING"
            | b"VALUES"
            | b"WHEN"
            | b"WHERE"
    )
}

/// Words that end a select list, so cannot be a column alias without `AS`.
fn ends_select_item(word: &str) -> bool {
    [
        "SELECT",
        "WHERE",
        "GROUP",
        "HAVING",
        "ORDER",
        "LIMIT",
        "UNION",
        "EXCEPT",
        "INTERSECT",
        "RETURNING",
        "VALUES",
        "FROM",
        "INTO",
    ]
    .iter()
    .any(|reserved| reserved.eq_ignore_ascii_case(word))
}

/// Words that continue a `FROM` clause, so cannot be a table alias without `AS`.
fn ends_table_factor(word: &str) -> bool {
    [
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
        "INDEXED",
        "NOT",
    ]
    .iter()
    .any(|reserved| reserved.eq_ignore_ascii_case(word))
}

#[inline]
fn is_word_start(byte: u8) -> bool {
    byte.is_ascii_alphabetic() || byte == b'_' || byte >= 0x80
}

#[inline]
fn is_word_part(byte: u8) -> bool {
    is_word_start(byte) || byte.is_ascii_digit() || byte == b'$'
}

/// Lexes `[name]`, which has no escape for `]`.
#[inline]
fn bracketed(rest: &str) -> LexResult {
    match rest.as_bytes().iter().position(|&b| b == b']') {
        Some(end) => Ok(Lexeme {
            kind: TokenKind::QuotedIdent,
            len: end + 1,
        }),
        None => Err("unterminated quoted text"),
    }
}

/// Counts digits from `from`, allowing one `_` between two digits.
#[inline]
fn digits(bytes: &[u8], from: usize) -> usize {
    let mut end = from;
    while end < bytes.len()
        && (bytes[end].is_ascii_digit()
            || (bytes[end] == b'_'
                && end > from
                && bytes[end - 1].is_ascii_digit()
                && bytes.get(end + 1).is_some_and(u8::is_ascii_digit)))
    {
        end += 1;
    }
    end - from
}

/// Lexes a number. Digits can be grouped with `_`, and `1.e5` has no fraction.
fn number(bytes: &[u8]) -> LexResult {
    let mut len = digits(bytes, 0);
    if bytes.get(len) == Some(&b'.') {
        len += 1 + digits(bytes, len + 1);
    }
    if matches!(bytes.get(len), Some(b'e' | b'E')) {
        let sign = usize::from(matches!(bytes.get(len + 1), Some(b'+' | b'-')));
        let exponent = digits(bytes, len + 1 + sign);
        if exponent > 0 {
            len += 1 + sign + exponent;
        }
    }
    if bytes.get(len).is_some_and(|&b| is_word_start(b)) {
        return Err("invalid number");
    }
    Ok(Lexeme {
        kind: TokenKind::Number,
        len,
    })
}

/// Lexes `$name`, `:name` and `@name`. Unlike the others, `$` needs no name.
fn placeholder(bytes: &[u8]) -> Option<LexResult> {
    let name = bytes[1..].iter().take_while(|&&b| is_word_part(b)).count();
    (name > 0 || bytes[0] == b'$').then_some(Ok(Lexeme {
        kind: TokenKind::Placeholder,
        len: 1 + name,
    }))
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
        [b'[', ..] => Some(bracketed(rest)),
        [b'=', b'=', ..] | [b':', b':', ..] => op(2),
        [b'-', b'>', b'>', ..] => op(3),
        [b'-', b'>', ..] => op(2),
        [b'~', ..] => op(1),
        [b'0', b'x', ..] => None,
        [b'x' | b'X', b'\'', ..] => None,
        [b'0'..=b'9', ..] | [b'.', b'0'..=b'9', ..] => Some(number(bytes)),
        [b'$' | b':' | b'@', ..] => placeholder(bytes),
        [b, ..] if is_word_start(*b) => Some(Ok(Lexeme {
            kind: TokenKind::Word,
            len: bytes.iter().take_while(|&&b| is_word_part(b)).count(),
        })),
        _ => None,
    }
}

/// `--` comments, and `/* */` comments that do not nest and need not be closed.
#[inline]
pub fn lex_comment(rest: &str) -> Option<Result<usize, &'static str>> {
    match rest.as_bytes() {
        [b'-', b'-', ..] => Some(Ok(lexer::line_comment(rest))),
        [b'/', b'*', ..] => Some(Ok(lexer::block_comment(rest, false).unwrap_or(rest.len()))),
        _ => None,
    }
}

fn string_ident(token: Token<'_>) -> Ident<'_> {
    Ident {
        text: Cow::Borrowed(&token.text[1..token.text.len() - 1]),
        quote: Some('\''),
        span: token.span,
    }
}

/// Parses any word, quoted name or string as a name, for aliases and windows.
fn parse_any_name<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Ident<'a>> {
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
        TokenKind::String => {
            p.advance();
            Ok(string_ident(token))
        }
        _ => Err(p.expected("a name")),
    }
}

/// Parses a dotted name in which any part can be a string.
fn parse_name<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<ObjectName<'a>> {
    let first = if p.peek().kind == TokenKind::String {
        parse_any_name(p)?
    } else {
        p.parse_ident()?
    };
    let mut parts = vec![first];
    while p.eat_op(".") {
        parts.push(p.parse_name_part()?);
    }
    Ok(ObjectName { parts })
}

pub fn parse_name_part<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Ident<'a>> {
    if p.peek().kind == TokenKind::String {
        return parse_any_name(p);
    }
    grammar::parse_name_part(p)
}

/// True at a `'name'.*` reference, whose first part is a string.
fn at_string_wildcard<'a, D: Dialect>(p: &Parser<'a, D>) -> bool {
    if p.peek().kind != TokenKind::String {
        return false;
    }
    let mut n = 1;
    loop {
        if !p.peek_nth(n).is_op(".") {
            return false;
        }
        let next = p.peek_nth(n + 1);
        if next.is_op("*") {
            return true;
        }
        if !matches!(
            next.kind,
            TokenKind::Word | TokenKind::QuotedIdent | TokenKind::String
        ) {
            return false;
        }
        n += 2;
    }
}

pub fn parse_select_item<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<SelectItem<'a, D::Ext>> {
    if at_string_wildcard(p) {
        let mut parts = vec![parse_any_name(p)?];
        loop {
            p.expect_op(".")?;
            if p.eat_op("*") {
                return Ok(SelectItemKind::QualifiedWildcard(ObjectName { parts }).into());
            }
            parts.push(p.parse_name_part()?);
        }
    }
    grammar::parse_select_item(p)
}

pub fn parse_select_item_alias<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
    expr: Expr<'a, D::Ext>,
) -> PResult<SelectItem<'a, D::Ext>> {
    let explicit = p.eat_keyword("AS");
    let token = p.peek();
    let implicit = matches!(token.kind, TokenKind::QuotedIdent | TokenKind::String)
        || (token.kind == TokenKind::Word && !ends_select_item(token.text));
    let alias = if explicit || implicit {
        Some(parse_any_name(p)?)
    } else {
        None
    };
    Ok(SelectItemKind::Expr { expr, alias }.into())
}

fn parse_table_alias<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Option<TableAlias<'a>>> {
    let explicit = p.eat_keyword("AS");
    let token = p.peek();
    let implicit = match token.kind {
        TokenKind::QuotedIdent | TokenKind::String => true,
        TokenKind::Word => !ends_table_factor(token.text),
        _ => false,
    };
    if !explicit && !implicit {
        return Ok(None);
    }
    let name = parse_any_name(p)?;
    let columns = if p.at_op("(") {
        grammar::parse_ident_list(p)?
    } else {
        Vec::new()
    };
    Ok(Some(TableAlias {
        name,
        explicit,
        columns,
        column_types: Vec::new(),
    }))
}

/// Parses `INDEXED BY name` or `NOT INDEXED`.
fn parse_indexed_clause<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<Option<Option<Ident<'a>>>> {
    if p.at_keyword("INDEXED") {
        p.advance();
        p.expect_keyword("BY")?;
        return p.parse_ident().map(|index| Some(Some(index)));
    }
    if p.at_keyword("NOT") && p.peek_nth(1).is_word("INDEXED") {
        p.advance();
        p.advance();
        return Ok(Some(None));
    }
    Ok(None)
}

/// Removes parentheses around a single table, which only group a join in the
/// core. An alias outside moves onto the table inside.
fn flatten_parentheses<'a, D: Dialect>(
    p: &Parser<'a, D>,
    table: TableRef<'a, Builtin>,
) -> PResult<TableRef<'a, Builtin>> {
    match table.factor {
        TableFactor::NestedJoin(nested)
            if nested.joins.is_empty()
                && !matches!(nested.relation.factor, TableFactor::NestedJoin(_)) =>
        {
            let mut inner = nested.relation;
            let Some(alias) = table.alias else {
                return Ok(inner);
            };
            if let TableFactor::Ext(BuiltinTableFactor::Sqlite(indexed)) = &mut inner.factor {
                if indexed.table.alias.is_none() {
                    indexed.table.alias = Some(alias);
                    return Ok(inner);
                }
            } else if inner.alias.is_none() {
                inner.alias = Some(alias);
                return Ok(inner);
            }
            Err(p.expected("one alias for a parenthesized table"))
        }
        factor => Ok(TableRef {
            factor,
            alias: table.alias,
            span: Span::default(),
        }),
    }
}

pub fn parse_table_ref<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<TableRef<'a, Builtin>> {
    let factor = D::parse_table_factor(p)?;
    let can_be_indexed = matches!(factor, TableFactor::Named(_) | TableFactor::Function { .. });
    let alias = parse_table_alias(p)?;
    let table = TableRef {
        factor,
        alias,
        span: Span::default(),
    };
    if can_be_indexed && let Some(index) = parse_indexed_clause(p)? {
        let indexed = SqliteIndexedTable { table, index };
        return Ok(TableRef {
            factor: TableFactor::Ext(BuiltinTableFactor::Sqlite(Box::new(indexed))),
            alias: None,
            span: Span::default(),
        });
    }
    flatten_parentheses(p, table)
}

pub fn parse_table_factor<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<TableFactor<'a, D::Ext>> {
    if p.peek().kind == TokenKind::String {
        return parse_name(p).map(TableFactor::Named);
    }
    grammar::parse_table_factor(p)
}

/// Parses a `FROM` list, where a comma can act as a join with a constraint.
pub fn parse_from_list<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<Vec<TableWithJoins<'a, D::Ext>>> {
    let mut tables: Vec<TableWithJoins<'a, D::Ext>> = Vec::new();
    loop {
        let table = D::parse_table_with_joins(p)?;
        let has_constraint = p.at_keyword("ON") || p.at_keyword("USING");
        match tables.last_mut() {
            Some(previous) if table.joins.is_empty() && has_constraint => {
                let constraint = grammar::parse_join_constraint(p)?;
                previous.joins.push(Join {
                    kind: JoinKind::Inner,
                    spelled_out: false,
                    table: table.relation,
                    constraint,
                });
                while let Some(join) = D::parse_join(p)? {
                    previous.joins.push(join);
                }
            }
            _ => tables.push(table),
        }
        if !p.eat_op(",") {
            return Ok(tables);
        }
    }
}

fn parse_is<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
    left: Expr<'a, D::Ext>,
    bp: u8,
) -> PResult<Expr<'a, D::Ext>> {
    let negated = p.peek_nth(1).is_word("NOT");
    let next = p.peek_nth(if negated { 2 } else { 1 });
    let core_test = ["NULL", "TRUE", "FALSE", "DISTINCT"]
        .iter()
        .any(|word| next.is_word(word));
    if core_test {
        return grammar::parse_is(p, left, bp);
    }
    p.advance();
    if negated {
        p.advance();
    }
    let right = p.parse_expr_bp(bp)?;
    Ok(ExprKind::Binary {
        left: Box::new(left),
        op: BinaryOp::Named(if negated {
            "IS NOT".into()
        } else {
            "IS".into()
        }),
        right: Box::new(right),
    }
    .into())
}

/// Parses `IN` and `NOT IN`, whose right side can name a table.
fn parse_in_operand<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
    left: Expr<'a, Builtin>,
    negated: bool,
) -> PResult<Expr<'a, Builtin>> {
    let token = p.peek();
    let table_name = token.kind == TokenKind::QuotedIdent
        || (token.kind == TokenKind::Word && !D::is_reserved(token.text));
    if table_name {
        let start = p.peek().span.start;
        let name = grammar::parse_object_name(p)?;
        let target = if p.at_op("(") {
            p.parse_function_call(name)?
        } else {
            ExprKind::Compound(name).into()
        };
        let target = Expr::new(target.kind, p.span_since(start));
        return Ok(ExprKind::Ext(BuiltinExpr::Sqlite(SqliteExpr::InTable {
            expr: Box::new(left),
            target: Box::new(target),
            negated,
        }))
        .into());
    }
    p.expect_op("(")?;
    if grammar::query_starts_at(p, 0) {
        let subquery = Box::new(grammar::parse_subquery_body(p)?);
        return Ok(ExprKind::InSubquery {
            expr: Box::new(left),
            subquery,
            negated,
        }
        .into());
    }
    let list = if p.at_op(")") {
        Vec::new()
    } else {
        p.parse_comma_separated(Parser::parse_expr)?
    };
    p.expect_op(")")?;
    Ok(ExprKind::InList {
        expr: Box::new(left),
        list,
        negated,
    }
    .into())
}

fn parse_in<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
    left: Expr<'a, Builtin>,
    _bp: u8,
) -> PResult<Expr<'a, Builtin>> {
    p.advance();
    parse_in_operand(p, left, false)
}

fn parse_not_in<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
    left: Expr<'a, Builtin>,
    _bp: u8,
) -> PResult<Expr<'a, Builtin>> {
    p.advance();
    p.advance();
    parse_in_operand(p, left, true)
}

/// A postfix `ISNULL`, `NOTNULL` or `NOT NULL`, as an `IS` test.
fn null_test<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
    left: Expr<'a, D::Ext>,
    words: usize,
    negated: bool,
) -> PResult<Expr<'a, D::Ext>> {
    for _ in 0..words {
        p.advance();
    }
    Ok(ExprKind::Is {
        expr: Box::new(left),
        test: IsTest::Null,
        negated,
    }
    .into())
}

fn parse_isnull<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
    left: Expr<'a, D::Ext>,
    _bp: u8,
) -> PResult<Expr<'a, D::Ext>> {
    null_test(p, left, 1, false)
}

fn parse_notnull<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
    left: Expr<'a, D::Ext>,
    _bp: u8,
) -> PResult<Expr<'a, D::Ext>> {
    null_test(p, left, 1, true)
}

fn parse_not_null<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
    left: Expr<'a, D::Ext>,
    _bp: u8,
) -> PResult<Expr<'a, D::Ext>> {
    null_test(p, left, 2, true)
}

fn parse_not_pattern<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
    left: Expr<'a, D::Ext>,
    bp: u8,
) -> PResult<Expr<'a, D::Ext>> {
    p.advance();
    let token = p.advance();
    let op = if token.is_word("MATCH") {
        BinaryOp::Named("NOT MATCH".into())
    } else if token.is_word("GLOB") {
        BinaryOp::Named("NOT GLOB".into())
    } else {
        BinaryOp::Named("NOT REGEXP".into())
    };
    let right = p.parse_expr_bp(bp)?;
    Ok(ExprKind::Binary {
        left: Box::new(left),
        op,
        right: Box::new(right),
    }
    .into())
}

fn pattern_op(word: &str) -> Option<BinaryOp<'static>> {
    if word.eq_ignore_ascii_case("MATCH") {
        Some(BinaryOp::Named("MATCH".into()))
    } else if word.eq_ignore_ascii_case("GLOB") {
        Some(BinaryOp::Named("GLOB".into()))
    } else if word.eq_ignore_ascii_case("REGEXP") {
        Some(BinaryOp::Named("REGEXP".into()))
    } else {
        None
    }
}

pub fn infix_rule<'a, D: Dialect<Ext = Builtin>>(p: &Parser<'a, D>) -> Option<InfixRule<'a, D>> {
    let token = p.peek();
    match token.kind {
        TokenKind::Op => {
            let op = |op| Some((RELATIONAL, op));
            let bit = |op| Some((BITWISE, op));
            let concat = |op| Some((CONCAT, op));
            let rule = match token.text {
                "==" => Some((EQUALITY, BinaryOp::Eq)),
                "<" => op(BinaryOp::Lt),
                "<=" => op(BinaryOp::LtEq),
                ">" => op(BinaryOp::Gt),
                ">=" => op(BinaryOp::GtEq),
                "&" => bit(BinaryOp::BitAnd),
                "|" => bit(BinaryOp::BitOr),
                "<<" => bit(BinaryOp::ShiftLeft),
                ">>" => bit(BinaryOp::ShiftRight),
                "||" => concat(BinaryOp::StringConcat),
                "->" => concat(BinaryOp::Named("->".into())),
                "->>" => concat(BinaryOp::Named("->>".into())),
                "::" => {
                    let parse: InfixParseFn<'a, D> = postgres::parse_double_colon;
                    return Some(InfixRule::custom(prec::POSTFIX, parse));
                }
                _ => None,
            };
            if let Some((bp, op)) = rule {
                return Some(InfixRule::binary(bp, op));
            }
        }
        TokenKind::Word => {
            if let Some(op) = pattern_op(token.text) {
                return Some(InfixRule::binary(EQUALITY, op));
            }
            if token.is_word("IS") {
                let parse: InfixParseFn<'a, D> = parse_is;
                return Some(InfixRule::custom(EQUALITY, parse));
            }
            if token.is_word("IN") {
                let parse: InfixParseFn<'a, D> = parse_in;
                return Some(InfixRule::custom(EQUALITY, parse));
            }
            if token.is_word("ISNULL") {
                let parse: InfixParseFn<'a, D> = parse_isnull;
                return Some(InfixRule::custom(EQUALITY, parse));
            }
            if token.is_word("NOTNULL") {
                let parse: InfixParseFn<'a, D> = parse_notnull;
                return Some(InfixRule::custom(EQUALITY, parse));
            }
            if token.is_word("NOT") {
                let next = p.peek_nth(1);
                let parse: Option<InfixParseFn<'a, D>> = if next.is_word("IN") {
                    Some(parse_not_in)
                } else if next.is_word("NULL") {
                    Some(parse_not_null)
                } else if next.kind == TokenKind::Word && pattern_op(next.text).is_some() {
                    Some(parse_not_pattern)
                } else {
                    None
                };
                if let Some(parse) = parse {
                    return Some(InfixRule::custom(EQUALITY, parse));
                }
            }
        }
        _ => {}
    }
    grammar::infix_rule(p)
}

pub fn parse_between_low<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Expr<'a, D::Ext>> {
    p.parse_expr_bp(prec::AND)
}

pub fn parse_prefix<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Expr<'a, D::Ext>> {
    if p.peek().kind == TokenKind::String && p.peek_nth(1).is_op(".") {
        let name = parse_name(p)?;
        return if p.at_op("(") {
            p.parse_function_call(name)
        } else {
            Ok(ExprKind::Compound(name).into())
        };
    }
    if p.eat_op("~") {
        let expr = p.parse_expr_bp(prec::UNARY)?;
        return Ok(ExprKind::Unary {
            op: UnaryOp::BitNot,
            expr: Box::new(expr),
        }
        .into());
    }
    grammar::parse_prefix(p)
}

/// `OVER` starts a window only before `(` or a name. Otherwise it is an alias,
/// left for the select list to read.
pub fn parse_function_over<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<Option<WindowOver<'a, D::Ext>>> {
    if !p.at_keyword("OVER") {
        return Ok(None);
    }
    let next = p.peek_nth(1);
    if next.is_op("(") {
        p.advance();
        return grammar::parse_window_over(p).map(Some);
    }
    let names_window = match next.kind {
        TokenKind::Word => !ends_select_item(next.text),
        TokenKind::String | TokenKind::QuotedIdent => true,
        _ => false,
    };
    if !names_window {
        return Ok(None);
    }
    p.advance();
    parse_any_name(p).map(|name| Some(WindowOver::Named(name)))
}

/// The arguments of a data type, with their signs, if they can only be read
/// by SQLite: at most two, and signed or not integers.
fn signed_args<'a, D: Dialect>(p: &Parser<'a, D>) -> Option<(Vec<&'a str>, [u8; 2], usize)> {
    if !p.peek_nth(1).is_op("(") {
        return None;
    }
    let mut args = Vec::with_capacity(2);
    let mut signs = [0; 2];
    let mut n = 2;
    let mut unusual = false;
    for sign in &mut signs {
        let token = p.peek_nth(n);
        if token.is_op("-") || token.is_op("+") {
            *sign = token.text.as_bytes()[0];
            unusual = true;
            n += 1;
        }
        let number = p.peek_nth(n);
        if number.kind != TokenKind::Number {
            return None;
        }
        unusual |= number.text.parse::<u64>().is_err();
        args.push(number.text);
        n += 1;
        if p.peek_nth(n).is_op(")") {
            n += 1;
            return unusual.then_some((args, signs, n));
        }
        if !p.peek_nth(n).is_op(",") {
            return None;
        }
        n += 1;
    }
    None
}

pub fn parse_data_type<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<DataType<'a>> {
    if p.at_op(")") {
        return Ok(DataType::unspecified(p.peek().span));
    }
    if matches!(p.peek().kind, TokenKind::Word | TokenKind::QuotedIdent)
        && let Some((args, signs, tokens)) = signed_args(p)
    {
        let name = p.parse_ident()?;
        for _ in 1..tokens {
            p.advance();
        }
        return Ok(DataType {
            qualifier: Vec::new(),
            name,
            angle: None,
            args: args.into_iter().map(Cow::Borrowed).collect(),
            suffix: DataTypeSuffix::SignedArgs(signs),
            array: ArrayType::None,
        });
    }
    let mut data_type = grammar::parse_data_type(p)?;
    if data_type.args.iter().any(|arg| arg.parse::<u64>().is_err()) {
        return Err(p.expected("at most two numeric type arguments"));
    }
    if data_type.suffix == DataTypeSuffix::None && p.eat_keyword("UNSIGNED") {
        data_type.suffix = DataTypeSuffix::Unsigned;
    }
    Ok(data_type)
}

/// True if a column definition goes straight to its constraints.
fn at_untyped_column<'a, D: Dialect>(p: &Parser<'a, D>) -> bool {
    let token = p.peek();
    match token.kind {
        TokenKind::QuotedIdent => false,
        TokenKind::Word => [
            "CONSTRAINT",
            "PRIMARY",
            "NOT",
            "UNIQUE",
            "CHECK",
            "DEFAULT",
            "COLLATE",
            "REFERENCES",
            "GENERATED",
            "AS",
        ]
        .iter()
        .any(|word| token.is_word(word)),
        _ => true,
    }
}

pub fn parse_column_def<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<ColumnDef<'a, D::Ext>> {
    let name = if p.peek().kind == TokenKind::String {
        parse_any_name(p)?
    } else {
        p.parse_ident()?
    };
    let data_type = if at_untyped_column(p) {
        DataType::unspecified(p.peek().span)
    } else {
        D::parse_data_type(p)?
    };
    let mut options = Vec::new();
    while let Some(option) = D::parse_column_option(p)? {
        options.push(option);
    }
    grammar::check_column_attributes(p, &options)?;
    Ok(ColumnDef {
        name,
        data_type,
        options,
    })
}

pub fn parse_column_option<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<Option<ColumnOption<'a, D::Ext>>> {
    if p.at_keyword("ON") && p.peek_nth(1).is_word("CONFLICT") {
        p.advance();
        p.advance();
        const ACTIONS: [(&str, &str); 5] = [
            ("ROLLBACK", "ON CONFLICT ROLLBACK"),
            ("ABORT", "ON CONFLICT ABORT"),
            ("FAIL", "ON CONFLICT FAIL"),
            ("IGNORE", "ON CONFLICT IGNORE"),
            ("REPLACE", "ON CONFLICT REPLACE"),
        ];
        let Some((_, keyword)) = ACTIONS.iter().find(|(word, _)| p.eat_keyword(word)) else {
            return Err(p.expected("ROLLBACK, ABORT, FAIL, IGNORE or REPLACE"));
        };
        return Ok(Some(ColumnOption::Keyword {
            keyword: (*keyword).into(),
            expr: None,
        }));
    }
    for keyword in ["AUTOINCREMENT", "ASC", "DESC"] {
        if p.eat_keyword(keyword) {
            return Ok(Some(ColumnOption::Keyword {
                keyword: keyword.into(),
                expr: None,
            }));
        }
    }
    if p.eat_keyword("CONSTRAINT") {
        let name = p.parse_ident()?;
        let Some(option) = D::parse_column_option(p)? else {
            return Err(p.expected("constraint details after CONSTRAINT <name>"));
        };
        return Ok(Some(ColumnOption::Named {
            name,
            option: Box::new(option),
        }));
    }
    // A full expression here would read a following `NOT NULL` as a postfix test.
    if p.eat_keyword("DEFAULT") {
        return p
            .nested(Parser::parse_prefix)
            .map(|expr| Some(ColumnOption::Default(expr)));
    }
    if p.at_keyword("AS") && p.peek_nth(1).is_op("(") {
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
        return Ok(Some(ColumnOption::Generated(Box::new(Generated {
            when: Some(GeneratedWhen::Omitted),
            kind: GeneratedKind::Expr { expr, storage },
        }))));
    }
    grammar::parse_column_option(p)
}

/// Parses `(columns)`, returning no columns if there is no list.
fn parse_optional_columns<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Vec<Ident<'a>>> {
    if grammar::at_column_list(p) {
        grammar::parse_ident_list(p)
    } else {
        Ok(Vec::new())
    }
}

fn parse_table_option<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Option<SqliteTableOption>> {
    if p.eat_keyword("WITHOUT") {
        p.expect_keyword("ROWID")?;
        return Ok(Some(SqliteTableOption::WithoutRowid));
    }
    Ok(p.eat_keyword("STRICT").then_some(SqliteTableOption::Strict))
}

/// Parses `[,] option [, option ...]`, or nothing if no option comes next.
fn parse_table_options<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<(bool, Vec<SqliteTableOption>)> {
    let next = p.peek_nth(1);
    let leading_comma = p.at_op(",") && (next.is_word("STRICT") || next.is_word("WITHOUT"));
    if leading_comma {
        p.advance();
    }
    let mut options = Vec::new();
    let Some(first) = parse_table_option(p)? else {
        return Ok((false, options));
    };
    options.push(first);
    while p.eat_op(",") {
        match parse_table_option(p)? {
            Some(option) => options.push(option),
            None => return Err(p.expected("WITHOUT ROWID or STRICT")),
        }
    }
    Ok((leading_comma, options))
}

fn parse_create_table<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
    temporary: bool,
) -> PResult<Statement<'a, Builtin>> {
    p.expect_keyword("TABLE")?;
    let if_not_exists = grammar::parse_if_not_exists(p)?;
    let name = parse_name(p)?;
    let (columns, constraints) = if p.at_op("(") {
        grammar::parse_columns(p)?
    } else {
        (Vec::new(), Vec::new())
    };
    let (leading_comma, options) = parse_table_options(p)?;
    let query = if p.eat_keyword("AS") {
        Some(Box::new(D::parse_query(p)?))
    } else {
        None
    };
    if columns.is_empty() && constraints.is_empty() && query.is_none() {
        return Err(p.expected("column definitions or AS"));
    }
    let table = CreateTable {
        or_replace: false,
        temporary,
        name,
        if_not_exists,
        columns,
        constraints,
        query,
    };
    if options.is_empty() {
        return Ok(StatementKind::CreateTable(Box::new(table)).into());
    }
    Ok(sqlite(SqliteStatement::CreateTable(Box::new(
        SqliteCreateTable {
            table,
            leading_comma,
            options,
        },
    ))))
}

fn parse_virtual_table<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<SqliteVirtualTable<'a>> {
    p.expect_keyword("VIRTUAL")?;
    p.expect_keyword("TABLE")?;
    let if_not_exists = grammar::parse_if_not_exists(p)?;
    let name = parse_name(p)?;
    p.expect_keyword("USING")?;
    let module = parse_any_name(p)?;
    let args = if p.eat_op("(") {
        let args = p.parse_comma_separated(parse_any_name)?;
        p.expect_op(")")?;
        args
    } else {
        Vec::new()
    };
    Ok(SqliteVirtualTable {
        if_not_exists,
        name,
        module,
        args,
    })
}

fn parse_trigger_event<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<TriggerEvent<'a>> {
    if p.eat_keyword("DELETE") {
        return Ok(TriggerEvent::Delete);
    }
    if p.eat_keyword("INSERT") {
        return Ok(TriggerEvent::Insert);
    }
    p.expect_keyword("UPDATE")?;
    let columns = if p.eat_keyword("OF") {
        p.parse_comma_separated(Parser::parse_ident)?
    } else {
        Vec::new()
    };
    Ok(TriggerEvent::Update(columns))
}

fn parse_trigger<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
    temporary: bool,
) -> PResult<SqliteTrigger<'a>> {
    p.expect_keyword("TRIGGER")?;
    let if_not_exists = grammar::parse_if_not_exists(p)?;
    let name = parse_name(p)?;
    let period = if p.eat_keyword("BEFORE") {
        Some(TriggerPeriod::Before)
    } else if p.eat_keyword("AFTER") {
        Some(TriggerPeriod::After)
    } else if p.eat_keyword("INSTEAD") {
        p.expect_keyword("OF")?;
        Some(TriggerPeriod::InsteadOf)
    } else {
        None
    };
    let event = parse_trigger_event(p)?;
    p.expect_keyword("ON")?;
    let table = parse_name(p)?;
    let for_each_row = p.eat_keyword("FOR");
    if for_each_row {
        p.expect_keyword("EACH")?;
        p.expect_keyword("ROW")?;
    }
    let when = if p.eat_keyword("WHEN") {
        Some(p.parse_expr()?)
    } else {
        None
    };
    p.expect_keyword("BEGIN")?;
    let mut statements = Vec::new();
    while !p.at_keyword("END") {
        statements.push(p.nested(|p| p.parse_statement())?);
        p.expect_op(";")?;
    }
    if statements.is_empty() {
        return Err(p.expected("a statement in the trigger body"));
    }
    p.expect_keyword("END")?;
    Ok(SqliteTrigger {
        temporary,
        if_not_exists,
        name,
        period,
        event,
        table,
        for_each_row,
        when,
        statements,
    })
}

pub fn parse_create<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Statement<'a, Builtin>> {
    let temporary = p.peek_nth(1).is_word("TEMP") || p.peek_nth(1).is_word("TEMPORARY");
    let object = p.peek_nth(if temporary { 2 } else { 1 });
    if object.is_word("TRIGGER") || object.is_word("TABLE") || object.is_word("VIRTUAL") {
        p.advance();
        if temporary {
            p.advance();
        }
        if object.is_word("TRIGGER") {
            let trigger = parse_trigger(p, temporary)?;
            return Ok(sqlite(SqliteStatement::CreateTrigger(Box::new(trigger))));
        }
        if object.is_word("TABLE") {
            return parse_create_table(p, temporary);
        }
        if temporary {
            return Err(p.expected("TABLE or TRIGGER"));
        }
        let table = parse_virtual_table(p)?;
        return Ok(sqlite(SqliteStatement::CreateVirtualTable(Box::new(table))));
    }
    grammar::parse_create(p)
}

fn sqlite<'a>(statement: SqliteStatement<'a>) -> Statement<'a, Builtin> {
    StatementKind::Ext(BuiltinStatement::Sqlite(statement)).into()
}

fn parse_or_action<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Option<&'static str>> {
    if !p.eat_keyword("OR") {
        return Ok(None);
    }
    match ["ROLLBACK", "ABORT", "REPLACE", "FAIL", "IGNORE"]
        .into_iter()
        .find(|word| p.eat_keyword(word))
    {
        Some(action) => Ok(Some(action)),
        None => Err(p.expected("ROLLBACK, ABORT, REPLACE, FAIL or IGNORE")),
    }
}

fn parse_assignment<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<SqliteAssignment<'a>> {
    let target = if p.eat_op("(") {
        let targets = p.parse_comma_separated(grammar::parse_object_name)?;
        p.expect_op(")")?;
        SqliteAssignmentTarget::Tuple(targets)
    } else {
        SqliteAssignmentTarget::Column(grammar::parse_object_name(p)?)
    };
    if !p.eat_op("=") && !p.eat_op("==") {
        return Err(p.expected("="));
    }
    let value = p.parse_expr()?;
    Ok(SqliteAssignment { target, value })
}

/// Converts assignments to the core's, or hands them back if one is a tuple.
fn core_assignments<'a>(
    assignments: Vec<SqliteAssignment<'a>>,
) -> Result<Vec<Assignment<'a, Builtin>>, Vec<SqliteAssignment<'a>>> {
    let has_tuple = assignments
        .iter()
        .any(|assignment| matches!(assignment.target, SqliteAssignmentTarget::Tuple(_)));
    if has_tuple {
        return Err(assignments);
    }
    Ok(assignments
        .into_iter()
        .filter_map(|assignment| match assignment.target {
            SqliteAssignmentTarget::Column(target) => Some(Assignment {
                target,
                value: assignment.value,
            }),
            SqliteAssignmentTarget::Tuple(_) => None,
        })
        .collect())
}

fn parse_on_conflict<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<OnConflict<'a>> {
    p.expect_keyword("ON")?;
    p.expect_keyword("CONFLICT")?;
    let columns = if p.at_op("(") {
        grammar::parse_ident_list(p)?
    } else {
        Vec::new()
    };
    let predicate = grammar::parse_optional_where(p)?;
    p.expect_keyword("DO")?;
    let action = if p.eat_keyword("NOTHING") {
        OnConflictAction::DoNothing
    } else {
        p.expect_keyword("UPDATE")?;
        p.expect_keyword("SET")?;
        OnConflictAction::DoUpdate {
            assignments: p.parse_comma_separated(parse_assignment)?,
            selection: grammar::parse_optional_where(p)?,
        }
    };
    Ok(OnConflict {
        columns,
        predicate,
        action,
    })
}

pub fn parse_insert<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Statement<'a, Builtin>> {
    let or_action = if p.eat_keyword("REPLACE") {
        Some("REPLACE")
    } else {
        p.expect_keyword("INSERT")?;
        parse_or_action(p)?
    };
    p.expect_keyword("INTO")?;
    let table = parse_name(p)?;
    let columns = parse_optional_columns(p)?;
    let source = if columns.is_empty() && p.at_keyword("DEFAULT") && p.peek_nth(1).is_word("VALUES")
    {
        p.advance();
        p.advance();
        None
    } else {
        Some(grammar::parse_insert_source(p)?)
    };
    let on_conflict = if p.at_keyword("ON") {
        Some(parse_on_conflict(p)?)
    } else {
        None
    };
    let returning = D::parse_returning(p)?;
    match source {
        Some(source) if or_action.is_none() && on_conflict.is_none() => {
            Ok(StatementKind::Insert(Box::new(Insert {
                into: true,
                output: None,
                modifiers: Default::default(),
                table,
                columns,
                source,
                returning,
            }))
            .into())
        }
        source => Ok(sqlite(SqliteStatement::Insert(Box::new(SqliteInsert {
            or_action: or_action.map(Cow::Borrowed),
            table,
            columns,
            source,
            on_conflict,
            returning,
        })))),
    }
}

fn parse_limit_expr<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Option<Expr<'a, Builtin>>> {
    if p.eat_keyword("LIMIT") {
        p.parse_expr().map(Some)
    } else {
        Ok(None)
    }
}

fn parse_update<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Statement<'a, Builtin>> {
    p.expect_keyword("UPDATE")?;
    let or_action = parse_or_action(p)?;
    let table = p.parse_table_ref()?;
    p.expect_keyword("SET")?;
    let mut assignments = p.parse_comma_separated(parse_assignment)?;
    let from = if p.eat_keyword("FROM") {
        D::parse_from_list(p)?
    } else {
        Vec::new()
    };
    let selection = grammar::parse_optional_where(p)?;
    let returning = D::parse_returning(p)?;
    let limit = parse_limit_expr(p)?;
    if or_action.is_none() && from.is_empty() && limit.is_none() {
        match core_assignments(assignments) {
            Ok(assignments) => {
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
                    returning,
                }))
                .into());
            }
            Err(unchanged) => assignments = unchanged,
        }
    }
    Ok(sqlite(SqliteStatement::Update(Box::new(SqliteUpdate {
        or_action: or_action.map(Cow::Borrowed),
        table,
        assignments,
        from,
        selection,
        returning,
        limit,
    }))))
}

fn parse_delete<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Statement<'a, Builtin>> {
    p.expect_keyword("DELETE")?;
    p.expect_keyword("FROM")?;
    let table = p.parse_table_ref()?;
    let selection = grammar::parse_optional_where(p)?;
    let returning = D::parse_returning(p)?;
    let order_by = if p.eat_keyword("ORDER") {
        p.expect_keyword("BY")?;
        p.parse_comma_separated(D::parse_order_by_item)?
    } else {
        Vec::new()
    };
    let limit = parse_limit_expr(p)?;
    if order_by.is_empty() && limit.is_none() {
        return Ok(StatementKind::Delete(Box::new(Delete {
            output: None,
            multi: None,
            using: Vec::new(),
            table,
            selection,
            order_by: Vec::new(),
            limit: None,
            returning,
        }))
        .into());
    }
    Ok(sqlite(SqliteStatement::Delete(Box::new(SqliteDelete {
        table,
        selection,
        returning,
        order_by,
        limit,
    }))))
}

fn parse_pragma_value<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Expr<'a, D::Ext>> {
    let token = p.peek();
    if token.kind == TokenKind::Word {
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
    if p.at_op("(") {
        return Err(p.expected("a pragma value"));
    }
    p.nested(Parser::parse_prefix)
}

fn parse_pragma<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<SqliteStatement<'a>> {
    p.expect_keyword("PRAGMA")?;
    let name = parse_name(p)?;
    let value = if p.eat_op("=") {
        Some((PragmaForm::Equals, parse_pragma_value(p)?))
    } else if p.eat_op("(") {
        let value = parse_pragma_value(p)?;
        p.expect_op(")")?;
        Some((PragmaForm::Call, value))
    } else {
        None
    };
    Ok(SqliteStatement::Pragma { name, value })
}

fn parse_attach<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<SqliteStatement<'a>> {
    p.expect_keyword("ATTACH")?;
    let database_keyword = p.eat_keyword("DATABASE");
    let file = p.parse_expr()?;
    p.expect_keyword("AS")?;
    let schema = parse_any_name(p)?;
    Ok(SqliteStatement::Attach {
        database_keyword,
        file,
        schema,
    })
}

fn at_begin_modifier<'a, D: Dialect>(p: &Parser<'a, D>) -> Option<&'static str> {
    if !p.at_keyword("BEGIN") {
        return None;
    }
    let next = p.peek_nth(1);
    ["DEFERRED", "IMMEDIATE", "EXCLUSIVE"]
        .into_iter()
        .find(|word| next.is_word(word))
}

fn parse_begin<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
    modifier: &'static str,
) -> SqliteStatement<'a> {
    p.advance();
    p.advance();
    let transaction_keyword = p.eat_keyword("TRANSACTION");
    SqliteStatement::Begin {
        modifier: modifier.into(),
        transaction_keyword,
    }
}

pub fn parse_statement<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Statement<'a, Builtin>> {
    if p.at_keyword("ANALYZE") {
        return grammar::parse_analyze(p);
    }
    if p.at_keyword("PRAGMA") {
        return parse_pragma(p).map(sqlite);
    }
    if p.at_keyword("ATTACH") {
        return parse_attach(p).map(sqlite);
    }
    if let Some(modifier) = at_begin_modifier(p) {
        return Ok(sqlite(parse_begin(p, modifier)));
    }
    if p.at_keyword("REPLACE") || p.at_keyword("INSERT") {
        return D::parse_insert_statement(p);
    }
    if p.at_keyword("UPDATE") {
        return parse_update(p);
    }
    if p.at_keyword("DELETE") {
        return parse_delete(p);
    }
    grammar::parse_statement(p)
}

impl Dialect for Sqlite {
    type Ext = Builtin;

    fn parse_statement_after_with<'a>(
        p: &mut Parser<'a, Self>,
    ) -> PResult<Option<Statement<'a, Builtin>>> {
        grammar::parse_dml_after_with(p, true, true)
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
        is_reserved(word)
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

    fn parse_create<'a>(p: &mut Parser<'a, Self>) -> PResult<Statement<'a, Builtin>> {
        parse_create(p)
    }

    fn parse_drop_object_type<'a>(p: &mut Parser<'a, Self>) -> Option<ObjectType<'a>> {
        p.eat_keyword("TRIGGER")
            .then_some(ObjectType::Named("TRIGGER".into()))
    }

    fn parse_column_def<'a>(p: &mut Parser<'a, Self>) -> PResult<ColumnDef<'a, Builtin>> {
        parse_column_def(p)
    }

    fn parse_column_option<'a>(
        p: &mut Parser<'a, Self>,
    ) -> PResult<Option<ColumnOption<'a, Builtin>>> {
        parse_column_option(p)
    }

    fn parse_data_type<'a>(p: &mut Parser<'a, Self>) -> PResult<DataType<'a>> {
        parse_data_type(p)
    }

    // Every word that can name a column can also stand here, so there is no
    // operator class to look for.
    fn parse_index_column<'a>(p: &mut Parser<'a, Self>) -> PResult<IndexColumn<'a, Builtin>> {
        grammar::parse_order_by_item(p).map(|item| IndexColumn {
            item,
            operator_class: None,
        })
    }

    fn parse_from_list<'a>(p: &mut Parser<'a, Self>) -> PResult<Vec<TableWithJoins<'a, Builtin>>> {
        parse_from_list(p)
    }

    fn check_nested_join<'a>(
        _p: &Parser<'a, Self>,
        _table: &TableWithJoins<'a, Builtin>,
    ) -> PResult<()> {
        Ok(())
    }

    fn parse_table_ref<'a>(p: &mut Parser<'a, Self>) -> PResult<TableRef<'a, Builtin>> {
        parse_table_ref(p)
    }

    fn parse_table_factor<'a>(p: &mut Parser<'a, Self>) -> PResult<TableFactor<'a, Builtin>> {
        parse_table_factor(p)
    }

    fn parse_name_part<'a>(p: &mut Parser<'a, Self>) -> PResult<Ident<'a>> {
        parse_name_part(p)
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

    fn parse_between_low<'a>(p: &mut Parser<'a, Self>) -> PResult<Expr<'a, Builtin>> {
        parse_between_low(p)
    }

    fn parse_limit<'a>(p: &mut Parser<'a, Self>) -> PResult<Limit<'a, Builtin>> {
        mysql::parse_limit(p)
    }

    fn at_empty_function_args<'a>(p: &Parser<'a, Self>) -> bool {
        p.at_op(")") || p.at_keyword("ORDER")
    }

    fn parse_function_over<'a>(
        p: &mut Parser<'a, Self>,
    ) -> PResult<Option<WindowOver<'a, Builtin>>> {
        parse_function_over(p)
    }

    fn parse_prefix<'a>(p: &mut Parser<'a, Self>) -> PResult<Expr<'a, Builtin>> {
        parse_prefix(p)
    }

    fn infix_rule<'a>(p: &Parser<'a, Self>) -> Option<InfixRule<'a, Self>> {
        infix_rule(p)
    }
}

impl fmt::Display for SqliteIndexedTable<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.table.fmt(f)?;
        match &self.index {
            Some(index) => write!(f, " INDEXED BY {index}"),
            None => f.write_str(" NOT INDEXED"),
        }
    }
}

impl fmt::Display for SqliteExpr<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InTable {
                expr,
                target,
                negated,
            } => {
                let not = if *negated { "NOT " } else { "" };
                write!(f, "{expr} {not}IN {target}")
            }
        }
    }
}

impl fmt::Display for SqliteAssignmentTarget<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Column(column) => column.fmt(f),
            Self::Tuple(columns) => write!(f, "({})", comma(columns)),
        }
    }
}

impl fmt::Display for SqliteAssignment<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} = {}", self.target, self.value)
    }
}

impl fmt::Display for SqliteTableOption {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::WithoutRowid => "WITHOUT ROWID",
            Self::Strict => "STRICT",
        })
    }
}

impl fmt::Display for SqliteCreateTable<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let table = &self.table;
        write_create_flags(f, table.or_replace, table.temporary)?;
        f.write_str("TABLE ")?;
        write_if_not_exists(f, table.if_not_exists)?;
        table.name.fmt(f)?;
        if !table.columns.is_empty() || !table.constraints.is_empty() {
            write_columns(f, &table.columns, &table.constraints)?;
        }
        let comma_prefix = if self.leading_comma { ", " } else { " " };
        write!(f, "{comma_prefix}{}", comma(&self.options))?;
        if let Some(query) = &table.query {
            write!(f, " AS {query}")?;
        }
        Ok(())
    }
}

impl fmt::Display for SqliteVirtualTable<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CREATE VIRTUAL TABLE ")?;
        write_if_not_exists(f, self.if_not_exists)?;
        write!(f, "{} USING {}", self.name, self.module)?;
        if !self.args.is_empty() {
            write!(f, " ({})", comma(&self.args))?;
        }
        Ok(())
    }
}

impl fmt::Display for TriggerEvent<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Delete => f.write_str("DELETE"),
            Self::Insert => f.write_str("INSERT"),
            Self::Update(columns) if columns.is_empty() => f.write_str("UPDATE"),
            Self::Update(columns) => write!(f, "UPDATE OF {}", comma(columns)),
        }
    }
}

impl fmt::Display for SqliteTrigger<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_create_flags(f, false, self.temporary)?;
        f.write_str("TRIGGER ")?;
        write_if_not_exists(f, self.if_not_exists)?;
        write!(f, "{} ", self.name)?;
        match self.period {
            Some(TriggerPeriod::Before) => f.write_str("BEFORE ")?,
            Some(TriggerPeriod::After) => f.write_str("AFTER ")?,
            Some(TriggerPeriod::InsteadOf) => f.write_str("INSTEAD OF ")?,
            None => {}
        }
        write!(f, "{} ON {}", self.event, self.table)?;
        if self.for_each_row {
            f.write_str(" FOR EACH ROW")?;
        }
        if let Some(when) = &self.when {
            write!(f, " WHEN {when}")?;
        }
        f.write_str(" BEGIN ")?;
        for statement in &self.statements {
            write!(f, "{statement}; ")?;
        }
        f.write_str("END")
    }
}

fn write_or_action(f: &mut fmt::Formatter<'_>, or_action: Option<&str>) -> fmt::Result {
    match or_action {
        Some(action) => write!(f, "OR {action} "),
        None => Ok(()),
    }
}

fn write_returning(
    f: &mut fmt::Formatter<'_>,
    returning: &[SelectItem<'_, Builtin>],
) -> fmt::Result {
    write_list_clause(f, "RETURNING", returning)
}

impl fmt::Display for OnConflict<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ON CONFLICT")?;
        if !self.columns.is_empty() {
            write!(f, "({})", comma(&self.columns))?;
        }
        if let Some(predicate) = &self.predicate {
            write!(f, " WHERE {predicate}")?;
        }
        match &self.action {
            OnConflictAction::DoNothing => f.write_str(" DO NOTHING"),
            OnConflictAction::DoUpdate {
                assignments,
                selection,
            } => {
                write!(f, " DO UPDATE SET {}", comma(assignments))?;
                if let Some(selection) = selection {
                    write!(f, " WHERE {selection}")?;
                }
                Ok(())
            }
        }
    }
}

impl fmt::Display for SqliteInsert<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("INSERT ")?;
        write_or_action(f, self.or_action.as_deref())?;
        write!(f, "INTO {}", self.table)?;
        if !self.columns.is_empty() {
            write!(f, " ({})", comma(&self.columns))?;
        }
        match &self.source {
            None => f.write_str(" DEFAULT VALUES")?,
            Some(source) => {
                crate::display::write_insert_source(f, source, !self.columns.is_empty())?;
            }
        }
        if let Some(on_conflict) = &self.on_conflict {
            write!(f, " {on_conflict}")?;
        }
        write_returning(f, &self.returning)
    }
}

impl fmt::Display for SqliteUpdate<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("UPDATE ")?;
        write_or_action(f, self.or_action.as_deref())?;
        self.table.fmt(f)?;
        write_list_clause(f, "SET", &self.assignments)?;
        write_list_clause(f, "FROM", &self.from)?;
        write_where_clause(f, &self.selection)?;
        write_returning(f, &self.returning)?;
        if let Some(limit) = &self.limit {
            SpaceOrNewline.fmt(f)?;
            write!(f, "LIMIT {limit}")?;
        }
        Ok(())
    }
}

impl fmt::Display for SqliteDelete<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("DELETE FROM")?;
        SpaceOrNewline.fmt(f)?;
        Indent(&self.table).fmt(f)?;
        write_where_clause(f, &self.selection)?;
        write_returning(f, &self.returning)?;
        write_list_clause(f, "ORDER BY", &self.order_by)?;
        if let Some(limit) = &self.limit {
            write_indented_clause(f, "LIMIT", limit)?;
        }
        Ok(())
    }
}

impl fmt::Display for SqliteStatement<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pragma { name, value } => {
                write!(f, "PRAGMA {name}")?;
                match value {
                    None => Ok(()),
                    Some((PragmaForm::Equals, value)) => write!(f, " = {value}"),
                    Some((PragmaForm::Call, value)) => write!(f, "({value})"),
                }
            }
            Self::Attach {
                database_keyword,
                file,
                schema,
            } => {
                let database = if *database_keyword { "DATABASE " } else { "" };
                write!(f, "ATTACH {database}{file} AS {schema}")
            }
            Self::Begin {
                modifier,
                transaction_keyword,
            } => {
                write!(f, "BEGIN {modifier}")?;
                if *transaction_keyword {
                    f.write_str(" TRANSACTION")?;
                }
                Ok(())
            }
            Self::CreateTable(table) => table.fmt(f),
            Self::CreateVirtualTable(table) => table.fmt(f),
            Self::CreateTrigger(trigger) => trigger.fmt(f),
            Self::Insert(insert) => insert.fmt(f),
            Self::Update(update) => update.fmt(f),
            Self::Delete(delete) => delete.fmt(f),
        }
    }
}

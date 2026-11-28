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

//! Apache Spark SQL.

use std::borrow::Cow;
use std::fmt;
use std::mem::discriminant;

use crate::PResult;
use crate::ast::{
    AlterTableOp, AngleArguments, BinaryOp, ColumnOption, CreateTable, Cte, DataType, Expr,
    ExprKind, GroupByModifier, Ident, Insert, InsertSource, Join, JoinKind, LateralView,
    ObjectName, OrderByItem, PipeOperator, Pivot, Quantifier, Query, QueryDistribution,
    SampleAmount, SampleClause, SelectItem, SelectItemKind, Span, SpecialExpr, Statement,
    StatementKind, StringLiteral, StructField, TableFactor, TableRef, TableSample, UnaryOp,
    Unpivot, WildcardOptions,
};
use crate::dialect::builtin::BuiltinStatement;
use crate::dialect::mysql::{self, MySqlExpr};
use crate::dialect::{Builtin, Dialect, ExprExtOf};
use crate::dialect::{bigquery, oracle, postgres};
use crate::display::{
    SpaceOrNewline, comma, write_columns, write_create_flags, write_if_not_exists,
};
use crate::grammar::{self, prec};
use crate::lexer::{LexResult, Lexeme, Token, TokenKind, quoted, quoted_with_backslash};
use crate::parser::{InfixAction, InfixRule, Parser, SetOperatorRule};

#[derive(Debug, Clone, PartialEq)]
pub struct Spark;

#[derive(Debug, Clone, PartialEq)]
pub enum SparkStatement<'a> {
    CreateTable(Box<SparkCreateTable<'a>>),
    Insert(Box<SparkInsert<'a>>),
    InsertDirectory(Box<SparkInsertDirectory<'a>>),
    Cache(Box<SparkCache<'a>>),
    Uncache {
        table: ObjectName<'a>,
        if_exists: bool,
    },
}

/// `CACHE [flag] TABLE name [OPTIONS(...)] [[AS] query]`
#[derive(Debug, Clone, PartialEq)]
pub struct SparkCache<'a> {
    /// The word before `TABLE`, such as `LAZY`.
    pub flag: Option<Ident<'a>>,
    pub table: ObjectName<'a>,
    pub options: Vec<SparkOption<'a>>,
    /// True if `AS` was written before the query.
    pub has_as: bool,
    pub query: Option<Query<'a, Builtin>>,
}

/// `INSERT` with syntax only Spark has. Without it, the statement is the core
/// [`Insert`].
#[derive(Debug, Clone, PartialEq)]
pub struct SparkInsert<'a> {
    pub overwrite: bool,
    pub into: bool,
    /// True if the optional `TABLE` keyword was written.
    pub table_keyword: bool,
    pub table: ObjectName<'a>,
    pub columns: Vec<Ident<'a>>,
    pub partition: Vec<Expr<'a, Builtin>>,
    /// True if the column list was written after `PARTITION (...)`.
    pub columns_after_partition: bool,
    pub source: InsertSource<'a, Builtin>,
}

/// `INSERT OVERWRITE [LOCAL] DIRECTORY 'path' [STORED AS format] query`
#[derive(Debug, Clone, PartialEq)]
pub struct SparkInsertDirectory<'a> {
    pub local: bool,
    pub path: StringLiteral<'a>,
    pub stored_as: Option<Ident<'a>>,
    pub source: Query<'a, Builtin>,
}

/// `CREATE TABLE` with clauses only Spark has. Without them, the statement is
/// the core [`CreateTable`].
#[derive(Debug, Clone, PartialEq)]
pub struct SparkCreateTable<'a> {
    pub table: CreateTable<'a, Builtin>,
    pub external: bool,
    /// In the order written.
    pub clauses: Vec<SparkTableClause<'a>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SparkTableClause<'a> {
    Using(Ident<'a>),
    Options(Vec<SparkOption<'a>>),
    PartitionedBy(Vec<PartitionColumn<'a>>),
    Location(StringLiteral<'a>),
    Comment(StringLiteral<'a>),
    TblProperties(Vec<SparkOption<'a>>),
    /// `LIKE source`
    Like(ObjectName<'a>),
}

/// `key = value` or `key value` in `OPTIONS` and `TBLPROPERTIES`.
#[derive(Debug, Clone, PartialEq)]
pub struct SparkOption<'a> {
    pub key: Expr<'a, Builtin>,
    pub equals: bool,
    pub value: Option<Expr<'a, Builtin>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PartitionColumn<'a> {
    pub name: Ident<'a>,
    pub data_type: Option<DataType<'a>>,
}

/// Reads backtick-quoted names, strings in either quote with backslash
/// escapes, and the operators the core does not have.
#[inline]
/// Lexes Spark's tokens: those of [`lex_token`], plus raw strings, `r'...'`,
/// in which a backslash is an ordinary character.
pub fn lex_spark_token(rest: &str) -> Option<LexResult> {
    if let [b'r' | b'R', b'"' | b'\'', ..] = rest.as_bytes() {
        return Some(quoted(&rest[1..], TokenKind::String).map(|l| Lexeme {
            kind: TokenKind::String,
            len: l.len + 1,
        }));
    }
    lex_token(rest)
}

/// Lexes the tokens Spark shares with Hive.
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
        [b'-', b'>', ..] | [b'|', b'>', ..] => op(2),
        [b'!', b'=', ..] => None,
        [b'!', ..] => op(1),
        // Type arguments close one at a time, as in `ARRAY<ARRAY<INT>>`.
        [b'>', b'>', ..] => op(1),
        // `:` before a name is a parameter, which the core reads.
        [b':', b':', ..] => op(2),
        [b':', next, ..] if !next.is_ascii_alphanumeric() && *next != b'_' => op(1),
        _ => None,
    }
}

/// Builds a string literal, in which a backslash starts an escape unless the
/// string is raw: `r'...'`.
pub fn string_literal(token: Token<'_>) -> StringLiteral<'_> {
    if matches!(token.text.as_bytes()[0], b'r' | b'R') {
        let text = &token.text[1..];
        return StringLiteral {
            text: Cow::Borrowed(&text[1..text.len() - 1]),
            quote: char::from(text.as_bytes()[0]),
            backslash_escapes: false,
            national: false,
            prefix: crate::ast::StringPrefix::Raw,
            triple: false,
        };
    }
    StringLiteral {
        backslash_escapes: true,
        ..grammar::string_literal(token)
    }
}

/// Parses `!`, lambdas, and otherwise the core's prefix expressions.
pub fn parse_prefix<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Expr<'a, D::Ext>> {
    if p.eat_op("!") {
        let expr = p.parse_expr_bp(prec::NOT)?;
        return Ok(ExprKind::Unary {
            op: UnaryOp::BangNot,
            expr: Box::new(expr),
        }
        .into());
    }
    if at_lambda(p) {
        return parse_lambda(p);
    }
    grammar::parse_prefix(p)
}

fn is_name(token: Token<'_>) -> bool {
    matches!(token.kind, TokenKind::Word | TokenKind::QuotedIdent)
}

/// True at `x ->` or `(x, y) ->`.
pub fn at_lambda<'a, D: Dialect>(p: &Parser<'a, D>) -> bool {
    if is_name(p.peek()) {
        return p.peek_nth(1).is_op("->");
    }
    if !p.at_op("(") {
        return false;
    }
    let mut n = 1;
    loop {
        if !is_name(p.peek_nth(n)) {
            return false;
        }
        let separator = p.peek_nth(n + 1);
        if separator.is_op(")") {
            return p.peek_nth(n + 2).is_op("->");
        }
        if !separator.is_op(",") {
            return false;
        }
        n += 2;
    }
}

pub fn parse_lambda<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Expr<'a, D::Ext>> {
    let parenthesized = p.eat_op("(");
    let params = if parenthesized {
        let params = p.parse_comma_separated(Parser::parse_ident)?;
        p.expect_op(")")?;
        params
    } else {
        vec![p.parse_ident()?]
    };
    p.expect_op("->")?;
    let body = p.parse_expr()?;
    Ok(ExprKind::Special(Box::new(SpecialExpr::Lambda {
        params,
        parenthesized,
        body,
    }))
    .into())
}

pub fn infix_rule<'a, D>(p: &Parser<'a, D>) -> Option<InfixRule<'a, D>>
where
    D: Dialect,
    ExprExtOf<'a, D>: From<MySqlExpr<'a, D::Ext>>,
{
    if p.at_keyword("DIV") {
        return Some(InfixRule::custom(prec::MUL, mysql::parse_int_div));
    }
    if p.at_op("||") {
        return Some(InfixRule::binary(prec::ADD, BinaryOp::StringConcat));
    }
    if p.at_op("::") {
        return Some(InfixRule::custom(
            prec::POSTFIX,
            postgres::parse_double_colon,
        ));
    }
    if p.at_keyword("ILIKE") {
        return Some(InfixRule::custom(prec::COMPARE, postgres::parse_ilike));
    }
    if p.at_keyword("NOT") && postgres::next_is_ilike(p) {
        return Some(InfixRule::custom(prec::COMPARE, postgres::parse_not_ilike));
    }
    grammar::infix_rule(p)
}

/// Parses an `ALTER TABLE` operation, adding `SET TBLPROPERTIES`.
pub fn parse_alter_table_op<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<AlterTableOp<'a, D::Ext>> {
    if p.at_keyword("SET") && p.peek_nth(1).is_word("TBLPROPERTIES") {
        p.advance();
        p.advance();
        p.expect_op("(")?;
        let properties = p.parse_comma_separated(|p| {
            let key = p.parse_expr_bp(grammar::prec::COMPARE)?;
            p.expect_op("=")?;
            Ok((key, p.parse_expr()?))
        })?;
        p.expect_op(")")?;
        return Ok(AlterTableOp::SetTblProperties(properties));
    }
    grammar::parse_alter_table_op(p)
}

/// Parses a select item, which can be `* EXCEPT (a, b)`.
pub fn parse_select_item<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<SelectItem<'a, D::Ext>> {
    // `(*)` is `*`.
    if p.at_op("(") && p.peek_nth(1).is_op("*") && p.peek_nth(2).is_op(")") {
        p.advance();
        p.advance();
        p.advance();
        return Ok(SelectItemKind::Wildcard.into());
    }
    let item = grammar::parse_select_item(p)?;
    let qualifier = match &item.kind {
        SelectItemKind::Wildcard => None,
        SelectItemKind::QualifiedWildcard(name) => Some(name.clone()),
        _ => return Ok(item),
    };
    let query_follows = ["SELECT", "WITH", "VALUES"]
        .iter()
        .any(|word| p.peek_nth(2).is_word(word));
    if !p.at_keyword("EXCEPT") || !p.peek_nth(1).is_op("(") || query_follows {
        return Ok(item);
    }
    p.advance();
    let exclude = grammar::parse_ident_list(p)?;
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

/// Parses a table, which can be `VALUES (...)` with no parentheses around it.
pub fn parse_table_factor<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<TableFactor<'a, D::Ext>> {
    if !(p.at_keyword("VALUES") && p.peek_nth(1).is_op("(")) {
        return grammar::parse_table_factor(p);
    }
    p.advance();
    let mut rows = vec![grammar::parse_values_row(p)?];
    // A comma before anything else starts the next table.
    while p.at_op(",") && p.peek_nth(1).is_op("(") {
        p.advance();
        rows.push(grammar::parse_values_row(p)?);
    }
    Ok(TableFactor::Values(rows))
}

/// Parses a common table expression, in which `AS` is optional.
pub fn parse_cte<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Cte<'a, D::Ext>> {
    let name = p.parse_ident()?;
    let columns = D::parse_cte_columns(p)?;
    p.eat_keyword("AS");
    p.expect_op("(")?;
    let query = p.nested(D::parse_query)?;
    p.expect_op(")")?;
    Ok(Cte {
        name,
        columns,
        materialized: None,
        query,
    })
}

/// Parses a type, which can be `ARRAY<T>`, `MAP<K, V>` or `STRUCT<a: T>`.
pub fn at_interval_with_fields<'a, D: Dialect>(p: &Parser<'a, D>) -> bool {
    let next = p.peek_nth(1);
    p.at_keyword("INTERVAL")
        && ["YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND"]
            .into_iter()
            .any(|field| next.is_word(field))
}

pub fn parse_data_type<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<DataType<'a>> {
    // Interval fields are rejected rather than leaving `DAY` in
    // `x::INTERVAL DAY` to be read as an alias.
    if at_interval_with_fields(p) {
        return Err(p.expected("an INTERVAL type without fields"));
    }
    let name = p.parse_ident()?;
    parse_type_after_name(p, name)
}

fn parse_type_after_name<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
    name: Ident<'a>,
) -> PResult<DataType<'a>> {
    let mut data_type = grammar::parse_data_type_after_name(p, name.clone())?;
    let complex = name.quote.is_none()
        && ["ARRAY", "MAP", "STRUCT"]
            .iter()
            .any(|word| name.text.eq_ignore_ascii_case(word));
    if !complex || !p.at_op("<") {
        return Ok(data_type);
    }
    p.advance();
    let arguments = if name.text.eq_ignore_ascii_case("STRUCT") {
        AngleArguments::Fields(p.parse_comma_separated(parse_struct_field)?)
    } else {
        let types = p.parse_comma_separated(|p| p.nested(D::parse_data_type))?;
        let expected = if name.text.eq_ignore_ascii_case("MAP") {
            2
        } else {
            1
        };
        if types.len() != expected {
            return Err(p.expected("the right number of type arguments"));
        }
        AngleArguments::Types(types)
    };
    p.expect_angle_close()?;
    data_type.angle = Some(Box::new(arguments));
    Ok(data_type)
}

fn parse_struct_field<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<StructField<'a>> {
    let name = p.parse_name_part()?;
    let token = p.peek();
    // With no space, the lexer reads `:INT` as a parameter.
    let (colon, data_type) = if token.kind == TokenKind::Placeholder && token.text.starts_with(':')
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
        (true, p.nested(|p| parse_type_after_name(p, type_name))?)
    } else {
        let colon = p.eat_op(":");
        (colon, p.nested(D::parse_data_type)?)
    };
    let comment = parse_optional_comment(p)?;
    Ok(StructField {
        name: Some(name),
        colon,
        data_type,
        not_null: false,
        comment,
        options: Vec::new(),
    })
}

pub use grammar::parse_string;

pub fn parse_optional_comment<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<Option<StringLiteral<'a>>> {
    if p.eat_keyword("COMMENT") {
        parse_string(p).map(Some)
    } else {
        Ok(None)
    }
}

/// Parses a column option, adding `COMMENT 'text'` to the core's.
pub fn parse_column_option<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<Option<ColumnOption<'a, D::Ext>>> {
    if let Some(comment) = parse_optional_comment(p)? {
        return Ok(Some(ColumnOption::Comment(comment)));
    }
    grammar::parse_column_option(p)
}

fn parse_option<'a, D: Dialect<Ext = Builtin>>(p: &mut Parser<'a, D>) -> PResult<SparkOption<'a>> {
    let key = p.nested(Parser::parse_prefix)?;
    let equals = p.eat_op("=");
    if !equals && p.peek().kind == TokenKind::Op && !p.at_op(",") && !p.at_op(")") {
        return Err(p.expected("an option value"));
    }
    let value = if equals || !(p.at_op(",") || p.at_op(")")) {
        Some(p.parse_expr_bp(prec::COMPARE)?)
    } else {
        None
    };
    Ok(SparkOption { key, equals, value })
}

pub fn parse_option_list<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Vec<SparkOption<'a>>> {
    p.expect_op("(")?;
    let options = p.parse_comma_separated(parse_option)?;
    p.expect_op(")")?;
    Ok(options)
}

pub fn parse_partition_column<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<PartitionColumn<'a>> {
    let name = p.parse_ident()?;
    let data_type = if p.at_op(",") || p.at_op(")") {
        None
    } else {
        Some(D::parse_data_type(p)?)
    };
    Ok(PartitionColumn { name, data_type })
}

fn parse_table_clause<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Option<SparkTableClause<'a>>> {
    if p.eat_keyword("USING") {
        return p.parse_ident().map(SparkTableClause::Using).map(Some);
    }
    if p.eat_keyword("LIKE") {
        return grammar::parse_object_name(p)
            .map(SparkTableClause::Like)
            .map(Some);
    }
    if p.eat_keyword("OPTIONS") {
        return parse_option_list(p)
            .map(SparkTableClause::Options)
            .map(Some);
    }
    if p.at_keyword("PARTITIONED") {
        p.advance();
        p.expect_keyword("BY")?;
        p.expect_op("(")?;
        let columns = p.parse_comma_separated(parse_partition_column)?;
        p.expect_op(")")?;
        return Ok(Some(SparkTableClause::PartitionedBy(columns)));
    }
    if p.eat_keyword("LOCATION") {
        return parse_string(p).map(SparkTableClause::Location).map(Some);
    }
    if let Some(comment) = parse_optional_comment(p)? {
        return Ok(Some(SparkTableClause::Comment(comment)));
    }
    if p.eat_keyword("TBLPROPERTIES") {
        return parse_option_list(p)
            .map(SparkTableClause::TblProperties)
            .map(Some);
    }
    Ok(None)
}

/// True at `CREATE [OR REPLACE] [TEMPORARY] [EXTERNAL] TABLE`.
pub fn at_create_table<'a, D: Dialect>(p: &Parser<'a, D>) -> bool {
    if !p.at_keyword("CREATE") {
        return false;
    }
    let mut n = 1;
    if p.peek_nth(n).is_word("OR") {
        n += 2;
    }
    if p.peek_nth(n).is_word("TEMPORARY") || p.peek_nth(n).is_word("TEMP") {
        n += 1;
    }
    if p.peek_nth(n).is_word("EXTERNAL") {
        n += 1;
    }
    p.peek_nth(n).is_word("TABLE")
}

/// Parses `CREATE`, handling Spark's table clauses.
pub fn parse_create<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Statement<'a, Builtin>> {
    if !at_create_table(p) {
        return grammar::parse_create(p);
    }
    p.expect_keyword("CREATE")?;
    let or_replace = grammar::parse_or_replace(p)?;
    let temporary = p.eat_keyword("TEMPORARY") || p.eat_keyword("TEMP");
    let external = p.eat_keyword("EXTERNAL");
    p.expect_keyword("TABLE")?;
    let if_not_exists = grammar::parse_if_not_exists(p)?;
    let name = grammar::parse_object_name(p)?;
    let (columns, constraints) = if p.at_op("(") {
        grammar::parse_columns(p)?
    } else {
        (Vec::new(), Vec::new())
    };
    let mut clauses: Vec<SparkTableClause<'a>> = Vec::new();
    while let Some(clause) = parse_table_clause(p)? {
        if clauses
            .iter()
            .any(|seen| discriminant(seen) == discriminant(&clause))
        {
            return Err(p.expected("each table clause only once"));
        }
        clauses.push(clause);
    }
    let query = if p.eat_keyword("AS") {
        Some(Box::new(D::parse_query(p)?))
    } else {
        None
    };
    if columns.is_empty() && query.is_none() && clauses.is_empty() {
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
    if !external && clauses.is_empty() {
        return Ok(StatementKind::CreateTable(Box::new(table)).into());
    }
    let create = SparkCreateTable {
        table,
        external,
        clauses,
    };
    Ok(
        StatementKind::Ext(BuiltinStatement::Spark(SparkStatement::CreateTable(
            Box::new(create),
        )))
        .into(),
    )
}

/// True where a word that could be a table alias starts a clause instead.
fn at_clause_after_table<'a, D: Dialect>(p: &Parser<'a, D>) -> bool {
    let next = p.peek_nth(1);
    if next.is_word("BY") {
        return p.at_keyword("SORT") || p.at_keyword("CLUSTER") || p.at_keyword("DISTRIBUTE");
    }
    (p.at_keyword("LATERAL") && next.is_word("VIEW"))
        || ((p.at_keyword("SEMI") || p.at_keyword("ANTI")) && next.is_word("JOIN"))
        || at_sample(p)
        || at_pivot(p)
        || at_unpivot(p)
}

fn at_sample<'a, D: Dialect>(p: &Parser<'a, D>) -> bool {
    let next = p.peek_nth(1);
    p.at_keyword("TABLESAMPLE")
        && (next.is_op("(")
            || ["BERNOULLI", "ROW", "SYSTEM", "BLOCK"]
                .into_iter()
                .any(|method| next.is_word(method)))
}

fn at_pivot<'a, D: Dialect>(p: &Parser<'a, D>) -> bool {
    p.at_keyword("PIVOT") && p.peek_nth(1).is_op("(")
}

fn at_unpivot<'a, D: Dialect>(p: &Parser<'a, D>) -> bool {
    let next = p.peek_nth(1);
    p.at_keyword("UNPIVOT")
        && (next.is_op("(") || next.is_word("INCLUDE") || next.is_word("EXCLUDE"))
}

fn parse_table_alias<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<Option<crate::ast::TableAlias<'a>>> {
    if at_clause_after_table(p) {
        return Ok(None);
    }
    grammar::parse_optional_table_alias(p)
}

/// Levels of tree height added for a clause that wraps the table before it,
/// whose nodes take twice the stack of an ordinary level to clone or render.
const TABLE_CLAUSE_LEVELS: usize = 2;

fn parse_sample<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
    table: TableRef<'a, D::Ext>,
) -> PResult<TableRef<'a, D::Ext>> {
    p.deepen_by(TABLE_CLAUSE_LEVELS)?;
    p.advance();
    let sample = D::parse_sample_clause(p, true)?;
    Ok(TableRef {
        factor: TableFactor::Sampled(Box::new(TableSample { table, sample })),
        alias: None,
        span: Span::default(),
    })
}

/// Parses what follows `TABLESAMPLE`: an amount, or a `BUCKET` in parentheses.
pub fn parse_sample_clause<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
    long_form: bool,
) -> PResult<SampleClause<'a, D::Ext>> {
    if !p.at_op("(") {
        return Err(p.expected("( after TABLESAMPLE"));
    }
    if !p.peek_nth(1).is_word("BUCKET") {
        return grammar::parse_sample_clause(p, long_form);
    }
    p.advance();
    p.advance();
    let bucket = grammar::parse_number(p)?;
    p.expect_keyword("OUT")?;
    p.expect_keyword("OF")?;
    let total = grammar::parse_number(p)?;
    let on = if p.eat_keyword("ON") {
        Some(Box::new(p.parse_expr()?))
    } else {
        None
    };
    p.expect_op(")")?;
    Ok(SampleClause {
        long_form,
        method: None,
        amount: SampleAmount::Bucket {
            bucket: bucket.into(),
            total: total.into(),
            on,
        },
        parenthesized: true,
        seed: None,
        offset: None,
    })
}

/// Parses a table with any sample, alias, `PIVOT` and `UNPIVOT`. A sample can
/// be written before or after the alias.
pub fn parse_table_ref<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<TableRef<'a, D::Ext>> {
    let mut table = TableRef {
        factor: D::parse_table_factor(p)?,
        alias: None,
        span: Span::default(),
    };
    if at_sample(p) {
        table = parse_sample(p, table)?;
    }
    table.alias = parse_table_alias(p)?;
    loop {
        let factor = if at_sample(p) {
            table = parse_sample(p, table)?;
            continue;
        } else if at_pivot(p) {
            p.deepen_by(TABLE_CLAUSE_LEVELS)?;
            p.advance();
            let clause = grammar::parse_pivot_head(p)?;
            p.expect_op(")")?;
            TableFactor::Pivot(Box::new(Pivot { table, clause }))
        } else if at_unpivot(p) {
            p.deepen_by(TABLE_CLAUSE_LEVELS)?;
            p.advance();
            let clause = grammar::parse_unpivot_clause(p)?;
            TableFactor::Unpivot(Box::new(Unpivot { table, clause }))
        } else {
            return Ok(table);
        };
        let alias = parse_table_alias(p)?;
        table = TableRef {
            factor,
            alias,
            span: Span::default(),
        };
    }
}

/// Consumes the words of a semi or anti join, if one starts here.
fn eat_semi_or_anti<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Option<JoinKind<'a>>> {
    let next = p.peek_nth(1);
    let (kind, words) = if p.at_keyword("LEFT") && next.is_word("SEMI") {
        (JoinKind::LeftSemi, 2)
    } else if p.at_keyword("LEFT") && next.is_word("ANTI") {
        (JoinKind::LeftAnti, 2)
    } else if p.at_keyword("SEMI") && next.is_word("JOIN") {
        (JoinKind::Semi, 1)
    } else if p.at_keyword("ANTI") && next.is_word("JOIN") {
        (JoinKind::Anti, 1)
    } else {
        return Ok(None);
    };
    for _ in 0..words {
        p.advance();
    }
    p.expect_keyword("JOIN")?;
    Ok(Some(kind))
}

/// Parses a join, which can be a semi or anti join.
pub fn parse_join<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Option<Join<'a, D::Ext>>> {
    let Some(kind) = eat_semi_or_anti(p)? else {
        return grammar::parse_join(p);
    };
    let table = p.parse_table_ref()?;
    let constraint = grammar::parse_join_constraint(p)?;
    Ok(Some(Join {
        kind,
        spelled_out: false,
        table,
        constraint,
    }))
}

/// Finishes a select item, which can name several columns: `f(a) AS (b, c)`.
pub fn parse_select_item_alias<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
    expr: Expr<'a, D::Ext>,
) -> PResult<SelectItem<'a, D::Ext>> {
    if p.at_keyword("AS") && p.peek_nth(1).is_op("(") {
        p.advance();
        let aliases = grammar::parse_ident_list(p)?;
        return Ok(SelectItemKind::ExprWithAliases { expr, aliases }.into());
    }
    grammar::parse_select_item_alias(p, expr)
}

/// Parses `LATERAL VIEW` clauses.
pub fn parse_lateral_views<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<Vec<LateralView<'a, D::Ext>>> {
    let mut views = Vec::new();
    while p.at_keyword("LATERAL") && p.peek_nth(1).is_word("VIEW") {
        p.advance();
        p.advance();
        let outer = p.eat_keyword("OUTER");
        let function = p.parse_expr()?;
        let name = grammar::parse_object_name(p)?;
        let mut columns = Vec::new();
        if let Some(column) = grammar::parse_optional_alias(p)? {
            columns.push(column);
            while p.eat_op(",") {
                columns.push(p.parse_ident()?);
            }
        }
        views.push(LateralView {
            outer,
            function,
            name,
            columns,
        });
    }
    Ok(views)
}

/// Parses `[CLUSTER BY ...] [DISTRIBUTE BY ...] [SORT BY ...]`.
pub fn parse_query_distribution<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<Option<Box<QueryDistribution<'a, D::Ext>>>> {
    let cluster_by = parse_by_list(p, "CLUSTER", Parser::parse_expr)?;
    let distribute_by = parse_by_list(p, "DISTRIBUTE", Parser::parse_expr)?;
    let sort_by: Vec<OrderByItem<'a, D::Ext>> = parse_by_list(p, "SORT", D::parse_order_by_item)?;
    if cluster_by.is_empty() && distribute_by.is_empty() && sort_by.is_empty() {
        return Ok(None);
    }
    Ok(Some(Box::new(QueryDistribution {
        cluster_by,
        distribute_by,
        sort_by,
    })))
}

fn parse_by_list<'a, D: Dialect, T>(
    p: &mut Parser<'a, D>,
    keyword: &str,
    item: impl FnMut(&mut Parser<'a, D>) -> PResult<T>,
) -> PResult<Vec<T>> {
    if !p.eat_keyword(keyword) {
        return Ok(Vec::new());
    }
    p.expect_keyword("BY")?;
    p.parse_comma_separated(item)
}

/// Parses a bound of `BETWEEN`, which can be one comparison.
pub fn parse_between_bound<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
    bound: Expr<'a, D::Ext>,
) -> PResult<Expr<'a, D::Ext>> {
    if !["=", "<>", "!=", "<", "<=", ">", ">="]
        .into_iter()
        .any(|op| p.at_op(op))
    {
        return Ok(bound);
    }
    let Some(rule) = D::infix_rule(p) else {
        return Ok(bound);
    };
    match rule.action {
        InfixAction::Binary(op) => {
            p.advance();
            grammar::parse_binary_right(p, bound, op, rule.bp)
        }
        InfixAction::Custom(parse) => parse(p, bound, rule.bp),
    }
}

/// True at `[LOCAL] DIRECTORY 'path'`.
fn at_directory<'a, D: Dialect>(p: &Parser<'a, D>) -> bool {
    let (word, path) = if p.at_keyword("LOCAL") {
        (p.peek_nth(1), p.peek_nth(2))
    } else {
        (p.peek(), p.peek_nth(1))
    };
    word.is_word("DIRECTORY") && path.kind == TokenKind::String
}

fn parse_insert_directory<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Statement<'a, Builtin>> {
    let local = p.eat_keyword("LOCAL");
    p.expect_keyword("DIRECTORY")?;
    let path = D::string_literal(p.peek());
    p.advance();
    let stored_as = if p.eat_keyword("STORED") {
        p.expect_keyword("AS")?;
        Some(p.parse_ident()?)
    } else {
        None
    };
    let source = D::parse_query(p)?;
    Ok(
        StatementKind::Ext(BuiltinStatement::Spark(SparkStatement::InsertDirectory(
            Box::new(SparkInsertDirectory {
                local,
                path,
                stored_as,
                source,
            }),
        )))
        .into(),
    )
}

/// Parses `INSERT`, producing the core statement unless Spark's syntax is used.
pub fn parse_insert<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Statement<'a, Builtin>> {
    p.expect_keyword("INSERT")?;
    let modifiers = D::parse_insert_modifiers(p)?;
    let overwrite = p.eat_keyword("OVERWRITE");
    if overwrite && at_directory(p) {
        return parse_insert_directory(p);
    }
    let into = p.eat_keyword("INTO");
    if !overwrite && !into {
        return Err(p.expected("INTO or OVERWRITE"));
    }
    let table_keyword = p.eat_keyword("TABLE");
    let table = grammar::parse_object_name(p)?;
    let mut columns = if grammar::at_column_list(p) {
        grammar::parse_ident_list(p)?
    } else {
        Vec::new()
    };
    let partition = if p.eat_keyword("PARTITION") {
        p.expect_op("(")?;
        let partition = p.parse_comma_separated(Parser::parse_expr)?;
        p.expect_op(")")?;
        partition
    } else {
        Vec::new()
    };
    let columns_after_partition = columns.is_empty() && grammar::at_column_list(p);
    if columns_after_partition {
        columns = grammar::parse_ident_list(p)?;
    }
    let source = grammar::parse_insert_source(p)?;
    if !overwrite && !table_keyword && partition.is_empty() {
        let returning = D::parse_returning(p)?;
        return Ok(StatementKind::Insert(Box::new(Insert {
            modifiers,
            into: true,
            table,
            columns,
            output: None,
            source,
            returning,
        }))
        .into());
    }
    Ok(
        StatementKind::Ext(BuiltinStatement::Spark(SparkStatement::Insert(Box::new(
            SparkInsert {
                overwrite,
                into,
                table_keyword,
                table,
                columns,
                partition,
                columns_after_partition,
                source,
            },
        ))))
        .into(),
    )
}

/// Parses a table name, which here can also be written as a string.
fn parse_cache_table_name<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<ObjectName<'a>> {
    let token = p.peek();
    if token.kind != TokenKind::String {
        return grammar::parse_object_name(p);
    }
    p.advance();
    Ok(ObjectName {
        parts: vec![Ident {
            text: Cow::Borrowed(&token.text[1..token.text.len() - 1]),
            quote: token.text.chars().next(),
            span: token.span,
        }],
    })
}

fn at_query<'a, D: Dialect>(p: &Parser<'a, D>) -> bool {
    p.at_op("(") || p.at_keyword("SELECT") || p.at_keyword("WITH") || p.at_keyword("VALUES")
}

fn parse_cache<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Statement<'a, Builtin>> {
    p.expect_keyword("CACHE")?;
    let flag = if p.at_keyword("TABLE") {
        None
    } else {
        Some(p.parse_ident()?)
    };
    p.expect_keyword("TABLE")?;
    let table = parse_cache_table_name(p)?;
    let options = if p.eat_keyword("OPTIONS") {
        parse_option_list(p)?
    } else {
        Vec::new()
    };
    let has_as = p.eat_keyword("AS");
    let query = if has_as || at_query(p) {
        Some(D::parse_query(p)?)
    } else {
        None
    };
    Ok(
        StatementKind::Ext(BuiltinStatement::Spark(SparkStatement::Cache(Box::new(
            SparkCache {
                flag,
                table,
                options,
                has_as,
                query,
            },
        ))))
        .into(),
    )
}

fn parse_uncache<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Statement<'a, Builtin>> {
    p.expect_keyword("UNCACHE")?;
    p.expect_keyword("TABLE")?;
    let if_exists = p.eat_keyword("IF");
    if if_exists {
        p.expect_keyword("EXISTS")?;
    }
    let table = parse_cache_table_name(p)?;
    Ok(
        StatementKind::Ext(BuiltinStatement::Spark(SparkStatement::Uncache {
            table,
            if_exists,
        }))
        .into(),
    )
}

pub fn parse_statement<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Statement<'a, Builtin>> {
    if grammar::at_alter_view(p) {
        return grammar::parse_alter_view(p, false)
            .map(|a| StatementKind::AlterView(Box::new(a)).into());
    }
    if p.at_keyword("CACHE") {
        return parse_cache(p);
    }
    if p.at_keyword("UNCACHE") {
        return parse_uncache(p);
    }
    grammar::parse_statement(p)
}

impl fmt::Display for SparkInsert<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("INSERT")?;
        if self.overwrite {
            f.write_str(" OVERWRITE")?;
        }
        if self.into {
            f.write_str(" INTO")?;
        }
        if self.table_keyword {
            f.write_str(" TABLE")?;
        }
        write!(f, " {}", self.table)?;
        if !self.columns_after_partition && !self.columns.is_empty() {
            write!(f, " ({})", comma(&self.columns))?;
        }
        if !self.partition.is_empty() {
            write!(f, " PARTITION ({})", comma(&self.partition))?;
        }
        if self.columns_after_partition && !self.columns.is_empty() {
            SpaceOrNewline.fmt(f)?;
            write!(f, "({})", comma(&self.columns))?;
        }
        let on_new_line = !self.columns.is_empty() || !self.partition.is_empty();
        crate::display::write_insert_source(f, &self.source, on_new_line)
    }
}

impl fmt::Display for SparkCache<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CACHE ")?;
        if let Some(flag) = &self.flag {
            write!(f, "{flag} ")?;
        }
        write!(f, "TABLE {}", self.table)?;
        if !self.options.is_empty() {
            write!(f, " OPTIONS({})", comma(&self.options))?;
        }
        if self.has_as {
            f.write_str(" AS")?;
        }
        match &self.query {
            Some(query) => write!(f, " {query}"),
            None => Ok(()),
        }
    }
}

impl fmt::Display for SparkInsertDirectory<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("INSERT OVERWRITE")?;
        if self.local {
            f.write_str(" LOCAL")?;
        }
        write!(f, " DIRECTORY {}", self.path)?;
        if let Some(format) = &self.stored_as {
            write!(f, " STORED AS {format}")?;
        }
        write!(f, " {}", self.source)
    }
}

impl Dialect for Spark {
    type Ext = Builtin;

    fn parse_alter_table_op<'a>(p: &mut Parser<'a, Self>) -> PResult<AlterTableOp<'a, Builtin>> {
        parse_alter_table_op(p)
    }

    fn parse_like_quantifier<'a>(p: &mut Parser<'a, Self>) -> PResult<Option<Quantifier>> {
        Ok(grammar::parse_like_any(p))
    }

    fn eat_limit_all<'a>(p: &mut Parser<'a, Self>) -> bool {
        p.eat_keyword("ALL")
    }

    fn set_operator_rule<'a>(p: &Parser<'a, Self>) -> Option<SetOperatorRule> {
        grammar::set_operator_rule_with_minus(p)
    }

    #[inline]
    fn is_reserved_for_alias(word: &str) -> bool {
        word.eq_ignore_ascii_case("MINUS") || grammar::is_reserved_for_alias(word)
    }

    fn parse_group_by_suffix<'a>(
        p: &mut Parser<'a, Self>,
    ) -> PResult<Option<Box<Expr<'a, Builtin>>>> {
        grammar::parse_group_by_grouping_sets(p)
    }

    fn parse_group_by_item<'a>(p: &mut Parser<'a, Self>) -> PResult<Expr<'a, Builtin>> {
        grammar::parse_grouping_item_or_empty_set(p)
    }

    fn parse_statement_after_with<'a>(
        p: &mut Parser<'a, Self>,
    ) -> PResult<Option<Statement<'a, Builtin>>> {
        grammar::parse_dml_after_with(p, true, false)
    }

    #[inline]
    fn is_reserved(word: &str) -> bool {
        !word.eq_ignore_ascii_case("ALL") && grammar::is_reserved(word)
    }

    fn parse_statement<'a>(p: &mut Parser<'a, Self>) -> PResult<Statement<'a, Builtin>> {
        if p.at_keyword("EXECUTE") && p.peek_nth(1).is_word("IMMEDIATE") {
            let execute = oracle::parse_execute_immediate(p)?;
            return Ok(StatementKind::Ext(BuiltinStatement::Oracle(execute)).into());
        }
        parse_statement(p)
    }

    fn parse_table_ref<'a>(p: &mut Parser<'a, Self>) -> PResult<TableRef<'a, Builtin>> {
        parse_table_ref(p)
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

    fn parse_lateral_views<'a>(p: &mut Parser<'a, Self>) -> PResult<Vec<LateralView<'a, Builtin>>> {
        parse_lateral_views(p)
    }

    fn parse_query_distribution<'a>(
        p: &mut Parser<'a, Self>,
    ) -> PResult<Option<Box<QueryDistribution<'a, Builtin>>>> {
        parse_query_distribution(p)
    }

    fn parse_insert_statement<'a>(p: &mut Parser<'a, Self>) -> PResult<Statement<'a, Builtin>> {
        parse_insert(p)
    }

    fn parse_sample_clause<'a>(
        p: &mut Parser<'a, Self>,
        long_form: bool,
    ) -> PResult<SampleClause<'a, Builtin>> {
        parse_sample_clause(p, long_form)
    }

    fn parse_between_low<'a>(p: &mut Parser<'a, Self>) -> PResult<Expr<'a, Builtin>> {
        let low = grammar::parse_between_low(p)?;
        parse_between_bound(p, low)
    }

    fn parse_between_high<'a>(p: &mut Parser<'a, Self>) -> PResult<Expr<'a, Builtin>> {
        let high = grammar::parse_between_high(p)?;
        parse_between_bound(p, high)
    }

    #[inline]
    fn lex_token(rest: &str) -> Option<LexResult> {
        lex_spark_token(rest)
    }

    #[inline]
    fn string_literal(token: Token<'_>) -> StringLiteral<'_> {
        string_literal(token)
    }

    fn parse_prefix<'a>(p: &mut Parser<'a, Self>) -> PResult<Expr<'a, Builtin>> {
        // `STRUCT(value AS name, ...)`, which BigQuery writes the same way.
        if p.at_keyword("STRUCT") && p.peek_nth(1).is_op("(") {
            return bigquery::parse_struct(p);
        }
        parse_prefix(p)
    }

    fn infix_rule<'a>(p: &Parser<'a, Self>) -> Option<InfixRule<'a, Self>> {
        infix_rule(p)
    }

    fn parse_select_item<'a>(p: &mut Parser<'a, Self>) -> PResult<SelectItem<'a, Builtin>> {
        parse_select_item(p)
    }

    fn parse_table_factor<'a>(p: &mut Parser<'a, Self>) -> PResult<TableFactor<'a, Builtin>> {
        parse_table_factor(p)
    }

    fn parse_cte<'a>(p: &mut Parser<'a, Self>) -> PResult<Cte<'a, Builtin>> {
        parse_cte(p)
    }

    fn parse_pipe_operators<'a>(
        p: &mut Parser<'a, Self>,
    ) -> PResult<Vec<PipeOperator<'a, Builtin>>> {
        grammar::parse_pipe_operators(p)
    }

    fn eat_group_by_all<'a>(p: &mut Parser<'a, Self>) -> bool {
        p.eat_keyword("ALL")
    }

    fn parse_group_by_modifier<'a>(p: &mut Parser<'a, Self>) -> Option<GroupByModifier> {
        grammar::parse_group_by_with(p, true)
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
}

impl fmt::Display for SparkOption<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.key.fmt(f)?;
        match &self.value {
            Some(value) if self.equals => write!(f, " = {value}"),
            Some(value) => write!(f, " {value}"),
            None => Ok(()),
        }
    }
}

impl fmt::Display for PartitionColumn<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.name.fmt(f)?;
        match &self.data_type {
            Some(data_type) => write!(f, " {data_type}"),
            None => Ok(()),
        }
    }
}

impl fmt::Display for SparkTableClause<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Using(format) => write!(f, "USING {format}"),
            Self::Options(options) => write!(f, "OPTIONS ({})", comma(options)),
            Self::PartitionedBy(columns) => write!(f, "PARTITIONED BY ({})", comma(columns)),
            Self::Location(location) => write!(f, "LOCATION {}", location),
            Self::Comment(comment) => write!(f, "COMMENT {}", comment),
            Self::TblProperties(options) => write!(f, "TBLPROPERTIES ({})", comma(options)),
            Self::Like(source) => write!(f, "LIKE {source}"),
        }
    }
}

impl fmt::Display for SparkCreateTable<'_> {
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
        if !table.columns.is_empty() || !table.constraints.is_empty() {
            write_columns(f, &table.columns, &table.constraints)?;
        }
        for clause in &self.clauses {
            write!(f, " {clause}")?;
        }
        if let Some(query) = &table.query {
            write!(f, " AS {query}")?;
        }
        Ok(())
    }
}

impl fmt::Display for SparkStatement<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CreateTable(create) => create.fmt(f),
            Self::Insert(insert) => insert.fmt(f),
            Self::InsertDirectory(insert) => insert.fmt(f),
            Self::Cache(cache) => cache.fmt(f),
            Self::Uncache { table, if_exists } => {
                let if_exists = if *if_exists { "IF EXISTS " } else { "" };
                write!(f, "UNCACHE TABLE {if_exists}{table}")
            }
        }
    }
}

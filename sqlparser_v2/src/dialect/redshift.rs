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

//! Amazon Redshift, built on the PostgreSQL rules.

use crate::ast::Span;
use std::borrow::Cow;
use std::fmt;

use crate::PResult;
use crate::ast::{
    AlterTableOp, BinaryOp, CreateFlags, CreateSchema, CreateTable, CreateView, Expr, ExprKind,
    Ident, InsertSource, NullTreatment, ObjectName, SelectInto, SelectItem, Statement,
    StatementKind, StringLiteral, TableFactor, TableRef, TableWithJoins,
};
use crate::dialect::builtin::{BuiltinExpr, BuiltinSelect, BuiltinStatement, BuiltinTableFactor};
use crate::dialect::postgres::{self, CopyLegacyKind};
use crate::dialect::{Builtin, Dialect};
use crate::display::{comma, write_create_flags};
use crate::grammar::{self, prec};
use crate::lexer::{
    LexResult, Lexeme, Token, TokenKind, block_comment, lex_core_comment, quoted_with_backslash,
};
use crate::parser::{InfixRule, Parser, SetOperatorRule};

#[derive(Debug, Clone, PartialEq)]
pub struct Redshift;

/// Statements only Redshift has.
#[derive(Debug, Clone, PartialEq)]
pub enum RedshiftStatement<'a> {
    CreateTable(Box<RedshiftCreateTable<'a>>),
    Vacuum(Box<Vacuum<'a>>),
}

/// A `CREATE TABLE` with Redshift's table attributes. Without them, the
/// statement is the core [`CreateTable`].
#[derive(Debug, Clone, PartialEq)]
pub struct RedshiftCreateTable<'a> {
    pub table: CreateTable<'a, Builtin>,
    /// `Some(true)` for `BACKUP YES`, `Some(false)` for `BACKUP NO`.
    pub backup: Option<bool>,
    /// `AUTO`, `EVEN`, `KEY` or `ALL`.
    pub diststyle: Option<Cow<'a, str>>,
    pub distkey: Option<Expr<'a, Builtin>>,
    pub sortkey: Vec<Expr<'a, Builtin>>,
}

/// `VACUUM [options] [table [TO n PERCENT] [BOOST]]`
#[derive(Debug, Clone, PartialEq)]
pub struct Vacuum<'a> {
    pub full: bool,
    pub sort_only: bool,
    pub delete_only: bool,
    pub reindex: bool,
    pub recluster: bool,
    pub table: Option<ObjectName<'a>>,
    /// The number in `TO n PERCENT`.
    pub threshold: Option<Cow<'a, str>>,
    pub boost: bool,
}

/// Expressions only Redshift has.
#[derive(Debug, Clone, PartialEq)]
pub enum RedshiftExpr<'a> {
    /// `APPROXIMATE PERCENTILE_DISC(...) ...`
    Approximate(Box<Expr<'a, Builtin>>),
}

/// PartiQL table references.
#[derive(Debug, Clone, PartialEq)]
pub enum RedshiftTableFactor<'a> {
    /// A path into a `SUPER` value: `src[0].a`.
    Path(Box<Expr<'a, Builtin>>),
    /// A table reference with the index alias after `AT`.
    At {
        table: Box<TableRef<'a, Builtin>>,
        at: Ident<'a>,
    },
    /// `UNPIVOT expr [AS alias] [AT name]`
    Unpivot {
        expr: Box<Expr<'a, Builtin>>,
        alias: Option<Ident<'a>>,
        at: Option<Ident<'a>>,
    },
}

fn statement<'a>(s: RedshiftStatement<'a>) -> Statement<'a, Builtin> {
    StatementKind::Ext(BuiltinStatement::Redshift(s)).into()
}

/// The legacy `COPY` options Redshift has.
const REDSHIFT_COPY_OPTIONS: &[(&str, CopyLegacyKind)] = &[
    ("ACCEPTANYDATE", CopyLegacyKind::Flag),
    ("ACCEPTINVCHARS", CopyLegacyKind::OptionalString),
    ("ADDQUOTES", CopyLegacyKind::Flag),
    ("ALLOWOVERWRITE", CopyLegacyKind::Flag),
    ("BINARY", CopyLegacyKind::Flag),
    ("BLANKSASNULL", CopyLegacyKind::Flag),
    ("BZIP2", CopyLegacyKind::Flag),
    ("CLEANPATH", CopyLegacyKind::Flag),
    ("COMPUPDATE", CopyLegacyKind::Toggle),
    ("CREDENTIALS", CopyLegacyKind::String),
    ("CSV", CopyLegacyKind::Flag),
    ("DATEFORMAT", CopyLegacyKind::OptionalString),
    ("DELIMITER", CopyLegacyKind::String),
    ("EMPTYASNULL", CopyLegacyKind::Flag),
    ("ENCRYPTED", CopyLegacyKind::Flag),
    ("ESCAPE", CopyLegacyKind::Flag),
    ("EXTENSION", CopyLegacyKind::String),
    ("FIXEDWIDTH", CopyLegacyKind::String),
    ("GZIP", CopyLegacyKind::Flag),
    ("HEADER", CopyLegacyKind::Flag),
    ("IAM_ROLE", CopyLegacyKind::StringOrDefault),
    ("IGNOREHEADER", CopyLegacyKind::Number),
    ("JSON", CopyLegacyKind::OptionalString),
    ("MANIFEST", CopyLegacyKind::Flag),
    ("MAXFILESIZE", CopyLegacyKind::Number),
    ("NULL", CopyLegacyKind::String),
    ("PARALLEL", CopyLegacyKind::Toggle),
    ("PARQUET", CopyLegacyKind::Flag),
    ("REGION", CopyLegacyKind::String),
    ("REMOVEQUOTES", CopyLegacyKind::Flag),
    ("ROWGROUPSIZE", CopyLegacyKind::Number),
    ("STATUPDATE", CopyLegacyKind::Toggle),
    ("TIMEFORMAT", CopyLegacyKind::OptionalString),
    ("TRUNCATECOLUMNS", CopyLegacyKind::Flag),
    ("ZSTD", CopyLegacyKind::Flag),
];

/// Parses Redshift's table attributes, fixed in this order as in the original.
struct Attributes<'a> {
    backup: Option<bool>,
    diststyle: Option<&'static str>,
    distkey: Option<Expr<'a, Builtin>>,
    sortkey: Vec<Expr<'a, Builtin>>,
}

impl Attributes<'_> {
    fn is_empty(&self) -> bool {
        self.backup.is_none()
            && self.diststyle.is_none()
            && self.distkey.is_none()
            && self.sortkey.is_empty()
    }
}

fn parse_table_attributes<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Attributes<'a>> {
    let backup = if p.eat_keyword("BACKUP") {
        if p.eat_keyword("YES") {
            Some(true)
        } else {
            p.expect_keyword("NO")?;
            Some(false)
        }
    } else {
        None
    };
    let diststyle = if p.eat_keyword("DISTSTYLE") {
        let style = ["AUTO", "EVEN", "KEY", "ALL"]
            .into_iter()
            .find(|word| p.eat_keyword(word));
        Some(style.ok_or_else(|| p.expected("AUTO, EVEN, KEY or ALL"))?)
    } else {
        None
    };
    let distkey = if p.eat_keyword("DISTKEY") {
        p.expect_op("(")?;
        let expr = p.parse_expr()?;
        p.expect_op(")")?;
        Some(expr)
    } else {
        None
    };
    let sortkey = if p.eat_keyword("SORTKEY") {
        p.expect_op("(")?;
        let exprs = p.parse_comma_separated(Parser::parse_expr)?;
        p.expect_op(")")?;
        exprs
    } else {
        Vec::new()
    };
    Ok(Attributes {
        backup,
        diststyle,
        distkey,
        sortkey,
    })
}

/// Parses `CREATE [OR REPLACE] [TEMP] TABLE` with table attributes.
fn parse_create_table_statement<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
    flags: CreateFlags,
) -> PResult<Statement<'a, Builtin>> {
    let if_not_exists = grammar::parse_if_not_exists(p)?;
    let name = grammar::parse_object_name(p)?;
    let (columns, constraints) = if p.at_op("(") {
        grammar::parse_columns(p)?
    } else {
        (Vec::new(), Vec::new())
    };
    let attributes = parse_table_attributes(p)?;
    let query = if p.eat_keyword("AS") {
        Some(Box::new(D::parse_query(p)?))
    } else {
        None
    };
    if columns.is_empty() && query.is_none() {
        return Err(p.expected("column definitions or AS"));
    }
    let table = CreateTable {
        or_replace: flags.or_replace,
        temporary: flags.temporary,
        name,
        if_not_exists,
        columns,
        constraints,
        query,
    };
    if attributes.is_empty() {
        return Ok(StatementKind::CreateTable(Box::new(table)).into());
    }
    let Attributes {
        backup,
        diststyle,
        distkey,
        sortkey,
    } = attributes;
    Ok(statement(RedshiftStatement::CreateTable(Box::new(
        RedshiftCreateTable {
            table,
            backup,
            diststyle: diststyle.map(Cow::Borrowed),
            distkey,
            sortkey,
        },
    ))))
}

fn parse_vacuum<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Statement<'a, Builtin>> {
    p.advance();
    let full = p.eat_keyword("FULL");
    let sort_only = p.at_keyword("SORT") && p.peek_nth(1).is_word("ONLY");
    if sort_only {
        p.advance();
        p.advance();
    }
    let delete_only = p.at_keyword("DELETE") && p.peek_nth(1).is_word("ONLY");
    if delete_only {
        p.advance();
        p.advance();
    }
    let reindex = p.eat_keyword("REINDEX");
    let recluster = p.eat_keyword("RECLUSTER");
    let (table, threshold, boost) = if p.at_ident() {
        let table = grammar::parse_object_name(p)?;
        let threshold = if p.eat_keyword("TO") {
            let number = grammar::parse_number_text(p)?;
            p.expect_keyword("PERCENT")?;
            Some(number)
        } else {
            None
        };
        let boost = p.eat_keyword("BOOST");
        (Some(table), threshold, boost)
    } else {
        (None, None, false)
    };
    Ok(statement(RedshiftStatement::Vacuum(Box::new(Vacuum {
        full,
        sort_only,
        delete_only,
        reindex,
        recluster,
        table,
        threshold: threshold.map(Cow::Borrowed),
        boost,
    }))))
}

/// Lexes a word starting with `#`, or a bracketed identifier. In Redshift
/// `[a]` delimits a name; only directly after a value, as `a[0]`, is it a
/// subscript, which [`Redshift::lex_after`] arranges.
fn lex_bracketed_name(rest: &str) -> Option<LexResult> {
    let bytes = rest.as_bytes();
    let first = bytes.iter().skip(1).find(|b| !b.is_ascii_whitespace())?;
    if *first != b'"' && !(first.is_ascii_alphabetic() || *first == b'_') {
        return None;
    }
    let mut i = 1;
    loop {
        match bytes.get(i)? {
            b'"' => {
                i += 1 + bytes[i + 1..].iter().position(|&b| b == b'"')? + 1;
            }
            b']' => {
                return Some(Ok(Lexeme {
                    kind: TokenKind::QuotedIdent,
                    len: i + 1,
                }));
            }
            _ => i += 1,
        }
    }
}

/// True if `word` can start a `SUPER` path in `FROM`, after which `[` follows.
fn at_super_path<D: Dialect>(p: &Parser<'_, D>) -> bool {
    let mut n = 0;
    while matches!(p.peek_nth(n).kind, TokenKind::Word | TokenKind::QuotedIdent)
        && p.peek_nth(n + 1).is_op(".")
    {
        n += 2;
    }
    matches!(p.peek_nth(n).kind, TokenKind::Word | TokenKind::QuotedIdent)
        && p.peek_nth(n + 1).is_op("[")
}

/// Parses a table reference, adding PartiQL paths, `AT` and `UNPIVOT`.
pub fn parse_table_ref<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<TableRef<'a, Builtin>> {
    if p.at_keyword("UNPIVOT") && !p.peek_nth(1).is_op("(") {
        p.advance();
        let expr = Box::new(p.parse_expr_bp(prec::COMPARE)?);
        let alias = if p.eat_keyword("AS") {
            Some(p.parse_ident()?)
        } else {
            None
        };
        let at = if p.eat_keyword("AT") {
            Some(p.parse_ident()?)
        } else {
            None
        };
        return Ok(TableRef {
            factor: TableFactor::Ext(BuiltinTableFactor::Redshift(Box::new(
                RedshiftTableFactor::Unpivot { expr, alias, at },
            ))),
            alias: None,
            span: Span::default(),
        });
    }
    let factor = if at_super_path(p) {
        let expr = Box::new(p.parse_expr_bp(prec::COMPARE)?);
        TableFactor::Ext(BuiltinTableFactor::Redshift(Box::new(
            RedshiftTableFactor::Path(expr),
        )))
    } else {
        D::parse_table_factor(p)?
    };
    let alias = grammar::parse_optional_table_alias(p)?;
    let table = TableRef {
        factor,
        alias,
        span: Span::default(),
    };
    if p.at_keyword("AT") && p.peek_nth(1).kind != TokenKind::Eof {
        p.advance();
        let at = p.parse_ident()?;
        return Ok(TableRef {
            factor: TableFactor::Ext(BuiltinTableFactor::Redshift(Box::new(
                RedshiftTableFactor::At {
                    table: Box::new(table),
                    at,
                },
            ))),
            alias: None,
            span: Span::default(),
        });
    }
    Ok(table)
}

/// `APPROXIMATE` before an aggregate's name, as in `APPROXIMATE
/// PERCENTILE_DISC(...)`.
fn parse_prefix<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Expr<'a, Builtin>> {
    let next = p.peek_nth(1);
    if p.at_keyword("APPROXIMATE")
        && matches!(next.kind, TokenKind::Word | TokenKind::QuotedIdent)
        && p.peek_nth(2).is_op("(")
    {
        p.advance();
        let call = p.parse_expr_bp(prec::POSTFIX)?;
        return Ok(
            ExprKind::Ext(BuiltinExpr::Redshift(RedshiftExpr::Approximate(Box::new(
                call,
            ))))
            .into(),
        );
    }
    grammar::parse_prefix(p)
}

impl fmt::Display for RedshiftStatement<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CreateTable(create) => {
                let table = &create.table;
                write_create_flags(f, table.or_replace, table.temporary)?;
                f.write_str("TABLE ")?;
                crate::display::write_if_not_exists(f, table.if_not_exists)?;
                table.name.fmt(f)?;
                if !(table.columns.is_empty() && table.constraints.is_empty()) {
                    crate::display::write_columns(f, &table.columns, &table.constraints)?;
                }
                match create.backup {
                    Some(true) => f.write_str(" BACKUP YES")?,
                    Some(false) => f.write_str(" BACKUP NO")?,
                    None => {}
                }
                if let Some(ref style) = create.diststyle {
                    write!(f, " DISTSTYLE {style}")?;
                }
                if let Some(key) = &create.distkey {
                    write!(f, " DISTKEY({key})")?;
                }
                if !create.sortkey.is_empty() {
                    write!(f, " SORTKEY({})", comma(&create.sortkey))?;
                }
                if let Some(query) = &table.query {
                    write!(f, " AS {query}")?;
                }
                Ok(())
            }
            Self::Vacuum(vacuum) => {
                f.write_str("VACUUM")?;
                let flags = [
                    (vacuum.full, " FULL"),
                    (vacuum.sort_only, " SORT ONLY"),
                    (vacuum.delete_only, " DELETE ONLY"),
                    (vacuum.reindex, " REINDEX"),
                    (vacuum.recluster, " RECLUSTER"),
                ];
                for (set, keyword) in flags {
                    if set {
                        f.write_str(keyword)?;
                    }
                }
                if let Some(table) = &vacuum.table {
                    write!(f, " {table}")?;
                }
                if let Some(ref threshold) = vacuum.threshold {
                    write!(f, " TO {threshold} PERCENT")?;
                }
                if vacuum.boost {
                    f.write_str(" BOOST")?;
                }
                Ok(())
            }
        }
    }
}

impl fmt::Display for RedshiftExpr<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Approximate(call) => write!(f, "APPROXIMATE {call}"),
        }
    }
}

impl fmt::Display for RedshiftTableFactor<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Path(expr) => expr.fmt(f),
            Self::At { table, at } => write!(f, "{table} AT {at}"),
            Self::Unpivot { expr, alias, at } => {
                write!(f, "UNPIVOT {expr}")?;
                if let Some(alias) = alias {
                    write!(f, " AS {alias}")?;
                }
                if let Some(at) = at {
                    write!(f, " AT {at}")?;
                }
                Ok(())
            }
        }
    }
}

impl Dialect for Redshift {
    type Ext = Builtin;

    fn parse_select_item_alias<'a>(
        p: &mut Parser<'a, Self>,
        expr: Expr<'a, Builtin>,
    ) -> PResult<SelectItem<'a, Builtin>> {
        grammar::parse_select_item_alias_any_word(p, expr)
    }

    fn parse_create_extension<'a>(
        p: &mut Parser<'a, Self>,
        flags: CreateFlags,
    ) -> PResult<Option<Statement<'a, Builtin>>> {
        grammar::parse_create_role(p, flags, false)
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
        grammar::parse_select_into_table(p)
    }

    fn eat_limit_all<'a>(p: &mut Parser<'a, Self>) -> bool {
        p.eat_keyword("ALL")
    }

    fn parse_statement_after_with<'a>(
        p: &mut Parser<'a, Self>,
    ) -> PResult<Option<Statement<'a, Builtin>>> {
        grammar::parse_dml_after_with(p, false, true)
    }

    fn parse_insert_source<'a>(p: &mut Parser<'a, Self>) -> PResult<InsertSource<'a, Builtin>> {
        grammar::parse_insert_source_with_default(p)
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

    fn set_operator_rule<'a>(p: &Parser<'a, Self>) -> Option<SetOperatorRule> {
        grammar::set_operator_rule_with_minus(p)
    }

    /// Unlike PostgreSQL, block comments do not nest.
    #[inline]
    fn lex_comment(rest: &str) -> Option<Result<usize, &'static str>> {
        if rest.starts_with("/*") {
            return Some(block_comment(rest, false));
        }
        lex_core_comment(rest)
    }

    #[inline]
    fn is_reserved(word: &str) -> bool {
        postgres::is_reserved(word)
    }

    /// `AT` introduces a PartiQL index alias after a table's alias.
    #[inline]
    fn is_reserved_for_alias(word: &str) -> bool {
        word.eq_ignore_ascii_case("AT")
            || word.eq_ignore_ascii_case("MINUS")
            || grammar::is_reserved_for_alias(word)
    }

    fn parse_statement<'a>(p: &mut Parser<'a, Self>) -> PResult<Statement<'a, Builtin>> {
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
        if p.at_keyword("CLOSE") {
            return postgres::utility::parse_close(p)
                .map(|s| StatementKind::Ext(BuiltinStatement::Postgres(s)).into());
        }
        if p.at_keyword("COPY") {
            return postgres::parse_copy(p, REDSHIFT_COPY_OPTIONS);
        }
        if p.at_keyword("VACUUM") {
            return parse_vacuum(p);
        }
        grammar::parse_statement(p)
    }

    fn parse_create<'a>(p: &mut Parser<'a, Self>) -> PResult<Statement<'a, Builtin>> {
        let or_replace = p.peek_nth(1).is_word("OR") && p.peek_nth(2).is_word("REPLACE");
        let n = if or_replace { 3 } else { 1 };
        let temporary = p.peek_nth(n).is_word("TEMPORARY") || p.peek_nth(n).is_word("TEMP");
        let at_table = p.peek_nth(n + usize::from(temporary)).is_word("TABLE");
        if !at_table {
            return grammar::parse_create(p);
        }
        for _ in 0..n + usize::from(temporary) + 1 {
            p.advance();
        }
        parse_create_table_statement(
            p,
            CreateFlags {
                or_replace,
                temporary,
            },
        )
    }

    fn parse_create_view<'a>(
        p: &mut Parser<'a, Self>,
        flags: CreateFlags,
    ) -> PResult<CreateView<'a, Builtin>> {
        let mut view = grammar::parse_create_view(p, flags)?;
        if p.at_keyword("WITH") && p.peek_nth(1).is_word("NO") && p.peek_nth(2).is_word("SCHEMA") {
            p.advance();
            p.advance();
            p.advance();
            p.expect_keyword("BINDING")?;
            view.no_schema_binding = true;
        }
        Ok(view)
    }

    fn parse_extract_field<'a>(p: &mut Parser<'a, Self>) -> PResult<&'a str> {
        let token = p.peek();
        if token.kind == TokenKind::String {
            p.advance();
            return Ok(token.text);
        }
        grammar::parse_date_time_field(p)
    }

    fn parse_function_arg_null_treatment<'a>(
        p: &mut Parser<'a, Self>,
    ) -> PResult<Option<NullTreatment>> {
        Ok(grammar::parse_null_treatment(p))
    }

    fn parse_table_ref<'a>(p: &mut Parser<'a, Self>) -> PResult<TableRef<'a, Builtin>> {
        parse_table_ref(p)
    }

    fn parse_prefix<'a>(p: &mut Parser<'a, Self>) -> PResult<Expr<'a, Builtin>> {
        parse_prefix(p)
    }

    #[inline]
    fn lex_after(prev: Option<Token<'_>>, rest: &str) -> Option<LexResult> {
        if rest.starts_with('[') && prev.is_none_or(|token| token.is_op(".")) {
            return lex_bracketed_name(rest);
        }
        None
    }

    fn parse_interval<'a>(p: &mut Parser<'a, Self>) -> PResult<Expr<'a, Builtin>> {
        grammar::parse_interval(p, false)
    }

    #[inline]
    fn lex_token(rest: &str) -> Option<LexResult> {
        if rest.as_bytes()[0] == b'#'
            && rest
                .as_bytes()
                .get(1)
                .is_some_and(|&b| b.is_ascii_alphabetic() || b == b'_')
        {
            let len = rest
                .as_bytes()
                .iter()
                .skip(1)
                .position(|&b| !(b.is_ascii_alphanumeric() || b == b'_' || !b.is_ascii()))
                .map_or(rest.len(), |at| at + 1);
            return Some(Ok(Lexeme {
                kind: TokenKind::Word,
                len,
            }));
        }
        if rest.as_bytes()[0] == b'#' && !rest[1..].starts_with(['>', '-']) {
            return Some(Ok(Lexeme {
                kind: TokenKind::Op,
                len: 1,
            }));
        }
        if rest.starts_with('\'') {
            return Some(quoted_with_backslash(rest, TokenKind::String));
        }
        if let Some(result) = postgres::lex_prefixed_string(rest, false) {
            return Some(result);
        }
        postgres::lex_token(rest)
    }

    #[inline]
    fn string_literal(token: Token<'_>) -> StringLiteral<'_> {
        postgres::string_literal(token, true)
    }

    /// Redshift has no `FILTER` clause.
    fn parse_function_filter<'a>(_p: &mut Parser<'a, Self>) -> PResult<Option<Expr<'a, Builtin>>> {
        Ok(None)
    }

    fn infix_rule<'a>(p: &Parser<'a, Self>) -> Option<InfixRule<'a, Self>> {
        if p.at_op("#") {
            return Some(InfixRule::binary(prec::OTHER, BinaryOp::Named("#".into())));
        }
        postgres::infix_rule_without_null_tests(p)
    }

    fn parse_alter_table_op<'a>(p: &mut Parser<'a, Self>) -> PResult<AlterTableOp<'a, Builtin>> {
        let next = p.peek_nth(1);
        if p.at_keyword("ALTER") && next.is_word("SORTKEY") && p.peek_nth(2).is_op("(") {
            p.advance();
            p.advance();
            return grammar::parse_ident_list(p).map(AlterTableOp::AlterSortKey);
        }
        grammar::parse_alter_table_op(p)
    }

    fn parse_select_modifiers<'a>(p: &mut Parser<'a, Self>) -> PResult<BuiltinSelect<'a>> {
        if !p.eat_keyword("TOP") {
            return Ok(BuiltinSelect::default());
        }
        let token = p.expect_kind(TokenKind::Number, "a number after TOP")?;
        Ok(BuiltinSelect {
            top: Some(token.text.into()),
            ..BuiltinSelect::default()
        })
    }
}

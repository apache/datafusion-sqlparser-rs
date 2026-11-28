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

//! `COPY`, cursors, `LOCK`, prepared statements and other utility statements.

use std::borrow::Cow;
use std::fmt;

use crate::PResult;
use crate::ast::{
    DataType, Expr, ExprKind, Ident, Literal, ObjectName, Query, Statement, StatementKind,
    StringLiteral,
};
use crate::dialect::postgres::PostgresStatement;
use crate::dialect::postgres::ddl::{statement, write_behavior};
use crate::dialect::{Builtin, Dialect};
use crate::display::comma;
use crate::grammar;
use crate::lexer::{Token, TokenKind};
use crate::parser::Parser;

/// The source of `COPY`: a table with optional columns, or a query.
#[derive(Debug, Clone, PartialEq)]
pub enum CopySource<'a> {
    Table {
        name: ObjectName<'a>,
        columns: Vec<Ident<'a>>,
    },
    Query(Box<Query<'a, Builtin>>),
}

/// Where `COPY` reads or writes.
#[derive(Debug, Clone, PartialEq)]
pub enum CopyTarget<'a> {
    Stdin,
    Stdout,
    Program(StringLiteral<'a>),
    File(StringLiteral<'a>),
}

/// One `(option value)` of the modern `COPY` option list.
#[derive(Debug, Clone, PartialEq)]
pub enum CopyOption<'a> {
    Format(Ident<'a>),
    Freeze(Option<bool>),
    Delimiter(StringLiteral<'a>),
    Null(StringLiteral<'a>),
    Header(Option<bool>),
    Quote(StringLiteral<'a>),
    Escape(StringLiteral<'a>),
    ForceQuote(Vec<Ident<'a>>),
    ForceNotNull(Vec<Ident<'a>>),
    ForceNull(Vec<Ident<'a>>),
    Encoding(StringLiteral<'a>),
}

/// A keyword option of the legacy `COPY` syntax. Redshift's are of this form too.
#[derive(Debug, Clone, PartialEq)]
pub struct CopyLegacyOption<'a> {
    pub keyword: Cow<'a, str>,
    pub value: CopyLegacyValue<'a>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum CopyLegacyValue<'a> {
    None,
    /// The word `DEFAULT` in place of a string.
    Default,
    String(StringLiteral<'a>),
    Number(Cow<'a, str>),
    /// `FORCE NOT NULL a, b` or `FORCE QUOTE a`.
    Force {
        not_null: bool,
        columns: Vec<Ident<'a>>,
    },
    /// An optional `PRESET` and a boolean word such as `ON`.
    Toggle {
        preset: bool,
        word: Option<Cow<'a, str>>,
    },
}

/// What a legacy option takes after its keyword.
#[derive(Clone, Copy)]
pub enum CopyLegacyKind {
    Flag,
    /// `[AS] 'text'`
    String,
    /// `[AS] 'text'` or `DEFAULT`.
    StringOrDefault,
    /// `[AS] 'text'`, or nothing.
    OptionalString,
    /// `[AS] n`
    Number,
    /// `[PRESET] [TRUE | FALSE | ON | OFF]`
    Toggle,
    /// `[NOT NULL | QUOTE] a, b`, after `FORCE`.
    Force,
}

/// The legacy options PostgreSQL has, shared with Redshift.
pub const LEGACY_OPTIONS: &[(&str, CopyLegacyKind)] = &[
    ("BINARY", CopyLegacyKind::Flag),
    ("DELIMITER", CopyLegacyKind::String),
    ("NULL", CopyLegacyKind::String),
    ("CSV", CopyLegacyKind::Flag),
    ("HEADER", CopyLegacyKind::Flag),
    ("QUOTE", CopyLegacyKind::String),
    ("ESCAPE", CopyLegacyKind::String),
    ("FORCE", CopyLegacyKind::Force),
    ("CREDENTIALS", CopyLegacyKind::String),
    ("IAM_ROLE", CopyLegacyKind::String),
];

/// `COPY ... FROM STDIN` data that follows the statement.
#[derive(Debug, Clone, PartialEq)]
pub struct CopyPayload<'a> {
    pub data: Cow<'a, str>,
    /// True if the `\.` terminator was present.
    pub terminated: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Copy<'a> {
    pub source: CopySource<'a>,
    /// True for `TO`, false for `FROM`.
    pub to: bool,
    pub target: CopyTarget<'a>,
    /// True if `WITH` was written before the option list.
    pub with: bool,
    pub options: Vec<CopyOption<'a>>,
    pub legacy_options: Vec<CopyLegacyOption<'a>>,
    pub payload: Option<CopyPayload<'a>>,
}

/// A table named by `TRUNCATE` or `LOCK`.
#[derive(Debug, Clone, PartialEq)]
pub struct TableItem<'a> {
    pub only: bool,
    pub name: ObjectName<'a>,
    /// True if `*` followed the name.
    pub star: bool,
}

/// `TRUNCATE` with clauses the core does not have.
#[derive(Debug, Clone, PartialEq)]
pub struct PostgresTruncate<'a> {
    pub table_keyword: bool,
    pub tables: Vec<TableItem<'a>>,
    /// `Some(true)` for `RESTART IDENTITY`, `Some(false)` for `CONTINUE IDENTITY`.
    pub identity: Option<bool>,
    pub behavior: Option<crate::ast::DropBehavior>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DeclareCursor<'a> {
    pub name: Ident<'a>,
    pub binary: bool,
    /// `ASENSITIVE` or `INSENSITIVE`.
    pub sensitivity: Option<Cow<'a, str>>,
    /// `Some(true)` for `SCROLL`, `Some(false)` for `NO SCROLL`.
    pub scroll: Option<bool>,
    /// `Some(true)` for `WITH HOLD`, `Some(false)` for `WITHOUT HOLD`.
    pub hold: Option<bool>,
    pub query: Box<Query<'a, Builtin>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum FetchDirection<'a> {
    Next,
    Prior,
    First,
    Last,
    All,
    Count(Cow<'a, str>),
    Absolute(Cow<'a, str>),
    Relative(Cow<'a, str>),
    Forward(Option<Cow<'a, str>>),
    ForwardAll,
    Backward(Option<Cow<'a, str>>),
    BackwardAll,
}

/// `FETCH direction {IN | FROM} cursor [INTO target]`
#[derive(Debug, Clone, PartialEq)]
pub struct FetchStatement<'a> {
    pub direction: FetchDirection<'a>,
    pub position: Cow<'a, str>,
    pub cursor: Ident<'a>,
    pub into: Option<ObjectName<'a>>,
}

/// `LOCK [TABLE] tables [IN mode MODE] [NOWAIT]`
#[derive(Debug, Clone, PartialEq)]
pub struct LockStatement<'a> {
    pub table_keyword: bool,
    pub tables: Vec<TableItem<'a>>,
    pub mode: Option<Cow<'a, str>>,
    pub nowait: bool,
}

/// `PREPARE name [(types)] AS statement`
#[derive(Debug, Clone, PartialEq)]
pub struct PrepareStatement<'a> {
    pub name: Ident<'a>,
    pub types: Vec<DataType<'a>>,
    pub statement: Box<Statement<'a, Builtin>>,
}

/// `EXECUTE name [(args)] [USING values]`
#[derive(Debug, Clone, PartialEq)]
pub struct ExecuteStatement<'a> {
    pub name: Ident<'a>,
    pub args: Option<Vec<Expr<'a, Builtin>>>,
    pub using: Vec<Expr<'a, Builtin>>,
}

/// An item of a `DO` statement.
#[derive(Debug, Clone, PartialEq)]
pub enum DoItem<'a> {
    Language(Ident<'a>),
    Body(Literal<'a>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct AnalyzeTable<'a> {
    pub name: ObjectName<'a>,
    pub columns: Vec<Ident<'a>>,
}

// Parsing

fn is_name_word<D: Dialect>(p: &Parser<'_, D>, token: Token<'_>) -> bool {
    p.at_ident() && matches!(token.kind, TokenKind::Word | TokenKind::QuotedIdent)
}

fn parse_paren_idents<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Vec<Ident<'a>>> {
    grammar::parse_ident_list(p)
}

fn parse_copy_option<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<CopyOption<'a>> {
    if p.eat_keyword("FORMAT") {
        return p.parse_name_part().map(CopyOption::Format);
    }
    if p.eat_keyword("FREEZE") {
        return Ok(CopyOption::Freeze(parse_optional_bool(p)));
    }
    if p.eat_keyword("DELIMITER") {
        return grammar::parse_string(p).map(CopyOption::Delimiter);
    }
    if p.eat_keyword("NULL") {
        return grammar::parse_string(p).map(CopyOption::Null);
    }
    if p.eat_keyword("HEADER") {
        return Ok(CopyOption::Header(parse_optional_bool(p)));
    }
    if p.eat_keyword("QUOTE") {
        return grammar::parse_string(p).map(CopyOption::Quote);
    }
    if p.eat_keyword("ESCAPE") {
        return grammar::parse_string(p).map(CopyOption::Escape);
    }
    if p.eat_keyword("FORCE_QUOTE") {
        return parse_paren_idents(p).map(CopyOption::ForceQuote);
    }
    if p.eat_keyword("FORCE_NOT_NULL") {
        return parse_paren_idents(p).map(CopyOption::ForceNotNull);
    }
    if p.eat_keyword("FORCE_NULL") {
        return parse_paren_idents(p).map(CopyOption::ForceNull);
    }
    if p.eat_keyword("ENCODING") {
        return grammar::parse_string(p).map(CopyOption::Encoding);
    }
    Err(p.expected("a COPY option"))
}

fn parse_optional_bool<D: Dialect>(p: &mut Parser<'_, D>) -> Option<bool> {
    if p.eat_keyword("TRUE") {
        Some(true)
    } else if p.eat_keyword("FALSE") {
        Some(false)
    } else {
        None
    }
}

fn parse_legacy_value<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
    kind: CopyLegacyKind,
) -> PResult<CopyLegacyValue<'a>> {
    match kind {
        CopyLegacyKind::Flag => Ok(CopyLegacyValue::None),
        CopyLegacyKind::String => {
            p.eat_keyword("AS");
            grammar::parse_string(p).map(CopyLegacyValue::String)
        }
        CopyLegacyKind::StringOrDefault => {
            p.eat_keyword("AS");
            if p.eat_keyword("DEFAULT") {
                return Ok(CopyLegacyValue::Default);
            }
            grammar::parse_string(p).map(CopyLegacyValue::String)
        }
        CopyLegacyKind::OptionalString => {
            p.eat_keyword("AS");
            if p.peek().kind == TokenKind::String {
                grammar::parse_string(p).map(CopyLegacyValue::String)
            } else {
                Ok(CopyLegacyValue::None)
            }
        }
        CopyLegacyKind::Number => {
            p.eat_keyword("AS");
            grammar::parse_number_text(p).map(|v| CopyLegacyValue::Number(v.into()))
        }
        CopyLegacyKind::Toggle => {
            let preset = p.eat_keyword("PRESET");
            let word = ["TRUE", "FALSE", "ON", "OFF"]
                .into_iter()
                .find(|word| p.eat_keyword(word));
            Ok(CopyLegacyValue::Toggle {
                preset,
                word: word.map(Cow::Borrowed),
            })
        }
        CopyLegacyKind::Force => {
            let not_null = p.eat_keyword("NOT");
            if not_null {
                p.expect_keyword("NULL")?;
            } else {
                p.expect_keyword("QUOTE")?;
            }
            let columns = p.parse_comma_separated(Parser::parse_ident)?;
            Ok(CopyLegacyValue::Force { not_null, columns })
        }
    }
}

/// Parses legacy options drawn from `table`, until something else.
pub fn parse_legacy_options<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
    table: &[(&'static str, CopyLegacyKind)],
) -> PResult<Vec<CopyLegacyOption<'a>>> {
    let mut options = Vec::new();
    loop {
        // `FORMAT [AS]` before an option carries no meaning of its own.
        if p.at_keyword("FORMAT") && table.iter().any(|(word, _)| *word == "PARQUET") {
            p.advance();
            p.eat_keyword("AS");
        }
        let token = p.peek();
        let Some((keyword, kind)) = table
            .iter()
            .find(|(word, _)| token.kind == TokenKind::Word && token.is_word(word))
        else {
            break;
        };
        p.advance();
        let value = parse_legacy_value(p, *kind)?;
        options.push(CopyLegacyOption {
            keyword: (*keyword).into(),
            value,
        });
    }
    Ok(options)
}

/// Parses `COPY ...`, with the cursor on the source, and any `COPY` data.
pub fn parse_copy<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
    legacy: &[(&'static str, CopyLegacyKind)],
) -> PResult<Statement<'a, Builtin>> {
    p.expect_keyword("COPY")?;
    let source = if p.eat_op("(") {
        let query = p.nested(D::parse_query)?;
        p.expect_op(")")?;
        CopySource::Query(Box::new(query))
    } else {
        let name = grammar::parse_object_name(p)?;
        let columns = if p.at_op("(") {
            grammar::parse_ident_list(p)?
        } else {
            Vec::new()
        };
        CopySource::Table { name, columns }
    };
    let to = if p.eat_keyword("TO") {
        true
    } else {
        p.expect_keyword("FROM")?;
        false
    };
    if !to && matches!(source, CopySource::Query(_)) {
        return Err(p.expected("TO after a query"));
    }
    let target = if p.eat_keyword("STDIN") {
        CopyTarget::Stdin
    } else if p.eat_keyword("STDOUT") {
        CopyTarget::Stdout
    } else if p.eat_keyword("PROGRAM") {
        CopyTarget::Program(grammar::parse_string(p)?)
    } else {
        CopyTarget::File(grammar::parse_string(p)?)
    };
    let with = p.eat_keyword("WITH");
    let options = if p.eat_op("(") {
        let options = p.parse_comma_separated(parse_copy_option)?;
        p.expect_op(")")?;
        options
    } else {
        Vec::new()
    };
    let legacy_options = parse_legacy_options(p, legacy)?;
    let payload = parse_copy_payload(p);
    Ok(statement(PostgresStatement::Copy(Box::new(Copy {
        source,
        to,
        target,
        with,
        options,
        legacy_options,
        payload,
    }))))
}

/// Consumes the payload token, if the lexer made one, leaving the `;` for
/// the statement separator.
fn parse_copy_payload<'a, D: Dialect>(p: &mut Parser<'a, D>) -> Option<CopyPayload<'a>> {
    if p.peek().kind != TokenKind::DialectLiteral {
        return None;
    }
    let text = p.advance().text;
    Some(match text.strip_suffix("\\.") {
        Some(data) => CopyPayload {
            data: data.into(),
            terminated: true,
        },
        None => match text
            .strip_suffix("\\.\n")
            .or_else(|| text.strip_suffix("\\.\r\n"))
        {
            Some(data) => CopyPayload {
                data: data.into(),
                terminated: true,
            },
            None => CopyPayload {
                data: text.into(),
                terminated: false,
            },
        },
    })
}

fn parse_table_items<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Vec<TableItem<'a>>> {
    p.parse_comma_separated(|p| {
        let only = p.eat_keyword("ONLY");
        let name = grammar::parse_object_name(p)?;
        let star = !only && p.eat_op("*");
        Ok(TableItem { only, name, star })
    })
}

/// Parses `TRUNCATE` and its clauses.
pub fn parse_truncate<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Statement<'a, Builtin>> {
    p.expect_keyword("TRUNCATE")?;
    let table_keyword = p.eat_keyword("TABLE");
    let tables = parse_table_items(p)?;
    let identity = if p.eat_keyword("RESTART") {
        p.expect_keyword("IDENTITY")?;
        Some(true)
    } else if p.eat_keyword("CONTINUE") {
        p.expect_keyword("IDENTITY")?;
        Some(false)
    } else {
        None
    };
    let behavior = if p.eat_keyword("CASCADE") {
        Some(crate::ast::DropBehavior::Cascade)
    } else if p.eat_keyword("RESTRICT") {
        Some(crate::ast::DropBehavior::Restrict)
    } else {
        None
    };
    if identity.is_none() && behavior.is_none() && tables.iter().all(|t| !t.only && !t.star) {
        return Ok(StatementKind::Truncate(Box::new(crate::ast::Truncate {
            table_keyword,
            if_exists: false,
            names: tables.into_iter().map(|t| t.name).collect(),
        }))
        .into());
    }
    Ok(statement(PostgresStatement::Truncate(Box::new(
        PostgresTruncate {
            table_keyword,
            tables,
            identity,
            behavior,
        },
    ))))
}

fn parse_declare<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<PostgresStatement<'a>> {
    p.advance();
    let name = p.parse_ident()?;
    let binary = p.eat_keyword("BINARY");
    let sensitivity = ["ASENSITIVE", "INSENSITIVE"]
        .into_iter()
        .find(|word| p.eat_keyword(word));
    let scroll = if p.eat_keyword("SCROLL") {
        Some(true)
    } else if p.at_keyword("NO") && p.peek_nth(1).is_word("SCROLL") {
        p.advance();
        p.advance();
        Some(false)
    } else {
        None
    };
    p.expect_keyword("CURSOR")?;
    let hold = if p.eat_keyword("WITH") {
        p.expect_keyword("HOLD")?;
        Some(true)
    } else if p.eat_keyword("WITHOUT") {
        p.expect_keyword("HOLD")?;
        Some(false)
    } else {
        None
    };
    p.expect_keyword("FOR")?;
    let query = D::parse_query(p)?;
    Ok(PostgresStatement::Declare(Box::new(DeclareCursor {
        name,
        binary,
        sensitivity: sensitivity.map(Cow::Borrowed),
        scroll,
        hold,
        query: Box::new(query),
    })))
}

fn parse_fetch<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<PostgresStatement<'a>> {
    p.advance();
    let direction = if p.eat_keyword("NEXT") {
        FetchDirection::Next
    } else if p.eat_keyword("PRIOR") {
        FetchDirection::Prior
    } else if p.eat_keyword("FIRST") {
        FetchDirection::First
    } else if p.eat_keyword("LAST") {
        FetchDirection::Last
    } else if p.eat_keyword("ALL") {
        FetchDirection::All
    } else if p.eat_keyword("ABSOLUTE") {
        FetchDirection::Absolute(grammar::parse_number_text(p)?.into())
    } else if p.eat_keyword("RELATIVE") {
        FetchDirection::Relative(grammar::parse_number_text(p)?.into())
    } else if p.at_keyword("FORWARD") || p.at_keyword("BACKWARD") {
        let forward = p.at_keyword("FORWARD");
        p.advance();
        if p.eat_keyword("ALL") {
            if forward {
                FetchDirection::ForwardAll
            } else {
                FetchDirection::BackwardAll
            }
        } else {
            let count = if p.peek().kind == TokenKind::Number {
                Some(grammar::parse_number_text(p)?)
            } else {
                None
            };
            if forward {
                FetchDirection::Forward(count.map(Cow::Borrowed))
            } else {
                FetchDirection::Backward(count.map(Cow::Borrowed))
            }
        }
    } else {
        FetchDirection::Count(grammar::parse_number_text(p)?.into())
    };
    let position = if p.eat_keyword("IN") {
        "IN"
    } else {
        p.expect_keyword("FROM")?;
        "FROM"
    };
    let cursor = p.parse_ident()?;
    let into = if p.eat_keyword("INTO") {
        Some(grammar::parse_object_name(p)?)
    } else {
        None
    };
    Ok(PostgresStatement::Fetch(Box::new(FetchStatement {
        direction,
        position: position.into(),
        cursor,
        into,
    })))
}

fn parse_lock<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<PostgresStatement<'a>> {
    p.advance();
    let table_keyword = p.eat_keyword("TABLE");
    let tables = parse_table_items(p)?;
    let mode = if p.eat_keyword("IN") {
        const MODES: [(&str, &[&str]); 8] = [
            ("ACCESS SHARE", &["ACCESS", "SHARE"]),
            ("ROW SHARE", &["ROW", "SHARE"]),
            ("ROW EXCLUSIVE", &["ROW", "EXCLUSIVE"]),
            ("SHARE UPDATE EXCLUSIVE", &["SHARE", "UPDATE", "EXCLUSIVE"]),
            ("SHARE ROW EXCLUSIVE", &["SHARE", "ROW", "EXCLUSIVE"]),
            ("SHARE", &["SHARE"]),
            ("EXCLUSIVE", &["EXCLUSIVE"]),
            ("ACCESS EXCLUSIVE", &["ACCESS", "EXCLUSIVE"]),
        ];
        let found = MODES.into_iter().find(|(_, words)| {
            words
                .iter()
                .enumerate()
                .all(|(i, word)| p.peek_nth(i).is_word(word))
        });
        let Some((mode, words)) = found else {
            return Err(p.expected("a lock mode"));
        };
        for _ in words {
            p.advance();
        }
        p.expect_keyword("MODE")?;
        Some(mode)
    } else {
        None
    };
    let nowait = p.eat_keyword("NOWAIT");
    Ok(PostgresStatement::Lock(Box::new(LockStatement {
        table_keyword,
        tables,
        mode: mode.map(Cow::Borrowed),
        nowait,
    })))
}

/// Parses `PREPARE`, `EXECUTE` or `DEALLOCATE`, or returns `None` at anything
/// else. DuckDB shares these with PostgreSQL.
pub fn parse_prepared_statement<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Option<PostgresStatement<'a>>> {
    let token = p.peek();
    let parsed = if token.is_word("PREPARE") {
        parse_prepare(p)?
    } else if token.is_word("EXECUTE") {
        parse_execute(p)?
    } else if token.is_word("DEALLOCATE") {
        parse_deallocate(p)?
    } else {
        return Ok(None);
    };
    Ok(Some(parsed))
}

fn parse_prepare<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<PostgresStatement<'a>> {
    p.advance();
    let name = p.parse_ident()?;
    let types = if p.eat_op("(") {
        let types = p.parse_comma_separated(D::parse_data_type)?;
        p.expect_op(")")?;
        types
    } else {
        Vec::new()
    };
    p.expect_keyword("AS")?;
    let inner = p.nested(Parser::parse_statement)?;
    Ok(PostgresStatement::Prepare(Box::new(PrepareStatement {
        name,
        types,
        statement: Box::new(inner),
    })))
}

fn parse_execute<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<PostgresStatement<'a>> {
    p.advance();
    let name = p.parse_ident()?;
    let args = if p.eat_op("(") {
        let args = if p.at_op(")") {
            Vec::new()
        } else {
            p.parse_comma_separated(Parser::parse_expr)?
        };
        p.expect_op(")")?;
        Some(args)
    } else {
        None
    };
    let using = if p.eat_keyword("USING") {
        p.parse_comma_separated(Parser::parse_expr)?
    } else {
        Vec::new()
    };
    Ok(PostgresStatement::Execute(Box::new(ExecuteStatement {
        name,
        args,
        using,
    })))
}

fn parse_deallocate<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<PostgresStatement<'a>> {
    p.advance();
    let prepare = p.eat_keyword("PREPARE");
    let name = if p.eat_keyword("ALL") {
        None
    } else {
        Some(p.parse_ident()?)
    };
    Ok(PostgresStatement::Deallocate { prepare, name })
}

fn parse_do<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<PostgresStatement<'a>> {
    p.advance();
    let mut items = Vec::new();
    loop {
        if p.at_keyword("LANGUAGE") {
            p.advance();
            items.push(DoItem::Language(p.parse_ident()?));
        } else if matches!(p.peek().kind, TokenKind::String | TokenKind::DollarString) {
            items.push(DoItem::Body(match p.parse_expr()?.kind {
                ExprKind::Literal(literal) => literal,
                _ => return Err(p.expected("a string")),
            }));
        } else {
            break;
        }
    }
    if items.is_empty() {
        return Err(p.expected("LANGUAGE or a string"));
    }
    Ok(PostgresStatement::Do(items))
}

fn parse_analyze<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<PostgresStatement<'a>> {
    p.advance();
    let verbose = p.eat_keyword("VERBOSE");
    let tables = if is_name_word(p, p.peek()) {
        p.parse_comma_separated(|p| {
            let name = grammar::parse_object_name(p)?;
            let columns = if p.at_op("(") {
                grammar::parse_ident_list(p)?
            } else {
                Vec::new()
            };
            Ok(AnalyzeTable { name, columns })
        })?
    } else {
        Vec::new()
    };
    Ok(PostgresStatement::Analyze { verbose, tables })
}

fn parse_notify<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<PostgresStatement<'a>> {
    p.advance();
    let channel = p.parse_ident()?;
    let payload = if p.eat_op(",") {
        Some(grammar::parse_string(p)?)
    } else {
        None
    };
    Ok(PostgresStatement::Notify { channel, payload })
}

/// Parses `RESET name`, `RESET ALL` or `RESET SESSION AUTHORIZATION`.
pub fn parse_reset<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<PostgresStatement<'a>> {
    use crate::dialect::postgres::ResetTarget;
    p.expect_keyword("RESET")?;
    let target = if p.eat_keyword("ALL") {
        ResetTarget::All
    } else if p.at_keyword("SESSION") && p.peek_nth(1).is_word("AUTHORIZATION") {
        p.advance();
        p.advance();
        ResetTarget::SessionAuthorization
    } else {
        ResetTarget::Name(grammar::parse_object_name(p)?)
    };
    Ok(PostgresStatement::Reset(target))
}

/// Parses `CLOSE name` or `CLOSE ALL`.
pub fn parse_close<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<PostgresStatement<'a>> {
    p.expect_keyword("CLOSE")?;
    if p.eat_keyword("ALL") {
        return Ok(PostgresStatement::Close(None));
    }
    Ok(PostgresStatement::Close(Some(p.parse_ident()?)))
}

/// Parses the statements in this module, or returns `None` at something else.
pub fn parse_utility<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
    legacy: &[(&'static str, CopyLegacyKind)],
) -> PResult<Option<Statement<'a, Builtin>>> {
    let token = p.peek();
    if token.kind != TokenKind::Word {
        return Ok(None);
    }
    let statement_of = |parsed: PostgresStatement<'a>| statement(parsed);
    if token.is_word("COPY") {
        return parse_copy(p, legacy).map(Some);
    }
    if token.is_word("TRUNCATE") {
        return parse_truncate(p).map(Some);
    }
    let parsed = if token.is_word("DECLARE") {
        parse_declare(p)?
    } else if token.is_word("FETCH") {
        parse_fetch(p)?
    } else if token.is_word("LOCK") {
        parse_lock(p)?
    } else if let Some(prepared) = parse_prepared_statement(p)? {
        prepared
    } else if token.is_word("DO") {
        parse_do(p)?
    } else if token.is_word("ANALYZE") {
        parse_analyze(p)?
    } else if token.is_word("NOTIFY") {
        parse_notify(p)?
    } else if token.is_word("LISTEN") {
        p.advance();
        PostgresStatement::Listen(p.parse_ident()?)
    } else if token.is_word("DISCARD") {
        p.advance();
        let target = p.peek();
        if !["ALL", "PLANS", "SEQUENCES", "TEMP", "TEMPORARY"]
            .iter()
            .any(|word| target.is_word(word))
        {
            return Err(p.expected("ALL, PLANS, SEQUENCES, TEMP or TEMPORARY"));
        }
        p.advance();
        PostgresStatement::Discard(target.text.into())
    } else if token.is_word("RESET") {
        parse_reset(p)?
    } else if token.is_word("CLOSE") {
        parse_close(p)?
    } else if token.is_word("LOAD") && p.peek_nth(1).kind == TokenKind::String {
        p.advance();
        PostgresStatement::Load(grammar::parse_string(p)?)
    } else if token.is_word("UNLISTEN") {
        p.advance();
        PostgresStatement::Unlisten(if p.eat_op("*") {
            None
        } else {
            Some(p.parse_ident()?)
        })
    } else {
        return Ok(None);
    };
    Ok(Some(statement_of(parsed)))
}

impl fmt::Display for CopyOption<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let flag = |f: &mut fmt::Formatter<'_>, word: &str, value: &Option<bool>| match value {
            Some(false) => write!(f, "{word} FALSE"),
            _ => f.write_str(word),
        };
        let literal = |s: StringLiteral<'_>| Literal::String(s).to_string();
        match self {
            CopyOption::Format(name) => write!(f, "FORMAT {name}"),
            CopyOption::Freeze(value) => flag(f, "FREEZE", value),
            CopyOption::Delimiter(s) => write!(f, "DELIMITER {}", literal(s.clone())),
            CopyOption::Null(s) => write!(f, "NULL {}", literal(s.clone())),
            CopyOption::Header(value) => flag(f, "HEADER", value),
            CopyOption::Quote(s) => write!(f, "QUOTE {}", literal(s.clone())),
            CopyOption::Escape(s) => write!(f, "ESCAPE {}", literal(s.clone())),
            CopyOption::ForceQuote(columns) => write!(f, "FORCE_QUOTE ({})", comma(columns)),
            CopyOption::ForceNotNull(columns) => write!(f, "FORCE_NOT_NULL ({})", comma(columns)),
            CopyOption::ForceNull(columns) => write!(f, "FORCE_NULL ({})", comma(columns)),
            CopyOption::Encoding(s) => write!(f, "ENCODING {}", literal(s.clone())),
        }
    }
}

impl fmt::Display for CopyLegacyOption<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.keyword)?;
        match &self.value {
            CopyLegacyValue::None => Ok(()),
            CopyLegacyValue::Default => f.write_str(" DEFAULT"),
            CopyLegacyValue::String(s) => write!(f, " {}", s),
            CopyLegacyValue::Number(n) => write!(f, " {n}"),
            CopyLegacyValue::Force { not_null, columns } => {
                write!(
                    f,
                    " {} {}",
                    if *not_null { "NOT NULL" } else { "QUOTE" },
                    comma(columns)
                )
            }
            CopyLegacyValue::Toggle { preset, word } => {
                if *preset {
                    f.write_str(" PRESET")?;
                }
                if let Some(word) = word {
                    write!(f, " {word}")?;
                }
                Ok(())
            }
        }
    }
}

impl fmt::Display for Copy<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("COPY ")?;
        match &self.source {
            CopySource::Table { name, columns } => {
                name.fmt(f)?;
                if !columns.is_empty() {
                    write!(f, " ({})", comma(columns))?;
                }
            }
            CopySource::Query(query) => write!(f, "({query})")?,
        }
        write!(f, " {} ", if self.to { "TO" } else { "FROM" })?;
        match &self.target {
            CopyTarget::Stdin => f.write_str("STDIN")?,
            CopyTarget::Stdout => f.write_str("STDOUT")?,
            CopyTarget::Program(command) => write!(f, "PROGRAM {}", command)?,
            CopyTarget::File(file) => file.fmt(f)?,
        }
        if !self.options.is_empty() {
            write!(f, " ({})", comma(&self.options))?;
        }
        for option in &self.legacy_options {
            write!(f, " {option}")?;
        }
        if let Some(payload) = &self.payload {
            f.write_str(";\n")?;
            f.write_str(&payload.data)?;
            if payload.terminated {
                f.write_str("\\.")?;
            }
        }
        Ok(())
    }
}

impl fmt::Display for TableItem<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.only {
            f.write_str("ONLY ")?;
        }
        self.name.fmt(f)?;
        if self.star {
            f.write_str(" *")?;
        }
        Ok(())
    }
}

impl fmt::Display for PostgresTruncate<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("TRUNCATE ")?;
        if self.table_keyword {
            f.write_str("TABLE ")?;
        }
        comma(&self.tables).fmt(f)?;
        match self.identity {
            Some(true) => f.write_str(" RESTART IDENTITY")?,
            Some(false) => f.write_str(" CONTINUE IDENTITY")?,
            None => {}
        }
        write_behavior(f, self.behavior)
    }
}

impl fmt::Display for DeclareCursor<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DECLARE {}", self.name)?;
        if self.binary {
            f.write_str(" BINARY")?;
        }
        if let Some(ref sensitivity) = self.sensitivity {
            write!(f, " {sensitivity}")?;
        }
        match self.scroll {
            Some(true) => f.write_str(" SCROLL")?,
            Some(false) => f.write_str(" NO SCROLL")?,
            None => {}
        }
        f.write_str(" CURSOR")?;
        match self.hold {
            Some(true) => f.write_str(" WITH HOLD")?,
            Some(false) => f.write_str(" WITHOUT HOLD")?,
            None => {}
        }
        write!(f, " FOR {}", self.query)
    }
}

impl fmt::Display for FetchDirection<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FetchDirection::Next => f.write_str("NEXT"),
            FetchDirection::Prior => f.write_str("PRIOR"),
            FetchDirection::First => f.write_str("FIRST"),
            FetchDirection::Last => f.write_str("LAST"),
            FetchDirection::All => f.write_str("ALL"),
            FetchDirection::Count(n) => f.write_str(n),
            FetchDirection::Absolute(n) => write!(f, "ABSOLUTE {n}"),
            FetchDirection::Relative(n) => write!(f, "RELATIVE {n}"),
            FetchDirection::Forward(None) => f.write_str("FORWARD"),
            FetchDirection::Forward(Some(n)) => write!(f, "FORWARD {n}"),
            FetchDirection::ForwardAll => f.write_str("FORWARD ALL"),
            FetchDirection::Backward(None) => f.write_str("BACKWARD"),
            FetchDirection::Backward(Some(n)) => write!(f, "BACKWARD {n}"),
            FetchDirection::BackwardAll => f.write_str("BACKWARD ALL"),
        }
    }
}

impl fmt::Display for FetchStatement<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "FETCH {} {} {}",
            self.direction, self.position, self.cursor
        )?;
        if let Some(into) = &self.into {
            write!(f, " INTO {into}")?;
        }
        Ok(())
    }
}

impl fmt::Display for LockStatement<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LOCK TABLE {}", comma(&self.tables))?;
        if let Some(ref mode) = self.mode {
            write!(f, " IN {mode} MODE")?;
        }
        if self.nowait {
            f.write_str(" NOWAIT")?;
        }
        Ok(())
    }
}

impl fmt::Display for PrepareStatement<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PREPARE {}", self.name)?;
        if !self.types.is_empty() {
            write!(f, " ({})", comma(&self.types))?;
        }
        write!(f, " AS {}", self.statement)
    }
}

impl fmt::Display for ExecuteStatement<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "EXECUTE {}", self.name)?;
        if let Some(args) = &self.args {
            write!(f, "({})", comma(args))?;
        }
        if !self.using.is_empty() {
            write!(f, " USING {}", comma(&self.using))?;
        }
        Ok(())
    }
}

impl fmt::Display for DoItem<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DoItem::Language(name) => write!(f, "LANGUAGE {name}"),
            DoItem::Body(body) => body.fmt(f),
        }
    }
}

impl fmt::Display for AnalyzeTable<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.name.fmt(f)?;
        if !self.columns.is_empty() {
            write!(f, " ({})", comma(&self.columns))?;
        }
        Ok(())
    }
}

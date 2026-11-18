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

//! MySQL statements and table factors the core does not have.

use std::borrow::Cow;
use std::fmt;

use super::{parse_user_host, string_ident};
use crate::PResult;
use crate::ast::{
    Assignment, CreateTable, CreateView, Expr, ExprKind, Grantee, Ident, IndexColumn, Insert,
    KeyOption, Literal, ObjectName, Statement, StatementKind, StringLiteral, TableConstraint,
    TableConstraintKind, TableRef,
};
use crate::dialect::builtin::BuiltinStatement;
use crate::dialect::{Builtin, Dialect};
use crate::display::{
    CommaLines, Indent, NewLine, SpaceOrNewline, comma, write_columns, write_create_flags,
    write_if_not_exists, write_insert_body,
};
use crate::grammar;
use crate::lexer::TokenKind;
use crate::parser::Parser;

fn mysql<'a>(statement: MySqlStatement<'a>) -> Statement<'a, Builtin> {
    StatementKind::Ext(BuiltinStatement::MySql(statement)).into()
}

#[derive(Debug, Clone, PartialEq)]
pub enum MySqlStatement<'a> {
    /// `RENAME TABLE a TO b, c TO d`
    RenameTable(Vec<(ObjectName<'a>, ObjectName<'a>)>),
    /// `LOCK TABLES t [[AS] alias] lock_type, ...`
    LockTables(Vec<MySqlLockedTable<'a>>),
    UnlockTables,
    Flush(Box<MySqlFlush<'a>>),
    /// `KILL [CONNECTION | QUERY] id`
    Kill {
        modifier: Option<Cow<'a, str>>,
        id: Cow<'a, str>,
    },
    Insert(Box<MySqlInsert<'a>>),
    CreateTable(Box<MySqlCreateTable<'a>>),
    CreateDatabase(Box<MySqlCreateDatabase<'a>>),
    CreateView(Box<MySqlCreateView<'a>>),
    CreateTrigger(Box<MySqlCreateTrigger<'a>>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct MySqlLockedTable<'a> {
    pub table: ObjectName<'a>,
    pub alias: Option<Ident<'a>>,
    pub lock: MySqlLock,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MySqlLock {
    Read { local: bool },
    Write { low_priority: bool },
}

#[derive(Debug, Clone, PartialEq)]
pub struct MySqlFlush<'a> {
    /// `NO_WRITE_TO_BINLOG` or `LOCAL`.
    pub location: Option<Cow<'a, str>>,
    /// What is flushed, such as `RELAY LOGS` or `TABLES`.
    pub object: Cow<'a, str>,
    /// The channel in `RELAY LOGS FOR CHANNEL name`.
    pub channel: Option<ObjectName<'a>>,
    pub tables: Vec<ObjectName<'a>>,
    pub read_lock: bool,
    pub export: bool,
}

/// `INSERT` or `REPLACE`, with what MySQL adds after its source.
#[derive(Debug, Clone, PartialEq)]
pub struct MySqlInsert<'a> {
    pub replace: bool,
    pub insert: Box<Insert<'a, Builtin>>,
    /// `AS name [(columns)]`
    pub alias: Option<(Ident<'a>, Vec<Ident<'a>>)>,
    /// The assignments in `ON DUPLICATE KEY UPDATE`.
    pub on_duplicate: Vec<Assignment<'a, Builtin>>,
}

/// Table options such as `ENGINE = InnoDB`, which come before any `AS`.
#[derive(Debug, Clone, PartialEq)]
pub struct MySqlCreateTable<'a> {
    pub table: Box<CreateTable<'a, Builtin>>,
    pub options: Vec<MySqlTableOption<'a>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum MySqlTableOption<'a> {
    /// A keyword or words and a value, always rendered with `=`.
    KeyValue {
        name: Cow<'a, str>,
        value: Expr<'a, Builtin>,
    },
    /// `COMMENT [=] 'text'`
    Comment {
        equals: bool,
        text: StringLiteral<'a>,
    },
    /// `ENGINE [=] name`
    Engine(Ident<'a>),
    /// `LIKE source`
    Like(ObjectName<'a>),
    /// `TABLESPACE name [STORAGE {DISK | MEMORY}]`
    Tablespace {
        name: Ident<'a>,
        storage: Option<Cow<'a, str>>,
    },
    /// `UNION [=] (a, b)`
    Union(Vec<Ident<'a>>),
    StartTransaction,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MySqlCreateDatabase<'a> {
    pub if_not_exists: bool,
    pub name: ObjectName<'a>,
    pub charset: Option<Ident<'a>>,
    pub collation: Option<Ident<'a>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MySqlCreateView<'a> {
    /// `UNDEFINED`, `MERGE` or `TEMPTABLE`.
    pub algorithm: Option<Cow<'a, str>>,
    pub definer: Option<Grantee<'a>>,
    /// `DEFINER` or `INVOKER`.
    pub security: Option<Cow<'a, str>>,
    pub view: Box<CreateView<'a, Builtin>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MySqlCreateTrigger<'a> {
    pub definer: Option<Grantee<'a>>,
    pub if_not_exists: bool,
    pub name: ObjectName<'a>,
    /// `BEFORE` or `AFTER`.
    pub period: Cow<'a, str>,
    /// `INSERT`, `UPDATE` or `DELETE`.
    pub event: Cow<'a, str>,
    pub table: ObjectName<'a>,
    /// `FOLLOWS` or `PRECEDES` and the other trigger.
    pub order: Option<(Cow<'a, str>, ObjectName<'a>)>,
    pub body: MySqlTriggerBody<'a>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum MySqlTriggerBody<'a> {
    Statement(Box<Statement<'a, Builtin>>),
    /// `BEGIN statement; ... END`
    Block(Vec<Statement<'a, Builtin>>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum MySqlTableFactor<'a> {
    /// `name PARTITION (a, b)`
    Partitioned {
        name: ObjectName<'a>,
        partitions: Vec<Ident<'a>>,
    },
    /// A table followed by index hints.
    Hinted {
        table: TableRef<'a, Builtin>,
        hints: Vec<MySqlIndexHint<'a>>,
    },
    JsonTable(MySqlJsonTable<'a>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct MySqlIndexHint<'a> {
    /// `USE`, `IGNORE` or `FORCE`.
    pub verb: Cow<'a, str>,
    /// True if written `KEY` rather than `INDEX`.
    pub key: bool,
    /// `JOIN`, `ORDER BY` or `GROUP BY`.
    pub target: Option<Cow<'a, str>>,
    pub names: Vec<Ident<'a>>,
}

/// `JSON_TABLE(json, path COLUMNS(...))`
#[derive(Debug, Clone, PartialEq)]
pub struct MySqlJsonTable<'a> {
    pub json: Expr<'a, Builtin>,
    pub path: StringLiteral<'a>,
    pub columns: Vec<MySqlJsonColumn<'a>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum MySqlJsonColumn<'a> {
    /// `name FOR ORDINALITY`
    Ordinality(Ident<'a>),
    /// `NESTED [PATH] path COLUMNS (...)`
    Nested {
        path: StringLiteral<'a>,
        columns: Vec<MySqlJsonColumn<'a>>,
    },
    /// `name type [EXISTS] PATH path [on_empty] [on_error]`
    Named(Box<MySqlJsonNamedColumn<'a>>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct MySqlJsonNamedColumn<'a> {
    pub name: Ident<'a>,
    pub data_type: crate::ast::DataType<'a>,
    pub exists: bool,
    pub path: StringLiteral<'a>,
    pub on_empty: Option<MySqlJsonFallback<'a>>,
    pub on_error: Option<MySqlJsonFallback<'a>>,
}

/// What a `JSON_TABLE` column does when its path finds nothing or fails.
#[derive(Debug, Clone, PartialEq)]
pub enum MySqlJsonFallback<'a> {
    Null,
    Error,
    /// `DEFAULT 'value'`
    Default(StringLiteral<'a>),
}

/// Parses `RENAME TABLE a TO b, c TO d`.
fn parse_rename_table<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<MySqlStatement<'a>> {
    p.expect_keyword("RENAME")?;
    p.expect_keyword("TABLE")?;
    let renames = p.parse_comma_separated(|p| {
        let from = grammar::parse_object_name(p)?;
        p.expect_keyword("TO")?;
        let to = grammar::parse_object_name(p)?;
        Ok((from, to))
    })?;
    Ok(MySqlStatement::RenameTable(renames))
}

/// Parses a statement, including those only MySQL has.
pub fn parse_statement<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Statement<'a, Builtin>> {
    if grammar::at_alter_view(p) {
        return grammar::parse_alter_view(p, true)
            .map(|a| StatementKind::AlterView(Box::new(a)).into());
    }
    let next = p.peek_nth(1);
    if p.at_keyword("ANALYZE") && next.is_word("TABLE") {
        return grammar::parse_analyze(p);
    }
    if p.at_keyword("RENAME") && next.is_word("TABLE") {
        return parse_rename_table(p).map(mysql);
    }
    if p.at_keyword("LOCK") && next.is_word("TABLES") {
        p.advance();
        p.advance();
        let tables = p.parse_comma_separated(parse_locked_table)?;
        return Ok(mysql(MySqlStatement::LockTables(tables)));
    }
    if p.at_keyword("UNLOCK") && next.is_word("TABLES") {
        p.advance();
        p.advance();
        return Ok(mysql(MySqlStatement::UnlockTables));
    }
    if p.at_keyword("FLUSH") {
        p.advance();
        return parse_flush(p).map(|flush| mysql(MySqlStatement::Flush(Box::new(flush))));
    }
    if p.at_keyword("KILL")
        && (next.kind == TokenKind::Number || next.is_word("CONNECTION") || next.is_word("QUERY"))
    {
        p.advance();
        return parse_kill(p);
    }
    if p.at_keyword("REPLACE") {
        return parse_replace(p);
    }
    grammar::parse_statement(p)
}

fn parse_locked_table<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<MySqlLockedTable<'a>> {
    let table = grammar::parse_object_name(p)?;
    let bare_alias = p.at_ident()
        && !p.at_keyword("READ")
        && !p.at_keyword("WRITE")
        && !p.at_keyword("LOW_PRIORITY");
    let alias = if p.eat_keyword("AS") || bare_alias {
        Some(p.parse_ident()?)
    } else {
        None
    };
    let lock = if p.eat_keyword("READ") {
        MySqlLock::Read {
            local: p.eat_keyword("LOCAL"),
        }
    } else {
        let low_priority = p.eat_keyword("LOW_PRIORITY");
        p.expect_keyword("WRITE")?;
        MySqlLock::Write { low_priority }
    };
    Ok(MySqlLockedTable { table, alias, lock })
}

const FLUSH_LOG_KINDS: [(&str, &str); 6] = [
    ("BINARY", "BINARY LOGS"),
    ("ENGINE", "ENGINE LOGS"),
    ("ERROR", "ERROR LOGS"),
    ("GENERAL", "GENERAL LOGS"),
    ("SLOW", "SLOW LOGS"),
    ("RELAY", "RELAY LOGS"),
];

const FLUSH_WORDS: [&str; 6] = [
    "HOSTS",
    "PRIVILEGES",
    "OPTIMIZER_COSTS",
    "STATUS",
    "USER_RESOURCES",
    "LOGS",
];

fn parse_flush<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<MySqlFlush<'a>> {
    let location = if p.eat_keyword("NO_WRITE_TO_BINLOG") {
        Some("NO_WRITE_TO_BINLOG")
    } else if p.eat_keyword("LOCAL") {
        Some("LOCAL")
    } else {
        None
    };
    let mut flush = MySqlFlush {
        location: location.map(Cow::Borrowed),
        object: "".into(),
        channel: None,
        tables: Vec::new(),
        read_lock: false,
        export: false,
    };
    if p.eat_keyword("TABLES") {
        flush.object = "TABLES".into();
        if p.at_ident() && !p.at_keyword("WITH") && !p.at_keyword("FOR") {
            flush.tables = p.parse_comma_separated(grammar::parse_object_name)?;
        }
        if p.eat_keyword("WITH") {
            p.expect_keyword("READ")?;
            p.expect_keyword("LOCK")?;
            flush.read_lock = true;
        } else if p.eat_keyword("FOR") {
            p.expect_keyword("EXPORT")?;
            flush.export = true;
        }
        return Ok(flush);
    }
    if let Some(&(first, object)) = FLUSH_LOG_KINDS.iter().find(|(word, _)| p.at_keyword(word)) {
        p.advance();
        p.expect_keyword("LOGS")?;
        flush.object = object.into();
        if first == "RELAY" && p.eat_keyword("FOR") {
            p.expect_keyword("CHANNEL")?;
            flush.channel = Some(grammar::parse_object_name(p)?);
        }
        return Ok(flush);
    }
    let Some(word) = FLUSH_WORDS.iter().find(|word| p.at_keyword(word)) else {
        return Err(p.expected("something to flush"));
    };
    p.advance();
    flush.object = (*word).into();
    Ok(flush)
}

fn parse_kill<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Statement<'a, Builtin>> {
    let modifier = if p.eat_keyword("CONNECTION") {
        Some("CONNECTION")
    } else if p.eat_keyword("QUERY") {
        Some("QUERY")
    } else {
        None
    };
    let id = grammar::parse_number_text(p)?;
    Ok(mysql(MySqlStatement::Kill {
        modifier: modifier.map(Cow::Borrowed),
        id: id.into(),
    }))
}

fn parse_replace<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Statement<'a, Builtin>> {
    p.expect_keyword("REPLACE")?;
    let insert = grammar::parse_insert_after_verb(p)?;
    finish_insert(p, insert, true)
}

/// Parses `INSERT`, with what MySQL adds after its source.
pub fn parse_insert_statement<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Statement<'a, Builtin>> {
    let insert = D::parse_insert(p)?;
    finish_insert(p, insert, false)
}

fn finish_insert<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
    insert: Insert<'a, Builtin>,
    replace: bool,
) -> PResult<Statement<'a, Builtin>> {
    let alias = if p.eat_keyword("AS") {
        let name = p.parse_ident()?;
        let columns = if p.at_op("(") {
            grammar::parse_ident_list(p)?
        } else {
            Vec::new()
        };
        Some((name, columns))
    } else {
        None
    };
    let on_duplicate = if p.at_keyword("ON") && p.peek_nth(1).is_word("DUPLICATE") {
        p.advance();
        p.advance();
        p.expect_keyword("KEY")?;
        p.expect_keyword("UPDATE")?;
        p.parse_comma_separated(D::parse_assignment)?
    } else {
        Vec::new()
    };
    if !replace && alias.is_none() && on_duplicate.is_empty() {
        return Ok(StatementKind::Insert(Box::new(insert)).into());
    }
    Ok(mysql(MySqlStatement::Insert(Box::new(MySqlInsert {
        replace,
        insert: Box::new(insert),
        alias,
        on_duplicate,
    }))))
}

/// Parses a `CREATE` statement, taking those only MySQL has before the core.
pub fn parse_create<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Statement<'a, Builtin>> {
    let mut n = 1;
    if p.peek_nth(n).is_word("OR") {
        n += 2;
    }
    let or_replace = n == 3;
    let temporary = p.peek_nth(n).is_word("TEMPORARY");
    if temporary {
        n += 1;
    }
    let target = p.peek_nth(n);
    let plain = !or_replace && !temporary;
    let at_definer = target.is_word("ALGORITHM")
        || target.is_word("DEFINER")
        || (target.is_word("SQL") && p.peek_nth(n + 1).is_word("SECURITY"));
    if !(target.is_word("TABLE")
        || (plain && (target.is_word("DATABASE") || target.is_word("TRIGGER")))
        || (!temporary && at_definer))
    {
        return grammar::parse_create(p);
    }
    let flags = crate::ast::CreateFlags {
        or_replace,
        temporary,
    };
    (0..n).for_each(|_| {
        p.advance();
    });
    if p.eat_keyword("TABLE") {
        return parse_create_table(p, flags);
    }
    if p.eat_keyword("DATABASE") {
        return parse_create_database(p);
    }
    let (algorithm, definer, security) = parse_view_params(p)?;
    if p.eat_keyword("TRIGGER") {
        if algorithm.is_some() || security.is_some() {
            return Err(p.expected("VIEW"));
        }
        return parse_create_trigger(p, definer);
    }
    let view = Box::new(D::parse_create_view(p, flags)?);
    Ok(mysql(MySqlStatement::CreateView(Box::new(
        MySqlCreateView {
            algorithm: algorithm.map(Cow::Borrowed),
            definer,
            security: security.map(Cow::Borrowed),
            view,
        },
    ))))
}

type ViewParams<'a> = (
    Option<&'static str>,
    Option<Grantee<'a>>,
    Option<&'static str>,
);

fn parse_view_params<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<ViewParams<'a>> {
    let algorithm = if p.eat_keyword("ALGORITHM") {
        p.expect_op("=")?;
        let value = ["UNDEFINED", "MERGE", "TEMPTABLE"]
            .into_iter()
            .find(|word| p.eat_keyword(word));
        Some(value.ok_or_else(|| p.expected("UNDEFINED, MERGE or TEMPTABLE"))?)
    } else {
        None
    };
    let definer = if p.eat_keyword("DEFINER") {
        p.expect_op("=")?;
        Some(parse_user_host(p)?)
    } else {
        None
    };
    let security = if p.at_keyword("SQL") && p.peek_nth(1).is_word("SECURITY") {
        p.advance();
        p.advance();
        let value = ["DEFINER", "INVOKER"]
            .into_iter()
            .find(|word| p.eat_keyword(word));
        Some(value.ok_or_else(|| p.expected("DEFINER or INVOKER"))?)
    } else {
        None
    };
    Ok((algorithm, definer, security))
}

fn parse_create_trigger<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
    definer: Option<Grantee<'a>>,
) -> PResult<Statement<'a, Builtin>> {
    let if_not_exists = grammar::parse_if_not_exists(p)?;
    let name = grammar::parse_object_name(p)?;
    let period = if p.eat_keyword("BEFORE") {
        "BEFORE"
    } else {
        p.expect_keyword("AFTER")?;
        "AFTER"
    };
    let event = ["INSERT", "UPDATE", "DELETE"]
        .into_iter()
        .find(|word| p.eat_keyword(word))
        .ok_or_else(|| p.expected("INSERT, UPDATE or DELETE"))?;
    p.expect_keyword("ON")?;
    let table = grammar::parse_object_name(p)?;
    p.expect_keyword("FOR")?;
    p.expect_keyword("EACH")?;
    p.expect_keyword("ROW")?;
    let order = if p.eat_keyword("FOLLOWS") {
        Some(("FOLLOWS", grammar::parse_object_name(p)?))
    } else if p.eat_keyword("PRECEDES") {
        Some(("PRECEDES", grammar::parse_object_name(p)?))
    } else {
        None
    };
    let body = if p.eat_keyword("BEGIN") {
        let mut statements = Vec::new();
        while !p.at_keyword("END") {
            statements.push(p.nested(Parser::parse_statement)?);
            p.expect_op(";")?;
        }
        p.advance();
        MySqlTriggerBody::Block(statements)
    } else {
        let statement = p.nested(Parser::parse_statement)?;
        MySqlTriggerBody::Statement(Box::new(statement))
    };
    Ok(mysql(MySqlStatement::CreateTrigger(Box::new(
        MySqlCreateTrigger {
            definer,
            if_not_exists,
            name,
            period: period.into(),
            event: event.into(),
            table,
            order: order.map(|(kind, name)| (kind.into(), name)),
            body,
        },
    ))))
}

fn parse_create_table<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
    flags: crate::ast::CreateFlags,
) -> PResult<Statement<'a, Builtin>> {
    let if_not_exists = grammar::parse_if_not_exists(p)?;
    let name = grammar::parse_object_name(p)?;
    let (columns, constraints) = if p.at_op("(") {
        grammar::parse_columns(p)?
    } else {
        (Vec::new(), Vec::new())
    };
    let like = if columns.is_empty() && p.eat_keyword("LIKE") {
        Some(MySqlTableOption::Like(grammar::parse_object_name(p)?))
    } else {
        None
    };
    let mut options = parse_table_options(p)?;
    options.extend(like);
    let query = if p.eat_keyword("AS") || grammar::query_starts_at(p, 0) {
        Some(Box::new(D::parse_create_table_query(p)?))
    } else {
        None
    };
    if columns.is_empty() && query.is_none() && options.is_empty() {
        return Err(p.expected("column definitions or AS"));
    }
    let table = Box::new(CreateTable {
        or_replace: flags.or_replace,
        temporary: flags.temporary,
        name,
        if_not_exists,
        columns,
        constraints,
        query,
    });
    if options.is_empty() {
        return Ok(StatementKind::CreateTable(table).into());
    }
    Ok(mysql(MySqlStatement::CreateTable(Box::new(
        MySqlCreateTable { table, options },
    ))))
}

/// Option keywords that take a single value.
const TABLE_OPTION_KEYS: [(&[&str], &str); 28] = [
    (&["DEFAULT", "CHARSET"], "DEFAULT CHARSET"),
    (&["DEFAULT", "CHARACTER", "SET"], "DEFAULT CHARACTER SET"),
    (&["DEFAULT", "COLLATE"], "DEFAULT COLLATE"),
    (&["CHARACTER", "SET"], "CHARACTER SET"),
    (&["CHARSET"], "CHARSET"),
    (&["COLLATE"], "COLLATE"),
    (&["DATA", "DIRECTORY"], "DATA DIRECTORY"),
    (&["INDEX", "DIRECTORY"], "INDEX DIRECTORY"),
    (&["KEY_BLOCK_SIZE"], "KEY_BLOCK_SIZE"),
    (&["ROW_FORMAT"], "ROW_FORMAT"),
    (&["PACK_KEYS"], "PACK_KEYS"),
    (&["STATS_AUTO_RECALC"], "STATS_AUTO_RECALC"),
    (&["STATS_PERSISTENT"], "STATS_PERSISTENT"),
    (&["STATS_SAMPLE_PAGES"], "STATS_SAMPLE_PAGES"),
    (&["DELAY_KEY_WRITE"], "DELAY_KEY_WRITE"),
    (&["COMPRESSION"], "COMPRESSION"),
    (&["ENCRYPTION"], "ENCRYPTION"),
    (&["MAX_ROWS"], "MAX_ROWS"),
    (&["MIN_ROWS"], "MIN_ROWS"),
    (&["AUTOEXTEND_SIZE"], "AUTOEXTEND_SIZE"),
    (&["AVG_ROW_LENGTH"], "AVG_ROW_LENGTH"),
    (&["CHECKSUM"], "CHECKSUM"),
    (&["CONNECTION"], "CONNECTION"),
    (&["ENGINE_ATTRIBUTE"], "ENGINE_ATTRIBUTE"),
    (&["PASSWORD"], "PASSWORD"),
    (
        &["SECONDARY_ENGINE_ATTRIBUTE"],
        "SECONDARY_ENGINE_ATTRIBUTE",
    ),
    (&["INSERT_METHOD"], "INSERT_METHOD"),
    (&["AUTO_INCREMENT"], "AUTO_INCREMENT"),
];

fn parse_table_options<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Vec<MySqlTableOption<'a>>> {
    let mut options = Vec::new();
    while let Some(option) = parse_table_option(p)? {
        options.push(option);
        p.eat_op(",");
    }
    Ok(options)
}

fn parse_table_option<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Option<MySqlTableOption<'a>>> {
    let next = p.peek_nth(1);
    if p.at_keyword("START") && next.is_word("TRANSACTION") {
        p.advance();
        p.advance();
        return Ok(Some(MySqlTableOption::StartTransaction));
    }
    if p.at_keyword("COMMENT") {
        p.advance();
        let equals = p.eat_op("=");
        let token = p.expect_kind(TokenKind::String, "a string")?;
        return Ok(Some(MySqlTableOption::Comment {
            equals,
            text: D::string_literal(token),
        }));
    }
    if p.at_keyword("ENGINE") {
        p.advance();
        p.eat_op("=");
        return Ok(Some(MySqlTableOption::Engine(p.parse_name_part()?)));
    }
    if p.at_keyword("TABLESPACE") {
        p.advance();
        p.eat_op("=");
        let name = p.parse_name_part()?;
        let storage = if p.eat_keyword("STORAGE") {
            p.eat_op("=");
            if p.eat_keyword("DISK") {
                Some("DISK")
            } else {
                p.expect_keyword("MEMORY")?;
                Some("MEMORY")
            }
        } else {
            None
        };
        return Ok(Some(MySqlTableOption::Tablespace {
            name,
            storage: storage.map(Cow::Borrowed),
        }));
    }
    if p.at_keyword("UNION") && (next.is_op("=") || next.is_op("(")) {
        p.advance();
        p.eat_op("=");
        return Ok(Some(MySqlTableOption::Union(grammar::parse_ident_list(p)?)));
    }
    for (words, name) in TABLE_OPTION_KEYS {
        if words
            .iter()
            .enumerate()
            .all(|(i, word)| p.peek_nth(i).is_word(word))
        {
            for _ in words {
                p.advance();
            }
            p.eat_op("=");
            let value = p.spanned_expr(parse_option_value)?;
            return Ok(Some(MySqlTableOption::KeyValue {
                name: name.into(),
                value,
            }));
        }
    }
    Ok(None)
}

/// Parses an option's value: a number, a string or a name.
fn parse_option_value<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Expr<'a, Builtin>> {
    let token = p.peek();
    match token.kind {
        TokenKind::Number => {
            p.advance();
            Ok(ExprKind::Literal(Literal::Number(token.text.into())).into())
        }
        TokenKind::String => {
            p.advance();
            Ok(ExprKind::Literal(Literal::String(D::string_literal(token))).into())
        }
        TokenKind::Word | TokenKind::QuotedIdent => p
            .parse_name_part()
            .map(|ident| ExprKind::Ident(ident).into()),
        _ => Err(p.expected("an option value")),
    }
}

fn parse_create_database<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Statement<'a, Builtin>> {
    let if_not_exists = grammar::parse_if_not_exists(p)?;
    let name = grammar::parse_object_name(p)?;
    let (mut charset, mut collation) = (None, None);
    loop {
        let start = p.peek();
        let _ = p.eat_keyword("DEFAULT");
        let at_charset =
            p.at_keyword("CHARSET") || (p.at_keyword("CHARACTER") && p.peek_nth(1).is_word("SET"));
        let target = if at_charset && charset.is_none() {
            if !p.eat_keyword("CHARSET") {
                p.advance();
                p.advance();
            }
            &mut charset
        } else if p.at_keyword("COLLATE") && collation.is_none() {
            p.advance();
            &mut collation
        } else if p.peek().span == start.span {
            break;
        } else {
            return Err(p.expected("CHARACTER SET or COLLATE"));
        };
        p.eat_op("=");
        let token = p.peek();
        *target = Some(if token.kind == TokenKind::String {
            string_ident(p.advance())
        } else {
            p.parse_name_part()?
        });
    }
    if charset.is_none() && collation.is_none() {
        return Ok(
            StatementKind::CreateDatabase(Box::new(crate::ast::CreateDatabase {
                if_not_exists,
                name,
            }))
            .into(),
        );
    }
    Ok(mysql(MySqlStatement::CreateDatabase(Box::new(
        MySqlCreateDatabase {
            if_not_exists,
            name,
            charset,
            collation,
        },
    ))))
}

/// Parses `(key_part, ...)`.
fn parse_key_parts<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Vec<IndexColumn<'a, D::Ext>>> {
    p.expect_op("(")?;
    let columns = p.parse_comma_separated(D::parse_index_column)?;
    p.expect_op(")")?;
    Ok(columns)
}

fn parse_key_options<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Vec<KeyOption<'a>>> {
    let mut options = Vec::new();
    loop {
        if p.at_keyword("USING") {
            p.advance();
            options.push(KeyOption::Using(p.parse_ident()?));
        } else if p.at_keyword("COMMENT") {
            p.advance();
            let token = p.expect_kind(TokenKind::String, "a comment string")?;
            options.push(KeyOption::Comment(D::string_literal(token)));
        } else {
            return Ok(options);
        }
    }
}

/// Parses an optional index name, which is absent at `(` or `USING`.
fn parse_key_name<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Option<Ident<'a>>> {
    if p.at_op("(") || p.at_keyword("USING") {
        return Ok(None);
    }
    p.parse_name_part().map(Some)
}

/// True if `token` can start what comes after an index keyword.
fn after_index_keyword<'a, D: Dialect>(p: &Parser<'a, D>, n: usize) -> bool {
    let token = p.peek_nth(n);
    token.is_op("(")
        || token.is_word("USING")
        || (matches!(token.kind, TokenKind::Word | TokenKind::QuotedIdent)
            && (p.peek_nth(n + 1).is_op("(") || p.peek_nth(n + 1).is_word("USING")))
}

/// Parses `USING INDEX name` if it is here.
fn parse_using_index<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Option<Ident<'a>>> {
    if p.at_keyword("USING") && p.peek_nth(1).is_word("INDEX") {
        p.advance();
        p.advance();
        return p.parse_ident().map(Some);
    }
    Ok(None)
}

/// Parses `INCLUDE (columns)` if it is here.
fn parse_include<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Vec<Ident<'a>>> {
    if p.eat_keyword("INCLUDE") {
        return grammar::parse_ident_list(p);
    }
    Ok(Vec::new())
}

/// Builds a key, which is the core node when it only has plain columns.
fn finish_key<'a>(
    keyword: &'static str,
    name: Option<Ident<'a>>,
    using: Option<Ident<'a>>,
    columns: Vec<IndexColumn<'a, Builtin>>,
    options: Vec<KeyOption<'a>>,
    plain: impl FnOnce(Vec<Ident<'a>>) -> TableConstraintKind<'a, Builtin>,
) -> TableConstraintKind<'a, Builtin> {
    let is_plain = name.is_none()
        && using.is_none()
        && options.is_empty()
        && columns.iter().all(|column| {
            column.operator_class.is_none()
                && column.item.asc.is_none()
                && column.item.nulls_first.is_none()
                && matches!(column.item.expr.kind, ExprKind::Ident(_))
        });
    if is_plain {
        let names = columns
            .into_iter()
            .map(|column| match column.item.expr.kind {
                ExprKind::Ident(name) => name,
                _ => unreachable!("checked above"),
            })
            .collect();
        return plain(names);
    }
    TableConstraintKind::Key {
        keyword: keyword.into(),
        name,
        using,
        columns,
        options,
    }
}

/// Parses a table constraint or index definition, or returns `None` if this
/// element is a column. The keyword `CONSTRAINT` can come without a name, and
/// `KEY` and `INDEX` are reserved in MySQL so cannot start a column.
pub fn parse_table_constraint<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Option<TableConstraint<'a, Builtin>>> {
    let mut name = None;
    if p.at_keyword("CONSTRAINT") {
        let next = p.peek_nth(1);
        let keyword_next = next.is_word("CHECK")
            || next.is_word("PRIMARY")
            || next.is_word("UNIQUE")
            || next.is_word("FOREIGN");
        p.advance();
        if !keyword_next {
            name = Some(p.parse_ident()?);
        }
    }
    let next = p.peek_nth(1);
    let mut include = Vec::new();
    let mut nulls_distinct = None;
    let kind = if p.at_keyword("PRIMARY") && next.is_word("KEY") {
        p.advance();
        p.advance();
        if let Some(index) = parse_using_index(p)? {
            TableConstraintKind::PrimaryKeyUsingIndex(index)
        } else {
            let index = parse_key_name(p)?;
            let using = grammar::parse_optional_using(p)?;
            let columns = parse_key_parts(p)?;
            include = parse_include(p)?;
            let options = parse_key_options(p)?;
            finish_key(
                "PRIMARY KEY",
                index,
                using,
                columns,
                options,
                TableConstraintKind::PrimaryKey,
            )
        }
    } else if p.at_keyword("UNIQUE")
        && (next.is_op("(")
            || next.is_word("KEY")
            || next.is_word("INDEX")
            || next.is_word("NULLS")
            || after_index_keyword(p, 1))
    {
        p.advance();
        if let Some(index) = parse_using_index(p)? {
            TableConstraintKind::UniqueUsingIndex(index)
        } else {
            let (keyword, had_keyword) = if p.eat_keyword("KEY") {
                ("UNIQUE KEY", true)
            } else if p.eat_keyword("INDEX") {
                ("UNIQUE INDEX", true)
            } else {
                ("UNIQUE", false)
            };
            nulls_distinct = grammar::parse_nulls_distinct(p)?;
            let index = parse_key_name(p)?;
            let using = grammar::parse_optional_using(p)?;
            let columns = parse_key_parts(p)?;
            include = parse_include(p)?;
            let options = parse_key_options(p)?;
            if had_keyword || index.is_some() || using.is_some() {
                TableConstraintKind::Key {
                    keyword: keyword.into(),
                    name: index,
                    using,
                    columns,
                    options,
                }
            } else {
                finish_key(
                    keyword,
                    index,
                    using,
                    columns,
                    options,
                    TableConstraintKind::Unique,
                )
            }
        }
    } else if p.at_keyword("FOREIGN") && next.is_word("KEY") {
        p.advance();
        p.advance();
        let index_name = if p.at_op("(") {
            None
        } else {
            Some(p.parse_ident()?)
        };
        let columns = grammar::parse_ident_list(p)?;
        p.expect_keyword("REFERENCES")?;
        let references = grammar::parse_foreign_key_ref(p)?;
        TableConstraintKind::ForeignKey {
            index_name,
            columns,
            references,
        }
    } else if p.at_keyword("CHECK") && next.is_op("(") {
        p.advance();
        p.advance();
        let expr = p.parse_expr()?;
        p.expect_op(")")?;
        TableConstraintKind::Check(expr)
    } else if (p.at_keyword("KEY") || p.at_keyword("INDEX"))
        && name.is_none()
        && after_index_keyword(p, 1)
    {
        let keyword = if p.at_keyword("KEY") { "KEY" } else { "INDEX" };
        p.advance();
        let index = parse_key_name(p)?;
        let using = grammar::parse_optional_using(p)?;
        let columns = parse_key_parts(p)?;
        let options = parse_key_options(p)?;
        TableConstraintKind::Key {
            keyword: keyword.into(),
            name: index,
            using,
            columns,
            options,
        }
    } else if (p.at_keyword("FULLTEXT") || p.at_keyword("SPATIAL"))
        && name.is_none()
        && (next.is_word("KEY") || next.is_word("INDEX") || after_index_keyword(p, 1))
    {
        let fulltext = p.at_keyword("FULLTEXT");
        p.advance();
        let keyword = match (fulltext, p.eat_keyword("KEY"), p.eat_keyword("INDEX")) {
            (true, true, _) => "FULLTEXT KEY",
            (true, _, true) => "FULLTEXT INDEX",
            (true, _, _) => "FULLTEXT",
            (false, true, _) => "SPATIAL KEY",
            (false, _, true) => "SPATIAL INDEX",
            (false, _, _) => "SPATIAL",
        };
        let index = parse_key_name(p)?;
        let columns = parse_key_parts(p)?;
        TableConstraintKind::Key {
            keyword: keyword.into(),
            name: index,
            using: None,
            columns,
            options: Vec::new(),
        }
    } else if name.is_some() {
        return Err(p.expected("a table constraint"));
    } else {
        return Ok(None);
    };
    let mut attributes = Vec::new();
    while let Some(attribute) = grammar::parse_constraint_attribute(p)? {
        attributes.push(attribute);
    }
    grammar::check_constraint_attributes(p, attributes.iter().copied())?;
    Ok(Some(TableConstraint {
        name,
        kind,
        include,
        nulls_distinct,
        attributes,
    }))
}

/// Parses the arguments of `JSON_TABLE`, with the current token on `(`.
pub fn parse_json_table<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<MySqlTableFactor<'a>> {
    p.expect_op("(")?;
    let json = p.parse_expr()?;
    p.expect_op(",")?;
    let path = parse_path(p)?;
    p.expect_keyword("COLUMNS")?;
    let columns = parse_json_columns(p)?;
    p.expect_op(")")?;
    Ok(MySqlTableFactor::JsonTable(MySqlJsonTable {
        json,
        path,
        columns,
    }))
}

fn parse_json_columns<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Vec<MySqlJsonColumn<'a>>> {
    p.expect_op("(")?;
    let columns = p.parse_comma_separated(parse_json_column)?;
    p.expect_op(")")?;
    Ok(columns)
}

fn parse_path<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<StringLiteral<'a>> {
    let token = p.expect_kind(TokenKind::String, "a JSON path string")?;
    Ok(D::string_literal(token))
}

fn parse_json_column<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<MySqlJsonColumn<'a>> {
    if p.eat_keyword("NESTED") {
        p.eat_keyword("PATH");
        let path = parse_path(p)?;
        p.expect_keyword("COLUMNS")?;
        let columns = p.nested(parse_json_columns)?;
        return Ok(MySqlJsonColumn::Nested { path, columns });
    }
    let name = p.parse_ident()?;
    if p.eat_keyword("FOR") {
        p.expect_keyword("ORDINALITY")?;
        return Ok(MySqlJsonColumn::Ordinality(name));
    }
    let data_type = D::parse_data_type(p)?;
    let exists = p.eat_keyword("EXISTS");
    p.expect_keyword("PATH")?;
    let path = parse_path(p)?;
    let (mut on_empty, mut on_error) = (None, None);
    while let Some(fallback) = parse_json_fallback(p)? {
        p.expect_keyword("ON")?;
        if p.eat_keyword("EMPTY") {
            on_empty = Some(fallback);
        } else {
            p.expect_keyword("ERROR")?;
            on_error = Some(fallback);
        }
    }
    Ok(MySqlJsonColumn::Named(Box::new(MySqlJsonNamedColumn {
        name,
        data_type,
        exists,
        path,
        on_empty,
        on_error,
    })))
}

fn parse_json_fallback<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<Option<MySqlJsonFallback<'a>>> {
    if p.eat_keyword("NULL") {
        Ok(Some(MySqlJsonFallback::Null))
    } else if p.eat_keyword("ERROR") {
        Ok(Some(MySqlJsonFallback::Error))
    } else if p.eat_keyword("DEFAULT") {
        parse_path(p).map(|value| Some(MySqlJsonFallback::Default(value)))
    } else {
        Ok(None)
    }
}

impl fmt::Display for MySqlStatement<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RenameTable(renames) => {
                f.write_str("RENAME TABLE ")?;
                for (i, (from, to)) in renames.iter().enumerate() {
                    if i > 0 {
                        f.write_str(", ")?;
                    }
                    write!(f, "{from} TO {to}")?;
                }
                Ok(())
            }
            Self::LockTables(tables) => write!(f, "LOCK TABLES {}", comma(tables)),
            Self::UnlockTables => f.write_str("UNLOCK TABLES"),
            Self::Flush(flush) => flush.fmt(f),
            Self::Kill { modifier, id } => match modifier {
                Some(modifier) => write!(f, "KILL {modifier} {id}"),
                None => write!(f, "KILL {id}"),
            },
            Self::Insert(insert) => insert.fmt(f),
            Self::CreateTable(table) => table.fmt(f),
            Self::CreateDatabase(database) => database.fmt(f),
            Self::CreateView(view) => view.fmt(f),
            Self::CreateTrigger(trigger) => trigger.fmt(f),
        }
    }
}

impl fmt::Display for MySqlLockedTable<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.table.fmt(f)?;
        if let Some(alias) = &self.alias {
            write!(f, " AS {alias}")?;
        }
        f.write_str(match self.lock {
            MySqlLock::Read { local: false } => " READ",
            MySqlLock::Read { local: true } => " READ LOCAL",
            MySqlLock::Write {
                low_priority: false,
            } => " WRITE",
            MySqlLock::Write { low_priority: true } => " LOW_PRIORITY WRITE",
        })
    }
}

impl fmt::Display for MySqlFlush<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("FLUSH ")?;
        if let Some(ref location) = self.location {
            write!(f, "{location} ")?;
        }
        f.write_str(&self.object)?;
        if let Some(channel) = &self.channel {
            write!(f, " FOR CHANNEL {channel}")?;
        }
        if !self.tables.is_empty() {
            write!(f, " {}", comma(&self.tables))?;
        }
        if self.read_lock {
            f.write_str(" WITH READ LOCK")?;
        } else if self.export {
            f.write_str(" FOR EXPORT")?;
        }
        Ok(())
    }
}

impl fmt::Display for MySqlInsert<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(if self.replace { "REPLACE " } else { "INSERT " })?;
        write_insert_body(f, &self.insert)?;
        if let Some((name, columns)) = &self.alias {
            write!(f, " AS {name}")?;
            if !columns.is_empty() {
                write!(f, " ({})", comma(columns))?;
            }
        }
        if !self.on_duplicate.is_empty() {
            write!(f, " ON DUPLICATE KEY UPDATE {}", comma(&self.on_duplicate))?;
        }
        Ok(())
    }
}

impl fmt::Display for MySqlTableOption<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::KeyValue { name, value } => write!(f, "{name} = {value}"),
            Self::Comment { equals, text } => {
                let equals = if *equals { "= " } else { "" };
                write!(f, "COMMENT {equals}{}", text)
            }
            Self::Engine(name) => write!(f, "ENGINE = {name}"),
            Self::Like(source) => write!(f, "LIKE {source}"),
            Self::Tablespace { name, storage } => {
                write!(f, "TABLESPACE {name}")?;
                match storage {
                    Some(storage) => write!(f, " STORAGE {storage}"),
                    None => Ok(()),
                }
            }
            Self::Union(tables) => write!(f, "UNION = ({})", comma(tables)),
            Self::StartTransaction => f.write_str("START TRANSACTION"),
        }
    }
}

impl fmt::Display for MySqlCreateTable<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let table = &self.table;
        write_create_flags(f, table.or_replace, table.temporary)?;
        f.write_str("TABLE ")?;
        write_if_not_exists(f, table.if_not_exists)?;
        table.name.fmt(f)?;
        if !table.columns.is_empty() || !table.constraints.is_empty() {
            write_columns(f, &table.columns, &table.constraints)?;
        }
        for option in &self.options {
            write!(f, " {option}")?;
        }
        match &table.query {
            Some(query) => write!(f, " AS {query}"),
            None => Ok(()),
        }
    }
}

impl fmt::Display for MySqlCreateDatabase<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CREATE DATABASE ")?;
        write_if_not_exists(f, self.if_not_exists)?;
        self.name.fmt(f)?;
        if let Some(charset) = &self.charset {
            write!(f, " DEFAULT CHARACTER SET {charset}")?;
        }
        if let Some(collation) = &self.collation {
            write!(f, " DEFAULT COLLATE {collation}")?;
        }
        Ok(())
    }
}

fn write_view_params(
    f: &mut fmt::Formatter<'_>,
    algorithm: Option<&str>,
    definer: Option<&Grantee<'_>>,
    security: Option<&str>,
) -> fmt::Result {
    if let Some(algorithm) = algorithm {
        write!(f, "ALGORITHM = {algorithm} ")?;
    }
    if let Some(definer) = definer {
        write!(f, "DEFINER = {definer} ")?;
    }
    if let Some(security) = security {
        write!(f, "SQL SECURITY {security} ")?;
    }
    Ok(())
}

impl fmt::Display for MySqlCreateView<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let view = &self.view;
        write_create_flags(f, view.or_replace, view.temporary)?;
        write_view_params(
            f,
            self.algorithm.as_deref(),
            self.definer.as_ref(),
            self.security.as_deref(),
        )?;
        if view.materialized {
            f.write_str("MATERIALIZED ")?;
        }
        f.write_str("VIEW ")?;
        write_if_not_exists(f, view.if_not_exists)?;
        view.name.fmt(f)?;
        if !view.columns.is_empty() {
            write!(f, " ({})", comma(&view.columns))?;
        }
        crate::display::write_view_query(f, &view.query)
    }
}

impl fmt::Display for MySqlCreateTrigger<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CREATE ")?;
        write_view_params(f, None, self.definer.as_ref(), None)?;
        f.write_str("TRIGGER ")?;
        write_if_not_exists(f, self.if_not_exists)?;
        write!(
            f,
            "{} {} {} ON {} FOR EACH ROW",
            self.name, self.period, self.event, self.table
        )?;
        if let Some((word, other)) = &self.order {
            write!(f, " {word} {other}")?;
        }
        match &self.body {
            MySqlTriggerBody::Statement(statement) => write!(f, " {statement}"),
            MySqlTriggerBody::Block(statements) => {
                f.write_str(" BEGIN ")?;
                for statement in statements {
                    write!(f, "{statement}; ")?;
                }
                f.write_str("END")
            }
        }
    }
}

impl fmt::Display for MySqlTableFactor<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Partitioned { name, partitions } => {
                write!(f, "{name} PARTITION ({})", comma(partitions))
            }
            Self::Hinted { table, hints } => {
                table.fmt(f)?;
                for hint in hints {
                    write!(f, " {hint}")?;
                }
                Ok(())
            }
            Self::JsonTable(table) => table.fmt(f),
        }
    }
}

impl fmt::Display for MySqlIndexHint<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} {}",
            self.verb,
            if self.key { "KEY" } else { "INDEX" }
        )?;
        if let Some(ref target) = self.target {
            write!(f, " FOR {target}")?;
        }
        write!(f, " ({})", comma(&self.names))
    }
}

impl fmt::Display for MySqlJsonTable<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if !f.alternate() {
            return write!(
                f,
                "JSON_TABLE({}, {} COLUMNS({}))",
                self.json,
                self.path,
                comma(&self.columns)
            );
        }
        f.write_str("JSON_TABLE(")?;
        NewLine.fmt(f)?;
        Indent(JsonTableArguments(self)).fmt(f)?;
        NewLine.fmt(f)?;
        f.write_str(")")
    }
}

/// The arguments of `JSON_TABLE`, in its multi-line form.
struct JsonTableArguments<'s, 'a>(&'s MySqlJsonTable<'a>);

impl fmt::Display for JsonTableArguments<'_, '_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{},", self.0.json)?;
        SpaceOrNewline.fmt(f)?;
        write!(f, "{} COLUMNS (", self.0.path)?;
        NewLine.fmt(f)?;
        Indent(CommaLines(&self.0.columns)).fmt(f)?;
        NewLine.fmt(f)?;
        f.write_str(")")
    }
}

impl fmt::Display for MySqlJsonColumn<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ordinality(name) => write!(f, "{name} FOR ORDINALITY"),
            Self::Nested { path, columns } => {
                write!(f, "NESTED PATH {} COLUMNS ({})", path, comma(columns))
            }
            Self::Named(column) => column.fmt(f),
        }
    }
}

impl fmt::Display for MySqlJsonNamedColumn<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.name, self.data_type)?;
        if self.exists {
            f.write_str(" EXISTS")?;
        }
        write!(f, " PATH {}", self.path)?;
        if let Some(fallback) = &self.on_empty {
            write!(f, " {fallback} ON EMPTY")?;
        }
        if let Some(fallback) = &self.on_error {
            write!(f, " {fallback} ON ERROR")?;
        }
        Ok(())
    }
}

impl fmt::Display for MySqlJsonFallback<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Null => f.write_str("NULL"),
            Self::Error => f.write_str("ERROR"),
            Self::Default(value) => write!(f, "DEFAULT {}", value),
        }
    }
}

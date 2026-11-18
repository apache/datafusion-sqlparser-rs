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

//! Snowflake's table and database options, scripting and `DECLARE`.

use std::borrow::Cow;
use std::fmt::{self, Display};

use super::{KeyValue, SnowflakeStatement, parse_key_value, parse_named_call, write_value};
use crate::PResult;
use crate::ast::{
    AlterTable, AngleArguments, ColumnDef, ColumnOption, CreateDatabase, CreateTable, CreateView,
    DataType, Expr, ExprKind, FunctionArg, Ident, IdentityParams, Literal, ObjectName, Query,
    Statement, StatementKind, StringLiteral, StructField, TableConstraint, Tag,
};
use crate::dialect::builtin::BuiltinStatement;
use crate::dialect::{Builtin, Dialect};
use crate::display::write_columns;
use crate::grammar;
use crate::lexer::TokenKind;
use crate::parser::Parser;

/// A `CREATE TABLE` using syntax the core statement cannot hold.
#[derive(Debug, Clone, PartialEq)]
pub struct SnowflakeCreateTable<'a> {
    pub or_replace: bool,
    /// `LOCAL` or `GLOBAL`.
    pub scope: Option<Cow<'a, str>>,
    pub dynamic: bool,
    /// `TEMPORARY`, `VOLATILE`, `TRANSIENT` or `ICEBERG`.
    pub lifetime: Option<Cow<'a, str>>,
    pub if_not_exists: bool,
    pub name: ObjectName<'a>,
    pub columns: Vec<ColumnDef<'a, Builtin>>,
    pub constraints: Vec<TableConstraint<'a, Builtin>>,
    /// In the order written, apart from the ones that render first.
    pub clauses: Vec<TableClause<'a>>,
    pub query: Option<Box<Query<'a, Builtin>>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TableClause<'a> {
    CopyGrants,
    /// `COMMENT [=] 'text'`
    Comment {
        equals: bool,
        text: StringLiteral<'a>,
    },
    Clone(ObjectName<'a>),
    Like(ObjectName<'a>),
    /// `AT(...)` or `BEFORE(...)` after `CLONE`.
    Version(Expr<'a, Builtin>),
    ClusterBy(Vec<Expr<'a, Builtin>>),
    Option(KeyValue<'a>),
    /// `WITH kind name [ON (columns)]`
    Policy {
        kind: Cow<'a, str>,
        name: ObjectName<'a>,
        on: Vec<Ident<'a>>,
    },
    Tags(Vec<Tag<'a>>),
    OnCommit(Cow<'a, str>),
    RequireUser,
}

impl TableClause<'_> {
    fn renders_first(&self) -> bool {
        matches!(
            self,
            TableClause::Clone(_) | TableClause::Like(_) | TableClause::Version(_)
        )
    }

    fn names_a_source(&self) -> bool {
        matches!(self, TableClause::Clone(_) | TableClause::Like(_))
    }
}

/// A `CREATE DATABASE` using syntax the core statement cannot hold.
#[derive(Debug, Clone, PartialEq)]
pub struct SnowflakeCreateDatabase<'a> {
    pub or_replace: bool,
    pub transient: bool,
    pub if_not_exists: bool,
    pub name: ObjectName<'a>,
    pub clauses: Vec<DatabaseClause<'a>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum DatabaseClause<'a> {
    Clone(ObjectName<'a>),
    Option(KeyValue<'a>),
    Tags(Vec<Tag<'a>>),
    /// `WITH CONTACT (purpose = contact, ...)`
    Contacts(Vec<(Ident<'a>, Ident<'a>)>),
}

/// A `CREATE VIEW` using syntax the core statement cannot hold.
#[derive(Debug, Clone, PartialEq)]
pub struct SnowflakeCreateView<'a> {
    pub or_replace: bool,
    pub secure: bool,
    pub materialized: bool,
    pub temporary: bool,
    pub if_not_exists: bool,
    /// True if `IF NOT EXISTS` was written after the name.
    pub if_not_exists_after_name: bool,
    pub name: ObjectName<'a>,
    pub copy_grants: bool,
    pub columns: Vec<ViewColumn<'a>>,
    pub cluster_by: Vec<Expr<'a, Builtin>>,
    pub comment: Option<StringLiteral<'a>>,
    pub query: Box<Query<'a, Builtin>>,
}

/// A column of a view, with its options.
#[derive(Debug, Clone, PartialEq)]
pub struct ViewColumn<'a> {
    pub name: Ident<'a>,
    pub options: Vec<ColumnOption<'a, Builtin>>,
}

/// The arguments of `SEMANTIC_VIEW(...)`.
#[derive(Debug, Clone, PartialEq)]
pub struct SemanticView<'a> {
    pub name: ObjectName<'a>,
    pub dimensions: Vec<FunctionArg<'a, Builtin>>,
    pub metrics: Vec<FunctionArg<'a, Builtin>>,
    pub facts: Vec<FunctionArg<'a, Builtin>>,
    pub selection: Option<Expr<'a, Builtin>>,
}

/// `BEGIN ... [EXCEPTION ...] END`
#[derive(Debug, Clone, PartialEq)]
pub struct Block<'a> {
    pub statements: Vec<Statement<'a, Builtin>>,
    pub exception: Option<Vec<ExceptionWhen<'a>>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExceptionWhen<'a> {
    pub names: Vec<Ident<'a>>,
    pub statements: Vec<Statement<'a, Builtin>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum RaiseValue<'a> {
    Expr(Expr<'a, Builtin>),
    Message(Expr<'a, Builtin>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Declaration<'a> {
    pub name: Ident<'a>,
    pub kind: DeclarationKind<'a>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum DeclarationKind<'a> {
    Variable {
        data_type: Option<DataType<'a>>,
        value: Option<Assignment<'a>>,
    },
    ResultSet(Option<Assignment<'a>>),
    /// `CURSOR FOR query`
    CursorQuery(Box<Query<'a, Builtin>>),
    /// `CURSOR FOR expr`
    CursorExpr(Expr<'a, Builtin>),
    Exception(Option<Expr<'a, Builtin>>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Assignment<'a> {
    /// `DEFAULT expr`
    Default(Expr<'a, Builtin>),
    /// `:= expr`
    Walrus(Expr<'a, Builtin>),
}

/// The offset of the word after `CREATE` and its modifiers.
fn create_object_offset<D: Dialect<Ext = Builtin>>(p: &Parser<'_, D>) -> usize {
    let mut n = 1;
    if p.peek_nth(n).is_word("OR") {
        n += 2;
    }
    for words in [
        &["LOCAL", "GLOBAL"][..],
        &["DYNAMIC"],
        &["TEMP", "TEMPORARY", "VOLATILE", "TRANSIENT", "ICEBERG"],
    ] {
        if words.iter().any(|word| p.peek_nth(n).is_word(word)) {
            n += 1;
        }
    }
    n
}

pub fn at_create_table<D: Dialect<Ext = Builtin>>(p: &Parser<'_, D>) -> bool {
    p.at_keyword("CREATE") && (p.peek_nth(create_object_offset(p))).is_word("TABLE")
}

pub fn at_create_database<D: Dialect<Ext = Builtin>>(p: &Parser<'_, D>) -> bool {
    p.at_keyword("CREATE") && (p.peek_nth(create_object_offset(p))).is_word("DATABASE")
}

pub fn at_create_view<D: Dialect<Ext = Builtin>>(p: &Parser<'_, D>) -> bool {
    if !p.at_keyword("CREATE") {
        return false;
    }
    let mut n = 1;
    if p.peek_nth(n).is_word("OR") {
        n += 2;
    }
    for words in [
        &["SECURE"][..],
        &["TEMP", "TEMPORARY"],
        &["SECURE"],
        &["MATERIALIZED"],
    ] {
        if words.iter().any(|word| p.peek_nth(n).is_word(word)) {
            n += 1;
        }
    }
    p.peek_nth(n).is_word("VIEW")
}

fn first_of<D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'_, D>,
    words: &[&'static str],
) -> Option<&'static str> {
    words.iter().copied().find(|word| p.eat_keyword(word))
}

/// Parses a comment's text, which can be dollar-quoted.
pub fn parse_comment<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<StringLiteral<'a>> {
    match p.parse_prefix()?.kind {
        ExprKind::Literal(Literal::String(text)) => Ok(text),
        ExprKind::Literal(Literal::DollarQuoted { text, tag }) => {
            // Only a body with nothing to escape means the same in single quotes.
            let plain = !text.contains(['\'', '\\']);
            if !plain && !tag.is_empty() {
                return Err(p.expected("a comment in plain dollar quotes"));
            }
            Ok(StringLiteral {
                text,
                quote: if plain { '\'' } else { '$' },
                backslash_escapes: false,
                national: false,
                prefix: Default::default(),
                triple: false,
            })
        }
        _ => Err(p.expected("a string")),
    }
}

fn parse_tag<'a, D: Dialect<Ext = Builtin>>(p: &mut Parser<'a, D>) -> PResult<Tag<'a>> {
    let name = grammar::parse_object_name(p)?;
    p.expect_op("=")?;
    let token = p.expect_kind(TokenKind::String, "a string")?;
    Ok(Tag {
        name,
        value: D::string_literal(token),
    })
}

fn parse_tags<'a, D: Dialect<Ext = Builtin>>(p: &mut Parser<'a, D>) -> PResult<Vec<Tag<'a>>> {
    p.expect_op("(")?;
    let tags = p.parse_comma_separated(parse_tag)?;
    p.expect_op(")")?;
    Ok(tags)
}

/// Parses a policy's name and its `ON (columns)`.
fn parse_policy_on<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
    kind: &'static str,
) -> PResult<TableClause<'a>> {
    let name = grammar::parse_object_name(p)?;
    p.expect_keyword("ON")?;
    let on = grammar::parse_ident_list(p)?;
    Ok(TableClause::Policy {
        kind: kind.into(),
        name,
        on,
    })
}

fn parse_on_commit<D: Dialect<Ext = Builtin>>(p: &mut Parser<'_, D>) -> PResult<&'static str> {
    if p.eat_keyword("DROP") {
        return Ok("DROP");
    }
    let action = if p.eat_keyword("PRESERVE") {
        "PRESERVE ROWS"
    } else {
        p.expect_keyword("DELETE")?;
        "DELETE ROWS"
    };
    p.expect_keyword("ROWS")?;
    Ok(action)
}

const TABLE_OPTIONS: [&str; 14] = [
    "ENABLE_SCHEMA_EVOLUTION",
    "CHANGE_TRACKING",
    "DATA_RETENTION_TIME_IN_DAYS",
    "MAX_DATA_EXTENSION_TIME_IN_DAYS",
    "DEFAULT_DDL_COLLATION",
    "EXTERNAL_VOLUME",
    "CATALOG",
    "BASE_LOCATION",
    "CATALOG_SYNC",
    "STORAGE_SERIALIZATION_POLICY",
    "TARGET_LAG",
    "WAREHOUSE",
    "REFRESH_MODE",
    "INITIALIZE",
];

const DATABASE_OPTIONS: [&str; 11] = [
    "DATA_RETENTION_TIME_IN_DAYS",
    "MAX_DATA_EXTENSION_TIME_IN_DAYS",
    "EXTERNAL_VOLUME",
    "CATALOG",
    "REPLACE_INVALID_CHARACTERS",
    "DEFAULT_DDL_COLLATION",
    "STORAGE_SERIALIZATION_POLICY",
    "COMMENT",
    "CATALOG_SYNC",
    "CATALOG_SYNC_NAMESPACE_FLATTEN_DELIMITER",
    "CATALOG_SYNC_NAMESPACE_MODE",
];

/// True at `key =` for one of `keys`.
fn at_option<D: Dialect<Ext = Builtin>>(p: &Parser<'_, D>, keys: &[&str]) -> bool {
    p.peek_nth(1).is_op("=") && keys.iter().any(|key| p.at_keyword(key))
}

/// Parses one clause, or returns `None` at the end of them.
fn parse_table_clause<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Option<TableClause<'a>>> {
    let clause = if p.eat_keyword("COPY") {
        p.expect_keyword("GRANTS")?;
        TableClause::CopyGrants
    } else if p.eat_keyword("COMMENT") {
        let equals = p.eat_op("=");
        TableClause::Comment {
            equals,
            text: parse_comment(p)?,
        }
    } else if p.eat_keyword("CLONE") {
        TableClause::Clone(grammar::parse_object_name(p)?)
    } else if p.eat_keyword("LIKE") {
        TableClause::Like(grammar::parse_object_name(p)?)
    } else if (p.at_keyword("AT") || p.at_keyword("BEFORE")) && p.peek_nth(1).is_op("(") {
        TableClause::Version(parse_named_call(p)?)
    } else if p.eat_keyword("CLUSTER") {
        p.expect_keyword("BY")?;
        p.expect_op("(")?;
        let exprs = p.parse_comma_separated(Parser::parse_expr)?;
        p.expect_op(")")?;
        TableClause::ClusterBy(exprs)
    } else if p.eat_keyword("AGGREGATION") {
        p.expect_keyword("POLICY")?;
        TableClause::Policy {
            kind: "AGGREGATION POLICY".into(),
            name: grammar::parse_object_name(p)?,
            on: Vec::new(),
        }
    } else if p.at_keyword("ROW") && p.peek_nth(1).is_word("ACCESS") {
        p.advance();
        p.expect_keywords(&["ACCESS", "POLICY"])?;
        parse_policy_on(p, "ROW ACCESS POLICY")?
    } else if p.at_keyword("STORAGE") && p.peek_nth(1).is_word("LIFECYCLE") {
        p.advance();
        p.expect_keywords(&["LIFECYCLE", "POLICY"])?;
        parse_policy_on(p, "STORAGE LIFECYCLE POLICY")?
    } else if p.eat_keyword("TAG") {
        TableClause::Tags(parse_tags(p)?)
    } else if p.at_keyword("ON") && p.peek_nth(1).is_word("COMMIT") {
        p.advance();
        p.advance();
        TableClause::OnCommit(parse_on_commit(p)?.into())
    } else if p.at_keyword("REQUIRE") && p.peek_nth(1).is_word("USER") {
        p.advance();
        p.advance();
        TableClause::RequireUser
    } else if at_option(p, &TABLE_OPTIONS) {
        TableClause::Option(parse_key_value(p)?)
    } else {
        return Ok(None);
    };
    Ok(Some(clause))
}

/// True at a `WITH` that only introduces the next table clause.
fn at_optional_with<D: Dialect<Ext = Builtin>>(p: &Parser<'_, D>) -> bool {
    let next = p.peek_nth(1);
    p.at_keyword("WITH")
        && ["AGGREGATION", "STORAGE", "TAG", "ROW"]
            .iter()
            .any(|word| next.is_word(word))
}

pub fn parse_create_table<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Statement<'a, Builtin>> {
    p.expect_keyword("CREATE")?;
    let or_replace = grammar::parse_or_replace(p)?;
    let scope = first_of(p, &["LOCAL", "GLOBAL"]);
    let dynamic = p.eat_keyword("DYNAMIC");
    let lifetime = if p.eat_keyword("TEMP") || p.eat_keyword("TEMPORARY") {
        Some("TEMPORARY")
    } else {
        first_of(p, &["VOLATILE", "TRANSIENT", "ICEBERG"])
    };
    p.expect_keyword("TABLE")?;
    let mut if_not_exists = grammar::parse_if_not_exists(p)?;
    let name = grammar::parse_object_name(p)?;

    let mut columns = Vec::new();
    let mut constraints = Vec::new();
    let mut clauses = Vec::new();
    let mut query = None;
    // Snowflake does not enforce an order on these.
    loop {
        if p.at_op("(") && p.peek_nth(1).is_op(")") {
            p.advance();
            p.advance();
        } else if p.at_op("(") {
            if !columns.is_empty() || !constraints.is_empty() {
                return Err(p.expected("only one column list"));
            }
            (columns, constraints) = grammar::parse_columns(p)?;
        } else if p.eat_keyword("AS") {
            query = Some(Box::new(D::parse_query(p)?));
        } else if p.at_keyword("IF") {
            if_not_exists = grammar::parse_if_not_exists(p)?;
            if !if_not_exists {
                return Err(p.expected("IF NOT EXISTS"));
            }
        } else {
            if at_optional_with(p) {
                p.advance();
            }
            match parse_table_clause(p)? {
                Some(clause) => clauses.push(clause),
                None => break,
            }
        }
    }

    let has_base_location = clauses.iter().any(|clause| {
        matches!(clause, TableClause::Option(option)
            if option.key.text.eq_ignore_ascii_case("BASE_LOCATION"))
    });
    if lifetime == Some("ICEBERG") && !has_base_location {
        return Err(p.expected("BASE_LOCATION for an ICEBERG table"));
    }

    let is_core = scope.is_none()
        && !dynamic
        && matches!(lifetime, None | Some("TEMPORARY"))
        && clauses.is_empty()
        && (!columns.is_empty() || query.is_some());
    if is_core {
        return Ok(StatementKind::CreateTable(Box::new(CreateTable {
            or_replace,
            temporary: lifetime.is_some(),
            name,
            if_not_exists,
            columns,
            constraints,
            query,
        }))
        .into());
    }
    let create = SnowflakeCreateTable {
        or_replace,
        scope: scope.map(Cow::Borrowed),
        dynamic,
        lifetime: lifetime.map(Cow::Borrowed),
        if_not_exists,
        name,
        columns,
        constraints,
        clauses,
        query,
    };
    Ok(StatementKind::Ext(BuiltinStatement::Snowflake(
        SnowflakeStatement::CreateTable(Box::new(create)),
    ))
    .into())
}

fn parse_contact<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<(Ident<'a>, Ident<'a>)> {
    let purpose = p.parse_ident()?;
    p.expect_op("=")?;
    let token = p.peek();
    if token.kind == TokenKind::String {
        p.advance();
        let contact = Ident {
            text: Cow::Borrowed(&token.text[1..token.text.len() - 1]),
            quote: None,
            span: token.span,
        };
        return Ok((purpose, contact));
    }
    Ok((purpose, p.parse_ident()?))
}

fn eat_copy_grants<D: Dialect<Ext = Builtin>>(p: &mut Parser<'_, D>) -> bool {
    let at_copy = p.at_keyword("COPY") && p.peek_nth(1).is_word("GRANTS");
    if at_copy {
        p.advance();
        p.advance();
    }
    at_copy
}

/// True at an option that a view's column can have.
fn at_view_column_option<D: Dialect<Ext = Builtin>>(p: &Parser<'_, D>) -> bool {
    let at_any = |n: usize| {
        ["MASKING", "PROJECTION", "TAG"]
            .iter()
            .any(|word| p.peek_nth(n).is_word(word))
    };
    p.at_keyword("COMMENT") || at_any(0) || (p.at_keyword("WITH") && at_any(1))
}

fn parse_view_column<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<ViewColumn<'a>> {
    let name = p.parse_ident()?;
    let mut options = Vec::new();
    while at_view_column_option(p) {
        match D::parse_column_option(p)? {
            Some(option) => options.push(option),
            None => break,
        }
    }
    Ok(ViewColumn { name, options })
}

pub fn parse_create_view<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Statement<'a, Builtin>> {
    p.expect_keyword("CREATE")?;
    let or_replace = grammar::parse_or_replace(p)?;
    let mut secure = p.eat_keyword("SECURE");
    let temporary = p.eat_keyword("TEMP") || p.eat_keyword("TEMPORARY");
    secure |= p.eat_keyword("SECURE");
    let materialized = p.eat_keyword("MATERIALIZED");
    p.expect_keyword("VIEW")?;
    let mut if_not_exists = grammar::parse_if_not_exists(p)?;
    let name = grammar::parse_object_name(p)?;
    let if_not_exists_after_name = !if_not_exists && grammar::parse_if_not_exists(p)?;
    if_not_exists |= if_not_exists_after_name;
    let mut copy_grants = eat_copy_grants(p);
    let columns = if p.eat_op("(") {
        let columns = p.parse_comma_separated(parse_view_column)?;
        p.expect_op(")")?;
        columns
    } else {
        Vec::new()
    };
    copy_grants |= eat_copy_grants(p);
    let cluster_by = if p.at_keyword("CLUSTER") && p.peek_nth(1).is_word("BY") {
        p.advance();
        p.advance();
        p.expect_op("(")?;
        let exprs = p.parse_comma_separated(Parser::parse_expr)?;
        p.expect_op(")")?;
        exprs
    } else {
        Vec::new()
    };
    let comment = if p.eat_keyword("COMMENT") {
        p.expect_op("=")?;
        Some(parse_comment(p)?)
    } else {
        None
    };
    p.expect_keyword("AS")?;
    let query = Box::new(D::parse_query(p)?);

    let is_core = !secure
        && !if_not_exists_after_name
        && !copy_grants
        && cluster_by.is_empty()
        && comment.is_none()
        && columns.iter().all(|column| column.options.is_empty());
    if is_core {
        return Ok(StatementKind::CreateView(Box::new(CreateView {
            no_schema_binding: false,
            with: Vec::new(),
            or_replace,
            temporary,
            materialized,
            if_not_exists,
            name,
            columns: columns.into_iter().map(|column| column.name).collect(),
            query,
        }))
        .into());
    }
    let create = SnowflakeCreateView {
        or_replace,
        secure,
        materialized,
        temporary,
        if_not_exists,
        if_not_exists_after_name,
        name,
        copy_grants,
        columns,
        cluster_by,
        comment,
        query,
    };
    Ok(
        StatementKind::Ext(BuiltinStatement::Snowflake(SnowflakeStatement::CreateView(
            Box::new(create),
        )))
        .into(),
    )
}

pub fn at_alter_iceberg_table<D: Dialect<Ext = Builtin>>(p: &Parser<'_, D>) -> bool {
    p.at_keyword("ALTER") && p.peek_nth(1).is_word("ICEBERG") && p.peek_nth(2).is_word("TABLE")
}

pub fn parse_alter_iceberg_table<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<SnowflakeStatement<'a>> {
    p.advance();
    p.advance();
    p.advance();
    let if_exists = p.eat_keyword("IF");
    if if_exists {
        p.expect_keyword("EXISTS")?;
    }
    let name = grammar::parse_object_name(p)?;
    let operations = p.parse_comma_separated(D::parse_alter_table_op)?;
    Ok(SnowflakeStatement::AlterIcebergTable(Box::new(
        AlterTable {
            if_exists,
            only: false,
            name,
            operations,
        },
    )))
}

fn parse_structured_field<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
    named: bool,
) -> PResult<StructField<'a>> {
    let name = if named {
        Some(p.parse_name_part()?)
    } else {
        None
    };
    let data_type = p.nested(D::parse_data_type)?;
    let not_null = p.eat_keyword("NOT");
    if not_null {
        p.expect_keyword("NULL")?;
    }
    Ok(StructField {
        name,
        colon: false,
        data_type,
        not_null,
        comment: None,
        options: Vec::new(),
    })
}

/// Parses a data type, which can be a structured `ARRAY(T)`, `MAP(K, V)` or
/// `OBJECT(name T, ...)`, whose elements can be `NOT NULL`.
pub fn parse_data_type<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<DataType<'a>> {
    let object = p.at_keyword("OBJECT");
    let count = if object {
        None
    } else if p.at_keyword("ARRAY") {
        Some(1)
    } else if p.at_keyword("MAP") {
        Some(2)
    } else {
        return grammar::parse_data_type(p);
    };
    if !p.peek_nth(1).is_op("(") {
        return grammar::parse_data_type(p);
    }
    let name = p.parse_ident()?;
    p.advance();
    let mut fields = Vec::new();
    if !p.at_op(")") || !object {
        fields = p.parse_comma_separated(|p| parse_structured_field(p, object))?;
    }
    p.expect_op(")")?;
    if count.is_some_and(|count| fields.len() != count) {
        return Err(p.expected("the right number of type arguments"));
    }
    let mut data_type = grammar::parse_data_type_after_name(p, name)?;
    data_type.angle = Some(Box::new(AngleArguments::ParenFields(fields)));
    Ok(data_type)
}

pub fn parse_create_database<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Statement<'a, Builtin>> {
    p.expect_keyword("CREATE")?;
    let or_replace = grammar::parse_or_replace(p)?;
    let transient = p.eat_keyword("TRANSIENT");
    p.expect_keyword("DATABASE")?;
    let if_not_exists = grammar::parse_if_not_exists(p)?;
    let name = grammar::parse_object_name(p)?;
    let mut clauses = Vec::new();
    loop {
        if p.eat_keyword("CLONE") {
            clauses.push(DatabaseClause::Clone(grammar::parse_object_name(p)?));
        } else if p.eat_keyword("WITH") {
            if p.eat_keyword("TAG") {
                clauses.push(DatabaseClause::Tags(parse_tags(p)?));
            } else {
                p.expect_keyword("CONTACT")?;
                p.expect_op("(")?;
                let contacts = p.parse_comma_separated(parse_contact)?;
                p.expect_op(")")?;
                clauses.push(DatabaseClause::Contacts(contacts));
            }
        } else if at_option(p, &DATABASE_OPTIONS) {
            clauses.push(DatabaseClause::Option(parse_key_value(p)?));
        } else {
            break;
        }
    }
    if !or_replace && !transient && clauses.is_empty() {
        return Ok(StatementKind::CreateDatabase(Box::new(CreateDatabase {
            if_not_exists,
            name,
        }))
        .into());
    }
    let create = SnowflakeCreateDatabase {
        or_replace,
        transient,
        if_not_exists,
        name,
        clauses,
    };
    Ok(StatementKind::Ext(BuiltinStatement::Snowflake(
        SnowflakeStatement::CreateDatabase(Box::new(create)),
    ))
    .into())
}

pub fn parse_column_option<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Option<ColumnOption<'a, Builtin>>> {
    if p.at_keyword("IDENTITY") || p.at_keyword("AUTOINCREMENT") {
        return parse_identity(p).map(Some);
    }
    if p.eat_keyword("COMMENT") {
        return parse_comment(p).map(|text| Some(ColumnOption::Comment(text)));
    }
    if p.at_keyword("COLLATE") && p.peek_nth(1).kind == TokenKind::String {
        p.advance();
        let token = p.peek();
        p.advance();
        let name = ObjectName {
            parts: vec![Ident {
                text: Cow::Borrowed(&token.text[1..token.text.len() - 1]),
                quote: Some('\''),
                span: token.span,
            }],
        };
        return Ok(Some(ColumnOption::Collate(name)));
    }
    let next = p.peek_nth(1);
    let with = p.at_keyword("WITH")
        && ["MASKING", "PROJECTION", "TAG"]
            .iter()
            .any(|word| next.is_word(word));
    if with {
        p.advance();
    }
    if p.eat_keyword("TAG") {
        return Ok(Some(ColumnOption::Tags {
            with,
            tags: parse_tags(p)?,
        }));
    }
    let masking = p.at_keyword("MASKING");
    if masking || p.at_keyword("PROJECTION") {
        p.advance();
        p.expect_keyword("POLICY")?;
        let name = grammar::parse_object_name(p)?;
        let using = if p.eat_keyword("USING") {
            grammar::parse_ident_list(p)?
        } else {
            Vec::new()
        };
        return Ok(Some(ColumnOption::Policy {
            with,
            masking,
            name,
            using,
        }));
    }
    grammar::parse_column_option(p)
}

fn parse_identity<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<ColumnOption<'a, Builtin>> {
    let autoincrement = p.at_keyword("AUTOINCREMENT");
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
    } else if p.eat_keyword("START") {
        let seed = grammar::parse_number_text(p)?;
        p.expect_keyword("INCREMENT")?;
        let increment = grammar::parse_number_text(p)?;
        Some(IdentityParams {
            seed: seed.into(),
            increment: increment.into(),
            start_form: true,
        })
    } else {
        None
    };
    let order = if p.eat_keyword("ORDER") {
        Some(true)
    } else if p.eat_keyword("NOORDER") {
        Some(false)
    } else {
        None
    };
    Ok(ColumnOption::Identity {
        autoincrement,
        params,
        order,
    })
}

pub fn at_alter_table_kind<D: Dialect<Ext = Builtin>>(p: &Parser<'_, D>) -> bool {
    let kind = p.peek_nth(1);
    p.at_keyword("ALTER")
        && (kind.is_word("DYNAMIC") || kind.is_word("EXTERNAL"))
        && p.peek_nth(2).is_word("TABLE")
}

pub fn parse_alter_table_kind<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<SnowflakeStatement<'a>> {
    p.advance();
    let dynamic = p.at_keyword("DYNAMIC");
    p.advance();
    p.advance();
    let if_exists = !dynamic && p.at_keyword("IF");
    if if_exists {
        p.advance();
        p.expect_keyword("EXISTS")?;
    }
    let name = grammar::parse_object_name(p)?;
    let action = if p.eat_keyword("REFRESH") {
        "REFRESH"
    } else if dynamic && p.eat_keyword("SUSPEND") {
        "SUSPEND"
    } else if dynamic && p.eat_keyword("RESUME") {
        "RESUME"
    } else {
        return Err(p.expected("REFRESH, SUSPEND or RESUME"));
    };
    let token = p.peek();
    let subpath = if !dynamic && token.kind == TokenKind::String {
        p.advance();
        Some(D::string_literal(token))
    } else {
        None
    };
    Ok(SnowflakeStatement::AlterTableKind {
        dynamic,
        if_exists,
        name,
        action: action.into(),
        subpath,
    })
}

/// True at a `BEGIN` that starts a block rather than a transaction.
pub fn at_block<D: Dialect<Ext = Builtin>>(p: &Parser<'_, D>) -> bool {
    let next = p.peek_nth(1);
    p.at_keyword("BEGIN")
        && !next.is_word("TRANSACTION")
        && !next.is_word("WORK")
        && !next.is_word("NAME")
        && !next.is_op(";")
        && next.kind != TokenKind::Eof
}

fn at_block_end<D: Dialect<Ext = Builtin>>(p: &Parser<'_, D>) -> bool {
    p.at_keyword("END") || p.at_keyword("EXCEPTION") || p.at_keyword("WHEN")
}

fn parse_statements<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Vec<Statement<'a, Builtin>>> {
    let mut statements = Vec::new();
    while !at_block_end(p) {
        statements.push(p.nested(Parser::parse_statement)?);
        p.expect_op(";")?;
    }
    Ok(statements)
}

pub fn parse_block<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<SnowflakeStatement<'a>> {
    p.expect_keyword("BEGIN")?;
    let statements = parse_statements(p)?;
    let exception = if p.eat_keyword("EXCEPTION") {
        let mut arms = Vec::new();
        while !p.at_keyword("END") {
            p.expect_keyword("WHEN")?;
            let mut names = vec![p.parse_ident()?];
            while p.eat_keyword("OR") {
                names.push(p.parse_ident()?);
            }
            p.expect_keyword("THEN")?;
            let statements = parse_statements(p)?;
            arms.push(ExceptionWhen { names, statements });
        }
        Some(arms)
    } else {
        None
    };
    p.expect_keyword("END")?;
    Ok(SnowflakeStatement::Block(Box::new(Block {
        statements,
        exception,
    })))
}

pub fn parse_raise<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<SnowflakeStatement<'a>> {
    p.expect_keyword("RAISE")?;
    if p.eat_keyword("USING") {
        p.expect_keyword("MESSAGE")?;
        p.expect_op("=")?;
        let message = p.parse_expr()?;
        return Ok(SnowflakeStatement::Raise(Some(RaiseValue::Message(
            message,
        ))));
    }
    let value = if p.at_op(";") || p.peek().kind == TokenKind::Eof {
        None
    } else {
        Some(RaiseValue::Expr(p.parse_expr()?))
    };
    Ok(SnowflakeStatement::Raise(value))
}

fn parse_assignment<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Option<Assignment<'a>>> {
    if p.eat_keyword("DEFAULT") {
        return Ok(Some(Assignment::Default(p.parse_expr()?)));
    }
    if p.eat_op(":=") {
        return Ok(Some(Assignment::Walrus(p.parse_expr()?)));
    }
    Ok(None)
}

fn parse_declaration<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Declaration<'a>> {
    let name = p.parse_ident()?;
    let kind = if p.eat_keyword("CURSOR") {
        p.expect_keyword("FOR")?;
        if p.at_keyword("SELECT") || p.at_keyword("WITH") {
            DeclarationKind::CursorQuery(Box::new(D::parse_query(p)?))
        } else {
            DeclarationKind::CursorExpr(p.parse_expr()?)
        }
    } else if p.eat_keyword("RESULTSET") {
        DeclarationKind::ResultSet(parse_assignment(p)?)
    } else if p.eat_keyword("EXCEPTION") {
        let value = if p.at_op("(") {
            Some(p.parse_expr()?)
        } else {
            None
        };
        DeclarationKind::Exception(value)
    } else {
        let mut value = parse_assignment(p)?;
        let mut data_type = None;
        if value.is_none() && p.peek().kind == TokenKind::Word {
            data_type = Some(D::parse_data_type(p)?);
            value = parse_assignment(p)?;
        }
        DeclarationKind::Variable { data_type, value }
    };
    Ok(Declaration { name, kind })
}

/// True if the token after a `;` starts another declaration of the same
/// `DECLARE` rather than a new statement.
fn another_declaration_follows<D: Dialect<Ext = Builtin>>(p: &Parser<'_, D>) -> bool {
    const STATEMENTS: [&str; 36] = [
        "SELECT", "WITH", "INSERT", "UPDATE", "DELETE", "MERGE", "CREATE", "DROP", "ALTER", "SET",
        "USE", "BEGIN", "COPY", "DECLARE", "SHOW", "TRUNCATE", "CALL", "COMMIT", "ROLLBACK",
        "LIST", "LS", "REMOVE", "RM", "PUT", "GET", "GRANT", "REVOKE", "DESCRIBE", "DESC",
        "EXPLAIN", "START", "ABORT", "RAISE", "RETURN", "UNSET", "COMMENT",
    ];
    let next = p.peek_nth(1);
    next.kind == TokenKind::Word && !STATEMENTS.iter().any(|word| next.is_word(word))
}

pub fn parse_declare<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<SnowflakeStatement<'a>> {
    p.expect_keyword("DECLARE")?;
    let mut declarations = vec![parse_declaration(p)?];
    while p.at_op(";") && another_declaration_follows(p) {
        p.advance();
        declarations.push(parse_declaration(p)?);
    }
    Ok(SnowflakeStatement::Declare(declarations))
}

pub fn at_set_tuple<D: Dialect<Ext = Builtin>>(p: &Parser<'_, D>) -> bool {
    p.at_keyword("SET") && p.peek_nth(1).is_op("(")
}

fn parse_set_value<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Expr<'a, Builtin>> {
    if p.at_keyword("SELECT") || p.at_keyword("WITH") {
        let query = p.nested(D::parse_query)?;
        return Ok(ExprKind::Subquery(Box::new(query)).into());
    }
    p.parse_expr()
}

pub fn parse_set_tuple<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<SnowflakeStatement<'a>> {
    p.expect_keyword("SET")?;
    let names = grammar::parse_ident_list(p)?;
    p.expect_op("=")?;
    p.expect_op("(")?;
    let values = p.parse_comma_separated(|p| p.spanned_expr(parse_set_value))?;
    p.expect_op(")")?;
    Ok(SnowflakeStatement::SetTuple { names, values })
}

fn write_comma<T: Display>(f: &mut fmt::Formatter<'_>, items: &[T]) -> fmt::Result {
    for (i, item) in items.iter().enumerate() {
        if i > 0 {
            f.write_str(", ")?;
        }
        write!(f, "{item}")?;
    }
    Ok(())
}

impl Display for ViewColumn<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name)?;
        for option in &self.options {
            write!(f, " {option}")?;
        }
        Ok(())
    }
}

impl Display for SnowflakeCreateView<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CREATE ")?;
        for (written, word) in [
            (self.or_replace, "OR REPLACE "),
            (self.secure, "SECURE "),
            (self.materialized, "MATERIALIZED "),
            (self.temporary, "TEMPORARY "),
        ] {
            if written {
                f.write_str(word)?;
            }
        }
        f.write_str("VIEW ")?;
        if self.if_not_exists && !self.if_not_exists_after_name {
            f.write_str("IF NOT EXISTS ")?;
        }
        write!(f, "{}", self.name)?;
        if self.if_not_exists_after_name {
            f.write_str(" IF NOT EXISTS")?;
        }
        if self.copy_grants {
            f.write_str(" COPY GRANTS")?;
        }
        if !self.columns.is_empty() {
            f.write_str(" (")?;
            write_comma(f, &self.columns)?;
            f.write_str(")")?;
        }
        if !self.cluster_by.is_empty() {
            f.write_str(" CLUSTER BY (")?;
            write_comma(f, &self.cluster_by)?;
            f.write_str(")")?;
        }
        if let Some(comment) = &self.comment {
            write!(f, " COMMENT = {}", comment)?;
        }
        crate::display::write_view_query(f, &self.query)
    }
}

pub fn write_alter_iceberg_table(
    f: &mut fmt::Formatter<'_>,
    alter: &AlterTable<'_, Builtin>,
) -> fmt::Result {
    f.write_str("ALTER ICEBERG TABLE ")?;
    if alter.if_exists {
        f.write_str("IF EXISTS ")?;
    }
    write!(f, "{} ", alter.name)?;
    write_comma(f, &alter.operations)
}

impl Display for SemanticView<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name)?;
        for (keyword, list) in [
            ("DIMENSIONS", &self.dimensions),
            ("METRICS", &self.metrics),
            ("FACTS", &self.facts),
        ] {
            if !list.is_empty() {
                write!(f, " {keyword} ")?;
                write_comma(f, list)?;
            }
        }
        if let Some(selection) = &self.selection {
            write!(f, " WHERE {selection}")?;
        }
        Ok(())
    }
}

impl Display for TableClause<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TableClause::CopyGrants => f.write_str("COPY GRANTS"),
            TableClause::Comment { equals, text } => {
                let equals = if *equals { "= " } else { "" };
                write!(f, "COMMENT {equals}{}", text)
            }
            TableClause::Clone(name) => write!(f, "CLONE {name}"),
            TableClause::Like(name) => write!(f, "LIKE {name}"),
            TableClause::Version(call) => write!(f, "{call}"),
            TableClause::ClusterBy(exprs) => {
                f.write_str("CLUSTER BY (")?;
                write_comma(f, exprs)?;
                f.write_str(")")
            }
            TableClause::Option(option) => write!(f, "{option}"),
            TableClause::Policy { kind, name, on } => {
                write!(f, "WITH {kind} {name}")?;
                if !on.is_empty() {
                    f.write_str(" ON (")?;
                    write_comma(f, on)?;
                    f.write_str(")")?;
                }
                Ok(())
            }
            TableClause::Tags(tags) => {
                f.write_str("WITH TAG (")?;
                write_comma(f, tags)?;
                f.write_str(")")
            }
            TableClause::OnCommit(action) => write!(f, "ON COMMIT {action}"),
            TableClause::RequireUser => f.write_str("REQUIRE USER"),
        }
    }
}

impl Display for SnowflakeCreateTable<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CREATE ")?;
        if self.or_replace {
            f.write_str("OR REPLACE ")?;
        }
        if let Some(ref scope) = self.scope {
            write!(f, "{scope} ")?;
        }
        if self.dynamic {
            f.write_str("DYNAMIC ")?;
        }
        if let Some(ref lifetime) = self.lifetime {
            write!(f, "{lifetime} ")?;
        }
        f.write_str("TABLE ")?;
        if self.if_not_exists {
            f.write_str("IF NOT EXISTS ")?;
        }
        write!(f, "{}", self.name)?;
        if !self.columns.is_empty() || !self.constraints.is_empty() {
            write_columns(f, &self.columns, &self.constraints)?;
        } else if self.query.is_none() && !self.clauses.iter().any(TableClause::names_a_source) {
            f.write_str(" ()")?;
        }
        for clause in self.clauses.iter().filter(|c| c.renders_first()) {
            write!(f, " {clause}")?;
        }
        for clause in self.clauses.iter().filter(|c| !c.renders_first()) {
            write!(f, " {clause}")?;
        }
        if let Some(query) = &self.query {
            write!(f, " AS {query}")?;
        }
        Ok(())
    }
}

impl Display for SnowflakeCreateDatabase<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CREATE ")?;
        if self.or_replace {
            f.write_str("OR REPLACE ")?;
        }
        if self.transient {
            f.write_str("TRANSIENT ")?;
        }
        f.write_str("DATABASE ")?;
        if self.if_not_exists {
            f.write_str("IF NOT EXISTS ")?;
        }
        write!(f, "{}", self.name)?;
        for clause in &self.clauses {
            match clause {
                DatabaseClause::Clone(name) => write!(f, " CLONE {name}")?,
                DatabaseClause::Option(option) => {
                    write!(f, " {} = ", option.key)?;
                    write_value(f, &option.value)?;
                }
                DatabaseClause::Tags(tags) => {
                    f.write_str(" WITH TAG (")?;
                    write_comma(f, tags)?;
                    f.write_str(")")?;
                }
                DatabaseClause::Contacts(contacts) => {
                    f.write_str(" WITH CONTACT (")?;
                    for (i, (purpose, contact)) in contacts.iter().enumerate() {
                        if i > 0 {
                            f.write_str(", ")?;
                        }
                        write!(f, "{purpose} = {contact}")?;
                    }
                    f.write_str(")")?;
                }
            }
        }
        Ok(())
    }
}

fn write_statements(
    f: &mut fmt::Formatter<'_>,
    statements: &[Statement<'_, Builtin>],
) -> fmt::Result {
    for statement in statements {
        write!(f, " {statement};")?;
    }
    Ok(())
}

impl Display for Block<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("BEGIN")?;
        write_statements(f, &self.statements)?;
        if let Some(arms) = &self.exception {
            f.write_str(" EXCEPTION")?;
            for arm in arms {
                f.write_str(" WHEN ")?;
                for (i, name) in arm.names.iter().enumerate() {
                    if i > 0 {
                        f.write_str(" OR ")?;
                    }
                    write!(f, "{name}")?;
                }
                f.write_str(" THEN")?;
                write_statements(f, &arm.statements)?;
            }
        }
        f.write_str(" END")
    }
}

impl Display for Assignment<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Assignment::Default(expr) => write!(f, "DEFAULT {expr}"),
            Assignment::Walrus(expr) => write!(f, ":= {expr}"),
        }
    }
}

impl Display for Declaration<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name)?;
        match &self.kind {
            DeclarationKind::Variable { data_type, value } => {
                if let Some(data_type) = data_type {
                    write!(f, " {data_type}")?;
                }
                if let Some(value) = value {
                    write!(f, " {value}")?;
                }
                Ok(())
            }
            DeclarationKind::ResultSet(value) => {
                f.write_str(" RESULTSET")?;
                match value {
                    Some(value) => write!(f, " {value}"),
                    None => Ok(()),
                }
            }
            DeclarationKind::CursorQuery(query) => write!(f, " CURSOR FOR {query}"),
            DeclarationKind::CursorExpr(expr) => write!(f, " CURSOR FOR {expr}"),
            DeclarationKind::Exception(value) => {
                f.write_str(" EXCEPTION")?;
                match value {
                    Some(value) => write!(f, " {value}"),
                    None => Ok(()),
                }
            }
        }
    }
}

impl Display for RaiseValue<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RaiseValue::Expr(expr) => write!(f, "{expr}"),
            RaiseValue::Message(expr) => write!(f, "USING MESSAGE = {expr}"),
        }
    }
}

pub fn write_set_tuple(
    f: &mut fmt::Formatter<'_>,
    names: &[Ident<'_>],
    values: &[Expr<'_, Builtin>],
) -> fmt::Result {
    f.write_str("SET (")?;
    write_comma(f, names)?;
    f.write_str(") = (")?;
    write_comma(f, values)?;
    f.write_str(")")
}

pub fn write_declare(f: &mut fmt::Formatter<'_>, declarations: &[Declaration<'_>]) -> fmt::Result {
    f.write_str("DECLARE ")?;
    for (i, declaration) in declarations.iter().enumerate() {
        if i > 0 {
            f.write_str("; ")?;
        }
        write!(f, "{declaration}")?;
    }
    Ok(())
}

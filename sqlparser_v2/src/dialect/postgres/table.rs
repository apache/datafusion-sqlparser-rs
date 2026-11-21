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

//! PostgreSQL's table clauses: `PARTITION OF`, `LIKE`, `INHERITS` and more.

use std::borrow::Cow;
use std::fmt;

use crate::PResult;
use crate::ast::{
    AlterColumnAction, AlterTable, AlterTableOp, ColumnOption, ConstraintAttribute, CreateFlags,
    CreateTable, ExcludeConstraint, ExcludeElement, ExcludeOperator, Expr, Ident, IndexStorage,
    ObjectName, PartitionBound, RangeBound, Statement, StatementKind, TableConstraint,
    TableConstraintKind,
};
use crate::dialect::postgres::ddl::{parse_generic_options, parse_operator_name, statement};
use crate::dialect::postgres::{PostgresStatement, ddl};
use crate::dialect::{Builtin, Dialect};
use crate::display::{Indent, NewLine, SpaceOrNewline, comma, write_storage};
use crate::grammar;
use crate::lexer::TokenKind;
use crate::parser::Parser;

type Exp<'a> = Expr<'a, Builtin>;

/// A column of `CREATE TABLE ... PARTITION OF`, which gives no type.
#[derive(Debug, Clone, PartialEq)]
pub struct PartitionColumn<'a> {
    pub name: Ident<'a>,
    /// True if `WITH OPTIONS` was written.
    pub with_options: bool,
    pub options: Vec<ColumnOption<'a, Builtin>>,
}

/// `LIKE source [INCLUDING | EXCLUDING option]...`
#[derive(Debug, Clone, PartialEq)]
pub struct TableLike<'a> {
    pub source: ObjectName<'a>,
    /// `(including, option)`, in the order written.
    pub options: Vec<(bool, Ident<'a>)>,
    /// How many columns come before this element.
    pub position: usize,
}

/// `CREATE TABLE` with clauses the core does not have.
#[derive(Debug, Clone, PartialEq)]
pub struct PostgresCreateTable<'a> {
    pub table: CreateTable<'a, Builtin>,
    pub local: bool,
    pub unlogged: bool,
    pub foreign: bool,
    pub partition_of: Option<ObjectName<'a>>,
    /// True if the column list was written, even when empty.
    pub has_elements: bool,
    pub likes: Vec<TableLike<'a>>,
    pub partition_columns: Vec<PartitionColumn<'a>>,
    pub inherits: Vec<ObjectName<'a>>,
    pub bound: Option<PartitionBound<'a, Builtin>>,
    pub partition_by: Option<Box<Exp<'a>>>,
    pub with: IndexStorage<'a, Builtin>,
    /// `DROP`, `DELETE ROWS` or `PRESERVE ROWS`.
    pub on_commit: Option<Cow<'a, str>>,
    pub server: Option<Ident<'a>>,
    pub options: Vec<crate::ast::GenericOption<'a>>,
}

impl PostgresCreateTable<'_> {
    /// True if this holds nothing beyond the core table.
    pub fn is_plain(&self) -> bool {
        (self.table.columns.len() + self.table.constraints.len() > 0 || self.table.query.is_some())
            && !self.local
            && !self.unlogged
            && !self.foreign
            && self.partition_of.is_none()
            && self.likes.is_empty()
            && self.partition_columns.is_empty()
            && self.inherits.is_empty()
            && self.bound.is_none()
            && self.partition_by.is_none()
            && self.with.is_empty()
            && self.on_commit.is_none()
            && self.server.is_none()
            && self.options.is_empty()
    }
}

/// True at `CREATE [OR REPLACE] [temporary-like] TABLE`, `n` tokens in.
pub fn at_create_table_at<D: Dialect>(p: &Parser<'_, D>, n: usize) -> bool {
    let mut n = n;
    if p.peek_nth(n).is_word("LOCAL") {
        n += 1;
    }
    if ["TEMP", "TEMPORARY", "UNLOGGED"]
        .iter()
        .any(|word| p.peek_nth(n).is_word(word))
    {
        n += 1;
    }
    p.peek_nth(n).is_word("TABLE")
}

/// Parses what follows `CREATE`, with the cursor on the first modifier.
pub fn parse_create_table_statement<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
    or_replace: bool,
) -> PResult<Statement<'a, Builtin>> {
    let local = p.eat_keyword("LOCAL");
    let temporary = p.eat_keyword("TEMPORARY") || p.eat_keyword("TEMP");
    if local && !temporary {
        return Err(p.expected("TEMPORARY after LOCAL"));
    }
    if or_replace && p.at_keyword("FOREIGN") {
        return Err(p.expected("TABLE"));
    }
    let unlogged = p.eat_keyword("UNLOGGED");
    let foreign = p.eat_keyword("FOREIGN");
    p.expect_keyword("TABLE")?;
    let flags = CreateFlags {
        or_replace,
        temporary,
    };
    let create = parse_table(p, flags, local, unlogged, foreign)?;
    if create.is_plain() {
        return Ok(StatementKind::CreateTable(Box::new(create.table)).into());
    }
    Ok(statement(PostgresStatement::CreateTable(Box::new(create))))
}

/// Parses what follows `CREATE ... TABLE`.
fn parse_table<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
    flags: CreateFlags,
    local: bool,
    unlogged: bool,
    foreign: bool,
) -> PResult<PostgresCreateTable<'a>> {
    let if_not_exists = grammar::parse_if_not_exists(p)?;
    let name = grammar::parse_object_name(p)?;
    let partition_of = if p.at_keyword("PARTITION") && p.peek_nth(1).is_word("OF") {
        p.advance();
        p.advance();
        Some(grammar::parse_object_name(p)?)
    } else {
        None
    };

    let mut columns = Vec::new();
    let mut constraints = Vec::new();
    let mut likes = Vec::new();
    let mut partition_columns = Vec::new();
    let mut has_elements = false;
    if p.eat_op("(") {
        has_elements = true;
        if !p.at_op(")") {
            loop {
                if let Some(constraint) = D::parse_table_constraint(p)? {
                    constraints.push(constraint);
                } else if p.at_keyword("LIKE") {
                    likes.push(parse_like(p, columns.len())?);
                } else if partition_of.is_some() {
                    partition_columns.push(parse_partition_column(p)?);
                } else {
                    columns.push(D::parse_column_def(p)?);
                }
                if !p.eat_op(",") {
                    break;
                }
            }
        }
        p.expect_op(")")?;
    }

    let inherits = if p.eat_keyword("INHERITS") {
        p.expect_op("(")?;
        let parents = p.parse_comma_separated(grammar::parse_object_name)?;
        p.expect_op(")")?;
        parents
    } else {
        Vec::new()
    };
    if partition_of.is_some() && !inherits.is_empty() {
        return Err(p.expected("FOR VALUES or DEFAULT before any INHERITS"));
    }
    let bound = if partition_of.is_some() {
        Some(parse_partition_bound(p)?)
    } else {
        None
    };
    let partition_by = if p.at_keyword("PARTITION") && p.peek_nth(1).is_word("BY") {
        p.advance();
        p.advance();
        Some(Box::new(p.parse_expr()?))
    } else {
        None
    };
    let with = crate::dialect::postgres::parse_storage_parameters(p)?;
    let on_commit = if p.at_keyword("ON") && p.peek_nth(1).is_word("COMMIT") {
        p.advance();
        p.advance();
        Some(parse_on_commit(p)?)
    } else {
        None
    };
    let (server, options) = if p.eat_keyword("SERVER") {
        let server = p.parse_ident()?;
        (Some(server), parse_generic_options(p)?)
    } else {
        (None, Vec::new())
    };
    if foreign && server.is_none() {
        return Err(p.expected("SERVER"));
    }
    let query = if p.eat_keyword("AS") {
        Some(Box::new(D::parse_create_table_query(p)?))
    } else {
        None
    };
    if !has_elements && partition_of.is_none() && query.is_none() {
        return Err(p.expected("column definitions or AS"));
    }

    Ok(PostgresCreateTable {
        table: CreateTable {
            or_replace: flags.or_replace,
            temporary: flags.temporary,
            name,
            if_not_exists,
            columns,
            constraints,
            query,
        },
        local,
        unlogged,
        foreign,
        partition_of,
        has_elements,
        likes,
        partition_columns,
        inherits,
        bound,
        partition_by,
        with,
        on_commit: on_commit.map(Cow::Borrowed),
        server,
        options,
    })
}

fn parse_like<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
    position: usize,
) -> PResult<TableLike<'a>> {
    p.advance();
    let source = grammar::parse_object_name(p)?;
    let mut options = Vec::new();
    while p.at_keyword("INCLUDING") || p.at_keyword("EXCLUDING") {
        let including = p.at_keyword("INCLUDING");
        p.advance();
        options.push((including, p.parse_ident()?));
    }
    Ok(TableLike {
        source,
        options,
        position,
    })
}

fn parse_partition_column<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<PartitionColumn<'a>> {
    let name = p.parse_ident()?;
    let with_options = p.at_keyword("WITH") && p.peek_nth(1).is_word("OPTIONS");
    if with_options {
        p.advance();
        p.advance();
    }
    let mut options = Vec::new();
    while let Some(option) = D::parse_column_option(p)? {
        options.push(option);
    }
    Ok(PartitionColumn {
        name,
        with_options,
        options,
    })
}

fn parse_on_commit<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<&'static str> {
    if p.eat_keyword("DROP") {
        return Ok("DROP");
    }
    if p.eat_keyword("DELETE") {
        p.expect_keyword("ROWS")?;
        return Ok("DELETE ROWS");
    }
    p.expect_keyword("PRESERVE")?;
    p.expect_keyword("ROWS")?;
    Ok("PRESERVE ROWS")
}

/// Parses a partition's bound: `DEFAULT` or `FOR VALUES ...`.
pub fn parse_partition_bound<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<PartitionBound<'a, Builtin>> {
    if p.eat_keyword("DEFAULT") {
        return Ok(PartitionBound::Default);
    }
    p.expect_keyword("FOR")?;
    p.expect_keyword("VALUES")?;
    if p.eat_keyword("IN") {
        p.expect_op("(")?;
        let values = p.parse_comma_separated(Parser::parse_expr)?;
        p.expect_op(")")?;
        return Ok(PartitionBound::In(values));
    }
    if p.eat_keyword("FROM") {
        let from = parse_range_bounds(p)?;
        p.expect_keyword("TO")?;
        let to = parse_range_bounds(p)?;
        return Ok(PartitionBound::Range { from, to });
    }
    p.expect_keyword("WITH")?;
    p.expect_op("(")?;
    p.expect_keyword("MODULUS")?;
    let modulus = grammar::parse_number_text(p)?;
    p.expect_op(",")?;
    p.expect_keyword("REMAINDER")?;
    let remainder = grammar::parse_number_text(p)?;
    p.expect_op(")")?;
    Ok(PartitionBound::Hash {
        modulus: modulus.into(),
        remainder: remainder.into(),
    })
}

fn parse_range_bounds<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Vec<RangeBound<'a, Builtin>>> {
    p.expect_op("(")?;
    let bounds = p.parse_comma_separated(|p| {
        Ok(if p.eat_keyword("MINVALUE") {
            RangeBound::MinValue
        } else if p.eat_keyword("MAXVALUE") {
            RangeBound::MaxValue
        } else {
            RangeBound::Value(p.parse_expr()?)
        })
    })?;
    p.expect_op(")")?;
    Ok(bounds)
}

/// Parses a table constraint, adding `EXCLUDE` and `[CONSTRAINT n] NOT NULL`.
pub fn parse_table_constraint<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Option<TableConstraint<'a, Builtin>>> {
    let named = p.at_keyword("CONSTRAINT");
    let offset = if named { 2 } else { 0 };
    let word = p.peek_nth(offset);
    let exclude = word.is_word("EXCLUDE")
        && (p.peek_nth(offset + 1).is_op("(") || p.peek_nth(offset + 1).is_word("USING"));
    let not_null = word.is_word("NOT") && p.peek_nth(offset + 1).is_word("NULL");
    if !exclude && !not_null {
        return grammar::parse_table_constraint(p);
    }
    let name = if named {
        p.advance();
        Some(p.parse_ident()?)
    } else {
        None
    };
    let kind = if exclude {
        p.advance();
        let using = if p.eat_keyword("USING") {
            Some(p.parse_ident()?)
        } else {
            None
        };
        p.expect_op("(")?;
        let elements = p.parse_comma_separated(parse_exclude_element)?;
        p.expect_op(")")?;
        TableConstraintKind::Exclude(Box::new(ExcludeConstraint {
            using,
            elements,
            predicate: None,
        }))
    } else {
        p.advance();
        p.advance();
        TableConstraintKind::NotNull(p.parse_ident()?)
    };
    let include = if p.eat_keyword("INCLUDE") {
        grammar::parse_ident_list(p)?
    } else {
        Vec::new()
    };
    let kind = match kind {
        TableConstraintKind::Exclude(mut exclude) => {
            if p.eat_keyword("WHERE") {
                p.expect_op("(")?;
                exclude.predicate = Some(p.parse_expr()?);
                p.expect_op(")")?;
            }
            TableConstraintKind::Exclude(exclude)
        }
        other => other,
    };
    let mut attributes: Vec<ConstraintAttribute> = Vec::new();
    while let Some(attribute) = grammar::parse_constraint_attribute(p)? {
        attributes.push(attribute);
    }
    grammar::check_constraint_attributes(p, attributes.iter().copied())?;
    if matches!(kind, TableConstraintKind::NotNull(_))
        && attributes.iter().any(|attribute| {
            !matches!(
                attribute,
                ConstraintAttribute::NoInherit | ConstraintAttribute::NotValid
            )
        })
    {
        return Err(p.expected("NO INHERIT or NOT VALID after NOT NULL"));
    }
    Ok(Some(TableConstraint {
        name,
        kind,
        include,
        nulls_distinct: None,
        attributes,
    }))
}

fn parse_exclude_element<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<ExcludeElement<'a, Builtin>> {
    let column = D::parse_index_column(p)?;
    p.expect_keyword("WITH")?;
    let operator = if p.at_keyword("OPERATOR") && p.peek_nth(1).is_op("(") {
        p.advance();
        p.advance();
        let operator = parse_operator_name(p)?;
        p.expect_op(")")?;
        ExcludeOperator::Qualified(operator)
    } else {
        let token = p.expect_kind(TokenKind::Op, "an operator")?;
        ExcludeOperator::Plain(token.text.into())
    };
    Ok(ExcludeElement { column, operator })
}

/// Parses `ALTER {MATERIALIZED VIEW | FOREIGN TABLE} ...`, with the cursor on
/// the first keyword of the kind.
pub fn parse_alter_relation<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
    keyword: &'static str,
) -> PResult<PostgresStatement<'a>> {
    for _ in keyword.split(' ') {
        p.advance();
    }
    let if_exists = p.at_keyword("IF") && p.peek_nth(1).is_word("EXISTS");
    if if_exists {
        p.advance();
        p.advance();
    }
    let only = D::parse_alter_table_only(p);
    let name = grammar::parse_object_name(p)?;
    let operations = p.parse_comma_separated(D::parse_alter_table_op)?;
    Ok(PostgresStatement::AlterRelation {
        keyword: keyword.into(),
        alter: Box::new(AlterTable {
            if_exists,
            only,
            name,
            operations,
        }),
    })
}

/// Parses `ALTER` table operations only PostgreSQL has, or returns `None`.
pub fn parse_alter_table_op_extra<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Option<AlterTableOp<'a, Builtin>>> {
    let next = p.peek_nth(1);
    if p.at_keyword("ATTACH") && next.is_word("PARTITION") {
        p.advance();
        p.advance();
        let name = grammar::parse_object_name(p)?;
        let bound = parse_partition_bound(p)?;
        return Ok(Some(AlterTableOp::AttachPartition { name, bound }));
    }
    if p.at_keyword("DETACH") && next.is_word("PARTITION") {
        p.advance();
        p.advance();
        let name = grammar::parse_object_name(p)?;
        let mode = ["CONCURRENTLY", "FINALIZE"]
            .into_iter()
            .find(|word| p.eat_keyword(word));
        return Ok(Some(AlterTableOp::DetachPartition {
            name,
            mode: mode.map(Cow::Borrowed),
        }));
    }
    if p.at_keyword("INHERIT") {
        p.advance();
        let name = grammar::parse_object_name(p)?;
        return Ok(Some(AlterTableOp::PhraseObject {
            phrase: "INHERIT".into(),
            name,
        }));
    }
    if p.at_keyword("NO") && next.is_word("INHERIT") {
        p.advance();
        p.advance();
        let name = grammar::parse_object_name(p)?;
        return Ok(Some(AlterTableOp::PhraseObject {
            phrase: "NO INHERIT".into(),
            name,
        }));
    }
    if p.at_keyword("OPTIONS") && next.is_op("(") {
        return Ok(Some(AlterTableOp::GenericOptions(parse_generic_options(
            p,
        )?)));
    }
    if p.at_keyword("RESET") && next.is_op("(") {
        p.advance();
        p.advance();
        let names = p.parse_comma_separated(|p| {
            let mut parts = vec![p.parse_name_part()?];
            while p.eat_op(".") {
                parts.push(p.parse_name_part()?);
            }
            Ok(ObjectName { parts })
        })?;
        p.expect_op(")")?;
        return Ok(Some(AlterTableOp::ResetOptions(names)));
    }
    if p.at_keyword("SET") && next.is_word("SCHEMA") {
        p.advance();
        p.advance();
        let name = p.parse_ident()?;
        return Ok(Some(AlterTableOp::PhraseName {
            phrase: "SET SCHEMA".into(),
            name,
        }));
    }
    if p.at_keyword("ALTER") {
        let column_keyword = next.is_word("COLUMN");
        let name_at = if column_keyword { 2 } else { 1 };
        if p.peek_nth(name_at + 1).is_word("OPTIONS") {
            p.advance();
            if column_keyword {
                p.advance();
            }
            let name = p.parse_ident()?;
            return Ok(Some(AlterTableOp::AlterColumn {
                column_keyword,
                name,
                action: AlterColumnAction::GenericOptions(parse_generic_options(p)?),
            }));
        }
    }
    Ok(None)
}

impl fmt::Display for TableLike<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LIKE {}", self.source)?;
        for (including, option) in &self.options {
            write!(
                f,
                " {} {option}",
                if *including { "INCLUDING" } else { "EXCLUDING" }
            )?;
        }
        Ok(())
    }
}

impl fmt::Display for PartitionColumn<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.name.fmt(f)?;
        for option in &self.options {
            write!(f, " {option}")?;
        }
        Ok(())
    }
}

/// The comma-separated contents of `CREATE TABLE ( ... )`.
struct TableElements<'s, 'a>(&'s PostgresCreateTable<'a>);

impl fmt::Display for TableElements<'_, '_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let this = self.0;
        let mut first = true;
        let mut separate = |f: &mut fmt::Formatter<'_>| -> fmt::Result {
            if !std::mem::take(&mut first) {
                f.write_str(",")?;
                SpaceOrNewline.fmt(f)?;
            }
            Ok(())
        };
        let mut likes = this.likes.iter().peekable();
        for (i, column) in this.table.columns.iter().enumerate() {
            while let Some(like) = likes.next_if(|like| like.position == i) {
                separate(f)?;
                write!(f, "{like}")?;
            }
            separate(f)?;
            write!(f, "{column}")?;
        }
        for like in likes {
            separate(f)?;
            write!(f, "{like}")?;
        }
        for column in &this.partition_columns {
            separate(f)?;
            write!(f, "{column}")?;
        }
        for constraint in &this.table.constraints {
            separate(f)?;
            write!(f, "{constraint}")?;
        }
        Ok(())
    }
}

impl fmt::Display for PostgresCreateTable<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let table = &self.table;
        f.write_str("CREATE ")?;
        if table.or_replace {
            f.write_str("OR REPLACE ")?;
        }
        if self.local {
            f.write_str("LOCAL ")?;
        }
        if table.temporary {
            f.write_str("TEMPORARY ")?;
        }
        if self.unlogged {
            f.write_str("UNLOGGED ")?;
        }
        if self.foreign {
            f.write_str("FOREIGN ")?;
        }
        f.write_str("TABLE ")?;
        if table.if_not_exists {
            f.write_str("IF NOT EXISTS ")?;
        }
        table.name.fmt(f)?;
        if let Some(parent) = &self.partition_of {
            write!(f, " PARTITION OF {parent}")?;
        }
        if self.has_elements {
            f.write_str(" (")?;
            // A list of only `LIKE` stays on one line, as in the original.
            if table.columns.is_empty()
                && self.partition_columns.is_empty()
                && table.constraints.is_empty()
            {
                write!(f, "{}", TableElements(self))?;
            } else {
                NewLine.fmt(f)?;
                Indent(TableElements(self)).fmt(f)?;
                NewLine.fmt(f)?;
            }
            f.write_str(")")?;
        }
        if !self.inherits.is_empty() {
            write!(f, " INHERITS ({})", comma(&self.inherits))?;
        }
        if let Some(bound) = &self.bound {
            write!(f, " {bound}")?;
        }
        if let Some(partition_by) = &self.partition_by {
            write!(f, " PARTITION BY {partition_by}")?;
        }
        if !self.with.is_empty() {
            f.write_str(" WITH ")?;
            write_storage(f, &self.with)?;
        }
        if let Some(ref on_commit) = self.on_commit {
            write!(f, " ON COMMIT {on_commit}")?;
        }
        if let Some(server) = &self.server {
            write!(f, " SERVER {server}")?;
        }
        if !self.options.is_empty() {
            f.write_str(" ")?;
            ddl::write_options(f, &self.options)?;
        }
        if let Some(query) = &table.query {
            write!(f, " AS {query}")?;
        }
        Ok(())
    }
}

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

//! `CREATE`, `DROP` and `ALTER`: tables, views, indexes, sequences and columns.

use super::*;
use crate::PResult;
use crate::ast::StatementKind;
use crate::dialect::Dialect;
use crate::lexer::TokenKind;
use crate::parser::Parser;
use std::borrow::Cow;

fn parse_signed_number<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<SignedNumber<'a>> {
    let sign = if p.eat_op("-") {
        "-"
    } else if p.eat_op("+") {
        "+"
    } else {
        ""
    };
    let token = p.expect_kind(TokenKind::Number, "a number")?;
    Ok(SignedNumber {
        sign: sign.into(),
        digits: token.text.into(),
    })
}

/// Parses one sequence option, or returns `None` at something else.
fn parse_sequence_option<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<Option<SequenceOption<'a>>> {
    let option = if p.eat_keyword("INCREMENT") {
        let by = p.eat_keyword("BY");
        SequenceOption::Increment {
            by,
            value: parse_signed_number(p)?,
        }
    } else if p.eat_keyword("MINVALUE") {
        SequenceOption::MinValue(Some(parse_signed_number(p)?))
    } else if p.eat_keyword("MAXVALUE") {
        SequenceOption::MaxValue(Some(parse_signed_number(p)?))
    } else if p.eat_keyword("START") {
        let with = p.eat_keyword("WITH");
        SequenceOption::Start {
            with,
            value: parse_signed_number(p)?,
        }
    } else if p.eat_keyword("CACHE") {
        SequenceOption::Cache(parse_signed_number(p)?)
    } else if p.eat_keyword("CYCLE") {
        SequenceOption::Cycle(true)
    } else if p.at_keyword("NO") {
        p.advance();
        if p.eat_keyword("MINVALUE") {
            SequenceOption::MinValue(None)
        } else if p.eat_keyword("MAXVALUE") {
            SequenceOption::MaxValue(None)
        } else {
            p.expect_keyword("CYCLE")?;
            SequenceOption::Cycle(false)
        }
    } else if p.eat_keyword("OWNED") {
        p.expect_keyword("BY")?;
        SequenceOption::OwnedBy(parse_object_name(p)?)
    } else {
        return Ok(None);
    };
    Ok(Some(option))
}

fn parse_sequence_options<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<Vec<SequenceOption<'a>>> {
    let mut options = Vec::new();
    while let Some(option) = parse_sequence_option(p)? {
        options.push(option);
    }
    Ok(options)
}

/// Parses what follows `CREATE [TEMPORARY] SEQUENCE`.
pub fn parse_create_sequence<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
    temporary: bool,
) -> PResult<CreateSequence<'a>> {
    let if_not_exists = super::parse_if_not_exists(p)?;
    let name = parse_object_name(p)?;
    let data_type = if p.eat_keyword("AS") {
        Some(D::parse_data_type(p)?)
    } else {
        None
    };
    let options = parse_sequence_options(p)?;
    Ok(CreateSequence {
        temporary,
        if_not_exists,
        name,
        data_type,
        options,
    })
}

/// Parses what follows `GENERATED`.
pub fn parse_generated<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Generated<'a, D::Ext>> {
    let when = if p.eat_keyword("ALWAYS") {
        Some(GeneratedWhen::Always)
    } else if p.eat_keyword("BY") {
        p.expect_keyword("DEFAULT")?;
        Some(GeneratedWhen::ByDefault)
    } else {
        None
    };
    p.expect_keyword("AS")?;
    let kind = if p.eat_keyword("IDENTITY") {
        let options = if p.eat_op("(") {
            let options = parse_sequence_options(p)?;
            p.expect_op(")")?;
            Some(options)
        } else {
            None
        };
        GeneratedKind::Identity(options)
    } else {
        p.expect_op("(")?;
        let expr = p.parse_expr()?;
        p.expect_op(")")?;
        let storage = if p.eat_keyword("STORED") {
            Some(GeneratedStorage::Stored)
        } else if p.eat_keyword("VIRTUAL") {
            Some(GeneratedStorage::Virtual)
        } else {
            None
        };
        GeneratedKind::Expr { expr, storage }
    };
    Ok(Generated { when, kind })
}

/// Rejects a list that sets the same kind of attribute twice.
pub fn check_constraint_attributes<'a, D: Dialect>(
    p: &Parser<'a, D>,
    attributes: impl Iterator<Item = ConstraintAttribute>,
) -> PResult<()> {
    let mut seen = Vec::new();
    for attribute in attributes {
        let kind = std::mem::discriminant(&attribute);
        if seen.contains(&kind) {
            return Err(p.expected("each constraint attribute at most once"));
        }
        seen.push(kind);
    }
    Ok(())
}

/// Parses a constraint attribute, or returns `None` at something else.
pub fn parse_constraint_attribute<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<Option<ConstraintAttribute>> {
    let next = p.peek_nth(1);
    let attribute = if p.eat_keyword("DEFERRABLE") {
        ConstraintAttribute::Deferrable(true)
    } else if p.at_keyword("NOT") && next.is_word("DEFERRABLE") {
        p.advance();
        p.advance();
        ConstraintAttribute::Deferrable(false)
    } else if p.eat_keyword("INITIALLY") {
        if p.eat_keyword("DEFERRED") {
            ConstraintAttribute::InitiallyDeferred(true)
        } else {
            p.expect_keyword("IMMEDIATE")?;
            ConstraintAttribute::InitiallyDeferred(false)
        }
    } else if p.eat_keyword("ENFORCED") {
        ConstraintAttribute::Enforced(true)
    } else if p.at_keyword("NOT") && next.is_word("ENFORCED") {
        p.advance();
        p.advance();
        ConstraintAttribute::Enforced(false)
    } else if p.at_keyword("NOT") && next.is_word("VALID") {
        p.advance();
        p.advance();
        ConstraintAttribute::NotValid
    } else if p.at_keyword("NO") && next.is_word("INHERIT") {
        p.advance();
        p.advance();
        ConstraintAttribute::NoInherit
    } else {
        return Ok(None);
    };
    Ok(Some(attribute))
}

/// Parses what follows `CREATE ... TABLE`.
pub fn parse_create_table<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
    flags: CreateFlags,
) -> PResult<CreateTable<'a, D::Ext>> {
    let if_not_exists = parse_if_not_exists(p)?;
    let name = parse_object_name(p)?;
    parse_create_table_after_name(p, flags, if_not_exists, name)
}

/// Parses what follows the table's name in `CREATE ... TABLE`.
pub fn parse_create_table_after_name<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
    flags: CreateFlags,
    if_not_exists: bool,
    name: ObjectName<'a>,
) -> PResult<CreateTable<'a, D::Ext>> {
    let (columns, constraints) = if p.at_op("(") {
        parse_columns(p)?
    } else {
        (Vec::new(), Vec::new())
    };

    let query = if p.eat_keyword("AS") {
        Some(Box::new(D::parse_create_table_query(p)?))
    } else {
        None
    };
    if columns.is_empty() && query.is_none() {
        return Err(p.expected("column definitions or AS"));
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

/// The columns and constraints in `(...)`, which is at the cursor.
pub type ColumnList<'a, X> = (Vec<ColumnDef<'a, X>>, Vec<TableConstraint<'a, X>>);

/// Parses a parenthesized list of column definitions and constraints.
pub fn parse_columns<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<ColumnList<'a, D::Ext>> {
    p.expect_op("(")?;
    let mut columns = Vec::new();
    let mut constraints = Vec::new();
    loop {
        if let Some(constraint) = D::parse_table_constraint(p)? {
            constraints.push(constraint);
        } else {
            columns.push(D::parse_column_def(p)?);
        }
        if !p.eat_op(",") {
            break;
        }
    }
    p.expect_op(")")?;
    Ok((columns, constraints))
}

/// Parses a table constraint, or returns `None` if this element is a column.
/// The words that start constraints are not reserved, so they are only read
/// as keywords when what follows confirms it.
pub fn parse_table_constraint<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<Option<TableConstraint<'a, D::Ext>>> {
    let third = p.peek_nth(2);
    let named = p.at_keyword("CONSTRAINT")
        && (third.is_word("PRIMARY")
            || third.is_word("UNIQUE")
            || third.is_word("CHECK")
            || third.is_word("FOREIGN"));
    let name = if named {
        p.advance();
        Some(p.parse_ident()?)
    } else {
        None
    };

    let mut nulls_distinct = None;
    let next = p.peek_nth(1);
    let kind = if p.at_keyword("PRIMARY") && next.is_word("KEY") {
        p.advance();
        p.advance();
        match parse_using_index(p)? {
            Some(index) => TableConstraintKind::PrimaryKeyUsingIndex(index),
            None => TableConstraintKind::PrimaryKey(parse_ident_list(p)?),
        }
    } else if p.at_keyword("UNIQUE")
        && (next.is_op("(") || next.is_word("NULLS") || next.is_word("USING"))
    {
        p.advance();
        nulls_distinct = parse_nulls_distinct(p)?;
        match parse_using_index(p)? {
            Some(index) => TableConstraintKind::UniqueUsingIndex(index),
            None => TableConstraintKind::Unique(parse_ident_list(p)?),
        }
    } else if p.at_keyword("CHECK") && p.peek_nth(1).is_op("(") {
        p.advance();
        TableConstraintKind::Check(parse_parenthesized_expr(p)?)
    } else if p.at_keyword("FOREIGN") && p.peek_nth(1).is_word("KEY") {
        p.advance();
        p.advance();
        let columns = parse_ident_list(p)?;
        p.expect_keyword("REFERENCES")?;
        let references = parse_foreign_key_ref(p)?;
        TableConstraintKind::ForeignKey {
            index_name: None,
            columns,
            references,
        }
    } else if named {
        return Err(p.expected("a table constraint"));
    } else {
        return Ok(None);
    };
    let include = if p.eat_keyword("INCLUDE") {
        parse_ident_list(p)?
    } else {
        Vec::new()
    };
    let mut attributes = Vec::new();
    while let Some(attribute) = parse_constraint_attribute(p)? {
        attributes.push(attribute);
    }
    check_constraint_attributes(p, attributes.iter().copied())?;
    Ok(Some(TableConstraint {
        name,
        kind,
        include,
        nulls_distinct,
        attributes,
    }))
}

/// Parses `USING INDEX name` if present.
fn parse_using_index<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Option<Ident<'a>>> {
    if !(p.at_keyword("USING") && p.peek_nth(1).is_word("INDEX")) {
        return Ok(None);
    }
    p.advance();
    p.advance();
    p.parse_ident().map(Some)
}

/// Parses `NULLS [NOT] DISTINCT` if present.
pub fn parse_nulls_distinct<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Option<bool>> {
    if !p.eat_keyword("NULLS") {
        return Ok(None);
    }
    let negated = p.eat_keyword("NOT");
    p.expect_keyword("DISTINCT")?;
    Ok(Some(!negated))
}

fn parse_parenthesized_expr<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Expr<'a, D::Ext>> {
    p.expect_op("(")?;
    let expr = p.parse_expr()?;
    p.expect_op(")")?;
    Ok(expr)
}

/// Parses what follows `REFERENCES`.
pub fn parse_foreign_key_ref<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<ForeignKeyRef<'a>> {
    let table = parse_object_name(p)?;
    let columns = if p.at_op("(") {
        parse_ident_list(p)?
    } else {
        Vec::new()
    };
    let match_kind = if p.eat_keyword("MATCH") {
        let kind = ["FULL", "PARTIAL", "SIMPLE"]
            .into_iter()
            .find(|kind| p.eat_keyword(kind));
        Some(kind.ok_or_else(|| p.expected("FULL, PARTIAL or SIMPLE"))?)
    } else {
        None
    };
    let (mut on_delete, mut on_update) = (None, None);
    while p.at_keyword("ON") {
        p.advance();
        if on_delete.is_none() && p.eat_keyword("DELETE") {
            on_delete = Some(parse_referential_action(p)?);
        } else if on_update.is_none() {
            p.expect_keyword("UPDATE")?;
            on_update = Some(parse_referential_action(p)?);
        } else {
            return Err(p.expected("at most one ON DELETE and one ON UPDATE"));
        }
    }
    Ok(ForeignKeyRef {
        table,
        columns,
        match_kind: match_kind.map(Cow::Borrowed),
        on_delete,
        on_update,
    })
}

fn parse_referential_action<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<ReferentialAction> {
    if p.eat_keyword("CASCADE") {
        Ok(ReferentialAction::Cascade)
    } else if p.eat_keyword("RESTRICT") {
        Ok(ReferentialAction::Restrict)
    } else if p.eat_keyword("NO") {
        p.expect_keyword("ACTION")?;
        Ok(ReferentialAction::NoAction)
    } else {
        p.expect_keyword("SET")?;
        if p.eat_keyword("DEFAULT") {
            Ok(ReferentialAction::SetDefault)
        } else {
            p.expect_keyword("NULL")?;
            Ok(ReferentialAction::SetNull)
        }
    }
}

pub fn parse_column_def<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<ColumnDef<'a, D::Ext>> {
    let name = p.parse_ident()?;
    parse_column_def_after_name(p, name)
}

/// Parses a column definition's type and options.
pub fn parse_column_def_after_name<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
    name: Ident<'a>,
) -> PResult<ColumnDef<'a, D::Ext>> {
    let data_type = D::parse_data_type(p)?;
    let mut options = Vec::new();
    while let Some(option) = D::parse_column_option(p)? {
        options.push(option);
    }
    check_column_attributes(p, &options)?;
    Ok(ColumnDef {
        name,
        data_type,
        options,
    })
}

/// Checks that no constraint in a column's options repeats an attribute.
pub fn check_column_attributes<'a, D: Dialect>(
    p: &Parser<'a, D>,
    options: &[ColumnOption<'a, D::Ext>],
) -> PResult<()> {
    // Attributes belong to the constraint before them, so each run is separate.
    for run in options.split(|option| !matches!(option, ColumnOption::Attribute(_))) {
        check_constraint_attributes(
            p,
            run.iter().filter_map(|option| match option {
                ColumnOption::Attribute(attribute) => Some(*attribute),
                _ => None,
            }),
        )?;
    }
    Ok(())
}

pub fn parse_column_option<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<Option<ColumnOption<'a, D::Ext>>> {
    if let Some(attribute) = parse_constraint_attribute(p)? {
        return Ok(Some(ColumnOption::Attribute(attribute)));
    }
    let option = if p.eat_keyword("COLLATE") {
        ColumnOption::Collate(parse_object_name(p)?)
    } else if p.eat_keyword("GENERATED") {
        ColumnOption::Generated(Box::new(parse_generated(p)?))
    } else if p.eat_keyword("NOT") {
        p.expect_keyword("NULL")?;
        ColumnOption::NotNull
    } else if p.eat_keyword("NULL") {
        ColumnOption::Null
    } else if p.eat_keyword("PRIMARY") {
        p.expect_keyword("KEY")?;
        ColumnOption::PrimaryKey
    } else if p.eat_keyword("UNIQUE") {
        ColumnOption::Unique
    } else if p.at_keyword("CHECK") && p.peek_nth(1).is_op("(") {
        p.advance();
        ColumnOption::Check(parse_parenthesized_expr(p)?)
    } else if p.eat_keyword("REFERENCES") {
        ColumnOption::References(parse_foreign_key_ref(p)?)
    } else if p.eat_keyword("DEFAULT") {
        ColumnOption::Default(p.parse_expr()?)
    } else {
        return Ok(None);
    };
    Ok(Some(option))
}

pub fn parse_data_type<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<DataType<'a>> {
    let name = p.parse_ident()?;
    parse_data_type_after_name(p, name)
}

/// Parses the rest of a data type, with its name already read.
pub fn parse_data_type_after_name<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
    name: Ident<'a>,
) -> PResult<DataType<'a>> {
    if name.quote.is_none() && name.text.eq_ignore_ascii_case("INTERVAL") && p.at_op("(") {
        return Err(p.expected("a plain INTERVAL type"));
    }
    let mut suffix = DataTypeSuffix::None;
    if p.eat_keyword("PRECISION") {
        suffix = DataTypeSuffix::Precision;
    } else if p.eat_keyword("VARYING") {
        suffix = DataTypeSuffix::Varying;
    }
    let args = if p.eat_op("(") {
        let args = p.parse_comma_separated(|p| parse_number_text(p).map(Cow::Borrowed))?;
        p.expect_op(")")?;
        args
    } else {
        Vec::new()
    };
    if suffix == DataTypeSuffix::None && p.peek_nth(1).is_word("TIME") {
        if p.eat_keyword("WITH") {
            suffix = DataTypeSuffix::WithTimeZone;
        } else if p.eat_keyword("WITHOUT") {
            suffix = DataTypeSuffix::WithoutTimeZone;
        }
        if suffix != DataTypeSuffix::None {
            p.expect_keyword("TIME")?;
            p.expect_keyword("ZONE")?;
        }
    }
    let array = parse_array_suffix(p)?;
    Ok(DataType {
        qualifier: Vec::new(),
        name,
        angle: None,
        args,
        suffix,
        array,
    })
}

/// Parses `[n]` or `[]`, with the cursor on the `[`.
fn parse_array_dimension<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Option<&'a str>> {
    p.expect_op("[")?;
    if p.eat_op("]") {
        return Ok(None);
    }
    let size = parse_number_text(p)?;
    p.expect_op("]")?;
    Ok(Some(size))
}

pub fn parse_array_suffix<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<ArrayType<'a>> {
    if p.eat_keyword("ARRAY") {
        let size = if p.at_op("[") {
            parse_array_dimension(p)?
        } else {
            None
        };
        return Ok(ArrayType::Keyword(size.map(Cow::Borrowed)));
    }
    Ok(ArrayType::None)
}

/// Parses `[]`, `[3][4]` and so on, for dialects whose array types use them.
pub fn parse_array_brackets<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<ArrayType<'a>> {
    let mut dimensions = Vec::new();
    while p.at_op("[") {
        dimensions.push(parse_array_dimension(p)?);
    }
    Ok(ArrayType::Brackets(
        dimensions
            .into_iter()
            .map(|size| size.map(Cow::Borrowed))
            .collect(),
    ))
}

pub fn parse_number_text<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<&'a str> {
    let token = p.expect_kind(TokenKind::Number, "a number")?;
    Ok(token.text)
}

/// Consumes `IF NOT EXISTS` and returns whether it was present.
pub fn parse_if_not_exists<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<bool> {
    let found = p.at_keyword("IF") && p.peek_nth(1).is_word("NOT");
    if found {
        p.advance();
        p.advance();
        p.expect_keyword("EXISTS")?;
    }
    Ok(found)
}

pub fn parse_or_replace<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<bool> {
    let or_replace = p.eat_keyword("OR");
    if or_replace {
        p.expect_keyword("REPLACE")?;
    }
    Ok(or_replace)
}

pub fn parse_if_exists<'a, D: Dialect>(p: &mut Parser<'a, D>) -> bool {
    let found = p.at_keyword("IF") && p.peek_nth(1).is_word("EXISTS");
    if found {
        p.advance();
        p.advance();
    }
    found
}

pub fn parse_drop_behavior<'a, D: Dialect>(p: &mut Parser<'a, D>) -> Option<DropBehavior> {
    if p.eat_keyword("CASCADE") {
        Some(DropBehavior::Cascade)
    } else if p.eat_keyword("RESTRICT") {
        Some(DropBehavior::Restrict)
    } else {
        None
    }
}

/// Reads the modifiers after `CREATE` and dispatches on the object type.
pub fn parse_create<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Statement<'a, D::Ext>> {
    p.expect_keyword("CREATE")?;
    let or_replace = parse_or_replace(p)?;
    let temporary = p.eat_keyword("TEMPORARY") || p.eat_keyword("TEMP");
    let flags = CreateFlags {
        or_replace,
        temporary,
    };

    if p.eat_keyword("TABLE") {
        return D::parse_create_table(p, flags)
            .map(|s| StatementKind::CreateTable(Box::new(s)).into());
    }
    if p.at_keyword("VIEW") || p.at_keyword("MATERIALIZED") {
        return D::parse_create_view(p, flags)
            .map(|s| StatementKind::CreateView(Box::new(s)).into());
    }
    if !flags.or_replace && p.eat_keyword("SEQUENCE") {
        return parse_create_sequence(p, flags.temporary)
            .map(|s| StatementKind::CreateSequence(Box::new(s)).into());
    }
    if p.eat_keyword("SCHEMA") {
        return D::parse_create_schema(p, flags)
            .map(|s| StatementKind::CreateSchema(Box::new(s)).into());
    }
    if let Some(statement) = D::parse_create_extension(p, flags)? {
        return Ok(statement);
    }
    if flags != CreateFlags::default() {
        return Err(p.expected("TABLE or VIEW"));
    }
    if p.at_keyword("INDEX") || p.at_keyword("UNIQUE") {
        return D::parse_create_index(p).map(|s| StatementKind::CreateIndex(Box::new(s)).into());
    }
    if p.eat_keyword("DATABASE") {
        let if_not_exists = parse_if_not_exists(p)?;
        let name = parse_object_name(p)?;
        return Ok(StatementKind::CreateDatabase(Box::new(CreateDatabase {
            if_not_exists,
            name,
        }))
        .into());
    }
    Err(p.expected("an object type after CREATE"))
}

/// Parses what follows `CREATE SCHEMA`, for a dialect with only the plain form.
pub fn parse_create_schema<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
    flags: CreateFlags,
) -> PResult<CreateSchema<'a>> {
    if flags != CreateFlags::default() {
        return Err(p.expected("TABLE or VIEW"));
    }
    parse_create_schema_body(p, flags)
}

/// Parses `[IF NOT EXISTS] name`, the part of `CREATE SCHEMA` every dialect has.
pub fn parse_create_schema_body<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
    flags: CreateFlags,
) -> PResult<CreateSchema<'a>> {
    let if_not_exists = parse_if_not_exists(p)?;
    let name = parse_object_name(p)?;
    Ok(CreateSchema {
        or_replace: flags.or_replace,
        if_not_exists,
        name: Some(name),
        authorization: None,
        clone: None,
    })
}

/// Parses `CREATE SCHEMA` with `AUTHORIZATION`. If `named`, the owner can also
/// follow a schema name; otherwise only the form with no name is allowed.
pub fn parse_create_schema_authorization<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
    named: bool,
) -> PResult<CreateSchema<'a>> {
    if p.at_keyword("AUTHORIZATION") && !p.peek_nth(1).is_op(".") {
        p.advance();
        let owner = p.parse_ident()?;
        return Ok(CreateSchema {
            or_replace: false,
            if_not_exists: false,
            name: None,
            authorization: Some(owner),
            clone: None,
        });
    }
    let mut schema = parse_create_schema_body(p, CreateFlags::default())?;
    if named && p.eat_keyword("AUTHORIZATION") {
        schema.authorization = Some(p.parse_ident()?);
    }
    Ok(schema)
}

/// Parses `ALTER VIEW name [(columns)] AS query`.
pub fn parse_alter_view<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
    allow_columns: bool,
) -> PResult<AlterView<'a, D::Ext>> {
    p.expect_keyword("ALTER")?;
    p.expect_keyword("VIEW")?;
    let name = parse_object_name(p)?;
    let columns = if allow_columns && p.at_op("(") {
        parse_ident_list(p)?
    } else {
        Vec::new()
    };
    p.expect_keyword("AS")?;
    let query = Box::new(D::parse_query(p)?);
    Ok(AlterView {
        name,
        columns,
        query,
    })
}

/// Parses `CREATE [OR REPLACE] ROLE [IF NOT EXISTS] name, ...`. A list of names
/// is only read if `many`; `CREATE` has been consumed. Returns `None` at
/// anything other than `ROLE`.
pub fn parse_create_role<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
    flags: CreateFlags,
    many: bool,
) -> PResult<Option<Statement<'a, D::Ext>>> {
    if flags.temporary || !p.eat_keyword("ROLE") {
        return Ok(None);
    }
    let if_not_exists = parse_if_not_exists(p)?;
    let names = if many {
        p.parse_comma_separated(parse_object_name)?
    } else {
        vec![parse_object_name(p)?]
    };
    Ok(Some(
        StatementKind::CreateRole(Box::new(CreateRole {
            or_replace: flags.or_replace,
            if_not_exists,
            names,
        }))
        .into(),
    ))
}

/// Parses `ANALYZE [TABLE] [names]`.
pub fn parse_analyze<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Statement<'a, D::Ext>> {
    p.expect_keyword("ANALYZE")?;
    let table_keyword = p.eat_keyword("TABLE");
    let names = if p.at_ident() {
        p.parse_comma_separated(parse_object_name)?
    } else {
        Vec::new()
    };
    Ok(StatementKind::Analyze(Box::new(Analyze {
        table_keyword,
        names,
    }))
    .into())
}

/// Parses `ALTER INDEX name RENAME TO new_name`, the one form several dialects share.
pub fn parse_alter_index_rename<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<Statement<'a, D::Ext>> {
    p.expect_keyword("ALTER")?;
    p.expect_keyword("INDEX")?;
    let name = parse_object_name(p)?;
    p.expect_keyword("RENAME")?;
    p.expect_keyword("TO")?;
    let new_name = parse_object_name(p)?;
    Ok(StatementKind::AlterIndexRename { name, new_name }.into())
}

pub fn at_alter_view<'a, D: Dialect>(p: &Parser<'a, D>) -> bool {
    p.at_keyword("ALTER") && p.peek_nth(1).is_word("VIEW")
}

pub fn parse_create_view<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
    flags: CreateFlags,
) -> PResult<CreateView<'a, D::Ext>> {
    let materialized = p.eat_keyword("MATERIALIZED");
    p.expect_keyword("VIEW")?;
    let if_not_exists = parse_if_not_exists(p)?;
    let name = parse_object_name(p)?;
    let columns = if p.at_op("(") {
        parse_ident_list(p)?
    } else {
        Vec::new()
    };
    p.expect_keyword("AS")?;
    let query = Box::new(D::parse_query(p)?);
    Ok(CreateView {
        or_replace: flags.or_replace,
        temporary: flags.temporary,
        materialized,
        if_not_exists,
        name,
        columns,
        with: Vec::new(),
        query,
        no_schema_binding: false,
    })
}

pub fn parse_create_index<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<CreateIndex<'a, D::Ext>> {
    let unique = p.eat_keyword("UNIQUE");
    p.expect_keyword("INDEX")?;
    let IndexFlags {
        concurrently,
        asynchronous,
    } = D::parse_index_flags(p);
    let if_not_exists = parse_if_not_exists(p)?;
    let name = if p.at_keyword("ON") {
        None
    } else {
        Some(parse_object_name(p)?)
    };
    let using_before_on = parse_optional_using(p)?;
    p.expect_keyword("ON")?;
    let table = parse_object_name(p)?;
    if using_before_on.is_some() && p.at_keyword("USING") {
        return Err(p.expected("one USING before the column list"));
    }
    let using = parse_optional_using(p)?;
    p.expect_op("(")?;
    let columns = p.parse_comma_separated(D::parse_index_column)?;
    p.expect_op(")")?;
    let trailing_using = parse_optional_using(p)?;
    let include = if p.eat_keyword("INCLUDE") {
        parse_ident_list(p)?
    } else {
        Vec::new()
    };
    let nulls_distinct = parse_nulls_distinct(p)?;
    let with = D::parse_index_storage(p)?;
    let predicate = parse_optional_where(p)?;
    let settings = D::parse_index_settings(p)?;
    Ok(CreateIndex {
        unique,
        concurrently,
        asynchronous,
        if_not_exists,
        name,
        using_before_on,
        table,
        using,
        columns,
        trailing_using,
        include,
        nulls_distinct,
        with,
        predicate,
        settings: settings
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect(),
    })
}

pub fn parse_optional_using<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Option<Ident<'a>>> {
    if p.eat_keyword("USING") {
        p.parse_ident().map(Some)
    } else {
        Ok(None)
    }
}

pub fn parse_index_column<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<IndexColumn<'a, D::Ext>> {
    let expr = p.parse_expr()?;
    let operator_class = if p.at_ident() && !p.at_keyword("NULLS") {
        Some(parse_object_name(p)?)
    } else {
        None
    };
    let (asc, nulls_first) = parse_order_by_modifiers(p)?;
    Ok(IndexColumn {
        item: OrderByItem {
            using: None,
            expr,
            asc,
            nulls_first,
            ext: Default::default(),
        },
        operator_class,
    })
}

/// Parses `WITH (name = value, name)`, for dialects with storage parameters.
pub fn parse_index_storage_parameters<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<IndexStorage<'a, D::Ext>> {
    if !p.eat_keyword("WITH") {
        return Ok(Vec::new());
    }
    parse_storage_parameter_list(p)
}

/// Parses `(name = value, name)`, in which a name can be qualified.
pub fn parse_storage_parameter_list<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<IndexStorage<'a, D::Ext>> {
    p.expect_op("(")?;
    let parameters = p.parse_comma_separated(|p| {
        let mut parts = vec![p.parse_name_part()?];
        while p.eat_op(".") {
            parts.push(p.parse_name_part()?);
        }
        let value = if p.eat_op("=") {
            Some(p.parse_expr()?)
        } else {
            None
        };
        Ok((ObjectName { parts }, value))
    })?;
    p.expect_op(")")?;
    Ok(parameters)
}

pub fn parse_drop<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<DropObject<'a>> {
    p.expect_keyword("DROP")?;
    let temporary = D::eat_drop_temporary(p);
    let object_type = if p.eat_keyword("TABLE") {
        ObjectType::Table
    } else if p.eat_keyword("VIEW") {
        ObjectType::View
    } else if p.eat_keyword("MATERIALIZED") {
        p.expect_keyword("VIEW")?;
        ObjectType::MaterializedView
    } else if p.eat_keyword("INDEX") {
        ObjectType::Index
    } else if p.eat_keyword("SCHEMA") {
        ObjectType::Schema
    } else if p.eat_keyword("DATABASE") {
        ObjectType::Database
    } else if p.eat_keyword("SEQUENCE") {
        ObjectType::Sequence
    } else if let Some(object_type) = eat_drop_object_type(p, &["ROLE", "USER"]) {
        object_type
    } else if let Some(object_type) = D::parse_drop_object_type(p) {
        object_type
    } else {
        return Err(p.expected("an object type after DROP"));
    };
    let if_exists = parse_if_exists(p);
    let names = p.parse_comma_separated(parse_object_name)?;
    let on = if (matches!(object_type, ObjectType::Index)
        || matches!(&object_type, ObjectType::Named(name) if name == "TRIGGER" || name == "POLICY"))
        && p.eat_keyword("ON")
    {
        Some(parse_object_name(p)?)
    } else {
        None
    };
    if object_type == ObjectType::Named("POLICY".into()) && on.is_none() {
        return Err(p.expected("ON and the policy's table"));
    }
    let behavior = parse_drop_behavior(p);
    let purge = D::eat_drop_purge(p);
    Ok(DropObject {
        temporary,
        object_type,
        if_exists,
        names,
        on,
        behavior,
        purge,
    })
}

pub fn parse_truncate<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Truncate<'a>> {
    p.expect_keyword("TRUNCATE")?;
    let table_keyword = p.eat_keyword("TABLE");
    let if_exists = parse_if_exists(p);
    let names = p.parse_comma_separated(parse_object_name)?;
    Ok(Truncate {
        table_keyword,
        if_exists,
        names,
    })
}

pub fn parse_alter_table<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<AlterTable<'a, D::Ext>> {
    p.expect_keyword("ALTER")?;
    p.expect_keyword("TABLE")?;
    let if_exists = parse_if_exists(p);
    let only = D::parse_alter_table_only(p);
    let name = parse_object_name(p)?;
    let operations = p.parse_comma_separated(D::parse_alter_table_op)?;
    Ok(AlterTable {
        if_exists,
        only,
        name,
        operations,
    })
}

pub fn parse_alter_table_op<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<AlterTableOp<'a, D::Ext>> {
    if p.eat_keyword("ADD") {
        if let Some(constraint) = D::parse_table_constraint(p)? {
            return Ok(AlterTableOp::AddConstraint(constraint));
        }
        let column_keyword = p.eat_keyword("COLUMN");
        let if_not_exists = D::parse_add_column_if_not_exists(p)?;
        let column = D::parse_column_def(p)?;
        return Ok(AlterTableOp::AddColumn {
            column_keyword,
            if_not_exists,
            column,
            position: None,
        });
    }
    if p.eat_keyword("DROP") {
        if p.eat_keyword("CONSTRAINT") {
            let if_exists = parse_if_exists(p);
            let name = p.parse_ident()?;
            let behavior = parse_drop_behavior(p);
            return Ok(AlterTableOp::DropConstraint {
                if_exists,
                name,
                behavior,
            });
        }
        let column_keyword = p.eat_keyword("COLUMN");
        let if_exists = parse_if_exists(p);
        let name = p.parse_ident()?;
        let behavior = parse_drop_behavior(p);
        return Ok(AlterTableOp::DropColumn {
            column_keyword,
            if_exists,
            name,
            behavior,
        });
    }
    if p.eat_keyword("RENAME") {
        if p.eat_keyword("TO") {
            return parse_object_name(p).map(AlterTableOp::RenameTable);
        }
        p.eat_keyword("COLUMN");
        let old = p.parse_ident()?;
        p.expect_keyword("TO")?;
        let new = p.parse_ident()?;
        return Ok(AlterTableOp::RenameColumn { old, new });
    }
    if p.eat_keyword("ALTER") {
        let column_keyword = p.eat_keyword("COLUMN");
        let name = p.parse_ident()?;
        let action = parse_alter_column_action(p)?;
        return Ok(AlterTableOp::AlterColumn {
            column_keyword,
            name,
            action,
        });
    }
    Err(p.expected("an ALTER TABLE operation"))
}

fn parse_alter_column_action<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<AlterColumnAction<'a, D::Ext>> {
    if p.at_keyword("ADD") && p.peek_nth(1).is_word("GENERATED") {
        p.advance();
        p.advance();
        let generated = parse_generated(p)?;
        if !matches!(generated.kind, GeneratedKind::Identity(_)) {
            return Err(p.expected("AS IDENTITY"));
        }
        return Ok(AlterColumnAction::AddGenerated(Box::new(generated)));
    }
    if p.eat_keyword("TYPE") {
        let data_type = D::parse_data_type(p)?;
        let using = D::parse_alter_type_using(p)?;
        return Ok(AlterColumnAction::SetType {
            data_type,
            set_data: false,
            using,
        });
    }
    if p.eat_keyword("DROP") {
        if p.eat_keyword("DEFAULT") {
            return Ok(AlterColumnAction::DropDefault);
        }
        p.expect_keyword("NOT")?;
        p.expect_keyword("NULL")?;
        return Ok(AlterColumnAction::DropNotNull);
    }
    p.expect_keyword("SET")?;
    if p.eat_keyword("DEFAULT") {
        return p.parse_expr().map(AlterColumnAction::SetDefault);
    }
    if p.eat_keyword("DATA") {
        p.expect_keyword("TYPE")?;
        let data_type = D::parse_data_type(p)?;
        let using = D::parse_alter_type_using(p)?;
        return Ok(AlterColumnAction::SetType {
            data_type,
            set_data: true,
            using,
        });
    }
    p.expect_keyword("NOT")?;
    p.expect_keyword("NULL")?;
    Ok(AlterColumnAction::SetNotNull)
}

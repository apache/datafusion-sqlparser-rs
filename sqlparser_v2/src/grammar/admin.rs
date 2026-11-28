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

//! Rules for `GRANT`, `REVOKE`, `MERGE` and `DROP FUNCTION`.

use super::*;
use crate::PResult;
use crate::dialect::Dialect;
use crate::lexer::{Token, TokenKind};
use crate::parser::Parser;
use std::borrow::Cow;

fn is_name_token(token: Token<'_>) -> bool {
    matches!(token.kind, TokenKind::Word | TokenKind::QuotedIdent)
}

/// Reads the words of one privilege, up to what ends it.
fn parse_privilege<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Privilege<'a>> {
    let mut words = Vec::new();
    while p.peek().kind == TokenKind::Word && !p.at_keyword("ON") {
        words.push(p.peek().text);
        p.advance();
    }
    if words.is_empty() {
        return Err(p.expected("a privilege"));
    }
    let columns = if p.at_op("(") {
        parse_ident_list(p)?
    } else {
        Vec::new()
    };
    Ok(Privilege {
        words: words.into_iter().map(Cow::Borrowed).collect(),
        columns,
    })
}

pub fn parse_privileges<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Privileges<'a>> {
    let next = p.peek_nth(1);
    if p.at_keyword("ALL") && (next.is_word("ON") || next.is_word("PRIVILEGES")) {
        p.advance();
        let privileges = p.eat_keyword("PRIVILEGES");
        return Ok(Privileges::All { privileges });
    }
    p.parse_comma_separated(parse_privilege)
        .map(Privileges::List)
}

/// Reads the words that say what kind of thing a name is, as in
/// `COMPUTE POOL p`. The last word before the end of the name is the name.
pub(super) fn parse_kind_words<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
    stops: &[&str],
) -> Vec<&'a str> {
    let mut words = Vec::new();
    while p.peek().kind == TokenKind::Word {
        let next = p.peek_nth(1);
        if !is_name_token(next) || stops.iter().any(|stop| next.is_word(stop)) {
            break;
        }
        words.push(p.peek().text);
        p.advance();
    }
    words
}

pub fn parse_grant_name<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<GrantName<'a>> {
    let name = parse_object_name(p)?;
    let args = if p.eat_op("(") {
        let args = p.parse_comma_separated(D::parse_data_type)?;
        p.expect_op(")")?;
        Some(args)
    } else {
        None
    };
    Ok(GrantName { name, args })
}

pub fn parse_grant_objects<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<GrantObjects<'a>> {
    let kind = parse_kind_words(p, &["TO", "FROM"]);
    let names = p.parse_comma_separated(D::parse_grant_name)?;
    Ok(GrantObjects {
        kind: kind.into_iter().map(Cow::Borrowed).collect(),
        names,
    })
}

const GRANTEE_STOPS: [&str; 7] = [
    "WITH", "COPY", "REVOKE", "AS", "GRANTED", "CASCADE", "RESTRICT",
];

const GRANTEE_KINDS: [&[&str]; 8] = [
    &[],
    &["ROLE"],
    &["USER"],
    &["GROUP"],
    &["SHARE"],
    &["APPLICATION"],
    &["DATABASE", "ROLE"],
    &["APPLICATION", "ROLE"],
];

pub fn parse_grantee<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Grantee<'a>> {
    let kind = parse_kind_words(p, &GRANTEE_STOPS);
    if !GRANTEE_KINDS.iter().any(|known| {
        known.len() == kind.len()
            && known
                .iter()
                .zip(&kind)
                .all(|(word, text)| word.eq_ignore_ascii_case(text))
    }) {
        return Err(p.expected("a role, user or other kind of grantee"));
    }
    let name = parse_object_name(p)?;
    Ok(Grantee {
        kind: kind.into_iter().map(Cow::Borrowed).collect(),
        name,
        host: None,
    })
}

/// Parses the list of grantees. `PUBLIC` must be the only one.
pub fn parse_grantees<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Vec<Grantee<'a>>> {
    let grantees = p.parse_comma_separated(D::parse_grantee)?;
    let public = grantees.iter().any(|grantee| {
        matches!(grantee.name.parts.as_slice(), [only]
            if only.quote.is_none() && only.text.eq_ignore_ascii_case("PUBLIC"))
    });
    if public && grantees.len() > 1 {
        return Err(p.expected("PUBLIC on its own"));
    }
    Ok(grantees)
}

/// True at `[DATABASE] ROLE name TO`, which grants a role rather than a
/// privilege on an object.
fn at_role_grant<'a, D: Dialect>(p: &Parser<'a, D>) -> bool {
    let offset = usize::from(p.at_keyword("DATABASE"));
    p.peek_nth(offset).is_word("ROLE")
        && is_name_token(p.peek_nth(offset + 1))
        && !p.peek_nth(offset + 1).is_word("ON")
}

pub fn parse_grant<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Grant<'a>> {
    p.expect_keyword("GRANT")?;
    let (privileges, objects) = if at_role_grant(p) {
        let database = p.eat_keyword("DATABASE");
        p.expect_keyword("ROLE")?;
        let name = parse_object_name(p)?;
        (Privileges::Role { database, name }, None)
    } else {
        let privileges = parse_privileges(p)?;
        p.expect_keyword("ON")?;
        (privileges, Some(parse_grant_objects(p)?))
    };
    p.expect_keyword("TO")?;
    let grantees = D::parse_grantees(p)?;

    let with_grant_option = p.eat_keyword("WITH");
    if with_grant_option {
        p.expect_keywords(&["GRANT", "OPTION"])?;
    }
    let current_grants = if p.eat_keyword("COPY") {
        p.expect_keywords(&["CURRENT", "GRANTS"])?;
        Some(CurrentGrants::Copy)
    } else if p.eat_keyword("REVOKE") {
        p.expect_keywords(&["CURRENT", "GRANTS"])?;
        Some(CurrentGrants::Revoke)
    } else {
        None
    };
    let as_grantor = if p.eat_keyword("AS") {
        Some(p.parse_ident()?)
    } else {
        None
    };
    let granted_by = parse_granted_by(p)?;
    Ok(Grant {
        privileges,
        objects,
        grantees,
        with_grant_option,
        current_grants,
        as_grantor,
        granted_by,
    })
}

fn parse_granted_by<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Option<Ident<'a>>> {
    if !p.eat_keyword("GRANTED") {
        return Ok(None);
    }
    p.expect_keyword("BY")?;
    p.parse_ident().map(Some)
}

pub fn parse_revoke<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Revoke<'a>> {
    p.expect_keyword("REVOKE")?;
    let grant_option_for = p.at_keyword("GRANT") && p.peek_nth(1).is_word("OPTION");
    if grant_option_for {
        p.advance();
        p.advance();
        p.expect_keyword("FOR")?;
    }
    let privileges = parse_privileges(p)?;
    p.expect_keyword("ON")?;
    let objects = parse_grant_objects(p)?;
    p.expect_keyword("FROM")?;
    let grantees = D::parse_grantees(p)?;
    let granted_by = parse_granted_by(p)?;
    let behavior = if p.eat_keyword("CASCADE") {
        Some(DropBehavior::Cascade)
    } else if p.eat_keyword("RESTRICT") {
        Some(DropBehavior::Restrict)
    } else {
        None
    };
    Ok(Revoke {
        grant_option_for,
        privileges,
        objects,
        grantees,
        granted_by,
        behavior,
    })
}

pub fn parse_merge<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Merge<'a, D::Ext>> {
    p.expect_keyword("MERGE")?;
    let into = p.eat_keyword("INTO");
    let table = p.parse_table_ref()?;
    p.expect_keyword("USING")?;
    let source = p.parse_table_ref()?;
    p.expect_keyword("ON")?;
    let on = p.parse_expr()?;
    let mut clauses = Vec::new();
    while p.eat_keyword("WHEN") {
        clauses.push(parse_merge_clause(p)?);
    }
    let output = D::parse_output_clause(p)?;
    let returning = D::parse_returning(p)?;
    Ok(Merge {
        into,
        table,
        source,
        on,
        clauses,
        output,
        returning,
    })
}

fn parse_merge_clause_kind<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<MergeClauseKind> {
    if !p.eat_keyword("NOT") {
        p.expect_keyword("MATCHED")?;
        return Ok(MergeClauseKind::Matched);
    }
    p.expect_keyword("MATCHED")?;
    if !p.eat_keyword("BY") {
        return Ok(MergeClauseKind::NotMatched);
    }
    if p.eat_keyword("SOURCE") {
        return Ok(MergeClauseKind::NotMatchedBySource);
    }
    p.expect_keyword("TARGET")?;
    Ok(MergeClauseKind::NotMatchedByTarget)
}

fn parse_merge_clause<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<MergeClause<'a, D::Ext>> {
    let kind = parse_merge_clause_kind(p)?;
    let predicate = if p.eat_keyword("AND") {
        Some(p.parse_expr()?)
    } else {
        None
    };
    p.expect_keyword("THEN")?;
    let inserts = matches!(
        kind,
        MergeClauseKind::NotMatched | MergeClauseKind::NotMatchedByTarget
    );
    let action = if p.eat_keyword("DO") {
        p.expect_keyword("NOTHING")?;
        MergeAction::DoNothing
    } else if inserts {
        p.expect_keyword("INSERT")?;
        parse_merge_insert(p)?
    } else if p.eat_keyword("DELETE") {
        MergeAction::Delete
    } else {
        p.expect_keyword("UPDATE")?;
        parse_merge_update(p)?
    };
    Ok(MergeClause {
        kind,
        predicate,
        action,
    })
}

fn parse_merge_update<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<MergeAction<'a, D::Ext>> {
    p.expect_keyword("SET")?;
    let assignments = if p.eat_op("*") {
        None
    } else {
        Some(p.parse_comma_separated(D::parse_assignment)?)
    };
    let predicate = super::parse_optional_where(p)?;
    let delete_predicate = if p.eat_keyword("DELETE") {
        p.expect_keyword("WHERE")?;
        Some(p.parse_expr()?)
    } else {
        None
    };
    Ok(MergeAction::Update {
        assignments,
        predicate,
        delete_predicate,
    })
}

fn parse_merge_insert<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<MergeAction<'a, D::Ext>> {
    if p.eat_op("*") {
        return Ok(MergeAction::InsertAll);
    }
    let columns = if p.eat_op("(") {
        let columns = p.parse_comma_separated(parse_object_name)?;
        p.expect_op(")")?;
        columns
    } else {
        Vec::new()
    };
    if D::eat_merge_insert_row(p) {
        let predicate = super::parse_optional_where(p)?;
        return Ok(MergeAction::InsertRow { columns, predicate });
    }
    p.expect_keyword("VALUES")?;
    let rows = p.parse_comma_separated(super::parse_values_row)?;
    let predicate = super::parse_optional_where(p)?;
    Ok(MergeAction::Insert {
        columns,
        rows,
        predicate,
    })
}

/// True at `DROP FUNCTION` or `DROP PROCEDURE`.
pub fn at_drop_function<'a, D: Dialect>(p: &Parser<'a, D>) -> bool {
    let next = p.peek_nth(1);
    p.at_keyword("DROP") && (next.is_word("FUNCTION") || next.is_word("PROCEDURE"))
}

pub fn parse_routine_arg<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<RoutineArg<'a, D::Ext>> {
    let mode = ["IN", "OUT", "INOUT", "VARIADIC"]
        .into_iter()
        .find(|mode| p.eat_keyword(mode));
    // A name is only present when a type follows it, and these words continue
    // a multi-word type instead.
    let next = p.peek_nth(1);
    let continues_type = ["PRECISION", "VARYING", "WITH", "WITHOUT"]
        .iter()
        .any(|word| next.is_word(word));
    let name = if p.at_ident() && is_name_token(next) && !continues_type {
        Some(p.parse_ident()?)
    } else {
        None
    };
    let data_type = D::parse_data_type(p)?;
    let default = if p.eat_op("=") {
        Some((true, p.parse_expr()?))
    } else if p.eat_keyword("DEFAULT") {
        Some((false, p.parse_expr()?))
    } else {
        None
    };
    Ok(RoutineArg {
        mode: mode.map(Cow::Borrowed),
        name,
        data_type,
        default,
    })
}

fn parse_routine_signature<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<RoutineSignature<'a, D::Ext>> {
    let name = parse_object_name(p)?;
    let args = if !p.eat_op("(") {
        None
    } else if p.eat_op(")") {
        Some(Vec::new())
    } else {
        let args = p.parse_comma_separated(parse_routine_arg)?;
        p.expect_op(")")?;
        Some(args)
    };
    Ok(RoutineSignature { name, args })
}

/// Parses `DROP {FUNCTION | PROCEDURE} [IF EXISTS] signature, ...`.
pub fn parse_drop_routine<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<DropRoutine<'a, D::Ext>> {
    p.expect_keyword("DROP")?;
    let procedure = p.at_keyword("PROCEDURE");
    p.advance();
    let if_exists = super::parse_if_exists(p);
    let routines = p.parse_comma_separated(parse_routine_signature)?;
    let behavior = super::parse_drop_behavior(p);
    Ok(DropRoutine {
        procedure,
        if_exists,
        routines,
        behavior,
    })
}

/// Consumes one of `kinds` as the object type of a `DROP`.
pub fn eat_drop_object_type<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
    kinds: &[&'static str],
) -> Option<ObjectType<'a>> {
    kinds
        .iter()
        .find(|kind| p.eat_keyword(kind))
        .map(|kind| ObjectType::Named((*kind).into()))
}

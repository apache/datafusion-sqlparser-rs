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

//! `INSERT`, `UPDATE` and `DELETE`.

use super::*;

/// True at a `(` that starts a column list rather than a parenthesized query.
pub fn at_column_list<'a, D: Dialect>(p: &Parser<'a, D>) -> bool {
    let next = p.peek_nth(1);
    p.at_op("(") && !next.is_op("(") && !next.is_word("SELECT") && !next.is_word("WITH")
}

pub fn parse_insert<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Insert<'a, D::Ext>> {
    p.expect_keyword("INSERT")?;
    parse_insert_after_verb(p)
}

/// Parses an `INSERT` once its opening keyword has been read.
pub fn parse_insert_after_verb<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<Insert<'a, D::Ext>> {
    let modifiers = D::parse_insert_modifiers(p)?;
    let into = D::parse_insert_into(p)?;
    let table = parse_object_name(p)?;
    let columns = D::parse_insert_columns(p)?;
    let output = D::parse_output_clause(p)?;
    let source = D::parse_insert_source(p)?;
    check_default_values(p, &columns, &source)?;
    let returning = D::parse_returning(p)?;
    Ok(Insert {
        modifiers,
        into,
        table,
        columns,
        output,
        source,
        returning,
    })
}

/// Parses the optional column list of an `INSERT`.
pub fn parse_insert_columns<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Vec<Ident<'a>>> {
    if at_column_list(p) {
        parse_ident_list(p)
    } else {
        Ok(Vec::new())
    }
}

/// Parses `VALUES` rows or a query.
pub fn parse_insert_source<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<InsertSource<'a, D::Ext>> {
    if p.eat_keyword("VALUES") {
        return Ok(InsertSource::Values(parse_values_rows(p)?));
    }
    Ok(InsertSource::Query(Box::new(D::parse_query(p)?)))
}

/// Rejects `DEFAULT VALUES` after a column list.
pub fn check_default_values<'a, D: Dialect>(
    p: &Parser<'a, D>,
    columns: &[Ident<'a>],
    source: &InsertSource<'a, D::Ext>,
) -> PResult<()> {
    if !columns.is_empty() && matches!(source, InsertSource::DefaultValues) {
        return Err(p.expected("no column list before DEFAULT VALUES"));
    }
    Ok(())
}

/// Parses `DEFAULT VALUES` or the usual source, for dialects that have it.
pub fn parse_insert_source_with_default<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<InsertSource<'a, D::Ext>> {
    if p.at_keyword("DEFAULT") && p.peek_nth(1).is_word("VALUES") {
        p.advance();
        p.advance();
        return Ok(InsertSource::DefaultValues);
    }
    parse_insert_source(p)
}

/// Parses `FROM` tables, for an `UPDATE` that has them.
pub fn parse_optional_update_from<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<Vec<TableWithJoins<'a, D::Ext>>> {
    if p.eat_keyword("FROM") {
        D::parse_from_list(p)
    } else {
        Ok(Vec::new())
    }
}

/// Parses `USING` tables, for a `DELETE` that has them.
pub fn parse_optional_delete_using<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<Vec<TableWithJoins<'a, D::Ext>>> {
    if p.eat_keyword("USING") {
        D::parse_from_list(p)
    } else {
        Ok(Vec::new())
    }
}

/// Parses rows, reserving each one's space from the width of the one before.
fn parse_values_rows<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Vec<Vec<Expr<'a, D::Ext>>>> {
    let mut width = 1;
    p.parse_comma_separated(|p| {
        let row = parse_row_of_width(p, width)?;
        width = row.len();
        Ok(row)
    })
}

pub fn parse_values_row<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Vec<Expr<'a, D::Ext>>> {
    parse_row_of_width(p, 1)
}

fn parse_row_of_width<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
    width: usize,
) -> PResult<Vec<Expr<'a, D::Ext>>> {
    p.expect_op("(")?;
    let mut row = Vec::with_capacity(width);
    row.push(p.parse_expr()?);
    while p.eat_op(",") {
        row.push(p.parse_expr()?);
    }
    p.expect_op(")")?;
    Ok(row)
}

pub fn parse_update<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Update<'a, D::Ext>> {
    p.expect_keyword("UPDATE")?;
    let table = p.parse_table_ref()?;
    let before_set = D::parse_update_from_before_set(p)?;
    p.expect_keyword("SET")?;
    let assignments = p.parse_comma_separated(D::parse_assignment)?;
    let from_before_set = !before_set.is_empty();
    let from = if from_before_set {
        before_set
    } else {
        D::parse_update_from_after_set(p)?
    };
    let output = D::parse_output_clause(p)?;
    let selection = parse_optional_where(p)?;
    let returning = D::parse_returning(p)?;
    Ok(Update {
        table,
        joins: Vec::new(),
        assignments,
        from,
        from_before_set,
        output,
        selection,
        order_by: Vec::new(),
        limit: None,
        returning,
    })
}

pub fn parse_assignment<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Assignment<'a, D::Ext>> {
    let target = parse_object_name(p)?;
    p.expect_op("=")?;
    let value = p.parse_expr()?;
    Ok(Assignment { target, value })
}

pub fn parse_delete<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Delete<'a, D::Ext>> {
    p.expect_keyword("DELETE")?;
    p.expect_keyword("FROM")?;
    let table = p.parse_table_ref()?;
    let using = D::parse_delete_using(p)?;
    let output = D::parse_output_clause(p)?;
    let selection = parse_optional_where(p)?;
    let returning = D::parse_returning(p)?;
    Ok(Delete {
        table,
        multi: None,
        using,
        output,
        selection,
        order_by: Vec::new(),
        limit: None,
        returning,
    })
}

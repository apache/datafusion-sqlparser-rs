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

//! `PIVOT`, `UNPIVOT` and sampling clauses, for the dialects that have them.

use super::*;

pub fn parse_expr_with_alias<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<ExprWithAlias<'a, D::Ext>> {
    let expr = p.parse_expr()?;
    let alias = parse_optional_alias(p)?;
    Ok(ExprWithAlias { expr, alias })
}

/// Parses the values after `IN`, a list of optionally aliased expressions.
pub fn parse_pivot_source<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<PivotSource<'a, D::Ext>> {
    p.parse_comma_separated(parse_expr_with_alias)
        .map(PivotSource::List)
}

/// Parses `(aggregates FOR columns IN (values)`, up to but not including the
/// closing parenthesis, which a dialect can precede with its own options.
pub fn parse_pivot_head<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<PivotClause<'a, D::Ext>> {
    p.expect_op("(")?;
    let aggregates = p.parse_comma_separated(parse_expr_with_alias)?;
    p.expect_keyword("FOR")?;
    let column = |p: &mut Parser<'a, D>| p.parse_expr_bp(prec::COMPARE);
    let columns = if p.eat_op("(") {
        let columns = p.parse_comma_separated(column)?;
        p.expect_op(")")?;
        columns
    } else {
        vec![column(p)?]
    };
    p.expect_keyword("IN")?;
    p.expect_op("(")?;
    let source = D::parse_pivot_source(p)?;
    p.expect_op(")")?;
    Ok(PivotClause {
        aggregates,
        columns,
        source,
        default_on_null: None,
    })
}

/// Parses what follows `UNPIVOT`.
pub fn parse_unpivot_clause<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<UnpivotClause<'a, D::Ext>> {
    let include_nulls = if p.eat_keyword("INCLUDE") {
        p.expect_keyword("NULLS")?;
        Some(true)
    } else if p.eat_keyword("EXCLUDE") {
        p.expect_keyword("NULLS")?;
        Some(false)
    } else {
        None
    };
    p.expect_op("(")?;
    let value = p.parse_expr()?;
    p.expect_keyword("FOR")?;
    let name = p.parse_ident()?;
    p.expect_keyword("IN")?;
    p.expect_op("(")?;
    let columns = p.parse_comma_separated(parse_expr_with_alias)?;
    p.expect_op(")")?;
    p.expect_op(")")?;
    Ok(UnpivotClause {
        include_nulls,
        value,
        name,
        columns,
    })
}

/// Parses what follows `SAMPLE` or `TABLESAMPLE`: an amount, which can be in
/// parentheses.
pub fn parse_sample_clause<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
    long_form: bool,
) -> PResult<SampleClause<'a, D::Ext>> {
    let parenthesized = p.eat_op("(");
    let amount = parse_sample_amount(p)?;
    if parenthesized {
        p.expect_op(")")?;
    }
    Ok(SampleClause {
        long_form,
        method: None,
        amount,
        parenthesized,
        seed: None,
        offset: None,
    })
}

/// Parses how much of a table to sample.
pub fn parse_sample_amount<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<SampleAmount<'a, D::Ext>> {
    let value = p.parse_expr()?;
    let unit = if p.eat_keyword("ROWS") {
        Some(SampleUnit::Rows)
    } else if p.eat_keyword("PERCENT") {
        Some(SampleUnit::Percent)
    } else {
        None
    };
    Ok(SampleAmount::Quantity { value, unit })
}

pub fn parse_number<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<&'a str> {
    let token = p.expect_kind(TokenKind::Number, "a number")?;
    Ok(token.text)
}

pub fn parse_sample_seed<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Option<SampleSeed<'a>>> {
    let repeatable = p.at_keyword("REPEATABLE");
    if !repeatable && !p.at_keyword("SEED") {
        return Ok(None);
    }
    p.advance();
    p.expect_op("(")?;
    let value = parse_number(p)?;
    p.expect_op(")")?;
    Ok(Some(SampleSeed {
        repeatable,
        value: value.into(),
    }))
}

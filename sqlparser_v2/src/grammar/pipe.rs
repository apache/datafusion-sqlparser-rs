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

//! Pipe syntax, for the dialects that have it: `SELECT * FROM t |> WHERE x |> SELECT y`.

use super::*;

/// Parses every `|> ...` step at the current position.
pub fn parse_pipe_operators<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<Vec<PipeOperator<'a, D::Ext>>> {
    let mut operators = Vec::new();
    while p.eat_op("|>") {
        operators.push(parse_pipe_operator(p)?);
    }
    Ok(operators)
}

/// Parses one step, after its `|>`.
///
/// Each step has its own function because queries can nest through any of
/// them, and one function holding them all would need a very large stack frame
/// in debug builds at every level.
pub fn parse_pipe_operator<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<PipeOperator<'a, D::Ext>> {
    if p.eat_keyword("SELECT") {
        return pipe_select(p);
    }
    if p.eat_keyword("EXTEND") {
        return pipe_extend(p);
    }
    if p.eat_keyword("SET") {
        return pipe_set(p);
    }
    if p.eat_keyword("DROP") {
        return pipe_drop(p);
    }
    if p.eat_keyword("AS") {
        return pipe_as(p);
    }
    if p.eat_keyword("WHERE") {
        return pipe_where(p);
    }
    if p.eat_keyword("LIMIT") {
        return pipe_limit(p);
    }
    if p.eat_keyword("ORDER") {
        return pipe_order_by(p);
    }
    if p.eat_keyword("AGGREGATE") {
        return parse_aggregate(p);
    }
    if p.eat_keyword("TABLESAMPLE") {
        return pipe_sample(p);
    }
    if p.eat_keyword("RENAME") {
        return pipe_rename(p);
    }
    if p.eat_keyword("UNION") {
        return parse_set_operation(p, SetOperator::Union);
    }
    if p.eat_keyword("INTERSECT") {
        return parse_set_operation(p, SetOperator::Intersect);
    }
    if p.eat_keyword("EXCEPT") {
        return parse_set_operation(p, SetOperator::Except);
    }
    if p.eat_keyword("CALL") {
        return pipe_call(p);
    }
    if p.eat_keyword("PIVOT") {
        return pipe_pivot(p);
    }
    if p.eat_keyword("UNPIVOT") {
        return pipe_unpivot(p);
    }
    pipe_join(p)
}

fn pipe_select<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<PipeOperator<'a, D::Ext>> {
    Ok(PipeOperator::Select(
        p.parse_comma_separated(Parser::parse_select_item)?,
    ))
}

fn pipe_extend<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<PipeOperator<'a, D::Ext>> {
    Ok(PipeOperator::Extend(
        p.parse_comma_separated(Parser::parse_select_item)?,
    ))
}

fn pipe_set<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<PipeOperator<'a, D::Ext>> {
    Ok(PipeOperator::Set(
        p.parse_comma_separated(D::parse_assignment)?,
    ))
}

fn pipe_drop<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<PipeOperator<'a, D::Ext>> {
    Ok(PipeOperator::Drop(
        p.parse_comma_separated(Parser::parse_ident)?,
    ))
}

fn pipe_as<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<PipeOperator<'a, D::Ext>> {
    Ok(PipeOperator::As(p.parse_ident()?))
}

fn pipe_where<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<PipeOperator<'a, D::Ext>> {
    Ok(PipeOperator::Where(p.parse_expr()?))
}

fn pipe_limit<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<PipeOperator<'a, D::Ext>> {
    let count = p.parse_expr()?;
    let offset = if p.eat_keyword("OFFSET") {
        Some(p.parse_expr()?)
    } else {
        None
    };
    Ok(PipeOperator::Limit { count, offset })
}

fn pipe_order_by<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<PipeOperator<'a, D::Ext>> {
    p.expect_keyword("BY")?;
    Ok(PipeOperator::OrderBy(
        p.parse_comma_separated(D::parse_order_by_item)?,
    ))
}

fn pipe_sample<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<PipeOperator<'a, D::Ext>> {
    Ok(PipeOperator::TableSample(Box::new(D::parse_sample_clause(
        p, true,
    )?)))
}

fn pipe_rename<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<PipeOperator<'a, D::Ext>> {
    Ok(PipeOperator::Rename(p.parse_comma_separated(parse_rename)?))
}

fn pipe_call<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<PipeOperator<'a, D::Ext>> {
    let name = parse_object_name(p)?;
    let function = p.parse_function_call(name)?;
    let alias = parse_optional_alias(p)?;
    Ok(PipeOperator::Call { function, alias })
}

fn pipe_pivot<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<PipeOperator<'a, D::Ext>> {
    let clause = Box::new(parse_pivot_head(p)?);
    p.expect_op(")")?;
    let alias = parse_optional_alias(p)?;
    Ok(PipeOperator::Pivot { clause, alias })
}

fn pipe_unpivot<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<PipeOperator<'a, D::Ext>> {
    let clause = Box::new(parse_unpivot_clause(p)?);
    let alias = parse_optional_alias(p)?;
    Ok(PipeOperator::Unpivot { clause, alias })
}

fn pipe_join<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<PipeOperator<'a, D::Ext>> {
    match D::parse_join(p)? {
        Some(join) => Ok(PipeOperator::Join(Box::new(join))),
        None => Err(p.expected("a pipe operator")),
    }
}

/// Parses what follows `AGGREGATE`: `[items] [GROUP BY items]`.
fn parse_aggregate<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<PipeOperator<'a, D::Ext>> {
    let aggregates = if p.at_keyword("GROUP") {
        Vec::new()
    } else {
        p.parse_comma_separated(parse_aggregate_item)?
    };
    let group_by = if p.eat_keyword("GROUP") {
        p.expect_keyword("BY")?;
        p.parse_comma_separated(parse_aggregate_item)?
    } else {
        Vec::new()
    };
    Ok(PipeOperator::Aggregate {
        aggregates,
        group_by,
    })
}

fn parse_aggregate_item<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<PipeAggregate<'a, D::Ext>> {
    let expr = parse_expr_with_alias(p)?;
    let asc = if p.eat_keyword("ASC") {
        Some(true)
    } else if p.eat_keyword("DESC") {
        Some(false)
    } else {
        None
    };
    Ok(PipeAggregate { expr, asc })
}

/// Parses `name [AS] new_name`.
fn parse_rename<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<PipeRename<'a>> {
    let name = p.parse_ident()?;
    p.eat_keyword("AS");
    let new_name = p.parse_ident()?;
    Ok(PipeRename { name, new_name })
}

/// Parses what follows a set operator. Only `UNION` can omit `DISTINCT`.
fn parse_set_operation<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
    op: SetOperator,
) -> PResult<PipeOperator<'a, D::Ext>> {
    let quantifier = super::query::parse_quantifier(p);
    if op != SetOperator::Union && quantifier != SetQuantifier::Distinct {
        return Err(p.expected("DISTINCT"));
    }
    let by_name = p.eat_keyword("BY");
    if by_name {
        p.expect_keyword("NAME")?;
    }
    let queries = p.parse_comma_separated(parse_parenthesized_query)?;
    Ok(PipeOperator::SetOperation {
        op,
        quantifier,
        by_name,
        queries,
    })
}

fn parse_parenthesized_query<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Query<'a, D::Ext>> {
    p.expect_op("(")?;
    let query = p.nested(D::parse_query)?;
    p.expect_op(")")?;
    Ok(query)
}

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

//! `CONNECT BY` and `MATCH_RECOGNIZE`.

use super::*;

/// Set while parsing a `CONNECT BY` clause, where `PRIOR` is an operator.
pub const CONTEXT_CONNECT_BY: u32 = 1;

/// Parses any `CONNECT BY` and `START WITH` clauses, in either order.
pub fn parse_connect_by_clauses<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<Vec<ConnectBy<'a, D::Ext>>> {
    let mut clauses = Vec::new();
    loop {
        if p.at_keyword("START") && p.peek_nth(1).is_word("WITH") {
            p.advance();
            p.advance();
            clauses.push(ConnectBy::StartWith(p.parse_expr()?));
        } else if p.at_keyword("CONNECT") && p.peek_nth(1).is_word("BY") {
            p.advance();
            p.advance();
            let nocycle = p.eat_keyword("NOCYCLE");
            let relationships = p.with_context(CONTEXT_CONNECT_BY, |p| {
                p.parse_comma_separated(Parser::parse_expr)
            })?;
            clauses.push(ConnectBy::ConnectBy {
                nocycle,
                relationships,
            });
        } else {
            return Ok(clauses);
        }
    }
}

/// Parses what follows `MATCH_RECOGNIZE`.
pub fn parse_match_recognize<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
    table: TableRef<'a, D::Ext>,
) -> PResult<TableFactor<'a, D::Ext>> {
    p.expect_op("(")?;
    let partition_by = if p.eat_keyword("PARTITION") {
        p.expect_keyword("BY")?;
        p.parse_comma_separated(Parser::parse_expr)?
    } else {
        Vec::new()
    };
    let order_by = parse_optional_order_by(p)?;
    let measures = if p.eat_keyword("MEASURES") {
        p.parse_comma_separated(|p| {
            let expr = p.parse_expr()?;
            p.eat_keyword("AS");
            Ok((expr, p.parse_ident()?))
        })?
    } else {
        Vec::new()
    };
    let rows_per_match = parse_rows_per_match(p)?;
    let after_match_skip = parse_after_match_skip(p)?;
    p.expect_keyword("PATTERN")?;
    p.expect_op("(")?;
    let pattern = parse_match_pattern(p)?;
    p.expect_op(")")?;
    p.expect_keyword("DEFINE")?;
    let symbols = p.parse_comma_separated(|p| {
        let symbol = p.parse_ident()?;
        p.expect_keyword("AS")?;
        Ok((symbol, p.parse_expr()?))
    })?;
    p.expect_op(")")?;
    Ok(TableFactor::MatchRecognize(Box::new(MatchRecognize {
        table,
        partition_by,
        order_by,
        measures,
        rows_per_match,
        after_match_skip,
        pattern,
        symbols,
    })))
}

fn parse_rows_per_match<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Option<RowsPerMatch>> {
    if p.eat_keyword("ONE") {
        p.expect_keywords(&["ROW", "PER", "MATCH"])?;
        return Ok(Some(RowsPerMatch::OneRow));
    }
    if !p.eat_keyword("ALL") {
        return Ok(None);
    }
    p.expect_keywords(&["ROWS", "PER", "MATCH"])?;
    let empty = if p.eat_keyword("SHOW") {
        p.expect_keywords(&["EMPTY", "MATCHES"])?;
        Some(EmptyMatches::Show)
    } else if p.eat_keyword("OMIT") {
        p.expect_keywords(&["EMPTY", "MATCHES"])?;
        Some(EmptyMatches::Omit)
    } else if p.eat_keyword("WITH") {
        p.expect_keywords(&["UNMATCHED", "ROWS"])?;
        Some(EmptyMatches::WithUnmatchedRows)
    } else {
        None
    };
    Ok(Some(RowsPerMatch::AllRows(empty)))
}

fn parse_after_match_skip<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<Option<AfterMatchSkip<'a>>> {
    if !p.eat_keyword("AFTER") {
        return Ok(None);
    }
    p.expect_keywords(&["MATCH", "SKIP"])?;
    if p.eat_keyword("PAST") {
        p.expect_keywords(&["LAST", "ROW"])?;
        return Ok(Some(AfterMatchSkip::PastLastRow));
    }
    p.expect_keyword("TO")?;
    Ok(Some(if p.eat_keyword("NEXT") {
        p.expect_keyword("ROW")?;
        AfterMatchSkip::ToNextRow
    } else if p.eat_keyword("FIRST") {
        AfterMatchSkip::ToFirst(p.parse_ident()?)
    } else {
        p.expect_keyword("LAST")?;
        AfterMatchSkip::ToLast(p.parse_ident()?)
    }))
}

fn parse_match_pattern<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<MatchPattern<'a>> {
    let first = parse_pattern_sequence(p)?;
    if !p.at_op("|") {
        return Ok(first);
    }
    let mut branches = vec![first];
    while p.eat_op("|") {
        branches.push(parse_pattern_sequence(p)?);
    }
    Ok(MatchPattern::Alternation(branches))
}

fn parse_pattern_sequence<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<MatchPattern<'a>> {
    let mut patterns = vec![parse_pattern_repetition(p)?];
    while !p.at_op(")") && !p.at_op("|") {
        patterns.push(parse_pattern_repetition(p)?);
    }
    if patterns.len() == 1 {
        return Ok(patterns.remove(0));
    }
    Ok(MatchPattern::Concat(patterns))
}

fn at_question_mark<'a, D: Dialect>(p: &Parser<'a, D>) -> bool {
    let token = p.peek();
    token.kind == TokenKind::Placeholder && token.text == "?"
}

fn parse_pattern_repetition<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<MatchPattern<'a>> {
    let pattern = parse_pattern_atom(p)?;
    let quantifier = if p.eat_op("*") {
        PatternQuantifier::ZeroOrMore
    } else if p.eat_op("+") {
        PatternQuantifier::OneOrMore
    } else if at_question_mark(p) {
        p.advance();
        PatternQuantifier::AtMostOne
    } else if p.eat_op("{") {
        parse_pattern_bounds(p)?
    } else {
        return Ok(pattern);
    };
    let reluctant = at_question_mark(p);
    if reluctant {
        p.advance();
    }
    Ok(MatchPattern::Repetition {
        pattern: Box::new(pattern),
        quantifier,
        reluctant,
    })
}

/// Parses `n}`, `n,}`, `,m}` or `n,m}`, with the `{` consumed.
fn parse_pattern_bounds<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<PatternQuantifier<'a>> {
    let quantifier = if p.eat_op(",") {
        PatternQuantifier::AtMost(parse_number_text(p)?.into())
    } else {
        let low = parse_number_text(p)?;
        if !p.eat_op(",") {
            PatternQuantifier::Exactly(low.into())
        } else if p.at_op("}") {
            PatternQuantifier::AtLeast(low.into())
        } else {
            PatternQuantifier::Range(low.into(), parse_number_text(p)?.into())
        }
    };
    p.expect_op("}")?;
    Ok(quantifier)
}

fn parse_pattern_atom<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<MatchPattern<'a>> {
    if p.eat_op("^") {
        return Ok(MatchPattern::Start);
    }
    if p.eat_op("$") {
        return Ok(MatchPattern::End);
    }
    if p.eat_op("(") {
        let pattern = p.nested(parse_match_pattern)?;
        p.expect_op(")")?;
        return Ok(MatchPattern::Group(Box::new(pattern)));
    }
    if p.eat_op("{") {
        p.expect_op("-")?;
        let symbol = p.parse_ident()?;
        p.expect_op("-")?;
        p.expect_op("}")?;
        return Ok(MatchPattern::Exclude(symbol));
    }
    if p.at_keyword("PERMUTE") && p.peek_nth(1).is_op("(") {
        p.advance();
        return parse_ident_list(p).map(MatchPattern::Permute);
    }
    p.parse_ident().map(MatchPattern::Symbol)
}

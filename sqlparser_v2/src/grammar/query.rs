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

//! Queries: `SELECT`, set operations, tables, joins, and the clauses after them.

use super::*;
use crate::ast::ExprKind;
use crate::ast::SelectItemKind;
use crate::ast::Span;
use std::borrow::Cow;

pub fn parse_query<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Query<'a, D::Ext>> {
    let with = if p.eat_keyword("WITH") {
        let recursive = D::eat_with_recursive(p);
        let ctes = p.parse_comma_separated(D::parse_cte)?;
        Some(Box::new(With { recursive, ctes }))
    } else {
        None
    };
    parse_query_after_with(p, with)
}

/// Consumes `RECURSIVE` after `WITH`. It is a CTE's name instead when `AS` or
/// a column list follows it.
pub fn eat_with_recursive<'a, D: Dialect>(p: &mut Parser<'a, D>) -> bool {
    let next = p.peek_nth(1);
    let names_a_cte =
        matches!(next.kind, TokenKind::Word | TokenKind::QuotedIdent) && !next.is_word("AS");
    names_a_cte && p.eat_keyword("RECURSIVE")
}

/// Parses the rest of a query once its `WITH` clause has been read.
pub fn parse_query_after_with<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
    with: Option<Box<With<'a, D::Ext>>>,
) -> PResult<Query<'a, D::Ext>> {
    let statement = if with.is_none() {
        None
    } else {
        D::parse_statement_after_with(p)?
    };
    if let Some(statement) = statement {
        return Ok(statement_query::<D>(with, statement));
    }
    let body = parse_set_expr(p, 0)?;

    let order_by = if p.eat_keyword("ORDER") {
        p.expect_keyword("BY")?;
        D::parse_query_order_by(p)?
    } else {
        Vec::new()
    };

    let distribution = D::parse_query_distribution(p)?;

    let limit = if p.eat_keyword("LIMIT") {
        Some(Box::new(D::parse_limit(p)?))
    } else if p.at_keyword("OFFSET") {
        Some(Box::new(parse_offset_first(p)?))
    } else {
        None
    };

    let fetch = if p.eat_keyword("FETCH") {
        Some(Box::new(parse_fetch(p)?))
    } else {
        None
    };

    let mut locks = Vec::new();
    while p.at_keyword("FOR") && {
        let next = p.peek_nth(1);
        next.is_word("UPDATE") || next.is_word("SHARE")
    } {
        p.advance();
        locks.push(parse_lock(p)?);
    }

    let limit = match limit {
        Some(limit) => Some(limit),
        None if !locks.is_empty() => D::parse_limit_after_locks(p)?,
        None => None,
    };

    let tail = D::parse_query_tail(p)?;

    let pipes = D::parse_pipe_operators(p)?;

    Ok(Query {
        with,
        body,
        order_by,
        distribution,
        limit,
        fetch,
        locks,
        tail,
        pipes,
    })
}

/// Wraps a statement that follows a `WITH` clause as a query.
fn statement_query<'a, D: Dialect>(
    with: Option<Box<With<'a, D::Ext>>>,
    statement: Statement<'a, D::Ext>,
) -> Query<'a, D::Ext> {
    Query {
        with,
        body: SetExpr::Statement(Box::new(statement)),
        order_by: Vec::new(),
        distribution: None,
        limit: None,
        fetch: None,
        locks: Vec::new(),
        tail: Default::default(),
        pipes: Vec::new(),
    }
}

/// Parses `INSERT`, `UPDATE` or `DELETE` after a `WITH` clause, each only if
/// its flag is set. Returns `None` at anything else.
pub fn parse_dml_after_with<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
    insert: bool,
    update_or_delete: bool,
) -> PResult<Option<Statement<'a, D::Ext>>> {
    let allowed = (insert && p.at_keyword("INSERT"))
        || (update_or_delete && (p.at_keyword("UPDATE") || p.at_keyword("DELETE")));
    if !allowed {
        return Ok(None);
    }
    p.parse_statement().map(Some)
}

/// Parses `TABLE name` or a query, for a `CREATE TABLE ... AS` that has both.
pub fn parse_table_or_query<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Query<'a, D::Ext>> {
    if !p.at_keyword("TABLE") {
        return D::parse_query(p);
    }
    p.advance();
    let name = parse_object_name(p)?;
    Ok(Query {
        with: None,
        body: SetExpr::Table(name),
        order_by: Vec::new(),
        distribution: None,
        limit: None,
        fetch: None,
        locks: Vec::new(),
        tail: Default::default(),
        pipes: Vec::new(),
    })
}

/// Parses `INTO [TEMPORARY | TEMP | UNLOGGED] [TABLE] name`, for a `SELECT`
/// that creates a table.
pub fn parse_select_into_table<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<Option<Box<SelectInto<'a, D::Ext>>>> {
    if !p.eat_keyword("INTO") {
        return Ok(None);
    }
    let token = p.peek();
    let modifier = ["TEMPORARY", "TEMP", "UNLOGGED"]
        .iter()
        .find(|word| p.eat_keyword(word))
        .map(|_| token.text);
    let table_keyword = p.eat_keyword("TABLE");
    let name = parse_object_name(p)?;
    Ok(Some(Box::new(SelectInto {
        modifier: modifier.map(Cow::Borrowed),
        table_keyword,
        targets: vec![name_expr(name)],
    })))
}

/// Parses `INTO a, b, ...`, for a `SELECT` that stores into variables.
pub fn parse_select_into_variables<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<Option<Box<SelectInto<'a, D::Ext>>>> {
    if !p.eat_keyword("INTO") {
        return Ok(None);
    }
    let targets = p.parse_comma_separated(|p| p.spanned_expr(parse_into_target))?;
    Ok(Some(Box::new(SelectInto {
        modifier: None,
        table_keyword: false,
        targets,
    })))
}

/// A variable after `INTO`: a name or a placeholder.
fn parse_into_target<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Expr<'a, D::Ext>> {
    let token = p.peek();
    if token.kind == TokenKind::Placeholder {
        p.advance();
        return Ok(ExprKind::Placeholder(token.text.into()).into());
    }
    Ok(name_expr(parse_object_name(p)?))
}

/// Builds the expression that names `name`.
fn name_expr<'a, X: crate::dialect::AstExt>(mut name: ObjectName<'a>) -> Expr<'a, X> {
    let span = crate::ast::Span {
        start: name.parts[0].span.start,
        end: name.parts[name.parts.len() - 1].span.end,
    };
    let kind = if name.parts.len() == 1 {
        ExprKind::Ident(name.parts.remove(0))
    } else {
        ExprKind::Compound(name)
    };
    Expr::new(kind, span)
}

/// Parses `OFFSET n [ROWS] [LIMIT m]`.
fn parse_offset_first<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Limit<'a, D::Ext>> {
    p.expect_keyword("OFFSET")?;
    let offset = Some(p.parse_expr()?);
    let offset_rows = parse_offset_rows(p);
    let mut all = false;
    let count = if p.eat_keyword("LIMIT") {
        all = D::eat_limit_all(p);
        if all { None } else { Some(p.parse_expr()?) }
    } else {
        None
    };
    Ok(Limit {
        count,
        offset,
        offset_rows,
        offset_comma: false,
        all,
        by: Vec::new(),
    })
}

fn parse_offset_rows<'a, D: Dialect>(p: &mut Parser<'a, D>) -> OffsetRows {
    if p.eat_keyword("ROWS") {
        OffsetRows::Rows
    } else if p.eat_keyword("ROW") {
        OffsetRows::Row
    } else {
        OffsetRows::None
    }
}

/// Parses what follows the `FETCH` keyword.
fn parse_fetch<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Fetch<'a, D::Ext>> {
    if !p.eat_keyword("FIRST") {
        p.eat_keyword("NEXT");
    }
    let (quantity, percent) = if p.eat_keyword("ROW") || p.eat_keyword("ROWS") {
        (None, false)
    } else {
        if !matches!(
            p.peek().kind,
            TokenKind::Number | TokenKind::String | TokenKind::Placeholder
        ) {
            return Err(p.expected("a row count"));
        }
        let quantity = p.parse_prefix()?;
        let percent = p.eat_keyword("PERCENT");
        if !p.eat_keyword("ROW") {
            p.eat_keyword("ROWS");
        }
        (Some(quantity), percent)
    };
    let with_ties = if p.eat_keyword("ONLY") {
        false
    } else if p.eat_keyword("WITH") {
        p.expect_keyword("TIES")?;
        true
    } else {
        false
    };
    Ok(Fetch {
        quantity,
        percent,
        with_ties,
    })
}

/// Parses what follows the `FOR` keyword.
fn parse_lock<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<LockClause<'a>> {
    let lock_type = if p.eat_keyword("UPDATE") {
        LockType::Update
    } else {
        p.expect_keyword("SHARE")?;
        LockType::Share
    };
    let of = if p.eat_keyword("OF") {
        Some(parse_object_name(p)?)
    } else {
        None
    };
    let nonblock = if p.eat_keyword("NOWAIT") {
        Some(NonBlock::Nowait)
    } else if p.eat_keyword("SKIP") {
        p.expect_keyword("LOCKED")?;
        Some(NonBlock::SkipLocked)
    } else {
        None
    };
    Ok(LockClause {
        lock_type,
        of,
        nonblock,
    })
}

pub fn parse_select<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Select<'a, D::Ext>> {
    let start = p.peek().span.start;
    p.expect_keyword("SELECT")?;
    let (modifiers, quantifier) = D::parse_select_head(p)?;
    let projection = D::parse_projection(p)?;
    let into = D::parse_select_into(p)?;

    let from = if p.eat_keyword("FROM") {
        D::parse_from_list(p)?
    } else {
        Vec::new()
    };

    let select = parse_select_tail(p, modifiers, quantifier, projection, into, from, false)?;
    Ok(Select {
        span: p.span_since(start),
        ..select
    })
}

/// Parses `FROM tables [SELECT items]` and the clauses that follow.
pub fn parse_from_first_select<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<Select<'a, D::Ext>> {
    let start = p.peek().span.start;
    p.expect_keyword("FROM")?;
    let from = D::parse_from_list(p)?;
    let (modifiers, quantifier, projection) = if p.eat_keyword("SELECT") {
        let (modifiers, quantifier) = D::parse_select_head(p)?;
        (modifiers, quantifier, D::parse_projection(p)?)
    } else {
        (Default::default(), SetQuantifier::None, Vec::new())
    };
    let select = parse_select_tail(p, modifiers, quantifier, projection, None, from, true)?;
    Ok(Select {
        span: p.span_since(start),
        ..select
    })
}

/// Parses the clauses after the projection and `FROM`, whichever came first.
fn parse_select_tail<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
    modifiers: crate::dialect::SelectExtOf<'a, D>,
    quantifier: SetQuantifier,
    projection: Vec<SelectItem<'a, D::Ext>>,
    into: Option<Box<SelectInto<'a, D::Ext>>>,
    from: Vec<TableWithJoins<'a, D::Ext>>,
    from_first: bool,
) -> PResult<Select<'a, D::Ext>> {
    let lateral_views = D::parse_lateral_views(p)?;

    let prewhere = D::parse_prewhere(p)?;

    let selection = parse_optional_where(p)?;

    let connect_by = D::parse_connect_by(p)?;

    let mut group_by_all = false;
    let group_by = if p.eat_keyword("GROUP") {
        p.expect_keyword("BY")?;
        group_by_all = D::eat_group_by_all(p);
        if group_by_all {
            Vec::new()
        } else {
            p.parse_comma_separated(|p| p.spanned_expr(D::parse_group_by_item))?
        }
    } else {
        Vec::new()
    };
    let group_by_modifier = if group_by_all || !group_by.is_empty() {
        D::parse_group_by_modifier(p)
    } else {
        None
    };
    let grouping_sets = if group_by.is_empty() {
        None
    } else {
        D::parse_group_by_suffix(p)?
    };

    let having = if p.eat_keyword("HAVING") {
        Some(p.parse_expr()?)
    } else {
        None
    };

    let qualify = D::parse_qualify(p)?;

    let windows = if p.eat_keyword("WINDOW") {
        p.parse_comma_separated(parse_named_window)?
    } else {
        Vec::new()
    };

    Ok(Select {
        modifiers,
        quantifier,
        projection,
        into,
        from,
        lateral_views,
        prewhere,
        selection,
        connect_by,
        group_by,
        group_by_all,
        group_by_modifier,
        grouping_sets,
        having,
        qualify,
        windows,
        from_first,
        span: Span::default(),
    })
}

/// Parses `REPLACE (expr AS name, ...)`.
pub fn parse_wildcard_replace<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<Vec<(Expr<'a, D::Ext>, Ident<'a>)>> {
    p.expect_keyword("REPLACE")?;
    p.expect_op("(")?;
    let replace = p.parse_comma_separated(|p| {
        let expr = p.parse_expr()?;
        p.expect_keyword("AS")?;
        Ok((expr, p.parse_ident()?))
    })?;
    p.expect_op(")")?;
    Ok(replace)
}

/// Like [`Parser::parse_comma_separated`], but a comma can end the list.
pub fn parse_list_with_trailing_comma<'a, D: Dialect, T>(
    p: &mut Parser<'a, D>,
    mut item: impl FnMut(&mut Parser<'a, D>) -> PResult<T>,
) -> PResult<Vec<T>> {
    let mut items = vec![item(p)?];
    while p.eat_op(",") {
        if at_list_end(p) {
            break;
        }
        items.push(item(p)?);
    }
    Ok(items)
}

/// True at what can follow a trailing comma: the end of the enclosing list.
pub fn at_list_end<'a, D: Dialect>(p: &Parser<'a, D>) -> bool {
    const CLAUSES: [&str; 14] = [
        "FROM",
        "WHERE",
        "GROUP",
        "HAVING",
        "QUALIFY",
        "WINDOW",
        "ORDER",
        "LIMIT",
        "OFFSET",
        "UNION",
        "EXCEPT",
        "MINUS",
        "INTERSECT",
        "FOR",
    ];
    let token = p.peek();
    // A clause word that names an argument, as in `f(a, offset => 1)`.
    let names_argument = p.peek_nth(1).is_op("=>") || p.peek_nth(1).is_op(":=");
    match token.kind {
        TokenKind::Eof => true,
        TokenKind::Op => token.text == ")" || token.text == ";",
        TokenKind::Word => !names_argument && CLAUSES.iter().any(|word| token.is_word(word)),
        _ => false,
    }
}

/// Parses the options after a `*`, returning `None` if there are none.
pub fn parse_wildcard_options<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<Option<WildcardOptions<'a, D::Ext>>> {
    let mut options = WildcardOptions {
        ilike: None,
        exclude: Vec::new(),
        exclude_parens: false,
        except_keyword: false,
        replace: Vec::new(),
        rename: Vec::new(),
        rename_parens: false,
    };
    let mut found = false;
    if p.eat_keyword("ILIKE") {
        let token = p.expect_kind(TokenKind::String, "a string after ILIKE")?;
        options.ilike = Some(D::string_literal(token));
        found = true;
    } else if p.eat_keyword("EXCLUDE") {
        options.exclude_parens = p.at_op("(");
        options.exclude = if options.exclude_parens {
            parse_ident_list(p)?
        } else {
            vec![p.parse_ident()?]
        };
        found = true;
    }
    if p.at_keyword("REPLACE") {
        options.replace = parse_wildcard_replace(p)?;
        found = true;
    }
    if p.eat_keyword("RENAME") {
        options.rename_parens = p.eat_op("(");
        let rename_pair = |p: &mut Parser<'a, D>| -> PResult<(Ident<'a>, Ident<'a>)> {
            let from = p.parse_ident()?;
            p.expect_keyword("AS")?;
            Ok((from, p.parse_ident()?))
        };
        if options.rename_parens {
            options.rename = p.parse_comma_separated(rename_pair)?;
            p.expect_op(")")?;
        } else {
            options.rename = vec![rename_pair(p)?];
        }
        found = true;
    }
    Ok(found.then_some(options))
}

/// Parses the default sequence after `SELECT`: dialect modifiers, then
/// `ALL` or `DISTINCT`.
pub fn parse_select_head<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<(crate::dialect::SelectExtOf<'a, D>, SetQuantifier)> {
    let modifiers = D::parse_select_modifiers(p)?;
    Ok((modifiers, parse_quantifier(p)))
}

fn parse_named_window<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<NamedWindow<'a, D::Ext>> {
    let name = p.parse_ident()?;
    p.expect_keyword("AS")?;
    if let Some(base) = D::parse_named_window_reference(p)? {
        return Ok(NamedWindow {
            name,
            window: WindowOver::Named(base),
        });
    }
    if !p.at_op("(") {
        return Err(p.expected("a parenthesized window definition"));
    }
    let window = parse_window_over(p)?;
    Ok(NamedWindow { name, window })
}

/// Parses `WITH ROLLUP`, and `WITH CUBE` if `cube` is true.
pub fn parse_group_by_with<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
    cube: bool,
) -> Option<GroupByModifier> {
    if !p.at_keyword("WITH") {
        return None;
    }
    let next = p.peek_nth(1);
    let modifier = if next.is_word("ROLLUP") {
        GroupByModifier::Rollup
    } else if cube && next.is_word("CUBE") {
        GroupByModifier::Cube
    } else {
        return None;
    };
    p.advance();
    p.advance();
    Some(modifier)
}

/// Parses `ROLLUP (...)`, `CUBE (...)` or `GROUPING SETS (...)`, or else an
/// ordinary expression. Dialects with these forms use this as their
/// `parse_group_by_item`.
pub fn parse_grouping_item<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Expr<'a, D::Ext>> {
    let kind = if p.at_keyword("ROLLUP") && p.peek_nth(1).is_op("(") {
        p.advance();
        GroupingKind::Rollup
    } else if p.at_keyword("CUBE") && p.peek_nth(1).is_op("(") {
        p.advance();
        GroupingKind::Cube
    } else if p.at_keyword("GROUPING") && p.peek_nth(1).is_word("SETS") {
        p.advance();
        p.advance();
        GroupingKind::Sets
    } else {
        return p.parse_expr();
    };
    p.expect_op("(")?;
    let sets = p.parse_comma_separated(parse_grouping_set)?;
    p.expect_op(")")?;
    Ok(ExprKind::Special(Box::new(SpecialExpr::Grouping { kind, sets })).into())
}

/// Like [`parse_grouping_item`], where `()` is an empty grouping set.
pub fn parse_grouping_item_or_empty_set<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<Expr<'a, D::Ext>> {
    if p.at_op("(") && p.peek_nth(1).is_op(")") {
        p.advance();
        p.advance();
        return Ok(special(SpecialExpr::Tuple(Vec::new())));
    }
    parse_grouping_item(p)
}

/// Parses `GROUPING SETS (...)` written after a `GROUP BY` list.
pub fn parse_group_by_grouping_sets<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<Option<Box<Expr<'a, D::Ext>>>> {
    if p.at_keyword("GROUPING") && p.peek_nth(1).is_word("SETS") {
        return p
            .spanned_expr(parse_grouping_item)
            .map(|item| Some(Box::new(item)));
    }
    Ok(None)
}

fn parse_grouping_set<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Vec<Expr<'a, D::Ext>>> {
    if !p.eat_op("(") {
        return Ok(vec![p.parse_expr()?]);
    }
    if p.eat_op(")") {
        return Ok(Vec::new());
    }
    let set = p.parse_comma_separated(Parser::parse_expr)?;
    p.expect_op(")")?;
    Ok(set)
}

/// Like [`set_operator_rule`], where `MINUS` means `EXCEPT`.
pub fn set_operator_rule_with_minus<'a, D: Dialect>(p: &Parser<'a, D>) -> Option<SetOperatorRule> {
    if p.at_keyword("MINUS") {
        return Some(SetOperatorRule {
            bp: prec::UNION,
            op: SetOperator::Minus,
        });
    }
    set_operator_rule(p)
}

pub fn set_operator_rule<'a, D: Dialect>(p: &Parser<'a, D>) -> Option<SetOperatorRule> {
    let (bp, op) = if p.at_keyword("UNION") {
        (prec::UNION, SetOperator::Union)
    } else if p.at_keyword("EXCEPT") {
        (prec::UNION, SetOperator::Except)
    } else if p.at_keyword("INTERSECT") {
        (prec::INTERSECT, SetOperator::Intersect)
    } else {
        return None;
    };
    Some(SetOperatorRule { bp, op })
}

/// The Pratt loop for set operators, parsing those binding tighter than
/// `min_bp`.
fn parse_set_expr<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
    min_bp: u8,
) -> PResult<SetExpr<'a, D::Ext>> {
    let mut left = parse_set_operand(p)?;
    while let Some(rule) = D::set_operator_rule(p) {
        if rule.bp <= min_bp {
            break;
        }
        p.deepen()?;
        p.advance();
        let (quantifier, by_name) = D::parse_set_quantifier(p);
        let right = p.nested(|p| parse_set_expr(p, rule.bp))?;
        left = SetExpr::SetOperation {
            left: Box::new(left),
            op: rule.op,
            quantifier,
            by_name,
            right: Box::new(right),
        };
    }
    Ok(left)
}

/// Parses an optional `ALL` or `DISTINCT`.
pub fn parse_quantifier<'a, D: Dialect>(p: &mut Parser<'a, D>) -> SetQuantifier {
    if p.eat_keyword("ALL") {
        SetQuantifier::All
    } else if p.eat_keyword("DISTINCT") {
        SetQuantifier::Distinct
    } else {
        SetQuantifier::None
    }
}

fn parse_set_operand<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<SetExpr<'a, D::Ext>> {
    if p.eat_op("(") {
        let query = parse_subquery_body(p)?;
        Ok(SetExpr::Query(Box::new(query)))
    } else if D::at_query_values(p, 0) {
        p.advance();
        D::parse_query_values(p)
    } else {
        Ok(SetExpr::Select(Box::new(D::parse_select(p)?)))
    }
}

/// True at a parenthesized query that continues with a set operator, which
/// starts a subquery instead of a parenthesized expression.
pub(super) fn at_set_operand_group<'a, D: Dialect>(p: &Parser<'a, D>) -> bool {
    let mut leading = 0;
    while p.peek_nth(leading).is_op("(") {
        leading += 1;
        if leading > p.max_nesting_depth() {
            return false;
        }
    }
    if leading == 0 || !query_starts_at(p, leading) {
        return false;
    }
    for close in closing_parens(p, leading) {
        let Some(close) = close else {
            return false;
        };
        let after = p.peek_nth(close + 1);
        if !after.is_op(")") {
            return ["UNION", "EXCEPT", "INTERSECT", "MINUS"]
                .iter()
                .any(|word| after.is_word(word));
        }
    }
    false
}

/// Parses a query and its closing parenthesis, with the opening one consumed.
pub fn parse_subquery_body<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Query<'a, D::Ext>> {
    let query = p.nested(D::parse_query)?;
    p.expect_op(")")?;
    Ok(query)
}

pub(super) fn at_query_start<'a, D: Dialect>(p: &Parser<'a, D>) -> bool {
    query_starts_at(p, 0)
}

/// True if the token `n` ahead starts a query.
pub fn query_starts_at<'a, D: Dialect>(p: &Parser<'a, D>, n: usize) -> bool {
    let token = p.peek_nth(n);
    token.is_word("SELECT")
        || token.is_word("WITH")
        || D::at_query_values(p, n)
        || D::at_from_first(p, n)
}

/// True at `VALUES (`, since `values` alone can be a column.
pub fn at_query_values<'a, D: Dialect>(p: &Parser<'a, D>, n: usize) -> bool {
    p.peek_nth(n).is_word("VALUES") && p.peek_nth(n + 1).is_op("(")
}

/// Parses the rows of a `VALUES` query, after the keyword.
pub fn parse_query_values<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<SetExpr<'a, D::Ext>> {
    Ok(SetExpr::Values(p.parse_comma_separated(parse_values_row)?))
}

/// With the opening `(` consumed, decides between a parenthesized join and a
/// subquery. When parentheses lead to a query, the group is a table inside a
/// join only if what follows it continues a table reference, as in
/// `((SELECT 1) AS x JOIN y)`.
fn at_nested_join<'a, D: Dialect>(p: &Parser<'a, D>) -> bool {
    let mut leading = 0;
    while p.peek_nth(leading).is_op("(") {
        leading += 1;
        // The nesting limit rejects anything deeper, however it is read.
        if leading > p.max_nesting_depth() {
            return true;
        }
    }
    if !query_starts_at(p, leading) {
        return true;
    }
    if leading == 0 {
        return false;
    }
    for close in closing_parens(p, leading) {
        let Some(close) = close else {
            return false;
        };
        let after = p.peek_nth(close + 1);
        if !after.is_op(")") {
            return ![
                "UNION",
                "EXCEPT",
                "INTERSECT",
                "ORDER",
                "LIMIT",
                "OFFSET",
                "FETCH",
                "FOR",
            ]
            .iter()
            .any(|word| after.is_word(word));
        }
    }
    false
}

/// The offsets of the `)` closing each of the `leading` parentheses at the
/// cursor, outermost first, found in a single pass.
fn closing_parens<'a, D: Dialect>(p: &Parser<'a, D>, leading: usize) -> Vec<Option<usize>> {
    let mut closes = vec![None; leading];
    let mut depth = leading;
    let mut offset = leading;
    while depth > 0 {
        let token = p.peek_nth(offset);
        if token.kind == TokenKind::Eof {
            break;
        }
        if token.is_op("(") {
            depth += 1;
        } else if token.is_op(")") {
            depth -= 1;
            // A later parenthesis can return to this depth; only the first
            // close belongs to the leading one.
            if depth < leading && closes[depth].is_none() {
                closes[depth] = Some(offset);
            }
        }
        offset += 1;
    }
    closes
}

pub fn parse_table_factor<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<TableFactor<'a, D::Ext>> {
    if p.at_keyword("LATERAL") && at_lateral_operand(p) {
        p.advance();
        let factor = p.nested(D::parse_table_factor)?;
        if matches!(factor, TableFactor::NestedJoin(_)) {
            return Err(p.expected("a subquery or function after LATERAL"));
        }
        return Ok(TableFactor::Lateral(Box::new(factor)));
    }
    if p.eat_op("(") {
        if at_nested_join(p) {
            let table = p.nested(D::parse_table_with_joins)?;
            D::check_nested_join(p, &table)?;
            p.expect_op(")")?;
            return Ok(TableFactor::NestedJoin(Box::new(table)));
        }
        // A derived table takes as much stack as a scalar subquery, which
        // also counts as two levels because it passes through an expression.
        let query = p.nested(parse_subquery_body)?;
        return Ok(TableFactor::Derived(Box::new(query)));
    }
    let name = parse_object_name(p)?;
    if p.at_op("(") {
        let call = Box::new(p.parse_function_call(name)?);
        let with_ordinality = p.at_keyword("WITH") && p.peek_nth(1).is_word("ORDINALITY");
        if with_ordinality {
            p.advance();
            p.advance();
        }
        return Ok(TableFactor::Function {
            call,
            with_ordinality,
        });
    }
    Ok(TableFactor::Named(name))
}

/// Parentheses group joins: `(a)` alone is not a table.
pub fn check_nested_join<'a, D: Dialect>(
    p: &Parser<'a, D>,
    table: &TableWithJoins<'a, D::Ext>,
) -> PResult<()> {
    if table.joins.is_empty() && !matches!(table.relation.factor, TableFactor::NestedJoin(_)) {
        return Err(p.expected("a join inside the parentheses"));
    }
    Ok(())
}

/// True if the word after `LATERAL` starts a subquery or a function call, as
/// opposed to `lateral` being the name of a table.
fn at_lateral_operand<'a, D: Dialect>(p: &Parser<'a, D>) -> bool {
    let next = p.peek_nth(1);
    next.is_op("(")
        || (matches!(next.kind, TokenKind::Word | TokenKind::QuotedIdent)
            && (p.peek_nth(2).is_op("(") || p.peek_nth(2).is_op(".")))
}

pub fn parse_select_item<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<SelectItem<'a, D::Ext>> {
    if p.eat_op("*") {
        return Ok(SelectItemKind::Wildcard.into());
    }
    if at_qualified_wildcard(p) {
        return parse_qualified_wildcard(p)
            .map(|name| SelectItemKind::QualifiedWildcard(name).into());
    }
    let expr = p.parse_expr()?;
    D::parse_select_item_alias(p, expr)
}

/// Like [`parse_select_item_alias`], but after `AS` any word can be the alias.
pub fn parse_select_item_alias_any_word<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
    expr: Expr<'a, D::Ext>,
) -> PResult<SelectItem<'a, D::Ext>> {
    if p.eat_keyword("AS") {
        let alias = Some(p.parse_name_part()?);
        return Ok(SelectItemKind::Expr { expr, alias }.into());
    }
    parse_select_item_alias(p, expr)
}

/// Finishes a select item, whose expression has been parsed.
pub fn parse_select_item_alias<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
    expr: Expr<'a, D::Ext>,
) -> PResult<SelectItem<'a, D::Ext>> {
    let alias = parse_optional_alias(p)?;
    Ok(SelectItemKind::Expr { expr, alias }.into())
}

/// Looks ahead for `name.name.*` without consuming anything.
pub(super) fn at_qualified_wildcard<'a, D: Dialect>(p: &Parser<'a, D>) -> bool {
    if !p.at_ident() {
        return false;
    }
    let mut n = 1;
    loop {
        if !p.peek_nth(n).is_op(".") {
            return false;
        }
        let next = p.peek_nth(n + 1);
        if next.is_op("*") {
            return true;
        }
        if !matches!(next.kind, TokenKind::Word | TokenKind::QuotedIdent) {
            return false;
        }
        n += 2;
    }
}

pub(super) fn parse_qualified_wildcard<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<ObjectName<'a>> {
    let mut parts = vec![p.parse_ident()?];
    loop {
        p.expect_op(".")?;
        if p.eat_op("*") {
            return Ok(ObjectName { parts });
        }
        parts.push(p.parse_name_part()?);
    }
}

pub fn parse_table_ref<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<TableRef<'a, D::Ext>> {
    let factor = D::parse_table_factor(p)?;
    let alias = parse_optional_table_alias(p)?;
    Ok(TableRef {
        factor,
        alias,
        span: Span::default(),
    })
}

pub fn parse_object_name<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<ObjectName<'a>> {
    let first = D::parse_object_name_part(p, &[])?;
    let mut parts = Vec::with_capacity(if p.at_op(".") { 2 } else { 1 });
    parts.push(first);
    while p.eat_op(".") {
        let part = D::parse_object_name_part(p, &parts)?;
        parts.push(part);
    }
    Ok(ObjectName { parts })
}

/// Parses one part of a name, given the parts before it.
pub fn parse_object_name_part<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
    before: &[Ident<'a>],
) -> PResult<Ident<'a>> {
    if before.is_empty() {
        p.parse_ident()
    } else {
        p.parse_name_part()
    }
}

pub fn parse_cte<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Cte<'a, D::Ext>> {
    let name = p.parse_ident()?;
    let columns = D::parse_cte_columns(p)?;
    p.expect_keyword("AS")?;
    p.expect_op("(")?;
    let query = p.nested(D::parse_query)?;
    p.expect_op(")")?;
    Ok(Cte {
        name,
        columns,
        materialized: None,
        query,
    })
}

pub fn parse_table_with_joins<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<TableWithJoins<'a, D::Ext>> {
    let relation = p.parse_table_ref()?;
    let mut joins = Vec::new();
    while let Some(join) = D::parse_join(p)? {
        joins.push(join);
    }
    Ok(TableWithJoins { relation, joins })
}

pub fn parse_join<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Option<Join<'a, D::Ext>>> {
    let natural = p.eat_keyword("NATURAL");
    let Some((kind, spelled_out)) = parse_join_kind(p)? else {
        return if natural {
            Err(p.expected("JOIN"))
        } else {
            Ok(None)
        };
    };
    if natural && kind == JoinKind::Cross {
        return Err(p.expected("a join type other than CROSS after NATURAL"));
    }
    let has_constraint = !natural && kind != JoinKind::Cross;
    let table = D::parse_join_table(p, has_constraint)?;
    let constraint = if natural {
        JoinConstraint::Natural
    } else if kind == JoinKind::Cross {
        JoinConstraint::None
    } else {
        parse_join_constraint(p)?
    };
    Ok(Some(Join {
        kind,
        spelled_out,
        table,
        constraint,
    }))
}

/// Consumes the keywords up to and including `JOIN`, if a join starts here.
/// Also returns whether the optional `INNER` or `OUTER` keyword was written.
pub fn parse_join_kind<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<Option<(JoinKind<'a>, bool)>> {
    if p.eat_keyword("JOIN") {
        return Ok(Some((JoinKind::Inner, false)));
    }
    let (kind, spelled_out) = if p.eat_keyword("INNER") {
        (JoinKind::Inner, true)
    } else if p.eat_keyword("CROSS") {
        (JoinKind::Cross, false)
    } else {
        let kind = if p.eat_keyword("LEFT") {
            JoinKind::Left
        } else if p.eat_keyword("RIGHT") {
            JoinKind::Right
        } else if p.eat_keyword("FULL") {
            JoinKind::Full
        } else {
            return Ok(None);
        };
        // `FULL OUTER JOIN` renders as `FULL JOIN`, as in the original.
        let outer = p.eat_keyword("OUTER");
        (kind.clone(), outer && kind != JoinKind::Full)
    };
    p.expect_keyword("JOIN")?;
    Ok(Some((kind, spelled_out)))
}

pub fn parse_join_constraint<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<JoinConstraint<'a, D::Ext>> {
    if p.eat_keyword("ON") {
        return p.parse_expr().map(JoinConstraint::On);
    }
    if p.eat_keyword("USING") {
        return parse_ident_list(p).map(JoinConstraint::Using);
    }
    Ok(JoinConstraint::None)
}

/// Parses a table alias, recording whether it was written with `AS`.
pub fn parse_optional_table_alias<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<Option<TableAlias<'a>>> {
    let explicit = p.eat_keyword("AS");
    if explicit || p.at_implicit_alias() {
        let name = p.parse_ident()?;
        let columns = if p.at_op("(") {
            parse_ident_list(p)?
        } else {
            Vec::new()
        };
        Ok(Some(TableAlias {
            name,
            explicit,
            columns,
            column_types: Vec::new(),
        }))
    } else {
        Ok(None)
    }
}

/// Parses `AS name`, a bare `name`, or nothing.
pub fn parse_optional_alias<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Option<Ident<'a>>> {
    if p.eat_keyword("AS") || p.at_implicit_alias() {
        p.parse_ident().map(Some)
    } else {
        Ok(None)
    }
}

pub fn parse_order_by_item<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<OrderByItem<'a, D::Ext>> {
    let expr = p.parse_expr()?;
    let (asc, nulls_first) = parse_order_by_modifiers(p)?;
    Ok(OrderByItem {
        using: None,
        expr,
        asc,
        nulls_first,
        ext: Default::default(),
    })
}

/// Parses `[ASC | DESC] [NULLS {FIRST | LAST}]`.
pub fn parse_order_by_modifiers<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<(Option<bool>, Option<bool>)> {
    let asc = if p.eat_keyword("ASC") {
        Some(true)
    } else if p.eat_keyword("DESC") {
        Some(false)
    } else {
        None
    };
    let nulls_first = if p.eat_keyword("NULLS") {
        if p.eat_keyword("FIRST") {
            Some(true)
        } else {
            p.expect_keyword("LAST")?;
            Some(false)
        }
    } else {
        None
    };
    Ok((asc, nulls_first))
}

/// Parses what follows the `LIMIT` keyword.
pub fn parse_limit<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Limit<'a, D::Ext>> {
    if D::eat_limit_all(p) {
        return parse_limit_all(p);
    }
    let count = p.parse_expr()?;
    parse_limit_offset(p, count)
}

/// Parses the optional `OFFSET` after `LIMIT ALL`.
fn parse_limit_all<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Limit<'a, D::Ext>> {
    let (offset, offset_rows) = if p.eat_keyword("OFFSET") {
        let offset = p.parse_expr()?;
        (Some(offset), parse_offset_rows(p))
    } else {
        (None, OffsetRows::None)
    };
    Ok(Limit {
        count: None,
        offset,
        offset_rows,
        offset_comma: false,
        all: true,
        by: Vec::new(),
    })
}

/// Parses an optional `OFFSET` after the limit's count.
pub fn parse_limit_offset<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
    count: Expr<'a, D::Ext>,
) -> PResult<Limit<'a, D::Ext>> {
    let (offset, offset_rows) = if p.eat_keyword("OFFSET") {
        let offset = p.parse_expr()?;
        (Some(offset), parse_offset_rows(p))
    } else {
        (None, OffsetRows::None)
    };
    Ok(Limit {
        count: Some(count),
        offset,
        offset_rows,
        offset_comma: false,
        all: false,
        by: Vec::new(),
    })
}

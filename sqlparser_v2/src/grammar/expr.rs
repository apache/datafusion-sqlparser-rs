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

//! Expressions: function calls, windows, prefix and infix rules.

use super::*;
use crate::ast::ExprKind;
use std::borrow::Cow;

/// Parses an optional `FILTER (WHERE condition)` after a call.
pub fn parse_function_filter<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<Option<Expr<'a, D::Ext>>> {
    if !(p.at_keyword("FILTER") && p.peek_nth(1).is_op("(") && p.peek_nth(2).is_word("WHERE")) {
        return Ok(None);
    }
    p.advance();
    p.advance();
    p.advance();
    let condition = p.parse_expr()?;
    p.expect_op(")")?;
    Ok(Some(condition))
}

/// Parses an optional `IGNORE NULLS` or `RESPECT NULLS`.
pub fn parse_null_treatment<'a, D: Dialect>(p: &mut Parser<'a, D>) -> Option<NullTreatment> {
    if !p.peek_nth(1).is_word("NULLS") {
        return None;
    }
    let treatment = if p.eat_keyword("IGNORE") {
        NullTreatment::Ignore
    } else if p.eat_keyword("RESPECT") {
        NullTreatment::Respect
    } else {
        return None;
    };
    p.advance();
    Some(treatment)
}

pub fn parse_function_call<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
    name: ObjectName<'a>,
) -> PResult<Expr<'a, D::Ext>> {
    p.expect_op("(")?;
    let quantifier = parse_quantifier(p);
    let args = D::parse_function_args(p, quantifier)?;
    let on_overflow = D::parse_function_on_overflow(p)?;
    let inner_null_treatment = D::parse_function_arg_null_treatment(p)?;
    let order_by = parse_optional_order_by(p)?;
    let separator = D::parse_function_separator(p)?;
    p.expect_op(")")?;

    let within_group = if p.at_keyword("WITHIN") && p.peek_nth(1).is_word("GROUP") {
        p.advance();
        p.advance();
        p.expect_op("(")?;
        let items = parse_optional_order_by(p)?;
        p.expect_op(")")?;
        items
    } else {
        Vec::new()
    };

    let filter = D::parse_function_filter(p)?;

    let null_treatment = if inner_null_treatment.is_none() {
        parse_null_treatment(p)
    } else {
        None
    };

    let over = D::parse_function_over(p)?;

    let is_plain = quantifier == SetQuantifier::None
        && order_by.is_empty()
        && within_group.is_empty()
        && filter.is_none()
        && null_treatment.is_none()
        && inner_null_treatment.is_none()
        && separator.is_none()
        && on_overflow.is_none()
        && over.is_none();
    let options = (!is_plain).then(|| {
        Box::new(FunctionOptions {
            parameters: Vec::new(),
            quantifier,
            order_by,
            within_group,
            filter,
            null_treatment,
            inner_null_treatment,
            separator,
            on_overflow,
            over,
        })
    });
    Ok(ExprKind::Function {
        name,
        args,
        options,
    }
    .into())
}

/// Parses a call's arguments, after any quantifier. The list can be empty
/// only if there is no quantifier.
pub fn parse_function_args<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
    quantifier: SetQuantifier,
) -> PResult<Vec<FunctionArg<'a, D::Ext>>> {
    if quantifier == SetQuantifier::None && D::at_empty_function_args(p) {
        Ok(Vec::new())
    } else {
        p.parse_comma_separated(D::parse_function_arg)
    }
}

/// Parses one argument of a function call.
pub fn parse_function_arg<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<FunctionArg<'a, D::Ext>> {
    if p.eat_op("*") {
        Ok(FunctionArg::Wildcard)
    } else if at_qualified_wildcard(p) {
        parse_qualified_wildcard(p).map(FunctionArg::QualifiedWildcard)
    } else if matches!(p.peek().kind, TokenKind::Word | TokenKind::QuotedIdent)
        && p.peek_nth(1).is_op("=>")
    {
        let name = p.parse_name_part()?;
        p.advance();
        let value = p.parse_expr()?;
        Ok(FunctionArg::Named { name, value })
    } else {
        p.parse_expr().map(FunctionArg::Expr)
    }
}

/// Parses `ORDER BY ...` if it is here.
pub fn parse_optional_order_by<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<Vec<OrderByItem<'a, D::Ext>>> {
    if p.eat_keyword("ORDER") {
        p.expect_keyword("BY")?;
        p.parse_comma_separated(D::parse_order_by_item)
    } else {
        Ok(Vec::new())
    }
}

/// Parses what follows `OVER`.
pub fn parse_window_over<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<WindowOver<'a, D::Ext>> {
    if !p.eat_op("(") {
        return p.parse_ident().map(WindowOver::Named);
    }
    let starts_spec = ["PARTITION", "ROWS", "RANGE", "GROUPS"]
        .iter()
        .any(|word| p.at_keyword(word));
    let base = if p.at_ident() && !starts_spec {
        Some(p.parse_ident()?)
    } else {
        None
    };
    let partition_by = if p.eat_keyword("PARTITION") {
        p.expect_keyword("BY")?;
        p.parse_comma_separated(Parser::parse_expr)?
    } else {
        Vec::new()
    };
    let order_by = parse_optional_order_by(p)?;
    let frame = parse_window_frame(p)?;
    p.expect_op(")")?;
    Ok(WindowOver::Spec(WindowSpec {
        base,
        partition_by,
        order_by,
        frame,
    }))
}

fn parse_window_frame<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<Option<WindowFrame<'a, D::Ext>>> {
    let units = if p.eat_keyword("ROWS") {
        FrameUnits::Rows
    } else if p.eat_keyword("RANGE") {
        FrameUnits::Range
    } else if p.eat_keyword("GROUPS") {
        FrameUnits::Groups
    } else {
        return Ok(None);
    };
    let (start, end) = if p.eat_keyword("BETWEEN") {
        let start = parse_frame_bound(p)?;
        p.expect_keyword("AND")?;
        (start, Some(parse_frame_bound(p)?))
    } else {
        (parse_frame_bound(p)?, None)
    };
    Ok(Some(WindowFrame { units, start, end }))
}

fn parse_frame_bound<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<FrameBound<'a, D::Ext>> {
    if p.eat_keyword("CURRENT") {
        p.expect_keyword("ROW")?;
        return Ok(FrameBound::CurrentRow);
    }
    if p.eat_keyword("UNBOUNDED") {
        if p.eat_keyword("PRECEDING") {
            return Ok(FrameBound::UnboundedPreceding);
        }
        p.expect_keyword("FOLLOWING")?;
        return Ok(FrameBound::UnboundedFollowing);
    }
    let offset = D::parse_frame_offset(p)?;
    if p.eat_keyword("PRECEDING") {
        return Ok(FrameBound::Preceding(offset));
    }
    p.expect_keyword("FOLLOWING")?;
    Ok(FrameBound::Following(offset))
}

/// True for the types that can introduce a string literal, as in
/// `DATE '2020-01-01'`.
pub fn is_typed_string_type(name: &str) -> bool {
    ["DATE", "TIME", "TIMESTAMP", "TIMESTAMPTZ", "DATETIME"]
        .iter()
        .any(|type_name| name.eq_ignore_ascii_case(type_name))
}

/// True for the standard functions written without parentheses, such as
/// `CURRENT_DATE`.
pub fn is_niladic_function(word: &str) -> bool {
    const NAMES: [&str; 8] = [
        "CURRENT_DATE",
        "CURRENT_TIME",
        "CURRENT_TIMESTAMP",
        "LOCALTIME",
        "LOCALTIMESTAMP",
        "CURRENT_USER",
        "SESSION_USER",
        "CURRENT_ROLE",
    ];
    NAMES.iter().any(|name| word.eq_ignore_ascii_case(name))
}

/// Parses a column, a qualified name, or a function call. A plain column is
/// returned without allocating.
pub fn parse_name_expr<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Expr<'a, D::Ext>> {
    let first = p.parse_ident()?;
    if first.quote.is_none()
        && let Some(expr) = parse_keyword_expr(p, first.clone())?
    {
        return Ok(expr);
    }
    if !p.at_op(".") && !D::at_function_call(p) {
        if first.quote.is_none() && D::is_niladic_function(&first.text) {
            return Ok(special(SpecialExpr::Niladic(first)));
        }
        return Ok(ExprKind::Ident(first).into());
    }

    let mut parts = Vec::with_capacity(if p.at_op(".") { 2 } else { 1 });
    parts.push(first);
    while p.eat_op(".") {
        parts.push(p.parse_name_part()?);
    }
    let name = ObjectName { parts };
    if D::at_function_call(p) {
        if let [only] = name.parts.as_slice()
            && only.quote.is_none()
            && let Some(expr) = parse_special_function(p, &only.text)?
        {
            return Ok(expr);
        }
        p.parse_function_call(name)
    } else {
        Ok(ExprKind::Compound(name).into())
    }
}

pub fn parse_prefix<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Expr<'a, D::Ext>> {
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
        TokenKind::Word if p.eat_keyword("NOT") => parse_unary(p, UnaryOp::Not, prec::NOT),
        TokenKind::Word if p.eat_keyword("NULL") => Ok(ExprKind::Literal(Literal::Null).into()),
        TokenKind::Word if p.eat_keyword("TRUE") => {
            Ok(ExprKind::Literal(Literal::Boolean(true)).into())
        }
        TokenKind::Word if p.eat_keyword("FALSE") => {
            Ok(ExprKind::Literal(Literal::Boolean(false)).into())
        }
        TokenKind::Word if p.eat_keyword("CASE") => parse_case(p),
        TokenKind::HexString => {
            p.advance();
            let digits = match token.text.strip_prefix("0x") {
                Some(digits) => digits,
                None => &token.text[2..token.text.len() - 1],
            };
            Ok(ExprKind::Literal(Literal::HexString(digits.into())).into())
        }
        TokenKind::DollarString => {
            p.advance();
            let body = &token.text[1..];
            let tag_len = body.find('$').unwrap_or(0);
            let tag = &body[..tag_len];
            let text = &body[tag_len + 1..body.len() - tag_len - 2];
            Ok(ExprKind::Literal(Literal::DollarQuoted {
                tag: tag.into(),
                text: text.into(),
            })
            .into())
        }
        TokenKind::Placeholder => {
            p.advance();
            Ok(ExprKind::Placeholder(token.text.into()).into())
        }
        TokenKind::Word if p.at_keyword("CAST") && p.peek_nth(1).text == "(" => {
            p.advance();
            parse_cast_arguments(p, CastStyle::Function)
        }
        TokenKind::Word if p.at_keyword("EXISTS") && p.peek_nth(1).is_op("(") => {
            p.advance();
            p.advance();
            let query = parse_subquery_body(p)?;
            Ok(ExprKind::Exists(Box::new(query)).into())
        }
        TokenKind::Word
            if (p.at_keyword("LEFT") || p.at_keyword("RIGHT")) && p.peek_nth(1).is_op("(") =>
        {
            let name = ObjectName {
                parts: vec![p.parse_name_part()?],
            };
            p.parse_function_call(name)
        }
        TokenKind::Word | TokenKind::QuotedIdent => parse_name_expr(p),
        TokenKind::Op if p.eat_op("-") => parse_unary(p, UnaryOp::Neg, prec::UNARY),
        TokenKind::Op if p.eat_op("+") => parse_unary(p, UnaryOp::Plus, prec::UNARY),
        TokenKind::Op if p.eat_op("(") => {
            if at_query_start(p) || at_set_operand_group(p) {
                let query = parse_subquery_body(p)?;
                return Ok(ExprKind::Subquery(Box::new(query)).into());
            }
            let expr = p.parse_expr()?;
            if p.at_op(",") {
                let mut items = vec![expr];
                while p.eat_op(",") {
                    items.push(p.parse_expr()?);
                }
                p.expect_op(")")?;
                return Ok(special(SpecialExpr::Tuple(items)));
            }
            p.expect_op(")")?;
            Ok(ExprKind::Nested(Box::new(expr)).into())
        }
        TokenKind::Op | TokenKind::Eof | TokenKind::DialectLiteral => Err(p.expected("expression")),
    }
}

/// Parses `(expr AS ty)`.
fn parse_cast_arguments<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
    style: CastStyle,
) -> PResult<Expr<'a, D::Ext>> {
    p.expect_op("(")?;
    let expr = p.parse_expr()?;
    p.expect_keyword("AS")?;
    let ty = Box::new(D::parse_data_type(p)?);
    p.expect_op(")")?;
    Ok(ExprKind::Cast {
        expr: Box::new(expr),
        ty,
        style,
    }
    .into())
}

/// True if a signed number or a parenthesized expression at the cursor is
/// followed by an interval unit. Without the unit, `interval - 1` is a column.
fn unit_follows_interval_value<D: Dialect>(p: &Parser<'_, D>) -> bool {
    let is_unit = |token: Token<'_>| token.kind == TokenKind::Word && is_interval_unit(token.text);
    if !p.at_op("(") {
        return p.peek_nth(1).kind == TokenKind::Number && is_unit(p.peek_nth(2));
    }
    let mut depth = 0usize;
    let mut n = 0;
    loop {
        let token = p.peek_nth(n);
        n += 1;
        match token.kind {
            TokenKind::Eof => return false,
            TokenKind::Op if token.text == "(" => depth += 1,
            TokenKind::Op if token.text == ")" => {
                depth -= 1;
                if depth == 0 {
                    return is_unit(p.peek_nth(n));
                }
            }
            _ => {}
        }
    }
}

/// True at `WITH TIME ZONE 'text'` or `WITHOUT TIME ZONE 'text'` after the
/// unquoted word `first`, if that is `TIMESTAMP` or `TIME`.
fn at_time_zone_literal<'a, D: Dialect>(p: &Parser<'a, D>, first: &Ident<'a>) -> bool {
    let next = p.peek();
    (next.is_word("WITH") || next.is_word("WITHOUT"))
        && p.peek_nth(1).is_word("TIME")
        && p.peek_nth(2).is_word("ZONE")
        && p.peek_nth(3).kind == TokenKind::String
        && ["TIMESTAMP", "TIME"]
            .iter()
            .any(|name| first.text.eq_ignore_ascii_case(name))
}

/// Parses what a keyword introduces after the unquoted word `first` was read:
/// an interval, a typed literal or an array. Only runs its comparisons when
/// the next token could continue such an expression, so that plain columns
/// do not pay for them.
fn parse_keyword_expr<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
    first: Ident<'a>,
) -> PResult<Option<Expr<'a, D::Ext>>> {
    let next = p.peek();
    match next.kind {
        TokenKind::String | TokenKind::Number | TokenKind::Placeholder => {
            if first.text.eq_ignore_ascii_case("INTERVAL") {
                return D::parse_interval(p).map(Some);
            }
            let is_type = next.kind == TokenKind::String && D::is_typed_string_type(&first.text);
            if !is_type {
                return Ok(None);
            }
            p.advance();
            Ok(Some(
                ExprKind::TypedString(Box::new(TypedString {
                    ty: DataType {
                        qualifier: Vec::new(),
                        angle: None,
                        name: first,
                        args: Vec::new(),
                        suffix: DataTypeSuffix::None,
                        array: ArrayType::None,
                    },
                    value: D::string_literal(next),
                }))
                .into(),
            ))
        }
        TokenKind::Word if at_time_zone_literal(p, &first) => {
            let suffix = if p.advance().is_word("WITH") {
                DataTypeSuffix::WithTimeZone
            } else {
                DataTypeSuffix::WithoutTimeZone
            };
            p.advance();
            p.advance();
            let value = D::string_literal(p.advance());
            Ok(Some(
                ExprKind::TypedString(Box::new(TypedString {
                    ty: DataType {
                        qualifier: Vec::new(),
                        angle: None,
                        name: first,
                        args: Vec::new(),
                        suffix,
                        array: ArrayType::None,
                    },
                    value,
                }))
                .into(),
            ))
        }
        TokenKind::Op
            if matches!(next.text, "+" | "-" | "(")
                && first.text.eq_ignore_ascii_case("INTERVAL")
                && unit_follows_interval_value(p) =>
        {
            D::parse_interval(p).map(Some)
        }
        TokenKind::Op if next.text == "[" && first.text.eq_ignore_ascii_case("ARRAY") => {
            p.advance();
            let items = if p.at_op("]") {
                Vec::new()
            } else {
                p.parse_comma_separated(Parser::parse_expr)?
            };
            p.expect_op("]")?;
            Ok(Some(special(SpecialExpr::Array(items))))
        }
        _ => Ok(None),
    }
}

/// True if `word` names a unit an interval can be measured in.
fn is_interval_unit(word: &str) -> bool {
    [
        "YEAR",
        "YEARS",
        "MONTH",
        "MONTHS",
        "WEEK",
        "WEEKS",
        "DAY",
        "DAYS",
        "HOUR",
        "HOURS",
        "MINUTE",
        "MINUTES",
        "SECOND",
        "SECONDS",
        "CENTURY",
        "DECADE",
        "DOW",
        "DOY",
        "EPOCH",
        "ISODOW",
        "ISOYEAR",
        "JULIAN",
        "MICROSECOND",
        "MICROSECONDS",
        "MILLENIUM",
        "MILLENNIUM",
        "MILLISECOND",
        "MILLISECONDS",
        "NANOSECOND",
        "NANOSECONDS",
        "QUARTER",
        "TIMEZONE",
        "TIMEZONE_HOUR",
        "TIMEZONE_MINUTE",
    ]
    .iter()
    .any(|unit| unit.eq_ignore_ascii_case(word))
}

/// True if `word` names a part that can be taken from a date or time.
pub fn is_date_time_field(word: &str) -> bool {
    is_interval_unit(word)
        || [
            "DATE",
            "DATETIME",
            "DAYOFWEEK",
            "DAYOFYEAR",
            "ISOWEEK",
            "TIME",
            "TIMEZONE_ABBR",
            "TIMEZONE_REGION",
        ]
        .iter()
        .any(|field| field.eq_ignore_ascii_case(word))
}

pub fn parse_date_time_field<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<&'a str> {
    let token = p.peek();
    if token.kind != TokenKind::Word || !is_date_time_field(token.text) {
        return Err(p.expected("a date or time field"));
    }
    p.advance();
    Ok(token.text)
}

/// Parses `(precision)` if present.
pub fn parse_optional_precision<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Option<&'a str>> {
    if !p.eat_op("(") {
        return Ok(None);
    }
    let precision = parse_number_text(p)?;
    p.expect_op(")")?;
    Ok(Some(precision))
}

/// Parses what follows `INTERVAL`. If `require_unit`, the value is a whole
/// expression that must be followed by a unit. Otherwise it is a single term
/// with an optional unit, so that `INTERVAL '5 days' > x` compares the interval.
pub fn parse_interval<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
    require_unit: bool,
) -> PResult<Expr<'a, D::Ext>> {
    parse_interval_with(p, require_unit, is_interval_unit)
}

/// Like [`parse_interval`], with the words that count as interval units given
/// by `is_unit`.
pub fn parse_interval_with<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
    require_unit: bool,
    is_unit: fn(&str) -> bool,
) -> PResult<Expr<'a, D::Ext>> {
    let value = if require_unit {
        p.parse_expr()?
    } else {
        p.nested(Parser::parse_prefix)?
    };

    let token = p.peek();
    let leading_field = if token.kind == TokenKind::Word && is_unit(token.text) {
        p.advance();
        Some(token.text)
    } else if require_unit {
        return Err(p.expected("an interval unit"));
    } else {
        None
    };

    let mut leading_precision = None;
    let mut last_field = None;
    let mut fractional_seconds_precision = None;
    if leading_field.is_some_and(|field| field.eq_ignore_ascii_case("SECOND")) {
        // `SECOND (leading, fractional)` replaces `SECOND TO SECOND`.
        if p.eat_op("(") {
            leading_precision = Some(parse_number_text(p)?);
            if p.eat_op(",") {
                fractional_seconds_precision = Some(parse_number_text(p)?);
            }
            p.expect_op(")")?;
        }
    } else if leading_field.is_some() {
        leading_precision = parse_optional_precision(p)?;
        if p.eat_keyword("TO") {
            let last = parse_date_time_field(p)?;
            if last.eq_ignore_ascii_case("SECOND") {
                fractional_seconds_precision = parse_optional_precision(p)?;
            }
            last_field = Some(last);
        }
    }

    Ok(ExprKind::Interval(Box::new(Interval {
        value,
        leading_field: leading_field.map(Cow::Borrowed),
        leading_precision: leading_precision.map(Cow::Borrowed),
        last_field: last_field.map(Cow::Borrowed),
        fractional_seconds_precision: fractional_seconds_precision.map(Cow::Borrowed),
    }))
    .into())
}

/// True if a word in `words` appears directly inside the `(` at the cursor.
fn call_has_keyword<'a, D: Dialect>(p: &Parser<'a, D>, words: &[&str]) -> bool {
    let mut depth = 0usize;
    let mut n = 0;
    loop {
        let token = p.peek_nth(n);
        match token.kind {
            TokenKind::Eof => return false,
            TokenKind::Op if token.text == "(" => depth += 1,
            TokenKind::Op if token.text == ")" => {
                depth -= 1;
                if depth == 0 {
                    return false;
                }
            }
            TokenKind::Word if depth == 1 && words.iter().any(|w| token.is_word(w)) => {
                return true;
            }
            _ => {}
        }
        n += 1;
    }
}

/// Parses a call that has its own syntax, with the cursor on the `(`. Returns
/// `None`, consuming nothing, if the call is an ordinary one: these are plain
/// functions unless their keywords are present.
fn parse_special_function<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
    name: &str,
) -> PResult<Option<Expr<'a, D::Ext>>> {
    let eq = |word: &str| name.eq_ignore_ascii_case(word);
    let special = if eq("EXTRACT") && call_has_keyword(p, &["FROM"]) {
        p.advance();
        let field = D::parse_extract_field(p)?;
        p.expect_keyword("FROM")?;
        let expr = p.parse_expr()?;
        SpecialExpr::Extract {
            field: field.into(),
            expr,
        }
    } else if (eq("CEIL") || eq("FLOOR")) && call_has_keyword(p, &["TO"]) {
        p.advance();
        let expr = p.parse_expr()?;
        p.expect_keyword("TO")?;
        let field = parse_date_time_field(p)?;
        SpecialExpr::Round {
            ceil: eq("CEIL"),
            expr,
            field: field.into(),
        }
    } else if eq("POSITION") && call_has_keyword(p, &["IN"]) {
        p.advance();
        let needle = p.parse_expr_bp(prec::COMPARE)?;
        p.expect_keyword("IN")?;
        let haystack = p.parse_expr()?;
        SpecialExpr::Position { needle, haystack }
    } else if (eq("SUBSTRING") || eq("SUBSTR")) && call_has_keyword(p, &["FROM", "FOR"]) {
        p.advance();
        let expr = p.parse_expr()?;
        let from = if p.eat_keyword("FROM") {
            Some(p.parse_expr()?)
        } else {
            None
        };
        let length = if p.eat_keyword("FOR") {
            Some(p.parse_expr()?)
        } else {
            None
        };
        SpecialExpr::Substring {
            shorthand: eq("SUBSTR"),
            expr,
            from,
            length,
        }
    } else if eq("TRIM") && at_special_trim(p) {
        p.advance();
        parse_trim_arguments(p)?
    } else if eq("OVERLAY") && call_has_keyword(p, &["PLACING"]) {
        p.advance();
        let expr = p.parse_expr()?;
        p.expect_keyword("PLACING")?;
        let what = p.parse_expr()?;
        p.expect_keyword("FROM")?;
        let from = p.parse_expr()?;
        let length = if p.eat_keyword("FOR") {
            Some(p.parse_expr()?)
        } else {
            None
        };
        SpecialExpr::Overlay {
            expr,
            what,
            from,
            length,
        }
    } else if eq("TRY_CAST") {
        return parse_cast_arguments(p, CastStyle::TryFunction).map(Some);
    } else {
        return Ok(None);
    };
    p.expect_op(")")?;
    Ok(Some(ExprKind::Special(Box::new(special)).into()))
}

fn at_special_trim<'a, D: Dialect>(p: &Parser<'a, D>) -> bool {
    let first = p.peek_nth(1);
    ["BOTH", "LEADING", "TRAILING"]
        .iter()
        .any(|side| first.is_word(side))
        || call_has_keyword(p, &["FROM"])
}

fn parse_trim_arguments<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<SpecialExpr<'a, D::Ext>> {
    let side = if p.eat_keyword("BOTH") {
        Some(TrimSide::Both)
    } else if p.eat_keyword("LEADING") {
        Some(TrimSide::Leading)
    } else if p.eat_keyword("TRAILING") {
        Some(TrimSide::Trailing)
    } else {
        None
    };
    let first = p.parse_expr()?;
    if p.eat_keyword("FROM") {
        let expr = p.parse_expr()?;
        Ok(SpecialExpr::Trim {
            side,
            what: Some(first),
            expr,
        })
    } else {
        Ok(SpecialExpr::Trim {
            side,
            what: None,
            expr: first,
        })
    }
}

/// Levels of tree height added for a [`SpecialExpr`] that wraps the expression
/// before it, on top of those the Pratt loop counts for a custom operator. Going
/// through the box and the larger enum takes about four times the stack of an
/// ordinary level when cloning or rendering.
const SPECIAL_LEVELS: usize = 2;

/// Parses the right side of a binary operator, which has been consumed. A
/// comparison can be quantified: `a = ANY (b)`.
pub fn parse_binary_right<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
    left: Expr<'a, D::Ext>,
    op: BinaryOp<'a>,
    bp: u8,
) -> PResult<Expr<'a, D::Ext>> {
    if bp == prec::COMPARE && p.peek_nth(1).is_op("(") {
        let token = p.peek();
        let quantifier = if token.is_word("ANY") {
            Some(Quantifier::Any)
        } else if token.is_word("SOME") {
            Some(Quantifier::Some)
        } else if token.is_word("ALL") {
            Some(Quantifier::All)
        } else {
            None
        };
        if let Some(quantifier) = quantifier {
            p.advance();
            let open_paren = p.advance().span.start;
            let right = if at_query_start(p) {
                let query = Box::new(parse_subquery_body(p)?);
                Expr::new(ExprKind::Subquery(query), p.span_since(open_paren))
            } else {
                let right = p.parse_expr()?;
                p.expect_op(")")?;
                right
            };
            p.deepen_by(SPECIAL_LEVELS + 1)?;
            return Ok(special(SpecialExpr::Quantified {
                left,
                op,
                quantifier,
                right,
            }));
        }
    }
    let right = p.parse_expr_bp(bp)?;
    Ok(ExprKind::Binary {
        left: Box::new(left),
        op,
        right: Box::new(right),
    }
    .into())
}

fn parse_unary<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
    op: UnaryOp,
    bp: u8,
) -> PResult<Expr<'a, D::Ext>> {
    let expr = p.parse_expr_bp(bp)?;
    Ok(ExprKind::Unary {
        op,
        expr: Box::new(expr),
    }
    .into())
}

fn parse_in<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
    left: Expr<'a, D::Ext>,
    _bp: u8,
) -> PResult<Expr<'a, D::Ext>> {
    p.advance();
    parse_in_operand(p, left, false)
}

fn parse_not_in<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
    left: Expr<'a, D::Ext>,
    _bp: u8,
) -> PResult<Expr<'a, D::Ext>> {
    p.advance();
    p.advance();
    parse_in_operand(p, left, true)
}

/// Parses the parenthesized list or subquery after `IN`.
fn parse_in_operand<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
    left: Expr<'a, D::Ext>,
    negated: bool,
) -> PResult<Expr<'a, D::Ext>> {
    p.expect_op("(")?;
    let expr = Box::new(left);
    if at_query_start(p) || at_set_operand_group(p) {
        let subquery = Box::new(parse_subquery_body(p)?);
        return Ok(ExprKind::InSubquery {
            expr,
            subquery,
            negated,
        }
        .into());
    }
    let list = p.parse_comma_separated(Parser::parse_expr)?;
    p.expect_op(")")?;
    Ok(ExprKind::InList {
        expr,
        list,
        negated,
    }
    .into())
}

pub fn parse_is<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
    left: Expr<'a, D::Ext>,
    _bp: u8,
) -> PResult<Expr<'a, D::Ext>> {
    p.advance();
    let negated = p.eat_keyword("NOT");
    let test = if p.eat_keyword("NULL") {
        IsTest::Null
    } else if p.eat_keyword("TRUE") {
        IsTest::True
    } else if p.eat_keyword("FALSE") {
        IsTest::False
    } else if p.eat_keyword("UNKNOWN") {
        IsTest::Unknown
    } else if p.eat_keyword("DISTINCT") {
        p.expect_keyword("FROM")?;
        IsTest::DistinctFrom(Box::new(p.parse_expr_bp(prec::IS)?))
    } else if let Some(test) = D::parse_is_test(p)? {
        test
    } else {
        return Err(p.expected("NULL, TRUE, FALSE, UNKNOWN or DISTINCT FROM"));
    };
    Ok(ExprKind::Is {
        expr: Box::new(left),
        test,
        negated,
    }
    .into())
}

fn parse_like<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
    left: Expr<'a, D::Ext>,
    _bp: u8,
) -> PResult<Expr<'a, D::Ext>> {
    p.advance();
    parse_like_operand(p, left, false, LikeKind::Like)
}

fn parse_not_like<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
    left: Expr<'a, D::Ext>,
    _bp: u8,
) -> PResult<Expr<'a, D::Ext>> {
    p.advance();
    p.advance();
    parse_like_operand(p, left, true, LikeKind::Like)
}

fn parse_similar<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
    left: Expr<'a, D::Ext>,
    _bp: u8,
) -> PResult<Expr<'a, D::Ext>> {
    p.advance();
    p.advance();
    parse_like_operand(p, left, false, LikeKind::SimilarTo)
}

fn parse_not_similar<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
    left: Expr<'a, D::Ext>,
    _bp: u8,
) -> PResult<Expr<'a, D::Ext>> {
    p.advance();
    p.advance();
    p.advance();
    parse_like_operand(p, left, true, LikeKind::SimilarTo)
}

pub(super) fn special<'a, X: crate::dialect::AstExt>(expr: SpecialExpr<'a, X>) -> Expr<'a, X> {
    ExprKind::Special(Box::new(expr)).into()
}

fn parse_field_access<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
    left: Expr<'a, D::Ext>,
    _bp: u8,
) -> PResult<Expr<'a, D::Ext>> {
    let composite = match &left.kind {
        ExprKind::Nested(_) | ExprKind::Function { .. } => true,
        ExprKind::Special(special) => matches!(
            **special,
            SpecialExpr::Subscript { .. } | SpecialExpr::FieldAccess { .. }
        ),
        _ => false,
    };
    if !composite {
        return Err(p.expected("a composite value before the field name"));
    }
    p.advance();
    let field = p.parse_name_part()?;
    p.deepen_by(SPECIAL_LEVELS)?;
    Ok(special(SpecialExpr::FieldAccess { expr: left, field }))
}

fn parse_at_time_zone<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
    left: Expr<'a, D::Ext>,
    bp: u8,
) -> PResult<Expr<'a, D::Ext>> {
    p.advance();
    p.advance();
    p.advance();
    let zone = p.parse_expr_bp(bp)?;
    p.deepen_by(SPECIAL_LEVELS)?;
    Ok(special(SpecialExpr::AtTimeZone { expr: left, zone }))
}

fn parse_collate<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
    left: Expr<'a, D::Ext>,
    _bp: u8,
) -> PResult<Expr<'a, D::Ext>> {
    p.advance();
    let collation = parse_object_name(p)?;
    p.deepen_by(SPECIAL_LEVELS)?;
    Ok(special(SpecialExpr::Collate {
        expr: left,
        collation,
    }))
}

fn parse_subscript<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
    left: Expr<'a, D::Ext>,
    _bp: u8,
) -> PResult<Expr<'a, D::Ext>> {
    p.advance();
    let index = p.parse_expr()?;
    p.expect_op("]")?;
    p.deepen_by(SPECIAL_LEVELS)?;
    Ok(special(SpecialExpr::Subscript { expr: left, index }))
}

/// Parses a bound of a slice, which ends at `:` or `]`.
/// Set while parsing an expression that a `:` ends, such as a slice bound, so
/// that a dialect whose `:` is also an operator leaves it alone.
pub const CONTEXT_COLON_SEPARATES: u32 = 1 << 30;

fn parse_slice_bound<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Option<Expr<'a, D::Ext>>> {
    if p.at_op(":") || p.at_op("]") {
        Ok(None)
    } else {
        p.with_context(CONTEXT_COLON_SEPARATES, Parser::parse_expr)
            .map(Some)
    }
}

/// Parses `[index]`, or `[lower:upper]`, or `[lower:upper:step]` if `step`.
fn parse_subscript_slice<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
    left: Expr<'a, D::Ext>,
    allow_step: bool,
) -> PResult<Expr<'a, D::Ext>> {
    p.advance();
    let lower = parse_slice_bound(p)?;
    if !p.eat_op(":") {
        let Some(index) = lower else {
            return Err(p.expected("an index"));
        };
        p.expect_op("]")?;
        p.deepen_by(SPECIAL_LEVELS)?;
        return Ok(special(SpecialExpr::Subscript { expr: left, index }));
    }
    let upper = parse_slice_bound(p)?;
    let step = if allow_step && p.eat_op(":") {
        parse_slice_bound(p)?
    } else {
        None
    };
    p.expect_op("]")?;
    p.deepen_by(SPECIAL_LEVELS)?;
    Ok(special(SpecialExpr::Slice {
        expr: left,
        lower,
        upper,
        step,
    }))
}

/// An infix rule for `[`, reading a subscript or a slice without a step.
pub fn parse_subscript_or_slice<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
    left: Expr<'a, D::Ext>,
    _bp: u8,
) -> PResult<Expr<'a, D::Ext>> {
    parse_subscript_slice(p, left, false)
}

/// An infix rule for `[`, reading a subscript or a slice with an optional step.
pub fn parse_subscript_or_slice_with_step<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
    left: Expr<'a, D::Ext>,
    _bp: u8,
) -> PResult<Expr<'a, D::Ext>> {
    parse_subscript_slice(p, left, true)
}

/// Parses `IS JSON` after `IS` and any `NOT`, or returns `None` at something else.
pub fn parse_is_json<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Option<IsTest<'a, D::Ext>>> {
    if !p.eat_keyword("JSON") {
        return Ok(None);
    }
    let kind = if p.eat_keyword("VALUE") {
        Some(JsonPredicateType::Value)
    } else if p.eat_keyword("ARRAY") {
        Some(JsonPredicateType::Array)
    } else if p.eat_keyword("OBJECT") {
        Some(JsonPredicateType::Object)
    } else if p.eat_keyword("SCALAR") {
        Some(JsonPredicateType::Scalar)
    } else {
        None
    };
    let unique_keys = if p.eat_keyword("WITH") {
        Some(true)
    } else if p.eat_keyword("WITHOUT") {
        Some(false)
    } else {
        None
    };
    let unique_keys = match unique_keys {
        Some(with) => {
            p.expect_keyword("UNIQUE")?;
            let keys = p.eat_keyword("KEYS");
            Some(JsonUniqueKeys { with, keys })
        }
        None => None,
    };
    Ok(Some(IsTest::Json { kind, unique_keys }))
}

/// Parses `IS NORMALIZED`, with an optional form, after `IS` and any `NOT`.
pub fn parse_is_normalized<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<Option<IsTest<'a, D::Ext>>> {
    if p.eat_keyword("NORMALIZED") {
        return Ok(Some(IsTest::Normalized(None)));
    }
    let form = if p.at_keyword("NFC") {
        NormalForm::Nfc
    } else if p.at_keyword("NFD") {
        NormalForm::Nfd
    } else if p.at_keyword("NFKC") {
        NormalForm::Nfkc
    } else if p.at_keyword("NFKD") {
        NormalForm::Nfkd
    } else {
        return Ok(None);
    };
    p.advance();
    p.expect_keyword("NORMALIZED")?;
    Ok(Some(IsTest::Normalized(Some(form))))
}

/// Parses `ON OVERFLOW ...` inside a `LISTAGG`-style call.
pub fn parse_on_overflow<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<Option<Box<ListAggOnOverflow<'a, D::Ext>>>> {
    if !(p.at_keyword("ON") && p.peek_nth(1).is_word("OVERFLOW")) {
        return Ok(None);
    }
    p.advance();
    p.advance();
    if p.eat_keyword("ERROR") {
        return Ok(Some(Box::new(ListAggOnOverflow::Error)));
    }
    p.expect_keyword("TRUNCATE")?;
    let filler = if p.at_keyword("WITH") || p.at_keyword("WITHOUT") {
        None
    } else {
        Some(p.parse_expr()?)
    };
    let with_count = if p.eat_keyword("WITH") {
        true
    } else {
        p.expect_keyword("WITHOUT")?;
        false
    };
    p.expect_keyword("COUNT")?;
    Ok(Some(Box::new(ListAggOnOverflow::Truncate {
        filler,
        with_count,
    })))
}

/// Consumes `ANY`, `SOME` or `ALL` directly before a `(`, for a `LIKE` over a
/// list of patterns.
pub fn parse_like_any<'a, D: Dialect>(p: &mut Parser<'a, D>) -> Option<Quantifier> {
    if !p.peek_nth(1).is_op("(") {
        return None;
    }
    let quantifier = if p.at_keyword("ANY") {
        Quantifier::Any
    } else if p.at_keyword("SOME") {
        Quantifier::Some
    } else if p.at_keyword("ALL") {
        Quantifier::All
    } else {
        return None;
    };
    p.advance();
    Some(quantifier)
}

/// True at an ODBC escape sequence: `{d 'x'}`, `{t 'x'}`, `{ts 'x'}` or `{fn f()}`.
pub fn at_odbc_escape<'a, D: Dialect>(p: &Parser<'a, D>) -> bool {
    let next = p.peek_nth(1);
    p.at_op("{") && ["d", "t", "ts", "fn"].iter().any(|kind| next.is_word(kind))
}

/// Parses an ODBC escape sequence, with the cursor on its `{`.
pub fn parse_odbc_escape<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Expr<'a, D::Ext>> {
    p.expect_op("{")?;
    let token = p.advance();
    let expr = p.parse_expr()?;
    p.expect_op("}")?;
    Ok(special(SpecialExpr::OdbcEscape {
        kind: token.text.into(),
        expr,
    }))
}

/// Parses the pattern and optional escape after a `LIKE`-style keyword.
pub fn parse_like_operand<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
    left: Expr<'a, D::Ext>,
    negated: bool,
    kind: LikeKind<'a>,
) -> PResult<Expr<'a, D::Ext>> {
    let start = p.peek().span.start;
    let pattern = match D::parse_like_quantifier(p)? {
        Some(quantifier) => {
            p.expect_op("(")?;
            let items = p.parse_comma_separated(Parser::parse_expr)?;
            p.expect_op(")")?;
            p.deepen_by(SPECIAL_LEVELS)?;
            let mut list = special(SpecialExpr::QuantifiedList { quantifier, items });
            list.span = p.span_since(start);
            Box::new(list)
        }
        None => Box::new(p.parse_expr_bp(prec::COMPARE)?),
    };
    let escape = if p.eat_keyword("ESCAPE") {
        Some(Box::new(p.parse_expr_bp(prec::COMPARE)?))
    } else {
        None
    };
    Ok(ExprKind::Like {
        expr: Box::new(left),
        pattern,
        negated,
        kind,
        escape,
    }
    .into())
}

fn parse_between<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
    left: Expr<'a, D::Ext>,
    _bp: u8,
) -> PResult<Expr<'a, D::Ext>> {
    p.advance();
    parse_between_operands(p, left, false)
}

/// Parses the lower bound of `BETWEEN`.
pub fn parse_between_low<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Expr<'a, D::Ext>> {
    p.parse_expr_bp(prec::COMPARE)
}

fn parse_not_between<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
    left: Expr<'a, D::Ext>,
    _bp: u8,
) -> PResult<Expr<'a, D::Ext>> {
    p.advance();
    p.advance();
    parse_between_operands(p, left, true)
}

/// Parses the upper bound of `BETWEEN`.
pub fn parse_between_high<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Expr<'a, D::Ext>> {
    p.parse_expr_bp(prec::COMPARE)
}

/// The bounds bind tighter than `AND`, which separates them here.
fn parse_between_operands<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
    left: Expr<'a, D::Ext>,
    negated: bool,
) -> PResult<Expr<'a, D::Ext>> {
    let low = Box::new(D::parse_between_low(p)?);
    p.expect_keyword("AND")?;
    let high = Box::new(D::parse_between_high(p)?);
    Ok(ExprKind::Between {
        expr: Box::new(left),
        low,
        high,
        negated,
    }
    .into())
}

/// The rule for `NOT` in infix position, which only negates certain operators.
fn negated_infix_rule<'a, D: Dialect>(p: &Parser<'a, D>) -> Option<InfixRule<'a, D>> {
    let next = p.peek_nth(1);
    let parse: InfixParseFn<'a, D> = if next.is_word("IN") {
        parse_not_in
    } else if next.is_word("LIKE") {
        parse_not_like
    } else if next.is_word("SIMILAR") && p.peek_nth(2).is_word("TO") {
        parse_not_similar
    } else if next.is_word("BETWEEN") {
        parse_not_between
    } else {
        return None;
    };
    Some(InfixRule::custom(prec::COMPARE, parse))
}

/// Parses `CASE ... END` with `CASE` already consumed.
fn parse_case<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Expr<'a, D::Ext>> {
    let operand = if p.at_keyword("WHEN") {
        None
    } else {
        Some(p.parse_expr()?)
    };
    let mut branches = Vec::new();
    while p.eat_keyword("WHEN") {
        let condition = p.parse_expr()?;
        p.expect_keyword("THEN")?;
        let result = p.parse_expr()?;
        branches.push(CaseWhen { condition, result });
    }
    if branches.is_empty() {
        return Err(p.expected("WHEN"));
    }
    let else_result = if p.eat_keyword("ELSE") {
        Some(p.parse_expr()?)
    } else {
        None
    };
    p.expect_keyword("END")?;
    Ok(ExprKind::Case(Box::new(CaseExpr {
        operand,
        branches,
        else_result,
    }))
    .into())
}

pub fn infix_rule<'a, D: Dialect>(p: &Parser<'a, D>) -> Option<InfixRule<'a, D>> {
    let token = p.peek();
    let (bp, op) = match token.kind {
        TokenKind::Op => match token.text {
            "=" => (prec::COMPARE, BinaryOp::Eq),
            "<>" | "!=" => (prec::COMPARE, BinaryOp::NotEq),
            "<" => (prec::COMPARE, BinaryOp::Lt),
            "<=" => (prec::COMPARE, BinaryOp::LtEq),
            ">" => (prec::COMPARE, BinaryOp::Gt),
            ">=" => (prec::COMPARE, BinaryOp::GtEq),
            "+" => (prec::ADD, BinaryOp::Add),
            "-" => (prec::ADD, BinaryOp::Sub),
            "*" => (prec::MUL, BinaryOp::Mul),
            "/" => (prec::MUL, BinaryOp::Div),
            "%" => (prec::MUL, BinaryOp::Mod),
            "||" => (prec::MUL, BinaryOp::StringConcat),
            "|" => (prec::BIT_OR, BinaryOp::BitOr),
            "&" => (prec::BIT_AND, BinaryOp::BitAnd),
            "^" => (prec::BIT_XOR, BinaryOp::BitXor),
            "[" => return Some(InfixRule::custom(prec::POSTFIX, parse_subscript)),
            "." => return Some(InfixRule::custom(prec::POSTFIX, parse_field_access)),
            _ => return None,
        },
        TokenKind::Word if token.text.eq_ignore_ascii_case("IN") => {
            return Some(InfixRule::custom(prec::COMPARE, parse_in));
        }
        TokenKind::Word if token.text.eq_ignore_ascii_case("NOT") => {
            return negated_infix_rule(p);
        }
        TokenKind::Word if token.text.eq_ignore_ascii_case("IS") => {
            return Some(InfixRule::custom(prec::IS, parse_is));
        }
        TokenKind::Word if token.text.eq_ignore_ascii_case("LIKE") => {
            return Some(InfixRule::custom(prec::COMPARE, parse_like));
        }
        TokenKind::Word if token.text.eq_ignore_ascii_case("BETWEEN") => {
            return Some(InfixRule::custom(prec::COMPARE, parse_between));
        }
        TokenKind::Word
            if token.text.eq_ignore_ascii_case("SIMILAR") && p.peek_nth(1).is_word("TO") =>
        {
            return Some(InfixRule::custom(prec::COMPARE, parse_similar));
        }
        TokenKind::Word if token.text.eq_ignore_ascii_case("OVERLAPS") => {
            (prec::COMPARE, BinaryOp::Named("OVERLAPS".into()))
        }
        TokenKind::Word
            if token.text.eq_ignore_ascii_case("AT")
                && p.peek_nth(1).is_word("TIME")
                && p.peek_nth(2).is_word("ZONE") =>
        {
            return Some(InfixRule::custom(prec::AT_TIME_ZONE, parse_at_time_zone));
        }
        TokenKind::Word
            if token.text.eq_ignore_ascii_case("COLLATE")
                && matches!(p.peek_nth(1).kind, TokenKind::Word | TokenKind::QuotedIdent) =>
        {
            return Some(InfixRule::custom(prec::POSTFIX, parse_collate));
        }
        TokenKind::Word if token.text.eq_ignore_ascii_case("OR") => (prec::OR, BinaryOp::Or),
        TokenKind::Word if token.text.eq_ignore_ascii_case("AND") => (prec::AND, BinaryOp::And),
        _ => return None,
    };
    Some(InfixRule::binary(bp, op))
}

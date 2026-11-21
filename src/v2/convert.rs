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

//! Conversion from the v2 AST to this crate's AST.
//!
//! Every conversion either reproduces what this crate's parser builds for the
//! same SQL, or fails with [`Error::Unconverted`].

use sqlparser_v2::ast as v2;
use sqlparser_v2::dialect::builtin::{BuiltinQueryTail, BuiltinSelect};
use sqlparser_v2::dialect::Builtin;

use super::Error;
use crate::ast::helpers::attached_token::AttachedToken;
use crate::ast::*;
use crate::dialect::{Dialect, PostgreSqlDialect};
use crate::parser::{Parser, ParserError};
use crate::tokenizer::{Location, Span};

type Result<T> = core::result::Result<T, Error>;

type V2Expr<'a> = v2::Expr<'a, Builtin>;

fn unconverted<T>(what: impl Into<String>) -> Result<T> {
    Err(Error::Unconverted(what.into()))
}

/// Byte offsets to line and column numbers.
struct LineIndex<'s> {
    sql: &'s str,
    line_starts: Vec<usize>,
    /// Whether every line is ASCII, so that columns are byte offsets.
    ascii: bool,
}

impl<'s> LineIndex<'s> {
    fn new(sql: &'s str) -> Self {
        let line_starts = core::iter::once(0)
            .chain(sql.match_indices('\n').map(|(i, _)| i + 1))
            .collect();
        Self {
            sql,
            line_starts,
            ascii: sql.is_ascii(),
        }
    }

    fn location(&self, offset: u32) -> Location {
        let offset = (offset as usize).min(self.sql.len());
        let line = self.line_starts.partition_point(|&start| start <= offset);
        let start = self.line_starts[line - 1];
        let column = if self.ascii {
            offset - start
        } else {
            self.sql.get(start..offset).map_or(0, |s| s.chars().count())
        };
        Location::new(line as u64, column as u64 + 1)
    }

    fn span(&self, span: v2::Span) -> Span {
        if span == v2::Span::default() {
            return Span::empty();
        }
        Span::new(self.location(span.start), self.location(span.end))
    }
}

pub(super) fn parse_error(sql: &str, error: &sqlparser_v2::ParseError) -> ParserError {
    let at = LineIndex::new(sql).location(error.span.start);
    ParserError::ParserError(format!("{}{at}", error.message))
}

pub(super) struct Converter<'s> {
    dialect: &'s dyn Dialect,
    lines: LineIndex<'s>,
}

impl<'s> Converter<'s> {
    pub(super) fn new(dialect: &'s dyn Dialect, sql: &'s str) -> Self {
        Self {
            dialect,
            lines: LineIndex::new(sql),
        }
    }

    pub(super) fn statements(
        &self,
        statements: &[v2::Statement<'_, Builtin>],
    ) -> Result<Vec<Statement>> {
        statements.iter().map(|s| self.statement(s)).collect()
    }

    fn statement(&self, statement: &v2::Statement<'_, Builtin>) -> Result<Statement> {
        match &statement.kind {
            v2::StatementKind::Query(query) => Ok(Statement::Query(Box::new(self.query(query)?))),
            other => unconverted(format!("statement {}", variant_name(other))),
        }
    }

    // Names and literals

    fn ident(&self, ident: &v2::Ident<'_>) -> Ident {
        Ident {
            value: ident.value().into_owned(),
            quote_style: ident.quote,
            span: self.lines.span(ident.span),
        }
    }

    fn idents(&self, idents: &[v2::Ident<'_>]) -> Vec<Ident> {
        idents.iter().map(|i| self.ident(i)).collect()
    }

    fn object_name(&self, name: &v2::ObjectName<'_>) -> ObjectName {
        ObjectName(
            name.parts
                .iter()
                .map(|part| ObjectNamePart::Identifier(self.ident(part)))
                .collect(),
        )
    }

    fn literal(&self, literal: &v2::Literal<'_>) -> Result<Value> {
        Ok(match literal {
            v2::Literal::Number(text) => number(text)?,
            v2::Literal::String(string) => string_value(string)?,
            v2::Literal::HexString(digits) => Value::HexStringLiteral(digits.to_string()),
            v2::Literal::DollarQuoted { tag, text } => {
                Value::DollarQuotedString(DollarQuotedString {
                    value: text.to_string(),
                    tag: (!tag.is_empty()).then(|| tag.to_string()),
                })
            }
            v2::Literal::Null => Value::Null,
            v2::Literal::Boolean(b) => Value::Boolean(*b),
        })
    }

    fn value(&self, value: Value, span: v2::Span) -> Expr {
        Expr::Value(ValueWithSpan {
            value,
            span: self.lines.span(span),
        })
    }

    /// Converts a type by rendering it and parsing it with this crate's
    /// parser, which resolves type names differently in each dialect.
    fn data_type(&self, ty: &v2::DataType<'_>) -> Result<DataType> {
        let text = ty.to_string();
        let parsed = Parser::new(self.dialect)
            .try_with_sql(&text)
            .and_then(|mut parser| {
                let ty = parser.parse_data_type()?;
                parser.expect_token(&crate::tokenizer::Token::EOF)?;
                Ok(ty)
            });
        parsed.or_else(|_| unconverted(format!("data type {text}")))
    }

    // Expressions

    fn boxed(&self, expr: &V2Expr<'_>) -> Result<Box<Expr>> {
        self.expr(expr).map(Box::new)
    }

    fn exprs(&self, exprs: &[V2Expr<'_>]) -> Result<Vec<Expr>> {
        exprs.iter().map(|e| self.expr(e)).collect()
    }

    fn opt_expr(&self, expr: Option<&V2Expr<'_>>) -> Result<Option<Expr>> {
        expr.map(|e| self.expr(e)).transpose()
    }

    #[cfg_attr(feature = "recursive-protection", recursive::recursive)]
    fn expr(&self, expr: &V2Expr<'_>) -> Result<Expr> {
        use v2::ExprKind as K;
        Ok(match &expr.kind {
            K::Ident(ident) => Expr::Identifier(self.ident(ident)),
            K::Compound(name) => Expr::CompoundIdentifier(self.idents(&name.parts)),
            K::Literal(literal) => self.value(self.literal(literal)?, expr.span),
            K::Placeholder(text) => self.value(Value::Placeholder(text.to_string()), expr.span),
            K::Unary { op, expr } => match (op, &expr.kind) {
                (v2::UnaryOp::Not, K::Exists(query)) => Expr::Exists {
                    subquery: Box::new(self.query(query)?),
                    negated: true,
                },
                _ => Expr::UnaryOp {
                    op: unary_op(op),
                    expr: self.boxed(expr)?,
                },
            },
            K::Binary { left, op, right } => Expr::BinaryOp {
                left: self.boxed(left)?,
                op: binary_op(op)?,
                right: self.boxed(right)?,
            },
            K::Cast { expr, ty, style } => Expr::Cast {
                kind: match style {
                    v2::CastStyle::Function => CastKind::Cast,
                    v2::CastStyle::TryFunction => CastKind::TryCast,
                    v2::CastStyle::SafeFunction => CastKind::SafeCast,
                    v2::CastStyle::DoubleColon => CastKind::DoubleColon,
                },
                expr: self.boxed(expr)?,
                data_type: self.data_type(ty)?,
                format: None,
            },
            K::Interval(interval) => Expr::Interval(Interval {
                value: self.boxed(&interval.value)?,
                leading_field: interval
                    .leading_field
                    .as_deref()
                    .map(date_time_field)
                    .transpose()?,
                leading_precision: interval
                    .leading_precision
                    .as_deref()
                    .map(u64_arg)
                    .transpose()?,
                last_field: interval
                    .last_field
                    .as_deref()
                    .map(date_time_field)
                    .transpose()?,
                fractional_seconds_precision: interval
                    .fractional_seconds_precision
                    .as_deref()
                    .map(u64_arg)
                    .transpose()?,
            }),
            K::TypedString(typed) => Expr::TypedString(TypedString {
                data_type: self.data_type(&typed.ty)?,
                value: ValueWithSpan {
                    value: string_value(&typed.value)?,
                    span: Span::empty(),
                },
                uses_odbc_syntax: false,
            }),
            K::Special(special) => self.special(special)?,
            K::Function {
                name,
                args,
                options,
            } => self.function(name, args, options.as_deref())?,
            K::Subquery(query) => Expr::Subquery(Box::new(self.query(query)?)),
            K::Exists(query) => Expr::Exists {
                subquery: Box::new(self.query(query)?),
                negated: false,
            },
            K::InList {
                expr,
                list,
                negated,
            } => Expr::In {
                expr: self.boxed(expr)?,
                target: InTarget::List(self.exprs(list)?),
                negated: *negated,
                global: false,
            },
            K::InSubquery {
                expr,
                subquery,
                negated,
            } => Expr::In {
                expr: self.boxed(expr)?,
                target: InTarget::Subquery(Box::new(self.query(subquery)?)),
                negated: *negated,
                global: false,
            },
            K::Is {
                expr,
                test,
                negated,
            } => self.is(expr, test, *negated)?,
            K::Like {
                expr,
                pattern,
                negated,
                kind,
                escape,
            } => {
                let expr = self.boxed(expr)?;
                let pattern = self.boxed(pattern)?;
                let escape_char = escape.as_deref().map(|e| self.boxed(e)).transpose()?;
                let negated = *negated;
                match kind {
                    v2::LikeKind::Like => Expr::Like {
                        negated,
                        any: false,
                        expr,
                        pattern,
                        escape_char,
                    },
                    v2::LikeKind::ILike => Expr::ILike {
                        negated,
                        any: false,
                        expr,
                        pattern,
                        escape_char,
                    },
                    v2::LikeKind::SimilarTo => Expr::SimilarTo {
                        negated,
                        expr,
                        pattern,
                        escape_char,
                    },
                    v2::LikeKind::Named(name) if escape_char.is_none() => {
                        let regexp = match name.to_ascii_uppercase().as_str() {
                            "REGEXP" => true,
                            "RLIKE" => false,
                            _ => return unconverted(format!("{name} operator")),
                        };
                        Expr::RLike {
                            negated,
                            expr,
                            pattern,
                            regexp,
                        }
                    }
                    v2::LikeKind::Named(name) => return unconverted(format!("{name} operator")),
                }
            }
            K::Between {
                expr,
                low,
                high,
                negated,
            } => Expr::Between {
                expr: self.boxed(expr)?,
                negated: *negated,
                low: self.boxed(low)?,
                high: self.boxed(high)?,
            },
            K::Case(case) => Expr::Case {
                case_token: AttachedToken::empty(),
                end_token: AttachedToken::empty(),
                operand: case.operand.as_ref().map(|e| self.boxed(e)).transpose()?,
                conditions: case
                    .branches
                    .iter()
                    .map(|branch| {
                        Ok(CaseWhen {
                            condition: self.expr(&branch.condition)?,
                            result: self.expr(&branch.result)?,
                        })
                    })
                    .collect::<Result<_>>()?,
                else_result: case
                    .else_result
                    .as_ref()
                    .map(|e| self.boxed(e))
                    .transpose()?,
            },
            K::Nested(inner) => Expr::Nested(self.boxed(inner)?),
            K::Ext(_) => return unconverted("dialect-specific expression"),
        })
    }

    fn is(&self, expr: &V2Expr<'_>, test: &v2::IsTest<'_, Builtin>, negated: bool) -> Result<Expr> {
        let expr = self.boxed(expr)?;
        Ok(match (test, negated) {
            (v2::IsTest::Null, false) => Expr::IsNull(expr),
            (v2::IsTest::Null, true) => Expr::IsNotNull(expr),
            (v2::IsTest::True, false) => Expr::IsTrue(expr),
            (v2::IsTest::True, true) => Expr::IsNotTrue(expr),
            (v2::IsTest::False, false) => Expr::IsFalse(expr),
            (v2::IsTest::False, true) => Expr::IsNotFalse(expr),
            (v2::IsTest::Unknown, false) => Expr::IsUnknown(expr),
            (v2::IsTest::Unknown, true) => Expr::IsNotUnknown(expr),
            (v2::IsTest::DistinctFrom(other), false) => {
                Expr::IsDistinctFrom(expr, self.boxed(other)?)
            }
            (v2::IsTest::DistinctFrom(other), true) => {
                Expr::IsNotDistinctFrom(expr, self.boxed(other)?)
            }
            (v2::IsTest::Json { .. }, _) => return unconverted("IS JSON"),
            (v2::IsTest::Normalized(_), _) => return unconverted("IS NORMALIZED"),
        })
    }

    fn special(&self, special: &v2::SpecialExpr<'_, Builtin>) -> Result<Expr> {
        use v2::SpecialExpr as S;
        Ok(match special {
            S::Quantified {
                left,
                op,
                quantifier,
                right,
            } => {
                let left = self.boxed(left)?;
                let compare_op = binary_op(op)?;
                let right = self.boxed(right)?;
                match quantifier {
                    v2::Quantifier::Any | v2::Quantifier::Some => Expr::AnyOp {
                        left,
                        compare_op,
                        right,
                        is_some: matches!(quantifier, v2::Quantifier::Some),
                    },
                    v2::Quantifier::All => Expr::AllOp {
                        left,
                        compare_op,
                        right,
                    },
                }
            }
            S::AtTimeZone { expr, zone } => Expr::AtTimeZone {
                timestamp: self.boxed(expr)?,
                time_zone: self.boxed(zone)?,
            },
            S::Collate { expr, collation } => Expr::Collate {
                expr: self.boxed(expr)?,
                collation: self.object_name(collation),
            },
            S::Subscript { .. } | S::Slice { .. } => self.subscripts(special)?,
            S::Array(items) => Expr::Array(Array {
                elem: self.exprs(items)?,
                named: true,
            }),
            S::Tuple(items) => Expr::Tuple(self.exprs(items)?),
            S::Extract { field, expr } => Expr::Extract {
                field: date_time_field(field)?,
                syntax: ExtractSyntax::From,
                expr: self.boxed(expr)?,
            },
            S::Round { ceil, expr, field } => {
                let expr = self.boxed(expr)?;
                let field = CeilFloorKind::DateTimeField(date_time_field(field)?);
                if *ceil {
                    Expr::Ceil { expr, field }
                } else {
                    Expr::Floor { expr, field }
                }
            }
            S::Position { needle, haystack } => Expr::Position {
                expr: self.boxed(needle)?,
                r#in: self.boxed(haystack)?,
            },
            S::Substring {
                shorthand,
                expr,
                from,
                length,
            } => Expr::Substring {
                expr: self.boxed(expr)?,
                substring_from: from.as_ref().map(|e| self.boxed(e)).transpose()?,
                substring_for: length.as_ref().map(|e| self.boxed(e)).transpose()?,
                special: false,
                shorthand: *shorthand,
            },
            S::Trim { side, what, expr } => Expr::Trim {
                trim_where: side.as_ref().map(|side| match side {
                    v2::TrimSide::Both => TrimWhereField::Both,
                    v2::TrimSide::Leading => TrimWhereField::Leading,
                    v2::TrimSide::Trailing => TrimWhereField::Trailing,
                }),
                trim_what: what.as_ref().map(|e| self.boxed(e)).transpose()?,
                expr: self.boxed(expr)?,
                trim_characters: None,
            },
            S::Overlay {
                expr,
                what,
                from,
                length,
            } => Expr::Overlay {
                expr: self.boxed(expr)?,
                overlay_what: self.boxed(what)?,
                overlay_from: self.boxed(from)?,
                overlay_for: length.as_ref().map(|e| self.boxed(e)).transpose()?,
            },
            S::Grouping { kind, sets } => {
                let sets = sets
                    .iter()
                    .map(|set| self.exprs(set))
                    .collect::<Result<_>>()?;
                match kind {
                    v2::GroupingKind::Rollup => Expr::Rollup(sets),
                    v2::GroupingKind::Cube => Expr::Cube(sets),
                    v2::GroupingKind::Sets => Expr::GroupingSets(sets),
                }
            }
            // This crate reads these as functions only in PostgreSQL.
            S::Niladic(name)
                if !self.dialect.is::<PostgreSqlDialect>()
                    && name.quote.is_none()
                    && ["CURRENT_CATALOG", "CURRENT_USER", "SESSION_USER", "USER"]
                        .iter()
                        .any(|word| name.text.eq_ignore_ascii_case(word)) =>
            {
                Expr::Identifier(self.ident(name))
            }
            S::Niladic(name) => Expr::Function(Function {
                name: ObjectName::from(vec![self.ident(name)]),
                uses_odbc_syntax: false,
                parameters: FunctionArguments::None,
                args: FunctionArguments::None,
                filter: None,
                null_treatment: None,
                over: None,
                within_group: vec![],
            }),
            S::Prior(expr) => Expr::Prior(self.boxed(expr)?),
            S::OuterJoinMark(expr) => Expr::OuterJoin(self.boxed(expr)?),
            other => return unconverted(format!("expression {}", variant_name(other))),
        })
    }

    /// Flattens a chain of subscripts into one access expression.
    fn subscripts(&self, special: &v2::SpecialExpr<'_, Builtin>) -> Result<Expr> {
        let mut chain = Vec::new();
        let mut current = special;
        let root = loop {
            let (inner, subscript) = match current {
                v2::SpecialExpr::Subscript { expr, index } => (
                    expr,
                    Subscript::Index {
                        index: self.expr(index)?,
                    },
                ),
                v2::SpecialExpr::Slice {
                    expr,
                    lower,
                    upper,
                    step,
                } => (
                    expr,
                    Subscript::Slice {
                        lower_bound: self.opt_expr(lower.as_ref())?,
                        upper_bound: self.opt_expr(upper.as_ref())?,
                        stride: self.opt_expr(step.as_ref())?,
                    },
                ),
                other => return unconverted(format!("expression {}", variant_name(other))),
            };
            chain.push(AccessExpr::Subscript(subscript));
            match &inner.kind {
                v2::ExprKind::Special(next)
                    if matches!(
                        **next,
                        v2::SpecialExpr::Subscript { .. } | v2::SpecialExpr::Slice { .. }
                    ) =>
                {
                    current = next
                }
                v2::ExprKind::Compound(name) => {
                    let (first, rest) =
                        name.parts.split_first().expect("compound names have parts");
                    chain.extend(
                        rest.iter()
                            .rev()
                            .map(|part| AccessExpr::Dot(Expr::Identifier(self.ident(part)))),
                    );
                    break Box::new(Expr::Identifier(self.ident(first)));
                }
                _ => break self.boxed(inner)?,
            }
        };
        chain.reverse();
        Ok(Expr::CompoundFieldAccess {
            root,
            access_chain: chain,
        })
    }

    fn function(
        &self,
        name: &v2::ObjectName<'_>,
        args: &[v2::FunctionArg<'_, Builtin>],
        options: Option<&v2::FunctionOptions<'_, Builtin>>,
    ) -> Result<Expr> {
        if options.is_none() {
            if let Some(expr) = self.keyword_function(name, args)? {
                return Ok(expr);
            }
        }
        let mut list = FunctionArgumentList {
            duplicate_treatment: None,
            args: self.function_args(args)?,
            clauses: vec![],
        };
        let mut function = Function {
            name: self.object_name(name),
            uses_odbc_syntax: false,
            parameters: FunctionArguments::None,
            args: FunctionArguments::None,
            filter: None,
            null_treatment: None,
            over: None,
            within_group: vec![],
        };
        if let Some(options) = options {
            if options.separator.is_some() || options.on_overflow.is_some() {
                return unconverted("function argument clauses");
            }
            if !options.parameters.is_empty() {
                function.parameters = FunctionArguments::List(FunctionArgumentList {
                    duplicate_treatment: None,
                    args: self.function_args(&options.parameters)?,
                    clauses: vec![],
                });
            }
            list.duplicate_treatment = match options.quantifier {
                v2::SetQuantifier::None => None,
                v2::SetQuantifier::All => Some(DuplicateTreatment::All),
                v2::SetQuantifier::Distinct => Some(DuplicateTreatment::Distinct),
            };
            if let Some(treatment) = &options.inner_null_treatment {
                list.clauses
                    .push(FunctionArgumentClause::IgnoreOrRespectNulls(
                        null_treatment(treatment),
                    ));
            }
            if !options.order_by.is_empty() {
                list.clauses.push(FunctionArgumentClause::OrderBy(
                    self.order_by_items(&options.order_by)?,
                ));
            }
            function.within_group = self.order_by_items(&options.within_group)?;
            function.filter = options.filter.as_ref().map(|e| self.boxed(e)).transpose()?;
            function.null_treatment = options.null_treatment.as_ref().map(null_treatment);
            function.over = options
                .over
                .as_ref()
                .map(|o| self.window_over(o))
                .transpose()?;
        }
        function.args = FunctionArguments::List(list);
        Ok(Expr::Function(function))
    }

    /// Calls that this crate's parser reads into their own nodes.
    fn keyword_function(
        &self,
        name: &v2::ObjectName<'_>,
        args: &[v2::FunctionArg<'_, Builtin>],
    ) -> Result<Option<Expr>> {
        let [name] = name.parts.as_slice() else {
            return Ok(None);
        };
        if name.quote.is_some() {
            return Ok(None);
        }
        let mut exprs = Vec::with_capacity(args.len());
        for arg in args {
            let v2::FunctionArg::Expr(expr) = arg else {
                return Ok(None);
            };
            exprs.push(expr);
        }
        let name = name.text.to_ascii_uppercase();
        Ok(Some(match (name.as_str(), exprs.as_slice()) {
            ("SUBSTR" | "SUBSTRING", [expr, rest @ ..]) if rest.len() <= 2 => Expr::Substring {
                expr: self.boxed(expr)?,
                substring_from: rest.first().map(|e| self.boxed(e)).transpose()?,
                substring_for: rest.get(1).map(|e| self.boxed(e)).transpose()?,
                special: !rest.is_empty(),
                shorthand: name == "SUBSTR",
            },
            ("CEIL" | "FLOOR", [expr, scale @ ..]) if scale.len() <= 1 => {
                let field = match scale.first().map(|e| &e.kind) {
                    None => CeilFloorKind::DateTimeField(DateTimeField::NoDateTime),
                    Some(v2::ExprKind::Literal(literal @ v2::Literal::Number(_))) => {
                        CeilFloorKind::Scale(ValueWithSpan {
                            value: self.literal(literal)?,
                            span: self.lines.span(scale[0].span),
                        })
                    }
                    Some(_) => return Ok(None),
                };
                let expr = self.boxed(expr)?;
                if name == "CEIL" {
                    Expr::Ceil { expr, field }
                } else {
                    Expr::Floor { expr, field }
                }
            }
            ("TRIM", [expr, characters @ ..])
                if characters.is_empty() || self.dialect.supports_comma_separated_trim() =>
            {
                Expr::Trim {
                    trim_where: None,
                    trim_what: None,
                    expr: self.boxed(expr)?,
                    trim_characters: if characters.is_empty() {
                        None
                    } else {
                        Some(
                            characters
                                .iter()
                                .map(|e| self.expr(e))
                                .collect::<Result<_>>()?,
                        )
                    },
                }
            }
            _ => return Ok(None),
        }))
    }

    fn function_args(&self, args: &[v2::FunctionArg<'_, Builtin>]) -> Result<Vec<FunctionArg>> {
        args.iter()
            .map(|arg| {
                Ok(match arg {
                    v2::FunctionArg::Wildcard => FunctionArg::Unnamed(FunctionArgExpr::Wildcard),
                    v2::FunctionArg::QualifiedWildcard(name) => FunctionArg::Unnamed(
                        FunctionArgExpr::QualifiedWildcard(self.object_name(name)),
                    ),
                    v2::FunctionArg::Expr(expr) => {
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(self.expr(expr)?))
                    }
                    v2::FunctionArg::Named { name, value } => FunctionArg::Named {
                        name: self.ident(name),
                        arg: FunctionArgExpr::Expr(self.expr(value)?),
                        operator: FunctionArgOperator::RightArrow,
                    },
                    v2::FunctionArg::NamedWith {
                        name,
                        operator,
                        value,
                    } => FunctionArg::Named {
                        name: self.ident(name),
                        arg: FunctionArgExpr::Expr(self.expr(value)?),
                        operator: match operator.as_ref() {
                            "=" => FunctionArgOperator::Equals,
                            ":=" => FunctionArgOperator::Assignment,
                            ":" => FunctionArgOperator::Colon,
                            other => {
                                return unconverted(format!("named argument operator {other}"))
                            }
                        },
                    },
                })
            })
            .collect()
    }

    fn window_over(&self, over: &v2::WindowOver<'_, Builtin>) -> Result<WindowType> {
        Ok(match over {
            v2::WindowOver::Named(name) => WindowType::NamedWindow(self.ident(name)),
            v2::WindowOver::Spec(spec) => WindowType::WindowSpec(self.window_spec(spec)?),
        })
    }

    fn window_spec(&self, spec: &v2::WindowSpec<'_, Builtin>) -> Result<WindowSpec> {
        Ok(WindowSpec {
            window_name: spec.base.as_ref().map(|i| self.ident(i)),
            partition_by: self.exprs(&spec.partition_by)?,
            order_by: self.order_by_items(&spec.order_by)?,
            window_frame: spec
                .frame
                .as_ref()
                .map(|frame| {
                    Ok(WindowFrame {
                        units: match frame.units {
                            v2::FrameUnits::Rows => WindowFrameUnits::Rows,
                            v2::FrameUnits::Range => WindowFrameUnits::Range,
                            v2::FrameUnits::Groups => WindowFrameUnits::Groups,
                        },
                        start_bound: self.frame_bound(&frame.start)?,
                        end_bound: frame
                            .end
                            .as_ref()
                            .map(|b| self.frame_bound(b))
                            .transpose()?,
                    })
                })
                .transpose()?,
        })
    }

    fn frame_bound(&self, bound: &v2::FrameBound<'_, Builtin>) -> Result<WindowFrameBound> {
        Ok(match bound {
            v2::FrameBound::CurrentRow => WindowFrameBound::CurrentRow,
            v2::FrameBound::UnboundedPreceding => WindowFrameBound::Preceding(None),
            v2::FrameBound::UnboundedFollowing => WindowFrameBound::Following(None),
            v2::FrameBound::Preceding(e) => WindowFrameBound::Preceding(Some(self.boxed(e)?)),
            v2::FrameBound::Following(e) => WindowFrameBound::Following(Some(self.boxed(e)?)),
        })
    }

    // Queries

    #[cfg_attr(feature = "recursive-protection", recursive::recursive)]
    fn query(&self, query: &v2::Query<'_, Builtin>) -> Result<Query> {
        if query.distribution.is_some()
            || !query.locks.is_empty()
            || !query.pipes.is_empty()
            || query.tail != BuiltinQueryTail::default()
        {
            return unconverted("query clauses");
        }
        Ok(Query {
            with: query.with.as_deref().map(|w| self.with(w)).transpose()?,
            body: Box::new(self.set_expr(&query.body)?),
            order_by: if query.order_by.is_empty() {
                None
            } else {
                Some(OrderBy {
                    kind: OrderByKind::Expressions(self.order_by_items(&query.order_by)?),
                    interpolate: None,
                })
            },
            limit_clause: query.limit.as_deref().map(|l| self.limit(l)).transpose()?,
            fetch: query
                .fetch
                .as_deref()
                .map(|fetch| {
                    Ok(Fetch {
                        with_ties: fetch.with_ties,
                        percent: fetch.percent,
                        quantity: self.opt_expr(fetch.quantity.as_ref())?,
                    })
                })
                .transpose()?,
            locks: vec![],
            for_clause: None,
            settings: None,
            format_clause: None,
            pipe_operators: vec![],
        })
    }

    fn with(&self, with: &v2::With<'_, Builtin>) -> Result<With> {
        Ok(With {
            with_token: AttachedToken::empty(),
            recursive: with.recursive,
            cte_tables: with
                .ctes
                .iter()
                .map(|cte| {
                    Ok(Cte {
                        alias: TableAlias {
                            explicit: false,
                            name: self.ident(&cte.name),
                            columns: cte
                                .columns
                                .iter()
                                .map(|c| TableAliasColumnDef::from_name(self.ident(c).value))
                                .collect(),
                            at: None,
                        },
                        query: Box::new(self.query(&cte.query)?),
                        from: None,
                        materialized: cte.materialized.map(|m| {
                            if m {
                                CteAsMaterialized::Materialized
                            } else {
                                CteAsMaterialized::NotMaterialized
                            }
                        }),
                        closing_paren_token: AttachedToken::empty(),
                    })
                })
                .collect::<Result<_>>()?,
        })
    }

    fn limit(&self, limit: &v2::Limit<'_, Builtin>) -> Result<LimitClause> {
        if limit.all {
            return unconverted("LIMIT ALL");
        }
        if limit.offset_comma {
            let (Some(offset), Some(count)) = (&limit.offset, &limit.count) else {
                return unconverted("LIMIT offset, count");
            };
            return Ok(LimitClause::OffsetCommaLimit {
                offset: self.expr(offset)?,
                limit: self.expr(count)?,
            });
        }
        Ok(LimitClause::LimitOffset {
            limit: self.opt_expr(limit.count.as_ref())?,
            offset: limit
                .offset
                .as_ref()
                .map(|offset| {
                    Ok(Offset {
                        value: self.expr(offset)?,
                        rows: match limit.offset_rows {
                            v2::OffsetRows::None => OffsetRows::None,
                            v2::OffsetRows::Row => OffsetRows::Row,
                            v2::OffsetRows::Rows => OffsetRows::Rows,
                        },
                    })
                })
                .transpose()?,
            limit_by: self.exprs(&limit.by)?,
        })
    }

    fn order_by_items(&self, items: &[v2::OrderByItem<'_, Builtin>]) -> Result<Vec<OrderByExpr>> {
        items
            .iter()
            .map(|item| {
                if item.ext != Default::default() {
                    return unconverted("ORDER BY item options");
                }
                let sort = match (&item.using, item.asc) {
                    (Some(_), _) => return unconverted("ORDER BY USING"),
                    (None, Some(true)) => Some(OrderBySort::Asc),
                    (None, Some(false)) => Some(OrderBySort::Desc),
                    (None, None) => None,
                };
                Ok(OrderByExpr {
                    expr: self.expr(&item.expr)?,
                    options: OrderByOptions {
                        sort,
                        nulls_first: item.nulls_first,
                    },
                    with_fill: None,
                })
            })
            .collect()
    }

    #[cfg_attr(feature = "recursive-protection", recursive::recursive)]
    fn set_expr(&self, body: &v2::SetExpr<'_, Builtin>) -> Result<SetExpr> {
        Ok(match body {
            v2::SetExpr::Select(select) => SetExpr::Select(Box::new(self.select(select)?)),
            v2::SetExpr::Query(query) => SetExpr::Query(Box::new(self.query(query)?)),
            v2::SetExpr::Values(rows) => SetExpr::Values(self.values(rows, false)?),
            v2::SetExpr::RowValues(rows) => SetExpr::Values(self.values(rows, true)?),
            v2::SetExpr::Statement(_) => return unconverted("statement as a query body"),
            v2::SetExpr::Table(_) => return unconverted("TABLE query"),
            v2::SetExpr::SetOperation {
                left,
                op,
                quantifier,
                by_name,
                right,
            } => SetExpr::SetOperation {
                left: Box::new(self.set_expr(left)?),
                op: match op {
                    v2::SetOperator::Union => SetOperator::Union,
                    v2::SetOperator::Except => SetOperator::Except,
                    v2::SetOperator::Intersect => SetOperator::Intersect,
                    v2::SetOperator::Minus => SetOperator::Minus,
                },
                set_quantifier: match (quantifier, by_name) {
                    (v2::SetQuantifier::None, false) => SetQuantifier::None,
                    (v2::SetQuantifier::All, false) => SetQuantifier::All,
                    (v2::SetQuantifier::Distinct, false) => SetQuantifier::Distinct,
                    (v2::SetQuantifier::None, true) => SetQuantifier::ByName,
                    (v2::SetQuantifier::All, true) => SetQuantifier::AllByName,
                    (v2::SetQuantifier::Distinct, true) => SetQuantifier::DistinctByName,
                },
                right: Box::new(self.set_expr(right)?),
            },
        })
    }

    fn values(&self, rows: &[Vec<V2Expr<'_>>], explicit_row: bool) -> Result<Values> {
        Ok(Values {
            explicit_row,
            value_keyword: false,
            rows: rows
                .iter()
                .map(|row| {
                    Ok(Parens {
                        opening_token: AttachedToken::empty(),
                        content: self.exprs(row)?,
                        closing_token: AttachedToken::empty(),
                    })
                })
                .collect::<Result<_>>()?,
        })
    }

    fn select(&self, select: &v2::Select<'_, Builtin>) -> Result<Select> {
        let BuiltinSelect { distinct_on, .. } = &select.modifiers;
        let without_distinct_on = BuiltinSelect {
            distinct_on: None,
            ..select.modifiers.clone()
        };
        if without_distinct_on != BuiltinSelect::default() {
            return unconverted("SELECT modifiers");
        }
        if select.into.is_some()
            || !select.lateral_views.is_empty()
            || select.prewhere.is_some()
            || !select.connect_by.is_empty()
            || select.grouping_sets.is_some()
            || select.group_by_modifier.is_some()
            || !select.windows.is_empty()
            || select.from_first
        {
            return unconverted("SELECT clauses");
        }
        let distinct = match (distinct_on, &select.quantifier) {
            (Some(exprs), _) => Some(Distinct::On(self.exprs(exprs)?)),
            (None, v2::SetQuantifier::None) => None,
            (None, v2::SetQuantifier::All) => Some(Distinct::All),
            (None, v2::SetQuantifier::Distinct) => Some(Distinct::Distinct),
        };
        let group_by = if select.group_by_all {
            GroupByExpr::All(vec![])
        } else {
            GroupByExpr::Expressions(self.exprs(&select.group_by)?, vec![])
        };
        Ok(Select {
            select_token: AttachedToken::empty(),
            optimizer_hints: vec![],
            distinct,
            select_modifiers: None,
            top: None,
            top_before_distinct: false,
            projection: select
                .projection
                .iter()
                .map(|item| self.select_item(item))
                .collect::<Result<_>>()?,
            exclude: None,
            into: None,
            from: select
                .from
                .iter()
                .map(|t| self.table_with_joins(t))
                .collect::<Result<_>>()?,
            lateral_views: vec![],
            prewhere: None,
            selection: self.opt_expr(select.selection.as_ref())?,
            connect_by: vec![],
            group_by,
            cluster_by: vec![],
            distribute_by: vec![],
            sort_by: vec![],
            having: self.opt_expr(select.having.as_ref())?,
            named_window: vec![],
            qualify: self.opt_expr(select.qualify.as_ref())?,
            window_before_qualify: false,
            value_table_mode: None,
            flavor: SelectFlavor::Standard,
        })
    }

    fn select_item(&self, item: &v2::SelectItem<'_, Builtin>) -> Result<SelectItem> {
        Ok(match &item.kind {
            v2::SelectItemKind::Wildcard => SelectItem::Wildcard(Default::default()),
            v2::SelectItemKind::QualifiedWildcard(name) => SelectItem::QualifiedWildcard(
                SelectItemQualifiedWildcardKind::ObjectName(self.object_name(name)),
                Default::default(),
            ),
            v2::SelectItemKind::WildcardWithOptions { .. } => {
                return unconverted("wildcard options")
            }
            v2::SelectItemKind::Expr { expr, alias: None } => {
                SelectItem::UnnamedExpr(self.expr(expr)?)
            }
            v2::SelectItemKind::Expr {
                expr,
                alias: Some(alias),
            } => SelectItem::ExprWithAlias {
                expr: self.expr(expr)?,
                alias: self.ident(alias),
            },
            v2::SelectItemKind::ExprWithAliases { expr, aliases } => SelectItem::ExprWithAliases {
                expr: self.expr(expr)?,
                aliases: self.idents(aliases),
            },
        })
    }

    fn table_with_joins(&self, table: &v2::TableWithJoins<'_, Builtin>) -> Result<TableWithJoins> {
        Ok(TableWithJoins {
            relation: self.table_ref(&table.relation)?,
            joins: table
                .joins
                .iter()
                .map(|join| self.join(join))
                .collect::<Result<_>>()?,
        })
    }

    fn table_alias(&self, alias: Option<&v2::TableAlias<'_>>) -> Result<Option<TableAlias>> {
        let Some(alias) = alias else {
            return Ok(None);
        };
        if !alias.column_types.is_empty() {
            return unconverted("table alias column types");
        }
        Ok(Some(TableAlias {
            explicit: alias.explicit,
            name: self.ident(&alias.name),
            columns: alias
                .columns
                .iter()
                .map(|c| TableAliasColumnDef::from_name(self.ident(c).value))
                .collect(),
            at: None,
        }))
    }

    fn table_ref(&self, table: &v2::TableRef<'_, Builtin>) -> Result<TableFactor> {
        let alias = self.table_alias(table.alias.as_ref())?;
        self.table_factor(&table.factor, alias, false)
    }

    fn table_factor(
        &self,
        factor: &v2::TableFactor<'_, Builtin>,
        alias: Option<TableAlias>,
        lateral: bool,
    ) -> Result<TableFactor> {
        Ok(match factor {
            v2::TableFactor::Named(name) if !lateral => TableFactor::Table {
                name: self.object_name(name),
                alias,
                args: None,
                with_hints: vec![],
                version: None,
                with_ordinality: false,
                partitions: vec![],
                json_path: None,
                sample: None,
                index_directives: vec![],
                inheritance: None,
            },
            v2::TableFactor::Derived(query) => TableFactor::Derived {
                lateral,
                subquery: Box::new(self.query(query)?),
                alias,
                sample: None,
            },
            v2::TableFactor::NestedJoin(inner) if !lateral => TableFactor::NestedJoin {
                table_with_joins: Box::new(self.table_with_joins(inner)?),
                alias,
            },
            v2::TableFactor::Lateral(inner) if !lateral => self.table_factor(inner, alias, true)?,
            other => return unconverted(format!("table factor {}", variant_name(other))),
        })
    }

    fn join(&self, join: &v2::Join<'_, Builtin>) -> Result<Join> {
        let relation = self.table_ref(&join.table)?;
        let join_operator = match (&join.kind, &join.constraint) {
            (
                v2::JoinKind::AsOf,
                v2::JoinConstraint::AsOf {
                    condition,
                    constraint,
                },
            ) => JoinOperator::AsOf {
                match_condition: self.expr(condition)?,
                constraint: self.join_constraint(constraint)?,
            },
            (kind, constraint) => {
                let constraint = self.join_constraint(constraint)?;
                match (kind, join.spelled_out) {
                    (v2::JoinKind::Inner, false) => JoinOperator::Join(constraint),
                    (v2::JoinKind::Inner, true) => JoinOperator::Inner(constraint),
                    (v2::JoinKind::Left, false) => JoinOperator::Left(constraint),
                    (v2::JoinKind::Left, true) => JoinOperator::LeftOuter(constraint),
                    (v2::JoinKind::Right, false) => JoinOperator::Right(constraint),
                    (v2::JoinKind::Right, true) => JoinOperator::RightOuter(constraint),
                    (v2::JoinKind::Full, _) => JoinOperator::FullOuter(constraint),
                    (v2::JoinKind::Cross, _) => JoinOperator::CrossJoin(constraint),
                    (v2::JoinKind::Semi, _) => JoinOperator::Semi(constraint),
                    (v2::JoinKind::LeftSemi, _) => JoinOperator::LeftSemi(constraint),
                    (v2::JoinKind::RightSemi, _) => JoinOperator::RightSemi(constraint),
                    (v2::JoinKind::Anti, _) => JoinOperator::Anti(constraint),
                    (v2::JoinKind::LeftAnti, _) => JoinOperator::LeftAnti(constraint),
                    (v2::JoinKind::RightAnti, _) => JoinOperator::RightAnti(constraint),
                    (other, _) => return unconverted(format!("join {other:?}")),
                }
            }
        };
        Ok(Join {
            relation,
            global: false,
            join_operator,
        })
    }

    fn join_constraint(
        &self,
        constraint: &v2::JoinConstraint<'_, Builtin>,
    ) -> Result<JoinConstraint> {
        Ok(match constraint {
            v2::JoinConstraint::None => JoinConstraint::None,
            v2::JoinConstraint::On(expr) => JoinConstraint::On(self.expr(expr)?),
            v2::JoinConstraint::Using(columns) => JoinConstraint::Using(
                columns
                    .iter()
                    .map(|c| ObjectName::from(vec![self.ident(c)]))
                    .collect(),
            ),
            v2::JoinConstraint::Natural => JoinConstraint::Natural,
            v2::JoinConstraint::AsOf { .. } | v2::JoinConstraint::Ext(_) => {
                return unconverted("join constraint")
            }
        })
    }
}

fn variant_name(value: &impl core::fmt::Debug) -> String {
    let debug = format!("{value:?}");
    let end = debug
        .find(|c: char| !c.is_alphanumeric() && c != '_')
        .unwrap_or(debug.len());
    debug[..end].to_string()
}

fn number(text: &str) -> Result<Value> {
    let (text, long) = match text.strip_suffix('L') {
        Some(digits) => (digits, true),
        None => (text, false),
    };
    #[cfg(feature = "bigdecimal")]
    {
        match text.parse() {
            Ok(n) => Ok(Value::Number(n, long)),
            Err(_) => unconverted(format!("number {text}")),
        }
    }
    #[cfg(not(feature = "bigdecimal"))]
    {
        Ok(Value::Number(text.to_string(), long))
    }
}

fn string_value(string: &v2::StringLiteral<'_>) -> Result<Value> {
    use v2::StringPrefix as P;
    let value = string.value().into_owned();
    Ok(
        match (string.prefix, string.quote, string.triple, string.national) {
            (P::None, '\'', false, false) => Value::SingleQuotedString(value),
            (P::None, '"', false, false) => Value::DoubleQuotedString(value),
            (P::None, '\'', true, false) => Value::TripleSingleQuotedString(value),
            (P::None, '"', true, false) => Value::TripleDoubleQuotedString(value),
            (P::None, '\'', false, true) => Value::NationalStringLiteral(value),
            (P::Escape, '\'', false, false) => Value::EscapedStringLiteral(value),
            (P::Unicode, '\'', false, false) => Value::UnicodeStringLiteral(value),
            (P::Byte, '\'', false, false) => Value::SingleQuotedByteStringLiteral(value),
            (P::Byte, '"', false, false) => Value::DoubleQuotedByteStringLiteral(value),
            (P::Byte, '\'', true, false) => Value::TripleSingleQuotedByteStringLiteral(value),
            (P::Byte, '"', true, false) => Value::TripleDoubleQuotedByteStringLiteral(value),
            (P::Raw, '\'', false, false) => Value::SingleQuotedRawStringLiteral(value),
            (P::Raw, '"', false, false) => Value::DoubleQuotedRawStringLiteral(value),
            (P::Raw, '\'', true, false) => Value::TripleSingleQuotedRawStringLiteral(value),
            (P::Raw, '"', true, false) => Value::TripleDoubleQuotedRawStringLiteral(value),
            _ => return unconverted("string literal form"),
        },
    )
}

fn u64_arg(text: &str) -> Result<u64> {
    text.parse()
        .or_else(|_| unconverted(format!("number {text}")))
}

fn date_time_field(text: &str) -> Result<DateTimeField> {
    use DateTimeField as F;
    Ok(match text.to_ascii_uppercase().as_str() {
        "YEAR" => F::Year,
        "YEARS" => F::Years,
        "MONTH" => F::Month,
        "MONTHS" => F::Months,
        "WEEK" => F::Week(None),
        "WEEKS" => F::Weeks,
        "DAY" => F::Day,
        "DAYS" => F::Days,
        "DAYOFWEEK" => F::DayOfWeek,
        "DAYOFYEAR" => F::DayOfYear,
        "DATE" => F::Date,
        "DATETIME" => F::Datetime,
        "HOUR" => F::Hour,
        "HOURS" => F::Hours,
        "MINUTE" => F::Minute,
        "MINUTES" => F::Minutes,
        "SECOND" => F::Second,
        "SECONDS" => F::Seconds,
        "CENTURY" => F::Century,
        "DECADE" => F::Decade,
        "DOW" => F::Dow,
        "DOY" => F::Doy,
        "EPOCH" => F::Epoch,
        "ISODOW" => F::Isodow,
        "ISOYEAR" => F::Isoyear,
        "ISOWEEK" => F::IsoWeek,
        "JULIAN" => F::Julian,
        "MICROSECOND" => F::Microsecond,
        "MICROSECONDS" => F::Microseconds,
        "MILLENIUM" => F::Millenium,
        "MILLENNIUM" => F::Millennium,
        "MILLISECOND" => F::Millisecond,
        "MILLISECONDS" => F::Milliseconds,
        "NANOSECOND" => F::Nanosecond,
        "NANOSECONDS" => F::Nanoseconds,
        "QUARTER" => F::Quarter,
        "TIME" => F::Time,
        "TIMEZONE" => F::Timezone,
        "TIMEZONE_ABBR" => F::TimezoneAbbr,
        "TIMEZONE_HOUR" => F::TimezoneHour,
        "TIMEZONE_MINUTE" => F::TimezoneMinute,
        "TIMEZONE_REGION" => F::TimezoneRegion,
        _ => return unconverted(format!("date/time field {text}")),
    })
}

fn null_treatment(treatment: &v2::NullTreatment) -> NullTreatment {
    match treatment {
        v2::NullTreatment::Ignore => NullTreatment::IgnoreNulls,
        v2::NullTreatment::Respect => NullTreatment::RespectNulls,
    }
}

fn unary_op(op: &v2::UnaryOp) -> UnaryOperator {
    match op {
        v2::UnaryOp::Neg => UnaryOperator::Minus,
        v2::UnaryOp::Plus => UnaryOperator::Plus,
        v2::UnaryOp::Not => UnaryOperator::Not,
        v2::UnaryOp::BangNot => UnaryOperator::BangNot,
        v2::UnaryOp::BitNot => UnaryOperator::BitwiseNot,
    }
}

fn binary_op(op: &v2::BinaryOp<'_>) -> Result<BinaryOperator> {
    use v2::BinaryOp as B;
    use BinaryOperator as O;
    Ok(match op {
        B::Or => O::Or,
        B::And => O::And,
        B::Eq => O::Eq,
        B::NotEq => O::NotEq,
        B::Lt => O::Lt,
        B::LtEq => O::LtEq,
        B::Gt => O::Gt,
        B::GtEq => O::GtEq,
        B::Add => O::Plus,
        B::Sub => O::Minus,
        B::Mul => O::Multiply,
        B::Div => O::Divide,
        B::Mod => O::Modulo,
        B::StringConcat => O::StringConcat,
        B::BitOr => O::BitwiseOr,
        B::BitAnd => O::BitwiseAnd,
        B::BitXor => O::BitwiseXor,
        B::ShiftLeft => O::PGBitwiseShiftLeft,
        B::ShiftRight => O::PGBitwiseShiftRight,
        B::Named(name) => match name.as_ref() {
            "<=>" => O::Spaceship,
            "->" => O::Arrow,
            "->>" => O::LongArrow,
            "#>" => O::HashArrow,
            "#>>" => O::HashLongArrow,
            "@>" => O::AtArrow,
            "<@" => O::ArrowAt,
            "~" => O::PGRegexMatch,
            "~*" => O::PGRegexIMatch,
            "!~" => O::PGRegexNotMatch,
            "!~*" => O::PGRegexNotIMatch,
            "&&" => O::PGOverlap,
            _ => return unconverted(format!("operator {name}")),
        },
    })
}

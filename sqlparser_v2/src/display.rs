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

//! Renders the AST back to SQL.
//!
//! Parsing then rendering reproduces the source's identifiers, literals,
//! parentheses and clause forms. Keywords are upper-cased, whitespace is
//! normalized, and equivalent spellings such as `INNER JOIN` take one form.
//!
//! `{:#}` renders the same text over several lines, using [`SpaceOrNewline`],
//! [`NewLine`] and [`Indent`]. A node that renders a child with `write!` drops
//! the alternate flag, which keeps that child on one line.

use crate::ast::ExprKind;
use crate::ast::SelectItemKind;
use crate::ast::StatementKind;
use std::borrow::Cow;
use std::cell::Cell;
use std::fmt::{Display, Formatter, Result};

use crate::ast::*;
use crate::dialect::AstExt;

thread_local! {
    /// How many levels of [`Indent`] enclose the node being rendered.
    ///
    /// A counter, rather than a writer per level as in the original crate,
    /// keeps the cost of a write independent of depth. It also means only the
    /// line breaks written here are indented, and never a newline in a string.
    static INDENT_DEPTH: Cell<usize> = const { Cell::new(0) };
}

const INDENT: &str = "  ";
const PADDING: &str = "                                                                ";

/// Restores the indent depth, even if a nested `Display` panics.
struct IndentScope;

impl IndentScope {
    fn enter() -> Self {
        INDENT_DEPTH.with(|depth| depth.set(depth.get() + 1));
        IndentScope
    }
}

impl Drop for IndentScope {
    fn drop(&mut self) {
        INDENT_DEPTH.with(|depth| depth.set(depth.get() - 1));
    }
}

fn write_line_break(f: &mut Formatter<'_>) -> Result {
    f.write_str("\n")?;
    let mut remaining = INDENT_DEPTH.with(Cell::get) * INDENT.len();
    while remaining > 0 {
        let chunk = remaining.min(PADDING.len());
        f.write_str(&PADDING[..chunk])?;
        remaining -= chunk;
    }
    Ok(())
}

/// A line break in `{:#}`, and nothing in `{}`.
pub struct NewLine;

impl Display for NewLine {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        if f.alternate() {
            write_line_break(f)?;
        }
        Ok(())
    }
}

/// A line break in `{:#}`, and a space in `{}`.
pub struct SpaceOrNewline;

impl Display for SpaceOrNewline {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        if f.alternate() {
            write_line_break(f)
        } else {
            f.write_str(" ")
        }
    }
}

/// Renders a value one level further in with `{:#}`, and unchanged with `{}`.
pub struct Indent<T>(pub T);

impl<T: Display> Display for Indent<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        if !f.alternate() {
            return self.0.fmt(f);
        }
        f.write_str(INDENT)?;
        let _scope = IndentScope::enter();
        self.0.fmt(f)
    }
}

/// Renders a value with `{}` even inside `{:#}`, because `write!` resets the flag.
struct OneLine<'s, T>(&'s T);

impl<T: Display> Display for OneLine<'_, T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        self.0.fmt(f)
    }
}

/// Renders `items` separated by commas, one per line with `{:#}`.
pub struct CommaLines<'s, T>(pub &'s [T]);

impl<T: Display> Display for CommaLines<'_, T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        let mut items = self.0.iter();
        if let Some(first) = items.next() {
            first.fmt(f)?;
        }
        for item in items {
            f.write_str(",")?;
            SpaceOrNewline.fmt(f)?;
            item.fmt(f)?;
        }
        Ok(())
    }
}

/// Renders a space or line break, then `items` one per line and indented.
pub fn indented_list<T: Display>(f: &mut Formatter<'_>, items: &[T]) -> Result {
    SpaceOrNewline.fmt(f)?;
    Indent(CommaLines(items)).fmt(f)
}

/// Renders a clause whose keyword and content are on separate lines in `{:#}`.
pub fn write_indented_clause<T: Display>(
    f: &mut Formatter<'_>,
    keyword: &str,
    content: T,
) -> Result {
    SpaceOrNewline.fmt(f)?;
    f.write_str(keyword)?;
    SpaceOrNewline.fmt(f)?;
    Indent(content).fmt(f)
}

/// Renders `keyword` and `items` one per line, or nothing if `items` is empty.
pub fn write_list_clause<T: Display>(f: &mut Formatter<'_>, keyword: &str, items: &[T]) -> Result {
    if items.is_empty() {
        return Ok(());
    }
    SpaceOrNewline.fmt(f)?;
    f.write_str(keyword)?;
    indented_list(f, items)
}

/// Renders `items` joined by `separator`.
pub struct Separated<'s, T>(&'s [T], &'static str);

impl<T: Display> Display for Separated<'_, T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        let mut items = self.0.iter();
        if let Some(first) = items.next() {
            first.fmt(f)?;
        }
        for item in items {
            f.write_str(self.1)?;
            item.fmt(f)?;
        }
        Ok(())
    }
}

pub fn comma<T>(items: &[T]) -> Separated<'_, T> {
    Separated(items, ", ")
}

pub fn space<T>(items: &[T]) -> Separated<'_, T> {
    Separated(items, " ")
}

/// Renders a clause prefixed by `keyword`, or nothing if `items` is empty.
fn write_list<T: Display>(f: &mut Formatter<'_>, keyword: &str, items: &[T]) -> Result {
    if items.is_empty() {
        return Ok(());
    }
    write!(f, " {keyword} {}", comma(items))
}

fn write_where<X: AstExt>(f: &mut Formatter<'_>, selection: &Option<Expr<'_, X>>) -> Result {
    match selection {
        Some(selection) => write!(f, " WHERE {selection}"),
        None => Ok(()),
    }
}

pub fn write_where_clause<X: AstExt>(
    f: &mut Formatter<'_>,
    selection: &Option<Expr<'_, X>>,
) -> Result {
    match selection {
        Some(selection) => write_indented_clause(f, "WHERE", selection),
        None => Ok(()),
    }
}

impl Display for Ident<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match self.quote {
            Some('[') => write!(f, "[{}]", self.text),
            Some(quote) => write!(f, "{quote}{}{quote}", self.text),
            None => f.write_str(&self.text),
        }
    }
}

impl Display for ObjectName<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        Separated(&self.parts, ".").fmt(f)
    }
}

impl Display for Literal<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match self {
            Literal::Number(text) => f.write_str(text),
            Literal::HexString(digits) => write!(f, "X'{digits}'"),
            Literal::DollarQuoted { tag, text } => write!(f, "${tag}${text}${tag}$"),
            Literal::Null => f.write_str("NULL"),
            Literal::Boolean(value) => write!(f, "{value}"),
            Literal::String(string) => string.fmt(f),
        }
    }
}

impl Display for StringLiteral<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        if self.quote == '$' {
            return write!(f, "$${}$$", self.text);
        }
        if self.national {
            f.write_str("N")?;
        }
        match self.prefix {
            StringPrefix::None => {}
            StringPrefix::Byte => f.write_str("B")?,
            StringPrefix::Raw => f.write_str("R")?,
            StringPrefix::Escape => f.write_str("E")?,
            StringPrefix::Unicode => f.write_str("U&")?,
            StringPrefix::Bit => f.write_str("B")?,
        }
        let quote = self.quote;
        if self.triple {
            write!(f, "{quote}{quote}{quote}{}{quote}{quote}{quote}", self.text)
        } else {
            write!(f, "{quote}{}{quote}", self.text)
        }
    }
}

impl Display for BinaryOp<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        f.write_str(match self {
            BinaryOp::Or => "OR",
            BinaryOp::And => "AND",
            BinaryOp::Eq => "=",
            BinaryOp::NotEq => "<>",
            BinaryOp::Lt => "<",
            BinaryOp::LtEq => "<=",
            BinaryOp::Gt => ">",
            BinaryOp::GtEq => ">=",
            BinaryOp::Add => "+",
            BinaryOp::Sub => "-",
            BinaryOp::Mul => "*",
            BinaryOp::Div => "/",
            BinaryOp::Mod => "%",
            BinaryOp::StringConcat => "||",
            BinaryOp::BitOr => "|",
            BinaryOp::BitAnd => "&",
            BinaryOp::BitXor => "^",
            BinaryOp::ShiftLeft => "<<",
            BinaryOp::ShiftRight => ">>",
            BinaryOp::Named(text) => text,
        })
    }
}

fn not(negated: bool) -> &'static str {
    if negated { "NOT " } else { "" }
}

impl<X: AstExt> Display for ExprKind<'_, X> {
    /// Left-deep chains recurse here once per level, up to the tree height
    /// limit. The arms that form those chains avoid `write!`, whose formatter
    /// frames and temporaries would overflow the stack in a debug build.
    ///
    /// Only `CASE` and window specifications have a multi-line form. Anything
    /// else renders its children on one line, so `{:#}` drops to `{}` here, at
    /// the cost of one `write!` per chain rather than per level.
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        if f.alternate() && !matches!(self, ExprKind::Case(_) | ExprKind::Function { .. }) {
            return write!(f, "{}", OneLine(self));
        }
        match self {
            ExprKind::Binary { left, op, right } => {
                (**left).fmt(f)?;
                f.write_str(" ")?;
                op.fmt(f)?;
                f.write_str(" ")?;
                (**right).fmt(f)
            }
            ExprKind::Cast {
                expr,
                ty,
                style: CastStyle::DoubleColon,
            } => {
                (**expr).fmt(f)?;
                f.write_str("::")?;
                ty.fmt(f)
            }
            ExprKind::InList {
                expr,
                list,
                negated,
            } => {
                (**expr).fmt(f)?;
                write_in_list(f, list, *negated)
            }
            ExprKind::InSubquery {
                expr,
                subquery,
                negated,
            } => {
                (**expr).fmt(f)?;
                write_in_subquery(f, subquery, *negated)
            }
            ExprKind::Is {
                expr,
                test,
                negated,
            } => {
                (**expr).fmt(f)?;
                write_is(f, test, *negated)
            }
            ExprKind::Like {
                expr,
                pattern,
                negated,
                kind,
                escape,
            } => {
                (**expr).fmt(f)?;
                write_like(f, pattern, *negated, kind, escape.as_deref())
            }
            ExprKind::Between {
                expr,
                low,
                high,
                negated,
            } => {
                (**expr).fmt(f)?;
                write_between(f, low, high, *negated)
            }
            ExprKind::Ext(ext) => ext.fmt(f),
            other => write_non_chain_expr(f, other),
        }
    }
}

fn write_in_list<X: AstExt>(f: &mut Formatter<'_>, list: &[Expr<'_, X>], negated: bool) -> Result {
    write!(f, " {}IN ({})", not(negated), comma(list))
}

fn write_in_subquery<X: AstExt>(
    f: &mut Formatter<'_>,
    subquery: &Query<'_, X>,
    negated: bool,
) -> Result {
    write!(f, " {}IN ({subquery})", not(negated))
}

/// Renders the expressions that cannot form long left-deep chains.
fn write_non_chain_expr<X: AstExt>(f: &mut Formatter<'_>, expr: &ExprKind<'_, X>) -> Result {
    match expr {
        ExprKind::Ident(ident) => ident.fmt(f),
        ExprKind::Literal(literal) => literal.fmt(f),
        ExprKind::Nested(expr) => write!(f, "({expr})"),
        ExprKind::Unary {
            op: UnaryOp::Not,
            expr,
        } => write!(f, "NOT {expr}"),
        ExprKind::Unary {
            op: UnaryOp::BangNot,
            expr,
        } => write!(f, "!{expr}"),
        ExprKind::Unary {
            op: UnaryOp::BitNot,
            expr,
        } => write!(f, "~ {expr}"),
        ExprKind::Unary {
            op: UnaryOp::Neg,
            expr,
        } => {
            // `--a` would read as the start of a comment.
            let is_negation = matches!(
                &expr.kind,
                ExprKind::Unary {
                    op: UnaryOp::Neg,
                    ..
                }
            );
            let separator = if is_negation { " " } else { "" };
            write!(f, "-{separator}{expr}")
        }
        ExprKind::Unary {
            op: UnaryOp::Plus,
            expr,
        } => write!(f, "+{expr}"),
        ExprKind::Cast {
            expr,
            ty,
            style: CastStyle::TryFunction,
        } => write!(f, "TRY_CAST({expr} AS {ty})"),
        ExprKind::Cast { expr, ty, .. } if ty.is_unspecified() => write!(f, "CAST({expr} AS)"),
        ExprKind::Cast {
            expr,
            ty,
            style: CastStyle::SafeFunction,
        } => write!(f, "SAFE_CAST({expr} AS {ty})"),
        ExprKind::Cast { expr, ty, .. } => write!(f, "CAST({expr} AS {ty})"),
        ExprKind::Placeholder(text) => f.write_str(text),
        ExprKind::Interval(interval) => interval.fmt(f),
        ExprKind::TypedString(typed) => {
            write!(f, "{} {}", typed.ty, typed.value)
        }
        ExprKind::Special(special) => special.fmt(f),
        ExprKind::Compound(name) => name.fmt(f),
        ExprKind::Function {
            name,
            args,
            options,
        } => write_function(f, name, args, options.as_deref()),
        ExprKind::Subquery(query) => write!(f, "({query})"),
        ExprKind::Exists(query) => write!(f, "EXISTS ({query})"),
        ExprKind::Case(case) => write_case(f, case),
        ExprKind::Binary { .. }
        | ExprKind::InList { .. }
        | ExprKind::InSubquery { .. }
        | ExprKind::Is { .. }
        | ExprKind::Like { .. }
        | ExprKind::Between { .. }
        | ExprKind::Ext(_) => {
            unreachable!("rendered by Expr::fmt")
        }
    }
}

impl<X: AstExt> Display for FunctionArg<'_, X> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match self {
            FunctionArg::Wildcard => f.write_str("*"),
            FunctionArg::QualifiedWildcard(name) => write!(f, "{name}.*"),
            FunctionArg::Expr(expr) => expr.fmt(f),
            FunctionArg::Named { name, value } => write!(f, "{name} => {value}"),
            FunctionArg::NamedWith {
                name,
                operator,
                value,
            } => write!(f, "{name} {operator} {value}"),
        }
    }
}

impl<X: AstExt> Display for SelectItemKind<'_, X> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match self {
            SelectItemKind::Wildcard => f.write_str("*"),
            SelectItemKind::QualifiedWildcard(name) => write!(f, "{name}.*"),
            SelectItemKind::WildcardWithOptions { qualifier, options } => {
                if let Some(qualifier) = qualifier {
                    write!(f, "{qualifier}.")?;
                }
                write!(f, "*{options}")
            }
            SelectItemKind::Expr { expr, alias: None } => expr.fmt(f),
            SelectItemKind::ExprWithAliases { expr, aliases } => {
                write!(f, "{expr} AS ({})", comma(aliases))
            }
            SelectItemKind::Expr {
                expr,
                alias: Some(alias),
            } => {
                expr.fmt(f)?;
                f.write_str(" AS ")?;
                alias.fmt(f)
            }
        }
    }
}

impl<X: AstExt> Display for WildcardOptions<'_, X> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        if let Some(ref pattern) = self.ilike {
            write!(f, " ILIKE {}", pattern)?;
        }
        if !self.exclude.is_empty() {
            if self.except_keyword {
                write!(f, " EXCEPT ({})", comma(&self.exclude))?;
            } else if self.exclude_parens {
                write!(f, " EXCLUDE ({})", comma(&self.exclude))?;
            } else {
                write!(f, " EXCLUDE {}", comma(&self.exclude))?;
            }
        }
        if !self.replace.is_empty() {
            f.write_str(" REPLACE (")?;
            for (i, (expr, name)) in self.replace.iter().enumerate() {
                if i > 0 {
                    f.write_str(", ")?;
                }
                write!(f, "{expr} AS {name}")?;
            }
            f.write_str(")")?;
        }
        if !self.rename.is_empty() {
            f.write_str(if self.rename_parens {
                " RENAME ("
            } else {
                " RENAME "
            })?;
            for (i, (from, to)) in self.rename.iter().enumerate() {
                if i > 0 {
                    f.write_str(", ")?;
                }
                write!(f, "{from} AS {to}")?;
            }
            if self.rename_parens {
                f.write_str(")")?;
            }
        }
        Ok(())
    }
}

impl<X: AstExt> Display for TableFactor<'_, X> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write_table_call(f, self)
    }
}

impl<X: AstExt> Display for TableRef<'_, X> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match &self.factor {
            TableFactor::Named(name) => name.fmt(f)?,
            TableFactor::Derived(query) => write_derived(f, query)?,
            TableFactor::Values(rows) => write_table_values(f, rows)?,
            TableFactor::NestedJoin(table) => write!(f, "({table})")?,
            TableFactor::Ext(factor) => factor.fmt(f)?,
            factor @ (TableFactor::Function { .. }
            | TableFactor::Lateral(_)
            | TableFactor::Versioned { .. }
            | TableFactor::TimeTravel(_)
            | TableFactor::Sampled(_)
            | TableFactor::XmlTable(_)
            | TableFactor::Pivot(_)
            | TableFactor::Unpivot(_)
            | TableFactor::MatchRecognize(_)) => {
                write_table_call(f, factor)?;
            }
        }
        let Some(alias) = &self.alias else {
            return Ok(());
        };
        f.write_str(if alias.explicit { " AS " } else { " " })?;
        alias.name.fmt(f)?;
        if !alias.columns.is_empty() {
            f.write_str(" (")?;
            for (i, column) in alias.columns.iter().enumerate() {
                if i > 0 {
                    f.write_str(", ")?;
                }
                column.fmt(f)?;
                if let Some(data_type) = alias.column_types.get(i) {
                    write!(f, " {data_type}")?;
                }
            }
            f.write_str(")")?;
        }
        Ok(())
    }
}

/// Renders a query in parentheses, on its own indented lines in `{:#}`.
fn write_derived<X: AstExt>(f: &mut Formatter<'_>, query: &Query<'_, X>) -> Result {
    f.write_str("(")?;
    NewLine.fmt(f)?;
    Indent(query).fmt(f)?;
    NewLine.fmt(f)?;
    f.write_str(")")
}

/// Renders the table factors that can follow `LATERAL`.
fn write_table_call<X: AstExt>(f: &mut Formatter<'_>, factor: &TableFactor<'_, X>) -> Result {
    match factor {
        TableFactor::Named(name) => name.fmt(f),
        TableFactor::Derived(query) => write_derived(f, query),
        TableFactor::Values(rows) => write_table_values(f, rows),
        TableFactor::NestedJoin(table) => write!(f, "({table})"),
        TableFactor::Ext(factor) => factor.fmt(f),
        TableFactor::Function {
            call,
            with_ordinality,
        } => {
            call.fmt(f)?;
            f.write_str(if *with_ordinality {
                " WITH ORDINALITY"
            } else {
                ""
            })
        }
        TableFactor::Lateral(inner) => {
            f.write_str("LATERAL ")?;
            write_table_call(f, inner)
        }
        TableFactor::TimeTravel(travel) => {
            write!(f, "{} {} AS OF {}", travel.name, travel.kind, travel.at)
        }
        TableFactor::Versioned { name, calls } => {
            name.fmt(f)?;
            for call in calls {
                write!(f, " {call}")?;
            }
            Ok(())
        }
        TableFactor::Sampled(sample) => sample.fmt(f),
        TableFactor::XmlTable(table) => table.fmt(f),
        TableFactor::Pivot(pivot) => pivot.fmt(f),
        TableFactor::Unpivot(unpivot) => unpivot.fmt(f),
        TableFactor::MatchRecognize(recognize) => recognize.fmt(f),
    }
}

impl<X: AstExt> Display for MatchRecognize<'_, X> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(f, "{} MATCH_RECOGNIZE(", self.table)?;
        if !self.partition_by.is_empty() {
            write!(f, "PARTITION BY {} ", comma(&self.partition_by))?;
        }
        if !self.order_by.is_empty() {
            write!(f, "ORDER BY {} ", comma(&self.order_by))?;
        }
        if !self.measures.is_empty() {
            f.write_str("MEASURES ")?;
            for (i, (expr, alias)) in self.measures.iter().enumerate() {
                if i > 0 {
                    f.write_str(", ")?;
                }
                write!(f, "{expr} AS {alias}")?;
            }
            f.write_str(" ")?;
        }
        match self.rows_per_match {
            Some(RowsPerMatch::OneRow) => f.write_str("ONE ROW PER MATCH ")?,
            Some(RowsPerMatch::AllRows(empty)) => {
                f.write_str("ALL ROWS PER MATCH ")?;
                f.write_str(match empty {
                    Some(EmptyMatches::Show) => "SHOW EMPTY MATCHES ",
                    Some(EmptyMatches::Omit) => "OMIT EMPTY MATCHES ",
                    Some(EmptyMatches::WithUnmatchedRows) => "WITH UNMATCHED ROWS ",
                    None => "",
                })?;
            }
            None => {}
        }
        match self.after_match_skip {
            Some(AfterMatchSkip::PastLastRow) => f.write_str("AFTER MATCH SKIP PAST LAST ROW ")?,
            Some(AfterMatchSkip::ToNextRow) => f.write_str("AFTER MATCH SKIP TO NEXT ROW ")?,
            Some(AfterMatchSkip::ToFirst(ref symbol)) => {
                write!(f, "AFTER MATCH SKIP TO FIRST {symbol} ")?;
            }
            Some(AfterMatchSkip::ToLast(ref symbol)) => {
                write!(f, "AFTER MATCH SKIP TO LAST {symbol} ")?;
            }
            None => {}
        }
        write!(f, "PATTERN ({}) DEFINE ", self.pattern)?;
        for (i, (symbol, definition)) in self.symbols.iter().enumerate() {
            if i > 0 {
                f.write_str(", ")?;
            }
            write!(f, "{symbol} AS {definition}")?;
        }
        f.write_str(")")
    }
}

impl Display for MatchPattern<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match self {
            MatchPattern::Symbol(symbol) => symbol.fmt(f),
            MatchPattern::Start => f.write_str("^"),
            MatchPattern::End => f.write_str("$"),
            MatchPattern::Exclude(symbol) => write!(f, "{{- {symbol} -}}"),
            MatchPattern::Permute(symbols) => write!(f, "PERMUTE({})", comma(symbols)),
            MatchPattern::Concat(patterns) => Separated(patterns, " ").fmt(f),
            MatchPattern::Group(pattern) => write!(f, "( {pattern} )"),
            MatchPattern::Alternation(patterns) => Separated(patterns, " | ").fmt(f),
            MatchPattern::Repetition {
                pattern,
                quantifier,
                reluctant,
            } => {
                pattern.fmt(f)?;
                match quantifier {
                    PatternQuantifier::ZeroOrMore => f.write_str("*")?,
                    PatternQuantifier::OneOrMore => f.write_str("+")?,
                    PatternQuantifier::AtMostOne => f.write_str("?")?,
                    PatternQuantifier::Exactly(n) => write!(f, "{{{n}}}")?,
                    PatternQuantifier::AtLeast(n) => write!(f, "{{{n},}}")?,
                    PatternQuantifier::AtMost(m) => write!(f, "{{,{m}}}")?,
                    PatternQuantifier::Range(n, m) => write!(f, "{{{n},{m}}}")?,
                }
                f.write_str(if *reluctant { "?" } else { "" })
            }
        }
    }
}

impl<X: AstExt> Display for ExprWithAlias<'_, X> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match &self.alias {
            Some(alias) => write!(f, "{} AS {alias}", self.expr),
            None => self.expr.fmt(f),
        }
    }
}

impl<X: AstExt> Display for TableSample<'_, X> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(f, "{} {}", self.table, self.sample)
    }
}

impl<X: AstExt> Display for SampleClause<'_, X> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        let keyword = if self.long_form {
            "TABLESAMPLE"
        } else {
            "SAMPLE"
        };
        write!(f, "{keyword} ")?;
        if let Some(ref method) = self.method {
            write!(f, "{method} ")?;
        }
        if self.parenthesized {
            f.write_str("(")?;
        }
        self.amount.fmt(f)?;
        if self.parenthesized {
            f.write_str(")")?;
        }
        if let Some(ref seed) = self.seed {
            let keyword = if seed.repeatable {
                "REPEATABLE"
            } else {
                "SEED"
            };
            write!(f, " {keyword} ({})", seed.value)?;
        }
        if let Some(offset) = &self.offset {
            write!(f, " OFFSET {offset}")?;
        }
        Ok(())
    }
}

impl<X: AstExt> Display for SampleAmount<'_, X> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match self {
            SampleAmount::Quantity { value, unit } => {
                value.fmt(f)?;
                f.write_str(match unit {
                    Some(SampleUnit::Rows) => " ROWS",
                    Some(SampleUnit::Percent) => " PERCENT",
                    None => "",
                })
            }
            SampleAmount::Bucket { bucket, total, on } => {
                write!(f, "BUCKET {bucket} OUT OF {total}")?;
                match on {
                    Some(on) => write!(f, " ON {on}"),
                    None => Ok(()),
                }
            }
        }
    }
}

impl<X: AstExt> Display for Pivot<'_, X> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(f, "{} {}", self.table, self.clause)
    }
}

impl<X: AstExt> Display for PivotClause<'_, X> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(f, "PIVOT({} FOR ", comma(&self.aggregates))?;
        match self.columns.as_slice() {
            [only] => only.fmt(f)?,
            columns => write!(f, "({})", comma(columns))?,
        }
        f.write_str(" IN (")?;
        match &self.source {
            PivotSource::List(values) => comma(values).fmt(f)?,
            PivotSource::Any(order_by) => {
                f.write_str("ANY")?;
                write_list(f, "ORDER BY", order_by)?;
            }
            PivotSource::Query(query) => query.fmt(f)?,
        }
        f.write_str(")")?;
        if let Some(default) = &self.default_on_null {
            write!(f, " DEFAULT ON NULL ({default})")?;
        }
        f.write_str(")")
    }
}

impl<X: AstExt> Display for Unpivot<'_, X> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(f, "{} {}", self.table, self.clause)
    }
}

impl<X: AstExt> Display for UnpivotClause<'_, X> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        f.write_str("UNPIVOT")?;
        f.write_str(match self.include_nulls {
            Some(true) => " INCLUDE NULLS ",
            Some(false) => " EXCLUDE NULLS ",
            None => "",
        })?;
        write!(
            f,
            "({} FOR {} IN ({}))",
            self.value,
            self.name,
            comma(&self.columns)
        )
    }
}

impl<X: AstExt> Display for Join<'_, X> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        if matches!(self.constraint, JoinConstraint::Natural) {
            f.write_str("NATURAL ")?;
        }
        f.write_str(match (&self.kind, self.spelled_out) {
            (JoinKind::Custom(keywords), _) => keywords,
            (JoinKind::Inner, false) => "JOIN",
            (JoinKind::Inner, true) => "INNER JOIN",
            (JoinKind::Left, false) => "LEFT JOIN",
            (JoinKind::Left, true) => "LEFT OUTER JOIN",
            (JoinKind::Right, false) => "RIGHT JOIN",
            (JoinKind::Right, true) => "RIGHT OUTER JOIN",
            (JoinKind::Full, _) => "FULL JOIN",
            (JoinKind::Cross, _) => "CROSS JOIN",
            (JoinKind::AsOf, _) => "ASOF JOIN",
            (JoinKind::Semi, _) => "SEMI JOIN",
            (JoinKind::LeftSemi, _) => "LEFT SEMI JOIN",
            (JoinKind::Anti, _) => "ANTI JOIN",
            (JoinKind::LeftAnti, _) => "LEFT ANTI JOIN",
            (JoinKind::RightSemi, _) => "RIGHT SEMI JOIN",
            (JoinKind::RightAnti, _) => "RIGHT ANTI JOIN",
        })?;
        write!(f, " {}", self.table)?;
        match &self.constraint {
            JoinConstraint::Ext(constraint) => write!(f, " {constraint}"),
            JoinConstraint::None | JoinConstraint::Natural => Ok(()),
            JoinConstraint::On(expr) => write!(f, " ON {expr}"),
            JoinConstraint::Using(columns) => write!(f, " USING({})", comma(columns)),
            JoinConstraint::AsOf {
                condition,
                constraint,
            } => {
                write!(f, " MATCH_CONDITION ({condition})")?;
                match constraint.as_ref() {
                    JoinConstraint::On(on) => write!(f, " ON {on}"),
                    JoinConstraint::Using(columns) => write!(f, " USING({})", comma(columns)),
                    _ => Ok(()),
                }
            }
        }
    }
}

impl<X: AstExt> Display for TableWithJoins<'_, X> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        self.relation.fmt(f)?;
        for join in &self.joins {
            SpaceOrNewline.fmt(f)?;
            join.fmt(f)?;
        }
        Ok(())
    }
}

impl<X: AstExt> Display for Cte<'_, X> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        let materialized = match self.materialized {
            Some(true) => "MATERIALIZED ",
            Some(false) => "NOT MATERIALIZED ",
            None => "",
        };
        self.name.fmt(f)?;
        if !self.columns.is_empty() {
            write!(f, " ({})", comma(&self.columns))?;
        }
        write!(f, " AS {materialized}(")?;
        NewLine.fmt(f)?;
        Indent(&self.query).fmt(f)?;
        NewLine.fmt(f)?;
        f.write_str(")")
    }
}

impl<X: AstExt> Display for ListAggOnOverflow<'_, X> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        f.write_str("ON OVERFLOW ")?;
        match self {
            ListAggOnOverflow::Error => f.write_str("ERROR"),
            ListAggOnOverflow::Truncate { filler, with_count } => {
                f.write_str("TRUNCATE ")?;
                if let Some(filler) = filler {
                    write!(f, "{filler} ")?;
                }
                f.write_str(if *with_count {
                    "WITH COUNT"
                } else {
                    "WITHOUT COUNT"
                })
            }
        }
    }
}

impl<X: AstExt> Display for SelectInto<'_, X> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        f.write_str("INTO ")?;
        if let Some(ref modifier) = self.modifier {
            write!(f, "{modifier} ")?;
        }
        if self.table_keyword {
            f.write_str("TABLE ")?;
        }
        write!(f, "{}", comma(&self.targets))
    }
}

impl<X: AstExt> Display for XmlTable<'_, X> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        f.write_str("XMLTABLE(")?;
        if !self.namespaces.is_empty() {
            write!(f, "XMLNAMESPACES({}), ", comma(&self.namespaces))?;
        }
        write!(f, "{}", self.row_expression)?;
        if !self.passing.is_empty() {
            write!(f, " PASSING {}", comma(&self.passing))?;
        }
        write!(f, " COLUMNS {})", comma(&self.columns))
    }
}

impl<X: AstExt> Display for XmlNamespace<'_, X> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(f, "{} AS {}", self.uri, self.name)
    }
}

impl<X: AstExt> Display for XmlPassingArgument<'_, X> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        if self.by_value {
            f.write_str("BY VALUE ")?;
        }
        write!(f, "{}", self.expr)?;
        if let Some(alias) = &self.alias {
            write!(f, " AS {alias}")?;
        }
        Ok(())
    }
}

impl<X: AstExt> Display for XmlTableColumn<'_, X> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        self.name.fmt(f)?;
        match &self.kind {
            XmlColumnKind::ForOrdinality => f.write_str(" FOR ORDINALITY"),
            XmlColumnKind::Data {
                data_type,
                path,
                default,
                not_null,
            } => {
                write!(f, " {data_type}")?;
                if let Some(path) = path {
                    write!(f, " PATH {path}")?;
                }
                if let Some(default) = default {
                    write!(f, " DEFAULT {default}")?;
                }
                if *not_null {
                    f.write_str(" NOT NULL")?;
                }
                Ok(())
            }
        }
    }
}

impl<X: AstExt> Display for OrderByItem<'_, X> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        self.expr.fmt(f)?;
        if let Some(using) = &self.using {
            f.write_str(" USING ")?;
            write_order_operator(f, using)?;
        }
        write_order_modifiers(f, self)?;
        self.ext.fmt(f)
    }
}

fn write_order_operator(f: &mut Formatter<'_>, name: &OperatorName<'_>) -> Result {
    if name.schema.is_empty() && !name.operator.contains(|c: char| c.is_alphabetic()) {
        return f.write_str(&name.operator);
    }
    f.write_str("OPERATOR(")?;
    for part in &name.schema {
        write!(f, "{part}.")?;
    }
    write!(f, "{})", name.operator)
}

fn write_order_modifiers<X: AstExt>(f: &mut Formatter<'_>, item: &OrderByItem<'_, X>) -> Result {
    match item.asc {
        Some(true) => f.write_str(" ASC")?,
        Some(false) => f.write_str(" DESC")?,
        None => {}
    }
    match item.nulls_first {
        Some(true) => f.write_str(" NULLS FIRST"),
        Some(false) => f.write_str(" NULLS LAST"),
        None => Ok(()),
    }
}

impl<X: AstExt> Display for Limit<'_, X> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match (&self.count, &self.offset) {
            (Some(count), Some(offset)) if self.offset_comma => {
                return write!(f, "LIMIT {offset}, {count}");
            }
            (Some(count), Some(_)) => write!(f, "LIMIT {count} ")?,
            (Some(count), None) => write!(f, "LIMIT {count}")?,
            (None, Some(_)) if self.all => f.write_str("LIMIT ALL ")?,
            (None, None) if self.all => f.write_str("LIMIT ALL")?,
            (None, _) => {}
        }
        if let Some(offset) = &self.offset {
            write!(f, "OFFSET {offset}")?;
            f.write_str(match self.offset_rows {
                OffsetRows::None => "",
                OffsetRows::Row => " ROW",
                OffsetRows::Rows => " ROWS",
            })?;
        }
        write_list(f, "BY", &self.by)
    }
}

impl<X: AstExt> Display for Fetch<'_, X> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        f.write_str("FETCH FIRST ")?;
        if let Some(quantity) = &self.quantity {
            write!(f, "{quantity} ")?;
            if self.percent {
                f.write_str("PERCENT ")?;
            }
        }
        f.write_str(if self.with_ties {
            "ROWS WITH TIES"
        } else {
            "ROWS ONLY"
        })
    }
}

impl Display for LockClause<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        f.write_str(match self.lock_type {
            LockType::Update => "FOR UPDATE",
            LockType::Share => "FOR SHARE",
        })?;
        if let Some(of) = &self.of {
            write!(f, " OF {of}")?;
        }
        match self.nonblock {
            Some(NonBlock::Nowait) => f.write_str(" NOWAIT"),
            Some(NonBlock::SkipLocked) => f.write_str(" SKIP LOCKED"),
            None => Ok(()),
        }
    }
}

/// Renders a date or time unit in upper case, or as written if it is quoted
/// or not a known unit.
fn write_unit(f: &mut Formatter<'_>, unit: &str) -> Result {
    if unit.starts_with('\'') || !crate::grammar::is_date_time_field(unit) {
        f.write_str(unit)
    } else {
        f.write_str(&unit.to_ascii_uppercase())
    }
}

impl<X: AstExt> Display for Interval<'_, X> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(f, "INTERVAL {}", self.value)?;
        if let Some(ref field) = self.leading_field {
            f.write_str(" ")?;
            write_unit(f, field)?;
        }
        let is_second = self
            .leading_field
            .as_ref()
            .is_some_and(|field| field.eq_ignore_ascii_case("SECOND"));
        if let (true, Some(leading), Some(fractional)) = (
            is_second,
            self.leading_precision.clone(),
            self.fractional_seconds_precision.clone(),
        ) {
            return write!(f, " ({leading}, {fractional})");
        }
        if let Some(ref precision) = self.leading_precision {
            write!(f, " ({precision})")?;
        }
        if let Some(ref field) = self.last_field {
            f.write_str(" TO ")?;
            write_unit(f, field)?;
        }
        if let Some(ref precision) = self.fractional_seconds_precision {
            write!(f, " ({precision})")?;
        }
        Ok(())
    }
}

impl<X: AstExt> Display for SpecialExpr<'_, X> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match self {
            SpecialExpr::Quantified {
                left,
                op,
                quantifier,
                right,
            } => {
                left.fmt(f)?;
                f.write_str(" ")?;
                op.fmt(f)?;
                f.write_str(match quantifier {
                    Quantifier::Any => " ANY",
                    Quantifier::Some => " SOME",
                    Quantifier::All => " ALL",
                })?;
                // A subquery renders its own parentheses.
                if matches!(right.kind, ExprKind::Subquery(_)) {
                    right.fmt(f)
                } else {
                    f.write_str("(")?;
                    right.fmt(f)?;
                    f.write_str(")")
                }
            }
            SpecialExpr::FieldAccess { expr, field } => {
                expr.fmt(f)?;
                f.write_str(".")?;
                field.fmt(f)
            }
            SpecialExpr::AtTimeZone { expr, zone } => {
                expr.fmt(f)?;
                f.write_str(" AT TIME ZONE ")?;
                zone.fmt(f)
            }
            SpecialExpr::Collate { expr, collation } => {
                expr.fmt(f)?;
                f.write_str(" COLLATE ")?;
                collation.fmt(f)
            }
            SpecialExpr::Subscript { expr, index } => {
                expr.fmt(f)?;
                f.write_str("[")?;
                index.fmt(f)?;
                f.write_str("]")
            }
            SpecialExpr::Slice {
                expr,
                lower,
                upper,
                step,
            } => {
                expr.fmt(f)?;
                f.write_str("[")?;
                if let Some(lower) = lower {
                    lower.fmt(f)?;
                }
                f.write_str(":")?;
                if let Some(upper) = upper {
                    upper.fmt(f)?;
                }
                if let Some(step) = step {
                    write!(f, ":{step}")?;
                }
                f.write_str("]")
            }
            SpecialExpr::OuterJoinMark(expr) => write!(f, "{expr} (+)"),
            SpecialExpr::QuantifiedList { quantifier, items } => {
                let quantifier = match quantifier {
                    Quantifier::Any => "ANY",
                    Quantifier::Some => "SOME",
                    Quantifier::All => "ALL",
                };
                write!(f, "{quantifier} ({})", comma(items))
            }
            SpecialExpr::OdbcEscape { kind, expr } => write!(f, "{{{kind} {expr}}}"),
            SpecialExpr::Prior(expr) => write!(f, "PRIOR {expr}"),
            SpecialExpr::Lambda {
                params,
                parenthesized,
                body,
            } => {
                if *parenthesized {
                    write!(f, "({}) -> {body}", comma(params))
                } else {
                    write!(f, "{} -> {body}", comma(params))
                }
            }
            SpecialExpr::ConnectByRoot(expr) => write!(f, "CONNECT_BY_ROOT {expr}"),
            SpecialExpr::Array(items) => write!(f, "ARRAY[{}]", comma(items)),
            SpecialExpr::Tuple(items) => write!(f, "({})", comma(items)),
            SpecialExpr::Extract { field, expr } => {
                f.write_str("EXTRACT(")?;
                write_unit(f, field)?;
                write!(f, " FROM {expr})")
            }
            SpecialExpr::Round { ceil, expr, field } => {
                let name = if *ceil { "CEIL" } else { "FLOOR" };
                write!(f, "{name}({expr} TO ")?;
                write_unit(f, field)?;
                f.write_str(")")
            }
            SpecialExpr::Niladic(name) => name.fmt(f),
            SpecialExpr::Position { needle, haystack } => {
                write!(f, "POSITION({needle} IN {haystack})")
            }
            SpecialExpr::Substring {
                shorthand,
                expr,
                from,
                length,
            } => {
                let name = if *shorthand { "SUBSTR" } else { "SUBSTRING" };
                write!(f, "{name}({expr}")?;
                if let Some(from) = from {
                    write!(f, " FROM {from}")?;
                }
                if let Some(length) = length {
                    write!(f, " FOR {length}")?;
                }
                f.write_str(")")
            }
            SpecialExpr::Trim { side, what, expr } => {
                f.write_str("TRIM(")?;
                f.write_str(match side {
                    Some(TrimSide::Both) => "BOTH ",
                    Some(TrimSide::Leading) => "LEADING ",
                    Some(TrimSide::Trailing) => "TRAILING ",
                    None => "",
                })?;
                if let Some(what) = what {
                    write!(f, "{what} FROM ")?;
                }
                write!(f, "{expr})")
            }
            SpecialExpr::Grouping { kind, sets } => {
                f.write_str(match kind {
                    GroupingKind::Rollup => "ROLLUP (",
                    GroupingKind::Cube => "CUBE (",
                    GroupingKind::Sets => "GROUPING SETS (",
                })?;
                for (i, set) in sets.iter().enumerate() {
                    if i > 0 {
                        f.write_str(", ")?;
                    }
                    match set.as_slice() {
                        [only] if *kind != GroupingKind::Sets => only.fmt(f)?,
                        _ => write!(f, "({})", comma(set))?,
                    }
                }
                f.write_str(")")
            }
            SpecialExpr::Overlay {
                expr,
                what,
                from,
                length,
            } => {
                write!(f, "OVERLAY({expr} PLACING {what} FROM {from}")?;
                if let Some(length) = length {
                    write!(f, " FOR {length}")?;
                }
                f.write_str(")")
            }
        }
    }
}

impl<X: AstExt> Display for Query<'_, X> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        if let Some(with) = &self.with {
            f.write_str(if with.recursive {
                "WITH RECURSIVE "
            } else {
                "WITH "
            })?;
            comma(&with.ctes).fmt(f)?;
            SpaceOrNewline.fmt(f)?;
        }
        self.body.fmt(f)?;
        write_list(f, "ORDER BY", &self.order_by)?;
        if let Some(distribution) = &self.distribution {
            if !distribution.cluster_by.is_empty() {
                write_indented_clause(f, "CLUSTER BY", comma(&distribution.cluster_by))?;
            }
            if !distribution.distribute_by.is_empty() {
                SpaceOrNewline.fmt(f)?;
                f.write_str("DISTRIBUTE BY")?;
                SpaceOrNewline.fmt(f)?;
                comma(&distribution.distribute_by).fmt(f)?;
            }
            if !distribution.sort_by.is_empty() {
                write_indented_clause(f, "SORT BY", comma(&distribution.sort_by))?;
            }
        }
        if let Some(limit) = &self.limit {
            write!(f, " {limit}")?;
        }
        if let Some(fetch) = &self.fetch {
            write!(f, " {fetch}")?;
        }
        for lock in &self.locks {
            write!(f, " {lock}")?;
        }
        self.tail.fmt(f)?;
        for pipe in &self.pipes {
            write!(f, " |> {pipe}")?;
        }
        Ok(())
    }
}

fn write_optional_as(f: &mut Formatter<'_>, alias: &Option<Ident<'_>>) -> Result {
    match alias {
        Some(alias) => write!(f, " AS {alias}"),
        None => Ok(()),
    }
}

impl<X: AstExt> Display for PipeOperator<'_, X> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match self {
            PipeOperator::Select(items) => write!(f, "SELECT {}", comma(items)),
            PipeOperator::Extend(items) => write!(f, "EXTEND {}", comma(items)),
            PipeOperator::Set(assignments) => write!(f, "SET {}", comma(assignments)),
            PipeOperator::Drop(columns) => write!(f, "DROP {}", comma(columns)),
            PipeOperator::As(alias) => write!(f, "AS {alias}"),
            PipeOperator::Where(condition) => write!(f, "WHERE {condition}"),
            PipeOperator::Limit { count, offset } => {
                write!(f, "LIMIT {count}")?;
                match offset {
                    Some(offset) => write!(f, " OFFSET {offset}"),
                    None => Ok(()),
                }
            }
            PipeOperator::OrderBy(items) => write!(f, "ORDER BY {}", comma(items)),
            PipeOperator::Aggregate {
                aggregates,
                group_by,
            } => {
                f.write_str("AGGREGATE")?;
                if !aggregates.is_empty() {
                    write!(f, " {}", comma(aggregates))?;
                }
                write_list(f, "GROUP BY", group_by)
            }
            PipeOperator::TableSample(sample) => sample.fmt(f),
            PipeOperator::Rename(renames) => write!(f, "RENAME {}", comma(renames)),
            PipeOperator::SetOperation {
                op,
                quantifier,
                by_name,
                queries,
            } => {
                f.write_str(match op {
                    SetOperator::Union => "UNION",
                    SetOperator::Except => "EXCEPT",
                    SetOperator::Intersect => "INTERSECT",
                    SetOperator::Minus => "MINUS",
                })?;
                f.write_str(match quantifier {
                    SetQuantifier::None => "",
                    SetQuantifier::All => " ALL",
                    SetQuantifier::Distinct => " DISTINCT",
                })?;
                if *by_name {
                    f.write_str(" BY NAME")?;
                }
                let mut separator = " ";
                for query in queries {
                    write!(f, "{separator}({query})")?;
                    separator = ", ";
                }
                Ok(())
            }
            PipeOperator::Call { function, alias } => {
                write!(f, "CALL {function}")?;
                write_optional_as(f, alias)
            }
            PipeOperator::Pivot { clause, alias } => {
                clause.fmt(f)?;
                write_optional_as(f, alias)
            }
            PipeOperator::Unpivot { clause, alias } => {
                clause.fmt(f)?;
                write_optional_as(f, alias)
            }
            PipeOperator::Join(join) => join.fmt(f),
        }
    }
}

impl<X: AstExt> Display for PipeAggregate<'_, X> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        self.expr.fmt(f)?;
        f.write_str(match self.asc {
            Some(true) => " ASC",
            Some(false) => " DESC",
            None => "",
        })
    }
}

impl Display for PipeRename<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(f, "{} AS {}", self.name, self.new_name)
    }
}

impl<X: AstExt> Display for SetExpr<'_, X> {
    /// Long `UNION` chains recurse here, so this avoids `write!` as
    /// [`Expr`]'s rendering does.
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match self {
            SetExpr::Select(select) => select.fmt(f),
            SetExpr::Query(query) => {
                f.write_str("(")?;
                query.fmt(f)?;
                f.write_str(")")
            }
            SetExpr::Statement(statement) => statement.fmt(f),
            SetExpr::Table(name) => write!(f, "TABLE {name}"),
            SetExpr::Values(rows) => write_values(f, rows, ""),
            SetExpr::RowValues(rows) => write_values(f, rows, "ROW"),
            SetExpr::SetOperation {
                left,
                op,
                quantifier,
                by_name,
                right,
            } => {
                (**left).fmt(f)?;
                SpaceOrNewline.fmt(f)?;
                f.write_str(match op {
                    SetOperator::Union => "UNION",
                    SetOperator::Except => "EXCEPT",
                    SetOperator::Intersect => "INTERSECT",
                    SetOperator::Minus => "MINUS",
                })?;
                f.write_str(match quantifier {
                    SetQuantifier::None => "",
                    SetQuantifier::All => " ALL",
                    SetQuantifier::Distinct => " DISTINCT",
                })?;
                if *by_name {
                    f.write_str(" BY NAME")?;
                }
                SpaceOrNewline.fmt(f)?;
                (**right).fmt(f)
            }
        }
    }
}

pub fn write_rows<X: AstExt>(f: &mut Formatter<'_>, rows: &[Vec<Expr<'_, X>>]) -> Result {
    for (i, row) in rows.iter().enumerate() {
        if i > 0 {
            f.write_str(", ")?;
        }
        write!(f, "({})", comma(row))?;
    }
    Ok(())
}

/// Renders `VALUES` and rows that each start with `prefix`, one per line in `{:#}`.
fn write_values<X: AstExt>(
    f: &mut Formatter<'_>,
    rows: &[Vec<Expr<'_, X>>],
    prefix: &str,
) -> Result {
    f.write_str("VALUES")?;
    for (i, row) in rows.iter().enumerate() {
        if i > 0 {
            f.write_str(",")?;
        }
        SpaceOrNewline.fmt(f)?;
        Indent(format_args!("{prefix}({})", comma(row))).fmt(f)?;
    }
    Ok(())
}

impl<X: AstExt> Select<'_, X> {
    fn write_head(&self, f: &mut Formatter<'_>) -> Result {
        if self.projection.is_empty() {
            return f.write_str("SELECT");
        }
        let quantifier = quantifier_prefix(self.quantifier);
        if f.alternate() {
            let prefix = if X::modifiers_follow_quantifier(&self.modifiers) {
                format!("{quantifier}{}", self.modifiers)
            } else {
                format!("{}{quantifier}", self.modifiers)
            };
            f.write_str("SELECT")?;
            let prefix = prefix.trim_end();
            if !prefix.is_empty() {
                f.write_str(" ")?;
                f.write_str(prefix)?;
            }
            return indented_list(f, &self.projection);
        }
        if X::modifiers_follow_quantifier(&self.modifiers) {
            write!(f, "SELECT {quantifier}{}", self.modifiers)?;
        } else {
            write!(f, "SELECT {}{quantifier}", self.modifiers)?;
        }
        comma(&self.projection).fmt(f)
    }
}

impl Display for GroupByModifier {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match self {
            GroupByModifier::Rollup => f.write_str(" WITH ROLLUP"),
            GroupByModifier::Cube => f.write_str(" WITH CUBE"),
            GroupByModifier::Totals => f.write_str(" WITH TOTALS"),
            GroupByModifier::Chain(modifiers) => modifiers.iter().try_for_each(|m| m.fmt(f)),
        }
    }
}

impl<X: AstExt> Display for Select<'_, X> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        if self.from_first {
            write!(f, "FROM {}", comma(&self.from))?;
            if !self.projection.is_empty() {
                f.write_str(" ")?;
                self.write_head(f)?;
            }
        } else {
            self.write_head(f)?;
            if let Some(into) = &self.into {
                write!(f, " {into}")?;
            }
            write_list_clause(f, "FROM", &self.from)?;
        }
        for view in &self.lateral_views {
            write!(f, " {view}")?;
        }
        if let Some(prewhere) = &self.prewhere {
            write!(f, " PREWHERE {prewhere}")?;
        }
        write_where_clause(f, &self.selection)?;
        for clause in &self.connect_by {
            SpaceOrNewline.fmt(f)?;
            clause.fmt(f)?;
        }
        if self.group_by_all {
            SpaceOrNewline.fmt(f)?;
            f.write_str("GROUP BY ALL")?;
        }
        if !self.group_by.is_empty() {
            write_indented_clause(f, "GROUP BY", comma(&self.group_by))?;
        }
        if let Some(modifier) = &self.group_by_modifier {
            modifier.fmt(f)?;
        }
        if let Some(sets) = &self.grouping_sets {
            write!(f, " {sets}")?;
        }
        if let Some(having) = &self.having {
            write_indented_clause(f, "HAVING", having)?;
        }
        if let Some(qualify) = &self.qualify {
            SpaceOrNewline.fmt(f)?;
            f.write_str("QUALIFY")?;
            SpaceOrNewline.fmt(f)?;
            qualify.fmt(f)?;
        }
        if !self.windows.is_empty() {
            SpaceOrNewline.fmt(f)?;
            f.write_str("WINDOW")?;
            SpaceOrNewline.fmt(f)?;
            comma(&self.windows).fmt(f)?;
        }
        Ok(())
    }
}

impl<X: AstExt> Display for LateralView<'_, X> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        let outer = if self.outer { " OUTER" } else { "" };
        write!(f, "LATERAL VIEW{outer} {} {}", self.function, self.name)?;
        if !self.columns.is_empty() {
            write!(f, " AS {}", comma(&self.columns))?;
        }
        Ok(())
    }
}

impl<X: AstExt> Display for ConnectBy<'_, X> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match self {
            ConnectBy::ConnectBy {
                nocycle,
                relationships,
            } => {
                let nocycle = if *nocycle { "NOCYCLE " } else { "" };
                write!(f, "CONNECT BY {nocycle}{}", comma(relationships))
            }
            ConnectBy::StartWith(condition) => write!(f, "START WITH {condition}"),
        }
    }
}

impl<X: AstExt> Display for NamedWindow<'_, X> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(f, "{} AS {}", self.name, self.window)
    }
}

impl<X: AstExt> Display for StatementKind<'_, X> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match self {
            StatementKind::Query(query) => query.fmt(f),
            StatementKind::Insert(insert) => insert.fmt(f),
            StatementKind::Update(update) => update.fmt(f),
            StatementKind::Delete(delete) => delete.fmt(f),
            StatementKind::CreateTable(create) => create.fmt(f),
            StatementKind::CreateView(create) => create.fmt(f),
            StatementKind::CreateIndex(create) => create.fmt(f),
            StatementKind::CreateSchema(create) => create.fmt(f),
            StatementKind::CreateDatabase(create) => create.fmt(f),
            StatementKind::Drop(drop) => drop.fmt(f),
            StatementKind::Truncate(truncate) => truncate.fmt(f),
            StatementKind::AlterTable(alter) => alter.fmt(f),
            StatementKind::Grant(grant) => grant.fmt(f),
            StatementKind::Revoke(revoke) => revoke.fmt(f),
            StatementKind::Merge(merge) => merge.fmt(f),
            StatementKind::DropRoutine(drop) => drop.fmt(f),
            StatementKind::Show(show) => show.fmt(f),
            StatementKind::CreateSequence(create) => create.fmt(f),
            StatementKind::AlterView(alter) => alter.fmt(f),
            StatementKind::AlterIndexRename { name, new_name } => {
                write!(f, "ALTER INDEX {name} RENAME TO {new_name}")
            }
            StatementKind::CreateRole(create) => {
                f.write_str(if create.or_replace {
                    "CREATE OR REPLACE ROLE "
                } else {
                    "CREATE ROLE "
                })?;
                write_if_not_exists(f, create.if_not_exists)?;
                comma(&create.names).fmt(f)
            }
            StatementKind::Analyze(analyze) => {
                f.write_str("ANALYZE")?;
                if analyze.table_keyword {
                    f.write_str(" TABLE")?;
                }
                if !analyze.names.is_empty() {
                    write!(f, " {}", comma(&analyze.names))?;
                }
                Ok(())
            }
            StatementKind::Transaction(transaction) => transaction.fmt(f),
            StatementKind::Use(name) => write!(f, "USE {name}"),
            StatementKind::Explain(explain) => explain.fmt(f),
            StatementKind::Set(set) => set.fmt(f),
            StatementKind::Comment(comment) => comment.fmt(f),
            StatementKind::Call(call) => write!(f, "CALL {call}"),
            StatementKind::Ext(ext) => ext.fmt(f),
        }
    }
}

impl<X: AstExt> Display for OutputClause<'_, X> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(f, "OUTPUT {}", comma(&self.items))?;
        if let Some(into) = &self.into {
            write!(f, " INTO {into}")?;
        }
        Ok(())
    }
}

fn write_output<X: AstExt>(
    f: &mut Formatter<'_>,
    output: &Option<Box<OutputClause<'_, X>>>,
) -> Result {
    match output {
        Some(output) => {
            SpaceOrNewline.fmt(f)?;
            output.fmt(f)
        }
        None => Ok(()),
    }
}

impl<X: AstExt> Display for Insert<'_, X> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        f.write_str("INSERT ")?;
        write_insert_body(f, self)
    }
}

/// Writes an `INSERT` from its modifiers onwards.
pub fn write_insert_body<X: AstExt>(f: &mut Formatter<'_>, insert: &Insert<'_, X>) -> Result {
    let into = if insert.into { "INTO " } else { "" };
    write!(f, "{}{into}{}", insert.modifiers, insert.table)?;
    if !insert.columns.is_empty() {
        write!(f, " ({})", comma(&insert.columns))?;
    }
    write_output(f, &insert.output)?;
    let on_new_line = !insert.columns.is_empty() || insert.output.is_some();
    write_insert_source(f, &insert.source, on_new_line)?;
    write_list_clause(f, "RETURNING", &insert.returning)
}

/// Writes the rows or query of an `INSERT` after a space. With `on_new_line`,
/// `{:#}` starts them on a new line instead, as it does after a column list.
pub fn write_insert_source<X: AstExt>(
    f: &mut Formatter<'_>,
    source: &InsertSource<'_, X>,
    on_new_line: bool,
) -> Result {
    if on_new_line {
        SpaceOrNewline.fmt(f)?;
    } else {
        f.write_str(" ")?;
    }
    match source {
        InsertSource::Values(rows) => write_values(f, rows, ""),
        InsertSource::RowValues(rows) => write_values(f, rows, "ROW"),
        InsertSource::Set(assignments) => {
            f.write_str("SET")?;
            indented_list(f, assignments)
        }
        InsertSource::Query(query) => query.fmt(f),
        InsertSource::DefaultValues => f.write_str("DEFAULT VALUES"),
    }
}

impl<X: AstExt> Display for Assignment<'_, X> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(f, "{} = {}", self.target, self.value)
    }
}

impl<X: AstExt> Display for Update<'_, X> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        f.write_str("UPDATE ")?;
        write_update_body(f, self)
    }
}

/// Writes an `UPDATE` from its table onwards.
pub fn write_update_body<X: AstExt>(f: &mut Formatter<'_>, update: &Update<'_, X>) -> Result {
    update.table.fmt(f)?;
    for join in &update.joins {
        SpaceOrNewline.fmt(f)?;
        join.fmt(f)?;
    }
    if update.from_before_set {
        write_list_clause(f, "FROM", &update.from)?;
    }
    write_list_clause(f, "SET", &update.assignments)?;
    if !update.from_before_set {
        write_list_clause(f, "FROM", &update.from)?;
    }
    write_output(f, &update.output)?;
    write_where_clause(f, &update.selection)?;
    write_order_by_and_limit(f, &update.order_by, &update.limit)?;
    write_list_clause(f, "RETURNING", &update.returning)
}

impl<X: AstExt> Display for Delete<'_, X> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        if let Some(multi) = &self.multi {
            return write_multi_table_delete(f, self, multi);
        }
        f.write_str("DELETE FROM")?;
        write_delete_body(f, self)
    }
}

/// Writes a multi-table `DELETE`, in whichever of its two forms it was read.
fn write_multi_table_delete<X: AstExt>(
    f: &mut Formatter<'_>,
    delete: &Delete<'_, X>,
    multi: &MultiTableDelete<'_>,
) -> Result {
    if multi.from_keyword {
        f.write_str("DELETE")?;
        indented_list(f, &multi.targets)?;
        f.write_str(" FROM")?;
        indented_list(f, &delete.using)?;
    } else {
        f.write_str("DELETE FROM")?;
        indented_list(f, &multi.targets)?;
        write_list_clause(f, "USING", &delete.using)?;
    }
    write_where_clause(f, &delete.selection)
}

/// Writes a `DELETE` from a space or line break before its table onwards.
pub fn write_delete_body<X: AstExt>(f: &mut Formatter<'_>, delete: &Delete<'_, X>) -> Result {
    SpaceOrNewline.fmt(f)?;
    Indent(&delete.table).fmt(f)?;
    write_list_clause(f, "USING", &delete.using)?;
    write_output(f, &delete.output)?;
    write_where_clause(f, &delete.selection)?;
    write_list_clause(f, "ORDER BY", &delete.order_by)?;
    if let Some(limit) = &delete.limit {
        match (&limit.count, &limit.offset, limit.all, limit.by.is_empty()) {
            (Some(count), None, false, true) => write_indented_clause(f, "LIMIT", count)?,
            _ => {
                SpaceOrNewline.fmt(f)?;
                limit.fmt(f)?;
            }
        }
    }
    write_list_clause(f, "RETURNING", &delete.returning)
}

fn write_order_by_and_limit<X: AstExt>(
    f: &mut Formatter<'_>,
    order_by: &[OrderByItem<'_, X>],
    limit: &Option<Box<Limit<'_, X>>>,
) -> Result {
    write_list_clause(f, "ORDER BY", order_by)?;
    if let Some(limit) = limit {
        SpaceOrNewline.fmt(f)?;
        limit.fmt(f)?;
    }
    Ok(())
}

impl Display for AngleArguments<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match self {
            Self::Types(types) | Self::ParenTypes(types) => comma(types).fmt(f),
            Self::Fields(fields) | Self::ParenFields(fields) => comma(fields).fmt(f),
        }
    }
}

impl Display for StructField<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        if let Some(name) = &self.name {
            let separator = if self.colon { ": " } else { " " };
            write!(f, "{name}{separator}")?;
        }
        self.data_type.fmt(f)?;
        if self.not_null {
            f.write_str(" NOT NULL")?;
        }
        if let Some(comment) = &self.comment {
            write!(f, " COMMENT {}", comment)?;
        }
        if !self.options.is_empty() {
            write!(f, " OPTIONS({})", comma(&self.options))?;
        }
        Ok(())
    }
}

impl Display for StructFieldOption<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(f, "{} = {}", self.key, self.value)
    }
}

impl<X: AstExt> Display for ExprOption<'_, X> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(f, "{} = {}", self.name, self.value)
    }
}

impl Display for DataType<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        for part in &self.qualifier {
            write!(f, "{part}.")?;
        }
        self.name.fmt(f)?;
        match self.angle.as_deref() {
            Some(arguments @ (AngleArguments::ParenTypes(_) | AngleArguments::ParenFields(_))) => {
                write!(f, "({arguments})")?;
            }
            Some(arguments) => write!(f, "<{arguments}>")?,
            None => {}
        }
        f.write_str(match self.suffix {
            DataTypeSuffix::Precision | DataTypeSuffix::PrecisionUnsigned => " PRECISION",
            DataTypeSuffix::Varying => " VARYING",
            DataTypeSuffix::IntervalFields(fields) => match fields {
                IntervalFields::Year => " YEAR",
                IntervalFields::Month => " MONTH",
                IntervalFields::Day => " DAY",
                IntervalFields::Hour => " HOUR",
                IntervalFields::Minute => " MINUTE",
                IntervalFields::Second => " SECOND",
                IntervalFields::YearToMonth => " YEAR TO MONTH",
                IntervalFields::DayToHour => " DAY TO HOUR",
                IntervalFields::DayToMinute => " DAY TO MINUTE",
                IntervalFields::DayToSecond => " DAY TO SECOND",
                IntervalFields::HourToMinute => " HOUR TO MINUTE",
                IntervalFields::HourToSecond => " HOUR TO SECOND",
                IntervalFields::MinuteToSecond => " MINUTE TO SECOND",
            },
            _ => "",
        })?;
        const PRECISION_AND_SCALE: [&str; 7] = [
            "BIGDECIMAL",
            "BIGNUMERIC",
            "DEC",
            "DECIMAL",
            "DOUBLE",
            "FLOAT",
            "NUMERIC",
        ];
        let unspaced = PRECISION_AND_SCALE
            .iter()
            .any(|name| self.name.to_string().eq_ignore_ascii_case(name));
        match (self.args.as_slice(), self.suffix) {
            ([], _) => {}
            (args, DataTypeSuffix::SignedArgs(signs)) => {
                f.write_str("(")?;
                for (i, arg) in args.iter().enumerate() {
                    if i > 0 {
                        f.write_str(", ")?;
                    }
                    if let Some(&sign) = signs.get(i).filter(|sign| **sign != 0) {
                        write!(f, "{}", sign as char)?;
                    }
                    f.write_str(arg)?;
                }
                f.write_str(")")?;
            }
            ([precision, scale], _) if unspaced => write!(f, "({precision},{scale})")?,
            (args, _) => write!(f, "({})", comma(args))?,
        }
        f.write_str(match self.suffix {
            DataTypeSuffix::WithTimeZone => " WITH TIME ZONE",
            DataTypeSuffix::WithoutTimeZone => " WITHOUT TIME ZONE",
            DataTypeSuffix::Unsigned | DataTypeSuffix::PrecisionUnsigned => " UNSIGNED",
            DataTypeSuffix::Zerofill => " ZEROFILL",
            DataTypeSuffix::UnsignedZerofill => " UNSIGNED ZEROFILL",
            DataTypeSuffix::Integer => " INTEGER",
            _ => "",
        })?;
        let write_size = |f: &mut Formatter<'_>, size: &Option<Cow<'_, str>>| match size {
            Some(size) => write!(f, "[{size}]"),
            None => f.write_str("[]"),
        };
        match &self.array {
            ArrayType::None => Ok(()),
            ArrayType::Keyword(None) => f.write_str(" ARRAY"),
            ArrayType::Keyword(Some(size)) => write!(f, " ARRAY[{size}]"),
            ArrayType::Brackets(dimensions) => {
                dimensions.iter().try_for_each(|size| write_size(f, size))
            }
        }
    }
}

impl Display for SignedNumber<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(f, "{}{}", self.sign, self.digits)
    }
}

impl Display for SequenceOption<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match self {
            SequenceOption::Increment { by: true, value } => write!(f, "INCREMENT BY {value}"),
            SequenceOption::Increment { by: false, value } => write!(f, "INCREMENT {value}"),
            SequenceOption::MinValue(Some(value)) => write!(f, "MINVALUE {value}"),
            SequenceOption::MinValue(None) => f.write_str("NO MINVALUE"),
            SequenceOption::MaxValue(Some(value)) => write!(f, "MAXVALUE {value}"),
            SequenceOption::MaxValue(None) => f.write_str("NO MAXVALUE"),
            SequenceOption::Start { with: true, value } => write!(f, "START WITH {value}"),
            SequenceOption::Start { with: false, value } => write!(f, "START {value}"),
            SequenceOption::Cache(value) => write!(f, "CACHE {value}"),
            SequenceOption::Cycle(true) => f.write_str("CYCLE"),
            SequenceOption::Cycle(false) => f.write_str("NO CYCLE"),
            SequenceOption::OwnedBy(name) => write!(f, "OWNED BY {name}"),
        }
    }
}

impl Display for CreateSequence<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write_create_flags(f, false, self.temporary)?;
        f.write_str("SEQUENCE ")?;
        write_if_not_exists(f, self.if_not_exists)?;
        self.name.fmt(f)?;
        if let Some(data_type) = &self.data_type {
            write!(f, " AS {data_type}")?;
        }
        for option in &self.options {
            write!(f, " {option}")?;
        }
        Ok(())
    }
}

impl<X: AstExt> Display for Generated<'_, X> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        f.write_str(match self.when {
            Some(GeneratedWhen::Always) => "GENERATED ALWAYS AS ",
            Some(GeneratedWhen::ByDefault) => "GENERATED BY DEFAULT AS ",
            Some(GeneratedWhen::Omitted) => "AS ",
            None => "GENERATED AS ",
        })?;
        match &self.kind {
            GeneratedKind::Identity(None) => f.write_str("IDENTITY"),
            GeneratedKind::Identity(Some(options)) => {
                f.write_str("IDENTITY ( ")?;
                for option in options {
                    write!(f, "{option} ")?;
                }
                f.write_str(")")
            }
            GeneratedKind::Expr { expr, storage } => {
                write!(f, "({expr})")?;
                f.write_str(match storage {
                    Some(GeneratedStorage::Stored) => " STORED",
                    Some(GeneratedStorage::Virtual) => " VIRTUAL",
                    None => "",
                })
            }
        }
    }
}

impl Display for ConstraintAttribute {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        f.write_str(match self {
            ConstraintAttribute::Deferrable(true) => "DEFERRABLE",
            ConstraintAttribute::Deferrable(false) => "NOT DEFERRABLE",
            ConstraintAttribute::InitiallyDeferred(true) => "INITIALLY DEFERRED",
            ConstraintAttribute::InitiallyDeferred(false) => "INITIALLY IMMEDIATE",
            ConstraintAttribute::Enforced(true) => "ENFORCED",
            ConstraintAttribute::Enforced(false) => "NOT ENFORCED",
            ConstraintAttribute::NotValid => "NOT VALID",
            ConstraintAttribute::NoInherit => "NO INHERIT",
        })
    }
}

impl Display for Tag<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(f, "{}={}", self.name, self.value)
    }
}

impl<X: AstExt> Display for ColumnOption<'_, X> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match self {
            ColumnOption::Collate(name) => write!(f, "COLLATE {name}"),
            ColumnOption::Identity {
                autoincrement,
                params,
                order,
            } => {
                f.write_str(if *autoincrement {
                    "AUTOINCREMENT"
                } else {
                    "IDENTITY"
                })?;
                match params {
                    Some(IdentityParams {
                        seed,
                        increment,
                        start_form: true,
                    }) => write!(f, " START {seed} INCREMENT {increment}")?,
                    Some(IdentityParams {
                        seed, increment, ..
                    }) => write!(f, "({seed}, {increment})")?,
                    None => {}
                }
                f.write_str(match order {
                    Some(true) => " ORDER",
                    Some(false) => " NOORDER",
                    None => "",
                })
            }
            ColumnOption::Policy {
                with,
                masking,
                name,
                using,
            } => {
                let with = if *with { "WITH " } else { "" };
                let kind = if *masking { "MASKING" } else { "PROJECTION" };
                write!(f, "{with}{kind} POLICY {name}")?;
                if !using.is_empty() {
                    write!(f, " USING ({})", comma(using))?;
                }
                Ok(())
            }
            ColumnOption::Tags { with, tags } => {
                let with = if *with { "WITH " } else { "" };
                write!(f, "{with}TAG ({})", comma(tags))
            }
            ColumnOption::Comment(text) => write!(f, "COMMENT {}", text),
            ColumnOption::GenericOptions(options) => {
                write!(f, "OPTIONS ({})", comma(options))
            }
            ColumnOption::NullsDistinct(true) => f.write_str("NULLS DISTINCT"),
            ColumnOption::NullsDistinct(false) => f.write_str("NULLS NOT DISTINCT"),
            ColumnOption::Options(options) => write!(f, "OPTIONS({})", comma(options)),
            ColumnOption::Generated(generated) => generated.fmt(f),
            ColumnOption::Attribute(attribute) => attribute.fmt(f),
            ColumnOption::NotNull => f.write_str("NOT NULL"),
            ColumnOption::Null => f.write_str("NULL"),
            ColumnOption::PrimaryKey => f.write_str("PRIMARY KEY"),
            ColumnOption::Unique => f.write_str("UNIQUE"),
            ColumnOption::Default(expr) => write!(f, "DEFAULT {expr}"),
            ColumnOption::Keyword { keyword, expr } => {
                f.write_str(keyword)?;
                match expr {
                    Some(expr) => write!(f, " {expr}"),
                    None => Ok(()),
                }
            }
            ColumnOption::AutoIncrement => f.write_str("AUTO_INCREMENT"),
            ColumnOption::Check(expr) => write!(f, "CHECK ({expr})"),
            ColumnOption::References(references) => references.fmt(f),
            ColumnOption::Named { name, option } => write!(f, "CONSTRAINT {name} {option}"),
        }
    }
}

impl<X: AstExt> Display for ColumnDef<'_, X> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        self.name.fmt(f)?;
        if !self.data_type.is_unspecified() {
            write!(f, " {}", self.data_type)?;
        }
        for option in &self.options {
            write!(f, " {option}")?;
        }
        Ok(())
    }
}

impl<X: AstExt> Display for ExcludeElement<'_, X> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(f, "{} WITH {}", self.column, self.operator)
    }
}

impl Display for ExcludeOperator<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match self {
            ExcludeOperator::Plain(operator) => f.write_str(operator),
            ExcludeOperator::Qualified(operator) => write!(f, "OPERATOR({operator})"),
        }
    }
}

impl Display for OperatorName<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        for part in &self.schema {
            write!(f, "{part}.")?;
        }
        f.write_str(&self.operator)
    }
}

impl<X: AstExt> Display for PartitionBound<'_, X> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match self {
            PartitionBound::Default => f.write_str("DEFAULT"),
            PartitionBound::In(values) => write!(f, "FOR VALUES IN ({})", comma(values)),
            PartitionBound::Range { from, to } => {
                write!(f, "FOR VALUES FROM ({}) TO ({})", comma(from), comma(to))
            }
            PartitionBound::Hash { modulus, remainder } => {
                write!(
                    f,
                    "FOR VALUES WITH (MODULUS {modulus}, REMAINDER {remainder})"
                )
            }
        }
    }
}

impl<X: AstExt> Display for RangeBound<'_, X> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match self {
            RangeBound::MinValue => f.write_str("MINVALUE"),
            RangeBound::MaxValue => f.write_str("MAXVALUE"),
            RangeBound::Value(value) => value.fmt(f),
        }
    }
}

impl Display for GenericOption<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        if let Some(ref action) = self.action {
            write!(f, "{action} ")?;
        }
        self.name.fmt(f)?;
        if let Some(ref value) = self.value {
            write!(f, " {}", value)?;
        }
        Ok(())
    }
}

impl<X: AstExt> Display for TableConstraint<'_, X> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        if let Some(name) = &self.name {
            write!(f, "CONSTRAINT {name} ")?;
        }
        let nulls = match self.nulls_distinct {
            Some(true) => "NULLS DISTINCT ",
            Some(false) => "NULLS NOT DISTINCT ",
            None => "",
        };
        match &self.kind {
            TableConstraintKind::PrimaryKey(columns) => {
                write!(f, "PRIMARY KEY ({})", comma(columns))?;
            }
            TableConstraintKind::PrimaryKeyUsingIndex(index) => {
                write!(f, "PRIMARY KEY USING INDEX {index}")?;
            }
            TableConstraintKind::Unique(columns) => {
                write!(f, "UNIQUE {nulls}({})", comma(columns))?;
            }
            TableConstraintKind::UniqueUsingIndex(index) => {
                write!(f, "UNIQUE {nulls}USING INDEX {index}")?;
            }
            TableConstraintKind::Check(expr) => write!(f, "CHECK ({expr})")?,
            TableConstraintKind::ForeignKey {
                index_name,
                columns,
                references,
            } => {
                f.write_str("FOREIGN KEY ")?;
                if let Some(index_name) = index_name {
                    write!(f, "{index_name} ")?;
                }
                write!(f, "({}) {references}", comma(columns))?;
            }
            TableConstraintKind::Key {
                keyword,
                name,
                using,
                columns,
                options,
            } => {
                f.write_str(keyword)?;
                if let Some(name) = name {
                    write!(f, " {name}")?;
                }
                if let Some(using) = using {
                    write!(f, " USING {using}")?;
                }
                write!(f, " ({})", comma(columns))?;
                for option in options {
                    write!(f, " {option}")?;
                }
            }
            TableConstraintKind::NotNull(column) => write!(f, "NOT NULL {column}")?,
            TableConstraintKind::Exclude(exclude) => {
                f.write_str("EXCLUDE ")?;
                if let Some(using) = &exclude.using {
                    write!(f, "USING {using} ")?;
                }
                write!(f, "({})", comma(&exclude.elements))?;
            }
        }
        if !self.include.is_empty() {
            write!(f, " INCLUDE ({})", comma(&self.include))?;
        }
        if let TableConstraintKind::Exclude(exclude) = &self.kind
            && let Some(predicate) = &exclude.predicate
        {
            write!(f, " WHERE ({predicate})")?;
        }
        for attribute in &self.attributes {
            write!(f, " {attribute}")?;
        }
        Ok(())
    }
}

/// Renders ` (columns, constraints)`.
pub fn write_columns<X: AstExt>(
    f: &mut Formatter<'_>,
    columns: &[ColumnDef<'_, X>],
    constraints: &[TableConstraint<'_, X>],
) -> Result {
    if columns.is_empty() && constraints.is_empty() {
        return f.write_str(" ()");
    }
    f.write_str(" (")?;
    NewLine.fmt(f)?;
    if !columns.is_empty() {
        Indent(CommaLines(columns)).fmt(f)?;
    }
    if !constraints.is_empty() {
        if !columns.is_empty() {
            f.write_str(",")?;
            SpaceOrNewline.fmt(f)?;
        }
        Indent(CommaLines(constraints)).fmt(f)?;
    }
    NewLine.fmt(f)?;
    f.write_str(")")
}

impl<X: AstExt> Display for CreateTable<'_, X> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write_create_flags(f, self.or_replace, self.temporary)?;
        f.write_str("TABLE ")?;
        if self.if_not_exists {
            f.write_str("IF NOT EXISTS ")?;
        }
        self.name.fmt(f)?;
        if !self.columns.is_empty() || !self.constraints.is_empty() {
            write_columns(f, &self.columns, &self.constraints)?;
        }
        match &self.query {
            Some(query) => write!(f, " AS {query}"),
            None => Ok(()),
        }
    }
}

fn write_is<X: AstExt>(f: &mut Formatter<'_>, test: &IsTest<'_, X>, negated: bool) -> Result {
    write!(f, " IS {}", not(negated))?;
    match test {
        IsTest::Null => f.write_str("NULL"),
        IsTest::True => f.write_str("TRUE"),
        IsTest::False => f.write_str("FALSE"),
        IsTest::Unknown => f.write_str("UNKNOWN"),
        IsTest::DistinctFrom(other) => write!(f, "DISTINCT FROM {other}"),
        IsTest::Json { kind, unique_keys } => {
            f.write_str("JSON")?;
            f.write_str(match kind {
                None => "",
                Some(JsonPredicateType::Value) => " VALUE",
                Some(JsonPredicateType::Array) => " ARRAY",
                Some(JsonPredicateType::Object) => " OBJECT",
                Some(JsonPredicateType::Scalar) => " SCALAR",
            })?;
            if let Some(unique) = unique_keys {
                f.write_str(if unique.with {
                    " WITH UNIQUE"
                } else {
                    " WITHOUT UNIQUE"
                })?;
                if unique.keys {
                    f.write_str(" KEYS")?;
                }
            }
            Ok(())
        }
        IsTest::Normalized(form) => f.write_str(match form {
            None => "NORMALIZED",
            Some(NormalForm::Nfc) => "NFC NORMALIZED",
            Some(NormalForm::Nfd) => "NFD NORMALIZED",
            Some(NormalForm::Nfkc) => "NFKC NORMALIZED",
            Some(NormalForm::Nfkd) => "NFKD NORMALIZED",
        }),
    }
}

fn write_like<X: AstExt>(
    f: &mut Formatter<'_>,
    pattern: &Expr<'_, X>,
    negated: bool,
    kind: &LikeKind<'_>,
    escape: Option<&Expr<'_, X>>,
) -> Result {
    let keyword = match kind {
        LikeKind::Like => "LIKE",
        LikeKind::ILike => "ILIKE",
        LikeKind::SimilarTo => "SIMILAR TO",
        LikeKind::Named(name) => name.as_ref(),
    };
    write!(f, " {}{keyword} {pattern}", not(negated))?;
    match escape {
        Some(escape) => write!(f, " ESCAPE {escape}"),
        None => Ok(()),
    }
}

fn write_between<X: AstExt>(
    f: &mut Formatter<'_>,
    low: &Expr<'_, X>,
    high: &Expr<'_, X>,
    negated: bool,
) -> Result {
    write!(f, " {}BETWEEN {low} AND {high}", not(negated))
}

impl<X: AstExt> Display for CaseWhen<'_, X> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        f.write_str("WHEN ")?;
        self.condition.fmt(f)?;
        f.write_str(" THEN")?;
        SpaceOrNewline.fmt(f)?;
        Indent(&self.result).fmt(f)
    }
}

fn write_case<X: AstExt>(f: &mut Formatter<'_>, case: &CaseExpr<'_, X>) -> Result {
    f.write_str("CASE")?;
    if let Some(operand) = &case.operand {
        f.write_str(" ")?;
        operand.fmt(f)?;
    }
    for branch in &case.branches {
        SpaceOrNewline.fmt(f)?;
        Indent(branch).fmt(f)?;
    }
    if let Some(result) = &case.else_result {
        SpaceOrNewline.fmt(f)?;
        Indent("ELSE").fmt(f)?;
        SpaceOrNewline.fmt(f)?;
        Indent(Indent(result)).fmt(f)?;
    }
    SpaceOrNewline.fmt(f)?;
    f.write_str("END")
}

fn quantifier_prefix(quantifier: SetQuantifier) -> &'static str {
    match quantifier {
        SetQuantifier::None => "",
        SetQuantifier::All => "ALL ",
        SetQuantifier::Distinct => "DISTINCT ",
    }
}

fn write_function<X: AstExt>(
    f: &mut Formatter<'_>,
    name: &ObjectName<'_>,
    args: &[FunctionArg<'_, X>],
    options: Option<&FunctionOptions<'_, X>>,
) -> Result {
    let Some(options) = options else {
        return write!(f, "{name}({})", comma(args));
    };
    let prefix = quantifier_prefix(options.quantifier);
    if options.parameters.is_empty() {
        write!(f, "{name}({prefix}{}", comma(args))?;
    } else {
        write!(
            f,
            "{name}({})({prefix}{}",
            comma(&options.parameters),
            comma(args)
        )?;
    }
    write_null_treatment(f, options.inner_null_treatment)?;
    if args.is_empty() && !options.order_by.is_empty() {
        write!(f, "ORDER BY {}", comma(&options.order_by))?;
    } else {
        write_list(f, "ORDER BY", &options.order_by)?;
    }
    if let Some(on_overflow) = &options.on_overflow {
        write!(f, " {on_overflow}")?;
    }
    if let Some(ref separator) = options.separator {
        write!(f, " SEPARATOR {}", separator)?;
    }
    f.write_str(")")?;
    if !options.within_group.is_empty() {
        write!(
            f,
            " WITHIN GROUP (ORDER BY {})",
            comma(&options.within_group)
        )?;
    }
    if let Some(filter) = &options.filter {
        write!(f, " FILTER (WHERE {filter})")?;
    }
    write_null_treatment(f, options.null_treatment)?;
    match &options.over {
        Some(over) => {
            f.write_str(" OVER ")?;
            over.fmt(f)
        }
        None => Ok(()),
    }
}

fn write_null_treatment(f: &mut Formatter<'_>, treatment: Option<NullTreatment>) -> Result {
    match treatment {
        Some(NullTreatment::Ignore) => f.write_str(" IGNORE NULLS"),
        Some(NullTreatment::Respect) => f.write_str(" RESPECT NULLS"),
        None => Ok(()),
    }
}

impl<X: AstExt> Display for WindowOver<'_, X> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match self {
            WindowOver::Named(name) => name.fmt(f),
            WindowOver::Spec(spec) if spec.is_empty() => f.write_str("()"),
            WindowOver::Spec(spec) => {
                f.write_str("(")?;
                NewLine.fmt(f)?;
                Indent(spec).fmt(f)?;
                NewLine.fmt(f)?;
                f.write_str(")")
            }
        }
    }
}

impl<X: AstExt> WindowSpec<'_, X> {
    fn is_empty(&self) -> bool {
        self.base.is_none()
            && self.partition_by.is_empty()
            && self.order_by.is_empty()
            && self.frame.is_none()
    }
}

impl<X: AstExt> Display for WindowSpec<'_, X> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        let mut first = true;
        let mut separate = |f: &mut Formatter<'_>| -> Result {
            if !std::mem::take(&mut first) {
                SpaceOrNewline.fmt(f)?;
            }
            Ok(())
        };
        if let Some(base) = &self.base {
            separate(f)?;
            write!(f, "{base}")?;
        }
        if !self.partition_by.is_empty() {
            separate(f)?;
            write!(f, "PARTITION BY {}", comma(&self.partition_by))?;
        }
        if !self.order_by.is_empty() {
            separate(f)?;
            write!(f, "ORDER BY {}", comma(&self.order_by))?;
        }
        if let Some(frame) = &self.frame {
            separate(f)?;
            write!(f, "{frame}")?;
        }
        Ok(())
    }
}

impl<X: AstExt> Display for WindowFrame<'_, X> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        f.write_str(match self.units {
            FrameUnits::Rows => "ROWS ",
            FrameUnits::Range => "RANGE ",
            FrameUnits::Groups => "GROUPS ",
        })?;
        match &self.end {
            Some(end) => write!(f, "BETWEEN {} AND {end}", self.start),
            None => self.start.fmt(f),
        }
    }
}

impl<X: AstExt> Display for FrameBound<'_, X> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match self {
            FrameBound::CurrentRow => f.write_str("CURRENT ROW"),
            FrameBound::UnboundedPreceding => f.write_str("UNBOUNDED PRECEDING"),
            FrameBound::UnboundedFollowing => f.write_str("UNBOUNDED FOLLOWING"),
            FrameBound::Preceding(offset) => write!(f, "{offset} PRECEDING"),
            FrameBound::Following(offset) => write!(f, "{offset} FOLLOWING"),
        }
    }
}

pub fn write_if_exists(f: &mut Formatter<'_>, if_exists: bool) -> Result {
    if if_exists {
        f.write_str("IF EXISTS ")?;
    }
    Ok(())
}

pub fn write_if_not_exists(f: &mut Formatter<'_>, if_not_exists: bool) -> Result {
    if if_not_exists {
        f.write_str("IF NOT EXISTS ")?;
    }
    Ok(())
}

fn write_behavior(f: &mut Formatter<'_>, behavior: Option<DropBehavior>) -> Result {
    match behavior {
        Some(DropBehavior::Cascade) => f.write_str(" CASCADE"),
        Some(DropBehavior::Restrict) => f.write_str(" RESTRICT"),
        None => Ok(()),
    }
}

fn column_word(column_keyword: bool) -> &'static str {
    if column_keyword { "COLUMN " } else { "" }
}

impl Display for ReferentialAction {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        f.write_str(match self {
            ReferentialAction::Cascade => "CASCADE",
            ReferentialAction::Restrict => "RESTRICT",
            ReferentialAction::SetNull => "SET NULL",
            ReferentialAction::SetDefault => "SET DEFAULT",
            ReferentialAction::NoAction => "NO ACTION",
        })
    }
}

impl Display for KeyOption<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match self {
            KeyOption::Using(method) => write!(f, "USING {method}"),
            KeyOption::Comment(text) => write!(f, "COMMENT {}", text),
        }
    }
}

impl Display for ForeignKeyRef<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(f, "REFERENCES {}", self.table)?;
        if !self.columns.is_empty() {
            write!(f, " ({})", comma(&self.columns))?;
        }
        if let Some(ref kind) = self.match_kind {
            write!(f, " MATCH {kind}")?;
        }
        if let Some(action) = self.on_delete {
            write!(f, " ON DELETE {action}")?;
        }
        if let Some(action) = self.on_update {
            write!(f, " ON UPDATE {action}")?;
        }
        Ok(())
    }
}

impl Display for DropObject<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        let object_type = match self.object_type {
            ObjectType::Table => "TABLE",
            ObjectType::View => "VIEW",
            ObjectType::MaterializedView => "MATERIALIZED VIEW",
            ObjectType::Index => "INDEX",
            ObjectType::Schema => "SCHEMA",
            ObjectType::Database => "DATABASE",
            ObjectType::Sequence => "SEQUENCE",
            ObjectType::Named(ref name) => name,
        };
        let temporary = if self.temporary { "TEMPORARY " } else { "" };
        write!(f, "DROP {temporary}{object_type} ")?;
        write_if_exists(f, self.if_exists)?;
        comma(&self.names).fmt(f)?;
        if let Some(on) = &self.on {
            write!(f, " ON {on}")?;
        }
        write_behavior(f, self.behavior)?;
        f.write_str(if self.purge { " PURGE" } else { "" })
    }
}

/// Renders source words in upper case, separated by spaces.
fn write_words(f: &mut Formatter<'_>, words: &[Cow<'_, str>]) -> Result {
    for (i, word) in words.iter().enumerate() {
        if i > 0 {
            f.write_str(" ")?;
        }
        f.write_str(&word.to_ascii_uppercase())?;
    }
    Ok(())
}

impl Display for Privilege<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write_words(f, &self.words)?;
        if !self.columns.is_empty() {
            write!(f, " ({})", comma(&self.columns))?;
        }
        Ok(())
    }
}

impl Display for Privileges<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match self {
            Privileges::All { privileges: false } => f.write_str("ALL"),
            Privileges::All { privileges: true } => f.write_str("ALL PRIVILEGES"),
            Privileges::List(privileges) => comma(privileges).fmt(f),
            Privileges::Role {
                database: true,
                name,
            } => write!(f, "DATABASE ROLE {name}"),
            Privileges::Role {
                database: false,
                name,
            } => write!(f, "ROLE {name}"),
        }
    }
}

impl Display for GrantName<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        self.name.fmt(f)?;
        if let Some(args) = &self.args {
            write!(f, "({})", comma(args))?;
        }
        Ok(())
    }
}

impl Display for GrantObjects<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        if !self.kind.is_empty() {
            write_words(f, &self.kind)?;
            f.write_str(" ")?;
        }
        comma(&self.names).fmt(f)
    }
}

impl Display for Grantee<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        if !self.kind.is_empty() {
            write_words(f, &self.kind)?;
            f.write_str(" ")?;
        }
        self.name.fmt(f)?;
        match &self.host {
            Some(host) => write!(f, "@{host}"),
            None => Ok(()),
        }
    }
}

impl Display for Grant<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(f, "GRANT {}", self.privileges)?;
        if let Some(objects) = &self.objects {
            write!(f, " ON {objects}")?;
        }
        write!(f, " TO {}", comma(&self.grantees))?;
        if self.with_grant_option {
            f.write_str(" WITH GRANT OPTION")?;
        }
        f.write_str(match self.current_grants {
            Some(CurrentGrants::Copy) => " COPY CURRENT GRANTS",
            Some(CurrentGrants::Revoke) => " REVOKE CURRENT GRANTS",
            None => "",
        })?;
        if let Some(grantor) = &self.as_grantor {
            write!(f, " AS {grantor}")?;
        }
        if let Some(granted_by) = &self.granted_by {
            write!(f, " GRANTED BY {granted_by}")?;
        }
        Ok(())
    }
}

impl Display for Revoke<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        f.write_str("REVOKE ")?;
        if self.grant_option_for {
            f.write_str("GRANT OPTION FOR ")?;
        }
        write!(
            f,
            "{} ON {} FROM {}",
            self.privileges,
            self.objects,
            comma(&self.grantees)
        )?;
        if let Some(granted_by) = &self.granted_by {
            write!(f, " GRANTED BY {granted_by}")?;
        }
        write_behavior(f, self.behavior)
    }
}

impl<X: AstExt> Display for Merge<'_, X> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        f.write_str("MERGE ")?;
        write_merge_body(f, self)
    }
}

/// Writes a `MERGE` from any `INTO` onwards.
pub fn write_merge_body<X: AstExt>(f: &mut Formatter<'_>, merge: &Merge<'_, X>) -> Result {
    if merge.into {
        f.write_str("INTO ")?;
    }
    write!(f, "{}", merge.table)?;
    SpaceOrNewline.fmt(f)?;
    write!(f, "USING {} ON {}", merge.source, merge.on)?;
    for clause in &merge.clauses {
        SpaceOrNewline.fmt(f)?;
        clause.fmt(f)?;
    }
    write_output(f, &merge.output)?;
    write_list_clause(f, "RETURNING", &merge.returning)
}

impl<X: AstExt> Display for MergeClause<'_, X> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        f.write_str(match self.kind {
            MergeClauseKind::Matched => "WHEN MATCHED",
            MergeClauseKind::NotMatched => "WHEN NOT MATCHED",
            MergeClauseKind::NotMatchedBySource => "WHEN NOT MATCHED BY SOURCE",
            MergeClauseKind::NotMatchedByTarget => "WHEN NOT MATCHED BY TARGET",
        })?;
        if let Some(predicate) = &self.predicate {
            write!(f, " AND {predicate}")?;
        }
        f.write_str(" THEN")?;
        SpaceOrNewline.fmt(f)?;
        Indent(&self.action).fmt(f)
    }
}

impl<X: AstExt> Display for MergeAction<'_, X> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match self {
            MergeAction::Delete => f.write_str("DELETE"),
            MergeAction::DoNothing => f.write_str("DO NOTHING"),
            MergeAction::InsertAll => f.write_str("INSERT *"),
            MergeAction::Update {
                assignments,
                predicate,
                delete_predicate,
            } => {
                f.write_str("UPDATE SET ")?;
                match assignments {
                    Some(assignments) => comma(assignments).fmt(f)?,
                    None => f.write_str("*")?,
                }
                write_where(f, predicate)?;
                if let Some(predicate) = delete_predicate {
                    write!(f, " DELETE WHERE {predicate}")?;
                }
                Ok(())
            }
            MergeAction::InsertRow { columns, predicate } => {
                f.write_str("INSERT ")?;
                if !columns.is_empty() {
                    write!(f, "({}) ", comma(columns))?;
                }
                f.write_str("ROW")?;
                write_where(f, predicate)
            }
            MergeAction::Insert {
                columns,
                rows,
                predicate,
            } => {
                f.write_str("INSERT ")?;
                if !columns.is_empty() {
                    write!(f, "({}) ", comma(columns))?;
                }
                f.write_str("VALUES ")?;
                write_rows(f, rows)?;
                write_where(f, predicate)
            }
        }
    }
}

impl<X: AstExt> Display for RoutineArg<'_, X> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        if let Some(ref mode) = self.mode {
            write!(f, "{mode} ")?;
        }
        if let Some(name) = &self.name {
            write!(f, "{name} ")?;
        }
        self.data_type.fmt(f)?;
        match &self.default {
            Some((true, value)) => write!(f, " = {value}"),
            Some((false, value)) => write!(f, " DEFAULT {value}"),
            None => Ok(()),
        }
    }
}

impl<X: AstExt> Display for RoutineSignature<'_, X> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        self.name.fmt(f)?;
        if let Some(args) = &self.args {
            write!(f, "({})", comma(args))?;
        }
        Ok(())
    }
}

impl<X: AstExt> Display for DropRoutine<'_, X> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        f.write_str(if self.procedure {
            "DROP PROCEDURE "
        } else {
            "DROP FUNCTION "
        })?;
        write_if_exists(f, self.if_exists)?;
        comma(&self.routines).fmt(f)?;
        write_behavior(f, self.behavior)
    }
}

impl<X: AstExt> Display for ShowClause<'_, X> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match self {
            ShowClause::History => f.write_str("HISTORY"),
            ShowClause::Like(text) => write!(f, "LIKE {}", text),
            ShowClause::Where(expr) => write!(f, "WHERE {expr}"),
            ShowClause::StartsWith(text) => write!(f, "STARTS WITH {}", text),
            ShowClause::Limit(expr) => write!(f, "LIMIT {expr}"),
            ShowClause::Pattern(text) => text.fmt(f),
            ShowClause::FromValue(text) => write!(f, "FROM {}", text),
            ShowClause::Location { from, scope, name } => {
                f.write_str(if *from { "FROM" } else { "IN" })?;
                if let Some(scope) = scope {
                    write!(f, " {scope}")?;
                }
                if let Some(name) = name {
                    write!(f, " {name}")?;
                }
                Ok(())
            }
        }
    }
}

impl<X: AstExt> Display for Show<'_, X> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        f.write_str("SHOW")?;
        for word in self.modifiers.iter().chain(&self.object) {
            write!(f, " {word}")?;
        }
        if let Some(name) = &self.name {
            write!(f, " {name}")?;
        }
        for clause in &self.clauses {
            write!(f, " {clause}")?;
        }
        Ok(())
    }
}

impl Display for TransactionMode {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        f.write_str(match self {
            TransactionMode::ReadOnly => "READ ONLY",
            TransactionMode::ReadWrite => "READ WRITE",
            TransactionMode::IsolationLevel(IsolationLevel::ReadUncommitted) => {
                "ISOLATION LEVEL READ UNCOMMITTED"
            }
            TransactionMode::IsolationLevel(IsolationLevel::ReadCommitted) => {
                "ISOLATION LEVEL READ COMMITTED"
            }
            TransactionMode::IsolationLevel(IsolationLevel::RepeatableRead) => {
                "ISOLATION LEVEL REPEATABLE READ"
            }
            TransactionMode::IsolationLevel(IsolationLevel::Serializable) => {
                "ISOLATION LEVEL SERIALIZABLE"
            }
            TransactionMode::IsolationLevel(IsolationLevel::Snapshot) => "ISOLATION LEVEL SNAPSHOT",
        })
    }
}

impl Display for Transaction<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match self {
            Transaction::Start(modes) if modes.is_empty() => f.write_str("START TRANSACTION"),
            Transaction::Start(modes) => write!(f, "START TRANSACTION {}", comma(modes)),
            Transaction::Begin(None) => f.write_str("BEGIN"),
            Transaction::Begin(Some(word)) => write!(f, "BEGIN {word}"),
            Transaction::Finish {
                kind,
                chain,
                savepoint,
            } => {
                f.write_str(match kind {
                    FinishKind::Commit => "COMMIT",
                    FinishKind::End => "END",
                    FinishKind::Rollback => "ROLLBACK",
                    FinishKind::Abort => "ABORT",
                })?;
                if *chain {
                    f.write_str(" AND CHAIN")?;
                }
                match savepoint {
                    Some(savepoint) => write!(f, " TO SAVEPOINT {savepoint}"),
                    None => Ok(()),
                }
            }
            Transaction::Savepoint(name) => write!(f, "SAVEPOINT {name}"),
            Transaction::Release(name) => write!(f, "RELEASE SAVEPOINT {name}"),
        }
    }
}

impl Display for ExplainOption<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        self.name.fmt(f)?;
        match self.value {
            Some(ref value) => write!(f, " {}{value}", self.sign),
            None => Ok(()),
        }
    }
}

impl<X: AstExt> Display for Explain<'_, X> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        f.write_str(&self.keyword)?;
        if !self.options.is_empty() {
            write!(f, " ({})", comma(&self.options))?;
        }
        if let Some(ref detail) = self.detail {
            write!(f, " {detail}")?;
        }
        for (set, word) in [
            (self.analyze, " ANALYZE"),
            (self.verbose, " VERBOSE"),
            (self.estimate, " ESTIMATE"),
            (self.query_plan, " QUERY PLAN"),
        ] {
            if set {
                f.write_str(word)?;
            }
        }
        if let Some(format) = &self.format {
            let separator = if format.equals { "=" } else { " " };
            write!(f, " FORMAT{separator}{}", format.name)?;
        }
        match &self.target {
            ExplainTarget::Statement(statement) => {
                SpaceOrNewline.fmt(f)?;
                statement.fmt(f)
            }
            ExplainTarget::Table {
                table_keyword,
                name,
            } => {
                if *table_keyword {
                    f.write_str(" TABLE")?;
                }
                write!(f, " {name}")
            }
        }
    }
}

fn write_scope(f: &mut Formatter<'_>, scope: Option<&str>) -> Result {
    match scope {
        Some(scope) => write!(f, "{scope} "),
        None => Ok(()),
    }
}

impl<X: AstExt> Display for SetAssignment<'_, X> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write_scope(f, self.scope.as_deref())?;
        write!(f, "{} = {}", self.name, self.value)
    }
}

impl<X: AstExt> Display for SetStatement<'_, X> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        f.write_str("SET ")?;
        match self {
            SetStatement::Variable {
                scope,
                name,
                values,
            } => {
                write_scope(f, scope.as_deref())?;
                write!(f, "{name} = {}", comma(values))
            }
            SetStatement::Assignments(assignments) => comma(assignments).fmt(f),
            SetStatement::Role { scope, role } => {
                write_scope(f, scope.as_deref())?;
                write!(f, "ROLE {role}")
            }
            SetStatement::Authorization { scope, user } => {
                write_scope(f, scope.as_deref())?;
                write!(f, "AUTHORIZATION {user}")
            }
            SetStatement::TimeZone { scope, value } => {
                write_scope(f, scope.as_deref())?;
                write!(f, "TIME ZONE {value}")
            }
            SetStatement::Transaction {
                characteristics,
                modes,
                snapshot,
            } => {
                f.write_str(if *characteristics {
                    "SESSION CHARACTERISTICS AS TRANSACTION"
                } else {
                    "TRANSACTION"
                })?;
                if let Some(snapshot) = snapshot {
                    write!(f, " SNAPSHOT {}", snapshot)?;
                }
                if !modes.is_empty() {
                    write!(f, " {}", comma(modes))?;
                }
                Ok(())
            }
            SetStatement::Names { charset, collate } => {
                write!(f, "NAMES {charset}")?;
                match collate {
                    Some(collate) => write!(f, " COLLATE {collate}"),
                    None => Ok(()),
                }
            }
        }
    }
}

impl Display for CommentOn<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        f.write_str("COMMENT ")?;
        write_if_exists(f, self.if_exists)?;
        f.write_str("ON ")?;
        write_words(f, &self.kind)?;
        write!(f, " {} IS ", self.name)?;
        match &self.comment {
            Some(text) => text.fmt(f),
            None => f.write_str("NULL"),
        }
    }
}

impl Display for Truncate<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        f.write_str(if self.table_keyword {
            "TRUNCATE TABLE "
        } else {
            "TRUNCATE "
        })?;
        write_if_exists(f, self.if_exists)?;
        comma(&self.names).fmt(f)
    }
}

impl<X: AstExt> Display for AlterColumnAction<'_, X> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match self {
            AlterColumnAction::SetNotNull => f.write_str("SET NOT NULL"),
            AlterColumnAction::DropNotNull => f.write_str("DROP NOT NULL"),
            AlterColumnAction::SetDefault(expr) => write!(f, "SET DEFAULT {expr}"),
            AlterColumnAction::DropDefault => f.write_str("DROP DEFAULT"),
            AlterColumnAction::SetType {
                data_type,
                set_data,
                using,
            } => {
                f.write_str(if *set_data { "SET DATA TYPE " } else { "TYPE " })?;
                data_type.fmt(f)?;
                match using {
                    Some(using) => write!(f, " USING {using}"),
                    None => Ok(()),
                }
            }
            AlterColumnAction::AddGenerated(generated) => write!(f, "ADD {generated}"),
            AlterColumnAction::GenericOptions(options) => {
                write!(f, "OPTIONS ({})", comma(options))
            }
        }
    }
}

fn write_position(f: &mut Formatter<'_>, position: &Option<ColumnPosition<'_>>) -> Result {
    match position {
        Some(ColumnPosition::First) => f.write_str(" FIRST"),
        Some(ColumnPosition::After(name)) => write!(f, " AFTER {name}"),
        None => Ok(()),
    }
}

impl<X: AstExt> Display for AlterTableOp<'_, X> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match self {
            AlterTableOp::SetTblProperties(properties) => {
                f.write_str("SET TBLPROPERTIES(")?;
                for (i, (key, value)) in properties.iter().enumerate() {
                    if i > 0 {
                        f.write_str(", ")?;
                    }
                    write!(f, "{key} = {value}")?;
                }
                f.write_str(")")
            }
            AlterTableOp::AddColumn {
                column_keyword,
                if_not_exists,
                column,
                position,
            } => {
                write!(f, "ADD {}", column_word(*column_keyword))?;
                write_if_not_exists(f, *if_not_exists)?;
                column.fmt(f)?;
                write_position(f, position)
            }
            AlterTableOp::Phrase(phrase) => f.write_str(phrase),
            AlterTableOp::PhraseName { phrase, name } => write!(f, "{phrase} {name}"),
            AlterTableOp::Setting {
                name,
                equals,
                value,
            } => {
                let separator = if *equals { " = " } else { " " };
                write!(f, "{name}{separator}{value}")
            }
            AlterTableOp::RenameConstraint { old, new } => {
                write!(f, "RENAME CONSTRAINT {old} TO {new}")
            }
            AlterTableOp::RenameAs(name) => write!(f, "RENAME AS {name}"),
            AlterTableOp::ChangeColumn {
                old,
                column,
                position,
            } => {
                write!(f, "CHANGE COLUMN {old} {column}")?;
                write_position(f, position)
            }
            AlterTableOp::ModifyColumn { column, position } => {
                write!(f, "MODIFY COLUMN {column}")?;
                write_position(f, position)
            }
            AlterTableOp::SwapWith(name) => write!(f, "SWAP WITH {name}"),
            AlterTableOp::PhraseObject { phrase, name } => write!(f, "{phrase} {name}"),
            AlterTableOp::AttachPartition { name, bound } => {
                write!(f, "ATTACH PARTITION {name} {bound}")
            }
            AlterTableOp::DetachPartition { name, mode } => {
                write!(f, "DETACH PARTITION {name}")?;
                match mode {
                    Some(mode) => write!(f, " {mode}"),
                    None => Ok(()),
                }
            }
            AlterTableOp::GenericOptions(options) => write!(f, "OPTIONS ({})", comma(options)),
            AlterTableOp::ResetOptions(names) => write!(f, "RESET ({})", comma(names)),
            AlterTableOp::ClusterBy(exprs) => write!(f, "CLUSTER BY ({})", comma(exprs)),
            AlterTableOp::AlterSortKey(columns) => {
                write!(f, "ALTER SORTKEY({})", comma(columns))
            }
            AlterTableOp::SetOptions(options) => {
                f.write_str("SET ")?;
                write_storage(f, options)
            }
            AlterTableOp::DropColumn {
                column_keyword,
                if_exists,
                name,
                behavior,
            } => {
                write!(f, "DROP {}", column_word(*column_keyword))?;
                write_if_exists(f, *if_exists)?;
                name.fmt(f)?;
                write_behavior(f, *behavior)
            }
            AlterTableOp::DropColumns {
                column_keyword,
                if_exists,
                names,
                behavior,
            } => {
                write!(f, "DROP {}", column_word(*column_keyword))?;
                write_if_exists(f, *if_exists)?;
                comma(names).fmt(f)?;
                write_behavior(f, *behavior)
            }
            AlterTableOp::DropForeignKey { name, behavior } => {
                write!(f, "DROP FOREIGN KEY {name}")?;
                write_behavior(f, *behavior)
            }
            AlterTableOp::RenameColumn { old, new } => write!(f, "RENAME COLUMN {old} TO {new}"),
            AlterTableOp::RenameTable(name) => write!(f, "RENAME TO {name}"),
            AlterTableOp::AddPartitions {
                if_not_exists,
                partitions,
            } => {
                f.write_str("ADD")?;
                if *if_not_exists {
                    f.write_str(" IF NOT EXISTS")?;
                }
                for partition in partitions {
                    write!(f, " PARTITION ({})", comma(partition))?;
                }
                Ok(())
            }
            AlterTableOp::DropPartition {
                if_exists,
                partition,
            } => {
                let if_exists = if *if_exists { "IF EXISTS " } else { "" };
                write!(f, "DROP {if_exists}PARTITION ({})", comma(partition))
            }
            AlterTableOp::RenamePartition { old, new, location } => {
                write!(
                    f,
                    "PARTITION ({}) RENAME TO PARTITION ({})",
                    comma(old),
                    comma(new)
                )?;
                if let Some((set, path)) = location {
                    let set = if *set { "SET " } else { "" };
                    write!(f, " {set}LOCATION {}", path)?;
                }
                Ok(())
            }
            AlterTableOp::AlterColumn {
                column_keyword,
                name,
                action,
            } => write!(f, "ALTER {}{name} {action}", column_word(*column_keyword)),
            AlterTableOp::AddConstraint(constraint) => write!(f, "ADD {constraint}"),
            AlterTableOp::DropConstraint {
                if_exists,
                name,
                behavior,
            } => {
                f.write_str("DROP CONSTRAINT ")?;
                write_if_exists(f, *if_exists)?;
                name.fmt(f)?;
                write_behavior(f, *behavior)
            }
        }
    }
}

impl<X: AstExt> Display for AlterTable<'_, X> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        f.write_str("ALTER TABLE ")?;
        write_if_exists(f, self.if_exists)?;
        if self.only {
            f.write_str("ONLY ")?;
        }
        write!(f, "{} {}", self.name, comma(&self.operations))
    }
}

/// Writes ` AS` and a view's query, which starts on a new line in `{:#}`.
pub fn write_view_query<X: AstExt>(f: &mut Formatter<'_>, query: &Query<'_, X>) -> Result {
    f.write_str(" AS")?;
    SpaceOrNewline.fmt(f)?;
    query.fmt(f)
}

pub fn write_create_flags(f: &mut Formatter<'_>, or_replace: bool, temporary: bool) -> Result {
    f.write_str("CREATE ")?;
    if or_replace {
        f.write_str("OR REPLACE ")?;
    }
    if temporary {
        f.write_str("TEMPORARY ")?;
    }
    Ok(())
}

impl<X: AstExt> Display for CreateView<'_, X> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write_create_flags(f, self.or_replace, self.temporary)?;
        if self.materialized {
            f.write_str("MATERIALIZED ")?;
        }
        f.write_str("VIEW ")?;
        write_if_not_exists(f, self.if_not_exists)?;
        self.name.fmt(f)?;
        if !self.columns.is_empty() {
            write!(f, " ({})", comma(&self.columns))?;
        }
        if !self.with.is_empty() {
            f.write_str(" WITH ")?;
            write_storage(f, &self.with)?;
        }
        write_view_query(f, &self.query)?;
        if self.no_schema_binding {
            f.write_str(" WITH NO SCHEMA BINDING")?;
        }
        Ok(())
    }
}

impl<X: AstExt> Display for CreateIndex<'_, X> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        f.write_str(if self.unique {
            "CREATE UNIQUE INDEX "
        } else {
            "CREATE INDEX "
        })?;
        if self.concurrently {
            f.write_str("CONCURRENTLY ")?;
        }
        if self.asynchronous {
            f.write_str("ASYNC ")?;
        }
        write_if_not_exists(f, self.if_not_exists)?;
        if let Some(name) = &self.name {
            name.fmt(f)?;
            if self.using_before_on.is_some() {
                f.write_str(" ")?;
            }
        }
        if let Some(using) = &self.using_before_on {
            write!(f, "USING {using}")?;
        }
        if self.name.is_some() || self.using_before_on.is_some() {
            SpaceOrNewline.fmt(f)?;
        }
        write!(f, "ON {}", self.table)?;
        if let Some(using) = &self.using {
            write!(f, " USING {using}")?;
        }
        write!(f, " ({})", comma(&self.columns))?;
        if let Some(using) = &self.trailing_using {
            write!(f, " USING {using}")?;
        }
        if !self.include.is_empty() {
            write!(f, " INCLUDE ({})", comma(&self.include))?;
        }
        f.write_str(match self.nulls_distinct {
            Some(true) => " NULLS DISTINCT",
            Some(false) => " NULLS NOT DISTINCT",
            None => "",
        })?;
        if !self.with.is_empty() {
            f.write_str(" WITH ")?;
            write_storage(f, &self.with)?;
        }
        write_where(f, &self.predicate)?;
        for (name, value) in &self.settings {
            write!(f, " {name} = {value}")?;
        }
        Ok(())
    }
}

fn write_table_values<X: AstExt>(f: &mut Formatter<'_>, rows: &[Vec<Expr<'_, X>>]) -> Result {
    f.write_str("VALUES ")?;
    write_rows(f, rows)
}

pub fn write_storage<X: AstExt>(f: &mut Formatter<'_>, parameters: &IndexStorage<'_, X>) -> Result {
    f.write_str("(")?;
    for (i, (name, value)) in parameters.iter().enumerate() {
        if i > 0 {
            f.write_str(", ")?;
        }
        name.fmt(f)?;
        if let Some(value) = value {
            write!(f, " = {value}")?;
        }
    }
    f.write_str(")")
}

impl<X: AstExt> Display for IndexColumn<'_, X> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        self.item.expr.fmt(f)?;
        if let Some(class) = &self.operator_class {
            write!(f, " {class}")?;
        }
        write_order_modifiers(f, &self.item)
    }
}

impl Display for CreateSchema<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        f.write_str(if self.or_replace {
            "CREATE OR REPLACE SCHEMA "
        } else {
            "CREATE SCHEMA "
        })?;
        write_if_not_exists(f, self.if_not_exists)?;
        if let Some(name) = &self.name {
            name.fmt(f)?;
        }
        if let Some(owner) = &self.authorization {
            let space = if self.name.is_some() { " " } else { "" };
            write!(f, "{space}AUTHORIZATION {owner}")?;
        }
        if let Some(source) = &self.clone {
            write!(f, " CLONE {source}")?;
        }
        Ok(())
    }
}

impl<X: AstExt> Display for AlterView<'_, X> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(f, "ALTER VIEW {}", self.name)?;
        if !self.columns.is_empty() {
            write!(f, " ({})", comma(&self.columns))?;
        }
        write!(f, " AS {}", self.query)
    }
}

impl Display for CreateDatabase<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        f.write_str("CREATE DATABASE ")?;
        write_if_not_exists(f, self.if_not_exists)?;
        self.name.fmt(f)
    }
}

impl<X: AstExt> Display for crate::ast::Expr<'_, X> {
    #[inline(always)]
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.kind, f)
    }
}

impl<X: AstExt> Display for crate::ast::Statement<'_, X> {
    #[inline(always)]
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.kind, f)
    }
}

impl<X: AstExt> Display for crate::ast::SelectItem<'_, X> {
    #[inline(always)]
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.kind, f)
    }
}

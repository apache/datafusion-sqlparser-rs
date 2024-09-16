use core::iter;

use crate::ast;
use crate::tokenizer::Span;

use super::{Expr, Join, Query, Select, SelectItem, SetExpr, TableFactor, TableWithJoins};

pub trait Spanned {
    fn span(&self) -> Span;
}

impl Spanned for Query {
    fn span(&self) -> Span {
        self.body.span()
    }
}

impl Spanned for SetExpr {
    fn span(&self) -> Span {
        match self {
            SetExpr::Select(select) => select.span(),
            SetExpr::Query(query) => todo!(),
            SetExpr::SetOperation {
                op,
                set_quantifier,
                left,
                right,
            } => todo!(),
            SetExpr::Values(values) => todo!(),
            SetExpr::Insert(statement) => todo!(),
            SetExpr::Table(table) => todo!(),
        }
    }
}

impl Spanned for Expr {
    fn span(&self) -> Span {
        match self {
            Expr::Identifier(ident) => ident.span,
            Expr::CompoundIdentifier(vec) => todo!(),
            Expr::JsonAccess {
                left,
                operator,
                right,
            } => todo!(),
            Expr::CompositeAccess { expr, key } => todo!(),
            Expr::IsFalse(expr) => todo!(),
            Expr::IsNotFalse(expr) => todo!(),
            Expr::IsTrue(expr) => todo!(),
            Expr::IsNotTrue(expr) => todo!(),
            Expr::IsNull(expr) => todo!(),
            Expr::IsNotNull(expr) => todo!(),
            Expr::IsUnknown(expr) => todo!(),
            Expr::IsNotUnknown(expr) => todo!(),
            Expr::IsDistinctFrom(expr, expr1) => todo!(),
            Expr::IsNotDistinctFrom(expr, expr1) => todo!(),
            Expr::InList {
                expr,
                list,
                negated,
            } => todo!(),
            Expr::InSubquery {
                expr,
                subquery,
                negated,
            } => todo!(),
            Expr::InUnnest {
                expr,
                array_expr,
                negated,
            } => todo!(),
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => todo!(),
            Expr::BinaryOp { left, op, right } => todo!(),
            Expr::Like {
                negated,
                expr,
                pattern,
                escape_char,
            } => todo!(),
            Expr::ILike {
                negated,
                expr,
                pattern,
                escape_char,
            } => todo!(),
            Expr::SimilarTo {
                negated,
                expr,
                pattern,
                escape_char,
            } => todo!(),
            Expr::AnyOp(expr) => todo!(),
            Expr::AllOp(expr) => todo!(),
            Expr::UnaryOp { op, expr } => todo!(),
            Expr::Cast { expr, data_type } => todo!(),
            Expr::TryCast { expr, data_type } => todo!(),
            Expr::SafeCast { expr, data_type } => todo!(),
            Expr::AtTimeZone {
                timestamp,
                time_zone,
            } => todo!(),
            Expr::Extract { field, expr } => todo!(),
            Expr::Ceil { expr, field } => todo!(),
            Expr::Floor { expr, field } => todo!(),
            Expr::Position { expr, r#in } => todo!(),
            Expr::Substring {
                expr,
                substring_from,
                substring_for,
            } => todo!(),
            Expr::Trim {
                expr,
                trim_where,
                trim_what,
            } => todo!(),
            Expr::Overlay {
                expr,
                overlay_what,
                overlay_from,
                overlay_for,
            } => todo!(),
            Expr::Collate { expr, collation } => todo!(),
            Expr::Nested(expr) => todo!(),
            Expr::Value(value) => todo!(),
            Expr::TypedString { data_type, value } => todo!(),
            Expr::MapAccess { column, keys } => todo!(),
            Expr::Function(function) => todo!(),
            Expr::AggregateExpressionWithFilter { expr, filter } => todo!(),
            Expr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => todo!(),
            Expr::Exists { subquery, negated } => todo!(),
            Expr::Subquery(query) => todo!(),
            Expr::ArraySubquery(query) => todo!(),
            Expr::ListAgg(list_agg) => todo!(),
            Expr::ArrayAgg(array_agg) => todo!(),
            Expr::GroupingSets(vec) => todo!(),
            Expr::Cube(vec) => todo!(),
            Expr::Rollup(vec) => todo!(),
            Expr::Tuple(vec) => todo!(),
            Expr::ArrayIndex { obj, indexes } => todo!(),
            Expr::Array(array) => todo!(),
            Expr::Interval {
                value,
                leading_field,
                leading_precision,
                last_field,
                fractional_seconds_precision,
            } => todo!(),
            Expr::MatchAgainst {
                columns,
                match_value,
                opt_search_modifier,
            } => todo!(),
        }
    }
}

impl Spanned for SelectItem {
    fn span(&self) -> Span {
        match self {
            SelectItem::UnnamedExpr(expr) => expr.span(),
            SelectItem::ExprWithAlias { expr, alias } => expr.span().union(&alias.span),
            SelectItem::QualifiedWildcard(object_name, wildcard_additional_options) => object_name
                .0
                .iter()
                .map(|i| i.span)
                .reduce(|acc, item| acc.union(&item))
                .expect("Empty iterator"),
            SelectItem::Wildcard(wildcard_additional_options) => todo!(),
        }
    }
}

impl Spanned for TableFactor {
    fn span(&self) -> Span {
        match self {
            TableFactor::Table {
                name,
                alias,
                args,
                with_hints,
            } => union_spans(
                name.0.iter().map(|i| i.span).chain(
                    alias
                        .as_ref()
                        .map(|alias| {
                            union_spans(
                                iter::once(alias.name.span)
                                    .chain(alias.columns.iter().map(|i| i.span)),
                            )
                        })
                        .into_iter(),
                ),
            ),
            TableFactor::Derived {
                lateral,
                subquery,
                alias,
            } => todo!(),
            TableFactor::TableFunction { expr, alias } => todo!(),
            TableFactor::UNNEST {
                alias,
                array_expr,
                with_offset,
                with_offset_alias,
            } => todo!(),
            TableFactor::NestedJoin {
                table_with_joins,
                alias,
            } => todo!(),
        }
    }
}

impl Spanned for Join {
    fn span(&self) -> Span {
        todo!()
    }
}

impl Spanned for TableWithJoins {
    fn span(&self) -> Span {
        union_spans(
            core::iter::once(self.relation.span()).chain(self.joins.iter().map(|item| item.span())),
        )
    }
}

pub fn union_spans<I: Iterator<Item = Span>>(iter: I) -> Span {
    iter.reduce(|acc, item| acc.union(&item))
        .expect("Empty iterator")
}

impl Spanned for Select {
    fn span(&self) -> Span {
        union_spans(
            core::iter::once(self.select_token.span.clone())
                .chain(self.projection.iter().map(|item| item.span()))
                .chain(self.from.iter().map(|item| item.span())),
        )
    }
}

/**
 * TODO:
 *
 * - CTE
 * - With
 * - SetExpr
 * - Fetch
 * - Lock Clause
 */
struct Ignore;

#[cfg(test)]
pub mod tests {
    use crate::dialect::{Dialect, GenericDialect};
    use crate::tokenizer::Span;

    use super::*;

    #[test]
    fn test_span() {
        let query = crate::parser::Parser::new(&GenericDialect::default())
            .try_with_sql("SELECT id, name FROM users")
            .unwrap()
            .parse_query()
            .unwrap();

        dbg!(&query);

        assert_eq!(
            query.span(),
            Span::new((1, 1).into(), (1, 54 - 28 + 1).into())
        );
    }
}

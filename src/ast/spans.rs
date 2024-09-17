use core::iter;

use crate::ast;
use crate::tokenizer::Span;

use super::{Cte, Expr, FunctionArg, FunctionArgExpr, Join, JoinConstraint, JoinOperator, JsonPath, JsonPathElem, Query, Select, SelectItem, SetExpr, TableAlias, TableFactor, TableWithJoins, With};

pub trait Spanned {
    fn span(&self) -> Span;
}

impl Spanned for Query {
    fn span(&self) -> Span {
        union_spans(
            self.with.iter().map(|item| item.span())
                .chain(core::iter::once(self.body.span()))
        )
    }
}

impl Spanned for With {
    fn span(&self) -> Span {
        union_spans(
            core::iter::once(self.with_token.span.clone())
                .chain(self.cte_tables.iter().map(|item| item.span()))
        )
    }
}

impl Spanned for Cte {
    fn span(&self) -> Span {
        union_spans(
            core::iter::once(self.alias.span())
                .chain(core::iter::once(self.query.span()))
                .chain(self.from.iter().map(|item| item.span))
        )
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
            } => left.span().union(&right.span()),
            SetExpr::Values(values) => todo!(),
            SetExpr::Insert(statement) => todo!(),
            SetExpr::Table(table) => todo!(),
            SetExpr::Update(statement) => todo!(),
        }
    }
}

impl Spanned for Expr {
    fn span(&self) -> Span {
        match self {
            Expr::Identifier(ident) => ident.span,
            Expr::CompoundIdentifier(vec) => union_spans(vec.iter().map(|i| i.span)),
            Expr::CompositeAccess { expr, key } => expr.span().union(&key.span),
            Expr::IsFalse(expr) => expr.span(),
            Expr::IsNotFalse(expr) => expr.span(),
            Expr::IsTrue(expr) => expr.span(),
            Expr::IsNotTrue(expr) => expr.span(),
            Expr::IsNull(expr) => expr.span(),
            Expr::IsNotNull(expr) => expr.span(),
            Expr::IsUnknown(expr) => expr.span(),
            Expr::IsNotUnknown(expr) => expr.span(),
            Expr::IsDistinctFrom(lhs, rhs) => lhs.span().union(&rhs.span()),
            Expr::IsNotDistinctFrom(lhs, rhs) => lhs.span().union(&rhs.span()),
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
            Expr::BinaryOp { left, op, right } => left.span().union(&right.span()),
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
            Expr::Ceil { expr, field } => todo!(),
            Expr::Floor { expr, field } => todo!(),
            Expr::Position { expr, r#in } => todo!(),
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
            Expr::GroupingSets(vec) => todo!(),
            Expr::Cube(vec) => todo!(),
            Expr::Rollup(vec) => todo!(),
            Expr::Tuple(vec) => todo!(),
            Expr::Array(array) => todo!(),
            Expr::MatchAgainst {
                columns,
                match_value,
                opt_search_modifier,
            } => todo!(),
            Expr::JsonAccess { value, path } => value.span().union(&path.span()),
            Expr::RLike { negated, expr, pattern, regexp } => todo!(),
            Expr::AnyOp { left, compare_op, right } => todo!(),
            Expr::AllOp { left, compare_op, right } => todo!(),
            Expr::UnaryOp { op, expr } => todo!(),
            Expr::Convert { expr, data_type, charset, target_before_value, styles } => todo!(),
            Expr::Cast { kind, expr, data_type, format } => expr.span(),
            Expr::AtTimeZone { timestamp, time_zone } => todo!(),
            Expr::Extract { field, syntax, expr } => todo!(),
            Expr::Substring { expr, substring_from, substring_for, special } => todo!(),
            Expr::Trim { expr, trim_where, trim_what, trim_characters } => todo!(),
            Expr::IntroducedString { introducer, value } => todo!(),
            Expr::Case { operand, conditions, results, else_result } => todo!(),
            Expr::Exists { subquery, negated } => todo!(),
            Expr::Subquery(query) => todo!(),
            Expr::Struct { values, fields } => todo!(),
            Expr::Named { expr, name } => todo!(),
            Expr::Dictionary(vec) => todo!(),
            Expr::Map(map) => todo!(),
            Expr::Subscript { expr, subscript } => todo!(),
            Expr::Interval(interval) => todo!(),
            Expr::Wildcard => todo!(),
            Expr::QualifiedWildcard(object_name) => todo!(),
            Expr::OuterJoin(expr) => todo!(),
            Expr::Prior(expr) => todo!(),
            Expr::Lambda(lambda_function) => todo!(),
        }
    }
}

impl Spanned for JsonPath {
    fn span(&self) -> Span {
        union_spans(self.path.iter().map(|i| i.span()))
    }
}


impl Spanned for JsonPathElem {
    fn span(&self) -> Span {
        match self {
            JsonPathElem::Dot { key, quoted } => Span::empty(),
            JsonPathElem::Bracket { key } => key.span(),
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
                version,
                with_ordinality,
                partitions,
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
            } => subquery.span().union_opt(&alias.as_ref().map(|alias| alias.span())),
            TableFactor::TableFunction { expr, alias } => todo!(),
            TableFactor::UNNEST {
                alias,
                with_offset,
                with_offset_alias,
                array_exprs,
                with_ordinality,
            } => todo!(),
            TableFactor::NestedJoin {
                table_with_joins,
                alias,
            } => todo!(),
            TableFactor::Function { lateral, name, args, alias } => union_spans(
                name.0.iter().map(|i| i.span)
                    .chain(args.iter().map(|i| i.span()))
                    .chain(alias.as_ref().map(|alias| alias.span())),
            ),
            TableFactor::JsonTable { json_expr, json_path, columns, alias } => todo!(),
            TableFactor::Pivot { table, aggregate_functions, value_column, value_source, default_on_null, alias } => todo!(),
            TableFactor::Unpivot { table, value, name, columns, alias } => todo!(),
            TableFactor::MatchRecognize { table, partition_by, order_by, measures, rows_per_match, after_match_skip, pattern, symbols, alias } => todo!(),
        }
    }
}


impl Spanned for FunctionArg {
    fn span(&self) -> Span {
        match self {
            FunctionArg::Named { name, arg, operator } => {
                name.span.union(&arg.span())
            },
            FunctionArg::Unnamed(arg) => arg.span(),
        }
    }
}

impl Spanned for FunctionArgExpr {
    fn span(&self) -> Span {
        match self {
            FunctionArgExpr::Expr(expr) => expr.span(),
            FunctionArgExpr::QualifiedWildcard(object_name) => union_spans(object_name.0.iter().map(|i| i.span)),
            FunctionArgExpr::Wildcard => Span::empty(),
        }
    }
}

impl Spanned for TableAlias {
    fn span(&self) -> Span {
        union_spans(
            iter::once(self.name.span)
                .chain(self.columns.iter().map(|i| i.span))
        )
    }
}

impl Spanned for Join {
    fn span(&self) -> Span {
        self.relation.span().union(&self.join_operator.span())
    }
}

impl Spanned for JoinOperator {
    fn span(&self) -> Span {
        match self {
            JoinOperator::Inner(join_constraint) => join_constraint.span(),
            JoinOperator::LeftOuter(join_constraint) => join_constraint.span(),
            JoinOperator::RightOuter(join_constraint) => join_constraint.span(),
            JoinOperator::FullOuter(join_constraint) => join_constraint.span(),
            JoinOperator::CrossJoin => Span::empty(),
            JoinOperator::LeftSemi(join_constraint) => join_constraint.span(),
            JoinOperator::RightSemi(join_constraint) => join_constraint.span(),
            JoinOperator::LeftAnti(join_constraint) => join_constraint.span(),
            JoinOperator::RightAnti(join_constraint) => join_constraint.span(),
            JoinOperator::CrossApply => Span::empty(),
            JoinOperator::OuterApply => Span::empty(),
            JoinOperator::AsOf { match_condition, constraint } => todo!(),
        }
    }
}

impl Spanned for JoinConstraint {
    fn span(&self) -> Span {
        match self {
            JoinConstraint::On(expr) => expr.span(),
            JoinConstraint::Using(vec) => union_spans(vec.iter().map(|i| i.span)),
            JoinConstraint::Natural => Span::empty(),
            JoinConstraint::None => Span::empty(),
        }
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

#[cfg(test)]
pub mod tests {
    use ast::query;

    use crate::dialect::{Dialect, GenericDialect, SnowflakeDialect};
    use crate::tokenizer::Span;

    use super::*;

    #[test]
    fn test_query_span() {
        let query = crate::parser::Parser::new(&GenericDialect::default())
            .try_with_sql("SELECT id, name FROM users LEFT JOIN companies ON users.company_id = companies.id")
            .unwrap()
            .parse_query()
            .unwrap();

        assert_eq!(
            query.span(),
            Span::new((1, 1).into(), (1, 109 - 28 + 1).into())
        );
    }


    #[test]
    pub fn test_union() {
        let query = crate::parser::Parser::new(&GenericDialect::default())
            .try_with_sql("SELECT a FROM postgres.public.source UNION SELECT a FROM postgres.public.source")
            .unwrap()
            .parse_query()
            .unwrap();

        query.span();
    }

    #[test]
    pub fn test_subquery() {
        let query = crate::parser::Parser::new(&GenericDialect::default())
            .try_with_sql("SELECT a FROM (SELECT a FROM postgres.public.source) AS b")
            .unwrap()
            .parse_query()
            .unwrap();

        query.span();
    }

    #[test]
    pub fn test_cte() {
        let query = crate::parser::Parser::new(&GenericDialect::default())
            .try_with_sql("WITH cte_outer AS (SELECT a FROM postgres.public.source), cte_ignored AS (SELECT a FROM cte_outer), cte_inner AS (SELECT a FROM cte_outer) SELECT a FROM cte_inner")
            .unwrap()
            .parse_query()
            .unwrap();

        query.span();
    }

    #[test]
    pub fn test_snowflake_lateral_flatten() {
        let query = crate::parser::Parser::new(&SnowflakeDialect::default())
            .try_with_sql("SELECT FLATTENED.VALUE:field::TEXT AS FIELD FROM SNOWFLAKE.SCHEMA.SOURCE AS S, LATERAL FLATTEN(INPUT => S.JSON_ARRAY) AS FLATTENED")
            .unwrap()
            .parse_query()
            .unwrap();

        query.span();
    }

    #[test]
    pub fn test_wildcard_from_cte() {
        let query = crate::parser::Parser::new(&GenericDialect::default())
            .try_with_sql("WITH cte AS (SELECT a FROM postgres.public.source) SELECT cte.* FROM cte")
            .unwrap()
            .parse_query()
            .unwrap();

        query.span();
    }
}

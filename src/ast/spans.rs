use core::iter;

use crate::ast;
use crate::tokenizer::Span;

use super::{
    Cte, ExceptSelectItem, ExcludeSelectItem, Expr, Function, FunctionArg, FunctionArgExpr,
    FunctionArgumentClause, FunctionArgumentList, FunctionArguments, HavingBound, IlikeSelectItem,
    Join, JoinConstraint, JoinOperator, JsonPath, JsonPathElem, Query, RenameSelectItem,
    ReplaceSelectElement, ReplaceSelectItem, Select, SelectItem, SetExpr, TableAlias, TableFactor,
    TableWithJoins, Value, WildcardAdditionalOptions, With,
};

pub trait Spanned {
    fn span(&self) -> Span;
}

impl Spanned for Query {
    fn span(&self) -> Span {
        union_spans(
            self.with
                .iter()
                .map(|item| item.span())
                .chain(core::iter::once(self.body.span())),
        )
    }
}

impl Spanned for With {
    fn span(&self) -> Span {
        union_spans(
            core::iter::once(self.with_token.span.clone())
                .chain(self.cte_tables.iter().map(|item| item.span())),
        )
    }
}

impl Spanned for Cte {
    fn span(&self) -> Span {
        union_spans(
            core::iter::once(self.alias.span())
                .chain(core::iter::once(self.query.span()))
                .chain(self.from.iter().map(|item| item.span)),
        )
    }
}

impl Spanned for SetExpr {
    fn span(&self) -> Span {
        match self {
            SetExpr::Select(select) => select.span(),
            SetExpr::Query(query) => query.span(),
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
            } => union_spans(
                core::iter::once(expr.span()).chain(list.iter().map(|item| item.span())),
            ),
            Expr::InSubquery {
                expr,
                subquery,
                negated,
            } => expr.span().union(&subquery.span()),
            Expr::InUnnest {
                expr,
                array_expr,
                negated,
            } => expr.span().union(&array_expr.span()),
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => expr.span().union(&low.span()).union(&high.span()),

            Expr::BinaryOp { left, op, right } => left.span().union(&right.span()),
            Expr::Like {
                negated,
                expr,
                pattern,
                escape_char,
            } => expr.span().union(&pattern.span()),
            Expr::ILike {
                negated,
                expr,
                pattern,
                escape_char,
            } => expr.span().union(&pattern.span()),
            Expr::SimilarTo {
                negated,
                expr,
                pattern,
                escape_char,
            } => expr.span().union(&pattern.span()),
            Expr::Ceil { expr, field } => expr.span(),
            Expr::Floor { expr, field } => expr.span(),
            Expr::Position { expr, r#in } => expr.span().union(&r#in.span()),
            Expr::Overlay {
                expr,
                overlay_what,
                overlay_from,
                overlay_for,
            } => expr
                .span()
                .union(&overlay_what.span())
                .union(&overlay_from.span())
                .union_opt(&overlay_for.as_ref().map(|i| i.span())),
            Expr::Collate { expr, collation } => expr
                .span()
                .union(&union_spans(collation.0.iter().map(|i| i.span))),
            Expr::Nested(expr) => expr.span(),
            Expr::Value(value) => value.span(),
            Expr::TypedString { data_type, value } => todo!(),
            Expr::MapAccess { column, keys } => todo!(),
            Expr::Function(function) => function.span(),
            Expr::GroupingSets(vec) => {
                union_spans(vec.iter().map(|i| i.iter().map(|k| k.span())).flatten())
            }
            Expr::Cube(vec) => {
                union_spans(vec.iter().map(|i| i.iter().map(|k| k.span())).flatten())
            }
            Expr::Rollup(vec) => {
                union_spans(vec.iter().map(|i| i.iter().map(|k| k.span())).flatten())
            }
            Expr::Tuple(vec) => union_spans(vec.iter().map(|i| i.span())),
            Expr::Array(array) => todo!(),
            Expr::MatchAgainst {
                columns,
                match_value,
                opt_search_modifier,
            } => todo!(),
            Expr::JsonAccess { value, path } => value.span().union(&path.span()),
            Expr::RLike {
                negated,
                expr,
                pattern,
                regexp,
            } => todo!(),
            Expr::AnyOp {
                left,
                compare_op,
                right,
            } => todo!(),
            Expr::AllOp {
                left,
                compare_op,
                right,
            } => todo!(),
            Expr::UnaryOp { op, expr } => expr.span(),
            Expr::Convert {
                expr,
                data_type,
                charset,
                target_before_value,
                styles,
            } => todo!(),
            Expr::Cast {
                kind,
                expr,
                data_type,
                format,
            } => expr.span(),
            Expr::AtTimeZone {
                timestamp,
                time_zone,
            } => todo!(),
            Expr::Extract {
                field,
                syntax,
                expr,
            } => todo!(),
            Expr::Substring {
                expr,
                substring_from,
                substring_for,
                special,
            } => union_spans(
                core::iter::once(expr.span())
                    .chain(substring_from.as_ref().map(|i| i.span()))
                    .chain(substring_for.as_ref().map(|i| i.span())),
            ),
            Expr::Trim {
                expr,
                trim_where,
                trim_what,
                trim_characters,
            } => union_spans(
                core::iter::once(expr.span())
                    .chain(trim_what.as_ref().map(|i| i.span()))
                    .chain(
                        trim_characters
                            .as_ref()
                            .map(|items| union_spans(items.iter().map(|i| i.span()))),
                    ),
            ),
            Expr::IntroducedString { introducer, value } => value.span(),
            Expr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => union_spans(
                operand
                    .as_ref()
                    .map(|i| i.span())
                    .into_iter()
                    .chain(conditions.iter().map(|i| i.span()))
                    .chain(results.iter().map(|i| i.span()))
                    .chain(else_result.as_ref().map(|i| i.span())),
            ),
            Expr::Exists { subquery, negated } => subquery.span(),
            Expr::Subquery(query) => query.span(),
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

impl Spanned for Function {
    fn span(&self) -> Span {
        union_spans(
            self.name
                .0
                .iter()
                .map(|i| i.span)
                .chain(iter::once(self.args.span())),
        )
    }
}

impl Spanned for FunctionArguments {
    fn span(&self) -> Span {
        match self {
            FunctionArguments::None => Span::empty(),
            FunctionArguments::Subquery(query) => query.span(),
            FunctionArguments::List(list) => list.span(),
        }
    }
}

impl Spanned for FunctionArgumentList {
    fn span(&self) -> Span {
        union_spans(
            // # todo: duplicate-treatment span
            self.args
                .iter()
                .map(|i| i.span())
                .chain(self.clauses.iter().map(|i| i.span())),
        )
    }
}

impl Spanned for FunctionArgumentClause {
    fn span(&self) -> Span {
        match self {
            FunctionArgumentClause::IgnoreOrRespectNulls(null_treatment) => Span::empty(),
            FunctionArgumentClause::OrderBy(vec) => union_spans(vec.iter().map(|i| i.expr.span())),
            FunctionArgumentClause::Limit(expr) => expr.span(),
            FunctionArgumentClause::OnOverflow(list_agg_on_overflow) => Span::empty(),
            FunctionArgumentClause::Having(HavingBound(kind, expr)) => expr.span(),
            FunctionArgumentClause::Separator(value) => value.span(),
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
            SelectItem::QualifiedWildcard(object_name, wildcard_additional_options) => union_spans(
                object_name
                    .0
                    .iter()
                    .map(|i| i.span)
                    .chain(iter::once(wildcard_additional_options.span())),
            ),
            SelectItem::Wildcard(wildcard_additional_options) => wildcard_additional_options.span(),
        }
    }
}

impl Spanned for WildcardAdditionalOptions {
    fn span(&self) -> Span {
        union_spans(
            self.opt_ilike
                .as_ref()
                .map(|i| i.span())
                .into_iter()
                .chain(self.opt_exclude.as_ref().map(|i| i.span()).into_iter())
                .chain(self.opt_rename.as_ref().map(|i| i.span()).into_iter())
                .chain(self.opt_replace.as_ref().map(|i| i.span()).into_iter())
                .chain(self.opt_except.as_ref().map(|i| i.span()).into_iter()),
        )
    }
}

impl Spanned for IlikeSelectItem {
    fn span(&self) -> Span {
        Span::empty() // # todo: missing span
    }
}

impl Spanned for ExcludeSelectItem {
    fn span(&self) -> Span {
        match self {
            ExcludeSelectItem::Single(ident) => ident.span,
            ExcludeSelectItem::Multiple(vec) => union_spans(vec.iter().map(|i| i.span)),
        }
    }
}

impl Spanned for RenameSelectItem {
    fn span(&self) -> Span {
        match self {
            RenameSelectItem::Single(ident) => ident.ident.span.union(&ident.alias.span),
            RenameSelectItem::Multiple(vec) => {
                union_spans(vec.iter().map(|i| i.ident.span.union(&i.alias.span)))
            }
        }
    }
}

impl Spanned for ExceptSelectItem {
    fn span(&self) -> Span {
        union_spans(
            iter::once(self.first_element.span)
                .chain(self.additional_elements.iter().map(|i| i.span)),
        )
    }
}

impl Spanned for ReplaceSelectItem {
    fn span(&self) -> Span {
        union_spans(self.items.iter().map(|i| i.span()))
    }
}

impl Spanned for ReplaceSelectElement {
    fn span(&self) -> Span {
        self.expr.span().union(&self.column_name.span)
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
            } => subquery
                .span()
                .union_opt(&alias.as_ref().map(|alias| alias.span())),
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
            TableFactor::Function {
                lateral,
                name,
                args,
                alias,
            } => union_spans(
                name.0
                    .iter()
                    .map(|i| i.span)
                    .chain(args.iter().map(|i| i.span()))
                    .chain(alias.as_ref().map(|alias| alias.span())),
            ),
            TableFactor::JsonTable {
                json_expr,
                json_path,
                columns,
                alias,
            } => todo!(),
            TableFactor::Pivot {
                table,
                aggregate_functions,
                value_column,
                value_source,
                default_on_null,
                alias,
            } => todo!(),
            TableFactor::Unpivot {
                table,
                value,
                name,
                columns,
                alias,
            } => todo!(),
            TableFactor::MatchRecognize {
                table,
                partition_by,
                order_by,
                measures,
                rows_per_match,
                after_match_skip,
                pattern,
                symbols,
                alias,
            } => todo!(),
        }
    }
}

impl Spanned for FunctionArg {
    fn span(&self) -> Span {
        match self {
            FunctionArg::Named {
                name,
                arg,
                operator,
            } => name.span.union(&arg.span()),
            FunctionArg::Unnamed(arg) => arg.span(),
        }
    }
}

impl Spanned for FunctionArgExpr {
    fn span(&self) -> Span {
        match self {
            FunctionArgExpr::Expr(expr) => expr.span(),
            FunctionArgExpr::QualifiedWildcard(object_name) => {
                union_spans(object_name.0.iter().map(|i| i.span))
            }
            FunctionArgExpr::Wildcard => Span::empty(),
        }
    }
}

impl Spanned for TableAlias {
    fn span(&self) -> Span {
        union_spans(iter::once(self.name.span).chain(self.columns.iter().map(|i| i.span)))
    }
}

impl Spanned for Value {
    fn span(&self) -> Span {
        Span::empty() // # todo: Value needs to store spans before this is possible
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
            JoinOperator::AsOf {
                match_condition,
                constraint,
            } => todo!(),
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
        .unwrap_or(Span::empty())
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
            .try_with_sql(
                "SELECT id, name FROM users LEFT JOIN companies ON users.company_id = companies.id",
            )
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
            .try_with_sql(
                "SELECT a FROM postgres.public.source UNION SELECT a FROM postgres.public.source",
            )
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
            .try_with_sql(
                "WITH cte AS (SELECT a FROM postgres.public.source) SELECT cte.* FROM cte",
            )
            .unwrap()
            .parse_query()
            .unwrap();

        query.span();
    }
}

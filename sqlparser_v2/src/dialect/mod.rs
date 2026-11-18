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

//! Dialects customize the grammar by overriding [`Dialect`] methods.

pub mod bigquery;
pub mod builtin;
pub mod clickhouse;
pub mod databricks;
pub mod duckdb;
pub mod hive;
pub mod mssql;
pub mod mysql;
pub mod oracle;
pub mod postgres;
pub mod redshift;
pub mod snowflake;
pub mod spark;
pub mod sqlite;
pub mod teradata;

use std::fmt::{self, Debug, Display};

pub use bigquery::BigQuery;
pub use builtin::{Builtin, DialectKind};
pub use clickhouse::ClickHouse;
pub use databricks::Databricks;
pub use duckdb::DuckDb;
pub use hive::Hive;
pub use mssql::MsSql;
pub use mysql::MySql;
pub use oracle::Oracle;
pub use postgres::Postgres;
pub use redshift::Redshift;
pub use snowflake::Snowflake;
pub use spark::Spark;
pub use sqlite::Sqlite;
pub use teradata::Teradata;

use crate::PResult;
use crate::ast::*;
use crate::grammar;
use crate::lexer::{LexResult, Token};
use crate::parser::{InfixRule, Parser, SetOperatorRule};

/// The types stored in the AST's extension slots.
///
/// This is separate from [`Dialect`] so that several dialects can share one
/// AST type. All built-in dialects use [`Builtin`]; a third-party dialect can
/// use it too, or define its own to add new nodes.
pub trait AstExt: 'static + Sized + Debug + Clone + PartialEq {
    /// Additional expression nodes.
    type Expr<'a>: Debug + Display + Clone + PartialEq;
    /// Data parsed directly after `SELECT`. It must render as nothing, or as
    /// text ending in a space.
    type Select<'a>: Debug + Display + Clone + PartialEq + Default;
    /// Additional statements.
    type Statement<'a>: Debug + Display + Clone + PartialEq;
    /// Data parsed directly after `INSERT`. It must render as nothing, or as
    /// text ending in a space.
    type Insert<'a>: Debug + Display + Clone + PartialEq + Default;
    /// Additional table factors, usable wherever a table can appear.
    type TableFactor<'a>: Debug + Display + Clone + PartialEq;
    /// Additional join conditions, rendered after the joined table with a
    /// space before them.
    type JoinConstraint<'a>: Debug + Display + Clone + PartialEq;
    /// Data parsed after an `ORDER BY` item. It must render as nothing, or as
    /// text starting with a space.
    type OrderByItem<'a>: Debug + Display + Clone + PartialEq + Default;
    /// Clauses parsed at the end of a query, before any pipe steps. It must
    /// render as nothing, or as text starting with a space.
    type QueryTail<'a>: Debug + Display + Clone + PartialEq + Default;

    /// True if `modifiers` render after `DISTINCT` rather than before it.
    fn modifiers_follow_quantifier(_modifiers: &Self::Select<'_>) -> bool {
        false
    }
}

/// The insert extension type of dialect `D`.
pub type InsertExtOf<'a, D> = <<D as Dialect>::Ext as AstExt>::Insert<'a>;

/// An empty extension, for slots a dialect does not use.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct NoExt;

impl Display for NoExt {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Ok(())
    }
}

/// The expression extension type of dialect `D`.
pub type ExprExtOf<'a, D> = <<D as Dialect>::Ext as AstExt>::Expr<'a>;

/// The select extension type of dialect `D`.
pub type SelectExtOf<'a, D> = <<D as Dialect>::Ext as AstExt>::Select<'a>;

/// The query tail extension type of dialect `D`.
pub type QueryTailOf<'a, D> = <<D as Dialect>::Ext as AstExt>::QueryTail<'a>;

/// A SQL dialect: the core grammar plus this dialect's overrides.
///
/// Every grammar rule is a method defaulting to [`crate::grammar`], so a
/// dialect overrides only the rules it changes and calls back into the core
/// for everything else.
///
/// To build on another dialect, delegate the hooks to that dialect's exported
/// generic rules, as [`Redshift`] does with [`postgres`].
pub trait Dialect: 'static + Sized {
    /// The AST extensions this dialect produces.
    type Ext: AstExt;

    /// Lexes a dialect-specific token at the start of the non-empty `rest`,
    /// or returns `None` to use the core lexer.
    #[inline]
    fn lex_token(_rest: &str) -> Option<LexResult> {
        None
    }

    /// Lexes a token whose meaning depends on the token directly before it,
    /// which is `None` if anything separates them. Runs before [`lex_token`].
    ///
    /// [`lex_token`]: Dialect::lex_token
    #[inline]
    fn lex_after(_prev: Option<Token<'_>>, _rest: &str) -> Option<LexResult> {
        None
    }

    /// Called at the start of each line after the first. A dialect with raw
    /// data on the lines after a statement returns where in `tokens` to place
    /// a [`TokenKind::DialectLiteral`] covering that data, and the data's
    /// length in `sql` from `pos`.
    ///
    /// [`TokenKind::DialectLiteral`]: crate::lexer::TokenKind::DialectLiteral
    #[inline]
    fn lex_payload_line(_sql: &str, _tokens: &[Token<'_>], _pos: usize) -> Option<(usize, usize)> {
        None
    }

    /// Returns the length of a comment at the start of the non-empty `rest`,
    /// or `None` if there is no comment there.
    #[inline]
    fn lex_comment(rest: &str) -> Option<Result<usize, &'static str>> {
        crate::lexer::lex_core_comment(rest)
    }

    #[inline]
    fn is_reserved(word: &str) -> bool {
        grammar::is_reserved(word)
    }

    /// True for words that cannot be an alias without `AS`.
    #[inline]
    fn is_reserved_for_alias(word: &str) -> bool {
        grammar::is_reserved_for_alias(word)
    }

    /// Builds a string literal from its token.
    #[inline]
    fn string_literal(token: Token<'_>) -> StringLiteral<'_> {
        grammar::string_literal(token)
    }

    /// Parses an optional filter after a function call.
    fn parse_function_filter<'a>(p: &mut Parser<'a, Self>) -> PResult<Option<Expr<'a, Self::Ext>>> {
        grammar::parse_function_filter(p)
    }

    fn parse_statement<'a>(p: &mut Parser<'a, Self>) -> PResult<Statement<'a, Self::Ext>> {
        grammar::parse_statement(p)
    }

    /// Parses `INSERT` where a dialect's forms are not all [`Insert`] nodes.
    fn parse_insert_statement<'a>(p: &mut Parser<'a, Self>) -> PResult<Statement<'a, Self::Ext>> {
        Self::parse_insert(p).map(|insert| StatementKind::Insert(Box::new(insert)).into())
    }

    fn parse_insert<'a>(p: &mut Parser<'a, Self>) -> PResult<Insert<'a, Self::Ext>> {
        grammar::parse_insert(p)
    }

    fn parse_insert_modifiers<'a>(_p: &mut Parser<'a, Self>) -> PResult<InsertExtOf<'a, Self>> {
        Ok(Default::default())
    }

    /// Parses the optional column list of an `INSERT`.
    fn parse_insert_columns<'a>(p: &mut Parser<'a, Self>) -> PResult<Vec<Ident<'a>>> {
        grammar::parse_insert_columns(p)
    }

    /// Parses the keyword between `INSERT` and the table, returning whether
    /// it was present. The core requires `INTO`.
    fn parse_insert_into<'a>(p: &mut Parser<'a, Self>) -> PResult<bool> {
        p.expect_keyword("INTO")?;
        Ok(true)
    }

    /// Parses what follows the table of an `INSERT`.
    fn parse_insert_source<'a>(p: &mut Parser<'a, Self>) -> PResult<InsertSource<'a, Self::Ext>> {
        grammar::parse_insert_source(p)
    }

    fn parse_update<'a>(p: &mut Parser<'a, Self>) -> PResult<Update<'a, Self::Ext>> {
        grammar::parse_update(p)
    }

    /// Parses an optional `FROM` after an `UPDATE`'s assignments. The core has none.
    fn parse_update_from_after_set<'a>(
        _p: &mut Parser<'a, Self>,
    ) -> PResult<Vec<TableWithJoins<'a, Self::Ext>>> {
        Ok(Vec::new())
    }

    /// Parses an optional `FROM` before an `UPDATE`'s `SET`. The core has none.
    fn parse_update_from_before_set<'a>(
        _p: &mut Parser<'a, Self>,
    ) -> PResult<Vec<TableWithJoins<'a, Self::Ext>>> {
        Ok(Vec::new())
    }

    fn parse_assignment<'a>(p: &mut Parser<'a, Self>) -> PResult<Assignment<'a, Self::Ext>> {
        grammar::parse_assignment(p)
    }

    fn parse_delete<'a>(p: &mut Parser<'a, Self>) -> PResult<Delete<'a, Self::Ext>> {
        grammar::parse_delete(p)
    }

    /// Parses an optional `USING` after a `DELETE`'s table. The core has none.
    fn parse_delete_using<'a>(
        _p: &mut Parser<'a, Self>,
    ) -> PResult<Vec<TableWithJoins<'a, Self::Ext>>> {
        Ok(Vec::new())
    }

    /// Parses an optional `RETURNING`-style clause. The core has none.
    fn parse_returning<'a>(_p: &mut Parser<'a, Self>) -> PResult<Vec<SelectItem<'a, Self::Ext>>> {
        Ok(Vec::new())
    }

    /// Parses an optional `OUTPUT`-style clause. The core has none.
    fn parse_output_clause<'a>(
        _p: &mut Parser<'a, Self>,
    ) -> PResult<Option<Box<OutputClause<'a, Self::Ext>>>> {
        Ok(None)
    }

    /// Dispatches a `CREATE` statement on its object type.
    fn parse_create<'a>(p: &mut Parser<'a, Self>) -> PResult<Statement<'a, Self::Ext>> {
        grammar::parse_create(p)
    }

    /// Parses what follows `CREATE ... TABLE`.
    fn parse_create_table<'a>(
        p: &mut Parser<'a, Self>,
        flags: CreateFlags,
    ) -> PResult<CreateTable<'a, Self::Ext>> {
        grammar::parse_create_table(p, flags)
    }

    fn parse_create_view<'a>(
        p: &mut Parser<'a, Self>,
        flags: CreateFlags,
    ) -> PResult<CreateView<'a, Self::Ext>> {
        grammar::parse_create_view(p, flags)
    }

    fn parse_create_index<'a>(p: &mut Parser<'a, Self>) -> PResult<CreateIndex<'a, Self::Ext>> {
        grammar::parse_create_index(p)
    }

    fn parse_drop<'a>(p: &mut Parser<'a, Self>) -> PResult<DropObject<'a>> {
        grammar::parse_drop(p)
    }

    fn parse_truncate<'a>(p: &mut Parser<'a, Self>) -> PResult<Truncate<'a>> {
        grammar::parse_truncate(p)
    }

    fn parse_alter_table<'a>(p: &mut Parser<'a, Self>) -> PResult<AlterTable<'a, Self::Ext>> {
        grammar::parse_alter_table(p)
    }

    fn parse_alter_table_op<'a>(p: &mut Parser<'a, Self>) -> PResult<AlterTableOp<'a, Self::Ext>> {
        grammar::parse_alter_table_op(p)
    }

    /// Consumes any `IF NOT EXISTS` in `ADD COLUMN`. The core has none.
    fn parse_add_column_if_not_exists<'a>(_p: &mut Parser<'a, Self>) -> PResult<bool> {
        Ok(false)
    }

    /// Consumes any `ONLY` after `ALTER TABLE`. The core has none.
    fn parse_alter_table_only<'a>(_p: &mut Parser<'a, Self>) -> bool {
        false
    }

    /// Parses what follows `CREATE SCHEMA`.
    fn parse_create_schema<'a>(
        p: &mut Parser<'a, Self>,
        flags: CreateFlags,
    ) -> PResult<CreateSchema<'a>> {
        grammar::parse_create_schema(p, flags)
    }

    /// Parses an object type that only this dialect can `CREATE`, with the
    /// cursor on the type's first word. The core has none.
    fn parse_create_extension<'a>(
        _p: &mut Parser<'a, Self>,
        _flags: CreateFlags,
    ) -> PResult<Option<Statement<'a, Self::Ext>>> {
        Ok(None)
    }

    /// Parses the source of `CREATE TABLE ... AS`.
    fn parse_create_table_query<'a>(p: &mut Parser<'a, Self>) -> PResult<Query<'a, Self::Ext>> {
        Self::parse_query(p)
    }

    /// Parses a table constraint, or returns `None` if there is none here.
    fn parse_table_constraint<'a>(
        p: &mut Parser<'a, Self>,
    ) -> PResult<Option<TableConstraint<'a, Self::Ext>>> {
        grammar::parse_table_constraint(p)
    }

    fn parse_column_def<'a>(p: &mut Parser<'a, Self>) -> PResult<ColumnDef<'a, Self::Ext>> {
        grammar::parse_column_def(p)
    }

    /// Parses a column option, or returns `None` if there is none here.
    fn parse_column_option<'a>(
        p: &mut Parser<'a, Self>,
    ) -> PResult<Option<ColumnOption<'a, Self::Ext>>> {
        grammar::parse_column_option(p)
    }

    fn parse_data_type<'a>(p: &mut Parser<'a, Self>) -> PResult<DataType<'a>> {
        grammar::parse_data_type(p)
    }

    fn parse_query<'a>(p: &mut Parser<'a, Self>) -> PResult<Query<'a, Self::Ext>> {
        grammar::parse_query(p)
    }

    /// Parses the lower bound of `BETWEEN`.
    fn parse_between_low<'a>(p: &mut Parser<'a, Self>) -> PResult<Expr<'a, Self::Ext>> {
        grammar::parse_between_low(p)
    }

    /// Parses the upper bound of `BETWEEN`, after `AND`.
    fn parse_between_high<'a>(p: &mut Parser<'a, Self>) -> PResult<Expr<'a, Self::Ext>> {
        grammar::parse_between_high(p)
    }

    /// Parses what follows `SAMPLE` or `TABLESAMPLE`.
    fn parse_sample_clause<'a>(
        p: &mut Parser<'a, Self>,
        long_form: bool,
    ) -> PResult<SampleClause<'a, Self::Ext>> {
        grammar::parse_sample_clause(p, long_form)
    }

    /// Parses the values that a `PIVOT` clause lists after `IN (`.
    fn parse_pivot_source<'a>(p: &mut Parser<'a, Self>) -> PResult<PivotSource<'a, Self::Ext>> {
        grammar::parse_pivot_source(p)
    }

    /// Parses any `LATERAL VIEW` clauses after `FROM`. The common grammar has none.
    fn parse_lateral_views<'a>(
        _p: &mut Parser<'a, Self>,
    ) -> PResult<Vec<LateralView<'a, Self::Ext>>> {
        Ok(Vec::new())
    }

    /// Parses `CLUSTER BY`, `DISTRIBUTE BY` and `SORT BY` at the end of a
    /// `SELECT`. The common grammar has none of them.
    fn parse_query_distribution<'a>(
        _p: &mut Parser<'a, Self>,
    ) -> PResult<Option<Box<QueryDistribution<'a, Self::Ext>>>> {
        Ok(None)
    }

    /// Finishes a select item once its expression has been parsed.
    fn parse_select_item_alias<'a>(
        p: &mut Parser<'a, Self>,
        expr: Expr<'a, Self::Ext>,
    ) -> PResult<SelectItem<'a, Self::Ext>> {
        grammar::parse_select_item_alias(p, expr)
    }

    /// Parses a `LIMIT` that follows the locking clauses, in dialects where
    /// `FOR UPDATE` can precede it. Runs only when no `LIMIT` came earlier.
    fn parse_limit_after_locks<'a>(
        _p: &mut Parser<'a, Self>,
    ) -> PResult<Option<Box<Limit<'a, Self::Ext>>>> {
        Ok(None)
    }

    /// Parses what ends a query after its locking clauses. The core has none.
    fn parse_query_tail<'a>(_p: &mut Parser<'a, Self>) -> PResult<QueryTailOf<'a, Self>> {
        Ok(Default::default())
    }

    /// Parses any `|> ...` steps after a query. The common grammar has none;
    /// a dialect with pipe syntax can use [`grammar::parse_pipe_operators`].
    fn parse_pipe_operators<'a>(
        _p: &mut Parser<'a, Self>,
    ) -> PResult<Vec<PipeOperator<'a, Self::Ext>>> {
        Ok(Vec::new())
    }

    /// Parses what follows a set operator's keyword: its quantifier, and
    /// whether columns are matched by name. The core has no matching by name.
    fn parse_set_quantifier<'a>(p: &mut Parser<'a, Self>) -> (SetQuantifier, bool) {
        (grammar::parse_quantifier(p), false)
    }

    /// The set operator at the current token, or `None` if there is none.
    fn set_operator_rule<'a>(p: &Parser<'a, Self>) -> Option<SetOperatorRule> {
        grammar::set_operator_rule(p)
    }

    fn parse_select<'a>(p: &mut Parser<'a, Self>) -> PResult<Select<'a, Self::Ext>> {
        grammar::parse_select(p)
    }

    fn parse_select_head<'a>(
        p: &mut Parser<'a, Self>,
    ) -> PResult<(SelectExtOf<'a, Self>, SetQuantifier)> {
        grammar::parse_select_head(p)
    }

    /// Consumes `RECURSIVE` after `WITH`, in dialects that write it.
    fn eat_with_recursive<'a>(p: &mut Parser<'a, Self>) -> bool {
        grammar::eat_with_recursive(p)
    }

    /// Parses a statement that a `WITH` clause leads into, with the cursor after
    /// that clause. The core has only queries there.
    fn parse_statement_after_with<'a>(
        _p: &mut Parser<'a, Self>,
    ) -> PResult<Option<Statement<'a, Self::Ext>>> {
        Ok(None)
    }

    /// True if the token `n` ahead starts a `VALUES` query.
    fn at_query_values<'a>(p: &Parser<'a, Self>, n: usize) -> bool {
        grammar::at_query_values(p, n)
    }

    /// Parses the rows of a `VALUES` query, after the keyword.
    fn parse_query_values<'a>(p: &mut Parser<'a, Self>) -> PResult<SetExpr<'a, Self::Ext>> {
        grammar::parse_query_values(p)
    }

    /// True if the token `n` ahead starts a query that puts `FROM` first. Such
    /// a dialect also overrides `parse_select` to read it.
    fn at_from_first<'a>(_p: &Parser<'a, Self>, _n: usize) -> bool {
        false
    }

    /// Parses the select list.
    fn parse_projection<'a>(p: &mut Parser<'a, Self>) -> PResult<Vec<SelectItem<'a, Self::Ext>>> {
        p.parse_comma_separated(Parser::parse_select_item)
    }

    /// Parses an optional `INTO` after the select list. The core has none.
    fn parse_select_into<'a>(
        _p: &mut Parser<'a, Self>,
    ) -> PResult<Option<Box<SelectInto<'a, Self::Ext>>>> {
        Ok(None)
    }

    /// Parses what follows `FROM`.
    fn parse_from_list<'a>(
        p: &mut Parser<'a, Self>,
    ) -> PResult<Vec<TableWithJoins<'a, Self::Ext>>> {
        p.parse_comma_separated(Self::parse_table_with_joins)
    }

    /// Parses an optional `PREWHERE` clause, before `WHERE`.
    fn parse_prewhere<'a>(_p: &mut Parser<'a, Self>) -> PResult<Option<Box<Expr<'a, Self::Ext>>>> {
        Ok(None)
    }

    /// Parses an optional `QUALIFY` clause.
    fn parse_qualify<'a>(_p: &mut Parser<'a, Self>) -> PResult<Option<Expr<'a, Self::Ext>>> {
        Ok(None)
    }

    /// Decides whether parentheses that hold no join are allowed.
    fn check_nested_join<'a>(
        p: &Parser<'a, Self>,
        table: &TableWithJoins<'a, Self::Ext>,
    ) -> PResult<()> {
        grammar::check_nested_join(p, table)
    }

    /// Parses `SEPARATOR 'text'` inside a call's parentheses, after its
    /// arguments and `ORDER BY`. The core has none.
    fn parse_function_separator<'a>(
        _p: &mut Parser<'a, Self>,
    ) -> PResult<Option<StringLiteral<'a>>> {
        Ok(None)
    }

    /// Parses `ON OVERFLOW ...` inside a call's parentheses, after its
    /// arguments. The core has none.
    fn parse_function_on_overflow<'a>(
        _p: &mut Parser<'a, Self>,
    ) -> PResult<Option<Box<ListAggOnOverflow<'a, Self::Ext>>>> {
        Ok(None)
    }

    /// True at the closing of a call that has no arguments before it.
    fn at_empty_function_args<'a>(p: &Parser<'a, Self>) -> bool {
        p.at_op(")")
    }

    /// Parses an optional `OVER` clause after a function call.
    fn parse_function_over<'a>(
        p: &mut Parser<'a, Self>,
    ) -> PResult<Option<WindowOver<'a, Self::Ext>>> {
        if p.eat_keyword("OVER") {
            grammar::parse_window_over(p).map(Some)
        } else {
            Ok(None)
        }
    }

    /// True if the name just read is followed by a call's arguments.
    fn at_function_call<'a>(p: &Parser<'a, Self>) -> bool {
        p.at_op("(")
    }

    fn parse_show<'a>(p: &mut Parser<'a, Self>) -> PResult<Show<'a, Self::Ext>> {
        grammar::parse_show(p)
    }

    fn parse_transaction<'a>(p: &mut Parser<'a, Self>) -> PResult<Transaction<'a>> {
        grammar::parse_transaction(p)
    }

    fn parse_use<'a>(p: &mut Parser<'a, Self>) -> PResult<ObjectName<'a>> {
        grammar::parse_use(p)
    }

    /// Parses `EXPLAIN` or `DESCRIBE`.
    fn parse_explain<'a>(p: &mut Parser<'a, Self>) -> PResult<Explain<'a, Self::Ext>> {
        grammar::parse_explain(p)
    }

    fn parse_call<'a>(p: &mut Parser<'a, Self>) -> PResult<Expr<'a, Self::Ext>> {
        grammar::parse_call(p)
    }

    /// Parses one grantee of `GRANT` or `REVOKE`.
    fn parse_grantee<'a>(p: &mut Parser<'a, Self>) -> PResult<Grantee<'a>> {
        grammar::parse_grantee(p)
    }

    /// Parses the list of grantees of `GRANT` and `REVOKE`.
    fn parse_grantees<'a>(p: &mut Parser<'a, Self>) -> PResult<Vec<Grantee<'a>>> {
        grammar::parse_grantees(p)
    }

    /// Parses one name a grant applies to.
    fn parse_grant_name<'a>(p: &mut Parser<'a, Self>) -> PResult<GrantName<'a>> {
        grammar::parse_grant_name(p)
    }

    fn parse_grant<'a>(p: &mut Parser<'a, Self>) -> PResult<Grant<'a>> {
        grammar::parse_grant(p)
    }

    fn parse_revoke<'a>(p: &mut Parser<'a, Self>) -> PResult<Revoke<'a>> {
        grammar::parse_revoke(p)
    }

    fn parse_merge<'a>(p: &mut Parser<'a, Self>) -> PResult<Merge<'a, Self::Ext>> {
        grammar::parse_merge(p)
    }

    /// Consumes `TEMPORARY` after `DROP`, in dialects that have it.
    fn eat_drop_temporary<'a>(_p: &mut Parser<'a, Self>) -> bool {
        false
    }

    /// Consumes `PURGE` at the end of `DROP`, in dialects that have it.
    fn eat_drop_purge<'a>(_p: &mut Parser<'a, Self>) -> bool {
        false
    }

    /// Parses `DROP FUNCTION` and the like.
    fn parse_drop_routine<'a>(p: &mut Parser<'a, Self>) -> PResult<DropRoutine<'a, Self::Ext>> {
        grammar::parse_drop_routine(p)
    }

    /// Consumes modifiers written before an index's name.
    fn parse_index_flags<'a>(_p: &mut Parser<'a, Self>) -> IndexFlags {
        IndexFlags::default()
    }

    /// Parses the settings written after an index's definition.
    fn parse_index_settings<'a>(
        _p: &mut Parser<'a, Self>,
    ) -> PResult<Vec<(&'static str, &'a str)>> {
        Ok(Vec::new())
    }

    /// Consumes `ALL` after `LIMIT`, in dialects that have `LIMIT ALL`.
    fn eat_limit_all<'a>(_p: &mut Parser<'a, Self>) -> bool {
        false
    }

    /// Parses `GROUPING SETS (...)` written after a `GROUP BY` list.
    fn parse_group_by_suffix<'a>(
        _p: &mut Parser<'a, Self>,
    ) -> PResult<Option<Box<Expr<'a, Self::Ext>>>> {
        Ok(None)
    }

    /// Parses the column list after a CTE's name. The core allows one.
    fn parse_cte_columns<'a>(p: &mut Parser<'a, Self>) -> PResult<Vec<Ident<'a>>> {
        if grammar::at_column_list(p) {
            grammar::parse_ident_list(p)
        } else {
            Ok(Vec::new())
        }
    }

    /// Parses a quantifier written before a list of patterns on the right of
    /// `LIKE`. The core has none.
    fn parse_like_quantifier<'a>(_p: &mut Parser<'a, Self>) -> PResult<Option<Quantifier>> {
        Ok(None)
    }

    /// Parses a form of `IS` beyond the common ones, after `IS` and any `NOT`.
    fn parse_is_test<'a>(_p: &mut Parser<'a, Self>) -> PResult<Option<IsTest<'a, Self::Ext>>> {
        Ok(None)
    }

    /// Consumes `ALL` after `GROUP BY`, in dialects that have `GROUP BY ALL`.
    fn eat_group_by_all<'a>(_p: &mut Parser<'a, Self>) -> bool {
        false
    }

    /// Parses the window another window is defined as: `WINDOW w AS other`. The
    /// common grammar requires a parenthesized definition.
    fn parse_named_window_reference<'a>(_p: &mut Parser<'a, Self>) -> PResult<Option<Ident<'a>>> {
        Ok(None)
    }

    /// Consumes `ROW` as what a `MERGE` clause inserts, in dialects that have it.
    fn eat_merge_insert_row<'a>(_p: &mut Parser<'a, Self>) -> bool {
        false
    }

    /// Parses `WITH ROLLUP` and the like after `GROUP BY`.
    fn parse_group_by_modifier<'a>(_p: &mut Parser<'a, Self>) -> Option<GroupByModifier> {
        None
    }

    /// Parses one column of `CREATE INDEX`.
    fn parse_index_column<'a>(p: &mut Parser<'a, Self>) -> PResult<IndexColumn<'a, Self::Ext>> {
        grammar::parse_index_column(p)
    }

    /// Parses `WITH (...)` storage parameters on an index.
    fn parse_index_storage<'a>(_p: &mut Parser<'a, Self>) -> PResult<IndexStorage<'a, Self::Ext>> {
        Ok(Vec::new())
    }

    /// Parses `USING expr` after a column's new type.
    fn parse_alter_type_using<'a>(
        _p: &mut Parser<'a, Self>,
    ) -> PResult<Option<Expr<'a, Self::Ext>>> {
        Ok(None)
    }

    /// Parses a `SET` statement.
    fn parse_set<'a>(p: &mut Parser<'a, Self>) -> PResult<SetStatement<'a, Self::Ext>> {
        grammar::parse_set(p)
    }

    /// Parses options written directly after `EXPLAIN`.
    fn parse_explain_options<'a>(_p: &mut Parser<'a, Self>) -> PResult<Vec<ExplainOption<'a>>> {
        Ok(Vec::new())
    }

    /// Parses what `EXPLAIN` or `DESCRIBE` applies to.
    fn parse_explain_target<'a>(p: &mut Parser<'a, Self>) -> PResult<ExplainTarget<'a, Self::Ext>> {
        grammar::parse_explain_target(p)
    }

    /// Consumes a kind of object that only this dialect can `DROP`.
    fn parse_drop_object_type<'a>(_p: &mut Parser<'a, Self>) -> Option<ObjectType<'a>> {
        None
    }

    /// Parses a part of a name after a dot, where reserved words are allowed.
    fn parse_name_part<'a>(p: &mut Parser<'a, Self>) -> PResult<Ident<'a>> {
        grammar::parse_name_part(p)
    }

    /// Parses one part of an object's name, given the parts before it.
    fn parse_object_name_part<'a>(
        p: &mut Parser<'a, Self>,
        before: &[Ident<'a>],
    ) -> PResult<Ident<'a>> {
        grammar::parse_object_name_part(p, before)
    }

    /// Parses the table on the right of a join. `has_constraint` is false for a
    /// natural or cross join, which takes no `ON` or `USING`.
    fn parse_join_table<'a>(
        p: &mut Parser<'a, Self>,
        _has_constraint: bool,
    ) -> PResult<TableRef<'a, Self::Ext>> {
        Self::parse_table_ref(p)
    }

    /// Parses `CONNECT BY` and `START WITH` clauses, in dialects with them.
    fn parse_connect_by<'a>(p: &mut Parser<'a, Self>) -> PResult<Vec<ConnectBy<'a, Self::Ext>>> {
        let _ = p;
        Ok(Vec::new())
    }

    /// Parses one item of a `GROUP BY` list.
    fn parse_group_by_item<'a>(p: &mut Parser<'a, Self>) -> PResult<Expr<'a, Self::Ext>> {
        p.parse_expr()
    }

    fn parse_select_modifiers<'a>(_p: &mut Parser<'a, Self>) -> PResult<SelectExtOf<'a, Self>> {
        Ok(Default::default())
    }

    fn parse_select_item<'a>(p: &mut Parser<'a, Self>) -> PResult<SelectItem<'a, Self::Ext>> {
        grammar::parse_select_item(p)
    }

    fn parse_table_ref<'a>(p: &mut Parser<'a, Self>) -> PResult<TableRef<'a, Self::Ext>> {
        grammar::parse_table_ref(p)
    }

    fn parse_table_factor<'a>(p: &mut Parser<'a, Self>) -> PResult<TableFactor<'a, Self::Ext>> {
        grammar::parse_table_factor(p)
    }

    fn parse_cte<'a>(p: &mut Parser<'a, Self>) -> PResult<Cte<'a, Self::Ext>> {
        grammar::parse_cte(p)
    }

    fn parse_table_with_joins<'a>(
        p: &mut Parser<'a, Self>,
    ) -> PResult<TableWithJoins<'a, Self::Ext>> {
        grammar::parse_table_with_joins(p)
    }

    /// Parses the next join of a `FROM` item, or returns `None` if there is
    /// no join at the current token.
    fn parse_join<'a>(p: &mut Parser<'a, Self>) -> PResult<Option<Join<'a, Self::Ext>>> {
        grammar::parse_join(p)
    }

    /// Parses `IGNORE NULLS` or `RESPECT NULLS` inside a call's parentheses,
    /// after its arguments. The core only has them after the parentheses.
    fn parse_function_arg_null_treatment<'a>(
        _p: &mut Parser<'a, Self>,
    ) -> PResult<Option<NullTreatment>> {
        Ok(None)
    }

    /// Parses a call's arguments, after any quantifier.
    fn parse_function_args<'a>(
        p: &mut Parser<'a, Self>,
        quantifier: SetQuantifier,
    ) -> PResult<Vec<FunctionArg<'a, Self::Ext>>> {
        grammar::parse_function_args(p, quantifier)
    }

    /// Parses one argument of a call.
    fn parse_function_arg<'a>(p: &mut Parser<'a, Self>) -> PResult<FunctionArg<'a, Self::Ext>> {
        grammar::parse_function_arg(p)
    }

    /// Parses a call's parenthesized arguments, with the current token on `(`.
    fn parse_function_call<'a>(
        p: &mut Parser<'a, Self>,
        name: ObjectName<'a>,
    ) -> PResult<Expr<'a, Self::Ext>> {
        grammar::parse_function_call(p, name)
    }

    fn parse_order_by_item<'a>(p: &mut Parser<'a, Self>) -> PResult<OrderByItem<'a, Self::Ext>> {
        grammar::parse_order_by_item(p)
    }

    /// Parses what follows `ORDER BY` at the end of a query.
    fn parse_query_order_by<'a>(
        p: &mut Parser<'a, Self>,
    ) -> PResult<Vec<OrderByItem<'a, Self::Ext>>> {
        p.parse_comma_separated(Self::parse_order_by_item)
    }

    /// Parses what follows the `LIMIT` keyword.
    fn parse_limit<'a>(p: &mut Parser<'a, Self>) -> PResult<Limit<'a, Self::Ext>> {
        grammar::parse_limit(p)
    }

    /// Parses the part that `EXTRACT` takes. The text is kept as written,
    /// with any quotes.
    /// True if the unquoted word `name` before a string makes it a literal of
    /// that type, as `DATE` does in `DATE '2020-01-01'`.
    #[inline]
    fn is_typed_string_type(name: &str) -> bool {
        grammar::is_typed_string_type(name)
    }

    /// True if the unquoted word `word`, without parentheses after it, calls a
    /// function rather than naming a column, as `CURRENT_DATE` does.
    #[inline]
    fn is_niladic_function(word: &str) -> bool {
        grammar::is_niladic_function(word)
    }

    /// Parses the offset of a window frame bound, before `PRECEDING` or
    /// `FOLLOWING`.
    fn parse_frame_offset<'a>(p: &mut Parser<'a, Self>) -> PResult<Expr<'a, Self::Ext>> {
        p.parse_expr()
    }

    fn parse_extract_field<'a>(p: &mut Parser<'a, Self>) -> PResult<&'a str> {
        grammar::parse_date_time_field(p)
    }

    /// Parses what follows the `INTERVAL` keyword.
    fn parse_interval<'a>(p: &mut Parser<'a, Self>) -> PResult<Expr<'a, Self::Ext>> {
        grammar::parse_interval(p, true)
    }

    /// The prefix half of the Pratt parser.
    fn parse_prefix<'a>(p: &mut Parser<'a, Self>) -> PResult<Expr<'a, Self::Ext>> {
        grammar::parse_prefix(p)
    }

    /// The infix half of the Pratt parser: how to parse the operator at the
    /// current token, or `None` if it is not an infix operator.
    fn infix_rule<'a>(p: &Parser<'a, Self>) -> Option<InfixRule<'a, Self>> {
        grammar::infix_rule(p)
    }
}

/// The core grammar with no overrides.
#[derive(Debug, Clone, PartialEq)]
pub struct Core;

impl Dialect for Core {
    type Ext = Builtin;
}

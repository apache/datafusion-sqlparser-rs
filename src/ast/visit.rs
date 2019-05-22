// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! SQL AST traversal.

// Visiting an enum struct variant with many arguments forces our hand here.
// If, over time, we convert these large variants to dedicated structs, we
// can remove this escape hatch.
#![allow(clippy::too_many_arguments)]

// Disable lints that want us to rewrite `&Ident` as `&str`, as `&str` is not as
// self-documenting as &Ident.
#![allow(clippy::ptr_arg)]

use super::*;

/// A trait that represents a visitor that walks through a SQL AST.
///
/// Each function corresponds to a node in the SQL AST, and has a default
/// implementation that visits all of its child nodes. Implementors of this
/// trait can override functions as desired to hook into AST traversal without
/// writing code to traverse the entire AST.
pub trait Visit<'ast> {
    fn visit_statement(&mut self, statement: &'ast Statement) {
        visit_statement(self, statement)
    }

    fn visit_query(&mut self, query: &'ast Query) {
        visit_query(self, query)
    }

    fn visit_cte(&mut self, cte: &'ast Cte) {
        visit_cte(self, cte)
    }

    fn visit_select(&mut self, select: &'ast Select) {
        visit_select(self, select)
    }

    fn visit_select_item(&mut self, select_item: &'ast SelectItem) {
        visit_select_item(self, select_item)
    }

    fn visit_table_with_joins(&mut self, table_with_joins: &'ast TableWithJoins) {
        visit_table_with_joins(self, table_with_joins)
    }

    fn visit_table_factor(&mut self, table_factor: &'ast TableFactor) {
        visit_table_factor(self, table_factor)
    }

    fn visit_table_table_factor(
        &mut self,
        name: &'ast ObjectName,
        alias: Option<&'ast TableAlias>,
        args: &'ast [Expr],
        with_hints: &'ast [Expr],
    ) {
        visit_table_table_factor(self, name, alias, args, with_hints)
    }

    fn visit_derived_table_factor(
        &mut self,
        lateral: bool,
        subquery: &'ast Query,
        alias: Option<&'ast TableAlias>,
    ) {
        visit_derived_table_factor(self, lateral, subquery, alias)
    }

    fn visit_nested_join_table_factor(&mut self, table_with_joins: &'ast TableWithJoins) {
        visit_nested_join_table_factor(self, table_with_joins)
    }

    fn visit_table_alias(&mut self, alias: &'ast TableAlias) {
        visit_table_alias(self, alias)
    }

    fn visit_join(&mut self, join: &'ast Join) {
        visit_join(self, join)
    }

    fn visit_join_operator(&mut self, op: &'ast JoinOperator) {
        visit_join_operator(self, op)
    }

    fn visit_join_constraint(&mut self, constraint: &'ast JoinConstraint) {
        visit_join_constraint(self, constraint)
    }

    fn visit_where(&mut self, expr: &'ast Expr) {
        visit_where(self, expr)
    }

    fn visit_group_by(&mut self, exprs: &'ast [Expr]) {
        visit_group_by(self, exprs)
    }

    fn visit_having(&mut self, expr: &'ast Expr) {
        visit_having(self, expr)
    }

    fn visit_set_expr(&mut self, set_expr: &'ast SetExpr) {
        visit_set_expr(self, set_expr)
    }

    fn visit_set_operation(
        &mut self,
        left: &'ast SetExpr,
        op: &'ast SetOperator,
        right: &'ast SetExpr,
        all: bool,
    ) {
        visit_set_operation(self, left, op, right, all)
    }

    fn visit_set_operator(&mut self, _operator: &'ast SetOperator) {}

    fn visit_order_by(&mut self, order_by: &'ast OrderByExpr) {
        visit_order_by(self, order_by)
    }

    fn visit_limit(&mut self, expr: &'ast Expr) {
        visit_limit(self, expr)
    }

    fn visit_type(&mut self, _data_type: &'ast DataType) {}

    fn visit_expr(&mut self, expr: &'ast Expr) {
        visit_expr(self, expr)
    }

    fn visit_unnamed_expr(&mut self, expr: &'ast Expr) {
        visit_unnamed_expr(self, expr)
    }

    fn visit_expr_with_alias(&mut self, expr: &'ast Expr, alias: &'ast Ident) {
        visit_expr_with_alias(self, expr, alias)
    }

    fn visit_object_name(&mut self, object_name: &'ast ObjectName) {
        visit_object_name(self, object_name)
    }

    fn visit_ident(&mut self, _ident: &'ast Ident) {}

    fn visit_compound_identifier(&mut self, idents: &'ast [Ident]) {
        visit_compound_identifier(self, idents)
    }

    fn visit_wildcard(&mut self) {}

    fn visit_qualified_wildcard(&mut self, idents: &'ast [Ident]) {
        visit_qualified_wildcard(self, idents)
    }

    fn visit_is_null(&mut self, expr: &'ast Expr) {
        visit_is_null(self, expr)
    }

    fn visit_is_not_null(&mut self, expr: &'ast Expr) {
        visit_is_not_null(self, expr)
    }

    fn visit_in_list(&mut self, expr: &'ast Expr, list: &'ast [Expr], negated: bool) {
        visit_in_list(self, expr, list, negated)
    }

    fn visit_in_subquery(&mut self, expr: &'ast Expr, subquery: &'ast Query, negated: bool) {
        visit_in_subquery(self, expr, subquery, negated)
    }

    fn visit_between(
        &mut self,
        expr: &'ast Expr,
        low: &'ast Expr,
        high: &'ast Expr,
        negated: bool,
    ) {
        visit_between(self, expr, low, high, negated)
    }

    fn visit_binary_op(&mut self, left: &'ast Expr, op: &'ast BinaryOperator, right: &'ast Expr) {
        visit_binary_op(self, left, op, right)
    }

    fn visit_binary_operator(&mut self, _op: &'ast BinaryOperator) {}

    fn visit_unary_op(&mut self, expr: &'ast Expr, op: &'ast UnaryOperator) {
        visit_unary_op(self, expr, op)
    }

    fn visit_unary_operator(&mut self, _op: &'ast UnaryOperator) {}

    fn visit_cast(&mut self, expr: &'ast Expr, data_type: &'ast DataType) {
        visit_cast(self, expr, data_type)
    }

    fn visit_collate(&mut self, expr: &'ast Expr, collation: &'ast ObjectName) {
        visit_collate(self, expr, collation)
    }

    fn visit_extract(&mut self, field: &'ast DateTimeField, expr: &'ast Expr) {
        visit_extract(self, field, expr)
    }

    fn visit_date_time_field(&mut self, _field: &'ast DateTimeField) {}

    fn visit_nested(&mut self, expr: &'ast Expr) {
        visit_nested(self, expr)
    }

    fn visit_value(&mut self, _val: &'ast Value) {}

    fn visit_function(&mut self, func: &'ast Function) {
        visit_function(self, func)
    }

    fn visit_window_spec(&mut self, window_spec: &'ast WindowSpec) {
        visit_window_spec(self, window_spec)
    }

    fn visit_window_frame(&mut self, window_frame: &'ast WindowFrame) {
        visit_window_frame(self, window_frame)
    }

    fn visit_window_frame_units(&mut self, _window_frame_units: &'ast WindowFrameUnits) {}

    fn visit_window_frame_bound(&mut self, _window_frame_bound: &'ast WindowFrameBound) {}

    fn visit_case(
        &mut self,
        operand: Option<&'ast Expr>,
        conditions: &'ast [Expr],
        results: &'ast [Expr],
        else_result: Option<&'ast Expr>,
    ) {
        visit_case(self, operand, conditions, results, else_result)
    }

    fn visit_exists(&mut self, subquery: &'ast Query) {
        visit_exists(self, subquery)
    }

    fn visit_subquery(&mut self, subquery: &'ast Query) {
        visit_subquery(self, subquery)
    }

    fn visit_insert(
        &mut self,
        table_name: &'ast ObjectName,
        columns: &'ast [Ident],
        source: &'ast Query,
    ) {
        visit_insert(self, table_name, columns, source)
    }

    fn visit_values(&mut self, values: &'ast Values) {
        visit_values(self, values)
    }

    fn visit_values_row(&mut self, row: &'ast [Expr]) {
        visit_values_row(self, row)
    }

    fn visit_copy(
        &mut self,
        table_name: &'ast ObjectName,
        columns: &'ast [Ident],
        values: &'ast [Option<String>],
    ) {
        visit_copy(self, table_name, columns, values)
    }

    fn visit_copy_values(&mut self, values: &'ast [Option<String>]) {
        visit_copy_values(self, values)
    }

    fn visit_copy_values_row(&mut self, _row: Option<&String>) {}

    fn visit_update(
        &mut self,
        table_name: &'ast ObjectName,
        assignments: &'ast [Assignment],
        selection: Option<&'ast Expr>,
    ) {
        visit_update(self, table_name, assignments, selection)
    }

    fn visit_assignment(&mut self, assignment: &'ast Assignment) {
        visit_assignment(self, assignment)
    }

    fn visit_delete(&mut self, table_name: &'ast ObjectName, selection: Option<&'ast Expr>) {
        visit_delete(self, table_name, selection)
    }

    fn visit_literal_string(&mut self, _string: &'ast String) {}

    fn visit_create_view(
        &mut self,
        name: &'ast ObjectName,
        columns: &'ast [Ident],
        query: &'ast Query,
        materialized: bool,
        with_options: &'ast [SqlOption],
    ) {
        visit_create_view(self, name, columns, query, materialized, with_options)
    }

    fn visit_create_table(
        &mut self,
        name: &'ast ObjectName,
        columns: &'ast [ColumnDef],
        constraints: &'ast [TableConstraint],
        with_options: &'ast [SqlOption],
        external: bool,
        file_format: &'ast Option<FileFormat>,
        location: &'ast Option<String>,
    ) {
        visit_create_table(
            self,
            name,
            columns,
            constraints,
            with_options,
            external,
            file_format,
            location,
        )
    }

    fn visit_column_def(&mut self, column_def: &'ast ColumnDef) {
        visit_column_def(self, column_def)
    }

    fn visit_column_option_def(&mut self, column_option_def: &'ast ColumnOptionDef) {
        visit_column_option_def(self, column_option_def)
    }

    fn visit_column_option(&mut self, column_option: &'ast ColumnOption) {
        visit_column_option(self, column_option)
    }

    fn visit_file_format(&mut self, _file_format: &'ast FileFormat) {}

    fn visit_option(&mut self, option: &'ast SqlOption) {
        visit_option(self, option)
    }

    fn visit_drop(
        &mut self,
        object_type: &'ast ObjectType,
        if_exists: bool,
        names: &'ast [ObjectName],
        cascade: bool,
    ) {
        visit_drop(self, object_type, if_exists, names, cascade)
    }

    fn visit_object_type(&mut self, _object_type: &'ast ObjectType) {}

    fn visit_alter_table(&mut self, name: &'ast ObjectName, operation: &'ast AlterTableOperation) {
        visit_alter_table(self, name, operation)
    }

    fn visit_alter_table_operation(&mut self, operation: &'ast AlterTableOperation) {
        visit_alter_table_operation(self, operation)
    }

    fn visit_alter_add_constraint(&mut self, table_constraint: &'ast TableConstraint) {
        visit_alter_add_constraint(self, table_constraint)
    }

    fn visit_table_constraint(&mut self, table_constraint: &'ast TableConstraint) {
        visit_table_constraint(self, table_constraint)
    }

    fn visit_table_constraint_unique(
        &mut self,
        name: Option<&'ast Ident>,
        columns: &'ast [Ident],
        is_primary: bool,
    ) {
        visit_table_constraint_unique(self, name, columns, is_primary)
    }

    fn visit_table_constraint_foreign_key(
        &mut self,
        name: Option<&'ast Ident>,
        columns: &'ast [Ident],
        foreign_table: &'ast ObjectName,
        referred_columns: &'ast [Ident],
    ) {
        visit_table_constraint_foreign_key(self, name, columns, foreign_table, referred_columns)
    }

    fn visit_table_constraint_check(&mut self, name: Option<&'ast Ident>, expr: &'ast Expr) {
        visit_table_constraint_check(self, name, expr)
    }

    fn visit_alter_drop_constraint(&mut self, name: &'ast Ident) {
        visit_alter_drop_constraint(self, name)
    }

    fn visit_start_transaction(&mut self, modes: &'ast [TransactionMode]) {
        visit_start_transaction(self, modes)
    }

    fn visit_set_transaction(&mut self, modes: &'ast [TransactionMode]) {
        visit_set_transaction(self, modes)
    }

    fn visit_transaction_mode(&mut self, mode: &'ast TransactionMode) {
        visit_transaction_mode(self, mode)
    }

    fn visit_transaction_access_mode(&mut self, _access_mode: &'ast TransactionAccessMode) {}

    fn visit_transaction_isolation_level(
        &mut self,
        _isolation_level: &'ast TransactionIsolationLevel,
    ) {

    }

    fn visit_commit(&mut self, _chain: bool) {}

    fn visit_rollback(&mut self, _chain: bool) {}
}

pub fn visit_statement<'ast, V: Visit<'ast> + ?Sized>(visitor: &mut V, statement: &'ast Statement) {
    match statement {
        Statement::Query(query) => visitor.visit_query(query),
        Statement::Insert {
            table_name,
            columns,
            source,
        } => visitor.visit_insert(table_name, columns, source),
        Statement::Copy {
            table_name,
            columns,
            values,
        } => visitor.visit_copy(table_name, columns, values),
        Statement::Update {
            table_name,
            assignments,
            selection,
        } => visitor.visit_update(table_name, assignments, selection.as_ref()),
        Statement::Delete {
            table_name,
            selection,
        } => visitor.visit_delete(table_name, selection.as_ref()),
        Statement::CreateView {
            name,
            columns,
            query,
            materialized,
            with_options,
        } => visitor.visit_create_view(name, columns, query, *materialized, with_options),
        Statement::Drop {
            object_type,
            if_exists,
            names,
            cascade,
        } => visitor.visit_drop(object_type, *if_exists, names, *cascade),
        Statement::CreateTable {
            name,
            columns,
            constraints,
            external,
            with_options,
            file_format,
            location,
        } => visitor.visit_create_table(
            name,
            columns,
            constraints,
            with_options,
            *external,
            file_format,
            location,
        ),
        Statement::AlterTable { name, operation } => visitor.visit_alter_table(name, operation),
        Statement::StartTransaction { modes } => visitor.visit_start_transaction(modes),
        Statement::SetTransaction { modes } => visitor.visit_set_transaction(modes),
        Statement::Commit { chain } => visitor.visit_commit(*chain),
        Statement::Rollback { chain } => visitor.visit_rollback(*chain),
    }
}

pub fn visit_query<'ast, V: Visit<'ast> + ?Sized>(visitor: &mut V, query: &'ast Query) {
    for cte in &query.ctes {
        visitor.visit_cte(cte);
    }
    visitor.visit_set_expr(&query.body);
    for order_by in &query.order_by {
        visitor.visit_order_by(order_by);
    }
    if let Some(limit) = &query.limit {
        visitor.visit_limit(limit);
    }
}

pub fn visit_cte<'ast, V: Visit<'ast> + ?Sized>(visitor: &mut V, cte: &'ast Cte) {
    visitor.visit_table_alias(&cte.alias);
    visitor.visit_query(&cte.query);
}

pub fn visit_select<'ast, V: Visit<'ast> + ?Sized>(visitor: &mut V, select: &'ast Select) {
    for select_item in &select.projection {
        visitor.visit_select_item(select_item)
    }
    for table_with_joins in &select.from {
        visitor.visit_table_with_joins(table_with_joins)
    }
    if let Some(selection) = &select.selection {
        visitor.visit_where(selection);
    }
    if !select.group_by.is_empty() {
        visitor.visit_group_by(&select.group_by);
    }
    if let Some(having) = &select.having {
        visitor.visit_having(having);
    }
}

pub fn visit_select_item<'ast, V: Visit<'ast> + ?Sized>(
    visitor: &mut V,
    select_item: &'ast SelectItem,
) {
    match select_item {
        SelectItem::UnnamedExpr(expr) => visitor.visit_unnamed_expr(expr),
        SelectItem::ExprWithAlias { expr, alias } => visitor.visit_expr_with_alias(expr, alias),
        SelectItem::QualifiedWildcard(object_name) => {
            visitor.visit_qualified_wildcard(&object_name.0)
        }
        SelectItem::Wildcard => visitor.visit_wildcard(),
    }
}

pub fn visit_table_with_joins<'ast, V: Visit<'ast> + ?Sized>(
    visitor: &mut V,
    table_with_joins: &'ast TableWithJoins,
) {
    visitor.visit_table_factor(&table_with_joins.relation);
    for join in &table_with_joins.joins {
        visitor.visit_join(&join);
    }
}

pub fn visit_table_factor<'ast, V: Visit<'ast> + ?Sized>(
    visitor: &mut V,
    table_factor: &'ast TableFactor,
) {
    match table_factor {
        TableFactor::Table {
            name,
            alias,
            args,
            with_hints,
        } => visitor.visit_table_table_factor(name, alias.as_ref(), args, with_hints),
        TableFactor::Derived {
            lateral,
            subquery,
            alias,
        } => visitor.visit_derived_table_factor(*lateral, subquery, alias.as_ref()),
        TableFactor::NestedJoin(table_with_joins) => {
            visitor.visit_nested_join_table_factor(table_with_joins)
        }
    }
}

pub fn visit_table_table_factor<'ast, V: Visit<'ast> + ?Sized>(
    visitor: &mut V,
    name: &'ast ObjectName,
    alias: Option<&'ast TableAlias>,
    args: &'ast [Expr],
    with_hints: &'ast [Expr],
) {
    visitor.visit_object_name(name);
    for expr in args {
        visitor.visit_expr(expr);
    }
    if let Some(alias) = alias {
        visitor.visit_table_alias(alias);
    }
    for expr in with_hints {
        visitor.visit_expr(expr);
    }
}

pub fn visit_derived_table_factor<'ast, V: Visit<'ast> + ?Sized>(
    visitor: &mut V,
    _lateral: bool,
    subquery: &'ast Query,
    alias: Option<&'ast TableAlias>,
) {
    visitor.visit_subquery(subquery);
    if let Some(alias) = alias {
        visitor.visit_table_alias(alias);
    }
}

pub fn visit_nested_join_table_factor<'ast, V: Visit<'ast> + ?Sized>(
    visitor: &mut V,
    table_with_joins: &'ast TableWithJoins,
) {
    visitor.visit_table_with_joins(table_with_joins);
}

pub fn visit_table_alias<'ast, V: Visit<'ast> + ?Sized>(visitor: &mut V, alias: &'ast TableAlias) {
    visitor.visit_ident(&alias.name);
    for column in &alias.columns {
        visitor.visit_ident(column);
    }
}

pub fn visit_join<'ast, V: Visit<'ast> + ?Sized>(visitor: &mut V, join: &'ast Join) {
    visitor.visit_table_factor(&join.relation);
    visitor.visit_join_operator(&join.join_operator);
}

pub fn visit_join_operator<'ast, V: Visit<'ast> + ?Sized>(visitor: &mut V, op: &'ast JoinOperator) {
    match op {
        JoinOperator::Inner(constraint) => visitor.visit_join_constraint(constraint),
        JoinOperator::LeftOuter(constraint) => visitor.visit_join_constraint(constraint),
        JoinOperator::RightOuter(constraint) => visitor.visit_join_constraint(constraint),
        JoinOperator::FullOuter(constraint) => visitor.visit_join_constraint(constraint),
        JoinOperator::CrossJoin | JoinOperator::CrossApply | JoinOperator::OuterApply => (),
    }
}

pub fn visit_join_constraint<'ast, V: Visit<'ast> + ?Sized>(
    visitor: &mut V,
    constraint: &'ast JoinConstraint,
) {
    match constraint {
        JoinConstraint::On(expr) => visitor.visit_expr(expr),
        JoinConstraint::Using(idents) => {
            for ident in idents {
                visitor.visit_ident(ident);
            }
        }
        JoinConstraint::Natural => (),
    }
}

pub fn visit_where<'ast, V: Visit<'ast> + ?Sized>(visitor: &mut V, expr: &'ast Expr) {
    visitor.visit_expr(expr);
}

pub fn visit_group_by<'ast, V: Visit<'ast> + ?Sized>(visitor: &mut V, exprs: &'ast [Expr]) {
    for expr in exprs {
        visitor.visit_expr(expr);
    }
}

pub fn visit_having<'ast, V: Visit<'ast> + ?Sized>(visitor: &mut V, expr: &'ast Expr) {
    visitor.visit_expr(expr);
}

pub fn visit_set_expr<'ast, V: Visit<'ast> + ?Sized>(visitor: &mut V, set_expr: &'ast SetExpr) {
    match set_expr {
        SetExpr::Select(select) => visitor.visit_select(select),
        SetExpr::Query(query) => visitor.visit_query(query),
        SetExpr::Values(values) => visitor.visit_values(values),
        SetExpr::SetOperation {
            left,
            op,
            right,
            all,
        } => visitor.visit_set_operation(left, op, right, *all),
    }
}

pub fn visit_set_operation<'ast, V: Visit<'ast> + ?Sized>(
    visitor: &mut V,
    left: &'ast SetExpr,
    op: &'ast SetOperator,
    right: &'ast SetExpr,
    _all: bool,
) {
    visitor.visit_set_expr(left);
    visitor.visit_set_operator(op);
    visitor.visit_set_expr(right);
}

pub fn visit_order_by<'ast, V: Visit<'ast> + ?Sized>(visitor: &mut V, order_by: &'ast OrderByExpr) {
    visitor.visit_expr(&order_by.expr);
}

pub fn visit_limit<'ast, V: Visit<'ast> + ?Sized>(visitor: &mut V, expr: &'ast Expr) {
    visitor.visit_expr(expr)
}

pub fn visit_expr<'ast, V: Visit<'ast> + ?Sized>(visitor: &mut V, expr: &'ast Expr) {
    match expr {
        Expr::Identifier(ident) => visitor.visit_ident(ident),
        Expr::Wildcard => visitor.visit_wildcard(),
        Expr::QualifiedWildcard(idents) => visitor.visit_qualified_wildcard(idents),
        Expr::CompoundIdentifier(idents) => visitor.visit_compound_identifier(idents),
        Expr::IsNull(expr) => visitor.visit_is_null(expr),
        Expr::IsNotNull(expr) => visitor.visit_is_not_null(expr),
        Expr::InList {
            expr,
            list,
            negated,
        } => visitor.visit_in_list(expr, list, *negated),
        Expr::InSubquery {
            expr,
            subquery,
            negated,
        } => visitor.visit_in_subquery(expr, subquery, *negated),
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => visitor.visit_between(expr, low, high, *negated),
        Expr::BinaryOp { left, op, right } => visitor.visit_binary_op(left, op, right),
        Expr::UnaryOp { expr, op } => visitor.visit_unary_op(expr, op),
        Expr::Cast { expr, data_type } => visitor.visit_cast(expr, data_type),
        Expr::Collate { expr, collation } => visitor.visit_collate(expr, collation),
        Expr::Extract { field, expr } => visitor.visit_extract(field, expr),
        Expr::Nested(expr) => visitor.visit_nested(expr),
        Expr::Value(val) => visitor.visit_value(val),
        Expr::Function(func) => visitor.visit_function(func),
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => visitor.visit_case(
            operand.as_ref().map(|o| o.as_ref()),
            conditions,
            results,
            else_result.as_ref().map(|r| r.as_ref()),
        ),
        Expr::Exists(query) => visitor.visit_exists(query),
        Expr::Subquery(query) => visitor.visit_subquery(query),
    }
}

pub fn visit_unnamed_expr<'ast, V: Visit<'ast> + ?Sized>(visitor: &mut V, expr: &'ast Expr) {
    visitor.visit_expr(expr);
}

pub fn visit_expr_with_alias<'ast, V: Visit<'ast> + ?Sized>(
    visitor: &mut V,
    expr: &'ast Expr,
    alias: &'ast Ident,
) {
    visitor.visit_expr(expr);
    visitor.visit_ident(alias);
}

pub fn visit_object_name<'ast, V: Visit<'ast> + ?Sized>(
    visitor: &mut V,
    object_name: &'ast ObjectName,
) {
    for ident in &object_name.0 {
        visitor.visit_ident(ident)
    }
}

pub fn visit_compound_identifier<'ast, V: Visit<'ast> + ?Sized>(
    visitor: &mut V,
    idents: &'ast [Ident],
) {
    for ident in idents {
        visitor.visit_ident(ident);
    }
}

pub fn visit_qualified_wildcard<'ast, V: Visit<'ast> + ?Sized>(
    visitor: &mut V,
    idents: &'ast [Ident],
) {
    for ident in idents {
        visitor.visit_ident(ident);
    }
}

pub fn visit_is_null<'ast, V: Visit<'ast> + ?Sized>(visitor: &mut V, expr: &'ast Expr) {
    visitor.visit_expr(expr);
}

pub fn visit_is_not_null<'ast, V: Visit<'ast> + ?Sized>(visitor: &mut V, expr: &'ast Expr) {
    visitor.visit_expr(expr);
}

pub fn visit_in_list<'ast, V: Visit<'ast> + ?Sized>(
    visitor: &mut V,
    expr: &'ast Expr,
    list: &'ast [Expr],
    _negated: bool,
) {
    visitor.visit_expr(expr);
    for e in list {
        visitor.visit_expr(e);
    }
}

pub fn visit_in_subquery<'ast, V: Visit<'ast> + ?Sized>(
    visitor: &mut V,
    expr: &'ast Expr,
    subquery: &'ast Query,
    _negated: bool,
) {
    visitor.visit_expr(expr);
    visitor.visit_query(subquery);
}

pub fn visit_between<'ast, V: Visit<'ast> + ?Sized>(
    visitor: &mut V,
    expr: &'ast Expr,
    low: &'ast Expr,
    high: &'ast Expr,
    _negated: bool,
) {
    visitor.visit_expr(expr);
    visitor.visit_expr(low);
    visitor.visit_expr(high);
}

pub fn visit_binary_op<'ast, V: Visit<'ast> + ?Sized>(
    visitor: &mut V,
    left: &'ast Expr,
    op: &'ast BinaryOperator,
    right: &'ast Expr,
) {
    visitor.visit_expr(left);
    visitor.visit_binary_operator(op);
    visitor.visit_expr(right);
}

pub fn visit_unary_op<'ast, V: Visit<'ast> + ?Sized>(
    visitor: &mut V,
    expr: &'ast Expr,
    op: &'ast UnaryOperator,
) {
    visitor.visit_expr(expr);
    visitor.visit_unary_operator(op);
}

pub fn visit_cast<'ast, V: Visit<'ast> + ?Sized>(
    visitor: &mut V,
    expr: &'ast Expr,
    data_type: &'ast DataType,
) {
    visitor.visit_expr(expr);
    visitor.visit_type(data_type);
}

pub fn visit_collate<'ast, V: Visit<'ast> + ?Sized>(
    visitor: &mut V,
    expr: &'ast Expr,
    collation: &'ast ObjectName,
) {
    visitor.visit_expr(expr);
    visitor.visit_object_name(collation);
}

pub fn visit_extract<'ast, V: Visit<'ast> + ?Sized>(
    visitor: &mut V,
    field: &'ast DateTimeField,
    expr: &'ast Expr,
) {
    visitor.visit_date_time_field(field);
    visitor.visit_expr(expr);
}

pub fn visit_nested<'ast, V: Visit<'ast> + ?Sized>(visitor: &mut V, expr: &'ast Expr) {
    visitor.visit_expr(expr);
}

pub fn visit_function<'ast, V: Visit<'ast> + ?Sized>(visitor: &mut V, func: &'ast Function) {
    visitor.visit_object_name(&func.name);
    for arg in &func.args {
        visitor.visit_expr(arg);
    }
    if let Some(over) = &func.over {
        visitor.visit_window_spec(over);
    }
}

pub fn visit_window_spec<'ast, V: Visit<'ast> + ?Sized>(
    visitor: &mut V,
    window_spec: &'ast WindowSpec,
) {
    for expr in &window_spec.partition_by {
        visitor.visit_expr(expr);
    }
    for order_by in &window_spec.order_by {
        visitor.visit_order_by(order_by);
    }
    if let Some(window_frame) = &window_spec.window_frame {
        visitor.visit_window_frame(window_frame);
    }
}

pub fn visit_window_frame<'ast, V: Visit<'ast> + ?Sized>(
    visitor: &mut V,
    window_frame: &'ast WindowFrame,
) {
    visitor.visit_window_frame_units(&window_frame.units);
    visitor.visit_window_frame_bound(&window_frame.start_bound);
    if let Some(end_bound) = &window_frame.end_bound {
        visitor.visit_window_frame_bound(end_bound);
    }
}

pub fn visit_case<'ast, V: Visit<'ast> + ?Sized>(
    visitor: &mut V,
    operand: Option<&'ast Expr>,
    conditions: &'ast [Expr],
    results: &'ast [Expr],
    else_result: Option<&'ast Expr>,
) {
    if let Some(operand) = operand {
        visitor.visit_expr(operand);
    }
    for cond in conditions {
        visitor.visit_expr(cond);
    }
    for res in results {
        visitor.visit_expr(res);
    }
    if let Some(else_result) = else_result {
        visitor.visit_expr(else_result);
    }
}

pub fn visit_exists<'ast, V: Visit<'ast> + ?Sized>(visitor: &mut V, subquery: &'ast Query) {
    visitor.visit_query(subquery)
}

pub fn visit_subquery<'ast, V: Visit<'ast> + ?Sized>(visitor: &mut V, subquery: &'ast Query) {
    visitor.visit_query(subquery)
}

pub fn visit_insert<'ast, V: Visit<'ast> + ?Sized>(
    visitor: &mut V,
    table_name: &'ast ObjectName,
    columns: &'ast [Ident],
    source: &'ast Query,
) {
    visitor.visit_object_name(table_name);
    for column in columns {
        visitor.visit_ident(column);
    }
    visitor.visit_query(source);
}

pub fn visit_values<'ast, V: Visit<'ast> + ?Sized>(visitor: &mut V, values: &'ast Values) {
    for row in &values.0 {
        visitor.visit_values_row(row)
    }
}

pub fn visit_values_row<'ast, V: Visit<'ast> + ?Sized>(visitor: &mut V, row: &'ast [Expr]) {
    for expr in row {
        visitor.visit_expr(expr)
    }
}

pub fn visit_copy<'ast, V: Visit<'ast> + ?Sized>(
    visitor: &mut V,
    table_name: &'ast ObjectName,
    columns: &'ast [Ident],
    values: &'ast [Option<String>],
) {
    visitor.visit_object_name(table_name);
    for column in columns {
        visitor.visit_ident(column);
    }
    visitor.visit_copy_values(values);
}

pub fn visit_copy_values<'ast, V: Visit<'ast> + ?Sized>(
    visitor: &mut V,
    values: &'ast [Option<String>],
) {
    for value in values {
        visitor.visit_copy_values_row(value.as_ref());
    }
}

pub fn visit_update<'ast, V: Visit<'ast> + ?Sized>(
    visitor: &mut V,
    table_name: &'ast ObjectName,
    assignments: &'ast [Assignment],
    selection: Option<&'ast Expr>,
) {
    visitor.visit_object_name(&table_name);
    for assignment in assignments {
        visitor.visit_assignment(assignment);
    }
    if let Some(selection) = selection {
        visitor.visit_where(selection);
    }
}

pub fn visit_assignment<'ast, V: Visit<'ast> + ?Sized>(
    visitor: &mut V,
    assignment: &'ast Assignment,
) {
    visitor.visit_ident(&assignment.id);
    visitor.visit_expr(&assignment.value);
}

pub fn visit_delete<'ast, V: Visit<'ast> + ?Sized>(
    visitor: &mut V,
    table_name: &'ast ObjectName,
    selection: Option<&'ast Expr>,
) {
    visitor.visit_object_name(table_name);
    if let Some(selection) = selection {
        visitor.visit_where(selection);
    }
}

pub fn visit_drop<'ast, V: Visit<'ast> + ?Sized>(
    visitor: &mut V,
    object_type: &'ast ObjectType,
    _if_exists: bool,
    names: &'ast [ObjectName],
    _cascade: bool,
) {
    visitor.visit_object_type(object_type);
    for name in names {
        visitor.visit_object_name(name);
    }
}

pub fn visit_create_view<'ast, V: Visit<'ast> + ?Sized>(
    visitor: &mut V,
    name: &'ast ObjectName,
    columns: &'ast [Ident],
    query: &'ast Query,
    _materialized: bool,
    with_options: &'ast [SqlOption],
) {
    visitor.visit_object_name(name);
    for column in columns {
        visitor.visit_ident(column);
    }
    for option in with_options {
        visitor.visit_option(option);
    }
    visitor.visit_query(&query);
}

pub fn visit_create_table<'ast, V: Visit<'ast> + ?Sized>(
    visitor: &mut V,
    name: &'ast ObjectName,
    columns: &'ast [ColumnDef],
    constraints: &'ast [TableConstraint],
    with_options: &'ast [SqlOption],
    _external: bool,
    file_format: &'ast Option<FileFormat>,
    location: &'ast Option<String>,
) {
    visitor.visit_object_name(name);
    for column in columns {
        visitor.visit_column_def(column);
    }
    for constraint in constraints {
        visitor.visit_table_constraint(constraint);
    }
    for option in with_options {
        visitor.visit_option(option);
    }
    if let Some(file_format) = file_format {
        visitor.visit_file_format(file_format);
    }
    if let Some(location) = location {
        visitor.visit_literal_string(location);
    }
}

pub fn visit_column_def<'ast, V: Visit<'ast> + ?Sized>(
    visitor: &mut V,
    column_def: &'ast ColumnDef,
) {
    visitor.visit_ident(&column_def.name);
    visitor.visit_type(&column_def.data_type);
    for option in &column_def.options {
        visitor.visit_column_option_def(option);
    }
}

pub fn visit_column_option_def<'ast, V: Visit<'ast> + ?Sized>(
    visitor: &mut V,
    column_option_def: &'ast ColumnOptionDef,
) {
    if let Some(name) = &column_option_def.name {
        visitor.visit_ident(name);
    }
    visitor.visit_column_option(&column_option_def.option)
}

pub fn visit_column_option<'ast, V: Visit<'ast> + ?Sized>(
    visitor: &mut V,
    column_option: &'ast ColumnOption,
) {
    match column_option {
        ColumnOption::Null | ColumnOption::NotNull | ColumnOption::Unique { .. } => (),
        ColumnOption::Default(expr) | ColumnOption::Check(expr) => visitor.visit_expr(expr),
        ColumnOption::ForeignKey {
            foreign_table,
            referred_columns,
        } => {
            visitor.visit_object_name(foreign_table);
            for column in referred_columns {
                visitor.visit_ident(column);
            }
        }
    }
}

pub fn visit_option<'ast, V: Visit<'ast> + ?Sized>(visitor: &mut V, option: &'ast SqlOption) {
    visitor.visit_ident(&option.name);
    visitor.visit_value(&option.value);
}

pub fn visit_alter_table<'ast, V: Visit<'ast> + ?Sized>(
    visitor: &mut V,
    name: &'ast ObjectName,
    operation: &'ast AlterTableOperation,
) {
    visitor.visit_object_name(name);
    visitor.visit_alter_table_operation(operation);
}

pub fn visit_alter_table_operation<'ast, V: Visit<'ast> + ?Sized>(
    visitor: &mut V,
    operation: &'ast AlterTableOperation,
) {
    match operation {
        AlterTableOperation::AddConstraint(table_constraint) => {
            visitor.visit_alter_add_constraint(table_constraint)
        }
        AlterTableOperation::DropConstraint { name } => visitor.visit_alter_drop_constraint(name),
    }
}

pub fn visit_alter_add_constraint<'ast, V: Visit<'ast> + ?Sized>(
    visitor: &mut V,
    table_constraint: &'ast TableConstraint,
) {
    visitor.visit_table_constraint(table_constraint);
}

pub fn visit_table_constraint<'ast, V: Visit<'ast> + ?Sized>(
    visitor: &mut V,
    table_constraint: &'ast TableConstraint,
) {
    match table_constraint {
        TableConstraint::Unique {
            name,
            columns,
            is_primary,
        } => visitor.visit_table_constraint_unique(name.as_ref(), columns, *is_primary),
        TableConstraint::ForeignKey {
            name,
            columns,
            foreign_table,
            referred_columns,
        } => visitor.visit_table_constraint_foreign_key(
            name.as_ref(),
            columns,
            foreign_table,
            referred_columns,
        ),
        TableConstraint::Check { name, expr } => {
            visitor.visit_table_constraint_check(name.as_ref(), expr)
        }
    }
}

pub fn visit_table_constraint_unique<'ast, V: Visit<'ast> + ?Sized>(
    visitor: &mut V,
    name: Option<&'ast Ident>,
    columns: &'ast [Ident],
    _is_primary: bool,
) {
    if let Some(name) = name {
        visitor.visit_ident(name);
    }
    for column in columns {
        visitor.visit_ident(column);
    }
}

pub fn visit_table_constraint_foreign_key<'ast, V: Visit<'ast> + ?Sized>(
    visitor: &mut V,
    name: Option<&'ast Ident>,
    columns: &'ast [Ident],
    foreign_table: &'ast ObjectName,
    referred_columns: &'ast [Ident],
) {
    if let Some(name) = name {
        visitor.visit_ident(name);
    }
    for column in columns {
        visitor.visit_ident(column);
    }
    visitor.visit_object_name(foreign_table);
    for column in referred_columns {
        visitor.visit_ident(column);
    }
}

pub fn visit_table_constraint_check<'ast, V: Visit<'ast> + ?Sized>(
    visitor: &mut V,
    name: Option<&'ast Ident>,
    expr: &'ast Expr,
) {
    if let Some(name) = name {
        visitor.visit_ident(name);
    }
    visitor.visit_expr(expr);
}

pub fn visit_alter_drop_constraint<'ast, V: Visit<'ast> + ?Sized>(
    visitor: &mut V,
    name: &'ast Ident,
) {
    visitor.visit_ident(name);
}

pub fn visit_start_transaction<'ast, V: Visit<'ast> + ?Sized>(
    visitor: &mut V,
    modes: &'ast [TransactionMode],
) {
    for mode in modes {
        visitor.visit_transaction_mode(mode)
    }
}

pub fn visit_set_transaction<'ast, V: Visit<'ast> + ?Sized>(
    visitor: &mut V,
    modes: &'ast [TransactionMode],
) {
    for mode in modes {
        visitor.visit_transaction_mode(mode)
    }
}

pub fn visit_transaction_mode<'ast, V: Visit<'ast> + ?Sized>(
    visitor: &mut V,
    mode: &'ast TransactionMode,
) {
    match mode {
        TransactionMode::AccessMode(access_mode) => {
            visitor.visit_transaction_access_mode(access_mode)
        }
        TransactionMode::IsolationLevel(isolation_level) => {
            visitor.visit_transaction_isolation_level(isolation_level)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Visit;
    use crate::ast::Ident;
    use crate::dialect::GenericDialect;
    use crate::parser::Parser;
    use std::error::Error;

    #[test]
    fn test_basic_visitor() -> Result<(), Box<dyn Error>> {
        struct Visitor<'a> {
            seen_idents: Vec<&'a Ident>,
        }

        impl<'a> Visit<'a> for Visitor<'a> {
            fn visit_ident(&mut self, ident: &'a Ident) {
                self.seen_idents.push(ident);
            }
        }

        let stmts = Parser::parse_sql(
            &GenericDialect {},
            r#"
            WITH a01 AS (SELECT 1)
                SELECT *, a02.*, a03 AS a04
                FROM (SELECT * FROM a05) a06 (a07)
                JOIN a08 ON a09.a10 = a11.a12
                WHERE a13
                GROUP BY a14
                HAVING a15
            UNION ALL
                SELECT a16 IS NULL
                    AND a17 IS NOT NULL
                    AND a18 IN (a19)
                    AND a20 IN (SELECT * FROM a21)
                    AND CAST(a22 AS int)
                    AND (a23)
                    AND NOT a24
                    AND a25(a26)
                    AND CASE a27 WHEN a28 THEN a29 ELSE a30 END
                    AND a31 BETWEEN a32 AND a33
                    AND a34 COLLATE a35 = a36
                    AND EXTRACT(YEAR FROM a37)
                    AND (SELECT a38)
                    AND EXISTS (SELECT a39)
                FROM a40(a41) AS a42 WITH (a43)
                LEFT JOIN a44 ON false
                RIGHT JOIN a45 ON false
                FULL JOIN a46 ON false
                JOIN a47 (a48) USING (a49)
                NATURAL JOIN (a50 NATURAL JOIN a51)
            EXCEPT
                (SELECT a52(a53) OVER (PARTITION BY a54 ORDER BY a55 ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING))
            ORDER BY a56
            LIMIT 1;
            UPDATE b01 SET b02 = b03 WHERE b04;
            INSERT INTO c01 (c02) VALUES (c03);
            INSERT INTO c04 SELECT * FROM c05;
            DELETE FROM d01 WHERE d02;
            CREATE TABLE e01 (
                e02 INT PRIMARY KEY DEFAULT e03 CHECK (e04),
                CHECK (e05)
            ) WITH (e06 = 1);
            CREATE VIEW f01 (f02) WITH (f03 = 1) AS SELECT * FROM f04;
            ALTER TABLE g01 ADD CONSTRAINT g02 PRIMARY KEY (g03);
            ALTER TABLE h01 ADD CONSTRAINT h02 FOREIGN KEY (h03) REFERENCES h04 (h05);
            ALTER TABLE i01 ADD CONSTRAINT i02 UNIQUE (i03);
            DROP TABLE j01;
            DROP VIEW k01;
            COPY l01 (l02) FROM stdin;
            START TRANSACTION READ ONLY;
            SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
            COMMIT;
            ROLLBACK;
"#
            .into(),
        )?;

        let mut visitor = Visitor {
            seen_idents: Vec::new(),
        };
        for stmt in &stmts {
            visitor.visit_statement(stmt);
        }

        #[rustfmt::skip]  // rustfmt loses the structure of the expected vector by wrapping all lines
        assert_eq!(
            visitor.seen_idents,
            vec![
                "a01", "a02", "a03", "a04", "a05", "a06", "a07", "a08", "a09", "a10", "a11", "a12",
                "a13", "a14", "a15", "a16", "a17", "a18", "a19", "a20", "a21", "a22", "a23", "a24",
                "a25", "a26", "a27", "a28", "a29", "a30", "a31", "a32", "a33", "a34", "a35", "a36",
                "a37", "a38", "a39", "a40", "a41", "a42", "a43", "a44", "a45", "a46", "a47", "a48",
                "a49", "a50", "a51", "a52", "a53", "a54", "a55", "a56",
                "b01", "b02", "b03", "b04",
                "c01", "c02", "c03", "c04", "c05",
                "d01", "d02",
                "e01", "e02", "e03", "e04", "e05", "e06",
                "f01", "f02", "f03", "f04",
                "g01", "g02", "g03",
                "h01", "h02", "h03", "h04", "h05",
                "i01", "i02", "i03",
                "j01",
                "k01",
                "l01", "l02",
            ]
        );

        Ok(())
    }
}

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

// Disable lints that want us to rewrite `&SQLIdent` as `&str`, as `&str` is not
// as self-documenting as &SQLIdent.
#![allow(clippy::ptr_arg)]

use super::*;

/// A trait that represents a visitor that walks through a SQL AST.
///
/// Each function corresponds to a node in the SQL AST, and has a default
/// implementation that visits all of its child nodes. Implementors of this
/// trait can override functions as desired to hook into AST traversal without
/// writing code to traverse the entire AST.
pub trait Visit<'ast> {
    fn visit_statement(&mut self, statement: &'ast SQLStatement) {
        visit_statement(self, statement)
    }

    fn visit_query(&mut self, query: &'ast SQLQuery) {
        visit_query(self, query)
    }

    fn visit_cte(&mut self, cte: &'ast Cte) {
        visit_cte(self, cte)
    }

    fn visit_select(&mut self, select: &'ast SQLSelect) {
        visit_select(self, select)
    }

    fn visit_select_item(&mut self, select_item: &'ast SQLSelectItem) {
        visit_select_item(self, select_item)
    }

    fn visit_table_factor(&mut self, table_factor: &'ast TableFactor) {
        visit_table_factor(self, table_factor)
    }

    fn visit_table_table_factor(
        &mut self,
        name: &'ast SQLObjectName,
        alias: Option<&'ast TableAlias>,
        args: &'ast [ASTNode],
        with_hints: &'ast [ASTNode],
    ) {
        visit_table_table_factor(self, name, alias, args, with_hints)
    }

    fn visit_derived_table_factor(
        &mut self,
        lateral: bool,
        subquery: &'ast SQLQuery,
        alias: Option<&'ast TableAlias>,
    ) {
        visit_derived_table_factor(self, lateral, subquery, alias)
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

    fn visit_where(&mut self, expr: &'ast ASTNode) {
        visit_where(self, expr)
    }

    fn visit_group_by(&mut self, exprs: &'ast [ASTNode]) {
        visit_group_by(self, exprs)
    }

    fn visit_having(&mut self, expr: &'ast ASTNode) {
        visit_having(self, expr)
    }

    fn visit_set_expr(&mut self, set_expr: &'ast SQLSetExpr) {
        visit_set_expr(self, set_expr)
    }

    fn visit_set_operation(
        &mut self,
        left: &'ast SQLSetExpr,
        op: &'ast SQLSetOperator,
        right: &'ast SQLSetExpr,
        all: bool,
    ) {
        visit_set_operation(self, left, op, right, all)
    }

    fn visit_set_operator(&mut self, _operator: &'ast SQLSetOperator) {}

    fn visit_order_by(&mut self, order_by: &'ast SQLOrderByExpr) {
        visit_order_by(self, order_by)
    }

    fn visit_limit(&mut self, expr: &'ast ASTNode) {
        visit_limit(self, expr)
    }

    fn visit_type(&mut self, _data_type: &'ast SQLType) {}

    fn visit_expr(&mut self, expr: &'ast ASTNode) {
        visit_expr(self, expr)
    }

    fn visit_unnamed_expression(&mut self, expr: &'ast ASTNode) {
        visit_unnamed_expression(self, expr)
    }

    fn visit_expression_with_alias(&mut self, expr: &'ast ASTNode, alias: &'ast SQLIdent) {
        visit_expression_with_alias(self, expr, alias)
    }

    fn visit_object_name(&mut self, object_name: &'ast SQLObjectName) {
        visit_object_name(self, object_name)
    }

    fn visit_ident(&mut self, _ident: &'ast SQLIdent) {}

    fn visit_compound_identifier(&mut self, idents: &'ast [SQLIdent]) {
        visit_compound_identifier(self, idents)
    }

    fn visit_wildcard(&mut self) {}

    fn visit_qualified_wildcard(&mut self, idents: &'ast [SQLIdent]) {
        visit_qualified_wildcard(self, idents)
    }

    fn visit_is_null(&mut self, expr: &'ast ASTNode) {
        visit_is_null(self, expr)
    }

    fn visit_is_not_null(&mut self, expr: &'ast ASTNode) {
        visit_is_not_null(self, expr)
    }

    fn visit_in_list(&mut self, expr: &'ast ASTNode, list: &'ast [ASTNode], negated: bool) {
        visit_in_list(self, expr, list, negated)
    }

    fn visit_in_subquery(&mut self, expr: &'ast ASTNode, subquery: &'ast SQLQuery, negated: bool) {
        visit_in_subquery(self, expr, subquery, negated)
    }

    fn visit_between(
        &mut self,
        expr: &'ast ASTNode,
        low: &'ast ASTNode,
        high: &'ast ASTNode,
        negated: bool,
    ) {
        visit_between(self, expr, low, high, negated)
    }

    fn visit_binary_expr(
        &mut self,
        left: &'ast ASTNode,
        op: &'ast SQLOperator,
        right: &'ast ASTNode,
    ) {
        visit_binary_expr(self, left, op, right)
    }

    fn visit_operator(&mut self, _op: &'ast SQLOperator) {}

    fn visit_cast(&mut self, expr: &'ast ASTNode, data_type: &'ast SQLType) {
        visit_cast(self, expr, data_type)
    }

    fn visit_collate(&mut self, expr: &'ast ASTNode, collation: &'ast SQLObjectName) {
        visit_collate(self, expr, collation)
    }

    fn visit_extract(&mut self, field: &'ast SQLDateTimeField, expr: &'ast ASTNode) {
        visit_extract(self, field, expr)
    }

    fn visit_date_time_field(&mut self, _field: &'ast SQLDateTimeField) {}

    fn visit_nested(&mut self, expr: &'ast ASTNode) {
        visit_nested(self, expr)
    }

    fn visit_unary(&mut self, expr: &'ast ASTNode, op: &'ast SQLOperator) {
        visit_unary(self, expr, op)
    }

    fn visit_value(&mut self, _val: &'ast Value) {}

    fn visit_function(&mut self, func: &'ast SQLFunction) {
        visit_function(self, func)
    }

    fn visit_window_spec(&mut self, window_spec: &'ast SQLWindowSpec) {
        visit_window_spec(self, window_spec)
    }

    fn visit_window_frame(&mut self, window_frame: &'ast SQLWindowFrame) {
        visit_window_frame(self, window_frame)
    }

    fn visit_window_frame_units(&mut self, _window_frame_units: &'ast SQLWindowFrameUnits) {}

    fn visit_window_frame_bound(&mut self, _window_frame_bound: &'ast SQLWindowFrameBound) {}

    fn visit_case(
        &mut self,
        operand: Option<&'ast ASTNode>,
        conditions: &'ast [ASTNode],
        results: &'ast [ASTNode],
        else_result: Option<&'ast ASTNode>,
    ) {
        visit_case(self, operand, conditions, results, else_result)
    }

    fn visit_exists(&mut self, subquery: &'ast SQLQuery) {
        visit_exists(self, subquery)
    }

    fn visit_subquery(&mut self, subquery: &'ast SQLQuery) {
        visit_subquery(self, subquery)
    }

    fn visit_insert(
        &mut self,
        table_name: &'ast SQLObjectName,
        columns: &'ast [SQLIdent],
        source: &'ast SQLQuery,
    ) {
        visit_insert(self, table_name, columns, source)
    }

    fn visit_values(&mut self, values: &'ast SQLValues) {
        visit_values(self, values)
    }

    fn visit_values_row(&mut self, row: &'ast [ASTNode]) {
        visit_values_row(self, row)
    }

    fn visit_copy(
        &mut self,
        table_name: &'ast SQLObjectName,
        columns: &'ast [SQLIdent],
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
        table_name: &'ast SQLObjectName,
        assignments: &'ast [SQLAssignment],
        selection: Option<&'ast ASTNode>,
    ) {
        visit_update(self, table_name, assignments, selection)
    }

    fn visit_assignment(&mut self, assignment: &'ast SQLAssignment) {
        visit_assignment(self, assignment)
    }

    fn visit_delete(&mut self, table_name: &'ast SQLObjectName, selection: Option<&'ast ASTNode>) {
        visit_delete(self, table_name, selection)
    }

    fn visit_literal_string(&mut self, _string: &'ast String) {}

    fn visit_create_view(
        &mut self,
        name: &'ast SQLObjectName,
        columns: &'ast [SQLIdent],
        query: &'ast SQLQuery,
        materialized: bool,
        with_options: &'ast [SQLOption],
    ) {
        visit_create_view(self, name, columns, query, materialized, with_options)
    }

    fn visit_create_table(
        &mut self,
        name: &'ast SQLObjectName,
        columns: &'ast [SQLColumnDef],
        constraints: &'ast [TableConstraint],
        with_options: &'ast [SQLOption],
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

    fn visit_column_def(&mut self, column_def: &'ast SQLColumnDef) {
        visit_column_def(self, column_def)
    }

    fn visit_column_default(&mut self, default: Option<&'ast ASTNode>) {
        visit_column_default(self, default)
    }

    fn visit_file_format(&mut self, _file_format: &'ast FileFormat) {}

    fn visit_option(&mut self, option: &'ast SQLOption) {
        visit_option(self, option)
    }

    fn visit_drop(
        &mut self,
        object_type: &'ast SQLObjectType,
        if_exists: bool,
        names: &'ast [SQLObjectName],
        cascade: bool,
    ) {
        visit_drop(self, object_type, if_exists, names, cascade)
    }

    fn visit_object_type(&mut self, _object_type: &'ast SQLObjectType) {}

    fn visit_alter_table(
        &mut self,
        name: &'ast SQLObjectName,
        operation: &'ast AlterTableOperation,
    ) {
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
        name: Option<&'ast SQLIdent>,
        columns: &'ast [SQLIdent],
        is_primary: bool,
    ) {
        visit_table_constraint_unique(self, name, columns, is_primary)
    }

    fn visit_table_constraint_foreign_key(
        &mut self,
        name: Option<&'ast SQLIdent>,
        columns: &'ast [SQLIdent],
        foreign_table: &'ast SQLObjectName,
        referred_columns: &'ast [SQLIdent],
    ) {
        visit_table_constraint_foreign_key(self, name, columns, foreign_table, referred_columns)
    }

    fn visit_table_constraint_check(&mut self, name: Option<&'ast SQLIdent>, expr: &'ast ASTNode) {
        visit_table_constraint_check(self, name, expr)
    }

    fn visit_alter_drop_constraint(&mut self, name: &'ast SQLIdent) {
        visit_alter_drop_constraint(self, name)
    }
}

pub fn visit_statement<'ast, V: Visit<'ast> + ?Sized>(
    visitor: &mut V,
    statement: &'ast SQLStatement,
) {
    match statement {
        SQLStatement::SQLQuery(query) => visitor.visit_query(query),
        SQLStatement::SQLInsert {
            table_name,
            columns,
            source,
        } => visitor.visit_insert(table_name, columns, source),
        SQLStatement::SQLCopy {
            table_name,
            columns,
            values,
        } => visitor.visit_copy(table_name, columns, values),
        SQLStatement::SQLUpdate {
            table_name,
            assignments,
            selection,
        } => visitor.visit_update(table_name, assignments, selection.as_ref()),
        SQLStatement::SQLDelete {
            table_name,
            selection,
        } => visitor.visit_delete(table_name, selection.as_ref()),
        SQLStatement::SQLCreateView {
            name,
            columns,
            query,
            materialized,
            with_options,
        } => visitor.visit_create_view(name, columns, query, *materialized, with_options),
        SQLStatement::SQLDrop {
            object_type,
            if_exists,
            names,
            cascade,
        } => visitor.visit_drop(object_type, *if_exists, names, *cascade),
        SQLStatement::SQLCreateTable {
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
        SQLStatement::SQLAlterTable { name, operation } => {
            visitor.visit_alter_table(name, operation)
        }
    }
}

pub fn visit_query<'ast, V: Visit<'ast> + ?Sized>(visitor: &mut V, query: &'ast SQLQuery) {
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
    visitor.visit_ident(&cte.alias);
    visitor.visit_query(&cte.query);
}

pub fn visit_select<'ast, V: Visit<'ast> + ?Sized>(visitor: &mut V, select: &'ast SQLSelect) {
    for select_item in &select.projection {
        visitor.visit_select_item(select_item)
    }
    if let Some(relation) = &select.relation {
        visitor.visit_table_factor(relation);
    }
    for join in &select.joins {
        visitor.visit_join(join);
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
    select_item: &'ast SQLSelectItem,
) {
    match select_item {
        SQLSelectItem::UnnamedExpression(expr) => visitor.visit_unnamed_expression(expr),
        SQLSelectItem::ExpressionWithAlias { expr, alias } => {
            visitor.visit_expression_with_alias(expr, alias)
        }
        SQLSelectItem::QualifiedWildcard(object_name) => {
            visitor.visit_qualified_wildcard(&object_name.0)
        }
        SQLSelectItem::Wildcard => visitor.visit_wildcard(),
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
    }
}

pub fn visit_table_table_factor<'ast, V: Visit<'ast> + ?Sized>(
    visitor: &mut V,
    name: &'ast SQLObjectName,
    alias: Option<&'ast TableAlias>,
    args: &'ast [ASTNode],
    with_hints: &'ast [ASTNode],
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
    subquery: &'ast SQLQuery,
    alias: Option<&'ast TableAlias>,
) {
    visitor.visit_subquery(subquery);
    if let Some(alias) = alias {
        visitor.visit_table_alias(alias);
    }
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
        JoinOperator::Implicit | JoinOperator::Cross => (),
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

pub fn visit_where<'ast, V: Visit<'ast> + ?Sized>(visitor: &mut V, expr: &'ast ASTNode) {
    visitor.visit_expr(expr);
}

pub fn visit_group_by<'ast, V: Visit<'ast> + ?Sized>(visitor: &mut V, exprs: &'ast [ASTNode]) {
    for expr in exprs {
        visitor.visit_expr(expr);
    }
}

pub fn visit_having<'ast, V: Visit<'ast> + ?Sized>(visitor: &mut V, expr: &'ast ASTNode) {
    visitor.visit_expr(expr);
}

pub fn visit_set_expr<'ast, V: Visit<'ast> + ?Sized>(visitor: &mut V, set_expr: &'ast SQLSetExpr) {
    match set_expr {
        SQLSetExpr::Select(select) => visitor.visit_select(select),
        SQLSetExpr::Query(query) => visitor.visit_query(query),
        SQLSetExpr::Values(values) => visitor.visit_values(values),
        SQLSetExpr::SetOperation {
            left,
            op,
            right,
            all,
        } => visitor.visit_set_operation(left, op, right, *all),
    }
}

pub fn visit_set_operation<'ast, V: Visit<'ast> + ?Sized>(
    visitor: &mut V,
    left: &'ast SQLSetExpr,
    op: &'ast SQLSetOperator,
    right: &'ast SQLSetExpr,
    _all: bool,
) {
    visitor.visit_set_expr(left);
    visitor.visit_set_operator(op);
    visitor.visit_set_expr(right);
}

pub fn visit_order_by<'ast, V: Visit<'ast> + ?Sized>(
    visitor: &mut V,
    order_by: &'ast SQLOrderByExpr,
) {
    visitor.visit_expr(&order_by.expr);
}

pub fn visit_limit<'ast, V: Visit<'ast> + ?Sized>(visitor: &mut V, expr: &'ast ASTNode) {
    visitor.visit_expr(expr)
}

pub fn visit_expr<'ast, V: Visit<'ast> + ?Sized>(visitor: &mut V, expr: &'ast ASTNode) {
    match expr {
        ASTNode::SQLIdentifier(ident) => visitor.visit_ident(ident),
        ASTNode::SQLWildcard => visitor.visit_wildcard(),
        ASTNode::SQLQualifiedWildcard(idents) => visitor.visit_qualified_wildcard(idents),
        ASTNode::SQLCompoundIdentifier(idents) => visitor.visit_compound_identifier(idents),
        ASTNode::SQLIsNull(expr) => visitor.visit_is_null(expr),
        ASTNode::SQLIsNotNull(expr) => visitor.visit_is_not_null(expr),
        ASTNode::SQLInList {
            expr,
            list,
            negated,
        } => visitor.visit_in_list(expr, list, *negated),
        ASTNode::SQLInSubquery {
            expr,
            subquery,
            negated,
        } => visitor.visit_in_subquery(expr, subquery, *negated),
        ASTNode::SQLBetween {
            expr,
            negated,
            low,
            high,
        } => visitor.visit_between(expr, low, high, *negated),
        ASTNode::SQLBinaryExpr { left, op, right } => visitor.visit_binary_expr(left, op, right),
        ASTNode::SQLCast { expr, data_type } => visitor.visit_cast(expr, data_type),
        ASTNode::SQLCollate { expr, collation } => visitor.visit_collate(expr, collation),
        ASTNode::SQLExtract { field, expr } => visitor.visit_extract(field, expr),
        ASTNode::SQLNested(expr) => visitor.visit_nested(expr),
        ASTNode::SQLUnary { expr, operator } => visitor.visit_unary(expr, operator),
        ASTNode::SQLValue(val) => visitor.visit_value(val),
        ASTNode::SQLFunction(func) => visitor.visit_function(func),
        ASTNode::SQLCase {
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
        ASTNode::SQLExists(query) => visitor.visit_exists(query),
        ASTNode::SQLSubquery(query) => visitor.visit_subquery(query),
    }
}

pub fn visit_unnamed_expression<'ast, V: Visit<'ast> + ?Sized>(
    visitor: &mut V,
    expr: &'ast ASTNode,
) {
    visitor.visit_expr(expr);
}

pub fn visit_expression_with_alias<'ast, V: Visit<'ast> + ?Sized>(
    visitor: &mut V,
    expr: &'ast ASTNode,
    alias: &'ast SQLIdent,
) {
    visitor.visit_expr(expr);
    visitor.visit_ident(alias);
}

pub fn visit_object_name<'ast, V: Visit<'ast> + ?Sized>(
    visitor: &mut V,
    object_name: &'ast SQLObjectName,
) {
    for ident in &object_name.0 {
        visitor.visit_ident(ident)
    }
}

pub fn visit_compound_identifier<'ast, V: Visit<'ast> + ?Sized>(
    visitor: &mut V,
    idents: &'ast [SQLIdent],
) {
    for ident in idents {
        visitor.visit_ident(ident);
    }
}

pub fn visit_qualified_wildcard<'ast, V: Visit<'ast> + ?Sized>(
    visitor: &mut V,
    idents: &'ast [SQLIdent],
) {
    for ident in idents {
        visitor.visit_ident(ident);
    }
}

pub fn visit_is_null<'ast, V: Visit<'ast> + ?Sized>(visitor: &mut V, expr: &'ast ASTNode) {
    visitor.visit_expr(expr);
}

pub fn visit_is_not_null<'ast, V: Visit<'ast> + ?Sized>(visitor: &mut V, expr: &'ast ASTNode) {
    visitor.visit_expr(expr);
}

pub fn visit_in_list<'ast, V: Visit<'ast> + ?Sized>(
    visitor: &mut V,
    expr: &'ast ASTNode,
    list: &'ast [ASTNode],
    _negated: bool,
) {
    visitor.visit_expr(expr);
    for e in list {
        visitor.visit_expr(e);
    }
}

pub fn visit_in_subquery<'ast, V: Visit<'ast> + ?Sized>(
    visitor: &mut V,
    expr: &'ast ASTNode,
    subquery: &'ast SQLQuery,
    _negated: bool,
) {
    visitor.visit_expr(expr);
    visitor.visit_query(subquery);
}

pub fn visit_between<'ast, V: Visit<'ast> + ?Sized>(
    visitor: &mut V,
    expr: &'ast ASTNode,
    low: &'ast ASTNode,
    high: &'ast ASTNode,
    _negated: bool,
) {
    visitor.visit_expr(expr);
    visitor.visit_expr(low);
    visitor.visit_expr(high);
}

pub fn visit_binary_expr<'ast, V: Visit<'ast> + ?Sized>(
    visitor: &mut V,
    left: &'ast ASTNode,
    op: &'ast SQLOperator,
    right: &'ast ASTNode,
) {
    visitor.visit_expr(left);
    visitor.visit_operator(op);
    visitor.visit_expr(right);
}

pub fn visit_cast<'ast, V: Visit<'ast> + ?Sized>(
    visitor: &mut V,
    expr: &'ast ASTNode,
    data_type: &'ast SQLType,
) {
    visitor.visit_expr(expr);
    visitor.visit_type(data_type);
}

pub fn visit_collate<'ast, V: Visit<'ast> + ?Sized>(
    visitor: &mut V,
    expr: &'ast ASTNode,
    collation: &'ast SQLObjectName,
) {
    visitor.visit_expr(expr);
    visitor.visit_object_name(collation);
}

pub fn visit_extract<'ast, V: Visit<'ast> + ?Sized>(
    visitor: &mut V,
    field: &'ast SQLDateTimeField,
    expr: &'ast ASTNode,
) {
    visitor.visit_date_time_field(field);
    visitor.visit_expr(expr);
}

pub fn visit_nested<'ast, V: Visit<'ast> + ?Sized>(visitor: &mut V, expr: &'ast ASTNode) {
    visitor.visit_expr(expr);
}

pub fn visit_unary<'ast, V: Visit<'ast> + ?Sized>(
    visitor: &mut V,
    expr: &'ast ASTNode,
    op: &'ast SQLOperator,
) {
    visitor.visit_expr(expr);
    visitor.visit_operator(op);
}

pub fn visit_function<'ast, V: Visit<'ast> + ?Sized>(visitor: &mut V, func: &'ast SQLFunction) {
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
    window_spec: &'ast SQLWindowSpec,
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
    window_frame: &'ast SQLWindowFrame,
) {
    visitor.visit_window_frame_units(&window_frame.units);
    visitor.visit_window_frame_bound(&window_frame.start_bound);
    if let Some(end_bound) = &window_frame.end_bound {
        visitor.visit_window_frame_bound(end_bound);
    }
}

pub fn visit_case<'ast, V: Visit<'ast> + ?Sized>(
    visitor: &mut V,
    operand: Option<&'ast ASTNode>,
    conditions: &'ast [ASTNode],
    results: &'ast [ASTNode],
    else_result: Option<&'ast ASTNode>,
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

pub fn visit_exists<'ast, V: Visit<'ast> + ?Sized>(visitor: &mut V, subquery: &'ast SQLQuery) {
    visitor.visit_query(subquery)
}

pub fn visit_subquery<'ast, V: Visit<'ast> + ?Sized>(visitor: &mut V, subquery: &'ast SQLQuery) {
    visitor.visit_query(subquery)
}

pub fn visit_insert<'ast, V: Visit<'ast> + ?Sized>(
    visitor: &mut V,
    table_name: &'ast SQLObjectName,
    columns: &'ast [SQLIdent],
    source: &'ast SQLQuery,
) {
    visitor.visit_object_name(table_name);
    for column in columns {
        visitor.visit_ident(column);
    }
    visitor.visit_query(source);
}

pub fn visit_values<'ast, V: Visit<'ast> + ?Sized>(visitor: &mut V, values: &'ast SQLValues) {
    for row in &values.0 {
        visitor.visit_values_row(row)
    }
}

pub fn visit_values_row<'ast, V: Visit<'ast> + ?Sized>(visitor: &mut V, row: &'ast [ASTNode]) {
    for expr in row {
        visitor.visit_expr(expr)
    }
}

pub fn visit_copy<'ast, V: Visit<'ast> + ?Sized>(
    visitor: &mut V,
    table_name: &'ast SQLObjectName,
    columns: &'ast [SQLIdent],
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
    table_name: &'ast SQLObjectName,
    assignments: &'ast [SQLAssignment],
    selection: Option<&'ast ASTNode>,
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
    assignment: &'ast SQLAssignment,
) {
    visitor.visit_ident(&assignment.id);
    visitor.visit_expr(&assignment.value);
}

pub fn visit_delete<'ast, V: Visit<'ast> + ?Sized>(
    visitor: &mut V,
    table_name: &'ast SQLObjectName,
    selection: Option<&'ast ASTNode>,
) {
    visitor.visit_object_name(table_name);
    if let Some(selection) = selection {
        visitor.visit_where(selection);
    }
}

pub fn visit_drop<'ast, V: Visit<'ast> + ?Sized>(
    visitor: &mut V,
    object_type: &'ast SQLObjectType,
    _if_exists: bool,
    names: &'ast [SQLObjectName],
    _cascade: bool,
) {
    visitor.visit_object_type(object_type);
    for name in names {
        visitor.visit_object_name(name);
    }
}

pub fn visit_create_view<'ast, V: Visit<'ast> + ?Sized>(
    visitor: &mut V,
    name: &'ast SQLObjectName,
    columns: &'ast [SQLIdent],
    query: &'ast SQLQuery,
    _materialized: bool,
    with_options: &'ast [SQLOption],
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
    name: &'ast SQLObjectName,
    columns: &'ast [SQLColumnDef],
    constraints: &'ast [TableConstraint],
    with_options: &'ast [SQLOption],
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
    column_def: &'ast SQLColumnDef,
) {
    visitor.visit_ident(&column_def.name);
    visitor.visit_type(&column_def.data_type);
    visitor.visit_column_default(column_def.default.as_ref());
}

pub fn visit_column_default<'ast, V: Visit<'ast> + ?Sized>(
    visitor: &mut V,
    default: Option<&'ast ASTNode>,
) {
    if let Some(default) = default {
        visitor.visit_expr(default);
    }
}

pub fn visit_option<'ast, V: Visit<'ast> + ?Sized>(visitor: &mut V, option: &'ast SQLOption) {
    visitor.visit_ident(&option.name);
    visitor.visit_value(&option.value);
}

pub fn visit_alter_table<'ast, V: Visit<'ast> + ?Sized>(
    visitor: &mut V,
    name: &'ast SQLObjectName,
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
    name: Option<&'ast SQLIdent>,
    columns: &'ast [SQLIdent],
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
    name: Option<&'ast SQLIdent>,
    columns: &'ast [SQLIdent],
    foreign_table: &'ast SQLObjectName,
    referred_columns: &'ast [SQLIdent],
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
    name: Option<&'ast SQLIdent>,
    expr: &'ast ASTNode,
) {
    if let Some(name) = name {
        visitor.visit_ident(name);
    }
    visitor.visit_expr(expr);
}

pub fn visit_alter_drop_constraint<'ast, V: Visit<'ast> + ?Sized>(
    visitor: &mut V,
    name: &'ast SQLIdent,
) {
    visitor.visit_ident(name);
}

#[cfg(test)]
mod tests {
    use super::Visit;
    use crate::dialect::GenericSqlDialect;
    use crate::sqlast::SQLIdent;
    use crate::sqlparser::Parser;
    use std::error::Error;

    #[test]
    fn test_basic_visitor() -> Result<(), Box<dyn Error>> {
        struct Visitor<'a> {
            seen_idents: Vec<&'a SQLIdent>,
        }

        impl<'a> Visit<'a> for Visitor<'a> {
            fn visit_ident(&mut self, ident: &'a SQLIdent) {
                self.seen_idents.push(ident);
            }
        }

        let stmts = Parser::parse_sql(
            &GenericSqlDialect {},
            r#"
            WITH a01 AS (SELECT 1)
                SELECT *, a02.*, a03 AS a04
                FROM (SELECT * FROM a05) a06
                JOIN a07 ON a08.a09 = a10.a11
                WHERE a12
                GROUP BY a13
                HAVING a14
            UNION ALL
                SELECT a15 IS NULL
                    AND a16 IS NOT NULL
                    AND a17 IN (a18)
                    AND a19 IN (SELECT * FROM a20)
                    AND CAST(a21 AS int)
                    AND (a22)
                    AND NOT a23
                    AND a24(a25)
                    AND CASE a26 WHEN a27 THEN a28 ELSE a29 END
                    AND a30 BETWEEN a31 AND a32
                    AND a33 COLLATE a34 = a35
                    AND (SELECT a36)
                FROM a37(a38) AS a39 WITH (a40)
                LEFT JOIN a41 ON false
                RIGHT JOIN a42 ON false
                FULL JOIN a43 ON false
                JOIN a44 (a45) USING (a46)
            EXCEPT
                (SELECT a47(a48) OVER (PARTITION BY a49 ORDER BY a50 ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING))
            ORDER BY a51
            LIMIT 1;
            UPDATE b01 SET b02 = b03;
            INSERT INTO c01 (c02) VALUES (c03);
            INSERT INTO c04 SELECT * FROM c05;
            DELETE FROM d01 WHERE d02;
            CREATE TABLE e01 (e02 INT) WITH (e03 = 1);
            CREATE VIEW f01 (f02) WITH (f03 = 1) AS SELECT * FROM f04;
            ALTER TABLE g01 ADD CONSTRAINT g02 PRIMARY KEY (g03);
            ALTER TABLE h01 ADD CONSTRAINT h02 FOREIGN KEY (h03) REFERENCES h04 (h05);
            ALTER TABLE i01 ADD CONSTRAINT i02 UNIQUE (i03);
            DROP TABLE j01;
            DROP VIEW k01;
            COPY l01 (l02) FROM stdin;
            1
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
                "a49", "a50", "a51",
                "b01", "b02", "b03",
                "c01", "c02", "c03", "c04", "c05",
                "d01", "d02",
                "e01", "e02", "e03",
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

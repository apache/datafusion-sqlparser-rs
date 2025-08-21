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

use crate::ast::{
    ddl::AlterSchema, query::SelectItemQualifiedWildcardKind, AlterSchemaOperation, ColumnOptions,
    ExportData, TypedString,
};
use core::iter;

use crate::tokenizer::Span;

use super::{
    dcl::SecondaryRoles, value::ValueWithSpan, AccessExpr, AlterColumnOperation,
    AlterIndexOperation, AlterTableOperation, Array, Assignment, AssignmentTarget, AttachedToken,
    BeginEndStatements, CaseStatement, CloseCursor, ClusteredIndex, ColumnDef, ColumnOption,
    ColumnOptionDef, ConditionalStatementBlock, ConditionalStatements, ConflictTarget, ConnectBy,
    ConstraintCharacteristics, CopySource, CreateIndex, CreateTable, CreateTableOptions, Cte,
    Delete, DoUpdate, ExceptSelectItem, ExcludeSelectItem, Expr, ExprWithAlias, Fetch, FromTable,
    Function, FunctionArg, FunctionArgExpr, FunctionArgumentClause, FunctionArgumentList,
    FunctionArguments, GroupByExpr, HavingBound, IfStatement, IlikeSelectItem, IndexColumn, Insert,
    Interpolate, InterpolateExpr, Join, JoinConstraint, JoinOperator, JsonPath, JsonPathElem,
    LateralView, LimitClause, MatchRecognizePattern, Measure, NamedParenthesizedList,
    NamedWindowDefinition, ObjectName, ObjectNamePart, Offset, OnConflict, OnConflictAction,
    OnInsert, OpenStatement, OrderBy, OrderByExpr, OrderByKind, Partition, PivotValueSource,
    ProjectionSelect, Query, RaiseStatement, RaiseStatementValue, ReferentialAction,
    RenameSelectItem, ReplaceSelectElement, ReplaceSelectItem, Select, SelectInto, SelectItem,
    SetExpr, SqlOption, Statement, Subscript, SymbolDefinition, TableAlias, TableAliasColumnDef,
    TableConstraint, TableFactor, TableObject, TableOptionsClustered, TableWithJoins,
    UpdateTableFromKind, Use, Value, Values, ViewColumnDef, WhileStatement,
    WildcardAdditionalOptions, With, WithFill,
};

/// Given an iterator of spans, return the [Span::union] of all spans.
fn union_spans<I: Iterator<Item = Span>>(iter: I) -> Span {
    Span::union_iter(iter)
}

/// Trait for AST nodes that have a source location information.
///
/// # Notes:
///
/// Source [`Span`] are not yet complete. They may be missing:
///
/// 1. keywords or other tokens
/// 2. span information entirely, in which case they return [`Span::empty()`].
///
/// Note Some impl blocks (rendered below) are annotated with which nodes are
/// missing spans. See [this ticket] for additional information and status.
///
/// [this ticket]: https://github.com/apache/datafusion-sqlparser-rs/issues/1548
///
/// # Example
/// ```
/// # use sqlparser::parser::{Parser, ParserError};
/// # use sqlparser::ast::Spanned;
/// # use sqlparser::dialect::GenericDialect;
/// # use sqlparser::tokenizer::Location;
/// # fn main() -> Result<(), ParserError> {
/// let dialect = GenericDialect {};
/// let sql = r#"SELECT *
///   FROM table_1"#;
/// let statements = Parser::new(&dialect)
///   .try_with_sql(sql)?
///   .parse_statements()?;
/// // Get the span of the first statement (SELECT)
/// let span = statements[0].span();
/// // statement starts at line 1, column 1 (1 based, not 0 based)
/// assert_eq!(span.start, Location::new(1, 1));
/// // statement ends on line 2, column 15
/// assert_eq!(span.end, Location::new(2, 15));
/// # Ok(())
/// # }
/// ```
///
pub trait Spanned {
    /// Return the [`Span`] (the minimum and maximum [`Location`]) for this AST
    /// node, by recursively combining the spans of its children.
    ///
    /// [`Location`]: crate::tokenizer::Location
    fn span(&self) -> Span;
}

impl Spanned for Query {
    fn span(&self) -> Span {
        let Query {
            with,
            body,
            order_by,
            limit_clause,
            fetch,
            locks: _,          // todo
            for_clause: _,     // todo, mssql specific
            settings: _,       // todo, clickhouse specific
            format_clause: _,  // todo, clickhouse specific
            pipe_operators: _, // todo bigquery specific
        } = self;

        union_spans(
            with.iter()
                .map(|i| i.span())
                .chain(core::iter::once(body.span()))
                .chain(order_by.as_ref().map(|i| i.span()))
                .chain(limit_clause.as_ref().map(|i| i.span()))
                .chain(fetch.as_ref().map(|i| i.span())),
        )
    }
}

impl Spanned for LimitClause {
    fn span(&self) -> Span {
        match self {
            LimitClause::LimitOffset {
                limit,
                offset,
                limit_by,
            } => union_spans(
                limit
                    .iter()
                    .map(|i| i.span())
                    .chain(offset.as_ref().map(|i| i.span()))
                    .chain(limit_by.iter().map(|i| i.span())),
            ),
            LimitClause::OffsetCommaLimit { offset, limit } => offset.span().union(&limit.span()),
        }
    }
}

impl Spanned for Offset {
    fn span(&self) -> Span {
        let Offset {
            value,
            rows: _, // enum
        } = self;

        value.span()
    }
}

impl Spanned for Fetch {
    fn span(&self) -> Span {
        let Fetch {
            with_ties: _, // bool
            percent: _,   // bool
            quantity,
        } = self;

        quantity.as_ref().map_or(Span::empty(), |i| i.span())
    }
}

impl Spanned for With {
    fn span(&self) -> Span {
        let With {
            with_token,
            recursive: _, // bool
            cte_tables,
        } = self;

        union_spans(
            core::iter::once(with_token.0.span).chain(cte_tables.iter().map(|item| item.span())),
        )
    }
}

impl Spanned for Cte {
    fn span(&self) -> Span {
        let Cte {
            alias,
            query,
            from,
            materialized: _, // enum
            closing_paren_token,
        } = self;

        union_spans(
            core::iter::once(alias.span())
                .chain(core::iter::once(query.span()))
                .chain(from.iter().map(|item| item.span))
                .chain(core::iter::once(closing_paren_token.0.span)),
        )
    }
}

/// # partial span
///
/// [SetExpr::Table] is not implemented.
impl Spanned for SetExpr {
    fn span(&self) -> Span {
        match self {
            SetExpr::Select(select) => select.span(),
            SetExpr::Query(query) => query.span(),
            SetExpr::SetOperation {
                op: _,
                set_quantifier: _,
                left,
                right,
            } => left.span().union(&right.span()),
            SetExpr::Values(values) => values.span(),
            SetExpr::Insert(statement) => statement.span(),
            SetExpr::Table(_) => Span::empty(),
            SetExpr::Update(statement) => statement.span(),
            SetExpr::Delete(statement) => statement.span(),
        }
    }
}

impl Spanned for Values {
    fn span(&self) -> Span {
        let Values {
            explicit_row: _, // bool,
            rows,
        } = self;

        union_spans(
            rows.iter()
                .map(|row| union_spans(row.iter().map(|expr| expr.span()))),
        )
    }
}

/// # partial span
///
/// Missing spans:
/// - [Statement::CopyIntoSnowflake]
/// - [Statement::CreateSecret]
/// - [Statement::CreateRole]
/// - [Statement::AlterType]
/// - [Statement::AlterRole]
/// - [Statement::AttachDatabase]
/// - [Statement::AttachDuckDBDatabase]
/// - [Statement::DetachDuckDBDatabase]
/// - [Statement::Drop]
/// - [Statement::DropFunction]
/// - [Statement::DropProcedure]
/// - [Statement::DropSecret]
/// - [Statement::Declare]
/// - [Statement::CreateExtension]
/// - [Statement::Fetch]
/// - [Statement::Flush]
/// - [Statement::Discard]
/// - [Statement::Set]
/// - [Statement::ShowFunctions]
/// - [Statement::ShowVariable]
/// - [Statement::ShowStatus]
/// - [Statement::ShowVariables]
/// - [Statement::ShowCreate]
/// - [Statement::ShowColumns]
/// - [Statement::ShowTables]
/// - [Statement::ShowCollation]
/// - [Statement::StartTransaction]
/// - [Statement::Comment]
/// - [Statement::Commit]
/// - [Statement::Rollback]
/// - [Statement::CreateSchema]
/// - [Statement::CreateDatabase]
/// - [Statement::CreateFunction]
/// - [Statement::CreateTrigger]
/// - [Statement::DropTrigger]
/// - [Statement::CreateProcedure]
/// - [Statement::CreateMacro]
/// - [Statement::CreateStage]
/// - [Statement::Assert]
/// - [Statement::Grant]
/// - [Statement::Revoke]
/// - [Statement::Deallocate]
/// - [Statement::Execute]
/// - [Statement::Prepare]
/// - [Statement::Kill]
/// - [Statement::ExplainTable]
/// - [Statement::Explain]
/// - [Statement::Savepoint]
/// - [Statement::ReleaseSavepoint]
/// - [Statement::Merge]
/// - [Statement::Cache]
/// - [Statement::UNCache]
/// - [Statement::CreateSequence]
/// - [Statement::CreateType]
/// - [Statement::Pragma]
/// - [Statement::LockTables]
/// - [Statement::UnlockTables]
/// - [Statement::Unload]
/// - [Statement::OptimizeTable]
impl Spanned for Statement {
    fn span(&self) -> Span {
        match self {
            Statement::Analyze {
                table_name,
                partitions,
                for_columns: _,
                columns,
                cache_metadata: _,
                noscan: _,
                compute_statistics: _,
                has_table_keyword: _,
            } => union_spans(
                core::iter::once(table_name.span())
                    .chain(partitions.iter().flat_map(|i| i.iter().map(|k| k.span())))
                    .chain(columns.iter().map(|i| i.span)),
            ),
            Statement::Truncate {
                table_names,
                partitions,
                table: _,
                identity: _,
                cascade: _,
                on_cluster: _,
            } => union_spans(
                table_names
                    .iter()
                    .map(|i| i.name.span())
                    .chain(partitions.iter().flat_map(|i| i.iter().map(|k| k.span()))),
            ),
            Statement::Msck {
                table_name,
                repair: _,
                partition_action: _,
            } => table_name.span(),
            Statement::Query(query) => query.span(),
            Statement::Insert(insert) => insert.span(),
            Statement::Install { extension_name } => extension_name.span,
            Statement::Load { extension_name } => extension_name.span,
            Statement::Directory {
                overwrite: _,
                local: _,
                path: _,
                file_format: _,
                source,
            } => source.span(),
            Statement::Case(stmt) => stmt.span(),
            Statement::If(stmt) => stmt.span(),
            Statement::While(stmt) => stmt.span(),
            Statement::Raise(stmt) => stmt.span(),
            Statement::Call(function) => function.span(),
            Statement::Copy {
                source,
                to: _,
                target: _,
                options: _,
                legacy_options: _,
                values: _,
            } => source.span(),
            Statement::CopyIntoSnowflake {
                into: _,
                into_columns: _,
                from_obj: _,
                from_obj_alias: _,
                stage_params: _,
                from_transformations: _,
                files: _,
                pattern: _,
                file_format: _,
                copy_options: _,
                validation_mode: _,
                kind: _,
                from_query: _,
                partition: _,
            } => Span::empty(),
            Statement::Open(open) => open.span(),
            Statement::Close { cursor } => match cursor {
                CloseCursor::All => Span::empty(),
                CloseCursor::Specific { name } => name.span,
            },
            Statement::Update {
                table,
                assignments,
                from,
                selection,
                returning,
                or: _,
                limit: _,
            } => union_spans(
                core::iter::once(table.span())
                    .chain(assignments.iter().map(|i| i.span()))
                    .chain(from.iter().map(|i| i.span()))
                    .chain(selection.iter().map(|i| i.span()))
                    .chain(returning.iter().flat_map(|i| i.iter().map(|k| k.span()))),
            ),
            Statement::Delete(delete) => delete.span(),
            Statement::CreateView {
                or_alter: _,
                or_replace: _,
                materialized: _,
                name,
                columns,
                query,
                options,
                cluster_by,
                comment: _,
                with_no_schema_binding: _,
                if_not_exists: _,
                temporary: _,
                to,
                name_before_not_exists: _,
                params: _,
            } => union_spans(
                core::iter::once(name.span())
                    .chain(columns.iter().map(|i| i.span()))
                    .chain(core::iter::once(query.span()))
                    .chain(core::iter::once(options.span()))
                    .chain(cluster_by.iter().map(|i| i.span))
                    .chain(to.iter().map(|i| i.span())),
            ),
            Statement::CreateTable(create_table) => create_table.span(),
            Statement::CreateVirtualTable {
                name,
                if_not_exists: _,
                module_name,
                module_args,
            } => union_spans(
                core::iter::once(name.span())
                    .chain(core::iter::once(module_name.span))
                    .chain(module_args.iter().map(|i| i.span)),
            ),
            Statement::CreateIndex(create_index) => create_index.span(),
            Statement::CreateRole { .. } => Span::empty(),
            Statement::CreateSecret { .. } => Span::empty(),
            Statement::CreateServer { .. } => Span::empty(),
            Statement::CreateConnector { .. } => Span::empty(),
            Statement::AlterTable {
                name,
                if_exists: _,
                only: _,
                operations,
                location: _,
                on_cluster,
                iceberg: _,
                end_token,
            } => union_spans(
                core::iter::once(name.span())
                    .chain(operations.iter().map(|i| i.span()))
                    .chain(on_cluster.iter().map(|i| i.span))
                    .chain(core::iter::once(end_token.0.span)),
            ),
            Statement::AlterIndex { name, operation } => name.span().union(&operation.span()),
            Statement::AlterView {
                name,
                columns,
                query,
                with_options,
            } => union_spans(
                core::iter::once(name.span())
                    .chain(columns.iter().map(|i| i.span))
                    .chain(core::iter::once(query.span()))
                    .chain(with_options.iter().map(|i| i.span())),
            ),
            // These statements need to be implemented
            Statement::AlterType { .. } => Span::empty(),
            Statement::AlterRole { .. } => Span::empty(),
            Statement::AlterSession { .. } => Span::empty(),
            Statement::AttachDatabase { .. } => Span::empty(),
            Statement::AttachDuckDBDatabase { .. } => Span::empty(),
            Statement::DetachDuckDBDatabase { .. } => Span::empty(),
            Statement::Drop { .. } => Span::empty(),
            Statement::DropFunction { .. } => Span::empty(),
            Statement::DropDomain { .. } => Span::empty(),
            Statement::DropProcedure { .. } => Span::empty(),
            Statement::DropSecret { .. } => Span::empty(),
            Statement::Declare { .. } => Span::empty(),
            Statement::CreateExtension { .. } => Span::empty(),
            Statement::DropExtension { .. } => Span::empty(),
            Statement::Fetch { .. } => Span::empty(),
            Statement::Flush { .. } => Span::empty(),
            Statement::Discard { .. } => Span::empty(),
            Statement::Set(_) => Span::empty(),
            Statement::ShowFunctions { .. } => Span::empty(),
            Statement::ShowVariable { .. } => Span::empty(),
            Statement::ShowStatus { .. } => Span::empty(),
            Statement::ShowVariables { .. } => Span::empty(),
            Statement::ShowCreate { .. } => Span::empty(),
            Statement::ShowColumns { .. } => Span::empty(),
            Statement::ShowTables { .. } => Span::empty(),
            Statement::ShowCollation { .. } => Span::empty(),
            Statement::ShowCharset { .. } => Span::empty(),
            Statement::Use(u) => u.span(),
            Statement::StartTransaction { .. } => Span::empty(),
            Statement::Comment { .. } => Span::empty(),
            Statement::Commit { .. } => Span::empty(),
            Statement::Rollback { .. } => Span::empty(),
            Statement::CreateSchema { .. } => Span::empty(),
            Statement::CreateDatabase { .. } => Span::empty(),
            Statement::CreateFunction { .. } => Span::empty(),
            Statement::CreateDomain { .. } => Span::empty(),
            Statement::CreateTrigger { .. } => Span::empty(),
            Statement::DropTrigger { .. } => Span::empty(),
            Statement::CreateProcedure { .. } => Span::empty(),
            Statement::CreateMacro { .. } => Span::empty(),
            Statement::CreateStage { .. } => Span::empty(),
            Statement::Assert { .. } => Span::empty(),
            Statement::Grant { .. } => Span::empty(),
            Statement::Deny { .. } => Span::empty(),
            Statement::Revoke { .. } => Span::empty(),
            Statement::Deallocate { .. } => Span::empty(),
            Statement::Execute { .. } => Span::empty(),
            Statement::Prepare { .. } => Span::empty(),
            Statement::Kill { .. } => Span::empty(),
            Statement::ExplainTable { .. } => Span::empty(),
            Statement::Explain { .. } => Span::empty(),
            Statement::Savepoint { .. } => Span::empty(),
            Statement::ReleaseSavepoint { .. } => Span::empty(),
            Statement::Merge { .. } => Span::empty(),
            Statement::Cache { .. } => Span::empty(),
            Statement::UNCache { .. } => Span::empty(),
            Statement::CreateSequence { .. } => Span::empty(),
            Statement::CreateType { .. } => Span::empty(),
            Statement::Pragma { .. } => Span::empty(),
            Statement::LockTables { .. } => Span::empty(),
            Statement::UnlockTables => Span::empty(),
            Statement::Unload { .. } => Span::empty(),
            Statement::OptimizeTable { .. } => Span::empty(),
            Statement::CreatePolicy { .. } => Span::empty(),
            Statement::AlterPolicy { .. } => Span::empty(),
            Statement::AlterConnector { .. } => Span::empty(),
            Statement::DropPolicy { .. } => Span::empty(),
            Statement::DropConnector { .. } => Span::empty(),
            Statement::ShowDatabases { .. } => Span::empty(),
            Statement::ShowSchemas { .. } => Span::empty(),
            Statement::ShowObjects { .. } => Span::empty(),
            Statement::ShowViews { .. } => Span::empty(),
            Statement::LISTEN { .. } => Span::empty(),
            Statement::NOTIFY { .. } => Span::empty(),
            Statement::LoadData { .. } => Span::empty(),
            Statement::UNLISTEN { .. } => Span::empty(),
            Statement::RenameTable { .. } => Span::empty(),
            Statement::RaisError { .. } => Span::empty(),
            Statement::Print { .. } => Span::empty(),
            Statement::Return { .. } => Span::empty(),
            Statement::List(..) | Statement::Remove(..) => Span::empty(),
            Statement::ExportData(ExportData {
                options,
                query,
                connection,
            }) => union_spans(
                options
                    .iter()
                    .map(|i| i.span())
                    .chain(core::iter::once(query.span()))
                    .chain(connection.iter().map(|i| i.span())),
            ),
            Statement::CreateUser(..) => Span::empty(),
            Statement::AlterSchema(s) => s.span(),
            Statement::Vacuum(..) => Span::empty(),
        }
    }
}

impl Spanned for Use {
    fn span(&self) -> Span {
        match self {
            Use::Catalog(object_name) => object_name.span(),
            Use::Schema(object_name) => object_name.span(),
            Use::Database(object_name) => object_name.span(),
            Use::Warehouse(object_name) => object_name.span(),
            Use::Role(object_name) => object_name.span(),
            Use::SecondaryRoles(secondary_roles) => {
                if let SecondaryRoles::List(roles) = secondary_roles {
                    return union_spans(roles.iter().map(|i| i.span));
                }
                Span::empty()
            }
            Use::Object(object_name) => object_name.span(),
            Use::Default => Span::empty(),
        }
    }
}

impl Spanned for CreateTable {
    fn span(&self) -> Span {
        let CreateTable {
            or_replace: _,    // bool
            temporary: _,     // bool
            external: _,      // bool
            global: _,        // bool
            dynamic: _,       // bool
            if_not_exists: _, // bool
            transient: _,     // bool
            volatile: _,      // bool
            iceberg: _,       // bool, Snowflake specific
            name,
            columns,
            constraints,
            hive_distribution: _, // hive specific
            hive_formats: _,      // hive specific
            file_format: _,       // enum
            location: _,          // string, no span
            query,
            without_rowid: _, // bool
            like: _,
            clone,
            comment: _, // todo, no span
            on_commit: _,
            on_cluster: _,                      // todo, clickhouse specific
            primary_key: _,                     // todo, clickhouse specific
            order_by: _,                        // todo, clickhouse specific
            partition_by: _,                    // todo, BigQuery specific
            cluster_by: _,                      // todo, BigQuery specific
            clustered_by: _,                    // todo, Hive specific
            inherits: _,                        // todo, PostgreSQL specific
            strict: _,                          // bool
            copy_grants: _,                     // bool
            enable_schema_evolution: _,         // bool
            change_tracking: _,                 // bool
            data_retention_time_in_days: _,     // u64, no span
            max_data_extension_time_in_days: _, // u64, no span
            default_ddl_collation: _,           // string, no span
            with_aggregation_policy: _,         // todo, Snowflake specific
            with_row_access_policy: _,          // todo, Snowflake specific
            with_tags: _,                       // todo, Snowflake specific
            external_volume: _,                 // todo, Snowflake specific
            base_location: _,                   // todo, Snowflake specific
            catalog: _,                         // todo, Snowflake specific
            catalog_sync: _,                    // todo, Snowflake specific
            storage_serialization_policy: _,
            table_options,
            target_lag: _,
            warehouse: _,
            version: _,
            refresh_mode: _,
            initialize: _,
            require_user: _,
        } = self;

        union_spans(
            core::iter::once(name.span())
                .chain(core::iter::once(table_options.span()))
                .chain(columns.iter().map(|i| i.span()))
                .chain(constraints.iter().map(|i| i.span()))
                .chain(query.iter().map(|i| i.span()))
                .chain(clone.iter().map(|i| i.span())),
        )
    }
}

impl Spanned for ColumnDef {
    fn span(&self) -> Span {
        let ColumnDef {
            name,
            data_type: _, // enum
            options,
        } = self;

        union_spans(core::iter::once(name.span).chain(options.iter().map(|i| i.span())))
    }
}

impl Spanned for ColumnOptionDef {
    fn span(&self) -> Span {
        let ColumnOptionDef { name, option } = self;

        option.span().union_opt(&name.as_ref().map(|i| i.span))
    }
}

impl Spanned for TableConstraint {
    fn span(&self) -> Span {
        match self {
            TableConstraint::Unique {
                name,
                index_name,
                index_type_display: _,
                index_type: _,
                columns,
                index_options: _,
                characteristics,
                nulls_distinct: _,
            } => union_spans(
                name.iter()
                    .map(|i| i.span)
                    .chain(index_name.iter().map(|i| i.span))
                    .chain(columns.iter().map(|i| i.span()))
                    .chain(characteristics.iter().map(|i| i.span())),
            ),
            TableConstraint::PrimaryKey {
                name,
                index_name,
                index_type: _,
                columns,
                index_options: _,
                characteristics,
            } => union_spans(
                name.iter()
                    .map(|i| i.span)
                    .chain(index_name.iter().map(|i| i.span))
                    .chain(columns.iter().map(|i| i.span()))
                    .chain(characteristics.iter().map(|i| i.span())),
            ),
            TableConstraint::ForeignKey {
                name,
                columns,
                index_name,
                foreign_table,
                referred_columns,
                on_delete,
                on_update,
                characteristics,
            } => union_spans(
                name.iter()
                    .map(|i| i.span)
                    .chain(index_name.iter().map(|i| i.span))
                    .chain(columns.iter().map(|i| i.span))
                    .chain(core::iter::once(foreign_table.span()))
                    .chain(referred_columns.iter().map(|i| i.span))
                    .chain(on_delete.iter().map(|i| i.span()))
                    .chain(on_update.iter().map(|i| i.span()))
                    .chain(characteristics.iter().map(|i| i.span())),
            ),
            TableConstraint::Check {
                name,
                expr,
                enforced: _,
            } => expr.span().union_opt(&name.as_ref().map(|i| i.span)),
            TableConstraint::Index {
                display_as_key: _,
                name,
                index_type: _,
                columns,
                index_options: _,
            } => union_spans(
                name.iter()
                    .map(|i| i.span)
                    .chain(columns.iter().map(|i| i.span())),
            ),
            TableConstraint::FulltextOrSpatial {
                fulltext: _,
                index_type_display: _,
                opt_index_name,
                columns,
            } => union_spans(
                opt_index_name
                    .iter()
                    .map(|i| i.span)
                    .chain(columns.iter().map(|i| i.span())),
            ),
        }
    }
}

impl Spanned for CreateIndex {
    fn span(&self) -> Span {
        let CreateIndex {
            name,
            table_name,
            using: _,
            columns,
            unique: _,        // bool
            concurrently: _,  // bool
            if_not_exists: _, // bool
            include,
            nulls_distinct: _, // bool
            with,
            predicate,
            index_options: _,
            alter_options,
        } = self;

        union_spans(
            name.iter()
                .map(|i| i.span())
                .chain(core::iter::once(table_name.span()))
                .chain(columns.iter().map(|i| i.column.span()))
                .chain(include.iter().map(|i| i.span))
                .chain(with.iter().map(|i| i.span()))
                .chain(predicate.iter().map(|i| i.span()))
                .chain(alter_options.iter().map(|i| i.span())),
        )
    }
}

impl Spanned for IndexColumn {
    fn span(&self) -> Span {
        self.column.span()
    }
}

impl Spanned for CaseStatement {
    fn span(&self) -> Span {
        let CaseStatement {
            case_token: AttachedToken(start),
            match_expr: _,
            when_blocks: _,
            else_block: _,
            end_case_token: AttachedToken(end),
        } = self;

        union_spans([start.span, end.span].into_iter())
    }
}

impl Spanned for IfStatement {
    fn span(&self) -> Span {
        let IfStatement {
            if_block,
            elseif_blocks,
            else_block,
            end_token,
        } = self;

        union_spans(
            iter::once(if_block.span())
                .chain(elseif_blocks.iter().map(|b| b.span()))
                .chain(else_block.as_ref().map(|b| b.span()))
                .chain(end_token.as_ref().map(|AttachedToken(t)| t.span)),
        )
    }
}

impl Spanned for WhileStatement {
    fn span(&self) -> Span {
        let WhileStatement { while_block } = self;

        while_block.span()
    }
}

impl Spanned for ConditionalStatements {
    fn span(&self) -> Span {
        match self {
            ConditionalStatements::Sequence { statements } => {
                union_spans(statements.iter().map(|s| s.span()))
            }
            ConditionalStatements::BeginEnd(bes) => bes.span(),
        }
    }
}

impl Spanned for ConditionalStatementBlock {
    fn span(&self) -> Span {
        let ConditionalStatementBlock {
            start_token: AttachedToken(start_token),
            condition,
            then_token,
            conditional_statements,
        } = self;

        union_spans(
            iter::once(start_token.span)
                .chain(condition.as_ref().map(|c| c.span()))
                .chain(then_token.as_ref().map(|AttachedToken(t)| t.span))
                .chain(iter::once(conditional_statements.span())),
        )
    }
}

impl Spanned for RaiseStatement {
    fn span(&self) -> Span {
        let RaiseStatement { value } = self;

        union_spans(value.iter().map(|value| value.span()))
    }
}

impl Spanned for RaiseStatementValue {
    fn span(&self) -> Span {
        match self {
            RaiseStatementValue::UsingMessage(expr) => expr.span(),
            RaiseStatementValue::Expr(expr) => expr.span(),
        }
    }
}

/// # partial span
///
/// Missing spans:
/// - [ColumnOption::Null]
/// - [ColumnOption::NotNull]
/// - [ColumnOption::Comment]
/// - [ColumnOption::Unique]¨
/// - [ColumnOption::DialectSpecific]
/// - [ColumnOption::Generated]
impl Spanned for ColumnOption {
    fn span(&self) -> Span {
        match self {
            ColumnOption::Null => Span::empty(),
            ColumnOption::NotNull => Span::empty(),
            ColumnOption::Default(expr) => expr.span(),
            ColumnOption::Materialized(expr) => expr.span(),
            ColumnOption::Ephemeral(expr) => expr.as_ref().map_or(Span::empty(), |e| e.span()),
            ColumnOption::Alias(expr) => expr.span(),
            ColumnOption::Unique { .. } => Span::empty(),
            ColumnOption::ForeignKey {
                foreign_table,
                referred_columns,
                on_delete,
                on_update,
                characteristics,
            } => union_spans(
                core::iter::once(foreign_table.span())
                    .chain(referred_columns.iter().map(|i| i.span))
                    .chain(on_delete.iter().map(|i| i.span()))
                    .chain(on_update.iter().map(|i| i.span()))
                    .chain(characteristics.iter().map(|i| i.span())),
            ),
            ColumnOption::Check(expr) => expr.span(),
            ColumnOption::DialectSpecific(_) => Span::empty(),
            ColumnOption::CharacterSet(object_name) => object_name.span(),
            ColumnOption::Collation(object_name) => object_name.span(),
            ColumnOption::Comment(_) => Span::empty(),
            ColumnOption::OnUpdate(expr) => expr.span(),
            ColumnOption::Generated { .. } => Span::empty(),
            ColumnOption::Options(vec) => union_spans(vec.iter().map(|i| i.span())),
            ColumnOption::Identity(..) => Span::empty(),
            ColumnOption::OnConflict(..) => Span::empty(),
            ColumnOption::Policy(..) => Span::empty(),
            ColumnOption::Tags(..) => Span::empty(),
            ColumnOption::Srid(..) => Span::empty(),
        }
    }
}

/// # missing span
impl Spanned for ReferentialAction {
    fn span(&self) -> Span {
        Span::empty()
    }
}

/// # missing span
impl Spanned for ConstraintCharacteristics {
    fn span(&self) -> Span {
        let ConstraintCharacteristics {
            deferrable: _, // bool
            initially: _,  // enum
            enforced: _,   // bool
        } = self;

        Span::empty()
    }
}

/// # partial span
///
/// Missing spans:
/// - [AlterColumnOperation::SetNotNull]
/// - [AlterColumnOperation::DropNotNull]
/// - [AlterColumnOperation::DropDefault]
/// - [AlterColumnOperation::AddGenerated]
impl Spanned for AlterColumnOperation {
    fn span(&self) -> Span {
        match self {
            AlterColumnOperation::SetNotNull => Span::empty(),
            AlterColumnOperation::DropNotNull => Span::empty(),
            AlterColumnOperation::SetDefault { value } => value.span(),
            AlterColumnOperation::DropDefault => Span::empty(),
            AlterColumnOperation::SetDataType {
                data_type: _,
                using,
                had_set: _,
            } => using.as_ref().map_or(Span::empty(), |u| u.span()),
            AlterColumnOperation::AddGenerated { .. } => Span::empty(),
        }
    }
}

impl Spanned for CopySource {
    fn span(&self) -> Span {
        match self {
            CopySource::Table {
                table_name,
                columns,
            } => union_spans(
                core::iter::once(table_name.span()).chain(columns.iter().map(|i| i.span)),
            ),
            CopySource::Query(query) => query.span(),
        }
    }
}

impl Spanned for Delete {
    fn span(&self) -> Span {
        let Delete {
            tables,
            from,
            using,
            selection,
            returning,
            order_by,
            limit,
        } = self;

        union_spans(
            tables
                .iter()
                .map(|i| i.span())
                .chain(core::iter::once(from.span()))
                .chain(
                    using
                        .iter()
                        .map(|u| union_spans(u.iter().map(|i| i.span()))),
                )
                .chain(selection.iter().map(|i| i.span()))
                .chain(returning.iter().flat_map(|i| i.iter().map(|k| k.span())))
                .chain(order_by.iter().map(|i| i.span()))
                .chain(limit.iter().map(|i| i.span())),
        )
    }
}

impl Spanned for FromTable {
    fn span(&self) -> Span {
        match self {
            FromTable::WithFromKeyword(vec) => union_spans(vec.iter().map(|i| i.span())),
            FromTable::WithoutKeyword(vec) => union_spans(vec.iter().map(|i| i.span())),
        }
    }
}

impl Spanned for ViewColumnDef {
    fn span(&self) -> Span {
        let ViewColumnDef {
            name,
            data_type: _, // todo, DataType
            options,
        } = self;

        name.span.union_opt(&options.as_ref().map(|o| o.span()))
    }
}

impl Spanned for ColumnOptions {
    fn span(&self) -> Span {
        union_spans(self.as_slice().iter().map(|i| i.span()))
    }
}

impl Spanned for SqlOption {
    fn span(&self) -> Span {
        match self {
            SqlOption::Clustered(table_options_clustered) => table_options_clustered.span(),
            SqlOption::Ident(ident) => ident.span,
            SqlOption::KeyValue { key, value } => key.span.union(&value.span()),
            SqlOption::Partition {
                column_name,
                range_direction: _,
                for_values,
            } => union_spans(
                core::iter::once(column_name.span).chain(for_values.iter().map(|i| i.span())),
            ),
            SqlOption::TableSpace(_) => Span::empty(),
            SqlOption::Comment(_) => Span::empty(),
            SqlOption::NamedParenthesizedList(NamedParenthesizedList {
                key: name,
                name: value,
                values,
            }) => union_spans(core::iter::once(name.span).chain(values.iter().map(|i| i.span)))
                .union_opt(&value.as_ref().map(|i| i.span)),
        }
    }
}

/// # partial span
///
/// Missing spans:
/// - [TableOptionsClustered::ColumnstoreIndex]
impl Spanned for TableOptionsClustered {
    fn span(&self) -> Span {
        match self {
            TableOptionsClustered::ColumnstoreIndex => Span::empty(),
            TableOptionsClustered::ColumnstoreIndexOrder(vec) => {
                union_spans(vec.iter().map(|i| i.span))
            }
            TableOptionsClustered::Index(vec) => union_spans(vec.iter().map(|i| i.span())),
        }
    }
}

impl Spanned for ClusteredIndex {
    fn span(&self) -> Span {
        let ClusteredIndex {
            name,
            asc: _, // bool
        } = self;

        name.span
    }
}

impl Spanned for CreateTableOptions {
    fn span(&self) -> Span {
        match self {
            CreateTableOptions::None => Span::empty(),
            CreateTableOptions::With(vec) => union_spans(vec.iter().map(|i| i.span())),
            CreateTableOptions::Options(vec) => {
                union_spans(vec.as_slice().iter().map(|i| i.span()))
            }
            CreateTableOptions::Plain(vec) => union_spans(vec.iter().map(|i| i.span())),
            CreateTableOptions::TableProperties(vec) => union_spans(vec.iter().map(|i| i.span())),
        }
    }
}

/// # partial span
///
/// Missing spans:
/// - [AlterTableOperation::OwnerTo]
impl Spanned for AlterTableOperation {
    fn span(&self) -> Span {
        match self {
            AlterTableOperation::AddConstraint {
                constraint,
                not_valid: _,
            } => constraint.span(),
            AlterTableOperation::AddColumn {
                column_keyword: _,
                if_not_exists: _,
                column_def,
                column_position: _,
            } => column_def.span(),
            AlterTableOperation::AddProjection {
                if_not_exists: _,
                name,
                select,
            } => name.span.union(&select.span()),
            AlterTableOperation::DropProjection { if_exists: _, name } => name.span,
            AlterTableOperation::MaterializeProjection {
                if_exists: _,
                name,
                partition,
            } => name.span.union_opt(&partition.as_ref().map(|i| i.span)),
            AlterTableOperation::ClearProjection {
                if_exists: _,
                name,
                partition,
            } => name.span.union_opt(&partition.as_ref().map(|i| i.span)),
            AlterTableOperation::DisableRowLevelSecurity => Span::empty(),
            AlterTableOperation::DisableRule { name } => name.span,
            AlterTableOperation::DisableTrigger { name } => name.span,
            AlterTableOperation::DropConstraint {
                if_exists: _,
                name,
                drop_behavior: _,
            } => name.span,
            AlterTableOperation::DropColumn {
                has_column_keyword: _,
                column_names,
                if_exists: _,
                drop_behavior: _,
            } => union_spans(column_names.iter().map(|i| i.span)),
            AlterTableOperation::AttachPartition { partition } => partition.span(),
            AlterTableOperation::DetachPartition { partition } => partition.span(),
            AlterTableOperation::FreezePartition {
                partition,
                with_name,
            } => partition
                .span()
                .union_opt(&with_name.as_ref().map(|n| n.span)),
            AlterTableOperation::UnfreezePartition {
                partition,
                with_name,
            } => partition
                .span()
                .union_opt(&with_name.as_ref().map(|n| n.span)),
            AlterTableOperation::DropPrimaryKey { .. } => Span::empty(),
            AlterTableOperation::DropForeignKey { name, .. } => name.span,
            AlterTableOperation::DropIndex { name } => name.span,
            AlterTableOperation::EnableAlwaysRule { name } => name.span,
            AlterTableOperation::EnableAlwaysTrigger { name } => name.span,
            AlterTableOperation::EnableReplicaRule { name } => name.span,
            AlterTableOperation::EnableReplicaTrigger { name } => name.span,
            AlterTableOperation::EnableRowLevelSecurity => Span::empty(),
            AlterTableOperation::EnableRule { name } => name.span,
            AlterTableOperation::EnableTrigger { name } => name.span,
            AlterTableOperation::RenamePartitions {
                old_partitions,
                new_partitions,
            } => union_spans(
                old_partitions
                    .iter()
                    .map(|i| i.span())
                    .chain(new_partitions.iter().map(|i| i.span())),
            ),
            AlterTableOperation::AddPartitions {
                if_not_exists: _,
                new_partitions,
            } => union_spans(new_partitions.iter().map(|i| i.span())),
            AlterTableOperation::DropPartitions {
                partitions,
                if_exists: _,
            } => union_spans(partitions.iter().map(|i| i.span())),
            AlterTableOperation::RenameColumn {
                old_column_name,
                new_column_name,
            } => old_column_name.span.union(&new_column_name.span),
            AlterTableOperation::RenameTable { table_name } => table_name.span(),
            AlterTableOperation::ChangeColumn {
                old_name,
                new_name,
                data_type: _,
                options,
                column_position: _,
            } => union_spans(
                core::iter::once(old_name.span)
                    .chain(core::iter::once(new_name.span))
                    .chain(options.iter().map(|i| i.span())),
            ),
            AlterTableOperation::ModifyColumn {
                col_name,
                data_type: _,
                options,
                column_position: _,
            } => {
                union_spans(core::iter::once(col_name.span).chain(options.iter().map(|i| i.span())))
            }
            AlterTableOperation::RenameConstraint { old_name, new_name } => {
                old_name.span.union(&new_name.span)
            }
            AlterTableOperation::AlterColumn { column_name, op } => {
                column_name.span.union(&op.span())
            }
            AlterTableOperation::SwapWith { table_name } => table_name.span(),
            AlterTableOperation::SetTblProperties { table_properties } => {
                union_spans(table_properties.iter().map(|i| i.span()))
            }
            AlterTableOperation::OwnerTo { .. } => Span::empty(),
            AlterTableOperation::ClusterBy { exprs } => union_spans(exprs.iter().map(|e| e.span())),
            AlterTableOperation::DropClusteringKey => Span::empty(),
            AlterTableOperation::SuspendRecluster => Span::empty(),
            AlterTableOperation::ResumeRecluster => Span::empty(),
            AlterTableOperation::Algorithm { .. } => Span::empty(),
            AlterTableOperation::AutoIncrement { value, .. } => value.span(),
            AlterTableOperation::Lock { .. } => Span::empty(),
            AlterTableOperation::ReplicaIdentity { .. } => Span::empty(),
            AlterTableOperation::ValidateConstraint { name } => name.span,
            AlterTableOperation::SetOptionsParens { options } => {
                union_spans(options.iter().map(|i| i.span()))
            }
        }
    }
}

impl Spanned for Partition {
    fn span(&self) -> Span {
        match self {
            Partition::Identifier(ident) => ident.span,
            Partition::Expr(expr) => expr.span(),
            Partition::Part(expr) => expr.span(),
            Partition::Partitions(vec) => union_spans(vec.iter().map(|i| i.span())),
        }
    }
}

impl Spanned for ProjectionSelect {
    fn span(&self) -> Span {
        let ProjectionSelect {
            projection,
            order_by,
            group_by,
        } = self;

        union_spans(
            projection
                .iter()
                .map(|i| i.span())
                .chain(order_by.iter().map(|i| i.span()))
                .chain(group_by.iter().map(|i| i.span())),
        )
    }
}

/// # partial span
///
/// Missing spans:
/// - [OrderByKind::All]
impl Spanned for OrderBy {
    fn span(&self) -> Span {
        match &self.kind {
            OrderByKind::All(_) => Span::empty(),
            OrderByKind::Expressions(exprs) => union_spans(
                exprs
                    .iter()
                    .map(|i| i.span())
                    .chain(self.interpolate.iter().map(|i| i.span())),
            ),
        }
    }
}

/// # partial span
///
/// Missing spans:
/// - [GroupByExpr::All]
impl Spanned for GroupByExpr {
    fn span(&self) -> Span {
        match self {
            GroupByExpr::All(_) => Span::empty(),
            GroupByExpr::Expressions(exprs, _modifiers) => {
                union_spans(exprs.iter().map(|i| i.span()))
            }
        }
    }
}

impl Spanned for Interpolate {
    fn span(&self) -> Span {
        let Interpolate { exprs } = self;

        union_spans(exprs.iter().flat_map(|i| i.iter().map(|e| e.span())))
    }
}

impl Spanned for InterpolateExpr {
    fn span(&self) -> Span {
        let InterpolateExpr { column, expr } = self;

        column.span.union_opt(&expr.as_ref().map(|e| e.span()))
    }
}

impl Spanned for AlterIndexOperation {
    fn span(&self) -> Span {
        match self {
            AlterIndexOperation::RenameIndex { index_name } => index_name.span(),
        }
    }
}

/// # partial span
///
/// Missing spans:ever
/// - [Insert::insert_alias]
impl Spanned for Insert {
    fn span(&self) -> Span {
        let Insert {
            or: _,     // enum, sqlite specific
            ignore: _, // bool
            into: _,   // bool
            table,
            table_alias,
            columns,
            overwrite: _, // bool
            source,
            partitioned,
            after_columns,
            has_table_keyword: _, // bool
            on,
            returning,
            replace_into: _, // bool
            priority: _,     // todo, mysql specific
            insert_alias: _, // todo, mysql specific
            assignments,
            settings: _,      // todo, clickhouse specific
            format_clause: _, // todo, clickhouse specific
        } = self;

        union_spans(
            core::iter::once(table.span())
                .chain(table_alias.as_ref().map(|i| i.span))
                .chain(columns.iter().map(|i| i.span))
                .chain(source.as_ref().map(|q| q.span()))
                .chain(assignments.iter().map(|i| i.span()))
                .chain(partitioned.iter().flat_map(|i| i.iter().map(|k| k.span())))
                .chain(after_columns.iter().map(|i| i.span))
                .chain(on.as_ref().map(|i| i.span()))
                .chain(returning.iter().flat_map(|i| i.iter().map(|k| k.span()))),
        )
    }
}

impl Spanned for OnInsert {
    fn span(&self) -> Span {
        match self {
            OnInsert::DuplicateKeyUpdate(vec) => union_spans(vec.iter().map(|i| i.span())),
            OnInsert::OnConflict(on_conflict) => on_conflict.span(),
        }
    }
}

impl Spanned for OnConflict {
    fn span(&self) -> Span {
        let OnConflict {
            conflict_target,
            action,
        } = self;

        action
            .span()
            .union_opt(&conflict_target.as_ref().map(|i| i.span()))
    }
}

impl Spanned for ConflictTarget {
    fn span(&self) -> Span {
        match self {
            ConflictTarget::Columns(vec) => union_spans(vec.iter().map(|i| i.span)),
            ConflictTarget::OnConstraint(object_name) => object_name.span(),
        }
    }
}

/// # partial span
///
/// Missing spans:
/// - [OnConflictAction::DoNothing]
impl Spanned for OnConflictAction {
    fn span(&self) -> Span {
        match self {
            OnConflictAction::DoNothing => Span::empty(),
            OnConflictAction::DoUpdate(do_update) => do_update.span(),
        }
    }
}

impl Spanned for DoUpdate {
    fn span(&self) -> Span {
        let DoUpdate {
            assignments,
            selection,
        } = self;

        union_spans(
            assignments
                .iter()
                .map(|i| i.span())
                .chain(selection.iter().map(|i| i.span())),
        )
    }
}

impl Spanned for Assignment {
    fn span(&self) -> Span {
        let Assignment { target, value } = self;

        target.span().union(&value.span())
    }
}

impl Spanned for AssignmentTarget {
    fn span(&self) -> Span {
        match self {
            AssignmentTarget::ColumnName(object_name) => object_name.span(),
            AssignmentTarget::Tuple(vec) => union_spans(vec.iter().map(|i| i.span())),
        }
    }
}

/// # partial span
///
/// Most expressions are missing keywords in their spans.
/// f.e. `IS NULL <expr>` reports as `<expr>::span`.
///
/// Missing spans:
/// - [Expr::MatchAgainst] # MySQL specific
/// - [Expr::RLike] # MySQL specific
/// - [Expr::Struct] # BigQuery specific
/// - [Expr::Named] # BigQuery specific
/// - [Expr::Dictionary] # DuckDB specific
/// - [Expr::Map] # DuckDB specific
/// - [Expr::Lambda]
impl Spanned for Expr {
    fn span(&self) -> Span {
        match self {
            Expr::Identifier(ident) => ident.span,
            Expr::CompoundIdentifier(vec) => union_spans(vec.iter().map(|i| i.span)),
            Expr::CompoundFieldAccess { root, access_chain } => {
                union_spans(iter::once(root.span()).chain(access_chain.iter().map(|i| i.span())))
            }
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
                negated: _,
            } => union_spans(
                core::iter::once(expr.span()).chain(list.iter().map(|item| item.span())),
            ),
            Expr::InSubquery {
                expr,
                subquery,
                negated: _,
            } => expr.span().union(&subquery.span()),
            Expr::InUnnest {
                expr,
                array_expr,
                negated: _,
            } => expr.span().union(&array_expr.span()),
            Expr::Between {
                expr,
                negated: _,
                low,
                high,
            } => expr.span().union(&low.span()).union(&high.span()),

            Expr::BinaryOp { left, op: _, right } => left.span().union(&right.span()),
            Expr::Like {
                negated: _,
                expr,
                pattern,
                escape_char: _,
                any: _,
            } => expr.span().union(&pattern.span()),
            Expr::ILike {
                negated: _,
                expr,
                pattern,
                escape_char: _,
                any: _,
            } => expr.span().union(&pattern.span()),
            Expr::RLike { .. } => Span::empty(),
            Expr::IsNormalized {
                expr,
                form: _,
                negated: _,
            } => expr.span(),
            Expr::SimilarTo {
                negated: _,
                expr,
                pattern,
                escape_char: _,
            } => expr.span().union(&pattern.span()),
            Expr::Ceil { expr, field: _ } => expr.span(),
            Expr::Floor { expr, field: _ } => expr.span(),
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
                .union(&union_spans(collation.0.iter().map(|i| i.span()))),
            Expr::Nested(expr) => expr.span(),
            Expr::Value(value) => value.span(),
            Expr::TypedString(TypedString { value, .. }) => value.span(),
            Expr::Function(function) => function.span(),
            Expr::GroupingSets(vec) => {
                union_spans(vec.iter().flat_map(|i| i.iter().map(|k| k.span())))
            }
            Expr::Cube(vec) => union_spans(vec.iter().flat_map(|i| i.iter().map(|k| k.span()))),
            Expr::Rollup(vec) => union_spans(vec.iter().flat_map(|i| i.iter().map(|k| k.span()))),
            Expr::Tuple(vec) => union_spans(vec.iter().map(|i| i.span())),
            Expr::Array(array) => array.span(),
            Expr::MatchAgainst { .. } => Span::empty(),
            Expr::JsonAccess { value, path } => value.span().union(&path.span()),
            Expr::AnyOp {
                left,
                compare_op: _,
                right,
                is_some: _,
            } => left.span().union(&right.span()),
            Expr::AllOp {
                left,
                compare_op: _,
                right,
            } => left.span().union(&right.span()),
            Expr::UnaryOp { op: _, expr } => expr.span(),
            Expr::Convert {
                expr,
                data_type: _,
                charset,
                target_before_value: _,
                styles,
                is_try: _,
            } => union_spans(
                core::iter::once(expr.span())
                    .chain(charset.as_ref().map(|i| i.span()))
                    .chain(styles.iter().map(|i| i.span())),
            ),
            Expr::Cast {
                kind: _,
                expr,
                data_type: _,
                format: _,
            } => expr.span(),
            Expr::AtTimeZone {
                timestamp,
                time_zone,
            } => timestamp.span().union(&time_zone.span()),
            Expr::Extract {
                field: _,
                syntax: _,
                expr,
            } => expr.span(),
            Expr::Substring {
                expr,
                substring_from,
                substring_for,
                special: _,
                shorthand: _,
            } => union_spans(
                core::iter::once(expr.span())
                    .chain(substring_from.as_ref().map(|i| i.span()))
                    .chain(substring_for.as_ref().map(|i| i.span())),
            ),
            Expr::Trim {
                expr,
                trim_where: _,
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
            Expr::Prefixed { value, .. } => value.span(),
            Expr::Case {
                case_token,
                end_token,
                operand,
                conditions,
                else_result,
            } => union_spans(
                iter::once(case_token.0.span)
                    .chain(
                        operand
                            .as_ref()
                            .map(|i| i.span())
                            .into_iter()
                            .chain(conditions.iter().flat_map(|case_when| {
                                [case_when.condition.span(), case_when.result.span()]
                            }))
                            .chain(else_result.as_ref().map(|i| i.span())),
                    )
                    .chain(iter::once(end_token.0.span)),
            ),
            Expr::Exists { subquery, .. } => subquery.span(),
            Expr::Subquery(query) => query.span(),
            Expr::Struct { .. } => Span::empty(),
            Expr::Named { .. } => Span::empty(),
            Expr::Dictionary(_) => Span::empty(),
            Expr::Map(_) => Span::empty(),
            Expr::Interval(interval) => interval.value.span(),
            Expr::Wildcard(token) => token.0.span,
            Expr::QualifiedWildcard(object_name, token) => union_spans(
                object_name
                    .0
                    .iter()
                    .map(|i| i.span())
                    .chain(iter::once(token.0.span)),
            ),
            Expr::OuterJoin(expr) => expr.span(),
            Expr::Prior(expr) => expr.span(),
            Expr::Lambda(_) => Span::empty(),
            Expr::MemberOf(member_of) => member_of.value.span().union(&member_of.array.span()),
        }
    }
}

impl Spanned for Subscript {
    fn span(&self) -> Span {
        match self {
            Subscript::Index { index } => index.span(),
            Subscript::Slice {
                lower_bound,
                upper_bound,
                stride,
            } => union_spans(
                [
                    lower_bound.as_ref().map(|i| i.span()),
                    upper_bound.as_ref().map(|i| i.span()),
                    stride.as_ref().map(|i| i.span()),
                ]
                .into_iter()
                .flatten(),
            ),
        }
    }
}

impl Spanned for AccessExpr {
    fn span(&self) -> Span {
        match self {
            AccessExpr::Dot(ident) => ident.span(),
            AccessExpr::Subscript(subscript) => subscript.span(),
        }
    }
}

impl Spanned for ObjectName {
    fn span(&self) -> Span {
        let ObjectName(segments) = self;

        union_spans(segments.iter().map(|i| i.span()))
    }
}

impl Spanned for ObjectNamePart {
    fn span(&self) -> Span {
        match self {
            ObjectNamePart::Identifier(ident) => ident.span,
            ObjectNamePart::Function(func) => func
                .name
                .span
                .union(&union_spans(func.args.iter().map(|i| i.span()))),
        }
    }
}

impl Spanned for Array {
    fn span(&self) -> Span {
        let Array {
            elem,
            named: _, // bool
        } = self;

        union_spans(elem.iter().map(|i| i.span()))
    }
}

impl Spanned for Function {
    fn span(&self) -> Span {
        let Function {
            name,
            uses_odbc_syntax: _,
            parameters,
            args,
            filter,
            null_treatment: _, // enum
            over: _,           // todo
            within_group,
        } = self;

        union_spans(
            name.0
                .iter()
                .map(|i| i.span())
                .chain(iter::once(args.span()))
                .chain(iter::once(parameters.span()))
                .chain(filter.iter().map(|i| i.span()))
                .chain(within_group.iter().map(|i| i.span())),
        )
    }
}

/// # partial span
///
/// The span of [FunctionArguments::None] is empty.
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
        let FunctionArgumentList {
            duplicate_treatment: _, // enum
            args,
            clauses,
        } = self;

        union_spans(
            // # todo: duplicate-treatment span
            args.iter()
                .map(|i| i.span())
                .chain(clauses.iter().map(|i| i.span())),
        )
    }
}

impl Spanned for FunctionArgumentClause {
    fn span(&self) -> Span {
        match self {
            FunctionArgumentClause::IgnoreOrRespectNulls(_) => Span::empty(),
            FunctionArgumentClause::OrderBy(vec) => union_spans(vec.iter().map(|i| i.expr.span())),
            FunctionArgumentClause::Limit(expr) => expr.span(),
            FunctionArgumentClause::OnOverflow(_) => Span::empty(),
            FunctionArgumentClause::Having(HavingBound(_kind, expr)) => expr.span(),
            FunctionArgumentClause::Separator(value) => value.span(),
            FunctionArgumentClause::JsonNullClause(_) => Span::empty(),
        }
    }
}

/// # partial span
///
/// see Spanned impl for JsonPathElem for more information
impl Spanned for JsonPath {
    fn span(&self) -> Span {
        let JsonPath { path } = self;

        union_spans(path.iter().map(|i| i.span()))
    }
}

/// # partial span
///
/// Missing spans:
/// - [JsonPathElem::Dot]
impl Spanned for JsonPathElem {
    fn span(&self) -> Span {
        match self {
            JsonPathElem::Dot { .. } => Span::empty(),
            JsonPathElem::Bracket { key } => key.span(),
        }
    }
}

impl Spanned for SelectItemQualifiedWildcardKind {
    fn span(&self) -> Span {
        match self {
            SelectItemQualifiedWildcardKind::ObjectName(object_name) => object_name.span(),
            SelectItemQualifiedWildcardKind::Expr(expr) => expr.span(),
        }
    }
}

impl Spanned for SelectItem {
    fn span(&self) -> Span {
        match self {
            SelectItem::UnnamedExpr(expr) => expr.span(),
            SelectItem::ExprWithAlias { expr, alias } => expr.span().union(&alias.span),
            SelectItem::QualifiedWildcard(kind, wildcard_additional_options) => union_spans(
                [kind.span()]
                    .into_iter()
                    .chain(iter::once(wildcard_additional_options.span())),
            ),
            SelectItem::Wildcard(wildcard_additional_options) => wildcard_additional_options.span(),
        }
    }
}

impl Spanned for WildcardAdditionalOptions {
    fn span(&self) -> Span {
        let WildcardAdditionalOptions {
            wildcard_token,
            opt_ilike,
            opt_exclude,
            opt_except,
            opt_replace,
            opt_rename,
        } = self;

        union_spans(
            core::iter::once(wildcard_token.0.span)
                .chain(opt_ilike.as_ref().map(|i| i.span()))
                .chain(opt_exclude.as_ref().map(|i| i.span()))
                .chain(opt_rename.as_ref().map(|i| i.span()))
                .chain(opt_replace.as_ref().map(|i| i.span()))
                .chain(opt_except.as_ref().map(|i| i.span())),
        )
    }
}

/// # missing span
impl Spanned for IlikeSelectItem {
    fn span(&self) -> Span {
        Span::empty()
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
        let ExceptSelectItem {
            first_element,
            additional_elements,
        } = self;

        union_spans(
            iter::once(first_element.span).chain(additional_elements.iter().map(|i| i.span)),
        )
    }
}

impl Spanned for ReplaceSelectItem {
    fn span(&self) -> Span {
        let ReplaceSelectItem { items } = self;

        union_spans(items.iter().map(|i| i.span()))
    }
}

impl Spanned for ReplaceSelectElement {
    fn span(&self) -> Span {
        let ReplaceSelectElement {
            expr,
            column_name,
            as_keyword: _, // bool
        } = self;

        expr.span().union(&column_name.span)
    }
}

/// # partial span
///
/// Missing spans:
/// - [TableFactor::JsonTable]
impl Spanned for TableFactor {
    fn span(&self) -> Span {
        match self {
            TableFactor::Table {
                name,
                alias,
                args: _,
                with_hints: _,
                version: _,
                with_ordinality: _,
                partitions: _,
                json_path: _,
                sample: _,
                index_hints: _,
            } => union_spans(
                name.0
                    .iter()
                    .map(|i| i.span())
                    .chain(alias.as_ref().map(|alias| {
                        union_spans(
                            iter::once(alias.name.span)
                                .chain(alias.columns.iter().map(|i| i.span())),
                        )
                    })),
            ),
            TableFactor::Derived {
                lateral: _,
                subquery,
                alias,
            } => subquery
                .span()
                .union_opt(&alias.as_ref().map(|alias| alias.span())),
            TableFactor::TableFunction { expr, alias } => expr
                .span()
                .union_opt(&alias.as_ref().map(|alias| alias.span())),
            TableFactor::UNNEST {
                alias,
                with_offset: _,
                with_offset_alias,
                array_exprs,
                with_ordinality: _,
            } => union_spans(
                alias
                    .iter()
                    .map(|i| i.span())
                    .chain(array_exprs.iter().map(|i| i.span()))
                    .chain(with_offset_alias.as_ref().map(|i| i.span)),
            ),
            TableFactor::NestedJoin {
                table_with_joins,
                alias,
            } => table_with_joins
                .span()
                .union_opt(&alias.as_ref().map(|alias| alias.span())),
            TableFactor::Function {
                lateral: _,
                name,
                args,
                alias,
            } => union_spans(
                name.0
                    .iter()
                    .map(|i| i.span())
                    .chain(args.iter().map(|i| i.span()))
                    .chain(alias.as_ref().map(|alias| alias.span())),
            ),
            TableFactor::JsonTable { .. } => Span::empty(),
            TableFactor::XmlTable { .. } => Span::empty(),
            TableFactor::Pivot {
                table,
                aggregate_functions,
                value_column,
                value_source,
                default_on_null,
                alias,
            } => union_spans(
                core::iter::once(table.span())
                    .chain(aggregate_functions.iter().map(|i| i.span()))
                    .chain(value_column.iter().map(|i| i.span()))
                    .chain(core::iter::once(value_source.span()))
                    .chain(default_on_null.as_ref().map(|i| i.span()))
                    .chain(alias.as_ref().map(|i| i.span())),
            ),
            TableFactor::Unpivot {
                table,
                value,
                null_inclusion: _,
                name,
                columns,
                alias,
            } => union_spans(
                core::iter::once(table.span())
                    .chain(core::iter::once(value.span()))
                    .chain(core::iter::once(name.span))
                    .chain(columns.iter().map(|ilist| ilist.span()))
                    .chain(alias.as_ref().map(|alias| alias.span())),
            ),
            TableFactor::MatchRecognize {
                table,
                partition_by,
                order_by,
                measures,
                rows_per_match: _,
                after_match_skip: _,
                pattern,
                symbols,
                alias,
            } => union_spans(
                core::iter::once(table.span())
                    .chain(partition_by.iter().map(|i| i.span()))
                    .chain(order_by.iter().map(|i| i.span()))
                    .chain(measures.iter().map(|i| i.span()))
                    .chain(core::iter::once(pattern.span()))
                    .chain(symbols.iter().map(|i| i.span()))
                    .chain(alias.as_ref().map(|i| i.span())),
            ),
            TableFactor::SemanticView {
                name,
                dimensions,
                metrics,
                facts,
                where_clause,
                alias,
            } => union_spans(
                name.0
                    .iter()
                    .map(|i| i.span())
                    .chain(dimensions.iter().map(|d| d.span()))
                    .chain(metrics.iter().map(|m| m.span()))
                    .chain(facts.iter().map(|f| f.span()))
                    .chain(where_clause.as_ref().map(|e| e.span()))
                    .chain(alias.as_ref().map(|a| a.span())),
            ),
            TableFactor::OpenJsonTable { .. } => Span::empty(),
        }
    }
}

impl Spanned for PivotValueSource {
    fn span(&self) -> Span {
        match self {
            PivotValueSource::List(vec) => union_spans(vec.iter().map(|i| i.span())),
            PivotValueSource::Any(vec) => union_spans(vec.iter().map(|i| i.span())),
            PivotValueSource::Subquery(query) => query.span(),
        }
    }
}

impl Spanned for ExprWithAlias {
    fn span(&self) -> Span {
        let ExprWithAlias { expr, alias } = self;

        expr.span().union_opt(&alias.as_ref().map(|i| i.span))
    }
}

/// # missing span
impl Spanned for MatchRecognizePattern {
    fn span(&self) -> Span {
        Span::empty()
    }
}

impl Spanned for SymbolDefinition {
    fn span(&self) -> Span {
        let SymbolDefinition { symbol, definition } = self;

        symbol.span.union(&definition.span())
    }
}

impl Spanned for Measure {
    fn span(&self) -> Span {
        let Measure { expr, alias } = self;

        expr.span().union(&alias.span)
    }
}

impl Spanned for OrderByExpr {
    fn span(&self) -> Span {
        let OrderByExpr {
            expr,
            options: _,
            with_fill,
        } = self;

        expr.span().union_opt(&with_fill.as_ref().map(|f| f.span()))
    }
}

impl Spanned for WithFill {
    fn span(&self) -> Span {
        let WithFill { from, to, step } = self;

        union_spans(
            from.iter()
                .map(|f| f.span())
                .chain(to.iter().map(|t| t.span()))
                .chain(step.iter().map(|s| s.span())),
        )
    }
}

impl Spanned for FunctionArg {
    fn span(&self) -> Span {
        match self {
            FunctionArg::Named {
                name,
                arg,
                operator: _,
            } => name.span.union(&arg.span()),
            FunctionArg::Unnamed(arg) => arg.span(),
            FunctionArg::ExprNamed {
                name,
                arg,
                operator: _,
            } => name.span().union(&arg.span()),
        }
    }
}

/// # partial span
///
/// Missing spans:
/// - [FunctionArgExpr::Wildcard]
impl Spanned for FunctionArgExpr {
    fn span(&self) -> Span {
        match self {
            FunctionArgExpr::Expr(expr) => expr.span(),
            FunctionArgExpr::QualifiedWildcard(object_name) => {
                union_spans(object_name.0.iter().map(|i| i.span()))
            }
            FunctionArgExpr::Wildcard => Span::empty(),
        }
    }
}

impl Spanned for TableAlias {
    fn span(&self) -> Span {
        let TableAlias { name, columns } = self;

        union_spans(iter::once(name.span).chain(columns.iter().map(|i| i.span())))
    }
}

impl Spanned for TableAliasColumnDef {
    fn span(&self) -> Span {
        let TableAliasColumnDef { name, data_type: _ } = self;

        name.span
    }
}

impl Spanned for ValueWithSpan {
    fn span(&self) -> Span {
        self.span
    }
}

/// The span is stored in the `ValueWrapper` struct
impl Spanned for Value {
    fn span(&self) -> Span {
        Span::empty() // # todo: Value needs to store spans before this is possible
    }
}

impl Spanned for Join {
    fn span(&self) -> Span {
        let Join {
            relation,
            global: _, // bool
            join_operator,
        } = self;

        relation.span().union(&join_operator.span())
    }
}

/// # partial span
///
/// Missing spans:
/// - [JoinOperator::CrossJoin]
/// - [JoinOperator::CrossApply]
/// - [JoinOperator::OuterApply]
impl Spanned for JoinOperator {
    fn span(&self) -> Span {
        match self {
            JoinOperator::Join(join_constraint) => join_constraint.span(),
            JoinOperator::Inner(join_constraint) => join_constraint.span(),
            JoinOperator::Left(join_constraint) => join_constraint.span(),
            JoinOperator::LeftOuter(join_constraint) => join_constraint.span(),
            JoinOperator::Right(join_constraint) => join_constraint.span(),
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
            } => match_condition.span().union(&constraint.span()),
            JoinOperator::Anti(join_constraint) => join_constraint.span(),
            JoinOperator::Semi(join_constraint) => join_constraint.span(),
            JoinOperator::StraightJoin(join_constraint) => join_constraint.span(),
        }
    }
}

/// # partial span
///
/// Missing spans:
/// - [JoinConstraint::Natural]
/// - [JoinConstraint::None]
impl Spanned for JoinConstraint {
    fn span(&self) -> Span {
        match self {
            JoinConstraint::On(expr) => expr.span(),
            JoinConstraint::Using(vec) => union_spans(vec.iter().map(|i| i.span())),
            JoinConstraint::Natural => Span::empty(),
            JoinConstraint::None => Span::empty(),
        }
    }
}

impl Spanned for TableWithJoins {
    fn span(&self) -> Span {
        let TableWithJoins { relation, joins } = self;

        union_spans(core::iter::once(relation.span()).chain(joins.iter().map(|item| item.span())))
    }
}

impl Spanned for Select {
    fn span(&self) -> Span {
        let Select {
            select_token,
            distinct: _, // todo
            top: _,      // todo, mysql specific
            projection,
            exclude: _,
            into,
            from,
            lateral_views,
            prewhere,
            selection,
            group_by,
            cluster_by,
            distribute_by,
            sort_by,
            having,
            named_window,
            qualify,
            window_before_qualify: _, // bool
            value_table_mode: _,      // todo, BigQuery specific
            connect_by,
            top_before_distinct: _,
            flavor: _,
        } = self;

        union_spans(
            core::iter::once(select_token.0.span)
                .chain(projection.iter().map(|item| item.span()))
                .chain(into.iter().map(|item| item.span()))
                .chain(from.iter().map(|item| item.span()))
                .chain(lateral_views.iter().map(|item| item.span()))
                .chain(prewhere.iter().map(|item| item.span()))
                .chain(selection.iter().map(|item| item.span()))
                .chain(core::iter::once(group_by.span()))
                .chain(cluster_by.iter().map(|item| item.span()))
                .chain(distribute_by.iter().map(|item| item.span()))
                .chain(sort_by.iter().map(|item| item.span()))
                .chain(having.iter().map(|item| item.span()))
                .chain(named_window.iter().map(|item| item.span()))
                .chain(qualify.iter().map(|item| item.span()))
                .chain(connect_by.iter().map(|item| item.span())),
        )
    }
}

impl Spanned for ConnectBy {
    fn span(&self) -> Span {
        let ConnectBy {
            condition,
            relationships,
        } = self;

        union_spans(
            core::iter::once(condition.span()).chain(relationships.iter().map(|item| item.span())),
        )
    }
}

impl Spanned for NamedWindowDefinition {
    fn span(&self) -> Span {
        let NamedWindowDefinition(
            ident,
            _, // todo: NamedWindowExpr
        ) = self;

        ident.span
    }
}

impl Spanned for LateralView {
    fn span(&self) -> Span {
        let LateralView {
            lateral_view,
            lateral_view_name,
            lateral_col_alias,
            outer: _, // bool
        } = self;

        union_spans(
            core::iter::once(lateral_view.span())
                .chain(core::iter::once(lateral_view_name.span()))
                .chain(lateral_col_alias.iter().map(|i| i.span)),
        )
    }
}

impl Spanned for SelectInto {
    fn span(&self) -> Span {
        let SelectInto {
            temporary: _, // bool
            unlogged: _,  // bool
            table: _,     // bool
            name,
        } = self;

        name.span()
    }
}

impl Spanned for UpdateTableFromKind {
    fn span(&self) -> Span {
        let from = match self {
            UpdateTableFromKind::BeforeSet(from) => from,
            UpdateTableFromKind::AfterSet(from) => from,
        };
        union_spans(from.iter().map(|t| t.span()))
    }
}

impl Spanned for TableObject {
    fn span(&self) -> Span {
        match self {
            TableObject::TableName(ObjectName(segments)) => {
                union_spans(segments.iter().map(|i| i.span()))
            }
            TableObject::TableFunction(func) => func.span(),
        }
    }
}

impl Spanned for BeginEndStatements {
    fn span(&self) -> Span {
        let BeginEndStatements {
            begin_token,
            statements,
            end_token,
        } = self;
        union_spans(
            core::iter::once(begin_token.0.span)
                .chain(statements.iter().map(|i| i.span()))
                .chain(core::iter::once(end_token.0.span)),
        )
    }
}

impl Spanned for OpenStatement {
    fn span(&self) -> Span {
        let OpenStatement { cursor_name } = self;
        cursor_name.span
    }
}

impl Spanned for AlterSchemaOperation {
    fn span(&self) -> Span {
        match self {
            AlterSchemaOperation::SetDefaultCollate { collate } => collate.span(),
            AlterSchemaOperation::AddReplica { replica, options } => union_spans(
                core::iter::once(replica.span)
                    .chain(options.iter().flat_map(|i| i.iter().map(|i| i.span()))),
            ),
            AlterSchemaOperation::DropReplica { replica } => replica.span,
            AlterSchemaOperation::SetOptionsParens { options } => {
                union_spans(options.iter().map(|i| i.span()))
            }
        }
    }
}

impl Spanned for AlterSchema {
    fn span(&self) -> Span {
        union_spans(
            core::iter::once(self.name.span()).chain(self.operations.iter().map(|i| i.span())),
        )
    }
}

#[cfg(test)]
pub mod tests {
    use crate::dialect::{Dialect, GenericDialect, SnowflakeDialect};
    use crate::parser::Parser;
    use crate::tokenizer::Span;

    use super::*;

    struct SpanTest<'a>(Parser<'a>, &'a str);

    impl<'a> SpanTest<'a> {
        fn new(dialect: &'a dyn Dialect, sql: &'a str) -> Self {
            Self(Parser::new(dialect).try_with_sql(sql).unwrap(), sql)
        }

        // get the subsection of the source string that corresponds to the span
        // only works on single-line strings
        fn get_source(&self, span: Span) -> &'a str {
            // lines in spans are 1-indexed
            &self.1[(span.start.column as usize - 1)..(span.end.column - 1) as usize]
        }
    }

    #[test]
    fn test_join() {
        let dialect = &GenericDialect;
        let mut test = SpanTest::new(
            dialect,
            "SELECT id, name FROM users LEFT JOIN companies ON users.company_id = companies.id",
        );

        let query = test.0.parse_select().unwrap();
        let select_span = query.span();

        assert_eq!(
            test.get_source(select_span),
            "SELECT id, name FROM users LEFT JOIN companies ON users.company_id = companies.id"
        );

        let join_span = query.from[0].joins[0].span();

        // 'LEFT JOIN' missing
        assert_eq!(
            test.get_source(join_span),
            "companies ON users.company_id = companies.id"
        );
    }

    #[test]
    pub fn test_union() {
        let dialect = &GenericDialect;
        let mut test = SpanTest::new(
            dialect,
            "SELECT a FROM postgres.public.source UNION SELECT a FROM postgres.public.source",
        );

        let query = test.0.parse_query().unwrap();
        let select_span = query.span();

        assert_eq!(
            test.get_source(select_span),
            "SELECT a FROM postgres.public.source UNION SELECT a FROM postgres.public.source"
        );
    }

    #[test]
    pub fn test_subquery() {
        let dialect = &GenericDialect;
        let mut test = SpanTest::new(
            dialect,
            "SELECT a FROM (SELECT a FROM postgres.public.source) AS b",
        );

        let query = test.0.parse_select().unwrap();
        let select_span = query.span();

        assert_eq!(
            test.get_source(select_span),
            "SELECT a FROM (SELECT a FROM postgres.public.source) AS b"
        );

        let subquery_span = query.from[0].span();

        // left paren missing
        assert_eq!(
            test.get_source(subquery_span),
            "SELECT a FROM postgres.public.source) AS b"
        );
    }

    #[test]
    pub fn test_cte() {
        let dialect = &GenericDialect;
        let mut test = SpanTest::new(dialect, "WITH cte_outer AS (SELECT a FROM postgres.public.source), cte_ignored AS (SELECT a FROM cte_outer), cte_inner AS (SELECT a FROM cte_outer) SELECT a FROM cte_inner");

        let query = test.0.parse_query().unwrap();

        let select_span = query.span();

        assert_eq!(test.get_source(select_span), "WITH cte_outer AS (SELECT a FROM postgres.public.source), cte_ignored AS (SELECT a FROM cte_outer), cte_inner AS (SELECT a FROM cte_outer) SELECT a FROM cte_inner");
    }

    #[test]
    pub fn test_snowflake_lateral_flatten() {
        let dialect = &SnowflakeDialect;
        let mut test = SpanTest::new(dialect, "SELECT FLATTENED.VALUE:field::TEXT AS FIELD FROM SNOWFLAKE.SCHEMA.SOURCE AS S, LATERAL FLATTEN(INPUT => S.JSON_ARRAY) AS FLATTENED");

        let query = test.0.parse_select().unwrap();

        let select_span = query.span();

        assert_eq!(test.get_source(select_span), "SELECT FLATTENED.VALUE:field::TEXT AS FIELD FROM SNOWFLAKE.SCHEMA.SOURCE AS S, LATERAL FLATTEN(INPUT => S.JSON_ARRAY) AS FLATTENED");
    }

    #[test]
    pub fn test_wildcard_from_cte() {
        let dialect = &GenericDialect;
        let mut test = SpanTest::new(
            dialect,
            "WITH cte AS (SELECT a FROM postgres.public.source) SELECT cte.* FROM cte",
        );

        let query = test.0.parse_query().unwrap();
        let cte_span = query.clone().with.unwrap().cte_tables[0].span();
        let cte_query_span = query.clone().with.unwrap().cte_tables[0].query.span();
        let body_span = query.body.span();

        // the WITH keyboard is part of the query
        assert_eq!(
            test.get_source(cte_span),
            "cte AS (SELECT a FROM postgres.public.source)"
        );
        assert_eq!(
            test.get_source(cte_query_span),
            "SELECT a FROM postgres.public.source"
        );

        assert_eq!(test.get_source(body_span), "SELECT cte.* FROM cte");
    }

    #[test]
    fn test_case_expr_span() {
        let dialect = &GenericDialect;
        let mut test = SpanTest::new(dialect, "CASE 1 WHEN 2 THEN 3 ELSE 4 END");
        let expr = test.0.parse_expr().unwrap();
        let expr_span = expr.span();
        assert_eq!(
            test.get_source(expr_span),
            "CASE 1 WHEN 2 THEN 3 ELSE 4 END"
        );
    }

    #[test]
    fn test_placeholder_span() {
        let sql = "\nSELECT\n  :fooBar";
        let r = Parser::parse_sql(&GenericDialect, sql).unwrap();
        assert_eq!(1, r.len());
        match &r[0] {
            Statement::Query(q) => {
                let col = &q.body.as_select().unwrap().projection[0];
                match col {
                    SelectItem::UnnamedExpr(Expr::Value(ValueWithSpan {
                        value: Value::Placeholder(s),
                        span,
                    })) => {
                        assert_eq!(":fooBar", s);
                        assert_eq!(&Span::new((3, 3).into(), (3, 10).into()), span);
                    }
                    _ => panic!("expected unnamed expression; got {col:?}"),
                }
            }
            stmt => panic!("expected query; got {stmt:?}"),
        }
    }

    #[test]
    fn test_alter_table_multiline_span() {
        let sql = r#"-- foo
ALTER TABLE users
  ADD COLUMN foo
  varchar; -- hi there"#;

        let r = Parser::parse_sql(&crate::dialect::PostgreSqlDialect {}, sql).unwrap();
        assert_eq!(1, r.len());

        let stmt_span = r[0].span();

        assert_eq!(stmt_span.start, (2, 13).into());
        assert_eq!(stmt_span.end, (4, 11).into());
    }
}

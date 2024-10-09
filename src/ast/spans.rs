use core::iter;

use crate::tokenizer::Span;

use super::{
    AlterColumnOperation, AlterIndexOperation, AlterTableOperation, Array, Assignment,
    AssignmentTarget, CloseCursor, ClusteredIndex, ColumnDef, ColumnOption, ColumnOptionDef,
    ConflictTarget, ConstraintCharacteristics, CopySource, CreateIndex, CreateTable,
    CreateTableOptions, Cte, Delete, DoUpdate, ExceptSelectItem, ExcludeSelectItem, Expr,
    ExprWithAlias, FromTable, Function, FunctionArg, FunctionArgExpr, FunctionArgumentClause,
    FunctionArgumentList, FunctionArguments, GroupByExpr, HavingBound, IlikeSelectItem, Insert,
    Interpolate, InterpolateExpr, Join, JoinConstraint, JoinOperator, JsonPath, JsonPathElem,
    MatchRecognizePattern, Measure, ObjectName, OnConflict, OnConflictAction, OnInsert, OrderBy,
    OrderByExpr, Partition, PivotValueSource, ProjectionSelect, Query, ReferentialAction,
    RenameSelectItem, ReplaceSelectElement, ReplaceSelectItem, Select, SelectItem, SetExpr,
    SqlOption, Statement, Subscript, SymbolDefinition, TableAlias, TableConstraint, TableFactor,
    TableOptionsClustered, TableWithJoins, Use, Value, Values, ViewColumnDef,
    WildcardAdditionalOptions, With, WithFill,
};

/// Given an iterator of spans, return the [Span::union] of all spans.
fn union_spans<I: Iterator<Item = Span>>(iter: I) -> Span {
    iter.reduce(|acc, item| acc.union(&item))
        .unwrap_or(Span::empty())
}

/// A trait for AST nodes that have a source span for use in diagnostics.
///
/// Source spans are not guaranteed to be entirely accurate. They may
/// be missing keywords or other tokens. Some nodes may not have a computable
/// span at all, in which case they return [`Span::empty()`].
///
/// Some impl blocks may contain doc comments with information
/// on which nodes are missing spans.
pub trait Spanned {
    /// Compute the source span for this AST node, by recursively
    /// combining the spans of its children.
    fn span(&self) -> Span;
}

impl Spanned for Query {
    fn span(&self) -> Span {
        self.body
            .span()
            .union_opt(&self.with.as_ref().map(|i| i.span()))
    }
}

impl Spanned for With {
    fn span(&self) -> Span {
        union_spans(
            core::iter::once(self.with_token.span)
                .chain(self.cte_tables.iter().map(|item| item.span())),
        )
    }
}

impl Spanned for Cte {
    fn span(&self) -> Span {
        union_spans(
            core::iter::once(self.alias.span())
                .chain(core::iter::once(self.query.span()))
                .chain(self.from.iter().map(|item| item.span))
                .chain(core::iter::once(self.closing_paren_token.span)),
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
        }
    }
}

impl Spanned for Values {
    fn span(&self) -> Span {
        union_spans(
            self.rows
                .iter()
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
/// - [Statement::SetRole]
/// - [Statement::SetVariable]
/// - [Statement::SetTimeZone]
/// - [Statement::SetNames]
/// - [Statement::SetNamesDefault]
/// - [Statement::ShowFunctions]
/// - [Statement::ShowVariable]
/// - [Statement::ShowStatus]
/// - [Statement::ShowVariables]
/// - [Statement::ShowCreate]
/// - [Statement::ShowColumns]
/// - [Statement::ShowTables]
/// - [Statement::ShowCollation]
/// - [Statement::StartTransaction]
/// - [Statement::SetTransaction]
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
            } => union_spans(
                core::iter::once(table_name.span())
                    .chain(partitions.iter().flat_map(|i| i.iter().map(|k| k.span())))
                    .chain(columns.iter().map(|i| i.span)),
            ),
            Statement::Truncate {
                table_names,
                partitions,
                table: _,
                only: _,
                identity: _,
                cascade: _,
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
                from_stage: _,
                from_stage_alias: _,
                stage_params: _,
                from_transformations: _,
                files: _,
                pattern: _,
                file_format: _,
                copy_options: _,
                validation_mode: _,
            } => Span::empty(),
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
            } => union_spans(
                core::iter::once(table.span())
                    .chain(assignments.iter().map(|i| i.span()))
                    .chain(from.iter().map(|i| i.span()))
                    .chain(selection.iter().map(|i| i.span()))
                    .chain(returning.iter().flat_map(|i| i.iter().map(|k| k.span()))),
            ),
            Statement::Delete(delete) => delete.span(),
            Statement::CreateView {
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
            Statement::AlterTable {
                name,
                if_exists: _,
                only: _,
                operations,
                location: _,
                on_cluster,
            } => union_spans(
                core::iter::once(name.span())
                    .chain(operations.iter().map(|i| i.span()))
                    .chain(on_cluster.iter().map(|i| i.span)),
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
            Statement::AlterRole { .. } => Span::empty(),
            Statement::AttachDatabase { .. } => Span::empty(),
            Statement::AttachDuckDBDatabase { .. } => Span::empty(),
            Statement::DetachDuckDBDatabase { .. } => Span::empty(),
            Statement::Drop { .. } => Span::empty(),
            Statement::DropFunction { .. } => Span::empty(),
            Statement::DropProcedure { .. } => Span::empty(),
            Statement::DropSecret { .. } => Span::empty(),
            Statement::Declare { .. } => Span::empty(),
            Statement::CreateExtension { .. } => Span::empty(),
            Statement::Fetch { .. } => Span::empty(),
            Statement::Flush { .. } => Span::empty(),
            Statement::Discard { .. } => Span::empty(),
            Statement::SetRole { .. } => Span::empty(),
            Statement::SetVariable { .. } => Span::empty(),
            Statement::SetTimeZone { .. } => Span::empty(),
            Statement::SetNames { .. } => Span::empty(),
            Statement::SetNamesDefault {} => Span::empty(),
            Statement::ShowFunctions { .. } => Span::empty(),
            Statement::ShowVariable { .. } => Span::empty(),
            Statement::ShowStatus { .. } => Span::empty(),
            Statement::ShowVariables { .. } => Span::empty(),
            Statement::ShowCreate { .. } => Span::empty(),
            Statement::ShowColumns { .. } => Span::empty(),
            Statement::ShowTables { .. } => Span::empty(),
            Statement::ShowCollation { .. } => Span::empty(),
            Statement::Use(u) => u.span(),
            Statement::StartTransaction { .. } => Span::empty(),
            Statement::SetTransaction { .. } => Span::empty(),
            Statement::Comment { .. } => Span::empty(),
            Statement::Commit { .. } => Span::empty(),
            Statement::Rollback { .. } => Span::empty(),
            Statement::CreateSchema { .. } => Span::empty(),
            Statement::CreateDatabase { .. } => Span::empty(),
            Statement::CreateFunction { .. } => Span::empty(),
            Statement::CreateTrigger { .. } => Span::empty(),
            Statement::DropTrigger { .. } => Span::empty(),
            Statement::CreateProcedure { .. } => Span::empty(),
            Statement::CreateMacro { .. } => Span::empty(),
            Statement::CreateStage { .. } => Span::empty(),
            Statement::Assert { .. } => Span::empty(),
            Statement::Grant { .. } => Span::empty(),
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
            Use::Object(object_name) => object_name.span(),
            Use::Default => Span::empty(),
        }
    }
}

impl Spanned for CreateTable {
    fn span(&self) -> Span {
        union_spans(
            core::iter::once(self.name.span())
                .chain(self.columns.iter().map(|i| i.span()))
                .chain(self.constraints.iter().map(|i| i.span()))
                .chain(self.table_properties.iter().map(|i| i.span()))
                .chain(self.with_options.iter().map(|i| i.span()))
                .chain(self.query.iter().map(|i| i.span()))
                .chain(self.like.iter().map(|i| i.span()))
                .chain(self.clone.iter().map(|i| i.span())),
        )
    }
}

impl Spanned for ColumnDef {
    fn span(&self) -> Span {
        union_spans(
            core::iter::once(self.name.span)
                .chain(self.collation.iter().map(|i| i.span()))
                .chain(self.options.iter().map(|i| i.span())),
        )
    }
}

impl Spanned for ColumnOptionDef {
    fn span(&self) -> Span {
        self.option
            .span()
            .union_opt(&self.name.as_ref().map(|i| i.span))
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
            } => union_spans(
                name.iter()
                    .map(|i| i.span)
                    .chain(index_name.iter().map(|i| i.span))
                    .chain(columns.iter().map(|i| i.span))
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
                    .chain(columns.iter().map(|i| i.span))
                    .chain(characteristics.iter().map(|i| i.span())),
            ),
            TableConstraint::ForeignKey {
                name,
                columns,
                foreign_table,
                referred_columns,
                on_delete,
                on_update,
                characteristics,
            } => union_spans(
                name.iter()
                    .map(|i| i.span)
                    .chain(columns.iter().map(|i| i.span))
                    .chain(core::iter::once(foreign_table.span()))
                    .chain(referred_columns.iter().map(|i| i.span))
                    .chain(on_delete.iter().map(|i| i.span()))
                    .chain(on_update.iter().map(|i| i.span()))
                    .chain(characteristics.iter().map(|i| i.span())),
            ),
            TableConstraint::Check { name, expr } => {
                expr.span().union_opt(&name.as_ref().map(|i| i.span))
            }
            TableConstraint::Index {
                display_as_key: _,
                name,
                index_type: _,
                columns,
            } => union_spans(
                name.iter()
                    .map(|i| i.span)
                    .chain(columns.iter().map(|i| i.span)),
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
                    .chain(columns.iter().map(|i| i.span)),
            ),
        }
    }
}

impl Spanned for CreateIndex {
    fn span(&self) -> Span {
        union_spans(
            self.name
                .iter()
                .map(|i| i.span())
                .chain(core::iter::once(self.table_name.span()))
                .chain(self.using.iter().map(|i| i.span))
                .chain(self.columns.iter().map(|i| i.span()))
                .chain(self.include.iter().map(|i| i.span))
                .chain(self.with.iter().map(|i| i.span()))
                .chain(self.predicate.iter().map(|i| i.span())),
        )
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
            ColumnOption::Comment(_) => Span::empty(),
            ColumnOption::OnUpdate(expr) => expr.span(),
            ColumnOption::Generated { .. } => Span::empty(),
            ColumnOption::Options(vec) => union_spans(vec.iter().map(|i| i.span())),
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
        union_spans(
            self.tables
                .iter()
                .map(|i| i.span())
                .chain(core::iter::once(self.from.span()))
                .chain(
                    self.using
                        .iter()
                        .map(|u| union_spans(u.iter().map(|i| i.span()))),
                )
                .chain(self.selection.iter().map(|i| i.span()))
                .chain(
                    self.returning
                        .iter()
                        .flat_map(|i| i.iter().map(|k| k.span())),
                )
                .chain(self.order_by.iter().map(|i| i.span()))
                .chain(self.limit.iter().map(|i| i.span())),
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
        union_spans(
            core::iter::once(self.name.span)
                .chain(self.options.iter().flat_map(|i| i.iter().map(|k| k.span()))),
        )
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
        self.name.span
    }
}

impl Spanned for CreateTableOptions {
    fn span(&self) -> Span {
        match self {
            CreateTableOptions::None => Span::empty(),
            CreateTableOptions::With(vec) => union_spans(vec.iter().map(|i| i.span())),
            CreateTableOptions::Options(vec) => union_spans(vec.iter().map(|i| i.span())),
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
            AlterTableOperation::AddConstraint(table_constraint) => table_constraint.span(),
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
                cascade: _,
            } => name.span,
            AlterTableOperation::DropColumn {
                column_name,
                if_exists: _,
                cascade: _,
            } => column_name.span,
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
            AlterTableOperation::DropPrimaryKey => Span::empty(),
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
        union_spans(
            self.projection
                .iter()
                .map(|i| i.span())
                .chain(self.order_by.iter().map(|i| i.span()))
                .chain(self.group_by.iter().map(|i| i.span())),
        )
    }
}

impl Spanned for OrderBy {
    fn span(&self) -> Span {
        union_spans(
            self.exprs
                .iter()
                .map(|i| i.span())
                .chain(self.interpolate.iter().map(|i| i.span())),
        )
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
        union_spans(self.exprs.iter().flat_map(|i| i.iter().map(|e| e.span())))
    }
}

impl Spanned for InterpolateExpr {
    fn span(&self) -> Span {
        self.column
            .span
            .union_opt(&self.expr.as_ref().map(|e| e.span()))
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
        union_spans(
            core::iter::once(self.table_name.span())
                .chain(self.table_alias.as_ref().map(|i| i.span))
                .chain(self.columns.iter().map(|i| i.span))
                .chain(self.source.as_ref().map(|q| q.span()))
                .chain(
                    self.partitioned
                        .iter()
                        .flat_map(|i| i.iter().map(|k| k.span())),
                )
                .chain(self.after_columns.iter().map(|i| i.span))
                .chain(self.on.as_ref().map(|i| i.span()))
                .chain(
                    self.returning
                        .iter()
                        .flat_map(|i| i.iter().map(|k| k.span())),
                ),
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
        self.action
            .span()
            .union_opt(&self.conflict_target.as_ref().map(|i| i.span()))
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
        union_spans(
            self.assignments
                .iter()
                .map(|i| i.span())
                .chain(self.selection.iter().map(|i| i.span())),
        )
    }
}

impl Spanned for Assignment {
    fn span(&self) -> Span {
        self.target.span().union(&self.value.span())
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
/// - [Expr::TypedString]
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
            } => expr.span().union(&pattern.span()),
            Expr::ILike {
                negated: _,
                expr,
                pattern,
                escape_char: _,
            } => expr.span().union(&pattern.span()),
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
                .union(&union_spans(collation.0.iter().map(|i| i.span))),
            Expr::Nested(expr) => expr.span(),
            Expr::Value(value) => value.span(),
            Expr::TypedString { .. } => Span::empty(),
            Expr::MapAccess { column, keys } => column
                .span()
                .union(&union_spans(keys.iter().map(|i| i.key.span()))),
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
            Expr::RLike { .. } => Span::empty(),
            Expr::AnyOp {
                left,
                compare_op: _,
                right,
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
            Expr::IntroducedString { value, .. } => value.span(),
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
            Expr::Exists { subquery, .. } => subquery.span(),
            Expr::Subquery(query) => query.span(),
            Expr::Struct { .. } => Span::empty(),
            Expr::Named { .. } => Span::empty(),
            Expr::Dictionary(_) => Span::empty(),
            Expr::Map(_) => Span::empty(),
            Expr::Subscript { expr, subscript } => expr.span().union(&subscript.span()),
            Expr::Interval(interval) => interval.value.span(),
            Expr::Wildcard(token) => token.span,
            Expr::QualifiedWildcard(object_name, token) => union_spans(
                object_name
                    .0
                    .iter()
                    .map(|i| i.span)
                    .chain(iter::once(token.span)),
            ),
            Expr::OuterJoin(expr) => expr.span(),
            Expr::Prior(expr) => expr.span(),
            Expr::Lambda(_) => Span::empty(),
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
                .iter()
                .flatten()
                .cloned(),
            ),
        }
    }
}

impl Spanned for ObjectName {
    fn span(&self) -> Span {
        union_spans(self.0.iter().map(|i| i.span))
    }
}

impl Spanned for Array {
    fn span(&self) -> Span {
        union_spans(self.elem.iter().map(|i| i.span()))
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
            FunctionArgumentClause::IgnoreOrRespectNulls(_) => Span::empty(),
            FunctionArgumentClause::OrderBy(vec) => union_spans(vec.iter().map(|i| i.expr.span())),
            FunctionArgumentClause::Limit(expr) => expr.span(),
            FunctionArgumentClause::OnOverflow(_) => Span::empty(),
            FunctionArgumentClause::Having(HavingBound(_kind, expr)) => expr.span(),
            FunctionArgumentClause::Separator(value) => value.span(),
        }
    }
}

/// # partial span
///
/// see Spanned impl for JsonPathElem for more information
impl Spanned for JsonPath {
    fn span(&self) -> Span {
        union_spans(self.path.iter().map(|i| i.span()))
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
            core::iter::once(self.wildcard_token.span)
                .chain(self.opt_ilike.as_ref().map(|i| i.span()))
                .chain(self.opt_exclude.as_ref().map(|i| i.span()))
                .chain(self.opt_rename.as_ref().map(|i| i.span()))
                .chain(self.opt_replace.as_ref().map(|i| i.span()))
                .chain(self.opt_except.as_ref().map(|i| i.span())),
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
            } => union_spans(
                name.0
                    .iter()
                    .map(|i| i.span)
                    .chain(alias.as_ref().map(|alias| {
                        union_spans(
                            iter::once(alias.name.span).chain(alias.columns.iter().map(|i| i.span)),
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
                    .map(|i| i.span)
                    .chain(args.iter().map(|i| i.span()))
                    .chain(alias.as_ref().map(|alias| alias.span())),
            ),
            TableFactor::JsonTable { .. } => Span::empty(),
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
                    .chain(value_column.iter().map(|i| i.span))
                    .chain(core::iter::once(value_source.span()))
                    .chain(default_on_null.as_ref().map(|i| i.span()))
                    .chain(alias.as_ref().map(|i| i.span())),
            ),
            TableFactor::Unpivot {
                table,
                value,
                name,
                columns,
                alias,
            } => union_spans(
                core::iter::once(table.span())
                    .chain(core::iter::once(value.span))
                    .chain(core::iter::once(name.span))
                    .chain(columns.iter().map(|i| i.span))
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
        self.expr
            .span()
            .union_opt(&self.alias.as_ref().map(|i| i.span))
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
        self.symbol.span.union(&self.definition.span())
    }
}

impl Spanned for Measure {
    fn span(&self) -> Span {
        self.expr.span().union(&self.alias.span)
    }
}

impl Spanned for OrderByExpr {
    fn span(&self) -> Span {
        self.expr
            .span()
            .union_opt(&self.with_fill.as_ref().map(|f| f.span()).clone())
    }
}

impl Spanned for WithFill {
    fn span(&self) -> Span {
        union_spans(
            self.from
                .iter()
                .map(|f| f.span())
                .chain(self.to.iter().map(|t| t.span()))
                .chain(self.step.iter().map(|s| s.span())),
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

/// # missing span
///
/// The span of a `Value` is currently not implemented, as doing so
/// requires a breaking changes, which may be done in a future release.
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

/// # partial span
///
/// Missing spans:
/// - [JoinOperator::CrossJoin]
/// - [JoinOperator::CrossApply]
/// - [JoinOperator::OuterApply]
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
            } => match_condition.span().union(&constraint.span()),
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

impl Spanned for Select {
    fn span(&self) -> Span {
        union_spans(
            core::iter::once(self.select_token.span)
                .chain(self.projection.iter().map(|item| item.span()))
                .chain(self.from.iter().map(|item| item.span())),
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
}

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

#[cfg(not(feature = "std"))]
use alloc::{boxed::Box, format, string::String, vec, vec::Vec};

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

#[cfg(feature = "visitor")]
use sqlparser_derive::{Visit, VisitMut};

use crate::ast::{
    ClusteredBy, ColumnDef, CommentDef, CreateTable, CreateTableLikeKind, CreateTableOptions, Expr,
    FileFormat, ForValues, HiveDistributionStyle, HiveFormat, Ident, InitializeKind, ObjectName,
    OnCommit, OneOrManyWithParens, Query, RefreshModeKind, RowAccessPolicy, Statement,
    StorageSerializationPolicy, TableConstraint, TableVersion, Tag, WrappedCollection,
};

use crate::parser::ParserError;

/// Builder for create table statement variant ([1]).
///
/// This structure helps building and accessing a create table with more ease, without needing to:
/// - Match the enum itself a lot of times; or
/// - Moving a lot of variables around the code.
///
/// # Example
/// ```rust
/// use sqlparser::ast::helpers::stmt_create_table::CreateTableBuilder;
/// use sqlparser::ast::{ColumnDef, DataType, Ident, ObjectName};
/// let builder = CreateTableBuilder::new(ObjectName::from(vec![Ident::new("table_name")]))
///    .if_not_exists(true)
///    .columns(vec![ColumnDef {
///        name: Ident::new("c1"),
///        data_type: DataType::Int(None),
///        options: vec![],
/// }]);
/// // You can access internal elements with ease
/// assert!(builder.if_not_exists);
/// // Convert to a statement
/// assert_eq!(
///    builder.build().to_string(),
///    "CREATE TABLE IF NOT EXISTS table_name (c1 INT)"
/// )
/// ```
///
/// [1]: crate::ast::Statement::CreateTable
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct CreateTableBuilder {
    /// Whether the statement uses `OR REPLACE`.
    pub or_replace: bool,
    /// Whether the table is `TEMPORARY`.
    pub temporary: bool,
    /// Whether the table is `EXTERNAL`.
    pub external: bool,
    /// Optional `GLOBAL` flag for dialects that support it.
    pub global: Option<bool>,
    /// Whether `IF NOT EXISTS` was specified.
    pub if_not_exists: bool,
    /// Whether `TRANSIENT` was specified.
    pub transient: bool,
    /// Whether `VOLATILE` was specified.
    pub volatile: bool,
    /// Iceberg-specific table flag.
    pub iceberg: bool,
    /// Whether `DYNAMIC` table option is set.
    pub dynamic: bool,
    /// The table name.
    pub name: ObjectName,
    /// Column definitions for the table.
    pub columns: Vec<ColumnDef>,
    /// Table-level constraints.
    pub constraints: Vec<TableConstraint>,
    /// Hive distribution style.
    pub hive_distribution: HiveDistributionStyle,
    /// Optional Hive format settings.
    pub hive_formats: Option<HiveFormat>,
    /// Optional file format for storage.
    pub file_format: Option<FileFormat>,
    /// Optional storage location.
    pub location: Option<String>,
    /// Optional `AS SELECT` query for the table.
    pub query: Option<Box<Query>>,
    /// Whether `WITHOUT ROWID` is set.
    pub without_rowid: bool,
    /// Optional `LIKE` clause kind.
    pub like: Option<CreateTableLikeKind>,
    /// Optional `CLONE` source object name.
    pub clone: Option<ObjectName>,
    /// Optional table version.
    pub version: Option<TableVersion>,
    /// Optional table comment.
    pub comment: Option<CommentDef>,
    /// Optional `ON COMMIT` behavior.
    pub on_commit: Option<OnCommit>,
    /// Optional cluster identifier.
    pub on_cluster: Option<Ident>,
    /// Optional primary key expression.
    pub primary_key: Option<Box<Expr>>,
    /// Optional `ORDER BY` for clustering/sorting.
    pub order_by: Option<OneOrManyWithParens<Expr>>,
    /// Optional `PARTITION BY` expression.
    pub partition_by: Option<Box<Expr>>,
    /// Optional `CLUSTER BY` expressions.
    pub cluster_by: Option<WrappedCollection<Vec<Expr>>>,
    /// Optional `CLUSTERED BY` clause.
    pub clustered_by: Option<ClusteredBy>,
    /// Optional parent tables (`INHERITS`).
    pub inherits: Option<Vec<ObjectName>>,
    /// Optional partitioned table (`PARTITION OF`)
    pub partition_of: Option<ObjectName>,
    /// Range of values associated with the partition (`FOR VALUES`)
    pub for_values: Option<ForValues>,
    /// `STRICT` table flag.
    pub strict: bool,
    /// Whether to copy grants from the source.
    pub copy_grants: bool,
    /// Optional flag for schema evolution support.
    pub enable_schema_evolution: Option<bool>,
    /// Optional change tracking flag.
    pub change_tracking: Option<bool>,
    /// Optional data retention time in days.
    pub data_retention_time_in_days: Option<u64>,
    /// Optional max data extension time in days.
    pub max_data_extension_time_in_days: Option<u64>,
    /// Optional default DDL collation.
    pub default_ddl_collation: Option<String>,
    /// Optional aggregation policy object name.
    pub with_aggregation_policy: Option<ObjectName>,
    /// Optional row access policy applied to the table.
    pub with_row_access_policy: Option<RowAccessPolicy>,
    /// Optional tags/labels attached to the table metadata.
    pub with_tags: Option<Vec<Tag>>,
    /// Optional base location for staged data.
    pub base_location: Option<String>,
    /// Optional external volume identifier.
    pub external_volume: Option<String>,
    /// Optional catalog name.
    pub catalog: Option<String>,
    /// Optional catalog synchronization option.
    pub catalog_sync: Option<String>,
    /// Optional storage serialization policy.
    pub storage_serialization_policy: Option<StorageSerializationPolicy>,
    /// Parsed table options from the statement.
    pub table_options: CreateTableOptions,
    /// Optional target lag configuration.
    pub target_lag: Option<String>,
    /// Optional warehouse identifier.
    pub warehouse: Option<Ident>,
    /// Optional refresh mode for materialized tables.
    pub refresh_mode: Option<RefreshModeKind>,
    /// Optional initialization kind for the table.
    pub initialize: Option<InitializeKind>,
    /// Whether operations require a user identity.
    pub require_user: bool,
}

impl CreateTableBuilder {
    /// Create a new `CreateTableBuilder` for the given table name.
    pub fn new(name: ObjectName) -> Self {
        Self {
            or_replace: false,
            temporary: false,
            external: false,
            global: None,
            if_not_exists: false,
            transient: false,
            volatile: false,
            iceberg: false,
            dynamic: false,
            name,
            columns: vec![],
            constraints: vec![],
            hive_distribution: HiveDistributionStyle::NONE,
            hive_formats: None,
            file_format: None,
            location: None,
            query: None,
            without_rowid: false,
            like: None,
            clone: None,
            version: None,
            comment: None,
            on_commit: None,
            on_cluster: None,
            primary_key: None,
            order_by: None,
            partition_by: None,
            cluster_by: None,
            clustered_by: None,
            inherits: None,
            partition_of: None,
            for_values: None,
            strict: false,
            copy_grants: false,
            enable_schema_evolution: None,
            change_tracking: None,
            data_retention_time_in_days: None,
            max_data_extension_time_in_days: None,
            default_ddl_collation: None,
            with_aggregation_policy: None,
            with_row_access_policy: None,
            with_tags: None,
            base_location: None,
            external_volume: None,
            catalog: None,
            catalog_sync: None,
            storage_serialization_policy: None,
            table_options: CreateTableOptions::None,
            target_lag: None,
            warehouse: None,
            refresh_mode: None,
            initialize: None,
            require_user: false,
        }
    }
    /// Set `OR REPLACE` for the CREATE TABLE statement.
    pub fn or_replace(mut self, or_replace: bool) -> Self {
        self.or_replace = or_replace;
        self
    }
    /// Mark the table as `TEMPORARY`.
    pub fn temporary(mut self, temporary: bool) -> Self {
        self.temporary = temporary;
        self
    }
    /// Mark the table as `EXTERNAL`.
    pub fn external(mut self, external: bool) -> Self {
        self.external = external;
        self
    }
    /// Set optional `GLOBAL` flag (dialect-specific).
    pub fn global(mut self, global: Option<bool>) -> Self {
        self.global = global;
        self
    }
    /// Set `IF NOT EXISTS`.
    pub fn if_not_exists(mut self, if_not_exists: bool) -> Self {
        self.if_not_exists = if_not_exists;
        self
    }
    /// Set `TRANSIENT` flag.
    pub fn transient(mut self, transient: bool) -> Self {
        self.transient = transient;
        self
    }
    /// Set `VOLATILE` flag.
    pub fn volatile(mut self, volatile: bool) -> Self {
        self.volatile = volatile;
        self
    }
    /// Enable Iceberg table semantics.
    pub fn iceberg(mut self, iceberg: bool) -> Self {
        self.iceberg = iceberg;
        self
    }
    /// Set `DYNAMIC` table option.
    pub fn dynamic(mut self, dynamic: bool) -> Self {
        self.dynamic = dynamic;
        self
    }
    /// Set the table column definitions.
    pub fn columns(mut self, columns: Vec<ColumnDef>) -> Self {
        self.columns = columns;
        self
    }
    /// Set table-level constraints.
    pub fn constraints(mut self, constraints: Vec<TableConstraint>) -> Self {
        self.constraints = constraints;
        self
    }
    /// Set Hive distribution style.
    pub fn hive_distribution(mut self, hive_distribution: HiveDistributionStyle) -> Self {
        self.hive_distribution = hive_distribution;
        self
    }
    /// Set Hive-specific formats.
    pub fn hive_formats(mut self, hive_formats: Option<HiveFormat>) -> Self {
        self.hive_formats = hive_formats;
        self
    }
    /// Set file format for the table (e.g., PARQUET).
    pub fn file_format(mut self, file_format: Option<FileFormat>) -> Self {
        self.file_format = file_format;
        self
    }
    /// Set storage `location` for the table.
    pub fn location(mut self, location: Option<String>) -> Self {
        self.location = location;
        self
    }
    /// Set an underlying `AS SELECT` query for the table.
    pub fn query(mut self, query: Option<Box<Query>>) -> Self {
        self.query = query;
        self
    }
    /// Set `WITHOUT ROWID` option.
    pub fn without_rowid(mut self, without_rowid: bool) -> Self {
        self.without_rowid = without_rowid;
        self
    }
    /// Set `LIKE` clause for the table.
    pub fn like(mut self, like: Option<CreateTableLikeKind>) -> Self {
        self.like = like;
        self
    }
    // Different name to allow the object to be cloned
    /// Set `CLONE` source object name.
    pub fn clone_clause(mut self, clone: Option<ObjectName>) -> Self {
        self.clone = clone;
        self
    }
    /// Set table `VERSION`.
    pub fn version(mut self, version: Option<TableVersion>) -> Self {
        self.version = version;
        self
    }
    /// Set a comment for the table or following column definitions.
    pub fn comment_after_column_def(mut self, comment: Option<CommentDef>) -> Self {
        self.comment = comment;
        self
    }
    /// Set `ON COMMIT` behavior for temporary tables.
    pub fn on_commit(mut self, on_commit: Option<OnCommit>) -> Self {
        self.on_commit = on_commit;
        self
    }
    /// Set cluster identifier for the table.
    pub fn on_cluster(mut self, on_cluster: Option<Ident>) -> Self {
        self.on_cluster = on_cluster;
        self
    }
    /// Set a primary key expression for the table.
    pub fn primary_key(mut self, primary_key: Option<Box<Expr>>) -> Self {
        self.primary_key = primary_key;
        self
    }
    /// Set `ORDER BY` clause for clustered/sorted tables.
    pub fn order_by(mut self, order_by: Option<OneOrManyWithParens<Expr>>) -> Self {
        self.order_by = order_by;
        self
    }
    /// Set `PARTITION BY` expression.
    pub fn partition_by(mut self, partition_by: Option<Box<Expr>>) -> Self {
        self.partition_by = partition_by;
        self
    }
    /// Set `CLUSTER BY` expression(s).
    pub fn cluster_by(mut self, cluster_by: Option<WrappedCollection<Vec<Expr>>>) -> Self {
        self.cluster_by = cluster_by;
        self
    }
    /// Set `CLUSTERED BY` clause.
    pub fn clustered_by(mut self, clustered_by: Option<ClusteredBy>) -> Self {
        self.clustered_by = clustered_by;
        self
    }
    /// Set parent tables via `INHERITS`.
    pub fn inherits(mut self, inherits: Option<Vec<ObjectName>>) -> Self {
        self.inherits = inherits;
        self
    }

    /// Sets the table which is partitioned to create the current table.
    pub fn partition_of(mut self, partition_of: Option<ObjectName>) -> Self {
        self.partition_of = partition_of;
        self
    }

    /// Sets the range of values associated with the partition.
    pub fn for_values(mut self, for_values: Option<ForValues>) -> Self {
        self.for_values = for_values;
        self
    }

    /// Set `STRICT` option.
    pub fn strict(mut self, strict: bool) -> Self {
        self.strict = strict;
        self
    }
    /// Enable copying grants from source object.
    pub fn copy_grants(mut self, copy_grants: bool) -> Self {
        self.copy_grants = copy_grants;
        self
    }
    /// Enable or disable schema evolution features.
    pub fn enable_schema_evolution(mut self, enable_schema_evolution: Option<bool>) -> Self {
        self.enable_schema_evolution = enable_schema_evolution;
        self
    }
    /// Enable or disable change tracking.
    pub fn change_tracking(mut self, change_tracking: Option<bool>) -> Self {
        self.change_tracking = change_tracking;
        self
    }
    /// Set data retention time (in days).
    pub fn data_retention_time_in_days(mut self, data_retention_time_in_days: Option<u64>) -> Self {
        self.data_retention_time_in_days = data_retention_time_in_days;
        self
    }
    /// Set maximum data extension time (in days).
    pub fn max_data_extension_time_in_days(
        mut self,
        max_data_extension_time_in_days: Option<u64>,
    ) -> Self {
        self.max_data_extension_time_in_days = max_data_extension_time_in_days;
        self
    }
    /// Set default DDL collation.
    pub fn default_ddl_collation(mut self, default_ddl_collation: Option<String>) -> Self {
        self.default_ddl_collation = default_ddl_collation;
        self
    }
    /// Set aggregation policy object.
    pub fn with_aggregation_policy(mut self, with_aggregation_policy: Option<ObjectName>) -> Self {
        self.with_aggregation_policy = with_aggregation_policy;
        self
    }
    /// Attach a row access policy to the table.
    pub fn with_row_access_policy(
        mut self,
        with_row_access_policy: Option<RowAccessPolicy>,
    ) -> Self {
        self.with_row_access_policy = with_row_access_policy;
        self
    }
    /// Attach tags/labels to the table metadata.
    pub fn with_tags(mut self, with_tags: Option<Vec<Tag>>) -> Self {
        self.with_tags = with_tags;
        self
    }
    /// Set a base storage location for staged data.
    pub fn base_location(mut self, base_location: Option<String>) -> Self {
        self.base_location = base_location;
        self
    }
    /// Set an external volume identifier.
    pub fn external_volume(mut self, external_volume: Option<String>) -> Self {
        self.external_volume = external_volume;
        self
    }
    /// Set the catalog name for the table.
    pub fn catalog(mut self, catalog: Option<String>) -> Self {
        self.catalog = catalog;
        self
    }
    /// Set catalog synchronization option.
    pub fn catalog_sync(mut self, catalog_sync: Option<String>) -> Self {
        self.catalog_sync = catalog_sync;
        self
    }
    /// Set a storage serialization policy.
    pub fn storage_serialization_policy(
        mut self,
        storage_serialization_policy: Option<StorageSerializationPolicy>,
    ) -> Self {
        self.storage_serialization_policy = storage_serialization_policy;
        self
    }
    /// Set arbitrary table options parsed from the statement.
    pub fn table_options(mut self, table_options: CreateTableOptions) -> Self {
        self.table_options = table_options;
        self
    }
    /// Set a target lag configuration (dialect-specific).
    pub fn target_lag(mut self, target_lag: Option<String>) -> Self {
        self.target_lag = target_lag;
        self
    }
    /// Associate the table with a warehouse identifier.
    pub fn warehouse(mut self, warehouse: Option<Ident>) -> Self {
        self.warehouse = warehouse;
        self
    }
    /// Set refresh mode for materialized/managed tables.
    pub fn refresh_mode(mut self, refresh_mode: Option<RefreshModeKind>) -> Self {
        self.refresh_mode = refresh_mode;
        self
    }
    /// Set initialization mode for the table.
    pub fn initialize(mut self, initialize: Option<InitializeKind>) -> Self {
        self.initialize = initialize;
        self
    }
    /// Require a user identity for table operations.
    pub fn require_user(mut self, require_user: bool) -> Self {
        self.require_user = require_user;
        self
    }
    /// Consume the builder and produce a `Statement::CreateTable`.
    pub fn build(self) -> Statement {
        CreateTable {
            or_replace: self.or_replace,
            temporary: self.temporary,
            external: self.external,
            global: self.global,
            if_not_exists: self.if_not_exists,
            transient: self.transient,
            volatile: self.volatile,
            iceberg: self.iceberg,
            dynamic: self.dynamic,
            name: self.name,
            columns: self.columns,
            constraints: self.constraints,
            hive_distribution: self.hive_distribution,
            hive_formats: self.hive_formats,
            file_format: self.file_format,
            location: self.location,
            query: self.query,
            without_rowid: self.without_rowid,
            like: self.like,
            clone: self.clone,
            version: self.version,
            comment: self.comment,
            on_commit: self.on_commit,
            on_cluster: self.on_cluster,
            primary_key: self.primary_key,
            order_by: self.order_by,
            partition_by: self.partition_by,
            cluster_by: self.cluster_by,
            clustered_by: self.clustered_by,
            inherits: self.inherits,
            partition_of: self.partition_of,
            for_values: self.for_values,
            strict: self.strict,
            copy_grants: self.copy_grants,
            enable_schema_evolution: self.enable_schema_evolution,
            change_tracking: self.change_tracking,
            data_retention_time_in_days: self.data_retention_time_in_days,
            max_data_extension_time_in_days: self.max_data_extension_time_in_days,
            default_ddl_collation: self.default_ddl_collation,
            with_aggregation_policy: self.with_aggregation_policy,
            with_row_access_policy: self.with_row_access_policy,
            with_tags: self.with_tags,
            base_location: self.base_location,
            external_volume: self.external_volume,
            catalog: self.catalog,
            catalog_sync: self.catalog_sync,
            storage_serialization_policy: self.storage_serialization_policy,
            table_options: self.table_options,
            target_lag: self.target_lag,
            warehouse: self.warehouse,
            refresh_mode: self.refresh_mode,
            initialize: self.initialize,
            require_user: self.require_user,
        }
        .into()
    }
}

impl TryFrom<Statement> for CreateTableBuilder {
    type Error = ParserError;

    // As the builder can be transformed back to a statement, it shouldn't be a problem to take the
    // ownership.
    fn try_from(stmt: Statement) -> Result<Self, Self::Error> {
        match stmt {
            Statement::CreateTable(CreateTable {
                or_replace,
                temporary,
                external,
                global,
                if_not_exists,
                transient,
                volatile,
                iceberg,
                dynamic,
                name,
                columns,
                constraints,
                hive_distribution,
                hive_formats,
                file_format,
                location,
                query,
                without_rowid,
                like,
                clone,
                version,
                comment,
                on_commit,
                on_cluster,
                primary_key,
                order_by,
                partition_by,
                cluster_by,
                clustered_by,
                inherits,
                partition_of,
                for_values,
                strict,
                copy_grants,
                enable_schema_evolution,
                change_tracking,
                data_retention_time_in_days,
                max_data_extension_time_in_days,
                default_ddl_collation,
                with_aggregation_policy,
                with_row_access_policy,
                with_tags,
                base_location,
                external_volume,
                catalog,
                catalog_sync,
                storage_serialization_policy,
                table_options,
                target_lag,
                warehouse,
                refresh_mode,
                initialize,
                require_user,
            }) => Ok(Self {
                or_replace,
                temporary,
                external,
                global,
                if_not_exists,
                transient,
                dynamic,
                name,
                columns,
                constraints,
                hive_distribution,
                hive_formats,
                file_format,
                location,
                query,
                without_rowid,
                like,
                clone,
                version,
                comment,
                on_commit,
                on_cluster,
                primary_key,
                order_by,
                partition_by,
                cluster_by,
                clustered_by,
                inherits,
                partition_of,
                for_values,
                strict,
                iceberg,
                copy_grants,
                enable_schema_evolution,
                change_tracking,
                data_retention_time_in_days,
                max_data_extension_time_in_days,
                default_ddl_collation,
                with_aggregation_policy,
                with_row_access_policy,
                with_tags,
                volatile,
                base_location,
                external_volume,
                catalog,
                catalog_sync,
                storage_serialization_policy,
                table_options,
                target_lag,
                warehouse,
                refresh_mode,
                initialize,
                require_user,
            }),
            _ => Err(ParserError::ParserError(format!(
                "Expected create table statement, but received: {stmt}"
            ))),
        }
    }
}

/// Helper return type when parsing configuration for a `CREATE TABLE` statement.
#[derive(Default)]
pub(crate) struct CreateTableConfiguration {
    pub partition_by: Option<Box<Expr>>,
    pub cluster_by: Option<WrappedCollection<Vec<Expr>>>,
    pub inherits: Option<Vec<ObjectName>>,
    pub table_options: CreateTableOptions,
}

#[cfg(test)]
mod tests {
    use crate::ast::helpers::stmt_create_table::CreateTableBuilder;
    use crate::ast::{Ident, ObjectName, Statement};
    use crate::parser::ParserError;

    #[test]
    pub fn test_from_valid_statement() {
        let builder = CreateTableBuilder::new(ObjectName::from(vec![Ident::new("table_name")]));

        let stmt = builder.clone().build();

        assert_eq!(builder, CreateTableBuilder::try_from(stmt).unwrap());
    }

    #[test]
    pub fn test_from_invalid_statement() {
        let stmt = Statement::Commit {
            chain: false,
            end: false,
            modifier: None,
        };

        assert_eq!(
            CreateTableBuilder::try_from(stmt).unwrap_err(),
            ParserError::ParserError(
                "Expected create table statement, but received: COMMIT".to_owned()
            )
        );
    }
}

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

use super::super::dml::CreateTable;
use crate::ast::{
    ClusteredBy, ColumnDef, CommentDef, Expr, FileFormat, HiveDistributionStyle, HiveFormat, Ident,
    ObjectName, OnCommit, OneOrManyWithParens, Query, RowAccessPolicy, SqlOption, Statement,
    StorageSerializationPolicy, TableConstraint, TableEngine, Tag, WrappedCollection,
};
use crate::parser::{
    Compression, DelayKeyWrite, DirectoryOption, Encryption, InsertMethod, OptionState,
    ParserError, RowFormat, TablespaceOption,
};

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
///        collation: None,
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
    pub or_replace: bool,
    pub temporary: bool,
    pub external: bool,
    pub global: Option<bool>,
    pub if_not_exists: bool,
    pub transient: bool,
    pub volatile: bool,
    pub iceberg: bool,
    pub name: ObjectName,
    pub columns: Vec<ColumnDef>,
    pub constraints: Vec<TableConstraint>,
    pub hive_distribution: HiveDistributionStyle,
    pub hive_formats: Option<HiveFormat>,
    pub table_properties: Vec<SqlOption>,
    pub with_options: Vec<SqlOption>,
    pub file_format: Option<FileFormat>,
    pub location: Option<String>,
    pub query: Option<Box<Query>>,
    pub without_rowid: bool,
    pub like: Option<ObjectName>,
    pub clone: Option<ObjectName>,
    pub engine: Option<TableEngine>,
    pub comment: Option<CommentDef>,
    pub auto_increment_offset: Option<u32>,
    pub default_charset: Option<String>,
    pub collation: Option<String>,
    pub key_block_size: Option<u32>,
    pub max_rows: Option<u32>,
    pub min_rows: Option<u32>,
    pub autoextend_size: Option<u32>,
    pub avg_row_length: Option<u32>,
    pub checksum: Option<bool>,
    pub row_format: Option<RowFormat>,
    pub compression: Option<Compression>,
    pub encryption: Option<Encryption>,
    pub insert_method: Option<InsertMethod>,
    pub pack_keys: Option<OptionState>,
    pub stats_auto_recalc: Option<OptionState>,
    pub stats_persistent: Option<OptionState>,
    pub stats_sample_pages: Option<u32>,
    pub delay_key_write: Option<DelayKeyWrite>,
    pub connection: Option<String>,
    pub engine_attribute: Option<String>,
    pub password: Option<String>,
    pub secondary_engine_attribute: Option<String>,
    pub start_transaction: Option<bool>,
    pub tablespace_option: Option<TablespaceOption>,
    pub union_tables: Option<Vec<String>>,
    pub data_directory: Option<DirectoryOption>,
    pub index_directory: Option<DirectoryOption>,
    pub on_commit: Option<OnCommit>,
    pub on_cluster: Option<Ident>,
    pub primary_key: Option<Box<Expr>>,
    pub order_by: Option<OneOrManyWithParens<Expr>>,
    pub partition_by: Option<Box<Expr>>,
    pub cluster_by: Option<WrappedCollection<Vec<Ident>>>,
    pub clustered_by: Option<ClusteredBy>,
    pub options: Option<Vec<SqlOption>>,
    pub strict: bool,
    pub copy_grants: bool,
    pub enable_schema_evolution: Option<bool>,
    pub change_tracking: Option<bool>,
    pub data_retention_time_in_days: Option<u64>,
    pub max_data_extension_time_in_days: Option<u64>,
    pub default_ddl_collation: Option<String>,
    pub with_aggregation_policy: Option<ObjectName>,
    pub with_row_access_policy: Option<RowAccessPolicy>,
    pub with_tags: Option<Vec<Tag>>,
    pub base_location: Option<String>,
    pub external_volume: Option<String>,
    pub catalog: Option<String>,
    pub catalog_sync: Option<String>,
    pub storage_serialization_policy: Option<StorageSerializationPolicy>,
}

impl CreateTableBuilder {
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
            name,
            columns: vec![],
            constraints: vec![],
            hive_distribution: HiveDistributionStyle::NONE,
            hive_formats: None,
            table_properties: vec![],
            with_options: vec![],
            file_format: None,
            location: None,
            query: None,
            without_rowid: false,
            like: None,
            clone: None,
            engine: None,
            comment: None,
            auto_increment_offset: None,
            compression: None,
            encryption: None,
            insert_method: None,
            key_block_size: None,
            row_format: None,
            data_directory: None,
            index_directory: None,
            pack_keys: None,
            stats_auto_recalc: None,
            stats_persistent: None,
            stats_sample_pages: None,
            delay_key_write: None,
            max_rows: None,
            min_rows: None,
            autoextend_size: None,
            avg_row_length: None,
            checksum: None,
            connection: None,
            engine_attribute: None,
            password: None,
            secondary_engine_attribute: None,
            start_transaction: None,
            tablespace_option: None,
            union_tables: None,
            default_charset: None,
            collation: None,
            on_commit: None,
            on_cluster: None,
            primary_key: None,
            order_by: None,
            partition_by: None,
            cluster_by: None,
            clustered_by: None,
            options: None,
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
        }
    }
    pub fn or_replace(mut self, or_replace: bool) -> Self {
        self.or_replace = or_replace;
        self
    }

    pub fn temporary(mut self, temporary: bool) -> Self {
        self.temporary = temporary;
        self
    }

    pub fn external(mut self, external: bool) -> Self {
        self.external = external;
        self
    }

    pub fn global(mut self, global: Option<bool>) -> Self {
        self.global = global;
        self
    }

    pub fn if_not_exists(mut self, if_not_exists: bool) -> Self {
        self.if_not_exists = if_not_exists;
        self
    }

    pub fn transient(mut self, transient: bool) -> Self {
        self.transient = transient;
        self
    }

    pub fn volatile(mut self, volatile: bool) -> Self {
        self.volatile = volatile;
        self
    }

    pub fn iceberg(mut self, iceberg: bool) -> Self {
        self.iceberg = iceberg;
        self
    }

    pub fn columns(mut self, columns: Vec<ColumnDef>) -> Self {
        self.columns = columns;
        self
    }

    pub fn constraints(mut self, constraints: Vec<TableConstraint>) -> Self {
        self.constraints = constraints;
        self
    }

    pub fn hive_distribution(mut self, hive_distribution: HiveDistributionStyle) -> Self {
        self.hive_distribution = hive_distribution;
        self
    }

    pub fn hive_formats(mut self, hive_formats: Option<HiveFormat>) -> Self {
        self.hive_formats = hive_formats;
        self
    }

    pub fn table_properties(mut self, table_properties: Vec<SqlOption>) -> Self {
        self.table_properties = table_properties;
        self
    }

    pub fn with_options(mut self, with_options: Vec<SqlOption>) -> Self {
        self.with_options = with_options;
        self
    }
    pub fn file_format(mut self, file_format: Option<FileFormat>) -> Self {
        self.file_format = file_format;
        self
    }
    pub fn location(mut self, location: Option<String>) -> Self {
        self.location = location;
        self
    }

    pub fn query(mut self, query: Option<Box<Query>>) -> Self {
        self.query = query;
        self
    }
    pub fn without_rowid(mut self, without_rowid: bool) -> Self {
        self.without_rowid = without_rowid;
        self
    }

    pub fn like(mut self, like: Option<ObjectName>) -> Self {
        self.like = like;
        self
    }

    // Different name to allow the object to be cloned
    pub fn clone_clause(mut self, clone: Option<ObjectName>) -> Self {
        self.clone = clone;
        self
    }

    pub fn engine(mut self, engine: Option<TableEngine>) -> Self {
        self.engine = engine;
        self
    }

    pub fn comment(mut self, comment: Option<CommentDef>) -> Self {
        self.comment = comment;
        self
    }

    pub fn auto_increment_offset(mut self, offset: Option<u32>) -> Self {
        self.auto_increment_offset = offset;
        self
    }

    pub fn default_charset(mut self, default_charset: Option<String>) -> Self {
        self.default_charset = default_charset;
        self
    }

    pub fn collation(mut self, collation: Option<String>) -> Self {
        self.collation = collation;
        self
    }
    pub fn compression(mut self, compression: Option<Compression>) -> Self {
        self.compression = compression;
        self
    }
    pub fn encryption(mut self, encryption: Option<Encryption>) -> Self {
        self.encryption = encryption;
        self
    }
    pub fn delay_key_write(mut self, delay_key_write: Option<DelayKeyWrite>) -> Self {
        self.delay_key_write = delay_key_write;
        self
    }
    pub fn insert_method(mut self, insert_method: Option<InsertMethod>) -> Self {
        self.insert_method = insert_method;
        self
    }

    pub fn key_block_size(mut self, key_block_size: Option<u32>) -> Self {
        self.key_block_size = key_block_size;
        self
    }
    pub fn max_rows(mut self, max_rows: Option<u32>) -> Self {
        self.max_rows = max_rows;
        self
    }

    pub fn min_rows(mut self, min_rows: Option<u32>) -> Self {
        self.min_rows = min_rows;
        self
    }

    pub fn autoextend_size(mut self, autoextend_size: Option<u32>) -> Self {
        self.autoextend_size = autoextend_size;
        self
    }
    pub fn avg_row_length(mut self, avg_row_length: Option<u32>) -> Self {
        self.avg_row_length = avg_row_length;
        self
    }

    pub fn checksum(mut self, checksum: Option<bool>) -> Self {
        self.checksum = checksum;
        self
    }
    pub fn connection(mut self, connection: Option<String>) -> Self {
        self.connection = connection;
        self
    }
    pub fn engine_attribute(mut self, engine_attribute: Option<String>) -> Self {
        self.engine_attribute = engine_attribute;
        self
    }
    pub fn password(mut self, password: Option<String>) -> Self {
        self.password = password;
        self
    }
    pub fn secondary_engine_attribute(
        mut self,
        secondary_engine_attribute: Option<String>,
    ) -> Self {
        self.secondary_engine_attribute = secondary_engine_attribute;
        self
    }

    pub fn start_transaction(mut self, start_transaction: Option<bool>) -> Self {
        self.start_transaction = start_transaction;
        self
    }

    pub fn tablespace_option(mut self, tablespace_option: Option<TablespaceOption>) -> Self {
        self.tablespace_option = tablespace_option;
        self
    }
    pub fn union_tables(mut self, union_tables: Option<Vec<String>>) -> Self {
        self.union_tables = union_tables;
        self
    }

    pub fn row_format(mut self, row_format: Option<RowFormat>) -> Self {
        self.row_format = row_format;
        self
    }

    pub fn data_directory(mut self, directory_option: Option<DirectoryOption>) -> Self {
        self.data_directory = directory_option;
        self
    }

    pub fn index_directory(mut self, directory_option: Option<DirectoryOption>) -> Self {
        self.index_directory = directory_option;
        self
    }

    pub fn pack_keys(mut self, pack_keys: Option<OptionState>) -> Self {
        self.pack_keys = pack_keys;
        self
    }

    pub fn stats_auto_recalc(mut self, stats_auto_recalc: Option<OptionState>) -> Self {
        self.stats_auto_recalc = stats_auto_recalc;
        self
    }

    pub fn stats_persistent(mut self, stats_persistent: Option<OptionState>) -> Self {
        self.stats_persistent = stats_persistent;
        self
    }

    pub fn stats_sample_pages(mut self, stats_sample_pages: Option<u32>) -> Self {
        self.stats_sample_pages = stats_sample_pages;
        self
    }

    pub fn on_commit(mut self, on_commit: Option<OnCommit>) -> Self {
        self.on_commit = on_commit;
        self
    }

    pub fn on_cluster(mut self, on_cluster: Option<Ident>) -> Self {
        self.on_cluster = on_cluster;
        self
    }

    pub fn primary_key(mut self, primary_key: Option<Box<Expr>>) -> Self {
        self.primary_key = primary_key;
        self
    }

    pub fn order_by(mut self, order_by: Option<OneOrManyWithParens<Expr>>) -> Self {
        self.order_by = order_by;
        self
    }

    pub fn partition_by(mut self, partition_by: Option<Box<Expr>>) -> Self {
        self.partition_by = partition_by;
        self
    }

    pub fn cluster_by(mut self, cluster_by: Option<WrappedCollection<Vec<Ident>>>) -> Self {
        self.cluster_by = cluster_by;
        self
    }

    pub fn clustered_by(mut self, clustered_by: Option<ClusteredBy>) -> Self {
        self.clustered_by = clustered_by;
        self
    }

    pub fn options(mut self, options: Option<Vec<SqlOption>>) -> Self {
        self.options = options;
        self
    }

    pub fn strict(mut self, strict: bool) -> Self {
        self.strict = strict;
        self
    }

    pub fn copy_grants(mut self, copy_grants: bool) -> Self {
        self.copy_grants = copy_grants;
        self
    }

    pub fn enable_schema_evolution(mut self, enable_schema_evolution: Option<bool>) -> Self {
        self.enable_schema_evolution = enable_schema_evolution;
        self
    }

    pub fn change_tracking(mut self, change_tracking: Option<bool>) -> Self {
        self.change_tracking = change_tracking;
        self
    }

    pub fn data_retention_time_in_days(mut self, data_retention_time_in_days: Option<u64>) -> Self {
        self.data_retention_time_in_days = data_retention_time_in_days;
        self
    }

    pub fn max_data_extension_time_in_days(
        mut self,
        max_data_extension_time_in_days: Option<u64>,
    ) -> Self {
        self.max_data_extension_time_in_days = max_data_extension_time_in_days;
        self
    }

    pub fn default_ddl_collation(mut self, default_ddl_collation: Option<String>) -> Self {
        self.default_ddl_collation = default_ddl_collation;
        self
    }

    pub fn with_aggregation_policy(mut self, with_aggregation_policy: Option<ObjectName>) -> Self {
        self.with_aggregation_policy = with_aggregation_policy;
        self
    }

    pub fn with_row_access_policy(
        mut self,
        with_row_access_policy: Option<RowAccessPolicy>,
    ) -> Self {
        self.with_row_access_policy = with_row_access_policy;
        self
    }

    pub fn with_tags(mut self, with_tags: Option<Vec<Tag>>) -> Self {
        self.with_tags = with_tags;
        self
    }

    pub fn base_location(mut self, base_location: Option<String>) -> Self {
        self.base_location = base_location;
        self
    }

    pub fn external_volume(mut self, external_volume: Option<String>) -> Self {
        self.external_volume = external_volume;
        self
    }

    pub fn catalog(mut self, catalog: Option<String>) -> Self {
        self.catalog = catalog;
        self
    }

    pub fn catalog_sync(mut self, catalog_sync: Option<String>) -> Self {
        self.catalog_sync = catalog_sync;
        self
    }

    pub fn storage_serialization_policy(
        mut self,
        storage_serialization_policy: Option<StorageSerializationPolicy>,
    ) -> Self {
        self.storage_serialization_policy = storage_serialization_policy;
        self
    }

    pub fn build(self) -> Statement {
        Statement::CreateTable(CreateTable {
            or_replace: self.or_replace,
            temporary: self.temporary,
            external: self.external,
            global: self.global,
            if_not_exists: self.if_not_exists,
            transient: self.transient,
            volatile: self.volatile,
            iceberg: self.iceberg,
            name: self.name,
            columns: self.columns,
            constraints: self.constraints,
            hive_distribution: self.hive_distribution,
            hive_formats: self.hive_formats,
            table_properties: self.table_properties,
            with_options: self.with_options,
            file_format: self.file_format,
            location: self.location,
            query: self.query,
            without_rowid: self.without_rowid,
            like: self.like,
            clone: self.clone,
            engine: self.engine,
            comment: self.comment,
            auto_increment_offset: self.auto_increment_offset,
            default_charset: self.default_charset,
            collation: self.collation,
            compression: self.compression,
            encryption: self.encryption,
            insert_method: self.insert_method,
            key_block_size: self.key_block_size,
            row_format: self.row_format,
            data_directory: self.data_directory,
            index_directory: self.index_directory,
            pack_keys: self.pack_keys,
            stats_auto_recalc: self.stats_auto_recalc,
            stats_persistent: self.stats_persistent,
            stats_sample_pages: self.stats_sample_pages,
            delay_key_write: self.delay_key_write,
            max_rows: self.max_rows,
            min_rows: self.min_rows,
            autoextend_size: self.autoextend_size,
            avg_row_length: self.avg_row_length,
            checksum: self.checksum,
            connection: self.connection,
            engine_attribute: self.engine_attribute,
            password: self.password,
            secondary_engine_attribute: self.secondary_engine_attribute,
            start_transaction: self.start_transaction,
            tablespace_option: self.tablespace_option,
            union_tables: self.union_tables,
            on_commit: self.on_commit,
            on_cluster: self.on_cluster,
            primary_key: self.primary_key,
            order_by: self.order_by,
            partition_by: self.partition_by,
            cluster_by: self.cluster_by,
            clustered_by: self.clustered_by,
            options: self.options,
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
        })
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
                name,
                columns,
                constraints,
                hive_distribution,
                hive_formats,
                table_properties,
                with_options,
                file_format,
                location,
                query,
                without_rowid,
                like,
                clone,
                engine,
                comment,
                auto_increment_offset,
                compression,
                encryption,
                insert_method,
                key_block_size,
                row_format,
                data_directory,
                index_directory,
                pack_keys,
                stats_auto_recalc,
                stats_persistent,
                stats_sample_pages,
                delay_key_write,
                max_rows,
                min_rows,
                autoextend_size,
                avg_row_length,
                checksum,
                connection,
                engine_attribute,
                password,
                secondary_engine_attribute,
                start_transaction,
                tablespace_option,
                union_tables,
                default_charset,
                collation,
                on_commit,
                on_cluster,
                primary_key,
                order_by,
                partition_by,
                cluster_by,
                clustered_by,
                options,
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
            }) => Ok(Self {
                or_replace,
                temporary,
                external,
                global,
                if_not_exists,
                transient,
                name,
                columns,
                constraints,
                hive_distribution,
                hive_formats,
                table_properties,
                with_options,
                file_format,
                location,
                query,
                without_rowid,
                like,
                clone,
                engine,
                comment,
                auto_increment_offset,
                compression,
                encryption,
                insert_method,
                key_block_size,
                row_format,
                data_directory,
                index_directory,
                pack_keys,
                stats_auto_recalc,
                stats_persistent,
                stats_sample_pages,
                delay_key_write,
                max_rows,
                min_rows,
                autoextend_size,
                avg_row_length,
                checksum,
                connection,
                engine_attribute,
                password,
                secondary_engine_attribute,
                start_transaction,
                tablespace_option,
                union_tables,
                default_charset,
                collation,
                on_commit,
                on_cluster,
                primary_key,
                order_by,
                partition_by,
                cluster_by,
                clustered_by,
                options,
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
    pub cluster_by: Option<WrappedCollection<Vec<Ident>>>,
    pub options: Option<Vec<SqlOption>>,
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

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
use alloc::{format, string::String, vec::Vec};

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

#[cfg(feature = "visitor")]
use sqlparser_derive::{Visit, VisitMut};

use crate::ast::{
    CatalogSyncNamespaceMode, ContactEntry, ObjectName, Statement, StorageSerializationPolicy, Tag,
};
use crate::parser::ParserError;

/// Builder for create database statement variant ([1]).
///
/// This structure helps building and accessing a create database with more ease, without needing to:
/// - Match the enum itself a lot of times; or
/// - Moving a lot of variables around the code.
///
/// # Example
/// ```rust
/// use sqlparser::ast::helpers::stmt_create_database::CreateDatabaseBuilder;
/// use sqlparser::ast::{ColumnDef, Ident, ObjectName};
/// let builder = CreateDatabaseBuilder::new(ObjectName::from(vec![Ident::new("database_name")]))
///    .if_not_exists(true);
/// // You can access internal elements with ease
/// assert!(builder.if_not_exists);
/// // Convert to a statement
/// assert_eq!(
///    builder.build().to_string(),
///    "CREATE DATABASE IF NOT EXISTS database_name"
/// )
/// ```
///
/// [1]: Statement::CreateDatabase
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct CreateDatabaseBuilder {
    /// The database name to create.
    pub db_name: ObjectName,
    /// Whether `IF NOT EXISTS` was specified.
    pub if_not_exists: bool,
    /// Optional storage location for the database.
    pub location: Option<String>,
    /// Optional managed storage location.
    pub managed_location: Option<String>,
    /// Whether `OR REPLACE` was specified.
    pub or_replace: bool,
    /// Whether the database is `TRANSIENT`.
    pub transient: bool,
    /// Optional `CLONE` source object name.
    pub clone: Option<ObjectName>,
    /// Optional data retention time in days.
    pub data_retention_time_in_days: Option<u64>,
    /// Optional max data extension time in days.
    pub max_data_extension_time_in_days: Option<u64>,
    /// Optional external volume identifier.
    pub external_volume: Option<String>,
    /// Optional catalog name.
    pub catalog: Option<String>,
    /// Whether to replace invalid characters.
    pub replace_invalid_characters: Option<bool>,
    /// Optional default DDL collation.
    pub default_ddl_collation: Option<String>,
    /// Optional storage serialization policy.
    pub storage_serialization_policy: Option<StorageSerializationPolicy>,
    /// Optional comment attached to the database.
    pub comment: Option<String>,
    /// Optional catalog sync configuration.
    pub catalog_sync: Option<String>,
    /// Optional catalog sync namespace mode.
    pub catalog_sync_namespace_mode: Option<CatalogSyncNamespaceMode>,
    /// Optional namespace flatten delimiter for catalog sync.
    pub catalog_sync_namespace_flatten_delimiter: Option<String>,
    /// Optional tags attached to the database.
    pub with_tags: Option<Vec<Tag>>,
    /// Optional contact entries associated with the database.
    pub with_contacts: Option<Vec<ContactEntry>>,
}

impl CreateDatabaseBuilder {
    /// Create a new `CreateDatabaseBuilder` with the given database name.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the database to be created.
    pub fn new(name: ObjectName) -> Self {
        Self {
            db_name: name,
            if_not_exists: false,
            location: None,
            managed_location: None,
            or_replace: false,
            transient: false,
            clone: None,
            data_retention_time_in_days: None,
            max_data_extension_time_in_days: None,
            external_volume: None,
            catalog: None,
            replace_invalid_characters: None,
            default_ddl_collation: None,
            storage_serialization_policy: None,
            comment: None,
            catalog_sync: None,
            catalog_sync_namespace_mode: None,
            catalog_sync_namespace_flatten_delimiter: None,
            with_tags: None,
            with_contacts: None,
        }
    }

    /// Set the location for the database.
    pub fn location(mut self, location: Option<String>) -> Self {
        self.location = location;
        self
    }

    /// Set the managed location for the database.
    pub fn managed_location(mut self, managed_location: Option<String>) -> Self {
        self.managed_location = managed_location;
        self
    }

    /// Set whether this is an `OR REPLACE` operation.
    pub fn or_replace(mut self, or_replace: bool) -> Self {
        self.or_replace = or_replace;
        self
    }

    /// Set whether this is a transient database.
    pub fn transient(mut self, transient: bool) -> Self {
        self.transient = transient;
        self
    }

    /// Set whether to use `IF NOT EXISTS`.
    pub fn if_not_exists(mut self, if_not_exists: bool) -> Self {
        self.if_not_exists = if_not_exists;
        self
    }

    /// Set the clone clause for the database.
    pub fn clone_clause(mut self, clone: Option<ObjectName>) -> Self {
        self.clone = clone;
        self
    }

    /// Set the data retention time in days.
    pub fn data_retention_time_in_days(mut self, data_retention_time_in_days: Option<u64>) -> Self {
        self.data_retention_time_in_days = data_retention_time_in_days;
        self
    }

    /// Set the maximum data extension time in days.
    pub fn max_data_extension_time_in_days(
        mut self,
        max_data_extension_time_in_days: Option<u64>,
    ) -> Self {
        self.max_data_extension_time_in_days = max_data_extension_time_in_days;
        self
    }

    /// Set the external volume for the database.
    pub fn external_volume(mut self, external_volume: Option<String>) -> Self {
        self.external_volume = external_volume;
        self
    }

    /// Set the catalog for the database.
    pub fn catalog(mut self, catalog: Option<String>) -> Self {
        self.catalog = catalog;
        self
    }

    /// Set whether to replace invalid characters.
    pub fn replace_invalid_characters(mut self, replace_invalid_characters: Option<bool>) -> Self {
        self.replace_invalid_characters = replace_invalid_characters;
        self
    }

    /// Set the default DDL collation.
    pub fn default_ddl_collation(mut self, default_ddl_collation: Option<String>) -> Self {
        self.default_ddl_collation = default_ddl_collation;
        self
    }

    /// Set the storage serialization policy.
    pub fn storage_serialization_policy(
        mut self,
        storage_serialization_policy: Option<StorageSerializationPolicy>,
    ) -> Self {
        self.storage_serialization_policy = storage_serialization_policy;
        self
    }

    /// Set the comment for the database.
    pub fn comment(mut self, comment: Option<String>) -> Self {
        self.comment = comment;
        self
    }

    /// Set the catalog sync for the database.
    pub fn catalog_sync(mut self, catalog_sync: Option<String>) -> Self {
        self.catalog_sync = catalog_sync;
        self
    }

    /// Set the catalog sync namespace mode for the database.
    pub fn catalog_sync_namespace_mode(
        mut self,
        catalog_sync_namespace_mode: Option<CatalogSyncNamespaceMode>,
    ) -> Self {
        self.catalog_sync_namespace_mode = catalog_sync_namespace_mode;
        self
    }

    /// Set the catalog sync namespace flatten delimiter for the database.
    pub fn catalog_sync_namespace_flatten_delimiter(
        mut self,
        catalog_sync_namespace_flatten_delimiter: Option<String>,
    ) -> Self {
        self.catalog_sync_namespace_flatten_delimiter = catalog_sync_namespace_flatten_delimiter;
        self
    }

    /// Set the tags for the database.
    pub fn with_tags(mut self, with_tags: Option<Vec<Tag>>) -> Self {
        self.with_tags = with_tags;
        self
    }

    /// Set the contacts for the database.
    pub fn with_contacts(mut self, with_contacts: Option<Vec<ContactEntry>>) -> Self {
        self.with_contacts = with_contacts;
        self
    }

    /// Build the `CREATE DATABASE` statement.
    pub fn build(self) -> Statement {
        Statement::CreateDatabase {
            db_name: self.db_name,
            if_not_exists: self.if_not_exists,
            managed_location: self.managed_location,
            location: self.location,
            or_replace: self.or_replace,
            transient: self.transient,
            clone: self.clone,
            data_retention_time_in_days: self.data_retention_time_in_days,
            max_data_extension_time_in_days: self.max_data_extension_time_in_days,
            external_volume: self.external_volume,
            catalog: self.catalog,
            replace_invalid_characters: self.replace_invalid_characters,
            default_ddl_collation: self.default_ddl_collation,
            storage_serialization_policy: self.storage_serialization_policy,
            comment: self.comment,
            catalog_sync: self.catalog_sync,
            catalog_sync_namespace_mode: self.catalog_sync_namespace_mode,
            catalog_sync_namespace_flatten_delimiter: self.catalog_sync_namespace_flatten_delimiter,
            with_tags: self.with_tags,
            with_contacts: self.with_contacts,
        }
    }
}

impl TryFrom<Statement> for CreateDatabaseBuilder {
    type Error = ParserError;

    fn try_from(stmt: Statement) -> Result<Self, Self::Error> {
        match stmt {
            Statement::CreateDatabase {
                db_name,
                if_not_exists,
                location,
                managed_location,
                or_replace,
                transient,
                clone,
                data_retention_time_in_days,
                max_data_extension_time_in_days,
                external_volume,
                catalog,
                replace_invalid_characters,
                default_ddl_collation,
                storage_serialization_policy,
                comment,
                catalog_sync,
                catalog_sync_namespace_mode,
                catalog_sync_namespace_flatten_delimiter,
                with_tags,
                with_contacts,
            } => Ok(Self {
                db_name,
                if_not_exists,
                location,
                managed_location,
                or_replace,
                transient,
                clone,
                data_retention_time_in_days,
                max_data_extension_time_in_days,
                external_volume,
                catalog,
                replace_invalid_characters,
                default_ddl_collation,
                storage_serialization_policy,
                comment,
                catalog_sync,
                catalog_sync_namespace_mode,
                catalog_sync_namespace_flatten_delimiter,
                with_tags,
                with_contacts,
            }),
            _ => Err(ParserError::ParserError(format!(
                "Expected create database statement, but received: {stmt}"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::ast::helpers::stmt_create_database::CreateDatabaseBuilder;
    use crate::ast::{Ident, ObjectName, Statement};
    use crate::parser::ParserError;

    #[test]
    pub fn test_from_valid_statement() {
        let builder = CreateDatabaseBuilder::new(ObjectName::from(vec![Ident::new("db_name")]));

        let stmt = builder.clone().build();

        assert_eq!(builder, CreateDatabaseBuilder::try_from(stmt).unwrap());
    }

    #[test]
    pub fn test_from_invalid_statement() {
        let stmt = Statement::Commit {
            chain: false,
            end: false,
            modifier: None,
        };

        assert_eq!(
            CreateDatabaseBuilder::try_from(stmt).unwrap_err(),
            ParserError::ParserError(
                "Expected create database statement, but received: COMMIT".to_owned()
            )
        );
    }
}

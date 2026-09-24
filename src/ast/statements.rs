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

//! Named structs for the fields of [`Statement`] variants.

#[cfg(not(feature = "std"))]
use alloc::{boxed::Box, string::String, vec::Vec};

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

#[cfg(feature = "visitor")]
use sqlparser_derive::{Visit, VisitMut};

use super::*;

/// The fields of [`Statement::Install`].
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct InstallStatement {
    /// Only for DuckDB
    pub extension_name: Ident,
}

/// The fields of [`Statement::Load`].
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct LoadStatement {
    /// Only for DuckDB
    pub extension_name: Ident,
}

/// The fields of [`Statement::Directory`].
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct DirectoryStatement {
    /// Whether to overwrite existing files.
    pub overwrite: bool,
    /// Whether the directory is local to the server.
    pub local: bool,
    /// Path to the directory or files.
    pub path: String,
    /// Optional file format for the data.
    pub file_format: Option<FileFormat>,
    /// Source query providing data to load.
    pub source: Box<Query>,
}

/// The fields of [`Statement::Copy`].
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct CopyStatement {
    /// The source of 'COPY TO', or the target of 'COPY FROM'
    pub source: CopySource,
    /// If true, is a 'COPY TO' statement. If false is a 'COPY FROM'
    pub to: bool,
    /// The target of 'COPY TO', or the source of 'COPY FROM'
    pub target: CopyTarget,
    /// WITH options (from PostgreSQL version 9.0)
    pub options: Vec<CopyOption>,
    /// WITH options (before PostgreSQL version 9.0)
    pub legacy_options: Vec<CopyLegacyOption>,
    /// VALUES a vector of values to be copied
    pub values: Vec<Option<String>>,
}

/// The fields of [`Statement::CopyIntoSnowflake`].
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct CopyIntoSnowflakeStatement {
    /// Kind of COPY INTO operation (table or location).
    pub kind: CopyIntoSnowflakeKind,
    /// Target object for the COPY INTO operation.
    pub into: ObjectName,
    /// Optional list of target columns.
    pub into_columns: Option<Vec<Ident>>,
    /// Optional source object name (staged data).
    pub from_obj: Option<ObjectName>,
    /// Optional alias for the source object.
    pub from_obj_alias: Option<Ident>,
    /// Stage-specific parameters (e.g., credentials, path).
    pub stage_params: StageParamsObject,
    /// Optional list of transformations applied when loading.
    pub from_transformations: Option<Vec<StageLoadSelectItemKind>>,
    /// Optional source query instead of a staged object.
    pub from_query: Option<Box<Query>>,
    /// Optional list of specific file names to load.
    pub files: Option<Vec<String>>,
    /// Optional filename matching pattern.
    pub pattern: Option<String>,
    /// File format options.
    pub file_format: KeyValueOptions,
    /// Additional copy options.
    pub copy_options: KeyValueOptions,
    /// Optional validation mode string.
    pub validation_mode: Option<String>,
    /// Optional partition expression for loading.
    pub partition: Option<Box<Expr>>,
}

/// The fields of [`Statement::Close`].
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct CloseStatement {
    /// Cursor name
    pub cursor: CloseCursor,
}

/// The fields of [`Statement::CreateVirtualTable`].
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct CreateVirtualTableStatement {
    #[cfg_attr(feature = "visitor", visit(with = "visit_relation"))]
    /// Name of the virtual table module instance.
    pub name: ObjectName,
    /// `true` when `IF NOT EXISTS` was specified.
    pub if_not_exists: bool,
    /// Module name used by the virtual table.
    pub module_name: Ident,
    /// Arguments passed to the module.
    pub module_args: Vec<Ident>,
}

/// The fields of [`Statement::CreateSecret`].
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct CreateSecretStatement {
    /// `true` when `OR REPLACE` was specified.
    pub or_replace: bool,
    /// Optional `TEMPORARY` flag.
    pub temporary: Option<bool>,
    /// `true` when `IF NOT EXISTS` was present.
    pub if_not_exists: bool,
    /// Optional secret name.
    pub name: Option<Ident>,
    /// Optional storage specifier identifier.
    pub storage_specifier: Option<Ident>,
    /// The secret type identifier.
    pub secret_type: Ident,
    /// Additional secret options.
    pub options: Vec<SecretOption>,
}

/// The fields of [`Statement::AlterIndex`].
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct AlterIndexStatement {
    /// Name of the index to alter.
    pub name: ObjectName,
    /// The operation to perform on the index.
    pub operation: AlterIndexOperation,
}

/// The fields of [`Statement::AlterView`].
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct AlterViewStatement {
    /// View name being altered.
    #[cfg_attr(feature = "visitor", visit(with = "visit_relation"))]
    pub name: ObjectName,
    /// Optional new column list for the view.
    pub columns: Vec<Ident>,
    /// Replacement query for the view definition.
    pub query: Box<Query>,
    /// Additional WITH options for the view.
    pub with_options: Vec<SqlOption>,
}

/// The fields of [`Statement::AlterRole`].
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct AlterRoleStatement {
    /// Role name being altered.
    pub name: Ident,
    /// Operation to perform on the role.
    pub operation: AlterRoleOperation,
}

/// The fields of [`Statement::AlterConnector`].
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct AlterConnectorStatement {
    /// Name of the connector to alter.
    pub name: Ident,
    /// Optional connector properties to set.
    pub properties: Option<Vec<SqlOption>>,
    /// Optional new URL for the connector.
    pub url: Option<String>,
    /// Optional new owner specification.
    pub owner: Option<ddl::AlterConnectorOwner>,
}

/// The fields of [`Statement::AlterSession`].
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct AlterSessionStatement {
    /// true is to set for the session parameters, false is to unset
    pub set: bool,
    /// The session parameters to set or unset
    pub session_params: KeyValueOptions,
}

/// The fields of [`Statement::AttachDatabase`].
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct AttachDatabaseStatement {
    /// The name to bind to the newly attached database
    pub schema_name: Ident,
    /// An expression that indicates the path to the database file
    pub database_file_name: Expr,
    /// true if the syntax is 'ATTACH DATABASE', false if it's just 'ATTACH'
    pub database: bool,
}

/// The fields of [`Statement::AttachDuckDBDatabase`].
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct AttachDuckDBDatabaseStatement {
    /// `true` when `IF NOT EXISTS` was present.
    pub if_not_exists: bool,
    /// `true` if the syntax used `ATTACH DATABASE` rather than `ATTACH`.
    pub database: bool,
    /// The path identifier to the database file being attached.
    pub database_path: Ident,
    /// Optional alias assigned to the attached database.
    pub database_alias: Option<Ident>,
    /// Dialect-specific attach options (e.g., `READ_ONLY`).
    pub attach_options: Vec<AttachDuckDBDatabaseOption>,
}

/// The fields of [`Statement::DetachDuckDBDatabase`].
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct DetachDuckDBDatabaseStatement {
    /// `true` when `IF EXISTS` was present.
    pub if_exists: bool,
    /// `true` if the syntax used `DETACH DATABASE` rather than `DETACH`.
    pub database: bool,
    /// Alias of the database to detach.
    pub database_alias: Ident,
}

/// The fields of [`Statement::Drop`].
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct DropStatement {
    /// The type of the object to drop: TABLE, VIEW, etc.
    pub object_type: ObjectType,
    /// An optional `IF EXISTS` clause. (Non-standard.)
    pub if_exists: bool,
    /// One or more objects to drop. (ANSI SQL requires exactly one.)
    pub names: Vec<ObjectName>,
    /// Whether `CASCADE` was specified. This will be `false` when
    /// `RESTRICT` or no drop behavior at all was specified.
    pub cascade: bool,
    /// Whether `RESTRICT` was specified. This will be `false` when
    /// `CASCADE` or no drop behavior at all was specified.
    pub restrict: bool,
    /// Hive allows you specify whether the table's stored data will be
    /// deleted along with the dropped table
    pub purge: bool,
    /// MySQL-specific "TEMPORARY" keyword
    pub temporary: bool,
    /// MySQL-specific drop index syntax, which requires table specification
    /// See <https://dev.mysql.com/doc/refman/8.4/en/drop-index.html>
    pub table: Option<ObjectName>,
}

/// The fields of [`Statement::DropProcedure`].
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct DropProcedureStatement {
    /// `true` when `IF EXISTS` was present.
    pub if_exists: bool,
    /// One or more functions/procedures to drop.
    pub proc_desc: Vec<FunctionDesc>,
    /// Optional drop behavior (`CASCADE` or `RESTRICT`).
    pub drop_behavior: Option<DropBehavior>,
}

/// The fields of [`Statement::DropSecret`].
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct DropSecretStatement {
    /// `true` when `IF EXISTS` was present.
    pub if_exists: bool,
    /// Optional `TEMPORARY` marker.
    pub temporary: Option<bool>,
    /// Name of the secret to drop.
    pub name: Ident,
    /// Optional storage specifier identifier.
    pub storage_specifier: Option<Ident>,
}

/// The fields of [`Statement::DropConnector`].
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct DropConnectorStatement {
    /// `true` when `IF EXISTS` was present.
    pub if_exists: bool,
    /// Name of the connector to drop.
    pub name: Ident,
}

/// The fields of [`Statement::Declare`].
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct DeclareStatement {
    /// Cursor declaration statements collected by `DECLARE`.
    pub stmts: Vec<Declare>,
}

/// The fields of [`Statement::Fetch`].
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct FetchStatement {
    /// Cursor name
    pub name: Ident,
    /// The fetch direction (e.g., `FORWARD`, `BACKWARD`).
    pub direction: FetchDirection,
    /// The fetch position (e.g., `ALL`, `NEXT`, `ABSOLUTE`).
    pub position: FetchPosition,
    /// Optional target table to fetch rows into.
    pub into: Option<ObjectName>,
}

/// The fields of [`Statement::Flush`].
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct FlushStatement {
    /// The specific flush option or object to flush.
    pub object_type: FlushType,
    /// Optional flush location (dialect-specific).
    pub location: Option<FlushLocation>,
    /// Optional channel name used for flush operations.
    pub channel: Option<String>,
    /// Whether a read lock was requested.
    pub read_lock: bool,
    /// Whether this is an export flush operation.
    pub export: bool,
    /// Optional list of tables involved in the flush.
    pub tables: Vec<ObjectName>,
}

/// The fields of [`Statement::Discard`].
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct DiscardStatement {
    /// The kind of object(s) to discard (ALL, PLANS, etc.).
    pub object_type: DiscardObject,
}

/// The fields of [`Statement::ShowFunctions`].
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct ShowFunctionsStatement {
    /// Optional filter for which functions to display.
    pub filter: Option<ShowStatementFilter>,
}

/// The fields of [`Statement::ShowVariable`].
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct ShowVariableStatement {
    /// Variable name as one or more identifiers.
    pub variable: Vec<Ident>,
}

/// The fields of [`Statement::ShowStatus`].
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct ShowStatusStatement {
    /// Optional filter for which status entries to display.
    pub filter: Option<ShowStatementFilter>,
    /// `true` when `GLOBAL` scope was requested.
    pub global: bool,
    /// `true` when `SESSION` scope was requested.
    pub session: bool,
}

/// The fields of [`Statement::ShowVariables`].
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct ShowVariablesStatement {
    /// Optional filter for which variables to display.
    pub filter: Option<ShowStatementFilter>,
    /// `true` when `GLOBAL` scope was requested.
    pub global: bool,
    /// `true` when `SESSION` scope was requested.
    pub session: bool,
}

/// The fields of [`Statement::ShowCreate`].
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct ShowCreateStatement {
    /// The kind of object being shown (TABLE, VIEW, etc.).
    pub obj_type: ShowCreateObject,
    /// The name of the object to show create statement for.
    pub obj_name: ObjectName,
}

/// The fields of [`Statement::ShowColumns`].
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct ShowColumnsStatement {
    /// `true` when extended column information was requested.
    pub extended: bool,
    /// `true` when full column details were requested.
    pub full: bool,
    /// Additional options for `SHOW COLUMNS`.
    pub show_options: ShowStatementOptions,
}

/// The fields of [`Statement::ShowCatalogs`].
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct ShowCatalogsStatement {
    /// `true` when terse output format was requested.
    pub terse: bool,
    /// `true` when history information was requested.
    pub history: bool,
    /// Additional options for `SHOW CATALOGS`.
    pub show_options: ShowStatementOptions,
}

/// The fields of [`Statement::ShowDatabases`].
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct ShowDatabasesStatement {
    /// `true` when terse output format was requested.
    pub terse: bool,
    /// `true` when history information was requested.
    pub history: bool,
    /// Additional options for `SHOW DATABASES`.
    pub show_options: ShowStatementOptions,
}

/// The fields of [`Statement::ShowProcessList`].
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct ShowProcessListStatement {
    /// `true` when full process information was requested.
    pub full: bool,
}

/// The fields of [`Statement::ShowSchemas`].
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct ShowSchemasStatement {
    /// `true` when terse (compact) output was requested.
    pub terse: bool,
    /// `true` when history information was requested.
    pub history: bool,
    /// Additional options for `SHOW SCHEMAS`.
    pub show_options: ShowStatementOptions,
}

/// The fields of [`Statement::ShowTables`].
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct ShowTablesStatement {
    /// `true` when terse output format was requested (compact listing).
    pub terse: bool,
    /// `true` when history rows are requested.
    pub history: bool,
    /// `true` when extended information should be shown.
    pub extended: bool,
    /// `true` when a full listing was requested.
    pub full: bool,
    /// `true` when external tables should be included.
    pub external: bool,
    /// Additional options for `SHOW` statements.
    pub show_options: ShowStatementOptions,
}

/// The fields of [`Statement::ShowViews`].
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct ShowViewsStatement {
    /// `true` when terse output format was requested.
    pub terse: bool,
    /// `true` when materialized views should be included.
    pub materialized: bool,
    /// Additional options for `SHOW` statements.
    pub show_options: ShowStatementOptions,
}

/// The fields of [`Statement::ShowCollation`].
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct ShowCollationStatement {
    /// Optional filter for which collations to display.
    pub filter: Option<ShowStatementFilter>,
}

/// The fields of [`Statement::StartTransaction`].
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct StartTransactionStatement {
    /// Transaction modes such as `ISOLATION LEVEL` or `READ WRITE`.
    pub modes: Vec<TransactionMode>,
    /// `true` when this was parsed as `BEGIN` instead of `START`.
    pub begin: bool,
    /// Optional specific keyword used: `TRANSACTION` or `WORK`.
    pub transaction: Option<BeginTransactionKind>,
    /// Optional transaction modifier (e.g., `AND NO CHAIN`).
    pub modifier: Option<TransactionModifier>,
    /// List of statements belonging to the `BEGIN` block.
    /// Example:
    /// ```sql
    /// BEGIN
    ///     SELECT 1;
    ///     SELECT 2;
    /// END;
    /// ```
    pub statements: Vec<Statement>,
    /// Exception handling with exception clauses.
    /// Example:
    /// ```sql
    /// EXCEPTION
    ///     WHEN EXCEPTION_1 THEN
    ///         SELECT 2;
    ///     WHEN EXCEPTION_2 OR EXCEPTION_3 THEN
    ///         SELECT 3;
    ///     WHEN OTHER THEN
    ///         SELECT 4;
    /// ```
    /// <https://cloud.google.com/bigquery/docs/reference/standard-sql/procedural-language#beginexceptionend>
    /// <https://docs.snowflake.com/en/sql-reference/snowflake-scripting/exception>
    pub exception: Option<Vec<ExceptionWhen>>,
    /// TRUE if the statement has an `END` keyword.
    pub has_end_keyword: bool,
}

/// The fields of [`Statement::Comment`].
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct CommentStatement {
    /// Type of object being commented (table, column, etc.).
    pub object_type: CommentObject,
    /// Name of the object the comment applies to.
    pub object_name: ObjectName,
    /// Optional comment text (None to remove comment).
    pub comment: Option<String>,
    /// An optional `IF EXISTS` clause. (Non-standard.)
    /// See <https://docs.snowflake.com/en/sql-reference/sql/comment>
    pub if_exists: bool,
}

/// The fields of [`Statement::Commit`].
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct CommitStatement {
    /// `true` when `AND [ NO ] CHAIN` was present.
    pub chain: bool,
    /// `true` when this `COMMIT` was parsed as an `END` block terminator.
    pub end: bool,
    /// Optional transaction modifier for commit semantics.
    pub modifier: Option<TransactionModifier>,
}

/// The fields of [`Statement::Rollback`].
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct RollbackStatement {
    /// `true` when `AND [ NO ] CHAIN` was present.
    pub chain: bool,
    /// Optional savepoint name to roll back to.
    pub savepoint: Option<Ident>,
}

/// The fields of [`Statement::CreateSchema`].
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct CreateSchemaStatement {
    /// `<schema name> | AUTHORIZATION <schema authorization identifier>  | <schema name>  AUTHORIZATION <schema authorization identifier>`
    pub schema_name: SchemaName,
    /// `true` when `OR REPLACE` was present.
    pub or_replace: bool,
    /// `true` when `IF NOT EXISTS` was present.
    pub if_not_exists: bool,
    /// Schema properties.
    ///
    /// ```sql
    /// CREATE SCHEMA myschema WITH (key1='value1');
    /// ```
    ///
    /// [Trino](https://trino.io/docs/current/sql/create-schema.html)
    pub with: Option<Vec<SqlOption>>,
    /// Schema options.
    ///
    /// ```sql
    /// CREATE SCHEMA myschema OPTIONS(key1='value1');
    /// ```
    ///
    /// [BigQuery](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#create_schema_statement)
    pub options: Option<Vec<SqlOption>>,
    /// Default collation specification for the schema.
    ///
    /// ```sql
    /// CREATE SCHEMA myschema DEFAULT COLLATE 'und:ci';
    /// ```
    ///
    /// [BigQuery](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#create_schema_statement)
    pub default_collate_spec: Option<Expr>,
    /// Clones a schema
    ///
    /// ```sql
    /// CREATE SCHEMA myschema CLONE otherschema
    /// ```
    ///
    /// [Snowflake](https://docs.snowflake.com/en/sql-reference/sql/create-clone#databases-schemas)
    pub clone: Option<ObjectName>,
}

/// The fields of [`Statement::CreateDatabase`].
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct CreateDatabaseStatement {
    /// Database name.
    pub db_name: ObjectName,
    /// `IF NOT EXISTS` flag.
    pub if_not_exists: bool,
    /// Optional location URI.
    pub location: Option<String>,
    /// Optional managed location.
    pub managed_location: Option<String>,
    /// `OR REPLACE` flag.
    pub or_replace: bool,
    /// `TRANSIENT` flag.
    pub transient: bool,
    /// Optional clone source.
    pub clone: Option<ObjectName>,
    /// Optional data retention time in days.
    pub data_retention_time_in_days: Option<u64>,
    /// Optional maximum data extension time in days.
    pub max_data_extension_time_in_days: Option<u64>,
    /// Optional external volume identifier.
    pub external_volume: Option<String>,
    /// Optional catalog name.
    pub catalog: Option<String>,
    /// Whether to replace invalid characters.
    pub replace_invalid_characters: Option<bool>,
    /// Default DDL collation string.
    pub default_ddl_collation: Option<String>,
    /// Storage serialization policy.
    pub storage_serialization_policy: Option<StorageSerializationPolicy>,
    /// Optional comment.
    pub comment: Option<String>,
    /// Optional default character set (MySQL).
    pub default_charset: Option<String>,
    /// Optional default collation (MySQL).
    pub default_collation: Option<String>,
    /// Optional catalog sync identifier.
    pub catalog_sync: Option<String>,
    /// Catalog sync namespace mode.
    pub catalog_sync_namespace_mode: Option<CatalogSyncNamespaceMode>,
    /// Optional flatten delimiter for namespace sync.
    pub catalog_sync_namespace_flatten_delimiter: Option<String>,
    /// Optional tags for the database.
    pub with_tags: Option<Vec<Tag>>,
    /// Optional contact entries for the database.
    pub with_contacts: Option<Vec<ContactEntry>>,
}

/// The fields of [`Statement::CreateProcedure`].
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct CreateProcedureStatement {
    /// `OR ALTER` flag.
    pub or_alter: bool,
    /// Procedure name.
    pub name: ObjectName,
    /// Optional procedure parameters.
    pub params: Option<Vec<ProcedureParam>>,
    /// Optional language identifier.
    pub language: Option<Ident>,
    /// Procedure body statements.
    pub body: ConditionalStatements,
}

/// The fields of [`Statement::CreateMacro`].
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct CreateMacroStatement {
    /// `OR REPLACE` flag.
    pub or_replace: bool,
    /// Whether macro is temporary.
    pub temporary: bool,
    /// Macro name.
    pub name: ObjectName,
    /// Optional macro arguments.
    pub args: Option<Vec<MacroArg>>,
    /// Macro definition body.
    pub definition: MacroDefinition,
}

/// The fields of [`Statement::CreateStage`].
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct CreateStageStatement {
    /// `OR REPLACE` flag for stage.
    pub or_replace: bool,
    /// Whether stage is temporary.
    pub temporary: bool,
    /// `IF NOT EXISTS` flag.
    pub if_not_exists: bool,
    /// Stage name.
    pub name: ObjectName,
    /// Stage parameters.
    pub stage_params: StageParamsObject,
    /// Directory table parameters.
    pub directory_table_params: KeyValueOptions,
    /// File format options.
    pub file_format: KeyValueOptions,
    /// Copy options for stage.
    pub copy_options: KeyValueOptions,
    /// Optional comment.
    pub comment: Option<String>,
}

/// The fields of [`Statement::CreateFileFormat`].
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct CreateFileFormatStatement {
    /// `OR REPLACE` flag.
    pub or_replace: bool,
    /// Whether file format is temporary.
    pub temporary: bool,
    /// Whether file format is volatile.
    pub volatile: bool,
    /// `IF NOT EXISTS` flag.
    pub if_not_exists: bool,
    /// File format name.
    pub name: ObjectName,
    /// Format type options (e.g. `TYPE`, `FIELD_DELIMITER`, `COMPRESSION`, ...).
    pub options: KeyValueOptions,
    /// Optional comment.
    pub comment: Option<String>,
}

/// The fields of [`Statement::Assert`].
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct AssertStatement {
    /// Assertion condition expression.
    pub condition: Expr,
    /// Optional message expression.
    pub message: Option<Expr>,
}

/// The fields of [`Statement::Deallocate`].
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct DeallocateStatement {
    /// Name to deallocate (or `ALL`).
    pub name: Ident,
    /// Whether `PREPARE` keyword was present.
    pub prepare: bool,
}

/// The fields of [`Statement::Execute`].
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct ExecuteStatement {
    /// Optional function/procedure name.
    pub name: Option<ObjectName>,
    /// Parameter expressions passed to execute.
    pub parameters: Vec<Expr>,
    /// Whether parentheses were present around `parameters`.
    pub has_parentheses: bool,
    /// Is this an `EXECUTE IMMEDIATE`.
    pub immediate: bool,
    /// Identifiers to capture results into.
    pub into: Vec<Ident>,
    /// `USING` expressions with optional aliases.
    pub using: Vec<ExprWithAlias>,
    /// Whether the last parameter is the return value of the procedure
    /// MSSQL: <https://learn.microsoft.com/en-us/sql/t-sql/language-elements/execute-transact-sql?view=sql-server-ver17#output>
    pub output: bool,
    /// Whether to invoke the procedure with the default parameter values
    /// MSSQL: <https://learn.microsoft.com/en-us/sql/t-sql/language-elements/execute-transact-sql?view=sql-server-ver17#default>
    pub default: bool,
}

/// The fields of [`Statement::Prepare`].
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct PrepareStatement {
    /// Name of the prepared statement.
    pub name: Ident,
    /// Optional data types for parameters.
    pub data_types: Vec<DataType>,
    /// Statement being prepared.
    pub statement: Box<Statement>,
}

/// The fields of [`Statement::Kill`].
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct KillStatement {
    /// Optional kill modifier (CONNECTION, QUERY, MUTATION).
    pub modifier: Option<KillType>,
    // processlist_id
    /// The id of the process to kill.
    pub id: u64,
}

/// The fields of [`Statement::ExplainTable`].
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct ExplainTableStatement {
    /// `EXPLAIN | DESC | DESCRIBE`
    pub describe_alias: DescribeAlias,
    /// Hive style `FORMATTED | EXTENDED`
    pub hive_format: Option<HiveDescribeFormat>,
    /// Snowflake and ClickHouse support `DESC|DESCRIBE TABLE <table_name>` syntax
    ///
    /// [Snowflake](https://docs.snowflake.com/en/sql-reference/sql/desc-table.html)
    /// [ClickHouse](https://clickhouse.com/docs/en/sql-reference/statements/describe-table)
    pub has_table_keyword: bool,
    /// Table name
    #[cfg_attr(feature = "visitor", visit(with = "visit_relation"))]
    pub table_name: ObjectName,
}

/// The fields of [`Statement::Explain`].
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct ExplainStatement {
    /// `EXPLAIN | DESC | DESCRIBE`
    pub describe_alias: DescribeAlias,
    /// Carry out the command and show actual run times and other statistics.
    pub analyze: bool,
    /// Display additional information regarding the plan.
    pub verbose: bool,
    /// `EXPLAIN QUERY PLAN`
    /// Display the query plan without running the query.
    ///
    /// [SQLite](https://sqlite.org/lang_explain.html)
    pub query_plan: bool,
    /// `EXPLAIN ESTIMATE`
    /// [Clickhouse](https://clickhouse.com/docs/en/sql-reference/statements/explain#explain-estimate)
    pub estimate: bool,
    /// A SQL query that specifies what to explain
    pub statement: Box<Statement>,
    /// Optional output format of explain
    pub format: Option<AnalyzeFormatKind>,
    /// Postgres style utility options, `(analyze, verbose true)`
    pub options: Option<Vec<UtilityOption>>,
}

/// The fields of [`Statement::Savepoint`].
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct SavepointStatement {
    /// Name of the savepoint being defined.
    pub name: Ident,
}

/// The fields of [`Statement::ReleaseSavepoint`].
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct ReleaseSavepointStatement {
    /// Name of the savepoint to release.
    pub name: Ident,
}

/// The fields of [`Statement::Cache`].
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct CacheStatement {
    /// Table flag
    pub table_flag: Option<ObjectName>,
    /// Table name
    #[cfg_attr(feature = "visitor", visit(with = "visit_relation"))]
    pub table_name: ObjectName,
    /// `true` if `AS` keyword was present before the query.
    pub has_as: bool,
    /// Table confs
    pub options: Vec<SqlOption>,
    /// Cache table as a Query
    pub query: Option<Box<Query>>,
}

/// The fields of [`Statement::UNCache`].
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct UNCacheStatement {
    /// Table name
    #[cfg_attr(feature = "visitor", visit(with = "visit_relation"))]
    pub table_name: ObjectName,
    /// `true` when `IF EXISTS` was present.
    pub if_exists: bool,
}

/// The fields of [`Statement::CreateSequence`].
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct CreateSequenceStatement {
    /// Whether the sequence is temporary.
    pub temporary: bool,
    /// `IF NOT EXISTS` flag.
    pub if_not_exists: bool,
    /// Sequence name.
    pub name: ObjectName,
    /// Optional data type for the sequence.
    pub data_type: Option<DataType>,
    /// Sequence options (INCREMENT, MINVALUE, etc.).
    pub sequence_options: Vec<SequenceOptions>,
    /// Optional `OWNED BY` target.
    pub owned_by: Option<ObjectName>,
}

/// The fields of [`Statement::CreateType`].
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct CreateTypeStatement {
    /// Type name to create.
    pub name: ObjectName,
    /// Optional type representation details.
    pub representation: Option<UserDefinedTypeRepresentation>,
}

/// The fields of [`Statement::Pragma`].
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct PragmaStatement {
    /// Pragma name (possibly qualified).
    pub name: ObjectName,
    /// Optional pragma value.
    pub value: Option<ValueWithSpan>,
    /// Whether the pragma used `=`.
    pub is_eq: bool,
}

/// The fields of [`Statement::LockTables`].
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct LockTablesStatement {
    /// List of tables to lock with modes.
    pub tables: Vec<LockTable>,
}

/// The fields of [`Statement::UnlockTables`].
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct UnlockTablesStatement;

/// The fields of [`Statement::Unload`].
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct UnloadStatement {
    /// Optional query AST to unload.
    pub query: Option<Box<Query>>,
    /// Optional original query text.
    pub query_text: Option<String>,
    /// Destination identifier.
    pub to: Ident,
    /// Optional IAM role/auth information.
    pub auth: Option<IamRoleKind>,
    /// Additional `WITH` options.
    pub with: Vec<SqlOption>,
    /// Legacy copy-style options.
    pub options: Vec<CopyLegacyOption>,
}

/// The fields of [`Statement::OptimizeTable`].
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct OptimizeTableStatement {
    /// Table name to optimize.
    pub name: ObjectName,
    /// Whether the `TABLE` keyword was present (ClickHouse uses `OPTIMIZE TABLE`, Databricks uses `OPTIMIZE`).
    pub has_table_keyword: bool,
    /// Optional cluster identifier.
    /// [ClickHouse](https://clickhouse.com/docs/en/sql-reference/statements/optimize)
    pub on_cluster: Option<Ident>,
    /// Optional partition spec.
    /// [ClickHouse](https://clickhouse.com/docs/en/sql-reference/statements/optimize)
    pub partition: Option<Partition>,
    /// Whether `FINAL` was specified.
    /// [ClickHouse](https://clickhouse.com/docs/en/sql-reference/statements/optimize)
    pub include_final: bool,
    /// Optional deduplication settings.
    /// [ClickHouse](https://clickhouse.com/docs/en/sql-reference/statements/optimize)
    pub deduplicate: Option<Deduplicate>,
    /// Optional WHERE predicate.
    /// [Databricks](https://docs.databricks.com/en/sql/language-manual/delta-optimize.html)
    pub predicate: Option<Expr>,
    /// Optional ZORDER BY columns.
    /// [Databricks](https://docs.databricks.com/en/sql/language-manual/delta-optimize.html)
    pub zorder: Option<Vec<Expr>>,
}

/// The fields of [`Statement::LISTEN`].
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct LISTENStatement {
    /// Notification channel identifier.
    pub channel: Ident,
}

/// The fields of [`Statement::UNLISTEN`].
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct UNLISTENStatement {
    /// Notification channel identifier.
    pub channel: Ident,
}

/// The fields of [`Statement::NOTIFY`].
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct NOTIFYStatement {
    /// Notification channel identifier.
    pub channel: Ident,
    /// Optional payload string.
    pub payload: Option<String>,
}

/// The fields of [`Statement::LoadData`].
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct LoadDataStatement {
    /// Whether `LOCAL` is present.
    pub local: bool,
    /// Input path for files to load.
    pub inpath: String,
    /// Whether `OVERWRITE` was specified.
    pub overwrite: bool,
    /// Target table name to load into.
    pub table_name: ObjectName,
    /// Optional partition specification.
    pub partitioned: Option<Vec<Expr>>,
    /// Optional table format information.
    pub table_format: Option<HiveLoadDataFormat>,
}

/// The fields of [`Statement::Put`].
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct PutStatement {
    /// Local source URI as written in the statement, e.g. `file:///tmp/data.csv`.
    pub source: String,
    /// Target internal stage (e.g. `@mystage`, `@~`, `@%table`).
    pub stage: ObjectName,
    /// Trailing options (`PARALLEL=4`, `AUTO_COMPRESS=TRUE`, ...).
    pub options: KeyValueOptions,
}

/// The fields of [`Statement::RaisError`].
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct RaisErrorStatement {
    /// Error message expression or identifier.
    pub message: Box<Expr>,
    /// Severity expression.
    pub severity: Box<Expr>,
    /// State expression.
    pub state: Box<Expr>,
    /// Substitution arguments for the message.
    pub arguments: Vec<Expr>,
    /// Additional `WITH` options for RAISERROR.
    pub options: Vec<RaisErrorOption>,
}

mod display;

use super::*;
/// A top-level statement (SELECT, INSERT, CREATE, etc.)

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum Statement {
    /// Analyze (Hive)
    Analyze(Analyze),
    /// Truncate (Hive)
    Truncate(Truncate),
    /// Msck (Hive)
    Msck(Msck),
    /// SELECT
    Query(Box<Query>),
    /// INSERT
    Insert(Insert),
    // TODO: Support ROW FORMAT
    Directory(Directory),
    Copy(Copy),
    /// UPDATE
    Update(Update),
    /// DELETE
    Delete(Delete),
    /// CREATE VIEW
    CreateView(CreateView),
    /// CREATE TABLE
    CreateTable(Box<CreateTable>),
    /// SQLite's `CREATE VIRTUAL TABLE .. USING <module_name> (<module_args>)`
    CreateVirtualTable(CreateVirtualTable),
    /// CREATE INDEX
    CreateIndex(CreateIndex),
    /// ALTER TABLE
    AlterTable(AlterTable),
    /// DROP
    Drop(Drop),
    /// SET <variable>
    ///
    /// Note: this is not a standard SQL statement, but it is supported by at
    /// least MySQL and PostgreSQL. Not all MySQL-specific syntatic forms are
    /// supported yet.
    SetVariable(SetVariable),
    /// SHOW <variable>
    ///
    /// Note: this is a PostgreSQL-specific statement.
    ShowVariable(ShowVariable),
    /// SHOW CREATE TABLE
    ///
    /// Note: this is a MySQL-specific statement.
    ShowCreate(ShowCreate),
    /// SHOW COLUMNS
    ///
    /// Note: this is a MySQL-specific statement.
    ShowColumns(ShowColumns),
    /// `{ BEGIN [ TRANSACTION | WORK ] | START TRANSACTION } ...`
    StartTransaction(StartTransaction),
    /// `SET TRANSACTION ...`
    SetTransaction(SetTransaction),
    /// `COMMIT [ TRANSACTION | WORK ] [ AND [ NO ] CHAIN ]`
    Commit(Commit),
    /// `ROLLBACK [ TRANSACTION | WORK ] [ AND [ NO ] CHAIN ]`
    Rollback(Rollback),
    /// CREATE SCHEMA
    CreateSchema(CreateSchema),
    /// CREATE DATABASE
    CreateDatabase(CreateDatabase),
    /// `ASSERT <condition> [AS <message>]`
    Assert(Assert),
    /// `DEALLOCATE [ PREPARE ] { name | ALL }`
    ///
    /// Note: this is a PostgreSQL-specific statement.
    Deallocate(Deallocate),
    /// `EXECUTE name [ ( parameter [, ...] ) ]`
    ///
    /// Note: this is a PostgreSQL-specific statement.
    Execute(Execute),
    /// `PREPARE name [ ( data_type [, ...] ) ] AS statement`
    ///
    /// Note: this is a PostgreSQL-specific statement.
    Prepare(Prepare),
    /// EXPLAIN
    Explain(Explain),
}
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Analyze {
    pub table_name: ObjectName,
    pub partitions: Option<Vec<Expr>>,
    pub for_columns: bool,
    pub columns: Vec<Ident>,
    pub cache_metadata: bool,
    pub noscan: bool,
    pub compute_statistics: bool,
}
/// Truncate (Hive)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Truncate {
    pub table_name: ObjectName,
    pub partitions: Option<Vec<Expr>>,
}
/// Msck (Hive)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Msck {
    pub table_name: ObjectName,
    pub repair: bool,
    pub partition_action: Option<AddDropSync>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Insert {
    /// Only for Sqlite
    pub or: Option<SqliteOnConflict>,
    /// TABLE
    pub table_name: ObjectName,
    /// COLUMNS
    pub columns: Vec<Ident>,
    /// Overwrite (Hive)
    pub overwrite: bool,
    /// A SQL query that specifies what to insert
    pub source: Box<Query>,
    /// partitioned insert (Hive)
    pub partitioned: Option<Vec<Expr>>,
    /// Columns defined after PARTITION
    pub after_columns: Vec<Ident>,
    /// whether the insert has the table keyword (Hive)
    pub table: bool,
}
// TODO: Support ROW FORMAT
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Directory {
    pub overwrite: bool,
    pub local: bool,
    pub path: String,
    pub file_format: Option<FileFormat>,
    pub source: Box<Query>,
}
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Copy {
    /// TABLE
    pub table_name: ObjectName,
    /// COLUMNS
    pub columns: Vec<Ident>,
    /// VALUES a vector of values to be copied
    pub values: Vec<Option<String>>,
}
/// UPDATE
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Update {
    /// TABLE
    pub table_name: ObjectName,
    /// Column assignments
    pub assignments: Vec<Assignment>,
    /// WHERE
    pub selection: Option<Expr>,
}
/// DELETE
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Delete {
    /// FROM
    pub table_name: ObjectName,
    /// WHERE
    pub selection: Option<Expr>,
}
/// CREATE VIEW
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct CreateView {
    pub or_replace: bool,
    pub materialized: bool,
    /// View name
    pub name: ObjectName,
    pub columns: Vec<Ident>,
    pub query: Box<Query>,
    pub with_options: Vec<SqlOption>,
}
/// CREATE TABLE

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct CreateTable {
    pub or_replace: bool,
    pub temporary: bool,
    pub external: bool,
    pub if_not_exists: bool,
    /// Table name
    pub name: ObjectName,
    /// Optional schema
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
}
/// SQLite's `CREATE VIRTUAL TABLE .. USING <module_name> (<module_args>)`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct CreateVirtualTable {
    pub name: ObjectName,
    pub if_not_exists: bool,
    pub module_name: Ident,
    pub module_args: Vec<Ident>,
}
/// CREATE INDEX
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct CreateIndex {
    /// index name
    pub name: ObjectName,
    pub table_name: ObjectName,
    pub columns: Vec<OrderByExpr>,
    pub unique: bool,
    pub if_not_exists: bool,
}
/// ALTER TABLE
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct AlterTable {
    /// Table name
    pub name: ObjectName,
    pub operation: AlterTableOperation,
}
/// DROP
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Drop {
    /// The type of the object to drop: TABLE, VIEW, etc.
    pub object_type: ObjectType,
    /// An optional `IF EXISTS` clause. (Non-standard.)
    pub if_exists: bool,
    /// One or more objects to drop. (ANSI SQL requires exactly one.)
    pub names: Vec<ObjectName>,
    /// Whether `CASCADE` was specified. This will be `false` when
    /// `RESTRICT` or no drop behavior at all was specified.
    pub cascade: bool,
    /// Hive allows you specify whether the table's stored data will be
    /// deleted along with the dropped table
    pub purge: bool,
}
/// SET <variable>
///
/// Note: this is not a standard SQL statement, but it is supported by at
/// least MySQL and PostgreSQL. Not all MySQL-specific syntatic forms are
/// supported yet.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct SetVariable {
    pub local: bool,
    pub hivevar: bool,
    pub variable: Ident,
    pub value: Vec<SetVariableValue>,
}
/// SHOW <variable>
///
/// Note: this is a PostgreSQL-specific statement.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct ShowVariable {
    pub variable: Vec<Ident>,
}
/// SHOW CREATE TABLE
///
/// Note: this is a MySQL-specific statement.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct ShowCreate {
    pub obj_type: ShowCreateObject,
    pub obj_name: ObjectName,
}
/// SHOW COLUMNS
///
/// Note: this is a MySQL-specific statement.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct ShowColumns {
    pub extended: bool,
    pub full: bool,
    pub table_name: ObjectName,
    pub filter: Option<ShowStatementFilter>,
}
/// `{ BEGIN [ TRANSACTION | WORK ] | START TRANSACTION } ...`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct StartTransaction {
    pub modes: Vec<TransactionMode>,
}
/// `SET TRANSACTION ...`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct SetTransaction {
    pub modes: Vec<TransactionMode>,
}
/// `COMMIT [ TRANSACTION | WORK ] [ AND [ NO ] CHAIN ]`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Commit {
    pub chain: bool,
}
/// `ROLLBACK [ TRANSACTION | WORK ] [ AND [ NO ] CHAIN ]`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Rollback {
    pub chain: bool,
}
/// CREATE SCHEMA
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct CreateSchema {
    pub schema_name: ObjectName,
    pub if_not_exists: bool,
}
/// CREATE DATABASE
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct CreateDatabase {
    pub db_name: ObjectName,
    pub if_not_exists: bool,
    pub location: Option<String>,
    pub managed_location: Option<String>,
}
/// `ASSERT <condition> [AS <message>]`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Assert {
    pub condition: Expr,
    pub message: Option<Expr>,
}
/// `DEALLOCATE [ PREPARE ] { name | ALL }`
///
/// Note: this is a PostgreSQL-specific statement.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Deallocate {
    pub name: Ident,
    pub prepare: bool,
}
/// `EXECUTE name [ ( parameter [, ...] ) ]`
///
/// Note: this is a PostgreSQL-specific statement.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Execute {
    pub name: Ident,
    pub parameters: Vec<Expr>,
}
/// `PREPARE name [ ( data_type [, ...] ) ] AS statement`
///
/// Note: this is a PostgreSQL-specific statement.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Prepare {
    pub name: Ident,
    pub data_types: Vec<DataType>,
    pub statement: Box<Statement>,
}
/// EXPLAIN
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Explain {
    /// Carry out the command and show actual run times and other statistics.
    pub analyze: bool,
    // Display additional information regarding the plan.
    pub verbose: bool,
    /// A SQL query that specifies what to explain
    pub statement: Box<Statement>,
}

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
use alloc::{
    boxed::Box,
    format,
    string::{String, ToString},
    vec::Vec,
};

use core::fmt::{self, Display};
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
#[cfg(feature = "visitor")]
use sqlparser_derive::{Visit, VisitMut};

use crate::display_utils::{indented_list, DisplayCommaSeparated, Indent, NewLine, SpaceOrNewline};

pub use super::ddl::{ColumnDef, TableConstraint};

use super::{
    display_comma_separated, display_separated, query::InputFormatClause, Assignment, ClusteredBy,
    CommentDef, CreateTableOptions, Expr, FileFormat, FromTable, HiveDistributionStyle, HiveFormat,
    HiveIOFormat, HiveRowFormat, Ident, IndexType, InsertAliases, MysqlInsertPriority, ObjectName,
    OnCommit, OnInsert, OneOrManyWithParens, OrderByExpr, Query, RowAccessPolicy, SelectItem,
    Setting, SqliteOnConflict, StorageSerializationPolicy, TableObject, TableWithJoins, Tag,
    WrappedCollection,
};

/// Index column type.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct IndexColumn {
    pub column: OrderByExpr,
    pub operator_class: Option<Ident>,
}

impl Display for IndexColumn {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.column)?;
        if let Some(operator_class) = &self.operator_class {
            write!(f, " {operator_class}")?;
        }
        Ok(())
    }
}

/// CREATE INDEX statement.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct CreateIndex {
    /// index name
    pub name: Option<ObjectName>,
    #[cfg_attr(feature = "visitor", visit(with = "visit_relation"))]
    pub table_name: ObjectName,
    pub using: Option<IndexType>,
    pub columns: Vec<IndexColumn>,
    pub unique: bool,
    pub concurrently: bool,
    pub if_not_exists: bool,
    pub include: Vec<Ident>,
    pub nulls_distinct: Option<bool>,
    /// WITH clause: <https://www.postgresql.org/docs/current/sql-createindex.html>
    pub with: Vec<Expr>,
    pub predicate: Option<Expr>,
}

impl Display for CreateIndex {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "CREATE {unique}INDEX {concurrently}{if_not_exists}",
            unique = if self.unique { "UNIQUE " } else { "" },
            concurrently = if self.concurrently {
                "CONCURRENTLY "
            } else {
                ""
            },
            if_not_exists = if self.if_not_exists {
                "IF NOT EXISTS "
            } else {
                ""
            },
        )?;
        if let Some(value) = &self.name {
            write!(f, "{value} ")?;
        }
        write!(f, "ON {}", self.table_name)?;
        if let Some(value) = &self.using {
            write!(f, " USING {value} ")?;
        }
        write!(f, "({})", display_separated(&self.columns, ","))?;
        if !self.include.is_empty() {
            write!(f, " INCLUDE ({})", display_separated(&self.include, ","))?;
        }
        if let Some(value) = self.nulls_distinct {
            if value {
                write!(f, " NULLS DISTINCT")?;
            } else {
                write!(f, " NULLS NOT DISTINCT")?;
            }
        }
        if !self.with.is_empty() {
            write!(f, " WITH ({})", display_comma_separated(&self.with))?;
        }
        if let Some(predicate) = &self.predicate {
            write!(f, " WHERE {predicate}")?;
        }
        Ok(())
    }
}

/// CREATE TABLE statement.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct CreateTable {
    pub or_replace: bool,
    pub temporary: bool,
    pub external: bool,
    pub global: Option<bool>,
    pub if_not_exists: bool,
    pub transient: bool,
    pub volatile: bool,
    pub iceberg: bool,
    /// Table name
    #[cfg_attr(feature = "visitor", visit(with = "visit_relation"))]
    pub name: ObjectName,
    /// Optional schema
    pub columns: Vec<ColumnDef>,
    pub constraints: Vec<TableConstraint>,
    pub hive_distribution: HiveDistributionStyle,
    pub hive_formats: Option<HiveFormat>,
    pub table_options: CreateTableOptions,
    pub file_format: Option<FileFormat>,
    pub location: Option<String>,
    pub query: Option<Box<Query>>,
    pub without_rowid: bool,
    pub like: Option<ObjectName>,
    pub clone: Option<ObjectName>,
    // For Hive dialect, the table comment is after the column definitions without `=`,
    // so the `comment` field is optional and different than the comment field in the general options list.
    // [Hive](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-CreateTable)
    pub comment: Option<CommentDef>,
    pub on_commit: Option<OnCommit>,
    /// ClickHouse "ON CLUSTER" clause:
    /// <https://clickhouse.com/docs/en/sql-reference/distributed-ddl/>
    pub on_cluster: Option<Ident>,
    /// ClickHouse "PRIMARY KEY " clause.
    /// <https://clickhouse.com/docs/en/sql-reference/statements/create/table/>
    pub primary_key: Option<Box<Expr>>,
    /// ClickHouse "ORDER BY " clause. Note that omitted ORDER BY is different
    /// than empty (represented as ()), the latter meaning "no sorting".
    /// <https://clickhouse.com/docs/en/sql-reference/statements/create/table/>
    pub order_by: Option<OneOrManyWithParens<Expr>>,
    /// BigQuery: A partition expression for the table.
    /// <https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#partition_expression>
    pub partition_by: Option<Box<Expr>>,
    /// BigQuery: Table clustering column list.
    /// <https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#table_option_list>
    /// Snowflake: Table clustering list which contains base column, expressions on base columns.
    /// <https://docs.snowflake.com/en/user-guide/tables-clustering-keys#defining-a-clustering-key-for-a-table>
    pub cluster_by: Option<WrappedCollection<Vec<Expr>>>,
    /// Hive: Table clustering column list.
    /// <https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-CreateTable>
    pub clustered_by: Option<ClusteredBy>,
    /// Postgres `INHERITs` clause, which contains the list of tables from which
    /// the new table inherits.
    /// <https://www.postgresql.org/docs/current/ddl-inherit.html>
    /// <https://www.postgresql.org/docs/current/sql-createtable.html#SQL-CREATETABLE-PARMS-INHERITS>
    pub inherits: Option<Vec<ObjectName>>,
    /// SQLite "STRICT" clause.
    /// if the "STRICT" table-option keyword is added to the end, after the closing ")",
    /// then strict typing rules apply to that table.
    pub strict: bool,
    /// Snowflake "COPY GRANTS" clause
    /// <https://docs.snowflake.com/en/sql-reference/sql/create-table>
    pub copy_grants: bool,
    /// Snowflake "ENABLE_SCHEMA_EVOLUTION" clause
    /// <https://docs.snowflake.com/en/sql-reference/sql/create-table>
    pub enable_schema_evolution: Option<bool>,
    /// Snowflake "CHANGE_TRACKING" clause
    /// <https://docs.snowflake.com/en/sql-reference/sql/create-table>
    pub change_tracking: Option<bool>,
    /// Snowflake "DATA_RETENTION_TIME_IN_DAYS" clause
    /// <https://docs.snowflake.com/en/sql-reference/sql/create-table>
    pub data_retention_time_in_days: Option<u64>,
    /// Snowflake "MAX_DATA_EXTENSION_TIME_IN_DAYS" clause
    /// <https://docs.snowflake.com/en/sql-reference/sql/create-table>
    pub max_data_extension_time_in_days: Option<u64>,
    /// Snowflake "DEFAULT_DDL_COLLATION" clause
    /// <https://docs.snowflake.com/en/sql-reference/sql/create-table>
    pub default_ddl_collation: Option<String>,
    /// Snowflake "WITH AGGREGATION POLICY" clause
    /// <https://docs.snowflake.com/en/sql-reference/sql/create-table>
    pub with_aggregation_policy: Option<ObjectName>,
    /// Snowflake "WITH ROW ACCESS POLICY" clause
    /// <https://docs.snowflake.com/en/sql-reference/sql/create-table>
    pub with_row_access_policy: Option<RowAccessPolicy>,
    /// Snowflake "WITH TAG" clause
    /// <https://docs.snowflake.com/en/sql-reference/sql/create-table>
    pub with_tags: Option<Vec<Tag>>,
    /// Snowflake "EXTERNAL_VOLUME" clause for Iceberg tables
    /// <https://docs.snowflake.com/en/sql-reference/sql/create-iceberg-table>
    pub external_volume: Option<String>,
    /// Snowflake "BASE_LOCATION" clause for Iceberg tables
    /// <https://docs.snowflake.com/en/sql-reference/sql/create-iceberg-table>
    pub base_location: Option<String>,
    /// Snowflake "CATALOG" clause for Iceberg tables
    /// <https://docs.snowflake.com/en/sql-reference/sql/create-iceberg-table>
    pub catalog: Option<String>,
    /// Snowflake "CATALOG_SYNC" clause for Iceberg tables
    /// <https://docs.snowflake.com/en/sql-reference/sql/create-iceberg-table>
    pub catalog_sync: Option<String>,
    /// Snowflake "STORAGE_SERIALIZATION_POLICY" clause for Iceberg tables
    /// <https://docs.snowflake.com/en/sql-reference/sql/create-iceberg-table>
    pub storage_serialization_policy: Option<StorageSerializationPolicy>,
}

impl Display for CreateTable {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // We want to allow the following options
        // Empty column list, allowed by PostgreSQL:
        //   `CREATE TABLE t ()`
        // No columns provided for CREATE TABLE AS:
        //   `CREATE TABLE t AS SELECT a from t2`
        // Columns provided for CREATE TABLE AS:
        //   `CREATE TABLE t (a INT) AS SELECT a from t2`
        write!(
            f,
            "CREATE {or_replace}{external}{global}{temporary}{transient}{volatile}{iceberg}TABLE {if_not_exists}{name}",
            or_replace = if self.or_replace { "OR REPLACE " } else { "" },
            external = if self.external { "EXTERNAL " } else { "" },
            global = self.global
                .map(|global| {
                    if global {
                        "GLOBAL "
                    } else {
                        "LOCAL "
                    }
                })
                .unwrap_or(""),
            if_not_exists = if self.if_not_exists { "IF NOT EXISTS " } else { "" },
            temporary = if self.temporary { "TEMPORARY " } else { "" },
            transient = if self.transient { "TRANSIENT " } else { "" },
            volatile = if self.volatile { "VOLATILE " } else { "" },
            // Only for Snowflake
            iceberg = if self.iceberg { "ICEBERG " } else { "" },
            name = self.name,
        )?;
        if let Some(on_cluster) = &self.on_cluster {
            write!(f, " ON CLUSTER {on_cluster}")?;
        }
        if !self.columns.is_empty() || !self.constraints.is_empty() {
            f.write_str(" (")?;
            NewLine.fmt(f)?;
            Indent(DisplayCommaSeparated(&self.columns)).fmt(f)?;
            if !self.columns.is_empty() && !self.constraints.is_empty() {
                f.write_str(",")?;
                SpaceOrNewline.fmt(f)?;
            }
            Indent(DisplayCommaSeparated(&self.constraints)).fmt(f)?;
            NewLine.fmt(f)?;
            f.write_str(")")?;
        } else if self.query.is_none() && self.like.is_none() && self.clone.is_none() {
            // PostgreSQL allows `CREATE TABLE t ();`, but requires empty parens
            f.write_str(" ()")?;
        }

        // Hive table comment should be after column definitions, please refer to:
        // [Hive](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-CreateTable)
        if let Some(comment) = &self.comment {
            write!(f, " COMMENT '{comment}'")?;
        }

        // Only for SQLite
        if self.without_rowid {
            write!(f, " WITHOUT ROWID")?;
        }

        // Only for Hive
        if let Some(l) = &self.like {
            write!(f, " LIKE {l}")?;
        }

        if let Some(c) = &self.clone {
            write!(f, " CLONE {c}")?;
        }

        match &self.hive_distribution {
            HiveDistributionStyle::PARTITIONED { columns } => {
                write!(f, " PARTITIONED BY ({})", display_comma_separated(columns))?;
            }
            HiveDistributionStyle::SKEWED {
                columns,
                on,
                stored_as_directories,
            } => {
                write!(
                    f,
                    " SKEWED BY ({})) ON ({})",
                    display_comma_separated(columns),
                    display_comma_separated(on)
                )?;
                if *stored_as_directories {
                    write!(f, " STORED AS DIRECTORIES")?;
                }
            }
            _ => (),
        }

        if let Some(clustered_by) = &self.clustered_by {
            write!(f, " {clustered_by}")?;
        }

        if let Some(HiveFormat {
            row_format,
            serde_properties,
            storage,
            location,
        }) = &self.hive_formats
        {
            match row_format {
                Some(HiveRowFormat::SERDE { class }) => write!(f, " ROW FORMAT SERDE '{class}'")?,
                Some(HiveRowFormat::DELIMITED { delimiters }) => {
                    write!(f, " ROW FORMAT DELIMITED")?;
                    if !delimiters.is_empty() {
                        write!(f, " {}", display_separated(delimiters, " "))?;
                    }
                }
                None => (),
            }
            match storage {
                Some(HiveIOFormat::IOF {
                    input_format,
                    output_format,
                }) => write!(
                    f,
                    " STORED AS INPUTFORMAT {input_format} OUTPUTFORMAT {output_format}"
                )?,
                Some(HiveIOFormat::FileFormat { format }) if !self.external => {
                    write!(f, " STORED AS {format}")?
                }
                _ => (),
            }
            if let Some(serde_properties) = serde_properties.as_ref() {
                write!(
                    f,
                    " WITH SERDEPROPERTIES ({})",
                    display_comma_separated(serde_properties)
                )?;
            }
            if !self.external {
                if let Some(loc) = location {
                    write!(f, " LOCATION '{loc}'")?;
                }
            }
        }
        if self.external {
            if let Some(file_format) = self.file_format {
                write!(f, " STORED AS {file_format}")?;
            }
            write!(f, " LOCATION '{}'", self.location.as_ref().unwrap())?;
        }

        match &self.table_options {
            options @ CreateTableOptions::With(_)
            | options @ CreateTableOptions::Plain(_)
            | options @ CreateTableOptions::TableProperties(_) => write!(f, " {options}")?,
            _ => (),
        }

        if let Some(primary_key) = &self.primary_key {
            write!(f, " PRIMARY KEY {primary_key}")?;
        }
        if let Some(order_by) = &self.order_by {
            write!(f, " ORDER BY {order_by}")?;
        }
        if let Some(inherits) = &self.inherits {
            write!(f, " INHERITS ({})", display_comma_separated(inherits))?;
        }
        if let Some(partition_by) = self.partition_by.as_ref() {
            write!(f, " PARTITION BY {partition_by}")?;
        }
        if let Some(cluster_by) = self.cluster_by.as_ref() {
            write!(f, " CLUSTER BY {cluster_by}")?;
        }
        if let options @ CreateTableOptions::Options(_) = &self.table_options {
            write!(f, " {options}")?;
        }
        if let Some(external_volume) = self.external_volume.as_ref() {
            write!(f, " EXTERNAL_VOLUME = '{external_volume}'")?;
        }

        if let Some(catalog) = self.catalog.as_ref() {
            write!(f, " CATALOG = '{catalog}'")?;
        }

        if self.iceberg {
            if let Some(base_location) = self.base_location.as_ref() {
                write!(f, " BASE_LOCATION = '{base_location}'")?;
            }
        }

        if let Some(catalog_sync) = self.catalog_sync.as_ref() {
            write!(f, " CATALOG_SYNC = '{catalog_sync}'")?;
        }

        if let Some(storage_serialization_policy) = self.storage_serialization_policy.as_ref() {
            write!(
                f,
                " STORAGE_SERIALIZATION_POLICY = {storage_serialization_policy}"
            )?;
        }

        if self.copy_grants {
            write!(f, " COPY GRANTS")?;
        }

        if let Some(is_enabled) = self.enable_schema_evolution {
            write!(
                f,
                " ENABLE_SCHEMA_EVOLUTION={}",
                if is_enabled { "TRUE" } else { "FALSE" }
            )?;
        }

        if let Some(is_enabled) = self.change_tracking {
            write!(
                f,
                " CHANGE_TRACKING={}",
                if is_enabled { "TRUE" } else { "FALSE" }
            )?;
        }

        if let Some(data_retention_time_in_days) = self.data_retention_time_in_days {
            write!(
                f,
                " DATA_RETENTION_TIME_IN_DAYS={data_retention_time_in_days}",
            )?;
        }

        if let Some(max_data_extension_time_in_days) = self.max_data_extension_time_in_days {
            write!(
                f,
                " MAX_DATA_EXTENSION_TIME_IN_DAYS={max_data_extension_time_in_days}",
            )?;
        }

        if let Some(default_ddl_collation) = &self.default_ddl_collation {
            write!(f, " DEFAULT_DDL_COLLATION='{default_ddl_collation}'",)?;
        }

        if let Some(with_aggregation_policy) = &self.with_aggregation_policy {
            write!(f, " WITH AGGREGATION POLICY {with_aggregation_policy}",)?;
        }

        if let Some(row_access_policy) = &self.with_row_access_policy {
            write!(f, " {row_access_policy}",)?;
        }

        if let Some(tag) = &self.with_tags {
            write!(f, " WITH TAG ({})", display_comma_separated(tag.as_slice()))?;
        }

        if self.on_commit.is_some() {
            let on_commit = match self.on_commit {
                Some(OnCommit::DeleteRows) => "ON COMMIT DELETE ROWS",
                Some(OnCommit::PreserveRows) => "ON COMMIT PRESERVE ROWS",
                Some(OnCommit::Drop) => "ON COMMIT DROP",
                None => "",
            };
            write!(f, " {on_commit}")?;
        }
        if self.strict {
            write!(f, " STRICT")?;
        }
        if let Some(query) = &self.query {
            write!(f, " AS {query}")?;
        }
        Ok(())
    }
}

/// INSERT statement.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct Insert {
    /// Only for Sqlite
    pub or: Option<SqliteOnConflict>,
    /// Only for mysql
    pub ignore: bool,
    /// INTO - optional keyword
    pub into: bool,
    /// TABLE
    pub table: TableObject,
    /// table_name as foo (for PostgreSQL)
    pub table_alias: Option<Ident>,
    /// COLUMNS
    pub columns: Vec<Ident>,
    /// Overwrite (Hive)
    pub overwrite: bool,
    /// A SQL query that specifies what to insert
    pub source: Option<Box<Query>>,
    /// MySQL `INSERT INTO ... SET`
    /// See: <https://dev.mysql.com/doc/refman/8.4/en/insert.html>
    pub assignments: Vec<Assignment>,
    /// partitioned insert (Hive)
    pub partitioned: Option<Vec<Expr>>,
    /// Columns defined after PARTITION
    pub after_columns: Vec<Ident>,
    /// whether the insert has the table keyword (Hive)
    pub has_table_keyword: bool,
    pub on: Option<OnInsert>,
    /// RETURNING
    pub returning: Option<Vec<SelectItem>>,
    /// Only for mysql
    pub replace_into: bool,
    /// Only for mysql
    pub priority: Option<MysqlInsertPriority>,
    /// Only for mysql
    pub insert_alias: Option<InsertAliases>,
    /// Settings used for ClickHouse.
    ///
    /// ClickHouse syntax: `INSERT INTO tbl SETTINGS format_template_resultset = '/some/path/resultset.format'`
    ///
    /// [ClickHouse `INSERT INTO`](https://clickhouse.com/docs/en/sql-reference/statements/insert-into)
    pub settings: Option<Vec<Setting>>,
    /// Format for `INSERT` statement when not using standard SQL format. Can be e.g. `CSV`,
    /// `JSON`, `JSONAsString`, `LineAsString` and more.
    ///
    /// ClickHouse syntax: `INSERT INTO tbl FORMAT JSONEachRow {"foo": 1, "bar": 2}, {"foo": 3}`
    ///
    /// [ClickHouse formats JSON insert](https://clickhouse.com/docs/en/interfaces/formats#json-inserting-data)
    pub format_clause: Option<InputFormatClause>,
}

impl Display for Insert {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let table_name = if let Some(alias) = &self.table_alias {
            format!("{0} AS {alias}", self.table)
        } else {
            self.table.to_string()
        };

        if let Some(on_conflict) = self.or {
            write!(f, "INSERT {on_conflict} INTO {table_name} ")?;
        } else {
            write!(
                f,
                "{start}",
                start = if self.replace_into {
                    "REPLACE"
                } else {
                    "INSERT"
                },
            )?;
            if let Some(priority) = self.priority {
                write!(f, " {priority}",)?;
            }

            write!(
                f,
                "{ignore}{over}{int}{tbl} {table_name} ",
                table_name = table_name,
                ignore = if self.ignore { " IGNORE" } else { "" },
                over = if self.overwrite { " OVERWRITE" } else { "" },
                int = if self.into { " INTO" } else { "" },
                tbl = if self.has_table_keyword { " TABLE" } else { "" },
            )?;
        }
        if !self.columns.is_empty() {
            write!(f, "({})", display_comma_separated(&self.columns))?;
            SpaceOrNewline.fmt(f)?;
        }
        if let Some(ref parts) = self.partitioned {
            if !parts.is_empty() {
                write!(f, "PARTITION ({})", display_comma_separated(parts))?;
                SpaceOrNewline.fmt(f)?;
            }
        }
        if !self.after_columns.is_empty() {
            write!(f, "({})", display_comma_separated(&self.after_columns))?;
            SpaceOrNewline.fmt(f)?;
        }

        if let Some(settings) = &self.settings {
            write!(f, "SETTINGS {}", display_comma_separated(settings))?;
            SpaceOrNewline.fmt(f)?;
        }

        if let Some(source) = &self.source {
            source.fmt(f)?;
        } else if !self.assignments.is_empty() {
            write!(f, "SET")?;
            indented_list(f, &self.assignments)?;
        } else if let Some(format_clause) = &self.format_clause {
            format_clause.fmt(f)?;
        } else if self.columns.is_empty() {
            write!(f, "DEFAULT VALUES")?;
        }

        if let Some(insert_alias) = &self.insert_alias {
            write!(f, " AS {0}", insert_alias.row_alias)?;

            if let Some(col_aliases) = &insert_alias.col_aliases {
                if !col_aliases.is_empty() {
                    write!(f, " ({})", display_comma_separated(col_aliases))?;
                }
            }
        }

        if let Some(on) = &self.on {
            write!(f, "{on}")?;
        }

        if let Some(returning) = &self.returning {
            SpaceOrNewline.fmt(f)?;
            f.write_str("RETURNING")?;
            indented_list(f, returning)?;
        }
        Ok(())
    }
}

/// DELETE statement.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct Delete {
    /// Multi tables delete are supported in mysql
    pub tables: Vec<ObjectName>,
    /// FROM
    pub from: FromTable,
    /// USING (Snowflake, Postgres, MySQL)
    pub using: Option<Vec<TableWithJoins>>,
    /// WHERE
    pub selection: Option<Expr>,
    /// RETURNING
    pub returning: Option<Vec<SelectItem>>,
    /// ORDER BY (MySQL)
    pub order_by: Vec<OrderByExpr>,
    /// LIMIT (MySQL)
    pub limit: Option<Expr>,
}

impl Display for Delete {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("DELETE")?;
        if !self.tables.is_empty() {
            indented_list(f, &self.tables)?;
        }
        match &self.from {
            FromTable::WithFromKeyword(from) => {
                f.write_str(" FROM")?;
                indented_list(f, from)?;
            }
            FromTable::WithoutKeyword(from) => {
                indented_list(f, from)?;
            }
        }
        if let Some(using) = &self.using {
            SpaceOrNewline.fmt(f)?;
            f.write_str("USING")?;
            indented_list(f, using)?;
        }
        if let Some(selection) = &self.selection {
            SpaceOrNewline.fmt(f)?;
            f.write_str("WHERE")?;
            SpaceOrNewline.fmt(f)?;
            Indent(selection).fmt(f)?;
        }
        if let Some(returning) = &self.returning {
            SpaceOrNewline.fmt(f)?;
            f.write_str("RETURNING")?;
            indented_list(f, returning)?;
        }
        if !self.order_by.is_empty() {
            SpaceOrNewline.fmt(f)?;
            f.write_str("ORDER BY")?;
            indented_list(f, &self.order_by)?;
        }
        if let Some(limit) = &self.limit {
            SpaceOrNewline.fmt(f)?;
            f.write_str("LIMIT")?;
            SpaceOrNewline.fmt(f)?;
            Indent(limit).fmt(f)?;
        }
        Ok(())
    }
}

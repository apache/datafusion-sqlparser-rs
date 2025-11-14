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

use crate::display_utils::{indented_list, Indent, SpaceOrNewline};

use super::{
    display_comma_separated, display_separated, helpers::attached_token::AttachedToken,
    query::InputFormatClause, Assignment, CopyLegacyCsvOption, CopyLegacyOption, CopyOption,
    CopySource, CopyTarget, Expr, FromTable, Ident, InsertAliases, MysqlInsertPriority, ObjectName,
    OnInsert, OrderByExpr, Query, SelectItem, Setting, SqliteOnConflict, TableObject,
    TableWithJoins, UpdateTableFromKind,
};

/// INSERT statement.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct Insert {
    /// Token for the `INSERT` keyword (or its substitutes)
    pub insert_token: AttachedToken,
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
    /// Token for the `DELETE` keyword
    pub delete_token: AttachedToken,
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

/// UPDATE statement.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct Update {
    /// Token for the `UPDATE` keyword
    pub update_token: AttachedToken,
    /// TABLE
    pub table: TableWithJoins,
    /// Column assignments
    pub assignments: Vec<Assignment>,
    /// Table which provide value to be set
    pub from: Option<UpdateTableFromKind>,
    /// WHERE
    pub selection: Option<Expr>,
    /// RETURNING
    pub returning: Option<Vec<SelectItem>>,
    /// SQLite-specific conflict resolution clause
    pub or: Option<SqliteOnConflict>,
    /// LIMIT
    pub limit: Option<Expr>,
}

impl Display for Update {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("UPDATE ")?;
        if let Some(or) = &self.or {
            or.fmt(f)?;
            f.write_str(" ")?;
        }
        self.table.fmt(f)?;
        if let Some(UpdateTableFromKind::BeforeSet(from)) = &self.from {
            SpaceOrNewline.fmt(f)?;
            f.write_str("FROM")?;
            indented_list(f, from)?;
        }
        if !self.assignments.is_empty() {
            SpaceOrNewline.fmt(f)?;
            f.write_str("SET")?;
            indented_list(f, &self.assignments)?;
        }
        if let Some(UpdateTableFromKind::AfterSet(from)) = &self.from {
            SpaceOrNewline.fmt(f)?;
            f.write_str("FROM")?;
            indented_list(f, from)?;
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
        if let Some(limit) = &self.limit {
            SpaceOrNewline.fmt(f)?;
            write!(f, "LIMIT {limit}")?;
        }
        Ok(())
    }
}

/// CSV formatting options extracted from COPY options.
///
/// This struct encapsulates the CSV formatting settings used when parsing
/// or formatting COPY statement data. It extracts relevant options from both
/// modern [`CopyOption`] and legacy [`CopyLegacyOption`] variants.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CsvFormatOptions {
    /// The field delimiter character (default: tab)
    pub(crate) delimiter: char,
    /// The quote character used to enclose fields (default: `"`)
    pub(crate) quote: char,
    /// The escape character (default: `\`)
    pub(crate) escape: char,
    /// The string representing NULL values (default: `\\N`)
    pub(crate) null_symbol: String,
}

impl Default for CsvFormatOptions {
    fn default() -> Self {
        Self {
            delimiter: '\t',
            quote: '"',
            escape: '\\',
            null_symbol: "\\N".to_string(),
        }
    }
}

impl CsvFormatOptions {
    /// Extract CSV format options from CopyOption and CopyLegacyOption lists.
    ///
    /// This method processes both modern and legacy COPY options to determine
    /// the CSV formatting settings. Later options in the lists override earlier ones.
    ///
    /// # Arguments
    ///
    /// * `options` - Modern COPY options (PostgreSQL 9.0+)
    /// * `legacy_options` - Legacy COPY options (pre-PostgreSQL 9.0)
    ///
    /// # Returns
    ///
    /// A `CsvFormatOptions` instance with the extracted settings, using defaults
    /// for any options not specified.
    pub(crate) fn from_copy_options(
        options: &[CopyOption],
        legacy_options: &[CopyLegacyOption],
    ) -> Self {
        let mut csv_options = Self::default();

        // Apply options
        for option in options {
            match option {
                CopyOption::Delimiter(c) => {
                    csv_options.delimiter = *c;
                }
                CopyOption::Quote(c) => {
                    csv_options.quote = *c;
                }
                CopyOption::Escape(c) => {
                    csv_options.escape = *c;
                }
                CopyOption::Null(null) => {
                    csv_options.null_symbol = null.clone();
                }
                // These options don't affect CSV formatting
                CopyOption::Format(_)
                | CopyOption::Freeze(_)
                | CopyOption::Header(_)
                | CopyOption::ForceQuote(_)
                | CopyOption::ForceNotNull(_)
                | CopyOption::ForceNull(_)
                | CopyOption::Encoding(_) => {}
            }
        }

        // Apply legacy options
        for option in legacy_options {
            match option {
                CopyLegacyOption::Delimiter(c) => {
                    csv_options.delimiter = *c;
                }
                CopyLegacyOption::Null(null) => {
                    csv_options.null_symbol = null.clone();
                }
                CopyLegacyOption::Csv(csv_opts) => {
                    for csv_option in csv_opts {
                        match csv_option {
                            CopyLegacyCsvOption::Quote(c) => {
                                csv_options.quote = *c;
                            }
                            CopyLegacyCsvOption::Escape(c) => {
                                csv_options.escape = *c;
                            }
                            // These CSV options don't affect CSV formatting
                            CopyLegacyCsvOption::Header
                            | CopyLegacyCsvOption::ForceQuote(_)
                            | CopyLegacyCsvOption::ForceNotNull(_) => {}
                        }
                    }
                }
                // These legacy options don't affect CSV formatting
                CopyLegacyOption::AcceptAnyDate
                | CopyLegacyOption::AcceptInvChars(_)
                | CopyLegacyOption::AddQuotes
                | CopyLegacyOption::AllowOverwrite
                | CopyLegacyOption::Binary
                | CopyLegacyOption::BlankAsNull
                | CopyLegacyOption::Bzip2
                | CopyLegacyOption::CleanPath
                | CopyLegacyOption::CompUpdate { .. }
                | CopyLegacyOption::DateFormat(_)
                | CopyLegacyOption::EmptyAsNull
                | CopyLegacyOption::Encrypted { .. }
                | CopyLegacyOption::Escape
                | CopyLegacyOption::Extension(_)
                | CopyLegacyOption::FixedWidth(_)
                | CopyLegacyOption::Gzip
                | CopyLegacyOption::Header
                | CopyLegacyOption::IamRole(_)
                | CopyLegacyOption::IgnoreHeader(_)
                | CopyLegacyOption::Json
                | CopyLegacyOption::Manifest { .. }
                | CopyLegacyOption::MaxFileSize(_)
                | CopyLegacyOption::Parallel(_)
                | CopyLegacyOption::Parquet
                | CopyLegacyOption::PartitionBy(_)
                | CopyLegacyOption::Region(_)
                | CopyLegacyOption::RemoveQuotes
                | CopyLegacyOption::RowGroupSize(_)
                | CopyLegacyOption::StatUpdate(_)
                | CopyLegacyOption::TimeFormat(_)
                | CopyLegacyOption::TruncateColumns
                | CopyLegacyOption::Zstd => {}
            }
        }

        csv_options
    }

    /// Format a single CSV field, adding quotes and escaping if necessary.
    ///
    /// This method handles CSV field formatting according to the configured options:
    /// - Writes NULL values using the configured `null_symbol`
    /// - Adds quotes around fields containing delimiters, quotes, or newlines
    /// - Escapes quote characters by doubling them
    /// - Escapes escape characters
    ///
    /// # Arguments
    ///
    /// * `f` - The formatter to write to
    /// * `field` - The field value to format, or `None` for NULL
    ///
    /// # Returns
    ///
    /// A `fmt::Result` indicating success or failure of the write operation.
    fn format_csv_field(&self, f: &mut fmt::Formatter, field: Option<&str>) -> fmt::Result {
        let field_value = field.unwrap_or(&self.null_symbol);

        // Check if field needs quoting
        let needs_quoting = field_value.contains(self.delimiter)
            || field_value.contains(self.quote)
            || field_value.contains('\n')
            || field_value.contains('\r');

        if needs_quoting {
            write!(f, "{}", self.quote)?;
            for ch in field_value.chars() {
                if ch == self.quote {
                    // Escape quote by doubling it
                    write!(f, "{}{}", self.quote, self.quote)?;
                } else if ch == self.escape {
                    // Escape escape character
                    write!(f, "{}{}", self.escape, self.escape)?;
                } else {
                    write!(f, "{}", ch)?;
                }
            }
            write!(f, "{}", self.quote)?;
        } else {
            write!(f, "{}", field_value)?;
        }
        Ok(())
    }
}

/// COPY statement.
///
/// Represents a PostgreSQL COPY statement for bulk data transfer between
/// a file and a table. The statement can copy data FROM a file to a table
/// or TO a file from a table or query.
///
/// # Syntax
///
/// ```sql
/// COPY table_name [(column_list)] FROM { 'filename' | STDIN | PROGRAM 'command' }
/// COPY { table_name [(column_list)] | (query) } TO { 'filename' | STDOUT | PROGRAM 'command' }
/// ```
///
/// # Examples
///
/// ```
/// # use sqlparser::ast::{Copy, CopySource, CopyTarget, ObjectName};
/// # use sqlparser::dialect::PostgreSqlDialect;
/// # use sqlparser::parser::Parser;
/// let sql = "COPY users FROM 'data.csv'";
/// let dialect = PostgreSqlDialect {};
/// let ast = Parser::parse_sql(&dialect, sql).unwrap();
/// ```
///
/// See [PostgreSQL documentation](https://www.postgresql.org/docs/current/sql-copy.html)
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct Copy {
    /// The source of 'COPY TO', or the target of 'COPY FROM'.
    /// Can be a table name with optional column list, or a query (for COPY TO only).
    pub source: CopySource,
    /// Direction of the copy operation.
    /// - `true` for COPY TO (table/query to file)
    /// - `false` for COPY FROM (file to table)
    pub to: bool,
    /// The target of 'COPY TO', or the source of 'COPY FROM'.
    /// Can be a file, STDIN, STDOUT, or a PROGRAM command.
    pub target: CopyTarget,
    /// Modern COPY options (PostgreSQL 9.0+), specified within parentheses.
    /// Examples: FORMAT, DELIMITER, NULL, HEADER, QUOTE, ESCAPE, etc.
    pub options: Vec<CopyOption>,
    /// Legacy COPY options (pre-PostgreSQL 9.0), specified without parentheses.
    /// Also used by AWS Redshift extensions like IAM_ROLE, MANIFEST, etc.
    pub legacy_options: Vec<CopyLegacyOption>,
    /// CSV data rows for COPY FROM STDIN statements.
    /// Each row is a vector of optional strings (None represents NULL).
    /// Populated only when copying from STDIN with inline data.
    pub values: Vec<Vec<Option<String>>>,
}

impl Display for Copy {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "COPY")?;
        match &self.source {
            CopySource::Query(query) => write!(f, " ({query})")?,
            CopySource::Table {
                table_name,
                columns,
            } => {
                write!(f, " {table_name}")?;
                if !columns.is_empty() {
                    write!(f, " ({})", display_comma_separated(columns))?;
                }
            }
        }
        write!(
            f,
            " {} {}",
            if self.to { "TO" } else { "FROM" },
            self.target
        )?;
        if !self.options.is_empty() {
            write!(f, " ({})", display_comma_separated(&self.options))?;
        }
        if !self.legacy_options.is_empty() {
            write!(f, " {}", display_separated(&self.legacy_options, " "))?;
        }

        if !self.values.is_empty() {
            writeln!(f, ";")?;

            let csv_options =
                CsvFormatOptions::from_copy_options(&self.options, &self.legacy_options);

            // Write CSV data
            for row in &self.values {
                for (idx, column) in row.iter().enumerate() {
                    if idx > 0 {
                        write!(f, "{}", csv_options.delimiter)?;
                    }
                    csv_options.format_csv_field(f, column.as_deref())?;
                }
                writeln!(f)?;
            }

            write!(f, "\\.")?;
        }
        Ok(())
    }
}

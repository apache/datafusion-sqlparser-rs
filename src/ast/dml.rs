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
use alloc::{boxed::Box, format, string::ToString, vec::Vec};

use core::fmt::{self, Display};
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
#[cfg(feature = "visitor")]
use sqlparser_derive::{Visit, VisitMut};

use crate::display_utils::{indented_list, Indent, SpaceOrNewline};

use super::{
    display_comma_separated, helpers::attached_token::AttachedToken, query::InputFormatClause,
    Assignment, Expr, FromTable, Ident, InsertAliases, MysqlInsertPriority, ObjectName, OnInsert,
    OrderByExpr, Query, SelectItem, Setting, SqliteOnConflict, TableObject, TableWithJoins,
    UpdateTableFromKind,
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

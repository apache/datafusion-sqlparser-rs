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

use crate::{
    ast::display_separated,
    display_utils::{indented_list, Indent, SpaceOrNewline},
};

use super::{
    display_comma_separated, helpers::attached_token::AttachedToken, query::InputFormatClause,
    Assignment, Expr, FromTable, Ident, InsertAliases, MysqlInsertPriority, ObjectName, OnInsert,
    OptimizerHint, OrderByExpr, Query, SelectInto, SelectItem, Setting, SqliteOnConflict,
    TableFactor, TableObject, TableWithJoins, UpdateTableFromKind, Values,
};

/// INSERT statement.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct Insert {
    /// Token for the `INSERT` keyword (or its substitutes)
    pub insert_token: AttachedToken,
    /// Query optimizer hints
    ///
    /// [MySQL](https://dev.mysql.com/doc/refman/8.4/en/optimizer-hints.html)
    /// [Oracle](https://docs.oracle.com/en/database/oracle/oracle-database/21/sqlrf/Comments.html#GUID-D316D545-89E2-4D54-977F-FC97815CD62E)
    pub optimizer_hints: Vec<OptimizerHint>,
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
    /// ON INSERT
    pub on: Option<OnInsert>,
    /// RETURNING
    pub returning: Option<Vec<SelectItem>>,
    /// OUTPUT (MSSQL)
    pub output: Option<OutputClause>,
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
    /// For Snowflake multi-table insert: specifies the type (`ALL` or `FIRST`)
    ///
    /// - `None` means this is a regular single-table INSERT
    /// - `Some(All)` means `INSERT ALL` (all matching WHEN clauses are executed)
    /// - `Some(First)` means `INSERT FIRST` (only the first matching WHEN clause is executed)
    ///
    /// See: <https://docs.snowflake.com/en/sql-reference/sql/insert-multi-table>
    pub multi_table_insert_type: Option<MultiTableInsertType>,
    /// For multi-table insert: additional INTO clauses (unconditional)
    ///
    /// Used for `INSERT ALL INTO t1 INTO t2 ... SELECT ...`
    ///
    /// See: <https://docs.snowflake.com/en/sql-reference/sql/insert-multi-table>
    pub multi_table_into_clauses: Vec<MultiTableInsertIntoClause>,
    /// For conditional multi-table insert: WHEN clauses
    ///
    /// Used for `INSERT ALL/FIRST WHEN cond THEN INTO t1 ... SELECT ...`
    ///
    /// See: <https://docs.snowflake.com/en/sql-reference/sql/insert-multi-table>
    pub multi_table_when_clauses: Vec<MultiTableInsertWhenClause>,
    /// For conditional multi-table insert: ELSE clause
    ///
    /// See: <https://docs.snowflake.com/en/sql-reference/sql/insert-multi-table>
    pub multi_table_else_clause: Option<Vec<MultiTableInsertIntoClause>>,
}

impl Display for Insert {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // SQLite OR conflict has a special format: INSERT OR ... INTO table_name
        let table_name = if let Some(alias) = &self.table_alias {
            format!("{0} AS {alias}", self.table)
        } else {
            self.table.to_string()
        };

        if let Some(on_conflict) = self.or {
            f.write_str("INSERT")?;
            for hint in &self.optimizer_hints {
                write!(f, " {hint}")?;
            }
            write!(f, " {on_conflict} INTO {table_name} ")?;
        } else {
            write!(
                f,
                "{start}",
                start = if self.replace_into {
                    "REPLACE"
                } else {
                    "INSERT"
                }
            )?;
            for hint in &self.optimizer_hints {
                write!(f, " {hint}")?;
            }
            if let Some(priority) = self.priority {
                write!(f, " {priority}")?;
            }

            if self.ignore {
                write!(f, " IGNORE")?;
            }

            if self.overwrite {
                write!(f, " OVERWRITE")?;
            }

            if let Some(insert_type) = &self.multi_table_insert_type {
                write!(f, " {}", insert_type)?;
            }

            if self.into {
                write!(f, " INTO")?;
            }

            if self.has_table_keyword {
                write!(f, " TABLE")?;
            }

            if !table_name.is_empty() {
                write!(f, " {table_name} ")?;
            }
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

        if let Some(output) = &self.output {
            write!(f, "{output}")?;
            SpaceOrNewline.fmt(f)?;
        }

        if let Some(settings) = &self.settings {
            write!(f, "SETTINGS {}", display_comma_separated(settings))?;
            SpaceOrNewline.fmt(f)?;
        }

        for into_clause in &self.multi_table_into_clauses {
            SpaceOrNewline.fmt(f)?;
            write!(f, "{}", into_clause)?;
        }

        for when_clause in &self.multi_table_when_clauses {
            SpaceOrNewline.fmt(f)?;
            write!(f, "{}", when_clause)?;
        }

        if let Some(else_clauses) = &self.multi_table_else_clause {
            SpaceOrNewline.fmt(f)?;
            write!(f, "ELSE")?;
            for into_clause in else_clauses {
                SpaceOrNewline.fmt(f)?;
                write!(f, "{}", into_clause)?;
            }
        }

        if let Some(source) = &self.source {
            if !self.multi_table_into_clauses.is_empty()
                || !self.multi_table_when_clauses.is_empty()
            {
                SpaceOrNewline.fmt(f)?;
            }
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
    /// Query optimizer hints
    ///
    /// [MySQL](https://dev.mysql.com/doc/refman/8.4/en/optimizer-hints.html)
    /// [Oracle](https://docs.oracle.com/en/database/oracle/oracle-database/21/sqlrf/Comments.html#GUID-D316D545-89E2-4D54-977F-FC97815CD62E)
    pub optimizer_hints: Vec<OptimizerHint>,
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
    /// OUTPUT (MSSQL)
    pub output: Option<OutputClause>,
    /// ORDER BY (MySQL)
    pub order_by: Vec<OrderByExpr>,
    /// LIMIT (MySQL)
    pub limit: Option<Expr>,
}

impl Display for Delete {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("DELETE")?;
        for hint in &self.optimizer_hints {
            f.write_str(" ")?;
            hint.fmt(f)?;
        }
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
        if let Some(output) = &self.output {
            SpaceOrNewline.fmt(f)?;
            write!(f, "{output}")?;
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
    /// Query optimizer hints
    ///
    /// [MySQL](https://dev.mysql.com/doc/refman/8.4/en/optimizer-hints.html)
    /// [Oracle](https://docs.oracle.com/en/database/oracle/oracle-database/21/sqlrf/Comments.html#GUID-D316D545-89E2-4D54-977F-FC97815CD62E)
    pub optimizer_hints: Vec<OptimizerHint>,
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
    /// OUTPUT (MSSQL)
    pub output: Option<OutputClause>,
    /// SQLite-specific conflict resolution clause
    pub or: Option<SqliteOnConflict>,
    /// LIMIT
    pub limit: Option<Expr>,
}

impl Display for Update {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("UPDATE")?;
        for hint in &self.optimizer_hints {
            f.write_str(" ")?;
            hint.fmt(f)?;
        }
        f.write_str(" ")?;
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
        if let Some(output) = &self.output {
            SpaceOrNewline.fmt(f)?;
            write!(f, "{output}")?;
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

/// A `MERGE` statement.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct Merge {
    /// The `MERGE` token that starts the statement.
    pub merge_token: AttachedToken,
    /// Query optimizer hints
    ///
    /// [Oracle](https://docs.oracle.com/en/database/oracle/oracle-database/21/sqlrf/Comments.html#GUID-D316D545-89E2-4D54-977F-FC97815CD62E)
    pub optimizer_hints: Vec<OptimizerHint>,
    /// optional INTO keyword
    pub into: bool,
    /// Specifies the table to merge
    pub table: TableFactor,
    /// Specifies the table or subquery to join with the target table
    pub source: TableFactor,
    /// Specifies the expression on which to join the target table and source
    pub on: Box<Expr>,
    /// Specifies the actions to perform when values match or do not match.
    pub clauses: Vec<MergeClause>,
    /// Specifies the output to save changes in MSSQL
    pub output: Option<OutputClause>,
}

impl Display for Merge {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("MERGE")?;
        for hint in &self.optimizer_hints {
            write!(f, " {hint}")?;
        }
        if self.into {
            write!(f, " INTO")?;
        }
        write!(
            f,
            " {table} USING {source} ",
            table = self.table,
            source = self.source
        )?;
        write!(f, "ON {on} ", on = self.on)?;
        write!(f, "{}", display_separated(&self.clauses, " "))?;
        if let Some(ref output) = self.output {
            write!(f, " {output}")?;
        }
        Ok(())
    }
}

/// A `WHEN` clause within a `MERGE` Statement
///
/// Example:
/// ```sql
/// WHEN NOT MATCHED BY SOURCE AND product LIKE '%washer%' THEN DELETE
/// ```
/// [Snowflake](https://docs.snowflake.com/en/sql-reference/sql/merge)
/// [BigQuery](https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#merge_statement)
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct MergeClause {
    /// The `WHEN` token that starts the sub-expression.
    pub when_token: AttachedToken,
    /// The type of `WHEN` clause.
    pub clause_kind: MergeClauseKind,
    /// An optional predicate to further restrict the clause.
    pub predicate: Option<Expr>,
    /// The action to perform when the clause is matched.
    pub action: MergeAction,
}

impl Display for MergeClause {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let MergeClause {
            when_token: _,
            clause_kind,
            predicate,
            action,
        } = self;

        write!(f, "WHEN {clause_kind}")?;
        if let Some(pred) = predicate {
            write!(f, " AND {pred}")?;
        }
        write!(f, " THEN {action}")
    }
}

/// Variant of `WHEN` clause used within a `MERGE` Statement.
///
/// Example:
/// ```sql
/// MERGE INTO T USING U ON FALSE WHEN MATCHED THEN DELETE
/// ```
/// [Snowflake](https://docs.snowflake.com/en/sql-reference/sql/merge)
/// [BigQuery](https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#merge_statement)
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum MergeClauseKind {
    /// `WHEN MATCHED`
    Matched,
    /// `WHEN NOT MATCHED`
    NotMatched,
    /// `WHEN MATCHED BY TARGET`
    ///
    /// [BigQuery](https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#merge_statement)
    NotMatchedByTarget,
    /// `WHEN MATCHED BY SOURCE`
    ///
    /// [BigQuery](https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#merge_statement)
    NotMatchedBySource,
}

impl Display for MergeClauseKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            MergeClauseKind::Matched => write!(f, "MATCHED"),
            MergeClauseKind::NotMatched => write!(f, "NOT MATCHED"),
            MergeClauseKind::NotMatchedByTarget => write!(f, "NOT MATCHED BY TARGET"),
            MergeClauseKind::NotMatchedBySource => write!(f, "NOT MATCHED BY SOURCE"),
        }
    }
}

/// Underlying statement of a `WHEN` clause within a `MERGE` Statement
///
/// Example
/// ```sql
/// INSERT (product, quantity) VALUES(product, quantity)
/// ```
///
/// [Snowflake](https://docs.snowflake.com/en/sql-reference/sql/merge)
/// [BigQuery](https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#merge_statement)
/// [Oracle](https://docs.oracle.com/en/database/oracle/oracle-database/21/sqlrf/MERGE.html)
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum MergeAction {
    /// An `INSERT` clause
    ///
    /// Example:
    /// ```sql
    /// INSERT (product, quantity) VALUES(product, quantity)
    /// ```
    Insert(MergeInsertExpr),
    /// An `UPDATE` clause
    ///
    /// Example:
    /// ```sql
    /// UPDATE SET quantity = T.quantity + S.quantity
    /// ```
    Update(MergeUpdateExpr),
    /// A plain `DELETE` clause
    Delete {
        /// The `DELETE` token that starts the sub-expression.
        delete_token: AttachedToken,
    },
}

impl Display for MergeAction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            MergeAction::Insert(insert) => {
                write!(f, "INSERT {insert}")
            }
            MergeAction::Update(update) => {
                write!(f, "UPDATE {update}")
            }
            MergeAction::Delete { .. } => {
                write!(f, "DELETE")
            }
        }
    }
}

/// The type of expression used to insert rows within a `MERGE` statement.
///
/// [Snowflake](https://docs.snowflake.com/en/sql-reference/sql/merge)
/// [BigQuery](https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#merge_statement)
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum MergeInsertKind {
    /// The insert expression is defined from an explicit `VALUES` clause
    ///
    /// Example:
    /// ```sql
    /// INSERT VALUES(product, quantity)
    /// ```
    Values(Values),
    /// The insert expression is defined using only the `ROW` keyword.
    ///
    /// Example:
    /// ```sql
    /// INSERT ROW
    /// ```
    /// [BigQuery](https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#merge_statement)
    Row,
}

impl Display for MergeInsertKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            MergeInsertKind::Values(values) => {
                write!(f, "{values}")
            }
            MergeInsertKind::Row => {
                write!(f, "ROW")
            }
        }
    }
}

/// The expression used to insert rows within a `MERGE` statement.
///
/// Examples
/// ```sql
/// INSERT (product, quantity) VALUES(product, quantity)
/// INSERT ROW
/// ```
///
/// [Snowflake](https://docs.snowflake.com/en/sql-reference/sql/merge)
/// [BigQuery](https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#merge_statement)
/// [Oracle](https://docs.oracle.com/en/database/oracle/oracle-database/21/sqlrf/MERGE.html)
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct MergeInsertExpr {
    /// The `INSERT` token that starts the sub-expression.
    pub insert_token: AttachedToken,
    /// Columns (if any) specified by the insert.
    ///
    /// Example:
    /// ```sql
    /// INSERT (product, quantity) VALUES(product, quantity)
    /// INSERT (product, quantity) ROW
    /// ```
    pub columns: Vec<ObjectName>,
    /// The token, `[VALUES | ROW]` starting `kind`.
    pub kind_token: AttachedToken,
    /// The insert type used by the statement.
    pub kind: MergeInsertKind,
    /// An optional condition to restrict the insertion (Oracle specific)
    pub insert_predicate: Option<Expr>,
}

impl Display for MergeInsertExpr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if !self.columns.is_empty() {
            write!(f, "({}) ", display_comma_separated(self.columns.as_slice()))?;
        }
        write!(f, "{}", self.kind)?;
        if let Some(predicate) = self.insert_predicate.as_ref() {
            write!(f, " WHERE {}", predicate)?;
        }
        Ok(())
    }
}

/// The expression used to update rows within a `MERGE` statement.
///
/// Examples
/// ```sql
/// UPDATE SET quantity = T.quantity + S.quantity
/// ```
///
/// [Snowflake](https://docs.snowflake.com/en/sql-reference/sql/merge)
/// [BigQuery](https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#merge_statement)
/// [Oracle](https://docs.oracle.com/en/database/oracle/oracle-database/21/sqlrf/MERGE.html)
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct MergeUpdateExpr {
    /// The `UPDATE` token that starts the sub-expression.
    pub update_token: AttachedToken,
    /// The update assiment expressions
    pub assignments: Vec<Assignment>,
    /// `where_clause` for the update (Oralce specific)
    pub update_predicate: Option<Expr>,
    /// `delete_clause` for the update "delete where" (Oracle specific)
    pub delete_predicate: Option<Expr>,
}

impl Display for MergeUpdateExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SET {}", display_comma_separated(&self.assignments))?;
        if let Some(predicate) = self.update_predicate.as_ref() {
            write!(f, " WHERE {predicate}")?;
        }
        if let Some(predicate) = self.delete_predicate.as_ref() {
            write!(f, " DELETE WHERE {predicate}")?;
        }
        Ok(())
    }
}

/// An `OUTPUT` clause on `MERGE`, `INSERT`, `UPDATE`, or `DELETE` (MSSQL).
///
/// Example:
/// OUTPUT $action, deleted.* INTO dbo.temp_products;
/// <https://learn.microsoft.com/en-us/sql/t-sql/queries/output-clause-transact-sql>
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum OutputClause {
    /// `OUTPUT` clause
    Output {
        /// The `OUTPUT` token that starts the sub-expression.
        output_token: AttachedToken,
        /// The select items to output
        select_items: Vec<SelectItem>,
        /// Optional `INTO` table to direct the output
        into_table: Option<SelectInto>,
    },
    /// `RETURNING` clause
    Returning {
        /// The `RETURNING` token that starts the sub-expression.
        returning_token: AttachedToken,
        /// The select items to return
        select_items: Vec<SelectItem>,
    },
}

impl fmt::Display for OutputClause {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            OutputClause::Output {
                output_token: _,
                select_items,
                into_table,
            } => {
                f.write_str("OUTPUT ")?;
                display_comma_separated(select_items).fmt(f)?;
                if let Some(into_table) = into_table {
                    f.write_str(" ")?;
                    into_table.fmt(f)?;
                }
                Ok(())
            }
            OutputClause::Returning {
                returning_token: _,
                select_items,
            } => {
                f.write_str("RETURNING ")?;
                display_comma_separated(select_items).fmt(f)
            }
        }
    }
}

/// A WHEN clause in a conditional multi-table INSERT.
///
/// Syntax:
/// ```sql
/// WHEN n1 > 100 THEN
///   INTO t1
///   INTO t2 (c1, c2) VALUES (n1, n2)
/// ```
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct MultiTableInsertWhenClause {
    /// The condition for this WHEN clause
    pub condition: Expr,
    /// The INTO clauses to execute when the condition is true
    pub into_clauses: Vec<MultiTableInsertIntoClause>,
}

impl Display for MultiTableInsertWhenClause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "WHEN {} THEN", self.condition)?;
        for into_clause in &self.into_clauses {
            SpaceOrNewline.fmt(f)?;
            write!(f, "{}", into_clause)?;
        }
        Ok(())
    }
}

/// An INTO clause in a multi-table INSERT.
///
/// Syntax:
/// ```sql
/// INTO <target_table> [ ( <target_col_name> [ , ... ] ) ] [ VALUES ( { <source_col_name> | DEFAULT | NULL } [ , ... ] ) ]
/// ```
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct MultiTableInsertIntoClause {
    /// The target table
    pub table_name: ObjectName,
    /// The target columns (optional)
    pub columns: Vec<Ident>,
    /// The VALUES clause (optional)
    pub values: Option<MultiTableInsertValues>,
}

impl Display for MultiTableInsertIntoClause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "INTO {}", self.table_name)?;
        if !self.columns.is_empty() {
            write!(f, " ({})", display_comma_separated(&self.columns))?;
        }
        if let Some(values) = &self.values {
            write!(f, " VALUES ({})", display_comma_separated(&values.values))?;
        }
        Ok(())
    }
}

/// The VALUES clause in a multi-table INSERT INTO clause.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct MultiTableInsertValues {
    /// The values to insert (can be column references, DEFAULT, or NULL)
    pub values: Vec<MultiTableInsertValue>,
}

/// A value in a multi-table INSERT VALUES clause.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum MultiTableInsertValue {
    /// A column reference or expression from the source
    Expr(Expr),
    /// The DEFAULT keyword
    Default,
}

impl Display for MultiTableInsertValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MultiTableInsertValue::Expr(expr) => write!(f, "{}", expr),
            MultiTableInsertValue::Default => write!(f, "DEFAULT"),
        }
    }
}

/// The type of multi-table INSERT statement(Snowflake).
///
/// See: <https://docs.snowflake.com/en/sql-reference/sql/insert-multi-table>
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum MultiTableInsertType {
    /// `INSERT ALL` - all matching WHEN clauses are executed
    All,
    /// `INSERT FIRST` - only the first matching WHEN clause is executed
    First,
}

impl Display for MultiTableInsertType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MultiTableInsertType::All => write!(f, "ALL"),
            MultiTableInsertType::First => write!(f, "FIRST"),
        }
    }
}

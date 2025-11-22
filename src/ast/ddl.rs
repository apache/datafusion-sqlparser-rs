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

//! AST types specific to CREATE/ALTER variants of [`Statement`](crate::ast::Statement)
//! (commonly referred to as Data Definition Language, or DDL)

#[cfg(not(feature = "std"))]
use alloc::{
    boxed::Box,
    format,
    string::{String, ToString},
    vec,
    vec::Vec,
};
use core::fmt::{self, Display, Write};

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

#[cfg(feature = "visitor")]
use sqlparser_derive::{Visit, VisitMut};

use crate::ast::value::escape_single_quote_string;
use crate::ast::{
    display_comma_separated, display_separated,
    table_constraints::{
        CheckConstraint, ForeignKeyConstraint, PrimaryKeyConstraint, TableConstraint,
        UniqueConstraint,
    },
    ArgMode, AttachedToken, CommentDef, ConditionalStatements, CreateFunctionBody,
    CreateFunctionUsing, CreateTableLikeKind, CreateTableOptions, CreateViewParams, DataType, Expr,
    FileFormat, FunctionBehavior, FunctionCalledOnNull, FunctionDesc, FunctionDeterminismSpecifier,
    FunctionParallel, HiveDistributionStyle, HiveFormat, HiveIOFormat, HiveRowFormat,
    HiveSetLocation, Ident, InitializeKind, MySQLColumnPosition, ObjectName, OnCommit,
    OneOrManyWithParens, OperateFunctionArg, OrderByExpr, ProjectionSelect, Query, RefreshModeKind,
    RowAccessPolicy, SequenceOptions, Spanned, SqlOption, StorageSerializationPolicy, TableVersion,
    Tag, TriggerEvent, TriggerExecBody, TriggerObject, TriggerPeriod, TriggerReferencing, Value,
    ValueWithSpan, WrappedCollection,
};
use crate::display_utils::{DisplayCommaSeparated, Indent, NewLine, SpaceOrNewline};
use crate::keywords::Keyword;
use crate::tokenizer::{Span, Token};

/// Index column type.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct IndexColumn {
    pub column: OrderByExpr,
    pub operator_class: Option<Ident>,
}

impl From<Ident> for IndexColumn {
    fn from(c: Ident) -> Self {
        Self {
            column: OrderByExpr::from(c),
            operator_class: None,
        }
    }
}

impl<'a> From<&'a str> for IndexColumn {
    fn from(c: &'a str) -> Self {
        let ident = Ident::new(c);
        ident.into()
    }
}

impl fmt::Display for IndexColumn {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.column)?;
        if let Some(operator_class) = &self.operator_class {
            write!(f, " {operator_class}")?;
        }
        Ok(())
    }
}

/// ALTER TABLE operation REPLICA IDENTITY values
/// See [Postgres ALTER TABLE docs](https://www.postgresql.org/docs/current/sql-altertable.html)
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum ReplicaIdentity {
    None,
    Full,
    Default,
    Index(Ident),
}

impl fmt::Display for ReplicaIdentity {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ReplicaIdentity::None => f.write_str("NONE"),
            ReplicaIdentity::Full => f.write_str("FULL"),
            ReplicaIdentity::Default => f.write_str("DEFAULT"),
            ReplicaIdentity::Index(idx) => write!(f, "USING INDEX {idx}"),
        }
    }
}

/// An `ALTER TABLE` (`Statement::AlterTable`) operation
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum AlterTableOperation {
    /// `ADD <table_constraint> [NOT VALID]`
    AddConstraint {
        constraint: TableConstraint,
        not_valid: bool,
    },
    /// `ADD [COLUMN] [IF NOT EXISTS] <column_def>`
    AddColumn {
        /// `[COLUMN]`.
        column_keyword: bool,
        /// `[IF NOT EXISTS]`
        if_not_exists: bool,
        /// <column_def>.
        column_def: ColumnDef,
        /// MySQL `ALTER TABLE` only  [FIRST | AFTER column_name]
        column_position: Option<MySQLColumnPosition>,
    },
    /// `ADD PROJECTION [IF NOT EXISTS] name ( SELECT <COLUMN LIST EXPR> [GROUP BY] [ORDER BY])`
    ///
    /// Note: this is a ClickHouse-specific operation.
    /// Please refer to [ClickHouse](https://clickhouse.com/docs/en/sql-reference/statements/alter/projection#add-projection)
    AddProjection {
        if_not_exists: bool,
        name: Ident,
        select: ProjectionSelect,
    },
    /// `DROP PROJECTION [IF EXISTS] name`
    ///
    /// Note: this is a ClickHouse-specific operation.
    /// Please refer to [ClickHouse](https://clickhouse.com/docs/en/sql-reference/statements/alter/projection#drop-projection)
    DropProjection {
        if_exists: bool,
        name: Ident,
    },
    /// `MATERIALIZE PROJECTION [IF EXISTS] name [IN PARTITION partition_name]`
    ///
    ///  Note: this is a ClickHouse-specific operation.
    /// Please refer to [ClickHouse](https://clickhouse.com/docs/en/sql-reference/statements/alter/projection#materialize-projection)
    MaterializeProjection {
        if_exists: bool,
        name: Ident,
        partition: Option<Ident>,
    },
    /// `CLEAR PROJECTION [IF EXISTS] name [IN PARTITION partition_name]`
    ///
    /// Note: this is a ClickHouse-specific operation.
    /// Please refer to [ClickHouse](https://clickhouse.com/docs/en/sql-reference/statements/alter/projection#clear-projection)
    ClearProjection {
        if_exists: bool,
        name: Ident,
        partition: Option<Ident>,
    },
    /// `DISABLE ROW LEVEL SECURITY`
    ///
    /// Note: this is a PostgreSQL-specific operation.
    DisableRowLevelSecurity,
    /// `DISABLE RULE rewrite_rule_name`
    ///
    /// Note: this is a PostgreSQL-specific operation.
    DisableRule {
        name: Ident,
    },
    /// `DISABLE TRIGGER [ trigger_name | ALL | USER ]`
    ///
    /// Note: this is a PostgreSQL-specific operation.
    DisableTrigger {
        name: Ident,
    },
    /// `DROP CONSTRAINT [ IF EXISTS ] <name>`
    DropConstraint {
        if_exists: bool,
        name: Ident,
        drop_behavior: Option<DropBehavior>,
    },
    /// `DROP [ COLUMN ] [ IF EXISTS ] <column_name> [ , <column_name>, ... ] [ CASCADE ]`
    DropColumn {
        has_column_keyword: bool,
        column_names: Vec<Ident>,
        if_exists: bool,
        drop_behavior: Option<DropBehavior>,
    },
    /// `ATTACH PART|PARTITION <partition_expr>`
    /// Note: this is a ClickHouse-specific operation, please refer to
    /// [ClickHouse](https://clickhouse.com/docs/en/sql-reference/statements/alter/partition#attach-partitionpart)
    AttachPartition {
        // PART is not a short form of PARTITION, it's a separate keyword
        // which represents a physical file on disk and partition is a logical entity.
        partition: Partition,
    },
    /// `DETACH PART|PARTITION <partition_expr>`
    /// Note: this is a ClickHouse-specific operation, please refer to
    /// [ClickHouse](https://clickhouse.com/docs/en/sql-reference/statements/alter/partition#detach-partitionpart)
    DetachPartition {
        // See `AttachPartition` for more details
        partition: Partition,
    },
    /// `FREEZE PARTITION <partition_expr>`
    /// Note: this is a ClickHouse-specific operation, please refer to
    /// [ClickHouse](https://clickhouse.com/docs/en/sql-reference/statements/alter/partition#freeze-partition)
    FreezePartition {
        partition: Partition,
        with_name: Option<Ident>,
    },
    /// `UNFREEZE PARTITION <partition_expr>`
    /// Note: this is a ClickHouse-specific operation, please refer to
    /// [ClickHouse](https://clickhouse.com/docs/en/sql-reference/statements/alter/partition#unfreeze-partition)
    UnfreezePartition {
        partition: Partition,
        with_name: Option<Ident>,
    },
    /// `DROP PRIMARY KEY`
    ///
    /// [MySQL](https://dev.mysql.com/doc/refman/8.4/en/alter-table.html)
    /// [Snowflake](https://docs.snowflake.com/en/sql-reference/constraints-drop)
    DropPrimaryKey {
        drop_behavior: Option<DropBehavior>,
    },
    /// `DROP FOREIGN KEY <fk_symbol>`
    ///
    /// [MySQL](https://dev.mysql.com/doc/refman/8.4/en/alter-table.html)
    /// [Snowflake](https://docs.snowflake.com/en/sql-reference/constraints-drop)
    DropForeignKey {
        name: Ident,
        drop_behavior: Option<DropBehavior>,
    },
    /// `DROP INDEX <index_name>`
    ///
    /// [MySQL]: https://dev.mysql.com/doc/refman/8.4/en/alter-table.html
    DropIndex {
        name: Ident,
    },
    /// `ENABLE ALWAYS RULE rewrite_rule_name`
    ///
    /// Note: this is a PostgreSQL-specific operation.
    EnableAlwaysRule {
        name: Ident,
    },
    /// `ENABLE ALWAYS TRIGGER trigger_name`
    ///
    /// Note: this is a PostgreSQL-specific operation.
    EnableAlwaysTrigger {
        name: Ident,
    },
    /// `ENABLE REPLICA RULE rewrite_rule_name`
    ///
    /// Note: this is a PostgreSQL-specific operation.
    EnableReplicaRule {
        name: Ident,
    },
    /// `ENABLE REPLICA TRIGGER trigger_name`
    ///
    /// Note: this is a PostgreSQL-specific operation.
    EnableReplicaTrigger {
        name: Ident,
    },
    /// `ENABLE ROW LEVEL SECURITY`
    ///
    /// Note: this is a PostgreSQL-specific operation.
    EnableRowLevelSecurity,
    /// `ENABLE RULE rewrite_rule_name`
    ///
    /// Note: this is a PostgreSQL-specific operation.
    EnableRule {
        name: Ident,
    },
    /// `ENABLE TRIGGER [ trigger_name | ALL | USER ]`
    ///
    /// Note: this is a PostgreSQL-specific operation.
    EnableTrigger {
        name: Ident,
    },
    /// `RENAME TO PARTITION (partition=val)`
    RenamePartitions {
        old_partitions: Vec<Expr>,
        new_partitions: Vec<Expr>,
    },
    /// REPLICA IDENTITY { DEFAULT | USING INDEX index_name | FULL | NOTHING }
    ///
    /// Note: this is a PostgreSQL-specific operation.
    /// Please refer to [PostgreSQL documentation](https://www.postgresql.org/docs/current/sql-altertable.html)
    ReplicaIdentity {
        identity: ReplicaIdentity,
    },
    /// Add Partitions
    AddPartitions {
        if_not_exists: bool,
        new_partitions: Vec<Partition>,
    },
    DropPartitions {
        partitions: Vec<Expr>,
        if_exists: bool,
    },
    /// `RENAME [ COLUMN ] <old_column_name> TO <new_column_name>`
    RenameColumn {
        old_column_name: Ident,
        new_column_name: Ident,
    },
    /// `RENAME TO <table_name>`
    RenameTable {
        table_name: RenameTableNameKind,
    },
    // CHANGE [ COLUMN ] <old_name> <new_name> <data_type> [ <options> ]
    ChangeColumn {
        old_name: Ident,
        new_name: Ident,
        data_type: DataType,
        options: Vec<ColumnOption>,
        /// MySQL `ALTER TABLE` only  [FIRST | AFTER column_name]
        column_position: Option<MySQLColumnPosition>,
    },
    // CHANGE [ COLUMN ] <col_name> <data_type> [ <options> ]
    ModifyColumn {
        col_name: Ident,
        data_type: DataType,
        options: Vec<ColumnOption>,
        /// MySQL `ALTER TABLE` only  [FIRST | AFTER column_name]
        column_position: Option<MySQLColumnPosition>,
    },
    /// `RENAME CONSTRAINT <old_constraint_name> TO <new_constraint_name>`
    ///
    /// Note: this is a PostgreSQL-specific operation.
    RenameConstraint {
        old_name: Ident,
        new_name: Ident,
    },
    /// `ALTER [ COLUMN ]`
    AlterColumn {
        column_name: Ident,
        op: AlterColumnOperation,
    },
    /// 'SWAP WITH <table_name>'
    ///
    /// Note: this is Snowflake specific <https://docs.snowflake.com/en/sql-reference/sql/alter-table>
    SwapWith {
        table_name: ObjectName,
    },
    /// 'SET TBLPROPERTIES ( { property_key [ = ] property_val } [, ...] )'
    SetTblProperties {
        table_properties: Vec<SqlOption>,
    },
    /// `OWNER TO { <new_owner> | CURRENT_ROLE | CURRENT_USER | SESSION_USER }`
    ///
    /// Note: this is PostgreSQL-specific <https://www.postgresql.org/docs/current/sql-altertable.html>
    OwnerTo {
        new_owner: Owner,
    },
    /// Snowflake table clustering options
    /// <https://docs.snowflake.com/en/sql-reference/sql/alter-table#clustering-actions-clusteringaction>
    ClusterBy {
        exprs: Vec<Expr>,
    },
    DropClusteringKey,
    SuspendRecluster,
    ResumeRecluster,
    /// `REFRESH`
    ///
    /// Note: this is Snowflake specific for dynamic tables <https://docs.snowflake.com/en/sql-reference/sql/alter-table>
    Refresh,
    /// `SUSPEND`
    ///
    /// Note: this is Snowflake specific for dynamic tables <https://docs.snowflake.com/en/sql-reference/sql/alter-table>
    Suspend,
    /// `RESUME`
    ///
    /// Note: this is Snowflake specific for dynamic tables <https://docs.snowflake.com/en/sql-reference/sql/alter-table>
    Resume,
    /// `ALGORITHM [=] { DEFAULT | INSTANT | INPLACE | COPY }`
    ///
    /// [MySQL]-specific table alter algorithm.
    ///
    /// [MySQL]: https://dev.mysql.com/doc/refman/8.4/en/alter-table.html
    Algorithm {
        equals: bool,
        algorithm: AlterTableAlgorithm,
    },

    /// `LOCK [=] { DEFAULT | NONE | SHARED | EXCLUSIVE }`
    ///
    /// [MySQL]-specific table alter lock.
    ///
    /// [MySQL]: https://dev.mysql.com/doc/refman/8.4/en/alter-table.html
    Lock {
        equals: bool,
        lock: AlterTableLock,
    },
    /// `AUTO_INCREMENT [=] <value>`
    ///
    /// [MySQL]-specific table option for raising current auto increment value.
    ///
    /// [MySQL]: https://dev.mysql.com/doc/refman/8.4/en/alter-table.html
    AutoIncrement {
        equals: bool,
        value: ValueWithSpan,
    },
    /// `VALIDATE CONSTRAINT <name>`
    ValidateConstraint {
        name: Ident,
    },
    /// Arbitrary parenthesized `SET` options.
    ///
    /// Example:
    /// ```sql
    /// SET (scale_factor = 0.01, threshold = 500)`
    /// ```
    /// [PostgreSQL](https://www.postgresql.org/docs/current/sql-altertable.html)
    SetOptionsParens {
        options: Vec<SqlOption>,
    },
}

/// An `ALTER Policy` (`Statement::AlterPolicy`) operation
///
/// [PostgreSQL Documentation](https://www.postgresql.org/docs/current/sql-altertable.html)
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum AlterPolicyOperation {
    Rename {
        new_name: Ident,
    },
    Apply {
        to: Option<Vec<Owner>>,
        using: Option<Expr>,
        with_check: Option<Expr>,
    },
}

impl fmt::Display for AlterPolicyOperation {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            AlterPolicyOperation::Rename { new_name } => {
                write!(f, " RENAME TO {new_name}")
            }
            AlterPolicyOperation::Apply {
                to,
                using,
                with_check,
            } => {
                if let Some(to) = to {
                    write!(f, " TO {}", display_comma_separated(to))?;
                }
                if let Some(using) = using {
                    write!(f, " USING ({using})")?;
                }
                if let Some(with_check) = with_check {
                    write!(f, " WITH CHECK ({with_check})")?;
                }
                Ok(())
            }
        }
    }
}

/// [MySQL] `ALTER TABLE` algorithm.
///
/// [MySQL]: https://dev.mysql.com/doc/refman/8.4/en/alter-table.html
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum AlterTableAlgorithm {
    Default,
    Instant,
    Inplace,
    Copy,
}

impl fmt::Display for AlterTableAlgorithm {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(match self {
            Self::Default => "DEFAULT",
            Self::Instant => "INSTANT",
            Self::Inplace => "INPLACE",
            Self::Copy => "COPY",
        })
    }
}

/// [MySQL] `ALTER TABLE` lock.
///
/// [MySQL]: https://dev.mysql.com/doc/refman/8.4/en/alter-table.html
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum AlterTableLock {
    Default,
    None,
    Shared,
    Exclusive,
}

impl fmt::Display for AlterTableLock {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(match self {
            Self::Default => "DEFAULT",
            Self::None => "NONE",
            Self::Shared => "SHARED",
            Self::Exclusive => "EXCLUSIVE",
        })
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum Owner {
    Ident(Ident),
    CurrentRole,
    CurrentUser,
    SessionUser,
}

impl fmt::Display for Owner {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Owner::Ident(ident) => write!(f, "{ident}"),
            Owner::CurrentRole => write!(f, "CURRENT_ROLE"),
            Owner::CurrentUser => write!(f, "CURRENT_USER"),
            Owner::SessionUser => write!(f, "SESSION_USER"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum AlterConnectorOwner {
    User(Ident),
    Role(Ident),
}

impl fmt::Display for AlterConnectorOwner {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            AlterConnectorOwner::User(ident) => write!(f, "USER {ident}"),
            AlterConnectorOwner::Role(ident) => write!(f, "ROLE {ident}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum AlterIndexOperation {
    RenameIndex { index_name: ObjectName },
}

impl fmt::Display for AlterTableOperation {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            AlterTableOperation::AddPartitions {
                if_not_exists,
                new_partitions,
            } => write!(
                f,
                "ADD{ine} {}",
                display_separated(new_partitions, " "),
                ine = if *if_not_exists { " IF NOT EXISTS" } else { "" }
            ),
            AlterTableOperation::AddConstraint {
                not_valid,
                constraint,
            } => {
                write!(f, "ADD {constraint}")?;
                if *not_valid {
                    write!(f, " NOT VALID")?;
                }
                Ok(())
            }
            AlterTableOperation::AddColumn {
                column_keyword,
                if_not_exists,
                column_def,
                column_position,
            } => {
                write!(f, "ADD")?;
                if *column_keyword {
                    write!(f, " COLUMN")?;
                }
                if *if_not_exists {
                    write!(f, " IF NOT EXISTS")?;
                }
                write!(f, " {column_def}")?;

                if let Some(position) = column_position {
                    write!(f, " {position}")?;
                }

                Ok(())
            }
            AlterTableOperation::AddProjection {
                if_not_exists,
                name,
                select: query,
            } => {
                write!(f, "ADD PROJECTION")?;
                if *if_not_exists {
                    write!(f, " IF NOT EXISTS")?;
                }
                write!(f, " {name} ({query})")
            }
            AlterTableOperation::Algorithm { equals, algorithm } => {
                write!(
                    f,
                    "ALGORITHM {}{}",
                    if *equals { "= " } else { "" },
                    algorithm
                )
            }
            AlterTableOperation::DropProjection { if_exists, name } => {
                write!(f, "DROP PROJECTION")?;
                if *if_exists {
                    write!(f, " IF EXISTS")?;
                }
                write!(f, " {name}")
            }
            AlterTableOperation::MaterializeProjection {
                if_exists,
                name,
                partition,
            } => {
                write!(f, "MATERIALIZE PROJECTION")?;
                if *if_exists {
                    write!(f, " IF EXISTS")?;
                }
                write!(f, " {name}")?;
                if let Some(partition) = partition {
                    write!(f, " IN PARTITION {partition}")?;
                }
                Ok(())
            }
            AlterTableOperation::ClearProjection {
                if_exists,
                name,
                partition,
            } => {
                write!(f, "CLEAR PROJECTION")?;
                if *if_exists {
                    write!(f, " IF EXISTS")?;
                }
                write!(f, " {name}")?;
                if let Some(partition) = partition {
                    write!(f, " IN PARTITION {partition}")?;
                }
                Ok(())
            }
            AlterTableOperation::AlterColumn { column_name, op } => {
                write!(f, "ALTER COLUMN {column_name} {op}")
            }
            AlterTableOperation::DisableRowLevelSecurity => {
                write!(f, "DISABLE ROW LEVEL SECURITY")
            }
            AlterTableOperation::DisableRule { name } => {
                write!(f, "DISABLE RULE {name}")
            }
            AlterTableOperation::DisableTrigger { name } => {
                write!(f, "DISABLE TRIGGER {name}")
            }
            AlterTableOperation::DropPartitions {
                partitions,
                if_exists,
            } => write!(
                f,
                "DROP{ie} PARTITION ({})",
                display_comma_separated(partitions),
                ie = if *if_exists { " IF EXISTS" } else { "" }
            ),
            AlterTableOperation::DropConstraint {
                if_exists,
                name,
                drop_behavior,
            } => {
                write!(
                    f,
                    "DROP CONSTRAINT {}{}",
                    if *if_exists { "IF EXISTS " } else { "" },
                    name
                )?;
                if let Some(drop_behavior) = drop_behavior {
                    write!(f, " {drop_behavior}")?;
                }
                Ok(())
            }
            AlterTableOperation::DropPrimaryKey { drop_behavior } => {
                write!(f, "DROP PRIMARY KEY")?;
                if let Some(drop_behavior) = drop_behavior {
                    write!(f, " {drop_behavior}")?;
                }
                Ok(())
            }
            AlterTableOperation::DropForeignKey {
                name,
                drop_behavior,
            } => {
                write!(f, "DROP FOREIGN KEY {name}")?;
                if let Some(drop_behavior) = drop_behavior {
                    write!(f, " {drop_behavior}")?;
                }
                Ok(())
            }
            AlterTableOperation::DropIndex { name } => write!(f, "DROP INDEX {name}"),
            AlterTableOperation::DropColumn {
                has_column_keyword,
                column_names: column_name,
                if_exists,
                drop_behavior,
            } => {
                write!(
                    f,
                    "DROP {}{}{}",
                    if *has_column_keyword { "COLUMN " } else { "" },
                    if *if_exists { "IF EXISTS " } else { "" },
                    display_comma_separated(column_name),
                )?;
                if let Some(drop_behavior) = drop_behavior {
                    write!(f, " {drop_behavior}")?;
                }
                Ok(())
            }
            AlterTableOperation::AttachPartition { partition } => {
                write!(f, "ATTACH {partition}")
            }
            AlterTableOperation::DetachPartition { partition } => {
                write!(f, "DETACH {partition}")
            }
            AlterTableOperation::EnableAlwaysRule { name } => {
                write!(f, "ENABLE ALWAYS RULE {name}")
            }
            AlterTableOperation::EnableAlwaysTrigger { name } => {
                write!(f, "ENABLE ALWAYS TRIGGER {name}")
            }
            AlterTableOperation::EnableReplicaRule { name } => {
                write!(f, "ENABLE REPLICA RULE {name}")
            }
            AlterTableOperation::EnableReplicaTrigger { name } => {
                write!(f, "ENABLE REPLICA TRIGGER {name}")
            }
            AlterTableOperation::EnableRowLevelSecurity => {
                write!(f, "ENABLE ROW LEVEL SECURITY")
            }
            AlterTableOperation::EnableRule { name } => {
                write!(f, "ENABLE RULE {name}")
            }
            AlterTableOperation::EnableTrigger { name } => {
                write!(f, "ENABLE TRIGGER {name}")
            }
            AlterTableOperation::RenamePartitions {
                old_partitions,
                new_partitions,
            } => write!(
                f,
                "PARTITION ({}) RENAME TO PARTITION ({})",
                display_comma_separated(old_partitions),
                display_comma_separated(new_partitions)
            ),
            AlterTableOperation::RenameColumn {
                old_column_name,
                new_column_name,
            } => write!(f, "RENAME COLUMN {old_column_name} TO {new_column_name}"),
            AlterTableOperation::RenameTable { table_name } => {
                write!(f, "RENAME {table_name}")
            }
            AlterTableOperation::ChangeColumn {
                old_name,
                new_name,
                data_type,
                options,
                column_position,
            } => {
                write!(f, "CHANGE COLUMN {old_name} {new_name} {data_type}")?;
                if !options.is_empty() {
                    write!(f, " {}", display_separated(options, " "))?;
                }
                if let Some(position) = column_position {
                    write!(f, " {position}")?;
                }

                Ok(())
            }
            AlterTableOperation::ModifyColumn {
                col_name,
                data_type,
                options,
                column_position,
            } => {
                write!(f, "MODIFY COLUMN {col_name} {data_type}")?;
                if !options.is_empty() {
                    write!(f, " {}", display_separated(options, " "))?;
                }
                if let Some(position) = column_position {
                    write!(f, " {position}")?;
                }

                Ok(())
            }
            AlterTableOperation::RenameConstraint { old_name, new_name } => {
                write!(f, "RENAME CONSTRAINT {old_name} TO {new_name}")
            }
            AlterTableOperation::SwapWith { table_name } => {
                write!(f, "SWAP WITH {table_name}")
            }
            AlterTableOperation::OwnerTo { new_owner } => {
                write!(f, "OWNER TO {new_owner}")
            }
            AlterTableOperation::SetTblProperties { table_properties } => {
                write!(
                    f,
                    "SET TBLPROPERTIES({})",
                    display_comma_separated(table_properties)
                )
            }
            AlterTableOperation::FreezePartition {
                partition,
                with_name,
            } => {
                write!(f, "FREEZE {partition}")?;
                if let Some(name) = with_name {
                    write!(f, " WITH NAME {name}")?;
                }
                Ok(())
            }
            AlterTableOperation::UnfreezePartition {
                partition,
                with_name,
            } => {
                write!(f, "UNFREEZE {partition}")?;
                if let Some(name) = with_name {
                    write!(f, " WITH NAME {name}")?;
                }
                Ok(())
            }
            AlterTableOperation::ClusterBy { exprs } => {
                write!(f, "CLUSTER BY ({})", display_comma_separated(exprs))?;
                Ok(())
            }
            AlterTableOperation::DropClusteringKey => {
                write!(f, "DROP CLUSTERING KEY")?;
                Ok(())
            }
            AlterTableOperation::SuspendRecluster => {
                write!(f, "SUSPEND RECLUSTER")?;
                Ok(())
            }
            AlterTableOperation::ResumeRecluster => {
                write!(f, "RESUME RECLUSTER")?;
                Ok(())
            }
            AlterTableOperation::Refresh => {
                write!(f, "REFRESH")
            }
            AlterTableOperation::Suspend => {
                write!(f, "SUSPEND")
            }
            AlterTableOperation::Resume => {
                write!(f, "RESUME")
            }
            AlterTableOperation::AutoIncrement { equals, value } => {
                write!(
                    f,
                    "AUTO_INCREMENT {}{}",
                    if *equals { "= " } else { "" },
                    value
                )
            }
            AlterTableOperation::Lock { equals, lock } => {
                write!(f, "LOCK {}{}", if *equals { "= " } else { "" }, lock)
            }
            AlterTableOperation::ReplicaIdentity { identity } => {
                write!(f, "REPLICA IDENTITY {identity}")
            }
            AlterTableOperation::ValidateConstraint { name } => {
                write!(f, "VALIDATE CONSTRAINT {name}")
            }
            AlterTableOperation::SetOptionsParens { options } => {
                write!(f, "SET ({})", display_comma_separated(options))
            }
        }
    }
}

impl fmt::Display for AlterIndexOperation {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            AlterIndexOperation::RenameIndex { index_name } => {
                write!(f, "RENAME TO {index_name}")
            }
        }
    }
}

/// An `ALTER TYPE` statement (`Statement::AlterType`)
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct AlterType {
    pub name: ObjectName,
    pub operation: AlterTypeOperation,
}

/// An [AlterType] operation
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum AlterTypeOperation {
    Rename(AlterTypeRename),
    AddValue(AlterTypeAddValue),
    RenameValue(AlterTypeRenameValue),
}

/// See [AlterTypeOperation::Rename]
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct AlterTypeRename {
    pub new_name: Ident,
}

/// See [AlterTypeOperation::AddValue]
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct AlterTypeAddValue {
    pub if_not_exists: bool,
    pub value: Ident,
    pub position: Option<AlterTypeAddValuePosition>,
}

/// See [AlterTypeAddValue]
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum AlterTypeAddValuePosition {
    Before(Ident),
    After(Ident),
}

/// See [AlterTypeOperation::RenameValue]
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct AlterTypeRenameValue {
    pub from: Ident,
    pub to: Ident,
}

impl fmt::Display for AlterTypeOperation {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Rename(AlterTypeRename { new_name }) => {
                write!(f, "RENAME TO {new_name}")
            }
            Self::AddValue(AlterTypeAddValue {
                if_not_exists,
                value,
                position,
            }) => {
                write!(f, "ADD VALUE")?;
                if *if_not_exists {
                    write!(f, " IF NOT EXISTS")?;
                }
                write!(f, " {value}")?;
                match position {
                    Some(AlterTypeAddValuePosition::Before(neighbor_value)) => {
                        write!(f, " BEFORE {neighbor_value}")?;
                    }
                    Some(AlterTypeAddValuePosition::After(neighbor_value)) => {
                        write!(f, " AFTER {neighbor_value}")?;
                    }
                    None => {}
                };
                Ok(())
            }
            Self::RenameValue(AlterTypeRenameValue { from, to }) => {
                write!(f, "RENAME VALUE {from} TO {to}")
            }
        }
    }
}

/// An `ALTER COLUMN` (`Statement::AlterTable`) operation
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum AlterColumnOperation {
    /// `SET NOT NULL`
    SetNotNull,
    /// `DROP NOT NULL`
    DropNotNull,
    /// `SET DEFAULT <expr>`
    SetDefault { value: Expr },
    /// `DROP DEFAULT`
    DropDefault,
    /// `[SET DATA] TYPE <data_type> [USING <expr>]`
    SetDataType {
        data_type: DataType,
        /// PostgreSQL specific
        using: Option<Expr>,
        /// Set to true if the statement includes the `SET DATA TYPE` keywords
        had_set: bool,
    },

    /// `ADD GENERATED { ALWAYS | BY DEFAULT } AS IDENTITY [ ( sequence_options ) ]`
    ///
    /// Note: this is a PostgreSQL-specific operation.
    AddGenerated {
        generated_as: Option<GeneratedAs>,
        sequence_options: Option<Vec<SequenceOptions>>,
    },
}

impl fmt::Display for AlterColumnOperation {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            AlterColumnOperation::SetNotNull => write!(f, "SET NOT NULL",),
            AlterColumnOperation::DropNotNull => write!(f, "DROP NOT NULL",),
            AlterColumnOperation::SetDefault { value } => {
                write!(f, "SET DEFAULT {value}")
            }
            AlterColumnOperation::DropDefault => {
                write!(f, "DROP DEFAULT")
            }
            AlterColumnOperation::SetDataType {
                data_type,
                using,
                had_set,
            } => {
                if *had_set {
                    write!(f, "SET DATA ")?;
                }
                write!(f, "TYPE {data_type}")?;
                if let Some(expr) = using {
                    write!(f, " USING {expr}")?;
                }
                Ok(())
            }
            AlterColumnOperation::AddGenerated {
                generated_as,
                sequence_options,
            } => {
                let generated_as = match generated_as {
                    Some(GeneratedAs::Always) => " ALWAYS",
                    Some(GeneratedAs::ByDefault) => " BY DEFAULT",
                    _ => "",
                };

                write!(f, "ADD GENERATED{generated_as} AS IDENTITY",)?;
                if let Some(options) = sequence_options {
                    write!(f, " (")?;

                    for sequence_option in options {
                        write!(f, "{sequence_option}")?;
                    }

                    write!(f, " )")?;
                }
                Ok(())
            }
        }
    }
}

/// Representation whether a definition can can contains the KEY or INDEX keywords with the same
/// meaning.
///
/// This enum initially is directed to `FULLTEXT`,`SPATIAL`, and `UNIQUE` indexes on create table
/// statements of `MySQL` [(1)].
///
/// [1]: https://dev.mysql.com/doc/refman/8.0/en/create-table.html
#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum KeyOrIndexDisplay {
    /// Nothing to display
    None,
    /// Display the KEY keyword
    Key,
    /// Display the INDEX keyword
    Index,
}

impl KeyOrIndexDisplay {
    pub fn is_none(self) -> bool {
        matches!(self, Self::None)
    }
}

impl fmt::Display for KeyOrIndexDisplay {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let left_space = matches!(f.align(), Some(fmt::Alignment::Right));

        if left_space && !self.is_none() {
            f.write_char(' ')?
        }

        match self {
            KeyOrIndexDisplay::None => {
                write!(f, "")
            }
            KeyOrIndexDisplay::Key => {
                write!(f, "KEY")
            }
            KeyOrIndexDisplay::Index => {
                write!(f, "INDEX")
            }
        }
    }
}

/// Indexing method used by that index.
///
/// This structure isn't present on ANSI, but is found at least in [`MySQL` CREATE TABLE][1],
/// [`MySQL` CREATE INDEX][2], and [Postgresql CREATE INDEX][3] statements.
///
/// [1]: https://dev.mysql.com/doc/refman/8.0/en/create-table.html
/// [2]: https://dev.mysql.com/doc/refman/8.0/en/create-index.html
/// [3]: https://www.postgresql.org/docs/14/sql-createindex.html
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum IndexType {
    BTree,
    Hash,
    GIN,
    GiST,
    SPGiST,
    BRIN,
    Bloom,
    /// Users may define their own index types, which would
    /// not be covered by the above variants.
    Custom(Ident),
}

impl fmt::Display for IndexType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::BTree => write!(f, "BTREE"),
            Self::Hash => write!(f, "HASH"),
            Self::GIN => write!(f, "GIN"),
            Self::GiST => write!(f, "GIST"),
            Self::SPGiST => write!(f, "SPGIST"),
            Self::BRIN => write!(f, "BRIN"),
            Self::Bloom => write!(f, "BLOOM"),
            Self::Custom(name) => write!(f, "{name}"),
        }
    }
}

/// MySQL index option, used in [`CREATE TABLE`], [`CREATE INDEX`], and [`ALTER TABLE`].
///
/// [`CREATE TABLE`]: https://dev.mysql.com/doc/refman/8.4/en/create-table.html
/// [`CREATE INDEX`]: https://dev.mysql.com/doc/refman/8.4/en/create-index.html
/// [`ALTER TABLE`]: https://dev.mysql.com/doc/refman/8.4/en/alter-table.html
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum IndexOption {
    /// `USING { BTREE | HASH }`: Index type to use for the index.
    ///
    /// Note that we permissively parse non-MySQL index types, like `GIN`.
    Using(IndexType),
    /// `COMMENT 'string'`: Specifies a comment for the index.
    Comment(String),
}

impl fmt::Display for IndexOption {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Using(index_type) => write!(f, "USING {index_type}"),
            Self::Comment(s) => write!(f, "COMMENT '{s}'"),
        }
    }
}

/// [PostgreSQL] unique index nulls handling option: `[ NULLS [ NOT ] DISTINCT ]`
///
/// [PostgreSQL]: https://www.postgresql.org/docs/17/sql-altertable.html
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum NullsDistinctOption {
    /// Not specified
    None,
    /// NULLS DISTINCT
    Distinct,
    /// NULLS NOT DISTINCT
    NotDistinct,
}

impl fmt::Display for NullsDistinctOption {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::None => Ok(()),
            Self::Distinct => write!(f, " NULLS DISTINCT"),
            Self::NotDistinct => write!(f, " NULLS NOT DISTINCT"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct ProcedureParam {
    pub name: Ident,
    pub data_type: DataType,
    pub mode: Option<ArgMode>,
    pub default: Option<Expr>,
}

impl fmt::Display for ProcedureParam {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let Some(mode) = &self.mode {
            if let Some(default) = &self.default {
                write!(f, "{mode} {} {} = {}", self.name, self.data_type, default)
            } else {
                write!(f, "{mode} {} {}", self.name, self.data_type)
            }
        } else if let Some(default) = &self.default {
            write!(f, "{} {} = {}", self.name, self.data_type, default)
        } else {
            write!(f, "{} {}", self.name, self.data_type)
        }
    }
}

/// SQL column definition
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct ColumnDef {
    pub name: Ident,
    pub data_type: DataType,
    pub options: Vec<ColumnOptionDef>,
}

impl fmt::Display for ColumnDef {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if self.data_type == DataType::Unspecified {
            write!(f, "{}", self.name)?;
        } else {
            write!(f, "{} {}", self.name, self.data_type)?;
        }
        for option in &self.options {
            write!(f, " {option}")?;
        }
        Ok(())
    }
}

/// Column definition specified in a `CREATE VIEW` statement.
///
/// Syntax
/// ```markdown
/// <name> [data_type][OPTIONS(option, ...)]
///
/// option: <name> = <value>
/// ```
///
/// Examples:
/// ```sql
/// name
/// age OPTIONS(description = "age column", tag = "prod")
/// amount COMMENT 'The total amount for the order line'
/// created_at DateTime64
/// ```
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct ViewColumnDef {
    pub name: Ident,
    pub data_type: Option<DataType>,
    pub options: Option<ColumnOptions>,
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum ColumnOptions {
    CommaSeparated(Vec<ColumnOption>),
    SpaceSeparated(Vec<ColumnOption>),
}

impl ColumnOptions {
    pub fn as_slice(&self) -> &[ColumnOption] {
        match self {
            ColumnOptions::CommaSeparated(options) => options.as_slice(),
            ColumnOptions::SpaceSeparated(options) => options.as_slice(),
        }
    }
}

impl fmt::Display for ViewColumnDef {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.name)?;
        if let Some(data_type) = self.data_type.as_ref() {
            write!(f, " {data_type}")?;
        }
        if let Some(options) = self.options.as_ref() {
            match options {
                ColumnOptions::CommaSeparated(column_options) => {
                    write!(f, " {}", display_comma_separated(column_options.as_slice()))?;
                }
                ColumnOptions::SpaceSeparated(column_options) => {
                    write!(f, " {}", display_separated(column_options.as_slice(), " "))?
                }
            }
        }
        Ok(())
    }
}

/// An optionally-named `ColumnOption`: `[ CONSTRAINT <name> ] <column-option>`.
///
/// Note that implementations are substantially more permissive than the ANSI
/// specification on what order column options can be presented in, and whether
/// they are allowed to be named. The specification distinguishes between
/// constraints (NOT NULL, UNIQUE, PRIMARY KEY, and CHECK), which can be named
/// and can appear in any order, and other options (DEFAULT, GENERATED), which
/// cannot be named and must appear in a fixed order. `PostgreSQL`, however,
/// allows preceding any option with `CONSTRAINT <name>`, even those that are
/// not really constraints, like NULL and DEFAULT. MSSQL is less permissive,
/// allowing DEFAULT, UNIQUE, PRIMARY KEY and CHECK to be named, but not NULL or
/// NOT NULL constraints (the last of which is in violation of the spec).
///
/// For maximum flexibility, we don't distinguish between constraint and
/// non-constraint options, lumping them all together under the umbrella of
/// "column options," and we allow any column option to be named.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct ColumnOptionDef {
    pub name: Option<Ident>,
    pub option: ColumnOption,
}

impl fmt::Display for ColumnOptionDef {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}{}", display_constraint_name(&self.name), self.option)
    }
}

/// Identity is a column option for defining an identity or autoincrement column in a `CREATE TABLE` statement.
/// Syntax
/// ```sql
/// { IDENTITY | AUTOINCREMENT } [ (seed , increment) | START num INCREMENT num ] [ ORDER | NOORDER ]
/// ```
/// [MS SQL Server]: https://learn.microsoft.com/en-us/sql/t-sql/statements/create-table-transact-sql-identity-property
/// [Snowflake]: https://docs.snowflake.com/en/sql-reference/sql/create-table
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum IdentityPropertyKind {
    /// An identity property declared via the `AUTOINCREMENT` key word
    /// Example:
    /// ```sql
    ///  AUTOINCREMENT(100, 1) NOORDER
    ///  AUTOINCREMENT START 100 INCREMENT 1 ORDER
    /// ```
    /// [Snowflake]: https://docs.snowflake.com/en/sql-reference/sql/create-table
    Autoincrement(IdentityProperty),
    /// An identity property declared via the `IDENTITY` key word
    /// Example, [MS SQL Server] or [Snowflake]:
    /// ```sql
    ///  IDENTITY(100, 1)
    /// ```
    /// [Snowflake]
    /// ```sql
    ///  IDENTITY(100, 1) ORDER
    ///  IDENTITY START 100 INCREMENT 1 NOORDER
    /// ```
    /// [MS SQL Server]: https://learn.microsoft.com/en-us/sql/t-sql/statements/create-table-transact-sql-identity-property
    /// [Snowflake]: https://docs.snowflake.com/en/sql-reference/sql/create-table
    Identity(IdentityProperty),
}

impl fmt::Display for IdentityPropertyKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let (command, property) = match self {
            IdentityPropertyKind::Identity(property) => ("IDENTITY", property),
            IdentityPropertyKind::Autoincrement(property) => ("AUTOINCREMENT", property),
        };
        write!(f, "{command}")?;
        if let Some(parameters) = &property.parameters {
            write!(f, "{parameters}")?;
        }
        if let Some(order) = &property.order {
            write!(f, "{order}")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct IdentityProperty {
    pub parameters: Option<IdentityPropertyFormatKind>,
    pub order: Option<IdentityPropertyOrder>,
}

/// A format of parameters of identity column.
///
/// It is [Snowflake] specific.
/// Syntax
/// ```sql
/// (seed , increment) | START num INCREMENT num
/// ```
/// [MS SQL Server] uses one way of representing these parameters.
/// Syntax
/// ```sql
/// (seed , increment)
/// ```
/// [MS SQL Server]: https://learn.microsoft.com/en-us/sql/t-sql/statements/create-table-transact-sql-identity-property
/// [Snowflake]: https://docs.snowflake.com/en/sql-reference/sql/create-table
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum IdentityPropertyFormatKind {
    /// A parameters of identity column declared like parameters of function call
    /// Example:
    /// ```sql
    ///  (100, 1)
    /// ```
    /// [MS SQL Server]: https://learn.microsoft.com/en-us/sql/t-sql/statements/create-table-transact-sql-identity-property
    /// [Snowflake]: https://docs.snowflake.com/en/sql-reference/sql/create-table
    FunctionCall(IdentityParameters),
    /// A parameters of identity column declared with keywords `START` and `INCREMENT`
    /// Example:
    /// ```sql
    ///  START 100 INCREMENT 1
    /// ```
    /// [Snowflake]: https://docs.snowflake.com/en/sql-reference/sql/create-table
    StartAndIncrement(IdentityParameters),
}

impl fmt::Display for IdentityPropertyFormatKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            IdentityPropertyFormatKind::FunctionCall(parameters) => {
                write!(f, "({}, {})", parameters.seed, parameters.increment)
            }
            IdentityPropertyFormatKind::StartAndIncrement(parameters) => {
                write!(
                    f,
                    " START {} INCREMENT {}",
                    parameters.seed, parameters.increment
                )
            }
        }
    }
}
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct IdentityParameters {
    pub seed: Expr,
    pub increment: Expr,
}

/// The identity column option specifies how values are generated for the auto-incremented column, either in increasing or decreasing order.
/// Syntax
/// ```sql
/// ORDER | NOORDER
/// ```
/// [Snowflake]: https://docs.snowflake.com/en/sql-reference/sql/create-table
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum IdentityPropertyOrder {
    Order,
    NoOrder,
}

impl fmt::Display for IdentityPropertyOrder {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            IdentityPropertyOrder::Order => write!(f, " ORDER"),
            IdentityPropertyOrder::NoOrder => write!(f, " NOORDER"),
        }
    }
}

/// Column policy that identify a security policy of access to a column.
/// Syntax
/// ```sql
/// [ WITH ] MASKING POLICY <policy_name> [ USING ( <col_name> , <cond_col1> , ... ) ]
/// [ WITH ] PROJECTION POLICY <policy_name>
/// ```
/// [Snowflake]: https://docs.snowflake.com/en/sql-reference/sql/create-table
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum ColumnPolicy {
    MaskingPolicy(ColumnPolicyProperty),
    ProjectionPolicy(ColumnPolicyProperty),
}

impl fmt::Display for ColumnPolicy {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let (command, property) = match self {
            ColumnPolicy::MaskingPolicy(property) => ("MASKING POLICY", property),
            ColumnPolicy::ProjectionPolicy(property) => ("PROJECTION POLICY", property),
        };
        if property.with {
            write!(f, "WITH ")?;
        }
        write!(f, "{command} {}", property.policy_name)?;
        if let Some(using_columns) = &property.using_columns {
            write!(f, " USING ({})", display_comma_separated(using_columns))?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct ColumnPolicyProperty {
    /// This flag indicates that the column policy option is declared using the `WITH` prefix.
    /// Example
    /// ```sql
    /// WITH PROJECTION POLICY sample_policy
    /// ```
    /// [Snowflake]: https://docs.snowflake.com/en/sql-reference/sql/create-table
    pub with: bool,
    pub policy_name: ObjectName,
    pub using_columns: Option<Vec<Ident>>,
}

/// Tags option of column
/// Syntax
/// ```sql
/// [ WITH ] TAG ( <tag_name> = '<tag_value>' [ , <tag_name> = '<tag_value>' , ... ] )
/// ```
/// [Snowflake]: https://docs.snowflake.com/en/sql-reference/sql/create-table
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct TagsColumnOption {
    /// This flag indicates that the tags option is declared using the `WITH` prefix.
    /// Example:
    /// ```sql
    /// WITH TAG (A = 'Tag A')
    /// ```
    /// [Snowflake]: https://docs.snowflake.com/en/sql-reference/sql/create-table
    pub with: bool,
    pub tags: Vec<Tag>,
}

impl fmt::Display for TagsColumnOption {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if self.with {
            write!(f, "WITH ")?;
        }
        write!(f, "TAG ({})", display_comma_separated(&self.tags))?;
        Ok(())
    }
}

/// `ColumnOption`s are modifiers that follow a column definition in a `CREATE
/// TABLE` statement.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum ColumnOption {
    /// `NULL`
    Null,
    /// `NOT NULL`
    NotNull,
    /// `DEFAULT <restricted-expr>`
    Default(Expr),

    /// `MATERIALIZE <expr>`
    /// Syntax: `b INT MATERIALIZE (a + 1)`
    ///
    /// [ClickHouse](https://clickhouse.com/docs/en/sql-reference/statements/create/table#default_values)
    Materialized(Expr),
    /// `EPHEMERAL [<expr>]`
    ///
    /// [ClickHouse](https://clickhouse.com/docs/en/sql-reference/statements/create/table#default_values)
    Ephemeral(Option<Expr>),
    /// `ALIAS <expr>`
    ///
    /// [ClickHouse](https://clickhouse.com/docs/en/sql-reference/statements/create/table#default_values)
    Alias(Expr),

    /// `PRIMARY KEY [<constraint_characteristics>]`
    PrimaryKey(PrimaryKeyConstraint),
    /// `UNIQUE [<constraint_characteristics>]`
    Unique(UniqueConstraint),
    /// A referential integrity constraint (`REFERENCES <foreign_table> (<referred_columns>)
    /// [ MATCH { FULL | PARTIAL | SIMPLE } ]
    /// { [ON DELETE <referential_action>] [ON UPDATE <referential_action>] |
    ///   [ON UPDATE <referential_action>] [ON DELETE <referential_action>]
    /// }         
    /// [<constraint_characteristics>]
    /// `).
    ForeignKey(ForeignKeyConstraint),
    /// `CHECK (<expr>)`
    Check(CheckConstraint),
    /// Dialect-specific options, such as:
    /// - MySQL's `AUTO_INCREMENT` or SQLite's `AUTOINCREMENT`
    /// - ...
    DialectSpecific(Vec<Token>),
    CharacterSet(ObjectName),
    Collation(ObjectName),
    Comment(String),
    OnUpdate(Expr),
    /// `Generated`s are modifiers that follow a column definition in a `CREATE
    /// TABLE` statement.
    Generated {
        generated_as: GeneratedAs,
        sequence_options: Option<Vec<SequenceOptions>>,
        generation_expr: Option<Expr>,
        generation_expr_mode: Option<GeneratedExpressionMode>,
        /// false if 'GENERATED ALWAYS' is skipped (option starts with AS)
        generated_keyword: bool,
    },
    /// BigQuery specific: Explicit column options in a view [1] or table [2]
    /// Syntax
    /// ```sql
    /// OPTIONS(description="field desc")
    /// ```
    /// [1]: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#view_column_option_list
    /// [2]: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#column_option_list
    Options(Vec<SqlOption>),
    /// Creates an identity or an autoincrement column in a table.
    /// Syntax
    /// ```sql
    /// { IDENTITY | AUTOINCREMENT } [ (seed , increment) | START num INCREMENT num ] [ ORDER | NOORDER ]
    /// ```
    /// [MS SQL Server]: https://learn.microsoft.com/en-us/sql/t-sql/statements/create-table-transact-sql-identity-property
    /// [Snowflake]: https://docs.snowflake.com/en/sql-reference/sql/create-table
    Identity(IdentityPropertyKind),
    /// SQLite specific: ON CONFLICT option on column definition
    /// <https://www.sqlite.org/lang_conflict.html>
    OnConflict(Keyword),
    /// Snowflake specific: an option of specifying security masking or projection policy to set on a column.
    /// Syntax:
    /// ```sql
    /// [ WITH ] MASKING POLICY <policy_name> [ USING ( <col_name> , <cond_col1> , ... ) ]
    /// [ WITH ] PROJECTION POLICY <policy_name>
    /// ```
    /// [Snowflake]: https://docs.snowflake.com/en/sql-reference/sql/create-table
    Policy(ColumnPolicy),
    /// Snowflake specific: Specifies the tag name and the tag string value.
    /// Syntax:
    /// ```sql
    /// [ WITH ] TAG ( <tag_name> = '<tag_value>' [ , <tag_name> = '<tag_value>' , ... ] )
    /// ```
    /// [Snowflake]: https://docs.snowflake.com/en/sql-reference/sql/create-table
    Tags(TagsColumnOption),
    /// MySQL specific: Spatial reference identifier
    /// Syntax:
    /// ```sql
    /// CREATE TABLE geom (g GEOMETRY NOT NULL SRID 4326);
    /// ```
    /// [MySQL]: https://dev.mysql.com/doc/refman/8.4/en/creating-spatial-indexes.html
    Srid(Box<Expr>),
    /// MySQL specific: Column is invisible via SELECT *
    /// Syntax:
    /// ```sql
    /// CREATE TABLE t (foo INT, bar INT INVISIBLE);
    /// ```
    /// [MySQL]: https://dev.mysql.com/doc/refman/8.4/en/invisible-columns.html
    Invisible,
}

impl From<UniqueConstraint> for ColumnOption {
    fn from(c: UniqueConstraint) -> Self {
        ColumnOption::Unique(c)
    }
}

impl From<PrimaryKeyConstraint> for ColumnOption {
    fn from(c: PrimaryKeyConstraint) -> Self {
        ColumnOption::PrimaryKey(c)
    }
}

impl From<CheckConstraint> for ColumnOption {
    fn from(c: CheckConstraint) -> Self {
        ColumnOption::Check(c)
    }
}
impl From<ForeignKeyConstraint> for ColumnOption {
    fn from(fk: ForeignKeyConstraint) -> Self {
        ColumnOption::ForeignKey(fk)
    }
}

impl fmt::Display for ColumnOption {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use ColumnOption::*;
        match self {
            Null => write!(f, "NULL"),
            NotNull => write!(f, "NOT NULL"),
            Default(expr) => write!(f, "DEFAULT {expr}"),
            Materialized(expr) => write!(f, "MATERIALIZED {expr}"),
            Ephemeral(expr) => {
                if let Some(e) = expr {
                    write!(f, "EPHEMERAL {e}")
                } else {
                    write!(f, "EPHEMERAL")
                }
            }
            Alias(expr) => write!(f, "ALIAS {expr}"),
            PrimaryKey(constraint) => {
                write!(f, "PRIMARY KEY")?;
                if let Some(characteristics) = &constraint.characteristics {
                    write!(f, " {characteristics}")?;
                }
                Ok(())
            }
            Unique(constraint) => {
                write!(f, "UNIQUE")?;
                if let Some(characteristics) = &constraint.characteristics {
                    write!(f, " {characteristics}")?;
                }
                Ok(())
            }
            ForeignKey(constraint) => {
                write!(f, "REFERENCES {}", constraint.foreign_table)?;
                if !constraint.referred_columns.is_empty() {
                    write!(
                        f,
                        " ({})",
                        display_comma_separated(&constraint.referred_columns)
                    )?;
                }
                if let Some(match_kind) = &constraint.match_kind {
                    write!(f, " {match_kind}")?;
                }
                if let Some(action) = &constraint.on_delete {
                    write!(f, " ON DELETE {action}")?;
                }
                if let Some(action) = &constraint.on_update {
                    write!(f, " ON UPDATE {action}")?;
                }
                if let Some(characteristics) = &constraint.characteristics {
                    write!(f, " {characteristics}")?;
                }
                Ok(())
            }
            Check(constraint) => write!(f, "{constraint}"),
            DialectSpecific(val) => write!(f, "{}", display_separated(val, " ")),
            CharacterSet(n) => write!(f, "CHARACTER SET {n}"),
            Collation(n) => write!(f, "COLLATE {n}"),
            Comment(v) => write!(f, "COMMENT '{}'", escape_single_quote_string(v)),
            OnUpdate(expr) => write!(f, "ON UPDATE {expr}"),
            Generated {
                generated_as,
                sequence_options,
                generation_expr,
                generation_expr_mode,
                generated_keyword,
            } => {
                if let Some(expr) = generation_expr {
                    let modifier = match generation_expr_mode {
                        None => "",
                        Some(GeneratedExpressionMode::Virtual) => " VIRTUAL",
                        Some(GeneratedExpressionMode::Stored) => " STORED",
                    };
                    if *generated_keyword {
                        write!(f, "GENERATED ALWAYS AS ({expr}){modifier}")?;
                    } else {
                        write!(f, "AS ({expr}){modifier}")?;
                    }
                    Ok(())
                } else {
                    // Like Postgres - generated from sequence
                    let when = match generated_as {
                        GeneratedAs::Always => "ALWAYS",
                        GeneratedAs::ByDefault => "BY DEFAULT",
                        // ExpStored goes with an expression, handled above
                        GeneratedAs::ExpStored => unreachable!(),
                    };
                    write!(f, "GENERATED {when} AS IDENTITY")?;
                    if sequence_options.is_some() {
                        let so = sequence_options.as_ref().unwrap();
                        if !so.is_empty() {
                            write!(f, " (")?;
                        }
                        for sequence_option in so {
                            write!(f, "{sequence_option}")?;
                        }
                        if !so.is_empty() {
                            write!(f, " )")?;
                        }
                    }
                    Ok(())
                }
            }
            Options(options) => {
                write!(f, "OPTIONS({})", display_comma_separated(options))
            }
            Identity(parameters) => {
                write!(f, "{parameters}")
            }
            OnConflict(keyword) => {
                write!(f, "ON CONFLICT {keyword:?}")?;
                Ok(())
            }
            Policy(parameters) => {
                write!(f, "{parameters}")
            }
            Tags(tags) => {
                write!(f, "{tags}")
            }
            Srid(srid) => {
                write!(f, "SRID {srid}")
            }
            Invisible => {
                write!(f, "INVISIBLE")
            }
        }
    }
}

/// `GeneratedAs`s are modifiers that follow a column option in a `generated`.
/// 'ExpStored' is used for a column generated from an expression and stored.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum GeneratedAs {
    Always,
    ByDefault,
    ExpStored,
}

/// `GeneratedExpressionMode`s are modifiers that follow an expression in a `generated`.
/// No modifier is typically the same as Virtual.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum GeneratedExpressionMode {
    Virtual,
    Stored,
}

#[must_use]
pub(crate) fn display_constraint_name(name: &'_ Option<Ident>) -> impl fmt::Display + '_ {
    struct ConstraintName<'a>(&'a Option<Ident>);
    impl fmt::Display for ConstraintName<'_> {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            if let Some(name) = self.0 {
                write!(f, "CONSTRAINT {name} ")?;
            }
            Ok(())
        }
    }
    ConstraintName(name)
}

/// If `option` is
/// * `Some(inner)` => create display struct for `"{prefix}{inner}{postfix}"`
/// * `_` => do nothing
#[must_use]
pub(crate) fn display_option<'a, T: fmt::Display>(
    prefix: &'a str,
    postfix: &'a str,
    option: &'a Option<T>,
) -> impl fmt::Display + 'a {
    struct OptionDisplay<'a, T>(&'a str, &'a str, &'a Option<T>);
    impl<T: fmt::Display> fmt::Display for OptionDisplay<'_, T> {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            if let Some(inner) = self.2 {
                let (prefix, postfix) = (self.0, self.1);
                write!(f, "{prefix}{inner}{postfix}")?;
            }
            Ok(())
        }
    }
    OptionDisplay(prefix, postfix, option)
}

/// If `option` is
/// * `Some(inner)` => create display struct for `" {inner}"`
/// * `_` => do nothing
#[must_use]
pub(crate) fn display_option_spaced<T: fmt::Display>(option: &Option<T>) -> impl fmt::Display + '_ {
    display_option(" ", "", option)
}

/// `<constraint_characteristics> = [ DEFERRABLE | NOT DEFERRABLE ] [ INITIALLY DEFERRED | INITIALLY IMMEDIATE ] [ ENFORCED | NOT ENFORCED ]`
///
/// Used in UNIQUE and foreign key constraints. The individual settings may occur in any order.
#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Default, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct ConstraintCharacteristics {
    /// `[ DEFERRABLE | NOT DEFERRABLE ]`
    pub deferrable: Option<bool>,
    /// `[ INITIALLY DEFERRED | INITIALLY IMMEDIATE ]`
    pub initially: Option<DeferrableInitial>,
    /// `[ ENFORCED | NOT ENFORCED ]`
    pub enforced: Option<bool>,
}

#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum DeferrableInitial {
    /// `INITIALLY IMMEDIATE`
    Immediate,
    /// `INITIALLY DEFERRED`
    Deferred,
}

impl ConstraintCharacteristics {
    fn deferrable_text(&self) -> Option<&'static str> {
        self.deferrable.map(|deferrable| {
            if deferrable {
                "DEFERRABLE"
            } else {
                "NOT DEFERRABLE"
            }
        })
    }

    fn initially_immediate_text(&self) -> Option<&'static str> {
        self.initially
            .map(|initially_immediate| match initially_immediate {
                DeferrableInitial::Immediate => "INITIALLY IMMEDIATE",
                DeferrableInitial::Deferred => "INITIALLY DEFERRED",
            })
    }

    fn enforced_text(&self) -> Option<&'static str> {
        self.enforced.map(
            |enforced| {
                if enforced {
                    "ENFORCED"
                } else {
                    "NOT ENFORCED"
                }
            },
        )
    }
}

impl fmt::Display for ConstraintCharacteristics {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let deferrable = self.deferrable_text();
        let initially_immediate = self.initially_immediate_text();
        let enforced = self.enforced_text();

        match (deferrable, initially_immediate, enforced) {
            (None, None, None) => Ok(()),
            (None, None, Some(enforced)) => write!(f, "{enforced}"),
            (None, Some(initial), None) => write!(f, "{initial}"),
            (None, Some(initial), Some(enforced)) => write!(f, "{initial} {enforced}"),
            (Some(deferrable), None, None) => write!(f, "{deferrable}"),
            (Some(deferrable), None, Some(enforced)) => write!(f, "{deferrable} {enforced}"),
            (Some(deferrable), Some(initial), None) => write!(f, "{deferrable} {initial}"),
            (Some(deferrable), Some(initial), Some(enforced)) => {
                write!(f, "{deferrable} {initial} {enforced}")
            }
        }
    }
}

/// `<referential_action> =
/// { RESTRICT | CASCADE | SET NULL | NO ACTION | SET DEFAULT }`
///
/// Used in foreign key constraints in `ON UPDATE` and `ON DELETE` options.
#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum ReferentialAction {
    Restrict,
    Cascade,
    SetNull,
    NoAction,
    SetDefault,
}

impl fmt::Display for ReferentialAction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(match self {
            ReferentialAction::Restrict => "RESTRICT",
            ReferentialAction::Cascade => "CASCADE",
            ReferentialAction::SetNull => "SET NULL",
            ReferentialAction::NoAction => "NO ACTION",
            ReferentialAction::SetDefault => "SET DEFAULT",
        })
    }
}

/// `<drop behavior> ::= CASCADE | RESTRICT`.
///
/// Used in `DROP` statements.
#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum DropBehavior {
    Restrict,
    Cascade,
}

impl fmt::Display for DropBehavior {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(match self {
            DropBehavior::Restrict => "RESTRICT",
            DropBehavior::Cascade => "CASCADE",
        })
    }
}

/// SQL user defined type definition
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum UserDefinedTypeRepresentation {
    /// Composite type: `CREATE TYPE name AS (attributes)`
    Composite {
        attributes: Vec<UserDefinedTypeCompositeAttributeDef>,
    },
    /// Enum type: `CREATE TYPE name AS ENUM (labels)`
    ///
    /// Note: this is PostgreSQL-specific. See <https://www.postgresql.org/docs/current/sql-createtype.html>
    Enum { labels: Vec<Ident> },
    /// Range type: `CREATE TYPE name AS RANGE (options)`
    ///
    /// Note: this is PostgreSQL-specific. See <https://www.postgresql.org/docs/current/sql-createtype.html>
    Range {
        options: Vec<UserDefinedTypeRangeOption>,
    },
    /// Base type (SQL definition): `CREATE TYPE name (options)`
    ///
    /// Note the lack of `AS` keyword
    ///
    /// Note: this is PostgreSQL-specific. See <https://www.postgresql.org/docs/current/sql-createtype.html>
    SqlDefinition {
        options: Vec<UserDefinedTypeSqlDefinitionOption>,
    },
}

impl fmt::Display for UserDefinedTypeRepresentation {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Composite { attributes } => {
                write!(f, "AS ({})", display_comma_separated(attributes))
            }
            Self::Enum { labels } => {
                write!(f, "AS ENUM ({})", display_comma_separated(labels))
            }
            Self::Range { options } => {
                write!(f, "AS RANGE ({})", display_comma_separated(options))
            }
            Self::SqlDefinition { options } => {
                write!(f, "({})", display_comma_separated(options))
            }
        }
    }
}

/// SQL user defined type attribute definition
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct UserDefinedTypeCompositeAttributeDef {
    pub name: Ident,
    pub data_type: DataType,
    pub collation: Option<ObjectName>,
}

impl fmt::Display for UserDefinedTypeCompositeAttributeDef {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} {}", self.name, self.data_type)?;
        if let Some(collation) = &self.collation {
            write!(f, " COLLATE {collation}")?;
        }
        Ok(())
    }
}

/// Internal length specification for PostgreSQL user-defined base types.
///
/// Specifies the internal length in bytes of the new type's internal representation.
/// The default assumption is that it is variable-length.
///
/// # PostgreSQL Documentation
/// See: <https://www.postgresql.org/docs/current/sql-createtype.html>
///
/// # Examples
/// ```sql
/// CREATE TYPE mytype (
///     INPUT = in_func,
///     OUTPUT = out_func,
///     INTERNALLENGTH = 16  -- Fixed 16-byte length
/// );
///
/// CREATE TYPE mytype2 (
///     INPUT = in_func,
///     OUTPUT = out_func,
///     INTERNALLENGTH = VARIABLE  -- Variable length
/// );
/// ```
#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum UserDefinedTypeInternalLength {
    /// Fixed internal length: `INTERNALLENGTH = <number>`
    Fixed(u64),
    /// Variable internal length: `INTERNALLENGTH = VARIABLE`
    Variable,
}

impl fmt::Display for UserDefinedTypeInternalLength {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            UserDefinedTypeInternalLength::Fixed(n) => write!(f, "{}", n),
            UserDefinedTypeInternalLength::Variable => write!(f, "VARIABLE"),
        }
    }
}

/// Alignment specification for PostgreSQL user-defined base types.
///
/// Specifies the storage alignment requirement for values of the data type.
/// The allowed values equate to alignment on 1, 2, 4, or 8 byte boundaries.
/// Note that variable-length types must have an alignment of at least 4, since
/// they necessarily contain an int4 as their first component.
///
/// # PostgreSQL Documentation
/// See: <https://www.postgresql.org/docs/current/sql-createtype.html>
///
/// # Examples
/// ```sql
/// CREATE TYPE mytype (
///     INPUT = in_func,
///     OUTPUT = out_func,
///     ALIGNMENT = int4  -- 4-byte alignment
/// );
/// ```
#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum Alignment {
    /// Single-byte alignment: `ALIGNMENT = char`
    Char,
    /// 2-byte alignment: `ALIGNMENT = int2`
    Int2,
    /// 4-byte alignment: `ALIGNMENT = int4`
    Int4,
    /// 8-byte alignment: `ALIGNMENT = double`
    Double,
}

impl fmt::Display for Alignment {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Alignment::Char => write!(f, "char"),
            Alignment::Int2 => write!(f, "int2"),
            Alignment::Int4 => write!(f, "int4"),
            Alignment::Double => write!(f, "double"),
        }
    }
}

/// Storage specification for PostgreSQL user-defined base types.
///
/// Specifies the storage strategy for values of the data type:
/// - `plain`: Prevents compression and out-of-line storage (for fixed-length types)
/// - `external`: Allows out-of-line storage but not compression
/// - `extended`: Allows both compression and out-of-line storage (default for most types)
/// - `main`: Allows compression but discourages out-of-line storage
///
/// # PostgreSQL Documentation
/// See: <https://www.postgresql.org/docs/current/sql-createtype.html>
///
/// # Examples
/// ```sql
/// CREATE TYPE mytype (
///     INPUT = in_func,
///     OUTPUT = out_func,
///     STORAGE = plain
/// );
/// ```
#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum UserDefinedTypeStorage {
    /// No compression or out-of-line storage: `STORAGE = plain`
    Plain,
    /// Out-of-line storage allowed, no compression: `STORAGE = external`
    External,
    /// Both compression and out-of-line storage allowed: `STORAGE = extended`
    Extended,
    /// Compression allowed, out-of-line discouraged: `STORAGE = main`
    Main,
}

impl fmt::Display for UserDefinedTypeStorage {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            UserDefinedTypeStorage::Plain => write!(f, "plain"),
            UserDefinedTypeStorage::External => write!(f, "external"),
            UserDefinedTypeStorage::Extended => write!(f, "extended"),
            UserDefinedTypeStorage::Main => write!(f, "main"),
        }
    }
}

/// Options for PostgreSQL `CREATE TYPE ... AS RANGE` statement.
///
/// Range types are data types representing a range of values of some element type
/// (called the range's subtype). These options configure the behavior of the range type.
///
/// # PostgreSQL Documentation
/// See: <https://www.postgresql.org/docs/current/sql-createtype.html>
///
/// # Examples
/// ```sql
/// CREATE TYPE int4range AS RANGE (
///     SUBTYPE = int4,
///     SUBTYPE_OPCLASS = int4_ops,
///     CANONICAL = int4range_canonical,
///     SUBTYPE_DIFF = int4range_subdiff
/// );
/// ```
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum UserDefinedTypeRangeOption {
    /// The element type that the range type will represent: `SUBTYPE = subtype`
    Subtype(DataType),
    /// The operator class for the subtype: `SUBTYPE_OPCLASS = subtype_operator_class`
    SubtypeOpClass(ObjectName),
    /// Collation to use for ordering the subtype: `COLLATION = collation`
    Collation(ObjectName),
    /// Function to convert range values to canonical form: `CANONICAL = canonical_function`
    Canonical(ObjectName),
    /// Function to compute the difference between two subtype values: `SUBTYPE_DIFF = subtype_diff_function`
    SubtypeDiff(ObjectName),
    /// Name of the corresponding multirange type: `MULTIRANGE_TYPE_NAME = multirange_type_name`
    MultirangeTypeName(ObjectName),
}

impl fmt::Display for UserDefinedTypeRangeOption {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            UserDefinedTypeRangeOption::Subtype(dt) => write!(f, "SUBTYPE = {}", dt),
            UserDefinedTypeRangeOption::SubtypeOpClass(name) => {
                write!(f, "SUBTYPE_OPCLASS = {}", name)
            }
            UserDefinedTypeRangeOption::Collation(name) => write!(f, "COLLATION = {}", name),
            UserDefinedTypeRangeOption::Canonical(name) => write!(f, "CANONICAL = {}", name),
            UserDefinedTypeRangeOption::SubtypeDiff(name) => write!(f, "SUBTYPE_DIFF = {}", name),
            UserDefinedTypeRangeOption::MultirangeTypeName(name) => {
                write!(f, "MULTIRANGE_TYPE_NAME = {}", name)
            }
        }
    }
}

/// Options for PostgreSQL `CREATE TYPE ... (<options>)` statement (base type definition).
///
/// Base types are the lowest-level data types in PostgreSQL. To define a new base type,
/// you must specify functions that convert it to and from text representation, and optionally
/// binary representation and other properties.
///
/// Note: This syntax uses parentheses directly after the type name, without the `AS` keyword.
///
/// # PostgreSQL Documentation
/// See: <https://www.postgresql.org/docs/current/sql-createtype.html>
///
/// # Examples
/// ```sql
/// CREATE TYPE complex (
///     INPUT = complex_in,
///     OUTPUT = complex_out,
///     INTERNALLENGTH = 16,
///     ALIGNMENT = double
/// );
/// ```
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum UserDefinedTypeSqlDefinitionOption {
    /// Function to convert from external text representation to internal: `INPUT = input_function`
    Input(ObjectName),
    /// Function to convert from internal to external text representation: `OUTPUT = output_function`
    Output(ObjectName),
    /// Function to convert from external binary representation to internal: `RECEIVE = receive_function`
    Receive(ObjectName),
    /// Function to convert from internal to external binary representation: `SEND = send_function`
    Send(ObjectName),
    /// Function to convert type modifiers from text array to internal form: `TYPMOD_IN = type_modifier_input_function`
    TypmodIn(ObjectName),
    /// Function to convert type modifiers from internal to text form: `TYPMOD_OUT = type_modifier_output_function`
    TypmodOut(ObjectName),
    /// Function to compute statistics for the data type: `ANALYZE = analyze_function`
    Analyze(ObjectName),
    /// Function to handle subscripting operations: `SUBSCRIPT = subscript_function`
    Subscript(ObjectName),
    /// Internal storage size in bytes, or VARIABLE for variable-length: `INTERNALLENGTH = { internallength | VARIABLE }`
    InternalLength(UserDefinedTypeInternalLength),
    /// Indicates values are passed by value rather than by reference: `PASSEDBYVALUE`
    PassedByValue,
    /// Storage alignment requirement (1, 2, 4, or 8 bytes): `ALIGNMENT = alignment`
    Alignment(Alignment),
    /// Storage strategy for varlena types: `STORAGE = storage`
    Storage(UserDefinedTypeStorage),
    /// Copy properties from an existing type: `LIKE = like_type`
    Like(ObjectName),
    /// Type category for implicit casting rules (single char): `CATEGORY = category`
    Category(char),
    /// Whether this type is preferred within its category: `PREFERRED = preferred`
    Preferred(bool),
    /// Default value for the type: `DEFAULT = default`
    Default(Expr),
    /// Element type for array types: `ELEMENT = element`
    Element(DataType),
    /// Delimiter character for array value display: `DELIMITER = delimiter`
    Delimiter(String),
    /// Whether the type supports collation: `COLLATABLE = collatable`
    Collatable(bool),
}

impl fmt::Display for UserDefinedTypeSqlDefinitionOption {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            UserDefinedTypeSqlDefinitionOption::Input(name) => write!(f, "INPUT = {}", name),
            UserDefinedTypeSqlDefinitionOption::Output(name) => write!(f, "OUTPUT = {}", name),
            UserDefinedTypeSqlDefinitionOption::Receive(name) => write!(f, "RECEIVE = {}", name),
            UserDefinedTypeSqlDefinitionOption::Send(name) => write!(f, "SEND = {}", name),
            UserDefinedTypeSqlDefinitionOption::TypmodIn(name) => write!(f, "TYPMOD_IN = {}", name),
            UserDefinedTypeSqlDefinitionOption::TypmodOut(name) => {
                write!(f, "TYPMOD_OUT = {}", name)
            }
            UserDefinedTypeSqlDefinitionOption::Analyze(name) => write!(f, "ANALYZE = {}", name),
            UserDefinedTypeSqlDefinitionOption::Subscript(name) => {
                write!(f, "SUBSCRIPT = {}", name)
            }
            UserDefinedTypeSqlDefinitionOption::InternalLength(len) => {
                write!(f, "INTERNALLENGTH = {}", len)
            }
            UserDefinedTypeSqlDefinitionOption::PassedByValue => write!(f, "PASSEDBYVALUE"),
            UserDefinedTypeSqlDefinitionOption::Alignment(align) => {
                write!(f, "ALIGNMENT = {}", align)
            }
            UserDefinedTypeSqlDefinitionOption::Storage(storage) => {
                write!(f, "STORAGE = {}", storage)
            }
            UserDefinedTypeSqlDefinitionOption::Like(name) => write!(f, "LIKE = {}", name),
            UserDefinedTypeSqlDefinitionOption::Category(c) => write!(f, "CATEGORY = '{}'", c),
            UserDefinedTypeSqlDefinitionOption::Preferred(b) => write!(f, "PREFERRED = {}", b),
            UserDefinedTypeSqlDefinitionOption::Default(expr) => write!(f, "DEFAULT = {}", expr),
            UserDefinedTypeSqlDefinitionOption::Element(dt) => write!(f, "ELEMENT = {}", dt),
            UserDefinedTypeSqlDefinitionOption::Delimiter(s) => {
                write!(f, "DELIMITER = '{}'", escape_single_quote_string(s))
            }
            UserDefinedTypeSqlDefinitionOption::Collatable(b) => write!(f, "COLLATABLE = {}", b),
        }
    }
}

/// PARTITION statement used in ALTER TABLE et al. such as in Hive and ClickHouse SQL.
/// For example, ClickHouse's OPTIMIZE TABLE supports syntax like PARTITION ID 'partition_id' and PARTITION expr.
/// [ClickHouse](https://clickhouse.com/docs/en/sql-reference/statements/optimize)
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum Partition {
    Identifier(Ident),
    Expr(Expr),
    /// ClickHouse supports PART expr which represents physical partition in disk.
    /// [ClickHouse](https://clickhouse.com/docs/en/sql-reference/statements/alter/partition#attach-partitionpart)
    Part(Expr),
    Partitions(Vec<Expr>),
}

impl fmt::Display for Partition {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Partition::Identifier(id) => write!(f, "PARTITION ID {id}"),
            Partition::Expr(expr) => write!(f, "PARTITION {expr}"),
            Partition::Part(expr) => write!(f, "PART {expr}"),
            Partition::Partitions(partitions) => {
                write!(f, "PARTITION ({})", display_comma_separated(partitions))
            }
        }
    }
}

/// DEDUPLICATE statement used in OPTIMIZE TABLE et al. such as in ClickHouse SQL
/// [ClickHouse](https://clickhouse.com/docs/en/sql-reference/statements/optimize)
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum Deduplicate {
    All,
    ByExpression(Expr),
}

impl fmt::Display for Deduplicate {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Deduplicate::All => write!(f, "DEDUPLICATE"),
            Deduplicate::ByExpression(expr) => write!(f, "DEDUPLICATE BY {expr}"),
        }
    }
}

/// Hive supports `CLUSTERED BY` statement in `CREATE TABLE`.
/// Syntax: `CLUSTERED BY (col_name, ...) [SORTED BY (col_name [ASC|DESC], ...)] INTO num_buckets BUCKETS`
///
/// [Hive](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-CreateTable)
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct ClusteredBy {
    pub columns: Vec<Ident>,
    pub sorted_by: Option<Vec<OrderByExpr>>,
    pub num_buckets: Value,
}

impl fmt::Display for ClusteredBy {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "CLUSTERED BY ({})",
            display_comma_separated(&self.columns)
        )?;
        if let Some(ref sorted_by) = self.sorted_by {
            write!(f, " SORTED BY ({})", display_comma_separated(sorted_by))?;
        }
        write!(f, " INTO {} BUCKETS", self.num_buckets)
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
    /// Index type used in the statement. Can also be found inside [`CreateIndex::index_options`]
    /// depending on the position of the option within the statement.
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
    pub index_options: Vec<IndexOption>,
    /// [MySQL] allows a subset of options normally used for `ALTER TABLE`:
    ///
    /// - `ALGORITHM`
    /// - `LOCK`
    ///
    /// [MySQL]: https://dev.mysql.com/doc/refman/8.4/en/create-index.html
    pub alter_options: Vec<AlterTableOperation>,
}

impl fmt::Display for CreateIndex {
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
        write!(f, "({})", display_comma_separated(&self.columns))?;
        if !self.include.is_empty() {
            write!(f, " INCLUDE ({})", display_comma_separated(&self.include))?;
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
        if !self.index_options.is_empty() {
            write!(f, " {}", display_separated(&self.index_options, " "))?;
        }
        if !self.alter_options.is_empty() {
            write!(f, " {}", display_separated(&self.alter_options, " "))?;
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
    pub dynamic: bool,
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
    pub like: Option<CreateTableLikeKind>,
    pub clone: Option<ObjectName>,
    pub version: Option<TableVersion>,
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
    /// Snowflake "TARGET_LAG" clause for dybamic tables
    /// <https://docs.snowflake.com/en/sql-reference/sql/create-dynamic-table>
    pub target_lag: Option<String>,
    /// Snowflake "WAREHOUSE" clause for dybamic tables
    /// <https://docs.snowflake.com/en/sql-reference/sql/create-dynamic-table>
    pub warehouse: Option<Ident>,
    /// Snowflake "REFRESH_MODE" clause for dybamic tables
    /// <https://docs.snowflake.com/en/sql-reference/sql/create-dynamic-table>
    pub refresh_mode: Option<RefreshModeKind>,
    /// Snowflake "INITIALIZE" clause for dybamic tables
    /// <https://docs.snowflake.com/en/sql-reference/sql/create-dynamic-table>
    pub initialize: Option<InitializeKind>,
    /// Snowflake "REQUIRE USER" clause for dybamic tables
    /// <https://docs.snowflake.com/en/sql-reference/sql/create-dynamic-table>
    pub require_user: bool,
}

impl fmt::Display for CreateTable {
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
            "CREATE {or_replace}{external}{global}{temporary}{transient}{volatile}{dynamic}{iceberg}TABLE {if_not_exists}{name}",
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
            dynamic = if self.dynamic { "DYNAMIC " } else { "" },
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
        } else if let Some(CreateTableLikeKind::Parenthesized(like_in_columns_list)) = &self.like {
            write!(f, " ({like_in_columns_list})")?;
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

        if let Some(CreateTableLikeKind::Plain(like)) = &self.like {
            write!(f, " {like}")?;
        }

        if let Some(c) = &self.clone {
            write!(f, " CLONE {c}")?;
        }

        if let Some(version) = &self.version {
            write!(f, " {version}")?;
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
            write!(f, " EXTERNAL_VOLUME='{external_volume}'")?;
        }

        if let Some(catalog) = self.catalog.as_ref() {
            write!(f, " CATALOG='{catalog}'")?;
        }

        if self.iceberg {
            if let Some(base_location) = self.base_location.as_ref() {
                write!(f, " BASE_LOCATION='{base_location}'")?;
            }
        }

        if let Some(catalog_sync) = self.catalog_sync.as_ref() {
            write!(f, " CATALOG_SYNC='{catalog_sync}'")?;
        }

        if let Some(storage_serialization_policy) = self.storage_serialization_policy.as_ref() {
            write!(
                f,
                " STORAGE_SERIALIZATION_POLICY={storage_serialization_policy}"
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

        if let Some(target_lag) = &self.target_lag {
            write!(f, " TARGET_LAG='{target_lag}'")?;
        }

        if let Some(warehouse) = &self.warehouse {
            write!(f, " WAREHOUSE={warehouse}")?;
        }

        if let Some(refresh_mode) = &self.refresh_mode {
            write!(f, " REFRESH_MODE={refresh_mode}")?;
        }

        if let Some(initialize) = &self.initialize {
            write!(f, " INITIALIZE={initialize}")?;
        }

        if self.require_user {
            write!(f, " REQUIRE USER")?;
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

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
/// ```sql
/// CREATE DOMAIN name [ AS ] data_type
///         [ COLLATE collation ]
///         [ DEFAULT expression ]
///         [ domain_constraint [ ... ] ]
///
///     where domain_constraint is:
///
///     [ CONSTRAINT constraint_name ]
///     { NOT NULL | NULL | CHECK (expression) }
/// ```
/// See [PostgreSQL](https://www.postgresql.org/docs/current/sql-createdomain.html)
pub struct CreateDomain {
    /// The name of the domain to be created.
    pub name: ObjectName,
    /// The data type of the domain.
    pub data_type: DataType,
    /// The collation of the domain.
    pub collation: Option<Ident>,
    /// The default value of the domain.
    pub default: Option<Expr>,
    /// The constraints of the domain.
    pub constraints: Vec<TableConstraint>,
}

impl fmt::Display for CreateDomain {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "CREATE DOMAIN {name} AS {data_type}",
            name = self.name,
            data_type = self.data_type
        )?;
        if let Some(collation) = &self.collation {
            write!(f, " COLLATE {collation}")?;
        }
        if let Some(default) = &self.default {
            write!(f, " DEFAULT {default}")?;
        }
        if !self.constraints.is_empty() {
            write!(f, " {}", display_separated(&self.constraints, " "))?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct CreateFunction {
    /// True if this is a `CREATE OR ALTER FUNCTION` statement
    ///
    /// [MsSql](https://learn.microsoft.com/en-us/sql/t-sql/statements/create-function-transact-sql?view=sql-server-ver16#or-alter)
    pub or_alter: bool,
    pub or_replace: bool,
    pub temporary: bool,
    pub if_not_exists: bool,
    pub name: ObjectName,
    pub args: Option<Vec<OperateFunctionArg>>,
    pub return_type: Option<DataType>,
    /// The expression that defines the function.
    ///
    /// Examples:
    /// ```sql
    /// AS ((SELECT 1))
    /// AS "console.log();"
    /// ```
    pub function_body: Option<CreateFunctionBody>,
    /// Behavior attribute for the function
    ///
    /// IMMUTABLE | STABLE | VOLATILE
    ///
    /// [PostgreSQL](https://www.postgresql.org/docs/current/sql-createfunction.html)
    pub behavior: Option<FunctionBehavior>,
    /// CALLED ON NULL INPUT | RETURNS NULL ON NULL INPUT | STRICT
    ///
    /// [PostgreSQL](https://www.postgresql.org/docs/current/sql-createfunction.html)
    pub called_on_null: Option<FunctionCalledOnNull>,
    /// PARALLEL { UNSAFE | RESTRICTED | SAFE }
    ///
    /// [PostgreSQL](https://www.postgresql.org/docs/current/sql-createfunction.html)
    pub parallel: Option<FunctionParallel>,
    /// USING ... (Hive only)
    pub using: Option<CreateFunctionUsing>,
    /// Language used in a UDF definition.
    ///
    /// Example:
    /// ```sql
    /// CREATE FUNCTION foo() LANGUAGE js AS "console.log();"
    /// ```
    /// [BigQuery](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#create_a_javascript_udf)
    pub language: Option<Ident>,
    /// Determinism keyword used for non-sql UDF definitions.
    ///
    /// [BigQuery](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#syntax_11)
    pub determinism_specifier: Option<FunctionDeterminismSpecifier>,
    /// List of options for creating the function.
    ///
    /// [BigQuery](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#syntax_11)
    pub options: Option<Vec<SqlOption>>,
    /// Connection resource for a remote function.
    ///
    /// Example:
    /// ```sql
    /// CREATE FUNCTION foo()
    /// RETURNS FLOAT64
    /// REMOTE WITH CONNECTION us.myconnection
    /// ```
    /// [BigQuery](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#create_a_remote_function)
    pub remote_connection: Option<ObjectName>,
}

impl fmt::Display for CreateFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "CREATE {or_alter}{or_replace}{temp}FUNCTION {if_not_exists}{name}",
            name = self.name,
            temp = if self.temporary { "TEMPORARY " } else { "" },
            or_alter = if self.or_alter { "OR ALTER " } else { "" },
            or_replace = if self.or_replace { "OR REPLACE " } else { "" },
            if_not_exists = if self.if_not_exists {
                "IF NOT EXISTS "
            } else {
                ""
            },
        )?;
        if let Some(args) = &self.args {
            write!(f, "({})", display_comma_separated(args))?;
        }
        if let Some(return_type) = &self.return_type {
            write!(f, " RETURNS {return_type}")?;
        }
        if let Some(determinism_specifier) = &self.determinism_specifier {
            write!(f, " {determinism_specifier}")?;
        }
        if let Some(language) = &self.language {
            write!(f, " LANGUAGE {language}")?;
        }
        if let Some(behavior) = &self.behavior {
            write!(f, " {behavior}")?;
        }
        if let Some(called_on_null) = &self.called_on_null {
            write!(f, " {called_on_null}")?;
        }
        if let Some(parallel) = &self.parallel {
            write!(f, " {parallel}")?;
        }
        if let Some(remote_connection) = &self.remote_connection {
            write!(f, " REMOTE WITH CONNECTION {remote_connection}")?;
        }
        if let Some(CreateFunctionBody::AsBeforeOptions(function_body)) = &self.function_body {
            write!(f, " AS {function_body}")?;
        }
        if let Some(CreateFunctionBody::Return(function_body)) = &self.function_body {
            write!(f, " RETURN {function_body}")?;
        }
        if let Some(CreateFunctionBody::AsReturnExpr(function_body)) = &self.function_body {
            write!(f, " AS RETURN {function_body}")?;
        }
        if let Some(CreateFunctionBody::AsReturnSelect(function_body)) = &self.function_body {
            write!(f, " AS RETURN {function_body}")?;
        }
        if let Some(using) = &self.using {
            write!(f, " {using}")?;
        }
        if let Some(options) = &self.options {
            write!(
                f,
                " OPTIONS({})",
                display_comma_separated(options.as_slice())
            )?;
        }
        if let Some(CreateFunctionBody::AsAfterOptions(function_body)) = &self.function_body {
            write!(f, " AS {function_body}")?;
        }
        if let Some(CreateFunctionBody::AsBeginEnd(bes)) = &self.function_body {
            write!(f, " AS {bes}")?;
        }
        Ok(())
    }
}

/// ```sql
/// CREATE CONNECTOR [IF NOT EXISTS] connector_name
/// [TYPE datasource_type]
/// [URL datasource_url]
/// [COMMENT connector_comment]
/// [WITH DCPROPERTIES(property_name=property_value, ...)]
/// ```
///
/// [Hive](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=27362034#LanguageManualDDL-CreateDataConnectorCreateConnector)
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct CreateConnector {
    pub name: Ident,
    pub if_not_exists: bool,
    pub connector_type: Option<String>,
    pub url: Option<String>,
    pub comment: Option<CommentDef>,
    pub with_dcproperties: Option<Vec<SqlOption>>,
}

impl fmt::Display for CreateConnector {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "CREATE CONNECTOR {if_not_exists}{name}",
            if_not_exists = if self.if_not_exists {
                "IF NOT EXISTS "
            } else {
                ""
            },
            name = self.name,
        )?;

        if let Some(connector_type) = &self.connector_type {
            write!(f, " TYPE '{connector_type}'")?;
        }

        if let Some(url) = &self.url {
            write!(f, " URL '{url}'")?;
        }

        if let Some(comment) = &self.comment {
            write!(f, " COMMENT = '{comment}'")?;
        }

        if let Some(with_dcproperties) = &self.with_dcproperties {
            write!(
                f,
                " WITH DCPROPERTIES({})",
                display_comma_separated(with_dcproperties)
            )?;
        }

        Ok(())
    }
}

/// An `ALTER SCHEMA` (`Statement::AlterSchema`) operation.
///
/// See [BigQuery](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#alter_schema_collate_statement)
/// See [PostgreSQL](https://www.postgresql.org/docs/current/sql-alterschema.html)
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum AlterSchemaOperation {
    SetDefaultCollate {
        collate: Expr,
    },
    AddReplica {
        replica: Ident,
        options: Option<Vec<SqlOption>>,
    },
    DropReplica {
        replica: Ident,
    },
    SetOptionsParens {
        options: Vec<SqlOption>,
    },
    Rename {
        name: ObjectName,
    },
    OwnerTo {
        owner: Owner,
    },
}

impl fmt::Display for AlterSchemaOperation {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            AlterSchemaOperation::SetDefaultCollate { collate } => {
                write!(f, "SET DEFAULT COLLATE {collate}")
            }
            AlterSchemaOperation::AddReplica { replica, options } => {
                write!(f, "ADD REPLICA {replica}")?;
                if let Some(options) = options {
                    write!(f, " OPTIONS ({})", display_comma_separated(options))?;
                }
                Ok(())
            }
            AlterSchemaOperation::DropReplica { replica } => write!(f, "DROP REPLICA {replica}"),
            AlterSchemaOperation::SetOptionsParens { options } => {
                write!(f, "SET OPTIONS ({})", display_comma_separated(options))
            }
            AlterSchemaOperation::Rename { name } => write!(f, "RENAME TO {name}"),
            AlterSchemaOperation::OwnerTo { owner } => write!(f, "OWNER TO {owner}"),
        }
    }
}
/// `RenameTableNameKind` is the kind used in an `ALTER TABLE _ RENAME` statement.
///
/// Note: [MySQL] is the only database that supports the AS keyword for this operation.
///
/// [MySQL]: https://dev.mysql.com/doc/refman/8.4/en/alter-table.html
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum RenameTableNameKind {
    As(ObjectName),
    To(ObjectName),
}

impl fmt::Display for RenameTableNameKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            RenameTableNameKind::As(name) => write!(f, "AS {name}"),
            RenameTableNameKind::To(name) => write!(f, "TO {name}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct AlterSchema {
    pub name: ObjectName,
    pub if_exists: bool,
    pub operations: Vec<AlterSchemaOperation>,
}

impl fmt::Display for AlterSchema {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ALTER SCHEMA ")?;
        if self.if_exists {
            write!(f, "IF EXISTS ")?;
        }
        write!(f, "{}", self.name)?;
        for operation in &self.operations {
            write!(f, " {operation}")?;
        }

        Ok(())
    }
}

impl Spanned for RenameTableNameKind {
    fn span(&self) -> Span {
        match self {
            RenameTableNameKind::As(name) => name.span(),
            RenameTableNameKind::To(name) => name.span(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
/// Whether the syntax used for the trigger object (ROW or STATEMENT) is `FOR` or `FOR EACH`.
pub enum TriggerObjectKind {
    /// The `FOR` syntax is used.
    For(TriggerObject),
    /// The `FOR EACH` syntax is used.
    ForEach(TriggerObject),
}

impl Display for TriggerObjectKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TriggerObjectKind::For(obj) => write!(f, "FOR {obj}"),
            TriggerObjectKind::ForEach(obj) => write!(f, "FOR EACH {obj}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
/// CREATE TRIGGER
///
/// Examples:
///
/// ```sql
/// CREATE TRIGGER trigger_name
/// BEFORE INSERT ON table_name
/// FOR EACH ROW
/// EXECUTE FUNCTION trigger_function();
/// ```
///
/// Postgres: <https://www.postgresql.org/docs/current/sql-createtrigger.html>
/// SQL Server: <https://learn.microsoft.com/en-us/sql/t-sql/statements/create-trigger-transact-sql>
pub struct CreateTrigger {
    /// True if this is a `CREATE OR ALTER TRIGGER` statement
    ///
    /// [MsSql](https://learn.microsoft.com/en-us/sql/t-sql/statements/create-trigger-transact-sql?view=sql-server-ver16#arguments)
    pub or_alter: bool,
    /// True if this is a temporary trigger.
    ///
    /// Examples:
    ///
    /// ```sql
    /// CREATE TEMP TRIGGER trigger_name
    /// ```
    ///
    /// or
    ///
    /// ```sql
    /// CREATE TEMPORARY TRIGGER trigger_name;
    /// CREATE TEMP TRIGGER trigger_name;
    /// ```
    ///
    /// [SQLite](https://sqlite.org/lang_createtrigger.html#temp_triggers_on_non_temp_tables)
    pub temporary: bool,
    /// The `OR REPLACE` clause is used to re-create the trigger if it already exists.
    ///
    /// Example:
    /// ```sql
    /// CREATE OR REPLACE TRIGGER trigger_name
    /// AFTER INSERT ON table_name
    /// FOR EACH ROW
    /// EXECUTE FUNCTION trigger_function();
    /// ```
    pub or_replace: bool,
    /// The `CONSTRAINT` keyword is used to create a trigger as a constraint.
    pub is_constraint: bool,
    /// The name of the trigger to be created.
    pub name: ObjectName,
    /// Determines whether the function is called before, after, or instead of the event.
    ///
    /// Example of BEFORE:
    ///
    /// ```sql
    /// CREATE TRIGGER trigger_name
    /// BEFORE INSERT ON table_name
    /// FOR EACH ROW
    /// EXECUTE FUNCTION trigger_function();
    /// ```
    ///
    /// Example of AFTER:
    ///
    /// ```sql
    /// CREATE TRIGGER trigger_name
    /// AFTER INSERT ON table_name
    /// FOR EACH ROW
    /// EXECUTE FUNCTION trigger_function();
    /// ```
    ///
    /// Example of INSTEAD OF:
    ///
    /// ```sql
    /// CREATE TRIGGER trigger_name
    /// INSTEAD OF INSERT ON table_name
    /// FOR EACH ROW
    /// EXECUTE FUNCTION trigger_function();
    /// ```
    pub period: Option<TriggerPeriod>,
    /// Whether the trigger period was specified before the target table name.
    /// This does not refer to whether the period is BEFORE, AFTER, or INSTEAD OF,
    /// but rather the position of the period clause in relation to the table name.
    ///
    /// ```sql
    /// -- period_before_table == true: Postgres, MySQL, and standard SQL
    /// CREATE TRIGGER t BEFORE INSERT ON table_name ...;
    /// -- period_before_table == false: MSSQL
    /// CREATE TRIGGER t ON table_name BEFORE INSERT ...;
    /// ```
    pub period_before_table: bool,
    /// Multiple events can be specified using OR, such as `INSERT`, `UPDATE`, `DELETE`, or `TRUNCATE`.
    pub events: Vec<TriggerEvent>,
    /// The table on which the trigger is to be created.
    pub table_name: ObjectName,
    /// The optional referenced table name that can be referenced via
    /// the `FROM` keyword.
    pub referenced_table_name: Option<ObjectName>,
    /// This keyword immediately precedes the declaration of one or two relation names that provide access to the transition relations of the triggering statement.
    pub referencing: Vec<TriggerReferencing>,
    /// This specifies whether the trigger function should be fired once for
    /// every row affected by the trigger event, or just once per SQL statement.
    /// This is optional in some SQL dialects, such as SQLite, and if not specified, in
    /// those cases, the implied default is `FOR EACH ROW`.
    pub trigger_object: Option<TriggerObjectKind>,
    ///  Triggering conditions
    pub condition: Option<Expr>,
    /// Execute logic block
    pub exec_body: Option<TriggerExecBody>,
    /// For MSSQL and dialects where statements are preceded by `AS`
    pub statements_as: bool,
    /// For SQL dialects with statement(s) for a body
    pub statements: Option<ConditionalStatements>,
    /// The characteristic of the trigger, which include whether the trigger is `DEFERRABLE`, `INITIALLY DEFERRED`, or `INITIALLY IMMEDIATE`,
    pub characteristics: Option<ConstraintCharacteristics>,
}

impl Display for CreateTrigger {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let CreateTrigger {
            or_alter,
            temporary,
            or_replace,
            is_constraint,
            name,
            period_before_table,
            period,
            events,
            table_name,
            referenced_table_name,
            referencing,
            trigger_object,
            condition,
            exec_body,
            statements_as,
            statements,
            characteristics,
        } = self;
        write!(
            f,
            "CREATE {temporary}{or_alter}{or_replace}{is_constraint}TRIGGER {name} ",
            temporary = if *temporary { "TEMPORARY " } else { "" },
            or_alter = if *or_alter { "OR ALTER " } else { "" },
            or_replace = if *or_replace { "OR REPLACE " } else { "" },
            is_constraint = if *is_constraint { "CONSTRAINT " } else { "" },
        )?;

        if *period_before_table {
            if let Some(p) = period {
                write!(f, "{p} ")?;
            }
            if !events.is_empty() {
                write!(f, "{} ", display_separated(events, " OR "))?;
            }
            write!(f, "ON {table_name}")?;
        } else {
            write!(f, "ON {table_name} ")?;
            if let Some(p) = period {
                write!(f, "{p}")?;
            }
            if !events.is_empty() {
                write!(f, " {}", display_separated(events, ", "))?;
            }
        }

        if let Some(referenced_table_name) = referenced_table_name {
            write!(f, " FROM {referenced_table_name}")?;
        }

        if let Some(characteristics) = characteristics {
            write!(f, " {characteristics}")?;
        }

        if !referencing.is_empty() {
            write!(f, " REFERENCING {}", display_separated(referencing, " "))?;
        }

        if let Some(trigger_object) = trigger_object {
            write!(f, " {trigger_object}")?;
        }
        if let Some(condition) = condition {
            write!(f, " WHEN {condition}")?;
        }
        if let Some(exec_body) = exec_body {
            write!(f, " EXECUTE {exec_body}")?;
        }
        if let Some(statements) = statements {
            if *statements_as {
                write!(f, " AS")?;
            }
            write!(f, " {statements}")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
/// DROP TRIGGER
///
/// ```sql
/// DROP TRIGGER [ IF EXISTS ] name ON table_name [ CASCADE | RESTRICT ]
/// ```
///
pub struct DropTrigger {
    /// Whether to include the `IF EXISTS` clause.
    pub if_exists: bool,
    /// The name of the trigger to be dropped.
    pub trigger_name: ObjectName,
    /// The name of the table from which the trigger is to be dropped.
    pub table_name: Option<ObjectName>,
    /// `CASCADE` or `RESTRICT`
    pub option: Option<ReferentialAction>,
}

impl fmt::Display for DropTrigger {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let DropTrigger {
            if_exists,
            trigger_name,
            table_name,
            option,
        } = self;
        write!(f, "DROP TRIGGER")?;
        if *if_exists {
            write!(f, " IF EXISTS")?;
        }
        match &table_name {
            Some(table_name) => write!(f, " {trigger_name} ON {table_name}")?,
            None => write!(f, " {trigger_name}")?,
        };
        if let Some(option) = option {
            write!(f, " {option}")?;
        }
        Ok(())
    }
}

/// A `TRUNCATE` statement.
///
/// ```sql
/// TRUNCATE TABLE table_names [PARTITION (partitions)] [RESTART IDENTITY | CONTINUE IDENTITY] [CASCADE | RESTRICT] [ON CLUSTER cluster_name]
/// ```
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct Truncate {
    /// Table names to truncate
    pub table_names: Vec<super::TruncateTableTarget>,
    /// Optional partition specification
    pub partitions: Option<Vec<Expr>>,
    /// TABLE - optional keyword
    pub table: bool,
    /// Postgres-specific option: [ RESTART IDENTITY | CONTINUE IDENTITY ]
    pub identity: Option<super::TruncateIdentityOption>,
    /// Postgres-specific option: [ CASCADE | RESTRICT ]
    pub cascade: Option<super::CascadeOption>,
    /// ClickHouse-specific option: [ ON CLUSTER cluster_name ]
    /// [ClickHouse](https://clickhouse.com/docs/en/sql-reference/statements/truncate/)
    pub on_cluster: Option<Ident>,
}

impl fmt::Display for Truncate {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let table = if self.table { "TABLE " } else { "" };

        write!(
            f,
            "TRUNCATE {table}{table_names}",
            table_names = display_comma_separated(&self.table_names)
        )?;

        if let Some(identity) = &self.identity {
            match identity {
                super::TruncateIdentityOption::Restart => write!(f, " RESTART IDENTITY")?,
                super::TruncateIdentityOption::Continue => write!(f, " CONTINUE IDENTITY")?,
            }
        }
        if let Some(cascade) = &self.cascade {
            match cascade {
                super::CascadeOption::Cascade => write!(f, " CASCADE")?,
                super::CascadeOption::Restrict => write!(f, " RESTRICT")?,
            }
        }

        if let Some(ref parts) = &self.partitions {
            if !parts.is_empty() {
                write!(f, " PARTITION ({})", display_comma_separated(parts))?;
            }
        }
        if let Some(on_cluster) = &self.on_cluster {
            write!(f, " ON CLUSTER {on_cluster}")?;
        }
        Ok(())
    }
}

impl Spanned for Truncate {
    fn span(&self) -> Span {
        Span::union_iter(
            self.table_names.iter().map(|i| i.name.span()).chain(
                self.partitions
                    .iter()
                    .flat_map(|i| i.iter().map(|k| k.span())),
            ),
        )
    }
}

/// An `MSCK` statement.
///
/// ```sql
/// MSCK [REPAIR] TABLE table_name [ADD|DROP|SYNC PARTITIONS]
/// ```
/// MSCK (Hive) - MetaStore Check command
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct Msck {
    /// Table name to check
    #[cfg_attr(feature = "visitor", visit(with = "visit_relation"))]
    pub table_name: ObjectName,
    /// Whether to repair the table
    pub repair: bool,
    /// Partition action (ADD, DROP, or SYNC)
    pub partition_action: Option<super::AddDropSync>,
}

impl fmt::Display for Msck {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "MSCK {repair}TABLE {table}",
            repair = if self.repair { "REPAIR " } else { "" },
            table = self.table_name
        )?;
        if let Some(pa) = &self.partition_action {
            write!(f, " {pa}")?;
        }
        Ok(())
    }
}

impl Spanned for Msck {
    fn span(&self) -> Span {
        self.table_name.span()
    }
}

/// CREATE VIEW statement.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct CreateView {
    /// True if this is a `CREATE OR ALTER VIEW` statement
    ///
    /// [MsSql](https://learn.microsoft.com/en-us/sql/t-sql/statements/create-view-transact-sql)
    pub or_alter: bool,
    pub or_replace: bool,
    pub materialized: bool,
    /// Snowflake: SECURE view modifier
    /// <https://docs.snowflake.com/en/sql-reference/sql/create-view#syntax>
    pub secure: bool,
    /// View name
    pub name: ObjectName,
    /// If `if_not_exists` is true, this flag is set to true if the view name comes before the `IF NOT EXISTS` clause.
    /// Example:
    /// ```sql
    /// CREATE VIEW myview IF NOT EXISTS AS SELECT 1`
    ///  ```
    /// Otherwise, the flag is set to false if the view name comes after the clause
    /// Example:
    /// ```sql
    /// CREATE VIEW IF NOT EXISTS myview AS SELECT 1`
    ///  ```
    pub name_before_not_exists: bool,
    pub columns: Vec<ViewColumnDef>,
    pub query: Box<Query>,
    pub options: CreateTableOptions,
    pub cluster_by: Vec<Ident>,
    /// Snowflake: Views can have comments in Snowflake.
    /// <https://docs.snowflake.com/en/sql-reference/sql/create-view#syntax>
    pub comment: Option<String>,
    /// if true, has RedShift [`WITH NO SCHEMA BINDING`] clause <https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_VIEW.html>
    pub with_no_schema_binding: bool,
    /// if true, has SQLite `IF NOT EXISTS` clause <https://www.sqlite.org/lang_createview.html>
    pub if_not_exists: bool,
    /// if true, has SQLite `TEMP` or `TEMPORARY` clause <https://www.sqlite.org/lang_createview.html>
    pub temporary: bool,
    /// if not None, has Clickhouse `TO` clause, specify the table into which to insert results
    /// <https://clickhouse.com/docs/en/sql-reference/statements/create/view#materialized-view>
    pub to: Option<ObjectName>,
    /// MySQL: Optional parameters for the view algorithm, definer, and security context
    pub params: Option<CreateViewParams>,
}

impl fmt::Display for CreateView {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "CREATE {or_alter}{or_replace}",
            or_alter = if self.or_alter { "OR ALTER " } else { "" },
            or_replace = if self.or_replace { "OR REPLACE " } else { "" },
        )?;
        if let Some(ref params) = self.params {
            params.fmt(f)?;
        }
        write!(
            f,
            "{secure}{materialized}{temporary}VIEW {if_not_and_name}{to}",
            if_not_and_name = if self.if_not_exists {
                if self.name_before_not_exists {
                    format!("{} IF NOT EXISTS", self.name)
                } else {
                    format!("IF NOT EXISTS {}", self.name)
                }
            } else {
                format!("{}", self.name)
            },
            secure = if self.secure { "SECURE " } else { "" },
            materialized = if self.materialized {
                "MATERIALIZED "
            } else {
                ""
            },
            temporary = if self.temporary { "TEMPORARY " } else { "" },
            to = self
                .to
                .as_ref()
                .map(|to| format!(" TO {to}"))
                .unwrap_or_default()
        )?;
        if !self.columns.is_empty() {
            write!(f, " ({})", display_comma_separated(&self.columns))?;
        }
        if matches!(self.options, CreateTableOptions::With(_)) {
            write!(f, " {}", self.options)?;
        }
        if let Some(ref comment) = self.comment {
            write!(f, " COMMENT = '{}'", escape_single_quote_string(comment))?;
        }
        if !self.cluster_by.is_empty() {
            write!(
                f,
                " CLUSTER BY ({})",
                display_comma_separated(&self.cluster_by)
            )?;
        }
        if matches!(self.options, CreateTableOptions::Options(_)) {
            write!(f, " {}", self.options)?;
        }
        f.write_str(" AS")?;
        SpaceOrNewline.fmt(f)?;
        self.query.fmt(f)?;
        if self.with_no_schema_binding {
            write!(f, " WITH NO SCHEMA BINDING")?;
        }
        Ok(())
    }
}

/// CREATE EXTENSION statement
/// Note: this is a PostgreSQL-specific statement
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct CreateExtension {
    pub name: Ident,
    pub if_not_exists: bool,
    pub cascade: bool,
    pub schema: Option<Ident>,
    pub version: Option<Ident>,
}

impl fmt::Display for CreateExtension {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "CREATE EXTENSION {if_not_exists}{name}",
            if_not_exists = if self.if_not_exists {
                "IF NOT EXISTS "
            } else {
                ""
            },
            name = self.name
        )?;
        if self.cascade || self.schema.is_some() || self.version.is_some() {
            write!(f, " WITH")?;

            if let Some(name) = &self.schema {
                write!(f, " SCHEMA {name}")?;
            }
            if let Some(version) = &self.version {
                write!(f, " VERSION {version}")?;
            }
            if self.cascade {
                write!(f, " CASCADE")?;
            }
        }

        Ok(())
    }
}

impl Spanned for CreateExtension {
    fn span(&self) -> Span {
        Span::empty()
    }
}

/// DROP EXTENSION statement  
/// Note: this is a PostgreSQL-specific statement
///
/// # References
///
/// PostgreSQL Documentation:
/// <https://www.postgresql.org/docs/current/sql-dropextension.html>
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct DropExtension {
    pub names: Vec<Ident>,
    pub if_exists: bool,
    /// `CASCADE` or `RESTRICT`
    pub cascade_or_restrict: Option<ReferentialAction>,
}

impl fmt::Display for DropExtension {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "DROP EXTENSION")?;
        if self.if_exists {
            write!(f, " IF EXISTS")?;
        }
        write!(f, " {}", display_comma_separated(&self.names))?;
        if let Some(cascade_or_restrict) = &self.cascade_or_restrict {
            write!(f, " {cascade_or_restrict}")?;
        }
        Ok(())
    }
}

impl Spanned for DropExtension {
    fn span(&self) -> Span {
        Span::empty()
    }
}

/// Table type for ALTER TABLE statements.
/// Used to distinguish between regular tables, Iceberg tables, and Dynamic tables.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum AlterTableType {
    /// Iceberg table type
    /// <https://docs.snowflake.com/en/sql-reference/sql/alter-iceberg-table>
    Iceberg,
    /// Dynamic table type
    /// <https://docs.snowflake.com/en/sql-reference/sql/alter-table>
    Dynamic,
}

/// ALTER TABLE statement
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct AlterTable {
    /// Table name
    #[cfg_attr(feature = "visitor", visit(with = "visit_relation"))]
    pub name: ObjectName,
    pub if_exists: bool,
    pub only: bool,
    pub operations: Vec<AlterTableOperation>,
    pub location: Option<HiveSetLocation>,
    /// ClickHouse dialect supports `ON CLUSTER` clause for ALTER TABLE
    /// For example: `ALTER TABLE table_name ON CLUSTER cluster_name ADD COLUMN c UInt32`
    /// [ClickHouse](https://clickhouse.com/docs/en/sql-reference/statements/alter/update)
    pub on_cluster: Option<Ident>,
    /// Table type: None for regular tables, Some(AlterTableType) for Iceberg or Dynamic tables
    pub table_type: Option<AlterTableType>,
    /// Token that represents the end of the statement (semicolon or EOF)
    pub end_token: AttachedToken,
}

impl fmt::Display for AlterTable {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self.table_type {
            Some(AlterTableType::Iceberg) => write!(f, "ALTER ICEBERG TABLE ")?,
            Some(AlterTableType::Dynamic) => write!(f, "ALTER DYNAMIC TABLE ")?,
            None => write!(f, "ALTER TABLE ")?,
        }

        if self.if_exists {
            write!(f, "IF EXISTS ")?;
        }
        if self.only {
            write!(f, "ONLY ")?;
        }
        write!(f, "{} ", &self.name)?;
        if let Some(cluster) = &self.on_cluster {
            write!(f, "ON CLUSTER {cluster} ")?;
        }
        write!(f, "{}", display_comma_separated(&self.operations))?;
        if let Some(loc) = &self.location {
            write!(f, " {loc}")?
        }
        Ok(())
    }
}

/// DROP FUNCTION statement
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct DropFunction {
    pub if_exists: bool,
    /// One or more functions to drop
    pub func_desc: Vec<FunctionDesc>,
    /// `CASCADE` or `RESTRICT`
    pub drop_behavior: Option<DropBehavior>,
}

impl fmt::Display for DropFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "DROP FUNCTION{} {}",
            if self.if_exists { " IF EXISTS" } else { "" },
            display_comma_separated(&self.func_desc),
        )?;
        if let Some(op) = &self.drop_behavior {
            write!(f, " {op}")?;
        }
        Ok(())
    }
}

impl Spanned for DropFunction {
    fn span(&self) -> Span {
        Span::empty()
    }
}

/// CREATE OPERATOR statement
/// See <https://www.postgresql.org/docs/current/sql-createoperator.html>
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct CreateOperator {
    /// Operator name (can be schema-qualified)
    pub name: ObjectName,
    /// FUNCTION or PROCEDURE parameter (function name)
    pub function: ObjectName,
    /// Whether PROCEDURE keyword was used (vs FUNCTION)
    pub is_procedure: bool,
    /// LEFTARG parameter (left operand type)
    pub left_arg: Option<DataType>,
    /// RIGHTARG parameter (right operand type)
    pub right_arg: Option<DataType>,
    /// COMMUTATOR parameter (commutator operator)
    pub commutator: Option<ObjectName>,
    /// NEGATOR parameter (negator operator)
    pub negator: Option<ObjectName>,
    /// RESTRICT parameter (restriction selectivity function)
    pub restrict: Option<ObjectName>,
    /// JOIN parameter (join selectivity function)
    pub join: Option<ObjectName>,
    /// HASHES flag
    pub hashes: bool,
    /// MERGES flag
    pub merges: bool,
}

/// CREATE OPERATOR FAMILY statement
/// See <https://www.postgresql.org/docs/current/sql-createopfamily.html>
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct CreateOperatorFamily {
    /// Operator family name (can be schema-qualified)
    pub name: ObjectName,
    /// Index method (btree, hash, gist, gin, etc.)
    pub using: Ident,
}

/// CREATE OPERATOR CLASS statement
/// See <https://www.postgresql.org/docs/current/sql-createopclass.html>
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct CreateOperatorClass {
    /// Operator class name (can be schema-qualified)
    pub name: ObjectName,
    /// Whether this is the default operator class for the type
    pub default: bool,
    /// The data type
    pub for_type: DataType,
    /// Index method (btree, hash, gist, gin, etc.)
    pub using: Ident,
    /// Optional operator family name
    pub family: Option<ObjectName>,
    /// List of operator class items (operators, functions, storage)
    pub items: Vec<OperatorClassItem>,
}

impl fmt::Display for CreateOperator {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CREATE OPERATOR {} (", self.name)?;

        let function_keyword = if self.is_procedure {
            "PROCEDURE"
        } else {
            "FUNCTION"
        };
        let mut params = vec![format!("{} = {}", function_keyword, self.function)];

        if let Some(left_arg) = &self.left_arg {
            params.push(format!("LEFTARG = {}", left_arg));
        }
        if let Some(right_arg) = &self.right_arg {
            params.push(format!("RIGHTARG = {}", right_arg));
        }
        if let Some(commutator) = &self.commutator {
            params.push(format!("COMMUTATOR = {}", commutator));
        }
        if let Some(negator) = &self.negator {
            params.push(format!("NEGATOR = {}", negator));
        }
        if let Some(restrict) = &self.restrict {
            params.push(format!("RESTRICT = {}", restrict));
        }
        if let Some(join) = &self.join {
            params.push(format!("JOIN = {}", join));
        }
        if self.hashes {
            params.push("HASHES".to_string());
        }
        if self.merges {
            params.push("MERGES".to_string());
        }

        write!(f, "{}", params.join(", "))?;
        write!(f, ")")
    }
}

impl fmt::Display for CreateOperatorFamily {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "CREATE OPERATOR FAMILY {} USING {}",
            self.name, self.using
        )
    }
}

impl fmt::Display for CreateOperatorClass {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CREATE OPERATOR CLASS {}", self.name)?;
        if self.default {
            write!(f, " DEFAULT")?;
        }
        write!(f, " FOR TYPE {} USING {}", self.for_type, self.using)?;
        if let Some(family) = &self.family {
            write!(f, " FAMILY {}", family)?;
        }
        write!(f, " AS {}", display_comma_separated(&self.items))
    }
}

/// Operator argument types for CREATE OPERATOR CLASS
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct OperatorArgTypes {
    pub left: DataType,
    pub right: DataType,
}

impl fmt::Display for OperatorArgTypes {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}, {}", self.left, self.right)
    }
}

/// An item in a CREATE OPERATOR CLASS statement
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum OperatorClassItem {
    /// OPERATOR clause
    Operator {
        strategy_number: u32,
        operator_name: ObjectName,
        /// Optional operator argument types
        op_types: Option<OperatorArgTypes>,
        /// FOR SEARCH or FOR ORDER BY
        purpose: Option<OperatorPurpose>,
    },
    /// FUNCTION clause
    Function {
        support_number: u32,
        /// Optional function argument types for the operator class
        op_types: Option<Vec<DataType>>,
        function_name: ObjectName,
        /// Function argument types
        argument_types: Vec<DataType>,
    },
    /// STORAGE clause
    Storage { storage_type: DataType },
}

/// Purpose of an operator in an operator class
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum OperatorPurpose {
    ForSearch,
    ForOrderBy { sort_family: ObjectName },
}

impl fmt::Display for OperatorClassItem {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            OperatorClassItem::Operator {
                strategy_number,
                operator_name,
                op_types,
                purpose,
            } => {
                write!(f, "OPERATOR {strategy_number} {operator_name}")?;
                if let Some(types) = op_types {
                    write!(f, " ({types})")?;
                }
                if let Some(purpose) = purpose {
                    write!(f, " {purpose}")?;
                }
                Ok(())
            }
            OperatorClassItem::Function {
                support_number,
                op_types,
                function_name,
                argument_types,
            } => {
                write!(f, "FUNCTION {support_number}")?;
                if let Some(types) = op_types {
                    write!(f, " ({})", display_comma_separated(types))?;
                }
                write!(f, " {function_name}")?;
                if !argument_types.is_empty() {
                    write!(f, "({})", display_comma_separated(argument_types))?;
                }
                Ok(())
            }
            OperatorClassItem::Storage { storage_type } => {
                write!(f, "STORAGE {storage_type}")
            }
        }
    }
}

impl fmt::Display for OperatorPurpose {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            OperatorPurpose::ForSearch => write!(f, "FOR SEARCH"),
            OperatorPurpose::ForOrderBy { sort_family } => {
                write!(f, "FOR ORDER BY {sort_family}")
            }
        }
    }
}

/// `DROP OPERATOR` statement
/// See <https://www.postgresql.org/docs/current/sql-dropoperator.html>
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct DropOperator {
    /// `IF EXISTS` clause
    pub if_exists: bool,
    /// One or more operators to drop with their signatures
    pub operators: Vec<OperatorSignature>,
    /// `CASCADE or RESTRICT`
    pub drop_behavior: Option<DropBehavior>,
}

/// Operator signature for a `DROP OPERATOR` statement
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct OperatorSignature {
    /// Operator name (can be schema-qualified)
    pub name: ObjectName,
    /// Left operand type (None for prefix operators)
    pub left_type: Option<DataType>,
    /// Right operand type (always required)
    pub right_type: DataType,
}

impl fmt::Display for OperatorSignature {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} (", self.name)?;
        if let Some(left_type) = &self.left_type {
            write!(f, "{}", left_type)?;
        } else {
            write!(f, "NONE")?;
        }
        write!(f, ", {})", self.right_type)
    }
}

impl fmt::Display for DropOperator {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "DROP OPERATOR")?;
        if self.if_exists {
            write!(f, " IF EXISTS")?;
        }
        write!(f, " {}", display_comma_separated(&self.operators))?;
        if let Some(drop_behavior) = &self.drop_behavior {
            write!(f, " {}", drop_behavior)?;
        }
        Ok(())
    }
}

impl Spanned for DropOperator {
    fn span(&self) -> Span {
        Span::empty()
    }
}

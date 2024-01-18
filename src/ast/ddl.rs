// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! AST types specific to CREATE/ALTER variants of [`Statement`](crate::ast::Statement)
//! (commonly referred to as Data Definition Language, or DDL)

#[cfg(not(feature = "std"))]
use alloc::{boxed::Box, string::String, vec::Vec};
use core::fmt;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

#[cfg(feature = "visitor")]
use sqlparser_derive::{Visit, VisitMut};

use crate::ast::value::escape_single_quote_string;
use crate::ast::{
    display_comma_separated, display_separated, DataType, Expr, Ident, ObjectName, SequenceOptions,
};
use crate::tokenizer::Token;

/// An `ALTER TABLE` (`Statement::AlterTable`) operation
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum AlterTableOperation {
    /// `ADD <table_constraint>`
    AddConstraint(TableConstraint),
    /// `ADD [COLUMN] [IF NOT EXISTS] <column_def>`
    AddColumn {
        /// `[COLUMN]`.
        column_keyword: bool,
        /// `[IF NOT EXISTS]`
        if_not_exists: bool,
        /// <column_def>.
        column_def: ColumnDef,
    },
    /// `DROP CONSTRAINT [ IF EXISTS ] <name>`
    DropConstraint {
        if_exists: bool,
        name: Ident,
        cascade: bool,
    },
    /// `DROP [ COLUMN ] [ IF EXISTS ] <column_name> [ CASCADE ]`
    DropColumn {
        column_name: Ident,
        if_exists: bool,
        cascade: bool,
    },
    /// `DROP PRIMARY KEY`
    ///
    /// Note: this is a MySQL-specific operation.
    DropPrimaryKey,
    /// `RENAME TO PARTITION (partition=val)`
    RenamePartitions {
        old_partitions: Vec<Expr>,
        new_partitions: Vec<Expr>,
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
    RenameTable { table_name: ObjectName },
    // CHANGE [ COLUMN ] <old_name> <new_name> <data_type> [ <options> ]
    ChangeColumn {
        old_name: Ident,
        new_name: Ident,
        data_type: DataType,
        options: Vec<ColumnOption>,
    },
    /// `RENAME CONSTRAINT <old_constraint_name> TO <new_constraint_name>`
    ///
    /// Note: this is a PostgreSQL-specific operation.
    RenameConstraint { old_name: Ident, new_name: Ident },
    /// `ALTER [ COLUMN ]`
    AlterColumn {
        column_name: Ident,
        op: AlterColumnOperation,
    },
    /// 'SWAP WITH <table_name>'
    ///
    /// Note: this is Snowflake specific <https://docs.snowflake.com/en/sql-reference/sql/alter-table>
    SwapWith { table_name: ObjectName },
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
            AlterTableOperation::AddConstraint(c) => write!(f, "ADD {c}"),
            AlterTableOperation::AddColumn {
                column_keyword,
                if_not_exists,
                column_def,
            } => {
                write!(f, "ADD")?;
                if *column_keyword {
                    write!(f, " COLUMN")?;
                }
                if *if_not_exists {
                    write!(f, " IF NOT EXISTS")?;
                }
                write!(f, " {column_def}")?;

                Ok(())
            }
            AlterTableOperation::AlterColumn { column_name, op } => {
                write!(f, "ALTER COLUMN {column_name} {op}")
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
                cascade,
            } => {
                write!(
                    f,
                    "DROP CONSTRAINT {}{}{}",
                    if *if_exists { "IF EXISTS " } else { "" },
                    name,
                    if *cascade { " CASCADE" } else { "" },
                )
            }
            AlterTableOperation::DropPrimaryKey => write!(f, "DROP PRIMARY KEY"),
            AlterTableOperation::DropColumn {
                column_name,
                if_exists,
                cascade,
            } => write!(
                f,
                "DROP COLUMN {}{}{}",
                if *if_exists { "IF EXISTS " } else { "" },
                column_name,
                if *cascade { " CASCADE" } else { "" }
            ),
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
                write!(f, "RENAME TO {table_name}")
            }
            AlterTableOperation::ChangeColumn {
                old_name,
                new_name,
                data_type,
                options,
            } => {
                write!(f, "CHANGE COLUMN {old_name} {new_name} {data_type}")?;
                if options.is_empty() {
                    Ok(())
                } else {
                    write!(f, " {}", display_separated(options, " "))
                }
            }
            AlterTableOperation::RenameConstraint { old_name, new_name } => {
                write!(f, "RENAME CONSTRAINT {old_name} TO {new_name}")
            }
            AlterTableOperation::SwapWith { table_name } => {
                write!(f, "SWAP WITH {table_name}")
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
            AlterColumnOperation::DropDefault {} => {
                write!(f, "DROP DEFAULT")
            }
            AlterColumnOperation::SetDataType { data_type, using } => {
                if let Some(expr) = using {
                    write!(f, "SET DATA TYPE {data_type} USING {expr}")
                } else {
                    write!(f, "SET DATA TYPE {data_type}")
                }
            }
        }
    }
}

/// A table-level constraint, specified in a `CREATE TABLE` or an
/// `ALTER TABLE ADD <constraint>` statement.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum TableConstraint {
    /// `[ CONSTRAINT <name> ] { PRIMARY KEY | UNIQUE } (<columns>)`
    Unique {
        name: Option<Ident>,
        columns: Vec<Ident>,
        /// Whether this is a `PRIMARY KEY` or just a `UNIQUE` constraint
        is_primary: bool,
    },
    /// A referential integrity constraint (`[ CONSTRAINT <name> ] FOREIGN KEY (<columns>)
    /// REFERENCES <foreign_table> (<referred_columns>)
    /// { [ON DELETE <referential_action>] [ON UPDATE <referential_action>] |
    ///   [ON UPDATE <referential_action>] [ON DELETE <referential_action>]
    /// }`).
    ForeignKey {
        name: Option<Ident>,
        columns: Vec<Ident>,
        foreign_table: ObjectName,
        referred_columns: Vec<Ident>,
        on_delete: Option<ReferentialAction>,
        on_update: Option<ReferentialAction>,
    },
    /// `[ CONSTRAINT <name> ] CHECK (<expr>)`
    Check {
        name: Option<Ident>,
        expr: Box<Expr>,
    },
    /// MySQLs [index definition][1] for index creation. Not present on ANSI so, for now, the usage
    /// is restricted to MySQL, as no other dialects that support this syntax were found.
    ///
    /// `{INDEX | KEY} [index_name] [index_type] (key_part,...) [index_option]...`
    ///
    /// [1]: https://dev.mysql.com/doc/refman/8.0/en/create-table.html
    Index {
        /// Whether this index starts with KEY (true) or INDEX (false), to maintain the same syntax.
        display_as_key: bool,
        /// Index name.
        name: Option<Ident>,
        /// Optional [index type][1].
        ///
        /// [1]: IndexType
        index_type: Option<IndexType>,
        /// Referred column identifier list.
        columns: Vec<Ident>,
    },
    /// MySQLs [fulltext][1] definition. Since the [`SPATIAL`][2] definition is exactly the same,
    /// and MySQL displays both the same way, it is part of this definition as well.
    ///
    /// Supported syntax:
    ///
    /// ```markdown
    /// {FULLTEXT | SPATIAL} [INDEX | KEY] [index_name] (key_part,...)
    ///
    /// key_part: col_name
    /// ```
    ///
    /// [1]: https://dev.mysql.com/doc/refman/8.0/en/fulltext-natural-language.html
    /// [2]: https://dev.mysql.com/doc/refman/8.0/en/spatial-types.html
    FulltextOrSpatial {
        /// Whether this is a `FULLTEXT` (true) or `SPATIAL` (false) definition.
        fulltext: bool,
        /// Whether the type is followed by the keyword `KEY`, `INDEX`, or no keyword at all.
        index_type_display: KeyOrIndexDisplay,
        /// Optional index name.
        opt_index_name: Option<Ident>,
        /// Referred column identifier list.
        columns: Vec<Ident>,
    },
}

impl fmt::Display for TableConstraint {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            TableConstraint::Unique {
                name,
                columns,
                is_primary,
            } => write!(
                f,
                "{}{} ({})",
                display_constraint_name(name),
                if *is_primary { "PRIMARY KEY" } else { "UNIQUE" },
                display_comma_separated(columns)
            ),
            TableConstraint::ForeignKey {
                name,
                columns,
                foreign_table,
                referred_columns,
                on_delete,
                on_update,
            } => {
                write!(
                    f,
                    "{}FOREIGN KEY ({}) REFERENCES {}({})",
                    display_constraint_name(name),
                    display_comma_separated(columns),
                    foreign_table,
                    display_comma_separated(referred_columns),
                )?;
                if let Some(action) = on_delete {
                    write!(f, " ON DELETE {action}")?;
                }
                if let Some(action) = on_update {
                    write!(f, " ON UPDATE {action}")?;
                }
                Ok(())
            }
            TableConstraint::Check { name, expr } => {
                write!(f, "{}CHECK ({})", display_constraint_name(name), expr)
            }
            TableConstraint::Index {
                display_as_key,
                name,
                index_type,
                columns,
            } => {
                write!(f, "{}", if *display_as_key { "KEY" } else { "INDEX" })?;
                if let Some(name) = name {
                    write!(f, " {name}")?;
                }
                if let Some(index_type) = index_type {
                    write!(f, " USING {index_type}")?;
                }
                write!(f, " ({})", display_comma_separated(columns))?;

                Ok(())
            }
            Self::FulltextOrSpatial {
                fulltext,
                index_type_display,
                opt_index_name,
                columns,
            } => {
                if *fulltext {
                    write!(f, "FULLTEXT")?;
                } else {
                    write!(f, "SPATIAL")?;
                }

                if !matches!(index_type_display, KeyOrIndexDisplay::None) {
                    write!(f, " {index_type_display}")?;
                }

                if let Some(name) = opt_index_name {
                    write!(f, " {name}")?;
                }

                write!(f, " ({})", display_comma_separated(columns))?;

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

impl fmt::Display for KeyOrIndexDisplay {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
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
#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum IndexType {
    BTree,
    Hash,
    // TODO add Postgresql's possible indexes
}

impl fmt::Display for IndexType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::BTree => write!(f, "BTREE"),
            Self::Hash => write!(f, "HASH"),
        }
    }
}
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct ProcedureParam {
    pub name: Ident,
    pub data_type: DataType,
}

impl fmt::Display for ProcedureParam {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} {}", self.name, self.data_type)
    }
}

/// SQL column definition
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct ColumnDef {
    pub name: Ident,
    pub data_type: DataType,
    pub collation: Option<ObjectName>,
    pub options: Vec<ColumnOptionDef>,
}

impl fmt::Display for ColumnDef {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} {}", self.name, self.data_type)?;
        if let Some(collation) = &self.collation {
            write!(f, " COLLATE {collation}")?;
        }
        for option in &self.options {
            write!(f, " {option}")?;
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
    /// `{ PRIMARY KEY | UNIQUE }`
    Unique {
        is_primary: bool,
    },
    /// A referential integrity constraint (`[FOREIGN KEY REFERENCES
    /// <foreign_table> (<referred_columns>)
    /// { [ON DELETE <referential_action>] [ON UPDATE <referential_action>] |
    ///   [ON UPDATE <referential_action>] [ON DELETE <referential_action>]
    /// }`).
    ForeignKey {
        foreign_table: ObjectName,
        referred_columns: Vec<Ident>,
        on_delete: Option<ReferentialAction>,
        on_update: Option<ReferentialAction>,
    },
    /// `CHECK (<expr>)`
    Check(Expr),
    /// Dialect-specific options, such as:
    /// - MySQL's `AUTO_INCREMENT` or SQLite's `AUTOINCREMENT`
    /// - ...
    DialectSpecific(Vec<Token>),
    CharacterSet(ObjectName),
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
}

impl fmt::Display for ColumnOption {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use ColumnOption::*;
        match self {
            Null => write!(f, "NULL"),
            NotNull => write!(f, "NOT NULL"),
            Default(expr) => write!(f, "DEFAULT {expr}"),
            Unique { is_primary } => {
                write!(f, "{}", if *is_primary { "PRIMARY KEY" } else { "UNIQUE" })
            }
            ForeignKey {
                foreign_table,
                referred_columns,
                on_delete,
                on_update,
            } => {
                write!(f, "REFERENCES {foreign_table}")?;
                if !referred_columns.is_empty() {
                    write!(f, " ({})", display_comma_separated(referred_columns))?;
                }
                if let Some(action) = on_delete {
                    write!(f, " ON DELETE {action}")?;
                }
                if let Some(action) = on_update {
                    write!(f, " ON UPDATE {action}")?;
                }
                Ok(())
            }
            Check(expr) => write!(f, "CHECK ({expr})"),
            DialectSpecific(val) => write!(f, "{}", display_separated(val, " ")),
            CharacterSet(n) => write!(f, "CHARACTER SET {n}"),
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

fn display_constraint_name(name: &'_ Option<Ident>) -> impl fmt::Display + '_ {
    struct ConstraintName<'a>(&'a Option<Ident>);
    impl<'a> fmt::Display for ConstraintName<'a> {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            if let Some(name) = self.0 {
                write!(f, "CONSTRAINT {name} ")?;
            }
            Ok(())
        }
    }
    ConstraintName(name)
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

/// SQL user defined type definition
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum UserDefinedTypeRepresentation {
    Composite {
        attributes: Vec<UserDefinedTypeCompositeAttributeDef>,
    },
}

impl fmt::Display for UserDefinedTypeRepresentation {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            UserDefinedTypeRepresentation::Composite { attributes } => {
                write!(f, "({})", display_comma_separated(attributes))
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

/// PARTITION statement used in ALTER TABLE et al. such as in Hive SQL
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct Partition {
    pub partitions: Vec<Expr>,
}

impl fmt::Display for Partition {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "PARTITION ({})",
            display_comma_separated(&self.partitions)
        )
    }
}

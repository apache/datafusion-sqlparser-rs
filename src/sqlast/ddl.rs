//! AST types specific to CREATE/ALTER variants of `SQLStatement`
//! (commonly referred to as Data Definition Language, or DDL)
use super::{Expr, SQLIdent, SQLObjectName, SQLType};

/// An `ALTER TABLE` (`SQLStatement::SQLAlterTable`) operation
#[derive(Debug, Clone, PartialEq, Hash)]
pub enum AlterTableOperation {
    /// `ADD <table_constraint>`
    AddConstraint(TableConstraint),
    /// TODO: implement `DROP CONSTRAINT <name>`
    DropConstraint { name: SQLIdent },
}

impl ToString for AlterTableOperation {
    fn to_string(&self) -> String {
        match self {
            AlterTableOperation::AddConstraint(c) => format!("ADD {}", c.to_string()),
            AlterTableOperation::DropConstraint { name } => format!("DROP CONSTRAINT {}", name),
        }
    }
}

/// A table-level constraint, specified in a `CREATE TABLE` or an
/// `ALTER TABLE ADD <constraint>` statement.
#[derive(Debug, Clone, PartialEq, Hash)]
pub enum TableConstraint {
    /// `[ CONSTRAINT <name> ] { PRIMARY KEY | UNIQUE } (<columns>)`
    Unique {
        name: Option<SQLIdent>,
        columns: Vec<SQLIdent>,
        /// Whether this is a `PRIMARY KEY` or just a `UNIQUE` constraint
        is_primary: bool,
    },
    /// A referential integrity constraint (`[ CONSTRAINT <name> ] FOREIGN KEY (<columns>)
    /// REFERENCES <foreign_table> (<referred_columns>)`)
    ForeignKey {
        name: Option<SQLIdent>,
        columns: Vec<SQLIdent>,
        foreign_table: SQLObjectName,
        referred_columns: Vec<SQLIdent>,
    },
    /// `[ CONSTRAINT <name> ] CHECK (<expr>)`
    Check {
        name: Option<SQLIdent>,
        expr: Box<Expr>,
    },
}

impl ToString for TableConstraint {
    fn to_string(&self) -> String {
        match self {
            TableConstraint::Unique {
                name,
                columns,
                is_primary,
            } => format!(
                "{}{} ({})",
                format_constraint_name(name),
                if *is_primary { "PRIMARY KEY" } else { "UNIQUE" },
                columns.join(", ")
            ),
            TableConstraint::ForeignKey {
                name,
                columns,
                foreign_table,
                referred_columns,
            } => format!(
                "{}FOREIGN KEY ({}) REFERENCES {}({})",
                format_constraint_name(name),
                columns.join(", "),
                foreign_table.to_string(),
                referred_columns.join(", ")
            ),
            TableConstraint::Check { name, expr } => format!(
                "{}CHECK ({})",
                format_constraint_name(name),
                expr.to_string()
            ),
        }
    }
}

/// SQL column definition
#[derive(Debug, Clone, PartialEq, Hash)]
pub struct SQLColumnDef {
    pub name: SQLIdent,
    pub data_type: SQLType,
    pub collation: Option<SQLObjectName>,
    pub options: Vec<ColumnOptionDef>,
}

impl ToString for SQLColumnDef {
    fn to_string(&self) -> String {
        format!(
            "{} {}{}",
            self.name,
            self.data_type.to_string(),
            self.options
                .iter()
                .map(|c| format!(" {}", c.to_string()))
                .collect::<Vec<_>>()
                .join("")
        )
    }
}

/// An optionally-named `ColumnOption`: `[ CONSTRAINT <name> ] <column-option>`.
///
/// Note that implementations are substantially more permissive than the ANSI
/// specification on what order column options can be presented in, and whether
/// they are allowed to be named. The specification distinguishes between
/// constraints (NOT NULL, UNIQUE, PRIMARY KEY, and CHECK), which can be named
/// and can appear in any order, and other options (DEFAULT, GENERATED), which
/// cannot be named and must appear in a fixed order. PostgreSQL, however,
/// allows preceding any option with `CONSTRAINT <name>`, even those that are
/// not really constraints, like NULL and DEFAULT. MSSQL is less permissive,
/// allowing DEFAULT, UNIQUE, PRIMARY KEY and CHECK to be named, but not NULL or
/// NOT NULL constraints (the last of which is in violation of the spec).
///
/// For maximum flexibility, we don't distinguish between constraint and
/// non-constraint options, lumping them all together under the umbrella of
/// "column options," and we allow any column option to be named.
#[derive(Debug, Clone, PartialEq, Hash)]
pub struct ColumnOptionDef {
    pub name: Option<SQLIdent>,
    pub option: ColumnOption,
}

impl ToString for ColumnOptionDef {
    fn to_string(&self) -> String {
        format!(
            "{}{}",
            format_constraint_name(&self.name),
            self.option.to_string()
        )
    }
}

/// `ColumnOption`s are modifiers that follow a column definition in a `CREATE
/// TABLE` statement.
#[derive(Debug, Clone, PartialEq, Hash)]
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
    /// <foreign_table> (<referred_columns>)`).
    ForeignKey {
        foreign_table: SQLObjectName,
        referred_columns: Vec<SQLIdent>,
    },
    // `CHECK (<expr>)`
    Check(Expr),
}

impl ToString for ColumnOption {
    fn to_string(&self) -> String {
        use ColumnOption::*;
        match self {
            Null => "NULL".to_string(),
            NotNull => "NOT NULL".to_string(),
            Default(expr) => format!("DEFAULT {}", expr.to_string()),
            Unique { is_primary } => {
                if *is_primary {
                    "PRIMARY KEY".to_string()
                } else {
                    "UNIQUE".to_string()
                }
            }
            ForeignKey {
                foreign_table,
                referred_columns,
            } => format!(
                "REFERENCES {} ({})",
                foreign_table.to_string(),
                referred_columns.join(", ")
            ),
            Check(expr) => format!("CHECK ({})", expr.to_string(),),
        }
    }
}

fn format_constraint_name(name: &Option<SQLIdent>) -> String {
    name.as_ref()
        .map(|name| format!("CONSTRAINT {} ", name))
        .unwrap_or_default()
}

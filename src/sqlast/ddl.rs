//! AST types specific to CREATE/ALTER variants of `SQLStatement`
//! (commonly referred to as Data Definition Language, or DDL)
use super::{ASTNode, SQLIdent, SQLObjectName};

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
        expr: Box<ASTNode>,
    },
}

impl ToString for TableConstraint {
    fn to_string(&self) -> String {
        fn format_constraint_name(name: &Option<SQLIdent>) -> String {
            name.as_ref()
                .map(|name| format!("CONSTRAINT {} ", name))
                .unwrap_or_default()
        }
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

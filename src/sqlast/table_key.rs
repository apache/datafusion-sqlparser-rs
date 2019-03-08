use super::SQLIdent;

#[derive(Debug, PartialEq, Clone)]
pub enum AlterOperation {
    AddConstraint(TableKey),
    RemoveConstraint { name: String },
}

impl ToString for AlterOperation {
    fn to_string(&self) -> String {
        match self {
            AlterOperation::AddConstraint(table_key) => {
                format!("ADD CONSTRAINT {}", table_key.to_string())
            }
            AlterOperation::RemoveConstraint { name } => format!("REMOVE CONSTRAINT {}", name),
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct Key {
    pub name: SQLIdent,
    pub columns: Vec<SQLIdent>,
}

#[derive(Debug, PartialEq, Clone)]
pub enum TableKey {
    PrimaryKey(Key),
    UniqueKey(Key),
    Key(Key),
    ForeignKey {
        key: Key,
        foreign_table: String,
        referred_columns: Vec<SQLIdent>,
    },
}

impl ToString for TableKey {
    fn to_string(&self) -> String {
        match self {
            TableKey::PrimaryKey(ref key) => {
                format!("{} PRIMARY KEY ({})", key.name, key.columns.join(", "))
            }
            TableKey::UniqueKey(ref key) => {
                format!("{} UNIQUE KEY ({})", key.name, key.columns.join(", "))
            }
            TableKey::Key(ref key) => format!("{} KEY ({})", key.name, key.columns.join(", ")),
            TableKey::ForeignKey {
                key,
                foreign_table,
                referred_columns,
            } => format!(
                "{} FOREIGN KEY ({}) REFERENCES {}({})",
                key.name,
                key.columns.join(", "),
                foreign_table,
                referred_columns.join(", ")
            ),
        }
    }
}

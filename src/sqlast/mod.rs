// Copyright 2018 Grove Enterprises LLC
//
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

//! SQL Abstract Syntax Tree (AST) types

mod query;
mod sql_operator;
mod sqltype;
mod table_key;
mod value;

pub use self::query::{Join, JoinConstraint, JoinOperator, SQLOrderByExpr, SQLSelect};
pub use self::sqltype::SQLType;
pub use self::table_key::{AlterOperation, Key, TableKey};
pub use self::value::Value;

pub use self::sql_operator::SQLOperator;

// This could be enhanced to remember the way the identifier was quoted
pub type SQLIdent = String;

/// SQL Abstract Syntax Tree (AST)
#[derive(Debug, Clone, PartialEq)]
pub enum ASTNode {
    /// Identifier e.g. table name or column name
    SQLIdentifier(SQLIdent),
    /// Wildcard e.g. `*`
    SQLWildcard,
    /// Multi part identifier e.g. `myschema.dbo.mytable`
    SQLCompoundIdentifier(Vec<SQLIdent>),
    /// `IS NULL` expression
    SQLIsNull(Box<ASTNode>),
    /// `IS NOT NULL` expression
    SQLIsNotNull(Box<ASTNode>),
    /// Binary expression e.g. `1 + 1` or `foo > bar`
    SQLBinaryExpr {
        left: Box<ASTNode>,
        op: SQLOperator,
        right: Box<ASTNode>,
    },
    /// CAST an expression to a different data type e.g. `CAST(foo AS VARCHAR(123))`
    SQLCast {
        expr: Box<ASTNode>,
        data_type: SQLType,
    },
    /// Nested expression e.g. `(foo > bar)` or `(1)`
    SQLNested(Box<ASTNode>),
    /// Unary expression
    SQLUnary {
        operator: SQLOperator,
        expr: Box<ASTNode>,
    },
    /// SQLValue
    SQLValue(Value),
    /// Scalar function call e.g. `LEFT(foo, 5)`
    SQLFunction { id: String, args: Vec<ASTNode> },
    /// CASE [<operand>] WHEN <condition> THEN <result> ... [ELSE <result>] END
    SQLCase {
        // TODO: support optional operand for "simple case"
        conditions: Vec<ASTNode>,
        results: Vec<ASTNode>,
        else_result: Option<Box<ASTNode>>,
    },
    /// A table name or a parenthesized subquery with an optional alias
    TableFactor {
        relation: Box<ASTNode>, // SQLNested or SQLCompoundIdentifier
        alias: Option<SQLIdent>,
    },
    /// SELECT
    SQLSelect(SQLSelect),
    /// INSERT
    SQLInsert {
        /// TABLE
        table_name: String,
        /// COLUMNS
        columns: Vec<SQLIdent>,
        /// VALUES (vector of rows to insert)
        values: Vec<Vec<ASTNode>>,
    },
    SQLCopy {
        /// TABLE
        table_name: String,
        /// COLUMNS
        columns: Vec<SQLIdent>,
        /// VALUES a vector of values to be copied
        values: Vec<Option<String>>,
    },
    /// UPDATE
    SQLUpdate {
        /// TABLE
        table_name: String,
        /// Column assignments
        assignments: Vec<SQLAssignment>,
        /// WHERE
        selection: Option<Box<ASTNode>>,
    },
    /// DELETE
    SQLDelete {
        /// FROM
        relation: Option<Box<ASTNode>>,
        /// WHERE
        selection: Option<Box<ASTNode>>,
    },
    /// CREATE TABLE
    SQLCreateTable {
        /// Table name
        name: String,
        /// Optional schema
        columns: Vec<SQLColumnDef>,
    },
    /// ALTER TABLE
    SQLAlterTable {
        /// Table name
        name: String,
        operation: AlterOperation,
    },
}

impl ToString for ASTNode {
    fn to_string(&self) -> String {
        match self {
            ASTNode::SQLIdentifier(s) => s.to_string(),
            ASTNode::SQLWildcard => "*".to_string(),
            ASTNode::SQLCompoundIdentifier(s) => s.join("."),
            ASTNode::SQLIsNull(ast) => format!("{} IS NULL", ast.as_ref().to_string()),
            ASTNode::SQLIsNotNull(ast) => format!("{} IS NOT NULL", ast.as_ref().to_string()),
            ASTNode::SQLBinaryExpr { left, op, right } => format!(
                "{} {} {}",
                left.as_ref().to_string(),
                op.to_string(),
                right.as_ref().to_string()
            ),
            ASTNode::SQLCast { expr, data_type } => format!(
                "CAST({} AS {})",
                expr.as_ref().to_string(),
                data_type.to_string()
            ),
            ASTNode::SQLNested(ast) => format!("({})", ast.as_ref().to_string()),
            ASTNode::SQLUnary { operator, expr } => {
                format!("{} {}", operator.to_string(), expr.as_ref().to_string())
            }
            ASTNode::SQLValue(v) => v.to_string(),
            ASTNode::SQLFunction { id, args } => format!(
                "{}({})",
                id,
                args.iter()
                    .map(|a| a.to_string())
                    .collect::<Vec<String>>()
                    .join(", ")
            ),
            ASTNode::SQLCase {
                conditions,
                results,
                else_result,
            } => {
                let mut s = format!(
                    "CASE {}",
                    conditions
                        .iter()
                        .zip(results)
                        .map(|(c, r)| format!("WHEN {} THEN {}", c.to_string(), r.to_string()))
                        .collect::<Vec<String>>()
                        .join(" ")
                );
                if let Some(else_result) = else_result {
                    s += &format!(" ELSE {}", else_result.to_string())
                }
                s + " END"
            }
            ASTNode::TableFactor { relation, alias } => {
                if let Some(alias) = alias {
                    format!("{} AS {}", relation.to_string(), alias)
                } else {
                    relation.to_string()
                }
            }
            ASTNode::SQLSelect(s) => s.to_string(),
            ASTNode::SQLInsert {
                table_name,
                columns,
                values,
            } => {
                let mut s = format!("INSERT INTO {}", table_name);
                if columns.len() > 0 {
                    s += &format!(" ({})", columns.join(", "));
                }
                if values.len() > 0 {
                    s += &format!(
                        " VALUES({})",
                        values
                            .iter()
                            .map(|row| row
                                .iter()
                                .map(|c| c.to_string())
                                .collect::<Vec<String>>()
                                .join(", "))
                            .collect::<Vec<String>>()
                            .join(", ")
                    );
                }
                s
            }
            ASTNode::SQLCopy {
                table_name,
                columns,
                values,
            } => {
                let mut s = format!("COPY {}", table_name);
                if columns.len() > 0 {
                    s += &format!(
                        " ({})",
                        columns
                            .iter()
                            .map(|c| c.to_string())
                            .collect::<Vec<String>>()
                            .join(", ")
                    );
                }
                s += " FROM stdin; ";
                if values.len() > 0 {
                    s += &format!(
                        "\n{}",
                        values
                            .iter()
                            .map(|v| v.clone().unwrap_or("\\N".to_string()))
                            .collect::<Vec<String>>()
                            .join("\t")
                    );
                }
                s += "\n\\.";
                s
            }
            ASTNode::SQLUpdate {
                table_name,
                assignments,
                selection,
            } => {
                let mut s = format!("UPDATE {}", table_name);
                if assignments.len() > 0 {
                    s += &format!(
                        "{}",
                        assignments
                            .iter()
                            .map(|ass| ass.to_string())
                            .collect::<Vec<String>>()
                            .join(", ")
                    );
                }
                if let Some(selection) = selection {
                    s += &format!(" WHERE {}", selection.as_ref().to_string());
                }
                s
            }
            ASTNode::SQLDelete {
                relation,
                selection,
            } => {
                let mut s = String::from("DELETE");
                if let Some(relation) = relation {
                    s += &format!(" FROM {}", relation.as_ref().to_string());
                }
                if let Some(selection) = selection {
                    s += &format!(" WHERE {}", selection.as_ref().to_string());
                }
                s
            }
            ASTNode::SQLCreateTable { name, columns } => format!(
                "CREATE TABLE {} ({})",
                name,
                columns
                    .iter()
                    .map(|c| c.to_string())
                    .collect::<Vec<String>>()
                    .join(", ")
            ),
            ASTNode::SQLAlterTable { name, operation } => {
                format!("ALTER TABLE {} {}", name, operation.to_string())
            }
        }
    }
}

/// SQL assignment `foo = expr` as used in SQLUpdate
#[derive(Debug, Clone, PartialEq)]
pub struct SQLAssignment {
    id: String,
    value: Box<ASTNode>,
}

impl ToString for SQLAssignment {
    fn to_string(&self) -> String {
        format!("SET {} = {}", self.id, self.value.as_ref().to_string())
    }
}

/// SQL column definition
#[derive(Debug, Clone, PartialEq)]
pub struct SQLColumnDef {
    pub name: SQLIdent,
    pub data_type: SQLType,
    pub is_primary: bool,
    pub is_unique: bool,
    pub default: Option<Box<ASTNode>>,
    pub allow_null: bool,
}

impl ToString for SQLColumnDef {
    fn to_string(&self) -> String {
        let mut s = format!("{} {}", self.name, self.data_type.to_string());
        if self.is_primary {
            s += " PRIMARY KEY";
        }
        if self.is_unique {
            s += " UNIQUE";
        }
        if let Some(ref default) = self.default {
            s += &format!(" DEFAULT {}", default.as_ref().to_string());
        }
        if !self.allow_null {
            s += " NOT NULL";
        }
        s
    }
}

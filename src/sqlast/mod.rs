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

pub use self::query::{
    Cte, Join, JoinConstraint, JoinOperator, SQLOrderByExpr, SQLQuery, SQLSelect, SQLSelectItem,
    SQLSetExpr, SQLSetOperator, TableFactor,
};
pub use self::sqltype::SQLType;
pub use self::table_key::{AlterOperation, Key, TableKey};
pub use self::value::Value;

pub use self::sql_operator::SQLOperator;

/// Identifier name, in the originally quoted form (e.g. `"id"`)
pub type SQLIdent = String;

/// Represents a parsed SQL expression, which is a common building
/// block of SQL statements (the part after SELECT, WHERE, etc.)
#[derive(Debug, Clone, PartialEq)]
pub enum ASTNode {
    /// Identifier e.g. table name or column name
    SQLIdentifier(SQLIdent),
    /// Unqualified wildcard (`*`). SQL allows this in limited contexts (such as right
    /// after `SELECT` or as part of an aggregate function, e.g. `COUNT(*)`, but we
    /// currently accept it in contexts where it doesn't make sense, such as `* + *`
    SQLWildcard,
    /// Qualified wildcard, e.g. `alias.*` or `schema.table.*`.
    /// (Same caveats apply to SQLQualifiedWildcard as to SQLWildcard.)
    SQLQualifiedWildcard(Vec<SQLIdent>),
    /// Multi part identifier e.g. `myschema.dbo.mytable`
    SQLCompoundIdentifier(Vec<SQLIdent>),
    /// `IS NULL` expression
    SQLIsNull(Box<ASTNode>),
    /// `IS NOT NULL` expression
    SQLIsNotNull(Box<ASTNode>),
    /// `[ NOT ] IN (val1, val2, ...)`
    SQLInList {
        expr: Box<ASTNode>,
        list: Vec<ASTNode>,
        negated: bool,
    },
    /// `[ NOT ] IN (SELECT ...)`
    SQLInSubquery {
        expr: Box<ASTNode>,
        subquery: Box<SQLQuery>,
        negated: bool,
    },
    /// <expr> [ NOT ] BETWEEN <low> AND <high>
    SQLBetween {
        expr: Box<ASTNode>,
        negated: bool,
        low: Box<ASTNode>,
        high: Box<ASTNode>,
    },
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
    /// TODO: this can be a compound SQLObjectName as well (for UDFs)
    SQLFunction { id: SQLIdent, args: Vec<ASTNode> },
    /// CASE [<operand>] WHEN <condition> THEN <result> ... [ELSE <result>] END
    SQLCase {
        // TODO: support optional operand for "simple case"
        conditions: Vec<ASTNode>,
        results: Vec<ASTNode>,
        else_result: Option<Box<ASTNode>>,
    },
    /// A parenthesized subquery `(SELECT ...)`, used in expression like
    /// `SELECT (subquery) AS x` or `WHERE (subquery) = x`
    SQLSubquery(Box<SQLQuery>),
}

impl ToString for ASTNode {
    fn to_string(&self) -> String {
        match self {
            ASTNode::SQLIdentifier(s) => s.to_string(),
            ASTNode::SQLWildcard => "*".to_string(),
            ASTNode::SQLQualifiedWildcard(q) => q.join(".") + "*",
            ASTNode::SQLCompoundIdentifier(s) => s.join("."),
            ASTNode::SQLIsNull(ast) => format!("{} IS NULL", ast.as_ref().to_string()),
            ASTNode::SQLIsNotNull(ast) => format!("{} IS NOT NULL", ast.as_ref().to_string()),
            ASTNode::SQLInList {
                expr,
                list,
                negated,
            } => format!(
                "{} {}IN ({})",
                expr.as_ref().to_string(),
                if *negated { "NOT " } else { "" },
                list.iter()
                    .map(|a| a.to_string())
                    .collect::<Vec<String>>()
                    .join(", ")
            ),
            ASTNode::SQLInSubquery {
                expr,
                subquery,
                negated,
            } => format!(
                "{} {}IN ({})",
                expr.as_ref().to_string(),
                if *negated { "NOT " } else { "" },
                subquery.to_string()
            ),
            ASTNode::SQLBetween {
                expr,
                negated,
                low,
                high,
            } => format!(
                "{} {}BETWEEN {} AND {}",
                expr.to_string(),
                if *negated { "NOT " } else { "" },
                low.to_string(),
                high.to_string()
            ),
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
            ASTNode::SQLSubquery(s) => format!("({})", s.to_string()),
        }
    }
}

/// A top-level statement (SELECT, INSERT, CREATE, etc.)
#[derive(Debug, Clone, PartialEq)]
pub enum SQLStatement {
    /// SELECT
    SQLSelect(SQLQuery),
    /// INSERT
    SQLInsert {
        /// TABLE
        table_name: SQLObjectName,
        /// COLUMNS
        columns: Vec<SQLIdent>,
        /// VALUES (vector of rows to insert)
        values: Vec<Vec<ASTNode>>,
    },
    SQLCopy {
        /// TABLE
        table_name: SQLObjectName,
        /// COLUMNS
        columns: Vec<SQLIdent>,
        /// VALUES a vector of values to be copied
        values: Vec<Option<String>>,
    },
    /// UPDATE
    SQLUpdate {
        /// TABLE
        table_name: SQLObjectName,
        /// Column assignments
        assignments: Vec<SQLAssignment>,
        /// WHERE
        selection: Option<ASTNode>,
    },
    /// DELETE
    SQLDelete {
        /// FROM
        table_name: SQLObjectName,
        /// WHERE
        selection: Option<ASTNode>,
    },
    /// CREATE VIEW
    SQLCreateView {
        /// View name
        name: SQLObjectName,
        query: SQLQuery,
        materialized: bool,
    },
    /// CREATE TABLE
    SQLCreateTable {
        /// Table name
        name: SQLObjectName,
        /// Optional schema
        columns: Vec<SQLColumnDef>,
        external: bool,
        file_format: Option<FileFormat>,
        location: Option<String>,
    },
    /// ALTER TABLE
    SQLAlterTable {
        /// Table name
        name: SQLObjectName,
        operation: AlterOperation,
    },
}

impl ToString for SQLStatement {
    fn to_string(&self) -> String {
        match self {
            SQLStatement::SQLSelect(s) => s.to_string(),
            SQLStatement::SQLInsert {
                table_name,
                columns,
                values,
            } => {
                let mut s = format!("INSERT INTO {}", table_name.to_string());
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
            SQLStatement::SQLCopy {
                table_name,
                columns,
                values,
            } => {
                let mut s = format!("COPY {}", table_name.to_string());
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
            SQLStatement::SQLUpdate {
                table_name,
                assignments,
                selection,
            } => {
                let mut s = format!("UPDATE {}", table_name.to_string());
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
                    s += &format!(" WHERE {}", selection.to_string());
                }
                s
            }
            SQLStatement::SQLDelete {
                table_name,
                selection,
            } => {
                let mut s = format!("DELETE FROM {}", table_name.to_string());
                if let Some(selection) = selection {
                    s += &format!(" WHERE {}", selection.to_string());
                }
                s
            }
            SQLStatement::SQLCreateView {
                name,
                query,
                materialized,
            } => {
                let modifier = if *materialized { " MATERIALIZED" } else { "" };
                format!(
                    "CREATE{} VIEW {} AS {}",
                    modifier,
                    name.to_string(),
                    query.to_string()
                )
            }
            SQLStatement::SQLCreateTable {
                name,
                columns,
                external,
                file_format,
                location,
            } if *external => format!(
                "CREATE EXTERNAL TABLE {} ({}) STORED AS {} LOCATION '{}'",
                name.to_string(),
                columns
                    .iter()
                    .map(|c| c.to_string())
                    .collect::<Vec<String>>()
                    .join(", "),
                file_format.as_ref().map(|f| f.to_string()).unwrap(),
                location.as_ref().unwrap()
            ),
            SQLStatement::SQLCreateTable {
                name,
                columns,
                external: _,
                file_format: _,
                location: _,
            } => format!(
                "CREATE TABLE {} ({})",
                name.to_string(),
                columns
                    .iter()
                    .map(|c| c.to_string())
                    .collect::<Vec<String>>()
                    .join(", ")
            ),
            SQLStatement::SQLAlterTable { name, operation } => {
                format!("ALTER TABLE {} {}", name.to_string(), operation.to_string())
            }
        }
    }
}

/// A name of a table, view, custom type, etc., possibly multi-part, i.e. db.schema.obj
#[derive(Debug, Clone, PartialEq)]
pub struct SQLObjectName(pub Vec<SQLIdent>);

impl ToString for SQLObjectName {
    fn to_string(&self) -> String {
        self.0.join(".")
    }
}

/// SQL assignment `foo = expr` as used in SQLUpdate
#[derive(Debug, Clone, PartialEq)]
pub struct SQLAssignment {
    id: SQLIdent,
    value: ASTNode,
}

impl ToString for SQLAssignment {
    fn to_string(&self) -> String {
        format!("SET {} = {}", self.id, self.value.to_string())
    }
}

/// SQL column definition
#[derive(Debug, Clone, PartialEq)]
pub struct SQLColumnDef {
    pub name: SQLIdent,
    pub data_type: SQLType,
    pub is_primary: bool,
    pub is_unique: bool,
    pub default: Option<ASTNode>,
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
            s += &format!(" DEFAULT {}", default.to_string());
        }
        if !self.allow_null {
            s += " NOT NULL";
        }
        s
    }
}

/// External table's available file format
#[derive(Debug, Clone, PartialEq)]
pub enum FileFormat {
    TEXTFILE,
    SEQUENCEFILE,
    ORC,
    PARQUET,
    AVRO,
    RCFILE,
    JSONFILE,
}

impl ToString for FileFormat {
    fn to_string(&self) -> String {
        use self::FileFormat::*;
        match self {
            TEXTFILE => "TEXTFILE".to_string(),
            SEQUENCEFILE => "SEQUENCEFILE".to_string(),
            ORC => "ORC".to_string(),
            PARQUET => "PARQUET".to_string(),
            AVRO => "AVRO".to_string(),
            RCFILE => "RCFILE".to_string(),
            JSONFILE => "TEXTFILE".to_string(),
        }
    }
}

use sqlparser::ParserError;
use std::str::FromStr;
impl FromStr for FileFormat {
    type Err = ParserError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        use self::FileFormat::*;
        match s {
            "TEXTFILE" => Ok(TEXTFILE),
            "SEQUENCEFILE" => Ok(SEQUENCEFILE),
            "ORC" => Ok(ORC),
            "PARQUET" => Ok(PARQUET),
            "AVRO" => Ok(AVRO),
            "RCFILE" => Ok(RCFILE),
            "JSONFILE" => Ok(JSONFILE),
            _ => Err(ParserError::ParserError(format!(
                "Unexpected file format: {}",
                s
            ))),
        }
    }
}

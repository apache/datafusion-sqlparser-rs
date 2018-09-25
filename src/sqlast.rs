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
//!
use chrono::{NaiveDate,
             NaiveDateTime,
             NaiveTime,
            };

/// SQL Abstract Syntax Tree (AST)
#[derive(Debug, Clone, PartialEq)]
pub enum ASTNode {
    /// Identifier e.g. table name or column name
    SQLIdentifier(String),
    /// Wildcard e.g. `*`
    SQLWildcard,
    /// Multi part identifier e.g. `myschema.dbo.mytable`
    SQLCompoundIdentifier(Vec<String>),
    /// Assigment e.g. `name = 'Fred'` in an UPDATE statement
    SQLAssignment(String, Box<ASTNode>),
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
        rex: Box<ASTNode>,
    },
    /// SQLValue
    SQLValue(Value),
    /// Scalar function call e.g. `LEFT(foo, 5)`
    SQLFunction { id: String, args: Vec<ASTNode> },
    /// SELECT
    SQLSelect {
        /// projection expressions
        projection: Vec<ASTNode>,
        /// FROM
        relation: Option<Box<ASTNode>>,
        /// WHERE
        selection: Option<Box<ASTNode>>,
        /// ORDER BY
        order_by: Option<Vec<SQLOrderByExpr>>,
        /// GROUP BY
        group_by: Option<Vec<ASTNode>>,
        /// HAVING
        having: Option<Box<ASTNode>>,
        /// LIMIT
        limit: Option<Box<ASTNode>>,
    },
    /// INSERT
    SQLInsert {
        /// TABLE
        table_name: String,
        /// COLUMNS
        columns: Vec<String>,
        /// VALUES (vector of rows to insert)
        values: Vec<Vec<ASTNode>>,
    },
    SQLCopy{
        /// TABLE
        table_name: String,
        /// COLUMNS
        columns: Vec<String>,
        /// VALUES a vector of values to be copied
        values: Vec<Value>,
    },
    /// UPDATE
    SQLUpdate {
        /// TABLE
        table_name: String,
        /// Column assignments
        assignemnts: Vec<SQLAssigment>,
        /// WHERE
        selection: Option<Box<ASTNode>>,
    },
    /// DELETE
    SQLDelete {
        /// FROM
        relation: Option<Box<ASTNode>>,
        /// WHERE
        selection: Option<Box<ASTNode>>,
        /// ORDER BY
        order_by: Option<Vec<SQLOrderByExpr>>,
        limit: Option<Box<ASTNode>>,
    },
    /// CREATE TABLE
    SQLCreateTable {
        /// Table name
        name: String,
        /// Optional schema
        columns: Vec<SQLColumnDef>,
    },
}

/// SQL values such as int, double, string timestamp
#[derive(Debug, Clone, PartialEq)]
pub enum Value{
    /// Literal signed long
    Long(i64),
    /// Literal floating point value
    Double(f64),
    /// Literal string
    String(String),
    /// Boolean value true or false,
    Boolean(bool),
    /// Date value
    Date(NaiveDate),
    // Time
    Time(NaiveTime),
    /// Timestamp
    DateTime(NaiveDateTime),
    /// NULL value in insert statements,
    Null,
}

/// SQL assignment `foo = expr` as used in SQLUpdate
#[derive(Debug, Clone, PartialEq)]
pub struct SQLAssigment {
    id: String,
    value: Box<ASTNode>,
}

/// SQL ORDER BY expression
#[derive(Debug, Clone, PartialEq)]
pub struct SQLOrderByExpr {
    pub expr: Box<ASTNode>,
    pub asc: bool,
}

impl SQLOrderByExpr {
    pub fn new(expr: Box<ASTNode>, asc: bool) -> Self {
        SQLOrderByExpr { expr, asc }
    }
}

/// SQL column definition
#[derive(Debug, Clone, PartialEq)]
pub struct SQLColumnDef {
    pub name: String,
    pub data_type: SQLType,
    pub allow_null: bool,
    pub default: Option<Box<ASTNode>>,
}

/// SQL datatypes for literals in SQL statements
#[derive(Debug, Clone, PartialEq)]
pub enum SQLType {
    /// Fixed-length character type e.g. CHAR(10)
    Char(Option<usize>),
    /// Variable-length character type e.g. VARCHAR(10)
    Varchar(Option<usize>),
    /// Large character object e.g. CLOB(1000)
    Clob(usize),
    /// Fixed-length binary type e.g. BINARY(10)
    Binary(usize),
    /// Variable-length binary type e.g. VARBINARY(10)
    Varbinary(usize),
    /// Large binary object e.g. BLOB(1000)
    Blob(usize),
    /// Decimal type with precision and optional scale e.g. DECIMAL(10,2)
    Decimal(usize, Option<usize>),
    /// Small integer
    SmallInt,
    /// Integer
    Int,
    /// Big integer
    BigInt,
    /// Floating point with optional precision e.g. FLOAT(8)
    Float(Option<usize>),
    /// Floating point e.g. REAL
    Real,
    /// Double e.g. DOUBLE PRECISION
    Double,
    /// Boolean
    Boolean,
    /// Date
    Date,
    /// Time
    Time,
    /// Timestamp
    Timestamp,
    /// Regclass used in postgresql serial
    Regclass,
    /// Text
    Text,
    /// Bytea
    Bytea,
    /// Custom type such as enums
    Custom(String),
    /// Arrays
    Array(Box<SQLType>),
}

/// SQL Operator
#[derive(Debug, PartialEq, Clone)]
pub enum SQLOperator {
    Plus,
    Minus,
    Multiply,
    Divide,
    Modulus,
    Gt,
    Lt,
    GtEq,
    LtEq,
    Eq,
    NotEq,
    And,
    Or,
}

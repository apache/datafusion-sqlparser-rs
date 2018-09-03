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

/// SQL Abstract Syntax Tree (AST)
#[derive(Debug, Clone, PartialEq)]
pub enum ASTNode {
    SQLIdentifier(String),
    SQLWildcard,
    SQLCompoundIdentifier(Vec<String>),
    SQLIsNull(Box<ASTNode>),
    SQLIsNotNull(Box<ASTNode>),
    SQLBinaryExpr {
        left: Box<ASTNode>,
        op: SQLOperator,
        right: Box<ASTNode>,
    },
    SQLCast {
        expr: Box<ASTNode>,
        data_type: SQLType,
    },
    SQLNested(Box<ASTNode>),
    SQLUnary {
        operator: SQLOperator,
        rex: Box<ASTNode>,
    },
    SQLLiteralLong(i64),
    SQLLiteralDouble(f64),
    SQLLiteralString(String),
    SQLFunction {
        id: String,
        args: Vec<ASTNode>,
    },
    SQLOrderBy {
        expr: Box<ASTNode>,
        asc: bool,
    },
    SQLSelect {
        projection: Vec<ASTNode>,
        relation: Option<Box<ASTNode>>,
        selection: Option<Box<ASTNode>>,
        order_by: Option<Vec<ASTNode>>,
        group_by: Option<Vec<ASTNode>>,
        having: Option<Box<ASTNode>>,
        limit: Option<Box<ASTNode>>,
    },
    SQLCreateTable {
        /// Table name
        name: String,
        /// Optional schema
        columns: Vec<SQLColumnDef>,
    },
}

/// SQL column definition
#[derive(Debug, Clone, PartialEq)]
pub struct SQLColumnDef {
    pub name: String,
    pub data_type: SQLType,
    pub allow_null: bool,
}

/// SQL datatypes for literals in SQL statements
#[derive(Debug, Clone, PartialEq)]
pub enum SQLType {
    /// Fixed-length character type e.g. CHAR(10)
    Char(usize),
    /// Variable-length character type e.g. VARCHAR(10)
    Varchar(usize),
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

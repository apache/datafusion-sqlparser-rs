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

/// Supported file types for `CREATE EXTERNAL TABLE`
#[derive(Debug, Clone, PartialEq)]
pub enum FileType {
    CSV,
    NdJson,
    Parquet,
}

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
        /// File type (CSV or Parquet)
        file_type: FileType,
        /// For CSV files, indicate whether the file has a header row or not
        header_row: bool,
        /// Path to file or directory contianing files
        location: String,
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
    Boolean,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Int8,
    Int16,
    Int32,
    Int64,
    Float32,
    Double64,
    Utf8(usize),
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

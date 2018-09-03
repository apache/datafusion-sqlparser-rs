use std::cmp::PartialEq;
use std::fmt::Debug;

use super::tokenizer::*;

// https://jakewheat.github.io/sql-overview/sql-2011-foundation-grammar.html

/// ANSI SQL:2011 Data Types
#[derive(Debug)]
pub enum SQLDataType {
    /// BOOLEAN
    Boolean,
    /// NUMERIC, DECIMAL, DEC
    Numeric { precision: usize, scale: Option<usize> },
    /// SMALLINT
    SmallInt,
    /// INT, INTEGER
    Int,
    /// BIGINT
    BigInt,
    /// Floating point: `FLOAT(precision)`
    Float(usize),
    /// REAL
    Real,
    /// Double: `DOUBLE PRECISION`
    Double,
    /// Fixed-length character. `CHAR, CHARACTER`
    Char(usize),
    /// Variable-length character: `VARCHAR, CHARACTER VARYING, CHAR VARYING`
    VarChar(usize),
    /// Character Large Object: `CHARACTER LARGE OBJECT, CHAR LARGE OBJECT, CLOB`
    Clob(usize),
    /// Fixed-length character. `NCHAR, NATIONAL CHAR, NATIONAL CHARACTER`
    NChar(usize),
    /// Variable-length character: `NCHAR VARYING, NATIONAL CHARACTER VARYING, NATIONAL CHAR VARYING`
    NVarChar(usize),
    /// National Character Large Object: `NATIONAL CHARACTER LARGE OBJECT, NCHAR LARGE OBJECT, NCLOB`
    NClob(usize),
    /// Fixed-length binary
    Binary(usize),
    /// Variable-length binary
    VarBinary(usize),
    /// Binary large object
    Blob(usize),
    /// Date
    Date,
    /// Time: `TIME [(precision)] [WITH TIME ZONE | WITHOUT TIME ZONE]`
    Time { precision: usize, tz: bool },
    /// Time: `TIMESTAMP [(precision)] [WITH TIME ZONE | WITHOUT TIME ZONE]`
    Timestamp { precision: usize, tz: bool },
}



#[derive(Debug)]
pub enum SQLOperator {
    Plus,
    Minus,
    Mult,
    Div,
    Eq,
    Gt,
    GtEq,
    Lt,
    LtEq,
}

/// SQL Expressions
#[derive(Debug)]
pub enum SQLExpr{
    /// Identifier e.g. table name or column name
    Identifier(String),
    /// Literal value
    Literal(String),
    /// Binary expression e.g. `1 + 2` or `fname LIKE "A%"`
    Binary(Box<SQLExpr>, SQLOperator, Box<SQLExpr>),
    /// Function invocation with function name and list of argument expressions
    FunctionCall(String, Vec<SQLExpr>),
    Insert,
    Update,
    Delete,
    Select,
    CreateTable,
}

#[derive(Debug)]
pub enum ParserError {
    WrongToken { expected: Vec<SQLToken>, actual: SQLToken, line: usize, col: usize },
    Custom(String)
}

impl From<TokenizerError> for ParserError {
    fn from(e: TokenizerError) -> Self {
        ParserError::Custom(format!("{:?}", e))
    }
}


pub trait SQLParser {
    fn parse_expr(&mut self) -> Result<Option<Box<SQLExpr>>, ParserError>;
    /// parse the prefix and stop once an infix operator is reached
    fn parse_prefix(&mut self) -> Result<Option<Box<SQLExpr>>, ParserError> ;
    /// parse the next infix expression, returning None if the precedence has changed
    fn parse_infix(&mut self, left: &SQLExpr, precedence: usize) -> Result<Option<Box<SQLExpr>>, ParserError>;
}


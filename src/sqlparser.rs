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

//! SQL Parser

use super::dialect::Dialect;
use super::sqlast::*;
use super::sqltokenizer::*;
use chrono::{NaiveDate,
             NaiveDateTime,
             NaiveTime,
             offset::{FixedOffset,
                 TimeZone,
             },
             DateTime,
             Utc,
            };

#[derive(Debug, Clone)]
pub enum ParserError {
    TokenizerError(String),
    ParserError(String),
}

macro_rules! parser_err {
    ($MSG:expr) => {
        Err(ParserError::ParserError($MSG.to_string()))
    };
}

impl From<TokenizerError> for ParserError {
    fn from(e: TokenizerError) -> Self {
        ParserError::TokenizerError(format!("{:?}", e))
    }
}

/// SQL Parser
pub struct Parser {
    tokens: Vec<Token>,
    index: usize,
}

impl Parser {
    /// Parse the specified tokens
    pub fn new(tokens: Vec<Token>) -> Self {
        Parser {
            tokens: tokens,
            index: 0,
        }
    }

    /// Parse a SQL statement and produce an Abstract Syntax Tree (AST)
    pub fn parse_sql(dialect: &Dialect, sql: String) -> Result<ASTNode, ParserError> {
        let mut tokenizer = Tokenizer::new(dialect, &sql);
        let tokens = tokenizer.tokenize()?;
        let mut parser = Parser::new(tokens);
        parser.parse()
    }

    /// Parse a new expression
    pub fn parse(&mut self) -> Result<ASTNode, ParserError> {
        self.parse_expr(0)
    }

    /// Parse tokens until the precedence changes
    pub fn parse_expr(&mut self, precedence: u8) -> Result<ASTNode, ParserError> {
        debug!("parsing expr");
        let mut expr = self.parse_prefix()?;
        debug!("prefix: {:?}", expr);
        loop {
            let next_precedence = self.get_next_precedence()?;
            debug!("next precedence: {:?}", next_precedence);
            if precedence >= next_precedence {
                break;
            }

            if let Some(infix_expr) = self.parse_infix(expr.clone(), next_precedence)? {
                expr = infix_expr;
            }
        }
        Ok(expr)
    }

    /// Parse an expression prefix
    pub fn parse_prefix(&mut self) -> Result<ASTNode, ParserError> {
        match self.next_token() {
            Some(t) => {
                match t {
                    Token::Keyword(k) => match k.to_uppercase().as_ref() {
                        "SELECT" => Ok(self.parse_select()?),
                        "CREATE" => Ok(self.parse_create()?),
                        "DELETE" => Ok(self.parse_delete()?),
                        "INSERT" => Ok(self.parse_insert()?),
                        "COPY"  => Ok(self.parse_copy()?),
                        "TRUE" => {
                            self.prev_token();
                            self.parse_sql_value()
                        }
                        "FALSE" => {
                            self.prev_token();
                            self.parse_sql_value()
                        }
                        "NULL" => {
                            self.prev_token();
                            self.parse_sql_value()
                        }
                        _ => return parser_err!(format!("No prefix parser for keyword {}", k)),
                    },
                    Token::Mult => Ok(ASTNode::SQLWildcard),
                    Token::Identifier(id) => {
                        if "CAST" == id.to_uppercase(){
                            self.parse_cast_expression()
                        }else{
                            match self.peek_token() {
                                Some(Token::LParen) => {
                                    self.parse_function_or_pg_cast(&id)
                                }
                                Some(Token::Period) => {
                                    let mut id_parts: Vec<String> = vec![id];
                                    while self.peek_token() == Some(Token::Period) {
                                        self.consume_token(&Token::Period)?;
                                        match self.next_token() {
                                            Some(Token::Identifier(id)) => id_parts.push(id),
                                            _ => {
                                                return parser_err!(format!(
                                                    "Error parsing compound identifier"
                                                ))
                                            }
                                        }
                                    }
                                    Ok(ASTNode::SQLCompoundIdentifier(id_parts))
                                }
                                _ => Ok(ASTNode::SQLIdentifier(id)),
                            }
                        }
                    }
                    Token::Number(ref n) => {
                        self.prev_token();
                        self.parse_sql_value()
                    },
                    Token::String(ref s) => {
                        self.prev_token();
                        self.parse_sql_value()
                    }
                    Token::SingleQuotedString(ref s) => {
                        self.prev_token();
                        self.parse_sql_value()
                    }
                    Token::DoubleQuotedString(ref s) => {
                        self.prev_token();
                        self.parse_sql_value()
                    }
                    _ => parser_err!(format!(
                        "Prefix parser expected a keyword but found {:?}",
                        t
                    )),
                }
            }
            None => parser_err!(format!("Prefix parser expected a keyword but hit EOF")),
        }
    }

    pub fn parse_function_or_pg_cast(&mut self, id: &str) -> Result<ASTNode, ParserError> {
        let func = self.parse_function(&id)?;
        if let Some(Token::DoubleColon) = self.peek_token(){
            self.parse_pg_cast(func)
        }else{
            Ok(func)
        }
    }

    pub fn parse_function(&mut self, id: &str) -> Result<ASTNode, ParserError> {
        self.consume_token(&Token::LParen)?;
        if let Ok(true) = self.consume_token(&Token::RParen){
            Ok(ASTNode::SQLFunction { id: id.to_string(), args: vec![] })
        }else{
            let args = self.parse_expr_list()?;
            self.consume_token(&Token::RParen)?;
            Ok(ASTNode::SQLFunction { id: id.to_string(), args })
        }
    }

    /// Parse a SQL CAST function e.g. `CAST(expr AS FLOAT)`
    pub fn parse_cast_expression(&mut self) -> Result<ASTNode, ParserError> {
        self.consume_token(&Token::LParen)?;
        let expr = self.parse_expr(0)?;
        self.consume_token(&Token::Keyword("AS".to_string()))?;
        let data_type = self.parse_data_type()?;
        self.consume_token(&Token::RParen)?;
        Ok(ASTNode::SQLCast {
            expr: Box::new(expr),
            data_type,
        })
    }


    /// Parse a postgresql casting style which is in the form or expr::datatype
    pub fn parse_pg_cast(&mut self, expr: ASTNode) -> Result<ASTNode, ParserError> {
        let ast = self.consume_token(&Token::DoubleColon)?;
        let datatype = if let Ok(data_type) = self.parse_data_type(){
            Ok(data_type)
        }else if let Ok(table_name) = self.parse_tablename(){
            Ok(SQLType::Custom(table_name))
        }else{
             parser_err!("Expecting datatype or identifier")
        };
        let pg_cast = ASTNode::SQLCast{
                expr: Box::new(expr),
                data_type: datatype?,
            };
        if let Some(Token::DoubleColon) = self.peek_token(){
            self.parse_pg_cast(pg_cast)
        }else{
            Ok(pg_cast)
        }
    }

    /// Parse an expression infix (typically an operator)
    pub fn parse_infix(
        &mut self,
        expr: ASTNode,
        precedence: u8,
    ) -> Result<Option<ASTNode>, ParserError> {
        debug!("parsing infix");
        match self.next_token() {
            Some(tok) => match tok {
                Token::Keyword(ref k) => if k == "IS" {
                    if self.parse_keywords(vec!["NULL"]) {
                        Ok(Some(ASTNode::SQLIsNull(Box::new(expr))))
                    } else if self.parse_keywords(vec!["NOT", "NULL"]) {
                        Ok(Some(ASTNode::SQLIsNotNull(Box::new(expr))))
                    } else {
                        parser_err!("Invalid tokens after IS")
                    }
                } else {
                    Ok(Some(ASTNode::SQLBinaryExpr {
                        left: Box::new(expr),
                        op: self.to_sql_operator(&tok)?,
                        right: Box::new(self.parse_expr(precedence)?),
                    }))
                },
                Token::Eq
                | Token::Neq
                | Token::Gt
                | Token::GtEq
                | Token::Lt
                | Token::LtEq
                | Token::Plus
                | Token::Minus
                | Token::Mult
                | Token::Mod
                | Token::Div => Ok(Some(ASTNode::SQLBinaryExpr {
                    left: Box::new(expr),
                    op: self.to_sql_operator(&tok)?,
                    right: Box::new(self.parse_expr(precedence)?),
                })),
                | Token::DoubleColon => {
                    let pg_cast = self.parse_pg_cast(expr)?;
                    Ok(Some(pg_cast))
                },
                _ => parser_err!(format!("No infix parser for token {:?}", tok)),
            },
            None => Ok(None),
        }
    }

    /// Convert a token operator to an AST operator
    pub fn to_sql_operator(&self, tok: &Token) -> Result<SQLOperator, ParserError> {
        match tok {
            &Token::Eq => Ok(SQLOperator::Eq),
            &Token::Neq => Ok(SQLOperator::NotEq),
            &Token::Lt => Ok(SQLOperator::Lt),
            &Token::LtEq => Ok(SQLOperator::LtEq),
            &Token::Gt => Ok(SQLOperator::Gt),
            &Token::GtEq => Ok(SQLOperator::GtEq),
            &Token::Plus => Ok(SQLOperator::Plus),
            &Token::Minus => Ok(SQLOperator::Minus),
            &Token::Mult => Ok(SQLOperator::Multiply),
            &Token::Div => Ok(SQLOperator::Divide),
            &Token::Mod => Ok(SQLOperator::Modulus),
            &Token::Keyword(ref k) if k == "AND" => Ok(SQLOperator::And),
            &Token::Keyword(ref k) if k == "OR" => Ok(SQLOperator::Or),
            _ => parser_err!(format!("Unsupported SQL operator {:?}", tok)),
        }
    }

    /// Get the precedence of the next token
    pub fn get_next_precedence(&self) -> Result<u8, ParserError> {
        if let Some(token) = self.peek_token() {
            self.get_precedence(&token)
        } else {
            Ok(0)
        }
    }

    /// Get the precedence of a token
    pub fn get_precedence(&self, tok: &Token) -> Result<u8, ParserError> {
        debug!("get_precedence() {:?}", tok);

        match tok {
            &Token::Keyword(ref k) if k == "OR" => Ok(5),
            &Token::Keyword(ref k) if k == "AND" => Ok(10),
            &Token::Keyword(ref k) if k == "IS" => Ok(15),
            &Token::Eq | &Token::Lt | &Token::LtEq | &Token::Neq | &Token::Gt | &Token::GtEq => {
                Ok(20)
            }
            &Token::Plus | &Token::Minus => Ok(30),
            &Token::Mult | &Token::Div | &Token::Mod => Ok(40),
            &Token::DoubleColon => Ok(50),
            _ => Ok(0),
        }
    }

    pub fn peek_token(&self) -> Option<Token> {
        self.peek_token_skip_whitespace()
    }


    pub fn skip_whitespace(&mut self) -> Option<Token> {
        loop {
            match self.next_token_no_skip(){
                Some(Token::Whitespace(ws)) => {
                    continue;
                }
                token => {
                    return token;
                }
            }
        }
    }

    /// get the index for non whitepsace token
    fn til_non_whitespace(&self) -> Option<usize> {
        let mut index = self.index;
        loop {
            match self.token_at(index){
                Some(Token::Whitespace(ws)) => {
                    index = index + 1;
                }
                Some(token) => {
                    return Some(index);
                }
                None => {
                    return None;
                }
            }
        }
    }

    /// see the token at this index
    fn token_at(&self, n: usize) -> Option<Token>{
        if let Some(token) = self.tokens.get(n){
            Some(token.clone())
        }else{
            None
        }
    }

    pub fn peek_token_skip_whitespace(&self) -> Option<Token> {
        if let Some(n) = self.til_non_whitespace(){
            self.token_at(n)
        }else{
            None
        }
    }


    /// Get the next token skipping whitespace and increment the token index
    pub fn next_token(&mut self) -> Option<Token> {
        self.skip_whitespace()
    }

    pub fn next_token_no_skip(&mut self) -> Option<Token> {
        if self.index < self.tokens.len() {
            self.index = self.index + 1;
            Some(self.tokens[self.index - 1].clone())
        } else {
            None
        }
    }

    /// if prev token is whitespace skip it
    /// if prev token is not whitespace skipt it as well
    pub fn prev_token_skip_whitespace(&mut self) -> Option<Token>{
        loop{
            match self.prev_token_no_skip(){
                Some(Token::Whitespace(ws)) => {
                    continue;
                }
                token => {
                    return token;
                },
            }
        }
    }

    pub fn prev_token(&mut self) -> Option<Token> {
        self.prev_token_skip_whitespace()
    }

    /// Get the previous token and decrement the token index
    pub fn prev_token_no_skip(&mut self) -> Option<Token> {
        if self.index > 0 {
            self.index = self.index - 1;
            Some(self.tokens[self.index].clone())
        } else {
            None
        }
    }

    /// Look for an expected keyword and consume it if it exists
    pub fn parse_keyword(&mut self, expected: &'static str) -> bool {
        match self.peek_token() {
            Some(Token::Keyword(k)) => {
                if expected.eq_ignore_ascii_case(k.as_str()) {
                    self.next_token();
                    true
                } else {
                    false
                }
            }
            _ => false,
        }
    }

    /// Look for an expected sequence of keywords and consume them if they exist
    pub fn parse_keywords(&mut self, keywords: Vec<&'static str>) -> bool {
        let index = self.index;
        for keyword in keywords {
            if !self.parse_keyword(&keyword) {
                //println!("parse_keywords aborting .. did not find {}", keyword);
                // reset index and return immediately
                self.index = index;
                return false;
            }
        }
        true
    }

    //TODO: this function is inconsistent and sometimes returns bool and sometimes fails

    /// Consume the next token if it matches the expected token, otherwise return an error
    pub fn consume_token(&mut self, expected: &Token) -> Result<bool, ParserError> {
        match self.peek_token() {
            Some(ref t) => if *t == *expected {
                self.next_token();
                Ok(true)
            } else {
                Ok(false)
            },
            other => parser_err!(format!(
                "expected token {:?} but was {:?}",
                expected,
                other,
            )),
        }
    }


    /// Parse a SQL CREATE statement
    pub fn parse_create(&mut self) -> Result<ASTNode, ParserError> {
        if self.parse_keywords(vec!["TABLE"]) {
            let table_name = self.parse_tablename()?;
            // parse optional column list (schema)
            let mut columns = vec![];
            if self.consume_token(&Token::LParen)? {
                loop {
                    if let Some(Token::Identifier(column_name)) = self.next_token() {
                        if let Ok(data_type) = self.parse_data_type() {
                            let is_primary = self.parse_keywords(vec!["PRIMARY", "KEY"]);
                            let is_unique = self.parse_keyword("UNIQUE");
                            let default = if self.parse_keyword("DEFAULT"){
                                let expr = self.parse_expr(0)?;
                                Some(Box::new(expr))
                            }else{
                                None
                            };
                            let allow_null = if self.parse_keywords(vec!["NOT", "NULL"]) {
                                false
                            } else if self.parse_keyword("NULL") {
                                true
                            } else {
                                true
                            };
                            debug!("default: {:?}", default);

                            match self.peek_token() {
                                Some(Token::Comma) => {
                                    self.next_token();
                                    columns.push(SQLColumnDef {
                                        name: column_name,
                                        data_type: data_type,
                                        allow_null,
                                        is_primary,
                                        is_unique,
                                        default,
                                    });
                                }
                                Some(Token::RParen) => {
                                    self.next_token();
                                    columns.push(SQLColumnDef {
                                        name: column_name,
                                        data_type: data_type,
                                        allow_null,
                                        is_primary,
                                        is_unique,
                                        default,
                                    });
                                    break;
                                }
                                other => {
                                    return parser_err!(
                                        format!("Expected ',' or ')' after column definition but found {:?}", other)
                                    );
                                }
                            }
                        } else {
                            return parser_err!(
                                format!("Error parsing data type in column definition near: {:?}", self.peek_token())
                            );
                        }
                    } else {
                        return parser_err!("Error parsing column name");
                    }
                }
            }
            Ok(ASTNode::SQLCreateTable { name: table_name, columns })
        } else {
            parser_err!(format!(
                "Unexpected token after CREATE: {:?}",
                self.peek_token()
            ))
        }
    }


    /// Parse a copy statement
    pub fn parse_copy(&mut self) -> Result<ASTNode, ParserError> {
        let table_name = self.parse_tablename()?;
        let columns = if self.consume_token(&Token::LParen)?{
            let column_names = self.parse_column_names()?;
            self.consume_token(&Token::RParen)?;
            column_names
        }else{
            vec![]
        };
        self.parse_keyword("FROM");
        self.parse_keyword("STDIN");
        self.consume_token(&Token::SemiColon);
        let values = self.parse_tsv()?;
        Ok(ASTNode::SQLCopy{table_name, columns, values})
    }

    /// Parse a tab separated values in
    /// COPY payload
    fn parse_tsv(&mut self) -> Result<Vec<Value>, ParserError>{
        let values = self.parse_tab_value()?;
        Ok(values)

    }

    fn parse_sql_value(&mut self) -> Result<ASTNode, ParserError> {
        Ok(ASTNode::SQLValue(self.parse_value()?))
    }

    fn parse_tab_value(&mut self) -> Result<Vec<Value>, ParserError> {
        let mut values = vec![];
        let mut content = String::from("");
        while let Some(t) = self.next_token_no_skip(){
            match t{
                Token::Whitespace(Whitespace::Tab) => {
                    values.push(Value::String(content.to_string()));
                    content.clear();
                }
                Token::Whitespace(Whitespace::Newline) => {
                    values.push(Value::String(content.to_string()));
                    content.clear();
                }
                Token::Backslash => {
                    if let Ok(true) = self.consume_token(&Token::Period) {
                        return Ok(values);
                    }else{
                        continue;
                    }
                }
                _ => {
                    content.push_str(&t.to_string());
                }
            }
        }
        Ok(values)
    }

    fn parse_value(&mut self) -> Result<Value, ParserError> {
        match self.next_token() {
            Some(t) => {
                match t {
                    Token::Keyword(k) => match k.to_uppercase().as_ref() {
                        "TRUE" => Ok(Value::Boolean(true)),
                        "FALSE" => Ok(Value::Boolean(false)),
                        "NULL" => Ok(Value::Null),
                        _ => return parser_err!(format!("No value parser for keyword {}", k)),
                    },
                    //TODO: parse the timestamp here
                    Token::Number(ref n) if n.contains(".") => match n.parse::<f64>() {
                        Ok(n) => Ok(Value::Double(n)),
                        Err(e) => parser_err!(format!("Could not parse '{}' as i64: {}", n, e)),
                    },
                    Token::Number(ref n) => match n.parse::<i64>() {
                        Ok(n) => {
                            if let Some(Token::Minus) = self.peek_token(){
                                self.prev_token();
                                self.parse_timestamp_value()
                            }else{
                                Ok(Value::Long(n))
                            }
                        }
                        Err(e) => parser_err!(format!("Could not parse '{}' as i64: {}", n, e)),
                    },
                    Token::Identifier(id) => Ok(Value::String(id.to_string())),
                    Token::String(ref s) => Ok(Value::String(s.to_string())),
                    Token::SingleQuotedString(ref s) => Ok(Value::SingleQuotedString(s.to_string())),
                    Token::DoubleQuotedString(ref s) => Ok(Value::DoubleQuotedString(s.to_string())),
                    other => parser_err!(format!("Unsupported value: {:?}", self.peek_token())),
                }
            }
            None => parser_err!("Expecting a value, but found EOF"),
        }
    }

    /// Parse a literal integer/long
    pub fn parse_literal_int(&mut self) -> Result<i64, ParserError> {
        match self.next_token() {
            Some(Token::Number(s)) => s.parse::<i64>().map_err(|e| {
                ParserError::ParserError(format!("Could not parse '{}' as i64: {}", s, e))
            }),
            other => parser_err!(format!("Expected literal int, found {:?}", other)),
        }
    }

    /// Parse a literal integer/long
    pub fn parse_literal_double(&mut self) -> Result<f64, ParserError> {
        match self.next_token() {
            Some(Token::Number(s)) => s.parse::<f64>().map_err(|e| {
                ParserError::ParserError(format!("Could not parse '{}' as i64: {}", s, e))
            }),
            other => parser_err!(format!("Expected literal int, found {:?}", other)),
        }
    }

    /// Parse a literal string
    pub fn parse_literal_string(&mut self) -> Result<String, ParserError> {
        match self.next_token() {
            Some(Token::String(ref s)) => Ok(s.clone()),
            other => parser_err!(format!("Expected literal string, found {:?}", other)),
        }
    }


    pub fn parse_timezone_offset(&mut self) -> Result<i8, ParserError> {
        match self.next_token(){
            Some(Token::Plus) => {
                let n = self.parse_literal_int()?;
                Ok(n as i8)
            }
            Some(Token::Minus) => {
                let n = self.parse_literal_int()?;
                Ok(-n as i8)
            }
            other => parser_err!(format!("Expecting `+` or `-` in timezone, but found {:?}", other)),
        }
    }


    pub fn parse_timestamp_value(&mut self) -> Result<Value, ParserError> {
        let year = self.parse_literal_int()?;
        let date = self.parse_date(year)?;
        if let Ok(time) = self.parse_time(){
            let date_time = NaiveDateTime::new(date, time);
            match self.peek_token(){
                Some(token) => match token{
                        Token::Plus | Token::Minus => {
                            let tz = self.parse_timezone_offset()?;
                            let offset =  FixedOffset::east(tz as i32 * 3600);
                            Ok(Value::Timestamp(DateTime::from_utc(date_time, offset)))
                        }
                        _ => Ok(Value::DateTime(date_time)),
                    },
                _ => Ok(Value::DateTime(date_time)),
            }
        }else{
            parser_err!(format!("Expecting time after date, but found {:?}", self.peek_token()))
        }
    }

    pub fn parse_date(&mut self, year: i64) -> Result<NaiveDate, ParserError> {
        if let Ok(true) = self.consume_token(&Token::Minus){
            let month = self.parse_literal_int()?;
            if let Ok(true) = self.consume_token(&Token::Minus){
                let day = self.parse_literal_int()?;
                let date = NaiveDate::from_ymd(year as i32, month as u32, day as u32);
                Ok(date)
            }else{
                parser_err!(format!("Expecting `-` for date separator, found {:?}", self.peek_token()))
            }
        }else{
            parser_err!(format!("Expecting `-` for date separator, found {:?}", self.peek_token()))
        }

    }

    pub fn parse_time(&mut self) -> Result<NaiveTime, ParserError> {
        let hour = self.parse_literal_int()?;
        self.consume_token(&Token::Colon)?;
        let min = self.parse_literal_int()?;
        self.consume_token(&Token::Colon)?;
        let sec = self.parse_literal_double()?;
        let ms = (sec.fract() * 1000.0).round();
        if let Ok(true) = self.consume_token(&Token::Period){
            let ms = self.parse_literal_int()?;
            Ok(NaiveTime::from_hms_milli(hour as u32, min as u32, sec as u32, ms as u32))
        }else{
            Ok(NaiveTime::from_hms(hour as u32, min as u32, sec as u32))
        }
    }

    /// Parse a SQL datatype (in the context of a CREATE TABLE statement for example)
    pub fn parse_data_type(&mut self) -> Result<SQLType, ParserError> {
        match self.next_token() {
            Some(Token::Keyword(k)) => match k.to_uppercase().as_ref() {
                "BOOLEAN" => Ok(SQLType::Boolean),
                "FLOAT" => Ok(SQLType::Float(self.parse_optional_precision()?)),
                "REAL" => Ok(SQLType::Real),
                "DOUBLE" => if self.parse_keyword("PRECISION"){
                    Ok(SQLType::Double)
                }else{
                    Ok(SQLType::Double)
                }
                "SMALLINT" => Ok(SQLType::SmallInt),
                "INT" | "INTEGER" => Ok(SQLType::Int),
                "BIGINT" => Ok(SQLType::BigInt),
                "VARCHAR" => Ok(SQLType::Varchar(self.parse_optional_precision()?)),
                "CHARACTER" => {
                    if self.parse_keyword("VARYING"){
                        Ok(SQLType::Varchar(self.parse_optional_precision()?))
                    }else{
                        Ok(SQLType::Char(self.parse_optional_precision()?))
                    }
                }
                "UUID" => Ok(SQLType::Uuid),
                "DATE" => Ok(SQLType::Date),
                "TIMESTAMP" => if self.parse_keyword("WITH"){
                    if self.parse_keywords(vec!["TIME","ZONE"]){
                        Ok(SQLType::Timestamp)
                    }else{
                        parser_err!(format!("Expecting 'time zone', found: {:?}", self.peek_token()))
                    }
                }else if self.parse_keyword("WITHOUT"){
                    if self.parse_keywords(vec!["TIME","ZONE"]){
                        Ok(SQLType::Timestamp)
                    }else{
                        parser_err!(format!("Expecting 'time zone', found: {:?}", self.peek_token()))
                    }
                }else{
                    Ok(SQLType::Timestamp)
                }
                "TIME" => if self.parse_keyword("WITH"){
                    if self.parse_keywords(vec!["TIME","ZONE"]){
                        Ok(SQLType::Time)
                    }else{
                        parser_err!(format!("Expecting 'time zone', found: {:?}", self.peek_token()))
                    }
                }else if self.parse_keyword("WITHOUT"){
                    if self.parse_keywords(vec!["TIME","ZONE"]){
                        Ok(SQLType::Time)
                    }else{
                        parser_err!(format!("Expecting 'time zone', found: {:?}", self.peek_token()))
                    }
                }else{
                    Ok(SQLType::Timestamp)
                }
                "REGCLASS" => Ok(SQLType::Regclass),
                "TEXT" => {
                    if let Ok(true) = self.consume_token(&Token::LBracket){
                        self.consume_token(&Token::RBracket)?;
                        Ok(SQLType::Array(Box::new(SQLType::Text)))
                    }else{
                        Ok(SQLType::Text)
                    }
                }
                "BYTEA" => Ok(SQLType::Bytea),
                "NUMERIC" => {
                    let (precision, scale) = self.parse_optional_precision_scale()?;
                    Ok(SQLType::Decimal(precision, scale))
                }
                _ => parser_err!(format!("Invalid data type '{:?}'", k)),
            },
            Some(Token::Identifier(id)) => {
                if let Ok(true) = self.consume_token(&Token::Period) {
                    let ids = self.parse_tablename()?;
                    Ok(SQLType::Custom(format!("{}.{}",id,ids)))
                }else{
                    Ok(SQLType::Custom(id))
                }
            }
            other => parser_err!(format!("Invalid data type: '{:?}'", other)),
        }
    }


    pub fn parse_compound_identifier(&mut self, separator: &Token) -> Result<ASTNode, ParserError> {
        let mut idents = vec![];
        let mut expect_identifier = true;
        loop {
            let token = &self.next_token();
            match token{
                Some(token) => match token{
                    Token::Identifier(s) => if expect_identifier{
                        expect_identifier = false;
                        idents.push(s.to_string());
                    }else{
                        self.prev_token();
                        break;
                    }
                    token if token == separator => {
                        if expect_identifier{
                            return parser_err!(format!("Expecting identifier, found {:?}", token));
                        }else{
                            expect_identifier = true;
                            continue;
                        }
                    }
                    _ => {
                        self.prev_token();
                        break;
                    }
                }
                None => {
                    self.prev_token();
                    break;
                }
            }
        }
        Ok(ASTNode::SQLCompoundIdentifier(idents))
    }

    pub fn parse_tablename(&mut self) -> Result<String, ParserError> {
        let identifier = self.parse_compound_identifier(&Token::Period)?;
        match identifier{
            ASTNode::SQLCompoundIdentifier(idents) => Ok(idents.join(".")),
            other => parser_err!(format!("Expecting compound identifier, found: {:?}", other)),
        }
    }

    pub fn parse_column_names(&mut self) -> Result<Vec<String>, ParserError> {
        let identifier = self.parse_compound_identifier(&Token::Comma)?;
        match identifier{
            ASTNode::SQLCompoundIdentifier(idents) => Ok(idents),
            other => parser_err!(format!("Expecting compound identifier, found: {:?}", other)),
        }
    }

    pub fn parse_precision(&mut self) -> Result<usize, ParserError> {
        //TODO: error handling
        Ok(self.parse_optional_precision()?.unwrap())
    }

    pub fn parse_optional_precision(&mut self) -> Result<Option<usize>, ParserError> {
        if self.consume_token(&Token::LParen)? {
            let n = self.parse_literal_int()?;
            //TODO: check return value of reading rparen
            self.consume_token(&Token::RParen)?;
            Ok(Some(n as usize))
        } else {
            Ok(None)
        }
    }

    pub fn parse_optional_precision_scale(&mut self) -> Result<(usize, Option<usize>), ParserError> {
        if self.consume_token(&Token::LParen)? {
            let n = self.parse_literal_int()?;
            let scale = if let Ok(true) = self.consume_token(&Token::Comma){
                Some(self.parse_literal_int()? as usize)
            }else{
                None
            };
            self.consume_token(&Token::RParen)?;
            Ok((n as usize, scale))
        } else {
            parser_err!("Expecting `(`")
        }
    }

    pub fn parse_delete(&mut self) -> Result<ASTNode, ParserError> {
        let relation: Option<Box<ASTNode>> = if self.parse_keyword("FROM") {
            Some(Box::new(self.parse_expr(0)?))
        } else {
            None
        };

        let order_by = if self.parse_keywords(vec!["ORDER", "BY"]) {
            Some(self.parse_order_by_expr_list()?)
        } else {
            None
        };

        let limit = if self.parse_keyword("LIMIT") {
            self.parse_limit()?
        } else {
            None
        };

        let selection = if self.parse_keyword("WHERE") {
            Some(Box::new(self.parse_expr(0)?))
        } else {
            None
        };

        // parse next token
        if let Some(next_token) = self.peek_token() {
            parser_err!(format!(
                "Unexpected token at end of DELETE: {:?}",
                next_token
            ))
        } else {
            Ok(ASTNode::SQLDelete {
                relation,
                selection,
                order_by,
                limit,
            })
        }
    }

    /// Parse a SELECT statement
    pub fn parse_select(&mut self) -> Result<ASTNode, ParserError> {
        let projection = self.parse_expr_list()?;

        let relation: Option<Box<ASTNode>> = if self.parse_keyword("FROM") {
            //TODO: add support for JOIN
            Some(Box::new(self.parse_expr(0)?))
        } else {
            None
        };

        let selection = if self.parse_keyword("WHERE") {
            let expr = self.parse_expr(0)?;
            Some(Box::new(expr))
        } else {
            None
        };

        let group_by = if self.parse_keywords(vec!["GROUP", "BY"]) {
            Some(self.parse_expr_list()?)
        } else {
            None
        };

        let having = if self.parse_keyword("HAVING") {
            Some(Box::new(self.parse_expr(0)?))
        } else {
            None
        };

        let order_by = if self.parse_keywords(vec!["ORDER", "BY"]) {
            Some(self.parse_order_by_expr_list()?)
        } else {
            None
        };

        let limit = if self.parse_keyword("LIMIT") {
            self.parse_limit()?
        } else {
            None
        };

        if let Some(next_token) = self.peek_token() {
            parser_err!(format!(
                "Unexpected token at end of SELECT: {:?}",
                next_token
            ))
        } else {
            Ok(ASTNode::SQLSelect {
                projection,
                selection,
                relation,
                limit,
                order_by,
                group_by,
                having,
            })
        }
    }

    /// Parse an INSERT statement
    pub fn parse_insert(&mut self) -> Result<ASTNode, ParserError> {
        self.parse_keyword("INTO");
        let table_name = self.parse_tablename()?;
        let columns = if self.consume_token(&Token::LParen)?{
            let column_names = self.parse_column_names()?;
            self.consume_token(&Token::RParen)?;
            column_names
        }else{
            vec![]
        };
        self.parse_keyword("VALUES");
        self.consume_token(&Token::LParen)?;
        let values = self.parse_expr_list()?;
        self.consume_token(&Token::RParen)?;
        Ok(ASTNode::SQLInsert{table_name, columns, values: vec![values]})
    }

    /// Parse a comma-delimited list of SQL expressions
    pub fn parse_expr_list(&mut self) -> Result<Vec<ASTNode>, ParserError> {
        let mut expr_list: Vec<ASTNode> = vec![];
        loop {
            expr_list.push(self.parse_expr(0)?);
            if let Some(t) = self.peek_token() {
                if t == Token::Comma {
                    self.next_token();
                } else {
                    break;
                }
            } else {
                //EOF
                break;
            }
        }
        Ok(expr_list)
    }

    /// Parse a comma-delimited list of SQL ORDER BY expressions
    pub fn parse_order_by_expr_list(&mut self) -> Result<Vec<SQLOrderByExpr>, ParserError> {
        let mut expr_list: Vec<SQLOrderByExpr> = vec![];
        loop {
            let expr = self.parse_expr(0)?;

            // look for optional ASC / DESC specifier
            let asc = match self.peek_token() {
                Some(Token::Keyword(k)) => {
                    self.next_token(); // consume it
                    match k.to_uppercase().as_ref() {
                        "ASC" => true,
                        "DESC" => false,
                        _ => {
                            return parser_err!(format!(
                                "Invalid modifier for ORDER BY expression: {:?}",
                                k
                            ))
                        }
                    }
                }
                Some(Token::Comma) => true,
                Some(other) => {
                    return parser_err!(format!("Unexpected token after ORDER BY expr: {:?}", other))
                }
                None => true,
            };

            expr_list.push(SQLOrderByExpr::new(Box::new(expr), asc));

            if let Some(t) = self.peek_token() {
                if t == Token::Comma {
                    self.next_token();
                } else {
                    break;
                }
            } else {
                // EOF
                break;
            }
        }
        Ok(expr_list)
    }

    /// Parse a LIMIT clause
    pub fn parse_limit(&mut self) -> Result<Option<Box<ASTNode>>, ParserError> {
        if self.parse_keyword("ALL") {
            Ok(None)
        } else {
            self.parse_literal_int()
                .map(|n| Some(Box::new(ASTNode::SQLValue(Value::Long(n)))))
        }
    }
}

#[cfg(test)]
mod tests {

    use super::super::dialect::PostgreSqlDialect;
    use super::*;

    #[test]
    fn test_prev_index(){
        let sql: &str = "SELECT version()";
        let mut tokenizer = Tokenizer::new(&PostgreSqlDialect{}, &sql);
        let tokens = tokenizer.tokenize().expect("error tokenizing");
        let mut parser = Parser::new(tokens);
        assert_eq!(parser.prev_token(), None);
        assert_eq!(parser.next_token(), Some(Token::Keyword("SELECT".into())));
        assert_eq!(parser.next_token(), Some(Token::Identifier("version".into())));
        assert_eq!(parser.prev_token(), Some(Token::Identifier("version".into())));
        assert_eq!(parser.peek_token(), Some(Token::Identifier("version".into())));
        assert_eq!(parser.prev_token(), Some(Token::Keyword("SELECT".into())));
        assert_eq!(parser.prev_token(), None);
    }

    #[test]
    fn parse_delete_statement() {
        let sql: &str = "DELETE FROM 'table'";

        match parse_sql(&sql) {
            ASTNode::SQLDelete { relation, .. } => {
                assert_eq!(
                    Some(Box::new(ASTNode::SQLValue(Value::SingleQuotedString("table".to_string())))),
                    relation
                );
            }

            _ => assert!(false),
        }
    }

    #[test]
    fn parse_where_delete_statement() {
        let sql: &str = "DELETE FROM 'table' WHERE name = 5";

        use self::ASTNode::*;
        use self::SQLOperator::*;

        match parse_sql(&sql) {
            ASTNode::SQLDelete {
                relation,
                selection,
                ..
            } => {
                assert_eq!(
                    Some(Box::new(ASTNode::SQLValue(Value::SingleQuotedString("table".to_string())))),
                    relation
                );

                assert_eq!(
                    SQLBinaryExpr {
                        left: Box::new(SQLIdentifier("name".to_string())),
                        op: Eq,
                        right: Box::new(ASTNode::SQLValue(Value::Long(5))),
                    },
                    *selection.unwrap(),
                );
            }

            _ => assert!(false),
        }
    }

    #[test]
    fn parse_simple_select() {
        let sql = String::from("SELECT id, fname, lname FROM customer WHERE id = 1 LIMIT 5");
        let ast = parse_sql(&sql);
        match ast {
            ASTNode::SQLSelect {
                projection, limit, ..
            } => {
                assert_eq!(3, projection.len());
                assert_eq!(Some(Box::new(ASTNode::SQLValue(Value::Long(5)))), limit);
            }
            _ => assert!(false),
        }
    }

    #[test]
    fn parse_simple_insert() {
        let sql = String::from("INSERT INTO customer VALUES(1, 2, 3)");
        let ast = parse_sql(&sql);
        match ast {
            ASTNode::SQLInsert {
                table_name, columns, values, ..
            } => {
                assert_eq!(table_name, "customer");
                assert!(columns.is_empty());
                assert_eq!(vec![vec![ASTNode::SQLValue(Value::Long(1)),ASTNode::SQLValue(Value::Long(2)),ASTNode::SQLValue(Value::Long(3))]], values);
            }
            _ => assert!(false),
        }
    }

    #[test]
    fn parse_common_insert() {
        let sql = String::from("INSERT INTO public.customer VALUES(1, 2, 3)");
        let ast = parse_sql(&sql);
        match ast {
            ASTNode::SQLInsert {
                table_name, columns, values, ..
            } => {
                assert_eq!(table_name, "public.customer");
                assert!(columns.is_empty());
                assert_eq!(vec![vec![ASTNode::SQLValue(Value::Long(1)),ASTNode::SQLValue(Value::Long(2)),ASTNode::SQLValue(Value::Long(3))]], values);
            }
            _ => assert!(false),
        }
    }

    #[test]
    fn parse_complex_insert() {
        let sql = String::from("INSERT INTO db.public.customer VALUES(1,2,3)");
        let ast = parse_sql(&sql);
        match ast {
            ASTNode::SQLInsert {
                table_name, columns, values, ..
            } => {
                assert_eq!(table_name, "db.public.customer");
                assert!(columns.is_empty());
                assert_eq!(vec![vec![ASTNode::SQLValue(Value::Long(1)),ASTNode::SQLValue(Value::Long(2)),ASTNode::SQLValue(Value::Long(3))]], values);
            }
            _ => assert!(false),
        }
    }

    #[test]
    fn parse_invalid_table_name() {
        let mut parser = parser("db.public..customer");
        let ast = parser.parse_tablename();
        assert!(ast.is_err());
    }

    #[test]
    fn parse_insert_with_columns() {
        let sql = String::from("INSERT INTO public.customer (id, name, active) VALUES(1, 2, 3)");
        let ast = parse_sql(&sql);
        match ast {
            ASTNode::SQLInsert {
                table_name, columns, values, ..
            } => {
                assert_eq!(table_name, "public.customer");
                assert_eq!(columns, vec!["id".to_string(), "name".to_string(), "active".to_string()]);
                assert_eq!(vec![vec![ASTNode::SQLValue(Value::Long(1)),ASTNode::SQLValue(Value::Long(2)),ASTNode::SQLValue(Value::Long(3))]], values);
            }
            _ => assert!(false),
        }
    }

    #[test]
    fn parse_select_wildcard() {
        let sql = String::from("SELECT * FROM customer");
        let ast = parse_sql(&sql);
        match ast {
            ASTNode::SQLSelect { projection, .. } => {
                assert_eq!(1, projection.len());
                assert_eq!(ASTNode::SQLWildcard, projection[0]);
            }
            _ => assert!(false),
        }
    }

    #[test]
    fn parse_select_count_wildcard() {
        let sql = String::from("SELECT COUNT(*) FROM customer");
        let ast = parse_sql(&sql);
        match ast {
            ASTNode::SQLSelect { projection, .. } => {
                assert_eq!(1, projection.len());
                assert_eq!(
                    ASTNode::SQLFunction {
                        id: "COUNT".to_string(),
                        args: vec![ASTNode::SQLWildcard],
                    },
                    projection[0]
                );
            }
            _ => assert!(false),
        }
    }

    #[test]
    fn parse_select_string_predicate() {
        let sql = String::from(
            "SELECT id, fname, lname FROM customer \
             WHERE salary != 'Not Provided' AND salary != ''",
        );
        let ast = parse_sql(&sql);
        //TODO: add assertions
    }

    #[test]
    fn parse_projection_nested_type() {
        let sql = String::from("SELECT customer.address.state FROM foo");
        let _ast = parse_sql(&sql);
        //TODO: add assertions
    }

    #[test]
    fn parse_compound_expr_1() {
        use self::ASTNode::*;
        use self::SQLOperator::*;
        let sql = String::from("a + b * c");
        let ast = parse_sql(&sql);
        assert_eq!(
            SQLBinaryExpr {
                left: Box::new(SQLIdentifier("a".to_string())),
                op: Plus,
                right: Box::new(SQLBinaryExpr {
                    left: Box::new(SQLIdentifier("b".to_string())),
                    op: Multiply,
                    right: Box::new(SQLIdentifier("c".to_string()))
                })
            },
            ast
        );
    }

    #[test]
    fn parse_compound_expr_2() {
        use self::ASTNode::*;
        use self::SQLOperator::*;
        let sql = String::from("a * b + c");
        let ast = parse_sql(&sql);
        assert_eq!(
            SQLBinaryExpr {
                left: Box::new(SQLBinaryExpr {
                    left: Box::new(SQLIdentifier("a".to_string())),
                    op: Multiply,
                    right: Box::new(SQLIdentifier("b".to_string()))
                }),
                op: Plus,
                right: Box::new(SQLIdentifier("c".to_string()))
            },
            ast
        );
    }

    #[test]
    fn parse_is_null() {
        use self::ASTNode::*;
        let sql = String::from("a IS NULL");
        let ast = parse_sql(&sql);
        assert_eq!(SQLIsNull(Box::new(SQLIdentifier("a".to_string()))), ast);
    }

    #[test]
    fn parse_is_not_null() {
        use self::ASTNode::*;
        let sql = String::from("a IS NOT NULL");
        let ast = parse_sql(&sql);
        assert_eq!(SQLIsNotNull(Box::new(SQLIdentifier("a".to_string()))), ast);
    }

    #[test]
    fn parse_select_order_by() {
        let sql = String::from(
            "SELECT id, fname, lname FROM customer WHERE id < 5 ORDER BY lname ASC, fname DESC",
        );
        let ast = parse_sql(&sql);
        match ast {
            ASTNode::SQLSelect { order_by, .. } => {
                assert_eq!(
                    Some(vec![
                        SQLOrderByExpr {
                            expr: Box::new(ASTNode::SQLIdentifier("lname".to_string())),
                            asc: true,
                        },
                        SQLOrderByExpr {
                            expr: Box::new(ASTNode::SQLIdentifier("fname".to_string())),
                            asc: false,
                        },
                    ]),
                    order_by
                );
            }
            _ => assert!(false),
        }
    }

    #[test]
    fn parse_select_group_by() {
        let sql = String::from("SELECT id, fname, lname FROM customer GROUP BY lname, fname");
        let ast = parse_sql(&sql);
        match ast {
            ASTNode::SQLSelect { group_by, .. } => {
                assert_eq!(
                    Some(vec![
                        ASTNode::SQLIdentifier("lname".to_string()),
                        ASTNode::SQLIdentifier("fname".to_string()),
                    ]),
                    group_by
                );
            }
            _ => assert!(false),
        }
    }

    #[test]
    fn parse_limit_accepts_all() {
        let sql = String::from("SELECT id, fname, lname FROM customer WHERE id = 1 LIMIT ALL");
        let ast = parse_sql(&sql);
        match ast {
            ASTNode::SQLSelect {
                projection, limit, ..
            } => {
                assert_eq!(3, projection.len());
                assert_eq!(None, limit);
            }
            _ => assert!(false),
        }
    }

    #[test]
    fn parse_cast() {
        let sql = String::from("SELECT CAST(id AS BIGINT) FROM customer");
        let ast = parse_sql(&sql);
        match ast {
            ASTNode::SQLSelect { projection, .. } => {
                assert_eq!(1, projection.len());
                assert_eq!(
                    ASTNode::SQLCast {
                        expr: Box::new(ASTNode::SQLIdentifier("id".to_string())),
                        data_type: SQLType::BigInt
                    },
                    projection[0]
                );
            }
            _ => assert!(false),
        }
    }

    #[test]
    fn parse_create_table() {
        let sql = String::from(
            "CREATE TABLE uk_cities (\
             name VARCHAR(100) NOT NULL,\
             lat DOUBLE NULL,\
             lng DOUBLE NULL)",
        );
        let ast = parse_sql(&sql);
        match ast {
            ASTNode::SQLCreateTable { name, columns } => {
                assert_eq!("uk_cities", name);
                assert_eq!(3, columns.len());

                let c_name = &columns[0];
                assert_eq!("name", c_name.name);
                assert_eq!(SQLType::Varchar(Some(100)), c_name.data_type);
                assert_eq!(false, c_name.allow_null);

                let c_lat = &columns[1];
                assert_eq!("lat", c_lat.name);
                assert_eq!(SQLType::Double, c_lat.data_type);
                assert_eq!(true, c_lat.allow_null);

                let c_lng = &columns[2];
                assert_eq!("lng", c_lng.name);
                assert_eq!(SQLType::Double, c_lng.data_type);
                assert_eq!(true, c_lng.allow_null);
            }
            _ => assert!(false),
        }
    }

    #[test]
    fn parse_create_table_with_defaults() {
        let sql = String::from(
            "CREATE TABLE public.customer (
                customer_id integer DEFAULT nextval(public.customer_customer_id_seq) NOT NULL,
                store_id smallint NOT NULL,
                first_name character varying(45) NOT NULL,
                last_name character varying(45) NOT NULL,
                email character varying(50),
                address_id smallint NOT NULL,
                activebool boolean DEFAULT true NOT NULL,
                create_date date DEFAULT now()::text NOT NULL,
                last_update timestamp without time zone DEFAULT now() NOT NULL,
                active integer NOT NULL)");
        let ast = parse_sql(&sql);
        match ast {
            ASTNode::SQLCreateTable { name, columns } => {
                assert_eq!("public.customer", name);
                assert_eq!(10, columns.len());

                let c_name = &columns[0];
                assert_eq!("customer_id", c_name.name);
                assert_eq!(SQLType::Int, c_name.data_type);
                assert_eq!(false, c_name.allow_null);

                let c_lat = &columns[1];
                assert_eq!("store_id", c_lat.name);
                assert_eq!(SQLType::SmallInt, c_lat.data_type);
                assert_eq!(false, c_lat.allow_null);

                let c_lng = &columns[2];
                assert_eq!("first_name", c_lng.name);
                assert_eq!(SQLType::Varchar(Some(45)), c_lng.data_type);
                assert_eq!(false, c_lng.allow_null);
            }
            _ => assert!(false),
        }
    }

    #[test]
    fn parse_create_table_from_pg_dump() {
        let sql = String::from("
            CREATE TABLE public.customer (
                customer_id integer DEFAULT nextval('public.customer_customer_id_seq'::regclass) NOT NULL,
                store_id smallint NOT NULL,
                first_name character varying(45) NOT NULL,
                last_name character varying(45) NOT NULL,
                info text[],
                address_id smallint NOT NULL,
                activebool boolean DEFAULT true NOT NULL,
                create_date date DEFAULT now()::date NOT NULL,
                create_date1 date DEFAULT 'now'::text::date NOT NULL,
                last_update timestamp without time zone DEFAULT now(),
                release_year public.year,
                active integer
            )");
        let ast = parse_sql(&sql);
        match ast {
            ASTNode::SQLCreateTable { name, columns } => {
                assert_eq!("public.customer", name);

                let c_name = &columns[0];
                assert_eq!("customer_id", c_name.name);
                assert_eq!(SQLType::Int, c_name.data_type);
                assert_eq!(false, c_name.allow_null);

                let c_lat = &columns[1];
                assert_eq!("store_id", c_lat.name);
                assert_eq!(SQLType::SmallInt, c_lat.data_type);
                assert_eq!(false, c_lat.allow_null);

                let c_lng = &columns[2];
                assert_eq!("first_name", c_lng.name);
                assert_eq!(SQLType::Varchar(Some(45)), c_lng.data_type);
                assert_eq!(false, c_lng.allow_null);
            }
            _ => assert!(false),
        }
    }

    #[test]
    fn parse_create_table_with_inherit() {
        let sql = String::from("
         CREATE TABLE bazaar.settings (
            settings_id uuid PRIMARY KEY DEFAULT uuid_generate_v4() NOT NULL,
            user_id uuid UNIQUE,
            value text[],
            use_metric boolean DEFAULT true
        )
        INHERITS (system.record)");
        let ast = parse_sql(&sql);
        match ast {
            ASTNode::SQLCreateTable { name, columns } => {
                assert_eq!("bazaar.settings", name);

                let c_name = &columns[0];
                assert_eq!("settings_id", c_name.name);
                assert_eq!(SQLType::Uuid, c_name.data_type);
                assert_eq!(false, c_name.allow_null);
                assert_eq!(true, c_name.is_primary);
                assert_eq!(false, c_name.is_unique);

                let c_name = &columns[1];
                assert_eq!("user_id", c_name.name);
                assert_eq!(SQLType::Uuid, c_name.data_type);
                assert_eq!(true, c_name.allow_null);
                assert_eq!(false, c_name.is_primary);
                assert_eq!(true, c_name.is_unique);
            }
            _ => assert!(false),
        }
    }

    #[test]
    fn parse_alter_table_constraint_primary_key(){
        let sql = String::from("
        ALTER TABLE ONLY bazaar.address
            ADD CONSTRAINT address_pkey PRIMARY KEY (address_id)");
        let ast = parse_sql(&sql);
    }

    #[test]
    fn parse_alter_table_constraint_foreign_key(){
        let sql = String::from("
        ALTER TABLE ONLY public.customer
            ADD CONSTRAINT customer_address_id_fkey FOREIGN KEY (address_id) REFERENCES public.address(address_id) ON UPDATE CASCADE ON DELETE RESTRICT;
        ");
        let ast = parse_sql(&sql);
    }

    #[test]
    fn parse_copy_example(){
        let sql = String::from(r#"
        COPY public.actor (actor_id, first_name, last_name, last_update, value) FROM stdin;
1	PENELOPE	GUINESS	2006-02-15 09:34:33 0.11111
2	NICK	WAHLBERG	2006-02-15 09:34:33 0.22222
3	ED	CHASE	2006-02-15 09:34:33 0.312323
4	JENNIFER	DAVIS	2006-02-15 09:34:33 0.3232
5	JOHNNY	LOLLOBRIGIDA	2006-02-15 09:34:33 1.343
6	BETTE	NICHOLSON	2006-02-15 09:34:33 5.0
7	GRACE	MOSTEL	2006-02-15 09:34:33 6.0
8	MATTHEW	JOHANSSON	2006-02-15 09:34:33 7.0
9	JOE	SWANK	2006-02-15 09:34:33 8.0
10	CHRISTIAN	GABLE	2006-02-15 09:34:33 9.1
11	ZERO	CAGE	2006-02-15 09:34:33 10.001
12	KARL	BERRY	2017-11-02 19:15:42.308637+08 11.001
A Fateful Reflection of a Waitress And a Boat who must Discover a Sumo Wrestler in Ancient China
Kwara & Kogi
{"Deleted Scenes","Behind the Scenes"}
'awe':5 'awe-inspir':4 'barbarella':1 'cat':13 'conquer':16 'dog':18 'feminist':10 'inspir':6 'monasteri':21 'must':15 'stori':7 'streetcar':2
PHP	₱ USD $

        \\.
        "#);
        let mut parser = parser(&sql);
        let ast = parser.parse();
        println!("ast: {:#?}", ast);
        assert!(ast.is_ok());
    }

    #[test]
    fn parse_timestamps_example(){
        let sql = "2016-02-15 09:43:33";
        let mut parser = parser(&sql);
        let ast = parser.parse_timestamp_value();
        assert!(ast.is_ok())
    }

    #[test]
    fn parse_timestamps_with_millis_example(){
        let sql = "2017-11-02 19:15:42.308637";
        let mut parser = parser(&sql);
        let ast = parser.parse_timestamp_value();
        assert!(ast.is_ok())
    }

    #[test]
    fn parse_example_value(){
        let sql = "SARAH.LEWIS@sakilacustomer.org";
        let mut parser = parser(&sql);
        let ast = parser.parse_value();
        assert!(ast.is_ok());
    }

    #[test]
    fn parse_scalar_function_in_projection() {
        let sql = String::from("SELECT sqrt(id) FROM foo");
        let ast = parse_sql(&sql);
        if let ASTNode::SQLSelect { projection, .. } = ast {
            assert_eq!(
                vec![ASTNode::SQLFunction {
                    id: String::from("sqrt"),
                    args: vec![ASTNode::SQLIdentifier(String::from("id"))],
                }],
                projection
            );
        } else {
            assert!(false);
        }
    }

    #[test]
    fn parse_aggregate_with_group_by() {
        let sql = String::from("SELECT a, COUNT(1), MIN(b), MAX(b) FROM foo GROUP BY a");
        let _ast = parse_sql(&sql);
        //TODO: assertions
    }

    #[test]
    fn parse_literal_string() {
        let sql = "SELECT 'one'";
        match parse_sql(&sql) {
            ASTNode::SQLSelect { ref projection, .. } => {
                assert_eq!(projection[0], ASTNode::SQLValue(Value::SingleQuotedString("one".to_string())));
            }
            _ => panic!(),
        }
    }

    #[test]
    fn parse_select_version() {
        let sql = "SELECT @@version";
        match parse_sql(&sql) {
            ASTNode::SQLSelect { ref projection, .. } => {
                assert_eq!(
                    projection[0],
                    ASTNode::SQLIdentifier("@@version".to_string())
                );
            }
            _ => panic!(),
        }
    }

    #[test]
    fn parse_function_now(){
        let sql = "now()";
        let mut parser = parser(sql);
        let ast = parser.parse();
        assert!(ast.is_ok());
    }

    fn parse_sql(sql: &str) -> ASTNode {
        debug!("sql: {}", sql);
        let mut parser = parser(sql);
        let ast = parser.parse().unwrap();
        ast
    }

    fn parser(sql: &str) -> Parser {
        let dialect = PostgreSqlDialect {};
        let mut tokenizer = Tokenizer::new(&dialect, &sql);
        let tokens = tokenizer.tokenize().unwrap();
        debug!("tokens: {:#?}", tokens);
        Parser::new(tokens)
    }

}

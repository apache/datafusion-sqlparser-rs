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

use super::sqlast::*;
use super::sqltokenizer::*;

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
    pub fn parse_sql(sql: String) -> Result<ASTNode, ParserError> {
        let mut tokenizer = Tokenizer::new(&sql);
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
        let mut expr = self.parse_prefix()?;
        loop {
            let next_precedence = self.get_next_precedence()?;
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
                        _ => return parser_err!(format!("No prefix parser for keyword {}", k)),
                    },
                    Token::Mult => Ok(ASTNode::SQLWildcard),
                    Token::Identifier(id) => {
                        match self.peek_token() {
                            Some(Token::LParen) => {
                                self.next_token(); // skip lparen
                                match id.to_uppercase().as_ref() {
                                    "CAST" => self.parse_cast_expression(),
                                    _ => {
                                        let args = self.parse_expr_list()?;
                                        self.next_token(); // skip rparen
                                        Ok(ASTNode::SQLFunction { id, args })
                                    }
                                }
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
                    Token::Number(ref n) if n.contains(".") => match n.parse::<f64>() {
                        Ok(n) => Ok(ASTNode::SQLLiteralDouble(n)),
                        Err(e) => parser_err!(format!("Could not parse '{}' as i64: {}", n, e)),
                    },
                    Token::Number(ref n) => match n.parse::<i64>() {
                        Ok(n) => Ok(ASTNode::SQLLiteralLong(n)),
                        Err(e) => parser_err!(format!("Could not parse '{}' as i64: {}", n, e)),
                    },
                    Token::String(ref s) => Ok(ASTNode::SQLLiteralString(s.to_string())),
                    _ => parser_err!(format!(
                        "Prefix parser expected a keyword but found {:?}",
                        t
                    )),
                }
            }
            None => parser_err!(format!("Prefix parser expected a keyword but hit EOF")),
        }
    }

    /// Parse a SQL CAST function e.g. `CAST(expr AS FLOAT)`
    pub fn parse_cast_expression(&mut self) -> Result<ASTNode, ParserError> {
        let expr = self.parse_expr(0)?;
        self.consume_token(&Token::Keyword("AS".to_string()))?;
        let data_type = self.parse_data_type()?;
        self.consume_token(&Token::RParen)?;
        Ok(ASTNode::SQLCast {
            expr: Box::new(expr),
            data_type,
        })
    }

    /// Parse an expression infix (typically an operator)
    pub fn parse_infix(
        &mut self,
        expr: ASTNode,
        precedence: u8,
    ) -> Result<Option<ASTNode>, ParserError> {
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
        if self.index < self.tokens.len() {
            self.get_precedence(&self.tokens[self.index])
        } else {
            Ok(0)
        }
    }

    /// Get the precedence of a token
    pub fn get_precedence(&self, tok: &Token) -> Result<u8, ParserError> {
        //println!("get_precedence() {:?}", tok);

        match tok {
            &Token::Keyword(ref k) if k == "OR" => Ok(5),
            &Token::Keyword(ref k) if k == "AND" => Ok(10),
            &Token::Keyword(ref k) if k == "IS" => Ok(15),
            &Token::Eq | &Token::Lt | &Token::LtEq | &Token::Neq | &Token::Gt | &Token::GtEq => {
                Ok(20)
            }
            &Token::Plus | &Token::Minus => Ok(30),
            &Token::Mult | &Token::Div | &Token::Mod => Ok(40),
            _ => Ok(0),
        }
    }

    /// Peek at the next token
    pub fn peek_token(&mut self) -> Option<Token> {
        if self.index < self.tokens.len() {
            Some(self.tokens[self.index].clone())
        } else {
            None
        }
    }

    /// Get the next token and increment the token index
    pub fn next_token(&mut self) -> Option<Token> {
        if self.index < self.tokens.len() {
            self.index = self.index + 1;
            Some(self.tokens[self.index - 1].clone())
        } else {
            None
        }
    }

    /// Get the previous token and decrement the token index
    pub fn prev_token(&mut self) -> Option<Token> {
        if self.index > 0 {
            Some(self.tokens[self.index - 1].clone())
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
            //println!("parse_keywords aborting .. expecting {}", keyword);
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
            _ => parser_err!(format!(
                "expected token {:?} but was {:?}",
                expected,
                self.prev_token()
            )),
        }
    }

    /// Parse a SQL CREATE statement
    pub fn parse_create(&mut self) -> Result<ASTNode, ParserError> {
        if self.parse_keywords(vec!["TABLE"]) {
            match self.next_token() {
                Some(Token::Identifier(id)) => {
                    // parse optional column list (schema)
                    let mut columns = vec![];
                    if self.consume_token(&Token::LParen)? {
                        loop {
                            if let Some(Token::Identifier(column_name)) = self.next_token() {
                                if let Ok(data_type) = self.parse_data_type() {
                                    let allow_null = if self.parse_keywords(vec!["NOT", "NULL"]) {
                                        false
                                    } else if self.parse_keyword("NULL") {
                                        true
                                    } else {
                                        true
                                    };

                                    match self.peek_token() {
                                        Some(Token::Comma) => {
                                            self.next_token();
                                            columns.push(SQLColumnDef {
                                                name: column_name,
                                                data_type: data_type,
                                                allow_null,
                                            });
                                        }
                                        Some(Token::RParen) => {
                                            self.next_token();
                                            columns.push(SQLColumnDef {
                                                name: column_name,
                                                data_type: data_type,
                                                allow_null,
                                            });
                                            break;
                                        }
                                        _ => {
                                            return parser_err!(
                                                "Expected ',' or ')' after column definition"
                                            );
                                        }
                                    }
                                } else {
                                    return parser_err!(
                                        "Error parsing data type in column definition"
                                    );
                                }
                            } else {
                                return parser_err!("Error parsing column name");
                            }
                        }
                    }

                    Ok(ASTNode::SQLCreateTable { name: id, columns })
                }
                _ => parser_err!(format!(
                    "Unexpected token after CREATE EXTERNAL TABLE: {:?}",
                    self.peek_token()
                )),
            }
        } else {
            parser_err!(format!(
                "Unexpected token after CREATE: {:?}",
                self.peek_token()
            ))
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

    /// Parse a literal string
    pub fn parse_literal_string(&mut self) -> Result<String, ParserError> {
        match self.next_token() {
            Some(Token::String(ref s)) => Ok(s.clone()),
            other => parser_err!(format!("Expected literal string, found {:?}", other)),
        }
    }

    /// Parse a SQL datatype (in the context of a CREATE TABLE statement for example)
    pub fn parse_data_type(&mut self) -> Result<SQLType, ParserError> {
        match self.next_token() {
            Some(Token::Keyword(k)) => match k.to_uppercase().as_ref() {
                "BOOLEAN" => Ok(SQLType::Boolean),
                "FLOAT" => Ok(SQLType::Float(self.parse_optional_precision()?)),
                "REAL" => Ok(SQLType::Real),
                "DOUBLE" => Ok(SQLType::Double),
                "SMALLINT" => Ok(SQLType::SmallInt),
                "INT" | "INTEGER" => Ok(SQLType::Int),
                "BIGINT" => Ok(SQLType::BigInt),
                "VARCHAR" => Ok(SQLType::Varchar(self.parse_precision()?)),
                _ => parser_err!(format!("Invalid data type '{:?}'", k)),
            },
            other => parser_err!(format!("Invalid data type: '{:?}'", other)),
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

    pub fn parse_delete(&mut self) -> Result<ASTNode, ParserError> {

        let relation:  Option<Box<ASTNode>> = if self.parse_keyword("FROM") {
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
            Some(Box::new(self.parse_expr(0)?))
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

            expr_list.push(SQLOrderByExpr::new(Box::new(expr),asc));

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
                .map(|n| Some(Box::new(ASTNode::SQLLiteralLong(n))))
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn parse_delete_statement() {
        
        let sql: &str = "DELETE FROM 'table'";

        match parse_sql(&sql) {
            
            ASTNode::SQLDelete {
                relation, 
                ..
            } => {
                assert_eq!(Some(Box::new(ASTNode::SQLLiteralString("table".to_string()))), relation);
            },

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
                assert_eq!(Some(Box::new(ASTNode::SQLLiteralString("table".to_string()))), relation);
                
                assert_eq!(
                    SQLBinaryExpr {
                        left: Box::new(SQLIdentifier("name".to_string())),
                        op: Eq,
                        right: Box::new(SQLLiteralLong(5)),
                    },
                    *selection.unwrap(),
                );

            },

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
                assert_eq!(Some(Box::new(ASTNode::SQLLiteralLong(5))), limit);
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
        let _ast = parse_sql(&sql);
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
                assert_eq!(SQLType::Varchar(100), c_name.data_type);
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
                assert_eq!(
                    projection[0],
                    ASTNode::SQLLiteralString("one".to_string())
                );
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

    fn parse_sql(sql: &str) -> ASTNode {
        let mut tokenizer = Tokenizer::new(&sql);
        let tokens = tokenizer.tokenize().unwrap();
        let mut parser = Parser::new(tokens);
        let ast = parser.parse().unwrap();
        ast
    }

}

use std::cmp::PartialEq;
use std::fmt::Debug;
//use std::rc::Rc;
//use std::sync::{Arc, Mutex};

use super::tokenizer::ANSISQLTokenizer;
use super::super::tokenizer::*;
use super::super::parser::*;

pub struct ANSISQLParser {
    tokenizer: Box<SQLTokenizer>
}

impl ANSISQLParser where {

    pub fn parse(sql: &str) -> Result<Option<Box<SQLExpr>>, ParserError> {
        let mut parser = ANSISQLParser { tokenizer: Box::new(ANSISQLTokenizer::new(sql)) };
        parser.parse_expr()
    }
}

impl SQLParser for ANSISQLParser {

    fn parse_expr(&mut self) -> Result<Option<Box<SQLExpr>>, ParserError> {

        let precedence: usize = 0;

        let mut e = self.parse_prefix()?;

        match e {
            Some(mut expr) => {
                while let Some(token) = self.tokenizer.peek_token()? {
                    let next_precedence = self.tokenizer.precedence(&token);

                    if precedence >= next_precedence {
                        break;
                    }

                    expr = self.parse_infix(&expr, next_precedence)?.unwrap(); //TODO: fix me
                }

                Ok(Some(expr))
            }
            _ => {
                Ok(None)
            }
        }

    }

    fn parse_prefix(&mut self) -> Result<Option<Box<SQLExpr>>, ParserError> {

        match self.tokenizer.next_token()? {
            Some(SQLToken::Keyword(ref k)) => match k.to_uppercase().as_ref() {
                "INSERT" => unimplemented!(),
                "UPDATE" => unimplemented!(),
                "DELETE" => unimplemented!(),
                "SELECT" => unimplemented!(),
                "CREATE" => unimplemented!(),
                _ => unimplemented!()
            },
            _ => unimplemented!()
        }
    }

    fn parse_infix(&mut self, _left: &SQLExpr, _precedence: usize) -> Result<Option<Box<SQLExpr>>, ParserError> {
        unimplemented!()
    }
}


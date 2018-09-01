use std::cmp::PartialEq;
use std::fmt::Debug;
//use std::iter::Peekable;
//use std::str::Chars;

use std::sync::{Arc, Mutex};

use super::super::tokenizer::*;
use super::super::parser::*;

pub struct ANSISQLParser<TokenType> {
    tokenizer: Arc<Mutex<SQLTokenizer<TokenType>>>
}

impl<TokenType> ANSISQLParser<TokenType> where TokenType: Debug + PartialEq {

    pub fn new(tokenizer: Arc<Mutex<SQLTokenizer<TokenType>>>) -> Self {
        ANSISQLParser { tokenizer: tokenizer.clone() }
    }
}

impl<TokenType, ExprType> SQLParser<TokenType, ExprType> for ANSISQLParser<TokenType>
    where TokenType: Debug + PartialEq, ExprType: Debug {

    fn parse_prefix(&mut self) -> Result<Box<SQLExpr<ExprType>>, ParserError<TokenType>> {

        match self.tokenizer.lock().unwrap().peek_token()? {
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

    fn parse_infix(&mut self, _left: &SQLExpr<ExprType>, _precedence: usize) -> Result<Option<Box<SQLExpr<ExprType>>>, ParserError<TokenType>> {
        unimplemented!()
    }
}


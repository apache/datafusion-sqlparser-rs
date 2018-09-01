use std::cmp::PartialEq;
use std::fmt::Debug;
use std::iter::Peekable;
use std::str::Chars;

use super::super::tokenizer::*;
use super::super::parser::*;

pub struct ANSISQLParser<'a, TokenType> {
    chars: Peekable<Chars<'a>>,
    tokenizer: SQLTokenizer<TokenType>
}

impl<'a, TokenType, ExprType> SQLParser<TokenType, ExprType> for ANSISQLParser<'a, TokenType>
    where TokenType: Debug + PartialEq, ExprType: Debug + PartialEq {

    fn parse_prefix(&mut self) -> Result<Box<SQLExpr<ExprType>>, ParserError<TokenType>> {

        match self.tokenizer.peek_token(&mut self.chars)? {
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

    fn parse_infix(&mut self, left: SQLExpr<ExprType>) -> Result<Option<Box<SQLExpr<ExprType>>>, ParserError<TokenType>> {
        unimplemented!()
    }
}


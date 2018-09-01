use std::cmp::PartialEq;
use std::fmt::Debug;
use std::iter::Peekable;
use std::str::Chars;

use super::super::tokenizer::*;
use super::super::parser::*;

pub struct ANSISQLParser {

}

impl<S,TE> SQLParser<S,TE> for ANSISQLParser
    where S: Debug + PartialEq {

    fn parse_prefix(&mut self) -> Result<Box<SQLExpr<S>>, ParserError<S, TE>> {
        unimplemented!()
    }

    fn parse_infix(&mut self, left: SQLExpr<S>) -> Result<Option<Box<SQLExpr<S>>>, ParserError<S, TE>> {
        unimplemented!()
    }
}


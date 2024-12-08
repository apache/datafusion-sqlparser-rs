use crate::parser::*;

impl Parser<'_> {
    pub fn parse_unlisten(&mut self) -> Result<Statement, ParserError> {
        let channel = if self.consume_token(&Token::Mul) {
            Ident::new(Expr::Wildcard(AttachedToken::empty()).to_string())
        } else {
            match self.parse_identifier(false) {
                Ok(expr) => expr,
                _ => {
                    self.prev_token();
                    return self.expected("wildcard or identifier", self.peek_token());
                }
            }
        };
        Ok(Statement::UNLISTEN { channel })
    }
}

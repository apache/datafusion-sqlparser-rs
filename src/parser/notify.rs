use crate::parser::*;

impl<'a> Parser<'a> {
    pub fn parse_notify(&mut self) -> Result<Statement, ParserError> {
        let channel = self.parse_identifier(false)?;
        let payload = if self.consume_token(&Token::Comma) {
            Some(self.parse_literal_string()?)
        } else {
            None
        };
        Ok(Statement::NOTIFY { channel, payload })
    }
}

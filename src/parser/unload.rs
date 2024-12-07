use crate::parser::*;

impl<'a> Parser<'a> {
    pub fn parse_unload(&mut self) -> Result<Statement, ParserError> {
        self.expect_token(&Token::LParen)?;
        let query = self.parse_query()?;
        self.expect_token(&Token::RParen)?;

        self.expect_keyword(Keyword::TO)?;
        let to = self.parse_identifier(false)?;

        let with_options = self.parse_options(Keyword::WITH)?;

        Ok(Statement::Unload {
            query,
            to,
            with: with_options,
        })
    }
}

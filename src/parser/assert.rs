use crate::parser::*;

impl<'a> Parser<'a> {
    pub fn parse_assert(&mut self) -> Result<Statement, ParserError> {
        let condition = self.parse_expr()?;
        let message = if self.parse_keyword(Keyword::AS) {
            Some(self.parse_expr()?)
        } else {
            None
        };

        Ok(Statement::Assert { condition, message })
    }
}

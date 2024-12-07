use crate::parser::*;

impl<'a> Parser<'a> {
    pub fn parse_release(&mut self) -> Result<Statement, ParserError> {
        let _ = self.parse_keyword(Keyword::SAVEPOINT);
        let name = self.parse_identifier(false)?;

        Ok(Statement::ReleaseSavepoint { name })
    }
}

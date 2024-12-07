use crate::parser::*;

impl<'a> Parser<'a> {
    pub fn parse_savepoint(&mut self) -> Result<Statement, ParserError> {
        let name = self.parse_identifier(false)?;
        Ok(Statement::Savepoint { name })
    }
}

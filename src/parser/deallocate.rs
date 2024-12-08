use crate::parser::*;

impl Parser<'_> {
    pub fn parse_deallocate(&mut self) -> Result<Statement, ParserError> {
        let prepare = self.parse_keyword(Keyword::PREPARE);
        let name = self.parse_identifier(false)?;
        Ok(Statement::Deallocate { name, prepare })
    }
}

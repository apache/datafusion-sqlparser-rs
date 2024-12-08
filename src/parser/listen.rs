use crate::parser::*;

impl Parser<'_> {
    pub fn parse_listen(&mut self) -> Result<Statement, ParserError> {
        let channel = self.parse_identifier(false)?;
        Ok(Statement::LISTEN { channel })
    }
}

use crate::parser::*;

impl<'a> Parser<'a> {
    /// `INSTALL [extension_name]`
    pub fn parse_install(&mut self) -> Result<Statement, ParserError> {
        let extension_name = self.parse_identifier(false)?;

        Ok(Statement::Install { extension_name })
    }
}

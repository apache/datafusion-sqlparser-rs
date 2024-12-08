use crate::parser::*;

impl Parser<'_> {
    pub fn parse_close(&mut self) -> Result<Statement, ParserError> {
        let cursor = if self.parse_keyword(Keyword::ALL) {
            CloseCursor::All
        } else {
            let name = self.parse_identifier(false)?;

            CloseCursor::Specific { name }
        };

        Ok(Statement::Close { cursor })
    }
}

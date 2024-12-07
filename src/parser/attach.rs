use crate::parser::*;

impl<'a> Parser<'a> {
    pub fn parse_attach_database(&mut self) -> Result<Statement, ParserError> {
        let database = self.parse_keyword(Keyword::DATABASE);
        let database_file_name = self.parse_expr()?;
        self.expect_keyword(Keyword::AS)?;
        let schema_name = self.parse_identifier(false)?;
        Ok(Statement::AttachDatabase {
            database,
            schema_name,
            database_file_name,
        })
    }
}

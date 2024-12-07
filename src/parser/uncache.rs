use crate::parser::*;

impl<'a> Parser<'a> {
    /// Parse a UNCACHE TABLE statement
    pub fn parse_uncache_table(&mut self) -> Result<Statement, ParserError> {
        self.expect_keyword(Keyword::TABLE)?;
        let if_exists = self.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
        let table_name = self.parse_object_name(false)?;
        Ok(Statement::UNCache {
            table_name,
            if_exists,
        })
    }
}

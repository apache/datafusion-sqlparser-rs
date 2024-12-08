use crate::parser::*;

impl Parser<'_> {
    /// SQLite-specific `CREATE VIRTUAL TABLE`
    pub fn parse_create_virtual_table(&mut self) -> Result<Statement, ParserError> {
        self.expect_keyword(Keyword::TABLE)?;
        let if_not_exists = self.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let table_name = self.parse_object_name(false)?;
        self.expect_keyword(Keyword::USING)?;
        let module_name = self.parse_identifier(false)?;
        // SQLite docs note that module "arguments syntax is sufficiently
        // general that the arguments can be made to appear as column
        // definitions in a traditional CREATE TABLE statement", but
        // we don't implement that.
        let module_args = self.parse_parenthesized_column_list(Optional, false)?;
        Ok(Statement::CreateVirtualTable {
            name: table_name,
            if_not_exists,
            module_name,
            module_args,
        })
    }
}

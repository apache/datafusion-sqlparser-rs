use crate::parser::*;

use crate::parser_err;

impl<'a> Parser<'a> {
    /// Parse an REPLACE statement
    pub fn parse_replace(&mut self) -> Result<Statement, ParserError> {
        if !dialect_of!(self is MySqlDialect | GenericDialect) {
            return parser_err!(
                "Unsupported statement REPLACE",
                self.peek_token().span.start
            );
        }

        let mut insert = self.parse_insert()?;
        if let Statement::Insert(Insert { replace_into, .. }) = &mut insert {
            *replace_into = true;
        }

        Ok(insert)
    }
}

use crate::parser::*;

impl<'a> Parser<'a> {
    pub fn parse_end(&mut self) -> Result<Statement, ParserError> {
        Ok(Statement::Commit {
            chain: self.parse_commit_rollback_chain()?,
        })
    }
}

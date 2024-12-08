use crate::parser::*;

impl Parser<'_> {
    pub fn parse_end(&mut self) -> Result<Statement, ParserError> {
        Ok(Statement::Commit {
            chain: self.parse_commit_rollback_chain()?,
        })
    }
}

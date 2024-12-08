use crate::parser::*;

impl Parser<'_> {
    pub fn parse_rollback(&mut self) -> Result<Statement, ParserError> {
        let chain = self.parse_commit_rollback_chain()?;
        let savepoint = self.parse_rollback_savepoint()?;

        Ok(Statement::Rollback { chain, savepoint })
    }

    pub fn parse_commit_rollback_chain(&mut self) -> Result<bool, ParserError> {
        let _ = self.parse_one_of_keywords(&[Keyword::TRANSACTION, Keyword::WORK]);
        if self.parse_keyword(Keyword::AND) {
            let chain = !self.parse_keyword(Keyword::NO);
            self.expect_keyword(Keyword::CHAIN)?;
            Ok(chain)
        } else {
            Ok(false)
        }
    }

    pub fn parse_rollback_savepoint(&mut self) -> Result<Option<Ident>, ParserError> {
        if self.parse_keyword(Keyword::TO) {
            let _ = self.parse_keyword(Keyword::SAVEPOINT);
            let savepoint = self.parse_identifier(false)?;

            Ok(Some(savepoint))
        } else {
            Ok(None)
        }
    }
}

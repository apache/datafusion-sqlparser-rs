use crate::parser::*;

impl<'a> Parser<'a> {
    pub fn parse_start_transaction(&mut self) -> Result<Statement, ParserError> {
        self.expect_keyword(Keyword::TRANSACTION)?;
        Ok(Statement::StartTransaction {
            modes: self.parse_transaction_modes()?,
            begin: false,
            transaction: Some(BeginTransactionKind::Transaction),
            modifier: None,
        })
    }

    pub fn parse_begin(&mut self) -> Result<Statement, ParserError> {
        let modifier = if !self.dialect.supports_start_transaction_modifier() {
            None
        } else if self.parse_keyword(Keyword::DEFERRED) {
            Some(TransactionModifier::Deferred)
        } else if self.parse_keyword(Keyword::IMMEDIATE) {
            Some(TransactionModifier::Immediate)
        } else if self.parse_keyword(Keyword::EXCLUSIVE) {
            Some(TransactionModifier::Exclusive)
        } else {
            None
        };
        let transaction = match self.parse_one_of_keywords(&[Keyword::TRANSACTION, Keyword::WORK]) {
            Some(Keyword::TRANSACTION) => Some(BeginTransactionKind::Transaction),
            Some(Keyword::WORK) => Some(BeginTransactionKind::Work),
            _ => None,
        };
        Ok(Statement::StartTransaction {
            modes: self.parse_transaction_modes()?,
            begin: true,
            transaction,
            modifier,
        })
    }

    pub fn parse_transaction_modes(&mut self) -> Result<Vec<TransactionMode>, ParserError> {
        let mut modes = vec![];
        let mut required = false;
        loop {
            let mode = if self.parse_keywords(&[Keyword::ISOLATION, Keyword::LEVEL]) {
                let iso_level = if self.parse_keywords(&[Keyword::READ, Keyword::UNCOMMITTED]) {
                    TransactionIsolationLevel::ReadUncommitted
                } else if self.parse_keywords(&[Keyword::READ, Keyword::COMMITTED]) {
                    TransactionIsolationLevel::ReadCommitted
                } else if self.parse_keywords(&[Keyword::REPEATABLE, Keyword::READ]) {
                    TransactionIsolationLevel::RepeatableRead
                } else if self.parse_keyword(Keyword::SERIALIZABLE) {
                    TransactionIsolationLevel::Serializable
                } else {
                    self.expected("isolation level", self.peek_token())?
                };
                TransactionMode::IsolationLevel(iso_level)
            } else if self.parse_keywords(&[Keyword::READ, Keyword::ONLY]) {
                TransactionMode::AccessMode(TransactionAccessMode::ReadOnly)
            } else if self.parse_keywords(&[Keyword::READ, Keyword::WRITE]) {
                TransactionMode::AccessMode(TransactionAccessMode::ReadWrite)
            } else if required {
                self.expected("transaction mode", self.peek_token())?
            } else {
                break;
            };
            modes.push(mode);
            // ANSI requires a comma after each transaction mode, but
            // PostgreSQL, for historical reasons, does not. We follow
            // PostgreSQL in making the comma optional, since that is strictly
            // more general.
            required = self.consume_token(&Token::Comma);
        }
        Ok(modes)
    }
}

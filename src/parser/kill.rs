use crate::parser::*;

impl Parser<'_> {
    // KILL [CONNECTION | QUERY | MUTATION] processlist_id
    pub fn parse_kill(&mut self) -> Result<Statement, ParserError> {
        let modifier_keyword =
            self.parse_one_of_keywords(&[Keyword::CONNECTION, Keyword::QUERY, Keyword::MUTATION]);

        let id = self.parse_literal_uint()?;

        let modifier = match modifier_keyword {
            Some(Keyword::CONNECTION) => Some(KillType::Connection),
            Some(Keyword::QUERY) => Some(KillType::Query),
            Some(Keyword::MUTATION) => {
                if dialect_of!(self is ClickHouseDialect | GenericDialect) {
                    Some(KillType::Mutation)
                } else {
                    self.expected(
                        "Unsupported type for KILL, allowed: CONNECTION | QUERY",
                        self.peek_token(),
                    )?
                }
            }
            _ => None,
        };

        Ok(Statement::Kill { modifier, id })
    }
}

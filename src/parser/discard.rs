use crate::parser::*;

impl Parser<'_> {
    pub fn parse_discard(&mut self) -> Result<Statement, ParserError> {
        let object_type = if self.parse_keyword(Keyword::ALL) {
            DiscardObject::ALL
        } else if self.parse_keyword(Keyword::PLANS) {
            DiscardObject::PLANS
        } else if self.parse_keyword(Keyword::SEQUENCES) {
            DiscardObject::SEQUENCES
        } else if self.parse_keyword(Keyword::TEMP) || self.parse_keyword(Keyword::TEMPORARY) {
            DiscardObject::TEMP
        } else {
            return self.expected(
                "ALL, PLANS, SEQUENCES, TEMP or TEMPORARY after DISCARD",
                self.peek_token(),
            );
        };
        Ok(Statement::Discard { object_type })
    }
}

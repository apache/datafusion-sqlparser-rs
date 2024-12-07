use crate::parser::*;

impl<'a> Parser<'a> {
    // FETCH [ direction { FROM | IN } ] cursor INTO target;
    pub fn parse_fetch_statement(&mut self) -> Result<Statement, ParserError> {
        let direction = if self.parse_keyword(Keyword::NEXT) {
            FetchDirection::Next
        } else if self.parse_keyword(Keyword::PRIOR) {
            FetchDirection::Prior
        } else if self.parse_keyword(Keyword::FIRST) {
            FetchDirection::First
        } else if self.parse_keyword(Keyword::LAST) {
            FetchDirection::Last
        } else if self.parse_keyword(Keyword::ABSOLUTE) {
            FetchDirection::Absolute {
                limit: self.parse_number_value()?,
            }
        } else if self.parse_keyword(Keyword::RELATIVE) {
            FetchDirection::Relative {
                limit: self.parse_number_value()?,
            }
        } else if self.parse_keyword(Keyword::FORWARD) {
            if self.parse_keyword(Keyword::ALL) {
                FetchDirection::ForwardAll
            } else {
                FetchDirection::Forward {
                    // TODO: Support optional
                    limit: Some(self.parse_number_value()?),
                }
            }
        } else if self.parse_keyword(Keyword::BACKWARD) {
            if self.parse_keyword(Keyword::ALL) {
                FetchDirection::BackwardAll
            } else {
                FetchDirection::Backward {
                    // TODO: Support optional
                    limit: Some(self.parse_number_value()?),
                }
            }
        } else if self.parse_keyword(Keyword::ALL) {
            FetchDirection::All
        } else {
            FetchDirection::Count {
                limit: self.parse_number_value()?,
            }
        };

        self.expect_one_of_keywords(&[Keyword::FROM, Keyword::IN])?;

        let name = self.parse_identifier(false)?;

        let into = if self.parse_keyword(Keyword::INTO) {
            Some(self.parse_object_name(false)?)
        } else {
            None
        };

        Ok(Statement::Fetch {
            name,
            direction,
            into,
        })
    }
}

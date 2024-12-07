use crate::parser::*;

impl<'a> Parser<'a> {
    // PRAGMA [schema-name '.'] pragma-name [('=' pragma-value) | '(' pragma-value ')']
    pub fn parse_pragma(&mut self) -> Result<Statement, ParserError> {
        let name = self.parse_object_name(false)?;
        if self.consume_token(&Token::LParen) {
            let value = self.parse_pragma_value()?;
            self.expect_token(&Token::RParen)?;
            Ok(Statement::Pragma {
                name,
                value: Some(value),
                is_eq: false,
            })
        } else if self.consume_token(&Token::Eq) {
            Ok(Statement::Pragma {
                name,
                value: Some(self.parse_pragma_value()?),
                is_eq: true,
            })
        } else {
            Ok(Statement::Pragma {
                name,
                value: None,
                is_eq: false,
            })
        }
    }

    fn parse_pragma_value(&mut self) -> Result<Value, ParserError> {
        match self.parse_value()? {
            v @ Value::SingleQuotedString(_) => Ok(v),
            v @ Value::DoubleQuotedString(_) => Ok(v),
            v @ Value::Number(_, _) => Ok(v),
            v @ Value::Placeholder(_) => Ok(v),
            _ => {
                self.prev_token();
                self.expected("number or string or ? placeholder", self.peek_token())
            }
        }
    }
}

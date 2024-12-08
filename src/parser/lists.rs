use crate::parser::*;

impl<'a> Parser<'a> {
    /// Parse the comma of a comma-separated syntax element.
    /// Allows for control over trailing commas
    /// Returns true if there is a next element
    fn is_parse_comma_separated_end_with_trailing_commas(&mut self, trailing_commas: bool) -> bool {
        if !self.consume_token(&Token::Comma) {
            true
        } else if trailing_commas {
            let token = self.peek_token().token;
            match token {
                Token::Word(ref kw)
                    if keywords::RESERVED_FOR_COLUMN_ALIAS.contains(&kw.keyword) =>
                {
                    true
                }
                Token::RParen | Token::SemiColon | Token::EOF | Token::RBracket | Token::RBrace => {
                    true
                }
                _ => false,
            }
        } else {
            false
        }
    }

    /// Parse the comma of a comma-separated syntax element.
    /// Returns true if there is a next element
    pub(crate) fn is_parse_comma_separated_end(&mut self) -> bool {
        self.is_parse_comma_separated_end_with_trailing_commas(self.options.trailing_commas)
    }

    /// Parse a comma-separated list of 1+ items accepted by `F`
    pub fn parse_comma_separated<T, F>(&mut self, f: F) -> Result<Vec<T>, ParserError>
    where
        F: FnMut(&mut Parser<'a>) -> Result<T, ParserError>,
    {
        self.parse_comma_separated_with_trailing_commas(f, self.options.trailing_commas)
    }

    /// Parse a comma-separated list of 1+ items accepted by `F`
    /// Allows for control over trailing commas
    pub(crate) fn parse_comma_separated_with_trailing_commas<T, F>(
        &mut self,
        mut f: F,
        trailing_commas: bool,
    ) -> Result<Vec<T>, ParserError>
    where
        F: FnMut(&mut Parser<'a>) -> Result<T, ParserError>,
    {
        let mut values = vec![];
        loop {
            values.push(f(self)?);
            if self.is_parse_comma_separated_end_with_trailing_commas(trailing_commas) {
                break;
            }
        }
        Ok(values)
    }

    pub fn parse_parenthesized<T, F>(&mut self, mut f: F) -> Result<T, ParserError>
    where
        F: FnMut(&mut Parser<'a>) -> Result<T, ParserError>,
    {
        self.expect_token(&Token::LParen)?;
        let res = f(self)?;
        self.expect_token(&Token::RParen)?;
        Ok(res)
    }

    /// Parse a comma-separated list of 0+ items accepted by `F`
    /// * `end_token` - expected end token for the closure (e.g. [Token::RParen], [Token::RBrace] ...)
    pub fn parse_comma_separated0<T, F>(
        &mut self,
        f: F,
        end_token: Token,
    ) -> Result<Vec<T>, ParserError>
    where
        F: FnMut(&mut Parser<'a>) -> Result<T, ParserError>,
    {
        if self.peek_token().token == end_token {
            return Ok(vec![]);
        }

        if self.options.trailing_commas && self.peek_tokens() == [Token::Comma, end_token] {
            let _ = self.consume_token(&Token::Comma);
            return Ok(vec![]);
        }

        self.parse_comma_separated(f)
    }

    pub(crate) fn parse_parenthesized_identifiers(&mut self) -> Result<Vec<Ident>, ParserError> {
        self.expect_token(&Token::LParen)?;
        let partitions = self.parse_comma_separated(|p| p.parse_identifier(false))?;
        self.expect_token(&Token::RParen)?;
        Ok(partitions)
    }
}

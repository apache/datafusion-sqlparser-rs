use crate::parser::*;

impl<'a> Parser<'a> {
    pub fn parse_execute(&mut self) -> Result<Statement, ParserError> {
        let name = self.parse_object_name(false)?;

        let has_parentheses = self.consume_token(&Token::LParen);

        let end_token = match (has_parentheses, self.peek_token().token) {
            (true, _) => Token::RParen,
            (false, Token::EOF) => Token::EOF,
            (false, Token::Word(w)) if w.keyword == Keyword::USING => Token::Word(w),
            (false, _) => Token::SemiColon,
        };

        let parameters = self.parse_comma_separated0(Parser::parse_expr, end_token)?;

        if has_parentheses {
            self.expect_token(&Token::RParen)?;
        }

        let mut using = vec![];
        if self.parse_keyword(Keyword::USING) {
            using.push(self.parse_expr()?);

            while self.consume_token(&Token::Comma) {
                using.push(self.parse_expr()?);
            }
        };

        Ok(Statement::Execute {
            name,
            parameters,
            has_parentheses,
            using,
        })
    }
}

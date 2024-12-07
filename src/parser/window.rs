use crate::parser::*;

impl<'a> Parser<'a> {
    pub fn parse_named_window(&mut self) -> Result<NamedWindowDefinition, ParserError> {
        let ident = self.parse_identifier(false)?;
        self.expect_keyword(Keyword::AS)?;

        let window_expr = if self.consume_token(&Token::LParen) {
            NamedWindowExpr::WindowSpec(self.parse_window_spec()?)
        } else if self.dialect.supports_window_clause_named_window_reference() {
            NamedWindowExpr::NamedWindow(self.parse_identifier(false)?)
        } else {
            return self.expected("(", self.peek_token());
        };

        Ok(NamedWindowDefinition(ident, window_expr))
    }

    pub fn parse_window_spec(&mut self) -> Result<WindowSpec, ParserError> {
        let window_name = match self.peek_token().token {
            Token::Word(word) if word.keyword == Keyword::NoKeyword => {
                self.parse_optional_indent()?
            }
            _ => None,
        };

        let partition_by = if self.parse_keywords(&[Keyword::PARTITION, Keyword::BY]) {
            self.parse_comma_separated(Parser::parse_expr)?
        } else {
            vec![]
        };
        let order_by = if self.parse_keywords(&[Keyword::ORDER, Keyword::BY]) {
            self.parse_comma_separated(Parser::parse_order_by_expr)?
        } else {
            vec![]
        };

        let window_frame = if !self.consume_token(&Token::RParen) {
            let window_frame = self.parse_window_frame()?;
            self.expect_token(&Token::RParen)?;
            Some(window_frame)
        } else {
            None
        };
        Ok(WindowSpec {
            window_name,
            partition_by,
            order_by,
            window_frame,
        })
    }

    pub fn parse_window_frame(&mut self) -> Result<WindowFrame, ParserError> {
        let units = self.parse_window_frame_units()?;
        let (start_bound, end_bound) = if self.parse_keyword(Keyword::BETWEEN) {
            let start_bound = self.parse_window_frame_bound()?;
            self.expect_keyword(Keyword::AND)?;
            let end_bound = Some(self.parse_window_frame_bound()?);
            (start_bound, end_bound)
        } else {
            (self.parse_window_frame_bound()?, None)
        };
        Ok(WindowFrame {
            units,
            start_bound,
            end_bound,
        })
    }

    pub fn parse_window_frame_units(&mut self) -> Result<WindowFrameUnits, ParserError> {
        let next_token = self.next_token();
        match &next_token.token {
            Token::Word(w) => match w.keyword {
                Keyword::ROWS => Ok(WindowFrameUnits::Rows),
                Keyword::RANGE => Ok(WindowFrameUnits::Range),
                Keyword::GROUPS => Ok(WindowFrameUnits::Groups),
                _ => self.expected("ROWS, RANGE, GROUPS", next_token)?,
            },
            _ => self.expected("ROWS, RANGE, GROUPS", next_token),
        }
    }

    /// Parse `CURRENT ROW` or `{ <positive number> | UNBOUNDED } { PRECEDING | FOLLOWING }`
    pub fn parse_window_frame_bound(&mut self) -> Result<WindowFrameBound, ParserError> {
        if self.parse_keywords(&[Keyword::CURRENT, Keyword::ROW]) {
            Ok(WindowFrameBound::CurrentRow)
        } else {
            let rows = if self.parse_keyword(Keyword::UNBOUNDED) {
                None
            } else {
                Some(Box::new(match self.peek_token().token {
                    Token::SingleQuotedString(_) => self.parse_interval()?,
                    _ => self.parse_expr()?,
                }))
            };
            if self.parse_keyword(Keyword::PRECEDING) {
                Ok(WindowFrameBound::Preceding(rows))
            } else if self.parse_keyword(Keyword::FOLLOWING) {
                Ok(WindowFrameBound::Following(rows))
            } else {
                self.expected("PRECEDING or FOLLOWING", self.peek_token())
            }
        }
    }
}

use crate::parser::*;

impl<'a> Parser<'a> {
    /// Parse a CACHE TABLE statement
    pub fn parse_cache_table(&mut self) -> Result<Statement, ParserError> {
        let (mut table_flag, mut options, mut has_as, mut query) = (None, vec![], false, None);
        if self.parse_keyword(Keyword::TABLE) {
            let table_name = self.parse_object_name(false)?;
            if self.peek_token().token != Token::EOF {
                if let Token::Word(word) = self.peek_token().token {
                    if word.keyword == Keyword::OPTIONS {
                        options = self.parse_options(Keyword::OPTIONS)?
                    }
                };

                if self.peek_token().token != Token::EOF {
                    let (a, q) = self.parse_as_query()?;
                    has_as = a;
                    query = Some(q);
                }

                Ok(Statement::Cache {
                    table_flag,
                    table_name,
                    has_as,
                    options,
                    query,
                })
            } else {
                Ok(Statement::Cache {
                    table_flag,
                    table_name,
                    has_as,
                    options,
                    query,
                })
            }
        } else {
            table_flag = Some(self.parse_object_name(false)?);
            if self.parse_keyword(Keyword::TABLE) {
                let table_name = self.parse_object_name(false)?;
                if self.peek_token() != Token::EOF {
                    if let Token::Word(word) = self.peek_token().token {
                        if word.keyword == Keyword::OPTIONS {
                            options = self.parse_options(Keyword::OPTIONS)?
                        }
                    };

                    if self.peek_token() != Token::EOF {
                        let (a, q) = self.parse_as_query()?;
                        has_as = a;
                        query = Some(q);
                    }

                    Ok(Statement::Cache {
                        table_flag,
                        table_name,
                        has_as,
                        options,
                        query,
                    })
                } else {
                    Ok(Statement::Cache {
                        table_flag,
                        table_name,
                        has_as,
                        options,
                        query,
                    })
                }
            } else {
                if self.peek_token() == Token::EOF {
                    self.prev_token();
                }
                self.expected("a `TABLE` keyword", self.peek_token())
            }
        }
    }

    /// Parse 'AS' before as query,such as `WITH XXX AS SELECT XXX` oer `CACHE TABLE AS SELECT XXX`
    pub fn parse_as_query(&mut self) -> Result<(bool, Box<Query>), ParserError> {
        match self.peek_token().token {
            Token::Word(word) => match word.keyword {
                Keyword::AS => {
                    self.next_token();
                    Ok((true, self.parse_query()?))
                }
                _ => Ok((false, self.parse_query()?)),
            },
            _ => self.expected("a QUERY statement", self.peek_token()),
        }
    }
}

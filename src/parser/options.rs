use crate::parser::*;

impl<'a> Parser<'a> {
    pub fn maybe_parse_options(
        &mut self,
        keyword: Keyword,
    ) -> Result<Option<Vec<SqlOption>>, ParserError> {
        if let Token::Word(word) = self.peek_token().token {
            if word.keyword == keyword {
                return Ok(Some(self.parse_options(keyword)?));
            }
        };
        Ok(None)
    }

    pub fn parse_options(&mut self, keyword: Keyword) -> Result<Vec<SqlOption>, ParserError> {
        if self.parse_keyword(keyword) {
            self.expect_token(&Token::LParen)?;
            let options = self.parse_comma_separated(Parser::parse_sql_option)?;
            self.expect_token(&Token::RParen)?;
            Ok(options)
        } else {
            Ok(vec![])
        }
    }

    pub fn parse_option_clustered(&mut self) -> Result<SqlOption, ParserError> {
        if self.parse_keywords(&[
            Keyword::CLUSTERED,
            Keyword::COLUMNSTORE,
            Keyword::INDEX,
            Keyword::ORDER,
        ]) {
            Ok(SqlOption::Clustered(
                TableOptionsClustered::ColumnstoreIndexOrder(
                    self.parse_parenthesized_column_list(IsOptional::Mandatory, false)?,
                ),
            ))
        } else if self.parse_keywords(&[Keyword::CLUSTERED, Keyword::COLUMNSTORE, Keyword::INDEX]) {
            Ok(SqlOption::Clustered(
                TableOptionsClustered::ColumnstoreIndex,
            ))
        } else if self.parse_keywords(&[Keyword::CLUSTERED, Keyword::INDEX]) {
            self.expect_token(&Token::LParen)?;

            let columns = self.parse_comma_separated(|p| {
                let name = p.parse_identifier(false)?;
                let asc = p.parse_asc_desc();

                Ok(ClusteredIndex { name, asc })
            })?;

            self.expect_token(&Token::RParen)?;

            Ok(SqlOption::Clustered(TableOptionsClustered::Index(columns)))
        } else {
            Err(ParserError::ParserError(
                "invalid CLUSTERED sequence".to_string(),
            ))
        }
    }

    pub fn parse_option_partition(&mut self) -> Result<SqlOption, ParserError> {
        self.expect_keyword(Keyword::PARTITION)?;
        self.expect_token(&Token::LParen)?;
        let column_name = self.parse_identifier(false)?;

        self.expect_keyword(Keyword::RANGE)?;
        let range_direction = if self.parse_keyword(Keyword::LEFT) {
            Some(PartitionRangeDirection::Left)
        } else if self.parse_keyword(Keyword::RIGHT) {
            Some(PartitionRangeDirection::Right)
        } else {
            None
        };

        self.expect_keywords(&[Keyword::FOR, Keyword::VALUES])?;
        self.expect_token(&Token::LParen)?;

        let for_values = self.parse_comma_separated(Parser::parse_expr)?;

        self.expect_token(&Token::RParen)?;
        self.expect_token(&Token::RParen)?;

        Ok(SqlOption::Partition {
            column_name,
            range_direction,
            for_values,
        })
    }

    pub fn parse_options_with_keywords(
        &mut self,
        keywords: &[Keyword],
    ) -> Result<Vec<SqlOption>, ParserError> {
        if self.parse_keywords(keywords) {
            self.expect_token(&Token::LParen)?;
            let options = self.parse_comma_separated(Parser::parse_sql_option)?;
            self.expect_token(&Token::RParen)?;
            Ok(options)
        } else {
            Ok(vec![])
        }
    }

    pub fn parse_sql_option(&mut self) -> Result<SqlOption, ParserError> {
        let is_mssql = dialect_of!(self is MsSqlDialect|GenericDialect);

        match self.peek_token().token {
            Token::Word(w) if w.keyword == Keyword::HEAP && is_mssql => {
                Ok(SqlOption::Ident(self.parse_identifier(false)?))
            }
            Token::Word(w) if w.keyword == Keyword::PARTITION && is_mssql => {
                self.parse_option_partition()
            }
            Token::Word(w) if w.keyword == Keyword::CLUSTERED && is_mssql => {
                self.parse_option_clustered()
            }
            _ => {
                let name = self.parse_identifier(false)?;
                self.expect_token(&Token::Eq)?;
                let value = self.parse_expr()?;

                Ok(SqlOption::KeyValue { key: name, value })
            }
        }
    }
}

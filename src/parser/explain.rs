use super::*;

impl Parser<'_> {
    pub fn parse_explain(
        &mut self,
        describe_alias: DescribeAlias,
    ) -> Result<Statement, ParserError> {
        let mut analyze = false;
        let mut verbose = false;
        let mut query_plan = false;
        let mut format = None;
        let mut options = None;

        // Note: DuckDB is compatible with PostgreSQL syntax for this statement,
        // although not all features may be implemented.
        if describe_alias == DescribeAlias::Explain
            && self.dialect.supports_explain_with_utility_options()
            && self.peek_token().token == Token::LParen
        {
            options = Some(self.parse_utility_options()?)
        } else if self.parse_keywords(&[Keyword::QUERY, Keyword::PLAN]) {
            query_plan = true;
        } else {
            analyze = self.parse_keyword(Keyword::ANALYZE);
            verbose = self.parse_keyword(Keyword::VERBOSE);
            if self.parse_keyword(Keyword::FORMAT) {
                format = Some(self.parse_analyze_format()?);
            }
        }

        match self.maybe_parse(|parser| parser.parse_statement())? {
            Some(Statement::Explain { .. }) | Some(Statement::ExplainTable { .. }) => Err(
                ParserError::ParserError("Explain must be root of the plan".to_string()),
            ),
            Some(statement) => Ok(Statement::Explain {
                describe_alias,
                analyze,
                verbose,
                query_plan,
                statement: Box::new(statement),
                format,
                options,
            }),
            _ => {
                let hive_format =
                    match self.parse_one_of_keywords(&[Keyword::EXTENDED, Keyword::FORMATTED]) {
                        Some(Keyword::EXTENDED) => Some(HiveDescribeFormat::Extended),
                        Some(Keyword::FORMATTED) => Some(HiveDescribeFormat::Formatted),
                        _ => None,
                    };

                let has_table_keyword = if self.dialect.describe_requires_table_keyword() {
                    // only allow to use TABLE keyword for DESC|DESCRIBE statement
                    self.parse_keyword(Keyword::TABLE)
                } else {
                    false
                };

                let table_name = self.parse_object_name(false)?;
                Ok(Statement::ExplainTable {
                    describe_alias,
                    hive_format,
                    has_table_keyword,
                    table_name,
                })
            }
        }
    }

    pub fn parse_utility_options(&mut self) -> Result<Vec<UtilityOption>, ParserError> {
        self.expect_token(&Token::LParen)?;
        let options = self.parse_comma_separated(Self::parse_utility_option)?;
        self.expect_token(&Token::RParen)?;

        Ok(options)
    }

    fn parse_utility_option(&mut self) -> Result<UtilityOption, ParserError> {
        let name = self.parse_identifier(false)?;

        let next_token = self.peek_token();
        if next_token == Token::Comma || next_token == Token::RParen {
            return Ok(UtilityOption { name, arg: None });
        }
        let arg = self.parse_expr()?;

        Ok(UtilityOption {
            name,
            arg: Some(arg),
        })
    }
}

use crate::parser::*;

impl Parser<'_> {
    /// Parse `CREATE FUNCTION` for [BigQuery]
    ///
    /// [BigQuery]: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#create_function_statement
    pub fn parse_bigquery_create_function(
        &mut self,
        or_replace: bool,
        temporary: bool,
    ) -> Result<Statement, ParserError> {
        let if_not_exists = self.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let name = self.parse_object_name(false)?;

        let parse_function_param =
            |parser: &mut Parser| -> Result<OperateFunctionArg, ParserError> {
                let name = parser.parse_identifier(false)?;
                let data_type = parser.parse_data_type()?;
                Ok(OperateFunctionArg {
                    mode: None,
                    name: Some(name),
                    data_type,
                    default_expr: None,
                })
            };
        self.expect_token(&Token::LParen)?;
        let args = self.parse_comma_separated0(parse_function_param, Token::RParen)?;
        self.expect_token(&Token::RParen)?;

        let return_type = if self.parse_keyword(Keyword::RETURNS) {
            Some(self.parse_data_type()?)
        } else {
            None
        };

        let determinism_specifier = if self.parse_keyword(Keyword::DETERMINISTIC) {
            Some(FunctionDeterminismSpecifier::Deterministic)
        } else if self.parse_keywords(&[Keyword::NOT, Keyword::DETERMINISTIC]) {
            Some(FunctionDeterminismSpecifier::NotDeterministic)
        } else {
            None
        };

        let language = if self.parse_keyword(Keyword::LANGUAGE) {
            Some(self.parse_identifier(false)?)
        } else {
            None
        };

        let remote_connection =
            if self.parse_keywords(&[Keyword::REMOTE, Keyword::WITH, Keyword::CONNECTION]) {
                Some(self.parse_object_name(false)?)
            } else {
                None
            };

        // `OPTIONS` may come before of after the function body but
        // may be specified at most once.
        let mut options = self.maybe_parse_options(Keyword::OPTIONS)?;

        let function_body = if remote_connection.is_none() {
            self.expect_keyword(Keyword::AS)?;
            let expr = self.parse_expr()?;
            if options.is_none() {
                options = self.maybe_parse_options(Keyword::OPTIONS)?;
                Some(CreateFunctionBody::AsBeforeOptions(expr))
            } else {
                Some(CreateFunctionBody::AsAfterOptions(expr))
            }
        } else {
            None
        };

        Ok(Statement::CreateFunction(CreateFunction {
            or_replace,
            temporary,
            if_not_exists,
            name,
            args: Some(args),
            return_type,
            function_body,
            language,
            determinism_specifier,
            options,
            remote_connection,
            using: None,
            behavior: None,
            called_on_null: None,
            parallel: None,
        }))
    }

    /// Parse a [BigQuery] `DECLARE` statement.
    ///
    /// Syntax:
    /// ```text
    /// DECLARE variable_name[, ...] [{ <variable_type> | <DEFAULT expression> }];
    /// ```
    /// [BigQuery]: https://cloud.google.com/bigquery/docs/reference/standard-sql/procedural-language#declare
    pub fn parse_big_query_declare(&mut self) -> Result<Statement, ParserError> {
        let names = self.parse_comma_separated(|parser| Parser::parse_identifier(parser, false))?;

        let data_type = match self.peek_token().token {
            Token::Word(w) if w.keyword == Keyword::DEFAULT => None,
            _ => Some(self.parse_data_type()?),
        };

        let expr = if data_type.is_some() {
            if self.parse_keyword(Keyword::DEFAULT) {
                Some(self.parse_expr()?)
            } else {
                None
            }
        } else {
            // If no variable type - default expression must be specified, per BQ docs.
            // i.e `DECLARE foo;` is invalid.
            self.expect_keyword(Keyword::DEFAULT)?;
            Some(self.parse_expr()?)
        };

        Ok(Statement::Declare {
            stmts: vec![Declare {
                names,
                data_type,
                assignment: expr.map(|expr| DeclareAssignment::Default(Box::new(expr))),
                declare_type: None,
                binary: None,
                sensitive: None,
                scroll: None,
                hold: None,
                for_query: None,
            }],
        })
    }
}

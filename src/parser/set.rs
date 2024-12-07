use crate::parser::*;

impl<'a> Parser<'a> {
    pub fn parse_set(&mut self) -> Result<Statement, ParserError> {
        let modifier =
            self.parse_one_of_keywords(&[Keyword::SESSION, Keyword::LOCAL, Keyword::HIVEVAR]);
        if let Some(Keyword::HIVEVAR) = modifier {
            self.expect_token(&Token::Colon)?;
        } else if let Some(set_role_stmt) =
            self.maybe_parse(|parser| parser.parse_set_role(modifier))?
        {
            return Ok(set_role_stmt);
        }

        let variables = if self.parse_keywords(&[Keyword::TIME, Keyword::ZONE]) {
            OneOrManyWithParens::One(ObjectName(vec!["TIMEZONE".into()]))
        } else if self.dialect.supports_parenthesized_set_variables()
            && self.consume_token(&Token::LParen)
        {
            let variables = OneOrManyWithParens::Many(
                self.parse_comma_separated(|parser: &mut Parser<'a>| {
                    parser.parse_identifier(false)
                })?
                .into_iter()
                .map(|ident| ObjectName(vec![ident]))
                .collect(),
            );
            self.expect_token(&Token::RParen)?;
            variables
        } else {
            OneOrManyWithParens::One(self.parse_object_name(false)?)
        };

        if matches!(&variables, OneOrManyWithParens::One(variable) if variable.to_string().eq_ignore_ascii_case("NAMES")
            && dialect_of!(self is MySqlDialect | GenericDialect))
        {
            if self.parse_keyword(Keyword::DEFAULT) {
                return Ok(Statement::SetNamesDefault {});
            }

            let charset_name = self.parse_literal_string()?;
            let collation_name = if self.parse_one_of_keywords(&[Keyword::COLLATE]).is_some() {
                Some(self.parse_literal_string()?)
            } else {
                None
            };

            return Ok(Statement::SetNames {
                charset_name,
                collation_name,
            });
        }

        let parenthesized_assignment = matches!(&variables, OneOrManyWithParens::Many(_));

        if self.consume_token(&Token::Eq) || self.parse_keyword(Keyword::TO) {
            if parenthesized_assignment {
                self.expect_token(&Token::LParen)?;
            }

            let mut values = vec![];
            loop {
                let value = if let Some(expr) = self.try_parse_expr_sub_query()? {
                    expr
                } else if let Ok(expr) = self.parse_expr() {
                    expr
                } else {
                    self.expected("variable value", self.peek_token())?
                };

                values.push(value);
                if self.consume_token(&Token::Comma) {
                    continue;
                }

                if parenthesized_assignment {
                    self.expect_token(&Token::RParen)?;
                }
                return Ok(Statement::SetVariable {
                    local: modifier == Some(Keyword::LOCAL),
                    hivevar: Some(Keyword::HIVEVAR) == modifier,
                    variables,
                    value: values,
                });
            }
        }

        let OneOrManyWithParens::One(variable) = variables else {
            return self.expected("set variable", self.peek_token());
        };

        if variable.to_string().eq_ignore_ascii_case("TIMEZONE") {
            // for some db (e.g. postgresql), SET TIME ZONE <value> is an alias for SET TIMEZONE [TO|=] <value>
            match self.parse_expr() {
                Ok(expr) => Ok(Statement::SetTimeZone {
                    local: modifier == Some(Keyword::LOCAL),
                    value: expr,
                }),
                _ => self.expected("timezone value", self.peek_token())?,
            }
        } else if variable.to_string() == "CHARACTERISTICS" {
            self.expect_keywords(&[Keyword::AS, Keyword::TRANSACTION])?;
            Ok(Statement::SetTransaction {
                modes: self.parse_transaction_modes()?,
                snapshot: None,
                session: true,
            })
        } else if variable.to_string() == "TRANSACTION" && modifier.is_none() {
            if self.parse_keyword(Keyword::SNAPSHOT) {
                let snapshot_id = self.parse_value()?;
                return Ok(Statement::SetTransaction {
                    modes: vec![],
                    snapshot: Some(snapshot_id),
                    session: false,
                });
            }
            Ok(Statement::SetTransaction {
                modes: self.parse_transaction_modes()?,
                snapshot: None,
                session: false,
            })
        } else {
            self.expected("equals sign or TO", self.peek_token())
        }
    }
}

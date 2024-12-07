use crate::parser::*;

impl<'a> Parser<'a> {
    /// Parse a GRANT statement.
    pub fn parse_grant(&mut self) -> Result<Statement, ParserError> {
        let (privileges, objects) = self.parse_grant_revoke_privileges_objects()?;

        self.expect_keyword(Keyword::TO)?;
        let grantees = self.parse_comma_separated(|p| p.parse_identifier(false))?;

        let with_grant_option =
            self.parse_keywords(&[Keyword::WITH, Keyword::GRANT, Keyword::OPTION]);

        let granted_by = self
            .parse_keywords(&[Keyword::GRANTED, Keyword::BY])
            .then(|| self.parse_identifier(false).unwrap());

        Ok(Statement::Grant {
            privileges,
            objects,
            grantees,
            with_grant_option,
            granted_by,
        })
    }

    pub fn parse_actions_list(&mut self) -> Result<Vec<ParsedAction>, ParserError> {
        let mut values = vec![];
        loop {
            values.push(self.parse_grant_permission()?);
            if !self.consume_token(&Token::Comma) {
                break;
            } else if self.options.trailing_commas {
                match self.peek_token().token {
                    Token::Word(kw) if kw.keyword == Keyword::ON => {
                        break;
                    }
                    Token::RParen
                    | Token::SemiColon
                    | Token::EOF
                    | Token::RBracket
                    | Token::RBrace => break,
                    _ => continue,
                }
            }
        }
        Ok(values)
    }

    pub fn parse_grant_permission(&mut self) -> Result<ParsedAction, ParserError> {
        if let Some(kw) = self.parse_one_of_keywords(&[
            Keyword::CONNECT,
            Keyword::CREATE,
            Keyword::DELETE,
            Keyword::EXECUTE,
            Keyword::INSERT,
            Keyword::REFERENCES,
            Keyword::SELECT,
            Keyword::TEMPORARY,
            Keyword::TRIGGER,
            Keyword::TRUNCATE,
            Keyword::UPDATE,
            Keyword::USAGE,
        ]) {
            let columns = match kw {
                Keyword::INSERT | Keyword::REFERENCES | Keyword::SELECT | Keyword::UPDATE => {
                    let columns = self.parse_parenthesized_column_list(Optional, false)?;
                    if columns.is_empty() {
                        None
                    } else {
                        Some(columns)
                    }
                }
                _ => None,
            };
            Ok((kw, columns))
        } else {
            self.expected("a privilege keyword", self.peek_token())?
        }
    }

    pub fn parse_grant_revoke_privileges_objects(
        &mut self,
    ) -> Result<(Privileges, GrantObjects), ParserError> {
        let privileges = if self.parse_keyword(Keyword::ALL) {
            Privileges::All {
                with_privileges_keyword: self.parse_keyword(Keyword::PRIVILEGES),
            }
        } else {
            let (actions, err): (Vec<_>, Vec<_>) = self
                .parse_actions_list()?
                .into_iter()
                .map(|(kw, columns)| match kw {
                    Keyword::DELETE => Ok(Action::Delete),
                    Keyword::INSERT => Ok(Action::Insert { columns }),
                    Keyword::REFERENCES => Ok(Action::References { columns }),
                    Keyword::SELECT => Ok(Action::Select { columns }),
                    Keyword::TRIGGER => Ok(Action::Trigger),
                    Keyword::TRUNCATE => Ok(Action::Truncate),
                    Keyword::UPDATE => Ok(Action::Update { columns }),
                    Keyword::USAGE => Ok(Action::Usage),
                    Keyword::CONNECT => Ok(Action::Connect),
                    Keyword::CREATE => Ok(Action::Create),
                    Keyword::EXECUTE => Ok(Action::Execute),
                    Keyword::TEMPORARY => Ok(Action::Temporary),
                    // This will cover all future added keywords to
                    // parse_grant_permission and unhandled in this
                    // match
                    _ => Err(kw),
                })
                .partition(Result::is_ok);

            if !err.is_empty() {
                let errors: Vec<Keyword> = err.into_iter().filter_map(|x| x.err()).collect();
                return Err(ParserError::ParserError(format!(
                    "INTERNAL ERROR: GRANT/REVOKE unexpected keyword(s) - {errors:?}"
                )));
            }
            let act = actions.into_iter().filter_map(|x| x.ok()).collect();
            Privileges::Actions(act)
        };

        self.expect_keyword(Keyword::ON)?;

        let objects = if self.parse_keywords(&[
            Keyword::ALL,
            Keyword::TABLES,
            Keyword::IN,
            Keyword::SCHEMA,
        ]) {
            GrantObjects::AllTablesInSchema {
                schemas: self.parse_comma_separated(|p| p.parse_object_name(false))?,
            }
        } else if self.parse_keywords(&[
            Keyword::ALL,
            Keyword::SEQUENCES,
            Keyword::IN,
            Keyword::SCHEMA,
        ]) {
            GrantObjects::AllSequencesInSchema {
                schemas: self.parse_comma_separated(|p| p.parse_object_name(false))?,
            }
        } else {
            let object_type =
                self.parse_one_of_keywords(&[Keyword::SEQUENCE, Keyword::SCHEMA, Keyword::TABLE]);
            let objects = self.parse_comma_separated(|p| p.parse_object_name(false));
            match object_type {
                Some(Keyword::SCHEMA) => GrantObjects::Schemas(objects?),
                Some(Keyword::SEQUENCE) => GrantObjects::Sequences(objects?),
                Some(Keyword::TABLE) | None => GrantObjects::Tables(objects?),
                _ => unreachable!(),
            }
        };

        Ok((privileges, objects))
    }
}

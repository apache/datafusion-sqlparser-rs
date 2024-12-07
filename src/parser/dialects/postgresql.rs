use crate::parser::*;

impl<'a> Parser<'a> {
    /// Parse `CREATE FUNCTION` for [Postgres]
    ///
    /// [Postgres]: https://www.postgresql.org/docs/15/sql-createfunction.html
    pub fn parse_postgres_create_function(
        &mut self,
        or_replace: bool,
        temporary: bool,
    ) -> Result<Statement, ParserError> {
        let name = self.parse_object_name(false)?;
        self.expect_token(&Token::LParen)?;
        let args = if self.consume_token(&Token::RParen) {
            self.prev_token();
            None
        } else {
            Some(self.parse_comma_separated(Parser::parse_function_arg)?)
        };

        self.expect_token(&Token::RParen)?;

        let return_type = if self.parse_keyword(Keyword::RETURNS) {
            Some(self.parse_data_type()?)
        } else {
            None
        };

        #[derive(Default)]
        struct Body {
            language: Option<Ident>,
            behavior: Option<FunctionBehavior>,
            function_body: Option<CreateFunctionBody>,
            called_on_null: Option<FunctionCalledOnNull>,
            parallel: Option<FunctionParallel>,
        }
        let mut body = Body::default();
        loop {
            fn ensure_not_set<T>(field: &Option<T>, name: &str) -> Result<(), ParserError> {
                if field.is_some() {
                    return Err(ParserError::ParserError(format!(
                        "{name} specified more than once",
                    )));
                }
                Ok(())
            }
            if self.parse_keyword(Keyword::AS) {
                ensure_not_set(&body.function_body, "AS")?;
                body.function_body = Some(CreateFunctionBody::AsBeforeOptions(
                    self.parse_create_function_body_string()?,
                ));
            } else if self.parse_keyword(Keyword::LANGUAGE) {
                ensure_not_set(&body.language, "LANGUAGE")?;
                body.language = Some(self.parse_identifier(false)?);
            } else if self.parse_keyword(Keyword::IMMUTABLE) {
                ensure_not_set(&body.behavior, "IMMUTABLE | STABLE | VOLATILE")?;
                body.behavior = Some(FunctionBehavior::Immutable);
            } else if self.parse_keyword(Keyword::STABLE) {
                ensure_not_set(&body.behavior, "IMMUTABLE | STABLE | VOLATILE")?;
                body.behavior = Some(FunctionBehavior::Stable);
            } else if self.parse_keyword(Keyword::VOLATILE) {
                ensure_not_set(&body.behavior, "IMMUTABLE | STABLE | VOLATILE")?;
                body.behavior = Some(FunctionBehavior::Volatile);
            } else if self.parse_keywords(&[
                Keyword::CALLED,
                Keyword::ON,
                Keyword::NULL,
                Keyword::INPUT,
            ]) {
                ensure_not_set(
                    &body.called_on_null,
                    "CALLED ON NULL INPUT | RETURNS NULL ON NULL INPUT | STRICT",
                )?;
                body.called_on_null = Some(FunctionCalledOnNull::CalledOnNullInput);
            } else if self.parse_keywords(&[
                Keyword::RETURNS,
                Keyword::NULL,
                Keyword::ON,
                Keyword::NULL,
                Keyword::INPUT,
            ]) {
                ensure_not_set(
                    &body.called_on_null,
                    "CALLED ON NULL INPUT | RETURNS NULL ON NULL INPUT | STRICT",
                )?;
                body.called_on_null = Some(FunctionCalledOnNull::ReturnsNullOnNullInput);
            } else if self.parse_keyword(Keyword::STRICT) {
                ensure_not_set(
                    &body.called_on_null,
                    "CALLED ON NULL INPUT | RETURNS NULL ON NULL INPUT | STRICT",
                )?;
                body.called_on_null = Some(FunctionCalledOnNull::Strict);
            } else if self.parse_keyword(Keyword::PARALLEL) {
                ensure_not_set(&body.parallel, "PARALLEL { UNSAFE | RESTRICTED | SAFE }")?;
                if self.parse_keyword(Keyword::UNSAFE) {
                    body.parallel = Some(FunctionParallel::Unsafe);
                } else if self.parse_keyword(Keyword::RESTRICTED) {
                    body.parallel = Some(FunctionParallel::Restricted);
                } else if self.parse_keyword(Keyword::SAFE) {
                    body.parallel = Some(FunctionParallel::Safe);
                } else {
                    return self.expected("one of UNSAFE | RESTRICTED | SAFE", self.peek_token());
                }
            } else if self.parse_keyword(Keyword::RETURN) {
                ensure_not_set(&body.function_body, "RETURN")?;
                body.function_body = Some(CreateFunctionBody::Return(self.parse_expr()?));
            } else {
                break;
            }
        }

        Ok(Statement::CreateFunction(CreateFunction {
            or_replace,
            temporary,
            name,
            args,
            return_type,
            behavior: body.behavior,
            called_on_null: body.called_on_null,
            parallel: body.parallel,
            language: body.language,
            function_body: body.function_body,
            if_not_exists: false,
            using: None,
            determinism_specifier: None,
            options: None,
            remote_connection: None,
        }))
    }

    /// Parse a postgresql casting style which is in the form of `expr::datatype`.
    pub fn parse_pg_cast(&mut self, expr: Expr) -> Result<Expr, ParserError> {
        Ok(Expr::Cast {
            kind: CastKind::DoubleColon,
            expr: Box::new(expr),
            data_type: self.parse_data_type()?,
            format: None,
        })
    }

    pub(crate) fn parse_pg_alter_role(&mut self) -> Result<Statement, ParserError> {
        let role_name = self.parse_identifier(false)?;

        // [ IN DATABASE _`database_name`_ ]
        let in_database = if self.parse_keywords(&[Keyword::IN, Keyword::DATABASE]) {
            self.parse_object_name(false).ok()
        } else {
            None
        };

        let operation = if self.parse_keyword(Keyword::RENAME) {
            if self.parse_keyword(Keyword::TO) {
                let role_name = self.parse_identifier(false)?;
                AlterRoleOperation::RenameRole { role_name }
            } else {
                return self.expected("TO after RENAME", self.peek_token());
            }
        // SET
        } else if self.parse_keyword(Keyword::SET) {
            let config_name = self.parse_object_name(false)?;
            // FROM CURRENT
            if self.parse_keywords(&[Keyword::FROM, Keyword::CURRENT]) {
                AlterRoleOperation::Set {
                    config_name,
                    config_value: SetConfigValue::FromCurrent,
                    in_database,
                }
            // { TO | = } { value | DEFAULT }
            } else if self.consume_token(&Token::Eq) || self.parse_keyword(Keyword::TO) {
                if self.parse_keyword(Keyword::DEFAULT) {
                    AlterRoleOperation::Set {
                        config_name,
                        config_value: SetConfigValue::Default,
                        in_database,
                    }
                } else if let Ok(expr) = self.parse_expr() {
                    AlterRoleOperation::Set {
                        config_name,
                        config_value: SetConfigValue::Value(expr),
                        in_database,
                    }
                } else {
                    self.expected("config value", self.peek_token())?
                }
            } else {
                self.expected("'TO' or '=' or 'FROM CURRENT'", self.peek_token())?
            }
        // RESET
        } else if self.parse_keyword(Keyword::RESET) {
            if self.parse_keyword(Keyword::ALL) {
                AlterRoleOperation::Reset {
                    config_name: ResetConfig::ALL,
                    in_database,
                }
            } else {
                let config_name = self.parse_object_name(false)?;
                AlterRoleOperation::Reset {
                    config_name: ResetConfig::ConfigName(config_name),
                    in_database,
                }
            }
        // option
        } else {
            // [ WITH ]
            let _ = self.parse_keyword(Keyword::WITH);
            // option
            let mut options = vec![];
            while let Some(opt) = self.maybe_parse(|parser| parser.parse_pg_role_option())? {
                options.push(opt);
            }
            // check option
            if options.is_empty() {
                return self.expected("option", self.peek_token())?;
            }

            AlterRoleOperation::WithOptions { options }
        };

        Ok(Statement::AlterRole {
            name: role_name,
            operation,
        })
    }

    pub(crate) fn parse_pg_role_option(&mut self) -> Result<RoleOption, ParserError> {
        let option = match self.parse_one_of_keywords(&[
            Keyword::BYPASSRLS,
            Keyword::NOBYPASSRLS,
            Keyword::CONNECTION,
            Keyword::CREATEDB,
            Keyword::NOCREATEDB,
            Keyword::CREATEROLE,
            Keyword::NOCREATEROLE,
            Keyword::INHERIT,
            Keyword::NOINHERIT,
            Keyword::LOGIN,
            Keyword::NOLOGIN,
            Keyword::PASSWORD,
            Keyword::REPLICATION,
            Keyword::NOREPLICATION,
            Keyword::SUPERUSER,
            Keyword::NOSUPERUSER,
            Keyword::VALID,
        ]) {
            Some(Keyword::BYPASSRLS) => RoleOption::BypassRLS(true),
            Some(Keyword::NOBYPASSRLS) => RoleOption::BypassRLS(false),
            Some(Keyword::CONNECTION) => {
                self.expect_keyword(Keyword::LIMIT)?;
                RoleOption::ConnectionLimit(Expr::Value(self.parse_number_value()?))
            }
            Some(Keyword::CREATEDB) => RoleOption::CreateDB(true),
            Some(Keyword::NOCREATEDB) => RoleOption::CreateDB(false),
            Some(Keyword::CREATEROLE) => RoleOption::CreateRole(true),
            Some(Keyword::NOCREATEROLE) => RoleOption::CreateRole(false),
            Some(Keyword::INHERIT) => RoleOption::Inherit(true),
            Some(Keyword::NOINHERIT) => RoleOption::Inherit(false),
            Some(Keyword::LOGIN) => RoleOption::Login(true),
            Some(Keyword::NOLOGIN) => RoleOption::Login(false),
            Some(Keyword::PASSWORD) => {
                let password = if self.parse_keyword(Keyword::NULL) {
                    Password::NullPassword
                } else {
                    Password::Password(Expr::Value(self.parse_value()?))
                };
                RoleOption::Password(password)
            }
            Some(Keyword::REPLICATION) => RoleOption::Replication(true),
            Some(Keyword::NOREPLICATION) => RoleOption::Replication(false),
            Some(Keyword::SUPERUSER) => RoleOption::SuperUser(true),
            Some(Keyword::NOSUPERUSER) => RoleOption::SuperUser(false),
            Some(Keyword::VALID) => {
                self.expect_keyword(Keyword::UNTIL)?;
                RoleOption::ValidUntil(Expr::Value(self.parse_value()?))
            }
            _ => self.expected("option", self.peek_token())?,
        };

        Ok(option)
    }
}

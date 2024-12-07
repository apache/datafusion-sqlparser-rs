use super::*;

impl<'a> Parser<'a> {
    pub fn parse_show(&mut self) -> Result<Statement, ParserError> {
        let terse = self.parse_keyword(Keyword::TERSE);
        let extended = self.parse_keyword(Keyword::EXTENDED);
        let full = self.parse_keyword(Keyword::FULL);
        let session = self.parse_keyword(Keyword::SESSION);
        let global = self.parse_keyword(Keyword::GLOBAL);
        let external = self.parse_keyword(Keyword::EXTERNAL);
        if self
            .parse_one_of_keywords(&[Keyword::COLUMNS, Keyword::FIELDS])
            .is_some()
        {
            Ok(self.parse_show_columns(extended, full)?)
        } else if self.parse_keyword(Keyword::TABLES) {
            Ok(self.parse_show_tables(terse, extended, full, external)?)
        } else if self.parse_keywords(&[Keyword::MATERIALIZED, Keyword::VIEWS]) {
            Ok(self.parse_show_views(terse, true)?)
        } else if self.parse_keyword(Keyword::VIEWS) {
            Ok(self.parse_show_views(terse, false)?)
        } else if self.parse_keyword(Keyword::FUNCTIONS) {
            Ok(self.parse_show_functions()?)
        } else if extended || full {
            Err(ParserError::ParserError(
                "EXTENDED/FULL are not supported with this type of SHOW query".to_string(),
            ))
        } else if self.parse_one_of_keywords(&[Keyword::CREATE]).is_some() {
            Ok(self.parse_show_create()?)
        } else if self.parse_keyword(Keyword::COLLATION) {
            Ok(self.parse_show_collation()?)
        } else if self.parse_keyword(Keyword::VARIABLES)
            && dialect_of!(self is MySqlDialect | GenericDialect)
        {
            Ok(Statement::ShowVariables {
                filter: self.parse_show_statement_filter()?,
                session,
                global,
            })
        } else if self.parse_keyword(Keyword::STATUS)
            && dialect_of!(self is MySqlDialect | GenericDialect)
        {
            Ok(Statement::ShowStatus {
                filter: self.parse_show_statement_filter()?,
                session,
                global,
            })
        } else if self.parse_keyword(Keyword::DATABASES) {
            self.parse_show_databases(terse)
        } else if self.parse_keyword(Keyword::SCHEMAS) {
            self.parse_show_schemas(terse)
        } else {
            Ok(Statement::ShowVariable {
                variable: self.parse_identifiers()?,
            })
        }
    }

    fn parse_show_databases(&mut self, terse: bool) -> Result<Statement, ParserError> {
        let history = self.parse_keyword(Keyword::HISTORY);
        let show_options = self.parse_show_stmt_options()?;
        Ok(Statement::ShowDatabases {
            terse,
            history,
            show_options,
        })
    }

    fn parse_show_schemas(&mut self, terse: bool) -> Result<Statement, ParserError> {
        let history = self.parse_keyword(Keyword::HISTORY);
        let show_options = self.parse_show_stmt_options()?;
        Ok(Statement::ShowSchemas {
            terse,
            history,
            show_options,
        })
    }

    pub fn parse_show_create(&mut self) -> Result<Statement, ParserError> {
        let obj_type = match self.expect_one_of_keywords(&[
            Keyword::TABLE,
            Keyword::TRIGGER,
            Keyword::FUNCTION,
            Keyword::PROCEDURE,
            Keyword::EVENT,
            Keyword::VIEW,
        ])? {
            Keyword::TABLE => Ok(ShowCreateObject::Table),
            Keyword::TRIGGER => Ok(ShowCreateObject::Trigger),
            Keyword::FUNCTION => Ok(ShowCreateObject::Function),
            Keyword::PROCEDURE => Ok(ShowCreateObject::Procedure),
            Keyword::EVENT => Ok(ShowCreateObject::Event),
            Keyword::VIEW => Ok(ShowCreateObject::View),
            keyword => Err(ParserError::ParserError(format!(
                "Unable to map keyword to ShowCreateObject: {keyword:?}"
            ))),
        }?;

        let obj_name = self.parse_object_name(false)?;

        Ok(Statement::ShowCreate { obj_type, obj_name })
    }

    pub fn parse_show_columns(
        &mut self,
        extended: bool,
        full: bool,
    ) -> Result<Statement, ParserError> {
        let show_options = self.parse_show_stmt_options()?;
        Ok(Statement::ShowColumns {
            extended,
            full,
            show_options,
        })
    }

    fn parse_show_tables(
        &mut self,
        terse: bool,
        extended: bool,
        full: bool,
        external: bool,
    ) -> Result<Statement, ParserError> {
        let history = !external && self.parse_keyword(Keyword::HISTORY);
        let show_options = self.parse_show_stmt_options()?;
        Ok(Statement::ShowTables {
            terse,
            history,
            extended,
            full,
            external,
            show_options,
        })
    }

    fn parse_show_views(
        &mut self,
        terse: bool,
        materialized: bool,
    ) -> Result<Statement, ParserError> {
        let show_options = self.parse_show_stmt_options()?;
        Ok(Statement::ShowViews {
            materialized,
            terse,
            show_options,
        })
    }

    pub fn parse_show_functions(&mut self) -> Result<Statement, ParserError> {
        let filter = self.parse_show_statement_filter()?;
        Ok(Statement::ShowFunctions { filter })
    }

    pub fn parse_show_collation(&mut self) -> Result<Statement, ParserError> {
        let filter = self.parse_show_statement_filter()?;
        Ok(Statement::ShowCollation { filter })
    }

    pub fn parse_show_statement_filter(
        &mut self,
    ) -> Result<Option<ShowStatementFilter>, ParserError> {
        if self.parse_keyword(Keyword::LIKE) {
            Ok(Some(ShowStatementFilter::Like(
                self.parse_literal_string()?,
            )))
        } else if self.parse_keyword(Keyword::ILIKE) {
            Ok(Some(ShowStatementFilter::ILike(
                self.parse_literal_string()?,
            )))
        } else if self.parse_keyword(Keyword::WHERE) {
            Ok(Some(ShowStatementFilter::Where(self.parse_expr()?)))
        } else {
            self.maybe_parse(|parser| -> Result<String, ParserError> {
                parser.parse_literal_string()
            })?
            .map_or(Ok(None), |filter| {
                Ok(Some(ShowStatementFilter::NoKeyword(filter)))
            })
        }
    }

    fn parse_show_stmt_options(&mut self) -> Result<ShowStatementOptions, ParserError> {
        let show_in;
        let mut filter_position = None;
        if self.dialect.supports_show_like_before_in() {
            if let Some(filter) = self.parse_show_statement_filter()? {
                filter_position = Some(ShowStatementFilterPosition::Infix(filter));
            }
            show_in = self.maybe_parse_show_stmt_in()?;
        } else {
            show_in = self.maybe_parse_show_stmt_in()?;
            if let Some(filter) = self.parse_show_statement_filter()? {
                filter_position = Some(ShowStatementFilterPosition::Suffix(filter));
            }
        }
        let starts_with = self.maybe_parse_show_stmt_starts_with()?;
        let limit = self.maybe_parse_show_stmt_limit()?;
        let from = self.maybe_parse_show_stmt_from()?;
        Ok(ShowStatementOptions {
            filter_position,
            show_in,
            starts_with,
            limit,
            limit_from: from,
        })
    }

    fn maybe_parse_show_stmt_in(&mut self) -> Result<Option<ShowStatementIn>, ParserError> {
        let clause = match self.parse_one_of_keywords(&[Keyword::FROM, Keyword::IN]) {
            Some(Keyword::FROM) => ShowStatementInClause::FROM,
            Some(Keyword::IN) => ShowStatementInClause::IN,
            None => return Ok(None),
            _ => return self.expected("FROM or IN", self.peek_token()),
        };

        let (parent_type, parent_name) = match self.parse_one_of_keywords(&[
            Keyword::ACCOUNT,
            Keyword::DATABASE,
            Keyword::SCHEMA,
            Keyword::TABLE,
            Keyword::VIEW,
        ]) {
            // If we see these next keywords it means we don't have a parent name
            Some(Keyword::DATABASE)
                if self.peek_keywords(&[Keyword::STARTS, Keyword::WITH])
                    | self.peek_keyword(Keyword::LIMIT) =>
            {
                (Some(ShowStatementInParentType::Database), None)
            }
            Some(Keyword::SCHEMA)
                if self.peek_keywords(&[Keyword::STARTS, Keyword::WITH])
                    | self.peek_keyword(Keyword::LIMIT) =>
            {
                (Some(ShowStatementInParentType::Schema), None)
            }
            Some(parent_kw) => {
                // The parent name here is still optional, for example:
                // SHOW TABLES IN ACCOUNT, so parsing the object name
                // may fail because the statement ends.
                let parent_name = self.maybe_parse(|p| p.parse_object_name(false))?;
                match parent_kw {
                    Keyword::ACCOUNT => (Some(ShowStatementInParentType::Account), parent_name),
                    Keyword::DATABASE => (Some(ShowStatementInParentType::Database), parent_name),
                    Keyword::SCHEMA => (Some(ShowStatementInParentType::Schema), parent_name),
                    Keyword::TABLE => (Some(ShowStatementInParentType::Table), parent_name),
                    Keyword::VIEW => (Some(ShowStatementInParentType::View), parent_name),
                    _ => {
                        return self.expected(
                            "one of ACCOUNT, DATABASE, SCHEMA, TABLE or VIEW",
                            self.peek_token(),
                        )
                    }
                }
            }
            None => {
                // Parsing MySQL style FROM tbl_name FROM db_name
                // which is equivalent to FROM tbl_name.db_name
                let mut parent_name = self.parse_object_name(false)?;
                if self
                    .parse_one_of_keywords(&[Keyword::FROM, Keyword::IN])
                    .is_some()
                {
                    parent_name.0.insert(0, self.parse_identifier(false)?);
                }
                (None, Some(parent_name))
            }
        };

        Ok(Some(ShowStatementIn {
            clause,
            parent_type,
            parent_name,
        }))
    }

    fn maybe_parse_show_stmt_starts_with(&mut self) -> Result<Option<Value>, ParserError> {
        if self.parse_keywords(&[Keyword::STARTS, Keyword::WITH]) {
            Ok(Some(self.parse_value()?))
        } else {
            Ok(None)
        }
    }

    fn maybe_parse_show_stmt_limit(&mut self) -> Result<Option<Expr>, ParserError> {
        if self.parse_keyword(Keyword::LIMIT) {
            Ok(self.parse_limit()?)
        } else {
            Ok(None)
        }
    }

    fn maybe_parse_show_stmt_from(&mut self) -> Result<Option<Value>, ParserError> {
        if self.parse_keyword(Keyword::FROM) {
            Ok(Some(self.parse_value()?))
        } else {
            Ok(None)
        }
    }
}

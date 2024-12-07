use super::*;

impl<'a> Parser<'a> {
    pub fn parse_columns(&mut self) -> Result<(Vec<ColumnDef>, Vec<TableConstraint>), ParserError> {
        let mut columns = vec![];
        let mut constraints = vec![];
        if !self.consume_token(&Token::LParen) || self.consume_token(&Token::RParen) {
            return Ok((columns, constraints));
        }

        loop {
            if let Some(constraint) = self.parse_optional_table_constraint()? {
                constraints.push(constraint);
            } else if let Token::Word(_) = self.peek_token().token {
                columns.push(self.parse_column_def()?);
            } else {
                return self.expected("column name or constraint definition", self.peek_token());
            }

            let comma = self.consume_token(&Token::Comma);
            let rparen = self.peek_token().token == Token::RParen;

            if !comma && !rparen {
                return self.expected("',' or ')' after column definition", self.peek_token());
            };

            if rparen && (!comma || self.options.trailing_commas) {
                let _ = self.consume_token(&Token::RParen);
                break;
            }
        }

        Ok((columns, constraints))
    }

    pub fn parse_column_def(&mut self) -> Result<ColumnDef, ParserError> {
        let name = self.parse_identifier(false)?;
        let data_type = if self.is_column_type_sqlite_unspecified() {
            DataType::Unspecified
        } else {
            self.parse_data_type()?
        };
        let mut collation = if self.parse_keyword(Keyword::COLLATE) {
            Some(self.parse_object_name(false)?)
        } else {
            None
        };
        let mut options = vec![];
        loop {
            if self.parse_keyword(Keyword::CONSTRAINT) {
                let name = Some(self.parse_identifier(false)?);
                if let Some(option) = self.parse_optional_column_option()? {
                    options.push(ColumnOptionDef { name, option });
                } else {
                    return self.expected(
                        "constraint details after CONSTRAINT <name>",
                        self.peek_token(),
                    );
                }
            } else if let Some(option) = self.parse_optional_column_option()? {
                options.push(ColumnOptionDef { name: None, option });
            } else if dialect_of!(self is MySqlDialect | SnowflakeDialect | GenericDialect)
                && self.parse_keyword(Keyword::COLLATE)
            {
                collation = Some(self.parse_object_name(false)?);
            } else {
                break;
            };
        }
        Ok(ColumnDef {
            name,
            data_type,
            collation,
            options,
        })
    }

    fn is_column_type_sqlite_unspecified(&mut self) -> bool {
        if dialect_of!(self is SQLiteDialect) {
            match self.peek_token().token {
                Token::Word(word) => matches!(
                    word.keyword,
                    Keyword::CONSTRAINT
                        | Keyword::PRIMARY
                        | Keyword::NOT
                        | Keyword::UNIQUE
                        | Keyword::CHECK
                        | Keyword::DEFAULT
                        | Keyword::COLLATE
                        | Keyword::REFERENCES
                        | Keyword::GENERATED
                        | Keyword::AS
                ),
                _ => true, // e.g. comma immediately after column name
            }
        } else {
            false
        }
    }

    pub fn parse_optional_column_option(&mut self) -> Result<Option<ColumnOption>, ParserError> {
        if let Some(option) = self.dialect.parse_column_option(self)? {
            return option;
        }

        if self.parse_keywords(&[Keyword::CHARACTER, Keyword::SET]) {
            Ok(Some(ColumnOption::CharacterSet(
                self.parse_object_name(false)?,
            )))
        } else if self.parse_keywords(&[Keyword::NOT, Keyword::NULL]) {
            Ok(Some(ColumnOption::NotNull))
        } else if self.parse_keywords(&[Keyword::COMMENT]) {
            let next_token = self.next_token();
            match next_token.token {
                Token::SingleQuotedString(value, ..) => Ok(Some(ColumnOption::Comment(value))),
                _ => self.expected("string", next_token),
            }
        } else if self.parse_keyword(Keyword::NULL) {
            Ok(Some(ColumnOption::Null))
        } else if self.parse_keyword(Keyword::DEFAULT) {
            Ok(Some(ColumnOption::Default(self.parse_expr()?)))
        } else if dialect_of!(self is ClickHouseDialect| GenericDialect)
            && self.parse_keyword(Keyword::MATERIALIZED)
        {
            Ok(Some(ColumnOption::Materialized(self.parse_expr()?)))
        } else if dialect_of!(self is ClickHouseDialect| GenericDialect)
            && self.parse_keyword(Keyword::ALIAS)
        {
            Ok(Some(ColumnOption::Alias(self.parse_expr()?)))
        } else if dialect_of!(self is ClickHouseDialect| GenericDialect)
            && self.parse_keyword(Keyword::EPHEMERAL)
        {
            // The expression is optional for the EPHEMERAL syntax, so we need to check
            // if the column definition has remaining tokens before parsing the expression.
            if matches!(self.peek_token().token, Token::Comma | Token::RParen) {
                Ok(Some(ColumnOption::Ephemeral(None)))
            } else {
                Ok(Some(ColumnOption::Ephemeral(Some(self.parse_expr()?))))
            }
        } else if self.parse_keywords(&[Keyword::PRIMARY, Keyword::KEY]) {
            let characteristics = self.parse_constraint_characteristics()?;
            Ok(Some(ColumnOption::Unique {
                is_primary: true,
                characteristics,
            }))
        } else if self.parse_keyword(Keyword::UNIQUE) {
            let characteristics = self.parse_constraint_characteristics()?;
            Ok(Some(ColumnOption::Unique {
                is_primary: false,
                characteristics,
            }))
        } else if self.parse_keyword(Keyword::REFERENCES) {
            let foreign_table = self.parse_object_name(false)?;
            // PostgreSQL allows omitting the column list and
            // uses the primary key column of the foreign table by default
            let referred_columns = self.parse_parenthesized_column_list(Optional, false)?;
            let mut on_delete = None;
            let mut on_update = None;
            loop {
                if on_delete.is_none() && self.parse_keywords(&[Keyword::ON, Keyword::DELETE]) {
                    on_delete = Some(self.parse_referential_action()?);
                } else if on_update.is_none()
                    && self.parse_keywords(&[Keyword::ON, Keyword::UPDATE])
                {
                    on_update = Some(self.parse_referential_action()?);
                } else {
                    break;
                }
            }
            let characteristics = self.parse_constraint_characteristics()?;

            Ok(Some(ColumnOption::ForeignKey {
                foreign_table,
                referred_columns,
                on_delete,
                on_update,
                characteristics,
            }))
        } else if self.parse_keyword(Keyword::CHECK) {
            self.expect_token(&Token::LParen)?;
            let expr = self.parse_expr()?;
            self.expect_token(&Token::RParen)?;
            Ok(Some(ColumnOption::Check(expr)))
        } else if self.parse_keyword(Keyword::AUTO_INCREMENT)
            && dialect_of!(self is MySqlDialect | GenericDialect)
        {
            // Support AUTO_INCREMENT for MySQL
            Ok(Some(ColumnOption::DialectSpecific(vec![
                Token::make_keyword("AUTO_INCREMENT"),
            ])))
        } else if self.parse_keyword(Keyword::AUTOINCREMENT)
            && dialect_of!(self is SQLiteDialect |  GenericDialect)
        {
            // Support AUTOINCREMENT for SQLite
            Ok(Some(ColumnOption::DialectSpecific(vec![
                Token::make_keyword("AUTOINCREMENT"),
            ])))
        } else if self.parse_keyword(Keyword::ASC)
            && self.dialect.supports_asc_desc_in_column_definition()
        {
            // Support ASC for SQLite
            Ok(Some(ColumnOption::DialectSpecific(vec![
                Token::make_keyword("ASC"),
            ])))
        } else if self.parse_keyword(Keyword::DESC)
            && self.dialect.supports_asc_desc_in_column_definition()
        {
            // Support DESC for SQLite
            Ok(Some(ColumnOption::DialectSpecific(vec![
                Token::make_keyword("DESC"),
            ])))
        } else if self.parse_keywords(&[Keyword::ON, Keyword::UPDATE])
            && dialect_of!(self is MySqlDialect | GenericDialect)
        {
            let expr = self.parse_expr()?;
            Ok(Some(ColumnOption::OnUpdate(expr)))
        } else if self.parse_keyword(Keyword::GENERATED) {
            self.parse_optional_column_option_generated()
        } else if dialect_of!(self is BigQueryDialect | GenericDialect)
            && self.parse_keyword(Keyword::OPTIONS)
        {
            self.prev_token();
            Ok(Some(ColumnOption::Options(
                self.parse_options(Keyword::OPTIONS)?,
            )))
        } else if self.parse_keyword(Keyword::AS)
            && dialect_of!(self is MySqlDialect | SQLiteDialect | DuckDbDialect | GenericDialect)
        {
            self.parse_optional_column_option_as()
        } else if self.parse_keyword(Keyword::IDENTITY)
            && dialect_of!(self is MsSqlDialect | GenericDialect)
        {
            let parameters = if self.consume_token(&Token::LParen) {
                let seed = self.parse_number()?;
                self.expect_token(&Token::Comma)?;
                let increment = self.parse_number()?;
                self.expect_token(&Token::RParen)?;

                Some(IdentityPropertyFormatKind::FunctionCall(
                    IdentityParameters { seed, increment },
                ))
            } else {
                None
            };
            Ok(Some(ColumnOption::Identity(
                IdentityPropertyKind::Identity(IdentityProperty {
                    parameters,
                    order: None,
                }),
            )))
        } else if dialect_of!(self is SQLiteDialect | GenericDialect)
            && self.parse_keywords(&[Keyword::ON, Keyword::CONFLICT])
        {
            // Support ON CONFLICT for SQLite
            Ok(Some(ColumnOption::OnConflict(
                self.expect_one_of_keywords(&[
                    Keyword::ROLLBACK,
                    Keyword::ABORT,
                    Keyword::FAIL,
                    Keyword::IGNORE,
                    Keyword::REPLACE,
                ])?,
            )))
        } else {
            Ok(None)
        }
    }

    pub(crate) fn parse_tag(&mut self) -> Result<Tag, ParserError> {
        let name = self.parse_identifier(false)?;
        self.expect_token(&Token::Eq)?;
        let value = self.parse_literal_string()?;

        Ok(Tag::new(name, value))
    }

    fn parse_optional_column_option_generated(
        &mut self,
    ) -> Result<Option<ColumnOption>, ParserError> {
        if self.parse_keywords(&[Keyword::ALWAYS, Keyword::AS, Keyword::IDENTITY]) {
            let mut sequence_options = vec![];
            if self.expect_token(&Token::LParen).is_ok() {
                sequence_options = self.parse_create_sequence_options()?;
                self.expect_token(&Token::RParen)?;
            }
            Ok(Some(ColumnOption::Generated {
                generated_as: GeneratedAs::Always,
                sequence_options: Some(sequence_options),
                generation_expr: None,
                generation_expr_mode: None,
                generated_keyword: true,
            }))
        } else if self.parse_keywords(&[
            Keyword::BY,
            Keyword::DEFAULT,
            Keyword::AS,
            Keyword::IDENTITY,
        ]) {
            let mut sequence_options = vec![];
            if self.expect_token(&Token::LParen).is_ok() {
                sequence_options = self.parse_create_sequence_options()?;
                self.expect_token(&Token::RParen)?;
            }
            Ok(Some(ColumnOption::Generated {
                generated_as: GeneratedAs::ByDefault,
                sequence_options: Some(sequence_options),
                generation_expr: None,
                generation_expr_mode: None,
                generated_keyword: true,
            }))
        } else if self.parse_keywords(&[Keyword::ALWAYS, Keyword::AS]) {
            if self.expect_token(&Token::LParen).is_ok() {
                let expr = self.parse_expr()?;
                self.expect_token(&Token::RParen)?;
                let (gen_as, expr_mode) = if self.parse_keywords(&[Keyword::STORED]) {
                    Ok((
                        GeneratedAs::ExpStored,
                        Some(GeneratedExpressionMode::Stored),
                    ))
                } else if dialect_of!(self is PostgreSqlDialect) {
                    // Postgres' AS IDENTITY branches are above, this one needs STORED
                    self.expected("STORED", self.peek_token())
                } else if self.parse_keywords(&[Keyword::VIRTUAL]) {
                    Ok((GeneratedAs::Always, Some(GeneratedExpressionMode::Virtual)))
                } else {
                    Ok((GeneratedAs::Always, None))
                }?;

                Ok(Some(ColumnOption::Generated {
                    generated_as: gen_as,
                    sequence_options: None,
                    generation_expr: Some(expr),
                    generation_expr_mode: expr_mode,
                    generated_keyword: true,
                }))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    fn parse_optional_column_option_as(&mut self) -> Result<Option<ColumnOption>, ParserError> {
        // Some DBs allow 'AS (expr)', shorthand for GENERATED ALWAYS AS
        self.expect_token(&Token::LParen)?;
        let expr = self.parse_expr()?;
        self.expect_token(&Token::RParen)?;

        let (gen_as, expr_mode) = if self.parse_keywords(&[Keyword::STORED]) {
            (
                GeneratedAs::ExpStored,
                Some(GeneratedExpressionMode::Stored),
            )
        } else if self.parse_keywords(&[Keyword::VIRTUAL]) {
            (GeneratedAs::Always, Some(GeneratedExpressionMode::Virtual))
        } else {
            (GeneratedAs::Always, None)
        };

        Ok(Some(ColumnOption::Generated {
            generated_as: gen_as,
            sequence_options: None,
            generation_expr: Some(expr),
            generation_expr_mode: expr_mode,
            generated_keyword: false,
        }))
    }

    /// Parses a parenthesized, comma-separated list of column definitions within a view.
    pub(crate) fn parse_view_columns(&mut self) -> Result<Vec<ViewColumnDef>, ParserError> {
        if self.consume_token(&Token::LParen) {
            if self.peek_token().token == Token::RParen {
                self.next_token();
                Ok(vec![])
            } else {
                let cols = self.parse_comma_separated(Parser::parse_view_column)?;
                self.expect_token(&Token::RParen)?;
                Ok(cols)
            }
        } else {
            Ok(vec![])
        }
    }

    /// Parses a column definition within a view.
    fn parse_view_column(&mut self) -> Result<ViewColumnDef, ParserError> {
        let name = self.parse_identifier(false)?;
        let options = if (dialect_of!(self is BigQueryDialect | GenericDialect)
            && self.parse_keyword(Keyword::OPTIONS))
            || (dialect_of!(self is SnowflakeDialect | GenericDialect)
                && self.parse_keyword(Keyword::COMMENT))
        {
            self.prev_token();
            self.parse_optional_column_option()?
                .map(|option| vec![option])
        } else {
            None
        };
        let data_type = if dialect_of!(self is ClickHouseDialect) {
            Some(self.parse_data_type()?)
        } else {
            None
        };
        Ok(ViewColumnDef {
            name,
            data_type,
            options,
        })
    }

    /// Parse a parenthesized comma-separated list of unqualified, possibly quoted identifiers
    pub fn parse_parenthesized_column_list(
        &mut self,
        optional: IsOptional,
        allow_empty: bool,
    ) -> Result<Vec<Ident>, ParserError> {
        if self.consume_token(&Token::LParen) {
            if allow_empty && self.peek_token().token == Token::RParen {
                self.next_token();
                Ok(vec![])
            } else {
                let cols = self.parse_comma_separated(|p| p.parse_identifier(false))?;
                self.expect_token(&Token::RParen)?;
                Ok(cols)
            }
        } else if optional == Optional {
            Ok(vec![])
        } else {
            self.expected("a list of columns in parentheses", self.peek_token())
        }
    }
}

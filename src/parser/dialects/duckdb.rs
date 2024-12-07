use crate::parser::*;

// DuckDB related parsing

impl<'a> Parser<'a> {
    pub fn parse_attach_duckdb_database_options(
        &mut self,
    ) -> Result<Vec<AttachDuckDBDatabaseOption>, ParserError> {
        if !self.consume_token(&Token::LParen) {
            return Ok(vec![]);
        }

        let mut options = vec![];
        loop {
            if self.parse_keyword(Keyword::READ_ONLY) {
                let boolean = if self.parse_keyword(Keyword::TRUE) {
                    Some(true)
                } else if self.parse_keyword(Keyword::FALSE) {
                    Some(false)
                } else {
                    None
                };
                options.push(AttachDuckDBDatabaseOption::ReadOnly(boolean));
            } else if self.parse_keyword(Keyword::TYPE) {
                let ident = self.parse_identifier(false)?;
                options.push(AttachDuckDBDatabaseOption::Type(ident));
            } else {
                return self.expected("expected one of: ), READ_ONLY, TYPE", self.peek_token());
            };

            if self.consume_token(&Token::RParen) {
                return Ok(options);
            } else if self.consume_token(&Token::Comma) {
                continue;
            } else {
                return self.expected("expected one of: ')', ','", self.peek_token());
            }
        }
    }

    pub fn parse_attach_duckdb_database(&mut self) -> Result<Statement, ParserError> {
        let database = self.parse_keyword(Keyword::DATABASE);
        let if_not_exists = self.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let database_path = self.parse_identifier(false)?;
        let database_alias = if self.parse_keyword(Keyword::AS) {
            Some(self.parse_identifier(false)?)
        } else {
            None
        };

        let attach_options = self.parse_attach_duckdb_database_options()?;
        Ok(Statement::AttachDuckDBDatabase {
            if_not_exists,
            database,
            database_path,
            database_alias,
            attach_options,
        })
    }

    pub fn parse_detach_duckdb_database(&mut self) -> Result<Statement, ParserError> {
        let database = self.parse_keyword(Keyword::DATABASE);
        let if_exists = self.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
        let database_alias = self.parse_identifier(false)?;
        Ok(Statement::DetachDuckDBDatabase {
            if_exists,
            database,
            database_alias,
        })
    }

    /// See [DuckDB Docs](https://duckdb.org/docs/sql/statements/create_secret.html) for more details.
    pub fn parse_create_secret(
        &mut self,
        or_replace: bool,
        temporary: bool,
        persistent: bool,
    ) -> Result<Statement, ParserError> {
        let if_not_exists = self.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);

        let mut storage_specifier = None;
        let mut name = None;
        if self.peek_token() != Token::LParen {
            if self.parse_keyword(Keyword::IN) {
                storage_specifier = self.parse_identifier(false).ok()
            } else {
                name = self.parse_identifier(false).ok();
            }

            // Storage specifier may follow the name
            if storage_specifier.is_none()
                && self.peek_token() != Token::LParen
                && self.parse_keyword(Keyword::IN)
            {
                storage_specifier = self.parse_identifier(false).ok();
            }
        }

        self.expect_token(&Token::LParen)?;
        self.expect_keyword(Keyword::TYPE)?;
        let secret_type = self.parse_identifier(false)?;

        let mut options = Vec::new();
        if self.consume_token(&Token::Comma) {
            options.append(&mut self.parse_comma_separated(|p| {
                let key = p.parse_identifier(false)?;
                let value = p.parse_identifier(false)?;
                Ok(SecretOption { key, value })
            })?);
        }
        self.expect_token(&Token::RParen)?;

        let temp = match (temporary, persistent) {
            (true, false) => Some(true),
            (false, true) => Some(false),
            (false, false) => None,
            _ => self.expected("TEMPORARY or PERSISTENT", self.peek_token())?,
        };

        Ok(Statement::CreateSecret {
            or_replace,
            temporary: temp,
            if_not_exists,
            name,
            storage_specifier,
            secret_type,
            options,
        })
    }

    /// Parse a field for a duckdb [dictionary]
    ///
    /// Syntax
    ///
    /// ```sql
    /// 'name': expr
    /// ```
    ///
    /// [dictionary]: https://duckdb.org/docs/sql/data_types/struct#creating-structs
    pub(crate) fn parse_duckdb_dictionary_field(&mut self) -> Result<DictionaryField, ParserError> {
        let key = self.parse_identifier(false)?;

        self.expect_token(&Token::Colon)?;

        let expr = self.parse_expr()?;

        Ok(DictionaryField {
            key,
            value: Box::new(expr),
        })
    }

    /// Parse a field for a duckdb [map]
    ///
    /// Syntax
    ///
    /// ```sql
    /// key: value
    /// ```
    ///
    /// [map]: https://duckdb.org/docs/sql/data_types/map.html#creating-maps
    pub(crate) fn parse_duckdb_map_field(&mut self) -> Result<MapEntry, ParserError> {
        let key = self.parse_expr()?;

        self.expect_token(&Token::Colon)?;

        let value = self.parse_expr()?;

        Ok(MapEntry {
            key: Box::new(key),
            value: Box::new(value),
        })
    }

    /// DuckDB specific: Parse a duckdb [map]
    ///
    /// Syntax:
    ///
    /// ```sql
    /// Map {key1: value1[, ... ]}
    /// ```
    ///
    /// [map]: https://duckdb.org/docs/sql/data_types/map.html#creating-maps
    pub(crate) fn parse_duckdb_map_literal(&mut self) -> Result<Expr, ParserError> {
        self.expect_token(&Token::LBrace)?;
        let fields = self.parse_comma_separated0(Self::parse_duckdb_map_field, Token::RBrace)?;
        self.expect_token(&Token::RBrace)?;
        Ok(Expr::Map(Map { entries: fields }))
    }

    /// DuckDB specific: Parse a duckdb [dictionary]
    ///
    /// Syntax:
    ///
    /// ```sql
    /// {'field_name': expr1[, ... ]}
    /// ```
    ///
    /// [dictionary]: https://duckdb.org/docs/sql/data_types/struct#creating-structs
    pub(crate) fn parse_duckdb_struct_literal(&mut self) -> Result<Expr, ParserError> {
        self.expect_token(&Token::LBrace)?;

        let fields = self.parse_comma_separated(Self::parse_duckdb_dictionary_field)?;

        self.expect_token(&Token::RBrace)?;

        Ok(Expr::Dictionary(fields))
    }

    /// Duckdb Struct Data Type <https://duckdb.org/docs/sql/data_types/struct.html#retrieving-from-structs>
    pub(crate) fn parse_duckdb_struct_type_def(&mut self) -> Result<Vec<StructField>, ParserError> {
        self.expect_keyword(Keyword::STRUCT)?;
        self.expect_token(&Token::LParen)?;
        let struct_body = self.parse_comma_separated(|parser| {
            let field_name = parser.parse_identifier(false)?;
            let field_type = parser.parse_data_type()?;

            Ok(StructField {
                field_name: Some(field_name),
                field_type,
            })
        });
        self.expect_token(&Token::RParen)?;
        struct_body
    }
}

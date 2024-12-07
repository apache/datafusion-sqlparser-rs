use crate::parser::*;

impl<'a> Parser<'a> {
    /// Parse `CREATE FUNCTION` for [Hive]
    ///
    /// [Hive]: https://cwiki.apache.org/confluence/display/hive/languagemanual+ddl#LanguageManualDDL-Create/Drop/ReloadFunction
    pub fn parse_hive_create_function(
        &mut self,
        or_replace: bool,
        temporary: bool,
    ) -> Result<Statement, ParserError> {
        let name = self.parse_object_name(false)?;
        self.expect_keyword(Keyword::AS)?;

        let as_ = self.parse_create_function_body_string()?;
        let using = self.parse_optional_create_function_using()?;

        Ok(Statement::CreateFunction(CreateFunction {
            or_replace,
            temporary,
            name,
            function_body: Some(CreateFunctionBody::AsBeforeOptions(as_)),
            using,
            if_not_exists: false,
            args: None,
            return_type: None,
            behavior: None,
            called_on_null: None,
            parallel: None,
            language: None,
            determinism_specifier: None,
            options: None,
            remote_connection: None,
        }))
    }

    pub fn parse_hive_formats(&mut self) -> Result<HiveFormat, ParserError> {
        let mut hive_format = HiveFormat::default();
        loop {
            match self.parse_one_of_keywords(&[
                Keyword::ROW,
                Keyword::STORED,
                Keyword::LOCATION,
                Keyword::WITH,
            ]) {
                Some(Keyword::ROW) => {
                    hive_format.row_format = Some(self.parse_row_format()?);
                }
                Some(Keyword::STORED) => {
                    self.expect_keyword(Keyword::AS)?;
                    if self.parse_keyword(Keyword::INPUTFORMAT) {
                        let input_format = self.parse_expr()?;
                        self.expect_keyword(Keyword::OUTPUTFORMAT)?;
                        let output_format = self.parse_expr()?;
                        hive_format.storage = Some(HiveIOFormat::IOF {
                            input_format,
                            output_format,
                        });
                    } else {
                        let format = self.parse_file_format()?;
                        hive_format.storage = Some(HiveIOFormat::FileFormat { format });
                    }
                }
                Some(Keyword::LOCATION) => {
                    hive_format.location = Some(self.parse_literal_string()?);
                }
                Some(Keyword::WITH) => {
                    self.prev_token();
                    let properties = self
                        .parse_options_with_keywords(&[Keyword::WITH, Keyword::SERDEPROPERTIES])?;
                    if !properties.is_empty() {
                        hive_format.serde_properties = Some(properties);
                    } else {
                        break;
                    }
                }
                None => break,
                _ => break,
            }
        }

        Ok(hive_format)
    }

    pub fn parse_row_format(&mut self) -> Result<HiveRowFormat, ParserError> {
        self.expect_keyword(Keyword::FORMAT)?;
        match self.parse_one_of_keywords(&[Keyword::SERDE, Keyword::DELIMITED]) {
            Some(Keyword::SERDE) => {
                let class = self.parse_literal_string()?;
                Ok(HiveRowFormat::SERDE { class })
            }
            _ => {
                let mut row_delimiters = vec![];

                loop {
                    match self.parse_one_of_keywords(&[
                        Keyword::FIELDS,
                        Keyword::COLLECTION,
                        Keyword::MAP,
                        Keyword::LINES,
                        Keyword::NULL,
                    ]) {
                        Some(Keyword::FIELDS) => {
                            if self.parse_keywords(&[Keyword::TERMINATED, Keyword::BY]) {
                                row_delimiters.push(HiveRowDelimiter {
                                    delimiter: HiveDelimiter::FieldsTerminatedBy,
                                    char: self.parse_identifier(false)?,
                                });

                                if self.parse_keywords(&[Keyword::ESCAPED, Keyword::BY]) {
                                    row_delimiters.push(HiveRowDelimiter {
                                        delimiter: HiveDelimiter::FieldsEscapedBy,
                                        char: self.parse_identifier(false)?,
                                    });
                                }
                            } else {
                                break;
                            }
                        }
                        Some(Keyword::COLLECTION) => {
                            if self.parse_keywords(&[
                                Keyword::ITEMS,
                                Keyword::TERMINATED,
                                Keyword::BY,
                            ]) {
                                row_delimiters.push(HiveRowDelimiter {
                                    delimiter: HiveDelimiter::CollectionItemsTerminatedBy,
                                    char: self.parse_identifier(false)?,
                                });
                            } else {
                                break;
                            }
                        }
                        Some(Keyword::MAP) => {
                            if self.parse_keywords(&[
                                Keyword::KEYS,
                                Keyword::TERMINATED,
                                Keyword::BY,
                            ]) {
                                row_delimiters.push(HiveRowDelimiter {
                                    delimiter: HiveDelimiter::MapKeysTerminatedBy,
                                    char: self.parse_identifier(false)?,
                                });
                            } else {
                                break;
                            }
                        }
                        Some(Keyword::LINES) => {
                            if self.parse_keywords(&[Keyword::TERMINATED, Keyword::BY]) {
                                row_delimiters.push(HiveRowDelimiter {
                                    delimiter: HiveDelimiter::LinesTerminatedBy,
                                    char: self.parse_identifier(false)?,
                                });
                            } else {
                                break;
                            }
                        }
                        Some(Keyword::NULL) => {
                            if self.parse_keywords(&[Keyword::DEFINED, Keyword::AS]) {
                                row_delimiters.push(HiveRowDelimiter {
                                    delimiter: HiveDelimiter::NullDefinedAs,
                                    char: self.parse_identifier(false)?,
                                });
                            } else {
                                break;
                            }
                        }
                        _ => {
                            break;
                        }
                    }
                }

                Ok(HiveRowFormat::DELIMITED {
                    delimiters: row_delimiters,
                })
            }
        }
    }

    //TODO: Implement parsing for Skewed
    pub fn parse_hive_distribution(&mut self) -> Result<HiveDistributionStyle, ParserError> {
        if self.parse_keywords(&[Keyword::PARTITIONED, Keyword::BY]) {
            self.expect_token(&Token::LParen)?;
            let columns = self.parse_comma_separated(Parser::parse_column_def)?;
            self.expect_token(&Token::RParen)?;
            Ok(HiveDistributionStyle::PARTITIONED { columns })
        } else {
            Ok(HiveDistributionStyle::NONE)
        }
    }
}

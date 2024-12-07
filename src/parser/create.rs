use super::*;

use crate::parser_err;

impl<'a> Parser<'a> {
    /// Parse a SQL CREATE statement
    pub fn parse_create(&mut self) -> Result<Statement, ParserError> {
        let or_replace = self.parse_keywords(&[Keyword::OR, Keyword::REPLACE]);
        let or_alter = self.parse_keywords(&[Keyword::OR, Keyword::ALTER]);
        let local = self.parse_one_of_keywords(&[Keyword::LOCAL]).is_some();
        let global = self.parse_one_of_keywords(&[Keyword::GLOBAL]).is_some();
        let transient = self.parse_one_of_keywords(&[Keyword::TRANSIENT]).is_some();
        let global: Option<bool> = if global {
            Some(true)
        } else if local {
            Some(false)
        } else {
            None
        };
        let temporary = self
            .parse_one_of_keywords(&[Keyword::TEMP, Keyword::TEMPORARY])
            .is_some();
        let persistent = dialect_of!(self is DuckDbDialect)
            && self.parse_one_of_keywords(&[Keyword::PERSISTENT]).is_some();
        if self.parse_keyword(Keyword::TABLE) {
            self.parse_create_table(or_replace, temporary, global, transient)
        } else if self.parse_keyword(Keyword::MATERIALIZED) || self.parse_keyword(Keyword::VIEW) {
            self.prev_token();
            self.parse_create_view(or_replace, temporary)
        } else if self.parse_keyword(Keyword::POLICY) {
            self.parse_create_policy()
        } else if self.parse_keyword(Keyword::EXTERNAL) {
            self.parse_create_external_table(or_replace)
        } else if self.parse_keyword(Keyword::FUNCTION) {
            self.parse_create_function(or_replace, temporary)
        } else if self.parse_keyword(Keyword::TRIGGER) {
            self.parse_create_trigger(or_replace, false)
        } else if self.parse_keywords(&[Keyword::CONSTRAINT, Keyword::TRIGGER]) {
            self.parse_create_trigger(or_replace, true)
        } else if self.parse_keyword(Keyword::MACRO) {
            self.parse_create_macro(or_replace, temporary)
        } else if self.parse_keyword(Keyword::SECRET) {
            self.parse_create_secret(or_replace, temporary, persistent)
        } else if or_replace {
            self.expected(
                "[EXTERNAL] TABLE or [MATERIALIZED] VIEW or FUNCTION after CREATE OR REPLACE",
                self.peek_token(),
            )
        } else if self.parse_keyword(Keyword::EXTENSION) {
            self.parse_create_extension()
        } else if self.parse_keyword(Keyword::INDEX) {
            self.parse_create_index(false)
        } else if self.parse_keywords(&[Keyword::UNIQUE, Keyword::INDEX]) {
            self.parse_create_index(true)
        } else if self.parse_keyword(Keyword::VIRTUAL) {
            self.parse_create_virtual_table()
        } else if self.parse_keyword(Keyword::SCHEMA) {
            self.parse_create_schema()
        } else if self.parse_keyword(Keyword::DATABASE) {
            self.parse_create_database()
        } else if self.parse_keyword(Keyword::ROLE) {
            self.parse_create_role()
        } else if self.parse_keyword(Keyword::SEQUENCE) {
            self.parse_create_sequence(temporary)
        } else if self.parse_keyword(Keyword::TYPE) {
            self.parse_create_type()
        } else if self.parse_keyword(Keyword::PROCEDURE) {
            self.parse_create_procedure(or_alter)
        } else {
            self.expected("an object type after CREATE", self.peek_token())
        }
    }

    pub fn parse_create_procedure(&mut self, or_alter: bool) -> Result<Statement, ParserError> {
        let name = self.parse_object_name(false)?;
        let params = self.parse_optional_procedure_parameters()?;
        self.expect_keyword(Keyword::AS)?;
        self.expect_keyword(Keyword::BEGIN)?;
        let statements = self.parse_statements()?;
        self.expect_keyword(Keyword::END)?;
        Ok(Statement::CreateProcedure {
            name,
            or_alter,
            params,
            body: statements,
        })
    }

    /// ```sql
    /// CREATE [ { TEMPORARY | TEMP } ] SEQUENCE [ IF NOT EXISTS ] <sequence_name>
    /// ```
    ///
    /// See [Postgres docs](https://www.postgresql.org/docs/current/sql-createsequence.html) for more details.
    pub fn parse_create_sequence(&mut self, temporary: bool) -> Result<Statement, ParserError> {
        //[ IF NOT EXISTS ]
        let if_not_exists = self.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        //name
        let name = self.parse_object_name(false)?;
        //[ AS data_type ]
        let mut data_type: Option<DataType> = None;
        if self.parse_keywords(&[Keyword::AS]) {
            data_type = Some(self.parse_data_type()?)
        }
        let sequence_options = self.parse_create_sequence_options()?;
        // [ OWNED BY { table_name.column_name | NONE } ]
        let owned_by = if self.parse_keywords(&[Keyword::OWNED, Keyword::BY]) {
            if self.parse_keywords(&[Keyword::NONE]) {
                Some(ObjectName(vec![Ident::new("NONE")]))
            } else {
                Some(self.parse_object_name(false)?)
            }
        } else {
            None
        };
        Ok(Statement::CreateSequence {
            temporary,
            if_not_exists,
            name,
            data_type,
            sequence_options,
            owned_by,
        })
    }

    pub fn parse_create_schema(&mut self) -> Result<Statement, ParserError> {
        let if_not_exists = self.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);

        let schema_name = self.parse_schema_name()?;

        Ok(Statement::CreateSchema {
            schema_name,
            if_not_exists,
        })
    }

    pub fn parse_create_type(&mut self) -> Result<Statement, ParserError> {
        let name = self.parse_object_name(false)?;
        self.expect_keyword(Keyword::AS)?;

        let mut attributes = vec![];
        if !self.consume_token(&Token::LParen) || self.consume_token(&Token::RParen) {
            return Ok(Statement::CreateType {
                name,
                representation: UserDefinedTypeRepresentation::Composite { attributes },
            });
        }

        loop {
            let attr_name = self.parse_identifier(false)?;
            let attr_data_type = self.parse_data_type()?;
            let attr_collation = if self.parse_keyword(Keyword::COLLATE) {
                Some(self.parse_object_name(false)?)
            } else {
                None
            };
            attributes.push(UserDefinedTypeCompositeAttributeDef {
                name: attr_name,
                data_type: attr_data_type,
                collation: attr_collation,
            });
            let comma = self.consume_token(&Token::Comma);
            if self.consume_token(&Token::RParen) {
                // allow a trailing comma
                break;
            } else if !comma {
                return self.expected("',' or ')' after attribute definition", self.peek_token());
            }
        }

        Ok(Statement::CreateType {
            name,
            representation: UserDefinedTypeRepresentation::Composite { attributes },
        })
    }

    pub fn parse_index_options(&mut self) -> Result<Vec<IndexOption>, ParserError> {
        let mut options = Vec::new();

        loop {
            match self.parse_optional_index_option()? {
                Some(index_option) => options.push(index_option),
                None => return Ok(options),
            }
        }
    }

    pub fn parse_index_type(&mut self) -> Result<IndexType, ParserError> {
        if self.parse_keyword(Keyword::BTREE) {
            Ok(IndexType::BTree)
        } else if self.parse_keyword(Keyword::HASH) {
            Ok(IndexType::Hash)
        } else {
            self.expected("index type {BTREE | HASH}", self.peek_token())
        }
    }

    #[must_use]
    pub fn parse_index_type_display(&mut self) -> KeyOrIndexDisplay {
        if self.parse_keyword(Keyword::KEY) {
            KeyOrIndexDisplay::Key
        } else if self.parse_keyword(Keyword::INDEX) {
            KeyOrIndexDisplay::Index
        } else {
            KeyOrIndexDisplay::None
        }
    }

    pub fn parse_optional_index_option(&mut self) -> Result<Option<IndexOption>, ParserError> {
        if let Some(index_type) = self.parse_optional_using_then_index_type()? {
            Ok(Some(IndexOption::Using(index_type)))
        } else if self.parse_keyword(Keyword::COMMENT) {
            let s = self.parse_literal_string()?;
            Ok(Some(IndexOption::Comment(s)))
        } else {
            Ok(None)
        }
    }

    pub fn parse_optional_inline_comment(&mut self) -> Result<Option<CommentDef>, ParserError> {
        let comment = if self.parse_keyword(Keyword::COMMENT) {
            let has_eq = self.consume_token(&Token::Eq);
            let next_token = self.next_token();
            match next_token.token {
                Token::SingleQuotedString(str) => Some(if has_eq {
                    CommentDef::WithEq(str)
                } else {
                    CommentDef::WithoutEq(str)
                }),
                _ => self.expected("comment", next_token)?,
            }
        } else {
            None
        };
        Ok(comment)
    }

    pub fn parse_optional_procedure_parameters(
        &mut self,
    ) -> Result<Option<Vec<ProcedureParam>>, ParserError> {
        let mut params = vec![];
        if !self.consume_token(&Token::LParen) || self.consume_token(&Token::RParen) {
            return Ok(Some(params));
        }
        loop {
            if let Token::Word(_) = self.peek_token().token {
                params.push(self.parse_procedure_param()?)
            }
            let comma = self.consume_token(&Token::Comma);
            if self.consume_token(&Token::RParen) {
                // allow a trailing comma, even though it's not in standard
                break;
            } else if !comma {
                return self.expected("',' or ')' after parameter definition", self.peek_token());
            }
        }
        Ok(Some(params))
    }

    /// Parse [USING {BTREE | HASH}]
    pub fn parse_optional_using_then_index_type(
        &mut self,
    ) -> Result<Option<IndexType>, ParserError> {
        if self.parse_keyword(Keyword::USING) {
            Ok(Some(self.parse_index_type()?))
        } else {
            Ok(None)
        }
    }

    pub fn parse_procedure_param(&mut self) -> Result<ProcedureParam, ParserError> {
        let name = self.parse_identifier(false)?;
        let data_type = self.parse_data_type()?;
        Ok(ProcedureParam { name, data_type })
    }

    pub fn parse_schema_name(&mut self) -> Result<SchemaName, ParserError> {
        if self.parse_keyword(Keyword::AUTHORIZATION) {
            Ok(SchemaName::UnnamedAuthorization(
                self.parse_identifier(false)?,
            ))
        } else {
            let name = self.parse_object_name(false)?;

            if self.parse_keyword(Keyword::AUTHORIZATION) {
                Ok(SchemaName::NamedAuthorization(
                    name,
                    self.parse_identifier(false)?,
                ))
            } else {
                Ok(SchemaName::Simple(name))
            }
        }
    }

    pub fn parse_create_database(&mut self) -> Result<Statement, ParserError> {
        let ine = self.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let db_name = self.parse_object_name(false)?;
        let mut location = None;
        let mut managed_location = None;
        loop {
            match self.parse_one_of_keywords(&[Keyword::LOCATION, Keyword::MANAGEDLOCATION]) {
                Some(Keyword::LOCATION) => location = Some(self.parse_literal_string()?),
                Some(Keyword::MANAGEDLOCATION) => {
                    managed_location = Some(self.parse_literal_string()?)
                }
                _ => break,
            }
        }
        Ok(Statement::CreateDatabase {
            db_name,
            if_not_exists: ine,
            location,
            managed_location,
        })
    }

    pub fn parse_optional_create_function_using(
        &mut self,
    ) -> Result<Option<CreateFunctionUsing>, ParserError> {
        if !self.parse_keyword(Keyword::USING) {
            return Ok(None);
        };
        let keyword =
            self.expect_one_of_keywords(&[Keyword::JAR, Keyword::FILE, Keyword::ARCHIVE])?;

        let uri = self.parse_literal_string()?;

        match keyword {
            Keyword::JAR => Ok(Some(CreateFunctionUsing::Jar(uri))),
            Keyword::FILE => Ok(Some(CreateFunctionUsing::File(uri))),
            Keyword::ARCHIVE => Ok(Some(CreateFunctionUsing::Archive(uri))),
            _ => self.expected(
                "JAR, FILE or ARCHIVE, got {:?}",
                TokenWithSpan::wrap(Token::make_keyword(format!("{keyword:?}").as_str())),
            ),
        }
    }

    pub fn parse_create_function(
        &mut self,
        or_replace: bool,
        temporary: bool,
    ) -> Result<Statement, ParserError> {
        if dialect_of!(self is HiveDialect) {
            self.parse_hive_create_function(or_replace, temporary)
        } else if dialect_of!(self is PostgreSqlDialect | GenericDialect) {
            self.parse_postgres_create_function(or_replace, temporary)
        } else if dialect_of!(self is DuckDbDialect) {
            self.parse_create_macro(or_replace, temporary)
        } else if dialect_of!(self is BigQueryDialect) {
            self.parse_bigquery_create_function(or_replace, temporary)
        } else {
            self.prev_token();
            self.expected("an object type after CREATE", self.peek_token())
        }
    }

    pub fn parse_create_trigger(
        &mut self,
        or_replace: bool,
        is_constraint: bool,
    ) -> Result<Statement, ParserError> {
        if !dialect_of!(self is PostgreSqlDialect | GenericDialect) {
            self.prev_token();
            return self.expected("an object type after CREATE", self.peek_token());
        }

        let name = self.parse_object_name(false)?;
        let period = self.parse_trigger_period()?;

        let events = self.parse_keyword_separated(Keyword::OR, Parser::parse_trigger_event)?;
        self.expect_keyword(Keyword::ON)?;
        let table_name = self.parse_object_name(false)?;

        let referenced_table_name = if self.parse_keyword(Keyword::FROM) {
            self.parse_object_name(true).ok()
        } else {
            None
        };

        let characteristics = self.parse_constraint_characteristics()?;

        let mut referencing = vec![];
        if self.parse_keyword(Keyword::REFERENCING) {
            while let Some(refer) = self.parse_trigger_referencing()? {
                referencing.push(refer);
            }
        }

        self.expect_keyword(Keyword::FOR)?;
        let include_each = self.parse_keyword(Keyword::EACH);
        let trigger_object =
            match self.expect_one_of_keywords(&[Keyword::ROW, Keyword::STATEMENT])? {
                Keyword::ROW => TriggerObject::Row,
                Keyword::STATEMENT => TriggerObject::Statement,
                _ => unreachable!(),
            };

        let condition = self
            .parse_keyword(Keyword::WHEN)
            .then(|| self.parse_expr())
            .transpose()?;

        self.expect_keyword(Keyword::EXECUTE)?;

        let exec_body = self.parse_trigger_exec_body()?;

        Ok(Statement::CreateTrigger {
            or_replace,
            is_constraint,
            name,
            period,
            events,
            table_name,
            referenced_table_name,
            referencing,
            trigger_object,
            include_each,
            condition,
            exec_body,
            characteristics,
        })
    }

    pub fn parse_trigger_period(&mut self) -> Result<TriggerPeriod, ParserError> {
        Ok(
            match self.expect_one_of_keywords(&[
                Keyword::BEFORE,
                Keyword::AFTER,
                Keyword::INSTEAD,
            ])? {
                Keyword::BEFORE => TriggerPeriod::Before,
                Keyword::AFTER => TriggerPeriod::After,
                Keyword::INSTEAD => self
                    .expect_keyword(Keyword::OF)
                    .map(|_| TriggerPeriod::InsteadOf)?,
                _ => unreachable!(),
            },
        )
    }

    pub fn parse_trigger_event(&mut self) -> Result<TriggerEvent, ParserError> {
        Ok(
            match self.expect_one_of_keywords(&[
                Keyword::INSERT,
                Keyword::UPDATE,
                Keyword::DELETE,
                Keyword::TRUNCATE,
            ])? {
                Keyword::INSERT => TriggerEvent::Insert,
                Keyword::UPDATE => {
                    if self.parse_keyword(Keyword::OF) {
                        let cols = self.parse_comma_separated(|ident| {
                            Parser::parse_identifier(ident, false)
                        })?;
                        TriggerEvent::Update(cols)
                    } else {
                        TriggerEvent::Update(vec![])
                    }
                }
                Keyword::DELETE => TriggerEvent::Delete,
                Keyword::TRUNCATE => TriggerEvent::Truncate,
                _ => unreachable!(),
            },
        )
    }

    pub fn parse_trigger_referencing(&mut self) -> Result<Option<TriggerReferencing>, ParserError> {
        let refer_type = match self.parse_one_of_keywords(&[Keyword::OLD, Keyword::NEW]) {
            Some(Keyword::OLD) if self.parse_keyword(Keyword::TABLE) => {
                TriggerReferencingType::OldTable
            }
            Some(Keyword::NEW) if self.parse_keyword(Keyword::TABLE) => {
                TriggerReferencingType::NewTable
            }
            _ => {
                return Ok(None);
            }
        };

        let is_as = self.parse_keyword(Keyword::AS);
        let transition_relation_name = self.parse_object_name(false)?;
        Ok(Some(TriggerReferencing {
            refer_type,
            is_as,
            transition_relation_name,
        }))
    }

    pub fn parse_trigger_exec_body(&mut self) -> Result<TriggerExecBody, ParserError> {
        Ok(TriggerExecBody {
            exec_type: match self
                .expect_one_of_keywords(&[Keyword::FUNCTION, Keyword::PROCEDURE])?
            {
                Keyword::FUNCTION => TriggerExecBodyType::Function,
                Keyword::PROCEDURE => TriggerExecBodyType::Procedure,
                _ => unreachable!(),
            },
            func_desc: self.parse_function_desc()?,
        })
    }

    pub fn parse_create_macro(
        &mut self,
        or_replace: bool,
        temporary: bool,
    ) -> Result<Statement, ParserError> {
        if dialect_of!(self is DuckDbDialect |  GenericDialect) {
            let name = self.parse_object_name(false)?;
            self.expect_token(&Token::LParen)?;
            let args = if self.consume_token(&Token::RParen) {
                self.prev_token();
                None
            } else {
                Some(self.parse_comma_separated(Parser::parse_macro_arg)?)
            };

            self.expect_token(&Token::RParen)?;
            self.expect_keyword(Keyword::AS)?;

            Ok(Statement::CreateMacro {
                or_replace,
                temporary,
                name,
                args,
                definition: if self.parse_keyword(Keyword::TABLE) {
                    MacroDefinition::Table(self.parse_query()?)
                } else {
                    MacroDefinition::Expr(self.parse_expr()?)
                },
            })
        } else {
            self.prev_token();
            self.expected("an object type after CREATE", self.peek_token())
        }
    }

    fn parse_macro_arg(&mut self) -> Result<MacroArg, ParserError> {
        let name = self.parse_identifier(false)?;

        let default_expr =
            if self.consume_token(&Token::Assignment) || self.consume_token(&Token::RArrow) {
                Some(self.parse_expr()?)
            } else {
                None
            };
        Ok(MacroArg { name, default_expr })
    }

    pub(crate) fn parse_create_sequence_options(
        &mut self,
    ) -> Result<Vec<SequenceOptions>, ParserError> {
        let mut sequence_options = vec![];
        //[ INCREMENT [ BY ] increment ]
        if self.parse_keywords(&[Keyword::INCREMENT]) {
            if self.parse_keywords(&[Keyword::BY]) {
                sequence_options.push(SequenceOptions::IncrementBy(self.parse_number()?, true));
            } else {
                sequence_options.push(SequenceOptions::IncrementBy(self.parse_number()?, false));
            }
        }
        //[ MINVALUE minvalue | NO MINVALUE ]
        if self.parse_keyword(Keyword::MINVALUE) {
            sequence_options.push(SequenceOptions::MinValue(Some(self.parse_number()?)));
        } else if self.parse_keywords(&[Keyword::NO, Keyword::MINVALUE]) {
            sequence_options.push(SequenceOptions::MinValue(None));
        }
        //[ MAXVALUE maxvalue | NO MAXVALUE ]
        if self.parse_keywords(&[Keyword::MAXVALUE]) {
            sequence_options.push(SequenceOptions::MaxValue(Some(self.parse_number()?)));
        } else if self.parse_keywords(&[Keyword::NO, Keyword::MAXVALUE]) {
            sequence_options.push(SequenceOptions::MaxValue(None));
        }

        //[ START [ WITH ] start ]
        if self.parse_keywords(&[Keyword::START]) {
            if self.parse_keywords(&[Keyword::WITH]) {
                sequence_options.push(SequenceOptions::StartWith(self.parse_number()?, true));
            } else {
                sequence_options.push(SequenceOptions::StartWith(self.parse_number()?, false));
            }
        }
        //[ CACHE cache ]
        if self.parse_keywords(&[Keyword::CACHE]) {
            sequence_options.push(SequenceOptions::Cache(self.parse_number()?));
        }
        // [ [ NO ] CYCLE ]
        if self.parse_keywords(&[Keyword::NO, Keyword::CYCLE]) {
            sequence_options.push(SequenceOptions::Cycle(true));
        } else if self.parse_keywords(&[Keyword::CYCLE]) {
            sequence_options.push(SequenceOptions::Cycle(false));
        }

        Ok(sequence_options)
    }

    pub fn parse_create_external_table(
        &mut self,
        or_replace: bool,
    ) -> Result<Statement, ParserError> {
        self.expect_keyword(Keyword::TABLE)?;
        let if_not_exists = self.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let table_name = self.parse_object_name(false)?;
        let (columns, constraints) = self.parse_columns()?;

        let hive_distribution = self.parse_hive_distribution()?;
        let hive_formats = self.parse_hive_formats()?;

        let file_format = if let Some(ff) = &hive_formats.storage {
            match ff {
                HiveIOFormat::FileFormat { format } => Some(*format),
                _ => None,
            }
        } else {
            None
        };
        let location = hive_formats.location.clone();
        let table_properties = self.parse_options(Keyword::TBLPROPERTIES)?;
        Ok(CreateTableBuilder::new(table_name)
            .columns(columns)
            .constraints(constraints)
            .hive_distribution(hive_distribution)
            .hive_formats(Some(hive_formats))
            .table_properties(table_properties)
            .or_replace(or_replace)
            .if_not_exists(if_not_exists)
            .external(true)
            .file_format(file_format)
            .location(location)
            .build())
    }

    pub fn parse_file_format(&mut self) -> Result<FileFormat, ParserError> {
        let next_token = self.next_token();
        match &next_token.token {
            Token::Word(w) => match w.keyword {
                Keyword::AVRO => Ok(FileFormat::AVRO),
                Keyword::JSONFILE => Ok(FileFormat::JSONFILE),
                Keyword::ORC => Ok(FileFormat::ORC),
                Keyword::PARQUET => Ok(FileFormat::PARQUET),
                Keyword::RCFILE => Ok(FileFormat::RCFILE),
                Keyword::SEQUENCEFILE => Ok(FileFormat::SEQUENCEFILE),
                Keyword::TEXTFILE => Ok(FileFormat::TEXTFILE),
                _ => self.expected("fileformat", next_token),
            },
            _ => self.expected("fileformat", next_token),
        }
    }

    pub fn parse_analyze_format(&mut self) -> Result<AnalyzeFormat, ParserError> {
        let next_token = self.next_token();
        match &next_token.token {
            Token::Word(w) => match w.keyword {
                Keyword::TEXT => Ok(AnalyzeFormat::TEXT),
                Keyword::GRAPHVIZ => Ok(AnalyzeFormat::GRAPHVIZ),
                Keyword::JSON => Ok(AnalyzeFormat::JSON),
                _ => self.expected("fileformat", next_token),
            },
            _ => self.expected("fileformat", next_token),
        }
    }

    pub fn parse_create_view(
        &mut self,
        or_replace: bool,
        temporary: bool,
    ) -> Result<Statement, ParserError> {
        let materialized = self.parse_keyword(Keyword::MATERIALIZED);
        self.expect_keyword(Keyword::VIEW)?;
        let if_not_exists = dialect_of!(self is BigQueryDialect|SQLiteDialect|GenericDialect)
            && self.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        // Many dialects support `OR ALTER` right after `CREATE`, but we don't (yet).
        // ANSI SQL and Postgres support RECURSIVE here, but we don't support it either.
        let allow_unquoted_hyphen = dialect_of!(self is BigQueryDialect);
        let name = self.parse_object_name(allow_unquoted_hyphen)?;
        let columns = self.parse_view_columns()?;
        let mut options = CreateTableOptions::None;
        let with_options = self.parse_options(Keyword::WITH)?;
        if !with_options.is_empty() {
            options = CreateTableOptions::With(with_options);
        }

        let cluster_by = if self.parse_keyword(Keyword::CLUSTER) {
            self.expect_keyword(Keyword::BY)?;
            self.parse_parenthesized_column_list(Optional, false)?
        } else {
            vec![]
        };

        if dialect_of!(self is BigQueryDialect | GenericDialect) {
            if let Some(opts) = self.maybe_parse_options(Keyword::OPTIONS)? {
                if !opts.is_empty() {
                    options = CreateTableOptions::Options(opts);
                }
            };
        }

        let to = if dialect_of!(self is ClickHouseDialect | GenericDialect)
            && self.parse_keyword(Keyword::TO)
        {
            Some(self.parse_object_name(false)?)
        } else {
            None
        };

        let comment = if dialect_of!(self is SnowflakeDialect | GenericDialect)
            && self.parse_keyword(Keyword::COMMENT)
        {
            self.expect_token(&Token::Eq)?;
            let next_token = self.next_token();
            match next_token.token {
                Token::SingleQuotedString(str) => Some(str),
                _ => self.expected("string literal", next_token)?,
            }
        } else {
            None
        };

        self.expect_keyword(Keyword::AS)?;
        let query = self.parse_query()?;
        // Optional `WITH [ CASCADED | LOCAL ] CHECK OPTION` is widely supported here.

        let with_no_schema_binding = dialect_of!(self is RedshiftSqlDialect | GenericDialect)
            && self.parse_keywords(&[
                Keyword::WITH,
                Keyword::NO,
                Keyword::SCHEMA,
                Keyword::BINDING,
            ]);

        Ok(Statement::CreateView {
            name,
            columns,
            query,
            materialized,
            or_replace,
            options,
            cluster_by,
            comment,
            with_no_schema_binding,
            if_not_exists,
            temporary,
            to,
        })
    }

    pub fn parse_create_role(&mut self) -> Result<Statement, ParserError> {
        let if_not_exists = self.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let names = self.parse_comma_separated(|p| p.parse_object_name(false))?;

        let _ = self.parse_keyword(Keyword::WITH); // [ WITH ]

        let optional_keywords = if dialect_of!(self is MsSqlDialect) {
            vec![Keyword::AUTHORIZATION]
        } else if dialect_of!(self is PostgreSqlDialect) {
            vec![
                Keyword::LOGIN,
                Keyword::NOLOGIN,
                Keyword::INHERIT,
                Keyword::NOINHERIT,
                Keyword::BYPASSRLS,
                Keyword::NOBYPASSRLS,
                Keyword::PASSWORD,
                Keyword::CREATEDB,
                Keyword::NOCREATEDB,
                Keyword::CREATEROLE,
                Keyword::NOCREATEROLE,
                Keyword::SUPERUSER,
                Keyword::NOSUPERUSER,
                Keyword::REPLICATION,
                Keyword::NOREPLICATION,
                Keyword::CONNECTION,
                Keyword::VALID,
                Keyword::IN,
                Keyword::ROLE,
                Keyword::ADMIN,
                Keyword::USER,
            ]
        } else {
            vec![]
        };

        // MSSQL
        let mut authorization_owner = None;
        // Postgres
        let mut login = None;
        let mut inherit = None;
        let mut bypassrls = None;
        let mut password = None;
        let mut create_db = None;
        let mut create_role = None;
        let mut superuser = None;
        let mut replication = None;
        let mut connection_limit = None;
        let mut valid_until = None;
        let mut in_role = vec![];
        let mut in_group = vec![];
        let mut role = vec![];
        let mut user = vec![];
        let mut admin = vec![];

        while let Some(keyword) = self.parse_one_of_keywords(&optional_keywords) {
            let loc = self
                .tokens
                .get(self.index - 1)
                .map_or(Location { line: 0, column: 0 }, |t| t.span.start);
            match keyword {
                Keyword::AUTHORIZATION => {
                    if authorization_owner.is_some() {
                        parser_err!("Found multiple AUTHORIZATION", loc)
                    } else {
                        authorization_owner = Some(self.parse_object_name(false)?);
                        Ok(())
                    }
                }
                Keyword::LOGIN | Keyword::NOLOGIN => {
                    if login.is_some() {
                        parser_err!("Found multiple LOGIN or NOLOGIN", loc)
                    } else {
                        login = Some(keyword == Keyword::LOGIN);
                        Ok(())
                    }
                }
                Keyword::INHERIT | Keyword::NOINHERIT => {
                    if inherit.is_some() {
                        parser_err!("Found multiple INHERIT or NOINHERIT", loc)
                    } else {
                        inherit = Some(keyword == Keyword::INHERIT);
                        Ok(())
                    }
                }
                Keyword::BYPASSRLS | Keyword::NOBYPASSRLS => {
                    if bypassrls.is_some() {
                        parser_err!("Found multiple BYPASSRLS or NOBYPASSRLS", loc)
                    } else {
                        bypassrls = Some(keyword == Keyword::BYPASSRLS);
                        Ok(())
                    }
                }
                Keyword::CREATEDB | Keyword::NOCREATEDB => {
                    if create_db.is_some() {
                        parser_err!("Found multiple CREATEDB or NOCREATEDB", loc)
                    } else {
                        create_db = Some(keyword == Keyword::CREATEDB);
                        Ok(())
                    }
                }
                Keyword::CREATEROLE | Keyword::NOCREATEROLE => {
                    if create_role.is_some() {
                        parser_err!("Found multiple CREATEROLE or NOCREATEROLE", loc)
                    } else {
                        create_role = Some(keyword == Keyword::CREATEROLE);
                        Ok(())
                    }
                }
                Keyword::SUPERUSER | Keyword::NOSUPERUSER => {
                    if superuser.is_some() {
                        parser_err!("Found multiple SUPERUSER or NOSUPERUSER", loc)
                    } else {
                        superuser = Some(keyword == Keyword::SUPERUSER);
                        Ok(())
                    }
                }
                Keyword::REPLICATION | Keyword::NOREPLICATION => {
                    if replication.is_some() {
                        parser_err!("Found multiple REPLICATION or NOREPLICATION", loc)
                    } else {
                        replication = Some(keyword == Keyword::REPLICATION);
                        Ok(())
                    }
                }
                Keyword::PASSWORD => {
                    if password.is_some() {
                        parser_err!("Found multiple PASSWORD", loc)
                    } else {
                        password = if self.parse_keyword(Keyword::NULL) {
                            Some(Password::NullPassword)
                        } else {
                            Some(Password::Password(Expr::Value(self.parse_value()?)))
                        };
                        Ok(())
                    }
                }
                Keyword::CONNECTION => {
                    self.expect_keyword(Keyword::LIMIT)?;
                    if connection_limit.is_some() {
                        parser_err!("Found multiple CONNECTION LIMIT", loc)
                    } else {
                        connection_limit = Some(Expr::Value(self.parse_number_value()?));
                        Ok(())
                    }
                }
                Keyword::VALID => {
                    self.expect_keyword(Keyword::UNTIL)?;
                    if valid_until.is_some() {
                        parser_err!("Found multiple VALID UNTIL", loc)
                    } else {
                        valid_until = Some(Expr::Value(self.parse_value()?));
                        Ok(())
                    }
                }
                Keyword::IN => {
                    if self.parse_keyword(Keyword::ROLE) {
                        if !in_role.is_empty() {
                            parser_err!("Found multiple IN ROLE", loc)
                        } else {
                            in_role = self.parse_comma_separated(|p| p.parse_identifier(false))?;
                            Ok(())
                        }
                    } else if self.parse_keyword(Keyword::GROUP) {
                        if !in_group.is_empty() {
                            parser_err!("Found multiple IN GROUP", loc)
                        } else {
                            in_group = self.parse_comma_separated(|p| p.parse_identifier(false))?;
                            Ok(())
                        }
                    } else {
                        self.expected("ROLE or GROUP after IN", self.peek_token())
                    }
                }
                Keyword::ROLE => {
                    if !role.is_empty() {
                        parser_err!("Found multiple ROLE", loc)
                    } else {
                        role = self.parse_comma_separated(|p| p.parse_identifier(false))?;
                        Ok(())
                    }
                }
                Keyword::USER => {
                    if !user.is_empty() {
                        parser_err!("Found multiple USER", loc)
                    } else {
                        user = self.parse_comma_separated(|p| p.parse_identifier(false))?;
                        Ok(())
                    }
                }
                Keyword::ADMIN => {
                    if !admin.is_empty() {
                        parser_err!("Found multiple ADMIN", loc)
                    } else {
                        admin = self.parse_comma_separated(|p| p.parse_identifier(false))?;
                        Ok(())
                    }
                }
                _ => break,
            }?
        }

        Ok(Statement::CreateRole {
            names,
            if_not_exists,
            login,
            inherit,
            bypassrls,
            password,
            create_db,
            create_role,
            replication,
            superuser,
            connection_limit,
            valid_until,
            in_role,
            in_group,
            role,
            user,
            admin,
            authorization_owner,
        })
    }

    pub fn parse_owner(&mut self) -> Result<Owner, ParserError> {
        let owner = match self.parse_one_of_keywords(&[Keyword::CURRENT_USER, Keyword::CURRENT_ROLE, Keyword::SESSION_USER]) {
            Some(Keyword::CURRENT_USER) => Owner::CurrentUser,
            Some(Keyword::CURRENT_ROLE) => Owner::CurrentRole,
            Some(Keyword::SESSION_USER) => Owner::SessionUser,
            Some(_) => unreachable!(),
            None => {
                match self.parse_identifier(false) {
                    Ok(ident) => Owner::Ident(ident),
                    Err(e) => {
                        return Err(ParserError::ParserError(format!("Expected: CURRENT_USER, CURRENT_ROLE, SESSION_USER or identifier after OWNER TO. {e}")))
                    }
                }
            }
        };
        Ok(owner)
    }

    /// ```sql
    ///     CREATE POLICY name ON table_name [ AS { PERMISSIVE | RESTRICTIVE } ]
    ///     [ FOR { ALL | SELECT | INSERT | UPDATE | DELETE } ]
    ///     [ TO { role_name | PUBLIC | CURRENT_USER | CURRENT_ROLE | SESSION_USER } [, ...] ]
    ///     [ USING ( using_expression ) ]
    ///     [ WITH CHECK ( with_check_expression ) ]
    /// ```
    ///
    /// [PostgreSQL Documentation](https://www.postgresql.org/docs/current/sql-createpolicy.html)
    pub fn parse_create_policy(&mut self) -> Result<Statement, ParserError> {
        let name = self.parse_identifier(false)?;
        self.expect_keyword(Keyword::ON)?;
        let table_name = self.parse_object_name(false)?;

        let policy_type = if self.parse_keyword(Keyword::AS) {
            let keyword =
                self.expect_one_of_keywords(&[Keyword::PERMISSIVE, Keyword::RESTRICTIVE])?;
            Some(match keyword {
                Keyword::PERMISSIVE => CreatePolicyType::Permissive,
                Keyword::RESTRICTIVE => CreatePolicyType::Restrictive,
                _ => unreachable!(),
            })
        } else {
            None
        };

        let command = if self.parse_keyword(Keyword::FOR) {
            let keyword = self.expect_one_of_keywords(&[
                Keyword::ALL,
                Keyword::SELECT,
                Keyword::INSERT,
                Keyword::UPDATE,
                Keyword::DELETE,
            ])?;
            Some(match keyword {
                Keyword::ALL => CreatePolicyCommand::All,
                Keyword::SELECT => CreatePolicyCommand::Select,
                Keyword::INSERT => CreatePolicyCommand::Insert,
                Keyword::UPDATE => CreatePolicyCommand::Update,
                Keyword::DELETE => CreatePolicyCommand::Delete,
                _ => unreachable!(),
            })
        } else {
            None
        };

        let to = if self.parse_keyword(Keyword::TO) {
            Some(self.parse_comma_separated(|p| p.parse_owner())?)
        } else {
            None
        };

        let using = if self.parse_keyword(Keyword::USING) {
            self.expect_token(&Token::LParen)?;
            let expr = self.parse_expr()?;
            self.expect_token(&Token::RParen)?;
            Some(expr)
        } else {
            None
        };

        let with_check = if self.parse_keywords(&[Keyword::WITH, Keyword::CHECK]) {
            self.expect_token(&Token::LParen)?;
            let expr = self.parse_expr()?;
            self.expect_token(&Token::RParen)?;
            Some(expr)
        } else {
            None
        };

        Ok(CreatePolicy {
            name,
            table_name,
            policy_type,
            command,
            to,
            using,
            with_check,
        })
    }

    pub fn parse_create_index(&mut self, unique: bool) -> Result<Statement, ParserError> {
        let concurrently = self.parse_keyword(Keyword::CONCURRENTLY);
        let if_not_exists = self.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let index_name = if if_not_exists || !self.parse_keyword(Keyword::ON) {
            let index_name = self.parse_object_name(false)?;
            self.expect_keyword(Keyword::ON)?;
            Some(index_name)
        } else {
            None
        };
        let table_name = self.parse_object_name(false)?;
        let using = if self.parse_keyword(Keyword::USING) {
            Some(self.parse_identifier(false)?)
        } else {
            None
        };
        self.expect_token(&Token::LParen)?;
        let columns = self.parse_comma_separated(Parser::parse_order_by_expr)?;
        self.expect_token(&Token::RParen)?;

        let include = if self.parse_keyword(Keyword::INCLUDE) {
            self.expect_token(&Token::LParen)?;
            let columns = self.parse_comma_separated(|p| p.parse_identifier(false))?;
            self.expect_token(&Token::RParen)?;
            columns
        } else {
            vec![]
        };

        let nulls_distinct = if self.parse_keyword(Keyword::NULLS) {
            let not = self.parse_keyword(Keyword::NOT);
            self.expect_keyword(Keyword::DISTINCT)?;
            Some(!not)
        } else {
            None
        };

        let with = if self.dialect.supports_create_index_with_clause()
            && self.parse_keyword(Keyword::WITH)
        {
            self.expect_token(&Token::LParen)?;
            let with_params = self.parse_comma_separated(Parser::parse_expr)?;
            self.expect_token(&Token::RParen)?;
            with_params
        } else {
            Vec::new()
        };

        let predicate = if self.parse_keyword(Keyword::WHERE) {
            Some(self.parse_expr()?)
        } else {
            None
        };

        Ok(Statement::CreateIndex(CreateIndex {
            name: index_name,
            table_name,
            using,
            columns,
            unique,
            concurrently,
            if_not_exists,
            include,
            nulls_distinct,
            with,
            predicate,
        }))
    }

    pub fn parse_create_extension(&mut self) -> Result<Statement, ParserError> {
        let if_not_exists = self.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let name = self.parse_identifier(false)?;

        let (schema, version, cascade) = if self.parse_keyword(Keyword::WITH) {
            let schema = if self.parse_keyword(Keyword::SCHEMA) {
                Some(self.parse_identifier(false)?)
            } else {
                None
            };

            let version = if self.parse_keyword(Keyword::VERSION) {
                Some(self.parse_identifier(false)?)
            } else {
                None
            };

            let cascade = self.parse_keyword(Keyword::CASCADE);

            (schema, version, cascade)
        } else {
            (None, None, false)
        };

        Ok(Statement::CreateExtension {
            name,
            if_not_exists,
            schema,
            version,
            cascade,
        })
    }

    pub fn parse_create_table(
        &mut self,
        or_replace: bool,
        temporary: bool,
        global: Option<bool>,
        transient: bool,
    ) -> Result<Statement, ParserError> {
        let allow_unquoted_hyphen = dialect_of!(self is BigQueryDialect);
        let if_not_exists = self.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let table_name = self.parse_object_name(allow_unquoted_hyphen)?;

        // Clickhouse has `ON CLUSTER 'cluster'` syntax for DDLs
        let on_cluster = self.parse_optional_on_cluster()?;

        let like = if self.parse_keyword(Keyword::LIKE) || self.parse_keyword(Keyword::ILIKE) {
            self.parse_object_name(allow_unquoted_hyphen).ok()
        } else {
            None
        };

        let clone = if self.parse_keyword(Keyword::CLONE) {
            self.parse_object_name(allow_unquoted_hyphen).ok()
        } else {
            None
        };

        // parse optional column list (schema)
        let (columns, constraints) = self.parse_columns()?;
        let mut comment = if dialect_of!(self is HiveDialect)
            && self.parse_keyword(Keyword::COMMENT)
        {
            let next_token = self.next_token();
            match next_token.token {
                Token::SingleQuotedString(str) => Some(CommentDef::AfterColumnDefsWithoutEq(str)),
                _ => self.expected("comment", next_token)?,
            }
        } else {
            None
        };

        // SQLite supports `WITHOUT ROWID` at the end of `CREATE TABLE`
        let without_rowid = self.parse_keywords(&[Keyword::WITHOUT, Keyword::ROWID]);

        let hive_distribution = self.parse_hive_distribution()?;
        let clustered_by = self.parse_optional_clustered_by()?;
        let hive_formats = self.parse_hive_formats()?;
        // PostgreSQL supports `WITH ( options )`, before `AS`
        let with_options = self.parse_options(Keyword::WITH)?;
        let table_properties = self.parse_options(Keyword::TBLPROPERTIES)?;

        let engine = if self.parse_keyword(Keyword::ENGINE) {
            self.expect_token(&Token::Eq)?;
            let next_token = self.next_token();
            match next_token.token {
                Token::Word(w) => {
                    let name = w.value;
                    let parameters = if self.peek_token() == Token::LParen {
                        Some(self.parse_parenthesized_identifiers()?)
                    } else {
                        None
                    };
                    Some(TableEngine { name, parameters })
                }
                _ => self.expected("identifier", next_token)?,
            }
        } else {
            None
        };

        let auto_increment_offset = if self.parse_keyword(Keyword::AUTO_INCREMENT) {
            let _ = self.consume_token(&Token::Eq);
            let next_token = self.next_token();
            match next_token.token {
                Token::Number(s, _) => Some(Self::parse::<u32>(s, next_token.span.start)?),
                _ => self.expected("literal int", next_token)?,
            }
        } else {
            None
        };

        // ClickHouse supports `PRIMARY KEY`, before `ORDER BY`
        // https://clickhouse.com/docs/en/sql-reference/statements/create/table#primary-key
        let primary_key = if dialect_of!(self is ClickHouseDialect | GenericDialect)
            && self.parse_keywords(&[Keyword::PRIMARY, Keyword::KEY])
        {
            Some(Box::new(self.parse_expr()?))
        } else {
            None
        };

        let order_by = if self.parse_keywords(&[Keyword::ORDER, Keyword::BY]) {
            if self.consume_token(&Token::LParen) {
                let columns = if self.peek_token() != Token::RParen {
                    self.parse_comma_separated(|p| p.parse_expr())?
                } else {
                    vec![]
                };
                self.expect_token(&Token::RParen)?;
                Some(OneOrManyWithParens::Many(columns))
            } else {
                Some(OneOrManyWithParens::One(self.parse_expr()?))
            }
        } else {
            None
        };

        let create_table_config = self.parse_optional_create_table_config()?;

        let default_charset = if self.parse_keywords(&[Keyword::DEFAULT, Keyword::CHARSET]) {
            self.expect_token(&Token::Eq)?;
            let next_token = self.next_token();
            match next_token.token {
                Token::Word(w) => Some(w.value),
                _ => self.expected("identifier", next_token)?,
            }
        } else {
            None
        };

        let collation = if self.parse_keywords(&[Keyword::COLLATE]) {
            self.expect_token(&Token::Eq)?;
            let next_token = self.next_token();
            match next_token.token {
                Token::Word(w) => Some(w.value),
                _ => self.expected("identifier", next_token)?,
            }
        } else {
            None
        };

        let on_commit: Option<OnCommit> =
            if self.parse_keywords(&[Keyword::ON, Keyword::COMMIT, Keyword::DELETE, Keyword::ROWS])
            {
                Some(OnCommit::DeleteRows)
            } else if self.parse_keywords(&[
                Keyword::ON,
                Keyword::COMMIT,
                Keyword::PRESERVE,
                Keyword::ROWS,
            ]) {
                Some(OnCommit::PreserveRows)
            } else if self.parse_keywords(&[Keyword::ON, Keyword::COMMIT, Keyword::DROP]) {
                Some(OnCommit::Drop)
            } else {
                None
            };

        let strict = self.parse_keyword(Keyword::STRICT);

        // Excludes Hive dialect here since it has been handled after table column definitions.
        if !dialect_of!(self is HiveDialect) && self.parse_keyword(Keyword::COMMENT) {
            // rewind the COMMENT keyword
            self.prev_token();
            comment = self.parse_optional_inline_comment()?
        };

        // Parse optional `AS ( query )`
        let query = if self.parse_keyword(Keyword::AS) {
            Some(self.parse_query()?)
        } else if self.dialect.supports_create_table_select() && self.parse_keyword(Keyword::SELECT)
        {
            // rewind the SELECT keyword
            self.prev_token();
            Some(self.parse_query()?)
        } else {
            None
        };

        Ok(CreateTableBuilder::new(table_name)
            .temporary(temporary)
            .columns(columns)
            .constraints(constraints)
            .with_options(with_options)
            .table_properties(table_properties)
            .or_replace(or_replace)
            .if_not_exists(if_not_exists)
            .transient(transient)
            .hive_distribution(hive_distribution)
            .hive_formats(Some(hive_formats))
            .global(global)
            .query(query)
            .without_rowid(without_rowid)
            .like(like)
            .clone_clause(clone)
            .engine(engine)
            .comment(comment)
            .auto_increment_offset(auto_increment_offset)
            .order_by(order_by)
            .default_charset(default_charset)
            .collation(collation)
            .on_commit(on_commit)
            .on_cluster(on_cluster)
            .clustered_by(clustered_by)
            .partition_by(create_table_config.partition_by)
            .cluster_by(create_table_config.cluster_by)
            .options(create_table_config.options)
            .primary_key(primary_key)
            .strict(strict)
            .build())
    }

    /// Parse configuration like partitioning, clustering information during the table creation.
    ///
    /// [BigQuery](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#syntax_2)
    /// [PostgreSQL](https://www.postgresql.org/docs/current/ddl-partitioning.html)
    fn parse_optional_create_table_config(
        &mut self,
    ) -> Result<CreateTableConfiguration, ParserError> {
        let partition_by = if dialect_of!(self is BigQueryDialect | PostgreSqlDialect | GenericDialect)
            && self.parse_keywords(&[Keyword::PARTITION, Keyword::BY])
        {
            Some(Box::new(self.parse_expr()?))
        } else {
            None
        };

        let mut cluster_by = None;
        let mut options = None;
        if dialect_of!(self is BigQueryDialect | GenericDialect) {
            if self.parse_keywords(&[Keyword::CLUSTER, Keyword::BY]) {
                cluster_by = Some(WrappedCollection::NoWrapping(
                    self.parse_comma_separated(|p| p.parse_identifier(false))?,
                ));
            };

            if let Token::Word(word) = self.peek_token().token {
                if word.keyword == Keyword::OPTIONS {
                    options = Some(self.parse_options(Keyword::OPTIONS)?);
                }
            };
        }

        Ok(CreateTableConfiguration {
            partition_by,
            cluster_by,
            options,
        })
    }

    pub fn parse_optional_clustered_by(&mut self) -> Result<Option<ClusteredBy>, ParserError> {
        let clustered_by = if dialect_of!(self is HiveDialect|GenericDialect)
            && self.parse_keywords(&[Keyword::CLUSTERED, Keyword::BY])
        {
            let columns = self.parse_parenthesized_column_list(Mandatory, false)?;

            let sorted_by = if self.parse_keywords(&[Keyword::SORTED, Keyword::BY]) {
                self.expect_token(&Token::LParen)?;
                let sorted_by_columns = self.parse_comma_separated(|p| p.parse_order_by_expr())?;
                self.expect_token(&Token::RParen)?;
                Some(sorted_by_columns)
            } else {
                None
            };

            self.expect_keyword(Keyword::INTO)?;
            let num_buckets = self.parse_number_value()?;
            self.expect_keyword(Keyword::BUCKETS)?;
            Some(ClusteredBy {
                columns,
                sorted_by,
                num_buckets,
            })
        } else {
            None
        };
        Ok(clustered_by)
    }

    pub fn parse_referential_action(&mut self) -> Result<ReferentialAction, ParserError> {
        if self.parse_keyword(Keyword::RESTRICT) {
            Ok(ReferentialAction::Restrict)
        } else if self.parse_keyword(Keyword::CASCADE) {
            Ok(ReferentialAction::Cascade)
        } else if self.parse_keywords(&[Keyword::SET, Keyword::NULL]) {
            Ok(ReferentialAction::SetNull)
        } else if self.parse_keywords(&[Keyword::NO, Keyword::ACTION]) {
            Ok(ReferentialAction::NoAction)
        } else if self.parse_keywords(&[Keyword::SET, Keyword::DEFAULT]) {
            Ok(ReferentialAction::SetDefault)
        } else {
            self.expected(
                "one of RESTRICT, CASCADE, SET NULL, NO ACTION or SET DEFAULT",
                self.peek_token(),
            )
        }
    }

    pub fn parse_constraint_characteristics(
        &mut self,
    ) -> Result<Option<ConstraintCharacteristics>, ParserError> {
        let mut cc = ConstraintCharacteristics::default();

        loop {
            if cc.deferrable.is_none() && self.parse_keywords(&[Keyword::NOT, Keyword::DEFERRABLE])
            {
                cc.deferrable = Some(false);
            } else if cc.deferrable.is_none() && self.parse_keyword(Keyword::DEFERRABLE) {
                cc.deferrable = Some(true);
            } else if cc.initially.is_none() && self.parse_keyword(Keyword::INITIALLY) {
                if self.parse_keyword(Keyword::DEFERRED) {
                    cc.initially = Some(DeferrableInitial::Deferred);
                } else if self.parse_keyword(Keyword::IMMEDIATE) {
                    cc.initially = Some(DeferrableInitial::Immediate);
                } else {
                    self.expected("one of DEFERRED or IMMEDIATE", self.peek_token())?;
                }
            } else if cc.enforced.is_none() && self.parse_keyword(Keyword::ENFORCED) {
                cc.enforced = Some(true);
            } else if cc.enforced.is_none()
                && self.parse_keywords(&[Keyword::NOT, Keyword::ENFORCED])
            {
                cc.enforced = Some(false);
            } else {
                break;
            }
        }

        if cc.deferrable.is_some() || cc.initially.is_some() || cc.enforced.is_some() {
            Ok(Some(cc))
        } else {
            Ok(None)
        }
    }

    pub fn parse_optional_table_constraint(
        &mut self,
    ) -> Result<Option<TableConstraint>, ParserError> {
        let name = if self.parse_keyword(Keyword::CONSTRAINT) {
            Some(self.parse_identifier(false)?)
        } else {
            None
        };

        let next_token = self.next_token();
        match next_token.token {
            Token::Word(w) if w.keyword == Keyword::UNIQUE => {
                let index_type_display = self.parse_index_type_display();
                if !dialect_of!(self is GenericDialect | MySqlDialect)
                    && !index_type_display.is_none()
                {
                    return self
                        .expected("`index_name` or `(column_name [, ...])`", self.peek_token());
                }

                let nulls_distinct = self.parse_optional_nulls_distinct()?;

                // optional index name
                let index_name = self.parse_optional_indent()?;
                let index_type = self.parse_optional_using_then_index_type()?;

                let columns = self.parse_parenthesized_column_list(Mandatory, false)?;
                let index_options = self.parse_index_options()?;
                let characteristics = self.parse_constraint_characteristics()?;
                Ok(Some(TableConstraint::Unique {
                    name,
                    index_name,
                    index_type_display,
                    index_type,
                    columns,
                    index_options,
                    characteristics,
                    nulls_distinct,
                }))
            }
            Token::Word(w) if w.keyword == Keyword::PRIMARY => {
                // after `PRIMARY` always stay `KEY`
                self.expect_keyword(Keyword::KEY)?;

                // optional index name
                let index_name = self.parse_optional_indent()?;
                let index_type = self.parse_optional_using_then_index_type()?;

                let columns = self.parse_parenthesized_column_list(Mandatory, false)?;
                let index_options = self.parse_index_options()?;
                let characteristics = self.parse_constraint_characteristics()?;
                Ok(Some(TableConstraint::PrimaryKey {
                    name,
                    index_name,
                    index_type,
                    columns,
                    index_options,
                    characteristics,
                }))
            }
            Token::Word(w) if w.keyword == Keyword::FOREIGN => {
                self.expect_keyword(Keyword::KEY)?;
                let columns = self.parse_parenthesized_column_list(Mandatory, false)?;
                self.expect_keyword(Keyword::REFERENCES)?;
                let foreign_table = self.parse_object_name(false)?;
                let referred_columns = self.parse_parenthesized_column_list(Mandatory, false)?;
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

                Ok(Some(TableConstraint::ForeignKey {
                    name,
                    columns,
                    foreign_table,
                    referred_columns,
                    on_delete,
                    on_update,
                    characteristics,
                }))
            }
            Token::Word(w) if w.keyword == Keyword::CHECK => {
                self.expect_token(&Token::LParen)?;
                let expr = Box::new(self.parse_expr()?);
                self.expect_token(&Token::RParen)?;
                Ok(Some(TableConstraint::Check { name, expr }))
            }
            Token::Word(w)
                if (w.keyword == Keyword::INDEX || w.keyword == Keyword::KEY)
                    && dialect_of!(self is GenericDialect | MySqlDialect)
                    && name.is_none() =>
            {
                let display_as_key = w.keyword == Keyword::KEY;

                let name = match self.peek_token().token {
                    Token::Word(word) if word.keyword == Keyword::USING => None,
                    _ => self.parse_optional_indent()?,
                };

                let index_type = self.parse_optional_using_then_index_type()?;
                let columns = self.parse_parenthesized_column_list(Mandatory, false)?;

                Ok(Some(TableConstraint::Index {
                    display_as_key,
                    name,
                    index_type,
                    columns,
                }))
            }
            Token::Word(w)
                if (w.keyword == Keyword::FULLTEXT || w.keyword == Keyword::SPATIAL)
                    && dialect_of!(self is GenericDialect | MySqlDialect) =>
            {
                if let Some(name) = name {
                    return self.expected(
                        "FULLTEXT or SPATIAL option without constraint name",
                        TokenWithSpan {
                            token: Token::make_keyword(&name.to_string()),
                            span: next_token.span,
                        },
                    );
                }

                let fulltext = w.keyword == Keyword::FULLTEXT;

                let index_type_display = self.parse_index_type_display();

                let opt_index_name = self.parse_optional_indent()?;

                let columns = self.parse_parenthesized_column_list(Mandatory, false)?;

                Ok(Some(TableConstraint::FulltextOrSpatial {
                    fulltext,
                    index_type_display,
                    opt_index_name,
                    columns,
                }))
            }
            _ => {
                if name.is_some() {
                    self.expected("PRIMARY, UNIQUE, FOREIGN, or CHECK", next_token)
                } else {
                    self.prev_token();
                    Ok(None)
                }
            }
        }
    }

    fn parse_optional_nulls_distinct(&mut self) -> Result<NullsDistinctOption, ParserError> {
        Ok(if self.parse_keyword(Keyword::NULLS) {
            let not = self.parse_keyword(Keyword::NOT);
            self.expect_keyword(Keyword::DISTINCT)?;
            if not {
                NullsDistinctOption::NotDistinct
            } else {
                NullsDistinctOption::Distinct
            }
        } else {
            NullsDistinctOption::None
        })
    }
}

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#[cfg(not(feature = "std"))]
use alloc::{boxed::Box, format, string::ToString, vec, vec::Vec};

use super::{IsOptional::*, Parser, ParserError};
use crate::ast::helpers::attached_token::AttachedToken;
use crate::ast::helpers::key_value_options::{KeyValueOptions, KeyValueOptionsDelimiter};
use crate::ast::helpers::stmt_create_table::CreateTableBuilder;
use crate::ast::*;
use crate::dialect::*;
use crate::keywords::Keyword;
use crate::tokenizer::*;

impl Parser<'_> {
    /// Parse a SQL CREATE statement
    pub fn parse_create(&mut self) -> Result<Statement, ParserError> {
        let modifier_loc = self.peek_token_ref().span.start;
        let or_replace = self.parse_keywords(&[Keyword::OR, Keyword::REPLACE]);
        let or_alter = self.parse_keywords(&[Keyword::OR, Keyword::ALTER]);
        let multiset = self.maybe_parse_multiset();
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
        let volatile = self.parse_keyword(Keyword::VOLATILE);
        let unlogged = self.peek_keywords(&[Keyword::UNLOGGED, Keyword::TABLE]);
        if unlogged {
            self.expect_keyword(Keyword::UNLOGGED)?;
        }
        let persistent = dialect_of!(self is DuckDbDialect)
            && self.parse_one_of_keywords(&[Keyword::PERSISTENT]).is_some();
        let create_view_params = self.parse_create_view_params()?;
        if self.peek_keywords(&[Keyword::SNAPSHOT, Keyword::TABLE]) {
            self.parse_create_snapshot_table().map(Into::into)
        } else if self.peek_keywords(&[Keyword::TEXT, Keyword::SEARCH]) {
            self.parse_create_text_search().map(Into::into)
        } else if self.parse_keyword(Keyword::TABLE) {
            self.parse_create_table(
                or_replace, temporary, unlogged, global, transient, volatile, multiset,
            )
            .map(Into::into)
        } else if self.peek_keyword(Keyword::MATERIALIZED)
            || self.peek_keyword(Keyword::VIEW)
            || self.peek_keywords(&[Keyword::SECURE, Keyword::MATERIALIZED, Keyword::VIEW])
            || self.peek_keywords(&[Keyword::SECURE, Keyword::VIEW])
        {
            self.parse_create_view(or_alter, or_replace, temporary, create_view_params)
                .map(Into::into)
        } else if self.parse_keyword(Keyword::POLICY) {
            self.parse_create_policy().map(Into::into)
        } else if self.parse_keyword(Keyword::EXTERNAL) {
            self.parse_create_external_table(or_replace).map(Into::into)
        } else if self.parse_keyword(Keyword::FUNCTION) {
            self.parse_create_function(or_alter, or_replace, temporary)
        } else if self.parse_keyword(Keyword::DOMAIN) {
            self.parse_create_domain().map(Into::into)
        } else if self.parse_keyword(Keyword::TRIGGER) {
            self.parse_create_trigger(temporary, or_alter, or_replace, false)
                .map(Into::into)
        } else if self.parse_keywords(&[Keyword::CONSTRAINT, Keyword::TRIGGER]) {
            self.parse_create_trigger(temporary, or_alter, or_replace, true)
                .map(Into::into)
        } else if self.parse_keyword(Keyword::MACRO) {
            self.parse_create_macro(or_replace, temporary)
        } else if self.parse_keyword(Keyword::SECRET) {
            self.parse_create_secret(or_replace, temporary, persistent)
        } else if self.parse_keyword(Keyword::USER) {
            self.parse_create_user(or_replace).map(Into::into)
        } else if self.parse_keyword(Keyword::SCHEMA) {
            self.parse_create_schema(or_replace)
        } else if self.parse_keyword(Keyword::WAREHOUSE) {
            self.parse_create_warehouse(or_replace).map(Into::into)
        } else if or_replace {
            self.expected_ref(
                "[EXTERNAL] TABLE or [MATERIALIZED] VIEW or FUNCTION or SCHEMA or WAREHOUSE after CREATE OR REPLACE",
                self.peek_token_ref(),
            )
        } else if self.parse_keyword(Keyword::EXTENSION) {
            self.parse_create_extension().map(Into::into)
        } else if self.parse_keyword(Keyword::INDEX) {
            self.parse_create_index(false).map(Into::into)
        } else if self.parse_keywords(&[Keyword::UNIQUE, Keyword::INDEX]) {
            self.parse_create_index(true).map(Into::into)
        } else if self.parse_keyword(Keyword::VIRTUAL) {
            self.parse_create_virtual_table()
        } else if self.parse_keyword(Keyword::DATABASE) {
            self.parse_create_database()
        } else if self.parse_keyword(Keyword::ROLE) {
            self.parse_create_role().map(Into::into)
        } else if self.parse_keyword(Keyword::SEQUENCE) {
            self.parse_create_sequence(temporary)
        } else if self.parse_keyword(Keyword::COLLATION) {
            self.parse_create_collation().map(Into::into)
        } else if self.parse_keyword(Keyword::TYPE) {
            self.parse_create_type()
        } else if self.parse_keyword(Keyword::PROCEDURE)
            || self.dialect.supports_create_proc_syntax() && self.parse_keyword(Keyword::PROC)
        {
            self.parse_create_procedure(or_alter)
        } else if self.parse_keyword(Keyword::CONNECTOR) {
            self.parse_create_connector().map(Into::into)
        } else if self.parse_keyword(Keyword::OPERATOR) {
            // Check if this is CREATE OPERATOR FAMILY or CREATE OPERATOR CLASS
            if self.parse_keyword(Keyword::FAMILY) {
                self.parse_create_operator_family().map(Into::into)
            } else if self.parse_keyword(Keyword::CLASS) {
                self.parse_create_operator_class().map(Into::into)
            } else {
                self.parse_create_operator().map(Into::into)
            }
        } else if self.parse_keyword(Keyword::SERVER) {
            self.parse_pg_create_server()
        } else if self.parse_keywords(&[Keyword::FOREIGN, Keyword::TABLE]) {
            // `or_replace` cannot reach here today, since the arm above catches it.
            // It stays so that reordering the arms cannot make it fall through.
            if or_replace
                || or_alter
                || temporary
                || global.is_some()
                || transient
                || volatile
                || multiset.is_some()
                || persistent
                || create_view_params.is_some()
            {
                return parser_err!(
                    "CREATE FOREIGN TABLE does not accept this modifier",
                    modifier_loc
                );
            }
            self.parse_create_foreign_table().map(Into::into)
        } else {
            self.expected_ref("an object type after CREATE", self.peek_token_ref())
        }
    }

    /// Parse a `CREATE TEXT SEARCH ...` statement.
    pub fn parse_create_text_search(&mut self) -> Result<CreateTextSearch, ParserError> {
        self.expect_keywords(&[Keyword::TEXT, Keyword::SEARCH])?;
        let object_type = self.parse_text_search_object_type()?;
        let name = self.parse_object_name(false)?;
        self.expect_token(&Token::LParen)?;
        let options = self.parse_comma_separated(Parser::parse_sql_option)?;
        self.expect_token(&Token::RParen)?;
        Ok(CreateTextSearch {
            object_type,
            name,
            options,
        })
    }

    fn parse_create_user(&mut self, or_replace: bool) -> Result<CreateUser, ParserError> {
        let if_not_exists = self.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let name = self.parse_identifier()?;
        let options = self
            .parse_key_value_options(false, &[Keyword::WITH, Keyword::TAG])?
            .options;
        let with_tags = self.parse_keyword(Keyword::WITH);
        let tags = if self.parse_keyword(Keyword::TAG) {
            self.parse_key_value_options(true, &[])?.options
        } else {
            vec![]
        };
        Ok(CreateUser {
            or_replace,
            if_not_exists,
            name,
            options: KeyValueOptions {
                options,
                delimiter: KeyValueOptionsDelimiter::Space,
            },
            with_tags,
            tags: KeyValueOptions {
                options: tags,
                delimiter: KeyValueOptionsDelimiter::Comma,
            },
        })
    }

    /// Parse a `CREATE WAREHOUSE` statement.
    ///
    /// See <https://docs.snowflake.com/en/sql-reference/sql/create-warehouse>
    fn parse_create_warehouse(&mut self, or_replace: bool) -> Result<CreateWarehouse, ParserError> {
        let if_not_exists = self.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let name = self.parse_object_name(false)?;
        let _ = self.parse_keyword(Keyword::WITH);
        let options = self.parse_key_value_options(false, &[])?;
        Ok(CreateWarehouse {
            or_replace,
            if_not_exists,
            name,
            options,
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
        if self.peek_token_ref().token != Token::LParen {
            if self.parse_keyword(Keyword::IN) {
                storage_specifier = self.parse_identifier().ok()
            } else {
                name = self.parse_identifier().ok();
            }

            // Storage specifier may follow the name
            if storage_specifier.is_none()
                && self.peek_token_ref().token != Token::LParen
                && self.parse_keyword(Keyword::IN)
            {
                storage_specifier = self.parse_identifier().ok();
            }
        }

        self.expect_token(&Token::LParen)?;
        self.expect_keyword_is(Keyword::TYPE)?;
        let secret_type = self.parse_identifier()?;

        let mut options = Vec::new();
        if self.consume_token(&Token::Comma) {
            options.append(&mut self.parse_comma_separated(|p| {
                let key = p.parse_identifier()?;
                let value = p.parse_identifier()?;
                Ok(SecretOption { key, value })
            })?);
        }
        self.expect_token(&Token::RParen)?;

        let temp = match (temporary, persistent) {
            (true, false) => Some(true),
            (false, true) => Some(false),
            (false, false) => None,
            _ => self.expected_ref("TEMPORARY or PERSISTENT", self.peek_token_ref())?,
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

    /// SQLite-specific `CREATE VIRTUAL TABLE`
    pub fn parse_create_virtual_table(&mut self) -> Result<Statement, ParserError> {
        self.expect_keyword_is(Keyword::TABLE)?;
        let if_not_exists = self.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let table_name = self.parse_object_name(false)?;
        self.expect_keyword_is(Keyword::USING)?;
        let module_name = self.parse_identifier()?;
        // SQLite docs note that module "arguments syntax is sufficiently
        // general that the arguments can be made to appear as column
        // definitions in a traditional CREATE TABLE statement", but
        // we don't implement that.
        let module_args = self.parse_parenthesized_column_list(Optional, false)?;
        Ok(Statement::CreateVirtualTable {
            name: table_name,
            if_not_exists,
            module_name,
            module_args,
        })
    }

    /// Parse a `CREATE SCHEMA` statement.
    pub fn parse_create_schema(&mut self, or_replace: bool) -> Result<Statement, ParserError> {
        let if_not_exists = self.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);

        let schema_name = self.parse_schema_name()?;

        let default_collate_spec = if self.parse_keywords(&[Keyword::DEFAULT, Keyword::COLLATE]) {
            Some(self.parse_expr()?)
        } else {
            None
        };

        let with = if self.peek_keyword(Keyword::WITH) {
            Some(self.parse_options(Keyword::WITH)?)
        } else {
            None
        };

        let options = if self.peek_keyword(Keyword::OPTIONS) {
            Some(self.parse_options(Keyword::OPTIONS)?)
        } else {
            None
        };

        let clone = if self.parse_keyword(Keyword::CLONE) {
            Some(self.parse_object_name(false)?)
        } else {
            None
        };

        Ok(Statement::CreateSchema {
            schema_name,
            or_replace,
            if_not_exists,
            with,
            options,
            default_collate_spec,
            clone,
        })
    }

    /// Parse a `CREATE DATABASE` statement.
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
        let clone = if self.parse_keyword(Keyword::CLONE) {
            Some(self.parse_object_name(false)?)
        } else {
            None
        };

        // Parse MySQL-style [DEFAULT] CHARACTER SET and [DEFAULT] COLLATE options
        //
        // Note: The docs only mention `CHARACTER SET`, but `CHARSET` is also supported.
        // Furthermore, MySQL will only accept one character set, raising an error if there is more
        // than one, but will accept multiple collations and use the last one.
        //
        // <https://dev.mysql.com/doc/refman/8.4/en/create-database.html>
        let mut default_charset = None;
        let mut default_collation = None;
        loop {
            let has_default = self.parse_keyword(Keyword::DEFAULT);
            if default_charset.is_none() && self.parse_keywords(&[Keyword::CHARACTER, Keyword::SET])
                || self.parse_keyword(Keyword::CHARSET)
            {
                let _ = self.consume_token(&Token::Eq);
                default_charset = Some(self.parse_identifier()?.value);
            } else if self.parse_keyword(Keyword::COLLATE) {
                let _ = self.consume_token(&Token::Eq);
                default_collation = Some(self.parse_identifier()?.value);
            } else if has_default {
                // DEFAULT keyword not followed by CHARACTER SET, CHARSET, or COLLATE
                self.prev_token();
                break;
            } else {
                break;
            }
        }

        Ok(Statement::CreateDatabase {
            db_name,
            if_not_exists: ine,
            location,
            managed_location,
            or_replace: false,
            transient: false,
            clone,
            data_retention_time_in_days: None,
            max_data_extension_time_in_days: None,
            external_volume: None,
            catalog: None,
            replace_invalid_characters: None,
            default_ddl_collation: None,
            storage_serialization_policy: None,
            comment: None,
            default_charset,
            default_collation,
            catalog_sync: None,
            catalog_sync_namespace_mode: None,
            catalog_sync_namespace_flatten_delimiter: None,
            with_tags: None,
            with_contacts: None,
        })
    }

    /// Parse an optional `USING` clause for `CREATE FUNCTION`.
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

    /// Parse a `CREATE FUNCTION` statement.
    pub fn parse_create_function(
        &mut self,
        or_alter: bool,
        or_replace: bool,
        temporary: bool,
    ) -> Result<Statement, ParserError> {
        if dialect_of!(self is HiveDialect) {
            self.parse_hive_create_function(or_replace, temporary)
                .map(Into::into)
        } else if dialect_of!(self is PostgreSqlDialect | GenericDialect) {
            self.parse_postgres_create_function(or_replace, temporary)
                .map(Into::into)
        } else if dialect_of!(self is DuckDbDialect) {
            self.parse_create_macro(or_replace, temporary)
        } else if dialect_of!(self is BigQueryDialect) {
            self.parse_bigquery_create_function(or_replace, temporary)
                .map(Into::into)
        } else if dialect_of!(self is MsSqlDialect) {
            self.parse_mssql_create_function(or_alter, or_replace, temporary)
                .map(Into::into)
        } else {
            self.prev_token();
            self.expected_ref("an object type after CREATE", self.peek_token_ref())
        }
    }

    /// Parse `CREATE FUNCTION` for [PostgreSQL]
    ///
    /// [PostgreSQL]: https://www.postgresql.org/docs/15/sql-createfunction.html
    fn parse_postgres_create_function(
        &mut self,
        or_replace: bool,
        temporary: bool,
    ) -> Result<CreateFunction, ParserError> {
        let name = self.parse_object_name(false)?;

        self.expect_token(&Token::LParen)?;
        let args = if Token::RParen != self.peek_token_ref().token {
            self.parse_comma_separated(Parser::parse_function_arg)?
        } else {
            vec![]
        };
        self.expect_token(&Token::RParen)?;

        let return_type = if self.parse_keyword(Keyword::RETURNS) {
            Some(self.parse_function_return_type()?)
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
            security: Option<FunctionSecurity>,
        }
        let mut body = Body::default();
        let mut set_params: Vec<FunctionDefinitionSetParam> = Vec::new();
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
                body.function_body = Some(self.parse_create_function_body_string()?);
            } else if self.parse_keyword(Keyword::LANGUAGE) {
                ensure_not_set(&body.language, "LANGUAGE")?;
                body.language = Some(self.parse_identifier()?);
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
                    return self
                        .expected_ref("one of UNSAFE | RESTRICTED | SAFE", self.peek_token_ref());
                }
            } else if self.parse_keyword(Keyword::SECURITY) {
                ensure_not_set(&body.security, "SECURITY { DEFINER | INVOKER }")?;
                if self.parse_keyword(Keyword::DEFINER) {
                    body.security = Some(FunctionSecurity::Definer);
                } else if self.parse_keyword(Keyword::INVOKER) {
                    body.security = Some(FunctionSecurity::Invoker);
                } else {
                    return self.expected_ref("DEFINER or INVOKER", self.peek_token_ref());
                }
            } else if self.parse_keyword(Keyword::SET) {
                let name = self.parse_object_name(false)?;
                let value = if self.parse_keywords(&[Keyword::FROM, Keyword::CURRENT]) {
                    FunctionSetValue::FromCurrent
                } else {
                    if !self.consume_token(&Token::Eq) && !self.parse_keyword(Keyword::TO) {
                        return self.expected_ref("= or TO", self.peek_token_ref());
                    }
                    if self.parse_keyword(Keyword::DEFAULT) {
                        FunctionSetValue::Default
                    } else {
                        let values = self.parse_comma_separated(Parser::parse_expr)?;
                        FunctionSetValue::Values(values)
                    }
                };
                set_params.push(FunctionDefinitionSetParam { name, value });
            } else if self.parse_keyword(Keyword::RETURN) {
                ensure_not_set(&body.function_body, "RETURN")?;
                body.function_body = Some(CreateFunctionBody::Return(self.parse_expr()?));
            } else {
                break;
            }
        }

        Ok(CreateFunction {
            or_alter: false,
            or_replace,
            temporary,
            name,
            args: Some(args),
            return_type,
            behavior: body.behavior,
            called_on_null: body.called_on_null,
            parallel: body.parallel,
            security: body.security,
            set_params,
            language: body.language,
            function_body: body.function_body,
            if_not_exists: false,
            using: None,
            determinism_specifier: None,
            options: None,
            remote_connection: None,
        })
    }

    /// Parse `CREATE FUNCTION` for [Hive]
    ///
    /// [Hive]: https://cwiki.apache.org/confluence/display/hive/languagemanual+ddl#LanguageManualDDL-Create/Drop/ReloadFunction
    fn parse_hive_create_function(
        &mut self,
        or_replace: bool,
        temporary: bool,
    ) -> Result<CreateFunction, ParserError> {
        let name = self.parse_object_name(false)?;
        self.expect_keyword_is(Keyword::AS)?;

        let body = self.parse_create_function_body_string()?;
        let using = self.parse_optional_create_function_using()?;

        Ok(CreateFunction {
            or_alter: false,
            or_replace,
            temporary,
            name,
            function_body: Some(body),
            using,
            if_not_exists: false,
            args: None,
            return_type: None,
            behavior: None,
            called_on_null: None,
            parallel: None,
            security: None,
            set_params: vec![],
            language: None,
            determinism_specifier: None,
            options: None,
            remote_connection: None,
        })
    }

    /// Parse `CREATE FUNCTION` for [BigQuery]
    ///
    /// [BigQuery]: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#create_function_statement
    fn parse_bigquery_create_function(
        &mut self,
        or_replace: bool,
        temporary: bool,
    ) -> Result<CreateFunction, ParserError> {
        let if_not_exists = self.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let (name, args) = self.parse_create_function_name_and_params()?;

        let return_type = if self.parse_keyword(Keyword::RETURNS) {
            Some(self.parse_function_return_type()?)
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
            Some(self.parse_identifier()?)
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
            self.expect_keyword_is(Keyword::AS)?;
            let expr = self.parse_expr()?;
            if options.is_none() {
                options = self.maybe_parse_options(Keyword::OPTIONS)?;
                Some(CreateFunctionBody::AsBeforeOptions {
                    body: expr,
                    link_symbol: None,
                })
            } else {
                Some(CreateFunctionBody::AsAfterOptions(expr))
            }
        } else {
            None
        };

        Ok(CreateFunction {
            or_alter: false,
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
            security: None,
            set_params: vec![],
        })
    }

    /// Parse `CREATE FUNCTION` for [MsSql]
    ///
    /// [MsSql]: https://learn.microsoft.com/en-us/sql/t-sql/statements/create-function-transact-sql
    fn parse_mssql_create_function(
        &mut self,
        or_alter: bool,
        or_replace: bool,
        temporary: bool,
    ) -> Result<CreateFunction, ParserError> {
        let (name, args) = self.parse_create_function_name_and_params()?;

        self.expect_keyword(Keyword::RETURNS)?;

        let return_table = self.maybe_parse(|p| {
            let return_table_name = p.parse_identifier()?;

            p.expect_keyword_is(Keyword::TABLE)?;
            p.prev_token();

            let table_column_defs = match p.parse_data_type()? {
                DataType::Table(Some(table_column_defs)) if !table_column_defs.is_empty() => {
                    table_column_defs
                }
                _ => parser_err!(
                    "Expected table column definitions after TABLE keyword",
                    p.peek_token_ref().span.start
                )?,
            };

            Ok(DataType::NamedTable {
                name: ObjectName(vec![ObjectNamePart::Identifier(return_table_name)]),
                columns: table_column_defs,
            })
        })?;

        let data_type = match return_table {
            Some(table_type) => table_type,
            None => self.parse_data_type()?,
        };
        let return_type = Some(FunctionReturnType::DataType(data_type));

        let _ = self.parse_keyword(Keyword::AS);

        let function_body = if self.peek_keyword(Keyword::BEGIN) {
            let begin_token = self.expect_keyword(Keyword::BEGIN)?;
            let statements = self.parse_statement_list(&[Keyword::END])?;
            let end_token = self.expect_keyword(Keyword::END)?;

            Some(CreateFunctionBody::AsBeginEnd(BeginEndStatements {
                begin_token: AttachedToken(begin_token),
                statements,
                end_token: AttachedToken(end_token),
            }))
        } else if self.parse_keyword(Keyword::RETURN) {
            if self.peek_token_ref().token == Token::LParen {
                Some(CreateFunctionBody::AsReturnExpr(self.parse_expr()?))
            } else if self.peek_keyword(Keyword::SELECT) {
                let select = self.parse_select()?;
                Some(CreateFunctionBody::AsReturnSelect(select))
            } else {
                parser_err!(
                    "Expected a subquery (or bare SELECT statement) after RETURN",
                    self.peek_token_ref().span.start
                )?
            }
        } else {
            parser_err!("Unparsable function body", self.peek_token_ref().span.start)?
        };

        Ok(CreateFunction {
            or_alter,
            or_replace,
            temporary,
            if_not_exists: false,
            name,
            args: Some(args),
            return_type,
            function_body,
            language: None,
            determinism_specifier: None,
            options: None,
            remote_connection: None,
            using: None,
            behavior: None,
            called_on_null: None,
            parallel: None,
            security: None,
            set_params: vec![],
        })
    }

    fn parse_function_return_type(&mut self) -> Result<FunctionReturnType, ParserError> {
        if self.parse_keyword(Keyword::SETOF) {
            Ok(FunctionReturnType::SetOf(self.parse_data_type()?))
        } else {
            Ok(FunctionReturnType::DataType(self.parse_data_type()?))
        }
    }

    fn parse_create_function_name_and_params(
        &mut self,
    ) -> Result<(ObjectName, Vec<OperateFunctionArg>), ParserError> {
        let name = self.parse_object_name(false)?;
        let parse_function_param =
            |parser: &mut Parser| -> Result<OperateFunctionArg, ParserError> {
                let name = parser.parse_identifier()?;
                let data_type = parser.parse_data_type()?;
                let default_expr = if parser.consume_token(&Token::Eq) {
                    Some(parser.parse_expr()?)
                } else {
                    None
                };

                Ok(OperateFunctionArg {
                    mode: None,
                    name: Some(name),
                    data_type,
                    default_expr,
                })
            };
        self.expect_token(&Token::LParen)?;
        let args = self.parse_comma_separated0(parse_function_param, Token::RParen)?;
        self.expect_token(&Token::RParen)?;
        Ok((name, args))
    }

    pub(super) fn parse_function_arg(&mut self) -> Result<OperateFunctionArg, ParserError> {
        let mode = if self.parse_keyword(Keyword::IN) {
            Some(ArgMode::In)
        } else if self.parse_keyword(Keyword::OUT) {
            Some(ArgMode::Out)
        } else if self.parse_keyword(Keyword::INOUT) {
            Some(ArgMode::InOut)
        } else if self.parse_keyword(Keyword::VARIADIC) {
            Some(ArgMode::Variadic)
        } else {
            None
        };

        // parse: [ argname ] argtype
        let mut name = None;
        let mut data_type = self.parse_data_type()?;

        // To check whether the first token is a name or a type, we need to
        // peek the next token, which if it is another type keyword, then the
        // first token is a name and not a type in itself.
        let data_type_idx = self.get_current_index();

        // DEFAULT will be parsed as `DataType::Custom`, which is undesirable in this context
        fn parse_data_type_no_default(parser: &mut Parser) -> Result<DataType, ParserError> {
            if parser.peek_keyword(Keyword::DEFAULT) {
                // This dummy error is ignored in `maybe_parse`
                parser_err!(
                    "The DEFAULT keyword is not a type",
                    parser.peek_token_ref().span.start
                )
            } else {
                parser.parse_data_type()
            }
        }

        if let Some(next_data_type) = self.maybe_parse(parse_data_type_no_default)? {
            let token = self.token_at(data_type_idx).clone();

            // We ensure that the token is a `Word` token, and not other special tokens.
            match token.token {
                Token::Word(word) => name = Some(word.into_ident(token.span)),
                _ => return self.expected("a name or type", token),
            }

            data_type = next_data_type;
        }

        let default_expr = if self.parse_keyword(Keyword::DEFAULT) || self.consume_token(&Token::Eq)
        {
            Some(self.parse_expr()?)
        } else {
            None
        };
        Ok(OperateFunctionArg {
            mode,
            name,
            data_type,
            default_expr,
        })
    }

    pub(super) fn parse_aggregate_function_arg(
        &mut self,
    ) -> Result<OperateFunctionArg, ParserError> {
        let mode = if self.parse_keyword(Keyword::IN) {
            Some(ArgMode::In)
        } else {
            if self
                .peek_one_of_keywords(&[Keyword::OUT, Keyword::INOUT, Keyword::VARIADIC])
                .is_some()
            {
                return self.expected_ref(
                    "IN or argument type in aggregate signature",
                    self.peek_token_ref(),
                );
            }
            None
        };

        // Parse: [ argname ] argtype, but do not consume ORDER from
        // `... argtype ORDER BY ...` as a type-name disambiguator.
        let mut name = None;
        let mut data_type = self.parse_data_type()?;
        let data_type_idx = self.get_current_index();

        fn parse_data_type_for_aggregate_arg(parser: &mut Parser) -> Result<DataType, ParserError> {
            if parser.peek_keyword(Keyword::DEFAULT)
                || parser.peek_keyword(Keyword::ORDER)
                || parser.peek_token_ref().token == Token::Comma
                || parser.peek_token_ref().token == Token::RParen
            {
                // Dummy error ignored by maybe_parse
                parser_err!(
                    "The current token cannot start an aggregate argument type",
                    parser.peek_token_ref().span.start
                )
            } else {
                parser.parse_data_type()
            }
        }

        if let Some(next_data_type) = self.maybe_parse(parse_data_type_for_aggregate_arg)? {
            let token = self.token_at(data_type_idx).clone();
            match token.token {
                Token::Word(word) => name = Some(word.into_ident(token.span)),
                _ => return self.expected("a name or type", token),
            }

            data_type = next_data_type;
        }

        if self.peek_keyword(Keyword::DEFAULT) || self.peek_token_ref().token == Token::Eq {
            return self.expected_ref(
                "',' or ')' or ORDER BY after aggregate argument type",
                self.peek_token_ref(),
            );
        }

        Ok(OperateFunctionArg {
            mode,
            name,
            data_type,
            default_expr: None,
        })
    }

    /// Parse a `CREATE TRIGGER` statement.
    pub fn parse_create_trigger(
        &mut self,
        temporary: bool,
        or_alter: bool,
        or_replace: bool,
        is_constraint: bool,
    ) -> Result<CreateTrigger, ParserError> {
        if !dialect_of!(self is PostgreSqlDialect | SQLiteDialect | GenericDialect | MySqlDialect | MsSqlDialect)
        {
            self.prev_token();
            return self.expected_ref("an object type after CREATE", self.peek_token_ref());
        }

        let name = self.parse_object_name(false)?;
        let period = self.maybe_parse(|parser| parser.parse_trigger_period())?;

        let events = self.parse_keyword_separated(Keyword::OR, Parser::parse_trigger_event)?;
        self.expect_keyword_is(Keyword::ON)?;
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

        let trigger_object = if self.parse_keyword(Keyword::FOR) {
            let include_each = self.parse_keyword(Keyword::EACH);
            let trigger_object =
                match self.expect_one_of_keywords(&[Keyword::ROW, Keyword::STATEMENT])? {
                    Keyword::ROW => TriggerObject::Row,
                    Keyword::STATEMENT => TriggerObject::Statement,
                    unexpected_keyword => return Err(ParserError::ParserError(
                        format!("Internal parser error: unexpected keyword `{unexpected_keyword}` in ROW/STATEMENT"),
                    )),
                };

            Some(if include_each {
                TriggerObjectKind::ForEach(trigger_object)
            } else {
                TriggerObjectKind::For(trigger_object)
            })
        } else {
            let _ = self.parse_keyword(Keyword::FOR);

            None
        };

        let condition = self
            .parse_keyword(Keyword::WHEN)
            .then(|| self.parse_expr())
            .transpose()?;

        let mut exec_body = None;
        let mut statements = None;
        if self.parse_keyword(Keyword::EXECUTE) {
            exec_body = Some(self.parse_trigger_exec_body()?);
        } else {
            statements = Some(self.parse_conditional_statements(&[Keyword::END])?);
        }

        Ok(CreateTrigger {
            or_alter,
            temporary,
            or_replace,
            is_constraint,
            name,
            period,
            period_before_table: true,
            events,
            table_name,
            referenced_table_name,
            referencing,
            trigger_object,
            condition,
            exec_body,
            statements_as: false,
            statements,
            characteristics,
        })
    }

    /// Parse the period part of a trigger (`BEFORE`, `AFTER`, etc.).
    pub fn parse_trigger_period(&mut self) -> Result<TriggerPeriod, ParserError> {
        Ok(
            match self.expect_one_of_keywords(&[
                Keyword::FOR,
                Keyword::BEFORE,
                Keyword::AFTER,
                Keyword::INSTEAD,
            ])? {
                Keyword::FOR => TriggerPeriod::For,
                Keyword::BEFORE => TriggerPeriod::Before,
                Keyword::AFTER => TriggerPeriod::After,
                Keyword::INSTEAD => self
                    .expect_keyword_is(Keyword::OF)
                    .map(|_| TriggerPeriod::InsteadOf)?,
                unexpected_keyword => return Err(ParserError::ParserError(
                    format!("Internal parser error: unexpected keyword `{unexpected_keyword}` in trigger period"),
                )),
            },
        )
    }

    /// Parse the event part of a trigger (`INSERT`, `UPDATE`, etc.).
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
                        let cols = self.parse_comma_separated(Parser::parse_identifier)?;
                        TriggerEvent::Update(cols)
                    } else {
                        TriggerEvent::Update(vec![])
                    }
                }
                Keyword::DELETE => TriggerEvent::Delete,
                Keyword::TRUNCATE => TriggerEvent::Truncate,
                unexpected_keyword => return Err(ParserError::ParserError(
                    format!("Internal parser error: unexpected keyword `{unexpected_keyword}` in trigger event"),
                )),
            },
        )
    }

    /// Parse the `REFERENCING` clause of a trigger.
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

    /// Parse the execution body of a trigger (`FUNCTION` or `PROCEDURE`).
    pub fn parse_trigger_exec_body(&mut self) -> Result<TriggerExecBody, ParserError> {
        Ok(TriggerExecBody {
            exec_type: match self
                .expect_one_of_keywords(&[Keyword::FUNCTION, Keyword::PROCEDURE])?
            {
                Keyword::FUNCTION => TriggerExecBodyType::Function,
                Keyword::PROCEDURE => TriggerExecBodyType::Procedure,
                unexpected_keyword => return Err(ParserError::ParserError(
                    format!("Internal parser error: unexpected keyword `{unexpected_keyword}` in trigger exec body"),
                )),
            },
            func_desc: self.parse_function_desc()?,
        })
    }

    /// Parse a `CREATE MACRO` statement.
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
            self.expect_keyword_is(Keyword::AS)?;

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
            self.expected_ref("an object type after CREATE", self.peek_token_ref())
        }
    }

    fn parse_macro_arg(&mut self) -> Result<MacroArg, ParserError> {
        let name = self.parse_identifier()?;

        let default_expr =
            if self.consume_token(&Token::Assignment) || self.consume_token(&Token::RArrow) {
                Some(self.parse_expr()?)
            } else {
                None
            };
        Ok(MacroArg { name, default_expr })
    }

    /// Parse a `CREATE EXTERNAL TABLE` statement.
    pub fn parse_create_external_table(
        &mut self,
        or_replace: bool,
    ) -> Result<CreateTable, ParserError> {
        self.expect_keyword_is(Keyword::TABLE)?;
        let if_not_exists = self.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let table_name = self.parse_object_name(false)?;
        let (columns, constraints) = self.parse_columns()?;

        let hive_distribution = self.parse_hive_distribution()?;
        let hive_formats = self.parse_hive_formats()?;

        let file_format = if let Some(ref hf) = hive_formats {
            if let Some(ref ff) = hf.storage {
                match ff {
                    HiveIOFormat::FileFormat { format } => Some(*format),
                    _ => None,
                }
            } else {
                None
            }
        } else {
            None
        };
        let location = hive_formats.as_ref().and_then(|hf| hf.location.clone());

        let with_connection = if self.parse_keywords(&[Keyword::WITH, Keyword::CONNECTION]) {
            Some(self.parse_object_name(false)?)
        } else {
            None
        };
        let table_properties = self.parse_options(Keyword::TBLPROPERTIES)?;
        let table_options = if !table_properties.is_empty() {
            CreateTableOptions::TableProperties(table_properties)
        } else if let Some(options) = self.maybe_parse_options(Keyword::OPTIONS)? {
            CreateTableOptions::Options(options)
        } else {
            CreateTableOptions::None
        };
        Ok(CreateTableBuilder::new(table_name)
            .columns(columns)
            .constraints(constraints)
            .hive_distribution(hive_distribution)
            .hive_formats(hive_formats)
            .table_options(table_options)
            .with_connection(with_connection)
            .or_replace(or_replace)
            .if_not_exists(if_not_exists)
            .external(true)
            .file_format(file_format)
            .location(location)
            .build())
    }

    /// Parse `CREATE SNAPSHOT TABLE` statement.
    ///
    /// <https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#create_snapshot_table_statement>
    pub fn parse_create_snapshot_table(&mut self) -> Result<CreateTable, ParserError> {
        self.expect_keywords(&[Keyword::SNAPSHOT, Keyword::TABLE])?;
        let if_not_exists = self.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let table_name = self.parse_object_name(true)?;

        self.expect_keyword_is(Keyword::CLONE)?;
        let clone = Some(self.parse_object_name(true)?);

        let version =
            if self.parse_keywords(&[Keyword::FOR, Keyword::SYSTEM_TIME, Keyword::AS, Keyword::OF])
            {
                Some(TableVersion::ForSystemTimeAsOf(self.parse_expr()?))
            } else {
                None
            };

        let table_options = if let Some(options) = self.maybe_parse_options(Keyword::OPTIONS)? {
            CreateTableOptions::Options(options)
        } else {
            CreateTableOptions::None
        };

        Ok(CreateTableBuilder::new(table_name)
            .snapshot(true)
            .if_not_exists(if_not_exists)
            .clone_clause(clone)
            .version(version)
            .table_options(table_options)
            .build())
    }

    /// Parse a `CREATE VIEW` statement.
    pub fn parse_create_view(
        &mut self,
        or_alter: bool,
        or_replace: bool,
        temporary: bool,
        create_view_params: Option<CreateViewParams>,
    ) -> Result<CreateView, ParserError> {
        let secure = self.parse_keyword(Keyword::SECURE);
        let materialized = self.parse_keyword(Keyword::MATERIALIZED);
        self.expect_keyword_is(Keyword::VIEW)?;
        let allow_unquoted_hyphen = dialect_of!(self is BigQueryDialect);
        // Tries to parse IF NOT EXISTS either before name or after name
        // Name before IF NOT EXISTS is supported by snowflake but undocumented
        let if_not_exists_first =
            self.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let name = self.parse_object_name(allow_unquoted_hyphen)?;
        let name_before_not_exists = !if_not_exists_first
            && self.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let if_not_exists = if_not_exists_first || name_before_not_exists;
        let mut copy_grants = self.parse_keywords(&[Keyword::COPY, Keyword::GRANTS]);
        // Many dialects support `OR ALTER` right after `CREATE`, but we don't (yet).
        // ANSI SQL and Postgres support RECURSIVE here, but we don't support it either.
        let columns = self.parse_view_columns()?;
        // Snowflake also documents `COPY GRANTS` *after* the column list; accept
        // either position, but not both.
        // <https://docs.snowflake.com/en/sql-reference/sql/create-view#syntax>
        if !copy_grants {
            copy_grants = self.parse_keywords(&[Keyword::COPY, Keyword::GRANTS]);
        }
        let mut options = CreateTableOptions::None;
        let with_options = self.parse_options(Keyword::WITH)?;
        if !with_options.is_empty() {
            options = CreateTableOptions::With(with_options);
        }

        let cluster_by = if self.parse_keyword(Keyword::CLUSTER) {
            self.expect_keyword_is(Keyword::BY)?;
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

        let comment = if self.dialect.supports_create_view_comment_syntax()
            && self.parse_keyword(Keyword::COMMENT)
        {
            self.expect_token(&Token::Eq)?;
            Some(self.parse_comment_value()?)
        } else {
            None
        };

        self.expect_keyword_is(Keyword::AS)?;
        let query = self.parse_query()?;
        // Optional `WITH [ CASCADED | LOCAL ] CHECK OPTION` is widely supported here.

        let with_no_schema_binding = dialect_of!(self is RedshiftSqlDialect | GenericDialect)
            && self.parse_keywords(&[
                Keyword::WITH,
                Keyword::NO,
                Keyword::SCHEMA,
                Keyword::BINDING,
            ]);

        Ok(CreateView {
            or_alter,
            name,
            columns,
            query,
            materialized,
            secure,
            or_replace,
            options,
            cluster_by,
            comment,
            with_no_schema_binding,
            if_not_exists,
            temporary,
            copy_grants,
            to,
            params: create_view_params,
            name_before_not_exists,
        })
    }

    /// Parse optional parameters for the `CREATE VIEW` statement supported by [MySQL].
    ///
    /// [MySQL]: https://dev.mysql.com/doc/refman/9.1/en/create-view.html
    fn parse_create_view_params(&mut self) -> Result<Option<CreateViewParams>, ParserError> {
        let algorithm = if self.parse_keyword(Keyword::ALGORITHM) {
            self.expect_token(&Token::Eq)?;
            Some(
                match self.expect_one_of_keywords(&[
                    Keyword::UNDEFINED,
                    Keyword::MERGE,
                    Keyword::TEMPTABLE,
                ])? {
                    Keyword::UNDEFINED => CreateViewAlgorithm::Undefined,
                    Keyword::MERGE => CreateViewAlgorithm::Merge,
                    Keyword::TEMPTABLE => CreateViewAlgorithm::TempTable,
                    _ => {
                        self.prev_token();
                        let found = self.next_token();
                        return self
                            .expected("UNDEFINED or MERGE or TEMPTABLE after ALGORITHM =", found);
                    }
                },
            )
        } else {
            None
        };
        let definer = if self.parse_keyword(Keyword::DEFINER) {
            self.expect_token(&Token::Eq)?;
            Some(self.parse_grantee_name()?)
        } else {
            None
        };
        let security = if self.parse_keywords(&[Keyword::SQL, Keyword::SECURITY]) {
            Some(
                match self.expect_one_of_keywords(&[Keyword::DEFINER, Keyword::INVOKER])? {
                    Keyword::DEFINER => CreateViewSecurity::Definer,
                    Keyword::INVOKER => CreateViewSecurity::Invoker,
                    _ => {
                        self.prev_token();
                        let found = self.next_token();
                        return self.expected("DEFINER or INVOKER after SQL SECURITY", found);
                    }
                },
            )
        } else {
            None
        };
        if algorithm.is_some() || definer.is_some() || security.is_some() {
            Ok(Some(CreateViewParams {
                algorithm,
                definer,
                security,
            }))
        } else {
            Ok(None)
        }
    }

    /// Parse a `CREATE ROLE` statement.
    pub fn parse_create_role(&mut self) -> Result<CreateRole, ParserError> {
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
                    self.expect_keyword_is(Keyword::LIMIT)?;
                    if connection_limit.is_some() {
                        parser_err!("Found multiple CONNECTION LIMIT", loc)
                    } else {
                        connection_limit = Some(Expr::Value(self.parse_number_value()?));
                        Ok(())
                    }
                }
                Keyword::VALID => {
                    self.expect_keyword_is(Keyword::UNTIL)?;
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
                            in_role = self.parse_comma_separated(|p| p.parse_identifier())?;
                            Ok(())
                        }
                    } else if self.parse_keyword(Keyword::GROUP) {
                        if !in_group.is_empty() {
                            parser_err!("Found multiple IN GROUP", loc)
                        } else {
                            in_group = self.parse_comma_separated(|p| p.parse_identifier())?;
                            Ok(())
                        }
                    } else {
                        self.expected_ref("ROLE or GROUP after IN", self.peek_token_ref())
                    }
                }
                Keyword::ROLE => {
                    if !role.is_empty() {
                        parser_err!("Found multiple ROLE", loc)
                    } else {
                        role = self.parse_comma_separated(|p| p.parse_identifier())?;
                        Ok(())
                    }
                }
                Keyword::USER => {
                    if !user.is_empty() {
                        parser_err!("Found multiple USER", loc)
                    } else {
                        user = self.parse_comma_separated(|p| p.parse_identifier())?;
                        Ok(())
                    }
                }
                Keyword::ADMIN => {
                    if !admin.is_empty() {
                        parser_err!("Found multiple ADMIN", loc)
                    } else {
                        admin = self.parse_comma_separated(|p| p.parse_identifier())?;
                        Ok(())
                    }
                }
                _ => break,
            }?
        }

        Ok(CreateRole {
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

    /// Parses a [Statement::CreateDomain] statement.
    fn parse_create_domain(&mut self) -> Result<CreateDomain, ParserError> {
        let name = self.parse_object_name(false)?;
        self.expect_keyword_is(Keyword::AS)?;
        let data_type = self.parse_data_type()?;
        let collation = if self.parse_keyword(Keyword::COLLATE) {
            Some(self.parse_identifier()?)
        } else {
            None
        };
        let default = if self.parse_keyword(Keyword::DEFAULT) {
            Some(self.parse_expr()?)
        } else {
            None
        };
        let mut constraints = Vec::new();
        while let Some(constraint) = self.parse_optional_table_constraint()? {
            constraints.push(constraint);
        }

        Ok(CreateDomain {
            name,
            data_type,
            collation,
            default,
            constraints,
        })
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
    pub fn parse_create_policy(&mut self) -> Result<CreatePolicy, ParserError> {
        let name = self.parse_identifier()?;
        self.expect_keyword_is(Keyword::ON)?;
        let table_name = self.parse_object_name(false)?;

        let policy_type = if self.parse_keyword(Keyword::AS) {
            let keyword =
                self.expect_one_of_keywords(&[Keyword::PERMISSIVE, Keyword::RESTRICTIVE])?;
            Some(match keyword {
                Keyword::PERMISSIVE => CreatePolicyType::Permissive,
                Keyword::RESTRICTIVE => CreatePolicyType::Restrictive,
                unexpected_keyword => return Err(ParserError::ParserError(
                    format!("Internal parser error: unexpected keyword `{unexpected_keyword}` in policy type"),
                )),
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
                unexpected_keyword => return Err(ParserError::ParserError(
                    format!("Internal parser error: unexpected keyword `{unexpected_keyword}` in policy command"),
                )),
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

    /// ```sql
    /// CREATE CONNECTOR [IF NOT EXISTS] connector_name
    /// [TYPE datasource_type]
    /// [URL datasource_url]
    /// [COMMENT connector_comment]
    /// [WITH DCPROPERTIES(property_name=property_value, ...)]
    /// ```
    ///
    /// [Hive Documentation](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=27362034#LanguageManualDDL-CreateDataConnectorCreateConnector)
    pub fn parse_create_connector(&mut self) -> Result<CreateConnector, ParserError> {
        let if_not_exists = self.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let name = self.parse_identifier()?;

        let connector_type = if self.parse_keyword(Keyword::TYPE) {
            Some(self.parse_literal_string()?)
        } else {
            None
        };

        let url = if self.parse_keyword(Keyword::URL) {
            Some(self.parse_literal_string()?)
        } else {
            None
        };

        let comment = self.parse_optional_inline_comment()?;

        let with_dcproperties =
            match self.parse_options_with_keywords(&[Keyword::WITH, Keyword::DCPROPERTIES])? {
                properties if !properties.is_empty() => Some(properties),
                _ => None,
            };

        Ok(CreateConnector {
            name,
            if_not_exists,
            connector_type,
            url,
            comment,
            with_dcproperties,
        })
    }

    /// Parse a [Statement::CreateOperator]
    ///
    /// [PostgreSQL Documentation](https://www.postgresql.org/docs/current/sql-createoperator.html)
    pub fn parse_create_operator(&mut self) -> Result<CreateOperator, ParserError> {
        let name = self.parse_operator_name()?;
        self.expect_token(&Token::LParen)?;

        let mut function: Option<ObjectName> = None;
        let mut is_procedure = false;
        let mut left_arg: Option<DataType> = None;
        let mut right_arg: Option<DataType> = None;
        let mut options: Vec<OperatorOption> = Vec::new();

        loop {
            let keyword = self.expect_one_of_keywords(&[
                Keyword::FUNCTION,
                Keyword::PROCEDURE,
                Keyword::LEFTARG,
                Keyword::RIGHTARG,
                Keyword::COMMUTATOR,
                Keyword::NEGATOR,
                Keyword::RESTRICT,
                Keyword::JOIN,
                Keyword::HASHES,
                Keyword::MERGES,
            ])?;

            match keyword {
                Keyword::HASHES if !options.iter().any(|o| matches!(o, OperatorOption::Hashes)) => {
                    options.push(OperatorOption::Hashes);
                }
                Keyword::MERGES if !options.iter().any(|o| matches!(o, OperatorOption::Merges)) => {
                    options.push(OperatorOption::Merges);
                }
                Keyword::FUNCTION | Keyword::PROCEDURE if function.is_none() => {
                    self.expect_token(&Token::Eq)?;
                    function = Some(self.parse_object_name(false)?);
                    is_procedure = keyword == Keyword::PROCEDURE;
                }
                Keyword::LEFTARG if left_arg.is_none() => {
                    self.expect_token(&Token::Eq)?;
                    left_arg = Some(self.parse_data_type()?);
                }
                Keyword::RIGHTARG if right_arg.is_none() => {
                    self.expect_token(&Token::Eq)?;
                    right_arg = Some(self.parse_data_type()?);
                }
                Keyword::COMMUTATOR
                    if !options
                        .iter()
                        .any(|o| matches!(o, OperatorOption::Commutator(_))) =>
                {
                    self.expect_token(&Token::Eq)?;
                    if self.parse_keyword(Keyword::OPERATOR) {
                        self.expect_token(&Token::LParen)?;
                        let op = self.parse_operator_name()?;
                        self.expect_token(&Token::RParen)?;
                        options.push(OperatorOption::Commutator(op));
                    } else {
                        options.push(OperatorOption::Commutator(self.parse_operator_name()?));
                    }
                }
                Keyword::NEGATOR
                    if !options
                        .iter()
                        .any(|o| matches!(o, OperatorOption::Negator(_))) =>
                {
                    self.expect_token(&Token::Eq)?;
                    if self.parse_keyword(Keyword::OPERATOR) {
                        self.expect_token(&Token::LParen)?;
                        let op = self.parse_operator_name()?;
                        self.expect_token(&Token::RParen)?;
                        options.push(OperatorOption::Negator(op));
                    } else {
                        options.push(OperatorOption::Negator(self.parse_operator_name()?));
                    }
                }
                Keyword::RESTRICT
                    if !options
                        .iter()
                        .any(|o| matches!(o, OperatorOption::Restrict(_))) =>
                {
                    self.expect_token(&Token::Eq)?;
                    options.push(OperatorOption::Restrict(Some(
                        self.parse_object_name(false)?,
                    )));
                }
                Keyword::JOIN if !options.iter().any(|o| matches!(o, OperatorOption::Join(_))) => {
                    self.expect_token(&Token::Eq)?;
                    options.push(OperatorOption::Join(Some(self.parse_object_name(false)?)));
                }
                _ => {
                    return Err(ParserError::ParserError(format!(
                        "Duplicate or unexpected keyword {:?} in CREATE OPERATOR",
                        keyword
                    )))
                }
            }

            if !self.consume_token(&Token::Comma) {
                break;
            }
        }

        // Expect closing parenthesis
        self.expect_token(&Token::RParen)?;

        // FUNCTION is required
        let function = function.ok_or_else(|| {
            ParserError::ParserError("CREATE OPERATOR requires FUNCTION parameter".to_string())
        })?;

        Ok(CreateOperator {
            name,
            function,
            is_procedure,
            left_arg,
            right_arg,
            options,
        })
    }

    /// Parse a [Statement::CreateOperatorFamily]
    ///
    /// [PostgreSQL Documentation](https://www.postgresql.org/docs/current/sql-createopfamily.html)
    pub fn parse_create_operator_family(&mut self) -> Result<CreateOperatorFamily, ParserError> {
        let name = self.parse_object_name(false)?;
        self.expect_keyword(Keyword::USING)?;
        let using = self.parse_identifier()?;

        Ok(CreateOperatorFamily { name, using })
    }

    /// Parse a [Statement::CreateOperatorClass]
    ///
    /// [PostgreSQL Documentation](https://www.postgresql.org/docs/current/sql-createopclass.html)
    pub fn parse_create_operator_class(&mut self) -> Result<CreateOperatorClass, ParserError> {
        let name = self.parse_object_name(false)?;
        let default = self.parse_keyword(Keyword::DEFAULT);
        self.expect_keywords(&[Keyword::FOR, Keyword::TYPE])?;
        let for_type = self.parse_data_type()?;
        self.expect_keyword(Keyword::USING)?;
        let using = self.parse_identifier()?;

        let family = if self.parse_keyword(Keyword::FAMILY) {
            Some(self.parse_object_name(false)?)
        } else {
            None
        };

        self.expect_keyword(Keyword::AS)?;

        let mut items = vec![];
        loop {
            if self.parse_keyword(Keyword::OPERATOR) {
                let strategy_number = self.parse_literal_uint()?;
                let operator_name = self.parse_operator_name()?;

                // Optional operator argument types
                let op_types = if self.consume_token(&Token::LParen) {
                    let left = self.parse_data_type()?;
                    self.expect_token(&Token::Comma)?;
                    let right = self.parse_data_type()?;
                    self.expect_token(&Token::RParen)?;
                    Some(OperatorArgTypes { left, right })
                } else {
                    None
                };

                // Optional purpose
                let purpose = if self.parse_keyword(Keyword::FOR) {
                    if self.parse_keyword(Keyword::SEARCH) {
                        Some(OperatorPurpose::ForSearch)
                    } else if self.parse_keywords(&[Keyword::ORDER, Keyword::BY]) {
                        let sort_family = self.parse_object_name(false)?;
                        Some(OperatorPurpose::ForOrderBy { sort_family })
                    } else {
                        return self
                            .expected_ref("SEARCH or ORDER BY after FOR", self.peek_token_ref());
                    }
                } else {
                    None
                };

                items.push(OperatorClassItem::Operator {
                    strategy_number,
                    operator_name,
                    op_types,
                    purpose,
                });
            } else if self.parse_keyword(Keyword::FUNCTION) {
                let support_number = self.parse_literal_uint()?;

                // Optional operator types
                let op_types = if self.consume_token(&Token::LParen)
                    && self.peek_token_ref().token != Token::RParen
                {
                    let mut types = vec![];
                    loop {
                        types.push(self.parse_data_type()?);
                        if !self.consume_token(&Token::Comma) {
                            break;
                        }
                    }
                    self.expect_token(&Token::RParen)?;
                    Some(types)
                } else if self.consume_token(&Token::LParen) {
                    self.expect_token(&Token::RParen)?;
                    Some(vec![])
                } else {
                    None
                };

                let function_name = self.parse_object_name(false)?;

                // Function argument types
                let argument_types = if self.consume_token(&Token::LParen) {
                    let mut types = vec![];
                    loop {
                        if self.peek_token_ref().token == Token::RParen {
                            break;
                        }
                        types.push(self.parse_data_type()?);
                        if !self.consume_token(&Token::Comma) {
                            break;
                        }
                    }
                    self.expect_token(&Token::RParen)?;
                    types
                } else {
                    vec![]
                };

                items.push(OperatorClassItem::Function {
                    support_number,
                    op_types,
                    function_name,
                    argument_types,
                });
            } else if self.parse_keyword(Keyword::STORAGE) {
                let storage_type = self.parse_data_type()?;
                items.push(OperatorClassItem::Storage { storage_type });
            } else {
                break;
            }

            // Check for comma separator
            if !self.consume_token(&Token::Comma) {
                break;
            }
        }

        Ok(CreateOperatorClass {
            name,
            default,
            for_type,
            using,
            family,
            items,
        })
    }
}

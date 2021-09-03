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
use alloc::{boxed::Box, format, vec, vec::Vec};

use crate::ast::*;
use crate::dialect::keywords::Keyword;
use crate::dialect::*;
use crate::parser::{IsOptional::*, Parser, ParserError};
use crate::tokenizer::Token;

impl<'a> Parser<'a> {
    /// Parse a SQL `CREATE DATABASE` statement
    pub fn parse_create_database(&mut self) -> Result<Statement, ParserError> {
        let ine = self.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let db_name = self.parse_object_name()?;
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

    /// Parse a SQL `CREATE TABLE` statement
    pub fn parse_create_table(
        &mut self,
        or_replace: bool,
        temporary: bool,
    ) -> Result<Statement, ParserError> {
        let if_not_exists = self.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let table_name = self.parse_object_name()?;
        let like = if self.parse_keyword(Keyword::LIKE) || self.parse_keyword(Keyword::ILIKE) {
            self.parse_object_name().ok()
        } else {
            None
        };
        // parse optional column list (schema)
        let (columns, constraints) = self.parse_columns()?;

        // SQLite supports `WITHOUT ROWID` at the end of `CREATE TABLE`
        let without_rowid = self.parse_keywords(&[Keyword::WITHOUT, Keyword::ROWID]);

        let hive_distribution = self.parse_hive_distribution()?;
        let hive_formats = self.parse_hive_formats()?;
        // PostgreSQL supports `WITH ( options )`, before `AS`
        let with_options = self.parse_options(Keyword::WITH)?;
        let table_properties = self.parse_options(Keyword::TBLPROPERTIES)?;
        // Parse optional `AS ( query )`
        let query = if self.parse_keyword(Keyword::AS) {
            Some(Box::new(self.parse_query()?))
        } else {
            None
        };

        Ok(Statement::CreateTable {
            name: table_name,
            temporary,
            columns,
            constraints,
            with_options,
            table_properties,
            or_replace,
            if_not_exists,
            hive_distribution,
            hive_formats: Some(hive_formats),
            external: false,
            file_format: None,
            location: None,
            query,
            without_rowid,
            like,
        })
    }

    /// Parse a SQL `CREATE EXTERNAL TABLE` statement
    pub fn parse_create_external_table(
        &mut self,
        or_replace: bool,
    ) -> Result<Statement, ParserError> {
        self.expect_keyword(Keyword::TABLE)?;
        let if_not_exists = self.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let table_name = self.parse_object_name()?;
        let (columns, constraints) = self.parse_columns()?;

        let hive_distribution = self.parse_hive_distribution()?;
        let hive_formats = self.parse_hive_formats()?;

        let file_format = if let Some(ff) = &hive_formats.storage {
            match ff {
                HiveIOFormat::FileFormat { format } => Some(format.clone()),
                _ => None,
            }
        } else {
            None
        };
        let location = hive_formats.location.clone();
        let table_properties = self.parse_options(Keyword::TBLPROPERTIES)?;
        Ok(Statement::CreateTable {
            name: table_name,
            columns,
            constraints,
            hive_distribution,
            hive_formats: Some(hive_formats),
            with_options: vec![],
            table_properties,
            or_replace,
            if_not_exists,
            external: true,
            temporary: false,
            file_format,
            location,
            query: None,
            without_rowid: false,
            like: None,
        })
    }

    /// SQLite-specific `CREATE VIRTUAL TABLE` statement
    pub fn parse_create_virtual_table(&mut self) -> Result<Statement, ParserError> {
        self.expect_keyword(Keyword::TABLE)?;
        let if_not_exists = self.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let table_name = self.parse_object_name()?;
        self.expect_keyword(Keyword::USING)?;
        let module_name = self.parse_identifier()?;
        // SQLite docs note that module "arguments syntax is sufficiently
        // general that the arguments can be made to appear as column
        // definitions in a traditional CREATE TABLE statement", but
        // we don't implement that.
        let module_args = self.parse_parenthesized_column_list(Optional)?;
        Ok(Statement::CreateVirtualTable {
            name: table_name,
            if_not_exists,
            module_name,
            module_args,
        })
    }

    /// Parse a SQL `CREATE MATERIALIZED` or `CREATE VIEW` statement
    pub fn parse_create_view(&mut self, or_replace: bool) -> Result<Statement, ParserError> {
        let materialized = self.parse_keyword(Keyword::MATERIALIZED);
        self.expect_keyword(Keyword::VIEW)?;
        // Many dialects support `OR ALTER` right after `CREATE`, but we don't (yet).
        // ANSI SQL and Postgres support RECURSIVE here, but we don't support it either.
        let name = self.parse_object_name()?;
        let columns = self.parse_parenthesized_column_list(Optional)?;
        let with_options = self.parse_options(Keyword::WITH)?;
        self.expect_keyword(Keyword::AS)?;
        let query = Box::new(self.parse_query()?);
        // Optional `WITH [ CASCADED | LOCAL ] CHECK OPTION` is widely supported here.
        Ok(Statement::CreateView {
            name,
            columns,
            query,
            materialized,
            or_replace,
            with_options,
        })
    }

    fn parse_columns(&mut self) -> Result<(Vec<ColumnDef>, Vec<TableConstraint>), ParserError> {
        let mut columns = vec![];
        let mut constraints = vec![];
        if !self.consume_token(&Token::LParen) || self.consume_token(&Token::RParen) {
            return Ok((columns, constraints));
        }

        loop {
            if let Some(constraint) = self.parse_optional_table_constraint()? {
                constraints.push(constraint);
            } else if let Token::Word(_) = self.peek_token() {
                columns.push(self.parse_column_def()?);
            } else {
                return self.expected("column name or constraint definition", self.peek_token());
            }
            let comma = self.consume_token(&Token::Comma);
            if self.consume_token(&Token::RParen) {
                // allow a trailing comma, even though it's not in standard
                break;
            } else if !comma {
                return self.expected("',' or ')' after column definition", self.peek_token());
            }
        }

        Ok((columns, constraints))
    }

    pub(super) fn parse_column_def(&mut self) -> Result<ColumnDef, ParserError> {
        let name = self.parse_identifier()?;
        let data_type = self.parse_data_type()?;
        let collation = if self.parse_keyword(Keyword::COLLATE) {
            Some(self.parse_object_name()?)
        } else {
            None
        };
        let mut options = vec![];
        loop {
            if self.parse_keyword(Keyword::CONSTRAINT) {
                let name = Some(self.parse_identifier()?);
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

    pub fn parse_optional_column_option(&mut self) -> Result<Option<ColumnOption>, ParserError> {
        if self.parse_keywords(&[Keyword::NOT, Keyword::NULL]) {
            Ok(Some(ColumnOption::NotNull))
        } else if self.parse_keyword(Keyword::NULL) {
            Ok(Some(ColumnOption::Null))
        } else if self.parse_keyword(Keyword::DEFAULT) {
            Ok(Some(ColumnOption::Default(self.parse_expr()?)))
        } else if self.parse_keywords(&[Keyword::PRIMARY, Keyword::KEY]) {
            Ok(Some(ColumnOption::Unique { is_primary: true }))
        } else if self.parse_keyword(Keyword::UNIQUE) {
            Ok(Some(ColumnOption::Unique { is_primary: false }))
        } else if self.parse_keyword(Keyword::REFERENCES) {
            let foreign_table = self.parse_object_name()?;
            // PostgreSQL allows omitting the column list and
            // uses the primary key column of the foreign table by default
            let referred_columns = self.parse_parenthesized_column_list(Optional)?;
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
            Ok(Some(ColumnOption::ForeignKey {
                foreign_table,
                referred_columns,
                on_delete,
                on_update,
            }))
        } else if self.parse_keyword(Keyword::CHECK) {
            self.expect_token(&Token::LParen)?;
            let expr = self.parse_expr()?;
            self.expect_token(&Token::RParen)?;
            Ok(Some(ColumnOption::Check(expr)))
        } else if self.parse_keyword(Keyword::AUTO_INCREMENT)
            && dialect_of!(self is MySqlDialect |  GenericDialect)
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
        } else {
            Ok(None)
        }
    }

    pub fn parse_optional_table_constraint(
        &mut self,
    ) -> Result<Option<TableConstraint>, ParserError> {
        let name = if self.parse_keyword(Keyword::CONSTRAINT) {
            Some(self.parse_identifier()?)
        } else {
            None
        };
        match self.next_token() {
            Token::Word(w) if w.keyword == Keyword::PRIMARY || w.keyword == Keyword::UNIQUE => {
                let is_primary = w.keyword == Keyword::PRIMARY;
                if is_primary {
                    self.expect_keyword(Keyword::KEY)?;
                }
                let columns = self.parse_parenthesized_column_list(Mandatory)?;
                Ok(Some(TableConstraint::Unique {
                    name,
                    columns,
                    is_primary,
                }))
            }
            Token::Word(w) if w.keyword == Keyword::FOREIGN => {
                self.expect_keyword(Keyword::KEY)?;
                let columns = self.parse_parenthesized_column_list(Mandatory)?;
                self.expect_keyword(Keyword::REFERENCES)?;
                let foreign_table = self.parse_object_name()?;
                let referred_columns = self.parse_parenthesized_column_list(Mandatory)?;
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
                Ok(Some(TableConstraint::ForeignKey {
                    name,
                    columns,
                    foreign_table,
                    referred_columns,
                    on_delete,
                    on_update,
                }))
            }
            Token::Word(w) if w.keyword == Keyword::CHECK => {
                self.expect_token(&Token::LParen)?;
                let expr = Box::new(self.parse_expr()?);
                self.expect_token(&Token::RParen)?;
                Ok(Some(TableConstraint::Check { name, expr }))
            }
            unexpected => {
                if name.is_some() {
                    self.expected("PRIMARY, UNIQUE, FOREIGN, or CHECK", unexpected)
                } else {
                    self.prev_token();
                    Ok(None)
                }
            }
        }
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

    pub fn parse_options(&mut self, keyword: Keyword) -> Result<Vec<SqlOption>, ParserError> {
        if self.parse_keyword(keyword) {
            self.expect_token(&Token::LParen)?;
            let options = self.parse_comma_separated(Parser::parse_sql_option)?;
            self.expect_token(&Token::RParen)?;
            Ok(options)
        } else {
            Ok(vec![])
        }
    }

    pub fn parse_sql_option(&mut self) -> Result<SqlOption, ParserError> {
        let name = self.parse_identifier()?;
        self.expect_token(&Token::Eq)?;
        let value = self.parse_value()?;
        Ok(SqlOption { name, value })
    }

    /// Parse a SQL `CREATE INDEX` statement
    pub fn parse_create_index(&mut self, unique: bool) -> Result<Statement, ParserError> {
        let if_not_exists = self.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let index_name = self.parse_object_name()?;
        self.expect_keyword(Keyword::ON)?;
        let table_name = self.parse_object_name()?;
        self.expect_token(&Token::LParen)?;
        let columns = self.parse_comma_separated(Parser::parse_order_by_expr)?;
        self.expect_token(&Token::RParen)?;
        Ok(Statement::CreateIndex {
            name: index_name,
            table_name,
            columns,
            unique,
            if_not_exists,
        })
    }

    /// Parse a SQL `CREATE SCHEMA` statement
    pub fn parse_create_schema(&mut self) -> Result<Statement, ParserError> {
        let if_not_exists = self.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let schema_name = self.parse_object_name()?;
        Ok(Statement::CreateSchema {
            schema_name,
            if_not_exists,
        })
    }

    pub(super) fn parse_show_create(&mut self) -> Result<Statement, ParserError> {
        let obj_type = match self.expect_one_of_keywords(&[
            Keyword::TABLE,
            Keyword::TRIGGER,
            Keyword::FUNCTION,
            Keyword::PROCEDURE,
            Keyword::EVENT,
        ])? {
            Keyword::TABLE => Ok(ShowCreateObject::Table),
            Keyword::TRIGGER => Ok(ShowCreateObject::Trigger),
            Keyword::FUNCTION => Ok(ShowCreateObject::Function),
            Keyword::PROCEDURE => Ok(ShowCreateObject::Procedure),
            Keyword::EVENT => Ok(ShowCreateObject::Event),
            keyword => Err(ParserError::ParserError(format!(
                "Unable to map keyword to ShowCreateObject: {:?}",
                keyword
            ))),
        }?;

        let obj_name = self.parse_object_name()?;

        Ok(Statement::ShowCreate { obj_type, obj_name })
    }

    pub(super) fn parse_show_columns(&mut self) -> Result<Statement, ParserError> {
        let extended = self.parse_keyword(Keyword::EXTENDED);
        let full = self.parse_keyword(Keyword::FULL);
        self.expect_one_of_keywords(&[Keyword::COLUMNS, Keyword::FIELDS])?;
        self.expect_one_of_keywords(&[Keyword::FROM, Keyword::IN])?;
        let table_name = self.parse_object_name()?;
        // MySQL also supports FROM <database> here. In other words, MySQL
        // allows both FROM <table> FROM <database> and FROM <database>.<table>,
        // while we only support the latter for now.
        let filter = self.parse_show_statement_filter()?;
        Ok(Statement::ShowColumns {
            extended,
            full,
            table_name,
            filter,
        })
    }

    fn parse_show_statement_filter(&mut self) -> Result<Option<ShowStatementFilter>, ParserError> {
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
            Ok(None)
        }
    }

    pub(super) fn parse_show_variable(&mut self) -> Result<Statement, ParserError> {
        Ok(Statement::ShowVariable {
            variable: self.parse_identifiers()?,
        })
    }

    fn parse_identifiers(&mut self) -> Result<Vec<Ident>, ParserError> {
        let mut idents = vec![];
        loop {
            match self.next_token() {
                Token::Word(w) => idents.push(w.to_ident()),
                Token::EOF => break,
                _ => {}
            }
        }
        Ok(idents)
    }

    //TODO: Implement parsing for Skewed and Clustered
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

    pub fn parse_hive_formats(&mut self) -> Result<HiveFormat, ParserError> {
        let mut hive_format = HiveFormat::default();
        loop {
            match self.parse_one_of_keywords(&[Keyword::ROW, Keyword::STORED, Keyword::LOCATION]) {
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
            _ => Ok(HiveRowFormat::DELIMITED),
        }
    }
}

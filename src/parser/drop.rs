use super::*;

use crate::parser_err;

impl<'a> Parser<'a> {
    /// Parse statements of the DropTrigger type such as:
    ///
    /// ```sql
    /// DROP TRIGGER [ IF EXISTS ] name ON table_name [ CASCADE | RESTRICT ]
    /// ```
    pub fn parse_drop_trigger(&mut self) -> Result<Statement, ParserError> {
        if !dialect_of!(self is PostgreSqlDialect | GenericDialect) {
            self.prev_token();
            return self.expected("an object type after DROP", self.peek_token());
        }
        let if_exists = self.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
        let trigger_name = self.parse_object_name(false)?;
        self.expect_keyword(Keyword::ON)?;
        let table_name = self.parse_object_name(false)?;
        let option = self
            .parse_one_of_keywords(&[Keyword::CASCADE, Keyword::RESTRICT])
            .map(|keyword| match keyword {
                Keyword::CASCADE => ReferentialAction::Cascade,
                Keyword::RESTRICT => ReferentialAction::Restrict,
                _ => unreachable!(),
            });
        Ok(Statement::DropTrigger {
            if_exists,
            trigger_name,
            table_name,
            option,
        })
    }

    pub fn parse_drop(&mut self) -> Result<Statement, ParserError> {
        // MySQL dialect supports `TEMPORARY`
        let temporary = dialect_of!(self is MySqlDialect | GenericDialect | DuckDbDialect)
            && self.parse_keyword(Keyword::TEMPORARY);
        let persistent = dialect_of!(self is DuckDbDialect)
            && self.parse_one_of_keywords(&[Keyword::PERSISTENT]).is_some();

        let object_type = if self.parse_keyword(Keyword::TABLE) {
            ObjectType::Table
        } else if self.parse_keyword(Keyword::VIEW) {
            ObjectType::View
        } else if self.parse_keyword(Keyword::INDEX) {
            ObjectType::Index
        } else if self.parse_keyword(Keyword::ROLE) {
            ObjectType::Role
        } else if self.parse_keyword(Keyword::SCHEMA) {
            ObjectType::Schema
        } else if self.parse_keyword(Keyword::DATABASE) {
            ObjectType::Database
        } else if self.parse_keyword(Keyword::SEQUENCE) {
            ObjectType::Sequence
        } else if self.parse_keyword(Keyword::STAGE) {
            ObjectType::Stage
        } else if self.parse_keyword(Keyword::TYPE) {
            ObjectType::Type
        } else if self.parse_keyword(Keyword::FUNCTION) {
            return self.parse_drop_function();
        } else if self.parse_keyword(Keyword::POLICY) {
            return self.parse_drop_policy();
        } else if self.parse_keyword(Keyword::PROCEDURE) {
            return self.parse_drop_procedure();
        } else if self.parse_keyword(Keyword::SECRET) {
            return self.parse_drop_secret(temporary, persistent);
        } else if self.parse_keyword(Keyword::TRIGGER) {
            return self.parse_drop_trigger();
        } else {
            return self.expected(
                "TABLE, VIEW, INDEX, ROLE, SCHEMA, DATABASE, FUNCTION, PROCEDURE, STAGE, TRIGGER, SECRET, SEQUENCE, or TYPE after DROP",
                self.peek_token(),
            );
        };
        // Many dialects support the non-standard `IF EXISTS` clause and allow
        // specifying multiple objects to delete in a single statement
        let if_exists = self.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
        let names = self.parse_comma_separated(|p| p.parse_object_name(false))?;

        let loc = self.peek_token().span.start;
        let cascade = self.parse_keyword(Keyword::CASCADE);
        let restrict = self.parse_keyword(Keyword::RESTRICT);
        let purge = self.parse_keyword(Keyword::PURGE);
        if cascade && restrict {
            return parser_err!("Cannot specify both CASCADE and RESTRICT in DROP", loc);
        }
        if object_type == ObjectType::Role && (cascade || restrict || purge) {
            return parser_err!(
                "Cannot specify CASCADE, RESTRICT, or PURGE in DROP ROLE",
                loc
            );
        }
        Ok(Statement::Drop {
            object_type,
            if_exists,
            names,
            cascade,
            restrict,
            purge,
            temporary,
        })
    }

    fn parse_optional_referential_action(&mut self) -> Option<ReferentialAction> {
        match self.parse_one_of_keywords(&[Keyword::CASCADE, Keyword::RESTRICT]) {
            Some(Keyword::CASCADE) => Some(ReferentialAction::Cascade),
            Some(Keyword::RESTRICT) => Some(ReferentialAction::Restrict),
            _ => None,
        }
    }

    /// ```sql
    /// DROP FUNCTION [ IF EXISTS ] name [ ( [ [ argmode ] [ argname ] argtype [, ...] ] ) ] [, ...]
    /// [ CASCADE | RESTRICT ]
    /// ```
    fn parse_drop_function(&mut self) -> Result<Statement, ParserError> {
        let if_exists = self.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
        let func_desc = self.parse_comma_separated(Parser::parse_function_desc)?;
        let option = self.parse_optional_referential_action();
        Ok(Statement::DropFunction {
            if_exists,
            func_desc,
            option,
        })
    }

    /// ```sql
    /// DROP POLICY [ IF EXISTS ] name ON table_name [ CASCADE | RESTRICT ]
    /// ```
    ///
    /// [PostgreSQL Documentation](https://www.postgresql.org/docs/current/sql-droppolicy.html)
    fn parse_drop_policy(&mut self) -> Result<Statement, ParserError> {
        let if_exists = self.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
        let name = self.parse_identifier(false)?;
        self.expect_keyword(Keyword::ON)?;
        let table_name = self.parse_object_name(false)?;
        let option = self.parse_optional_referential_action();
        Ok(Statement::DropPolicy {
            if_exists,
            name,
            table_name,
            option,
        })
    }

    /// ```sql
    /// DROP PROCEDURE [ IF EXISTS ] name [ ( [ [ argmode ] [ argname ] argtype [, ...] ] ) ] [, ...]
    /// [ CASCADE | RESTRICT ]
    /// ```
    fn parse_drop_procedure(&mut self) -> Result<Statement, ParserError> {
        let if_exists = self.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
        let proc_desc = self.parse_comma_separated(Parser::parse_function_desc)?;
        let option = self.parse_optional_referential_action();
        Ok(Statement::DropProcedure {
            if_exists,
            proc_desc,
            option,
        })
    }

    /// See [DuckDB Docs](https://duckdb.org/docs/sql/statements/create_secret.html) for more details.
    fn parse_drop_secret(
        &mut self,
        temporary: bool,
        persistent: bool,
    ) -> Result<Statement, ParserError> {
        let if_exists = self.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
        let name = self.parse_identifier(false)?;
        let storage_specifier = if self.parse_keyword(Keyword::FROM) {
            self.parse_identifier(false).ok()
        } else {
            None
        };
        let temp = match (temporary, persistent) {
            (true, false) => Some(true),
            (false, true) => Some(false),
            (false, false) => None,
            _ => self.expected("TEMPORARY or PERSISTENT", self.peek_token())?,
        };

        Ok(Statement::DropSecret {
            if_exists,
            temporary: temp,
            name,
            storage_specifier,
        })
    }
}

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

//! SQL Parser for role

use super::{Parser, ParserError};
use crate::{
    ast::{AlterRoleOperation, RoleOption, Statement},
    dialect::{MsSqlDialect, PostgreSqlDialect},
    keywords::Keyword,
    tokenizer::Token,
};

impl<'a> Parser<'a> {
    pub fn parse_alter_role(&mut self) -> Result<Statement, ParserError> {
        if dialect_of!(self is PostgreSqlDialect) {
            return self.parse_pg_alter_role();
        } else if dialect_of!(self is MsSqlDialect) {
            return self.parse_mssql_alter_role();
        }

        Err(ParserError::ParserError(
            "ALTER ROLE is only support for PostgreSqlDialect, MsSqlDialect".to_string(),
        ))
    }

    fn parse_mssql_alter_role(&mut self) -> Result<Statement, ParserError> {
        let role_name = self.parse_object_name()?;

        let operation = if self.parse_keywords(&[Keyword::ADD, Keyword::MEMBER]) {
            let member_name = self.parse_object_name()?;
            AlterRoleOperation::AddMember { member_name }
        } else if self.parse_keywords(&[Keyword::DROP, Keyword::MEMBER]) {
            let member_name = self.parse_object_name()?;
            AlterRoleOperation::DropMember { member_name }
        } else if self.parse_keywords(&[Keyword::WITH, Keyword::NAME]) {
            if self.consume_token(&Token::Eq) {
                let role_name = self.parse_object_name()?;
                AlterRoleOperation::RenameRole { role_name }
            } else {
                return self.expected("= after WITH NAME ", self.peek_token());
            }
        } else {
            return self.expected("TO after RENAME", self.peek_token());
        };

        Ok(Statement::AlterRole {
            name: role_name,
            operation,
        })
    }

    fn parse_pg_alter_role(&mut self) -> Result<Statement, ParserError> {
        let role_name = self.parse_object_name()?;

        // [ IN DATABASE _`database_name`_ ]
        let in_database = if self.parse_keywords(&[Keyword::IN, Keyword::DATABASE]) {
            self.parse_object_name().ok()
        } else {
            None
        };

        let operation = if self.parse_keyword(Keyword::RENAME) {
            if self.parse_keyword(Keyword::TO) {
                let role_name = self.parse_object_name()?;
                AlterRoleOperation::RenameRole { role_name }
            } else {
                return self.expected("TO after RENAME", self.peek_token());
            }
        // SET
        } else if self.parse_keyword(Keyword::SET) {
            let config_param = self.parse_object_name()?;
            // FROM CURRENT
            if self.parse_keywords(&[Keyword::FROM, Keyword::CURRENT]) {
                AlterRoleOperation::Set {
                    config_param,
                    default_value: false,
                    from_current: true,
                    in_database,
                    value: None,
                }
            // { TO | = } { value | DEFAULT }
            } else if self.consume_token(&Token::Eq) || self.parse_keyword(Keyword::TO) {
                if self.parse_keyword(Keyword::DEFAULT) {
                    AlterRoleOperation::Set {
                        config_param,
                        default_value: true,
                        from_current: false,
                        in_database,
                        value: None,
                    }
                } else if let Ok(expr) = self.parse_expr() {
                    AlterRoleOperation::Set {
                        config_param,
                        default_value: false,
                        from_current: false,
                        in_database,
                        value: Some(expr),
                    }
                } else {
                    self.expected("variable value", self.peek_token())?
                }
            } else {
                self.expected("variable value", self.peek_token())?
            }
        // RESET
        } else if self.parse_keyword(Keyword::RESET) {
            if self.parse_keyword(Keyword::ALL) {
                AlterRoleOperation::Reset {
                    config_param: None,
                    all: true,
                }
            } else {
                let config_param = self.parse_object_name()?;
                AlterRoleOperation::Reset {
                    config_param: Some(config_param),
                    all: false,
                }
            }
        // option
        } else {
            // [ WITH ]
            let _ = self.parse_keyword(Keyword::WITH);
            // option
            let mut options = vec![];
            while let Some(opt) = self.maybe_parse(|parser| parser.parse_pg_role_option()) {
                options.push(opt);
            }
            AlterRoleOperation::WithOptions { options }
        };

        Ok(Statement::AlterRole {
            name: role_name,
            operation,
        })
    }

    fn parse_pg_role_option(&mut self) -> Result<RoleOption, ParserError> {
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
            Some(Keyword::BYPASSRLS) => RoleOption::BypassRls(true),
            Some(Keyword::NOBYPASSRLS) => RoleOption::BypassRls(false),
            Some(Keyword::CREATEDB) => RoleOption::CreateDB(true),
            Some(Keyword::NOCREATEDB) => RoleOption::CreateDB(false),
            _ => self.expected("option", self.peek_token())?,
        };

        Ok(option)
    }
}

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
    keywords::Keyword,
};

impl<'a> Parser<'a> {
    pub fn parse_alter_role(&mut self) -> Result<Statement, ParserError> {
        let role_name = self.parse_object_name()?;

        // [ IN DATABASE _`database_name`_ ]
        let database_name = if self.parse_keywords(&[Keyword::IN, Keyword::DATABASE]) {
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
        // } else if self.parse_keyword(Keyword::SET) {
        //     let config_param = self.parse_identifier()?;
        // self.parse_one_of_keywords(&[Keyword::TO, Keyword::QUALIFY]);
        // AlterRoleOperation::RenameRole { role_name }
        } else {
            // [ WITH ]
            let _ = self.parse_keyword(Keyword::WITH);
            // option
            let mut options = vec![];
            while let Some(opt) = self.maybe_parse(|parser| parser.parse_role_option()) {
                options.push(opt);
            }
            println!("with options: {options:#?}");
            AlterRoleOperation::WithOptions { options }
        };

        Ok(Statement::AlterRole {
            name: role_name,
            operation,
        })
    }

    fn parse_role_option(&mut self) -> Result<RoleOption, ParserError> {
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

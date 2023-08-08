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

//! AST types specific to GRANT/REVOKE/ROLE variants of [`Statement`](crate::ast::Statement)
//! (commonly referred to as Data Control Language, or DCL)

#[cfg(not(feature = "std"))]
use alloc::{boxed::Box, string::String, vec::Vec};
use core::fmt;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

#[cfg(feature = "visitor")]
use sqlparser_derive::{Visit, VisitMut};

use super::Expr;
use crate::ast::{display_separated, ObjectName};

/// An option in `ROLE` statement.
///
/// <https://www.postgresql.org/docs/current/sql-createrole.html>
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum RoleOption {
    SuperUser(bool),
    CreateDB(bool),
    BypassRls(bool),
}

impl fmt::Display for RoleOption {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            RoleOption::SuperUser(value) => {
                write!(f, "{}", if *value { "SUPERUSER" } else { "NOSUPERUSER" })
            }
            RoleOption::CreateDB(value) => {
                write!(f, "{}", if *value { "CREATEDB" } else { "NOCREATEDB" })
            }
            RoleOption::BypassRls(value) => {
                write!(f, "{}", if *value { "BYPASSRLS" } else { "NOBYPASSRLS" })
            }
        }
    }
}

/// An `ALTER ROLE` (`Statement::AlterRole`) operation
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum AlterRoleOperation {
    /// Generic
    RenameRole {
        role_name: ObjectName,
    },
    /// MS SQL Server
    /// <https://learn.microsoft.com/en-us/sql/t-sql/statements/alter-role-transact-sql>
    AddMember {
        member_name: ObjectName,
    },
    DropMember {
        member_name: ObjectName,
    },
    /// PostgreSQL
    /// <https://www.postgresql.org/docs/current/sql-alterrole.html>
    WithOptions {
        options: Vec<RoleOption>,
    },
    Set {
        config_param: ObjectName,
        default_value: bool,
        from_current: bool,
        in_database: Option<ObjectName>,
        value: Option<Expr>,
    },
    Reset {
        config_param: Option<ObjectName>,
        all: bool,
    },
}

impl fmt::Display for AlterRoleOperation {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            AlterRoleOperation::RenameRole { role_name } => {
                write!(f, "RENAME TO {role_name}")
            }
            AlterRoleOperation::AddMember { member_name } => {
                write!(f, "ADD MEMBER {member_name}")
            }
            AlterRoleOperation::DropMember { member_name } => {
                write!(f, "DROP MEMBER {member_name}")
            }
            AlterRoleOperation::WithOptions { options } => {
                write!(f, "WITH {}", display_separated(options, " "))
            }
            AlterRoleOperation::Set {
                config_param,
                default_value,
                from_current,
                in_database,
                value,
            } => {
                if let Some(database_name) = in_database {
                    write!(f, "IN DATABASE {} ", database_name)?;
                }

                if *from_current {
                    write!(f, "SET {config_param} FROM CURRENT")
                } else if *default_value {
                    write!(f, "SET {config_param} TO DEFAULT")
                } else {
                    write!(
                        f,
                        "SET {config_param} TO {value}",
                        value = if let Some(value) = value {
                            value.to_string()
                        } else {
                            "".to_string()
                        }
                    )
                }
            }
            AlterRoleOperation::Reset { config_param, all } => {
                if *all {
                    write!(f, "RESET ALL")
                } else {
                    write!(
                        f,
                        "RESET {param}",
                        param = if let Some(param) = config_param {
                            param.to_string()
                        } else {
                            "".to_string()
                        }
                    )
                }
            }
        }
    }
}

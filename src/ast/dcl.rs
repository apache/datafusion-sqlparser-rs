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

use crate::ast::ObjectName;

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
    RenameRole { role_name: ObjectName },
    WithOptions { options: Vec<RoleOption> },
}

impl fmt::Display for AlterRoleOperation {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            AlterRoleOperation::RenameRole { role_name } => {
                write!(f, "RENAME TO {role_name}")
            }
            AlterRoleOperation::WithOptions { options } => {
                write!(
                    f,
                    "WITH {}",
                    options
                        .iter()
                        .map(|v| v.to_string())
                        .collect::<Vec<_>>()
                        .join(" ")
                )
            }
        }
    }
}

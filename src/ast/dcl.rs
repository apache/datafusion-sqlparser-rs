// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! AST types specific to GRANT/REVOKE/ROLE variants of [`Statement`](crate::ast::Statement)
//! (commonly referred to as Data Control Language, or DCL)

#[cfg(not(feature = "std"))]
use alloc::vec::Vec;
use core::fmt;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

#[cfg(feature = "visitor")]
use sqlparser_derive::{Visit, VisitMut};

use super::{display_comma_separated, Expr, Ident, Password, Spanned};
use crate::ast::{
    display_separated, CascadeOption, CurrentGrantsKind, GrantObjects, Grantee, ObjectName,
    Privileges,
};
use crate::tokenizer::Span;

/// An option in `ROLE` statement.
///
/// <https://www.postgresql.org/docs/current/sql-createrole.html>
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum RoleOption {
    /// Enable or disable BYPASSRLS.
    BypassRLS(bool),
    /// Connection limit expression.
    ConnectionLimit(Expr),
    /// CREATEDB flag.
    CreateDB(bool),
    /// CREATEROLE flag.
    CreateRole(bool),
    /// INHERIT flag.
    Inherit(bool),
    /// LOGIN flag.
    Login(bool),
    /// Password value or NULL password.
    Password(Password),
    /// Replication privilege flag.
    Replication(bool),
    /// SUPERUSER flag.
    SuperUser(bool),
    /// `VALID UNTIL` expression.
    ValidUntil(Expr),
}

impl fmt::Display for RoleOption {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            RoleOption::BypassRLS(value) => {
                write!(f, "{}", if *value { "BYPASSRLS" } else { "NOBYPASSRLS" })
            }
            RoleOption::ConnectionLimit(expr) => {
                write!(f, "CONNECTION LIMIT {expr}")
            }
            RoleOption::CreateDB(value) => {
                write!(f, "{}", if *value { "CREATEDB" } else { "NOCREATEDB" })
            }
            RoleOption::CreateRole(value) => {
                write!(f, "{}", if *value { "CREATEROLE" } else { "NOCREATEROLE" })
            }
            RoleOption::Inherit(value) => {
                write!(f, "{}", if *value { "INHERIT" } else { "NOINHERIT" })
            }
            RoleOption::Login(value) => {
                write!(f, "{}", if *value { "LOGIN" } else { "NOLOGIN" })
            }
            RoleOption::Password(password) => match password {
                Password::Password(expr) => write!(f, "PASSWORD {expr}"),
                Password::NullPassword => write!(f, "PASSWORD NULL"),
            },
            RoleOption::Replication(value) => {
                write!(
                    f,
                    "{}",
                    if *value {
                        "REPLICATION"
                    } else {
                        "NOREPLICATION"
                    }
                )
            }
            RoleOption::SuperUser(value) => {
                write!(f, "{}", if *value { "SUPERUSER" } else { "NOSUPERUSER" })
            }
            RoleOption::ValidUntil(expr) => {
                write!(f, "VALID UNTIL {expr}")
            }
        }
    }
}

/// SET config value option:
/// * SET `configuration_parameter` { TO | = } { `value` | DEFAULT }
/// * SET `configuration_parameter` FROM CURRENT
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum SetConfigValue {
    /// Use the default value.
    Default,
    /// Use the current value (`FROM CURRENT`).
    FromCurrent,
    /// Set to the provided expression value.
    Value(Expr),
}

/// RESET config option:
/// * RESET `configuration_parameter`
/// * RESET ALL
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum ResetConfig {
    /// Reset all configuration parameters.
    ALL,
    /// Reset the named configuration parameter.
    ConfigName(ObjectName),
}

/// An `ALTER ROLE` (`Statement::AlterRole`) operation
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum AlterRoleOperation {
    /// Generic
    RenameRole {
        /// Role name to rename.
        role_name: Ident,
    },
    /// MS SQL Server
    /// <https://learn.microsoft.com/en-us/sql/t-sql/statements/alter-role-transact-sql>
    AddMember {
        /// Member name to add to the role.
        member_name: Ident,
    },
    /// MS SQL Server
    ///
    /// <https://learn.microsoft.com/en-us/sql/t-sql/statements/alter-role-transact-sql>
    DropMember {
        /// Member name to remove from the role.
        member_name: Ident,
    },
    /// PostgreSQL
    /// <https://www.postgresql.org/docs/current/sql-alterrole.html>
    WithOptions {
        /// Role options to apply.
        options: Vec<RoleOption>,
    },
    /// PostgreSQL
    /// <https://www.postgresql.org/docs/current/sql-alterrole.html>
    ///
    /// `SET configuration_parameter { TO | = } { value | DEFAULT }`
    Set {
        /// Configuration name to set.
        config_name: ObjectName,
        /// Value to assign to the configuration.
        config_value: SetConfigValue,
        /// Optional database scope for the setting.
        in_database: Option<ObjectName>,
    },
    /// PostgreSQL
    /// <https://www.postgresql.org/docs/current/sql-alterrole.html>
    ///
    /// `RESET configuration_parameter` | `RESET ALL`
    Reset {
        /// Configuration to reset.
        config_name: ResetConfig,
        /// Optional database scope for the reset.
        in_database: Option<ObjectName>,
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
                config_name,
                config_value,
                in_database,
            } => {
                if let Some(database_name) = in_database {
                    write!(f, "IN DATABASE {database_name} ")?;
                }

                match config_value {
                    SetConfigValue::Default => write!(f, "SET {config_name} TO DEFAULT"),
                    SetConfigValue::FromCurrent => write!(f, "SET {config_name} FROM CURRENT"),
                    SetConfigValue::Value(expr) => write!(f, "SET {config_name} TO {expr}"),
                }
            }
            AlterRoleOperation::Reset {
                config_name,
                in_database,
            } => {
                if let Some(database_name) = in_database {
                    write!(f, "IN DATABASE {database_name} ")?;
                }

                match config_name {
                    ResetConfig::ALL => write!(f, "RESET ALL"),
                    ResetConfig::ConfigName(name) => write!(f, "RESET {name}"),
                }
            }
        }
    }
}

/// A `USE` (`Statement::Use`) operation
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum Use {
    /// Switch to the given catalog (e.g. `USE CATALOG ...`).
    Catalog(ObjectName),
    /// Switch to the given schema (e.g. `USE SCHEMA ...`).
    Schema(ObjectName),
    /// Switch to the given database (e.g. `USE DATABASE ...`).
    Database(ObjectName),
    /// Switch to the given warehouse (e.g. `USE WAREHOUSE ...`).
    Warehouse(ObjectName),
    /// Switch to the given role (e.g. `USE ROLE ...`).
    Role(ObjectName),
    /// Use secondary roles specification (e.g. `USE SECONDARY ROLES ...`).
    SecondaryRoles(SecondaryRoles),
    /// Use the specified object (e.g. `USE foo.bar`).
    Object(ObjectName),
    /// Reset to default (e.g. `USE DEFAULT`).
    Default,
}

impl fmt::Display for Use {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("USE ")?;
        match self {
            Use::Catalog(name) => write!(f, "CATALOG {name}"),
            Use::Schema(name) => write!(f, "SCHEMA {name}"),
            Use::Database(name) => write!(f, "DATABASE {name}"),
            Use::Warehouse(name) => write!(f, "WAREHOUSE {name}"),
            Use::Role(name) => write!(f, "ROLE {name}"),
            Use::SecondaryRoles(secondary_roles) => {
                write!(f, "SECONDARY ROLES {secondary_roles}")
            }
            Use::Object(name) => write!(f, "{name}"),
            Use::Default => write!(f, "DEFAULT"),
        }
    }
}

/// Snowflake `SECONDARY ROLES` USE variant
/// See: <https://docs.snowflake.com/en/sql-reference/sql/use-secondary-roles>
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum SecondaryRoles {
    /// Use all secondary roles.
    All,
    /// Use no secondary roles.
    None,
    /// Explicit list of secondary roles.
    List(Vec<Ident>),
}

impl fmt::Display for SecondaryRoles {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SecondaryRoles::All => write!(f, "ALL"),
            SecondaryRoles::None => write!(f, "NONE"),
            SecondaryRoles::List(roles) => write!(f, "{}", display_comma_separated(roles)),
        }
    }
}

/// CREATE ROLE statement
/// See [PostgreSQL](https://www.postgresql.org/docs/current/sql-createrole.html)
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct CreateRole {
    /// Role names to create.
    pub names: Vec<ObjectName>,
    /// Whether `IF NOT EXISTS` was specified.
    pub if_not_exists: bool,
    // Postgres
    /// Whether `LOGIN` was specified.
    pub login: Option<bool>,
    /// Whether `INHERIT` was specified.
    pub inherit: Option<bool>,
    /// Whether `BYPASSRLS` was specified.
    pub bypassrls: Option<bool>,
    /// Optional password for the role.
    pub password: Option<Password>,
    /// Whether `SUPERUSER` was specified.
    pub superuser: Option<bool>,
    /// Whether `CREATEDB` was specified.
    pub create_db: Option<bool>,
    /// Whether `CREATEROLE` was specified.
    pub create_role: Option<bool>,
    /// Whether `REPLICATION` privilege was specified.
    pub replication: Option<bool>,
    /// Optional connection limit expression.
    pub connection_limit: Option<Expr>,
    /// Optional account validity expression.
    pub valid_until: Option<Expr>,
    /// Members of `IN ROLE` clause.
    pub in_role: Vec<Ident>,
    /// Members of `IN GROUP` clause.
    pub in_group: Vec<Ident>,
    /// Roles listed in `ROLE` clause.
    pub role: Vec<Ident>,
    /// Users listed in `USER` clause.
    pub user: Vec<Ident>,
    /// Admin users listed in `ADMIN` clause.
    pub admin: Vec<Ident>,
    // MSSQL
    /// Optional authorization owner.
    pub authorization_owner: Option<ObjectName>,
}

impl fmt::Display for CreateRole {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "CREATE ROLE {if_not_exists}{names}{superuser}{create_db}{create_role}{inherit}{login}{replication}{bypassrls}",
            if_not_exists = if self.if_not_exists { "IF NOT EXISTS " } else { "" },
            names = display_separated(&self.names, ", "),
            superuser = match self.superuser {
                Some(true) => " SUPERUSER",
                Some(false) => " NOSUPERUSER",
                None => ""
            },
            create_db = match self.create_db {
                Some(true) => " CREATEDB",
                Some(false) => " NOCREATEDB",
                None => ""
            },
            create_role = match self.create_role {
                Some(true) => " CREATEROLE",
                Some(false) => " NOCREATEROLE",
                None => ""
            },
            inherit = match self.inherit {
                Some(true) => " INHERIT",
                Some(false) => " NOINHERIT",
                None => ""
            },
            login = match self.login {
                Some(true) => " LOGIN",
                Some(false) => " NOLOGIN",
                None => ""
            },
            replication = match self.replication {
                Some(true) => " REPLICATION",
                Some(false) => " NOREPLICATION",
                None => ""
            },
            bypassrls = match self.bypassrls {
                Some(true) => " BYPASSRLS",
                Some(false) => " NOBYPASSRLS",
                None => ""
            }
        )?;
        if let Some(limit) = &self.connection_limit {
            write!(f, " CONNECTION LIMIT {limit}")?;
        }
        match &self.password {
            Some(Password::Password(pass)) => write!(f, " PASSWORD {pass}")?,
            Some(Password::NullPassword) => write!(f, " PASSWORD NULL")?,
            None => {}
        };
        if let Some(until) = &self.valid_until {
            write!(f, " VALID UNTIL {until}")?;
        }
        if !self.in_role.is_empty() {
            write!(f, " IN ROLE {}", display_comma_separated(&self.in_role))?;
        }
        if !self.in_group.is_empty() {
            write!(f, " IN GROUP {}", display_comma_separated(&self.in_group))?;
        }
        if !self.role.is_empty() {
            write!(f, " ROLE {}", display_comma_separated(&self.role))?;
        }
        if !self.user.is_empty() {
            write!(f, " USER {}", display_comma_separated(&self.user))?;
        }
        if !self.admin.is_empty() {
            write!(f, " ADMIN {}", display_comma_separated(&self.admin))?;
        }
        if let Some(owner) = &self.authorization_owner {
            write!(f, " AUTHORIZATION {owner}")?;
        }
        Ok(())
    }
}

impl Spanned for CreateRole {
    fn span(&self) -> Span {
        Span::empty()
    }
}

/// GRANT privileges ON objects TO grantees
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct Grant {
    /// Privileges being granted.
    pub privileges: Privileges,
    /// Optional objects the privileges apply to.
    pub objects: Option<GrantObjects>,
    /// List of grantees receiving the privileges.
    pub grantees: Vec<Grantee>,
    /// Whether `WITH GRANT OPTION` is present.
    pub with_grant_option: bool,
    /// Optional `AS GRANTOR` identifier.
    pub as_grantor: Option<Ident>,
    /// Optional `GRANTED BY` identifier.
    ///
    /// [BigQuery](https://cloud.google.com/bigquery/docs/reference/standard-sql/dcl-statements)
    pub granted_by: Option<Ident>,
    /// Optional `CURRENT GRANTS` modifier.
    ///
    /// [Snowflake](https://docs.snowflake.com/en/sql-reference/sql/grant-privilege)
    pub current_grants: Option<CurrentGrantsKind>,
}

impl fmt::Display for Grant {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "GRANT {privileges}", privileges = self.privileges)?;
        if let Some(ref objects) = self.objects {
            write!(f, " ON {objects}")?;
        }
        write!(f, " TO {}", display_comma_separated(&self.grantees))?;
        if let Some(ref current_grants) = self.current_grants {
            write!(f, " {current_grants}")?;
        }
        if self.with_grant_option {
            write!(f, " WITH GRANT OPTION")?;
        }
        if let Some(ref as_grantor) = self.as_grantor {
            write!(f, " AS {as_grantor}")?;
        }
        if let Some(ref granted_by) = self.granted_by {
            write!(f, " GRANTED BY {granted_by}")?;
        }
        Ok(())
    }
}

impl From<Grant> for crate::ast::Statement {
    fn from(v: Grant) -> Self {
        crate::ast::Statement::Grant(v)
    }
}

/// REVOKE privileges ON objects FROM grantees
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct Revoke {
    /// Privileges to revoke.
    pub privileges: Privileges,
    /// Optional objects from which to revoke.
    pub objects: Option<GrantObjects>,
    /// Grantees affected by the revoke.
    pub grantees: Vec<Grantee>,
    /// Optional `GRANTED BY` identifier.
    ///
    /// [BigQuery](https://cloud.google.com/bigquery/docs/reference/standard-sql/dcl-statements)
    pub granted_by: Option<Ident>,
    /// Optional `CASCADE`/`RESTRICT` behavior.
    pub cascade: Option<CascadeOption>,
}

impl fmt::Display for Revoke {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "REVOKE {privileges}", privileges = self.privileges)?;
        if let Some(ref objects) = self.objects {
            write!(f, " ON {objects}")?;
        }
        write!(f, " FROM {}", display_comma_separated(&self.grantees))?;
        if let Some(ref granted_by) = self.granted_by {
            write!(f, " GRANTED BY {granted_by}")?;
        }
        if let Some(ref cascade) = self.cascade {
            write!(f, " {cascade}")?;
        }
        Ok(())
    }
}

impl From<Revoke> for crate::ast::Statement {
    fn from(v: Revoke) -> Self {
        crate::ast::Statement::Revoke(v)
    }
}

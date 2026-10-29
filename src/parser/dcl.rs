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

//! SQL Parser for DCL: `GRANT`, `REVOKE`, `DENY`, roles, users and policies

#[cfg(not(feature = "std"))]
use alloc::{format, vec, vec::Vec};

use super::{IsOptional::Optional, Parser, ParserError};
use crate::{
    ast::{
        helpers::key_value_options::{KeyValueOptions, KeyValueOptionsDelimiter},
        Action, ActionApplyType, ActionCreateObjectType, ActionExecuteObjectType, ActionManageType,
        ActionModifyType, ActionMonitorType, ContextModifier, CreatePolicy, CreatePolicyCommand,
        CreatePolicyType, CreateRole, CreateUser, CurrentGrantsKind, DenyStatement, DropPolicy,
        Expr, Grant, GrantObjects, GrantRole, Grantee, GranteeName, GranteesType, Ident,
        ObjectName, Password, Privileges, Revoke, RevokeRole, RoleGrantOption,
        RoleGrantOptionValue, RoleKeyword, SecondaryRoles, Set, Statement, Use,
    },
    dialect::{DatabricksDialect, HiveDialect, MsSqlDialect, PostgreSqlDialect, SnowflakeDialect},
    keywords::Keyword,
    tokenizer::{Location, Token},
};

impl Parser<'_> {
    /// Parse a GRANT statement.
    pub fn parse_grant(&mut self) -> Result<Grant, ParserError> {
        let (privileges, objects) = self.parse_grant_deny_revoke_privileges_objects()?;

        self.expect_keyword_is(Keyword::TO)?;
        let grantees = self.parse_grantees()?;

        let with_grant_option =
            self.parse_keywords(&[Keyword::WITH, Keyword::GRANT, Keyword::OPTION]);

        let current_grants =
            if self.parse_keywords(&[Keyword::COPY, Keyword::CURRENT, Keyword::GRANTS]) {
                Some(CurrentGrantsKind::CopyCurrentGrants)
            } else if self.parse_keywords(&[Keyword::REVOKE, Keyword::CURRENT, Keyword::GRANTS]) {
                Some(CurrentGrantsKind::RevokeCurrentGrants)
            } else {
                None
            };

        let as_grantor = if self.parse_keywords(&[Keyword::AS]) {
            Some(self.parse_identifier()?)
        } else {
            None
        };

        let granted_by = if self.parse_keywords(&[Keyword::GRANTED, Keyword::BY]) {
            Some(self.parse_identifier()?)
        } else {
            None
        };

        Ok(Grant {
            privileges,
            objects,
            grantees,
            with_grant_option,
            as_grantor,
            granted_by,
            current_grants,
        })
    }

    /// Parse a `GRANT` of role membership, after the `GRANT` keyword.
    pub fn parse_grant_role(&mut self) -> Result<GrantRole, ParserError> {
        let roles = self.parse_comma_separated(Parser::parse_identifier)?;
        self.expect_keyword_is(Keyword::TO)?;
        let grantees = self.parse_grantees()?;
        let options = if self.parse_keyword(Keyword::WITH) {
            self.parse_comma_separated(Parser::parse_role_grant_option)?
        } else {
            vec![]
        };
        let granted_by = if self.parse_keywords(&[Keyword::GRANTED, Keyword::BY]) {
            Some(self.parse_identifier()?)
        } else {
            None
        };
        Ok(GrantRole {
            roles,
            grantees,
            options,
            granted_by,
        })
    }

    fn parse_role_grant_option(&mut self) -> Result<RoleGrantOption, ParserError> {
        let name = self.parse_identifier()?;
        let value =
            match self.parse_one_of_keywords(&[Keyword::OPTION, Keyword::TRUE, Keyword::FALSE]) {
                Some(Keyword::OPTION) => RoleGrantOptionValue::Option,
                Some(Keyword::TRUE) => RoleGrantOptionValue::True,
                Some(Keyword::FALSE) => RoleGrantOptionValue::False,
                _ => return self.expected_ref("OPTION, TRUE or FALSE", self.peek_token_ref()),
            };
        Ok(RoleGrantOption { name, value })
    }

    /// Parses a statement body with `privileges`, and with `roles` instead when that fails and
    /// the body opens with a role list followed by `keyword`.
    pub(super) fn parse_privileges_or_roles<P: Into<Statement>, R: Into<Statement>>(
        &mut self,
        keyword: Keyword,
        privileges: fn(&mut Self) -> Result<P, ParserError>,
        roles: fn(&mut Self) -> Result<R, ParserError>,
    ) -> Result<Statement, ParserError> {
        let index = self.index;
        let privileges_err = match privileges(self) {
            Ok(statement) => return Ok(statement.into()),
            Err(err) => err,
        };
        self.index = index;
        let is_role_list = self.parse_comma_separated(Parser::parse_identifier).is_ok()
            && self.parse_keyword(keyword);
        self.index = index;
        if is_role_list {
            roles(self).map(Into::into)
        } else {
            Err(privileges_err)
        }
    }

    fn parse_grantees(&mut self) -> Result<Vec<Grantee>, ParserError> {
        let mut values = vec![];
        let mut grantee_type = GranteesType::None;
        loop {
            let new_grantee_type = if self.parse_keyword(Keyword::ROLE) {
                GranteesType::Role
            } else if self.parse_keyword(Keyword::USER) {
                GranteesType::User
            } else if self.parse_keyword(Keyword::SHARE) {
                GranteesType::Share
            } else if self.parse_keyword(Keyword::GROUP) {
                GranteesType::Group
            } else if self.parse_keyword(Keyword::PUBLIC) {
                GranteesType::Public
            } else if self.parse_keywords(&[Keyword::DATABASE, Keyword::ROLE]) {
                GranteesType::DatabaseRole
            } else if self.parse_keywords(&[Keyword::APPLICATION, Keyword::ROLE]) {
                GranteesType::ApplicationRole
            } else if self.parse_keyword(Keyword::APPLICATION) {
                GranteesType::Application
            } else if grantee_type == GranteesType::Public {
                // PUBLIC does not carry over to the next grantee.
                GranteesType::None
            } else {
                grantee_type.clone() // keep from previous iteraton, if not specified
            };

            if self
                .dialect
                .get_reserved_grantees_types()
                .contains(&new_grantee_type)
            {
                self.prev_token();
            } else {
                grantee_type = new_grantee_type;
            }

            let grantee = if grantee_type == GranteesType::Public {
                Grantee {
                    grantee_type: grantee_type.clone(),
                    name: None,
                }
            } else {
                let mut name = self.parse_grantee_name()?;
                if self.consume_token(&Token::Colon) {
                    // Redshift supports namespace prefix for external users and groups:
                    // <Namespace>:<GroupName> or <Namespace>:<UserName>
                    // https://docs.aws.amazon.com/redshift/latest/mgmt/redshift-iam-access-control-native-idp.html
                    let ident = self.parse_identifier()?;
                    if let GranteeName::ObjectName(namespace) = name {
                        name = GranteeName::ObjectName(ObjectName::from(vec![Ident::new(
                            format!("{namespace}:{ident}"),
                        )]));
                    };
                }
                Grantee {
                    grantee_type: grantee_type.clone(),
                    name: Some(name),
                }
            };

            values.push(grantee);

            if !self.consume_token(&Token::Comma) {
                break;
            }
        }

        Ok(values)
    }

    /// Parse privileges and optional target objects for GRANT/DENY/REVOKE statements.
    pub fn parse_grant_deny_revoke_privileges_objects(
        &mut self,
    ) -> Result<(Privileges, Option<GrantObjects>), ParserError> {
        let privileges = if self.parse_keyword(Keyword::ALL) {
            Privileges::All {
                with_privileges_keyword: self.parse_keyword(Keyword::PRIVILEGES),
            }
        } else {
            let actions = self.parse_actions_list()?;
            Privileges::Actions(actions)
        };

        let objects = if self.parse_keyword(Keyword::ON) {
            if self.parse_keywords(&[Keyword::ALL, Keyword::TABLES, Keyword::IN, Keyword::SCHEMA]) {
                Some(GrantObjects::AllTablesInSchema {
                    schemas: self.parse_comma_separated(|p| p.parse_object_name(false))?,
                })
            } else if self.parse_keywords(&[
                Keyword::ALL,
                Keyword::EXTERNAL,
                Keyword::TABLES,
                Keyword::IN,
                Keyword::SCHEMA,
            ]) {
                Some(GrantObjects::AllExternalTablesInSchema {
                    schemas: self.parse_comma_separated(|p| p.parse_object_name(false))?,
                })
            } else if self.parse_keywords(&[
                Keyword::ALL,
                Keyword::VIEWS,
                Keyword::IN,
                Keyword::SCHEMA,
            ]) {
                Some(GrantObjects::AllViewsInSchema {
                    schemas: self.parse_comma_separated(|p| p.parse_object_name(false))?,
                })
            } else if self.parse_keywords(&[
                Keyword::ALL,
                Keyword::MATERIALIZED,
                Keyword::VIEWS,
                Keyword::IN,
                Keyword::SCHEMA,
            ]) {
                Some(GrantObjects::AllMaterializedViewsInSchema {
                    schemas: self.parse_comma_separated(|p| p.parse_object_name(false))?,
                })
            } else if self.parse_keywords(&[
                Keyword::ALL,
                Keyword::FUNCTIONS,
                Keyword::IN,
                Keyword::SCHEMA,
            ]) {
                Some(GrantObjects::AllFunctionsInSchema {
                    schemas: self.parse_comma_separated(|p| p.parse_object_name(false))?,
                })
            } else if self.parse_keywords(&[
                Keyword::FUTURE,
                Keyword::SCHEMAS,
                Keyword::IN,
                Keyword::DATABASE,
            ]) {
                Some(GrantObjects::FutureSchemasInDatabase {
                    databases: self.parse_comma_separated(|p| p.parse_object_name(false))?,
                })
            } else if self.parse_keywords(&[
                Keyword::FUTURE,
                Keyword::TABLES,
                Keyword::IN,
                Keyword::SCHEMA,
            ]) {
                Some(GrantObjects::FutureTablesInSchema {
                    schemas: self.parse_comma_separated(|p| p.parse_object_name(false))?,
                })
            } else if self.parse_keywords(&[
                Keyword::FUTURE,
                Keyword::EXTERNAL,
                Keyword::TABLES,
                Keyword::IN,
                Keyword::SCHEMA,
            ]) {
                Some(GrantObjects::FutureExternalTablesInSchema {
                    schemas: self.parse_comma_separated(|p| p.parse_object_name(false))?,
                })
            } else if self.parse_keywords(&[
                Keyword::FUTURE,
                Keyword::VIEWS,
                Keyword::IN,
                Keyword::SCHEMA,
            ]) {
                Some(GrantObjects::FutureViewsInSchema {
                    schemas: self.parse_comma_separated(|p| p.parse_object_name(false))?,
                })
            } else if self.parse_keywords(&[
                Keyword::FUTURE,
                Keyword::MATERIALIZED,
                Keyword::VIEWS,
                Keyword::IN,
                Keyword::SCHEMA,
            ]) {
                Some(GrantObjects::FutureMaterializedViewsInSchema {
                    schemas: self.parse_comma_separated(|p| p.parse_object_name(false))?,
                })
            } else if self.parse_keywords(&[
                Keyword::ALL,
                Keyword::SEQUENCES,
                Keyword::IN,
                Keyword::SCHEMA,
            ]) {
                Some(GrantObjects::AllSequencesInSchema {
                    schemas: self.parse_comma_separated(|p| p.parse_object_name(false))?,
                })
            } else if self.parse_keywords(&[
                Keyword::FUTURE,
                Keyword::SEQUENCES,
                Keyword::IN,
                Keyword::SCHEMA,
            ]) {
                Some(GrantObjects::FutureSequencesInSchema {
                    schemas: self.parse_comma_separated(|p| p.parse_object_name(false))?,
                })
            } else if self.parse_keywords(&[Keyword::RESOURCE, Keyword::MONITOR]) {
                Some(GrantObjects::ResourceMonitors(
                    self.parse_comma_separated(|p| p.parse_object_name(false))?,
                ))
            } else if self.parse_keywords(&[Keyword::COMPUTE, Keyword::POOL]) {
                Some(GrantObjects::ComputePools(
                    self.parse_comma_separated(|p| p.parse_object_name(false))?,
                ))
            } else if self.parse_keywords(&[Keyword::FAILOVER, Keyword::GROUP]) {
                Some(GrantObjects::FailoverGroup(
                    self.parse_comma_separated(|p| p.parse_object_name(false))?,
                ))
            } else if self.parse_keywords(&[Keyword::REPLICATION, Keyword::GROUP]) {
                Some(GrantObjects::ReplicationGroup(
                    self.parse_comma_separated(|p| p.parse_object_name(false))?,
                ))
            } else if self.parse_keywords(&[Keyword::EXTERNAL, Keyword::VOLUME]) {
                Some(GrantObjects::ExternalVolumes(
                    self.parse_comma_separated(|p| p.parse_object_name(false))?,
                ))
            } else if self.parse_keyword(Keyword::FUNCTION) {
                Some(GrantObjects::Functions(
                    self.parse_comma_separated(Parser::parse_function_desc)?,
                ))
            } else if self.parse_keyword(Keyword::PROCEDURE) {
                Some(GrantObjects::Procedures(
                    self.parse_comma_separated(Parser::parse_function_desc)?,
                ))
            } else {
                let object_type = self.parse_one_of_keywords(&[
                    Keyword::SEQUENCE,
                    Keyword::DATABASE,
                    Keyword::SCHEMA,
                    Keyword::TABLE,
                    Keyword::VIEW,
                    Keyword::WAREHOUSE,
                    Keyword::INTEGRATION,
                    Keyword::VIEW,
                    Keyword::WAREHOUSE,
                    Keyword::INTEGRATION,
                    Keyword::USER,
                    Keyword::CONNECTION,
                ]);
                let objects =
                    self.parse_comma_separated(|p| p.parse_object_name_inner(false, true));
                match object_type {
                    Some(Keyword::DATABASE) => Some(GrantObjects::Databases(objects?)),
                    Some(Keyword::SCHEMA) => Some(GrantObjects::Schemas(objects?)),
                    Some(Keyword::SEQUENCE) => Some(GrantObjects::Sequences(objects?)),
                    Some(Keyword::WAREHOUSE) => Some(GrantObjects::Warehouses(objects?)),
                    Some(Keyword::INTEGRATION) => Some(GrantObjects::Integrations(objects?)),
                    Some(Keyword::VIEW) => Some(GrantObjects::Views(objects?)),
                    Some(Keyword::USER) => Some(GrantObjects::Users(objects?)),
                    Some(Keyword::CONNECTION) => Some(GrantObjects::Connections(objects?)),
                    Some(Keyword::TABLE) | None => Some(GrantObjects::Tables(objects?)),
                    Some(unexpected_keyword) => return Err(ParserError::ParserError(
                        format!("Internal parser error: unexpected keyword `{unexpected_keyword}` in grant objects"),
                    )),
                }
            }
        } else {
            None
        };

        Ok((privileges, objects))
    }

    /// Parse a single grantable permission/action (used within GRANT statements).
    pub fn parse_grant_permission(&mut self) -> Result<Action, ParserError> {
        fn parse_columns(parser: &mut Parser) -> Result<Option<Vec<Ident>>, ParserError> {
            let columns = parser.parse_parenthesized_column_list(Optional, false)?;
            if columns.is_empty() {
                Ok(None)
            } else {
                Ok(Some(columns))
            }
        }

        // Multi-word privileges
        if self.parse_keywords(&[Keyword::IMPORTED, Keyword::PRIVILEGES]) {
            Ok(Action::ImportedPrivileges)
        } else if self.parse_keywords(&[Keyword::ADD, Keyword::SEARCH, Keyword::OPTIMIZATION]) {
            Ok(Action::AddSearchOptimization)
        } else if self.parse_keywords(&[Keyword::ATTACH, Keyword::LISTING]) {
            Ok(Action::AttachListing)
        } else if self.parse_keywords(&[Keyword::ATTACH, Keyword::POLICY]) {
            Ok(Action::AttachPolicy)
        } else if self.parse_keywords(&[Keyword::BIND, Keyword::SERVICE, Keyword::ENDPOINT]) {
            Ok(Action::BindServiceEndpoint)
        } else if self.parse_keywords(&[Keyword::DATABASE, Keyword::ROLE]) {
            let role = self.parse_object_name(false)?;
            Ok(Action::DatabaseRole { role })
        } else if self.parse_keywords(&[Keyword::EVOLVE, Keyword::SCHEMA]) {
            Ok(Action::EvolveSchema)
        } else if self.parse_keywords(&[Keyword::IMPORT, Keyword::SHARE]) {
            Ok(Action::ImportShare)
        } else if self.parse_keywords(&[Keyword::MANAGE, Keyword::VERSIONS]) {
            Ok(Action::ManageVersions)
        } else if self.parse_keywords(&[Keyword::MANAGE, Keyword::RELEASES]) {
            Ok(Action::ManageReleases)
        } else if self.parse_keywords(&[Keyword::OVERRIDE, Keyword::SHARE, Keyword::RESTRICTIONS]) {
            Ok(Action::OverrideShareRestrictions)
        } else if self.parse_keywords(&[
            Keyword::PURCHASE,
            Keyword::DATA,
            Keyword::EXCHANGE,
            Keyword::LISTING,
        ]) {
            Ok(Action::PurchaseDataExchangeListing)
        } else if self.parse_keywords(&[Keyword::RESOLVE, Keyword::ALL]) {
            Ok(Action::ResolveAll)
        } else if self.parse_keywords(&[Keyword::READ, Keyword::SESSION]) {
            Ok(Action::ReadSession)

        // Single-word privileges
        } else if self.parse_keyword(Keyword::APPLY) {
            let apply_type = self.parse_action_apply_type()?;
            Ok(Action::Apply { apply_type })
        } else if self.parse_keyword(Keyword::APPLYBUDGET) {
            Ok(Action::ApplyBudget)
        } else if self.parse_keyword(Keyword::AUDIT) {
            Ok(Action::Audit)
        } else if self.parse_keyword(Keyword::CONNECT) {
            Ok(Action::Connect)
        } else if self.parse_keyword(Keyword::CREATE) {
            let obj_type = self.maybe_parse_action_create_object_type();
            Ok(Action::Create { obj_type })
        } else if self.parse_keyword(Keyword::DELETE) {
            Ok(Action::Delete)
        } else if self.parse_keyword(Keyword::EXEC) {
            let obj_type = self.maybe_parse_action_execute_obj_type();
            Ok(Action::Exec { obj_type })
        } else if self.parse_keyword(Keyword::EXECUTE) {
            let obj_type = self.maybe_parse_action_execute_obj_type();
            Ok(Action::Execute { obj_type })
        } else if self.parse_keyword(Keyword::FAILOVER) {
            Ok(Action::Failover)
        } else if self.parse_keyword(Keyword::INSERT) {
            Ok(Action::Insert {
                columns: parse_columns(self)?,
            })
        } else if self.parse_keyword(Keyword::MANAGE) {
            let manage_type = self.parse_action_manage_type()?;
            Ok(Action::Manage { manage_type })
        } else if self.parse_keyword(Keyword::MODIFY) {
            let modify_type = self.parse_action_modify_type();
            Ok(Action::Modify { modify_type })
        } else if self.parse_keyword(Keyword::MONITOR) {
            let monitor_type = self.parse_action_monitor_type();
            Ok(Action::Monitor { monitor_type })
        } else if self.parse_keyword(Keyword::OPERATE) {
            Ok(Action::Operate)
        } else if self.parse_keyword(Keyword::REFERENCES) {
            Ok(Action::References {
                columns: parse_columns(self)?,
            })
        } else if self.parse_keyword(Keyword::READ) {
            Ok(Action::Read)
        } else if self.parse_keyword(Keyword::REPLICATE) {
            Ok(Action::Replicate)
        } else if self.parse_keyword(Keyword::ROLE) {
            let role = self.parse_object_name(false)?;
            Ok(Action::Role { role })
        } else if self.parse_keyword(Keyword::SELECT) {
            Ok(Action::Select {
                columns: parse_columns(self)?,
            })
        } else if self.parse_keyword(Keyword::TEMPORARY) {
            Ok(Action::Temporary)
        } else if self.parse_keyword(Keyword::TRIGGER) {
            Ok(Action::Trigger)
        } else if self.parse_keyword(Keyword::TRUNCATE) {
            Ok(Action::Truncate)
        } else if self.parse_keyword(Keyword::UPDATE) {
            Ok(Action::Update {
                columns: parse_columns(self)?,
            })
        } else if self.parse_keyword(Keyword::USAGE) {
            Ok(Action::Usage)
        } else if self.parse_keyword(Keyword::OWNERSHIP) {
            Ok(Action::Ownership)
        } else if self.parse_keyword(Keyword::DROP) {
            Ok(Action::Drop)
        } else {
            self.expected_ref("a privilege keyword", self.peek_token_ref())?
        }
    }

    fn maybe_parse_action_create_object_type(&mut self) -> Option<ActionCreateObjectType> {
        // Multi-word object types
        if self.parse_keywords(&[Keyword::APPLICATION, Keyword::PACKAGE]) {
            Some(ActionCreateObjectType::ApplicationPackage)
        } else if self.parse_keywords(&[Keyword::COMPUTE, Keyword::POOL]) {
            Some(ActionCreateObjectType::ComputePool)
        } else if self.parse_keywords(&[Keyword::DATA, Keyword::EXCHANGE, Keyword::LISTING]) {
            Some(ActionCreateObjectType::DataExchangeListing)
        } else if self.parse_keywords(&[Keyword::EXTERNAL, Keyword::VOLUME]) {
            Some(ActionCreateObjectType::ExternalVolume)
        } else if self.parse_keywords(&[Keyword::FAILOVER, Keyword::GROUP]) {
            Some(ActionCreateObjectType::FailoverGroup)
        } else if self.parse_keywords(&[Keyword::NETWORK, Keyword::POLICY]) {
            Some(ActionCreateObjectType::NetworkPolicy)
        } else if self.parse_keywords(&[Keyword::ORGANIZATION, Keyword::LISTING]) {
            Some(ActionCreateObjectType::OrganiationListing)
        } else if self.parse_keywords(&[Keyword::REPLICATION, Keyword::GROUP]) {
            Some(ActionCreateObjectType::ReplicationGroup)
        }
        // Single-word object types
        else if self.parse_keyword(Keyword::ACCOUNT) {
            Some(ActionCreateObjectType::Account)
        } else if self.parse_keyword(Keyword::APPLICATION) {
            Some(ActionCreateObjectType::Application)
        } else if self.parse_keyword(Keyword::DATABASE) {
            Some(ActionCreateObjectType::Database)
        } else if self.parse_keyword(Keyword::INTEGRATION) {
            Some(ActionCreateObjectType::Integration)
        } else if self.parse_keyword(Keyword::ROLE) {
            Some(ActionCreateObjectType::Role)
        } else if self.parse_keyword(Keyword::SCHEMA) {
            Some(ActionCreateObjectType::Schema)
        } else if self.parse_keyword(Keyword::SHARE) {
            Some(ActionCreateObjectType::Share)
        } else if self.parse_keyword(Keyword::USER) {
            Some(ActionCreateObjectType::User)
        } else if self.parse_keyword(Keyword::WAREHOUSE) {
            Some(ActionCreateObjectType::Warehouse)
        } else {
            None
        }
    }

    fn parse_action_apply_type(&mut self) -> Result<ActionApplyType, ParserError> {
        if self.parse_keywords(&[Keyword::AGGREGATION, Keyword::POLICY]) {
            Ok(ActionApplyType::AggregationPolicy)
        } else if self.parse_keywords(&[Keyword::AUTHENTICATION, Keyword::POLICY]) {
            Ok(ActionApplyType::AuthenticationPolicy)
        } else if self.parse_keywords(&[Keyword::JOIN, Keyword::POLICY]) {
            Ok(ActionApplyType::JoinPolicy)
        } else if self.parse_keywords(&[Keyword::MASKING, Keyword::POLICY]) {
            Ok(ActionApplyType::MaskingPolicy)
        } else if self.parse_keywords(&[Keyword::PACKAGES, Keyword::POLICY]) {
            Ok(ActionApplyType::PackagesPolicy)
        } else if self.parse_keywords(&[Keyword::PASSWORD, Keyword::POLICY]) {
            Ok(ActionApplyType::PasswordPolicy)
        } else if self.parse_keywords(&[Keyword::PROJECTION, Keyword::POLICY]) {
            Ok(ActionApplyType::ProjectionPolicy)
        } else if self.parse_keywords(&[Keyword::ROW, Keyword::ACCESS, Keyword::POLICY]) {
            Ok(ActionApplyType::RowAccessPolicy)
        } else if self.parse_keywords(&[Keyword::SESSION, Keyword::POLICY]) {
            Ok(ActionApplyType::SessionPolicy)
        } else if self.parse_keyword(Keyword::TAG) {
            Ok(ActionApplyType::Tag)
        } else {
            self.expected_ref("GRANT APPLY type", self.peek_token_ref())
        }
    }

    fn maybe_parse_action_execute_obj_type(&mut self) -> Option<ActionExecuteObjectType> {
        if self.parse_keywords(&[Keyword::DATA, Keyword::METRIC, Keyword::FUNCTION]) {
            Some(ActionExecuteObjectType::DataMetricFunction)
        } else if self.parse_keywords(&[Keyword::MANAGED, Keyword::ALERT]) {
            Some(ActionExecuteObjectType::ManagedAlert)
        } else if self.parse_keywords(&[Keyword::MANAGED, Keyword::TASK]) {
            Some(ActionExecuteObjectType::ManagedTask)
        } else if self.parse_keyword(Keyword::ALERT) {
            Some(ActionExecuteObjectType::Alert)
        } else if self.parse_keyword(Keyword::TASK) {
            Some(ActionExecuteObjectType::Task)
        } else {
            None
        }
    }

    fn parse_action_manage_type(&mut self) -> Result<ActionManageType, ParserError> {
        if self.parse_keywords(&[Keyword::ACCOUNT, Keyword::SUPPORT, Keyword::CASES]) {
            Ok(ActionManageType::AccountSupportCases)
        } else if self.parse_keywords(&[Keyword::EVENT, Keyword::SHARING]) {
            Ok(ActionManageType::EventSharing)
        } else if self.parse_keywords(&[Keyword::LISTING, Keyword::AUTO, Keyword::FULFILLMENT]) {
            Ok(ActionManageType::ListingAutoFulfillment)
        } else if self.parse_keywords(&[Keyword::ORGANIZATION, Keyword::SUPPORT, Keyword::CASES]) {
            Ok(ActionManageType::OrganizationSupportCases)
        } else if self.parse_keywords(&[Keyword::USER, Keyword::SUPPORT, Keyword::CASES]) {
            Ok(ActionManageType::UserSupportCases)
        } else if self.parse_keyword(Keyword::GRANTS) {
            Ok(ActionManageType::Grants)
        } else if self.parse_keyword(Keyword::WAREHOUSES) {
            Ok(ActionManageType::Warehouses)
        } else {
            self.expected_ref("GRANT MANAGE type", self.peek_token_ref())
        }
    }

    fn parse_action_modify_type(&mut self) -> Option<ActionModifyType> {
        if self.parse_keywords(&[Keyword::LOG, Keyword::LEVEL]) {
            Some(ActionModifyType::LogLevel)
        } else if self.parse_keywords(&[Keyword::TRACE, Keyword::LEVEL]) {
            Some(ActionModifyType::TraceLevel)
        } else if self.parse_keywords(&[Keyword::SESSION, Keyword::LOG, Keyword::LEVEL]) {
            Some(ActionModifyType::SessionLogLevel)
        } else if self.parse_keywords(&[Keyword::SESSION, Keyword::TRACE, Keyword::LEVEL]) {
            Some(ActionModifyType::SessionTraceLevel)
        } else {
            None
        }
    }

    fn parse_action_monitor_type(&mut self) -> Option<ActionMonitorType> {
        if self.parse_keyword(Keyword::EXECUTION) {
            Some(ActionMonitorType::Execution)
        } else if self.parse_keyword(Keyword::SECURITY) {
            Some(ActionMonitorType::Security)
        } else if self.parse_keyword(Keyword::USAGE) {
            Some(ActionMonitorType::Usage)
        } else {
            None
        }
    }

    /// Parse a grantee name, possibly with a host qualifier (user@host).
    pub fn parse_grantee_name(&mut self) -> Result<GranteeName, ParserError> {
        let mut name = self.parse_object_name(false)?;
        if self.dialect.supports_user_host_grantee()
            && name.0.len() == 1
            && name.0[0].as_ident().is_some()
            && self.consume_token(&Token::AtSign)
        {
            let user = name.0.pop().unwrap().as_ident().unwrap().clone();
            let host = self.parse_identifier()?;
            Ok(GranteeName::UserHost { user, host })
        } else {
            Ok(GranteeName::ObjectName(name))
        }
    }

    /// Parse [`Statement::Deny`]
    pub fn parse_deny(&mut self) -> Result<Statement, ParserError> {
        self.expect_keyword(Keyword::DENY)?;

        let (privileges, objects) = self.parse_grant_deny_revoke_privileges_objects()?;
        let objects = match objects {
            Some(o) => o,
            None => {
                return parser_err!(
                    "DENY statements must specify an object",
                    self.peek_token_ref().span.start
                )
            }
        };

        self.expect_keyword_is(Keyword::TO)?;
        let grantees = self.parse_grantees()?;
        let cascade = self.parse_cascade_option();
        let granted_by = if self.parse_keywords(&[Keyword::AS]) {
            Some(self.parse_identifier()?)
        } else {
            None
        };

        Ok(Statement::Deny(DenyStatement {
            privileges,
            objects,
            grantees,
            cascade,
            granted_by,
        }))
    }

    /// Parse a REVOKE statement
    pub fn parse_revoke(&mut self) -> Result<Revoke, ParserError> {
        let grant_option_for =
            self.parse_keywords(&[Keyword::GRANT, Keyword::OPTION, Keyword::FOR]);

        let (privileges, objects) = self.parse_grant_deny_revoke_privileges_objects()?;

        self.expect_keyword_is(Keyword::FROM)?;
        let grantees = self.parse_grantees()?;

        let granted_by = if self.parse_keywords(&[Keyword::GRANTED, Keyword::BY]) {
            Some(self.parse_identifier()?)
        } else {
            None
        };

        let cascade = self.parse_cascade_option();

        Ok(Revoke {
            grant_option_for,
            privileges,
            objects,
            grantees,
            granted_by,
            cascade,
        })
    }

    /// Parse a `REVOKE` of role membership, after the `REVOKE` keyword.
    pub fn parse_revoke_role(&mut self) -> Result<RevokeRole, ParserError> {
        let option_for = self.parse_role_grant_option_for();
        let roles = self.parse_comma_separated(Parser::parse_identifier)?;
        self.expect_keyword_is(Keyword::FROM)?;
        let grantees = self.parse_grantees()?;
        let granted_by = if self.parse_keywords(&[Keyword::GRANTED, Keyword::BY]) {
            Some(self.parse_identifier()?)
        } else {
            None
        };
        let cascade = self.parse_cascade_option();
        Ok(RevokeRole {
            option_for,
            roles,
            grantees,
            granted_by,
            cascade,
        })
    }

    /// Parses `name OPTION FOR`, leaving `GRANT OPTION FOR` to the privilege form.
    fn parse_role_grant_option_for(&mut self) -> Option<Ident> {
        let index = self.index;
        if !self.peek_keyword(Keyword::GRANT) {
            if let Ok(name) = self.parse_identifier() {
                if self.parse_keywords(&[Keyword::OPTION, Keyword::FOR]) {
                    return Some(name);
                }
            }
        }
        self.index = index;
        None
    }

    pub(super) fn peek_role_grant_option_for(&mut self) -> bool {
        let index = self.index;
        let matched = self.parse_role_grant_option_for().is_some();
        self.index = index;
        matched
    }

    /// Parse a list of actions for `GRANT` statements.
    pub fn parse_actions_list(&mut self) -> Result<Vec<Action>, ParserError> {
        let mut values = vec![];
        loop {
            values.push(self.parse_grant_permission()?);
            if !self.consume_token(&Token::Comma) {
                break;
            } else if self.options.trailing_commas {
                match &self.peek_token_ref().token {
                    Token::Word(kw) if kw.keyword == Keyword::ON => {
                        break;
                    }
                    Token::RParen
                    | Token::SemiColon
                    | Token::EOF
                    | Token::RBracket
                    | Token::RBrace => break,
                    _ => continue,
                }
            }
        }
        Ok(values)
    }

    pub(super) fn parse_create_user(
        &mut self,
        or_replace: bool,
    ) -> Result<CreateUser, ParserError> {
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

    /// Parse a `CREATE ROLE` or `CREATE GROUP` statement, after the keyword has been consumed.
    pub fn parse_create_role(
        &mut self,
        role_keyword: RoleKeyword,
    ) -> Result<CreateRole, ParserError> {
        let supports_if_not_exists = matches!(role_keyword, RoleKeyword::Role)
            && self.dialect.supports_create_role_if_not_exists();
        if !supports_if_not_exists
            && self.peek_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS])
        {
            return parser_err!(
                format!("IF NOT EXISTS is not supported for CREATE {role_keyword}"),
                self.peek_token_ref().span.start
            );
        }
        let if_not_exists = supports_if_not_exists
            && self.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
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
                        connection_limit = Some(self.parse_number()?);
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
            keyword: role_keyword,
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

    /// Parse a `SET ROLE` statement. Expects SET to be consumed already.
    pub(super) fn parse_set_role(
        &mut self,
        modifier: Option<ContextModifier>,
    ) -> Result<Statement, ParserError> {
        self.expect_keyword_is(Keyword::ROLE)?;

        let role_name = if self.parse_keyword(Keyword::NONE) {
            None
        } else {
            Some(self.parse_identifier()?)
        };
        Ok(Statement::Set(Set::SetRole {
            context_modifier: modifier,
            role_name,
        }))
    }

    /// Parse a `USE` statement (database/catalog/schema/warehouse/role selection).
    pub fn parse_use(&mut self) -> Result<Statement, ParserError> {
        // Determine which keywords are recognized by the current dialect
        let parsed_keyword = if dialect_of!(self is HiveDialect) {
            // HiveDialect accepts USE DEFAULT; statement without any db specified
            if self.parse_keyword(Keyword::DEFAULT) {
                return Ok(Statement::Use(Use::Default));
            }
            None // HiveDialect doesn't expect any other specific keyword after `USE`
        } else if dialect_of!(self is DatabricksDialect) {
            self.parse_one_of_keywords(&[Keyword::CATALOG, Keyword::DATABASE, Keyword::SCHEMA])
        } else if dialect_of!(self is SnowflakeDialect) {
            self.parse_one_of_keywords(&[
                Keyword::DATABASE,
                Keyword::SCHEMA,
                Keyword::WAREHOUSE,
                Keyword::ROLE,
                Keyword::SECONDARY,
            ])
        } else {
            None // No specific keywords for other dialects, including GenericDialect
        };

        let result = if matches!(parsed_keyword, Some(Keyword::SECONDARY)) {
            self.parse_secondary_roles()?
        } else {
            let obj_name = self.parse_object_name(false)?;
            match parsed_keyword {
                Some(Keyword::CATALOG) => Use::Catalog(obj_name),
                Some(Keyword::DATABASE) => Use::Database(obj_name),
                Some(Keyword::SCHEMA) => Use::Schema(obj_name),
                Some(Keyword::WAREHOUSE) => Use::Warehouse(obj_name),
                Some(Keyword::ROLE) => Use::Role(obj_name),
                _ => Use::Object(obj_name),
            }
        };

        Ok(Statement::Use(result))
    }

    fn parse_secondary_roles(&mut self) -> Result<Use, ParserError> {
        self.expect_one_of_keywords(&[Keyword::ROLES, Keyword::ROLE])?;
        if self.parse_keyword(Keyword::NONE) {
            Ok(Use::SecondaryRoles(SecondaryRoles::None))
        } else if self.parse_keyword(Keyword::ALL) {
            Ok(Use::SecondaryRoles(SecondaryRoles::All))
        } else {
            let roles = self.parse_comma_separated(|parser| parser.parse_identifier())?;
            Ok(Use::SecondaryRoles(SecondaryRoles::List(roles)))
        }
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
    /// DROP POLICY [ IF EXISTS ] name ON table_name [ CASCADE | RESTRICT ]
    /// ```
    ///
    /// [PostgreSQL Documentation](https://www.postgresql.org/docs/current/sql-droppolicy.html)
    pub(super) fn parse_drop_policy(&mut self) -> Result<DropPolicy, ParserError> {
        let if_exists = self.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
        let name = self.parse_identifier()?;
        self.expect_keyword_is(Keyword::ON)?;
        let table_name = self.parse_object_name(false)?;
        let drop_behavior = self.parse_optional_drop_behavior();
        Ok(DropPolicy {
            if_exists,
            name,
            table_name,
            drop_behavior,
        })
    }
}

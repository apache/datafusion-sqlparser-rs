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

//! SQL Parser for ALTER

#[cfg(not(feature = "std"))]
use alloc::vec;

use crate::parser::*;

impl Parser<'_> {
    pub fn parse_alter(&mut self) -> Result<Statement, ParserError> {
        let object_type = self.expect_one_of_keywords(&[
            Keyword::VIEW,
            Keyword::TABLE,
            Keyword::INDEX,
            Keyword::ROLE,
            Keyword::POLICY,
        ])?;
        match object_type {
            Keyword::VIEW => self.parse_alter_view(),
            Keyword::TABLE => {
                let if_exists = self.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
                let only = self.parse_keyword(Keyword::ONLY); // [ ONLY ]
                let table_name = self.parse_object_name(false)?;
                let on_cluster = self.parse_optional_on_cluster()?;
                let operations = self.parse_comma_separated(Parser::parse_alter_table_operation)?;

                let mut location = None;
                if self.parse_keyword(Keyword::LOCATION) {
                    location = Some(HiveSetLocation {
                        has_set: false,
                        location: self.parse_identifier(false)?,
                    });
                } else if self.parse_keywords(&[Keyword::SET, Keyword::LOCATION]) {
                    location = Some(HiveSetLocation {
                        has_set: true,
                        location: self.parse_identifier(false)?,
                    });
                }

                Ok(Statement::AlterTable {
                    name: table_name,
                    if_exists,
                    only,
                    operations,
                    location,
                    on_cluster,
                })
            }
            Keyword::INDEX => {
                let index_name = self.parse_object_name(false)?;
                let operation = if self.parse_keyword(Keyword::RENAME) {
                    if self.parse_keyword(Keyword::TO) {
                        let index_name = self.parse_object_name(false)?;
                        AlterIndexOperation::RenameIndex { index_name }
                    } else {
                        return self.expected("TO after RENAME", self.peek_token());
                    }
                } else {
                    return self.expected("RENAME after ALTER INDEX", self.peek_token());
                };

                Ok(Statement::AlterIndex {
                    name: index_name,
                    operation,
                })
            }
            Keyword::ROLE => self.parse_alter_role(),
            Keyword::POLICY => self.parse_alter_policy(),
            // unreachable because expect_one_of_keywords used above
            _ => unreachable!(),
        }
    }

    /// Parse ALTER POLICY statement
    /// ```sql
    /// ALTER POLICY policy_name ON table_name [ RENAME TO new_name ]
    /// or
    /// ALTER POLICY policy_name ON table_name
    /// [ TO { role_name | PUBLIC | CURRENT_ROLE | CURRENT_USER | SESSION_USER } [, ...] ]
    /// [ USING ( using_expression ) ]
    /// [ WITH CHECK ( check_expression ) ]
    /// ```
    ///
    /// [PostgreSQL](https://www.postgresql.org/docs/current/sql-alterpolicy.html)
    pub fn parse_alter_policy(&mut self) -> Result<Statement, ParserError> {
        let name = self.parse_identifier(false)?;
        self.expect_keyword(Keyword::ON)?;
        let table_name = self.parse_object_name(false)?;

        if self.parse_keyword(Keyword::RENAME) {
            self.expect_keyword(Keyword::TO)?;
            let new_name = self.parse_identifier(false)?;
            Ok(Statement::AlterPolicy {
                name,
                table_name,
                operation: AlterPolicyOperation::Rename { new_name },
            })
        } else {
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
            Ok(Statement::AlterPolicy {
                name,
                table_name,
                operation: AlterPolicyOperation::Apply {
                    to,
                    using,
                    with_check,
                },
            })
        }
    }

    pub fn parse_alter_role(&mut self) -> Result<Statement, ParserError> {
        if dialect_of!(self is PostgreSqlDialect) {
            return self.parse_pg_alter_role();
        } else if dialect_of!(self is MsSqlDialect) {
            return self.parse_mssql_alter_role();
        }

        Err(ParserError::ParserError(
            "ALTER ROLE is only support for PostgreSqlDialect, MsSqlDialect".into(),
        ))
    }

    pub fn parse_alter_table_add_projection(&mut self) -> Result<AlterTableOperation, ParserError> {
        let if_not_exists = self.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let name = self.parse_identifier(false)?;
        let query = self.parse_projection_select()?;
        Ok(AlterTableOperation::AddProjection {
            if_not_exists,
            name,
            select: query,
        })
    }

    pub fn parse_alter_table_operation(&mut self) -> Result<AlterTableOperation, ParserError> {
        let operation = if self.parse_keyword(Keyword::ADD) {
            if let Some(constraint) = self.parse_optional_table_constraint()? {
                AlterTableOperation::AddConstraint(constraint)
            } else if dialect_of!(self is ClickHouseDialect|GenericDialect)
                && self.parse_keyword(Keyword::PROJECTION)
            {
                return self.parse_alter_table_add_projection();
            } else {
                let if_not_exists =
                    self.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
                let mut new_partitions = vec![];
                loop {
                    if self.parse_keyword(Keyword::PARTITION) {
                        new_partitions.push(self.parse_partition()?);
                    } else {
                        break;
                    }
                }
                if !new_partitions.is_empty() {
                    AlterTableOperation::AddPartitions {
                        if_not_exists,
                        new_partitions,
                    }
                } else {
                    let column_keyword = self.parse_keyword(Keyword::COLUMN);

                    let if_not_exists = if dialect_of!(self is PostgreSqlDialect | BigQueryDialect | DuckDbDialect | GenericDialect)
                    {
                        self.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS])
                            || if_not_exists
                    } else {
                        false
                    };

                    let column_def = self.parse_column_def()?;

                    let column_position = self.parse_column_position()?;

                    AlterTableOperation::AddColumn {
                        column_keyword,
                        if_not_exists,
                        column_def,
                        column_position,
                    }
                }
            }
        } else if self.parse_keyword(Keyword::RENAME) {
            if dialect_of!(self is PostgreSqlDialect) && self.parse_keyword(Keyword::CONSTRAINT) {
                let old_name = self.parse_identifier(false)?;
                self.expect_keyword(Keyword::TO)?;
                let new_name = self.parse_identifier(false)?;
                AlterTableOperation::RenameConstraint { old_name, new_name }
            } else if self.parse_keyword(Keyword::TO) {
                let table_name = self.parse_object_name(false)?;
                AlterTableOperation::RenameTable { table_name }
            } else {
                let _ = self.parse_keyword(Keyword::COLUMN); // [ COLUMN ]
                let old_column_name = self.parse_identifier(false)?;
                self.expect_keyword(Keyword::TO)?;
                let new_column_name = self.parse_identifier(false)?;
                AlterTableOperation::RenameColumn {
                    old_column_name,
                    new_column_name,
                }
            }
        } else if self.parse_keyword(Keyword::DISABLE) {
            if self.parse_keywords(&[Keyword::ROW, Keyword::LEVEL, Keyword::SECURITY]) {
                AlterTableOperation::DisableRowLevelSecurity {}
            } else if self.parse_keyword(Keyword::RULE) {
                let name = self.parse_identifier(false)?;
                AlterTableOperation::DisableRule { name }
            } else if self.parse_keyword(Keyword::TRIGGER) {
                let name = self.parse_identifier(false)?;
                AlterTableOperation::DisableTrigger { name }
            } else {
                return self.expected(
                    "ROW LEVEL SECURITY, RULE, or TRIGGER after DISABLE",
                    self.peek_token(),
                );
            }
        } else if self.parse_keyword(Keyword::ENABLE) {
            if self.parse_keywords(&[Keyword::ALWAYS, Keyword::RULE]) {
                let name = self.parse_identifier(false)?;
                AlterTableOperation::EnableAlwaysRule { name }
            } else if self.parse_keywords(&[Keyword::ALWAYS, Keyword::TRIGGER]) {
                let name = self.parse_identifier(false)?;
                AlterTableOperation::EnableAlwaysTrigger { name }
            } else if self.parse_keywords(&[Keyword::ROW, Keyword::LEVEL, Keyword::SECURITY]) {
                AlterTableOperation::EnableRowLevelSecurity {}
            } else if self.parse_keywords(&[Keyword::REPLICA, Keyword::RULE]) {
                let name = self.parse_identifier(false)?;
                AlterTableOperation::EnableReplicaRule { name }
            } else if self.parse_keywords(&[Keyword::REPLICA, Keyword::TRIGGER]) {
                let name = self.parse_identifier(false)?;
                AlterTableOperation::EnableReplicaTrigger { name }
            } else if self.parse_keyword(Keyword::RULE) {
                let name = self.parse_identifier(false)?;
                AlterTableOperation::EnableRule { name }
            } else if self.parse_keyword(Keyword::TRIGGER) {
                let name = self.parse_identifier(false)?;
                AlterTableOperation::EnableTrigger { name }
            } else {
                return self.expected(
                    "ALWAYS, REPLICA, ROW LEVEL SECURITY, RULE, or TRIGGER after ENABLE",
                    self.peek_token(),
                );
            }
        } else if self.parse_keywords(&[Keyword::CLEAR, Keyword::PROJECTION])
            && dialect_of!(self is ClickHouseDialect|GenericDialect)
        {
            let if_exists = self.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
            let name = self.parse_identifier(false)?;
            let partition = if self.parse_keywords(&[Keyword::IN, Keyword::PARTITION]) {
                Some(self.parse_identifier(false)?)
            } else {
                None
            };
            AlterTableOperation::ClearProjection {
                if_exists,
                name,
                partition,
            }
        } else if self.parse_keywords(&[Keyword::MATERIALIZE, Keyword::PROJECTION])
            && dialect_of!(self is ClickHouseDialect|GenericDialect)
        {
            let if_exists = self.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
            let name = self.parse_identifier(false)?;
            let partition = if self.parse_keywords(&[Keyword::IN, Keyword::PARTITION]) {
                Some(self.parse_identifier(false)?)
            } else {
                None
            };
            AlterTableOperation::MaterializeProjection {
                if_exists,
                name,
                partition,
            }
        } else if self.parse_keyword(Keyword::DROP) {
            if self.parse_keywords(&[Keyword::IF, Keyword::EXISTS, Keyword::PARTITION]) {
                self.expect_token(&Token::LParen)?;
                let partitions = self.parse_comma_separated(Parser::parse_expr)?;
                self.expect_token(&Token::RParen)?;
                AlterTableOperation::DropPartitions {
                    partitions,
                    if_exists: true,
                }
            } else if self.parse_keyword(Keyword::PARTITION) {
                self.expect_token(&Token::LParen)?;
                let partitions = self.parse_comma_separated(Parser::parse_expr)?;
                self.expect_token(&Token::RParen)?;
                AlterTableOperation::DropPartitions {
                    partitions,
                    if_exists: false,
                }
            } else if self.parse_keyword(Keyword::CONSTRAINT) {
                let if_exists = self.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
                let name = self.parse_identifier(false)?;
                let cascade = self.parse_keyword(Keyword::CASCADE);
                AlterTableOperation::DropConstraint {
                    if_exists,
                    name,
                    cascade,
                }
            } else if self.parse_keywords(&[Keyword::PRIMARY, Keyword::KEY])
                && dialect_of!(self is MySqlDialect | GenericDialect)
            {
                AlterTableOperation::DropPrimaryKey
            } else if self.parse_keyword(Keyword::PROJECTION)
                && dialect_of!(self is ClickHouseDialect|GenericDialect)
            {
                let if_exists = self.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
                let name = self.parse_identifier(false)?;
                AlterTableOperation::DropProjection { if_exists, name }
            } else if self.parse_keywords(&[Keyword::CLUSTERING, Keyword::KEY]) {
                AlterTableOperation::DropClusteringKey
            } else {
                let _ = self.parse_keyword(Keyword::COLUMN); // [ COLUMN ]
                let if_exists = self.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
                let column_name = self.parse_identifier(false)?;
                let cascade = self.parse_keyword(Keyword::CASCADE);
                AlterTableOperation::DropColumn {
                    column_name,
                    if_exists,
                    cascade,
                }
            }
        } else if self.parse_keyword(Keyword::PARTITION) {
            self.expect_token(&Token::LParen)?;
            let before = self.parse_comma_separated(Parser::parse_expr)?;
            self.expect_token(&Token::RParen)?;
            self.expect_keyword(Keyword::RENAME)?;
            self.expect_keywords(&[Keyword::TO, Keyword::PARTITION])?;
            self.expect_token(&Token::LParen)?;
            let renames = self.parse_comma_separated(Parser::parse_expr)?;
            self.expect_token(&Token::RParen)?;
            AlterTableOperation::RenamePartitions {
                old_partitions: before,
                new_partitions: renames,
            }
        } else if self.parse_keyword(Keyword::CHANGE) {
            let _ = self.parse_keyword(Keyword::COLUMN); // [ COLUMN ]
            let old_name = self.parse_identifier(false)?;
            let new_name = self.parse_identifier(false)?;
            let data_type = self.parse_data_type()?;
            let mut options = vec![];
            while let Some(option) = self.parse_optional_column_option()? {
                options.push(option);
            }

            let column_position = self.parse_column_position()?;

            AlterTableOperation::ChangeColumn {
                old_name,
                new_name,
                data_type,
                options,
                column_position,
            }
        } else if self.parse_keyword(Keyword::MODIFY) {
            let _ = self.parse_keyword(Keyword::COLUMN); // [ COLUMN ]
            let col_name = self.parse_identifier(false)?;
            let data_type = self.parse_data_type()?;
            let mut options = vec![];
            while let Some(option) = self.parse_optional_column_option()? {
                options.push(option);
            }

            let column_position = self.parse_column_position()?;

            AlterTableOperation::ModifyColumn {
                col_name,
                data_type,
                options,
                column_position,
            }
        } else if self.parse_keyword(Keyword::ALTER) {
            let _ = self.parse_keyword(Keyword::COLUMN); // [ COLUMN ]
            let column_name = self.parse_identifier(false)?;
            let is_postgresql = dialect_of!(self is PostgreSqlDialect);

            let op: AlterColumnOperation = if self.parse_keywords(&[
                Keyword::SET,
                Keyword::NOT,
                Keyword::NULL,
            ]) {
                AlterColumnOperation::SetNotNull {}
            } else if self.parse_keywords(&[Keyword::DROP, Keyword::NOT, Keyword::NULL]) {
                AlterColumnOperation::DropNotNull {}
            } else if self.parse_keywords(&[Keyword::SET, Keyword::DEFAULT]) {
                AlterColumnOperation::SetDefault {
                    value: self.parse_expr()?,
                }
            } else if self.parse_keywords(&[Keyword::DROP, Keyword::DEFAULT]) {
                AlterColumnOperation::DropDefault {}
            } else if self.parse_keywords(&[Keyword::SET, Keyword::DATA, Keyword::TYPE])
                || (is_postgresql && self.parse_keyword(Keyword::TYPE))
            {
                let data_type = self.parse_data_type()?;
                let using = if is_postgresql && self.parse_keyword(Keyword::USING) {
                    Some(self.parse_expr()?)
                } else {
                    None
                };
                AlterColumnOperation::SetDataType { data_type, using }
            } else if self.parse_keywords(&[Keyword::ADD, Keyword::GENERATED]) {
                let generated_as = if self.parse_keyword(Keyword::ALWAYS) {
                    Some(GeneratedAs::Always)
                } else if self.parse_keywords(&[Keyword::BY, Keyword::DEFAULT]) {
                    Some(GeneratedAs::ByDefault)
                } else {
                    None
                };

                self.expect_keywords(&[Keyword::AS, Keyword::IDENTITY])?;

                let mut sequence_options: Option<Vec<SequenceOptions>> = None;

                if self.peek_token().token == Token::LParen {
                    self.expect_token(&Token::LParen)?;
                    sequence_options = Some(self.parse_create_sequence_options()?);
                    self.expect_token(&Token::RParen)?;
                }

                AlterColumnOperation::AddGenerated {
                    generated_as,
                    sequence_options,
                }
            } else {
                let message = if is_postgresql {
                    "SET/DROP NOT NULL, SET DEFAULT, SET DATA TYPE, or ADD GENERATED after ALTER COLUMN"
                } else {
                    "SET/DROP NOT NULL, SET DEFAULT, or SET DATA TYPE after ALTER COLUMN"
                };

                return self.expected(message, self.peek_token());
            };
            AlterTableOperation::AlterColumn { column_name, op }
        } else if self.parse_keyword(Keyword::SWAP) {
            self.expect_keyword(Keyword::WITH)?;
            let table_name = self.parse_object_name(false)?;
            AlterTableOperation::SwapWith { table_name }
        } else if dialect_of!(self is PostgreSqlDialect | GenericDialect)
            && self.parse_keywords(&[Keyword::OWNER, Keyword::TO])
        {
            let new_owner = self.parse_owner()?;
            AlterTableOperation::OwnerTo { new_owner }
        } else if dialect_of!(self is ClickHouseDialect|GenericDialect)
            && self.parse_keyword(Keyword::ATTACH)
        {
            AlterTableOperation::AttachPartition {
                partition: self.parse_part_or_partition()?,
            }
        } else if dialect_of!(self is ClickHouseDialect|GenericDialect)
            && self.parse_keyword(Keyword::DETACH)
        {
            AlterTableOperation::DetachPartition {
                partition: self.parse_part_or_partition()?,
            }
        } else if dialect_of!(self is ClickHouseDialect|GenericDialect)
            && self.parse_keyword(Keyword::FREEZE)
        {
            let partition = self.parse_part_or_partition()?;
            let with_name = if self.parse_keyword(Keyword::WITH) {
                self.expect_keyword(Keyword::NAME)?;
                Some(self.parse_identifier(false)?)
            } else {
                None
            };
            AlterTableOperation::FreezePartition {
                partition,
                with_name,
            }
        } else if dialect_of!(self is ClickHouseDialect|GenericDialect)
            && self.parse_keyword(Keyword::UNFREEZE)
        {
            let partition = self.parse_part_or_partition()?;
            let with_name = if self.parse_keyword(Keyword::WITH) {
                self.expect_keyword(Keyword::NAME)?;
                Some(self.parse_identifier(false)?)
            } else {
                None
            };
            AlterTableOperation::UnfreezePartition {
                partition,
                with_name,
            }
        } else if self.parse_keywords(&[Keyword::CLUSTER, Keyword::BY]) {
            self.expect_token(&Token::LParen)?;
            let exprs = self.parse_comma_separated(|parser| parser.parse_expr())?;
            self.expect_token(&Token::RParen)?;
            AlterTableOperation::ClusterBy { exprs }
        } else if self.parse_keywords(&[Keyword::SUSPEND, Keyword::RECLUSTER]) {
            AlterTableOperation::SuspendRecluster
        } else if self.parse_keywords(&[Keyword::RESUME, Keyword::RECLUSTER]) {
            AlterTableOperation::ResumeRecluster
        } else {
            let options: Vec<SqlOption> =
                self.parse_options_with_keywords(&[Keyword::SET, Keyword::TBLPROPERTIES])?;
            if !options.is_empty() {
                AlterTableOperation::SetTblProperties {
                    table_properties: options,
                }
            } else {
                return self.expected(
                    "ADD, RENAME, PARTITION, SWAP, DROP, or SET TBLPROPERTIES after ALTER TABLE",
                    self.peek_token(),
                );
            }
        };
        Ok(operation)
    }

    pub fn parse_alter_view(&mut self) -> Result<Statement, ParserError> {
        let name = self.parse_object_name(false)?;
        let columns = self.parse_parenthesized_column_list(Optional, false)?;

        let with_options = self.parse_options(Keyword::WITH)?;

        self.expect_keyword(Keyword::AS)?;
        let query = self.parse_query()?;

        Ok(Statement::AlterView {
            name,
            columns,
            query,
            with_options,
        })
    }

    pub fn parse_partition(&mut self) -> Result<Partition, ParserError> {
        self.expect_token(&Token::LParen)?;
        let partitions = self.parse_comma_separated(Parser::parse_expr)?;
        self.expect_token(&Token::RParen)?;
        Ok(Partition::Partitions(partitions))
    }

    pub fn parse_projection_select(&mut self) -> Result<ProjectionSelect, ParserError> {
        self.expect_token(&Token::LParen)?;
        self.expect_keyword(Keyword::SELECT)?;
        let projection = self.parse_projection()?;
        let group_by = self.parse_optional_group_by()?;
        let order_by = self.parse_optional_order_by()?;
        self.expect_token(&Token::RParen)?;
        Ok(ProjectionSelect {
            projection,
            group_by,
            order_by,
        })
    }

    fn parse_column_position(&mut self) -> Result<Option<MySQLColumnPosition>, ParserError> {
        if dialect_of!(self is MySqlDialect | GenericDialect) {
            if self.parse_keyword(Keyword::FIRST) {
                Ok(Some(MySQLColumnPosition::First))
            } else if self.parse_keyword(Keyword::AFTER) {
                let ident = self.parse_identifier(false)?;
                Ok(Some(MySQLColumnPosition::After(ident)))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    fn parse_part_or_partition(&mut self) -> Result<Partition, ParserError> {
        let keyword = self.expect_one_of_keywords(&[Keyword::PART, Keyword::PARTITION])?;
        match keyword {
            Keyword::PART => Ok(Partition::Part(self.parse_expr()?)),
            Keyword::PARTITION => Ok(Partition::Expr(self.parse_expr()?)),
            // unreachable because expect_one_of_keywords used above
            _ => unreachable!(),
        }
    }
}

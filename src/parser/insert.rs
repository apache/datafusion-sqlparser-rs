use super::*;

impl<'a> Parser<'a> {
    /// Parse an INSERT statement, returning a `Box`ed SetExpr
    ///
    /// This is used to reduce the size of the stack frames in debug builds
    pub(crate) fn parse_insert_setexpr_boxed(&mut self) -> Result<Box<SetExpr>, ParserError> {
        Ok(Box::new(SetExpr::Insert(self.parse_insert()?)))
    }

    /// Parse an INSERT statement
    pub fn parse_insert(&mut self) -> Result<Statement, ParserError> {
        let or = self.parse_conflict_clause();
        let priority = if !dialect_of!(self is MySqlDialect | GenericDialect) {
            None
        } else if self.parse_keyword(Keyword::LOW_PRIORITY) {
            Some(MysqlInsertPriority::LowPriority)
        } else if self.parse_keyword(Keyword::DELAYED) {
            Some(MysqlInsertPriority::Delayed)
        } else if self.parse_keyword(Keyword::HIGH_PRIORITY) {
            Some(MysqlInsertPriority::HighPriority)
        } else {
            None
        };

        let ignore = dialect_of!(self is MySqlDialect | GenericDialect)
            && self.parse_keyword(Keyword::IGNORE);

        let replace_into = false;

        let action = self.parse_one_of_keywords(&[Keyword::INTO, Keyword::OVERWRITE]);
        let into = action == Some(Keyword::INTO);
        let overwrite = action == Some(Keyword::OVERWRITE);

        let local = self.parse_keyword(Keyword::LOCAL);

        if self.parse_keyword(Keyword::DIRECTORY) {
            let path = self.parse_literal_string()?;
            let file_format = if self.parse_keywords(&[Keyword::STORED, Keyword::AS]) {
                Some(self.parse_file_format()?)
            } else {
                None
            };
            let source = self.parse_query()?;
            Ok(Statement::Directory {
                local,
                path,
                overwrite,
                file_format,
                source,
            })
        } else {
            // Hive lets you put table here regardless
            let table = self.parse_keyword(Keyword::TABLE);
            let table_name = self.parse_object_name(false)?;

            let table_alias =
                if dialect_of!(self is PostgreSqlDialect) && self.parse_keyword(Keyword::AS) {
                    Some(self.parse_identifier(false)?)
                } else {
                    None
                };

            let is_mysql = dialect_of!(self is MySqlDialect);

            let (columns, partitioned, after_columns, source) =
                if self.parse_keywords(&[Keyword::DEFAULT, Keyword::VALUES]) {
                    (vec![], None, vec![], None)
                } else {
                    let columns = self.parse_parenthesized_column_list(Optional, is_mysql)?;

                    let partitioned = self.parse_insert_partition()?;
                    // Hive allows you to specify columns after partitions as well if you want.
                    let after_columns = if dialect_of!(self is HiveDialect) {
                        self.parse_parenthesized_column_list(Optional, false)?
                    } else {
                        vec![]
                    };

                    let source = Some(self.parse_query()?);

                    (columns, partitioned, after_columns, source)
                };

            let insert_alias = if dialect_of!(self is MySqlDialect | GenericDialect)
                && self.parse_keyword(Keyword::AS)
            {
                let row_alias = self.parse_object_name(false)?;
                let col_aliases = Some(self.parse_parenthesized_column_list(Optional, false)?);
                Some(InsertAliases {
                    row_alias,
                    col_aliases,
                })
            } else {
                None
            };

            let on = if self.parse_keyword(Keyword::ON) {
                if self.parse_keyword(Keyword::CONFLICT) {
                    let conflict_target =
                        if self.parse_keywords(&[Keyword::ON, Keyword::CONSTRAINT]) {
                            Some(ConflictTarget::OnConstraint(self.parse_object_name(false)?))
                        } else if self.peek_token() == Token::LParen {
                            Some(ConflictTarget::Columns(
                                self.parse_parenthesized_column_list(IsOptional::Mandatory, false)?,
                            ))
                        } else {
                            None
                        };

                    self.expect_keyword(Keyword::DO)?;
                    let action = if self.parse_keyword(Keyword::NOTHING) {
                        OnConflictAction::DoNothing
                    } else {
                        self.expect_keyword(Keyword::UPDATE)?;
                        self.expect_keyword(Keyword::SET)?;
                        let assignments = self.parse_comma_separated(Parser::parse_assignment)?;
                        let selection = if self.parse_keyword(Keyword::WHERE) {
                            Some(self.parse_expr()?)
                        } else {
                            None
                        };
                        OnConflictAction::DoUpdate(DoUpdate {
                            assignments,
                            selection,
                        })
                    };

                    Some(OnInsert::OnConflict(OnConflict {
                        conflict_target,
                        action,
                    }))
                } else {
                    self.expect_keyword(Keyword::DUPLICATE)?;
                    self.expect_keyword(Keyword::KEY)?;
                    self.expect_keyword(Keyword::UPDATE)?;
                    let l = self.parse_comma_separated(Parser::parse_assignment)?;

                    Some(OnInsert::DuplicateKeyUpdate(l))
                }
            } else {
                None
            };

            let returning = if self.parse_keyword(Keyword::RETURNING) {
                Some(self.parse_comma_separated(Parser::parse_select_item)?)
            } else {
                None
            };

            Ok(Statement::Insert(Insert {
                or,
                table_name,
                table_alias,
                ignore,
                into,
                overwrite,
                partitioned,
                columns,
                after_columns,
                source,
                table,
                on,
                returning,
                replace_into,
                priority,
                insert_alias,
            }))
        }
    }

    pub(crate) fn parse_conflict_clause(&mut self) -> Option<SqliteOnConflict> {
        if self.parse_keywords(&[Keyword::OR, Keyword::REPLACE]) {
            Some(SqliteOnConflict::Replace)
        } else if self.parse_keywords(&[Keyword::OR, Keyword::ROLLBACK]) {
            Some(SqliteOnConflict::Rollback)
        } else if self.parse_keywords(&[Keyword::OR, Keyword::ABORT]) {
            Some(SqliteOnConflict::Abort)
        } else if self.parse_keywords(&[Keyword::OR, Keyword::FAIL]) {
            Some(SqliteOnConflict::Fail)
        } else if self.parse_keywords(&[Keyword::OR, Keyword::IGNORE]) {
            Some(SqliteOnConflict::Ignore)
        } else if self.parse_keyword(Keyword::REPLACE) {
            Some(SqliteOnConflict::Replace)
        } else {
            None
        }
    }

    pub fn parse_insert_partition(&mut self) -> Result<Option<Vec<Expr>>, ParserError> {
        if self.parse_keyword(Keyword::PARTITION) {
            self.expect_token(&Token::LParen)?;
            let partition_cols = Some(self.parse_comma_separated(Parser::parse_expr)?);
            self.expect_token(&Token::RParen)?;
            Ok(partition_cols)
        } else {
            Ok(None)
        }
    }
}

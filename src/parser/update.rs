use crate::parser::*;

impl Parser<'_> {
    pub fn parse_update(&mut self) -> Result<Statement, ParserError> {
        let or = self.parse_conflict_clause();
        let table = self.parse_table_and_joins()?;
        self.expect_keyword(Keyword::SET)?;
        let assignments = self.parse_comma_separated(Parser::parse_assignment)?;
        let from = if self.parse_keyword(Keyword::FROM)
            && dialect_of!(self is GenericDialect | PostgreSqlDialect | DuckDbDialect | BigQueryDialect | SnowflakeDialect | RedshiftSqlDialect | MsSqlDialect | SQLiteDialect )
        {
            Some(self.parse_table_and_joins()?)
        } else {
            None
        };
        let selection = if self.parse_keyword(Keyword::WHERE) {
            Some(self.parse_expr()?)
        } else {
            None
        };
        let returning = if self.parse_keyword(Keyword::RETURNING) {
            Some(self.parse_comma_separated(Parser::parse_select_item)?)
        } else {
            None
        };
        Ok(Statement::Update {
            table,
            assignments,
            from,
            selection,
            returning,
            or,
        })
    }
}

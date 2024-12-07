use crate::parser::*;

impl<'a> Parser<'a> {
    pub fn parse_delete(&mut self) -> Result<Statement, ParserError> {
        let (tables, with_from_keyword) = if !self.parse_keyword(Keyword::FROM) {
            // `FROM` keyword is optional in BigQuery SQL.
            // https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#delete_statement
            if dialect_of!(self is BigQueryDialect | GenericDialect) {
                (vec![], false)
            } else {
                let tables = self.parse_comma_separated(|p| p.parse_object_name(false))?;
                self.expect_keyword(Keyword::FROM)?;
                (tables, true)
            }
        } else {
            (vec![], true)
        };

        let from = self.parse_comma_separated(Parser::parse_table_and_joins)?;
        let using = if self.parse_keyword(Keyword::USING) {
            Some(self.parse_comma_separated(Parser::parse_table_and_joins)?)
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
        let order_by = if self.parse_keywords(&[Keyword::ORDER, Keyword::BY]) {
            self.parse_comma_separated(Parser::parse_order_by_expr)?
        } else {
            vec![]
        };
        let limit = if self.parse_keyword(Keyword::LIMIT) {
            self.parse_limit()?
        } else {
            None
        };

        Ok(Statement::Delete(Delete {
            tables,
            from: if with_from_keyword {
                FromTable::WithFromKeyword(from)
            } else {
                FromTable::WithoutKeyword(from)
            },
            using,
            selection,
            returning,
            order_by,
            limit,
        }))
    }
}

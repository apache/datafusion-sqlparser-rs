use crate::parser::*;

impl Parser<'_> {
    /// ```sql
    /// OPTIMIZE TABLE [db.]name [ON CLUSTER cluster] [PARTITION partition | PARTITION ID 'partition_id'] [FINAL] [DEDUPLICATE [BY expression]]
    /// ```
    /// [ClickHouse](https://clickhouse.com/docs/en/sql-reference/statements/optimize)
    pub fn parse_optimize_table(&mut self) -> Result<Statement, ParserError> {
        self.expect_keyword(Keyword::TABLE)?;
        let name = self.parse_object_name(false)?;
        let on_cluster = self.parse_optional_on_cluster()?;

        let partition = if self.parse_keyword(Keyword::PARTITION) {
            if self.parse_keyword(Keyword::ID) {
                Some(Partition::Identifier(self.parse_identifier(false)?))
            } else {
                Some(Partition::Expr(self.parse_expr()?))
            }
        } else {
            None
        };

        let include_final = self.parse_keyword(Keyword::FINAL);
        let deduplicate = if self.parse_keyword(Keyword::DEDUPLICATE) {
            if self.parse_keyword(Keyword::BY) {
                Some(Deduplicate::ByExpression(self.parse_expr()?))
            } else {
                Some(Deduplicate::All)
            }
        } else {
            None
        };

        Ok(Statement::OptimizeTable {
            name,
            on_cluster,
            partition,
            include_final,
            deduplicate,
        })
    }
}

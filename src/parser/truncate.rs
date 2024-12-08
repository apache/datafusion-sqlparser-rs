use crate::parser::*;

impl Parser<'_> {
    pub fn parse_truncate(&mut self) -> Result<Statement, ParserError> {
        let table = self.parse_keyword(Keyword::TABLE);
        let only = self.parse_keyword(Keyword::ONLY);

        let table_names = self
            .parse_comma_separated(|p| p.parse_object_name(false))?
            .into_iter()
            .map(|n| TruncateTableTarget { name: n })
            .collect();

        let mut partitions = None;
        if self.parse_keyword(Keyword::PARTITION) {
            self.expect_token(&Token::LParen)?;
            partitions = Some(self.parse_comma_separated(Parser::parse_expr)?);
            self.expect_token(&Token::RParen)?;
        }

        let mut identity = None;
        let mut cascade = None;

        if dialect_of!(self is PostgreSqlDialect | GenericDialect) {
            identity = if self.parse_keywords(&[Keyword::RESTART, Keyword::IDENTITY]) {
                Some(TruncateIdentityOption::Restart)
            } else if self.parse_keywords(&[Keyword::CONTINUE, Keyword::IDENTITY]) {
                Some(TruncateIdentityOption::Continue)
            } else {
                None
            };

            cascade = if self.parse_keyword(Keyword::CASCADE) {
                Some(TruncateCascadeOption::Cascade)
            } else if self.parse_keyword(Keyword::RESTRICT) {
                Some(TruncateCascadeOption::Restrict)
            } else {
                None
            };
        };

        let on_cluster = self.parse_optional_on_cluster()?;

        Ok(Statement::Truncate {
            table_names,
            partitions,
            table,
            only,
            identity,
            cascade,
            on_cluster,
        })
    }
}

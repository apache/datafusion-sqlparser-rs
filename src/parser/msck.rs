use crate::parser::*;

impl Parser<'_> {
    pub fn parse_msck(&mut self) -> Result<Statement, ParserError> {
        let repair = self.parse_keyword(Keyword::REPAIR);
        self.expect_keyword(Keyword::TABLE)?;
        let table_name = self.parse_object_name(false)?;
        let partition_action = self
            .maybe_parse(|parser| {
                let pa = match parser.parse_one_of_keywords(&[
                    Keyword::ADD,
                    Keyword::DROP,
                    Keyword::SYNC,
                ]) {
                    Some(Keyword::ADD) => Some(AddDropSync::ADD),
                    Some(Keyword::DROP) => Some(AddDropSync::DROP),
                    Some(Keyword::SYNC) => Some(AddDropSync::SYNC),
                    _ => None,
                };
                parser.expect_keyword(Keyword::PARTITIONS)?;
                Ok(pa)
            })?
            .unwrap_or_default();
        Ok(Statement::Msck {
            repair,
            table_name,
            partition_action,
        })
    }
}

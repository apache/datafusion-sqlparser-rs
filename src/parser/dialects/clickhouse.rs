use crate::parser::*;

impl Parser<'_> {
    /// Parse clickhouse [map]
    ///
    /// Syntax
    ///
    /// ```sql
    /// Map(key_data_type, value_data_type)
    /// ```
    ///
    /// [map]: https://clickhouse.com/docs/en/sql-reference/data-types/map
    pub(crate) fn parse_click_house_map_def(
        &mut self,
    ) -> Result<(DataType, DataType), ParserError> {
        self.expect_keyword(Keyword::MAP)?;
        self.expect_token(&Token::LParen)?;
        let key_data_type = self.parse_data_type()?;
        self.expect_token(&Token::Comma)?;
        let value_data_type = self.parse_data_type()?;
        self.expect_token(&Token::RParen)?;

        Ok((key_data_type, value_data_type))
    }

    /// Parse clickhouse [tuple]
    ///
    /// Syntax
    ///
    /// ```sql
    /// Tuple([field_name] field_type, ...)
    /// ```
    ///
    /// [tuple]: https://clickhouse.com/docs/en/sql-reference/data-types/tuple
    pub(crate) fn parse_click_house_tuple_def(&mut self) -> Result<Vec<StructField>, ParserError> {
        self.expect_keyword(Keyword::TUPLE)?;
        self.expect_token(&Token::LParen)?;
        let mut field_defs = vec![];
        loop {
            let (def, _) = self.parse_struct_field_def()?;
            field_defs.push(def);
            if !self.consume_token(&Token::Comma) {
                break;
            }
        }
        self.expect_token(&Token::RParen)?;

        Ok(field_defs)
    }
}

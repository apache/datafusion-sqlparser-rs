use crate::parser::*;

impl<'a> Parser<'a> {
    /// Parse a SQL LOAD statement
    pub fn parse_load(&mut self) -> Result<Statement, ParserError> {
        if self.dialect.supports_load_extension() {
            let extension_name = self.parse_identifier(false)?;
            Ok(Statement::Load { extension_name })
        } else if self.parse_keyword(Keyword::DATA) && self.dialect.supports_load_data() {
            let local = self.parse_one_of_keywords(&[Keyword::LOCAL]).is_some();
            self.expect_keyword(Keyword::INPATH)?;
            let inpath = self.parse_literal_string()?;
            let overwrite = self.parse_one_of_keywords(&[Keyword::OVERWRITE]).is_some();
            self.expect_keyword(Keyword::INTO)?;
            self.expect_keyword(Keyword::TABLE)?;
            let table_name = self.parse_object_name(false)?;
            let partitioned = self.parse_insert_partition()?;
            let table_format = self.parse_load_data_table_format()?;
            Ok(Statement::LoadData {
                local,
                inpath,
                overwrite,
                table_name,
                partitioned,
                table_format,
            })
        } else {
            self.expected(
                "`DATA` or an extension name after `LOAD`",
                self.peek_token(),
            )
        }
    }

    pub fn parse_load_data_table_format(
        &mut self,
    ) -> Result<Option<HiveLoadDataFormat>, ParserError> {
        if self.parse_keyword(Keyword::INPUTFORMAT) {
            let input_format = self.parse_expr()?;
            self.expect_keyword(Keyword::SERDE)?;
            let serde = self.parse_expr()?;
            Ok(Some(HiveLoadDataFormat {
                input_format,
                serde,
            }))
        } else {
            Ok(None)
        }
    }
}

use crate::parser::*;

impl Parser<'_> {
    /// Parse a `DECLARE` statement.
    ///
    /// ```sql
    /// DECLARE name [ BINARY ] [ ASENSITIVE | INSENSITIVE ] [ [ NO ] SCROLL ]
    ///     CURSOR [ { WITH | WITHOUT } HOLD ] FOR query
    /// ```
    ///
    /// The syntax can vary significantly between warehouses. See the grammar
    /// on the warehouse specific function in such cases.
    pub fn parse_declare(&mut self) -> Result<Statement, ParserError> {
        if dialect_of!(self is BigQueryDialect) {
            return self.parse_big_query_declare();
        }
        if dialect_of!(self is SnowflakeDialect) {
            return self.parse_snowflake_declare();
        }
        if dialect_of!(self is MsSqlDialect) {
            return self.parse_mssql_declare();
        }

        let name = self.parse_identifier(false)?;

        let binary = Some(self.parse_keyword(Keyword::BINARY));
        let sensitive = if self.parse_keyword(Keyword::INSENSITIVE) {
            Some(true)
        } else if self.parse_keyword(Keyword::ASENSITIVE) {
            Some(false)
        } else {
            None
        };
        let scroll = if self.parse_keyword(Keyword::SCROLL) {
            Some(true)
        } else if self.parse_keywords(&[Keyword::NO, Keyword::SCROLL]) {
            Some(false)
        } else {
            None
        };

        self.expect_keyword(Keyword::CURSOR)?;
        let declare_type = Some(DeclareType::Cursor);

        let hold = match self.parse_one_of_keywords(&[Keyword::WITH, Keyword::WITHOUT]) {
            Some(keyword) => {
                self.expect_keyword(Keyword::HOLD)?;

                match keyword {
                    Keyword::WITH => Some(true),
                    Keyword::WITHOUT => Some(false),
                    _ => unreachable!(),
                }
            }
            None => None,
        };

        self.expect_keyword(Keyword::FOR)?;

        let query = Some(self.parse_query()?);

        Ok(Statement::Declare {
            stmts: vec![Declare {
                names: vec![name],
                data_type: None,
                assignment: None,
                declare_type,
                binary,
                sensitive,
                scroll,
                hold,
                for_query: query,
            }],
        })
    }
}

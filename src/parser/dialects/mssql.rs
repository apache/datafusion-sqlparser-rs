use crate::parser::*;

impl<'a> Parser<'a> {
    /// Parse a [MsSql] `DECLARE` statement.
    ///
    /// Syntax:
    /// ```text
    /// DECLARE
    // {
    //   { @local_variable [AS] data_type [ = value ] }
    //   | { @cursor_variable_name CURSOR }
    // } [ ,...n ]
    /// ```
    /// [MsSql]: https://learn.microsoft.com/en-us/sql/t-sql/language-elements/declare-local-variable-transact-sql?view=sql-server-ver16
    pub fn parse_mssql_declare(&mut self) -> Result<Statement, ParserError> {
        let stmts = self.parse_comma_separated(Parser::parse_mssql_declare_stmt)?;

        Ok(Statement::Declare { stmts })
    }

    /// Parse the body of a [MsSql] `DECLARE`statement.
    ///
    /// Syntax:
    /// ```text
    // {
    //   { @local_variable [AS] data_type [ = value ] }
    //   | { @cursor_variable_name CURSOR }
    // } [ ,...n ]
    /// ```
    /// [MsSql]: https://learn.microsoft.com/en-us/sql/t-sql/language-elements/declare-local-variable-transact-sql?view=sql-server-ver16
    pub fn parse_mssql_declare_stmt(&mut self) -> Result<Declare, ParserError> {
        let name = {
            let ident = self.parse_identifier(false)?;
            if !ident.value.starts_with('@') {
                Err(ParserError::TokenizerError(
                    "Invalid MsSql variable declaration.".to_string(),
                ))
            } else {
                Ok(ident)
            }
        }?;

        let (declare_type, data_type) = match self.peek_token().token {
            Token::Word(w) => match w.keyword {
                Keyword::CURSOR => {
                    self.next_token();
                    (Some(DeclareType::Cursor), None)
                }
                Keyword::AS => {
                    self.next_token();
                    (None, Some(self.parse_data_type()?))
                }
                _ => (None, Some(self.parse_data_type()?)),
            },
            _ => (None, Some(self.parse_data_type()?)),
        };

        let assignment = self.parse_mssql_variable_declaration_expression()?;

        Ok(Declare {
            names: vec![name],
            data_type,
            assignment,
            declare_type,
            binary: None,
            sensitive: None,
            scroll: None,
            hold: None,
            for_query: None,
        })
    }

    /// Parses the assigned expression in a variable declaration.
    ///
    /// Syntax:
    /// ```text
    /// [ = <expression>]
    /// ```
    pub fn parse_mssql_variable_declaration_expression(
        &mut self,
    ) -> Result<Option<DeclareAssignment>, ParserError> {
        Ok(match self.peek_token().token {
            Token::Eq => {
                self.next_token(); // Skip `=`
                Some(DeclareAssignment::MsSqlAssignment(Box::new(
                    self.parse_expr()?,
                )))
            }
            _ => None,
        })
    }

    pub(crate) fn parse_mssql_alter_role(&mut self) -> Result<Statement, ParserError> {
        let role_name = self.parse_identifier(false)?;

        let operation = if self.parse_keywords(&[Keyword::ADD, Keyword::MEMBER]) {
            let member_name = self.parse_identifier(false)?;
            AlterRoleOperation::AddMember { member_name }
        } else if self.parse_keywords(&[Keyword::DROP, Keyword::MEMBER]) {
            let member_name = self.parse_identifier(false)?;
            AlterRoleOperation::DropMember { member_name }
        } else if self.parse_keywords(&[Keyword::WITH, Keyword::NAME]) {
            if self.consume_token(&Token::Eq) {
                let role_name = self.parse_identifier(false)?;
                AlterRoleOperation::RenameRole { role_name }
            } else {
                return self.expected("= after WITH NAME ", self.peek_token());
            }
        } else {
            return self.expected("'ADD' or 'DROP' or 'WITH NAME'", self.peek_token());
        };

        Ok(Statement::AlterRole {
            name: role_name,
            operation,
        })
    }

    /// Parse a mssql `FOR [XML | JSON | BROWSE]` clause
    pub fn parse_for_clause(&mut self) -> Result<Option<ForClause>, ParserError> {
        if self.parse_keyword(Keyword::XML) {
            Ok(Some(self.parse_for_xml()?))
        } else if self.parse_keyword(Keyword::JSON) {
            Ok(Some(self.parse_for_json()?))
        } else if self.parse_keyword(Keyword::BROWSE) {
            Ok(Some(ForClause::Browse))
        } else {
            Ok(None)
        }
    }

    /// Parse a mssql `FOR XML` clause
    pub fn parse_for_xml(&mut self) -> Result<ForClause, ParserError> {
        let for_xml = if self.parse_keyword(Keyword::RAW) {
            let mut element_name = None;
            if self.peek_token().token == Token::LParen {
                self.expect_token(&Token::LParen)?;
                element_name = Some(self.parse_literal_string()?);
                self.expect_token(&Token::RParen)?;
            }
            ForXml::Raw(element_name)
        } else if self.parse_keyword(Keyword::AUTO) {
            ForXml::Auto
        } else if self.parse_keyword(Keyword::EXPLICIT) {
            ForXml::Explicit
        } else if self.parse_keyword(Keyword::PATH) {
            let mut element_name = None;
            if self.peek_token().token == Token::LParen {
                self.expect_token(&Token::LParen)?;
                element_name = Some(self.parse_literal_string()?);
                self.expect_token(&Token::RParen)?;
            }
            ForXml::Path(element_name)
        } else {
            return Err(ParserError::ParserError(
                "Expected FOR XML [RAW | AUTO | EXPLICIT | PATH ]".to_string(),
            ));
        };
        let mut elements = false;
        let mut binary_base64 = false;
        let mut root = None;
        let mut r#type = false;
        while self.peek_token().token == Token::Comma {
            self.next_token();
            if self.parse_keyword(Keyword::ELEMENTS) {
                elements = true;
            } else if self.parse_keyword(Keyword::BINARY) {
                self.expect_keyword(Keyword::BASE64)?;
                binary_base64 = true;
            } else if self.parse_keyword(Keyword::ROOT) {
                self.expect_token(&Token::LParen)?;
                root = Some(self.parse_literal_string()?);
                self.expect_token(&Token::RParen)?;
            } else if self.parse_keyword(Keyword::TYPE) {
                r#type = true;
            }
        }
        Ok(ForClause::Xml {
            for_xml,
            elements,
            binary_base64,
            root,
            r#type,
        })
    }

    /// Parse a mssql `FOR JSON` clause
    pub fn parse_for_json(&mut self) -> Result<ForClause, ParserError> {
        let for_json = if self.parse_keyword(Keyword::AUTO) {
            ForJson::Auto
        } else if self.parse_keyword(Keyword::PATH) {
            ForJson::Path
        } else {
            return Err(ParserError::ParserError(
                "Expected FOR JSON [AUTO | PATH ]".to_string(),
            ));
        };
        let mut root = None;
        let mut include_null_values = false;
        let mut without_array_wrapper = false;
        while self.peek_token().token == Token::Comma {
            self.next_token();
            if self.parse_keyword(Keyword::ROOT) {
                self.expect_token(&Token::LParen)?;
                root = Some(self.parse_literal_string()?);
                self.expect_token(&Token::RParen)?;
            } else if self.parse_keyword(Keyword::INCLUDE_NULL_VALUES) {
                include_null_values = true;
            } else if self.parse_keyword(Keyword::WITHOUT_ARRAY_WRAPPER) {
                without_array_wrapper = true;
            }
        }
        Ok(ForClause::Json {
            for_json,
            root,
            include_null_values,
            without_array_wrapper,
        })
    }

    /// mssql-like convert function
    pub(crate) fn parse_mssql_convert(&mut self, is_try: bool) -> Result<Expr, ParserError> {
        self.expect_token(&Token::LParen)?;
        let data_type = self.parse_data_type()?;
        self.expect_token(&Token::Comma)?;
        let expr = self.parse_expr()?;
        let styles = if self.consume_token(&Token::Comma) {
            self.parse_comma_separated(Parser::parse_expr)?
        } else {
            Default::default()
        };
        self.expect_token(&Token::RParen)?;
        Ok(Expr::Convert {
            is_try,
            expr: Box::new(expr),
            data_type: Some(data_type),
            charset: None,
            target_before_value: true,
            styles,
        })
    }
}

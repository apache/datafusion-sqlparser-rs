use crate::keywords::ALL_KEYWORDS;
use crate::parser::*;

impl<'a> Parser<'a> {
    /// Parse a [Snowflake] `DECLARE` statement.
    ///
    /// Syntax:
    /// ```text
    /// DECLARE
    ///   [{ <variable_declaration>
    ///      | <cursor_declaration>
    ///      | <resultset_declaration>
    ///      | <exception_declaration> }; ... ]
    ///
    /// <variable_declaration>
    /// <variable_name> [<type>] [ { DEFAULT | := } <expression>]
    ///
    /// <cursor_declaration>
    /// <cursor_name> CURSOR FOR <query>
    ///
    /// <resultset_declaration>
    /// <resultset_name> RESULTSET [ { DEFAULT | := } ( <query> ) ] ;
    ///
    /// <exception_declaration>
    /// <exception_name> EXCEPTION [ ( <exception_number> , '<exception_message>' ) ] ;
    /// ```
    ///
    /// [Snowflake]: https://docs.snowflake.com/en/sql-reference/snowflake-scripting/declare
    pub fn parse_snowflake_declare(&mut self) -> Result<Statement, ParserError> {
        let mut stmts = vec![];
        loop {
            let name = self.parse_identifier(false)?;
            let (declare_type, for_query, assigned_expr, data_type) =
                if self.parse_keyword(Keyword::CURSOR) {
                    self.expect_keyword(Keyword::FOR)?;
                    match self.peek_token().token {
                        Token::Word(w) if w.keyword == Keyword::SELECT => (
                            Some(DeclareType::Cursor),
                            Some(self.parse_query()?),
                            None,
                            None,
                        ),
                        _ => (
                            Some(DeclareType::Cursor),
                            None,
                            Some(DeclareAssignment::For(Box::new(self.parse_expr()?))),
                            None,
                        ),
                    }
                } else if self.parse_keyword(Keyword::RESULTSET) {
                    let assigned_expr = if self.peek_token().token != Token::SemiColon {
                        self.parse_snowflake_variable_declaration_expression()?
                    } else {
                        // Nothing more to do. The statement has no further parameters.
                        None
                    };

                    (Some(DeclareType::ResultSet), None, assigned_expr, None)
                } else if self.parse_keyword(Keyword::EXCEPTION) {
                    let assigned_expr = if self.peek_token().token == Token::LParen {
                        Some(DeclareAssignment::Expr(Box::new(self.parse_expr()?)))
                    } else {
                        // Nothing more to do. The statement has no further parameters.
                        None
                    };

                    (Some(DeclareType::Exception), None, assigned_expr, None)
                } else {
                    // Without an explicit keyword, the only valid option is variable declaration.
                    let (assigned_expr, data_type) = if let Some(assigned_expr) =
                        self.parse_snowflake_variable_declaration_expression()?
                    {
                        (Some(assigned_expr), None)
                    } else if let Token::Word(_) = self.peek_token().token {
                        let data_type = self.parse_data_type()?;
                        (
                            self.parse_snowflake_variable_declaration_expression()?,
                            Some(data_type),
                        )
                    } else {
                        (None, None)
                    };
                    (None, None, assigned_expr, data_type)
                };
            let stmt = Declare {
                names: vec![name],
                data_type,
                assignment: assigned_expr,
                declare_type,
                binary: None,
                sensitive: None,
                scroll: None,
                hold: None,
                for_query,
            };

            stmts.push(stmt);
            if self.consume_token(&Token::SemiColon) {
                match self.peek_token().token {
                    Token::Word(w)
                        if ALL_KEYWORDS
                            .binary_search(&w.value.to_uppercase().as_str())
                            .is_err() =>
                    {
                        // Not a keyword - start of a new declaration.
                        continue;
                    }
                    _ => {
                        // Put back the semicolon, this is the end of the DECLARE statement.
                        self.prev_token();
                    }
                }
            }

            break;
        }

        Ok(Statement::Declare { stmts })
    }

    /// Parses the assigned expression in a variable declaration.
    ///
    /// Syntax:
    /// ```text
    /// [ { DEFAULT | := } <expression>]
    /// ```
    /// <https://docs.snowflake.com/en/sql-reference/snowflake-scripting/declare#variable-declaration-syntax>
    pub fn parse_snowflake_variable_declaration_expression(
        &mut self,
    ) -> Result<Option<DeclareAssignment>, ParserError> {
        Ok(match self.peek_token().token {
            Token::Word(w) if w.keyword == Keyword::DEFAULT => {
                self.next_token(); // Skip `DEFAULT`
                Some(DeclareAssignment::Default(Box::new(self.parse_expr()?)))
            }
            Token::Assignment => {
                self.next_token(); // Skip `:=`
                Some(DeclareAssignment::DuckAssignment(Box::new(
                    self.parse_expr()?,
                )))
            }
            _ => None,
        })
    }
}

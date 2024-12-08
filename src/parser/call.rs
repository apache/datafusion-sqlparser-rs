use crate::parser::*;

use crate::parser_err;

impl Parser<'_> {
    /// Parse a `CALL procedure_name(arg1, arg2, ...)`
    /// or `CALL procedure_name` statement
    pub fn parse_call(&mut self) -> Result<Statement, ParserError> {
        let object_name = self.parse_object_name(false)?;
        if self.peek_token().token == Token::LParen {
            match self.parse_function(object_name)? {
                Expr::Function(f) => Ok(Statement::Call(f)),
                other => parser_err!(
                    format!("Expected a simple procedure call but found: {other}"),
                    self.peek_token().span.start
                ),
            }
        } else {
            Ok(Statement::Call(Function {
                name: object_name,
                parameters: FunctionArguments::None,
                args: FunctionArguments::None,
                over: None,
                filter: None,
                null_treatment: None,
                within_group: vec![],
            }))
        }
    }

    pub fn parse_function_desc(&mut self) -> Result<FunctionDesc, ParserError> {
        let name = self.parse_object_name(false)?;

        let args = if self.consume_token(&Token::LParen) {
            if self.consume_token(&Token::RParen) {
                None
            } else {
                let args = self.parse_comma_separated(Parser::parse_function_arg)?;
                self.expect_token(&Token::RParen)?;
                Some(args)
            }
        } else {
            None
        };

        Ok(FunctionDesc { name, args })
    }

    pub(crate) fn parse_function_arg(&mut self) -> Result<OperateFunctionArg, ParserError> {
        let mode = if self.parse_keyword(Keyword::IN) {
            Some(ArgMode::In)
        } else if self.parse_keyword(Keyword::OUT) {
            Some(ArgMode::Out)
        } else if self.parse_keyword(Keyword::INOUT) {
            Some(ArgMode::InOut)
        } else {
            None
        };

        // parse: [ argname ] argtype
        let mut name = None;
        let mut data_type = self.parse_data_type()?;
        if let DataType::Custom(n, _) = &data_type {
            // the first token is actually a name
            name = Some(n.0[0].clone());
            data_type = self.parse_data_type()?;
        }

        let default_expr = if self.parse_keyword(Keyword::DEFAULT) || self.consume_token(&Token::Eq)
        {
            Some(self.parse_expr()?)
        } else {
            None
        };
        Ok(OperateFunctionArg {
            mode,
            name,
            data_type,
            default_expr,
        })
    }
}

use crate::parser::*;

impl Parser<'_> {
    /// Parse the body of a `CREATE FUNCTION` specified as a string.
    /// e.g. `CREATE FUNCTION ... AS $$ body $$`.
    pub(crate) fn parse_create_function_body_string(&mut self) -> Result<Expr, ParserError> {
        let peek_token = self.peek_token();
        match peek_token.token {
            Token::DollarQuotedString(s) if dialect_of!(self is PostgreSqlDialect | GenericDialect) =>
            {
                self.next_token();
                Ok(Expr::Value(Value::DollarQuotedString(s)))
            }
            _ => Ok(Expr::Value(Value::SingleQuotedString(
                self.parse_literal_string()?,
            ))),
        }
    }
}

use crate::parser::*;

impl<'a> Parser<'a> {
    /// Parse a `var = expr` assignment, used in an UPDATE statement
    pub fn parse_assignment(&mut self) -> Result<Assignment, ParserError> {
        let target = self.parse_assignment_target()?;
        self.expect_token(&Token::Eq)?;
        let value = self.parse_expr()?;
        Ok(Assignment { target, value })
    }

    /// Parse the left-hand side of an assignment, used in an UPDATE statement
    pub fn parse_assignment_target(&mut self) -> Result<AssignmentTarget, ParserError> {
        if self.consume_token(&Token::LParen) {
            let columns = self.parse_comma_separated(|p| p.parse_object_name(false))?;
            self.expect_token(&Token::RParen)?;
            Ok(AssignmentTarget::Tuple(columns))
        } else {
            let column = self.parse_object_name(false)?;
            Ok(AssignmentTarget::ColumnName(column))
        }
    }
}

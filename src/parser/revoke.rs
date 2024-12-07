use crate::parser::*;

use crate::parser_err;

impl<'a> Parser<'a> {
    /// Parse a REVOKE statement
    pub fn parse_revoke(&mut self) -> Result<Statement, ParserError> {
        let (privileges, objects) = self.parse_grant_revoke_privileges_objects()?;

        self.expect_keyword(Keyword::FROM)?;
        let grantees = self.parse_comma_separated(|p| p.parse_identifier(false))?;

        let granted_by = self
            .parse_keywords(&[Keyword::GRANTED, Keyword::BY])
            .then(|| self.parse_identifier(false).unwrap());

        let loc = self.peek_token().span.start;
        let cascade = self.parse_keyword(Keyword::CASCADE);
        let restrict = self.parse_keyword(Keyword::RESTRICT);
        if cascade && restrict {
            return parser_err!("Cannot specify both CASCADE and RESTRICT in REVOKE", loc);
        }

        Ok(Statement::Revoke {
            privileges,
            objects,
            grantees,
            granted_by,
            cascade,
        })
    }
}

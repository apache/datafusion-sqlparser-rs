use crate::parser::*;

impl<'a> Parser<'a> {
    pub fn parse_use(&mut self) -> Result<Statement, ParserError> {
        // Determine which keywords are recognized by the current dialect
        let parsed_keyword = if dialect_of!(self is HiveDialect) {
            // HiveDialect accepts USE DEFAULT; statement without any db specified
            if self.parse_keyword(Keyword::DEFAULT) {
                return Ok(Statement::Use(Use::Default));
            }
            None // HiveDialect doesn't expect any other specific keyword after `USE`
        } else if dialect_of!(self is DatabricksDialect) {
            self.parse_one_of_keywords(&[Keyword::CATALOG, Keyword::DATABASE, Keyword::SCHEMA])
        } else if dialect_of!(self is SnowflakeDialect) {
            self.parse_one_of_keywords(&[
                Keyword::DATABASE,
                Keyword::SCHEMA,
                Keyword::WAREHOUSE,
                Keyword::ROLE,
                Keyword::SECONDARY,
            ])
        } else {
            None // No specific keywords for other dialects, including GenericDialect
        };

        let result = if matches!(parsed_keyword, Some(Keyword::SECONDARY)) {
            self.parse_secondary_roles()?
        } else {
            let obj_name = self.parse_object_name(false)?;
            match parsed_keyword {
                Some(Keyword::CATALOG) => Use::Catalog(obj_name),
                Some(Keyword::DATABASE) => Use::Database(obj_name),
                Some(Keyword::SCHEMA) => Use::Schema(obj_name),
                Some(Keyword::WAREHOUSE) => Use::Warehouse(obj_name),
                Some(Keyword::ROLE) => Use::Role(obj_name),
                _ => Use::Object(obj_name),
            }
        };

        Ok(Statement::Use(result))
    }

    fn parse_secondary_roles(&mut self) -> Result<Use, ParserError> {
        self.expect_keyword(Keyword::ROLES)?;
        if self.parse_keyword(Keyword::NONE) {
            Ok(Use::SecondaryRoles(SecondaryRoles::None))
        } else if self.parse_keyword(Keyword::ALL) {
            Ok(Use::SecondaryRoles(SecondaryRoles::All))
        } else {
            let roles = self.parse_comma_separated(|parser| parser.parse_identifier(false))?;
            Ok(Use::SecondaryRoles(SecondaryRoles::List(roles)))
        }
    }

    /// Parse a `SET ROLE` statement. Expects SET to be consumed already.
    pub(crate) fn parse_set_role(
        &mut self,
        modifier: Option<Keyword>,
    ) -> Result<Statement, ParserError> {
        self.expect_keyword(Keyword::ROLE)?;
        let context_modifier = match modifier {
            Some(Keyword::LOCAL) => ContextModifier::Local,
            Some(Keyword::SESSION) => ContextModifier::Session,
            _ => ContextModifier::None,
        };

        let role_name = if self.parse_keyword(Keyword::NONE) {
            None
        } else {
            Some(self.parse_identifier(false)?)
        };
        Ok(Statement::SetRole {
            context_modifier,
            role_name,
        })
    }
}

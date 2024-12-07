use crate::parser::*;

impl<'a> Parser<'a> {
    pub fn parse_comment(&mut self) -> Result<Statement, ParserError> {
        let if_exists = self.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);

        self.expect_keyword(Keyword::ON)?;
        let token = self.next_token();

        let (object_type, object_name) = match token.token {
            Token::Word(w) if w.keyword == Keyword::COLUMN => {
                (CommentObject::Column, self.parse_object_name(false)?)
            }
            Token::Word(w) if w.keyword == Keyword::TABLE => {
                (CommentObject::Table, self.parse_object_name(false)?)
            }
            Token::Word(w) if w.keyword == Keyword::EXTENSION => {
                (CommentObject::Extension, self.parse_object_name(false)?)
            }
            Token::Word(w) if w.keyword == Keyword::SCHEMA => {
                (CommentObject::Schema, self.parse_object_name(false)?)
            }
            Token::Word(w) if w.keyword == Keyword::DATABASE => {
                (CommentObject::Database, self.parse_object_name(false)?)
            }
            Token::Word(w) if w.keyword == Keyword::USER => {
                (CommentObject::User, self.parse_object_name(false)?)
            }
            Token::Word(w) if w.keyword == Keyword::ROLE => {
                (CommentObject::Role, self.parse_object_name(false)?)
            }
            _ => self.expected("comment object_type", token)?,
        };

        self.expect_keyword(Keyword::IS)?;
        let comment = if self.parse_keyword(Keyword::NULL) {
            None
        } else {
            Some(self.parse_literal_string()?)
        };
        Ok(Statement::Comment {
            object_type,
            object_name,
            comment,
            if_exists,
        })
    }
}

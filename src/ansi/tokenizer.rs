use std::cmp::PartialEq;
use std::fmt::Debug;
use std::iter::Peekable;
use std::str::Chars;

use super::super::tokenizer::*;

pub struct ANSISQLTokenizer<'a> {
    pub chars: Peekable<Chars<'a>>
}

impl<'a, TokenType> SQLTokenizer<TokenType> for ANSISQLTokenizer<'a>
    where TokenType: Debug + PartialEq {

    fn precedence(&self, _token: &SQLToken<TokenType>) -> usize {
        unimplemented!()
    }

    fn peek_token(&mut self) -> Result<Option<SQLToken<TokenType>>, TokenizerError<TokenType>> {
        unimplemented!()
    }

    fn next_token(&mut self) -> Result<Option<SQLToken<TokenType>>, TokenizerError<TokenType>> {
        match self.chars.next() {
            Some(ch) => match ch {
                ' ' | '\t' | '\n' => Ok(Some(SQLToken::Whitespace(ch))),
                '0' ... '9' => {
                    let mut s = String::new();
                    s.push(ch);
                    while let Some(&ch) = self.chars.peek() {
                        match ch {
                            '0' ... '9' => {
                                self.chars.next(); // consume
                                s.push(ch);
                            },
                            _ => break
                        }
                    }
                    Ok(Some(SQLToken::Literal(s)))
                },
                '+' => Ok(Some(SQLToken::Plus)),
                '-' => Ok(Some(SQLToken::Minus)),
                '*' => Ok(Some(SQLToken::Mult)),
                '/' => Ok(Some(SQLToken::Divide)),
                _ => Err(TokenizerError::UnexpectedChar(ch,Position::new(0, 0)))
            },
            None => Ok(None)
        }
    }

    fn peek_char(&mut self) -> Option<&char> {
        unimplemented!()
    }

    fn next_char(&mut self) -> Option<&char> {
        unimplemented!()
    }
}


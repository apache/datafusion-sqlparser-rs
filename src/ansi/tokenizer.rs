use std::cmp::PartialEq;
use std::fmt::Debug;
use std::iter::Peekable;
use std::str::Chars;

use super::super::tokenizer::*;

pub struct ANSISQLTokenizer {}

impl<TokenType> SQLTokenizer<TokenType> for ANSISQLTokenizer
    where TokenType: Debug + PartialEq {

    fn precedence(&self, _token: &SQLToken<TokenType>) -> usize {
        unimplemented!()
    }

    fn peek_token(&self, _chars: &mut Peekable<Chars>) -> Result<Option<SQLToken<TokenType>>, TokenizerError<TokenType>> {
        unimplemented!()
    }

    fn next_token(&self, chars: &mut Peekable<Chars>) -> Result<Option<SQLToken<TokenType>>, TokenizerError<TokenType>> {
        match chars.next() {
            Some(ch) => match ch {
                ' ' | '\t' | '\n' => Ok(Some(SQLToken::Whitespace(ch))),
                '0' ... '9' => {
                    let mut s = String::new();
                    s.push(ch);
                    while let Some(&ch) = chars.peek() {
                        match ch {
                            '0' ... '9' => {
                                chars.next(); // consume
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
}


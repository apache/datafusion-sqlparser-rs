use std::cmp::PartialEq;
use std::fmt::Debug;

use super::super::tokenizer::*;

pub struct ANSISQLTokenizer {
    chars: CharSeq
}

impl ANSISQLTokenizer {
    pub fn new(sql: &str) -> Self {
        ANSISQLTokenizer { chars: CharSeq::new(sql) }
    }
}

impl SQLTokenizer for ANSISQLTokenizer {

    fn precedence(&self, _token: &SQLToken) -> usize {
        unimplemented!()
    }

    fn peek_token(&mut self) -> Result<Option<SQLToken>, TokenizerError> {
        unimplemented!()
    }


    fn next_token(&mut self) -> Result<Option<SQLToken>, TokenizerError> {
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

}


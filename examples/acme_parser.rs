use std::str::Chars;
use std::iter::Peekable;

extern crate datafusion_sql;

use datafusion_sql::ansi::tokenizer::ANSISQLTokenizer;
use datafusion_sql::tokenizer::*;
use datafusion_sql::parser::*;

///
/// This example demonstrates building a custom ACME parser that extends the generic parser
/// by adding support for a factorial operator !!
///

#[derive(Debug,PartialEq)]
enum AcmeToken {
    /// Factorial operator `!!`
    Factorial
}

#[derive(Debug)]
enum AcmeOperator {
    Factorial
}

#[derive(Debug)]
enum AcmeTokenizerError {
}

struct AcmeTokenizer {
    generic: ANSISQLTokenizer
}

/// The ACME tokenizer looks for the factorial operator `!!` but delegates everything else
impl SQLTokenizer<AcmeToken, AcmeTokenizerError> for AcmeTokenizer {

    fn next_token(&self, chars: &mut Peekable<Chars>) -> Result<Option<SQLToken<AcmeToken>>, TokenizerError<AcmeTokenizerError>> {
        match chars.peek() {
            Some(&ch) => match ch {
                '!' => {
                    chars.next(); // consume the first `!`
                    match chars.peek() {
                        Some(&ch) => match ch {
                            '!' => {
                                chars.next(); // consume the second `!`
                                Ok(Some(SQLToken::Custom(AcmeToken::Factorial)))
                            },
                            _ => Err(TokenizerError::UnexpectedChar(ch,Position::new(0,0)))
                        },
                        None => Ok(Some(SQLToken::Not))
                    }
                },
                _ => self.generic.next_token(chars)
            }
            _ => self.generic.next_token(chars)
        }
    }
}



fn main() {

    let sql = "1 + !! 5 * 2";

    let mut acme_tokenizer = AcmeTokenizer {
        generic: ANSISQLTokenizer { }
    };

    let tokens = tokenize(&sql, &mut acme_tokenizer).unwrap();

    println!("tokens = {:?}", tokens);



}

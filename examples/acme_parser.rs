use std::str::Chars;

extern crate datafusion_sql;

use datafusion_sql::tokenizer::*;
use datafusion_sql::parser::*;

#[derive(Debug)]
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
    //chars: &'a Chars
}

impl SQLTokenizer<AcmeToken, AcmeTokenizerError> for AcmeTokenizer {

    fn peek_token(&mut self) -> Result<Option<SQLToken<AcmeToken>>, TokenizerError<AcmeTokenizerError>> {
        Ok(Some(SQLToken::Custom(AcmeToken::Factorial)))
    }

    fn next_token(&mut self) -> Result<Option<SQLToken<AcmeToken>>, TokenizerError<AcmeTokenizerError>> {
        Ok(Some(SQLToken::Custom(AcmeToken::Factorial)))
    }
}



fn main() {

    let sql = "1 + !! 5 * 2";

    let mut tokenizer = AcmeTokenizer { };

    println!("token = {:?}", tokenizer.peek_token().unwrap());



}

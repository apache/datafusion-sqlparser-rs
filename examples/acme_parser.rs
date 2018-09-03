use std::sync::{Arc, Mutex};

extern crate datafusion_sql;

use datafusion_sql::ansi::tokenizer::ANSISQLTokenizer;
use datafusion_sql::ansi::parser::ANSISQLParser;
use datafusion_sql::tokenizer::*;
use datafusion_sql::parser::*;

/// This example demonstrates building a custom ACME parser that extends the generic parser
/// by adding support for a factorial expression `!! expr`.

/// Custom SQLToken
#[derive(Debug,PartialEq)]
enum AcmeToken {
    /// Factorial token `!!`
    Factorial
}

/// Custom SQLExpr
#[derive(Debug)]
enum AcmeExpr {
    /// Factorial expression
    Factorial(Box<SQLExpr<AcmeExpr>>)
}

struct AcmeTokenizer {
    ansi_tokenizer: Arc<Mutex<SQLTokenizer<AcmeToken>>>
}

/// The ACME tokenizer looks for the factorial operator `!!` but delegates everything else
impl SQLTokenizer<AcmeToken> for AcmeTokenizer {

    fn precedence(&self, _token: &SQLToken<AcmeToken>) -> usize {
        unimplemented!()
    }

    fn next_token(&mut self, chars: &mut CharSeq) -> Result<Option<SQLToken<AcmeToken>>, TokenizerError> {
//        let mut arc = self.ansi_tokenizer.lock().unwrap();
//        match arc.peek_char() {
//            Some(&ch) => match ch {
//                '!' => {
//                    arc.next_char(); // consume the first `!`
//                    match arc.peek_char() {
//                        Some(&ch) => match ch {
//                            '!' => {
//                                arc.next_char(); // consume the second `!`
//                                Ok(Some(SQLToken::Custom(AcmeToken::Factorial)))
//                            },
//                            _ => Err(TokenizerError::UnexpectedChar(ch,Position::new(0,0)))
//                        },
//                        None => Ok(Some(SQLToken::Not))
//                    }
//                }
//                _ => arc.next_token()
//            }
//            _ => arc.next_token()
//        }
        unimplemented!()
    }

}

struct AcmeParser {
    tokenizer: Arc<Mutex<SQLTokenizer<AcmeToken>>>
}

impl AcmeParser {

    pub fn new(tokenizer: Arc<Mutex<SQLTokenizer<AcmeToken>>>) -> Self {
        AcmeParser { tokenizer: tokenizer.clone() }
    }

}
impl SQLParser<AcmeToken, AcmeExpr> for AcmeParser {

    fn parse_prefix(&mut self, chars: &mut CharSeq) -> Result<Option<Box<SQLExpr<AcmeExpr>>>, ParserError<AcmeToken>> {
        Ok(None)
    }

    fn parse_infix(&mut self, chars: &mut CharSeq, left: &SQLExpr<AcmeExpr>, precedence: usize) -> Result<Option<Box<SQLExpr<AcmeExpr>>>, ParserError<AcmeToken>> {
        Ok(None)
    }
}

fn main() {

    let sql = "1 + !! 5 * 2";

    // ANSI SQL tokenizer
    let ansi_tokenizer = Arc::new(Mutex::new(ANSISQLTokenizer { }));

    // Custom ACME tokenizer
    let mut acme_tokenizer = Arc::new(Mutex::new(AcmeTokenizer {
        ansi_tokenizer: ansi_tokenizer.clone()
    }));

    // Create parsers
    let ansi_parser = Arc::new(Mutex::new(ANSISQLParser::new(acme_tokenizer.clone())));
    let acme_parser = Arc::new(Mutex::new(AcmeParser::new(acme_tokenizer.clone())));

    //let parser_list: Vec<Arc<Mutex<SQLParser<>>>> = vec![acme_parser, ansi_parser];

    // Custom ACME parser
//    let acme_parser: Arc<Mutex<SQLParser<AcmeToken, AcmeExpr>>> = Arc::new(Mutex::new(AcmeParser {
//        ansi_parser: Arc::new(Mutex::new(ANSISQLParser::new(acme_tokenizer)))
//    }));

//    let expr = parse_expr(acme_parser).unwrap();
//
//    println!("Parsed: {:?}", expr);
}

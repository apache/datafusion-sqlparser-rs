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
        let mut ansi = self.ansi_tokenizer.lock().unwrap();
        match chars.peek() {
            Some(&ch) => match ch {
                '!' => {
                    chars.mark();
                    chars.next(); // consume the first `!`
                    match chars.peek() {
                        Some(&ch) => match ch {
                            '!' => {
                                chars.next(); // consume the second `!`
                                Ok(Some(SQLToken::Custom(AcmeToken::Factorial)))
                            },
                            _ => {
                                chars.reset();
                                ansi.next_token(chars)
                            }
                        },
                        None => {
                            chars.reset();
                            ansi.next_token(chars)
                        }
                    }
                }
                _ => ansi.next_token(chars)
            }
            _ => ansi.next_token(chars)
        }
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

    let mut pratt_parser = PrattParser {
        chars: CharSeq::new(sql),
        parser: acme_parser
    };

    let expr = pratt_parser.parse_expr().unwrap();
    println!("{:?}", expr);
}

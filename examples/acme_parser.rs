use std::str::Chars;
use std::iter::Peekable;
use std::rc::Rc;

extern crate datafusion_sql;

use datafusion_sql::ansi::tokenizer::ANSISQLTokenizer;
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
    generic: ANSISQLTokenizer
}

/// The ACME tokenizer looks for the factorial operator `!!` but delegates everything else
impl SQLTokenizer<AcmeToken> for AcmeTokenizer {

    fn precedence(&self, token: &SQLToken<AcmeToken>) -> usize {
        unimplemented!()
    }

    fn peek_token(&self, chars: &mut Peekable<Chars>) -> Result<Option<SQLToken<AcmeToken>>, TokenizerError<AcmeToken>> {
        unimplemented!()
    }

    fn next_token(&self, chars: &mut Peekable<Chars>) -> Result<Option<SQLToken<AcmeToken>>, TokenizerError<AcmeToken>> {
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
                }
                _ => self.generic.next_token(chars)
            }
            _ => self.generic.next_token(chars)
        }
    }
}

struct AcmeParser {
    tokenizer: Rc<SQLTokenizer<AcmeToken>>
}
//
//impl<'a> AcmeParser<'a> {
//
//    pub fn new(sql: &'a str) -> Self {
//        AcmeParser {
//            chars: sql.chars().peekable()
//        }
//    }
//}

impl SQLParser<AcmeToken, AcmeExpr> for AcmeParser {

    fn parse_prefix(&mut self) -> Result<Box<SQLExpr<AcmeExpr>>, ParserError<AcmeToken>> {
        unimplemented!()
    }

    fn parse_infix(&mut self, left: &SQLExpr<AcmeExpr>, _precedence: usize) -> Result<Option<Box<SQLExpr<AcmeExpr>>>, ParserError<AcmeToken>> {
        unimplemented!()
    }
}

fn main() {

    let sql = "1 + !! 5 * 2";

//    let acme_parser = AcmeParser::new(sql);


    //acme_parser

    let mut acme_tokenizer: Rc<SQLTokenizer<AcmeToken>> = Rc::new(AcmeTokenizer {
        generic: ANSISQLTokenizer { }
    });

    let mut acme_parser: Rc<SQLParser<AcmeToken, AcmeExpr>> = Rc::new(AcmeParser {
        tokenizer: acme_tokenizer.clone()
    });

//    let mut pratt_parser = Rc::new(PrattParser {
//        chars: sql.chars().peekable(),
//        tokenizer: acme_tokenizer.clone(),
//        parser: acme_parser.clone()
//    });

    let mut chars = sql.chars().peekable();

    let expr = parse_expr(acme_tokenizer, acme_parser, &mut chars);

    println!("Parsed: {:?}", expr);
//
//    let tokens = tokenize(&sql, &mut acme_tokenizer).unwrap();
//
//    println!("tokens = {:?}", tokens);



}

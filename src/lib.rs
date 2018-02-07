
/* --- TOKENIZER API --- */

enum TokenizerError {
    WrongToken { expected: SQLToken, actual: SQLToken, line: usize, col: usize },
    TBD
}

/// SQL Tokens
enum SQLToken {
    Keyword(String),
    Identifier(String),
    Eq,
    Gt,
    GtEq,
    Lt,
    LtEq,
    LParen,
    RParen,
    Comma,
    Custom(Box<CustomToken>) // extension point for vendor-specific tokens
}

trait CustomToken {
    //TODO: ???
}

trait SQLTokenizer<'a> {
    // return a reference to the next token without consuming it (look ahead)
    fn peek_token(&'a mut self) -> Result<Option<&'a SQLToken>, Box<TokenizerError>>;
    // return a reference to the next token and advance the index
    fn next_token(&'a mut self) -> Result<Option<&'a SQLToken>, Box<TokenizerError>>;
}

/* --- PARSER API --- */

/// SQL Operators
enum SQLOperator {
    Plus,
    Minus,
    Mult,
    Div,
    Eq,
    Gt,
    GtEq,
    Lt,
    LtEq,
    Custom(Box<CustomOperator>) // extension point for vendor-specific operators
}

trait CustomOperator {
    //TODO: ???
}

/// SQL Expressions
enum SQLExpr {
    /// Identifier e.g. table name or column name
    Identifier(String),
    /// Literal value
    Literal(String),
    /// Binary expression e.g. `1 + 2` or `fname LIKE "A%"`
    Binary(Box<SQLExpr>, SQLOperator, Box<SQLExpr>),
    /// Function invocation with function name and list of argument expressions
    FunctionCall(String, Vec<SQLExpr>),
    /// Custom expression (vendor-specific)
    Custom(Box<CustomExpr>)
}

trait CustomExpr {
    //TODO: ???
}

enum ParserError {
    TBD
}

trait Parser<'a> {
    fn parse_expr(&mut self) -> Result<Box<SQLExpr>, Box<ParserError>>;
    fn parse_expr_list(&mut self) -> Result<Vec<SQLExpr>, Box<ParserError>>;
    fn parse_identifier(&mut self) -> Result<String, Box<ParserError>>;
    fn parse_keywords(&mut self, keywords: Vec<&str>) -> Result<bool, Box<ParserError>>;
}

/* --- KUDU PARSER IMPL --- */

struct KuduParser<'a> {
    generic_parser: Box<Parser<'a>>
}

impl<'a> Parser<'a> for KuduParser<'a> {

    fn parse_expr(&mut self) -> Result<Box<SQLExpr>, Box<ParserError>> {
        self.generic_parser.parse_expr()
    }

    fn parse_expr_list(&mut self) -> Result<Vec<SQLExpr>, Box<ParserError>> {
        self.generic_parser.parse_expr_list()
    }

    fn parse_identifier(&mut self) -> Result<String, Box<ParserError>> {
        self.generic_parser.parse_identifier()
    }

    fn parse_keywords(&mut self, keywords: Vec<&str>) -> Result<bool, Box<ParserError>> {
        self.parse_keywords(keywords)
    }
}

/* --- PRATT PARSER IMPL --- */

struct PrattParser<'a> {
    parser: Box<Parser<'a>>
}

impl<'a> PrattParser<'a> {

    fn parse_expr(&'a mut self, precedence: u8) -> SQLExpr {
        unimplemented!()
    }

//
//        // Not complete/accurate, but enough to demonstrate the concept that the pratt parser
//        // does not need knowledge of the specific tokenizer or parser to operate
//
//        loop {
//            match self.tokenizer.peek_token() {
//                Ok(Some(token)) => {
//                    let next_precedence = self.parser.get_precedence(&token);
//                    unimplemented!()
//                },
//                _ => {
//                }
//            }
//        }
//
//
}

#[cfg(test)]
mod tests {

    use super::*;
    #[test]
    fn it_works() {
        let tokens = vec![
            SQLToken::Keyword("CREATE".to_string()),
            SQLToken::Keyword("TABLE".to_string()),
            SQLToken::Keyword("test".to_string()),
        ];
    }
}

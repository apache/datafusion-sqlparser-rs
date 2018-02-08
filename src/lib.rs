///
/// This is a mock up of some key data structures and traits for a SQL parser
/// that can be used with custom dialects
///

/* --- TOKENIZER API --- */

enum TokenizerError {
    TBD
}

/// SQL Tokens
enum SQLToken {
    Keyword(String),
    Identifier(String),
    Literal(String), //TODO: need to model different types of literal
    Eq,
    NotEq,
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

trait SQLTokenizer {
    // return a reference to the next token without consuming it (look ahead)
    fn peek_token(&mut self) -> Result<Option<SQLToken>, TokenizerError>;
    // return a reference to the next token and advance the index
    fn next_token(&mut self) -> Result<Option<SQLToken>, TokenizerError>;
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
    Insert,
    Update,
    Delete,
    Select,
    CreateTable,
    /// Custom expression (vendor-specific)
    Custom(Box<CustomExpr>)
}

trait CustomExpr {
    //TODO: ???
}

enum ParserError {
    WrongToken { expected: Vec<SQLToken>, actual: SQLToken, line: usize, col: usize },
    TBD
}

impl From<TokenizerError> for ParserError {
    fn from(_: TokenizerError) -> Self {
        unimplemented!()
    }
}

trait Parser {
    fn parse_prefix(&mut self) -> Result<Box<SQLExpr>, ParserError> ;
    fn parse_infix(&mut self, left: SQLExpr) -> Result<Box<SQLExpr>, ParserError> ;
}

/* -- GENERIC (ANSI SQL) PARSER -- */

struct GenericParser {
    tokenizer: SQLTokenizer
}

impl GenericParser {

    fn parse_expr(&mut self, precedence: u8) -> Result<Box<SQLExpr>, ParserError> {

        let mut expr = self.parse_prefix()?;

        // loop while there are more tokens and until the precedence changes
        while let Some(token) = self.tokenizer.peek_token()? {

            let next_precedence = self.get_precedence(&token);

            if precedence >= next_precedence {
                break;
            }

            expr = self.parse_infix(expr, next_precedence)?;
        }

        Ok(expr)
    }

    fn parse_prefix(&mut self) -> Result<Box<SQLExpr>, ParserError> {

        match self.tokenizer.peek_token()? {
            Some(SQLToken::Keyword(ref k)) => match k.to_uppercase().as_ref() {
                "INSERT" => unimplemented!(),
                "UPDATE" => unimplemented!(),
                "DELETE" => unimplemented!(),
                "SELECT" => unimplemented!(),
                "CREATE" => unimplemented!(),
                _ => unimplemented!()
            },
            _ => unimplemented!()
        }
        unimplemented!()
    }

    fn parse_infix(&mut self, expr: Box<SQLExpr>, precedence: u8) -> Result<Box<SQLExpr>, ParserError> {

        match self.tokenizer.next_token()? {
            Some(tok) => {
                match tok {
                    SQLToken::Eq | SQLToken::Gt | SQLToken::GtEq |
                    SQLToken::Lt | SQLToken::LtEq => Ok(Box::new(SQLExpr::Binary(
                        expr,
                        self.to_sql_operator(&tok),
                        self.parse_expr(precedence)?
                    ))),
                    _ => Err(ParserError::WrongToken {
                        expected: vec![SQLToken::Eq, SQLToken::Gt], //TODO: complete
                        actual: tok,
                        line: 0,
                        col: 0
                    })
                }
            },
            None => Err(ParserError::TBD)
        }
    }

    fn to_sql_operator(&self, token: &SQLToken) -> SQLOperator {
        unimplemented!()
    }

    fn get_precedence(&self, token: &SQLToken) -> u8 {
        unimplemented!()
    }

    /// parse a list of SQL expressions separated by a comma
    fn parse_expr_list(&mut self, precedence: u8) -> Result<Vec<SQLExpr>, ParserError> {
        unimplemented!()
    }

}

//impl GenericParser {
//
//    fn tokenizer(&mut self) -> &mut SQLTokenizer {
//        &mut self.tokenizer
//    }
//
//    fn parse_keywords(&mut self, keywords: Vec<&str>) -> Result<bool, ParserError> {
//        unimplemented!()
//    }
//
////    fn parse_identifier(&mut self) -> Result<String, ParserError>;
//
//}

/* --- KUDU PARSER IMPL --- */


///// KuduParser is a wrapper around GenericParser
//struct KuduParser {
//    generic: GenericParser
//}
//
//impl Parser for KuduParser {
//
//    fn parse_prefix(&mut self) -> Result<Box<SQLExpr>, ParserError> {
//
//        // just take over the statements we need to and delegate everything else
//        // to the generic parser
//        if self.generic.parse_keywords(vec!["CREATE", "TABLE"])? {
//
//            //TODO: insert kudu CREATE TABLE parsing logic here
//            // .. we can delegate to the generic parsers for parts of that even
//
//            // mock response
//            let kudu_create_table = KuduCreateTable {
//                partition: vec![KuduPartition::Hash]
//            };
//
//            Ok(Box::new(SQLExpr::Custom(Box::new(kudu_create_table ))))
//        } else {
//            _ => self.generic.parse_prefix()
//        }
//    }
//
//    fn parse_infix(&mut self) -> Result<Box<SQLExpr>, ParserError> {
//        self.generic.parse_infix()
//    }
//}
//
//impl KuduParser {
//
//    fn tokenizer(&mut self) -> &mut SQLTokenizer {
//        &mut self.generic.tokenizer
//    }
//
//}
//
//enum KuduPartition {
//    Hash,
//    Range,
//}
//
//struct KuduCreateTable {
//    partition: Vec<KuduPartition>
//}
//
//impl CustomExpr for KuduCreateTable {
//
//}

#[cfg(test)]
mod tests {

    use super::SQLToken::*;
    use super::*;
    #[test]
    fn parse_kudu_create_table() {

        // CREATE TABLE test (col1 int8) HASH (col1)
        let tokens = vec![
            k("CREATE"), k("TABLE"), i("test"), LParen,
            i("col1"), k("int8"),
            RParen,
            k("HASH"), LParen, i("col1"), RParen
        ];

        //let parser = KuduParser { generic_parser: }
    }

    fn k(s: &str) -> SQLToken {
        Keyword(s.to_string())
    }

    fn i(s: &str) -> SQLToken {
        Identifier(s.to_string())
    }


}

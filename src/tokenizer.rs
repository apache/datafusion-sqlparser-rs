
#[derive(Debug)]
pub struct Position {
    line: usize,
    col: usize
}

#[derive(Debug)]
pub enum TokenizerError<T> {
    UnexpectedEof(Position),
    UnterminatedStringLiteral(Position),
    Custom(T)
}

/// SQL Tokens
#[derive(Debug)]
pub enum SQLToken<T> {
    Keyword(String), //TODO: &str ?
    Identifier(String), //TODO: &str ?
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
    /// Custom token
    Custom(T)
}

pub trait SQLTokenizer<S, T> {
    /// return a reference to the next token without consuming it (look ahead)
    fn peek_token(&mut self) -> Result<Option<SQLToken<S>>, TokenizerError<T>>;
    /// return a reference to the next token and advance the index
    fn next_token(&mut self) -> Result<Option<SQLToken<S>>, TokenizerError<T>>;
}
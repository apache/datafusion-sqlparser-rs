use std::cmp::PartialEq;
use std::fmt::Debug;

/// Simple holder for a sequence of characters that supports iteration and mark/reset methods
pub struct CharSeq {
    chars: Vec<char>,
    i: usize,
    m: usize
}

impl CharSeq {

    /// Create a CharSeq from a string
    pub fn new(sql: &str) -> Self {
        CharSeq {
            chars: sql.chars().collect(),
            i: 0,
            m: 0
        }
    }

    /// Mark the current index
    pub fn mark(&mut self) {
        self.m = self.i;
    }

    /// Reset the index
    pub fn reset(&mut self) {
        self.i = self.m;
    }

    /// Peek the next char
    pub fn peek(&mut self) -> Option<&char> {
        if self.i < self.chars.len() {
            Some(&self.chars[self.i])
        } else {
            None
        }
    }

    /// Get the next char
    pub fn next(&mut self) -> Option<char> {
        if self.i < self.chars.len() {
            self.i += 1;
            Some(self.chars[self.i-1])
        } else {
            None
        }
    }
}

#[derive(Debug)]
pub struct Position {
    line: usize,
    col: usize
}
impl Position {
    pub fn new(line: usize, col: usize) -> Self {
        Position { line, col }
    }
}

#[derive(Debug)]
pub enum TokenizerError {
    UnexpectedChar(char,Position),
    UnexpectedEof(Position),
    UnterminatedStringLiteral(Position),
    Custom(String)
}

/// SQL Tokens
#[derive(Debug,PartialEq)]
pub enum SQLToken<T: Debug + PartialEq> {
    Whitespace(char),
    Keyword(String),
    Identifier(String),
    Literal(String), //TODO: need to model different types of literal
    Plus,
    Minus,
    Mult,
    Divide,
    Eq,
    Not,
    NotEq,
    Gt,
    GtEq,
    Lt,
    LtEq,
    LParen,
    RParen,
    Comma,
    /// Custom token (dialect-specific)
    Custom(T)
}

pub trait SQLTokenizer<TokenType>
    where TokenType: Debug + PartialEq {

    /// get the precendence of a token
    fn precedence(&self, token: &SQLToken<TokenType>) -> usize;

    /// return a reference to the next token and advance the index
    fn next_token(&mut self, chars: &mut CharSeq) -> Result<Option<SQLToken<TokenType>>, TokenizerError>;
}


pub fn tokenize<TokenType>(sql: &str, tokenizer: &mut SQLTokenizer<TokenType>) -> Result<Vec<SQLToken<TokenType>>, TokenizerError>
    where TokenType: Debug + PartialEq
    {

    let mut chars = CharSeq::new(sql);

    let mut tokens : Vec<SQLToken<TokenType>> = vec![];

    loop {
        match tokenizer.next_token(&mut chars)? {
            Some(SQLToken::Whitespace(_)) => { /* ignore */ },
            Some(token) => {
                println!("Token: {:?}", token);
                tokens.push(token)
            },
            None => break
        }
    }

    Ok(tokens)
}
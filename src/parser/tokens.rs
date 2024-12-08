use super::*;

use crate::parser_err;

impl Parser<'_> {
    /// Consume the parser and return its underlying token buffer
    pub fn into_tokens(self) -> Vec<TokenWithSpan> {
        self.tokens
    }

    /// Get the precedence of the next token
    pub fn get_next_precedence(&self) -> Result<u8, ParserError> {
        self.dialect.get_next_precedence_default(self)
    }

    /// Return the first non-whitespace token that has not yet been processed
    /// (or None if reached end-of-file)
    pub fn peek_token(&self) -> TokenWithSpan {
        self.peek_nth_token(0)
    }

    /// Returns the `N` next non-whitespace tokens that have not yet been
    /// processed.
    ///
    /// Example:
    /// ```rust
    /// # use sqlparser::dialect::GenericDialect;
    /// # use sqlparser::parser::Parser;
    /// # use sqlparser::keywords::Keyword;
    /// # use sqlparser::tokenizer::{Token, Word};
    /// let dialect = GenericDialect {};
    /// let mut parser = Parser::new(&dialect).try_with_sql("ORDER BY foo, bar").unwrap();
    ///
    /// // Note that Rust infers the number of tokens to peek based on the
    /// // length of the slice pattern!
    /// assert!(matches!(
    ///     parser.peek_tokens(),
    ///     [
    ///         Token::Word(Word { keyword: Keyword::ORDER, .. }),
    ///         Token::Word(Word { keyword: Keyword::BY, .. }),
    ///     ]
    /// ));
    /// ```
    pub fn peek_tokens<const N: usize>(&self) -> [Token; N] {
        self.peek_tokens_with_location()
            .map(|with_loc| with_loc.token)
    }

    /// Returns the `N` next non-whitespace tokens with locations that have not
    /// yet been processed.
    ///
    /// See [`Self::peek_token`] for an example.
    pub fn peek_tokens_with_location<const N: usize>(&self) -> [TokenWithSpan; N] {
        let mut index = self.index;
        core::array::from_fn(|_| loop {
            let token = self.tokens.get(index);
            index += 1;
            if let Some(TokenWithSpan {
                token: Token::Whitespace(_),
                span: _,
            }) = token
            {
                continue;
            }
            break token.cloned().unwrap_or(TokenWithSpan {
                token: Token::EOF,
                span: Span::empty(),
            });
        })
    }

    /// Return nth non-whitespace token that has not yet been processed
    pub fn peek_nth_token(&self, mut n: usize) -> TokenWithSpan {
        let mut index = self.index;
        loop {
            index += 1;
            match self.tokens.get(index - 1) {
                Some(TokenWithSpan {
                    token: Token::Whitespace(_),
                    span: _,
                }) => continue,
                non_whitespace => {
                    if n == 0 {
                        return non_whitespace.cloned().unwrap_or(TokenWithSpan {
                            token: Token::EOF,
                            span: Span::empty(),
                        });
                    }
                    n -= 1;
                }
            }
        }
    }

    /// Return the first token, possibly whitespace, that has not yet been processed
    /// (or None if reached end-of-file).
    pub fn peek_token_no_skip(&self) -> TokenWithSpan {
        self.peek_nth_token_no_skip(0)
    }

    /// Return nth token, possibly whitespace, that has not yet been processed.
    pub fn peek_nth_token_no_skip(&self, n: usize) -> TokenWithSpan {
        self.tokens
            .get(self.index + n)
            .cloned()
            .unwrap_or(TokenWithSpan {
                token: Token::EOF,
                span: Span::empty(),
            })
    }

    /// Look for all of the expected keywords in sequence, without consuming them
    pub fn peek_keywords(&mut self, expected: &[Keyword]) -> bool {
        let index = self.index;
        let matched = self.parse_keywords(expected);
        self.index = index;
        matched
    }

    /// Return the first non-whitespace token that has not yet been processed
    /// (or None if reached end-of-file) and mark it as processed. OK to call
    /// repeatedly after reaching EOF.
    pub fn next_token(&mut self) -> TokenWithSpan {
        loop {
            self.index += 1;
            match self.tokens.get(self.index - 1) {
                Some(TokenWithSpan {
                    token: Token::Whitespace(_),
                    span: _,
                }) => continue,
                token => {
                    return token
                        .cloned()
                        .unwrap_or_else(|| TokenWithSpan::wrap(Token::EOF))
                }
            }
        }
    }

    /// Return the first unprocessed token, possibly whitespace.
    pub fn next_token_no_skip(&mut self) -> Option<&TokenWithSpan> {
        self.index += 1;
        self.tokens.get(self.index - 1)
    }

    /// Push back the last one non-whitespace token. Must be called after
    /// `next_token()`, otherwise might panic. OK to call after
    /// `next_token()` indicates an EOF.
    pub fn prev_token(&mut self) {
        loop {
            assert!(self.index > 0);
            self.index -= 1;
            if let Some(TokenWithSpan {
                token: Token::Whitespace(_),
                span: _,
            }) = self.tokens.get(self.index)
            {
                continue;
            }
            return;
        }
    }

    /// Report `found` was encountered instead of `expected`
    pub fn expected<T>(&self, expected: &str, found: TokenWithSpan) -> Result<T, ParserError> {
        parser_err!(
            format!("Expected: {expected}, found: {found}"),
            found.span.start
        )
    }

    /// If the current token is the `expected` keyword, consume it and returns
    /// true. Otherwise, no tokens are consumed and returns false.
    #[must_use]
    pub fn parse_keyword(&mut self, expected: Keyword) -> bool {
        self.parse_keyword_token(expected).is_some()
    }

    #[must_use]
    pub fn parse_keyword_token(&mut self, expected: Keyword) -> Option<TokenWithSpan> {
        match self.peek_token().token {
            Token::Word(w) if expected == w.keyword => Some(self.next_token()),
            _ => None,
        }
    }

    #[must_use]
    pub fn peek_keyword(&mut self, expected: Keyword) -> bool {
        matches!(self.peek_token().token, Token::Word(w) if expected == w.keyword)
    }

    /// If the current token is the `expected` keyword followed by
    /// specified tokens, consume them and returns true.
    /// Otherwise, no tokens are consumed and returns false.
    ///
    /// Note that if the length of `tokens` is too long, this function will
    /// not be efficient as it does a loop on the tokens with `peek_nth_token`
    /// each time.
    pub fn parse_keyword_with_tokens(&mut self, expected: Keyword, tokens: &[Token]) -> bool {
        match self.peek_token().token {
            Token::Word(w) if expected == w.keyword => {
                for (idx, token) in tokens.iter().enumerate() {
                    if self.peek_nth_token(idx + 1).token != *token {
                        return false;
                    }
                }
                // consume all tokens
                for _ in 0..(tokens.len() + 1) {
                    self.next_token();
                }
                true
            }
            _ => false,
        }
    }

    /// If the current and subsequent tokens exactly match the `keywords`
    /// sequence, consume them and returns true. Otherwise, no tokens are
    /// consumed and returns false
    #[must_use]
    pub fn parse_keywords(&mut self, keywords: &[Keyword]) -> bool {
        let index = self.index;
        for &keyword in keywords {
            if !self.parse_keyword(keyword) {
                // println!("parse_keywords aborting .. did not find {:?}", keyword);
                // reset index and return immediately
                self.index = index;
                return false;
            }
        }
        true
    }

    /// If the current token is one of the given `keywords`, consume the token
    /// and return the keyword that matches. Otherwise, no tokens are consumed
    /// and returns [`None`].
    #[must_use]
    pub fn parse_one_of_keywords(&mut self, keywords: &[Keyword]) -> Option<Keyword> {
        match self.peek_token().token {
            Token::Word(w) => {
                keywords
                    .iter()
                    .find(|keyword| **keyword == w.keyword)
                    .map(|keyword| {
                        self.next_token();
                        *keyword
                    })
            }
            _ => None,
        }
    }

    /// If the current token is one of the expected keywords, consume the token
    /// and return the keyword that matches. Otherwise, return an error.
    pub fn expect_one_of_keywords(&mut self, keywords: &[Keyword]) -> Result<Keyword, ParserError> {
        if let Some(keyword) = self.parse_one_of_keywords(keywords) {
            Ok(keyword)
        } else {
            let keywords: Vec<String> = keywords.iter().map(|x| format!("{x:?}")).collect();
            self.expected(
                &format!("one of {}", keywords.join(" or ")),
                self.peek_token(),
            )
        }
    }

    /// If the current token is the `expected` keyword, consume the token.
    /// Otherwise, return an error.
    pub fn expect_keyword(&mut self, expected: Keyword) -> Result<TokenWithSpan, ParserError> {
        if let Some(token) = self.parse_keyword_token(expected) {
            Ok(token)
        } else {
            self.expected(format!("{:?}", &expected).as_str(), self.peek_token())
        }
    }

    /// If the current and subsequent tokens exactly match the `keywords`
    /// sequence, consume them and returns Ok. Otherwise, return an Error.
    pub fn expect_keywords(&mut self, expected: &[Keyword]) -> Result<(), ParserError> {
        for &kw in expected {
            self.expect_keyword(kw)?;
        }
        Ok(())
    }

    /// Consume the next token if it matches the expected token, otherwise return false
    #[must_use]
    pub fn consume_token(&mut self, expected: &Token) -> bool {
        if self.peek_token() == *expected {
            self.next_token();
            true
        } else {
            false
        }
    }

    /// If the current and subsequent tokens exactly match the `tokens`
    /// sequence, consume them and returns true. Otherwise, no tokens are
    /// consumed and returns false
    #[must_use]
    pub fn consume_tokens(&mut self, tokens: &[Token]) -> bool {
        let index = self.index;
        for token in tokens {
            if !self.consume_token(token) {
                self.index = index;
                return false;
            }
        }
        true
    }

    /// Bail out if the current token is not an expected keyword, or consume it if it is
    pub fn expect_token(&mut self, expected: &Token) -> Result<TokenWithSpan, ParserError> {
        if self.peek_token() == *expected {
            Ok(self.next_token())
        } else {
            self.expected(&expected.to_string(), self.peek_token())
        }
    }

    /// Peek at the next token and determine if it is a temporal unit
    /// like `second`.
    pub fn next_token_is_temporal_unit(&mut self) -> bool {
        if let Token::Word(word) = self.peek_token().token {
            matches!(
                word.keyword,
                Keyword::YEAR
                    | Keyword::MONTH
                    | Keyword::WEEK
                    | Keyword::DAY
                    | Keyword::HOUR
                    | Keyword::MINUTE
                    | Keyword::SECOND
                    | Keyword::CENTURY
                    | Keyword::DECADE
                    | Keyword::DOW
                    | Keyword::DOY
                    | Keyword::EPOCH
                    | Keyword::ISODOW
                    | Keyword::ISOYEAR
                    | Keyword::JULIAN
                    | Keyword::MICROSECOND
                    | Keyword::MICROSECONDS
                    | Keyword::MILLENIUM
                    | Keyword::MILLENNIUM
                    | Keyword::MILLISECOND
                    | Keyword::MILLISECONDS
                    | Keyword::NANOSECOND
                    | Keyword::NANOSECONDS
                    | Keyword::QUARTER
                    | Keyword::TIMEZONE
                    | Keyword::TIMEZONE_HOUR
                    | Keyword::TIMEZONE_MINUTE
            )
        } else {
            false
        }
    }
}

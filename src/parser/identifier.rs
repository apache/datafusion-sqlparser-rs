use crate::parser::*;

impl<'a> Parser<'a> {
    /// Parse a simple one-word identifier (possibly quoted, possibly a keyword)
    ///
    /// The `in_table_clause` parameter indicates whether the identifier is a table in a FROM, JOIN, or
    /// similar table clause. Currently, this is used only to support unquoted hyphenated identifiers in
    //  this context on BigQuery.
    pub fn parse_identifier(&mut self, in_table_clause: bool) -> Result<Ident, ParserError> {
        let next_token = self.next_token();
        match next_token.token {
            Token::Word(w) => {
                let mut ident = w.to_ident(next_token.span);

                // On BigQuery, hyphens are permitted in unquoted identifiers inside of a FROM or
                // TABLE clause [0].
                //
                // The first segment must be an ordinary unquoted identifier, e.g. it must not start
                // with a digit. Subsequent segments are either must either be valid identifiers or
                // integers, e.g. foo-123 is allowed, but foo-123a is not.
                //
                // [0] https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical
                if dialect_of!(self is BigQueryDialect)
                    && w.quote_style.is_none()
                    && in_table_clause
                {
                    let mut requires_whitespace = false;
                    while matches!(self.peek_token_no_skip().token, Token::Minus) {
                        self.next_token();
                        ident.value.push('-');

                        let token = self
                            .next_token_no_skip()
                            .cloned()
                            .unwrap_or(TokenWithSpan::wrap(Token::EOF));
                        requires_whitespace = match token.token {
                            Token::Word(next_word) if next_word.quote_style.is_none() => {
                                ident.value.push_str(&next_word.value);
                                false
                            }
                            Token::Number(s, false) if s.chars().all(|c| c.is_ascii_digit()) => {
                                ident.value.push_str(&s);
                                true
                            }
                            _ => {
                                return self
                                    .expected("continuation of hyphenated identifier", token);
                            }
                        }
                    }

                    // If the last segment was a number, we must check that it's followed by whitespace,
                    // otherwise foo-123a will be parsed as `foo-123` with the alias `a`.
                    if requires_whitespace {
                        let token = self.next_token();
                        if !matches!(token.token, Token::EOF | Token::Whitespace(_)) {
                            return self
                                .expected("whitespace following hyphenated identifier", token);
                        }
                    }
                }
                Ok(ident)
            }
            Token::SingleQuotedString(s) => Ok(Ident::with_quote('\'', s)),
            Token::DoubleQuotedString(s) => Ok(Ident::with_quote('\"', s)),
            _ => self.expected("identifier", next_token),
        }
    }

    /// Parse identifiers
    pub fn parse_identifiers(&mut self) -> Result<Vec<Ident>, ParserError> {
        let mut idents = vec![];
        loop {
            match self.peek_token().token {
                Token::Word(w) => {
                    idents.push(w.to_ident(self.peek_token().span));
                }
                Token::EOF | Token::Eq => break,
                _ => {}
            }
            self.next_token();
        }
        Ok(idents)
    }

    /// Parse a possibly qualified, possibly quoted identifier, e.g.
    /// `foo` or `myschema."table"
    ///
    /// The `in_table_clause` parameter indicates whether the object name is a table in a FROM, JOIN,
    /// or similar table clause. Currently, this is used only to support unquoted hyphenated identifiers
    /// in this context on BigQuery.
    pub fn parse_object_name(&mut self, in_table_clause: bool) -> Result<ObjectName, ParserError> {
        let mut idents = vec![];
        loop {
            if self.dialect.supports_object_name_double_dot_notation()
                && idents.len() == 1
                && self.consume_token(&Token::Period)
            {
                // Empty string here means default schema
                idents.push(Ident::new(""));
            }
            idents.push(self.parse_identifier(in_table_clause)?);
            if !self.consume_token(&Token::Period) {
                break;
            }
        }

        // BigQuery accepts any number of quoted identifiers of a table name.
        // https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#quoted_identifiers
        if dialect_of!(self is BigQueryDialect)
            && idents.iter().any(|ident| ident.value.contains('.'))
        {
            idents = idents
                .into_iter()
                .flat_map(|ident| {
                    ident
                        .value
                        .split('.')
                        .map(|value| Ident {
                            value: value.into(),
                            quote_style: ident.quote_style,
                            span: ident.span,
                        })
                        .collect::<Vec<_>>()
                })
                .collect()
        }

        Ok(ObjectName(idents))
    }

    /// Strictly parse `identifier AS identifier`
    pub fn parse_identifier_with_alias(&mut self) -> Result<IdentWithAlias, ParserError> {
        let ident = self.parse_identifier(false)?;
        self.expect_keyword(Keyword::AS)?;
        let alias = self.parse_identifier(false)?;
        Ok(IdentWithAlias { ident, alias })
    }

    /// Parse identifiers of form ident1[.identN]*
    ///
    /// Similar in functionality to [parse_identifiers], with difference
    /// being this function is much more strict about parsing a valid multipart identifier, not
    /// allowing extraneous tokens to be parsed, otherwise it fails.
    ///
    /// For example:
    ///
    /// ```rust
    /// use sqlparser::ast::Ident;
    /// use sqlparser::dialect::GenericDialect;
    /// use sqlparser::parser::Parser;
    ///
    /// let dialect = GenericDialect {};
    /// let expected = vec![Ident::new("one"), Ident::new("two")];
    ///
    /// // expected usage
    /// let sql = "one.two";
    /// let mut parser = Parser::new(&dialect).try_with_sql(sql).unwrap();
    /// let actual = parser.parse_multipart_identifier().unwrap();
    /// assert_eq!(&actual, &expected);
    ///
    /// // parse_identifiers is more loose on what it allows, parsing successfully
    /// let sql = "one + two";
    /// let mut parser = Parser::new(&dialect).try_with_sql(sql).unwrap();
    /// let actual = parser.parse_identifiers().unwrap();
    /// assert_eq!(&actual, &expected);
    ///
    /// // expected to strictly fail due to + separator
    /// let sql = "one + two";
    /// let mut parser = Parser::new(&dialect).try_with_sql(sql).unwrap();
    /// let actual = parser.parse_multipart_identifier().unwrap_err();
    /// assert_eq!(
    ///     actual.to_string(),
    ///     "sql parser error: Unexpected token in identifier: +"
    /// );
    /// ```
    ///
    /// [parse_identifiers]: Parser::parse_identifiers
    pub fn parse_multipart_identifier(&mut self) -> Result<Vec<Ident>, ParserError> {
        let mut idents = vec![];

        // expecting at least one word for identifier
        let next_token = self.next_token();
        match next_token.token {
            Token::Word(w) => idents.push(w.to_ident(next_token.span)),
            Token::EOF => {
                return Err(ParserError::ParserError(
                    "Empty input when parsing identifier".to_string(),
                ))?
            }
            token => {
                return Err(ParserError::ParserError(format!(
                    "Unexpected token in identifier: {token}"
                )))?
            }
        };

        // parse optional next parts if exist
        loop {
            match self.next_token().token {
                // ensure that optional period is succeeded by another identifier
                Token::Period => {
                    let next_token = self.next_token();
                    match next_token.token {
                        Token::Word(w) => idents.push(w.to_ident(next_token.span)),
                        Token::EOF => {
                            return Err(ParserError::ParserError(
                                "Trailing period in identifier".to_string(),
                            ))?
                        }
                        token => {
                            return Err(ParserError::ParserError(format!(
                                "Unexpected token following period in identifier: {token}"
                            )))?
                        }
                    }
                }
                Token::EOF => break,
                token => {
                    return Err(ParserError::ParserError(format!(
                        "Unexpected token in identifier: {token}"
                    )))?
                }
            }
        }

        Ok(idents)
    }

    /// Parse `AS identifier` (or simply `identifier` if it's not a reserved keyword)
    /// Some examples with aliases: `SELECT 1 foo`, `SELECT COUNT(*) AS cnt`,
    /// `SELECT ... FROM t1 foo, t2 bar`, `SELECT ... FROM (...) AS bar`
    pub fn parse_optional_alias(
        &mut self,
        reserved_kwds: &[Keyword],
    ) -> Result<Option<Ident>, ParserError> {
        let after_as = self.parse_keyword(Keyword::AS);
        let next_token = self.next_token();
        match next_token.token {
            // Accept any identifier after `AS` (though many dialects have restrictions on
            // keywords that may appear here). If there's no `AS`: don't parse keywords,
            // which may start a construct allowed in this position, to be parsed as aliases.
            // (For example, in `FROM t1 JOIN` the `JOIN` will always be parsed as a keyword,
            // not an alias.)
            Token::Word(w) if after_as || !reserved_kwds.contains(&w.keyword) => {
                Ok(Some(w.to_ident(next_token.span)))
            }
            // MSSQL supports single-quoted strings as aliases for columns
            // We accept them as table aliases too, although MSSQL does not.
            //
            // Note, that this conflicts with an obscure rule from the SQL
            // standard, which we don't implement:
            // https://crate.io/docs/sql-99/en/latest/chapters/07.html#character-string-literal-s
            //    "[Obscure Rule] SQL allows you to break a long <character
            //    string literal> up into two or more smaller <character string
            //    literal>s, split by a <separator> that includes a newline
            //    character. When it sees such a <literal>, your DBMS will
            //    ignore the <separator> and treat the multiple strings as
            //    a single <literal>."
            Token::SingleQuotedString(s) => Ok(Some(Ident::with_quote('\'', s))),
            // Support for MySql dialect double-quoted string, `AS "HOUR"` for example
            Token::DoubleQuotedString(s) => Ok(Some(Ident::with_quote('\"', s))),
            _ => {
                if after_as {
                    return self.expected("an identifier after AS", next_token);
                }
                self.prev_token();
                Ok(None) // no alias found
            }
        }
    }

    /// Parse `[ident]`, mostly `ident` is name, like:
    /// `window_name`, `index_name`, ...
    pub fn parse_optional_indent(&mut self) -> Result<Option<Ident>, ParserError> {
        self.maybe_parse(|parser| parser.parse_identifier(false))
    }
}

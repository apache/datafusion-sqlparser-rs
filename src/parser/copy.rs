use super::*;

use crate::parser_err;

impl Parser<'_> {
    /// Parse a copy statement
    pub fn parse_copy(&mut self) -> Result<Statement, ParserError> {
        let source;
        if self.consume_token(&Token::LParen) {
            source = CopySource::Query(self.parse_query()?);
            self.expect_token(&Token::RParen)?;
        } else {
            let table_name = self.parse_object_name(false)?;
            let columns = self.parse_parenthesized_column_list(Optional, false)?;
            source = CopySource::Table {
                table_name,
                columns,
            };
        }
        let to = match self.parse_one_of_keywords(&[Keyword::FROM, Keyword::TO]) {
            Some(Keyword::FROM) => false,
            Some(Keyword::TO) => true,
            _ => self.expected("FROM or TO", self.peek_token())?,
        };
        if !to {
            // Use a separate if statement to prevent Rust compiler from complaining about
            // "if statement in this position is unstable: https://github.com/rust-lang/rust/issues/53667"
            if let CopySource::Query(_) = source {
                return Err(ParserError::ParserError(
                    "COPY ... FROM does not support query as a source".to_string(),
                ));
            }
        }
        let target = if self.parse_keyword(Keyword::STDIN) {
            CopyTarget::Stdin
        } else if self.parse_keyword(Keyword::STDOUT) {
            CopyTarget::Stdout
        } else if self.parse_keyword(Keyword::PROGRAM) {
            CopyTarget::Program {
                command: self.parse_literal_string()?,
            }
        } else {
            CopyTarget::File {
                filename: self.parse_literal_string()?,
            }
        };
        let _ = self.parse_keyword(Keyword::WITH); // [ WITH ]
        let mut options = vec![];
        if self.consume_token(&Token::LParen) {
            options = self.parse_comma_separated(Parser::parse_copy_option)?;
            self.expect_token(&Token::RParen)?;
        }
        let mut legacy_options = vec![];
        while let Some(opt) = self.maybe_parse(|parser| parser.parse_copy_legacy_option())? {
            legacy_options.push(opt);
        }
        let values = if let CopyTarget::Stdin = target {
            self.expect_token(&Token::SemiColon)?;
            self.parse_tsv()
        } else {
            vec![]
        };
        Ok(Statement::Copy {
            source,
            to,
            target,
            options,
            legacy_options,
            values,
        })
    }

    fn parse_copy_option(&mut self) -> Result<CopyOption, ParserError> {
        let ret = match self.parse_one_of_keywords(&[
            Keyword::FORMAT,
            Keyword::FREEZE,
            Keyword::DELIMITER,
            Keyword::NULL,
            Keyword::HEADER,
            Keyword::QUOTE,
            Keyword::ESCAPE,
            Keyword::FORCE_QUOTE,
            Keyword::FORCE_NOT_NULL,
            Keyword::FORCE_NULL,
            Keyword::ENCODING,
        ]) {
            Some(Keyword::FORMAT) => CopyOption::Format(self.parse_identifier(false)?),
            Some(Keyword::FREEZE) => CopyOption::Freeze(!matches!(
                self.parse_one_of_keywords(&[Keyword::TRUE, Keyword::FALSE]),
                Some(Keyword::FALSE)
            )),
            Some(Keyword::DELIMITER) => CopyOption::Delimiter(self.parse_literal_char()?),
            Some(Keyword::NULL) => CopyOption::Null(self.parse_literal_string()?),
            Some(Keyword::HEADER) => CopyOption::Header(!matches!(
                self.parse_one_of_keywords(&[Keyword::TRUE, Keyword::FALSE]),
                Some(Keyword::FALSE)
            )),
            Some(Keyword::QUOTE) => CopyOption::Quote(self.parse_literal_char()?),
            Some(Keyword::ESCAPE) => CopyOption::Escape(self.parse_literal_char()?),
            Some(Keyword::FORCE_QUOTE) => {
                CopyOption::ForceQuote(self.parse_parenthesized_column_list(Mandatory, false)?)
            }
            Some(Keyword::FORCE_NOT_NULL) => {
                CopyOption::ForceNotNull(self.parse_parenthesized_column_list(Mandatory, false)?)
            }
            Some(Keyword::FORCE_NULL) => {
                CopyOption::ForceNull(self.parse_parenthesized_column_list(Mandatory, false)?)
            }
            Some(Keyword::ENCODING) => CopyOption::Encoding(self.parse_literal_string()?),
            _ => self.expected("option", self.peek_token())?,
        };
        Ok(ret)
    }

    fn parse_copy_legacy_option(&mut self) -> Result<CopyLegacyOption, ParserError> {
        let ret = match self.parse_one_of_keywords(&[
            Keyword::BINARY,
            Keyword::DELIMITER,
            Keyword::NULL,
            Keyword::CSV,
        ]) {
            Some(Keyword::BINARY) => CopyLegacyOption::Binary,
            Some(Keyword::DELIMITER) => {
                let _ = self.parse_keyword(Keyword::AS); // [ AS ]
                CopyLegacyOption::Delimiter(self.parse_literal_char()?)
            }
            Some(Keyword::NULL) => {
                let _ = self.parse_keyword(Keyword::AS); // [ AS ]
                CopyLegacyOption::Null(self.parse_literal_string()?)
            }
            Some(Keyword::CSV) => CopyLegacyOption::Csv({
                let mut opts = vec![];
                while let Some(opt) =
                    self.maybe_parse(|parser| parser.parse_copy_legacy_csv_option())?
                {
                    opts.push(opt);
                }
                opts
            }),
            _ => self.expected("option", self.peek_token())?,
        };
        Ok(ret)
    }

    fn parse_copy_legacy_csv_option(&mut self) -> Result<CopyLegacyCsvOption, ParserError> {
        let ret = match self.parse_one_of_keywords(&[
            Keyword::HEADER,
            Keyword::QUOTE,
            Keyword::ESCAPE,
            Keyword::FORCE,
        ]) {
            Some(Keyword::HEADER) => CopyLegacyCsvOption::Header,
            Some(Keyword::QUOTE) => {
                let _ = self.parse_keyword(Keyword::AS); // [ AS ]
                CopyLegacyCsvOption::Quote(self.parse_literal_char()?)
            }
            Some(Keyword::ESCAPE) => {
                let _ = self.parse_keyword(Keyword::AS); // [ AS ]
                CopyLegacyCsvOption::Escape(self.parse_literal_char()?)
            }
            Some(Keyword::FORCE) if self.parse_keywords(&[Keyword::NOT, Keyword::NULL]) => {
                CopyLegacyCsvOption::ForceNotNull(
                    self.parse_comma_separated(|p| p.parse_identifier(false))?,
                )
            }
            Some(Keyword::FORCE) if self.parse_keywords(&[Keyword::QUOTE]) => {
                CopyLegacyCsvOption::ForceQuote(
                    self.parse_comma_separated(|p| p.parse_identifier(false))?,
                )
            }
            _ => self.expected("csv option", self.peek_token())?,
        };
        Ok(ret)
    }

    fn parse_literal_char(&mut self) -> Result<char, ParserError> {
        let s = self.parse_literal_string()?;
        if s.len() != 1 {
            let loc = self
                .tokens
                .get(self.index - 1)
                .map_or(Location { line: 0, column: 0 }, |t| t.span.start);
            return parser_err!(format!("Expect a char, found {s:?}"), loc);
        }
        Ok(s.chars().next().unwrap())
    }

    /// Parse a tab separated values in
    /// COPY payload
    pub fn parse_tsv(&mut self) -> Vec<Option<String>> {
        self.parse_tab_value()
    }

    pub fn parse_tab_value(&mut self) -> Vec<Option<String>> {
        let mut values = vec![];
        let mut content = String::from("");
        while let Some(t) = self.next_token_no_skip().map(|t| &t.token) {
            match t {
                Token::Whitespace(Whitespace::Tab) => {
                    values.push(Some(content.to_string()));
                    content.clear();
                }
                Token::Whitespace(Whitespace::Newline) => {
                    values.push(Some(content.to_string()));
                    content.clear();
                }
                Token::Backslash => {
                    if self.consume_token(&Token::Period) {
                        return values;
                    }
                    if let Token::Word(w) = self.next_token().token {
                        if w.value == "N" {
                            values.push(None);
                        }
                    }
                }
                _ => {
                    content.push_str(&t.to_string());
                }
            }
        }
        values
    }
}

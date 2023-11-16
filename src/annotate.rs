// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Helpers for annotating parse errors with the source query.

use crate::{parser::ParserError, tokenizer::TokenizerError};
use std::fmt::Write;

/// Generates a string containing the orginal query annotated with the error
/// message.
///
/// In cases where the parser error does not contain location information, or
/// the location isn't valid, the returned string will just be the error string
/// itself.
pub fn annotate_with_error(query: &str, err: ParserError) -> String {
    // Pull out the message and location...
    let (msg, loc) = match &err {
        ParserError::TokenizerError(TokenizerError { message, location }) => (message, location),
        ParserError::ParserError { message, location } => match location {
            Some(loc) if loc.line > 0 && loc.column > 0 => (message, loc),
            _ => return err.to_string(),
        },
        ParserError::RecursionLimitExceeded => return err.to_string(),
    };

    let lines: Vec<_> = query.lines().collect();

    let top_lines = match lines.get(0..loc.line as usize) {
        Some(lines) => lines,
        None => return err.to_string(),
    };

    let mut buf = String::with_capacity(query.len());

    // Write lines before the annotation (including the line where the error
    // ocurred).
    for line in top_lines {
        buf.push_str(line);
        buf.push('\n');
    }

    // Write the error message left padded with spaces to line it up with where
    // the error ocurred.
    let pad = (loc.column - 1) as usize;
    write!(buf, "{:<1$}", "", pad).unwrap();
    write!(buf, "^ {msg}").unwrap();

    let remaining_lines = lines.get(loc.line as usize..).unwrap_or_default();

    for line in remaining_lines {
        buf.push('\n');
        buf.push_str(line);
    }
    buf.push('\n');

    buf
}

#[cfg(test)]
mod tests {
    use crate::{ast::Statement, dialect::GenericDialect, parser::Parser, tokenizer::Tokenizer};

    use super::*;

    fn test_parse(sql: &str) -> Result<Vec<Statement>, ParserError> {
        let dialect = &GenericDialect {};
        let mut tokenizer = Tokenizer::new(dialect, sql);
        let tokens = tokenizer.tokenize_with_location()?;
        Parser::new(dialect)
            .with_tokens_with_locations(tokens)
            .parse_statements()
    }

    fn assert_eq_with_print(expected: impl Into<String>, got: impl Into<String>) {
        let expected = expected.into();
        let got = got.into();
        assert_eq!(expected, got, "\nexpected ---\n{expected}\ngot ---\n{got}");
    }

    #[test]
    fn not_a_sql_statement() {
        let sql = "hello";
        let err = test_parse(sql).unwrap_err();

        let expected = ["hello", "^ Expected an SQL statement, found: hello"].join("\n");
        let got = annotate_with_error(sql, err);

        assert_eq_with_print(expected, got);
    }

    #[test]
    fn single_line() {
        let sql = "select 1 order by a from b";
        let err = test_parse(sql).unwrap_err();

        let expected = [
            "select 1 order by a from b",
            "                    ^ Expected end of statement, found: from",
        ]
        .join("\n");
        let got = annotate_with_error(sql, err);

        assert_eq_with_print(expected, got);
    }

    #[test]
    fn multiline_with_error_in_middle() {
        let sql = [
            "select",
            "    a,",
            "    b,,",
            "from public.letters",
            "order by b",
        ]
        .join("\n");
        let err = test_parse(&sql).unwrap_err();

        let expected = [
            "select",
            "    a,",
            "    b,,",
            "      ^ Expected an expression:, found: ,",
            "from public.letters",
            "order by b",
        ]
        .join("\n");
        let got = annotate_with_error(&sql, err);

        assert_eq_with_print(expected, got);
    }

    #[test]
    fn multiline_with_error_last_line() {
        let sql = [
            "select a, b, c, d",
            "from public.letters",
            "order by b",
            "limit a from public.letters",
        ]
        .join("\n");
        let err = test_parse(&sql).unwrap_err();

        let expected = [
            "select a, b, c, d",
            "from public.letters",
            "order by b",
            "limit a from public.letters",
            "        ^ Expected end of statement, found: from",
        ]
        .join("\n");
        let got = annotate_with_error(&sql, err);

        assert_eq_with_print(expected, got);
    }

    #[test]
    fn different_query_annotated() {
        let sql = [
            "select a, b, c, d",
            "from public.letters",
            "order by b",
            "limit a from public.letters",
        ]
        .join("\n");
        let err = test_parse(&sql).unwrap_err();
        let expected =
            "sql parser error: Expected end of statement, found: from at Line: 4, Column 9";
        let got = annotate_with_error("select 1, 2", err);

        assert_eq_with_print(expected, got);
    }
}

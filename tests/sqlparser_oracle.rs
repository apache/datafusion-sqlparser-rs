// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Test SQL syntax, specific to [sqlparser::dialect::OracleDialect].

#[cfg(test)]
use pretty_assertions::assert_eq;

use sqlparser::{
    ast::{BinaryOperator, Expr, Ident, QuoteDelimitedString, Value, ValueWithSpan},
    dialect::OracleDialect,
    parser::ParserError,
    tokenizer::Span,
};
use test_utils::{all_dialects_where, expr_from_projection, number, TestedDialects};

mod test_utils;

fn oracle() -> TestedDialects {
    TestedDialects::new(vec![Box::new(OracleDialect)])
}

/// Convenience constructor for [QuoteDelimitedstring].
fn quote_delimited_string(
    start_quote: char,
    value: &'static str,
    end_quote: char,
) -> QuoteDelimitedString {
    QuoteDelimitedString {
        start_quote,
        value: value.into(),
        end_quote,
    }
}

/// Oracle: `||` has a lower precedence than `*` and `/`
#[test]
fn muldiv_have_higher_precedence_than_strconcat() {
    // ...............  A .. B ...... C .. D ...........
    let sql = "SELECT 3 / 5 || 'asdf' || 7 * 9 FROM dual";
    let select = oracle().verified_only_select(sql);
    assert_eq!(1, select.projection.len());
    assert_eq!(
        expr_from_projection(&select.projection[0]),
        // (C || D)
        &Expr::BinaryOp {
            // (A || B)
            left: Box::new(Expr::BinaryOp {
                // A
                left: Box::new(Expr::BinaryOp {
                    left: Box::new(Expr::Value(number("3").into())),
                    op: BinaryOperator::Divide,
                    right: Box::new(Expr::Value(number("5").into())),
                }),
                op: BinaryOperator::StringConcat,
                right: Box::new(Expr::Value(ValueWithSpan {
                    value: Value::SingleQuotedString("asdf".into()),
                    span: Span::empty(),
                })),
            }),
            op: BinaryOperator::StringConcat,
            // D
            right: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Value(number("7").into())),
                op: BinaryOperator::Multiply,
                right: Box::new(Expr::Value(number("9").into())),
            }),
        }
    );
}

/// Oracle: `+`, `-`, and `||` have the same precedence and parse from left-to-right
#[test]
fn plusminus_have_same_precedence_as_strconcat() {
    // ................ A .. B .... C .. D ............
    let sql = "SELECT 3 + 5 || '.3' || 7 - 9 FROM dual";
    let select = oracle().verified_only_select(sql);
    assert_eq!(1, select.projection.len());
    assert_eq!(
        expr_from_projection(&select.projection[0]),
        // D
        &Expr::BinaryOp {
            left: Box::new(Expr::BinaryOp {
                // B
                left: Box::new(Expr::BinaryOp {
                    // A
                    left: Box::new(Expr::BinaryOp {
                        left: Box::new(Expr::Value(number("3").into())),
                        op: BinaryOperator::Plus,
                        right: Box::new(Expr::Value(number("5").into())),
                    }),
                    op: BinaryOperator::StringConcat,
                    right: Box::new(Expr::Value(ValueWithSpan {
                        value: Value::SingleQuotedString(".3".into()),
                        span: Span::empty(),
                    })),
                }),
                op: BinaryOperator::StringConcat,
                right: Box::new(Expr::Value(number("7").into())),
            }),
            op: BinaryOperator::Minus,
            right: Box::new(Expr::Value(number("9").into()))
        }
    );
}

#[test]
fn parse_quote_delimited_string() {
    let dialect = all_dialects_where(|d| d.supports_quote_delimited_string());
    let sql = "SELECT Q'.abc.', \
                      Q'Xab'cX', \
                      Q'|abc'''|', \
                      Q'{abc}d}', \
                      Q'[]abc[]', \
                      Q'<a'bc>', \
                      Q'<<<a'bc>', \
                      Q'('abc'('abc)', \
                      Q'(abc'def))', \
                      Q'(abc'def)))' \
                 FROM dual";
    let select = dialect.verified_only_select(sql);
    assert_eq!(10, select.projection.len());
    assert_eq!(
        &Expr::Value(
            Value::QuoteDelimitedStringLiteral(quote_delimited_string('.', "abc", '.'))
                .with_empty_span()
        ),
        expr_from_projection(&select.projection[0])
    );
    assert_eq!(
        &Expr::Value(
            (Value::QuoteDelimitedStringLiteral(quote_delimited_string('X', "ab'c", 'X')))
                .with_empty_span()
        ),
        expr_from_projection(&select.projection[1])
    );
    assert_eq!(
        &Expr::Value(
            (Value::QuoteDelimitedStringLiteral(quote_delimited_string('|', "abc'''", '|')))
                .with_empty_span()
        ),
        expr_from_projection(&select.projection[2])
    );
    assert_eq!(
        &Expr::Value(
            (Value::QuoteDelimitedStringLiteral(quote_delimited_string('{', "abc}d", '}')))
                .with_empty_span()
        ),
        expr_from_projection(&select.projection[3])
    );
    assert_eq!(
        &Expr::Value(
            (Value::QuoteDelimitedStringLiteral(quote_delimited_string('[', "]abc[", ']')))
                .with_empty_span()
        ),
        expr_from_projection(&select.projection[4])
    );
    assert_eq!(
        &Expr::Value(
            (Value::QuoteDelimitedStringLiteral(quote_delimited_string('<', "a'bc", '>')))
                .with_empty_span()
        ),
        expr_from_projection(&select.projection[5])
    );
    assert_eq!(
        &Expr::Value(
            (Value::QuoteDelimitedStringLiteral(quote_delimited_string('<', "<<a'bc", '>')))
                .with_empty_span()
        ),
        expr_from_projection(&select.projection[6])
    );
    assert_eq!(
        &Expr::Value(
            (Value::QuoteDelimitedStringLiteral(quote_delimited_string('(', "'abc'('abc", ')')))
                .with_empty_span()
        ),
        expr_from_projection(&select.projection[7])
    );
    assert_eq!(
        &Expr::Value(
            (Value::QuoteDelimitedStringLiteral(quote_delimited_string('(', "abc'def)", ')')))
                .with_empty_span()
        ),
        expr_from_projection(&select.projection[8])
    );
    assert_eq!(
        &Expr::Value(
            (Value::QuoteDelimitedStringLiteral(quote_delimited_string('(', "abc'def))", ')')))
                .with_empty_span()
        ),
        expr_from_projection(&select.projection[9])
    );
}

#[test]
fn parse_invalid_quote_delimited_strings() {
    let dialect = all_dialects_where(|d| d.supports_quote_delimited_string());
    // ~ invalid quote delimiter
    for q in [' ', '\t', '\r', '\n'] {
        assert_eq!(
            dialect.parse_sql_statements(&format!("SELECT Q'{q}abc{q}' FROM dual")),
            Err(ParserError::TokenizerError(
                "Invalid space, tab, newline, or EOF after 'Q'' at Line: 1, Column: 10".into()
            )),
            "with quote char {q:?}"
        );
    }
    // ~ invalid eof after quote
    assert_eq!(
        dialect.parse_sql_statements("SELECT Q'"),
        Err(ParserError::TokenizerError(
            "Invalid space, tab, newline, or EOF after 'Q'' at Line: 1, Column: 10".into()
        )),
        "with EOF quote char"
    );
    // ~ unterminated string
    assert_eq!(
        dialect.parse_sql_statements("SELECT Q'|asdfa...."),
        Err(ParserError::TokenizerError(
            "Unterminated string literal at Line: 1, Column: 9".into()
        )),
        "with EOF quote char"
    );
}

#[test]
fn parse_quote_delimited_string_lowercase() {
    let dialect = all_dialects_where(|d| d.supports_quote_delimited_string());
    let sql = "select q'!a'b'c!d!' from dual";
    let select = dialect.verified_only_select_with_canonical(sql, "SELECT Q'!a'b'c!d!' FROM dual");
    assert_eq!(1, select.projection.len());
    assert_eq!(
        &Expr::Value(
            Value::QuoteDelimitedStringLiteral(quote_delimited_string('!', "a'b'c!d", '!'))
                .with_empty_span()
        ),
        expr_from_projection(&select.projection[0])
    );
}

#[test]
fn parse_quote_delimited_string_but_is_a_word() {
    let dialect = all_dialects_where(|d| d.supports_quote_delimited_string());
    let sql = "SELECT q, quux, q.abc FROM dual q";
    let select = dialect.verified_only_select(sql);
    assert_eq!(3, select.projection.len());
    assert_eq!(
        &Expr::Identifier(Ident::with_span(Span::empty(), "q")),
        expr_from_projection(&select.projection[0])
    );
    assert_eq!(
        &Expr::Identifier(Ident::with_span(Span::empty(), "quux")),
        expr_from_projection(&select.projection[1])
    );
    assert_eq!(
        &Expr::CompoundIdentifier(vec![
            Ident::with_span(Span::empty(), "q"),
            Ident::with_span(Span::empty(), "abc")
        ]),
        expr_from_projection(&select.projection[2])
    );
}

#[test]
fn parse_national_quote_delimited_string() {
    let dialect = all_dialects_where(|d| d.supports_quote_delimited_string());
    let sql = "SELECT NQ'.abc.' FROM dual";
    let select = dialect.verified_only_select(sql);
    assert_eq!(1, select.projection.len());
    assert_eq!(
        &Expr::Value(
            Value::NationalQuoteDelimitedStringLiteral(quote_delimited_string('.', "abc", '.'))
                .with_empty_span()
        ),
        expr_from_projection(&select.projection[0])
    );
}

#[test]
fn parse_national_quote_delimited_string_lowercase() {
    let dialect = all_dialects_where(|d| d.supports_quote_delimited_string());
    for prefix in ["nq", "Nq", "nQ", "NQ"] {
        let select = dialect.verified_only_select_with_canonical(
            &format!("select {prefix}'!a'b'c!d!' from dual"),
            "SELECT NQ'!a'b'c!d!' FROM dual",
        );
        assert_eq!(1, select.projection.len());
        assert_eq!(
            &Expr::Value(
                Value::NationalQuoteDelimitedStringLiteral(quote_delimited_string(
                    '!', "a'b'c!d", '!'
                ))
                .with_empty_span()
            ),
            expr_from_projection(&select.projection[0])
        );
    }
}

#[test]
fn parse_national_quote_delimited_string_but_is_a_word() {
    let dialect = all_dialects_where(|d| d.supports_quote_delimited_string());
    let sql = "SELECT nq, nqoo, nq.abc FROM dual q";
    let select = dialect.verified_only_select(sql);
    assert_eq!(3, select.projection.len());
    assert_eq!(
        &Expr::Identifier(Ident::with_span(Span::empty(), "nq")),
        expr_from_projection(&select.projection[0])
    );
    assert_eq!(
        &Expr::Identifier(Ident::with_span(Span::empty(), "nqoo")),
        expr_from_projection(&select.projection[1])
    );
    assert_eq!(
        &Expr::CompoundIdentifier(vec![
            Ident::with_span(Span::empty(), "nq"),
            Ident::with_span(Span::empty(), "abc")
        ]),
        expr_from_projection(&select.projection[2])
    );
}

#[test]
fn parse_optimizer_hints() {
    let oracle_dialect = oracle();

    let select = oracle_dialect.verified_only_select_with_canonical(
        "SELECT /*+one two three*/ /*+not a hint!*/ 1 FROM dual",
        "SELECT /*+one two three*/ 1 FROM dual",
    );
    assert_eq!(
        select
            .optimizer_hint
            .as_ref()
            .map(|hint| hint.text.as_str()),
        Some("one two three")
    );

    let select = oracle_dialect.verified_only_select_with_canonical(
        "SELECT /*one two three*/ /*+not a hint!*/ 1 FROM dual",
        "SELECT 1 FROM dual",
    );
    assert_eq!(select.optimizer_hint, None);

    let select = oracle_dialect.verified_only_select_with_canonical(
        "SELECT --+ one two three /* asdf */\n 1 FROM dual",
        "SELECT --+ one two three /* asdf */\n 1 FROM dual",
    );
    assert_eq!(
        select
            .optimizer_hint
            .as_ref()
            .map(|hint| hint.text.as_str()),
        Some(" one two three /* asdf */\n")
    );
}

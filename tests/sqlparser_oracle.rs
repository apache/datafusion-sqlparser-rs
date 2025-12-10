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
    ast::{BinaryOperator, Expr, Ident, Value, ValueWithSpan},
    dialect::OracleDialect,
    tokenizer::Span,
};
use test_utils::{expr_from_projection, number, TestedDialects};

mod test_utils;

fn oracle() -> TestedDialects {
    TestedDialects::new(vec![Box::new(OracleDialect)])
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
    let select = oracle().verified_only_select(sql);
    assert_eq!(10, select.projection.len());
    assert_eq!(
        &Expr::Value(Value::QuoteDelimitedStringLiteral('.', "abc".into(), '.').with_empty_span()),
        expr_from_projection(&select.projection[0])
    );
    assert_eq!(
        &Expr::Value(
            (Value::QuoteDelimitedStringLiteral('X', "ab'c".into(), 'X')).with_empty_span()
        ),
        expr_from_projection(&select.projection[1])
    );
    assert_eq!(
        &Expr::Value(
            (Value::QuoteDelimitedStringLiteral('|', "abc'''".into(), '|')).with_empty_span()
        ),
        expr_from_projection(&select.projection[2])
    );
    assert_eq!(
        &Expr::Value(
            (Value::QuoteDelimitedStringLiteral('{', "abc}d".into(), '}')).with_empty_span()
        ),
        expr_from_projection(&select.projection[3])
    );
    assert_eq!(
        &Expr::Value(
            (Value::QuoteDelimitedStringLiteral('[', "]abc[".into(), ']')).with_empty_span()
        ),
        expr_from_projection(&select.projection[4])
    );
    assert_eq!(
        &Expr::Value(
            (Value::QuoteDelimitedStringLiteral('<', "a'bc".into(), '>')).with_empty_span()
        ),
        expr_from_projection(&select.projection[5])
    );
    assert_eq!(
        &Expr::Value(
            (Value::QuoteDelimitedStringLiteral('<', "<<a'bc".into(), '>')).with_empty_span()
        ),
        expr_from_projection(&select.projection[6])
    );
    assert_eq!(
        &Expr::Value(
            (Value::QuoteDelimitedStringLiteral('(', "'abc'('abc".into(), ')')).with_empty_span()
        ),
        expr_from_projection(&select.projection[7])
    );
    assert_eq!(
        &Expr::Value(
            (Value::QuoteDelimitedStringLiteral('(', "abc'def)".into(), ')')).with_empty_span()
        ),
        expr_from_projection(&select.projection[8])
    );
    assert_eq!(
        &Expr::Value(
            (Value::QuoteDelimitedStringLiteral('(', "abc'def))".into(), ')')).with_empty_span()
        ),
        expr_from_projection(&select.projection[9])
    );
}

#[test]
fn parse_quote_delimited_string_lowercase() {
    let sql = "select q'!a'b'c!d!' from dual";
    let select = oracle().verified_only_select_with_canonical(sql, "SELECT Q'!a'b'c!d!' FROM dual");
    assert_eq!(1, select.projection.len());
    assert_eq!(
        &Expr::Value(
            Value::QuoteDelimitedStringLiteral('!', "a'b'c!d".into(), '!').with_empty_span()
        ),
        expr_from_projection(&select.projection[0])
    );
}

#[test]
fn parse_quote_delimited_string_but_is_a_word() {
    let sql = "SELECT q, quux, q.abc FROM dual q";
    let select = oracle().verified_only_select(sql);
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
    let sql = "SELECT NQ'.abc.' FROM dual";
    let select = oracle().verified_only_select(sql);
    assert_eq!(1, select.projection.len());
    assert_eq!(
        &Expr::Value(
            Value::NationalQuoteDelimitedStringLiteral('.', "abc".into(), '.').with_empty_span()
        ),
        expr_from_projection(&select.projection[0])
    );
}

#[test]
fn parse_national_quote_delimited_string_lowercase() {
    for prefix in ["nq", "Nq", "nQ", "NQ"] {
        let select = oracle().verified_only_select_with_canonical(
            &format!("select {prefix}'!a'b'c!d!' from dual"),
            "SELECT NQ'!a'b'c!d!' FROM dual",
        );
        assert_eq!(1, select.projection.len());
        assert_eq!(
            &Expr::Value(
                Value::NationalQuoteDelimitedStringLiteral('!', "a'b'c!d".into(), '!')
                    .with_empty_span()
            ),
            expr_from_projection(&select.projection[0])
        );
    }
}

#[test]
fn parse_national_quote_delimited_string_but_is_a_word() {
    let sql = "SELECT nq, nqoo, nq.abc FROM dual q";
    let select = oracle().verified_only_select(sql);
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

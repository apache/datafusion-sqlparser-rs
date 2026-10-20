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

#![warn(clippy::all)]
//! Test comment extraction from SQL source code.

#[cfg(test)]
use pretty_assertions::assert_eq;

use sqlparser::{
    ast::comments::{Comment, CommentWithSpan},
    dialect::GenericDialect,
    parser::Parser,
    test_utils::all_dialects,
    tokenizer::{Location, Span, Token, TokenWithSpan, Tokenizer},
};

#[test]
fn parse_sql_with_comments() {
    let sql = r#"
-- second line comment
select * from /* inline comment after `from` */ dual;

/*select
some
more*/

  -- end-of-script-with-no-newline"#;

    let comments = all_dialects().one_of_identical_results(|dialect| {
        Vec::from(Parser::parse_sql_with_comments(dialect, sql).unwrap().1)
    });

    assert_eq!(
        comments,
        vec![
            CommentWithSpan {
                comment: Comment::SingleLine {
                    content: " second line comment".into(),
                    prefix: "--".into()
                },
                span: Span::new((2, 1).into(), (2, 23).into()),
                prev_token_end: None,
                next_token_start: Some(Location::new(3, 1)),
            },
            CommentWithSpan {
                comment: Comment::MultiLine(" inline comment after `from` ".into()),
                span: Span::new((3, 15).into(), (3, 48).into()),
                prev_token_end: Some(Location::new(3, 14)),
                next_token_start: Some(Location::new(3, 49)),
            },
            CommentWithSpan {
                comment: Comment::MultiLine("select\nsome\nmore".into()),
                span: Span::new((5, 1).into(), (7, 7).into()),
                prev_token_end: Some(Location::new(3, 54)),
                next_token_start: None,
            },
            CommentWithSpan {
                comment: Comment::SingleLine {
                    content: " end-of-script-with-no-newline".into(),
                    prefix: "--".into()
                },
                span: Span::new((9, 3).into(), (9, 35).into()),
                prev_token_end: Some(Location::new(3, 54)),
                next_token_start: None,
            }
        ]
    );
}

#[test]
fn parse_sql_with_comments_between_tokens() {
    let sql = r#"SELECT 1; /* a */

-- b
/* c */ SELECT 2"#;
    let comments = all_dialects().one_of_identical_results(|dialect| {
        Vec::from(Parser::parse_sql_with_comments(dialect, sql).unwrap().1)
    });

    assert_eq!(
        comments,
        vec![
            CommentWithSpan {
                comment: Comment::MultiLine(" a ".into()),
                span: Span::new((1, 11).into(), (1, 18).into()),
                prev_token_end: Some(Location::new(1, 10)),
                next_token_start: Some(Location::new(4, 9)),
            },
            CommentWithSpan {
                comment: Comment::SingleLine {
                    content: " b".into(),
                    prefix: "--".into(),
                },
                span: Span::new((3, 1).into(), (3, 5).into()),
                prev_token_end: Some(Location::new(1, 10)),
                next_token_start: Some(Location::new(4, 9)),
            },
            CommentWithSpan {
                comment: Comment::MultiLine(" c ".into()),
                span: Span::new((4, 1).into(), (4, 8).into()),
                prev_token_end: Some(Location::new(1, 10)),
                next_token_start: Some(Location::new(4, 9)),
            },
        ]
    );
}

#[test]
fn parse_sql_with_comments_only() {
    for (sql, expected) in [
        ("", vec![]),
        (
            r#"-- a
/* b */"#,
            vec![
                CommentWithSpan {
                    comment: Comment::SingleLine {
                        content: " a".into(),
                        prefix: "--".into(),
                    },
                    span: Span::new((1, 1).into(), (1, 5).into()),
                    prev_token_end: None,
                    next_token_start: None,
                },
                CommentWithSpan {
                    comment: Comment::MultiLine(" b ".into()),
                    span: Span::new((2, 1).into(), (2, 8).into()),
                    prev_token_end: None,
                    next_token_start: None,
                },
            ],
        ),
    ] {
        let comments = all_dialects().one_of_identical_results(|dialect| {
            Vec::from(Parser::parse_sql_with_comments(dialect, sql).unwrap().1)
        });
        assert_eq!(comments, expected);
    }
}

#[test]
fn into_comments_with_eof() {
    let mut tokens = Tokenizer::new(&GenericDialect, "SELECT 1; -- tail")
        .tokenize_with_location()
        .unwrap();
    let eof = tokens.last().unwrap().span.end;
    tokens.push(TokenWithSpan {
        token: Token::EOF,
        span: Span::new(eof, eof),
    });
    let comments = Parser::new(&GenericDialect)
        .with_tokens_with_locations(tokens)
        .into_comments();

    assert_eq!(
        Vec::from(comments),
        vec![CommentWithSpan {
            comment: Comment::SingleLine {
                content: " tail".into(),
                prefix: "--".into(),
            },
            span: Span::new((1, 11).into(), (1, 18).into()),
            prev_token_end: Some(Location::new(1, 10)),
            next_token_start: None,
        }]
    );
}

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
    tokenizer::Span,
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

    let comments = match Parser::parse_sql_with_comments(&GenericDialect, sql) {
        Ok((_, comments)) => comments,
        Err(e) => panic!("Invalid sql script: {e}"),
    };

    assert_eq!(
        Vec::from(comments),
        vec![
            CommentWithSpan {
                comment: Comment::SingleLine {
                    content: " second line comment\n".into(),
                    prefix: "--".into()
                },
                span: Span::new((2, 1).into(), (3, 1).into()),
            },
            CommentWithSpan {
                comment: Comment::MultiLine(" inline comment after `from` ".into()),
                span: Span::new((3, 15).into(), (3, 48).into()),
            },
            CommentWithSpan {
                comment: Comment::MultiLine("select\nsome\nmore".into()),
                span: Span::new((5, 1).into(), (7, 7).into())
            },
            CommentWithSpan {
                comment: Comment::SingleLine {
                    content: " end-of-script-with-no-newline".into(),
                    prefix: "--".into()
                },
                span: Span::new((9, 3).into(), (9, 35).into()),
            }
        ]
    );
}

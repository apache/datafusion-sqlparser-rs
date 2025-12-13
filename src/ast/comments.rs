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

//! Provides a representation of source code comments in parsed SQL code.
//!
//! See [Comments::find] for an example.

#[cfg(not(feature = "std"))]
use alloc::{string::String, vec::Vec};

use core::{
    ops::{Bound, Deref, RangeBounds},
    slice,
};

use crate::tokenizer::{Location, Span};

/// An opaque container for comments from a parse SQL source code.
#[derive(Default, Debug)]
pub struct Comments(Vec<CommentWithSpan>);

impl Comments {
    pub(crate) fn push(&mut self, comment: CommentWithSpan) {
        debug_assert!(
            self.0
                .last()
                .map(|last| last.span < comment.span)
                .unwrap_or(true)
        );
        self.0.push(comment);
    }

    /// Finds comments starting within the given location range. The order of
    /// iterator reflects the order of the comments as encountered in the parsed
    /// source code.
    ///
    /// # Example
    /// ```rust
    /// use sqlparser::{dialect::GenericDialect, parser::Parser, tokenizer::Location};
    ///
    /// let sql = r#"/*
    ///  header comment ...
    ///  ... spanning multiple lines
    /// */
    ///
    ///  -- first statement
    ///  SELECT 'hello' /* world */ FROM DUAL;
    ///
    ///  -- second statement
    ///  SELECT 123 FROM DUAL;
    ///
    ///  -- trailing comment
    /// "#;
    ///
    /// let (ast, comments) = Parser::parse_sql_with_comments(&GenericDialect, sql).unwrap();
    ///
    /// // all comments appearing before line seven, i.e. before the first statement itself
    /// assert_eq!(
    ///    &comments.find(..Location::new(7, 1)).map(|c| c.as_str()).collect::<Vec<_>>(),
    ///    &["\n header comment ...\n ... spanning multiple lines\n", " first statement\n"]);
    ///
    /// // all comments appearing within the first statement
    /// assert_eq!(
    ///    &comments.find(Location::new(7, 1)..Location::new(8,1)).map(|c| c.as_str()).collect::<Vec<_>>(),
    ///    &[" world "]);
    ///
    /// // all comments appearing within or after the first statement
    /// assert_eq!(
    ///    &comments.find(Location::new(7, 1)..).map(|c| c.as_str()).collect::<Vec<_>>(),
    ///    &[" world ", " second statement\n", " trailing comment\n"]);
    /// ```
    ///
    /// The [Spanned](crate::ast::Spanned) trait allows you to access location
    /// information for certain AST nodes.
    pub fn find<R: RangeBounds<Location>>(&self, range: R) -> Iter<'_> {
        let (start, end) = (
            self.start_index(range.start_bound()),
            self.end_index(range.end_bound()),
        );
        debug_assert!((0..=self.0.len()).contains(&start));
        debug_assert!((0..=self.0.len()).contains(&end));
        // in case the user specified a reverse range
        Iter(if start <= end {
            self.0[start..end].iter()
        } else {
            self.0[0..0].iter()
        })
    }

    /// Find the index of the first comment starting "before" the given location.
    ///
    /// The returned index is _inclusive_ and within the range of `0..=self.0.len()`.
    fn start_index(&self, location: Bound<&Location>) -> usize {
        match location {
            Bound::Included(location) => {
                match self.0.binary_search_by(|c| c.span.start.cmp(location)) {
                    Ok(i) => i,
                    Err(i) => i,
                }
            }
            Bound::Excluded(location) => {
                match self.0.binary_search_by(|c| c.span.start.cmp(location)) {
                    Ok(i) => i + 1,
                    Err(i) => i,
                }
            }
            Bound::Unbounded => 0,
        }
    }

    /// Find the index of the first comment starting "after" the given location.
    ///
    /// The returned index is _exclusive_ and within the range of `0..=self.0.len()`.
    fn end_index(&self, location: Bound<&Location>) -> usize {
        match location {
            Bound::Included(location) => {
                match self.0.binary_search_by(|c| c.span.start.cmp(location)) {
                    Ok(i) => i + 1,
                    Err(i) => i,
                }
            }
            Bound::Excluded(location) => {
                match self.0.binary_search_by(|c| c.span.start.cmp(location)) {
                    Ok(i) => i,
                    Err(i) => i,
                }
            }
            Bound::Unbounded => self.0.len(),
        }
    }
}

impl From<Comments> for Vec<CommentWithSpan> {
    fn from(comments: Comments) -> Self {
        comments.0
    }
}

/// A source code comment with information of its entire span.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommentWithSpan {
    /// The source code comment iself
    pub comment: Comment,
    /// The span of the comment including its markers
    pub span: Span,
}

impl Deref for CommentWithSpan {
    type Target = Comment;

    fn deref(&self) -> &Self::Target {
        &self.comment
    }
}

/// A unified type of the different source code comment formats.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Comment {
    /// A single line comment, typically introduced with a prefix and spanning
    /// until end-of-line or end-of-file in the source code.
    ///
    /// Note: `content` will include the terminating new-line character, if any.
    SingleLine { content: String, prefix: String },

    /// A multi-line comment, typically enclosed in `/* .. */` markers. The
    /// string represents the content excluding the markers.
    MultiLine(String),
}

impl Comment {
    /// Retrieves the content of the comment as string slice.
    pub fn as_str(&self) -> &str {
        match self {
            Comment::SingleLine { content, prefix: _ } => content.as_str(),
            Comment::MultiLine(content) => content.as_str(),
        }
    }
}

impl Deref for Comment {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

/// An opaque iterator implementation over comments served by [Comments::find].
pub struct Iter<'a>(slice::Iter<'a, CommentWithSpan>);

impl<'a> Iterator for Iter<'a> {
    type Item = &'a CommentWithSpan;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_find() {
        let comments = {
            // ```
            // -- abc
            //   /* hello */--, world
            // /* def
            //  ghi
            //  jkl
            // */
            // ```
            let mut c = Comments(Vec::new());
            c.push(CommentWithSpan {
                comment: Comment::SingleLine {
                    content: " abc".into(),
                    prefix: "--".into(),
                },
                span: Span::new((1, 1).into(), (1, 7).into()),
            });
            c.push(CommentWithSpan {
                comment: Comment::MultiLine(" hello ".into()),
                span: Span::new((2, 3).into(), (2, 14).into()),
            });
            c.push(CommentWithSpan {
                comment: Comment::SingleLine {
                    content: ", world".into(),
                    prefix: "--".into(),
                },
                span: Span::new((2, 14).into(), (2, 21).into()),
            });
            c.push(CommentWithSpan {
                comment: Comment::MultiLine(" def\n ghi\n jkl\n".into()),
                span: Span::new((3, 3).into(), (7, 1).into()),
            });
            c
        };

        fn find<R: RangeBounds<Location>>(comments: &Comments, range: R) -> Vec<&str> {
            comments.find(range).map(|c| c.as_str()).collect::<Vec<_>>()
        }

        // ~ end-points only --------------------------------------------------
        assert_eq!(find(&comments, ..Location::new(0, 0)), Vec::<&str>::new());
        assert_eq!(find(&comments, ..Location::new(2, 1)), vec![" abc"]);
        assert_eq!(find(&comments, ..Location::new(2, 3)), vec![" abc"]);
        assert_eq!(
            find(&comments, ..=Location::new(2, 3)),
            vec![" abc", " hello "]
        );
        assert_eq!(
            find(&comments, ..=Location::new(2, 3)),
            vec![" abc", " hello "]
        );
        assert_eq!(
            find(&comments, ..Location::new(2, 15)),
            vec![" abc", " hello ", ", world"]
        );

        // ~ start-points only ------------------------------------------------
        assert_eq!(
            find(&comments, Location::new(1000, 1000)..),
            Vec::<&str>::new()
        );
        assert_eq!(
            find(&comments, Location::new(2, 14)..),
            vec![", world", " def\n ghi\n jkl\n"]
        );
        assert_eq!(
            find(&comments, Location::new(2, 15)..),
            vec![" def\n ghi\n jkl\n"]
        );
        assert_eq!(
            find(&comments, Location::new(0, 0)..),
            vec![" abc", " hello ", ", world", " def\n ghi\n jkl\n"]
        );
        assert_eq!(
            find(&comments, Location::new(1, 1)..),
            vec![" abc", " hello ", ", world", " def\n ghi\n jkl\n"]
        );

        // ~ ranges -----------------------------------------------------------
        assert_eq!(
            find(&comments, Location::new(2, 1)..Location::new(1, 1)),
            Vec::<&str>::new()
        );
        assert_eq!(
            find(&comments, Location::new(1, 1)..Location::new(2, 3)),
            vec![" abc"]
        );
        assert_eq!(
            find(&comments, Location::new(1, 1)..=Location::new(2, 3)),
            vec![" abc", " hello "]
        );
        assert_eq!(
            find(&comments, Location::new(1, 1)..=Location::new(2, 10)),
            vec![" abc", " hello "]
        );
        assert_eq!(
            find(&comments, Location::new(1, 1)..=Location::new(2, 14)),
            vec![" abc", " hello ", ", world"]
        );
        assert_eq!(
            find(&comments, Location::new(1, 1)..Location::new(2, 15)),
            vec![" abc", " hello ", ", world"]
        );

        // ~ find everything --------------------------------------------------
        assert_eq!(
            find(&comments, ..),
            vec![" abc", " hello ", ", world", " def\n ghi\n jkl\n"]
        );
    }
}

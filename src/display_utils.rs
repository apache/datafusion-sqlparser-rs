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

//! Utilities for formatting SQL AST nodes with pretty printing support.
//!
//! The module provides formatters that implement the `Display` trait with support
//! for both regular (`{}`) and pretty (`{:#}`) formatting modes. Pretty printing
//! adds proper indentation and line breaks to make SQL statements more readable.

use core::fmt::{self, Display, Write};

/// A wrapper around a value that adds an indent to the value when displayed with {:#}.
pub(crate) struct Indent<T>(pub T);

const INDENT: &str = "  ";

impl<T> Display for Indent<T>
where
    T: Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if f.alternate() {
            f.write_str(INDENT)?;
            write!(Indent(f), "{:#}", self.0)
        } else {
            self.0.fmt(f)
        }
    }
}

/// Adds an indent to the inner writer
impl<T> Write for Indent<T>
where
    T: Write,
{
    fn write_str(&mut self, s: &str) -> fmt::Result {
        self.0.write_str(s)?;
        // Our NewLine and SpaceOrNewline utils always print individual newlines as a single-character string.
        if s == "\n" {
            self.0.write_str(INDENT)?;
        }
        Ok(())
    }
}

/// A value that inserts a newline when displayed with {:#}, but not when displayed with {}.
pub(crate) struct NewLine;

impl Display for NewLine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if f.alternate() {
            f.write_char('\n')
        } else {
            Ok(())
        }
    }
}

/// A value that inserts a space when displayed with {}, but a newline when displayed with {:#}.
pub(crate) struct SpaceOrNewline;

impl Display for SpaceOrNewline {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if f.alternate() {
            f.write_char('\n')
        } else {
            f.write_char(' ')
        }
    }
}

/// A value that displays a comma-separated list of values.
/// When pretty-printed (using {:#}), it displays each value on a new line.
pub(crate) struct DisplayCommaSeparated<'a, T: fmt::Display>(pub(crate) &'a [T]);

impl<T: fmt::Display> fmt::Display for DisplayCommaSeparated<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut first = true;
        for t in self.0 {
            if !first {
                f.write_char(',')?;
                SpaceOrNewline.fmt(f)?;
            }
            first = false;
            t.fmt(f)?;
        }
        Ok(())
    }
}

/// Displays a whitespace, followed by a comma-separated list that is indented when pretty-printed.
pub(crate) fn indented_list<T: fmt::Display>(f: &mut fmt::Formatter, items: &[T]) -> fmt::Result {
    SpaceOrNewline.fmt(f)?;
    Indent(DisplayCommaSeparated(items)).fmt(f)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_indent() {
        struct TwoLines;

        impl Display for TwoLines {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("line 1")?;
                SpaceOrNewline.fmt(f)?;
                f.write_str("line 2")
            }
        }

        let indent = Indent(TwoLines);
        assert_eq!(
            indent.to_string(),
            TwoLines.to_string(),
            "Only the alternate form should be indented"
        );
        assert_eq!(format!("{:#}", indent), "  line 1\n  line 2");
    }
}

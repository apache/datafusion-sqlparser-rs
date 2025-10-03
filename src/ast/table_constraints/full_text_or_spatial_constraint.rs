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

//! SQL Abstract Syntax Tree (AST) for the `FullTextOrSpatialConstraint` table constraint.

use crate::ast::{Ident, IndexColumn, KeyOrIndexDisplay};
use crate::tokenizer::Span;
use core::fmt;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

#[cfg(feature = "visitor")]
use sqlparser_derive::{Visit, VisitMut};

/// MySQLs [fulltext][1] definition. Since the [`SPATIAL`][2] definition is exactly the same,
/// and MySQL displays both the same way, it is part of this definition as well.
///
/// Supported syntax:
///
/// ```markdown
/// {FULLTEXT | SPATIAL} [INDEX | KEY] [index_name] (key_part,...)
///
/// key_part: col_name
/// ```
///
/// [1]: https://dev.mysql.com/doc/refman/8.0/en/fulltext-natural-language.html
/// [2]: https://dev.mysql.com/doc/refman/8.0/en/spatial-types.html
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct FullTextOrSpatialConstraint {
    /// Whether this is a `FULLTEXT` (true) or `SPATIAL` (false) definition.
    pub fulltext: bool,
    /// Whether the type is followed by the keyword `KEY`, `INDEX`, or no keyword at all.
    pub index_type_display: KeyOrIndexDisplay,
    /// Optional index name.
    pub opt_index_name: Option<Ident>,
    /// Referred column identifier list.
    pub columns: Vec<IndexColumn>,
}

impl fmt::Display for FullTextOrSpatialConstraint {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use crate::ast::display_comma_separated;

        if self.fulltext {
            write!(f, "FULLTEXT")?;
        } else {
            write!(f, "SPATIAL")?;
        }

        write!(f, "{:>}", self.index_type_display)?;

        if let Some(name) = &self.opt_index_name {
            write!(f, " {name}")?;
        }

        write!(f, " ({})", display_comma_separated(&self.columns))?;

        Ok(())
    }
}

impl crate::ast::Spanned for FullTextOrSpatialConstraint {
    fn span(&self) -> Span {
        fn union_spans<I: Iterator<Item = Span>>(iter: I) -> Span {
            Span::union_iter(iter)
        }

        union_spans(
            self.opt_index_name
                .iter()
                .map(|i| i.span)
                .chain(self.columns.iter().map(|i| i.span())),
        )
    }
}

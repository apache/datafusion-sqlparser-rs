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

//! SQL Abstract Syntax Tree (AST) for the `IndexConstraint` table constraint.

use crate::ast::{display_comma_separated, Ident, IndexColumn, IndexOption, IndexType};
use crate::tokenizer::Span;
use core::fmt;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

#[cfg(feature = "visitor")]
use sqlparser_derive::{Visit, VisitMut};

/// MySQLs [index definition][1] for index creation. Not present on ANSI so, for now, the usage
/// is restricted to MySQL, as no other dialects that support this syntax were found.
///
/// `{INDEX | KEY} [index_name] [index_type] (key_part,...) [index_option]...`
///
/// [1]: https://dev.mysql.com/doc/refman/8.0/en/create-table.html
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct IndexConstraint {
    /// Whether this index starts with KEY (true) or INDEX (false), to maintain the same syntax.
    pub display_as_key: bool,
    /// Index name.
    pub name: Option<Ident>,
    /// Optional [index type][1].
    ///
    /// [1]: IndexType
    pub index_type: Option<IndexType>,
    /// Referred column identifier list.
    pub columns: Vec<IndexColumn>,
    /// Optional index options such as `USING`; see [`IndexOption`].
    pub index_options: Vec<IndexOption>,
}

impl fmt::Display for IndexConstraint {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", if self.display_as_key { "KEY" } else { "INDEX" })?;
        if let Some(name) = &self.name {
            write!(f, " {name}")?;
        }
        if let Some(index_type) = &self.index_type {
            write!(f, " USING {index_type}")?;
        }
        write!(f, " ({})", display_comma_separated(&self.columns))?;
        if !self.index_options.is_empty() {
            write!(f, " {}", display_comma_separated(&self.index_options))?;
        }
        Ok(())
    }
}

impl crate::ast::Spanned for IndexConstraint {
    fn span(&self) -> Span {
        fn union_spans<I: Iterator<Item = Span>>(iter: I) -> Span {
            Span::union_iter(iter)
        }

        union_spans(
            self.name
                .iter()
                .map(|i| i.span)
                .chain(self.columns.iter().map(|i| i.span())),
        )
    }
}

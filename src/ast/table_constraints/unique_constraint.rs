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

//! SQL Abstract Syntax Tree (AST) for the `UniqueConstraint` table constraint.

use crate::ast::{
    display_comma_separated, display_separated, ConstraintCharacteristics, Ident, IndexColumn,
    IndexOption, IndexType, KeyOrIndexDisplay, NullsDistinctOption,
};
use crate::tokenizer::Span;
use core::fmt;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

#[cfg(feature = "visitor")]
use sqlparser_derive::{Visit, VisitMut};

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct UniqueConstraint {
    /// Constraint name.
    ///
    /// Can be not the same as `index_name`
    pub name: Option<Ident>,
    /// Index name
    pub index_name: Option<Ident>,
    /// Whether the type is followed by the keyword `KEY`, `INDEX`, or no keyword at all.
    pub index_type_display: KeyOrIndexDisplay,
    /// Optional `USING` of [index type][1] statement before columns.
    ///
    /// [1]: IndexType
    pub index_type: Option<IndexType>,
    /// Identifiers of the columns that are unique.
    pub columns: Vec<IndexColumn>,
    pub index_options: Vec<IndexOption>,
    pub characteristics: Option<ConstraintCharacteristics>,
    /// Optional Postgres nulls handling: `[ NULLS [ NOT ] DISTINCT ]`
    pub nulls_distinct: NullsDistinctOption,
}

impl fmt::Display for UniqueConstraint {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use crate::ast::ddl::{display_constraint_name, display_option, display_option_spaced};
        write!(
            f,
            "{}UNIQUE{}{:>}{}{} ({})",
            display_constraint_name(&self.name),
            self.nulls_distinct,
            self.index_type_display,
            display_option_spaced(&self.index_name),
            display_option(" USING ", "", &self.index_type),
            display_comma_separated(&self.columns),
        )?;

        if !self.index_options.is_empty() {
            write!(f, " {}", display_separated(&self.index_options, " "))?;
        }

        write!(f, "{}", display_option_spaced(&self.characteristics))?;
        Ok(())
    }
}

impl crate::ast::Spanned for UniqueConstraint {
    fn span(&self) -> Span {
        fn union_spans<I: Iterator<Item = Span>>(iter: I) -> Span {
            Span::union_iter(iter)
        }

        union_spans(
            self.name
                .iter()
                .map(|i| i.span)
                .chain(self.index_name.iter().map(|i| i.span))
                .chain(self.columns.iter().map(|i| i.span()))
                .chain(self.characteristics.iter().map(|i| i.span())),
        )
    }
}

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

//! SQL Abstract Syntax Tree (AST) for the `PrimaryKeyConstraint` table constraint.

use crate::ast::{
    display_comma_separated, display_separated, ConstraintCharacteristics, Ident, IndexColumn,
    IndexOption, IndexType,
};
use crate::tokenizer::Span;
use core::fmt;

#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

#[cfg(feature = "visitor")]
use sqlparser_derive::{Visit, VisitMut};

/// MySQL [definition][1] for `PRIMARY KEY` constraints statements:
/// * `[CONSTRAINT [<name>]] PRIMARY KEY [index_name] [index_type] (<columns>) <index_options>`
///
/// Actually the specification have no `[index_name]` but the next query will complete successfully:
/// ```sql
/// CREATE TABLE unspec_table (
///   xid INT NOT NULL,
///   CONSTRAINT p_name PRIMARY KEY index_name USING BTREE (xid)
/// );
/// ```
///
/// where:
/// * [index_type][2] is `USING {BTREE | HASH}`
/// * [index_options][3] is `{index_type | COMMENT 'string' | ... %currently unsupported stmts% } ...`
///
/// [1]: https://dev.mysql.com/doc/refman/8.3/en/create-table.html
/// [2]: IndexType
/// [3]: IndexOption
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct PrimaryKeyConstraint {
    /// Constraint name.
    ///
    /// Can be not the same as `index_name`
    pub name: Option<Ident>,
    /// Index name
    pub index_name: Option<Ident>,
    /// Optional `USING` of [index type][1] statement before columns.
    ///
    /// [1]: IndexType
    pub index_type: Option<IndexType>,
    /// Identifiers of the columns that form the primary key.
    pub columns: Vec<IndexColumn>,
    pub index_options: Vec<IndexOption>,
    pub characteristics: Option<ConstraintCharacteristics>,
}

impl fmt::Display for PrimaryKeyConstraint {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use crate::ast::ddl::{display_constraint_name, display_option, display_option_spaced};
        write!(
            f,
            "{}PRIMARY KEY{}{} ({})",
            display_constraint_name(&self.name),
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

impl crate::ast::Spanned for PrimaryKeyConstraint {
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

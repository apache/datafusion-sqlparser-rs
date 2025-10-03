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

//! SQL Abstract Syntax Tree (AST) for the `ForeignKeyConstraint` table constraint.

use crate::ast::{
    display_comma_separated, ConstraintCharacteristics, Ident, ObjectName, ReferentialAction,
};
use crate::tokenizer::Span;
use core::fmt;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

#[cfg(feature = "visitor")]
use sqlparser_derive::{Visit, VisitMut};

/// A referential integrity constraint (`[ CONSTRAINT <name> ] FOREIGN KEY (<columns>)
/// REFERENCES <foreign_table> (<referred_columns>)
/// { [ON DELETE <referential_action>] [ON UPDATE <referential_action>] |
///   [ON UPDATE <referential_action>] [ON DELETE <referential_action>]
/// }`).
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct ForeignKeyConstraint {
    pub name: Option<Ident>,
    /// MySQL-specific field
    /// <https://dev.mysql.com/doc/refman/8.4/en/create-table-foreign-keys.html>
    pub index_name: Option<Ident>,
    pub columns: Vec<Ident>,
    pub foreign_table: ObjectName,
    pub referred_columns: Vec<Ident>,
    pub on_delete: Option<ReferentialAction>,
    pub on_update: Option<ReferentialAction>,
    pub characteristics: Option<ConstraintCharacteristics>,
}

impl fmt::Display for ForeignKeyConstraint {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use crate::ast::ddl::{display_constraint_name, display_option_spaced};
        write!(
            f,
            "{}FOREIGN KEY{} ({}) REFERENCES {}",
            display_constraint_name(&self.name),
            display_option_spaced(&self.index_name),
            display_comma_separated(&self.columns),
            self.foreign_table,
        )?;
        if !self.referred_columns.is_empty() {
            write!(f, "({})", display_comma_separated(&self.referred_columns))?;
        }
        if let Some(action) = &self.on_delete {
            write!(f, " ON DELETE {action}")?;
        }
        if let Some(action) = &self.on_update {
            write!(f, " ON UPDATE {action}")?;
        }
        if let Some(characteristics) = &self.characteristics {
            write!(f, " {characteristics}")?;
        }
        Ok(())
    }
}

impl crate::ast::Spanned for ForeignKeyConstraint {
    fn span(&self) -> Span {
        fn union_spans<I: Iterator<Item = Span>>(iter: I) -> Span {
            Span::union_iter(iter)
        }

        union_spans(
            self.name
                .iter()
                .map(|i| i.span)
                .chain(self.index_name.iter().map(|i| i.span))
                .chain(self.columns.iter().map(|i| i.span))
                .chain(core::iter::once(self.foreign_table.span()))
                .chain(self.referred_columns.iter().map(|i| i.span))
                .chain(self.on_delete.iter().map(|i| i.span()))
                .chain(self.on_update.iter().map(|i| i.span()))
                .chain(self.characteristics.iter().map(|i| i.span())),
        )
    }
}

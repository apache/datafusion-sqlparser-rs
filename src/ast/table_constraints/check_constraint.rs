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

//! SQL Abstract Syntax Tree (AST) for the `CheckConstraint` table constraint.

use crate::ast::{Expr, Ident};
use crate::tokenizer::Span;
use core::fmt;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

#[cfg(feature = "visitor")]
use sqlparser_derive::{Visit, VisitMut};

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct CheckConstraint {
    pub name: Option<Ident>,
    pub expr: Box<Expr>,
    /// MySQL-specific syntax
    /// <https://dev.mysql.com/doc/refman/8.4/en/create-table.html>
    pub enforced: Option<bool>,
}

impl fmt::Display for CheckConstraint {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use crate::ast::ddl::display_constraint_name;
        write!(
            f,
            "{}CHECK ({})",
            display_constraint_name(&self.name),
            self.expr
        )?;
        if let Some(b) = self.enforced {
            write!(f, " {}", if b { "ENFORCED" } else { "NOT ENFORCED" })
        } else {
            Ok(())
        }
    }
}

impl crate::ast::Spanned for CheckConstraint {
    fn span(&self) -> Span {
        self.expr
            .span()
            .union_opt(&self.name.as_ref().map(|i| i.span))
    }
}

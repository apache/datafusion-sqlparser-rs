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

#[cfg(not(feature = "std"))]
use alloc::vec::Vec;
use core::fmt;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
#[cfg(feature = "visitor")]
use sqlparser_derive::{Visit, VisitMut};

use super::{
    display_comma_separated, display_separated, Expr, ExprWithAlias, Ident, ObjectName, Spanned,
};
use crate::tokenizer::Span;

/// A SQL/PGQ `GRAPH_TABLE` table expression.
///
/// ISO/IEC 9075-16:2023, 7.1.
///
/// <https://docs.oracle.com/en/database/oracle/oracle-database/26/sqlrf/graph_table-operator.html>
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct GraphTable {
    /// The property graph to match against.
    #[cfg_attr(feature = "visitor", visit(with = "visit_relation"))]
    pub graph: ObjectName,
    /// The graph pattern following `MATCH`.
    pub pattern: GraphPattern,
    /// The expressions projected by `COLUMNS`.
    pub columns: Vec<ExprWithAlias>,
}

impl fmt::Display for GraphTable {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "GRAPH_TABLE ({} MATCH {} COLUMNS ({}))",
            self.graph,
            self.pattern,
            display_comma_separated(&self.columns)
        )
    }
}

/// A graph pattern following `MATCH` (ISO/IEC 9075-16:2023, 10.4).
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct GraphPattern {
    /// Comma-separated path patterns.
    pub paths: Vec<GraphPathPattern>,
    /// The graph pattern's `WHERE` condition.
    pub filter: Option<Expr>,
}

impl fmt::Display for GraphPattern {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", display_comma_separated(&self.paths))?;
        if let Some(filter) = &self.filter {
            write!(f, " WHERE {filter}")?;
        }
        Ok(())
    }
}

/// A sequence of graph pattern elements, including implicit vertices between edges.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct GraphPathPattern {
    /// Elements in path order.
    pub elements: Vec<GraphPatternElement>,
}

impl fmt::Display for GraphPathPattern {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", display_separated(&self.elements, " "))
    }
}

/// A vertex or a full edge pattern.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum GraphPatternElement {
    /// A vertex pattern enclosed in parentheses.
    Vertex(GraphElementPattern),
    /// An edge pattern with its direction and bracketed filler.
    Edge {
        /// The edge's direction relative to the path.
        direction: GraphEdgeDirection,
        /// The variable, label, and condition enclosed in brackets.
        pattern: GraphElementPattern,
    },
}

impl fmt::Display for GraphPatternElement {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Vertex(pattern) => write!(f, "({pattern})"),
            Self::Edge { direction, pattern } => match direction {
                GraphEdgeDirection::Right => write!(f, "-[{pattern}]->"),
                GraphEdgeDirection::Left => write!(f, "<-[{pattern}]-"),
                GraphEdgeDirection::Any => write!(f, "-[{pattern}]-"),
            },
        }
    }
}

/// The direction in which an edge pattern can match.
#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum GraphEdgeDirection {
    /// A directed edge pointing right, `-[...]->`.
    Right,
    /// A directed edge pointing left, `<-[...]-`.
    Left,
    /// A directed edge in either direction or an undirected edge, `-[...]-`.
    Any,
}

/// The optional filler shared by vertex and edge patterns.
#[derive(Debug, Default, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct GraphElementPattern {
    /// The element variable, if named.
    pub variable: Option<Ident>,
    /// The label introduced by `IS`.
    pub label: Option<GraphLabelExpression>,
    /// The element pattern's `WHERE` condition.
    pub filter: Option<Expr>,
}

impl fmt::Display for GraphElementPattern {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let Some(variable) = &self.variable {
            write!(f, "{variable}")?;
        }
        if let Some(label) = &self.label {
            if self.variable.is_some() {
                f.write_str(" ")?;
            }
            write!(f, "IS {label}")?;
        }
        if let Some(filter) = &self.filter {
            if self.variable.is_some() || self.label.is_some() {
                f.write_str(" ")?;
            }
            write!(f, "WHERE {filter}")?;
        }
        Ok(())
    }
}

/// A label expression within a graph element pattern.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum GraphLabelExpression {
    /// A single label name.
    Label(Ident),
}

impl fmt::Display for GraphLabelExpression {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Label(label) => write!(f, "{label}"),
        }
    }
}

impl Spanned for GraphTable {
    fn span(&self) -> Span {
        Span::union_iter(
            [self.graph.span(), self.pattern.span()]
                .into_iter()
                .chain(self.columns.iter().map(Spanned::span)),
        )
    }
}

impl Spanned for GraphPattern {
    fn span(&self) -> Span {
        Span::union_iter(
            self.paths
                .iter()
                .map(Spanned::span)
                .chain(self.filter.iter().map(Spanned::span)),
        )
    }
}

impl Spanned for GraphPathPattern {
    fn span(&self) -> Span {
        Span::union_iter(self.elements.iter().map(Spanned::span))
    }
}

impl Spanned for GraphPatternElement {
    fn span(&self) -> Span {
        match self {
            Self::Vertex(pattern) | Self::Edge { pattern, .. } => pattern.span(),
        }
    }
}

impl Spanned for GraphElementPattern {
    fn span(&self) -> Span {
        Span::union_iter(
            self.variable
                .iter()
                .map(|variable| variable.span)
                .chain(self.label.iter().map(Spanned::span))
                .chain(self.filter.iter().map(Spanned::span)),
        )
    }
}

impl Spanned for GraphLabelExpression {
    fn span(&self) -> Span {
        match self {
            Self::Label(label) => label.span,
        }
    }
}

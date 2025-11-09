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

use core::fmt;
use crate::ast::{Expr, Ident};


#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum SingleQuery{

    Single(SinglePartQuery),

    Multiple(MultiPartQuery),
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct SinglePartQuery{

    pub reading_clause: ReadingClause
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct SimpleSinglePartQuery{

    pub reading_clause: ReadingClause,

    pub returning_clause: ReturningClause,
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct MultiPartQuery{

    pub reading_clauses: Vec<ReadingClause>

}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum ReadingClause{
    Match(MatchClause),
    // Other reading clauses can be added here
}

/// A Cypher MATCH clause
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct MatchClause {
    /// Whether the MATCH is optional
    pub optional: bool,
    /// The pattern to match
    pub pattern: Pattern,
}

/// A complete path pattern in Cypher
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct Pattern {
    /// The sequence of nodes and relationships that make up this path
    pub parts: Vec<PatternPart>,
}

/// A Cypher simple pattern part, e.g., (a)-[r:KNOWS]->(b)
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct PatternPart {

    pub variable: Option<Ident>,
    
    pub anon_pattern_part: PatternElement,
}

/// An element in a path pattern (either a node or relationship)
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum PatternElement {
    // First variant: Node with optional chains
    Simple(SimplePatternElement),
    // Second variant: Nested pattern element in parentheses
    Nested(Box<PatternElement>)
}

/// A Cypher simple pattern element, e.g., (a)-[r:KNOWS]->(b)
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct SimplePatternElement {

    pub node: NodePattern,
    
    pub chain: Vec<PatternElementChain>,
}

/// A Cypher relationships pattern, e.g., (a)-[r:KNOWS]->(b)-[r2:WORKS_AT]->(c)
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct RelationshipsPattern {
    // The starting node of the pattern
    pub node: NodePattern,
    // One or more chains (relationship + node pairs)
    pub chain: Vec<PatternElementChain>
}

/// A Cypher node pattern, e.g., (n:Person {name: 'Alice'})
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct NodePattern {
    /// Variable to bind to this node, e.g., 'n' in (n:Person)
    pub variable: Option<Ident>,
    /// Labels for this node, e.g., [:Person:Customer] in (n:Person:Customer)
    pub labels: Vec<Ident>,
    /// Properties map for this node, e.g., {name: 'Alice', age: 30}
    pub properties: Option<Expr>,
}

/// A chain of relationship and node in a path pattern
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct PatternElementChain {

    pub relationship: RelationshipPattern,

    pub node: NodePattern,
}

/// A Cypher relationship pattern, e.g., -[r:KNOWS]->
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct RelationshipPattern {
    /// Relationship details
    pub details: RelationshipDetail,
    /// Direction of the left of the relationship
    pub l_direction: Option<RelationshipDirection>,
    /// Direction of the right of the relationship
    pub r_direction: Option<RelationshipDirection>,
}


#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct RelationshipDetail {
    /// Variable to bind to this relationship
    pub variable: Option<Ident>,
    /// Types for this relationship
    pub types: Vec<Ident>,
    /// Properties map for this relationship
    pub properties: Option<Expr>,
    /// Variable length of the relationship, e.g., *1..5
    pub length: Option<RelationshipRange>,
}


/// Direction of a relationship
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum RelationshipDirection {
    /// <-
    Incoming,
    /// ->
    Outgoing,
    /// -
    Undirected,
}

/// Variable length relationship specification
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct RelationshipRange {
    /// Minimum length (None means 1)
    pub min: Option<u64>,
    /// Maximum length (None means unlimited)
    pub max: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct ReturningClause {

    pub body: ProjectionBody,
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct ProjectionBody {

    pub distinct: bool,

    pub projections: Vec<ProjectionItem>,
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum ProjectionItem {

    Expr{ expr: Expr, alias: Option<Ident>},

    All,

    AllFromNode{ node: Ident },
}

impl fmt::Display for NodePattern {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "(")?;
        if let Some(ref var) = self.variable {
            write!(f, "{}", var)?;
        }
        for label in &self.labels {
            write!(f, ":{}", label)?;
        }
        if let Some(ref props) = self.properties {
            write!(f, " {}", props)?;
        }
        write!(f, ")")
    }
}


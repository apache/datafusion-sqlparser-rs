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

use sqlparser::ast::*;
use sqlparser::dialect::CypherDialect;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Tokenizer;

#[test]
fn parse_simple_node_pattern() {
    let sql = "(n:Person)";
    let dialect = CypherDialect {};
    let mut tokenizer = Tokenizer::new(&dialect, sql);
    let tokens = tokenizer.tokenize().unwrap();
    let mut parser = Parser::new(&dialect).with_tokens(tokens);
    
    let node = parser.parse_cypher_node_pattern().unwrap();
    assert_eq!(
        node,
        NodePattern {
            variable: Some(Ident::new("n")),
            labels: vec![Ident::new("Person")],
            properties: None,
        }
    );
}

#[test]
fn parse_cypher_node_pattern_with_properties() {
    let sql = "(n:Person {name: 'Alice', age: 30})";
    let dialect = CypherDialect {};
    let mut tokenizer = Tokenizer::new(&dialect, sql);
    let tokens = tokenizer.tokenize().unwrap();
    let mut parser = Parser::new(&dialect).with_tokens(tokens);
    
    let node = parser.parse_cypher_node_pattern().unwrap();
    assert_eq!(
        node,
        NodePattern {
            variable: Some(Ident::new("n")),
            labels: vec![Ident::new("Person")],
            properties: Some(Expr::Map(Map {
                entries: vec![
                    MapEntry { 
                        key: Box::new(Expr::Identifier(Ident::new("name"))), 
                        value: Box::new(Expr::Value(Value::SingleQuotedString("Alice".to_string()).into())) 
                    },
                    MapEntry { 
                        key: Box::new(Expr::Identifier(Ident::new("age"))), 
                        value: Box::new(Expr::Value(Value::Number("30".to_string(), false).into())) 
                    },
                ],
            })),
        }
    );
}

#[test]
fn parse_cypher_relationship_details() {
    let sql = "[r:KNOWS]"; 
    let dialect = CypherDialect {};
    let mut tokenizer = Tokenizer::new(&dialect, sql);
    let tokens = tokenizer.tokenize().unwrap();
    let mut parser = Parser::new(&dialect).with_tokens(tokens);
    
    let rel = parser.parse_cypher_relationship_details().unwrap();
    assert_eq!(
        rel,
        RelationshipDetail {
            variable: Some(Ident::new("r")),
            types: vec![Ident::new("KNOWS")],
            properties: None,
            length: None,
        }
    );
}

#[test]
fn parse_relationship_with_properties() {
    let sql = "[r:KNOWS {since: 2020}]"; 
    let dialect = CypherDialect {};
    let mut tokenizer = Tokenizer::new(&dialect, sql);
    let tokens = tokenizer.tokenize().unwrap();
    let mut parser = Parser::new(&dialect).with_tokens(tokens);
    
    let rel = parser.parse_cypher_relationship_details().unwrap();
    assert_eq!(
        rel,
        RelationshipDetail {
            variable: Some(Ident::new("r")),
            types: vec![Ident::new("KNOWS")],
            properties: Some(Expr::Map(Map {
                entries: vec![
                    MapEntry { 
                        key: Box::new(Expr::Identifier(Ident::new("since"))), 
                        value: Box::new(Expr::Value(Value::Number("2020".to_string(), false).into())) 
                    },
                ],
            })),
            length: None,
        }
    );
}

#[test]
fn parse_variable_length_relationship() {
    let sql = "[r:KNOWS*2]"; 
    let dialect = CypherDialect {};
    let mut tokenizer = Tokenizer::new(&dialect, sql);
    let tokens = tokenizer.tokenize().unwrap();
    let mut parser = Parser::new(&dialect).with_tokens(tokens);
    
    let rel = parser.parse_cypher_relationship_details().unwrap();
    assert_eq!(
        rel,
        RelationshipDetail {
            variable: Some(Ident::new("r")),
            types: vec![Ident::new("KNOWS")],
            properties: None,
            length: Some(RelationshipRange {
                min: Some(2),
                max: None,
            }),
        }
    );
}

#[test]
fn parse_relationship_with_length_and_properties() {
    let sql = "[r:KNOWS*1..5{since: 2020}]"; 
    let dialect = CypherDialect {};
    let mut tokenizer = Tokenizer::new(&dialect, sql);
    let tokens = tokenizer.tokenize().unwrap();
    let mut parser = Parser::new(&dialect).with_tokens(tokens);
    
    let rel = parser.parse_cypher_relationship_details().unwrap();
    assert_eq!(
        rel,
        RelationshipDetail {
            variable: Some(Ident::new("r")),
            types: vec![Ident::new("KNOWS")],
            properties: Some(Expr::Map(Map {
                entries: vec![
                    MapEntry { 
                        key: Box::new(Expr::Identifier(Ident::new("since"))), 
                        value: Box::new(Expr::Value(Value::Number("2020".to_string(), false).into())) 
                    },
                ],
            })),
            length: Some(RelationshipRange {
                min: Some(1),
                max: Some(5),
            }),
        }
    );
}

#[test]
fn parse_cypher_relationship_pattern_undirected() {
    let sql = "-[r:KNOWS]-"; 
    let dialect = CypherDialect {};
    let mut tokenizer = Tokenizer::new(&dialect, sql);
    let tokens = tokenizer.tokenize().unwrap();
    let mut parser = Parser::new(&dialect).with_tokens(tokens);
    
    let rel = parser.parse_cypher_relationship_pattern().unwrap().unwrap();
    assert_eq!(
        rel,
        RelationshipPattern {
            details: RelationshipDetail {
                variable: Some(Ident::new("r")),
                types: vec![Ident::new("KNOWS")],
                properties: None,
                length: None,
            },
            l_direction: Some(RelationshipDirection::Undirected),
            r_direction: Some(RelationshipDirection::Undirected),
        }
    );
}

#[test]
fn parse_cypher_relationship_pattern_outgoing() {
    let sql = "<-[r:KNOWS]->"; 
    let dialect = CypherDialect {};
    let mut tokenizer = Tokenizer::new(&dialect, sql);
    let tokens = tokenizer.tokenize().unwrap();
    let mut parser = Parser::new(&dialect).with_tokens(tokens);
    
    let rel = parser.parse_cypher_relationship_pattern().unwrap().unwrap();
    assert_eq!(
        rel,
        RelationshipPattern {
            details: RelationshipDetail {
                variable: Some(Ident::new("r")),
                types: vec![Ident::new("KNOWS")],
                properties: None,
                length: None,
            },
            l_direction: Some(RelationshipDirection::Outgoing),
            r_direction: Some(RelationshipDirection::Outgoing),
        }
    );
}

#[test]
fn parse_cypher_relationship_pattern_incoming() {
    let sql = "->[r:KNOWS]<-"; 
    let dialect = CypherDialect {};
    let mut tokenizer = Tokenizer::new(&dialect, sql);
    let tokens = tokenizer.tokenize().unwrap();
    let mut parser = Parser::new(&dialect).with_tokens(tokens);
    
    let rel = parser.parse_cypher_relationship_pattern().unwrap().unwrap();
    assert_eq!(
        rel,
        RelationshipPattern {
            details: RelationshipDetail {
                variable: Some(Ident::new("r")),
                types: vec![Ident::new("KNOWS")],
                properties: None,
                length: None,
            },
            l_direction: Some(RelationshipDirection::Incoming),
            r_direction: Some(RelationshipDirection::Incoming),
        }
    );
}

#[test]
fn parse_cypher_pattern_chain() {
    let sql = "-[r:KNOWS]->(b:Person)<-[s:FRIENDS_WITH]-(c:Person)";
    let dialect = CypherDialect {};
    let mut tokenizer = Tokenizer::new(&dialect, sql);
    let tokens = tokenizer.tokenize().unwrap();
    
    let mut parser = Parser::new(&dialect).with_tokens(tokens);
    
    let pattern_chain = parser.parse_cypher_pattern_chain().unwrap();
    
    assert_eq!(pattern_chain.len(), 2, "Expected 2 chain elements");
    
    let first_chain = &pattern_chain[0];
    assert_eq!(first_chain.relationship.l_direction, Some(RelationshipDirection::Undirected), "First chain element should start with Undirected");
    assert_eq!(first_chain.relationship.r_direction, Some(RelationshipDirection::Outgoing), "First chain element should end with Outgoing");
    
    let second_chain = &pattern_chain[1];
    assert_eq!(second_chain.relationship.l_direction, Some(RelationshipDirection::Outgoing), "Second chain element should start with Outgoing");
    assert_eq!(second_chain.relationship.r_direction, Some(RelationshipDirection::Undirected), "Second chain element should end with Undirected");
}

#[test]
fn parse_full_path_pattern() {
    let sql = "(a:Person)-[r:KNOWS]->(b:Person)<-[s:FRIENDS_WITH]-(c:Person)";
    let dialect = CypherDialect {};
    let mut tokenizer = Tokenizer::new(&dialect, sql);
    let tokens = tokenizer.tokenize().unwrap();
    
    let mut parser = Parser::new(&dialect).with_tokens(tokens);
    
    let path_pattern = parser.parse_cypher_simple_pattern_element().unwrap();
    
    assert_eq!(path_pattern.node.variable, Some(Ident::new("a")), "Start node variable should be 'a'");
    assert_eq!(path_pattern.chain.len(), 2, "Expected 2 chain elements in the path pattern");

    let first_chain = &path_pattern.chain[0];
    assert_eq!(first_chain.relationship.l_direction, Some(RelationshipDirection::Undirected), "First chain element should start with Undirected");
    assert_eq!(first_chain.relationship.r_direction, Some(RelationshipDirection::Outgoing), "First chain element should end with Outgoing");
    
    let second_chain = &path_pattern.chain[1];
    assert_eq!(second_chain.relationship.l_direction, Some(RelationshipDirection::Outgoing), "Second chain element should start with Outgoing");
    assert_eq!(second_chain.relationship.r_direction, Some(RelationshipDirection::Undirected), "Second chain element should end with Undirected");
}

#[test]
fn parse_cypher_simple_pattern_element() {
    let sql = "(a:Person)-[r:KNOWS]->(b:Person)";
    let dialect = CypherDialect {};
    let mut tokenizer = Tokenizer::new(&dialect, sql);
    let tokens = tokenizer.tokenize().unwrap();
    
    let mut parser = Parser::new(&dialect).with_tokens(tokens);
    
    let path_pattern = parser.parse_cypher_simple_pattern_element().unwrap();
    
    assert_eq!(path_pattern.node.variable, Some(Ident::new("a")), "Start node variable should be 'a'");
    assert_eq!(path_pattern.chain.len(), 1, "Expected 1 chain element in the path pattern");

    let first_chain = &path_pattern.chain[0];
    assert_eq!(first_chain.relationship.l_direction, Some(RelationshipDirection::Undirected), "Chain element should start with Undirected");
    assert_eq!(first_chain.relationship.r_direction, Some(RelationshipDirection::Outgoing), "Chain element should end with Outgoing");
}

#[test]
fn parse_nested_pattern_element() {
    let sql = "((a:Person)-[r:KNOWS]->(b:Person))";
    let dialect = CypherDialect {};
    let mut tokenizer = Tokenizer::new(&dialect, sql);
    let tokens = tokenizer.tokenize().unwrap();
    
    let mut parser = Parser::new(&dialect).with_tokens(tokens);
    
    let nested_pattern = parser.parse_cypher_pattern_element().unwrap();
    
    match nested_pattern {
        PatternElement::Nested(boxed_element) => {
            match *boxed_element {
                PatternElement::Simple(simple_element) => {
                    assert_eq!(simple_element.node.variable, Some(Ident::new("a")), "Start node variable should be 'a'");
                    assert_eq!(simple_element.chain.len(), 1, "Expected 1 chain element in the nested pattern");

                    let first_chain = &simple_element.chain[0];
                    assert_eq!(first_chain.relationship.l_direction, Some(RelationshipDirection::Undirected), "Chain element should start with Undirected");
                    assert_eq!(first_chain.relationship.r_direction, Some(RelationshipDirection::Outgoing), "Chain element should end with Outgoing");
                },
                _ => panic!("Expected a SimplePatternElement inside the nested pattern"),
            }
        },
        _ => panic!("Expected a Nested PatternElement"),
    }
}

#[test]
fn parse_pattern_part_with_variable(){
    let sql = "v = (a:Person)-[r:KNOWS]->(b:Person)";
    let dialect = CypherDialect {};
    let mut tokenizer = Tokenizer::new(&dialect, sql);
    let tokens = tokenizer.tokenize().unwrap();
    
    let mut parser = Parser::new(&dialect).with_tokens(tokens);
    
    let pattern_part = parser.parse_cypher_pattern_part().unwrap();

    assert_eq!(pattern_part.variable, Some(Ident::new("v")), "Pattern part variable should be 'v'");

    match pattern_part.anon_pattern_part {
        PatternElement::Simple(simple_element) => {
            assert_eq!(simple_element.node.variable, Some(Ident::new("a")), "Start node variable should be 'a'");
            assert_eq!(simple_element.chain.len(), 1, "Expected 1 chain element in the pattern part");

            let first_chain = &simple_element.chain[0];
            assert_eq!(first_chain.relationship.l_direction, Some(RelationshipDirection::Undirected), "Chain element should start with Undirected");
            assert_eq!(first_chain.relationship.r_direction, Some(RelationshipDirection::Outgoing), "Chain element should end with Outgoing");
        },
        _ => panic!("Expected a SimplePatternElement in the PatternPart"),
    }
}

#[test]
fn parse_pattern_part_no_variable(){
    let sql = "(a:Person)-[r:KNOWS]->(b:Person)";
    let dialect = CypherDialect {};
    let mut tokenizer = Tokenizer::new(&dialect, sql);
    let tokens = tokenizer.tokenize().unwrap();
    
    let mut parser = Parser::new(&dialect).with_tokens(tokens);
    
    let pattern_part = parser.parse_cypher_pattern_part().unwrap();

    assert_eq!(pattern_part.variable, None, "Pattern part variable should be None");

    match pattern_part.anon_pattern_part {
        PatternElement::Simple(simple_element) => {
            assert_eq!(simple_element.node.variable, Some(Ident::new("a")), "Start node variable should be 'a'");
            assert_eq!(simple_element.chain.len(), 1, "Expected 1 chain element in the pattern part");

            let first_chain = &simple_element.chain[0];
            assert_eq!(first_chain.relationship.l_direction, Some(RelationshipDirection::Undirected), "Chain element should start with Undirected");
            assert_eq!(first_chain.relationship.r_direction, Some(RelationshipDirection::Outgoing), "Chain element should end with Outgoing");
        },
        _ => panic!("Expected a SimplePatternElement in the PatternPart"),
    }
}

#[test]
fn parse_full_pattern() {
    let sql = "v = (a:Person)-[r:KNOWS]->(b:Person)<-[s:FRIENDS_WITH]-(c:Person)";
    let dialect = CypherDialect {};
    let mut tokenizer = Tokenizer::new(&dialect, sql);
    let tokens = tokenizer.tokenize().unwrap();
    
    let mut parser = Parser::new(&dialect).with_tokens(tokens);
    
    let pattern = parser.parse_cypher_pattern().unwrap();
    
    assert_eq!(pattern.parts.len(), 1, "Expected 1 pattern part in the full pattern");

    let pattern_part = &pattern.parts[0];
    assert_eq!(pattern_part.variable, Some(Ident::new("v")), "Pattern part variable should be 'v'");

    match &pattern_part.anon_pattern_part {
        PatternElement::Simple(simple_element) => {
            assert_eq!(simple_element.node.variable, Some(Ident::new("a")), "Start node variable should be 'a'");
            assert_eq!(simple_element.chain.len(), 2, "Expected 2 chain elements in the pattern part");

            let first_chain = &simple_element.chain[0];
            assert_eq!(first_chain.relationship.l_direction, Some(RelationshipDirection::Undirected), "First chain element should start with Undirected");
            assert_eq!(first_chain.relationship.r_direction, Some(RelationshipDirection::Outgoing), "First chain element should end with Outgoing");

            let second_chain = &simple_element.chain[1];
            assert_eq!(second_chain.relationship.l_direction, Some(RelationshipDirection::Outgoing), "Second chain element should start with Outgoing");
            assert_eq!(second_chain.relationship.r_direction, Some(RelationshipDirection::Undirected), "Second chain element should end with Undirected");
        },
        _ => panic!("Expected a SimplePatternElement in the PatternPart"),
    }
}

#[test]
fn parse_multiple_pattern_parts() {
    let sql = "v1 = (a:Person)-[r:KNOWS]->(b:Person), v2 = (c:Person)-[s:FRIENDS_WITH]->(d:Person)";
    let dialect = CypherDialect {};
    let mut tokenizer = Tokenizer::new(&dialect, sql);
    let tokens = tokenizer.tokenize().unwrap();
    
    let mut parser = Parser::new(&dialect).with_tokens(tokens);
    
    let pattern = parser.parse_cypher_pattern().unwrap();
    
    assert_eq!(pattern.parts.len(), 2, "Expected 2 pattern parts in the full pattern");

    let first_part = &pattern.parts[0];
    assert_eq!(first_part.variable, Some(Ident::new("v1")), "First pattern part variable should be 'v1'");

    let second_part = &pattern.parts[1];
    assert_eq!(second_part.variable, Some(Ident::new("v2")), "Second pattern part variable should be 'v2'");
}

#[test]
fn parse_pattern_with_nested_elements() {
    let sql = "v = ((a:Person)-[r:KNOWS]->(b:Person))<-[:FRIENDS_WITH]-(c:Person)";
    let dialect = CypherDialect {};
    let mut tokenizer = Tokenizer::new(&dialect, sql);
    let tokens = tokenizer.tokenize().unwrap();
    
    let mut parser = Parser::new(&dialect).with_tokens(tokens);
    
    let pattern = parser.parse_cypher_pattern().unwrap();
    
    assert_eq!(pattern.parts.len(), 1, "Expected 1 pattern part in the full pattern");

    let pattern_part = &pattern.parts[0];
    assert_eq!(pattern_part.variable, Some(Ident::new("v")), "Pattern part variable should be 'v'");

    match &pattern_part.anon_pattern_part {
        PatternElement::Nested(boxed_element) => {
            match &**boxed_element {
                PatternElement::Simple(simple_element) => {
                    assert_eq!(simple_element.node.variable, Some(Ident::new("a")), "Start node variable should be 'a'");
                    assert_eq!(simple_element.chain.len(), 1, "Expected 1 chain element in the nested simple pattern");

                    let first_chain = &simple_element.chain[0];
                    assert_eq!(first_chain.relationship.l_direction, Some(RelationshipDirection::Undirected), "Chain element should start with Undirected");
                    assert_eq!(first_chain.relationship.r_direction, Some(RelationshipDirection::Outgoing), "Chain element should end with Outgoing");
                },
                _ => panic!("Expected a SimplePatternElement inside the nested pattern"),
            }
        },
        _ => panic!("Expected a Nested PatternElement in the PatternPart"),
    }
}

#[test]
fn parse_match_clause() {
    let sql = "MATCH (a:Person)-[r:KNOWS]->(b:Person)";
    let dialect = CypherDialect {};
    let mut tokenizer = Tokenizer::new(&dialect, sql);
    let tokens = tokenizer.tokenize().unwrap();
    
    let mut parser = Parser::new(&dialect).with_tokens(tokens);
    
    let match_clause = parser.parse_cypher_match_clause().unwrap();

    match match_clause {
        CypherReadingClause::Match(match_clause) => {
    
            assert_eq!(match_clause.pattern.parts.len(), 1, "Expected 1 pattern part in the MATCH clause");

            let pattern_part = &match_clause.pattern.parts[0];
            match &pattern_part.anon_pattern_part {
                PatternElement::Simple(simple_element) => {
                    assert_eq!(simple_element.node.variable, Some(Ident::new("a")), "Start node variable should be 'a'");
                    assert_eq!(simple_element.chain.len(), 1, "Expected 1 chain element in the pattern part");

                    let first_chain = &simple_element.chain[0];
                    assert_eq!(first_chain.relationship.l_direction, Some(RelationshipDirection::Undirected), "Chain element should start with Undirected");
                    assert_eq!(first_chain.relationship.r_direction, Some(RelationshipDirection::Outgoing), "Chain element should end with Outgoing");
                },
                _ => panic!("Expected a SimplePatternElement in the PatternPart"),
            }
        },
    }
}

#[test]
fn parse_optional_match_clause() {
    let sql = "OPTIONAL MATCH (a:Person)-[r:KNOWS]->(b:Person)";
    let dialect = CypherDialect {};
    let mut tokenizer = Tokenizer::new(&dialect, sql);
    let tokens = tokenizer.tokenize().unwrap();
    
    let mut parser = Parser::new(&dialect).with_tokens(tokens);
    
    let match_clause = parser.parse_cypher_match_clause().unwrap();

    match match_clause {
        CypherReadingClause::Match(match_clause) => {
            assert!(match_clause.optional, "MATCH clause should be optional");
    
            assert_eq!(match_clause.pattern.parts.len(), 1, "Expected 1 pattern part in the MATCH clause");

            let pattern_part = &match_clause.pattern.parts[0];
            match &pattern_part.anon_pattern_part {
                PatternElement::Simple(simple_element) => {
                    assert_eq!(simple_element.node.variable, Some(Ident::new("a")), "Start node variable should be 'a'");
                    assert_eq!(simple_element.chain.len(), 1, "Expected 1 chain element in the pattern part");

                    let first_chain = &simple_element.chain[0];
                    assert_eq!(first_chain.relationship.l_direction, Some(RelationshipDirection::Undirected), "Chain element should start with Undirected");
                    assert_eq!(first_chain.relationship.r_direction, Some(RelationshipDirection::Outgoing), "Chain element should end with Outgoing");
                },
                _ => panic!("Expected a SimplePatternElement in the PatternPart"),
            }
        },
    }
}

#[test]
fn parse_multiple_match_clauses() {
    let sql = "MATCH (a:Person)-[r:KNOWS]->(b:Person) OPTIONAL MATCH (c:Person)-[s:FRIENDS_WITH]->(d:Person)";
    let dialect = CypherDialect {};
    let mut tokenizer = Tokenizer::new(&dialect, sql);
    let tokens = tokenizer.tokenize().unwrap();
    
    let mut parser = Parser::new(&dialect).with_tokens(tokens);
    
    let first_match_clause = parser.parse_cypher_match_clause().unwrap();
    let second_match_clause = parser.parse_cypher_match_clause().unwrap();

    match first_match_clause {
        CypherReadingClause::Match(match_clause) => {
            assert!(!match_clause.optional, "First MATCH clause should not be optional");
    
            assert_eq!(match_clause.pattern.parts.len(), 1, "Expected 1 pattern part in the first MATCH clause");

            let pattern_part = &match_clause.pattern.parts[0];
            match &pattern_part.anon_pattern_part {
                PatternElement::Simple(simple_element) => {
                    assert_eq!(simple_element.node.variable, Some(Ident::new("a")), "Start node variable should be 'a'");
                    assert_eq!(simple_element.chain.len(), 1, "Expected 1 chain element in the pattern part");

                    let first_chain = &simple_element.chain[0];
                    assert_eq!(first_chain.relationship.l_direction, Some(RelationshipDirection::Undirected), "Chain element should start with Undirected");
                    assert_eq!(first_chain.relationship.r_direction, Some(RelationshipDirection::Outgoing), "Chain element should end with Outgoing");
                },
                _ => panic!("Expected a SimplePatternElement in the PatternPart"),
            }
        },
    }

    match second_match_clause {
        CypherReadingClause::Match(match_clause) => {
            assert!(match_clause.optional, "Second MATCH clause should be optional");
    
            assert_eq!(match_clause.pattern.parts.len(), 1, "Expected 1 pattern part in the second MATCH clause");

            let pattern_part = &match_clause.pattern.parts[0];
            match &pattern_part.anon_pattern_part {
                PatternElement::Simple(simple_element) => {
                    assert_eq!(simple_element.node.variable, Some(Ident::new("c")), "Start node variable should be 'c'");
                    assert_eq!(simple_element.chain.len(), 1, "Expected 1 chain element in the pattern part");
                    let first_chain = &simple_element.chain[0];
                    assert_eq!(first_chain.relationship.l_direction, Some(RelationshipDirection::Undirected), "Chain element should start with Undirected");
                    assert_eq!(first_chain.relationship.r_direction, Some(RelationshipDirection::Outgoing), "Chain element should end with Outgoing");
                },
                _ => panic!("Expected a SimplePatternElement in the PatternPart"),
            }
        },
    }
}

#[test]
fn parse_projection_body() {
    let sql = "p.name, p.age";
    let dialect = CypherDialect {};
    let mut tokenizer = Tokenizer::new(&dialect, sql);
    let tokens = tokenizer.tokenize().unwrap();
    
    let mut parser = Parser::new(&dialect).with_tokens(tokens);
    
    let projection_body = parser.parse_cypher_projection_body().unwrap();
    
    assert_eq!(projection_body.projections.len(), 2, "Expected 2 entries in the projection body");

    assert_eq!(
        projection_body.projections[0],
        ProjectionItem::Expr {
            expr: Expr::CompoundIdentifier(vec![
                Ident::new("p"),
                Ident::new("name"),
            ]),
            alias: None,
        },
        "First projection item should be 'p.name'"
    );
}

#[test]
fn parse_projection_body_with_aliases() {
    let sql = "p.name AS personName, p.age AS personAge";
    let dialect = CypherDialect {};
    let mut tokenizer = Tokenizer::new(&dialect, sql);
    let tokens = tokenizer.tokenize().unwrap();
    
    let mut parser = Parser::new(&dialect).with_tokens(tokens);
    
    let projection_body = parser.parse_cypher_projection_body().unwrap();
    
    assert_eq!(projection_body.projections.len(), 2, "Expected 2 entries in the projection body");

    assert_eq!(
        projection_body.projections[0],
        ProjectionItem::Expr {
            expr: Expr::CompoundIdentifier(vec![
                Ident::new("p"),
                Ident::new("name"),
            ]),
            alias: Some(Ident::new("personName")),
        },
        "First projection item should be 'p.name AS personName'"
    );

    assert_eq!(
        projection_body.projections[1],
        ProjectionItem::Expr {
            expr: Expr::CompoundIdentifier(vec![
                Ident::new("p"),
                Ident::new("age"),
            ]),
            alias: Some(Ident::new("personAge")),
        },
        "Second projection item should be 'p.age AS personAge'"
    );
}

#[test]
fn parse_where_clause() {
    let sql = "WHERE p.age > 30 AND p.name = 'Alice'";
    let dialect = CypherDialect {};
    let mut tokenizer = Tokenizer::new(&dialect, sql);
    let tokens = tokenizer.tokenize().unwrap();
    
    let mut parser = Parser::new(&dialect).with_tokens(tokens);
    
    let where_clause = parser.parse_cypher_where_clause().unwrap();
    
    assert_eq!(
        where_clause.expr,
        Expr::BinaryOp {
            left: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::CompoundIdentifier(vec![
                    Ident::new("p"),
                    Ident::new("age"),
                ])),
                op: BinaryOperator::Gt,
                right: Box::new(Expr::Value(Value::Number("30".to_string(), false).into())),
            }),
            op: BinaryOperator::And,
            right: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::CompoundIdentifier(vec![
                    Ident::new("p"),
                    Ident::new("name"),
                ])),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::Value(Value::SingleQuotedString("Alice".to_string()).into())),
            }),
        },
        "WHERE clause expression did not match expected structure"
    );
}

#[test]
fn parse_returning_clause() {
    let sql = "RETURN DISTINCT p.name AS personName, p.age AS personAge";
    let dialect = CypherDialect {};
    let mut tokenizer = Tokenizer::new(&dialect, sql);
    let tokens = tokenizer.tokenize().unwrap();
    
    let mut parser = Parser::new(&dialect).with_tokens(tokens);
    
    let returning_clause = parser.parse_cypher_return().unwrap();
    
    assert!(returning_clause.body.distinct, "RETURNING clause should be DISTINCT");
    assert_eq!(returning_clause.body.projections.len(), 2, "Expected 2 entries in the RETURNING clause");

    assert_eq!(
        returning_clause.body.projections[0],
        ProjectionItem::Expr {
            expr: Expr::CompoundIdentifier(vec![
                Ident::new("p"),
                Ident::new("name"),
            ]),
            alias: Some(Ident::new("personName")),
        },
        "First projection item should be 'p.name AS personName'"
    );

    assert_eq!(
        returning_clause.body.projections[1],
        ProjectionItem::Expr {
            expr: Expr::CompoundIdentifier(vec![
                Ident::new("p"),
                Ident::new("age"),
            ]),
            alias: Some(Ident::new("personAge")),
        },
        "Second projection item should be 'p.age AS personAge'"
    );
}

#[test]
fn parse_returning_clause_all() {
    let sql = "RETURN *";
    let dialect = CypherDialect {};
    let mut tokenizer = Tokenizer::new(&dialect, sql);
    let tokens = tokenizer.tokenize().unwrap();
    
    let mut parser = Parser::new(&dialect).with_tokens(tokens);
    
    let returning_clause = parser.parse_cypher_return().unwrap();
    
    assert!(!returning_clause.body.distinct, "RETURNING clause should not be DISTINCT");
    assert_eq!(returning_clause.body.projections.len(), 1, "Expected 1 entry in the RETURNING clause");

    assert_eq!(
        returning_clause.body.projections[0],
        ProjectionItem::All,
        "Projection item should be ALL (*)"
    );
}

#[test]
fn parse_create_single_part_query() {
    let sql = "CREATE (a:Person {name: 'Alice', age: 30})";
    let dialect = CypherDialect {};
    let mut tokenizer = Tokenizer::new(&dialect, sql);
    let tokens = tokenizer.tokenize().unwrap();
    
    let mut parser = Parser::new(&dialect).with_tokens(tokens);

    let single_part_query = parser.parse_cypher_single_part_query().unwrap();

    match single_part_query {
        SinglePartQuery::Updating(create_query) => {
            let create_clause = create_query.create_clause;
            assert_eq!(create_clause.pattern.parts.len(), 1, "Expected 1 pattern part in the CREATE clause");

            let pattern_part = &create_clause.pattern.parts[0];
            match &pattern_part.anon_pattern_part {
                PatternElement::Simple(simple_element) => {
                    assert_eq!(simple_element.node.variable, Some(Ident::new("a")), "Node variable should be 'a'");
                    assert_eq!(simple_element.node.labels, vec![Ident::new("Person")], "Node label should be 'Person'");

                    match &simple_element.node.properties {
                        Some(Expr::Map(map)) => {
                            assert_eq!(map.entries.len(), 2, "Expected 2 properties in the node properties");

                            let name_entry = &map.entries[0];
                            assert_eq!(
                                *name_entry.key,
                                Expr::Identifier(Ident::new("name")),
                                "First property key should be 'name'"
                            );
                            assert_eq!(
                                *name_entry.value,
                                Expr::Value(Value::SingleQuotedString("Alice".to_string()).into()),
                                "First property value should be 'Alice'"
                            );

                            let age_entry = &map.entries[1];
                            assert_eq!(
                                *age_entry.key,
                                Expr::Identifier(Ident::new("age")),
                                "Second property key should be 'age'"
                            );
                            assert_eq!(
                                *age_entry.value,
                                Expr::Value(Value::Number("30".to_string(), false).into()),
                                "Second property value should be 30"
                            );
                        },
                        _ => panic!("Expected node properties to be a Map expression"),
                    }
                },
                _ => panic!("Expected a SimplePatternElement in the PatternPart"),
            }
        },
        SinglePartQuery::Reading(_) => {
            panic!("Expected a Updating query, got Reading");
        }
    }
}

#[test]
fn parse_match_with_returning() {
    let sql = "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a.name AS personName, b.name AS friendName";
    let dialect = CypherDialect {};
    let mut tokenizer = Tokenizer::new(&dialect, sql);
    let tokens = tokenizer.tokenize().unwrap();
    
    let mut parser = Parser::new(&dialect).with_tokens(tokens);

    let single_part_query = parser.parse_cypher_single_part_query().unwrap();

    match single_part_query {
        SinglePartQuery::Reading(simple_query) => {
            let match_clause = simple_query.reading_clause;
            match match_clause {
                CypherReadingClause::Match(match_clause) => {
                    assert_eq!(match_clause.pattern.parts.len(), 1, "Expected 1 pattern part in the MATCH clause");

                    let pattern_part = &match_clause.pattern.parts[0];
                    match &pattern_part.anon_pattern_part {
                        PatternElement::Simple(simple_element) => {
                            assert_eq!(simple_element.node.variable, Some(Ident::new("a")), "Start node variable should be 'a'");
                            assert_eq!(simple_element.chain.len(), 1, "Expected 1 chain element in the pattern part");

                            let first_chain = &simple_element.chain[0];
                            assert_eq!(first_chain.relationship.l_direction, Some(RelationshipDirection::Undirected), "Chain element should start with Undirected");
                            assert_eq!(first_chain.relationship.r_direction, Some(RelationshipDirection::Outgoing), "Chain element should end with Outgoing");
                        },
                        _ => panic!("Expected a SimplePatternElement in the PatternPart"),
                    }
                },
            }
            let returning_clause = simple_query.returning_clause;
            assert_eq!(returning_clause.body.projections.len(), 2, "Expected 2 entries in the RETURNING clause");

            assert_eq!(
                returning_clause.body.projections[0],
                ProjectionItem::Expr {
                    expr: Expr::CompoundIdentifier(vec![
                        Ident::new("a"),
                        Ident::new("name"),
                    ]),
                    alias: Some(Ident::new("personName")),
                },
                "First projection item should be 'a.name AS personName'"
            );
        },
        SinglePartQuery::Updating(_) => {
            panic!("Expected a Reading query, got Writing");
        }
    }
}

#[test]
fn parse_match_with_where(){
    let sql = "MATCH (a:Person)-[r:KNOWS]->(b:Person) WHERE a.age > 30 RETURN a.name AS personName, b.name AS friendName";
    let dialect = CypherDialect {};
    let mut tokenizer = Tokenizer::new(&dialect, sql);
    let tokens = tokenizer.tokenize().unwrap();
    
    let mut parser = Parser::new(&dialect).with_tokens(tokens);

    let single_part_query = parser.parse_cypher_single_part_query().unwrap();

    match single_part_query {
        SinglePartQuery::Reading(simple_query) => {
            let match_clause = simple_query.reading_clause;
            match match_clause {
                CypherReadingClause::Match(match_clause) => {
                    assert_eq!(match_clause.pattern.parts.len(), 1, "Expected 1 pattern part in the MATCH clause");

                    let pattern_part = &match_clause.pattern.parts[0];
                    match &pattern_part.anon_pattern_part {
                        PatternElement::Simple(simple_element) => {
                            assert_eq!(simple_element.node.variable, Some(Ident::new("a")), "Start node variable should be 'a'");
                            assert_eq!(simple_element.chain.len(), 1, "Expected 1 chain element in the pattern part");

                            let first_chain = &simple_element.chain[0];
                            assert_eq!(first_chain.relationship.l_direction, Some(RelationshipDirection::Undirected), "Chain element should start with Undirected");
                            assert_eq!(first_chain.relationship.r_direction, Some(RelationshipDirection::Outgoing), "Chain element should end with Outgoing");
                        },
                        _ => panic!("Expected a SimplePatternElement in the PatternPart"),
                    }
                    let where_clause = match_clause.where_clause.unwrap();
                    assert_eq!(where_clause.expr,
                        Expr::BinaryOp {
                            left: Box::new(Expr::CompoundIdentifier(vec![
                                Ident::new("a"),
                                Ident::new("age"),
                            ])),
                            op: BinaryOperator::Gt,
                            right: Box::new(Expr::Value(Value::Number("30".to_string(), false).into())),
                        },
                        "WHERE clause expression did not match expected structure"
                    );
                },
            }
        },
        SinglePartQuery::Updating(_) => {
            panic!("Expected a Reading query, got Writing");
        }
    }
 }

#[test]
fn parse_cypher_create(){
    let sql = "CREATE (a:Person {name: 'Alice', age: 30})";
    let dialect = CypherDialect {};
    let mut tokenizer = Tokenizer::new(&dialect, sql);
    let tokens = tokenizer.tokenize().unwrap();
    
    let mut parser = Parser::new(&dialect).with_tokens(tokens);

    let create_clause = parser.parse_cypher_create_clause().unwrap();

    assert_eq!(create_clause.pattern.parts.len(), 1, "Expected 1 pattern part in the CREATE clause");

    let pattern_part = &create_clause.pattern.parts[0];
    match &pattern_part.anon_pattern_part {
        PatternElement::Simple(simple_element) => {
            assert_eq!(simple_element.node.variable, Some(Ident::new("a")), "Node variable should be 'a'");
            assert_eq!(simple_element.node.labels, vec![Ident::new("Person")], "Node label should be 'Person'");

            match &simple_element.node.properties {
                Some(Expr::Map(map)) => {
                    assert_eq!(map.entries.len(), 2, "Expected 2 properties in the node properties");

                    let name_entry = &map.entries[0];
                    assert_eq!(
                        *name_entry.key,
                        Expr::Identifier(Ident::new("name")),
                        "First property key should be 'name'"
                    );
                    assert_eq!(
                        *name_entry.value,
                        Expr::Value(Value::SingleQuotedString("Alice".to_string()).into()),
                        "First property value should be 'Alice'"
                    );

                    let age_entry = &map.entries[1];
                    assert_eq!(
                        *age_entry.key,
                        Expr::Identifier(Ident::new("age")),
                        "Second property key should be 'age'"
                    );
                    assert_eq!(
                        *age_entry.value,
                        Expr::Value(Value::Number("30".to_string(), false).into()),
                        "Second property value should be 30"
                    );
                },
                _ => panic!("Expected node properties to be a Map expression"),
            }
        },
        _ => panic!("Expected a SimplePatternElement in the PatternPart"),
    }
}

#[test]
fn parse_cypher_create_nodes_only(){
    let cypher = "CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), (c:Person {name: 'Carol'})";
    let dialect = CypherDialect {};

    match Parser::parse_sql(&dialect, cypher) {
        Ok(ast) => {
            // Convert each statement back to a string
            let sql: String = ast.into_iter().map(|stmt| stmt.to_string()).collect::<Vec<String>>().join(", ");

            let expected_sql = "INSERT INTO nodes (Label, Properties) VALUES ('Person', '{\"name\": \"Alice\"}'), ('Person', '{\"name\": \"Bob\"}'), ('Person', '{\"name\": \"Carol\"}')";
            assert_eq!(sql, expected_sql, "Desugared SQL did not match expected output");
        }
        _ => panic!("Parsing failed"),
    };
}

#[test]
fn parse_cypher_create_with_relationship(){
    let cypher = "CREATE (a:Person {name: 'Alice', age: 30})-[:KNOWS {since: 2020}]->(b:Person {name: 'Bob', age: 28})";
    let dialect = CypherDialect {};

    match Parser::parse_sql(&dialect, cypher) {
        Ok(ast) => {
            // Convert each statement back to a string
            let sql: String = ast.into_iter().map(|stmt| stmt.to_string()).collect::<Vec<String>>().join(", ");

            let expected_sql = "WITH \
                node1 AS \
                (INSERT INTO nodes (Label, Properties) VALUES ('Person', '{\"name\": \"Alice\", \"age\": 30}') RETURNING id), \
                node2 AS \
                (INSERT INTO nodes (Label, Properties) VALUES ('Person', '{\"name\": \"Bob\", \"age\": 28}') RETURNING id) \
                INSERT INTO edges (Label, Source_id, Target_id, Properties) \
                SELECT 'KNOWS', node1.id, node2.id, '{\"since\": 2020}' \
                FROM node1, node2";
            assert_eq!(sql, expected_sql, "Desugared SQL did not match expected output");
        }
        _ => panic!("Parsing failed"),
    };
}

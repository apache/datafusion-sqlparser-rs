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
    
    let node = parser.parse_node_pattern().unwrap();
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
fn parse_node_pattern_with_properties() {
    let sql = "(n:Person {name: 'Alice', age: 30})";
    let dialect = CypherDialect {};
    let mut tokenizer = Tokenizer::new(&dialect, sql);
    let tokens = tokenizer.tokenize().unwrap();
    let mut parser = Parser::new(&dialect).with_tokens(tokens);
    
    let node = parser.parse_node_pattern().unwrap();
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
fn parse_relationship_details() {
    let sql = "[r:KNOWS]"; 
    let dialect = CypherDialect {};
    let mut tokenizer = Tokenizer::new(&dialect, sql);
    let tokens = tokenizer.tokenize().unwrap();
    let mut parser = Parser::new(&dialect).with_tokens(tokens);
    
    let rel = parser.parse_relationship_details().unwrap();
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
    
    let rel = parser.parse_relationship_details().unwrap();
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
    
    let rel = parser.parse_relationship_details().unwrap();
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
    
    let rel = parser.parse_relationship_details().unwrap();
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
fn parse_relationship_pattern_undirected() {
    let sql = "-[r:KNOWS]-"; 
    let dialect = CypherDialect {};
    let mut tokenizer = Tokenizer::new(&dialect, sql);
    let tokens = tokenizer.tokenize().unwrap();
    let mut parser = Parser::new(&dialect).with_tokens(tokens);
    
    let rel = parser.parse_relationship_pattern().unwrap().unwrap();
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
fn parse_relationship_pattern_outgoing() {
    let sql = "<-[r:KNOWS]->"; 
    let dialect = CypherDialect {};
    let mut tokenizer = Tokenizer::new(&dialect, sql);
    let tokens = tokenizer.tokenize().unwrap();
    let mut parser = Parser::new(&dialect).with_tokens(tokens);
    
    let rel = parser.parse_relationship_pattern().unwrap().unwrap();
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
fn parse_relationship_pattern_incoming() {
    let sql = "->[r:KNOWS]<-"; 
    let dialect = CypherDialect {};
    let mut tokenizer = Tokenizer::new(&dialect, sql);
    let tokens = tokenizer.tokenize().unwrap();
    let mut parser = Parser::new(&dialect).with_tokens(tokens);
    
    let rel = parser.parse_relationship_pattern().unwrap().unwrap();
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
fn parse_pattern_chain() {
    let sql = "-[r:KNOWS]->(b:Person)<-[s:FRIENDS_WITH]-(c:Person)";
    let dialect = CypherDialect {};
    let mut tokenizer = Tokenizer::new(&dialect, sql);
    let tokens = tokenizer.tokenize().unwrap();
    
    let mut parser = Parser::new(&dialect).with_tokens(tokens);
    
    let pattern_chain = parser.parse_pattern_chain().unwrap();
    
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
    
    let path_pattern = parser.parse_simple_pattern_element().unwrap();
    
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
fn parse_simple_pattern_element() {
    let sql = "(a:Person)-[r:KNOWS]->(b:Person)";
    let dialect = CypherDialect {};
    let mut tokenizer = Tokenizer::new(&dialect, sql);
    let tokens = tokenizer.tokenize().unwrap();
    
    let mut parser = Parser::new(&dialect).with_tokens(tokens);
    
    let path_pattern = parser.parse_simple_pattern_element().unwrap();
    
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
    
    let nested_pattern = parser.parse_pattern_element().unwrap();
    
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
    
    let pattern_part = parser.parse_pattern_part().unwrap();

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
    
    let pattern_part = parser.parse_pattern_part().unwrap();

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
    
    let match_clause = parser.parse_match_clause().unwrap();

    match match_clause {
        ReadingClause::Match(match_clause) => {
    
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
        _ => panic!("Expected a MatchClause"),
    }
}
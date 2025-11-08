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

// #[test]
// fn parse_simple_path_pattern() {
//     let sql = "(a:Person)-[r:KNOWS]->(b:Person)";
//     let dialect = GenericDialect {};
//     let mut tokenizer = Tokenizer::new(&dialect, sql);
//     let tokens = tokenizer.tokenize().unwrap();
//     let mut parser = Parser::new(&dialect).with_tokens(tokens);
    
//     let pattern = parser.parse_pattern_element().unwrap();
//     // Once we implement the full pattern parsing, we would assert the complete structure here
// }

// #[test]
// fn parse_variable_length_relationship() {
//     let sql = "-[r:KNOWS*2..5]->";
//     let dialect = GenericDialect {};
//     let mut tokenizer = Tokenizer::new(&dialect, sql);
//     let tokens = tokenizer.tokenize().unwrap();
//     let mut parser = Parser::new(&dialect).with_tokens(tokens);
    
//     let rel = parser.parse_relationship_pattern().unwrap();
//     assert_eq!(
//         rel,
//         RelationshipPattern {
//             variable: Some(Ident::new("r")),
//             types: vec![Ident::new("KNOWS")],
//             properties: None,
//             direction: RelationshipDirection::Outgoing,
//             length: Some(RelationshipRange {
//                 min: Some(2),
//                 max: Some(5)
//             }),
//         }
//     );
// }

// #[test]
// fn parse_match_clause() {
//     let sql = "MATCH (a:Person)-[r:KNOWS]->(b:Person) WHERE a.age > 30";
//     let dialect = GenericDialect {};
//     let mut tokenizer = Tokenizer::new(&dialect, sql);
//     let tokens = tokenizer.tokenize().unwrap();
//     let mut parser = Parser::new(&dialect).with_tokens(tokens);
    
//     let _match_clause = parser.parse_match().unwrap();
//     // We would assert the match clause structure once implemented
// }
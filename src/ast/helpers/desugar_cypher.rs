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

//! Cypher query desugaring utilities
//! 
//! Functions to transform Cypher CREATE patterns into SQL INSERT statements

use crate::ast::*;
use crate::ast::helpers::attached_token::AttachedToken;
use crate::tokenizer::TokenWithSpan;
use crate::parser::{ParserError};

pub struct Desugarer;

impl Desugarer {

    /// Desugar Cypher property map into JSON string format for INSERT statements
    fn cypher_properties_to_string(properties: Map) -> Result<String, ParserError>{
        let entries: Vec<String> = properties.entries.into_iter()
                            .map(|kv| {
                                // Format key as JSON string
                                let key_str = match *kv.key {
                                    Expr::Identifier(id) => format!("\"{}\"", id.value),
                                    _ => format!("\"{}\"", kv.key),
                                };
                                
                                // Format value as JSON
                                let value_str = match *kv.value {
                                    Expr::Value(val_with_span) => {
                                        match val_with_span.value {
                                            Value::SingleQuotedString(s) => format!("\"{}\"", s),
                                            Value::DoubleQuotedString(s) => format!("\"{}\"", s),
                                            Value::Number(n, _) => n.to_string(),
                                            Value::Boolean(b) => b.to_string().to_lowercase(),
                                            Value::Null => "null".to_string(),
                                            _ => format!("\"{}\"", val_with_span),
                                        }
                                    }
                                    _ => format!("\"{}\"", kv.value),
                                };
                                
                                format!("{}: {}", key_str, value_str)
                            })
                            .collect();
        Ok(format!("{{{}}}", entries.join(", ")))
    }

    /// Desugar Cypher property map into WHERE clause expression
    fn desugar_properties_map(properties: Map) -> Result<Option<Expr>, ParserError>{

        let mut entries: Vec<Expr> = Vec::new();
        for entry in &properties.entries {
            let key_expr = Expr::BinaryOp {
                left: Box::new(Expr::Identifier(Ident::new("properties"))),
                op: BinaryOperator::Arrow,
                right: Box::new(Expr::Value(Value::SingleQuotedString(entry.key.to_string()).into())),
            };

            let value_expr = Expr::BinaryOp {
                left: Box::new(key_expr),
                op: BinaryOperator::Eq,
                right: entry.value.clone(),
            };
            entries.push(value_expr);
        }
        
        // Combine all expressions with AND
        if entries.is_empty() {
            return Ok(None);
        }
        
        let mut combined = entries[0].clone();
        for expr in entries.iter().skip(1) {
            combined = Expr::BinaryOp {
                left: Box::new(combined),
                op: BinaryOperator::And,
                right: Box::new(expr.clone()),
            };
        }
        
        Ok(Some(combined))
    }

    fn desugar_filters(properties: Option<Expr>, initial: Option<&Ident>) -> Result<Option<Expr>, ParserError> {
        
        let label_expr = if let Some(label) = initial {
            Some(Expr::BinaryOp {
                left: Box::new(Expr::Identifier(Ident::new("Label"))),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::Value(Value::SingleQuotedString(label.to_string()).into())),
            })
        } else {
            None
        };

        if let Some(Expr::Map(map)) = properties {
            let properties_expr = Self::desugar_properties_map(map.clone())?;

            let combined_expr = match (label_expr, properties_expr) {
                (Some(label), Some(props)) => {
                    Some(Expr::BinaryOp {
                        left: Box::new(label),
                        op: BinaryOperator::And,
                        right: Box::new(props),
                    })
                }
                (Some(label), None) => Some(label),
                (None, Some(props)) => Some(props),
                (None, None) => None,
            };

            Ok(combined_expr)            
        } else {
            Ok(label_expr)
        }
    }

    fn desugar_cypher_node_filter(node: NodePattern) -> Result<Option<Expr>, ParserError> {

        let filter = Self::desugar_filters(node.properties.clone(), node.labels.first().clone())?;
        Ok(filter)
    }

    fn desugar_cypher_relationship_filter(relationship: RelationshipPattern) -> Result<Option<Expr>, ParserError> {

        let filter = Self::desugar_filters(relationship.details.properties.clone(), relationship.details.types.first())?;
        Ok(filter)
    }

    // Desugar Cypher node pattern into INSERT INTO nodes statement for individual CTE statement
    fn desugar_cypher_node_insert(node:NodePattern) -> Result<Box<SetExpr>, ParserError>{

        let mut columns = Vec::new();
        let mut node_values = Vec::new();
        let mut values = Vec::new();
        let mut returning = Vec::new();

        match node.labels.first() {
            Some(l) => {
                let label_expr = Expr::Value(Value::SingleQuotedString(l.to_string()).into());
                columns.push(Ident::new("Label"));
                node_values.push(label_expr);
            }
            None => {},
        };
        match node.properties {
            Some(Expr::Map(map)) => {
                let properties_str = Self::cypher_properties_to_string(map.clone())?;
                columns.push(Ident::new("Properties"));
                node_values.push(
                    Expr::Value(Value::SingleQuotedString(properties_str.to_string()).into()),
                );
            },
            _ => {},
        };

        values.push(node_values);

        returning.push(SelectItem::UnnamedExpr(Expr::Identifier(Ident::new("id"))));

        let values_clause = Values {
                explicit_row: false,
                rows: values,
            };
        let source = Some(Box::new(Query {
            with: None,
            body: Box::new(SetExpr::Values(values_clause)),
            order_by: None,
            limit_clause: None,
            for_clause: None,
            settings: None,
            format_clause: None,
            pipe_operators: vec![],
            fetch: None,
            locks: vec![],
        }));

        let table_object = TableObject::TableName(ObjectName(
            vec![ObjectNamePart::Identifier(Ident::new("nodes"))]));

        Ok(Box::new(SetExpr::Insert(Statement::Insert(Insert {
            or: None,
            table: table_object,
            table_alias: None,
            ignore: false,
            into: true,
            overwrite: false,
            partitioned: None,
            columns: columns,
            after_columns: vec![],
            source: source,
            assignments: vec![],
            has_table_keyword: false,
            on: None,
            returning: Some(returning),
            replace_into: false,
            priority: None,
            insert_alias: None,
            settings: None,
            format_clause: None,
        }))))
    }

    // Desugar Cypher node pattern into INSERT INTO nodes statement wrapped in CTE for node and relationship creation
    fn desugar_cypher_node_cte(node_counter: &mut i32, initial_node: NodePattern) -> Result<Cte, ParserError> {

        let node_insert = Self::desugar_cypher_node_insert(initial_node.clone())?;
        let node_alias = Ident::new(format!("node{}", node_counter));
        let subquery = Query {
            with: None,
            body: node_insert,
            order_by: None,
            limit_clause: None,
            fetch: None,
            locks: vec![],
            for_clause: None,
            settings: None,
            format_clause: None,
            pipe_operators: vec![],
        }
        .into();

        Ok(Cte{
            alias: TableAlias { name: node_alias, columns: vec![] },
            query: subquery,
            from: None,
            materialized: None,
            closing_paren_token: AttachedToken(TokenWithSpan {
                token: Token::RParen,
                span: Span::empty(),
            })
        })
    }

    /// Desugar Cypher relationship pattern into SELECT statement for relationship insertion.
    fn desugar_cypher_relationship_select(relationship: RelationshipPattern, s_idx:usize, t_idx:usize) -> Result<Select, ParserError> {
        let rel_type = relationship.details.types.first().map(|id| id.value.clone()).unwrap_or_default();
        let type_expr = Expr::Value(Value::SingleQuotedString(rel_type).into());

        let source_alias = format!("node{}", s_idx);
        let target_alias = format!("node{}", t_idx);

        let source_expr = Expr::CompoundIdentifier(vec![Ident::new(source_alias.clone()), Ident::new("id")]);
        let target_expr = Expr::CompoundIdentifier(vec![Ident::new(target_alias.clone()), Ident::new("id")]);

        let props_expr = match relationship.details.properties.clone() {
            Some(Expr::Map(map)) => {
                let properties_str = Self::cypher_properties_to_string(map)?;
                Expr::Value(Value::SingleQuotedString(properties_str.to_string()).into())
            }
            _ => Expr::Value(Value::SingleQuotedString("{}".to_string()).into()),
        };

        let projection = vec![
            SelectItem::UnnamedExpr(type_expr),
            SelectItem::UnnamedExpr(source_expr),
            SelectItem::UnnamedExpr(target_expr),
            SelectItem::UnnamedExpr(props_expr),
        ];

        let from = vec![TableWithJoins { relation: TableFactor::Table {
            name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new(source_alias.clone()))]),
            alias: None, args: None, with_hints: vec![], version: None, with_ordinality: false, partitions: vec![], json_path: None, sample: None, index_hints: vec![],
        }, joins: vec![] }, TableWithJoins { relation: TableFactor::Table {
            name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new(target_alias.clone()))]),
            alias: None, args: None, with_hints: vec![], version: None, with_ordinality: false, partitions: vec![], json_path: None, sample: None, index_hints: vec![],
        }, joins: vec![] }];

        Ok(Select {
            select_token: AttachedToken::empty(),
            distinct: None,
            top: None,
            top_before_distinct: false,
            projection,
            exclude: None,
            into: None,
            from,
            lateral_views: vec![],
            prewhere: None,
            selection: None,
            group_by: GroupByExpr::Expressions(vec![], vec![]),
            cluster_by: vec![],
            distribute_by: vec![],
            sort_by: vec![],
            having: None,
            named_window: vec![],
            window_before_qualify: false,
            qualify: None,
            value_table_mode: None,
            connect_by: None,
            flavor: SelectFlavor::Standard,
        })
    }

    // fn desugar_cypher_pattern(pattern: Pattern) -> Result<Select, ParserError> {

    //     let mut filters = Vec::new();

    //     for pattern_part in &pattern.parts {
    //         match &pattern_part.anon_pattern_part {
    //             PatternElement::Simple(simple_element) => {

    //                 let node_filter = Self::desugar_cypher_node_filter(simple_element.node.clone())?;
    //                 if let Some(combined_expr) = node_filter {
    //                     filters.push(combined_expr);
    //                 }

    //                 for chain_elem in &simple_element.chain {
    //                     let rel = &chain_elem.relationship;
    //                     // Currently, we only desugar node properties, not relationship properties
    //                     let rel_filter = Self::desugar_cypher_node_filter(chain_elem.node.clone())?;
    //                     if let Some(combined_expr) = rel_filter {
    //                         filters.push(combined_expr);
    //                     }
    //                 }
    //             }
    //             _ => {}
    //         }
    //     }

    //     let selection = if !filters.is_empty() {
    //         let mut combined = filters[0].clone();
    //         for expr in filters.iter().skip(1) {
    //             combined = Expr::BinaryOp {
    //                 left: Box::new(combined),
    //                 op: BinaryOperator::And,
    //                 right: Box::new(expr.clone()),
    //             };
    //         }
    //         Some(combined)
    //     } else {
    //         None
    //     };

    //     let body = Box::new(SetExpr::Select(Select{
    //         select_token: AttachedToken::empty(),
    //         distinct: None,
    //         top: None,
    //         top_before_distinct: false,
    //         projection: vec![],
    //         exclude: None,
    //         into: None,
    //         from: vec![],
    //         lateral_views: vec![],
    //         prewhere: None,
    //         selection,
    //         group_by: GroupByExpr::Expressions(vec![], vec![]),
    //         cluster_by: vec![],
    //         distribute_by: vec![],
    //         sort_by: vec![],
    //         having: None,
    //         named_window: vec![],
    //         window_before_qualify: false,
    //         qualify: None,
    //         value_table_mode: None,
    //         connect_by: None,
    //         flavor: SelectFlavor::Standard,
    //     }));

    //     let query = Query {
    //         with: None,
    //         body,
    //         order_by: None,
    //         limit_clause: None,
    //         for_clause: None,
    //         settings: None,
    //         format_clause: None,
    //         pipe_operators: vec![],
    //         fetch: None,
    //         locks: vec![],
    //     };

    //     Ok(Statement::Query(Box::new(query)))
    // }

    /// Desugar simple node-only CREATE patterns into INSERT INTO nodes statement.
    /// Handles patterns like: CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
    fn desugar_cypher_create_nodes_only(create_clause: CypherCreateClause) -> Result<Statement, ParserError> {
        let mut columns = Vec::new();
        let mut values = Vec::new();

        for pattern_part in &create_clause.pattern.parts {
            match &pattern_part.anon_pattern_part {
                PatternElement::Simple(simple_element) => {
                    let mut node_values = Vec::new();
                    
                    if let Some(label) = simple_element.node.labels.first() {
                        let label_expr = Expr::Value(Value::SingleQuotedString(label.to_string()).into());
                        if !columns.contains(&Ident::new("Label")) {
                            columns.push(Ident::new("Label"));
                        }
                        node_values.push(label_expr);
                    }
                    
                    if let Some(Expr::Map(map)) = &simple_element.node.properties {
                        let properties_str = Self::cypher_properties_to_string(map.clone())?;
                        if !columns.contains(&Ident::new("Properties")) {
                            columns.push(Ident::new("Properties"));
                        }
                        node_values.push(Expr::Value(Value::SingleQuotedString(properties_str).into()));
                        values.push(node_values);
                    }
                }
                _ => {
                    return Err(ParserError::ParserError(
                        "Only simple node patterns are supported in CREATE clause for desugaring to INSERT statements.".to_string()
                    ));
                }
            }
        }

        let values_clause = Values {
            explicit_row: false,
            rows: values,
        };
        
        let source = Some(Box::new(Query {
            with: None,
            body: Box::new(SetExpr::Values(values_clause)),
            order_by: None,
            limit_clause: None,
            for_clause: None,
            settings: None,
            format_clause: None,
            pipe_operators: vec![],
            fetch: None,
            locks: vec![],
        }));

        let table_object = TableObject::TableName(ObjectName(
            vec![ObjectNamePart::Identifier(Ident::new("nodes"))]
        ));

        Ok(Statement::Insert(Insert {
            or: None,
            table: table_object,
            table_alias: None,
            ignore: false,
            into: true,
            overwrite: false,
            partitioned: None,
            columns,
            after_columns: vec![],
            source,
            assignments: vec![],
            has_table_keyword: false,
            on: None,
            returning: None,
            replace_into: false,
            priority: None,
            insert_alias: None,
            settings: None,
            format_clause: None,
        }))
    }

    /// Desugar relationship CREATE patterns into CTEs + INSERT INTO edges statement.
    /// Handles patterns like: CREATE (a:Person)-[:KNOWS]->(b:Person)
    fn desugar_cypher_create_with_relationships(create_clause: CypherCreateClause) -> Result<Statement, ParserError> {
        let mut cte_tables = Vec::new();
        let mut node_counter = 1;

        // Build CTEs for all nodes in the pattern
        for pattern_part in &create_clause.pattern.parts {
            if let PatternElement::Simple(simple) = &pattern_part.anon_pattern_part {
                // Create CTE for initial node
                let initial_node = simple.node.clone();
                let initial_cte = Self::desugar_cypher_node_cte(&mut node_counter, initial_node)?;
                cte_tables.push(initial_cte);
                node_counter += 1;

                // Create CTEs for chained nodes
                for chain_elem in &simple.chain {
                    let chained_node = chain_elem.node.clone();
                    let chain_cte = Self::desugar_cypher_node_cte(&mut node_counter, chained_node)?;
                    cte_tables.push(chain_cte);
                    node_counter += 1;
                }
            } else {
                return Err(ParserError::ParserError(
                    "Only simple node patterns are supported in CREATE clause for desugaring to INSERT statements.".to_string()
                ));
            }
        }

        let with_clause = With {
            with_token: AttachedToken::empty(),
            recursive: false,
            cte_tables,
        };

        // Extract relationship list with source/target indices
        let mut rels: Vec<(RelationshipPattern, usize, usize)> = Vec::new();
        let mut idx: usize = 1;
        
        for part in &create_clause.pattern.parts {
            if let PatternElement::Simple(simple) = &part.anon_pattern_part {
                if !simple.chain.is_empty() {
                    for (k, chain_elem) in simple.chain.iter().enumerate() {
                        let source_idx = idx + k;
                        let target_idx = idx + k + 1;
                        rels.push((chain_elem.relationship.clone(), source_idx, target_idx));
                    }
                    idx += 1 + simple.chain.len();
                }
            }
        }

        if rels.is_empty() {
            return Err(ParserError::ParserError(
                "No relationship found to desugar".to_string()
            ));
        }

        // Build SELECT for each relationship
        let mut selects: Vec<Select> = Vec::new();
        for (rel, s_idx, t_idx) in rels.iter() {
            let select = Self::desugar_cypher_relationship_select(rel.clone(), *s_idx, *t_idx)?;
            selects.push(select);
        }

        // Combine SELECTs with UNION ALL
        let mut combined: SetExpr = SetExpr::Select(Box::new(selects[0].clone()));
        for sel in selects.iter().skip(1) {
            combined = SetExpr::SetOperation {
                op: SetOperator::Union,
                set_quantifier: SetQuantifier::All,
                left: Box::new(combined),
                right: Box::new(SetExpr::Select(Box::new(sel.clone()))),
            };
        }

        let union_source = Query {
            with: None,
            body: Box::new(combined),
            order_by: None,
            limit_clause: None,
            for_clause: None,
            settings: None,
            format_clause: None,
            pipe_operators: vec![],
            fetch: None,
            locks: vec![],
        };

        // Build INSERT INTO edges statement
        let table_object = TableObject::TableName(ObjectName(vec![
            ObjectNamePart::Identifier(Ident::new("edges"))
        ]));

        let columns = vec![
            Ident::new("Label"),
            Ident::new("Source_id"),
            Ident::new("Target_id"),
            Ident::new("Properties")
        ];

        let insert_stmt = Insert {
            or: None,
            table: table_object,
            table_alias: None,
            ignore: false,
            into: true,
            overwrite: false,
            partitioned: None,
            columns,
            after_columns: vec![],
            source: Some(Box::new(union_source)),
            assignments: vec![],
            has_table_keyword: false,
            on: None,
            returning: None,
            replace_into: false,
            priority: None,
            insert_alias: None,
            settings: None,
            format_clause: None,
        };

        // Wrap with WITH clause
        let final_query = Query {
            with: Some(with_clause),
            body: Box::new(SetExpr::Insert(Statement::Insert(insert_stmt))),
            order_by: None,
            limit_clause: None,
            for_clause: None,
            settings: None,
            format_clause: None,
            pipe_operators: vec![],
            fetch: None,
            locks: vec![],
        };

        Ok(Statement::Query(Box::new(final_query)))
    }

    // fn desugar_cypher_reading(reading_query: ReadingQuery) -> Result<Statement, ParserError> {
    //     let mut current_query: Option<Box<Query>> = None;

    //     match reading_query.reading_clause {
    //         CypherReadingClause::Match(match_clause) => {
    //             let current_query = Self::desugar_cypher_match(match_clause)?;
    //         },
    //         // Handle other reading clauses like RETURN, WHERE, etc.
    //         _ => {
    //             return Err(ParserError::ParserError(
    //                 "Desugaring for this Cypher reading clause is not yet implemented.".to_string()
    //             ));
    //         }
    //     }

    //     if let Some(query) = current_query {
    //         Ok(Statement::Query(query))
    //     } else {
    //         Err(ParserError::ParserError(
    //             "No valid Cypher reading clauses found to desugar.".to_string()
    //         ))
    //     }
    // }

    /// Desugar Cypher CREATE clause into SQL INSERT statement(s).
    fn desugar_cypher_create(create_clause: CypherCreateClause) -> Result<Statement, ParserError> {
        
        // Determine if pattern contains relationships or just nodes
        let has_relationships = create_clause.pattern.parts.iter()
            .any(|p| matches!(&p.anon_pattern_part, PatternElement::Simple(s) if !s.chain.is_empty()));
        
        if has_relationships {
            Self::desugar_cypher_create_with_relationships(create_clause)
        } else {
            Self::desugar_cypher_create_nodes_only(create_clause)
        }
    }

    // Desugar Cypher MATCH clause into SQL SELECT statement(s).
    // fn desugar_cypher_match(match_clause: CypherMatchClause) -> Result<Statement, ParserError> {

    //     if match_clause.optional {
    //         return Err(ParserError::ParserError(
    //             "Desugaring Cypher OPTIONAL MATCH clause is not supported.".to_string()
    //         ));
    //     }
    //     else
    //     {
    //         let pattern = self.desugar_cypher_pattern(match_clause.pattern)?;
    //     }
    // }

    pub fn desugar_cypher_query(query: SinglePartQuery) -> Result<Statement, ParserError> {
        match query {
            SinglePartQuery::Reading(reading_query) => {
                //Self::desugar_cypher_reading(reading_query)
                Err(ParserError::ParserError(
                    "Desugaring for Cypher reading queries is not yet implemented.".to_string()
                ))
            },
            SinglePartQuery::Updating(updating_query) => {
                Self::desugar_cypher_create(updating_query.create_clause)
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::helpers::desugar_cypher::Desugarer;
    use crate::parser::ParserError;

    #[test]
    fn test_cypher_properties_to_string() -> Result<(), ParserError> {

        let properties = Map {
            entries: vec![
                MapEntry {
                    key: Box::new(Expr::Identifier(Ident::new("name"))),
                    value: Box::new(Expr::Value(Value::SingleQuotedString("Alice".to_string()).into())),
                },
                MapEntry {
                    key: Box::new(Expr::Identifier(Ident::new("age"))),
                    value: Box::new(Expr::Value(Value::Number("30".to_string(), false).into())),
                },
            ],
        };

        let desugared = Desugarer::desugar_properties_map(properties)?;
        let expected = Some(Expr::BinaryOp {
            left: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::BinaryOp {
                    left: Box::new(Expr::Identifier(Ident::new("properties"))),
                    op: BinaryOperator::Arrow,
                    right: Box::new(Expr::Value(Value::SingleQuotedString("name".to_string()).into())),
                }),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::Value(Value::SingleQuotedString("Alice".to_string()).into())),
            }),
            op: BinaryOperator::And,
            right: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::BinaryOp {
                    left: Box::new(Expr::Identifier(Ident::new("properties"))),
                    op: BinaryOperator::Arrow,
                    right: Box::new(Expr::Value(Value::SingleQuotedString("age".to_string()).into())),
                }),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::Value(Value::Number("30".to_string(), false).into())),
            }),
        });
        assert_eq!(desugared, expected);
        Ok(())
    }

    #[test]
    fn test_desugar_cypher_node_pattern() -> Result<(), ParserError> {
        let node_pattern = NodePattern {
            variable: None,
            labels: vec![Ident::new("Person")],
            properties: Some(Expr::Map(Map {
                entries: vec![
                    MapEntry {
                        key: Box::new(Expr::Identifier(Ident::new("name"))),
                        value: Box::new(Expr::Value(Value::SingleQuotedString("Alice".to_string()).into())),
                    },
                ],
            })),
        };

        let desugared = Desugarer::desugar_cypher_node_filter(node_pattern)?;
        let expected = Some(Expr::BinaryOp {
            left: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Identifier(Ident::new("Label"))),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::Value(Value::SingleQuotedString("Person".to_string()).into())),
            }),
            op: BinaryOperator::And,
            right: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::BinaryOp {
                    left: Box::new(Expr::Identifier(Ident::new("properties"))),
                    op: BinaryOperator::Arrow,
                    right: Box::new(Expr::Value(Value::SingleQuotedString("name".to_string()).into())),
                }),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::Value(Value::SingleQuotedString("Alice".to_string()).into())),
            }),
        });
        assert_eq!(desugared, expected);
        Ok(())
    }

    #[test]
    fn test_desugar_cypher_relationship_pattern(){
        let relationship_pattern = RelationshipPattern {
            details: RelationshipDetail {
                types: vec![Ident::new("KNOWS")],
                properties: Some(Expr::Map(Map {
                    entries: vec![
                        MapEntry {
                            key: Box::new(Expr::Identifier(Ident::new("since"))),
                            value: Box::new(Expr::Value(Value::Number("2020".to_string(), false).into())),
                        },
                    ],
                })),
                length: None,
                variable: None,
            },
            l_direction: Some(RelationshipDirection::Undirected),
            r_direction: Some(RelationshipDirection::Undirected),
        };

        let desugared = Desugarer::desugar_cypher_relationship_filter(relationship_pattern).unwrap();
        let expected = Some(Expr::BinaryOp {
            left: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Identifier(Ident::new("Label"))),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::Value(Value::SingleQuotedString("KNOWS".to_string()).into())),
            }),
            op: BinaryOperator::And,
            right: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::BinaryOp {
                    left: Box::new(Expr::Identifier(Ident::new("properties"))),
                    op: BinaryOperator::Arrow,
                    right: Box::new(Expr::Value(Value::SingleQuotedString("since".to_string()).into())),
                }),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::Value(Value::Number("2020".to_string(), false).into())),
            }),
        });
        assert_eq!(desugared, expected);

    }
}
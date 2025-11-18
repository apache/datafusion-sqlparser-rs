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

    /// Desugar Cypher property map into JSON string format
    fn desugar_cypher_properties(properties: Map) -> Result<String, ParserError>{
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
                let properties_str = Self::desugar_cypher_properties(map.clone())?;
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
                let properties_str = Self::desugar_cypher_properties(map)?;
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
                        let properties_str = Self::desugar_cypher_properties(map.clone())?;
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
    
    /// Desugar Cypher reading query into SQL SELECT statement(s).
    // fn desugar_cypher_reading(reading_query: CypherReadingClause) -> Result<Statement, ParserError> {
    //     let mut current_query: Option<Box<Query>> = None;

    //     for clause in reading_query.clauses {
    //         match clause {
    //             CypherReadingClause::Match(match_clause) => {
    //                 let match_stmt = Self::desugar_cypher_match(match_clause)?;
    //                 current_query = Some(Box::new(Query {
    //                     with: None,
    //                     body: Box::new(SetExpr::Select(Box::new(Select {
    //                         select_token: AttachedToken::empty(),
    //                         distinct: None,
    //                         top: None,
    //                         top_before_distinct: false,
    //                         projection: vec![SelectItem::Wildcard],
    //                         exclude: None,
    //                         into: None,
    //                         from: vec![],
    //                         lateral_views: vec![],
    //                         prewhere: None,
    //                         selection: None,
    //                         group_by: GroupByExpr::Expressions(vec![], vec![]),
    //                         cluster_by: vec![],
    //                         distribute_by: vec![],
    //                         sort_by: vec![],
    //                         having: None,
    //                         named_window: vec![],
    //                         window_before_qualify: false,
    //                         qualify: None,
    //                         value_table_mode: None,
    //                         connect_by: None,
    //                         flavor: SelectFlavor::Standard,
    //                     }))),
    //                     order_by: None,
    //                     limit_clause: None,
    //                     for_clause: None,
    //                     settings: None,
    //                     format_clause: None,
    //                     pipe_operators: vec![],
    //                     fetch: None,
    //                     locks: vec![],
    //                 }));
    //             },
    //             // Handle other reading clauses like RETURN, WHERE, etc.
    //             _ => {
    //                 return Err(ParserError::ParserError(
    //                     "Desugaring for this Cypher reading clause is not yet implemented.".to_string()
    //                 ));
    //             }
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
    fn desugar_cypher_match(match_clause: CypherMatchClause) -> Result<Statement, ParserError> {
        // Placeholder implementation
        Err(ParserError::ParserError(
            "Desugaring Cypher MATCH clause is not yet implemented.".to_string()
        ))
    }

    pub fn desugar_cypher_query(query: SinglePartQuery) -> Result<Statement, ParserError> {
        match query {
            SinglePartQuery::Reading(reading_query) => {
                //Self::desugar_cypher_reading(reading_query)
                Err(ParserError::ParserError(
                    "Desugaring Cypher reading queries is not yet implemented.".to_string()
                ))
            },
            SinglePartQuery::Updating(updating_query) => {
                Self::desugar_cypher_create(updating_query.create_clause)
            },
        }
    }
}
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

    // Helper function to extract alias from TableFactor
    fn extract_alias(table: &TableFactor) -> Result<Ident, ParserError> {
        match table {
            TableFactor::Table { alias: Some(a), .. } => Ok(a.name.clone()),
            _ => Err(ParserError::ParserError("Table must have an alias".to_string())),
        }
    }

    // Helper function to create a table factor with common defaults
    fn create_table_factor(table_name: &str, alias: Ident) -> TableFactor {
        TableFactor::Table {
            name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new(table_name))]),
            alias: Some(TableAlias { name: alias, columns: vec![] }),
            args: None,
            with_hints: vec![],
            version: None,
            with_ordinality: false,
            partitions: vec![],
            json_path: None,
            sample: None,
            index_hints: vec![],
        }
    }

    // Helper function to combine multiple expressions with AND operator
    fn combine_filters_with_and(filters: Vec<Expr>) -> Option<Expr> {
        if filters.is_empty() {
            return None;
        }
        
        let mut combined = filters[0].clone();
        for expr in filters.iter().skip(1) {
            combined = Expr::BinaryOp {
                left: Box::new(combined),
                op: BinaryOperator::And,
                right: Box::new(expr.clone()),
            };
        }
        Some(combined)
    }

    // Helper function to create edge table with automatic alias generation
    fn create_edge_table(relationship: &RelationshipPattern, edge_counter: &mut i32) -> TableFactor {
        let alias = if let Some(ref var) = relationship.details.variable {
            var.clone()
        } else {
            let alias = Ident::new(format!("e{}", edge_counter));
            *edge_counter += 1;
            alias
        };
        Self::create_table_factor("edges", alias)
    }

    // Helper function to create join condition
    fn create_join_condition(left_table: Ident, left_col: &str, right_table: Ident, right_col: &str) -> Expr {
        Expr::BinaryOp {
            left: Box::new(Expr::CompoundIdentifier(vec![left_table, Ident::new(left_col)])),
            op: BinaryOperator::Eq,
            right: Box::new(Expr::CompoundIdentifier(vec![right_table, Ident::new(right_col)])),
        }
    }

    /// Desugar Cypher property map into JSON string format for INSERT statements
    fn properties_to_string(properties: Map) -> Result<String, ParserError>{
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

    /// Desugar Cypher property map into WHERE clause expression with table alias
    fn desugar_properties_map(properties: Map, table_alias: &Ident) -> Result<Option<Expr>, ParserError>{

        let mut entries: Vec<Expr> = Vec::new();
        for entry in &properties.entries {
            let key_expr = Expr::BinaryOp {
                left: Box::new(Expr::CompoundIdentifier(vec![
                    table_alias.clone(),
                    Ident::new("Properties"),
                ])),
                op: BinaryOperator::LongArrow,
                right: Box::new(Expr::Value(Value::SingleQuotedString(entry.key.to_string()).into())),
            };

            // Apply type casting for numeric values
            let left_expr = if let Expr::Value(v) = entry.value.as_ref() {
                if let Value::Number(num_str, _) = &v.value {
                    let cast_type = if num_str.contains('.') {
                        DataType::Float(ExactNumberInfo::None)
                    } else {
                        DataType::Int(None)
                    };
                    
                    Box::new(Expr::Cast {
                        kind: CastKind::DoubleColon,
                        expr: Box::new(Expr::Nested(Box::new(key_expr))),
                        data_type: cast_type,
                        format: None,
                    })
                } else {
                    Box::new(key_expr)
                }
            } else {
                Err(ParserError::ParserError("Unsupported value type for property desugaring".to_string()))?
            };
            
            let value_expr = Expr::BinaryOp {
                left: left_expr,
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

    fn desugar_filters(properties: Option<Expr>, initial: Option<&Ident>, table_alias: &Ident) -> Result<Option<Expr>, ParserError> {
        
        let label_expr = if let Some(label) = initial {
            Some(Expr::BinaryOp {
                left: Box::new(Expr::CompoundIdentifier(vec![
                    table_alias.clone(),
                    Ident::new("Label"),
                ])),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::Value(Value::SingleQuotedString(label.to_string()).into())),
            })
        } else {
            None
        };

        if let Some(Expr::Map(map)) = properties {
            let properties_expr = Self::desugar_properties_map(map.clone(), table_alias)?;

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

    fn desugar_node_filter(node: NodePattern, table_alias: &Ident) -> Result<Option<Expr>, ParserError> {

        let filter = Self::desugar_filters(node.properties.clone(), node.labels.first(), table_alias)?;
        Ok(filter)
    }

    fn desugar_relationship_filter(relationship: RelationshipPattern, table_alias: &Ident) -> Result<Option<Expr>, ParserError> {

        if relationship.details.length.is_some() {
            return Err(ParserError::ParserError("Relationship length is not supported for Desugaring".to_string()));
        }
        let filter = Self::desugar_filters(relationship.details.properties.clone(), relationship.details.types.first(), table_alias)?;
        Ok(filter)
    }

    fn desugar_where(where_clause: CypherWhereClause) -> Result<Expr, ParserError> {

        match where_clause.expr{
            Expr::BinaryOp {left, op, right } => {
                match op {
                    BinaryOperator::And | BinaryOperator::Or => {
                        let left_expr = Self::desugar_where(CypherWhereClause { expr: *left })?;
                        let right_expr = Self::desugar_where(CypherWhereClause { expr: *right })?;
                        return Ok(Expr::BinaryOp {
                            left: Box::new(left_expr),
                            op,
                            right: Box::new(right_expr),
                        });
                    },
                        _ => {},
                }
                match left.as_ref(){
                    Expr::CompoundIdentifier(idents) => {
                        if idents.len() !=2 {
                            return Err(ParserError::ParserError("WHERE clause identifier not valid".to_string()));
                        }
                        
                        // Determine if we need type casting based on the right operand
                        let needs_cast = match right.as_ref() {
                            Expr::Value(v) => matches!(v.value, Value::Number(_, _)),
                            _ => false,
                        };
                        
                        let mut key_expr = Expr::BinaryOp {
                            left: Box::new(Expr::CompoundIdentifier(vec![
                                idents[0].clone(),
                                Ident::new("Properties"),
                            ])),
                            op: BinaryOperator::LongArrow,
                            right: Box::new(Expr::Value(Value::SingleQuotedString(idents[1].to_string()).into())),
                        };
                        
                        // Add type casting if comparing with a number
                        if needs_cast {
                            if let Expr::Value(v) = right.as_ref() {
                                if let Value::Number(num_str, _) = &v.value {
                                    let cast_type = if num_str.contains('.') {
                                        DataType::Float(ExactNumberInfo::None)
                                    } else {
                                        DataType::Int(None)
                                    };
                                    
                                    key_expr = Expr::Cast {
                                        kind: CastKind::DoubleColon,
                                        expr: Box::new(Expr::Nested(Box::new(key_expr))),
                                        data_type: cast_type,
                                        format: None,
                                    };
                                }
                            }
                        }
                        
                        Ok(Expr::BinaryOp {
                            left: Box::new(key_expr),
                            op,
                            right,
                        })
                    },
                    _ => Err(ParserError::ParserError("WHERE clause left expression must be a property access".to_string())),
                }
            },
            _ => Err(ParserError::ParserError("Only binary operations are supported in WHERE clause desugaring".to_string())),
        }
    }

    // Desugar Cypher node pattern into INSERT INTO nodes statement for individual CTE statement
    fn create_insert_from_node(node:NodePattern) -> Result<Box<SetExpr>, ParserError>{

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
                let properties_str = Self::properties_to_string(map.clone())?;
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
    fn create_cte_from_node(node_counter: &mut i32, initial_node: NodePattern) -> Result<Cte, ParserError> {

        let node_insert = Self::create_insert_from_node(initial_node.clone())?;
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

    fn create_table_factor_from_node(node: NodePattern, node_counter: &mut i32) -> Result<TableFactor, ParserError> {
        let table_alias = if let Some(ref var) = node.variable {
            var.clone()
        } else {
            let alias = Ident::new(format!("n{}", node_counter));
            *node_counter += 1;
            alias
        };
        Ok(Self::create_table_factor("nodes", table_alias))
    }

    /// Desugar Cypher relationship pattern into SELECT statement for relationship insertion.
    fn create_select_from_relationship(relationship: RelationshipPattern, s_idx:usize, t_idx:usize) -> Result<Select, ParserError> {
        let rel_type = relationship.details.types.first().map(|id| id.value.clone()).unwrap_or_default();
        let type_expr = Expr::Value(Value::SingleQuotedString(rel_type).into());

        let source_alias = format!("node{}", s_idx);
        let target_alias = format!("node{}", t_idx);

        let source_expr = Expr::CompoundIdentifier(vec![Ident::new(source_alias.clone()), Ident::new("id")]);
        let target_expr = Expr::CompoundIdentifier(vec![Ident::new(target_alias.clone()), Ident::new("id")]);

        let props_expr = match relationship.details.properties.clone() {
            Some(Expr::Map(map)) => {
                let properties_str = Self::properties_to_string(map)?;
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

    fn desugar_nodes_only_for_match(pattern: Pattern) -> Result<Select, ParserError> {
        let mut filters = Vec::new();
        let mut first_table: Option<TableWithJoins> = None;
        let mut node_counter = 1;

        // Process each node pattern to build FROM clause and WHERE filters
        for (idx, pattern_part) in pattern.parts.iter().enumerate() {
            match &pattern_part.anon_pattern_part {
                PatternElement::Simple(simple_element) => {
                    let table_factor = Self::create_table_factor_from_node(simple_element.node.clone(), &mut node_counter)?;
                    let table_alias = Self::extract_alias(&table_factor)?;

                    // Build FROM clause with explicit CROSS JOINs
                    if idx == 0 {
                        first_table = Some(TableWithJoins {
                            relation: table_factor,
                            joins: vec![],
                        });
                    } else if let Some(ref mut first) = first_table {
                        first.joins.push(Join {
                            relation: table_factor,
                            global: false,
                            join_operator: JoinOperator::CrossJoin(JoinConstraint::None),
                        });
                    }

                    if let Some(filter) = Self::desugar_node_filter(simple_element.node.clone(), &table_alias)? {
                        filters.push(filter);
                    }
                }
                _ => {
                    return Err(ParserError::ParserError(
                        "Only simple node patterns are supported in MATCH clause".to_string()
                    ));
                }
            }
        }

        let selection = Self::combine_filters_with_and(filters);

        // Build the SELECT statement
        let from_tables = if let Some(first) = first_table {
            vec![first]
        } else {
            vec![]
        };

        Ok(Select {
            select_token: AttachedToken::empty(),
            distinct: None,
            top: None,
            top_before_distinct: false,
            projection: vec![SelectItem::Wildcard(WildcardAdditionalOptions::default())],
            exclude: None,
            into: None,
            from: from_tables,
            lateral_views: vec![],
            prewhere: None,
            selection,
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

    fn desugar_pattern_for_match(pattern: Pattern) -> Result<Select, ParserError> {
        let mut first_table: Option<TableWithJoins> = None;
        let mut edge_filters = Vec::new();
        let mut node_filters = Vec::new();
        let mut node_counter = 1;
        let mut edge_counter = 1;

        // Process all pattern parts
        for pattern_part in &pattern.parts {
            let simple_element = match &pattern_part.anon_pattern_part {
                PatternElement::Simple(s) => s,
                PatternElement::Nested(_) => {
                    return Err(ParserError::ParserError(
                        "Nested pattern elements are not supported in MATCH clause".to_string()
                    ));
                }
            };

            // Start with the first node in the pattern
            let mut current_node = Self::create_table_factor_from_node(simple_element.node.clone(), &mut node_counter)?;
            let current_alias = Self::extract_alias(&current_node)?;
            let first_node_filter = Self::desugar_node_filter(simple_element.node.clone(), &current_alias)?;
            if let Some(combined_expr) = first_node_filter {
                node_filters.push(combined_expr);
            }

            // Process each relationship chain element
            for chain_elem in &simple_element.chain {
                let edge_table = Self::create_edge_table(&chain_elem.relationship, &mut edge_counter);
                let rel_alias = Self::extract_alias(&edge_table)?;
                
                if let Some(filter) = Self::desugar_relationship_filter(chain_elem.relationship.clone(), &rel_alias)? {
                    edge_filters.push(filter);
                }

                let target_node = Self::create_table_factor_from_node(chain_elem.node.clone(), &mut node_counter)?;
                let target_alias = Self::extract_alias(&target_node)?;
                if let Some(filter) = Self::desugar_node_filter(chain_elem.node.clone(), &target_alias)? {
                    node_filters.push(filter);
                }

                // Start with first edge table in FROM clause
                if first_table.is_none() {
                    first_table = Some(TableWithJoins {
                        relation: edge_table.clone(),
                        joins: vec![],
                    });
                    
                    // Join the source node
                    if let Some(ref mut first) = first_table {
                        let source_alias = Self::extract_alias(&current_node)?;
                        let source_join_condition = Self::create_join_condition(rel_alias.clone(), "Source_id", source_alias, "id");
                        first.joins.push(Join {
                            relation: current_node.clone(),
                            global: false,
                            join_operator: JoinOperator::Join(JoinConstraint::On(source_join_condition)),
                        });
                    }
                } else {
                    // For subsequent edges in the chain, join the edge first
                    if let Some(ref mut first) = first_table {
                        let source_alias = Self::extract_alias(&current_node)?;
                        let edge_join_condition = Self::create_join_condition(rel_alias.clone(), "Source_id", source_alias, "id");
                        first.joins.push(Join {
                            relation: edge_table.clone(),
                            global: false,
                            join_operator: JoinOperator::Join(JoinConstraint::On(edge_join_condition)),
                        });
                    }
                }

                // Join target node: r.Target_id = b.id
                if let Some(ref mut first) = first_table {
                    let target_join_condition = Self::create_join_condition(rel_alias, "Target_id", target_alias, "id");
                    first.joins.push(Join {
                        relation: target_node.clone(),
                        global: false,
                        join_operator: JoinOperator::Join(JoinConstraint::On(target_join_condition)),
                    });
                }

                // Update current node to be the target for the next iteration
                current_node = target_node;
            }
        }

        // Combine filters: edge filters first, then node filters
        edge_filters.extend(node_filters);
        let selection = Self::combine_filters_with_and(edge_filters);

        Ok(Select {
            select_token: AttachedToken::empty(),
            distinct: None,
            top: None,
            top_before_distinct: false,
            projection: vec![SelectItem::Wildcard(WildcardAdditionalOptions::default())],
            exclude: None,
            into: None,
            from: first_table.into_iter().collect(),
            lateral_views: vec![],
            prewhere: None,
            selection,
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
    fn desugar_nodes_only_for_create(create_clause: CypherCreateClause) -> Result<Statement, ParserError> {
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
                        let properties_str = Self::properties_to_string(map.clone())?;
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
    fn desugar_pattern_for_create(create_clause: CypherCreateClause) -> Result<Statement, ParserError> {
        let mut cte_tables = Vec::new();
        let mut node_counter = 1;

        // Build CTEs for all nodes in the pattern
        for pattern_part in &create_clause.pattern.parts {
            if let PatternElement::Simple(simple) = &pattern_part.anon_pattern_part {
                // Create CTE for initial node
                let initial_node = simple.node.clone();
                let initial_cte = Self::create_cte_from_node(&mut node_counter, initial_node)?;
                cte_tables.push(initial_cte);
                node_counter += 1;

                // Create CTEs for chained nodes
                for chain_elem in &simple.chain {
                    let chained_node = chain_elem.node.clone();
                    let chain_cte = Self::create_cte_from_node(&mut node_counter, chained_node)?;
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
            let select = Self::create_select_from_relationship(rel.clone(), *s_idx, *t_idx)?;
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

    fn desugar_return(returning_clause: ReturningClause, mut select: Select) -> Result<Select, ParserError> {
        
        let mut projections = Vec::new();
        
        let distinct = if returning_clause.body.distinct {
            Some(Distinct::Distinct)
        } else {
            None
        };

        for item in returning_clause.body.projections {
            match item {
                ProjectionItem::Expr { expr, alias } => {
                    match expr {
                        // Handle property access: a.name -> a.Properties ->> 'name'
                        Expr::CompoundIdentifier(idents) if idents.len() == 2 => {
                            let key_expr = Expr::BinaryOp {
                                left: Box::new(Expr::CompoundIdentifier(vec![
                                    idents[0].clone(),
                                    Ident::new("Properties"),
                                ])),
                                op: BinaryOperator::LongArrow,
                                right: Box::new(Expr::Value(Value::SingleQuotedString(idents[1].to_string()).into())),
                            };
                            
                            if let Some(a) = alias {
                                projections.push(SelectItem::ExprWithAlias {
                                    expr: key_expr,
                                    alias: a,
                                });
                            } else {
                                projections.push(SelectItem::UnnamedExpr(key_expr));
                            }
                        },
                        // Handle simple identifier: a -> a.* (select all columns from that table)
                        Expr::Identifier(ident) => {
                            projections.push(SelectItem::QualifiedWildcard(
                                SelectItemQualifiedWildcardKind::ObjectName(
                                    ObjectName(vec![ObjectNamePart::Identifier(ident)])
                                ),
                                WildcardAdditionalOptions::default(),
                            ));
                        },
                        _ => return Err(ParserError::ParserError(
                            "RETURN only supports identifiers (e.g., 'a') or property access expressions (e.g., 'a.name')".to_string()
                        )),
                    }
                },
                ProjectionItem::All => {
                    projections.push(SelectItem::Wildcard(WildcardAdditionalOptions::default()));
                }
            }
        }

        select.projection = projections;
        select.distinct = distinct;

        Ok(select)
    }

    fn desugar_reading_clause(reading_query: CypherReadingClause) -> Result<Select, ParserError> {
        match reading_query {
            CypherReadingClause::Match(match_clause) => {
                Ok(Self::desugar_match(match_clause)?)
            },
        }
    }

    // Desugar Cypher MATCH clause into SQL SELECT statement(s).
    fn desugar_match(match_clause: CypherMatchClause) -> Result<Select, ParserError> {

        if match_clause.optional {
            return Err(ParserError::ParserError(
                "Desugaring Cypher OPTIONAL MATCH clause is not supported.".to_string()
            ));
        }
        else
        {
            // Determine if pattern contains relationships or just nodes
            let has_relationships = match_clause.pattern.parts.iter()
                .any(|p| matches!(&p.anon_pattern_part, PatternElement::Simple(s) if !s.chain.is_empty()));
                
            let mut select =if has_relationships {
                Self::desugar_pattern_for_match(match_clause.pattern)?
            } else {
                Self::desugar_nodes_only_for_match(match_clause.pattern)?
            };
            
            // Add WHERE clause if present
            if let Some(where_clause) = match_clause.where_clause {
                let where_expr = Self::desugar_where(where_clause)?;
                select.selection = match select.selection {
                    Some(existing) => Some(Expr::BinaryOp {
                        left: Box::new(existing),
                        op: BinaryOperator::And,
                        right: Box::new(where_expr),
                    }),
                    None => Some(where_expr),
                };
            }

            Ok(select)
        }
    }

    /// Desugar Cypher CREATE clause into SQL INSERT statement(s).
    fn desugar_create(create_clause: CypherCreateClause) -> Result<Statement, ParserError> {
        
        // Determine if pattern contains relationships or just nodes
        let has_relationships = create_clause.pattern.parts.iter()
            .any(|p| matches!(&p.anon_pattern_part, PatternElement::Simple(s) if !s.chain.is_empty()));
        
        if has_relationships {
            Self::desugar_pattern_for_create(create_clause)
        } else {
            Self::desugar_nodes_only_for_create(create_clause)
        }
    }

    pub fn desugar_cypher_query(query: SinglePartQuery) -> Result<Statement, ParserError> {
        match query {
            SinglePartQuery::Reading(reading_query) => {
                let reading = Self::desugar_reading_clause(reading_query.reading_clause)?;
                let select = Self::desugar_return(reading_query.returning_clause, reading)?;

                Ok(Statement::Query(Box::new(Query {
                    with: None,
                    body: Box::new(SetExpr::Select(Box::new(select))),
                    order_by: None,
                    limit_clause: None,
                    for_clause: None,
                    settings: None,
                    format_clause: None,
                    pipe_operators: vec![],
                    fetch: None,
                    locks: vec![],
                })))
            },
            SinglePartQuery::Updating(updating_query) => {
                Self::desugar_create(updating_query.create_clause)
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::helpers::desugar_cypher::Desugarer;

    #[test]
    fn test_properties_to_string(){

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

        let dummy_alias = Ident::new("n");
        let desugared = Desugarer::desugar_properties_map(properties, &dummy_alias).unwrap();
        let expected = Some(Expr::BinaryOp {
            left: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::BinaryOp {
                    left: Box::new(Expr::CompoundIdentifier(vec![Ident::new("n"), Ident::new("Properties")])),
                    op: BinaryOperator::LongArrow,
                    right: Box::new(Expr::Value(Value::SingleQuotedString("name".to_string()).into())),
                }),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::Value(Value::SingleQuotedString("Alice".to_string()).into())),
            }),
            op: BinaryOperator::And,
            right: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Cast {
                    kind: CastKind::DoubleColon,
                    expr: Box::new(Expr::Nested(Box::new(
                        Expr::BinaryOp {
                            left: Box::new(Expr::CompoundIdentifier(vec![Ident::new("n"), Ident::new("Properties")])),
                            op: BinaryOperator::LongArrow,
                            right: Box::new(Expr::Value(Value::SingleQuotedString("age".to_string()).into())),
                        }
                    ))),
                    data_type: DataType::Int(None),
                    format: None,
                }),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::Value(Value::Number("30".to_string(), false).into())),
            }),
        });
        assert_eq!(desugared, expected);
    }

    #[test]
    fn test_desugar_cypher_node_pattern()  {
        let node_pattern = NodePattern {
            variable: Some(Ident::new("n")),
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

        let alias = Ident::new("n");
        let desugared = Desugarer::desugar_node_filter(node_pattern, &alias).unwrap();
        let expected = Some(Expr::BinaryOp {
            left: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::CompoundIdentifier(vec![Ident::new("n"), Ident::new("Label")])),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::Value(Value::SingleQuotedString("Person".to_string()).into())),
            }),
            op: BinaryOperator::And,
            right: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::BinaryOp {
                    left: Box::new(Expr::CompoundIdentifier(vec![Ident::new("n"), Ident::new("Properties")])),
                    op: BinaryOperator::LongArrow,
                    right: Box::new(Expr::Value(Value::SingleQuotedString("name".to_string()).into())),
                }),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::Value(Value::SingleQuotedString("Alice".to_string()).into())),
            }),
        });
        assert_eq!(desugared, expected);
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

        let alias = Ident::new("r");
        let desugared = Desugarer::desugar_relationship_filter(relationship_pattern, &alias).unwrap();
        let expected = Some(Expr::BinaryOp {
            left: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::CompoundIdentifier(vec![Ident::new("r"), Ident::new("Label")])),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::Value(Value::SingleQuotedString("KNOWS".to_string()).into())),
            }),
            op: BinaryOperator::And,
            right: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Cast {
                    kind: CastKind::DoubleColon,
                    expr: Box::new(Expr::Nested(Box::new(
                        Expr::BinaryOp {
                            left: Box::new(Expr::CompoundIdentifier(vec![Ident::new("r"), Ident::new("Properties")])),
                            op: BinaryOperator::LongArrow,
                            right: Box::new(Expr::Value(Value::SingleQuotedString("since".to_string()).into())),
                        }
                    ))),
                    data_type: DataType::Int(None),
                    format: None,
                }),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::Value(Value::Number("2020".to_string(), false).into())),
            }),
        });
        assert_eq!(desugared, expected);
    }

    #[test]
    fn test_desguar_cypher_match_nodes_only() {
        let pattern = Pattern {
            parts: vec![
                PatternPart {
                    variable: None,
                    anon_pattern_part: PatternElement::Simple(SimplePatternElement {
                        node: NodePattern {
                            variable: Some(Ident::new("a")),
                            labels: vec![Ident::new("Person")],
                            properties: Some(Expr::Map(Map {
                                entries: vec![
                                    MapEntry {
                                        key: Box::new(Expr::Identifier(Ident::new("name"))),
                                        value: Box::new(Expr::Value(Value::SingleQuotedString("Alice".to_string()).into())),
                                    },
                                ],
                            })),
                        },
                        chain: vec![],
                    }),
                },
                PatternPart {
                    variable: None,
                    anon_pattern_part: PatternElement::Simple(SimplePatternElement {
                        node: NodePattern {
                            variable: Some(Ident::new("b")),
                            labels: vec![Ident::new("Person")],
                            properties: Some(Expr::Map(Map {
                                entries: vec![
                                    MapEntry {
                                        key: Box::new(Expr::Identifier(Ident::new("name"))),
                                        value: Box::new(Expr::Value(Value::SingleQuotedString("Bob".to_string()).into())),
                                    },
                                ],
                            })),
                        },
                        chain: vec![],
                    }),
                },
            ],
        };

        let desugared = Desugarer::desugar_nodes_only_for_match(pattern).unwrap();
        let expected = Select {
            select_token: AttachedToken::empty(),
            distinct: None,
            top: None,
            top_before_distinct: false,
            projection: vec![SelectItem::Wildcard(WildcardAdditionalOptions::default())],
            exclude: None,
            into: None,
            from: vec![
                TableWithJoins {
                    relation: TableFactor::Table {
                        name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new("nodes"))]),
                        alias: Some(TableAlias {
                            name: Ident::new("a"),
                            columns: vec![],
                        }),
                        args: None,
                        with_hints: vec![],
                        version: None,
                        with_ordinality: false,
                        partitions: vec![],
                        json_path: None,
                        sample: None,
                        index_hints: vec![],
                    },
                    joins: vec![
                        Join {
                            relation: TableFactor::Table {
                                name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new("nodes"))]),
                                alias: Some(TableAlias {
                                    name: Ident::new("b"),
                                    columns: vec![],
                                }),
                                args: None,
                                with_hints: vec![],
                                version: None,
                                with_ordinality: false,
                                partitions: vec![],
                                json_path: None,
                                sample: None,
                                index_hints: vec![],
                            },
                            global: false,
                            join_operator: JoinOperator::CrossJoin(JoinConstraint::None),
                        }
                    ],
                }
            ],
            lateral_views: vec![],
            prewhere: None,
            selection: Some(Expr::BinaryOp {
                left: Box::new(Expr::BinaryOp {
                    left: Box::new(Expr::BinaryOp {
                        left: Box::new(Expr::CompoundIdentifier(vec![Ident::new("a"), Ident::new("Label")])),
                        op: BinaryOperator::Eq,
                        right: Box::new(Expr::Value(Value::SingleQuotedString("Person".to_string()).into())),
                    }),
                    op: BinaryOperator::And,
                    right: Box::new(Expr::BinaryOp {
                        left: Box::new(Expr::BinaryOp {
                            left: Box::new(Expr::CompoundIdentifier(vec![Ident::new("a"), Ident::new("Properties")])),
                            op: BinaryOperator::LongArrow,
                            right: Box::new(Expr::Value(Value::SingleQuotedString("name".to_string()).into())),
                        }),
                        op: BinaryOperator::Eq,
                        right: Box::new(Expr::Value(Value::SingleQuotedString("Alice".to_string()).into())),
                    }),
                }),
                op: BinaryOperator::And,
                right: Box::new(Expr::BinaryOp {
                    left: Box::new(Expr::BinaryOp {
                        left: Box::new(Expr::CompoundIdentifier(vec![Ident::new("b"), Ident::new("Label")])),
                        op: BinaryOperator::Eq,
                        right: Box::new(Expr::Value(Value::SingleQuotedString("Person".to_string()).into())),
                    }),
                    op: BinaryOperator::And,
                    right: Box::new(Expr::BinaryOp {
                        left: Box::new(Expr::BinaryOp {
                            left: Box::new(Expr::CompoundIdentifier(vec![Ident::new("b"), Ident::new("Properties")])),
                            op: BinaryOperator::LongArrow,
                            right: Box::new(Expr::Value(Value::SingleQuotedString("name".to_string()).into())),
                        }),
                        op: BinaryOperator::Eq,
                        right: Box::new(Expr::Value(Value::SingleQuotedString("Bob".to_string()).into())),
                    }),
                }),
            }),
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
        };
        assert_eq!(desugared, expected);
    }

    #[test]
    fn test_desugar_cypher_single_where() {
        let where_clause = CypherWhereClause {
            expr: Expr::BinaryOp {
                left: Box::new(Expr::CompoundIdentifier(vec![Ident::new("n"), Ident::new("age")])),
                op: BinaryOperator::Gt,
                right: Box::new(Expr::Value(Value::Number("30".to_string(), false).into())),
            },
        };

        let desugared = Desugarer::desugar_where(where_clause).unwrap();
        let expected = Expr::BinaryOp {
            left: Box::new(
                Expr::Cast {
                    kind: CastKind::DoubleColon,
                    expr: Box::new(Expr::Nested(Box::new(
                        Expr::BinaryOp {
                            left: Box::new(Expr::CompoundIdentifier(vec![Ident::new("n"), Ident::new("Properties"),])),
                            op: BinaryOperator::LongArrow,
                            right: Box::new(Expr::Value(Value::SingleQuotedString("age".to_string()).into())),
                        }
                    ))),
                    data_type: DataType::Int(None),
                    format: None,
                }),
            op: BinaryOperator::Gt,
            right: Box::new(Expr::Value(Value::Number("30".to_string(), false).into())),
        };
        assert_eq!(desugared, expected);
    }

    #[test]
    fn test_desugar_cypher_and_where(){
        let where_clause = CypherWhereClause {
            expr: Expr::BinaryOp {
                left: Box::new(Expr::BinaryOp {
                    left: Box::new(Expr::CompoundIdentifier(vec![Ident::new("n"), Ident::new("age")])),
                    op: BinaryOperator::Gt,
                    right: Box::new(Expr::Value(Value::Number("30".to_string(), false).into())),
                }),
                op: BinaryOperator::And,
                right: Box::new(Expr::BinaryOp {
                    left: Box::new(Expr::CompoundIdentifier(vec![Ident::new("n"), Ident::new("city")])),
                    op: BinaryOperator::Eq,
                    right: Box::new(Expr::Value(Value::SingleQuotedString("London".to_string()).into())),
                }),
            },
        };

        let desugared = Desugarer::desugar_where(where_clause).unwrap();
        let expected = Expr::BinaryOp {
            left: Box::new(
                Expr::BinaryOp {
                    left: Box::new(
                        Expr::Cast {
                            kind: CastKind::DoubleColon,
                            expr: Box::new(Expr::Nested(Box::new(
                                Expr::BinaryOp {
                                    left: Box::new(Expr::CompoundIdentifier(vec![Ident::new("n"), Ident::new("Properties"),])),
                                    op: BinaryOperator::LongArrow,
                                    right: Box::new(Expr::Value(Value::SingleQuotedString("age".to_string()).into())),
                                }
                            ))),
                            data_type: DataType::Int(None),
                            format: None,
                        }
                    ),
                    op: BinaryOperator::Gt,
                    right: Box::new(Expr::Value(Value::Number("30".to_string(), false).into())),
                }),
            op: BinaryOperator::And,
            right: Box::new(
                Expr::BinaryOp {
                    left: Box::new(Expr::BinaryOp {
                        left: Box::new(Expr::CompoundIdentifier(vec![Ident::new("n"), Ident::new("Properties"),])),
                        op: BinaryOperator::LongArrow,
                        right: Box::new(Expr::Value(Value::SingleQuotedString("city".to_string()).into())),
                    }),
                    op: BinaryOperator::Eq,
                    right: Box::new(Expr::Value(Value::SingleQuotedString("London".to_string()).into())),
                }),
        };
        assert_eq!(desugared, expected);
    }

    #[test]
    fn test_desugar_cypher_or_where(){
        let where_clause = CypherWhereClause {
            expr: Expr::BinaryOp {
                left: Box::new(Expr::BinaryOp {
                    left: Box::new(Expr::CompoundIdentifier(vec![Ident::new("n"), Ident::new("age")])),
                    op: BinaryOperator::Lt,
                    right: Box::new(Expr::Value(Value::Number("25".to_string(), false).into())),
                }),
                op: BinaryOperator::Or,
                right: Box::new(Expr::BinaryOp {
                    left: Box::new(Expr::CompoundIdentifier(vec![Ident::new("n"), Ident::new("city")])),
                    op: BinaryOperator::Eq,
                    right: Box::new(Expr::Value(Value::SingleQuotedString("New York".to_string()).into())),
                }),
            },
        };

        let desugared = Desugarer::desugar_where(where_clause).unwrap();
        let expected = Expr::BinaryOp {
            left: Box::new(
                Expr::BinaryOp {
                    left: Box::new(
                        Expr::Cast {
                            kind: CastKind::DoubleColon,
                            expr: Box::new(Expr::Nested(Box::new(
                                Expr::BinaryOp {
                                    left: Box::new(Expr::CompoundIdentifier(vec![Ident::new("n"), Ident::new("Properties"),])),
                                    op: BinaryOperator::LongArrow,
                                    right: Box::new(Expr::Value(Value::SingleQuotedString("age".to_string()).into())),
                                }
                            ))),
                            data_type: DataType::Int(None),
                            format: None,
                        }
                    ),
                    op: BinaryOperator::Lt,
                    right: Box::new(Expr::Value(Value::Number("25".to_string(), false).into())),
                }),
            op: BinaryOperator::Or,
            right: Box::new(
                Expr::BinaryOp {
                    left: Box::new(Expr::BinaryOp {
                        left: Box::new(Expr::CompoundIdentifier(vec![Ident::new("n"), Ident::new("Properties"),])),
                        op: BinaryOperator::LongArrow,
                        right: Box::new(Expr::Value(Value::SingleQuotedString("city".to_string()).into())),
                    }),
                    op: BinaryOperator::Eq,
                    right: Box::new(Expr::Value(Value::SingleQuotedString("New York".to_string()).into())),
                }),
        };
        assert_eq!(desugared, expected);
    }

    #[test]
    fn test_cypher_return(){
        let returning_clause = ReturningClause {
            body: ProjectionBody {
                distinct: true,
                projections: vec![
                    ProjectionItem::Expr {
                        expr: Expr::CompoundIdentifier(vec![Ident::new("n"), Ident::new("name")]),
                        alias: Some(Ident::new("person_name")),
                    },
                    ProjectionItem::All,
                ],
            },
        };

        let select = Select {
            select_token: AttachedToken::empty(),
            distinct: None,
            top: None,
            top_before_distinct: false,
            projection: vec![],
            exclude: None,
            into: None,
            from: vec![],
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
        };

        let desugared = Desugarer::desugar_return(returning_clause, select).unwrap();
        let expected = Select {
            select_token: AttachedToken::empty(),
            distinct: Some(Distinct::Distinct),
            top: None,
            top_before_distinct: false,
            projection: vec![
                SelectItem::ExprWithAlias {
                    expr: Expr::BinaryOp {
                        left: Box::new(Expr::CompoundIdentifier(vec![
                            Ident::new("n"),
                            Ident::new("Properties"),
                        ])),
                        op: BinaryOperator::LongArrow,
                        right: Box::new(Expr::Value(Value::SingleQuotedString("name".to_string()).into())),
                    },
                    alias: Ident::new("person_name"),
                },
                SelectItem::Wildcard(WildcardAdditionalOptions::default()),
            ],
            exclude: None,
            into: None,
            from: vec![],
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
        };

        assert_eq!(desugared, expected);
    }
}
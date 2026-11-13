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

//! PostgreSQL spelling for the expression shape target.

use sqlparser::ast::{visit_expressions_mut, BinaryOperator, Expr, Statement};
use std::ops::ControlFlow;

/// Spells every custom operator as `OPERATOR(name)`, so its name reaches the engine even when the bare text would lex differently.
pub fn spell_custom_operators(statement: &mut Statement) {
    let _ = visit_expressions_mut(statement, |expr| {
        if let Expr::BinaryOp { op, .. } = expr {
            if let BinaryOperator::Custom(name) = op {
                *op = BinaryOperator::PGCustomBinaryOperator(vec![std::mem::take(name)]);
            }
        }
        ControlFlow::<()>::Continue(())
    });
}

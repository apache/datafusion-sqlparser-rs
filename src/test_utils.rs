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

/// This module contains internal utilities used for testing the library.
/// While technically public, the library's users are not supposed to rely
/// on this module, as it will change without notice.
//
// Integration tests (i.e. everything under `tests/`) import this
// via `tests::test_utils::*`.

#[cfg(not(feature = "std"))]
use alloc::{
    boxed::Box,
    string::{String, ToString},
    vec,
    vec::Vec,
};

use crate::ast::*;

#[cfg(test)]
use pretty_assertions::assert_eq;

// Re-export everything from `src/test_dialect_utils.rs` since the symbols
// from test_dialect_utils used to be part of this module.
#[cfg(feature = "parser")]
pub use crate::test_dialect_utils::*;

pub fn assert_eq_vec<T: ToString>(expected: &[&str], actual: &[T]) {
    assert_eq!(
        expected,
        actual.iter().map(ToString::to_string).collect::<Vec<_>>()
    );
}

pub fn only<T>(v: impl IntoIterator<Item = T>) -> T {
    let mut iter = v.into_iter();
    if let (Some(item), None) = (iter.next(), iter.next()) {
        item
    } else {
        panic!("only called on collection without exactly one item")
    }
}

pub fn expr_from_projection(item: &SelectItem) -> &Expr {
    match item {
        SelectItem::UnnamedExpr(expr) => expr,
        _ => panic!("Expected UnnamedExpr"),
    }
}

pub fn alter_table_op_with_name(stmt: Statement, expected_name: &str) -> AlterTableOperation {
    match stmt {
        Statement::AlterTable {
            name,
            if_exists,
            only: is_only,
            operations,
            on_cluster: _,
            location: _,
        } => {
            assert_eq!(name.to_string(), expected_name);
            assert!(!if_exists);
            assert!(!is_only);
            only(operations)
        }
        _ => panic!("Expected ALTER TABLE statement"),
    }
}

pub fn alter_table_op(stmt: Statement) -> AlterTableOperation {
    alter_table_op_with_name(stmt, "tab")
}

/// Creates a `Value::Number`, panic'ing if n is not a number
pub fn number(n: &str) -> Value {
    Value::Number(n.parse().unwrap(), false)
}

pub fn table_alias(name: impl Into<String>) -> Option<TableAlias> {
    Some(TableAlias {
        name: Ident::new(name),
        columns: vec![],
    })
}

pub fn table(name: impl Into<String>) -> TableFactor {
    TableFactor::Table {
        name: ObjectName::from(vec![Ident::new(name.into())]),
        alias: None,
        args: None,
        with_hints: vec![],
        version: None,
        partitions: vec![],
        with_ordinality: false,
        json_path: None,
        sample: None,
        index_hints: vec![],
    }
}

pub fn table_from_name(name: ObjectName) -> TableFactor {
    TableFactor::Table {
        name,
        alias: None,
        args: None,
        with_hints: vec![],
        version: None,
        partitions: vec![],
        with_ordinality: false,
        json_path: None,
        sample: None,
        index_hints: vec![],
    }
}

pub fn table_with_alias(name: impl Into<String>, alias: impl Into<String>) -> TableFactor {
    TableFactor::Table {
        name: ObjectName::from(vec![Ident::new(name)]),
        alias: Some(TableAlias {
            name: Ident::new(alias),
            columns: vec![],
        }),
        args: None,
        with_hints: vec![],
        version: None,
        partitions: vec![],
        with_ordinality: false,
        json_path: None,
        sample: None,
        index_hints: vec![],
    }
}

pub fn join(relation: TableFactor) -> Join {
    Join {
        relation,
        global: false,
        join_operator: JoinOperator::Inner(JoinConstraint::Natural),
    }
}

pub fn call(function: &str, args: impl IntoIterator<Item = Expr>) -> Expr {
    Expr::Function(Function {
        name: ObjectName::from(vec![Ident::new(function)]),
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: args
                .into_iter()
                .map(|arg| FunctionArg::Unnamed(FunctionArgExpr::Expr(arg)))
                .collect(),
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
    })
}

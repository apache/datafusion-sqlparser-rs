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

#![warn(clippy::all)]
//! Test SQL syntax, specific to [sqlparser::dialect::OracleDialect].

extern crate core;

#[cfg(test)]
use pretty_assertions::assert_eq;

use sqlparser::{
    ast::{Expr, SelectItem, Value},
    dialect::OracleDialect,
};
#[cfg(test)]
use test_utils::TestedDialects;

mod test_utils;

#[test]
fn muldiv_have_same_precedence_as_strconcat() {
    // ~ oracle: `*`, `/`, and `||` have the same precedence and parse from left to right
    for op in ["*", "/"] {
        let sql = format!("SELECT 1 {op} 2 || 'asdf' FROM dual");
        let mut query = oracle_dialect().verified_query(&sql);

        nest_binary_ops(&mut query.body.as_select_mut().expect("not a SELECT").projection[0]);
        assert_eq!(
            &format!("{query}"),
            &format!("SELECT ((1 {op} 2) || 'asdf') FROM dual")
        );
    }
}

#[test]
fn plusminus_have_lower_precedence_than_strconcat() {
    // ~ oracle: `||` has higher precedence than `+` or `-`
    for op in ["+", "-"] {
        let sql = format!("SELECT 1 {op} 2 || 'asdf' FROM dual");
        let mut query = oracle_dialect().verified_query(&sql);

        nest_binary_ops(&mut query.body.as_select_mut().expect("not a SELECT").projection[0]);
        assert_eq!(
            &format!("{query}"),
            &format!("SELECT (1 {op} (2 || 'asdf')) FROM dual")
        );
    }
}

fn oracle_dialect() -> TestedDialects {
    TestedDialects::new(vec![Box::new(OracleDialect)])
}

/// Wraps [Expr::BinaryExpr]s in `item` with a [Expr::Nested] recursively.
fn nest_binary_ops(item: &mut SelectItem) {
    // ~ idealy, we could use `VisitorMut` at this point
    fn nest(expr: &mut Expr) {
        // ~ ideally we could use VisitorMut here
        if let Expr::BinaryOp { left, op: _, right } = expr {
            nest(&mut *left);
            nest(&mut *right);
            let inner = std::mem::replace(expr, Expr::Value(Value::Null.into()));
            *expr = Expr::Nested(Box::new(inner));
        }
    }
    match item {
        SelectItem::UnnamedExpr(expr) => nest(expr),
        SelectItem::ExprWithAlias { expr, alias: _ } => nest(expr),
        SelectItem::QualifiedWildcard(_, _) => {}
        SelectItem::Wildcard(_) => {}
    }
}

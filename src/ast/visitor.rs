// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::ast::{Expr, ObjectName, Statement};
use core::ops::ControlFlow;

/// A type that can be visited by a `visitor`
pub trait Visit {
    fn visit<V: Visitor>(&self, visitor: &mut V) -> ControlFlow<V::Break>;
}

impl<T: Visit> Visit for Option<T> {
    fn visit<V: Visitor>(&self, visitor: &mut V) -> ControlFlow<V::Break> {
        if let Some(s) = self {
            s.visit(visitor)?;
        }
        ControlFlow::Continue(())
    }
}

impl<T: Visit> Visit for Vec<T> {
    fn visit<V: Visitor>(&self, visitor: &mut V) -> ControlFlow<V::Break> {
        for v in self {
            v.visit(visitor)?;
        }
        ControlFlow::Continue(())
    }
}

impl<T: Visit> Visit for Box<T> {
    fn visit<V: Visitor>(&self, visitor: &mut V) -> ControlFlow<V::Break> {
        T::visit(self, visitor)
    }
}

/// A visitor that can be used to walk an AST tree
pub trait Visitor {
    type Break;

    /// Invoked for any tables, virtual or otherwise that appear in the AST
    fn visit_table(&mut self, _table: &ObjectName) -> ControlFlow<Self::Break> {
        ControlFlow::Continue(())
    }

    /// Invoked for any expressions that appear in the AST
    fn visit_expr(&mut self, _expr: &Expr) -> ControlFlow<Self::Break> {
        ControlFlow::Continue(())
    }

    /// Invoked for any statements that appear in the AST
    fn visit_statement(&mut self, _statement: &Statement) -> ControlFlow<Self::Break> {
        ControlFlow::Continue(())
    }
}

struct TableVisitor<F>(F);

impl<E, F: FnMut(&ObjectName) -> ControlFlow<E>> Visitor for TableVisitor<F> {
    type Break = E;

    fn visit_table(&mut self, table: &ObjectName) -> ControlFlow<Self::Break> {
        self.0(table)
    }
}

/// Invokes the provided closure on all tables present in v
pub fn visit_tables<V, E, F>(v: &V, f: F) -> ControlFlow<E>
where
    V: Visit,
    F: FnMut(&ObjectName) -> ControlFlow<E>,
{
    let mut visitor = TableVisitor(f);
    v.visit(&mut visitor)?;
    ControlFlow::Continue(())
}

struct ExprVisitor<F>(F);

impl<E, F: FnMut(&Expr) -> ControlFlow<E>> Visitor for ExprVisitor<F> {
    type Break = E;

    fn visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
        self.0(expr)
    }
}

/// Invokes the provided closure on all expressions present in v
pub fn visit_expressions<V, E, F>(v: &V, f: F) -> ControlFlow<E>
where
    V: Visit,
    F: FnMut(&Expr) -> ControlFlow<E>,
{
    let mut visitor = ExprVisitor(f);
    v.visit(&mut visitor)?;
    ControlFlow::Continue(())
}

struct StatementVisitor<F>(F);

impl<E, F: FnMut(&Statement) -> ControlFlow<E>> Visitor for StatementVisitor<F> {
    type Break = E;

    fn visit_statement(&mut self, statement: &Statement) -> ControlFlow<Self::Break> {
        self.0(statement)
    }
}

/// Invokes the provided closure on all statements present in v
pub fn visit_statements<V, E, F>(v: &V, f: F) -> ControlFlow<E>
where
    V: Visit,
    F: FnMut(&Statement) -> ControlFlow<E>,
{
    let mut visitor = StatementVisitor(f);
    v.visit(&mut visitor)?;
    ControlFlow::Continue(())
}

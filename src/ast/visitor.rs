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

//! Recursive visitors for ast Nodes. See [`Visitor`] for more details.

use crate::ast::{Expr, ObjectName, Statement};
use core::ops::ControlFlow;

/// A type that can be visited by a [`Visitor`]. See [`Visitor`] for
/// recursively visiting parsed SQL statements.
///
/// # Note
///
/// This trait should be automatically derived for sqlparser AST nodes
/// using the [Visit](sqlparser_derive::Visit) proc macro.
///
/// ```text
/// #[cfg_attr(feature = "visitor", derive(Visit))]
/// ```
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

macro_rules! visit_noop {
    ($($t:ty),+) => {
        $(impl Visit for $t {
            fn visit<V: Visitor>(&self, _visitor: &mut V) -> ControlFlow<V::Break> {
               ControlFlow::Continue(())
            }
        })+
    };
}

visit_noop!(u8, u16, u32, u64, i8, i16, i32, i64, char, bool, String);

#[cfg(feature = "bigdecimal")]
visit_noop!(bigdecimal::BigDecimal);

/// A visitor that can be used to walk an AST tree.
///
/// `previst_` methods are invoked before visiting all children of the
/// node and `postvisit_` methods are invoked after visiting all
/// children of the node.
///
/// # See also
///
/// These methods provide a more concise way of visiting nodes of a certain type:
/// * [visit_relations]
/// * [visit_expressions]
/// * [visit_statements]
///
/// # Example
/// ```
/// # use sqlparser::parser::Parser;
/// # use sqlparser::dialect::GenericDialect;
/// # use sqlparser::ast::{Visit, Visitor, ObjectName, Expr};
/// # use core::ops::ControlFlow;
/// // A structure that records statements and relations
/// #[derive(Default)]
/// struct V {
///    visited: Vec<String>,
/// }
///
/// // Visit relations and exprs before children are visited (depth first walk)
/// // Note you can also visit statements and visit exprs after children have been visitoed
/// impl Visitor for V {
///   type Break = ();
///
///   fn pre_visit_relation(&mut self, relation: &ObjectName) -> ControlFlow<Self::Break> {
///     self.visited.push(format!("PRE: RELATION: {}", relation));
///     ControlFlow::Continue(())
///   }
///
///   fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
///     self.visited.push(format!("PRE: EXPR: {}", expr));
///     ControlFlow::Continue(())
///   }
/// }
///
/// let sql = "SELECT a FROM foo where x IN (SELECT y FROM bar)";
/// let statements = Parser::parse_sql(&GenericDialect{}, sql)
///    .unwrap();
///
/// // Drive the visitor through the AST
/// let mut visitor = V::default();
/// statements.visit(&mut visitor);
///
/// // The visitor has visited statements and expressions in pre-traversal order
/// let expected : Vec<_> = [
///   "PRE: EXPR: a",
///   "PRE: RELATION: foo",
///   "PRE: EXPR: x IN (SELECT y FROM bar)",
///   "PRE: EXPR: x",
///   "PRE: EXPR: y",
///   "PRE: RELATION: bar",
/// ]
///   .into_iter().map(|s| s.to_string()).collect();
///
/// assert_eq!(visitor.visited, expected);
/// ```
pub trait Visitor {
    /// Type returned when the recursion returns early.
    type Break;

    /// Invoked for any relations (e.g. tables) that appear in the AST before visiting children
    fn pre_visit_relation(&mut self, _relation: &ObjectName) -> ControlFlow<Self::Break> {
        ControlFlow::Continue(())
    }

    /// Invoked for any relations (e.g. tables) that appear in the AST after visiting children
    fn post_visit_relation(&mut self, _relation: &ObjectName) -> ControlFlow<Self::Break> {
        ControlFlow::Continue(())
    }

    /// Invoked for any expressions that appear in the AST before visiting children
    fn pre_visit_expr(&mut self, _expr: &Expr) -> ControlFlow<Self::Break> {
        ControlFlow::Continue(())
    }

    /// Invoked for any expressions that appear in the AST
    fn post_visit_expr(&mut self, _expr: &Expr) -> ControlFlow<Self::Break> {
        ControlFlow::Continue(())
    }

    /// Invoked for any statements that appear in the AST before visiting children
    fn pre_visit_statement(&mut self, _statement: &Statement) -> ControlFlow<Self::Break> {
        ControlFlow::Continue(())
    }

    /// Invoked for any statements that appear in the AST after visiting children
    fn post_visit_statement(&mut self, _statement: &Statement) -> ControlFlow<Self::Break> {
        ControlFlow::Continue(())
    }
}

struct RelationVisitor<F>(F);

impl<E, F: FnMut(&ObjectName) -> ControlFlow<E>> Visitor for RelationVisitor<F> {
    type Break = E;

    fn pre_visit_relation(&mut self, relation: &ObjectName) -> ControlFlow<Self::Break> {
        self.0(relation)
    }
}

/// Invokes the provided closure on all relations (e.g. table names) present in `v`
///
/// # Example
/// ```
/// # use sqlparser::parser::Parser;
/// # use sqlparser::dialect::GenericDialect;
/// # use sqlparser::ast::{visit_relations};
/// # use core::ops::ControlFlow;
/// let sql = "SELECT a FROM foo where x IN (SELECT y FROM bar)";
/// let statements = Parser::parse_sql(&GenericDialect{}, sql)
///    .unwrap();
///
/// // visit statements, capturing relations (table names)
/// let mut visited = vec![];
/// visit_relations(&statements, |relation| {
///   visited.push(format!("RELATION: {}", relation));
///   ControlFlow::<()>::Continue(())
/// });
///
/// let expected : Vec<_> = [
///   "RELATION: foo",
///   "RELATION: bar",
/// ]
///   .into_iter().map(|s| s.to_string()).collect();
///
/// assert_eq!(visited, expected);
/// ```
pub fn visit_relations<V, E, F>(v: &V, f: F) -> ControlFlow<E>
where
    V: Visit,
    F: FnMut(&ObjectName) -> ControlFlow<E>,
{
    let mut visitor = RelationVisitor(f);
    v.visit(&mut visitor)?;
    ControlFlow::Continue(())
}

struct ExprVisitor<F>(F);

impl<E, F: FnMut(&Expr) -> ControlFlow<E>> Visitor for ExprVisitor<F> {
    type Break = E;

    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
        self.0(expr)
    }
}

/// Invokes the provided closure on all expressions (e.g. `1 + 2`) present in `v`
///
/// # Example
/// ```
/// # use sqlparser::parser::Parser;
/// # use sqlparser::dialect::GenericDialect;
/// # use sqlparser::ast::{visit_expressions};
/// # use core::ops::ControlFlow;
/// let sql = "SELECT a FROM foo where x IN (SELECT y FROM bar)";
/// let statements = Parser::parse_sql(&GenericDialect{}, sql)
///    .unwrap();
///
/// // visit all expressions
/// let mut visited = vec![];
/// visit_expressions(&statements, |expr| {
///   visited.push(format!("EXPR: {}", expr));
///   ControlFlow::<()>::Continue(())
/// });
///
/// let expected : Vec<_> = [
///   "EXPR: a",
///   "EXPR: x IN (SELECT y FROM bar)",
///   "EXPR: x",
///   "EXPR: y",
/// ]
///   .into_iter().map(|s| s.to_string()).collect();
///
/// assert_eq!(visited, expected);
/// ```
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

    fn pre_visit_statement(&mut self, statement: &Statement) -> ControlFlow<Self::Break> {
        self.0(statement)
    }
}

/// Invokes the provided closure on all statements (e.g. `SELECT`, `CREATE TABLE`, etc) present in `v`
///
/// # Example
/// ```
/// # use sqlparser::parser::Parser;
/// # use sqlparser::dialect::GenericDialect;
/// # use sqlparser::ast::{visit_statements};
/// # use core::ops::ControlFlow;
/// let sql = "SELECT a FROM foo where x IN (SELECT y FROM bar); CREATE TABLE baz(q int)";
/// let statements = Parser::parse_sql(&GenericDialect{}, sql)
///    .unwrap();
///
/// // visit all statements
/// let mut visited = vec![];
/// visit_statements(&statements, |stmt| {
///   visited.push(format!("STATEMENT: {}", stmt));
///   ControlFlow::<()>::Continue(())
/// });
///
/// let expected : Vec<_> = [
///   "STATEMENT: SELECT a FROM foo WHERE x IN (SELECT y FROM bar)",
///   "STATEMENT: CREATE TABLE baz (q INT)"
/// ]
///   .into_iter().map(|s| s.to_string()).collect();
///
/// assert_eq!(visited, expected);
/// ```
pub fn visit_statements<V, E, F>(v: &V, f: F) -> ControlFlow<E>
where
    V: Visit,
    F: FnMut(&Statement) -> ControlFlow<E>,
{
    let mut visitor = StatementVisitor(f);
    v.visit(&mut visitor)?;
    ControlFlow::Continue(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dialect::GenericDialect;
    use crate::parser::Parser;
    use crate::tokenizer::Tokenizer;

    #[derive(Default)]
    struct TestVisitor {
        visited: Vec<String>,
    }

    impl Visitor for TestVisitor {
        type Break = ();

        fn pre_visit_relation(&mut self, relation: &ObjectName) -> ControlFlow<Self::Break> {
            self.visited.push(format!("PRE: RELATION: {}", relation));
            ControlFlow::Continue(())
        }

        fn post_visit_relation(&mut self, relation: &ObjectName) -> ControlFlow<Self::Break> {
            self.visited.push(format!("POST: RELATION: {}", relation));
            ControlFlow::Continue(())
        }

        fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
            self.visited.push(format!("PRE: EXPR: {}", expr));
            ControlFlow::Continue(())
        }

        fn post_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
            self.visited.push(format!("POST: EXPR: {}", expr));
            ControlFlow::Continue(())
        }

        fn pre_visit_statement(&mut self, statement: &Statement) -> ControlFlow<Self::Break> {
            self.visited.push(format!("PRE: STATEMENT: {}", statement));
            ControlFlow::Continue(())
        }

        fn post_visit_statement(&mut self, statement: &Statement) -> ControlFlow<Self::Break> {
            self.visited.push(format!("POST: STATEMENT: {}", statement));
            ControlFlow::Continue(())
        }
    }

    fn do_visit(sql: &str) -> Vec<String> {
        let dialect = GenericDialect {};
        let mut tokenizer = Tokenizer::new(&dialect, sql);
        let tokens = tokenizer.tokenize().unwrap();
        let s = Parser::new(&dialect)
            .with_tokens(tokens)
            .parse_statement()
            .unwrap();

        let mut visitor = TestVisitor::default();
        s.visit(&mut visitor);
        visitor.visited
    }

    #[test]
    fn test_sql() {
        let tests = vec![
            (
                "SELECT * from table_name",
                vec![
                    "PRE: STATEMENT: SELECT * FROM table_name",
                    "PRE: RELATION: table_name",
                    "POST: RELATION: table_name",
                    "POST: STATEMENT: SELECT * FROM table_name",
                ],
            ),
            (
                "SELECT * from t1 join t2 on t1.id = t2.t1_id",
                vec![
                    "PRE: STATEMENT: SELECT * FROM t1 JOIN t2 ON t1.id = t2.t1_id",
                    "PRE: RELATION: t1",
                    "POST: RELATION: t1",
                    "PRE: RELATION: t2",
                    "POST: RELATION: t2",
                    "PRE: EXPR: t1.id = t2.t1_id",
                    "PRE: EXPR: t1.id",
                    "POST: EXPR: t1.id",
                    "PRE: EXPR: t2.t1_id",
                    "POST: EXPR: t2.t1_id",
                    "POST: EXPR: t1.id = t2.t1_id",
                    "POST: STATEMENT: SELECT * FROM t1 JOIN t2 ON t1.id = t2.t1_id",
                ],
            ),
            (
                "SELECT * from t1 where EXISTS(SELECT column from t2)",
                vec![
                    "PRE: STATEMENT: SELECT * FROM t1 WHERE EXISTS (SELECT column FROM t2)",
                    "PRE: RELATION: t1",
                    "POST: RELATION: t1",
                    "PRE: EXPR: EXISTS (SELECT column FROM t2)",
                    "PRE: EXPR: column",
                    "POST: EXPR: column",
                    "PRE: RELATION: t2",
                    "POST: RELATION: t2",
                    "POST: EXPR: EXISTS (SELECT column FROM t2)",
                    "POST: STATEMENT: SELECT * FROM t1 WHERE EXISTS (SELECT column FROM t2)",
                ],
            ),
            (
                "SELECT * from t1 where EXISTS(SELECT column from t2)",
                vec![
                    "PRE: STATEMENT: SELECT * FROM t1 WHERE EXISTS (SELECT column FROM t2)",
                    "PRE: RELATION: t1",
                    "POST: RELATION: t1",
                    "PRE: EXPR: EXISTS (SELECT column FROM t2)",
                    "PRE: EXPR: column",
                    "POST: EXPR: column",
                    "PRE: RELATION: t2",
                    "POST: RELATION: t2",
                    "POST: EXPR: EXISTS (SELECT column FROM t2)",
                    "POST: STATEMENT: SELECT * FROM t1 WHERE EXISTS (SELECT column FROM t2)",
                ],
            ),
            (
                "SELECT * from t1 where EXISTS(SELECT column from t2) UNION SELECT * from t3",
                vec![
                    "PRE: STATEMENT: SELECT * FROM t1 WHERE EXISTS (SELECT column FROM t2) UNION SELECT * FROM t3",
                    "PRE: RELATION: t1",
                    "POST: RELATION: t1",
                    "PRE: EXPR: EXISTS (SELECT column FROM t2)",
                    "PRE: EXPR: column",
                    "POST: EXPR: column",
                    "PRE: RELATION: t2",
                    "POST: RELATION: t2",
                    "POST: EXPR: EXISTS (SELECT column FROM t2)",
                    "PRE: RELATION: t3",
                    "POST: RELATION: t3",
                    "POST: STATEMENT: SELECT * FROM t1 WHERE EXISTS (SELECT column FROM t2) UNION SELECT * FROM t3",
                ],
            ),
        ];
        for (sql, expected) in tests {
            let actual = do_visit(sql);
            let actual: Vec<_> = actual.iter().map(|x| x.as_str()).collect();
            assert_eq!(actual, expected)
        }
    }
}

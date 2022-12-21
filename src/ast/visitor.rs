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

/// A visitor that can be used to walk an AST tree
pub trait Visitor {
    type Break;

    /// Invoked for any tables, virtual or otherwise that appear in the AST
    fn visit_relation(&mut self, _relation: &ObjectName) -> ControlFlow<Self::Break> {
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

struct RelationVisitor<F>(F);

impl<E, F: FnMut(&ObjectName) -> ControlFlow<E>> Visitor for RelationVisitor<F> {
    type Break = E;

    fn visit_relation(&mut self, relation: &ObjectName) -> ControlFlow<Self::Break> {
        self.0(relation)
    }
}

/// Invokes the provided closure on all relations present in v
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

        fn visit_relation(&mut self, relation: &ObjectName) -> ControlFlow<Self::Break> {
            self.visited.push(format!("RELATION: {}", relation));
            ControlFlow::Continue(())
        }

        fn visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
            self.visited.push(format!("EXPR: {}", expr));
            ControlFlow::Continue(())
        }

        fn visit_statement(&mut self, statement: &Statement) -> ControlFlow<Self::Break> {
            self.visited.push(format!("STATEMENT: {}", statement));
            ControlFlow::Continue(())
        }
    }

    fn do_visit(sql: &str) -> Vec<String> {
        let dialect = GenericDialect {};
        let mut tokenizer = Tokenizer::new(&dialect, sql);
        let tokens = tokenizer.tokenize().unwrap();
        let mut parser = Parser::new(tokens, &dialect);
        let s = parser.parse_statement().unwrap();

        let mut visitor = TestVisitor::default();
        s.visit(&mut visitor);
        visitor.visited
    }

    #[test]
    fn test_sql() {
        let tests = vec![
            (
                "SELECT * from table_name",
                vec!["STATEMENT: SELECT * FROM table_name", "RELATION: table_name"],
            ),
            (
                "SELECT * from t1 join t2 on t1.id = t2.t1_id",
                vec![
                    "STATEMENT: SELECT * FROM t1 JOIN t2 ON t1.id = t2.t1_id",
                    "RELATION: t1",
                    "RELATION: t2",
                    "EXPR: t1.id = t2.t1_id",
                    "EXPR: t1.id",
                    "EXPR: t2.t1_id",
                ],
            ),
            (
                "SELECT * from t1 where EXISTS(SELECT column from t2)",
                vec![
                    "STATEMENT: SELECT * FROM t1 WHERE EXISTS (SELECT column FROM t2)",
                    "RELATION: t1",
                    "EXPR: EXISTS (SELECT column FROM t2)",
                    "EXPR: column",
                    "RELATION: t2",
                ],
            ),
            (
                "SELECT * from t1 where EXISTS(SELECT column from t2)",
                vec![
                    "STATEMENT: SELECT * FROM t1 WHERE EXISTS (SELECT column FROM t2)",
                    "RELATION: t1",
                    "EXPR: EXISTS (SELECT column FROM t2)",
                    "EXPR: column",
                    "RELATION: t2",
                ],
            ),
            (
                "SELECT * from t1 where EXISTS(SELECT column from t2) UNION SELECT * from t3",
                vec![
                    "STATEMENT: SELECT * FROM t1 WHERE EXISTS (SELECT column FROM t2) UNION SELECT * FROM t3",
                    "RELATION: t1",
                    "EXPR: EXISTS (SELECT column FROM t2)",
                    "EXPR: column",
                    "RELATION: t2",
                    "RELATION: t3",
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

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

//! Unparenthesized operator chains for the expression shape targets.

use libfuzzer_sys::arbitrary::{self, Arbitrary};
use sqlparser::ast::{visit_expressions_mut, Expr, Statement};
use std::fmt::Write;
use std::ops::ControlFlow;

const MAX_DEPTH: usize = 6;

/// The tokens one dialect's chains are built from.
pub struct Grammar {
    pub atoms: &'static [&'static str],
    pub prefix: &'static [&'static str],
    pub infix: &'static [&'static str],
    pub postfix: &'static [&'static str],
    pub functions: &'static [&'static str],
    pub types: &'static [&'static str],
    /// Opening and closing text of an array constructor.
    pub array: (&'static str, &'static str),
    /// Statements a chain is placed in, with `{}` where the chain goes.
    pub contexts: &'static [&'static str],
}

/// A chain placed in one of the grammar's statement contexts.
#[derive(Arbitrary, Debug)]
pub struct Case {
    context: u8,
    chain: Chain,
}

impl Case {
    /// Writes the statement as SQL text in the tokens of `grammar`.
    pub fn render(&self, grammar: &Grammar) -> String {
        pick(grammar.contexts, self.context).replacen("{}", &self.chain.render(grammar), 1)
    }
}

/// An expression written as operands joined by operators, with no parentheses unless the input asks for a group.
#[derive(Arbitrary, Debug)]
pub struct Chain {
    first: Operand,
    rest: Vec<(Infix, Operand)>,
}

#[derive(Arbitrary, Debug)]
struct Infix {
    op: u8,
    tight: bool,
}

#[derive(Arbitrary, Debug)]
struct Operand {
    prefix: Vec<u8>,
    primary: Primary,
    postfix: Vec<u8>,
    tight: bool,
}

#[derive(Arbitrary, Debug)]
enum Primary {
    Atom(u8),
    Group(Box<Chain>),
    List(Box<Chain>, Vec<Chain>),
    Call(u8, Vec<Chain>),
    Cast(Box<Chain>, u8),
    Case {
        operand: Option<Box<Chain>>,
        when: Box<(Chain, Chain)>,
        more: Vec<(Chain, Chain)>,
        otherwise: Option<Box<Chain>>,
    },
    Scalar(Box<Chain>),
    Array(Vec<Chain>),
}

fn pick(table: &'static [&'static str], index: u8) -> &'static str {
    table[usize::from(index) % table.len()]
}

impl Chain {
    /// Writes the chain as SQL text in the tokens of `grammar`.
    pub fn render(&self, grammar: &Grammar) -> String {
        let mut out = String::new();
        self.write(grammar, 0, &mut out);
        out
    }

    fn write(&self, grammar: &Grammar, depth: usize, out: &mut String) {
        self.first.write(grammar, depth, out);
        for (infix, operand) in &self.rest {
            let gap = if infix.tight { "" } else { " " };
            let _ = write!(out, "{gap}{}{gap}", pick(grammar.infix, infix.op));
            operand.write(grammar, depth, out);
        }
    }
}

impl Operand {
    fn write(&self, grammar: &Grammar, depth: usize, out: &mut String) {
        let gap = if self.tight { "" } else { " " };
        for &op in &self.prefix {
            let _ = write!(out, "{}{gap}", pick(grammar.prefix, op));
        }
        self.primary.write(grammar, depth, out);
        for &op in &self.postfix {
            let _ = write!(out, "{gap}{}", pick(grammar.postfix, op));
        }
    }
}

fn write_list<'a>(
    chains: impl IntoIterator<Item = &'a Chain>,
    grammar: &Grammar,
    depth: usize,
    out: &mut String,
) {
    for (i, chain) in chains.into_iter().enumerate() {
        if i > 0 {
            out.push_str(", ");
        }
        chain.write(grammar, depth, out);
    }
}

impl Primary {
    fn write(&self, grammar: &Grammar, depth: usize, out: &mut String) {
        let inner = depth + 1;
        if inner > MAX_DEPTH && !matches!(self, Primary::Atom(_)) {
            out.push_str(grammar.atoms[0]);
            return;
        }
        match self {
            Primary::Atom(i) => out.push_str(pick(grammar.atoms, *i)),
            Primary::Group(chain) => {
                out.push('(');
                chain.write(grammar, inner, out);
                out.push(')');
            }
            Primary::List(first, rest) => {
                out.push('(');
                write_list(std::iter::once(&**first).chain(rest), grammar, inner, out);
                out.push(')');
            }
            Primary::Call(name, args) => {
                let _ = write!(out, "{}(", pick(grammar.functions, *name));
                write_list(args, grammar, inner, out);
                out.push(')');
            }
            Primary::Cast(chain, ty) => {
                out.push_str("CAST(");
                chain.write(grammar, inner, out);
                let _ = write!(out, " AS {})", pick(grammar.types, *ty));
            }
            Primary::Case {
                operand,
                when,
                more,
                otherwise,
            } => {
                out.push_str("CASE");
                if let Some(operand) = operand {
                    out.push(' ');
                    operand.write(grammar, inner, out);
                }
                for (condition, result) in std::iter::once(&**when).chain(more) {
                    out.push_str(" WHEN ");
                    condition.write(grammar, inner, out);
                    out.push_str(" THEN ");
                    result.write(grammar, inner, out);
                }
                if let Some(otherwise) = otherwise {
                    out.push_str(" ELSE ");
                    otherwise.write(grammar, inner, out);
                }
                out.push_str(" END");
            }
            Primary::Scalar(chain) => {
                out.push_str("(SELECT ");
                chain.write(grammar, inner, out);
                out.push(')');
            }
            Primary::Array(elements) => {
                out.push_str(grammar.array.0);
                write_list(elements, grammar, inner, out);
                out.push_str(grammar.array.1);
            }
        }
    }
}

/// Wraps every compound expression of `statement` in parentheses, so its text spells out the tree sqlparser built.
pub fn parenthesize(statement: &mut Statement) {
    let _ = visit_expressions_mut(statement, |expr| {
        // These already print inside their own parentheses or are single tokens.
        if !matches!(
            expr,
            Expr::Value(_)
                | Expr::Identifier(_)
                | Expr::CompoundIdentifier(_)
                | Expr::Nested(_)
                | Expr::Subquery(_)
                | Expr::Tuple(_)
        ) {
            let inner = std::mem::replace(expr, Expr::value(sqlparser::ast::Value::Null));
            *expr = Expr::Nested(Box::new(inner));
        }
        ControlFlow::<()>::Continue(())
    });
}

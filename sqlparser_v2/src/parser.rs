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

//! The token cursor and the Pratt expression loop.
//!
//! This module holds no grammar. Rules live in [`crate::grammar`] and are
//! always invoked through the [`Dialect`] trait so that a dialect's override
//! applies wherever the rule is used.

use std::borrow::Cow;
use std::marker::PhantomData;

use crate::ast::{
    BinaryOp, Expr, Ident, ObjectName, SelectItem, SetOperator, Span, Statement, TableRef,
};
use crate::dialect::Dialect;
use crate::lexer::{Token, TokenKind, tokenize};
use crate::{PResult, ParseError};

/// The default nesting allowed before parsing fails, protecting the stack.
pub const DEFAULT_MAX_NESTING_DEPTH: usize = 64;

/// The default limit on the height of a parsed tree.
///
/// Long operator chains parse in a loop, so they never reach
/// [`DEFAULT_MAX_NESTING_DEPTH`], but they build trees whose destructor and derived
/// traits recurse once per level. This keeps those safe on a 2 MiB stack in a
/// debug build.
/// Debug-build frames grow with the number of `Expr` variants, so this margin
/// shrinks as the grammar grows. `tests/tree_limits.rs` fails when it runs out.
const DEFAULT_MAX_TREE_HEIGHT: usize = 1_200;

/// Parses the operator at the current token, which has not been consumed.
/// The final argument is the operator's binding power.
pub type InfixParseFn<'a, D> = fn(&mut Parser<'a, D>, ExprOf<'a, D>, u8) -> PResult<ExprOf<'a, D>>;

type ExprOf<'a, D> = Expr<'a, <D as Dialect>::Ext>;

pub enum InfixAction<'a, D: Dialect> {
    /// A left-associative operator producing [`Binary`](crate::ast::ExprKind::Binary).
    Binary(BinaryOp<'a>),
    Custom(InfixParseFn<'a, D>),
}

/// How to parse the infix operator at the current token. The binding power
/// and the action are returned together so that they cannot disagree.
pub struct InfixRule<'a, D: Dialect> {
    pub bp: u8,
    pub action: InfixAction<'a, D>,
}

impl<'a, D: Dialect> InfixRule<'a, D> {
    pub fn binary(bp: u8, op: BinaryOp<'a>) -> Self {
        Self {
            bp,
            action: InfixAction::Binary(op),
        }
    }

    pub fn custom(bp: u8, parse: InfixParseFn<'a, D>) -> Self {
        Self {
            bp,
            action: InfixAction::Custom(parse),
        }
    }
}

/// A set operator at the current token and its binding power.
pub struct SetOperatorRule {
    pub bp: u8,
    pub op: SetOperator,
}

pub struct Parser<'a, D: Dialect> {
    source: &'a str,
    tokens: Vec<Token<'a>>,
    pos: usize,
    depth: usize,
    max_nesting_depth: usize,
    /// Height of the tree built so far at the current nesting level.
    height: usize,
    max_tree_height: usize,
    /// Flags set by [`Parser::with_context`].
    context: u32,
    dialect: PhantomData<D>,
}

/// Runs a rule and records the span of the node it builds. A macro, because a
/// function would add a stack frame to every level of recursion.
macro_rules! spanned {
    ($parser:ident, $rule:expr) => {{
        let start = $parser.peek().span.start;
        spanned!($parser, start, $rule)
    }};
    ($parser:ident, $start:expr, $rule:expr) => {{
        let mut node = $rule?;
        node.span = $parser.span_since($start);
        Ok(node)
    }};
}

impl<'a, D: Dialect> Parser<'a, D> {
    /// True if `flag` is set by an enclosing [`Parser::with_context`].
    pub fn has_context(&self, flag: u32) -> bool {
        self.context & flag != 0
    }

    /// Runs `rule` with `flag` set, for syntax that means something different
    /// inside a particular clause.
    pub fn with_context<T>(
        &mut self,
        flag: u32,
        rule: impl FnOnce(&mut Self) -> PResult<T>,
    ) -> PResult<T> {
        let outer = self.context;
        self.context |= flag;
        let result = rule(self);
        self.context = outer;
        result
    }

    /// Runs `rule` with `flag` cleared, for syntax where a clause's meaning
    /// stops applying, such as inside parentheses.
    pub fn without_context<T>(
        &mut self,
        flag: u32,
        rule: impl FnOnce(&mut Self) -> PResult<T>,
    ) -> PResult<T> {
        let outer = self.context;
        self.context &= !flag;
        let result = rule(self);
        self.context = outer;
        result
    }

    pub fn new(sql: &'a str) -> PResult<Self> {
        Ok(Self {
            source: sql,
            tokens: tokenize::<D>(sql)?,
            pos: 0,
            depth: 0,
            max_nesting_depth: DEFAULT_MAX_NESTING_DEPTH,
            height: 0,
            max_tree_height: DEFAULT_MAX_TREE_HEIGHT,
            context: 0,
            dialect: PhantomData,
        })
    }

    /// Sets the tallest tree this parser will build. Raising it above the
    /// default is only safe where every thread that drops, clones, compares
    /// or walks the tree has a correspondingly larger stack.
    pub fn with_max_tree_height(mut self, max_tree_height: usize) -> Self {
        self.max_tree_height = max_tree_height;
        self
    }

    /// Sets how deeply rules may nest, such as parenthesized expressions and
    /// subqueries, before parsing fails. Each level uses stack, so raising it
    /// above the default is only safe on a correspondingly larger stack.
    pub fn with_max_nesting_depth(mut self, max_nesting_depth: usize) -> Self {
        self.max_nesting_depth = max_nesting_depth;
        self
    }

    /// How deeply rules may nest; see [`Parser::with_max_nesting_depth`].
    pub fn max_nesting_depth(&self) -> usize {
        self.max_nesting_depth
    }

    /// The SQL being parsed, for a rule that joins adjacent tokens into one
    /// slice.
    pub fn source(&self) -> &'a str {
        self.source
    }

    pub fn peek(&self) -> Token<'a> {
        self.tokens[self.pos]
    }

    /// The source text of `span`, which may cover several tokens.
    pub fn text(&self, span: crate::ast::Span) -> &'a str {
        &self.source[span.start as usize..span.end as usize]
    }

    /// The token `n` places after the current one, or the final
    /// [`TokenKind::Eof`] token.
    pub fn peek_nth(&self, n: usize) -> Token<'a> {
        self.tokens[(self.pos + n).min(self.tokens.len() - 1)]
    }

    /// The whitespace and comments just before the token `n` places after
    /// the current one. The lexer drops these, so this is the only way for a
    /// dialect to give meaning to a comment.
    pub fn gap_before_nth(&self, n: usize) -> &'a str {
        let index = (self.pos + n).min(self.tokens.len() - 1);
        let start = match index {
            0 => 0,
            _ => self.tokens[index - 1].span.end as usize,
        };
        &self.source[start..self.tokens[index].span.start as usize]
    }

    /// Returns the current token and moves past it. Stays on the final
    /// [`TokenKind::Eof`] token once it is reached.
    pub fn advance(&mut self) -> Token<'a> {
        let token = self.peek();
        if token.kind != TokenKind::Eof {
            self.pos += 1;
        }
        token
    }

    pub fn at_keyword(&self, keyword: &str) -> bool {
        let token = self.peek();
        token.kind == TokenKind::Word && token.text.eq_ignore_ascii_case(keyword)
    }

    pub fn eat_keyword(&mut self, keyword: &str) -> bool {
        let found = self.at_keyword(keyword);
        if found {
            self.advance();
        }
        found
    }

    pub fn expect_keyword(&mut self, keyword: &str) -> PResult<()> {
        if self.eat_keyword(keyword) {
            Ok(())
        } else {
            Err(self.expected(keyword))
        }
    }

    /// Expects each of `keywords` in turn.
    pub fn expect_keywords(&mut self, keywords: &[&str]) -> PResult<()> {
        keywords
            .iter()
            .try_for_each(|word| self.expect_keyword(word))
    }

    /// Consumes a token of `kind`, or fails naming `what` was expected.
    pub fn expect_kind(&mut self, kind: TokenKind, what: &str) -> PResult<Token<'a>> {
        let token = self.peek();
        if token.kind != kind {
            return Err(self.expected(what));
        }
        self.advance();
        Ok(token)
    }

    pub fn at_op(&self, op: &str) -> bool {
        let token = self.peek();
        token.kind == TokenKind::Op && token.text == op
    }

    pub fn eat_op(&mut self, op: &str) -> bool {
        let found = self.at_op(op);
        if found {
            self.advance();
        }
        found
    }

    pub fn expect_op(&mut self, op: &str) -> PResult<()> {
        if self.eat_op(op) {
            Ok(())
        } else {
            Err(self.expected(op))
        }
    }

    /// Consumes the `>` that closes a type's angle brackets. When `>>` closes
    /// two of them at once, as in `ARRAY<ARRAY<INT>>`, it consumes one `>` and
    /// leaves the other.
    pub fn expect_angle_close(&mut self) -> PResult<()> {
        let token = self.peek();
        if token.is_op(">") {
            self.advance();
            return Ok(());
        }
        if token.kind != TokenKind::Op || !token.text.starts_with('>') || token.text.len() < 2 {
            return Err(self.expected(">"));
        }
        self.tokens[self.pos] = Token {
            kind: TokenKind::Op,
            text: &token.text[1..],
            span: crate::ast::Span {
                start: token.span.start + 1,
                end: token.span.end,
            },
        };
        Ok(())
    }

    pub fn at_eof(&self) -> bool {
        self.peek().kind == TokenKind::Eof
    }

    pub fn expect_eof(&self) -> PResult<()> {
        if self.at_eof() {
            Ok(())
        } else {
            Err(self.expected("end of input"))
        }
    }

    /// An error saying that the SQL nests more deeply than `limit`.
    fn too_deep(&self, limit: usize) -> ParseError {
        ParseError {
            message: format!("SQL is nested too deeply (the limit is {limit})"),
            span: self.peek().span,
        }
    }

    /// An error saying that `what` was expected at the current token.
    pub fn expected(&self, what: &str) -> ParseError {
        let token = self.peek();
        let found = match token.kind {
            TokenKind::Eof => "end of input",
            _ => token.text,
        };
        ParseError {
            message: format!("expected {what}, found {found}"),
            span: token.span,
        }
    }

    /// True if the current token can be an alias without a preceding `AS`.
    pub fn at_implicit_alias(&self) -> bool {
        let token = self.peek();
        self.at_ident() && !(token.kind == TokenKind::Word && D::is_reserved_for_alias(token.text))
    }

    /// True if the current token can be used as an identifier.
    pub fn at_ident(&self) -> bool {
        let token = self.peek();
        match token.kind {
            TokenKind::QuotedIdent => true,
            TokenKind::Word => !D::is_reserved(token.text),
            _ => false,
        }
    }

    pub fn parse_ident(&mut self) -> PResult<Ident<'a>> {
        if !self.at_ident() {
            return Err(self.expected("identifier"));
        }
        let token = self.advance();
        Ok(match token.kind {
            TokenKind::QuotedIdent => Ident {
                text: Cow::Borrowed(&token.text[1..token.text.len() - 1]),
                quote: token.text.chars().next(),
                span: token.span,
            },
            _ => Ident {
                text: token.text.into(),
                quote: None,
                span: token.span,
            },
        })
    }

    /// Parses a name after a dot, where reserved words are allowed.
    pub fn parse_name_part(&mut self) -> PResult<Ident<'a>> {
        D::parse_name_part(self)
    }

    pub fn parse_expr(&mut self) -> PResult<Expr<'a, D::Ext>> {
        self.parse_expr_bp(0)
    }

    /// Parses an expression containing only operators that bind tighter than
    /// `min_bp`.
    pub fn parse_expr_bp(&mut self, min_bp: u8) -> PResult<Expr<'a, D::Ext>> {
        self.nested(|p| p.pratt_loop(min_bp))
    }

    /// Runs a rule that can recurse, failing if nesting gets too deep.
    /// Every recursive path through the grammar must pass through this.
    pub fn nested<T>(&mut self, rule: impl FnOnce(&mut Self) -> PResult<T>) -> PResult<T> {
        if self.depth >= self.max_nesting_depth {
            return Err(self.too_deep(self.max_nesting_depth));
        }
        self.depth += 1;
        let outer_height = self.height;
        self.height = 0;
        let result = rule(self);
        self.depth -= 1;
        // Siblings do not add up: the tree is as tall as its tallest child.
        self.height = outer_height.max(self.height + 1);
        if result.is_ok() {
            self.check_height()?;
        }
        result
    }

    /// Records that a loop added a level to the tree, failing if it is now
    /// too tall. Every loop that builds a left-deep tree must call this.
    pub fn deepen(&mut self) -> PResult<()> {
        self.deepen_by(1)
    }

    /// Like [`Parser::deepen`], for a node that takes more stack to clone or
    /// render than one ordinary level of the tree.
    pub fn deepen_by(&mut self, levels: usize) -> PResult<()> {
        self.height += levels;
        self.check_height()
    }

    fn check_height(&self) -> PResult<()> {
        if self.height > self.max_tree_height {
            return Err(self.too_deep(self.max_tree_height));
        }
        Ok(())
    }

    /// Parses one or more items separated by commas.
    pub fn parse_comma_separated<T>(
        &mut self,
        mut item: impl FnMut(&mut Self) -> PResult<T>,
    ) -> PResult<Vec<T>> {
        let mut items = vec![item(self)?];
        while self.eat_op(",") {
            items.push(item(self)?);
        }
        Ok(items)
    }

    fn pratt_loop(&mut self, min_bp: u8) -> PResult<Expr<'a, D::Ext>> {
        let start = self.peek().span.start;
        let mut left = D::parse_prefix(self)?;
        left.span = self.span_since(start);
        while let Some(rule) = D::infix_rule(self) {
            if rule.bp <= min_bp {
                break;
            }
            self.deepen()?;
            left = match rule.action {
                InfixAction::Binary(op) => {
                    self.advance();
                    crate::grammar::parse_binary_right(self, left, op, rule.bp)?
                }
                InfixAction::Custom(parse) => {
                    // Dialect nodes pass through wrapper types, so each level of
                    // them takes more stack than a core node.
                    self.deepen()?;
                    parse(self, left, rule.bp)?
                }
            };
            left.span = self.span_since(start);
        }
        Ok(left)
    }

    /// Parses a prefix expression alone, recording its span.
    pub fn parse_prefix(&mut self) -> PResult<Expr<'a, D::Ext>> {
        spanned!(self, D::parse_prefix(self))
    }

    /// Parses a statement, recording its span.
    pub fn parse_statement(&mut self) -> PResult<Statement<'a, D::Ext>> {
        spanned!(self, D::parse_statement(self))
    }

    /// Runs `rule`, recording the span of the expression it builds. Only rules
    /// that are reached without going through [`Parser::parse_expr`] need this.
    pub fn spanned_expr(
        &mut self,
        rule: impl FnOnce(&mut Self) -> PResult<Expr<'a, D::Ext>>,
    ) -> PResult<Expr<'a, D::Ext>> {
        spanned!(self, rule(self))
    }

    /// Parses a function call after its `name`, recording its span.
    pub fn parse_function_call(&mut self, name: ObjectName<'a>) -> PResult<Expr<'a, D::Ext>> {
        let start = name.parts[0].span.start;
        spanned!(self, start, D::parse_function_call(self, name))
    }

    /// Parses a select item, recording its span.
    pub fn parse_select_item(&mut self) -> PResult<SelectItem<'a, D::Ext>> {
        spanned!(self, D::parse_select_item(self))
    }

    /// Parses a table reference, recording its span.
    pub fn parse_table_ref(&mut self) -> PResult<TableRef<'a, D::Ext>> {
        spanned!(self, D::parse_table_ref(self))
    }

    /// The span from byte offset `start` to the end of the last token read.
    pub fn span_since(&self, start: u32) -> Span {
        let end = match self.pos {
            0 => start,
            pos => self.tokens[pos - 1].span.end,
        };
        Span {
            start,
            end: end.max(start),
        }
    }
}

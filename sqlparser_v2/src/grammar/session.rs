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

//! Rules for `SHOW`, transactions, `USE`, `EXPLAIN`, `SET`, `COMMENT` and `CALL`.

use super::*;
use crate::PResult;
use crate::ast::ExprKind;
use crate::ast::Span;
use crate::ast::StatementKind;
use crate::dialect::Dialect;
use crate::lexer::{Token, TokenKind};
use crate::parser::Parser;
use std::borrow::Cow;

fn at_any<'a, D: Dialect>(p: &Parser<'a, D>, words: &[&str]) -> bool {
    words.iter().any(|word| p.at_keyword(word))
}

fn first_of<'a, D: Dialect>(p: &mut Parser<'a, D>, words: &[&'static str]) -> Option<&'static str> {
    words.iter().copied().find(|word| p.eat_keyword(word))
}

pub fn parse_string<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<StringLiteral<'a>> {
    let token = p.expect_kind(TokenKind::String, "a string")?;
    Ok(D::string_literal(token))
}

fn at_statement_end<'a, D: Dialect>(p: &Parser<'a, D>, n: usize) -> bool {
    let token = p.peek_nth(n);
    token.kind == TokenKind::Eof || token.is_op(";")
}

// SHOW

const SHOW_CLAUSES: [&str; 7] = ["IN", "FROM", "LIKE", "WHERE", "STARTS", "LIMIT", "HISTORY"];

fn at_show_clause<'a, D: Dialect>(p: &Parser<'a, D>) -> bool {
    at_any(p, &SHOW_CLAUSES)
}

pub fn parse_show<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Show<'a, D::Ext>> {
    p.expect_keyword("SHOW")?;
    let mut modifiers = Vec::new();
    while at_any(p, &["TERSE", "EXTENDED", "FULL", "GLOBAL", "SESSION"])
        && p.peek_nth(1).kind == TokenKind::Word
    {
        modifiers.push(p.peek().text);
        p.advance();
    }

    let mut object = Vec::new();
    let mut name = None;
    if p.at_keyword("CREATE") {
        object.push(p.peek().text);
        p.advance();
        if p.peek().kind != TokenKind::Word {
            return Err(p.expected("the kind of object"));
        }
        object.push(p.peek().text);
        p.advance();
        name = Some(parse_object_name(p)?);
    } else {
        while p.peek().kind == TokenKind::Word && !at_show_clause(p) {
            let word = p.peek().text;
            object.push(if word.eq_ignore_ascii_case("FIELDS") {
                "COLUMNS"
            } else {
                word
            });
            p.advance();
        }
        if object.is_empty() {
            return Err(p.expected("what to show"));
        }
    }

    // These two only qualify listings of tables and columns.
    let detailed = modifiers
        .iter()
        .any(|word| word.eq_ignore_ascii_case("EXTENDED") || word.eq_ignore_ascii_case("FULL"));
    let detailable = ["TABLES", "COLUMNS", "PROCESSLIST"]
        .iter()
        .any(|kind| object[0].eq_ignore_ascii_case(kind));
    if detailed && !detailable {
        return Err(p.expected("TABLES or COLUMNS"));
    }

    let mut clauses = Vec::new();
    while let Some(clause) = parse_show_clause(p)? {
        clauses.push(clause);
    }
    Ok(Show {
        modifiers: modifiers.into_iter().map(Cow::Borrowed).collect(),
        object: object.into_iter().map(Cow::Borrowed).collect(),
        name,
        clauses,
    })
}

fn parse_show_clause<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<Option<ShowClause<'a, D::Ext>>> {
    let clause = if p.eat_keyword("HISTORY") {
        ShowClause::History
    } else if p.eat_keyword("LIKE") {
        ShowClause::Like(parse_string(p)?)
    } else if p.eat_keyword("WHERE") {
        ShowClause::Where(p.parse_expr()?)
    } else if p.eat_keyword("STARTS") {
        p.expect_keyword("WITH")?;
        ShowClause::StartsWith(parse_string(p)?)
    } else if p.eat_keyword("LIMIT") {
        ShowClause::Limit(p.parse_expr()?)
    } else if p.peek().kind == TokenKind::String {
        ShowClause::Pattern(parse_string(p)?)
    } else if p.at_keyword("FROM") && p.peek_nth(1).kind == TokenKind::String {
        p.advance();
        ShowClause::FromValue(parse_string(p)?)
    } else if at_any(p, &["IN", "FROM"]) {
        let from = p.at_keyword("FROM");
        p.advance();
        let scope = first_of(p, &["ACCOUNT", "DATABASE", "SCHEMA", "TABLE"]);
        let name = if p.at_ident() && !at_show_clause(p) {
            Some(parse_object_name(p)?)
        } else {
            None
        };
        ShowClause::Location {
            from,
            scope: scope.map(Cow::Borrowed),
            name,
        }
    } else {
        return Ok(None);
    };
    Ok(Some(clause))
}

// Transactions

fn parse_isolation_level<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<IsolationLevel> {
    if p.eat_keyword("READ") {
        if p.eat_keyword("COMMITTED") {
            return Ok(IsolationLevel::ReadCommitted);
        }
        p.expect_keyword("UNCOMMITTED")?;
        return Ok(IsolationLevel::ReadUncommitted);
    }
    if p.eat_keyword("REPEATABLE") {
        p.expect_keyword("READ")?;
        return Ok(IsolationLevel::RepeatableRead);
    }
    if p.eat_keyword("SERIALIZABLE") {
        return Ok(IsolationLevel::Serializable);
    }
    p.expect_keyword("SNAPSHOT")?;
    Ok(IsolationLevel::Snapshot)
}

fn parse_transaction_modes<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Vec<TransactionMode>> {
    let mut modes = Vec::new();
    loop {
        if p.eat_keyword("ISOLATION") {
            p.expect_keyword("LEVEL")?;
            modes.push(TransactionMode::IsolationLevel(parse_isolation_level(p)?));
        } else if p.eat_keyword("READ") {
            if p.eat_keyword("ONLY") {
                modes.push(TransactionMode::ReadOnly);
            } else {
                p.expect_keyword("WRITE")?;
                modes.push(TransactionMode::ReadWrite);
            }
        } else {
            return Ok(modes);
        }
        let next = p.peek_nth(1);
        if p.at_op(",") && (next.is_word("ISOLATION") || next.is_word("READ")) {
            p.advance();
        }
    }
}

fn parse_finish<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
    kind: FinishKind,
) -> PResult<Transaction<'a>> {
    p.advance();
    first_of(p, &["WORK", "TRAN", "TRANSACTION"]);
    let mut chain = false;
    if p.eat_keyword("AND") {
        let negated = p.eat_keyword("NO");
        p.expect_keyword("CHAIN")?;
        chain = !negated;
    }
    let savepoint = if kind == FinishKind::Rollback && p.eat_keyword("TO") {
        p.eat_keyword("SAVEPOINT");
        Some(p.parse_ident()?)
    } else {
        None
    };
    Ok(Transaction::Finish {
        kind,
        chain,
        savepoint,
    })
}

/// True at a statement that controls a transaction.
pub fn at_transaction<'a, D: Dialect>(p: &Parser<'a, D>) -> bool {
    at_any(
        p,
        &[
            "START",
            "BEGIN",
            "COMMIT",
            "END",
            "ROLLBACK",
            "SAVEPOINT",
            "RELEASE",
        ],
    )
}

/// Parses `ABORT [WORK | TRANSACTION] [AND [NO] CHAIN]`.
pub fn parse_abort<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Statement<'a, D::Ext>> {
    parse_finish(p, FinishKind::Abort).map(|t| StatementKind::Transaction(Box::new(t)).into())
}

pub fn parse_transaction<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Transaction<'a>> {
    if p.eat_keyword("START") {
        p.expect_keyword("TRANSACTION")?;
        return parse_transaction_modes(p).map(Transaction::Start);
    }
    if p.eat_keyword("BEGIN") {
        return Ok(Transaction::Begin(
            first_of(p, &["TRAN", "TRANSACTION", "WORK"]).map(Cow::Borrowed),
        ));
    }
    if p.at_keyword("COMMIT") {
        return parse_finish(p, FinishKind::Commit);
    }
    if p.at_keyword("END") {
        return parse_finish(p, FinishKind::End);
    }
    if p.at_keyword("ROLLBACK") {
        return parse_finish(p, FinishKind::Rollback);
    }
    if p.eat_keyword("SAVEPOINT") {
        return p.parse_ident().map(Transaction::Savepoint);
    }
    p.expect_keyword("RELEASE")?;
    p.eat_keyword("SAVEPOINT");
    p.parse_ident().map(Transaction::Release)
}

// USE

fn parse_use_part<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Ident<'a>> {
    let token = p.peek();
    if token.kind == TokenKind::String {
        p.advance();
        return Ok(Ident {
            text: Cow::Borrowed(&token.text[1..token.text.len() - 1]),
            quote: Some(char::from(token.text.as_bytes()[0])),
            span: token.span,
        });
    }
    p.parse_ident()
}

/// Parses a name for `USE`, in which each part can be a string.
pub fn parse_use_name<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<ObjectName<'a>> {
    let mut parts = vec![parse_use_part(p)?];
    while p.eat_op(".") {
        parts.push(parse_use_part(p)?);
    }
    Ok(ObjectName { parts })
}

// EXPLAIN

fn at_explain_modifier<'a, D: Dialect>(p: &Parser<'a, D>, word: &str) -> bool {
    p.at_keyword(word) && !at_statement_end(p, 1) && !p.peek_nth(1).is_op(".")
}

fn eat_explain_modifier<'a, D: Dialect>(p: &mut Parser<'a, D>, word: &str) -> bool {
    let found = at_explain_modifier(p, word);
    if found {
        p.advance();
    }
    found
}

fn parse_explain_format<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<Option<ExplainFormat<'a>>> {
    if !at_explain_modifier(p, "FORMAT") {
        return Ok(None);
    }
    p.advance();
    let equals = p.eat_op("=");
    let token = p.expect_kind(TokenKind::Word, "a format name")?;
    Ok(Some(ExplainFormat {
        equals,
        name: token.text.into(),
    }))
}

fn at_explainable_statement<'a, D: Dialect>(p: &Parser<'a, D>) -> bool {
    p.at_op("(")
        || at_any(
            p,
            &[
                "SELECT", "WITH", "VALUES", "INSERT", "UPDATE", "DELETE", "MERGE", "CREATE",
                "DROP", "ALTER", "TRUNCATE",
            ],
        )
}

pub fn parse_explain<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Explain<'a, D::Ext>> {
    let keyword = first_of(p, &["EXPLAIN", "DESCRIBE", "DESC"])
        .ok_or_else(|| p.expected("EXPLAIN or DESCRIBE"))?;
    let options = if keyword == "EXPLAIN" {
        D::parse_explain_options(p)?
    } else {
        Vec::new()
    };
    let detail = if keyword == "EXPLAIN" {
        None
    } else if at_explain_modifier(p, "EXTENDED") {
        p.advance();
        Some("EXTENDED")
    } else if at_explain_modifier(p, "FORMATTED") {
        p.advance();
        Some("FORMATTED")
    } else {
        None
    };
    let analyze = eat_explain_modifier(p, "ANALYZE");
    let verbose = eat_explain_modifier(p, "VERBOSE");
    let estimate = eat_explain_modifier(p, "ESTIMATE");
    let query_plan = at_explain_modifier(p, "QUERY") && p.peek_nth(1).is_word("PLAN");
    if query_plan {
        p.advance();
        p.advance();
    }
    let format = parse_explain_format(p)?;
    let target = D::parse_explain_target(p)?;
    Ok(Explain {
        keyword: keyword.into(),
        options,
        detail: detail.map(Cow::Borrowed),
        analyze,
        verbose,
        estimate,
        query_plan,
        format,
        target,
    })
}

/// Parses what `EXPLAIN` or `DESCRIBE` applies to.
pub fn parse_explain_target<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<ExplainTarget<'a, D::Ext>> {
    if at_explainable_statement(p) {
        return Ok(ExplainTarget::Statement(Box::new(
            p.nested(Parser::parse_statement)?,
        )));
    }
    Ok(ExplainTarget::Table {
        table_keyword: false,
        name: parse_object_name(p)?,
    })
}

/// Parses `USE name`.
pub fn parse_use<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<ObjectName<'a>> {
    p.expect_keyword("USE")?;
    parse_use_name(p)
}

/// Parses a parenthesized list of `EXPLAIN` options, for dialects with them.
pub fn parse_explain_option_list<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<Vec<ExplainOption<'a>>> {
    // No option is named like the start of a statement, so these mean the
    // parentheses hold a query: `EXPLAIN (VALUES (1))`.
    let next = p.peek_nth(1);
    let is_query = query_starts_at(p, 1)
        || next.is_op("(")
        || ["TABLE", "INSERT", "UPDATE", "DELETE", "MERGE"]
            .iter()
            .any(|word| next.is_word(word));
    if !p.at_op("(") || is_query {
        return Ok(Vec::new());
    }
    p.advance();
    let options = p.parse_comma_separated(parse_explain_option)?;
    p.expect_op(")")?;
    Ok(options)
}

fn is_option_value(token: Token<'_>) -> bool {
    matches!(
        token.kind,
        TokenKind::Word | TokenKind::QuotedIdent | TokenKind::String | TokenKind::Number
    )
}

fn parse_explain_option<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<ExplainOption<'a>> {
    let name = p.parse_name_part()?;
    let sign = if p.eat_op("-") {
        "-"
    } else if p.eat_op("+") {
        "+"
    } else {
        ""
    };
    let token = p.peek();
    let value = if is_option_value(token) {
        p.advance();
        Some(token.text)
    } else if sign.is_empty() {
        None
    } else {
        return Err(p.expected("a number"));
    };
    Ok(ExplainOption {
        name,
        sign: sign.into(),
        value: value.map(Cow::Borrowed),
    })
}

// SET

fn parse_set_scope<'a, D: Dialect>(p: &mut Parser<'a, D>) -> Option<&'static str> {
    first_of(p, &["LOCAL", "SESSION", "GLOBAL"])
}

fn is_assignment_token(token: Token<'_>) -> bool {
    token.is_op("=") || token.is_word("TO")
}

/// True if what follows `SET` and any scope is not a variable assignment.
pub fn at_special_set<'a, D: Dialect>(p: &Parser<'a, D>) -> bool {
    let scoped = ["LOCAL", "SESSION", "GLOBAL"]
        .iter()
        .any(|word| p.peek_nth(1).is_word(word));
    let at = 1 + usize::from(scoped);
    let word = p.peek_nth(at);
    let next = p.peek_nth(at + 1);
    (word.is_word("ROLE") && !is_assignment_token(next))
        || word.is_word("AUTHORIZATION")
        || word.is_word("CHARACTERISTICS")
        || word.is_word("TRANSACTION")
        || (word.is_word("TIME") && next.is_word("ZONE"))
}

pub fn parse_set<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<SetStatement<'a, D::Ext>> {
    let special = at_special_set(p);
    p.expect_keyword("SET")?;
    let scope = parse_set_scope(p);
    if special {
        return parse_special_set(p, scope);
    }
    let name = parse_object_name(p)?;
    if !is_assignment_token(p.peek()) {
        return Err(p.expected("= or TO"));
    }
    p.advance();
    let values = p.parse_comma_separated(Parser::parse_expr)?;
    Ok(SetStatement::Variable {
        scope: scope.map(Cow::Borrowed),
        name,
        values,
    })
}

fn parse_special_set<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
    scope: Option<&'static str>,
) -> PResult<SetStatement<'a, D::Ext>> {
    if p.eat_keyword("ROLE") {
        let role = p.parse_expr()?;
        return Ok(SetStatement::Role {
            scope: scope.map(Cow::Borrowed),
            role,
        });
    }
    if p.eat_keyword("AUTHORIZATION") {
        let user = p.parse_expr()?;
        return Ok(SetStatement::Authorization {
            scope: scope.map(Cow::Borrowed),
            user,
        });
    }
    if p.eat_keyword("CHARACTERISTICS") {
        p.expect_keyword("AS")?;
        p.expect_keyword("TRANSACTION")?;
        let modes = parse_transaction_modes(p)?;
        return Ok(SetStatement::Transaction {
            characteristics: true,
            modes,
            snapshot: None,
        });
    }
    if p.eat_keyword("TRANSACTION") {
        let snapshot = if p.eat_keyword("SNAPSHOT") {
            Some(parse_string(p)?)
        } else {
            None
        };
        let modes = parse_transaction_modes(p)?;
        return Ok(SetStatement::Transaction {
            characteristics: false,
            modes,
            snapshot,
        });
    }
    p.expect_keyword("TIME")?;
    p.expect_keyword("ZONE")?;
    if p.eat_keyword("TO") {
        let name = ObjectName {
            parts: vec![Ident {
                text: "TIMEZONE".into(),
                quote: None,
                span: Span::default(),
            }],
        };
        let values = vec![p.parse_expr()?];
        return Ok(SetStatement::Variable {
            scope: scope.map(Cow::Borrowed),
            name,
            values,
        });
    }
    let value = p.parse_expr()?;
    Ok(SetStatement::TimeZone {
        scope: scope.map(Cow::Borrowed),
        value,
    })
}

/// True at `SET NAMES`, for dialects that have it.
pub fn at_set_names<'a, D: Dialect>(p: &Parser<'a, D>) -> bool {
    p.at_keyword("SET") && p.peek_nth(1).is_word("NAMES")
}

/// Parses `SET NAMES charset [COLLATE collation]`.
pub fn parse_set_names<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<SetStatement<'a, D::Ext>> {
    p.advance();
    p.advance();
    let charset = p.nested(Parser::parse_prefix)?;
    let collate = if p.eat_keyword("COLLATE") {
        Some(p.parse_ident()?)
    } else {
        None
    };
    Ok(SetStatement::Names { charset, collate })
}

/// Parses `SET [scope] a = 1, [scope] b = 2`, for dialects that assign
/// several variables at once.
pub fn parse_set_assignments<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<SetStatement<'a, D::Ext>> {
    p.expect_keyword("SET")?;
    let assignments = p.parse_comma_separated(|p| {
        let scope = parse_set_scope(p);
        let name = parse_object_name(p)?;
        if !is_assignment_token(p.peek()) {
            return Err(p.expected("= or TO"));
        }
        p.advance();
        let value = p.parse_expr()?;
        Ok(SetAssignment {
            scope: scope.map(Cow::Borrowed),
            name,
            value,
        })
    })?;
    Ok(SetStatement::Assignments(assignments))
}

// COMMENT and CALL

const COMMENT_OBJECTS: [&str; 15] = [
    "COLLATION",
    "COLUMN",
    "DATABASE",
    "DOMAIN",
    "EXTENSION",
    "FUNCTION",
    "INDEX",
    "PROCEDURE",
    "ROLE",
    "SCHEMA",
    "SEQUENCE",
    "TABLE",
    "TYPE",
    "USER",
    "VIEW",
];

/// Parses `COMMENT [IF EXISTS] ON kind name IS value`, for dialects with it.
pub fn parse_comment_on<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<CommentOn<'a>> {
    parse_comment_on_with(p, &[])
}

/// Parses `COMMENT ON` with additional multi-word object kinds, each given
/// as its words. A dialect that has a kind the core lacks lists it here.
pub fn parse_comment_on_with<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
    extra: &[&[&'static str]],
) -> PResult<CommentOn<'a>> {
    p.expect_keyword("COMMENT")?;
    let if_exists = super::parse_if_exists(p);
    p.expect_keyword("ON")?;
    let kind = if p.at_keyword("MATERIALIZED") {
        parse_kind_words(p, &["IS"])
    } else if let Some(words) = extra.iter().find(|words| {
        words
            .iter()
            .enumerate()
            .all(|(i, w)| p.peek_nth(i).is_word(w))
    }) {
        words.iter().map(|_| p.advance().text).collect::<Vec<_>>()
    } else {
        let token = p.peek();
        let known = COMMENT_OBJECTS.iter().any(|kind| token.is_word(kind));
        if !known {
            return Err(p.expected("a kind of object"));
        }
        p.advance();
        vec![token.text]
    };
    let name = parse_object_name(p)?;
    p.expect_keyword("IS")?;
    let comment = if p.eat_keyword("NULL") {
        None
    } else {
        Some(parse_string(p)?)
    };
    Ok(CommentOn {
        if_exists,
        kind: kind.into_iter().map(Cow::Borrowed).collect(),
        name,
        comment,
    })
}

pub fn parse_call<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Expr<'a, D::Ext>> {
    p.expect_keyword("CALL")?;
    p.spanned_expr(|p| {
        let name = parse_object_name(p)?;
        if p.at_op("(") {
            return p.parse_function_call(name);
        }
        Ok(ExprKind::Compound(name).into())
    })
}

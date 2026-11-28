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

//! Databricks SQL, built on the Spark SQL rules.

use std::borrow::Cow;
use std::fmt;

use crate::PResult;
use crate::ast::{
    AlterTableOp, ColumnOption, Cte, DataType, Expr, GroupByModifier, Join, LateralView,
    ObjectName, PipeOperator, Quantifier, QueryDistribution, SampleClause, Select, SelectItem,
    SelectItemKind, Statement, StatementKind, StringLiteral, TableFactor, TableRef, TimeTravel,
    WildcardOptions,
};
use crate::dialect::builtin::BuiltinStatement;
use crate::dialect::snowflake;
use crate::dialect::{Builtin, Dialect};
use crate::dialect::{bigquery, oracle, spark};
use crate::display::comma;
use crate::grammar::{self, prec};
use crate::lexer::{LexResult, Lexeme, Token, TokenKind};
use crate::parser::{InfixRule, Parser, SetOperatorRule};

#[derive(Debug, Clone, PartialEq)]
pub struct Databricks;

/// Statements only Databricks has.
#[derive(Debug, Clone, PartialEq)]
pub enum DatabricksStatement<'a> {
    /// `USE CATALOG name`, or the same with `DATABASE` or `SCHEMA`.
    Use {
        kind: Cow<'a, str>,
        name: ObjectName<'a>,
    },
    /// `OPTIMIZE table [WHERE predicate] [ZORDER BY (a, b)]`
    Optimize {
        table: ObjectName<'a>,
        predicate: Option<Expr<'a, Builtin>>,
        zorder_by: Vec<Expr<'a, Builtin>>,
    },
}

impl fmt::Display for DatabricksStatement<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Use { kind, name } => write!(f, "USE {kind} {name}"),
            Self::Optimize {
                table,
                predicate,
                zorder_by,
            } => {
                write!(f, "OPTIMIZE {table}")?;
                if let Some(predicate) = predicate {
                    write!(f, " WHERE {predicate}")?;
                }
                if !zorder_by.is_empty() {
                    write!(f, " ZORDER BY ({})", comma(zorder_by))?;
                }
                Ok(())
            }
        }
    }
}

/// True if `run`, the letters, digits and underscores from a digit up to the
/// first other byte, is the start of a number rather than a name. `after` is
/// the byte that follows it.
#[inline]
fn is_number_run(run: &[u8], after: Option<u8>) -> bool {
    let digits = run.iter().take_while(|b| b.is_ascii_digit()).count();
    match run[digits..] {
        [] => true,
        // `1e5`, or the start of `1e+5`.
        [b'e' | b'E', ref exponent @ ..] if exponent.iter().all(u8::is_ascii_digit) => {
            !exponent.is_empty() || matches!(after, Some(b'+' | b'-'))
        }
        _ => false,
    }
}

/// The length of the run of name bytes at the start of `bytes`.
#[inline]
fn name_run(bytes: &[u8]) -> usize {
    bytes
        .iter()
        .take_while(|b| b.is_ascii_alphanumeric() || **b == b'_')
        .count()
}

/// Lexes a name that starts with a digit, such as `1st_table` or the `1c` of
/// `a.1c`, and otherwise leaves numbers to the core.
#[inline]
pub fn lex_digit_name(rest: &str) -> Option<LexResult> {
    let bytes = rest.as_bytes();
    let lexeme = |kind, len| Some(Ok(Lexeme { kind, len }));
    match bytes {
        [b'0', b'x' | b'X', ..] => None,
        [b'0'..=b'9', ..] => {
            let len = name_run(bytes);
            if is_number_run(&bytes[..len], bytes.get(len).copied()) {
                return None;
            }
            lexeme(TokenKind::Word, len)
        }
        [b'.', b'0'..=b'9', ..] => {
            let len = name_run(&bytes[1..]);
            if is_number_run(&bytes[1..=len], bytes.get(len + 1).copied()) {
                return None;
            }
            lexeme(TokenKind::Op, 1)
        }
        _ => None,
    }
}

#[inline]
pub fn lex_token(rest: &str) -> Option<LexResult> {
    lex_digit_name(rest).or_else(|| spark::lex_spark_token(rest))
}

fn parse_optimize<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<DatabricksStatement<'a>> {
    p.expect_keyword("OPTIMIZE")?;
    let table = grammar::parse_object_name(p)?;
    let predicate = if p.eat_keyword("WHERE") {
        Some(p.parse_expr()?)
    } else {
        None
    };
    let zorder_by = if p.eat_keyword("ZORDER") {
        p.expect_keyword("BY")?;
        p.expect_op("(")?;
        let columns = p.parse_comma_separated(Parser::parse_expr)?;
        p.expect_op(")")?;
        columns
    } else {
        Vec::new()
    };
    Ok(DatabricksStatement::Optimize {
        table,
        predicate,
        zorder_by,
    })
}

/// Parses `USE` before a kind of object, if that is what is here.
fn parse_use_of_kind<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Option<DatabricksStatement<'a>>> {
    let Some(kind) = ["CATALOG", "DATABASE", "SCHEMA"]
        .into_iter()
        .find(|kind| p.peek_nth(1).is_word(kind))
    else {
        return Ok(None);
    };
    p.advance();
    p.advance();
    let name = grammar::parse_use_name(p)?;
    Ok(Some(DatabricksStatement::Use {
        kind: kind.into(),
        name,
    }))
}

pub fn parse_statement<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Statement<'a, Builtin>> {
    if p.at_keyword("EXECUTE") && p.peek_nth(1).is_word("IMMEDIATE") {
        let execute = oracle::parse_execute_immediate(p)?;
        return Ok(StatementKind::Ext(BuiltinStatement::Oracle(execute)).into());
    }
    if grammar::at_alter_view(p) {
        return grammar::parse_alter_view(p, false)
            .map(|a| StatementKind::AlterView(Box::new(a)).into());
    }
    let statement = if p.at_keyword("OPTIMIZE") {
        Some(parse_optimize(p)?)
    } else if p.at_keyword("USE") {
        parse_use_of_kind(p)?
    } else {
        None
    };
    match statement {
        Some(statement) => Ok(StatementKind::Ext(BuiltinStatement::Databricks(statement)).into()),
        None => spark::parse_statement(p),
    }
}

/// Parses a table, which can be read as of a version or a time.
pub fn parse_table_factor<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<TableFactor<'a, D::Ext>> {
    let factor = spark::parse_table_factor(p)?;
    let TableFactor::Named(name) = factor else {
        return Ok(factor);
    };
    let at_as_of = p.peek_nth(1).is_word("AS") && p.peek_nth(2).is_word("OF");
    let kind = ["TIMESTAMP", "VERSION"]
        .into_iter()
        .find(|kind| at_as_of && p.at_keyword(kind));
    let Some(kind) = kind else {
        return Ok(TableFactor::Named(name));
    };
    p.advance();
    p.advance();
    p.advance();
    let at = if kind == "VERSION" {
        if !matches!(p.peek().kind, TokenKind::Number | TokenKind::String) {
            return Err(p.expected("a version number"));
        }
        p.parse_prefix()?
    } else {
        p.parse_expr()?
    };
    Ok(TableFactor::TimeTravel(Box::new(TimeTravel {
        name,
        kind: kind.into(),
        at,
    })))
}

/// Parses a select item, where a wildcard can end in `REPLACE (...)`.
pub fn parse_select_item<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<SelectItem<'a, D::Ext>> {
    let item = spark::parse_select_item(p)?;
    if !p.at_keyword("REPLACE") || !p.peek_nth(1).is_op("(") {
        return Ok(item);
    }
    let (qualifier, options) = match item.kind {
        SelectItemKind::Wildcard => (None, None),
        SelectItemKind::QualifiedWildcard(name) => (Some(name), None),
        SelectItemKind::WildcardWithOptions { qualifier, options } => (qualifier, Some(options)),
        _ => return Ok(item),
    };
    let mut options = options.unwrap_or_else(|| {
        Box::new(WildcardOptions {
            ilike: None,
            exclude: Vec::new(),
            exclude_parens: false,
            except_keyword: false,
            replace: Vec::new(),
            rename: Vec::new(),
            rename_parens: false,
        })
    });
    options.replace = grammar::parse_wildcard_replace(p)?;
    Ok(SelectItemKind::WildcardWithOptions { qualifier, options }.into())
}

/// Parses a prefix expression, where `exists` over anything but a query is an
/// ordinary function.
pub fn parse_prefix<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Expr<'a, D::Ext>> {
    if p.at_keyword("EXISTS") && p.peek_nth(1).is_op("(") && !grammar::query_starts_at(p, 2) {
        return grammar::parse_name_expr(p);
    }
    spark::parse_prefix(p)
}

/// Parses a `SELECT`, which can start with its `FROM` clause.
pub fn parse_select<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Select<'a, D::Ext>> {
    if p.at_keyword("FROM") {
        grammar::parse_from_first_select(p)
    } else {
        grammar::parse_select(p)
    }
}

impl Dialect for Databricks {
    type Ext = Builtin;

    fn parse_alter_table_op<'a>(p: &mut Parser<'a, Self>) -> PResult<AlterTableOp<'a, Builtin>> {
        spark::parse_alter_table_op(p)
    }

    fn parse_qualify<'a>(p: &mut Parser<'a, Self>) -> PResult<Option<Expr<'a, Builtin>>> {
        snowflake::parse_qualify(p)
    }

    fn parse_like_quantifier<'a>(p: &mut Parser<'a, Self>) -> PResult<Option<Quantifier>> {
        Ok(grammar::parse_like_any(p))
    }

    fn eat_limit_all<'a>(p: &mut Parser<'a, Self>) -> bool {
        p.eat_keyword("ALL")
    }

    fn set_operator_rule<'a>(p: &Parser<'a, Self>) -> Option<SetOperatorRule> {
        grammar::set_operator_rule_with_minus(p)
    }

    #[inline]
    fn is_reserved_for_alias(word: &str) -> bool {
        word.eq_ignore_ascii_case("MINUS")
            || word.eq_ignore_ascii_case("QUALIFY")
            || grammar::is_reserved_for_alias(word)
    }

    fn parse_group_by_suffix<'a>(
        p: &mut Parser<'a, Self>,
    ) -> PResult<Option<Box<Expr<'a, Builtin>>>> {
        grammar::parse_group_by_grouping_sets(p)
    }

    fn parse_group_by_item<'a>(p: &mut Parser<'a, Self>) -> PResult<Expr<'a, Builtin>> {
        grammar::parse_grouping_item_or_empty_set(p)
    }

    fn parse_statement_after_with<'a>(
        p: &mut Parser<'a, Self>,
    ) -> PResult<Option<Statement<'a, Builtin>>> {
        grammar::parse_dml_after_with(p, true, false)
    }

    #[inline]
    fn is_reserved(word: &str) -> bool {
        !word.eq_ignore_ascii_case("ALL") && grammar::is_reserved(word)
    }

    fn parse_statement<'a>(p: &mut Parser<'a, Self>) -> PResult<Statement<'a, Builtin>> {
        parse_statement(p)
    }

    fn at_from_first<'a>(p: &Parser<'a, Self>, n: usize) -> bool {
        p.peek_nth(n).is_word("FROM")
    }

    fn parse_select<'a>(p: &mut Parser<'a, Self>) -> PResult<Select<'a, Builtin>> {
        parse_select(p)
    }

    fn parse_table_ref<'a>(p: &mut Parser<'a, Self>) -> PResult<TableRef<'a, Builtin>> {
        spark::parse_table_ref(p)
    }

    fn parse_join<'a>(p: &mut Parser<'a, Self>) -> PResult<Option<Join<'a, Builtin>>> {
        spark::parse_join(p)
    }

    fn parse_select_item_alias<'a>(
        p: &mut Parser<'a, Self>,
        expr: Expr<'a, Builtin>,
    ) -> PResult<SelectItem<'a, Builtin>> {
        spark::parse_select_item_alias(p, expr)
    }

    fn parse_lateral_views<'a>(p: &mut Parser<'a, Self>) -> PResult<Vec<LateralView<'a, Builtin>>> {
        spark::parse_lateral_views(p)
    }

    fn parse_query_distribution<'a>(
        p: &mut Parser<'a, Self>,
    ) -> PResult<Option<Box<QueryDistribution<'a, Builtin>>>> {
        spark::parse_query_distribution(p)
    }

    fn parse_insert_statement<'a>(p: &mut Parser<'a, Self>) -> PResult<Statement<'a, Builtin>> {
        spark::parse_insert(p)
    }

    fn parse_sample_clause<'a>(
        p: &mut Parser<'a, Self>,
        long_form: bool,
    ) -> PResult<SampleClause<'a, Builtin>> {
        spark::parse_sample_clause(p, long_form)
    }

    fn parse_between_low<'a>(p: &mut Parser<'a, Self>) -> PResult<Expr<'a, Builtin>> {
        let low = grammar::parse_between_low(p)?;
        spark::parse_between_bound(p, low)
    }

    fn parse_between_high<'a>(p: &mut Parser<'a, Self>) -> PResult<Expr<'a, Builtin>> {
        let high = grammar::parse_between_high(p)?;
        spark::parse_between_bound(p, high)
    }

    #[inline]
    fn lex_token(rest: &str) -> Option<LexResult> {
        lex_token(rest)
    }

    #[inline]
    fn string_literal(token: Token<'_>) -> StringLiteral<'_> {
        spark::string_literal(token)
    }

    fn parse_prefix<'a>(p: &mut Parser<'a, Self>) -> PResult<Expr<'a, Builtin>> {
        if p.at_keyword("STRUCT") && p.peek_nth(1).is_op("(") {
            return bigquery::parse_struct(p);
        }
        parse_prefix(p)
    }

    fn infix_rule<'a>(p: &Parser<'a, Self>) -> Option<InfixRule<'a, Self>> {
        if snowflake::at_path(p) {
            return Some(InfixRule::custom(prec::POSTFIX, snowflake::parse_path));
        }
        spark::infix_rule(p)
    }

    fn parse_select_item<'a>(p: &mut Parser<'a, Self>) -> PResult<SelectItem<'a, Builtin>> {
        parse_select_item(p)
    }

    fn parse_table_factor<'a>(p: &mut Parser<'a, Self>) -> PResult<TableFactor<'a, Builtin>> {
        parse_table_factor(p)
    }

    fn parse_cte<'a>(p: &mut Parser<'a, Self>) -> PResult<Cte<'a, Builtin>> {
        spark::parse_cte(p)
    }

    fn parse_pipe_operators<'a>(
        p: &mut Parser<'a, Self>,
    ) -> PResult<Vec<PipeOperator<'a, Builtin>>> {
        grammar::parse_pipe_operators(p)
    }

    fn eat_group_by_all<'a>(p: &mut Parser<'a, Self>) -> bool {
        p.eat_keyword("ALL")
    }

    fn parse_group_by_modifier<'a>(p: &mut Parser<'a, Self>) -> Option<GroupByModifier> {
        grammar::parse_group_by_with(p, true)
    }

    fn parse_data_type<'a>(p: &mut Parser<'a, Self>) -> PResult<DataType<'a>> {
        spark::parse_data_type(p)
    }

    fn parse_column_option<'a>(
        p: &mut Parser<'a, Self>,
    ) -> PResult<Option<ColumnOption<'a, Builtin>>> {
        spark::parse_column_option(p)
    }

    fn parse_create<'a>(p: &mut Parser<'a, Self>) -> PResult<Statement<'a, Builtin>> {
        spark::parse_create(p)
    }
}

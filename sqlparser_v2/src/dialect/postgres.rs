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

//! PostgreSQL.
//!
//! The rules are public and generic over the host dialect so that dialects
//! derived from PostgreSQL can reuse them. See [`super::redshift`].

use crate::ast::Span;
use std::borrow::Cow;
use std::fmt;

use crate::PResult;
pub use crate::ast::PartitionBound;
use crate::ast::{
    AlterTable, AlterTableOp, ArrayType, Assignment, BinaryOp, CastStyle, ColumnOption,
    CreateFlags, CreateSchema, CreateView, Cte, DataType, DataTypeSuffix, DropBehavior, DropObject,
    ExplainOption, Expr, ExprKind, FunctionArg, GeneratedKind, GeneratedStorage, Grant, Ident,
    IndexFlags, IndexStorage, Insert, InsertSource, IntervalFields, IsTest, Join, JoinConstraint,
    JoinKind, LikeKind, Limit, ObjectName, ObjectType, OperatorName, OrderByItem, Privileges,
    Query, SelectInto, SelectItem, SetExpr, SetStatement, SpecialExpr, Statement, StatementKind,
    StringLiteral, StringPrefix, TableAlias, TableConstraint, TableFactor, TableRef,
    TableWithJoins, TypedString,
};
use crate::dialect::builtin::{
    BuiltinExpr, BuiltinJoinConstraint, BuiltinSelect, BuiltinTableFactor,
};
use crate::dialect::{Builtin, Dialect};
use crate::display::{comma, write_insert_source, write_list_clause};
use crate::grammar::{self, prec};
use crate::lexer::{LexResult, Lexeme, Token, TokenKind, quoted, quoted_with_backslash};
use crate::parser::{InfixRule, Parser};

pub mod ddl;
pub mod table;
pub mod utility;

pub use ddl::*;
pub use table::*;
pub use utility::*;

#[derive(Debug, Clone, PartialEq)]
pub struct Postgres;

/// PostgreSQL's operators with their binding powers, longest first so that
/// a prefix cannot match before the whole operator.
const OPERATORS: [(&str, u8); 21] = [
    ("!~~*", prec::COMPARE),
    ("!~~", prec::COMPARE),
    ("!~*", prec::COMPARE),
    ("~~*", prec::COMPARE),
    ("->>", prec::OTHER),
    ("#>>", prec::OTHER),
    ("!~", prec::COMPARE),
    ("~~", prec::COMPARE),
    ("~*", prec::COMPARE),
    ("->", prec::OTHER),
    ("#>", prec::OTHER),
    // Deletes a path from a `jsonb` value.
    ("#-", prec::OTHER),
    ("@>", prec::OTHER),
    ("<@", prec::OTHER),
    ("@?", prec::OTHER),
    ("@@", prec::OTHER),
    ("?|", prec::OTHER),
    ("?&", prec::OTHER),
    ("&&", prec::OTHER),
    ("~", prec::COMPARE),
    ("?", prec::OTHER),
];

/// True for the functions PostgreSQL calls without parentheses: the standard
/// ones, plus `USER`, `CURRENT_CATALOG`, `CURRENT_SCHEMA` and `SYSTEM_USER`.
pub fn is_niladic_function(word: &str) -> bool {
    grammar::is_niladic_function(word)
        || ["USER", "CURRENT_CATALOG", "CURRENT_SCHEMA", "SYSTEM_USER"]
            .iter()
            .any(|name| word.eq_ignore_ascii_case(name))
}

#[inline]
fn may_start_operator(byte: u8) -> bool {
    matches!(byte, b'!' | b'~' | b'-' | b'#' | b'@' | b'<' | b'?' | b'&')
}

#[inline]
pub fn lex_token(rest: &str) -> Option<LexResult> {
    let op = |len| {
        Ok(Lexeme {
            kind: TokenKind::Op,
            len,
        })
    };
    if rest.starts_with("::") {
        return Some(op(2));
    }
    if rest.as_bytes()[0] == b'$' {
        return dollar_string(rest);
    }
    if !may_start_operator(rest.as_bytes()[0]) {
        return None;
    }
    OPERATORS
        .iter()
        .find(|(text, _)| rest.starts_with(text))
        .map(|(text, _)| op(text.len()))
}

/// Lexes `$tag$text$tag$`. Returns `None` for what is not one, such as `$1`.
#[inline]
pub fn dollar_string(rest: &str) -> Option<LexResult> {
    let bytes = rest.as_bytes();
    if bytes.get(1).is_some_and(u8::is_ascii_digit) {
        return None;
    }
    let tag = bytes[1..]
        .iter()
        .take_while(|&&b| b.is_ascii_alphanumeric() || b == b'_' || !b.is_ascii())
        .count();
    if bytes.get(tag + 1) != Some(&b'$') {
        return None;
    }
    let delimiter = &rest[..tag + 2];
    let body = tag + 2;
    Some(match rest[body..].find(delimiter) {
        Some(end) => Ok(Lexeme {
            kind: TokenKind::DollarString,
            len: body + end + delimiter.len(),
        }),
        None => Err("unterminated dollar-quoted string"),
    })
}

/// Everything PostgreSQL ranks differently from the core.
fn operator_rule<'a, D: Dialect>(text: &str) -> Option<InfixRule<'a, D>> {
    let core = |op| Some(InfixRule::binary(prec::OTHER, op));
    match text {
        "||" => core(BinaryOp::StringConcat),
        "|" => core(BinaryOp::BitOr),
        "&" => core(BinaryOp::BitAnd),
        "<<" => core(BinaryOp::ShiftLeft),
        ">>" => core(BinaryOp::ShiftRight),
        // Exponentiation; PostgreSQL writes XOR as `#`.
        "^" => Some(InfixRule::binary(prec::EXP, BinaryOp::Named("^".into()))),
        _ if may_start_operator(text.as_bytes()[0]) => OPERATORS
            .iter()
            .find(|(operator, _)| *operator == text)
            .map(|(operator, bp)| InfixRule::binary(*bp, BinaryOp::Named((*operator).into()))),
        _ => None,
    }
}

pub fn infix_rule<'a, D: Dialect>(p: &Parser<'a, D>) -> Option<InfixRule<'a, D>> {
    if p.at_keyword("NOTNULL") || p.at_keyword("ISNULL") {
        return Some(InfixRule::custom(prec::IS, parse_postfix_null));
    }
    infix_rule_without_null_tests(p)
}

/// The operators of [`infix_rule`] apart from the postfix `ISNULL` and
/// `NOTNULL`, which Redshift does not have.
pub fn infix_rule_without_null_tests<'a, D: Dialect>(
    p: &Parser<'a, D>,
) -> Option<InfixRule<'a, D>> {
    let token = p.peek();
    if token.kind == TokenKind::Op
        && let Some(rule) = operator_rule(token.text)
    {
        return Some(rule);
    }
    if p.at_op("::") {
        Some(InfixRule::custom(prec::POSTFIX, parse_double_colon))
    } else if p.at_keyword("ILIKE") {
        Some(InfixRule::custom(prec::COMPARE, parse_ilike))
    } else if p.at_keyword("NOT") && next_is_ilike(p) {
        Some(InfixRule::custom(prec::COMPARE, parse_not_ilike))
    } else {
        grammar::infix_rule(p)
    }
}

pub fn next_is_ilike<'a, D: Dialect>(p: &Parser<'a, D>) -> bool {
    p.peek_nth(1).is_word("ILIKE")
}

pub fn parse_ilike<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
    left: Expr<'a, D::Ext>,
    _bp: u8,
) -> PResult<Expr<'a, D::Ext>> {
    p.advance();
    grammar::parse_like_operand(p, left, false, LikeKind::ILike)
}

pub fn parse_not_ilike<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
    left: Expr<'a, D::Ext>,
    _bp: u8,
) -> PResult<Expr<'a, D::Ext>> {
    p.advance();
    p.advance();
    grammar::parse_like_operand(p, left, true, LikeKind::ILike)
}

pub fn parse_double_colon<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
    left: Expr<'a, D::Ext>,
    _bp: u8,
) -> PResult<Expr<'a, D::Ext>> {
    p.advance();
    let ty = Box::new(D::parse_data_type(p)?);
    Ok(ExprKind::Cast {
        expr: Box::new(left),
        ty,
        style: CastStyle::DoubleColon,
    }
    .into())
}

/// Accepts `INTERVAL [fields] [(precision)]` as well as the core types.
pub fn parse_data_type<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<DataType<'a>> {
    let mut qualifier = Vec::new();
    while matches!(p.peek().kind, TokenKind::Word | TokenKind::QuotedIdent)
        && p.peek_nth(1).is_op(".")
        && matches!(p.peek_nth(2).kind, TokenKind::Word | TokenKind::QuotedIdent)
    {
        qualifier.push(p.parse_name_part()?);
        p.advance();
    }
    if !p.at_keyword("INTERVAL") {
        let mut ty = grammar::parse_data_type(p)?;
        if ty.array == ArrayType::None && p.at_op("[") {
            ty.array = grammar::parse_array_brackets(p)?;
        }
        ty.qualifier = qualifier;
        return Ok(ty);
    }
    let name = p.parse_ident()?;
    let suffix =
        parse_interval_fields(p)?.map_or(DataTypeSuffix::None, DataTypeSuffix::IntervalFields);
    let args = grammar::parse_optional_precision(p)?
        .into_iter()
        .map(Cow::Borrowed)
        .collect();
    Ok(DataType {
        qualifier,
        angle: None,
        name,
        args,
        suffix,
        array: ArrayType::None,
    })
}

/// Parses `ISNULL` and `NOTNULL`, which render in their standard spelling.
fn parse_postfix_null<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
    left: Expr<'a, D::Ext>,
    _bp: u8,
) -> PResult<Expr<'a, D::Ext>> {
    let negated = p.at_keyword("NOTNULL");
    p.advance();
    Ok(ExprKind::Is {
        expr: Box::new(left),
        test: IsTest::Null,
        negated,
    }
    .into())
}

/// Parses `DISTINCT ON (...)` if present.
pub fn parse_distinct_on<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<BuiltinSelect<'a>> {
    let next = p.peek_nth(1);
    if !(p.at_keyword("DISTINCT") && next.is_word("ON")) {
        return Ok(BuiltinSelect::default());
    }
    p.advance();
    p.advance();
    p.expect_op("(")?;
    let exprs = if p.at_op(")") {
        Vec::new()
    } else {
        p.parse_comma_separated(Parser::parse_expr)?
    };
    p.expect_op(")")?;
    Ok(BuiltinSelect {
        distinct_on: Some(exprs),
        ..BuiltinSelect::default()
    })
}

fn parse_interval_fields<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Option<IntervalFields>> {
    use IntervalFields::*;
    // Each leading unit, with the units it can run to.
    type Range = (&'static str, IntervalFields);
    let (ranges, plain): (&[Range], IntervalFields) = if p.eat_keyword("YEAR") {
        (&[("MONTH", YearToMonth)], Year)
    } else if p.eat_keyword("DAY") {
        (
            &[
                ("HOUR", DayToHour),
                ("MINUTE", DayToMinute),
                ("SECOND", DayToSecond),
            ],
            Day,
        )
    } else if p.eat_keyword("HOUR") {
        (&[("MINUTE", HourToMinute), ("SECOND", HourToSecond)], Hour)
    } else if p.eat_keyword("MINUTE") {
        (&[("SECOND", MinuteToSecond)], Minute)
    } else if p.eat_keyword("MONTH") {
        return Ok(Some(Month));
    } else if p.eat_keyword("SECOND") {
        return Ok(Some(Second));
    } else {
        return Ok(None);
    };
    if !p.eat_keyword("TO") {
        return Ok(Some(plain));
    }
    for (unit, fields) in ranges {
        if p.eat_keyword(unit) {
            return Ok(Some(*fields));
        }
    }
    Err(p.expected("a smaller interval unit"))
}

/// Consumes `CONCURRENTLY` and `ASYNC`, the second of which can name an index.
pub fn parse_index_flags<D: Dialect>(p: &mut Parser<'_, D>) -> IndexFlags {
    let concurrently = p.eat_keyword("CONCURRENTLY");
    let asynchronous = p.at_keyword("ASYNC") && !p.peek_nth(1).is_word("ON");
    if asynchronous {
        p.advance();
    }
    IndexFlags {
        concurrently,
        asynchronous,
    }
}

fn phrase_name<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
    phrase: &'static str,
) -> PResult<AlterTableOp<'a, D::Ext>> {
    Ok(AlterTableOp::PhraseName {
        phrase: phrase.into(),
        name: p.parse_name_part()?,
    })
}

fn expect_row_level_security<D: Dialect>(p: &mut Parser<'_, D>) -> PResult<()> {
    ["ROW", "LEVEL", "SECURITY"]
        .iter()
        .try_for_each(|word| p.expect_keyword(word))
}

/// Parses what follows `ENABLE`.
fn parse_enable<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<AlterTableOp<'a, D::Ext>> {
    if p.at_keyword("ROW") {
        expect_row_level_security(p)?;
        return Ok(AlterTableOp::Phrase("ENABLE ROW LEVEL SECURITY".into()));
    }
    let always = p.eat_keyword("ALWAYS");
    let replica = !always && p.eat_keyword("REPLICA");
    let phrase = if p.eat_keyword("TRIGGER") {
        match (always, replica) {
            (true, _) => "ENABLE ALWAYS TRIGGER",
            (_, true) => "ENABLE REPLICA TRIGGER",
            _ => "ENABLE TRIGGER",
        }
    } else {
        p.expect_keyword("RULE")?;
        match (always, replica) {
            (true, _) => "ENABLE ALWAYS RULE",
            (_, true) => "ENABLE REPLICA RULE",
            _ => "ENABLE RULE",
        }
    };
    phrase_name(p, phrase)
}

/// Parses what follows `DISABLE`.
fn parse_disable<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<AlterTableOp<'a, D::Ext>> {
    if p.at_keyword("ROW") {
        expect_row_level_security(p)?;
        return Ok(AlterTableOp::Phrase("DISABLE ROW LEVEL SECURITY".into()));
    }
    if p.eat_keyword("TRIGGER") {
        return phrase_name(p, "DISABLE TRIGGER");
    }
    p.expect_keyword("RULE")?;
    phrase_name(p, "DISABLE RULE")
}

fn parse_replica_identity<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<AlterTableOp<'a, D::Ext>> {
    p.expect_keyword("IDENTITY")?;
    if p.eat_keyword("USING") {
        p.expect_keyword("INDEX")?;
        return phrase_name(p, "REPLICA IDENTITY USING INDEX");
    }
    if p.eat_keyword("DEFAULT") {
        return Ok(AlterTableOp::Phrase("REPLICA IDENTITY DEFAULT".into()));
    }
    if p.eat_keyword("FULL") {
        return Ok(AlterTableOp::Phrase("REPLICA IDENTITY FULL".into()));
    }
    p.expect_keyword("NOTHING")?;
    Ok(AlterTableOp::Phrase("REPLICA IDENTITY NOTHING".into()))
}

pub fn parse_alter_table_op<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<AlterTableOp<'a, D::Ext>> {
    let next = p.peek_nth(1);
    if p.eat_keyword("ENABLE") {
        return parse_enable(p);
    }
    if p.eat_keyword("DISABLE") {
        return parse_disable(p);
    }
    if p.eat_keyword("FORCE") {
        expect_row_level_security(p)?;
        return Ok(AlterTableOp::Phrase("FORCE ROW LEVEL SECURITY".into()));
    }
    if p.at_keyword("NO") && next.is_word("FORCE") {
        p.advance();
        p.advance();
        expect_row_level_security(p)?;
        return Ok(AlterTableOp::Phrase("NO FORCE ROW LEVEL SECURITY".into()));
    }
    if p.eat_keyword("REPLICA") {
        return parse_replica_identity(p);
    }
    if p.eat_keyword("VALIDATE") {
        p.expect_keyword("CONSTRAINT")?;
        return phrase_name(p, "VALIDATE CONSTRAINT");
    }
    if p.eat_keyword("OWNER") {
        p.expect_keyword("TO")?;
        return phrase_name(p, "OWNER TO");
    }
    if p.at_keyword("RENAME") && next.is_word("CONSTRAINT") {
        p.advance();
        p.advance();
        let old = p.parse_ident()?;
        p.expect_keyword("TO")?;
        let new = p.parse_ident()?;
        return Ok(AlterTableOp::RenameConstraint { old, new });
    }
    if p.at_keyword("SET") && next.kind == TokenKind::Op && next.text == "(" {
        p.advance();
        return grammar::parse_storage_parameter_list(p).map(AlterTableOp::SetOptions);
    }
    if p.at_keyword("SET") && (next.is_word("LOGGED") || next.is_word("UNLOGGED")) {
        p.advance();
        let phrase = if p.eat_keyword("LOGGED") {
            "SET LOGGED"
        } else {
            p.advance();
            "SET UNLOGGED"
        };
        return Ok(AlterTableOp::Phrase(phrase.into()));
    }
    grammar::parse_alter_table_op(p)
}

/// Statements only PostgreSQL has.
#[derive(Debug, Clone, PartialEq)]
pub enum PostgresStatement<'a> {
    DropOperator {
        if_exists: bool,
        operators: Vec<OperatorSignature<'a>>,
        behavior: Option<DropBehavior>,
    },
    /// `DROP OPERATOR {CLASS | FAMILY}`
    DropOperatorGroup {
        family: bool,
        if_exists: bool,
        names: Vec<ObjectName<'a>>,
        using: Ident<'a>,
        behavior: Option<DropBehavior>,
    },
    CreateOperator {
        name: OperatorName<'a>,
        definition: Vec<DefElem<'a>>,
    },
    CreateOperatorFamily {
        name: ObjectName<'a>,
        using: Ident<'a>,
    },
    CreateOperatorClass(Box<CreateOperatorClass<'a>>),
    AlterOperator {
        name: OperatorName<'a>,
        /// The left and right argument types.
        types: Box<(DataType<'a>, DataType<'a>)>,
        op: AlterOperatorOp<'a>,
    },
    AlterOperatorClass {
        family: bool,
        name: ObjectName<'a>,
        using: Ident<'a>,
        op: AlterFamilyOp<'a>,
    },
    CreateAggregate(Box<CreateAggregate<'a>>),
    AlterAggregate(Box<AlterAggregate<'a>>),
    CreateFunction(Box<CreateFunction<'a>>),
    /// The first field is `FUNCTION`, `PROCEDURE` or `ROUTINE`.
    AlterFunction(Cow<'a, str>, Box<AlterFunction<'a>>),
    CreateRole(Box<CreateRole<'a>>),
    AlterRole(Box<AlterRole<'a>>),
    CreateExtension(Box<CreateExtension<'a>>),
    CreateDomain(Box<CreateDomain<'a>>),
    CreateType(Box<CreateType<'a>>),
    AlterType(Box<AlterType<'a>>),
    CreateCollation(Box<CreateCollation<'a>>),
    AlterCollation(Box<AlterCollation<'a>>),
    CreateTextSearch {
        kind: TextSearchKind,
        name: ObjectName<'a>,
        definition: Vec<DefElem<'a>>,
    },
    AlterTextSearch {
        kind: TextSearchKind,
        name: ObjectName<'a>,
        op: AlterTextSearchOp<'a>,
    },
    CreateForeignDataWrapper {
        name: Ident<'a>,
        clauses: Vec<FdwClause<'a>>,
    },
    CreateServer(Box<CreateServer<'a>>),
    CreateUserMapping(Box<CreateUserMapping<'a>>),
    AlterDefaultPrivileges(Box<AlterDefaultPrivileges<'a>>),
    CreateTrigger(Box<CreateTrigger<'a>>),
    CreateRule(Box<CreateRule<'a>>),
    CreateTable(Box<PostgresCreateTable<'a>>),
    /// `ALTER {MATERIALIZED VIEW | FOREIGN TABLE} ...`
    AlterRelation {
        keyword: Cow<'a, str>,
        alter: Box<AlterTable<'a, Builtin>>,
    },
    AlterSchema {
        name: ObjectName<'a>,
        action: ObjectAction<'a>,
    },
    /// `RETURN expr`, inside `BEGIN ATOMIC`.
    Return(Box<Expr<'a, Builtin>>),
    Copy(Box<Copy<'a>>),
    Truncate(Box<PostgresTruncate<'a>>),
    Insert(Box<PostgresInsert<'a>>),
    Declare(Box<DeclareCursor<'a>>),
    Fetch(Box<FetchStatement<'a>>),
    Lock(Box<LockStatement<'a>>),
    Prepare(Box<PrepareStatement<'a>>),
    Execute(Box<ExecuteStatement<'a>>),
    Deallocate {
        prepare: bool,
        /// `None` for `DEALLOCATE ALL`.
        name: Option<Ident<'a>>,
    },
    Do(Vec<DoItem<'a>>),
    Analyze {
        verbose: bool,
        tables: Vec<AnalyzeTable<'a>>,
    },
    Notify {
        channel: Ident<'a>,
        payload: Option<StringLiteral<'a>>,
    },
    Listen(Ident<'a>),
    /// `None` for `UNLISTEN *`.
    Unlisten(Option<Ident<'a>>),
    /// `DISCARD ALL`, `PLANS`, `SEQUENCES`, `TEMP` or `TEMPORARY`.
    Discard(Cow<'a, str>),
    /// `RESET name`, `RESET ALL` or `RESET SESSION AUTHORIZATION`.
    Reset(ResetTarget<'a>),
    /// `CLOSE name`, or `CLOSE ALL` for `None`.
    Close(Option<Ident<'a>>),
    /// `LOAD 'file'`
    Load(StringLiteral<'a>),
    CreatePolicy(Box<CreatePolicy<'a>>),
    AlterPolicy(Box<AlterPolicy<'a>>),
}

/// What `RESET` applies to.
#[derive(Debug, Clone, PartialEq)]
pub enum ResetTarget<'a> {
    All,
    SessionAuthorization,
    Name(ObjectName<'a>),
}

/// `CREATE POLICY name ON table [AS kind] [FOR command] [TO roles] [USING (e)] [WITH CHECK (e)]`
#[derive(Debug, Clone, PartialEq)]
pub struct CreatePolicy<'a> {
    pub name: Ident<'a>,
    pub table: ObjectName<'a>,
    pub kind: Option<Cow<'a, str>>,
    pub command: Option<Cow<'a, str>>,
    pub to: Vec<Ident<'a>>,
    pub using: Option<Expr<'a, Builtin>>,
    pub with_check: Option<Expr<'a, Builtin>>,
}

/// `ALTER POLICY name ON table ...`
#[derive(Debug, Clone, PartialEq)]
pub struct AlterPolicy<'a> {
    pub name: Ident<'a>,
    pub table: ObjectName<'a>,
    pub rename_to: Option<Ident<'a>>,
    pub to: Vec<Ident<'a>>,
    pub using: Option<Expr<'a, Builtin>>,
    pub with_check: Option<Expr<'a, Builtin>>,
}

/// `[schema.]operator (left, right)`
#[derive(Debug, Clone, PartialEq)]
pub struct OperatorSignature<'a> {
    pub schema: Vec<Ident<'a>>,
    pub operator: Cow<'a, str>,
    pub left: DataType<'a>,
    pub right: DataType<'a>,
}

/// `INSERT` with clauses the core does not have.
#[derive(Debug, Clone, PartialEq)]
pub struct PostgresInsert<'a> {
    pub insert: Insert<'a, Builtin>,
    /// The table's alias: `INSERT INTO t AS x`.
    pub alias: Option<Ident<'a>>,
    /// `OVERRIDING {SYSTEM | USER} VALUE`, holding the middle word.
    pub overriding: Option<Cow<'a, str>>,
    pub on_conflict: Option<OnConflict<'a>>,
}

/// `ON CONFLICT [target] {DO NOTHING | DO UPDATE ...}`
#[derive(Debug, Clone, PartialEq)]
pub struct OnConflict<'a> {
    pub target: OnConflictTarget<'a>,
    pub predicate: Option<Expr<'a, Builtin>>,
    /// The `SET` assignments, or `None` for `DO NOTHING`.
    pub update: Option<OnConflictUpdate<'a>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum OnConflictTarget<'a> {
    None,
    Columns(Vec<OrderByItem<'a, Builtin>>),
    /// `ON CONSTRAINT name`
    Constraint(Ident<'a>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct OnConflictUpdate<'a> {
    pub assignments: Vec<Assignment<'a, Builtin>>,
    pub selection: Option<Expr<'a, Builtin>>,
}

/// Expressions only PostgreSQL has.
#[derive(Debug, Clone, PartialEq)]
pub enum PostgresExpr<'a> {
    /// A prefix operator not in the core, written with its operator text.
    Prefix {
        op: Cow<'a, str>,
        expr: Box<Expr<'a, Builtin>>,
    },
    /// `expr !`
    Postfix {
        expr: Box<Expr<'a, Builtin>>,
        op: Cow<'a, str>,
    },
    /// A binary operator not in the core.
    Binary(Box<PostgresBinary<'a>>),
    /// `ARRAY(subquery)`
    ArraySubquery(Box<Query<'a, Builtin>>),
    /// `XMLFOREST(a AS x, b)`
    XmlForest(Vec<(Expr<'a, Builtin>, Option<Ident<'a>>)>),
    /// `XMLPARSE(CONTENT expr)` or `XMLPARSE(DOCUMENT expr)`
    XmlParse {
        document: bool,
        value: Box<Expr<'a, Builtin>>,
    },
    /// `JSON_OBJECT(key VALUE value, ...)`, with `:` for `VALUE` if `colon`.
    JsonObject(Box<JsonObjectCall<'a>>),
}

/// `left OPERATOR(schema.op) right`, or a custom operator written directly.
#[derive(Debug, Clone, PartialEq)]
pub struct PostgresBinary<'a> {
    pub left: Expr<'a, Builtin>,
    /// `Some` if written with `OPERATOR(...)`.
    pub qualified: Option<OperatorName<'a>>,
    pub op: Cow<'a, str>,
    pub right: Expr<'a, Builtin>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct JsonObjectCall<'a> {
    pub entries: Vec<(Expr<'a, Builtin>, bool, Expr<'a, Builtin>)>,
    /// `Some(true)` for `NULL ON NULL`, `Some(false)` for `ABSENT ON NULL`.
    pub null_on_null: Option<bool>,
    pub returning: Option<DataType<'a>>,
}

/// A table with `ONLY` before it, or `*` after it.
#[derive(Debug, Clone, PartialEq)]
pub struct InheritedTable<'a> {
    pub only: bool,
    /// True if written `ONLY (name)`.
    pub parenthesized: bool,
    pub name: ObjectName<'a>,
    pub star: bool,
}

/// PostgreSQL's table factors.
#[derive(Debug, Clone, PartialEq)]
pub enum PostgresTableFactor<'a> {
    Inherited(InheritedTable<'a>),
}

/// `USING (columns) AS alias`
#[derive(Debug, Clone, PartialEq)]
pub struct PostgresUsingConstraint<'a> {
    pub columns: Vec<Ident<'a>>,
    pub alias: Ident<'a>,
}

impl fmt::Display for PostgresTableFactor<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PostgresTableFactor::Inherited(table) => {
                if table.only {
                    f.write_str("ONLY ")?;
                }
                if table.parenthesized {
                    write!(f, "({})", table.name)?;
                } else {
                    table.name.fmt(f)?;
                }
                if table.star {
                    f.write_str(" *")?;
                }
                Ok(())
            }
        }
    }
}

impl fmt::Display for PostgresUsingConstraint<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "USING({}) AS {}", comma(&self.columns), self.alias)
    }
}

fn parse_operator_signature<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<OperatorSignature<'a>> {
    let name = ddl::parse_operator_name(p)?;
    p.expect_op("(")?;
    let left = D::parse_data_type(p)?;
    p.expect_op(",")?;
    let right = D::parse_data_type(p)?;
    p.expect_op(")")?;
    Ok(OperatorSignature {
        schema: name.schema,
        operator: name.operator,
        left,
        right,
    })
}

fn parse_drop_operator<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<PostgresStatement<'a>> {
    p.advance();
    p.advance();
    if p.at_keyword("CLASS") || p.at_keyword("FAMILY") {
        return ddl::parse_drop_operator_group(p);
    }
    let if_exists = p.at_keyword("IF");
    if if_exists {
        p.advance();
        p.expect_keyword("EXISTS")?;
    }
    let operators = p.parse_comma_separated(parse_operator_signature)?;
    let behavior = if p.eat_keyword("CASCADE") {
        Some(DropBehavior::Cascade)
    } else if p.eat_keyword("RESTRICT") {
        Some(DropBehavior::Restrict)
    } else {
        None
    };
    Ok(PostgresStatement::DropOperator {
        if_exists,
        operators,
        behavior,
    })
}

impl fmt::Display for PostgresStatement<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PostgresStatement::DropOperator {
                if_exists,
                operators,
                behavior,
            } => {
                f.write_str("DROP OPERATOR ")?;
                if *if_exists {
                    f.write_str("IF EXISTS ")?;
                }
                for (i, operator) in operators.iter().enumerate() {
                    if i > 0 {
                        f.write_str(", ")?;
                    }
                    for part in &operator.schema {
                        write!(f, "{part}.")?;
                    }
                    write!(
                        f,
                        "{} ({}, {})",
                        operator.operator, operator.left, operator.right
                    )?;
                }
                ddl::write_behavior(f, *behavior)
            }
            PostgresStatement::DropOperatorGroup {
                family,
                if_exists,
                names,
                using,
                behavior,
            } => {
                write!(
                    f,
                    "DROP OPERATOR {}",
                    if *family { "FAMILY" } else { "CLASS" }
                )?;
                if *if_exists {
                    f.write_str(" IF EXISTS")?;
                }
                write!(f, " {} USING {using}", comma(names))?;
                ddl::write_behavior(f, *behavior)
            }
            PostgresStatement::CreateOperator { name, definition } => {
                write!(f, "CREATE OPERATOR {name} ({})", comma(definition))
            }
            PostgresStatement::CreateOperatorFamily { name, using } => {
                write!(f, "CREATE OPERATOR FAMILY {name} USING {using}")
            }
            PostgresStatement::CreateOperatorClass(class) => {
                write!(f, "CREATE OPERATOR CLASS {} ", class.name)?;
                if class.default {
                    f.write_str("DEFAULT ")?;
                }
                write!(f, "FOR TYPE {} USING {}", class.for_type, class.using)?;
                if let Some(family) = &class.family {
                    write!(f, " FAMILY {family}")?;
                }
                write!(f, " AS {}", comma(&class.items))
            }
            PostgresStatement::AlterOperator { name, types, op } => {
                let (left, right) = &**types;
                write!(f, "ALTER OPERATOR {name} ({left}, {right}) {op}")
            }
            PostgresStatement::AlterOperatorClass {
                family,
                name,
                using,
                op,
            } => write!(
                f,
                "ALTER OPERATOR {} {name} USING {using} {op}",
                if *family { "FAMILY" } else { "CLASS" }
            ),
            PostgresStatement::CreateAggregate(create) => create.fmt(f),
            PostgresStatement::AlterAggregate(alter) => {
                write!(
                    f,
                    "ALTER AGGREGATE {}({}) {}",
                    alter.name, alter.args, alter.action
                )
            }
            PostgresStatement::CreateFunction(create) => create.fmt(f),
            PostgresStatement::AlterFunction(keyword, alter) => {
                write!(f, "ALTER {keyword} {alter}")
            }
            PostgresStatement::CreateRole(create) => create.fmt(f),
            PostgresStatement::AlterRole(alter) => alter.fmt(f),
            PostgresStatement::CreateExtension(create) => create.fmt(f),
            PostgresStatement::CreateDomain(create) => create.fmt(f),
            PostgresStatement::CreateType(create) => create.fmt(f),
            PostgresStatement::AlterType(alter) => alter.fmt(f),
            PostgresStatement::CreateCollation(create) => create.fmt(f),
            PostgresStatement::AlterCollation(alter) => alter.fmt(f),
            PostgresStatement::CreateTextSearch {
                kind,
                name,
                definition,
            } => write!(
                f,
                "CREATE TEXT SEARCH {} {name} ({})",
                kind.keyword(),
                comma(definition)
            ),
            PostgresStatement::AlterTextSearch { kind, name, op } => {
                write!(f, "ALTER TEXT SEARCH {} {name} ", kind.keyword())?;
                match op {
                    AlterTextSearchOp::Options(options) => write!(f, "({})", comma(options)),
                    AlterTextSearchOp::Action(action) => action.fmt(f),
                }
            }
            PostgresStatement::CreateForeignDataWrapper { name, clauses } => {
                write!(f, "CREATE FOREIGN DATA WRAPPER {name}")?;
                for clause in clauses {
                    write!(f, " {clause}")?;
                }
                Ok(())
            }
            PostgresStatement::CreateServer(create) => create.fmt(f),
            PostgresStatement::CreateUserMapping(create) => create.fmt(f),
            PostgresStatement::AlterDefaultPrivileges(alter) => alter.fmt(f),
            PostgresStatement::CreateTrigger(create) => create.fmt(f),
            PostgresStatement::CreateRule(create) => create.fmt(f),
            PostgresStatement::CreateTable(create) => create.fmt(f),
            PostgresStatement::AlterRelation { keyword, alter } => {
                write!(f, "ALTER {keyword} ")?;
                if alter.if_exists {
                    f.write_str("IF EXISTS ")?;
                }
                if alter.only {
                    f.write_str("ONLY ")?;
                }
                write!(f, "{} {}", alter.name, comma(&alter.operations))
            }
            PostgresStatement::AlterSchema { name, action } => {
                write!(f, "ALTER SCHEMA {name} {action}")
            }
            PostgresStatement::Return(value) => write!(f, "RETURN {value}"),
            PostgresStatement::Copy(copy) => copy.fmt(f),
            PostgresStatement::Truncate(truncate) => truncate.fmt(f),
            PostgresStatement::Insert(insert) => insert.fmt(f),
            PostgresStatement::Declare(declare) => declare.fmt(f),
            PostgresStatement::Fetch(fetch) => fetch.fmt(f),
            PostgresStatement::Lock(lock) => lock.fmt(f),
            PostgresStatement::Prepare(prepare) => prepare.fmt(f),
            PostgresStatement::Execute(execute) => execute.fmt(f),
            PostgresStatement::Deallocate { prepare, name } => {
                f.write_str("DEALLOCATE ")?;
                if *prepare {
                    f.write_str("PREPARE ")?;
                }
                match name {
                    Some(name) => name.fmt(f),
                    None => f.write_str("ALL"),
                }
            }
            PostgresStatement::Do(items) => {
                f.write_str("DO")?;
                for item in items {
                    write!(f, " {item}")?;
                }
                Ok(())
            }
            PostgresStatement::Analyze { verbose, tables } => {
                f.write_str("ANALYZE")?;
                if *verbose {
                    f.write_str(" VERBOSE")?;
                }
                if !tables.is_empty() {
                    write!(f, " {}", comma(tables))?;
                }
                Ok(())
            }
            PostgresStatement::Notify { channel, payload } => {
                write!(f, "NOTIFY {channel}")?;
                if let Some(payload) = payload {
                    write!(f, ", {}", payload)?;
                }
                Ok(())
            }
            PostgresStatement::Discard(target) => write!(f, "DISCARD {target}"),
            PostgresStatement::Reset(ResetTarget::All) => f.write_str("RESET ALL"),
            PostgresStatement::Reset(ResetTarget::SessionAuthorization) => {
                f.write_str("RESET SESSION AUTHORIZATION")
            }
            PostgresStatement::Reset(ResetTarget::Name(name)) => write!(f, "RESET {name}"),
            PostgresStatement::Close(None) => f.write_str("CLOSE ALL"),
            PostgresStatement::Close(Some(name)) => write!(f, "CLOSE {name}"),
            PostgresStatement::Load(file) => write!(f, "LOAD {}", file),
            PostgresStatement::CreatePolicy(policy) => {
                write!(f, "CREATE POLICY {} ON {}", policy.name, policy.table)?;
                if let Some(ref kind) = policy.kind {
                    write!(f, " AS {kind}")?;
                }
                if let Some(ref command) = policy.command {
                    write!(f, " FOR {command}")?;
                }
                if !policy.to.is_empty() {
                    write!(f, " TO {}", comma(&policy.to))?;
                }
                if let Some(using) = &policy.using {
                    write!(f, " USING ({using})")?;
                }
                if let Some(check) = &policy.with_check {
                    write!(f, " WITH CHECK ({check})")?;
                }
                Ok(())
            }
            PostgresStatement::AlterPolicy(policy) => {
                write!(f, "ALTER POLICY {} ON {}", policy.name, policy.table)?;
                if let Some(new_name) = &policy.rename_to {
                    return write!(f, " RENAME TO {new_name}");
                }
                if !policy.to.is_empty() {
                    write!(f, " TO {}", comma(&policy.to))?;
                }
                if let Some(using) = &policy.using {
                    write!(f, " USING ({using})")?;
                }
                if let Some(check) = &policy.with_check {
                    write!(f, " WITH CHECK ({check})")?;
                }
                Ok(())
            }
            PostgresStatement::Listen(channel) => write!(f, "LISTEN {channel}"),
            PostgresStatement::Unlisten(Some(channel)) => write!(f, "UNLISTEN {channel}"),
            PostgresStatement::Unlisten(None) => f.write_str("UNLISTEN *"),
        }
    }
}

impl fmt::Display for PostgresInsert<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let insert = &self.insert;
        write!(f, "INSERT {}INTO {}", insert.modifiers, insert.table)?;
        if let Some(alias) = &self.alias {
            write!(f, " AS {alias}")?;
        }
        if !insert.columns.is_empty() {
            write!(f, " ({})", comma(&insert.columns))?;
        }
        if let Some(ref word) = self.overriding {
            write!(f, " OVERRIDING {word} VALUE")?;
        }
        write_insert_source(f, &insert.source, !insert.columns.is_empty())?;
        if let Some(on_conflict) = &self.on_conflict {
            write!(f, " {on_conflict}")?;
        }
        write_list_clause(f, "RETURNING", &insert.returning)
    }
}

impl fmt::Display for OnConflict<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ON CONFLICT")?;
        match &self.target {
            OnConflictTarget::None => {}
            OnConflictTarget::Columns(columns) => write!(f, "({})", comma(columns))?,
            OnConflictTarget::Constraint(name) => write!(f, " ON CONSTRAINT {name}")?,
        }
        if let Some(predicate) = &self.predicate {
            write!(f, " WHERE {predicate}")?;
        }
        match &self.update {
            None => f.write_str(" DO NOTHING"),
            Some(update) => {
                write!(f, " DO UPDATE SET {}", comma(&update.assignments))?;
                if let Some(selection) = &update.selection {
                    write!(f, " WHERE {selection}")?;
                }
                Ok(())
            }
        }
    }
}

impl fmt::Display for PostgresExpr<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PostgresExpr::Prefix { op, expr } if op == "!!" => write!(f, "!!{expr}"),
            PostgresExpr::Prefix { op, expr } => write!(f, "{op} {expr}"),
            PostgresExpr::Postfix { expr, op } => {
                let spaced = matches!(
                    &expr.kind,
                    ExprKind::Ext(BuiltinExpr::Postgres(PostgresExpr::Postfix { .. }))
                );
                write!(f, "{expr}{}{op}", if spaced { " " } else { "" })
            }
            PostgresExpr::Binary(binary) => {
                let PostgresBinary {
                    left,
                    qualified,
                    op,
                    right,
                } = &**binary;
                match qualified {
                    Some(name) => write!(f, "{left} OPERATOR({name}) {right}"),
                    None => write!(f, "{left} {op} {right}"),
                }
            }
            PostgresExpr::ArraySubquery(query) => write!(f, "ARRAY({query})"),
            PostgresExpr::XmlForest(items) => {
                f.write_str("XMLFOREST(")?;
                for (i, (expr, name)) in items.iter().enumerate() {
                    if i > 0 {
                        f.write_str(", ")?;
                    }
                    expr.fmt(f)?;
                    if let Some(name) = name {
                        write!(f, " AS {name}")?;
                    }
                }
                f.write_str(")")
            }
            PostgresExpr::XmlParse { document, value } => {
                write!(
                    f,
                    "XMLPARSE({} {value})",
                    if *document { "DOCUMENT" } else { "CONTENT" }
                )
            }
            PostgresExpr::JsonObject(call) => {
                f.write_str("JSON_OBJECT(")?;
                let mut sep = "";
                for (key, colon, value) in &call.entries {
                    write!(
                        f,
                        "{sep}{key}{}{value}",
                        if *colon { " : " } else { " VALUE " }
                    )?;
                    sep = ", ";
                }
                let mut gap = if call.entries.is_empty() { "" } else { " " };
                match call.null_on_null {
                    Some(true) => {
                        write!(f, "{gap}NULL ON NULL")?;
                        gap = " ";
                    }
                    Some(false) => {
                        write!(f, "{gap}ABSENT ON NULL")?;
                        gap = " ";
                    }
                    None => {}
                }
                if let Some(returning) = &call.returning {
                    write!(f, "{gap}RETURNING {returning}")?;
                }
                f.write_str(")")
            }
        }
    }
}

#[inline]
pub fn is_reserved(word: &str) -> bool {
    ["RETURNING", "ONLY", "NOTNULL", "ISNULL"]
        .iter()
        .any(|reserved| reserved.eq_ignore_ascii_case(word))
        || grammar::is_reserved(word)
}

pub fn parse_returning<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<Vec<SelectItem<'a, D::Ext>>> {
    if p.eat_keyword("RETURNING") {
        p.parse_comma_separated(Parser::parse_select_item)
    } else {
        Ok(Vec::new())
    }
}

/// The dispatch shared with [`super::redshift`]: what both dialects' `COPY`
/// and comments accept.
pub fn parse_statement<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Statement<'a, Builtin>> {
    let next = p.peek_nth(1);
    if p.at_keyword("DROP") && next.is_word("OPERATOR") {
        let drop = parse_drop_operator(p)?;
        return Ok(statement(drop));
    }
    if p.at_keyword("COMMENT") {
        return grammar::parse_comment_on_with(p, &[&["FOREIGN", "TABLE"]])
            .map(|c| StatementKind::Comment(Box::new(c)).into());
    }
    grammar::parse_statement(p)
}

/// Parses a statement with PostgreSQL's whole statement set.
pub fn parse_full_statement<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Statement<'a, Builtin>> {
    if p.at_keyword("ALTER") {
        let next = p.peek_nth(1);
        if next.is_word("INDEX") {
            return grammar::parse_alter_index_rename(p);
        }
        if next.is_word("MATERIALIZED") && p.peek_nth(2).is_word("VIEW") {
            p.advance();
            let alter = table::parse_alter_relation(p, "MATERIALIZED VIEW")?;
            return Ok(statement(alter));
        }
        if next.is_word("FOREIGN") && p.peek_nth(2).is_word("TABLE") {
            p.advance();
            let alter = table::parse_alter_relation(p, "FOREIGN TABLE")?;
            return Ok(statement(alter));
        }
        if next.is_word("SCHEMA") {
            p.advance();
            p.advance();
            let name = grammar::parse_object_name(p)?;
            if p.at_keyword("RENAME") || p.at_keyword("OWNER") {
                let action = ddl::parse_rename_or_owner(p)?;
                return Ok(statement(PostgresStatement::AlterSchema { name, action }));
            }
            return Err(p.expected("RENAME TO or OWNER TO"));
        }
        if !next.is_word("TABLE") {
            p.advance();
            if let Some(alter) = ddl::parse_alter_object(p)? {
                return Ok(statement(alter));
            }
            return Err(p.expected("TABLE"));
        }
    }
    if p.at_keyword("ABORT") {
        return grammar::parse_abort(p);
    }
    if let Some(parsed) = utility::parse_utility(p, utility::LEGACY_OPTIONS)? {
        return Ok(parsed);
    }
    parse_statement(p)
}

/// Parses `CREATE` with everything PostgreSQL adds.
pub fn parse_create<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Statement<'a, Builtin>> {
    let or_replace = p.peek_nth(1).is_word("OR") && p.peek_nth(2).is_word("REPLACE");
    let n = if or_replace { 3 } else { 1 };
    let is_foreign_table = p.peek_nth(n).is_word("FOREIGN") && p.peek_nth(n + 1).is_word("TABLE");
    if table::at_create_table_at(p, n) || is_foreign_table {
        p.advance();
        if or_replace {
            p.advance();
            p.advance();
        }
        return table::parse_create_table_statement(p, or_replace);
    }
    let word = p.peek_nth(n);
    let next = p.peek_nth(n + 1);
    let is_object = !or_replace
        && ([
            "OPERATOR",
            "EXTENSION",
            "DOMAIN",
            "TYPE",
            "COLLATION",
            "SERVER",
            "ROLE",
            "GROUP",
            "POLICY",
        ]
        .iter()
        .any(|kind| word.is_word(kind))
            || word.is_word("USER")
            || (word.is_word("TEXT") && next.is_word("SEARCH"))
            || (word.is_word("FOREIGN") && next.is_word("DATA")));
    let is_replaceable = ["AGGREGATE", "FUNCTION", "RULE", "TRIGGER"]
        .iter()
        .any(|kind| word.is_word(kind))
        || (word.is_word("CONSTRAINT") && next.is_word("TRIGGER"));
    if is_object || is_replaceable {
        p.advance();
        if or_replace {
            p.advance();
            p.advance();
        }
        if let Some(parsed) = ddl::parse_create_object(p, or_replace)? {
            return Ok(statement(parsed));
        }
        return Err(p.expected("an object type after CREATE"));
    }
    grammar::parse_create(p)
}

/// Parses a table and its alias, whose columns can carry types.
pub fn parse_table_ref<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<TableRef<'a, Builtin>> {
    let factor = D::parse_table_factor(p)?;
    let explicit = p.eat_keyword("AS");
    if !explicit && !p.at_implicit_alias() {
        return Ok(TableRef {
            factor,
            alias: None,
            span: Span::default(),
        });
    }
    let name = p.parse_ident()?;
    let mut columns = Vec::new();
    let mut column_types = Vec::new();
    if p.eat_op("(") {
        let items = p.parse_comma_separated(|p| {
            let column = p.parse_ident()?;
            let data_type = if p.at_op(",") || p.at_op(")") {
                None
            } else {
                Some(D::parse_data_type(p)?)
            };
            Ok((column, data_type))
        })?;
        p.expect_op(")")?;
        for (column, data_type) in items {
            columns.push(column);
            if let Some(data_type) = data_type {
                column_types.push(data_type);
            }
        }
        if !column_types.is_empty() && column_types.len() != columns.len() {
            return Err(p.expected("a type for each column"));
        }
    }
    Ok(TableRef {
        factor,
        alias: Some(TableAlias {
            name,
            explicit,
            columns,
            column_types,
        }),
        span: Span::default(),
    })
}

/// Parses an `ORDER BY` item, which can name an operator with `USING`.
pub fn parse_order_by_item<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<OrderByItem<'a, Builtin>> {
    let expr = p.parse_expr()?;
    let using = if p.eat_keyword("USING") {
        Some(Box::new(if p.at_keyword("OPERATOR") {
            p.advance();
            p.expect_op("(")?;
            let name = ddl::parse_operator_name(p)?;
            p.expect_op(")")?;
            name
        } else {
            ddl::parse_operator_name(p)?
        }))
    } else {
        None
    };
    let (asc, nulls_first) = grammar::parse_order_by_modifiers(p)?;
    Ok(OrderByItem {
        expr,
        asc,
        nulls_first,
        using,
        ext: Default::default(),
    })
}

/// Parses `WITH (name = value, name)`, in which a value is a simple one: a
/// literal or a word, unlike an expression such as a function call.
pub fn parse_storage_parameters<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<IndexStorage<'a, Builtin>> {
    let parameters = grammar::parse_index_storage_parameters(p)?;
    let plain = parameters.iter().all(|(_, value)| {
        matches!(
            value.as_ref().map(|value| &value.kind),
            None | Some(
                ExprKind::Literal(_)
                    | ExprKind::Ident(_)
                    | ExprKind::Unary { .. }
                    | ExprKind::TypedString(_)
            )
        )
    });
    if !plain {
        return Err(p.expected("a literal or a word as the value of a storage parameter"));
    }
    Ok(parameters)
}

/// Parses `CREATE [OR REPLACE] [TEMP] [MATERIALIZED] VIEW`, adding `WITH (...)`.
pub fn parse_create_view<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
    flags: CreateFlags,
) -> PResult<CreateView<'a, Builtin>> {
    let materialized = p.eat_keyword("MATERIALIZED");
    p.expect_keyword("VIEW")?;
    let if_not_exists = grammar::parse_if_not_exists(p)?;
    let name = grammar::parse_object_name(p)?;
    let columns = if p.at_op("(") {
        grammar::parse_ident_list(p)?
    } else {
        Vec::new()
    };
    let with = parse_storage_parameters(p)?;
    p.expect_keyword("AS")?;
    let query = Box::new(D::parse_query(p)?);
    Ok(CreateView {
        or_replace: flags.or_replace,
        temporary: flags.temporary,
        materialized,
        if_not_exists,
        name,
        columns,
        with,
        query,
        no_schema_binding: false,
    })
}

pub fn parse_alter_type_using<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<Option<Expr<'a, D::Ext>>> {
    if p.eat_keyword("USING") {
        p.parse_expr().map(Some)
    } else {
        Ok(None)
    }
}

/// Parses a column option, adding `CONSTRAINT name`, `NULLS [NOT] DISTINCT`
/// and `OPTIONS (...)`.
pub fn parse_column_option<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Option<ColumnOption<'a, Builtin>>> {
    if p.at_keyword("NULLS") && (p.peek_nth(1).is_word("DISTINCT") || p.peek_nth(1).is_word("NOT"))
    {
        p.advance();
        let not = p.eat_keyword("NOT");
        p.expect_keyword("DISTINCT")?;
        return Ok(Some(ColumnOption::NullsDistinct(!not)));
    }
    if p.at_keyword("OPTIONS") && p.peek_nth(1).is_op("(") {
        return ddl::parse_generic_options(p).map(|o| Some(ColumnOption::GenericOptions(o)));
    }
    if p.at_keyword("CONSTRAINT") {
        p.advance();
        let name = p.parse_ident()?;
        let Some(option) = parse_column_option(p)? else {
            return Err(p.expected("a column constraint"));
        };
        return Ok(Some(ColumnOption::Named {
            name,
            option: Box::new(option),
        }));
    }
    let option = grammar::parse_column_option(p)?;
    if let Some(ColumnOption::Generated(generated)) = &option
        && matches!(
            generated.kind,
            GeneratedKind::Expr { storage, .. } if storage != Some(GeneratedStorage::Stored)
        )
    {
        return Err(p.expected("STORED"));
    }
    Ok(option)
}

/// Parses `DROP`, rejecting `CASCADE` and `RESTRICT` on roles and groups.
pub fn parse_drop<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<DropObject<'a>> {
    let drop = grammar::parse_drop(p)?;
    if drop.behavior.is_some() && drop.object_type == ObjectType::Named("GROUP".into()) {
        return Err(p.expected("end of statement after the names"));
    }
    Ok(drop)
}

pub fn parse_set<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<SetStatement<'a, D::Ext>> {
    if grammar::at_set_names(p) {
        return grammar::parse_set_names(p);
    }
    grammar::parse_set(p)
}

pub fn parse_drop_object_type<'a, D: Dialect>(p: &mut Parser<'a, D>) -> Option<ObjectType<'a>> {
    if p.at_keyword("FOREIGN") && p.peek_nth(1).is_word("TABLE") {
        p.advance();
        p.advance();
        return Some(ObjectType::Named("FOREIGN TABLE".into()));
    }
    grammar::eat_drop_object_type(
        p,
        &[
            "EXTENSION",
            "TYPE",
            "COLLATION",
            "DOMAIN",
            "POLICY",
            "TRIGGER",
            "GROUP",
        ],
    )
}

/// True if a word can start a typed literal in `parse_prefix`: these reserved
/// words are also type or function names in PostgreSQL.
fn is_type_func_keyword(word: &str) -> bool {
    [
        "LEFT", "RIGHT", "FULL", "INNER", "CROSS", "NATURAL", "OUTER", "LIKE", "ILIKE",
    ]
    .iter()
    .any(|kw| kw.eq_ignore_ascii_case(word))
}

/// True at `type 'text'`, where `type` can be qualified.
fn at_typed_string<'a, D: Dialect>(p: &Parser<'a, D>) -> bool {
    let mut n = 0;
    loop {
        let name = p.peek_nth(n);
        if !matches!(name.kind, TokenKind::Word | TokenKind::QuotedIdent) {
            return false;
        }
        let after = p.peek_nth(n + 1);
        if after.is_op(".") {
            n += 2;
            continue;
        }
        if !matches!(after.kind, TokenKind::String | TokenKind::DollarString) {
            return false;
        }
        break;
    }
    let first = p.peek();
    match first.kind {
        TokenKind::QuotedIdent => first.text.len() > 2,
        TokenKind::Word => {
            !grammar::is_reserved(first.text) && !is_core_prefix_word(first.text)
                || is_type_func_keyword(first.text)
        }
        _ => false,
    }
}

/// Words the core reads as the start of an expression, not as a type.
fn is_core_prefix_word(word: &str) -> bool {
    [
        "COALESCE",
        "GREATEST",
        "LEAST",
        "NULLIF",
        "ROW",
        "POSITION",
        "SUBSTRING",
        "OVERLAY",
        "EXTRACT",
        "TRIM",
        "INTERVAL",
        "DATE",
        "TIME",
        "TIMESTAMP",
        "TIMESTAMPTZ",
        "DATETIME",
        "NOT",
        "NULL",
        "TRUE",
        "FALSE",
        "CASE",
        "CAST",
        "TRY_CAST",
        "EXISTS",
        "ARRAY",
    ]
    .iter()
    .any(|kw| kw.eq_ignore_ascii_case(word))
}

fn parse_typed_string<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Expr<'a, Builtin>> {
    let mut qualifier = Vec::new();
    let mut name = p.parse_name_part()?;
    while p.eat_op(".") {
        qualifier.push(name);
        name = p.parse_name_part()?;
    }
    let token = p.advance();
    let value = if token.kind == TokenKind::DollarString {
        let text = token.text.trim_start_matches(|c| c != '$');
        let body = &text[1..];
        let tag_len = body.find('$').unwrap_or(0);
        let text = &body[tag_len + 1..body.len() - tag_len - 2];
        crate::ast::StringLiteral {
            text: text.into(),
            quote: '$',
            backslash_escapes: false,
            national: false,
            prefix: StringPrefix::None,
            triple: false,
        }
    } else {
        D::string_literal(token)
    };
    Ok(ExprKind::TypedString(Box::new(TypedString {
        ty: DataType {
            qualifier,
            angle: None,
            name,
            args: Vec::new(),
            suffix: DataTypeSuffix::None,
            array: ArrayType::None,
        },
        value,
    }))
    .into())
}

/// Parses a prefix: PostgreSQL's prefix operators and typed strings, and
/// reads `0x1F` as the integer it is here rather than a hex string.
pub fn parse_prefix<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Expr<'a, D::Ext>> {
    let token = p.peek();
    if token.kind == TokenKind::Op && is_prefix_operator(token.text) {
        p.advance();
        let expr = p.parse_expr_bp(prec::UNARY)?;
        return Ok(ExprKind::Ext(BuiltinExpr::Postgres(PostgresExpr::Prefix {
            op: token.text.into(),
            expr: Box::new(expr),
        }))
        .into());
    }
    if token.is_op("-")
        && is_prefix_operator(p.peek_nth(1).text)
        && p.peek_nth(1).kind == TokenKind::Op
    {
        p.advance();
        let expr = p.parse_expr_bp(prec::UNARY)?;
        return Ok(ExprKind::Ext(BuiltinExpr::Postgres(PostgresExpr::Prefix {
            op: "-".into(),
            expr: Box::new(expr),
        }))
        .into());
    }
    if token.is_word("TRIM") && p.peek_nth(1).is_op("(") && p.peek_nth(2).is_word("FROM") {
        p.advance();
        p.advance();
        p.advance();
        let expr = p.parse_expr()?;
        p.expect_op(")")?;
        return Ok(ExprKind::Special(Box::new(SpecialExpr::Trim {
            side: None,
            what: None,
            expr,
        }))
        .into());
    }
    if matches!(token.kind, TokenKind::Word | TokenKind::QuotedIdent) && at_typed_string(p) {
        return parse_typed_string(p);
    }
    grammar::parse_prefix(p)
}

/// True for an operator that can start an expression.
fn is_prefix_operator(text: &str) -> bool {
    matches!(text, "~" | "@" | "#" | "|/" | "||/" | "!!")
}

/// Parses a call, reading the forms with their own syntax.
pub fn parse_function_call<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
    name: ObjectName<'a>,
) -> PResult<Expr<'a, Builtin>> {
    if let [only] = name.parts.as_slice()
        && only.quote.is_none()
        && let Some(expr) = parse_special_call(p, only.text.clone())?
    {
        return Ok(expr);
    }
    grammar::parse_function_call(p, name)
}

fn parse_special_call<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
    name: Cow<'a, str>,
) -> PResult<Option<Expr<'a, Builtin>>> {
    let eq = |word: &str| name.eq_ignore_ascii_case(word);
    let ext = |expr| ExprKind::Ext(BuiltinExpr::Postgres(expr)).into();
    if eq("ARRAY") && (p.peek_nth(1).is_word("SELECT") || p.peek_nth(1).is_word("WITH")) {
        p.advance();
        let query = p.nested(D::parse_query)?;
        p.expect_op(")")?;
        return Ok(Some(ext(PostgresExpr::ArraySubquery(Box::new(query)))));
    }
    if eq("XMLPARSE") && (p.peek_nth(1).is_word("CONTENT") || p.peek_nth(1).is_word("DOCUMENT")) {
        let document = p.peek_nth(1).is_word("DOCUMENT");
        p.advance();
        p.advance();
        let value = Box::new(p.parse_expr()?);
        p.expect_op(")")?;
        return Ok(Some(ext(PostgresExpr::XmlParse { document, value })));
    }
    if eq("XMLFOREST") {
        p.advance();
        let items = p.parse_comma_separated(|p| {
            let expr = p.parse_expr()?;
            let name = if p.eat_keyword("AS") {
                Some(p.parse_ident()?)
            } else {
                None
            };
            Ok((expr, name))
        })?;
        p.expect_op(")")?;
        return Ok(Some(ext(PostgresExpr::XmlForest(items))));
    }
    if eq("JSON_OBJECT") && at_json_object_syntax(p) {
        return parse_json_object(p)
            .map(|call| Some(ext(PostgresExpr::JsonObject(Box::new(call)))));
    }
    Ok(None)
}

/// True if the `JSON_OBJECT` call at `(` uses `key VALUE value`, `key : value`
/// or ends with `RETURNING` or `... ON NULL`, rather than `key, value` pairs.
fn at_json_object_syntax<D: Dialect>(p: &Parser<'_, D>) -> bool {
    let mut depth = 0usize;
    let mut n = 0;
    loop {
        let token = p.peek_nth(n);
        n += 1;
        match token.kind {
            TokenKind::Eof => return false,
            TokenKind::Op if token.text == "(" => depth += 1,
            TokenKind::Op if token.text == ")" => {
                depth -= 1;
                if depth == 0 {
                    return false;
                }
            }
            TokenKind::Op if depth == 1 && token.text == ":" => return true,
            TokenKind::Word
                if depth == 1
                    && (token.is_word("VALUE")
                        || token.is_word("RETURNING")
                        || (token.is_word("NULL") && p.peek_nth(n).is_word("ON"))
                        || token.is_word("ABSENT")) =>
            {
                return true;
            }
            _ => {}
        }
    }
}

fn parse_json_object<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<JsonObjectCall<'a>> {
    p.advance();
    let mut entries = Vec::new();
    if !p.at_keyword("RETURNING") && !p.at_keyword("NULL") && !p.at_keyword("ABSENT") {
        loop {
            let key = p.parse_expr()?;
            let colon = if p.eat_op(":") {
                true
            } else {
                p.expect_keyword("VALUE")?;
                false
            };
            let value = p.parse_expr()?;
            entries.push((key, colon, value));
            if !p.eat_op(",") {
                break;
            }
        }
    }
    let null_on_null = if p.at_keyword("NULL") {
        p.advance();
        p.expect_keyword("ON")?;
        p.expect_keyword("NULL")?;
        Some(true)
    } else if p.at_keyword("ABSENT") {
        p.advance();
        p.expect_keyword("ON")?;
        p.expect_keyword("NULL")?;
        Some(false)
    } else {
        None
    };
    let returning = if p.eat_keyword("RETURNING") {
        Some(D::parse_data_type(p)?)
    } else {
        None
    };
    p.expect_op(")")?;
    Ok(JsonObjectCall {
        entries,
        null_on_null,
        returning,
    })
}

/// Parses one argument of a call, adding `name := value`.
pub fn parse_function_arg<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<FunctionArg<'a, Builtin>> {
    if matches!(p.peek().kind, TokenKind::Word | TokenKind::QuotedIdent)
        && p.peek_nth(1).is_op(":=")
    {
        let name = p.parse_name_part()?;
        p.advance();
        let value = p.parse_expr()?;
        return Ok(FunctionArg::NamedWith {
            name,
            operator: ":=".into(),
            value,
        });
    }
    grammar::parse_function_arg(p)
}

/// Lexes PostgreSQL's prefixed strings, numbers and operators.
#[inline]
pub fn lex_postgres_token(rest: &str) -> Option<LexResult> {
    if let Some(result) = lex_prefixed_string(rest, true) {
        return Some(result);
    }
    if let Some(result) = lex_national_string(rest) {
        return Some(result);
    }
    if let Some(result) = lex_postgres_number(rest) {
        return Some(result);
    }
    if let Some(result) = lex_named_arg_or_colon(rest) {
        return Some(result);
    }
    if let Some(result) = lex_custom_operator(rest) {
        return Some(result);
    }
    lex_token(rest)
}

/// Lexes `N'text'`, in which a backslash has no special meaning.
fn lex_national_string(rest: &str) -> Option<LexResult> {
    if !matches!(rest.as_bytes(), [b'n' | b'N', b'\'', ..]) {
        return None;
    }
    Some(quoted(&rest[1..], TokenKind::String).map(|string| Lexeme {
        kind: TokenKind::String,
        len: string.len + 1,
    }))
}

/// Inside a subscript, `:` splits slice bounds even directly before a word.
pub fn lex_slice_colon(prev: Option<Token<'_>>, rest: &str) -> Option<LexResult> {
    let bytes = rest.as_bytes();
    let starts_bound = bytes[0] == b':'
        && bytes
            .get(1)
            .is_some_and(|&b| b.is_ascii_alphabetic() || b == b'_');
    let after_bound = prev.is_some_and(|token| {
        matches!(
            token.kind,
            TokenKind::Number | TokenKind::Word | TokenKind::QuotedIdent
        ) || token.is_op(")")
            || token.is_op("]")
            || token.is_op("[")
    });
    (starts_bound && after_bound).then_some(Ok(Lexeme {
        kind: TokenKind::Op,
        len: 1,
    }))
}

fn lex_named_arg_or_colon(rest: &str) -> Option<LexResult> {
    let bytes = rest.as_bytes();
    if bytes[0] != b':' {
        return None;
    }
    if bytes.get(1) == Some(&b'=') {
        return Some(Ok(Lexeme {
            kind: TokenKind::Op,
            len: 2,
        }));
    }
    if matches!(bytes.get(1), Some(b':')) {
        return None;
    }
    // `:name` remains a placeholder; `:` otherwise is a separator.
    match bytes.get(1) {
        Some(b) if b.is_ascii_alphabetic() || *b == b'_' => None,
        _ => Some(Ok(Lexeme {
            kind: TokenKind::Op,
            len: 1,
        })),
    }
}

/// Lexes `E'...'`, and `B'...'` and `U&'...'` when `postgres_only` allows.
#[inline]
pub fn lex_prefixed_string(rest: &str, postgres_only: bool) -> Option<LexResult> {
    let bytes = rest.as_bytes();
    match bytes[0] {
        b'e' | b'E' if bytes.get(1) == Some(&b'\'') => {
            let string = quoted_with_backslash(&rest[1..], TokenKind::String);
            Some(string.and_then(|l| {
                let body = &rest[2..l.len];
                crate::ast::unescape_escape_string(body, '\'')?;
                Ok(Lexeme {
                    kind: TokenKind::String,
                    len: l.len + 1,
                })
            }))
        }
        b'b' | b'B' if postgres_only && bytes.get(1) == Some(&b'\'') => {
            let string = quoted(&rest[1..], TokenKind::String);
            Some(string.map(|l| Lexeme {
                kind: TokenKind::String,
                len: l.len + 1,
            }))
        }
        b'u' | b'U'
            if postgres_only && bytes.get(1) == Some(&b'&') && bytes.get(2) == Some(&b'\'') =>
        {
            let string = quoted(&rest[2..], TokenKind::String);
            Some(string.map(|l| Lexeme {
                kind: TokenKind::String,
                len: l.len + 2,
            }))
        }
        _ => None,
    }
}

/// Builds a string literal from a token that may have a prefix.
#[inline]
pub fn string_literal<'a>(token: Token<'a>, backslash_escapes: bool) -> StringLiteral<'a> {
    let text = token.text;
    let (prefix, body, escapes) = match text.as_bytes()[0] {
        b'e' | b'E' => (StringPrefix::Escape, &text[1..], true),
        b'b' | b'B' => (StringPrefix::Bit, &text[1..], false),
        b'u' | b'U' => (StringPrefix::Unicode, &text[2..], false),
        b'n' | b'N' => (StringPrefix::None, &text[1..], false),
        _ => (StringPrefix::None, text, backslash_escapes),
    };
    StringLiteral {
        text: Cow::Borrowed(&body[1..body.len() - 1]),
        quote: char::from(body.as_bytes()[0]),
        backslash_escapes: escapes,
        national: matches!(text.as_bytes()[0], b'n' | b'N'),
        prefix,
        triple: false,
    }
}

/// Lexes PostgreSQL's `0x`, `0o` and `0b` integers and `_` digit separators.
fn lex_postgres_number(rest: &str) -> Option<LexResult> {
    let bytes = rest.as_bytes();
    if !bytes[0].is_ascii_digit() {
        return None;
    }
    if bytes[0] == b'0' && matches!(bytes.get(1), Some(b'x' | b'X' | b'o' | b'O' | b'b' | b'B')) {
        let radix = bytes[1].to_ascii_lowercase();
        let is_digit = |b: u8| match radix {
            b'x' => b.is_ascii_hexdigit(),
            b'o' => (b'0'..=b'7').contains(&b),
            _ => b == b'0' || b == b'1',
        };
        let mut len = 2;
        let mut digits = 0;
        while let Some(&b) = bytes.get(len) {
            if is_digit(b) {
                digits += 1;
                len += 1;
            } else if b == b'_' && bytes.get(len + 1).is_some_and(|&n| is_digit(n)) {
                len += 1;
            } else {
                break;
            }
        }
        if digits == 0 {
            return Some(Err("expected digits after the radix prefix"));
        }
        if bytes
            .get(len)
            .is_some_and(|&b| b.is_ascii_alphanumeric() || b == b'_')
        {
            return Some(Err("invalid number"));
        }
        return Some(Ok(Lexeme {
            kind: TokenKind::Number,
            len,
        }));
    }
    // Only numbers containing `_` are this lexer's business.
    let mut len = 0;
    let mut underscore = false;
    while let Some(&b) = bytes.get(len) {
        if b.is_ascii_digit() {
            len += 1;
        } else if b == b'_' {
            if !bytes.get(len + 1).is_some_and(u8::is_ascii_digit) {
                if bytes.get(len + 1) == Some(&b'_') {
                    return Some(Err("invalid number"));
                }
                break;
            }
            underscore = true;
            len += 1;
        } else {
            break;
        }
    }
    if !underscore {
        return None;
    }
    if crate::lexer::point_continues_number(bytes, len) {
        len += 1;
        loop {
            while bytes.get(len).is_some_and(u8::is_ascii_digit) {
                len += 1;
            }
            if bytes.get(len) == Some(&b'_') && bytes.get(len + 1).is_some_and(u8::is_ascii_digit) {
                len += 1;
            } else {
                break;
            }
        }
    }
    if bytes
        .get(len)
        .is_some_and(|&b| b.is_ascii_alphabetic() || b == b'_')
    {
        return Some(Err("invalid number"));
    }
    Some(Ok(Lexeme {
        kind: TokenKind::Number,
        len,
    }))
}

/// Lexes an operator of PostgreSQL's: the longest run of operator characters,
/// shortened so as not to contain `--` or `/*`, and so as not to end in `+` or
/// `-` unless it holds a character that only an operator can.
#[inline]
fn is_special_operator_byte(byte: u8) -> bool {
    matches!(
        byte,
        b'~' | b'!' | b'@' | b'#' | b'%' | b'^' | b'&' | b'|' | b'`' | b'?'
    )
}

/// True for a byte that can be part of an operator. This runs on every token.
#[inline]
fn is_operator_byte(byte: u8) -> bool {
    is_special_operator_byte(byte) || matches!(byte, b'+' | b'-' | b'*' | b'/' | b'<' | b'>' | b'=')
}

fn lex_custom_operator(rest: &str) -> Option<LexResult> {
    let bytes = rest.as_bytes();
    if !is_operator_byte(bytes[0]) {
        return None;
    }
    let mut len = bytes
        .iter()
        .position(|&b| !is_operator_byte(b))
        .unwrap_or(bytes.len());
    if let Some(at) = bytes[..len]
        .windows(2)
        .position(|pair| pair == b"--" || pair == b"/*")
    {
        len = at;
    }
    if len == 0 {
        return None;
    }
    if bytes[0] == b'!' {
        // These are `!` followed by operators of their own.
        const BANG_OPERATORS: [&str; 6] = ["!~~*", "!~~", "!~*", "!~", "!=", "!!"];
        let known = BANG_OPERATORS
            .iter()
            .find(|operator| rest[..len].starts_with(**operator));
        return Some(Ok(Lexeme {
            kind: TokenKind::Op,
            len: known.map_or(1, |operator| operator.len()),
        }));
    }
    while len > 1
        && matches!(bytes[len - 1], b'+' | b'-')
        && !bytes[..len].iter().any(|&b| is_special_operator_byte(b))
    {
        len -= 1;
    }
    Some(Ok(Lexeme {
        kind: TokenKind::Op,
        len,
    }))
}

/// Parses `COPY ... FROM STDIN` data: the lines after the one holding the `;`
/// that ended the statement, up to a line holding only `\.`.
pub fn lex_copy_payload(sql: &str, tokens: &[Token<'_>], pos: usize) -> Option<(usize, usize)> {
    // Only look at statements that end on the line just finished.
    let finished = sql[..pos].trim_end_matches(['\r', '\n']);
    let line_start = finished.rfind(['\n', '\r']).map_or(0, |at| at + 1);
    let last_line_tokens = tokens
        .iter()
        .rev()
        .take_while(|t| (t.span.start as usize) >= line_start)
        .count();
    let first_on_line = tokens.len() - last_line_tokens;
    let semicolon = (first_on_line..tokens.len()).find(|&i| tokens[i].is_op(";"))?;
    let statement_start = tokens[..semicolon]
        .iter()
        .rposition(|t| t.is_op(";"))
        .map_or(0, |at| at + 1);
    let statement = &tokens[statement_start..semicolon];
    if !statement.first()?.is_word("COPY") {
        return None;
    }
    let mut depth = 0i32;
    let mut from_stdin = false;
    for pair in statement.windows(2) {
        depth += i32::from(pair[0].is_op("(")) - i32::from(pair[0].is_op(")"));
        if depth == 0 && pair[0].is_word("FROM") && pair[1].is_word("STDIN") {
            from_stdin = true;
        }
    }
    if !from_stdin {
        return None;
    }
    let bytes = sql.as_bytes();
    let mut end = pos;
    loop {
        let line_end = sql[end..]
            .find(['\n', '\r'])
            .map_or(bytes.len(), |at| end + at);
        if &sql[end..line_end] == "\\." {
            let mut after = line_end;
            if bytes.get(after) == Some(&b'\r') {
                after += 1;
            }
            if bytes.get(after) == Some(&b'\n') {
                after += 1;
            }
            return Some((semicolon, after - pos));
        }
        if line_end == bytes.len() {
            return Some((semicolon, bytes.len() - pos));
        }
        end = line_end + 1;
        if bytes[line_end] == b'\r' && bytes.get(end) == Some(&b'\n') {
            end += 1;
        }
    }
}

/// Parses `INSERT`, adding an alias, `OVERRIDING` and `ON CONFLICT`.
pub fn parse_insert_statement<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Statement<'a, Builtin>> {
    p.expect_keyword("INSERT")?;
    let modifiers = D::parse_insert_modifiers(p)?;
    p.expect_keyword("INTO")?;
    let table = grammar::parse_object_name(p)?;
    let alias = if p.eat_keyword("AS") {
        Some(p.parse_ident()?)
    } else {
        None
    };
    let columns = if grammar::at_column_list(p) {
        grammar::parse_ident_list(p)?
    } else {
        Vec::new()
    };
    let overriding = if p.eat_keyword("OVERRIDING") {
        let word = ["SYSTEM", "USER"]
            .into_iter()
            .find(|word| p.eat_keyword(word))
            .ok_or_else(|| p.expected("SYSTEM or USER"))?;
        p.expect_keyword("VALUE")?;
        Some(word)
    } else {
        None
    };
    let source = D::parse_insert_source(p)?;
    grammar::check_default_values(p, &columns, &source)?;
    let on_conflict = if p.at_keyword("ON") && p.peek_nth(1).is_word("CONFLICT") {
        p.advance();
        p.advance();
        Some(parse_on_conflict(p)?)
    } else {
        None
    };
    let returning = D::parse_returning(p)?;
    let insert = Insert {
        modifiers,
        into: true,
        table,
        columns,
        output: None,
        source,
        returning,
    };
    if alias.is_none() && overriding.is_none() && on_conflict.is_none() {
        return Ok(StatementKind::Insert(Box::new(insert)).into());
    }
    Ok(statement(PostgresStatement::Insert(Box::new(
        PostgresInsert {
            insert,
            alias,
            overriding: overriding.map(Cow::Borrowed),
            on_conflict,
        },
    ))))
}

fn parse_on_conflict<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<OnConflict<'a>> {
    let target = if p.eat_op("(") {
        let columns = p.parse_comma_separated(D::parse_order_by_item)?;
        p.expect_op(")")?;
        OnConflictTarget::Columns(columns)
    } else if p.at_keyword("ON") {
        p.advance();
        p.expect_keyword("CONSTRAINT")?;
        OnConflictTarget::Constraint(p.parse_ident()?)
    } else {
        OnConflictTarget::None
    };
    let predicate = if matches!(target, OnConflictTarget::Columns(_)) {
        grammar::parse_optional_where(p)?
    } else {
        None
    };
    p.expect_keyword("DO")?;
    let update = if p.eat_keyword("NOTHING") {
        None
    } else {
        p.expect_keyword("UPDATE")?;
        p.expect_keyword("SET")?;
        let assignments = p.parse_comma_separated(D::parse_assignment)?;
        let selection = grammar::parse_optional_where(p)?;
        Some(OnConflictUpdate {
            assignments,
            selection,
        })
    };
    Ok(OnConflict {
        target,
        predicate,
        update,
    })
}

/// Parses `name AS [[NOT] MATERIALIZED] (query | DML)`.
pub fn parse_cte<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Cte<'a, Builtin>> {
    let name = p.parse_ident()?;
    let columns = D::parse_cte_columns(p)?;
    p.expect_keyword("AS")?;
    let materialized = if p.eat_keyword("MATERIALIZED") {
        Some(true)
    } else if p.at_keyword("NOT") && p.peek_nth(1).is_word("MATERIALIZED") {
        p.advance();
        p.advance();
        Some(false)
    } else {
        None
    };
    p.expect_op("(")?;
    let dml = p.at_keyword("INSERT") || p.at_keyword("UPDATE") || p.at_keyword("DELETE");
    let query = if dml {
        let statement = p.nested(Parser::parse_statement)?;
        Query {
            with: None,
            body: SetExpr::Statement(Box::new(statement)),
            order_by: Vec::new(),
            distribution: None,
            limit: None,
            fetch: None,
            locks: Vec::new(),
            tail: Default::default(),
            pipes: Vec::new(),
        }
    } else {
        p.nested(D::parse_query)?
    };
    p.expect_op(")")?;
    Ok(Cte {
        name,
        columns,
        materialized,
        query,
    })
}

/// True at a word that begins another join in a chain.
fn at_join_word<D: Dialect>(p: &Parser<'_, D>) -> bool {
    ["JOIN", "INNER", "LEFT", "RIGHT", "FULL", "CROSS", "NATURAL"]
        .iter()
        .any(|word| p.at_keyword(word))
}

/// Parses the next join, allowing `a JOIN b JOIN c ON x ON y`, in which the
/// second join nests inside the first without parentheses.
pub fn parse_join<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Option<Join<'a, Builtin>>> {
    if !at_join_word(p) {
        return Ok(None);
    }
    let natural = p.eat_keyword("NATURAL");
    let Some((kind, spelled_out)) = grammar::parse_join_kind(p)? else {
        return Err(p.expected("JOIN"));
    };
    if natural && kind == JoinKind::Cross {
        return Err(p.expected("a join type other than CROSS after NATURAL"));
    }
    let mut table = p.parse_table_ref()?;
    if !natural && kind != JoinKind::Cross && at_join_word(p) && !p.at_keyword("CROSS") {
        let relation = table;
        let inner = p.nested(|p| {
            let mut joins = Vec::new();
            while let Some(join) = D::parse_join(p)? {
                joins.push(join);
                if p.at_keyword("ON") || p.at_keyword("USING") {
                    break;
                }
            }
            Ok(joins)
        })?;
        table = TableRef {
            factor: TableFactor::NestedJoin(Box::new(TableWithJoins {
                relation,
                joins: inner,
            })),
            alias: None,
            span: Span::default(),
        };
    }
    let constraint = if natural {
        JoinConstraint::Natural
    } else if kind == JoinKind::Cross {
        JoinConstraint::None
    } else if p.at_keyword("USING") {
        p.advance();
        let columns = grammar::parse_ident_list(p)?;
        if p.eat_keyword("AS") {
            let alias = p.parse_ident()?;
            JoinConstraint::Ext(BuiltinJoinConstraint::Postgres(PostgresUsingConstraint {
                columns,
                alias,
            }))
        } else {
            JoinConstraint::Using(columns)
        }
    } else {
        grammar::parse_join_constraint(p)?
    };
    Ok(Some(Join {
        kind,
        spelled_out,
        table,
        constraint,
    }))
}

/// Parses a table factor, adding `ONLY` and the `*` after a table's name.
pub fn parse_table_factor<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<TableFactor<'a, Builtin>> {
    if grammar::at_xml_table(p) {
        return grammar::parse_xml_table(p, true);
    }
    if p.at_keyword("ONLY") {
        p.advance();
        let parenthesized = p.eat_op("(");
        let name = grammar::parse_object_name(p)?;
        if parenthesized {
            p.expect_op(")")?;
        }
        return Ok(TableFactor::Ext(BuiltinTableFactor::Postgres(Box::new(
            PostgresTableFactor::Inherited(InheritedTable {
                only: true,
                parenthesized,
                name,
                star: false,
            }),
        ))));
    }
    let factor = grammar::parse_table_factor(p)?;
    if p.at_op("*")
        && let TableFactor::Named(name) = factor
    {
        p.advance();
        return Ok(TableFactor::Ext(BuiltinTableFactor::Postgres(Box::new(
            PostgresTableFactor::Inherited(InheritedTable {
                only: false,
                parenthesized: false,
                name,
                star: true,
            }),
        ))));
    }
    Ok(factor)
}

/// The operators and `OPERATOR(...)` that only PostgreSQL has, or `None`.
fn postgres_infix_rule<'a, D: Dialect<Ext = Builtin>>(
    p: &Parser<'a, D>,
) -> Option<InfixRule<'a, D>> {
    let token = p.peek();
    if token.is_op("!") {
        return Some(InfixRule::custom(prec::POSTFIX, parse_factorial));
    }
    if token.kind == TokenKind::Word
        && token.is_word("OPERATOR")
        && p.peek_nth(1).is_op("(")
        && !matches!(p.peek_nth(2).kind, TokenKind::Eof)
    {
        return Some(InfixRule::custom(prec::OTHER, parse_operator_call));
    }
    if token.kind == TokenKind::Op && is_custom_binary(token.text) {
        return Some(InfixRule::custom(prec::OTHER, parse_custom_binary));
    }
    None
}

/// True for an operator text this module reads rather than the core.
fn is_custom_binary(text: &str) -> bool {
    if text == "!" || text == "(" || text == ")" {
        return false;
    }
    let known = operator_rule::<Postgres>(text).is_some()
        || matches!(
            text,
            "=" | "<>"
                | "!="
                | "<"
                | "<="
                | ">"
                | ">="
                | "+"
                | "-"
                | "*"
                | "/"
                | "%"
                | "["
                | "."
                | ","
                | ";"
                | "::"
                | ":="
                | ":"
                | "]"
        );
    !known
        && text
            .as_bytes()
            .iter()
            .all(|b| b"+-*/<>=~!@#%^&|`?".contains(b))
        && text.as_bytes().iter().any(|b| b"~!@#%^&|`?".contains(b))
}

fn parse_factorial<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
    left: Expr<'a, Builtin>,
    _bp: u8,
) -> PResult<Expr<'a, Builtin>> {
    let op = p.advance().text;
    Ok(ExprKind::Ext(BuiltinExpr::Postgres(PostgresExpr::Postfix {
        expr: Box::new(left),
        op: op.into(),
    }))
    .into())
}

fn parse_custom_binary<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
    left: Expr<'a, Builtin>,
    bp: u8,
) -> PResult<Expr<'a, Builtin>> {
    let op = p.advance().text;
    let right = p.parse_expr_bp(bp)?;
    Ok(
        ExprKind::Ext(BuiltinExpr::Postgres(PostgresExpr::Binary(Box::new(
            PostgresBinary {
                left,
                qualified: None,
                op: op.into(),
                right,
            },
        ))))
        .into(),
    )
}

fn parse_operator_call<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
    left: Expr<'a, Builtin>,
    bp: u8,
) -> PResult<Expr<'a, Builtin>> {
    p.advance();
    p.expect_op("(")?;
    let operator = ddl::parse_operator_name(p)?;
    p.expect_op(")")?;
    let right = p.parse_expr_bp(bp)?;
    let op = operator.operator.clone();
    Ok(
        ExprKind::Ext(BuiltinExpr::Postgres(PostgresExpr::Binary(Box::new(
            PostgresBinary {
                left,
                qualified: Some(operator),
                op,
                right,
            },
        ))))
        .into(),
    )
}

/// Parses `EXTRACT`'s field: a word, or a string as PostgreSQL allows.
pub fn parse_extract_field<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<&'a str> {
    let token = p.peek();
    match token.kind {
        TokenKind::String | TokenKind::Word | TokenKind::QuotedIdent => {
            p.advance();
            Ok(token.text)
        }
        _ => Err(p.expected("a date or time field")),
    }
}

/// True if `word` is an interval qualifier in PostgreSQL's `INTERVAL` literals.
fn is_postgres_interval_unit(word: &str) -> bool {
    ["YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND"]
        .iter()
        .any(|unit| unit.eq_ignore_ascii_case(word))
}

/// Rejects `GRANT MAINTAIN (columns)`, which has no column form.
pub fn parse_grant<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Grant<'a>> {
    let grant = grammar::parse_grant(p)?;
    if let Privileges::List(list) = &grant.privileges
        && list.iter().any(|privilege| {
            !privilege.columns.is_empty()
                && privilege.words.len() == 1
                && privilege.words[0].eq_ignore_ascii_case("MAINTAIN")
        })
    {
        return Err(p.expected("a privilege without columns for MAINTAIN"));
    }
    Ok(grant)
}

impl Dialect for Postgres {
    type Ext = Builtin;

    fn parse_select_item_alias<'a>(
        p: &mut Parser<'a, Self>,
        expr: Expr<'a, Builtin>,
    ) -> PResult<SelectItem<'a, Builtin>> {
        grammar::parse_select_item_alias_any_word(p, expr)
    }

    fn parse_table_ref<'a>(p: &mut Parser<'a, Self>) -> PResult<TableRef<'a, Builtin>> {
        parse_table_ref(p)
    }

    fn parse_order_by_item<'a>(p: &mut Parser<'a, Self>) -> PResult<OrderByItem<'a, Builtin>> {
        parse_order_by_item(p)
    }

    fn parse_update_from_after_set<'a>(
        p: &mut Parser<'a, Self>,
    ) -> PResult<Vec<TableWithJoins<'a, Builtin>>> {
        grammar::parse_optional_update_from(p)
    }

    fn parse_delete_using<'a>(
        p: &mut Parser<'a, Self>,
    ) -> PResult<Vec<TableWithJoins<'a, Builtin>>> {
        grammar::parse_optional_delete_using(p)
    }

    fn parse_select_into<'a>(
        p: &mut Parser<'a, Self>,
    ) -> PResult<Option<Box<SelectInto<'a, Builtin>>>> {
        grammar::parse_select_into_table(p)
    }

    fn eat_limit_all<'a>(p: &mut Parser<'a, Self>) -> bool {
        p.eat_keyword("ALL")
    }

    fn parse_is_test<'a>(p: &mut Parser<'a, Self>) -> PResult<Option<IsTest<'a, Builtin>>> {
        if let Some(test) = grammar::parse_is_json(p)? {
            return Ok(Some(test));
        }
        grammar::parse_is_normalized(p)
    }

    fn parse_create_table_query<'a>(p: &mut Parser<'a, Self>) -> PResult<Query<'a, Builtin>> {
        grammar::parse_table_or_query(p)
    }

    fn parse_statement_after_with<'a>(
        p: &mut Parser<'a, Self>,
    ) -> PResult<Option<Statement<'a, Builtin>>> {
        grammar::parse_dml_after_with(p, true, true)
    }

    fn parse_insert_source<'a>(p: &mut Parser<'a, Self>) -> PResult<InsertSource<'a, Builtin>> {
        grammar::parse_insert_source_with_default(p)
    }

    fn parse_group_by_item<'a>(p: &mut Parser<'a, Self>) -> PResult<Expr<'a, Builtin>> {
        grammar::parse_grouping_item_or_empty_set(p)
    }

    fn parse_create_schema<'a>(
        p: &mut Parser<'a, Self>,
        flags: CreateFlags,
    ) -> PResult<CreateSchema<'a>> {
        if flags != CreateFlags::default() {
            return Err(p.expected("TABLE or VIEW"));
        }
        grammar::parse_create_schema_authorization(p, true)
    }

    fn lex_after(prev: Option<Token<'_>>, rest: &str) -> Option<LexResult> {
        lex_slice_colon(prev, rest)
    }

    fn parse_prefix<'a>(p: &mut Parser<'a, Self>) -> PResult<Expr<'a, Builtin>> {
        parse_prefix(p)
    }

    fn parse_statement<'a>(p: &mut Parser<'a, Self>) -> PResult<Statement<'a, Builtin>> {
        parse_full_statement(p)
    }

    fn parse_insert_statement<'a>(p: &mut Parser<'a, Self>) -> PResult<Statement<'a, Builtin>> {
        parse_insert_statement(p)
    }

    fn parse_create<'a>(p: &mut Parser<'a, Self>) -> PResult<Statement<'a, Builtin>> {
        parse_create(p)
    }

    fn parse_create_view<'a>(
        p: &mut Parser<'a, Self>,
        flags: CreateFlags,
    ) -> PResult<CreateView<'a, Builtin>> {
        parse_create_view(p, flags)
    }

    fn parse_table_constraint<'a>(
        p: &mut Parser<'a, Self>,
    ) -> PResult<Option<TableConstraint<'a, Builtin>>> {
        table::parse_table_constraint(p)
    }

    fn parse_index_flags<'a>(p: &mut Parser<'a, Self>) -> IndexFlags {
        parse_index_flags(p)
    }

    fn parse_index_storage<'a>(p: &mut Parser<'a, Self>) -> PResult<IndexStorage<'a, Builtin>> {
        grammar::parse_index_storage_parameters(p)
    }

    fn parse_alter_type_using<'a>(p: &mut Parser<'a, Self>) -> PResult<Option<Expr<'a, Builtin>>> {
        parse_alter_type_using(p)
    }

    fn parse_alter_table_op<'a>(p: &mut Parser<'a, Self>) -> PResult<AlterTableOp<'a, Builtin>> {
        if let Some(op) = table::parse_alter_table_op_extra(p)? {
            return Ok(op);
        }
        parse_alter_table_op(p)
    }

    fn parse_column_option<'a>(
        p: &mut Parser<'a, Self>,
    ) -> PResult<Option<ColumnOption<'a, Builtin>>> {
        parse_column_option(p)
    }

    fn parse_set<'a>(p: &mut Parser<'a, Self>) -> PResult<SetStatement<'a, Builtin>> {
        parse_set(p)
    }

    fn parse_drop<'a>(p: &mut Parser<'a, Self>) -> PResult<DropObject<'a>> {
        parse_drop(p)
    }

    fn parse_explain_options<'a>(p: &mut Parser<'a, Self>) -> PResult<Vec<ExplainOption<'a>>> {
        grammar::parse_explain_option_list(p)
    }

    fn is_niladic_function(word: &str) -> bool {
        is_niladic_function(word)
    }

    fn parse_drop_object_type<'a>(p: &mut Parser<'a, Self>) -> Option<ObjectType<'a>> {
        parse_drop_object_type(p)
    }

    fn parse_interval<'a>(p: &mut Parser<'a, Self>) -> PResult<Expr<'a, Builtin>> {
        grammar::parse_interval_with(p, false, is_postgres_interval_unit)
    }

    fn parse_data_type<'a>(p: &mut Parser<'a, Self>) -> PResult<DataType<'a>> {
        parse_data_type(p)
    }

    fn parse_alter_table_only<'a>(p: &mut Parser<'a, Self>) -> bool {
        p.eat_keyword("ONLY")
    }

    fn parse_add_column_if_not_exists<'a>(p: &mut Parser<'a, Self>) -> PResult<bool> {
        grammar::parse_if_not_exists(p)
    }

    #[inline]
    fn is_reserved(word: &str) -> bool {
        is_reserved(word)
    }

    #[inline]
    fn is_reserved_for_alias(word: &str) -> bool {
        word.eq_ignore_ascii_case("END") || grammar::is_reserved_for_alias(word)
    }

    fn parse_returning<'a>(p: &mut Parser<'a, Self>) -> PResult<Vec<SelectItem<'a, Builtin>>> {
        parse_returning(p)
    }

    #[inline]
    fn lex_token(rest: &str) -> Option<LexResult> {
        lex_postgres_token(rest)
    }

    #[inline]
    fn lex_payload_line(sql: &str, tokens: &[Token<'_>], pos: usize) -> Option<(usize, usize)> {
        lex_copy_payload(sql, tokens, pos)
    }

    #[inline]
    fn string_literal(token: Token<'_>) -> StringLiteral<'_> {
        string_literal(token, false)
    }

    fn infix_rule<'a>(p: &Parser<'a, Self>) -> Option<InfixRule<'a, Self>> {
        if p.at_op("[") {
            return Some(InfixRule::custom(
                prec::POSTFIX,
                grammar::parse_subscript_or_slice,
            ));
        }
        postgres_infix_rule(p).or_else(|| infix_rule(p))
    }

    fn parse_select_modifiers<'a>(p: &mut Parser<'a, Self>) -> PResult<BuiltinSelect<'a>> {
        parse_distinct_on(p)
    }

    fn parse_cte<'a>(p: &mut Parser<'a, Self>) -> PResult<Cte<'a, Builtin>> {
        parse_cte(p)
    }

    /// `SELECT FROM t` selects no columns.
    fn parse_projection<'a>(p: &mut Parser<'a, Self>) -> PResult<Vec<SelectItem<'a, Builtin>>> {
        if p.at_keyword("FROM") {
            return Ok(Vec::new());
        }
        p.parse_comma_separated(Parser::parse_select_item)
    }

    fn parse_join<'a>(p: &mut Parser<'a, Self>) -> PResult<Option<Join<'a, Builtin>>> {
        parse_join(p)
    }

    fn parse_table_factor<'a>(p: &mut Parser<'a, Self>) -> PResult<TableFactor<'a, Builtin>> {
        parse_table_factor(p)
    }

    fn parse_function_call<'a>(
        p: &mut Parser<'a, Self>,
        name: ObjectName<'a>,
    ) -> PResult<Expr<'a, Builtin>> {
        parse_function_call(p, name)
    }

    fn parse_function_arg<'a>(p: &mut Parser<'a, Self>) -> PResult<FunctionArg<'a, Builtin>> {
        parse_function_arg(p)
    }

    fn parse_extract_field<'a>(p: &mut Parser<'a, Self>) -> PResult<&'a str> {
        parse_extract_field(p)
    }

    fn parse_grant<'a>(p: &mut Parser<'a, Self>) -> PResult<Grant<'a>> {
        parse_grant(p)
    }

    fn parse_limit_after_locks<'a>(
        p: &mut Parser<'a, Self>,
    ) -> PResult<Option<Box<Limit<'a, Builtin>>>> {
        if p.eat_keyword("LIMIT") {
            return Self::parse_limit(p).map(|limit| Some(Box::new(limit)));
        }
        Ok(None)
    }
}

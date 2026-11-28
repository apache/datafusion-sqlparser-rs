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

//! Objects only PostgreSQL has: operators, aggregates, functions, types,
//! roles, text search, foreign data, triggers and rules.

use std::borrow::Cow;
use std::fmt;

use crate::PResult;
use crate::ast::{
    ColumnDef, ColumnOption, ConstraintAttribute, DataType, DropBehavior, Expr, ExprKind,
    GenericOption, Grantee, Ident, Literal, ObjectName, OperatorName, Privileges, RoutineArg,
    RoutineSignature, Statement, StatementKind, StringLiteral,
};
use crate::dialect::postgres::{AlterPolicy, CreatePolicy, PostgresStatement};
use crate::dialect::{Builtin, Dialect};
use crate::display::comma;
use crate::grammar::{self, prec};
use crate::lexer::TokenKind;
use crate::parser::Parser;

type Exp<'a> = Expr<'a, Builtin>;
type Arg<'a> = RoutineArg<'a, Builtin>;
/// The expressions of `USING (...)` and `WITH CHECK (...)`.
type PolicyConditions<'a> = (Option<Exp<'a>>, Option<Exp<'a>>);

/// An element of a `(name [= value], ...)` definition list.
#[derive(Debug, Clone, PartialEq)]
pub struct DefElem<'a> {
    pub name: Ident<'a>,
    pub value: Option<DefArg<'a>>,
}

/// The value of a definition element, which can name a type, an operator, a
/// function or a word, or be a number or string.
#[derive(Debug, Clone, PartialEq)]
pub enum DefArg<'a> {
    /// A type or function name, which PostgreSQL reads identically.
    Type(DataType<'a>),
    /// `[schema.]operator`
    Operator(OperatorName<'a>),
    /// `OPERATOR([schema.]operator)`
    OperatorCall(OperatorName<'a>),
    Number(Cow<'a, str>),
    String(StringLiteral<'a>),
    /// A reserved word used as a value.
    Word(Cow<'a, str>),
}

/// `RENAME TO`, `OWNER TO` or `SET SCHEMA`.
#[derive(Debug, Clone, PartialEq)]
pub enum ObjectAction<'a> {
    Rename(Ident<'a>),
    Owner(Ident<'a>),
    SetSchema(Ident<'a>),
}

/// The name, method and items of `CREATE OPERATOR CLASS`.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateOperatorClass<'a> {
    pub name: ObjectName<'a>,
    pub default: bool,
    pub for_type: DataType<'a>,
    pub using: Ident<'a>,
    pub family: Option<ObjectName<'a>>,
    pub items: Vec<OperatorClassItem<'a>>,
}

/// One item of an operator class or family.
#[derive(Debug, Clone, PartialEq)]
pub enum OperatorClassItem<'a> {
    Operator {
        strategy: Cow<'a, str>,
        operator: OperatorName<'a>,
        types: Option<Box<(DataType<'a>, DataType<'a>)>>,
        /// `FOR SEARCH`, or `FOR ORDER BY` this family.
        purpose: Option<OperatorPurpose<'a>>,
    },
    Function {
        support: Cow<'a, str>,
        /// The optional types written directly after the support number.
        op_types: Vec<DataType<'a>>,
        name: Option<ObjectName<'a>>,
        args: Option<Vec<DataType<'a>>>,
    },
    Storage(DataType<'a>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum OperatorPurpose<'a> {
    Search,
    OrderBy(ObjectName<'a>),
}

/// `CREATE OPERATOR CLASS` and `FAMILY`, `ALTER` and `DROP`.
#[derive(Debug, Clone, PartialEq)]
pub enum AlterFamilyOp<'a> {
    Add(Vec<OperatorClassItem<'a>>),
    Drop(Vec<OperatorClassItem<'a>>),
    Action(ObjectAction<'a>),
}

/// `(*)`, `(args)`, `(args ORDER BY args)` or `(ORDER BY args)`.
#[derive(Debug, Clone, PartialEq)]
pub enum AggregateArgs<'a> {
    Star,
    List {
        args: Vec<Arg<'a>>,
        order_by: Vec<Arg<'a>>,
    },
}

/// `CREATE [OR REPLACE] AGGREGATE`
#[derive(Debug, Clone, PartialEq)]
pub struct CreateAggregate<'a> {
    pub or_replace: bool,
    pub name: ObjectName<'a>,
    /// `None` for the legacy form, whose definition holds `BASETYPE`.
    pub args: Option<AggregateArgs<'a>>,
    pub definition: Vec<DefElem<'a>>,
}

/// `ALTER AGGREGATE name (args) ...`
#[derive(Debug, Clone, PartialEq)]
pub struct AlterAggregate<'a> {
    pub name: ObjectName<'a>,
    pub args: AggregateArgs<'a>,
    pub action: ObjectAction<'a>,
}

/// `RETURNS` in `CREATE FUNCTION`.
#[derive(Debug, Clone, PartialEq)]
pub enum FunctionReturn<'a> {
    Type(DataType<'a>),
    SetOf(DataType<'a>),
    Table(Vec<ColumnDef<'a, Builtin>>),
}

/// `SET parameter ...`, in a function or role.
#[derive(Debug, Clone, PartialEq)]
pub struct ParameterSetting<'a> {
    pub name: ObjectName<'a>,
    pub value: ParameterValue<'a>,
    /// True if written with `TO` rather than `=`.
    pub to: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ParameterValue<'a> {
    Default,
    FromCurrent,
    Values(Vec<Exp<'a>>),
}

/// The body of `CREATE FUNCTION`.
#[derive(Debug, Clone, PartialEq)]
pub enum FunctionBody<'a> {
    /// `AS 'text'[, 'link_symbol']`
    As {
        body: Literal<'a>,
        link_symbol: Option<Literal<'a>>,
    },
    /// `RETURN expr`
    Return(Exp<'a>),
    /// `BEGIN ATOMIC statement; ... END`
    BeginAtomic(Vec<Statement<'a, Builtin>>),
}

/// `CREATE [OR REPLACE] FUNCTION`
#[derive(Debug, Clone, PartialEq)]
pub struct CreateFunction<'a> {
    pub or_replace: bool,
    pub name: ObjectName<'a>,
    pub args: Vec<Arg<'a>>,
    pub returns: Option<FunctionReturn<'a>>,
    pub language: Option<Ident<'a>>,
    pub window: bool,
    /// `IMMUTABLE`, `STABLE` or `VOLATILE`.
    pub volatility: Option<Cow<'a, str>>,
    /// `CALLED ON NULL INPUT`, `RETURNS NULL ON NULL INPUT` or `STRICT`.
    pub null_input: Option<Cow<'a, str>>,
    /// `SAFE`, `UNSAFE` or `RESTRICTED`.
    pub parallel: Option<Cow<'a, str>>,
    /// `DEFINER` or `INVOKER`.
    pub security: Option<Cow<'a, str>>,
    pub leakproof: Option<bool>,
    pub cost: Option<Cow<'a, str>>,
    pub rows: Option<Cow<'a, str>>,
    pub support: Option<ObjectName<'a>>,
    pub set: Vec<ParameterSetting<'a>>,
    pub body: Option<FunctionBody<'a>>,
}

/// An `ALTER FUNCTION` action, in the order written.
#[derive(Debug, Clone, PartialEq)]
pub enum FunctionAttr<'a> {
    Volatility(Cow<'a, str>),
    NullInput(Cow<'a, str>),
    Parallel(Cow<'a, str>),
    Security {
        external: bool,
        kind: Cow<'a, str>,
    },
    Leakproof(bool),
    Cost(Cow<'a, str>),
    Rows(Cow<'a, str>),
    Support(ObjectName<'a>),
    Set(ParameterSetting<'a>),
    /// `RESET name` or `RESET ALL`, the second with `None`.
    Reset(Option<ObjectName<'a>>),
    Restrict,
}

/// What `ALTER FUNCTION` changes.
#[derive(Debug, Clone, PartialEq)]
pub enum AlterFunctionOp<'a> {
    Action(ObjectAction<'a>),
    /// `[NO] DEPENDS ON EXTENSION name`
    DependsOn {
        no: bool,
        extension: Ident<'a>,
    },
    Attributes(Vec<FunctionAttr<'a>>),
}

/// `ALTER FUNCTION signature ...`
#[derive(Debug, Clone, PartialEq)]
pub struct AlterFunction<'a> {
    pub signature: RoutineSignature<'a, Builtin>,
    pub op: AlterFunctionOp<'a>,
}

/// One clause of `CREATE ROLE` or `ALTER ROLE`.
#[derive(Debug, Clone, PartialEq)]
pub enum RoleOption<'a> {
    /// `SUPERUSER`, `NOLOGIN`, ... kept as written.
    Flag(Cow<'a, str>),
    ConnectionLimit(Cow<'a, str>),
    Password(Option<StringLiteral<'a>>),
    ValidUntil(StringLiteral<'a>),
    /// `IN ROLE`, `ROLE`, `ADMIN` and `USER` lists, holding their keyword.
    Members {
        keyword: Cow<'a, str>,
        names: Vec<Ident<'a>>,
    },
    Sysid(Cow<'a, str>),
}

/// `CREATE ROLE`, `USER` or `GROUP`.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateRole<'a> {
    /// `ROLE`, `USER` or `GROUP`.
    pub keyword: Cow<'a, str>,
    pub name: Ident<'a>,
    pub options: Vec<RoleOption<'a>>,
}

/// What `ALTER ROLE` does.
#[derive(Debug, Clone, PartialEq)]
pub enum AlterRoleOp<'a> {
    Rename(Ident<'a>),
    Options { options: Vec<RoleOption<'a>> },
    Set(ParameterSetting<'a>),
    Reset(Option<ObjectName<'a>>),
}

/// `ALTER ROLE`, `USER` or `GROUP`.
#[derive(Debug, Clone, PartialEq)]
pub struct AlterRole<'a> {
    pub keyword: Cow<'a, str>,
    pub name: Ident<'a>,
    pub in_database: Option<Ident<'a>>,
    pub op: AlterRoleOp<'a>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ExtensionVersion<'a> {
    Name(Ident<'a>),
    String(StringLiteral<'a>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateExtension<'a> {
    pub if_not_exists: bool,
    pub name: Ident<'a>,
    pub with: bool,
    pub schema: Option<Ident<'a>>,
    pub version: Option<ExtensionVersion<'a>>,
    pub cascade: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateDomain<'a> {
    pub name: ObjectName<'a>,
    /// True if `AS` was written.
    pub as_keyword: bool,
    pub data_type: DataType<'a>,
    pub options: Vec<ColumnOption<'a, Builtin>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum CreateType<'a> {
    Enum {
        name: ObjectName<'a>,
        labels: Vec<StringLiteral<'a>>,
    },
    Composite {
        name: ObjectName<'a>,
        columns: Vec<ColumnDef<'a, Builtin>>,
    },
    Range {
        name: ObjectName<'a>,
        definition: Vec<DefElem<'a>>,
    },
    Base {
        name: ObjectName<'a>,
        definition: Vec<DefElem<'a>>,
    },
    Shell {
        name: ObjectName<'a>,
    },
}

/// `ALTER TYPE name ...`
#[derive(Debug, Clone, PartialEq)]
pub struct AlterType<'a> {
    pub name: ObjectName<'a>,
    pub op: AlterTypeOp<'a>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AlterTypeOp<'a> {
    Action(ObjectAction<'a>),
    AddValue {
        if_not_exists: bool,
        value: StringLiteral<'a>,
        /// `true` for `BEFORE`, `false` for `AFTER`, with the neighbour.
        position: Option<(bool, StringLiteral<'a>)>,
    },
    RenameValue {
        old: StringLiteral<'a>,
        new: StringLiteral<'a>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateCollation<'a> {
    pub if_not_exists: bool,
    pub name: ObjectName<'a>,
    pub source: CollationSource<'a>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum CollationSource<'a> {
    Definition(Vec<DefElem<'a>>),
    From(ObjectName<'a>),
}

/// `ALTER COLLATION name ...`
#[derive(Debug, Clone, PartialEq)]
pub struct AlterCollation<'a> {
    pub name: ObjectName<'a>,
    pub op: AlterCollationOp<'a>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AlterCollationOp<'a> {
    Action(ObjectAction<'a>),
    RefreshVersion,
}

/// The four text search object kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TextSearchKind {
    Configuration,
    Dictionary,
    Parser,
    Template,
}

/// `ALTER TEXT SEARCH kind name ...`
#[derive(Debug, Clone, PartialEq)]
pub enum AlterTextSearchOp<'a> {
    Options(Vec<DefElem<'a>>),
    Action(ObjectAction<'a>),
}

/// A clause of `CREATE FOREIGN DATA WRAPPER`.
#[derive(Debug, Clone, PartialEq)]
pub enum FdwClause<'a> {
    Handler(Option<Ident<'a>>),
    Validator(Option<Ident<'a>>),
    Options(Vec<GenericOption<'a>>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateServer<'a> {
    pub if_not_exists: bool,
    pub name: Ident<'a>,
    pub server_type: Option<StringLiteral<'a>>,
    pub version: Option<StringLiteral<'a>>,
    pub wrapper: Ident<'a>,
    pub options: Vec<GenericOption<'a>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateUserMapping<'a> {
    pub if_not_exists: bool,
    pub user: Ident<'a>,
    pub server: Ident<'a>,
    pub options: Vec<GenericOption<'a>>,
}

/// One action of `ALTER DEFAULT PRIVILEGES`.
#[derive(Debug, Clone, PartialEq)]
pub enum DefaultPrivilegesAction<'a> {
    Grant {
        privileges: Privileges<'a>,
        object: Vec<Cow<'a, str>>,
        grantees: Vec<Grantee<'a>>,
        with_grant_option: bool,
    },
    Revoke {
        grant_option: bool,
        privileges: Privileges<'a>,
        object: Vec<Cow<'a, str>>,
        grantees: Vec<Grantee<'a>>,
        behavior: Option<DropBehavior>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct AlterDefaultPrivileges<'a> {
    /// `FOR ROLE names` and `IN SCHEMA names`, in the order written.
    pub clauses: Vec<DefaultPrivilegesClause<'a>>,
    pub action: DefaultPrivilegesAction<'a>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum DefaultPrivilegesClause<'a> {
    Roles {
        keyword: Cow<'a, str>,
        names: Vec<Ident<'a>>,
    },
    Schemas(Vec<Ident<'a>>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum TriggerEvent<'a> {
    Insert,
    Delete,
    Truncate,
    Update(Vec<Ident<'a>>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateTrigger<'a> {
    pub or_replace: bool,
    pub constraint: bool,
    pub name: Ident<'a>,
    /// `BEFORE`, `AFTER` or `INSTEAD OF`.
    pub timing: Cow<'a, str>,
    pub events: Vec<TriggerEvent<'a>>,
    pub table: ObjectName<'a>,
    pub from: Option<ObjectName<'a>>,
    pub attributes: Vec<ConstraintAttribute>,
    /// `{OLD | NEW} [TABLE] [AS] name`, in the order written.
    pub referencing: Vec<TriggerReferencing<'a>>,
    /// `Some((each, row))`, for `FOR [EACH] {ROW | STATEMENT}`.
    pub for_each: Option<(bool, bool)>,
    pub when: Option<Exp<'a>>,
    /// True if written with `PROCEDURE` rather than `FUNCTION`.
    pub procedure: bool,
    pub function: ObjectName<'a>,
    pub args: Option<Vec<Literal<'a>>>,
}

/// One `REFERENCING` item.
#[derive(Debug, Clone, PartialEq)]
pub struct TriggerReferencing<'a> {
    pub new: bool,
    pub table: bool,
    pub as_keyword: bool,
    pub name: Ident<'a>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateRule<'a> {
    pub or_replace: bool,
    pub name: Ident<'a>,
    pub event: Cow<'a, str>,
    pub table: ObjectName<'a>,
    pub condition: Option<Exp<'a>>,
    /// `Some(true)` for `INSTEAD`, `Some(false)` for `ALSO`.
    pub instead: Option<bool>,
    pub action: RuleAction<'a>,
}

/// What a rule does.
#[derive(Debug, Clone, PartialEq)]
pub enum RuleAction<'a> {
    Nothing,
    Single(Box<Statement<'a, Builtin>>),
    Multiple(Vec<Statement<'a, Builtin>>),
}

pub fn statement(statement: PostgresStatement<'_>) -> Statement<'_, Builtin> {
    StatementKind::Ext(crate::dialect::builtin::BuiltinStatement::Postgres(
        statement,
    ))
    .into()
}

// Parsing

fn is_name_word(kind: TokenKind) -> bool {
    matches!(kind, TokenKind::Word | TokenKind::QuotedIdent)
}

/// True at `[schema.]...operator`, where `n` tokens of the schema have been
/// checked to be a dotted prefix.
fn at_qualified_operator<D: Dialect>(p: &Parser<'_, D>) -> bool {
    let mut n = 0;
    loop {
        let token = p.peek_nth(n);
        if is_name_word(token.kind) && p.peek_nth(n + 1).is_op(".") {
            n += 2;
        } else {
            return n > 0 && token.kind == TokenKind::Op && token.text != "(";
        }
    }
}

/// Parses `[schema.]operator`.
pub fn parse_operator_name<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<OperatorName<'a>> {
    let mut schema = Vec::new();
    while is_name_word(p.peek().kind) && p.peek_nth(1).is_op(".") {
        schema.push(p.parse_name_part()?);
        p.advance();
    }
    let token = p.peek();
    if token.kind != TokenKind::Op || token.text == "(" || token.text == ")" {
        return Err(p.expected("an operator"));
    }
    p.advance();
    Ok(OperatorName {
        schema,
        operator: token.text.into(),
    })
}

fn parse_operator_call<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<OperatorName<'a>> {
    p.expect_keyword("OPERATOR")?;
    p.expect_op("(")?;
    let operator = parse_operator_name(p)?;
    p.expect_op(")")?;
    Ok(operator)
}

/// Parses `(name [= value], ...)`.
pub fn parse_definition<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Vec<DefElem<'a>>> {
    p.expect_op("(")?;
    let elements = p.parse_comma_separated(parse_def_elem)?;
    p.expect_op(")")?;
    Ok(elements)
}

fn parse_def_elem<'a, D: Dialect<Ext = Builtin>>(p: &mut Parser<'a, D>) -> PResult<DefElem<'a>> {
    let name = p.parse_name_part()?;
    let value = if p.eat_op("=") {
        Some(parse_def_arg(p)?)
    } else {
        None
    };
    Ok(DefElem { name, value })
}

fn parse_def_arg<'a, D: Dialect<Ext = Builtin>>(p: &mut Parser<'a, D>) -> PResult<DefArg<'a>> {
    let token = p.peek();
    match token.kind {
        TokenKind::String => {
            p.advance();
            return Ok(DefArg::String(D::string_literal(token)));
        }
        TokenKind::Number => {
            p.advance();
            return Ok(DefArg::Number(token.text.into()));
        }
        TokenKind::Op
            if (token.text == "-" || token.text == "+")
                && p.peek_nth(1).kind == TokenKind::Number =>
        {
            let start = token.span.start;
            p.advance();
            let number = p.advance();
            let span = crate::ast::Span {
                start,
                end: number.span.end,
            };
            return Ok(DefArg::Number(p.text(span).into()));
        }
        TokenKind::Op => return Ok(DefArg::Operator(parse_operator_name(p)?)),
        TokenKind::Word if token.is_word("OPERATOR") && p.peek_nth(1).is_op("(") => {
            return Ok(DefArg::OperatorCall(parse_operator_call(p)?));
        }
        _ => {}
    }
    if at_qualified_operator(p) {
        return Ok(DefArg::Operator(parse_operator_name(p)?));
    }
    if token.kind == TokenKind::Word && !p.at_ident() {
        p.advance();
        return Ok(DefArg::Word(token.text.into()));
    }
    Ok(DefArg::Type(D::parse_data_type(p)?))
}

/// Parses `RENAME TO` or `OWNER TO`, for `ALTER SCHEMA`.
pub fn parse_rename_or_owner<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<ObjectAction<'a>> {
    expect_object_action(p)
}

/// Parses `RENAME TO`, `OWNER TO` or `SET SCHEMA`.
fn parse_object_action<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Option<ObjectAction<'a>>> {
    let next = p.peek_nth(1);
    if p.at_keyword("RENAME") && next.is_word("TO") {
        p.advance();
        p.advance();
        return p.parse_ident().map(ObjectAction::Rename).map(Some);
    }
    if p.at_keyword("OWNER") && next.is_word("TO") {
        p.advance();
        p.advance();
        return p.parse_name_part().map(ObjectAction::Owner).map(Some);
    }
    if p.at_keyword("SET") && next.is_word("SCHEMA") {
        p.advance();
        p.advance();
        return p.parse_ident().map(ObjectAction::SetSchema).map(Some);
    }
    Ok(None)
}

fn expect_object_action<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<ObjectAction<'a>> {
    parse_object_action(p)?.ok_or_else(|| p.expected("RENAME TO, OWNER TO or SET SCHEMA"))
}

/// Parses `(type, type)`, for an operator's arguments.
fn parse_operator_types<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<(DataType<'a>, DataType<'a>)> {
    p.expect_op("(")?;
    let left = D::parse_data_type(p)?;
    p.expect_op(",")?;
    let right = D::parse_data_type(p)?;
    p.expect_op(")")?;
    Ok((left, right))
}

fn parse_family_item<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
    dropping: bool,
) -> PResult<OperatorClassItem<'a>> {
    if p.eat_keyword("OPERATOR") {
        let strategy = parse_integer_text(p)?;
        if dropping {
            let types = parse_operator_types(p)?;
            return Ok(OperatorClassItem::Operator {
                strategy: strategy.into(),
                operator: OperatorName {
                    schema: Vec::new(),
                    operator: "".into(),
                },
                types: Some(Box::new(types)),
                purpose: None,
            });
        }
        let operator = parse_operator_name(p)?;
        let types = if p.at_op("(") {
            Some(Box::new(parse_operator_types(p)?))
        } else {
            None
        };
        let purpose = if p.eat_keyword("FOR") {
            if p.eat_keyword("SEARCH") {
                Some(OperatorPurpose::Search)
            } else {
                p.expect_keyword("ORDER")?;
                p.expect_keyword("BY")?;
                Some(OperatorPurpose::OrderBy(grammar::parse_object_name(p)?))
            }
        } else {
            None
        };
        return Ok(OperatorClassItem::Operator {
            strategy: strategy.into(),
            operator,
            types,
            purpose,
        });
    }
    if p.eat_keyword("FUNCTION") {
        let support = parse_integer_text(p)?;
        let op_types = if p.at_op("(") {
            let types = parse_paren_types(p)?;
            if types.is_empty() {
                return Err(p.expected("a data type"));
            }
            types
        } else {
            Vec::new()
        };
        let (name, args) = if dropping {
            (None, None)
        } else {
            let name = grammar::parse_object_name(p)?;
            let args = if p.at_op("(") {
                Some(parse_paren_types(p)?)
            } else {
                None
            };
            // `name()` means the same as `name`.
            (Some(name), args.filter(|args| !args.is_empty()))
        };
        return Ok(OperatorClassItem::Function {
            support: support.into(),
            op_types,
            name,
            args,
        });
    }
    p.expect_keyword("STORAGE")?;
    Ok(OperatorClassItem::Storage(D::parse_data_type(p)?))
}

/// Parses a strategy or support number, which must be a small integer.
fn parse_integer_text<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<&'a str> {
    let token = p.peek();
    let text = grammar::parse_number_text(p)?;
    if text.parse::<u64>().is_err() {
        return Err(crate::ParseError {
            message: format!("expected an integer, found {text}"),
            span: token.span,
        });
    }
    Ok(text)
}

fn parse_paren_types<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Vec<DataType<'a>>> {
    p.expect_op("(")?;
    if p.eat_op(")") {
        return Ok(Vec::new());
    }
    let types = p.parse_comma_separated(D::parse_data_type)?;
    p.expect_op(")")?;
    Ok(types)
}

/// Parses what follows `CREATE OPERATOR`.
fn parse_create_operator<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<PostgresStatement<'a>> {
    let family = p.at_keyword("FAMILY");
    if family || p.at_keyword("CLASS") {
        p.advance();
        let name = grammar::parse_object_name(p)?;
        if family {
            p.expect_keyword("USING")?;
            let using = p.parse_ident()?;
            return Ok(PostgresStatement::CreateOperatorFamily { name, using });
        }
        let default = p.eat_keyword("DEFAULT");
        p.expect_keyword("FOR")?;
        p.expect_keyword("TYPE")?;
        let for_type = D::parse_data_type(p)?;
        p.expect_keyword("USING")?;
        let using = p.parse_ident()?;
        let family = if p.eat_keyword("FAMILY") {
            Some(grammar::parse_object_name(p)?)
        } else {
            None
        };
        p.expect_keyword("AS")?;
        let items = p.parse_comma_separated(|p| parse_family_item(p, false))?;
        return Ok(PostgresStatement::CreateOperatorClass(Box::new(
            CreateOperatorClass {
                name,
                default,
                for_type,
                using,
                family,
                items,
            },
        )));
    }
    let mut name = parse_operator_name(p)?;
    if name.operator == "!=" {
        name.operator = "<>".into();
    }
    let definition: Vec<DefElem<'a>> = parse_definition(p)?
        .into_iter()
        .map(|element| DefElem {
            name: element.name,
            value: element.value.map(|value| match value {
                DefArg::OperatorCall(operator) => DefArg::Operator(operator),
                other => other,
            }),
        })
        .collect();
    const KEYS: [&str; 10] = [
        "FUNCTION",
        "PROCEDURE",
        "LEFTARG",
        "RIGHTARG",
        "COMMUTATOR",
        "NEGATOR",
        "RESTRICT",
        "JOIN",
        "HASHES",
        "MERGES",
    ];
    check_definition(p, &definition, &KEYS, true)?;
    let has = |key: &str| {
        definition
            .iter()
            .any(|element| element.name.text.eq_ignore_ascii_case(key))
    };
    if has("FUNCTION") == has("PROCEDURE") {
        return Err(p.expected("exactly one of FUNCTION or PROCEDURE"));
    }
    Ok(PostgresStatement::CreateOperator { name, definition })
}

/// Checks that a definition sets only `keys` and, if `unique`, each of them
/// at most once.
fn check_definition<'a, D: Dialect>(
    p: &Parser<'a, D>,
    definition: &[DefElem<'a>],
    keys: &[&str],
    unique: bool,
) -> PResult<()> {
    let mut seen: Vec<&str> = Vec::new();
    for element in definition {
        let name = &element.name.text;
        let key = keys.iter().find(|key| key.eq_ignore_ascii_case(name));
        let Some(key) = key.filter(|_| element.name.quote.is_none()) else {
            return Err(p.expected(&format!("a definition option, not {name}")));
        };
        if unique && seen.contains(key) {
            return Err(p.expected(&format!("{key} at most once")));
        }
        seen.push(key);
    }
    Ok(())
}

/// Parses `ALTER OPERATOR ...`, with the cursor on `OPERATOR`.
fn parse_alter_operator<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<PostgresStatement<'a>> {
    p.advance();
    let family = p.at_keyword("FAMILY");
    if family || p.at_keyword("CLASS") {
        p.advance();
        let name = grammar::parse_object_name(p)?;
        p.expect_keyword("USING")?;
        let using = p.parse_ident()?;
        if !family {
            let action = expect_object_action(p)?;
            return Ok(PostgresStatement::AlterOperatorClass {
                family: false,
                name,
                using,
                op: AlterFamilyOp::Action(action),
            });
        }
        let op = if p.eat_keyword("ADD") {
            AlterFamilyOp::Add(p.parse_comma_separated(|p| parse_family_item(p, false))?)
        } else if p.eat_keyword("DROP") {
            AlterFamilyOp::Drop(p.parse_comma_separated(|p| parse_family_item(p, true))?)
        } else {
            AlterFamilyOp::Action(expect_object_action(p)?)
        };
        return Ok(PostgresStatement::AlterOperatorClass {
            family: true,
            name,
            using,
            op,
        });
    }
    let name = parse_operator_name(p)?;
    let (left, right) = parse_operator_types(p)?;
    let op = if p.at_keyword("SET") && p.peek_nth(1).is_op("(") {
        p.advance();
        AlterOperatorOp::Set(parse_definition(p)?)
    } else {
        AlterOperatorOp::Action(expect_object_action(p)?)
    };
    Ok(PostgresStatement::AlterOperator {
        name,
        types: Box::new((left, right)),
        op,
    })
}

/// What `ALTER OPERATOR` does.
#[derive(Debug, Clone, PartialEq)]
pub enum AlterOperatorOp<'a> {
    Action(ObjectAction<'a>),
    Set(Vec<DefElem<'a>>),
}

/// Parses `DROP OPERATOR {CLASS|FAMILY}`, with the cursor on `CLASS`.
pub fn parse_drop_operator_group<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<PostgresStatement<'a>> {
    let family = p.at_keyword("FAMILY");
    p.advance();
    let if_exists = p.at_keyword("IF");
    if if_exists {
        p.advance();
        p.expect_keyword("EXISTS")?;
    }
    let names = p.parse_comma_separated(grammar::parse_object_name)?;
    p.expect_keyword("USING")?;
    let using = p.parse_ident()?;
    let behavior = if p.eat_keyword("CASCADE") {
        Some(DropBehavior::Cascade)
    } else if p.eat_keyword("RESTRICT") {
        Some(DropBehavior::Restrict)
    } else {
        None
    };
    Ok(PostgresStatement::DropOperatorGroup {
        family,
        if_exists,
        names,
        using,
        behavior,
    })
}

/// Parses one function or aggregate argument. The written `DEFAULT` is
/// normalized to `=`.
fn parse_routine_param<'a, D: Dialect<Ext = Builtin>>(p: &mut Parser<'a, D>) -> PResult<Arg<'a>> {
    let mode = ["IN", "OUT", "INOUT", "VARIADIC"]
        .into_iter()
        .find(|mode| p.eat_keyword(mode));
    let next = p.peek_nth(1);
    let continues_type = ["PRECISION", "VARYING", "WITH", "WITHOUT"]
        .iter()
        .any(|word| next.is_word(word));
    let has_name = p.at_ident()
        && matches!(next.kind, TokenKind::Word | TokenKind::QuotedIdent)
        && !continues_type
        && !next.is_word("DEFAULT")
        && !next.is_word("ORDER");
    let name = if has_name {
        Some(p.parse_ident()?)
    } else {
        None
    };
    let data_type = D::parse_data_type(p)?;
    let default = if p.eat_op("=") || p.eat_keyword("DEFAULT") {
        Some((true, p.parse_expr()?))
    } else {
        None
    };
    Ok(RoutineArg {
        mode: mode.map(Cow::Borrowed),
        name,
        data_type,
        default,
    })
}

fn parse_aggregate_arg<'a, D: Dialect<Ext = Builtin>>(p: &mut Parser<'a, D>) -> PResult<Arg<'a>> {
    let token = p.peek();
    let arg = parse_routine_param(p)?;
    if matches!(arg.mode.as_deref(), Some("OUT" | "INOUT")) || arg.default.is_some() {
        return Err(crate::ParseError {
            message: "aggregate arguments cannot be output arguments or have defaults".into(),
            span: token.span,
        });
    }
    Ok(arg)
}

fn parse_aggregate_args<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<AggregateArgs<'a>> {
    p.expect_op("(")?;
    if p.eat_op("*") {
        p.expect_op(")")?;
        return Ok(AggregateArgs::Star);
    }
    let args = if p.at_keyword("ORDER") {
        Vec::new()
    } else {
        p.parse_comma_separated(parse_aggregate_arg)?
    };
    let order_by = if p.eat_keyword("ORDER") {
        p.expect_keyword("BY")?;
        p.parse_comma_separated(parse_aggregate_arg)?
    } else {
        Vec::new()
    };
    p.expect_op(")")?;
    Ok(AggregateArgs::List { args, order_by })
}

/// True if the list at the cursor is an aggregate's legacy definition, whose
/// first element is `name =`, or has no second list after it.
fn at_legacy_aggregate<D: Dialect>(p: &Parser<'_, D>) -> bool {
    p.peek_nth(2).is_op("=")
}

fn parse_create_aggregate<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
    or_replace: bool,
) -> PResult<PostgresStatement<'a>> {
    let name = grammar::parse_object_name(p)?;
    let args = if at_legacy_aggregate(p) {
        None
    } else {
        Some(parse_aggregate_args(p)?)
    };
    let definition = parse_definition(p)?;
    const KEYS: [&str; 23] = [
        "SFUNC",
        "STYPE",
        "SSPACE",
        "FINALFUNC",
        "FINALFUNC_EXTRA",
        "FINALFUNC_MODIFY",
        "COMBINEFUNC",
        "SERIALFUNC",
        "DESERIALFUNC",
        "INITCOND",
        "MSFUNC",
        "MINVFUNC",
        "MSTYPE",
        "MSSPACE",
        "MFINALFUNC",
        "MFINALFUNC_EXTRA",
        "MFINALFUNC_MODIFY",
        "MINITCOND",
        "SORTOP",
        "PARALLEL",
        "HYPOTHETICAL",
        "BASETYPE",
        "COMPUTEFUNC",
    ];
    check_definition(p, &definition, &KEYS, false)?;
    Ok(PostgresStatement::CreateAggregate(Box::new(
        CreateAggregate {
            or_replace,
            name,
            args,
            definition,
        },
    )))
}

fn parse_set_value<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
    name: ObjectName<'a>,
) -> PResult<ParameterSetting<'a>> {
    if p.at_keyword("FROM") && p.peek_nth(1).is_word("CURRENT") {
        p.advance();
        p.advance();
        return Ok(ParameterSetting {
            name,
            value: ParameterValue::FromCurrent,
            to: false,
        });
    }
    let to = if p.eat_op("=") {
        false
    } else {
        p.expect_keyword("TO")?;
        true
    };
    let value = if p.eat_keyword("DEFAULT") {
        ParameterValue::Default
    } else {
        ParameterValue::Values(p.parse_comma_separated(|p| p.parse_expr_bp(prec::COMPARE))?)
    };
    Ok(ParameterSetting { name, value, to })
}

fn parse_number_literal<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<&'a str> {
    grammar::parse_number_text(p)
}

/// Parses what follows `FUNCTION`, with `or_replace` already read.
fn parse_create_function<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
    or_replace: bool,
) -> PResult<PostgresStatement<'a>> {
    let name = grammar::parse_object_name(p)?;
    p.expect_op("(")?;
    let args = if p.at_op(")") {
        Vec::new()
    } else {
        p.parse_comma_separated(parse_routine_param)?
    };
    p.expect_op(")")?;
    let returns = if p.at_keyword("RETURNS") && !p.peek_nth(1).is_word("NULL") {
        p.advance();
        Some(parse_function_return(p)?)
    } else {
        None
    };

    let mut function = CreateFunction {
        or_replace,
        name,
        args,
        returns,
        language: None,
        window: false,
        volatility: None,
        null_input: None,
        parallel: None,
        security: None,
        leakproof: None,
        cost: None,
        rows: None,
        support: None,
        set: Vec::new(),
        body: None,
    };
    loop {
        if p.eat_keyword("LANGUAGE") {
            let language = p.parse_ident();
            set_once(p, &mut function.language, language, "LANGUAGE")?;
        } else if p.eat_keyword("WINDOW") {
            if function.window {
                return Err(p.expected("WINDOW at most once"));
            }
            function.window = true;
        } else if let Some(volatility) = parse_volatility(p) {
            set_once(
                p,
                &mut function.volatility,
                Ok(volatility.into()),
                "IMMUTABLE, STABLE or VOLATILE",
            )?;
        } else if let Some(null_input) = parse_null_input(p) {
            set_once(
                p,
                &mut function.null_input,
                Ok(null_input.into()),
                "null input",
            )?;
        } else if p.eat_keyword("PARALLEL") {
            let level = parse_parallel(p)?;
            set_once(p, &mut function.parallel, Ok(level.into()), "PARALLEL")?;
        } else if p.at_keyword("SECURITY") {
            p.advance();
            let kind = parse_security_kind(p)?;
            set_once(p, &mut function.security, Ok(kind.into()), "SECURITY")?;
        } else if p.eat_keyword("LEAKPROOF") {
            set_once(p, &mut function.leakproof, Ok(true), "LEAKPROOF")?;
        } else if p.at_keyword("NOT") && p.peek_nth(1).is_word("LEAKPROOF") {
            p.advance();
            p.advance();
            set_once(p, &mut function.leakproof, Ok(false), "LEAKPROOF")?;
        } else if p.eat_keyword("COST") {
            let cost = parse_number_literal(p).map(Cow::from);
            set_once(p, &mut function.cost, cost, "COST")?;
        } else if p.eat_keyword("ROWS") {
            let rows = parse_number_literal(p).map(Cow::from);
            set_once(p, &mut function.rows, rows, "ROWS")?;
        } else if p.eat_keyword("SUPPORT") {
            let name = grammar::parse_object_name(p);
            set_once(p, &mut function.support, name, "SUPPORT")?;
        } else if p.eat_keyword("SET") {
            let name = grammar::parse_object_name(p)?;
            let setting = parse_set_value(p, name)?;
            function.set.push(ParameterSetting {
                to: false,
                ..setting
            });
        } else if p.eat_keyword("AS") {
            let body = parse_body_literal(p)?;
            let link_symbol = if p.eat_op(",") {
                Some(parse_body_literal(p)?)
            } else {
                None
            };
            set_once(
                p,
                &mut function.body,
                Ok(FunctionBody::As { body, link_symbol }),
                "AS",
            )?;
        } else if p.eat_keyword("RETURN") {
            let value = p.parse_expr();
            set_once(
                p,
                &mut function.body,
                value.map(FunctionBody::Return),
                "RETURN",
            )?;
        } else if p.at_keyword("BEGIN") && p.peek_nth(1).is_word("ATOMIC") {
            p.advance();
            p.advance();
            let statements = parse_begin_atomic(p)?;
            set_once(
                p,
                &mut function.body,
                Ok(FunctionBody::BeginAtomic(statements)),
                "BEGIN ATOMIC",
            )?;
        } else {
            break;
        }
    }
    Ok(PostgresStatement::CreateFunction(Box::new(function)))
}

fn set_once<'a, D: Dialect, T>(
    p: &Parser<'a, D>,
    slot: &mut Option<T>,
    value: PResult<T>,
    what: &str,
) -> PResult<()> {
    let value = value?;
    if slot.is_some() {
        return Err(p.expected(&format!("{what} at most once")));
    }
    *slot = Some(value);
    Ok(())
}

fn parse_body_literal<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Literal<'a>> {
    let token = p.peek();
    match token.kind {
        TokenKind::String => {
            p.advance();
            Ok(Literal::String(D::string_literal(token)))
        }
        TokenKind::DollarString => match p.parse_expr()?.kind {
            ExprKind::Literal(literal) => Ok(literal),
            _ => Err(p.expected("a dollar-quoted string")),
        },
        _ => Err(p.expected("a string")),
    }
}

fn parse_function_return<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<FunctionReturn<'a>> {
    if p.eat_keyword("SETOF") {
        return D::parse_data_type(p).map(FunctionReturn::SetOf);
    }
    if p.at_keyword("TABLE") && p.peek_nth(1).is_op("(") {
        p.advance();
        p.advance();
        let columns = p.parse_comma_separated(grammar::parse_column_def)?;
        p.expect_op(")")?;
        return Ok(FunctionReturn::Table(columns));
    }
    D::parse_data_type(p).map(FunctionReturn::Type)
}

fn parse_volatility<D: Dialect>(p: &mut Parser<'_, D>) -> Option<&'static str> {
    ["IMMUTABLE", "STABLE", "VOLATILE"]
        .into_iter()
        .find(|word| p.eat_keyword(word))
}

fn parse_null_input<D: Dialect>(p: &mut Parser<'_, D>) -> Option<&'static str> {
    let next = p.peek_nth(1);
    if p.at_keyword("CALLED") && next.is_word("ON") {
        for _ in 0..4 {
            p.advance();
        }
        return Some("CALLED ON NULL INPUT");
    }
    if p.at_keyword("RETURNS") && next.is_word("NULL") {
        for _ in 0..5 {
            p.advance();
        }
        return Some("RETURNS NULL ON NULL INPUT");
    }
    if p.eat_keyword("STRICT") {
        return Some("STRICT");
    }
    None
}

fn parse_parallel<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<&'static str> {
    ["SAFE", "UNSAFE", "RESTRICTED"]
        .into_iter()
        .find(|word| p.eat_keyword(word))
        .ok_or_else(|| p.expected("SAFE, UNSAFE or RESTRICTED"))
}

fn parse_security_kind<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<&'static str> {
    ["DEFINER", "INVOKER"]
        .into_iter()
        .find(|word| p.eat_keyword(word))
        .ok_or_else(|| p.expected("DEFINER or INVOKER"))
}

/// Parses `statement; ... END`.
fn parse_begin_atomic<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Vec<Statement<'a, Builtin>>> {
    let mut statements = Vec::new();
    while !p.eat_keyword("END") {
        let parsed = if p.eat_keyword("RETURN") {
            statement(PostgresStatement::Return(Box::new(p.parse_expr()?)))
        } else {
            p.nested(Parser::parse_statement)?
        };
        statements.push(parsed);
        p.expect_op(";")?;
    }
    Ok(statements)
}

/// Parses what follows `ALTER {FUNCTION | PROCEDURE | ROUTINE | AGGREGATE}`.
fn parse_alter_function<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
    aggregate: bool,
    keyword: &'static str,
) -> PResult<PostgresStatement<'a>> {
    if aggregate {
        let name = grammar::parse_object_name(p)?;
        let args = parse_aggregate_args(p)?;
        let action = expect_object_action(p)?;
        return Ok(PostgresStatement::AlterAggregate(Box::new(
            AlterAggregate { name, args, action },
        )));
    }
    let name = grammar::parse_object_name(p)?;
    let args = if p.eat_op("(") {
        let args = if p.at_op(")") {
            Vec::new()
        } else {
            p.parse_comma_separated(parse_routine_param)?
        };
        p.expect_op(")")?;
        Some(args)
    } else {
        None
    };
    let signature = RoutineSignature { name, args };
    let no_depends = p.at_keyword("NO") && p.peek_nth(1).is_word("DEPENDS");
    let op = if p.at_keyword("DEPENDS") || no_depends {
        if no_depends {
            p.advance();
        }
        p.advance();
        p.expect_keyword("ON")?;
        p.expect_keyword("EXTENSION")?;
        AlterFunctionOp::DependsOn {
            no: no_depends,
            extension: p.parse_ident()?,
        }
    } else if let Some(action) = parse_object_action(p)? {
        AlterFunctionOp::Action(action)
    } else {
        let mut attrs = Vec::new();
        while let Some(attr) = parse_function_attr(p)? {
            attrs.push(attr);
        }
        if attrs.is_empty() {
            return Err(p.expected("an ALTER FUNCTION action"));
        }
        AlterFunctionOp::Attributes(attrs)
    };
    Ok(PostgresStatement::AlterFunction(
        keyword.into(),
        Box::new(AlterFunction { signature, op }),
    ))
}

fn parse_function_attr<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Option<FunctionAttr<'a>>> {
    if let Some(volatility) = parse_volatility(p) {
        return Ok(Some(FunctionAttr::Volatility(volatility.into())));
    }
    if let Some(null_input) = parse_null_input(p) {
        return Ok(Some(FunctionAttr::NullInput(null_input.into())));
    }
    if p.eat_keyword("PARALLEL") {
        return parse_parallel(p)
            .map(|v| FunctionAttr::Parallel(v.into()))
            .map(Some);
    }
    if p.at_keyword("SECURITY") || (p.at_keyword("EXTERNAL") && p.peek_nth(1).is_word("SECURITY")) {
        let external = p.eat_keyword("EXTERNAL");
        p.advance();
        return Ok(Some(FunctionAttr::Security {
            external,
            kind: parse_security_kind(p)?.into(),
        }));
    }
    if p.eat_keyword("LEAKPROOF") {
        return Ok(Some(FunctionAttr::Leakproof(true)));
    }
    if p.at_keyword("NOT") && p.peek_nth(1).is_word("LEAKPROOF") {
        p.advance();
        p.advance();
        return Ok(Some(FunctionAttr::Leakproof(false)));
    }
    if p.eat_keyword("COST") {
        return parse_number_literal(p)
            .map(|v| FunctionAttr::Cost(v.into()))
            .map(Some);
    }
    if p.eat_keyword("ROWS") {
        return parse_number_literal(p)
            .map(|v| FunctionAttr::Rows(v.into()))
            .map(Some);
    }
    if p.eat_keyword("SUPPORT") {
        return grammar::parse_object_name(p)
            .map(FunctionAttr::Support)
            .map(Some);
    }
    if p.eat_keyword("RESTRICT") {
        return Ok(Some(FunctionAttr::Restrict));
    }
    if p.at_keyword("SET") {
        p.advance();
        let name = grammar::parse_object_name(p)?;
        let setting = parse_set_value(p, name)?;
        return Ok(Some(FunctionAttr::Set(ParameterSetting {
            to: false,
            ..setting
        })));
    }
    if p.eat_keyword("RESET") {
        return Ok(Some(FunctionAttr::Reset(if p.eat_keyword("ALL") {
            None
        } else {
            Some(grammar::parse_object_name(p)?)
        })));
    }
    Ok(None)
}

const ROLE_FLAGS: [&str; 14] = [
    "SUPERUSER",
    "NOSUPERUSER",
    "CREATEDB",
    "NOCREATEDB",
    "CREATEROLE",
    "NOCREATEROLE",
    "INHERIT",
    "NOINHERIT",
    "LOGIN",
    "NOLOGIN",
    "REPLICATION",
    "NOREPLICATION",
    "BYPASSRLS",
    "NOBYPASSRLS",
];

fn parse_role_option<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Option<RoleOption<'a>>> {
    let token = p.peek();
    if token.kind == TokenKind::Word && ROLE_FLAGS.iter().any(|word| token.is_word(word)) {
        p.advance();
        return Ok(Some(RoleOption::Flag(token.text.into())));
    }
    if p.eat_keyword("CONNECTION") {
        p.expect_keyword("LIMIT")?;
        let start = p.peek().span.start;
        if !p.eat_op("-") {
            p.eat_op("+");
        }
        let number = grammar::parse_number_text(p)?;
        let end = number.as_ptr() as usize + number.len() - p.source().as_ptr() as usize;
        return Ok(Some(RoleOption::ConnectionLimit(Cow::Borrowed(
            &p.source()[start as usize..end],
        ))));
    }
    if p.eat_keyword("PASSWORD") {
        let value = if p.eat_keyword("NULL") {
            None
        } else {
            Some(grammar::parse_string(p)?)
        };
        return Ok(Some(RoleOption::Password(value)));
    }
    if p.eat_keyword("VALID") {
        p.expect_keyword("UNTIL")?;
        return grammar::parse_string(p)
            .map(RoleOption::ValidUntil)
            .map(Some);
    }
    if p.eat_keyword("SYSID") {
        return parse_number_literal(p)
            .map(|v| RoleOption::Sysid(v.into()))
            .map(Some);
    }
    let keyword = if p.at_keyword("IN") && p.peek_nth(1).is_word("ROLE") {
        p.advance();
        p.advance();
        Some("IN ROLE")
    } else if p.at_keyword("IN") && p.peek_nth(1).is_word("GROUP") {
        p.advance();
        p.advance();
        Some("IN GROUP")
    } else if p.eat_keyword("ROLE") {
        Some("ROLE")
    } else if p.eat_keyword("ADMIN") {
        Some("ADMIN")
    } else if p.eat_keyword("USER") {
        Some("USER")
    } else {
        None
    };
    let Some(keyword) = keyword else {
        return Ok(None);
    };
    let names = p.parse_comma_separated(Parser::parse_ident)?;
    Ok(Some(RoleOption::Members {
        keyword: keyword.into(),
        names,
    }))
}

fn parse_role_options<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Vec<RoleOption<'a>>> {
    let mut options = Vec::new();
    let mut seen_flags: Vec<String> = Vec::new();
    let start = p.peek();
    while let Some(option) = parse_role_option(p)? {
        if let RoleOption::Flag(ref word) = option {
            let base = word
                .strip_prefix(['n', 'N'])
                .filter(|rest| {
                    rest.strip_prefix(['o', 'O'])
                        .is_some_and(|flag| ROLE_FLAGS.iter().any(|f| f.eq_ignore_ascii_case(flag)))
                })
                .map_or(word.as_ref(), |rest| &rest[1..]);
            if seen_flags.iter().any(|f| f.eq_ignore_ascii_case(base)) {
                return Err(p.expected(&format!("only one {base} option, near {}", start.text)));
            }
            seen_flags.push(base.to_owned());
        }
        options.push(option);
    }
    Ok(options)
}

/// The rank of a role option in the canonical order the options render in.
fn role_option_rank(option: &RoleOption<'_>) -> usize {
    match option {
        RoleOption::Flag(word) => {
            let word = word.to_ascii_uppercase();
            let word = word.strip_prefix("NO").unwrap_or(&word);
            ROLE_FLAGS.iter().position(|f| *f == word).unwrap_or(0) / 2
        }
        RoleOption::ConnectionLimit(_) => 100,
        RoleOption::Password(_) => 101,
        RoleOption::ValidUntil(_) => 102,
        RoleOption::Members { keyword, .. } => {
            ["IN ROLE", "IN GROUP", "ROLE", "ADMIN", "USER"]
                .iter()
                .position(|k| k == keyword)
                .unwrap_or(5)
                + 103
        }
        RoleOption::Sysid(_) => 110,
    }
}

fn write_role_options(f: &mut fmt::Formatter<'_>, options: &[RoleOption<'_>]) -> fmt::Result {
    let mut sorted: Vec<&RoleOption<'_>> = options.iter().collect();
    sorted.sort_by_key(|option| role_option_rank(option));
    for option in sorted {
        write!(f, " {option}")?;
    }
    Ok(())
}

/// Parses what follows `CREATE {ROLE | USER | GROUP}`.
fn parse_create_role<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
    keyword: &'static str,
) -> PResult<PostgresStatement<'a>> {
    let name = p.parse_ident()?;
    p.eat_keyword("WITH");
    let options = parse_role_options(p)?;
    Ok(PostgresStatement::CreateRole(Box::new(CreateRole {
        keyword: keyword.into(),
        name,
        options,
    })))
}

/// Parses what follows `ALTER {ROLE | USER | GROUP}`.
fn parse_alter_role<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
    keyword: &'static str,
) -> PResult<PostgresStatement<'a>> {
    let name = p.parse_ident()?;
    let in_database = if p.at_keyword("IN") && p.peek_nth(1).is_word("DATABASE") {
        p.advance();
        p.advance();
        Some(p.parse_ident()?)
    } else {
        None
    };
    let op = if p.at_keyword("RENAME") && p.peek_nth(1).is_word("TO") {
        p.advance();
        p.advance();
        AlterRoleOp::Rename(p.parse_ident()?)
    } else if p.eat_keyword("SET") {
        let parameter = grammar::parse_object_name(p)?;
        let setting = parse_set_value(p, parameter)?;
        AlterRoleOp::Set(ParameterSetting {
            to: true,
            ..setting
        })
    } else if p.eat_keyword("RESET") {
        AlterRoleOp::Reset(if p.eat_keyword("ALL") {
            None
        } else {
            Some(grammar::parse_object_name(p)?)
        })
    } else {
        p.eat_keyword("WITH");
        let options = parse_role_options(p)?;
        if options.is_empty() {
            return Err(p.expected("an ALTER ROLE action"));
        }
        AlterRoleOp::Options { options }
    };
    Ok(PostgresStatement::AlterRole(Box::new(AlterRole {
        keyword: keyword.into(),
        name,
        in_database,
        op,
    })))
}

fn parse_create_extension<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<PostgresStatement<'a>> {
    let if_not_exists = grammar::parse_if_not_exists(p)?;
    let name = p.parse_ident()?;
    let with = p.eat_keyword("WITH");
    let (mut schema, mut version, mut cascade) = (None, None, false);
    loop {
        if p.eat_keyword("SCHEMA") {
            let value = p.parse_ident();
            set_once(p, &mut schema, value, "SCHEMA")?;
        } else if p.eat_keyword("VERSION") {
            let token = p.peek();
            let value = if token.kind == TokenKind::String {
                p.advance();
                Ok(ExtensionVersion::String(D::string_literal(token)))
            } else {
                p.parse_ident().map(ExtensionVersion::Name)
            };
            set_once(p, &mut version, value, "VERSION")?;
        } else if p.eat_keyword("CASCADE") {
            if cascade {
                return Err(p.expected("CASCADE at most once"));
            }
            cascade = true;
        } else {
            break;
        }
    }
    Ok(PostgresStatement::CreateExtension(Box::new(
        CreateExtension {
            if_not_exists,
            name,
            with,
            schema,
            version,
            cascade,
        },
    )))
}

fn parse_create_domain<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<PostgresStatement<'a>> {
    let name = grammar::parse_object_name(p)?;
    let as_keyword = p.eat_keyword("AS");
    let data_type = D::parse_data_type(p)?;
    let mut options = Vec::new();
    while let Some(option) = D::parse_column_option(p)? {
        options.push(option);
    }
    Ok(PostgresStatement::CreateDomain(Box::new(CreateDomain {
        name,
        as_keyword,
        data_type,
        options,
    })))
}

pub fn parse_create_type<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<PostgresStatement<'a>> {
    let name = grammar::parse_object_name(p)?;
    if p.eat_keyword("AS") {
        if p.eat_keyword("ENUM") {
            p.expect_op("(")?;
            let labels = if p.at_op(")") {
                Vec::new()
            } else {
                p.parse_comma_separated(grammar::parse_string)?
            };
            p.expect_op(")")?;
            return Ok(PostgresStatement::CreateType(Box::new(CreateType::Enum {
                name,
                labels,
            })));
        }
        if p.eat_keyword("RANGE") {
            let definition = parse_definition(p)?;
            return Ok(PostgresStatement::CreateType(Box::new(CreateType::Range {
                name,
                definition,
            })));
        }
        p.expect_op("(")?;
        let columns = if p.at_op(")") {
            Vec::new()
        } else {
            p.parse_comma_separated(D::parse_column_def)?
        };
        p.expect_op(")")?;
        return Ok(PostgresStatement::CreateType(Box::new(
            CreateType::Composite { name, columns },
        )));
    }
    if p.at_op("(") {
        let definition = parse_definition(p)?;
        return Ok(PostgresStatement::CreateType(Box::new(CreateType::Base {
            name,
            definition,
        })));
    }
    Ok(PostgresStatement::CreateType(Box::new(CreateType::Shell {
        name,
    })))
}

fn parse_alter_type<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<PostgresStatement<'a>> {
    let name = grammar::parse_object_name(p)?;
    let op = if p.at_keyword("ADD") && p.peek_nth(1).is_word("VALUE") {
        p.advance();
        p.advance();
        let if_not_exists = grammar::parse_if_not_exists(p)?;
        let value = grammar::parse_string(p)?;
        let position = if p.eat_keyword("BEFORE") {
            Some((true, grammar::parse_string(p)?))
        } else if p.eat_keyword("AFTER") {
            Some((false, grammar::parse_string(p)?))
        } else {
            None
        };
        AlterTypeOp::AddValue {
            if_not_exists,
            value,
            position,
        }
    } else if p.at_keyword("RENAME") && p.peek_nth(1).is_word("VALUE") {
        p.advance();
        p.advance();
        let old = grammar::parse_string(p)?;
        p.expect_keyword("TO")?;
        AlterTypeOp::RenameValue {
            old,
            new: grammar::parse_string(p)?,
        }
    } else {
        AlterTypeOp::Action(expect_object_action(p)?)
    };
    Ok(PostgresStatement::AlterType(Box::new(AlterType {
        name,
        op,
    })))
}

fn parse_create_collation<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<PostgresStatement<'a>> {
    let if_not_exists = grammar::parse_if_not_exists(p)?;
    let name = grammar::parse_object_name(p)?;
    let source = if p.eat_keyword("FROM") {
        CollationSource::From(grammar::parse_object_name(p)?)
    } else {
        CollationSource::Definition(parse_definition(p)?)
    };
    Ok(PostgresStatement::CreateCollation(Box::new(
        CreateCollation {
            if_not_exists,
            name,
            source,
        },
    )))
}

fn parse_alter_collation<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<PostgresStatement<'a>> {
    let name = grammar::parse_object_name(p)?;
    let op = if p.at_keyword("REFRESH") && p.peek_nth(1).is_word("VERSION") {
        p.advance();
        p.advance();
        AlterCollationOp::RefreshVersion
    } else {
        AlterCollationOp::Action(expect_object_action(p)?)
    };
    Ok(PostgresStatement::AlterCollation(Box::new(
        AlterCollation { name, op },
    )))
}

fn parse_text_search_kind<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<TextSearchKind> {
    let token = p.peek();
    let kind = if token.is_word("CONFIGURATION") {
        TextSearchKind::Configuration
    } else if token.is_word("DICTIONARY") {
        TextSearchKind::Dictionary
    } else if token.is_word("PARSER") {
        TextSearchKind::Parser
    } else if token.is_word("TEMPLATE") {
        TextSearchKind::Template
    } else {
        return Err(p.expected("a text search object"));
    };
    p.advance();
    Ok(kind)
}

fn parse_create_text_search<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<PostgresStatement<'a>> {
    p.expect_keyword("SEARCH")?;
    let kind = parse_text_search_kind(p)?;
    let name = grammar::parse_object_name(p)?;
    let definition = parse_definition(p)?;
    Ok(PostgresStatement::CreateTextSearch {
        kind,
        name,
        definition,
    })
}

fn parse_alter_text_search<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<PostgresStatement<'a>> {
    p.expect_keyword("SEARCH")?;
    let kind = parse_text_search_kind(p)?;
    let name = grammar::parse_object_name(p)?;
    let op = if p.at_op("(") {
        AlterTextSearchOp::Options(parse_definition(p)?)
    } else {
        AlterTextSearchOp::Action(expect_object_action(p)?)
    };
    Ok(PostgresStatement::AlterTextSearch { kind, name, op })
}

/// Parses `OPTIONS (...)` if present.
pub fn parse_generic_options<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Vec<GenericOption<'a>>> {
    if !p.eat_keyword("OPTIONS") {
        return Ok(Vec::new());
    }
    p.expect_op("(")?;
    let options = p.parse_comma_separated(parse_generic_option)?;
    p.expect_op(")")?;
    Ok(options)
}

/// Parses `[ADD | SET | DROP] name ['value']`.
fn parse_generic_option<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<GenericOption<'a>> {
    let next = p.peek_nth(1);
    let keyword = if next.kind == TokenKind::String || next.is_op(",") || next.is_op(")") {
        None
    } else {
        ["ADD", "SET", "DROP"]
            .into_iter()
            .find(|word| p.at_keyword(word))
    };
    if keyword.is_some() {
        p.advance();
    }
    let name = p.parse_name_part()?;
    let value = if keyword == Some("DROP") {
        None
    } else {
        Some(grammar::parse_string(p)?)
    };
    Ok(GenericOption {
        action: keyword.map(Cow::Borrowed),
        name,
        value,
    })
}

fn parse_create_fdw<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<PostgresStatement<'a>> {
    p.expect_keyword("DATA")?;
    p.expect_keyword("WRAPPER")?;
    let name = p.parse_ident()?;
    let mut clauses = Vec::new();
    loop {
        if p.at_keyword("NO") && p.peek_nth(1).is_word("HANDLER") {
            p.advance();
            p.advance();
            clauses.push(FdwClause::Handler(None));
        } else if p.at_keyword("NO") && p.peek_nth(1).is_word("VALIDATOR") {
            p.advance();
            p.advance();
            clauses.push(FdwClause::Validator(None));
        } else if p.eat_keyword("HANDLER") {
            clauses.push(FdwClause::Handler(Some(p.parse_ident()?)));
        } else if p.eat_keyword("VALIDATOR") {
            clauses.push(FdwClause::Validator(Some(p.parse_ident()?)));
        } else if p.at_keyword("OPTIONS") {
            clauses.push(FdwClause::Options(parse_generic_options(p)?));
        } else {
            break;
        }
    }
    Ok(PostgresStatement::CreateForeignDataWrapper { name, clauses })
}

fn parse_create_server<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<PostgresStatement<'a>> {
    let if_not_exists = grammar::parse_if_not_exists(p)?;
    let name = p.parse_ident()?;
    let server_type = if p.eat_keyword("TYPE") {
        Some(grammar::parse_string(p)?)
    } else {
        None
    };
    let version = if p.eat_keyword("VERSION") {
        Some(grammar::parse_string(p)?)
    } else {
        None
    };
    p.expect_keyword("FOREIGN")?;
    p.expect_keyword("DATA")?;
    p.expect_keyword("WRAPPER")?;
    let wrapper = p.parse_ident()?;
    let options = parse_generic_options(p)?;
    Ok(PostgresStatement::CreateServer(Box::new(CreateServer {
        if_not_exists,
        name,
        server_type,
        version,
        wrapper,
        options,
    })))
}

/// Parses what follows `CREATE USER`, when `MAPPING` follows it.
fn parse_create_user_mapping<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<PostgresStatement<'a>> {
    let if_not_exists = grammar::parse_if_not_exists(p)?;
    p.expect_keyword("FOR")?;
    let user = p.parse_name_part()?;
    p.expect_keyword("SERVER")?;
    let server = p.parse_ident()?;
    let options = parse_generic_options(p)?;
    Ok(PostgresStatement::CreateUserMapping(Box::new(
        CreateUserMapping {
            if_not_exists,
            user,
            server,
            options,
        },
    )))
}

fn parse_default_privileges<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<PostgresStatement<'a>> {
    p.expect_keyword("PRIVILEGES")?;
    let mut clauses = Vec::new();
    loop {
        if p.at_keyword("FOR") && (p.peek_nth(1).is_word("ROLE") || p.peek_nth(1).is_word("USER")) {
            p.advance();
            let keyword = if p.eat_keyword("ROLE") {
                "ROLE"
            } else {
                p.advance();
                "USER"
            };
            clauses.push(DefaultPrivilegesClause::Roles {
                keyword: keyword.into(),
                names: p.parse_comma_separated(Parser::parse_ident)?,
            });
        } else if p.at_keyword("IN") && p.peek_nth(1).is_word("SCHEMA") {
            p.advance();
            p.advance();
            clauses.push(DefaultPrivilegesClause::Schemas(
                p.parse_comma_separated(Parser::parse_ident)?,
            ));
        } else {
            break;
        }
    }
    let action = if p.eat_keyword("GRANT") {
        let privileges = grammar::parse_privileges(p)?;
        let object = parse_default_object(p)?;
        p.expect_keyword("TO")?;
        let grantees = D::parse_grantees(p)?;
        let with_grant_option = p.at_keyword("WITH");
        if with_grant_option {
            p.advance();
            p.expect_keyword("GRANT")?;
            p.expect_keyword("OPTION")?;
        }
        DefaultPrivilegesAction::Grant {
            privileges,
            object: object.into_iter().map(Cow::Borrowed).collect(),
            grantees,
            with_grant_option,
        }
    } else {
        p.expect_keyword("REVOKE")?;
        let grant_option = p.at_keyword("GRANT");
        if grant_option {
            p.advance();
            p.expect_keyword("OPTION")?;
            p.expect_keyword("FOR")?;
        }
        let privileges = grammar::parse_privileges(p)?;
        let object = parse_default_object(p)?;
        p.expect_keyword("FROM")?;
        let grantees = D::parse_grantees(p)?;
        let behavior = if p.eat_keyword("CASCADE") {
            Some(DropBehavior::Cascade)
        } else if p.eat_keyword("RESTRICT") {
            Some(DropBehavior::Restrict)
        } else {
            None
        };
        DefaultPrivilegesAction::Revoke {
            grant_option,
            privileges,
            object: object.into_iter().map(Cow::Borrowed).collect(),
            grantees,
            behavior,
        }
    };
    Ok(PostgresStatement::AlterDefaultPrivileges(Box::new(
        AlterDefaultPrivileges { clauses, action },
    )))
}

/// Parses `ON kind` of `ALTER DEFAULT PRIVILEGES`, as written.
fn parse_default_object<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Vec<&'a str>> {
    p.expect_keyword("ON")?;
    const KINDS: [&[&str]; 7] = [
        &["TABLES"],
        &["SEQUENCES"],
        &["FUNCTIONS"],
        &["ROUTINES"],
        &["TYPES"],
        &["SCHEMAS"],
        &["LARGE", "OBJECTS"],
    ];
    let Some(words) = KINDS.iter().find(|words| {
        words
            .iter()
            .enumerate()
            .all(|(i, word)| p.peek_nth(i).is_word(word))
    }) else {
        return Err(
            p.expected("TABLES, SEQUENCES, FUNCTIONS, ROUTINES, TYPES, SCHEMAS or LARGE OBJECTS")
        );
    };
    Ok(words.iter().map(|_| p.advance().text).collect())
}

fn parse_create_trigger<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
    or_replace: bool,
    constraint: bool,
) -> PResult<PostgresStatement<'a>> {
    let name = p.parse_ident()?;
    let timing = if p.eat_keyword("BEFORE") {
        "BEFORE"
    } else if p.eat_keyword("AFTER") {
        "AFTER"
    } else {
        p.expect_keyword("INSTEAD")?;
        p.expect_keyword("OF")?;
        "INSTEAD OF"
    };
    let mut events = vec![parse_trigger_event(p)?];
    while p.eat_keyword("OR") {
        events.push(parse_trigger_event(p)?);
    }
    p.expect_keyword("ON")?;
    let table = grammar::parse_object_name(p)?;
    let from = if p.eat_keyword("FROM") {
        Some(grammar::parse_object_name(p)?)
    } else {
        None
    };
    let mut attributes = Vec::new();
    while let Some(attribute) = grammar::parse_constraint_attribute(p)? {
        attributes.push(attribute);
    }
    let mut referencing = Vec::new();
    if p.eat_keyword("REFERENCING") {
        while p.at_keyword("OLD") || p.at_keyword("NEW") {
            let new = p.at_keyword("NEW");
            p.advance();
            let table = p.eat_keyword("TABLE");
            let as_keyword = p.eat_keyword("AS");
            let name = p.parse_ident()?;
            referencing.push(TriggerReferencing {
                new,
                table,
                as_keyword,
                name,
            });
        }
        if referencing.is_empty() {
            return Err(p.expected("OLD or NEW"));
        }
    }
    let for_each = if p.eat_keyword("FOR") {
        let each = p.eat_keyword("EACH");
        let row = p.eat_keyword("ROW");
        if !row {
            p.expect_keyword("STATEMENT")?;
        }
        Some((each, row))
    } else {
        None
    };
    let when = if p.eat_keyword("WHEN") {
        p.expect_op("(")?;
        let condition = p.parse_expr()?;
        p.expect_op(")")?;
        Some(condition)
    } else {
        None
    };
    p.expect_keyword("EXECUTE")?;
    let procedure = if p.eat_keyword("PROCEDURE") {
        true
    } else {
        p.expect_keyword("FUNCTION")?;
        false
    };
    let function = grammar::parse_object_name(p)?;
    let args = if p.eat_op("(") {
        let args = if p.at_op(")") {
            Vec::new()
        } else {
            p.parse_comma_separated(parse_trigger_arg)?
        };
        p.expect_op(")")?;
        Some(args)
    } else {
        None
    };
    Ok(PostgresStatement::CreateTrigger(Box::new(CreateTrigger {
        or_replace,
        constraint,
        name,
        timing: timing.into(),
        events,
        table,
        from,
        attributes,
        referencing,
        for_each,
        when,
        procedure,
        function,
        args,
    })))
}

/// Trigger arguments are literals: numbers and strings, or bare words.
fn parse_trigger_arg<'a, D: Dialect<Ext = Builtin>>(p: &mut Parser<'a, D>) -> PResult<Literal<'a>> {
    let token = p.peek();
    match token.kind {
        TokenKind::Number => {
            p.advance();
            Ok(Literal::Number(token.text.into()))
        }
        TokenKind::String => {
            p.advance();
            Ok(Literal::String(D::string_literal(token)))
        }
        _ => Err(p.expected("a literal")),
    }
}

fn parse_trigger_event<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<TriggerEvent<'a>> {
    if p.eat_keyword("INSERT") {
        return Ok(TriggerEvent::Insert);
    }
    if p.eat_keyword("DELETE") {
        return Ok(TriggerEvent::Delete);
    }
    if p.eat_keyword("TRUNCATE") {
        return Ok(TriggerEvent::Truncate);
    }
    p.expect_keyword("UPDATE")?;
    let columns = if p.eat_keyword("OF") {
        p.parse_comma_separated(Parser::parse_ident)?
    } else {
        Vec::new()
    };
    Ok(TriggerEvent::Update(columns))
}

fn parse_create_rule<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
    or_replace: bool,
) -> PResult<PostgresStatement<'a>> {
    let name = p.parse_ident()?;
    p.expect_keyword("AS")?;
    p.expect_keyword("ON")?;
    let event = ["SELECT", "INSERT", "UPDATE", "DELETE"]
        .into_iter()
        .find(|word| p.eat_keyword(word))
        .ok_or_else(|| p.expected("SELECT, INSERT, UPDATE or DELETE"))?;
    p.expect_keyword("TO")?;
    let table = grammar::parse_object_name(p)?;
    let condition = grammar::parse_optional_where(p)?;
    p.expect_keyword("DO")?;
    let instead = if p.eat_keyword("INSTEAD") {
        Some(true)
    } else if p.eat_keyword("ALSO") {
        Some(false)
    } else {
        None
    };
    let action = if p.eat_keyword("NOTHING") {
        RuleAction::Nothing
    } else if p.eat_op("(") {
        let mut statements = Vec::new();
        loop {
            statements.push(p.nested(Parser::parse_statement)?);
            if !p.eat_op(";") {
                break;
            }
            if p.at_op(")") {
                break;
            }
        }
        p.expect_op(")")?;
        RuleAction::Multiple(statements)
    } else {
        RuleAction::Single(Box::new(p.nested(Parser::parse_statement)?))
    };
    Ok(PostgresStatement::CreateRule(Box::new(CreateRule {
        or_replace,
        name,
        event: event.into(),
        table,
        condition,
        instead,
        action,
    })))
}

fn at_user_mapping<D: Dialect>(p: &Parser<'_, D>) -> bool {
    p.at_keyword("USER")
        && p.peek_nth(1).is_word("MAPPING")
        && (p.peek_nth(2).is_word("FOR") || p.peek_nth(2).is_word("IF"))
}

/// Dispatches `CREATE` for the kinds only PostgreSQL has, or returns `None`
/// to let the caller handle it, with the cursor on the object type.
fn parse_policy_roles<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Vec<Ident<'a>>> {
    if p.eat_keyword("TO") {
        p.parse_comma_separated(Parser::parse_ident)
    } else {
        Ok(Vec::new())
    }
}

/// Parses `USING (expr)` and `WITH CHECK (expr)`, in that order.
fn parse_policy_conditions<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<PolicyConditions<'a>> {
    let parse_condition = |p: &mut Parser<'a, D>| {
        p.expect_op("(")?;
        let expr = p.parse_expr()?;
        p.expect_op(")")?;
        Ok(expr)
    };
    let using = if p.eat_keyword("USING") {
        Some(parse_condition(p)?)
    } else {
        None
    };
    let with_check = if p.at_keyword("WITH") && p.peek_nth(1).is_word("CHECK") {
        p.advance();
        p.advance();
        Some(parse_condition(p)?)
    } else {
        None
    };
    Ok((using, with_check))
}

fn parse_create_policy<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<PostgresStatement<'a>> {
    let name = p.parse_ident()?;
    p.expect_keyword("ON")?;
    let table = grammar::parse_object_name(p)?;
    let kind = if p.eat_keyword("AS") {
        let token = p.peek();
        if !(token.is_word("PERMISSIVE") || token.is_word("RESTRICTIVE")) {
            return Err(p.expected("PERMISSIVE or RESTRICTIVE"));
        }
        p.advance();
        Some(token.text)
    } else {
        None
    };
    let command = if p.eat_keyword("FOR") {
        let token = p.peek();
        if !["ALL", "SELECT", "INSERT", "UPDATE", "DELETE"]
            .iter()
            .any(|word| token.is_word(word))
        {
            return Err(p.expected("ALL, SELECT, INSERT, UPDATE or DELETE"));
        }
        p.advance();
        Some(token.text)
    } else {
        None
    };
    let to = parse_policy_roles(p)?;
    let (using, with_check) = parse_policy_conditions(p)?;
    Ok(PostgresStatement::CreatePolicy(Box::new(CreatePolicy {
        name,
        table,
        kind: kind.map(Cow::Borrowed),
        command: command.map(Cow::Borrowed),
        to,
        using,
        with_check,
    })))
}

fn parse_alter_policy<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<PostgresStatement<'a>> {
    let name = p.parse_ident()?;
    p.expect_keyword("ON")?;
    let table = grammar::parse_object_name(p)?;
    if p.at_keyword("RENAME") && p.peek_nth(1).is_word("TO") {
        p.advance();
        p.advance();
        let rename_to = Some(p.parse_ident()?);
        return Ok(PostgresStatement::AlterPolicy(Box::new(AlterPolicy {
            name,
            table,
            rename_to,
            to: Vec::new(),
            using: None,
            with_check: None,
        })));
    }
    let to = parse_policy_roles(p)?;
    let (using, with_check) = parse_policy_conditions(p)?;
    Ok(PostgresStatement::AlterPolicy(Box::new(AlterPolicy {
        name,
        table,
        rename_to: None,
        to,
        using,
        with_check,
    })))
}

pub fn parse_create_object<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
    or_replace: bool,
) -> PResult<Option<PostgresStatement<'a>>> {
    let token = p.peek();
    let next = p.peek_nth(1);
    if !or_replace {
        if p.at_keyword("POLICY") {
            p.advance();
            return parse_create_policy(p).map(Some);
        }
        if p.at_keyword("OPERATOR") {
            p.advance();
            return parse_create_operator(p).map(Some);
        }
        if p.at_keyword("EXTENSION") {
            p.advance();
            return parse_create_extension(p).map(Some);
        }
        if p.at_keyword("DOMAIN") {
            p.advance();
            return parse_create_domain(p).map(Some);
        }
        if p.at_keyword("TYPE") {
            p.advance();
            return parse_create_type(p).map(Some);
        }
        if p.at_keyword("COLLATION") {
            p.advance();
            return parse_create_collation(p).map(Some);
        }
        if p.at_keyword("TEXT") {
            p.advance();
            return parse_create_text_search(p).map(Some);
        }
        if p.at_keyword("SERVER") {
            p.advance();
            return parse_create_server(p).map(Some);
        }
        if p.at_keyword("FOREIGN") && next.is_word("DATA") {
            p.advance();
            return parse_create_fdw(p).map(Some);
        }
        if at_user_mapping(p) {
            p.advance();
            p.advance();
            return parse_create_user_mapping(p).map(Some);
        }
        for keyword in ["ROLE", "USER", "GROUP"] {
            if token.is_word(keyword) {
                p.advance();
                return parse_create_role(p, keyword).map(Some);
            }
        }
    }
    if p.eat_keyword("AGGREGATE") {
        return parse_create_aggregate(p, or_replace).map(Some);
    }
    if p.eat_keyword("FUNCTION") {
        return parse_create_function(p, or_replace).map(Some);
    }
    if p.eat_keyword("RULE") {
        return parse_create_rule(p, or_replace).map(Some);
    }
    if p.at_keyword("CONSTRAINT") && next.is_word("TRIGGER") {
        p.advance();
        p.advance();
        return parse_create_trigger(p, or_replace, true).map(Some);
    }
    if p.eat_keyword("TRIGGER") {
        return parse_create_trigger(p, or_replace, false).map(Some);
    }
    Ok(None)
}

/// Parses `ALTER` for objects other than tables, or returns `None`.
pub fn parse_alter_object<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Option<PostgresStatement<'a>>> {
    if p.at_keyword("OPERATOR") {
        return parse_alter_operator(p).map(Some);
    }
    if p.eat_keyword("POLICY") {
        return parse_alter_policy(p).map(Some);
    }
    if p.eat_keyword("AGGREGATE") {
        return parse_alter_function(p, true, "AGGREGATE").map(Some);
    }
    for keyword in ["FUNCTION", "PROCEDURE", "ROUTINE"] {
        if p.eat_keyword(keyword) {
            return parse_alter_function(p, false, keyword).map(Some);
        }
    }
    for keyword in ["ROLE", "USER", "GROUP"] {
        if p.eat_keyword(keyword) {
            return parse_alter_role(p, keyword).map(Some);
        }
    }
    if p.eat_keyword("TYPE") {
        return parse_alter_type(p).map(Some);
    }
    if p.eat_keyword("COLLATION") {
        return parse_alter_collation(p).map(Some);
    }
    if p.eat_keyword("TEXT") {
        return parse_alter_text_search(p).map(Some);
    }
    if p.eat_keyword("DEFAULT") {
        return parse_default_privileges(p).map(Some);
    }
    Ok(None)
}

impl fmt::Display for DefElem<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.name.fmt(f)?;
        if let Some(value) = &self.value {
            write!(f, " = {value}")?;
        }
        Ok(())
    }
}

impl fmt::Display for DefArg<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DefArg::Type(ty) => ty.fmt(f),
            DefArg::Operator(op) => op.fmt(f),
            DefArg::OperatorCall(op) => write!(f, "OPERATOR({op})"),
            DefArg::Number(n) => f.write_str(n),
            DefArg::String(s) => s.fmt(f),
            DefArg::Word(w) => f.write_str(w),
        }
    }
}

impl fmt::Display for ObjectAction<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ObjectAction::Rename(name) => write!(f, "RENAME TO {name}"),
            ObjectAction::Owner(name) => write!(f, "OWNER TO {name}"),
            ObjectAction::SetSchema(name) => write!(f, "SET SCHEMA {name}"),
        }
    }
}

impl fmt::Display for OperatorClassItem<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OperatorClassItem::Operator {
                strategy,
                operator,
                types,
                purpose,
            } => {
                write!(f, "OPERATOR {strategy}")?;
                if !operator.operator.is_empty() {
                    write!(f, " {operator}")?;
                }
                if let Some((left, right)) = types.as_deref() {
                    write!(f, " ({left}, {right})")?;
                }
                match purpose {
                    Some(OperatorPurpose::Search) => f.write_str(" FOR SEARCH"),
                    Some(OperatorPurpose::OrderBy(name)) => write!(f, " FOR ORDER BY {name}"),
                    None => Ok(()),
                }
            }
            OperatorClassItem::Function {
                support,
                op_types,
                name,
                args,
            } => {
                write!(f, "FUNCTION {support}")?;
                if !op_types.is_empty() {
                    write!(f, " ({})", comma(op_types))?;
                }
                if let Some(name) = name {
                    write!(f, " {name}")?;
                }
                if let Some(args) = args {
                    write!(f, "({})", comma(args))?;
                }
                Ok(())
            }
            OperatorClassItem::Storage(ty) => write!(f, "STORAGE {ty}"),
        }
    }
}

impl fmt::Display for AlterFamilyOp<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AlterFamilyOp::Add(items) => write!(f, "ADD {}", comma(items)),
            AlterFamilyOp::Drop(items) => write!(f, "DROP {}", comma(items)),
            AlterFamilyOp::Action(action) => action.fmt(f),
        }
    }
}

impl fmt::Display for AlterOperatorOp<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AlterOperatorOp::Action(action) => action.fmt(f),
            AlterOperatorOp::Set(definition) => write!(f, "SET ({})", comma(definition)),
        }
    }
}

impl fmt::Display for AggregateArgs<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AggregateArgs::Star => f.write_str("*"),
            AggregateArgs::List { args, order_by } => {
                comma(args).fmt(f)?;
                if !order_by.is_empty() {
                    if !args.is_empty() {
                        f.write_str(" ")?;
                    }
                    write!(f, "ORDER BY {}", comma(order_by))?;
                }
                Ok(())
            }
        }
    }
}

impl fmt::Display for CreateAggregate<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CREATE ")?;
        if self.or_replace {
            f.write_str("OR REPLACE ")?;
        }
        write!(f, "AGGREGATE {} ", self.name)?;
        if let Some(args) = &self.args {
            write!(f, "({args}) ")?;
        }
        write!(f, "({})", comma(&self.definition))
    }
}

impl fmt::Display for FunctionReturn<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FunctionReturn::Type(ty) => ty.fmt(f),
            FunctionReturn::SetOf(ty) => write!(f, "SETOF {ty}"),
            FunctionReturn::Table(columns) => write!(f, "TABLE({})", comma(columns)),
        }
    }
}

impl fmt::Display for ParameterSetting<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SET {}", self.name)?;
        match &self.value {
            ParameterValue::FromCurrent => f.write_str(" FROM CURRENT"),
            ParameterValue::Default => {
                write!(f, " {} DEFAULT", if self.to { "TO" } else { "=" })
            }
            ParameterValue::Values(values) => {
                write!(f, " {} {}", if self.to { "TO" } else { "=" }, comma(values))
            }
        }
    }
}

impl fmt::Display for CreateFunction<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CREATE ")?;
        if self.or_replace {
            f.write_str("OR REPLACE ")?;
        }
        write!(f, "FUNCTION {}({})", self.name, comma(&self.args))?;
        if let Some(returns) = &self.returns {
            write!(f, " RETURNS {returns}")?;
        }
        if let Some(language) = &self.language {
            write!(f, " LANGUAGE {language}")?;
        }
        if self.window {
            f.write_str(" WINDOW")?;
        }
        if let Some(ref volatility) = self.volatility {
            write!(f, " {volatility}")?;
        }
        if let Some(ref null_input) = self.null_input {
            write!(f, " {null_input}")?;
        }
        if let Some(ref parallel) = self.parallel {
            write!(f, " PARALLEL {parallel}")?;
        }
        if let Some(ref security) = self.security {
            write!(f, " SECURITY {security}")?;
        }
        match self.leakproof {
            Some(true) => f.write_str(" LEAKPROOF")?,
            Some(false) => f.write_str(" NOT LEAKPROOF")?,
            None => {}
        }
        if let Some(ref cost) = self.cost {
            write!(f, " COST {cost}")?;
        }
        if let Some(ref rows) = self.rows {
            write!(f, " ROWS {rows}")?;
        }
        if let Some(support) = &self.support {
            write!(f, " SUPPORT {support}")?;
        }
        for setting in &self.set {
            write!(f, " {setting}")?;
        }
        match &self.body {
            Some(FunctionBody::As { body, link_symbol }) => {
                write!(f, " AS {body}")?;
                if let Some(symbol) = link_symbol {
                    write!(f, ", {symbol}")?;
                }
            }
            Some(FunctionBody::Return(value)) => write!(f, " RETURN {value}")?,
            Some(FunctionBody::BeginAtomic(statements)) => {
                f.write_str(" BEGIN ATOMIC")?;
                for statement in statements {
                    write!(f, " {statement};")?;
                }
                f.write_str(" END")?;
            }
            None => {}
        }
        Ok(())
    }
}

impl fmt::Display for FunctionAttr<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FunctionAttr::Volatility(word) | FunctionAttr::NullInput(word) => f.write_str(word),
            FunctionAttr::Parallel(level) => write!(f, "PARALLEL {level}"),
            FunctionAttr::Security { external, kind } => {
                if *external {
                    f.write_str("EXTERNAL ")?;
                }
                write!(f, "SECURITY {kind}")
            }
            FunctionAttr::Leakproof(true) => f.write_str("LEAKPROOF"),
            FunctionAttr::Leakproof(false) => f.write_str("NOT LEAKPROOF"),
            FunctionAttr::Cost(n) => write!(f, "COST {n}"),
            FunctionAttr::Rows(n) => write!(f, "ROWS {n}"),
            FunctionAttr::Support(name) => write!(f, "SUPPORT {name}"),
            FunctionAttr::Set(setting) => setting.fmt(f),
            FunctionAttr::Reset(None) => f.write_str("RESET ALL"),
            FunctionAttr::Reset(Some(name)) => write!(f, "RESET {name}"),
            FunctionAttr::Restrict => f.write_str("RESTRICT"),
        }
    }
}

impl fmt::Display for AlterFunction<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ", self.signature)?;
        match &self.op {
            AlterFunctionOp::Action(action) => action.fmt(f),
            AlterFunctionOp::DependsOn { no, extension } => {
                if *no {
                    f.write_str("NO ")?;
                }
                write!(f, "DEPENDS ON EXTENSION {extension}")
            }
            AlterFunctionOp::Attributes(attrs) => crate::display::space(attrs).fmt(f),
        }
    }
}

impl fmt::Display for RoleOption<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RoleOption::Flag(word) => f.write_str(word),
            RoleOption::ConnectionLimit(limit) => write!(f, "CONNECTION LIMIT {limit}"),
            RoleOption::Password(None) => f.write_str("PASSWORD NULL"),
            RoleOption::Password(Some(password)) => {
                write!(f, "PASSWORD {}", password)
            }
            RoleOption::ValidUntil(when) => write!(f, "VALID UNTIL {}", when),
            RoleOption::Members { keyword, names } => write!(f, "{keyword} {}", comma(names)),
            RoleOption::Sysid(id) => write!(f, "SYSID {id}"),
        }
    }
}

impl fmt::Display for CreateRole<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CREATE {} {}", self.keyword, self.name)?;
        write_role_options(f, &self.options)
    }
}

impl fmt::Display for AlterRole<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ALTER ROLE {}", self.name)?;
        if let Some(database) = &self.in_database {
            write!(f, " IN DATABASE {database}")?;
        }
        match &self.op {
            AlterRoleOp::Rename(name) => write!(f, " RENAME TO {name}"),
            AlterRoleOp::Set(setting) => write!(f, " {setting}"),
            AlterRoleOp::Reset(None) => f.write_str(" RESET ALL"),
            AlterRoleOp::Reset(Some(name)) => write!(f, " RESET {name}"),
            AlterRoleOp::Options { options } => {
                f.write_str(" WITH")?;
                for option in options {
                    write!(f, " {option}")?;
                }
                Ok(())
            }
        }
    }
}

impl fmt::Display for CreateExtension<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CREATE EXTENSION ")?;
        crate::display::write_if_not_exists(f, self.if_not_exists)?;
        self.name.fmt(f)?;
        if self.with {
            f.write_str(" WITH")?;
        }
        if let Some(schema) = &self.schema {
            write!(f, " SCHEMA {schema}")?;
        }
        match &self.version {
            Some(ExtensionVersion::Name(version)) => write!(f, " VERSION {version}")?,
            Some(ExtensionVersion::String(version)) => write!(f, " VERSION {}", version)?,
            None => {}
        }
        if self.cascade {
            f.write_str(" CASCADE")?;
        }
        Ok(())
    }
}

impl fmt::Display for CreateDomain<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CREATE DOMAIN {}", self.name)?;
        if self.as_keyword {
            f.write_str(" AS")?;
        }
        write!(f, " {}", self.data_type)?;
        for option in &self.options {
            write!(f, " {option}")?;
        }
        Ok(())
    }
}

impl fmt::Display for CreateType<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CreateType::Enum { name, labels } => {
                write!(f, "CREATE TYPE {name} AS ENUM (")?;
                for (i, label) in labels.iter().enumerate() {
                    if i > 0 {
                        f.write_str(", ")?;
                    }
                    label.fmt(f)?;
                }
                f.write_str(")")
            }
            CreateType::Composite { name, columns } => {
                write!(f, "CREATE TYPE {name} AS ({})", comma(columns))
            }
            CreateType::Range { name, definition } => {
                write!(f, "CREATE TYPE {name} AS RANGE ({})", comma(definition))
            }
            CreateType::Base { name, definition } => {
                write!(f, "CREATE TYPE {name} ({})", comma(definition))
            }
            CreateType::Shell { name } => write!(f, "CREATE TYPE {name}"),
        }
    }
}

impl fmt::Display for AlterType<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ALTER TYPE {} ", self.name)?;
        match &self.op {
            AlterTypeOp::Action(action) => action.fmt(f),
            AlterTypeOp::AddValue {
                if_not_exists,
                value,
                position,
            } => {
                f.write_str("ADD VALUE ")?;
                crate::display::write_if_not_exists(f, *if_not_exists)?;
                value.fmt(f)?;
                match position {
                    Some((true, other)) => write!(f, " BEFORE {}", other),
                    Some((false, other)) => write!(f, " AFTER {}", other),
                    None => Ok(()),
                }
            }
            AlterTypeOp::RenameValue { old, new } => {
                write!(f, "RENAME VALUE {} TO {}", old, new)
            }
        }
    }
}

impl fmt::Display for CreateCollation<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CREATE COLLATION ")?;
        crate::display::write_if_not_exists(f, self.if_not_exists)?;
        self.name.fmt(f)?;
        match &self.source {
            CollationSource::Definition(definition) => write!(f, " ({})", comma(definition)),
            CollationSource::From(source) => write!(f, " FROM {source}"),
        }
    }
}

impl fmt::Display for AlterCollation<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ALTER COLLATION {} ", self.name)?;
        match &self.op {
            AlterCollationOp::Action(action) => action.fmt(f),
            AlterCollationOp::RefreshVersion => f.write_str("REFRESH VERSION"),
        }
    }
}

impl TextSearchKind {
    pub fn keyword(self) -> &'static str {
        match self {
            TextSearchKind::Configuration => "CONFIGURATION",
            TextSearchKind::Dictionary => "DICTIONARY",
            TextSearchKind::Parser => "PARSER",
            TextSearchKind::Template => "TEMPLATE",
        }
    }
}

impl fmt::Display for FdwClause<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FdwClause::Handler(Some(name)) => write!(f, "HANDLER {name}"),
            FdwClause::Handler(None) => f.write_str("NO HANDLER"),
            FdwClause::Validator(Some(name)) => write!(f, "VALIDATOR {name}"),
            FdwClause::Validator(None) => f.write_str("NO VALIDATOR"),
            FdwClause::Options(options) => write_options(f, options),
        }
    }
}

/// Writes `OPTIONS (...)`.
pub fn write_options(f: &mut fmt::Formatter<'_>, options: &[GenericOption<'_>]) -> fmt::Result {
    write!(f, "OPTIONS ({})", comma(options))
}

impl fmt::Display for CreateServer<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CREATE SERVER ")?;
        crate::display::write_if_not_exists(f, self.if_not_exists)?;
        self.name.fmt(f)?;
        if let Some(ref server_type) = self.server_type {
            write!(f, " TYPE {}", server_type)?;
        }
        if let Some(ref version) = self.version {
            write!(f, " VERSION {}", version)?;
        }
        write!(f, " FOREIGN DATA WRAPPER {}", self.wrapper)?;
        if !self.options.is_empty() {
            f.write_str(" ")?;
            write_options(f, &self.options)?;
        }
        Ok(())
    }
}

impl fmt::Display for CreateUserMapping<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CREATE USER MAPPING ")?;
        crate::display::write_if_not_exists(f, self.if_not_exists)?;
        write!(f, "FOR {} SERVER {}", self.user, self.server)?;
        if !self.options.is_empty() {
            f.write_str(" ")?;
            write_options(f, &self.options)?;
        }
        Ok(())
    }
}

fn write_object_words(f: &mut fmt::Formatter<'_>, words: &[Cow<'_, str>]) -> fmt::Result {
    for (i, word) in words.iter().enumerate() {
        if i > 0 {
            f.write_str(" ")?;
        }
        f.write_str(word)?;
    }
    Ok(())
}

impl fmt::Display for AlterDefaultPrivileges<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ALTER DEFAULT PRIVILEGES")?;
        for clause in &self.clauses {
            match clause {
                DefaultPrivilegesClause::Roles { keyword: _, names } => {
                    write!(f, " FOR ROLE {}", comma(names))?
                }
                DefaultPrivilegesClause::Schemas(names) => {
                    write!(f, " IN SCHEMA {}", comma(names))?
                }
            }
        }
        match &self.action {
            DefaultPrivilegesAction::Grant {
                privileges,
                object,
                grantees,
                with_grant_option,
            } => {
                write!(f, " GRANT {privileges} ON ")?;
                write_object_words(f, object)?;
                write!(f, " TO {}", comma(grantees))?;
                if *with_grant_option {
                    f.write_str(" WITH GRANT OPTION")?;
                }
                Ok(())
            }
            DefaultPrivilegesAction::Revoke {
                grant_option,
                privileges,
                object,
                grantees,
                behavior,
            } => {
                f.write_str(" REVOKE ")?;
                if *grant_option {
                    f.write_str("GRANT OPTION FOR ")?;
                }
                write!(f, "{privileges} ON ")?;
                write_object_words(f, object)?;
                write!(f, " FROM {}", comma(grantees))?;
                write_behavior(f, *behavior)
            }
        }
    }
}

/// Writes ` CASCADE` or ` RESTRICT` if present.
pub fn write_behavior(f: &mut fmt::Formatter<'_>, behavior: Option<DropBehavior>) -> fmt::Result {
    f.write_str(match behavior {
        Some(DropBehavior::Cascade) => " CASCADE",
        Some(DropBehavior::Restrict) => " RESTRICT",
        None => "",
    })
}

impl fmt::Display for TriggerEvent<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TriggerEvent::Insert => f.write_str("INSERT"),
            TriggerEvent::Delete => f.write_str("DELETE"),
            TriggerEvent::Truncate => f.write_str("TRUNCATE"),
            TriggerEvent::Update(columns) if columns.is_empty() => f.write_str("UPDATE"),
            TriggerEvent::Update(columns) => write!(f, "UPDATE OF {}", comma(columns)),
        }
    }
}

impl fmt::Display for CreateTrigger<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CREATE ")?;
        if self.or_replace {
            f.write_str("OR REPLACE ")?;
        }
        if self.constraint {
            f.write_str("CONSTRAINT ")?;
        }
        write!(f, "TRIGGER {} {} ", self.name, self.timing)?;
        for (i, event) in self.events.iter().enumerate() {
            if i > 0 {
                f.write_str(" OR ")?;
            }
            event.fmt(f)?;
        }
        write!(f, " ON {}", self.table)?;
        if let Some(from) = &self.from {
            write!(f, " FROM {from}")?;
        }
        for attribute in &self.attributes {
            write!(f, " {attribute}")?;
        }
        if !self.referencing.is_empty() {
            f.write_str(" REFERENCING")?;
            for item in &self.referencing {
                f.write_str(if item.new { " NEW" } else { " OLD" })?;
                if item.table {
                    f.write_str(" TABLE")?;
                }
                if item.as_keyword {
                    f.write_str(" AS")?;
                }
                write!(f, " {}", item.name)?;
            }
        }
        if let Some((each, row)) = self.for_each {
            f.write_str(" FOR ")?;
            if each {
                f.write_str("EACH ")?;
            }
            f.write_str(if row { "ROW" } else { "STATEMENT" })?;
        }
        if let Some(when) = &self.when {
            write!(f, " WHEN ({when})")?;
        }
        write!(
            f,
            " EXECUTE {} {}",
            if self.procedure {
                "PROCEDURE"
            } else {
                "FUNCTION"
            },
            self.function
        )?;
        if let Some(args) = &self.args {
            write!(f, "({})", comma(args))?;
        }
        Ok(())
    }
}

impl fmt::Display for CreateRule<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CREATE ")?;
        if self.or_replace {
            f.write_str("OR REPLACE ")?;
        }
        write!(
            f,
            "RULE {} AS ON {} TO {}",
            self.name, self.event, self.table
        )?;
        if let Some(condition) = &self.condition {
            write!(f, " WHERE {condition}")?;
        }
        f.write_str(" DO ")?;
        match self.instead {
            Some(true) => f.write_str("INSTEAD ")?,
            Some(false) => f.write_str("ALSO ")?,
            None => {}
        }
        match &self.action {
            RuleAction::Nothing => f.write_str("NOTHING"),
            RuleAction::Single(statement) => statement.fmt(f),
            RuleAction::Multiple(statements) => {
                f.write_str("(")?;
                for (i, statement) in statements.iter().enumerate() {
                    if i > 0 {
                        f.write_str("; ")?;
                    }
                    statement.fmt(f)?;
                }
                f.write_str(")")
            }
        }
    }
}

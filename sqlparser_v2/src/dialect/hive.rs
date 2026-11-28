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

//! Apache Hive, built on the Spark SQL rules.

use std::borrow::Cow;
use std::fmt;

use crate::PResult;
use crate::ast::BinaryOp;
use crate::ast::{
    AlterTableOp, ColumnOption, CreateFlags, CreateTable, Cte, DataType, Expr, GroupByModifier,
    Ident, Join, LateralView, ObjectName, OrderByItem, Quantifier, QueryDistribution, SampleClause,
    SelectItem, Span, Statement, StatementKind, StringLiteral, TableFactor, TableRef,
};
use crate::dialect::builtin::BuiltinStatement;
use crate::dialect::databricks;
use crate::dialect::mysql::MySqlExpr;
use crate::dialect::spark::{self, PartitionColumn, SparkOption};
use crate::dialect::{Builtin, Dialect, ExprExtOf};
use crate::display::{comma, write_columns, write_create_flags, write_if_not_exists};
use crate::grammar::{self, prec};
use crate::lexer::{self, LexResult, Lexeme, Token, TokenKind, quoted};
use crate::parser::{InfixRule, Parser, SetOperatorRule};

#[derive(Debug, Clone, PartialEq)]
pub struct Hive;

#[derive(Debug, Clone, PartialEq)]
pub struct HiveCreateConnector<'a> {
    pub if_not_exists: bool,
    pub name: Ident<'a>,
    pub connector_type: Option<StringLiteral<'a>>,
    pub url: Option<StringLiteral<'a>>,
    pub comment: Option<StringLiteral<'a>>,
    pub properties: Vec<(StringLiteral<'a>, StringLiteral<'a>)>,
}

/// What `ALTER CONNECTOR ... SET` changes.
#[derive(Debug, Clone, PartialEq)]
pub enum HiveAlterConnectorAction<'a> {
    Properties(Vec<(StringLiteral<'a>, StringLiteral<'a>)>),
    Url(StringLiteral<'a>),
    Owner(Cow<'a, str>, StringLiteral<'a>),
}

/// Parses `CREATE`, `ALTER` or `DROP CONNECTOR`, or returns `None` at anything else.
fn parse_connector<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Option<Statement<'a, Builtin>>> {
    let verb = ["CREATE", "ALTER", "DROP"]
        .into_iter()
        .find(|verb| p.at_keyword(verb));
    if verb.is_none() || !p.peek_nth(1).is_word("CONNECTOR") {
        return Ok(None);
    }
    p.advance();
    p.advance();
    let parsed = match verb {
        Some("CREATE") => {
            let if_not_exists = grammar::parse_if_not_exists(p)?;
            let name = p.parse_ident()?;
            let connector_type = parse_connector_string(p, "TYPE")?;
            let url = parse_connector_string(p, "URL")?;
            let comment = parse_connector_string(p, "COMMENT")?;
            let properties = if p.at_keyword("WITH") && p.peek_nth(1).is_word("DCPROPERTIES") {
                p.advance();
                p.advance();
                parse_connector_properties(p)?
            } else {
                Vec::new()
            };
            HiveStatement::CreateConnector(Box::new(HiveCreateConnector {
                if_not_exists,
                name,
                connector_type,
                url,
                comment,
                properties,
            }))
        }
        Some("DROP") => {
            let if_exists = p.at_keyword("IF") && p.peek_nth(1).is_word("EXISTS");
            if if_exists {
                p.advance();
                p.advance();
            }
            HiveStatement::DropConnector {
                if_exists,
                name: p.parse_ident()?,
            }
        }
        _ => {
            let name = p.parse_ident()?;
            p.expect_keyword("SET")?;
            let action = if p.eat_keyword("DCPROPERTIES") {
                HiveAlterConnectorAction::Properties(parse_connector_properties(p)?)
            } else if p.eat_keyword("URL") {
                HiveAlterConnectorAction::Url(spark::parse_string(p)?)
            } else {
                p.expect_keyword("OWNER")?;
                let kind = if p.eat_keyword("USER") {
                    "USER"
                } else {
                    p.expect_keyword("ROLE")?;
                    "ROLE"
                };
                HiveAlterConnectorAction::Owner(kind.into(), spark::parse_string(p)?)
            };
            HiveStatement::AlterConnector { name, action }
        }
    };
    Ok(Some(hive(parsed)))
}

fn parse_connector_string<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
    keyword: &str,
) -> PResult<Option<StringLiteral<'a>>> {
    if p.at_keyword(keyword) && p.peek_nth(1).kind == TokenKind::String {
        p.advance();
        return spark::parse_string(p).map(Some);
    }
    Ok(None)
}

fn parse_connector_properties<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Vec<(StringLiteral<'a>, StringLiteral<'a>)>> {
    p.expect_op("(")?;
    let properties = p.parse_comma_separated(|p| {
        let key = spark::parse_string(p)?;
        p.expect_op("=")?;
        Ok((key, spark::parse_string(p)?))
    })?;
    p.expect_op(")")?;
    Ok(properties)
}

/// Statements only Hive has.
#[derive(Debug, Clone, PartialEq)]
pub enum HiveStatement<'a> {
    CreateConnector(Box<HiveCreateConnector<'a>>),
    DropConnector {
        if_exists: bool,
        name: Ident<'a>,
    },
    AlterConnector {
        name: Ident<'a>,
        action: HiveAlterConnectorAction<'a>,
    },
    CreateTable(Box<HiveCreateTable<'a>>),
    CreateFunction(Box<HiveCreateFunction<'a>>),
    /// `WITH ... [FROM name] INSERT ...`
    WithInsert {
        with: Vec<Cte<'a, Builtin>>,
        from: Option<ObjectName<'a>>,
        insert: Box<Statement<'a, Builtin>>,
    },
    Analyze(Box<HiveAnalyze<'a>>),
    /// `MSCK [REPAIR] TABLE name [ADD | DROP | SYNC PARTITIONS]`
    Msck {
        repair: bool,
        table: ObjectName<'a>,
        action: Option<Cow<'a, str>>,
    },
    /// `SET HIVEVAR:name = value, ...`
    SetHiveVar {
        name: ObjectName<'a>,
        values: Vec<Expr<'a, Builtin>>,
    },
    LoadData(Box<HiveLoadData<'a>>),
}

/// `CREATE [TEMPORARY] [EXTERNAL] TABLE` with clauses only Hive has.
#[derive(Debug, Clone, PartialEq)]
pub struct HiveCreateTable<'a> {
    pub table: CreateTable<'a, Builtin>,
    pub external: bool,
    pub like: Option<ObjectName<'a>>,
    /// In the order written.
    pub clauses: Vec<HiveTableClause<'a>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum HiveTableClause<'a> {
    Comment(StringLiteral<'a>),
    PartitionedBy(Vec<PartitionColumn<'a>>),
    ClusteredBy(Box<HiveClusteredBy<'a>>),
    RowFormat(HiveRowFormat<'a>),
    Stored(HiveStorage<'a>),
    Location(StringLiteral<'a>),
    TblProperties(Vec<SparkOption<'a>>),
}

/// `CLUSTERED BY (a) [SORTED BY (a ASC)] INTO n BUCKETS`
#[derive(Debug, Clone, PartialEq)]
pub struct HiveClusteredBy<'a> {
    pub columns: Vec<Ident<'a>>,
    pub sorted_by: Vec<OrderByItem<'a, Builtin>>,
    pub buckets: Cow<'a, str>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum HiveRowFormat<'a> {
    /// `SERDE 'class' [WITH SERDEPROPERTIES (...)]`
    Serde {
        class: StringLiteral<'a>,
        properties: Vec<SparkOption<'a>>,
    },
    /// `DELIMITED` and its delimiters, each with the words before it.
    Delimited(Vec<(Cow<'a, str>, StringLiteral<'a>)>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum HiveStorage<'a> {
    /// `STORED AS format`
    Format(Ident<'a>),
    /// `STORED AS INPUTFORMAT 'class' OUTPUTFORMAT 'class'`
    InputOutput {
        input: StringLiteral<'a>,
        output: StringLiteral<'a>,
    },
}

/// `CREATE [OR REPLACE] [TEMPORARY] FUNCTION name AS 'class' [USING kind 'uri']`
#[derive(Debug, Clone, PartialEq)]
pub struct HiveCreateFunction<'a> {
    pub or_replace: bool,
    pub temporary: bool,
    pub name: ObjectName<'a>,
    pub class: StringLiteral<'a>,
    pub using: Option<(Cow<'a, str>, StringLiteral<'a>)>,
}

/// `ANALYZE TABLE name ...`
#[derive(Debug, Clone, PartialEq)]
pub struct HiveAnalyze<'a> {
    pub table: ObjectName<'a>,
    pub partition: Vec<Expr<'a, Builtin>>,
    pub compute_statistics: bool,
    pub noscan: bool,
    pub cache_metadata: bool,
    pub for_columns: bool,
    pub columns: Vec<Ident<'a>>,
}

/// `LOAD DATA [LOCAL] INPATH 'path' [OVERWRITE] INTO TABLE name ...`
#[derive(Debug, Clone, PartialEq)]
pub struct HiveLoadData<'a> {
    pub local: bool,
    pub path: StringLiteral<'a>,
    pub overwrite: bool,
    pub table: ObjectName<'a>,
    pub partition: Vec<Expr<'a, Builtin>>,
    /// `INPUTFORMAT expr SERDE expr`
    pub format: Option<(Expr<'a, Builtin>, Expr<'a, Builtin>)>,
}

#[inline]
fn is_name_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'$' | b'{' | b'}')
}

/// Lexes a name holding `$`, `{` or `}`, and a number ending in `L`.
#[inline]
fn lex_name(rest: &str) -> Option<LexResult> {
    let bytes = rest.as_bytes();
    if !bytes[0].is_ascii_alphanumeric() && bytes[0] != b'$' {
        return None;
    }
    if bytes[0] == b'$' && bytes.get(1).is_some_and(u8::is_ascii_digit) {
        return None;
    }
    let len = bytes.iter().take_while(|b| is_name_byte(**b)).count();
    let run = &bytes[..len];
    if run.iter().any(|b| matches!(b, b'$' | b'{' | b'}')) {
        return Some(Ok(Lexeme {
            kind: TokenKind::Word,
            len,
        }));
    }
    let digits = run.iter().take_while(|b| b.is_ascii_digit()).count();
    if digits > 0 && digits + 1 == len && run[digits] == b'L' {
        return Some(Ok(Lexeme {
            kind: TokenKind::Number,
            len,
        }));
    }
    None
}

#[inline]
pub fn lex_token(rest: &str) -> Option<LexResult> {
    match rest.as_bytes() {
        [b'"', ..] => Some(quoted(rest, TokenKind::QuotedIdent)),
        [b'<', b'=', b'>', ..] => Some(Ok(Lexeme {
            kind: TokenKind::Op,
            len: 3,
        })),
        _ => lex_name(rest)
            .or_else(|| databricks::lex_digit_name(rest))
            .or_else(|| spark::lex_token(rest)),
    }
}

/// Parses `PARTITION`'s parenthesized list, with the keyword still to read.
fn parse_partition<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Vec<Expr<'a, D::Ext>>> {
    p.expect_keyword("PARTITION")?;
    p.expect_op("(")?;
    let partition = p.parse_comma_separated(Parser::parse_expr)?;
    p.expect_op(")")?;
    Ok(partition)
}

fn parse_optional_partition<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<Vec<Expr<'a, D::Ext>>> {
    if p.at_keyword("PARTITION") {
        parse_partition(p)
    } else {
        Ok(Vec::new())
    }
}

fn parse_clustered_by<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<HiveClusteredBy<'a>> {
    p.expect_keyword("BY")?;
    let columns = grammar::parse_ident_list(p)?;
    let sorted_by = if p.eat_keyword("SORTED") {
        p.expect_keyword("BY")?;
        p.expect_op("(")?;
        let sorted_by = p.parse_comma_separated(D::parse_order_by_item)?;
        p.expect_op(")")?;
        sorted_by
    } else {
        Vec::new()
    };
    p.expect_keyword("INTO")?;
    let buckets = grammar::parse_number(p)?;
    p.expect_keyword("BUCKETS")?;
    Ok(HiveClusteredBy {
        columns,
        sorted_by,
        buckets: buckets.into(),
    })
}

/// Parses one optional delimiter of `ROW FORMAT DELIMITED`.
fn parse_delimiters<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<Vec<(&'static str, StringLiteral<'a>)>> {
    const DELIMITERS: [(&[&str], &str); 5] = [
        (&["FIELDS", "TERMINATED", "BY"], "FIELDS TERMINATED BY"),
        (
            &["COLLECTION", "ITEMS", "TERMINATED", "BY"],
            "COLLECTION ITEMS TERMINATED BY",
        ),
        (
            &["MAP", "KEYS", "TERMINATED", "BY"],
            "MAP KEYS TERMINATED BY",
        ),
        (&["LINES", "TERMINATED", "BY"], "LINES TERMINATED BY"),
        (&["NULL", "DEFINED", "AS"], "NULL DEFINED AS"),
    ];
    let mut delimiters = Vec::new();
    loop {
        let found = DELIMITERS
            .iter()
            .find(|(words, _)| p.at_keyword(words[0]) && p.peek_nth(1).is_word(words[1]));
        let Some((words, label)) = found else {
            return Ok(delimiters);
        };
        for word in *words {
            p.expect_keyword(word)?;
        }
        delimiters.push((*label, spark::parse_string(p)?));
        if *label == "FIELDS TERMINATED BY" && p.at_keyword("ESCAPED") {
            p.advance();
            p.expect_keyword("BY")?;
            delimiters.push(("ESCAPED BY", spark::parse_string(p)?));
        }
    }
}

fn parse_row_format<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<HiveRowFormat<'a>> {
    p.expect_keyword("FORMAT")?;
    if p.eat_keyword("DELIMITED") {
        return parse_delimiters(p).map(|delimiters| {
            HiveRowFormat::Delimited(delimiters.into_iter().map(|(k, v)| (k.into(), v)).collect())
        });
    }
    p.expect_keyword("SERDE")?;
    let class = spark::parse_string(p)?;
    let properties = if p.at_keyword("WITH") && p.peek_nth(1).is_word("SERDEPROPERTIES") {
        p.advance();
        p.advance();
        spark::parse_option_list(p)?
    } else {
        Vec::new()
    };
    Ok(HiveRowFormat::Serde { class, properties })
}

fn parse_stored<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<HiveStorage<'a>> {
    p.expect_keyword("AS")?;
    if p.eat_keyword("INPUTFORMAT") {
        let input = spark::parse_string(p)?;
        p.expect_keyword("OUTPUTFORMAT")?;
        let output = spark::parse_string(p)?;
        return Ok(HiveStorage::InputOutput { input, output });
    }
    p.parse_ident().map(HiveStorage::Format)
}

/// Parses a table clause, with the place it holds in Hive's fixed order.
fn parse_table_clause<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Option<(u8, HiveTableClause<'a>)>> {
    use HiveTableClause as Clause;
    if let Some(comment) = spark::parse_optional_comment(p)? {
        return Ok(Some((0, Clause::Comment(comment))));
    }
    if p.eat_keyword("PARTITIONED") {
        p.expect_keyword("BY")?;
        p.expect_op("(")?;
        let columns = p.parse_comma_separated(spark::parse_partition_column)?;
        p.expect_op(")")?;
        return Ok(Some((1, Clause::PartitionedBy(columns))));
    }
    if p.eat_keyword("CLUSTERED") {
        let clustered = parse_clustered_by(p)?;
        return Ok(Some((2, Clause::ClusteredBy(Box::new(clustered)))));
    }
    if p.at_keyword("ROW") && p.peek_nth(1).is_word("FORMAT") {
        p.advance();
        return Ok(Some((3, Clause::RowFormat(parse_row_format(p)?))));
    }
    if p.eat_keyword("STORED") {
        return Ok(Some((3, Clause::Stored(parse_stored(p)?))));
    }
    if p.eat_keyword("LOCATION") {
        return Ok(Some((3, Clause::Location(spark::parse_string(p)?))));
    }
    if p.eat_keyword("TBLPROPERTIES") {
        return Ok(Some((
            4,
            Clause::TblProperties(spark::parse_option_list(p)?),
        )));
    }
    Ok(None)
}

fn parse_table_clauses<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Vec<HiveTableClause<'a>>> {
    let mut clauses: Vec<HiveTableClause<'a>> = Vec::new();
    let mut last_rank = 0;
    while let Some((rank, clause)) = parse_table_clause(p)? {
        let repeated = clauses
            .iter()
            .any(|seen| std::mem::discriminant(seen) == std::mem::discriminant(&clause));
        if rank < last_rank || repeated {
            return Err(p.expected("table clauses in order, each only once"));
        }
        last_rank = rank;
        clauses.push(clause);
    }
    Ok(clauses)
}

fn parse_create_table<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Statement<'a, Builtin>> {
    p.expect_keyword("CREATE")?;
    let or_replace = grammar::parse_or_replace(p)?;
    let temporary = p.eat_keyword("TEMPORARY") || p.eat_keyword("TEMP");
    let external = p.eat_keyword("EXTERNAL");
    p.expect_keyword("TABLE")?;
    let if_not_exists = grammar::parse_if_not_exists(p)?;
    let name = grammar::parse_object_name(p)?;
    let like = if p.eat_keyword("LIKE") {
        Some(grammar::parse_object_name(p)?)
    } else {
        None
    };
    let (columns, constraints) = if like.is_none() && p.at_op("(") {
        grammar::parse_columns(p)?
    } else {
        (Vec::new(), Vec::new())
    };
    let clauses = parse_table_clauses(p)?;
    let query = if like.is_none() && p.eat_keyword("AS") {
        Some(Box::new(D::parse_query(p)?))
    } else {
        None
    };
    if columns.is_empty() && query.is_none() && clauses.is_empty() && like.is_none() {
        return Err(p.expected("column definitions, table clauses, LIKE or AS"));
    }
    let table = CreateTable {
        or_replace,
        temporary,
        name,
        if_not_exists,
        columns,
        constraints,
        query,
    };
    if !external && clauses.is_empty() && like.is_none() {
        return Ok(StatementKind::CreateTable(Box::new(table)).into());
    }
    Ok(hive(HiveStatement::CreateTable(Box::new(
        HiveCreateTable {
            table,
            external,
            like,
            clauses,
        },
    ))))
}

/// True at `CREATE [OR REPLACE] [TEMPORARY] FUNCTION`.
fn at_create_function<'a, D: Dialect>(p: &Parser<'a, D>) -> bool {
    let mut n = 1;
    if p.peek_nth(n).is_word("OR") {
        n += 2;
    }
    if p.peek_nth(n).is_word("TEMPORARY") {
        n += 1;
    }
    p.at_keyword("CREATE") && p.peek_nth(n).is_word("FUNCTION")
}

fn parse_create_function<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<HiveCreateFunction<'a>> {
    p.expect_keyword("CREATE")?;
    let or_replace = grammar::parse_or_replace(p)?;
    let temporary = p.eat_keyword("TEMPORARY");
    p.expect_keyword("FUNCTION")?;
    let name = grammar::parse_object_name(p)?;
    p.expect_keyword("AS")?;
    let class = spark::parse_string(p)?;
    let using = if p.eat_keyword("USING") {
        let Some(kind) = ["JAR", "FILE", "ARCHIVE"]
            .into_iter()
            .find(|kind| p.eat_keyword(kind))
        else {
            return Err(p.expected("JAR, FILE or ARCHIVE"));
        };
        Some((kind, spark::parse_string(p)?))
    } else {
        None
    };
    Ok(HiveCreateFunction {
        or_replace,
        temporary,
        name,
        class,
        using: using.map(|(kind, text)| (kind.into(), text)),
    })
}

/// Parses `CREATE`, handling Hive's tables and functions.
pub fn parse_create<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Statement<'a, Builtin>> {
    if at_create_function(p) {
        let function = parse_create_function(p)?;
        return Ok(hive(HiveStatement::CreateFunction(Box::new(function))));
    }
    if spark::at_create_table(p) {
        return parse_create_table(p);
    }
    grammar::parse_create(p)
}

fn parse_analyze<'a, D: Dialect<Ext = Builtin>>(p: &mut Parser<'a, D>) -> PResult<HiveAnalyze<'a>> {
    p.expect_keyword("ANALYZE")?;
    p.expect_keyword("TABLE")?;
    let mut analyze = HiveAnalyze {
        table: grammar::parse_object_name(p)?,
        partition: Vec::new(),
        compute_statistics: false,
        noscan: false,
        cache_metadata: false,
        for_columns: false,
        columns: Vec::new(),
    };
    loop {
        if p.at_keyword("PARTITION") {
            analyze.partition = parse_partition(p)?;
        } else if p.eat_keyword("NOSCAN") {
            analyze.noscan = true;
        } else if p.eat_keyword("COMPUTE") {
            p.expect_keyword("STATISTICS")?;
            analyze.compute_statistics = true;
        } else if p.eat_keyword("CACHE") {
            p.expect_keyword("METADATA")?;
            analyze.cache_metadata = true;
        } else if p.eat_keyword("FOR") {
            p.expect_keyword("COLUMNS")?;
            analyze.for_columns = true;
            if matches!(p.peek().kind, TokenKind::Word | TokenKind::QuotedIdent)
                && !at_analyze_clause(p)
            {
                analyze.columns = p.parse_comma_separated(Parser::parse_ident)?;
            }
        } else {
            return Ok(analyze);
        }
    }
}

fn at_analyze_clause<'a, D: Dialect>(p: &Parser<'a, D>) -> bool {
    ["PARTITION", "NOSCAN", "COMPUTE", "CACHE", "FOR"]
        .into_iter()
        .any(|word| p.at_keyword(word))
}

fn parse_msck<'a, D: Dialect<Ext = Builtin>>(p: &mut Parser<'a, D>) -> PResult<HiveStatement<'a>> {
    p.expect_keyword("MSCK")?;
    let repair = p.eat_keyword("REPAIR");
    p.expect_keyword("TABLE")?;
    let table = grammar::parse_object_name(p)?;
    let action = ["ADD", "DROP", "SYNC"]
        .into_iter()
        .find(|action| p.eat_keyword(action));
    if action.is_some() {
        p.expect_keyword("PARTITIONS")?;
    }
    Ok(HiveStatement::Msck {
        repair,
        table,
        action: action.map(Cow::Borrowed),
    })
}

/// True at `SET HIVEVAR:name`, which lexes as a word and then a parameter.
fn at_set_hivevar<'a, D: Dialect>(p: &Parser<'a, D>) -> bool {
    let name = p.peek_nth(2);
    p.at_keyword("SET")
        && p.peek_nth(1).is_word("HIVEVAR")
        && name.kind == TokenKind::Placeholder
        && name.text.starts_with(':')
}

fn parse_set_hivevar<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<HiveStatement<'a>> {
    p.advance();
    p.advance();
    let token = p.peek();
    p.advance();
    let mut parts = vec![Ident {
        text: Cow::Borrowed(&token.text[1..]),
        quote: None,
        span: Span {
            start: token.span.start + 1,
            end: token.span.end,
        },
    }];
    while p.eat_op(".") {
        parts.push(p.parse_name_part()?);
    }
    if !p.eat_op("=") && !p.eat_keyword("TO") {
        return Err(p.expected("= or TO"));
    }
    let values = p.parse_comma_separated(Parser::parse_expr)?;
    Ok(HiveStatement::SetHiveVar {
        name: ObjectName { parts },
        values,
    })
}

fn parse_load_data<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<HiveLoadData<'a>> {
    p.expect_keyword("LOAD")?;
    p.expect_keyword("DATA")?;
    let local = p.eat_keyword("LOCAL");
    p.expect_keyword("INPATH")?;
    let path = spark::parse_string(p)?;
    let overwrite = p.eat_keyword("OVERWRITE");
    p.expect_keyword("INTO")?;
    p.expect_keyword("TABLE")?;
    let table = grammar::parse_object_name(p)?;
    let partition = parse_optional_partition(p)?;
    let format = if p.eat_keyword("INPUTFORMAT") {
        let input = p.parse_expr()?;
        p.expect_keyword("SERDE")?;
        Some((input, p.parse_expr()?))
    } else {
        None
    };
    Ok(HiveLoadData {
        local,
        path,
        overwrite,
        table,
        partition,
        format,
    })
}

/// Parses a statement that starts with `WITH`, which can be an `INSERT`.
fn parse_with<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Statement<'a, Builtin>> {
    p.expect_keyword("WITH")?;
    let with = p.parse_comma_separated(D::parse_cte)?;
    let from = if p.eat_keyword("FROM") {
        Some(grammar::parse_object_name(p)?)
    } else {
        None
    };
    if from.is_none() && !p.at_keyword("INSERT") {
        let with = Some(Box::new(crate::ast::With {
            recursive: false,
            ctes: with,
        }));
        let query = grammar::parse_query_after_with(p, with)?;
        return Ok(StatementKind::Query(Box::new(query)).into());
    }
    let insert = Box::new(D::parse_insert_statement(p)?);
    Ok(hive(HiveStatement::WithInsert { with, from, insert }))
}

fn hive<'a>(statement: HiveStatement<'a>) -> Statement<'a, Builtin> {
    StatementKind::Ext(BuiltinStatement::Hive(statement)).into()
}

pub fn parse_statement<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Statement<'a, Builtin>> {
    if let Some(statement) = parse_connector(p)? {
        return Ok(statement);
    }
    if grammar::at_alter_view(p) {
        return grammar::parse_alter_view(p, false)
            .map(|a| StatementKind::AlterView(Box::new(a)).into());
    }
    if p.at_keyword("WITH") {
        return parse_with(p);
    }
    if p.at_keyword("ANALYZE") {
        return Ok(hive(HiveStatement::Analyze(Box::new(parse_analyze(p)?))));
    }
    if p.at_keyword("MSCK") {
        return parse_msck(p).map(hive);
    }
    if at_set_hivevar(p) {
        return parse_set_hivevar(p).map(hive);
    }
    if p.at_keyword("LOAD") {
        return Ok(hive(HiveStatement::LoadData(Box::new(parse_load_data(p)?))));
    }
    spark::parse_statement(p)
}

/// Parses an `ALTER TABLE` operation, adding those on partitions.
pub fn parse_alter_table_op<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<AlterTableOp<'a, D::Ext>> {
    if p.at_keyword("ADD") {
        let if_not_exists = p.peek_nth(1).is_word("IF") && p.peek_nth(2).is_word("NOT");
        let words = if if_not_exists { 4 } else { 1 };
        if p.peek_nth(words).is_word("PARTITION") {
            for _ in 0..words {
                p.advance();
            }
            let mut partitions = Vec::new();
            while p.at_keyword("PARTITION") {
                partitions.push(parse_partition(p)?);
            }
            return Ok(AlterTableOp::AddPartitions {
                if_not_exists,
                partitions,
            });
        }
    }
    if p.at_keyword("DROP") {
        let if_exists = p.peek_nth(1).is_word("IF") && p.peek_nth(2).is_word("EXISTS");
        let words = if if_exists { 3 } else { 1 };
        if p.peek_nth(words).is_word("PARTITION") {
            for _ in 0..words {
                p.advance();
            }
            let partition = parse_partition(p)?;
            return Ok(AlterTableOp::DropPartition {
                if_exists,
                partition,
            });
        }
    }
    if p.at_keyword("PARTITION") {
        let old = parse_partition(p)?;
        p.expect_keyword("RENAME")?;
        p.expect_keyword("TO")?;
        let new = parse_partition(p)?;
        let set = p.at_keyword("SET") && p.peek_nth(1).is_word("LOCATION");
        if set {
            p.advance();
        }
        let location = if p.eat_keyword("LOCATION") {
            Some((set, spark::parse_string(p)?))
        } else {
            None
        };
        return Ok(AlterTableOp::RenamePartition { old, new, location });
    }
    spark::parse_alter_table_op(p)
}

/// Parses a table, whose name can be the word `LIMIT`.
pub fn parse_table_factor<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<TableFactor<'a, D::Ext>> {
    if p.at_keyword("LIMIT") {
        let token = p.peek();
        p.advance();
        return Ok(TableFactor::Named(ObjectName {
            parts: vec![Ident {
                text: token.text.into(),
                quote: None,
                span: token.span,
            }],
        }));
    }
    grammar::parse_table_factor(p)
}

pub fn infix_rule<'a, D>(p: &Parser<'a, D>) -> Option<InfixRule<'a, D>>
where
    D: Dialect,
    ExprExtOf<'a, D>: From<MySqlExpr<'a, D::Ext>>,
{
    if p.at_op("<=>") {
        return Some(InfixRule::binary(
            prec::COMPARE,
            BinaryOp::Named("<=>".into()),
        ));
    }
    spark::infix_rule(p)
}

impl Dialect for Hive {
    type Ext = Builtin;

    fn parse_create_extension<'a>(
        p: &mut Parser<'a, Self>,
        flags: CreateFlags,
    ) -> PResult<Option<Statement<'a, Builtin>>> {
        grammar::parse_create_role(p, flags, false)
    }

    fn parse_like_quantifier<'a>(p: &mut Parser<'a, Self>) -> PResult<Option<Quantifier>> {
        Ok(grammar::parse_like_any(p))
    }

    fn set_operator_rule<'a>(p: &Parser<'a, Self>) -> Option<SetOperatorRule> {
        grammar::set_operator_rule_with_minus(p)
    }

    #[inline]
    fn is_reserved_for_alias(word: &str) -> bool {
        word.eq_ignore_ascii_case("MINUS") || grammar::is_reserved_for_alias(word)
    }

    fn parse_group_by_suffix<'a>(
        p: &mut Parser<'a, Self>,
    ) -> PResult<Option<Box<Expr<'a, Builtin>>>> {
        grammar::parse_group_by_grouping_sets(p)
    }

    fn parse_cte_columns<'a>(_p: &mut Parser<'a, Self>) -> PResult<Vec<Ident<'a>>> {
        Ok(Vec::new())
    }

    fn parse_statement<'a>(p: &mut Parser<'a, Self>) -> PResult<Statement<'a, Builtin>> {
        parse_statement(p)
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

    fn parse_alter_table_op<'a>(p: &mut Parser<'a, Self>) -> PResult<AlterTableOp<'a, Builtin>> {
        parse_alter_table_op(p)
    }

    fn eat_drop_purge<'a>(p: &mut Parser<'a, Self>) -> bool {
        p.eat_keyword("PURGE")
    }

    #[inline]
    fn lex_token(rest: &str) -> Option<LexResult> {
        lex_token(rest)
    }

    #[inline]
    fn lex_comment(rest: &str) -> Option<Result<usize, &'static str>> {
        if rest.starts_with('#') {
            return Some(Ok(lexer::line_comment(rest)));
        }
        lexer::lex_core_comment(rest)
    }

    #[inline]
    fn string_literal(token: Token<'_>) -> StringLiteral<'_> {
        StringLiteral {
            backslash_escapes: true,
            ..grammar::string_literal(token)
        }
    }

    fn parse_prefix<'a>(p: &mut Parser<'a, Self>) -> PResult<Expr<'a, Builtin>> {
        spark::parse_prefix(p)
    }

    fn infix_rule<'a>(p: &Parser<'a, Self>) -> Option<InfixRule<'a, Self>> {
        infix_rule(p)
    }

    fn parse_table_factor<'a>(p: &mut Parser<'a, Self>) -> PResult<TableFactor<'a, Builtin>> {
        parse_table_factor(p)
    }

    fn parse_group_by_item<'a>(p: &mut Parser<'a, Self>) -> PResult<Expr<'a, Builtin>> {
        grammar::parse_grouping_item(p)
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
        parse_create(p)
    }
}

impl fmt::Display for HiveRowFormat<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Serde { class, properties } => {
                write!(f, "ROW FORMAT SERDE {}", class)?;
                if !properties.is_empty() {
                    write!(f, " WITH SERDEPROPERTIES ({})", comma(properties))?;
                }
                Ok(())
            }
            Self::Delimited(delimiters) => {
                f.write_str("ROW FORMAT DELIMITED")?;
                for (label, value) in delimiters {
                    write!(f, " {label} {}", value)?;
                }
                Ok(())
            }
        }
    }
}

impl fmt::Display for HiveStorage<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Format(format) => write!(f, "STORED AS {format}"),
            Self::InputOutput { input, output } => {
                write!(f, "STORED AS INPUTFORMAT {} OUTPUTFORMAT {}", input, output)
            }
        }
    }
}

impl fmt::Display for HiveClusteredBy<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CLUSTERED BY ({})", comma(&self.columns))?;
        if !self.sorted_by.is_empty() {
            write!(f, " SORTED BY ({})", comma(&self.sorted_by))?;
        }
        write!(f, " INTO {} BUCKETS", self.buckets)
    }
}

impl fmt::Display for HiveTableClause<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Comment(comment) => write!(f, "COMMENT {}", comment),
            Self::PartitionedBy(columns) => write!(f, "PARTITIONED BY ({})", comma(columns)),
            Self::ClusteredBy(clustered) => clustered.fmt(f),
            Self::RowFormat(format) => format.fmt(f),
            Self::Stored(storage) => storage.fmt(f),
            Self::Location(location) => write!(f, "LOCATION {}", location),
            Self::TblProperties(options) => write!(f, "TBLPROPERTIES ({})", comma(options)),
        }
    }
}

impl fmt::Display for HiveCreateTable<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let table = &self.table;
        write_create_flags(f, table.or_replace, table.temporary)?;
        f.write_str(if self.external {
            "EXTERNAL TABLE "
        } else {
            "TABLE "
        })?;
        write_if_not_exists(f, table.if_not_exists)?;
        table.name.fmt(f)?;
        if let Some(like) = &self.like {
            write!(f, " LIKE {like}")?;
        }
        if !table.columns.is_empty() || !table.constraints.is_empty() {
            write_columns(f, &table.columns, &table.constraints)?;
        }
        for clause in &self.clauses {
            write!(f, " {clause}")?;
        }
        if let Some(query) = &table.query {
            write!(f, " AS {query}")?;
        }
        Ok(())
    }
}

impl fmt::Display for HiveCreateFunction<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_create_flags(f, self.or_replace, self.temporary)?;
        write!(f, "FUNCTION {} AS {}", self.name, self.class)?;
        if let Some((kind, uri)) = &self.using {
            write!(f, " USING {kind} {}", uri)?;
        }
        Ok(())
    }
}

impl fmt::Display for HiveAnalyze<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ANALYZE TABLE {}", self.table)?;
        if !self.partition.is_empty() {
            write!(f, " PARTITION ({})", comma(&self.partition))?;
        }
        if self.compute_statistics {
            f.write_str(" COMPUTE STATISTICS")?;
        }
        if self.noscan {
            f.write_str(" NOSCAN")?;
        }
        if self.cache_metadata {
            f.write_str(" CACHE METADATA")?;
        }
        if self.for_columns {
            f.write_str(" FOR COLUMNS")?;
            if !self.columns.is_empty() {
                write!(f, " {}", comma(&self.columns))?;
            }
        }
        Ok(())
    }
}

impl fmt::Display for HiveLoadData<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let local = if self.local { "LOCAL " } else { "" };
        let overwrite = if self.overwrite { "OVERWRITE " } else { "" };
        write!(
            f,
            "LOAD DATA {local}INPATH {} {overwrite}INTO TABLE {}",
            self.path, self.table
        )?;
        if !self.partition.is_empty() {
            write!(f, " PARTITION ({})", comma(&self.partition))?;
        }
        if let Some((input, serde)) = &self.format {
            write!(f, " INPUTFORMAT {input} SERDE {serde}")?;
        }
        Ok(())
    }
}

impl fmt::Display for HiveStatement<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CreateConnector(connector) => {
                f.write_str("CREATE CONNECTOR ")?;
                write_if_not_exists(f, connector.if_not_exists)?;
                connector.name.fmt(f)?;
                if let Some(ref connector_type) = connector.connector_type {
                    write!(f, " TYPE {}", connector_type)?;
                }
                if let Some(ref url) = connector.url {
                    write!(f, " URL {}", url)?;
                }
                if let Some(ref comment) = connector.comment {
                    write!(f, " COMMENT {}", comment)?;
                }
                if !connector.properties.is_empty() {
                    f.write_str(" WITH DCPROPERTIES ")?;
                    write_connector_properties(f, &connector.properties)?;
                }
                Ok(())
            }
            Self::DropConnector { if_exists, name } => {
                f.write_str("DROP CONNECTOR ")?;
                if *if_exists {
                    f.write_str("IF EXISTS ")?;
                }
                name.fmt(f)
            }
            Self::AlterConnector { name, action } => {
                write!(f, "ALTER CONNECTOR {name} SET ")?;
                match action {
                    HiveAlterConnectorAction::Properties(properties) => {
                        f.write_str("DCPROPERTIES")?;
                        write_connector_properties(f, properties)
                    }
                    HiveAlterConnectorAction::Url(url) => {
                        write!(f, "URL {}", url)
                    }
                    HiveAlterConnectorAction::Owner(kind, name) => {
                        write!(f, "OWNER {kind} {}", name)
                    }
                }
            }
            Self::CreateTable(create) => create.fmt(f),
            Self::CreateFunction(function) => function.fmt(f),
            Self::WithInsert { with, from, insert } => {
                write!(f, "WITH {}", comma(with))?;
                if let Some(from) = from {
                    write!(f, " FROM {from}")?;
                }
                write!(f, " {insert}")
            }
            Self::Analyze(analyze) => analyze.fmt(f),
            Self::Msck {
                repair,
                table,
                action,
            } => {
                let repair = if *repair { "REPAIR " } else { "" };
                write!(f, "MSCK {repair}TABLE {table}")?;
                if let Some(action) = action {
                    write!(f, " {action} PARTITIONS")?;
                }
                Ok(())
            }
            Self::SetHiveVar { name, values } => {
                write!(f, "SET HIVEVAR:{name} = {}", comma(values))
            }
            Self::LoadData(load) => load.fmt(f),
        }
    }
}

fn write_connector_properties(
    f: &mut fmt::Formatter<'_>,
    properties: &[(StringLiteral<'_>, StringLiteral<'_>)],
) -> fmt::Result {
    f.write_str("(")?;
    for (i, (key, value)) in properties.iter().enumerate() {
        if i > 0 {
            f.write_str(", ")?;
        }
        write!(f, "{} = {}", key, value)?;
    }
    f.write_str(")")
}

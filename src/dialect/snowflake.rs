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

#[cfg(not(feature = "std"))]
use crate::alloc::string::ToString;
use crate::ast::helpers::attached_token::AttachedToken;
use crate::ast::helpers::key_value_options::{
    KeyValueOption, KeyValueOptionKind, KeyValueOptions, KeyValueOptionsDelimiter,
};
use crate::ast::helpers::stmt_create_database::CreateDatabaseBuilder;
use crate::ast::helpers::stmt_create_table::CreateTableBuilder;
use crate::ast::helpers::stmt_data_loading::{
    FileStagingCommand, StageLoadSelectItem, StageLoadSelectItemKind, StageParamsObject,
};
use crate::ast::{
    AlterTable, AlterTableOperation, AlterTableType, CatalogSyncNamespaceMode, ColumnOption,
    ColumnPolicy, ColumnPolicyProperty, ContactEntry, CopyIntoSnowflakeKind, CreateTableLikeKind,
    DollarQuotedString, Ident, IdentityParameters, IdentityProperty, IdentityPropertyFormatKind,
    IdentityPropertyKind, IdentityPropertyOrder, InitializeKind, ObjectName, ObjectNamePart,
    RefreshModeKind, RowAccessPolicy, ShowObjects, SqlOption, Statement,
    StorageSerializationPolicy, TagsColumnOption, Value, WrappedCollection,
};
use crate::dialect::{Dialect, Precedence};
use crate::keywords::Keyword;
use crate::parser::{IsOptional, Parser, ParserError};
use crate::tokenizer::BorrowedToken;
#[cfg(not(feature = "std"))]
use alloc::boxed::Box;
#[cfg(not(feature = "std"))]
use alloc::string::String;
#[cfg(not(feature = "std"))]
use alloc::vec::Vec;
#[cfg(not(feature = "std"))]
use alloc::{format, vec};

use super::keywords::RESERVED_FOR_IDENTIFIER;

const RESERVED_KEYWORDS_FOR_SELECT_ITEM_OPERATOR: [Keyword; 1] = [Keyword::CONNECT_BY_ROOT];

// See: <https://docs.snowflake.com/en/sql-reference/reserved-keywords>
const RESERVED_KEYWORDS_FOR_TABLE_FACTOR: &[Keyword] = &[
    Keyword::ALL,
    Keyword::ALTER,
    Keyword::AND,
    Keyword::ANY,
    Keyword::AS,
    Keyword::BETWEEN,
    Keyword::BY,
    Keyword::CHECK,
    Keyword::COLUMN,
    Keyword::CONNECT,
    Keyword::CREATE,
    Keyword::CROSS,
    Keyword::CURRENT,
    Keyword::DELETE,
    Keyword::DISTINCT,
    Keyword::DROP,
    Keyword::ELSE,
    Keyword::EXISTS,
    Keyword::FOLLOWING,
    Keyword::FOR,
    Keyword::FROM,
    Keyword::FULL,
    Keyword::GRANT,
    Keyword::GROUP,
    Keyword::HAVING,
    Keyword::ILIKE,
    Keyword::IN,
    Keyword::INCREMENT,
    Keyword::INNER,
    Keyword::INSERT,
    Keyword::INTERSECT,
    Keyword::INTO,
    Keyword::IS,
    Keyword::JOIN,
    Keyword::LEFT,
    Keyword::LIKE,
    Keyword::MINUS,
    Keyword::NATURAL,
    Keyword::NOT,
    Keyword::NULL,
    Keyword::OF,
    Keyword::ON,
    Keyword::OR,
    Keyword::ORDER,
    Keyword::QUALIFY,
    Keyword::REGEXP,
    Keyword::REVOKE,
    Keyword::RIGHT,
    Keyword::RLIKE,
    Keyword::ROW,
    Keyword::ROWS,
    Keyword::SAMPLE,
    Keyword::SELECT,
    Keyword::SET,
    Keyword::SOME,
    Keyword::START,
    Keyword::TABLE,
    Keyword::TABLESAMPLE,
    Keyword::THEN,
    Keyword::TO,
    Keyword::TRIGGER,
    Keyword::UNION,
    Keyword::UNIQUE,
    Keyword::UPDATE,
    Keyword::USING,
    Keyword::VALUES,
    Keyword::WHEN,
    Keyword::WHENEVER,
    Keyword::WHERE,
    Keyword::WINDOW,
    Keyword::WITH,
];

/// A [`Dialect`] for [Snowflake](https://www.snowflake.com/)
#[derive(Debug, Default)]
pub struct SnowflakeDialect;

impl Dialect for SnowflakeDialect {
    // see https://docs.snowflake.com/en/sql-reference/identifiers-syntax.html
    fn is_identifier_start(&self, ch: char) -> bool {
        ch.is_ascii_lowercase() || ch.is_ascii_uppercase() || ch == '_'
    }

    fn supports_projection_trailing_commas(&self) -> bool {
        true
    }

    fn supports_from_trailing_commas(&self) -> bool {
        true
    }

    // Snowflake supports double-dot notation when the schema name is not specified
    // In this case the default PUBLIC schema is used
    //
    // see https://docs.snowflake.com/en/sql-reference/name-resolution#resolution-when-schema-omitted-double-dot-notation
    fn supports_object_name_double_dot_notation(&self) -> bool {
        true
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        ch.is_ascii_lowercase()
            || ch.is_ascii_uppercase()
            || ch.is_ascii_digit()
            || ch == '$'
            || ch == '_'
    }

    // See https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#escape_sequences
    fn supports_string_literal_backslash_escape(&self) -> bool {
        true
    }

    fn supports_within_after_array_aggregation(&self) -> bool {
        true
    }

    /// See <https://docs.snowflake.com/en/sql-reference/constructs/where#joins-in-the-where-clause>
    fn supports_outer_join_operator(&self) -> bool {
        true
    }

    fn supports_connect_by(&self) -> bool {
        true
    }

    /// See <https://docs.snowflake.com/en/sql-reference/sql/execute-immediate>
    fn supports_execute_immediate(&self) -> bool {
        true
    }

    fn supports_match_recognize(&self) -> bool {
        true
    }

    // Snowflake uses this syntax for "object constants" (the values of which
    // are not actually required to be constants).
    //
    // https://docs.snowflake.com/en/sql-reference/data-types-semistructured#label-object-constant
    fn supports_dictionary_syntax(&self) -> bool {
        true
    }

    // Snowflake doesn't document this but `FIRST_VALUE(arg, { IGNORE | RESPECT } NULLS)`
    // works (i.e. inside the argument list instead of after).
    fn supports_window_function_null_treatment_arg(&self) -> bool {
        true
    }

    /// See [doc](https://docs.snowflake.com/en/sql-reference/sql/set#syntax)
    fn supports_parenthesized_set_variables(&self) -> bool {
        true
    }

    /// See [doc](https://docs.snowflake.com/en/sql-reference/sql/comment)
    fn supports_comment_on(&self) -> bool {
        true
    }

    fn parse_statement(&self, parser: &Parser) -> Option<Result<Statement, ParserError>> {
        if parser.parse_keyword(Keyword::BEGIN) {
            return Some(parser.parse_begin_exception_end());
        }

        if parser.parse_keywords(&[Keyword::ALTER, Keyword::DYNAMIC, Keyword::TABLE]) {
            // ALTER DYNAMIC TABLE
            return Some(parse_alter_dynamic_table(parser));
        }

        if parser.parse_keywords(&[Keyword::ALTER, Keyword::SESSION]) {
            // ALTER SESSION
            let set = match parser.parse_one_of_keywords(&[Keyword::SET, Keyword::UNSET]) {
                Some(Keyword::SET) => true,
                Some(Keyword::UNSET) => false,
                _ => return Some(parser.expected("SET or UNSET", parser.peek_token())),
            };
            return Some(parse_alter_session(parser, set));
        }

        if parser.parse_keyword(Keyword::CREATE) {
            // possibly CREATE STAGE
            //[ OR  REPLACE ]
            let or_replace = parser.parse_keywords(&[Keyword::OR, Keyword::REPLACE]);
            // LOCAL | GLOBAL
            let global = match parser.parse_one_of_keywords(&[Keyword::LOCAL, Keyword::GLOBAL]) {
                Some(Keyword::LOCAL) => Some(false),
                Some(Keyword::GLOBAL) => Some(true),
                _ => None,
            };

            let dynamic = parser.parse_keyword(Keyword::DYNAMIC);

            let mut temporary = false;
            let mut volatile = false;
            let mut transient = false;
            let mut iceberg = false;

            match parser.parse_one_of_keywords(&[
                Keyword::TEMP,
                Keyword::TEMPORARY,
                Keyword::VOLATILE,
                Keyword::TRANSIENT,
                Keyword::ICEBERG,
            ]) {
                Some(Keyword::TEMP | Keyword::TEMPORARY) => temporary = true,
                Some(Keyword::VOLATILE) => volatile = true,
                Some(Keyword::TRANSIENT) => transient = true,
                Some(Keyword::ICEBERG) => iceberg = true,
                _ => {}
            }

            if parser.parse_keyword(Keyword::STAGE) {
                // OK - this is CREATE STAGE statement
                return Some(parse_create_stage(or_replace, temporary, parser));
            } else if parser.parse_keyword(Keyword::TABLE) {
                return Some(parse_create_table(
                    or_replace, global, temporary, volatile, transient, iceberg, dynamic, parser,
                ));
            } else if parser.parse_keyword(Keyword::DATABASE) {
                return Some(parse_create_database(or_replace, transient, parser));
            } else {
                // need to go back with the cursor
                let mut back = 1;
                if or_replace {
                    back += 2
                }
                if temporary {
                    back += 1
                }
                for _i in 0..back {
                    parser.prev_token();
                }
            }
        }
        if parser.parse_keywords(&[Keyword::COPY, Keyword::INTO]) {
            // COPY INTO
            return Some(parse_copy_into(parser));
        }

        if let Some(kw) = parser.parse_one_of_keywords(&[
            Keyword::LIST,
            Keyword::LS,
            Keyword::REMOVE,
            Keyword::RM,
        ]) {
            return Some(parse_file_staging_command(kw, parser));
        }

        if parser.parse_keyword(Keyword::SHOW) {
            let terse = parser.parse_keyword(Keyword::TERSE);
            if parser.parse_keyword(Keyword::OBJECTS) {
                return Some(parse_show_objects(terse, parser));
            }
            //Give back Keyword::TERSE
            if terse {
                parser.prev_token();
            }
            //Give back Keyword::SHOW
            parser.prev_token();
        }

        None
    }

    fn parse_column_option(
        &self,
        parser: &Parser,
    ) -> Result<Option<Result<Option<ColumnOption>, ParserError>>, ParserError> {
        parser.maybe_parse(|parser| {
            let with = parser.parse_keyword(Keyword::WITH);

            if parser.parse_keyword(Keyword::IDENTITY) {
                Ok(parse_identity_property(parser)
                    .map(|p| Some(ColumnOption::Identity(IdentityPropertyKind::Identity(p)))))
            } else if parser.parse_keyword(Keyword::AUTOINCREMENT) {
                Ok(parse_identity_property(parser).map(|p| {
                    Some(ColumnOption::Identity(IdentityPropertyKind::Autoincrement(
                        p,
                    )))
                }))
            } else if parser.parse_keywords(&[Keyword::MASKING, Keyword::POLICY]) {
                Ok(parse_column_policy_property(parser, with)
                    .map(|p| Some(ColumnOption::Policy(ColumnPolicy::MaskingPolicy(p)))))
            } else if parser.parse_keywords(&[Keyword::PROJECTION, Keyword::POLICY]) {
                Ok(parse_column_policy_property(parser, with)
                    .map(|p| Some(ColumnOption::Policy(ColumnPolicy::ProjectionPolicy(p)))))
            } else if parser.parse_keywords(&[Keyword::TAG]) {
                Ok(parse_column_tags(parser, with).map(|p| Some(ColumnOption::Tags(p))))
            } else {
                Err(ParserError::ParserError("not found match".to_string()))
            }
        })
    }

    fn get_next_precedence(&self, parser: &Parser) -> Option<Result<u8, ParserError>> {
        let token = parser.peek_token();
        // Snowflake supports the `:` cast operator unlike other dialects
        match token.token {
            BorrowedToken::Colon => Some(Ok(self.prec_value(Precedence::DoubleColon))),
            _ => None,
        }
    }

    fn describe_requires_table_keyword(&self) -> bool {
        true
    }

    fn allow_extract_custom(&self) -> bool {
        true
    }

    fn allow_extract_single_quotes(&self) -> bool {
        true
    }

    /// Snowflake expects the `LIKE` option before the `IN` option,
    /// for example: <https://docs.snowflake.com/en/sql-reference/sql/show-views#syntax>
    fn supports_show_like_before_in(&self) -> bool {
        true
    }

    fn supports_left_associative_joins_without_parens(&self) -> bool {
        false
    }

    fn is_reserved_for_identifier(&self, kw: Keyword) -> bool {
        // Unreserve some keywords that Snowflake accepts as identifiers
        // See: https://docs.snowflake.com/en/sql-reference/reserved-keywords
        if matches!(kw, Keyword::INTERVAL) {
            false
        } else {
            RESERVED_FOR_IDENTIFIER.contains(&kw)
        }
    }

    fn supports_partiql(&self) -> bool {
        true
    }

    fn is_column_alias(&self, kw: &Keyword, parser: &Parser) -> bool {
        match kw {
            // The following keywords can be considered an alias as long as 
            // they are not followed by other tokens that may change their meaning
            // e.g. `SELECT * EXCEPT (col1) FROM tbl`
            Keyword::EXCEPT
            // e.g. `INSERT INTO t SELECT 1 RETURNING *`
            | Keyword::RETURNING if !matches!(parser.peek_token_ref().token, BorrowedToken::Comma | BorrowedToken::EOF) =>
            {
                false
            }

            // e.g. `SELECT 1 LIMIT 5` - not an alias
            // e.g. `SELECT 1 OFFSET 5 ROWS` - not an alias
            Keyword::LIMIT | Keyword::OFFSET if peek_for_limit_options(parser) => false,

            // `FETCH` can be considered an alias as long as it's not followed by `FIRST`` or `NEXT`
            // which would give it a different meanings, for example: 
            // `SELECT 1 FETCH FIRST 10 ROWS` - not an alias
            // `SELECT 1 FETCH 10` - not an alias
            Keyword::FETCH if parser.peek_one_of_keywords(&[Keyword::FIRST, Keyword::NEXT]).is_some()
                    || peek_for_limit_options(parser) =>
            {
                false
            }

            // Reserved keywords by the Snowflake dialect, which seem to be less strictive 
            // than what is listed in `keywords::RESERVED_FOR_COLUMN_ALIAS`. The following 
            // keywords were tested with the this statement: `SELECT 1 <KW>`.
            Keyword::FROM
            | Keyword::GROUP
            | Keyword::HAVING
            | Keyword::INTERSECT
            | Keyword::INTO
            | Keyword::MINUS
            | Keyword::ORDER
            | Keyword::SELECT
            | Keyword::UNION
            | Keyword::WHERE
            | Keyword::WITH => false,

            // Any other word is considered an alias
            _ => true,
        }
    }

    fn is_table_alias(&self, kw: &Keyword, parser: &Parser) -> bool {
        match kw {
            // The following keywords can be considered an alias as long as
            // they are not followed by other tokens that may change their meaning
            Keyword::RETURNING
            | Keyword::INNER
            | Keyword::USING
            | Keyword::PIVOT
            | Keyword::UNPIVOT
            | Keyword::EXCEPT
            | Keyword::MATCH_RECOGNIZE
                if !matches!(
                    parser.peek_token_ref().token,
                    BorrowedToken::SemiColon | BorrowedToken::EOF
                ) =>
            {
                false
            }

            // `LIMIT` can be considered an alias as long as it's not followed by a value. For example:
            // `SELECT * FROM tbl LIMIT WHERE 1=1` - alias
            // `SELECT * FROM tbl LIMIT 3` - not an alias
            Keyword::LIMIT | Keyword::OFFSET if peek_for_limit_options(parser) => false,

            // `FETCH` can be considered an alias as long as it's not followed by `FIRST`` or `NEXT`
            // which would give it a different meanings, for example:
            // `SELECT * FROM tbl FETCH FIRST 10 ROWS` - not an alias
            // `SELECT * FROM tbl FETCH 10` - not an alias
            Keyword::FETCH
                if parser
                    .peek_one_of_keywords(&[Keyword::FIRST, Keyword::NEXT])
                    .is_some()
                    || peek_for_limit_options(parser) =>
            {
                false
            }

            // All sorts of join-related keywords can be considered aliases unless additional
            // keywords change their meaning.
            Keyword::RIGHT | Keyword::LEFT | Keyword::SEMI | Keyword::ANTI
                if parser
                    .peek_one_of_keywords(&[Keyword::JOIN, Keyword::OUTER])
                    .is_some() =>
            {
                false
            }

            Keyword::GLOBAL if parser.peek_keyword(Keyword::FULL) => false,

            // Reserved keywords by the Snowflake dialect, which seem to be less strictive
            // than what is listed in `keywords::RESERVED_FOR_TABLE_ALIAS`. The following
            // keywords were tested with the this statement: `SELECT <KW>.* FROM tbl <KW>`.
            Keyword::WITH
            | Keyword::ORDER
            | Keyword::SELECT
            | Keyword::WHERE
            | Keyword::GROUP
            | Keyword::HAVING
            | Keyword::LATERAL
            | Keyword::UNION
            | Keyword::INTERSECT
            | Keyword::MINUS
            | Keyword::ON
            | Keyword::JOIN
            | Keyword::INNER
            | Keyword::CROSS
            | Keyword::FULL
            | Keyword::LEFT
            | Keyword::RIGHT
            | Keyword::NATURAL
            | Keyword::USING
            | Keyword::ASOF
            | Keyword::MATCH_CONDITION
            | Keyword::SET
            | Keyword::QUALIFY
            | Keyword::FOR
            | Keyword::START
            | Keyword::CONNECT
            | Keyword::SAMPLE
            | Keyword::TABLESAMPLE
            | Keyword::FROM => false,

            // Any other word is considered an alias
            _ => true,
        }
    }

    fn is_table_factor(&self, kw: &Keyword, parser: &Parser) -> bool {
        match kw {
            Keyword::LIMIT if peek_for_limit_options(parser) => false,
            // Table function
            Keyword::TABLE if matches!(parser.peek_token_ref().token, BorrowedToken::LParen) => {
                true
            }
            _ => !RESERVED_KEYWORDS_FOR_TABLE_FACTOR.contains(kw),
        }
    }

    /// See: <https://docs.snowflake.com/en/sql-reference/constructs/at-before>
    fn supports_timestamp_versioning(&self) -> bool {
        true
    }

    /// See: <https://docs.snowflake.com/en/sql-reference/constructs/group-by>
    fn supports_group_by_expr(&self) -> bool {
        true
    }

    /// See: <https://docs.snowflake.com/en/sql-reference/constructs/connect-by>
    fn get_reserved_keywords_for_select_item_operator(&self) -> &[Keyword] {
        &RESERVED_KEYWORDS_FOR_SELECT_ITEM_OPERATOR
    }

    fn supports_space_separated_column_options(&self) -> bool {
        true
    }

    fn supports_comma_separated_drop_column_list(&self) -> bool {
        true
    }

    fn is_identifier_generating_function_name(
        &self,
        ident: &Ident,
        name_parts: &[ObjectNamePart],
    ) -> bool {
        ident.quote_style.is_none()
            && ident.value.to_lowercase() == "identifier"
            && !name_parts
                .iter()
                .any(|p| matches!(p, ObjectNamePart::Function(_)))
    }

    // For example: `SELECT IDENTIFIER('alias1').* FROM tbl AS alias1`
    fn supports_select_expr_star(&self) -> bool {
        true
    }

    fn supports_select_wildcard_exclude(&self) -> bool {
        true
    }

    fn supports_semantic_view_table_factor(&self) -> bool {
        true
    }
}

// Peeks ahead to identify tokens that are expected after
// a LIMIT/FETCH keyword.
fn peek_for_limit_options(parser: &Parser) -> bool {
    match &parser.peek_token_ref().token {
        BorrowedToken::Number(_, _) | BorrowedToken::Placeholder(_) => true,
        BorrowedToken::SingleQuotedString(val) if val.is_empty() => true,
        BorrowedToken::DollarQuotedString(DollarQuotedString { value, .. }) if value.is_empty() => {
            true
        }
        BorrowedToken::Word(w) if w.keyword == Keyword::NULL => true,
        _ => false,
    }
}

fn parse_file_staging_command(kw: Keyword, parser: &Parser) -> Result<Statement, ParserError> {
    let stage = parse_snowflake_stage_name(parser)?;
    let pattern = if parser.parse_keyword(Keyword::PATTERN) {
        parser.expect_token(&BorrowedToken::Eq)?;
        Some(parser.parse_literal_string()?)
    } else {
        None
    };

    match kw {
        Keyword::LIST | Keyword::LS => Ok(Statement::List(FileStagingCommand { stage, pattern })),
        Keyword::REMOVE | Keyword::RM => {
            Ok(Statement::Remove(FileStagingCommand { stage, pattern }))
        }
        _ => Err(ParserError::ParserError(
            "unexpected stage command, expecting LIST, LS, REMOVE or RM".to_string(),
        )),
    }
}

/// Parse snowflake alter dynamic table.
/// <https://docs.snowflake.com/en/sql-reference/sql/alter-table>
fn parse_alter_dynamic_table(parser: &Parser) -> Result<Statement, ParserError> {
    // Use parse_object_name(true) to support IDENTIFIER() function
    let table_name = parser.parse_object_name(true)?;

    // Parse the operation (REFRESH, SUSPEND, or RESUME)
    let operation = if parser.parse_keyword(Keyword::REFRESH) {
        AlterTableOperation::Refresh
    } else if parser.parse_keyword(Keyword::SUSPEND) {
        AlterTableOperation::Suspend
    } else if parser.parse_keyword(Keyword::RESUME) {
        AlterTableOperation::Resume
    } else {
        return parser.expected(
            "REFRESH, SUSPEND, or RESUME after ALTER DYNAMIC TABLE",
            parser.peek_token(),
        );
    };

    let end_token = if parser.peek_token_ref().token == BorrowedToken::SemiColon {
        parser.peek_token_ref().clone()
    } else {
        parser.get_current_token().clone()
    };

    Ok(Statement::AlterTable(AlterTable {
        name: table_name,
        if_exists: false,
        only: false,
        operations: vec![operation],
        location: None,
        on_cluster: None,
        table_type: Some(AlterTableType::Dynamic),
        end_token: AttachedToken(end_token.to_static()),
    }))
}

/// Parse snowflake alter session.
/// <https://docs.snowflake.com/en/sql-reference/sql/alter-session>
fn parse_alter_session(parser: &Parser, set: bool) -> Result<Statement, ParserError> {
    let session_options = parse_session_options(parser, set)?;
    Ok(Statement::AlterSession {
        set,
        session_params: KeyValueOptions {
            options: session_options,
            delimiter: KeyValueOptionsDelimiter::Space,
        },
    })
}

/// Parse snowflake create table statement.
/// <https://docs.snowflake.com/en/sql-reference/sql/create-table>
/// <https://docs.snowflake.com/en/sql-reference/sql/create-iceberg-table>
#[allow(clippy::too_many_arguments)]
pub fn parse_create_table(
    or_replace: bool,
    global: Option<bool>,
    temporary: bool,
    volatile: bool,
    transient: bool,
    iceberg: bool,
    dynamic: bool,
    parser: &Parser,
) -> Result<Statement, ParserError> {
    let if_not_exists = parser.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
    let table_name = parser.parse_object_name(false)?;

    let mut builder = CreateTableBuilder::new(table_name)
        .or_replace(or_replace)
        .if_not_exists(if_not_exists)
        .temporary(temporary)
        .transient(transient)
        .volatile(volatile)
        .iceberg(iceberg)
        .global(global)
        .dynamic(dynamic)
        .hive_formats(Some(Default::default()));

    // Snowflake does not enforce order of the parameters in the statement. The parser needs to
    // parse the statement in a loop.
    //
    // "CREATE TABLE x COPY GRANTS (c INT)" and "CREATE TABLE x (c INT) COPY GRANTS" are both
    // accepted by Snowflake

    let mut plain_options = vec![];

    loop {
        let next_token = parser.next_token();
        match &next_token.token {
            BorrowedToken::Word(word) => match word.keyword {
                Keyword::COPY => {
                    parser.expect_keyword_is(Keyword::GRANTS)?;
                    builder = builder.copy_grants(true);
                }
                Keyword::COMMENT => {
                    // Rewind the COMMENT keyword
                    parser.prev_token();
                    if let Some(comment_def) = parser.parse_optional_inline_comment()? {
                        plain_options.push(SqlOption::Comment(comment_def))
                    }
                }
                Keyword::AS => {
                    let query = parser.parse_query()?;
                    builder = builder.query(Some(query));
                }
                Keyword::CLONE => {
                    let clone = parser.parse_object_name(false).ok();
                    builder = builder.clone_clause(clone);
                }
                Keyword::LIKE => {
                    let name = parser.parse_object_name(false)?;
                    builder = builder.like(Some(CreateTableLikeKind::Plain(
                        crate::ast::CreateTableLike {
                            name,
                            defaults: None,
                        },
                    )));
                }
                Keyword::CLUSTER => {
                    parser.expect_keyword_is(Keyword::BY)?;
                    parser.expect_token(&BorrowedToken::LParen)?;
                    let cluster_by = Some(WrappedCollection::Parentheses(
                        parser.parse_comma_separated(|p| p.parse_expr())?,
                    ));
                    parser.expect_token(&BorrowedToken::RParen)?;

                    builder = builder.cluster_by(cluster_by)
                }
                Keyword::ENABLE_SCHEMA_EVOLUTION => {
                    parser.expect_token(&BorrowedToken::Eq)?;
                    builder = builder.enable_schema_evolution(Some(parser.parse_boolean_string()?));
                }
                Keyword::CHANGE_TRACKING => {
                    parser.expect_token(&BorrowedToken::Eq)?;
                    builder = builder.change_tracking(Some(parser.parse_boolean_string()?));
                }
                Keyword::DATA_RETENTION_TIME_IN_DAYS => {
                    parser.expect_token(&BorrowedToken::Eq)?;
                    let data_retention_time_in_days = parser.parse_literal_uint()?;
                    builder =
                        builder.data_retention_time_in_days(Some(data_retention_time_in_days));
                }
                Keyword::MAX_DATA_EXTENSION_TIME_IN_DAYS => {
                    parser.expect_token(&BorrowedToken::Eq)?;
                    let max_data_extension_time_in_days = parser.parse_literal_uint()?;
                    builder = builder
                        .max_data_extension_time_in_days(Some(max_data_extension_time_in_days));
                }
                Keyword::DEFAULT_DDL_COLLATION => {
                    parser.expect_token(&BorrowedToken::Eq)?;
                    let default_ddl_collation = parser.parse_literal_string()?;
                    builder = builder.default_ddl_collation(Some(default_ddl_collation));
                }
                // WITH is optional, we just verify that next token is one of the expected ones and
                // fallback to the default match statement
                Keyword::WITH => {
                    parser.expect_one_of_keywords(&[
                        Keyword::AGGREGATION,
                        Keyword::TAG,
                        Keyword::ROW,
                    ])?;
                    parser.prev_token();
                }
                Keyword::AGGREGATION => {
                    parser.expect_keyword_is(Keyword::POLICY)?;
                    let aggregation_policy = parser.parse_object_name(false)?;
                    builder = builder.with_aggregation_policy(Some(aggregation_policy));
                }
                Keyword::ROW => {
                    parser.expect_keywords(&[Keyword::ACCESS, Keyword::POLICY])?;
                    let policy = parser.parse_object_name(false)?;
                    parser.expect_keyword_is(Keyword::ON)?;
                    parser.expect_token(&BorrowedToken::LParen)?;
                    let columns = parser.parse_comma_separated(|p| p.parse_identifier())?;
                    parser.expect_token(&BorrowedToken::RParen)?;

                    builder =
                        builder.with_row_access_policy(Some(RowAccessPolicy::new(policy, columns)))
                }
                Keyword::TAG => {
                    parser.expect_token(&BorrowedToken::LParen)?;
                    let tags = parser.parse_comma_separated(Parser::parse_tag)?;
                    parser.expect_token(&BorrowedToken::RParen)?;
                    builder = builder.with_tags(Some(tags));
                }
                Keyword::ON if parser.parse_keyword(Keyword::COMMIT) => {
                    let on_commit = Some(parser.parse_create_table_on_commit()?);
                    builder = builder.on_commit(on_commit);
                }
                Keyword::EXTERNAL_VOLUME => {
                    parser.expect_token(&BorrowedToken::Eq)?;
                    builder.external_volume = Some(parser.parse_literal_string()?);
                }
                Keyword::CATALOG => {
                    parser.expect_token(&BorrowedToken::Eq)?;
                    builder.catalog = Some(parser.parse_literal_string()?);
                }
                Keyword::BASE_LOCATION => {
                    parser.expect_token(&BorrowedToken::Eq)?;
                    builder.base_location = Some(parser.parse_literal_string()?);
                }
                Keyword::CATALOG_SYNC => {
                    parser.expect_token(&BorrowedToken::Eq)?;
                    builder.catalog_sync = Some(parser.parse_literal_string()?);
                }
                Keyword::STORAGE_SERIALIZATION_POLICY => {
                    parser.expect_token(&BorrowedToken::Eq)?;

                    builder.storage_serialization_policy =
                        Some(parse_storage_serialization_policy(parser)?);
                }
                Keyword::IF if parser.parse_keywords(&[Keyword::NOT, Keyword::EXISTS]) => {
                    builder = builder.if_not_exists(true);
                }
                Keyword::TARGET_LAG => {
                    parser.expect_token(&BorrowedToken::Eq)?;
                    let target_lag = parser.parse_literal_string()?;
                    builder = builder.target_lag(Some(target_lag));
                }
                Keyword::WAREHOUSE => {
                    parser.expect_token(&BorrowedToken::Eq)?;
                    let warehouse = parser.parse_identifier()?;
                    builder = builder.warehouse(Some(warehouse));
                }
                Keyword::AT | Keyword::BEFORE => {
                    parser.prev_token();
                    let version = parser.maybe_parse_table_version()?;
                    builder = builder.version(version);
                }
                Keyword::REFRESH_MODE => {
                    parser.expect_token(&BorrowedToken::Eq)?;
                    let refresh_mode = match parser.parse_one_of_keywords(&[
                        Keyword::AUTO,
                        Keyword::FULL,
                        Keyword::INCREMENTAL,
                    ]) {
                        Some(Keyword::AUTO) => Some(RefreshModeKind::Auto),
                        Some(Keyword::FULL) => Some(RefreshModeKind::Full),
                        Some(Keyword::INCREMENTAL) => Some(RefreshModeKind::Incremental),
                        _ => return parser.expected("AUTO, FULL or INCREMENTAL", next_token),
                    };
                    builder = builder.refresh_mode(refresh_mode);
                }
                Keyword::INITIALIZE => {
                    parser.expect_token(&BorrowedToken::Eq)?;
                    let initialize = match parser
                        .parse_one_of_keywords(&[Keyword::ON_CREATE, Keyword::ON_SCHEDULE])
                    {
                        Some(Keyword::ON_CREATE) => Some(InitializeKind::OnCreate),
                        Some(Keyword::ON_SCHEDULE) => Some(InitializeKind::OnSchedule),
                        _ => return parser.expected("ON_CREATE or ON_SCHEDULE", next_token),
                    };
                    builder = builder.initialize(initialize);
                }
                Keyword::REQUIRE if parser.parse_keyword(Keyword::USER) => {
                    builder = builder.require_user(true);
                }
                _ => {
                    return parser.expected("end of statement", next_token);
                }
            },
            BorrowedToken::LParen => {
                parser.prev_token();
                let (columns, constraints) = parser.parse_columns()?;
                builder = builder.columns(columns).constraints(constraints);
            }
            BorrowedToken::EOF => {
                break;
            }
            BorrowedToken::SemiColon => {
                parser.prev_token();
                break;
            }
            _ => {
                return parser.expected("end of statement", next_token);
            }
        }
    }
    let table_options = if !plain_options.is_empty() {
        crate::ast::CreateTableOptions::Plain(plain_options)
    } else {
        crate::ast::CreateTableOptions::None
    };

    builder = builder.table_options(table_options);

    if iceberg && builder.base_location.is_none() {
        return Err(ParserError::ParserError(
            "BASE_LOCATION is required for ICEBERG tables".to_string(),
        ));
    }

    Ok(builder.build())
}

/// Parse snowflake create database statement.
/// <https://docs.snowflake.com/en/sql-reference/sql/create-database>
pub fn parse_create_database(
    or_replace: bool,
    transient: bool,
    parser: &Parser,
) -> Result<Statement, ParserError> {
    let if_not_exists = parser.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
    let name = parser.parse_object_name(false)?;

    let mut builder = CreateDatabaseBuilder::new(name)
        .or_replace(or_replace)
        .transient(transient)
        .if_not_exists(if_not_exists);

    loop {
        let next_token = parser.next_token();
        match &next_token.token {
            BorrowedToken::Word(word) => match word.keyword {
                Keyword::CLONE => {
                    builder = builder.clone_clause(Some(parser.parse_object_name(false)?));
                }
                Keyword::DATA_RETENTION_TIME_IN_DAYS => {
                    parser.expect_token(&BorrowedToken::Eq)?;
                    builder =
                        builder.data_retention_time_in_days(Some(parser.parse_literal_uint()?));
                }
                Keyword::MAX_DATA_EXTENSION_TIME_IN_DAYS => {
                    parser.expect_token(&BorrowedToken::Eq)?;
                    builder =
                        builder.max_data_extension_time_in_days(Some(parser.parse_literal_uint()?));
                }
                Keyword::EXTERNAL_VOLUME => {
                    parser.expect_token(&BorrowedToken::Eq)?;
                    builder = builder.external_volume(Some(parser.parse_literal_string()?));
                }
                Keyword::CATALOG => {
                    parser.expect_token(&BorrowedToken::Eq)?;
                    builder = builder.catalog(Some(parser.parse_literal_string()?));
                }
                Keyword::REPLACE_INVALID_CHARACTERS => {
                    parser.expect_token(&BorrowedToken::Eq)?;
                    builder =
                        builder.replace_invalid_characters(Some(parser.parse_boolean_string()?));
                }
                Keyword::DEFAULT_DDL_COLLATION => {
                    parser.expect_token(&BorrowedToken::Eq)?;
                    builder = builder.default_ddl_collation(Some(parser.parse_literal_string()?));
                }
                Keyword::STORAGE_SERIALIZATION_POLICY => {
                    parser.expect_token(&BorrowedToken::Eq)?;
                    let policy = parse_storage_serialization_policy(parser)?;
                    builder = builder.storage_serialization_policy(Some(policy));
                }
                Keyword::COMMENT => {
                    parser.expect_token(&BorrowedToken::Eq)?;
                    builder = builder.comment(Some(parser.parse_literal_string()?));
                }
                Keyword::CATALOG_SYNC => {
                    parser.expect_token(&BorrowedToken::Eq)?;
                    builder = builder.catalog_sync(Some(parser.parse_literal_string()?));
                }
                Keyword::CATALOG_SYNC_NAMESPACE_FLATTEN_DELIMITER => {
                    parser.expect_token(&BorrowedToken::Eq)?;
                    builder = builder.catalog_sync_namespace_flatten_delimiter(Some(
                        parser.parse_literal_string()?,
                    ));
                }
                Keyword::CATALOG_SYNC_NAMESPACE_MODE => {
                    parser.expect_token(&BorrowedToken::Eq)?;
                    let mode =
                        match parser.parse_one_of_keywords(&[Keyword::NEST, Keyword::FLATTEN]) {
                            Some(Keyword::NEST) => CatalogSyncNamespaceMode::Nest,
                            Some(Keyword::FLATTEN) => CatalogSyncNamespaceMode::Flatten,
                            _ => {
                                return parser.expected("NEST or FLATTEN", next_token);
                            }
                        };
                    builder = builder.catalog_sync_namespace_mode(Some(mode));
                }
                Keyword::WITH => {
                    if parser.parse_keyword(Keyword::TAG) {
                        parser.expect_token(&BorrowedToken::LParen)?;
                        let tags = parser.parse_comma_separated(Parser::parse_tag)?;
                        parser.expect_token(&BorrowedToken::RParen)?;
                        builder = builder.with_tags(Some(tags));
                    } else if parser.parse_keyword(Keyword::CONTACT) {
                        parser.expect_token(&BorrowedToken::LParen)?;
                        let contacts = parser.parse_comma_separated(|p| {
                            let purpose = p.parse_identifier()?.value;
                            p.expect_token(&BorrowedToken::Eq)?;
                            let contact = p.parse_identifier()?.value;
                            Ok(ContactEntry { purpose, contact })
                        })?;
                        parser.expect_token(&BorrowedToken::RParen)?;
                        builder = builder.with_contacts(Some(contacts));
                    } else {
                        return parser.expected("TAG or CONTACT", next_token);
                    }
                }
                _ => return parser.expected("end of statement", next_token),
            },
            BorrowedToken::SemiColon | BorrowedToken::EOF => break,
            _ => return parser.expected("end of statement", next_token),
        }
    }
    Ok(builder.build())
}

pub fn parse_storage_serialization_policy(
    parser: &Parser,
) -> Result<StorageSerializationPolicy, ParserError> {
    let next_token = parser.next_token();
    match &next_token.token {
        BorrowedToken::Word(w) => match w.keyword {
            Keyword::COMPATIBLE => Ok(StorageSerializationPolicy::Compatible),
            Keyword::OPTIMIZED => Ok(StorageSerializationPolicy::Optimized),
            _ => parser.expected("storage_serialization_policy", next_token),
        },
        _ => parser.expected("storage_serialization_policy", next_token),
    }
}

pub fn parse_create_stage(
    or_replace: bool,
    temporary: bool,
    parser: &Parser,
) -> Result<Statement, ParserError> {
    //[ IF NOT EXISTS ]
    let if_not_exists = parser.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
    let name = parser.parse_object_name(false)?;
    let mut directory_table_params = Vec::new();
    let mut file_format = Vec::new();
    let mut copy_options = Vec::new();
    let mut comment = None;

    // [ internalStageParams | externalStageParams ]
    let stage_params = parse_stage_params(parser)?;

    // [ directoryTableParams ]
    if parser.parse_keyword(Keyword::DIRECTORY) {
        parser.expect_token(&BorrowedToken::Eq)?;
        directory_table_params = parser.parse_key_value_options(true, &[])?.options;
    }

    // [ file_format]
    if parser.parse_keyword(Keyword::FILE_FORMAT) {
        parser.expect_token(&BorrowedToken::Eq)?;
        file_format = parser.parse_key_value_options(true, &[])?.options;
    }

    // [ copy_options ]
    if parser.parse_keyword(Keyword::COPY_OPTIONS) {
        parser.expect_token(&BorrowedToken::Eq)?;
        copy_options = parser.parse_key_value_options(true, &[])?.options;
    }

    // [ comment ]
    if parser.parse_keyword(Keyword::COMMENT) {
        parser.expect_token(&BorrowedToken::Eq)?;
        comment = Some(parser.parse_comment_value()?);
    }

    Ok(Statement::CreateStage {
        or_replace,
        temporary,
        if_not_exists,
        name,
        stage_params,
        directory_table_params: KeyValueOptions {
            options: directory_table_params,
            delimiter: KeyValueOptionsDelimiter::Space,
        },
        file_format: KeyValueOptions {
            options: file_format,
            delimiter: KeyValueOptionsDelimiter::Space,
        },
        copy_options: KeyValueOptions {
            options: copy_options,
            delimiter: KeyValueOptionsDelimiter::Space,
        },
        comment,
    })
}

pub fn parse_stage_name_identifier(parser: &Parser) -> Result<Ident, ParserError> {
    let mut ident = String::new();
    while let Some(next_token) = parser.next_token_no_skip() {
        match &next_token.token {
            BorrowedToken::Whitespace(_) | BorrowedToken::SemiColon => break,
            BorrowedToken::Period => {
                parser.prev_token();
                break;
            }
            BorrowedToken::RParen => {
                parser.prev_token();
                break;
            }
            BorrowedToken::AtSign => ident.push('@'),
            BorrowedToken::Tilde => ident.push('~'),
            BorrowedToken::Mod => ident.push('%'),
            BorrowedToken::Div => ident.push('/'),
            BorrowedToken::Plus => ident.push('+'),
            BorrowedToken::Minus => ident.push('-'),
            BorrowedToken::Number(n, _) => ident.push_str(n),
            BorrowedToken::Word(w) => ident.push_str(&w.to_string()),
            _ => return parser.expected("stage name identifier", parser.peek_token()),
        }
    }
    Ok(Ident::new(ident))
}

pub fn parse_snowflake_stage_name(parser: &Parser) -> Result<ObjectName, ParserError> {
    match parser.next_token().token {
        BorrowedToken::AtSign => {
            parser.prev_token();
            let mut idents = vec![];
            loop {
                idents.push(parse_stage_name_identifier(parser)?);
                if !parser.consume_token(&BorrowedToken::Period) {
                    break;
                }
            }
            Ok(ObjectName::from(idents))
        }
        _ => {
            parser.prev_token();
            Ok(parser.parse_object_name(false)?)
        }
    }
}

/// Parses a `COPY INTO` statement. Snowflake has two variants, `COPY INTO <table>`
/// and `COPY INTO <location>` which have different syntax.
pub fn parse_copy_into(parser: &Parser) -> Result<Statement, ParserError> {
    let kind = match parser.peek_token().token {
        // Indicates an internal stage
        BorrowedToken::AtSign => CopyIntoSnowflakeKind::Location,
        // Indicates an external stage, i.e. s3://, gcs:// or azure://
        BorrowedToken::SingleQuotedString(s) if s.contains("://") => {
            CopyIntoSnowflakeKind::Location
        }
        _ => CopyIntoSnowflakeKind::Table,
    };

    let mut files: Vec<String> = vec![];
    let mut from_transformations: Option<Vec<StageLoadSelectItemKind>> = None;
    let mut from_stage_alias = None;
    let mut from_stage = None;
    let mut stage_params = StageParamsObject {
        url: None,
        encryption: KeyValueOptions {
            options: vec![],
            delimiter: KeyValueOptionsDelimiter::Space,
        },
        endpoint: None,
        storage_integration: None,
        credentials: KeyValueOptions {
            options: vec![],
            delimiter: KeyValueOptionsDelimiter::Space,
        },
    };
    let mut from_query = None;
    let mut partition = None;
    let mut file_format = Vec::new();
    let mut pattern = None;
    let mut validation_mode = None;
    let mut copy_options = Vec::new();

    let into: ObjectName = parse_snowflake_stage_name(parser)?;
    if kind == CopyIntoSnowflakeKind::Location {
        stage_params = parse_stage_params(parser)?;
    }

    let into_columns = match &parser.peek_token().token {
        BorrowedToken::LParen => {
            Some(parser.parse_parenthesized_column_list(IsOptional::Optional, true)?)
        }
        _ => None,
    };

    parser.expect_keyword_is(Keyword::FROM)?;
    match parser.next_token().token {
        BorrowedToken::LParen if kind == CopyIntoSnowflakeKind::Table => {
            // Data load with transformations
            parser.expect_keyword_is(Keyword::SELECT)?;
            from_transformations = parse_select_items_for_data_load(parser)?;

            parser.expect_keyword_is(Keyword::FROM)?;
            from_stage = Some(parse_snowflake_stage_name(parser)?);
            stage_params = parse_stage_params(parser)?;

            // Parse an optional alias
            from_stage_alias = parser
                .maybe_parse_table_alias()?
                .map(|table_alias| table_alias.name);
            parser.expect_token(&BorrowedToken::RParen)?;
        }
        BorrowedToken::LParen if kind == CopyIntoSnowflakeKind::Location => {
            // Data unload with a query
            from_query = Some(parser.parse_query()?);
            parser.expect_token(&BorrowedToken::RParen)?;
        }
        _ => {
            parser.prev_token();
            from_stage = Some(parse_snowflake_stage_name(parser)?);
            stage_params = parse_stage_params(parser)?;

            // as
            from_stage_alias = if parser.parse_keyword(Keyword::AS) {
                Some(match parser.next_token().token {
                    BorrowedToken::Word(w) => Ok(Ident::new(w.value)),
                    _ => parser.expected("stage alias", parser.peek_token()),
                }?)
            } else {
                None
            };
        }
    }

    loop {
        // FILE_FORMAT
        if parser.parse_keyword(Keyword::FILE_FORMAT) {
            parser.expect_token(&BorrowedToken::Eq)?;
            file_format = parser.parse_key_value_options(true, &[])?.options;
        // PARTITION BY
        } else if parser.parse_keywords(&[Keyword::PARTITION, Keyword::BY]) {
            partition = Some(Box::new(parser.parse_expr()?))
        // FILES
        } else if parser.parse_keyword(Keyword::FILES) {
            parser.expect_token(&BorrowedToken::Eq)?;
            parser.expect_token(&BorrowedToken::LParen)?;
            let mut continue_loop = true;
            while continue_loop {
                continue_loop = false;
                let next_token = parser.next_token();
                match next_token.token {
                    BorrowedToken::SingleQuotedString(s) => files.push(s.into_owned()),
                    _ => parser.expected("file token", next_token)?,
                };
                if parser.next_token().token.eq(&BorrowedToken::Comma) {
                    continue_loop = true;
                } else {
                    parser.prev_token(); // not a comma, need to go back
                }
            }
            parser.expect_token(&BorrowedToken::RParen)?;
        // PATTERN
        } else if parser.parse_keyword(Keyword::PATTERN) {
            parser.expect_token(&BorrowedToken::Eq)?;
            let next_token = parser.next_token();
            pattern = Some(match next_token.token {
                BorrowedToken::SingleQuotedString(s) => s.into_owned(),
                _ => parser.expected("pattern", next_token)?,
            });
        // VALIDATION MODE
        } else if parser.parse_keyword(Keyword::VALIDATION_MODE) {
            parser.expect_token(&BorrowedToken::Eq)?;
            validation_mode = Some(parser.next_token().token.to_string());
        // COPY OPTIONS
        } else if parser.parse_keyword(Keyword::COPY_OPTIONS) {
            parser.expect_token(&BorrowedToken::Eq)?;
            copy_options = parser.parse_key_value_options(true, &[])?.options;
        } else {
            match parser.next_token().token {
                BorrowedToken::SemiColon | BorrowedToken::EOF => break,
                BorrowedToken::Comma => continue,
                // In `COPY INTO <location>` the copy options do not have a shared key
                // like in `COPY INTO <table>`
                BorrowedToken::Word(key) => copy_options.push(parser.parse_key_value_option(&key)?),
                _ => return parser.expected("another copy option, ; or EOF'", parser.peek_token()),
            }
        }
    }

    Ok(Statement::CopyIntoSnowflake {
        kind,
        into,
        into_columns,
        from_obj: from_stage,
        from_obj_alias: from_stage_alias,
        stage_params,
        from_transformations,
        from_query,
        files: if files.is_empty() { None } else { Some(files) },
        pattern,
        file_format: KeyValueOptions {
            options: file_format,
            delimiter: KeyValueOptionsDelimiter::Space,
        },
        copy_options: KeyValueOptions {
            options: copy_options,
            delimiter: KeyValueOptionsDelimiter::Space,
        },
        validation_mode,
        partition,
    })
}

fn parse_select_items_for_data_load(
    parser: &Parser,
) -> Result<Option<Vec<StageLoadSelectItemKind>>, ParserError> {
    let mut select_items: Vec<StageLoadSelectItemKind> = vec![];
    loop {
        match parser.maybe_parse(parse_select_item_for_data_load)? {
            // [<alias>.]$<file_col_num>[.<element>] [ , [<alias>.]$<file_col_num>[.<element>] ... ]
            Some(item) => select_items.push(StageLoadSelectItemKind::StageLoadSelectItem(item)),
            // Fallback, try to parse a standard SQL select item
            None => select_items.push(StageLoadSelectItemKind::SelectItem(
                parser.parse_select_item()?,
            )),
        }
        if matches!(parser.peek_token_ref().token, BorrowedToken::Comma) {
            parser.advance_token();
        } else {
            break;
        }
    }
    Ok(Some(select_items))
}

fn parse_select_item_for_data_load(parser: &Parser) -> Result<StageLoadSelectItem, ParserError> {
    let mut alias: Option<Ident> = None;
    let mut file_col_num: i32 = 0;
    let mut element: Option<Ident> = None;
    let mut item_as: Option<Ident> = None;

    let next_token = parser.next_token();
    match next_token.token {
        BorrowedToken::Placeholder(w) => {
            file_col_num = w.to_string().split_off(1).parse::<i32>().map_err(|e| {
                ParserError::ParserError(format!("Could not parse '{w}' as i32: {e}"))
            })?;
            Ok(())
        }
        BorrowedToken::Word(w) => {
            alias = Some(Ident::new(w.value));
            Ok(())
        }
        _ => parser.expected("alias or file_col_num", next_token),
    }?;

    if alias.is_some() {
        parser.expect_token(&BorrowedToken::Period)?;
        // now we get col_num token
        let col_num_token = parser.next_token();
        match col_num_token.token {
            BorrowedToken::Placeholder(w) => {
                file_col_num = w.to_string().split_off(1).parse::<i32>().map_err(|e| {
                    ParserError::ParserError(format!("Could not parse '{w}' as i32: {e}"))
                })?;
                Ok(())
            }
            _ => parser.expected("file_col_num", col_num_token),
        }?;
    }

    // try extracting optional element
    match parser.next_token().token {
        BorrowedToken::Colon => {
            // parse element
            element = Some(Ident::new(match parser.next_token().token {
                BorrowedToken::Word(w) => Ok(w.value),
                _ => parser.expected("file_col_num", parser.peek_token()),
            }?));
        }
        _ => {
            // element not present move back
            parser.prev_token();
        }
    }

    // as
    if parser.parse_keyword(Keyword::AS) {
        item_as = Some(match parser.next_token().token {
            BorrowedToken::Word(w) => Ok(Ident::new(w.value)),
            _ => parser.expected("column item alias", parser.peek_token()),
        }?);
    }

    Ok(StageLoadSelectItem {
        alias,
        file_col_num,
        element,
        item_as,
    })
}

fn parse_stage_params(parser: &Parser) -> Result<StageParamsObject, ParserError> {
    let (mut url, mut storage_integration, mut endpoint) = (None, None, None);
    let mut encryption: KeyValueOptions = KeyValueOptions {
        options: vec![],
        delimiter: KeyValueOptionsDelimiter::Space,
    };
    let mut credentials: KeyValueOptions = KeyValueOptions {
        options: vec![],
        delimiter: KeyValueOptionsDelimiter::Space,
    };

    // URL
    if parser.parse_keyword(Keyword::URL) {
        parser.expect_token(&BorrowedToken::Eq)?;
        url = Some(match parser.next_token().token {
            BorrowedToken::SingleQuotedString(word) => Ok(word.into_owned()),
            _ => parser.expected("a URL statement", parser.peek_token()),
        }?)
    }

    // STORAGE INTEGRATION
    if parser.parse_keyword(Keyword::STORAGE_INTEGRATION) {
        parser.expect_token(&BorrowedToken::Eq)?;
        storage_integration = Some(parser.next_token().token.to_string());
    }

    // ENDPOINT
    if parser.parse_keyword(Keyword::ENDPOINT) {
        parser.expect_token(&BorrowedToken::Eq)?;
        endpoint = Some(match parser.next_token().token {
            BorrowedToken::SingleQuotedString(word) => Ok(word.into_owned()),
            _ => parser.expected("an endpoint statement", parser.peek_token()),
        }?)
    }

    // CREDENTIALS
    if parser.parse_keyword(Keyword::CREDENTIALS) {
        parser.expect_token(&BorrowedToken::Eq)?;
        credentials = KeyValueOptions {
            options: parser.parse_key_value_options(true, &[])?.options,
            delimiter: KeyValueOptionsDelimiter::Space,
        };
    }

    // ENCRYPTION
    if parser.parse_keyword(Keyword::ENCRYPTION) {
        parser.expect_token(&BorrowedToken::Eq)?;
        encryption = KeyValueOptions {
            options: parser.parse_key_value_options(true, &[])?.options,
            delimiter: KeyValueOptionsDelimiter::Space,
        };
    }

    Ok(StageParamsObject {
        url,
        encryption,
        endpoint,
        storage_integration,
        credentials,
    })
}

/// Parses options separated by blank spaces, commas, or new lines like:
/// ABORT_DETACHED_QUERY = { TRUE | FALSE }
///      [ ACTIVE_PYTHON_PROFILER = { 'LINE' | 'MEMORY' } ]
///      [ BINARY_INPUT_FORMAT = '\<string\>' ]
fn parse_session_options(parser: &Parser, set: bool) -> Result<Vec<KeyValueOption>, ParserError> {
    let mut options: Vec<KeyValueOption> = Vec::new();
    let empty = String::new;
    loop {
        let next_token = parser.peek_token();
        match next_token.token {
            BorrowedToken::SemiColon | BorrowedToken::EOF => break,
            BorrowedToken::Comma => {
                parser.advance_token();
                continue;
            }
            BorrowedToken::Word(key) => {
                parser.advance_token();
                if set {
                    let option = parser.parse_key_value_option(&key)?;
                    options.push(option);
                } else {
                    options.push(KeyValueOption {
                        option_name: key.value.to_string(),
                        option_value: KeyValueOptionKind::Single(Value::Placeholder(empty())),
                    });
                }
            }
            _ => {
                return parser.expected("another option or end of statement", next_token);
            }
        }
    }
    if options.is_empty() {
        Err(ParserError::ParserError(
            "expected at least one option".to_string(),
        ))
    } else {
        Ok(options)
    }
}

/// Parsing a property of identity or autoincrement column option
/// Syntax:
/// ```sql
/// [ (seed , increment) | START num INCREMENT num ] [ ORDER | NOORDER ]
/// ```
/// [Snowflake]: https://docs.snowflake.com/en/sql-reference/sql/create-table
fn parse_identity_property(parser: &Parser) -> Result<IdentityProperty, ParserError> {
    let parameters = if parser.consume_token(&BorrowedToken::LParen) {
        let seed = parser.parse_number()?;
        parser.expect_token(&BorrowedToken::Comma)?;
        let increment = parser.parse_number()?;
        parser.expect_token(&BorrowedToken::RParen)?;

        Some(IdentityPropertyFormatKind::FunctionCall(
            IdentityParameters { seed, increment },
        ))
    } else if parser.parse_keyword(Keyword::START) {
        let seed = parser.parse_number()?;
        parser.expect_keyword_is(Keyword::INCREMENT)?;
        let increment = parser.parse_number()?;

        Some(IdentityPropertyFormatKind::StartAndIncrement(
            IdentityParameters { seed, increment },
        ))
    } else {
        None
    };
    let order = match parser.parse_one_of_keywords(&[Keyword::ORDER, Keyword::NOORDER]) {
        Some(Keyword::ORDER) => Some(IdentityPropertyOrder::Order),
        Some(Keyword::NOORDER) => Some(IdentityPropertyOrder::NoOrder),
        _ => None,
    };
    Ok(IdentityProperty { parameters, order })
}

/// Parsing a policy property of column option
/// Syntax:
/// ```sql
/// <policy_name> [ USING ( <col_name> , <cond_col1> , ... )
/// ```
/// [Snowflake]: https://docs.snowflake.com/en/sql-reference/sql/create-table
fn parse_column_policy_property(
    parser: &Parser,
    with: bool,
) -> Result<ColumnPolicyProperty, ParserError> {
    let policy_name = parser.parse_object_name(false)?;
    let using_columns = if parser.parse_keyword(Keyword::USING) {
        parser.expect_token(&BorrowedToken::LParen)?;
        let columns = parser.parse_comma_separated(|p| p.parse_identifier())?;
        parser.expect_token(&BorrowedToken::RParen)?;
        Some(columns)
    } else {
        None
    };

    Ok(ColumnPolicyProperty {
        with,
        policy_name,
        using_columns,
    })
}

/// Parsing tags list of column
/// Syntax:
/// ```sql
/// ( <tag_name> = '<tag_value>' [ , <tag_name> = '<tag_value>' , ... ] )
/// ```
/// [Snowflake]: https://docs.snowflake.com/en/sql-reference/sql/create-table
fn parse_column_tags(parser: &Parser, with: bool) -> Result<TagsColumnOption, ParserError> {
    parser.expect_token(&BorrowedToken::LParen)?;
    let tags = parser.parse_comma_separated(Parser::parse_tag)?;
    parser.expect_token(&BorrowedToken::RParen)?;

    Ok(TagsColumnOption { with, tags })
}

/// Parse snowflake show objects.
/// <https://docs.snowflake.com/en/sql-reference/sql/show-objects>
fn parse_show_objects(terse: bool, parser: &Parser) -> Result<Statement, ParserError> {
    let show_options = parser.parse_show_stmt_options()?;
    Ok(Statement::ShowObjects(ShowObjects {
        terse,
        show_options,
    }))
}

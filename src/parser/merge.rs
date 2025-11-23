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

//! SQL Parser for MERGE

#[cfg(not(feature = "std"))]
use alloc::{boxed::Box, format, string::ToString, vec, vec::Vec};

use crate::{
    ast::{
        Ident, Merge, MergeAction, MergeClause, MergeClauseKind, MergeInsertExpr, MergeInsertKind,
        MergeUpdateExpr, ObjectName, ObjectNamePart, OutputClause, SetExpr, Spanned, Statement,
        TableFactor,
    },
    dialect::{BigQueryDialect, GenericDialect, MySqlDialect},
    keywords::Keyword,
    parser::IsOptional,
    tokenizer::{Location, TokenWithSpan},
};

use super::{Parser, ParserError};

impl Parser<'_> {
    /// Parse a MERGE statement, returning a `Box`ed SetExpr
    ///
    /// This is used to reduce the size of the stack frames in debug builds
    pub(super) fn parse_merge_setexpr_boxed(
        &mut self,
        merge_token: TokenWithSpan,
    ) -> Result<Box<SetExpr>, ParserError> {
        Ok(Box::new(SetExpr::Merge(self.parse_merge(merge_token)?)))
    }

    pub fn parse_merge(&mut self, merge_token: TokenWithSpan) -> Result<Statement, ParserError> {
        let into = self.parse_keyword(Keyword::INTO);

        let table = self.parse_table_factor()?;

        self.expect_keyword_is(Keyword::USING)?;
        let source = self.parse_table_factor()?;
        self.expect_keyword_is(Keyword::ON)?;
        let on = self.parse_expr()?;
        let clauses = self.parse_merge_clauses(&table)?;
        let output = match self.parse_one_of_keywords(&[Keyword::OUTPUT, Keyword::RETURNING]) {
            Some(keyword) => Some(self.parse_output(keyword, self.get_current_token().clone())?),
            None => None,
        };

        Ok(Statement::Merge(Merge {
            merge_token: merge_token.into(),
            into,
            table,
            source,
            on: Box::new(on),
            clauses,
            output,
        }))
    }

    fn parse_merge_clauses(
        &mut self,
        target_table: &TableFactor,
    ) -> Result<Vec<MergeClause>, ParserError> {
        let mut clauses = vec![];
        loop {
            if !(self.parse_keyword(Keyword::WHEN)) {
                break;
            }
            let when_token = self.get_current_token().clone();

            let mut clause_kind = MergeClauseKind::Matched;
            if self.parse_keyword(Keyword::NOT) {
                clause_kind = MergeClauseKind::NotMatched;
            }
            self.expect_keyword_is(Keyword::MATCHED)?;

            if matches!(clause_kind, MergeClauseKind::NotMatched)
                && self.parse_keywords(&[Keyword::BY, Keyword::SOURCE])
            {
                clause_kind = MergeClauseKind::NotMatchedBySource;
            } else if matches!(clause_kind, MergeClauseKind::NotMatched)
                && self.parse_keywords(&[Keyword::BY, Keyword::TARGET])
            {
                clause_kind = MergeClauseKind::NotMatchedByTarget;
            }

            let predicate = if self.parse_keyword(Keyword::AND) {
                Some(self.parse_expr()?)
            } else {
                None
            };

            self.expect_keyword_is(Keyword::THEN)?;

            let merge_clause = match self.parse_one_of_keywords(&[
                Keyword::UPDATE,
                Keyword::INSERT,
                Keyword::DELETE,
            ]) {
                Some(Keyword::UPDATE) => {
                    if matches!(
                        clause_kind,
                        MergeClauseKind::NotMatched | MergeClauseKind::NotMatchedByTarget
                    ) {
                        return parser_err!(
                            format_args!("UPDATE is not allowed in a {clause_kind} merge clause"),
                            self.get_current_token().span.start
                        );
                    }

                    let update_token = self.get_current_token().clone();
                    self.expect_keyword_is(Keyword::SET)?;
                    let assignments = self.parse_comma_separated(Parser::parse_assignment)?;
                    let update_predicate = if self.dialect.supports_merge_update_predicate()
                        && self.parse_keyword(Keyword::WHERE)
                    {
                        Some(self.parse_expr()?)
                    } else {
                        None
                    };
                    let delete_predicate = if self.dialect.supports_merge_update_delete_predicate()
                        && self.parse_keyword(Keyword::DELETE)
                    {
                        let _ = self.expect_keyword(Keyword::WHERE)?;
                        Some(self.parse_expr()?)
                    } else {
                        None
                    };
                    MergeAction::Update(MergeUpdateExpr {
                        update_token: update_token.into(),
                        assignments,
                        update_predicate,
                        delete_predicate,
                    })
                }
                Some(Keyword::DELETE) => {
                    if matches!(
                        clause_kind,
                        MergeClauseKind::NotMatched | MergeClauseKind::NotMatchedByTarget
                    ) {
                        return parser_err!(
                            format_args!("DELETE is not allowed in a {clause_kind} merge clause"),
                            self.get_current_token().span.start
                        );
                    };

                    let delete_token = self.get_current_token().clone();
                    MergeAction::Delete {
                        delete_token: delete_token.into(),
                    }
                }
                Some(Keyword::INSERT) => {
                    if !matches!(
                        clause_kind,
                        MergeClauseKind::NotMatched | MergeClauseKind::NotMatchedByTarget
                    ) {
                        return parser_err!(
                            format_args!("INSERT is not allowed in a {clause_kind} merge clause"),
                            self.get_current_token().span.start
                        );
                    };

                    let insert_token = self.get_current_token().clone();
                    let is_mysql = dialect_of!(self is MySqlDialect);

                    let columns = self.parse_merge_clause_insert_columns(
                        target_table,
                        &clause_kind,
                        is_mysql,
                    )?;
                    let (kind, kind_token) = if dialect_of!(self is BigQueryDialect | GenericDialect)
                        && self.parse_keyword(Keyword::ROW)
                    {
                        (MergeInsertKind::Row, self.get_current_token().clone())
                    } else {
                        self.expect_keyword_is(Keyword::VALUES)?;
                        let values_token = self.get_current_token().clone();
                        let values = self.parse_values(is_mysql, false)?;
                        (MergeInsertKind::Values(values), values_token)
                    };
                    let insert_predicate = if self.dialect.supports_merge_insert_predicate()
                        && self.parse_keyword(Keyword::WHERE)
                    {
                        Some(self.parse_expr()?)
                    } else {
                        None
                    };

                    MergeAction::Insert(MergeInsertExpr {
                        insert_token: insert_token.into(),
                        columns,
                        kind_token: kind_token.into(),
                        kind,
                        insert_predicate,
                    })
                }
                _ => {
                    return parser_err!(
                        "expected UPDATE, DELETE or INSERT in merge clause",
                        self.peek_token_ref().span.start
                    );
                }
            };
            clauses.push(MergeClause {
                when_token: when_token.into(),
                clause_kind,
                predicate,
                action: merge_clause,
            });
        }
        Ok(clauses)
    }

    fn parse_merge_clause_insert_columns(
        &mut self,
        target_table: &TableFactor,
        clause_kind: &MergeClauseKind,
        allow_empty: bool,
    ) -> Result<Vec<Ident>, ParserError> {
        if self.dialect.supports_merge_insert_qualified_columns() {
            let cols =
                self.parse_parenthesized_qualified_column_list(IsOptional::Optional, allow_empty)?;
            if let TableFactor::Table { name, alias, .. } = target_table {
                if let Some(alias) = alias {
                    if alias.columns.is_empty() {
                        // ~ only the alias is supported at this point
                        match unqualify_columns(cols, None, Some(&alias.name)) {
                            Ok(column) => Ok(column),
                            Err((err, loc)) => parser_err!(
                                format_args!("Invalid column for INSERT in a {clause_kind} merge clause: {err}"),
                                loc
                            ),
                        }
                    } else {
                        parser_err!(
                            format_args!("Invalid target ALIAS for INSERT in a {clause_kind} merge clause; must be an identifier"),
                            alias.name.span.start
                        )
                    }
                } else {
                    // ~ allow the full qualifier, but also just the table name
                    if name.0.len() == 1 {
                        match unqualify_columns(cols, Some(name), None) {
                            Ok(column) => Ok(column),
                            Err((err, loc)) => parser_err!(
                                format_args!("Invalid column for INSERT in a {clause_kind} merge clause: {err}"),
                                loc)
                        }
                    } else if let Some(unqualified_name) =
                        name.0.last().and_then(ObjectNamePart::as_ident)
                    {
                        match unqualify_columns(cols, Some(name), Some(unqualified_name)) {
                            Ok(column) => Ok(column),
                            Err((err, loc)) => parser_err!(
                                format_args!("Invalid column for INSERT in a {clause_kind} merge clause: {err}"),
                                loc)
                        }
                    } else {
                        parser_err!(
                            format_args!("Invalid target table NAME for INSERT in a {clause_kind} merge clause; must be an identifier"),
                            name.span().start
                        )
                    }
                }
            } else {
                parser_err!(
                    format_args!("Invalid target for INSERT in a {clause_kind} merge clause; must be a TABLE identifier"),
                    target_table.span().start)
            }
        } else {
            self.parse_parenthesized_column_list(IsOptional::Optional, allow_empty)
        }
    }

    fn parse_output(
        &mut self,
        start_keyword: Keyword,
        start_token: TokenWithSpan,
    ) -> Result<OutputClause, ParserError> {
        let select_items = self.parse_projection()?;
        let into_table = if start_keyword == Keyword::OUTPUT && self.peek_keyword(Keyword::INTO) {
            self.expect_keyword_is(Keyword::INTO)?;
            Some(self.parse_select_into()?)
        } else {
            None
        };

        Ok(if start_keyword == Keyword::OUTPUT {
            OutputClause::Output {
                output_token: start_token.into(),
                select_items,
                into_table,
            }
        } else {
            OutputClause::Returning {
                returning_token: start_token.into(),
                select_items,
            }
        })
    }
}

/// Helper to unqualify a list of columns with either a qualified prefix
/// (`allowed_qualifier_1`) or a qualifier identifier (`allowed_qualifier_2`.)
///
/// Oracle allows `INSERT ([qualifier.]column_name, ...)` in MERGE statements
/// with `qualifier` referring to the alias of the target table (if one is
/// present) or, if no alias is present, to the target table name itself -
/// either qualified or unqualified.
fn unqualify_columns(
    columns: Vec<ObjectName>,
    allowed_qualifier_1: Option<&ObjectName>,
    allowed_qualifier_2: Option<&Ident>,
) -> Result<Vec<Ident>, (&'static str, Location)> {
    // ~ helper to turn a column name (part) into a plain `ident`
    // possibly bailing with error
    fn to_ident(name: ObjectNamePart) -> Result<Ident, (&'static str, Location)> {
        match name {
            ObjectNamePart::Identifier(ident) => Ok(ident),
            ObjectNamePart::Function(_) => Err(("not an identifier", name.span().start)),
        }
    }

    // ~ helper to return the last part of `name` if it is
    // preceded by `prefix`
    fn unqualify_column(
        mut name: ObjectName,
        prefix: &ObjectName,
    ) -> Result<ObjectNamePart, ObjectName> {
        let mut name_iter = name.0.iter();
        let mut prefix_iter = prefix.0.iter();
        loop {
            match (name_iter.next(), prefix_iter.next()) {
                (Some(_), None) => {
                    if name_iter.next().is_none() {
                        return Ok(name.0.pop().expect("missing name part"));
                    } else {
                        return Err(name);
                    }
                }
                (Some(c), Some(q)) if c == q => {
                    // ~ continue matching next part
                }
                _ => {
                    return Err(name);
                }
            }
        }
    }

    let mut unqualified = Vec::<Ident>::with_capacity(columns.len());
    for mut name in columns {
        if name.0.is_empty() {
            return Err(("empty column name", name.span().start));
        }

        if name.0.len() == 1 {
            unqualified.push(to_ident(name.0.pop().expect("missing name part"))?);
            continue;
        }

        // ~ try matching by the primary prefix
        if let Some(allowed_qualifier) = allowed_qualifier_1 {
            match unqualify_column(name, allowed_qualifier) {
                Ok(ident) => {
                    unqualified.push(to_ident(ident)?);
                    continue;
                }
                Err(n) => {
                    // ~ continue trying with the alternate prefix below
                    name = n;
                }
            }
        }

        // ~ try matching by the alternate prefix
        if let Some(allowed_qualifier) = allowed_qualifier_2 {
            if name.0.len() == 2
                && name
                    .0
                    .first()
                    .and_then(ObjectNamePart::as_ident)
                    .map(|i| i == allowed_qualifier)
                    .unwrap_or(false)
            {
                unqualified.push(to_ident(name.0.pop().expect("missing name part"))?);
                continue;
            }
        }

        return Err(("not matching target table", name.span().start));
    }
    Ok(unqualified)
}

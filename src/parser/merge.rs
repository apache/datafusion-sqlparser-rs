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
        Merge, MergeAction, MergeClause, MergeClauseKind, MergeInsertExpr, MergeInsertKind,
        MergeUpdateExpr, ObjectName, OutputClause, SetExpr, Statement,
    },
    dialect::{BigQueryDialect, GenericDialect, MySqlDialect},
    keywords::Keyword,
    parser::IsOptional,
    tokenizer::TokenWithSpan,
};

use super::{Parser, ParserError};

impl Parser<'_> {
    /// Parse a `MERGE` statement, returning a `Box`ed SetExpr
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
        let clauses = self.parse_merge_clauses()?;
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

    fn parse_merge_clauses(&mut self) -> Result<Vec<MergeClause>, ParserError> {
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
                    let update_predicate = if self.parse_keyword(Keyword::WHERE) {
                        Some(self.parse_expr()?)
                    } else {
                        None
                    };
                    let delete_predicate = if self.parse_keyword(Keyword::DELETE) {
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

                    let columns = self.parse_merge_clause_insert_columns(is_mysql)?;
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
                    let insert_predicate = if self.parse_keyword(Keyword::WHERE) {
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
        allow_empty: bool,
    ) -> Result<Vec<ObjectName>, ParserError> {
        self.parse_parenthesized_qualified_column_list(IsOptional::Optional, allow_empty)
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

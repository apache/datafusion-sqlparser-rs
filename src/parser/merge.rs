use crate::parser::*;

impl Parser<'_> {
    pub fn parse_merge(&mut self) -> Result<Statement, ParserError> {
        let into = self.parse_keyword(Keyword::INTO);

        let table = self.parse_table_factor()?;

        self.expect_keyword(Keyword::USING)?;
        let source = self.parse_table_factor()?;
        self.expect_keyword(Keyword::ON)?;
        let on = self.parse_expr()?;
        let clauses = self.parse_merge_clauses()?;

        Ok(Statement::Merge {
            into,
            table,
            source,
            on: Box::new(on),
            clauses,
        })
    }

    pub fn parse_merge_clauses(&mut self) -> Result<Vec<MergeClause>, ParserError> {
        let mut clauses = vec![];
        loop {
            if self.peek_token() == Token::EOF || self.peek_token() == Token::SemiColon {
                break;
            }
            self.expect_keyword(Keyword::WHEN)?;

            let mut clause_kind = MergeClauseKind::Matched;
            if self.parse_keyword(Keyword::NOT) {
                clause_kind = MergeClauseKind::NotMatched;
            }
            self.expect_keyword(Keyword::MATCHED)?;

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

            self.expect_keyword(Keyword::THEN)?;

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
                        return Err(ParserError::ParserError(format!(
                            "UPDATE is not allowed in a {clause_kind} merge clause"
                        )));
                    }
                    self.expect_keyword(Keyword::SET)?;
                    MergeAction::Update {
                        assignments: self.parse_comma_separated(Parser::parse_assignment)?,
                    }
                }
                Some(Keyword::DELETE) => {
                    if matches!(
                        clause_kind,
                        MergeClauseKind::NotMatched | MergeClauseKind::NotMatchedByTarget
                    ) {
                        return Err(ParserError::ParserError(format!(
                            "DELETE is not allowed in a {clause_kind} merge clause"
                        )));
                    }
                    MergeAction::Delete
                }
                Some(Keyword::INSERT) => {
                    if !matches!(
                        clause_kind,
                        MergeClauseKind::NotMatched | MergeClauseKind::NotMatchedByTarget
                    ) {
                        return Err(ParserError::ParserError(format!(
                            "INSERT is not allowed in a {clause_kind} merge clause"
                        )));
                    }
                    let is_mysql = dialect_of!(self is MySqlDialect);

                    let columns = self.parse_parenthesized_column_list(Optional, is_mysql)?;
                    let kind = if dialect_of!(self is BigQueryDialect | GenericDialect)
                        && self.parse_keyword(Keyword::ROW)
                    {
                        MergeInsertKind::Row
                    } else {
                        self.expect_keyword(Keyword::VALUES)?;
                        let values = self.parse_values(is_mysql)?;
                        MergeInsertKind::Values(values)
                    };
                    MergeAction::Insert(MergeInsertExpr { columns, kind })
                }
                _ => {
                    return Err(ParserError::ParserError(
                        "expected UPDATE, DELETE or INSERT in merge clause".to_string(),
                    ));
                }
            };
            clauses.push(MergeClause {
                clause_kind,
                predicate,
                action: merge_clause,
            });
        }
        Ok(clauses)
    }
}

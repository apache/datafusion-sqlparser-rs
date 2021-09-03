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

#[cfg(not(feature = "std"))]
use alloc::{boxed::Box, format, vec};

use crate::ast::*;
use crate::dialect::keywords::Keyword;
use crate::dialect::*;
use crate::parser::{IsLateral, IsOptional, Parser, ParserError};
use crate::tokenizer::Token;

impl<'a> Parser<'a> {
    /// Parse a query expression, i.e. a `SELECT` statement optionally
    /// preceded with some `WITH` CTE declarations and optionally followed
    /// by `ORDER BY`. Unlike some other parse_... methods, this one doesn't
    /// expect the initial keyword to be already consumed
    pub fn parse_query(&mut self) -> Result<Query, ParserError> {
        let with = if self.parse_keyword(Keyword::WITH) {
            Some(With {
                recursive: self.parse_keyword(Keyword::RECURSIVE),
                cte_tables: self.parse_comma_separated(Parser::parse_cte)?,
            })
        } else {
            None
        };

        if !self.parse_keyword(Keyword::INSERT) {
            let body = self.parse_query_body(0)?;

            let order_by = if self.parse_keywords(&[Keyword::ORDER, Keyword::BY]) {
                self.parse_comma_separated(Parser::parse_order_by_expr)?
            } else {
                vec![]
            };

            let limit = if self.parse_keyword(Keyword::LIMIT) {
                self.parse_limit()?
            } else {
                None
            };

            let offset = if self.parse_keyword(Keyword::OFFSET) {
                Some(self.parse_offset()?)
            } else {
                None
            };

            let fetch = if self.parse_keyword(Keyword::FETCH) {
                Some(self.parse_fetch()?)
            } else {
                None
            };

            Ok(Query {
                with,
                body,
                order_by,
                limit,
                offset,
                fetch,
            })
        } else {
            let insert = self.parse_insert()?;
            Ok(Query {
                with,
                body: SetExpr::Insert(insert),
                limit: None,
                order_by: vec![],
                offset: None,
                fetch: None,
            })
        }
    }

    /// Parse a CTE (`alias [( col1, col2, ... )] AS (subquery)`)
    pub(super) fn parse_cte(&mut self) -> Result<Cte, ParserError> {
        let name = self.parse_identifier()?;

        let mut cte = if self.parse_keyword(Keyword::AS) {
            self.expect_token(&Token::LParen)?;
            let query = self.parse_query()?;
            self.expect_token(&Token::RParen)?;
            let alias = TableAlias {
                name,
                columns: vec![],
            };
            Cte {
                alias,
                query,
                from: None,
            }
        } else {
            let columns = self.parse_parenthesized_column_list(IsOptional::Optional)?;
            self.expect_keyword(Keyword::AS)?;
            self.expect_token(&Token::LParen)?;
            let query = self.parse_query()?;
            self.expect_token(&Token::RParen)?;
            let alias = TableAlias { name, columns };
            Cte {
                alias,
                query,
                from: None,
            }
        };
        if self.parse_keyword(Keyword::FROM) {
            cte.from = Some(self.parse_identifier()?);
        }
        Ok(cte)
    }

    /// Parse a "query body", which is an expression with roughly the
    /// following grammar:
    /// ```text
    ///   query_body ::= restricted_select | '(' subquery ')' | set_operation
    ///   restricted_select ::= 'SELECT' [expr_list] [ from ] [ where ] [ groupby_having ]
    ///   subquery ::= query_body [ order_by_limit ]
    ///   set_operation ::= query_body { 'UNION' | 'EXCEPT' | 'INTERSECT' } [ 'ALL' ] query_body
    /// ```
    pub(super) fn parse_query_body(&mut self, precedence: u8) -> Result<SetExpr, ParserError> {
        // We parse the expression using a Pratt parser, as in `parse_expr()`.
        // Start by parsing a restricted SELECT or a `(subquery)`:
        let mut expr = if self.parse_keyword(Keyword::SELECT) {
            SetExpr::Select(Box::new(self.parse_select()?))
        } else if self.consume_token(&Token::LParen) {
            // CTEs are not allowed here, but the parser currently accepts them
            let subquery = self.parse_query()?;
            self.expect_token(&Token::RParen)?;
            SetExpr::Query(Box::new(subquery))
        } else if self.parse_keyword(Keyword::VALUES) {
            SetExpr::Values(self.parse_values()?)
        } else {
            return self.expected(
                "SELECT, VALUES, or a subquery in the query body",
                self.peek_token(),
            );
        };

        loop {
            // The query can be optionally followed by a set operator:
            let op = self.parse_set_operator(&self.peek_token());
            let next_precedence = match op {
                // UNION and EXCEPT have the same binding power and evaluate left-to-right
                Some(SetOperator::Union) | Some(SetOperator::Except) => 10,
                // INTERSECT has higher precedence than UNION/EXCEPT
                Some(SetOperator::Intersect) => 20,
                // Unexpected token or EOF => stop parsing the query body
                None => break,
            };
            if precedence >= next_precedence {
                break;
            }
            self.next_token(); // skip past the set operator
            expr = SetExpr::SetOperation {
                left: Box::new(expr),
                op: op.unwrap(),
                all: self.parse_keyword(Keyword::ALL),
                right: Box::new(self.parse_query_body(next_precedence)?),
            };
        }

        Ok(expr)
    }

    fn parse_set_operator(&mut self, token: &Token) -> Option<SetOperator> {
        match token {
            Token::Word(w) if w.keyword == Keyword::UNION => Some(SetOperator::Union),
            Token::Word(w) if w.keyword == Keyword::EXCEPT => Some(SetOperator::Except),
            Token::Word(w) if w.keyword == Keyword::INTERSECT => Some(SetOperator::Intersect),
            _ => None,
        }
    }

    /// Parse a restricted `SELECT` statement (no CTEs / `UNION` / `ORDER BY`),
    /// assuming the initial `SELECT` was already consumed
    pub fn parse_select(&mut self) -> Result<Select, ParserError> {
        let distinct = self.parse_all_or_distinct()?;

        let top = if self.parse_keyword(Keyword::TOP) {
            Some(self.parse_top()?)
        } else {
            None
        };

        let projection = self.parse_comma_separated(Parser::parse_select_item)?;

        // Note that for keywords to be properly handled here, they need to be
        // added to `RESERVED_FOR_COLUMN_ALIAS` / `RESERVED_FOR_TABLE_ALIAS`,
        // otherwise they may be parsed as an alias as part of the `projection`
        // or `from`.

        let from = if self.parse_keyword(Keyword::FROM) {
            self.parse_comma_separated(Parser::parse_table_and_joins)?
        } else {
            vec![]
        };
        let mut lateral_views = vec![];
        loop {
            if self.parse_keywords(&[Keyword::LATERAL, Keyword::VIEW]) {
                let outer = self.parse_keyword(Keyword::OUTER);
                let lateral_view = self.parse_expr()?;
                let lateral_view_name = self.parse_object_name()?;
                let lateral_col_alias = self
                    .parse_comma_separated(|parser| {
                        parser.parse_optional_alias(&[
                            Keyword::WHERE,
                            Keyword::GROUP,
                            Keyword::CLUSTER,
                            Keyword::HAVING,
                            Keyword::LATERAL,
                        ]) // This couldn't possibly be a bad idea
                    })?
                    .into_iter()
                    .flatten()
                    .collect();

                lateral_views.push(LateralView {
                    lateral_view,
                    lateral_view_name,
                    lateral_col_alias,
                    outer,
                });
            } else {
                break;
            }
        }

        let selection = if self.parse_keyword(Keyword::WHERE) {
            Some(self.parse_expr()?)
        } else {
            None
        };

        let group_by = if self.parse_keywords(&[Keyword::GROUP, Keyword::BY]) {
            self.parse_comma_separated(Parser::parse_expr)?
        } else {
            vec![]
        };

        let cluster_by = if self.parse_keywords(&[Keyword::CLUSTER, Keyword::BY]) {
            self.parse_comma_separated(Parser::parse_expr)?
        } else {
            vec![]
        };

        let distribute_by = if self.parse_keywords(&[Keyword::DISTRIBUTE, Keyword::BY]) {
            self.parse_comma_separated(Parser::parse_expr)?
        } else {
            vec![]
        };

        let sort_by = if self.parse_keywords(&[Keyword::SORT, Keyword::BY]) {
            self.parse_comma_separated(Parser::parse_expr)?
        } else {
            vec![]
        };

        let having = if self.parse_keyword(Keyword::HAVING) {
            Some(self.parse_expr()?)
        } else {
            None
        };

        Ok(Select {
            distinct,
            top,
            projection,
            from,
            lateral_views,
            selection,
            group_by,
            cluster_by,
            distribute_by,
            sort_by,
            having,
        })
    }

    /// Parse a comma-delimited list of projections after SELECT
    pub fn parse_select_item(&mut self) -> Result<SelectItem, ParserError> {
        let expr = self.parse_expr()?;
        if let Expr::Wildcard = expr {
            Ok(SelectItem::Wildcard)
        } else if let Expr::QualifiedWildcard(prefix) = expr {
            Ok(SelectItem::QualifiedWildcard(ObjectName(prefix)))
        } else {
            // `expr` is a regular SQL expression and can be followed by an alias
            if let Some(alias) = self.parse_optional_alias(keywords::RESERVED_FOR_COLUMN_ALIAS)? {
                Ok(SelectItem::ExprWithAlias { expr, alias })
            } else {
                Ok(SelectItem::UnnamedExpr(expr))
            }
        }
    }

    pub fn parse_table_and_joins(&mut self) -> Result<TableWithJoins, ParserError> {
        let relation = self.parse_table_factor()?;

        // Note that for keywords to be properly handled here, they need to be
        // added to `RESERVED_FOR_TABLE_ALIAS`, otherwise they may be parsed as
        // a table alias.
        let mut joins = vec![];
        loop {
            let join = if self.parse_keyword(Keyword::CROSS) {
                let join_operator = if self.parse_keyword(Keyword::JOIN) {
                    JoinOperator::CrossJoin
                } else if self.parse_keyword(Keyword::APPLY) {
                    // MSSQL extension, similar to CROSS JOIN LATERAL
                    JoinOperator::CrossApply
                } else {
                    return self.expected("JOIN or APPLY after CROSS", self.peek_token());
                };
                Join {
                    relation: self.parse_table_factor()?,
                    join_operator,
                }
            } else if self.parse_keyword(Keyword::OUTER) {
                // MSSQL extension, similar to LEFT JOIN LATERAL .. ON 1=1
                self.expect_keyword(Keyword::APPLY)?;
                Join {
                    relation: self.parse_table_factor()?,
                    join_operator: JoinOperator::OuterApply,
                }
            } else {
                let natural = self.parse_keyword(Keyword::NATURAL);
                let peek_keyword = if let Token::Word(w) = self.peek_token() {
                    w.keyword
                } else {
                    Keyword::NoKeyword
                };

                let join_operator_type = match peek_keyword {
                    Keyword::INNER | Keyword::JOIN => {
                        let _ = self.parse_keyword(Keyword::INNER);
                        self.expect_keyword(Keyword::JOIN)?;
                        JoinOperator::Inner
                    }
                    kw @ Keyword::LEFT | kw @ Keyword::RIGHT | kw @ Keyword::FULL => {
                        let _ = self.next_token();
                        let _ = self.parse_keyword(Keyword::OUTER);
                        self.expect_keyword(Keyword::JOIN)?;
                        match kw {
                            Keyword::LEFT => JoinOperator::LeftOuter,
                            Keyword::RIGHT => JoinOperator::RightOuter,
                            Keyword::FULL => JoinOperator::FullOuter,
                            _ => unreachable!(),
                        }
                    }
                    Keyword::OUTER => {
                        return self.expected("LEFT, RIGHT, or FULL", self.peek_token());
                    }
                    _ if natural => {
                        return self.expected("a join type after NATURAL", self.peek_token());
                    }
                    _ => break,
                };
                let relation = self.parse_table_factor()?;
                let join_constraint = self.parse_join_constraint(natural)?;
                Join {
                    relation,
                    join_operator: join_operator_type(join_constraint),
                }
            };
            joins.push(join);
        }
        Ok(TableWithJoins { relation, joins })
    }

    /// A table name or a parenthesized subquery, followed by optional `[AS] alias`
    pub fn parse_table_factor(&mut self) -> Result<TableFactor, ParserError> {
        if self.parse_keyword(Keyword::LATERAL) {
            // LATERAL must always be followed by a subquery.
            if !self.consume_token(&Token::LParen) {
                self.expected("subquery after LATERAL", self.peek_token())?;
            }
            self.parse_derived_table_factor(IsLateral::Lateral)
        } else if self.parse_keyword(Keyword::TABLE) {
            // parse table function (SELECT * FROM TABLE (<expr>) [ AS <alias> ])
            self.expect_token(&Token::LParen)?;
            let expr = self.parse_expr()?;
            self.expect_token(&Token::RParen)?;
            let alias = self.parse_optional_table_alias(keywords::RESERVED_FOR_TABLE_ALIAS)?;
            Ok(TableFactor::TableFunction { expr, alias })
        } else if self.consume_token(&Token::LParen) {
            // A left paren introduces either a derived table (i.e., a subquery)
            // or a nested join. It's nearly impossible to determine ahead of
            // time which it is... so we just try to parse both.
            //
            // Here's an example that demonstrates the complexity:
            //                     /-------------------------------------------------------\
            //                     | /-----------------------------------\                 |
            //     SELECT * FROM ( ( ( (SELECT 1) UNION (SELECT 2) ) AS t1 NATURAL JOIN t2 ) )
            //                   ^ ^ ^ ^
            //                   | | | |
            //                   | | | |
            //                   | | | (4) belongs to a SetExpr::Query inside the subquery
            //                   | | (3) starts a derived table (subquery)
            //                   | (2) starts a nested join
            //                   (1) an additional set of parens around a nested join
            //

            // If the recently consumed '(' starts a derived table, the call to
            // `parse_derived_table_factor` below will return success after parsing the
            // subquery, followed by the closing ')', and the alias of the derived table.
            // In the example above this is case (3).
            return_ok_if_some!(
                self.maybe_parse(|parser| parser.parse_derived_table_factor(IsLateral::NotLateral))
            );
            // A parsing error from `parse_derived_table_factor` indicates that the '(' we've
            // recently consumed does not start a derived table (cases 1, 2, or 4).
            // `maybe_parse` will ignore such an error and rewind to be after the opening '('.

            // Inside the parentheses we expect to find an (A) table factor
            // followed by some joins or (B) another level of nesting.
            let mut table_and_joins = self.parse_table_and_joins()?;

            if !table_and_joins.joins.is_empty() {
                self.expect_token(&Token::RParen)?;
                Ok(TableFactor::NestedJoin(Box::new(table_and_joins))) // (A)
            } else if let TableFactor::NestedJoin(_) = &table_and_joins.relation {
                // (B): `table_and_joins` (what we found inside the parentheses)
                // is a nested join `(foo JOIN bar)`, not followed by other joins.
                self.expect_token(&Token::RParen)?;
                Ok(TableFactor::NestedJoin(Box::new(table_and_joins)))
            } else if dialect_of!(self is SnowflakeDialect | GenericDialect) {
                // Dialect-specific behavior: Snowflake diverges from the
                // standard and from most of the other implementations by
                // allowing extra parentheses not only around a join (B), but
                // around lone table names (e.g. `FROM (mytable [AS alias])`)
                // and around derived tables (e.g. `FROM ((SELECT ...)
                // [AS alias])`) as well.
                self.expect_token(&Token::RParen)?;

                if let Some(outer_alias) =
                    self.parse_optional_table_alias(keywords::RESERVED_FOR_TABLE_ALIAS)?
                {
                    // Snowflake also allows specifying an alias *after* parens
                    // e.g. `FROM (mytable) AS alias`
                    match &mut table_and_joins.relation {
                        TableFactor::Derived { alias, .. }
                        | TableFactor::Table { alias, .. }
                        | TableFactor::TableFunction { alias, .. } => {
                            // but not `FROM (mytable AS alias1) AS alias2`.
                            if let Some(inner_alias) = alias {
                                return Err(ParserError::ParserError(format!(
                                    "duplicate alias {}",
                                    inner_alias
                                )));
                            }
                            // Act as if the alias was specified normally next
                            // to the table name: `(mytable) AS alias` ->
                            // `(mytable AS alias)`
                            alias.replace(outer_alias);
                        }
                        TableFactor::NestedJoin(_) => unreachable!(),
                    };
                }
                // Do not store the extra set of parens in the AST
                Ok(table_and_joins.relation)
            } else {
                // The SQL spec prohibits derived tables and bare tables from
                // appearing alone in parentheses (e.g. `FROM (mytable)`)
                self.expected("joined table", self.peek_token())
            }
        } else {
            let name = self.parse_object_name()?;
            // Postgres, MSSQL: table-valued functions:
            let args = if self.consume_token(&Token::LParen) {
                self.parse_optional_args()?
            } else {
                vec![]
            };
            let alias = self.parse_optional_table_alias(keywords::RESERVED_FOR_TABLE_ALIAS)?;
            // MSSQL-specific table hints:
            let mut with_hints = vec![];
            if self.parse_keyword(Keyword::WITH) {
                if self.consume_token(&Token::LParen) {
                    with_hints = self.parse_comma_separated(Parser::parse_expr)?;
                    self.expect_token(&Token::RParen)?;
                } else {
                    // rewind, as WITH may belong to the next statement's CTE
                    self.prev_token();
                }
            };
            Ok(TableFactor::Table {
                name,
                alias,
                args,
                with_hints,
            })
        }
    }

    pub fn parse_derived_table_factor(
        &mut self,
        lateral: IsLateral,
    ) -> Result<TableFactor, ParserError> {
        let subquery = Box::new(self.parse_query()?);
        self.expect_token(&Token::RParen)?;
        let alias = self.parse_optional_table_alias(keywords::RESERVED_FOR_TABLE_ALIAS)?;
        Ok(TableFactor::Derived {
            lateral: match lateral {
                IsLateral::Lateral => true,
                IsLateral::NotLateral => false,
            },
            subquery,
            alias,
        })
    }

    /// Parse `AS identifier` when the AS is describing a table-valued object,
    /// like in `... FROM generate_series(1, 10) AS t (col)`. In this case
    /// the alias is allowed to optionally name the columns in the table, in
    /// addition to the table itself.
    pub fn parse_optional_table_alias(
        &mut self,
        reserved_kwds: &[Keyword],
    ) -> Result<Option<TableAlias>, ParserError> {
        match self.parse_optional_alias(reserved_kwds)? {
            Some(name) => {
                let columns = self.parse_parenthesized_column_list(IsOptional::Optional)?;
                Ok(Some(TableAlias { name, columns }))
            }
            None => Ok(None),
        }
    }

    /// Parse `AS identifier` (or simply `identifier` if it's not a reserved keyword)
    /// Some examples with aliases: `SELECT 1 foo`, `SELECT COUNT(*) AS cnt`,
    /// `SELECT ... FROM t1 foo, t2 bar`, `SELECT ... FROM (...) AS bar`
    pub fn parse_optional_alias(
        &mut self,
        reserved_kwds: &[Keyword],
    ) -> Result<Option<Ident>, ParserError> {
        let after_as = self.parse_keyword(Keyword::AS);
        match self.next_token() {
            // Accept any identifier after `AS` (though many dialects have restrictions on
            // keywords that may appear here). If there's no `AS`: don't parse keywords,
            // which may start a construct allowed in this position, to be parsed as aliases.
            // (For example, in `FROM t1 JOIN` the `JOIN` will always be parsed as a keyword,
            // not an alias.)
            Token::Word(w) if after_as || !reserved_kwds.contains(&w.keyword) => {
                Ok(Some(w.to_ident()))
            }
            // MSSQL supports single-quoted strings as aliases for columns
            // We accept them as table aliases too, although MSSQL does not.
            //
            // Note, that this conflicts with an obscure rule from the SQL
            // standard, which we don't implement:
            // https://crate.io/docs/sql-99/en/latest/chapters/07.html#character-string-literal-s
            //    "[Obscure Rule] SQL allows you to break a long <character
            //    string literal> up into two or more smaller <character string
            //    literal>s, split by a <separator> that includes a newline
            //    character. When it sees such a <literal>, your DBMS will
            //    ignore the <separator> and treat the multiple strings as
            //    a single <literal>."
            Token::SingleQuotedString(s) => Ok(Some(Ident::with_quote('\'', s))),
            not_an_ident => {
                if after_as {
                    return self.expected("an identifier after AS", not_an_ident);
                }
                self.prev_token();
                Ok(None) // no alias found
            }
        }
    }

    fn parse_join_constraint(&mut self, natural: bool) -> Result<JoinConstraint, ParserError> {
        if natural {
            Ok(JoinConstraint::Natural)
        } else if self.parse_keyword(Keyword::ON) {
            let constraint = self.parse_expr()?;
            Ok(JoinConstraint::On(constraint))
        } else if self.parse_keyword(Keyword::USING) {
            let columns = self.parse_parenthesized_column_list(IsOptional::Mandatory)?;
            Ok(JoinConstraint::Using(columns))
        } else {
            Ok(JoinConstraint::None)
            //self.expected("ON, or USING after JOIN", self.peek_token())
        }
    }

    pub fn parse_values(&mut self) -> Result<Values, ParserError> {
        let values = self.parse_comma_separated(|parser| {
            parser.expect_token(&Token::LParen)?;
            let exprs = parser.parse_comma_separated(Parser::parse_expr)?;
            parser.expect_token(&Token::RParen)?;
            Ok(exprs)
        })?;
        Ok(Values(values))
    }

    /// Parse a `TOP` clause, MSSQL equivalent of `LIMIT`,
    /// that follows after `SELECT [DISTINCT]`.
    pub fn parse_top(&mut self) -> Result<Top, ParserError> {
        let quantity = if self.consume_token(&Token::LParen) {
            let quantity = self.parse_expr()?;
            self.expect_token(&Token::RParen)?;
            Some(quantity)
        } else {
            Some(Expr::Value(self.parse_number_value()?))
        };

        let percent = self.parse_keyword(Keyword::PERCENT);

        let with_ties = self.parse_keywords(&[Keyword::WITH, Keyword::TIES]);

        Ok(Top {
            with_ties,
            percent,
            quantity,
        })
    }

    /// Parse a `LIMIT` clause
    pub fn parse_limit(&mut self) -> Result<Option<Expr>, ParserError> {
        if self.parse_keyword(Keyword::ALL) {
            Ok(None)
        } else {
            Ok(Some(Expr::Value(self.parse_number_value()?)))
        }
    }

    /// Parse an `OFFSET` clause
    pub fn parse_offset(&mut self) -> Result<Offset, ParserError> {
        let value = Expr::Value(self.parse_number_value()?);
        let rows = if self.parse_keyword(Keyword::ROW) {
            OffsetRows::Row
        } else if self.parse_keyword(Keyword::ROWS) {
            OffsetRows::Rows
        } else {
            OffsetRows::None
        };
        Ok(Offset { value, rows })
    }

    /// Parse a `FETCH` clause
    pub fn parse_fetch(&mut self) -> Result<Fetch, ParserError> {
        self.expect_one_of_keywords(&[Keyword::FIRST, Keyword::NEXT])?;
        let (quantity, percent) = if self
            .parse_one_of_keywords(&[Keyword::ROW, Keyword::ROWS])
            .is_some()
        {
            (None, false)
        } else {
            let quantity = Expr::Value(self.parse_value()?);
            let percent = self.parse_keyword(Keyword::PERCENT);
            self.expect_one_of_keywords(&[Keyword::ROW, Keyword::ROWS])?;
            (Some(quantity), percent)
        };
        let with_ties = if self.parse_keyword(Keyword::ONLY) {
            false
        } else if self.parse_keywords(&[Keyword::WITH, Keyword::TIES]) {
            true
        } else {
            return self.expected("one of ONLY or WITH TIES", self.peek_token());
        };
        Ok(Fetch {
            with_ties,
            percent,
            quantity,
        })
    }
}

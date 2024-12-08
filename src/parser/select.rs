use super::*;

use crate::parser_err;

impl Parser<'_> {
    /// Parse a query expression, i.e. a `SELECT` statement optionally
    /// preceded with some `WITH` CTE declarations and optionally followed
    /// by `ORDER BY`. Unlike some other parse_... methods, this one doesn't
    /// expect the initial keyword to be already consumed
    pub fn parse_query(&mut self) -> Result<Box<Query>, ParserError> {
        let _guard = self.recursion_counter.try_decrease()?;
        let with = if let Some(with_token) = self.parse_keyword_token(Keyword::WITH) {
            Some(With {
                with_token: with_token.into(),
                recursive: self.parse_keyword(Keyword::RECURSIVE),
                cte_tables: self.parse_comma_separated(Parser::parse_cte)?,
            })
        } else {
            None
        };
        if self.parse_keyword(Keyword::INSERT) {
            Ok(Query {
                with,
                body: self.parse_insert_setexpr_boxed()?,
                limit: None,
                limit_by: vec![],
                order_by: None,
                offset: None,
                fetch: None,
                locks: vec![],
                for_clause: None,
                settings: None,
                format_clause: None,
            }
            .into())
        } else if self.parse_keyword(Keyword::UPDATE) {
            Ok(Query {
                with,
                body: self.parse_update_setexpr_boxed()?,
                limit: None,
                limit_by: vec![],
                order_by: None,
                offset: None,
                fetch: None,
                locks: vec![],
                for_clause: None,
                settings: None,
                format_clause: None,
            }
            .into())
        } else {
            let body = self.parse_query_body(self.dialect.prec_unknown())?;

            let order_by = self.parse_optional_order_by()?;

            let mut limit = None;
            let mut offset = None;

            for _x in 0..2 {
                if limit.is_none() && self.parse_keyword(Keyword::LIMIT) {
                    limit = self.parse_limit()?
                }

                if offset.is_none() && self.parse_keyword(Keyword::OFFSET) {
                    offset = Some(self.parse_offset()?)
                }

                if self.dialect.supports_limit_comma()
                    && limit.is_some()
                    && offset.is_none()
                    && self.consume_token(&Token::Comma)
                {
                    // MySQL style LIMIT x,y => LIMIT y OFFSET x.
                    // Check <https://dev.mysql.com/doc/refman/8.0/en/select.html> for more details.
                    offset = Some(Offset {
                        value: limit.unwrap(),
                        rows: OffsetRows::None,
                    });
                    limit = Some(self.parse_expr()?);
                }
            }

            let limit_by = if dialect_of!(self is ClickHouseDialect | GenericDialect)
                && self.parse_keyword(Keyword::BY)
            {
                self.parse_comma_separated(Parser::parse_expr)?
            } else {
                vec![]
            };

            let settings = self.parse_settings()?;

            let fetch = if self.parse_keyword(Keyword::FETCH) {
                Some(self.parse_fetch()?)
            } else {
                None
            };

            let mut for_clause = None;
            let mut locks = Vec::new();
            while self.parse_keyword(Keyword::FOR) {
                if let Some(parsed_for_clause) = self.parse_for_clause()? {
                    for_clause = Some(parsed_for_clause);
                    break;
                } else {
                    locks.push(self.parse_lock()?);
                }
            }
            let format_clause = if dialect_of!(self is ClickHouseDialect | GenericDialect)
                && self.parse_keyword(Keyword::FORMAT)
            {
                if self.parse_keyword(Keyword::NULL) {
                    Some(FormatClause::Null)
                } else {
                    let ident = self.parse_identifier(false)?;
                    Some(FormatClause::Identifier(ident))
                }
            } else {
                None
            };

            Ok(Query {
                with,
                body,
                order_by,
                limit,
                limit_by,
                offset,
                fetch,
                locks,
                for_clause,
                settings,
                format_clause,
            }
            .into())
        }
    }

    /// Parse either `ALL`, `DISTINCT` or `DISTINCT ON (...)`. Returns [`None`] if `ALL` is parsed
    /// and results in a [`ParserError`] if both `ALL` and `DISTINCT` are found.
    pub fn parse_all_or_distinct(&mut self) -> Result<Option<Distinct>, ParserError> {
        let loc = self.peek_token().span.start;
        let all = self.parse_keyword(Keyword::ALL);
        let distinct = self.parse_keyword(Keyword::DISTINCT);
        if !distinct {
            return Ok(None);
        }
        if all {
            return parser_err!("Cannot specify both ALL and DISTINCT".to_string(), loc);
        }
        let on = self.parse_keyword(Keyword::ON);
        if !on {
            return Ok(Some(Distinct::Distinct));
        }

        self.expect_token(&Token::LParen)?;
        let col_names = if self.consume_token(&Token::RParen) {
            self.prev_token();
            Vec::new()
        } else {
            self.parse_comma_separated(Parser::parse_expr)?
        };
        self.expect_token(&Token::RParen)?;
        Ok(Some(Distinct::On(col_names)))
    }

    /// Parse `CREATE TABLE x AS TABLE y`
    pub fn parse_as_table(&mut self) -> Result<Table, ParserError> {
        let token1 = self.next_token();
        let token2 = self.next_token();
        let token3 = self.next_token();

        let table_name;
        let schema_name;
        if token2 == Token::Period {
            match token1.token {
                Token::Word(w) => {
                    schema_name = w.value;
                }
                _ => {
                    return self.expected("Schema name", token1);
                }
            }
            match token3.token {
                Token::Word(w) => {
                    table_name = w.value;
                }
                _ => {
                    return self.expected("Table name", token3);
                }
            }
            Ok(Table {
                table_name: Some(table_name),
                schema_name: Some(schema_name),
            })
        } else {
            match token1.token {
                Token::Word(w) => {
                    table_name = w.value;
                }
                _ => {
                    return self.expected("Table name", token1);
                }
            }
            Ok(Table {
                table_name: Some(table_name),
                schema_name: None,
            })
        }
    }

    /// Parse ASC or DESC, returns an Option with true if ASC, false of DESC or `None` if none of
    /// them.
    pub fn parse_asc_desc(&mut self) -> Option<bool> {
        if self.parse_keyword(Keyword::ASC) {
            Some(true)
        } else if self.parse_keyword(Keyword::DESC) {
            Some(false)
        } else {
            None
        }
    }

    pub fn parse_connect_by(&mut self) -> Result<ConnectBy, ParserError> {
        let (condition, relationships) = if self.parse_keywords(&[Keyword::CONNECT, Keyword::BY]) {
            let relationships = self.with_state(ParserState::ConnectBy, |parser| {
                parser.parse_comma_separated(Parser::parse_expr)
            })?;
            self.expect_keywords(&[Keyword::START, Keyword::WITH])?;
            let condition = self.parse_expr()?;
            (condition, relationships)
        } else {
            self.expect_keywords(&[Keyword::START, Keyword::WITH])?;
            let condition = self.parse_expr()?;
            self.expect_keywords(&[Keyword::CONNECT, Keyword::BY])?;
            let relationships = self.with_state(ParserState::ConnectBy, |parser| {
                parser.parse_comma_separated(Parser::parse_expr)
            })?;
            (condition, relationships)
        };
        Ok(ConnectBy {
            condition,
            relationships,
        })
    }

    /// Parse a CTE (`alias [( col1, col2, ... )] AS (subquery)`)
    pub fn parse_cte(&mut self) -> Result<Cte, ParserError> {
        let name = self.parse_identifier(false)?;

        let mut cte = if self.parse_keyword(Keyword::AS) {
            let mut is_materialized = None;
            if dialect_of!(self is PostgreSqlDialect) {
                if self.parse_keyword(Keyword::MATERIALIZED) {
                    is_materialized = Some(CteAsMaterialized::Materialized);
                } else if self.parse_keywords(&[Keyword::NOT, Keyword::MATERIALIZED]) {
                    is_materialized = Some(CteAsMaterialized::NotMaterialized);
                }
            }
            self.expect_token(&Token::LParen)?;

            let query = self.parse_query()?;
            let closing_paren_token = self.expect_token(&Token::RParen)?;

            let alias = TableAlias {
                name,
                columns: vec![],
            };
            Cte {
                alias,
                query,
                from: None,
                materialized: is_materialized,
                closing_paren_token: closing_paren_token.into(),
            }
        } else {
            let columns = self.parse_table_alias_column_defs()?;
            self.expect_keyword(Keyword::AS)?;
            let mut is_materialized = None;
            if dialect_of!(self is PostgreSqlDialect) {
                if self.parse_keyword(Keyword::MATERIALIZED) {
                    is_materialized = Some(CteAsMaterialized::Materialized);
                } else if self.parse_keywords(&[Keyword::NOT, Keyword::MATERIALIZED]) {
                    is_materialized = Some(CteAsMaterialized::NotMaterialized);
                }
            }
            self.expect_token(&Token::LParen)?;

            let query = self.parse_query()?;
            let closing_paren_token = self.expect_token(&Token::RParen)?;

            let alias = TableAlias { name, columns };
            Cte {
                alias,
                query,
                from: None,
                materialized: is_materialized,
                closing_paren_token: closing_paren_token.into(),
            }
        };
        if self.parse_keyword(Keyword::FROM) {
            cte.from = Some(self.parse_identifier(false)?);
        }
        Ok(cte)
    }

    pub fn parse_derived_table_factor(
        &mut self,
        lateral: IsLateral,
    ) -> Result<TableFactor, ParserError> {
        let subquery = self.parse_query()?;
        self.expect_token(&Token::RParen)?;
        let alias = self.parse_optional_table_alias(keywords::RESERVED_FOR_TABLE_ALIAS)?;
        Ok(TableFactor::Derived {
            lateral: match lateral {
                Lateral => true,
                NotLateral => false,
            },
            subquery,
            alias,
        })
    }

    /// Parses an expression with an optional alias
    ///
    /// Examples:
    ///
    /// ```sql
    /// SUM(price) AS total_price
    /// ```
    /// ```sql
    /// SUM(price)
    /// ```
    ///
    /// Example
    /// ```
    /// # use sqlparser::parser::{Parser, ParserError};
    /// # use sqlparser::dialect::GenericDialect;
    /// # fn main() ->Result<(), ParserError> {
    /// let sql = r#"SUM("a") as "b""#;
    /// let mut parser = Parser::new(&GenericDialect).try_with_sql(sql)?;
    /// let expr_with_alias = parser.parse_expr_with_alias()?;
    /// assert_eq!(Some("b".to_string()), expr_with_alias.alias.map(|x|x.value));
    /// # Ok(())
    /// # }
    pub fn parse_expr_with_alias(&mut self) -> Result<ExprWithAlias, ParserError> {
        let expr = self.parse_expr()?;
        let alias = if self.parse_keyword(Keyword::AS) {
            Some(self.parse_identifier(false)?)
        } else {
            None
        };

        Ok(ExprWithAlias { expr, alias })
    }

    /// Parse a FETCH clause
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

    pub fn parse_function_args(&mut self) -> Result<FunctionArg, ParserError> {
        let arg = if self.dialect.supports_named_fn_args_with_expr_name() {
            self.maybe_parse(|p| {
                let name = p.parse_expr()?;
                let operator = p.parse_function_named_arg_operator()?;
                let arg = p.parse_wildcard_expr()?.into();
                Ok(FunctionArg::ExprNamed {
                    name,
                    arg,
                    operator,
                })
            })?
        } else {
            self.maybe_parse(|p| {
                let name = p.parse_identifier(false)?;
                let operator = p.parse_function_named_arg_operator()?;
                let arg = p.parse_wildcard_expr()?.into();
                Ok(FunctionArg::Named {
                    name,
                    arg,
                    operator,
                })
            })?
        };
        if let Some(arg) = arg {
            return Ok(arg);
        }
        Ok(FunctionArg::Unnamed(self.parse_wildcard_expr()?.into()))
    }

    // Parse a INTERPOLATE expression (ClickHouse dialect)
    pub fn parse_interpolation(&mut self) -> Result<InterpolateExpr, ParserError> {
        let column = self.parse_identifier(false)?;
        let expr = if self.parse_keyword(Keyword::AS) {
            Some(self.parse_expr()?)
        } else {
            None
        };
        Ok(InterpolateExpr { column, expr })
    }

    // Parse a set of comma seperated INTERPOLATE expressions (ClickHouse dialect)
    // that follow the INTERPOLATE keyword in an ORDER BY clause with the WITH FILL modifier
    pub fn parse_interpolations(&mut self) -> Result<Option<Interpolate>, ParserError> {
        if !self.parse_keyword(Keyword::INTERPOLATE) {
            return Ok(None);
        }

        if self.consume_token(&Token::LParen) {
            let interpolations =
                self.parse_comma_separated0(|p| p.parse_interpolation(), Token::RParen)?;
            self.expect_token(&Token::RParen)?;
            // INTERPOLATE () and INTERPOLATE ( ... ) variants
            return Ok(Some(Interpolate {
                exprs: Some(interpolations),
            }));
        }

        // INTERPOLATE
        Ok(Some(Interpolate { exprs: None }))
    }

    pub fn parse_join_constraint(&mut self, natural: bool) -> Result<JoinConstraint, ParserError> {
        if natural {
            Ok(JoinConstraint::Natural)
        } else if self.parse_keyword(Keyword::ON) {
            let constraint = self.parse_expr()?;
            Ok(JoinConstraint::On(constraint))
        } else if self.parse_keyword(Keyword::USING) {
            let columns = self.parse_parenthesized_column_list(Mandatory, false)?;
            Ok(JoinConstraint::Using(columns))
        } else {
            Ok(JoinConstraint::None)
            //self.expected("ON, or USING after JOIN", self.peek_token())
        }
    }

    /// Parses MySQL's JSON_TABLE column definition.
    /// For example: `id INT EXISTS PATH '$' DEFAULT '0' ON EMPTY ERROR ON ERROR`
    pub fn parse_json_table_column_def(&mut self) -> Result<JsonTableColumn, ParserError> {
        if self.parse_keyword(Keyword::NESTED) {
            let _has_path_keyword = self.parse_keyword(Keyword::PATH);
            let path = self.parse_value()?;
            self.expect_keyword(Keyword::COLUMNS)?;
            let columns = self.parse_parenthesized(|p| {
                p.parse_comma_separated(Self::parse_json_table_column_def)
            })?;
            return Ok(JsonTableColumn::Nested(JsonTableNestedColumn {
                path,
                columns,
            }));
        }
        let name = self.parse_identifier(false)?;
        if self.parse_keyword(Keyword::FOR) {
            self.expect_keyword(Keyword::ORDINALITY)?;
            return Ok(JsonTableColumn::ForOrdinality(name));
        }
        let r#type = self.parse_data_type()?;
        let exists = self.parse_keyword(Keyword::EXISTS);
        self.expect_keyword(Keyword::PATH)?;
        let path = self.parse_value()?;
        let mut on_empty = None;
        let mut on_error = None;
        while let Some(error_handling) = self.parse_json_table_column_error_handling()? {
            if self.parse_keyword(Keyword::EMPTY) {
                on_empty = Some(error_handling);
            } else {
                self.expect_keyword(Keyword::ERROR)?;
                on_error = Some(error_handling);
            }
        }
        Ok(JsonTableColumn::Named(JsonTableNamedColumn {
            name,
            r#type,
            path,
            exists,
            on_empty,
            on_error,
        }))
    }

    /// Parse a LIMIT clause
    pub fn parse_limit(&mut self) -> Result<Option<Expr>, ParserError> {
        if self.parse_keyword(Keyword::ALL) {
            Ok(None)
        } else {
            Ok(Some(self.parse_expr()?))
        }
    }

    /// Parse a FOR UPDATE/FOR SHARE clause
    pub fn parse_lock(&mut self) -> Result<LockClause, ParserError> {
        let lock_type = match self.expect_one_of_keywords(&[Keyword::UPDATE, Keyword::SHARE])? {
            Keyword::UPDATE => LockType::Update,
            Keyword::SHARE => LockType::Share,
            _ => unreachable!(),
        };
        let of = if self.parse_keyword(Keyword::OF) {
            Some(self.parse_object_name(false)?)
        } else {
            None
        };
        let nonblock = if self.parse_keyword(Keyword::NOWAIT) {
            Some(NonBlock::Nowait)
        } else if self.parse_keywords(&[Keyword::SKIP, Keyword::LOCKED]) {
            Some(NonBlock::SkipLocked)
        } else {
            None
        };
        Ok(LockClause {
            lock_type,
            of,
            nonblock,
        })
    }

    /// Parses MSSQL's `OPENJSON WITH` column definition.
    ///
    /// ```sql
    /// colName type [ column_path ] [ AS JSON ]
    /// ```
    ///
    /// Reference: <https://learn.microsoft.com/en-us/sql/t-sql/functions/openjson-transact-sql?view=sql-server-ver16#syntax>
    pub fn parse_openjson_table_column_def(&mut self) -> Result<OpenJsonTableColumn, ParserError> {
        let name = self.parse_identifier(false)?;
        let r#type = self.parse_data_type()?;
        let path = if let Token::SingleQuotedString(path) = self.peek_token().token {
            self.next_token();
            Some(path)
        } else {
            None
        };
        let as_json = self.parse_keyword(Keyword::AS);
        if as_json {
            self.expect_keyword(Keyword::JSON)?;
        }
        Ok(OpenJsonTableColumn {
            name,
            r#type,
            path,
            as_json,
        })
    }

    pub fn parse_optional_order_by(&mut self) -> Result<Option<OrderBy>, ParserError> {
        if self.parse_keywords(&[Keyword::ORDER, Keyword::BY]) {
            let order_by_exprs = self.parse_comma_separated(Parser::parse_order_by_expr)?;
            let interpolate = if dialect_of!(self is ClickHouseDialect | GenericDialect) {
                self.parse_interpolations()?
            } else {
                None
            };

            Ok(Some(OrderBy {
                exprs: order_by_exprs,
                interpolate,
            }))
        } else {
            Ok(None)
        }
    }

    /// Parse an [`Except`](ExceptSelectItem) information for wildcard select items.
    ///
    /// If it is not possible to parse it, will return an option.
    pub fn parse_optional_select_item_except(
        &mut self,
    ) -> Result<Option<ExceptSelectItem>, ParserError> {
        let opt_except = if self.parse_keyword(Keyword::EXCEPT) {
            if self.peek_token().token == Token::LParen {
                let idents = self.parse_parenthesized_column_list(Mandatory, false)?;
                match &idents[..] {
                    [] => {
                        return self.expected(
                            "at least one column should be parsed by the expect clause",
                            self.peek_token(),
                        )?;
                    }
                    [first, idents @ ..] => Some(ExceptSelectItem {
                        first_element: first.clone(),
                        additional_elements: idents.to_vec(),
                    }),
                }
            } else {
                // Clickhouse allows EXCEPT column_name
                let ident = self.parse_identifier(false)?;
                Some(ExceptSelectItem {
                    first_element: ident,
                    additional_elements: vec![],
                })
            }
        } else {
            None
        };

        Ok(opt_except)
    }

    /// Parse an [`Exclude`](ExcludeSelectItem) information for wildcard select items.
    ///
    /// If it is not possible to parse it, will return an option.
    pub fn parse_optional_select_item_exclude(
        &mut self,
    ) -> Result<Option<ExcludeSelectItem>, ParserError> {
        let opt_exclude = if self.parse_keyword(Keyword::EXCLUDE) {
            if self.consume_token(&Token::LParen) {
                let columns =
                    self.parse_comma_separated(|parser| parser.parse_identifier(false))?;
                self.expect_token(&Token::RParen)?;
                Some(ExcludeSelectItem::Multiple(columns))
            } else {
                let column = self.parse_identifier(false)?;
                Some(ExcludeSelectItem::Single(column))
            }
        } else {
            None
        };

        Ok(opt_exclude)
    }

    /// Parse an [`Ilike`](IlikeSelectItem) information for wildcard select items.
    ///
    /// If it is not possible to parse it, will return an option.
    pub fn parse_optional_select_item_ilike(
        &mut self,
    ) -> Result<Option<IlikeSelectItem>, ParserError> {
        let opt_ilike = if self.parse_keyword(Keyword::ILIKE) {
            let next_token = self.next_token();
            let pattern = match next_token.token {
                Token::SingleQuotedString(s) => s,
                _ => return self.expected("ilike pattern", next_token),
            };
            Some(IlikeSelectItem { pattern })
        } else {
            None
        };
        Ok(opt_ilike)
    }

    /// Parse a [`Rename`](RenameSelectItem) information for wildcard select items.
    pub fn parse_optional_select_item_rename(
        &mut self,
    ) -> Result<Option<RenameSelectItem>, ParserError> {
        let opt_rename = if self.parse_keyword(Keyword::RENAME) {
            if self.consume_token(&Token::LParen) {
                let idents =
                    self.parse_comma_separated(|parser| parser.parse_identifier_with_alias())?;
                self.expect_token(&Token::RParen)?;
                Some(RenameSelectItem::Multiple(idents))
            } else {
                let ident = self.parse_identifier_with_alias()?;
                Some(RenameSelectItem::Single(ident))
            }
        } else {
            None
        };

        Ok(opt_rename)
    }

    /// Parse a [`Replace`](ReplaceSelectItem) information for wildcard select items.
    pub fn parse_optional_select_item_replace(
        &mut self,
    ) -> Result<Option<ReplaceSelectItem>, ParserError> {
        let opt_replace = if self.parse_keyword(Keyword::REPLACE) {
            if self.consume_token(&Token::LParen) {
                let items = self.parse_comma_separated(|parser| {
                    Ok(Box::new(parser.parse_replace_elements()?))
                })?;
                self.expect_token(&Token::RParen)?;
                Some(ReplaceSelectItem { items })
            } else {
                let tok = self.next_token();
                return self.expected("( after REPLACE but", tok);
            }
        } else {
            None
        };

        Ok(opt_replace)
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
                let columns = self.parse_table_alias_column_defs()?;
                Ok(Some(TableAlias { name, columns }))
            }
            None => Ok(None),
        }
    }

    /// Parse an expression, optionally followed by ASC or DESC (used in ORDER BY)
    pub fn parse_order_by_expr(&mut self) -> Result<OrderByExpr, ParserError> {
        let expr = self.parse_expr()?;

        let asc = self.parse_asc_desc();

        let nulls_first = if self.parse_keywords(&[Keyword::NULLS, Keyword::FIRST]) {
            Some(true)
        } else if self.parse_keywords(&[Keyword::NULLS, Keyword::LAST]) {
            Some(false)
        } else {
            None
        };

        let with_fill = if dialect_of!(self is ClickHouseDialect | GenericDialect)
            && self.parse_keywords(&[Keyword::WITH, Keyword::FILL])
        {
            Some(self.parse_with_fill()?)
        } else {
            None
        };

        Ok(OrderByExpr {
            expr,
            asc,
            nulls_first,
            with_fill,
        })
    }

    /// Parse a comma-separated list of 1+ SelectItem
    pub fn parse_projection(&mut self) -> Result<Vec<SelectItem>, ParserError> {
        // BigQuery and Snowflake allow trailing commas, but only in project lists
        // e.g. `SELECT 1, 2, FROM t`
        // https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#trailing_commas
        // https://docs.snowflake.com/en/release-notes/2024/8_11#select-supports-trailing-commas

        let trailing_commas =
            self.options.trailing_commas | self.dialect.supports_projection_trailing_commas();

        self.parse_comma_separated_with_trailing_commas(|p| p.parse_select_item(), trailing_commas)
    }

    pub fn parse_replace_elements(&mut self) -> Result<ReplaceSelectElement, ParserError> {
        let expr = self.parse_expr()?;
        let as_keyword = self.parse_keyword(Keyword::AS);
        let ident = self.parse_identifier(false)?;
        Ok(ReplaceSelectElement {
            expr,
            column_name: ident,
            as_keyword,
        })
    }

    /// Parse a comma-delimited list of projections after SELECT
    pub fn parse_select_item(&mut self) -> Result<SelectItem, ParserError> {
        match self.parse_wildcard_expr()? {
            Expr::QualifiedWildcard(prefix, token) => Ok(SelectItem::QualifiedWildcard(
                prefix,
                self.parse_wildcard_additional_options(token.0)?,
            )),
            Expr::Wildcard(token) => Ok(SelectItem::Wildcard(
                self.parse_wildcard_additional_options(token.0)?,
            )),
            Expr::Identifier(v) if v.value.to_lowercase() == "from" && v.quote_style.is_none() => {
                parser_err!(
                    format!("Expected an expression, found: {}", v),
                    self.peek_token().span.start
                )
            }
            Expr::BinaryOp {
                left,
                op: BinaryOperator::Eq,
                right,
            } if self.dialect.supports_eq_alias_assignment()
                && matches!(left.as_ref(), Expr::Identifier(_)) =>
            {
                let Expr::Identifier(alias) = *left else {
                    return parser_err!(
                        "BUG: expected identifier expression as alias",
                        self.peek_token().span.start
                    );
                };
                Ok(SelectItem::ExprWithAlias {
                    expr: *right,
                    alias,
                })
            }
            expr => self
                .parse_optional_alias(keywords::RESERVED_FOR_COLUMN_ALIAS)
                .map(|alias| match alias {
                    Some(alias) => SelectItem::ExprWithAlias { expr, alias },
                    None => SelectItem::UnnamedExpr(expr),
                }),
        }
    }

    pub fn parse_table_and_joins(&mut self) -> Result<TableWithJoins, ParserError> {
        let relation = self.parse_table_factor()?;
        // Note that for keywords to be properly handled here, they need to be
        // added to `RESERVED_FOR_TABLE_ALIAS`, otherwise they may be parsed as
        // a table alias.
        let mut joins = vec![];
        loop {
            let global = self.parse_keyword(Keyword::GLOBAL);
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
                    global,
                    join_operator,
                }
            } else if self.parse_keyword(Keyword::OUTER) {
                // MSSQL extension, similar to LEFT JOIN LATERAL .. ON 1=1
                self.expect_keyword(Keyword::APPLY)?;
                Join {
                    relation: self.parse_table_factor()?,
                    global,
                    join_operator: JoinOperator::OuterApply,
                }
            } else if self.parse_keyword(Keyword::ASOF) {
                self.expect_keyword(Keyword::JOIN)?;
                let relation = self.parse_table_factor()?;
                self.expect_keyword(Keyword::MATCH_CONDITION)?;
                let match_condition = self.parse_parenthesized(Self::parse_expr)?;
                Join {
                    relation,
                    global,
                    join_operator: JoinOperator::AsOf {
                        match_condition,
                        constraint: self.parse_join_constraint(false)?,
                    },
                }
            } else {
                let natural = self.parse_keyword(Keyword::NATURAL);
                let peek_keyword = if let Token::Word(w) = self.peek_token().token {
                    w.keyword
                } else {
                    Keyword::NoKeyword
                };

                let join_operator_type = match peek_keyword {
                    Keyword::INNER | Keyword::JOIN => {
                        let _ = self.parse_keyword(Keyword::INNER); // [ INNER ]
                        self.expect_keyword(Keyword::JOIN)?;
                        JoinOperator::Inner
                    }
                    kw @ Keyword::LEFT | kw @ Keyword::RIGHT => {
                        let _ = self.next_token(); // consume LEFT/RIGHT
                        let is_left = kw == Keyword::LEFT;
                        let join_type = self.parse_one_of_keywords(&[
                            Keyword::OUTER,
                            Keyword::SEMI,
                            Keyword::ANTI,
                            Keyword::JOIN,
                        ]);
                        match join_type {
                            Some(Keyword::OUTER) => {
                                self.expect_keyword(Keyword::JOIN)?;
                                if is_left {
                                    JoinOperator::LeftOuter
                                } else {
                                    JoinOperator::RightOuter
                                }
                            }
                            Some(Keyword::SEMI) => {
                                self.expect_keyword(Keyword::JOIN)?;
                                if is_left {
                                    JoinOperator::LeftSemi
                                } else {
                                    JoinOperator::RightSemi
                                }
                            }
                            Some(Keyword::ANTI) => {
                                self.expect_keyword(Keyword::JOIN)?;
                                if is_left {
                                    JoinOperator::LeftAnti
                                } else {
                                    JoinOperator::RightAnti
                                }
                            }
                            Some(Keyword::JOIN) => {
                                if is_left {
                                    JoinOperator::LeftOuter
                                } else {
                                    JoinOperator::RightOuter
                                }
                            }
                            _ => {
                                return Err(ParserError::ParserError(format!(
                                    "expected OUTER, SEMI, ANTI or JOIN after {kw:?}"
                                )))
                            }
                        }
                    }
                    Keyword::ANTI => {
                        let _ = self.next_token(); // consume ANTI
                        self.expect_keyword(Keyword::JOIN)?;
                        JoinOperator::Anti
                    }
                    Keyword::SEMI => {
                        let _ = self.next_token(); // consume SEMI
                        self.expect_keyword(Keyword::JOIN)?;
                        JoinOperator::Semi
                    }
                    Keyword::FULL => {
                        let _ = self.next_token(); // consume FULL
                        let _ = self.parse_keyword(Keyword::OUTER); // [ OUTER ]
                        self.expect_keyword(Keyword::JOIN)?;
                        JoinOperator::FullOuter
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
                    global,
                    join_operator: join_operator_type(join_constraint),
                }
            };
            joins.push(join);
        }
        Ok(TableWithJoins { relation, joins })
    }

    /// Parse a given table version specifier.
    ///
    /// For now it only supports timestamp versioning for BigQuery and MSSQL dialects.
    pub fn parse_table_version(&mut self) -> Result<Option<TableVersion>, ParserError> {
        if dialect_of!(self is BigQueryDialect | MsSqlDialect)
            && self.parse_keywords(&[Keyword::FOR, Keyword::SYSTEM_TIME, Keyword::AS, Keyword::OF])
        {
            let expr = self.parse_expr()?;
            Ok(Some(TableVersion::ForSystemTimeAsOf(expr)))
        } else {
            Ok(None)
        }
    }

    /// Parse an [`WildcardAdditionalOptions`] information for wildcard select items.
    ///
    /// If it is not possible to parse it, will return an option.
    pub fn parse_wildcard_additional_options(
        &mut self,
        wildcard_token: TokenWithSpan,
    ) -> Result<WildcardAdditionalOptions, ParserError> {
        let opt_ilike = if dialect_of!(self is GenericDialect | SnowflakeDialect) {
            self.parse_optional_select_item_ilike()?
        } else {
            None
        };
        let opt_exclude = if opt_ilike.is_none()
            && dialect_of!(self is GenericDialect | DuckDbDialect | SnowflakeDialect)
        {
            self.parse_optional_select_item_exclude()?
        } else {
            None
        };
        let opt_except = if self.dialect.supports_select_wildcard_except() {
            self.parse_optional_select_item_except()?
        } else {
            None
        };
        let opt_replace = if dialect_of!(self is GenericDialect | BigQueryDialect | ClickHouseDialect | DuckDbDialect | SnowflakeDialect)
        {
            self.parse_optional_select_item_replace()?
        } else {
            None
        };
        let opt_rename = if dialect_of!(self is GenericDialect | SnowflakeDialect) {
            self.parse_optional_select_item_rename()?
        } else {
            None
        };

        Ok(WildcardAdditionalOptions {
            wildcard_token: wildcard_token.into(),
            opt_ilike,
            opt_exclude,
            opt_except,
            opt_rename,
            opt_replace,
        })
    }

    // Parse a WITH FILL clause (ClickHouse dialect)
    // that follow the WITH FILL keywords in a ORDER BY clause
    pub fn parse_with_fill(&mut self) -> Result<WithFill, ParserError> {
        let from = if self.parse_keyword(Keyword::FROM) {
            Some(self.parse_expr()?)
        } else {
            None
        };

        let to = if self.parse_keyword(Keyword::TO) {
            Some(self.parse_expr()?)
        } else {
            None
        };

        let step = if self.parse_keyword(Keyword::STEP) {
            Some(self.parse_expr()?)
        } else {
            None
        };

        Ok(WithFill { from, to, step })
    }

    fn parse_settings(&mut self) -> Result<Option<Vec<Setting>>, ParserError> {
        let settings = if dialect_of!(self is ClickHouseDialect|GenericDialect)
            && self.parse_keyword(Keyword::SETTINGS)
        {
            let key_values = self.parse_comma_separated(|p| {
                let key = p.parse_identifier(false)?;
                p.expect_token(&Token::Eq)?;
                let value = p.parse_value()?;
                Ok(Setting { key, value })
            })?;
            Some(key_values)
        } else {
            None
        };
        Ok(settings)
    }

    /// Parse an UPDATE statement, returning a `Box`ed SetExpr
    ///
    /// This is used to reduce the size of the stack frames in debug builds
    fn parse_update_setexpr_boxed(&mut self) -> Result<Box<SetExpr>, ParserError> {
        Ok(Box::new(SetExpr::Update(self.parse_update()?)))
    }

    /// Parse a "query body", which is an expression with roughly the
    /// following grammar:
    /// ```sql
    ///   query_body ::= restricted_select | '(' subquery ')' | set_operation
    ///   restricted_select ::= 'SELECT' [expr_list] [ from ] [ where ] [ groupby_having ]
    ///   subquery ::= query_body [ order_by_limit ]
    ///   set_operation ::= query_body { 'UNION' | 'EXCEPT' | 'INTERSECT' } [ 'ALL' ] query_body
    /// ```
    pub fn parse_query_body(&mut self, precedence: u8) -> Result<Box<SetExpr>, ParserError> {
        // We parse the expression using a Pratt parser, as in `parse_expr()`.
        // Start by parsing a restricted SELECT or a `(subquery)`:
        let expr = if self.peek_keyword(Keyword::SELECT) {
            SetExpr::Select(self.parse_select().map(Box::new)?)
        } else if self.consume_token(&Token::LParen) {
            // CTEs are not allowed here, but the parser currently accepts them
            let subquery = self.parse_query()?;
            self.expect_token(&Token::RParen)?;
            SetExpr::Query(subquery)
        } else if self.parse_keyword(Keyword::VALUES) {
            let is_mysql = dialect_of!(self is MySqlDialect);
            SetExpr::Values(self.parse_values(is_mysql)?)
        } else if self.parse_keyword(Keyword::TABLE) {
            SetExpr::Table(Box::new(self.parse_as_table()?))
        } else {
            return self.expected(
                "SELECT, VALUES, or a subquery in the query body",
                self.peek_token(),
            );
        };

        self.parse_remaining_set_exprs(expr, precedence)
    }

    /// Parse any extra set expressions that may be present in a query body
    ///
    /// (this is its own function to reduce required stack size in debug builds)
    fn parse_remaining_set_exprs(
        &mut self,
        mut expr: SetExpr,
        precedence: u8,
    ) -> Result<Box<SetExpr>, ParserError> {
        loop {
            // The query can be optionally followed by a set operator:
            let op = self.parse_set_operator(&self.peek_token().token);
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
            let set_quantifier = self.parse_set_quantifier(&op);
            expr = SetExpr::SetOperation {
                left: Box::new(expr),
                op: op.unwrap(),
                set_quantifier,
                right: self.parse_query_body(next_precedence)?,
            };
        }

        Ok(expr.into())
    }

    pub fn parse_set_operator(&mut self, token: &Token) -> Option<SetOperator> {
        match token {
            Token::Word(w) if w.keyword == Keyword::UNION => Some(SetOperator::Union),
            Token::Word(w) if w.keyword == Keyword::EXCEPT => Some(SetOperator::Except),
            Token::Word(w) if w.keyword == Keyword::INTERSECT => Some(SetOperator::Intersect),
            _ => None,
        }
    }

    pub fn parse_set_quantifier(&mut self, op: &Option<SetOperator>) -> SetQuantifier {
        match op {
            Some(SetOperator::Except | SetOperator::Intersect | SetOperator::Union) => {
                if self.parse_keywords(&[Keyword::DISTINCT, Keyword::BY, Keyword::NAME]) {
                    SetQuantifier::DistinctByName
                } else if self.parse_keywords(&[Keyword::BY, Keyword::NAME]) {
                    SetQuantifier::ByName
                } else if self.parse_keyword(Keyword::ALL) {
                    if self.parse_keywords(&[Keyword::BY, Keyword::NAME]) {
                        SetQuantifier::AllByName
                    } else {
                        SetQuantifier::All
                    }
                } else if self.parse_keyword(Keyword::DISTINCT) {
                    SetQuantifier::Distinct
                } else {
                    SetQuantifier::None
                }
            }
            _ => SetQuantifier::None,
        }
    }

    /// Parse a restricted `SELECT` statement (no CTEs / `UNION` / `ORDER BY`)
    pub fn parse_select(&mut self) -> Result<Select, ParserError> {
        let select_token = self.expect_keyword(Keyword::SELECT)?;
        let value_table_mode =
            if dialect_of!(self is BigQueryDialect) && self.parse_keyword(Keyword::AS) {
                if self.parse_keyword(Keyword::VALUE) {
                    Some(ValueTableMode::AsValue)
                } else if self.parse_keyword(Keyword::STRUCT) {
                    Some(ValueTableMode::AsStruct)
                } else {
                    self.expected("VALUE or STRUCT", self.peek_token())?
                }
            } else {
                None
            };

        let mut top_before_distinct = false;
        let mut top = None;
        if self.dialect.supports_top_before_distinct() && self.parse_keyword(Keyword::TOP) {
            top = Some(self.parse_top()?);
            top_before_distinct = true;
        }
        let distinct = self.parse_all_or_distinct()?;
        if !self.dialect.supports_top_before_distinct() && self.parse_keyword(Keyword::TOP) {
            top = Some(self.parse_top()?);
        }

        let projection = self.parse_projection()?;

        let into = if self.parse_keyword(Keyword::INTO) {
            let temporary = self
                .parse_one_of_keywords(&[Keyword::TEMP, Keyword::TEMPORARY])
                .is_some();
            let unlogged = self.parse_keyword(Keyword::UNLOGGED);
            let table = self.parse_keyword(Keyword::TABLE);
            let name = self.parse_object_name(false)?;
            Some(SelectInto {
                temporary,
                unlogged,
                table,
                name,
            })
        } else {
            None
        };

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
                let lateral_view_name = self.parse_object_name(false)?;
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

        let prewhere = if dialect_of!(self is ClickHouseDialect|GenericDialect)
            && self.parse_keyword(Keyword::PREWHERE)
        {
            Some(self.parse_expr()?)
        } else {
            None
        };

        let selection = if self.parse_keyword(Keyword::WHERE) {
            Some(self.parse_expr()?)
        } else {
            None
        };

        let group_by = self
            .parse_optional_group_by()?
            .unwrap_or_else(|| GroupByExpr::Expressions(vec![], vec![]));

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

        // Accept QUALIFY and WINDOW in any order and flag accordingly.
        let (named_windows, qualify, window_before_qualify) = if self.parse_keyword(Keyword::WINDOW)
        {
            let named_windows = self.parse_comma_separated(Parser::parse_named_window)?;
            if self.parse_keyword(Keyword::QUALIFY) {
                (named_windows, Some(self.parse_expr()?), true)
            } else {
                (named_windows, None, true)
            }
        } else if self.parse_keyword(Keyword::QUALIFY) {
            let qualify = Some(self.parse_expr()?);
            if self.parse_keyword(Keyword::WINDOW) {
                (
                    self.parse_comma_separated(Parser::parse_named_window)?,
                    qualify,
                    false,
                )
            } else {
                (Default::default(), qualify, false)
            }
        } else {
            Default::default()
        };

        let connect_by = if self.dialect.supports_connect_by()
            && self
                .parse_one_of_keywords(&[Keyword::START, Keyword::CONNECT])
                .is_some()
        {
            self.prev_token();
            Some(self.parse_connect_by()?)
        } else {
            None
        };

        Ok(Select {
            select_token: AttachedToken(select_token),
            distinct,
            top,
            top_before_distinct,
            projection,
            into,
            from,
            lateral_views,
            prewhere,
            selection,
            group_by,
            cluster_by,
            distribute_by,
            sort_by,
            having,
            named_window: named_windows,
            window_before_qualify,
            qualify,
            value_table_mode,
            connect_by,
        })
    }

    /// Parse an OFFSET clause
    pub fn parse_offset(&mut self) -> Result<Offset, ParserError> {
        let value = self.parse_expr()?;
        let rows = if self.parse_keyword(Keyword::ROW) {
            OffsetRows::Row
        } else if self.parse_keyword(Keyword::ROWS) {
            OffsetRows::Rows
        } else {
            OffsetRows::None
        };
        Ok(Offset { value, rows })
    }

    pub fn parse_pivot_table_factor(
        &mut self,
        table: TableFactor,
    ) -> Result<TableFactor, ParserError> {
        self.expect_token(&Token::LParen)?;
        let aggregate_functions = self.parse_comma_separated(Self::parse_aliased_function_call)?;
        self.expect_keyword(Keyword::FOR)?;
        let value_column = self.parse_object_name(false)?.0;
        self.expect_keyword(Keyword::IN)?;

        self.expect_token(&Token::LParen)?;
        let value_source = if self.parse_keyword(Keyword::ANY) {
            let order_by = if self.parse_keywords(&[Keyword::ORDER, Keyword::BY]) {
                self.parse_comma_separated(Parser::parse_order_by_expr)?
            } else {
                vec![]
            };
            PivotValueSource::Any(order_by)
        } else if self.peek_sub_query() {
            PivotValueSource::Subquery(self.parse_query()?)
        } else {
            PivotValueSource::List(self.parse_comma_separated(Self::parse_expr_with_alias)?)
        };
        self.expect_token(&Token::RParen)?;

        let default_on_null =
            if self.parse_keywords(&[Keyword::DEFAULT, Keyword::ON, Keyword::NULL]) {
                self.expect_token(&Token::LParen)?;
                let expr = self.parse_expr()?;
                self.expect_token(&Token::RParen)?;
                Some(expr)
            } else {
                None
            };

        self.expect_token(&Token::RParen)?;
        let alias = self.parse_optional_table_alias(keywords::RESERVED_FOR_TABLE_ALIAS)?;
        Ok(TableFactor::Pivot {
            table: Box::new(table),
            aggregate_functions,
            value_column,
            value_source,
            default_on_null,
            alias,
        })
    }

    /// Parse a TOP clause, MSSQL equivalent of LIMIT,
    /// that follows after `SELECT [DISTINCT]`.
    pub fn parse_top(&mut self) -> Result<Top, ParserError> {
        let quantity = if self.consume_token(&Token::LParen) {
            let quantity = self.parse_expr()?;
            self.expect_token(&Token::RParen)?;
            Some(TopQuantity::Expr(quantity))
        } else {
            let next_token = self.next_token();
            let quantity = match next_token.token {
                Token::Number(s, _) => Self::parse::<u64>(s, next_token.span.start)?,
                _ => self.expected("literal int", next_token)?,
            };
            Some(TopQuantity::Constant(quantity))
        };

        let percent = self.parse_keyword(Keyword::PERCENT);

        let with_ties = self.parse_keywords(&[Keyword::WITH, Keyword::TIES]);

        Ok(Top {
            with_ties,
            percent,
            quantity,
        })
    }

    /// A table name or a parenthesized subquery, followed by optional `[AS] alias`
    pub fn parse_table_factor(&mut self) -> Result<TableFactor, ParserError> {
        if self.parse_keyword(Keyword::LATERAL) {
            // LATERAL must always be followed by a subquery or table function.
            if self.consume_token(&Token::LParen) {
                self.parse_derived_table_factor(Lateral)
            } else {
                let name = self.parse_object_name(false)?;
                self.expect_token(&Token::LParen)?;
                let args = self.parse_optional_args()?;
                let alias = self.parse_optional_table_alias(keywords::RESERVED_FOR_TABLE_ALIAS)?;
                Ok(TableFactor::Function {
                    lateral: true,
                    name,
                    args,
                    alias,
                })
            }
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
            if let Some(mut table) =
                self.maybe_parse(|parser| parser.parse_derived_table_factor(NotLateral))?
            {
                while let Some(kw) = self.parse_one_of_keywords(&[Keyword::PIVOT, Keyword::UNPIVOT])
                {
                    table = match kw {
                        Keyword::PIVOT => self.parse_pivot_table_factor(table)?,
                        Keyword::UNPIVOT => self.parse_unpivot_table_factor(table)?,
                        _ => unreachable!(),
                    }
                }
                return Ok(table);
            }

            // A parsing error from `parse_derived_table_factor` indicates that the '(' we've
            // recently consumed does not start a derived table (cases 1, 2, or 4).
            // `maybe_parse` will ignore such an error and rewind to be after the opening '('.

            // Inside the parentheses we expect to find an (A) table factor
            // followed by some joins or (B) another level of nesting.
            let mut table_and_joins = self.parse_table_and_joins()?;

            #[allow(clippy::if_same_then_else)]
            if !table_and_joins.joins.is_empty() {
                self.expect_token(&Token::RParen)?;
                let alias = self.parse_optional_table_alias(keywords::RESERVED_FOR_TABLE_ALIAS)?;
                Ok(TableFactor::NestedJoin {
                    table_with_joins: Box::new(table_and_joins),
                    alias,
                }) // (A)
            } else if let TableFactor::NestedJoin {
                table_with_joins: _,
                alias: _,
            } = &table_and_joins.relation
            {
                // (B): `table_and_joins` (what we found inside the parentheses)
                // is a nested join `(foo JOIN bar)`, not followed by other joins.
                self.expect_token(&Token::RParen)?;
                let alias = self.parse_optional_table_alias(keywords::RESERVED_FOR_TABLE_ALIAS)?;
                Ok(TableFactor::NestedJoin {
                    table_with_joins: Box::new(table_and_joins),
                    alias,
                })
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
                        | TableFactor::Function { alias, .. }
                        | TableFactor::UNNEST { alias, .. }
                        | TableFactor::JsonTable { alias, .. }
                        | TableFactor::OpenJsonTable { alias, .. }
                        | TableFactor::TableFunction { alias, .. }
                        | TableFactor::Pivot { alias, .. }
                        | TableFactor::Unpivot { alias, .. }
                        | TableFactor::MatchRecognize { alias, .. }
                        | TableFactor::NestedJoin { alias, .. } => {
                            // but not `FROM (mytable AS alias1) AS alias2`.
                            if let Some(inner_alias) = alias {
                                return Err(ParserError::ParserError(format!(
                                    "duplicate alias {inner_alias}"
                                )));
                            }
                            // Act as if the alias was specified normally next
                            // to the table name: `(mytable) AS alias` ->
                            // `(mytable AS alias)`
                            alias.replace(outer_alias);
                        }
                    };
                }
                // Do not store the extra set of parens in the AST
                Ok(table_and_joins.relation)
            } else {
                // The SQL spec prohibits derived tables and bare tables from
                // appearing alone in parentheses (e.g. `FROM (mytable)`)
                self.expected("joined table", self.peek_token())
            }
        } else if dialect_of!(self is SnowflakeDialect | DatabricksDialect | GenericDialect)
            && matches!(
                self.peek_tokens(),
                [
                    Token::Word(Word {
                        keyword: Keyword::VALUES,
                        ..
                    }),
                    Token::LParen
                ]
            )
        {
            self.expect_keyword(Keyword::VALUES)?;

            // Snowflake and Databricks allow syntax like below:
            // SELECT * FROM VALUES (1, 'a'), (2, 'b') AS t (col1, col2)
            // where there are no parentheses around the VALUES clause.
            let values = SetExpr::Values(self.parse_values(false)?);
            let alias = self.parse_optional_table_alias(keywords::RESERVED_FOR_TABLE_ALIAS)?;
            Ok(TableFactor::Derived {
                lateral: false,
                subquery: Box::new(Query {
                    with: None,
                    body: Box::new(values),
                    order_by: None,
                    limit: None,
                    limit_by: vec![],
                    offset: None,
                    fetch: None,
                    locks: vec![],
                    for_clause: None,
                    settings: None,
                    format_clause: None,
                }),
                alias,
            })
        } else if dialect_of!(self is BigQueryDialect | PostgreSqlDialect | GenericDialect)
            && self.parse_keyword(Keyword::UNNEST)
        {
            self.expect_token(&Token::LParen)?;
            let array_exprs = self.parse_comma_separated(Parser::parse_expr)?;
            self.expect_token(&Token::RParen)?;

            let with_ordinality = self.parse_keywords(&[Keyword::WITH, Keyword::ORDINALITY]);
            let alias = match self.parse_optional_table_alias(keywords::RESERVED_FOR_TABLE_ALIAS) {
                Ok(Some(alias)) => Some(alias),
                Ok(None) => None,
                Err(e) => return Err(e),
            };

            let with_offset = match self.expect_keywords(&[Keyword::WITH, Keyword::OFFSET]) {
                Ok(()) => true,
                Err(_) => false,
            };

            let with_offset_alias = if with_offset {
                match self.parse_optional_alias(keywords::RESERVED_FOR_COLUMN_ALIAS) {
                    Ok(Some(alias)) => Some(alias),
                    Ok(None) => None,
                    Err(e) => return Err(e),
                }
            } else {
                None
            };

            Ok(TableFactor::UNNEST {
                alias,
                array_exprs,
                with_offset,
                with_offset_alias,
                with_ordinality,
            })
        } else if self.parse_keyword_with_tokens(Keyword::JSON_TABLE, &[Token::LParen]) {
            let json_expr = self.parse_expr()?;
            self.expect_token(&Token::Comma)?;
            let json_path = self.parse_value()?;
            self.expect_keyword(Keyword::COLUMNS)?;
            self.expect_token(&Token::LParen)?;
            let columns = self.parse_comma_separated(Parser::parse_json_table_column_def)?;
            self.expect_token(&Token::RParen)?;
            self.expect_token(&Token::RParen)?;
            let alias = self.parse_optional_table_alias(keywords::RESERVED_FOR_TABLE_ALIAS)?;
            Ok(TableFactor::JsonTable {
                json_expr,
                json_path,
                columns,
                alias,
            })
        } else if self.parse_keyword_with_tokens(Keyword::OPENJSON, &[Token::LParen]) {
            self.prev_token();
            self.parse_open_json_table_factor()
        } else {
            let name = self.parse_object_name(true)?;

            let json_path = match self.peek_token().token {
                Token::LBracket if self.dialect.supports_partiql() => Some(self.parse_json_path()?),
                _ => None,
            };

            let partitions: Vec<Ident> = if dialect_of!(self is MySqlDialect | GenericDialect)
                && self.parse_keyword(Keyword::PARTITION)
            {
                self.parse_parenthesized_identifiers()?
            } else {
                vec![]
            };

            // Parse potential version qualifier
            let version = self.parse_table_version()?;

            // Postgres, MSSQL, ClickHouse: table-valued functions:
            let args = if self.consume_token(&Token::LParen) {
                Some(self.parse_table_function_args()?)
            } else {
                None
            };

            let with_ordinality = self.parse_keywords(&[Keyword::WITH, Keyword::ORDINALITY]);

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

            let mut table = TableFactor::Table {
                name,
                alias,
                args,
                with_hints,
                version,
                partitions,
                with_ordinality,
                json_path,
            };

            while let Some(kw) = self.parse_one_of_keywords(&[Keyword::PIVOT, Keyword::UNPIVOT]) {
                table = match kw {
                    Keyword::PIVOT => self.parse_pivot_table_factor(table)?,
                    Keyword::UNPIVOT => self.parse_unpivot_table_factor(table)?,
                    _ => unreachable!(),
                }
            }

            if self.dialect.supports_match_recognize()
                && self.parse_keyword(Keyword::MATCH_RECOGNIZE)
            {
                table = self.parse_match_recognize(table)?;
            }

            Ok(table)
        }
    }

    pub fn parse_unpivot_table_factor(
        &mut self,
        table: TableFactor,
    ) -> Result<TableFactor, ParserError> {
        self.expect_token(&Token::LParen)?;
        let value = self.parse_identifier(false)?;
        self.expect_keyword(Keyword::FOR)?;
        let name = self.parse_identifier(false)?;
        self.expect_keyword(Keyword::IN)?;
        let columns = self.parse_parenthesized_column_list(Mandatory, false)?;
        self.expect_token(&Token::RParen)?;
        let alias = self.parse_optional_table_alias(keywords::RESERVED_FOR_TABLE_ALIAS)?;
        Ok(TableFactor::Unpivot {
            table: Box::new(table),
            value,
            name,
            columns,
            alias,
        })
    }

    /// Parses `OPENJSON( jsonExpression [ , path ] )  [ <with_clause> ]` clause,
    /// assuming the `OPENJSON` keyword was already consumed.
    fn parse_open_json_table_factor(&mut self) -> Result<TableFactor, ParserError> {
        self.expect_token(&Token::LParen)?;
        let json_expr = self.parse_expr()?;
        let json_path = if self.consume_token(&Token::Comma) {
            Some(self.parse_value()?)
        } else {
            None
        };
        self.expect_token(&Token::RParen)?;
        let columns = if self.parse_keyword(Keyword::WITH) {
            self.expect_token(&Token::LParen)?;
            let columns = self.parse_comma_separated(Parser::parse_openjson_table_column_def)?;
            self.expect_token(&Token::RParen)?;
            columns
        } else {
            Vec::new()
        };
        let alias = self.parse_optional_table_alias(keywords::RESERVED_FOR_TABLE_ALIAS)?;
        Ok(TableFactor::OpenJsonTable {
            json_expr,
            json_path,
            columns,
            alias,
        })
    }

    fn parse_match_recognize(&mut self, table: TableFactor) -> Result<TableFactor, ParserError> {
        self.expect_token(&Token::LParen)?;

        let partition_by = if self.parse_keywords(&[Keyword::PARTITION, Keyword::BY]) {
            self.parse_comma_separated(Parser::parse_expr)?
        } else {
            vec![]
        };

        let order_by = if self.parse_keywords(&[Keyword::ORDER, Keyword::BY]) {
            self.parse_comma_separated(Parser::parse_order_by_expr)?
        } else {
            vec![]
        };

        let measures = if self.parse_keyword(Keyword::MEASURES) {
            self.parse_comma_separated(|p| {
                let expr = p.parse_expr()?;
                let _ = p.parse_keyword(Keyword::AS);
                let alias = p.parse_identifier(false)?;
                Ok(Measure { expr, alias })
            })?
        } else {
            vec![]
        };

        let rows_per_match =
            if self.parse_keywords(&[Keyword::ONE, Keyword::ROW, Keyword::PER, Keyword::MATCH]) {
                Some(RowsPerMatch::OneRow)
            } else if self.parse_keywords(&[
                Keyword::ALL,
                Keyword::ROWS,
                Keyword::PER,
                Keyword::MATCH,
            ]) {
                Some(RowsPerMatch::AllRows(
                    if self.parse_keywords(&[Keyword::SHOW, Keyword::EMPTY, Keyword::MATCHES]) {
                        Some(EmptyMatchesMode::Show)
                    } else if self.parse_keywords(&[
                        Keyword::OMIT,
                        Keyword::EMPTY,
                        Keyword::MATCHES,
                    ]) {
                        Some(EmptyMatchesMode::Omit)
                    } else if self.parse_keywords(&[
                        Keyword::WITH,
                        Keyword::UNMATCHED,
                        Keyword::ROWS,
                    ]) {
                        Some(EmptyMatchesMode::WithUnmatched)
                    } else {
                        None
                    },
                ))
            } else {
                None
            };

        let after_match_skip =
            if self.parse_keywords(&[Keyword::AFTER, Keyword::MATCH, Keyword::SKIP]) {
                if self.parse_keywords(&[Keyword::PAST, Keyword::LAST, Keyword::ROW]) {
                    Some(AfterMatchSkip::PastLastRow)
                } else if self.parse_keywords(&[Keyword::TO, Keyword::NEXT, Keyword::ROW]) {
                    Some(AfterMatchSkip::ToNextRow)
                } else if self.parse_keywords(&[Keyword::TO, Keyword::FIRST]) {
                    Some(AfterMatchSkip::ToFirst(self.parse_identifier(false)?))
                } else if self.parse_keywords(&[Keyword::TO, Keyword::LAST]) {
                    Some(AfterMatchSkip::ToLast(self.parse_identifier(false)?))
                } else {
                    let found = self.next_token();
                    return self.expected("after match skip option", found);
                }
            } else {
                None
            };

        self.expect_keyword(Keyword::PATTERN)?;
        let pattern = self.parse_parenthesized(Self::parse_pattern)?;

        self.expect_keyword(Keyword::DEFINE)?;

        let symbols = self.parse_comma_separated(|p| {
            let symbol = p.parse_identifier(false)?;
            p.expect_keyword(Keyword::AS)?;
            let definition = p.parse_expr()?;
            Ok(SymbolDefinition { symbol, definition })
        })?;

        self.expect_token(&Token::RParen)?;

        let alias = self.parse_optional_table_alias(keywords::RESERVED_FOR_TABLE_ALIAS)?;

        Ok(TableFactor::MatchRecognize {
            table: Box::new(table),
            partition_by,
            order_by,
            measures,
            rows_per_match,
            after_match_skip,
            pattern,
            symbols,
            alias,
        })
    }

    fn parse_base_pattern(&mut self) -> Result<MatchRecognizePattern, ParserError> {
        match self.next_token().token {
            Token::Caret => Ok(MatchRecognizePattern::Symbol(MatchRecognizeSymbol::Start)),
            Token::Placeholder(s) if s == "$" => {
                Ok(MatchRecognizePattern::Symbol(MatchRecognizeSymbol::End))
            }
            Token::LBrace => {
                self.expect_token(&Token::Minus)?;
                let symbol = self
                    .parse_identifier(false)
                    .map(MatchRecognizeSymbol::Named)?;
                self.expect_token(&Token::Minus)?;
                self.expect_token(&Token::RBrace)?;
                Ok(MatchRecognizePattern::Exclude(symbol))
            }
            Token::Word(Word {
                value,
                quote_style: None,
                ..
            }) if value == "PERMUTE" => {
                self.expect_token(&Token::LParen)?;
                let symbols = self.parse_comma_separated(|p| {
                    p.parse_identifier(false).map(MatchRecognizeSymbol::Named)
                })?;
                self.expect_token(&Token::RParen)?;
                Ok(MatchRecognizePattern::Permute(symbols))
            }
            Token::LParen => {
                let pattern = self.parse_pattern()?;
                self.expect_token(&Token::RParen)?;
                Ok(MatchRecognizePattern::Group(Box::new(pattern)))
            }
            _ => {
                self.prev_token();
                self.parse_identifier(false)
                    .map(MatchRecognizeSymbol::Named)
                    .map(MatchRecognizePattern::Symbol)
            }
        }
    }

    fn parse_json_table_column_error_handling(
        &mut self,
    ) -> Result<Option<JsonTableColumnErrorHandling>, ParserError> {
        let res = if self.parse_keyword(Keyword::NULL) {
            JsonTableColumnErrorHandling::Null
        } else if self.parse_keyword(Keyword::ERROR) {
            JsonTableColumnErrorHandling::Error
        } else if self.parse_keyword(Keyword::DEFAULT) {
            JsonTableColumnErrorHandling::Default(self.parse_value()?)
        } else {
            return Ok(None);
        };
        self.expect_keyword(Keyword::ON)?;
        Ok(Some(res))
    }

    fn parse_repetition_pattern(&mut self) -> Result<MatchRecognizePattern, ParserError> {
        let mut pattern = self.parse_base_pattern()?;
        loop {
            let token = self.next_token();
            let quantifier = match token.token {
                Token::Mul => RepetitionQuantifier::ZeroOrMore,
                Token::Plus => RepetitionQuantifier::OneOrMore,
                Token::Placeholder(s) if s == "?" => RepetitionQuantifier::AtMostOne,
                Token::LBrace => {
                    // quantifier is a range like {n} or {n,} or {,m} or {n,m}
                    let token = self.next_token();
                    match token.token {
                        Token::Comma => {
                            let next_token = self.next_token();
                            let Token::Number(n, _) = next_token.token else {
                                return self.expected("literal number", next_token);
                            };
                            self.expect_token(&Token::RBrace)?;
                            RepetitionQuantifier::AtMost(Self::parse(n, token.span.start)?)
                        }
                        Token::Number(n, _) if self.consume_token(&Token::Comma) => {
                            let next_token = self.next_token();
                            match next_token.token {
                                Token::Number(m, _) => {
                                    self.expect_token(&Token::RBrace)?;
                                    RepetitionQuantifier::Range(
                                        Self::parse(n, token.span.start)?,
                                        Self::parse(m, token.span.start)?,
                                    )
                                }
                                Token::RBrace => {
                                    RepetitionQuantifier::AtLeast(Self::parse(n, token.span.start)?)
                                }
                                _ => {
                                    return self.expected("} or upper bound", next_token);
                                }
                            }
                        }
                        Token::Number(n, _) => {
                            self.expect_token(&Token::RBrace)?;
                            RepetitionQuantifier::Exactly(Self::parse(n, token.span.start)?)
                        }
                        _ => return self.expected("quantifier range", token),
                    }
                }
                _ => {
                    self.prev_token();
                    break;
                }
            };
            pattern = MatchRecognizePattern::Repetition(Box::new(pattern), quantifier);
        }
        Ok(pattern)
    }

    fn parse_concat_pattern(&mut self) -> Result<MatchRecognizePattern, ParserError> {
        let mut patterns = vec![self.parse_repetition_pattern()?];
        while !matches!(self.peek_token().token, Token::RParen | Token::Pipe) {
            patterns.push(self.parse_repetition_pattern()?);
        }
        match <[MatchRecognizePattern; 1]>::try_from(patterns) {
            Ok([pattern]) => Ok(pattern),
            Err(patterns) => Ok(MatchRecognizePattern::Concat(patterns)),
        }
    }

    fn parse_pattern(&mut self) -> Result<MatchRecognizePattern, ParserError> {
        let pattern = self.parse_concat_pattern()?;
        if self.consume_token(&Token::Pipe) {
            match self.parse_pattern()? {
                // flatten nested alternations
                MatchRecognizePattern::Alternation(mut patterns) => {
                    patterns.insert(0, pattern);
                    Ok(MatchRecognizePattern::Alternation(patterns))
                }
                next => Ok(MatchRecognizePattern::Alternation(vec![pattern, next])),
            }
        } else {
            Ok(pattern)
        }
    }

    pub fn parse_optional_args(&mut self) -> Result<Vec<FunctionArg>, ParserError> {
        if self.consume_token(&Token::RParen) {
            Ok(vec![])
        } else {
            let args = self.parse_comma_separated(Parser::parse_function_args)?;
            self.expect_token(&Token::RParen)?;
            Ok(args)
        }
    }

    fn parse_aliased_function_call(&mut self) -> Result<ExprWithAlias, ParserError> {
        let function_name = match self.next_token().token {
            Token::Word(w) => Ok(w.value),
            _ => self.expected("a function identifier", self.peek_token()),
        }?;
        let expr = self.parse_function(ObjectName(vec![Ident::new(function_name)]))?;
        let alias = if self.parse_keyword(Keyword::AS) {
            Some(self.parse_identifier(false)?)
        } else {
            None
        };

        Ok(ExprWithAlias { expr, alias })
    }

    fn parse_function_named_arg_operator(&mut self) -> Result<FunctionArgOperator, ParserError> {
        if self.parse_keyword(Keyword::VALUE) {
            return Ok(FunctionArgOperator::Value);
        }
        let tok = self.next_token();
        match tok.token {
            Token::RArrow if self.dialect.supports_named_fn_args_with_rarrow_operator() => {
                Ok(FunctionArgOperator::RightArrow)
            }
            Token::Eq if self.dialect.supports_named_fn_args_with_eq_operator() => {
                Ok(FunctionArgOperator::Equals)
            }
            Token::Assignment
                if self
                    .dialect
                    .supports_named_fn_args_with_assignment_operator() =>
            {
                Ok(FunctionArgOperator::Assignment)
            }
            Token::Colon if self.dialect.supports_named_fn_args_with_colon_operator() => {
                Ok(FunctionArgOperator::Colon)
            }
            _ => {
                self.prev_token();
                self.expected("argument operator", tok)
            }
        }
    }

    fn parse_table_function_args(&mut self) -> Result<TableFunctionArgs, ParserError> {
        if self.consume_token(&Token::RParen) {
            return Ok(TableFunctionArgs {
                args: vec![],
                settings: None,
            });
        }
        let mut args = vec![];
        let settings = loop {
            if let Some(settings) = self.parse_settings()? {
                break Some(settings);
            }
            args.push(self.parse_function_args()?);
            if self.is_parse_comma_separated_end() {
                break None;
            }
        };
        self.expect_token(&Token::RParen)?;
        Ok(TableFunctionArgs { args, settings })
    }

    /// Parse a parenthesized comma-separated list of table alias column definitions.
    fn parse_table_alias_column_defs(&mut self) -> Result<Vec<TableAliasColumnDef>, ParserError> {
        if self.consume_token(&Token::LParen) {
            let cols = self.parse_comma_separated(|p| {
                let name = p.parse_identifier(false)?;
                let data_type = p.maybe_parse(|p| p.parse_data_type())?;
                Ok(TableAliasColumnDef { name, data_type })
            })?;
            self.expect_token(&Token::RParen)?;
            Ok(cols)
        } else {
            Ok(vec![])
        }
    }

    /// Invoke `f` after first setting the parser's `ParserState` to `state`.
    ///
    /// Upon return, restores the parser's state to what it started at.
    fn with_state<T, F>(&mut self, state: ParserState, mut f: F) -> Result<T, ParserError>
    where
        F: FnMut(&mut Parser) -> Result<T, ParserError>,
    {
        let current_state = self.state;
        self.state = state;
        let res = f(self);
        self.state = current_state;
        res
    }
}

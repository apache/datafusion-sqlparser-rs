use super::*;

use crate::parser_err;

// Every Parser method with "parse" and "expr" in the name.

impl<'a> Parser<'a> {
    /// Parse a new expression.
    pub fn parse_expr(&mut self) -> Result<Expr, ParserError> {
        self.parse_subexpr(self.dialect.prec_unknown())
    }

    /// Parse tokens until the precedence changes.
    pub fn parse_subexpr(&mut self, precedence: u8) -> Result<Expr, ParserError> {
        let _guard = self.recursion_counter.try_decrease()?;
        debug!("parsing expr");
        let mut expr = self.parse_prefix()?;
        debug!("prefix: {:?}", expr);
        loop {
            let next_precedence = self.get_next_precedence()?;
            debug!("next precedence: {:?}", next_precedence);

            if precedence >= next_precedence {
                break;
            }

            expr = self.parse_infix(expr, next_precedence)?;
        }
        Ok(expr)
    }

    /// Parses an array expression `[ex1, ex2, ..]`
    /// if `named` is `true`, came from an expression like  `ARRAY[ex1, ex2]`
    pub fn parse_array_expr(&mut self, named: bool) -> Result<Expr, ParserError> {
        let exprs = self.parse_comma_separated0(Parser::parse_expr, Token::RBracket)?;
        self.expect_token(&Token::RBracket)?;
        Ok(Expr::Array(Array { elem: exprs, named }))
    }

    /// Parses `BETWEEN <low> AND <high>`, assuming the `BETWEEN` keyword was already consumed.
    pub fn parse_between(&mut self, expr: Expr, negated: bool) -> Result<Expr, ParserError> {
        // Stop parsing subexpressions for <low> and <high> on tokens with
        // precedence lower than that of `BETWEEN`, such as `AND`, `IS`, etc.
        let low = self.parse_subexpr(self.dialect.prec_value(Precedence::Between))?;
        self.expect_keyword(Keyword::AND)?;
        let high = self.parse_subexpr(self.dialect.prec_value(Precedence::Between))?;
        Ok(Expr::Between {
            expr: Box::new(expr),
            negated,
            low: Box::new(low),
            high: Box::new(high),
        })
    }

    pub fn parse_case_expr(&mut self) -> Result<Expr, ParserError> {
        let mut operand = None;
        if !self.parse_keyword(Keyword::WHEN) {
            operand = Some(Box::new(self.parse_expr()?));
            self.expect_keyword(Keyword::WHEN)?;
        }
        let mut conditions = vec![];
        let mut results = vec![];
        loop {
            conditions.push(self.parse_expr()?);
            self.expect_keyword(Keyword::THEN)?;
            results.push(self.parse_expr()?);
            if !self.parse_keyword(Keyword::WHEN) {
                break;
            }
        }
        let else_result = if self.parse_keyword(Keyword::ELSE) {
            Some(Box::new(self.parse_expr()?))
        } else {
            None
        };
        self.expect_keyword(Keyword::END)?;
        Ok(Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        })
    }

    /// Parse a SQL CAST function e.g. `CAST(expr AS FLOAT)`
    pub fn parse_cast_expr(&mut self, kind: CastKind) -> Result<Expr, ParserError> {
        self.expect_token(&Token::LParen)?;
        let expr = self.parse_expr()?;
        self.expect_keyword(Keyword::AS)?;
        let data_type = self.parse_data_type()?;
        let format = self.parse_optional_cast_format()?;
        self.expect_token(&Token::RParen)?;
        Ok(Expr::Cast {
            kind,
            expr: Box::new(expr),
            data_type,
            format,
        })
    }

    pub fn parse_ceil_floor_expr(&mut self, is_ceil: bool) -> Result<Expr, ParserError> {
        self.expect_token(&Token::LParen)?;
        let expr = self.parse_expr()?;
        // Parse `CEIL/FLOOR(expr)`
        let field = if self.parse_keyword(Keyword::TO) {
            // Parse `CEIL/FLOOR(expr TO DateTimeField)`
            CeilFloorKind::DateTimeField(self.parse_date_time_field()?)
        } else if self.consume_token(&Token::Comma) {
            // Parse `CEIL/FLOOR(expr, scale)`
            match self.parse_value()? {
                Value::Number(n, s) => CeilFloorKind::Scale(Value::Number(n, s)),
                _ => {
                    return Err(ParserError::ParserError(
                        "Scale field can only be of number type".to_string(),
                    ))
                }
            }
        } else {
            CeilFloorKind::DateTimeField(DateTimeField::NoDateTime)
        };
        self.expect_token(&Token::RParen)?;
        if is_ceil {
            Ok(Expr::Ceil {
                expr: Box::new(expr),
                field,
            })
        } else {
            Ok(Expr::Floor {
                expr: Box::new(expr),
                field,
            })
        }
    }

    pub fn parse_character_length(&mut self) -> Result<CharacterLength, ParserError> {
        if self.parse_keyword(Keyword::MAX) {
            return Ok(CharacterLength::Max);
        }
        let length = self.parse_literal_uint()?;
        let unit = if self.parse_keyword(Keyword::CHARACTERS) {
            Some(CharLengthUnits::Characters)
        } else if self.parse_keyword(Keyword::OCTETS) {
            Some(CharLengthUnits::Octets)
        } else {
            None
        };
        Ok(CharacterLength::IntegerLength { length, unit })
    }

    /// Parse a SQL CONVERT function:
    ///  - `CONVERT('héhé' USING utf8mb4)` (MySQL)
    ///  - `CONVERT('héhé', CHAR CHARACTER SET utf8mb4)` (MySQL)
    ///  - `CONVERT(DECIMAL(10, 5), 42)` (MSSQL) - the type comes first
    pub fn parse_convert_expr(&mut self, is_try: bool) -> Result<Expr, ParserError> {
        if self.dialect.convert_type_before_value() {
            return self.parse_mssql_convert(is_try);
        }
        self.expect_token(&Token::LParen)?;
        let expr = self.parse_expr()?;
        if self.parse_keyword(Keyword::USING) {
            let charset = self.parse_object_name(false)?;
            self.expect_token(&Token::RParen)?;
            return Ok(Expr::Convert {
                is_try,
                expr: Box::new(expr),
                data_type: None,
                charset: Some(charset),
                target_before_value: false,
                styles: vec![],
            });
        }
        self.expect_token(&Token::Comma)?;
        let data_type = self.parse_data_type()?;
        let charset = if self.parse_keywords(&[Keyword::CHARACTER, Keyword::SET]) {
            Some(self.parse_object_name(false)?)
        } else {
            None
        };
        self.expect_token(&Token::RParen)?;
        Ok(Expr::Convert {
            is_try,
            expr: Box::new(expr),
            data_type: Some(data_type),
            charset,
            target_before_value: false,
            styles: vec![],
        })
    }

    // This function parses date/time fields for the EXTRACT function-like
    // operator, interval qualifiers, and the ceil/floor operations.
    // EXTRACT supports a wider set of date/time fields than interval qualifiers,
    // so this function may need to be split in two.
    pub fn parse_date_time_field(&mut self) -> Result<DateTimeField, ParserError> {
        let next_token = self.next_token();
        match &next_token.token {
            Token::Word(w) => match w.keyword {
                Keyword::YEAR => Ok(DateTimeField::Year),
                Keyword::MONTH => Ok(DateTimeField::Month),
                Keyword::WEEK => {
                    let week_day = if dialect_of!(self is BigQueryDialect | GenericDialect)
                        && self.consume_token(&Token::LParen)
                    {
                        let week_day = self.parse_identifier(false)?;
                        self.expect_token(&Token::RParen)?;
                        Some(week_day)
                    } else {
                        None
                    };
                    Ok(DateTimeField::Week(week_day))
                }
                Keyword::DAY => Ok(DateTimeField::Day),
                Keyword::DAYOFWEEK => Ok(DateTimeField::DayOfWeek),
                Keyword::DAYOFYEAR => Ok(DateTimeField::DayOfYear),
                Keyword::DATE => Ok(DateTimeField::Date),
                Keyword::DATETIME => Ok(DateTimeField::Datetime),
                Keyword::HOUR => Ok(DateTimeField::Hour),
                Keyword::MINUTE => Ok(DateTimeField::Minute),
                Keyword::SECOND => Ok(DateTimeField::Second),
                Keyword::CENTURY => Ok(DateTimeField::Century),
                Keyword::DECADE => Ok(DateTimeField::Decade),
                Keyword::DOY => Ok(DateTimeField::Doy),
                Keyword::DOW => Ok(DateTimeField::Dow),
                Keyword::EPOCH => Ok(DateTimeField::Epoch),
                Keyword::ISODOW => Ok(DateTimeField::Isodow),
                Keyword::ISOYEAR => Ok(DateTimeField::Isoyear),
                Keyword::ISOWEEK => Ok(DateTimeField::IsoWeek),
                Keyword::JULIAN => Ok(DateTimeField::Julian),
                Keyword::MICROSECOND => Ok(DateTimeField::Microsecond),
                Keyword::MICROSECONDS => Ok(DateTimeField::Microseconds),
                Keyword::MILLENIUM => Ok(DateTimeField::Millenium),
                Keyword::MILLENNIUM => Ok(DateTimeField::Millennium),
                Keyword::MILLISECOND => Ok(DateTimeField::Millisecond),
                Keyword::MILLISECONDS => Ok(DateTimeField::Milliseconds),
                Keyword::NANOSECOND => Ok(DateTimeField::Nanosecond),
                Keyword::NANOSECONDS => Ok(DateTimeField::Nanoseconds),
                Keyword::QUARTER => Ok(DateTimeField::Quarter),
                Keyword::TIME => Ok(DateTimeField::Time),
                Keyword::TIMEZONE => Ok(DateTimeField::Timezone),
                Keyword::TIMEZONE_ABBR => Ok(DateTimeField::TimezoneAbbr),
                Keyword::TIMEZONE_HOUR => Ok(DateTimeField::TimezoneHour),
                Keyword::TIMEZONE_MINUTE => Ok(DateTimeField::TimezoneMinute),
                Keyword::TIMEZONE_REGION => Ok(DateTimeField::TimezoneRegion),
                _ if self.dialect.allow_extract_custom() => {
                    self.prev_token();
                    let custom = self.parse_identifier(false)?;
                    Ok(DateTimeField::Custom(custom))
                }
                _ => self.expected("date/time field", next_token),
            },
            Token::SingleQuotedString(_) if self.dialect.allow_extract_single_quotes() => {
                self.prev_token();
                let custom = self.parse_identifier(false)?;
                Ok(DateTimeField::Custom(custom))
            }
            _ => self.expected("date/time field", next_token),
        }
    }

    /// Parse the `ESCAPE CHAR` portion of `LIKE`, `ILIKE`, and `SIMILAR TO`
    pub fn parse_escape_char(&mut self) -> Result<Option<String>, ParserError> {
        if self.parse_keyword(Keyword::ESCAPE) {
            Ok(Some(self.parse_literal_string()?))
        } else {
            Ok(None)
        }
    }

    /// Parse a SQL EXISTS expression e.g. `WHERE EXISTS(SELECT ...)`.
    pub fn parse_exists_expr(&mut self, negated: bool) -> Result<Expr, ParserError> {
        self.expect_token(&Token::LParen)?;
        let exists_node = Expr::Exists {
            negated,
            subquery: self.parse_query()?,
        };
        self.expect_token(&Token::RParen)?;
        Ok(exists_node)
    }

    pub fn parse_extract_expr(&mut self) -> Result<Expr, ParserError> {
        self.expect_token(&Token::LParen)?;
        let field = self.parse_date_time_field()?;

        let syntax = if self.parse_keyword(Keyword::FROM) {
            ExtractSyntax::From
        } else if self.consume_token(&Token::Comma)
            && dialect_of!(self is SnowflakeDialect | GenericDialect)
        {
            ExtractSyntax::Comma
        } else {
            return Err(ParserError::ParserError(
                "Expected 'FROM' or ','".to_string(),
            ));
        };

        let expr = self.parse_expr()?;
        self.expect_token(&Token::RParen)?;
        Ok(Expr::Extract {
            field,
            expr: Box::new(expr),
            syntax,
        })
    }

    pub fn parse_function(&mut self, name: ObjectName) -> Result<Expr, ParserError> {
        self.expect_token(&Token::LParen)?;

        // Snowflake permits a subquery to be passed as an argument without
        // an enclosing set of parens if it's the only argument.
        if dialect_of!(self is SnowflakeDialect) && self.peek_sub_query() {
            let subquery = self.parse_query()?;
            self.expect_token(&Token::RParen)?;
            return Ok(Expr::Function(Function {
                name,
                parameters: FunctionArguments::None,
                args: FunctionArguments::Subquery(subquery),
                filter: None,
                null_treatment: None,
                over: None,
                within_group: vec![],
            }));
        }

        let mut args = self.parse_function_argument_list()?;
        let mut parameters = FunctionArguments::None;
        // ClickHouse aggregations support parametric functions like `HISTOGRAM(0.5, 0.6)(x, y)`
        // which (0.5, 0.6) is a parameter to the function.
        if dialect_of!(self is ClickHouseDialect | GenericDialect)
            && self.consume_token(&Token::LParen)
        {
            parameters = FunctionArguments::List(args);
            args = self.parse_function_argument_list()?;
        }

        let within_group = if self.parse_keywords(&[Keyword::WITHIN, Keyword::GROUP]) {
            self.expect_token(&Token::LParen)?;
            self.expect_keywords(&[Keyword::ORDER, Keyword::BY])?;
            let order_by = self.parse_comma_separated(Parser::parse_order_by_expr)?;
            self.expect_token(&Token::RParen)?;
            order_by
        } else {
            vec![]
        };

        let filter = if self.dialect.supports_filter_during_aggregation()
            && self.parse_keyword(Keyword::FILTER)
            && self.consume_token(&Token::LParen)
            && self.parse_keyword(Keyword::WHERE)
        {
            let filter = Some(Box::new(self.parse_expr()?));
            self.expect_token(&Token::RParen)?;
            filter
        } else {
            None
        };

        // Syntax for null treatment shows up either in the args list
        // or after the function call, but not both.
        let null_treatment = if args
            .clauses
            .iter()
            .all(|clause| !matches!(clause, FunctionArgumentClause::IgnoreOrRespectNulls(_)))
        {
            self.parse_null_treatment()?
        } else {
            None
        };

        let over = if self.parse_keyword(Keyword::OVER) {
            if self.consume_token(&Token::LParen) {
                let window_spec = self.parse_window_spec()?;
                Some(WindowType::WindowSpec(window_spec))
            } else {
                Some(WindowType::NamedWindow(self.parse_identifier(false)?))
            }
        } else {
            None
        };

        Ok(Expr::Function(Function {
            name,
            parameters,
            args: FunctionArguments::List(args),
            null_treatment,
            filter,
            over,
            within_group,
        }))
    }

    /// Parses the parens following the `[ NOT ] IN` operator.
    pub fn parse_in(&mut self, expr: Expr, negated: bool) -> Result<Expr, ParserError> {
        // BigQuery allows `IN UNNEST(array_expression)`
        // https://cloud.google.com/bigquery/docs/reference/standard-sql/operators#in_operators
        if self.parse_keyword(Keyword::UNNEST) {
            self.expect_token(&Token::LParen)?;
            let array_expr = self.parse_expr()?;
            self.expect_token(&Token::RParen)?;
            return Ok(Expr::InUnnest {
                expr: Box::new(expr),
                array_expr: Box::new(array_expr),
                negated,
            });
        }
        self.expect_token(&Token::LParen)?;
        let in_op = if self.parse_keyword(Keyword::SELECT) || self.parse_keyword(Keyword::WITH) {
            self.prev_token();
            Expr::InSubquery {
                expr: Box::new(expr),
                subquery: self.parse_query()?,
                negated,
            }
        } else {
            Expr::InList {
                expr: Box::new(expr),
                list: if self.dialect.supports_in_empty_list() {
                    self.parse_comma_separated0(Parser::parse_expr, Token::RParen)?
                } else {
                    self.parse_comma_separated(Parser::parse_expr)?
                },
                negated,
            }
        };
        self.expect_token(&Token::RParen)?;
        Ok(in_op)
    }

    /// Parse an operator following an expression
    pub fn parse_infix(&mut self, expr: Expr, precedence: u8) -> Result<Expr, ParserError> {
        // allow the dialect to override infix parsing
        if let Some(infix) = self.dialect.parse_infix(self, &expr, precedence) {
            return infix;
        }

        let mut tok = self.next_token();
        let regular_binary_operator = match &mut tok.token {
            Token::Spaceship => Some(BinaryOperator::Spaceship),
            Token::DoubleEq => Some(BinaryOperator::Eq),
            Token::Eq => Some(BinaryOperator::Eq),
            Token::Neq => Some(BinaryOperator::NotEq),
            Token::Gt => Some(BinaryOperator::Gt),
            Token::GtEq => Some(BinaryOperator::GtEq),
            Token::Lt => Some(BinaryOperator::Lt),
            Token::LtEq => Some(BinaryOperator::LtEq),
            Token::Plus => Some(BinaryOperator::Plus),
            Token::Minus => Some(BinaryOperator::Minus),
            Token::Mul => Some(BinaryOperator::Multiply),
            Token::Mod => Some(BinaryOperator::Modulo),
            Token::StringConcat => Some(BinaryOperator::StringConcat),
            Token::Pipe => Some(BinaryOperator::BitwiseOr),
            Token::Caret => {
                // In PostgreSQL, ^ stands for the exponentiation operation,
                // and # stands for XOR. See https://www.postgresql.org/docs/current/functions-math.html
                if dialect_of!(self is PostgreSqlDialect) {
                    Some(BinaryOperator::PGExp)
                } else {
                    Some(BinaryOperator::BitwiseXor)
                }
            }
            Token::Ampersand => Some(BinaryOperator::BitwiseAnd),
            Token::Div => Some(BinaryOperator::Divide),
            Token::DuckIntDiv if dialect_of!(self is DuckDbDialect | GenericDialect) => {
                Some(BinaryOperator::DuckIntegerDivide)
            }
            Token::ShiftLeft if dialect_of!(self is PostgreSqlDialect | DuckDbDialect | GenericDialect) => {
                Some(BinaryOperator::PGBitwiseShiftLeft)
            }
            Token::ShiftRight if dialect_of!(self is PostgreSqlDialect | DuckDbDialect | GenericDialect) => {
                Some(BinaryOperator::PGBitwiseShiftRight)
            }
            Token::Sharp if dialect_of!(self is PostgreSqlDialect) => {
                Some(BinaryOperator::PGBitwiseXor)
            }
            Token::Overlap if dialect_of!(self is PostgreSqlDialect | GenericDialect) => {
                Some(BinaryOperator::PGOverlap)
            }
            Token::CaretAt if dialect_of!(self is PostgreSqlDialect | GenericDialect) => {
                Some(BinaryOperator::PGStartsWith)
            }
            Token::Tilde => Some(BinaryOperator::PGRegexMatch),
            Token::TildeAsterisk => Some(BinaryOperator::PGRegexIMatch),
            Token::ExclamationMarkTilde => Some(BinaryOperator::PGRegexNotMatch),
            Token::ExclamationMarkTildeAsterisk => Some(BinaryOperator::PGRegexNotIMatch),
            Token::DoubleTilde => Some(BinaryOperator::PGLikeMatch),
            Token::DoubleTildeAsterisk => Some(BinaryOperator::PGILikeMatch),
            Token::ExclamationMarkDoubleTilde => Some(BinaryOperator::PGNotLikeMatch),
            Token::ExclamationMarkDoubleTildeAsterisk => Some(BinaryOperator::PGNotILikeMatch),
            Token::Arrow => Some(BinaryOperator::Arrow),
            Token::LongArrow => Some(BinaryOperator::LongArrow),
            Token::HashArrow => Some(BinaryOperator::HashArrow),
            Token::HashLongArrow => Some(BinaryOperator::HashLongArrow),
            Token::AtArrow => Some(BinaryOperator::AtArrow),
            Token::ArrowAt => Some(BinaryOperator::ArrowAt),
            Token::HashMinus => Some(BinaryOperator::HashMinus),
            Token::AtQuestion => Some(BinaryOperator::AtQuestion),
            Token::AtAt => Some(BinaryOperator::AtAt),
            Token::Question => Some(BinaryOperator::Question),
            Token::QuestionAnd => Some(BinaryOperator::QuestionAnd),
            Token::QuestionPipe => Some(BinaryOperator::QuestionPipe),
            Token::CustomBinaryOperator(s) => Some(BinaryOperator::Custom(core::mem::take(s))),

            Token::Word(w) => match w.keyword {
                Keyword::AND => Some(BinaryOperator::And),
                Keyword::OR => Some(BinaryOperator::Or),
                Keyword::XOR => Some(BinaryOperator::Xor),
                Keyword::OPERATOR if dialect_of!(self is PostgreSqlDialect | GenericDialect) => {
                    self.expect_token(&Token::LParen)?;
                    // there are special rules for operator names in
                    // postgres so we can not use 'parse_object'
                    // or similar.
                    // See https://www.postgresql.org/docs/current/sql-createoperator.html
                    let mut idents = vec![];
                    loop {
                        idents.push(self.next_token().to_string());
                        if !self.consume_token(&Token::Period) {
                            break;
                        }
                    }
                    self.expect_token(&Token::RParen)?;
                    Some(BinaryOperator::PGCustomBinaryOperator(idents))
                }
                _ => None,
            },
            _ => None,
        };

        if let Some(op) = regular_binary_operator {
            if let Some(keyword) =
                self.parse_one_of_keywords(&[Keyword::ANY, Keyword::ALL, Keyword::SOME])
            {
                self.expect_token(&Token::LParen)?;
                let right = if self.peek_sub_query() {
                    // We have a subquery ahead (SELECT\WITH ...) need to rewind and
                    // use the parenthesis for parsing the subquery as an expression.
                    self.prev_token(); // LParen
                    self.parse_subexpr(precedence)?
                } else {
                    // Non-subquery expression
                    let right = self.parse_subexpr(precedence)?;
                    self.expect_token(&Token::RParen)?;
                    right
                };

                if !matches!(
                    op,
                    BinaryOperator::Gt
                        | BinaryOperator::Lt
                        | BinaryOperator::GtEq
                        | BinaryOperator::LtEq
                        | BinaryOperator::Eq
                        | BinaryOperator::NotEq
                ) {
                    return parser_err!(
                        format!(
                        "Expected one of [=, >, <, =>, =<, !=] as comparison operator, found: {op}"
                    ),
                        tok.span.start
                    );
                };

                Ok(match keyword {
                    Keyword::ALL => Expr::AllOp {
                        left: Box::new(expr),
                        compare_op: op,
                        right: Box::new(right),
                    },
                    Keyword::ANY | Keyword::SOME => Expr::AnyOp {
                        left: Box::new(expr),
                        compare_op: op,
                        right: Box::new(right),
                        is_some: keyword == Keyword::SOME,
                    },
                    _ => unreachable!(),
                })
            } else {
                Ok(Expr::BinaryOp {
                    left: Box::new(expr),
                    op,
                    right: Box::new(self.parse_subexpr(precedence)?),
                })
            }
        } else if let Token::Word(w) = &tok.token {
            match w.keyword {
                Keyword::IS => {
                    if self.parse_keyword(Keyword::NULL) {
                        Ok(Expr::IsNull(Box::new(expr)))
                    } else if self.parse_keywords(&[Keyword::NOT, Keyword::NULL]) {
                        Ok(Expr::IsNotNull(Box::new(expr)))
                    } else if self.parse_keywords(&[Keyword::TRUE]) {
                        Ok(Expr::IsTrue(Box::new(expr)))
                    } else if self.parse_keywords(&[Keyword::NOT, Keyword::TRUE]) {
                        Ok(Expr::IsNotTrue(Box::new(expr)))
                    } else if self.parse_keywords(&[Keyword::FALSE]) {
                        Ok(Expr::IsFalse(Box::new(expr)))
                    } else if self.parse_keywords(&[Keyword::NOT, Keyword::FALSE]) {
                        Ok(Expr::IsNotFalse(Box::new(expr)))
                    } else if self.parse_keywords(&[Keyword::UNKNOWN]) {
                        Ok(Expr::IsUnknown(Box::new(expr)))
                    } else if self.parse_keywords(&[Keyword::NOT, Keyword::UNKNOWN]) {
                        Ok(Expr::IsNotUnknown(Box::new(expr)))
                    } else if self.parse_keywords(&[Keyword::DISTINCT, Keyword::FROM]) {
                        let expr2 = self.parse_expr()?;
                        Ok(Expr::IsDistinctFrom(Box::new(expr), Box::new(expr2)))
                    } else if self.parse_keywords(&[Keyword::NOT, Keyword::DISTINCT, Keyword::FROM])
                    {
                        let expr2 = self.parse_expr()?;
                        Ok(Expr::IsNotDistinctFrom(Box::new(expr), Box::new(expr2)))
                    } else {
                        self.expected(
                            "[NOT] NULL or TRUE|FALSE or [NOT] DISTINCT FROM after IS",
                            self.peek_token(),
                        )
                    }
                }
                Keyword::AT => {
                    self.expect_keywords(&[Keyword::TIME, Keyword::ZONE])?;
                    Ok(Expr::AtTimeZone {
                        timestamp: Box::new(expr),
                        time_zone: Box::new(self.parse_subexpr(precedence)?),
                    })
                }
                Keyword::NOT
                | Keyword::IN
                | Keyword::BETWEEN
                | Keyword::LIKE
                | Keyword::ILIKE
                | Keyword::SIMILAR
                | Keyword::REGEXP
                | Keyword::RLIKE => {
                    self.prev_token();
                    let negated = self.parse_keyword(Keyword::NOT);
                    let regexp = self.parse_keyword(Keyword::REGEXP);
                    let rlike = self.parse_keyword(Keyword::RLIKE);
                    if regexp || rlike {
                        Ok(Expr::RLike {
                            negated,
                            expr: Box::new(expr),
                            pattern: Box::new(
                                self.parse_subexpr(self.dialect.prec_value(Precedence::Like))?,
                            ),
                            regexp,
                        })
                    } else if self.parse_keyword(Keyword::IN) {
                        self.parse_in(expr, negated)
                    } else if self.parse_keyword(Keyword::BETWEEN) {
                        self.parse_between(expr, negated)
                    } else if self.parse_keyword(Keyword::LIKE) {
                        Ok(Expr::Like {
                            negated,
                            any: self.parse_keyword(Keyword::ANY),
                            expr: Box::new(expr),
                            pattern: Box::new(
                                self.parse_subexpr(self.dialect.prec_value(Precedence::Like))?,
                            ),
                            escape_char: self.parse_escape_char()?,
                        })
                    } else if self.parse_keyword(Keyword::ILIKE) {
                        Ok(Expr::ILike {
                            negated,
                            any: self.parse_keyword(Keyword::ANY),
                            expr: Box::new(expr),
                            pattern: Box::new(
                                self.parse_subexpr(self.dialect.prec_value(Precedence::Like))?,
                            ),
                            escape_char: self.parse_escape_char()?,
                        })
                    } else if self.parse_keywords(&[Keyword::SIMILAR, Keyword::TO]) {
                        Ok(Expr::SimilarTo {
                            negated,
                            expr: Box::new(expr),
                            pattern: Box::new(
                                self.parse_subexpr(self.dialect.prec_value(Precedence::Like))?,
                            ),
                            escape_char: self.parse_escape_char()?,
                        })
                    } else {
                        self.expected("IN or BETWEEN after NOT", self.peek_token())
                    }
                }
                // Can only happen if `get_next_precedence` got out of sync with this function
                _ => parser_err!(
                    format!("No infix parser for token {:?}", tok.token),
                    tok.span.start
                ),
            }
        } else if Token::DoubleColon == tok {
            Ok(Expr::Cast {
                kind: CastKind::DoubleColon,
                expr: Box::new(expr),
                data_type: self.parse_data_type()?,
                format: None,
            })
        } else if Token::ExclamationMark == tok && self.dialect.supports_factorial_operator() {
            Ok(Expr::UnaryOp {
                op: UnaryOperator::PGPostfixFactorial,
                expr: Box::new(expr),
            })
        } else if Token::LBracket == tok {
            if dialect_of!(self is PostgreSqlDialect | DuckDbDialect | GenericDialect) {
                self.parse_subscript(expr)
            } else if dialect_of!(self is SnowflakeDialect) || self.dialect.supports_partiql() {
                self.prev_token();
                self.parse_json_access(expr)
            } else {
                self.parse_map_access(expr)
            }
        } else if dialect_of!(self is SnowflakeDialect | GenericDialect) && Token::Colon == tok {
            self.prev_token();
            self.parse_json_access(expr)
        } else {
            // Can only happen if `get_next_precedence` got out of sync with this function
            parser_err!(
                format!("No infix parser for token {:?}", tok.token),
                tok.span.start
            )
        }
    }

    /// Parse an `INTERVAL` expression.
    ///
    /// Some syntactically valid intervals:
    ///
    /// ```sql
    ///   1. INTERVAL '1' DAY
    ///   2. INTERVAL '1-1' YEAR TO MONTH
    ///   3. INTERVAL '1' SECOND
    ///   4. INTERVAL '1:1:1.1' HOUR (5) TO SECOND (5)
    ///   5. INTERVAL '1.1' SECOND (2, 2)
    ///   6. INTERVAL '1:1' HOUR (5) TO MINUTE (5)
    ///   7. (MySql & BigQuery only): INTERVAL 1 DAY
    /// ```
    ///
    /// Note that we do not currently attempt to parse the quoted value.
    pub fn parse_interval(&mut self) -> Result<Expr, ParserError> {
        // The SQL standard allows an optional sign before the value string, but
        // it is not clear if any implementations support that syntax, so we
        // don't currently try to parse it. (The sign can instead be included
        // inside the value string.)

        // to match the different flavours of INTERVAL syntax, we only allow expressions
        // if the dialect requires an interval qualifier,
        // see https://github.com/sqlparser-rs/sqlparser-rs/pull/1398 for more details
        let value = if self.dialect.require_interval_qualifier() {
            // parse a whole expression so `INTERVAL 1 + 1 DAY` is valid
            self.parse_expr()?
        } else {
            // parse a prefix expression so `INTERVAL 1 DAY` is valid, but `INTERVAL 1 + 1 DAY` is not
            // this also means that `INTERVAL '5 days' > INTERVAL '1 day'` treated properly
            self.parse_prefix()?
        };

        // Following the string literal is a qualifier which indicates the units
        // of the duration specified in the string literal.
        //
        // Note that PostgreSQL allows omitting the qualifier, so we provide
        // this more general implementation.
        let leading_field = if self.next_token_is_temporal_unit() {
            Some(self.parse_date_time_field()?)
        } else if self.dialect.require_interval_qualifier() {
            return parser_err!(
                "INTERVAL requires a unit after the literal value",
                self.peek_token().span.start
            );
        } else {
            None
        };

        let (leading_precision, last_field, fsec_precision) =
            if leading_field == Some(DateTimeField::Second) {
                // SQL mandates special syntax for `SECOND TO SECOND` literals.
                // Instead of
                //     `SECOND [(<leading precision>)] TO SECOND[(<fractional seconds precision>)]`
                // one must use the special format:
                //     `SECOND [( <leading precision> [ , <fractional seconds precision>] )]`
                let last_field = None;
                let (leading_precision, fsec_precision) = self.parse_optional_precision_scale()?;
                (leading_precision, last_field, fsec_precision)
            } else {
                let leading_precision = self.parse_optional_precision()?;
                if self.parse_keyword(Keyword::TO) {
                    let last_field = Some(self.parse_date_time_field()?);
                    let fsec_precision = if last_field == Some(DateTimeField::Second) {
                        self.parse_optional_precision()?
                    } else {
                        None
                    };
                    (leading_precision, last_field, fsec_precision)
                } else {
                    (leading_precision, None, None)
                }
            };

        Ok(Expr::Interval(Interval {
            value: Box::new(value),
            leading_field,
            leading_precision,
            last_field,
            fractional_seconds_precision: fsec_precision,
        }))
    }

    pub fn parse_listagg_on_overflow(&mut self) -> Result<Option<ListAggOnOverflow>, ParserError> {
        if self.parse_keywords(&[Keyword::ON, Keyword::OVERFLOW]) {
            if self.parse_keyword(Keyword::ERROR) {
                Ok(Some(ListAggOnOverflow::Error))
            } else {
                self.expect_keyword(Keyword::TRUNCATE)?;
                let filler = match self.peek_token().token {
                    Token::Word(w)
                        if w.keyword == Keyword::WITH || w.keyword == Keyword::WITHOUT =>
                    {
                        None
                    }
                    Token::SingleQuotedString(_)
                    | Token::EscapedStringLiteral(_)
                    | Token::UnicodeStringLiteral(_)
                    | Token::NationalStringLiteral(_)
                    | Token::HexStringLiteral(_) => Some(Box::new(self.parse_expr()?)),
                    _ => self.expected(
                        "either filler, WITH, or WITHOUT in LISTAGG",
                        self.peek_token(),
                    )?,
                };
                let with_count = self.parse_keyword(Keyword::WITH);
                if !with_count && !self.parse_keyword(Keyword::WITHOUT) {
                    self.expected("either WITH or WITHOUT in LISTAGG", self.peek_token())?;
                }
                self.expect_keyword(Keyword::COUNT)?;
                Ok(Some(ListAggOnOverflow::Truncate { filler, with_count }))
            }
        } else {
            Ok(None)
        }
    }

    pub fn parse_map_access(&mut self, expr: Expr) -> Result<Expr, ParserError> {
        let key = self.parse_expr()?;
        self.expect_token(&Token::RBracket)?;

        let mut keys = vec![MapAccessKey {
            key,
            syntax: MapAccessSyntax::Bracket,
        }];
        loop {
            let key = match self.peek_token().token {
                Token::LBracket => {
                    self.next_token(); // consume `[`
                    let key = self.parse_expr()?;
                    self.expect_token(&Token::RBracket)?;
                    MapAccessKey {
                        key,
                        syntax: MapAccessSyntax::Bracket,
                    }
                }
                // Access on BigQuery nested and repeated expressions can
                // mix notations in the same expression.
                // https://cloud.google.com/bigquery/docs/nested-repeated#query_nested_and_repeated_columns
                Token::Period if dialect_of!(self is BigQueryDialect) => {
                    self.next_token(); // consume `.`
                    MapAccessKey {
                        key: self.parse_expr()?,
                        syntax: MapAccessSyntax::Period,
                    }
                }
                _ => break,
            };
            keys.push(key);
        }

        Ok(Expr::MapAccess {
            column: Box::new(expr),
            keys,
        })
    }

    /// Parses fulltext expressions [`sqlparser::ast::Expr::MatchAgainst`]
    ///
    /// # Errors
    /// This method will raise an error if the column list is empty or with invalid identifiers,
    /// the match expression is not a literal string, or if the search modifier is not valid.
    pub fn parse_match_against(&mut self) -> Result<Expr, ParserError> {
        let columns = self.parse_parenthesized_column_list(Mandatory, false)?;

        self.expect_keyword(Keyword::AGAINST)?;

        self.expect_token(&Token::LParen)?;

        // MySQL is too permissive about the value, IMO we can't validate it perfectly on syntax level.
        let match_value = self.parse_value()?;

        let in_natural_language_mode_keywords = &[
            Keyword::IN,
            Keyword::NATURAL,
            Keyword::LANGUAGE,
            Keyword::MODE,
        ];

        let with_query_expansion_keywords = &[Keyword::WITH, Keyword::QUERY, Keyword::EXPANSION];

        let in_boolean_mode_keywords = &[Keyword::IN, Keyword::BOOLEAN, Keyword::MODE];

        let opt_search_modifier = if self.parse_keywords(in_natural_language_mode_keywords) {
            if self.parse_keywords(with_query_expansion_keywords) {
                Some(SearchModifier::InNaturalLanguageModeWithQueryExpansion)
            } else {
                Some(SearchModifier::InNaturalLanguageMode)
            }
        } else if self.parse_keywords(in_boolean_mode_keywords) {
            Some(SearchModifier::InBooleanMode)
        } else if self.parse_keywords(with_query_expansion_keywords) {
            Some(SearchModifier::WithQueryExpansion)
        } else {
            None
        };

        self.expect_token(&Token::RParen)?;

        Ok(Expr::MatchAgainst {
            columns,
            match_value,
            opt_search_modifier,
        })
    }

    pub fn parse_not(&mut self) -> Result<Expr, ParserError> {
        match self.peek_token().token {
            Token::Word(w) => match w.keyword {
                Keyword::EXISTS => {
                    let negated = true;
                    let _ = self.parse_keyword(Keyword::EXISTS);
                    self.parse_exists_expr(negated)
                }
                _ => Ok(Expr::UnaryOp {
                    op: UnaryOperator::Not,
                    expr: Box::new(
                        self.parse_subexpr(self.dialect.prec_value(Precedence::UnaryNot))?,
                    ),
                }),
            },
            _ => Ok(Expr::UnaryOp {
                op: UnaryOperator::Not,
                expr: Box::new(self.parse_subexpr(self.dialect.prec_value(Precedence::UnaryNot))?),
            }),
        }
    }

    pub fn parse_optional_cast_format(&mut self) -> Result<Option<CastFormat>, ParserError> {
        if self.parse_keyword(Keyword::FORMAT) {
            let value = self.parse_value()?;
            match self.parse_optional_time_zone()? {
                Some(tz) => Ok(Some(CastFormat::ValueAtTimeZone(value, tz))),
                None => Ok(Some(CastFormat::Value(value))),
            }
        } else {
            Ok(None)
        }
    }

    pub fn parse_optional_group_by(&mut self) -> Result<Option<GroupByExpr>, ParserError> {
        if self.parse_keywords(&[Keyword::GROUP, Keyword::BY]) {
            let expressions = if self.parse_keyword(Keyword::ALL) {
                None
            } else {
                Some(self.parse_comma_separated(Parser::parse_group_by_expr)?)
            };

            let mut modifiers = vec![];
            if dialect_of!(self is ClickHouseDialect | GenericDialect) {
                loop {
                    if !self.parse_keyword(Keyword::WITH) {
                        break;
                    }
                    let keyword = self.expect_one_of_keywords(&[
                        Keyword::ROLLUP,
                        Keyword::CUBE,
                        Keyword::TOTALS,
                    ])?;
                    modifiers.push(match keyword {
                        Keyword::ROLLUP => GroupByWithModifier::Rollup,
                        Keyword::CUBE => GroupByWithModifier::Cube,
                        Keyword::TOTALS => GroupByWithModifier::Totals,
                        _ => {
                            return parser_err!(
                                "BUG: expected to match GroupBy modifier keyword",
                                self.peek_token().span.start
                            )
                        }
                    });
                }
            }
            let group_by = match expressions {
                None => GroupByExpr::All(modifiers),
                Some(exprs) => GroupByExpr::Expressions(exprs, modifiers),
            };
            Ok(Some(group_by))
        } else {
            Ok(None)
        }
    }

    pub fn parse_optional_precision_scale(
        &mut self,
    ) -> Result<(Option<u64>, Option<u64>), ParserError> {
        if self.consume_token(&Token::LParen) {
            let n = self.parse_literal_uint()?;
            let scale = if self.consume_token(&Token::Comma) {
                Some(self.parse_literal_uint()?)
            } else {
                None
            };
            self.expect_token(&Token::RParen)?;
            Ok((Some(n), scale))
        } else {
            Ok((None, None))
        }
    }

    pub fn parse_optional_time_zone(&mut self) -> Result<Option<Value>, ParserError> {
        if self.parse_keywords(&[Keyword::AT, Keyword::TIME, Keyword::ZONE]) {
            self.parse_value().map(Some)
        } else {
            Ok(None)
        }
    }

    pub fn parse_overlay_expr(&mut self) -> Result<Expr, ParserError> {
        // PARSE OVERLAY (EXPR PLACING EXPR FROM 1 [FOR 3])
        self.expect_token(&Token::LParen)?;
        let expr = self.parse_expr()?;
        self.expect_keyword(Keyword::PLACING)?;
        let what_expr = self.parse_expr()?;
        self.expect_keyword(Keyword::FROM)?;
        let from_expr = self.parse_expr()?;
        let mut for_expr = None;
        if self.parse_keyword(Keyword::FOR) {
            for_expr = Some(self.parse_expr()?);
        }
        self.expect_token(&Token::RParen)?;

        Ok(Expr::Overlay {
            expr: Box::new(expr),
            overlay_what: Box::new(what_expr),
            overlay_from: Box::new(from_expr),
            overlay_for: for_expr.map(Box::new),
        })
    }

    pub fn parse_position_expr(&mut self, ident: Ident) -> Result<Expr, ParserError> {
        let between_prec = self.dialect.prec_value(Precedence::Between);
        let position_expr = self.maybe_parse(|p| {
            // PARSE SELECT POSITION('@' in field)
            p.expect_token(&Token::LParen)?;

            // Parse the subexpr till the IN keyword
            let expr = p.parse_subexpr(between_prec)?;
            p.expect_keyword(Keyword::IN)?;
            let from = p.parse_expr()?;
            p.expect_token(&Token::RParen)?;
            Ok(Expr::Position {
                expr: Box::new(expr),
                r#in: Box::new(from),
            })
        })?;
        match position_expr {
            Some(expr) => Ok(expr),
            // Snowflake supports `position` as an ordinary function call
            // without the special `IN` syntax.
            None => self.parse_function(ObjectName(vec![ident])),
        }
    }

    /// Parse an expression prefix.
    pub fn parse_prefix(&mut self) -> Result<Expr, ParserError> {
        // allow the dialect to override prefix parsing
        if let Some(prefix) = self.dialect.parse_prefix(self) {
            return prefix;
        }

        // PostgreSQL allows any string literal to be preceded by a type name, indicating that the
        // string literal represents a literal of that type. Some examples:
        //
        //      DATE '2020-05-20'
        //      TIMESTAMP WITH TIME ZONE '2020-05-20 7:43:54'
        //      BOOL 'true'
        //
        // The first two are standard SQL, while the latter is a PostgreSQL extension. Complicating
        // matters is the fact that INTERVAL string literals may optionally be followed by special
        // keywords, e.g.:
        //
        //      INTERVAL '7' DAY
        //
        // Note also that naively `SELECT date` looks like a syntax error because the `date` type
        // name is not followed by a string literal, but in fact in PostgreSQL it is a valid
        // expression that should parse as the column name "date".
        let loc = self.peek_token().span.start;
        let opt_expr = self.maybe_parse(|parser| {
            match parser.parse_data_type()? {
                DataType::Interval => parser.parse_interval(),
                // PostgreSQL allows almost any identifier to be used as custom data type name,
                // and we support that in `parse_data_type()`. But unlike Postgres we don't
                // have a list of globally reserved keywords (since they vary across dialects),
                // so given `NOT 'a' LIKE 'b'`, we'd accept `NOT` as a possible custom data type
                // name, resulting in `NOT 'a'` being recognized as a `TypedString` instead of
                // an unary negation `NOT ('a' LIKE 'b')`. To solve this, we don't accept the
                // `type 'string'` syntax for the custom data types at all.
                DataType::Custom(..) => parser_err!("dummy", loc),
                data_type => Ok(Expr::TypedString {
                    data_type,
                    value: parser.parse_literal_string()?,
                }),
            }
        })?;

        if let Some(expr) = opt_expr {
            return Ok(expr);
        }

        let next_token = self.next_token();
        let expr = match next_token.token {
            Token::Word(w) => {
                // The word we consumed may fall into one of two cases: it has a special meaning, or not.
                // For example, in Snowflake, the word `interval` may have two meanings depending on the context:
                // `SELECT CURRENT_DATE() + INTERVAL '1 DAY', MAX(interval) FROM tbl;`
                //                          ^^^^^^^^^^^^^^^^      ^^^^^^^^
                //                         interval expression   identifier
                //
                // We first try to parse the word and following tokens as a special expression, and if that fails,
                // we rollback and try to parse it as an identifier.
                match self.try_parse(|parser| {
                    parser.parse_expr_prefix_by_reserved_word(&w, next_token.span)
                }) {
                    // This word indicated an expression prefix and parsing was successful
                    Ok(Some(expr)) => Ok(expr),

                    // No expression prefix associated with this word
                    Ok(None) => Ok(self.parse_expr_prefix_by_unreserved_word(&w, next_token.span)?),

                    // If parsing of the word as a special expression failed, we are facing two options:
                    // 1. The statement is malformed, e.g. `SELECT INTERVAL '1 DAI` (`DAI` instead of `DAY`)
                    // 2. The word is used as an identifier, e.g. `SELECT MAX(interval) FROM tbl`
                    // We first try to parse the word as an identifier and if that fails
                    // we rollback and return the parsing error we got from trying to parse a
                    // special expression (to maintain backwards compatibility of parsing errors).
                    Err(e) => {
                        if !self.dialect.is_reserved_for_identifier(w.keyword) {
                            if let Ok(Some(expr)) = self.maybe_parse(|parser| {
                                parser.parse_expr_prefix_by_unreserved_word(&w, next_token.span)
                            }) {
                                return Ok(expr);
                            }
                        }
                        return Err(e);
                    }
                }
            } // End of Token::Word
            // array `[1, 2, 3]`
            Token::LBracket => self.parse_array_expr(false),
            tok @ Token::Minus | tok @ Token::Plus => {
                let op = if tok == Token::Plus {
                    UnaryOperator::Plus
                } else {
                    UnaryOperator::Minus
                };
                Ok(Expr::UnaryOp {
                    op,
                    expr: Box::new(
                        self.parse_subexpr(self.dialect.prec_value(Precedence::MulDivModOp))?,
                    ),
                })
            }
            Token::ExclamationMark if self.dialect.supports_bang_not_operator() => {
                Ok(Expr::UnaryOp {
                    op: UnaryOperator::BangNot,
                    expr: Box::new(
                        self.parse_subexpr(self.dialect.prec_value(Precedence::UnaryNot))?,
                    ),
                })
            }
            tok @ Token::DoubleExclamationMark
            | tok @ Token::PGSquareRoot
            | tok @ Token::PGCubeRoot
            | tok @ Token::AtSign
            | tok @ Token::Tilde
                if dialect_of!(self is PostgreSqlDialect) =>
            {
                let op = match tok {
                    Token::DoubleExclamationMark => UnaryOperator::PGPrefixFactorial,
                    Token::PGSquareRoot => UnaryOperator::PGSquareRoot,
                    Token::PGCubeRoot => UnaryOperator::PGCubeRoot,
                    Token::AtSign => UnaryOperator::PGAbs,
                    Token::Tilde => UnaryOperator::PGBitwiseNot,
                    _ => unreachable!(),
                };
                Ok(Expr::UnaryOp {
                    op,
                    expr: Box::new(
                        self.parse_subexpr(self.dialect.prec_value(Precedence::PlusMinus))?,
                    ),
                })
            }
            Token::EscapedStringLiteral(_) if dialect_of!(self is PostgreSqlDialect | GenericDialect) =>
            {
                self.prev_token();
                Ok(Expr::Value(self.parse_value()?))
            }
            Token::UnicodeStringLiteral(_) => {
                self.prev_token();
                Ok(Expr::Value(self.parse_value()?))
            }
            Token::Number(_, _)
            | Token::SingleQuotedString(_)
            | Token::DoubleQuotedString(_)
            | Token::TripleSingleQuotedString(_)
            | Token::TripleDoubleQuotedString(_)
            | Token::DollarQuotedString(_)
            | Token::SingleQuotedByteStringLiteral(_)
            | Token::DoubleQuotedByteStringLiteral(_)
            | Token::TripleSingleQuotedByteStringLiteral(_)
            | Token::TripleDoubleQuotedByteStringLiteral(_)
            | Token::SingleQuotedRawStringLiteral(_)
            | Token::DoubleQuotedRawStringLiteral(_)
            | Token::TripleSingleQuotedRawStringLiteral(_)
            | Token::TripleDoubleQuotedRawStringLiteral(_)
            | Token::NationalStringLiteral(_)
            | Token::HexStringLiteral(_) => {
                self.prev_token();
                Ok(Expr::Value(self.parse_value()?))
            }
            Token::LParen => {
                let expr = if let Some(expr) = self.try_parse_expr_sub_query()? {
                    expr
                } else if let Some(lambda) = self.try_parse_lambda()? {
                    return Ok(lambda);
                } else {
                    let exprs = self.parse_comma_separated(Parser::parse_expr)?;
                    match exprs.len() {
                        0 => unreachable!(), // parse_comma_separated ensures 1 or more
                        1 => Expr::Nested(Box::new(exprs.into_iter().next().unwrap())),
                        _ => Expr::Tuple(exprs),
                    }
                };
                self.expect_token(&Token::RParen)?;
                let expr = self.try_parse_method(expr)?;
                if !self.consume_token(&Token::Period) {
                    Ok(expr)
                } else {
                    let tok = self.next_token();
                    let key = match tok.token {
                        Token::Word(word) => word.to_ident(tok.span),
                        _ => {
                            return parser_err!(
                                format!("Expected identifier, found: {tok}"),
                                tok.span.start
                            )
                        }
                    };
                    Ok(Expr::CompositeAccess {
                        expr: Box::new(expr),
                        key,
                    })
                }
            }
            Token::Placeholder(_) | Token::Colon | Token::AtSign => {
                self.prev_token();
                Ok(Expr::Value(self.parse_value()?))
            }
            Token::LBrace if self.dialect.supports_dictionary_syntax() => {
                self.prev_token();
                self.parse_duckdb_struct_literal()
            }
            _ => self.expected("an expression", next_token),
        }?;

        let expr = self.try_parse_method(expr)?;

        if self.parse_keyword(Keyword::COLLATE) {
            Ok(Expr::Collate {
                expr: Box::new(expr),
                collation: self.parse_object_name(false)?,
            })
        } else {
            Ok(expr)
        }
    }

    /// Parses an array subscript like `[1:3]`
    ///
    /// Parser is right after `[`
    pub fn parse_subscript(&mut self, expr: Expr) -> Result<Expr, ParserError> {
        let subscript = self.parse_subscript_inner()?;
        Ok(Expr::Subscript {
            expr: Box::new(expr),
            subscript: Box::new(subscript),
        })
    }

    pub fn parse_substring_expr(&mut self) -> Result<Expr, ParserError> {
        // PARSE SUBSTRING (EXPR [FROM 1] [FOR 3])
        self.expect_token(&Token::LParen)?;
        let expr = self.parse_expr()?;
        let mut from_expr = None;
        let special = self.consume_token(&Token::Comma);
        if special || self.parse_keyword(Keyword::FROM) {
            from_expr = Some(self.parse_expr()?);
        }

        let mut to_expr = None;
        if self.parse_keyword(Keyword::FOR) || self.consume_token(&Token::Comma) {
            to_expr = Some(self.parse_expr()?);
        }
        self.expect_token(&Token::RParen)?;

        Ok(Expr::Substring {
            expr: Box::new(expr),
            substring_from: from_expr.map(Box::new),
            substring_for: to_expr.map(Box::new),
            special,
        })
    }

    pub fn parse_time_functions(&mut self, name: ObjectName) -> Result<Expr, ParserError> {
        let args = if self.consume_token(&Token::LParen) {
            FunctionArguments::List(self.parse_function_argument_list()?)
        } else {
            FunctionArguments::None
        };
        Ok(Expr::Function(Function {
            name,
            parameters: FunctionArguments::None,
            args,
            filter: None,
            over: None,
            null_treatment: None,
            within_group: vec![],
        }))
    }

    /// ```sql
    /// TRIM ([WHERE] ['text' FROM] 'text')
    /// TRIM ('text')
    /// TRIM(<expr>, [, characters]) -- only Snowflake or BigQuery
    /// ```
    pub fn parse_trim_expr(&mut self) -> Result<Expr, ParserError> {
        self.expect_token(&Token::LParen)?;
        let mut trim_where = None;
        if let Token::Word(word) = self.peek_token().token {
            if [Keyword::BOTH, Keyword::LEADING, Keyword::TRAILING]
                .iter()
                .any(|d| word.keyword == *d)
            {
                trim_where = Some(self.parse_trim_where()?);
            }
        }
        let expr = self.parse_expr()?;
        if self.parse_keyword(Keyword::FROM) {
            let trim_what = Box::new(expr);
            let expr = self.parse_expr()?;
            self.expect_token(&Token::RParen)?;
            Ok(Expr::Trim {
                expr: Box::new(expr),
                trim_where,
                trim_what: Some(trim_what),
                trim_characters: None,
            })
        } else if self.consume_token(&Token::Comma)
            && dialect_of!(self is SnowflakeDialect | BigQueryDialect | GenericDialect)
        {
            let characters = self.parse_comma_separated(Parser::parse_expr)?;
            self.expect_token(&Token::RParen)?;
            Ok(Expr::Trim {
                expr: Box::new(expr),
                trim_where: None,
                trim_what: None,
                trim_characters: Some(characters),
            })
        } else {
            self.expect_token(&Token::RParen)?;
            Ok(Expr::Trim {
                expr: Box::new(expr),
                trim_where,
                trim_what: None,
                trim_characters: None,
            })
        }
    }

    pub fn parse_trim_where(&mut self) -> Result<TrimWhereField, ParserError> {
        let next_token = self.next_token();
        match &next_token.token {
            Token::Word(w) => match w.keyword {
                Keyword::BOTH => Ok(TrimWhereField::Both),
                Keyword::LEADING => Ok(TrimWhereField::Leading),
                Keyword::TRAILING => Ok(TrimWhereField::Trailing),
                _ => self.expected("trim_where field", next_token)?,
            },
            _ => self.expected("trim_where field", next_token),
        }
    }

    /// Parse a new expression including wildcard & qualified wildcard.
    pub fn parse_wildcard_expr(&mut self) -> Result<Expr, ParserError> {
        let index = self.index;

        let next_token = self.next_token();
        match next_token.token {
            t @ (Token::Word(_) | Token::SingleQuotedString(_)) => {
                if self.peek_token().token == Token::Period {
                    let mut id_parts: Vec<Ident> = vec![match t {
                        Token::Word(w) => w.to_ident(next_token.span),
                        Token::SingleQuotedString(s) => Ident::with_quote('\'', s),
                        _ => unreachable!(), // We matched above
                    }];

                    while self.consume_token(&Token::Period) {
                        let next_token = self.next_token();
                        match next_token.token {
                            Token::Word(w) => id_parts.push(w.to_ident(next_token.span)),
                            Token::SingleQuotedString(s) => {
                                // SQLite has single-quoted identifiers
                                id_parts.push(Ident::with_quote('\'', s))
                            }
                            Token::Mul => {
                                return Ok(Expr::QualifiedWildcard(
                                    ObjectName(id_parts),
                                    AttachedToken(next_token),
                                ));
                            }
                            _ => {
                                return self
                                    .expected("an identifier or a '*' after '.'", next_token);
                            }
                        }
                    }
                }
            }
            Token::Mul => {
                return Ok(Expr::Wildcard(AttachedToken(next_token)));
            }
            _ => (),
        };

        self.index = index;
        self.parse_expr()
    }

    /// Parse a Struct type definition as a sequence of field-value pairs.
    /// The syntax of the Struct elem differs by dialect so it is customised
    /// by the `elem_parser` argument.
    ///
    /// Syntax
    /// ```sql
    /// Hive:
    /// STRUCT<field_name: field_type>
    ///
    /// BigQuery:
    /// STRUCT<[field_name] field_type>
    /// ```
    pub(crate) fn parse_struct_type_def<F>(
        &mut self,
        mut elem_parser: F,
    ) -> Result<(Vec<StructField>, MatchedTrailingBracket), ParserError>
    where
        F: FnMut(&mut Parser<'a>) -> Result<(StructField, MatchedTrailingBracket), ParserError>,
    {
        let start_token = self.peek_token();
        self.expect_keyword(Keyword::STRUCT)?;

        // Nothing to do if we have no type information.
        if Token::Lt != self.peek_token() {
            return Ok((Default::default(), false.into()));
        }
        self.next_token();

        let mut field_defs = vec![];
        let trailing_bracket = loop {
            let (def, trailing_bracket) = elem_parser(self)?;
            field_defs.push(def);
            if !self.consume_token(&Token::Comma) {
                break trailing_bracket;
            }

            // Angle brackets are balanced so we only expect the trailing `>>` after
            // we've matched all field types for the current struct.
            // e.g. this is invalid syntax `STRUCT<STRUCT<INT>>>, INT>(NULL)`
            if trailing_bracket.0 {
                return parser_err!("unmatched > in STRUCT definition", start_token.span.start);
            }
        };

        Ok((
            field_defs,
            self.expect_closing_angle_bracket(trailing_bracket)?,
        ))
    }

    pub(crate) fn try_parse_expr_sub_query(&mut self) -> Result<Option<Expr>, ParserError> {
        if !self.peek_sub_query() {
            return Ok(None);
        }

        Ok(Some(Expr::Subquery(self.parse_query()?)))
    }

    fn parse_duplicate_treatment(&mut self) -> Result<Option<DuplicateTreatment>, ParserError> {
        let loc = self.peek_token().span.start;
        match (
            self.parse_keyword(Keyword::ALL),
            self.parse_keyword(Keyword::DISTINCT),
        ) {
            (true, false) => Ok(Some(DuplicateTreatment::All)),
            (false, true) => Ok(Some(DuplicateTreatment::Distinct)),
            (false, false) => Ok(None),
            (true, true) => parser_err!("Cannot specify both ALL and DISTINCT".to_string(), loc),
        }
    }

    // Tries to parse an expression by matching the specified word to known keywords that have a special meaning in the dialect.
    // Returns `None if no match is found.
    fn parse_expr_prefix_by_reserved_word(
        &mut self,
        w: &Word,
        w_span: Span,
    ) -> Result<Option<Expr>, ParserError> {
        match w.keyword {
            Keyword::TRUE | Keyword::FALSE if self.dialect.supports_boolean_literals() => {
                self.prev_token();
                Ok(Some(Expr::Value(self.parse_value()?)))
            }
            Keyword::NULL => {
                self.prev_token();
                Ok(Some(Expr::Value(self.parse_value()?)))
            }
            Keyword::CURRENT_CATALOG
            | Keyword::CURRENT_USER
            | Keyword::SESSION_USER
            | Keyword::USER
            if dialect_of!(self is PostgreSqlDialect | GenericDialect) =>
                {
                    Ok(Some(Expr::Function(Function {
                        name: ObjectName(vec![w.to_ident(w_span)]),
                        parameters: FunctionArguments::None,
                        args: FunctionArguments::None,
                        null_treatment: None,
                        filter: None,
                        over: None,
                        within_group: vec![],
                    })))
                }
            Keyword::CURRENT_TIMESTAMP
            | Keyword::CURRENT_TIME
            | Keyword::CURRENT_DATE
            | Keyword::LOCALTIME
            | Keyword::LOCALTIMESTAMP => {
                Ok(Some(self.parse_time_functions(ObjectName(vec![w.to_ident(w_span)]))?))
            }
            Keyword::CASE => Ok(Some(self.parse_case_expr()?)),
            Keyword::CONVERT => Ok(Some(self.parse_convert_expr(false)?)),
            Keyword::TRY_CONVERT if self.dialect.supports_try_convert() => Ok(Some(self.parse_convert_expr(true)?)),
            Keyword::CAST => Ok(Some(self.parse_cast_expr(CastKind::Cast)?)),
            Keyword::TRY_CAST => Ok(Some(self.parse_cast_expr(CastKind::TryCast)?)),
            Keyword::SAFE_CAST => Ok(Some(self.parse_cast_expr(CastKind::SafeCast)?)),
            Keyword::EXISTS
            // Support parsing Databricks has a function named `exists`.
            if !dialect_of!(self is DatabricksDialect)
                || matches!(
                        self.peek_nth_token(1).token,
                        Token::Word(Word {
                            keyword: Keyword::SELECT | Keyword::WITH,
                            ..
                        })
                    ) =>
                {
                    Ok(Some(self.parse_exists_expr(false)?))
                }
            Keyword::EXTRACT => Ok(Some(self.parse_extract_expr()?)),
            Keyword::CEIL => Ok(Some(self.parse_ceil_floor_expr(true)?)),
            Keyword::FLOOR => Ok(Some(self.parse_ceil_floor_expr(false)?)),
            Keyword::POSITION if self.peek_token().token == Token::LParen => {
                Ok(Some(self.parse_position_expr(w.to_ident(w_span))?))
            }
            Keyword::SUBSTRING => Ok(Some(self.parse_substring_expr()?)),
            Keyword::OVERLAY => Ok(Some(self.parse_overlay_expr()?)),
            Keyword::TRIM => Ok(Some(self.parse_trim_expr()?)),
            Keyword::INTERVAL => Ok(Some(self.parse_interval()?)),
            // Treat ARRAY[1,2,3] as an array [1,2,3], otherwise try as subquery or a function call
            Keyword::ARRAY if self.peek_token() == Token::LBracket => {
                self.expect_token(&Token::LBracket)?;
                Ok(Some(self.parse_array_expr(true)?))
            }
            Keyword::ARRAY
            if self.peek_token() == Token::LParen
                && !dialect_of!(self is ClickHouseDialect | DatabricksDialect) =>
                {
                    self.expect_token(&Token::LParen)?;
                    let query = self.parse_query()?;
                    self.expect_token(&Token::RParen)?;
                    Ok(Some(Expr::Function(Function {
                        name: ObjectName(vec![w.to_ident(w_span)]),
                        parameters: FunctionArguments::None,
                        args: FunctionArguments::Subquery(query),
                        filter: None,
                        null_treatment: None,
                        over: None,
                        within_group: vec![],
                    })))
                }
            Keyword::NOT => Ok(Some(self.parse_not()?)),
            Keyword::MATCH if dialect_of!(self is MySqlDialect | GenericDialect) => {
                Ok(Some(self.parse_match_against()?))
            }
            Keyword::STRUCT if self.dialect.supports_struct_literal() => {
                Ok(Some(self.parse_struct_literal()?))
            }
            Keyword::PRIOR if matches!(self.state, ParserState::ConnectBy) => {
                let expr = self.parse_subexpr(self.dialect.prec_value(Precedence::PlusMinus))?;
                Ok(Some(Expr::Prior(Box::new(expr))))
            }
            Keyword::MAP if self.peek_token() == Token::LBrace && self.dialect.support_map_literal_syntax() => {
                Ok(Some(self.parse_duckdb_map_literal()?))
            }
            _ => Ok(None)
        }
    }

    // Tries to parse an expression by a word that is not known to have a special meaning in the dialect.
    fn parse_expr_prefix_by_unreserved_word(
        &mut self,
        w: &Word,
        w_span: Span,
    ) -> Result<Expr, ParserError> {
        match self.peek_token().token {
            Token::LParen | Token::Period => {
                let mut id_parts: Vec<Ident> = vec![w.to_ident(w_span)];
                let mut ending_wildcard: Option<TokenWithSpan> = None;
                while self.consume_token(&Token::Period) {
                    let next_token = self.next_token();
                    match next_token.token {
                        Token::Word(w) => id_parts.push(w.to_ident(next_token.span)),
                        Token::Mul => {
                            // Postgres explicitly allows funcnm(tablenm.*) and the
                            // function array_agg traverses this control flow
                            if dialect_of!(self is PostgreSqlDialect) {
                                ending_wildcard = Some(next_token);
                                break;
                            } else {
                                return self.expected("an identifier after '.'", next_token);
                            }
                        }
                        Token::SingleQuotedString(s) => id_parts.push(Ident::with_quote('\'', s)),
                        _ => {
                            return self.expected("an identifier or a '*' after '.'", next_token);
                        }
                    }
                }

                if let Some(wildcard_token) = ending_wildcard {
                    Ok(Expr::QualifiedWildcard(
                        ObjectName(id_parts),
                        AttachedToken(wildcard_token),
                    ))
                } else if self.consume_token(&Token::LParen) {
                    if dialect_of!(self is SnowflakeDialect | MsSqlDialect)
                        && self.consume_tokens(&[Token::Plus, Token::RParen])
                    {
                        Ok(Expr::OuterJoin(Box::new(
                            match <[Ident; 1]>::try_from(id_parts) {
                                Ok([ident]) => Expr::Identifier(ident),
                                Err(parts) => Expr::CompoundIdentifier(parts),
                            },
                        )))
                    } else {
                        self.prev_token();
                        self.parse_function(ObjectName(id_parts))
                    }
                } else {
                    Ok(Expr::CompoundIdentifier(id_parts))
                }
            }
            // string introducer https://dev.mysql.com/doc/refman/8.0/en/charset-introducer.html
            Token::SingleQuotedString(_)
            | Token::DoubleQuotedString(_)
            | Token::HexStringLiteral(_)
                if w.value.starts_with('_') =>
            {
                Ok(Expr::IntroducedString {
                    introducer: w.value.clone(),
                    value: self.parse_introduced_string_value()?,
                })
            }
            Token::Arrow if self.dialect.supports_lambda_functions() => {
                self.expect_token(&Token::Arrow)?;
                Ok(Expr::Lambda(LambdaFunction {
                    params: OneOrManyWithParens::One(w.to_ident(w_span)),
                    body: Box::new(self.parse_expr()?),
                }))
            }
            _ => Ok(Expr::Identifier(w.to_ident(w_span))),
        }
    }

    /// Parses a potentially empty list of arguments to a window function
    /// (including the closing parenthesis).
    ///
    /// Examples:
    /// ```sql
    /// FIRST_VALUE(x ORDER BY 1,2,3);
    /// FIRST_VALUE(x IGNORE NULL);
    /// ```
    fn parse_function_argument_list(&mut self) -> Result<FunctionArgumentList, ParserError> {
        let mut clauses = vec![];

        // For MSSQL empty argument list with json-null-clause case, e.g. `JSON_ARRAY(NULL ON NULL)`
        if let Some(null_clause) = self.parse_json_null_clause() {
            clauses.push(FunctionArgumentClause::JsonNullClause(null_clause));
        }

        if self.consume_token(&Token::RParen) {
            return Ok(FunctionArgumentList {
                duplicate_treatment: None,
                args: vec![],
                clauses,
            });
        }

        let duplicate_treatment = self.parse_duplicate_treatment()?;
        let args = self.parse_comma_separated(Parser::parse_function_args)?;

        if self.dialect.supports_window_function_null_treatment_arg() {
            if let Some(null_treatment) = self.parse_null_treatment()? {
                clauses.push(FunctionArgumentClause::IgnoreOrRespectNulls(null_treatment));
            }
        }

        if self.parse_keywords(&[Keyword::ORDER, Keyword::BY]) {
            clauses.push(FunctionArgumentClause::OrderBy(
                self.parse_comma_separated(Parser::parse_order_by_expr)?,
            ));
        }

        if self.parse_keyword(Keyword::LIMIT) {
            clauses.push(FunctionArgumentClause::Limit(self.parse_expr()?));
        }

        if dialect_of!(self is GenericDialect | BigQueryDialect)
            && self.parse_keyword(Keyword::HAVING)
        {
            let kind = match self.expect_one_of_keywords(&[Keyword::MIN, Keyword::MAX])? {
                Keyword::MIN => HavingBoundKind::Min,
                Keyword::MAX => HavingBoundKind::Max,
                _ => unreachable!(),
            };
            clauses.push(FunctionArgumentClause::Having(HavingBound(
                kind,
                self.parse_expr()?,
            )))
        }

        if dialect_of!(self is GenericDialect | MySqlDialect)
            && self.parse_keyword(Keyword::SEPARATOR)
        {
            clauses.push(FunctionArgumentClause::Separator(self.parse_value()?));
        }

        if let Some(on_overflow) = self.parse_listagg_on_overflow()? {
            clauses.push(FunctionArgumentClause::OnOverflow(on_overflow));
        }

        if let Some(null_clause) = self.parse_json_null_clause() {
            clauses.push(FunctionArgumentClause::JsonNullClause(null_clause));
        }

        self.expect_token(&Token::RParen)?;
        Ok(FunctionArgumentList {
            duplicate_treatment,
            args,
            clauses,
        })
    }

    /// Parse a group by expr. Group by expr can be one of group sets, roll up, cube, or simple expr.
    fn parse_group_by_expr(&mut self) -> Result<Expr, ParserError> {
        if self.dialect.supports_group_by_expr() {
            if self.parse_keywords(&[Keyword::GROUPING, Keyword::SETS]) {
                self.expect_token(&Token::LParen)?;
                let result = self.parse_comma_separated(|p| p.parse_tuple(false, true))?;
                self.expect_token(&Token::RParen)?;
                Ok(Expr::GroupingSets(result))
            } else if self.parse_keyword(Keyword::CUBE) {
                self.expect_token(&Token::LParen)?;
                let result = self.parse_comma_separated(|p| p.parse_tuple(true, true))?;
                self.expect_token(&Token::RParen)?;
                Ok(Expr::Cube(result))
            } else if self.parse_keyword(Keyword::ROLLUP) {
                self.expect_token(&Token::LParen)?;
                let result = self.parse_comma_separated(|p| p.parse_tuple(true, true))?;
                self.expect_token(&Token::RParen)?;
                Ok(Expr::Rollup(result))
            } else if self.consume_tokens(&[Token::LParen, Token::RParen]) {
                // PostgreSQL allow to use empty tuple as a group by expression,
                // e.g. `GROUP BY (), name`. Please refer to GROUP BY Clause section in
                // [PostgreSQL](https://www.postgresql.org/docs/16/sql-select.html)
                Ok(Expr::Tuple(vec![]))
            } else {
                self.parse_expr()
            }
        } else {
            // TODO parse rollup for other dialects
            self.parse_expr()
        }
    }

    fn parse_json_access(&mut self, expr: Expr) -> Result<Expr, ParserError> {
        let path = self.parse_json_path()?;
        Ok(Expr::JsonAccess {
            value: Box::new(expr),
            path,
        })
    }

    /// Parses MSSQL's json-null-clause
    fn parse_json_null_clause(&mut self) -> Option<JsonNullClause> {
        if self.parse_keywords(&[Keyword::ABSENT, Keyword::ON, Keyword::NULL]) {
            Some(JsonNullClause::AbsentOnNull)
        } else if self.parse_keywords(&[Keyword::NULL, Keyword::ON, Keyword::NULL]) {
            Some(JsonNullClause::NullOnNull)
        } else {
            None
        }
    }

    pub(crate) fn parse_json_path(&mut self) -> Result<JsonPath, ParserError> {
        let mut path = Vec::new();
        loop {
            match self.next_token().token {
                Token::Colon if path.is_empty() => {
                    path.push(self.parse_json_path_object_key()?);
                }
                Token::Period if !path.is_empty() => {
                    path.push(self.parse_json_path_object_key()?);
                }
                Token::LBracket => {
                    let key = self.parse_expr()?;
                    self.expect_token(&Token::RBracket)?;

                    path.push(JsonPathElem::Bracket { key });
                }
                _ => {
                    self.prev_token();
                    break;
                }
            };
        }

        debug_assert!(!path.is_empty());
        Ok(JsonPath { path })
    }

    fn parse_json_path_object_key(&mut self) -> Result<JsonPathElem, ParserError> {
        let token = self.next_token();
        match token.token {
            Token::Word(Word {
                value,
                // path segments in SF dot notation can be unquoted or double-quoted
                quote_style: quote_style @ (Some('"') | None),
                // some experimentation suggests that snowflake permits
                // any keyword here unquoted.
                keyword: _,
            }) => Ok(JsonPathElem::Dot {
                key: value,
                quoted: quote_style.is_some(),
            }),

            // This token should never be generated on snowflake or generic
            // dialects, but we handle it just in case this is used on future
            // dialects.
            Token::DoubleQuotedString(key) => Ok(JsonPathElem::Dot { key, quoted: true }),

            _ => self.expected("variant object key name", token),
        }
    }

    /// Optionally parses a null treatment clause.
    fn parse_null_treatment(&mut self) -> Result<Option<NullTreatment>, ParserError> {
        match self.parse_one_of_keywords(&[Keyword::RESPECT, Keyword::IGNORE]) {
            Some(keyword) => {
                self.expect_keyword(Keyword::NULLS)?;

                Ok(match keyword {
                    Keyword::RESPECT => Some(NullTreatment::RespectNulls),
                    Keyword::IGNORE => Some(NullTreatment::IgnoreNulls),
                    _ => None,
                })
            }
            None => Ok(None),
        }
    }

    /// Parse an expression value for a struct literal
    /// Syntax
    /// ```sql
    /// expr [AS name]
    /// ```
    ///
    /// For biquery [1], Parameter typed_syntax is set to true if the expression
    /// is to be parsed as a field expression declared using typed
    /// struct syntax [2], and false if using typeless struct syntax [3].
    ///
    /// [1]: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#constructing_a_struct
    /// [2]: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#typed_struct_syntax
    /// [3]: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#typeless_struct_syntax
    fn parse_struct_field_expr(&mut self, typed_syntax: bool) -> Result<Expr, ParserError> {
        let expr = self.parse_expr()?;
        if self.parse_keyword(Keyword::AS) {
            if typed_syntax {
                return parser_err!("Typed syntax does not allow AS", {
                    self.prev_token();
                    self.peek_token().span.start
                });
            }
            let field_name = self.parse_identifier(false)?;
            Ok(Expr::Named {
                expr: expr.into(),
                name: field_name,
            })
        } else {
            Ok(expr)
        }
    }

    /// Syntax
    /// ```sql
    /// -- typed
    /// STRUCT<[field_name] field_type, ...>( expr1 [, ... ])
    /// -- typeless
    /// STRUCT( expr1 [AS field_name] [, ... ])
    /// ```
    fn parse_struct_literal(&mut self) -> Result<Expr, ParserError> {
        // Parse the fields definition if exist `<[field_name] field_type, ...>`
        self.prev_token();
        let (fields, trailing_bracket) =
            self.parse_struct_type_def(Self::parse_struct_field_def)?;
        if trailing_bracket.0 {
            return parser_err!(
                "unmatched > in STRUCT literal",
                self.peek_token().span.start
            );
        }

        // Parse the struct values `(expr1 [, ... ])`
        self.expect_token(&Token::LParen)?;
        let values = self
            .parse_comma_separated(|parser| parser.parse_struct_field_expr(!fields.is_empty()))?;
        self.expect_token(&Token::RParen)?;

        Ok(Expr::Struct { values, fields })
    }

    /// Parses an array subscript like
    /// * `[:]`
    /// * `[l]`
    /// * `[l:]`
    /// * `[:u]`
    /// * `[l:u]`
    /// * `[l:u:s]`
    ///
    /// Parser is right after `[`
    fn parse_subscript_inner(&mut self) -> Result<Subscript, ParserError> {
        // at either `<lower>:(rest)` or `:(rest)]`
        let lower_bound = if self.consume_token(&Token::Colon) {
            None
        } else {
            Some(self.parse_expr()?)
        };

        // check for end
        if self.consume_token(&Token::RBracket) {
            if let Some(lower_bound) = lower_bound {
                return Ok(Subscript::Index { index: lower_bound });
            };
            return Ok(Subscript::Slice {
                lower_bound,
                upper_bound: None,
                stride: None,
            });
        }

        // consume the `:`
        if lower_bound.is_some() {
            self.expect_token(&Token::Colon)?;
        }

        // we are now at either `]`, `<upper>(rest)]`
        let upper_bound = if self.consume_token(&Token::RBracket) {
            return Ok(Subscript::Slice {
                lower_bound,
                upper_bound: None,
                stride: None,
            });
        } else {
            Some(self.parse_expr()?)
        };

        // check for end
        if self.consume_token(&Token::RBracket) {
            return Ok(Subscript::Slice {
                lower_bound,
                upper_bound,
                stride: None,
            });
        }

        // we are now at `:]` or `:stride]`
        self.expect_token(&Token::Colon)?;
        let stride = if self.consume_token(&Token::RBracket) {
            None
        } else {
            Some(self.parse_expr()?)
        };

        if stride.is_some() {
            self.expect_token(&Token::RBracket)?;
        }

        Ok(Subscript::Slice {
            lower_bound,
            upper_bound,
            stride,
        })
    }

    /// Parse a tuple with `(` and `)`.
    /// If `lift_singleton` is true, then a singleton tuple is lifted to a tuple of length 1, otherwise it will fail.
    /// If `allow_empty` is true, then an empty tuple is allowed.
    fn parse_tuple(
        &mut self,
        lift_singleton: bool,
        allow_empty: bool,
    ) -> Result<Vec<Expr>, ParserError> {
        if lift_singleton {
            if self.consume_token(&Token::LParen) {
                let result = if allow_empty && self.consume_token(&Token::RParen) {
                    vec![]
                } else {
                    let result = self.parse_comma_separated(Parser::parse_expr)?;
                    self.expect_token(&Token::RParen)?;
                    result
                };
                Ok(result)
            } else {
                Ok(vec![self.parse_expr()?])
            }
        } else {
            self.expect_token(&Token::LParen)?;
            let result = if allow_empty && self.consume_token(&Token::RParen) {
                vec![]
            } else {
                let result = self.parse_comma_separated(Parser::parse_expr)?;
                self.expect_token(&Token::RParen)?;
                result
            };
            Ok(result)
        }
    }

    fn try_parse_lambda(&mut self) -> Result<Option<Expr>, ParserError> {
        if !self.dialect.supports_lambda_functions() {
            return Ok(None);
        }
        self.maybe_parse(|p| {
            let params = p.parse_comma_separated(|p| p.parse_identifier(false))?;
            p.expect_token(&Token::RParen)?;
            p.expect_token(&Token::Arrow)?;
            let expr = p.parse_expr()?;
            Ok(Expr::Lambda(LambdaFunction {
                params: OneOrManyWithParens::Many(params),
                body: Box::new(expr),
            }))
        })
    }

    /// Parses method call expression
    fn try_parse_method(&mut self, expr: Expr) -> Result<Expr, ParserError> {
        if !self.dialect.supports_methods() {
            return Ok(expr);
        }
        let method_chain = self.maybe_parse(|p| {
            let mut method_chain = Vec::new();
            while p.consume_token(&Token::Period) {
                let tok = p.next_token();
                let name = match tok.token {
                    Token::Word(word) => word.to_ident(tok.span),
                    _ => return p.expected("identifier", tok),
                };
                let func = match p.parse_function(ObjectName(vec![name]))? {
                    Expr::Function(func) => func,
                    _ => return p.expected("function", p.peek_token()),
                };
                method_chain.push(func);
            }
            if !method_chain.is_empty() {
                Ok(method_chain)
            } else {
                p.expected("function", p.peek_token())
            }
        })?;
        if let Some(method_chain) = method_chain {
            Ok(Expr::Method(Method {
                expr: Box::new(expr),
                method_chain,
            }))
        } else {
            Ok(expr)
        }
    }
}

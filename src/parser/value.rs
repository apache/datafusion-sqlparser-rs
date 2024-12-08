use super::*;

use crate::parser_err;

impl Parser<'_> {
    /// Parse a literal value (numbers, strings, date/time, booleans)
    pub fn parse_value(&mut self) -> Result<Value, ParserError> {
        let next_token = self.next_token();
        let span = next_token.span;
        match next_token.token {
            Token::Word(w) => match w.keyword {
                Keyword::TRUE if self.dialect.supports_boolean_literals() => {
                    Ok(Value::Boolean(true))
                }
                Keyword::FALSE if self.dialect.supports_boolean_literals() => {
                    Ok(Value::Boolean(false))
                }
                Keyword::NULL => Ok(Value::Null),
                Keyword::NoKeyword if w.quote_style.is_some() => match w.quote_style {
                    Some('"') => Ok(Value::DoubleQuotedString(w.value)),
                    Some('\'') => Ok(Value::SingleQuotedString(w.value)),
                    _ => self.expected(
                        "A value?",
                        TokenWithSpan {
                            token: Token::Word(w),
                            span,
                        },
                    )?,
                },
                _ => self.expected(
                    "a concrete value",
                    TokenWithSpan {
                        token: Token::Word(w),
                        span,
                    },
                ),
            },
            // The call to n.parse() returns a bigdecimal when the
            // bigdecimal feature is enabled, and is otherwise a no-op
            // (i.e., it returns the input string).
            Token::Number(n, l) => Ok(Value::Number(Self::parse(n, span.start)?, l)),
            Token::SingleQuotedString(ref s) => Ok(Value::SingleQuotedString(s.to_string())),
            Token::DoubleQuotedString(ref s) => Ok(Value::DoubleQuotedString(s.to_string())),
            Token::TripleSingleQuotedString(ref s) => {
                Ok(Value::TripleSingleQuotedString(s.to_string()))
            }
            Token::TripleDoubleQuotedString(ref s) => {
                Ok(Value::TripleDoubleQuotedString(s.to_string()))
            }
            Token::DollarQuotedString(ref s) => Ok(Value::DollarQuotedString(s.clone())),
            Token::SingleQuotedByteStringLiteral(ref s) => {
                Ok(Value::SingleQuotedByteStringLiteral(s.clone()))
            }
            Token::DoubleQuotedByteStringLiteral(ref s) => {
                Ok(Value::DoubleQuotedByteStringLiteral(s.clone()))
            }
            Token::TripleSingleQuotedByteStringLiteral(ref s) => {
                Ok(Value::TripleSingleQuotedByteStringLiteral(s.clone()))
            }
            Token::TripleDoubleQuotedByteStringLiteral(ref s) => {
                Ok(Value::TripleDoubleQuotedByteStringLiteral(s.clone()))
            }
            Token::SingleQuotedRawStringLiteral(ref s) => {
                Ok(Value::SingleQuotedRawStringLiteral(s.clone()))
            }
            Token::DoubleQuotedRawStringLiteral(ref s) => {
                Ok(Value::DoubleQuotedRawStringLiteral(s.clone()))
            }
            Token::TripleSingleQuotedRawStringLiteral(ref s) => {
                Ok(Value::TripleSingleQuotedRawStringLiteral(s.clone()))
            }
            Token::TripleDoubleQuotedRawStringLiteral(ref s) => {
                Ok(Value::TripleDoubleQuotedRawStringLiteral(s.clone()))
            }
            Token::NationalStringLiteral(ref s) => Ok(Value::NationalStringLiteral(s.to_string())),
            Token::EscapedStringLiteral(ref s) => Ok(Value::EscapedStringLiteral(s.to_string())),
            Token::UnicodeStringLiteral(ref s) => Ok(Value::UnicodeStringLiteral(s.to_string())),
            Token::HexStringLiteral(ref s) => Ok(Value::HexStringLiteral(s.to_string())),
            Token::Placeholder(ref s) => Ok(Value::Placeholder(s.to_string())),
            tok @ Token::Colon | tok @ Token::AtSign => {
                // Not calling self.parse_identifier(false)? because only in placeholder we want to check numbers as idfentifies
                // This because snowflake allows numbers as placeholders
                let next_token = self.next_token();
                let ident = match next_token.token {
                    Token::Word(w) => Ok(w.to_ident(next_token.span)),
                    Token::Number(w, false) => Ok(Ident::new(w)),
                    _ => self.expected("placeholder", next_token),
                }?;
                let placeholder = tok.to_string() + &ident.value;
                Ok(Value::Placeholder(placeholder))
            }
            unexpected => self.expected(
                "a value",
                TokenWithSpan {
                    token: unexpected,
                    span,
                },
            ),
        }
    }

    pub fn parse_values(&mut self, allow_empty: bool) -> Result<Values, ParserError> {
        let mut explicit_row = false;

        let rows = self.parse_comma_separated(|parser| {
            if parser.parse_keyword(Keyword::ROW) {
                explicit_row = true;
            }

            parser.expect_token(&Token::LParen)?;
            if allow_empty && parser.peek_token().token == Token::RParen {
                parser.next_token();
                Ok(vec![])
            } else {
                let exprs = parser.parse_comma_separated(Parser::parse_expr)?;
                parser.expect_token(&Token::RParen)?;
                Ok(exprs)
            }
        })?;
        Ok(Values { explicit_row, rows })
    }

    /// Parse an unsigned numeric literal
    pub fn parse_number_value(&mut self) -> Result<Value, ParserError> {
        match self.parse_value()? {
            v @ Value::Number(_, _) => Ok(v),
            v @ Value::Placeholder(_) => Ok(v),
            _ => {
                self.prev_token();
                self.expected("literal number", self.peek_token())
            }
        }
    }

    /// Parse a numeric literal as an expression. Returns a [`Expr::UnaryOp`] if the number is signed,
    /// otherwise returns a [`Expr::Value`]
    pub fn parse_number(&mut self) -> Result<Expr, ParserError> {
        let next_token = self.next_token();
        match next_token.token {
            Token::Plus => Ok(Expr::UnaryOp {
                op: UnaryOperator::Plus,
                expr: Box::new(Expr::Value(self.parse_number_value()?)),
            }),
            Token::Minus => Ok(Expr::UnaryOp {
                op: UnaryOperator::Minus,
                expr: Box::new(Expr::Value(self.parse_number_value()?)),
            }),
            _ => {
                self.prev_token();
                Ok(Expr::Value(self.parse_number_value()?))
            }
        }
    }

    pub(crate) fn parse_introduced_string_value(&mut self) -> Result<Value, ParserError> {
        let next_token = self.next_token();
        let span = next_token.span;
        match next_token.token {
            Token::SingleQuotedString(ref s) => Ok(Value::SingleQuotedString(s.to_string())),
            Token::DoubleQuotedString(ref s) => Ok(Value::DoubleQuotedString(s.to_string())),
            Token::HexStringLiteral(ref s) => Ok(Value::HexStringLiteral(s.to_string())),
            unexpected => self.expected(
                "a string value",
                TokenWithSpan {
                    token: unexpected,
                    span,
                },
            ),
        }
    }

    /// Parse an unsigned literal integer/long
    pub fn parse_literal_uint(&mut self) -> Result<u64, ParserError> {
        let next_token = self.next_token();
        match next_token.token {
            Token::Number(s, _) => Self::parse::<u64>(s, next_token.span.start),
            _ => self.expected("literal int", next_token),
        }
    }

    /// Parse a literal string
    pub fn parse_literal_string(&mut self) -> Result<String, ParserError> {
        let next_token = self.next_token();
        match next_token.token {
            Token::Word(Word {
                value,
                keyword: Keyword::NoKeyword,
                ..
            }) => Ok(value),
            Token::SingleQuotedString(s) => Ok(s),
            Token::DoubleQuotedString(s) => Ok(s),
            Token::EscapedStringLiteral(s) if dialect_of!(self is PostgreSqlDialect | GenericDialect) => {
                Ok(s)
            }
            Token::UnicodeStringLiteral(s) => Ok(s),
            _ => self.expected("literal string", next_token),
        }
    }

    pub fn parse_enum_values(&mut self) -> Result<Vec<EnumMember>, ParserError> {
        self.expect_token(&Token::LParen)?;
        let values = self.parse_comma_separated(|parser| {
            let name = parser.parse_literal_string()?;
            let e = if parser.consume_token(&Token::Eq) {
                let value = parser.parse_number()?;
                EnumMember::NamedValue(name, value)
            } else {
                EnumMember::Name(name)
            };
            Ok(e)
        })?;
        self.expect_token(&Token::RParen)?;

        Ok(values)
    }

    /// Parse datetime64 [1]
    /// Syntax
    /// ```sql
    /// DateTime64(precision[, timezone])
    /// ```
    ///
    /// [1]: https://clickhouse.com/docs/en/sql-reference/data-types/datetime64
    pub fn parse_datetime_64(&mut self) -> Result<(u64, Option<String>), ParserError> {
        self.expect_keyword(Keyword::DATETIME64)?;
        self.expect_token(&Token::LParen)?;
        let precision = self.parse_literal_uint()?;
        let time_zone = if self.consume_token(&Token::Comma) {
            Some(self.parse_literal_string()?)
        } else {
            None
        };
        self.expect_token(&Token::RParen)?;
        Ok((precision, time_zone))
    }

    /// Parse a SQL datatype (in the context of a CREATE TABLE statement for example)
    pub fn parse_data_type(&mut self) -> Result<DataType, ParserError> {
        let (ty, trailing_bracket) = self.parse_data_type_helper()?;
        if trailing_bracket.0 {
            return parser_err!(
                format!("unmatched > after parsing data type {ty}"),
                self.peek_token()
            );
        }

        Ok(ty)
    }

    pub(crate) fn parse_data_type_helper(
        &mut self,
    ) -> Result<(DataType, MatchedTrailingBracket), ParserError> {
        let next_token = self.next_token();
        let mut trailing_bracket: MatchedTrailingBracket = false.into();
        let mut data = match next_token.token {
            Token::Word(w) => match w.keyword {
                Keyword::BOOLEAN => Ok(DataType::Boolean),
                Keyword::BOOL => Ok(DataType::Bool),
                Keyword::FLOAT => Ok(DataType::Float(self.parse_optional_precision()?)),
                Keyword::REAL => Ok(DataType::Real),
                Keyword::FLOAT4 => Ok(DataType::Float4),
                Keyword::FLOAT32 => Ok(DataType::Float32),
                Keyword::FLOAT64 => Ok(DataType::Float64),
                Keyword::FLOAT8 => Ok(DataType::Float8),
                Keyword::DOUBLE => {
                    if self.parse_keyword(Keyword::PRECISION) {
                        Ok(DataType::DoublePrecision)
                    } else {
                        Ok(DataType::Double)
                    }
                }
                Keyword::TINYINT => {
                    let optional_precision = self.parse_optional_precision();
                    if self.parse_keyword(Keyword::UNSIGNED) {
                        Ok(DataType::UnsignedTinyInt(optional_precision?))
                    } else {
                        Ok(DataType::TinyInt(optional_precision?))
                    }
                }
                Keyword::INT2 => {
                    let optional_precision = self.parse_optional_precision();
                    if self.parse_keyword(Keyword::UNSIGNED) {
                        Ok(DataType::UnsignedInt2(optional_precision?))
                    } else {
                        Ok(DataType::Int2(optional_precision?))
                    }
                }
                Keyword::SMALLINT => {
                    let optional_precision = self.parse_optional_precision();
                    if self.parse_keyword(Keyword::UNSIGNED) {
                        Ok(DataType::UnsignedSmallInt(optional_precision?))
                    } else {
                        Ok(DataType::SmallInt(optional_precision?))
                    }
                }
                Keyword::MEDIUMINT => {
                    let optional_precision = self.parse_optional_precision();
                    if self.parse_keyword(Keyword::UNSIGNED) {
                        Ok(DataType::UnsignedMediumInt(optional_precision?))
                    } else {
                        Ok(DataType::MediumInt(optional_precision?))
                    }
                }
                Keyword::INT => {
                    let optional_precision = self.parse_optional_precision();
                    if self.parse_keyword(Keyword::UNSIGNED) {
                        Ok(DataType::UnsignedInt(optional_precision?))
                    } else {
                        Ok(DataType::Int(optional_precision?))
                    }
                }
                Keyword::INT4 => {
                    let optional_precision = self.parse_optional_precision();
                    if self.parse_keyword(Keyword::UNSIGNED) {
                        Ok(DataType::UnsignedInt4(optional_precision?))
                    } else {
                        Ok(DataType::Int4(optional_precision?))
                    }
                }
                Keyword::INT8 => {
                    let optional_precision = self.parse_optional_precision();
                    if self.parse_keyword(Keyword::UNSIGNED) {
                        Ok(DataType::UnsignedInt8(optional_precision?))
                    } else {
                        Ok(DataType::Int8(optional_precision?))
                    }
                }
                Keyword::INT16 => Ok(DataType::Int16),
                Keyword::INT32 => Ok(DataType::Int32),
                Keyword::INT64 => Ok(DataType::Int64),
                Keyword::INT128 => Ok(DataType::Int128),
                Keyword::INT256 => Ok(DataType::Int256),
                Keyword::INTEGER => {
                    let optional_precision = self.parse_optional_precision();
                    if self.parse_keyword(Keyword::UNSIGNED) {
                        Ok(DataType::UnsignedInteger(optional_precision?))
                    } else {
                        Ok(DataType::Integer(optional_precision?))
                    }
                }
                Keyword::BIGINT => {
                    let optional_precision = self.parse_optional_precision();
                    if self.parse_keyword(Keyword::UNSIGNED) {
                        Ok(DataType::UnsignedBigInt(optional_precision?))
                    } else {
                        Ok(DataType::BigInt(optional_precision?))
                    }
                }
                Keyword::UINT8 => Ok(DataType::UInt8),
                Keyword::UINT16 => Ok(DataType::UInt16),
                Keyword::UINT32 => Ok(DataType::UInt32),
                Keyword::UINT64 => Ok(DataType::UInt64),
                Keyword::UINT128 => Ok(DataType::UInt128),
                Keyword::UINT256 => Ok(DataType::UInt256),
                Keyword::VARCHAR => Ok(DataType::Varchar(self.parse_optional_character_length()?)),
                Keyword::NVARCHAR => {
                    Ok(DataType::Nvarchar(self.parse_optional_character_length()?))
                }
                Keyword::CHARACTER => {
                    if self.parse_keyword(Keyword::VARYING) {
                        Ok(DataType::CharacterVarying(
                            self.parse_optional_character_length()?,
                        ))
                    } else if self.parse_keywords(&[Keyword::LARGE, Keyword::OBJECT]) {
                        Ok(DataType::CharacterLargeObject(
                            self.parse_optional_precision()?,
                        ))
                    } else {
                        Ok(DataType::Character(self.parse_optional_character_length()?))
                    }
                }
                Keyword::CHAR => {
                    if self.parse_keyword(Keyword::VARYING) {
                        Ok(DataType::CharVarying(
                            self.parse_optional_character_length()?,
                        ))
                    } else if self.parse_keywords(&[Keyword::LARGE, Keyword::OBJECT]) {
                        Ok(DataType::CharLargeObject(self.parse_optional_precision()?))
                    } else {
                        Ok(DataType::Char(self.parse_optional_character_length()?))
                    }
                }
                Keyword::CLOB => Ok(DataType::Clob(self.parse_optional_precision()?)),
                Keyword::BINARY => Ok(DataType::Binary(self.parse_optional_precision()?)),
                Keyword::VARBINARY => Ok(DataType::Varbinary(self.parse_optional_precision()?)),
                Keyword::BLOB => Ok(DataType::Blob(self.parse_optional_precision()?)),
                Keyword::TINYBLOB => Ok(DataType::TinyBlob),
                Keyword::MEDIUMBLOB => Ok(DataType::MediumBlob),
                Keyword::LONGBLOB => Ok(DataType::LongBlob),
                Keyword::BYTES => Ok(DataType::Bytes(self.parse_optional_precision()?)),
                Keyword::BIT => {
                    if self.parse_keyword(Keyword::VARYING) {
                        Ok(DataType::BitVarying(self.parse_optional_precision()?))
                    } else {
                        Ok(DataType::Bit(self.parse_optional_precision()?))
                    }
                }
                Keyword::UUID => Ok(DataType::Uuid),
                Keyword::DATE => Ok(DataType::Date),
                Keyword::DATE32 => Ok(DataType::Date32),
                Keyword::DATETIME => Ok(DataType::Datetime(self.parse_optional_precision()?)),
                Keyword::DATETIME64 => {
                    self.prev_token();
                    let (precision, time_zone) = self.parse_datetime_64()?;
                    Ok(DataType::Datetime64(precision, time_zone))
                }
                Keyword::TIMESTAMP => {
                    let precision = self.parse_optional_precision()?;
                    let tz = if self.parse_keyword(Keyword::WITH) {
                        self.expect_keywords(&[Keyword::TIME, Keyword::ZONE])?;
                        TimezoneInfo::WithTimeZone
                    } else if self.parse_keyword(Keyword::WITHOUT) {
                        self.expect_keywords(&[Keyword::TIME, Keyword::ZONE])?;
                        TimezoneInfo::WithoutTimeZone
                    } else {
                        TimezoneInfo::None
                    };
                    Ok(DataType::Timestamp(precision, tz))
                }
                Keyword::TIMESTAMPTZ => Ok(DataType::Timestamp(
                    self.parse_optional_precision()?,
                    TimezoneInfo::Tz,
                )),
                Keyword::TIME => {
                    let precision = self.parse_optional_precision()?;
                    let tz = if self.parse_keyword(Keyword::WITH) {
                        self.expect_keywords(&[Keyword::TIME, Keyword::ZONE])?;
                        TimezoneInfo::WithTimeZone
                    } else if self.parse_keyword(Keyword::WITHOUT) {
                        self.expect_keywords(&[Keyword::TIME, Keyword::ZONE])?;
                        TimezoneInfo::WithoutTimeZone
                    } else {
                        TimezoneInfo::None
                    };
                    Ok(DataType::Time(precision, tz))
                }
                Keyword::TIMETZ => Ok(DataType::Time(
                    self.parse_optional_precision()?,
                    TimezoneInfo::Tz,
                )),
                // Interval types can be followed by a complicated interval
                // qualifier that we don't currently support. See
                // parse_interval for a taste.
                Keyword::INTERVAL => Ok(DataType::Interval),
                Keyword::JSON => Ok(DataType::JSON),
                Keyword::JSONB => Ok(DataType::JSONB),
                Keyword::REGCLASS => Ok(DataType::Regclass),
                Keyword::STRING => Ok(DataType::String(self.parse_optional_precision()?)),
                Keyword::FIXEDSTRING => {
                    self.expect_token(&Token::LParen)?;
                    let character_length = self.parse_literal_uint()?;
                    self.expect_token(&Token::RParen)?;
                    Ok(DataType::FixedString(character_length))
                }
                Keyword::TEXT => Ok(DataType::Text),
                Keyword::TINYTEXT => Ok(DataType::TinyText),
                Keyword::MEDIUMTEXT => Ok(DataType::MediumText),
                Keyword::LONGTEXT => Ok(DataType::LongText),
                Keyword::BYTEA => Ok(DataType::Bytea),
                Keyword::NUMERIC => Ok(DataType::Numeric(
                    self.parse_exact_number_optional_precision_scale()?,
                )),
                Keyword::DECIMAL => Ok(DataType::Decimal(
                    self.parse_exact_number_optional_precision_scale()?,
                )),
                Keyword::DEC => Ok(DataType::Dec(
                    self.parse_exact_number_optional_precision_scale()?,
                )),
                Keyword::BIGNUMERIC => Ok(DataType::BigNumeric(
                    self.parse_exact_number_optional_precision_scale()?,
                )),
                Keyword::BIGDECIMAL => Ok(DataType::BigDecimal(
                    self.parse_exact_number_optional_precision_scale()?,
                )),
                Keyword::ENUM => Ok(DataType::Enum(self.parse_enum_values()?, None)),
                Keyword::ENUM8 => Ok(DataType::Enum(self.parse_enum_values()?, Some(8))),
                Keyword::ENUM16 => Ok(DataType::Enum(self.parse_enum_values()?, Some(16))),
                Keyword::SET => Ok(DataType::Set(self.parse_string_values()?)),
                Keyword::ARRAY => {
                    if dialect_of!(self is SnowflakeDialect) {
                        Ok(DataType::Array(ArrayElemTypeDef::None))
                    } else if dialect_of!(self is ClickHouseDialect) {
                        Ok(self.parse_sub_type(|internal_type| {
                            DataType::Array(ArrayElemTypeDef::Parenthesis(internal_type))
                        })?)
                    } else {
                        self.expect_token(&Token::Lt)?;
                        let (inside_type, _trailing_bracket) = self.parse_data_type_helper()?;
                        trailing_bracket = self.expect_closing_angle_bracket(_trailing_bracket)?;
                        Ok(DataType::Array(ArrayElemTypeDef::AngleBracket(Box::new(
                            inside_type,
                        ))))
                    }
                }
                Keyword::STRUCT if dialect_of!(self is DuckDbDialect) => {
                    self.prev_token();
                    let field_defs = self.parse_duckdb_struct_type_def()?;
                    Ok(DataType::Struct(field_defs, StructBracketKind::Parentheses))
                }
                Keyword::STRUCT if dialect_of!(self is BigQueryDialect | GenericDialect) => {
                    self.prev_token();
                    let (field_defs, _trailing_bracket) =
                        self.parse_struct_type_def(Self::parse_struct_field_def)?;
                    trailing_bracket = _trailing_bracket;
                    Ok(DataType::Struct(
                        field_defs,
                        StructBracketKind::AngleBrackets,
                    ))
                }
                Keyword::UNION if dialect_of!(self is DuckDbDialect | GenericDialect) => {
                    self.prev_token();
                    let fields = self.parse_union_type_def()?;
                    Ok(DataType::Union(fields))
                }
                Keyword::NULLABLE if dialect_of!(self is ClickHouseDialect | GenericDialect) => {
                    Ok(self.parse_sub_type(DataType::Nullable)?)
                }
                Keyword::LOWCARDINALITY if dialect_of!(self is ClickHouseDialect | GenericDialect) => {
                    Ok(self.parse_sub_type(DataType::LowCardinality)?)
                }
                Keyword::MAP if dialect_of!(self is ClickHouseDialect | GenericDialect) => {
                    self.prev_token();
                    let (key_data_type, value_data_type) = self.parse_click_house_map_def()?;
                    Ok(DataType::Map(
                        Box::new(key_data_type),
                        Box::new(value_data_type),
                    ))
                }
                Keyword::NESTED if dialect_of!(self is ClickHouseDialect | GenericDialect) => {
                    self.expect_token(&Token::LParen)?;
                    let field_defs = self.parse_comma_separated(Parser::parse_column_def)?;
                    self.expect_token(&Token::RParen)?;
                    Ok(DataType::Nested(field_defs))
                }
                Keyword::TUPLE if dialect_of!(self is ClickHouseDialect | GenericDialect) => {
                    self.prev_token();
                    let field_defs = self.parse_click_house_tuple_def()?;
                    Ok(DataType::Tuple(field_defs))
                }
                Keyword::TRIGGER => Ok(DataType::Trigger),
                _ => {
                    self.prev_token();
                    let type_name = self.parse_object_name(false)?;
                    if let Some(modifiers) = self.parse_optional_type_modifiers()? {
                        Ok(DataType::Custom(type_name, modifiers))
                    } else {
                        Ok(DataType::Custom(type_name, vec![]))
                    }
                }
            },
            _ => self.expected("a data type name", next_token),
        }?;

        // Parse array data types. Note: this is postgresql-specific and different from
        // Keyword::ARRAY syntax from above
        while self.consume_token(&Token::LBracket) {
            let size = if dialect_of!(self is GenericDialect | DuckDbDialect | PostgreSqlDialect) {
                self.maybe_parse(|p| p.parse_literal_uint())?
            } else {
                None
            };
            self.expect_token(&Token::RBracket)?;
            data = DataType::Array(ArrayElemTypeDef::SquareBracket(Box::new(data), size))
        }
        Ok((data, trailing_bracket))
    }

    pub fn parse_exact_number_optional_precision_scale(
        &mut self,
    ) -> Result<ExactNumberInfo, ParserError> {
        if self.consume_token(&Token::LParen) {
            let precision = self.parse_literal_uint()?;
            let scale = if self.consume_token(&Token::Comma) {
                Some(self.parse_literal_uint()?)
            } else {
                None
            };

            self.expect_token(&Token::RParen)?;

            match scale {
                None => Ok(ExactNumberInfo::Precision(precision)),
                Some(scale) => Ok(ExactNumberInfo::PrecisionAndScale(precision, scale)),
            }
        } else {
            Ok(ExactNumberInfo::None)
        }
    }

    pub fn parse_optional_character_length(
        &mut self,
    ) -> Result<Option<CharacterLength>, ParserError> {
        if self.consume_token(&Token::LParen) {
            let character_length = self.parse_character_length()?;
            self.expect_token(&Token::RParen)?;
            Ok(Some(character_length))
        } else {
            Ok(None)
        }
    }

    pub fn parse_optional_precision(&mut self) -> Result<Option<u64>, ParserError> {
        if self.consume_token(&Token::LParen) {
            let n = self.parse_literal_uint()?;
            self.expect_token(&Token::RParen)?;
            Ok(Some(n))
        } else {
            Ok(None)
        }
    }

    pub fn parse_optional_type_modifiers(&mut self) -> Result<Option<Vec<String>>, ParserError> {
        if self.consume_token(&Token::LParen) {
            let mut modifiers = Vec::new();
            loop {
                let next_token = self.next_token();
                match next_token.token {
                    Token::Word(w) => modifiers.push(w.to_string()),
                    Token::Number(n, _) => modifiers.push(n),
                    Token::SingleQuotedString(s) => modifiers.push(s),

                    Token::Comma => {
                        continue;
                    }
                    Token::RParen => {
                        break;
                    }
                    _ => self.expected("type modifiers", next_token)?,
                }
            }

            Ok(Some(modifiers))
        } else {
            Ok(None)
        }
    }

    pub fn parse_string_values(&mut self) -> Result<Vec<String>, ParserError> {
        self.expect_token(&Token::LParen)?;
        let mut values = Vec::new();
        loop {
            let next_token = self.next_token();
            match next_token.token {
                Token::SingleQuotedString(value) => values.push(value),
                _ => self.expected("a string", next_token)?,
            }
            let next_token = self.next_token();
            match next_token.token {
                Token::Comma => (),
                Token::RParen => break,
                _ => self.expected(", or }", next_token)?,
            }
        }
        Ok(values)
    }

    /// Parse a field definition in a [struct] or [tuple].
    /// Syntax:
    ///
    /// ```sql
    /// [field_name] field_type
    /// ```
    ///
    /// [struct]: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#declaring_a_struct_type
    /// [tuple]: https://clickhouse.com/docs/en/sql-reference/data-types/tuple
    pub(crate) fn parse_struct_field_def(
        &mut self,
    ) -> Result<(StructField, MatchedTrailingBracket), ParserError> {
        // Look beyond the next item to infer whether both field name
        // and type are specified.
        let is_anonymous_field = !matches!(
            (self.peek_nth_token(0).token, self.peek_nth_token(1).token),
            (Token::Word(_), Token::Word(_))
        );

        let field_name = if is_anonymous_field {
            None
        } else {
            Some(self.parse_identifier(false)?)
        };

        let (field_type, trailing_bracket) = self.parse_data_type_helper()?;

        Ok((
            StructField {
                field_name,
                field_type,
            },
            trailing_bracket,
        ))
    }

    /// Parse a parenthesized sub data type
    fn parse_sub_type<F>(&mut self, parent_type: F) -> Result<DataType, ParserError>
    where
        F: FnOnce(Box<DataType>) -> DataType,
    {
        self.expect_token(&Token::LParen)?;
        let inside_type = self.parse_data_type()?;
        self.expect_token(&Token::RParen)?;
        Ok(parent_type(inside_type.into()))
    }

    /// For nested types that use the angle bracket syntax, this matches either
    /// `>`, `>>` or nothing depending on which variant is expected (specified by the previously
    /// matched `trailing_bracket` argument). It returns whether there is a trailing
    /// left to be matched - (i.e. if '>>' was matched).
    pub(crate) fn expect_closing_angle_bracket(
        &mut self,
        trailing_bracket: MatchedTrailingBracket,
    ) -> Result<MatchedTrailingBracket, ParserError> {
        let trailing_bracket = if !trailing_bracket.0 {
            match self.peek_token().token {
                Token::Gt => {
                    self.next_token();
                    false.into()
                }
                Token::ShiftRight => {
                    self.next_token();
                    true.into()
                }
                _ => return self.expected(">", self.peek_token()),
            }
        } else {
            false.into()
        };

        Ok(trailing_bracket)
    }

    /// DuckDB specific: Parse a Union type definition as a sequence of field-value pairs.
    ///
    /// Syntax:
    ///
    /// ```sql
    /// UNION(field_name field_type[,...])
    /// ```
    ///
    /// [1]: https://duckdb.org/docs/sql/data_types/union.html
    fn parse_union_type_def(&mut self) -> Result<Vec<UnionField>, ParserError> {
        self.expect_keyword(Keyword::UNION)?;

        self.expect_token(&Token::LParen)?;

        let fields = self.parse_comma_separated(|p| {
            Ok(UnionField {
                field_name: p.parse_identifier(false)?,
                field_type: p.parse_data_type()?,
            })
        })?;

        self.expect_token(&Token::RParen)?;

        Ok(fields)
    }
}

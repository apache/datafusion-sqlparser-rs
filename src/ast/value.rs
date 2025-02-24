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
use alloc::string::String;

use core::fmt;

#[cfg(feature = "bigdecimal")]
use bigdecimal::BigDecimal;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use crate::{ast::Ident, tokenizer::Span};
#[cfg(feature = "visitor")]
use sqlparser_derive::{Visit, VisitMut};

/// Wraps a primitive SQL [`Value`]  with its [`Span`] location
///
/// # Example: create a `ValueWithSpan` from a `Value`
/// ```
/// # use sqlparser::ast::{Value, ValueWithSpan};
/// # use sqlparser::tokenizer::{Location, Span};
/// let value = Value::SingleQuotedString(String::from("endpoint"));
/// // from line 1, column 1 to line 1, column 7
/// let span = Span::new(Location::new(1, 1), Location::new(1, 7));
/// let value_with_span = value.with_span(span);
/// ```
///
/// # Example: create a `ValueWithSpan` from a `Value` with an empty span
///
/// You can call [`Value::with_empty_span`] to create a `ValueWithSpan` with an empty span
/// ```
/// # use sqlparser::ast::{Value, ValueWithSpan};
/// # use sqlparser::tokenizer::{Location, Span};
/// let value = Value::SingleQuotedString(String::from("endpoint"));
/// let value_with_span = value.with_empty_span();
/// assert_eq!(value_with_span.span, Span::empty());
/// ```
///
/// You can also use the [`From`] trait to convert  `ValueWithSpan` to/from `Value`s
/// ```
/// # use sqlparser::ast::{Value, ValueWithSpan};
/// # use sqlparser::tokenizer::{Location, Span};
/// let value = Value::SingleQuotedString(String::from("endpoint"));
/// // converting `Value` to `ValueWithSpan` results in an empty span
/// let value_with_span: ValueWithSpan = value.into();
/// assert_eq!(value_with_span.span, Span::empty());
/// // convert back to `Value`
/// let value: Value = value_with_span.into();
/// ```
#[derive(Debug, Clone, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct ValueWithSpan {
    pub value: Value,
    pub span: Span,
}

impl PartialEq for ValueWithSpan {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}

impl Ord for ValueWithSpan {
    fn cmp(&self, other: &Self) -> core::cmp::Ordering {
        self.value.cmp(&other.value)
    }
}

impl PartialOrd for ValueWithSpan {
    fn partial_cmp(&self, other: &Self) -> Option<core::cmp::Ordering> {
        Some(Ord::cmp(self, other))
    }
}

impl core::hash::Hash for ValueWithSpan {
    fn hash<H: core::hash::Hasher>(&self, state: &mut H) {
        self.value.hash(state);
    }
}

impl From<Value> for ValueWithSpan {
    fn from(value: Value) -> Self {
        value.with_empty_span()
    }
}

impl From<ValueWithSpan> for Value {
    fn from(value: ValueWithSpan) -> Self {
        value.value
    }
}

/// Primitive SQL values such as number and string
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(
    feature = "visitor",
    derive(Visit, VisitMut),
    visit(with = "visit_value")
)]

pub enum Value {
    /// Numeric literal
    #[cfg(not(feature = "bigdecimal"))]
    Number(String, bool),
    #[cfg(feature = "bigdecimal")]
    // HINT: use `test_utils::number` to make an instance of
    // Value::Number This might help if you your tests pass locally
    // but fail on CI with the `--all-features` flag enabled
    Number(BigDecimal, bool),
    /// 'string value'
    SingleQuotedString(String),
    // $<tag_name>$string value$<tag_name>$ (postgres syntax)
    DollarQuotedString(DollarQuotedString),
    /// Triple single quoted strings: Example '''abc'''
    /// [BigQuery](https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#quoted_literals)
    TripleSingleQuotedString(String),
    /// Triple double quoted strings: Example """abc"""
    /// [BigQuery](https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#quoted_literals)
    TripleDoubleQuotedString(String),
    /// e'string value' (postgres extension)
    /// See [Postgres docs](https://www.postgresql.org/docs/8.3/sql-syntax-lexical.html#SQL-SYNTAX-STRINGS)
    /// for more details.
    EscapedStringLiteral(String),
    /// u&'string value' (postgres extension)
    /// See [Postgres docs](https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-STRINGS-UESCAPE)
    /// for more details.
    UnicodeStringLiteral(String),
    /// B'string value'
    SingleQuotedByteStringLiteral(String),
    /// B"string value"
    DoubleQuotedByteStringLiteral(String),
    /// Triple single quoted literal with byte string prefix. Example `B'''abc'''`
    /// [BigQuery](https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#quoted_literals)
    TripleSingleQuotedByteStringLiteral(String),
    /// Triple double quoted literal with byte string prefix. Example `B"""abc"""`
    /// [BigQuery](https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#quoted_literals)
    TripleDoubleQuotedByteStringLiteral(String),
    /// Single quoted literal with raw string prefix. Example `R'abc'`
    /// [BigQuery](https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#quoted_literals)
    SingleQuotedRawStringLiteral(String),
    /// Double quoted literal with raw string prefix. Example `R"abc"`
    /// [BigQuery](https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#quoted_literals)
    DoubleQuotedRawStringLiteral(String),
    /// Triple single quoted literal with raw string prefix. Example `R'''abc'''`
    /// [BigQuery](https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#quoted_literals)
    TripleSingleQuotedRawStringLiteral(String),
    /// Triple double quoted literal with raw string prefix. Example `R"""abc"""`
    /// [BigQuery](https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#quoted_literals)
    TripleDoubleQuotedRawStringLiteral(String),
    /// N'string value'
    NationalStringLiteral(String),
    /// X'hex value'
    HexStringLiteral(String),

    DoubleQuotedString(String),
    /// Boolean value true or false
    Boolean(bool),
    /// `NULL` value
    Null,
    /// `?` or `$` Prepared statement arg placeholder
    Placeholder(String),
}

impl ValueWithSpan {
    /// If the underlying literal is a string, regardless of quote style, returns the associated string value
    pub fn into_string(self) -> Option<String> {
        self.value.into_string()
    }
}

impl Value {
    /// If the underlying literal is a string, regardless of quote style, returns the associated string value
    pub fn into_string(self) -> Option<String> {
        match self {
            Value::SingleQuotedString(s)
            | Value::DoubleQuotedString(s)
            | Value::TripleSingleQuotedString(s)
            | Value::TripleDoubleQuotedString(s)
            | Value::SingleQuotedByteStringLiteral(s)
            | Value::DoubleQuotedByteStringLiteral(s)
            | Value::TripleSingleQuotedByteStringLiteral(s)
            | Value::TripleDoubleQuotedByteStringLiteral(s)
            | Value::SingleQuotedRawStringLiteral(s)
            | Value::DoubleQuotedRawStringLiteral(s)
            | Value::TripleSingleQuotedRawStringLiteral(s)
            | Value::TripleDoubleQuotedRawStringLiteral(s)
            | Value::EscapedStringLiteral(s)
            | Value::UnicodeStringLiteral(s)
            | Value::NationalStringLiteral(s)
            | Value::HexStringLiteral(s) => Some(s),
            Value::DollarQuotedString(s) => Some(s.value),
            _ => None,
        }
    }

    pub fn with_span(self, span: Span) -> ValueWithSpan {
        ValueWithSpan { value: self, span }
    }

    pub fn with_empty_span(self) -> ValueWithSpan {
        self.with_span(Span::empty())
    }
}

impl fmt::Display for ValueWithSpan {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.value)
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Value::Number(v, l) => write!(f, "{}{long}", v, long = if *l { "L" } else { "" }),
            Value::DoubleQuotedString(v) => write!(f, "\"{}\"", escape_double_quote_string(v)),
            Value::SingleQuotedString(v) => write!(f, "'{}'", escape_single_quote_string(v)),
            Value::TripleSingleQuotedString(v) => {
                write!(f, "'''{v}'''")
            }
            Value::TripleDoubleQuotedString(v) => {
                write!(f, r#""""{v}""""#)
            }
            Value::DollarQuotedString(v) => write!(f, "{v}"),
            Value::EscapedStringLiteral(v) => write!(f, "E'{}'", escape_escaped_string(v)),
            Value::UnicodeStringLiteral(v) => write!(f, "U&'{}'", escape_unicode_string(v)),
            Value::NationalStringLiteral(v) => write!(f, "N'{v}'"),
            Value::HexStringLiteral(v) => write!(f, "X'{v}'"),
            Value::Boolean(v) => write!(f, "{v}"),
            Value::SingleQuotedByteStringLiteral(v) => write!(f, "B'{v}'"),
            Value::DoubleQuotedByteStringLiteral(v) => write!(f, "B\"{v}\""),
            Value::TripleSingleQuotedByteStringLiteral(v) => write!(f, "B'''{v}'''"),
            Value::TripleDoubleQuotedByteStringLiteral(v) => write!(f, r#"B"""{v}""""#),
            Value::SingleQuotedRawStringLiteral(v) => write!(f, "R'{v}'"),
            Value::DoubleQuotedRawStringLiteral(v) => write!(f, "R\"{v}\""),
            Value::TripleSingleQuotedRawStringLiteral(v) => write!(f, "R'''{v}'''"),
            Value::TripleDoubleQuotedRawStringLiteral(v) => write!(f, r#"R"""{v}""""#),
            Value::Null => write!(f, "NULL"),
            Value::Placeholder(v) => write!(f, "{v}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct DollarQuotedString {
    pub value: String,
    pub tag: Option<String>,
}

impl fmt::Display for DollarQuotedString {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self.tag {
            Some(tag) => {
                write!(f, "${}${}${}$", tag, self.value, tag)
            }
            None => {
                write!(f, "$${}$$", self.value)
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum DateTimeField {
    Year,
    Years,
    Month,
    Months,
    /// Week optionally followed by a WEEKDAY.
    ///
    /// ```sql
    /// WEEK(MONDAY)
    /// ```
    ///
    /// [BigQuery](https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions#extract)
    Week(Option<Ident>),
    Weeks,
    Day,
    DayOfWeek,
    DayOfYear,
    Days,
    Date,
    Datetime,
    Hour,
    Hours,
    Minute,
    Minutes,
    Second,
    Seconds,
    Century,
    Decade,
    Dow,
    Doy,
    Epoch,
    Isodow,
    IsoWeek,
    Isoyear,
    Julian,
    Microsecond,
    Microseconds,
    Millenium,
    Millennium,
    Millisecond,
    Milliseconds,
    Nanosecond,
    Nanoseconds,
    Quarter,
    Time,
    Timezone,
    TimezoneAbbr,
    TimezoneHour,
    TimezoneMinute,
    TimezoneRegion,
    NoDateTime,
    /// Arbitrary abbreviation or custom date-time part.
    ///
    /// ```sql
    /// EXTRACT(q FROM CURRENT_TIMESTAMP)
    /// ```
    /// [Snowflake](https://docs.snowflake.com/en/sql-reference/functions-date-time#supported-date-and-time-parts)
    Custom(Ident),
}

impl fmt::Display for DateTimeField {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            DateTimeField::Year => write!(f, "YEAR"),
            DateTimeField::Years => write!(f, "YEARS"),
            DateTimeField::Month => write!(f, "MONTH"),
            DateTimeField::Months => write!(f, "MONTHS"),
            DateTimeField::Week(week_day) => {
                write!(f, "WEEK")?;
                if let Some(week_day) = week_day {
                    write!(f, "({week_day})")?
                }
                Ok(())
            }
            DateTimeField::Weeks => write!(f, "WEEKS"),
            DateTimeField::Day => write!(f, "DAY"),
            DateTimeField::DayOfWeek => write!(f, "DAYOFWEEK"),
            DateTimeField::DayOfYear => write!(f, "DAYOFYEAR"),
            DateTimeField::Days => write!(f, "DAYS"),
            DateTimeField::Date => write!(f, "DATE"),
            DateTimeField::Datetime => write!(f, "DATETIME"),
            DateTimeField::Hour => write!(f, "HOUR"),
            DateTimeField::Hours => write!(f, "HOURS"),
            DateTimeField::Minute => write!(f, "MINUTE"),
            DateTimeField::Minutes => write!(f, "MINUTES"),
            DateTimeField::Second => write!(f, "SECOND"),
            DateTimeField::Seconds => write!(f, "SECONDS"),
            DateTimeField::Century => write!(f, "CENTURY"),
            DateTimeField::Decade => write!(f, "DECADE"),
            DateTimeField::Dow => write!(f, "DOW"),
            DateTimeField::Doy => write!(f, "DOY"),
            DateTimeField::Epoch => write!(f, "EPOCH"),
            DateTimeField::Isodow => write!(f, "ISODOW"),
            DateTimeField::Isoyear => write!(f, "ISOYEAR"),
            DateTimeField::IsoWeek => write!(f, "ISOWEEK"),
            DateTimeField::Julian => write!(f, "JULIAN"),
            DateTimeField::Microsecond => write!(f, "MICROSECOND"),
            DateTimeField::Microseconds => write!(f, "MICROSECONDS"),
            DateTimeField::Millenium => write!(f, "MILLENIUM"),
            DateTimeField::Millennium => write!(f, "MILLENNIUM"),
            DateTimeField::Millisecond => write!(f, "MILLISECOND"),
            DateTimeField::Milliseconds => write!(f, "MILLISECONDS"),
            DateTimeField::Nanosecond => write!(f, "NANOSECOND"),
            DateTimeField::Nanoseconds => write!(f, "NANOSECONDS"),
            DateTimeField::Quarter => write!(f, "QUARTER"),
            DateTimeField::Time => write!(f, "TIME"),
            DateTimeField::Timezone => write!(f, "TIMEZONE"),
            DateTimeField::TimezoneAbbr => write!(f, "TIMEZONE_ABBR"),
            DateTimeField::TimezoneHour => write!(f, "TIMEZONE_HOUR"),
            DateTimeField::TimezoneMinute => write!(f, "TIMEZONE_MINUTE"),
            DateTimeField::TimezoneRegion => write!(f, "TIMEZONE_REGION"),
            DateTimeField::NoDateTime => write!(f, "NODATETIME"),
            DateTimeField::Custom(custom) => write!(f, "{custom}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
/// The Unicode Standard defines four normalization forms, which are intended to eliminate
/// certain distinctions between visually or functionally identical characters.
///
/// See [Unicode Normalization Forms](https://unicode.org/reports/tr15/) for details.
pub enum NormalizationForm {
    /// Canonical Decomposition, followed by Canonical Composition.
    NFC,
    /// Canonical Decomposition.
    NFD,
    /// Compatibility Decomposition, followed by Canonical Composition.
    NFKC,
    /// Compatibility Decomposition.
    NFKD,
}

impl fmt::Display for NormalizationForm {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            NormalizationForm::NFC => write!(f, "NFC"),
            NormalizationForm::NFD => write!(f, "NFD"),
            NormalizationForm::NFKC => write!(f, "NFKC"),
            NormalizationForm::NFKD => write!(f, "NFKD"),
        }
    }
}

pub struct EscapeQuotedString<'a> {
    string: &'a str,
    quote: char,
}

impl fmt::Display for EscapeQuotedString<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // EscapeQuotedString doesn't know which mode of escape was
        // chosen by the user. So this code must to correctly display
        // strings without knowing if the strings are already escaped
        // or not.
        //
        // If the quote symbol in the string is repeated twice, OR, if
        // the quote symbol is after backslash, display all the chars
        // without any escape. However, if the quote symbol is used
        // just between usual chars, `fmt()` should display it twice."
        //
        // The following table has examples
        //
        // | original query | mode      | AST Node                                           | serialized   |
        // | -------------  | --------- | -------------------------------------------------- | ------------ |
        // | `"A""B""A"`    | no-escape | `DoubleQuotedString(String::from("A\"\"B\"\"A"))`  | `"A""B""A"`  |
        // | `"A""B""A"`    | default   | `DoubleQuotedString(String::from("A\"B\"A"))`      | `"A""B""A"`  |
        // | `"A\"B\"A"`    | no-escape | `DoubleQuotedString(String::from("A\\\"B\\\"A"))`  | `"A\"B\"A"`  |
        // | `"A\"B\"A"`    | default   | `DoubleQuotedString(String::from("A\"B\"A"))`      | `"A""B""A"`  |
        let quote = self.quote;
        let mut previous_char = char::default();
        let mut peekable_chars = self.string.chars().peekable();
        while let Some(&ch) = peekable_chars.peek() {
            match ch {
                char if char == quote => {
                    if previous_char == '\\' {
                        write!(f, "{char}")?;
                        peekable_chars.next();
                        continue;
                    }
                    peekable_chars.next();
                    if peekable_chars.peek().map(|c| *c == quote).unwrap_or(false) {
                        write!(f, "{char}{char}")?;
                        peekable_chars.next();
                    } else {
                        write!(f, "{char}{char}")?;
                    }
                }
                _ => {
                    write!(f, "{ch}")?;
                    peekable_chars.next();
                }
            }
            previous_char = ch;
        }
        Ok(())
    }
}

pub fn escape_quoted_string(string: &str, quote: char) -> EscapeQuotedString<'_> {
    EscapeQuotedString { string, quote }
}

pub fn escape_single_quote_string(s: &str) -> EscapeQuotedString<'_> {
    escape_quoted_string(s, '\'')
}

pub fn escape_double_quote_string(s: &str) -> EscapeQuotedString<'_> {
    escape_quoted_string(s, '\"')
}

pub struct EscapeEscapedStringLiteral<'a>(&'a str);

impl fmt::Display for EscapeEscapedStringLiteral<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for c in self.0.chars() {
            match c {
                '\'' => {
                    write!(f, r#"\'"#)?;
                }
                '\\' => {
                    write!(f, r#"\\"#)?;
                }
                '\n' => {
                    write!(f, r#"\n"#)?;
                }
                '\t' => {
                    write!(f, r#"\t"#)?;
                }
                '\r' => {
                    write!(f, r#"\r"#)?;
                }
                _ => {
                    write!(f, "{c}")?;
                }
            }
        }
        Ok(())
    }
}

pub fn escape_escaped_string(s: &str) -> EscapeEscapedStringLiteral<'_> {
    EscapeEscapedStringLiteral(s)
}

pub struct EscapeUnicodeStringLiteral<'a>(&'a str);

impl fmt::Display for EscapeUnicodeStringLiteral<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for c in self.0.chars() {
            match c {
                '\'' => {
                    write!(f, "''")?;
                }
                '\\' => {
                    write!(f, r#"\\"#)?;
                }
                x if x.is_ascii() => {
                    write!(f, "{}", c)?;
                }
                _ => {
                    let codepoint = c as u32;
                    // if the character fits in 32 bits, we can use the \XXXX format
                    // otherwise, we need to use the \+XXXXXX format
                    if codepoint <= 0xFFFF {
                        write!(f, "\\{:04X}", codepoint)?;
                    } else {
                        write!(f, "\\+{:06X}", codepoint)?;
                    }
                }
            }
        }
        Ok(())
    }
}

pub fn escape_unicode_string(s: &str) -> EscapeUnicodeStringLiteral<'_> {
    EscapeUnicodeStringLiteral(s)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum TrimWhereField {
    Both,
    Leading,
    Trailing,
}

impl fmt::Display for TrimWhereField {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use TrimWhereField::*;
        f.write_str(match self {
            Both => "BOTH",
            Leading => "LEADING",
            Trailing => "TRAILING",
        })
    }
}

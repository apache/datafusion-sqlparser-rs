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
use alloc::string::String;

#[cfg(not(feature = "std"))]
use alloc::format;

#[cfg(not(feature = "std"))]
use alloc::string::ToString;

use core::fmt;

#[cfg(feature = "bigdecimal")]
use bigdecimal::BigDecimal;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use crate::ast::Ident;
use crate::ast::{display_comma_separated, Expr};
#[cfg(feature = "visitor")]
use sqlparser_derive::{Visit, VisitMut};

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct ObjectConstantKeyValue {
    pub key: String,
    pub value: Box<Expr>,
}

/// Primitive SQL values such as number and string
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
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
    /// e'string value' (postgres extension)
    /// See [Postgres docs](https://www.postgresql.org/docs/8.3/sql-syntax-lexical.html#SQL-SYNTAX-STRINGS)
    /// for more details.
    EscapedStringLiteral(String),
    /// B'string value'
    SingleQuotedByteStringLiteral(String),
    /// B"string value"
    DoubleQuotedByteStringLiteral(String),
    /// R'string value' or r'string value' or r"string value"
    /// <https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#quoted_literals>
    RawStringLiteral(String),
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
    /// Add support of snowflake field:key - key should be a value
    UnQuotedString(String),

    /// OBJECT constant as used by Snowflake
    /// https://docs.snowflake.com/en/sql-reference/data-types-semistructured#object-constants
    ObjectConstant(Vec<ObjectConstantKeyValue>),
    Array(Vec<Value>),
    /// TUPLE as used by BigQuery
    /// ("org_unit", "development")
    Tuple(Vec<Value>),
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Value::Number(v, l) => write!(f, "{}{long}", v, long = if *l { "L" } else { "" }),
            Value::DoubleQuotedString(v) => write!(f, "\"{}\"", escape_double_quote_string(v)),
            Value::SingleQuotedString(v) => write!(f, "'{}'", escape_single_quote_string(v)),
            Value::DollarQuotedString(v) => write!(f, "{v}"),
            Value::EscapedStringLiteral(v) => write!(f, "E'{}'", escape_escaped_string(v)),
            Value::NationalStringLiteral(v) => write!(f, "N'{v}'"),
            Value::HexStringLiteral(v) => write!(f, "X'{v}'"),
            Value::Boolean(v) => write!(f, "{v}"),
            Value::SingleQuotedByteStringLiteral(v) => write!(f, "B'{v}'"),
            Value::DoubleQuotedByteStringLiteral(v) => write!(f, "B\"{v}\""),
            Value::RawStringLiteral(v) => write!(f, "R'{v}'"),
            Value::Null => write!(f, "NULL"),
            Value::Placeholder(v) => write!(f, "{v}"),
            Value::UnQuotedString(v) => write!(f, "{v}"),
            Value::ObjectConstant(fields) => {
                if fields.is_empty() {
                    write!(f, "{}", "{}")
                } else {
                    let mut first = true;
                    write!(f, "{}", "{ ")?;
                    for ObjectConstantKeyValue { key, value } in fields {
                        if first {
                            first = false;
                        } else {
                            write!(f, ", ")?;
                        }
                        write!(f, "'{}': {}", key, value)?;
                    }
                    write!(f, "{}", " }")
                }
            }
            Value::Array(values) => {
                write!(f, "[{}]", display_comma_separated(values))
            }
            Value::Tuple(values) => {
                write!(f, "({})", display_comma_separated(values))
            }
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
    Month,
    /// Week optionally followed by a WEEKDAY.
    ///
    /// ```sql
    /// WEEK(MONDAY)
    /// ```
    ///
    /// [BigQuery](https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions#extract)
    Week(Option<Ident>),
    Day,
    DayOfWeek,
    DayOfYear,
    Date,
    Datetime,
    Hour,
    Minute,
    Second,
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
        f.write_str(
            match self {
                DateTimeField::Year => "YEAR".to_string(),
                DateTimeField::Month => "MONTH".to_string(),
                DateTimeField::Week(week_day) => {
                    format!(
                        "WEEK{}",
                        week_day
                            .as_ref()
                            .map(|w| format!("({w})"))
                            .unwrap_or_default()
                    )
                }
                DateTimeField::Day => "DAY".to_string(),
                DateTimeField::DayOfWeek => "DAYOFWEEK".to_string(),
                DateTimeField::DayOfYear => "DAYOFYEAR".to_string(),
                DateTimeField::Date => "DATE".to_string(),
                DateTimeField::Datetime => "DATETIME".to_string(),
                DateTimeField::Hour => "HOUR".to_string(),
                DateTimeField::Minute => "MINUTE".to_string(),
                DateTimeField::Second => "SECOND".to_string(),
                DateTimeField::Century => "CENTURY".to_string(),
                DateTimeField::Decade => "DECADE".to_string(),
                DateTimeField::Dow => "DOW".to_string(),
                DateTimeField::Doy => "DOY".to_string(),
                DateTimeField::Epoch => "EPOCH".to_string(),
                DateTimeField::Isodow => "ISODOW".to_string(),
                DateTimeField::Isoyear => "ISOYEAR".to_string(),
                DateTimeField::IsoWeek => "ISOWEEK".to_string(),
                DateTimeField::Julian => "JULIAN".to_string(),
                DateTimeField::Microsecond => "MICROSECOND".to_string(),
                DateTimeField::Microseconds => "MICROSECONDS".to_string(),
                DateTimeField::Millenium => "MILLENIUM".to_string(),
                DateTimeField::Millennium => "MILLENNIUM".to_string(),
                DateTimeField::Millisecond => "MILLISECOND".to_string(),
                DateTimeField::Milliseconds => "MILLISECONDS".to_string(),
                DateTimeField::Nanosecond => "NANOSECOND".to_string(),
                DateTimeField::Nanoseconds => "NANOSECONDS".to_string(),
                DateTimeField::Quarter => "QUARTER".to_string(),
                DateTimeField::Time => "TIME".to_string(),
                DateTimeField::Timezone => "TIMEZONE".to_string(),
                DateTimeField::TimezoneAbbr => "TIMEZONE_ABBR".to_string(),
                DateTimeField::TimezoneHour => "TIMEZONE_HOUR".to_string(),
                DateTimeField::TimezoneMinute => "TIMEZONE_MINUTE".to_string(),
                DateTimeField::TimezoneRegion => "TIMEZONE_REGION".to_string(),
                DateTimeField::NoDateTime => "NODATETIME".to_string(),
                DateTimeField::Custom(custom) => format!("{custom}"),
            }
            .as_str(),
        )
    }
}

pub struct EscapeQuotedString<'a> {
    string: &'a str,
    quote: char,
}

impl<'a> fmt::Display for EscapeQuotedString<'a> {
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

impl<'a> fmt::Display for EscapeEscapedStringLiteral<'a> {
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

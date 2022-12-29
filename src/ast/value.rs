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
use core::fmt;

#[cfg(feature = "bigdecimal")]
use bigdecimal::BigDecimal;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

#[cfg(feature = "visitor")]
use sqlparser_derive::Visit;

/// Primitive SQL values such as number and string
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit))]
pub enum Value {
    /// Numeric literal
    #[cfg(not(feature = "bigdecimal"))]
    Number(String, bool),
    #[cfg(feature = "bigdecimal")]
    Number(BigDecimal, bool),
    /// 'string value'
    SingleQuotedString(String),
    // $<tag_name>$string value$<tag_name>$ (postgres syntax)
    DollarQuotedString(DollarQuotedString),
    /// e'string value' (postgres extension)
    /// See [Postgres docs](https://www.postgresql.org/docs/8.3/sql-syntax-lexical.html#SQL-SYNTAX-STRINGS)
    /// for more details.
    EscapedStringLiteral(String),
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
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Value::Number(v, l) => write!(f, "{}{long}", v, long = if *l { "L" } else { "" }),
            Value::DoubleQuotedString(v) => write!(f, "\"{}\"", v),
            Value::SingleQuotedString(v) => write!(f, "'{}'", escape_single_quote_string(v)),
            Value::DollarQuotedString(v) => write!(f, "{}", v),
            Value::EscapedStringLiteral(v) => write!(f, "E'{}'", escape_escaped_string(v)),
            Value::NationalStringLiteral(v) => write!(f, "N'{}'", v),
            Value::HexStringLiteral(v) => write!(f, "X'{}'", v),
            Value::Boolean(v) => write!(f, "{}", v),
            Value::Null => write!(f, "NULL"),
            Value::Placeholder(v) => write!(f, "{}", v),
            Value::UnQuotedString(v) => write!(f, "{}", v),
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit))]
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

#[derive(Debug, Copy, Clone, PartialEq, Eq, Ord, PartialOrd, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit))]
pub enum DateTimeField {
    Year,
    Month,
    Week,
    Day,
    Date,
    Hour,
    Minute,
    Second,
    Century,
    Decade,
    Dow,
    Doy,
    Epoch,
    Isodow,
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
    Timezone,
    TimezoneHour,
    TimezoneMinute,
    NoDateTime,
}

impl fmt::Display for DateTimeField {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(match self {
            DateTimeField::Year => "YEAR",
            DateTimeField::Month => "MONTH",
            DateTimeField::Week => "WEEK",
            DateTimeField::Day => "DAY",
            DateTimeField::Date => "DATE",
            DateTimeField::Hour => "HOUR",
            DateTimeField::Minute => "MINUTE",
            DateTimeField::Second => "SECOND",
            DateTimeField::Century => "CENTURY",
            DateTimeField::Decade => "DECADE",
            DateTimeField::Dow => "DOW",
            DateTimeField::Doy => "DOY",
            DateTimeField::Epoch => "EPOCH",
            DateTimeField::Isodow => "ISODOW",
            DateTimeField::Isoyear => "ISOYEAR",
            DateTimeField::Julian => "JULIAN",
            DateTimeField::Microsecond => "MICROSECOND",
            DateTimeField::Microseconds => "MICROSECONDS",
            DateTimeField::Millenium => "MILLENIUM",
            DateTimeField::Millennium => "MILLENNIUM",
            DateTimeField::Millisecond => "MILLISECOND",
            DateTimeField::Milliseconds => "MILLISECONDS",
            DateTimeField::Nanosecond => "NANOSECOND",
            DateTimeField::Nanoseconds => "NANOSECONDS",
            DateTimeField::Quarter => "QUARTER",
            DateTimeField::Timezone => "TIMEZONE",
            DateTimeField::TimezoneHour => "TIMEZONE_HOUR",
            DateTimeField::TimezoneMinute => "TIMEZONE_MINUTE",
            DateTimeField::NoDateTime => "NODATETIME",
        })
    }
}

pub struct EscapeQuotedString<'a> {
    string: &'a str,
    quote: char,
}

impl<'a> fmt::Display for EscapeQuotedString<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for c in self.string.chars() {
            if c == self.quote {
                write!(f, "{q}{q}", q = self.quote)?;
            } else {
                write!(f, "{}", c)?;
            }
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
                    write!(f, "{}", c)?;
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
#[cfg_attr(feature = "visitor", derive(Visit))]
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

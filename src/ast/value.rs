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
use alloc::boxed::Box;
#[cfg(not(feature = "std"))]
use alloc::string::String;
use core::fmt;

#[cfg(feature = "bigdecimal")]
use bigdecimal::BigDecimal;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

#[cfg(feature = "derive-visitor")]
use derive_visitor::{Drive, DriveMut};

/// Primitive SQL values such as number and string
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "derive-visitor", derive(Drive, DriveMut))]
pub enum Value {
    /// Numeric literal
    #[cfg(not(feature = "bigdecimal"))]
    Number(#[cfg_attr(feature = "derive-visitor", drive(skip))] String, #[cfg_attr(feature = "derive-visitor", drive(skip))] bool),
    #[cfg(feature = "bigdecimal")]
    Number(#[cfg_attr(feature = "derive-visitor", drive(skip))] BigDecimal, #[cfg_attr(feature = "derive-visitor", drive(skip))] bool),
    /// 'string value'
    SingleQuotedString(#[cfg_attr(feature = "derive-visitor", drive(skip))] String),
    /// e'string value' (postgres extension)
    /// <https://www.postgresql.org/docs/8.3/sql-syntax-lexical.html#SQL-SYNTAX-STRINGS
    EscapedStringLiteral(#[cfg_attr(feature = "derive-visitor", drive(skip))] String),
    /// N'string value'
    NationalStringLiteral(#[cfg_attr(feature = "derive-visitor", drive(skip))] String),
    /// X'hex value'
    HexStringLiteral(#[cfg_attr(feature = "derive-visitor", drive(skip))] String),

    DoubleQuotedString(#[cfg_attr(feature = "derive-visitor", drive(skip))] String),
    /// Boolean value true or false
    Boolean(#[cfg_attr(feature = "derive-visitor", drive(skip))] bool),
    /// `NULL` value
    Null,
    /// `?` or `$` Prepared statement arg placeholder
    Placeholder(#[cfg_attr(feature = "derive-visitor", drive(skip))] String),
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Value::Number(v, l) => write!(f, "{}{long}", v, long = if *l { "L" } else { "" }),
            Value::DoubleQuotedString(v) => write!(f, "\"{}\"", v),
            Value::SingleQuotedString(v) => write!(f, "'{}'", escape_single_quote_string(v)),
            Value::EscapedStringLiteral(v) => write!(f, "E'{}'", escape_escaped_string(v)),
            Value::NationalStringLiteral(v) => write!(f, "N'{}'", v),
            Value::HexStringLiteral(v) => write!(f, "X'{}'", v),
            Value::Boolean(v) => write!(f, "{}", v),
            Value::Null => write!(f, "NULL"),
            Value::Placeholder(v) => write!(f, "{}", v),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "derive-visitor", derive(Drive, DriveMut))]
pub enum DateTimeField {
    Year,
    Month,
    Week,
    Day,
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
    Microseconds,
    Millenium,
    Milliseconds,
    Quarter,
    Timezone,
    TimezoneHour,
    TimezoneMinute,
}

impl fmt::Display for DateTimeField {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(match self {
            DateTimeField::Year => "YEAR",
            DateTimeField::Month => "MONTH",
            DateTimeField::Week => "WEEK",
            DateTimeField::Day => "DAY",
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
            DateTimeField::Microseconds => "MICROSECONDS",
            DateTimeField::Millenium => "MILLENIUM",
            DateTimeField::Milliseconds => "MILLISECONDS",
            DateTimeField::Quarter => "QUARTER",
            DateTimeField::Timezone => "TIMEZONE",
            DateTimeField::TimezoneHour => "TIMEZONE_HOUR",
            DateTimeField::TimezoneMinute => "TIMEZONE_MINUTE",
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "derive-visitor", derive(Drive, DriveMut))]
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

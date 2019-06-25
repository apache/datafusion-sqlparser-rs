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

use ordered_float::OrderedFloat;

/// Primitive SQL values such as number and string
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Value {
    /// Unsigned integer value
    Long(u64),
    /// Unsigned floating point value
    Double(OrderedFloat<f64>),
    /// 'string value'
    SingleQuotedString(String),
    /// N'string value'
    NationalStringLiteral(String),
    /// X'hex value'
    HexStringLiteral(String),
    /// Boolean value true or false
    Boolean(bool),
    /// `DATE '...'` literals
    Date(String),
    /// `TIME '...'` literals
    Time(String),
    /// `TIMESTAMP '...'` literals
    Timestamp(String),
    /// INTERVAL literals, roughly in the following format:
    /// `INTERVAL '<value>' <leading_field> [ (<leading_precision>) ]
    /// [ TO <last_field> [ (<fractional_seconds_precision>) ] ]`,
    /// e.g. `INTERVAL '123:45.67' MINUTE(3) TO SECOND(2)`.
    ///
    /// The parser does not validate the `<value>`, nor does it ensure
    /// that the `<leading_field>` units >= the units in `<last_field>`,
    /// so the user will have to reject intervals like `HOUR TO YEAR`.
    Interval {
        value: String,
        leading_field: DateTimeField,
        leading_precision: Option<u64>,
        last_field: Option<DateTimeField>,
        /// The seconds precision can be specified in SQL source as
        /// `INTERVAL '__' SECOND(_, x)` (in which case the `leading_field`
        /// will be `Second` and the `last_field` will be `None`),
        /// or as `__ TO SECOND(x)`.
        fractional_seconds_precision: Option<u64>,
    },
    /// `NULL` value
    Null,
}

impl ToString for Value {
    fn to_string(&self) -> String {
        match self {
            Value::Long(v) => v.to_string(),
            Value::Double(v) => v.to_string(),
            Value::SingleQuotedString(v) => format!("'{}'", escape_single_quote_string(v)),
            Value::NationalStringLiteral(v) => format!("N'{}'", v),
            Value::HexStringLiteral(v) => format!("X'{}'", v),
            Value::Boolean(v) => v.to_string(),
            Value::Date(v) => format!("DATE '{}'", escape_single_quote_string(v)),
            Value::Time(v) => format!("TIME '{}'", escape_single_quote_string(v)),
            Value::Timestamp(v) => format!("TIMESTAMP '{}'", escape_single_quote_string(v)),
            Value::Interval {
                value,
                leading_field: DateTimeField::Second,
                leading_precision: Some(leading_precision),
                last_field,
                fractional_seconds_precision: Some(fractional_seconds_precision),
            } => {
                // When the leading field is SECOND, the parser guarantees that
                // the last field is None.
                assert!(last_field.is_none());
                format!(
                    "INTERVAL '{}' SECOND ({}, {})",
                    escape_single_quote_string(value),
                    leading_precision,
                    fractional_seconds_precision
                )
            }
            Value::Interval {
                value,
                leading_field,
                leading_precision,
                last_field,
                fractional_seconds_precision,
            } => {
                let mut s = format!(
                    "INTERVAL '{}' {}",
                    escape_single_quote_string(value),
                    leading_field.to_string()
                );
                if let Some(leading_precision) = leading_precision {
                    s += &format!(" ({})", leading_precision);
                }
                if let Some(last_field) = last_field {
                    s += &format!(" TO {}", last_field.to_string());
                }
                if let Some(fractional_seconds_precision) = fractional_seconds_precision {
                    s += &format!(" ({})", fractional_seconds_precision);
                }
                s
            }
            Value::Null => "NULL".to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DateTimeField {
    Year,
    Month,
    Day,
    Hour,
    Minute,
    Second,
}

impl ToString for DateTimeField {
    fn to_string(&self) -> String {
        match self {
            DateTimeField::Year => "YEAR".to_string(),
            DateTimeField::Month => "MONTH".to_string(),
            DateTimeField::Day => "DAY".to_string(),
            DateTimeField::Hour => "HOUR".to_string(),
            DateTimeField::Minute => "MINUTE".to_string(),
            DateTimeField::Second => "SECOND".to_string(),
        }
    }
}

fn escape_single_quote_string(s: &str) -> String {
    let mut escaped = String::new();
    for c in s.chars() {
        if c == '\'' {
            escaped.push_str("\'\'");
        } else {
            escaped.push(c);
        }
    }
    escaped
}

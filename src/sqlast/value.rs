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
#[derive(Debug, Clone, PartialEq, Hash)]
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
    /// Date literals
    Date(String),
    /// Time literals
    Time(String),
    /// Timestamp literals, which include both a date and time
    Timestamp(String),
    /// NULL value in insert statements,
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
            Value::Null => "NULL".to_string(),
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

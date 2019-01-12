use chrono::{offset::FixedOffset, DateTime, NaiveDate, NaiveDateTime, NaiveTime};

use uuid::Uuid;

/// SQL values such as int, double, string timestamp
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    /// Literal signed long
    Long(i64),
    /// Literal floating point value
    Double(f64),
    /// Unquoted string
    String(String),
    /// Uuid value
    Uuid(Uuid),
    /// 'string value'
    SingleQuotedString(String),
    /// "string value"
    DoubleQuotedString(String),
    /// Boolean value true or false,
    Boolean(bool),
    /// Date value
    Date(NaiveDate),
    // Time
    Time(NaiveTime),
    /// Date and time
    DateTime(NaiveDateTime),
    /// Timstamp with time zone
    Timestamp(DateTime<FixedOffset>),
    /// NULL value in insert statements,
    Null,
}

impl ToString for Value {
    fn to_string(&self) -> String {
        match self {
            Value::Long(v) => v.to_string(),
            Value::Double(v) => v.to_string(),
            Value::String(v) => v.to_string(),
            Value::Uuid(v) => v.to_string(),
            Value::SingleQuotedString(v) => format!("'{}'", v),
            Value::DoubleQuotedString(v) => format!("\"{}\"", v),
            Value::Boolean(v) => v.to_string(),
            Value::Date(v) => v.to_string(),
            Value::Time(v) => v.to_string(),
            Value::DateTime(v) => v.to_string(),
            Value::Timestamp(v) => format!("{}", v),
            Value::Null => "NULL".to_string(),
        }
    }
}

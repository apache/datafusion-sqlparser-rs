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
    /// INTERVAL literals, e.g. INTERVAL '12:34.56' MINUTE TO SECOND (2)
    Interval {
        value: String,
        leading_field: SQLDateTimeField,
        leading_precision: Option<u64>,
        last_field: Option<SQLDateTimeField>,
        fractional_seconds_precision: Option<u64>,
    },
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
            Value::Interval {
                value,
                leading_field: SQLDateTimeField::Second,
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

#[derive(Debug, Clone, PartialEq, Hash)]
pub enum SQLDateTimeField {
    Year,
    Month,
    Day,
    Hour,
    Minute,
    Second,
}

impl ToString for SQLDateTimeField {
    fn to_string(&self) -> String {
        match self {
            SQLDateTimeField::Year => "YEAR".to_string(),
            SQLDateTimeField::Month => "MONTH".to_string(),
            SQLDateTimeField::Day => "DAY".to_string(),
            SQLDateTimeField::Hour => "HOUR".to_string(),
            SQLDateTimeField::Minute => "MINUTE".to_string(),
            SQLDateTimeField::Second => "SECOND".to_string(),
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

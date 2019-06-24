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

use super::ObjectName;

/// SQL data types
#[derive(Debug, Clone, PartialEq, Hash)]
pub enum DataType {
    /// Fixed-length character type e.g. CHAR(10)
    Char(Option<u64>),
    /// Variable-length character type e.g. VARCHAR(10)
    Varchar(Option<u64>),
    /// Uuid type
    Uuid,
    /// Large character object e.g. CLOB(1000)
    Clob(u64),
    /// Fixed-length binary type e.g. BINARY(10)
    Binary(u64),
    /// Variable-length binary type e.g. VARBINARY(10)
    Varbinary(u64),
    /// Large binary object e.g. BLOB(1000)
    Blob(u64),
    /// Decimal type with optional precision and scale e.g. DECIMAL(10,2)
    Decimal(Option<u64>, Option<u64>),
    /// Floating point with optional precision e.g. FLOAT(8)
    Float(Option<u64>),
    /// Small integer
    SmallInt,
    /// Integer
    Int,
    /// Big integer
    BigInt,
    /// Floating point e.g. REAL
    Real,
    /// Double e.g. DOUBLE PRECISION
    Double,
    /// Boolean
    Boolean,
    /// Date
    Date,
    /// Time
    Time,
    /// Timestamp
    Timestamp,
    /// Interval
    Interval,
    /// Regclass used in postgresql serial
    Regclass,
    /// Text
    Text,
    /// Bytea
    Bytea,
    /// Custom type such as enums
    Custom(ObjectName),
    /// Arrays
    Array(Box<DataType>),
}

impl ToString for DataType {
    fn to_string(&self) -> String {
        match self {
            DataType::Char(size) => format_type_with_optional_length("char", size),
            DataType::Varchar(size) => format_type_with_optional_length("character varying", size),
            DataType::Uuid => "uuid".to_string(),
            DataType::Clob(size) => format!("clob({})", size),
            DataType::Binary(size) => format!("binary({})", size),
            DataType::Varbinary(size) => format!("varbinary({})", size),
            DataType::Blob(size) => format!("blob({})", size),
            DataType::Decimal(precision, scale) => {
                if let Some(scale) = scale {
                    format!("numeric({},{})", precision.unwrap(), scale)
                } else {
                    format_type_with_optional_length("numeric", precision)
                }
            }
            DataType::Float(size) => format_type_with_optional_length("float", size),
            DataType::SmallInt => "smallint".to_string(),
            DataType::Int => "int".to_string(),
            DataType::BigInt => "bigint".to_string(),
            DataType::Real => "real".to_string(),
            DataType::Double => "double".to_string(),
            DataType::Boolean => "boolean".to_string(),
            DataType::Date => "date".to_string(),
            DataType::Time => "time".to_string(),
            DataType::Timestamp => "timestamp".to_string(),
            DataType::Interval => "interval".to_string(),
            DataType::Regclass => "regclass".to_string(),
            DataType::Text => "text".to_string(),
            DataType::Bytea => "bytea".to_string(),
            DataType::Array(ty) => format!("{}[]", ty.to_string()),
            DataType::Custom(ty) => ty.to_string(),
        }
    }
}

fn format_type_with_optional_length(sql_type: &str, len: &Option<u64>) -> String {
    let mut s = sql_type.to_string();
    if let Some(len) = len {
        s += &format!("({})", len);
    }
    s
}

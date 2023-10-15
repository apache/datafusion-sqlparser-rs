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
use alloc::{boxed::Box, format, string::String, vec::Vec};
use core::fmt;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

#[cfg(feature = "visitor")]
use sqlparser_derive::{Visit, VisitMut};

use crate::ast::{display_comma_separated, ObjectName, StructField};

use super::value::escape_single_quote_string;

/// SQL data types
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum DataType {
    /// Fixed-length character type e.g. CHARACTER(10)
    Character(Option<CharacterLength>),
    /// Fixed-length char type e.g. CHAR(10)
    Char(Option<CharacterLength>),
    /// Character varying type e.g. CHARACTER VARYING(10)
    CharacterVarying(Option<CharacterLength>),
    /// Char varying type e.g. CHAR VARYING(10)
    CharVarying(Option<CharacterLength>),
    /// Variable-length character type e.g. VARCHAR(10)
    Varchar(Option<CharacterLength>),
    /// Variable-length character type e.g. NVARCHAR(10)
    Nvarchar(Option<u64>),
    /// Uuid type
    Uuid,
    /// Large character object with optional length e.g. CHARACTER LARGE OBJECT, CHARACTER LARGE OBJECT(1000), [standard]
    ///
    /// [standard]: https://jakewheat.github.io/sql-overview/sql-2016-foundation-grammar.html#character-large-object-type
    CharacterLargeObject(Option<u64>),
    /// Large character object with optional length e.g. CHAR LARGE OBJECT, CHAR LARGE OBJECT(1000), [standard]
    ///
    /// [standard]: https://jakewheat.github.io/sql-overview/sql-2016-foundation-grammar.html#character-large-object-type
    CharLargeObject(Option<u64>),
    /// Large character object with optional length e.g. CLOB, CLOB(1000), [standard]
    ///
    /// [standard]: https://jakewheat.github.io/sql-overview/sql-2016-foundation-grammar.html#character-large-object-type
    /// [Oracle]: https://docs.oracle.com/javadb/10.10.1.2/ref/rrefclob.html
    Clob(Option<u64>),
    /// Fixed-length binary type with optional length e.g.  [standard], [MS SQL Server]
    ///
    /// [standard]: https://jakewheat.github.io/sql-overview/sql-2016-foundation-grammar.html#binary-string-type
    /// [MS SQL Server]: https://learn.microsoft.com/pt-br/sql/t-sql/data-types/binary-and-varbinary-transact-sql?view=sql-server-ver16
    Binary(Option<u64>),
    /// Variable-length binary with optional length type e.g. [standard], [MS SQL Server]
    ///
    /// [standard]: https://jakewheat.github.io/sql-overview/sql-2016-foundation-grammar.html#binary-string-type
    /// [MS SQL Server]: https://learn.microsoft.com/pt-br/sql/t-sql/data-types/binary-and-varbinary-transact-sql?view=sql-server-ver16
    Varbinary(Option<u64>),
    /// Large binary object with optional length e.g. BLOB, BLOB(1000), [standard], [Oracle]
    ///
    /// [standard]: https://jakewheat.github.io/sql-overview/sql-2016-foundation-grammar.html#binary-large-object-string-type
    /// [Oracle]: https://docs.oracle.com/javadb/10.8.3.0/ref/rrefblob.html
    Blob(Option<u64>),
    /// Variable-length binary data with optional length.
    ///
    /// [bigquery]: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#bytes_type
    Bytes(Option<u64>),
    /// Numeric type with optional precision and scale e.g. NUMERIC(10,2), [standard][1]
    ///
    /// [1]: https://jakewheat.github.io/sql-overview/sql-2016-foundation-grammar.html#exact-numeric-type
    Numeric(ExactNumberInfo),
    /// Decimal type with optional precision and scale e.g. DECIMAL(10,2), [standard][1]
    ///
    /// [1]: https://jakewheat.github.io/sql-overview/sql-2016-foundation-grammar.html#exact-numeric-type
    Decimal(ExactNumberInfo),
    /// [BigNumeric] type used in BigQuery
    ///
    /// [BigNumeric]: https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#bignumeric_literals
    BigNumeric(ExactNumberInfo),
    /// This is alias for `BigNumeric` type used in BigQuery
    ///
    /// [BigDecimal]: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#decimal_types
    BigDecimal(ExactNumberInfo),
    /// Dec type with optional precision and scale e.g. DEC(10,2), [standard][1]
    ///
    /// [1]: https://jakewheat.github.io/sql-overview/sql-2016-foundation-grammar.html#exact-numeric-type
    Dec(ExactNumberInfo),
    /// Floating point with optional precision e.g. FLOAT(8)
    Float(Option<u64>),
    /// Tiny integer with optional display width e.g. TINYINT or TINYINT(3)
    TinyInt(Option<u64>),
    /// Unsigned tiny integer with optional display width e.g. TINYINT UNSIGNED or TINYINT(3) UNSIGNED
    UnsignedTinyInt(Option<u64>),
    /// Int2 as alias for SmallInt in [postgresql]
    /// Note: Int2 mean 2 bytes in postgres (not 2 bits)
    /// Int2 with optional display width e.g. INT2 or INT2(5)
    ///
    /// [postgresql]: https://www.postgresql.org/docs/15/datatype.html
    Int2(Option<u64>),
    /// Unsigned Int2 with optional display width e.g. INT2 Unsigned or INT2(5) Unsigned
    UnsignedInt2(Option<u64>),
    /// Small integer with optional display width e.g. SMALLINT or SMALLINT(5)
    SmallInt(Option<u64>),
    /// Unsigned small integer with optional display width e.g. SMALLINT UNSIGNED or SMALLINT(5) UNSIGNED
    UnsignedSmallInt(Option<u64>),
    /// MySQL medium integer ([1]) with optional display width e.g. MEDIUMINT or MEDIUMINT(5)
    ///
    /// [1]: https://dev.mysql.com/doc/refman/8.0/en/integer-types.html
    MediumInt(Option<u64>),
    /// Unsigned medium integer ([1]) with optional display width e.g. MEDIUMINT UNSIGNED or MEDIUMINT(5) UNSIGNED
    ///
    /// [1]: https://dev.mysql.com/doc/refman/8.0/en/integer-types.html
    UnsignedMediumInt(Option<u64>),
    /// Int with optional display width e.g. INT or INT(11)
    Int(Option<u64>),
    /// Int4 as alias for Integer in [postgresql]
    /// Note: Int4 mean 4 bytes in postgres (not 4 bits)
    /// Int4 with optional display width e.g. Int4 or Int4(11)
    ///
    /// [postgresql]: https://www.postgresql.org/docs/15/datatype.html
    Int4(Option<u64>),
    /// Integer type in [bigquery]
    ///
    /// [bigquery]: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#integer_types
    Int64,
    /// Integer with optional display width e.g. INTEGER or INTEGER(11)
    Integer(Option<u64>),
    /// Unsigned int with optional display width e.g. INT UNSIGNED or INT(11) UNSIGNED
    UnsignedInt(Option<u64>),
    /// Unsigned int4 with optional display width e.g. INT4 UNSIGNED or INT4(11) UNSIGNED
    UnsignedInt4(Option<u64>),
    /// Unsigned integer with optional display width e.g. INTGER UNSIGNED or INTEGER(11) UNSIGNED
    UnsignedInteger(Option<u64>),
    /// Big integer with optional display width e.g. BIGINT or BIGINT(20)
    BigInt(Option<u64>),
    /// Unsigned big integer with optional display width e.g. BIGINT UNSIGNED or BIGINT(20) UNSIGNED
    UnsignedBigInt(Option<u64>),
    /// Int8 as alias for Bigint in [postgresql]
    /// Note: Int8 mean 8 bytes in postgres (not 8 bits)
    /// Int8 with optional display width e.g. INT8 or INT8(11)
    ///
    /// [postgresql]: https://www.postgresql.org/docs/15/datatype.html
    Int8(Option<u64>),
    /// Unsigned Int8 with optional display width e.g. INT8 UNSIGNED or INT8(11) UNSIGNED
    UnsignedInt8(Option<u64>),
    /// Float4 as alias for Real in [postgresql]
    ///
    /// [postgresql]: https://www.postgresql.org/docs/15/datatype.html
    Float4,
    /// Floating point in [bigquery]
    ///
    /// [bigquery]: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#floating_point_types
    Float64,
    /// Floating point e.g. REAL
    Real,
    /// Float8 as alias for Double in [postgresql]
    ///
    /// [postgresql]: https://www.postgresql.org/docs/15/datatype.html
    Float8,
    /// Double
    Double,
    /// Double PRECISION e.g. [standard], [postgresql]
    ///
    /// [standard]: https://jakewheat.github.io/sql-overview/sql-2016-foundation-grammar.html#approximate-numeric-type
    /// [postgresql]: https://www.postgresql.org/docs/current/datatype-numeric.html
    DoublePrecision,
    /// Bool as alias for Boolean in [postgresql]
    ///
    /// [postgresql]: https://www.postgresql.org/docs/15/datatype.html
    Bool,
    /// Boolean
    Boolean,
    /// Date
    Date,
    /// Time with optional time precision and time zone information e.g. [standard][1].
    ///
    /// [1]: https://jakewheat.github.io/sql-overview/sql-2016-foundation-grammar.html#datetime-type
    Time(Option<u64>, TimezoneInfo),
    /// Datetime with optional time precision e.g. [MySQL][1].
    ///
    /// [1]: https://dev.mysql.com/doc/refman/8.0/en/datetime.html
    Datetime(Option<u64>),
    /// Timestamp with optional time precision and time zone information e.g. [standard][1].
    ///
    /// [1]: https://jakewheat.github.io/sql-overview/sql-2016-foundation-grammar.html#datetime-type
    Timestamp(Option<u64>, TimezoneInfo),
    /// Interval
    Interval,
    /// JSON type used in BigQuery
    JSON,
    /// Regclass used in postgresql serial
    Regclass,
    /// Text
    Text,
    /// String with optional length.
    String(Option<u64>),
    /// Bytea
    Bytea,
    /// Custom type such as enums
    Custom(ObjectName, Vec<String>),
    /// Arrays
    Array(ArrayElemTypeDef),
    /// Enums
    Enum(Vec<String>),
    /// Set
    Set(Vec<String>),
    /// Struct
    ///
    /// [hive]: https://docs.cloudera.com/cdw-runtime/cloud/impala-sql-reference/topics/impala-struct.html
    /// [bigquery]: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#struct_type
    Struct(Vec<StructField>),
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            DataType::Character(size) => format_character_string_type(f, "CHARACTER", size),
            DataType::Char(size) => format_character_string_type(f, "CHAR", size),
            DataType::CharacterVarying(size) => {
                format_character_string_type(f, "CHARACTER VARYING", size)
            }

            DataType::CharVarying(size) => format_character_string_type(f, "CHAR VARYING", size),
            DataType::Varchar(size) => format_character_string_type(f, "VARCHAR", size),
            DataType::Nvarchar(size) => {
                format_type_with_optional_length(f, "NVARCHAR", size, false)
            }
            DataType::Uuid => write!(f, "UUID"),
            DataType::CharacterLargeObject(size) => {
                format_type_with_optional_length(f, "CHARACTER LARGE OBJECT", size, false)
            }
            DataType::CharLargeObject(size) => {
                format_type_with_optional_length(f, "CHAR LARGE OBJECT", size, false)
            }
            DataType::Clob(size) => format_type_with_optional_length(f, "CLOB", size, false),
            DataType::Binary(size) => format_type_with_optional_length(f, "BINARY", size, false),
            DataType::Varbinary(size) => {
                format_type_with_optional_length(f, "VARBINARY", size, false)
            }
            DataType::Blob(size) => format_type_with_optional_length(f, "BLOB", size, false),
            DataType::Bytes(size) => format_type_with_optional_length(f, "BYTES", size, false),
            DataType::Numeric(info) => {
                write!(f, "NUMERIC{info}")
            }
            DataType::Decimal(info) => {
                write!(f, "DECIMAL{info}")
            }
            DataType::Dec(info) => {
                write!(f, "DEC{info}")
            }
            DataType::BigNumeric(info) => write!(f, "BIGNUMERIC{info}"),
            DataType::BigDecimal(info) => write!(f, "BIGDECIMAL{info}"),
            DataType::Float(size) => format_type_with_optional_length(f, "FLOAT", size, false),
            DataType::TinyInt(zerofill) => {
                format_type_with_optional_length(f, "TINYINT", zerofill, false)
            }
            DataType::UnsignedTinyInt(zerofill) => {
                format_type_with_optional_length(f, "TINYINT", zerofill, true)
            }
            DataType::Int2(zerofill) => {
                format_type_with_optional_length(f, "INT2", zerofill, false)
            }
            DataType::UnsignedInt2(zerofill) => {
                format_type_with_optional_length(f, "INT2", zerofill, true)
            }
            DataType::SmallInt(zerofill) => {
                format_type_with_optional_length(f, "SMALLINT", zerofill, false)
            }
            DataType::UnsignedSmallInt(zerofill) => {
                format_type_with_optional_length(f, "SMALLINT", zerofill, true)
            }
            DataType::MediumInt(zerofill) => {
                format_type_with_optional_length(f, "MEDIUMINT", zerofill, false)
            }
            DataType::UnsignedMediumInt(zerofill) => {
                format_type_with_optional_length(f, "MEDIUMINT", zerofill, true)
            }
            DataType::Int(zerofill) => format_type_with_optional_length(f, "INT", zerofill, false),
            DataType::UnsignedInt(zerofill) => {
                format_type_with_optional_length(f, "INT", zerofill, true)
            }
            DataType::Int4(zerofill) => {
                format_type_with_optional_length(f, "INT4", zerofill, false)
            }
            DataType::Int64 => {
                write!(f, "INT64")
            }
            DataType::UnsignedInt4(zerofill) => {
                format_type_with_optional_length(f, "INT4", zerofill, true)
            }
            DataType::Integer(zerofill) => {
                format_type_with_optional_length(f, "INTEGER", zerofill, false)
            }
            DataType::UnsignedInteger(zerofill) => {
                format_type_with_optional_length(f, "INTEGER", zerofill, true)
            }
            DataType::BigInt(zerofill) => {
                format_type_with_optional_length(f, "BIGINT", zerofill, false)
            }
            DataType::UnsignedBigInt(zerofill) => {
                format_type_with_optional_length(f, "BIGINT", zerofill, true)
            }
            DataType::Int8(zerofill) => {
                format_type_with_optional_length(f, "INT8", zerofill, false)
            }
            DataType::UnsignedInt8(zerofill) => {
                format_type_with_optional_length(f, "INT8", zerofill, true)
            }
            DataType::Real => write!(f, "REAL"),
            DataType::Float4 => write!(f, "FLOAT4"),
            DataType::Float64 => write!(f, "FLOAT64"),
            DataType::Double => write!(f, "DOUBLE"),
            DataType::Float8 => write!(f, "FLOAT8"),
            DataType::DoublePrecision => write!(f, "DOUBLE PRECISION"),
            DataType::Bool => write!(f, "BOOL"),
            DataType::Boolean => write!(f, "BOOLEAN"),
            DataType::Date => write!(f, "DATE"),
            DataType::Time(precision, timezone_info) => {
                format_datetime_precision_and_tz(f, "TIME", precision, timezone_info)
            }
            DataType::Datetime(precision) => {
                format_type_with_optional_length(f, "DATETIME", precision, false)
            }
            DataType::Timestamp(precision, timezone_info) => {
                format_datetime_precision_and_tz(f, "TIMESTAMP", precision, timezone_info)
            }
            DataType::Interval => write!(f, "INTERVAL"),
            DataType::JSON => write!(f, "JSON"),
            DataType::Regclass => write!(f, "REGCLASS"),
            DataType::Text => write!(f, "TEXT"),
            DataType::String(size) => format_type_with_optional_length(f, "STRING", size, false),
            DataType::Bytea => write!(f, "BYTEA"),
            DataType::Array(ty) => match ty {
                ArrayElemTypeDef::None => write!(f, "ARRAY"),
                ArrayElemTypeDef::SquareBracket(t) => write!(f, "{t}[]"),
                ArrayElemTypeDef::AngleBracket(t) => write!(f, "ARRAY<{t}>"),
            },
            DataType::Custom(ty, modifiers) => {
                if modifiers.is_empty() {
                    write!(f, "{ty}")
                } else {
                    write!(f, "{}({})", ty, modifiers.join(", "))
                }
            }
            DataType::Enum(vals) => {
                write!(f, "ENUM(")?;
                for (i, v) in vals.iter().enumerate() {
                    if i != 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "'{}'", escape_single_quote_string(v))?;
                }
                write!(f, ")")
            }
            DataType::Set(vals) => {
                write!(f, "SET(")?;
                for (i, v) in vals.iter().enumerate() {
                    if i != 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "'{}'", escape_single_quote_string(v))?;
                }
                write!(f, ")")
            }
            DataType::Struct(fields) => {
                if !fields.is_empty() {
                    write!(f, "STRUCT<{}>", display_comma_separated(fields))
                } else {
                    write!(f, "STRUCT")
                }
            }
        }
    }
}

fn format_type_with_optional_length(
    f: &mut fmt::Formatter,
    sql_type: &'static str,
    len: &Option<u64>,
    unsigned: bool,
) -> fmt::Result {
    write!(f, "{sql_type}")?;
    if let Some(len) = len {
        write!(f, "({len})")?;
    }
    if unsigned {
        write!(f, " UNSIGNED")?;
    }
    Ok(())
}

fn format_character_string_type(
    f: &mut fmt::Formatter,
    sql_type: &str,
    size: &Option<CharacterLength>,
) -> fmt::Result {
    write!(f, "{sql_type}")?;
    if let Some(size) = size {
        write!(f, "({size})")?;
    }
    Ok(())
}

fn format_datetime_precision_and_tz(
    f: &mut fmt::Formatter,
    sql_type: &'static str,
    len: &Option<u64>,
    time_zone: &TimezoneInfo,
) -> fmt::Result {
    write!(f, "{sql_type}")?;
    let len_fmt = len.as_ref().map(|l| format!("({l})")).unwrap_or_default();

    match time_zone {
        TimezoneInfo::Tz => {
            write!(f, "{time_zone}{len_fmt}")?;
        }
        _ => {
            write!(f, "{len_fmt}{time_zone}")?;
        }
    }

    Ok(())
}

/// Timestamp and Time data types information about TimeZone formatting.
///
/// This is more related to a display information than real differences between each variant. To
/// guarantee compatibility with the input query we must maintain its exact information.
#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum TimezoneInfo {
    /// No information about time zone. E.g., TIMESTAMP
    None,
    /// Temporal type 'WITH TIME ZONE'. E.g., TIMESTAMP WITH TIME ZONE, [standard], [Oracle]
    ///
    /// [standard]: https://jakewheat.github.io/sql-overview/sql-2016-foundation-grammar.html#datetime-type
    /// [Oracle]: https://docs.oracle.com/en/database/oracle/oracle-database/12.2/nlspg/datetime-data-types-and-time-zone-support.html#GUID-3F1C388E-C651-43D5-ADBC-1A49E5C2CA05
    WithTimeZone,
    /// Temporal type 'WITHOUT TIME ZONE'. E.g., TIME WITHOUT TIME ZONE, [standard], [Postgresql]
    ///
    /// [standard]: https://jakewheat.github.io/sql-overview/sql-2016-foundation-grammar.html#datetime-type
    /// [Postgresql]: https://www.postgresql.org/docs/current/datatype-datetime.html
    WithoutTimeZone,
    /// Postgresql specific `WITH TIME ZONE` formatting, for both TIME and TIMESTAMP. E.g., TIMETZ, [Postgresql]
    ///
    /// [Postgresql]: https://www.postgresql.org/docs/current/datatype-datetime.html
    Tz,
}

impl fmt::Display for TimezoneInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TimezoneInfo::None => {
                write!(f, "")
            }
            TimezoneInfo::WithTimeZone => {
                write!(f, " WITH TIME ZONE")
            }
            TimezoneInfo::WithoutTimeZone => {
                write!(f, " WITHOUT TIME ZONE")
            }
            TimezoneInfo::Tz => {
                // TZ is the only one that is displayed BEFORE the precision, so the datatype display
                // must be aware of that. Check <https://www.postgresql.org/docs/14/datatype-datetime.html>
                // for more information
                write!(f, "TZ")
            }
        }
    }
}

/// Additional information for `NUMERIC`, `DECIMAL`, and `DEC` data types
/// following the 2016 [standard].
///
/// [standard]: https://jakewheat.github.io/sql-overview/sql-2016-foundation-grammar.html#exact-numeric-type
#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum ExactNumberInfo {
    /// No additional information e.g. `DECIMAL`
    None,
    /// Only precision information e.g. `DECIMAL(10)`
    Precision(u64),
    /// Precision and scale information e.g. `DECIMAL(10,2)`
    PrecisionAndScale(u64, u64),
}

impl fmt::Display for ExactNumberInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExactNumberInfo::None => {
                write!(f, "")
            }
            ExactNumberInfo::Precision(p) => {
                write!(f, "({p})")
            }
            ExactNumberInfo::PrecisionAndScale(p, s) => {
                write!(f, "({p},{s})")
            }
        }
    }
}

/// Information about [character length][1], including length and possibly unit.
///
/// [1]: https://jakewheat.github.io/sql-overview/sql-2016-foundation-grammar.html#character-length
#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct CharacterLength {
    /// Default (if VARYING) or maximum (if not VARYING) length
    pub length: u64,
    /// Optional unit. If not informed, the ANSI handles it as CHARACTERS implicitly
    pub unit: Option<CharLengthUnits>,
}

impl fmt::Display for CharacterLength {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.length)?;
        if let Some(unit) = &self.unit {
            write!(f, " {unit}")?;
        }
        Ok(())
    }
}

/// Possible units for characters, initially based on 2016 ANSI [standard][1].
///
/// [1]: https://jakewheat.github.io/sql-overview/sql-2016-foundation-grammar.html#char-length-units
#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum CharLengthUnits {
    /// CHARACTERS unit
    Characters,
    /// OCTETS unit
    Octets,
}

impl fmt::Display for CharLengthUnits {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Characters => {
                write!(f, "CHARACTERS")
            }
            Self::Octets => {
                write!(f, "OCTETS")
            }
        }
    }
}

/// Represents the data type of the elements in an array (if any) as well as
/// the syntax used to declare the array.
///
/// For example: Bigquery/Hive use `ARRAY<INT>` whereas snowflake uses ARRAY.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum ArrayElemTypeDef {
    /// `ARRAY`
    None,
    /// `ARRAY<INT>`
    AngleBracket(Box<DataType>),
    /// `[]INT`
    SquareBracket(Box<DataType>),
}

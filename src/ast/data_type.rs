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
use alloc::{boxed::Box, format, string::String, vec::Vec};
use core::fmt;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

#[cfg(feature = "visitor")]
use sqlparser_derive::{Visit, VisitMut};

use crate::ast::{display_comma_separated, Expr, ObjectName, StructField, UnionField};

use super::{value::escape_single_quote_string, ColumnDef};

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum EnumMember {
    Name(String),
    /// ClickHouse allows to specify an integer value for each enum value.
    ///
    /// [ClickHouse](https://clickhouse.com/docs/en/sql-reference/data-types/enum)
    NamedValue(String, Expr),
}

/// SQL data types
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum DataType {
    /// Table type in [PostgreSQL], e.g. CREATE FUNCTION RETURNS TABLE(...).
    ///
    /// [PostgreSQL]: https://www.postgresql.org/docs/15/sql-createfunction.html
    /// [MsSQL]: https://learn.microsoft.com/en-us/sql/t-sql/statements/create-function-transact-sql?view=sql-server-ver16#c-create-a-multi-statement-table-valued-function
    Table(Option<Vec<ColumnDef>>),
    /// Table type with a name, e.g. CREATE FUNCTION RETURNS @result TABLE(...).
    ///
    /// [MsSQl]: https://learn.microsoft.com/en-us/sql/t-sql/statements/create-function-transact-sql?view=sql-server-ver16#table
    NamedTable {
        /// Table name.
        name: ObjectName,
        /// Table columns.
        columns: Vec<ColumnDef>,
    },
    /// Fixed-length character type, e.g. CHARACTER(10).
    Character(Option<CharacterLength>),
    /// Fixed-length char type, e.g. CHAR(10).
    Char(Option<CharacterLength>),
    /// Character varying type, e.g. CHARACTER VARYING(10).
    CharacterVarying(Option<CharacterLength>),
    /// Char varying type, e.g. CHAR VARYING(10).
    CharVarying(Option<CharacterLength>),
    /// Variable-length character type, e.g. VARCHAR(10).
    Varchar(Option<CharacterLength>),
    /// Variable-length character type, e.g. NVARCHAR(10).
    Nvarchar(Option<CharacterLength>),
    /// Uuid type.
    Uuid,
    /// Large character object with optional length,
    /// e.g. CHARACTER LARGE OBJECT, CHARACTER LARGE OBJECT(1000), [SQL Standard].
    ///
    /// [SQL Standard]: https://jakewheat.github.io/sql-overview/sql-2016-foundation-grammar.html#character-large-object-type
    CharacterLargeObject(Option<u64>),
    /// Large character object with optional length,
    /// e.g. CHAR LARGE OBJECT, CHAR LARGE OBJECT(1000), [SQL Standard].
    ///
    /// [SQL Standard]: https://jakewheat.github.io/sql-overview/sql-2016-foundation-grammar.html#character-large-object-type
    CharLargeObject(Option<u64>),
    /// Large character object with optional length,
    /// e.g. CLOB, CLOB(1000), [SQL Standard].
    ///
    /// [SQL Standard]: https://jakewheat.github.io/sql-overview/sql-2016-foundation-grammar.html#character-large-object-type
    /// [Oracle]: https://docs.oracle.com/javadb/10.10.1.2/ref/rrefclob.html
    Clob(Option<u64>),
    /// Fixed-length binary type with optional length,
    /// see [SQL Standard], [MS SQL Server].
    ///
    /// [SQL Standard]: https://jakewheat.github.io/sql-overview/sql-2016-foundation-grammar.html#binary-string-type
    /// [MS SQL Server]: https://learn.microsoft.com/pt-br/sql/t-sql/data-types/binary-and-varbinary-transact-sql?view=sql-server-ver16
    Binary(Option<u64>),
    /// Variable-length binary with optional length type,
    /// see [SQL Standard], [MS SQL Server].
    ///
    /// [SQL Standard]: https://jakewheat.github.io/sql-overview/sql-2016-foundation-grammar.html#binary-string-type
    /// [MS SQL Server]: https://learn.microsoft.com/pt-br/sql/t-sql/data-types/binary-and-varbinary-transact-sql?view=sql-server-ver16
    Varbinary(Option<BinaryLength>),
    /// Large binary object with optional length,
    /// see [SQL Standard], [Oracle].
    ///
    /// [SQL Standard]: https://jakewheat.github.io/sql-overview/sql-2016-foundation-grammar.html#binary-large-object-string-type
    /// [Oracle]: https://docs.oracle.com/javadb/10.8.3.0/ref/rrefblob.html
    Blob(Option<u64>),
    /// [MySQL] blob with up to 2**8 bytes.
    ///
    /// [MySQL]: https://dev.mysql.com/doc/refman/9.1/en/blob.html
    TinyBlob,
    /// [MySQL] blob with up to 2**24 bytes.
    ///
    /// [MySQL]: https://dev.mysql.com/doc/refman/9.1/en/blob.html
    MediumBlob,
    /// [MySQL] blob with up to 2**32 bytes.
    ///
    /// [MySQL]: https://dev.mysql.com/doc/refman/9.1/en/blob.html
    LongBlob,
    /// Variable-length binary data with optional length.
    ///
    /// [BigQuery]: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#bytes_type
    Bytes(Option<u64>),
    /// Numeric type with optional precision and scale, e.g. NUMERIC(10,2), [SQL Standard][1].
    ///
    /// [1]: https://jakewheat.github.io/sql-overview/sql-2016-foundation-grammar.html#exact-numeric-type
    Numeric(ExactNumberInfo),
    /// Decimal type with optional precision and scale, e.g. DECIMAL(10,2), [SQL Standard][1].
    ///
    /// [1]: https://jakewheat.github.io/sql-overview/sql-2016-foundation-grammar.html#exact-numeric-type
    Decimal(ExactNumberInfo),
    /// [BigNumeric] type used in BigQuery.
    ///
    /// [BigNumeric]: https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#bignumeric_literals
    BigNumeric(ExactNumberInfo),
    /// This is alias for `BigNumeric` type used in BigQuery.
    ///
    /// [BigDecimal]: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#decimal_types
    BigDecimal(ExactNumberInfo),
    /// Dec type with optional precision and scale, e.g. DEC(10,2), [SQL Standard][1].
    ///
    /// [1]: https://jakewheat.github.io/sql-overview/sql-2016-foundation-grammar.html#exact-numeric-type
    Dec(ExactNumberInfo),
    /// Floating point with optional precision, e.g. FLOAT(8).
    Float(Option<u64>),
    /// Tiny integer with optional display width, e.g. TINYINT or TINYINT(3).
    TinyInt(Option<u64>),
    /// Unsigned tiny integer with optional display width,
    /// e.g. TINYINT UNSIGNED or TINYINT(3) UNSIGNED.
    TinyIntUnsigned(Option<u64>),
    /// Unsigned tiny integer, e.g. UTINYINT
    UTinyInt,
    /// Int2 is an alias for SmallInt in [PostgreSQL].
    /// Note: Int2 means 2 bytes in PostgreSQL (not 2 bits).
    /// Int2 with optional display width, e.g. INT2 or INT2(5).
    ///
    /// [PostgreSQL]: https://www.postgresql.org/docs/current/datatype.html
    Int2(Option<u64>),
    /// Unsigned Int2 with optional display width, e.g. INT2 UNSIGNED or INT2(5) UNSIGNED.
    Int2Unsigned(Option<u64>),
    /// Small integer with optional display width, e.g. SMALLINT or SMALLINT(5).
    SmallInt(Option<u64>),
    /// Unsigned small integer with optional display width,
    /// e.g. SMALLINT UNSIGNED or SMALLINT(5) UNSIGNED.
    SmallIntUnsigned(Option<u64>),
    /// Unsigned small integer, e.g. USMALLINT.
    USmallInt,
    /// MySQL medium integer ([1]) with optional display width,
    /// e.g. MEDIUMINT or MEDIUMINT(5).
    ///
    /// [1]: https://dev.mysql.com/doc/refman/8.0/en/integer-types.html
    MediumInt(Option<u64>),
    /// Unsigned medium integer ([1]) with optional display width,
    /// e.g. MEDIUMINT UNSIGNED or MEDIUMINT(5) UNSIGNED.
    ///
    /// [1]: https://dev.mysql.com/doc/refman/8.0/en/integer-types.html
    MediumIntUnsigned(Option<u64>),
    /// Int with optional display width, e.g. INT or INT(11).
    Int(Option<u64>),
    /// Int4 is an alias for Integer in [PostgreSQL].
    /// Note: Int4 means 4 bytes in PostgreSQL (not 4 bits).
    /// Int4 with optional display width, e.g. Int4 or Int4(11).
    ///
    /// [PostgreSQL]: https://www.postgresql.org/docs/current/datatype.html
    Int4(Option<u64>),
    /// Int8 is an alias for BigInt in [PostgreSQL] and Integer type in [ClickHouse].
    /// Int8 with optional display width, e.g. INT8 or INT8(11).
    /// Note: Int8 means 8 bytes in [PostgreSQL], but 8 bits in [ClickHouse].
    ///
    /// [PostgreSQL]: https://www.postgresql.org/docs/current/datatype.html
    /// [ClickHouse]: https://clickhouse.com/docs/en/sql-reference/data-types/int-uint
    Int8(Option<u64>),
    /// Integer type in [ClickHouse].
    /// Note: Int16 means 16 bits in [ClickHouse].
    ///
    /// [ClickHouse]: https://clickhouse.com/docs/en/sql-reference/data-types/int-uint
    Int16,
    /// Integer type in [ClickHouse].
    /// Note: Int32 means 32 bits in [ClickHouse].
    ///
    /// [ClickHouse]: https://clickhouse.com/docs/en/sql-reference/data-types/int-uint
    Int32,
    /// Integer type in [BigQuery], [ClickHouse].
    ///
    /// [BigQuery]: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#integer_types
    /// [ClickHouse]: https://clickhouse.com/docs/en/sql-reference/data-types/int-uint
    Int64,
    /// Integer type in [ClickHouse].
    /// Note: Int128 means 128 bits in [ClickHouse].
    ///
    /// [ClickHouse]: https://clickhouse.com/docs/en/sql-reference/data-types/int-uint
    Int128,
    /// Integer type in [ClickHouse].
    /// Note: Int256 means 256 bits in [ClickHouse].
    ///
    /// [ClickHouse]: https://clickhouse.com/docs/en/sql-reference/data-types/int-uint
    Int256,
    /// Integer with optional display width, e.g. INTEGER or INTEGER(11).
    Integer(Option<u64>),
    /// Unsigned int with optional display width, e.g. INT UNSIGNED or INT(11) UNSIGNED.
    IntUnsigned(Option<u64>),
    /// Unsigned int4 with optional display width, e.g. INT4 UNSIGNED or INT4(11) UNSIGNED.
    Int4Unsigned(Option<u64>),
    /// Unsigned integer with optional display width, e.g. INTEGER UNSIGNED or INTEGER(11) UNSIGNED.
    IntegerUnsigned(Option<u64>),
    /// 128-bit integer type, e.g. HUGEINT.
    HugeInt,
    /// Unsigned 128-bit integer type, e.g. UHUGEINT.
    UHugeInt,
    /// Unsigned integer type in [ClickHouse].
    /// Note: UInt8 means 8 bits in [ClickHouse].
    ///
    /// [ClickHouse]: https://clickhouse.com/docs/en/sql-reference/data-types/int-uint
    UInt8,
    /// Unsigned integer type in [ClickHouse].
    /// Note: UInt16 means 16 bits in [ClickHouse].
    ///
    /// [ClickHouse]: https://clickhouse.com/docs/en/sql-reference/data-types/int-uint
    UInt16,
    /// Unsigned integer type in [ClickHouse].
    /// Note: UInt32 means 32 bits in [ClickHouse].
    ///
    /// [ClickHouse]: https://clickhouse.com/docs/en/sql-reference/data-types/int-uint
    UInt32,
    /// Unsigned integer type in [ClickHouse].
    /// Note: UInt64 means 64 bits in [ClickHouse].
    ///
    /// [ClickHouse]: https://clickhouse.com/docs/en/sql-reference/data-types/int-uint
    UInt64,
    /// Unsigned integer type in [ClickHouse].
    /// Note: UInt128 means 128 bits in [ClickHouse].
    ///
    /// [ClickHouse]: https://clickhouse.com/docs/en/sql-reference/data-types/int-uint
    UInt128,
    /// Unsigned integer type in [ClickHouse].
    /// Note: UInt256 means 256 bits in [ClickHouse].
    ///
    /// [ClickHouse]: https://clickhouse.com/docs/en/sql-reference/data-types/int-uint
    UInt256,
    /// Big integer with optional display width, e.g. BIGINT or BIGINT(20).
    BigInt(Option<u64>),
    /// Unsigned big integer with optional display width, e.g. BIGINT UNSIGNED or BIGINT(20) UNSIGNED.
    BigIntUnsigned(Option<u64>),
    /// Unsigned big integer, e.g. UBIGINT.
    UBigInt,
    /// Unsigned Int8 with optional display width, e.g. INT8 UNSIGNED or INT8(11) UNSIGNED.
    Int8Unsigned(Option<u64>),
    /// Signed integer as used in [MySQL CAST] target types, without optional `INTEGER` suffix,
    /// e.g. `SIGNED`
    ///
    /// [MySQL CAST]: https://dev.mysql.com/doc/refman/8.4/en/cast-functions.html
    Signed,
    /// Signed integer as used in [MySQL CAST] target types, with optional `INTEGER` suffix,
    /// e.g. `SIGNED INTEGER`
    ///
    /// [MySQL CAST]: https://dev.mysql.com/doc/refman/8.4/en/cast-functions.html
    SignedInteger,
    /// Signed integer as used in [MySQL CAST] target types, without optional `INTEGER` suffix,
    /// e.g. `SIGNED`
    ///
    /// [MySQL CAST]: https://dev.mysql.com/doc/refman/8.4/en/cast-functions.html
    Unsigned,
    /// Unsigned integer as used in [MySQL CAST] target types, with optional `INTEGER` suffix,
    /// e.g. `UNSIGNED INTEGER`.
    ///
    /// [MySQL CAST]: https://dev.mysql.com/doc/refman/8.4/en/cast-functions.html
    UnsignedInteger,
    /// Float4 is an alias for Real in [PostgreSQL].
    ///
    /// [PostgreSQL]: https://www.postgresql.org/docs/current/datatype.html
    Float4,
    /// Floating point in [ClickHouse].
    ///
    /// [ClickHouse]: https://clickhouse.com/docs/en/sql-reference/data-types/float
    Float32,
    /// Floating point in [BigQuery].
    ///
    /// [BigQuery]: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#floating_point_types
    /// [ClickHouse]: https://clickhouse.com/docs/en/sql-reference/data-types/float
    Float64,
    /// Floating point, e.g. REAL.
    Real,
    /// Float8 is an alias for Double in [PostgreSQL].
    ///
    /// [PostgreSQL]: https://www.postgresql.org/docs/current/datatype.html
    Float8,
    /// Double
    Double(ExactNumberInfo),
    /// Double Precision, see [SQL Standard], [PostgreSQL].
    ///
    /// [SQL Standard]: https://jakewheat.github.io/sql-overview/sql-2016-foundation-grammar.html#approximate-numeric-type
    /// [PostgreSQL]: https://www.postgresql.org/docs/current/datatype-numeric.html
    DoublePrecision,
    /// Bool is an alias for Boolean, see [PostgreSQL].
    ///
    /// [PostgreSQL]: https://www.postgresql.org/docs/current/datatype.html
    Bool,
    /// Boolean type.
    Boolean,
    /// Date type.
    Date,
    /// Date32 with the same range as Datetime64.
    ///
    /// [1]: https://clickhouse.com/docs/en/sql-reference/data-types/date32
    Date32,
    /// Time with optional time precision and time zone information, see [SQL Standard][1].
    ///
    /// [1]: https://jakewheat.github.io/sql-overview/sql-2016-foundation-grammar.html#datetime-type
    Time(Option<u64>, TimezoneInfo),
    /// Datetime with optional time precision, see [MySQL][1].
    ///
    /// [1]: https://dev.mysql.com/doc/refman/8.0/en/datetime.html
    Datetime(Option<u64>),
    /// Datetime with time precision and optional timezone, see [ClickHouse][1].
    ///
    /// [1]: https://clickhouse.com/docs/en/sql-reference/data-types/datetime64
    Datetime64(u64, Option<String>),
    /// Timestamp with optional time precision and time zone information, see [SQL Standard][1].
    ///
    /// [1]: https://jakewheat.github.io/sql-overview/sql-2016-foundation-grammar.html#datetime-type
    Timestamp(Option<u64>, TimezoneInfo),
    /// Databricks timestamp without time zone. See [1].
    ///
    /// [1]: https://docs.databricks.com/aws/en/sql/language-manual/data-types/timestamp-ntz-type
    TimestampNtz,
    /// Interval type.
    Interval,
    /// JSON type.
    JSON,
    /// Binary JSON type.
    JSONB,
    /// Regclass used in [PostgreSQL] serial.
    ///
    /// [PostgreSQL]: https://www.postgresql.org/docs/current/datatype.html
    Regclass,
    /// Text type.
    Text,
    /// [MySQL] text with up to 2**8 bytes.
    ///
    /// [MySQL]: https://dev.mysql.com/doc/refman/9.1/en/blob.html
    TinyText,
    /// [MySQL] text with up to 2**24 bytes.
    ///
    /// [MySQL]: https://dev.mysql.com/doc/refman/9.1/en/blob.html
    MediumText,
    /// [MySQL] text with up to 2**32 bytes.
    ///
    /// [MySQL]: https://dev.mysql.com/doc/refman/9.1/en/blob.html
    LongText,
    /// String with optional length.
    String(Option<u64>),
    /// A fixed-length string e.g [ClickHouse][1].
    ///
    /// [1]: https://clickhouse.com/docs/en/sql-reference/data-types/fixedstring
    FixedString(u64),
    /// Bytea type, see [PostgreSQL].
    ///
    /// [PostgreSQL]: https://www.postgresql.org/docs/current/datatype-bit.html
    Bytea,
    /// Bit string, see [PostgreSQL], [MySQL], or [MSSQL].
    ///
    /// [PostgreSQL]: https://www.postgresql.org/docs/current/datatype-bit.html
    /// [MySQL]: https://dev.mysql.com/doc/refman/9.1/en/bit-type.html
    /// [MSSQL]: https://learn.microsoft.com/en-us/sql/t-sql/data-types/bit-transact-sql?view=sql-server-ver16
    Bit(Option<u64>),
    /// `BIT VARYING(n)`: Variable-length bit string, see [PostgreSQL].
    ///
    /// [PostgreSQL]: https://www.postgresql.org/docs/current/datatype-bit.html
    BitVarying(Option<u64>),
    /// `VARBIT(n)`: Variable-length bit string. [PostgreSQL] alias for `BIT VARYING`.
    ///
    /// [PostgreSQL]: https://www.postgresql.org/docs/current/datatype.html
    VarBit(Option<u64>),
    /// Custom types.
    Custom(ObjectName, Vec<String>),
    /// Arrays.
    Array(ArrayElemTypeDef),
    /// Map, see [ClickHouse].
    ///
    /// [ClickHouse]: https://clickhouse.com/docs/en/sql-reference/data-types/map
    Map(Box<DataType>, Box<DataType>),
    /// Tuple, see [ClickHouse].
    ///
    /// [ClickHouse]: https://clickhouse.com/docs/en/sql-reference/data-types/tuple
    Tuple(Vec<StructField>),
    /// Nested type, see [ClickHouse].
    ///
    /// [ClickHouse]: https://clickhouse.com/docs/en/sql-reference/data-types/nested-data-structures/nested
    Nested(Vec<ColumnDef>),
    /// Enum type.
    Enum(Vec<EnumMember>, Option<u8>),
    /// Set type.
    Set(Vec<String>),
    /// Struct type, see [Hive], [BigQuery].
    ///
    /// [Hive]: https://docs.cloudera.com/cdw-runtime/cloud/impala-sql-reference/topics/impala-struct.html
    /// [BigQuery]: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#struct_type
    Struct(Vec<StructField>, StructBracketKind),
    /// Union type, see [DuckDB].
    ///
    /// [DuckDB]: https://duckdb.org/docs/sql/data_types/union.html
    Union(Vec<UnionField>),
    /// Nullable - special marker NULL represents in ClickHouse as a data type.
    ///
    /// [ClickHouse]: https://clickhouse.com/docs/en/sql-reference/data-types/nullable
    Nullable(Box<DataType>),
    /// LowCardinality - changes the internal representation of other data types to be dictionary-encoded.
    ///
    /// [ClickHouse]: https://clickhouse.com/docs/en/sql-reference/data-types/lowcardinality
    LowCardinality(Box<DataType>),
    /// No type specified - only used with
    /// [`SQLiteDialect`](crate::dialect::SQLiteDialect), from statements such
    /// as `CREATE TABLE t1 (a)`.
    Unspecified,
    /// Trigger data type, returned by functions associated with triggers, see [PostgreSQL].
    ///
    /// [PostgreSQL]: https://www.postgresql.org/docs/current/plpgsql-trigger.html
    Trigger,
    /// Any data type, used in BigQuery UDF definitions for templated parameters, see [BigQuery].
    ///
    /// [BigQuery]: https://cloud.google.com/bigquery/docs/user-defined-functions#templated-sql-udf-parameters
    AnyType,
    /// Geometric type, see [PostgreSQL].
    ///
    /// [PostgreSQL]: https://www.postgresql.org/docs/9.5/functions-geometry.html
    GeometricType(GeometricTypeKind),
    /// PostgreSQL text search vectors, see [PostgreSQL].
    ///
    /// [PostgreSQL]: https://www.postgresql.org/docs/17/datatype-textsearch.html
    TsVector,
    /// PostgreSQL text search query, see [PostgreSQL].
    ///
    /// [PostgreSQL]: https://www.postgresql.org/docs/17/datatype-textsearch.html
    TsQuery,
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
            DataType::Nvarchar(size) => format_character_string_type(f, "NVARCHAR", size),
            DataType::Uuid => write!(f, "UUID"),
            DataType::CharacterLargeObject(size) => {
                format_type_with_optional_length(f, "CHARACTER LARGE OBJECT", size, false)
            }
            DataType::CharLargeObject(size) => {
                format_type_with_optional_length(f, "CHAR LARGE OBJECT", size, false)
            }
            DataType::Clob(size) => format_type_with_optional_length(f, "CLOB", size, false),
            DataType::Binary(size) => format_type_with_optional_length(f, "BINARY", size, false),
            DataType::Varbinary(size) => format_varbinary_type(f, "VARBINARY", size),
            DataType::Blob(size) => format_type_with_optional_length(f, "BLOB", size, false),
            DataType::TinyBlob => write!(f, "TINYBLOB"),
            DataType::MediumBlob => write!(f, "MEDIUMBLOB"),
            DataType::LongBlob => write!(f, "LONGBLOB"),
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
            DataType::TinyIntUnsigned(zerofill) => {
                format_type_with_optional_length(f, "TINYINT", zerofill, true)
            }
            DataType::Int2(zerofill) => {
                format_type_with_optional_length(f, "INT2", zerofill, false)
            }
            DataType::Int2Unsigned(zerofill) => {
                format_type_with_optional_length(f, "INT2", zerofill, true)
            }
            DataType::SmallInt(zerofill) => {
                format_type_with_optional_length(f, "SMALLINT", zerofill, false)
            }
            DataType::SmallIntUnsigned(zerofill) => {
                format_type_with_optional_length(f, "SMALLINT", zerofill, true)
            }
            DataType::MediumInt(zerofill) => {
                format_type_with_optional_length(f, "MEDIUMINT", zerofill, false)
            }
            DataType::MediumIntUnsigned(zerofill) => {
                format_type_with_optional_length(f, "MEDIUMINT", zerofill, true)
            }
            DataType::Int(zerofill) => format_type_with_optional_length(f, "INT", zerofill, false),
            DataType::IntUnsigned(zerofill) => {
                format_type_with_optional_length(f, "INT", zerofill, true)
            }
            DataType::Int4(zerofill) => {
                format_type_with_optional_length(f, "INT4", zerofill, false)
            }
            DataType::Int8(zerofill) => {
                format_type_with_optional_length(f, "INT8", zerofill, false)
            }
            DataType::Int16 => {
                write!(f, "Int16")
            }
            DataType::Int32 => {
                write!(f, "Int32")
            }
            DataType::Int64 => {
                write!(f, "INT64")
            }
            DataType::Int128 => {
                write!(f, "Int128")
            }
            DataType::Int256 => {
                write!(f, "Int256")
            }
            DataType::HugeInt => {
                write!(f, "HUGEINT")
            }
            DataType::Int4Unsigned(zerofill) => {
                format_type_with_optional_length(f, "INT4", zerofill, true)
            }
            DataType::Integer(zerofill) => {
                format_type_with_optional_length(f, "INTEGER", zerofill, false)
            }
            DataType::IntegerUnsigned(zerofill) => {
                format_type_with_optional_length(f, "INTEGER", zerofill, true)
            }
            DataType::BigInt(zerofill) => {
                format_type_with_optional_length(f, "BIGINT", zerofill, false)
            }
            DataType::BigIntUnsigned(zerofill) => {
                format_type_with_optional_length(f, "BIGINT", zerofill, true)
            }
            DataType::Int8Unsigned(zerofill) => {
                format_type_with_optional_length(f, "INT8", zerofill, true)
            }
            DataType::UTinyInt => {
                write!(f, "UTINYINT")
            }
            DataType::USmallInt => {
                write!(f, "USMALLINT")
            }
            DataType::UBigInt => {
                write!(f, "UBIGINT")
            }
            DataType::UHugeInt => {
                write!(f, "UHUGEINT")
            }
            DataType::UInt8 => {
                write!(f, "UInt8")
            }
            DataType::UInt16 => {
                write!(f, "UInt16")
            }
            DataType::UInt32 => {
                write!(f, "UInt32")
            }
            DataType::UInt64 => {
                write!(f, "UInt64")
            }
            DataType::UInt128 => {
                write!(f, "UInt128")
            }
            DataType::UInt256 => {
                write!(f, "UInt256")
            }
            DataType::Signed => {
                write!(f, "SIGNED")
            }
            DataType::SignedInteger => {
                write!(f, "SIGNED INTEGER")
            }
            DataType::Unsigned => {
                write!(f, "UNSIGNED")
            }
            DataType::UnsignedInteger => {
                write!(f, "UNSIGNED INTEGER")
            }
            DataType::Real => write!(f, "REAL"),
            DataType::Float4 => write!(f, "FLOAT4"),
            DataType::Float32 => write!(f, "Float32"),
            DataType::Float64 => write!(f, "FLOAT64"),
            DataType::Double(info) => write!(f, "DOUBLE{info}"),
            DataType::Float8 => write!(f, "FLOAT8"),
            DataType::DoublePrecision => write!(f, "DOUBLE PRECISION"),
            DataType::Bool => write!(f, "BOOL"),
            DataType::Boolean => write!(f, "BOOLEAN"),
            DataType::Date => write!(f, "DATE"),
            DataType::Date32 => write!(f, "Date32"),
            DataType::Time(precision, timezone_info) => {
                format_datetime_precision_and_tz(f, "TIME", precision, timezone_info)
            }
            DataType::Datetime(precision) => {
                format_type_with_optional_length(f, "DATETIME", precision, false)
            }
            DataType::Timestamp(precision, timezone_info) => {
                format_datetime_precision_and_tz(f, "TIMESTAMP", precision, timezone_info)
            }
            DataType::TimestampNtz => write!(f, "TIMESTAMP_NTZ"),
            DataType::Datetime64(precision, timezone) => {
                format_clickhouse_datetime_precision_and_timezone(
                    f,
                    "DateTime64",
                    precision,
                    timezone,
                )
            }
            DataType::Interval => write!(f, "INTERVAL"),
            DataType::JSON => write!(f, "JSON"),
            DataType::JSONB => write!(f, "JSONB"),
            DataType::Regclass => write!(f, "REGCLASS"),
            DataType::Text => write!(f, "TEXT"),
            DataType::TinyText => write!(f, "TINYTEXT"),
            DataType::MediumText => write!(f, "MEDIUMTEXT"),
            DataType::LongText => write!(f, "LONGTEXT"),
            DataType::String(size) => format_type_with_optional_length(f, "STRING", size, false),
            DataType::Bytea => write!(f, "BYTEA"),
            DataType::Bit(size) => format_type_with_optional_length(f, "BIT", size, false),
            DataType::BitVarying(size) => {
                format_type_with_optional_length(f, "BIT VARYING", size, false)
            }
            DataType::VarBit(size) => format_type_with_optional_length(f, "VARBIT", size, false),
            DataType::Array(ty) => match ty {
                ArrayElemTypeDef::None => write!(f, "ARRAY"),
                ArrayElemTypeDef::SquareBracket(t, None) => write!(f, "{t}[]"),
                ArrayElemTypeDef::SquareBracket(t, Some(size)) => write!(f, "{t}[{size}]"),
                ArrayElemTypeDef::AngleBracket(t) => write!(f, "ARRAY<{t}>"),
                ArrayElemTypeDef::Parenthesis(t) => write!(f, "Array({t})"),
            },
            DataType::Custom(ty, modifiers) => {
                if modifiers.is_empty() {
                    write!(f, "{ty}")
                } else {
                    write!(f, "{}({})", ty, modifiers.join(", "))
                }
            }
            DataType::Enum(vals, bits) => {
                match bits {
                    Some(bits) => write!(f, "ENUM{bits}"),
                    None => write!(f, "ENUM"),
                }?;
                write!(f, "(")?;
                for (i, v) in vals.iter().enumerate() {
                    if i != 0 {
                        write!(f, ", ")?;
                    }
                    match v {
                        EnumMember::Name(name) => {
                            write!(f, "'{}'", escape_single_quote_string(name))?
                        }
                        EnumMember::NamedValue(name, value) => {
                            write!(f, "'{}' = {}", escape_single_quote_string(name), value)?
                        }
                    }
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
            DataType::Struct(fields, bracket) => {
                if !fields.is_empty() {
                    match bracket {
                        StructBracketKind::Parentheses => {
                            write!(f, "STRUCT({})", display_comma_separated(fields))
                        }
                        StructBracketKind::AngleBrackets => {
                            write!(f, "STRUCT<{}>", display_comma_separated(fields))
                        }
                    }
                } else {
                    write!(f, "STRUCT")
                }
            }
            DataType::Union(fields) => {
                write!(f, "UNION({})", display_comma_separated(fields))
            }
            // ClickHouse
            DataType::Nullable(data_type) => {
                write!(f, "Nullable({data_type})")
            }
            DataType::FixedString(character_length) => {
                write!(f, "FixedString({character_length})")
            }
            DataType::LowCardinality(data_type) => {
                write!(f, "LowCardinality({data_type})")
            }
            DataType::Map(key_data_type, value_data_type) => {
                write!(f, "Map({key_data_type}, {value_data_type})")
            }
            DataType::Tuple(fields) => {
                write!(f, "Tuple({})", display_comma_separated(fields))
            }
            DataType::Nested(fields) => {
                write!(f, "Nested({})", display_comma_separated(fields))
            }
            DataType::Unspecified => Ok(()),
            DataType::Trigger => write!(f, "TRIGGER"),
            DataType::AnyType => write!(f, "ANY TYPE"),
            DataType::Table(fields) => match fields {
                Some(fields) => {
                    write!(f, "TABLE({})", display_comma_separated(fields))
                }
                None => {
                    write!(f, "TABLE")
                }
            },
            DataType::NamedTable { name, columns } => {
                write!(f, "{} TABLE ({})", name, display_comma_separated(columns))
            }
            DataType::GeometricType(kind) => write!(f, "{kind}"),
            DataType::TsVector => write!(f, "TSVECTOR"),
            DataType::TsQuery => write!(f, "TSQUERY"),
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

fn format_varbinary_type(
    f: &mut fmt::Formatter,
    sql_type: &str,
    size: &Option<BinaryLength>,
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

fn format_clickhouse_datetime_precision_and_timezone(
    f: &mut fmt::Formatter,
    sql_type: &'static str,
    len: &u64,
    time_zone: &Option<String>,
) -> fmt::Result {
    write!(f, "{sql_type}({len}")?;

    if let Some(time_zone) = time_zone {
        write!(f, ", '{time_zone}'")?;
    }

    write!(f, ")")?;

    Ok(())
}

/// Type of brackets used for `STRUCT` literals.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum StructBracketKind {
    /// Example: `STRUCT(a INT, b STRING)`
    Parentheses,
    /// Example: `STRUCT<a INT, b STRING>`
    AngleBrackets,
}

/// Timestamp and Time data types information about TimeZone formatting.
///
/// This is more related to a display information than real differences between each variant. To
/// guarantee compatibility with the input query we must maintain its exact information.
#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum TimezoneInfo {
    /// No information about time zone, e.g. TIMESTAMP
    None,
    /// Temporal type 'WITH TIME ZONE', e.g. TIMESTAMP WITH TIME ZONE, [SQL Standard], [Oracle]
    ///
    /// [SQL Standard]: https://jakewheat.github.io/sql-overview/sql-2016-foundation-grammar.html#datetime-type
    /// [Oracle]: https://docs.oracle.com/en/database/oracle/oracle-database/12.2/nlspg/datetime-data-types-and-time-zone-support.html#GUID-3F1C388E-C651-43D5-ADBC-1A49E5C2CA05
    WithTimeZone,
    /// Temporal type 'WITHOUT TIME ZONE', e.g. TIME WITHOUT TIME ZONE, [SQL Standard], [Postgresql]
    ///
    /// [SQL Standard]: https://jakewheat.github.io/sql-overview/sql-2016-foundation-grammar.html#datetime-type
    /// [Postgresql]: https://www.postgresql.org/docs/current/datatype-datetime.html
    WithoutTimeZone,
    /// Postgresql specific `WITH TIME ZONE` formatting, for both TIME and TIMESTAMP, e.g. TIMETZ, [Postgresql]
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
/// following the 2016 [SQL Standard].
///
/// [SQL Standard]: https://jakewheat.github.io/sql-overview/sql-2016-foundation-grammar.html#exact-numeric-type
#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum ExactNumberInfo {
    /// No additional information, e.g. `DECIMAL`
    None,
    /// Only precision information, e.g. `DECIMAL(10)`
    Precision(u64),
    /// Precision and scale information, e.g. `DECIMAL(10,2)`
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
pub enum CharacterLength {
    IntegerLength {
        /// Default (if VARYING) or maximum (if not VARYING) length
        length: u64,
        /// Optional unit. If not informed, the ANSI handles it as CHARACTERS implicitly
        unit: Option<CharLengthUnits>,
    },
    /// VARCHAR(MAX) or NVARCHAR(MAX), used in T-SQL (Microsoft SQL Server)
    Max,
}

impl fmt::Display for CharacterLength {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CharacterLength::IntegerLength { length, unit } => {
                write!(f, "{length}")?;
                if let Some(unit) = unit {
                    write!(f, " {unit}")?;
                }
            }
            CharacterLength::Max => {
                write!(f, "MAX")?;
            }
        }
        Ok(())
    }
}

/// Possible units for characters, initially based on 2016 ANSI [SQL Standard][1].
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

#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum BinaryLength {
    IntegerLength {
        /// Default (if VARYING)
        length: u64,
    },
    /// VARBINARY(MAX) used in T-SQL (Microsoft SQL Server)
    Max,
}

impl fmt::Display for BinaryLength {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BinaryLength::IntegerLength { length } => {
                write!(f, "{length}")?;
            }
            BinaryLength::Max => {
                write!(f, "MAX")?;
            }
        }
        Ok(())
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
    /// `INT[]` or `INT[2]`
    SquareBracket(Box<DataType>, Option<u64>),
    /// `Array(Int64)`
    Parenthesis(Box<DataType>),
}

/// Represents different types of geometric shapes which are commonly used in
/// PostgreSQL/Redshift for spatial operations and geometry-related computations.
///
/// [PostgreSQL]: https://www.postgresql.org/docs/9.5/functions-geometry.html
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum GeometricTypeKind {
    Point,
    Line,
    LineSegment,
    GeometricBox,
    GeometricPath,
    Polygon,
    Circle,
}

impl fmt::Display for GeometricTypeKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            GeometricTypeKind::Point => write!(f, "point"),
            GeometricTypeKind::Line => write!(f, "line"),
            GeometricTypeKind::LineSegment => write!(f, "lseg"),
            GeometricTypeKind::GeometricBox => write!(f, "box"),
            GeometricTypeKind::GeometricPath => write!(f, "path"),
            GeometricTypeKind::Polygon => write!(f, "polygon"),
            GeometricTypeKind::Circle => write!(f, "circle"),
        }
    }
}

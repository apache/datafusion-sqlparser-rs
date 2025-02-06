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

use core::fmt::{self, Display};

use crate::keywords::Keyword;

/// Trait characterizing the operator classes
pub trait OperatorClass: From<Keyword> + Into<IndexOperatorClass> {
    /// List of keywords associated to the operator class
    const KEYWORDS: &'static [Keyword];
}

/// Bloom-index specific operator classes
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum BloomOperatorClass {
    Int4,
    Text,
}

impl OperatorClass for BloomOperatorClass {
    const KEYWORDS: &'static [Keyword] = &[Keyword::INT4_OPS, Keyword::TEXT_OPS];
}

impl From<Keyword> for BloomOperatorClass {
    fn from(keyword: Keyword) -> Self {
        match keyword {
            Keyword::INT4_OPS => BloomOperatorClass::Int4,
            Keyword::TEXT_OPS => BloomOperatorClass::Text,
            _ => unreachable!(),
        }
    }
}

impl Display for BloomOperatorClass {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            BloomOperatorClass::Int4 => write!(f, "int4_ops"),
            BloomOperatorClass::Text => write!(f, "text_ops"),
        }
    }
}

/// BTree GIN-based index operator class
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum GINOperatorClass {
    Int2,
    Int4,
    Int8,
    Float4,
    Float8,
    Money,
    OID,
    Timestamp,
    TimestampTZ,
    Time,
    TimeTZ,
    Date,
    Interval,
    MACAddr,
    MACAddr8,
    INET,
    CIDR,
    Text,
    VARCHAR,
    CHAR,
    Bytea,
    Bit,
    Varbit,
    Numeric,
    Enum,
    UUID,
    Name,
    Bool,
    BPChar,
    /// Support for similarity of text using trigram matching: [`pg_tgrm`](https://www.postgresql.org/docs/current/pgtrgm.html)
    TRGM,
    /// Type for storing sets of key/value pairs within a single PostgreSQL value: [`hstore`](https://www.postgresql.org/docs/current/hstore.html)
    HStore,
}

impl OperatorClass for GINOperatorClass {
    const KEYWORDS: &'static [Keyword] = &[
        Keyword::INT2_OPS,
        Keyword::INT4_OPS,
        Keyword::INT8_OPS,
        Keyword::FLOAT4_OPS,
        Keyword::FLOAT8_OPS,
        Keyword::MONEY_OPS,
        Keyword::OID_OPS,
        Keyword::TIMESTAMP_OPS,
        Keyword::TIMESTAMPTZ_OPS,
        Keyword::TIME_OPS,
        Keyword::TIMETZ_OPS,
        Keyword::DATE_OPS,
        Keyword::INTERVAL_OPS,
        Keyword::MACADDR_OPS,
        Keyword::MACADDR8_OPS,
        Keyword::INET_OPS,
        Keyword::CIDR_OPS,
        Keyword::TEXT_OPS,
        Keyword::VARCHAR_OPS,
        Keyword::CHAR_OPS,
        Keyword::BYTEA_OPS,
        Keyword::BIT_OPS,
        Keyword::VARBIT_OPS,
        Keyword::NUMERIC_OPS,
        Keyword::ENUM_OPS,
        Keyword::UUID_OPS,
        Keyword::NAME_OPS,
        Keyword::BOOL_OPS,
        Keyword::BPCHAR_OPS,
        Keyword::GIN_TRGM_OPS,
        Keyword::GIN_HSTORE_OPS,
    ];
}

impl From<Keyword> for GINOperatorClass {
    fn from(keyword: Keyword) -> Self {
        match keyword {
            Keyword::INT2_OPS => GINOperatorClass::Int2,
            Keyword::INT4_OPS => GINOperatorClass::Int4,
            Keyword::INT8_OPS => GINOperatorClass::Int8,
            Keyword::FLOAT4_OPS => GINOperatorClass::Float4,
            Keyword::FLOAT8_OPS => GINOperatorClass::Float8,
            Keyword::MONEY_OPS => GINOperatorClass::Money,
            Keyword::OID_OPS => GINOperatorClass::OID,
            Keyword::TIMESTAMP_OPS => GINOperatorClass::Timestamp,
            Keyword::TIMESTAMPTZ_OPS => GINOperatorClass::TimestampTZ,
            Keyword::TIME_OPS => GINOperatorClass::Time,
            Keyword::TIMETZ_OPS => GINOperatorClass::TimeTZ,
            Keyword::DATE_OPS => GINOperatorClass::Date,
            Keyword::INTERVAL_OPS => GINOperatorClass::Interval,
            Keyword::MACADDR_OPS => GINOperatorClass::MACAddr,
            Keyword::MACADDR8_OPS => GINOperatorClass::MACAddr8,
            Keyword::INET_OPS => GINOperatorClass::INET,
            Keyword::CIDR_OPS => GINOperatorClass::CIDR,
            Keyword::TEXT_OPS => GINOperatorClass::Text,
            Keyword::VARCHAR_OPS => GINOperatorClass::VARCHAR,
            Keyword::CHAR_OPS => GINOperatorClass::CHAR,
            Keyword::BYTEA_OPS => GINOperatorClass::Bytea,
            Keyword::BIT_OPS => GINOperatorClass::Bit,
            Keyword::VARBIT_OPS => GINOperatorClass::Varbit,
            Keyword::NUMERIC_OPS => GINOperatorClass::Numeric,
            Keyword::ENUM_OPS => GINOperatorClass::Enum,
            Keyword::UUID_OPS => GINOperatorClass::UUID,
            Keyword::NAME_OPS => GINOperatorClass::Name,
            Keyword::BOOL_OPS => GINOperatorClass::Bool,
            Keyword::BPCHAR_OPS => GINOperatorClass::BPChar,
            Keyword::GIN_TRGM_OPS => GINOperatorClass::TRGM,
            Keyword::GIN_HSTORE_OPS => GINOperatorClass::HStore,
            _ => unreachable!(),
        }
    }
}

impl Display for GINOperatorClass {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            GINOperatorClass::Int2 => write!(f, "int2_ops"),
            GINOperatorClass::Int4 => write!(f, "int4_ops"),
            GINOperatorClass::Int8 => write!(f, "int8_ops"),
            GINOperatorClass::Float4 => write!(f, "float4_ops"),
            GINOperatorClass::Float8 => write!(f, "float8_ops"),
            GINOperatorClass::Money => write!(f, "money_ops"),
            GINOperatorClass::OID => write!(f, "oid_ops"),
            GINOperatorClass::Timestamp => write!(f, "timestamp_ops"),
            GINOperatorClass::TimestampTZ => write!(f, "timestamptz_ops"),
            GINOperatorClass::Time => write!(f, "time_ops"),
            GINOperatorClass::TimeTZ => write!(f, "timetz_ops"),
            GINOperatorClass::Date => write!(f, "date_ops"),
            GINOperatorClass::Interval => write!(f, "interval_ops"),
            GINOperatorClass::MACAddr => write!(f, "macaddr_ops"),
            GINOperatorClass::MACAddr8 => write!(f, "macaddr8_ops"),
            GINOperatorClass::INET => write!(f, "inet_ops"),
            GINOperatorClass::CIDR => write!(f, "cidr_ops"),
            GINOperatorClass::Text => write!(f, "text_ops"),
            GINOperatorClass::VARCHAR => write!(f, "varchar_ops"),
            GINOperatorClass::CHAR => write!(f, "char_ops"),
            GINOperatorClass::Bytea => write!(f, "bytea_ops"),
            GINOperatorClass::Bit => write!(f, "bit_ops"),
            GINOperatorClass::Varbit => write!(f, "varbit_ops"),
            GINOperatorClass::Numeric => write!(f, "numeric_ops"),
            GINOperatorClass::Enum => write!(f, "enum_ops"),
            GINOperatorClass::UUID => write!(f, "uuid_ops"),
            GINOperatorClass::Name => write!(f, "name_ops"),
            GINOperatorClass::Bool => write!(f, "bool_ops"),
            GINOperatorClass::BPChar => write!(f, "bpchar_ops"),
            GINOperatorClass::TRGM => write!(f, "gin_trgm_ops"),
            GINOperatorClass::HStore => write!(f, "gin_hstore_ops"),
        }
    }
}

/// BTree GIST-based index operator class
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum GiSTOperatorClass {
    OID,
    UUID,
    Int2,
    Int4,
    Int8,
    Float4,
    Float8,
    Timestamp,
    TimestampTZ,
    Time,
    TimeTZ,
    Date,
    Interval,
    Cash,
    MACAddr,
    MACAddr8,
    Text,
    BPChar,
    Bytea,
    Numeric,
    Bit,
    VBit,
    INet,
    CIDR,
    Enum,
    Bool,
    /// Type for representing line segments, or floating point intervals: [`seg`](https://www.postgresql.org/docs/current/seg.html)
    SEG,
    /// Support for similarity of text using trigram matching: [`pg_tgrm`](https://www.postgresql.org/docs/current/pgtrgm.html)
    TRGM,
    /// Type for representing labels of data stored in a hierarchical tree-like structure: [`ltree`](https://www.postgresql.org/docs/current/ltree.html)
    LTREE,
    /// Type for storing sets of key/value pairs within a single PostgreSQL value: [`hstore`](https://www.postgresql.org/docs/current/hstore.html)
    HStore,
    /// Type cube for representing multidimensional cubes: [`cube`](https://www.postgresql.org/docs/current/cube.html)
    Cube,
}

impl OperatorClass for GiSTOperatorClass {
    const KEYWORDS: &'static [Keyword] = &[
        Keyword::GIST_OID_OPS,
        Keyword::GIST_UUID_OPS,
        Keyword::GIST_INT2_OPS,
        Keyword::GIST_INT4_OPS,
        Keyword::GIST_INT8_OPS,
        Keyword::GIST_FLOAT4_OPS,
        Keyword::GIST_FLOAT8_OPS,
        Keyword::GIST_TIMESTAMP_OPS,
        Keyword::GIST_TIMESTAMPTZ_OPS,
        Keyword::GIST_TIME_OPS,
        Keyword::GIST_TIMETZ_OPS,
        Keyword::GIST_DATE_OPS,
        Keyword::GIST_INTERVAL_OPS,
        Keyword::GIST_CASH_OPS,
        Keyword::GIST_MACADDR_OPS,
        Keyword::GIST_MACADDR8_OPS,
        Keyword::GIST_TEXT_OPS,
        Keyword::GIST_BPCHAR_OPS,
        Keyword::GIST_BYTEA_OPS,
        Keyword::GIST_NUMERIC_OPS,
        Keyword::GIST_BIT_OPS,
        Keyword::GIST_VBIT_OPS,
        Keyword::GIST_INET_OPS,
        Keyword::GIST_CIDR_OPS,
        Keyword::GIST_ENUM_OPS,
        Keyword::GIST_BOOL_OPS,
        Keyword::GIST_SEG_OPS,
        Keyword::GIST_TRGM_OPS,
        Keyword::GIST_LTREE_OPS,
        Keyword::GIST_HSTORE_OPS,
        Keyword::GIST_CUBE_OPS,
    ];
}

impl From<Keyword> for GiSTOperatorClass {
    fn from(keyword: Keyword) -> Self {
        match keyword {
            Keyword::GIST_OID_OPS => GiSTOperatorClass::OID,
            Keyword::GIST_UUID_OPS => GiSTOperatorClass::UUID,
            Keyword::GIST_INT2_OPS => GiSTOperatorClass::Int2,
            Keyword::GIST_INT4_OPS => GiSTOperatorClass::Int4,
            Keyword::GIST_INT8_OPS => GiSTOperatorClass::Int8,
            Keyword::GIST_FLOAT4_OPS => GiSTOperatorClass::Float4,
            Keyword::GIST_FLOAT8_OPS => GiSTOperatorClass::Float8,
            Keyword::GIST_TIMESTAMP_OPS => GiSTOperatorClass::Timestamp,
            Keyword::GIST_TIMESTAMPTZ_OPS => GiSTOperatorClass::TimestampTZ,
            Keyword::GIST_TIME_OPS => GiSTOperatorClass::Time,
            Keyword::GIST_TIMETZ_OPS => GiSTOperatorClass::TimeTZ,
            Keyword::GIST_DATE_OPS => GiSTOperatorClass::Date,
            Keyword::GIST_INTERVAL_OPS => GiSTOperatorClass::Interval,
            Keyword::GIST_CASH_OPS => GiSTOperatorClass::Cash,
            Keyword::GIST_MACADDR_OPS => GiSTOperatorClass::MACAddr,
            Keyword::GIST_MACADDR8_OPS => GiSTOperatorClass::MACAddr8,
            Keyword::GIST_TEXT_OPS => GiSTOperatorClass::Text,
            Keyword::GIST_BPCHAR_OPS => GiSTOperatorClass::BPChar,
            Keyword::GIST_BYTEA_OPS => GiSTOperatorClass::Bytea,
            Keyword::GIST_NUMERIC_OPS => GiSTOperatorClass::Numeric,
            Keyword::GIST_BIT_OPS => GiSTOperatorClass::Bit,
            Keyword::GIST_VBIT_OPS => GiSTOperatorClass::VBit,
            Keyword::GIST_INET_OPS => GiSTOperatorClass::INet,
            Keyword::GIST_CIDR_OPS => GiSTOperatorClass::CIDR,
            Keyword::GIST_ENUM_OPS => GiSTOperatorClass::Enum,
            Keyword::GIST_BOOL_OPS => GiSTOperatorClass::Bool,
            Keyword::GIST_SEG_OPS => GiSTOperatorClass::SEG,
            Keyword::GIST_TRGM_OPS => GiSTOperatorClass::TRGM,
            Keyword::GIST_LTREE_OPS => GiSTOperatorClass::LTREE,
            Keyword::GIST_HSTORE_OPS => GiSTOperatorClass::HStore,
            Keyword::GIST_CUBE_OPS => GiSTOperatorClass::Cube,
            _ => unreachable!(),
        }
    }
}

impl Display for GiSTOperatorClass {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            GiSTOperatorClass::OID => write!(f, "gist_oid_ops"),
            GiSTOperatorClass::UUID => write!(f, "gist_uuid_ops"),
            GiSTOperatorClass::Int2 => write!(f, "gist_int2_ops"),
            GiSTOperatorClass::Int4 => write!(f, "gist_int4_ops"),
            GiSTOperatorClass::Int8 => write!(f, "gist_int8_ops"),
            GiSTOperatorClass::Float4 => write!(f, "gist_float4_ops"),
            GiSTOperatorClass::Float8 => write!(f, "gist_float8_ops"),
            GiSTOperatorClass::Timestamp => write!(f, "gist_timestamp_ops"),
            GiSTOperatorClass::TimestampTZ => write!(f, "gist_timestamptz_ops"),
            GiSTOperatorClass::Time => write!(f, "gist_time_ops"),
            GiSTOperatorClass::TimeTZ => write!(f, "gist_timetz_ops"),
            GiSTOperatorClass::Date => write!(f, "gist_date_ops"),
            GiSTOperatorClass::Interval => write!(f, "gist_interval_ops"),
            GiSTOperatorClass::Cash => write!(f, "gist_cash_ops"),
            GiSTOperatorClass::MACAddr => write!(f, "gist_macaddr_ops"),
            GiSTOperatorClass::MACAddr8 => write!(f, "gist_macaddr8_ops"),
            GiSTOperatorClass::Text => write!(f, "gist_text_ops"),
            GiSTOperatorClass::BPChar => write!(f, "gist_bpchar_ops"),
            GiSTOperatorClass::Bytea => write!(f, "gist_bytea_ops"),
            GiSTOperatorClass::Numeric => write!(f, "gist_numeric_ops"),
            GiSTOperatorClass::Bit => write!(f, "gist_bit_ops"),
            GiSTOperatorClass::VBit => write!(f, "GIST_VBIT_OPS"),
            GiSTOperatorClass::INet => write!(f, "gist_inet_ops"),
            GiSTOperatorClass::CIDR => write!(f, "gist_cidr_ops"),
            GiSTOperatorClass::Enum => write!(f, "gist_enum_ops"),
            GiSTOperatorClass::Bool => write!(f, "gist_bool_ops"),
            GiSTOperatorClass::SEG => write!(f, "gist_seg_ops"),
            GiSTOperatorClass::TRGM => write!(f, "gist_trgm_ops"),
            GiSTOperatorClass::LTREE => write!(f, "gist_ltree_ops"),
            GiSTOperatorClass::HStore => write!(f, "gist_hstore_ops"),
            GiSTOperatorClass::Cube => write!(f, "gist_cube_ops"),
        }
    }
}

/// BTree-index specific operator classes
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum BTreeOperatorClass {
    /// The isn module provides data types for the following international product numbering standards: [`isn`](https://www.postgresql.org/docs/current/isn.html)
    EAN13,
    ISBN,
    ISBN13,
    ISMN,
    ISMN13,
    ISSN,
    ISSN13,
    UPC,
    /// Type for representing line segments, or floating point intervals: [`seg`](https://www.postgresql.org/docs/current/seg.html)
    SEG,
    /// Type for storing sets of key/value pairs within a single PostgreSQL value: [`hstore`](https://www.postgresql.org/docs/current/hstore.html)
    HStore,
    /// Type cube for representing multidimensional cubes: [`cube`](https://www.postgresql.org/docs/current/cube.html)
    Cube,
    /// Case-insensitive character string type: [`citext`](https://www.postgresql.org/docs/current/citext.html)
    Citext,
}

impl OperatorClass for BTreeOperatorClass {
    const KEYWORDS: &'static [Keyword] = &[
        Keyword::EAN13_OPS,
        Keyword::ISBN_OPS,
        Keyword::ISBN13_OPS,
        Keyword::ISMN_OPS,
        Keyword::ISMN13_OPS,
        Keyword::ISSN_OPS,
        Keyword::ISSN13_OPS,
        Keyword::UPC_OPS,
        Keyword::SEG_OPS,
        Keyword::BTREE_HSTORE_OPS,
        Keyword::BTREE_CUBE_OPS,
        Keyword::CITEXT_OPS,
    ];
}

impl From<Keyword> for BTreeOperatorClass {
    fn from(keyword: Keyword) -> Self {
        match keyword {
            Keyword::EAN13_OPS => BTreeOperatorClass::EAN13,
            Keyword::ISBN_OPS => BTreeOperatorClass::ISBN,
            Keyword::ISBN13_OPS => BTreeOperatorClass::ISBN13,
            Keyword::ISMN_OPS => BTreeOperatorClass::ISMN,
            Keyword::ISMN13_OPS => BTreeOperatorClass::ISMN13,
            Keyword::ISSN_OPS => BTreeOperatorClass::ISSN,
            Keyword::ISSN13_OPS => BTreeOperatorClass::ISSN13,
            Keyword::UPC_OPS => BTreeOperatorClass::UPC,
            Keyword::SEG_OPS => BTreeOperatorClass::SEG,
            Keyword::BTREE_HSTORE_OPS => BTreeOperatorClass::HStore,
            Keyword::BTREE_CUBE_OPS => BTreeOperatorClass::Cube,
            Keyword::CITEXT_OPS => BTreeOperatorClass::Citext,
            _ => unreachable!(),
        }
    }
}

impl Display for BTreeOperatorClass {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            BTreeOperatorClass::EAN13 => write!(f, "ean13_ops"),
            BTreeOperatorClass::ISBN => write!(f, "isbn_ops"),
            BTreeOperatorClass::ISBN13 => write!(f, "isbn13_ops"),
            BTreeOperatorClass::ISMN => write!(f, "ismn_ops"),
            BTreeOperatorClass::ISMN13 => write!(f, "ismn13_ops"),
            BTreeOperatorClass::ISSN => write!(f, "issn_ops"),
            BTreeOperatorClass::ISSN13 => write!(f, "issn13_ops"),
            BTreeOperatorClass::UPC => write!(f, "upc_ops"),
            BTreeOperatorClass::SEG => write!(f, "seg_ops"),
            BTreeOperatorClass::HStore => write!(f, "btree_hstore_ops"),
            BTreeOperatorClass::Cube => write!(f, "btree_cube_ops"),
            BTreeOperatorClass::Citext => write!(f, "citext_ops"),
        }
    }
}

/// Hash-index specific operator classes
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum HashOperatorClass {
    /// The isn module provides data types for the following international product numbering standards: [`isn`](https://www.postgresql.org/docs/current/isn.html)
    EAN13,
    ISBN,
    ISBN13,
    ISMN,
    ISMN13,
    ISSN,
    ISSN13,
    UPC,
    /// Type for representing labels of data stored in a hierarchical tree-like structure: [`ltree`](https://www.postgresql.org/docs/current/ltree.html)
    LTREE,
    /// Type for storing sets of key/value pairs within a single PostgreSQL value: [`hstore`](https://www.postgresql.org/docs/current/hstore.html)
    HStore,
    /// Case-insensitive character string type: [`citext`](https://www.postgresql.org/docs/current/citext.html)
    Citext,
}

impl OperatorClass for HashOperatorClass {
    const KEYWORDS: &'static [Keyword] = &[
        Keyword::EAN13_OPS,
        Keyword::ISBN_OPS,
        Keyword::ISBN13_OPS,
        Keyword::ISMN_OPS,
        Keyword::ISMN13_OPS,
        Keyword::ISSN_OPS,
        Keyword::ISSN13_OPS,
        Keyword::UPC_OPS,
        Keyword::HASH_LTREE_OPS,
        Keyword::HASH_HSTORE_OPS,
        Keyword::CITEXT_OPS,
    ];
}

impl From<Keyword> for HashOperatorClass {
    fn from(keyword: Keyword) -> Self {
        match keyword {
            Keyword::EAN13_OPS => HashOperatorClass::EAN13,
            Keyword::ISBN_OPS => HashOperatorClass::ISBN,
            Keyword::ISBN13_OPS => HashOperatorClass::ISBN13,
            Keyword::ISMN_OPS => HashOperatorClass::ISMN,
            Keyword::ISMN13_OPS => HashOperatorClass::ISMN13,
            Keyword::ISSN_OPS => HashOperatorClass::ISSN,
            Keyword::ISSN13_OPS => HashOperatorClass::ISSN13,
            Keyword::UPC_OPS => HashOperatorClass::UPC,
            Keyword::HASH_LTREE_OPS => HashOperatorClass::LTREE,
            Keyword::HASH_HSTORE_OPS => HashOperatorClass::HStore,
            Keyword::CITEXT_OPS => HashOperatorClass::Citext,
            _ => unreachable!(),
        }
    }
}

impl Display for HashOperatorClass {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            HashOperatorClass::EAN13 => write!(f, "ean13_ops"),
            HashOperatorClass::ISBN => write!(f, "isbn_ops"),
            HashOperatorClass::ISBN13 => write!(f, "isbn13_ops"),
            HashOperatorClass::ISMN => write!(f, "ismn_ops"),
            HashOperatorClass::ISMN13 => write!(f, "ismn13_ops"),
            HashOperatorClass::ISSN => write!(f, "issn_ops"),
            HashOperatorClass::ISSN13 => write!(f, "issn13_ops"),
            HashOperatorClass::UPC => write!(f, "upc_ops"),
            HashOperatorClass::LTREE => write!(f, "hash_ltree_ops"),
            HashOperatorClass::HStore => write!(f, "hash_hstore_ops"),
            HashOperatorClass::Citext => write!(f, "citext_ops"),
        }
    }
}

/// Index Operator Classes
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum IndexOperatorClass {
    GIN(GINOperatorClass),
    GIST(GiSTOperatorClass),
    Bloom(BloomOperatorClass),
    Hash(HashOperatorClass),
    BTree(BTreeOperatorClass),
}

impl Display for IndexOperatorClass {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            IndexOperatorClass::GIN(op) => write!(f, "{}", op),
            IndexOperatorClass::GIST(op) => write!(f, "{}", op),
            IndexOperatorClass::Bloom(op) => write!(f, "{}", op),
            IndexOperatorClass::Hash(op) => write!(f, "{}", op),
            IndexOperatorClass::BTree(op) => write!(f, "{}", op),
        }
    }
}

impl From<GINOperatorClass> for IndexOperatorClass {
    fn from(op: GINOperatorClass) -> Self {
        IndexOperatorClass::GIN(op)
    }
}

impl From<GiSTOperatorClass> for IndexOperatorClass {
    fn from(op: GiSTOperatorClass) -> Self {
        IndexOperatorClass::GIST(op)
    }
}

impl From<BloomOperatorClass> for IndexOperatorClass {
    fn from(op: BloomOperatorClass) -> Self {
        IndexOperatorClass::Bloom(op)
    }
}

impl From<HashOperatorClass> for IndexOperatorClass {
    fn from(op: HashOperatorClass) -> Self {
        IndexOperatorClass::Hash(op)
    }
}

impl From<BTreeOperatorClass> for IndexOperatorClass {
    fn from(op: BTreeOperatorClass) -> Self {
        IndexOperatorClass::BTree(op)
    }
}

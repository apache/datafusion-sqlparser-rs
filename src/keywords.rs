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

//! This module defines
//! 1) a list of constants for every keyword that
//! can appear in [Word::keyword]:
//!    pub const KEYWORD = "KEYWORD"
//! 2) an `ALL_KEYWORDS` array with every keyword in it
//!     This is not a list of *reserved* keywords: some of these can be
//!     parsed as identifiers if the parser decides so. This means that
//!     new keywords can be added here without affecting the parse result.
//!
//!     As a matter of fact, most of these keywords are not used at all
//!     and could be removed.
//! 3) a `RESERVED_FOR_TABLE_ALIAS` array with keywords reserved in a
//! "table alias" context.

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// Defines a string constant for a single keyword: `kw_def!(SELECT);`
/// expands to `pub const SELECT = "SELECT";`
macro_rules! kw_def {
    ($ident:ident = $string_keyword:expr) => {
        pub const $ident: &'static str = $string_keyword;
    };
    ($ident:ident) => {
        kw_def!($ident = stringify!($ident));
    };
}

/// Expands to a list of `kw_def!()` invocations for each keyword
/// and defines an ALL_KEYWORDS array of the defined constants.
macro_rules! define_keywords {
    ($(
        $ident:ident $(= $string_keyword:expr)?
    ),*) => {
        #[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Eq, Ord, Hash)]
        #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
        #[allow(non_camel_case_types)]
        pub enum Keyword {
            NoKeyword,
            $($ident),*
        }

        pub const ALL_KEYWORDS_INDEX: &[Keyword] = &[
            $(Keyword::$ident),*
        ];

        $(kw_def!($ident $(= $string_keyword)?);)*
        pub const ALL_KEYWORDS: &[&str] = &[
            $($ident),*
        ];
    };
}

// The following keywords should be sorted to be able to match using binary search
define_keywords!(
    ABORT,
    ABS,
    ABSOLUTE,
    ACTION,
    ADD,
    ADMIN,
    ALL,
    ALLOCATE,
    ALTER,
    ANALYZE,
    AND,
    ANY,
    APPLY,
    ARCHIVE,
    ARE,
    ARRAY,
    ARRAY_AGG,
    ARRAY_MAX_CARDINALITY,
    AS,
    ASC,
    ASENSITIVE,
    ASSERT,
    ASYMMETRIC,
    AT,
    ATOMIC,
    AUTHORIZATION,
    AUTOINCREMENT,
    AUTO_INCREMENT,
    AVG,
    AVRO,
    BACKWARD,
    BEGIN,
    BEGIN_FRAME,
    BEGIN_PARTITION,
    BETWEEN,
    BIGINT,
    BINARY,
    BLOB,
    BOOLEAN,
    BOTH,
    BY,
    BYPASSRLS,
    BYTEA,
    CACHE,
    CALL,
    CALLED,
    CARDINALITY,
    CASCADE,
    CASCADED,
    CASE,
    CAST,
    CEIL,
    CEILING,
    CENTURY,
    CHAIN,
    CHANGE,
    CHAR,
    CHARACTER,
    CHARACTER_LENGTH,
    CHARSET,
    CHAR_LENGTH,
    CHECK,
    CLOB,
    CLONE,
    CLOSE,
    CLUSTER,
    COALESCE,
    COLLATE,
    COLLATION,
    COLLECT,
    COLUMN,
    COLUMNS,
    COMMENT,
    COMMIT,
    COMMITTED,
    COMPUTE,
    CONDITION,
    CONNECT,
    CONNECTION,
    CONSTRAINT,
    CONTAINS,
    CONVERT,
    COPY,
    CORR,
    CORRESPONDING,
    COUNT,
    COVAR_POP,
    COVAR_SAMP,
    CREATE,
    CREATEDB,
    CREATEROLE,
    CROSS,
    CSV,
    CUBE,
    CUME_DIST,
    CURRENT,
    CURRENT_CATALOG,
    CURRENT_DATE,
    CURRENT_DEFAULT_TRANSFORM_GROUP,
    CURRENT_PATH,
    CURRENT_ROLE,
    CURRENT_ROW,
    CURRENT_SCHEMA,
    CURRENT_TIME,
    CURRENT_TIMESTAMP,
    CURRENT_TRANSFORM_GROUP_FOR_TYPE,
    CURRENT_USER,
    CURSOR,
    CYCLE,
    DATA,
    DATABASE,
    DATE,
    DATETIME,
    DAY,
    DEALLOCATE,
    DEC,
    DECADE,
    DECIMAL,
    DECLARE,
    DEFAULT,
    DELETE,
    DELIMITED,
    DELIMITER,
    DENSE_RANK,
    DEREF,
    DESC,
    DESCRIBE,
    DETERMINISTIC,
    DIRECTORY,
    DISCARD,
    DISCONNECT,
    DISTINCT,
    DISTRIBUTE,
    DOUBLE,
    DOW,
    DOY,
    DROP,
    DUPLICATE,
    DYNAMIC,
    EACH,
    ELEMENT,
    ELSE,
    ENCODING,
    END,
    END_EXEC = "END-EXEC",
    END_FRAME,
    END_PARTITION,
    ENGINE,
    ENUM,
    EPOCH,
    EQUALS,
    ERROR,
    ESCAPE,
    EVENT,
    EVERY,
    EXCEPT,
    EXEC,
    EXECUTE,
    EXISTS,
    EXP,
    EXPLAIN,
    EXTENDED,
    EXTERNAL,
    EXTRACT,
    FAIL,
    FALSE,
    FETCH,
    FIELDS,
    FILE,
    FILTER,
    FIRST,
    FIRST_VALUE,
    FLOAT,
    FLOOR,
    FOLLOWING,
    FOR,
    FORCE,
    FORCE_NOT_NULL,
    FORCE_NULL,
    FORCE_QUOTE,
    FOREIGN,
    FORMAT,
    FORWARD,
    FRAME_ROW,
    FREE,
    FREEZE,
    FROM,
    FULL,
    FUNCTION,
    FUNCTIONS,
    FUSION,
    GET,
    GLOBAL,
    GRANT,
    GRANTED,
    GRAPHVIZ,
    GROUP,
    GROUPING,
    GROUPS,
    HAVING,
    HEADER,
    HIVEVAR,
    HOLD,
    HOUR,
    IDENTITY,
    IF,
    IGNORE,
    ILIKE,
    IN,
    INDEX,
    INDICATOR,
    INHERIT,
    INNER,
    INOUT,
    INPUTFORMAT,
    INSENSITIVE,
    INSERT,
    INT,
    INTEGER,
    INTERSECT,
    INTERSECTION,
    INTERVAL,
    INTO,
    IS,
    ISODOW,
    ISOLATION,
    ISOYEAR,
    JAR,
    JOIN,
    JSON,
    JSONFILE,
    JULIAN,
    KEY,
    KILL,
    LAG,
    LANGUAGE,
    LARGE,
    LAST,
    LAST_VALUE,
    LATERAL,
    LEAD,
    LEADING,
    LEFT,
    LEVEL,
    LIKE,
    LIKE_REGEX,
    LIMIT,
    LISTAGG,
    LN,
    LOCAL,
    LOCALTIME,
    LOCALTIMESTAMP,
    LOCATION,
    LOGIN,
    LOWER,
    MANAGEDLOCATION,
    MATCH,
    MATCHED,
    MATERIALIZED,
    MAX,
    MEDIUMINT,
    MEMBER,
    MERGE,
    METADATA,
    METHOD,
    MICROSECOND,
    MICROSECONDS,
    MILLENIUM,
    MILLENNIUM,
    MILLISECOND,
    MILLISECONDS,
    MIN,
    MINUTE,
    MOD,
    MODIFIES,
    MODULE,
    MONTH,
    MSCK,
    MULTISET,
    MUTATION,
    NATIONAL,
    NATURAL,
    NCHAR,
    NCLOB,
    NEW,
    NEXT,
    NO,
    NOBYPASSRLS,
    NOCREATEDB,
    NOCREATEROLE,
    NOINHERIT,
    NOLOGIN,
    NONE,
    NOREPLICATION,
    NORMALIZE,
    NOSCAN,
    NOSUPERUSER,
    NOT,
    NTH_VALUE,
    NTILE,
    NULL,
    NULLIF,
    NULLS,
    NUMERIC,
    NVARCHAR,
    OBJECT,
    OCCURRENCES_REGEX,
    OCTET_LENGTH,
    OF,
    OFFSET,
    OLD,
    ON,
    ONLY,
    OPEN,
    OPERATOR,
    OPTION,
    OR,
    ORC,
    ORDER,
    OUT,
    OUTER,
    OUTPUTFORMAT,
    OVER,
    OVERFLOW,
    OVERLAPS,
    OVERLAY,
    OVERWRITE,
    PARAMETER,
    PARQUET,
    PARTITION,
    PARTITIONED,
    PARTITIONS,
    PASSWORD,
    PERCENT,
    PERCENTILE_CONT,
    PERCENTILE_DISC,
    PERCENT_RANK,
    PERIOD,
    PLACING,
    PLANS,
    PORTION,
    POSITION,
    POSITION_REGEX,
    POWER,
    PRECEDES,
    PRECEDING,
    PRECISION,
    PREPARE,
    PRESERVE,
    PRIMARY,
    PRIOR,
    PRIVILEGES,
    PROCEDURE,
    PROGRAM,
    PURGE,
    QUALIFY,
    QUARTER,
    QUERY,
    QUOTE,
    RANGE,
    RANK,
    RCFILE,
    READ,
    READS,
    REAL,
    RECURSIVE,
    REF,
    REFERENCES,
    REFERENCING,
    REGCLASS,
    REGR_AVGX,
    REGR_AVGY,
    REGR_COUNT,
    REGR_INTERCEPT,
    REGR_R2,
    REGR_SLOPE,
    REGR_SXX,
    REGR_SXY,
    REGR_SYY,
    RELATIVE,
    RELEASE,
    RENAME,
    REPAIR,
    REPEATABLE,
    REPLACE,
    REPLICATION,
    RESTRICT,
    RESULT,
    RETURN,
    RETURNS,
    REVOKE,
    RIGHT,
    ROLE,
    ROLLBACK,
    ROLLUP,
    ROW,
    ROWID,
    ROWS,
    ROW_NUMBER,
    SAFE_CAST,
    SAVEPOINT,
    SCHEMA,
    SCOPE,
    SCROLL,
    SEARCH,
    SECOND,
    SELECT,
    SENSITIVE,
    SEQUENCE,
    SEQUENCEFILE,
    SEQUENCES,
    SERDE,
    SERIALIZABLE,
    SESSION,
    SESSION_USER,
    SET,
    SETS,
    SHARE,
    SHOW,
    SIMILAR,
    SMALLINT,
    SNAPSHOT,
    SOME,
    SORT,
    SPECIFIC,
    SPECIFICTYPE,
    SQL,
    SQLEXCEPTION,
    SQLSTATE,
    SQLWARNING,
    SQRT,
    START,
    STATIC,
    STATISTICS,
    STDDEV_POP,
    STDDEV_SAMP,
    STDIN,
    STDOUT,
    STORED,
    STRING,
    SUBMULTISET,
    SUBSTRING,
    SUBSTRING_REGEX,
    SUCCEEDS,
    SUM,
    SUPER,
    SUPERUSER,
    SYMMETRIC,
    SYNC,
    SYSTEM,
    SYSTEM_TIME,
    SYSTEM_USER,
    TABLE,
    TABLES,
    TABLESAMPLE,
    TBLPROPERTIES,
    TEMP,
    TEMPORARY,
    TEXT,
    TEXTFILE,
    THEN,
    TIES,
    TIME,
    TIMESTAMP,
    TIMESTAMPTZ,
    TIMEZONE,
    TIMEZONE_HOUR,
    TIMEZONE_MINUTE,
    TINYINT,
    TO,
    TOP,
    TRAILING,
    TRANSACTION,
    TRANSLATE,
    TRANSLATE_REGEX,
    TRANSLATION,
    TREAT,
    TRIGGER,
    TRIM,
    TRIM_ARRAY,
    TRUE,
    TRUNCATE,
    TRY_CAST,
    TYPE,
    UESCAPE,
    UNBOUNDED,
    UNCOMMITTED,
    UNION,
    UNIQUE,
    UNKNOWN,
    UNLOGGED,
    UNNEST,
    UNSIGNED,
    UNTIL,
    UPDATE,
    UPPER,
    USAGE,
    USE,
    USER,
    USING,
    UUID,
    VALID,
    VALUE,
    VALUES,
    VALUE_OF,
    VARBINARY,
    VARCHAR,
    VARIABLES,
    VARYING,
    VAR_POP,
    VAR_SAMP,
    VERBOSE,
    VERSIONING,
    VIEW,
    VIRTUAL,
    WEEK,
    WHEN,
    WHENEVER,
    WHERE,
    WIDTH_BUCKET,
    WINDOW,
    WITH,
    WITHIN,
    WITHOUT,
    WORK,
    WRITE,
    XOR,
    YEAR,
    ZONE
);

/// These keywords can't be used as a table alias, so that `FROM table_name alias`
/// can be parsed unambiguously without looking ahead.
pub const RESERVED_FOR_TABLE_ALIAS: &[Keyword] = &[
    // Reserved as both a table and a column alias:
    Keyword::WITH,
    Keyword::EXPLAIN,
    Keyword::ANALYZE,
    Keyword::SELECT,
    Keyword::WHERE,
    Keyword::GROUP,
    Keyword::SORT,
    Keyword::HAVING,
    Keyword::ORDER,
    Keyword::TOP,
    Keyword::LATERAL,
    Keyword::VIEW,
    Keyword::LIMIT,
    Keyword::OFFSET,
    Keyword::FETCH,
    Keyword::UNION,
    Keyword::EXCEPT,
    Keyword::INTERSECT,
    // Reserved only as a table alias in the `FROM`/`JOIN` clauses:
    Keyword::ON,
    Keyword::JOIN,
    Keyword::INNER,
    Keyword::CROSS,
    Keyword::FULL,
    Keyword::LEFT,
    Keyword::RIGHT,
    Keyword::NATURAL,
    Keyword::USING,
    Keyword::CLUSTER,
    Keyword::DISTRIBUTE,
    // for MSSQL-specific OUTER APPLY (seems reserved in most dialects)
    Keyword::OUTER,
    Keyword::SET,
    Keyword::QUALIFY,
];

/// Can't be used as a column alias, so that `SELECT <expr> alias`
/// can be parsed unambiguously without looking ahead.
pub const RESERVED_FOR_COLUMN_ALIAS: &[Keyword] = &[
    // Reserved as both a table and a column alias:
    Keyword::WITH,
    Keyword::EXPLAIN,
    Keyword::ANALYZE,
    Keyword::SELECT,
    Keyword::WHERE,
    Keyword::GROUP,
    Keyword::SORT,
    Keyword::HAVING,
    Keyword::ORDER,
    Keyword::TOP,
    Keyword::LATERAL,
    Keyword::VIEW,
    Keyword::LIMIT,
    Keyword::OFFSET,
    Keyword::FETCH,
    Keyword::UNION,
    Keyword::EXCEPT,
    Keyword::INTERSECT,
    Keyword::CLUSTER,
    Keyword::DISTRIBUTE,
    // Reserved only as a column alias in the `SELECT` clause
    Keyword::FROM,
    Keyword::INTO,
];

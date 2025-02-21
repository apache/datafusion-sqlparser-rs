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

//! This module defines
//! 1) a list of constants for every keyword
//! 2) an `ALL_KEYWORDS` array with every keyword in it
//!     This is not a list of *reserved* keywords: some of these can be
//!     parsed as identifiers if the parser decides so. This means that
//!     new keywords can be added here without affecting the parse result.
//!
//!     As a matter of fact, most of these keywords are not used at all
//!     and could be removed.
//! 3) a `RESERVED_FOR_TABLE_ALIAS` array with keywords reserved in a
//!     "table alias" context.

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

#[cfg(feature = "visitor")]
use sqlparser_derive::{Visit, VisitMut};

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
        #[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
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
    ABSENT,
    ABSOLUTE,
    ACCESS,
    ACCOUNT,
    ACTION,
    ADD,
    ADMIN,
    AFTER,
    AGAINST,
    AGGREGATION,
    ALERT,
    ALGORITHM,
    ALIAS,
    ALL,
    ALLOCATE,
    ALTER,
    ALWAYS,
    ANALYZE,
    AND,
    ANTI,
    ANY,
    APPLICATION,
    APPLY,
    APPLYBUDGET,
    ARCHIVE,
    ARE,
    ARRAY,
    ARRAY_MAX_CARDINALITY,
    AS,
    ASC,
    ASENSITIVE,
    ASOF,
    ASSERT,
    ASYMMETRIC,
    AT,
    ATOMIC,
    ATTACH,
    AUDIT,
    AUTHENTICATION,
    AUTHORIZATION,
    AUTO,
    AUTOINCREMENT,
    AUTO_INCREMENT,
    AVG,
    AVRO,
    BACKWARD,
    BASE64,
    BASE_LOCATION,
    BEFORE,
    BEGIN,
    BEGIN_FRAME,
    BEGIN_PARTITION,
    BERNOULLI,
    BETWEEN,
    BIGDECIMAL,
    BIGINT,
    BIGNUMERIC,
    BINARY,
    BIND,
    BINDING,
    BIT,
    BLOB,
    BLOCK,
    BLOOMFILTER,
    BOOL,
    BOOLEAN,
    BOTH,
    BOX,
    BROWSE,
    BTREE,
    BUCKET,
    BUCKETS,
    BY,
    BYPASSRLS,
    BYTEA,
    BYTES,
    CACHE,
    CALL,
    CALLED,
    CARDINALITY,
    CASCADE,
    CASCADED,
    CASE,
    CASES,
    CAST,
    CATALOG,
    CATALOG_SYNC,
    CATCH,
    CEIL,
    CEILING,
    CENTURY,
    CHAIN,
    CHANGE,
    CHANGE_TRACKING,
    CHANNEL,
    CHAR,
    CHARACTER,
    CHARACTERS,
    CHARACTER_LENGTH,
    CHARSET,
    CHAR_LENGTH,
    CHECK,
    CIRCLE,
    CLEAR,
    CLOB,
    CLONE,
    CLOSE,
    CLUSTER,
    CLUSTERED,
    CLUSTERING,
    COALESCE,
    COLLATE,
    COLLATION,
    COLLECT,
    COLLECTION,
    COLUMN,
    COLUMNS,
    COLUMNSTORE,
    COMMENT,
    COMMIT,
    COMMITTED,
    COMPATIBLE,
    COMPRESSION,
    COMPUTE,
    CONCURRENTLY,
    CONDITION,
    CONFLICT,
    CONNECT,
    CONNECTION,
    CONNECTOR,
    CONSTRAINT,
    CONTAINS,
    CONTINUE,
    CONVERT,
    COPY,
    COPY_OPTIONS,
    CORR,
    CORRESPONDING,
    COUNT,
    COVAR_POP,
    COVAR_SAMP,
    CREATE,
    CREATEDB,
    CREATEROLE,
    CREDENTIALS,
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
    DATABASES,
    DATA_RETENTION_TIME_IN_DAYS,
    DATE,
    DATE32,
    DATETIME,
    DATETIME64,
    DAY,
    DAYOFWEEK,
    DAYOFYEAR,
    DAYS,
    DCPROPERTIES,
    DEALLOCATE,
    DEC,
    DECADE,
    DECIMAL,
    DECLARE,
    DEDUPLICATE,
    DEFAULT,
    DEFAULT_DDL_COLLATION,
    DEFERRABLE,
    DEFERRED,
    DEFINE,
    DEFINED,
    DEFINER,
    DELAYED,
    DELETE,
    DELIMITED,
    DELIMITER,
    DELTA,
    DENSE_RANK,
    DEREF,
    DESC,
    DESCRIBE,
    DETACH,
    DETAIL,
    DETERMINISTIC,
    DIRECTORY,
    DISABLE,
    DISCARD,
    DISCONNECT,
    DISTINCT,
    DISTRIBUTE,
    DIV,
    DO,
    DOUBLE,
    DOW,
    DOY,
    DROP,
    DRY,
    DUPLICATE,
    DYNAMIC,
    EACH,
    ELEMENT,
    ELEMENTS,
    ELSE,
    EMPTY,
    ENABLE,
    ENABLE_SCHEMA_EVOLUTION,
    ENCODING,
    ENCRYPTION,
    END,
    END_EXEC = "END-EXEC",
    ENDPOINT,
    END_FRAME,
    END_PARTITION,
    ENFORCED,
    ENGINE,
    ENUM,
    ENUM16,
    ENUM8,
    EPHEMERAL,
    EPOCH,
    EQUALS,
    ERROR,
    ESCAPE,
    ESCAPED,
    ESTIMATE,
    EVENT,
    EVERY,
    EVOLVE,
    EXCEPT,
    EXCEPTION,
    EXCHANGE,
    EXCLUDE,
    EXCLUSIVE,
    EXEC,
    EXECUTE,
    EXECUTION,
    EXISTS,
    EXP,
    EXPANSION,
    EXPLAIN,
    EXPLICIT,
    EXPORT,
    EXTENDED,
    EXTENSION,
    EXTERNAL,
    EXTERNAL_VOLUME,
    EXTRACT,
    FAIL,
    FAILOVER,
    FALSE,
    FETCH,
    FIELDS,
    FILE,
    FILES,
    FILE_FORMAT,
    FILL,
    FILTER,
    FINAL,
    FIRST,
    FIRST_VALUE,
    FIXEDSTRING,
    FLOAT,
    FLOAT32,
    FLOAT4,
    FLOAT64,
    FLOAT8,
    FLOOR,
    FLUSH,
    FN,
    FOLLOWING,
    FOR,
    FORCE,
    FORCE_NOT_NULL,
    FORCE_NULL,
    FORCE_QUOTE,
    FOREIGN,
    FORMAT,
    FORMATTED,
    FORWARD,
    FRAME_ROW,
    FREE,
    FREEZE,
    FROM,
    FSCK,
    FULFILLMENT,
    FULL,
    FULLTEXT,
    FUNCTION,
    FUNCTIONS,
    FUSION,
    GENERAL,
    GENERATE,
    GENERATED,
    GEOGRAPHY,
    GET,
    GLOBAL,
    GRANT,
    GRANTED,
    GRANTS,
    GRAPHVIZ,
    GROUP,
    GROUPING,
    GROUPS,
    HASH,
    HAVING,
    HEADER,
    HEAP,
    HIGH_PRIORITY,
    HISTORY,
    HIVEVAR,
    HOLD,
    HOSTS,
    HOUR,
    HOURS,
    ICEBERG,
    ID,
    IDENTITY,
    IDENTITY_INSERT,
    IF,
    IGNORE,
    ILIKE,
    IMMEDIATE,
    IMMUTABLE,
    IMPORT,
    IMPORTED,
    IN,
    INCLUDE,
    INCLUDE_NULL_VALUES,
    INCREMENT,
    INDEX,
    INDICATOR,
    INHERIT,
    INITIALLY,
    INNER,
    INOUT,
    INPATH,
    INPUT,
    INPUTFORMAT,
    INSENSITIVE,
    INSERT,
    INSTALL,
    INSTEAD,
    INT,
    INT128,
    INT16,
    INT2,
    INT256,
    INT32,
    INT4,
    INT64,
    INT8,
    INTEGER,
    INTEGRATION,
    INTERPOLATE,
    INTERSECT,
    INTERSECTION,
    INTERVAL,
    INTO,
    INVOKER,
    IO,
    IS,
    ISODOW,
    ISOLATION,
    ISOWEEK,
    ISOYEAR,
    ITEMS,
    JAR,
    JOIN,
    JSON,
    JSONB,
    JSONFILE,
    JSON_TABLE,
    JULIAN,
    KEY,
    KEYS,
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
    LINE,
    LINES,
    LIST,
    LISTEN,
    LISTING,
    LN,
    LOAD,
    LOCAL,
    LOCALTIME,
    LOCALTIMESTAMP,
    LOCATION,
    LOCK,
    LOCKED,
    LOG,
    LOGIN,
    LOGS,
    LONGBLOB,
    LONGTEXT,
    LOWCARDINALITY,
    LOWER,
    LOW_PRIORITY,
    LS,
    LSEG,
    MACRO,
    MANAGE,
    MANAGED,
    MANAGEDLOCATION,
    MAP,
    MASKING,
    MATCH,
    MATCHED,
    MATCHES,
    MATCH_CONDITION,
    MATCH_RECOGNIZE,
    MATERIALIZE,
    MATERIALIZED,
    MAX,
    MAXVALUE,
    MAX_DATA_EXTENSION_TIME_IN_DAYS,
    MEASURES,
    MEDIUMBLOB,
    MEDIUMINT,
    MEDIUMTEXT,
    MEMBER,
    MERGE,
    METADATA,
    METHOD,
    METRIC,
    MICROSECOND,
    MICROSECONDS,
    MILLENIUM,
    MILLENNIUM,
    MILLISECOND,
    MILLISECONDS,
    MIN,
    MINUS,
    MINUTE,
    MINUTES,
    MINVALUE,
    MOD,
    MODE,
    MODIFIES,
    MODIFY,
    MODULE,
    MONITOR,
    MONTH,
    MONTHS,
    MSCK,
    MULTISET,
    MUTATION,
    NAME,
    NANOSECOND,
    NANOSECONDS,
    NATIONAL,
    NATURAL,
    NCHAR,
    NCLOB,
    NESTED,
    NETWORK,
    NEW,
    NEXT,
    NFC,
    NFD,
    NFKC,
    NFKD,
    NO,
    NOBYPASSRLS,
    NOCREATEDB,
    NOCREATEROLE,
    NOINHERIT,
    NOLOGIN,
    NONE,
    NOORDER,
    NOREPLICATION,
    NORMALIZE,
    NORMALIZED,
    NOSCAN,
    NOSUPERUSER,
    NOT,
    NOTHING,
    NOTIFY,
    NOWAIT,
    NO_WRITE_TO_BINLOG,
    NTH_VALUE,
    NTILE,
    NULL,
    NULLABLE,
    NULLIF,
    NULLS,
    NUMERIC,
    NVARCHAR,
    OBJECT,
    OBJECTS,
    OCCURRENCES_REGEX,
    OCTETS,
    OCTET_LENGTH,
    OF,
    OFF,
    OFFSET,
    OFFSETS,
    OLD,
    OMIT,
    ON,
    ONE,
    ONLY,
    OPEN,
    OPENJSON,
    OPERATE,
    OPERATOR,
    OPTIMIZATION,
    OPTIMIZE,
    OPTIMIZED,
    OPTIMIZER_COSTS,
    OPTION,
    OPTIONS,
    OR,
    ORC,
    ORDER,
    ORDINALITY,
    ORGANIZATION,
    OUT,
    OUTER,
    OUTPUTFORMAT,
    OVER,
    OVERFLOW,
    OVERLAPS,
    OVERLAY,
    OVERRIDE,
    OVERWRITE,
    OWNED,
    OWNER,
    OWNERSHIP,
    PACKAGE,
    PACKAGES,
    PARALLEL,
    PARAMETER,
    PARQUET,
    PART,
    PARTITION,
    PARTITIONED,
    PARTITIONS,
    PASSWORD,
    PAST,
    PATH,
    PATTERN,
    PER,
    PERCENT,
    PERCENTILE_CONT,
    PERCENTILE_DISC,
    PERCENT_RANK,
    PERIOD,
    PERMISSIVE,
    PERSISTENT,
    PIVOT,
    PLACING,
    PLAN,
    PLANS,
    POINT,
    POLICY,
    POLYGON,
    POOL,
    PORTION,
    POSITION,
    POSITION_REGEX,
    POWER,
    PRAGMA,
    PRECEDES,
    PRECEDING,
    PRECISION,
    PREPARE,
    PRESERVE,
    PREWHERE,
    PRIMARY,
    PRIOR,
    PRIVILEGES,
    PROCEDURE,
    PROFILE,
    PROGRAM,
    PROJECTION,
    PUBLIC,
    PURCHASE,
    PURGE,
    QUALIFY,
    QUARTER,
    QUERY,
    QUOTE,
    RAISERROR,
    RANGE,
    RANK,
    RAW,
    RCFILE,
    READ,
    READS,
    READ_ONLY,
    REAL,
    RECLUSTER,
    RECURSIVE,
    REF,
    REFERENCES,
    REFERENCING,
    REGCLASS,
    REGEXP,
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
    RELAY,
    RELEASE,
    RELEASES,
    REMOTE,
    REMOVE,
    RENAME,
    REORG,
    REPAIR,
    REPEATABLE,
    REPLACE,
    REPLICA,
    REPLICATE,
    REPLICATION,
    RESET,
    RESOLVE,
    RESPECT,
    RESTART,
    RESTRICT,
    RESTRICTED,
    RESTRICTIONS,
    RESTRICTIVE,
    RESULT,
    RESULTSET,
    RESUME,
    RETAIN,
    RETURN,
    RETURNING,
    RETURNS,
    REVOKE,
    RIGHT,
    RLIKE,
    RM,
    ROLE,
    ROLES,
    ROLLBACK,
    ROLLUP,
    ROOT,
    ROW,
    ROWID,
    ROWS,
    ROW_NUMBER,
    RULE,
    RUN,
    SAFE,
    SAFE_CAST,
    SAMPLE,
    SAVEPOINT,
    SCHEMA,
    SCHEMAS,
    SCOPE,
    SCROLL,
    SEARCH,
    SECOND,
    SECONDARY,
    SECONDS,
    SECRET,
    SECURITY,
    SEED,
    SELECT,
    SEMI,
    SENSITIVE,
    SEPARATOR,
    SEQUENCE,
    SEQUENCEFILE,
    SEQUENCES,
    SERDE,
    SERDEPROPERTIES,
    SERIALIZABLE,
    SERVICE,
    SESSION,
    SESSION_USER,
    SET,
    SETERROR,
    SETS,
    SETTINGS,
    SHARE,
    SHARING,
    SHOW,
    SIGNED,
    SIMILAR,
    SKIP,
    SLOW,
    SMALLINT,
    SNAPSHOT,
    SOME,
    SORT,
    SORTED,
    SOURCE,
    SPATIAL,
    SPECIFIC,
    SPECIFICTYPE,
    SQL,
    SQLEXCEPTION,
    SQLSTATE,
    SQLWARNING,
    SQRT,
    STABLE,
    STAGE,
    START,
    STARTS,
    STATEMENT,
    STATIC,
    STATISTICS,
    STATUS,
    STDDEV_POP,
    STDDEV_SAMP,
    STDIN,
    STDOUT,
    STEP,
    STORAGE_INTEGRATION,
    STORAGE_SERIALIZATION_POLICY,
    STORED,
    STRICT,
    STRING,
    STRUCT,
    SUBMULTISET,
    SUBSTRING,
    SUBSTRING_REGEX,
    SUCCEEDS,
    SUM,
    SUPER,
    SUPERUSER,
    SUPPORT,
    SUSPEND,
    SWAP,
    SYMMETRIC,
    SYNC,
    SYSTEM,
    SYSTEM_TIME,
    SYSTEM_USER,
    TABLE,
    TABLES,
    TABLESAMPLE,
    TAG,
    TARGET,
    TASK,
    TBLPROPERTIES,
    TEMP,
    TEMPORARY,
    TEMPTABLE,
    TERMINATED,
    TERSE,
    TEXT,
    TEXTFILE,
    THEN,
    TIES,
    TIME,
    TIMESTAMP,
    TIMESTAMPTZ,
    TIMETZ,
    TIMEZONE,
    TIMEZONE_ABBR,
    TIMEZONE_HOUR,
    TIMEZONE_MINUTE,
    TIMEZONE_REGION,
    TINYBLOB,
    TINYINT,
    TINYTEXT,
    TO,
    TOP,
    TOTALS,
    TRACE,
    TRAILING,
    TRANSACTION,
    TRANSIENT,
    TRANSLATE,
    TRANSLATE_REGEX,
    TRANSLATION,
    TREAT,
    TRIGGER,
    TRIM,
    TRIM_ARRAY,
    TRUE,
    TRUNCATE,
    TRY,
    TRY_CAST,
    TRY_CONVERT,
    TUPLE,
    TYPE,
    UESCAPE,
    UINT128,
    UINT16,
    UINT256,
    UINT32,
    UINT64,
    UINT8,
    UNBOUNDED,
    UNCACHE,
    UNCOMMITTED,
    UNDEFINED,
    UNFREEZE,
    UNION,
    UNIQUE,
    UNKNOWN,
    UNLISTEN,
    UNLOAD,
    UNLOCK,
    UNLOGGED,
    UNMATCHED,
    UNNEST,
    UNPIVOT,
    UNSAFE,
    UNSET,
    UNSIGNED,
    UNTIL,
    UPDATE,
    UPPER,
    URL,
    USAGE,
    USE,
    USER,
    USER_RESOURCES,
    USING,
    UUID,
    VACUUM,
    VALID,
    VALIDATION_MODE,
    VALUE,
    VALUES,
    VALUE_OF,
    VARBINARY,
    VARBIT,
    VARCHAR,
    VARIABLES,
    VARYING,
    VAR_POP,
    VAR_SAMP,
    VERBOSE,
    VERSION,
    VERSIONING,
    VERSIONS,
    VIEW,
    VIEWS,
    VIRTUAL,
    VOLATILE,
    VOLUME,
    WAREHOUSE,
    WAREHOUSES,
    WEEK,
    WEEKS,
    WHEN,
    WHENEVER,
    WHERE,
    WIDTH_BUCKET,
    WINDOW,
    WITH,
    WITHIN,
    WITHOUT,
    WITHOUT_ARRAY_WRAPPER,
    WORK,
    WRITE,
    XML,
    XOR,
    YEAR,
    YEARS,
    ZONE,
    ZORDER
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
    Keyword::PIVOT,
    Keyword::UNPIVOT,
    Keyword::TOP,
    Keyword::LATERAL,
    Keyword::VIEW,
    Keyword::LIMIT,
    Keyword::OFFSET,
    Keyword::FETCH,
    Keyword::UNION,
    Keyword::EXCEPT,
    Keyword::INTERSECT,
    Keyword::MINUS,
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
    Keyword::GLOBAL,
    Keyword::ANTI,
    Keyword::SEMI,
    Keyword::RETURNING,
    Keyword::ASOF,
    Keyword::MATCH_CONDITION,
    // for MSSQL-specific OUTER APPLY (seems reserved in most dialects)
    Keyword::OUTER,
    Keyword::SET,
    Keyword::QUALIFY,
    Keyword::WINDOW,
    Keyword::END,
    Keyword::FOR,
    // for MYSQL PARTITION SELECTION
    Keyword::PARTITION,
    // for Clickhouse PREWHERE
    Keyword::PREWHERE,
    Keyword::SETTINGS,
    Keyword::FORMAT,
    // for Snowflake START WITH .. CONNECT BY
    Keyword::START,
    Keyword::CONNECT,
    // Reserved for snowflake MATCH_RECOGNIZE
    Keyword::MATCH_RECOGNIZE,
    // Reserved for Snowflake table sample
    Keyword::SAMPLE,
    Keyword::TABLESAMPLE,
    Keyword::FROM,
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
    Keyword::MINUS,
    Keyword::CLUSTER,
    Keyword::DISTRIBUTE,
    Keyword::RETURNING,
    // Reserved only as a column alias in the `SELECT` clause
    Keyword::FROM,
    Keyword::INTO,
    Keyword::END,
];

// Global list of reserved keywords alloweed after FROM.
// Parser should call Dialect::get_reserved_keyword_after_from
// to allow for each dialect to customize the list.
pub const RESERVED_FOR_TABLE_FACTOR: &[Keyword] = &[
    Keyword::INTO,
    Keyword::LIMIT,
    Keyword::HAVING,
    Keyword::WHERE,
];

/// Global list of reserved keywords that cannot be parsed as identifiers
/// without special handling like quoting. Parser should call `Dialect::is_reserved_for_identifier`
/// to allow for each dialect to customize the list.
pub const RESERVED_FOR_IDENTIFIER: &[Keyword] = &[
    Keyword::EXISTS,
    Keyword::INTERVAL,
    Keyword::STRUCT,
    Keyword::TRIM,
];

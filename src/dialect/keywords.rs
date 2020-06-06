/// make a listing of keywords
/// with static str and their stringified value
macro_rules! keyword {
    ($($ident:ident),*) => {
        $(pub static $ident: &'static str = stringify!($ident);)*
    }
}

// enumerate all the keywords here for all dialects to support in this project
keyword!(
    ABS,
    ADD,
    ASC,
    ALL,
    ALLOCATE,
    ALTER,
    AND,
    ANY,
    ARE,
    ARRAY,
    ARRAY_AGG,
    ARRAY_MAX_CARDINALITY,
    AS,
    ASENSITIVE,
    ASYMMETRIC,
    AT,
    ATOMIC,
    AUTHORIZATION,
    AVG,
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
    BYTEA,
    CALL,
    CALLED,
    CARDINALITY,
    CASCADED,
    CASE,
    CAST,
    CEIL,
    CEILING,
    CHAR,
    CHAR_LENGTH,
    CHARACTER,
    CHARACTER_LENGTH,
    CHECK,
    CLOB,
    CLOSE,
    COALESCE,
    COLLATE,
    COLLECT,
    COLUMN,
    COMMIT,
    CONDITION,
    CONNECT,
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
    DATE,
    DAY,
    DEALLOCATE,
    DEC,
    DECIMAL,
    DECLARE,
    DEFAULT,
    DELETE,
    DENSE_RANK,
    DEREF,
    DESC,
    DESCRIBE,
    DETERMINISTIC,
    DISCONNECT,
    DISTINCT,
    DOUBLE,
    DROP,
    DYNAMIC,
    EACH,
    ELEMENT,
    ELSE,
    END,
    END_FRAME,
    END_PARTITION,
    EQUALS,
    ESCAPE,
    EVERY,
    EXCEPT,
    EXEC,
    EXECUTE,
    EXISTS,
    EXP,
    EXTERNAL,
    EXTRACT,
    FALSE,
    FETCH,
    FILTER,
    FIRST,
    FIRST_VALUE,
    FLOAT,
    FLOOR,
    FOR,
    FOREIGN,
    FRAME_ROW,
    FREE,
    FROM,
    FULL,
    FUNCTION,
    FUSION,
    GET,
    GLOBAL,
    GRANT,
    GROUP,
    GROUPING,
    GROUPS,
    HAVING,
    HEADER,
    HOLD,
    HOUR,
    IDENTITY,
    IN,
    INDICATOR,
    INNER,
    INOUT,
    INSENSITIVE,
    INSERT,
    INT,
    INTEGER,
    INTERSECT,
    INTERSECTION,
    INTERVAL,
    INTO,
    IS,
    JOIN,
    KEY,
    LAG,
    LANGUAGE,
    LARGE,
    LAST,
    LAST_VALUE,
    LATERAL,
    LEAD,
    LEADING,
    LEFT,
    LIKE,
    LIKE_REGEX,
    LIMIT,
    LN,
    LOCAL,
    LOCALTIME,
    LOCALTIMESTAMP,
    LOCATION,
    LOWER,
    MATCH,
    MAX,
    MEMBER,
    MERGE,
    METHOD,
    MIN,
    MINUTE,
    MOD,
    MODIFIES,
    MODULE,
    MONTH,
    MULTISET,
    NATIONAL,
    NATURAL,
    NCHAR,
    NCLOB,
    NEW,
    NO,
    NONE,
    NORMALIZE,
    NOT,
    NTH_VALUE,
    NTILE,
    NULL,
    NULLS,
    NULLIF,
    NUMERIC,
    OBJECT,
    OCTET_LENGTH,
    OCCURRENCES_REGEX,
    OF,
    OFFSET,
    OLD,
    ON,
    ONLY,
    OPEN,
    OR,
    ORDER,
    OUT,
    OUTER,
    OVER,
    OVERLAPS,
    OVERLAY,
    PARAMETER,
    PARTITION,
    PARQUET,
    PERCENT,
    PERCENT_RANK,
    PERCENTILE_CONT,
    PERCENTILE_DISC,
    PERIOD,
    PORTION,
    POSITION,
    POSITION_REGEX,
    POWER,
    PRECEDES,
    PRECISION,
    PREPARE,
    PRIMARY,
    PROCEDURE,
    RANGE,
    RANK,
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
    RELEASE,
    RESULT,
    RETURN,
    RETURNS,
    REVOKE,
    RIGHT,
    ROLLBACK,
    ROLLUP,
    ROW,
    ROW_NUMBER,
    ROWS,
    SAVEPOINT,
    SCOPE,
    SCROLL,
    SEARCH,
    SECOND,
    SELECT,
    SENSITIVE,
    SESSION_USER,
    SET,
    SIMILAR,
    SMALLINT,
    SOME,
    SPECIFIC,
    SPECIFICTYPE,
    SQL,
    SQLEXCEPTION,
    SQLSTATE,
    SQLWARNING,
    SQRT,
    START,
    STATIC,
    STDDEV_POP,
    STDDEV_SAMP,
    STDIN,
    STORED,
    SUBMULTISET,
    SUBSTRING,
    SUBSTRING_REGEX,
    SUCCEEDS,
    SUM,
    SYMMETRIC,
    SYSTEM,
    SYSTEM_TIME,
    SYSTEM_USER,
    TABLE,
    TABLESAMPLE,
    TEXT,
    THEN,
    TIME,
    TIMESTAMP,
    TIMEZONE_HOUR,
    TIMEZONE_MINUTE,
    TO,
    TRAILING,
    TRANSLATE,
    TRANSLATE_REGEX,
    TRANSLATION,
    TREAT,
    TRIGGER,
    TRUNCATE,
    TRIM,
    TRIM_ARRAY,
    TRUE,
    UESCAPE,
    UNION,
    UNIQUE,
    UNKNOWN,
    UNNEST,
    UPDATE,
    UPPER,
    USER,
    USING,
    UUID,
    VALUE,
    VALUES,
    VALUE_OF,
    VAR_POP,
    VAR_SAMP,
    VARBINARY,
    VARCHAR,
    VARYING,
    VERSIONING,
    WHEN,
    WHENEVER,
    WHERE,
    WIDTH_BUCKET,
    WINDOW,
    WITH,
    WITHIN,
    WITHOUT,
    YEAR,
    ZONE
);

/// special case of keyword where the it is an invalid identifier
pub static END_EXEC: &'static str = "END-EXEC";

use dialect::Dialect;

use dialect::keywords::*;
pub struct GenericSqlDialect {}

impl Dialect for GenericSqlDialect {
    fn keywords(&self) -> Vec<&'static str> {
        return vec![
            SELECT, FROM, WHERE, LIMIT, ORDER, GROUP, BY, HAVING, UNION, ALL, INSERT, INTO, UPDATE,
            DELETE, IN, IS, NULL, SET, CREATE, EXTERNAL, TABLE, ASC, DESC, AND, OR, NOT, AS,
            STORED, CSV, PARQUET, LOCATION, WITH, WITHOUT, HEADER, ROW, // SQL types
            CHAR, CHARACTER, VARYING, LARGE, OBJECT, VARCHAR, CLOB, BINARY, VARBINARY, BLOB, FLOAT,
            REAL, DOUBLE, PRECISION, INT, INTEGER, SMALLINT, BIGINT, NUMERIC, DECIMAL, DEC,
            BOOLEAN, DATE, TIME, TIMESTAMP, CASE, WHEN, THEN, ELSE, END, JOIN, LEFT, RIGHT, FULL,
            CROSS, OUTER, INNER, NATURAL, ON, USING,
            BOOLEAN, DATE, TIME, TIMESTAMP, CASE, WHEN, THEN, ELSE, END, LIKE,
        ];
    }

    fn is_identifier_start(&self, ch: char) -> bool {
        (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '@'
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        (ch >= 'a' && ch <= 'z')
            || (ch >= 'A' && ch <= 'Z')
            || (ch >= '0' && ch <= '9')
            || ch == '@'
            || ch == '_'
    }
}

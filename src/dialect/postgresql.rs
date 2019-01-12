use dialect::Dialect;

use dialect::keywords::*;

pub struct PostgreSqlDialect {}

impl Dialect for PostgreSqlDialect {
    fn keywords(&self) -> Vec<&'static str> {
        return vec![
            ALTER, ONLY, SELECT, FROM, WHERE, LIMIT, ORDER, GROUP, BY, HAVING, UNION, ALL, INSERT,
            INTO, UPDATE, DELETE, IN, IS, NULL, SET, CREATE, EXTERNAL, TABLE, ASC, DESC, AND, OR,
            NOT, AS, STORED, CSV, WITH, WITHOUT, ROW, // SQL types
            CHAR, CHARACTER, VARYING, LARGE, VARCHAR, CLOB, BINARY, VARBINARY, BLOB, FLOAT, REAL,
            DOUBLE, PRECISION, INT, INTEGER, SMALLINT, BIGINT, NUMERIC, DECIMAL, DEC, BOOLEAN,
            DATE, TIME, TIMESTAMP, VALUES, DEFAULT, ZONE, REGCLASS, TEXT, BYTEA, TRUE, FALSE, COPY,
            STDIN, PRIMARY, KEY, UNIQUE, UUID, ADD, CONSTRAINT, FOREIGN, REFERENCES, CASE, WHEN,
            THEN, ELSE, END, JOIN, LEFT, RIGHT, FULL, CROSS, OUTER, INNER, NATURAL, ON, USING,
            LIKE, CAST,
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

use crate::dialect::Dialect;

#[derive(Debug)]
pub struct PostgreSqlDialect {}

impl Dialect for PostgreSqlDialect {
    fn is_identifier_start(&self, ch: char) -> bool {
        // See https://www.postgresql.org/docs/11/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS
        // We don't yet support identifiers beginning with "letters with
        // diacritical marks and non-Latin letters"
        (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_'
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        (ch >= 'a' && ch <= 'z')
            || (ch >= 'A' && ch <= 'Z')
            || (ch >= '0' && ch <= '9')
            || ch == '$'
            || ch == '_'
    }
}

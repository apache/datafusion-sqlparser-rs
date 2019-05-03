use crate::dialect::Dialect;

#[derive(Debug)]
pub struct MsSqlDialect {}

impl Dialect for MsSqlDialect {
    fn is_identifier_start(&self, ch: char) -> bool {
        // See https://docs.microsoft.com/en-us/sql/relational-databases/databases/database-identifiers?view=sql-server-2017#rules-for-regular-identifiers
        // We don't support non-latin "letters" currently.
        (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_' || ch == '#' || ch == '@'
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        (ch >= 'a' && ch <= 'z')
            || (ch >= 'A' && ch <= 'Z')
            || (ch >= '0' && ch <= '9')
            || ch == '@'
            || ch == '$'
            || ch == '#'
            || ch == '_'
    }
}

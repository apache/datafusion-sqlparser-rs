mod ansi_sql;
mod generic_sql;
pub mod keywords;
mod postgresql;

pub use self::ansi_sql::AnsiSqlDialect;
pub use self::generic_sql::GenericSqlDialect;
pub use self::postgresql::PostgreSqlDialect;

pub trait Dialect {
    /// Determine if a character is a valid identifier start character
    fn is_identifier_start(&self, ch: char) -> bool;
    /// Determine if a character is a valid identifier character
    fn is_identifier_part(&self, ch: char) -> bool;
}

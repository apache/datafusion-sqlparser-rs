use crate::dialect::Dialect;

#[derive(Debug, Default)]
pub struct SnowflakeDialect;

impl Dialect for SnowflakeDialect {
    //Revisit: currently copied from Genric dialect
    fn is_identifier_start(&self, ch: char) -> bool {
        (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_' || ch == '#' || ch == '@'
    }

    //Revisit: currently copied from Genric dialect
    fn is_identifier_part(&self, ch: char) -> bool {
        (ch >= 'a' && ch <= 'z')
            || (ch >= 'A' && ch <= 'Z')
            || (ch >= '0' && ch <= '9')
            || ch == '@'
            || ch == '$'
            || ch == '#'
            || ch == '_'
    }

    fn alllow_single_table_in_parenthesis(&self) -> bool {
        true
    }
}

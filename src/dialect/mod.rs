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

mod ansi;
mod generic;
pub mod keywords;
mod mssql;
mod mysql;
mod postgresql;
mod snowflake;
mod sqlite;

use std::any::{Any, TypeId};
use std::fmt::Debug;

pub use self::ansi::AnsiDialect;
pub use self::generic::GenericDialect;
pub use self::mssql::MsSqlDialect;
pub use self::mysql::MySqlDialect;
pub use self::postgresql::PostgreSqlDialect;
pub use self::snowflake::SnowflakeDialect;
pub use self::sqlite::SQLiteDialect;

/// `dialect_of!(parser is SQLiteDialect |  GenericDialect)` evaluates
/// to `true` iff `parser.dialect` is one of the `Dialect`s specified.
macro_rules! dialect_of {
    ( $parsed_dialect: ident is $($dialect_type: ty)|+ ) => {
        ($($parsed_dialect.dialect.is::<$dialect_type>())||+)
    };
}

pub trait Dialect: Debug + Any {
    /// Determine if a character starts a quoted identifier. The default
    /// implementation, accepting "double quoted" ids is both ANSI-compliant
    /// and appropriate for most dialects (with the notable exception of
    /// MySQL, MS SQL, and sqlite). You can accept one of characters listed
    /// in `Word::matching_end_quote` here
    fn is_delimited_identifier_start(&self, ch: char) -> bool {
        ch == '"'
    }
    /// Determine if a character is a valid start character for an unquoted identifier
    fn is_identifier_start(&self, ch: char) -> bool;
    /// Determine if a character is a valid unquoted identifier character
    fn is_identifier_part(&self, ch: char) -> bool;
}

impl dyn Dialect {
    #[inline]
    pub fn is<T: Dialect>(&self) -> bool {
        // borrowed from `Any` implementation
        TypeId::of::<T>() == self.type_id()
    }
}

#[cfg(test)]
mod tests {
    use super::ansi::AnsiDialect;
    use super::generic::GenericDialect;
    use super::*;

    struct DialectHolder<'a> {
        dialect: &'a dyn Dialect,
    }

    #[test]
    fn test_is_dialect() {
        let generic_dialect: &dyn Dialect = &GenericDialect {};
        let ansi_dialect: &dyn Dialect = &AnsiDialect {};

        let generic_holder = DialectHolder {
            dialect: generic_dialect,
        };
        let ansi_holder = DialectHolder {
            dialect: ansi_dialect,
        };

        assert_eq!(
            dialect_of!(generic_holder is GenericDialect |  AnsiDialect),
            true
        );
        assert_eq!(dialect_of!(generic_holder is  AnsiDialect), false);

        assert_eq!(dialect_of!(ansi_holder is  AnsiDialect), true);
        assert_eq!(
            dialect_of!(ansi_holder is  GenericDialect | AnsiDialect),
            true
        );
        assert_eq!(
            dialect_of!(ansi_holder is  GenericDialect | MsSqlDialect),
            false
        );
    }
}

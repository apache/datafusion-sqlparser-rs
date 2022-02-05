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
mod clickhouse;
mod generic;
mod hive;
mod mssql;
mod mysql;
mod postgresql;
mod snowflake;
mod sqlite;

use core::any::{Any, TypeId};
use core::fmt::Debug;

pub use self::ansi::AnsiDialect;
pub use self::clickhouse::ClickHouseDialect;
pub use self::generic::GenericDialect;
pub use self::hive::HiveDialect;
pub use self::mssql::MsSqlDialect;
pub use self::mysql::MySqlDialect;
pub use self::postgresql::PostgreSqlDialect;
pub use self::snowflake::SnowflakeDialect;
pub use self::sqlite::SQLiteDialect;
pub use crate::keywords;

pub trait Dialect: Debug + Any {
    /// Determine if a character starts a quoted identifier. The default
    /// implementation, accepting "double quoted" ids is both ANSI-compliant
    /// and appropriate for most dialects (with the notable exception of
    /// MySQL, MS SQL, and sqlite). You can accept one of characters listed
    /// in `Word::matching_end_quote` here
    fn is_delimited_identifier_start(ch: char) -> bool {
        ch == '"'
    }
    /// Determine if a character is a valid start character for an unquoted identifier
    fn is_identifier_start(ch: char) -> bool;
    /// Determine if a character is a valid unquoted identifier character
    fn is_identifier_part(ch: char) -> bool;

    fn is<T: Dialect>() -> bool {
        TypeId::of::<Self>() == TypeId::of::<T>()
    }
}

// Copyright 2018 Grove Enterprises LLC
//
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

//! Support for custom dialects

pub use self::ansi_sql::AnsiSqlDialect;
pub use self::generic_sql::GenericSqlDialect;
pub use self::postgresql::PostgreSqlDialect;

mod ansi_sql;
mod generic_sql;
mod postgresql;

pub mod keywords;

pub trait Dialect {
    /// Get a list of keywords for this dialect
    fn keywords(&self) -> Vec<&'static str>;
    /// Determine if a character is a valid identifier start character
    fn is_identifier_start(&self, ch: char) -> bool;
    /// Determine if a character is a valid identifier character
    fn is_identifier_part(&self, ch: char) -> bool;
}



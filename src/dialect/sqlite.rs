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

use crate::dialect::Dialect;

/// For resilience when confronted with historical SQL statements, SQLite will sometimes bend the quoting rules above:
///     * If a keyword in single quotes (ex: 'key' or 'glob') is used in a context where an identifier is allowed but where a string literal is not allowed, then the token is understood to be an identifier instead of a string literal.
///     * If a keyword in double quotes (ex: "key" or "glob") is used in a context where it cannot be resolved to an identifier but where a string literal is allowed, then the token is understood to be a string literal instead of an identifier.
#[derive(Debug)]
pub struct SQLiteDialect {}

impl Dialect for SQLiteDialect {
    // see https://www.sqlite.org/lang_keywords.html
    fn is_delimited_identifier_start(&self, ch: char) -> bool {
        ch == '`' || ch == '\'' || ch == '"' || ch == '['
    }

    fn is_identifier_start(&self, ch: char) -> bool {
        // See https://www.sqlite.org/draft/tokenreq.html
        (ch >= 'a' && ch <= 'z')
            || (ch >= 'A' && ch <= 'Z')
            || ch == '_'
            || ch == '$'
            || (ch >= '\u{007f}' && ch <= '\u{ffff}')
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        self.is_identifier_start(ch) || (ch >= '0' && ch <= '9')
    }
}

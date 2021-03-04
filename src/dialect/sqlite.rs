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

#[derive(Debug)]
pub struct SQLiteDialect {}

impl Dialect for SQLiteDialect {
    // see https://www.sqlite.org/lang_keywords.html
    // parse `...`, [...] and "..." as identifier
    // TODO: support depending on the context tread '...' as identifier too.
    fn is_delimited_identifier_start(&self, ch: char) -> bool {
        ch == '`' || ch == '"' || ch == '['
    }

    fn is_identifier_start(&self, ch: char) -> bool {
        // See https://www.sqlite.org/draft/tokenreq.html
        ('a'..='z').contains(&ch)
            || ('A'..='Z').contains(&ch)
            || ch == '_'
            || ch == '$'
            || ('\u{007f}'..='\u{ffff}').contains(&ch)
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        self.is_identifier_start(ch) || ('0'..='9').contains(&ch)
    }
}

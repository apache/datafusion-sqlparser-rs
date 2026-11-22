// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! SQLite spelling for the expression shape target.

/// Lowercases `TRUE` and `FALSE` outside quotes, since SQLite reuses a constant only when its token text matches.
pub fn fold_booleans(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len());
    let mut chars = sql.char_indices().peekable();
    while let Some((start, c)) = chars.next() {
        let close = match c {
            '\'' | '"' | '`' => Some(c),
            '[' => Some(']'),
            _ => None,
        };
        let mut end = start + c.len_utf8();
        if let Some(close) = close {
            for (i, d) in chars.by_ref() {
                end = i + d.len_utf8();
                if d == close {
                    break;
                }
            }
        } else if c.is_alphanumeric() || c == '_' {
            while let Some(&(i, d)) = chars.peek() {
                if !(d.is_alphanumeric() || d == '_' || d == '$') {
                    break;
                }
                end = i + d.len_utf8();
                chars.next();
            }
        }
        let token = &sql[start..end];
        if token.eq_ignore_ascii_case("true") || token.eq_ignore_ascii_case("false") {
            out.push_str(&token.to_ascii_lowercase());
        } else {
            out.push_str(token);
        }
    }
    out
}

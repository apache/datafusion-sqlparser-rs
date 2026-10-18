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

#![no_main]

use libfuzzer_sys::fuzz_target;
use libsqlite3_sys as ffi;
use sqlparser::dialect::SQLiteDialect;
use sqlparser::parser::{Parser, ParserError};
use std::ffi::{c_int, CStr};
use std::ptr;

/// Errors SQLite raises while resolving names, after the grammar accepted the statement.
const RESOLUTION_ERRORS: &[&[u8]] = &[
    b"no such table: ",
    b"no such column: ",
    b"no such function: ",
    b"no such index: ",
    b"no such collation sequence: ",
    b"no such module: ",
    b"no such window: ",
    b"unknown database ",
    b"wrong number of arguments to function ",
    b"ambiguous column name: ",
    b"misuse of aggregate",
    b"misuse of window function ",
    b"no tables specified",
];

struct Db(*mut ffi::sqlite3);

impl Db {
    fn open() -> Self {
        let mut db = ptr::null_mut();
        // SAFETY: the filename is NUL terminated and `db` is a valid out pointer.
        let rc = unsafe {
            ffi::sqlite3_open_v2(
                c":memory:".as_ptr(),
                &mut db,
                ffi::SQLITE_OPEN_READWRITE | ffi::SQLITE_OPEN_CREATE,
                ptr::null(),
            )
        };
        assert_eq!(
            rc,
            ffi::SQLITE_OK,
            "cannot open an in-memory SQLite database"
        );
        Self(db)
    }

    /// Compiles, without running, the first statement of `sql` and returns SQLite's error message.
    fn prepare_error(&self, sql: &[u8]) -> Option<Vec<u8>> {
        let len = c_int::try_from(sql.len()).expect("fuzz input fits a c_int");
        let mut stmt = ptr::null_mut();
        // SAFETY: SQLite reads at most `len` bytes of `sql`, and `stmt` is finalized before return.
        let rc = unsafe {
            let rc = ffi::sqlite3_prepare_v2(
                self.0,
                sql.as_ptr().cast(),
                len,
                &mut stmt,
                ptr::null_mut(),
            );
            ffi::sqlite3_finalize(stmt);
            rc
        };
        if rc == ffi::SQLITE_OK {
            return None;
        }
        // SAFETY: sqlite3_errmsg never returns NULL, and the message stays valid until the next call on this handle.
        let message = unsafe { CStr::from_ptr(ffi::sqlite3_errmsg(self.0)) };
        Some(message.to_bytes().to_vec())
    }

    fn accepts(&self, statement: &[u8]) -> bool {
        let Some(error) = self.prepare_error(statement) else {
            return true;
        };
        if !RESOLUTION_ERRORS.iter().any(|e| error.starts_with(e)) {
            return false;
        }
        // An error raised mid-statement leaves the tail unparsed and survives a trailing syntax error.
        let mut probe = statement.strip_suffix(b";").unwrap_or(statement).to_vec();
        probe.extend_from_slice(b"\n)");
        self.prepare_error(&probe).as_deref() == Some(br#"near ")": syntax error"#)
    }
}

impl Drop for Db {
    fn drop(&mut self) {
        // SAFETY: every statement is finalized, so the handle closes.
        unsafe { ffi::sqlite3_close(self.0) };
    }
}

/// Splits `sql` into statements the way the SQLite shell does, each keeping its `;`.
fn statements(sql: &str) -> Vec<&str> {
    let mut statements = Vec::new();
    let mut prefix = Vec::with_capacity(sql.len() + 1);
    let mut start = 0;
    for (end, _) in sql.match_indices(';') {
        prefix.clear();
        prefix.extend_from_slice(&sql.as_bytes()[start..=end]);
        prefix.push(0);
        // SAFETY: `prefix` ends with its only NUL because the caller rejects inputs holding one.
        if unsafe { ffi::sqlite3_complete(prefix.as_ptr().cast()) } != 0 {
            statements.push(&sql[start..=end]);
            start = end + 1;
        }
    }
    statements.push(&sql[start..]);
    statements
}

fuzz_target!(|sql: &str| {
    // SQLite stops reading at a NUL, sqlparser does not.
    if sql.contains('\0') {
        return;
    }
    let db = Db::open();
    if !statements(sql).iter().all(|s| db.accepts(s.as_bytes())) {
        return;
    }
    match Parser::parse_sql(&SQLiteDialect {}, sql) {
        Ok(_) | Err(ParserError::RecursionLimitExceeded) => {}
        Err(err) => {
            // SAFETY: sqlite3_libversion returns a static NUL terminated string.
            let version = unsafe { CStr::from_ptr(ffi::sqlite3_libversion()) };
            panic!(
                "SQLite {version:?} accepts SQL the SQLite dialect rejects\n  sql: {sql:?}\n  error: {err}"
            )
        }
    }
});

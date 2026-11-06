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

//! Checks that sqlparser groups SQLite operators the way SQLite does.
//!
//! sqlparser's tree is printed with every compound expression parenthesized, and SQLite must
//! compile that text to the same bytecode as the original.

#![no_main]

mod expression;

use expression::{parenthesize, Chain, Grammar};
use libfuzzer_sys::fuzz_target;
use libsqlite3_sys as ffi;
use sqlparser::dialect::SQLiteDialect;
use sqlparser::parser::Parser;
use std::ffi::{c_int, CStr};
use std::ptr;

const SQLITE: Grammar = Grammar {
    atoms: &[
        "1",
        "2",
        "3",
        "0",
        "2.5",
        "1e3",
        "'a'",
        "NULL",
        "TRUE",
        "FALSE",
        "a",
        "t.b",
        "x'01'",
        "?1",
        "CURRENT_DATE",
        "9223372036854775808",
    ],
    prefix: &["-", "+", "~", "NOT"],
    infix: &[
        "+",
        "-",
        "*",
        "/",
        "%",
        "||",
        "&",
        "|",
        "<<",
        ">>",
        "=",
        "==",
        "<>",
        "!=",
        "<",
        ">",
        "<=",
        ">=",
        "AND",
        "OR",
        "IS",
        "IS NOT",
        "IS DISTINCT FROM",
        "IS NOT DISTINCT FROM",
        "LIKE",
        "NOT LIKE",
        "GLOB",
        "NOT GLOB",
        "ESCAPE",
        "BETWEEN",
        "NOT BETWEEN",
        "IN",
        "NOT IN",
        "->",
        "->>",
    ],
    postfix: &[
        "ISNULL",
        "NOTNULL",
        "NOT NULL",
        "IS NULL",
        "IS NOT NULL",
        "COLLATE NOCASE",
        "COLLATE BINARY",
    ],
    functions: &["abs", "coalesce", "max", "length", "iif", "json"],
    types: &["INTEGER", "TEXT", "REAL", "NUMERIC", "BLOB"],
    array: ("(", ")"),
};

struct Db(*mut ffi::sqlite3);

/// One bytecode instruction as `EXPLAIN` lists it: opcode and p1 to p5.
type Instruction = [Option<Vec<u8>>; 6];

/// SQLite's message for SQL it refuses to compile.
#[derive(Debug, thiserror::Error)]
#[error("{}", .0.escape_ascii())]
struct Rejected(Vec<u8>);

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
        let db = Self(db);
        // SAFETY: the statement is NUL terminated and the handle is open.
        let rc = unsafe {
            ffi::sqlite3_exec(
                db.0,
                c"CREATE TABLE t(a INTEGER, b TEXT)".as_ptr(),
                None,
                ptr::null_mut(),
                ptr::null_mut(),
            )
        };
        assert_eq!(rc, ffi::SQLITE_OK, "cannot create the fixture table");
        db
    }

    /// Compiles `EXPLAIN sql` and returns its program, or SQLite's error message.
    fn explain(&self, sql: &[u8]) -> Result<Vec<Instruction>, Rejected> {
        let mut text = b"EXPLAIN ".to_vec();
        text.extend_from_slice(sql);
        let len = c_int::try_from(text.len()).expect("SQL fits a c_int");
        let mut stmt = ptr::null_mut();
        // SAFETY: SQLite reads at most `len` bytes of `text`, which outlives the call.
        let rc = unsafe {
            ffi::sqlite3_prepare_v2(
                self.0,
                text.as_ptr().cast(),
                len,
                &mut stmt,
                ptr::null_mut(),
            )
        };
        if rc != ffi::SQLITE_OK || stmt.is_null() {
            // SAFETY: finalizing NULL is a no-op, and sqlite3_errmsg returns a NUL terminated
            // string owned by the handle that stays valid until the next call, so it is copied first.
            return Err(Rejected(unsafe {
                ffi::sqlite3_finalize(stmt);
                CStr::from_ptr(ffi::sqlite3_errmsg(self.0))
                    .to_bytes()
                    .to_vec()
            }));
        }
        let mut program = Vec::new();
        // SAFETY: `stmt` is a live prepared statement until the final finalize, and each column's
        // bytes are copied before the next step invalidates them.
        unsafe {
            while ffi::sqlite3_step(stmt) == ffi::SQLITE_ROW {
                let instruction: Instruction = std::array::from_fn(|i| {
                    let column = c_int::try_from(i + 1).expect("column index fits a c_int");
                    let data = ffi::sqlite3_column_blob(stmt, column);
                    let len = ffi::sqlite3_column_bytes(stmt, column);
                    (!data.is_null()).then(|| {
                        let len = usize::try_from(len).expect("column length is non-negative");
                        std::slice::from_raw_parts(data.cast::<u8>(), len).to_vec()
                    })
                });
                if instruction[0].as_deref() != Some(b"Init") {
                    program.push(instruction);
                }
            }
            ffi::sqlite3_finalize(stmt);
        }
        Ok(program)
    }
}

impl Drop for Db {
    fn drop(&mut self) {
        // SAFETY: every statement is finalized, so the handle closes.
        unsafe { ffi::sqlite3_close(self.0) };
    }
}

thread_local! {
    // EXPLAIN compiles without running, so one database serves every input.
    static DB: Db = Db::open();
}

fn check(db: &Db, chain: &Chain) {
    let sql = format!("SELECT {} FROM t", chain.render(&SQLITE));
    let Ok(original) = db.explain(sql.as_bytes()) else {
        return;
    };
    let Ok(mut statements) = Parser::parse_sql(&SQLiteDialect {}, &sql) else {
        return;
    };
    let [statement] = statements.as_mut_slice() else {
        return;
    };
    parenthesize(statement);
    let grouped = statement.to_string();
    // SAFETY: sqlite3_libversion returns a static NUL terminated string.
    let version = unsafe { CStr::from_ptr(ffi::sqlite3_libversion()) };
    match db.explain(grouped.as_bytes()) {
        Err(err) => panic!(
            "SQLite {version:?} rejects sqlparser's grouping of SQL it accepts\n  sql:     {sql}\n  grouped: {grouped}\n  error:   {err}"
        ),
        Ok(regrouped) => assert!(
            original == regrouped,
            "sqlparser groups the expression differently from SQLite {version:?}\n  sql:     {sql}\n  grouped: {grouped}"
        ),
    }
}

fuzz_target!(|chain: Chain| DB.with(|db| check(db, &chain)));

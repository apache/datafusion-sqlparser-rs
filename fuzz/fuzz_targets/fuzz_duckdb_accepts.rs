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

use libduckdb_sys as ffi;
use libfuzzer_sys::fuzz_target;
use serde_json::Value;
use sqlparser::dialect::DuckDbDialect;
use sqlparser::parser::{Parser, ParserError};
use std::collections::HashSet;
use std::ffi::{CStr, CString};
use std::mem;
use std::ptr;

struct Ctx {
    _db: ffi::duckdb_database,
    _conn: ffi::duckdb_connection,
    // Prepared `SELECT json_serialize_sql($1)` reused for every input
    stmt: ffi::duckdb_prepared_statement,
    catalog: HashSet<String>,
}

fn load_catalog(conn: ffi::duckdb_connection) -> HashSet<String> {
    // SAFETY: duckdb_result is plain C data that the query call overwrites before any read.
    let mut result = unsafe { mem::zeroed::<ffi::duckdb_result>() };
    // SAFETY: conn is valid, the query is NUL-terminated, and result is a valid out value.
    let rc = unsafe {
        ffi::duckdb_query(
            conn,
            c"SELECT DISTINCT function_name FROM duckdb_functions()".as_ptr(),
            &mut result,
        )
    };
    assert_eq!(rc, ffi::duckdb_state_DuckDBSuccess, "cannot load catalog");
    // SAFETY: result holds a successful query result.
    let rows = unsafe { ffi::duckdb_row_count(&mut result) };
    let mut catalog = HashSet::new();
    for i in 0..rows {
        // SAFETY: result is valid and col 0 row i is within bounds.
        let ptr = unsafe { ffi::duckdb_value_varchar(&mut result, 0, i) };
        if !ptr.is_null() {
            // SAFETY: ptr is non-null and NUL-terminated per duckdb_value_varchar contract.
            let name = unsafe { CStr::from_ptr(ptr) }
                .to_string_lossy()
                .into_owned();
            // SAFETY: ptr is a DuckDB allocation and must be freed with duckdb_free.
            unsafe { ffi::duckdb_free(ptr.cast()) };
            catalog.insert(name);
        }
    }
    // SAFETY: result must always be destroyed.
    unsafe { ffi::duckdb_destroy_result(&mut result) };
    catalog
}

fn init_ctx() -> Ctx {
    let mut db: ffi::duckdb_database = ptr::null_mut();
    // SAFETY: null path opens an in-memory database, and db is a valid out pointer.
    let rc = unsafe { ffi::duckdb_open(ptr::null(), &mut db) };
    assert_eq!(rc, ffi::duckdb_state_DuckDBSuccess, "cannot open DuckDB");
    let mut conn: ffi::duckdb_connection = ptr::null_mut();
    // SAFETY: db is a valid open database handle, and conn is a valid out pointer.
    let rc = unsafe { ffi::duckdb_connect(db, &mut conn) };
    assert_eq!(
        rc,
        ffi::duckdb_state_DuckDBSuccess,
        "cannot connect to DuckDB"
    );
    // SAFETY: duckdb_result is plain C data that the query call overwrites before any read.
    let mut load_result = unsafe { mem::zeroed::<ffi::duckdb_result>() };
    // SAFETY: conn is valid, the query is NUL-terminated, and load_result is a valid out value.
    let rc = unsafe { ffi::duckdb_query(conn, c"LOAD json".as_ptr(), &mut load_result) };
    assert_eq!(
        rc,
        ffi::duckdb_state_DuckDBSuccess,
        "cannot load json extension"
    );
    // SAFETY: result must always be destroyed.
    unsafe { ffi::duckdb_destroy_result(&mut load_result) };
    let catalog = load_catalog(conn);
    let mut stmt: ffi::duckdb_prepared_statement = ptr::null_mut();
    // SAFETY: conn is valid, the query is NUL-terminated, and stmt is a valid out pointer.
    let rc = unsafe {
        ffi::duckdb_prepare(
            conn,
            c"SELECT json_serialize_sql($1::VARCHAR)".as_ptr(),
            &mut stmt,
        )
    };
    assert_eq!(
        rc,
        ffi::duckdb_state_DuckDBSuccess,
        "cannot prepare json_serialize_sql"
    );
    Ctx {
        _db: db,
        _conn: conn,
        stmt,
        catalog,
    }
}

// json_serialize_sql is parse-only with no transaction or schema state accumulated across calls.
thread_local! {
    static CTX: Ctx = init_ctx();
}

fn collect_fn_names<'a>(val: &'a Value, out: &mut Vec<&'a str>) {
    match val {
        Value::Object(map) => {
            if let Some(Value::String(name)) = map.get("function_name") {
                out.push(name.as_str());
            }
            for v in map.values() {
                collect_fn_names(v, out);
            }
        }
        Value::Array(arr) => {
            for v in arr {
                collect_fn_names(v, out);
            }
        }
        _ => {}
    }
}

// DuckDB parses unknown operator strings as calls to functions that do not exist, so they never bind.
fn is_bogus_operator(name: &str, catalog: &HashSet<String>) -> bool {
    if catalog.contains(name) {
        return false;
    }
    let base = name.strip_suffix("__postfix").unwrap_or(name);
    !base.is_empty() && base.chars().all(|c| !c.is_alphanumeric() && c != '_')
}

fuzz_target!(|sql: &str| {
    if sql.contains('\0') {
        return;
    }
    CTX.with(|ctx| {
        // Bound to a name so the pointer outlives the FFI call
        let c_sql = CString::new(sql).unwrap();
        // SAFETY: stmt is valid, param index 1 matches $1, and c_sql outlives this call.
        if unsafe { ffi::duckdb_bind_varchar(ctx.stmt, 1, c_sql.as_ptr()) }
            != ffi::duckdb_state_DuckDBSuccess
        {
            return;
        }
        // SAFETY: duckdb_result is plain C data that the query call overwrites before any read.
        let mut result = unsafe { mem::zeroed::<ffi::duckdb_result>() };
        // SAFETY: stmt is valid with param 1 bound, and result is a valid out value.
        if unsafe { ffi::duckdb_execute_prepared(ctx.stmt, &mut result) }
            != ffi::duckdb_state_DuckDBSuccess
        {
            // SAFETY: result must always be destroyed.
            unsafe { ffi::duckdb_destroy_result(&mut result) };
            return;
        }
        // SAFETY: result is valid, col 0 row 0 exists, and the returned pointer is freed below.
        let json_ptr = unsafe { ffi::duckdb_value_varchar(&mut result, 0, 0) };
        // SAFETY: result must always be destroyed.
        unsafe { ffi::duckdb_destroy_result(&mut result) };
        if json_ptr.is_null() {
            return;
        }
        // SAFETY: json_ptr is non-null and NUL-terminated per duckdb_value_varchar contract.
        let ast_result =
            serde_json::from_slice::<Value>(unsafe { CStr::from_ptr(json_ptr) }.to_bytes());
        // SAFETY: json_ptr is a DuckDB allocation and must be freed with duckdb_free.
        unsafe { ffi::duckdb_free(json_ptr.cast()) };
        let ast = match ast_result {
            Ok(v) => v,
            Err(_) => return,
        };
        // json_serialize_sql sets "error":true for parse failures and non-SELECT statements.
        if ast.get("error").and_then(Value::as_bool).unwrap_or(false) {
            return;
        }
        let mut fn_names: Vec<&str> = Vec::new();
        collect_fn_names(&ast, &mut fn_names);
        if fn_names.iter().any(|n| is_bogus_operator(n, &ctx.catalog)) {
            return;
        }
        match Parser::parse_sql(&DuckDbDialect {}, sql) {
            Ok(_) | Err(ParserError::RecursionLimitExceeded) => {}
            Err(err) => {
                // SAFETY: duckdb_library_version returns a static NUL-terminated string.
                let version = unsafe { CStr::from_ptr(ffi::duckdb_library_version()) };
                panic!(
                    "DuckDB {version:?} accepts SQL the DuckDB dialect rejects\n  sql: {sql:?}\n  error: {err}"
                )
            }
        }
    });
});

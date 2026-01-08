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

//! Dialect-aware SQL serialization traits and utilities.
//!
//! This module provides the [`ToSql`] trait for converting AST nodes to SQL strings
//! with dialect-specific formatting. This is necessary because some SQL dialects
//! (like ClickHouse) require specific casing for type names that differs from the
//! standard uppercase convention.
//!
//! # Example
//!
//! ```
//! use sqlparser::ast::ToSql;
//! use sqlparser::dialect::ClickHouseDialect;
//! use sqlparser::parser::Parser;
//!
//! let sql = "CREATE TABLE t (col String)";
//! let dialect = ClickHouseDialect {};
//! let ast = Parser::parse_sql(&dialect, sql).unwrap();
//!
//! // Dialect-aware serialization preserves ClickHouse's PascalCase types
//! let regenerated = ast[0].to_sql(&dialect);
//! assert!(regenerated.contains("String"));
//! ```
//!
//! # Design Rationale
//!
//! The existing `Display` trait implementation cannot be made dialect-aware because
//! `fmt::Display::fmt` has a fixed signature that doesn't accept dialect context.
//! Rather than changing `Display` (which would be a breaking change), we introduce
//! `ToSql` as a separate trait that accepts a `&dyn Dialect` parameter.
//!
//! ## Key Design Decisions
//!
//! 1. **Coexistence with Display**: `ToSql` and `Display` coexist. `Display` continues
//!    to provide standard SQL formatting (uppercase types), while `ToSql` enables
//!    dialect-specific formatting.
//!
//! 2. **Macro for Delegation**: Types that don't contain `DataType` fields use the
//!    [`impl_to_sql_display!`] macro to delegate `ToSql::write_sql` to their `Display`
//!    implementation. This avoids code duplication (DRY principle).
//!
//! 3. **Recursive Propagation**: Types containing `DataType` fields implement `write_sql`
//!    explicitly, calling `write_sql` recursively on nested types to propagate dialect
//!    context through the AST tree.
//!
//! # Coverage
//!
//! The `ToSql` trait is implemented for all major AST types that users are likely to
//! serialize, including:
//!
//! - `Statement`, `Query`, `Select`, `Expr`
//! - `CreateTable`, `AlterTable`, `CreateView`, `CreateFunction`
//! - `DataType`, `ColumnDef`, `ViewColumnDef`
//! - `Function`, `WindowSpec`, `OrderByExpr`
//! - And many more supporting types
//!
//! # Migration from Display
//!
//! If you currently use `format!("{}", statement)` or `statement.to_string()` and need
//! dialect-aware formatting, change to:
//!
//! ```ignore
//! use sqlparser::ast::ToSql;
//! let sql = statement.to_sql(&dialect);
//! ```

#[cfg(not(feature = "std"))]
use alloc::string::String;
use core::fmt::{self, Write};

use crate::dialect::Dialect;

/// Trait for dialect-aware SQL serialization.
///
/// Types implementing this trait can be converted to SQL strings while respecting
/// dialect-specific formatting rules, such as ClickHouse's requirement for
/// PascalCase type names.
///
/// The default `to_sql` implementation calls `write_sql` with a string buffer.
/// Types should implement `write_sql` to perform the actual formatting.
pub trait ToSql {
    /// Converts this AST node to a SQL string using dialect-specific formatting.
    ///
    /// # Example
    ///
    /// ```
    /// use sqlparser::ast::{DataType, ToSql};
    /// use sqlparser::dialect::{ClickHouseDialect, GenericDialect};
    ///
    /// let dt = DataType::Int64;
    ///
    /// // ClickHouse requires PascalCase
    /// assert_eq!(dt.to_sql(&ClickHouseDialect {}), "Int64");
    ///
    /// // Other dialects use uppercase
    /// assert_eq!(dt.to_sql(&GenericDialect {}), "INT64");
    /// ```
    fn to_sql(&self, dialect: &dyn Dialect) -> String {
        let mut s = String::new();
        // write_sql should not fail when writing to a String
        self.write_sql(&mut s, dialect).unwrap();
        s
    }

    /// Writes this AST node as SQL to the given formatter using dialect-specific formatting.
    ///
    /// Implementors should use this method to perform the actual SQL generation,
    /// calling `write_sql` on nested types that contain dialect-sensitive elements
    /// (like `DataType`).
    fn write_sql(&self, f: &mut dyn Write, dialect: &dyn Dialect) -> fmt::Result;
}

/// Macro to implement `ToSql` by delegating to `Display`.
///
/// Use this macro for types that don't contain `DataType` fields and can
/// safely use their existing `Display` implementation for all dialects.
///
/// # Example
///
/// ```ignore
/// impl_to_sql_display!(CreateDatabase, CreateSchema, CreateIndex);
/// ```
#[macro_export]
macro_rules! impl_to_sql_display {
    ($($t:ty),+ $(,)?) => {
        $(
            impl $crate::ast::ToSql for $t {
                fn write_sql(
                    &self,
                    f: &mut dyn ::core::fmt::Write,
                    _dialect: &dyn $crate::dialect::Dialect,
                ) -> ::core::fmt::Result {
                    write!(f, "{}", self)
                }
            }
        )+
    };
}

/// Helper to write a comma-separated list of items using dialect-aware formatting.
pub fn write_comma_separated_tosql<T: ToSql>(
    f: &mut dyn Write,
    items: &[T],
    dialect: &dyn Dialect,
) -> fmt::Result {
    let mut first = true;
    for item in items {
        if !first {
            write!(f, ", ")?;
        }
        first = false;
        item.write_sql(f, dialect)?;
    }
    Ok(())
}

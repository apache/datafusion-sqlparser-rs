# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Test Commands

```bash
# Build
cargo build
cargo build --all-features

# Run all tests
cargo test --all-features

# Run a single test
cargo test test_name --all-features

# Run tests for a specific dialect
cargo test sqlparser_postgres --all-features

# Lint
cargo clippy --all-targets --all-features -- -D warnings

# Format
cargo fmt --all

# Check (faster than full build)
cargo check --all-targets --all-features

# Build docs
cargo doc --document-private-items --no-deps --workspace --all-features

# Run benchmarks (from sqlparser_bench directory)
cd sqlparser_bench && cargo bench
```

## Crate Features

- `serde`: Adds Serialize/Deserialize for all AST nodes
- `visitor`: Adds a Visitor for recursively walking the AST
- `recursive-protection` (default): Stack overflow protection
- `json_example`: For CLI example only

## Architecture

This is an extensible SQL lexer and parser that produces an Abstract Syntax Tree (AST).

### Core Components

- **`src/tokenizer.rs`**: Lexer that converts SQL text into tokens. `Tokenizer::new(dialect, sql).tokenize()` returns `Vec<TokenWithLocation>`.

- **`src/parser/mod.rs`**: Recursive descent parser using Pratt parsing for expressions. Entry point is `Parser::parse_sql(&dialect, sql)` returning `Vec<Statement>`.

- **`src/ast/mod.rs`**: AST type definitions. `Statement` is the top-level enum. Key types: `Query`, `Select`, `Expr`, `DataType`, `ObjectName`.

- **`src/dialect/mod.rs`**: SQL dialect trait and implementations. Each dialect (PostgreSQL, MySQL, etc.) customizes parsing behavior. `GenericDialect` is the most permissive.

### Dialect System

Dialects customize parsing via the `Dialect` trait. Methods control identifier quoting, keyword handling, and syntax variations. Dialect-specific features should work with both the specific dialect AND `GenericDialect`.

### Testing Patterns

Tests use `TestedDialects` from `src/test_utils.rs`:

```rust
use sqlparser::test_utils::*;

// Test across all dialects
all_dialects().verified_stmt("SELECT 1");

// Test specific dialects
TestedDialects::new(vec![Box::new(PostgreSqlDialect {})]).verified_stmt("...");

// Test all dialects except specific ones
all_dialects_except(|d| d.is::<MySqlDialect>()).verified_stmt("...");
```

Key test helpers:
- `verified_stmt(sql)`: Parse and verify round-trip serialization
- `verified_query(sql)`: Same but returns `Query`
- `one_statement_parses_to(sql, canonical)`: Test with different canonical form

### Round-Trip Invariant

AST nodes implement `Display` to reproduce the original SQL (minus comments/whitespace). Tests verify `parse(sql).to_string() == sql`.

### Source Spans

AST nodes include `Span` information for source locations. When constructing AST nodes manually, use `Span::empty()`.

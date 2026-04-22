# Extensible SQL Lexer and Parser for Rust Agents Guidelines

## General Agent Workflow
1. You will write unit tests to ensure your code change is working as expected.
2. You will run the commands in the Pre Commit Checks section below to ensure your change is ready for a pull request.
3. When instructed to open a PR, you will follow the instructions in the Pull Request Guidelines section below.

## General Coding Guidelines
1. Refrain from adding conditions on specific dialects, such as `dialect_is!(...)` or `dialect_of!(... | ...)`. Instead, define a new function in the `Dialect` trait that describes the condition, so that dialects can turn this condition on more easily.
2. Make targeted code changes and refrain from refactoring, unless it's absolutely required.

## Unit Tests Guidelines
- New unit tests should be added to the `tests` module in the corresponding dialect file (e.g., `tests/sqlparser_redshift.rs` for Redshift), and should be placed at the end of the file.
- If the new functionality is gated using a dialect function, and the SQL is likely relevant in most dialects, tests should be placed under `tests/sqlparser_common.rs`.
- When testing a multi-line SQL statement, use a raw string literal, i.e. `r#"..."#` to preserve formatting.
- The parser builds an abstract syntax tree (AST) from the SQL statement and has functionality to display the tree as SQL. Use the following template for simple unit tests where you expect the SQL created from the AST to be the same as the input SQL:
```rust
<dialect>().verified_stmt(r#"..."#);
```
For example: `snowflake().verified_stmt(r#"SELECT * FROM my_table"#)`. Use `one_statement_parses_to` instead of `verified_stmt` when you expect the SQL created by the AST to differ than the input SQL. For example:
```rust
snowflake().one_statement_parses_to(
    "SELECT * FROM my_table t",
    "SELECT * FROM my_table AS t",
)
```

## Analyzing Parsing Issues
You can try to simplify the SQL statement to identify the root cause of the parsing issue. This may involve removing certain clauses or components of the SQL statement to see if it can be parsed successfully. Additionally, you can compare the problematic SQL statement with similar statements that are parsed correctly to identify any differences that may be causing the issue.

## Pre Commit Checks
Run the following commands before you commit to ensure the change will pass the CI process:
```bash
cargo test --all-features
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
```

## Pull Request Guidelines
1. PR title should follow this format: `<DIALECT>: <SHORT DESCRIPTION>`, For example, `Showflake: Add support for casting to VARIANT`.
2. Make the PR comment short, provide an example of what was not working and a short description of the fix. Be succint.

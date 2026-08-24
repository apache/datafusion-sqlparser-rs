# Extensible SQL Lexer and Parser for Rust Agents Guidelines

## General Agent Workflow
1. You will write unit tests to ensure your code change is working as expected.
2. You will run the commands in the Pre Commit Checks section below to ensure your change is ready for a pull request.
3. You will not open a PR unless explicitly instructed to, following the Pull Request Guidelines section below.

## General Coding Guidelines
1. Refrain from adding conditions on specific dialects, such as `dialect_is!(...)` or `dialect_of!(... | ...)`. Instead, define a new function in the `Dialect` trait that describes the condition, so that dialects can turn this condition on more easily.
2. Make targeted code changes and refrain from refactoring, unless it's absolutely required.

## Code Documentation Guidelines
1. Keep comments and doc comments brief and non-repetitive.
2. Document only what the code does not plainly state, never the trivial or self-evident.
3. Never describe the change itself in comments. That belongs in the PR.
4. The human must read every comment you add and confirm it is necessary in its form.

## Unit Tests Guidelines
- New unit tests should be added to the `tests` module in the corresponding dialect file (e.g., `tests/sqlparser_redshift.rs` for Redshift), and should be placed at the end of the file.
- If the new functionality is gated using a dialect function, and the SQL is likely relevant in most dialects, tests should be placed under `tests/sqlparser_common.rs`.
- Cover both positive and negative cases: valid input parses and round-trips, invalid input fails with the expected error.
- Cover every dialect the syntax is relevant to, e.g. via `all_dialects_where(...)`.
- When adding new syntax, run the fuzzer to check for panics (see `docs/fuzzing.md`).
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
1. Keep PRs small, atomic, and self-contained: one feature or fix per PR, with its tests.
2. Before opening a PR, check for an existing PR covering the same change and for open issues to reference.
3. Never open a PR automatically: show the human the title, description, and diff, and only open after they have actually read them and approved.
4. The human must be able to explain every line of the change.
5. PR title should follow this format: `<DIALECT>: <SHORT DESCRIPTION>`. For example, `Snowflake: Add support for casting to VARIANT`.
6. Keep the PR description under 20 lines: an example of what was not working and a short description of the fix.
7. Verify new dialect syntax against the real engine or its official documentation, and link it in the description.
8. Disclose AI assistance in the description.

## Code Review Guidelines
1. An agent SHALL NOT do or post reviews, review comments, or replies to review comments.
2. Address reviewer feedback on your open PRs before opening new ones.

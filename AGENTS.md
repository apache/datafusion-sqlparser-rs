# SQL Parser in Rust

## Agent Workflow
1. You will receive a SQL statement that is not parsed correctly by the project. Typically parsing will fail with an error.
2. You will add a unit test to check that the SQL statement is indeed unparsed. Follow the instructions in the "Unit Tests" section below.
3. You will analyze why the SQL statement is not parsed correctly, pointing to the code section that you think is faulty. See more info in the Analyzing Parsing Issues and General Coding Guidelines below.
4. You will fix the parsing issue and ensure that the new unit test passes successfully by running `cargo test --all-features`.
5. You will remove the first unit test that you added in step 2 and instead create "synthetic" unit tests to cover the code sections that you modified, to ensure that similar parsing issues are caught in the future.
6. Run the commands in the Pre Commit Checks section below to ensure that all unit tests are passing and that the code is ready for a pull request.
7. You will create a PR in the with the changes you made, follow the instructions on Pull Request Guidelines below.

## General Coding Guidelines
1. Refrain from adding conditions on specific dialects, such as `dialect_id!(self is SnowflakeDialect)` or `dialect_of!(self is SnowflakeDialect | BigQueryDialect | MySqlDialect | HiveDialect)`. Instead, define a new function in the `Dialect` trait that describes the condition, so that dialects can turn this condition on more easily.
2. Make targeted code changes and refrain from refactoring, unless it's absolutely required.

## Unit Tests
Follow these rules:
- When testing a multi-line SQL statement, use a raw string literal (r#"..."#) to preserve formatting.
- You can use the following template for simple unit tests:
```rust
<dialect>().parse_sql_statements(r#"..."#).unwrap();
```
For example: `snowflake().parse_sql_statements(r#"SELECT * FROM my_table;"#).unwrap();`

- New unit tests should be added to the `tests` module in the corresponding dialect file (e.g., `src/tests/sqlparser_redshift.rs` for Redshift), and should be placed at the end of the file.

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

# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project aims to adhere to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Given that the parser produces a typed AST, any changes to the AST will technically be breaking and thus will result in a `0.(N+1)` version. We document changes that break via addition as "Added".

## [Unreleased]
Check https://github.com/andygrove/sqlparser-rs/commits/master for undocumented changes.

### Changed
- Change `Ident` (previously a simple `String`) to store the parsed (unquoted) `value` of the identifier and the `quote_style` separately (#143) - thanks @apparebit!
- Support Snowflake's `FROM (table_name)` (#155) - thanks @eyalleshem!

### Added
- Support basic forms of `CREATE INDEX` and `DROP INDEX` (#167) - thanks @mashuai!
- Support MSSQL `TOP (<N>) [ PERCENT ] [ WITH TIES ]` (#150) - thanks @alexkyllo!
- Support MySQL `LIMIT row_count OFFSET offset` (not followed by `ROW` or `ROWS`) and remember which variant was parsed (#158) - thanks @mjibson!
- Support PostgreSQL `CREATE TABLE IF NOT EXISTS table_name` (#163) - thanks @alex-dukhno!

### Fixed
- Report an error for unterminated string literals (#165)

## [0.5.0] - 2019-10-10

### Changed
- Replace the `Value::Long(u64)` and `Value::Double(f64)` variants with `Value::Number(String)` to avoid losing precision when parsing decimal literals (#130) - thanks @benesch!
- `--features bigdecimal` can be enabled to work with `Value::Number(BigDecimal)` instead, at the cost of an additional dependency.

### Added
- Support MySQL `SHOW COLUMNS`, `SET <variable>=<value>`, and `SHOW <variable>` statements (#135) - thanks @quodlibetor and @benesch!

### Fixed
- Don't fail to parse `START TRANSACTION` followed by a semicolon (#139) - thanks @gaffneyk!


## [0.4.0] - 2019-07-02
This release brings us closer to SQL-92 support, mainly thanks to the improvements contributed back from @MaterializeInc's fork and other work by @benesch.

### Changed
- Remove "SQL" from type and enum variant names, `SQLType` -> `DataType`, remove "sql" prefix from module names (#105, #122)
- Rename `ASTNode` -> `Expr` (#119)
- Improve consistency of binary/unary op nodes (#112):
    - `ASTNode::SQLBinaryExpr` is now `Expr::BinaryOp` and `ASTNode::SQLUnary` is `Expr::UnaryOp`;
    - The `op: SQLOperator` field is now either a `BinaryOperator` or an `UnaryOperator`.
- Change the representation of JOINs to match the standard (#109): `SQLSelect`'s `relation` and `joins` are replaced with `from: Vec<TableWithJoins>`. Before this change `FROM foo NATURAL JOIN bar, baz` was represented as "foo" as the `relation` followed by two joins (`Inner(Natural)` and `Implicit`); now it's two `TableWithJoins` (`foo NATURAL JOIN bar` and `baz`).
- Extract a `SQLFunction` struct (#89)
- Replace `Option<Vec<T>>` with `Vec<T>` in the AST structs (#73)
- Change `Value::Long()` to be unsigned, use u64 consistently (#65)

### Added
- Infra:
    - Implement `fmt::Display` on AST nodes (#124) - thanks @vemoo!
    - Implement `Hash` (#88) and `Eq` (#123) on all AST nodes
    - Implement `std::error::Error` for `ParserError` (#72)
    - Handle Windows line-breaks (#54)
- Expressions:
    - Support `INTERVAL` literals (#103)
    - Support `DATE` / `TIME` / `TIMESTAMP` literals (#99)
    - Support `EXTRACT` (#96)
    - Support `X'hex value'` literals (#95)
    - Support `EXISTS` subqueries (#90)
    - Support nested expressions in `BETWEEN` (#80)
    - Support `COUNT(DISTINCT x)` and similar (#77)
    - Support `CASE operand WHEN expected_value THEN ..` and table-valued functions (#59)
    - Support analytic (window) functions (`OVER` clause) (#50)
- Queries / DML:
    - Support nested joins (#100) and derived tables with set operations (#111)
    - Support `UPDATE` statements (#97)
    - Support `INSERT INTO foo SELECT * FROM bar` and `FROM VALUES (...)` (#91)
    - Support `SELECT ALL` (#76)
    - Add `FETCH` and `OFFSET` support, and `LATERAL` (#69) - thanks @thomas-jeepe!
    - Support `COLLATE`, optional column list in CTEs (#64)
- DDL/TCL:
    - Support `START/SET/COMMIT/ROLLBACK TRANSACTION` (#106) - thanks @SamuelMarks!
    - Parse column constraints in any order (#93)
    - Parse `DECIMAL` and `DEC` aliases for `NUMERIC` type (#92)
    - Support `DROP [TABLE|VIEW]` (#75)
    - Support arbitrary `WITH` options for `CREATE [TABLE|VIEW]` (#74)
    - Support constraints in `CREATE TABLE` (#65)
- Add basic MSSQL dialect (#61) and some MSSQL-specific features:
    - `CROSS`/`OUTER APPLY` (#120)
    - MSSQL identifier and alias parsing rules (#66)
    - `WITH` hints (#59)

### Fixed
- Report an error for `SELECT * FROM a OUTER JOIN b` instead of parsing `OUTER` as an alias (#118)
- Fix the precedence of `NOT LIKE` (#82) and unary `NOT` (#107)
- Do not panic when `NOT` is not followed by an expected keyword (#71)
successfully instead of returning a parse error - thanks @ivanceras! (#67) - and similar fixes for queries with no `FROM` (#116)
- Fix issues with `ALTER TABLE ADD CONSTRAINT` parsing (#65)
- Serialize the "not equals" operator as `<>` instead of `!=` (#64)
- Remove dependencies on `uuid` (#59) and `chrono` (#61)
- Make `SELECT` query with `LIMIT` clause but no `WHERE` parse - Fix incorrect behavior of `ASTNode::SQLQualifiedWildcard::to_string()` (returned `foo*` instead of `foo.*`) - thanks @thomas-jeepe! (#52)

## [0.3.1] - 2019-04-20
### Added
- Extended `SQLStatement::SQLCreateTable` to support Hive's EXTERNAL TABLES (`CREATE EXTERNAL TABLE .. STORED AS .. LOCATION '..'`) - thanks @zhzy0077! (#46)
- Parse `SELECT DISTINCT` to `SQLSelect::distinct` (#49)

## [0.3.0] - 2019-04-03
### Changed
This release includes major changes to the AST structs to add a number of features, as described in #37 and #43. In particular:
- `ASTNode` variants that represent statements were extracted from `ASTNode` into a separate `SQLStatement` enum;
    - `Parser::parse_sql` now returns a `Vec` of parsed statements.
    - `ASTNode` now represents an expression (renamed to `Expr` in 0.4.0)
- The query representation (formerly `ASTNode::SQLSelect`) became more complicated to support:
    - `WITH` and `UNION`/`EXCEPT`/`INTERSECT` (via `SQLQuery`, `Cte`, and `SQLSetExpr`),
    - aliases and qualified wildcards in `SELECT` (via `SQLSelectItem`),
    - and aliases in `FROM`/`JOIN` (via `TableFactor`).
- A new `SQLObjectName` struct is used instead of `String` or `ASTNode::SQLCompoundIdentifier` - for objects like tables, custom types, etc.
- Added support for "delimited identifiers" and made keywords context-specific (thus accepting them as valid identifiers in most contexts) - **this caused a regression in parsing `SELECT .. FROM .. LIMIT ..` (#67), fixed in 0.4.0**

### Added
Other than the changes listed above, some less intrusive additions include:
- Support `CREATE [MATERIALIZED] VIEW` statement
- Support `IN`, `BETWEEN`, unary +/- in epressions
- Support `CHAR` data type and `NUMERIC` not followed by `(p,s)`.
- Support national string literals (`N'...'`)

## [0.2.4] - 2019-03-08
Same as 0.2.2.

## [0.2.3] - 2019-03-08 [YANKED]

## [0.2.2] - 2019-03-08
### Changed
- Removed `Value::String`, `Value::DoubleQuotedString`, and `Token::String`, making
    - `'...'` parse as a string literal (`Value::SingleQuotedString`), and
    - `"..."` fail to parse until version 0.3.0 (#36)

## [0.2.1] - 2019-01-13
We don't have a changelog for the changes made in 2018, but thanks to @crw5996, @cswinter, @fredrikroos, @ivanceras, @nickolay, @virattara for their contributions in the early stages of the project!

## [0.1.0] - 2018-09-03
Initial release

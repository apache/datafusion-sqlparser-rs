# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project aims to adhere to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Given that the parser produces a typed AST, any changes to the AST will technically be breaking and thus will result in a `0.(N+1)` version. We document changes that break via addition as "Added".

## [Unreleased]
Check https://github.com/sqlparser-rs/sqlparser-rs/commits/main for undocumented changes.

## [0.11.0] 2020-09-24

### Added
* Support minimum display width for integer data types (#337) Thanks @vasilev-alex!
* Add logical XOR operator (#357) - Thanks @xzmrdltl!
* Support DESCRIBE table_name (#340) - Thanks @ovr!
* Support SHOW CREATE TABLE|EVENT|FUNCTION (#338) - Thanks @ovr!
* Add referential actions to TableConstraint foreign key (#306) - Thanks @joshwd36!

### Changed
* Enable map access for numbers, multiple nesting levels (#356) - Thanks @Igosuki!
* Rename Token::Mult to Token::Mul (#353) - Thanks @koushiro!
* Use derive(Default) for HiveFormat (#348) - Thanks @koushiro!
* Improve tokenizer error (#347)  - Thanks @koushiro!
* Eliminate redundant string copy in Tokenizer (#343) - Thanks @koushiro!
* Update bigdecimal requirement from 0.2 to 0.3 dependencies (#341)
* Support parsing hexadecimal literals that start with `0x` (#324) - Thanks @TheSchemm!


## [0.10.0] 2020-08-23

### Added
* Support for `no_std` (#332) - Thanks @koushiro!
* Postgres regular expression operators (`~`, `~*`, `!~`, `!~*`) (#328) - Thanks @b41sh!
* tinyint (#320) - Thanks @sundy-li
* ILIKE (#300) - Thanks @maxcountryman!
* TRIM syntax (#331, #334) - Thanks ever0de


### Fixed
* Return error instead of panic (#316) - Thanks @BohuTANG!

### Changed
- Rename `Modulus` to `Modulo` (#335) - Thanks @RGRAVITY817!
- Update links to reflect repository move to `sqlparser-rs` GitHub org (#333) - Thanks @andygrove
- Add default value for `WindowFrame` (#313) - Thanks @Jimexist!

## [0.9.0] 2020-03-21

### Added
* Add support for `TRY_CAST` syntax (#299) - Thanks @seddonm1!

## [0.8.0] 2020-02-20

### Added
* Introduce Hive QL dialect `HiveDialect` and syntax (#235) - Thanks @hntd187!
* Add `SUBSTRING(col [FROM <expr>] [FOR <expr>])` syntax (#293)
* Support parsing floats without leading digits `.01` (#294)
* Support parsing multiple show variables (#290) - Thanks @francis-du!
* Support SQLite `INSERT OR [..]` syntax (#281) - Thanks @zhangli-pear!

## [0.7.0] 2020-12-28

### Changed
- Change the MySQL dialect to support `` `identifiers` `` quoted with backticks instead of the standard `"double-quoted"` identifiers (#247) - thanks @mashuai!
- Update bigdecimal requirement from 0.1 to 0.2 (#268)

### Added
- Enable dialect-specific behaviours in the parser (`dialect_of!()`) (#254) - thanks @eyalleshem!
- Support named arguments in function invocations (`ARG_NAME => val`) (#250) - thanks @eyalleshem!
- Support `TABLE()` functions in `FROM` (#253) - thanks @eyalleshem!
- Support Snowflake's single-line comments starting with '#' or '//' (#264) - thanks @eyalleshem!
- Support PostgreSQL `PREPARE`, `EXECUTE`, and `DEALLOCATE` (#243) - thanks @silathdiir!
- Support PostgreSQL math operators (#267) - thanks @alex-dukhno!
- Add SQLite dialect (#248) - thanks @mashuai!
- Add Snowflake dialect (#259) - thanks @eyalleshem!
- Support for Recursive CTEs - thanks @rhanqtl!
- Support `FROM (table_name) alias` syntax - thanks @eyalleshem!
- Support for `EXPLAIN [ANALYZE] VERBOSE` - thanks @ovr!
- Support `ANALYZE TABLE`
- DDL:
    - Support `OR REPLACE` in `CREATE VIEW`/`TABLE` (#239)  - thanks @Dandandan!
    - Support specifying `ASC`/`DESC` in index columns (#249) - thanks @mashuai!
    - Support SQLite `AUTOINCREMENT` and MySQL `AUTO_INCREMENT` column option in `CREATE TABLE` (#234) - thanks @mashuai!
    - Support PostgreSQL `IF NOT EXISTS` for `CREATE SCHEMA` (#276) - thanks @alex-dukhno!

### Fixed
- Fix a typo in `JSONFILE` serialization, introduced in 0.3.1 (#237)
- Change `CREATE INDEX` serialization to not end with a semicolon, introduced in 0.5.1 (#245)
- Don't fail parsing `ALTER TABLE ADD COLUMN` ending with a semicolon, introduced in 0.5.1 (#246) - thanks @mashuai

## [0.6.1] - 2020-07-20

### Added
- Support BigQuery `ASSERT` statement (#226)

## [0.6.0] - 2020-07-20

### Added
- Support SQLite's `CREATE TABLE (...) WITHOUT ROWID` (#208) - thanks @mashuai!
- Support SQLite's `CREATE VIRTUAL TABLE` (#209) - thanks @mashuai!

## [0.5.1] - 2020-06-26
This release should have been called `0.6`, as it introduces multiple incompatible changes to the API. If you don't want to upgrade yet, you can revert to the previous version by changing your `Cargo.toml` to:

    sqlparser = "= 0.5.0"


### Changed
- **`Parser::parse_sql` now accepts a `&str` instead of `String` (#182)** - thanks @Dandandan!
- Change `Ident` (previously a simple `String`) to store the parsed (unquoted) `value` of the identifier and the `quote_style` separately (#143) - thanks @apparebit!
- Support Snowflake's `FROM (table_name)` (#155) - thanks @eyalleshem!
- Add line and column number to TokenizerError (#194) - thanks @Dandandan!
- Use Token::EOF instead of Option<Token> (#195)
- Make the units keyword following `INTERVAL '...'` optional (#184) - thanks @maxcountryman!
- Generalize `DATE`/`TIME`/`TIMESTAMP` literals representation in the AST (`TypedString { data_type, value }`) and allow `DATE` and other keywords to be used as identifiers when not followed by a string (#187) - thanks @maxcountryman!
- Output DataType capitalized (`fmt::Display`) (#202) - thanks @Dandandan!

### Added
- Support MSSQL `TOP (<N>) [ PERCENT ] [ WITH TIES ]` (#150) - thanks @alexkyllo!
- Support MySQL `LIMIT row_count OFFSET offset` (not followed by `ROW` or `ROWS`) and remember which variant was parsed (#158) - thanks @mjibson!
- Support PostgreSQL `CREATE TABLE IF NOT EXISTS table_name` (#163) - thanks @alex-dukhno!
- Support basic forms of `CREATE INDEX` and `DROP INDEX` (#167) - thanks @mashuai!
- Support `ON { UPDATE | DELETE } { RESTRICT | CASCADE | SET NULL | NO ACTION | SET DEFAULT }` in `FOREIGN KEY` constraints (#170) - thanks @c7hm4r!
- Support basic forms of `CREATE SCHEMA` and `DROP SCHEMA` (#173) - thanks @alex-dukhno!
- Support `NULLS FIRST`/`LAST` in `ORDER BY` expressions (#176) - thanks @houqp!
- Support `LISTAGG()` (#174) - thanks @maxcountryman!
- Support the string concatentation operator `||` (#178) - thanks @Dandandan!
- Support bitwise AND (`&`), OR (`|`), XOR (`^`) (#181) - thanks @Dandandan!
- Add serde support to AST structs and enums (#196) - thanks @panarch!
- Support `ALTER TABLE ADD COLUMN`, `RENAME COLUMN`, and `RENAME TO` (#203) - thanks @mashuai!
- Support `ALTER TABLE DROP COLUMN` (#148) - thanks @ivanceras!
- Support `CREATE TABLE ... AS ...` (#206) - thanks @Dandandan!

### Fixed
- Report an error for unterminated string literals (#165)
- Make file format (`STORED AS`) case insensitive (#200) and don't allow quoting it (#201) - thanks @Dandandan!

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

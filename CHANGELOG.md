# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project aims to adhere to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Given that the parser produces a typed AST, any changes to the AST will technically be breaking and thus will result in a `0.(N+1)` version. We document changes that break via addition as "Added".

## [Unreleased]
Check https://github.com/sqlparser-rs/sqlparser-rs/commits/main for undocumented changes.


## [0.41.0] 2023-12-22

### Added
* Support `DEFERRED`, `IMMEDIATE`, and `EXCLUSIVE` in SQLite's `BEGIN TRANSACTION` command (#1067) - Thanks @takaebato
* Support generated columns skipping `GENERATED ALWAYS` keywords (#1058) - Thanks @takluyver
* Support `LOCK/UNLOCK TABLES` for MySQL (#1059) - Thanks @zzzdong
* Support `JSON_TABLE` (#1062) - Thanks @lovasoa
* Support `CALL` statements (#1063) - Thanks @lovasoa

### Fixed
* fix rendering of SELECT TOP (#1070) for Snowflake - Thanks jmhain

### Changed
* Improve documentation formatting (#1068) - Thanks @alamb
* Replace type_id() by trait method to allow wrapping dialects (#1065) - Thanks @jjbayer
* Document that comments aren't preserved for round trip (#1060) - Thanks @takluyver
* Update sqlparser-derive to use `syn 2.0` (#1040) - Thanks @serprex

## [0.40.0] 2023-11-27

### Added
* Add `{pre,post}_visit_query` to `Visitor` (#1044) - Thanks @jmhain
* Support generated virtual columns with expression (#1051) - Thanks @takluyver
* Support PostgreSQL `END` (#1035) - Thanks @tobyhede
* Support `INSERT INTO ... DEFAULT VALUES ...` (#1036) - Thanks @CDThomas
* Support `RELEASE` and `ROLLBACK TO SAVEPOINT` (#1045) - Thanks @CDThomas
* Support `CONVERT` expressions (#1048) - Thanks @lovasoa
* Support `GLOBAL` and `SESSION` parts in `SHOW VARIABLES` for mysql and generic - Thanks @emin100
* Support snowflake `PIVOT` on derived table factors (#1027) - Thanks @lustefaniak
* Support mssql json and xml extensions (#1043) - Thanks @lovasoa
* Support for `MAX` as a character length (#1038) - Thanks @lovasoa
* Support `IN ()` syntax of SQLite (#1028) - Thanks @alamb

### Fixed
* Fix extra whitespace printed before `ON CONFLICT` (#1037) - Thanks @CDThomas

### Changed
* Document round trip ability (#1052) - Thanks @alamb
* Add PRQL to list of users (#1031) - Thanks @vanillajonathan

## [0.39.0] 2023-10-27

### Added
* Support for `LATERAL FLATTEN` and similar (#1026) - Thanks @lustefaniak
* Support BigQuery struct, array and bytes , int64, `float64` datatypes (#1003) - Thanks @iffyio
* Support numbers as placeholders in Snowflake (e.g. `:1)` (#1001) - Thanks @yuval-illumex
* Support date 'key' when using semi structured data (#1023) @yuval-illumex
* Support IGNORE|RESPECT NULLs clause in window functions (#998) - Thanks @yuval-illumex
* Support for single-quoted identifiers (#1021) - Thanks @lovasoa
* Support multiple PARTITION statements in ALTER TABLE ADD statement (#1011) - Thanks @bitemyapp
* Support "with" identifiers surrounded by backticks in GenericDialect (#1010) - Thanks @bitemyapp
* Support INSERT IGNORE in MySql and GenericDialect (#1004) - Thanks @emin100
* Support SQLite `pragma` statement (#969) - Thanks @marhoily
* Support `position` as a column name (#1022) - Thanks @lustefaniak
* Support `FILTER` in Functions (for `OVER`) clause (#1007) - Thanks @lovasoa
* Support `SELECT * EXCEPT/REPLACE` syntax from ClickHouse (#1013) - Thanks @lustefaniak
* Support subquery as function arg w/o parens in Snowflake dialect (#996) - Thanks @jmhain
* Support `UNION DISTINCT BY NAME` syntax (#997) - Thanks @alexander-beedie
* Support mysql `RLIKE` and `REGEXP` binary operators (#1017) - Thanks @lovasoa
* Support bigquery `CAST AS x [STRING|DATE] FORMAT` syntax (#978) - Thanks @lustefaniak
* Support Snowflake/BigQuery `TRIM`. (#975) - Thanks @zdenal
* Support `CREATE [TEMPORARY|TEMP] VIEW [IF NOT EXISTS] `(#993) - Thanks @gabivlj
* Support for `CREATE VIEW … WITH NO SCHEMA BINDING` Redshift (#979) - Thanks @lustefaniak
* Support `UNPIVOT` and a fix for chained PIVOTs (#983) - @jmhain
* Support for `LIMIT BY` (#977) - Thanks @lustefaniak
* Support for mixed BigQuery table name quoting (#971) - Thanks @iffyio
* Support `DELETE` with `ORDER BY` and `LIMIT` (MySQL) (#992) - Thanks @ulrichsg
* Support `EXTRACT` for `DAYOFWEEK`, `DAYOFYEAR`, `ISOWEEK`, `TIME` (#980) - Thanks @lustefaniak
* Support `ATTACH DATABASE` (#989) - Thanks @lovasoa

### Fixed
* Fix handling of `/~%` in Snowflake stage name (#1009) - Thanks @lustefaniak
* Fix column `COLLATE` not displayed (#1012) - Thanks @lustefaniak
* Fix for clippy 1.73 (#995) - Thanks @alamb

### Changed
* Test to ensure `+ - * / %` binary operators work the same in all dialects (#1025)  - Thanks @lustefaniak
* Improve documentation on Parser::consume_token and friends (#994) - Thanks @alamb
* Test that regexp can be used as an identifier in postgres (#1018) - Thanks @lovasoa
* Add docstrings for Dialects, update README (#1016) - Thanks @alamb
* Add JumpWire to users in README (#990) - Thanks @hexedpackets
* Add tests for clickhouse: `tokenize == as Token::DoubleEq` (#981)- Thanks @lustefaniak

## [0.38.0] 2023-09-21

### Added

* Support `==`operator for Sqlite (#970) - Thanks @marhoily
* Support mysql `PARTITION` to table selection (#959) - Thanks  @chunshao90
* Support `UNNEST` as a table factor for PostgreSQL (#968) @hexedpackets
* Support MySQL `UNIQUE KEY` syntax (#962) - Thanks @artorias1024
* Support` `GROUP BY ALL` (#964) - @berkaysynnada
* Support multiple actions in one ALTER TABLE statement (#960) - Thanks @ForbesLindesay
* Add `--sqlite param` to CLI (#956) - Thanks @ddol

### Fixed
* Fix Rust 1.72 clippy lints (#957) - Thanks @alamb

### Changed
* Add missing token loc in parse err msg (#965) - Thanks @ding-young
* Change how `ANY` and `ALL` expressions are represented in AST (#963) - Thanks @SeanTroyUWO
* Show location info in parse errors (#958) - Thanks @MartinNowak
* Update release documentation (#954) - Thanks @alamb
* Break test and coverage test into separate jobs (#949) - Thanks @alamb


## [0.37.0] 2023-08-22

### Added
* Support `FOR SYSTEM_TIME AS OF` table time travel clause support, `visit_table_factor` to Visitor (#951) - Thanks @gruuya
* Support MySQL `auto_increment` offset in table definition (#950) - Thanks @ehoeve
* Test for mssql table name in square brackets (#952) - Thanks @lovasoa
* Support additional Postgres `CREATE INDEX` syntax (#943) - Thanks @ForbesLindesay
* Support `ALTER ROLE` syntax of PostgreSQL and MS SQL Server (#942) - Thanks @r4ntix
* Support table-level comments (#946) - Thanks @ehoeve
* Support `DROP TEMPORARY TABLE`, MySQL syntax (#916) - Thanks @liadgiladi
* Support posgres type alias (#933) - Thanks @Kikkon

### Fixed
* Clarify the value of the special flag (#948) - Thanks @alamb
* Fix `SUBSTRING` from/to argument construction for mssql (#947) - Thanks @jmaness
* Fix: use Rust idiomatic capitalization for newly added DataType enums (#939) - Thanks @Kikkon
* Fix `BEGIN TRANSACTION` being serialized as `START TRANSACTION` (#935) - Thanks @lovasoa
* Fix parsing of datetime functions without parenthesis (#930) - Thanks @lovasoa

## [0.36.1] 2023-07-19

### Fixed
* Fix parsing of identifiers after '%' symbol (#927) - Thanks @alamb

## [0.36.0] 2023-07-19

### Added
* Support toggling "unescape" mode to retain original escaping (#870)  - Thanks @canalun
* Support UNION (ALL) BY NAME syntax (#915) - Thanks @parkma99
* Add doc comment for all operators (#917) - Thanks @izveigor
* Support `PGOverlap` operator (#912) - Thanks @izveigor
* Support multi args for unnest (#909) - Thanks  @jayzhan211
* Support `ALTER VIEW`, MySQL syntax (#907) - Thanks  @liadgiladi
* Add DeltaLake keywords (#906) - Thanks @roeap

### Fixed
* Parse JsonOperators correctly (#913) - Thanks @izveigor
* Fix dependabot by removing rust-toolchain toml (#922) - Thanks @alamb

### Changed
* Clean up JSON operator tokenizing code (#923) - Thanks @alamb
* Upgrade bigdecimal to 0.4.1 (#921) - Thanks @jinlee0
* Remove most instances of #[cfg(feature(bigdecimal))] in tests (#910) - Thanks @alamb

## [0.35.0] 2023-06-23

### Added
* Support `CREATE PROCEDURE` of MSSQL (#900) - Thanks  @delsehi
* Support DuckDB's `CREATE MACRO` statements (#897) - Thanks @MartinNowak
* Support for `CREATE TYPE (AS)` statements (#888) - Thanks @srijs
* Support `STRICT` tables of sqlite (#903) - Thanks @parkma99

### Fixed
* Fixed precedence of unary negation operator with operators: Mul, Div and Mod (#902) - Thanks  @izveigor

### Changed
* Add `support_group_by_expr` to `Dialect` trait (#896) - Thanks @jdye64
* Update criterion requirement from `0.4` to `0.5` in `/sqlparser_bench` (#890) - Thanks @dependabot (!!)

## [0.34.0] 2023-05-19

### Added

* Support named window frames (#881)  - Thanks @berkaysynnada, @mustafasrepo, and @ozankabak
* Support for `ORDER BY` clauses in aggregate functions (#882)  - Thanks @mustafasrepo
* Support `DuckDB` dialect (#878) - Thanks @eitsupi
* Support optional `TABLE` keyword for `TRUNCATE TABLE` (#883) - Thanks @mobuchowski
* Support MySQL's `DIV` operator (#876) - Thanks @eitsupi
* Support Custom operators (#868) - Thanks @max-sixty
* Add `Parser::parse_multipart_identifier`  (#860) - Thanks @Jefffrey
* Support for multiple expressions, order by in `ARRAY_AGG` (#879) - Thanks @mustafasrepo
* Support for query source in `COPY .. TO` statement (#858) - Thanks @aprimadi
* Support `DISTINCT ON (...)` (#852) - Thanks @aljazerzen
* Support multiple-table `DELETE` syntax (#855) - Thanks @AviRaboah
* Support `COPY INTO` in `SnowflakeDialect` (#841) - Thanks @pawel-big-lebowski
* Support identifiers beginning with digits in MySQL (#856) - Thanks @AviRaboah

### Changed
* Include license file in published crate (#871) - Thanks @ankane
* Make `Expr::Interval` its own struct (#872) - Thanks @aprimadi
* Add dialect_from_str and improve Dialect documentation (#848) - Thanks @alamb
* Add clickhouse to example (#849) - Thanks @anglinb

### Fixed
* Fix merge conflict (#885) - Thanks @alamb
* Fix tiny typo in custom_sql_parser.md (#864) - Thanks @okue
* Fix logical merge conflict (#865) - Thanks @alamb
* Test trailing commas (#859) - Thanks @aljazerzen


## [0.33.0] 2023-04-10

### Added
* Support for Mysql Backslash escapes (enabled by default) (#844) - Thanks @cobyge
* Support "UPDATE" statement in "WITH" subquery (#842) - Thanks @nicksrandall
* Support PIVOT table syntax (#836) - Thanks @pawel-big-lebowski
* Support CREATE/DROP STAGE for Snowflake (#833) - Thanks @pawel-big-lebowski
* Support Non-Latin characters (#840) - Thanks @mskrzypkows
* Support PostgreSQL: GENERATED { ALWAYS | BY DEFAULT } AS IDENTITY and GENERATED  - Thanks @sam-mmm
* Support IF EXISTS in COMMENT statements (#831) - Thanks @pawel-big-lebowski
* Support snowflake alter table swap with (#825) - Thanks @pawel-big-lebowski

### Changed
* Move tests from parser.rs to appropriate parse_XX tests (#845) - Thanks @alamb
* Correct typos in parser.rs (#838) - Thanks @felixonmars
* Improve documentation on verified_* methods (#828) - Thanks @alamb

## [0.32.0] 2023-03-6

### Added
* Support ClickHouse `CREATE TABLE` with `ORDER BY` (#824) - Thanks @ankrgyl
* Support PostgreSQL exponentiation `^` operator (#813) - Thanks @michael-2956
* Support `BIGNUMERIC` type in BigQuery (#811) - Thanks @togami2864
* Support for optional trailing commas (#810) - Thanks @ankrgyl

### Fixed
* Fix table alias parsing regression by backing out redshift column definition list (#827) - Thanks @alamb
* Fix typo in `ReplaceSelectElement`  `colum_name` --> `column_name` (#822) - Thanks @togami2864

## [0.31.0] 2023-03-1

### Added
* Support raw string literals for BigQuery dialect (#812) - Thanks @togami2864
* Support `SELECT * REPLACE <Expr> AS <Identifier>` in BigQuery dialect (#798) - Thanks @togami2864
* Support byte string literals for BigQuery dialect (#802) - Thanks @togami2864
* Support  columns definition list for system information functions in RedShift dialect (#769) - Thanks @mskrzypkows
* Support `TRANSIENT` keyword in Snowflake dialect (#807) - Thanks @mobuchowski
* Support `JSON` keyword (#799) - Thanks @togami2864
* Support MySQL Character Set Introducers (#788) - Thanks @mskrzypkows

### Fixed
* Fix clippy error in ci (#803) - Thanks @togami2864
* Handle  offset in map key in BigQuery dialect  (#797) - Thanks @Ziinc
* Fix a typo (precendence -> precedence) (#794) - Thanks @SARDONYX-sard
* use post_* visitors for mutable visits (#789) - Thanks @lovasoa

### Changed
* Add another known user (#787) - Thanks @joocer

## [0.30.0] 2023-01-02

### Added
* Support `RENAME` for wildcard `SELECTs` (#784) - Thanks @Jefffrey
* Add a mutable visitor (#782) - Thanks @lovasoa

### Changed
* Allow parsing of mysql empty row inserts (#783) - Thanks @Jefffrey

### Fixed
* Fix logical conflict (#785) - Thanks @alamb

## [0.29.0] 2022-12-29

### Highlights
* Partial source location tracking: see #710
* Recursion limit to prevent stack overflows: #764
* AST visitor: #765

### Added
feat: dollar-quoted strings support (#772) - Thanks @vasilev-alex
* Add derive based AST visitor (#765) - Thanks @tustvold
* Support `ALTER INDEX {INDEX_NAME} RENAME TO {NEW_INDEX_NAME}` (#767) - Thanks @devgony
* Support `CREATE TABLE ON UPDATE <expr>` Function (#685) - Thanks @CEOJINSUNG
* Support `CREATE FUNCTION` definition with `$$` (#755)- Thanks @zidaye
* Add location tracking in the tokenizer and parser (#710)  - Thanks @ankrgyl
* Add configurable recursion limit to parser, to protect against stackoverflows (#764)  - Thanks @alamb
* Support parsing scientific notation (such as `10e5`) (#768) - Thanks @Jefffrey
* Support `DROP FUNCTION` syntax (#752) - Thanks @zidaye
* Support json operators `@>` `<@`, `@?` and `@@` - Thanks @audunska
* Support the type key (#750)- Thanks @yuval-illumex

### Changed
* Improve docs and add examples for Visitor (#778) - Thanks @alamb
* Add a backlink from sqlparse_derive to sqlparser and publishing instructions (#779) - Thanks @alamb
* Document new features, update authors (#776) - Thanks @alamb
* Improve Readme (#774) - Thanks @alamb
* Standardize comments on parsing optional keywords (#773) - Thanks @alamb
* Enable grouping sets parsing for `GenericDialect` (#771) - Thanks @Jefffrey
* Generalize conflict target (#762) - Thanks @audunska
* Generalize locking clause (#759) - Thanks @audunska
* Add negative test for except clause on wildcards (#746)- Thanks @alamb
* Add `NANOSECOND` keyword (#749)- Thanks @waitingkuo

### Fixed
* ParserError if nested explain (#781) - Thanks @Jefffrey
* Fix cargo docs / warnings and add CI check (#777) - Thanks @alamb
* unnest join constraint with alias parsing for BigQuery dialect (#732)- Thanks @Ziinc

## [0.28.0] 2022-12-05

### Added
* Support for `EXCEPT` clause on wildcards (#745) - Thanks @AugustoFKL
* Support `CREATE FUNCTION` Postgres options (#722) - Thanks @wangrunji0408
* Support `CREATE TABLE x AS TABLE y` (#704) - Thanks @sarahyurick
* Support MySQL `ROWS` syntax for `VALUES` (#737) - Thanks @aljazerzen
* Support `WHERE` condition for `UPDATE ON CONFLICT` (#735) - Thanks @zidaye
* Support `CLUSTER BY` when creating Materialized View (#736) - Thanks @yuval-illumex
* Support nested comments (#726) - Thanks @yang-han
* Support `USING` method when creating indexes. (#731) - Thanks @step-baby and @yangjiaxin01
* Support `SEMI`/`ANTI` `JOIN` syntax (#723) - Thanks @mingmwang
* Support `EXCLUDE` support for snowflake and generic dialect (#721) - Thanks @AugustoFKL
* Support `MATCH AGAINST` (#708) - Thanks @AugustoFKL
* Support `IF NOT EXISTS` in `ALTER TABLE ADD COLUMN` (#707) - Thanks @AugustoFKL
* Support `SET TIME ZONE <value>` (#727)  - Thanks @waitingkuo
* Support `UPDATE ... FROM ( subquery )` (#694) - Thanks  @unvalley

### Changed
* Add `Parser::index()` method to get current parsing index (#728) - Thanks @neverchanje
* Add `COMPRESSION` as keyword (#720)- Thanks @AugustoFKL
* Derive `PartialOrd`, `Ord`, and `Copy` whenever possible (#717) - Thanks @AugustoFKL
* Fixed `INTERVAL` parsing logic and precedence (#705) - Thanks @sarahyurick
* Support updating multiple column names whose names are the same as(#725)  - Thanks @step-baby

### Fixed
* Clean up some redundant code in parser (#741) - Thanks @alamb
* Fix logical conflict - Thanks @alamb
* Cleanup to avoid is_ok() (#740) - Thanks @alamb
* Cleanup to avoid using unreachable! when parsing semi/anti join (#738) - Thanks @alamb
* Add an example to docs to clarify semantic analysis (#739) - Thanks @alamb
* Add information about parting semantic logic to README.md (#724) - Thanks @AugustoFKL
* Logical conflicts - Thanks @alamb
* Tiny typo in docs (#709) - Thanks @pmcgee69


## [0.27.0] 2022-11-11

### Added
* Support `ON CONFLICT` and `RETURNING` in `UPDATE` statement (#666) - Thanks @main and @gamife
* Support `FULLTEXT` option on create table for MySQL and Generic dialects (#702) - Thanks @AugustoFKL
* Support `ARRAY_AGG` for Bigquery and Snowflake (#662) - Thanks @SuperBo
* Support DISTINCT for SetOperator (#689) - Thanks @unvalley
* Support the ARRAY type of Snowflake (#699) - Thanks @yuval-illumex
* Support create sequence with options INCREMENT, MINVALUE, MAXVALUE, START etc. (#681) - Thanks @sam-mmm
* Support `:` operator for semi-structured data in Snowflake(#693) - Thanks @yuval-illumex
* Support ALTER TABLE DROP PRIMARY KEY (#682) - Thanks @ding-young
* Support `NUMERIC` and `DEC` ANSI data types (#695) - Thanks @AugustoFKL
* Support modifiers for Custom Datatype (#680) - Thanks @sunng87

### Changed
* Add precision for TIME, DATETIME, and TIMESTAMP data types (#701) - Thanks @AugustoFKL
* add Date keyword (#691) - Thanks @sarahyurick
* Update simple_logger requirement from 2.1 to 4.0 - Thanks @dependabot

### Fixed
* Fix broken DataFusion link (#703) - Thanks @jmg-duarte
* Add MySql, BigQuery to all dialects tests, fixed bugs (#697) - Thanks @omer-shtivi


## [0.26.0] 2022-10-19

### Added
* Support MySQL table option `{INDEX | KEY}` in CREATE TABLE definiton (#665) - Thanks @AugustoFKL
* Support `CREATE [ { TEMPORARY | TEMP } ] SEQUENCE [ IF NOT EXISTS ] <sequence_name>` (#678) - Thanks @sam-mmm
* Support `DROP SEQUENCE` statement (#673) - Thanks @sam-mmm
* Support for ANSI types  `CHARACTER LARGE OBJECT[(p)]` and `CHAR LARGE OBJECT[(p)]` (#671)  - Thanks @AugustoFKL
* Support `[CACHE|UNCACHE] TABLE` (#670) - Thanks @francis-du
* Support `CEIL(expr TO DateTimeField)` and `FLOOR(expr TO DateTimeField)` - Thanks @sarahyurick
* Support all ansii character string types, (#648) - Thanks @AugustoFKL

### Changed
* Support expressions inside window frames (#655) - Thanks @mustafasrepo and @ozankabak
* Support unit on char length units for small character strings (#663) - Thanks @AugustoFKL
* Replace booleans on `SET ROLE` with a single enum. (#664) - Thanks @AugustoFKL
* Replace `Option`s with enum for `DECIMAL` precision (#654) - Thanks @AugustoFKL

## [0.25.0] 2022-10-03

### Added

* Support `AUTHORIZATION` clause in `CREATE SCHEMA` statements (#641) - Thanks @AugustoFKL
* Support optional precision for `CLOB` and `BLOB` (#639) - Thanks @AugustoFKL
* Support optional precision in `VARBINARY` and `BINARY` (#637) - Thanks @AugustoFKL


### Changed
* `TIMESTAMP` and `TIME` parsing preserve zone information (#646) - Thanks @AugustoFKL

### Fixed

* Correct order of arguments when parsing  `LIMIT x,y` , restrict to `MySql` and `Generic` dialects - Thanks @AugustoFKL


## [0.24.0] 2022-09-29

### Added

* Support `MILLENNIUM` (2 Ns) (#633) - Thanks @sarahyurick
* Support `MEDIUMINT`  (#630) - Thanks @AugustoFKL
* Support `DOUBLE PRECISION` (#629) - Thanks @AugustoFKL
* Support precision in `CLOB`, `BINARY`, `VARBINARY`, `BLOB` data type (#618) - Thanks @ding-young
* Support `CREATE ROLE` and `DROP ROLE` (#598) - Thanks @blx
* Support full range of sqlite prepared statement placeholders (#604) - Thanks @lovasoa
* Support National string literal with lower case `n` (#612) - Thanks @mskrzypkows
* Support SHOW FUNCTIONS (#620) - Thanks @joocer
* Support `set time zone to 'some-timezone'` (#617) - Thanks @waitingkuo

### Changed
* Move `Value::Interval` to `Expr::Interval` (#609) - Thanks @ding-young
* Update `criterion` dev-requirement from 0.3 to 0.4 in /sqlparser_bench (#611) - Thanks @dependabot
* Box `Query` in `Cte` (#572) - Thanks @MazterQyou

### Other
* Disambiguate CREATE ROLE ... USER and GROUP (#628) - Thanks @alamb
* Add test for optional WITH in CREATE ROLE (#627) - Thanks @alamb

## [0.23.0] 2022-09-08

### Added
* Add support for aggregate expressions with filters (#585) - Thanks @andygrove
* Support `LOCALTIME` and `LOCALTIMESTAMP` time functions (#592) - Thanks @MazterQyou

## [0.22.0] 2022-08-26

### Added
* Support `OVERLAY` expressions (#594) - Thanks @ayushg
* Support `WITH TIMEZONE` and `WITHOUT TIMEZONE` when parsing `TIMESTAMP` expressions (#589) - Thanks @waitingkuo
* Add ability for dialects to override prefix, infix, and statement parsing (#581) - Thanks @andygrove

## [0.21.0] 2022-08-18

### Added
* Support `IS [NOT] TRUE`, `IS [NOT] FALSE`, and `IS [NOT] UNKNOWN` - Thanks (#583) @sarahyurick
* Support `SIMILAR TO` syntax (#569) - Thanks @ayushdg
* Support `SHOW COLLATION` (#564) - Thanks @MazterQyou
* Support `SHOW TABLES` (#563) - Thanks @MazterQyou
* Support `SET NAMES literal [COLLATE literal]` (#558) - Thanks @ovr
* Support trailing commas (#557) in `BigQuery` dialect - Thanks @komukomo
* Support `USE <DB>` (#565) - Thanks @MazterQyou
* Support `SHOW COLUMNS FROM tbl FROM db` (#562) - Thanks @MazterQyou
* Support `SHOW VARIABLES` for `MySQL` dialect  (#559) - Thanks @ovr and @vasilev-alex

### Changed
* Support arbitrary expression in `SET` statement (#574) - Thanks @ovr and @vasilev-alex
* Parse LIKE patterns as Expr not Value (#579) - Thanks @andygrove
* Update Ballista link in README (#576) - Thanks @sanxiyn
* Parse  `TRIM` from with optional expr and `FROM` expr  (#573) - Thanks @ayushdg
* Support PostgreSQL array subquery constructor (#566) - Thanks @MazterQyou
* Clarify contribution licensing (#570) - Thanks @alamb
* Update for new clippy ints (#571) - Thanks @alamb
* Change `Like` and `ILike` to `Expr` variants, allow escape char (#569) - Thanks @ayushdg
* Parse special keywords as functions (`current_user`, `user`, etc) (#561) - Thanks @ovr
* Support expressions in `LIMIT`/`OFFSET` (#567) - Thanks @MazterQyou

## [0.20.0] 2022-08-05

### Added
* Support custom `OPERATOR` postgres syntax (#548) - Thanks @iskakaushik
* Support `SAFE_CAST` for BigQuery (#552) - Thanks @togami2864

### Changed
* Added SECURITY.md (#546) - Thanks @JamieSlome
* Allow `>>` and `<<` binary operators in Generic dialect (#553) - Thanks @ovr
* Allow `NestedJoin` with an alias (#551) - Thanks @waitingkuo

## [0.19.0] 2022-07-28

### Added

* Support `ON CLUSTER` for `CREATE TABLE` statement (ClickHouse DDL) (#527) - Thanks @andyrichardson
* Support empty `ARRAY` literals (#532) - Thanks @bitemyapp
* Support `AT TIME ZONE` clause (#539) - Thanks @bitemyapp
* Support `USING` clause and table aliases in `DELETE` (#541) - Thanks @mobuchowski
* Support `SHOW CREATE VIEW` statement (#536) - Thanks @mrob95
* Support `CLONE` clause in `CREATE TABLE` statements (#542) - Thanks @mobuchowski
* Support `WITH OFFSET Alias` in table references (#528) - Thanks @sivchari
* Support double quoted (`"`) literal strings:  (#530) - Thanks @komukomo
* Support `ON UPDATE` clause on column definitions in `CREATE TABLE` statements (#522) - Thanks @frolovdev


### Changed:

* `Box`ed `Query` body to save stack space (#540) - Thanks @5tan
* Distinguish between `INT` and `INTEGER` types (#525) - Thanks @frolovdev
* Parse `WHERE NOT EXISTS` as `Expr::Exists` rather than `Expr::UnaryOp` for consistency (#523) - Thanks @frolovdev
* Support `Expr` instead of `String` for argument to `INTERVAL` (#517) - Thanks @togami2864

### Fixed:

* Report characters instead of bytes in error messages (#529) - Thanks @michael-2956


## [0.18.0] 2022-06-06

### Added

* Support `CLOSE` (cursors) (#515) - Thanks @ovr
* Support `DECLARE` (cursors) (#509) - Thanks @ovr
* Support `FETCH` (cursors) (#510) - Thanks @ovr
* Support `DATETIME` keyword (#512) - Thanks @komukomo
* Support `UNNEST` as a table factor (#493) - Thanks @sivchari
* Support `CREATE FUNCTION` (hive flavor) (#496) - Thanks @mobuchowski
* Support placeholders (`$` or `?`) in `LIMIT` clause (#494) - Thanks @step-baby
* Support escaped string literals (PostgreSQL) (#502) - Thanks @ovr
* Support  `IS TRUE` and `IS FALSE` (#499) - Thanks @ovr
* Support `DISCARD [ALL | PLANS | SEQUENCES | TEMPORARY | TEMP]` (#500) - Thanks @gandronchik
* Support `array<..>` HIVE data types (#491) - Thanks @mobuchowski
* Support `SET` values that begin with `-` #495  - Thanks @mobuchowski
* Support unicode whitespace (#482) - Thanks @alexsatori
* Support `BigQuery` dialect (#490) - Thanks @komukomo

### Changed:
* Add docs for MapAccess (#489) - Thanks @alamb
* Rename `ArrayIndex::indexs` to `ArrayIndex::indexes` (#492) - Thanks @alamb

### Fixed:
* Fix escaping of trailing quote in quoted identifiers (#505) - Thanks @razzolini-qpq
* Fix parsing of `COLLATE` after parentheses in expressions (#507) - Thanks @razzolini-qpq
* Distinguish tables and nullary functions in `FROM` (#506) - Thanks @razzolini-qpq
* Fix `MERGE INTO` semicolon handling (#508) - Thanks @mskrzypkows

## [0.17.0] 2022-05-09

### Added

* Support `#` as first character in field name for `RedShift` dialect (#485) - Thanks @yuval-illumex
* Support for postgres composite types (#466) - Thanks @poonai
* Support `TABLE` keyword with SELECT INTO (#487) - Thanks @MazterQyou
* Support `ANY`/`ALL` operators (#477) - Thanks @ovr
* Support `ArrayIndex` in `GenericDialect` (#480) - Thanks @ovr
* Support `Redshift` dialect, handle square brackets properly (#471) - Thanks @mskrzypkows
* Support `KILL` statement (#479) - Thanks @ovr
* Support `QUALIFY` clause on `SELECT` for `Snowflake` dialect (#465) - Thanks @mobuchowski
* Support `POSITION(x IN y)` function  syntax  (#463)  @yuval-illumex
* Support  `global`,`local`, `on commit` for `create temporary table` (#456) - Thanks @gandronchik
* Support `NVARCHAR` data type (#462) - Thanks @yuval-illumex
* Support for postgres json operators `->`, `->>`, `#>`, and `#>>` (#458) - Thanks @poonai
* Support `SET ROLE` statement (#455) - Thanks @slhmy

### Changed:
* Improve docstrings for `KILL` statement (#481) - Thanks @alamb
* Add negative tests for `POSITION` (#469) - Thanks @alamb
* Add negative tests for `IN` parsing (#468) - Thanks @alamb
* Suppport table names (as well as subqueries) as source in `MERGE` statements (#483) - Thanks @mskrzypkows


### Fixed:
* `INTO` keyword is optional for `INSERT`, `MERGE`  (#473) - Thanks @mobuchowski
* Support `IS TRUE` and `IS FALSE` expressions in boolean filter (#474) - Thanks @yuval-illumex
* Support fully qualified object names in `SET VARIABLE` (#484) - Thanks mobuchowski

## [0.16.0] 2022-04-03

### Added

* Support `WEEK` keyword in `EXTRACT` (#436) - Thanks @Ted-Jiang
* Support `MERGE` statement (#430) - Thanks @mobuchowski
* Support `SAVEPOINT` statement (#438) - Thanks @poonai
* Support `TO` clause in `COPY` (#441) - Thanks @matthewmturner
* Support `CREATE DATABASE` statement (#451) - Thanks @matthewmturner
* Support `FROM` clause in `UPDATE` statement (#450) - Thanks @slhmy
* Support additional `COPY` options (#446) - Thanks @wangrunji0408

### Fixed:
* Bug in array / map access parsing (#433) - Thanks @monadbobo

## [0.15.0] 2022-03-07

### Added

* Support for ClickHouse array types (e.g. [1,2,3]) (#429) - Thanks @monadbobo
* Support for `unsigned tinyint`, `unsigned int`, `unsigned smallint` and `unsigned bigint` datatypes (#428) - Thanks @watarukura
* Support additional keywords for `EXTRACT`  (#427) - Thanks @mobuchowski
* Support IN UNNEST(expression) (#426) - Thanks @komukomo
* Support COLLATION keywork on CREATE TABLE (#424) - Thanks @watarukura
* Support FOR UPDATE/FOR SHARE clause (#418) - Thanks @gamife
* Support prepared statement placeholder arg `?` and `$` (#420) - Thanks @gamife
* Support array expressions such as `ARRAY[1,2]` , `foo[1]` and `INT[][]` (#419) - Thanks @gamife

### Changed:
* remove Travis CI (#421) - Thanks @efx

### Fixed:
* Allow `array` to be used as a function name again (#432) - @alamb
* Update docstring reference to `Query` (#423) - Thanks @max-sixty

## [0.14.0] 2022-02-09

### Added
* Support `CURRENT_TIMESTAMP`, `CURRENT_TIME`, and `CURRENT_DATE` (#391) - Thanks @yuval-illumex
* SUPPORT `SUPER` keyword (#387) - Thanks @flaneur2020
* Support differing orders of `OFFSET` `LIMIT` as well as `LIMIT` `OFFSET` (#413) - Thanks @yuval-illumex
* Support for `FROM <filename>`, `DELIMITER`, and `CSV HEADER` options for `COPY` command (#409) - Thanks @poonai
* Support `CHARSET` and `ENGINE` clauses on `CREATE TABLE` for mysql (#392) - Thanks @antialize
* Support `DROP CONSTRAINT [ IF EXISTS ] <name> [ CASCADE ]` (#396) - Thanks @tvallotton
* Support parsing tuples and add `Expr::Tuple` (#414) - @alamb
* Support MySQL style `LIMIT X, Y` (#415) - @alamb
* Support `SESSION TRANSACTION` and `TRANSACTION SNAPSHOT`. (#379) - Thanks @poonai
* Support `ALTER COLUMN` and `RENAME CONSTRAINT`  (#381) - Thanks @zhamlin
* Support for Map access, add ClickHouse dialect (#382)  - Thanks @monadbobo

### Changed
* Restrict where wildcard (`*`) can appear, add to `FunctionArgExpr` remove `Expr::[Qualified]Wildcard`, (#378) - Thanks @panarch
* Update simple_logger requirement from 1.9 to 2.1 (#403)
* export all methods of parser (#397) - Thanks @neverchanje!
* Clarify maintenance status on README (#416) - @alamb

### Fixed
* Fix new clippy errors (#412) - @alamb
* Fix panic with `GRANT/REVOKE` in `CONNECT`, `CREATE`, `EXECUTE` or `TEMPORARY`  - Thanks @evgenyx00
* Handle double quotes inside quoted identifiers correctly (#411) - Thanks @Marwes
* Handle mysql backslash escaping (#373) - Thanks @vasilev-alex

## [0.13.0] 2021-12-10

### Added
* Add ALTER TABLE CHANGE COLUMN, extend the UPDATE statement with ON clause (#375) - Thanks @0xA537FD!
* Add support for GROUPIING SETS, ROLLUP and CUBE - Thanks @Jimexist!
* Add basic support for GRANT and REVOKE (#365) - Thanks @blx!

### Changed
* Use Rust 2021 edition (#368) - Thanks @Jimexist!

### Fixed
* Fix clippy errors (#367, #374) - Thanks @Jimexist!


## [0.12.0] 2021-10-14

### Added
* Add support for [NOT] IS DISTINCT FROM (#306) - @Dandandan

### Changed
* Move the keywords module - Thanks @koushiro!


## [0.11.0] 2021-09-24

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


## [0.10.0] 2021-08-23

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

## [0.9.0] 2021-03-21

### Added
* Add support for `TRY_CAST` syntax (#299) - Thanks @seddonm1!

## [0.8.0] 2021-02-20

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

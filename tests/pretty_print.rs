use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

fn prettify(sql: &str) -> String {
    let ast = Parser::parse_sql(&GenericDialect {}, sql).unwrap();
    format!("{:#}", ast[0])
}

#[test]
fn test_pretty_print_select() {
    assert_eq!(
        prettify("SELECT a, b, c FROM my_table WHERE x = 1 AND y = 2"),
        r#"
SELECT
  a,
  b,
  c
FROM
  my_table
WHERE
  x = 1 AND y = 2
"#
        .trim()
    );
}

#[test]
fn test_pretty_print_join() {
    assert_eq!(
        prettify("SELECT a FROM table1 JOIN table2 ON table1.id = table2.id"),
        r#"
SELECT
  a
FROM
  table1
  JOIN table2 ON table1.id = table2.id
"#
        .trim()
    );
}

#[test]
fn test_pretty_print_subquery() {
    assert_eq!(
        prettify("SELECT * FROM (SELECT a, b FROM my_table) AS subquery"),
        r#"
SELECT
  *
FROM
  (
    SELECT
      a,
      b
    FROM
      my_table
  ) AS subquery
"#
        .trim()
    );
}

#[test]
fn test_pretty_print_union() {
    assert_eq!(
        prettify("SELECT a FROM table1 UNION SELECT b FROM table2"),
        r#"
SELECT
  a
FROM
  table1
UNION
SELECT
  b
FROM
  table2
"#
        .trim()
    );
}

#[test]
fn test_pretty_print_group_by() {
    assert_eq!(
        prettify("SELECT a, COUNT(*) FROM my_table GROUP BY a HAVING COUNT(*) > 1"),
        r#"
SELECT
  a,
  COUNT(*)
FROM
  my_table
GROUP BY
  a
HAVING
  COUNT(*) > 1
"#
        .trim()
    );
}

#[test]
fn test_pretty_print_cte() {
    assert_eq!(
        prettify("WITH cte AS (SELECT a, b FROM my_table) SELECT * FROM cte"),
        r#"
WITH cte AS (
  SELECT
    a,
    b
  FROM
    my_table
)
SELECT
  *
FROM
  cte
"#
        .trim()
    );
}

#[test]
fn test_pretty_print_case_when() {
    assert_eq!(
        prettify("SELECT CASE WHEN x > 0 THEN 'positive' WHEN x < 0 THEN 'negative' ELSE 'zero' END FROM my_table"),
        r#"
SELECT
  CASE
    WHEN x > 0 THEN
      'positive'
    WHEN x < 0 THEN
      'negative'
    ELSE
      'zero'
  END
FROM
  my_table
"#.trim()
    );
}

#[test]
fn test_pretty_print_window_function() {
    assert_eq!(
        prettify("SELECT id, value, ROW_NUMBER() OVER (PARTITION BY category ORDER BY value DESC) as rank FROM my_table"),
        r#"
SELECT
  id,
  value,
  ROW_NUMBER() OVER (
    PARTITION BY category
    ORDER BY value DESC
  ) AS rank
FROM
  my_table
"#.trim()
    );
}

#[test]
fn test_pretty_print_multiline_string() {
    assert_eq!(
        prettify("SELECT 'multiline\nstring' AS str"),
        r#"
SELECT
  'multiline
string' AS str
"#
        .trim(),
        "A  literal string with a newline should be kept as is. The contents of the string should not be indented."
    );
}

#[test]
#[ignore = "https://github.com/apache/datafusion-sqlparser-rs/issues/1850"]
fn test_pretty_print_insert_values() {
    assert_eq!(
        prettify("INSERT INTO my_table (a, b, c) VALUES (1, 2, 3), (4, 5, 6)"),
        r#"
INSERT INTO my_table (a, b, c)
VALUES
  (1, 2, 3),
  (4, 5, 6)
"#
        .trim()
    );
}

#[test]
#[ignore = "https://github.com/apache/datafusion-sqlparser-rs/issues/1850"]
fn test_pretty_print_insert_select() {
    assert_eq!(
        prettify("INSERT INTO my_table (a, b) SELECT x, y FROM source_table"),
        r#"
INSERT INTO my_table (a, b)
SELECT
  x,
  y
FROM
  source_table
"#
        .trim()
    );
}

#[test]
fn test_pretty_print_update() {
    assert_eq!(
        prettify("UPDATE my_table SET a = 1, b = 2 WHERE x > 0 RETURNING id, name"),
        r#"
UPDATE my_table
SET
  a = 1,
  b = 2
WHERE
  x > 0
RETURNING
  id,
  name
"#
        .trim()
    );
}

#[test]
fn test_pretty_print_delete() {
    assert_eq!(
        prettify("DELETE FROM my_table WHERE x > 0 RETURNING id, name"),
        r#"
DELETE FROM
  my_table
WHERE
  x > 0
RETURNING
  id,
  name
"#
        .trim()
    );

    assert_eq!(
        prettify("DELETE table1, table2"),
        r#"
DELETE
  table1,
  table2
"#
        .trim()
    );
}

#[test]
#[ignore = "https://github.com/apache/datafusion-sqlparser-rs/issues/1850"]
fn test_pretty_print_create_table() {
    assert_eq!(
        prettify("CREATE TABLE my_table (id INT PRIMARY KEY, name VARCHAR(255) NOT NULL, CONSTRAINT fk_other FOREIGN KEY (id) REFERENCES other_table(id))"),
        r#"
CREATE TABLE my_table (
  id INT PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  CONSTRAINT fk_other FOREIGN KEY (id) REFERENCES other_table(id)
)
"#
        .trim()
    );
}

#[test]
#[ignore = "https://github.com/apache/datafusion-sqlparser-rs/issues/1850"]
fn test_pretty_print_create_view() {
    assert_eq!(
        prettify("CREATE VIEW my_view AS SELECT a, b FROM my_table WHERE x > 0"),
        r#"
CREATE VIEW my_view AS
SELECT
  a,
  b
FROM
  my_table
WHERE
  x > 0
"#
        .trim()
    );
}

#[test]
#[ignore = "https://github.com/apache/datafusion-sqlparser-rs/issues/1850"]
fn test_pretty_print_create_function() {
    assert_eq!(
        prettify("CREATE FUNCTION my_func() RETURNS INT BEGIN SELECT COUNT(*) INTO @count FROM my_table; RETURN @count; END"),
        r#"
CREATE FUNCTION my_func() RETURNS INT
BEGIN
  SELECT COUNT(*) INTO @count FROM my_table;
  RETURN @count;
END
"#
        .trim()
    );
}

#[test]
#[ignore = "https://github.com/apache/datafusion-sqlparser-rs/issues/1850"]
fn test_pretty_print_json_table() {
    assert_eq!(
        prettify("SELECT * FROM JSON_TABLE(@json, '$[*]' COLUMNS (id INT PATH '$.id', name VARCHAR(255) PATH '$.name')) AS jt"),
        r#"
SELECT
  *
FROM
  JSON_TABLE(
    @json,
    '$[*]' COLUMNS (
      id INT PATH '$.id',
      name VARCHAR(255) PATH '$.name'
    )
  ) AS jt
"#
        .trim()
    );
}

#[test]
#[ignore = "https://github.com/apache/datafusion-sqlparser-rs/issues/1850"]
fn test_pretty_print_transaction_blocks() {
    assert_eq!(
        prettify("BEGIN; UPDATE my_table SET x = 1; COMMIT;"),
        r#"
BEGIN;
UPDATE my_table SET x = 1;
COMMIT;
"#
        .trim()
    );
}

#[test]
#[ignore = "https://github.com/apache/datafusion-sqlparser-rs/issues/1850"]
fn test_pretty_print_control_flow() {
    assert_eq!(
        prettify("IF x > 0 THEN SELECT 'positive'; ELSE SELECT 'negative'; END IF;"),
        r#"
IF x > 0 THEN
  SELECT 'positive';
ELSE
  SELECT 'negative';
END IF;
"#
        .trim()
    );
}

#[test]
#[ignore = "https://github.com/apache/datafusion-sqlparser-rs/issues/1850"]
fn test_pretty_print_merge() {
    assert_eq!(
        prettify("MERGE INTO target_table t USING source_table s ON t.id = s.id WHEN MATCHED THEN UPDATE SET t.value = s.value WHEN NOT MATCHED THEN INSERT (id, value) VALUES (s.id, s.value)"),
        r#"
MERGE INTO target_table t
USING source_table s ON t.id = s.id
WHEN MATCHED THEN
  UPDATE SET t.value = s.value
WHEN NOT MATCHED THEN
  INSERT (id, value) VALUES (s.id, s.value)
"#
        .trim()
    );
}

#[test]
#[ignore = "https://github.com/apache/datafusion-sqlparser-rs/issues/1850"]
fn test_pretty_print_create_index() {
    assert_eq!(
        prettify("CREATE INDEX idx_name ON my_table (column1, column2)"),
        r#"
CREATE INDEX idx_name
ON my_table (column1, column2)
"#
        .trim()
    );
}

#[test]
#[ignore = "https://github.com/apache/datafusion-sqlparser-rs/issues/1850"]
fn test_pretty_print_explain() {
    assert_eq!(
        prettify("EXPLAIN ANALYZE SELECT * FROM my_table WHERE x > 0"),
        r#"
EXPLAIN ANALYZE
SELECT
  *
FROM
  my_table
WHERE
  x > 0
"#
        .trim()
    );
}

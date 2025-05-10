use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

fn parse_and_format(sql: &str) -> String {
    let ast = Parser::parse_sql(&GenericDialect {}, sql).unwrap();
    format!("{:#}", ast[0])
}

#[test]
fn test_pretty_print_select() {
    assert_eq!(
        parse_and_format("SELECT a, b, c FROM my_table WHERE x = 1 AND y = 2"),
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
        parse_and_format("SELECT a FROM table1 JOIN table2 ON table1.id = table2.id"),
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
        parse_and_format("SELECT * FROM (SELECT a, b FROM my_table) AS subquery"),
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
        parse_and_format("SELECT a FROM table1 UNION SELECT b FROM table2"),
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
        parse_and_format("SELECT a, COUNT(*) FROM my_table GROUP BY a HAVING COUNT(*) > 1"),
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
        parse_and_format("WITH cte AS (SELECT a, b FROM my_table) SELECT * FROM cte"),
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
        parse_and_format("SELECT CASE WHEN x > 0 THEN 'positive' WHEN x < 0 THEN 'negative' ELSE 'zero' END FROM my_table"),
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
        parse_and_format("SELECT id, value, ROW_NUMBER() OVER (PARTITION BY category ORDER BY value DESC) as rank FROM my_table"),
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

use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

#[test]
fn test_pretty_print_select() {
    let sql = "SELECT a, b, c FROM my_table WHERE x = 1 AND y = 2";
    let ast = Parser::parse_sql(&GenericDialect {}, sql).unwrap();
    let pretty = format!("{:#}", ast[0]);
    assert_eq!(
        pretty,
        r#"SELECT
  a,
  b,
  c
FROM
  my_table
WHERE
  x = 1 AND y = 2"#
    );
}

#[test]
fn test_pretty_print_join() {
    let sql = "SELECT a FROM table1 JOIN table2 ON table1.id = table2.id";
    let ast = Parser::parse_sql(&GenericDialect {}, sql).unwrap();
    let pretty = format!("{:#}", ast[0]);
    assert_eq!(
        pretty,
        r#"SELECT
  a
FROM
  table1
  JOIN table2 ON table1.id = table2.id"#
    );
}

#[test]
fn test_pretty_print_subquery() {
    let sql = "SELECT * FROM (SELECT a, b FROM my_table) AS subquery";
    let ast = Parser::parse_sql(&GenericDialect {}, sql).unwrap();
    let pretty = format!("{:#}", ast[0]);
    assert_eq!(
        pretty,
        r#"SELECT
  *
FROM
  (
    SELECT
      a,
      b
    FROM
      my_table
  ) AS subquery"#
    );
}

#[test]
fn test_pretty_print_union() {
    let sql = "SELECT a FROM table1 UNION SELECT b FROM table2";
    let ast = Parser::parse_sql(&GenericDialect {}, sql).unwrap();
    let pretty = format!("{:#}", ast[0]);
    assert_eq!(
        pretty,
        r#"SELECT
  a
FROM
  table1
UNION
SELECT
  b
FROM
  table2"#
    );
}

#[test]
fn test_pretty_print_group_by() {
    let sql = "SELECT a, COUNT(*) FROM my_table GROUP BY a HAVING COUNT(*) > 1";
    let ast = Parser::parse_sql(&GenericDialect {}, sql).unwrap();
    let pretty = format!("{:#}", ast[0]);
    assert_eq!(
        pretty,
        r#"SELECT
  a,
  COUNT(*)
FROM
  my_table
GROUP BY
  a
HAVING
  COUNT(*) > 1"#
    );
}

#[test]
fn test_pretty_print_cte() {
    let sql = "WITH cte AS (SELECT a, b FROM my_table) SELECT * FROM cte";
    let ast = Parser::parse_sql(&GenericDialect {}, sql).unwrap();
    let pretty = format!("{:#}", ast[0]);
    assert_eq!(
        pretty,
        r#"WITH cte AS (
  SELECT
    a,
    b
  FROM
    my_table
)
SELECT
  *
FROM
  cte"#
    );
}

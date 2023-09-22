SELECT
  table_a.* EXCLUDE foo,
  table_b.* EXCLUDE (bar, baz)
FROM
  table_a,
  table_b

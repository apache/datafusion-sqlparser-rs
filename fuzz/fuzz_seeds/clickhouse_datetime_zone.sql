CREATE TABLE t (a DateTime('UTC'), b DateTime(3, 'Europe/Rome')) ENGINE = MergeTree ORDER BY a

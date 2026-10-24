-- digit-leading identifiers, read like MySQL's: identifiers unless the run is a number
SELECT 123abc, 1_a, 1e, 1ea, 0x1_a, 0xg, 1L, 10_000_rows FROM t;
SELECT t.1, t.123, t.12e34, t.1col, 1db.1table.1column FROM 1db.1table t;
SELECT 1 AS 1e, 2 AS 123abc;
SELECT * FROM 1_t JOIN 2_t ON 1_t.1id = 2_t.2id WHERE 2_t.3col > 1 GROUP BY 1_t.1id;
INSERT INTO TABLE 1_t SELECT 1col, 2col FROM 2_t;
CREATE TABLE 1_t (1col INT, 2col STRING);
-- numbers that must stay numbers next to the identifiers
SELECT 1e3a, 1e5x, 1.5abc, 1e+5, 1e-5, 15e29, 0x1F, MIN(MIN(10, 5), 1L) FROM t;

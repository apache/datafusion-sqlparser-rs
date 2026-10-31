-- digit-leading identifiers, which MySQL reads as identifiers unless the run is a number
SELECT 123abc, 1_a, 1e, 1ea, 0x1_a, 0xg, 0b01z, 100L, 123col_$@123abc FROM t;
SELECT t.1, t.123, t.12e34, t.1col, 1db.1table.1column FROM 1db.1table AS t;
SELECT 1 AS 1e, 2 AS 123abc, 3 AS 0x1_a;
SELECT * FROM 1_t JOIN 2_t ON 1_t.1id = 2_t.2id WHERE 2_t.3col > 1 GROUP BY 1_t.1id;
INSERT INTO 1_t (8_e, 123col_$@length123) VALUES (1, 67.654);
UPDATE 1_t SET 1col = 2 WHERE 1col > 1;
CREATE TABLE 1_t (1col INT, 2col VARCHAR(10));
-- numbers that must stay numbers next to the identifiers
SELECT 1e3a, 1e5x, 1.5abc, 1e+5, 1e-5, 15e29, 0x1F, 0b01, 1_a + 1e3, -1_a FROM t;

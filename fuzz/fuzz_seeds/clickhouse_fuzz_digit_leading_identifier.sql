-- digit-leading identifiers, which ClickHouse lexes as bare words
SELECT 8_e;
SELECT 8_e1largeALTER, 8large, 1_a, 10_00_, 10___0, 1_;
SELECT 1e_1, 1e1_, 1e1__0, 1e3a, 1ea, 1e5x, 1L;
SELECT 0x1_, 0x_1, 0xg, 0b2;
SELECT 1 AS 8_e, 2 AS 1e3a, 3 AS 0x1_;
SELECT 8_e.x FROM (SELECT 1 AS x) AS 8_e;
SELECT t.8_e, t.1a, t.1, t.1.2, (1, 2).1 FROM t;
SELECT 8_e FROM 1_t WHERE 2_c = 3_d GROUP BY 4_g ORDER BY 5_o;
CREATE TABLE t8 (8_e Int32, 1a String) ENGINE = Memory;
DROP TABLE 8_e;
ALTER TABLE 1_t ADD COLUMN 2_c Int32;
INSERT INTO 1_t (8_e) VALUES (1);
-- numbers that must stay numbers next to the identifiers
SELECT 1_000, 1_000.123_456, 1e5, 1E5, 1e+5, 1e-5, 1e10, 0xFF_FF, 0b101, 1.5L, 8_e;
SELECT 1_000 + 8_e, 8_e * 1e5, -1_a, +0x1_;
SELECT 8_e$x, 1$;

CREATE OR REPLACE AGGREGATE my_count(*) (SFUNC = int8inc, STYPE = bigint, INITCOND = '0')

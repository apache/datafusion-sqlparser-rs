CREATE AGGREGATE public.my_min(integer) (SFUNC = int4smaller, STYPE = integer, SORTOP = OPERATOR(pg_catalog.<))

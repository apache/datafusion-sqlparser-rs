CREATE AGGREGATE s.my_agg(IN a numeric(10,2), VARIADIC b text[]) (SFUNC = s.f, STYPE = integer[], INITCOND = '{}', SORTOP = <, HYPOTHETICAL)

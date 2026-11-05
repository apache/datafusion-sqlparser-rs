CREATE FUNCTION public.f(x integer) RETURNS SETOF integer
    LANGUAGE sql
    IMMUTABLE STRICT
    COST 2.5 ROWS 10 SUPPORT public.f_support
    AS 'SELECT x'

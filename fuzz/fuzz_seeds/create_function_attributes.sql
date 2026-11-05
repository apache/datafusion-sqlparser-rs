CREATE FUNCTION public.f(x integer) RETURNS integer
    LANGUAGE sql
    IMMUTABLE STRICT
    COST 2.5
    RETURN x

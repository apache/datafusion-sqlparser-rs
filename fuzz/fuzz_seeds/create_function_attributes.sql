CREATE FUNCTION public.f(x integer) RETURNS SETOF integer
    LANGUAGE c
    WINDOW
    IMMUTABLE STRICT NOT LEAKPROOF
    COST 2.5 ROWS 10 SUPPORT public.f_support
    AS 'obj', 'sym'

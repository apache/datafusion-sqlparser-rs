CREATE STATISTICS IF NOT EXISTS public.s (mcv, dependencies) ON a, (b + 1) FROM public.t;
CREATE STATISTICS (ndistinct) ON lower(a) FROM t

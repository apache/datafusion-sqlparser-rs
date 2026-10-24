SELECT concat(VARIADIC ARRAY[1, 2]);
SELECT concat_ws('-', VARIADIC ARRAY['a']);
SELECT * FROM pg_get_publication_tables(VARIADIC ARRAY['pub'::TEXT]) AS gpt;
SELECT f(VARIADIC a => ARRAY[1]);
SELECT concat(VARIADIC -a);
SELECT f(VARIADIC separator)

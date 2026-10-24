SELECT a IN t, a NOT IN t, a IN main.t, a IN f(), a NOT IN f(1, 2), a IN main.f(1) FROM t;
SELECT 1 IN view, 1 IN offset, 1 IN key, 1 IN abort FROM t;
SELECT 1 IN "from" FROM t

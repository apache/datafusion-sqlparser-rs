#!/usr/bin/clickhouse-local --queries-file
# leading line comment
#! shebang style comment
SELECT 1 # trailing comment
;
SELECT a, b # , c
FROM t # table
WHERE x = 1 # it's a comment with an apostrophe
;
SELECT # comment right after SELECT
    a,
    #! comment between select items
    b
FROM t;
SELECT 1 #!no space after bang, the semicolon is part of the comment;
;
SELECT 1 #   several spaces then comment
;
SELECT '# not a comment', "# not a comment", `# not a comment` FROM t # real comment
;
SELECT a FROM t WHERE s = 'a # b' # comment containing 'quotes' and "double quotes"
;
SELECT a FROM t -- dash comment
# hash comment
/* block comment */
#! shebang comment
WHERE a = 1;
SELECT /* # not a comment */ 1 # comment with /* block */ inside
;
SELECT 1 # comment with -- inside
;
SELECT 1 # comment with # another hash # and more
;
SELECT * FROM t PREWHERE x GLOBAL IN (1, 2) # after prewhere
SETTINGS max_threads = 1 # after settings
;
INSERT INTO t2 SELECT * FROM t # comment in insert
;
CREATE TABLE t (a Int32, # comment after column
    b String # comment after last column
) ENGINE = MergeTree ORDER BY a # comment after order by
;
WITH c AS (SELECT 1 AS v) # comment after cte
SELECT * FROM c;
SELECT arrayMap(x -> x + 1, # comment inside call
    [1, 2]) # comment after call
;
SELECT 1 # comment at end of file without newline
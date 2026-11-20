CREATE VIRTUAL TABLE ft USING fts5(body, b UNINDEXED, tokenize = 'porter', content='it''s', f(x, (y)));

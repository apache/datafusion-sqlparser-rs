CREATE INDEX i ON t USING gist (c tsvector_ops (siglen = 100))

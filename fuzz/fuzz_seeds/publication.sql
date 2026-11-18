CREATE PUBLICATION a FOR ALL TABLES WITH (publish = 'insert', publish_via_partition_root);
DROP PUBLICATION IF EXISTS p, a CASCADE;

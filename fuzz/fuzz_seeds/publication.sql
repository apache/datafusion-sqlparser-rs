CREATE PUBLICATION p FOR TABLE ONLY public.items (id) WHERE ((id > 0)), other *, TABLES IN SCHEMA feed, CURRENT_SCHEMA, TABLE ONLY (last) WITH (publish = 'insert');
CREATE PUBLICATION a FOR ALL TABLES WITH (publish = 'insert', publish_via_partition_root);
ALTER PUBLICATION p SET (publish_generated_columns = stored);
ALTER PUBLICATION p OWNER TO CURRENT_ROLE;
ALTER PUBLICATION a RENAME TO b;
DROP PUBLICATION IF EXISTS p, b CASCADE;

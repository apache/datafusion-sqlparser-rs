CREATE PUBLICATION p FOR TABLE ONLY public.items (id) WHERE ((id > 0)), other *, TABLES IN SCHEMA feed, CURRENT_SCHEMA, TABLE ONLY (last) WITH (publish = 'insert');
CREATE PUBLICATION a FOR ALL TABLES WITH (publish = 'insert', publish_via_partition_root);
DROP PUBLICATION IF EXISTS p, a CASCADE;

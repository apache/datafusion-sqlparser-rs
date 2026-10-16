UPDATE t SET a = a + 1 WHERE c IS NOT NULL;
UPDATE db.t ON CLUSTER c1 SET a = a + 1 WHERE c IS NOT NULL;
UPDATE t ON CLUSTER '{cluster}' SET a = a + 1, b = 'z' WHERE c IS NOT NULL;
UPDATE t SET a = a + 1 WHERE c IS NOT NULL SETTINGS update_parallel_mode = 'auto';
UPDATE db.t ON CLUSTER c1 SET a = a + 1, b = 'z' WHERE c IS NOT NULL SETTINGS update_parallel_mode = 'auto', mutations_sync = 2
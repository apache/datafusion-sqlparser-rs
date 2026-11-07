ALTER TABLE t UPDATE x = x IN (1, 2) IN PARTITION ID '2', ID '3' WHERE user_id = 42, DELETE IN PARTITION 2 WHERE tenant_id = 7 SETTINGS mutations_sync = 1;

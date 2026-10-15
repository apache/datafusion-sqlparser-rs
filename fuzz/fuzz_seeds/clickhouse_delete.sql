DELETE FROM t WHERE a < 0;
DELETE FROM db.t ON CLUSTER c1 WHERE a < 0;
DELETE FROM t ON CLUSTER '{cluster}' WHERE a < 0;
DELETE FROM t WHERE a < 0 SETTINGS lightweight_deletes_sync = 0;
DELETE FROM db.t ON CLUSTER '{cluster}' WHERE a < 0 SETTINGS lightweight_deletes_sync = 0, mutations_sync = 2
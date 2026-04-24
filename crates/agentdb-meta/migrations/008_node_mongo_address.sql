-- Advertise the Mongo wire listener address alongside the
-- REST address so peer nodes can forward `find` / `insert` /
-- `update` / `delete` commands targeting a session this
-- node doesn't own.
--
-- Nullable so clusters mid-upgrade don't crash: existing
-- rows keep `mongo_address = NULL` until the node rewrites
-- its heartbeat with the new column populated.

ALTER TABLE nodes
    ADD COLUMN mongo_address TEXT;

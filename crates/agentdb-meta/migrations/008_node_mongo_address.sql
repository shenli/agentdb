-- Legacy peer listener address retained for migration compatibility.
-- New V2 deployments do not use this field.
--
-- Nullable so clusters mid-upgrade don't crash: existing
-- rows keep `mongo_address = NULL` until the node rewrites
-- its heartbeat with the new column populated.

ALTER TABLE nodes
    ADD COLUMN mongo_address TEXT;

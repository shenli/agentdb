-- Legacy peer listener address retained for migration compatibility.
-- New V2 deployments do not use this field.

ALTER TABLE nodes
    ADD COLUMN mongo_address TEXT;

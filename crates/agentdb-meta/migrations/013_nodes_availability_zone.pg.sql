-- Per agent-db-v2.md §2.7: cross-AZ placement (Postgres dialect).
ALTER TABLE nodes ADD COLUMN availability_zone TEXT;

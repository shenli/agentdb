-- Cross-AZ placement (Postgres dialect).
ALTER TABLE nodes ADD COLUMN availability_zone TEXT;

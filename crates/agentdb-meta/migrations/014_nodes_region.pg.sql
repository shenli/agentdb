-- Durable node region metadata. `availability_zone` remains the finer-grained
-- failure-domain field; residency/routing uses region first and falls back to
-- AZ for pre-migration deployments that only populated the prototype label.
ALTER TABLE nodes ADD COLUMN region TEXT;

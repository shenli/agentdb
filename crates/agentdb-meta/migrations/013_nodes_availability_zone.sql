-- Per agent-db-v2.md §2.7: each node declares its AZ in
-- the membership row so the scheduler's placement / handoff
-- logic can prefer cross-AZ candidates. NULL = "no AZ
-- declared" (single-AZ deployments and pre-migration nodes
-- both fall through to the existing load-balanced
-- placement, exactly as today).
ALTER TABLE nodes ADD COLUMN availability_zone TEXT;

-- Store-incarnation registry: the control-plane half of the durable
-- store-identity check (distributed-safety-hardening.md §3).
--
-- A store_incarnation is a random id minted once, when a data directory
-- is first initialized (see agentdb_storage::store_identity). This table
-- is the authority on which incarnation currently owns each node id, so a
-- node can learn a fact its local IDENTITY.json cannot: that its own
-- incarnation was retired (its disk replaced), or that another live
-- incarnation already holds its node id. Startup fails closed on both.
--
-- One row per (node_id, store_incarnation); at most one 'active' per
-- node_id (enforced by admission logic, not a constraint, so history is
-- retained). Replacing a store is an explicit two-step: retire the old
-- incarnation, then the new one is admitted on next boot.

CREATE TABLE IF NOT EXISTS store_incarnations (
    node_id           TEXT NOT NULL,
    store_incarnation TEXT NOT NULL,
    state             TEXT NOT NULL,           -- 'active' | 'retired'
    admitted_at_ms    BIGINT NOT NULL,
    retired_at_ms     BIGINT,
    PRIMARY KEY (node_id, store_incarnation)
);
